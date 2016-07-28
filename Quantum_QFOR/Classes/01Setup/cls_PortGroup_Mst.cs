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

using System;
using System.Collections;
using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_PortGroup_Mst : CommonFeatures
    {
        #region "List of Members of Class"

        /// <summary>
        /// The m_ por t_ grou p_ pk
        /// </summary>
        private Int16 M_PORT_GROUP_PK;

        /// <summary>
        /// The m_ por t_ grou p_ code
        /// </summary>
        private string M_PORT_GROUP_CODE;

        /// <summary>
        /// The m_ por t_ grou p_ desc
        /// </summary>
        private string M_PORT_GROUP_DESC;

        #endregion "List of Members of Class"

        /// <summary>
        /// The m_ data set
        /// </summary>
        private static DataSet M_DataSet = new DataSet();

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the comm GRP_ pk.
        /// </summary>
        /// <value>
        /// The comm GRP_ pk.
        /// </value>
        public Int16 CommGrp_Pk
        {
            get { return M_PORT_GROUP_PK; }
            set { M_PORT_GROUP_PK = value; }
        }

        /// <summary>
        /// Gets or sets the comm GRP_ code.
        /// </summary>
        /// <value>
        /// The comm GRP_ code.
        /// </value>
        public string CommGrp_Code
        {
            get { return M_PORT_GROUP_CODE; }
            set { M_PORT_GROUP_CODE = value; }
        }

        /// <summary>
        /// Gets or sets the comm GRP_ desc.
        /// </summary>
        /// <value>
        /// The comm GRP_ desc.
        /// </value>
        public string CommGrp_Desc
        {
            get { return M_PORT_GROUP_DESC; }
            set { M_PORT_GROUP_DESC = value; }
        }

        /// <summary>
        /// Gets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public static DataSet MyDataSet
        {
            get { return M_DataSet; }
        }

        #endregion "List of Properties"

        #region "Constructor"

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_PortGroup_Mst"/> class.
        /// </summary>
        /// <param name="SelectAll">if set to <c>true</c> [select all].</param>
        public cls_PortGroup_Mst(bool SelectAll = false)
        {
            string Sql = null;
            if (SelectAll)
            {
                Sql += " select 0 COMMODITY_GROUP_PK,";
                Sql += " '<ALL>' COMMODITY_GROUP_CODE, ";
                Sql += " ' ' COMMODITY_GROUP_DESC, ";
                Sql += " 0 VERSION_NO from dual UNION ";
            }
            Sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            Sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO ";
            Sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            Sql += " WHERE CG.ACTIVE_FLAG=1 ";
            Sql += " ORDER BY COMMODITY_GROUP_CODE ";
            try
            {
                M_DataSet = (new WorkFlow()).GetDataSet(Sql);
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

        #endregion "Constructor"

        #region "Fetch Listing"

        /// <summary>
        /// Fetches the listing.
        /// </summary>
        /// <param name="P_Port_Group_Pk">The p_ port_ group_ pk.</param>
        /// <param name="P_Port_Group_Code">The p_ port_ group_ code.</param>
        /// <param name="P_Port_Group_Desc">The p_ port_ group_ desc.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="isActive">The is active.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchListing(Int16 P_Port_Group_Pk = 0, string P_Port_Group_Code = "", string P_Port_Group_Desc = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2, Int16 isActive = -1, bool blnSortAscending = false,
        Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition = "";
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            strCondition = strCondition + " PGMT.ACTIVE_FLAG = 1 ";

            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }
            else
            {
                strCondition += " AND 1=1 ";
            }

            if (P_Port_Group_Pk > 0)
            {
                strCondition = strCondition + " AND PGMT.PORT_GRP_MST_PK = " + P_Port_Group_Pk;
            }

            if (P_Port_Group_Code.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(PGMT.PORT_GRP_ID) LIKE '" + P_Port_Group_Code.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(PGMT.PORT_GRP_ID) LIKE '%" + P_Port_Group_Code.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(PGMT.PORT_GRP_ID) LIKE '%" + P_Port_Group_Code.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (P_Port_Group_Desc.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(PGMT.PORT_GRP_NAME) LIKE '" + P_Port_Group_Desc.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(PGMT.PORT_GRP_NAME) LIKE '%" + P_Port_Group_Desc.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(PGMT.PORT_GRP_NAME) LIKE '%" + P_Port_Group_Desc.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (isActive == 1)
            {
                strCondition += " AND PGMT.ACTIVE_FLAG = 1 ";
            }
            sb.Append(" SELECT COUNT(*)");
            sb.Append("  FROM PORT_GRP_MST_TBL PGMT");
            sb.Append("  WHERE ");
            sb.Append("" + strCondition + "");

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

            sb.Length = 0;
            sb.Append(" SELECT * FROM (SELECT ROWNUM SLNO,q.* FROM");
            sb.Append("   (SELECT PGMT.ACTIVE_FLAG,");
            sb.Append("       PGMT.PORT_GRP_MST_PK,");
            sb.Append("       PGMT.PORT_GRP_ID,");
            sb.Append("       PGMT.PORT_GRP_NAME,");
            sb.Append("       PGMT.VERSION_NO,");
            sb.Append("       '' DELFLAG,");
            sb.Append("       '' CHGFLAG");
            sb.Append("  FROM PORT_GRP_MST_TBL PGMT");
            sb.Append("  WHERE ");
            sb.Append("" + strCondition + "");

            if (!strColumnName.Equals("SLNO"))
            {
                sb.Append(" ORDER BY " + strColumnName);
            }
            if (!blnSortAscending & !strColumnName.Equals("SLNO"))
            {
                sb.Append(" DESC");
            }
            sb.Append(" )Q ) WHERE SLNO  BETWEEN " + start + " AND " + last);
            try
            {
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

        #endregion "Fetch Listing"

        #region "Fetch Entry Header"

        /// <summary>
        /// Fetches the header.
        /// </summary>
        /// <param name="PortGroupPK">The port group pk.</param>
        /// <returns></returns>
        public DataSet FetchHeader(string PortGroupPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("   SELECT PGMT.ACTIVE_FLAG,");
            sb.Append("       PGMT.PORT_GRP_MST_PK,");
            sb.Append("       PGMT.PORT_GRP_ID,");
            sb.Append("       PGMT.PORT_GRP_NAME,");
            sb.Append("       PGMT.BIZ_TYPE,");
            sb.Append("       PGMT.VERSION_NO,");
            sb.Append("       '' DELFLAG,");
            sb.Append("       '' CHGFLAG");
            sb.Append("  FROM PORT_GRP_MST_TBL PGMT");
            sb.Append(" WHERE PGMT.ACTIVE_FLAG = 1");
            sb.Append("  AND PGMT.PORT_GRP_MST_PK IN (" + PortGroupPK + ")");
            try
            {
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

        #endregion "Fetch Entry Header"

        #region "Fetch Grid Details"

        /// <summary>
        /// Fetches the details.
        /// </summary>
        /// <param name="PortsGroupPK">The ports group pk.</param>
        /// <param name="PortsPK">The ports pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchDetails(string PortsGroupPK = "0", string PortsPK = "0", string BizType = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append(" SELECT * FROM (");
            sb.Append("  SELECT PGT.PORT_GRP_TRN_PK,");
            sb.Append("       '0' DELFLAG,");
            sb.Append("       CMT.COUNTRY_MST_PK,");
            sb.Append("       CMT.COUNTRY_ID,");
            sb.Append("       CMT.COUNTRY_NAME,");
            sb.Append("       PMT.PORT_MST_PK PORT_MST_FK,");
            sb.Append("       PMT.PORT_ID,");
            sb.Append("       PMT.PORT_NAME,");
            sb.Append("       '' CHGFLAG");
            sb.Append("  FROM PORT_GRP_MST_TBL PGMT,");
            sb.Append("       PORT_GRP_TRN_TBL PGT,");
            sb.Append("       COUNTRY_MST_TBL  CMT,");
            sb.Append("       PORT_MST_TBL     PMT");
            sb.Append(" WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
            sb.Append("   AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
            sb.Append("   AND PMT.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
            sb.Append("   AND PMT.BUSINESS_TYPE =" + BizType);
            sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + PortsPK + ")");
            sb.Append("   AND PGMT.PORT_GRP_MST_PK IN (" + PortsGroupPK + ")");
            sb.Append(" UNION ");
            sb.Append(" SELECT 0 PORT_GRP_TRN_PK,");
            sb.Append("       '0' DELFLAG,");
            sb.Append("       CMT.COUNTRY_MST_PK,");
            sb.Append("       CMT.COUNTRY_ID,");
            sb.Append("       CMT.COUNTRY_NAME,");
            sb.Append("       PMT.PORT_MST_PK PORT_MST_FK,");
            sb.Append("       PMT.PORT_ID,");
            sb.Append("       PMT.PORT_NAME,");
            sb.Append("       '' CHGFLAG");
            sb.Append("  FROM COUNTRY_MST_TBL CMT, PORT_MST_TBL PMT");
            sb.Append(" WHERE PMT.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
            sb.Append("   AND PMT.PORT_MST_PK IN (" + PortsPK + ")");
            sb.Append("   AND PMT.BUSINESS_TYPE =" + BizType);
            sb.Append(" )ORDER BY COUNTRY_NAME, PORT_ID");

            try
            {
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

        #endregion "Fetch Grid Details"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="GridDS">The grid ds.</param>
        /// <returns></returns>
        public ArrayList save(DataSet M_DataSet, DataSet GridDS)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            int PortGrpPK = 0;
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
                _with1.CommandText = objWK.MyUserName + ".PORT_GRP_MST_TBL_PKG.PORT_GRP_MST_TBL_INS";
                var _with2 = _with1.Parameters;
                _with2.Add("PORT_GRP_ID_IN", M_DataSet.Tables[0].Rows[0]["PORT_GRP_ID"]).Direction = ParameterDirection.Input;
                _with2.Add("PORT_GRP_NAME_IN", M_DataSet.Tables[0].Rows[0]["PORT_GRP_NAME"]).Direction = ParameterDirection.Input;
                _with2.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("ACTIVE_FLAG_IN", M_DataSet.Tables[0].Rows[0]["ACTIVE_FLAG"]).Direction = ParameterDirection.Input;
                _with2.Add("BIZ_TYPE_IN", M_DataSet.Tables[0].Rows[0]["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                _with2.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PORT_GRP_MST_PK").Direction = ParameterDirection.Output;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".PORT_GRP_MST_TBL_PKG.PORT_GRP_MST_TBL_UPD";
                var _with4 = _with3.Parameters;
                _with4.Add("PORT_GRP_MST_PK_IN", M_DataSet.Tables[0].Rows[0]["PORT_GRP_MST_PK"]).Direction = ParameterDirection.Input;
                _with4.Add("PORT_GRP_ID_IN", M_DataSet.Tables[0].Rows[0]["PORT_GRP_ID"]).Direction = ParameterDirection.Input;
                _with4.Add("PORT_GRP_NAME_IN", M_DataSet.Tables[0].Rows[0]["PORT_GRP_NAME"]).Direction = ParameterDirection.Input;
                _with4.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with4.Add("VERSION_NO_IN", M_DataSet.Tables[0].Rows[0]["VERSION_NO"]).Direction = ParameterDirection.Input;
                _with4.Add("ACTIVE_FLAG_IN", M_DataSet.Tables[0].Rows[0]["ACTIVE_FLAG"]).Direction = ParameterDirection.Input;
                _with4.Add("BIZ_TYPE_IN", M_DataSet.Tables[0].Rows[0]["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                _with4.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;


                var _with5 = objWK.MyDataAdapter;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["PORT_GRP_MST_PK"].ToString()))
                {
                    _with5.InsertCommand = insCommand;
                    _with5.InsertCommand.Transaction = TRAN;
                    RecAfct = _with5.InsertCommand.ExecuteNonQuery();
                    PortGrpPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                }
                else
                {
                    _with5.UpdateCommand = updCommand;
                    _with5.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with5.UpdateCommand.ExecuteNonQuery();
                    PortGrpPK = Convert.ToInt32(M_DataSet.Tables[0].Rows[0]["PORT_GRP_MST_PK"]);
                    //CType(updCommand.Parameters["RETURN_VALUE"].Value, Long)
                }

                if (RecAfct > 0)
                {
                    arrMessage = (ArrayList)SavePortGroupGrid(PortGrpPK, GridDS, TRAN);
                    if (arrMessage.Count == 0)
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        arrMessage.Add(PortGrpPK);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                else
                {
                    arrMessage.Add("Error");
                    TRAN.Rollback();
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

        #endregion "Save Function"

        #region "Save Port GroupGrid"

        /// <summary>
        /// Saves the port group grid.
        /// </summary>
        /// <param name="PortGrpPK">The port GRP pk.</param>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        public object SavePortGroupGrid(int PortGrpPK = 0, DataSet dsGrid = null, OracleTransaction TRAN = null)
        {
            Int32 RecAfct = default(Int32);
            Int32 i = default(Int32);
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            objWK.MyConnection = TRAN.Connection;
            try
            {
                if (dsGrid.Tables[0].Rows.Count > 0)
                {
                    for (i = 0; i <= dsGrid.Tables[0].Rows.Count - 1; i++)
                    {
                        string delFlag = "0";
                        if (!string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["DELFLAG"].ToString()))
                        {
                            delFlag = Convert.ToString(dsGrid.Tables[0].Rows[i]["DELFLAG"]);
                            if (delFlag.ToUpper() == "1" | delFlag.ToUpper() == "TRUE")
                            {
                                delFlag = "1";
                            }
                            else
                            {
                                delFlag = "0";
                            }
                        }

                        var _with6 = insCommand;
                        insCommand.Parameters.Clear();
                        _with6.Connection = objWK.MyConnection;
                        _with6.CommandType = CommandType.StoredProcedure;
                        _with6.CommandText = objWK.MyUserName + ".PORT_GRP_TRN_TBL_PKG.PORT_GRP_TRN_TBL_INS";
                        var _with7 = _with6.Parameters;
                        _with7.Add("PORT_GRP_MST_FK_IN", PortGrpPK).Direction = ParameterDirection.Input;
                        //.Add("ACTIVE_FLAG_IN", 1).Direction = ParameterDirection.Input
                        _with7.Add("PORT_MST_FK_IN", dsGrid.Tables[0].Rows[i]["PORT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with7.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                        _with7.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                        _with7.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        var _with8 = updCommand;
                        updCommand.Parameters.Clear();
                        _with8.Connection = objWK.MyConnection;
                        _with8.CommandType = CommandType.StoredProcedure;
                        _with8.CommandText = objWK.MyUserName + ".PORT_GRP_TRN_TBL_PKG.PORT_GRP_TRN_TBL_UPD";
                        var _with9 = _with8.Parameters;
                        _with9.Add("PORT_GRP_TRN_PK_IN", dsGrid.Tables[0].Rows[i]["PORT_GRP_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with9.Add("PORT_GRP_MST_FK_IN", PortGrpPK).Direction = ParameterDirection.Input;
                        _with9.Add("PORT_MST_FK_IN", dsGrid.Tables[0].Rows[i]["PORT_MST_FK"]).Direction = ParameterDirection.Input;
                        //.Add("ACTIVE_FLAG_IN", 1).Direction = ParameterDirection.Input
                        _with9.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                        //.Add("VERSION_NO_IN", 0).Direction = ParameterDirection.Input
                        _with9.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                        _with9.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        var _with10 = delCommand;
                        _with10.Parameters.Clear();
                        _with10.Connection = objWK.MyConnection;
                        _with10.CommandType = CommandType.StoredProcedure;
                        _with10.CommandText = objWK.MyUserName + ".PORT_GRP_TRN_TBL_PKG.PORT_GRP_TRN_DEL";
                        var _with11 = _with10.Parameters;
                        _with11.Add("PORT_GRP_MST_FK_IN", PortGrpPK).Direction = ParameterDirection.Input;
                        _with11.Add("PORT_MST_FK_IN", dsGrid.Tables[0].Rows[i]["PORT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with11.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                        _with11.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                        _with11.Add("RETURN_VALUE", OracleDbType.Varchar2, 250, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        

                        var _with12 = objWK.MyDataAdapter;
                        if (delFlag == "1")
                        {
                            _with12.DeleteCommand = delCommand;
                            _with12.DeleteCommand.Transaction = TRAN;
                            RecAfct = _with12.DeleteCommand.ExecuteNonQuery();
                        }
                        else
                        {
                            if (Convert.ToInt32(dsGrid.Tables[0].Rows[i]["PORT_GRP_TRN_PK"]) == 0)
                            {
                                _with12.InsertCommand = insCommand;
                                _with12.InsertCommand.Transaction = TRAN;
                                RecAfct = _with12.InsertCommand.ExecuteNonQuery();
                            }
                            else
                            {
                                _with12.UpdateCommand = updCommand;
                                _with12.UpdateCommand.Transaction = TRAN;
                                RecAfct = _with12.UpdateCommand.ExecuteNonQuery();
                            }
                        }
                    }
                }
                return arrMessage;
            }
            catch (OracleException oraEx)
            {
                arrMessage.Add(oraEx.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Save Port GroupGrid"

        #region "FetchNavigationSector"

        /// <summary>
        /// Fetches the navigation sector.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchNavigationSector()
        {
            DataSet objds = new DataSet();
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = " SELECT R.REGION_MST_PK RPK, R.REGION_CODE, R.REGION_NAME REGION_NAME FROM REGION_MST_TBL R WHERE R.ACTIVE_FLAG=1  ORDER BY R.REGION_NAME";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[0].TableName = "REGION";

                str = " SELECT A.AREA_MST_PK APK, A.AREA_ID, A.AREA_NAME AREA_NAME, A.REGION_MST_FK RFK FROM AREA_MST_TBL A, REGION_MST_TBL R WHERE R.REGION_MST_PK = A.REGION_MST_FK AND A.ACTIVE_FLAG = 1 AND R.ACTIVE_FLAG = 1 ORDER BY A.AREA_NAME";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[1].TableName = "AREA";

                DataRelation objRel1 = new DataRelation("REL_REG_AREA", objds.Tables[0].Columns["RPK"], objds.Tables[1].Columns["RFK"]);

                objRel1.Nested = true;
                objds.Relations.Add(objRel1);
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

        #endregion "FetchNavigationSector"

        #region "FetchNavigationPorts"

        /// <summary>
        /// Fetches the navigation ports.
        /// </summary>
        /// <param name="RegionPK">The region pk.</param>
        /// <param name="AreaPK">The area pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchNavigationPorts(string RegionPK = "0", string AreaPK = "0", string BizType = "0")
        {
            DataSet objds = new DataSet();
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = " SELECT A.AREA_MST_PK APK, A.AREA_ID, A.AREA_NAME AREA_NAME FROM AREA_MST_TBL A WHERE A.ACTIVE_FLAG = 1  AND A.AREA_MST_PK IN (" + AreaPK + ") ORDER BY A.AREA_NAME";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[0].TableName = "AREA";

                str = " SELECT C.COUNTRY_MST_PK CPK,  C.COUNTRY_ID, C.COUNTRY_NAME COUNTRY_NAME,  C.AREA_MST_FK AFK FROM COUNTRY_MST_TBL C WHERE C.ACTIVE_FLAG = 1  AND C.AREA_MST_FK IN(" + AreaPK + ") ORDER BY C.COUNTRY_NAME ";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[1].TableName = "COUNTRY";

                str = "SELECT DISTINCT P.PORT_MST_PK PPK, P.PORT_ID, P.PORT_NAME PORT_NAME, P.COUNTRY_MST_FK CFK FROM PORT_MST_TBL P,COUNTRY_MST_TBL C WHERE P.ACTIVE_FLAG = 1 AND c.active_flag=1 AND P.COUNTRY_MST_FK = C.COUNTRY_MST_PK  AND C.AREA_MST_FK IN (" + AreaPK + ")";
                if (BizType != "0")
                {
                    str += " AND P.BUSINESS_TYPE = " + BizType;
                }
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[2].TableName = "PORT";

                DataRelation objRel1 = new DataRelation("REL_AREA_CON", objds.Tables[0].Columns["APK"], objds.Tables[1].Columns["AFK"]);
                DataRelation objRel2 = new DataRelation("REL_CON_LOC", objds.Tables[1].Columns["CPK"], objds.Tables[2].Columns["CFK"]);

                objRel1.Nested = true;
                objRel2.Nested = true;

                objds.Relations.Add(objRel1);
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

        #endregion "FetchNavigationPorts"

        #region "FetchNavigationForLocation"

        /// <summary>
        /// Fetches the navigation for location.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchNavigationForLocation()
        {
            DataSet objds = new DataSet();
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = "SELECT CP.CORPORATE_MST_PK COPK, CP.CORPORATE_ID, CP.CORPORATE_NAME CORPORATE_NAME FROM CORPORATE_MST_TBL CP";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[0].TableName = "Corporate";

                str = "SELECT R.REGION_MST_PK RPK, R.REGIONCODE, R.REGIONNAME REGIONNAME,R.CORPORATE_MST_FK COFK FROM REGION_MST_TBL R ";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[1].TableName = "REGION";

                str = "SELECT L.Location_MST_PK LPK,l.location_id,l.location_name LOCATIONNAME,L.REPORTING_TO_FK RFK FROM LOCATION_MST_TBL l ";
                objds.Tables.Add(objWF.GetDataTable(str));
                DataRelation objRel1 = new DataRelation("REL_REG_LOC", objds.Tables[1].Columns["RPK"], objds.Tables[2].Columns["RFK"]);
                DataRelation objRel2 = new DataRelation("REL_COR_REG", objds.Tables[0].Columns["COPK"], objds.Tables[1].Columns["COFK"]);

                objRel1.Nested = true;

                objRel2.Nested = true;
                objds.Relations.Add(objRel1);
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

        #endregion "FetchNavigationForLocation"

        #region "GenerateSectors"

        /// <summary>
        /// Generates the sectors.
        /// </summary>
        /// <param name="RegionPK">The region pk.</param>
        /// <param name="AreaPK">The area pk.</param>
        /// <param name="CountryPk">The country pk.</param>
        /// <param name="Ports">The ports.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public DataSet generateSectors(string RegionPK = "", string AreaPK = "", string CountryPk = "", string Ports = "", string Flag = "0")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            DataSet CountDS = null;
            System.Text.StringBuilder strMainSql = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            RegionPK = RegionPK.TrimStart(',');
            AreaPK = AreaPK.TrimStart(',');
            CountryPk = CountryPk.TrimStart(',');
            Ports = Ports.TrimStart(',');

            strMainSql.Append(" SELECT ROWNUM \"SL_NR\", 'false' Sel,");
            strMainSql.Append(" ' ' TLI, ");
            strMainSql.Append("       POL.PORT_ID || '-' || POD.PORT_ID SECTOR_DESC,");
            strMainSql.Append("       POL.PORT_MST_PK POL_FK,");
            strMainSql.Append("       POD.PORT_MST_PK POD_FK,");
            strMainSql.Append("       POL.PORT_ID FROMPORT,");
            strMainSql.Append("       POD.PORT_ID TOPORT,");
            strMainSql.Append("       NULL DISTANCE_IN_MILES");
            strMainSql.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD");
            strMainSql.Append(" Where pol.port_mst_pk in");
            strMainSql.Append("       (SELECT PT.PORT_MST_PK");
            strMainSql.Append("          FROM PORT_MST_TBL    PT,");
            strMainSql.Append("               COUNTRY_MST_TBL CT,");
            strMainSql.Append("                 AREA_MST_TBL AR,");
            strMainSql.Append("                 REGION_MST_TBL  RG");
            strMainSql.Append("         WHERE PT.COUNTRY_MST_FK = CT.COUNTRY_MST_PK");
            strMainSql.Append("           AND CT.AREA_MST_FK = AR.AREA_MST_PK");
            strMainSql.Append("           AND AR.REGION_MST_FK = RG.REGION_MST_PK ");

            if (!string.IsNullOrEmpty(RegionPK) & RegionPK != "0")
            {
                strMainSql.Append(" AND RG.REGION_MST_PK IN (" + RegionPK + ")");
            }
            if (!string.IsNullOrEmpty(AreaPK) & AreaPK != "0")
            {
                strMainSql.Append(" AND AR.AREA_MST_PK IN (" + AreaPK + ")");
            }
            if (!string.IsNullOrEmpty(CountryPk) & CountryPk != "0")
            {
                strMainSql.Append(" AND CT.COUNTRY_MST_PK IN (" + CountryPk + ")");
            }
            if (!string.IsNullOrEmpty(Ports) & Ports != "0")
            {
                strMainSql.Append(" AND PT.PORT_MST_PK IN (" + Ports + ")");
            }

            strMainSql.Append("     )");

            strMainSql.Append("   and pod.port_mst_pk in");
            strMainSql.Append("       (SELECT PT.PORT_MST_PK");
            strMainSql.Append("          FROM PORT_MST_TBL    PT,");
            strMainSql.Append("               COUNTRY_MST_TBL CT, ");
            strMainSql.Append("                 AREA_MST_TBL AR,");
            strMainSql.Append("                 REGION_MST_TBL  RG");
            strMainSql.Append("         WHERE PT.COUNTRY_MST_FK = CT.COUNTRY_MST_PK");
            strMainSql.Append("           AND CT.AREA_MST_FK = AR.AREA_MST_PK");
            strMainSql.Append("           AND AR.REGION_MST_FK = RG.REGION_MST_PK ");
            strMainSql.Append("           And pol.port_mst_pk != pod.port_mst_pk ");
            strMainSql.Append("     )");
            strMainSql.Append("     AND NOT EXISTS");
            strMainSql.Append("       (SELECT 1");
            strMainSql.Append("                FROM SECTOR_MST_TBL SEC");
            strMainSql.Append("               WHERE SEC.FROM_PORT_FK =POL.PORT_MST_PK");
            strMainSql.Append("               AND SEC.TO_PORT_FK= POD.PORT_MST_PK )");
            if (Flag == "1")
            {
                strMainSql.Append("   AND 1=2 ");
            }

            strMainSql.Append("   ORDER BY SECTOR_DESC");

            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT T.*  FROM ");
            sqlstr.Append("  (" + strMainSql.ToString() + " ");
            sqlstr.Append("  ) T) QRY WHERE SL_NR  Between " + start + " and " + last + "  ");

            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "GenerateSectors"

        #region "Fetch Area"

        /// <summary>
        /// Fetches the area.
        /// </summary>
        /// <param name="regioncode">The regioncode.</param>
        /// <returns></returns>
        public string FetchArea(string regioncode)
        {
            string strSql = "SELECT * FROM";

            strSql = "SELECT REGION_mst_fk FROM AREA_MST_TBL WHERE area_mst_pk= " + regioncode;
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.ExecuteScaler(strSql);
            }
            catch (OracleException oraEx)
            {
                throw oraEx;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Area"
    }
}