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
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Cost_Element_Mst_Tbl : CommonFeatures
    {
        /// <summary>
        /// The m_ data set
        /// </summary>
        private DataSet M_DataSet;

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
        /// Initializes a new instance of the <see cref="cls_Cost_Element_Mst_Tbl"/> class.
        /// </summary>
        /// <param name="biztype">The biztype.</param>
        public cls_Cost_Element_Mst_Tbl(string biztype)
        {
            WorkFlow objWS = new WorkFlow();
            string strSQL = null;

            if (biztype == "3")
            {
                biztype = "1,2";
            }

            strSQL = " select c.cost_element_mst_pk,";
            strSQL += "      c.cost_element_id,";
            strSQL += "      c.cost_element_name ";
            strSQL += " from cost_element_mst_tbl c ";
            strSQL += "where c.business_type in (3," + biztype + ") ";
            strSQL += " and c.active_flag =1 order by c.cost_element_name ";
            try
            {
                M_DataSet = objWS.GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  13/09/2011
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

        #region "List of Members of the Class"

        /// <summary>
        /// The m_ cost_ element_ MST_ pk
        /// </summary>
        private Int64 M_Cost_Element_Mst_Pk;

        /// <summary>
        /// The m_ cost_ element_ identifier
        /// </summary>
        private string M_Cost_Element_Id;

        /// <summary>
        /// The m_ cost_ element_ name
        /// </summary>
        private string M_Cost_Element_Name;

        #endregion "List of Members of the Class"

        /// <summary>
        /// The m_ account_ code_ fk
        /// </summary>
        private Int64 M_Account_Code_Fk;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the cost_ element_ MST_ pk.
        /// </summary>
        /// <value>
        /// The cost_ element_ MST_ pk.
        /// </value>
        public Int64 Cost_Element_Mst_Pk
        {
            get { return M_Cost_Element_Mst_Pk; }
            set { M_Cost_Element_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the cost_ element_ identifier.
        /// </summary>
        /// <value>
        /// The cost_ element_ identifier.
        /// </value>
        public string Cost_Element_Id
        {
            get { return M_Cost_Element_Id; }
            set { M_Cost_Element_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the cost_ element_.
        /// </summary>
        /// <value>
        /// The name of the cost_ element_.
        /// </value>
        public string Cost_Element_Name
        {
            get { return M_Cost_Element_Name; }
            set { M_Cost_Element_Name = value; }
        }

        /// <summary>
        /// Gets or sets the account_ code_ fk.
        /// </summary>
        /// <value>
        /// The account_ code_ fk.
        /// </value>
        public Int64 Account_Code_Fk
        {
            get { return M_Account_Code_Fk; }
            set { M_Account_Code_Fk = value; }
        }

        #endregion "List of Properties"

        #region "Insert Function"

        /// <summary>
        /// Inserts this instance.
        /// </summary>
        /// <returns></returns>
        public Int64 Insert()
        {
            WorkFlow objWS = new WorkFlow();
            Int64 intPkVal = default(Int64);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with1 = objWS.MyCommand.Parameters;
                _with1.Add("Cost_Element_Id_IN", M_Cost_Element_Id).Direction = ParameterDirection.Input;
                _with1.Add("Cost_Element_Name_IN", M_Cost_Element_Name).Direction = ParameterDirection.Input;
                _with1.Add("Account_Code_Fk_IN", M_Account_Code_Fk).Direction = ParameterDirection.Input;
                _with1.Add("Created_By_Fk_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = "COST_ELEMENT_MST_TBL_Ins";
                if (objWS.ExecuteCommands() == true)
                {
                    return intPkVal;
                }
                else
                {
                    return -1;
                }
                //Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Insert Function"

        #region "Update Function"

        /// <summary>
        /// Updates this instance.
        /// </summary>
        /// <returns></returns>
        public int Update()
        {
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with2 = objWS.MyCommand.Parameters;
                _with2.Add("Cost_Element_Mst_Pk_IN", M_Cost_Element_Mst_Pk).Direction = ParameterDirection.Input;
                _with2.Add("Cost_Element_Id_IN", M_Cost_Element_Id).Direction = ParameterDirection.Input;
                _with2.Add("Cost_Element_Name_IN", M_Cost_Element_Name).Direction = ParameterDirection.Input;
                _with2.Add("Account_Code_Fk_IN", M_Account_Code_Fk).Direction = ParameterDirection.Input;
                _with2.Add("Last_Modified_By_Fk_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("Version_No_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = "COST_ELEMENT_MST_TBL_UPD";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
                //Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Update Function"

        #region "Delete Function"

        /// <summary>
        /// Deletes this instance.
        /// </summary>
        /// <returns></returns>
        public int Delete()
        {
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with3 = objWS.MyCommand.Parameters;
                _with3.Add("Cost_Element_Mst_Pk_IN", M_Cost_Element_Mst_Pk).Direction = ParameterDirection.Input;
                _with3.Add("Version_No_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = "COST_ELEMENT_MST_TBL_DEL";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
                //Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Delete Function"

        #region "Fetch All"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="Cost_ElementPK">The cost_ element pk.</param>
        /// <param name="Cost_ElementID">The cost_ element identifier.</param>
        /// <param name="Cost_ElementName">Name of the cost_ element.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="intBusType">Type of the int bus.</param>
        /// <param name="intUser">The int user.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int16 Cost_ElementPK = 0, string Cost_ElementID = "", string Cost_ElementName = "", string SearchType = "", string strColumnName = "", int ActiveFlag = 1, Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, int intBusType = 0,
        int intUser = 0, Int32 flag = 0)
        {
            //two more parameters are added by soman,
            //to search for perticular businessType
            // Optional ByVal Cost_ElementGroup_Fk As Int32 = 0, _
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (Cost_ElementPK > 0)
            {
                strCondition = strCondition + " AND Cost_Element_MST_PK=" + Cost_ElementPK;
            }
            if (Cost_ElementID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition = strCondition + " AND UPPER(Cost_Element_ID) LIKE '" + Cost_ElementID.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition = strCondition + " AND UPPER(Cost_Element_ID) LIKE '%" + Cost_ElementID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition = strCondition + " AND UPPER(Cost_Element_ID) LIKE '%" + Cost_ElementID.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (Cost_ElementName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition = strCondition + " AND UPPER(Cost_Element_NAME) LIKE '" + Cost_ElementName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition = strCondition + " AND UPPER(Cost_Element_NAME) LIKE '%" + Cost_ElementName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition = strCondition + " AND UPPER(Cost_Element_NAME) LIKE '%" + Cost_ElementName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            //Added by soman to incorporate the BusinessType.
            if (intBusType == 3 & intUser == 3)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,2,3) ";
            }
            else if (intBusType == 3 & intUser == 2)
            {
                strCondition += " AND BUSINESS_TYPE IN (2,3) ";
            }
            else if (intBusType == 3 & intUser == 1)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,3) ";
            }
            else
            {
                strCondition += " AND BUSINESS_TYPE = " + intBusType + " ";
            }
            //1-->Air
            //2-->Sea
            //3-->Both
            //------------------------------------------------------
            if (ActiveFlag == 1)
            {
                strCondition = strCondition + " AND COST.ACTIVE_FLAG= 1";
            }
            strSQL = "SELECT Count(*) from Cost_Element_MST_TBL COST where 1=1 ";
            strSQL += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
            strSQL = " select * from ( ";
            strSQL += "SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL += "(SELECT ";
            strSQL = strSQL + "Cost_Element_MST_PK, ";
            strSQL = strSQL + "COST.Active_Flag, ";
            strSQL = strSQL + "Cost_Element_ID, ";
            strSQL = strSQL + "Cost_Element_NAME, ";
            strSQL = strSQL + "  COST.APPLICABLE_ON,";
            strSQL = strSQL + " DECODE(  COST.APPLICABLE_ON,'0','','1','BOF','2','Other') APPLICABLE_ON1,";
            strSQL = strSQL + "   COST.CHARGE_BASIS,";
            strSQL = strSQL + " DECODE(  COST.CHARGE_BASIS,'0','','1','%','2','Flat rate') Charge_basis1,";
            strSQL = strSQL + " NVL(  COST.BY_DEFAULT,0) BY_DEFAULT , ";
            strSQL = strSQL + " NVL(  COST.FIXED,0) FIXED , ";
            strSQL = strSQL + " UOM_MST_FK,";
            strSQL = strSQL + " DIMENTION_ID,";
            strSQL = strSQL + " COST.COST_TO,";
            strSQL = strSQL + " DECODE(COST.COST_TO,'0','','1','SELF','2','LINE')COST_TO1,";
            strSQL = strSQL + " NVL(  COST.VAT_APPLICABLE,0) VAT_APPLICABLE , ";
            strSQL = strSQL + "   COST.VAT_BASIS,";
            strSQL = strSQL + " DECODE(  COST.VAT_BASIS,'0','','1','%','2','Flat rate') VAT_BASIS1,";
            strSQL = strSQL + " VMT.VENDOR_TYPE_PK,";
            strSQL = strSQL + " VMT.VENDOR_TYPE_ID,";
            strSQL += " cost.BUSINESS_TYPE, ";
            strSQL += " cost.VERSION_NO";
            strSQL = strSQL + " FROM Cost_Element_MST_TBL cost , DIMENTION_UNIT_MST_TBL dm,VENDOR_TYPE_MST_TBL VMT ";
            strSQL = strSQL + " WHERE 1=1 ";
            strSQL = strSQL + " AND cost.UOM_MST_FK= dm.DIMENTION_UNIT_MST_PK(+) ";
            strSQL = strSQL + " AND COST.VENDOR_TYPE_MST_FK = VMT.VENDOR_TYPE_PK(+)";
            strSQL += strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += " )q )WHERE SR_NO  Between " + start + " and " + last;
            try
            {
                return objWF.GetDataSet(strSQL);
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Fetch All"

        #region "Fetch Cost Element Function"

        /// <summary>
        /// Fetches the cost element.
        /// </summary>
        /// <param name="CostElementPK">The cost element pk.</param>
        /// <param name="CostElementID">The cost element identifier.</param>
        /// <param name="CostElementName">Name of the cost element.</param>
        /// <returns></returns>
        public DataSet FetchCostElement(Int16 CostElementPK = 0, string CostElementID = "", string CostElementName = "")
        {
            string strSQL = null;
            strSQL = strSQL + " select ";
            strSQL = strSQL + " ' ' Cost_Element_ID,";
            strSQL = strSQL + " 0 Active_Flag,";
            strSQL = strSQL + " ' ' Cost_Element_Name,";
            strSQL = strSQL + " 0 Cost_Element_mst_pk ";
            strSQL = strSQL + " from ";
            strSQL = strSQL + " DUAL ";
            strSQL = strSQL + " UNION";
            strSQL = strSQL + " select ";
            strSQL = strSQL + " Cost_Element_id,";
            strSQL = strSQL + " Active_Flag,";
            strSQL = strSQL + " Cost_Element_name,";
            strSQL = strSQL + " Cost_Element_mst_pk ";
            strSQL = strSQL + " from Cost_Element_mst_tbl";
            strSQL = strSQL + " order by Cost_Element_ID ";
            WorkFlow objWF = new WorkFlow();
            OracleDataReader objDR = default(OracleDataReader);
            try
            {
                return objWF.GetDataSet(strSQL);
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Fetch Cost Element Function"

        #region "Fetch Cost Element Group Function"

        /// <summary>
        /// Fetches the cost element GRP.
        /// </summary>
        /// <param name="CostElementGrpPK">The cost element GRP pk.</param>
        /// <param name="CostElementIDGrp">The cost element identifier GRP.</param>
        /// <param name="CostElementNameGrp">The cost element name GRP.</param>
        /// <returns></returns>
        public DataSet FetchCostElementGrp(Int16 CostElementGrpPK = 0, string CostElementIDGrp = "", string CostElementNameGrp = "")
        {
            string strSQL = null;
            strSQL = strSQL + " select ";
            strSQL = strSQL + " ' ' COST_ELEMENT_GROUP_ID,";
            strSQL = strSQL + " ' ' COST_ELEMENT_GROUP_NAME,";
            strSQL = strSQL + " 0 COST_ELEMENT_GROUP_MST_PK ";
            strSQL = strSQL + " from ";
            strSQL = strSQL + " DUAL ";
            strSQL = strSQL + " UNION";
            strSQL = strSQL + " select ";
            strSQL = strSQL + " COST_ELEMENT_GROUP_ID,";
            strSQL = strSQL + " COST_ELEMENT_GROUP_NAME,";
            strSQL = strSQL + " COST_ELEMENT_GROUP_MST_PK ";
            strSQL = strSQL + " from COST_ELEMENT_GROUP_MST_TBL";
            strSQL = strSQL + " order by COST_ELEMENT_GROUP_ID ";
            WorkFlow objWF = new WorkFlow();
            OracleDataReader objDR = default(OracleDataReader);
            try
            {
                return objWF.GetDataSet(strSQL);
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Fetch Cost Element Group Function"

        #region "Fetch Cost Group Function"

        /// <summary>
        /// Fetches the cost GRP.
        /// </summary>
        /// <param name="VendorTypePK">The vendor type pk.</param>
        /// <param name="VendorTypeID">The vendor type identifier.</param>
        /// <param name="VendorTypeName">Name of the vendor type.</param>
        /// <returns></returns>
        public DataSet FetchCostGrp(Int16 VendorTypePK = 0, string VendorTypeID = "", string VendorTypeName = "")
        {
            string strSQL = null;
            strSQL = strSQL + " select ";
            strSQL = strSQL + " ' ' VENDOR_TYPE_ID,";
            strSQL = strSQL + " ' ' VENDOR_TYPE,";
            strSQL = strSQL + " 0 VENDOR_TYPE_PK ";
            strSQL = strSQL + " from ";
            strSQL = strSQL + " DUAL ";
            strSQL = strSQL + " UNION";
            strSQL = strSQL + " select ";
            strSQL = strSQL + " VENDOR_TYPE_ID,";
            strSQL = strSQL + " VENDOR_TYPE,";
            strSQL = strSQL + " VENDOR_TYPE_PK ";
            strSQL = strSQL + " from VENDOR_TYPE_MST_TBL";
            strSQL = strSQL + " order by VENDOR_TYPE_ID ";
            WorkFlow objWF = new WorkFlow();
            OracleDataReader objDR = default(OracleDataReader);
            try
            {
                return objWF.GetDataSet(strSQL);
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Fetch Cost Group Function"

        #region "Fetch Business type"

        /// <summary>
        /// Fetches the type of the business.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchBusinessType()
        {
            string strSQL = null;
            strSQL = "SELECT  B.BUSINESS_TYPE,B.BUSINESS_TYPE_DISPLAY FROM BUSINESS_TYPE_MST_TBL B";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Fetch Business type"

        /// <summary>
        /// Fetches the type of the supplier.
        /// </summary>
        /// <param name="biztype">The biztype.</param>
        /// <returns></returns>
        public object FetchSupplierType(int biztype = 0)
        {
            string strSql = null;
            try
            {
                strSql = strSql + "SELECT ";
                strSql = strSql + "0 VENDOR_TYPE_PK,";
                strSql = strSql + "' ' VENDOR_TYPE_ID";
                strSql = strSql + "FROM dual";
                strSql = strSql + "Union ";
                strSql = strSql + "SELECT ";
                strSql = strSql + "VENDOR_TYPE_PK,";
                strSql = strSql + " VENDOR_TYPE_ID";
                strSql = strSql + " FROM VENDOR_TYPE_MST_TBL";
                strSql = strSql + " WHERE ACTIVE_FLAG = 1";
                if ((biztype == 1))
                {
                    strSql = strSql + " And VENDOR_TYPE_ID not in 'SHIPPINGLINE' ";
                }
                else if ((biztype == 2))
                {
                    strSql = strSql + " And VENDOR_TYPE_ID not in 'AIRLINE' ";
                }
                else
                {
                    strSql = strSql + " ";
                }
                strSql = strSql + " order by VENDOR_TYPE_ID";
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(strSql);
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
    }
}