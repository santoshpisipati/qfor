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

using Newtonsoft.Json;
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
    public class clsFREIGHT_ELEMENT_MST_TBL : CommonFeatures
	{
        #region "List of Members of the Class"
        /// <summary>
        /// The m_ freight_ element_ MST_ pk
        /// </summary>
        private Int64 M_Freight_Element_Mst_Pk;
        /// <summary>
        /// The m_ freight_ element_ identifier
        /// </summary>
        private string M_Freight_Element_Id;
        /// <summary>
        /// The m_ freight_ element_ name
        /// </summary>
        private string M_Freight_Element_Name;
        /// <summary>
        /// The m_ commisionable
        /// </summary>
        private Int16 M_Commisionable;
        /// <summary>
        /// The m_ local_ charges
        /// </summary>
        private Int16 M_Local_Charges;
        /// <summary>
        /// The m_ income_ to_ principle
        /// </summary>
        private Int16 M_Income_To_Principle;
        /// <summary>
        /// The m_ income_ to_ agency
        /// </summary>
        private Int16 M_Income_To_Agency;
        /// <summary>
        /// The m_ surcharge_ type
        /// </summary>
        private string M_Surcharge_Type;
        /// <summary>
        /// The m_ ite m_ type
        /// </summary>
        private Int16 M_ITEM_TYPE;
        /// <summary>
        /// The m_ charg e_ basis
        /// </summary>
        private Int16 M_CHARGE_BASIS;

        /// <summary>
        /// The m_ printin g_ priority
        /// </summary>
        private Int16 M_PRINTING_PRIORITY;

        #endregion

        #region "List of Properties"
        /// <summary>
        /// Gets or sets the freight_ element_ MST_ pk.
        /// </summary>
        /// <value>
        /// The freight_ element_ MST_ pk.
        /// </value>
        public Int64 Freight_Element_Mst_Pk {
			get { return M_Freight_Element_Mst_Pk; }
			set { M_Freight_Element_Mst_Pk = value; }
		}

        /// <summary>
        /// Gets or sets the freight_ element_ identifier.
        /// </summary>
        /// <value>
        /// The freight_ element_ identifier.
        /// </value>
        public string Freight_Element_Id {
			get { return M_Freight_Element_Id; }
			set { M_Freight_Element_Id = value; }
		}

        /// <summary>
        /// Gets or sets the name of the freight_ element_.
        /// </summary>
        /// <value>
        /// The name of the freight_ element_.
        /// </value>
        public string Freight_Element_Name {
			get { return M_Freight_Element_Name; }
			set { M_Freight_Element_Name = value; }
		}

        /// <summary>
        /// Gets or sets the commisionable.
        /// </summary>
        /// <value>
        /// The commisionable.
        /// </value>
        public Int16 Commisionable {
			get { return M_Commisionable; }
			set { M_Commisionable = value; }
		}

        /// <summary>
        /// Gets or sets the local_ charges.
        /// </summary>
        /// <value>
        /// The local_ charges.
        /// </value>
        public Int16 Local_Charges {
			get { return M_Local_Charges; }
			set { M_Local_Charges = value; }
		}

        /// <summary>
        /// Gets or sets the income_ to_ principle.
        /// </summary>
        /// <value>
        /// The income_ to_ principle.
        /// </value>
        public Int16 Income_To_Principle {
			get { return M_Income_To_Principle; }
			set { M_Income_To_Principle = value; }
		}

        /// <summary>
        /// Gets or sets the income_ to_ agency.
        /// </summary>
        /// <value>
        /// The income_ to_ agency.
        /// </value>
        public Int16 Income_To_Agency {
			get { return M_Income_To_Agency; }
			set { M_Income_To_Agency = value; }
		}

        /// <summary>
        /// Gets or sets the type of the surcharge_.
        /// </summary>
        /// <value>
        /// The type of the surcharge_.
        /// </value>
        public string Surcharge_Type {
			get { return M_Surcharge_Type; }
			set { M_Surcharge_Type = value; }
		}
        /// <summary>
        /// Gets or sets the type of the ite m_.
        /// </summary>
        /// <value>
        /// The type of the ite m_.
        /// </value>
        public Int16 ITEM_TYPE {
			get { return M_ITEM_TYPE; }
			set { M_ITEM_TYPE = value; }
		}
        /// <summary>
        /// Gets or sets the charg e_ basis.
        /// </summary>
        /// <value>
        /// The charg e_ basis.
        /// </value>
        public Int16 CHARGE_BASIS {
			get { return M_CHARGE_BASIS; }
			set { M_CHARGE_BASIS = value; }
		}
        /// <summary>
        /// Gets or sets the printin g_ priority.
        /// </summary>
        /// <value>
        /// The printin g_ priority.
        /// </value>
        public Int16 PRINTING_PRIORITY {
			get { return M_PRINTING_PRIORITY; }
			set { M_PRINTING_PRIORITY = value; }
		}

        #endregion

        #region "Insert Function"
        /// <summary>
        /// Inserts this instance.
        /// </summary>
        /// <returns></returns>
        public int Insert()
		{
			WorkFlow objWS = new WorkFlow();
			Int32 intPkVal = default(Int32);
			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;
				var _with1 = objWS.MyCommand.Parameters;
				_with1.Add("Freight_Element_Id_IN", M_Freight_Element_Id).Direction = ParameterDirection.Input;
				_with1.Add("Freight_Element_Name_IN", M_Freight_Element_Name).Direction = ParameterDirection.Input;
				_with1.Add("Commisionable_IN", M_Commisionable).Direction = ParameterDirection.Input;
				_with1.Add("Local_Charges_IN", M_Local_Charges).Direction = ParameterDirection.Input;
				_with1.Add("Income_To_Principle_IN", M_Income_To_Principle).Direction = ParameterDirection.Input;
				_with1.Add("Income_To_Agency_IN", M_Income_To_Agency).Direction = ParameterDirection.Input;
				_with1.Add("Surcharge_Type_IN", M_Surcharge_Type).Direction = ParameterDirection.Input;
				_with1.Add("CHARGE_BASIS_IN", M_CHARGE_BASIS).Direction = ParameterDirection.Input;
				_with1.Add("ITEM_TYPE_IN", M_ITEM_TYPE).Direction = ParameterDirection.Input;
				_with1.Add("PRINTING_PRIORITY_IN", M_PRINTING_PRIORITY).Direction = ParameterDirection.Input;
				_with1.Add("Created_By_Fk_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
				objWS.MyCommand.CommandText = objWS.MyUserName + ".FREIGHT_ELEMENT_MST_TBL_PKG.FREIGHT_ELEMENT_MST_TBL_Ins";
				if (objWS.ExecuteCommands() == true) {
					return intPkVal;
				} else {
					return -1;
				}
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Update Function"
        /// <summary>
        /// Updates this instance.
        /// </summary>
        /// <returns></returns>
        public int Update()
		{
			WorkFlow objWS = new WorkFlow();
			Int32 intPkVal = default(Int32);
			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;
				var _with2 = objWS.MyCommand.Parameters;
				_with2.Add("Freight_Element_Mst_Pk_IN", M_Freight_Element_Mst_Pk).Direction = ParameterDirection.Input;
				_with2.Add("Freight_Element_Id_IN", M_Freight_Element_Id).Direction = ParameterDirection.Input;
				_with2.Add("Freight_Element_Name_IN", M_Freight_Element_Name).Direction = ParameterDirection.Input;
				_with2.Add("Commisionable_IN", M_Commisionable).Direction = ParameterDirection.Input;
				_with2.Add("Local_Charges_IN", M_Local_Charges).Direction = ParameterDirection.Input;
				_with2.Add("Income_To_Principle_IN", M_Income_To_Principle).Direction = ParameterDirection.Input;
				_with2.Add("Income_To_Agency_IN", M_Income_To_Agency).Direction = ParameterDirection.Input;
				_with2.Add("Surcharge_Type_IN", M_Surcharge_Type).Direction = ParameterDirection.Input;
				_with2.Add("Surcharge_Type_IN", M_Surcharge_Type).Direction = ParameterDirection.Input;
				_with2.Add("CHARGE_BASIS_IN", M_CHARGE_BASIS).Direction = ParameterDirection.Input;
				_with2.Add("ITEM_TYPE_IN", M_ITEM_TYPE).Direction = ParameterDirection.Input;
				_with2.Add("PRINTING_PRIORITY_IN", M_PRINTING_PRIORITY).Direction = ParameterDirection.Input;
				_with2.Add("Last_Modified_By_Fk_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				_with2.Add("Version_No_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
				_with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Output;
				objWS.MyCommand.CommandText = objWS.MyUserName + ".FREIGHT_ELEMENT_MST_TBL_PKG.FREIGHT_ELEMENT_MST_TBL_UPD";
				if (objWS.ExecuteCommands() == true) {
					return 1;
				} else {
					return -1;
				}
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Delete Function"
        /// <summary>
        /// Deletes this instance.
        /// </summary>
        /// <returns></returns>
        public int Delete()
		{
			WorkFlow objWS = new WorkFlow();
			Int32 intPkVal = default(Int32);
			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;
				var _with3 = objWS.MyCommand.Parameters;
				_with3.Add("Freight_Element_Mst_Pk_IN", M_Freight_Element_Mst_Pk).Direction = ParameterDirection.Input;
				_with3.Add("Version_No_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
				_with3.Add("RETURN_VALUE", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Output;
				objWS.MyCommand.CommandText = objWS.MyUserName + ".FREIGHT_ELEMENT_MST_TBL_PKG.FREIGHT_ELEMENT_MST_TBL_DEL";
				if (objWS.ExecuteCommands() == true) {
					return 1;
				} else {
					return -1;
				}
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Fetch All"
        /// <summary>
        /// Fetches the unit.
        /// </summary>
        /// <returns></returns>
        public object FetchUnit()
		{
			string strSql = null;
			try {
				strSql = strSql + "SELECT ";
				strSql = strSql + "0 DIMENTION_UNIT_MST_PK,";
				strSql = strSql + "' ' DIMENTION_ID";
				strSql = strSql + "FROM dual";
				strSql = strSql + "Union ";
				strSql = strSql + "SELECT ";
				strSql = strSql + "DIMENTION_UNIT_MST_PK,";
				strSql = strSql + " DIMENTION_ID";
				strSql = strSql + " FROM DIMENTION_UNIT_MST_TBL";
				strSql = strSql + " WHERE ACTIVE = 1";
				strSql = strSql + " order by DIMENTION_ID";

				WorkFlow objWF = new WorkFlow();

				return objWF.GetDataSet(strSql);
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetches the service.
        /// </summary>
        /// <returns></returns>
        public object FetchService()
		{
			string strSql = null;
			try {
				strSql = strSql + "SELECT ";
				strSql = strSql + "0 SERVICE_MST_PK,";
				strSql = strSql + "' ' SERVICE_NAME";
				strSql = strSql + "FROM dual";
				strSql = strSql + "Union ";
				strSql = strSql + "SELECT ";
				strSql = strSql + "SERVICE_MST_PK,";
				strSql = strSql + "SERVICE_NAME";
				strSql = strSql + "FROM SERVICES_MST_TBL";
				strSql = strSql + " ORDER BY SERVICE_NAME";

				WorkFlow objWF = new WorkFlow();

				return objWF.GetDataSet(strSql);
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetches the freight.
        /// </summary>
        /// <returns></returns>
        public object FetchFreight()
		{
			string strSql = null;
			try {
				strSql = strSql + "SELECT ";
				strSql = strSql + "0 FREIGHT_ELEMENT_MST_PK,";
				strSql = strSql + "' ' FREIGHT_ELEMENT_ID";
				strSql = strSql + "FROM dual";
				strSql = strSql + "Union ";
				strSql = strSql + "SELECT ";
				strSql = strSql + "FREIGHT_ELEMENT_MST_PK,";
				strSql = strSql + "FREIGHT_ELEMENT_ID";
				strSql = strSql + "FROM FREIGHT_ELEMENT_MST_TBL";
				strSql = strSql + "WHERE ACTIVE_FLAG = 1";
				strSql = strSql + " order by Freight_Element_ID";

				WorkFlow objWF = new WorkFlow();

				return objWF.GetDataSet(strSql);
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="Freight_ElementPK">The freight_ element pk.</param>
        /// <param name="Freight_ElementID">The freight_ element identifier.</param>
        /// <param name="Freight_ElementName">Name of the freight_ element.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="intBusType">Type of the int bus.</param>
        /// <param name="intUser">The int user.</param>
        /// <param name="intFreightType">Type of the int freight.</param>
        /// <param name="intCrDrType">Type of the int cr dr.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int16 Freight_ElementPK = 0, string Freight_ElementID = "", string Freight_ElementName = "", string SearchType = "", string strColumnName = "FREIGHT_ELEMENT_ID", int ActiveFlag = 1, Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, int intBusType = 0,
		int intUser = 0, int intFreightType = 0, int intCrDrType = 0, Int32 flag = 0)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();

			if (flag == 0) {
				strCondition += " AND 1=2";
			}
			if (Freight_ElementPK > 0) {
				strCondition = strCondition + " AND Freight_Element_MST_PK=" + Freight_ElementPK;
			}
			if (Freight_ElementID.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition = strCondition + " AND UPPER(Freight_Element_ID) LIKE '" + Freight_ElementID.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition = strCondition + " AND UPPER(Freight_Element_ID) LIKE '%" + Freight_ElementID.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition = strCondition + " AND UPPER(Freight_Element_ID) LIKE '%" + Freight_ElementID.ToUpper().Replace("'", "''") + "%'" ;
				}
			}
			if (Freight_ElementName.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition = strCondition + " AND UPPER(Freight_Element_NAME) LIKE '" + Freight_ElementName.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition = strCondition + " AND UPPER(Freight_Element_NAME) LIKE '%" + Freight_ElementName.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition = strCondition + " AND UPPER(Freight_Element_NAME) LIKE '%" + Freight_ElementName.ToUpper().Replace("'", "''") + "%'" ;
				}
			}

			//Added by soman to incorporate the BusinessType.
			if (intBusType == 3 & intUser == 3) {
				strCondition += " AND BUSINESS_TYPE IN (1,2,3) ";
			} else if (intBusType == 3 & intUser == 2) {
				strCondition += " AND BUSINESS_TYPE IN (2,3) ";
			} else if (intBusType == 3 & intUser == 1) {
				strCondition += " AND BUSINESS_TYPE IN (1,3) ";
			} else {
				strCondition += " AND BUSINESS_TYPE = " + intBusType + " ";
			}
			//1-->Air
			//2-->Sea
			//3-->Both
			//------------------------------------------------------
			if (intFreightType != 0) {
				strCondition += " AND FREIGHT_TYPE = " + intFreightType + " ";
			}
			strCondition += " AND CREDIT = " + intCrDrType + " ";
			if (ActiveFlag == 1) {
				strCondition = strCondition + " AND ACTIVE_FLAG= 1";
			}
			strSQL = "SELECT Count(*) from FREIGHT_ELEMENT_MST_TBL where 1=1";
			strSQL += strCondition;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;

			//If CInt(SortCol) > 0 Then
			//    strCondition = strCondition & " order by " & CInt(SortCol)
			//End If

			strSQL = "SELECT * FROM (";
			strSQL += "SELECT ROWNUM SR_NO, q.* FROM ";
			strSQL += "(SELECT ";
			strSQL = strSQL + " fr.FREIGHT_ELEMENT_MST_PK,";
			strSQL = strSQL + " NVL(fr.ACTIVE_FLAG,0) ACTIVE_FLAG , ";
			strSQL = strSQL + " fr.FREIGHT_ELEMENT_ID,";
			strSQL = strSQL + " fr.FREIGHT_ELEMENT_NAME,";
			strSQL = strSQL + " fr.FREIGHT_TYPE FREIGHT_TYPE_FK,";
			//strSQL = strSQL & vbCrLf & " DECODE(fr.FREIGHT_TYPE, '1', 'Freight', '2', 'Operator', '3', 'Trade', '4', '') FREIGHT_TYPE, "
			strSQL = strSQL + " DECODE(fr.FREIGHT_TYPE,'0', 'All', '1', 'Freight', '2', 'Operator', '3', 'Trade', '4', '') FREIGHT_TYPE, ";
			strSQL = strSQL + " fr.CHARGE_BASIS CHARGE_BASIS_FK,";
			//strSQL = strSQL & vbCrLf & " DECODE(fr.CHARGE_BASIS,'0','','1','%','2','Flat rate','3','Kgs') CHARGE_BASIS,"
			strSQL = strSQL + " DECODE(fr.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,";
			strSQL = strSQL + " DECODE(fr.Basic_Chrage,'0','','1','YES','2','NO')BASIC_CHRAGE,";
			///Added By Koteshwari
			strSQL = strSQL + " fr.BASIS_VALUE BASIS_VALUE, ";
			//Added By Snigdha to display formula
			strSQL = strSQL + " UOM_MST_FK UOM_FK, ";
			//'changed strSQL = strSQL & vbCrLf & " DECODE(fr.CREDIT,'0','Cr','1','Dr') CREDIT," 'added by prakash chandra on 24/06/2008
			strSQL = strSQL + " '' FORMULA_VALUE, ";
			strSQL = strSQL + " fr.APPLICABLE_ON APPLICABLE_FK,";
			//strSQL = strSQL & vbCrLf & " DECODE(fr.APPLICABLE_ON,'0','','1','BOF','2','PHL','3','PHD','4','MDO','5','OTH') APPLICABLE,"
			strSQL = strSQL + " ''APPLICABLE,";
			///Modified By Koteshwari on 11/3/2011
			strSQL = strSQL + " DECODE(fr.CREDIT,'0','Cr','1','Dr')CREDIT, DECODE(FR.LOCAL_CHARGE,1,'true',0,'false')LOCAL_CHARGE,";
			//'changed strSQL = strSQL & vbCrLf & " NVL(fr.BY_DEFAULT,0) BY_DEFAULT , "
			strSQL = strSQL + " NVL(fr.BY_DEFAULT,0) BY_DEFAULT , ";
			//'changed strSQL = strSQL & vbCrLf & " UOM_MST_FK UOM_FK, "
			strSQL = strSQL + " dm.DIMENTION_ID UOM_ID, ";
			strSQL = strSQL + " dm.DIMENTION_ID UOM_ID, ";
			strSQL = strSQL + " fr.BUSINESS_TYPE, ";
			strSQL = strSQL + " fr.VERSION_NO, ";
			strSQL = strSQL + " fr.Preference Preference, ";
			strSQL = strSQL + " to_char(fr.FORMULA) FORMULA, ";
			//Added y Snigdha to keep formula string in hidden field.
			//'added for the validation 
			strSQL = strSQL + " (SELECT COUNT(*)  FROM FREIGHT_ELEMENT_MST_TBL FEM,  FREIGHT_ELEMENT_TRN_TBL FE ";
			strSQL = strSQL + "  WHERE FEM.FREIGHT_ELEMENT_MST_PK =FE.FREIGHT_ELEMENT_FK AND FE.CHKFLAG = 1  AND FEM.FREIGHT_ELEMENT_MST_PK =FR.FREIGHT_ELEMENT_MST_PK) TRNPKCNT ";

			//'added
			strSQL = strSQL + " FROM ";
			strSQL = strSQL + " FREIGHT_ELEMENT_MST_TBL fr, DIMENTION_UNIT_MST_TBL dm";
			strSQL = strSQL + " WHERE 1=1";
			strSQL = strSQL + " and";
			strSQL = strSQL + " fr.UOM_MST_FK= dm.DIMENTION_UNIT_MST_PK(+)";

			strSQL += strCondition;

			if (!strColumnName.Equals("SR_NO")) {
				strSQL += "order by " + strColumnName;
			}

			if (!blnSortAscending & !strColumnName.Equals("SR_NO")) {
				strSQL += " DESC";
			}

			strSQL += ")q ) WHERE SR_NO  Between " + start + " and " + last;

			//strSQL &= vbCrLf & " order by Freight_Element_ID "
			try {
				return objWF.GetDataSet(strSQL);
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion



        #region "currency "
        /// <summary>
        /// Currencies this instance.
        /// </summary>
        /// <returns></returns>
        public DataSet currency()
		{
			string strSQL = null;
			strSQL = "SELECT ";
			strSQL = strSQL + " currency_mst_pk,";
			strSQL = strSQL + " currency_id";
			strSQL = strSQL + " from";
			strSQL = strSQL + " currency_type_mst_tbl";
			strSQL = strSQL + " where";
			strSQL = strSQL + " active_flag = 1";
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL);
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch KIND"
        /// <summary>
        /// Fetches the kind.
        /// </summary>
        /// <param name="ContainerKindPK">The container kind pk.</param>
        /// <param name="ContainerKind">Kind of the container.</param>
        /// <returns></returns>
        public DataSet FetchKind(Int16 ContainerKindPK = 0, string ContainerKind = "")
		{

			string strSQL = null;
			strSQL = "select c.surcharge_type_mst_pk, c.surcharge_type from surcharge_type_mst_tbl c";
			strSQL = strSQL + " order by c.surcharge_type";

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL);
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Save Function"

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
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			int intPKVal = 0;
			string FrtPks = "";
			long lngI = 0;
            int RecAfct = default(int);
			ArrayList schDtls = null;
			bool errGen = false;
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
			arrMessage.Clear();


			try {
				DataTable dttbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;
				dttbl = M_DataSet.Tables[0];
				if ((M_DataSet != null)) {
					for (i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++) {
						if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["CHARGE_PK"].ToString())) {
							M_DataSet.Tables[0].Rows[i]["CHARGE_PK"] = 0;
						}
						if (Convert.ToInt32(M_DataSet.Tables[0].Rows[i]["CHARGE_PK"]) <= 0) {
							var _with4 = insCommand;
							_with4.Connection = objWK.MyConnection;
							_with4.CommandType = CommandType.StoredProcedure;
							_with4.CommandText = objWK.MyUserName + ".FREIGHT_ELEMENT_MST_TBL_PKG.FREIGHT_ELEMENT_MST_TBL_INS";
							_with4.Parameters.Clear();
							insCommand.Parameters.Add("ACTIVE_FLAG_IN", M_DataSet.Tables[0].Rows[i]["ACTIVE_FLAG"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("CHARGE_ID_IN", M_DataSet.Tables[0].Rows[i]["CHARGE_ID"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("CHARGE_NAME_IN", M_DataSet.Tables[0].Rows[i]["CHARGE_NAME"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("CHARGE_TYPE_IN", M_DataSet.Tables[0].Rows[i]["CHARGE_TYPE_FK"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("APPLICABLE_FK_IN", M_DataSet.Tables[0].Rows[i]["APPLICABLE_FK"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("FIN_CODE_IN", M_DataSet.Tables[0].Rows[i]["FIN_CODE"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("COA_CODE_IN", M_DataSet.Tables[0].Rows[i]["COA_CODE"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("CHARGE_BASIS_FK_IN", M_DataSet.Tables[0].Rows[i]["CHARGE_BASIS_FK"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("CREDIT_IN", M_DataSet.Tables[0].Rows[i]["CREDIT"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("BY_DEFAULT_IN", M_DataSet.Tables[0].Rows[i]["BYDEFAULT"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("SERVICE_IN", M_DataSet.Tables[0].Rows[i]["SERVICE_PK"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("BUSINESS_TYPE_IN", M_DataSet.Tables[0].Rows[i]["BIZ_TYPE"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("PREFERENCE_IN", M_DataSet.Tables[0].Rows[i]["PREFERENCE"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("PAYMENT_TYPE_FK_IN", M_DataSet.Tables[0].Rows[i]["PAYMENT_TYPE_FK"]).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
							insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 100, "FREIGHT_ELEMENT_MST_PK").Direction = ParameterDirection.Output;
							insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
							var _with5 = objWK.MyDataAdapter;
							_with5.InsertCommand = insCommand;
							_with5.InsertCommand.Transaction = TRAN;
							_with5.InsertCommand.ExecuteNonQuery();
							intPKVal = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
							if (string.IsNullOrEmpty(FrtPks)) {
								FrtPks = Convert.ToString(intPKVal);
							} else {
								FrtPks = FrtPks + "," + Convert.ToString(intPKVal);
							}
						} else {
							var _with6 = updCommand;
							_with6.Connection = objWK.MyConnection;
							_with6.CommandType = CommandType.StoredProcedure;
							_with6.CommandText = objWK.MyUserName + ".FREIGHT_ELEMENT_MST_TBL_PKG.FREIGHT_ELEMENT_MST_TBL_UPD";
							_with6.Parameters.Clear();
							updCommand.Parameters.Add("CHARGE_PK_IN", M_DataSet.Tables[0].Rows[i]["CHARGE_PK"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("ACTIVE_FLAG_IN", M_DataSet.Tables[0].Rows[i]["ACTIVE_FLAG"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("CHARGE_ID_IN", M_DataSet.Tables[0].Rows[i]["CHARGE_ID"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("CHARGE_NAME_IN", M_DataSet.Tables[0].Rows[i]["CHARGE_NAME"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("CHARGE_TYPE_IN", M_DataSet.Tables[0].Rows[i]["CHARGE_TYPE_FK"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("APPLICABLE_FK_IN", M_DataSet.Tables[0].Rows[i]["APPLICABLE_FK"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("FIN_CODE_IN", M_DataSet.Tables[0].Rows[i]["FIN_CODE"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("COA_CODE_IN", M_DataSet.Tables[0].Rows[i]["COA_CODE"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("CHARGE_BASIS_FK_IN", M_DataSet.Tables[0].Rows[i]["CHARGE_BASIS_FK"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("CREDIT_IN", M_DataSet.Tables[0].Rows[i]["CREDIT"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("BY_DEFAULT_IN", M_DataSet.Tables[0].Rows[i]["BYDEFAULT"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("SERVICE_IN", M_DataSet.Tables[0].Rows[i]["SERVICE_PK"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("BUSINESS_TYPE_IN", M_DataSet.Tables[0].Rows[i]["BIZ_TYPE"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("PREFERENCE_IN", M_DataSet.Tables[0].Rows[i]["PREFERENCE"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("PAYMENT_TYPE_FK_IN", M_DataSet.Tables[0].Rows[i]["PAYMENT_TYPE_FK"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("VERSION_NO_IN", M_DataSet.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
							updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
							updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

							var _with7 = objWK.MyDataAdapter;
							_with7.UpdateCommand = updCommand;
							_with7.UpdateCommand.Transaction = TRAN;
							_with7.UpdateCommand.ExecuteNonQuery();
							if (string.IsNullOrEmpty(FrtPks)) {
								FrtPks = Convert.ToString(M_DataSet.Tables[0].Rows[i]["CHARGE_PK"]);
							} else {
								FrtPks = FrtPks + "," + Convert.ToString(M_DataSet.Tables[0].Rows[i]["CHARGE_PK"]);
							}
						}
					}
				}

				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					TRAN.Commit();
                    //Push to financial system if realtime is selected
                    if (objSch.GetSchedulerPushType() == true)
                    {
                    }
					if (Import == true) {
						arrMessage.Add("Data Imported Successfully");
					} else {
						arrMessage.Add("All Data Saved Successfully");
					}
					return arrMessage;
				}


			} catch (OracleException oraexp) {
				TRAN.Rollback();
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}


			
		}



        #endregion

        /// <summary>
        /// Saves the freight.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="M_CREATED_BY_FK">The m_ create d_ b y_ fk.</param>
        /// <param name="M_Last_Modified_By_FK">The m_ last_ modified_ by_ fk.</param>
        /// <returns></returns>
        /// Added By Koteshwari on 16/3/2011
        #region "Save Freight Element"
        public ArrayList SaveFreight(DataSet M_DataSet, int M_CREATED_BY_FK = 0, int M_Last_Modified_By_FK = 0)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			int i = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			arrMessage.Clear();
			try {
				for (i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++) {
					//Checking whether Chk flag is true
					if (M_DataSet.Tables[0].Rows[i][6] == "1") {
						//Checking whether Frt_trn_pk is available.If not insert else update
						if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i][2].ToString())) {
							var _with8 = insCommand;
							insCommand.Parameters.Clear();
							_with8.Connection = objWK.MyConnection;
							_with8.CommandType = CommandType.StoredProcedure;
							var _with9 = _with8.Parameters;
							_with9.Clear();
							_with9.Add("FREIGHT_ELEMENT_ID_IN", M_DataSet.Tables[0].Rows[i][4]).Direction = ParameterDirection.Input;
							_with9.Add("FREIGHT_ELEMENT_NAME_IN", M_DataSet.Tables[0].Rows[i][5]).Direction = ParameterDirection.Input;
							_with9.Add("CHKFLAG_IN", M_DataSet.Tables[0].Rows[i][6]).Direction = ParameterDirection.Input;
							if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i][3].ToString())) {
								_with9.Add("FREIGHT_ELEMENT_FK_IN", M_DataSet.Tables[0].Rows[i][3]).Direction = ParameterDirection.Input;
							} else {
								_with9.Add("FREIGHT_ELEMENT_FK_IN", "").Direction = ParameterDirection.Input;
							}
							//.Add("FREIGHT_ELEMENT_FK_IN", M_DataSet.Tables(0).Rows(i).Item(3)).Direction = ParameterDirection.Input
							_with9.Add("FREIGHT_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[i][1]).Direction = ParameterDirection.Input;
							_with9.Add("LAST_MODIFIED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input;
							_with9.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
							_with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_TRN_PK").Direction = ParameterDirection.Output;
							insCommand.CommandText = objWK.MyUserName + ".FREIGHT_ELEMENT_TRN_TBL_PKG.FREIGHT_ELEMENT_TRN_TBL_INS";
							insCommand.Transaction = TRAN;
							insCommand.ExecuteNonQuery();
						} else {
							var _with10 = updCommand;
							updCommand.Parameters.Clear();
							_with10.Connection = objWK.MyConnection;
							_with10.CommandType = CommandType.StoredProcedure;
							var _with11 = _with10.Parameters;
							_with11.Clear();
							_with11.Add("FREIGHT_ELEMENT_TRN_PK_IN", M_DataSet.Tables[0].Rows[i][2]).Direction = ParameterDirection.Input;
							_with11.Add("FREIGHT_ELEMENT_ID_IN", M_DataSet.Tables[0].Rows[i][4]).Direction = ParameterDirection.Input;
							_with11.Add("FREIGHT_ELEMENT_NAME_IN", M_DataSet.Tables[0].Rows[i][5]).Direction = ParameterDirection.Input;
							_with11.Add("CHKFLAG_IN", M_DataSet.Tables[0].Rows[i][6]).Direction = ParameterDirection.Input;
							if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i][3].ToString())) {
								_with11.Add("FREIGHT_ELEMENT_FK_IN", M_DataSet.Tables[0].Rows[i][3]).Direction = ParameterDirection.Input;
							} else {
								_with11.Add("FREIGHT_ELEMENT_FK_IN", "").Direction = ParameterDirection.Input;
							}
							// .Add("FREIGHT_ELEMENT_FK_IN", M_DataSet.Tables(0).Rows(i).Item(3)).Direction = ParameterDirection.Input
							_with11.Add("FREIGHT_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[i][1]).Direction = ParameterDirection.Input;
							_with11.Add("LAST_MODIFIED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input;
							_with11.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
							_with11.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
							updCommand.CommandText = objWK.MyUserName + ".FREIGHT_ELEMENT_TRN_TBL_PKG.FREIGHT_ELEMENT_TRN_TBL_UPD";
							updCommand.Transaction = TRAN;
							updCommand.ExecuteNonQuery();
						}
					} else {
						if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i][2].ToString())) {
							var _with12 = updCommand;
							updCommand.Parameters.Clear();
							_with12.Connection = objWK.MyConnection;
							_with12.CommandType = CommandType.StoredProcedure;
							var _with13 = _with12.Parameters;
							_with13.Clear();
							_with13.Add("FREIGHT_ELEMENT_TRN_PK_IN", M_DataSet.Tables[0].Rows[i][2]).Direction = ParameterDirection.Input;
							_with13.Add("FREIGHT_ELEMENT_ID_IN", M_DataSet.Tables[0].Rows[i][4]).Direction = ParameterDirection.Input;
							_with13.Add("FREIGHT_ELEMENT_NAME_IN", M_DataSet.Tables[0].Rows[i][5]).Direction = ParameterDirection.Input;
							_with13.Add("CHKFLAG_IN", M_DataSet.Tables[0].Rows[i][6]).Direction = ParameterDirection.Input;
							if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i][3].ToString())) {
								_with13.Add("FREIGHT_ELEMENT_FK_IN", M_DataSet.Tables[0].Rows[i][3]).Direction = ParameterDirection.Input;
							} else {
								_with13.Add("FREIGHT_ELEMENT_FK_IN", "").Direction = ParameterDirection.Input;
							}
							//.Add("FREIGHT_ELEMENT_FK_IN", M_DataSet.Tables(0).Rows(i).Item(3)).Direction = ParameterDirection.Input
							_with13.Add("FREIGHT_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[i][1]).Direction = ParameterDirection.Input;
							_with13.Add("LAST_MODIFIED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input;
							_with13.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
							_with13.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
							updCommand.CommandText = objWK.MyUserName + ".FREIGHT_ELEMENT_TRN_TBL_PKG.FREIGHT_ELEMENT_TRN_TBL_UPD";
							updCommand.Transaction = TRAN;
							updCommand.ExecuteNonQuery();
						}
					}
				}
				if (arrMessage.Count == 0) {
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				TRAN.Rollback();
				arrMessage.Add(ex.Message);
				return arrMessage;
			} finally {
				objWK.CloseConnection();
			}
            return arrMessage;
		}
        #endregion
        /// <summary>
        /// Fetches the currency.
        /// </summary>
        /// <param name="CurrencyPK">The currency pk.</param>
        /// <param name="CurrencyID">The currency identifier.</param>
        /// <param name="CurrencyName">Name of the currency.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <returns></returns>
        /// End

        #region "Fetch Currency"
        public DataSet FetchCurrency(Int16 CurrencyPK = 0, string CurrencyID = "", string CurrencyName = "", bool ActiveOnly = true)
		{
			string strSQL = null;
			strSQL = "select ' ' CURRENCY_ID,";
			strSQL = strSQL + "' ' CURRENCY_NAME, ";
			strSQL = strSQL + "0 CURRENCY_MST_PK ";
			strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL ";
			strSQL = strSQL + "UNION ";
			strSQL = strSQL + "Select CURRENCY_ID, ";
			strSQL = strSQL + "CURRENCY_NAME,";
			strSQL = strSQL + "CURRENCY_MST_PK ";
			strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL Where 1=1 ";
			if (ActiveOnly) {
				strSQL = strSQL + " And Active_Flag = 1  ";
			}
			strSQL = strSQL + "order by CURRENCY_ID";
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL);
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Business type"
        /// <summary>
        /// Fetches the type of the business.
        /// </summary>
        /// <param name="IsRemoval">The is removal.</param>
        /// <returns></returns>
        public DataSet FetchBusinessType(int IsRemoval = 0)
		{

			string strSQL = null;
			strSQL = "SELECT  B.BUSINESS_TYPE,B.BUSINESS_TYPE_DISPLAY FROM BUSINESS_TYPE_MST_TBL B";
			if (IsRemoval == 1) {
				strSQL = strSQL + " UNION SELECT 4 BUSINESS_TYPE,'Removals' BUSINESS_TYPE_DISPLAY FROM DUAL";
			}
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL);
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        // Added By jitendra 
        #region "Fetch Freight Element Into Parameters"
        /// <summary>
        /// Fetches the freight_ parameters.
        /// </summary>
        /// <returns></returns>
        public string FetchFreight_Parameters()
        {
            string strSql = null;
            try
            {
                //strSql = strSql & vbCrLf & "SELECT "
                //strSql = strSql & vbCrLf & "0 FREIGHT_ELEMENT_MST_PK,"
                //strSql = strSql & vbCrLf & "' ' FREIGHT_ELEMENT_ID"
                //strSql = strSql & vbCrLf & "FROM dual"
                //strSql = strSql & vbCrLf & "Union "
                strSql = strSql + "SELECT ";
                strSql = strSql + "FREIGHT_ELEMENT_MST_PK,";
                strSql = strSql + "FREIGHT_ELEMENT_ID,";
                //freight_element_name
                strSql = strSql + "freight_element_name";
                strSql = strSql + " FROM FREIGHT_ELEMENT_MST_TBL";
                strSql = strSql + " WHERE ACTIVE_FLAG = 1";
                strSql = strSql + " order by Freight_Element_ID";

                WorkFlow objWF = new WorkFlow();

                DataSet DS = objWF.GetDataSet(strSql);
                return JsonConvert.SerializeObject(DS, Formatting.Indented);
                // Manjunath  PTS ID:Sep-02  14/09/2011
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
        #region "Fetch Freight Element Into Parameters"
        /// <summary>
        /// Fetches the freight surcharge_ parameters.
        /// </summary>
        /// <returns></returns>
        public string FetchFreightSurcharge_Parameters()
        {
            string strSql = null;
            try
            {
                strSql = strSql + "SELECT ";
                strSql = strSql + "FREIGHT_ELEMENT_MST_PK,";
                strSql = strSql + "FREIGHT_ELEMENT_ID,";
                strSql = strSql + " freight_element_name";
                strSql = strSql + " FROM FREIGHT_ELEMENT_MST_TBL ";
                strSql = strSql + " WHERE CHARGE_TYPE IN (1,2) ";
                strSql = strSql + " AND ACTIVE_FLAG = 1 ";
                strSql = strSql + " order by Freight_Element_ID";

                WorkFlow objWF = new WorkFlow();
                DataSet DS = objWF.GetDataSet(strSql);
                return JsonConvert.SerializeObject(DS, Formatting.Indented);
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

        #region "Fetch Freight Element Into Parameters"
        /// <summary>
        /// Fetches the freight mi s_ parameters.
        /// </summary>
        /// <returns></returns>
        public string FetchFreightMIS_Parameters()
        {
            string strSql = null;
            try
            {
                strSql = strSql + "SELECT ";
                strSql = strSql + "FREIGHT_ELEMENT_MST_PK,";
                strSql = strSql + "FREIGHT_ELEMENT_ID,";
                strSql = strSql + " freight_element_name";
                strSql = strSql + " FROM FREIGHT_ELEMENT_MST_TBL ";
                strSql = strSql + " WHERE ";
                strSql = strSql + " ACTIVE_FLAG = 1 ";
                strSql = strSql + " order by Freight_Element_ID";
                WorkFlow objWF = new WorkFlow();
                DataSet DS = objWF.GetDataSet(strSql);
                return JsonConvert.SerializeObject(DS, Formatting.Indented);
                // Manjunath  PTS ID:Sep-02  14/09/2011
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

        #region "Fetch Detention "
        /// <summary>
        /// Fetch_s the detention.
        /// </summary>
        /// <returns></returns>
        public string Fetch_Detention()
		{
			try {
				string strSql = null;
				//Modified By Koteshwari on 28/5/2011
				//strSql = strSql & vbCrLf & "SELECT FREIGHT_ELEMENT_MST_PK,FREIGHT_ELEMENT_ID,freight_element_name FROM FREIGHT_ELEMENT_MST_TBL FEM WHERE FEM.BUSINESS_TYPE = 2 AND FEM.ACTIVE_FLAG = 1"
				strSql = strSql + "SELECT FREIGHT_ELEMENT_MST_PK,FREIGHT_ELEMENT_ID,freight_element_name FROM FREIGHT_ELEMENT_MST_TBL FEM WHERE FEM.BUSINESS_TYPE = 2 AND FEM.ACTIVE_FLAG = 1  ORDER BY FREIGHT_ELEMENT_ID";
				//End

				WorkFlow objWF = new WorkFlow();
                DataSet DS = objWF.GetDataSet(strSql);
                return JsonConvert.SerializeObject(DS, Formatting.Indented);
                // Manjunath  PTS ID:Sep-02  14/09/2011
            } catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

        #endregion

        #region "Fetch Demurage"
        /// <summary>
        /// Fetch_s the demurage.
        /// </summary>
        /// <returns></returns>
        public string Fetch_Demurage()
		{
			try {
				string strSql = null;
				//Modified By Koteshwari on 28/5/2011
				strSql = strSql + "SELECT FREIGHT_ELEMENT_MST_PK,FREIGHT_ELEMENT_ID,freight_element_name FROM FREIGHT_ELEMENT_MST_TBL FEM WHERE FEM.BUSINESS_TYPE = 1 AND FEM.ACTIVE_FLAG = 1   ORDER BY FREIGHT_ELEMENT_ID";
				WorkFlow objWF = new WorkFlow();

                DataSet DS = objWF.GetDataSet(strSql);
                return JsonConvert.SerializeObject(DS, Formatting.Indented);
                // Manjunath  PTS ID:Sep-02  14/09/2011
            } catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}



        #endregion

        //Added by Snigdharani for formula form
        #region "Fetch Freight Elements for Formula Form"
        /// <summary>
        /// Fetches all freight.
        /// </summary>
        /// <param name="biztype">The biztype.</param>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public object fetchAllFreight(string biztype, int pk)
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			strSql = "SELECT FRT.FREIGHT_ELEMENT_MST_PK FRT_ELE_PK,";
			strSql += " FRT.FREIGHT_ELEMENT_ID FRT_ELE_ID,";
			strSql += " '' PERCENTAGE,";
			strSql += " '' SEL";
			strSql += " FROM FREIGHT_ELEMENT_MST_TBL FRT";
			strSql += " WHERE FRT.ACTIVE_FLAG = 1";
			if (Convert.ToInt32(biztype) == 1) {
				strSql += " AND FRT.BUSINESS_TYPE = 1";
			} else if (Convert.ToInt32(biztype) == 2)
            {
                strSql += " AND FRT.BUSINESS_TYPE = 2";
			}
			strSql += " AND FRT.FREIGHT_ELEMENT_MST_PK <> " + pk;
			strSql += " ORDER BY FRT.FREIGHT_ELEMENT_ID";
			try {
				return objWF.GetDataSet(strSql);
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        //End Snigdharani
        #endregion

        /// <summary>
        /// Fetches all freight element.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        /// Added By Koteshwari
        #region "Fetch Freight Elements"
        public object fetchAllFreightElement(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder strCount = new System.Text.StringBuilder(5000);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			Int32 TotalRecords = default(Int32);
			System.Text.StringBuilder strSQLBuilder = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			sb.Append("SELECT FMT.FREIGHT_ELEMENT_MST_PK,");
			sb.Append("       FT.FREIGHT_ELEMENT_TRN_PK,");
			sb.Append("       " + PK + " FREIGHT_ELEMENT_FK,");
			sb.Append("       FMT.FREIGHT_ELEMENT_ID,");
			sb.Append("       FMT.FREIGHT_ELEMENT_NAME,");
			//sb.Append("       FT.CHKFLAG SEL")
			sb.Append("        (CASE  ");
			sb.Append("         WHEN FT.CHKFLAG IS NOT NULL THEN");
			sb.Append("        FT.CHKFLAG");
			sb.Append("        ELSE");
			sb.Append("         0  END)  SEL,");
			sb.Append("  '% of ' || CONCADINATE_FUN_FREIGHTELEMENT(" + PK + ", 1) SURCHARGE,(SELECT COUNT(*)  FROM FREIGHT_ELEMENT_MST_TBL FEM, FREIGHT_ELEMENT_TRN_TBL FE ");
			//'added for dts:6496
			sb.Append("  WHERE FMT.FREIGHT_ELEMENT_MST_PK =FE.FREIGHT_ELEMENT_FK   AND FE.CHKFLAG = 1 AND FMT.FREIGHT_ELEMENT_MST_PK =FMT.FREIGHT_ELEMENT_MST_PK)TRNPKCNT ");

			sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FMT, FREIGHT_ELEMENT_TRN_TBL FT");
			sb.Append(" WHERE FMT.BASIC_CHRAGE = 1");
			sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = FT.FREIGHT_ELEMENT_MST_FK(+)");
			sb.Append("   AND FT.FREIGHT_ELEMENT_FK(+) IN ( " + PK + " )");
			strCount.Append(" SELECT COUNT(*)");
			strCount.Append(" FROM ");
			strCount.Append("(" + sb.ToString() + ")");
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;
			strCount.Remove(0, strCount.Length);
			strSQLBuilder.Append(" SELECT  qry.* FROM ");
			strSQLBuilder.Append(" (SELECT ROWNUM SR_NO,T.* FROM ");
			strSQLBuilder.Append((" (" + sb.ToString() + ")"));
			strSQLBuilder.Append(" T) qry  WHERE SR_NO  Between " + start + " and " + last);
			strSQLBuilder.Append(" ORDER BY SR_NO ");
			try {
				return (new WorkFlow()).GetDataSet(strSQLBuilder.ToString());
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion
        //'End

        #region "New Freight Element"
        /// <summary>
        /// Fetches all new.
        /// </summary>
        /// <param name="Freight_ElementPK">The freight_ element pk.</param>
        /// <param name="Freight_ElementID">The freight_ element identifier.</param>
        /// <param name="Freight_ElementName">Name of the freight_ element.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="intBusType">Type of the int bus.</param>
        /// <param name="intUser">The int user.</param>
        /// <param name="intFreightType">Type of the int freight.</param>
        /// <param name="intCrDrType">Type of the int cr dr.</param>
        /// <param name="intServiceType">Type of the int service.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAllNew(Int16 Freight_ElementPK = 0, string Freight_ElementID = "", string Freight_ElementName = "", string SearchType = "", string strColumnName = "FREIGHT_ELEMENT_ID", int ActiveFlag = 1, Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, int intBusType = 0,
		int intUser = 0, int intFreightType = 0, int intCrDrType = 0, int intServiceType = 0, Int32 flag = 0)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();

			if (flag == 0) {
				strCondition += " AND 1=2";
			}
			if (Freight_ElementPK > 0) {
				strCondition = strCondition + " AND Freight_Element_MST_PK=" + Freight_ElementPK;
			}
			if (Freight_ElementID.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition = strCondition + " AND UPPER(Freight_Element_ID) LIKE '" + Freight_ElementID.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition = strCondition + " AND UPPER(Freight_Element_ID) LIKE '%" + Freight_ElementID.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition = strCondition + " AND UPPER(Freight_Element_ID) LIKE '%" + Freight_ElementID.ToUpper().Replace("'", "''") + "%'" ;
				}
			}
			if (Freight_ElementName.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition = strCondition + " AND UPPER(Freight_Element_NAME) LIKE '" + Freight_ElementName.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition = strCondition + " AND UPPER(Freight_Element_NAME) LIKE '%" + Freight_ElementName.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition = strCondition + " AND UPPER(Freight_Element_NAME) LIKE '%" + Freight_ElementName.ToUpper().Replace("'", "''") + "%'" ;
				}
			}

			if (intBusType == 3 & intUser == 3) {
				strCondition += " AND BUSINESS_TYPE IN (1,2,3) ";
			} else if (intBusType == 3 & intUser == 2) {
				strCondition += " AND BUSINESS_TYPE IN (2,3) ";
			} else if (intBusType == 3 & intUser == 1) {
				strCondition += " AND BUSINESS_TYPE IN (1,3) ";
			} else if (intBusType == 1) {
				strCondition += " AND BUSINESS_TYPE IN (" + intBusType + ",3) ";
			} else if (intBusType == 2) {
				strCondition += " AND BUSINESS_TYPE IN (" + intBusType + ",3) ";
			} else {
				strCondition += " AND BUSINESS_TYPE = " + intBusType + " ";
			}
			if (intFreightType != 0) {
				//strCondition &= " AND FREIGHT_TYPE = " & intFreightType & " "
				strCondition += " AND CHARGE_TYPE = " + intFreightType + " ";
			}
			if (intCrDrType != 3) {
				strCondition += " AND CREDIT = " + intCrDrType + " ";
			}
			if (intServiceType != 0) {
				strCondition += " AND SERVICE_TYPE_FK = " + intServiceType + " ";
			}
			if (ActiveFlag == 1) {
				strCondition = strCondition + " AND FR.ACTIVE_FLAG= 1";
			}
			strSQL = "SELECT Count(*) from FREIGHT_ELEMENT_MST_TBL FR WHERE 1=1";
			strSQL += strCondition;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;


			strSQL = "SELECT * FROM (";
			strSQL += "SELECT ROWNUM SR_NO, Q.* FROM ";
			strSQL += "(SELECT ";
			strSQL = strSQL + " NVL(fr.ACTIVE_FLAG,0) ACTIVE_FLAG , ";
			strSQL = strSQL + " FR.FREIGHT_ELEMENT_MST_PK CHARGE_PK,";
			strSQL = strSQL + " FR.FREIGHT_ELEMENT_ID CHARGE_ID,";
			strSQL = strSQL + " INITCAP(FR.FREIGHT_ELEMENT_NAME) CHARGE_NAME,";
			strSQL = strSQL + " FR.CHARGE_TYPE CHARGE_TYPE_FK,";
			strSQL = strSQL + " DECODE(FR.CHARGE_TYPE,1,'Freight',2,'Surcharge',3,'Local') CHARGE_TYPE, ";
			strSQL = strSQL + " FR.APPLICABLE_ON APPLICABLE_FK,";
			strSQL = strSQL + " DECODE(FR.APPLICABLE_ON,1,'Trade',2,'PortPair',3,'Local') APPLICABLE,";
			strSQL = strSQL + " FR.PAYMENT_TYPE PAYMENT_TYPE_FK,";
			strSQL = strSQL + " DECODE(FR.PAYMENT_TYPE,0,'N/A',1,'Origin',2,'Destination') PAYMENT_TYPE,";
			strSQL = strSQL + " CGEMT.CHARGE_REV_ELE_ID FIN_CODE,";
			strSQL = strSQL + " CGEMT.CHARGE_COST_ELE_ID COA_CODE, ";
			strSQL = strSQL + " '...' CONFIG_BTN, ";
			strSQL = strSQL + " FR.CHARGE_BASIS CHARGE_BASIS_FK, ";
			//strSQL = strSQL & vbCrLf & " DECODE(FR.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,"
			strSQL = strSQL + " D.DDID CHARGE_BASIS,";
			strSQL = strSQL + " DECODE(FR.CREDIT, '0', 'Cr', '1', 'Dr') CREDIT ,";
			strSQL = strSQL + " NVL(FR.BY_DEFAULT, 0) BYDEFAULT,";
			strSQL = strSQL + " FR.BUSINESS_TYPE BIZ_TYPE , ";
			strSQL = strSQL + " FR.SERVICE_TYPE_FK SERVICE_PK, ";
			strSQL = strSQL + " DECODE(FR.SERVICE_TYPE_FK,'1','Warehouse','2','Transport','3','Haulage','4','Customs brokerage') SERVICES,";
			strSQL = strSQL + " FR.PREFERENCE, FR.VERSION_NO, '' DELFLAG , (SELECT COUNT(*) FROM CHARGE_GRUOP_ELEMENT_MAP_TBL CGEMT";
			strSQL = strSQL + " WHERE CGEMT.FREIGHT_ELE_MST_FK = FR.FREIGHT_ELEMENT_MST_PK) MAPPED ";
			strSQL = strSQL + " FROM ";
			strSQL = strSQL + " FREIGHT_ELEMENT_MST_TBL FR, CHARGE_GRUOP_ELEMENT_MAP_TBL CGEMT, ";
			strSQL = strSQL + "          (SELECT TO_NUMBER(DD.DD_VALUE) DDVALUE, DD.DD_ID DDID";
			strSQL = strSQL + "              FROM QFOR_DROP_DOWN_TBL DD";
			strSQL = strSQL + "              WHERE DD.DD_FLAG = 'BASIS'";
			strSQL = strSQL + "               AND DD.CONFIG_ID = 'QFOR4458') D ";

			strSQL = strSQL + " WHERE 1=1";
			strSQL = strSQL + " AND FR.FREIGHT_ELEMENT_MST_PK =CGEMT.FREIGHT_ELE_MST_FK(+) ";
			strSQL = strSQL + " AND FR.CHARGE_BASIS = D.DDVALUE(+)";
			strSQL += strCondition;

			if (!strColumnName.Equals("SR_NO")) {
				strSQL += "order by CHARGE_ID," + strColumnName;
			}

			if (!blnSortAscending & !strColumnName.Equals("SR_NO")) {
				strSQL += " DESC";
			}
			strSQL += ")q ) WHERE SR_NO  Between " + start + " and " + last;
			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Freight Pop Header"
        /// <summary>
        /// Fetches the pop up header.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchPopUpHeader()
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT *");
			sb.Append("  FROM (SELECT 1 AS ODR, 'Trade' ATTRIBUTE, '' VALUE, '...' SEL, '' PK");
			sb.Append("          FROM DUAL");
			sb.Append("        UNION");
			sb.Append("        SELECT 2 AS ODR, 'Port Pair' ATTRIBUTE, '' VALUE, '...' SEL, '' PK");
			sb.Append("          FROM DUAL");
			sb.Append("        UNION");
			sb.Append("        SELECT 3 AS ODR, 'Currency' ATTRIBUTE, '' VALUE, '...' SEL, '' PK");
			sb.Append("          FROM DUAL )");
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        /// <summary>
        /// Fetches the FRT pop up data.
        /// </summary>
        /// <param name="ChargePK">The charge pk.</param>
        /// <param name="FreightType">Type of the freight.</param>
        /// <returns></returns>
        public DataSet FetchFrtPopUpData(int ChargePK, int FreightType)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();

			sb.Append("SELECT ROWTOCOL('SELECT DISTINCT TD.TRADE_MST_PK");
			sb.Append("  FROM FREIGHT_CONFIG_TRN_TBL FT,");
			sb.Append("       SECTOR_MST_TBL         ST,");
			sb.Append("       TRADE_MST_TBL          TD,");
			sb.Append("       PORT_MST_TBL           POL,");
			sb.Append("       PORT_MST_TBL           POD");
			sb.Append(" WHERE FT.SECTOR_MST_FK = ST.SECTOR_MST_PK");
			sb.Append("   AND TD.TRADE_MST_PK = ST.TRADE_MST_FK");
			sb.Append("   AND POL.PORT_MST_PK = ST.FROM_PORT_FK");
			sb.Append("   AND POD.PORT_MST_PK = ST.TO_PORT_FK");
			sb.Append("   AND FT.FREIGHT_TYPE=" + FreightType);
			sb.Append("   AND FT.FREIGHT_ELEMENT_FK = " + ChargePK + " ') TRADE_PK,");
			sb.Append("       ROWTOCOL('SELECT DISTINCT TD.TRADE_CODE");
			sb.Append("  FROM FREIGHT_CONFIG_TRN_TBL FT,");
			sb.Append("       SECTOR_MST_TBL         ST,");
			sb.Append("       TRADE_MST_TBL          TD,");
			sb.Append("       PORT_MST_TBL           POL,");
			sb.Append("       PORT_MST_TBL           POD");
			sb.Append(" WHERE FT.SECTOR_MST_FK = ST.SECTOR_MST_PK");
			sb.Append("   AND TD.TRADE_MST_PK = ST.TRADE_MST_FK");
			sb.Append("   AND POL.PORT_MST_PK = ST.FROM_PORT_FK");
			sb.Append("   AND POD.PORT_MST_PK = ST.TO_PORT_FK");
			sb.Append("   AND FT.FREIGHT_TYPE=" + FreightType);
			sb.Append("   AND FT.FREIGHT_ELEMENT_FK= " + ChargePK + " ') TRADE,");
			sb.Append("       ROWTOCOL('SELECT DISTINCT ST.SECTOR_MST_PK");
			sb.Append("  FROM FREIGHT_CONFIG_TRN_TBL FT,");
			sb.Append("       SECTOR_MST_TBL         ST,");
			sb.Append("       PORT_MST_TBL           POL,");
			sb.Append("       PORT_MST_TBL           POD");
			sb.Append(" WHERE FT.SECTOR_MST_FK = ST.SECTOR_MST_PK");
			sb.Append("   AND POL.PORT_MST_PK = ST.FROM_PORT_FK");
			sb.Append("   AND POD.PORT_MST_PK = ST.TO_PORT_FK ");
			sb.Append("   AND FT.FREIGHT_TYPE=" + FreightType);
			sb.Append("   AND FT.FREIGHT_ELEMENT_FK = " + ChargePK + " ') SECTOR_PK,");
			sb.Append("       ROWTOCOL('SELECT DISTINCT (POL.PORT_ID ||''-''||POD.PORT_ID)");
			sb.Append("  FROM FREIGHT_CONFIG_TRN_TBL FT, SECTOR_MST_TBL ST,PORT_MST_TBL POL,PORT_MST_TBL POD");
			sb.Append(" WHERE FT.SECTOR_MST_FK = ST.SECTOR_MST_PK");
			sb.Append(" AND POL.PORT_MST_PK=ST.FROM_PORT_FK");
			sb.Append(" AND POD.PORT_MST_PK=ST.TO_PORT_FK");
			sb.Append("   AND FT.FREIGHT_TYPE=" + FreightType);
			sb.Append(" AND FT.FREIGHT_ELEMENT_FK= " + ChargePK + " ') PORT_PAIR,");
			sb.Append("       (SELECT DISTINCT CY.CURRENCY_MST_PK");
			sb.Append("          FROM FREIGHT_CONFIG_TRN_TBL FT, CURRENCY_TYPE_MST_TBL CY");
			sb.Append("         WHERE CY.CURRENCY_MST_PK = FT.CURRENCY_MST_FK  AND ROWNUM =1 ");
			sb.Append("   AND FT.FREIGHT_TYPE=" + FreightType);
			sb.Append("           AND FT.FREIGHT_ELEMENT_FK = " + ChargePK + " ) CURR_PK,");
			sb.Append("       (SELECT DISTINCT CY.CURRENCY_ID");
			sb.Append("          FROM FREIGHT_CONFIG_TRN_TBL FT, CURRENCY_TYPE_MST_TBL CY");
			sb.Append("         WHERE CY.CURRENCY_MST_PK = FT.CURRENCY_MST_FK  AND ROWNUM =1");
			sb.Append("   AND FT.FREIGHT_TYPE=" + FreightType);
			sb.Append("           AND FT.FREIGHT_ELEMENT_FK = " + ChargePK + " ) CURR,");
			sb.Append("(SELECT DISTINCT FT.VATOS_FLAG");
			sb.Append("          FROM FREIGHT_CONFIG_TRN_TBL FT");
			sb.Append("   WHERE FT.FREIGHT_TYPE=" + FreightType);
			sb.Append("          AND ROWNUM =1 AND FT.FREIGHT_ELEMENT_FK = " + ChargePK + " )VATOS_FLAG");
			sb.Append("  FROM DUAL");
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Surcharge Pop Header"
        /// <summary>
        /// Fetches the surcharge pop up.
        /// </summary>
        /// <param name="Charge_PK">The charge_ pk.</param>
        /// <param name="Charge_Type">Type of the charge_.</param>
        /// <returns></returns>
        public DataSet FetchSurchargePopUp(int Charge_PK, int Charge_Type)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			//sb.Append("SELECT DISTINCT ROWNUM SLNR,")
			//sb.Append("                '' ACTIVE,")
			//sb.Append("                TMT.TRADE_MST_PK,")
			//sb.Append("                SMT.SECTOR_MST_PK,")
			//sb.Append("                TMT.TRADE_NAME TRADE,")
			//sb.Append("                POL.PORT_MST_PK POL_PK,")
			//sb.Append("                POD.PORT_MST_PK POD_PK,")
			//sb.Append("                (POL.PORT_ID || '-' || POD.PORT_ID) SECTOR,")
			//sb.Append("                0 CURRENCY_MST_PK,")
			//sb.Append("                '' CURRENCY_ID,")
			//sb.Append("                0 VATOS_PK,")
			//sb.Append("                '' VATOS,")
			//sb.Append("                0 BASIS_PK,")
			//sb.Append("                '' BASIS,")
			//sb.Append("                0 VALUE,")
			//sb.Append("                '' CHARGE_ID,")
			//sb.Append("                '' SEL,'' FRTPK ")
			//sb.Append("  FROM TRADE_MST_TBL  TMT,")
			//sb.Append("       SECTOR_MST_TBL SMT,")
			//sb.Append("       PORT_MST_TBL   POL,")
			//sb.Append("       PORT_MST_TBL   POD")
			//sb.Append(" WHERE TMT.TRADE_MST_PK = SMT.TRADE_MST_FK")
			//sb.Append("   AND POL.PORT_MST_PK = SMT.FROM_PORT_FK")
			//sb.Append("   AND POD.PORT_MST_PK = SMT.TO_PORT_FK")
			//sb.Append("   AND TMT.ACTIVE_FLAG = 1")
			//sb.Append("   AND SMT.ACTIVE = 1")
			//sb.Append("   AND TMT.TRADE_MST_PK = 824")
			//sb.Append("   AND 1 = 2")

			sb.Append("SELECT ROWNUM SLNR,Q.* FROM ( SELECT DISTINCT ");
			sb.Append("                DECODE(FT.ACTIVE_FLAG,1,'true',0,'false') ACTIVE,");
			sb.Append("                TMT.TRADE_MST_PK,");
			sb.Append("                SMT.SECTOR_MST_PK,");
			sb.Append("                TMT.TRADE_NAME TRADE,");
			sb.Append("                POL.PORT_MST_PK POL_PK,");
			sb.Append("                POD.PORT_MST_PK POD_PK,");
			sb.Append("                (POL.PORT_ID || '-' || POD.PORT_ID) SECTOR,");
			sb.Append("                CTY.CURRENCY_MST_PK,");
			sb.Append("                CTY.CURRENCY_ID,");
			sb.Append("                FT.VATOS_FLAG VATOS_PK,");
			sb.Append("                DECODE(FT.VATOS_FLAG, 0, '', 1, 'VATOS', 2, 'Add-VATOS') VATOS,");
			sb.Append("                FT.BASIS BASIS_PK,");
			sb.Append("                DECODE(FT.BASIS, 1, '%', 2, 'Flat Rate', 3, 'Kgs', 4, 'Unit') BASIS,");
			sb.Append("                FT.BASIS_VALUE VALUE,");
			sb.Append("                '' CHARGE_ID,");
			sb.Append("                '' SEL,");
			sb.Append("                FT.BASIS_ELEMENT_FKS FRTPK,FT.FREIGHT_CONFIG_PK,");
			sb.Append("                FT.FREIGHT_ELEMENT_FK,FT.FREIGHT_TYPE,'' DELFLAG ");
			sb.Append("  FROM FREIGHT_CONFIG_TRN_TBL FT,");
			sb.Append("       SECTOR_MST_TBL         SMT,");
			sb.Append("       TRADE_MST_TBL          TMT,");
			sb.Append("       CURRENCY_TYPE_MST_TBL  CTY,");
			sb.Append("       PORT_MST_TBL           POL,");
			sb.Append("       PORT_MST_TBL           POD");
			sb.Append(" WHERE SMT.SECTOR_MST_PK = FT.SECTOR_MST_FK");
			sb.Append("   AND TMT.TRADE_MST_PK = SMT.TRADE_MST_FK");
			sb.Append("   AND CTY.CURRENCY_MST_PK = FT.CURRENCY_MST_FK");
			sb.Append("   AND POL.PORT_MST_PK = SMT.FROM_PORT_FK");
			sb.Append("   AND POD.PORT_MST_PK = SMT.TO_PORT_FK");
			sb.Append("   AND FT.FREIGHT_ELEMENT_FK = " + Charge_PK);
			sb.Append("   AND FT.FREIGHT_TYPE = " + Charge_Type);
			sb.Append("    ORDER BY CURRENCY_ID ) Q ");

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Local Charges Tree "
        /// <summary>
        /// Fetches the local charge pop up.
        /// </summary>
        /// <param name="CountryPK">The country pk.</param>
        /// <param name="PortPK">The port pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchLocalChargePopUp(string CountryPK = "", string PortPK = "", Int32 BizType = 0)
		{
			DataSet objds = new DataSet();
			string str = null;
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				//To Populate All the Ports If BizType is Both
				if (BizType == 3) {
					BizType = 0;
				}
				sb.Append("SELECT DISTINCT C.COUNTRY_MST_PK,");
				sb.Append("      C.COUNTRY_NAME ");
				sb.Append("  FROM COUNTRY_MST_TBL C,PORT_MST_TBL P ");
				sb.Append("  WHERE C.ACTIVE_FLAG = 1 ");
				sb.Append("  AND P.ACTIVE_FLAG = 1 ");
				sb.Append("  AND C.COUNTRY_MST_PK=P.COUNTRY_MST_FK ");
				if (!string.IsNullOrEmpty(PortPK)) {
					sb.Append("  AND P.PORT_MST_PK IN (" + PortPK + ")");
				}
				if (!string.IsNullOrEmpty(CountryPK)) {
					sb.Append("  AND C.COUNTRY_MST_PK IN (" + CountryPK + ")");
				}
				if (BizType > 0) {
					sb.Append("  AND P.BUSINESS_TYPE IN (" + BizType + ")");
				}
				sb.Append(" ORDER BY C.COUNTRY_NAME");

				objds.Tables.Add(objWF.GetDataTable(sb.ToString()));
				objds.Tables[0].TableName = "COUNTRY";

				sb1.Append("SELECT P.COUNTRY_MST_FK,");
				sb1.Append("       P.PORT_MST_PK,");
				sb1.Append("      (P.PORT_ID || '-' || P.PORT_NAME) PORT");
				sb1.Append("  FROM PORT_MST_TBL P, COUNTRY_MST_TBL C");
				sb1.Append(" WHERE C.COUNTRY_MST_PK = P.COUNTRY_MST_FK ");
				sb1.Append(" AND C.ACTIVE_FLAG = 1 ");
				sb1.Append(" AND P.ACTIVE_FLAG = 1 ");
				if (!string.IsNullOrEmpty(PortPK)) {
					sb1.Append("  AND P.PORT_MST_PK IN (" + PortPK + ")");
				}
				if (BizType > 0) {
					sb1.Append("  AND P.BUSINESS_TYPE IN (" + BizType + ")");
				}
				sb1.Append(" ORDER BY C.COUNTRY_ID, P.PORT_ID ");
				objds.Tables.Add(objWF.GetDataTable(sb1.ToString()));
				objds.Tables[1].TableName = "PORT";


				DataRelation objRel1 = new DataRelation("REL_COUNTRY_PORT", objds.Tables[0].Columns["COUNTRY_MST_PK"], objds.Tables[1].Columns["COUNTRY_MST_FK"]);

				objRel1.Nested = true;
				objds.Relations.Add(objRel1);
				return objds;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Pk Values"
        /// <summary>
        /// Gets the trade pk.
        /// </summary>
        /// <param name="SectorPK">The sector pk.</param>
        /// <returns></returns>
        public long GetTradePK(int SectorPK)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append(" SELECT DISTINCT S.TRADE_MST_FK FROM SECTOR_MST_TBL S WHERE ROWNUM=1 AND S.SECTOR_MST_PK=" + SectorPK);
			WorkFlow objWF = new WorkFlow();
			try {
                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        /// <summary>
        /// Gets the port p ks.
        /// </summary>
        /// <param name="Charge_PK">The charge_ pk.</param>
        /// <param name="Charge_Type">Type of the charge_.</param>
        /// <returns></returns>
        public string GetPortPKs(int Charge_PK, int Charge_Type)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append(" SELECT ROWTOCOL('SELECT DISTINCT T.PORT_MST_FK FROM FREIGHT_CONFIG_TRN_TBL T ");
			sb.Append(" WHERE T.FREIGHT_ELEMENT_FK =  " + Charge_PK);
			sb.Append("   AND T.FREIGHT_TYPE= " + Charge_Type);
			sb.Append(" ') PORT_MST_FK FROM DUAL ");
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.ExecuteScaler(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        /// <summary>
        /// Gets the type of the charge.
        /// </summary>
        /// <param name="Charge_PK">The charge_ pk.</param>
        /// <param name="Charge_Type">Type of the charge_.</param>
        /// <returns></returns>
        public int GetChargeType(int Charge_PK, int Charge_Type)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT DISTINCT T.CHARGE_TYPE");
			sb.Append("  FROM FREIGHT_CONFIG_TRN_TBL T");
			sb.Append(" WHERE T.FREIGHT_ELEMENT_FK = " + Charge_PK);
			sb.Append("   AND T.FREIGHT_TYPE = " + Charge_Type);

			WorkFlow objWF = new WorkFlow();
			try {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            } catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Pop Header"
        /// <summary>
        /// Gets the blank dataset.
        /// </summary>
        /// <returns></returns>
        public DataSet GetBlankDataset()
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append(" SELECT * FROM FREIGHT_CONFIG_TRN_TBL WHERE 1=2 ");
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Save Freight PopUp"
        /// <summary>
        /// Saves the freight pop up.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="ChargePK">The charge pk.</param>
        /// <param name="FreightPKs">The freight p ks.</param>
        /// <returns></returns>
        public ArrayList SaveFreightPopUp(DataSet M_DataSet, long ChargePK, string FreightPKs)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			arrMessage.Clear();
			try {
				//DeleteCharge(M_DataSet.Tables[0].Rows[0]["FREIGHT_ELEMENT_FK"], M_DataSet.Tables[0].Rows[0]["FREIGHT_TYPE"]);

				var _with14 = insCommand;
				_with14.Connection = objWK.MyConnection;
				_with14.CommandType = CommandType.StoredProcedure;
				_with14.CommandText = objWK.MyUserName + ".FREIGHT_CONFIG_TRN_TBL_PKG.FREIGHT_CONFIG_TRN_TBL_INS";
				var _with15 = _with14.Parameters;

				insCommand.CommandText = objWK.MyUserName + ".FREIGHT_CONFIG_TRN_TBL_PKG.FREIGHT_CONFIG_TRN_TBL_INS";

				insCommand.Parameters.Add("FREIGHT_ELEMENT_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["FREIGHT_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 2, "FREIGHT_TYPE").Direction = ParameterDirection.Input;
				insCommand.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("TRADE_MST_FK_IN", OracleDbType.Int32, 10, "TRADE_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["TRADE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("SECTOR_MST_FK_IN", OracleDbType.Int32, 10, "SECTOR_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["SECTOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;


				insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;


				insCommand.Parameters.Add("VATOS_FLAG_IN", OracleDbType.Int32, 1, "VATOS_FLAG").Direction = ParameterDirection.Input;
				insCommand.Parameters["VATOS_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "FREIGHT_CONFIG_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


				var _with16 = objWK.MyDataAdapter;
				_with16.InsertCommand = insCommand;
				_with16.InsertCommand.Transaction = TRAN;
				RecAfct = _with16.Update(M_DataSet);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					UpdateAIFCharge(ChargePK, FreightPKs);
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
        /// <summary>
        /// Deletes the charge.
        /// </summary>
        /// <param name="ChargePK">The charge pk.</param>
        /// <param name="ChargeType">Type of the charge.</param>
        /// <returns></returns>
        public string DeleteCharge(long ChargePK, long ChargeType)
		{
			string Strsql = null;
			WorkFlow Objwk = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
			int RcdCnt = 0;
			try {
				sb.Append("SELECT COUNT(*)");
				sb.Append("      FROM FREIGHT_CONFIG_TRN_TBL F");
				sb.Append("     WHERE F.FREIGHT_ELEMENT_FK = " + ChargePK);
				//sb.Append("       AND F.FREIGHT_TYPE = " & ChargeType)

				//RcdCnt = Objwk.ExecuteScaler(sb.ToString());

				if (RcdCnt > 0) {
					sb1.Append(" DELETE FROM FREIGHT_CONFIG_TRN_TBL F ");
					sb1.Append(" WHERE F.FREIGHT_ELEMENT_FK =" + ChargePK);
					//sb1.Append(" AND F.FREIGHT_TYPE = " & ChargeType)
					Objwk.ExecuteCommands(sb1.ToString());
				}
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
            return "";
		}
        #endregion

        #region "Save Surcharge PopUp"
        /// <summary>
        /// Saves the surcharge pop up.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList SaveSurchargePopUp(DataSet M_DataSet)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			arrMessage.Clear();
			try {
				//DeleteCharge(M_DataSet.Tables[0].Rows[0]["FREIGHT_ELEMENT_FK"], M_DataSet.Tables[0].Rows[0]["FREIGHT_TYPE"]);
				var _with17 = insCommand;
				_with17.Connection = objWK.MyConnection;
				_with17.CommandType = CommandType.StoredProcedure;
				_with17.CommandText = objWK.MyUserName + ".FREIGHT_CONFIG_TRN_TBL_PKG.FREIGHT_CONFIG_TRN_TBL_INS";
				var _with18 = _with17.Parameters;

				insCommand.CommandText = objWK.MyUserName + ".FREIGHT_CONFIG_TRN_TBL_PKG.FREIGHT_CONFIG_TRN_TBL_INS";

				insCommand.Parameters.Add("FREIGHT_ELEMENT_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["FREIGHT_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 2, "FREIGHT_TYPE").Direction = ParameterDirection.Input;
				insCommand.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("TRADE_MST_FK_IN", OracleDbType.Int32, 10, "TRADE_MST_PK").Direction = ParameterDirection.Input;
				insCommand.Parameters["TRADE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("SECTOR_MST_FK_IN", OracleDbType.Int32, 10, "SECTOR_MST_PK").Direction = ParameterDirection.Input;
				insCommand.Parameters["SECTOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_PK").Direction = ParameterDirection.Input;
				insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "BASIS_PK").Direction = ParameterDirection.Input;
				insCommand.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("BASIS_VALUE_IN", OracleDbType.Int32, 10, "VALUE").Direction = ParameterDirection.Input;
				insCommand.Parameters["BASIS_VALUE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("BASIS_ELEMENT_FKS_IN", OracleDbType.Varchar2, 20, "FRTPK").Direction = ParameterDirection.Input;
				insCommand.Parameters["BASIS_ELEMENT_FKS_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("VATOS_FLAG_IN", OracleDbType.Int32, 1, "VATOS_PK").Direction = ParameterDirection.Input;
				insCommand.Parameters["VATOS_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
				insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "FREIGHT_CONFIG_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


				var _with19 = objWK.MyDataAdapter;
				_with19.InsertCommand = insCommand;
				_with19.InsertCommand.Transaction = TRAN;
				RecAfct = _with19.Update(M_DataSet);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
        #endregion

        #region "Save Local Charge PopUp"
        /// <summary>
        /// Saves the local pop up.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList SaveLocalPopUp(DataSet M_DataSet)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			arrMessage.Clear();
			try {
				//DeleteCharge(M_DataSet.Tables[0].Rows[0]["FREIGHT_ELEMENT_FK"], M_DataSet.Tables[0].Rows[0]["FREIGHT_TYPE"]);
				var _with20 = insCommand;
				_with20.Connection = objWK.MyConnection;
				_with20.CommandType = CommandType.StoredProcedure;
				_with20.CommandText = objWK.MyUserName + ".FREIGHT_CONFIG_TRN_TBL_PKG.FREIGHT_CONFIG_TRN_TBL_INS";
				var _with21 = _with20.Parameters;

				insCommand.CommandText = objWK.MyUserName + ".FREIGHT_CONFIG_TRN_TBL_PKG.FREIGHT_CONFIG_TRN_TBL_INS";

				insCommand.Parameters.Add("FREIGHT_ELEMENT_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["FREIGHT_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 2, "FREIGHT_TYPE").Direction = ParameterDirection.Input;
				insCommand.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 10, "PORT_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["PORT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CHARGE_TYPE_IN", OracleDbType.Int32, 1, "CHARGE_TYPE").Direction = ParameterDirection.Input;
				insCommand.Parameters["CHARGE_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "FREIGHT_CONFIG_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


				var _with22 = objWK.MyDataAdapter;
				_with22.InsertCommand = insCommand;
				_with22.InsertCommand.Transaction = TRAN;
				RecAfct = _with22.Update(M_DataSet);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
        #endregion

        #region "Value List Queries"
        /// <summary>
        /// Fetches the vatos value list.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchVatosValueList()
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT 0 PK , ' ' VALUE FROM DUAL  ");
			sb.Append(" UNION ");
			sb.Append(" SELECT 1 PK , 'VATOS' VALUE FROM DUAL ");
			sb.Append(" UNION ");
			sb.Append(" SELECT 2 PK , 'ADVATOS' VALUE FROM DUAL");
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get default Currency as USD"
        /// <summary>
        /// Gets the default curr.
        /// </summary>
        /// <returns></returns>
        public DataSet GetDefaultCurr()
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT CY.CURRENCY_MST_PK, CY.CURRENCY_ID");
				sb.Append("  FROM CURRENCY_TYPE_MST_TBL CY");
				sb.Append(" WHERE CY.CURRENCY_ID = 'USD'");
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion
        #region "Get AIF Charge Element"
        /// <summary>
        /// Gets the aif.
        /// </summary>
        /// <param name="ChargePK">The charge pk.</param>
        /// <returns></returns>
        public DataSet GetAIF(int ChargePK)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT COUNT(*) FROM PARAMETERS_TBL P WHERE P.FRT_AIF_FK=" + ChargePK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        /// <summary>
        /// Fetches the aif data.
        /// </summary>
        /// <param name="ChargePK">The charge pk.</param>
        /// <returns></returns>
        public DataSet FetchAIFData(int ChargePK)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();

			sb.Append("SELECT F.AIF_FREIGHT_FKS FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=" + ChargePK);

			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        /// <summary>
        /// Updates the aif charge.
        /// </summary>
        /// <param name="ChargePK">The charge pk.</param>
        /// <param name="FreightPKs">The freight p ks.</param>
        /// <returns></returns>
        public string UpdateAIFCharge(long ChargePK, string FreightPKs)
		{
			//'AUG-047
			string Strsql = null;
			WorkFlow Objwk = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
			int RcdCnt = 0;
			try {
				sb.Append("SELECT COUNT(*)");
				sb.Append("      FROM FREIGHT_ELEMENT_MST_TBL F");
				sb.Append("     WHERE F.FREIGHT_ELEMENT_MST_PK = " + ChargePK);
				//sb.Append("       AND F.FREIGHT_TYPE = " & ChargeType)

				RcdCnt = Convert.ToInt32(Objwk.ExecuteScaler(sb.ToString()));

				if (RcdCnt > 0) {
					sb1.Append(" UPDATE FREIGHT_ELEMENT_MST_TBL F SET F.AIF_FREIGHT_FKS='" + FreightPKs + "'");
					sb1.Append(" WHERE F.FREIGHT_ELEMENT_MST_PK ='" + ChargePK + "'");
					//sb1.Append(" AND F.FREIGHT_TYPE = " & ChargeType)
					Objwk.ExecuteCommands(sb1.ToString());
				}
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
            return "";
		}
		#endregion
	}
}
