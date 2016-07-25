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

using Oracle.DataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsUser_Mst_Tbl : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ user_ MST_ pk
        /// </summary>
        private Int32 M_User_Mst_Pk;

        /// <summary>
        /// The m_ user_ identifier
        /// </summary>
        private string M_User_Id;

        /// <summary>
        /// The m_ user_ name
        /// </summary>
        private string M_User_Name;

        /// <summary>
        /// The m_ default_ branch_ fk
        /// </summary>
        private Int32 M_Default_Branch_Fk;

        /// <summary>
        /// The m_ location_ identifier
        /// </summary>
        private string M_Location_Id;

        /// <summary>
        /// The m_ location_ name
        /// </summary>
        private string M_Location_Name;

        /// <summary>
        /// The m_ pass_ word
        /// </summary>
        private string M_Pass_Word;

        /// <summary>
        /// The m_ is_ activated
        /// </summary>
        private Int16 M_Is_Activated;

        /// <summary>
        /// The m_ employee_ MST_ fk
        /// </summary>
        private Int32 M_Employee_Mst_Fk;

        /// <summary>
        /// The m_ customer_ MST_ fk
        /// </summary>
        private Int32 M_Customer_Mst_FK;

        //THis property is only used by IsUserValid Function it has no concern with User Class at all.
        /// <summary>
        /// The m_ menu available
        /// </summary>
        private bool M_MenuAvailable;

        #endregion "List of Members of the Class"

        #region "List of Properties"

        /// <summary>
        /// Gets or sets a value indicating whether [menu available for user].
        /// </summary>
        /// <value>
        /// <c>true</c> if [menu available for user]; otherwise, <c>false</c>.
        /// </value>
        public bool MenuAvailableForUser
        {
            get { return M_MenuAvailable; }
            set { M_MenuAvailable = value; }
        }

        /// <summary>
        /// Gets or sets the user_ MST_ pk.
        /// </summary>
        /// <value>
        /// The user_ MST_ pk.
        /// </value>
        public Int32 User_Mst_Pk
        {
            get { return M_User_Mst_Pk; }
            set { M_User_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the user_ identifier.
        /// </summary>
        /// <value>
        /// The user_ identifier.
        /// </value>
        public string User_Id
        {
            get { return M_User_Id; }
            set { M_User_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the user_.
        /// </summary>
        /// <value>
        /// The name of the user_.
        /// </value>
        public string User_Name
        {
            get { return M_User_Name; }
            set { M_User_Name = value; }
        }

        /// <summary>
        /// Gets or sets the customer_ MST_ fk.
        /// </summary>
        /// <value>
        /// The customer_ MST_ fk.
        /// </value>
        public Int32 Customer_Mst_Fk
        {
            get { return M_Customer_Mst_FK; }
            set { M_Customer_Mst_FK = value; }
        }

        /// <summary>
        /// Gets or sets the employee_ MST_ fk.
        /// </summary>
        /// <value>
        /// The employee_ MST_ fk.
        /// </value>
        public Int32 Employee_Mst_Fk
        {
            get { return M_Employee_Mst_Fk; }
            set { M_Employee_Mst_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the default_ branch_ fk.
        /// </summary>
        /// <value>
        /// The default_ branch_ fk.
        /// </value>
        public Int32 Default_Branch_Fk
        {
            get { return M_Default_Branch_Fk; }
            set { M_Default_Branch_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the location_ identifier.
        /// </summary>
        /// <value>
        /// The location_ identifier.
        /// </value>
        public string Location_Id
        {
            get { return M_Location_Id; }
            set { M_Location_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the location_.
        /// </summary>
        /// <value>
        /// The name of the location_.
        /// </value>
        public string Location_Name
        {
            get { return M_Location_Name; }
            set { M_Location_Name = value; }
        }

        /// <summary>
        /// Gets or sets the pass_ word.
        /// </summary>
        /// <value>
        /// The pass_ word.
        /// </value>
        public string Pass_Word
        {
            get { return M_Pass_Word; }
            set { M_Pass_Word = value; }
        }

        /// <summary>
        /// Gets or sets the is_ activated.
        /// </summary>
        /// <value>
        /// The is_ activated.
        /// </value>
        public Int16 Is_Activated
        {
            get { return M_Is_Activated; }
            set { M_Is_Activated = value; }
        }

        #endregion "List of Properties"

        #region "Fetch All Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="UserID">The user identifier.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="BranchID">The branch identifier.</param>
        /// <param name="BranchName">Name of the branch.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string UserID, string UserName, string BranchID, string BranchName, bool ActiveOnly, string SearchType, string strColumnName, Int32 CurrentPage, Int32 TotalPage, bool blnSortAscending, Int32 flag)
        {
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
            if (SearchType == "C")
            {
                if (UserID.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(USER_ID) LIKE '%" + UserID.ToUpper().Replace("'", "''") + "%'";
                }
                if (UserName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(USER_NAME) LIKE '%" + UserName.ToUpper().Replace("'", "''") + "%'";
                }
                if (BranchID.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(LOCATION_ID) LIKE '%" + BranchID.ToUpper().Replace("'", "''") + "%'";
                }
                if (BranchName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(LOCATION_NAME) LIKE '%" + BranchName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (UserID.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(USER_ID) LIKE '" + UserID.ToUpper().Replace("'", "''") + "%'";
                }
                if (UserName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(USER_NAME) LIKE '" + UserName.ToUpper().Replace("'", "''") + "%'";
                }
                if (BranchID.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(LOCATION_ID) LIKE '" + BranchID.ToUpper().Replace("'", "''") + "%'";
                }
                if (BranchName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(LOCATION_NAME) LIKE '" + BranchName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (ActiveOnly)
            {
                strCondition = strCondition + " AND usr.IS_ACTIVATED=1";
            }
            strSQL = "SELECT Count(*)  ";
            strSQL = strSQL + " FROM USER_MST_TBL usr,LOCATION_MST_TBL loc, EMPLOYEE_MST_TBL emp, CUSTOMER_MST_TBL cus";
            strSQL = strSQL + " WHERE usr.DEFAULT_LOCATION_FK=loc.LOCATION_MST_PK And emp.EMPLOYEE_MST_PK(+) = usr.EMPLOYEE_MST_FK And cus.Customer_MST_PK(+) = usr.CUSTOMER_MST_FK ";
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

            //If SortCol > 0 Then
            //    strCondition = strCondition & " order by " & SortCol
            //Else
            //strCondition = strCondition & " ORDER BY USR.IS_ACTIVATED DESC,USR.USER_ID "
            //End If

            strSQL = "SELECT * FROM ";
            strSQL += "(SELECT ROWNUM SR_NO,q.*from ";
            strSQL = strSQL + "( select User_Mst_Pk, ";
            strSQL = strSQL + " Is_Activated, ";
            strSQL = strSQL + " User_Id, ";
            strSQL = strSQL + " User_Name User_Name, ";
            strSQL = strSQL + " DEFAULT_LOCATION_FK, ";
            strSQL = strSQL + " LOCATION_ID, ";
            strSQL = strSQL + " UPPER(LOCATION_NAME) LOCATION_NAME, ";
            strSQL = strSQL + " DECODER(Pass_Word) Pass_Word, ";

            strSQL = strSQL + " (case when usr.employee_mst_fk is not null and ( usr.customer_mst_fk=0 or usr.customer_mst_fk is null ) then ";
            strSQL = strSQL + " 'Employee' else 'Customer' end) UserType,";

            strSQL = strSQL + " usr.EMPLOYEE_MST_FK , ";
            strSQL = strSQL + " emp.EMPLOYEE_ID , ";
            strSQL = strSQL + " null as Employee_SEARCH, ";

            //thiyagarajan
            //To display customer infor. in the grid on 22/2/08 for PTS Task.
            strSQL = strSQL + " usr.customer_mst_fk ,";
            strSQL = strSQL + " (CASE when usr.customer_mst_fk is not null then ";
            strSQL = strSQL + "(select cust.customer_id from customer_mst_tbl cust where cust.customer_mst_pk=usr.customer_mst_fk) else ' ' end) CUSTOMERID,";
            strSQL = strSQL + " null as Cust_Search ,";
            //end

            strSQL = strSQL + " usr.Version_No,  null as PWDChange, ";
            //added and modified  by surya prasad on 04-Jan-2009 for implementing removals concept
            strSQL = strSQL + " DECODE(usr.BUSINESS_TYPE,'0','','1','Air','2','Sea','3','Both','4','Removals') BUSINESS_TYPE";
            //end
            strSQL = strSQL + " FROM USER_MST_TBL usr,LOCATION_MST_TBL loc, EMPLOYEE_MST_TBL emp";
            strSQL = strSQL + " WHERE usr.DEFAULT_LOCATION_FK=loc.LOCATION_MST_PK And emp.EMPLOYEE_MST_PK(+) = usr.EMPLOYEE_MST_FK ";
            strSQL += strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by  LOCATION_NAME, User_Name";
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL += " )q) WHERE SR_NO  Between " + start + " and " + last;
            strSQL += " Order By SR_NO ";
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

        #endregion "Fetch All Function"

        #region "Insert Function"

        /// <summary>
        /// Inserts this instance.
        /// </summary>
        /// <returns></returns>
        public int Insert()
        {
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with2 = objWS.MyCommand.Parameters;
                _with2.Add("User_Id_IN", M_User_Id).Direction = ParameterDirection.Input;
                _with2.Add("User_Name_IN", M_User_Name).Direction = ParameterDirection.Input;
                _with2.Add("Default_Location_Fk_IN", M_Default_Branch_Fk).Direction = ParameterDirection.Input;
                _with2.Add("Pass_Word_IN", M_Pass_Word).Direction = ParameterDirection.Input;
                _with2.Add("Is_Activated_IN", M_Is_Activated).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = "FEEDERUSER.USER_MST_TBL_PKG.USER_MST_TBL_INS";
                if (objWS.ExecuteCommands() == true)
                {
                    return intPkVal;
                }
                else
                {
                    return -1;
                }
                //Manjunath  PTS ID:Sep-02  16/09/2011
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
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with3 = objWS.MyCommand.Parameters;
                _with3.Add("User_Mst_Pk_IN", M_User_Mst_Pk).Direction = ParameterDirection.Input;
                _with3.Add("User_Id_IN", M_User_Id).Direction = ParameterDirection.Input;
                _with3.Add("User_Name_IN", M_User_Name).Direction = ParameterDirection.Input;
                _with3.Add("Default_Location_Fk_IN", M_Default_Branch_Fk).Direction = ParameterDirection.Input;
                _with3.Add("Pass_Word_IN", M_Pass_Word).Direction = ParameterDirection.Input;
                _with3.Add("Is_Activated_IN", M_Is_Activated).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = "FEEDERUSER.USER_MST_TBL_PKG.USER_MST_TBL_UPD";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
                //Manjunath  PTS ID:Sep-02  16/09/2011
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
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with4 = objWS.MyCommand.Parameters;
                _with4.Add("User_Mst_Pk_IN", M_User_Mst_Pk).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = "FEEDERUSER.USER_MST_TBL_PKG.USER_MST_TBL_DEL";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
                //Manjunath  PTS ID:Sep-02  16/09/2011
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

        #region "Other Functions"

        /// <summary>
        /// Determines whether [is user valid] [the specified user identifier].
        /// </summary>
        /// <param name="UserID">The user identifier.</param>
        /// <param name="Password">The password.</param>
        /// <param name="LocationPK">The location pk.</param>
        /// <param name="PCC">The PCC.</param>
        /// <returns></returns>
        public string IsUserValid(string UserID, string Password, double LocationPK = 0, double PCC = 0)
        {
            // If "0" is Returned then the User is not ther
            // If "1" is Returned then the Password is Wrong
            // IF "2" is Returned then the User is Not Active
            // If "3" is Returned then the User Do Not have the Menu Rights for the Location
            // If a String is Returned then it returns
            string strSQL = null;
            string strSQL1 = null;
            DataSet ObjDs = new DataSet();
            WorkFlow ObjWf = new WorkFlow();
            DataRow ObjDRw = null;
            string strUserPK = null;
            string strUsrName = null;
            string strLocName = null;
            string strLocTypeID = null;
            double dblEmpPK = 0;
            double dblCusPk = 0;
            string strDesignation = null;
            int Customer = 0;
            Customer = 0;
            int intBusinessType = 0;
            //QFOR 21 Oct

            try
            {
                strSQL = "";
                strSQL += " SELECT * ";
                strSQL += " FROM ( ";
                strSQL += " Select";
                strSQL += " UsrMst.User_Mst_Pk PK, ";
                strSQL += " decoder(VUM.PASS_WORD)  PWD, ";
                strSQL += " UsrMst.User_Name UsrName, ";
                strSQL += " UsrMst.Business_Type BusinessType, ";
                strSQL += " USRMST.DEFAULT_LOCATION_FK LOCFK, ";
                strSQL += " Nvl(UsrMst.Is_Activated, 0) IsActive, ";
                strSQL += " Nvl(UsrMst.Employee_Mst_Fk,0) EmpFK, ";
                strSQL += " Nvl(UsrMst.Customer_Mst_Fk,0) CustFK, ";
                strSQL += " NVL(Lt.Location_Type_Id,'') LocType, ";
                strSQL += " NVL(UPT.PWD_COUNT, '') PCC ";
                strSQL += " From";
                strSQL += " User_Mst_Tbl UsrMst, Location_Mst_Tbl loc, Location_Type_Mst_Tbl LT,USER_PASSWORD_TRN UPT,VIEW_USER_MST_TBL VUM ";
                strSQL += " where usrmst.default_location_fk = loc.location_mst_pk(+)";
                strSQL += " and loc.location_type_fk = lt.location_type_mst_pk";
                strSQL += " and usrmst.user_mst_pk=upt.user_mst_fk(+)";
                strSQL += " AND VUM.USER_MST_PK = USRMST.USER_MST_PK ";
                strSQL += " and UPPER(UsrMst.User_Id) = '" + UserID.ToUpper() + "'";
                strSQL += " and UsrMst.USER_TYPE=0";
                strSQL += " and vum.PASS_WARD_CHANGE_DT is not null";
                strSQL += " ORDER BY VUM.PASS_WARD_CHANGE_DT DESC )q ";
                strSQL += "WHERE rownum =1";
                ObjDs = ObjWf.GetDataSet(strSQL);

                if (ObjDs.Tables[0].Rows.Count == 0)
                {
                    strSQL = "";
                    strSQL += " SELECT * ";
                    strSQL += " FROM ( ";
                    strSQL += " Select";
                    strSQL += " UsrMst.User_Mst_Pk PK, ";
                    strSQL += " decoder(VUM.PASS_WORD)  PWD, ";
                    strSQL += " UsrMst.User_Name UsrName, ";
                    strSQL += " UsrMst.Business_Type BusinessType, ";
                    strSQL += " USRMST.DEFAULT_LOCATION_FK LOCFK, ";
                    strSQL += " Nvl(UsrMst.Is_Activated, 0) IsActive, ";
                    strSQL += " Nvl(UsrMst.Employee_Mst_Fk,0) EmpFK, ";
                    strSQL += " Nvl(UsrMst.Customer_Mst_Fk,0) CustFK, ";
                    strSQL += " NVL(Lt.Location_Type_Id,'') LocType, ";
                    strSQL += " NVL(UPT.PWD_COUNT, '') PCC ";
                    strSQL += " From";
                    strSQL += " User_Mst_Tbl UsrMst, Location_Mst_Tbl loc, Location_Type_Mst_Tbl LT,USER_PASSWORD_TRN UPT, VIEW_USER_MST_TBL VUM ";
                    strSQL += " where usrmst.default_location_fk = loc.location_mst_pk(+)";
                    strSQL += " and loc.location_type_fk = lt.location_type_mst_pk";
                    strSQL += " and usrmst.user_mst_pk=upt.user_mst_fk(+)";
                    strSQL += " and UPPER(UsrMst.User_Id) = '" + UserID.ToUpper() + "'";
                    strSQL += " and UsrMst.USER_TYPE=1";
                    strSQL += " and vum.PASS_WARD_CHANGE_DT is not null";
                    strSQL += " ORDER BY VUM.PASS_WARD_CHANGE_DT DESC )q ";
                    strSQL += "WHERE rownum <=1";
                    strSQL += "ORDER BY rownum";
                    ObjDs = ObjWf.GetDataSet(strSQL);
                    Customer = 1;
                }

                if (ObjDs.Tables[0].Rows.Count == 0)
                {
                    return "0";
                }
                else
                {
                    ObjDRw = ObjDs.Tables[0].Rows[0];
                    if (Convert.ToString(ObjDRw["IsActive"]) == "0")
                    {
                        return "2";
                    }
                    if ((Password.Length < 7))
                    {
                        return "4";
                    }
                    if (Password != Convert.ToString(ObjDRw["Pwd"]))
                    {
                        strSQL1 = " UPDATE USER_MST_TBL UMT SET UMT.WRONG_PWD_COUNT = NVL(UMT.WRONG_PWD_COUNT, 0) + 1 WHERE UPPER(UMT.USER_ID) = '" + UserID.ToUpper() + "'";
                        ObjWf.ExecuteCommands(strSQL1);
                        return "1";
                    }
                }
                strSQL1 = " UPDATE USER_MST_TBL UMT SET UMT.WRONG_PWD_COUNT = 0 WHERE UPPER(UMT.USER_ID) = '" + UserID.ToUpper() + "'";
                ObjWf.ExecuteCommands(strSQL1);

                strUserPK = Convert.ToString(ObjDRw["PK"]);
                strUsrName = Convert.ToString(ObjDRw["UsrName"]);
                dblEmpPK = Convert.ToDouble(ObjDRw["EmpFk"]);
                dblCusPk = Convert.ToDouble(ObjDRw["CustFk"]);
                intBusinessType = Convert.ToInt32(ObjDRw["BusinessType"]);
                //QFOR 21 Oct
                strLocTypeID = Convert.ToString(ObjDRw["LocType"]);
                //modyfying by thiyagarajan on 10/2/09 : implementing logged loc. time from server's time using GMT
                //timezone = CStr(ObjDRw.Item("TIME_ZONE"))
                if (LocationPK == 0)
                {
                    LocationPK = Convert.ToDouble(ObjDRw["LOCFK"]);
                }
                strDesignation = "";
                if (Customer == 0)
                {
                    if (dblEmpPK != 0)
                    {
                        strSQL = string.Empty;
                        strSQL += "SELECT  ";
                        strSQL += "emp.employee_name EmpName, ";
                        strSQL += "Desig.Designation_Name Designation ";
                        strSQL += "FROM ";
                        strSQL += "employee_mst_tbl Emp, ";
                        strSQL += "Designation_Mst_Tbl Desig ";
                        strSQL += "WHERE emp.employee_mst_pk = " + dblEmpPK + " ";
                        strSQL += "AND Emp.Designation_Mst_Fk = Desig.Designation_Mst_Pk ";
                        ObjDs = ObjWf.GetDataSet(strSQL);
                        ObjDRw = ObjDs.Tables[0].Rows[0];
                        strDesignation = Convert.ToString(ObjDRw["Designation"]);
                    }
                }

                strSQL = string.Empty;
                strSQL += "SELECT  ";
                strSQL += "T.LOCATION_NAME LOCNAME ";
                strSQL += "FROM  ";
                strSQL += "LOCATION_MST_TBL T  ";
                strSQL += "WHERE T.LOCATION_MST_PK = " + LocationPK + " ";
                ObjDs = ObjWf.GetDataSet(strSQL);
                ObjDRw = ObjDs.Tables[0].Rows[0];
                strLocName = Convert.ToString(ObjDRw["LOCNAME"]);

                strSQL = "";
                strSQL += " select ";
                strSQL += " count(*) AccCnt ";
                strSQL += " from ";
                strSQL += " user_access_trn AcTrn";
                strSQL += " where";
                strSQL += " Actrn.Location_Mst_Fk = " + LocationPK + " ";
                strSQL += " and actrn.user_mst_fk = " + strUserPK + " ";
                ObjDs = ObjWf.GetDataSet(strSQL);
                if (ObjDs.Tables[0].Rows.Count == 0)
                {
                    return "3";
                }
                else
                {
                    strSQL = "";
                    //Return strUsrName & "|" & strUserPK & "|" & CStr(LocationPK) & "|" & strDesignation & "|" & strLocName
                    //modyfying by thiyagarajan on 10/2/09 : implementing logged loc. time from server's time using GMT
                    if (Customer == 0)
                    {
                        return strUsrName + "|" + strUserPK + "|" + Convert.ToString(LocationPK) + "|" + strDesignation + "|" + strLocName + "|" + Convert.ToString(intBusinessType) + "|" + strLocTypeID + "|" + 0;
                        //& "|" & timezone 'QFOR 21 Oct
                    }
                    if (Customer == 1)
                    {
                        return strUsrName + "|" + strUserPK + "|" + Convert.ToString(LocationPK) + "|" + strDesignation + "|" + strLocName + "|" + Convert.ToString(intBusinessType) + "|" + strLocTypeID + "|" + 1;
                        //& "|" & timezone 'QFOR 21 Oct
                    }
                }
                //end
                //Manjunath  PTS ID:Sep-02  16/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "5";
            }
            return "";
        }

        #endregion "Other Functions"

        #region "Other Functions1"

        /// <summary>
        /// Determines whether [is user valid1] [the specified user identifier].
        /// </summary>
        /// <param name="UserID">The user identifier.</param>
        /// <param name="Password">The password.</param>
        /// <param name="LocationPK">The location pk.</param>
        /// <returns></returns>
        public bool IsUserValid1(string UserID, string Password, int LocationPK = 0)
        {
            string strSQL = null;
            OracleDataReader objReader = default(OracleDataReader);
            if (LocationPK == 0)
            {
                try
                {
                    strSQL += "SELECT  *  ";
                    strSQL += "FROM    USER_MST_TBL  ";
                    strSQL += "WHERE   upper(USER_ID)='" + UserID.ToUpper() + "'  ";
                    strSQL += "AND     decoder(PASS_WORD) ='" + Password + "' ";
                    strSQL += "AND     IS_ACTIVATED =1";
                    WorkFlow objWK = new WorkFlow();
                    objReader = objWK.GetDataReader(strSQL);
                    if (objReader.HasRows)
                    {
                        while (objReader.Read())
                        {
                            User_Mst_Pk = Convert.ToInt16(objReader["USER_MST_PK"]);
                            User_Id = Convert.ToString(objReader["USER_ID"]);
                            User_Name = Convert.ToString(objReader["USER_NAME"]);
                            break; // TODO: might not be correct. Was : Exit While
                        }
                        strSQL = "SELECT * FROM USER_ACCESS_TRN WHERE USER_MST_FK=" + User_Mst_Pk;
                        objReader = objWK.GetDataReader(strSQL);
                        if (objReader.HasRows)
                        {
                            MenuAvailableForUser = true;
                        }
                        else
                        {
                            MenuAvailableForUser = false;
                        }
                        objWK.MyCommand.Cancel();
                        objWK.MyConnection.Close();
                        return true;
                    }
                    else
                    {
                        objWK.MyCommand.Cancel();
                        objWK.MyConnection.Close();
                        return false;
                    }
                    //Manjunath  PTS ID:Sep-02  16/09/2011
                }
                catch (OracleException OraExp)
                {
                    throw OraExp;
                }
                catch (Exception ex)
                {
                    return false;
                }
            }
            else
            {
                try
                {
                    strSQL += "SELECT  *  ";
                    strSQL += "FROM    USER_MST_TBL  ";
                    strSQL += "WHERE   USER_ID='" + UserID + "'  ";
                    strSQL += "AND     decoder(PASS_WORD) ='" + Password + "' ";
                    strSQL += "AND     IS_ACTIVATED =1";
                    WorkFlow objWK = new WorkFlow();
                    objWK.MyCommand.Cancel();
                    objWK.MyConnection.Close();
                    return objWK.GetDataReader(strSQL).HasRows;
                    //Manjunath  PTS ID:Sep-02  16/09/2011
                }
                catch (OracleException OraExp)
                {
                    throw OraExp;
                }
                catch (Exception ex)
                {
                    return false;
                }
            }
        }

        #endregion "Other Functions1"

        #region "Fetch Employee for user Function"

        /// <summary>
        /// Fetches the emp for user.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <returns></returns>
        public Int64 FetchEmpForUser(Int16 UserPK = 0)
        {
            string strSQL = null;
            strSQL += " SELECT ";
            strSQL += " NVL(EMPLOYEE_MST_FK ,0) ";
            strSQL += " from User_MST_TBL";
            strSQL += " Where User_Mst_Pk=" + UserPK;

            WorkFlow objWF = new WorkFlow();
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                //Throw sqlExp
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                //Throw exp
            }
            return 0;
        }

        #endregion "Fetch Employee for user Function"

        #region "Fetch Customer for user Function"

        /// <summary>
        /// Fetches the customer for user.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <returns></returns>
        public Int64 FetchCustForUser(Int16 UserPK = 0)
        {
            string strSQL = null;
            strSQL += " SELECT ";
            strSQL += " NVL(Customer_MST_FK ,0) ";
            strSQL += " from User_MST_TBL";
            strSQL += " Where User_Mst_Pk=" + UserPK;

            WorkFlow objWF = new WorkFlow();
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(strSQL));
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

        #endregion "Fetch Customer for user Function"

        #region "Fetch ROLEFK for user Function"

        /// <summary>
        /// Fetches the role for user.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <returns></returns>
        public Int64 FetchROLEForUser(Int16 UserPK = 0)
        {
            string strSQL = null;
            strSQL += " SELECT ";
            strSQL += " NVL(ROLE_MST_FK ,0) ";
            strSQL += " from User_MST_TBL";
            strSQL += " Where User_Mst_Pk=" + UserPK;

            WorkFlow objWF = new WorkFlow();
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(strSQL));
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

        #endregion "Fetch ROLEFK for user Function"

        #region "Fetch User Task List"

        /// <summary>
        /// Fetches the message summary.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <returns></returns>
        public DataSet FetchMessageSummary(Int32 UserPK)
        {
            string strSQL = null;
            strSQL = string.Empty;
            strSQL += " SELECT folderFk, FolderName, sum(NewMsg) || '/' || sum(total) Msgs ";
            strSQL += " FROM ";
            strSQL += " (SELECT umf.user_message_folders_pk folderFk, ";
            strSQL += " umf.folder_name FolderName,  ";
            strSQL += " COUNT(um.user_message_pk) NewMsg, ";
            strSQL += " 0  total ";
            strSQL += " FROM  ";
            strSQL += " User_Message_Trn UM, ";
            strSQL += " User_Message_Folders_Trn umf ";
            strSQL += " WHERE   ";
            strSQL += " nvl(um.user_message_folders_fk(+),1) =umf.user_message_folders_pk ";
            strSQL += " AND um.msg_read(+) = 0 AND UM.DELETE_FLAG(+) is null And um.receiver_fk(+) = " + UserPK;
            strSQL += " GROUP BY umf.folder_name , umf.user_message_folders_pk ";
            strSQL += " UNION ";
            strSQL += " SELECT umf.user_message_folders_pk folderFk, ";
            strSQL += " umf.folder_name FolderName,  ";
            strSQL += " 0 NewMsg, ";
            strSQL += " COUNT(um.user_message_pk) Total ";
            strSQL += " FROM  ";
            strSQL += " User_Message_Trn UM,  ";
            strSQL += " User_Message_Folders_Trn umf ";
            strSQL += " WHERE  UM.DELETE_FLAG(+) is null And ";
            strSQL += " nvl(um.user_message_folders_fk(+),1) =umf.user_message_folders_pk ";
            strSQL += " AND um.receiver_fk(+) = " + UserPK;
            strSQL += " GROUP BY umf.folder_name , umf.user_message_folders_pk ";
            strSQL += " ) ";
            strSQL += " GROUP BY folderFk, FolderName ";
            strSQL += " ORDER BY folderFk, FolderName ";
            WorkFlow objWF = new WorkFlow();
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

        #endregion "Fetch User Task List"

        #region "Fetch Message"

        /// <summary>
        /// Fetches the message.
        /// </summary>
        /// <param name="FolderPK">The folder pk.</param>
        /// <param name="UserPK">The user pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="sCol">The s col.</param>
        /// <returns></returns>
        public DataSet FetchMessage(Int32 FolderPK, Int32 UserPK, Int32 CurrentPage, Int32 TotalPage, Int32 sCol)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string searchQuery = null;

            if (FolderPK == -1)
            {
                searchQuery = " AND Usrmsg.del_sender_flag is null And Usrmsg.delete_flag is null  And usrmsg.Sender_FK= " + UserPK;
            }
            else if (FolderPK == -2)
            {
                searchQuery = " AND Usrmsg.delete_flag=1 And usrmsg.Sender_FK= " + UserPK;
            }
            else
            {
                searchQuery = " AND UsrMsg.DELETE_FLAG is Null And usrmsg.user_message_folders_fk = " + FolderPK + " AND usrmsg.receiver_fk = " + UserPK;
            }

            strSQL = "SELECT Count(*) ";
            strSQL += " FROM ";
            strSQL += " user_message_trn UsrMsg,";
            strSQL += " user_mst_tbl Usr ";
            strSQL += " WHERE   ";
            strSQL += " usrmsg.sender_fk = usr.user_mst_pk ";
            strSQL += searchQuery;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            if (sCol == 0)
                sCol = 2;

            strSQL = " Select * From (";
            strSQL = strSQL + " SELECT ROWNUM SR_NO,qry.* FROM ";
            strSQL += "( Select ";
            strSQL += " usrmsg.user_message_pk, ";
            strSQL += " '' Del, ";
            strSQL += " usrmsg.have_attachment HvAttch, ";
            strSQL += " usrmsg.followup_flag FollowFlg, ";
            strSQL += " usrmsg.sender_fk SenderFk,";
            strSQL += " usr.user_name Sender,";
            strSQL += " usrmsg.msg_subject MsgSub, ";
            strSQL += " usrmsg.msg_received_dt ReceiveDt,";
            strSQL += " usrmsg.msg_read,";
            strSQL += " usrmsg.read_receipt";
            strSQL += " FROM ";
            strSQL += " user_message_trn UsrMsg,";
            strSQL += " user_mst_tbl Usr Where ";
            //strSQL &= vbCrLf & " WHERE UsrMsg.DELETE_FLAG is Null " 'AND usrmsg.receiver_fk = " & UserPK
            strSQL += " usrmsg.sender_fk = usr.user_mst_pk ";
            strSQL += searchQuery;
            strSQL += " Order By " + sCol;
            //strSQL &= vbCrLf & " AND usrmsg.user_message_folders_fk = " & FolderPK
            strSQL += " ) qry ) WHERE SR_NO  Between " + start + " and " + last;
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

        #endregion "Fetch Message"

        #region "Display Message"

        /// <summary>
        /// Displays the message.
        /// </summary>
        /// <param name="MessagePK">The message pk.</param>
        /// <returns></returns>
        public DataSet DisplayMessage(Int32 MessagePK)
        {
            string strSQL = null;

            strSQL += " SELECT ";
            strSQL += " usrmsg.sender_fk,";
            strSQL += " usr.user_id MsgFrom,";
            strSQL += " usr1.user_id MsgTo,";
            strSQL += " usrmsg.msg_received_dt,";
            strSQL += " usrmsg.msg_subject,";
            strSQL += " usrmsg.msg_body";
            strSQL += " FROM user_message_trn usrMsg, ";
            strSQL += " User_Mst_Tbl usr,";
            strSQL += " user_mst_tbl usr1";
            strSQL += " WHERE  UsrMsg.DELETE_FLAG is Null AND ";
            strSQL += " usrMsg.User_Message_Pk = " + MessagePK;
            strSQL += " AND usr.user_mst_pk = usrmsg.sender_fk";
            strSQL += " AND usr1.user_mst_pk = usrmsg.receiver_fk";
            WorkFlow objWF = new WorkFlow();
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
        /// Displays the message detail.
        /// </summary>
        /// <param name="MessagePK">The message pk.</param>
        /// <returns></returns>
        public DataSet DisplayMessageDetail(Int32 MessagePK)
        {
            string strSQL = null;
            strSQL += " SELECT ";
            strSQL += " msgdet.attachment_caption,";
            strSQL += " msgdet.attachment_data,";
            strSQL += " msgdet.url_page";
            strSQL += " from user_message_det_trn msgDet";
            strSQL += " where msgdet.user_message_fk = " + MessagePK;

            WorkFlow objWF = new WorkFlow();
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

        #endregion "Display Message"

        #region "Change Password"

        /// <summary>
        /// Changes the password.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <param name="OldPwd">The old password.</param>
        /// <param name="NewPwd">The new password.</param>
        /// <param name="from">From.</param>
        /// <returns></returns>
        public string ChangePassword(double UserPK, string OldPwd, string NewPwd, string @from)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand insCommand = new OracleCommand();

            try
            {
                int i = 0;
                var _with10 = insCommand;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".CHANGE_PWD_PKG.CHANGE_PWD";
                var _with11 = _with10.Parameters;
                _with11.Add("USER_PK_IN", UserPK).Direction = ParameterDirection.Input;
                _with11.Add("OLD_PWD_IN", OldPwd).Direction = ParameterDirection.Input;
                _with11.Add("NEW_PWD_IN", NewPwd).Direction = ParameterDirection.Input;
                _with11.Add("FROM_IN", @from).Direction = ParameterDirection.Input;

                var _with12 = objWK.MyDataAdapter;
                _with12.InsertCommand = insCommand;
                _with12.InsertCommand.Transaction = TRAN;
                _with12.InsertCommand.ExecuteNonQuery();
                TRAN.Commit();
                if (arrMessage.Count > 0)
                {
                    return "1";
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    return "OK";
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                if (string.Compare(Convert.ToString(arrMessage[0]), "ORA-20777") > 0)
                {
                    return "1";
                }
                else if (string.Compare(Convert.ToString(arrMessage[0]), "ORA-20778") > 0)
                {
                    return "2";
                }
                //  Throw oraexp
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
            return "";
        }

        #region "Fetch UserId"

        /// <summary>
        /// Fetches the user identifier.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <returns></returns>
        public string FetchUserID(double UserPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader objDR = default(OracleDataReader);
            try
            {
                strSQL += " SELECT ";
                strSQL += " user_id ";
                strSQL += " from User_MST_TBL";
                strSQL += " Where User_Mst_Pk=" + UserPK;
                return Convert.ToString(objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
            }
            return "";
        }

        #endregion "Fetch UserId"

        /// <summary>
        /// Changes the password1.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <param name="NewPwd">The new password.</param>
        /// <returns></returns>
        public string ChangePassword1(long UserPK, string NewPwd)
        {
            try
            {
                string Strsql = null;
                DateTime todate = default(DateTime);
                todate = System.DateTime.Now;
                WorkFlow ObjWf = new WorkFlow();
                Strsql = string.Empty;
                Strsql += "UPDATE ";
                Strsql += "User_Mst_Tbl x ";
                Strsql += "SET x.pass_word = encoder('" + NewPwd + "'),";
                //Strsql &= "x.pass_ward_change_dt = to_date('" & DateTime.Now & "','mm/dd/yyyy HH24:MI:SS')" & vbCrLf
                //Strsql &= "x.pass_ward_change_dt =to_date('" & todate & "', 'mm/dd/yyyy HH:MI:SS A.M.')" & vbCrLf
                Strsql += "x.last_modified_dt = sysdate ";
                Strsql += " where x.user_mst_pk = " + UserPK + " ";
                if (ObjWf.ExecuteCommands(Strsql))
                {
                    return "OK";
                }
                //Manjunath  PTS ID:Sep-02  16/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            return "";
        }

        #endregion "Change Password"

        #region "Check Password Expiry"

        /// <summary>
        /// Chkpasswords the expiry.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <returns></returns>
        public System.DateTime chkpasswordExpiry(int UserPK)
        {
            System.DateTime functionReturnValue = default(System.DateTime);
            string strSql = null;
            DataSet dsdata = null;
            WorkFlow ObjWf = new WorkFlow();
            try
            {
                strSql = " select UMT.CH_DT,UMT.created_dt ";
                strSql += "from USER_MST_TBL UMT ";
                strSql += "where UMT.user_mst_pk =" + UserPK + "";
                dsdata = ObjWf.GetDataSet(strSql);
                var _with13 = dsdata.Tables[0];
                if (_with13.Rows.Count > 0)
                {
                    functionReturnValue = Convert.ToDateTime(_with13.Rows[0]["CH_DT"]);
                }
            }
            catch (Exception ex)
            {
            }
            return functionReturnValue;
        }

        #endregion "Check Password Expiry"

        #region "ENHANCE SEARCH"

        /// <summary>
        /// Fetches the user.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchUser(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strLOC_MST_IN = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            strBizType = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_USER_PKG.GETUSER";

                var _with14 = selectCommand.Parameters;
                _with14.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : null)).Direction = ParameterDirection.Input;
                _with14.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input
                _with14.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : null)).Direction = ParameterDirection.Input;
                _with14.Add("BIZTYPE_IN", (!string.IsNullOrEmpty(strBizType) ? strBizType : null)).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  16/09/2011
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

        #endregion "ENHANCE SEARCH"

        #region "FetchUserName"

        /// <summary>
        /// Fetches the name of the user.
        /// </summary>
        /// <param name="selectId">The select identifier.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchUserName(string selectId, string SortColumn, Int32 CurrentPage, Int32 TotalPage)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQLBuilder = null;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            Int32 TotalRecords = default(Int32);
            string strCount = null;
            if (selectId.Trim().Length > 1)
            {
                selectId = selectId.TrimEnd(',');
                selectId = selectId.Replace(",", "','");
                selectId = "'" + selectId + "'";
            }
            //'fetch non-selected users
            strSQL = " SELECT * FROM ";
            strSQL = strSQL + "( SELECT DISTINCT";
            strSQL = strSQL + " USER1.USER_MST_PK, ";
            strSQL = strSQL + "USER1.USER_ID, ";
            strSQL = strSQL + "USER1.USER_NAME,";
            strSQL = strSQL + " '0' CHK";
            strSQL = strSQL + " FROM ";
            strSQL = strSQL + " USER_MST_TBL USER1, ";
            strSQL = strSQL + " LOCATION_MST_TBL LOC ";
            strSQL = strSQL + " WHERE 1=1 AND USER1.DEFAULT_LOCATION_FK=LOC.LOCATION_MST_PK ";
            if (selectId.Trim().Length > 1)
            {
                strSQL = strSQL + " AND USER_ID NOT in (" + selectId + ")";
                //'fetch selected users as check box equals checked
                strSQL = strSQL + "UNION";
                strSQL = strSQL + " SELECT DISTINCT";
                strSQL = strSQL + " USER2.USER_MST_PK, ";
                strSQL = strSQL + "USER2.USER_ID, ";
                strSQL = strSQL + "USER2.USER_NAME, ";
                strSQL = strSQL + " '1' CHK";
                strSQL = strSQL + " FROM ";
                strSQL = strSQL + " USER_MST_TBL USER2, ";
                strSQL = strSQL + " LOCATION_MST_TBL LOC ";
                strSQL = strSQL + "WHERE 1=1 AND USER2.DEFAULT_LOCATION_FK=LOC.LOCATION_MST_PK ";
                strSQL = strSQL + " AND USER_ID in (" + selectId + ")";
            }
            //If userid.Trim.Length > 0 Then
            //    strSQL = strSQL & " AND USER_ID LIKE '%" & userid & "%'"
            //End If
            //If username.Trim.Length > 0 Then
            //    strSQL = strSQL & " AND USER_NAME LIKE '%" & username & "%'"
            //End If
            strSQL = strSQL + " ) T ORDER BY USER_ID ASC ";
            //Count the  Records
            strCount = strCount + " SELECT COUNT(*)";
            strCount = strCount + "  FROM ";
            strCount = strCount + " (" + strSQL.ToString() + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
            TotalPage = TotalRecords / M_MasterPageSize;

            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            strSQLBuilder = strSQLBuilder + " SELECT  qry.* FROM ";
            strSQLBuilder = strSQLBuilder + " ( SELECT ROWNUM SR_NO,T.* FROM ";
            strSQLBuilder = strSQLBuilder + " (" + strSQL.ToString() + ") ";
            strSQLBuilder = strSQLBuilder + " T ) qry  WHERE SR_NO  Between " + start + " and " + last + " ";
            //strSQLBuilder = strSQLBuilder & " ORDER BY CHK DESC, USER_ID "

            try
            {
                return (new WorkFlow()).GetDataSet(strSQLBuilder.ToString());
                //Manjunath  PTS ID:Sep-02  16/09/2011
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

        #endregion "FetchUserName"

        #region "Fetch Wrong Password Count"

        /// <summary>
        /// Fetches the wrong password.
        /// </summary>
        /// <param name="UserID">The user identifier.</param>
        /// <returns></returns>
        public Int64 FetchWrongPwd(string UserID)
        {
            string strSQL = null;
            strSQL += " SELECT ";
            strSQL += " UMT.WRONG_PWD_COUNT  ";
            strSQL += " FROM USER_MST_TBL UMT";
            strSQL += " WHERE UMT.USER_ID='" + UserID.ToUpper() + "'";

            WorkFlow objWF = new WorkFlow();
            OracleDataReader objDR = default(OracleDataReader);
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
            }
            return 0;
        }

        #endregion "Fetch Wrong Password Count"
    }
}