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
    public class cls_Employee_Mst_Table : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ employee_ MST_ pk
        /// </summary>
        private Int64 M_Employee_Mst_Pk;

        /// <summary>
        /// The m_ employee_ identifier
        /// </summary>
        private string M_Employee_Id;

        /// <summary>
        /// The m_ employee_ name
        /// </summary>
        private string M_Employee_Name;

        /// <summary>
        /// The m_ location_ TBL_ fk
        /// </summary>
        private Int64 M_Location_Tbl_Fk;

        /// <summary>
        /// The m_ reporting_ to_ id_ fk
        /// </summary>
        private Int64 M_Reporting_To_Id_Fk;

        /// <summary>
        /// The m_ location_ identifier
        /// </summary>
        private string M_Location_Id;

        #endregion "List of Members of the Class"

        /// <summary>
        /// The m_ reporting_ to
        /// </summary>
        private string M_Reporting_To;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the reporting_to.
        /// </summary>
        /// <value>
        /// The reporting_to.
        /// </value>
        public string reporting_to
        {
            get { return M_Reporting_To; }
            set { M_Reporting_To = value; }
        }

        /// <summary>
        /// Gets or sets the employee_ MST_ pk.
        /// </summary>
        /// <value>
        /// The employee_ MST_ pk.
        /// </value>
        public Int64 Employee_Mst_Pk
        {
            get { return M_Employee_Mst_Pk; }
            set { M_Employee_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the employee_ identifier.
        /// </summary>
        /// <value>
        /// The employee_ identifier.
        /// </value>
        public string Employee_Id
        {
            get { return M_Employee_Id; }
            set { M_Employee_Id = value; }
        }

        /// <summary>
        /// Gets or sets the name of the employee_.
        /// </summary>
        /// <value>
        /// The name of the employee_.
        /// </value>
        public string Employee_Name
        {
            get { return M_Employee_Name; }
            set { M_Employee_Name = value; }
        }

        /// <summary>
        /// Gets or sets the location_ TBL_ fk.
        /// </summary>
        /// <value>
        /// The location_ TBL_ fk.
        /// </value>
        public Int64 Location_Tbl_Fk
        {
            get { return M_Location_Tbl_Fk; }
            set { M_Location_Tbl_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the reporting_ to_ id_ fk.
        /// </summary>
        /// <value>
        /// The reporting_ to_ id_ fk.
        /// </value>
        public Int64 Reporting_To_Id_Fk
        {
            get { return M_Reporting_To_Id_Fk; }
            set { M_Reporting_To_Id_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the location_id.
        /// </summary>
        /// <value>
        /// The location_id.
        /// </value>
        public string Location_id
        {
            get { return M_Location_Id; }
            set { M_Location_Id = value; }
        }

        #endregion "List of Properties"

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
                var _with1 = objWS.MyCommand.Parameters;
                _with1.Add("Employee_Mst_Pk_IN", M_Employee_Mst_Pk).Direction = ParameterDirection.Input;
                _with1.Add("Employee_Id_IN", M_Employee_Id).Direction = ParameterDirection.Input;
                _with1.Add("Employee_Name_IN", M_Employee_Name).Direction = ParameterDirection.Input;
                _with1.Add("Location_Tbl_Fk_IN", M_Location_Tbl_Fk).Direction = ParameterDirection.Input;
                _with1.Add("Reporting_To_Id_Fk_IN", M_Reporting_To_Id_Fk).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = objWS.MyUserName.Trim() + ".Employee_MST_TBL_PKG.Employee_Mst_Tbl_Ins";
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
                var _with2 = objWS.MyCommand.Parameters;
                _with2.Add("Employee_Mst_Pk_IN", M_Employee_Mst_Pk).Direction = ParameterDirection.Input;
                _with2.Add("Employee_Id_IN", M_Employee_Id).Direction = ParameterDirection.Input;
                _with2.Add("Employee_Name_IN", M_Employee_Name).Direction = ParameterDirection.Input;
                _with2.Add("Location_Tbl_Fk_IN", M_Location_Tbl_Fk).Direction = ParameterDirection.Input;
                _with2.Add("Reporting_To_Id_Fk_IN", M_Reporting_To_Id_Fk).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = objWS.MyUserName.Trim() + ".Employee_MST_TBL_PKG.Employee_Mst_Tbl_UPD";
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
                var _with3 = objWS.MyCommand.Parameters;
                objWS.MyCommand.CommandText = objWS.MyUserName.Trim() + ".Employee_MST_TBL_PKG.Employee_Mst_Tbl_DEL";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #region "Fetch All Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="EmpId">The emp identifier.</param>
        /// <param name="EmpName">Name of the emp.</param>
        /// <param name="FirstName">The first name.</param>
        /// <param name="LastName">The last name.</param>
        /// <param name="LocId">The loc identifier.</param>
        /// <param name="LocName">Name of the loc.</param>
        /// <param name="DepartmentID">The department identifier.</param>
        /// <param name="DepartmentName">Name of the department.</param>
        /// <param name="DesignationID">The designation identifier.</param>
        /// <param name="DesignationName">Name of the designation.</param>
        /// <param name="StateId">The state identifier.</param>
        /// <param name="StateName">Name of the state.</param>
        /// <param name="Address1">The address1.</param>
        /// <param name="Address2">The address2.</param>
        /// <param name="City">The city.</param>
        /// <param name="ZIP">The zip.</param>
        /// <param name="Phone">The phone.</param>
        /// <param name="Mobile">The mobile.</param>
        /// <param name="EMail">The e mail.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="intBusType">Type of the int bus.</param>
        /// <param name="intUser">The int user.</param>
        /// <param name="intActive">The int active.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public string FetchAll(string EmpId, string EmpName, string FirstName, string LastName, string LocId, string LocName, string DepartmentID, string DepartmentName, string DesignationID, string DesignationName,
        string StateId, string StateName, string Address1, string Address2, string City, string ZIP, string Phone, string Mobile, string EMail, string SearchType,
        string strColumnName, Int32 CurrentPage, Int32 TotalPage, int intBusType, int intUser, int intActive, bool blnSortAscending, Int32 flag)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Web.UI.Page objPage = new System.Web.UI.Page();

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (SearchType == "S")
            {
                if (EmpId.Length > 0)
                {
                    strCondition = strCondition + " AND upper(emp.employee_id) like '" + EmpId.ToUpper().Replace("'", "''") + "%'";
                }
                if (EmpName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(emp.employee_name) like '" + EmpName.ToUpper().Replace("'", "''") + "%'";
                }
                if (FirstName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(emp.first_name) like '" + FirstName.ToUpper().Replace("'", "''") + "%'";
                }
                if (LastName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(emp.last_name) like '" + LastName.ToUpper().Replace("'", "''") + "%'";
                }
                if (LocId.Length > 0)
                {
                    strCondition = strCondition + " AND upper(loc.location_ID) like '" + LocId.ToUpper().Replace("'", "''") + "%'";
                }
                if (LocName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(loc.location_Name) like '" + LocName.ToUpper().Replace("'", "''") + "%'";
                }
                if (StateId.Length > 0)
                {
                    strCondition = strCondition + " AND upper(STATE.STATE_ID) like '" + StateId.ToUpper().Replace("'", "''") + "%'";
                }
                if (StateName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(STATE.STATE_NAME) like '" + StateName.ToUpper().Replace("'", "''") + "%'";
                }
                if (DepartmentID.Length > 0)
                {
                    strCondition = strCondition + " AND upper(DEPT.DEPARTMENT_ID) like '" + DepartmentID.ToUpper().Replace("'", "''") + "%'";
                }
                if (DepartmentName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(DEPT.DEPARTMENT_NAME) like '" + DepartmentName.ToUpper().Replace("'", "''") + "%'";
                }
                if (DesignationID.Length > 0)
                {
                    strCondition = strCondition + " AND upper(DESG.DESIGNATION_ID) like '" + DesignationID.ToUpper().Replace("'", "''") + "%'";
                }
                if (DesignationName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(DESG.DESIGNATION_NAME) like '" + DesignationName.ToUpper().Replace("'", "''") + "%'";
                }
                if (Address1.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.ADDRESS1) like '" + Address1.ToUpper().Replace("'", "''") + "%'";
                }
                if (Address2.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.ADDRESS2) like '" + Address2.ToUpper().Replace("'", "''") + "%'";
                }
                if (City.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.CITY) like '" + City.ToUpper().Replace("'", "''") + "%'";
                }
                if (Phone.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.PHONE) like '" + Phone.ToUpper().Replace("'", "''") + "%'";
                }
                if (Mobile.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.Mobile) like '" + Mobile.ToUpper().Replace("'", "''") + "%'";
                }
                if (EMail.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.EMAIL) like '" + EMail.ToUpper().Replace("'", "''") + "%'";
                }
                if (ZIP.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.ZIP) like '" + ZIP.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (EmpId.Length > 0)
                {
                    strCondition = strCondition + " AND upper(emp.employee_id) like '%" + EmpId.ToUpper().Replace("'", "''") + "%'";
                }
                if (EmpName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(emp.employee_name) like '%" + EmpName.ToUpper().Replace("'", "''") + "%'";
                }
                if (FirstName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(emp.first_name) like '" + FirstName.ToUpper().Replace("'", "''") + "%'";
                }
                if (LastName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(emp.last_name) like '" + LastName.ToUpper().Replace("'", "''") + "%'";
                }
                if (LocId.Length > 0)
                {
                    strCondition = strCondition + " AND upper(loc.location_ID) like '%" + LocId.ToUpper().Replace("'", "''") + "%'";
                }
                if (LocName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(loc.location_Name) like '%" + LocName.ToUpper().Replace("'", "''") + "%'";
                }
                if (StateId.Length > 0)
                {
                    strCondition = strCondition + " AND upper(STATE.STATE_ID) like '%" + StateId.ToUpper().Replace("'", "''") + "%'";
                }
                if (StateName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(STATE.STATE_NAME) like '%" + StateName.ToUpper().Replace("'", "''") + "%'";
                }
                if (DepartmentID.Length > 0)
                {
                    strCondition = strCondition + " AND upper(DEPT.DEPARTMENT_ID) like '%" + DepartmentID.ToUpper().Replace("'", "''") + "%'";
                }
                if (DepartmentName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(DEPT.DEPARTMENT_NAME) like '%" + DepartmentName.ToUpper().Replace("'", "''") + "%'";
                }
                if (DesignationID.Length > 0)
                {
                    strCondition = strCondition + " AND upper(DESG.DESIGNATION_ID) like '%" + DesignationID.ToUpper().Replace("'", "''") + "%'";
                }
                if (DesignationName.Length > 0)
                {
                    strCondition = strCondition + " AND upper(DESG.DESIGNATION_NAME) like '%" + DesignationName.ToUpper().Replace("'", "''") + "%'";
                }
                if (Address1.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.ADDRESS1) like '%" + Address1.ToUpper().Replace("'", "''") + "%'";
                }
                if (Address2.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.ADDRESS2) like '%" + Address2.ToUpper().Replace("'", "''") + "%'";
                }
                if (City.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.CITY) like '%" + City.ToUpper().Replace("'", "''") + "%'";
                }
                if (Phone.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.PHONE) like '%" + Phone.ToUpper().Replace("'", "''") + "%'";
                }
                if (Mobile.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.Mobile) like '%" + Mobile.ToUpper().Replace("'", "''") + "%'";
                }
                if (EMail.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.EMAIL) like '%" + EMail.ToUpper().Replace("'", "''") + "%'";
                }
                if (ZIP.Length > 0)
                {
                    strCondition = strCondition + " AND upper(EMP.ZIP) like '%" + ZIP.ToUpper().Replace("'", "''") + "%'";
                }
            }

            // to incorporate the BusinessType.
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

            if (intActive == 1)
            {
                strCondition += " AND TERMINATED = 1 ";
            }

            strSQL = "SELECT Count(*) ";
            strSQL += "from employee_mst_tbl EMP, location_mst_tbl LOC, ";
            strSQL += " DEPARTMENT_MST_TBL DEPT, DESIGNATION_MST_TBL DESG";
            strSQL += " where  ";
            strSQL += "loc.LOCATION_MST_PK=emp.LOCATION_MST_FK And ";
            strSQL += "DEPT.DEPARTMENT_MST_PK = EMP.DEPARTMENT_MST_FK AND ";
            strSQL += "DESG.DESIGNATION_MST_PK = EMP.DESIGNATION_MST_FK ";
            strSQL += "and loc.LOCATION_MST_PK in (SELECT DISTINCT LOCATION_MST_PK ";
            strSQL += "FROM LOCATION_MST_TBL ";
            strSQL += "start with location_mst_pk = " + 1841;
            strSQL += " connect by prior location_mst_pk = reporting_to_fk)";

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

            strSQL = " SELECT * FROM ";
            strSQL += "(SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL += " (SELECT ";
            strSQL += " EMP.EMPLOYEE_MST_PK EMPLOYEE_MST_PK,";
            strSQL += " EMP.TERMINATED TERMINATED,";
            strSQL += " EMP.EMPLOYEE_ID EMPLOYEE_ID,";
            strSQL += " EMP.EMPLOYEE_NAME EMPLOYEE_NAME, ";
            strSQL += " CASE WHEN EMP.FIRST_NAME IS NULL THEN";
            strSQL += " EMP.EMPLOYEE_NAME";
            strSQL += " ELSE ";
            strSQL += " EMP.FIRST_NAME END FIRST_NAME,";
            strSQL += " EMP.LAST_NAME,";
            strSQL += " EMP.LOCATION_MST_FK LOCATION_MST_FK,";
            strSQL += " LOC.LOCATION_ID LOCATION_ID,";
            strSQL += " LOC.LOCATION_NAME LOCATION_NAME,";

            strSQL += " DEPARTMENT_MST_FK, ";
            strSQL += " DEPARTMENT_NAME, ";
            strSQL += " DEPARTMENT_ID, ";
            strSQL += " DESIGNATION_MST_FK, ";
            strSQL += " DESIGNATION_NAME, ";
            strSQL += " DESIGNATION_ID, ";
            strSQL += " DECODE(EMP.EMP_TYPE, 0, 'Employee', 1, 'Management') EMP_TYPE, ";
            // strSQL &= vbCrLf & "TO_CHAR(JOIN_DATE,'" & dateFormat & "') JOIN_DATE,"
            strSQL += " JOIN_DATE JOIN_DATE,";
            strSQL += " EMP.Version_No, ";
            strSQL += " DECODE(EMP.BUSINESS_TYPE,1,'AIR',2,'SEA',3,'BOTH') BUSINESS_TYPE, ";
            strSQL += " EMP.EMAIL_ID, EMP.PHONE_NO,EMP.KEY_CONTACT ";

            strSQL += " FROM EMPLOYEE_MST_TBL EMP, LOCATION_MST_TBL LOC, ";
            strSQL += " STATE_MST_TBL STATE, DEPARTMENT_MST_TBL DEPT, DESIGNATION_MST_TBL DESG";
            strSQL += " where  ";
            strSQL += " loc.LOCATION_MST_PK=emp.LOCATION_MST_FK And ";
            strSQL += " DEPT.DEPARTMENT_MST_PK = EMP.DEPARTMENT_MST_FK AND ";
            strSQL += " DESG.DESIGNATION_MST_PK = EMP.DESIGNATION_MST_FK AND ";
            strSQL += " STATE.STATE_MST_PK(+) = EMP.STATE_MST_FK ";
            strSQL += " and loc.LOCATION_MST_PK in (SELECT DISTINCT LOCATION_MST_PK ";
            strSQL += " FROM LOCATION_MST_TBL ";
            strSQL += " start with location_mst_pk =  " + 1841;
            strSQL += " connect by prior location_mst_pk = reporting_to_fk)";

            strSQL += strCondition;

            if (!strColumnName.Equals("SR_NO"))
            {
                // strSQL &= vbCrLf & "order by " & strColumnName
                strSQL += "order by LOCATION_ID, FIRST_NAME, LAST_NAME ";
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL += " )q) WHERE SR_NO  Between " + start + " and " + last;

            try
            {
                return JsonConvert.SerializeObject(objWF.GetDataSet(strSQL), Formatting.Indented);
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

        #region "Fetch Function"

        /// <summary>
        /// Fetches the specified current pk value.
        /// </summary>
        /// <param name="CurrentPKValue">The current pk value.</param>
        /// <param name="EmployeeMSTPk">The employee MST pk.</param>
        /// <param name="EmployeeId">The employee identifier.</param>
        /// <param name="EmployeeName">Name of the employee.</param>
        /// <param name="LocationId">The location identifier.</param>
        /// <param name="ReportingTo">The reporting to.</param>
        /// <returns></returns>
        public bool Fetch(Int16 CurrentPKValue = 0, Int16 EmployeeMSTPk = 0, string EmployeeId = "", string EmployeeName = "", string LocationId = "", string ReportingTo = "")
        {
            string strSQL = null;

            strSQL += "select  ";
            strSQL += "emp.employee_mst_pk, ";
            strSQL += "emp.employee_id, ";
            strSQL += "emp.employee_name, ";
            strSQL += "loc.location_id, ";
            strSQL += "loc.location_name  ";
            strSQL += "from ";
            strSQL += "employee_mst_tbl emp,location_mst_tbl loc ";
            strSQL += "where  ";
            strSQL += "loc.location_mst_pk=emp.location_tbl_fk ";
            strSQL += "and  ";
            strSQL += "loc.reporting_to_fk=emp.reporting_to_id_fk ";
            strSQL += "and  ";
            strSQL += "1=1 ";

            if (EmployeeMSTPk > 0)
            {
                strSQL = strSQL + " AND emp.employee_mst_pk=" + EmployeeMSTPk;
            }
            if (EmployeeId.Length > 0)
            {
                strSQL = strSQL + " AND emp.employee_id=" + EmployeeId;
            }
            if (EmployeeName.Length > 0)
            {
                strSQL = strSQL + " AND emp.employee_name='" + EmployeeName + "'";
            }
            if (ReportingTo.Length > 0)
            {
                strSQL = strSQL + " AND emp.reporting_to_fk='" + ReportingTo + "'";
            }
            if (LocationId.Length > 0)
            {
                strSQL = strSQL + " AND loc.location_id='" + LocationId + "'";
            }

            WorkFlow objWF = new WorkFlow();
            DataSet objDS = new DataSet();

            try
            {
                objDS = objWF.GetDataSet(strSQL);
                if (objDS.Tables[0].Rows.Count > 0)
                {
                    Employee_Mst_Pk = (Int16)objDS.Tables[0].Rows[0]["Employee_Mst_Pk"];
                    Employee_Id = objDS.Tables[0].Rows[0]["employee_id"].ToString();
                    Employee_Name = objDS.Tables[0].Rows[0]["Employee_Name"].ToString();
                    Location_id = objDS.Tables[0].Rows[0]["Location_Id"].ToString();
                    reporting_to = objDS.Tables[0].Rows[0]["Location_Name"].ToString();
                    return true;
                }
                else
                {
                    return false;
                }
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

        #endregion "Fetch Function"

        /// <summary>
        /// Fetches the super usr.
        /// </summary>
        /// <returns></returns>
        public string FetchSuperUsr()
        {
            string strSQL = null;
            DataSet RetVal = new DataSet();
            WorkFlow objWK = new WorkFlow();
            strSQL = "select * from employee_mst_tbl emt where emt.super_user='1'";
            try
            {
                RetVal = objWK.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(RetVal, Formatting.Indented);
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Fetches the super adm.
        /// </summary>
        /// <param name="strempfk">The strempfk.</param>
        /// <returns></returns>
        public string FetchSuperAdm(string strempfk)
        {
            string strSQL = null;
            DataSet RetVal = new DataSet();
            WorkFlow objWK = new WorkFlow();
            strSQL = "select * from employee_mst_tbl emt where emt.employee_mst_pk=" + strempfk + "";
            try
            {
                RetVal = objWK.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(RetVal, Formatting.Indented);
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Fetches the user identifier.
        /// </summary>
        /// <param name="strUsrPk">The string usr pk.</param>
        /// <returns></returns>
        public string FetchUserID(string strUsrPk)
        {
            string strSQL = null;
            DataSet RetVal = new DataSet();
            WorkFlow objWK = new WorkFlow();
            strSQL = "select * from user_mst_tbl umt where umt.user_mst_pk=" + strUsrPk + "";
            try
            {
                RetVal = objWK.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(RetVal, Formatting.Indented);
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #region "Fetch Function"

        /// <summary>
        /// Fetches the emp details.
        /// </summary>
        /// <param name="EmployeePK">The employee pk.</param>
        /// <returns></returns>
        public string FetchEmpDetails(string EmployeePK)
        {
            string strSQL = null;

            strSQL += "select  ";
            strSQL += "emp.employee_mst_pk, ";
            strSQL += "emp.employee_id, ";
            strSQL += "emp.employee_name, ";
            strSQL += " CASE WHEN EMP.FIRST_NAME IS NULL THEN ";
            strSQL += " EMP.EMPLOYEE_NAME ";
            strSQL += " ELSE ";
            strSQL += " EMP.FIRST_NAME END FIRST_NAME,";
            strSQL += "emp.last_name, ";
            strSQL += "loc.location_mst_pk, ";
            strSQL += "usr.user_id, ";
            strSQL += "emp.email_id, ";
            strSQL += "emp.business_type, ";
            strSQL += "des.designation_mst_pk, ";
            strSQL += "dep.department_mst_pk, ";
            strSQL += "emp.join_date,  ";
            strSQL += "emp.terminated, ";
            strSQL += "emp.key_contact,  ";
            strSQL += "emp.Version_No,  ";
            strSQL += "emp.Phone_No,  ";
            strSQL += " emp.super_user, ";
            strSQL += "DECODE(EMP.EMP_TYPE, 0, '', 1, 'Management') As EMP_TYPE ";
            strSQL += "from ";
            strSQL += "employee_mst_tbl emp, location_mst_tbl loc, USER_MST_TBL usr, ";
            strSQL += "department_mst_tbl dep, designation_mst_tbl des ";

            strSQL += "where  ";
            strSQL += "loc.location_mst_pk = emp.location_mst_fk ";
            strSQL += "and  ";
            strSQL += "usr.employee_mst_fk(+) = emp.employee_mst_pk ";
            strSQL += "and  ";
            strSQL += "dep.department_mst_pk = emp.department_mst_fk ";
            strSQL += "and  ";
            strSQL += "des.designation_mst_pk = emp.designation_mst_fk ";
            strSQL = strSQL + " AND emp.employee_mst_pk = " + EmployeePK;

            WorkFlow objWF = new WorkFlow();
            DataSet objDS = new DataSet();

            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        #endregion "Fetch Function"

        #region "Fetch Employee Function"

        /// <summary>
        /// Fetches the employee.
        /// </summary>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="EmployeePK">The employee pk.</param>
        /// <param name="EmployeeID">The employee identifier.</param>
        /// <param name="EmployeeName">Name of the employee.</param>
        /// <returns></returns>
        public string FetchEmployee(long LocFK = 0, Int16 EmployeePK = 0, string EmployeeID = "", string EmployeeName = "")
        {
            string strSQL = null;
            strSQL = " SELECT ";
            strSQL += " ' ' EMPLOYEE_ID,";
            strSQL += " ' ' EMPLOYEE_NAME,";
            strSQL += " 0 EMPLOYEE_MST_PK ";
            strSQL += " FROM DUAL";
            strSQL += " UNION";
            strSQL += " SELECT ";
            strSQL += " Emp.EMPLOYEE_ID ,";
            strSQL += " Emp.EMPLOYEE_NAME,";
            strSQL += " Emp.EMPLOYEE_MST_PK ";
            strSQL += " from EMPLOYEE_MST_TBL Emp";

            if (LocFK > 0)
            {
                strSQL = strSQL + " where Emp.location_mst_fk=" + LocFK;
            }
            strSQL += " ORDER BY EMPLOYEE_NAME ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        #endregion "Fetch Employee Function"

        #region "Fetch Approver Employee Function" 'for ODC  & HZ Approval

        /// <summary>
        /// Fetches the approver employee.
        /// </summary>
        /// <param name="EmpAppPK">The emp application pk.</param>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="EmployeePK">The employee pk.</param>
        /// <param name="EmployeeID">The employee identifier.</param>
        /// <param name="EmployeeName">Name of the employee.</param>
        /// <returns></returns>
        public string FetchApproverEmployee(Int16 EmpAppPK, long LocFK = 0, Int16 EmployeePK = 0, string EmployeeID = "", string EmployeeName = "")
        {
            string strSQL = null;
            strSQL = " SELECT ";
            strSQL += " ' ' EMPLOYEE_ID,";
            strSQL += " ' ' EMPLOYEE_NAME,";
            strSQL += " 0 EMPLOYEE_MST_PK ";
            strSQL += " FROM DUAL";
            strSQL += " UNION";
            strSQL += " SELECT ";
            strSQL += " Emp.EMPLOYEE_ID ,";
            strSQL += " Emp.EMPLOYEE_NAME,";
            strSQL += " Emp.EMPLOYEE_MST_PK ";
            strSQL += " from EMPLOYEE_MST_TBL Emp";
            strSQL = strSQL + " where Emp.location_mst_fk=" + LocFK;
            strSQL = strSQL + " and emp.EMPLOYEE_MST_PK <>" + EmpAppPK;
            strSQL += " ORDER BY EMPLOYEE_ID ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        #endregion "Fetch Approver Employee Function" 'for ODC  & HZ Approval

        #endregion "Other Functions"

        #region "Dropdown population"

        #region "FetchLocation"

        /// <summary>
        /// Fetches the locationonly.
        /// </summary>
        /// <returns></returns>
        public string FetchLocationonly()
        {
            string strSQL = null;
            strSQL = " select * from ( ";
            strSQL += "SELECT  ROWNUM Sr_No,Abc.* FROM  ";
            strSQL += "(SELECT   ";
            strSQL += " loc.Location_Mst_Pk,";
            strSQL += " loc.Location_Id, ";
            strSQL += " loc.Location_Name ";
            strSQL += " FROM LOCATION_MST_TBL loc, COUNTRY_MST_TBL country,  ";
            strSQL += " LOCATION_TYPE_MST_TBL loctype, LOCATION_MST_TBL loc1  ";
            strSQL += "Where 1= 1 ";
            strSQL += " AND loc.Reporting_To_Fk= loc1.Location_Mst_Pk(+)  ";
            strSQL += " AND loc.Location_Type_Fk= loctype.Location_Type_Mst_Pk  ";
            strSQL += " AND loc.Country_Mst_FK= country.Country_Mst_PK AND loc.ACTIVE_FLAG = 1 order by loc.Location_ID ASC";
            strSQL += " ) ABC)";
            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        #endregion "FetchLocation"

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="EmployeePK">The employee pk.</param>
        /// <param name="EmployeeID">The employee identifier.</param>
        /// <param name="EmployeeName">Name of the employee.</param>
        /// <returns></returns>
        public string FetchLocation(long LocFK = 0, Int16 EmployeePK = 0, string EmployeeID = "", string EmployeeName = "")
        {
            string strSQL = null;
            strSQL = " SELECT LOCATION_NAME, LOCATION_ID, LOCATION_MST_PK, 1 ACTIVE";
            strSQL += " FROM LOCATION_MST_TBL S";
            strSQL += " WHERE S.LOCATION_MST_PK IN (" + LocFK + ")";
            strSQL += " UNION ";
            strSQL += " SELECT LOCATION_NAME, LOCATION_ID, LOCATION_MST_PK, 0 ACTIVE";
            strSQL += " FROM LOCATION_MST_TBL L";
            strSQL += " WHERE L.LOCATION_MST_PK NOT IN (" + LocFK + ")";
            strSQL += " ORDER BY ACTIVE DESC, LOCATION_ID";

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        #endregion "Dropdown population"

        #region "FetchDepartment"

        /// <summary>
        /// Fetches the departmentloc.
        /// </summary>
        /// <param name="LocFK">The loc fk.</param>
        /// <returns></returns>
        public string FetchDepartmentloc(long LocFK = 0)
        {
            string strSQL = null;
            strSQL = " Select ";
            strSQL += " dp.department_name,";
            strSQL += " dp.department_id,";
            strSQL += " dp.department_mst_pk,";
            strSQL += " l.location_mst_pk,";
            strSQL += " ldt.location_mst_fk";
            strSQL += " from department_mst_tbl dp,location_departments_trn ldt,location_mst_tbl l";
            strSQL += " where";
            strSQL += " l.location_mst_pk =" + LocFK;
            strSQL += " and ldt.location_mst_fk=l.location_mst_pk";
            strSQL += " and ldt.department_mst_fk=dp.department_mst_pk";
            strSQL += " and ldt.active = 1";

            strSQL += " order by department_id ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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
        /// Fetches the department.
        /// </summary>
        /// <param name="DepFK">The dep fk.</param>
        /// <param name="EmployeePK">The employee pk.</param>
        /// <param name="EmployeeID">The employee identifier.</param>
        /// <param name="EmployeeName">Name of the employee.</param>
        /// <returns></returns>
        public string FetchDepartment(long DepFK = 0, Int16 EmployeePK = 0, string EmployeeID = "", string EmployeeName = "")
        {
            string strSQL = null;
            strSQL = " SELECT ";
            strSQL += " department_name,";
            strSQL += " department_id,";
            strSQL += " department_mst_pk ";
            strSQL += " from department_mst_tbl";
            strSQL = strSQL + " where active_flag=1";
            if (DepFK > 0)
            {
                strSQL = strSQL + " AND location_mst_fk=" + DepFK;
            }

            strSQL += " order by department_id ";
            //active_flag

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        #endregion "FetchDepartment"

        #region "FetchDesignation"

        /// <summary>
        /// Fetches the designation dep.
        /// </summary>
        /// <param name="DepFK">The dep fk.</param>
        /// <returns></returns>
        public string FetchDesignationDep(long DepFK = 0)
        {
            string strSQL = null;
            strSQL = " SELECT ";
            strSQL += " designation_name,";
            strSQL += " designation_id,";
            strSQL += " designation_mst_pk";
            strSQL += " from department_mst_tbl dp,";
            strSQL += " dept_desig_trn dd,";
            strSQL += " designation_mst_tbl dm";
            strSQL += " where dp.department_mst_pk= " + DepFK;
            strSQL += "and dd.depart_mst_fk=dp.department_mst_pk";
            strSQL += "and dd.active=1";
            strSQL += "and dd.designation_mst_fk=dm.designation_mst_pk";
            strSQL += " order by designation_id ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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
        /// Fetches the designation.
        /// </summary>
        /// <param name="desigFK">The desig fk.</param>
        /// <param name="EmployeePK">The employee pk.</param>
        /// <param name="EmployeeID">The employee identifier.</param>
        /// <param name="EmployeeName">Name of the employee.</param>
        /// <returns></returns>
        public string FetchDesignation(long desigFK = 0, Int16 EmployeePK = 0, string EmployeeID = "", string EmployeeName = "")
        {
            string strSQL = null;
            strSQL = " SELECT ";
            strSQL += " designation_name,";
            strSQL += " designation_id,";
            strSQL += " designation_mst_pk ";
            strSQL += " from designation_mst_tbl";
            strSQL = strSQL + " where active_flag=1";
            if (desigFK > 0)
            {
                strSQL = strSQL + " AND location_mst_fk=" + desigFK;
            }

            strSQL += " order by designation_id ";
            //active_flag

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        #endregion "FetchDesignation"

        #region "Fetch Designation"

        /// <summary>
        /// Fetches the desig on department.
        /// </summary>
        /// <param name="DepartmentFK">The department fk.</param>
        /// <returns></returns>
        public string FetchDesigOnDepartment(int DepartmentFK = 0)
        {
            string strSQL = "";
            WorkFlow objWF = new WorkFlow();

            strSQL = "select DISTINCT DES.DESIGNATION_MST_PK, DES.DESIGNATION_ID, DES.DESIGNATION_NAME ";
            strSQL += " from DESIGNATION_MST_TBL DES INNER JOIN DEPT_DESIG_TRN DDT ON ";
            strSQL += " DES.DESIGNATION_MST_PK = DDT.DESIGNATION_MST_FK INNER JOIN ";
            strSQL += " DEPARTMENT_MST_TBL DEP ON DEP.DEPARTMENT_MST_PK = DDT.DEPART_MST_FK ";

            if (DepartmentFK > 0)
            {
                strSQL += " WHERE DDT.DEPART_MST_FK = " + DepartmentFK;
            }

            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        #endregion "Fetch Designation"

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
            OracleCommand inskeyCommand = new OracleCommand();

            try
            {
                DataTable DtTbl = new DataTable();
                DtTbl = M_DataSet.Tables[0];
                var _with4 = insCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".EMPLOYEE_MST_TBL_PKG.EMPLOYEE_MST_TBL_INS";
                var _with5 = _with4.Parameters;

                insCommand.Parameters.Add("EMPLOYEE_ID_IN", OracleDbType.Varchar2, 20, "EMPLOYEE_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["EMPLOYEE_ID_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("EMPLOYEE_NAME_IN", OracleDbType.Varchar2, 50, "EMPLOYEE_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["EMPLOYEE_NAME_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("EMAIL_ID_IN", OracleDbType.Varchar2, 50, "EMAIL_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["EMAIL_ID_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("DEPARTMENT_MST_FK_IN", OracleDbType.Int32, 10, "DEPARTMENT_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEPARTMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("DESIGNATION_MST_FK_IN", OracleDbType.Int32, 10, "DESIGNATION_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["DESIGNATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("JOIN_DATE_IN", OracleDbType.Date, 10, "JOIN_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOIN_DATE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("BUSINESS_TYPE_IN", intBusinessType).Direction = ParameterDirection.Input
                insCommand.Parameters.Add("TERMINATED_IN", OracleDbType.Int32, 1, "TERMINATED").Direction = ParameterDirection.Input;
                insCommand.Parameters["TERMINATED_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("KEYCONT_IN", OracleDbType.Int32, 1, "KEY_CONTACT").Direction = ParameterDirection.Input;
                insCommand.Parameters["KEYCONT_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "EMPLOYEE_MST_TBL_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with6 = delCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".EMPLOYEE_MST_TBL_PKG.EMPLOYEE_MST_TBL_DEL";
                var _with7 = _with6.Parameters;
                delCommand.Parameters.Add("EMPLOYEE_MST_PK_IN", OracleDbType.Int32, 10, "EMPLOYEE_MST_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["EMPLOYEE_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with8 = updCommand;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".EMPLOYEE_MST_TBL_PKG.EMPLOYEE_MST_TBL_UPD";
                var _with9 = _with8.Parameters;

                updCommand.Parameters.Add("EMPLOYEE_MST_PK_IN", OracleDbType.Int32, 10, "EMPLOYEE_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["EMPLOYEE_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("EMPLOYEE_ID_IN", OracleDbType.Varchar2, 20, "EMPLOYEE_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["EMPLOYEE_ID_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("EMPLOYEE_NAME_IN", OracleDbType.Varchar2, 50, "EMPLOYEE_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["EMPLOYEE_NAME_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("EMAIL_ID_IN", OracleDbType.Varchar2, 50, "EMAIL_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["EMAIL_ID_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("DEPARTMENT_MST_FK_IN", OracleDbType.Int32, 10, "DEPARTMENT_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEPARTMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("DESIGNATION_MST_FK_IN", OracleDbType.Int32, 10, "DESIGNATION_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["DESIGNATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("JOIN_DATE_IN", OracleDbType.Date, 10, "JOIN_DATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOIN_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TERMINATED_IN", OracleDbType.Int32, 1, "TERMINATED").Direction = ParameterDirection.Input;
                updCommand.Parameters["TERMINATED_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("KEYCONT_IN", OracleDbType.Int32, 1, "KEY_CONTACT").Direction = ParameterDirection.Input;
                updCommand.Parameters["KEYCONT_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with10 = objWK.MyDataAdapter;
                _with10.InsertCommand = insCommand;
                _with10.InsertCommand.Transaction = TRAN;
                _with10.UpdateCommand = updCommand;
                _with10.UpdateCommand.Transaction = TRAN;
                _with10.DeleteCommand = delCommand;
                _with10.DeleteCommand.Transaction = TRAN;
                inskeyCommand.Connection = objWK.MyConnection;
                inskeyCommand.Transaction = TRAN;
                //Goutam (05/09/2013) : Not required as implement common functionality of Licensing.
                //EnableDisableUser(M_DataSet, inskeyCommand)
                RecAfct = _with10.Update(M_DataSet);
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
                //Manjunath
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save Function"

        #region "Save Function for new Employee"

        /// <summary>
        /// Saves the employee.
        /// </summary>
        /// <param name="empId">The emp identifier.</param>
        /// <param name="empName">Name of the emp.</param>
        /// <param name="locationFK">The location fk.</param>
        /// <param name="deptFK">The dept fk.</param>
        /// <param name="desigFK">The desig fk.</param>
        /// <param name="active">The active.</param>
        /// <param name="joinDate">The join date.</param>
        /// <param name="userID">The user identifier.</param>
        /// <param name="M_CREATED_BY_FK">The m_ create d_ b y_ fk.</param>
        /// <param name="ConfigurationPK">The configuration pk.</param>
        /// <param name="intBusinessType">Type of the int business.</param>
        /// <param name="intEmpType">Type of the int emp.</param>
        /// <param name="keycont">The keycont.</param>
        /// <param name="supusr">The supusr.</param>
        /// <param name="Mode">The mode.</param>
        /// <param name="EmpPK">The emp pk.</param>
        /// <param name="VersionNo">The version no.</param>
        /// <param name="emailId">The email identifier.</param>
        /// <param name="PhoneID">The phone identifier.</param>
        /// <param name="FirstName">The first name.</param>
        /// <param name="LastName">The last name.</param>
        /// <returns></returns>
        public ArrayList SaveEmployee(string empId, string empName, long locationFK, long deptFK, long desigFK, string active, DateTime joinDate, string userID, string M_CREATED_BY_FK, string ConfigurationPK,
        int intBusinessType, int intEmpType, string keycont, string supusr, int Mode, string EmpPK = "", int VersionNo = 0, string emailId = "", string PhoneID = "", string FirstName = "",
        string LastName = "")
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            Int32 RecAffct = default(Int32);
            dynamic strNull = "";
            int emp_fk = 0;
            int Cus_fk = 0;
            string str = null;
            Int16 intIns = default(Int16);
            OracleCommand insCommand = new OracleCommand();
            insCommand.Transaction = TRAN;

            if (Mode == 0)
            {
                try
                {
                    var _with11 = insCommand;
                    _with11.Connection = objWK.MyConnection;
                    _with11.CommandType = CommandType.StoredProcedure;
                    _with11.CommandText = objWK.MyUserName + ".EMPLOYEE_MST_TBL_PKG.EMPLOYEE_MST_TBL_INS";
                    var _with12 = _with11.Parameters;

                    insCommand.Parameters.Add("EMPLOYEE_ID_IN", empId).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("EMPLOYEE_NAME_IN", empName).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("EMAIL_ID_IN", (string.IsNullOrEmpty(emailId) ? strNull : emailId)).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("DEPARTMENT_MST_FK_IN", deptFK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("DESIGNATION_MST_FK_IN", desigFK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("LOCATION_MST_FK_IN", locationFK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("JOIN_DATE_IN", joinDate).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("BUSINESS_TYPE_IN", intBusinessType).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("TERMINATED_IN", active).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("KEYCONT_IN", keycont).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("SUPER_USER_IN", keycont).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("PHONENR_IN", (string.IsNullOrEmpty(PhoneID) ? strNull : PhoneID)).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("FIRST_NAME_IN", FirstName).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("LAST_NAME_IN", LastName).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("EMP_TYPE_IN", intEmpType).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "EMPLOYEE_MST_TBL_PK").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    try
                    {
                        RecAfct = insCommand.ExecuteNonQuery();
                        emp_fk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    }
                    catch (OracleException OraEx)
                    {
                        TRAN.Rollback();
                        arrMessage.Add(OraEx.Message);
                        return arrMessage;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }

                    if (RecAfct > 0)
                    {
                        if (!string.IsNullOrEmpty(userID))
                        {
                            OracleCommand insCmdUser = new OracleCommand();

                            insCmdUser.Transaction = TRAN;

                            var _with13 = insCmdUser;
                            _with13.Connection = objWK.MyConnection;
                            _with13.CommandType = CommandType.StoredProcedure;
                            _with13.CommandText = objWK.MyUserName + ".USER_MST_TBL_PKG.USER_MST_TBL_INS";
                            var _with14 = _with13.Parameters;
                            insCmdUser.Parameters.Add("USER_ID_IN", userID).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("USER_NAME_IN", empName).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("DEFAULT_LOCATION_FK_IN", locationFK).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("PASS_WORD_IN", "welcome").Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("PASS_WARD_CHANGE_DT_IN", joinDate).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("IS_ACTIVATED_IN", active).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("EMPLOYEE_MST_FK_IN", emp_fk).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("CUSTOMER_MST_FK_IN", Cus_fk).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("BUSINESS_TYPE_IN", intBusinessType).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            insCmdUser.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "return_value").Direction = ParameterDirection.Output;
                            insCmdUser.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            try
                            {
                                RecAffct = insCmdUser.ExecuteNonQuery();
                            }
                            catch (OracleException Oraex)
                            {
                                TRAN.Rollback();
                                arrMessage.Add(Oraex.Message);
                                return arrMessage;
                            }
                        }

                        TRAN.Commit();
                        arrMessage.Add("All data Saved Successfully");
                        return arrMessage;
                    }
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
                finally
                {
                    objWK.CloseConnection();
                }
            }
            else
            {
                try
                {
                    var _with15 = insCommand;
                    _with15.Connection = objWK.MyConnection;
                    _with15.CommandType = CommandType.StoredProcedure;
                    _with15.CommandText = objWK.MyUserName + ".EMPLOYEE_MST_TBL_PKG.EMPLOYEE_MST_TBL_UPD";
                    var _with16 = _with15.Parameters;
                    insCommand.Parameters.Add("EMPLOYEE_MST_PK_IN", EmpPK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("EMPLOYEE_ID_IN", empId).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("EMPLOYEE_NAME_IN", empName).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("EMAIL_ID_IN", (string.IsNullOrEmpty(emailId) ? strNull : emailId)).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("DEPARTMENT_MST_FK_IN", deptFK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("DESIGNATION_MST_FK_IN", desigFK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("LOCATION_MST_FK_IN", locationFK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("JOIN_DATE_IN", joinDate).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("TERMINATED_IN", active).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("KEYCONT_IN", keycont).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("SUPER_USER_IN", supusr).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("PHONENR_IN", (string.IsNullOrEmpty(PhoneID) ? strNull : PhoneID)).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("FIRST_NAME_IN", FirstName).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("LAST_NAME_IN", LastName).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("EMP_TYPE_IN", intEmpType).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("BUSINESS_TYPE_IN", intBusinessType).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("VERSION_NO_IN", VersionNo).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "EMPLOYEE_MST_TBL_PK").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    try
                    {
                        RecAfct = insCommand.ExecuteNonQuery();
                        emp_fk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                        if (RecAfct > 0)
                        {
                            OracleCommand updCmdUser = new OracleCommand();
                            updCmdUser.Transaction = TRAN;
                            str = "update user_mst_tbl u set u.business_type=" + intBusinessType;
                            str += " where u.employee_mst_fk=" + emp_fk;
                            var _with17 = updCmdUser;
                            _with17.Connection = objWK.MyConnection;
                            _with17.Transaction = TRAN;
                            _with17.CommandType = CommandType.Text;
                            _with17.CommandText = str;
                            intIns = Convert.ToInt16(_with17.ExecuteNonQuery());
                        }
                    }
                    catch (OracleException OraEx)
                    {
                        TRAN.Rollback();
                        arrMessage.Add(OraEx.Message);
                        return arrMessage;
                    }

                    TRAN.Commit();
                    //Push to financial system if realtime is selected
                    cls_Scheduler objSch = new cls_Scheduler();
                    arrMessage.Add("All data Saved Successfully");
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
                finally
                {
                    objWK.CloseConnection();
                }
            }
            return arrMessage;
        }

        #endregion "Save Function for new Employee"

        #region "Fetch Employee for user Function"

        //updated by vimlesh kumar on 05-july-2006
        //added 1 optional parameter for geting user id
        //it can get employee name as well as user id
        /// <summary>
        /// Fetches the emp name for user.
        /// </summary>
        /// <param name="UserPK">The user pk.</param>
        /// <param name="strUser">The string user.</param>
        /// <returns></returns>
        public string FetchEmpNameForUser(Int16 UserPK = 0, string strUser = "")
        {
            string strSQL = null;
            if (string.IsNullOrEmpty(strUser))
            {
                strSQL += " SELECT  NVL(EMT.EMPLOYEE_NAME,'') FROM USER_MST_TBL UMT , EMPLOYEE_MST_TBL EMT ";
                strSQL += " WHERE UMT.EMPLOYEE_MST_FK = EMT.EMPLOYEE_MST_PK(+) AND  USER_MST_PK = " + UserPK;
            }
            else
            {
                strSQL += " SELECT  NVL(UMT.USER_ID,'') FROM USER_MST_TBL UMT , EMPLOYEE_MST_TBL EMT ";
                strSQL += " WHERE UMT.EMPLOYEE_MST_FK = EMT.EMPLOYEE_MST_PK(+) AND EMT.EMPLOYEE_MST_PK = " + UserPK;
            }
            WorkFlow objWF = new WorkFlow();
            try
            {
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

        #endregion "Fetch Employee for user Function"

        #region "Fetch Business type"

        /// <summary>
        /// Fetches the type of the business.
        /// </summary>
        /// <returns></returns>
        public string FetchBusinessType()
        {
            string strSQL = null;
            strSQL = "SELECT  B.BUSINESS_TYPE,B.BUSINESS_TYPE_DISPLAY FROM BUSINESS_TYPE_MST_TBL B";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return Convert.ToString(objWF.ExecuteScaler(strSQL));
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

        //added by surya prasad on 04-Jan-2009 for implementing removals concept
        /// <summary>
        /// Fetches the business type_ removals.
        /// </summary>
        /// <returns></returns>
        public string FetchBusinessType_Removals()
        {
            string strSQL = null;
            strSQL = "SELECT  B.BUSINESS_TYPE,B.BUSINESS_TYPE_DISPLAY FROM Rem_m_business_type_mst_tbl B";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return Convert.ToString(objWF.ExecuteScaler(strSQL));
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

        //end

        #endregion "Fetch Business type"

        #region "ENHANCE SEARCH"

        /// <summary>
        /// Fetches the emp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchEmp(string strCond)
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
            strReq = (string)arr.GetValue(0);
            strSERACH_IN = (string)arr.GetValue(1);
            strLOC_MST_IN = (string)arr.GetValue(2);
            strBizType = (string)arr.GetValue(3);
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EMP_PKG.GETEMP";

                var _with18 = selectCommand.Parameters;
                _with18.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with18.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with18.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with18.Add("BIZTYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.NChar, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        /// <summary>
        /// Fetches the customer based emp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCustBasedEmp(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strCustPk = null;
            string strReq = null;
            string strLOC_MST_IN = null;

            arr = strCond.Split('~');
            strReq = (string)arr.GetValue(0);
            strSERACH_IN = (string)arr.GetValue(1);
            strLOC_MST_IN = (string)arr.GetValue(2);
            strBizType = (string)arr.GetValue(3);
            strCustPk = (string)arr.GetValue(4);
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EMP_PKG.GETCUSTEMP";

                var _with19 = selectCommand.Parameters;
                _with19.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with19.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with19.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with19.Add("BIZTYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with19.Add("CUSTPK_IN", (!string.IsNullOrEmpty(strCustPk) ? strCustPk : "")).Direction = ParameterDirection.Input;
                _with19.Add("RETURN_VALUE", OracleDbType.NChar, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion "ENHANCE SEARCH"

        #region "ENHANCE SEARCH"

        /// <summary>
        /// Fetches the emp identifier.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchEmpID(string strCond)
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
            strReq = (string)arr.GetValue(0);
            strSERACH_IN = (string)arr.GetValue(1);
            strLOC_MST_IN = (string)arr.GetValue(2);
            strBizType = (string)arr.GetValue(3);
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EMP_PKG.GETEMPID";

                var _with20 = selectCommand.Parameters;
                _with20.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with20.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with20.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : "")).Direction = ParameterDirection.Input;
                _with20.Add("BIZTYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with20.Add("RETURN_VALUE", OracleDbType.NChar, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion "ENHANCE SEARCH"

        #region "To check No: of active users"

        /// <summary>
        /// FN_s the get_ stored users.
        /// </summary>
        /// <param name="NoOfLicenseUsers">The no of license users.</param>
        /// <param name="PK_VALUE">The p k_ value.</param>
        /// <returns></returns>
        public string fn_Get_StoredUsers(Int32 NoOfLicenseUsers, int PK_VALUE = 0)
        {
            string strSQL = null;
            Int32 RetVal = default(Int32);
            WorkFlow objWK = new WorkFlow();

            if (PK_VALUE > 0)
            {
                strSQL = " select count(*)  from (select rownum slnr, u.* from user_mst_tbl u where u.is_activated = 1 order by rowid) q where q.slnr >" + NoOfLicenseUsers;
            }
            else
            {
                strSQL = " select count(*)  from (select rownum slnr, u.* from user_mst_tbl u where u.is_activated = 1 order by rowid) q where q.slnr >=" + NoOfLicenseUsers;
            }

            try
            {
                RetVal = Convert.ToInt32(objWK.ExecuteScaler(strSQL));
                return RetVal.ToString();
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// FN_s the get stored users.
        /// </summary>
        /// <param name="NoOfLicenseUsers">The no of license users.</param>
        /// <returns></returns>
        public string fn_GetStoredUsers(Int32 NoOfLicenseUsers)
        {
            string strSQL = null;
            Int32 RetVal = default(Int32);
            WorkFlow objWK = new WorkFlow();
            strSQL = " select count(*)  from (select rownum slnr, u.* from user_mst_tbl u where u.is_activated = 1 order by rowid) q where q.slnr >" + NoOfLicenseUsers;
            try
            {
                RetVal = Convert.ToInt32(objWK.ExecuteScaler(strSQL));
                return RetVal.ToString();
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "To check No: of active users"
    }
}