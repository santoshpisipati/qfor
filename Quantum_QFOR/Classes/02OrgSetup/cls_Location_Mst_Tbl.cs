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
using System.Configuration;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Location_Mst_Tbl : CommonFeatures
    {
        /// <summary>
        /// Fetches the profit margin setup.
        /// </summary>
        /// <param name="LocationMstPk">The location MST pk.</param>
        /// <returns></returns>
        public string FetchProfitMarginSetup(long LocationMstPk = 0)
        {
            string json = string.Empty;
            System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
            DataTable dt = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("LOCATION_MST_FK_IN", LocationMstPk).Direction = ParameterDirection.Input;
                _with1.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dt = objWF.GetDataTable("LOCATION_MST_TBL_PKG", "FETCH_PROFIT_MARGIN_SETUP");
                return json = JsonConvert.SerializeObject(dt, Formatting.Indented);
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches all org.
        /// </summary>
        /// <param name="LocationPK">The location pk.</param>
        /// <param name="LocationId">The location identifier.</param>
        /// <param name="LocationName">Name of the location.</param>
        /// <param name="LocationType">Type of the location.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <returns></returns>
        public string FetchAllOrg(Int64 LocationPK = 0, string LocationId = "", string LocationName = "", Int64 LocationType = 0, string SearchType = "", string SortExpression = "")
        {
            string json = null;
            string StrSQL = null;
            try
            {
                StrSQL = "Select ROWNUM SR_NO, q.* from (";
                StrSQL += "SELECT ";
                StrSQL += " loc.Location_Mst_Pk,";
                StrSQL += " loc.Corporate_Mst_Fk,";
                StrSQL += " corp.company_reg_no,";
                StrSQL += " loc.Location_Id,";
                StrSQL += " loc.Location_Name,";
                StrSQL += " loc.Location_Type_Fk,";
                StrSQL += " loctype.LOCATION_TYPE_DESC,";
                StrSQL += " loc.Reporting_To_Fk,";
                StrSQL += " loc1.Location_Name REP_LOCNAME,";
                StrSQL += " loc.Address_Line1,";
                StrSQL += " loc.Address_Line2,";
                StrSQL += " loc.Address_Line3,";
                StrSQL += " loc.Country_Mst_Fk ,";
                StrSQL += " country.Country_Name ,";
                StrSQL += " loc.City,";
                StrSQL += " loc.Zip,";
                StrSQL += " loc.Tele_Phone_No,";
                StrSQL += " loc.Fax_No,";
                StrSQL += " loc.E_Mail_Id,";
                StrSQL += " loc.Remarks,";
                StrSQL += " loc.TIME_ZONE,";
                StrSQL += " loc.COST_CENTER,";
                StrSQL += " loc.Version_No, ";
                StrSQL += " loc.OFFICE_NAME ";
                StrSQL += " FROM LOCATION_MST_TBL loc, COUNTRY_MST_TBL country, ";
                StrSQL += " LOCATION_TYPE_MST_TBL loctype, LOCATION_MST_TBL loc1, corporate_mst_tbl corp ";
                StrSQL += " Where 1= 1";
                StrSQL += " AND loc.Reporting_To_Fk= loc1.Location_Mst_Pk(+) ";
                StrSQL += " AND loc.Location_Type_Fk= loctype.Location_Type_Mst_Pk ";
                StrSQL += " AND loc.Country_Mst_FK= country.Country_Mst_PK ";
                StrSQL += " AND loc.corporate_mst_fk=corp.corporate_mst_pk ";

                if (LocationPK != 0)
                {
                    StrSQL = StrSQL + " AND upper(loc.Location_Mst_PK) =" + LocationPK;
                }

                if (LocationId.Trim().Length > 0 & SearchType == "C")
                {
                    StrSQL = StrSQL + " AND upper(loc.location_ID) LIKE '%" + LocationId.ToUpper() + "%'";
                }
                else if (LocationId.Trim().Length > 0 & SearchType == "S")
                {
                    StrSQL = StrSQL + " AND upper(loc.location_ID) LIKE '" + LocationId.ToUpper() + "%'";
                }

                if (LocationName.Trim().Length > 0 & SearchType == "C")
                {
                    StrSQL = StrSQL + " AND upper(loc.location_Name) LIKE '%" + LocationName.ToUpper() + "%'";
                }
                else if (LocationName.Trim().Length > 0 & SearchType == "S")
                {
                    StrSQL = StrSQL + " AND upper(loc.location_Name) LIKE '" + LocationName.ToUpper() + "%'";
                }

                if (LocationType > 0)
                {
                    StrSQL += " AND loc.Location_Type_Fk=" + LocationType;
                }
                StrSQL += " )q ";

                WorkFlow objWF = new WorkFlow();
                DataSet objDS = new DataSet();
                return json = JsonConvert.SerializeObject(objDS, Formatting.Indented);
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

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <param name="BoolUnion">if set to <c>true</c> [bool union].</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <returns></returns>
        public string FetchLocation(bool BoolUnion = true, bool ActiveOnly = false)
        {
            string json = null;
            string StrSQL = null;
            string strCondition = " ";
            if (ActiveOnly == true)
            {
                strCondition += " and ACTIVE_FLAG = 1 ";
            }
            try
            {
                if (BoolUnion)
                {
                    StrSQL = " select ";
                    StrSQL += " 0 LOCATION_MST_PK, ";
                    StrSQL += " ' ' LOCATION_ID, ";
                    StrSQL += " ' ' LOCATION_NAME ";
                    StrSQL += " FROM DUAL ";
                    StrSQL += " UNION ";
                }
                StrSQL += "SELECT ";
                StrSQL += "LOCATION_MST_PK, ";
                StrSQL += "LOCATION_ID, ";
                StrSQL += "LOCATION_NAME ";
                StrSQL += "FROM Location_mst_tbl ";
                StrSQL += "WHERE 1=1 ";
                StrSQL += strCondition;
                StrSQL += "order by  LOCATION_NAME";
                WorkFlow objWF = new WorkFlow();
                DataSet objDS = null;

                objDS = objWF.GetDataSet(StrSQL);
                return json = JsonConvert.SerializeObject(objDS, Formatting.Indented);
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

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_Location_Mst_Fk">The p_ location_ MST_ fk.</param>
        /// <param name="P_Location_Working_Ports_Pk">The p_ location_ working_ ports_ pk.</param>
        /// <param name="P_Port_Mst_Fk">The p_ port_ MST_ fk.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <returns></returns>
        public string FetchAll(Int64 P_Location_Mst_Fk = 0, Int64 P_Location_Working_Ports_Pk = 0, Int64 P_Port_Mst_Fk = 0, string SearchType = "", string SortExpression = "")
        {
            string json = null;
            string strSQL = null;
            strSQL = "SELECT ";
            strSQL = strSQL + " Location_Mst_Fk,";
            strSQL = strSQL + " Port_Mst_Fk,";
            strSQL = strSQL + " Active ";
            strSQL = strSQL + " FROM LOCATION_WORKING_PORTS_TRN loc";
            strSQL = strSQL + " where (1=1) ";
            if (P_Location_Mst_Fk != 0)
            {
                strSQL = strSQL + " And loc.Location_Mst_Fk =" + P_Location_Mst_Fk;
            }
            WorkFlow objWF = new WorkFlow();
            try
            {
                return json = JsonConvert.SerializeObject(objWF.GetDataSet(strSQL), Formatting.Indented);
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

        /// <summary>
        /// CHKs the head quaters count.
        /// </summary>
        /// <param name="locpk">The locpk.</param>
        /// <returns></returns>
        public string chkHeadQuatersCount(int locpk)
        {
            string json = string.Empty;
            string sqlQry = null;
            int cnt = 0;
            WorkFlow objWF = new WorkFlow();
            sqlQry = "select count(*) from location_mst_tbl l where l.reporting_to_fk is null and l.location_mst_pk <> " + locpk;
            try
            {
                return json = JsonConvert.SerializeObject(objWF.ExecuteScaler(sqlQry), Formatting.Indented);
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

        /// <summary>
        /// Fetchpreferences the specified p_ locatio n_ ms t_ fk.
        /// </summary>
        /// <param name="P_LOCATION_MST_FK">The p_ locatio n_ ms t_ fk.</param>
        /// <param name="P_LOCATION_DWORKFLOW_PK">The p_ locatio n_ dworkflo w_ pk.</param>
        /// <param name="P_DOC_PREFERENCE_FK">The p_ do c_ preferenc e_ fk.</param>
        /// <param name="P_ACTIVE">The p_ active.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <returns></returns>
        public string Fetchpreference(Int64 P_LOCATION_MST_FK = 0, Int64 P_LOCATION_DWORKFLOW_PK = 0, Int64 P_DOC_PREFERENCE_FK = 0, Int64 P_ACTIVE = 0, string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ";
            strSQL = strSQL + " LOCATION_MST_FK,";
            strSQL = strSQL + " DOC_PREFERENCE_FK,";
            strSQL = strSQL + " ACTIVE";
            strSQL = strSQL + " FROM DOCUMENT_PREF_LOC_MST_TBL ";
            strSQL = strSQL + " where (1=1) ";
            if (P_LOCATION_MST_FK != 0)
            {
                strSQL = strSQL + " And LOCATION_MST_FK =" + P_LOCATION_MST_FK;
            }
            WorkFlow objWF = new WorkFlow();
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

        /// <summary>
        /// Fetches the monthly cutoff.
        /// </summary>
        /// <param name="LocationMstPK">The location MST pk.</param>
        /// <param name="No_Months">The no_ months.</param>
        /// <returns></returns>
        public string FetchMonthlyCutoff(int LocationMstPK, Int32 No_Months)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            int CloseDate = 0;
            if ((ConfigurationManager.AppSettings["Month_End_Closing_Date"] != null))
            {
                CloseDate = Convert.ToInt32(ConfigurationManager.AppSettings["Month_End_Closing_Date"]);
            }
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with1 = objWF.MyDataAdapter;
                _with1.SelectCommand = new OracleCommand();
                _with1.SelectCommand.Connection = objWF.MyConnection;
                _with1.SelectCommand.CommandText = objWF.MyUserName + ".MONTH_END_CLOSING_TRN_PKG.FETCH_MONTH_END_CLOSING_TRN";
                _with1.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with1.SelectCommand.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32).Value = LocationMstPK;
                _with1.SelectCommand.Parameters.Add("NO_OF_MONTHS_IN", OracleDbType.Int32).Value = (No_Months < 6 ? 6 : No_Months);
                _with1.SelectCommand.Parameters.Add("CLOSE_DATE_IN", OracleDbType.Int32).Value = CloseDate;
                _with1.SelectCommand.Parameters.Add("CREATED_BY_FK_IN", OracleDbType.Int32).Value = CREATED_BY;
                _with1.SelectCommand.Parameters.Add("CONFIG_MST_FK_IN", OracleDbType.Int32).Value = ConfigurationPK;
                _with1.SelectCommand.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Fill(DS);
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

        /// <summary>
        /// Fetches the country.
        /// </summary>
        /// <param name="CountryPK">The country pk.</param>
        /// <param name="CountryID">The country identifier.</param>
        /// <param name="CountryName">Name of the country.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="RemoveBlankRecord">if set to <c>true</c> [remove blank record].</param>
        /// <returns></returns>
        public string FetchCountry(Int16 CountryPK = 0, string CountryID = "", string CountryName = "", bool ActiveOnly = true, bool RemoveBlankRecord = false)
        {
            string strSQL = null;
            string strCondition = " 1 = 1 ";
            if (ActiveOnly == true)
            {
                strCondition += " and ACTIVE_FLAG = 1 ";
            }
            strSQL = strSQL + " SELECT * FROM ( ";
            strSQL = strSQL + " select ";
            strSQL = strSQL + " ' ' COUNTRY_ID,";
            strSQL = strSQL + " ' ' COUNTRY_NAME,";
            strSQL = strSQL + " 0 COUNTRY_MST_PK ";
            strSQL = strSQL + " FROM ";
            strSQL = strSQL + " DUAL ";
            strSQL = strSQL + " UNION";
            strSQL = strSQL + " SELECT ";
            strSQL = strSQL + " COUNTRY_ID,";
            strSQL = strSQL + " COUNTRY_NAME,";
            strSQL = strSQL + " COUNTRY_MST_PK ";
            strSQL = strSQL + " FROM COUNTRY_MST_TBL";
            strSQL = strSQL + " WHERE " + strCondition;
            strSQL = strSQL + " )";
            strSQL = strSQL + " ORDER BY COUNTRY_NAME";
            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet ds = null;
                ds = objWF.GetDataSet(strSQL);
                if (RemoveBlankRecord == true)
                {
                    ds.Tables[0].Rows.RemoveAt(0);
                }
                return JsonConvert.SerializeObject(ds, Formatting.Indented);
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
        /// Fetch_s the location_ user.
        /// </summary>
        /// <returns></returns>
        public string Fetch_Location_User()
        {
            string strSQL = null;
            strSQL = "select ' ' LOCATION_ID,";
            strSQL += "' ' LOCATION_NAME, ";
            strSQL += "0 LOCATION_MST_PK ";
            strSQL += "from Dual ";
            strSQL += "UNION ";
            strSQL += "Select LOCATION_ID, ";
            strSQL += "LOCATION_NAME,";
            strSQL += "LOCATION_MST_PK ";
            strSQL += "from Location_Mst_Tbl ";
            strSQL += "order by LOCATION_ID";

            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(objDS, Formatting.Indented);
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

        #region "List of Members of the Class"

        /// <summary>
        /// The m_ port_ data set
        /// </summary>
        private DataSet M_Port_DataSet;

        /// <summary>
        /// The m_ department_ data set
        /// </summary>
        private DataSet M_Department_DataSet;

        /// <summary>
        /// The m_ preference_ dataset
        /// </summary>
        private DataSet M_Preference_Dataset;

        /// <summary>
        /// The m_ bank_ data set
        /// </summary>
        private DataSet M_Bank_DataSet;

        /// <summary>
        /// The m_ location_ MST_ pk
        /// </summary>
        private Int64 M_Location_Mst_Pk;

        /// <summary>
        /// The m_ corporate_ MST_ fk
        /// </summary>
        private Int64 M_Corporate_Mst_Fk;

        /// <summary>
        /// The m_ location_ identifier
        /// </summary>
        private string M_Location_Id;

        /// <summary>
        /// The m_ location_ name
        /// </summary>
        private string M_Location_Name;

        /// <summary>
        /// The m_ location_ type_ fk
        /// </summary>
        private Int64 M_Location_Type_Fk;

        /// <summary>
        /// The m_ reporting_ to_ fk
        /// </summary>
        private Int64 M_Reporting_To_Fk;

        /// <summary>
        /// The m_ location_ incharge_ fk
        /// </summary>
        private Int64 M_Location_Incharge_Fk;

        /// <summary>
        /// The m_ address_ line1
        /// </summary>
        private string M_Address_Line1;

        /// <summary>
        /// The m_ address_ line2
        /// </summary>
        private string M_Address_Line2;

        /// <summary>
        /// The m_ address_ line3
        /// </summary>
        private string M_Address_Line3;

        /// <summary>
        /// The m_ state_ MST_ fk
        /// </summary>
        private Int64 M_State_Mst_Fk;

        /// <summary>
        /// The m_ city
        /// </summary>
        private string M_City;

        /// <summary>
        /// The m_ tele_ phone_ no
        /// </summary>
        private string M_Tele_Phone_No;

        /// <summary>
        /// The m_ fax_ no
        /// </summary>
        private string M_Fax_No;

        /// <summary>
        /// The m_ e_ mail_ identifier
        /// </summary>
        private string M_E_Mail_Id;

        /// <summary>
        /// The m_ remarks
        /// </summary>
        private string M_Remarks;

        /// <summary>
        /// The m_ corporate_ identifier
        /// </summary>
        private string M_Corporate_Id;

        /// <summary>
        /// The m_ corporate_ name
        /// </summary>
        private string M_Corporate_Name;

        /// <summary>
        /// The m_ location_ type
        /// </summary>
        private string M_Location_Type;

        /// <summary>
        /// The m_ reporting_ to
        /// </summary>
        private string M_Reporting_To;

        /// <summary>
        /// The m_ location_ incharge
        /// </summary>
        private string M_Location_Incharge;

        /// <summary>
        /// The m_ state
        /// </summary>
        private string M_State;

        /// <summary>
        /// The m_ country
        /// </summary>
        private string M_Country;

        /// <summary>
        /// The m_ state_ name
        /// </summary>
        private string M_State_Name;

        /// <summary>
        /// The m_ country_ name
        /// </summary>
        private string M_Country_Name;

        /// <summary>
        /// The m_ location_ country_ fk
        /// </summary>
        private Int64 M_Location_Country_Fk;

        /// <summary>
        /// The m_ biz type
        /// </summary>
        private Int16 M_BizType;

        #endregion "List of Members of the Class"

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the name of the corporate_.
        /// </summary>
        /// <value>
        /// The name of the corporate_.
        /// </value>
        public string Corporate_Name
        {
            get { return M_Corporate_Name; }
            set { M_Corporate_Name = value; }
        }

        /// <summary>
        /// Gets or sets the location_ country_ fk.
        /// </summary>
        /// <value>
        /// The location_ country_ fk.
        /// </value>
        public Int64 Location_Country_Fk
        {
            get { return M_Location_Country_Fk; }
            set { M_Location_Country_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the country.
        /// </summary>
        /// <value>
        /// The country.
        /// </value>
        public string Country
        {
            get { return M_Country; }
            set { M_Country = value; }
        }

        /// <summary>
        /// Gets or sets the state.
        /// </summary>
        /// <value>
        /// The state.
        /// </value>
        public string State
        {
            get { return M_State; }
            set { M_State = value; }
        }

        /// <summary>
        /// Gets or sets the location_ incharge.
        /// </summary>
        /// <value>
        /// The location_ incharge.
        /// </value>
        public string Location_Incharge
        {
            get { return M_Location_Incharge; }
            set { M_Location_Incharge = value; }
        }

        /// <summary>
        /// Gets or sets the reporting_ to.
        /// </summary>
        /// <value>
        /// The reporting_ to.
        /// </value>
        public string Reporting_To
        {
            get { return M_Reporting_To; }
            set { M_Reporting_To = value; }
        }

        /// <summary>
        /// Gets or sets the type of the location_.
        /// </summary>
        /// <value>
        /// The type of the location_.
        /// </value>
        public string Location_Type
        {
            get { return M_Location_Type; }
            set { M_Location_Type = value; }
        }

        /// <summary>
        /// Gets or sets the corporate_ identifier.
        /// </summary>
        /// <value>
        /// The corporate_ identifier.
        /// </value>
        public string Corporate_Id
        {
            get { return M_Corporate_Id; }
            set { M_Corporate_Id = value; }
        }

        /// <summary>
        /// Gets or sets the location_ MST_ pk.
        /// </summary>
        /// <value>
        /// The location_ MST_ pk.
        /// </value>
        public Int64 Location_Mst_Pk
        {
            get { return M_Location_Mst_Pk; }
            set { M_Location_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the corporate_ MST_ fk.
        /// </summary>
        /// <value>
        /// The corporate_ MST_ fk.
        /// </value>
        public Int64 Corporate_Mst_Fk
        {
            get { return M_Corporate_Mst_Fk; }
            set { M_Corporate_Mst_Fk = value; }
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
        /// Gets or sets the location_ type_ fk.
        /// </summary>
        /// <value>
        /// The location_ type_ fk.
        /// </value>
        public Int64 Location_Type_Fk
        {
            get { return M_Location_Type_Fk; }
            set { M_Location_Type_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the reporting_ to_ fk.
        /// </summary>
        /// <value>
        /// The reporting_ to_ fk.
        /// </value>
        public Int64 Reporting_To_Fk
        {
            get { return M_Reporting_To_Fk; }
            set { M_Reporting_To_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the location_ incharge_ fk.
        /// </summary>
        /// <value>
        /// The location_ incharge_ fk.
        /// </value>
        public Int64 Location_Incharge_Fk
        {
            get { return M_Location_Incharge_Fk; }
            set { M_Location_Incharge_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the address_ line1.
        /// </summary>
        /// <value>
        /// The address_ line1.
        /// </value>
        public string Address_Line1
        {
            get { return M_Address_Line1; }
            set { M_Address_Line1 = value; }
        }

        /// <summary>
        /// Gets or sets the address_ line2.
        /// </summary>
        /// <value>
        /// The address_ line2.
        /// </value>
        public string Address_Line2
        {
            get { return M_Address_Line2; }
            set { M_Address_Line2 = value; }
        }

        /// <summary>
        /// Gets or sets the address_ line3.
        /// </summary>
        /// <value>
        /// The address_ line3.
        /// </value>
        public string Address_Line3
        {
            get { return M_Address_Line3; }
            set { M_Address_Line3 = value; }
        }

        /// <summary>
        /// Gets or sets the state_ MST_ fk.
        /// </summary>
        /// <value>
        /// The state_ MST_ fk.
        /// </value>
        public Int64 State_Mst_Fk
        {
            get { return M_State_Mst_Fk; }
            set { M_State_Mst_Fk = value; }
        }

        /// <summary>
        /// Gets or sets the city.
        /// </summary>
        /// <value>
        /// The city.
        /// </value>
        public string City
        {
            get { return M_City; }
            set { M_City = value; }
        }

        /// <summary>
        /// Gets or sets the tele_ phone_ no.
        /// </summary>
        /// <value>
        /// The tele_ phone_ no.
        /// </value>
        public string Tele_Phone_No
        {
            get { return M_Tele_Phone_No; }
            set { M_Tele_Phone_No = value; }
        }

        /// <summary>
        /// Gets or sets the fax_ no.
        /// </summary>
        /// <value>
        /// The fax_ no.
        /// </value>
        public string Fax_No
        {
            get { return M_Fax_No; }
            set { M_Fax_No = value; }
        }

        /// <summary>
        /// Gets or sets the e_ mail_ identifier.
        /// </summary>
        /// <value>
        /// The e_ mail_ identifier.
        /// </value>
        public string E_Mail_Id
        {
            get { return M_E_Mail_Id; }
            set { M_E_Mail_Id = value; }
        }

        /// <summary>
        /// Gets or sets the remarks.
        /// </summary>
        /// <value>
        /// The remarks.
        /// </value>
        public string Remarks
        {
            get { return M_Remarks; }
            set { M_Remarks = value; }
        }

        /// <summary>
        /// Gets or sets the name of the country_.
        /// </summary>
        /// <value>
        /// The name of the country_.
        /// </value>
        public string Country_Name
        {
            get { return M_Country_Name; }
            set { M_Country_Name = value; }
        }

        /// <summary>
        /// Gets or sets the name of the state_.
        /// </summary>
        /// <value>
        /// The name of the state_.
        /// </value>
        public string State_Name
        {
            get { return M_State_Name; }
            set { M_State_Name = value; }
        }

        /// <summary>
        /// Gets or sets the port data set.
        /// </summary>
        /// <value>
        /// The port data set.
        /// </value>
        public DataSet PortDataSet
        {
            get { return M_Port_DataSet; }
            set { M_Port_DataSet = value; }
        }

        /// <summary>
        /// Gets or sets the bank data set.
        /// </summary>
        /// <value>
        /// The bank data set.
        /// </value>
        public DataSet BankDataSet
        {
            get { return M_Bank_DataSet; }
            set { M_Bank_DataSet = value; }
        }

        /// <summary>
        /// Gets or sets the deprtment data set.
        /// </summary>
        /// <value>
        /// The deprtment data set.
        /// </value>
        public DataSet DeprtmentDataSet
        {
            get { return M_Department_DataSet; }
            set { M_Department_DataSet = value; }
        }

        /// <summary>
        /// Gets or sets the preference dataset.
        /// </summary>
        /// <value>
        /// The preference dataset.
        /// </value>
        public DataSet PreferenceDataset
        {
            get { return M_Preference_Dataset; }
            set { M_Preference_Dataset = value; }
        }

        /// <summary>
        /// Gets or sets the type of the biz.
        /// </summary>
        /// <value>
        /// The type of the biz.
        /// </value>
        public Int16 BizType
        {
            get { return M_BizType; }
            set { M_BizType = value; }
        }

        #endregion "List of Properties"

        #region "Insert Function"

        /// <summary>
        /// Inserts this instance.
        /// </summary>
        /// <returns></returns>
        public int Insert()
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                Int32 intPkVal = default(Int32);
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with1 = objWS.MyCommand.Parameters;
                _with1.Add("Corporate_Mst_Fk_IN", M_Corporate_Mst_Fk).Direction = ParameterDirection.Input;
                _with1.Add("Location_Id_IN", M_Location_Id).Direction = ParameterDirection.Input;
                _with1.Add("Location_Name_IN", M_Location_Name).Direction = ParameterDirection.Input;
                _with1.Add("Location_Type_Fk_IN", M_Location_Type_Fk).Direction = ParameterDirection.Input;
                _with1.Add("Reporting_To_Fk_IN", M_Reporting_To_Fk).Direction = ParameterDirection.Input;
                _with1.Add("Location_Incharge_Fk_IN", M_Location_Incharge_Fk).Direction = ParameterDirection.Input;
                _with1.Add("Address_Line1_IN", M_Address_Line1).Direction = ParameterDirection.Input;
                _with1.Add("Address_Line2_IN", M_Address_Line2).Direction = ParameterDirection.Input;
                _with1.Add("Address_Line3_IN", M_Address_Line3).Direction = ParameterDirection.Input;
                _with1.Add("State_Mst_Fk_IN", M_State_Mst_Fk).Direction = ParameterDirection.Input;
                _with1.Add("City_IN", M_City).Direction = ParameterDirection.Input;
                _with1.Add("Tele_Phone_No_IN", M_Tele_Phone_No).Direction = ParameterDirection.Input;
                _with1.Add("Fax_No_IN", M_Fax_No).Direction = ParameterDirection.Input;
                _with1.Add("E_Mail_Id_IN", M_E_Mail_Id).Direction = ParameterDirection.Input;
                _with1.Add("Remarks_IN", M_Remarks).Direction = ParameterDirection.Input;
                _with1.Add("Created_By_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = objWS.MyUserName.Trim() + ".Location_MST_TBL_PKG.Location_Mst_Tbl_Ins";
                if (objWS.ExecuteCommands() == true)
                {
                    return intPkVal;
                }
                else
                {
                    return -1;
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
        }

        #endregion "Insert Function"

        #region "Update Function"

        /// <summary>
        /// Updates this instance.
        /// </summary>
        /// <returns></returns>
        public int Update()
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                Int32 intPkVal = default(Int32);
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with2 = objWS.MyCommand.Parameters;
                _with2.Add("Location_Mst_Pk_IN", M_Location_Mst_Pk).Direction = ParameterDirection.Input;
                _with2.Add("Corporate_Mst_Fk_IN", M_Corporate_Mst_Fk).Direction = ParameterDirection.Input;
                _with2.Add("Location_Id_IN", M_Location_Id).Direction = ParameterDirection.Input;
                _with2.Add("Location_Name_IN", M_Location_Name).Direction = ParameterDirection.Input;
                _with2.Add("Location_Type_Fk_IN", M_Location_Type_Fk).Direction = ParameterDirection.Input;
                _with2.Add("Reporting_To_Fk_IN", M_Reporting_To_Fk).Direction = ParameterDirection.Input;
                _with2.Add("Location_Incharge_Fk_IN", M_Location_Incharge_Fk).Direction = ParameterDirection.Input;
                _with2.Add("Address_Line1_IN", M_Address_Line1).Direction = ParameterDirection.Input;
                _with2.Add("Address_Line2_IN", M_Address_Line2).Direction = ParameterDirection.Input;
                _with2.Add("Address_Line3_IN", M_Address_Line3).Direction = ParameterDirection.Input;
                _with2.Add("State_Mst_Fk_IN", M_State_Mst_Fk).Direction = ParameterDirection.Input;
                _with2.Add("City_IN", (M_City.Trim().Length > 0 ? M_City : " ")).Direction = ParameterDirection.Input;
                _with2.Add("Tele_Phone_No_IN", (M_Tele_Phone_No.Trim().Length > 0 ? M_Tele_Phone_No : " ")).Direction = ParameterDirection.Input;
                _with2.Add("Fax_No_IN", (M_Fax_No.Trim().Length > 0 ? M_Fax_No : " ")).Direction = ParameterDirection.Input;
                _with2.Add("E_Mail_Id_IN", (M_E_Mail_Id.Trim().Length > 0 ? M_E_Mail_Id : " ")).Direction = ParameterDirection.Input;
                _with2.Add("Remarks_IN", (M_Remarks.Trim().Length > 0 ? M_Remarks : " ")).Direction = ParameterDirection.Input;
                _with2.Add("Last_Modified_By_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("VERSION_NO_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = objWS.MyUserName.Trim() + ".Location_MST_TBL_PKG.Location_Mst_Tbl_UPD";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
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
        }

        #endregion "Update Function"

        #region "Delete Function"

        /// <summary>
        /// Deletes this instance.
        /// </summary>
        /// <returns></returns>
        public int Delete()
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                Int32 intPkVal = default(Int32);
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with3 = objWS.MyCommand.Parameters;
                _with3.Add("Location_Mst_Pk_IN", M_Location_Mst_Pk).Direction = ParameterDirection.Input;
                _with3.Add("VERSION_NO", M_VERSION_NO).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = objWS.MyUserName.Trim() + ".Location_MST_TBL_PKG.Location_Mst_Tbl_DEL";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
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
        }

        #endregion "Delete Function"

        #region "Change text if own location"

        /// <summary>
        /// Gets the text if own location.
        /// </summary>
        /// <param name="IsOwnLocation">if set to <c>true</c> [is own location].</param>
        /// <returns></returns>
        public string GetTextIfOwnLocation(bool IsOwnLocation)
        {
            if (IsOwnLocation)
            {
                return "Yes";
            }
            else
            {
                return "No";
            }
        }

        #endregion "Change text if own location"

        #region "FetchListing Function"

        /// <summary>
        /// Fetches the listing.
        /// </summary>
        /// <param name="P_Location_Mst_Pk">The p_ location_ MST_ pk.</param>
        /// <param name="P_Location_Id">The p_ location_ identifier.</param>
        /// <param name="P_Location_Name">Name of the p_ location_.</param>
        /// <param name="P_LocationType">Type of the p_ location.</param>
        /// <param name="P_Rep_Location_Id">The p_ rep_ location_ identifier.</param>
        /// <param name="P_Rep_Location_Name">Name of the p_ rep_ location_.</param>
        /// <param name="P_COUNTRY_Id">The p_ countr y_ identifier.</param>
        /// <param name="P_COUNTRY_Name">Name of the p_ countr y_.</param>
        /// <param name="P_OFFICE_NAME">Name of the p_ offic e_.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="isActive">The is active.</param>
        /// <param name="isEFS">if set to <c>true</c> [is efs].</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public string FetchListing(Int64 P_Location_Mst_Pk, string P_Location_Id, string P_Location_Name, long P_LocationType, string P_Rep_Location_Id, string P_Rep_Location_Name, string P_COUNTRY_Id, string P_COUNTRY_Name, string P_OFFICE_NAME, string SearchType,
        string strColumnName, Int32 CurrentPage, Int32 TotalPage, int isActive, bool isEFS, bool blnSortAscending, Int32 flag)
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
            if (P_Location_Id.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " And upper(loc.location_ID) like '%" + P_Location_Id.ToUpper().Trim().Replace("'", "''") + "%' ";
            }
            else if (P_Location_Id.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND upper(loc.location_ID) LIKE '" + P_Location_Id.ToUpper().Trim().Replace("'", "''") + "%'";
            }

            if (P_Location_Name.Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " AND upper(loc.location_Name) LIKE '%" + P_Location_Name.ToUpper().Trim().Replace("'", "''") + "%'";
            }
            else if (P_Location_Name.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND upper(loc.location_Name) LIKE '" + P_Location_Name.ToUpper().Trim().Replace("'", "''") + "%'";
            }

            if (P_Rep_Location_Id.Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " AND upper(loc1.location_ID) LIKE '%" + P_Rep_Location_Id.ToUpper().Trim().Replace("'", "''") + "%'";
            }
            else if (P_Rep_Location_Id.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND upper(loc1.location_ID) LIKE '" + P_Rep_Location_Id.ToUpper().Trim().Replace("'", "''") + "%'";
            }

            if (P_Rep_Location_Name.Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " AND upper(loc1.location_Name) LIKE '%" + P_Rep_Location_Name.ToUpper().Trim().Replace("'", "''") + "%'";
            }
            else if (P_Rep_Location_Name.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND upper(loc1.location_Name) LIKE '" + P_Rep_Location_Name.ToUpper().Trim().Replace("'", "''") + "%'";
            }

            if (P_LocationType > 0)
            {
                strCondition += " AND loc.Location_Type_Fk=" + P_LocationType;
            }

            if (P_OFFICE_NAME.Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " AND UPPER(loc.OFFICE_NAME) LIKE '%" + P_OFFICE_NAME.ToUpper().Trim().Replace("'", "''") + "%'";
            }
            else if (P_OFFICE_NAME.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND UPPER(loc.OFFICE_NAME) LIKE '" + P_OFFICE_NAME.ToUpper().Trim().Replace("'", "''") + "%'";
            }

            if (P_COUNTRY_Id.Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " AND UPPER(country.Country_ID) LIKE '%" + P_COUNTRY_Id.ToUpper().Trim().Replace("'", "''") + "%'";
            }
            else if (P_COUNTRY_Id.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND UPPER(country.Country_ID) LIKE '" + P_COUNTRY_Id.ToUpper().Trim().Replace("'", "''") + "%'";
            }

            if (P_COUNTRY_Name.Trim().Length > 0 & SearchType == "C")
            {
                strCondition = strCondition + " AND UPPER(country.Country_NAME) LIKE '%" + P_COUNTRY_Name.ToUpper().Trim().Replace("'", "''") + "%'";
            }
            else if (P_COUNTRY_Name.Trim().Length > 0 & SearchType == "S")
            {
                strCondition = strCondition + " AND UPPER(country.Country_NAME) LIKE '" + P_COUNTRY_Name.ToUpper().Trim().Replace("'", "''") + "%'";
            }

            if (isActive == 1)
            {
                strCondition += " AND loc.ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += "";
            }
            //======
            if (isEFS)
            {
                strCondition += " AND loc.COMP_LOCATION = 1 ";
            }
            else
            {
                strCondition += "";
            }
            //======
            strSQL = "SELECT Count(*) ";
            strSQL += " FROM LOCATION_MST_TBL loc, COUNTRY_MST_TBL country, ";
            strSQL += " LOCATION_TYPE_MST_TBL loctype, LOCATION_MST_TBL loc1 ";
            strSQL += "Where 1= 1";
            strSQL += " AND loc.Reporting_To_Fk= loc1.Location_Mst_Pk(+) ";
            strSQL += " AND loc.Location_Type_Fk= loctype.Location_Type_Mst_Pk ";
            strSQL += " AND loc.Country_Mst_FK= country.Country_Mst_PK";
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
            strSQL += "SELECT  ROWNUM Sr_No,Abc.* FROM  ";
            strSQL += "(SELECT   ";
            strSQL += " loc.Location_Mst_Pk,loc.ACTIVE_FLAG, ";
            strSQL += " loc.Corporate_Mst_Fk, ";
            strSQL += " loc.Location_Id, ";
            strSQL += " loc.Location_Name, ";
            strSQL += " loc.office_Name, ";
            strSQL += " loc.Location_Type_Fk, ";
            strSQL += " loctype.LOCATION_TYPE_DESC, ";
            strSQL += " loc.Reporting_To_Fk, ";
            strSQL += " loc1.Location_Id Rep_Loc, ";
            strSQL += " loc1.Location_Name REP_LOCNAME, ";
            strSQL += " loc.Country_Mst_FK , ";
            strSQL += " country.Country_ID , ";
            strSQL += " country.Country_Name , ";
            strSQL += " loc.Version_No  ";
            strSQL += " FROM LOCATION_MST_TBL loc, COUNTRY_MST_TBL country,  ";
            strSQL += " LOCATION_TYPE_MST_TBL loctype, LOCATION_MST_TBL loc1  ";
            strSQL += "Where 1= 1 ";
            strSQL += " AND loc.Reporting_To_Fk= loc1.Location_Mst_Pk(+)  ";
            strSQL += " AND loc.Location_Type_Fk= loctype.Location_Type_Mst_Pk  ";
            strSQL += " AND loc.Country_Mst_FK= country.Country_Mst_PK ";
            strSQL += strCondition;

            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL += " ) ABC) WHERE Sr_No  Between " + start + " and " + last;
            try
            {
                DataSet DS = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(DS, Formatting.Indented);
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

        #endregion "FetchListing Function"

        #region "Fetch All"

        /// <summary>
        /// Fetches the currency.
        /// </summary>
        /// <param name="CountryPk">The country pk.</param>
        /// <returns></returns>
        public int FetchCurrency(int CountryPk)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            try
            {
                strsql.Append("select country.currency_mst_fk from country_mst_tbl country where country.country_mst_pk=" + CountryPk);
                return Convert.ToInt32(objWF.ExecuteScaler(strsql.ToString()));
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the countrywise location.
        /// </summary>
        /// <param name="FinancialYear">The financial year.</param>
        /// <returns></returns>
        public DataSet Fetch_CountrywiseLocation(Int64 FinancialYear = 0)
        {
            string StrSQL = null;
            try
            {
                StrSQL += "Select ";
                StrSQL += "LOC.LOCATION_ID, ";
                StrSQL += "LOC.LOCATION_NAME, ";
                StrSQL += "Country.COUNTRY_ID, ";
                StrSQL += "DECODE(NVL(FIN.Financial_Year_FK,0),0,0,1) CHKVal ";
                StrSQL += "FROM Location_mst_tbl Loc, Country_Mst_Tbl Country,FINANCIAL_YEAR_LOC_TRN FIN ";
                StrSQL += "Where LOC.Country_Mst_Fk = Country.Country_Mst_Pk and FIN.Location_Mst_Fk=Location_mst_Pk(+) and";
                StrSQL += "FIN.Financial_Year_FK=" + FinancialYear + "";

                WorkFlow objWF = new WorkFlow();
                DataSet objDS = null;

                objDS = objWF.GetDataSet(StrSQL);
                return objDS;
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

        /// <summary>
        /// Fetch_s the loc identifier.
        /// </summary>
        /// <param name="LocationPK">The location pk.</param>
        /// <returns></returns>
        public object Fetch_LocID(Int64 LocationPK)
        {
            string StrSQL = null;
            StrSQL = "select l.location_id from location_mst_tbl l where l.location_mst_pk = " + LocationPK + " ";
            try
            {
                return (new WorkFlow()).ExecuteScaler(StrSQL);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the location.
        /// </summary>
        /// <param name="LocationPK">The location pk.</param>
        /// <param name="BoolUnion">if set to <c>true</c> [bool union].</param>
        /// <returns></returns>
        public DataSet Fetch_Location(Int64 LocationPK, bool BoolUnion = true)
        {
            string StrSQL = null;
            try
            {
                if (BoolUnion)
                {
                    StrSQL = " select ";
                    StrSQL += " 0 LOCATION_MST_PK, ";
                    StrSQL += " ' ' LOCATION_ID, ";
                    StrSQL += " ' ' LOCATION_NAME ";
                    StrSQL += " FROM DUAL ";
                    StrSQL += " UNION ";
                }
                StrSQL += " Select ";
                StrSQL += " LOCATION_MST_PK, ";
                StrSQL += " LOCATION_ID, ";
                StrSQL += " LOCATION_NAME ";
                StrSQL += " FROM Location_mst_tbl ";
                StrSQL += " Where 1=1";
                if (LocationPK > 0)
                {
                    StrSQL += " AND  Location_Mst_Pk <= " + LocationPK + " ";
                }
                WorkFlow objWF = new WorkFlow();
                DataSet objDS = null;

                objDS = objWF.GetDataSet(StrSQL);
                return objDS;
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

        /// <summary>
        /// Fetch_s the location_ wf.
        /// </summary>
        /// <param name="lngWFPk">The LNG wf pk.</param>
        /// <returns></returns>
        public DataSet Fetch_Location_WF(long lngWFPk = 0)
        {
            System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
            DataSet DS = new DataSet();
            WorkFlow objWF = new WorkFlow();
            try
            {
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("WORKFLOW_RULES_FK_IN", lngWFPk).Direction = ParameterDirection.Input;
                _with4.Add("WF_FROMLOC", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DOC_WORKFLOW", "FETCH_WORKFLOW_LOC");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Fetch All"

        #region "Fetch All Locations"

        /// <summary>
        /// Fetches all locations.
        /// </summary>
        /// <returns></returns>
        public string FetchAllLocations()
        {
            string strSQL = null;
            strSQL = "          SELECT                 ";
            strSQL += "      \tLOCATION_MST_PK\t      ,";
            strSQL += "      \tCORPORATE_MST_FK\t  ,";
            strSQL += "      \tLOCATION_ID\t          ,";
            strSQL += "      \tLOCATION_NAME\t      ,";
            strSQL += "      \tLOCATION_TYPE_FK\t  ,";
            strSQL += "      \tREPORTING_TO_FK\t      ,";
            strSQL += "      \tADDRESS_LINE1\t      ,";
            strSQL += "      \tADDRESS_LINE2\t      ,";
            strSQL += "      \tADDRESS_LINE3\t      ,";
            strSQL += "      \tZIP\t                  ,";
            strSQL += "      \tCITY\t              ,";
            strSQL += "      \tTELE_PHONE_NO\t      ,";
            strSQL += "      \tFAX_NO\t              ,";
            strSQL += "      \tE_MAIL_ID\t          ,";
            strSQL += "      \tREMARKS\t              ,";
            strSQL += "      \tTIME_ZONE\t          ,";
            strSQL += "      \tCOST_CENTER\t          ,";
            strSQL += "      \tCREATED_BY_FK\t      ,";
            strSQL += "      \tCREATED_DT\t          ,";
            strSQL += "      \tLAST_MODIFIED_BY_FK\t  ,";
            strSQL += "      \tLAST_MODIFIED_DT\t  ,";
            strSQL += "      \tVERSION_NO\t          ,";
            strSQL += "      \tCOUNTRY_MST_FK\t      ,";
            strSQL += "      \tACTIVE_FLAG\t          ,";
            strSQL += "      \tCOMP_LOCATION\t      ,";
            strSQL += "      \tOFFICE_NAME\t          ,";
            strSQL += "      \tLOGO_FILE_PATH\t      ,";
            strSQL += "      \tIATA_CODE\t          ,";
            strSQL += "      \tVAT_NO\t               ";
            strSQL += " FROM    LOCATION_MST_TBL WHERE ACTIVE_FLAG=1 ORDER BY LOCATION_ID";

            WorkFlow objWF = new WorkFlow();
            DataTable objDT = new DataTable();
            try
            {
                objDT = objWF.GetDataTable(strSQL);
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
            return JsonConvert.SerializeObject(objDT, Formatting.Indented);
        }

        #endregion "Fetch All Locations"

        #region "Fetch Location"

        /// <summary>
        /// Fetch_s the location.
        /// </summary>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <returns></returns>
        public DataSet Fetch_Location(bool ActiveOnly = false)
        {
            string strSQL = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            strSQL = "select ' ' LOCATION_ID,";
            strSQL += "' ' LOCATION_NAME, ";
            strSQL += "0 LOCATION_MST_PK ";
            strSQL += "FROM DUAL ";
            strSQL += "UNION ";
            strSQL += "SELECT LOCATION_ID, ";
            strSQL += "LOCATION_NAME,";
            strSQL += "LOCATION_MST_PK ";
            strSQL += "FROM LOCATION_MST_TBL WHERE ";

            if (ActiveOnly == true)
            {
                strSQL += " ACTIVE_FLAG in (1,0)";
            }
            else
            {
                strSQL += "COMP_LOCATION = 1 AND ACTIVE_FLAG = 1";
            }
            strSQL += "order by LOCATION_NAME";
            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
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

        #endregion "Fetch Location"

        #region "Fetch Location"

        /// <summary>
        /// Fetch_s the location freight entry.
        /// </summary>
        /// <returns></returns>
        public DataSet Fetch_LocationFreightEntry()
        {
            string strSQL = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();

            strSQL = "select ' ' LOCATION_ID,";
            strSQL += "' ' LOCATION_NAME, ";
            strSQL += "0 LOCATION_MST_PK ";
            strSQL += "from Dual ";
            strSQL += "UNION ";
            strSQL += "Select LOCATION_ID, ";
            strSQL += "LOCATION_NAME,";
            strSQL += "LOCATION_MST_PK ";
            strSQL += "from Location_Mst_Tbl ";
            strSQL += "order by LOCATION_ID";

            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
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

        #endregion "Fetch Location"

        #region "Fetch Location Sort by Location name"

        /// <summary>
        /// Fetch_s the name of the location_.
        /// </summary>
        /// <returns></returns>
        public DataSet Fetch_Location_Name()
        {
            string strSQL = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();

            strSQL = "select ' ' LOCATION_ID,";
            strSQL += "' ' LOCATION_NAME, ";
            strSQL += "0 LOCATION_MST_PK ";
            strSQL += "from Dual ";
            strSQL += "UNION ";
            strSQL += "Select LOCATION_ID, ";
            strSQL += "LOCATION_NAME,";
            strSQL += "LOCATION_MST_PK ";
            strSQL += "from Location_Mst_Tbl ";
            strSQL += " where Location_Mst_Tbl.Active_Flag = 1";
            strSQL += "order by LOCATION_NAME";

            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
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

        #endregion "Fetch Location Sort by Location name"

        #region "Update File Name"

        /// <summary>
        /// Updates the name of the file.
        /// </summary>
        /// <param name="Location_Type_Fk">The location_ type_ fk.</param>
        /// <param name="strFileName">Name of the string file.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public bool UpdateFileName(long Location_Type_Fk, string strFileName, Int16 Flag)
        {
            if (strFileName.Trim().Length > 0)
            {
                string RemQuery = null;
                WorkFlow objwk = new WorkFlow();
                if (Flag == 1)
                {
                    RemQuery = " UPDATE AGENT_COMM_SETUP_TBL AGT SET AGT.ATTACHED_FILE_NAME= '" + strFileName + "'";
                    RemQuery += " WHERE AGT.LOCATION_TYPE_MST_FK= " + Location_Type_Fk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
                else if (Flag == 2)
                {
                    RemQuery = " UPDATE AAGENT_COMM_SETUP_TBL AGT SET AGT.ATTACHED_FILE_NAME='" + "" + "'";
                    RemQuery += " WHERE AGT.LOCATION_TYPE_MST_FK= " + Location_Type_Fk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Fetches the name of the file.
        /// </summary>
        /// <param name="Location_Type_Fk">The location_ type_ fk.</param>
        /// <returns></returns>
        public string FetchFileName(long Location_Type_Fk)
        {
            string strSQL = null;
            string strUpdFileName = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT ";
            strSQL += " AGT.ATTACHED_FILE_NAME FROM AGENT_COMM_SETUP_TBL AGT  WHERE AGT.LOCATION_TYPE_MST_FK = " + Location_Type_Fk;
            try
            {
                strUpdFileName = objWF.ExecuteScaler(strSQL);
                return strUpdFileName;
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

        #endregion "Update File Name"

        #region "Delete"

        /// <summary>
        /// Deletes the specified deleted row.
        /// </summary>
        /// <param name="DeletedRow">The deleted row.</param>
        /// <returns></returns>
        public ArrayList Delete(ArrayList DeletedRow)
        {
            WorkFlow obJWk = new WorkFlow();
            OracleTransaction oraTran = default(OracleTransaction);
            OracleCommand delCommand = new OracleCommand();
            string strReturn = null;
            string[] arrRowDetail = null;
            Int32 i = default(Int32);
            try
            {
                obJWk.OpenConnection();
                for (i = 0; i <= DeletedRow.Count - 1; i++)
                {
                    oraTran = obJWk.MyConnection.BeginTransaction();
                    var _with14 = obJWk.MyCommand;
                    _with14.Transaction = oraTran;
                    _with14.Connection = obJWk.MyConnection;
                    _with14.CommandType = CommandType.StoredProcedure;
                    _with14.CommandText = obJWk.MyUserName + ".LOCATION_MST_TBL_PKG.LOCATION_MST_TBL_DEL";
                    arrRowDetail = DeletedRow[i].ToString().Split(',');
                    _with14.Parameters.Clear();
                    var _with15 = _with14.Parameters;
                    _with15.Add("LOCATION_MST_PK_IN", arrRowDetail[0]).Direction = ParameterDirection.Input;
                    _with15.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with15.Add("VERSION_NO_IN", arrRowDetail[1]).Direction = ParameterDirection.Input;
                    _with15.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with15.Add("RETURN_VALUE", strReturn).Direction = ParameterDirection.Output;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].OracleDbType = OracleDbType.Varchar2;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].Size = 50;
                    _with14.ExecuteNonQuery();
                }
                if (arrMessage.Count > 0)
                {
                    oraTran.Rollback();
                }
                else
                {
                    oraTran.Commit();
                    arrMessage.Add("Record Deleted Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            finally
            {
                obJWk.MyConnection.Close();
            }
            return arrMessage;
        }

        #endregion "Delete"

        #region "To Know Location Id Exist In Agent Id "

        /// <summary>
        /// Check_s the agent_ in_ agent MSTTBL.
        /// </summary>
        /// <param name="LocationId">The location identifier.</param>
        /// <returns></returns>
        public DataSet Check_Agent_In_AgentMsttbl(string LocationId)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("select amt.agent_mst_pk");
                strQuery.Append("  from agent_mst_tbl amt");
                strQuery.Append(" where amt.agent_id = '" + LocationId + "'");
                strQuery.Append("");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "To Know Location Id Exist In Agent Id "

        #region "Update If location ID Exist In Agent ID "

        /// <summary>
        /// Update_s the agent_ MST_ table.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="AgentMstPk">The agent MST pk.</param>
        /// <param name="cmd">The command.</param>
        /// <returns></returns>
        public ArrayList Update_Agent_Mst_Tbl(DataSet M_DataSet, int AgentMstPk, OracleCommand cmd)
        {
            WorkFlow objWK = new WorkFlow();
            int exe = 0;
            System.Text.StringBuilder strQuery = null;

            string strLocationId = null;
            string strLocationOfficeName = null;

            string strAdd1 = null;
            string strAdd2 = null;
            string strAdd3 = null;
            string strCity = null;
            string strZipCode = null;
            string strphone_no_1 = null;
            string strMail_Id = null;
            string strFax_No = null;

            arrMessage.Clear();

            try
            {
                cmd.CommandType = CommandType.Text;
                strLocationId = Convert.ToString(M_DataSet.Tables[0].Rows[0]["LOCATION_ID"]);
                strLocationOfficeName = Convert.ToString(M_DataSet.Tables[0].Rows[0]["OFFICE_NAME"]);

                strAdd1 = Convert.ToString(getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["ADDRESS_LINE1"]), ""));
                strAdd2 = Convert.ToString(getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["ADDRESS_LINE2"]), ""));
                strAdd3 = Convert.ToString(getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["ADDRESS_LINE3"]), ""));
                strCity = Convert.ToString(getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["CITY"]), "")); ;
                strZipCode = Convert.ToString(getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["ZIP"]), ""));
                strphone_no_1 = Convert.ToString(getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["TELE_PHONE_NO"]), ""));
                strMail_Id = Convert.ToString(getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["E_MAIL_ID"]), ""));
                strFax_No = Convert.ToString(getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["FAX_NO"]), ""));

                cmd.Parameters.Clear();
                strQuery = new System.Text.StringBuilder();

                strQuery.Append(" update agent_mst_tbl amt");
                strQuery.Append(" SET amt.location_agent=1, amt.agent_name='" + strLocationOfficeName + "'");
                strQuery.Append(" where ");
                strQuery.Append(" amt.agent_mst_pk=" + AgentMstPk);
                strQuery.Append("");

                cmd.CommandText = strQuery.ToString();
                exe = cmd.ExecuteNonQuery();
                string strSQL = "";
                strSQL += " update agent_contact_dtls ac ";
                //1
                strSQL += " set ac.adm_address_1 ='" + strAdd1 + "'";
                //2 'mandatory
                strSQL += " ,ac.adm_address_2 ='" + strAdd2 + "'";
                //3
                strSQL += " ,ac.adm_address_3 ='" + strAdd3 + "'";
                //4
                strSQL += " ,ac.adm_city ='" + strCity + "'";
                //5  'mandatory
                strSQL += " ,ac.adm_zip_code ='" + strZipCode + "'";
                //6
                strSQL += " ,ac.adm_phone_no_1 ='" + strphone_no_1 + "'";
                //7
                strSQL += " ,ac.adm_email_id ='" + strMail_Id + "'";
                //9
                strSQL += " ,ac.adm_fax_no ='" + strFax_No + "'";
                //10
                strSQL += " where ac.agent_mst_fk =" + AgentMstPk;
                //11

                cmd.CommandText = strSQL.ToString();
                exe = cmd.ExecuteNonQuery();

                arrMessage.Add("All data saved successfully");
                return arrMessage;
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
        }

        #endregion "Update If location ID Exist In Agent ID "

        #region "Insert Into Agent_Mst_Tbl"

        /// <summary>
        /// Insert_s the agent_ MST.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="cmd">The command.</param>
        /// <returns></returns>
        public ArrayList Insert_Agent_Mst(DataSet M_DataSet, OracleCommand cmd)
        {
            WorkFlow objWK = new WorkFlow();
            int exe = 0;

            arrMessage.Clear();
            try
            {
                cmd.Parameters.Clear();
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = objWK.MyUserName + ".AGENCY_MST_TBL_PKG.AGENT_MST_TBL_INS";

                var _with16 = cmd.Parameters;
                _with16.Add("AGENT_ID_IN", Convert.ToString(M_DataSet.Tables[0].Rows[0]["LOCATION_ID"])).Direction = ParameterDirection.Input;
                _with16.Add("AGENT_NAME_IN", getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["OFFICE_NAME"]), "")).Direction = ParameterDirection.Input;
                //.Add("AGENT_NAME_IN", CType(M_DataSet.Tables(0).Rows(0).Item("OFFICE_NAME"), String)).Direction = ParameterDirection.Input
                _with16.Add("ACTIVE_FLAG_IN", "1").Direction = ParameterDirection.Input;
                _with16.Add("IATA_CODE_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("CHANNEL_PARTNER_IN", "0").Direction = ParameterDirection.Input;
                _with16.Add("LOCATION_AGENT_IN", "1").Direction = ParameterDirection.Input;

                _with16.Add("ACCOUNT_NO_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("LOCATION_MST_FK_IN", Convert.ToInt32(M_DataSet.Tables[0].Rows[0]["Location_MST_PK"])).Direction = ParameterDirection.Input;
                _with16.Add("VAT_NO_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("IATA_APPROVED_IN", "1").Direction = ParameterDirection.Input;

                _with16.Add("BUSINESS_TYPE_IN", "3").Direction = ParameterDirection.Input;
                _with16.Add("EXP_PROFIT_PER_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("EXP_COMM_PER_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("IMP_PROFIT_PER_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("IMP_COMM_PER_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                //' ADMINISTRATIVE
                _with16.Add("ADM_ADDRESS_1_IN", Convert.ToString(M_DataSet.Tables[0].Rows[0]["address_line1"])).Direction = ParameterDirection.Input;
                _with16.Add("ADM_ADDRESS_2_IN", getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["address_line2"]), "")).Direction = ParameterDirection.Input;
                _with16.Add("ADM_ADDRESS_3_IN", getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["address_line3"]), "")).Direction = ParameterDirection.Input;
                _with16.Add("ADM_CITY_IN", Convert.ToString(M_DataSet.Tables[0].Rows[0]["city"])).Direction = ParameterDirection.Input;
                _with16.Add("ADM_ZIP_CODE_IN", getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["zip"]), "")).Direction = ParameterDirection.Input;
                _with16.Add("ADM_COUNTRY_MST_FK_IN", Convert.ToInt32(M_DataSet.Tables[0].Rows[0]["country_mst_fk"])).Direction = ParameterDirection.Input;

                _with16.Add("ADM_SALUTATION_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("ADM_CONTACT_PERSON_IN", getDefault(Convert.ToString(M_DataSet.Tables[0].Rows[0]["office_name"]), "")).Direction = ParameterDirection.Input;
                _with16.Add("ADM_PHONE_NO_1_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("ADM_PHONE_NO_2_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("ADM_FAX_NO_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("ADM_EMAIL_ID_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("ADM_URL_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("ADM_SHORT_NAME_IN", "").Direction = ParameterDirection.Input;
                //SUMI START
                _with16.Add("REMARKS_IN", "").Direction = ParameterDirection.Input;
                //END
                //' CORRESPONDENCE
                _with16.Add("COR_ADDRESS_1_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_ADDRESS_2_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_ADDRESS_3_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_CITY_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_ZIP_CODE_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_COUNTRY_MST_FK_IN", "").Direction = ParameterDirection.Input;

                _with16.Add("COR_SALUTATION_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_CONTACT_PERSON_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_PHONE_NO_1_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_PHONE_NO_2_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_FAX_NO_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_EMAIL_ID_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_URL_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("COR_SHORT_NAME_IN", "").Direction = ParameterDirection.Input;
                //' BILLING
                _with16.Add("BILL_ADDRESS_1_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_ADDRESS_2_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_ADDRESS_3_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_CITY_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_ZIP_CODE_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_COUNTRY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_SALUTATION_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_CONTACT_PERSON_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_PHONE_NO_1_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_PHONE_NO_2_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_FAX_NO_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_EMAIL_ID_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_URL_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("BILL_SHORT_NAME_IN", "").Direction = ParameterDirection.Input;

                // ============= Parameters for Freight Elements =========================================
                _with16.Add("FREIGHT_FK_EXP_PS_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("FREIGHT_FK_EXP_CM_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("FREIGHT_FK_IMP_PS_IN", "").Direction = ParameterDirection.Input;
                _with16.Add("FREIGHT_FK_IMP_CM_IN", "").Direction = ParameterDirection.Input;
                // =======================================================================================
                _with16.Add("CREATEDD_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with16.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with16.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;

                exe = cmd.ExecuteNonQuery();

                arrMessage.Add("All data saved successfully");
                return arrMessage;
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
        }

        #endregion "Insert Into Agent_Mst_Tbl"

        #region "To Know Location Id Exist In Agent Id "

        // To Fetch LocationId of LOGED_IN_LOC_FK
        /// <summary>
        /// Gets the locaton identifier.
        /// </summary>
        /// <param name="LocationPK">The location pk.</param>
        /// <returns></returns>
        public DataSet GetLocatonID(string LocationPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("select lmt.location_id");
                strQuery.Append("  from location_mst_tbl lmt");
                strQuery.Append(" where lmt.location_mst_pk = '" + LocationPK + "'");
                strQuery.Append("");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "To Know Location Id Exist In Agent Id "

        #region "To Know Location Id Exist In Agent Id "

        // To Fetch LocationId of AOD/POD
        /// <summary>
        /// Gets the location.
        /// </summary>
        /// <param name="LocationPK">The location pk.</param>
        /// <returns></returns>
        public DataSet GetLocation(string LocationPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("select PMT.PORT_ID");
                strQuery.Append("   from PORT_MST_TBL PMT");
                strQuery.Append("  where PMT.PORT_MST_PK= '" + LocationPK + "'");
                strQuery.Append("");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "To Know Location Id Exist In Agent Id "

        #region "Fetching Document Preference"

        /// <summary>
        /// Fetch_s the document preference.
        /// </summary>
        /// <param name="P_Location_Mst_Fk">The p_ location_ MST_ fk.</param>
        /// <returns></returns>
        public DataSet Fetch_DocumentPreference(Int64 P_Location_Mst_Fk)
        {
            string strSQL = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            strSQL = "SELECT ROWNUM ";
            strSQL += " SLNO,";
            strSQL += "DOCUMENT_PREFERENCE_MST_PK PRFMSTPK,";
            strSQL += "DOCUMENT_PREFERENCE_ID PRF_ID,";
            strSQL += "DOCUMENT_PREFERENCE_NAME PRF_NAME,";
            strSQL += "To_char(ACTIVE) as active";
            strSQL += "FROM ";
            strSQL += "(SELECT ";
            strSQL += "DOCPREF.LOCATION_DWORKFLOW_PK DOCPREFPK, ";
            strSQL += "DOC.DOCUMENT_PREFERENCE_MST_PK, ";
            strSQL += "DOC.DOCUMENT_PREFERENCE_ID, ";
            strSQL += "DOC.DOCUMENT_PREFERENCE_NAME, ";
            strSQL += "DOCPREF.DOC_PREFERENCE_FK,";
            strSQL += "LMT.location_id, ";
            strSQL += "DOCPREF.Active,  ";
            strSQL += "DOCPREF.VERSION_NO ";
            strSQL += "FROM ";
            strSQL += "DOCUMENT_PREFERENCE_MST_TBL DOC, ";
            strSQL += "DOCUMENT_PREF_LOC_MST_TBL DOCPREF, ";
            strSQL += "LOCATION_MST_TBL LMT ";
            strSQL += "WHERE ";
            strSQL += "DOCPREF.DOC_PREFERENCE_FK =DOC.DOCUMENT_PREFERENCE_MST_PK ";
            strSQL += "AND DOCPREF.LOCATION_MST_FK  = " + P_Location_Mst_Fk + " ";
            strSQL += "AND LMT.location_mst_pk(+) = DOCPREF.LOCATION_MST_FK";
            strSQL += "  ";
            strSQL += "UNION  ";
            strSQL += "  ";
            strSQL += "SELECT  ";
            strSQL += "0 DOCPREFPK,   ";
            strSQL += "DOC.DOCUMENT_PREFERENCE_MST_PK,  ";
            strSQL += "DOC.DOCUMENT_PREFERENCE_ID,  ";
            strSQL += "DOC.DOCUMENT_PREFERENCE_NAME DOCUMENT_PREFERENCE_NAME,  ";
            strSQL += "0 DOC_PREFERENCE_FK,  ";
            strSQL += "'' location_id,";
            strSQL += "0 Active,  ";
            strSQL += "0 VERSION_NO  ";
            strSQL += "FROM   ";
            strSQL += "DOCUMENT_PREFERENCE_MST_TBL DOC ";
            strSQL += "WHERE  ";
            strSQL += "DOC.DOCUMENT_PREFERENCE_MST_PK ";
            strSQL += "NOT IN (  ";
            strSQL += "SELECT   ";
            strSQL += "DOCPREF.DOC_PREFERENCE_FK ";
            strSQL += "FROM  ";
            strSQL += "DOCUMENT_PREF_LOC_MST_TBL DOCPREF ";
            strSQL += "WHERE  ";
            //Strsql &= "LOCDEPT.Location_Mst_Fk = " & P_Location_Mst_Fk & ") ) " & vbCrLf
            strSQL += "DOCPREF.LOCATION_MST_FK= " + P_Location_Mst_Fk + ")";
            strSQL += " and DOC.ACTIVE_FLAG=1";
            strSQL += "ORDER BY DOCUMENT_PREFERENCE_NAME) ";
            strSQL += " ";

            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
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

        #endregion "Fetching Document Preference"

        #region "savepreference function"

        /// <summary>
        /// Saves the preference.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="LocationPK">The location pk.</param>
        /// <returns></returns>
        public Int64 SavePreference(DataSet M_DataSet, OracleTransaction TRAN, Int32 LocationPK)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.MyConnection = TRAN.Connection;

            int RecAfct = default(int);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();

            try
            {
                var _with17 = insCommand;
                _with17.Connection = objWK.MyConnection;
                _with17.CommandType = CommandType.StoredProcedure;
                _with17.CommandText = objWK.MyUserName + ".LOCATION_PREFERENCE_TRN_PKG.LOCATION_PREFERENCE_TRN_INS";
                var _with18 = _with17.Parameters;

                _with18.Add("LOCATION_MST_FK_IN", LocationPK).Direction = ParameterDirection.Input;
                _with18.Add("DOC_PREF_FK_IN", OracleDbType.Int32, 10, "DOC_PREFERENCE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["DOC_PREF_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with18.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;
                _with18.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.Int32, 10, "LOCATION_DWORKFLOW_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with19 = delCommand;
                _with19.Connection = objWK.MyConnection;
                _with19.CommandType = CommandType.StoredProcedure;
                _with19.CommandText = objWK.MyUserName + ".LOCATION_PREFERENCE_TRN_PKG.LOCATION_PREFERENCE_TRN_DEL";
                var _with20 = _with19.Parameters;
                delCommand.Parameters.Add("LOCATION_DWORKFLOW_PK_IN", OracleDbType.Int32, 10, "LOCATION_DWORKFLOW_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["LOCATION_DWORKFLOW_PK_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with21 = updCommand;
                _with21.Connection = objWK.MyConnection;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWK.MyUserName + ".LOCATION_PREFERENCE_TRN_PKG.LOCATION_PREFERENCE_TRN_UPDATE";
                var _with22 = _with21.Parameters;
                updCommand.Parameters.Add("LOCATION_DWORKFLOW_PK_IN", OracleDbType.Int32, 10, "LOCATION_DWORKFLOW_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["LOCATION_DWORKFLOW_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LOCATION_MST_FK_IN", LocationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("DOC_PREFERENCE_FK_IN", OracleDbType.Int32, 10, "DOC_PREFERENCE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["DOC_PREFERENCE_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with23 = objWK.MyDataAdapter;

                _with23.InsertCommand = insCommand;
                _with23.InsertCommand.Transaction = TRAN;
                _with23.UpdateCommand = updCommand;
                _with23.UpdateCommand.Transaction = TRAN;
                _with23.DeleteCommand = delCommand;
                _with23.DeleteCommand.Transaction = TRAN;
                RecAfct = _with23.Update(M_DataSet);
                return RecAfct;
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
            }
        }

        #endregion "savepreference function"

        #region "Delete All"

        /// <summary>
        /// Deletes all.
        /// </summary>
        /// <param name="LocationPk">The location pk.</param>
        /// <returns></returns>
        public bool DeleteAll(int LocationPk)
        {
            try
            {
                WorkFlow objwk = new WorkFlow();
                return objwk.ExecuteCommands("delete from DOCUMENT_PREF_LOC_MST_TBL WHERE LOCATION_MST_FK=" + LocationPk);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Delete All"

        #region "Month Cutoff Save"

        /// <summary>
        /// Monthlies the cuttoff save.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="LocationPK">The location pk.</param>
        /// <returns></returns>
        public Int32 MonthlyCuttoffSave(DataSet M_DataSet, OracleTransaction TRAN, Int32 LocationPK)
        {
            Int32 functionReturnValue = default(Int32);
            WorkFlow objWF = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            Int32 RecAfct = default(Int32);
            int RowCnt = 0;
            int i = 0;
            int intZero = 0;
            int intOne = 1;
            string MonthPK = "0";
            string strQry = null;
            if ((M_DataSet == null))
                return functionReturnValue;
            try
            {
                objWF.MyConnection = TRAN.Connection;
                if (M_DataSet.Tables[0].Rows.Count > 0)
                {
                    for (RowCnt = 0; RowCnt <= M_DataSet.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["MONTH_CLOSING_DAY"].ToString()))
                        {
                            var _with25 = insCommand;
                            _with25.Connection = objWF.MyConnection;
                            _with25.CommandType = CommandType.StoredProcedure;
                            _with25.CommandText = objWF.MyUserName + ".MONTH_END_CLOSING_TRN_PKG.MONTH_END_CLOSING_TRN_INS";
                            var _with26 = _with25.Parameters;
                            _with26.Clear();
                            _with26.Add("LOCATION_MST_FK_IN", LocationPK).Direction = ParameterDirection.Input;
                            _with26.Add("MONTH_YEAR_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["MONTH_YEAR"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["MONTH_YEAR"])).Direction = ParameterDirection.Input;
                            _with26.Add("MONTH_CLOSING_DAY_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["MONTH_CLOSING_DAY"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["MONTH_CLOSING_DAY"])).Direction = ParameterDirection.Input;
                            _with26.Add("STATUS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["STATUS_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["STATUS_PK"])).Direction = ParameterDirection.Input;
                            _with26.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            _with26.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with26.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            var _with27 = updCommand;
                            _with27.Connection = objWF.MyConnection;
                            _with27.CommandType = CommandType.StoredProcedure;
                            _with27.CommandText = objWF.MyUserName + ".MONTH_END_CLOSING_TRN_PKG.MONTH_END_CLOSING_TRN_UPD";
                            var _with28 = _with27.Parameters;
                            _with28.Clear();
                            _with28.Add("MONTH_END_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["MONTH_END_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["MONTH_END_PK"])).Direction = ParameterDirection.Input;
                            _with28.Add("LOCATION_MST_FK_IN", LocationPK).Direction = ParameterDirection.Input;
                            _with28.Add("MONTH_YEAR_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["MONTH_YEAR"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["MONTH_YEAR"])).Direction = ParameterDirection.Input;
                            _with28.Add("MONTH_CLOSING_DAY_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["MONTH_CLOSING_DAY"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["MONTH_CLOSING_DAY"])).Direction = ParameterDirection.Input;
                            _with28.Add("VERSION_NO_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["VERSION_NO"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["VERSION_NO"])).Direction = ParameterDirection.Input;
                            _with28.Add("STATUS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["STATUS_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["STATUS_PK"])).Direction = ParameterDirection.Input;
                            if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["STATUS_PK"]) == 2)
                            {
                                _with28.Add("MANUAL_OPEN_IN", intOne).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with28.Add("MANUAL_OPEN_IN", intZero).Direction = ParameterDirection.Input;
                            }
                            _with28.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            _with28.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with28.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;

                            var _with29 = objWF.MyDataAdapter;
                            MonthPK = Convert.ToString(string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["MONTH_END_PK"].ToString()) ? 0 : M_DataSet.Tables[0].Rows[RowCnt]["MONTH_END_PK"]);
                            if (Convert.ToInt32(MonthPK) <= 0)
                            {
                                _with29.InsertCommand = insCommand;
                                _with29.InsertCommand.Transaction = TRAN;
                                RecAfct = RecAfct + _with29.InsertCommand.ExecuteNonQuery();
                            }
                            else
                            {
                                _with29.UpdateCommand = updCommand;
                                _with29.UpdateCommand.Transaction = TRAN;
                                RecAfct = RecAfct + _with29.UpdateCommand.ExecuteNonQuery();
                            }
                        }
                    }
                }

                return RecAfct;
            }
            catch (OracleException oraEx)
            {
                throw oraEx;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        #endregion "Month Cutoff Save"
    }
}