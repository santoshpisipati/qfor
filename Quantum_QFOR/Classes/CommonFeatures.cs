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

using CrystalDecisions.CrystalReports.Engine;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ error message
        /// </summary>
        protected string M_ErrorMessage = "";

        /// <summary>
        /// The m_ create d_ b y_ fk
        /// </summary>
        protected long M_CREATED_BY_FK;

        /// <summary>
        /// The m_ grid col no
        /// </summary>
        protected int M_GridColNo = 0;

        /// <summary>
        /// The result
        /// </summary>
        protected static int result = 0;

        /// <summary>
        /// The m_blank_ grid
        /// </summary>
        protected int M_blank_Grid = 0;

        /// <summary>
        /// The m_ create d_ dt
        /// </summary>
        protected DateTime M_CREATED_DT;

        /// <summary>
        /// The m_ las t_ modifie d_ b y_ fk
        /// </summary>
        protected long M_LAST_MODIFIED_BY_FK;

        /// <summary>
        /// The m_ las t_ modifie d_ dt
        /// </summary>
        protected DateTime M_LAST_MODIFIED_DT;

        /// <summary>
        /// The m_ versio n_ no
        /// </summary>
        protected long M_VERSION_NO;

        /// <summary>
        /// The m_ configuration_ pk
        /// </summary>
        protected long M_Configuration_PK;

        /// <summary>
        /// The m_ ar r_ o r_ dep
        /// </summary>
        protected long M_ARR_OR_DEP;

        /// <summary>
        /// The m_ ca r_ hd r_ fk
        /// </summary>
        protected long M_CAR_HDR_FK;

        /// <summary>
        /// The m_ master page size
        /// </summary>
        public int M_MasterPageSize = 15;

        /// <summary>
        /// The arr message
        /// </summary>
        protected ArrayList arrMessage = new ArrayList();

        //variable to maintain the logged in user location
        /// <summary>
        /// The m_ logged in_ loc_ fk
        /// </summary>
        protected long M_LoggedIn_Loc_FK;

        //FOR EDI
        /// <summary>
        /// The m_ container exists
        /// </summary>
        protected bool M_ContainerExists;

        /// <summary>
        /// The m_ container exist status
        /// </summary>
        protected bool M_ContainerExistStatus;

        /// <summary>
        /// The m_ no records
        /// </summary>
        protected bool M_NoRecords;

        /// <summary>
        /// The m_ records to save
        /// </summary>
        protected bool M_RecordsToSave;

        /// <summary>
        /// The m_ exceed container allocated
        /// </summary>
        protected bool M_ExceedContainerAllocated;

        /// <summary>
        /// The m_ container_ type_ MST_ fk edi
        /// </summary>
        protected Int64 M_Container_Type_Mst_FkEdi;

        /// <summary>
        /// The m_ execute pk
        /// </summary>
        protected long M_ExecPk;

        #endregion "List of Members of the Class"

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the logged in_ loc_ fk.
        /// </summary>
        /// <value>
        /// The logged in_ loc_ fk.
        /// </value>
        public Int64 LoggedIn_Loc_FK
        {
            get { return M_LoggedIn_Loc_FK; }
            set { M_LoggedIn_Loc_FK = value; }
        }

        /// <summary>
        /// Gets the records per page.
        /// </summary>
        /// <value>
        /// The records per page.
        /// </value>
        public Int32 RecordsPerPage
        {
            get { return 10; }
        }

        /// <summary>
        /// Gets or sets the size of the master page.
        /// </summary>
        /// <value>
        /// The size of the master page.
        /// </value>
        public Int32 MasterPageSize
        {
            get { return M_MasterPageSize; }
            set { M_MasterPageSize = value; }
        }

        /// <summary>
        /// Gets or sets the ed i_ ca r_ hd r_ fk.
        /// </summary>
        /// <value>
        /// The ed i_ ca r_ hd r_ fk.
        /// </value>
        public Int64 EDI_CAR_HDR_FK
        {
            get { return M_CAR_HDR_FK; }
            set { M_CAR_HDR_FK = value; }
        }

        /// <summary>
        /// Gets or sets the created_ at.
        /// </summary>
        /// <value>
        /// The created_ at.
        /// </value>
        public System.DateTime Created_AT
        {
            get { return M_CREATED_DT; }
            set { M_CREATED_DT = value; }
        }

        /// <summary>
        /// Gets or sets the last_ modified_ at.
        /// </summary>
        /// <value>
        /// The last_ modified_ at.
        /// </value>
        public System.DateTime Last_Modified_At
        {
            get { return M_LAST_MODIFIED_DT; }
            set { M_LAST_MODIFIED_DT = value; }
        }

        /// <summary>
        /// Gets or sets the las t_ modifie d_ by.
        /// </summary>
        /// <value>
        /// The las t_ modifie d_ by.
        /// </value>
        public Int64 LAST_MODIFIED_BY
        {
            get { return M_LAST_MODIFIED_BY_FK; }
            set { M_LAST_MODIFIED_BY_FK = value; }
        }

        /// <summary>
        /// Gets or sets the create d_ by.
        /// </summary>
        /// <value>
        /// The create d_ by.
        /// </value>
        public Int64 CREATED_BY
        {
            get { return M_CREATED_BY_FK; }
            set { M_CREATED_BY_FK = value; }
        }

        /// <summary>
        /// Gets or sets the version_ no.
        /// </summary>
        /// <value>
        /// The version_ no.
        /// </value>
        public Int64 Version_No
        {
            get { return M_VERSION_NO; }
            set { M_VERSION_NO = value; }
        }

        /// <summary>
        /// Gets or sets the error message.
        /// </summary>
        /// <value>
        /// The error message.
        /// </value>
        public string ErrorMessage
        {
            get { return M_ErrorMessage; }
            set { M_ErrorMessage = value; }
        }

        /// <summary>
        /// Gets or sets the configuration pk.
        /// </summary>
        /// <value>
        /// The configuration pk.
        /// </value>
        public Int64 ConfigurationPK
        {
            get { return M_Configuration_PK; }
            set { M_Configuration_PK = value; }
        }

        /// <summary>
        /// Gets or sets the ar r_ dep.
        /// </summary>
        /// <value>
        /// The ar r_ dep.
        /// </value>
        public Int64 ARR_DEP
        {
            get { return M_ARR_OR_DEP; }
            set { M_ARR_OR_DEP = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is container exist.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is container exist; otherwise, <c>false</c>.
        /// </value>
        public bool IsContainerExist
        {
            get { return M_ContainerExists; }
            set { M_ContainerExists = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether [container exist status].
        /// </summary>
        /// <value>
        /// <c>true</c> if [container exist status]; otherwise, <c>false</c>.
        /// </value>
        public bool ContainerExistStatus
        {
            get { return M_ContainerExistStatus; }
            set { M_ContainerExistStatus = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether [no records].
        /// </summary>
        /// <value>
        ///   <c>true</c> if [no records]; otherwise, <c>false</c>.
        /// </value>
        public bool NoRecords
        {
            get { return M_NoRecords; }
            set { M_NoRecords = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether [records to save].
        /// </summary>
        /// <value>
        ///   <c>true</c> if [records to save]; otherwise, <c>false</c>.
        /// </value>
        public bool RecordsToSave
        {
            get { return M_RecordsToSave; }
            set { M_RecordsToSave = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether [excess container allocation].
        /// </summary>
        /// <value>
        /// <c>true</c> if [excess container allocation]; otherwise, <c>false</c>.
        /// </value>
        public bool ExcessContainerAllocation
        {
            get { return M_ExceedContainerAllocated; }
            set { M_ExceedContainerAllocated = value; }
        }

        /// <summary>
        /// Gets or sets the container_ type_ MST_ fk edi.
        /// </summary>
        /// <value>
        /// The container_ type_ MST_ fk edi.
        /// </value>
        public Int64 Container_Type_Mst_FkEdi
        {
            get { return M_Container_Type_Mst_FkEdi; }
            set { M_Container_Type_Mst_FkEdi = value; }
        }

        // Added for BOF [ 24-Apr-2006 ] Rajesh
        /// <summary>
        /// The m_ bof pk
        /// </summary>
        private long M_BofPk = -1;

        /// <summary>
        /// Gets or sets the bof pk.
        /// </summary>
        /// <value>
        /// The bof pk.
        /// </value>
        public long BofPk
        {
            get { return M_BofPk; }
            set { M_BofPk = value; }
        }

        /// <summary>
        /// Gets or sets the grid col no.
        /// </summary>
        /// <value>
        /// The grid col no.
        /// </value>
        public int GridColNo
        {
            get { return M_GridColNo; }
            set { M_GridColNo = value; }
        }

        /// <summary>
        /// Gets or sets the blank grid.
        /// </summary>
        /// <value>
        /// The blank grid.
        /// </value>
        public int BlankGrid
        {
            get { return M_blank_Grid; }
            set { M_blank_Grid = value; }
        }

        //add by latha
        /// <summary>
        /// Gets or sets the execute pk.
        /// </summary>
        /// <value>
        /// The execute pk.
        /// </value>
        public long ExecPk
        {
            get { return M_ExecPk; }
            set { M_ExecPk = value; }
        }

        #endregion "List of Properties"

        #region "Generate Protocol Key"

        /// <summary>
        /// Generates the protocol key.
        /// </summary>
        /// <param name="sProtocolName">Name of the s protocol.</param>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <param name="ProtocolDate">The protocol date.</param>
        /// <param name="sVSL">The s VSL.</param>
        /// <param name="sVOY">The s voy.</param>
        /// <param name="sPOL">The s pol.</param>
        /// <param name="UserId">The user identifier.</param>
        /// <param name="objWS">The object ws.</param>
        /// <param name="SID">The sid.</param>
        /// <param name="PODID">The podid.</param>
        /// <returns></returns>
        protected string GenerateProtocolKey(string sProtocolName, Int64 ILocationId, Int64 IEmployeeId, System.DateTime ProtocolDate, string sVSL = "", string sVOY = "", string sPOL = "", Int64 UserId = 0, WorkFlow objWS = null, string SID = "",
        string PODID = "")
        {
            //Added Optional ByVal Userid As Int64 = 0 as parameter for EDI as there is no scope for the user
            //so when using this function , call  Session("USER_PK") as parameter
            bool bNewObject = false;
            if (objWS == null)
            {
                bNewObject = true;
                objWS = new WorkFlow();
            }
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                objWS.MyCommand.CommandText = objWS.MyUserName + ".GENERATE_PROTOCOL_KEY";
                objWS.MyCommand.Parameters.Clear();
                var _with1 = objWS.MyCommand.Parameters;
                _with1.Add("PROTOCOL_NAME_IN", sProtocolName).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", ILocationId).Direction = ParameterDirection.Input;
                _with1.Add("EMPLOYEE_MST_FK_IN", IEmployeeId).Direction = ParameterDirection.Input;
                _with1.Add("USER_MST_FK_IN", UserId).Direction = ParameterDirection.Input;
                _with1.Add("DATE_IN", ProtocolDate).Direction = ParameterDirection.Input;
                _with1.Add("VSL_IN", (string.IsNullOrEmpty(sVSL) ? "" : sVSL)).Direction = ParameterDirection.Input;
                _with1.Add("VOY_IN", (string.IsNullOrEmpty(sVOY) ? "" : sVOY)).Direction = ParameterDirection.Input;
                _with1.Add("POL_IN", (string.IsNullOrEmpty(sPOL) ? "" : sPOL)).Direction = ParameterDirection.Input;
                _with1.Add("SL_IN", (string.IsNullOrEmpty(sPOL) ? "" : SID)).Direction = ParameterDirection.Input;
                _with1.Add("POD_IN", (string.IsNullOrEmpty(sPOL) ? "" : PODID)).Direction = ParameterDirection.Input;
                objWS.MyCommand.Parameters["VSL_IN"].Size = 20;
                objWS.MyCommand.Parameters["VOY_IN"].Size = 20;
                objWS.MyCommand.Parameters["POL_IN"].Size = 20;
                _with1.Add("RETURN_VALUE", OracleDbType.Varchar2, 30).Direction = ParameterDirection.Output;

                if (!bNewObject)
                {
                    objWS.MyCommand.ExecuteNonQuery();
                    return objWS.MyCommand.Parameters["RETURN_VALUE"].Value.ToString();
                }

                if (objWS.ExecuteCommands())
                {
                    return objWS.MyCommand.Parameters["RETURN_VALUE"].Value.ToString();
                }
                else
                {
                    return "Protocol Not Defined.";
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the barcode flag.
        /// </summary>
        /// <param name="protocolId">The protocol identifier.</param>
        /// <returns></returns>
        public string GetBarcodeFlag(string protocolId)
        {
            try
            {
                string strQuery = "SELECT Get_Barcode_flage('" + protocolId + "')FROM dual";
                WorkFlow objWF = new WorkFlow();
                return objWF.ExecuteScaler(strQuery);
            }
            catch (Exception ex)
            {
                return "";
            }
        }

        #endregion "Generate Protocol Key"

        #region "Get Server Time"

        /// <summary>
        /// Gets the server date time.
        /// </summary>
        /// <returns></returns>
        protected string GetServerDateTime()
        {
            string StrSql = null;
            string strTim = null;
            DataRow Dr = null;
            StrSql = "SELECT to_char(SYSDATE,'dd-Mon-yyyy hh24:mi:ss') CurDate FROM dual";
            WorkFlow ObjWs = new WorkFlow();
            ObjWs.MyCommand.CommandType = CommandType.Text;
            ObjWs.MyCommand.CommandText = StrSql;
            strTim = Convert.ToString(ObjWs.MyCommand.ExecuteScalar());
            return strTim;
        }

        #endregion "Get Server Time"

        #region "Fetch Local Currency"

        //modified  By Thiyagarajan 18/11/08 for getting location based currency task
        //for getting sell exchange rate
        /// <summary>
        /// Fetches the curr and exchange.
        /// </summary>
        /// <param name="RFQDate">The RFQ date.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="From">From.</param>
        /// <param name="RefFK">The reference fk.</param>
        /// <returns></returns>
        public DataSet FetchCurrAndExchange(string RFQDate = "", int ExType = 1, string From = "", long RefFK = 0)
        {
            DataSet DS = null;
            try
            {
                string strSQL = null;
                string locPk = HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString();
                string strCondition = null;
                if (string.IsNullOrEmpty(RFQDate))
                {
                    strCondition = " AND ROUND(SYSDATE-0.5) Between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                    //nvl(TO_DATE,'31-Dec-9999') "
                }
                else
                {
                    strCondition = " AND TO_DATE(' " + RFQDate + "','" + dateFormat + "') between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                    //nvl(TO_DATE,'31-Dec-9999') "
                }
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT C.CURRENCY_MST_PK,");
                sb.Append("       C.CURRENCY_ID CURRENCY_ID,");
                sb.Append("       C.CURRENCY_NAME,");
                sb.Append("       1 EXCHANGE_RATE");
                sb.Append("  FROM CURRENCY_TYPE_MST_TBL C, COUNTRY_MST_TBL COUNTRY");
                sb.Append(" WHERE C.CURRENCY_MST_PK = COUNTRY.CURRENCY_MST_FK");
                sb.Append("   AND COUNTRY.COUNTRY_MST_PK =");
                sb.Append("       (select loc.country_mst_fk");
                sb.Append("          from location_mst_tbl loc");
                sb.Append("         where loc.location_mst_pk = " + locPk + ")");
                sb.Append("   AND C.ACTIVE_FLAG = 1");

                sb.Append(" UNION ");

                sb.Append(" SELECT CURR.CURRENCY_MST_PK,");
                sb.Append("       CURR.CURRENCY_ID,");
                sb.Append("       CURR.CURRENCY_NAME,");
                sb.Append("       EXCHANGE_RATE");
                sb.Append("  FROM CURRENCY_TYPE_MST_TBL CURR, V_EXCHANGE_RATE_TYPE EXC");
                sb.Append(" WHERE EXC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND CURR.ACTIVE_FLAG = 1");
                sb.Append("   AND EXC.EXCH_RATE_TYPE_FK = " + ExType.ToString());
                sb.Append("   AND EXC.CURRENCY_MST_BASE_FK =");
                sb.Append("       (SELECT CONTRY.CURRENCY_MST_FK");
                sb.Append("          FROM COUNTRY_MST_TBL CONTRY");
                sb.Append("         WHERE CONTRY.COUNTRY_MST_PK =");
                sb.Append("               (select loc.country_mst_fk");
                sb.Append("                  from location_mst_tbl loc");
                sb.Append("                 where loc.location_mst_pk = " + locPk + "))");
                sb.Append("   AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK ");
                sb.Append(strCondition);

                if ((!string.IsNullOrEmpty(RFQDate) & !string.IsNullOrEmpty(From)) | From == "SEAENQUIRY")
                {
                    sb.Append(" UNION SELECT DISTINCT EX.TO_CURRENCY_FK,  CTMT.CURRENCY_ID, CTMT.CURRENCY_NAME, EX.EXCHANGE_RATE ");
                    sb.Append(" FROM TEMP_EX_RATE_TRN EX, CURRENCY_TYPE_MST_TBL CTMT ");
                    sb.Append(" WHERE EX.TO_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                    sb.Append(" AND EX.BASE_CURRENCY_FK = " + HttpContext.Current.Session["CURRENCY_MST_PK"].ToString() + "");
                    if (!string.IsNullOrEmpty(RFQDate))
                    {
                        sb.Append("  AND to_date(EX.FROM_DATE,'dd/MM/yyyy') = to_date('" + RFQDate + "','dd/MM/yyyy')");
                    }
                    sb.Append("  AND EX.FROM_FLAG = '" + From + "'");
                    sb.Append("  AND EX.TO_CURRENCY_FK NOT IN(" + HttpContext.Current.Session["CURRENCY_MST_PK"].ToString() + ")");
                    sb.Append("  AND EX.TO_CURRENCY_FK NOT IN( ");
                    sb.Append("SELECT  CURR.CURRENCY_MST_PK FROM CURRENCY_TYPE_MST_TBL CURR , V_EXCHANGE_RATE_TYPE EXC WHERE   ");
                    sb.Append("EXC.CURRENCY_MST_FK  = CURR.CURRENCY_MST_PK  ");
                    sb.Append("AND CURR.ACTIVE_FLAG = 1  AND EXC.EXCH_RATE_TYPE_FK = " + ExType + "");
                    sb.Append("AND EXC.CURRENCY_MST_BASE_FK =(SELECT CONTRY.CURRENCY_MST_FK FROM COUNTRY_MST_TBL CONTRY WHERE CONTRY.COUNTRY_MST_PK= ");
                    sb.Append(" (select loc.country_mst_fk from location_mst_tbl loc where loc.location_mst_pk=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString() + " )) ");
                    sb.Append("AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK ");
                    sb.Append(" " + strCondition + " )");
                    if (RefFK != 0)
                    {
                        sb.Append(" AND EX.REF_FK = " + RefFK);
                    }
                    else
                    {
                        sb.Append(" AND EX.REF_FK = 0");
                    }
                }
                sb.Append(" ORDER BY CURRENCY_ID");
                DS = (new WorkFlow()).GetDataSet(sb.ToString());
                if (DS.Tables[0].Rows.Count > 0)
                {
                    short RowCnt = default(Int16);
                    string CurrId = Convert.ToString(DS.Tables[0].Rows[0]["CURRENCY_ID"]).Trim();
                    DS.Tables[0].Rows[0]["CURRENCY_ID"] = CurrId;
                }
            }
            catch (Exception ex)
            {
            }
            return DS;
        }

        //------------------------------------------------------------------------------------
        //for getting buy exchange rate
        /// <summary>
        /// Fetches the curr and exchange buy.
        /// </summary>
        /// <param name="RFQDate">The RFQ date.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="From">From.</param>
        /// <param name="RefFK">The reference fk.</param>
        /// <returns></returns>
        public DataSet FetchCurrAndExchangeBuy(string RFQDate = "", int ExType = 1, string From = "", long RefFK = 0)
        {
            try
            {
                string strSQL = null;
                string strCondition = null;
                if (string.IsNullOrEmpty(RFQDate))
                {
                    strCondition = " AND ROUND(SYSDATE-0.5) Between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                    //nvl(TO_DATE,'31-Dec-9999') "
                }
                else
                {
                    strCondition = " AND TO_DATE('" + RFQDate + "','" + dateFormat + "') between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                    //nvl(TO_DATE,'31-Dec-9999') "
                }
                strSQL = " SELECT C.CURRENCY_MST_PK, " + "    ' ' || C.CURRENCY_ID        CURRENCY_ID,    " + "    C.CURRENCY_NAME,                            " + "    1            EXCHANGE_RATE   " + "FROM    CURRENCY_TYPE_MST_TBL      C   ,         " + "COUNTRY_MST_TBL COUNTRY WHERE                      C.CURRENCY_MST_PK=COUNTRY.CURRENCY_MST_FK   " + "AND COUNTRY.COUNTRY_MST_PK=(select loc.country_mst_fk from location_mst_tbl loc where loc.location_mst_pk=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString() + ")" + "AND C.ACTIVE_FLAG   =   1                       " + " UNION                                            " + " SELECT                                           " + "    CURR.CURRENCY_MST_PK,                       " + "    CURR.CURRENCY_ID,                           " + "    CURR.CURRENCY_NAME,                         " + "    NVL(ROE_BUY,EXCHANGE_RATE) EXCHANGE_RATE    " + "FROM CURRENCY_TYPE_MST_TBL CURR,                " + "    V_EXCHANGE_RATE_TYPE EXC                         " + "WHERE                                           " + "    EXC.CURRENCY_MST_FK             =   CURR.CURRENCY_MST_PK        " + "    AND CURR.ACTIVE_FLAG            =   1  AND EXC.EXCH_RATE_TYPE_FK = " + ExType + " " + "    AND EXC.CURRENCY_MST_BASE_FK    =                               " + "(SELECT CONTRY.CURRENCY_MST_FK FROM COUNTRY_MST_TBL CONTRY WHERE CONTRY.COUNTRY_MST_PK=" + "(select loc.country_mst_fk from location_mst_tbl loc where loc.location_mst_pk=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString() + " ))  " + "AND EXC.CURRENCY_MST_BASE_FK   <> EXC.CURRENCY_MST_FK       " + strCondition + "";

                if (!string.IsNullOrEmpty(RFQDate) & !string.IsNullOrEmpty(From))
                {
                    strSQL = strSQL + "     AND EXC.CURRENCY_MST_FK not in  ";
                    strSQL = strSQL + "      (SELECT EXC.CURRENCY_MST_FK      ";
                    strSQL = strSQL + " FROM V_EXCHANGE_RATE_TYPE EXC ";
                    strSQL = strSQL + "WHERE EXC.EXCH_RATE_TYPE_FK = " + ExType + " ";
                    strSQL = strSQL + "  AND EXC.CURRENCY_MST_BASE_FK = ";
                    strSQL = strSQL + "      (SELECT CONTRY.CURRENCY_MST_FK ";
                    strSQL = strSQL + "         FROM COUNTRY_MST_TBL CONTRY ";
                    strSQL = strSQL + "        WHERE CONTRY.COUNTRY_MST_PK = ";
                    strSQL = strSQL + "              (SELECT LOC.COUNTRY_MST_FK ";
                    strSQL = strSQL + "                 FROM LOCATION_MST_TBL LOC ";
                    strSQL = strSQL + "                WHERE LOC.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString() + ")) ";
                    strSQL = strSQL + "    AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK ";
                    strSQL = strSQL + "    AND to_date(EXC.FROM_DATE,'dd/MM/yyyy')=to_date('" + RFQDate + "','dd/MM/yyyy') AND to_date(exc.TO_DATE,'dd/MM/yyyy')=to_date('" + RFQDate + "','dd/MM/yyyy') )  ";

                    strSQL = strSQL + "    UNION  ";
                    strSQL = strSQL + "    SELECT CURR.CURRENCY_MST_PK, ";
                    strSQL = strSQL + "        CURR.CURRENCY_ID, ";
                    strSQL = strSQL + "        CURR.CURRENCY_NAME, ";
                    strSQL = strSQL + "        NVL(ROE_BUY, EXCHANGE_RATE) EXCHANGE_RATE ";
                    strSQL = strSQL + "   FROM CURRENCY_TYPE_MST_TBL CURR, V_EXCHANGE_RATE_TYPE EXC ";
                    strSQL = strSQL + "  WHERE EXC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ";
                    strSQL = strSQL + "    AND CURR.ACTIVE_FLAG = 1 ";
                    strSQL = strSQL + "    AND EXC.EXCH_RATE_TYPE_FK = " + ExType + " ";
                    strSQL = strSQL + "    AND EXC.CURRENCY_MST_BASE_FK = ";
                    strSQL = strSQL + "        (SELECT CONTRY.CURRENCY_MST_FK ";
                    strSQL = strSQL + "           FROM COUNTRY_MST_TBL CONTRY ";
                    strSQL = strSQL + "          WHERE CONTRY.COUNTRY_MST_PK = ";
                    strSQL = strSQL + "                (SELECT LOC.COUNTRY_MST_FK ";
                    strSQL = strSQL + "                   FROM LOCATION_MST_TBL LOC ";
                    strSQL = strSQL + "                  WHERE LOC.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString() + ")) ";
                    strSQL = strSQL + "    AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK ";
                    //strSQL = strSQL & "    AND exc.FROM_DATE='04/08/2015' AND exc.TO_DATE='04/08/2015'    "
                    strSQL = strSQL + "    AND to_date(EXC.FROM_DATE,'dd/MM/yyyy')=to_date('" + RFQDate + "','dd/MM/yyyy') AND to_date(exc.TO_DATE,'dd/MM/yyyy')=to_date('" + RFQDate + "','dd/MM/yyyy')   ";

                    //*************

                    strSQL = strSQL + " UNION SELECT EX.TO_CURRENCY_FK,  CTMT.CURRENCY_ID, CTMT.CURRENCY_NAME, EX.EXCHANGE_RATE ";
                    strSQL = strSQL + " FROM TEMP_EX_RATE_TRN EX, CURRENCY_TYPE_MST_TBL CTMT ";
                    strSQL = strSQL + " WHERE EX.TO_CURRENCY_FK = CTMT.CURRENCY_MST_PK";
                    strSQL = strSQL + " AND EX.BASE_CURRENCY_FK = " + HttpContext.Current.Session["CURRENCY_MST_PK"].ToString() + "";
                    strSQL = strSQL + "  AND to_date(EX.FROM_DATE,'dd/MM/yyyy') = to_date('" + RFQDate + "','dd/MM/yyyy')";
                    strSQL = strSQL + "  AND EX.FROM_FLAG = '" + From + "'";
                    strSQL = strSQL + "  AND EX.TO_CURRENCY_FK NOT IN(" + HttpContext.Current.Session["CURRENCY_MST_PK"].ToString() + ")";
                    strSQL = strSQL + "  AND EX.TO_CURRENCY_FK NOT IN( ";
                    strSQL = strSQL + "  SELECT  CURR.CURRENCY_MST_PK FROM CURRENCY_TYPE_MST_TBL CURR , V_EXCHANGE_RATE_TYPE EXC WHERE   ";
                    strSQL = strSQL + "  EXC.CURRENCY_MST_FK  = CURR.CURRENCY_MST_PK  ";
                    strSQL = strSQL + "  AND CURR.ACTIVE_FLAG = 1  AND EXC.EXCH_RATE_TYPE_FK = " + ExType + "";
                    strSQL = strSQL + "  AND EXC.CURRENCY_MST_BASE_FK =(SELECT CONTRY.CURRENCY_MST_FK FROM COUNTRY_MST_TBL CONTRY WHERE CONTRY.COUNTRY_MST_PK= ";
                    strSQL = strSQL + " (select loc.country_mst_fk from location_mst_tbl loc where loc.location_mst_pk=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString() + " )) ";
                    strSQL = strSQL + "  AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK ";
                    strSQL = strSQL + "   " + strCondition + " )";
                    if (RefFK != 0)
                    {
                        strSQL = strSQL + " AND EX.REF_FK = " + RefFK;
                    }
                    else
                    {
                        strSQL = strSQL + " AND EX.REF_FK = 0";
                    }
                }
                strSQL = strSQL + "  ORDER BY  CURRENCY_ID   ";
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                DataSet DS = null;
                DS = (new WorkFlow()).GetDataSet(strSQL);
                short RowCnt = default(Int16);
                string CurrId = Convert.ToString(DS.Tables[0].Rows[0]["CURRENCY_ID"]).Trim();
                DS.Tables[0].Rows[0]["CURRENCY_ID"] = CurrId;
                return DS;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the active currency.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchActiveCurrency()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string locPk = HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString();
            sb.Append("SELECT C.CURRENCY_MST_PK,");
            sb.Append("       C.CURRENCY_ID CURRENCY_ID,");
            sb.Append("       C.CURRENCY_NAME,");
            sb.Append("       1 EXCHANGE_RATE");
            sb.Append("  FROM CURRENCY_TYPE_MST_TBL C, COUNTRY_MST_TBL COUNTRY");
            sb.Append(" WHERE C.CURRENCY_MST_PK = COUNTRY.CURRENCY_MST_FK");
            sb.Append("   AND COUNTRY.COUNTRY_MST_PK =");
            sb.Append("       (select loc.country_mst_fk");
            sb.Append("          from location_mst_tbl loc");
            sb.Append("         where loc.location_mst_pk = " + locPk + ")");
            sb.Append("   AND C.ACTIVE_FLAG = 1");

            sb.Append(" UNION ");

            sb.Append(" SELECT CURR.CURRENCY_MST_PK,");
            sb.Append("       CURR.CURRENCY_ID,");
            sb.Append("       CURR.CURRENCY_NAME,");
            sb.Append("       0 EXCHANGE_RATE");
            sb.Append("  FROM CURRENCY_TYPE_MST_TBL CURR ");
            sb.Append(" WHERE CURR.CURRENCY_MST_PK NOT IN ");
            sb.Append("   (SELECT COUNTRY.CURRENCY_MST_FK ");
            sb.Append("   FROM LOCATION_MST_TBL LOC, COUNTRY_MST_TBL COUNTRY ");
            sb.Append("      WHERE LOC.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK ");
            sb.Append("          AND LOC.LOCATION_MST_PK = " + locPk + ") ");
            sb.Append("   AND CURR.ACTIVE_FLAG = 1");
            sb.Append(" ORDER BY CURRENCY_ID");

            DataSet DS = null;
            DS = (new WorkFlow()).GetDataSet(sb.ToString());
            return DS;
        }

        //adding by thiyagarajan on 20/2/09:Fetch Exchange Rates while changing  other currency  as base currency in Receivables Module
        //As per Manoranjan suggestion,the block created
        /// <summary>
        /// Fetches the exchange for oth current.
        /// </summary>
        /// <param name="CurrPk">The curr pk.</param>
        /// <param name="RFQDate">The RFQ date.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <returns></returns>
        public DataSet FetchExchangeForOthCur(Int32 CurrPk, string RFQDate = "", int ExType = 1)
        {
            try
            {
                string strSQL = null;
                string strCondition = null;
                if (string.IsNullOrEmpty(RFQDate))
                {
                    strCondition = " AND ROUND(SYSDATE-0.5) Between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                    //nvl(TO_DATE,'31-Dec-9999') "
                }
                else
                {
                    strCondition = " AND TO_DATE(' " + RFQDate + "','" + dateFormat + "') between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                    //nvl(TO_DATE,'31-Dec-9999') "
                }
                strSQL = " SELECT C.CURRENCY_MST_PK, " + "' ' || C.CURRENCY_ID  CURRENCY_ID,    " + "C.CURRENCY_NAME,    " + "1   EXCHANGE_RATE   " + "  FROM    CURRENCY_TYPE_MST_TBLC" + "  WHERE C.CURRENCY_MST_PK=  " + CurrPk + "  AND C.ACTIVE_FLAG   =   1     " + " UNION  " + " SELECT " + "CURR.CURRENCY_MST_PK,                       " + "      CURR.CURRENCY_ID,                           " + "      CURR.CURRENCY_NAME,                         " + "      EXCHANGE_RATE                               " + "  FROM CURRENCY_TYPE_MST_TBL CURR,                " + "      V_EXCHANGE_RATE_TYPE EXC                         " + "  WHERE                                           " + "      EXC.CURRENCY_MST_FK             =   CURR.CURRENCY_MST_PK        " + "      AND CURR.ACTIVE_FLAG            =   1  AND EXC.EXCH_RATE_TYPE_FK = " + ExType + " " + "      AND EXC.CURRENCY_MST_BASE_FK    =   " + CurrPk + "      AND EXC.CURRENCY_MST_BASE_FK   <>   EXC.CURRENCY_MST_FK       " + strCondition + "   ORDER BY        CURRENCY_ID      ";
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                DataSet DS = null;
                DS = (new WorkFlow()).GetDataSet(strSQL);
                //Dim RowCnt As Int16
                //Dim CurrId As String = CStr(DS.Tables(0).Rows(0).Item("CURRENCY_ID")).Trim
                //DS.Tables(0).Rows(0).Item("CURRENCY_ID") = CurrId
                return DS;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //end by thiyagarajan on 20/2/09
        /// <summary>
        /// Gets the inv currency.
        /// </summary>
        /// <param name="Invpk">The invpk.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public DataSet GetInvCurrency(string Invpk, Int32 Biz, Int32 process)
        {
            string strSQL = null;
            WorkFlow objwf = new WorkFlow();
            try
            {
                strSQL = " select CUR.CURRENCY_ID,CUR.CURRENCY_MST_PK from INV_AGENT_TBL INV,CURRENCY_TYPE_MST_TBL CUR WHERE ";
                strSQL += " CUR.CURRENCY_MST_PK=INV.CURRENCY_MST_FK AND INV.INV_AGENT_PK=" + Invpk;
                if (Biz == 1)
                {
                    strSQL = strSQL.Replace("SEA", "AIR");
                }
                if (process == 2)
                {
                    strSQL = strSQL.Replace("EXP", "IMP");
                }
                return objwf.GetDataSet(strSQL);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #endregion "Fetch Local Currency"

        #region "Fetch Exchange Rate "

        //commented by thiyagarajan on 18/11/08 for fetching location based currency instead of Corporate Currency task
        // ' Created By Akhilesh : 12-Dec-2005 06:00PM
        // ' Modified By Rajesh  : 28-Feb-2006 12:45PM for inclusive comparision of date and using V_Exchange_Rate View
        // 'MODIFIED
        /// <summary>
        /// Fetches the location curr and exchange.
        /// </summary>
        /// <param name="locpk">The locpk.</param>
        /// <param name="RFQDate">The RFQ date.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <returns></returns>
        public DataSet FetchLocationCurrAndExchange(Int32 locpk, string RFQDate = "", int ExType = 1)
        {
            try
            {
                string strSQL = null;
                string strCondition = null;
                if (string.IsNullOrEmpty(RFQDate))
                {
                    strCondition = " AND ROUND(SYSDATE-0.5) Between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                    //nvl(TO_DATE,'31-Dec-9999') "
                }
                else
                {
                    strCondition = " AND TO_DATE(' " + RFQDate + "','" + dateFormat + "') between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                    //nvl(TO_DATE,'31-Dec-9999') "
                }
                strSQL = " SELECT C.CURRENCY_MST_PK, " + "      ' ' || C.CURRENCY_ID        CURRENCY_ID,    " + "      C.CURRENCY_NAME,                            " + "      1                           EXCHANGE_RATE   " + "  FROM    CURRENCY_TYPE_MST_TBL      C   ,         " + "  COUNTRY_MST_TBL COUNTRY WHERE C.CURRENCY_MST_PK=COUNTRY.CURRENCY_MST_FK   " + "  AND COUNTRY.COUNTRY_MST_PK=(select loc.country_mst_fk from location_mst_tbl loc where loc.location_mst_pk=" + locpk + ")" + "  AND C.ACTIVE_FLAG   =   1                       " + " UNION                                            " + " SELECT                                           " + "      CURR.CURRENCY_MST_PK,                       " + "      CURR.CURRENCY_ID,                           " + "      CURR.CURRENCY_NAME,                         " + "      EXCHANGE_RATE                               " + "  FROM CURRENCY_TYPE_MST_TBL CURR,                " + "      V_EXCHANGE_RATE_TYPE EXC                         " + "  WHERE                                           " + "      EXC.CURRENCY_MST_FK             =   CURR.CURRENCY_MST_PK        " + "      AND CURR.ACTIVE_FLAG            =   1  AND EXC.EXCH_RATE_TYPE_FK = " + ExType + " " + "      AND EXC.CURRENCY_MST_BASE_FK    =                               " + "  (SELECT CONTRY.CURRENCY_MST_FK FROM COUNTRY_MST_TBL CONTRY WHERE CONTRY.COUNTRY_MST_PK=" + "  (select loc.country_mst_fk from location_mst_tbl loc where loc.location_mst_pk=" + locpk + " ))  " + "  AND EXC.CURRENCY_MST_BASE_FK   <> EXC.CURRENCY_MST_FK       " + strCondition + "   ORDER BY        CURRENCY_ID     ";
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                DataSet DS = null;
                DS = (new WorkFlow()).GetDataSet(strSQL);
                Int16 RowCnt = default(Int16);
                string CurrId = Convert.ToString(DS.Tables[0].Rows[0]["CURRENCY_ID"]).Trim();
                DS.Tables[0].Rows[0]["CURRENCY_ID"] = CurrId;
                return DS;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Exchange Rate "

        #region " Fetch Exchange Rate Type "

        /// <summary>
        /// Fetches the type of the ex rate.
        /// </summary>
        /// <param name="business_type">The business_type.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public DataSet FetchExRateType(int business_type = 0, int process = 0)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strBuilder.Append(" SELECT");
                strBuilder.Append(" ERT.EXCH_RATE_TYPE_PK,");
                strBuilder.Append(" ERT.EXCH_RATE_TYPE ");
                strBuilder.Append(" FROM");
                strBuilder.Append(" EXCHANGE_RATE_TYPE ERT");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" ERT.ACTIVE=1");
                if ((business_type != 0))
                {
                    if ((business_type == 1 | business_type == 2))
                    {
                        strBuilder.Append(" AND ERT.BUSINESS_TYPE in (" + business_type + ",3)");
                    }
                }
                if ((process != 0))
                {
                    if ((process == 1 | process == 2))
                    {
                        strBuilder.Append(" AND ERT.PROCESS_TYPE = " + process + ",3)");
                    }
                }
                strBuilder.Append("  ORDER BY ERT.EXCH_RATE_TYPE");
                return objWF.GetDataSet(strBuilder.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion " Fetch Exchange Rate Type "

        #region " Fetch Voyage Wise Exchange Rate"

        //Rijesh April 05-2006
        //For fetching Vessel Voyage Wise Exchange Rate
        /// <summary>
        /// Fetches the ves voy curr and exchange.
        /// </summary>
        /// <param name="VoyageFk">The voyage fk.</param>
        /// <returns></returns>
        public DataSet FetchVesVoyCurrAndExchange(int VoyageFk)
        {
            //modified by thiyagarajan on 29/11/08 for location based currency task
            //HttpContext.Current.Session("CURRENCY_MST_PK") replaced Corporate currency task
            try
            {
                string strSQL = null;
                strSQL = " SELECT C.CURRENCY_MST_PK, " + "      ' ' || C.CURRENCY_ID        CURRENCY_ID,    " + "      C.CURRENCY_NAME,                            " + "      1                           EXCHANGE_RATE   " + "  FROM    CURRENCY_TYPE_MST_TBL      C            " + "  WHERE   C.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"].ToString() + "      AND C.ACTIVE_FLAG   =   1                   " + " UNION                                            " + " SELECT                                           " + "      CURR.CURRENCY_MST_PK,                       " + "      CURR.CURRENCY_ID,                           " + "      CURR.CURRENCY_NAME,                         " + "      EXCHANGE_RATE                               " + "  FROM CURRENCY_TYPE_MST_TBL CURR,                " + "     V_EXCHANGE_RATE_VES_VOY EXC                        " + "  WHERE                                           " + "      EXC.CURRENCY_MST_FK             =   CURR.CURRENCY_MST_PK        " + "      AND CURR.ACTIVE_FLAG            =   1                           " + "      AND EXC.Voyage_Trn_Fk is not null                               " + "      AND exc.voyage_trn_fk =  " + VoyageFk + "                       " + "      AND EXC.CURRENCY_MST_BASE_FK    = " + HttpContext.Current.Session["CURRENCY_MST_PK"].ToString() + "      AND EXC.CURRENCY_MST_BASE_FK   <> EXC.CURRENCY_MST_FK           " + "   ORDER BY        CURRENCY_ID     ";
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                DataSet DS = null;
                DS = (new WorkFlow()).GetDataSet(strSQL);
                Int16 RowCnt = default(Int16);
                string CurrId = Convert.ToString(DS.Tables[0].Rows[0]["CURRENCY_ID"]).Trim();
                DS.Tables[0].Rows[0]["CURRENCY_ID"] = CurrId;
                return DS;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch Voyage Wise Exchange Rate"

        #region "String for Sector and Container"

        //Akhilesh : 12/28/05
        //Used in FCL Transactions selecting multiple containers for mulitple sectors.
        /// <summary>
        /// Makes the condition string.
        /// </summary>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="OperPks">The oper PKS.</param>
        public void MakeConditionString(string strCondition, string OperPks = "")
        {
            string[] arr = null;
            string[] OperPK = null;
            Int32 i = default(Int32);
            Int32 j = default(Int32);
            Int32 k = default(Int32);
            string[] Port = null;
            string[] Container = null;
            if (string.IsNullOrEmpty(OperPks))
            {
                if (!(strCondition == "n") & !string.IsNullOrEmpty(strCondition) & !(strCondition == "undefined"))
                {
                    arr = strCondition.Split('$');
                    strCondition = "n";
                    for (i = 0; i <= arr.Length - 2; i++)
                    {
                        Port = arr[i].Split('^');
                        Container = Port[2].Split(',');
                        for (j = 0; j <= Container.Length - 1; j++)
                        {
                            if ((strCondition == "n"))
                            {
                                strCondition = "(" + Port[0] + "," + Port[1] + "," + Container[j] + ")";
                            }
                            else
                            {
                                strCondition += ", (" + Port[0] + "," + Port[1] + "," + Container[j] + ")";
                            }
                        }
                    }
                }
            }
            else
            {
                if (!(strCondition == "n") & !string.IsNullOrEmpty(strCondition) & !(strCondition == "undefined"))
                {
                    arr = strCondition.Split('$');
                    strCondition = "n";
                    for (i = 0; i <= arr.Length - 2; i++)
                    {
                        Port = arr[i].Split('^');
                        Container = Port[2].Split(',');
                        for (j = 0; j <= Container.Length - 1; j++)
                        {
                            OperPK = OperPks.Split(',');
                            for (k = 0; k <= OperPK.Length - 1; k++)
                            {
                                if ((strCondition == "n"))
                                {
                                    strCondition = "(" + Port[0] + "," + Port[1] + "," + Container[j] + "," + OperPK[k] + ")";
                                }
                                else
                                {
                                    strCondition += ", (" + Port[0] + "," + Port[1] + "," + Container[j] + "," + OperPK[k] + ")";
                                }
                            }
                        }
                    }
                }
            }
        }

        #endregion "String for Sector and Container"

        #region "String for FCL and LCL"

        //created by thiyagarajan on 21/5/08 for combined FCL and LCL in quotation
        /// <summary>
        /// Makes the condition strings.
        /// </summary>
        /// <param name="strCondition">The string condition.</param>
        public void MakeConditionStrings(string strCondition)
        {
            string[] arr = null;
            Int32 i = default(Int32);
            Int32 j = default(Int32);
            string[] Port = null;
            string[] Container = null;
            string result = "n";
            string[] basis = null;
            string[] cargo = null;
            if (!(strCondition == "n") & !string.IsNullOrEmpty(strCondition) & !(strCondition == "undefined"))
            {
                arr = strCondition.Split('$');
                strCondition = "n";
                for (i = 0; i <= arr.Length - 2; i++)
                {
                    Port = arr[i].Split('^');
                    cargo = Port[2].Split('~');
                    Container = cargo[0].Split(',');
                    basis = cargo[1].Split(',');
                    //fcl pks
                    for (j = 0; j <= Container.Length - 1; j++)
                    {
                        if ((strCondition == "n"))
                        {
                            strCondition = "(" + Port[0] + "," + Port[1] + "," + Container[j] + ")";
                        }
                        else
                        {
                            strCondition += ", (" + Port[0] + "," + Port[1] + "," + Container[j] + ")";
                        }
                    }
                    //lcl pks
                    for (j = 0; j <= basis.Length - 1; j++)
                    {
                        if ((result == "n"))
                        {
                            result = "(" + Port[0] + "," + Port[1] + "," + basis[j] + ")";
                        }
                        else
                        {
                            result += ", (" + Port[0] + "," + Port[1] + "," + basis[j] + ")";
                        }
                    }
                }
                strCondition += "~" + result;
            }
        }

        #endregion "String for FCL and LCL"

        #region " Format Members and properties "

        // [ Added on 18-Feb-2006 For Formatting in Class file.  Rajesh ]
        /// <summary>
        ///
        /// </summary>
        public enum formatText
        {
            /// <summary>
            /// The amount
            /// </summary>
            Amount = 1,

            /// <summary>
            /// The weight
            /// </summary>
            Weight = 2,

            /// <summary>
            /// The volume
            /// </summary>
            Volume = 3,

            /// <summary>
            /// The exchange
            /// </summary>
            Exchange = 4
        }

        /// <summary>
        /// The m_ amount format
        /// </summary>
        protected string M_AmountFormat = "###,##0.00";

        /// <summary>
        /// The m_ weight format
        /// </summary>
        protected string M_WeightFormat = "###,##0.000";

        /// <summary>
        /// The m_ volume format
        /// </summary>
        protected string M_VolumeFormat = "###,##0.000";

        //Modified by Mikky
        /// <summary>
        /// The m_ exchange format
        /// </summary>
        protected string M_ExchangeFormat = "#,##0.000000";

        /// <summary>
        /// The m_ date format
        /// </summary>
        protected static string M_DateFormat = ConfigurationManager.AppSettings["dateFormat"];

        /// <summary>
        /// The m_ date time format
        /// </summary>
        protected string M_DateTimeFormat = M_DateFormat + " HH:MM";

        /// <summary>
        /// The m_ time format24
        /// </summary>
        protected string M_TimeFormat24 = "HH24:Mi";

        /// <summary>
        /// The m_ date time format24
        /// </summary>
        protected string M_DateTimeFormat24 = M_DateFormat + " HH24:Mi";

        /// <summary>
        /// Gets or sets the amount format.
        /// </summary>
        /// <value>
        /// The amount format.
        /// </value>
        public string AmountFormat
        {
            get { return M_AmountFormat; }
            set { M_AmountFormat = value; }
        }

        /// <summary>
        /// Gets or sets the weight format.
        /// </summary>
        /// <value>
        /// The weight format.
        /// </value>
        public string WeightFormat
        {
            get { return M_WeightFormat; }
            set { M_WeightFormat = value; }
        }

        /// <summary>
        /// Gets or sets the volume format.
        /// </summary>
        /// <value>
        /// The volume format.
        /// </value>
        public string VolumeFormat
        {
            get { return M_VolumeFormat; }
            set { M_VolumeFormat = value; }
        }

        /// <summary>
        /// Gets or sets the exchange format.
        /// </summary>
        /// <value>
        /// The exchange format.
        /// </value>
        public string ExchangeFormat
        {
            get { return M_ExchangeFormat; }
            set { M_ExchangeFormat = value; }
        }

        /// <summary>
        /// Formats the text box.
        /// </summary>
        /// <param name="strTextBox">The string text box.</param>
        /// <param name="formatStyle">The format style.</param>
        /// <returns></returns>
        public object formatTextBox(Object strTextBox, formatText formatStyle)
        {
            try
            {
                if (formatStyle == formatText.Amount)
                {
                    strTextBox = string.Format(Convert.ToString(strTextBox), AmountFormat);
                }
                else if (formatStyle == formatText.Weight)
                {
                    strTextBox = string.Format(Convert.ToString(strTextBox), WeightFormat);
                }
                else if (formatStyle == formatText.Volume)
                {
                    strTextBox = string.Format(Convert.ToString(strTextBox), VolumeFormat);
                }
                else if (formatStyle == formatText.Exchange)
                {
                    strTextBox = string.Format(Convert.ToString(strTextBox), ExchangeFormat);
                }
            }
            catch
            {
            }
            return strTextBox;
        }

        /// <summary>
        /// Gets or sets the date format.
        /// </summary>
        /// <value>
        /// The date format.
        /// </value>
        public string dateFormat
        {
            get { return M_DateFormat; }
            set { M_DateFormat = value; }
        }

        /// <summary>
        /// Gets or sets the date time format.
        /// </summary>
        /// <value>
        /// The date time format.
        /// </value>
        public string dateTimeFormat
        {
            get { return M_DateTimeFormat; }
            set { M_DateTimeFormat = value; }
        }

        /// <summary>
        /// Gets or sets the date time format24.
        /// </summary>
        /// <value>
        /// The date time format24.
        /// </value>
        public string dateTimeFormat24
        {
            get { return M_DateTimeFormat24; }
            set { M_DateTimeFormat24 = value; }
        }

        /// <summary>
        /// Gets or sets the time format24.
        /// </summary>
        /// <value>
        /// The time format24.
        /// </value>
        public string timeFormat24
        {
            get { return M_TimeFormat24; }
            set { M_TimeFormat24 = value; }
        }

        #endregion " Format Members and properties "

        #region " Shared Methods "

        // This method will return default value if the provided value will be in
        // dbnull.value or nothing or null string
        // [ Added on 08-Feb-2006  By Rajesh ]
        /// <summary>
        /// Gets the default.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <param name="defaultVal">The default value.</param>
        /// <returns></returns>
        public static object getDefault(object col, object defaultVal)
        {
            if (object.ReferenceEquals(col, ""))
            {
                return defaultVal;
            }
            else if (col == null)
            {
                return defaultVal;
            }
            else if (Convert.ToString(col).Trim().Length == 0)
            {
                return defaultVal;
            }
            else if (Convert.ToString(col).Trim().ToUpper() == "NULL")
            {
                return defaultVal;
            }
            else if (Convert.ToString(col).Trim() == "/  /")
            {
                return defaultVal;
            }
            else if (Convert.ToString(col) == "  /  /       :  ")
            {
                return defaultVal;
            }
            else if (Convert.ToString(col) == "  /  /       :  :  ")
            {
                return defaultVal;
            }
            else if (int.TryParse(col.ToString(), out result))
            {
                return col;
            }
            else if (object.ReferenceEquals(col, (object)0))
            {
                return defaultVal;
            }
            else if (Convert.ToDouble(col.ToString()) == 0)
            {
                return defaultVal;
            }
            else
            {
                return col;
            }
        }

        #endregion " Shared Methods "

        #region " Fetch Base Currency Id "

        /// <summary>
        /// Fetches the base curr identifier.
        /// </summary>
        /// <returns></returns>
        public string FetchBaseCurrId()
        {
            System.Text.StringBuilder str = new System.Text.StringBuilder();
            str.Append(" select cm.currency_id from corporate_mst_tbl cr,currency_type_mst_tbl cm where cr.currency_mst_fk = cm.currency_mst_pk");
            return Convert.ToString((new WorkFlow()).ExecuteScaler(str.ToString()));
        }

        #endregion " Fetch Base Currency Id "

        #region " Fetch Exchange Rate -Hidden"

        /// <summary>
        /// Fetches the roe.
        /// </summary>
        /// <param name="baseCurrency">The base currency.</param>
        /// <param name="ConversionDate">The conversion date.</param>
        /// <returns></returns>
        public DataSet FetchROE(Int64 baseCurrency, string ConversionDate = "")
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                ConversionDate = ConversionDate.Trim();
                if (string.IsNullOrEmpty(ConversionDate))
                {
                    ConversionDate = " TO_DATE(SYSDATE,DATEFORMAT) ";
                }
                else
                {
                    ConversionDate = " TO_DATE('" + ConversionDate + "',DATEFORMAT) ";
                }
                strBuilder.Append(" SELECT");
                strBuilder.Append(" CMT.CURRENCY_MST_PK,");
                strBuilder.Append(" CMT.CURRENCY_ID,");
                strBuilder.Append(" ROUND(GET_EX_RATE(CMT.CURRENCY_MST_PK, ");
                strBuilder.Append(" " + baseCurrency + ",ROUND(" + ConversionDate + " - .5)),6) AS ROE");
                strBuilder.Append(" FROM");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CMT");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" CMT.ACTIVE_FLAG = 1");
                return objWF.GetDataSet(strBuilder.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        /// <summary>
        /// Fetches the ro e_ buy.
        /// </summary>
        /// <param name="baseCurrency">The base currency.</param>
        /// <param name="ConversionDate">The conversion date.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="VslVoyFk">The VSL voy fk.</param>
        /// <returns></returns>
        public DataSet FetchROE_BUY(Int64 baseCurrency, string ConversionDate = "", int ExType = 1, int VslVoyFk = 0)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                ConversionDate = ConversionDate.Trim();
                if (string.IsNullOrEmpty(ConversionDate))
                {
                    ConversionDate = " TO_DATE(SYSDATE,DATEFORMAT) ";
                }
                else
                {
                    ConversionDate = " TO_DATE('" + ConversionDate + "',DATEFORMAT) ";
                }

                strBuilder.Append(" SELECT");
                strBuilder.Append(" CMT.CURRENCY_MST_PK,");
                strBuilder.Append(" CMT.CURRENCY_ID,");
                if (ExType == 2)
                {
                    strBuilder.Append(" ROUND(GET_EX_RATE_BUY(CMT.CURRENCY_MST_PK, ");
                    strBuilder.Append(" " + baseCurrency + ",ROUND(" + ConversionDate + " - .5)," + ExType + "," + VslVoyFk + "),6) AS ROE");
                }
                else
                {
                    strBuilder.Append(" ROUND(GET_EX_RATE_BUY(CMT.CURRENCY_MST_PK, ");
                    strBuilder.Append(" " + baseCurrency + ",ROUND(" + ConversionDate + " - .5)),6) AS ROE");
                }
                strBuilder.Append(" FROM");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CMT");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" CMT.ACTIVE_FLAG = 1");
                return objWF.GetDataSet(strBuilder.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion " Fetch Exchange Rate -Hidden"

        #region " Fetch Exchange Rate Bassed On Exchange Rate Type -Hidden"

        /// <summary>
        /// Fetches the ex type roe.
        /// </summary>
        /// <param name="baseCurrency">The base currency.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="Coldate">The coldate.</param>
        /// <returns></returns>
        public DataSet FetchExTypeROE(Int64 baseCurrency, Int64 ExType = 1, string Coldate = "")
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strBuilder.Append(" SELECT DISTINCT");
                strBuilder.Append(" CMT.CURRENCY_MST_PK,");
                strBuilder.Append(" CMT.CURRENCY_ID,");
                if (ExType == 3)
                {
                    strBuilder.Append(" ROUND(GET_EX_RATE1( ");
                    strBuilder.Append("CMT.CURRENCY_MST_PK, " + baseCurrency + ",ROUND(TO_DATE(' " + Coldate + "',dateFormat))," + ExType + "),6) AS ROE");
                }
                else
                {
                    strBuilder.Append(" ROUND(GET_EX_RATE( ");
                    strBuilder.Append("CMT.CURRENCY_MST_PK, " + baseCurrency + ",ROUND(TO_DATE(' " + Coldate + "',dateFormat))),6) AS ROE");
                }
                //AND TO_DATE(' " & RFQDate & "',dateFormat) between EXC.FROM_DATE and nvl(TO_DATE,NULL_DATE_FORMAT)
                strBuilder.Append(" FROM");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CMT , EXCHANGE_RATE_TRN EXC");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" CMT.ACTIVE_FLAG = 1");
                strBuilder.Append(" AND EXC.CURRENCY_MST_FK=CMT.CURRENCY_MST_PK ");
                strBuilder.Append(" AND EXC.CURRENCY_MST_BASE_FK =(SELECT C0MT.CURRENCY_MST_FK FROM CORPORATE_MST_TBL C0MT)");
                strBuilder.Append(" AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK");
                strBuilder.Append(" AND EXC.VOYAGE_TRN_FK IS NULL");
                strBuilder.Append(" AND EXC.EXCH_RATE_TYPE_FK = " + ExType + "");
                return objWF.GetDataSet(strBuilder.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion " Fetch Exchange Rate Bassed On Exchange Rate Type -Hidden"

        #region "Commodity Group"

        /// <summary>
        /// The commodity GRP
        /// </summary>
        private int CommodityGrp = 0;

        /// <summary>
        /// Gets the general.
        /// </summary>
        /// <value>
        /// The general.
        /// </value>
        public int GENERAL
        {
            get
            {
                CommodityGrp = getCommodityGrp(1);
                return CommodityGrp;
            }
        }

        /// <summary>
        /// Gets the hazardous.
        /// </summary>
        /// <value>
        /// The hazardous.
        /// </value>
        public int HAZARDOUS
        {
            get
            {
                CommodityGrp = getCommodityGrp(2);
                return CommodityGrp;
            }
        }

        /// <summary>
        /// Gets the reefer.
        /// </summary>
        /// <value>
        /// The reefer.
        /// </value>
        public int REEFER
        {
            get
            {
                CommodityGrp = getCommodityGrp(3);
                return CommodityGrp;
            }
        }

        /// <summary>
        /// Gets the odc.
        /// </summary>
        /// <value>
        /// The odc.
        /// </value>
        public int ODC
        {
            get
            {
                CommodityGrp = getCommodityGrp(4);
                return CommodityGrp;
            }
        }

        //MODIFIED BY LATHA FOR PARAMETERS
        /// <summary>
        /// Gets the precarriage.
        /// </summary>
        /// <value>
        /// The precarriage.
        /// </value>
        public int PRECARRIAGE
        {
            get
            {
                CommodityGrp = getCommodityGrp(5);
                return CommodityGrp;
            }
        }

        /// <summary>
        /// Gets the oncarriage.
        /// </summary>
        /// <value>
        /// The oncarriage.
        /// </value>
        public int ONCARRIAGE
        {
            get
            {
                CommodityGrp = getCommodityGrp(6);
                return CommodityGrp;
            }
        }

        /// <summary>
        /// Gets the bof.
        /// </summary>
        /// <value>
        /// The bof.
        /// </value>
        public int BOF
        {
            get
            {
                CommodityGrp = getCommodityGrp(7);
                return CommodityGrp;
            }
        }

        /// <summary>
        /// Gets the afc.
        /// </summary>
        /// <value>
        /// The afc.
        /// </value>
        public int AFC
        {
            get
            {
                CommodityGrp = getCommodityGrp(8);
                return CommodityGrp;
            }
        }

        /// <summary>
        /// Gets the commodity GRP.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public int getCommodityGrp(int type)
        {
            string strQuery = null;
            OracleDataReader dr = default(OracleDataReader);
            strQuery = "SELECT P.GENERAL_CARGO_FK,P.HAZ_CARGO_FK,P.REEFER_CARGO_FK,P.ODC_CARGO_FK ,P.COST_PRECARRIAGE_FK," + "P.COST_ONCARRIAGE_FK, P.COST_BOF_FK,P.COST_AFC_FK, P.UOM_KG, P.UOM_TON, P.UOM_LBS, P.COST_FRT_FK, P.COST_TPC_FK, P.SALES_MANAGER, P.SALES_EXECUTIVE, P.FRT_BOF_FK, P.FRT_AFC_FK, P.FRT_DET_CHARGE_FK, P.FRT_DEM_CHARGE_FK,P.FRT_AIF_FK,P.FRT_FAC_FK,P.FRT_MIS_FK  FROM PARAMETERS_TBL P";
            dr = (new WorkFlow()).GetDataReader(strQuery);
            try
            {
                while (dr.Read())
                {
                    switch (type)
                    {
                        case 1:
                            //general
                            return Convert.ToInt32(dr[0]);

                        case 2:
                            //Haz
                            return Convert.ToInt32(dr[1]);

                        case 3:
                            //reefer
                            return Convert.ToInt32(dr[2]);

                        case 4:
                            //ODC
                            return Convert.ToInt32(dr[3]);

                        case 5:
                            //precarriage
                            return Convert.ToInt32(dr[4]);

                        case 6:
                            //oncarriage
                            return Convert.ToInt32(dr[5]);

                        case 7:
                            //bof
                            return Convert.ToInt32(dr[6]);

                        case 8:
                            //afc
                            return Convert.ToInt32(dr[7]);
                        //Manoharan 08Feb07: to add UOM values
                        case 9:
                            //kg
                            return Convert.ToInt32(dr[8]);

                        case 10:
                            //ton
                            return Convert.ToInt32(dr[19]);

                        case 11:
                            //lbs
                            return Convert.ToInt32(dr[10]);

                        case 12:
                            // operator cost
                            return Convert.ToInt32(dr[11]);

                        case 13:
                            // transporter cost
                            return Convert.ToInt32(dr[12]);

                        case 14:
                            //sales Manager
                            return Convert.ToInt32(dr[13]);

                        case 15:
                            //sales Executive
                            return Convert.ToInt32(dr[14]);

                        case 16:
                            //Freight Elements BOF
                            return Convert.ToInt32(dr[15]);

                        case 17:
                            //Freight Elements AFC
                            return Convert.ToInt32(dr[16]);

                        case 18:
                            //Freight Elements Detention Charges
                            return Convert.ToInt32(dr[17]);

                        case 19:
                            //Freight Elements Demurage Charges
                            return Convert.ToInt32(dr[18]);

                        case 20:
                            //All in Freight  (AIF)
                            return Convert.ToInt32(dr[19]);

                        case 21:
                            //Forwarder Agency Commission (FAC)
                            return Convert.ToInt32(dr[20]);

                        case 22:
                            //Miscellaneous Charges
                            return Convert.ToInt32(dr[21]);
                    }
                }
                return 0;
            }
            catch (Exception ex)
            {
                return 0;
            }
            finally
            {
                dr.Close();
            }
        }

        #endregion "Commodity Group"

        #region "To fetch captions for multilingual"

        /// <summary>
        /// Fetches the caption.
        /// </summary>
        /// <param name="Param">The parameter.</param>
        /// <param name="LocPK">The loc pk.</param>
        /// <returns></returns>
        public DataSet FetchCaption(string Param, int LocPK)
        {
            try
            {
                string strSQL = null;
                strSQL += " select hdr.report_default_caption mainLang , trn.report_country_caption subLang ";
                strSQL += " from report_caption_cntry_trn trn, report_caption_mapper_hdr hdr,";
                strSQL += " country_mst_tbl cmt,location_mst_tbl loc";
                strSQL += " where trn.report_capton_mapper_hdr_fk = hdr.report_caption_mapper_pk";
                strSQL += " and cmt.country_mst_pk=trn.country_mst_fk";
                strSQL += " and loc.country_mst_fk =cmt.country_mst_pk";
                strSQL += " and loc.location_mst_pk = " + LocPK;
                strSQL += " and hdr.report_default_caption in (" + Param + ")";
                return ((new WorkFlow()).GetDataSet(strSQL));
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region "RollBackProtocolID"

        /// <summary>
        /// Rollbacks the protocol key.
        /// </summary>
        /// <param name="sProtocolName">Name of the s protocol.</param>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <param name="ProtocolId">The protocol identifier.</param>
        /// <param name="ProtocolDate">The protocol date.</param>
        /// <returns></returns>
        public string RollbackProtocolKey(string sProtocolName, Int64 ILocationId, Int64 IEmployeeId, string ProtocolId, System.DateTime ProtocolDate)
        {
            WorkFlow objWS = new WorkFlow();
            string sVSL = "";
            string sVOY = "";
            string sPOL = "";
            CREATED_BY = Convert.ToInt64(HttpContext.Current.Session["USER_PK"]);
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                objWS.MyCommand.CommandText = objWS.MyUserName + ".ROLLBACK_PROTOCOL_SERIAL_NR";

                var _with2 = objWS.MyCommand.Parameters;
                _with2.Add("PROTOCOL_NAME_IN", sProtocolName).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", ILocationId).Direction = ParameterDirection.Input;
                _with2.Add("EMPLOYEE_MST_FK_IN", IEmployeeId).Direction = ParameterDirection.Input;
                //.Add("USER_MST_FK_IN", CREATED_BY).Direction = ParameterDirection.Input
                _with2.Add("DATE_IN", ProtocolDate).Direction = ParameterDirection.Input;
                _with2.Add("VSL_IN", getDefault(sVSL, "")).Direction = ParameterDirection.Input;
                _with2.Add("VOY_IN", getDefault(sVOY, "")).Direction = ParameterDirection.Input;
                _with2.Add("POL_IN", getDefault(sPOL, "")).Direction = ParameterDirection.Input;
                _with2.Add("PROTOCOL_ID_IN", ProtocolId).Direction = ParameterDirection.Input;
                objWS.MyCommand.Parameters.Add("VSL_IN", OracleDbType.Int32, 20);
                objWS.MyCommand.Parameters.Add("VOY_IN", OracleDbType.Int32, 20);
                objWS.MyCommand.Parameters.Add("POL_IN", OracleDbType.Int32, 20);
                _with2.Add("RETURN_VALUE", OracleDbType.Int32, 20).Direction = ParameterDirection.Output;
                objWS.ExecuteCommands();
                return Convert.ToString(objWS.MyCommand.Parameters["RETURN_VALUE"].Value);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "RollBackProtocolID"

        #region "Generate Renadom Code"

        /// <summary>
        /// Generates the code.
        /// </summary>
        /// <param name="codeType">Type of the code.</param>
        /// <returns></returns>
        public string GenerateCode(int codeType)
        {
            WorkFlow objwf = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            try
            {
                objwf.OpenConnection();
                selectCommand.Connection = objwf.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objwf.MyUserName + ".PKG_EBK_CUST_REG.EBK_M_GENERATE_RANDOM_CODE";
                var _with3 = selectCommand.Parameters;
                _with3.Add("CODE_TYPE", codeType).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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

        #endregion "Generate Renadom Code"

        #endregion "To fetch captions for multilingual"

        public void getReportControls(ReportDocument repDoc, string str_controlId, Int32 num_sub_report_id = 0)
        {
            OracleDataReader dr = default(OracleDataReader);
            try
            {
                WorkFlow objWF = new WorkFlow();
                string strSQL = null;
                strSQL = strSQL +  "  SELECT CCT.CONFIG_CTRL_ID,nvl(ct.config_control_text,CCT.CONFIG_CTRL_DEF_TEXT)as CONFIG_CTRL_DEF_TEXT ,CCT.CONFIG_CONTROL_TOOLTIP";
                strSQL = strSQL +  "  FROM CONFIG_CONTROLS_TBL CCT,CONFIG_MST_TBL CMT,config_controls_text_trn ct";
                strSQL = strSQL +  "  WHERE CMT.CONFIG_MST_PK=CCT.CONFIG_MST_FK";
                strSQL = strSQL +  "  AND CMT.CONFIG_ID='" + str_controlId + "' ";
                strSQL = strSQL +  "  AND CCT.CONFIG_CTRL_FIELD_TYPE='RH' ";
                strSQL = strSQL +  "  AND CCT.CONFIG_CTRL_MAX_LENGTH=" + num_sub_report_id + " ";
                if ((HttpContext.Current.Session["ENVIRONMENT_PK"] == null) | Convert.ToInt32(HttpContext.Current.Session["ENVIRONMENT_PK"]) == 0)
                {
                    strSQL = strSQL +  " and (ct.config_controls_fk(+) = cct.config_controls_pk";
                    strSQL = strSQL +  " and ct.environment_tbl_fk(+) = 1)";
                }
                else
                {
                    strSQL = strSQL +  " and (ct.config_controls_fk(+) = cct.config_controls_pk";
                    strSQL = strSQL +  " and ct.environment_tbl_fk(+) = " + HttpContext.Current.Session["ENVIRONMENT_PK"] + ") ";
                }

                dr = objWF.GetDataReader(strSQL);
                while (dr.Read())
                {
                    try
                    {
                        repDoc.SetParameterValue(Convert.ToInt32(dr["CONFIG_CTRL_ID"]), dr["CONFIG_CTRL_DEF_TEXT"]);
                    }
                    catch (Exception ex)
                    {
                    }
                }
                string sb = null;
                //sb = sb & vbCrLf & "  SELECT CCT.CONFIG_CTRL_ID,CCT.CONFIG_CTRL_DEF_TEXT,CCT.CONFIG_CONTROL_TOOLTIP"
                //sb = sb & vbCrLf & "  FROM CONFIG_CONTROLS_TBL CCT,CONFIG_MST_TBL CMT"
                //sb = sb & vbCrLf & "  WHERE CMT.CONFIG_MST_PK=CCT.CONFIG_MST_FK"
                sb = sb +  "  SELECT CCT.CONFIG_CTRL_ID,nvl(ct.config_control_text,CCT.CONFIG_CTRL_DEF_TEXT)as CONFIG_CTRL_DEF_TEXT ,CCT.CONFIG_CONTROL_TOOLTIP";
                sb = sb +  "  FROM CONFIG_CONTROLS_TBL CCT,CONFIG_MST_TBL CMT,config_controls_text_trn ct";
                sb = sb +  "  WHERE CMT.CONFIG_MST_PK=CCT.CONFIG_MST_FK";
                sb = sb +  "  AND CMT.CONFIG_ID='" + str_controlId + "' ";
                sb = sb +  "  AND CCT.CONFIG_CTRL_FIELD_TYPE='RL' ";
                sb = sb +  "  AND CCT.CONFIG_CTRL_MAX_LENGTH=" + num_sub_report_id + " ";
                if ((HttpContext.Current.Session["ENVIRONMENT_PK"] == null) | Convert.ToInt32(HttpContext.Current.Session["ENVIRONMENT_PK"]) == 0)
                {
                    sb = sb +  " and (ct.config_controls_fk(+) = cct.config_controls_pk";
                    sb = sb +  " and ct.environment_tbl_fk(+) = 1)";
                }
                else
                {
                    sb = sb +  " and (ct.config_controls_fk(+) = cct.config_controls_pk";
                    sb = sb +  " and ct.environment_tbl_fk(+) = " + HttpContext.Current.Session["ENVIRONMENT_PK"] + ") ";
                }
                dr = objWF.GetDataReader(sb);
                while (dr.Read())
                {
                    try
                    {
                        repDoc.SetParameterValue(Convert.ToInt32(dr["CONFIG_CTRL_ID"]), (object.ReferenceEquals(dr["CONFIG_CTRL_DEF_TEXT"], DBNull.Value) ? "" : dr["CONFIG_CTRL_DEF_TEXT"]));
                        //Modified - Sivachandran to accept Null value
                    }
                    catch (Exception ex)
                    {
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                dr.Close();
            }
        }

        /// <summary>
        /// Gets the vek invoice reference.
        /// </summary>
        /// <param name="minRange">The minimum range.</param>
        /// <param name="maxRange">The maximum range.</param>
        /// <param name="baseNumber">The base number.</param>
        /// <returns></returns>
        public string GetVEKInvoiceRef(long minRange = 0, long maxRange = 0, string baseNumber = "")
        {
            string strRefNr = null;
            string strUnique = null;
            string strInvNr = null;
            Array arrRefNr = null;
            Array arrUnique = null;
            Array arrValue = null;
            int i = 0;
            int j = 0;
            int value = 0;
            int uniqueDigit = 0;
            Random r = new Random(DateTime.Now.Millisecond);
            strUnique = "7,3,1";
            //This string is suggested by VEK
            try
            {
                if (minRange > 0 & maxRange > 0)
                {
                    strRefNr = Convert.ToString(r.Next(Convert.ToInt32(minRange), Convert.ToInt32(maxRange)));
                }
                else if (baseNumber.Length > 0)
                {
                    strRefNr = baseNumber;
                }
                value = 0;
                j = 0;
                arrRefNr = strRefNr.ToCharArray();
                Array.Reverse(arrRefNr);
                arrUnique = strUnique.Split(',');
                for (i = 0; i <= arrRefNr.Length - 1; i++)
                {
                    value = value + (Convert.ToInt32(Convert.ToString(arrRefNr.GetValue(i))) * Convert.ToInt32(Convert.ToString(arrUnique.GetValue(j))));
                    j = j + 1;
                    //After multiplying with 7,3,1 it should again start from 7...
                    if (j == 3)
                    {
                        j = 0;
                    }
                }
                arrValue = value.ToString().ToCharArray();
                Array.Reverse(arrValue);
                if (arrValue.GetValue(0) == "0")
                {
                    uniqueDigit = 0;
                }
                else
                {
                    uniqueDigit = 10 - Convert.ToInt32(Convert.ToString(arrValue.GetValue(0)));
                    //Getting the unique digit
                }
                strInvNr = strRefNr + uniqueDigit;
                //Concatinating the unique digit to the base number
                return strInvNr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}