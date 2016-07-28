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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Agent_Definition : CommonFeatures
    {
        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="AGENTID">The agentid.</param>
        /// <param name="AGENT_Name">Name of the agen t_.</param>
        /// <param name="businessType">Type of the business.</param>
        /// <param name="Location">The location.</param>
        /// <param name="currentBusinessType">Type of the current business.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="channelPartner">if set to <c>true</c> [channel partner].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string AGENTID = "", string AGENT_Name = "", int businessType = 3, string Location = "", int currentBusinessType = 3, string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true,
        string SortType = " ASC ", bool channelPartner = true, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (AGENTID.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(ALM.AGENT_ID) like '%" + AGENTID.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(ALM.AGENT_ID) like '" + AGENTID.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (AGENT_Name.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(ALM.AGENT_NAME) like '%" + AGENT_Name.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(ALM.AGENT_NAME) like '" + AGENT_Name.ToUpper().Replace("'", "''") + "%'";
                }
            }

            // Business rule
            if (businessType == 3 & currentBusinessType == 3)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,2,3) ";
            }
            else if (businessType == 3 & currentBusinessType == 2)
            {
                strCondition += " AND BUSINESS_TYPE IN (2,3) ";
            }
            else if (businessType == 3 & currentBusinessType == 1)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,3) ";
            }
            else
            {
                strCondition += " AND BUSINESS_TYPE IN ( " + businessType + ",3) ";
            }

            if (ActiveFlag == true)
            {
                strCondition = strCondition + " and ALM.ACTIVE_FLAG = 1 ";
            }
            if (channelPartner == true)
            {
                strCondition = strCondition + " and ALM.CHANNEL_PARTNER = 1 ";
            }
            if (flag == 0)
            {
                strCondition = strCondition + " AND 1=2 ";
            }
            strSQL = "SELECT Count(*) from AGENT_MST_TBL ALM";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
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
            if (Location.Trim().Length > 0)
            {
                strCondition += " AND UPPER(LO.LOCATION_ID) = '" + Location.ToUpper().Replace("'", "''").Trim() + "'";
            }
            strSQL = " SELECT * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += " (SELECT ALM.AGENT_MST_PK,";
            strSQL += "ALM.ACTIVE_FLAG,";
            strSQL += "ALM.AGENT_ID, ";
            strSQL += "ALM.AGENT_NAME,";
            strSQL += "decode(ALM.BUSINESS_TYPE,1 ,'Air', 2, 'Sea',3, 'Both') BUSINESS_TYPE ,";
            strSQL += " LO.LOCATION_NAME,";
            strSQL += "ALM.VERSION_NO";
            strSQL += " FROM AGENT_MST_TBL ALM,LOCATION_MST_TBL LO";
            strSQL += "WHERE ( 1 = 1) AND LO.LOCATION_MST_PK(+) = ALM.Location_Mst_Fk ";
            strSQL += strCondition;
            strSQL += " order by " + SortColumn + SortType + " ) q  ) ";
            strSQL += " WHERE SR_NO  Between " + start + " and " + last;

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

        #endregion "Fetch Function"

        #region "Define ENUM"

        /// <summary>
        ///
        /// </summary>
        private enum Header
        {
            /// <summary>
            /// The slno
            /// </summary>
            SLNO = 0,

            /// <summary>
            /// The agentid
            /// </summary>
            AGENTID = 1,

            /// <summary>
            /// The agentname
            /// </summary>
            AGENTNAME = 2,

            /// <summary>
            /// The agentfk
            /// </summary>
            AGENTFK = 3,

            /// <summary>
            /// The delete
            /// </summary>
            DELETE = 4
        }

        #endregion "Define ENUM"

        #region "Fetch AIRLINEName"

        /// <summary>
        /// Fetches the name of the airline.
        /// </summary>
        /// <param name="AIRLINEPK">The airlinepk.</param>
        /// <returns></returns>
        public object FetchAIRLINEName(long AIRLINEPK = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT";
            strSQL += " ALM.AIRLINE_ID, ";
            strSQL += " ALM.AIRLINE_NAME,";
            strSQL += " ALM.AIRLINE_MST_PK ";
            strSQL += " FROM AIRLINE_MST_TBL ALM ";

            if (AIRLINEPK > 0)
            {
                strSQL += "WHERE ALM.AIRLINE_MST_PK=" + AIRLINEPK;
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

        #endregion "Fetch AIRLINEName"

        #region "FetchAgentName"

        /// <summary>
        /// Fetches the name of the agent.
        /// </summary>
        /// <param name="selectPK">The select pk.</param>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchAgentName(string selectPK = "0", long Biztype = 3, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();

            selectPK = selectPK.TrimEnd(',');
            //Grid Selection

            strSQL.Append(" SELECT * FROM ");
            strSQL.Append("( SELECT ");
            strSQL.Append(" AGNT.AGENT_MST_PK,");
            strSQL.Append(" AGNT.AGENT_ID,");
            strSQL.Append(" AGNT.AGENT_NAME ,");
            strSQL.Append(" '0' CHK");
            strSQL.Append(" FROM ");
            strSQL.Append(" AGENT_MST_TBL AGNT ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" AGNT.BUSINESS_TYPE IN (3," + Biztype + ")");

            if (selectPK == null | string.IsNullOrEmpty(selectPK))
            {
                strSQL.Append(" AND AGNT.AGENT_MST_PK NOT IN (0) ");
            }
            else
            {
                strSQL.Append(" AND AGNT.AGENT_MST_PK NOT IN (" + selectPK + " ) ");
            }

            strSQL.Append(" AND  AGNT.ACTIVE_FLAG =1 ");
            strSQL.Append(" UNION ");
            strSQL.Append(" SELECT");
            strSQL.Append(" AGNT.AGENT_MST_PK,");
            strSQL.Append(" AGNT.AGENT_ID,");
            strSQL.Append(" AGNT.AGENT_NAME,");
            strSQL.Append(" '1' CHK");
            strSQL.Append(" FROM ");
            strSQL.Append("  AGENT_MST_TBL AGNT ");
            strSQL.Append(" WHERE AGNT.BUSINESS_TYPE IN (3," + Biztype + ")");
            if (selectPK == null | string.IsNullOrEmpty(selectPK))
            {
                strSQL.Append("AND AGNT.AGENT_MST_PK IN (0) ");
                strSQL.Append(" AND AGNT.ACTIVE_FLAG =1 ) T ");
            }
            else
            {
                strSQL.Append(" AND AGNT.AGENT_MST_PK  IN (" + selectPK + " ) ");
                strSQL.Append(" AND AGNT.ACTIVE_FLAG =1 ) T  ");
            }

            strSQL.Append(" ORDER BY CHK DESC,AGENT_ID ");

            //Count the  Records

            strCount.Append("SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + strSQL.ToString() + ")");

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

            strSQLBuilder.Append(" SELECT  qry.* FROM ");
            strSQLBuilder.Append(" (SELECT ROWNUM SR_NO,T.* FROM ");
            strSQLBuilder.Append((" (" + strSQL.ToString() + ")"));
            strSQLBuilder.Append(" T) qry  WHERE SR_NO  Between " + start + " and " + last);
            strSQLBuilder.Append(" ORDER BY CHK DESC, AGENT_ID ");

            try
            {
                return (new WorkFlow()).GetDataSet(strSQLBuilder.ToString());
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

        #endregion "FetchAgentName"

        #region "FetchVendorName"

        /// <summary>
        /// Fetches the name of the vendor.
        /// </summary>
        /// <param name="selectPK">The select pk.</param>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchVendorName(string selectPK = "0", long Biztype = 3, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();

            selectPK = selectPK.TrimEnd(',');
            strSQL.Append(" SELECT * FROM ");
            strSQL.Append("( SELECT ");
            strSQL.Append(" VMT.VENDOR_MST_PK,");
            strSQL.Append(" VMT.VENDOR_ID,");
            strSQL.Append(" VMT.VENDOR_NAME ,");
            strSQL.Append(" '0' CHK");
            strSQL.Append(" FROM ");
            strSQL.Append(" VENDOR_MST_TBL VMT, ");
            strSQL.Append(" VENDOR_SERVICES_TRN VST, ");
            strSQL.Append(" VENDOR_TYPE_MST_TBL VTMT ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" VST.VENDOR_MST_FK = VMT.VENDOR_MST_PK ");
            strSQL.Append(" AND VST.VENDOR_TYPE_FK = VTMT.VENDOR_TYPE_PK ");
            strSQL.Append(" AND VMT.ACTIVE=1 ");
            strSQL.Append(" AND UPPER(VTMT.VENDOR_TYPE_ID) IN('AIRLINE','SHIPPINGLINE') ");
            if (selectPK == null | string.IsNullOrEmpty(selectPK))
            {
                strSQL.Append(" AND VMT.VENDOR_MST_PK NOT IN (0) ");
            }
            else
            {
                strSQL.Append(" AND VMT.VENDOR_MST_PK NOT IN (" + selectPK + " ) ");
            }
            strSQL.Append(" UNION ");
            strSQL.Append(" SELECT");
            strSQL.Append(" VMT.VENDOR_MST_PK,");
            strSQL.Append(" VMT.VENDOR_ID,");
            strSQL.Append(" VMT.VENDOR_NAME,");
            strSQL.Append(" '1' CHK");
            strSQL.Append(" FROM ");
            strSQL.Append("  VENDOR_MST_TBL VMT, ");
            strSQL.Append(" VENDOR_SERVICES_TRN VST, ");
            strSQL.Append(" VENDOR_TYPE_MST_TBL VTMT ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" VST.VENDOR_MST_FK = VMT.VENDOR_MST_PK ");
            strSQL.Append(" AND VST.VENDOR_TYPE_FK = VTMT.VENDOR_TYPE_PK ");
            strSQL.Append(" AND VMT.ACTIVE=1 ");
            strSQL.Append(" AND UPPER(VTMT.VENDOR_TYPE_ID) IN('AIRLINE','SHIPPINGLINE') ");
            if (selectPK == null | string.IsNullOrEmpty(selectPK))
            {
                strSQL.Append("AND VMT.VENDOR_MST_PK IN (0) ");
                strSQL.Append(" AND VMT.ACTIVE =1 ) T ");
            }
            else
            {
                strSQL.Append(" AND VMT.VENDOR_MST_PK  IN (" + selectPK + " ) ");
                strSQL.Append(" AND VMT.ACTIVE =1 ) T  ");
            }

            strSQL.Append(" ORDER BY CHK DESC,VENDOR_ID ");
            strCount.Append("SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + strSQL.ToString() + ")");

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

            strSQLBuilder.Append(" SELECT  qry.* FROM ");
            strSQLBuilder.Append(" (SELECT ROWNUM SR_NO,T.* FROM ");
            strSQLBuilder.Append((" (" + strSQL.ToString() + ")"));
            strSQLBuilder.Append(" T) qry  WHERE SR_NO  Between " + start + " and " + last);
            strSQLBuilder.Append(" ORDER BY CHK DESC, VENDOR_ID ");
            try
            {
                return (new WorkFlow()).GetDataSet(strSQLBuilder.ToString());
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

        #endregion "FetchVendorName"
    }
}