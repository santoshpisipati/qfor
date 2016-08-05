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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_FinMasters : CommonFeatures
    {
        #region "GET DATA"

        /// <summary>
        /// Gets the data.
        /// </summary>
        /// <param name="FromFlag">From flag.</param>
        /// <param name="DBName">Name of the database.</param>
        /// <param name="ProductName">Name of the product.</param>
        /// <param name="LocationName">Name of the location.</param>
        /// <param name="Description">The description.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="Todate">The todate.</param>
        /// <param name="Status">The status.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <returns></returns>
        public string GetData(int FromFlag, string DBName, string ProductName, string LocationName = "", string Description = "", string fromDate = "", string Todate = "", int Status = 0, string SearchType = "", Int32 TotalPage = 0,
        Int32 CurrentPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = new DataSet();
            var _with1 = objWF.MyCommand.Parameters;
            _with1.Add("FLAG_IN", FromFlag).Direction = ParameterDirection.Input;
            _with1.Add("DATABASE_NAME_IN", DBName).Direction = ParameterDirection.Input;
            _with1.Add("PRODUCT_NAME_IN", ProductName).Direction = ParameterDirection.Input;
            _with1.Add("LOCATION_IN", (string.IsNullOrEmpty(LocationName) ? "" : LocationName.ToUpper())).Direction = ParameterDirection.Input;
            _with1.Add("DESCRIPTION_IN", (string.IsNullOrEmpty(Description) ? "" : Description.ToUpper())).Direction = ParameterDirection.Input;
            _with1.Add("FROMDATE_IN", (string.IsNullOrEmpty(fromDate) ? "" : fromDate)).Direction = ParameterDirection.Input;
            _with1.Add("TODATE_IN", (string.IsNullOrEmpty(Todate) ? "" : Todate)).Direction = ParameterDirection.Input;
            _with1.Add("SEARCH_TYPE_IN", SearchType).Direction = ParameterDirection.Input;
            _with1.Add("STSTUS_IN", (Status == 0 ? 0 : Status)).Direction = ParameterDirection.Input;
            _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
            _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
            _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            dsAll = objWF.GetDataSet("FETCH_MASTER_DATA", "FETCH_GRIDDETAILS");
            TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
            CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
            return JsonConvert.SerializeObject(dsAll, Formatting.Indented);
        }

        #endregion "GET DATA"

        #region "Get Drop Down"

        /// <summary>
        /// Gets the drop down details.
        /// </summary>
        /// <returns></returns>
        public DataSet getDropDownDetails()
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append(" SELECT FINAPP.APPLICATION_NAME, FINAPP.DB_NAME");
                sb.Append("  FROM APPLICATION_MST_TBL FINAPP");
                sb.Append("   WHERE FINAPP.ACTIVE = 1");
                sb.Append("   ORDER BY APPLICATION_NAME");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Gets the name of the finance application.
        /// </summary>
        /// <returns></returns>
        public DataSet GetFinanceAppName()
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT APP.APPLICATION_NAME, FINAPP.FINANCE_APP_NAME");
                sb.Append("  FROM APPLICATION_MST_TBL APP, FINANCE_MST_TBL FINAPP");
                sb.Append("  WHERE APP.FINANCE_MST_FK = FINAPP.FINANCE_MST_PK");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Get Drop Down"

        #region "Get Activity Log Details"

        /// <summary>
        /// Gets the act log details.
        /// </summary>
        /// <param name="ProductName">Name of the product.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <returns></returns>
        public DataSet getActLogDetails(string ProductName, Int32 TotalPage = 0, Int32 CurrentPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = new DataSet();
            var _with2 = objWF.MyCommand.Parameters;
            _with2.Add("PRODUCT_NAME_IN", ProductName).Direction = ParameterDirection.Input;
            _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
            _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
            _with2.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            dsAll = objWF.GetDataSet("ACTIVITY_LOG_PKG", "FETCH_GRIDDETAILS");
            TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
            CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
            return dsAll;
        }

        #endregion "Get Activity Log Details"
    }
}