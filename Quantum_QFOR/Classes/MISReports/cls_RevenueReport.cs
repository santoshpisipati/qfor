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
using System.Web;

namespace Quantum_QFOR
{
    public class clsRevenueReport : CommonFeatures
    {
        #region "Property"

        private string type;

        public string GetProcedure
        {
            get
            {
                if (type == "ALL")
                {
                    return "FETCH_REVENUE_REPORT,FETCH_DATA_ALL";
                }
                return "";
            }
            set { type = value; }
        }

        #endregion "Property"

        #region "Fetch Data"

        public DataSet Fetch_Data(string BizType = "", string ProcessType = "", string FROM_DATE = "", string TO_DATE = "", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string VslVoy = "", string SortColumn = "LOCATION", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, string SortType = "DESC", int JobType = 0, string RefNo = "")
        {
            WorkFlow objWF = new WorkFlow();

            DataSet dsAll = null;
            string[] strPKGProc = null;

            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;

                _with1.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;

                _with1.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;

                _with1.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;

                _with1.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;

                _with1.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;

                _with1.Add("VESSEL", getDefault(VslVoy, 0)).Direction = ParameterDirection.Input;

                _with1.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;

                _with1.Add("COLUMN", getDefault(SortColumn, "LOCATION")).Direction = ParameterDirection.Input;

                _with1.Add("SORT", getDefault(SortType, "DESC")).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", getDefault(BizType, 0)).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", getDefault(ProcessType, 0)).Direction = ParameterDirection.Input;
                _with1.Add("JOBTYPE_IN", getDefault(JobType, 0)).Direction = ParameterDirection.Input;
                _with1.Add("RFE_NO_IN", getDefault(RefNo, "")).Direction = ParameterDirection.Input;

                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;

                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("BASE_CURR", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.InputOutput;

                _with1.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("POL_POD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                strPKGProc = GetProcedure.Split(',');
                dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                CreateRelation(dsAll);

                return dsAll;
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

        #endregion "Fetch Data"

        #region "Create Relation"

        private void CreateRelation(DataSet dsMain)
        {
            DataColumn parentCol = null;
            DataColumn childCol = null;

            parentCol = dsMain.Tables[0].Columns["LOCATION_NAME"];
            childCol = dsMain.Tables[1].Columns["LOCATION_NAME"];

            DataRelation relCust = null;
            relCust = new DataRelation("Trade", parentCol, childCol);

            DataRelation relLoc = null;
            relLoc = new DataRelation("Cust", new DataColumn[] {
                dsMain.Tables[1].Columns["LOCATION_NAME"],
                dsMain.Tables[1].Columns["POL"],
                dsMain.Tables[1].Columns["POD"]
            }, new DataColumn[] {
                dsMain.Tables[2].Columns["LOCATION_NAME"],
                dsMain.Tables[2].Columns["POL"],
                dsMain.Tables[2].Columns["POD"]
            });

            relCust.Nested = true;
            relLoc.Nested = true;

            dsMain.Relations.Add(relCust);
            dsMain.Relations.Add(relLoc);
        }

        #endregion "Create Relation"
    }
}