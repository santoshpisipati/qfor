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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class clsOTC : CommonFeatures
    {
        private string type;

        public DataSet Fetch_Data(string BizType = "", string ShipStatus = "", string FROM_DATE = "", string TO_DATE = "", Int32 flag = 0, string CUSTOMER = "", string LOCATION = "", string SECTOR = "", string VESSEL = "", Int32 TotalPage = 0,
        Int32 CurrentPage = 0, string SortColumn = "BOOKING", string SortType = " DESC ", int FlagGrand = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTrade = null;
            DataTable dtCust = null;
            DataTable dtLocation = null;
            DataTable dtCommodity = null;
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

                _with1.Add("SECTOR", getDefault(SECTOR, 0)).Direction = ParameterDirection.Input;

                _with1.Add("VESSEL", getDefault(VESSEL, 0)).Direction = ParameterDirection.Input;

                _with1.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;

                _with1.Add("ONLOAD_IN", flag).Direction = ParameterDirection.InputOutput;

                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;

                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;

                _with1.Add("COLUMN", getDefault(SortColumn, "BOOKING")).Direction = ParameterDirection.Input;

                _with1.Add("SORT", getDefault(SortType, " DESC")).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", getDefault(BizType, 0)).Direction = ParameterDirection.Input;
                _with1.Add("SHIPMENT_STATUS_IN", getDefault(ShipStatus, 0)).Direction = ParameterDirection.Input;
                _with1.Add("FLAG_GRAND", getDefault(FlagGrand, 0)).Direction = ParameterDirection.Input;

                _with1.Add("BASE_CURR", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with1.Add("VSLVOY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("POL_POD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("BOOKING_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

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

        public DataSet GetLocation(string userLocPK, string strALL)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader dr = null;
            try
            {
                strQuery.Append("");
                strQuery.Append("   SELECT 'ALL' LOCATION_ID, ");
                strQuery.Append("       0 LOCATION_MST_PK, ");
                strQuery.Append("       0 REPORTING_TO_FK, ");
                strQuery.Append("       0 LOCATION_TYPE_FK ");
                strQuery.Append("  FROM DUAL ");
                strQuery.Append("UNION ");
                strQuery.Append(" SELECT L.LOCATION_ID, ");
                strQuery.Append("       L.LOCATION_MST_PK, ");
                strQuery.Append("       L.REPORTING_TO_FK, ");
                strQuery.Append("       L.LOCATION_TYPE_FK ");
                strQuery.Append("  FROM LOCATION_MST_TBL L ");
                strQuery.Append(" START WITH L.LOCATION_TYPE_FK = 1 ");
                strQuery.Append("        AND L.ACTIVE_FLAG = 1 ");
                strQuery.Append("        AND L.LOCATION_MST_PK =" + userLocPK);
                strQuery.Append(" CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK ");
                dr = objWF.GetDataReader(strQuery.ToString());
                while (dr.Read())
                {
                    strReturn += dr["LOCATION_MST_PK"] + "~$";
                }
                dr.Close();
                strALL = strReturn;
                return objWF.GetDataSet(strQuery.ToString());
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

        private void CreateRelation(DataSet dsMain)
        {
            DataRelation drVessel = null;
            DataRelation drCustomer = null;
            DataRelation drLoc = null;
            DataRelation POLPOD = null;

            try
            {
                drVessel = new DataRelation("Vessel", dsMain.Tables[0].Columns["VSLVOY"], dsMain.Tables[1].Columns["VSLVOY"]);
                drVessel.Nested = true;
                dsMain.Relations.Add(drVessel);

                drCustomer = new DataRelation("Cust", new DataColumn[] {
                    dsMain.Tables[1].Columns["VSLVOY"],
                    dsMain.Tables[1].Columns["CUSTOMER_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[2].Columns["VSLVOY"],
                    dsMain.Tables[2].Columns["CUSTOMER_NAME"]
                });
                drCustomer.Nested = true;
                dsMain.Relations.Add(drCustomer);

                drLoc = new DataRelation("Loc", new DataColumn[] {
                    dsMain.Tables[2].Columns["VSLVOY"],
                    dsMain.Tables[2].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[2].Columns["LOCATION_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[3].Columns["VSLVOY"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[3].Columns["LOCATION_NAME"]
                });
                drLoc.Nested = true;
                dsMain.Relations.Add(drLoc);

                POLPOD = new DataRelation("POLPOD", new DataColumn[] {
                    dsMain.Tables[3].Columns["VSLVOY"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["POL"],
                    dsMain.Tables[3].Columns["POD"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["VSLVOY"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["POL"],
                    dsMain.Tables[4].Columns["POD"]
                });
                POLPOD.Nested = true;
                dsMain.Relations.Add(POLPOD);
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

        public string GetProcedure
        {
            get
            {
                if (type == "ALL")
                {
                    return "ORDER_TO_CASH_PKG,FETCH_DATA_ALL";
                }
                return "";
            }
            set { type = value; }
        }
    }
}