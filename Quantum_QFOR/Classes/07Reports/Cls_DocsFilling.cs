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
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_DocsFilling : CommonFeatures
    {
        #region "FetchContainerDetailsAirSeaBoth"

        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="VoyageName">Name of the voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(Int32 LocFk = 0, string CustName = "", string VslName = "", string VoyageName = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 expExcel = 0, Int32 CargoType = 0, Int32 CommGrp = 0, Int16 BizType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = "";
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And BST.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And BST.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JC.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (CommGrp != 0)
                {
                    strCondition = strCondition + " And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "";
                }
                if (BizType > 0)
                {
                    strCondition = strCondition + " And BST.BUSINESS_TYPE = " + BizType + "";
                }
                sb.Append(" SELECT BOOKING_PK,  ");
                sb.Append("        BOOKING_REF_NO,  ");
                sb.Append("        JOBCARD_PK,  ");
                sb.Append("        JOBCARD_REF_NO,  ");
                sb.Append("        SHIPMENT_DATE,  ");
                sb.Append("        BIZ_TYPE,  ");
                sb.Append("        CARGO_TYPE,  ");
                sb.Append("        CUSTOMER_NAME,  ");
                sb.Append("        POL,  ");
                sb.Append("        POD,  ");
                sb.Append("        VESSEL_FLIGHT,  ");
                sb.Append("        ETD_DATE,  ");
                sb.Append("        ETA_DATE,  ");
                sb.Append("        COMMODITY_GROUP_CODE,  ");
                sb.Append("        STUFF_LOC  ");
                sb.Append("   FROM (SELECT BST.BOOKING_MST_PK BOOKING_PK,  ");
                sb.Append("                BST.BOOKING_REF_NO,  ");
                sb.Append("                JC.JOB_CARD_TRN_PK JOBCARD_PK,  ");
                sb.Append("                JC.JOBCARD_REF_NO,  ");
                sb.Append("                JC.JOBCARD_DATE,  ");
                sb.Append("                TO_DATE(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,  ");
                sb.Append("                CASE  ");
                sb.Append("                  WHEN BST.BUSINESS_TYPE = 1 THEN  ");
                sb.Append("                   'Air'  ");
                sb.Append("                  ELSE  ");
                sb.Append("                   'Sea'  ");
                sb.Append("                END BIZ_TYPE,  ");
                sb.Append("                CASE  ");
                sb.Append("                  WHEN BST.BUSINESS_TYPE = 2 THEN  ");
                sb.Append("                   DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC')  ");
                sb.Append("                  ELSE  ");
                sb.Append("                   DECODE(BST.CARGO_TYPE, 1, 'KGS', 2, 'ULD')  ");
                sb.Append("                END CARGO_TYPE,  ");
                sb.Append("                CMT.CUSTOMER_NAME,  ");
                sb.Append("                POL.PORT_ID POL,  ");
                sb.Append("                POD.PORT_ID POD,  ");
                sb.Append("                CASE  ");
                sb.Append("                  WHEN BST.BUSINESS_TYPE=1 THEN  ");
                sb.Append("                   AMT.AIRLINE_NAME||'/'||JC.VOYAGE_FLIGHT_NO ");
                sb.Append("                  ELSE  ");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || JC.VOYAGE_FLIGHT_NO)  ");
                sb.Append("                END AS VESSEL_FLIGHT,  ");
                sb.Append("                JC.ETD_DATE ETD_DATE,  ");
                sb.Append("                JC.ETA_DATE ETA_DATE,  ");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,  ");
                sb.Append("                JC.STUFF_LOC  ");
                sb.Append("           FROM BOOKING_MST_TBL         BST,  ");
                sb.Append("                JOB_CARD_TRN            JC,  ");
                sb.Append("                CUSTOMER_MST_TBL        CMT,  ");
                sb.Append("                PORT_MST_TBL            POL,  ");
                sb.Append("                PORT_MST_TBL            POD,  ");
                sb.Append("                VESSEL_VOYAGE_TBL       VVT,  ");
                sb.Append("                VESSEL_VOYAGE_TRN       VT,  ");
                sb.Append("                COMMODITY_GROUP_MST_TBL CGMT,  ");
                sb.Append("                AIRLINE_MST_TBL         AMT  ");
                sb.Append("          WHERE BST.BOOKING_MST_PK = JC.BOOKING_MST_FK  ");
                sb.Append("            AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK  ");
                sb.Append("            AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK  ");
                sb.Append("            AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK  ");
                sb.Append("            AND JC.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)  ");
                sb.Append("            AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK  ");
                sb.Append("            AND CGMT.COMMODITY_GROUP_PK(+) = JC.COMMODITY_GROUP_FK  ");
                sb.Append("            AND JC.SB_NUMBER IS NULL  ");
                sb.Append("            AND JC.SB_DATE IS NULL  ");
                sb.Append("            AND POL.LOCATION_MST_FK =  " + LocFk + "");
                sb.Append("            AND BST.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+)  ");

                if (!string.IsNullOrEmpty(VoyageName))
                {
                    sb.Append(" And UPPER(JC.VOYAGE_FLIGHT_NO) = '" + VoyageName.ToUpper() + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And (UPPER(VVT.VESSEL_NAME) = '" + VslName.ToUpper() + "' OR UPPER(AMT.AIRLINE_NAME) = '" + VslName.ToUpper() + "')");
                }
                if (CargoType != 0)
                {
                    sb.Append(" AND BST.CARGO_TYPE = '" + CargoType + "'");
                }
                sb.Append(strCondition);
                sb.Append(" ) ORDER BY JOBCARD_PK DESC  ");
                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";
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
                strSQL = " SELECT * FROM (SELECT ROWNUM SLNO, q.* FROM(";
                strSQL += sb.ToString();
                if (expExcel == 0)
                {
                    strSQL += " )q ) WHERE SLNO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " )q )";
                }
                return objWF.GetDataSet(strSQL);
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

        #endregion "FetchContainerDetailsAirSeaBoth"

        #region " FetchPLBL "

        /// <summary>
        /// Fetches the grid details.
        /// </summary>
        /// <param name="InvoicePK">The invoice pk.</param>
        /// <param name="LocationFK">The location fk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <param name="VslVoyPk">The VSL voy pk.</param>
        /// <param name="FlightNo">The flight no.</param>
        /// <param name="POLFK">The polfk.</param>
        /// <param name="PODFK">The podfk.</param>
        /// <param name="HawbNr">The hawb nr.</param>
        /// <param name="MawbNr">The mawb nr.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Status">The status.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="CurPK">The current pk.</param>
        /// <param name="ChkonLoad">The chkon load.</param>
        /// <param name="ReportFlg">The report FLG.</param>
        /// <param name="isAdmin">The is admin.</param>
        /// <param name="txtRefNr">The text reference nr.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchGridDetails(string InvoicePK = "0", string LocationFK = "0", string BizType = "0", string Process = "0", string CustomerPK = "0", string VslVoyPk = "0", string FlightNo = "", string POLFK = "0", string PODFK = "", string HawbNr = "",
        string MawbNr = "0", string FromDate = "", string ToDate = "", string CargoType = "0", string Status = "0", string JobType = "0", string CurPK = "0", short ChkonLoad = 0, short ReportFlg = 0, Int32 isAdmin = 0,
        string txtRefNr = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with1 = objWF.MyDataAdapter;
                _with1.SelectCommand = new OracleCommand();
                _with1.SelectCommand.Connection = objWF.MyConnection;
                _with1.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_VAT_RPT_PKG.FETCH_VAT";
                _with1.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with1.SelectCommand.Parameters.Add("INVOICE_PK_IN", OracleDbType.Varchar2).Value = getDefault(InvoicePK, "");
                _with1.SelectCommand.Parameters.Add("LOCATION_MST_PK_IN", OracleDbType.Varchar2).Value = getDefault(LocationFK, "");
                _with1.SelectCommand.Parameters.Add("VESSEL_VOY_FK_IN", OracleDbType.Varchar2).Value = getDefault(VslVoyPk, "");
                _with1.SelectCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2).Value = getDefault(FlightNo, "");
                _with1.SelectCommand.Parameters.Add("CUSTOMER_PK_IN", OracleDbType.Varchar2).Value = getDefault(CustomerPK, "");
                _with1.SelectCommand.Parameters.Add("POL_PK_IN", OracleDbType.Varchar2).Value = getDefault(POLFK, "");
                _with1.SelectCommand.Parameters.Add("POD_PK_IN", OracleDbType.Varchar2).Value = getDefault(PODFK, "");
                _with1.SelectCommand.Parameters.Add("FROM_DATE_IN", OracleDbType.Varchar2).Value = getDefault(FromDate, "");
                _with1.SelectCommand.Parameters.Add("TO_DATE_IN", OracleDbType.Varchar2).Value = getDefault(ToDate, "");
                _with1.SelectCommand.Parameters.Add("BIZTYPE_IN", OracleDbType.Varchar2).Value = BizType;
                _with1.SelectCommand.Parameters.Add("PROCESS_IN", OracleDbType.Varchar2).Value = Process;
                _with1.SelectCommand.Parameters.Add("CARGOTYPE_IN", OracleDbType.Varchar2).Value = CargoType;
                _with1.SelectCommand.Parameters.Add("STATUS_IN", OracleDbType.Varchar2).Value = Status;
                _with1.SelectCommand.Parameters.Add("JOBTYPE_IN", OracleDbType.Varchar2).Value = JobType;
                _with1.SelectCommand.Parameters.Add("HAWB_NR_IN", OracleDbType.Varchar2).Value = getDefault(HawbNr, "");
                _with1.SelectCommand.Parameters.Add("MAWB_NR_IN", OracleDbType.Varchar2).Value = getDefault(MawbNr, "");
                _with1.SelectCommand.Parameters.Add("CURRENCY_MST_PK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(CurPK) ? HttpContext.Current.Session["CURRENCY_MST_PK"] : CurPK);
                _with1.SelectCommand.Parameters.Add("LOGGED_LOC_MST_PK_IN", OracleDbType.Int32).Value = HttpContext.Current.Session["LOGED_IN_LOC_FK"];
                //_with1.SelectCommand.Parameters.Add("IS_ADMIN_IN", OracleDbType.Int32).Value = (isAdmin ? 1 : 0);
                _with1.SelectCommand.Parameters.Add("REF_NR_IN", OracleDbType.Varchar2).Value = getDefault(txtRefNr, "");
                _with1.SelectCommand.Parameters.Add("LOAD_FLAG_IN", ChkonLoad).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("REPORT_FLAG_IN", ReportFlg).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = RecordsPerPage;
                _with1.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with1.SelectCommand.Parameters.Add("VAT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Fill(ds);
                //TotalPage = _with1.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value;
                //CurrentPage = _with1.SelectCommand.Parameters[("CURRENT_PAGE_IN"].Value;

                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion " FetchPLBL "
    }
}