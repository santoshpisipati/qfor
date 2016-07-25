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

using Oracle.DataAccess.Client;
using System;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_FACInvoice : CommonFeatures
	{

        #region "FETCH FAC INVOICE"
        /// <summary>
        /// Fetches the fac invoice.
        /// </summary>
        /// <param name="OperatorPK">The operator pk.</param>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="CARGO_TYPE">Type of the carg o_.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Summary">if set to <c>true</c> [summary].</param>
        /// <param name="FlightNr">The flight nr.</param>
        /// <returns></returns>
        public DataSet FetchFacInvoice(string OperatorPK = "", string VesselPK = "", string MBLPK = "", string FromDate = "", string ToDate = "", string POLPK = "", string PODPK = "", int CARGO_TYPE = 0, string BIZ_TYPE = "", string Process = "",
		bool Summary = false, string FlightNr = "")
		{
			WorkFlow objWF = new WorkFlow();
			DataSet dsAll = null;
			int CurrencyPK = 0;
			string LocationPK = null;
			try {
				objWF.MyCommand.Parameters.Clear();
				CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
				LocationPK = Convert.ToString(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
				if (Convert.ToInt32(BIZ_TYPE) == 1) {
					CARGO_TYPE = 0;
				}
				var _with1 = objWF.MyCommand.Parameters;
				_with1.Add("OPERATOR_PK_IN", (string.IsNullOrEmpty(OperatorPK) ? "" : OperatorPK)).Direction = ParameterDirection.Input;
				_with1.Add("VSL_VOY_FK_IN", (string.IsNullOrEmpty(VesselPK) ? "" : VesselPK)).Direction = ParameterDirection.Input;
				_with1.Add("MBL_PK_IN", (string.IsNullOrEmpty(MBLPK) ? "" : MBLPK)).Direction = ParameterDirection.Input;
				_with1.Add("CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
				_with1.Add("LOCATION_FK_IN", (string.IsNullOrEmpty(LocationPK) ? "" : LocationPK)).Direction = ParameterDirection.Input;
				_with1.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDate) ? "" : FromDate)).Direction = ParameterDirection.Input;
				_with1.Add("TODATE_IN", (string.IsNullOrEmpty(ToDate) ? "" : ToDate)).Direction = ParameterDirection.Input;
				_with1.Add("POL_PK_IN", (string.IsNullOrEmpty(POLPK) ? "" : POLPK)).Direction = ParameterDirection.Input;
				_with1.Add("POD_PK_IN", (string.IsNullOrEmpty(PODPK) ? "" : PODPK)).Direction = ParameterDirection.Input;
				_with1.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(BIZ_TYPE) ? "" : BIZ_TYPE)).Direction = ParameterDirection.Input;
				_with1.Add("PROCESS_TYPE_IN", (string.IsNullOrEmpty(Process) ? "" : Process)).Direction = ParameterDirection.Input;
				_with1.Add("CARGO_TYPE_IN", CARGO_TYPE).Direction = ParameterDirection.Input;
				_with1.Add("FLIGHT_NO_IN", (string.IsNullOrEmpty(FlightNr) ? "" : FlightNr)).Direction = ParameterDirection.Input;

				_with1.Add("OPR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with1.Add("MBL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with1.Add("HBL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dsAll = objWF.GetDataSet("FETCH_FACINVOICE_PKG1", "FETCH_FACINVOICE");

				if (Summary == true) {
					if (Convert.ToInt32(BIZ_TYPE) == 2) {
						DataRelation relOperator = new DataRelation("OPR", new DataColumn[] {
							dsAll.Tables[0].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[0].Columns["VOYAGE_TRN_PK"]
						}, new DataColumn[] {
							dsAll.Tables[1].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[1].Columns["VOYAGE_TRN_FK"]
						});
						relOperator.Nested = true;
						dsAll.Relations.Add(relOperator);
					} else {
						DataRelation relOperator = new DataRelation("OPR", new DataColumn[] {
							dsAll.Tables[0].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[0].Columns["VESSEL_NAME"]
						}, new DataColumn[] {
							dsAll.Tables[1].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[1].Columns["VESSEL_NAME"]
						});
						relOperator.Nested = true;
						dsAll.Relations.Add(relOperator);
					}

				} else {
					if (Convert.ToInt32(BIZ_TYPE) == 2) {
						DataRelation relOperator = new DataRelation("OPR", new DataColumn[] {
							dsAll.Tables[0].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[0].Columns["VOYAGE_TRN_PK"]
						}, new DataColumn[] {
							dsAll.Tables[1].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[1].Columns["VOYAGE_TRN_FK"]
						});
						DataRelation relFAC = new DataRelation("FACINV", new DataColumn[] { dsAll.Tables[1].Columns["MBL_EXP_TBL_PK"] }, new DataColumn[] { dsAll.Tables[2].Columns["MBL_EXP_TBL_PK"] });
						relOperator.Nested = true;
						relFAC.Nested = true;
						dsAll.Relations.Add(relOperator);
						dsAll.Relations.Add(relFAC);
					} else {
						DataRelation relOperator = new DataRelation("OPR", new DataColumn[] {
							dsAll.Tables[0].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[0].Columns["VESSEL_NAME"]
						}, new DataColumn[] {
							dsAll.Tables[1].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[1].Columns["VESSEL_NAME"]
						});
						DataRelation relFAC = new DataRelation("FACINV", new DataColumn[] { dsAll.Tables[1].Columns["MBL_EXP_TBL_PK"] }, new DataColumn[] { dsAll.Tables[2].Columns["MBL_EXP_TBL_PK"] });
						relOperator.Nested = true;
						relFAC.Nested = true;
						dsAll.Relations.Add(relOperator);
						dsAll.Relations.Add(relFAC);
					}
				}
				return dsAll;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "FETCH FAC INVOICE"
        /// <summary>
        /// Fetches the fac invoice history.
        /// </summary>
        /// <param name="OperatorPK">The operator pk.</param>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="CARGO_TYPE">Type of the carg o_.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Summary">if set to <c>true</c> [summary].</param>
        /// <param name="FlightNr">The flight nr.</param>
        /// <param name="InvPK">The inv pk.</param>
        /// <param name="InvStatus">The inv status.</param>
        /// <param name="Active">if set to <c>true</c> [active].</param>
        /// <returns></returns>
        public DataSet FetchFacInvoiceHistory(string OperatorPK = "", string VesselPK = "", string MBLPK = "", string FromDate = "", string ToDate = "", string POLPK = "", string PODPK = "", int CARGO_TYPE = 0, string BIZ_TYPE = "", string Process = "",
		bool Summary = false, string FlightNr = "", string InvPK = "", string InvStatus = "", bool Active = false)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet dsAll = null;
			int CurrencyPK = 0;
			string LocationPK = null;
			int IsActive = 0;
			try {
				objWF.MyCommand.Parameters.Clear();
				CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
				LocationPK = Convert.ToString(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
				if (Convert.ToInt32(BIZ_TYPE) == 1) {
					CARGO_TYPE = 0;
				}
				if (Active == true) {
					IsActive = 1;
				} else {
					IsActive = 0;
				}
				var _with2 = objWF.MyCommand.Parameters;
				_with2.Add("OPERATOR_PK_IN", (string.IsNullOrEmpty(OperatorPK) ? "" : OperatorPK)).Direction = ParameterDirection.Input;
				_with2.Add("VSL_VOY_FK_IN", (string.IsNullOrEmpty(VesselPK) ? "" : VesselPK)).Direction = ParameterDirection.Input;
				_with2.Add("MBL_PK_IN", (string.IsNullOrEmpty(MBLPK) ? "" : MBLPK)).Direction = ParameterDirection.Input;
				_with2.Add("CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
				_with2.Add("LOCATION_FK_IN", (string.IsNullOrEmpty(LocationPK) ? "" : LocationPK)).Direction = ParameterDirection.Input;
				_with2.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDate) ? "" : FromDate)).Direction = ParameterDirection.Input;
				_with2.Add("TODATE_IN", (string.IsNullOrEmpty(ToDate) ? "" : ToDate)).Direction = ParameterDirection.Input;
				_with2.Add("POL_PK_IN", (string.IsNullOrEmpty(POLPK) ? "" : POLPK)).Direction = ParameterDirection.Input;
				_with2.Add("POD_PK_IN", (string.IsNullOrEmpty(PODPK) ? "" : PODPK)).Direction = ParameterDirection.Input;
				_with2.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(BIZ_TYPE) ? "" : BIZ_TYPE)).Direction = ParameterDirection.Input;
				_with2.Add("CARGO_TYPE_IN", CARGO_TYPE).Direction = ParameterDirection.Input;
				_with2.Add("FLIGHT_NO_IN", (string.IsNullOrEmpty(FlightNr) ? "" : FlightNr)).Direction = ParameterDirection.Input;
				_with2.Add("INV_PK_IN", (string.IsNullOrEmpty(InvPK) ? "" : InvPK)).Direction = ParameterDirection.Input;
				_with2.Add("INV_STATUS_IN", (string.IsNullOrEmpty(InvStatus) ? "" : InvStatus)).Direction = ParameterDirection.Input;
				_with2.Add("ACTIVE_IN", IsActive).Direction = ParameterDirection.Input;

				_with2.Add("OPR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with2.Add("MBL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with2.Add("HBL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dsAll = objWF.GetDataSet("FETCH_FACINVOICE_PKG1", "FETCH_FACINVOICE_HISTORY");

				if (Summary == true) {
					if (Convert.ToInt32(BIZ_TYPE) == 2) {
						DataRelation relOperator = new DataRelation("OPR", new DataColumn[] {
							dsAll.Tables[0].Columns["PK"],
							dsAll.Tables[0].Columns["SUPPLIER_MST_FK"],
							dsAll.Tables[0].Columns["VOYAGE_TRN_FK"]
						}, new DataColumn[] {
							dsAll.Tables[1].Columns["SEL"],
							dsAll.Tables[1].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[1].Columns["VOYAGE_TRN_FK"]
						});
						relOperator.Nested = true;
						dsAll.Relations.Add(relOperator);
					} else {
						DataRelation relOperator = new DataRelation("OPR", new DataColumn[] {
							dsAll.Tables[0].Columns["PK"],
							dsAll.Tables[0].Columns["SUPPLIER_MST_FK"],
							dsAll.Tables[0].Columns["VESSEL_NAME"]
						}, new DataColumn[] {
							dsAll.Tables[1].Columns["SEL"],
							dsAll.Tables[1].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[1].Columns["VESSEL_NAME"]
						});
						relOperator.Nested = true;
						dsAll.Relations.Add(relOperator);
					}

				} else {
					if (Convert.ToInt32(BIZ_TYPE) == 2) {
						DataRelation relOperator = new DataRelation("OPR", new DataColumn[] {
							dsAll.Tables[0].Columns["PK"],
							dsAll.Tables[0].Columns["SUPPLIER_MST_FK"],
							dsAll.Tables[0].Columns["VOYAGE_TRN_FK"]
						}, new DataColumn[] {
							dsAll.Tables[1].Columns["SEL"],
							dsAll.Tables[1].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[1].Columns["VOYAGE_TRN_FK"]
						});
						DataRelation relFAC = new DataRelation("FACINV", new DataColumn[] {
							dsAll.Tables[1].Columns["SEL"],
							dsAll.Tables[1].Columns["MBL_EXP_TBL_PK"]
						}, new DataColumn[] {
							dsAll.Tables[2].Columns["SEL"],
							dsAll.Tables[2].Columns["MBL_EXP_TBL_PK"]
						});

						relOperator.Nested = true;
						relFAC.Nested = true;
						dsAll.Relations.Add(relOperator);
						dsAll.Relations.Add(relFAC);
					} else {
						DataRelation relOperator = new DataRelation("OPR", new DataColumn[] {
							dsAll.Tables[0].Columns["PK"],
							dsAll.Tables[0].Columns["SUPPLIER_MST_FK"],
							dsAll.Tables[0].Columns["VESSEL_NAME"]
						}, new DataColumn[] {
							dsAll.Tables[1].Columns["SEL"],
							dsAll.Tables[1].Columns["OPERATOR_MST_PK"],
							dsAll.Tables[1].Columns["VESSEL_NAME"]
						});
						DataRelation relFAC = new DataRelation("FACINV", new DataColumn[] {
							dsAll.Tables[1].Columns["SEL"],
							dsAll.Tables[1].Columns["MBL_EXP_TBL_PK"]
						}, new DataColumn[] {
							dsAll.Tables[2].Columns["SEL"],
							dsAll.Tables[2].Columns["MBL_EXP_TBL_PK"]
						});
						relOperator.Nested = true;
						relFAC.Nested = true;
						dsAll.Relations.Add(relOperator);
						dsAll.Relations.Add(relFAC);
					}
				}
				return dsAll;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "FETCH FAC BASIS"
        /// <summary>
        /// Fetches the fac basis.
        /// </summary>
        /// <param name="OperatorPK">The operator pk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="VslTrnpk">The VSL TRNPK.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public DataSet FetchFacBasis(string OperatorPK = "", string BIZ_TYPE = "", string VslTrnpk = "", string FromDt = "", string ToDt = "")
		{
			WorkFlow objWF = new WorkFlow();
			DataSet dsAll = null;
			int CurrencyPK = 0;
			string LocationPK = null;
			try {
				objWF.MyCommand.Parameters.Clear();
				CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
				LocationPK = Convert.ToString(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

				var _with3 = objWF.MyCommand.Parameters;
				_with3.Add("OPERATOR_PK_IN", (string.IsNullOrEmpty(OperatorPK) ? "" : OperatorPK)).Direction = ParameterDirection.Input;
				_with3.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(BIZ_TYPE) ? "" : BIZ_TYPE)).Direction = ParameterDirection.Input;
				_with3.Add("CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
				_with3.Add("LOCATION_FK_IN", LocationPK).Direction = ParameterDirection.Input;
				_with3.Add("VSLVOY_TRN_PK_IN", (string.IsNullOrEmpty(VslTrnpk) ? "" : VslTrnpk)).Direction = ParameterDirection.Input;
				_with3.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
				_with3.Add("TODATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
				_with3.Add("MBL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with3.Add("HBL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dsAll = objWF.GetDataSet("FETCH_FACINVOICE_PKG1", "FETCH_FACBASIS");
				DataRelation relOperator = new DataRelation("OPR", new DataColumn[] { dsAll.Tables[0].Columns["MBL_EXP_TBL_PK"] }, new DataColumn[] { dsAll.Tables[1].Columns["MBL_EXP_TBL_PK"] });
				relOperator.Nested = true;
				dsAll.Relations.Add(relOperator);
				return dsAll;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "UpdateInvStatus"
        /// <summary>
        /// Updates the inv status.
        /// </summary>
        /// <param name="InvoicePkS">The invoice pk s.</param>
        /// <param name="remarks">The remarks.</param>
        /// <param name="CUST_OR_AGENT">The cus t_ o r_ agent.</param>
        /// <returns></returns>
        public string UpdateInvStatus(string InvoicePkS, string remarks, short CUST_OR_AGENT = 0)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand updCmd = new OracleCommand();
			DataSet dsPia = null;
			string str = null;
			string strIns = null;
			string Ret = null;
			Int16 intDel = default(Int16);
			Int16 intIns = default(Int16);
			try {
				objWF.OpenConnection();
				var _with4 = updCmd;
				_with4.Connection = objWF.MyConnection;
				_with4.CommandType = CommandType.StoredProcedure;
				_with4.CommandText = objWF.MyUserName + ".INVOICE_CANCELLATION_PKG.CANCEL_INVOICE";
				var _with5 = _with4.Parameters;
				updCmd.Parameters.Add("INVOICE_PK_IN", InvoicePkS).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("INV_TYPE_FLAG_IN", "CONSOL_INV").Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				intIns = Convert.ToInt16(_with4.ExecuteNonQuery());
				return Convert.ToString(updCmd.Parameters["RETURN_VALUE"].Value);
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyConnection.Close();
			}
		}
		#endregion
	}
}
