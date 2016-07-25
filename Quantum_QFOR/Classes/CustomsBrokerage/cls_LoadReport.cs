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
namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_LoadReport : CommonFeatures
	{
        #region "Fetch Cargo Tracking"
        /// <summary>
        /// Fetches the load report.
        /// </summary>
        /// <param name="txtVslPK">The text VSL pk.</param>
        /// <param name="txtConsigneePK">The text consignee pk.</param>
        /// <param name="CBJCNrPK">The CBJC nr pk.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="PortChrg">The port CHRG.</param>
        /// <param name="PendingFor">The pending for.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="loc">The loc.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchLoadReport(int txtVslPK = 0, int txtConsigneePK = 0, int CBJCNrPK = 0, string Voyage = "", string PortChrg = "0", string PendingFor = "0", string RefType = "0", string RefNr = "", string Fromdt = "", string ToDt = "",
		Int32 Export = 0, int CurrentPage = 0, int TotalPage = 0, int loc = 0, int Flag = 0)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand objCommand = new OracleCommand();
			DataSet dsData = new DataSet();

			try {
				objWK.OpenConnection();
				objWK.MyCommand.Connection = objWK.MyConnection;

				var _with1 = objWK.MyCommand;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".CUSTOMS_BROKERAGE_PKG.LOAD_REPORT_FETCH";

				objWK.MyCommand.Parameters.Clear();
				var _with2 = objWK.MyCommand.Parameters;

				_with2.Add("VSL_PK_IN", txtVslPK).Direction = ParameterDirection.Input;
				_with2.Add("CONSIGNEE_PK_IN", txtConsigneePK).Direction = ParameterDirection.Input;
				_with2.Add("CBJC_PK_IN", CBJCNrPK).Direction = ParameterDirection.Input;
				_with2.Add("VOYAGE_IN", (string.IsNullOrEmpty(Voyage) ? "" : Voyage)).Direction = ParameterDirection.Input;
				_with2.Add("PORT_CHRG_IN", Convert.ToInt32(PortChrg)).Direction = ParameterDirection.Input;
				_with2.Add("PENDING_FOR_IN", Convert.ToInt32(PendingFor)).Direction = ParameterDirection.Input;
				_with2.Add("REF_TYPE_IN", Convert.ToInt32(RefType)).Direction = ParameterDirection.Input;
				_with2.Add("REF_NR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
				_with2.Add("FROM_DATE_IN", (string.IsNullOrEmpty(Fromdt) ? "" : Fromdt)).Direction = ParameterDirection.Input;
				_with2.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
				_with2.Add("LOCATION_PK_IN", loc).Direction = ParameterDirection.Input;
				_with2.Add("POST_BACK_IN", Flag).Direction = ParameterDirection.Input;
				_with2.Add("EXPORT_EXCEL_IN", Export).Direction = ParameterDirection.Input;
				_with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				_with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with2.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
				objWK.MyDataAdapter.Fill(dsData);
                TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
                if (TotalPage == 0) {
					CurrentPage = 0;
				} else {
                    CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
				return dsData;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
	}
}

