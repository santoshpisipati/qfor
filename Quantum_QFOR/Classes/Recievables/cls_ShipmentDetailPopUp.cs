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
    public class clsShipmentDetailPopUp : CommonFeatures
	{
		#region "Fetch Grid Detail"
		public DataSet FetchBiztype(int ColPK)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet DS = new DataSet();
			WorkFlow objWF = new WorkFlow();
			sb.Append("SELECT C.CUSTOMER_MST_FK, C.AGENT_MST_FK, C.BUSINESS_TYPE, C.PROCESS_TYPE");
			sb.Append("  FROM COLLECTIONS_TBL C");
			sb.Append(" WHERE C.COLLECTIONS_TBL_PK =" + ColPK);
			DS = objWF.GetDataSet(sb.ToString());
			return DS;
		}
		public int FetchCargoType(int ColPK, int BizType, int ProcessType)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			int Cargo = 0;
			WorkFlow objWF = new WorkFlow();
			if (BizType == 2) {
				if (ProcessType == 1) {
					sb.Append("SELECT B.CARGO_TYPE");
					sb.Append("  FROM JOB_CARD_TRN J, BOOKING_MST_TBL B");
					sb.Append(" WHERE ");
					sb.Append("   B.BOOKING_MST_PK = J.BOOKING_MST_FK");
					sb.Append("  AND J.BUSINESS_TYPE = " + BizType);
					sb.Append("  AND J.PROCESS_TYPE = " + ProcessType);
					sb.Append("   AND J.JOB_CARD_TRN_PK = " + ColPK);
				} else {
					sb.Append("SELECT JJ.CARGO_TYPE");
					sb.Append("  FROM JOB_CARD_TRN JJ");
					sb.Append(" WHERE JJ.JOB_CARD_TRN_PK = " + ColPK);
					sb.Append("  AND JJ.BUSINESS_TYPE = " + BizType);
					sb.Append("  AND JJ.PROCESS_TYPE = " + ProcessType);
				}
			}
			Cargo = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
			return Cargo;
		}
		#endregion

		#region "Fetch All"
		public DataSet FetchJobPKID(int ColPk)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand objCommand = new OracleCommand();
			DataSet dsData = new DataSet();

			try {
				objWK.OpenConnection();
				objWK.MyCommand.Connection = objWK.MyConnection;

				var _with1 = objWK.MyCommand;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".SHIPMENT_DETAIL_PKG.JOBCARD_PK_FETCH";

				objWK.MyCommand.Parameters.Clear();
				var _with2 = objWK.MyCommand.Parameters;

				_with2.Add("COL_PK_IN", ColPk).Direction = ParameterDirection.Input;
				_with2.Add("JOB_PK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
				objWK.MyDataAdapter.Fill(dsData);

				return dsData;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		public DataSet FetchJobDetails(int ColPk, int JobPK, int BizType, int ProcessType)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand objCommand = new OracleCommand();
			DataSet dsData = new DataSet();

			try {
				objWK.OpenConnection();
				objWK.MyCommand.Connection = objWK.MyConnection;

				var _with3 = objWK.MyCommand;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".SHIPMENT_DETAIL_PKG.SHIPMENT_DETAIL_FETCH";

				objWK.MyCommand.Parameters.Clear();
				var _with4 = objWK.MyCommand.Parameters;

				_with4.Add("COL_PK_IN", ColPk).Direction = ParameterDirection.Input;
				_with4.Add("JOB_PK_IN", JobPK).Direction = ParameterDirection.Input;
				_with4.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with4.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
				_with4.Add("JOB_HEADER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with4.Add("CONTAINER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with4.Add("INVOICE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
				objWK.MyDataAdapter.Fill(dsData);

				return dsData;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion
	}
}
