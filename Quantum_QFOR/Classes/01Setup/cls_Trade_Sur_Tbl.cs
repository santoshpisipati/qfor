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
using System.Collections;
using System.Data;
namespace Quantum_QFOR
{
    public class cls_Trade_Sur_Tbl : CommonFeatures
	{

		#region "Fetch TradeSurcharge"

		public DataSet FetchTradeSur(string tradePK = "0", Int32 CurrentPage = 0, Int32 TotalPage = 0)
		{

			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			strQuery.Append("SELECT Count(*) from TRADE_SUR_TBL TRD," );
			strQuery.Append("FREIGHT_ELEMENT_MST_TBL FRE," );
			strQuery.Append("FREIGHT_ELEMENT_MST_TBL FRE1" );
			strQuery.Append("WHERE TRD.FRT_ELEMENT_MST_FK=FRE.FREIGHT_ELEMENT_MST_PK(+)" );
			strQuery.Append("AND TRD.FRT_APPLICABLE_ON_FK=FRE1.FREIGHT_ELEMENT_MST_PK(+)" );
			strQuery.Append("AND TRD.TRADE_MST_FK=" + tradePK );

			strSQL = strQuery.ToString();


			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / M_MasterPageSize;
			if (TotalRecords % M_MasterPageSize != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * M_MasterPageSize;
			start = (CurrentPage - 1) * M_MasterPageSize + 1;


			strQuery.Remove(0, strQuery.Length);
			strQuery.Append(" select * from (" );
			strQuery.Append("SELECT ROWNUM SR_NO,q.* FROM (" );

			strQuery.Append("SELECT" );
			strQuery.Append("TRD.TRADE_MST_FK," );
			strQuery.Append("FRE.FREIGHT_ELEMENT_MST_PK," );
			strQuery.Append("FRE.FREIGHT_ELEMENT_ID AS \"SURCHARGE_CODE\"," );
			strQuery.Append("FRE.FREIGHT_ELEMENT_NAME AS \"DESCRIPTION\"," );
			strQuery.Append("FRE1.FREIGHT_ELEMENT_MST_PK AS \"APPLICABLE_PK\"," );
			strQuery.Append("FRE1.FREIGHT_ELEMENT_ID AS \"APPLICABLE_ON\"," );
			strQuery.Append("TRD.PERC_APPLICABLE AS \"PERCENTAGE\", " );
			strQuery.Append("To_DATE(TRD.Fromdate,'" + dateFormat + "') AS \"VALID_FROMDATE\" ," );
			//Snigdharani
			strQuery.Append("To_DATE(TRD.Todate,'" + dateFormat + "') AS \"VALID_TODATE\" " );
			//Snigdharani
			strQuery.Append("FROM TRADE_SUR_TBL TRD," );
			strQuery.Append("FREIGHT_ELEMENT_MST_TBL FRE," );
			strQuery.Append("FREIGHT_ELEMENT_MST_TBL FRE1" );
			strQuery.Append("WHERE TRD.FRT_ELEMENT_MST_FK=FRE.FREIGHT_ELEMENT_MST_PK(+)" );
			strQuery.Append("AND TRD.FRT_APPLICABLE_ON_FK=FRE1.FREIGHT_ELEMENT_MST_PK(+)" );
			strQuery.Append("AND TRD.TRADE_MST_FK=" + tradePK );

			strQuery.Append("  ) q ) " );
			strQuery.Append(" WHERE SR_NO  Between " + start + " and " + last );

			strSQL = strQuery.ToString();

			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}


		}
		#endregion

		#region "TradePK"
		public string TradePK(string trdPK = "0")
		{
			try {
				string strSQL = null;
				string totRecords = "0";
				WorkFlow objTotRecCount = new WorkFlow();
				DataSet DsTradePk = new DataSet();
				strSQL = "select trd.trade_mst_pk from trade_mst_tbl trd";
				strSQL += "where trd.trade_code ='" + trdPK.ToUpper() + "'";
				DsTradePk = objTotRecCount.GetDataSet(strSQL.ToString());
				if (DsTradePk.Tables[0].Rows.Count > 0) {
					var _with1 = DsTradePk.Tables[0].Rows[0];
					totRecords = _with1["trade_mst_pk"].ToString();
				}
				return totRecords;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "GetTrade"
		public string GetTrade(string pol, string pod)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			DataSet dsTrade = new DataSet();
			string strRet = "";
			try {
				strSQL = " select mst.trade_mst_pk, mst.trade_code from trade_mst_tbl mst, trade_mst_trn trn ";
				strSQL += " where mst.trade_mst_pk = trn.trade_mst_fk and trn.port_mst_fk = " + pol;
				strSQL += " and mst.trade_mst_pk in ( select mst.trade_mst_pk from trade_mst_tbl mst, trade_mst_trn trn ";
				strSQL += " where mst.trade_mst_pk = trn.trade_mst_fk and trn.port_mst_fk = " + pod + ")";
				dsTrade = objWF.GetDataSet(strSQL.ToString());

				if (dsTrade.Tables[0].Rows.Count > 0) {
					var _with2 = dsTrade.Tables[0].Rows[0];
					strRet = _with2["trade_mst_pk"] + "~" + _with2["trade_code"];
				}
				return strRet;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Save"
		public ArrayList Save(DataSet GridDS, string TradePk)
		{

			WorkFlow objWK = new WorkFlow();
			OracleTransaction insertTrans = null;
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			int intPkValue = 0;
			bool isUpdate = false;
			int Afct = 0;
			int nRowCnt = 0;
			Int16 Del = default(Int16);
			objWK.OpenConnection();
			insertTrans = objWK.MyConnection.BeginTransaction();
			try {
				//On Insert New Record
				for (nRowCnt = 0; nRowCnt <= GridDS.Tables[0].Rows.Count - 1; nRowCnt++) {
					if (string.IsNullOrEmpty(GridDS.Tables[0].Rows[nRowCnt]["TRADE_MST_FK"].ToString())) {
						isUpdate = false;
						var _with3 = objWK.MyCommand;
						_with3.Transaction = insertTrans;
						_with3.Connection = objWK.MyConnection;
						_with3.CommandType = CommandType.StoredProcedure;
						_with3.CommandText = objWK.MyUserName + ".TRADE_SUR_TBL_PKG.TRADE_SUR_TBL_INS";
						_with3.Parameters.Clear();
						var _with4 = _with3.Parameters;
						_with4.Add("TRADE_MST_FK_IN", TradePk).Direction = ParameterDirection.Input;
						_with4.Add("FRT_ELEMENT_MST_FK_IN", GridDS.Tables[0].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
						_with4.Add("FRT_APPLICABLE_ON_FK_IN", GridDS.Tables[0].Rows[nRowCnt]["APPLICABLE_PK"]).Direction = ParameterDirection.Input;
						_with4.Add("PERC_APPLICABLE_IN", GridDS.Tables[0].Rows[nRowCnt]["PERCENTAGE"]).Direction = ParameterDirection.Input;
						//ADD BY PRAKASH INSERT THE FROMDATE AND TODATE 
						//Snigdharani
						if (!string.IsNullOrEmpty(GridDS.Tables[0].Rows[nRowCnt]["VALID_FROMDATE"].ToString())) {
							System.DateTime ValidFrom = Convert.ToDateTime(GridDS.Tables[0].Rows[nRowCnt]["VALID_FROMDATE"]);
							_with4.Add("VALIDITY_FROM_DATE_IN", ValidFrom).Direction = ParameterDirection.Input;
						} else {
							_with4.Add("VALIDITY_FROM_DATE_IN", "").Direction = ParameterDirection.Input;
						}
						//Snigdharani
						if (!string.IsNullOrEmpty(GridDS.Tables[0].Rows[nRowCnt]["VALID_TODATE"].ToString())) {
							System.DateTime ValidTo = Convert.ToDateTime(GridDS.Tables[0].Rows[nRowCnt]["VALID_TODATE"]);
							_with4.Add("VALIDITY_TO_DATE_IN", ValidTo).Direction = ParameterDirection.Input;
						} else {
							_with4.Add("VALIDITY_TO_DATE_IN", "").Direction = ParameterDirection.Input;
						}
						//END

						_with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

						Afct = _with3.ExecuteNonQuery();


						if (Afct > 0) {
							intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
						} else {
							insertTrans.Rollback();
						}


					} else {
						isUpdate = true;
						var _with5 = objWK.MyCommand;
						_with5.Transaction = insertTrans;
						_with5.Connection = objWK.MyConnection;
						_with5.CommandType = CommandType.StoredProcedure;
						_with5.CommandText = objWK.MyUserName + ".TRADE_SUR_TBL_PKG.TRADE_SUR_TBL_UPD";
						_with5.Parameters.Clear();
						var _with6 = _with5.Parameters;
						_with6.Add("TRADE_MST_FK_IN", GridDS.Tables[0].Rows[nRowCnt]["TRADE_MST_FK"]).Direction = ParameterDirection.Input;
						_with6.Add("FRT_ELEMENT_MST_FK_IN", GridDS.Tables[0].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
						_with6.Add("FRT_APPLICABLE_ON_FK_IN", GridDS.Tables[0].Rows[nRowCnt]["APPLICABLE_PK"]).Direction = ParameterDirection.Input;
						_with6.Add("PERC_APPLICABLE_IN", GridDS.Tables[0].Rows[nRowCnt]["PERCENTAGE"]).Direction = ParameterDirection.Input;
						//ADD BY PRAKASH INSERT THE FROMDATE AND TODATE 
						//Snigdharani
						if (!string.IsNullOrEmpty(GridDS.Tables[0].Rows[nRowCnt]["VALID_FROMDATE"].ToString())) {
							System.DateTime ValidFrom = Convert.ToDateTime(GridDS.Tables[0].Rows[nRowCnt]["VALID_FROMDATE"]);
							_with6.Add("VALIDITY_FROM_DATE_IN", ValidFrom).Direction = ParameterDirection.Input;
						} else {
							_with6.Add("VALIDITY_FROM_DATE_IN", "").Direction = ParameterDirection.Input;
						}
						//Snigdharani
						if (!string.IsNullOrEmpty(GridDS.Tables[0].Rows[nRowCnt]["VALID_TODATE"].ToString())) {
							System.DateTime ValidTo = Convert.ToDateTime(GridDS.Tables[0].Rows[nRowCnt]["VALID_TODATE"]);
							_with6.Add("VALIDITY_TO_DATE_IN", ValidTo).Direction = ParameterDirection.Input;
						} else {
							_with6.Add("VALIDITY_TO_DATE_IN", "").Direction = ParameterDirection.Input;
						}
						//END
						_with6.Add("DEL_IN", nRowCnt).Direction = ParameterDirection.Input;
						_with6.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;


						Afct = _with5.ExecuteNonQuery();

						if (Afct > 0) {
							intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);

						} else {
							insertTrans.Rollback();
						}
					}
				}

				if (arrMessage.Count > 0) {
					insertTrans.Rollback();
					return arrMessage;
				} else {
					insertTrans.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				insertTrans.Rollback();
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				insertTrans.Rollback();
				arrMessage.Add(ex.Message);
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion


	}
}
