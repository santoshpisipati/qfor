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
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class clsSurcharge : CommonFeatures
	{

		#region "Private Variables"
			#endregion
		private long _PkValue;

		#region "Property"
		public long PkValue {
			get { return _PkValue; }
		}
		#endregion

		#region "Fetch surcharge"
		public DataSet Fetch_Surcharge_assign(DataSet dsGrid, int Int_pk = 0, string MAIN_TABLE = "", string TRN_TABLE = "", string MAIN_TABLE_PK = "", string PK_OUT = "", string TRN_TABLE_PK = "")
		{
			int Rcnt = 0;
			int Rcnt1 = 0;
			DataSet Dssurcharge = null;
			int RowCnt = 0;
			try {
				if (MAIN_TABLE == "TARIFF_MAIN_SEA_TBL" | MAIN_TABLE == "CONT_MAIN_SEA_TBL" | MAIN_TABLE == "RFQ_MAIN_SEA_TBL") {
					for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++) {
						if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE"))) {
							dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
						}
						Dssurcharge = Fetch_surcharge_fortwotable(Int_pk, MAIN_TABLE, TRN_TABLE, MAIN_TABLE_PK, PK_OUT, TRN_TABLE_PK, Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POLPK"]), Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PODPK"]));

						if (Dssurcharge.Tables[0].Rows.Count == 0) {
							Dssurcharge = Fetch_surcharge_fortwotable(0, MAIN_TABLE, TRN_TABLE, MAIN_TABLE_PK, PK_OUT, TRN_TABLE_PK, Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POLPK"]), Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PODPK"]));
						}

						for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++) {
							for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++) {
								if ((dsGrid.Tables[1].Rows[Rcnt]["FREIGHT_ELEMENT_MST_PK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"] & dsGrid.Tables[1].Rows[Rcnt]["PORT_MST_PK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["PORT_MST_PK1"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"])) {
									dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
								}
							}
						}
					}

				} else {
				}
				return (dsGrid);
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch surcharge"
		public DataSet Fetch_surcharge_fortwotable(int Valuepk = 0, string MAIN_TABLE = "", string TRN_TABLE = "", string MAIN_TABLE_PK = "", string PK_OUT = "", string TRN_TABLE_PK = "", int POL_PK = 0, int POD_PK = 0)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = null;
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			//'for 2-table

			try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".FETCH_SURCHARGE.FETCH_SURCHARGE_DATA";
				var _with1 = selectCommand.Parameters;
				_with1.Clear();
				_with1.Add("PK_IN", Valuepk).Direction = ParameterDirection.Input;
				_with1.Add("PK_OUT_IN", PK_OUT).Direction = ParameterDirection.Input;
				_with1.Add("MAIN_TABLE_IN", MAIN_TABLE).Direction = ParameterDirection.Input;
				_with1.Add("MAIN_TABLE_PK_IN", MAIN_TABLE_PK).Direction = ParameterDirection.Input;
				_with1.Add("TRN_TABLE_IN", TRN_TABLE).Direction = ParameterDirection.Input;
				_with1.Add("TRN_TABLE_PK_IN", TRN_TABLE_PK).Direction = ParameterDirection.Input;
				_with1.Add("POL_PK_IN", POL_PK).Direction = ParameterDirection.Input;
				_with1.Add("POD_PK_IN", POD_PK).Direction = ParameterDirection.Input;
				_with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value).Trim();

				return (objWF.GetDataSet(strReturn));
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}

		}


		public ArrayList SaveSurcharge(DataSet dsMain, long PkValue, int TrnPk_colnumber, OracleCommand SelectCommand, int Freight_Elepk_colno, string TRN_SEA_PK_COL = "", string TRN_SEA_FREIGHT_COL = "", string MAIN_SEA_COL = "", string TRN_TABLE_IN = "")
		{

			WorkFlow objWF = new WorkFlow();
			arrMessage.Clear();
			Int32 nRowCnt = default(Int32);
			Int32 IntReturn = default(Int32);
			Int32 nRowCnt1 = default(Int32);

			try {
				var _with2 = SelectCommand;
				if (dsMain.Tables["tblTransaction"].Rows.Count > 0) {
					for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++) {
						_with2.CommandType = CommandType.StoredProcedure;
						_with2.CommandText = objWF.MyUserName + ".RFQ_MAIN_SEA_TBL_PKG.SURCHARGE_UPD";
						var _with3 = _with2.Parameters;
						_with3.Clear();

						_with3.Add("TRN_TABLE_IN", TRN_TABLE_IN).Direction = ParameterDirection.Input;
						_with3.Add("TRN_SEA_PK_IN", TrnPk_colnumber).Direction = ParameterDirection.Input;
						_with3.Add("TRN_SEA_PK_COL_IN", TRN_SEA_PK_COL).Direction = ParameterDirection.Input;

						_with3.Add("TRN_SEA_FREIGHT_COL_IN", TRN_SEA_FREIGHT_COL).Direction = ParameterDirection.Input;
						_with3.Add("TRN_SEA_FREIGHT_VAL_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt][Freight_Elepk_colno]).Direction = ParameterDirection.Input;
						_with3.Add("MAIN_SEA_COL_IN", MAIN_SEA_COL).Direction = ParameterDirection.Input;
						_with3.Add("MAIN_SEA_PK_IN", PkValue).Direction = ParameterDirection.Input;
						_with3.Add("SURCHARGE_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"]).Direction = ParameterDirection.Input;
						_with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;

						_with2.ExecuteNonQuery();
						IntReturn = Convert.ToInt32(_with2.Parameters["RETURN_VALUE"].Value);
					}
				}

				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;

			} catch (OracleException oraexp) {
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
		}
		#endregion

		#region "Fetch for THree table"
		public DataSet Fetch_Surcharge(DataSet dsGrid, int FREIGHT_ELEMENT_MST_PK = 0, string MAIN_TABLE = "", int MAIN_TABLE_PK_VAL = 0, string MAIN_TABLE_PK_COL = "", string TRN_TABLE = "", string TRN_TABLE_PK_COL = "", string TRN_MAIN_FK_COL = "", string SURCHARGE_TABLE = "", string SURCHARGE_TRN_FK_COL = "",
		string SURCHARGE_TRN__FREIGHT_FK_COL = "", int POL_MST_FK = 0, int POD_MST_FK = 0)
		{
			int Rcnt = 0;
			int Rcnt1 = 0;
			int RowCnt = 0;
			DataSet Dssurcharge = null;
			//'for more than 3-table

			try {
				if (MAIN_TABLE == "CONT_MAIN_SEA_TBL") {
					for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++) {
						if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE"))) {
							dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
						}
						Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POLPK"]),
                        Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PODPK"]));
						for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++) {
							for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++) {
								if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]) & dsGrid.Tables[1].Rows[Rcnt]["POL"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["POD"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"]) {
									dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
								}
							}
						}
					}
					if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE"))) {
						dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
					}

				} else if (MAIN_TABLE == "cont_cust_sea_tbl" | MAIN_TABLE == "SRR_SEA_TBL") {
					for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++) {
						if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE"))) {
							dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
						}
						Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POLPK"]),
                        Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PODPK"]));
						for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++) {
							for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++) {
								if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]) & dsGrid.Tables[1].Rows[Rcnt]["POLPK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["PODPK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"]) {
									dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
								}
							}
						}
					}
					if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE"))) {
						dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
					}
				} else if (MAIN_TABLE == "QUOTATION_MST_TBL") {
					for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++) {
						if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE"))) {
							dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
						}
						if (POL_MST_FK > 0 & POD_MST_FK > 0) {
							Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, POL_MST_FK,
							POD_MST_FK);
						} else {
							Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POL_PK"]),
                            Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POD_PK"]));
						}

						for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++) {

							for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++) {
								if (POL_MST_FK > 0 & POD_MST_FK > 0) {
									if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"])) {
										dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
									}
								} else {
									if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]) & dsGrid.Tables[1].Rows[Rcnt]["POL_PK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["POD_PK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"]) {
										dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
									}
								}

							}
						}
					}
					if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE"))) {
						dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
					}
				} else if (MAIN_TABLE == "booking_sea_tbl") {
					for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++) {
						if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE"))) {
							dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
						}
						Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, POL_MST_FK,
						POD_MST_FK);
						for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++) {
							for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++) {
								if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]) & dsGrid.Tables[1].Rows[Rcnt]["PORT_MST_PK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["PORT_MST_PK1"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"]) {
									dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
								}
							}
						}
					}
					if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE"))) {
						dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
					}
				}

				//If Not (dsGrid.Tables(1).Columns.Contains("SURCHARGE")) Then

				//    dsGrid.Tables(1).Columns.Add(New DataColumn("SURCHARGE", GetType(String)))
				//    Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL)


				//    For Rcnt = 0 To dsGrid.Tables(1).Rows.Count - 1
				//        For Rcnt1 = 0 To Dssurcharge.Tables(0).Rows.Count - 1
				//            If (dsGrid.Tables(1).Rows(Rcnt).Item(FREIGHT_ELEMENT_MST_PK) = Dssurcharge.Tables(0).Rows(Rcnt1).Item("FREIGHT_ELEMENT_MST_PK")) Then
				//                dsGrid.Tables(1).Rows(Rcnt).Item("SURCHARGE") = Dssurcharge.Tables(0).Rows(Rcnt1).Item("SURCHARGE")
				//            End If
				//        Next
				//    Next

				//Else

				//    'Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL)
				//    'For Rcnt = 0 To dsGrid.Tables(1).Rows.Count - 1
				//    '    For Rcnt1 = 0 To Dssurcharge.Tables(0).Rows.Count - 1
				//    '        If (dsGrid.Tables(1).Rows(Rcnt).Item(FREIGHT_ELEMENT_MST_PK) = Dssurcharge.Tables(0).Rows(Rcnt1).Item("FREIGHT_ELEMENT_MST_PK")) Then
				//    '            dsGrid.Tables(1).Rows(Rcnt).Item("SURCHARGE") = Dssurcharge.Tables(0).Rows(Rcnt1).Item("SURCHARGE")
				//    '        End If
				//    '    Next
				//    'Next
				//End If
				return (dsGrid);
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#endregion

		#region "fetch thre table"
		public DataSet Fetch_surcharge_forThreetable(string MAIN_TABLE = "", int MAIN_TABLE_PK_VAL = 0, string MAIN_TABLE_PK_COL = "", string TRN_TABLE = "", string TRN_TABLE_PK_COL = "", string TRN_MAIN_FK_COL = "", string SURCHARGE_TABLE = "", string SURCHARGE_TRN_FK_COL = "", string SURCHARGE_TRN__FREIGHT_FK_COL = "", int POL_PK = 0,
		int POD_PK = 0)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = null;
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;

			try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".FETCH_QUOT_SURCHARGE.FETCH_SURCHARGE_DATA_PKG";
				var _with4 = selectCommand.Parameters;
				_with4.Clear();

				_with4.Add("MAIN_TABLE", MAIN_TABLE).Direction = ParameterDirection.Input;
				_with4.Add("MAIN_TABLE_PK_VAL", MAIN_TABLE_PK_VAL).Direction = ParameterDirection.Input;
				_with4.Add("MAIN_TABLE_PK_COL", MAIN_TABLE_PK_COL).Direction = ParameterDirection.Input;

				_with4.Add("TRN_TABLE", TRN_TABLE).Direction = ParameterDirection.Input;
				_with4.Add("TRN_TABLE_PK_COL", TRN_TABLE_PK_COL).Direction = ParameterDirection.Input;
				_with4.Add("TRN_MAIN_FK_COL", TRN_MAIN_FK_COL).Direction = ParameterDirection.Input;

				_with4.Add("SURCHARGE_TABLE", SURCHARGE_TABLE).Direction = ParameterDirection.Input;
				_with4.Add("SURCHARGE_TRN_FK_COL", SURCHARGE_TRN_FK_COL).Direction = ParameterDirection.Input;
				_with4.Add("SURCHARGE_TRN__FREIGHT_FK_COL", SURCHARGE_TRN__FREIGHT_FK_COL).Direction = ParameterDirection.Input;

				_with4.Add("POL_PK_IN", POL_PK).Direction = ParameterDirection.Input;
				_with4.Add("POD_PK_IN", POD_PK).Direction = ParameterDirection.Input;

				_with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value).Trim();
				return (objWF.GetDataSet(strReturn));
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}

		}
		#endregion

		#region "Fetch function for creating New RFQ "
		//This function returns the header for the band(0) in the grid.
		//fetchRFQHDR first retrives all the POL,POD combinations and then call function fetchActiveCont().
		//The result of the fetchActiveCont() is the all the active containers in row.
		//For loop: Transposes the rows written by fetchActiveCont into the columns of header in dtMain datatable
		public DataTable FetchRFQHDR(string strPolPk, string strPodPk, string strContId, bool IsLCL, string ValidFrom, string ValidTo, string Mode)
		{
			string str = null;
			WorkFlow objWF = new WorkFlow();
			DataTable dtMain = new DataTable();
			DataTable dtContainerType = new DataTable();
			//This datatable contains the active containers
			DataColumn dcCol = null;
			Int16 i = default(Int16);
			Array arrPolPk = null;
			Array arrPodPk = null;
			string strCondition = null;
			string strNewModeCondition = null;
			arrPolPk = strPolPk.Split(',');
			arrPodPk = strPodPk.Split(',');
			for (i = 0; i <= arrPolPk.Length - 1; i++) {
				if (string.IsNullOrEmpty(strCondition)) {
					strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
				} else {
					strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
				}
			}

			if (!(Mode == "EDIT")) {
				strNewModeCondition = " AND POL.BUSINESS_TYPE = 2";
				strNewModeCondition += " AND POD.BUSINESS_TYPE = 2";
			}

			str = " SELECT POL.PORT_MST_PK AS \"POLPK\",POL.PORT_ID AS \"POL\",";
			str += " POD.PORT_MST_PK AS \"PODPK\",POD.PORT_ID AS \"POD\",";
			str += " TO_DATE('" + ValidFrom + "','" + M_DateFormat + "') AS \"VALID_FROM\", ";
			str += " TO_DATE('" + ValidTo + "','" + M_DateFormat + "') AS \"VALID_TO\"";
			str += " FROM PORT_MST_TBL POL, PORT_MST_TBL POD";
			str += " WHERE (1=1)";
			str += " AND (";
			str += strCondition + ")";
			str += strNewModeCondition;
			str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_ID,POD.PORT_MST_PK";
			str += " HAVING POL.PORT_ID<>POD.PORT_ID";
			str += " ORDER BY POL.PORT_ID";
			try {
				dtMain = objWF.GetDataTable(str);
				if (!IsLCL) {
					dtContainerType = FetchActiveCont(strContId);
					//For loop: Transposes the rows written by fetchActiveCont into the columns 
					//of header in dtMain datatable
					for (i = 0; i <= dtContainerType.Rows.Count - 1; i++) {
						dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][0]), typeof(decimal));
						dtMain.Columns.Add(dcCol);
						dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(decimal));
						dtMain.Columns.Add(dcCol);
					}
				} else {
					dtMain.Columns.Add("Basis");
					dtMain.Columns.Add("Curr", typeof(decimal));
					dtMain.Columns.Add("Req", typeof(decimal));
					dtMain.Columns.Add("BasisPK");
				}
				return dtMain;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		//The result of the fetchActiveCont() is the all the active containers in row.
		//The sql fetchs two columns
		// 1. C-Container Type - Having Curr Rate.
		public DataTable FetchActiveCont(string strContId = "")
		{
			string str = null;
			WorkFlow objWF = new WorkFlow();
			if (string.IsNullOrEmpty(strContId)) {
				strContId = " AND ROWNUM <=10 ";
			} else {
				strContId = " AND CTMT.CONTAINER_TYPE_MST_ID IN (" + strContId + ") ";
			}

			str = " SELECT 'C-' || CTMT.CONTAINER_TYPE_MST_ID,CTMT.CONTAINER_TYPE_MST_ID" + " FROM CONTAINER_TYPE_MST_TBL CTMT WHERE CTMT.ACTIVE_FLAG=1" + strContId + " ORDER BY CTMT.PREFERENCES";
			try {
				return objWF.GetDataTable(str);
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "fetch"
		//This function returns the header for the band(1) in the grid.
		//fetchRFQHDR first retrives all the POL,POD combinations 
		//with active freight elements and base currency and then call function fetchActiveCont() 
		//the result of the fetchActiveCont() is the all the active containers in row.
		//For loop: Transposes the rows written by fetchActiveCont into the columns of header in dtMain datatable
		public DataTable FetchFreight(string strPolPk, string strPodPk, string strContId, bool IsLCL, string Mode)
		{
			string str = null;
			WorkFlow objWF = new WorkFlow();
			DataTable dtMain = new DataTable();
			DataTable dtContainerType = new DataTable();
			//This datatable contains the active containers
			DataColumn dcCol = null;
			Int16 i = default(Int16);
			Array arrPolPk = null;
			Array arrPodPk = null;
			string strCondition = null;
			string strNewModeCondition = null;
			arrPolPk = strPolPk.Split(',');
			arrPodPk = strPodPk.Split(',');
			//Making condition as the record should have only selected POL and POD
			//POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
			//is the selected sector.
			for (i = 0; i <= arrPolPk.Length - 1; i++) {
				if (string.IsNullOrEmpty(strCondition)) {
					strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
				} else {
					strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
				}
			}

			if (!(Mode == "EDIT")) {
				strNewModeCondition = " AND POL.BUSINESS_TYPE = 2";
				strNewModeCondition += " AND POD.BUSINESS_TYPE = 2";
				strNewModeCondition += " AND FMT.ACTIVE_FLAG = 1";
				strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (2,3)";
			}
			str = " SELECT Q.POLPK PORT_MST_PK, ";
			str += " Q.POL, ";
			str += " Q.PODPK PORT_MST_PK, ";
			str += " Q.POD, ";
			str += " Q.FREIGHT_ELEMENT_MST_PK, ";
			str += " Q.FREIGHT_ELEMENT_ID, ";
			str += " Q.CHARGE_BASIS, ";
			str += " Q.CHK, ";
			str += " Q.CURRENCY_MST_PK, ";
			str += " Q.CURRENCY_ID ";
			str += " FROM (";
			str += " ";
			str += " SELECT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
			str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
			str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
			//'Added By Koyeshwari 22/3/2011
			str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,";
			//'End
			str += " TO_CHAR('0') AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID";
			str += " ,FMT.PREFERENCE ";
			str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
			str += " CURRENCY_TYPE_MST_TBL CURR ";
			//removed "Corporate mst. table"  by thiyagarajan on 18/11/08 for loc. based curr.
			str += " WHERE (1=1)";
			str += " AND (";
			str += strCondition + ")";
			str += strNewModeCondition;
			//adding  "HttpContext.Current.Session("CURRENCY_MST_PK")"  by thiyagarajan on 18/11/08 for loc. based curr.
			str += " AND CURR.CURRENCY_MST_PK =" + HttpContext.Current.Session["CURRENCY_MST_PK"];
			//modified
			str += " AND FMT.CHARGE_TYPE <> 3 ";
			//'
			//Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column.
			str += " AND FMT.FREIGHT_ELEMENT_MST_PK IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
			str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
			str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.CHARGE_BASIS,";
			str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,FMT.PREFERENCE";
			str += " HAVING POL.PORT_ID<>POD.PORT_ID";
			//str &= vbCrLf & " ORDER BY FMT.FREIGHT_ELEMENT_ID"

			str += "UNION ALL";
			//Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column.
			str += " SELECT POL.PORT_MST_PK POLPK,POL.PORT_ID AS \"POL\",";
			str += " POD.PORT_MST_PK PODPK,POD.PORT_ID AS \"POD\", ";
			str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,";
			//'Added By Koyeshwari 22/3/2011
			str += " DECODE(FMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,";
			//'End
			str += " TO_CHAR('0') AS \"CHK\",CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID";
			str += " ,FMT.PREFERENCE ";
			str += " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,";
			str += " CURRENCY_TYPE_MST_TBL CURR ";
			//removed "Corporate mst. table"  by thiyagarajan on 18/11/08 for loc. based curr.
			str += " WHERE (1=1)";
			str += " AND (";
			str += strCondition + ")";
			str += strNewModeCondition;
			//adding  "HttpContext.Current.Session("CURRENCY_MST_PK")"  by thiyagarajan on 18/11/08 for loc. based curr.
			str += " AND CURR.CURRENCY_MST_PK =" + HttpContext.Current.Session["CURRENCY_MST_PK"];
			//modified
			str += " AND FMT.CHARGE_TYPE <> 3 ";
			//'
			str += " AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN(SELECT PARM.frt_bof_fk FROM PARAMETERS_TBL PARM)";
			str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,";
			str += " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.CHARGE_BASIS,";
			str += " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,FMT.PREFERENCE";
			str += " HAVING POL.PORT_ID<>POD.PORT_ID";
			str += " ) Q ORDER BY Q.PREFERENCE ";
			//str &= vbCrLf & " ORDER BY FMT.FREIGHT_ELEMENT_ID"
			//End by rabbani on 24/3/07
			try {
				dtMain = objWF.GetDataTable(str);
				if (!IsLCL) {
					dtContainerType = FetchActiveCont(strContId);
					//For loop: Transposes the rows written by fetchActiveCont into the columns 
					//of header in dtMain datatable
					for (i = 0; i <= dtContainerType.Rows.Count - 1; i++) {
						dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][0]), typeof(decimal));
						dtMain.Columns.Add(dcCol);
						dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(decimal));
						dtMain.Columns.Add(dcCol);
					}
				} else {
					dtMain.Columns.Add("Basis");
					dtMain.Columns.Add("CurrMin", typeof(decimal));
					//ADDED BY RABBANI REASON USS GAP ON 7/2/07
					dtMain.Columns.Add("Curr", typeof(decimal));
					dtMain.Columns.Add("ReqMin", typeof(decimal));
					//ADDED BY RABBANI REASON USS GAP ON 7/2/07
					dtMain.Columns.Add("Req", typeof(decimal));
					dtMain.Columns.Add("BasisPK");
				}
				return dtMain;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Called by Select Container/Sector"
		//This function returns all the active sectors from the database.
		//If the given POL and POD are present then the value will come as checked.
		public DataTable ActiveSector(long LocationPk, string strPOLPk = "", string strPODPk = "", string TariffGrpPk = "0", int strGroup = 0)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			Int16 i = default(Int16);
			Array arrPolPk = null;
			Array arrPodPk = null;
			string strCondition = null;
			arrPolPk = strPOLPk.Split(',');
			arrPodPk = strPODPk.Split(',');
			//Making condition as the record should have only selected POL and POD
			//POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
			//is the selected sector.
			if (strGroup == 1) {
				strSQL = " SELECT POLGP.PORT_GRP_MST_PK PORT_MST_PK," + " POLGP.PORT_GRP_ID AS POL," + " PODGP.PORT_GRP_MST_PK PORT_MST_PK," + " PODGP.PORT_GRP_ID PORT_ID," + " '0' CHK" + " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP" + " WHERE(POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK)" + " AND POLGP.PORT_GRP_MST_PK IN (" + strPOLPk + ")" + " AND PODGP.PORT_GRP_MST_PK IN (" + strPODPk + ")" ;
			} else if (strGroup == 2) {
				strSQL = " SELECT POLGP.PORT_GRP_MST_PK PORT_MST_PK," + " POLGP.PORT_GRP_ID AS POL," + " PODGP.PORT_GRP_MST_PK PORT_MST_PK," + " PODGP.PORT_GRP_ID PORT_ID," + " '0' CHK" + " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP" + " WHERE(POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK)" + " AND POLGP.PORT_GRP_MST_PK IN (" + strPOLPk + ")" + " AND PODGP.PORT_GRP_MST_PK IN (" + strPODPk + ")" ;
			} else {
				for (i = 0; i <= arrPolPk.Length - 1; i++) {
					if (string.IsNullOrEmpty(strCondition)) {
						strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
					} else {
						strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
					}
				}
				strSQL = "";

				//Creating the sql if the user has already selected one port pair in calling form 
				//incase of veiwing also then that port pair will come and active port pair in the grid.
				//BUSINESS_TYPE = 2 :- Is the business type for SEA            
				strSQL = "SELECT POL.PORT_MST_PK ,POL.PORT_ID AS \"POL\", " + "POD.PORT_MST_PK,POD.PORT_ID,'1' CHK " + "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  " + "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " + "AND POL.BUSINESS_TYPE = 2 " + "AND POD.BUSINESS_TYPE = 2 " + "AND ( " + strCondition + " ) " + "UNION " + "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, " + "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK " + "FROM SECTOR_MST_TBL SMT, " + "PORT_MST_TBL POL, " + "PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM " + "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + "AND   LPM.PORT_MST_FK = POL.PORT_MST_PK " + "AND   POL.BUSINESS_TYPE = 2 " + "AND   POD.BUSINESS_TYPE = 2 " + "AND   LPM.LOCATION_MST_FK =" + LocationPk + "AND ( " + strCondition + " ) " + "AND   SMT.ACTIVE = 1 " + "ORDER BY CHK DESC,POL";
			}
			try {
				return objWF.GetDataTable(strSQL);
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public DataTable ActiveContainers(string strContainer = "")
		{
			string strSQL = null;
			Int16 i = default(Int16);
			string strCondition = null;
			Array arrContainer = null;
			WorkFlow objWF = new WorkFlow();
			strSQL = "";
			arrContainer = strContainer.Split(',');
			for (i = 0; i <= arrContainer.Length - 1; i++) {
				if (string.IsNullOrEmpty(strCondition)) {
					if (arrContainer.GetValue(i).ToString() == "0") {
						strCondition = " ( CMT.CONTAINER_TYPE_MST_ID ='" + arrContainer.GetValue(i).ToString() + "')";
					} else {
						strCondition = " ( CMT.CONTAINER_TYPE_MST_ID =" + arrContainer.GetValue(i).ToString() + ")";
					}

				} else {
					strCondition += " OR ( CMT.CONTAINER_TYPE_MST_ID =" + arrContainer.GetValue(i).ToString() + ")";
				}
			}

			if (!string.IsNullOrEmpty(strContainer)) {
				strSQL = "SELECT " + " CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + " '1' CHK " + " FROM CONTAINER_TYPE_MST_TBL CMT " + " WHERE CMT.ACTIVE_FLAG = 1  " + "AND ( " + strCondition + " ) " + "  UNION " + " SELECT " + " CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK " + " FROM CONTAINER_TYPE_MST_TBL CMT " + " WHERE CMT.ACTIVE_FLAG=1  " + " ORDER BY CHK DESC";
			} else {
				strSQL = "SELECT " + "CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + " 0 CHK " + "FROM CONTAINER_TYPE_MST_TBL CMT " + "WHERE CMT.ACTIVE_FLAG = 1  " + "ORDER BY CMT.PREFERENCES";
			}
			try {
				return objWF.GetDataTable(strSQL);
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataTable ActiveBasis(string strContainer = "")
		{
			string strSQL = null;
			Int16 i = default(Int16);
			string strCondition = null;
			Array arrContainer = null;
			WorkFlow objWF = new WorkFlow();
			strSQL = "";
			arrContainer = strContainer.Split(',');
			for (i = 0; i <= arrContainer.Length - 1; i++) {
				if (string.IsNullOrEmpty(strCondition)) {
					if (arrContainer.GetValue(i).ToString() == "0") {
						strCondition = " ( DUMT.DIMENTION_ID ='" + arrContainer.GetValue(i).ToString() + "')";
					} else {
						strCondition = " ( DUMT.DIMENTION_ID =" + arrContainer.GetValue(i).ToString() + ")";
					}

				} else {
					strCondition += " OR ( DUMT.DIMENTION_ID =" + arrContainer.GetValue(i).ToString() + ")";
				}
			}

			if (!string.IsNullOrEmpty(strContainer)) {
				strSQL = "SELECT " + " DUMT.DIMENTION_UNIT_MST_PK , DUMT.DIMENTION_ID, " + " '1' CHK " + " FROM DIMENTION_UNIT_MST_TBL DUMT " + " WHERE DUMT.ACTIVE=1  " + "AND ( " + strCondition + " ) " + "  UNION " + " SELECT " + " DUMT.DIMENTION_UNIT_MST_PK, DUMT.DIMENTION_ID, " + "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK " + " FROM DIMENTION_UNIT_MST_TBL DUMT " + " WHERE DUMT.ACTIVE=1  " + " ORDER BY CHK DESC";
			} else {
				strSQL = "SELECT " + "DUMT.DIMENTION_UNIT_MST_PK, DUMT.DIMENTION_ID, " + " 0 CHK " + "FROM DIMENTION_UNIT_MST_TBL DUMT " + "WHERE DUMT.ACTIVE=1  " + "ORDER BY DUMT.DIMENTION_ID";
			}
			try {
				return objWF.GetDataTable(strSQL);
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Save"
		//This region save the RFQ in database
		//Here first the data is entered into the Header Table (RFQ_MAIN_SEA_TBL) then taking the PkValue of the 
		//header the transaction table is filled (RFQ_TRN_SEA_FCL_LCL)
		//Concurrency control is take care as the OracleCommand itself is send as a 
		//parameter to the function filling transaction table
		public ArrayList SaveRFQ(DataSet dsMain, object txtRFQNo, long nPrevRFQPk, long nLocationId, long nEmpId)
		{
			string RFQRefNo = null;
			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();
			arrMessage.Clear();
			objWK.MyCommand.Transaction = TRAN;
			try {
				if (string.IsNullOrEmpty(txtRFQNo.ToString())) {
					RFQRefNo = GenerateRFQNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK);
					if (RFQRefNo == "Protocol Not Defined.") {
						arrMessage.Add("Protocol Not Defined.");
						return arrMessage;
					} else {
						txtRFQNo = RFQRefNo;
					}
				} else {
					RFQRefNo = txtRFQNo.ToString();
				}
				objWK.MyCommand.Parameters.Clear();
				var _with5 = objWK.MyCommand;
				_with5.CommandType = CommandType.StoredProcedure;
				_with5.CommandText = objWK.MyUserName + ".RFQ_MAIN_SEA_TBL_PKG.RFQ_MAIN_SEA_TBL_INS";
				//OperatorFk
				_with5.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
				_with5.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				//PREV_RFQ_PK
				_with5.Parameters.Add("PREV_RFQ_PK", nPrevRFQPk).Direction = ParameterDirection.Input;
				//RFQ NO.
				_with5.Parameters.Add("RFQ_REF_NO_IN", RFQRefNo).Direction = ParameterDirection.Input;
				//RFQ Generation Date
				_with5.Parameters.Add("RFQ_DATE_IN", dsMain.Tables["tblMaster"].Rows[0]["RFQ_DATE"]).Direction = ParameterDirection.Input;
				_with5.Parameters["RFQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
				//Master starting validity date for the RFQ
				_with5.Parameters.Add("VALID_FROM_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["VALID_FROM"])).Direction = ParameterDirection.Input;
				_with5.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;
				//Master ending validity date for the RFQ (If given)
				_with5.Parameters.Add("VALID_TO_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["VALID_TO"])).Direction = ParameterDirection.Input;
				_with5.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;
				//Cargo Type
				_with5.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
				_with5.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
				//CommodityFk
				if (!string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_MST_FK"].ToString())) {
					_with5.Parameters.Add("COMMODITY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;
				} else {
					_with5.Parameters.Add("COMMODITY_MST_FK_IN", "").Direction = ParameterDirection.Input;
				}
				_with5.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				//CreatedByFk (Logged in UserPk )
				_with5.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
				if (!string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["BASE_CURRENCY_FK"].ToString())) {
					_with5.Parameters.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["BASE_CURRENCY_FK"])).Direction = ParameterDirection.Input;
				} else {
					_with5.Parameters.Add("BASE_CURRENCY_FK_IN", "").Direction = ParameterDirection.Input;
				}

				_with5.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with5.ExecuteNonQuery();

				_PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
				arrMessage = SaveRFQTRN(dsMain, _PkValue, objWK.MyCommand);


				if (arrMessage.Count > 0) {
					if (string.Compare(arrMessage[0].ToString(), "saved")>0) {
						arrMessage.Add("All data saved successfully");
						TRAN.Commit();
						return arrMessage;
					} else {
						RollbackProtocolKey("OPERATOR RFQ", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RFQRefNo, System.DateTime.Now);
						//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
						TRAN.Rollback();
						return arrMessage;
					}
				}
			} catch (OracleException oraexp) {
				RollbackProtocolKey("OPERATOR RFQ", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RFQRefNo, System.DateTime.Now);
				//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
				TRAN.Rollback();
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				RollbackProtocolKey("OPERATOR RFQ", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RFQRefNo, System.DateTime.Now);
				//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
				TRAN.Rollback();
				arrMessage.Add(ex.Message);
				return arrMessage;
			} finally {
				objWK.MyCommand.Connection.Close();
			}
            return new ArrayList();
		}

		public ArrayList SaveRFQTRN(DataSet dsMain, long PkValue, OracleCommand SelectCommand)
		{
			Int32 nRowCnt = default(Int32);
			WorkFlow objWK = new WorkFlow();
			arrMessage.Clear();
			int Trnpk = 0;

			try {
				var _with6 = SelectCommand;
				_with6.CommandType = CommandType.StoredProcedure;
				_with6.CommandText = objWK.MyUserName + ".RFQ_TRN_SEA_FCL_LCL_PKG.RFQ_TRN_SEA_FCL_LCL_INS";
				for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++) {
					SelectCommand.Parameters.Clear();
					//RFQ Fk
					_with6.Parameters.Add("RFQ_MAIN_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

					//POL Fk
					_with6.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;
					_with6.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

					//POD Fk
					_with6.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
					_with6.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

					//Freight Element Fk
					_with6.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"])).Direction = ParameterDirection.Input;
					_with6.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

					//All In Rate
					_with6.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"])).Direction = ParameterDirection.Input;
					_with6.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;

					//Currency FK
					_with6.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;
					_with6.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

					//If LCL Basis is null the send null to the database in case of FCL
					//Else send the data in case of LCL
					if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"].ToString())) {
						_with6.Parameters.Add("LCL_BASIS_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("LCL_BASIS_IN", Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_BASIS"])).Direction = ParameterDirection.Input;
					}
					_with6.Parameters["LCL_BASIS_IN"].SourceVersion = DataRowVersion.Current;

					//If LCL Current Rate is null the send null to the database in case of FCL
					//Else send the data in case of LCL
					if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"].ToString())) {
						_with6.Parameters.Add("LCL_CURRENT_RATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("LCL_CURRENT_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_RATE"])).Direction = ParameterDirection.Input;
					}
					_with6.Parameters["LCL_CURRENT_RATE_IN"].SourceVersion = DataRowVersion.Current;

					//If LCL Requested Rate is null the send null to the database in case of FCL
					//Else send the data in case of LCL
					if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_RATE"].ToString())) {
						_with6.Parameters.Add("LCL_REQUEST_RATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("LCL_REQUEST_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_RATE"])).Direction = ParameterDirection.Input;
					}
					_with6.Parameters["LCL_REQUEST_RATE_IN"].SourceVersion = DataRowVersion.Current;
					if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHARGE_BASIS"].ToString())) {
						_with6.Parameters.Add("CHARGE_BASIS_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("CHARGE_BASIS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CHARGE_BASIS"]).Direction = ParameterDirection.Input;
					}
					if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"].ToString())) {
						_with6.Parameters.Add("CONTAINER_DTL_FCL_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("CONTAINER_DTL_FCL_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_DTL_FCL"]).Direction = ParameterDirection.Input;
					}
					_with6.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;

					//Valid From date
					_with6.Parameters.Add("VALID_FROM_IN", Convert.ToDateTime(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_FROM"])).Direction = ParameterDirection.Input;

					//VALID_TO_IN
					if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"].ToString())) {
						_with6.Parameters.Add("VALID_TO_IN", Convert.ToDateTime(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"])).Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("VALID_TO_IN", "").Direction = ParameterDirection.Input;
					}
					if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"].ToString())) {
						_with6.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("LCL_CURRENT_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"])).Direction = ParameterDirection.Input;
					}
					_with6.Parameters["LCL_CURRENT_MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;
					//ADDED BY RABBANI REASON USS GAP ON 07/2/07
					if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"].ToString())) {
						_with6.Parameters.Add("LCL_REQUEST_MIN_RATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("LCL_REQUEST_MIN_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"])).Direction = ParameterDirection.Input;
					}
					_with6.Parameters["LCL_REQUEST_MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;

					//Return value of the proc.
					_with6.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

					//'added by subhransu for surcharge implementation
					if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"].ToString())) {
						_with6.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("SURCHARGE_IN", (dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
					}
					_with6.ExecuteNonQuery();
					Trnpk = Convert.ToInt32(_with6.Parameters["RETURN_VALUE"].Value);

				}
				if (arrMessage.Count == 0) {
					arrMessage.Add("All data saved successfully");
				}

				return arrMessage;
			} catch (OracleException oraexp) {
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
		}

		public string GenerateRFQNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
		{
			string functionReturnValue = null;
			try {
				functionReturnValue = GenerateProtocolKey("OPERATOR RFQ", nLocationId, nEmployeeId, DateTime.Now, "","" ,"" , nCreatedBy, objWK);
				return functionReturnValue;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
			return functionReturnValue;
		}
		#endregion

		#region "Fetch RFQ"
		//This function fetch the RFQ from the database against the supplied RFQ Pk and selected container type
		public DataSet FetchRFQ(long nRFQPk, Int16 nIsLCL)
		{
			try {
				string strSQL = null;
				if (nIsLCL == 1) {
					strSQL = "SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, " + "RFQHDR.RFQ_REF_NO,RFQHDR.RFQ_DATE, " + "RFQHDR.CARGO_TYPE, RFQHDR.COMMODITY_MST_FK, RFQHDR.VALID_FROM, " + "RFQHDR.VALID_TO,RFQHDR.VERSION_NO, " + "RFQTRN.PORT_MST_POL_FK,RFQTRN.PORT_MST_POD_FK,RFQTRN.CHECK_FOR_ALL_IN_RT, " + "RFQTRN.FREIGHT_ELEMENT_MST_FK,RFQTRN.CURRENCY_MST_FK,RFQTRN.CHARGE_BASIS,CURR.CURRENCY_ID, " + "TO_CHAR(RFQTRN.VALID_FROM,'" + dateFormat + "') AS \"P_VALID_FROM\", " + "TO_CHAR(RFQTRN.VALID_TO,'" + dateFormat + "') AS \"P_VALID_TO\", " + "CMT.CONTAINER_TYPE_MST_ID, CONT.FCL_CURRENT_RATE, CONT.FCL_REQ_RATE, " + "CURRS.CURRENCY_ID BASE_CURRENCY_ID,CURRS.CURRENCY_MST_PK BASE_CURRENCY_FK " + "FROM OPERATOR_MST_TBL OPR, " + "RFQ_TRN_SEA_FCL_LCL RFQTRN,  ";
					strSQL = strSQL + "RFQ_TRN_SEA_CONT_DTL CONT,  " + "CONTAINER_TYPE_MST_TBL CMT, " + "RFQ_MAIN_SEA_TBL RFQHDR,CURRENCY_TYPE_MST_TBL CURR,CURRENCY_TYPE_MST_TBL  CURRS  " + "WHERE RFQHDR.RFQ_MAIN_SEA_PK = RFQTRN.RFQ_MAIN_SEA_FK " + "AND OPR.OPERATOR_MST_PK = RFQHDR.OPERATOR_MST_FK " + "AND CONT.RFQ_TRN_SEA_FK = RFQTRN.RFQ_TRN_SEA_PK" + "AND CURR.CURRENCY_MST_PK = RFQTRN.CURRENCY_MST_FK " + "AND CMT.CONTAINER_TYPE_MST_PK = CONT.CONTAINER_TYPE_MST_FK " + "AND CURRS.CURRENCY_MST_PK(+) = RFQHDR.BASE_CURRENCY_FK" + "AND RFQHDR.RFQ_MAIN_SEA_PK = " + nRFQPk;
				} else {
					strSQL = "SELECT OPR.OPERATOR_MST_PK,OPR.OPERATOR_ID,OPR.OPERATOR_NAME, " + "RFQHDR.RFQ_REF_NO,RFQHDR.RFQ_DATE, " + "RFQHDR.CARGO_TYPE, RFQHDR.COMMODITY_MST_FK, RFQHDR.VALID_FROM, " + "RFQHDR.VALID_TO,RFQHDR.VERSION_NO, " + "RFQTRN.PORT_MST_POL_FK,RFQTRN.PORT_MST_POD_FK,RFQTRN.CHECK_FOR_ALL_IN_RT, " + "RFQTRN.FREIGHT_ELEMENT_MST_FK,RFQTRN.CURRENCY_MST_FK,RFQTRN.CHARGE_BASIS,CURR.CURRENCY_ID, " + "TO_CHAR(RFQTRN.VALID_FROM,'" + dateFormat + "') AS P_VALID_FROM, " + "TO_CHAR(RFQTRN.VALID_TO,'" + dateFormat + "') AS P_VALID_TO, " + "RFQTRN.LCL_BASIS,RFQTRN.LCL_CURRENT_MIN_RATE," + "RFQTRN.LCL_CURRENT_RATE,RFQTRN.LCL_REQUEST_MIN_RATE," + "RFQTRN.LCL_REQUEST_RATE,DMT.DIMENTION_ID, " + "CURRS.CURRENCY_ID BASE_CURRENCY_ID,CURRS.CURRENCY_MST_PK BASE_CURRENCY_FK " + "FROM OPERATOR_MST_TBL OPR, " + "RFQ_TRN_SEA_FCL_LCL RFQTRN,RFQ_MAIN_SEA_TBL RFQHDR, " + "CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL DMT,CURRENCY_TYPE_MST_TBL  CURRS " + "WHERE RFQHDR.RFQ_MAIN_SEA_PK = RFQTRN.RFQ_MAIN_SEA_FK " + "AND OPR.OPERATOR_MST_PK = RFQHDR.OPERATOR_MST_FK " + "AND CURR.CURRENCY_MST_PK = RFQTRN.CURRENCY_MST_FK " + "AND DMT.DIMENTION_UNIT_MST_PK = RFQTRN.LCL_BASIS " + "AND CURRS.CURRENCY_MST_PK(+) = RFQHDR.BASE_CURRENCY_FK" + "AND RFQHDR.RFQ_MAIN_SEA_PK =" + nRFQPk;
				}
				return (new WorkFlow()).GetDataSet(strSQL);
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region " Fetch Max RFQ No."
		public string FetchRFQNo(string strRFQNo)
		{
			try {
				string strSQL = null;
				strSQL = "SELECT NVL(MAX(T.RFQ_REF_NO),0) FROM RFQ_MAIN_SEA_TBL T " + "WHERE T.RFQ_REF_NO LIKE '" + strRFQNo + "/%' " + "ORDER BY T.RFQ_REF_NO";
				return (new WorkFlow()).ExecuteScaler(strSQL);
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Contract"
		public DataSet FetchContract(long OperatorFk, Int16 CargoType, long CommodityGrpFk, string POLPk, string PODPk, string Containers, string RFQDate)
		{
			try {
				System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
				string Ports = null;
				Int16 i = default(Int16);
				Array arrPolPk = null;
				Array arrPodPk = null;
				arrPolPk = POLPk.Split(',');
				arrPodPk = PODPk.Split(',');
				//Making condition as the record should have only selected POL and POD
				//POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
				//is the selected sector.
				for (i = 0; i <= arrPolPk.Length - 1; i++) {
					if (string.IsNullOrEmpty(Ports)) {
						Ports = " (CONTTRN.PORT_MST_POL_FK =" + arrPolPk.GetValue(i) + " AND CONTTRN.PORT_MST_POD_FK =" + arrPodPk.GetValue(i) + ")";
					} else {
						Ports += " OR (CONTTRN.PORT_MST_POL_FK =" + arrPolPk.GetValue(i) + " AND CONTTRN.PORT_MST_POD_FK =" + arrPodPk.GetValue(i) + ")";
					}
				}
				if (Containers.Trim().Length <= 0) {
					Containers = "SELECT CMT.CONTAINER_TYPE_MST_ID FROM CONTAINER_TYPE_MST_TBL CMT " + "";
				}
				if (CargoType == 1) {
					strSQL.Append("SELECT CONTTRN.PORT_MST_POL_FK,");
					strSQL.Append("       CONTTRN.PORT_MST_POD_FK,");
					strSQL.Append("       CONTTRN.FREIGHT_ELEMENT_MST_FK,");
					strSQL.Append("       CMT.CONTAINER_TYPE_MST_ID,");
					strSQL.Append("       CONT.FCL_APP_RATE *");
					strSQL.Append("       GET_EX_RATE(CONTTRN.CURRENCY_MST_FK,");
					strSQL.Append("                   CURR1.CURRENCY_MST_PK,");
					strSQL.Append("                   CONTHDR.CONTRACT_DATE) AS FCL_CURRENT_RATE,");
					strSQL.Append("       CURR1.CURRENCY_ID");
					strSQL.Append("  FROM OPERATOR_MST_TBL OPR,");
					strSQL.Append("       CONT_TRN_SEA_FCL_LCL CONTTRN,");
					strSQL.Append("       CONT_TRN_SEA_FCL_RATES CONT,");
					strSQL.Append("       CONTAINER_TYPE_MST_TBL CMT,");
					strSQL.Append("       CONT_MAIN_SEA_TBL CONTHDR,");
					strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR,");
					strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR1");
					strSQL.Append(" WHERE CONTHDR.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK");
					strSQL.Append("   AND OPR.OPERATOR_MST_PK = CONTHDR.OPERATOR_MST_FK");
					strSQL.Append("   AND CONTTRN.CONT_TRN_SEA_PK = CONT.CONT_TRN_SEA_FK");
					strSQL.Append("   AND CURR.CURRENCY_MST_PK = CONTTRN.CURRENCY_MST_FK");
					strSQL.Append("   AND CURR1.CURRENCY_MST_PK =" + HttpContext.Current.Session["CURRENCY_MST_PK"]);
					strSQL.Append("   AND CMT.CONTAINER_TYPE_MST_PK = CONT.CONTAINER_TYPE_MST_FK");
					strSQL.Append("   AND CONTHDR.ACTIVE =1");
					strSQL.Append("   AND CONTHDR.CONT_APPROVED = 1");
					//Operator Fk
					strSQL.Append("   AND CONTHDR.OPERATOR_MST_FK = " + OperatorFk);
					//Cargo Type
					strSQL.Append("   AND CONTHDR.CARGO_TYPE = " + CargoType);
					//Commodity Type Fk
					strSQL.Append("   AND CONTHDR.COMMODITY_GROUP_FK = " + CommodityGrpFk);
					//Ports
					strSQL.Append("   AND (" + Ports + ")");
					//Containers
					strSQL.Append("   AND CMT.CONTAINER_TYPE_MST_ID IN (" + Containers + ")");
					//RFQDate
					strSQL.Append("   AND TO_DATE('" + RFQDate + "', '" + dateFormat + "') BETWEEN CONTHDR.VALID_FROM AND");
					strSQL.Append("       NVL(CONTHDR.VALID_TO, NULL_DATE_FORMAT)");
				} else if (CargoType == 2) {
					strSQL.Append("SELECT CONTTRN.PORT_MST_POL_FK,");
					strSQL.Append("       CONTTRN.PORT_MST_POD_FK,");
					strSQL.Append("       CONTTRN.FREIGHT_ELEMENT_MST_FK,");
					strSQL.Append("       CONTTRN.LCL_APPROVED_RATE *");
					strSQL.Append("       GET_EX_RATE(CONTTRN.CURRENCY_MST_FK,");
					strSQL.Append("                   CURR1.CURRENCY_MST_PK,");
					strSQL.Append("                   CONTHDR.CONTRACT_DATE) AS LCL_CURRENT_RATE,");
					strSQL.Append("      CURR1.CURRENCY_ID,");
					strSQL.Append("      CONTTRN.LCL_BASIS,");
					strSQL.Append("      UOM.DIMENTION_ID,");
					strSQL.Append("       CONTTRN.LCL_APPROVED_MIN_RATE *");
					strSQL.Append("       GET_EX_RATE(CONTTRN.CURRENCY_MST_FK,");
					strSQL.Append("                   CURR1.CURRENCY_MST_PK,");
					strSQL.Append("                   CONTHDR.CONTRACT_DATE) AS LCL_CURRENT_MIN_RATE");
					strSQL.Append("  FROM OPERATOR_MST_TBL OPR,");
					strSQL.Append("       CONT_TRN_SEA_FCL_LCL CONTTRN,");
					strSQL.Append("       CONT_MAIN_SEA_TBL CONTHDR,");
					strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR,");
					//strSQL.Append("       CORPORATE_MST_TBL CORP,")
					strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR1,");
					strSQL.Append("       DIMENTION_UNIT_MST_TBL  UOM ");
					strSQL.Append(" WHERE CONTHDR.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK");
					strSQL.Append("   AND OPR.OPERATOR_MST_PK = CONTHDR.OPERATOR_MST_FK");
					strSQL.Append("   AND CURR.CURRENCY_MST_PK = CONTTRN.CURRENCY_MST_FK");
					strSQL.Append("   AND CURR1.CURRENCY_MST_PK =" + HttpContext.Current.Session["CURRENCY_MST_PK"]);
					strSQL.Append("   AND CONTTRN.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK");
					strSQL.Append("   AND CONTHDR.ACTIVE =1");
					strSQL.Append("   AND CONTHDR.CONT_APPROVED = 1");

					strSQL.Append("   AND CONTHDR.OPERATOR_MST_FK = " + OperatorFk);
					//Cargo Type
					strSQL.Append("   AND CONTHDR.CARGO_TYPE = " + CargoType);
					//Commodity Type Fk
					strSQL.Append("   AND CONTHDR.COMMODITY_GROUP_FK = " + CommodityGrpFk);
					//Ports
					strSQL.Append("   AND (" + Ports + ")");
					//RFQDate
					strSQL.Append("   AND TO_DATE('" + RFQDate + "', '" + dateFormat + "') BETWEEN CONTHDR.VALID_FROM AND");
					strSQL.Append("       NVL(CONTHDR.VALID_TO, NULL_DATE_FORMAT)");
				}
				return (new WorkFlow()).GetDataSet(strSQL.ToString());
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "FetchBasisPk"
		public int fetchBasisPk(int BasisID)
		{
			try {
				string sql = null;
				sql = "select dumt.dimention_unit_mst_pk from dimention_unit_mst_tbl dumt where dumt.dimention_id = '" + BasisID + "'";
				WorkFlow objWF = new WorkFlow();
				return Convert.ToInt32(objWF.ExecuteScaler(sql));
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetching Port Name and Assigning"
		public string GetPolName(string PolPK)
		{
			try {
				string strSQL = null;
				WorkFlow objWF = new WorkFlow();
				strSQL = "  SELECT POL.PORT_NAME FROM PORT_MST_TBL POL WHERE POL.PORT_MST_PK= " + PolPK;
				return (objWF.ExecuteScaler(strSQL));
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public string GetPodName(string PolPK)
		{
			try {
				string strSQL = null;
				WorkFlow objWF = new WorkFlow();
				strSQL = "  SELECT POL.PORT_NAME FROM PORT_MST_TBL POL WHERE POL.PORT_MST_PK= " + PolPK;
				return (objWF.ExecuteScaler(strSQL));
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
	}
}



