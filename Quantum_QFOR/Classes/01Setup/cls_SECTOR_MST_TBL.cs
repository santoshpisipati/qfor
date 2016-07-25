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
using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
namespace Quantum_QFOR
{
    public class cls_SECTOR_MST_TBL : CommonFeatures
	{

		#region "Fetch All Function"
		public DataSet FetchAll(Int64 SectorPK = 0, string SectorID = "", string sectorDef = "", string FromPortID = "", string ToPortID = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false,
		string strTradeCode = "", int ActiveFlag = -1, int intBusType = 0, int intUser = 0, Int32 flag = 0)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			if (flag == 0) {
				strCondition += " AND 1=2";
			}
			if (SectorPK > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and SECTMST.SECTOR_MST_PK like '%" + SectorPK + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and SECTMST.SECTOR_MST_PK like '" + SectorPK + "%'";
				}
			}
			if (SectorID.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(SECTMST.SECTOR_ID) like '%" + SectorID.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(SECTMST.SECTOR_ID) like '" + SectorID.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (sectorDef.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(SECTMST.SECTOR_DESC) like '%" + sectorDef.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(SECTMST.SECTOR_DESC) like '" + sectorDef.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (FromPortID.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " AND UPPER(FRMPRT.PORT_ID) LIKE '%" + FromPortID.Trim().ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " AND UPPER(FRMPRT.PORT_ID) LIKE '" + FromPortID.Trim().ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (ToPortID.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " AND UPPER(TOPRT.PORT_ID) LIKE '%" + ToPortID.Trim().ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " AND UPPER(TOPRT.PORT_ID) LIKE '" + ToPortID.Trim().ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (strTradeCode.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " AND UPPER(TRADE_MST.TRADE_CODE) LIKE '%" + strTradeCode.Trim().ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " AND UPPER(TRADE_MST.TRADE_CODE) LIKE '" + strTradeCode.Trim().ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (ActiveFlag == 1) {
				strCondition += " AND SECTMST.ACTIVE = 1 ";
			} else {
				strCondition += "";
			}
			if (intBusType == 2 & intUser == 3) {
				strCondition += " AND SECTMST.BUSINESS_TYPE IN (2) ";
			} else if (intBusType == 1 & intUser == 3) {
				strCondition += " AND SECTMST.BUSINESS_TYPE IN (1) ";
			} else if (intBusType == 2 & intUser == 2) {
				strCondition += " AND SECTMST.BUSINESS_TYPE IN (2) ";
			} else if (intBusType == 1 & intUser == 1) {
				strCondition += " AND SECTMST.BUSINESS_TYPE IN (1) ";
			} else if (intBusType > 0) {
				strCondition += " AND SECTMST.BUSINESS_TYPE = " + intBusType + " ";
			}
			strSQL = "SELECT Count(*) from SECTOR_MST_TBL  SECTMST,";
			strSQL = strSQL + "  PORT_MST_TBL FRMPRT,";
			strSQL = strSQL + "  PORT_MST_TBL TOPRT ,TRADE_MST_TBL TRADE_MST";
			strSQL = strSQL + "  WHERE ";
			strSQL = strSQL + "  SECTMST.FROM_PORT_FK = FRMPRT.PORT_MST_PK";
			strSQL = strSQL + "  AND SECTMST.TO_PORT_FK = TOPRT.PORT_MST_PK AND TRADE_MST.ACTIVE_FLAG = 1 AND TRADE_MST.TRADE_MST_PK =SECTMST.TRADE_MST_FK ";
			strSQL += strCondition;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;
			strSQL = " select * from (";
			strSQL += "SELECT ROWNUM SR_NO, q.* FROM ";
			strSQL = strSQL + "(SELECT ";
			strSQL = strSQL + " \tSECTMST.SECTOR_MST_PK SECTOR_MST_PK,";
			strSQL = strSQL + " \tDECODE(SECTMST.ACTIVE ,'0','false','1','true') ACTIVE_FLAG,";
			strSQL = strSQL + " \tSECTMST.SECTOR_ID SECTOR_ID,";
			strSQL = strSQL + " \tSECTMST.SECTOR_DESC SECTOR_DESC,";
			strSQL = strSQL + "    FRMPRT.PORT_ID FRM_PORT_ID,";
			strSQL = strSQL + "  TOPRT.PORT_ID TO_PORT_ID,";
			strSQL = strSQL + "  TRADE_MST.TRADE_CODE TRADE_CODE,SECTMST.BUSINESS_TYPE";
			strSQL = strSQL + "  FROM SECTOR_MST_TBL  SECTMST, ";
			strSQL = strSQL + "  PORT_MST_TBL FRMPRT,";
			strSQL = strSQL + "  PORT_MST_TBL TOPRT,TRADE_MST_TBL TRADE_MST";
			strSQL = strSQL + "  WHERE ";
			strSQL = strSQL + "  SECTMST.FROM_PORT_FK = FRMPRT.PORT_MST_PK";
			strSQL = strSQL + "  AND SECTMST.TO_PORT_FK = TOPRT.PORT_MST_PK AND TRADE_MST.ACTIVE_FLAG = 1 AND TRADE_MST.TRADE_MST_PK =SECTMST.TRADE_MST_FK ";
			strSQL += strCondition;
			if (!strColumnName.Equals("SR_NO")) {
				strSQL += "order by " + strColumnName;
			}
			if (!blnSortAscending & !strColumnName.Equals("SR_NO")) {
				strSQL += " DESC";
			}
			strSQL += " )q ) WHERE SR_NO  Between " + start + " and " + last;
			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			} finally {
			}
		}
		#endregion

		#region "Save Function"

		public ArrayList Save(DataSet M_DataSet, Int64 intTrade, int businessType)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			try {
				DataTable dttbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;
				dttbl = M_DataSet.Tables[0];
				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".SECTOR_MST_TBL_PKG.SECTOR_MST_TBL_INS";
				var _with2 = _with1.Parameters;
				insCommand.Parameters.Add("SECTOR_ID_IN", OracleDbType.Varchar2, 20, "SECTOR_ID").Direction = ParameterDirection.Input;
				insCommand.Parameters["SECTOR_ID_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("SECTOR_DESC_IN", OracleDbType.Varchar2, 50, "SECTOR_DESC").Direction = ParameterDirection.Input;
				insCommand.Parameters["SECTOR_DESC_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				insCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("FROM_PORT_FK_IN", OracleDbType.Int32, 10, "FROM_PORT_PK").Direction = ParameterDirection.Input;
				insCommand.Parameters["FROM_PORT_FK_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("TO_PORT_FK_IN", OracleDbType.Int32, 10, "PORT_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["TO_PORT_FK_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("TRADE_MST_FK_IN", intTrade).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "SECTOR_MST_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with3 = updCommand;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".SECTOR_MST_TBL_PKG.SECTOR_MST_TBL_UPD";
				var _with4 = _with3.Parameters;
				updCommand.Parameters.Add("SECTOR_MST_PK_IN", OracleDbType.Int32, 10, "SECTOR_MST_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["SECTOR_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("SECTOR_ID_IN", OracleDbType.Varchar2, 20, "SECTOR_ID").Direction = ParameterDirection.Input;
				updCommand.Parameters["SECTOR_ID_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("SECTOR_DESC_IN", OracleDbType.Varchar2, 50, "SECTOR_DESC").Direction = ParameterDirection.Input;
				updCommand.Parameters["SECTOR_DESC_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("FROM_PORT_FK_IN", OracleDbType.Int32, 10, "FROM_PORT_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["FROM_PORT_FK_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("TO_PORT_FK_IN", OracleDbType.Int32, 10, "TO_PORT_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["TO_PORT_FK_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
				updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("TRADE_MST_FK_IN", intTrade).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "SECTOR_MST_PK").Direction = ParameterDirection.Output;
				updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with5 = objWK.MyDataAdapter;
				_with5.InsertCommand = insCommand;
				_with5.InsertCommand.Transaction = TRAN;
				_with5.UpdateCommand = updCommand;
				_with5.UpdateCommand.Transaction = TRAN;
				RecAfct = _with5.Update(M_DataSet);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion

		public DataSet FillCombo(int businessType)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			strSQL = " SELECT T.TRADE_MST_PK,T.TRADE_CODE FROM TRADE_MST_TBL T WHERE T.ACTIVE_FLAG = 1 AND T.BUSINESS_TYPE =" + businessType + " ORDER BY T.TRADE_CODE";
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

		public DataSet FillGridOnAdd(Int64 intTradeNo)
		{
			string strSQL = "";
			WorkFlow objWF = new WorkFlow();
			strSQL = strSQL + " SELECT ROWNUM SR_NO, Q.* FROM (";
			strSQL = strSQL + "    SELECT  DISTINCT  0 SECTOR_MST_PK, ";
			strSQL = strSQL + "                'true' ACTIVE_FLAG,  ";
			strSQL = strSQL + "                 C.PORT_ID FROM_PORT_CODE, ";
			strSQL = strSQL + "                 A.PORT_MST_FK FROM_PORT_PK , ";
			strSQL = strSQL + "                 D.PORT_ID, B.PORT_MST_FK ,";
			strSQL = strSQL + "                 Concat( substr(C.port_id,case when Length(C.port_id) = 3 then 1 else 3 end,3), substr(D.port_id,case when Length(D.port_id) = 3 then 1 else 3 end,3) ) SECTOR_ID,'' SECTOR_DESC,0 VERSION_NO,'false' SelectAll ";
			strSQL = strSQL + "        FROM  TRADE_MST_TRN A, ";
			strSQL = strSQL + "              TRADE_MST_TRN B, ";
			strSQL = strSQL + "              PORT_MST_TBL C, ";
			strSQL = strSQL + "              PORT_MST_TBL D,";
			strSQL = strSQL + "              TRADE_MST_TBL TR_MST";
			strSQL = strSQL + "        WHERE     A.PORT_MST_FK <> B.PORT_MST_FK";
			strSQL = strSQL + "              AND A.TRADE_MST_FK = " + intTradeNo;
			strSQL = strSQL + "              AND B.TRADE_MST_FK = " + intTradeNo;
			strSQL = strSQL + "              AND C.PORT_MST_PK = A.PORT_MST_FK";
			strSQL = strSQL + "              AND D.PORT_MST_PK = B.PORT_MST_FK";
			strSQL = strSQL + "              AND TR_MST.TRADE_MST_PK =" + intTradeNo;
			strSQL = strSQL + "              AND NOT EXISTS (SELECT 1 FROM  SECTOR_MST_TBL SM";
			strSQL = strSQL + "                            WHERE SM.FROM_PORT_FK=C.PORT_MST_PK";
			strSQL = strSQL + "                              AND SM.TO_PORT_FK=D.PORT_MST_PK )";
			strSQL = strSQL + "        ORDER BY C.PORT_ID, D.PORT_ID)Q";
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

		public DataSet FillGridOnEdit(Int64 intSectorPk)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			strSQL = strSQL + " SELECT ROWNUM SR_NO, Q.* FROM(";
			strSQL = strSQL + " SELECT SC_MST.SECTOR_MST_PK, ";
			strSQL = strSQL + " DECODE(SC_MST.ACTIVE ,'0','false','1','true') ACTIVE_FLAG, ";
			strSQL = strSQL + " FROMPORT.PORT_ID FROM_PORT_CODE, ";
			strSQL = strSQL + " SC_MST.FROM_PORT_FK FROM_PORT_PK,";
			strSQL = strSQL + " TOPORT.PORT_ID, SC_MST.TO_PORT_FK,";
			strSQL = strSQL + " SC_MST.SECTOR_ID,";
			strSQL = strSQL + " SC_MST.SECTOR_DESC, SC_MST.VERSION_NO,'false' SelectAll";
			strSQL = strSQL + "  FROM PORT_MST_TBL FROMPORT, ";
			strSQL = strSQL + "  PORT_MST_TBL TOPORT, ";
			strSQL = strSQL + " SECTOR_MST_TBL SC_MST";
			strSQL = strSQL + " WHERE SC_MST.SECTOR_MST_PK = " + intSectorPk;
			strSQL = strSQL + " AND SC_MST.FROM_PORT_FK = FROMPORT.PORT_MST_PK ";
			strSQL = strSQL + " AND SC_MST.TO_PORT_FK = TOPORT.PORT_MST_PK)Q";
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

		public string GetPrimaryKey(string strDesc, int businessType)
		{
			WorkFlow objWF = new WorkFlow();
			string strReturn = "";
			string strSQL = "";
			strSQL = " SELECT C.TRADE_MST_PK FROM TRADE_MST_TBL C WHERE C.TRADE_CODE LIKE '%" + strDesc + "%' and c.business_type=" + businessType + " ";
			objWF.MyDataReader = objWF.GetDataReader(strSQL);
			try {
				while (objWF.MyDataReader.Read()) {
					strReturn = objWF.MyDataReader[0].ToString();
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				string s = ex.Message;
			} finally {
				objWF.MyDataReader.Close();
				objWF.MyConnection.Close();
			}
			return ((string.IsNullOrEmpty(strReturn) ? "0" : strReturn));
		}

		public DataSet FillGridOnCancel(Int64 intTradeNo)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			strSQL = strSQL + " SELECT ROWNUM SR_NO, Q2.* FROM(";
			strSQL = strSQL + "    SELECT Q.* FROM( ";
			strSQL = strSQL + "    SELECT  DISTINCT  0 SECTOR_MST_PK, ";
			strSQL = strSQL + "                'true' ACTIVE_FLAG,  ";
			strSQL = strSQL + "                 C.PORT_ID FROM_PORT_CODE, ";
			strSQL = strSQL + "                 A.PORT_MST_FK FROM_PORT_PK , ";
			strSQL = strSQL + "                 D.PORT_ID, B.PORT_MST_FK ,";
			strSQL = strSQL + "                 '' SECTOR_ID,'' SECTOR_DESC,0 VERSION_NO , 'false' SelectAll ";
			strSQL = strSQL + "         FROM  TRADE_MST_TRN A, ";
			strSQL = strSQL + "               TRADE_MST_TRN B, ";
			strSQL = strSQL + "               PORT_MST_TBL C, ";
			strSQL = strSQL + "               PORT_MST_TBL D,";
			strSQL = strSQL + "               TRADE_MST_TBL TR_MST,";
			strSQL = strSQL + "               SECTOR_MST_TBL SM1";
			strSQL = strSQL + "         WHERE     A.PORT_MST_FK <> B.PORT_MST_FK";
			strSQL = strSQL + "               AND A.TRADE_MST_FK =" + intTradeNo;
			strSQL = strSQL + "               AND B.TRADE_MST_FK =" + intTradeNo;
			strSQL = strSQL + "               AND C.PORT_MST_PK = A.PORT_MST_FK";
			strSQL = strSQL + "               AND D.PORT_MST_PK = B.PORT_MST_FK";
			strSQL = strSQL + "               AND TR_MST.TRADE_MST_PK =" + intTradeNo;
			strSQL = strSQL + "               AND NOT EXISTS (SELECT 1 FROM  SECTOR_MST_TBL SM";
			strSQL = strSQL + "                             WHERE SM.FROM_PORT_FK=C.PORT_MST_PK";
			strSQL = strSQL + "                               AND SM.TO_PORT_FK=D.PORT_MST_PK";
			strSQL = strSQL + "                               AND SM.TRADE_MST_FK = " + intTradeNo + ") ";
			strSQL = strSQL + "         ORDER BY C.PORT_ID, D.PORT_ID)Q";
			strSQL = strSQL + "  )Q2";
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

		public DataSet FillGridOnSave(Int64 intTradeNo)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			strSQL = strSQL + " SELECT ROWNUM SR_NO, Q2.* FROM(";
			strSQL = strSQL + "         SELECT Q1.* FROM( ";
			strSQL = strSQL + "         SELECT  DISTINCT  SM1.SECTOR_MST_PK, ";
			strSQL = strSQL + "                 DECODE(SM1.ACTIVE,'0','false','1','true')  ACTIVE_FLAG,  ";
			strSQL = strSQL + "                  C.PORT_ID FROM_PORT_CODE, ";
			strSQL = strSQL + "                  A.PORT_MST_FK FROM_PORT_PK , ";
			strSQL = strSQL + "                  D.PORT_ID, SM1.TO_PORT_FK ,";
			strSQL = strSQL + "                  SM1.SECTOR_ID SECTOR_ID,SM1.SECTOR_DESC SECTOR_DESC,SM1.VERSION_NO, 'false' SelectAll ";
			strSQL = strSQL + "         FROM  TRADE_MST_TRN A, ";
			strSQL = strSQL + "               TRADE_MST_TRN B, ";
			strSQL = strSQL + "               PORT_MST_TBL C, ";
			strSQL = strSQL + "               PORT_MST_TBL D,";
			strSQL = strSQL + "               TRADE_MST_TBL TR_MST,";
			strSQL = strSQL + "               SECTOR_MST_TBL SM1";
			strSQL = strSQL + "         WHERE  A.PORT_MST_FK <> B.PORT_MST_FK";
			strSQL = strSQL + "               AND A.TRADE_MST_FK = " + intTradeNo;
			strSQL = strSQL + "               AND B.TRADE_MST_FK = " + intTradeNo;
			strSQL = strSQL + "               AND C.PORT_MST_PK = A.PORT_MST_FK";
			strSQL = strSQL + "               AND D.PORT_MST_PK = B.PORT_MST_FK";
			strSQL = strSQL + "               AND TR_MST.TRADE_MST_PK =" + intTradeNo;
			strSQL = strSQL + "               AND SM1.FROM_PORT_FK = C.PORT_MST_PK";
			strSQL = strSQL + "               AND SM1.TO_PORT_FK = D.PORT_MST_PK";
			strSQL = strSQL + "   AND EXISTS (SELECT 1 FROM  SECTOR_MST_TBL SM";
			strSQL = strSQL + "                      WHERE SM.FROM_PORT_FK=C.PORT_MST_PK";
			strSQL = strSQL + "                               AND SM.TO_PORT_FK=D.PORT_MST_PK";
			strSQL = strSQL + "                               AND SM.TRADE_MST_FK =" + intTradeNo + " )   ";
			strSQL = strSQL + "   ORDER BY C.PORT_ID, D.PORT_ID)Q1";
			strSQL = strSQL + "  )Q2";
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
	}
}
