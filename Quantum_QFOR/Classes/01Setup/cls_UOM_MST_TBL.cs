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
namespace Quantum_QFOR
{
    public class cls_UOM_MST_TBL : CommonFeatures
	{
		#region "Private Variables"
			#endregion
		private DataSet M_DataSet;

		#region "Property"
		public DataSet MyDataSet {
			get { return M_DataSet; }
		}
		#endregion

		#region "Constructor"
		public cls_UOM_MST_TBL()
		{
			string strSQL = null;
			strSQL = "SELECT UOM.DIMENTION_UNIT_MST_PK,UOM.DIMENTION_ID FROM DIMENTION_UNIT_MST_TBL UOM " + "WHERE UOM.ACTIVE = 1 AND UOM.CARGO_TYPE=1";
			try {
				M_DataSet = (new WorkFlow()).GetDataSet(strSQL);
			//Manjunath  PTS ID:Sep-02  15/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#endregion
		#region "Basis" 'introduced by purnanand reason-basis should apply for FCL and LCL both.PTS RTA-MAY-007
		public DataSet FetchBasis(string CargoType)
		{
			string strSQL = null;
			WorkFlow ObjWk = new WorkFlow();

			string strCondition = null;
			strCondition = strCondition + " AND UOM.CARGO_TYPE=  " + CargoType ;

			strSQL = "SELECT UOM.DIMENTION_UNIT_MST_PK,UOM.DIMENTION_ID FROM DIMENTION_UNIT_MST_TBL UOM " + "WHERE UOM.ACTIVE = 1  ";
			strSQL += strCondition;
			try {
				return ObjWk.GetDataSet(strSQL);
			//Manjunath  PTS ID:Sep-02  15/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#endregion
		#region "Fetch All Function"
		public DataSet FetchAll(string UnitType = "", string UnitDesc = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, Int16 IsActive = 0, bool blnSortAscending = false, Int32 flag = 0)
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
			if (UnitDesc.Trim().Length > 0 & SearchType == "C") {
				strCondition += " AND UPPER(dimention_id) LIKE '%" + UnitDesc.ToUpper().Replace("'", "''") + "%'" ;
			} else if (UnitDesc.Trim().Length > 0 & SearchType == "S") {
				strCondition += " AND UPPER(dimention_id) LIKE '" + UnitDesc.ToUpper().Replace("'", "''") + "%'" ;
			}

			if (UnitType.Trim().Length > 0) {
				if (UnitType.Trim().Length == 6) {
					strCondition += " AND dimention_type = 1";
				} else if (UnitType.Trim().Length == 3) {
				} else {
					strCondition += " AND dimention_type = 2";
				}
				//strCondition &= vbCrLf & " AND UPPER(dimention_type) = '" & UnitType.ToUpper.Replace("'", "''") & "'" & vbCrLf
			}
			if (IsActive == 1) {
				strCondition += " AND active = 1";
			}
			strSQL = " SELECT Count(*) ";
			strSQL += " FROM dimention_unit_mst_tbl ";
			strSQL += " WHERE(1 = 1) ";
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



			strSQL = "select * from ( ";
			strSQL += "select ROWNUM SR_NO,q.* FROM ( ";
			strSQL += "select dimention_unit_mst_pk, ";
			strSQL += "active, ";
			strSQL += "dimention_type, ";
			strSQL += "decode(dimention_type,1,'Weight',2,'Measurement')dimention_type_name, ";
			strSQL += "dimention_id, ";
			strSQL += "version_no ";
			strSQL += "from dimention_unit_mst_tbl ";
			strSQL += "where (1=1)  ";

			strSQL += strCondition;

			if (!strColumnName.Equals("SR_NO")) {
				strSQL += "order by " + strColumnName;
			}

			if (!blnSortAscending & !strColumnName.Equals("SR_NO")) {
				strSQL += " DESC";
			}


			strSQL += ")q ";
			strSQL += ") where sr_no between " + start + " and " + last;

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
		public object FetchDimension(Int16 dimension_type)
		{
			string sql = null;
			sql = "  SELECT 0 dimention_unit_mst_pk, '' DIMENTION_ID FROM dual Union";
			sql += " select D.dimention_unit_mst_pk, D.DIMENTION_ID ";
			sql += " from dimention_unit_mst_tbl D ";
			sql += " where d.dimention_type=" + dimension_type;
			sql += " order by DIMENTION_ID";

			WorkFlow objWF = new WorkFlow();
			OracleDataReader objDR = null;
			try {
				return objWF.GetDataSet(sql);
			} catch (OracleException dbExp) {
				ErrorMessage = dbExp.Message;
				throw dbExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Save Function"

		public ArrayList Save(DataSet M_DataSet, bool Import = false)
		{
			//sivachandran 05Jun08 Imp-Exp-Wiz 16May08
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
				DataTable DtTbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;
				DtTbl = M_DataSet.Tables[0];

				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".DIMENTION_UNIT_MST_TBL_PKG.DIMENTION_UNIT_MST_TBL_INS";
				var _with2 = _with1.Parameters;
				insCommand.Parameters.Add("DIMENTION_TYPE_IN", OracleDbType.Int32, 1, "DIMENTION_TYPE").Direction = ParameterDirection.Input;
				insCommand.Parameters["DIMENTION_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("DIMENTION_ID_IN", OracleDbType.Varchar2, 20, "DIMENTION_ID").Direction = ParameterDirection.Input;
				insCommand.Parameters["DIMENTION_ID_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
				insCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CONTAINER_TYPE_MST_TBL_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;





				var _with3 = updCommand;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".DIMENTION_UNIT_MST_TBL_PKG.DIMENTION_UNIT_MST_TBL_UPD";
				var _with4 = _with3.Parameters;

				updCommand.Parameters.Add("DIMENTION_UNIT_MST_PK_IN", OracleDbType.Int32, 10, "DIMENTION_UNIT_MST_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["DIMENTION_UNIT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("DIMENTION_TYPE_IN", OracleDbType.Int32, 1, "DIMENTION_TYPE").Direction = ParameterDirection.Input;
				updCommand.Parameters["DIMENTION_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("DIMENTION_ID_IN", OracleDbType.Varchar2, 20, "DIMENTION_ID").Direction = ParameterDirection.Input;
				updCommand.Parameters["DIMENTION_ID_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
				updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                

				var _with5 = objWK.MyDataAdapter;
				_with5.InsertCommand = insCommand;
				_with5.InsertCommand.Transaction = TRAN;
				_with5.UpdateCommand = updCommand;
				_with5.UpdateCommand.Transaction = TRAN;
				RecAfct = _with5.Update(M_DataSet);

				if (arrMessage.Count > 0) {
					//This is if any error occurs then rollback..
					TRAN.Rollback();
					return arrMessage;
				} else {
					//This part executes only when no error in the execution..
					TRAN.Commit();
					//arrMessage.Add("All Data Saved Successfully") 'sivachandran 05Jun08 Imp-Exp-Wiz 16May08
					if (Import == false) {
						arrMessage.Add("All Data Saved Successfully");
					} else {
						arrMessage.Add("Data Imported Successfully");
					}
					//End
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

	}
}
