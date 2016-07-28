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
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_VATMaster : CommonFeatures
	{
        #region "Fetch Function"
        /// <summary>
        /// </summary>
        public Int32 Chk = 0;
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="contrypk">The contrypk.</param>
        /// <param name="vatcode">The vatcode.</param>
        /// <param name="chked">The chked.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ChkONLD">The CHK onld.</param>
        /// <returns></returns>
        public DataSet FetchALL(Int32 contrypk, string vatcode, Int32 chked, string SortColumn, string SortType, Int32 CurrentPage, Int32 TotalPage, Int32 ChkONLD = 0)
		{
			StringBuilder strsql = new StringBuilder();
			StringBuilder strqry = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strsort = null;
			try {
				if (SortColumn == "VATMAS") {
					strsort = " VATMASFLAG" + SortType + " ,CNTNAME " + SortType;
				} else if (SortColumn == "VATCODE") {
					strsort = " VATCODE " + SortType;
				} else {
					strsort = " VATPER " + SortType;
				}
				strsql.Append(" SELECT MAINQRY.* FROM ( SELECT ROWNUM SLNO,QRY.* FROM ( ");
				//strsql.Append(" SELECT VAT.VAT_MST_PK PK, VAT.VAT_MASTER_FK VATMAS, NULL CNTNAME, VAT.VAT_MASTER_FLAG  VATMASFLAG, ")
				//strsql.Append(" VAT.VAT_CODE VATCODE ,VAT.VAT_PERCENT VATPER ,VAT.SEL , VAT.VERSION FROM VAT_MST_TBL VAT WHERE VAT.VAT_MASTER_FLAG <= 2 UNION ")
				strsql.Append(" SELECT VAT.VAT_MST_PK PK,CNT.COUNTRY_MST_PK VATMAS, CNT.COUNTRY_NAME,VAT.VAT_MASTER_FLAG  VATMASFLAG, ");
				strsql.Append(" VAT.VAT_CODE VATCODE ,VAT.VAT_PERCENT VATPER ,VAT.SEL , VAT.VERSION  CNTNAME FROM VAT_MST_TBL VAT,COUNTRY_MST_TBL CNT WHERE  1=1 ");
				if (ChkONLD == 0) {
					strsql.Append(" AND 1=2  ");
				}

				if (contrypk > 0) {
					strsql.Append(" AND VAT.VAT_MASTER_FK=" + contrypk);
				}

				if (vatcode.Length > 0) {
					if (chked == 1) {
						strsql.Append(" AND UPPER(VAT.VAT_CODE) LIKE '" + vatcode.ToUpper().Replace("'", "''") + "%'" );
						// strsql.Append(vbCrLf & " AND UPPER(VAT.VAT_CODE) like '" & Replace(vatcode, "'", "''") & "%' ")
					} else {
						//strsql.Append(vbCrLf & " AND UPPER(VAT.VAT_CODE) like '%" & Replace(vatcode, "'", "''") & "%' ")
						strsql.Append(" AND UPPER(VAT.VAT_CODE) LIKE '%" + vatcode.ToUpper().Replace("'", "''") + "%'" );
					}
				}

				//strsql.Append(" AND VAT.VAT_MASTER_FK=CNT.COUNTRY_MST_PK  ORDER BY " & strsort & ")QRY ) MAINQRY ")        'Commnet by prasant
				strsql.Append(" AND VAT.VAT_MASTER_FK=CNT.COUNTRY_MST_PK  ORDER BY CNT.COUNTRY_NAME" + ")QRY ) MAINQRY ");
				TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strsql.ToString()));
				// Getting No of satisfying records.
				TotalPage = TotalRecords / RecordsPerPage;
				if (TotalRecords % RecordsPerPage != 0)
					TotalPage += 1;
				if (CurrentPage > TotalPage)
					CurrentPage = 1;
				if (TotalRecords == 0)
					CurrentPage = 0;
				last = CurrentPage * RecordsPerPage;
				start = (CurrentPage - 1) * RecordsPerPage + 1;
				strsql.Append("  where SLNO between " + start + " and " + last);
				return objWF.GetDataSet(strsql.ToString());

				//If objvat.Tables(0).Rows.Count = 0 Then
				//    strqry.Append(" SELECT 1 SLNO,0 PK ,null VATMAS,1 VATMASFLAG ,'' VATCODE,'' VATPER,'false' SEL , null VERSION ,NULL CNTNAME FROM DUAL UNION ")
				//    strqry.Append(" SELECT 2 SLNO,0 PK ,null  VATMAS,2 VATMASFLAG ,'' VATCODE,'' VATPER,'false' SEL , null VERSION ,NULL CNTNAME FROM DUAL ")
				//    objvat = objWF.GetDataSet(strqry.ToString())
				//    Chk = 1
				//End If
				//Return objvat
			//Manjunath  PTS ID:Sep-02  15/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetches all cont.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchAllCont()
		{
			StringBuilder strsql = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				strsql.Append(" SELECT CONT.COUNTRY_MST_PK,CONT.COUNTRY_NAME FROM COUNTRY_MST_TBL CONT WHERE CONT.ACTIVE_FLAG=1 order by CONT.COUNTRY_NAME");
				return objWF.GetDataSet(strsql.ToString());
			//Manjunath  PTS ID:Sep-02  15/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetches all conry.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchAllConry()
		{
			StringBuilder strsql = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				strsql.Append(" SELECT -1 COUNTRY_MST_PK, '' COUNTRY_NAME FROM DUAL ");
				strsql.Append(" UNION SELECT CONT.COUNTRY_MST_PK,CONT.COUNTRY_NAME FROM COUNTRY_MST_TBL CONT WHERE CONT.ACTIVE_FLAG=1  order by COUNTRY_NAME asc  ");
				return objWF.GetDataSet(strsql.ToString());
			//Manjunath  PTS ID:Sep-02  15/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="Import">if set to <c>true</c> [import].</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, bool Import = false)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			//Dim selectCommand As New OracleCommand          'Manjunath  PTS ID:Sep-02  15/09/2011
			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			arrMessage.Clear();
			try {
				DataTable dttbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;
				dttbl = M_DataSet.Tables[0];
				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".VAT_MST_TBL_PKG.VAT_MST_TBL_INS";
				var _with2 = _with1.Parameters;

				insCommand.Parameters.Add("VAT_MASTER_IN", OracleDbType.Int32, 3, "VATMAS").Direction = ParameterDirection.Input;
				insCommand.Parameters["VAT_MASTER_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("VAT_MASTER_FLAG_IN", OracleDbType.Int32, 3, "VATMASFLAG").Direction = ParameterDirection.Input;
				insCommand.Parameters["VAT_MASTER_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("VAT_CODE_IN", OracleDbType.Varchar2, 10, "VATCODE").Direction = ParameterDirection.Input;
				insCommand.Parameters["VAT_CODE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("VAT_PERCENT_IN", OracleDbType.BinaryFloat, 5, "VATPER").Direction = ParameterDirection.Input;
				insCommand.Parameters["VAT_PERCENT_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with3 = updCommand;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".VAT_MST_TBL_PKG.VAT_MST_TBL_UPD";
				var _with4 = _with3.Parameters;
				updCommand.Parameters.Add("VAT_MASTER_FK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["VAT_MASTER_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("VAT_MASTER_IN", OracleDbType.Int32, 3, "VATMAS").Direction = ParameterDirection.Input;
				updCommand.Parameters["VAT_MASTER_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("VAT_MASTER_FLAG_IN", OracleDbType.Int32, 3, "VATMASFLAG").Direction = ParameterDirection.Input;
				updCommand.Parameters["VAT_MASTER_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("VAT_CODE_IN", OracleDbType.Varchar2, 10, "VATCODE").Direction = ParameterDirection.Input;
				updCommand.Parameters["VAT_CODE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("VAT_PERCENT_IN", OracleDbType.BinaryFloat, 5, "VATPER").Direction = ParameterDirection.Input;
				updCommand.Parameters["VAT_PERCENT_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				updCommand.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
				//updCommand.Parameters.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 6, "VERSION").Direction = ParameterDirection.Input
				//updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current
				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
				//Finally             'Manjunath  PTS ID:Sep-02  15/09/2011
				//selectCommand.Connection.Close()
			}
		}
		#endregion
	}
}

