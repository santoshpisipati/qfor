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

    public class ClsTRADEContracts : CommonFeatures
	{

		#region " Fetch One "
		// This function will fetch only one record.
		// In case a tradePK is provided then the corresponding record will be fetched
		// in case a wrong tradePK is provided that will raise an exception
		// in case no tradePK is provided then a blank record will be returned by the DataSet
		public DataSet FetchOne(string tradePK = "")
		{
			bool NewRecord = true;
			if (tradePK.Trim().Length > 0)
				NewRecord = false;

			string strSQL = null;
			string strCondition = null;

			// Record Type Condition
			if (!NewRecord) {
				strCondition += " and TRADE_MST_PK = " + tradePK + " ";
				// one satisfying row in dataset
			} else {
				strCondition += " and 1 = 2 ";
			}

			strSQL = "              Select ";
			strSQL += "        TRADE_MST_PK,";
			strSQL += "        TRADE_CODE,";
			strSQL += "        TRADE_NAME,";
			strSQL += "        VERSION_NO,";
			strSQL += "        BUSINESS_TYPE,";
			strSQL += "        ACTIVE_FLAG ";
			strSQL += "    from ";
			strSQL += "        TRADE_MST_TBL ";
			strSQL += "        Where 1 = 1 " + strCondition;

			DataSet ds = null;
			WorkFlow objWF = new WorkFlow();
			try {
				ds = objWF.GetDataSet(strSQL);
				if (NewRecord == true) {
					ds.Tables[0].Rows.Add(ds.Tables[0].NewRow());
					// to return a new blank record
					ds.Tables[0].Rows[0]["TRADE_MST_PK"] = 0;
					ds.Tables[0].Rows[0]["ACTIVE_FLAG"] = 1;
				}
				return ds;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

		#region " Save Record "

		public ArrayList SaveRecord(DataSet DS, string PortPKs = "")
		{
			if (string.IsNullOrEmpty(PortPKs))
				PortPKs = "-";
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();

			string retVal = null;
			Int32 RecAfct = default(Int32);

			OracleCommand ICM = new OracleCommand();
			OracleCommand UCM = new OracleCommand();
			OracleCommand DCM = new OracleCommand();
			string INS_Proc = null;
			string DEL_Proc = null;
			string UPD_Proc = null;
			string UserName = objWK.MyUserName;
			INS_Proc = UserName + ".TRADE_MST_TBL_PKG.TRADE_MST_TBL_INS";
			DEL_Proc = UserName + ".TRADE_MST_TBL_PKG.TRADE_MST_TBL_DEL";
			UPD_Proc = UserName + ".TRADE_MST_TBL_PKG.TRADE_MST_TBL_UPD";

			try {
				var _with1 = ICM;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = INS_Proc;
				// TRADE_CODE
				_with1.Parameters.Add("TRADE_CODE_IN", OracleDbType.Varchar2, 20, "TRADE_CODE").Direction = ParameterDirection.Input;
				_with1.Parameters["TRADE_CODE_IN"].SourceVersion = DataRowVersion.Current;
				// TRADE_NAME
				_with1.Parameters.Add("TRADE_NAME_IN", OracleDbType.Varchar2, 50, "TRADE_NAME").Direction = ParameterDirection.Input;
				_with1.Parameters["TRADE_NAME_IN"].SourceVersion = DataRowVersion.Current;

				// ACTIVE_FLAG
				_with1.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				_with1.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				// BUSINESS_TYPE
				_with1.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
				_with1.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				// PORT_MST_FKs (Direct Value)
				_with1.Parameters.Add("PORT_MST_FK_IN", PortPKs).Direction = ParameterDirection.Input;

				// CREATED_BY_FK_IN
				_with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

				// CONFIG_MST_FK_IN
				_with1.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				// RETURN_VALUE ( OUTPUT PARAMETER )
				_with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				// DELETE COMMAND *******************************************************************
				var _with2 = DCM;
				_with2.Connection = objWK.MyConnection;
				_with2.CommandType = CommandType.StoredProcedure;
				_with2.CommandText = DEL_Proc;

				// TRADE_MST_PK
				_with2.Parameters.Add("TRADE_MST_PK_IN", OracleDbType.Int32, 10, "TRADE_MST_PK").Direction = ParameterDirection.Input;
				_with2.Parameters["TRADE_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

				// DELETED_BY_FK
				_with2.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

				// CONFIG_MST_FK_IN
				_with2.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				// VERSON_NO
				_with2.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				_with2.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				// RETURN VALUE
				_with2.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with2.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with3 = UCM;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = UPD_Proc;

				// TRADE_MST_PK
				_with3.Parameters.Add("TRADE_MST_PK_IN", OracleDbType.Int32, 10, "TRADE_MST_PK").Direction = ParameterDirection.Input;
				_with3.Parameters["TRADE_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

				// TRADE_CODE
				_with3.Parameters.Add("TRADE_CODE_IN", OracleDbType.Varchar2, 20, "TRADE_CODE").Direction = ParameterDirection.Input;
				_with3.Parameters["TRADE_CODE_IN"].SourceVersion = DataRowVersion.Current;
				// TRADE_NAME
				_with3.Parameters.Add("TRADE_NAME_IN", OracleDbType.Varchar2, 50, "TRADE_NAME").Direction = ParameterDirection.Input;
				_with3.Parameters["TRADE_NAME_IN"].SourceVersion = DataRowVersion.Current;

				// ACTIVE_FLAG
				_with3.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				_with3.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				// BUSINESS_TYPE
				_with3.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
				_with3.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				// PORT_MST_FKs (Direct Value)
				_with3.Parameters.Add("PORT_MST_FK_IN", PortPKs).Direction = ParameterDirection.Input;

				// LAST_MODIFIED_BY_FK
				_with3.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				// CONFIG_MST_FK_IN
				_with3.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				// VERSON_NO
				_with3.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				_with3.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				// RETURN VALUE
				_with3.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with3.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                

				var _with4 = objWK.MyDataAdapter;

				_with4.InsertCommand = ICM;
				_with4.InsertCommand.Transaction = TRAN;
				_with4.UpdateCommand = UCM;
				_with4.UpdateCommand.Transaction = TRAN;
				_with4.DeleteCommand = DCM;
				_with4.DeleteCommand.Transaction = TRAN;
				RecAfct = _with4.Update(DS);
				retVal = Convert.ToString(ICM.Parameters["RETURN_VALUE"].Value);

				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					TRAN.Commit();
					if ((retVal != null)) {
						DS.Tables[0].Rows[0]["TRADE_MST_PK"] = retVal;
						DS.Tables[0].Rows[0]["VERSION_NO"] = 0;
					} else {
						DS.Tables[0].Rows[0]["VERSION_NO"] = Convert.ToInt32(DS.Tables[0].Rows[0]["VERSION_NO"]) + 1;
					}
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

		#region " Fetch Trade Ports "
		// Important ( Selected Ports for a Trade.. If no tradePk then no Selected ports )
		public DataTable FetchSelectedTradePorts(string forTradePKs = "", bool ActiveOnly = true)
		{

			string strSQL = null;
			string strCondition = "";

			strSQL = "              Select ";
			// TRADE_MST_TBL Columns
			strSQL += "        0 SELECTED,";
			strSQL += "        rm.REGION_MST_PK,";
			strSQL += "        rm.REGION_CODE,";
			strSQL += "        rm.REGION_NAME,";
			strSQL += "        cn.COUNTRY_MST_PK,";
			strSQL += "        cn.COUNTRY_ID,";
			strSQL += "        cn.COUNTRY_NAME,";
			strSQL += "        pt.PORT_MST_PK,";
			strSQL += "        pt.PORT_ID,";
			strSQL += "        pt.PORT_NAME,";
			strSQL += "        decode(pt.PORT_TYPE, 0, '', 1, 'ICD',2,'SEA','') PORT_TYPE_NAME,";
			strSQL += "        cn.COUNTRY_NAME COUNTRYNAME";
			strSQL += "    from ";
			strSQL += "        REGION_MST_TBL rm, ";
			strSQL += "        REGION_MST_TRN rt, ";
			strSQL += "        PORT_MST_TBL pt, ";
			strSQL += "        COUNTRY_MST_TBL cn ";
			strSQL += "    where 1=1 ";
			// Join Condition
			strSQL += "        and rt.REGION_MST_FK  = rm.REGION_MST_PK(+) ";
			strSQL += "        and rt.PORT_MST_FK    = pt.PORT_MST_PK(+) ";
			strSQL += "        and rt.COUNTRY_MST_FK = cn.COUNTRY_MST_PK(+) ";
			// Active Only Condition
			if (ActiveOnly == true) {
				strCondition += "  and rm.ACTIVE_FLAG = 1 ";
				strCondition += "  and pt.ACTIVE_FLAG = 1 ";
				strCondition += "  and cn.ACTIVE_FLAG = 1 ";
			}
			// Trade Condition
			if (!string.IsNullOrEmpty(forTradePKs)) {
				strCondition += "  and  pt.PORT_MST_PK in ";
				strCondition += "      ( Select PORT_MST_FK from TRADE_MST_TRN where ";
				strCondition += "          TRADE_MST_FK in (" + forTradePKs + ")";
				strCondition += "      )";
			} else {
				strCondition += "  and  1=2 ";
			}
			// Adding All Condition to SQL Statement

			strSQL += strCondition;

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL).Tables[0];
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

		#region " Available Ports For a Region "
		// All Ports Under a region. If no Region then All Ports Under All Region
		public DataTable FetchPortsForRegions(string regionPKs = "", bool ActiveOnly = true)
		{
			string strSQL = null;
			string strCondition = "";

			strSQL = "              Select ";
			// TRADE_MST_TBL Columns
			strSQL += "        0 SELECTED,";
			strSQL += "        rm.REGION_MST_PK,";
			strSQL += "        rm.REGION_CODE,";
			strSQL += "        rm.REGION_NAME,";
			strSQL += "        cn.COUNTRY_MST_PK,";
			strSQL += "        cn.COUNTRY_ID,";
			strSQL += "        cn.COUNTRY_NAME,";
			strSQL += "        pt.PORT_MST_PK,";
			strSQL += "        pt.PORT_ID,";
			strSQL += "        pt.PORT_NAME,";
			strSQL += "        decode(pt.PORT_TYPE, 0, '', 1, 'ICD',2,'SEA','') PORT_TYPE_NAME,";
			strSQL += "        cn.COUNTRY_NAME COUNTRYNAME";
			strSQL += "    from ";
			strSQL += "        REGION_MST_TBL rm, ";
			strSQL += "        REGION_MST_TRN rt, ";
			strSQL += "        PORT_MST_TBL pt, ";
			strSQL += "        COUNTRY_MST_TBL cn ";
			strSQL += "    where 1=1 ";
			// Join Condition

			strSQL += "        and rt.REGION_MST_FK  = rm.REGION_MST_PK(+) ";
			strSQL += "        and rt.PORT_MST_FK    = pt.PORT_MST_PK(+) ";
			strSQL += "        and rt.COUNTRY_MST_FK = cn.COUNTRY_MST_PK(+) ";
			// For Selected Region Condition
			if (!string.IsNullOrEmpty(regionPKs)) {
				strCondition += "  and rm.REGION_MST_PK in (" + regionPKs + ") ";
			}
			// Active Only Condition
			if (ActiveOnly == true) {
				strCondition += "  and rm.ACTIVE_FLAG = 1 ";
				strCondition += "  and pt.ACTIVE_FLAG = 1 ";
				strCondition += "  and cn.ACTIVE_FLAG = 1 ";
			}
			strSQL += strCondition;

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL).Tables[0];
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

		#region " Available Ports For a Trade "
		// This will show list of all available ports under given region which are still not selected
		// If no region then no ports available
		// if region = "ALL" then all regions will be aplicable
		// if tradePk then only for those tradePKs
		// If no Trade then all ports falling under given regions are available
		public DataTable FetchAvailablePortsForTrade(string forTradePKs = "", string underRegionPKs = "", bool ActiveOnly = true, string businessType = "3")
		{
			string strSQL = null;
			string strCondition = "";

			strSQL = "              Select ";
			// TRADE_MST_TBL Columns
			strSQL += "        0 SELECTED,";
			strSQL += "        rm.REGION_MST_PK,";
			strSQL += "        rm.REGION_CODE,";
			strSQL += "        rm.REGION_NAME,";
			strSQL += "        cn.COUNTRY_MST_PK,";
			strSQL += "        cn.COUNTRY_ID,";
			strSQL += "        cn.COUNTRY_NAME,";
			strSQL += "        pt.PORT_MST_PK,";
			strSQL += "        pt.PORT_ID,";
			//strSQL &= vbCrLf & "        cn.COUNTRY_NAME COUNTRYNAME,"
			strSQL += "        pt.PORT_NAME,";
			strSQL += "        decode(pt.PORT_TYPE, 0, '', 1, 'ICD',2,'SEA','') PORT_TYPE_NAME,";
			//strSQL &= vbCrLf & "        pt.PORT_NAME"
			strSQL += "        cn.COUNTRY_NAME COUNTRYNAME";
			strSQL += "    from ";
			strSQL += "        REGION_MST_TBL rm, ";
			strSQL += "        REGION_MST_TRN rt, ";
			strSQL += "        PORT_MST_TBL pt, ";
			strSQL += "        COUNTRY_MST_TBL cn ";
			strSQL += "    where 1=1 ";
			// Join Condition
			strSQL += "        and rt.REGION_MST_FK  = rm.REGION_MST_PK(+) ";
			strSQL += "        and rt.PORT_MST_FK    = pt.PORT_MST_PK(+) ";
			strSQL += "        and rt.COUNTRY_MST_FK = cn.COUNTRY_MST_PK(+) ";
			// Active Only Condition
			if (ActiveOnly == true) {
				strCondition += "  and rm.ACTIVE_FLAG = 1 ";
				strCondition += "  and pt.ACTIVE_FLAG = 1 ";
				strCondition += "  and cn.ACTIVE_FLAG = 1 ";
			}
			// For Selected Region Condition
			if (underRegionPKs.ToUpper().Trim() == "ALL") {
				strCondition += " and 1=1 ";
			} else {
				if (string.IsNullOrEmpty(underRegionPKs)) {
					strCondition += " and 1=2 ";
				} else {
					strCondition += "  and rm.REGION_MST_PK in (" + underRegionPKs + ") ";
					// strCondition &= vbCrLf & "and pt.port_mst_pk not in (select distinct tm.port_mst_fk from TRADE_MST_TRN tm )"
				}
			}

			// Trade Condition
			if (!string.IsNullOrEmpty(forTradePKs)) {
				strCondition += "  and  pt.PORT_MST_PK not in ";
				strCondition += "      ( Select PORT_MST_FK from TRADE_MST_TRN where ";
				strCondition += "          TRADE_MST_FK in (" + forTradePKs + ")";
				strCondition += "      )";
			} else {
				strCondition += "  and  1=1 ";
			}
			if (businessType == "1") {
				strCondition += " and pt.BUSINESS_TYPE in(1) ";
			} else if (businessType == "2") {
				strCondition += " and pt.BUSINESS_TYPE in(2) ";
			}

			strSQL += strCondition;

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL).Tables[0];
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

	}

}
