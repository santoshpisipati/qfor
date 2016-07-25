using Microsoft.VisualBasic;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Xml.Linq;

using Oracle.DataAccess.Client;

namespace Quantum_QFOR
{

	public class clsRegionDetails : CommonFeatures
	{

		#region " Fetch One "
		// This function will fetch only one record.
		// In case a regionPK is provided then the corresponding record will be fetched
		// in case a wrong RegionPK is provided that will raise an exception
		// in case no RegionPK is provided then a blank record will be returned by the DataSet
		public DataSet FetchOne(string RegionPK = "")
		{
			bool NewRecord = true;
			if (RegionPK.Trim().Length > 0)
				NewRecord = false;

			string strSQL = null;
			string strCondition = null;

			// Record Type Condition
			if (!NewRecord) {
				strCondition += " and REGION_MST_PK = " + RegionPK + " ";
				// one satisfying row in dataset
			} else {
				strCondition += " and 1 = 2 ";
				// dataset with no row just the format
			}

			strSQL = "              Select ";
			//REGION_MST_TBL Columns
			strSQL += "        REGION_MST_PK,";
			strSQL += "        REGION_CODE,";
			strSQL += "        REGION_NAME,";
			strSQL += "        LOCATION_MST_FK,";
			strSQL += "        VERSION_NO,";
			strSQL += "        BUSINESS_TYPE,";
			strSQL += "        ACTIVE_FLAG ";
			strSQL += "    from ";
			strSQL += "        REGION_MST_TBL ";
			strSQL += "        Where 1 = 1 " + strCondition;

			DataSet ds = null;
			WorkFlow objWF = new WorkFlow();
			try {
				ds = objWF.GetDataSet(strSQL);
				if (NewRecord == true) {
					ds.Tables[0].Rows.Add(ds.Tables[0].NewRow());
					// to return a new blank record
					ds.Tables[0].Rows[0]["ACTIVE_FLAG"] = 1;
				}
				return ds;
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

		public ArrayList SaveRecord(DataSet DS, string PortPKs = "", string CountryPKs = "")
		{
			if (string.IsNullOrEmpty(PortPKs))
				PortPKs = "-";
			if (string.IsNullOrEmpty(CountryPKs))
				CountryPKs = "-";
			string retVal = null;
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			Int32 RecAfct = default(Int32);
			//To Remove
			//M_Configuration_PK = 40
			OracleCommand ICM = new OracleCommand();
			OracleCommand UCM = new OracleCommand();
			OracleCommand DCM = new OracleCommand();
			string INS_Proc = null;
			string DEL_Proc = null;
			string UPD_Proc = null;
			string UserName = objWK.MyUserName;
			INS_Proc = UserName + ".REGION_MST_TBL_PKG.REGION_MST_TBL_INS";
			DEL_Proc = UserName + ".REGION_MST_TBL_PKG.REGION_MST_TBL_DEL";
			UPD_Proc = UserName + ".REGION_MST_TBL_PKG.REGION_MST_TBL_UPD";

			try {
				// INSERT COMMAND ***********************************************************************
				var _with1 = ICM;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = INS_Proc;
				// REGION_CODE
				_with1.Parameters.Add("REGION_CODE_IN", OracleDbType.Varchar2, 20, "REGION_CODE").Direction = ParameterDirection.Input;
				_with1.Parameters["REGION_CODE_IN"].SourceVersion = DataRowVersion.Current;
				// REGION_NAME
				_with1.Parameters.Add("REGION_NAME_IN", OracleDbType.Varchar2, 50, "REGION_NAME").Direction = ParameterDirection.Input;
				_with1.Parameters["REGION_NAME_IN"].SourceVersion = DataRowVersion.Current;

				//LOCATION MST FK
				_with1.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
				_with1.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;


				// BUSINESS_TYPE
				_with1.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
				_with1.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;


				// ACTIVE_FLAG
				_with1.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				_with1.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;


				// PORT_MST_FKs (Direct Value)
				_with1.Parameters.Add("PORT_MST_FK_IN", PortPKs).Direction = ParameterDirection.Input;

				// COUNTRY_MST_FKs (Direct Value)
				_with1.Parameters.Add("COUNTRY_MST_FK_IN", CountryPKs).Direction = ParameterDirection.Input;

				// CREATED_BY_FK_IN
				_with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

				// CONFIG_MST_FK_IN
				_with1.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				// RETURN_VALUE ( OUTPUT PARAMETER )
				_with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



				var _with2 = UCM;
				_with2.Connection = objWK.MyConnection;
				_with2.CommandType = CommandType.StoredProcedure;
				_with2.CommandText = UPD_Proc;

				// REGION_MST_PK
				_with2.Parameters.Add("REGION_MST_PK_IN", OracleDbType.Int32, 10, "REGION_MST_PK").Direction = ParameterDirection.Input;
				_with2.Parameters["REGION_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

				// REGION_CODE
				_with2.Parameters.Add("REGION_CODE_IN", OracleDbType.Varchar2, 20, "REGION_CODE").Direction = ParameterDirection.Input;
				_with2.Parameters["REGION_CODE_IN"].SourceVersion = DataRowVersion.Current;
				// REGION_NAME
				_with2.Parameters.Add("REGION_NAME_IN", OracleDbType.Varchar2, 50, "REGION_NAME").Direction = ParameterDirection.Input;
				_with2.Parameters["REGION_NAME_IN"].SourceVersion = DataRowVersion.Current;

				// LOCATION_NAME
				_with2.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
				_with2.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;


				// BUSINESS_TYPE
				_with2.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
				_with2.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;


				// ACTIVE_FLAG
				_with2.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				_with2.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

				// PORT_MST_FKs (Direct Value)
				_with2.Parameters.Add("PORT_MST_FK_IN", PortPKs).Direction = ParameterDirection.Input;

				// COUNTRY_MST_FKs (Direct Value)
				_with2.Parameters.Add("COUNTRY_MST_FK_IN", CountryPKs).Direction = ParameterDirection.Input;

				// LAST_MODIFIED_BY_FK
				_with2.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				// CONFIG_MST_FK_IN
				_with2.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				//VERSION_NO
				_with2.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				_with2.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
				// RETURN VALUE
				_with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with2.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


				objWK.MyDataAdapter.RowUpdated += new OracleRowUpdatedEventHandler(OnRowUpdated);

				var _with3 = objWK.MyDataAdapter;

				_with3.InsertCommand = ICM;
				_with3.InsertCommand.Transaction = TRAN;
				_with3.UpdateCommand = UCM;
				_with3.UpdateCommand.Transaction = TRAN;
				//.DeleteCommand = DCM
				//.DeleteCommand.Transaction = TRAN
				RecAfct = _with3.Update(DS);
				retVal = ICM.Parameters["RETURN_VALUE"].Value;

				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					TRAN.Commit();
					if ((retVal != null)) {
						DS.Tables[0].Rows[0]["REGION_MST_PK"] = retVal;
						DS.Tables[0].Rows[0]["VERSION_NO"] = 0;
					} else {
						DS.Tables[0].Rows[0]["VERSION_NO"] = DS.Tables[0].Rows[0]["VERSION_NO"] + 1;
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
		#region " Fetch Selected Region Ports "

		// QFOR
		// Important ( Selected Ports for a Region.. If no RegionPk then no Selected ports )
		public DataTable FetchSelectedRegionPorts(string forRegionPKs = "", bool ActiveOnly = true)
		{

			string strSQL = null;
			string strCondition = "";

			strSQL = "              Select ";
			// REGION_MST_TBL Columns
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
				//strCondition &= vbCrLf & "  and rm.ACTIVE_FLAG = 1 "
				strCondition += "  and pt.ACTIVE_FLAG = 1 ";
				strCondition += "  and cn.ACTIVE_FLAG = 1 ";
			}
			// Region Condition
			if (!string.IsNullOrEmpty(forRegionPKs)) {
				strCondition += "  and rt.region_mst_fk in (" + forRegionPKs + ")";
				//strCondition &= vbCrLf & "  and  pt.PORT_MST_PK in "
				//strCondition &= vbCrLf & "      ( Select PORT_MST_FK from REGION_MST_TRN where "
				//strCondition &= vbCrLf & "          REGION_MST_FK in (" & forRegionPKs & ")"
				//strCondition &= vbCrLf & "      )"
			} else {
				strCondition += "  and  1=2 ";
			}
			// Adding All Condition to SQL Statement

			strSQL += strCondition;
			strSQL += " ORDER BY cn.COUNTRY_NAME, pt.PORT_NAME";
			//strSQL &= vbCrLf & " ORDER BY cn.COUNTRY_NAME"

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL).Tables[0];
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion
		#region "Enhance Search Function"
		public string FetchLocation(string strCond)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = null;
			string strReq = null;
			string strLOC_MST_IN = "";
			string SEARCH_FLAG_IN = "";
			string CREDIT_FLAG = "";
			var strNull = "";
			arr = strCond.Split("~");
			strReq = arr(0);
			strSERACH_IN = arr(1);

			if (arr.Length > 2)
				strLOC_MST_IN = arr(2);
			if (arr.Length > 3)
				SEARCH_FLAG_IN = arr(3);
			if (arr.Length > 4)
				CREDIT_FLAG = arr(4);
			try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_LOCATION_PKG.GET_LOC_COMMON_CRDMGNR";

				var _with4 = selectCommand.Parameters;
				_with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				_with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with4.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : "")).Direction = ParameterDirection.Input;
				_with4.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
				_with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with4.Add("CREDIT_FLAG", (string.IsNullOrEmpty(CREDIT_FLAG) ? strNull : CREDIT_FLAG)).Direction = ParameterDirection.Input;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}

		public string FetchLocationByName(string strCond)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = null;
			string strReq = null;
			var strNull = "";
			arr = strCond.Split("~");
			strReq = arr(0);
			strSERACH_IN = arr(1);

			try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_LOCATION_PKG.GETLOCATION_COMMON_BY_LOCNAME";

				var _with5 = selectCommand.Parameters;
				_with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				_with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}

		public string FetchLocationByNameForCountry(string strCond)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = null;
			string strReq = null;
			int CountryPk = 0;
			var strNull = "";
			arr = strCond.Split("~");
			strReq = arr(0);
			strSERACH_IN = arr(1);
			CountryPk = arr(2);

			try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_LOCATION_PKG.GETLOC_BY_LOCNAME_FOR_COUNTRY";

				var _with6 = selectCommand.Parameters;
				_with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				_with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with6.Add("COUNTRY_PK_IN", CountryPk).Direction = ParameterDirection.Input;
				_with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}
		#endregion
		#region " Available Ports For a Country "
		// All Ports Under a country. If no Country then All Ports Under All Countries
		public DataTable FetchPortsForCountries(string forCountryPKs = "", bool ActiveOnly = true, Int16 intBusinessType = 0, string Region_Key = "")
		{
			string strSQL = null;
			string strCondition = "";
			// all ports under given countries
			// select * from port_mst_tbl where country_mst_fk in forcountryPKs
			// minus selected ports under those countries

			strSQL = "              Select ";
			// REGION_MST_TBL Columns
			strSQL += "        0 SELECTED,";
			strSQL += "        0  REGION_MST_PK,";
			strSQL += "        '' REGION_CODE,";
			strSQL += "        '' REGION_NAME,";
			strSQL += "        cn.COUNTRY_MST_PK,";
			strSQL += "        cn.COUNTRY_ID,";
			strSQL += "        cn.COUNTRY_NAME,";
			strSQL += "        pt.PORT_MST_PK,";
			strSQL += "        pt.PORT_ID,";
			strSQL += "        pt.PORT_NAME,";
			strSQL += "        decode(pt.PORT_TYPE, 0, '', 1, 'ICD',2,'SEA','') PORT_TYPE_NAME,";
			strSQL += "        cn.COUNTRY_NAME COUNTRYNAME";
			strSQL += "    from ";
			strSQL += "        PORT_MST_TBL pt, ";
			strSQL += "        COUNTRY_MST_TBL cn ";
			strSQL += "    where 1=1 ";
			// Join Condition

			strSQL += "        and pt.COUNTRY_MST_FK = cn.COUNTRY_MST_PK(+) ";

			//'Commented To Hide business type
			//'strSQL &= vbCrLf & "    and pt.Business_Type = " & intBusinessType & " "

			if (!string.IsNullOrEmpty(forCountryPKs)) {
				strCondition += "  and cn.COUNTRY_MST_PK in (" + forCountryPKs + ") ";
			}
			// Active Only Condition
			if (ActiveOnly == true) {
				strCondition += "  and pt.ACTIVE_FLAG = 1 ";
				strCondition += "  and cn.ACTIVE_FLAG = 1 ";
			}
			//****************************************
			// Country FK Condition
			string strCountryCondition = " and 1=2 ";
			if (!string.IsNullOrEmpty(forCountryPKs)) {
				strCountryCondition = " and  COUNTRY_MST_FK in (" + forCountryPKs + ") ";
			}
			strCondition += "  and pt.PORT_MST_PK in ";
			strCondition += "      (  Select PORT_MST_PK from PORT_MST_TBL ";
			strCondition += "         where 1 = 1 " + strCountryCondition;
			strCondition += "      ) ";
			if (!string.IsNullOrEmpty(Region_Key)) {
				strCondition += "  and pt.PORT_MST_PK not in (";
				//strCondition &= vbCrLf & "        Select PORT_MST_FK from REGION_MST_TRN "
				//strCondition &= vbCrLf & "         minus "
				strCondition += "         select PORT_MST_FK from REGION_MST_TRN where ";
				strCondition += "         REGION_MST_FK = " + Region_Key + strCountryCondition;
				strCondition += "      ) ";
			} else {
				strCondition += "  and pt.PORT_MST_PK not in ";
				strCondition += "      (select PORT_MST_FK from REGION_MST_TRN) ";
			}
			//If All = False Then
			//    strCondition &= vbCrLf & "  and pt.PORT_MST_PK not in " 
			//    strCondition &= vbCrLf & "      ( Select PORT_MST_FK from REGION_MST_TRN  "
			//    strCondition &= vbCrLf & "           where 1=1 " & strCountryCondition
			//    strCondition &= vbCrLf & "      ) "
			//Else
			//End If


			strSQL += strCondition;
			strSQL += " ORDER BY cn.COUNTRY_NAME, pt.PORT_NAME";
			//strSQL &= vbCrLf & " ORDER BY pt.PORT_NAME"
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL).Tables[0];
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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
		// This will show list of all available ports under given region which are still not selected
		// If no region then no ports available
		// if region = "ALL" then all regions will be aplicable
		// if RegionPk then only for those tradePKs
		// If no region then all ports falling under given Countries are available
		public DataTable FetchAvailablePortsForRegions(string forRegionPKs = "", string underCountryPKs = "", bool ActiveOnly = true, Int16 intBusinessType = 0)
		{
			string strSQL = null;
			string strCondition = "";

			strSQL = "              Select ";
			// REGION_MST_TBL Columns
			strSQL += "        0 SELECTED,";
			strSQL += "        '' REGION_MST_PK,";
			strSQL += "        '' REGION_CODE,";
			strSQL += "        '' REGION_NAME,";
			strSQL += "        cn.COUNTRY_MST_PK,";
			strSQL += "        cn.COUNTRY_ID,";
			strSQL += "        cn.COUNTRY_NAME,";
			strSQL += "        pt.PORT_MST_PK,";
			strSQL += "        pt.PORT_ID,";
			strSQL += "        pt.PORT_NAME,";
			strSQL += "        decode(pt.PORT_TYPE, 0, '', 1, 'ICD',2,'SEA','') PORT_TYPE_NAME,";
			strSQL += "        cn.COUNTRY_NAME COUNTRYNAME";
			strSQL += "    from ";
			// strSQL &= vbCrLf & "        REGION_MST_TBL rm, "
			//strSQL &= vbCrLf & "        REGION_MST_TRN rt, "
			strSQL += "        PORT_MST_TBL pt, ";
			strSQL += "        COUNTRY_MST_TBL cn ";
			strSQL += "    where 1=1 ";
			// Join Condition
			if (!string.IsNullOrEmpty(underCountryPKs)) {
				strSQL += " AND pt.COUNTRY_MST_FK IN(" + underCountryPKs + ") ";
			}
			//strSQL &= vbCrLf & "        and rt.REGION_MST_FK  = rm.REGION_MST_PK(+) "
			//strSQL &= vbCrLf & "        and rt.PORT_MST_FK    = pt.PORT_MST_PK(+) "
			//strSQL &= vbCrLf & "        and rt.COUNTRY_MST_FK = cn.COUNTRY_MST_PK(+) "
			//'Commented to Hide business type from Region Master
			///strSQL &= vbCrLf & "        and pt.BUSINESS_TYPE = " & intBusinessType & ""

			// Active Only Condition
			if (ActiveOnly == true) {
				//strCondition &= vbCrLf & "  and rm.ACTIVE_FLAG = 1 "
				strCondition += "  and pt.ACTIVE_FLAG = 1 ";
				strCondition += "  and cn.ACTIVE_FLAG = 1 ";
			}

			// For Selected Country Condition
			if (underCountryPKs.ToUpper().Trim() == "ALL") {
				strCondition += " and 1=1 ";
			} else {
				if (string.IsNullOrEmpty(underCountryPKs)) {
					strCondition += " and 1=2 ";
				} else {
					strCondition += "  and cn.COUNTRY_MST_PK in (" + underCountryPKs + ") ";
				}
			}
			strSQL += "      and cn.country_mst_pk = pt.country_mst_fk ";
			// Region Condition
			if (!string.IsNullOrEmpty(forRegionPKs)) {
				strCondition += "  and  pt.PORT_MST_PK not in ";
				strCondition += "      ( Select PORT_MST_FK from REGION_MST_TRN  ";
				strCondition += " where         REGION_MST_FK in (" + forRegionPKs + ")";
				//uncommented by Anand G on 30-05-08 because it restrincting the ports for the country and region
				strCondition += "      )";
			} else {
				strCondition += "  and  1=1 ";
			}
			strSQL += strCondition;
			strSQL += " ORDER BY cn.COUNTRY_NAME, pt.PORT_NAME";
			//strSQL &= vbCrLf & " ORDER BY cn.PORT_NAME"

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL).Tables[0];
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion
		#region "Get Location Id and Location Name when Sending Location Pk"
		public string GetLocationID(int locpk)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			try {
				strSQL = "select LOCATION_ID from LOCATION_MST_TBL where LOCATION_MST_PK = " + locpk + " ";
				string LocName = null;
				LocName = objWF.ExecuteScaler(strSQL);
				return LocName;
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public string GetLocationName(int locPk)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			try {
				strSQL = "select LOCATION_NAME from LOCATION_MST_TBL where LOCATION_MST_PK = " + locPk + " ";
				string LocName = null;
				LocName = objWF.ExecuteScaler(strSQL);
				return LocName;
			//Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
	}

}
