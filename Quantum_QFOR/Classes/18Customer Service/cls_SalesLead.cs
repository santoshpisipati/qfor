
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

using System;
using System.Collections;
using System.Data;
using Oracle.DataAccess.Client;
using System.Web;

namespace Quantum_QFOR
{
    public class clsSalesLead : CommonFeatures
	{

		#region " FetchSalesLead "
		public DataSet FetchSalesLead(long SalesCallPK, long SalesLeadPK = 0)
		{

			WorkFlow objWF = new WorkFlow();
			DataSet ds = null;
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			try {
				sb.Append("SELECT SLH.SALES_LEAD_HDR_PK,");
				sb.Append("       SLH.SALES_LEAD_ID,");
				sb.Append("       SLH.SALES_LEAD_DATE,");
				sb.Append("       SLH.LEAD_STATUS,");
				sb.Append("       SLH.SALES_CALL_FK,");
				sb.Append("       SLH.NEXT_ACTION,");
				sb.Append("       SLH.CUSTOMER_MST_FK,");
				sb.Append("       CMT.CUSTOMER_ID,");
				sb.Append("       CMT.CUSTOMER_NAME,");
				sb.Append("       CCD.ADM_ADDRESS_1,");
				sb.Append("       CCD.ADM_ADDRESS_2,");
				sb.Append("       CCD.ADM_ADDRESS_3,");
				sb.Append("       CCD.ADM_CITY,");
				sb.Append("       CCD.ADM_ZIP_CODE,");
				sb.Append("       CT.COUNTRY_MST_PK,");
				sb.Append("       CT.COUNTRY_ID,");
				sb.Append("       CT.COUNTRY_NAME,");
				sb.Append("       CCD.ADM_PHONE_NO_1,");
				sb.Append("       CCD.ADM_PHONE_NO_2,");
				sb.Append("       CCD.ADM_FAX_NO,");
				sb.Append("       CCD.ADM_EMAIL_ID,");
				sb.Append("       CCD.ADM_URL,");
				sb.Append("       CMT.BUSINESS_TYPE BIZTYPE,");
				sb.Append("       POL.PORT_MST_PK           POL_FK,");
				sb.Append("       POL.PORT_ID               POL_ID,");
				sb.Append("       POL.PORT_NAME             POL_NAME,");
				sb.Append("       POD.PORT_MST_PK           POD_FK,");
				sb.Append("       POD.PORT_ID               POD_ID,");
				sb.Append("       POD.PORT_NAME             POD_NAME,");
				sb.Append("       CGMT.COMMODITY_GROUP_PK   COMMODITY_GRP_FK,");
				sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
				sb.Append("       CGMT.COMMODITY_GROUP_DESC COMMODITY_GROUP_DESC,");
				sb.Append("       CM.COMMODITY_MST_PK       COMMODITY_MST_FK,");
				sb.Append("       CM.COMMODITY_ID,");
				sb.Append("       CM.COMMODITY_NAME");
				sb.Append("  FROM SALES_LEAD_HDR          SLH,");
				sb.Append("       SALES_CALL_TRN          SCT,");
				sb.Append("       CUSTOMER_MST_TBL        CMT,");
				sb.Append("       CUSTOMER_CONTACT_DTLS   CCD,");
				sb.Append("       COUNTRY_MST_TBL         CT,");
				sb.Append("       PORT_MST_TBL            POL,");
				sb.Append("       PORT_MST_TBL            POD,");
				sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
				sb.Append("       COMMODITY_MST_TBL       CM");
				sb.Append(" WHERE SCT.SALES_CALL_PK = SLH.SALES_CALL_FK");
				sb.Append("   AND CMT.CUSTOMER_MST_PK = SLH.CUSTOMER_MST_FK");
				sb.Append("   AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("   AND CT.COUNTRY_MST_PK = CCD.ADM_COUNTRY_MST_FK");
				sb.Append("   AND POL.PORT_MST_PK = SLH.POL_FK");
				sb.Append("   AND POD.PORT_MST_PK = SLH.POD_FK");
				sb.Append("   AND CGMT.COMMODITY_GROUP_PK = SLH.COMMODITY_GRP_FK");
				sb.Append("   AND CM.COMMODITY_MST_PK = SLH.COMMODITY_MST_FK");
				sb.Append("   AND SCT.SALES_CALL_PK = " + SalesCallPK);

				ds = objWF.GetDataSet(sb.ToString());

				if (ds.Tables[0].Rows.Count > 0) {
					return ds;
				}

				sb.Replace(sb.ToString(), "");
				sb.Append("SELECT 0 SALES_LEAD_HDR_PK,");
				sb.Append("       '' SALES_LEAD_ID,");
				sb.Append("       SYSDATE SALES_LEAD_DATE,");
				sb.Append("       0 LEAD_STATUS,");
				sb.Append("       SCT.SALES_CALL_PK SALES_CALL_FK,");
				sb.Append("       0 NEXT_ACTION,");
				sb.Append("       SCT.CUSTOMER_MST_FK,");
				sb.Append("       CMT.CUSTOMER_ID,");
				sb.Append("       CMT.CUSTOMER_NAME,");
				sb.Append("       CCD.ADM_ADDRESS_1,");
				sb.Append("       CCD.ADM_ADDRESS_2,");
				sb.Append("       CCD.ADM_ADDRESS_3,");
				sb.Append("       CCD.ADM_CITY,");
				sb.Append("       CCD.ADM_ZIP_CODE,");
				sb.Append("       CT.COUNTRY_MST_PK,");
				sb.Append("       CT.COUNTRY_ID,");
				sb.Append("       CT.COUNTRY_NAME,");
				sb.Append("       CCD.ADM_PHONE_NO_1,");
				sb.Append("       CCD.ADM_PHONE_NO_2,");
				sb.Append("       CCD.ADM_FAX_NO,");
				sb.Append("       CCD.ADM_EMAIL_ID,");
				sb.Append("       CCD.ADM_URL,");
				sb.Append("       SCT.BIZTYPE,");
				sb.Append("       NULL                POL_FK,");
				sb.Append("       NULL                POL_ID,");
				sb.Append("       NULL                POL_NAME,");
				sb.Append("       NULL                POD_FK,");
				sb.Append("       NULL                POD_ID,");
				sb.Append("       NULL                POD_NAME,");
				sb.Append("       NULL                COMMODITY_GRP_FK,");
				sb.Append("       NULL                COMMODITY_GROUP_CODE,");
				sb.Append("       NULL                COMMODITY_GROUP_DESC,");
				sb.Append("       NULL                COMMODITY_MST_FK,");
				sb.Append("       NULL                COMMODITY_ID,");
				sb.Append("       NULL                COMMODITY_NAME");
				sb.Append("  FROM SALES_CALL_TRN SCT, ");
				sb.Append("       CUSTOMER_MST_TBL CMT, ");
				sb.Append("       CUSTOMER_CONTACT_DTLS CCD, ");
				sb.Append("       COUNTRY_MST_TBL CT ");
				sb.Append(" WHERE CMT.CUSTOMER_MST_PK = SCT.CUSTOMER_MST_FK");
				sb.Append("   AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("   AND CT.COUNTRY_MST_PK = CCD.ADM_COUNTRY_MST_FK");
				sb.Append("   AND SCT.SALES_CALL_PK = " + SalesCallPK);

				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}

		}
		#endregion

		#region " Fetch Customer Contact "
		public DataSet FetchCustomerContact(long CustPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				sb.Append("SELECT ROWNUM SLNR,");
				sb.Append("       CCT.CUST_CONTACT_PK,");
				sb.Append("       CCT.NAME,");
				sb.Append("       CCT.ALIAS ALIAS_NAME,");
				sb.Append("       CCT.DESIGNATION,");
				sb.Append("       CCT.RESPONSIBILITY,");
				sb.Append("       CCT.DIR_PHONE,");
				sb.Append("       CCT.MOBILE,");
				sb.Append("       CCT.FAX,");
				sb.Append("       CCT.EMAIL,");
				sb.Append("       '' PREFERENCE,");
				sb.Append("       '' DEL");
				sb.Append("  FROM CUSTOMER_CONTACT_TRN CCT");
				sb.Append(" WHERE CCT.CUSTOMER_MST_FK = " + CustPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Fill LeadSource DropDown "
		public DataSet FillLeadSource()
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT  LSMT.LEAD_SRC_PK, ");
			sb.Append("   LSMT.LEAD_SRC_DESC");
			sb.Append("  FROM LEAD_SRC_MST_TBL LSMT");

			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region " Fetch Lead Source "
		public DataSet FetchLeadSource(long SalesLeadPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT ROWNUM SLNR, ");
			sb.Append("       SLINF.SALES_LEAD_INFO_PK, ");
			sb.Append("       SLINF.LEAD_SRC_FK, ");
			sb.Append("       LSMT.LEAD_SRC_DESC, ");
			sb.Append("       CMT.CUSTOMER_MST_PK CUSTOMER_MST_FK, ");
			sb.Append("       CMT.CUSTOMER_ID, ");
			sb.Append("       CMT.CUSTOMER_NAME, ");
			sb.Append("       SLINF.CONTACT, ");
			sb.Append("       SLINF.LEAD_SOURCE_INFO, ");
			sb.Append("       SLINF.VERSION_NO, ");
			sb.Append("       0 CHKFLAG, ");
			sb.Append("       '' DELFLAG ");
			sb.Append("  FROM SALES_LEAD_SRC_TRN           SLINF, ");
			sb.Append("       SALES_LEAD_HDR            SLH, ");
			sb.Append("       CUSTOMER_MST_TBL          CMT, ");
			sb.Append("       LEAD_SRC_MST_TBL          LSMT ");
			sb.Append(" WHERE SLH.SALES_LEAD_HDR_PK = SLINF.SALES_LEAD_HDR_FK");
			sb.Append("   AND LSMT.LEAD_SRC_PK = SLINF.LEAD_SRC_FK");
			sb.Append("   AND SLINF.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
			sb.Append("   AND SLINF.SALES_LEAD_HDR_FK = " + SalesLeadPK);
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region " Fetch Lead Container Details "
		public DataSet FetchLeadContainer(long SalesLeadPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT ROWNUM SLNR, ");
			sb.Append("       SCT.SALES_LEAD_CONT_PK,");
			sb.Append("       CTMT.CONTAINER_TYPE_MST_PK CONTAINER_TYPE_MST_FK,");
			sb.Append("       CTMT.CONTAINER_TYPE_MST_ID,");
			sb.Append("       SCT.VOLUME,");
			sb.Append("       SCT.RATE,");
			sb.Append("       '' DEL");
			sb.Append("  FROM SALES_LEAD_CONT_TRN SCT, CONTAINER_TYPE_MST_TBL CTMT");
			sb.Append(" WHERE SCT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK");
			sb.Append("   AND SCT.SALES_LEAD_FK = " + SalesLeadPK);
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region " Save "
		public ArrayList Save(DataSet Mn_DataSet, DataSet Cn_DataSet, DataSet LS_DataSet, long SalesLeadPK)
		{

			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();

			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();


			try {
				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".SALES_LEAD_HDR_PKG.SALES_LEAD_HDR_INS";

				_with1.Parameters.Add("SALES_LEAD_ID_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_ID"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_ID"])).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("SALES_LEAD_DATE_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_DATE"].ToString()) ? DateTime.MinValue : Convert.ToDateTime(Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_DATE"]))).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("LEAD_STATUS_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["LEAD_STATUS"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["LEAD_STATUS"])).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("SALES_CALL_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["SALES_CALL_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["SALES_CALL_FK"])).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("NEXT_ACTION_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["NEXT_ACTION"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["NEXT_ACTION"])).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("POL_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["POL_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["POL_FK"])).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("POD_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["POD_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["POD_FK"])).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("COMMODITY_GRP_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["COMMODITY_GRP_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["COMMODITY_GRP_FK"])).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("COMMODITY_MST_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["COMMODITY_MST_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;

				_with1.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

				var _with2 = updCommand;
				_with2.Connection = objWK.MyConnection;
				_with2.CommandType = CommandType.StoredProcedure;
				_with2.CommandText = objWK.MyUserName + ".SALES_LEAD_HDR_PKG.SALES_LEAD_HDR_UPD";

				_with2.Parameters.Add("SALES_LEAD_HDR_PK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_HDR_PK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_HDR_PK"])).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("SALES_LEAD_ID_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_ID"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_ID"])).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("SALES_LEAD_DATE_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_DATE"].ToString()) ? DateTime.MinValue : Convert.ToDateTime(Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_DATE"]))).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("LEAD_STATUS_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["LEAD_STATUS"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["LEAD_STATUS"])).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("SALES_CALL_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["SALES_CALL_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["SALES_CALL_FK"])).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("NEXT_ACTION_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["NEXT_ACTION"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["NEXT_ACTION"])).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("POL_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["POL_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["POL_FK"])).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("POD_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["POD_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["POD_FK"])).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("COMMODITY_GRP_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["COMMODITY_GRP_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["COMMODITY_GRP_FK"])).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("COMMODITY_MST_FK_IN", (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["COMMODITY_MST_FK"].ToString()) ? "" : Mn_DataSet.Tables[0].Rows[0]["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;

				_with2.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                

				var _with3 = objWK.MyDataAdapter;

				if (SalesLeadPK <= 0) {
					_with3.InsertCommand = insCommand;
					_with3.InsertCommand.Transaction = TRAN;

					RecAfct = _with3.InsertCommand.ExecuteNonQuery();
					SalesLeadPK = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);
				} else {
					_with3.UpdateCommand = updCommand;
					_with3.UpdateCommand.Transaction = TRAN;
					RecAfct = _with3.UpdateCommand.ExecuteNonQuery();
					SalesLeadPK = (string.IsNullOrEmpty(Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_HDR_PK"].ToString()) ? 0 : Convert.ToInt64(Mn_DataSet.Tables[0].Rows[0]["SALES_LEAD_HDR_PK"]));
				}

				if (RecAfct > 0) {
					if (arrMessage.Count == 0) {
						//  If SaveContainer(Cn_DataSet, SalesLeadPK, TRAN) > 0 Then
						if (SaveContainer(Cn_DataSet, SalesLeadPK, TRAN) > 0 & SaveLeadSrc(LS_DataSet, SalesLeadPK, TRAN) >= 0) {
							TRAN.Commit();
							arrMessage.Add("All Data Saved Successfully");
							arrMessage.Add(SalesLeadPK);
							return arrMessage;
						} else {
							arrMessage.Add("Error");
							TRAN.Rollback();
							return arrMessage;
						}
					} else {
						TRAN.Rollback();
						return arrMessage;
					}
				} else {
					TRAN.Rollback();
					arrMessage.Add("Error");
					return arrMessage;
				}

			} catch (OracleException oraEx) {
				TRAN.Rollback();
				arrMessage.Add(oraEx.Message);
				return arrMessage;
			} catch (Exception ex) {
				throw ex;
			} finally {
				if (objWK.MyConnection.State == ConnectionState.Open) {
					objWK.MyConnection.Close();
				}
			}
		}
		#endregion

		#region " Save Container "
		private long SaveContainer(DataSet Cn_DataSet, long SalesLeadPK, OracleTransaction TRAN)
		{

			WorkFlow objWS = new WorkFlow();
			WorkFlow objWK = new WorkFlow();
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			string strContainerPKs = "0";

			Int32 SalesCommentsPK = default(Int32);
			Int32 RecAfct = default(Int32);
			int i = 0;

			for (i = 0; i <= Cn_DataSet.Tables[0].Rows.Count - 1; i++) {
				if (!string.IsNullOrEmpty(Cn_DataSet.Tables[0].Rows[i]["SALES_LEAD_CONT_PK"].ToString())) {
					if (Convert.ToInt32(Cn_DataSet.Tables[0].Rows[i]["SALES_LEAD_CONT_PK"]) > 0) {
						strContainerPKs = strContainerPKs + ", " + Cn_DataSet.Tables[0].Rows[i]["SALES_LEAD_CONT_PK"];
					}
				}
			}

			objWK.ExecuteCommands("DELETE FROM SALES_LEAD_CONT_TRN SCC WHERE SCC.SALES_LEAD_CONT_PK NOT IN (" + strContainerPKs + ")");

			objWK.MyConnection = TRAN.Connection;


			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;

				for (i = 0; i <= Cn_DataSet.Tables[0].Rows.Count - 1; i++) {
					var _with4 = insCommand;
					_with4.Connection = objWK.MyConnection;
					_with4.CommandType = CommandType.StoredProcedure;
					_with4.CommandText = objWK.MyUserName + ".SALES_LEAD_CONT_TRN_PKG.SALES_LEAD_CONT_TRN_INS";
					_with4.Parameters.Clear();

					_with4.Parameters.Add("SALES_LEAD_FK_IN", SalesLeadPK).Direction = ParameterDirection.Input;
					_with4.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", (string.IsNullOrEmpty(Cn_DataSet.Tables[0].Rows[i]["CONTAINER_TYPE_MST_FK"].ToString()) ? "" : Cn_DataSet.Tables[0].Rows[i]["CONTAINER_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
					_with4.Parameters.Add("VOLUME_IN", (string.IsNullOrEmpty(Cn_DataSet.Tables[0].Rows[i]["VOLUME"].ToString()) ? "" : Cn_DataSet.Tables[0].Rows[i]["VOLUME"])).Direction = ParameterDirection.Input;
					_with4.Parameters.Add("RATE_IN", (string.IsNullOrEmpty(Cn_DataSet.Tables[0].Rows[i]["RATE"].ToString()) ? "" : Cn_DataSet.Tables[0].Rows[i]["RATE"])).Direction = ParameterDirection.Input;

					_with4.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
					_with4.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

					_with4.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					_with4.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

					var _with5 = updCommand;
					_with5.Connection = objWK.MyConnection;
					_with5.CommandType = CommandType.StoredProcedure;
					_with5.CommandText = objWK.MyUserName + ".SALES_LEAD_CONT_TRN_PKG.SALES_LEAD_CONT_TRN_UPD";
					_with5.Parameters.Clear();

					_with5.Parameters.Add("SALES_LEAD_CONT_PK_IN", Cn_DataSet.Tables[0].Rows[i]["SALES_LEAD_CONT_PK"]).Direction = ParameterDirection.Input;
					_with5.Parameters.Add("SALES_LEAD_FK_IN", SalesLeadPK).Direction = ParameterDirection.Input;
					_with5.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", (string.IsNullOrEmpty(Cn_DataSet.Tables[0].Rows[i]["CONTAINER_TYPE_MST_FK"].ToString()) ? "" : Cn_DataSet.Tables[0].Rows[i]["CONTAINER_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
					_with5.Parameters.Add("VOLUME_IN", (string.IsNullOrEmpty(Cn_DataSet.Tables[0].Rows[i]["VOLUME"].ToString()) ? "" : Cn_DataSet.Tables[0].Rows[i]["VOLUME"])).Direction = ParameterDirection.Input;
					_with5.Parameters.Add("RATE_IN", (string.IsNullOrEmpty(Cn_DataSet.Tables[0].Rows[i]["RATE"].ToString()) ? "" : Cn_DataSet.Tables[0].Rows[i]["RATE"])).Direction = ParameterDirection.Input;

					_with5.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
					_with5.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

					_with5.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    

					var _with6 = objWK.MyDataAdapter;

					SalesCommentsPK = (string.IsNullOrEmpty(Cn_DataSet.Tables[0].Rows[i]["SALES_LEAD_CONT_PK"].ToString()) ? 0 :Convert.ToInt32(Cn_DataSet.Tables[0].Rows[i]["SALES_LEAD_CONT_PK"]));

					if (SalesCommentsPK <= 0) {
						_with6.InsertCommand = insCommand;
						_with6.InsertCommand.Transaction = TRAN;
						RecAfct = RecAfct + _with6.InsertCommand.ExecuteNonQuery();
					} else {
						_with6.UpdateCommand = updCommand;
						_with6.UpdateCommand.Transaction = TRAN;
						RecAfct = RecAfct + _with6.UpdateCommand.ExecuteNonQuery();
					}

				}

				return 1;

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region " Save Lead Source "
		private long SaveLeadSrc(DataSet LS_DataSet, long SalesLeadPK, OracleTransaction TRAN)
		{

			WorkFlow objWS = new WorkFlow();
			WorkFlow objWK = new WorkFlow();
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();

			Int32 SalesCommentsPK = default(Int32);
			Int32 RecAfct = default(Int32);
			int i = 0;

			string strLeadSrcPKs = "0";
			for (i = 0; i <= LS_DataSet.Tables[0].Rows.Count - 1; i++) {
				if (!string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["SALES_LEAD_INFO_PK"].ToString())) {
					if (Convert.ToInt32(LS_DataSet.Tables[0].Rows[i]["SALES_LEAD_INFO_PK"]) > 0) {
						strLeadSrcPKs = strLeadSrcPKs + ", " + LS_DataSet.Tables[0].Rows[i]["SALES_LEAD_INFO_PK"];
					}
				}
			}

			objWK.ExecuteCommands("DELETE FROM SALES_LEAD_SRC_TRN SCC WHERE SCC.SALES_LEAD_INFO_PK NOT IN (" + strLeadSrcPKs + ")");

			objWK.MyConnection = TRAN.Connection;


			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;

				for (i = 0; i <= LS_DataSet.Tables[0].Rows.Count - 1; i++) {
					var _with7 = insCommand;
					_with7.Connection = objWK.MyConnection;
					_with7.CommandType = CommandType.StoredProcedure;
					_with7.CommandText = objWK.MyUserName + ".SALES_LEAD_SRC_TRN_PKG.SALES_LEAD_SRC_TRN_INS";
					_with7.Parameters.Clear();

					_with7.Parameters.Add("SALES_LEAD_HDR_FK_IN", SalesLeadPK).Direction = ParameterDirection.Input;
					if (string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["LEAD_SRC_FK"].ToString())) {
						_with7.Parameters.Add("LEAD_SRC_FK_IN", 1).Direction = ParameterDirection.Input;
					} else {
						_with7.Parameters.Add("LEAD_SRC_FK_IN", Convert.ToInt32(LS_DataSet.Tables[0].Rows[i]["LEAD_SRC_FK"])).Direction = ParameterDirection.Input;
					}
					_with7.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["CUSTOMER_MST_FK"].ToString()) ? "" : LS_DataSet.Tables[0].Rows[i]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
					_with7.Parameters.Add("CONTACT_IN", (string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["CONTACT"].ToString()) ? "" : LS_DataSet.Tables[0].Rows[i]["CONTACT"])).Direction = ParameterDirection.Input;
					_with7.Parameters.Add("LEAD_SOURCE_INFO_IN", (string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["LEAD_SOURCE_INFO"].ToString()) ? "" : LS_DataSet.Tables[0].Rows[i]["LEAD_SOURCE_INFO"])).Direction = ParameterDirection.Input;

					_with7.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
					_with7.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

					_with7.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					_with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

					var _with8 = updCommand;
					_with8.Connection = objWK.MyConnection;
					_with8.CommandType = CommandType.StoredProcedure;
					_with8.CommandText = objWK.MyUserName + ".SALES_LEAD_SRC_TRN_PKG.SALES_LEAD_SRC_TRN_UPD";
					_with8.Parameters.Clear();

					_with8.Parameters.Add("SALES_LEAD_INFO_PK_IN", LS_DataSet.Tables[0].Rows[i]["SALES_LEAD_INFO_PK"]).Direction = ParameterDirection.Input;
					_with8.Parameters.Add("SALES_LEAD_HDR_FK_IN", SalesLeadPK).Direction = ParameterDirection.Input;
					if (string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["LEAD_SRC_FK"].ToString())) {
						_with8.Parameters.Add("LEAD_SRC_FK_IN", 1).Direction = ParameterDirection.Input;
					} else {
						_with8.Parameters.Add("LEAD_SRC_FK_IN", Convert.ToInt32(LS_DataSet.Tables[0].Rows[i]["LEAD_SRC_FK"])).Direction = ParameterDirection.Input;
					}
					_with8.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["CUSTOMER_MST_FK"].ToString()) ? "" : LS_DataSet.Tables[0].Rows[i]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
					_with8.Parameters.Add("CONTACT_IN", (string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["CONTACT"].ToString()) ? "" : LS_DataSet.Tables[0].Rows[i]["CONTACT"])).Direction = ParameterDirection.Input;
					_with8.Parameters.Add("LEAD_SOURCE_INFO_IN", (string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["LEAD_SOURCE_INFO"].ToString()) ? "" : LS_DataSet.Tables[0].Rows[i]["LEAD_SOURCE_INFO"])).Direction = ParameterDirection.Input;

					_with8.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
					_with8.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

					_with8.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    

					var _with9 = objWK.MyDataAdapter;

					SalesCommentsPK = (string.IsNullOrEmpty(LS_DataSet.Tables[0].Rows[i]["SALES_LEAD_INFO_PK"].ToString()) ? 0 : Convert.ToInt32(LS_DataSet.Tables[0].Rows[i]["SALES_LEAD_INFO_PK"]));

					if (SalesCommentsPK <= 0) {
						_with9.InsertCommand = insCommand;
						_with9.InsertCommand.Transaction = TRAN;
						RecAfct = RecAfct + _with9.InsertCommand.ExecuteNonQuery();
					} else {
						_with9.UpdateCommand = updCommand;
						_with9.UpdateCommand.Transaction = TRAN;
						RecAfct = RecAfct + _with9.UpdateCommand.ExecuteNonQuery();
					}

				}

				return RecAfct;

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region " Protocol "
		public string GenerateServiceNo(long nLocationId, long nEmployeeId)
		{
			string functionReturnValue = null;

			functionReturnValue = GenerateProtocolKey("SALES LEAD", nLocationId, nEmployeeId, DateTime.Now, "","" ,"" , Convert.ToInt32(HttpContext.Current.Session["USER_PK"]));

			return functionReturnValue;
			return functionReturnValue;

		}
		#endregion

	}
}
