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
    public class Cls_SplitBL : CommonFeatures
	{

		#region "Private Variables"
		private long _PkValue;
			#endregion
		private long _JOBPKVALUE;

		#region "Property"
		public long PkValue {
			get { return _PkValue; }
		}
		public long JOBPKVALUE {
			get { return _JOBPKVALUE; }
		}

		#endregion

		//
		// This region contains public function used for fetching details of Job Card and Split BL. 
		// FetchJobCardDtls(JobCardPk) :- returns details of Job Card in DataTable
		// FetchSplitBLDetails(SplitBLpk) :- returns details of Split BL in DataTable
		// FetchSelectedBLDetails(SplitBLpk) :- returns details of selected BL in DataTable
		#region " Fetch Job Card Details "
		// This function is called while editing or viewing the record.
		// Param name="JobCardPk" :- Primary Key of Job card sent ByValue        
		// Param name="Check_Split_BL" :- Check whether Split_BL table to be checked while fetching
		// Exception cref="sqlExp" :- Catch SQL Exception
		public DataTable FetchJobCardDtls(long JobCardPk, Int16 Check_Split_BL)
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();

			try {
				objWF.MyCommand.Parameters.Clear();
				var _with1 = objWF.MyCommand.Parameters;
				_with1.Add("JOB_CARD_SEA_IMP_PK_IN", JobCardPk).Direction = ParameterDirection.Input;
				_with1.Add("CHECK_SPLIT_BL_IN", Check_Split_BL).Direction = ParameterDirection.Input;
				_with1.Add("BL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				return objWF.GetDataTable("FETCH_SPLIT_BL_PKG", "FETCH_BL_DATA");
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}

		// This function is called while viewing the record in frmViewSplitBL.aspx
		// Param name="SplitBLpk"  :- Primary Key of Split BL table, sent ByValue        
		// Exception cref="sqlExp" :- Catch SQL Exception
		public DataTable FetchSplitBLDetails(long SplitBLpk)
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with2 = objWF.MyCommand.Parameters;
				_with2.Add("JOB_CARD_SEA_IMP_PK_IN", SplitBLpk).Direction = ParameterDirection.Input;
				_with2.Add("BL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataTable("FETCH_SPLIT_BL_PKG", "FETCH_SPLITBL_DATA");
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}

		// This function is called while viewing selected BL from the frmViewSplitBL.aspx
		// Param name="SplitBLpk"      :- Primary Key of Split BL table, sent ByValue        
		// Exception cref="sqlExp" :- Catch SQL Exception
		public DataTable FetchSelectedBLDetails(long SplitBLpk)
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with3 = objWF.MyCommand.Parameters;
				_with3.Add("SPLIT_BL_PK_IN", SplitBLpk).Direction = ParameterDirection.Input;
				_with3.Add("BL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataTable("FETCH_SPLIT_BL_PKG", "FETCH_SELECTEDBL_DATA");
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		// This function is called while viewing selected BL from the frmViewSplitBL.aspx
		// Param name="JobCardpk"  :- Foreign Key of Job Card, sent ByValue        
		// Exception cref="sqlExp" :- Catch SQL Exception
		public DataSet Job_Card_And_BL_Dtls(long JobCardpk)
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with4 = objWF.MyCommand.Parameters;
				_with4.Add("JOB_CARD_SEA_IMP_PK_IN", JobCardpk).Direction = ParameterDirection.Input;
				_with4.Add("BL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataSet("FETCH_SPLIT_BL_PKG", "JOB_CARD_AND_BL_DTLS");
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}

		// This function is called while viewing selected BL from the frmViewSplitBL.aspx
		// Param name="JobCardpk"  :- Foreign Key of Job Card, sent ByValue        
		// Exception cref="sqlExp" :- Catch SQL Exception
		public DataSet Job_Card_Cont_Dtls(long JobCardpk)
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			try {
				//objWF.MyCommand.Parameters.Clear()
				//With objWF.MyCommand.Parameters
				//    .Add("JOB_CARD_SEA_IMP_PK_IN", JobCardpk).Direction = ParameterDirection.Input
				//    .Add("BL_CUR", OracleClient.OracleDbType.RefCursor).Direction = ParameterDirection.Output
				//End With
				strSql = "SELECT JJ.JOB_CARD_TRN_PK JOBPK," + 
                    "       JJ.JOBCARD_REF_NO JOBREFNO," + 
                    "       JJC.CONTAINER_NUMBER CONTAINER," + 
                    "       JJC.VOLUME_IN_CBM VOLUME," + 
                    "       JJC.PACK_COUNT PIECES," + 
                    "       JJC.GROSS_WEIGHT WEIGHT" + 
                    "  FROM JOB_CARD_TRN JJ, JOB_TRN_CONT JJC" +
                    " WHERE JJ.JOB_CARD_TRN_PK = JJC.JOB_CARD_TRN_FK" + 
                    " AND JJ.JOB_CARD_TRN_PK =" + JobCardpk +
                    " ORDER BY JJ.JOB_CARD_TRN_PK";

				return objWF.GetDataSet(strSql);
				//Return objWF.GetDataSet("FETCH_SPLIT_BL_PKG", "JOB_CARD_CONT_DTLS")
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		// 
		// This function saves data in SPLIT_BL_TBL
		#region " Save "
		public ArrayList SaveBL(DataTable dtSplitBL, DataSet Contds, string Status)
		{
			int RecAfct = 0;
			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;

			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();
			arrMessage.Clear();


			try {
				objWK.MyCommand.Parameters.Clear();
				objWK.MyCommand.CommandType = CommandType.StoredProcedure;

				var _with5 = objWK.MyCommand;

				if (Status == "EDIT") {
					//*************Split BL PK*************
					_with5.Parameters.Add("SPLIT_BL_PK_IN", OracleDbType.Int32, 10, "SPLIT_BL_PK").Direction = ParameterDirection.Input;
					_with5.Parameters["SPLIT_BL_PK_IN"].SourceVersion = DataRowVersion.Current;
					//Chandra
					_with5.Parameters.Add("JOB_CARD_SEA_IMP_PK_IN", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_PK").Direction = ParameterDirection.Input;
					_with5.Parameters["JOB_CARD_SEA_IMP_PK_IN"].SourceVersion = DataRowVersion.Current;

					//*************Version No*************
					_with5.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
					_with5.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
					//Chandra
					_with5.Parameters.Add("JOB_VERSION_NO_IN", OracleDbType.Int32, 10, "JOB_VERSION_NO").Direction = ParameterDirection.Input;
					_with5.Parameters["JOB_VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

					//*************Modified By*************
					_with5.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					//*************Return Value*************
					_with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
					//Chandra
					_with5.Parameters.Add("JOB_RETURN_VALUE", OracleDbType.Int32, 10, "JOB_RETURN_VALUE").Direction = ParameterDirection.Output;


				} else if (Status == "NEW") {
					//*************Created By*************
					_with5.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					//*************Reference Int32*************
					_with5.Parameters.Add("REFERENCE_NO_IN", OracleDbType.Varchar2, 22, "REFERENCE_NO").Direction = ParameterDirection.Input;
					_with5.Parameters["REFERENCE_NO_IN"].SourceVersion = DataRowVersion.Current;
					//*************Return Value*************
					_with5.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					//Chandra
					_with5.Parameters.Add("JOB_RETURN_VALUE", OracleDbType.Int32, 10, "JOB_RETURN_VALUE").Direction = ParameterDirection.Output;

				}
				//*************Job Card FK*************
				_with5.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_FK").Direction = ParameterDirection.Input;
				_with5.Parameters["JOB_CARD_SEA_IMP_FK_IN"].SourceVersion = DataRowVersion.Current;
				//*************Marks and Numbers*************
				_with5.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
				_with5.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;
				//*************Party Name FK*************
				_with5.Parameters.Add("PARTY_NAME_IN", OracleDbType.Int32, 10, "PARTY_NAME").Direction = ParameterDirection.Input;
				_with5.Parameters["PARTY_NAME_IN"].SourceVersion = DataRowVersion.Current;
				//*************Goods Description*************
				_with5.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "GOODS_DESCRIPTION").Direction = ParameterDirection.Input;
				_with5.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;
				//*************Remarks*************
				_with5.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 2000, "REMARKS").Direction = ParameterDirection.Input;
				_with5.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
				//*************Weight and Volume*************
				_with5.Parameters.Add("WEIGHT_VOLUME_IN", OracleDbType.Varchar2, 2000, "WEIGHT_VOLUME").Direction = ParameterDirection.Input;
				_with5.Parameters["WEIGHT_VOLUME_IN"].SourceVersion = DataRowVersion.Current;
				//*************Splits required*************
				_with5.Parameters.Add("SPLITS_REQUIRED_IN", OracleDbType.Int32, 2, "SPLITS_REQUIRED").Direction = ParameterDirection.Input;
				_with5.Parameters["SPLITS_REQUIRED_IN"].SourceVersion = DataRowVersion.Current;
				//*************HBL Int32*************
				_with5.Parameters.Add("HBL_NO_IN", OracleDbType.Varchar2, 22, "HBL_NO").Direction = ParameterDirection.Input;
				_with5.Parameters["HBL_NO_IN"].SourceVersion = DataRowVersion.Current;
				//*************Config Pk*************
				_with5.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

				var _with6 = objWK.MyDataAdapter;
				if (!(Status == "EDIT")) {
					objWK.MyCommand.CommandText = objWK.MyUserName + "." + "SPLIT_BL_TBL_PKG1.SPLIT_BL_TBL_INS";
					_with6.InsertCommand = objWK.MyCommand;
					_with6.InsertCommand.Transaction = TRAN;

				} else {
					objWK.MyCommand.CommandText = objWK.MyUserName + "." + "SPLIT_BL_TBL_PKG1.SPLIT_BL_TBL_UPD";
					_with6.UpdateCommand = objWK.MyCommand;
					_with6.UpdateCommand.Transaction = TRAN;
				}
				RecAfct = _with6.Update(dtSplitBL);

				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					if (string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "deleted") > 0 | string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "Modified") > 0) {
						arrMessage.Add(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString());
						TRAN.Rollback();
						return arrMessage;
					} else {
						_PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
						_JOBPKVALUE = Convert.ToInt64(objWK.MyCommand.Parameters["JOB_RETURN_VALUE"].Value);
						if (Status == "EDIT") {
							deleteConjob(_JOBPKVALUE);
						}
						arrMessage.Add(SaveJobContainer(Contds, _JOBPKVALUE, objWK, Status));
						arrMessage.Add(SaveSplitContainer(Contds, _PkValue, objWK, Status));
					}
					arrMessage.Add("All data saved successfully");
					TRAN.Commit();
					return arrMessage;
				}
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				TRAN.Rollback();
				throw ex;
			} finally {
				objWK.MyConnection.Close();
			}
		}
		#region "save container details"

		public ArrayList SaveJobContainer(DataSet ds, long JobCardPK, WorkFlow obj, string Status)
		{
			DataRow dr = null;
			Int32 RecAfct = default(Int32);
			try {
				var _with7 = obj.MyCommand;
				foreach (DataRow dr_loopVariable in ds.Tables[0].Rows) {
					dr = dr_loopVariable;
					_with7.Connection = obj.MyConnection;
					_with7.CommandType = CommandType.StoredProcedure;
					_with7.CommandText = obj.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_CONT_INS";
					//JOB_CARD_SEA_IMP_FK_IN()
					_with7.Parameters.Clear();
					_with7.Parameters.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
					//CONTAINER_NUMBER_IN()
					_with7.Parameters.Add("CONTAINER_NUMBER_IN", dr["CONTAINER_NUMBER"]).Direction = ParameterDirection.Input;
					_with7.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;
					//CONTAINER_TYPE_MST_FK_IN()
					_with7.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", (Convert.ToInt32(dr["CONTAINER_TYPE_MST_FK"]) == 0 ? "" : dr["CONTAINER_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
					_with7.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//SEAL_NUMBER_IN()
					_with7.Parameters.Add("SEAL_NUMBER_IN", dr["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
					_with7.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;
					//VOLUME_IN_CBM_IN()
					_with7.Parameters.Add("VOLUME_IN_CBM_IN", dr["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
					_with7.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;
					//GROSS_WEIGHT_IN()
					_with7.Parameters.Add("GROSS_WEIGHT_IN", dr["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
					_with7.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
					//NET_WEIGHT_IN()
					_with7.Parameters.Add("NET_WEIGHT_IN", dr["NET_WEIGHT"]).Direction = ParameterDirection.Input;
					_with7.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
					//CHARGEABLE_WEIGHT_IN()
					_with7.Parameters.Add("CHARGEABLE_WEIGHT_IN", dr["CHARGEABLE_WEIGHT"]).Direction = ParameterDirection.Input;
					_with7.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
					//PACK_TYPE_MST_FK_IN()
					_with7.Parameters.Add("PACK_TYPE_MST_FK_IN", (Convert.ToInt32(dr["PACK_TYPE_MST_FK"]) == 0 ? "" : dr["PACK_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
					_with7.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//PACK_COUNT_IN()
					_with7.Parameters.Add("PACK_COUNT_IN", dr["PACK_COUNT"]).Direction = ParameterDirection.Input;
					_with7.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;
					//COMMODITY_MST_FK_IN()
					if (Convert.ToInt32(dr["COMMODITY_MST_FK"]) == 0) {
						_with7.Parameters.Add("COMMODITY_MST_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with7.Parameters.Add("COMMODITY_MST_FK_IN", (Convert.ToInt32(dr["COMMODITY_MST_FK"]) == 0 ? "" : dr["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;
					}
					_with7.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//GEN_LAND_DATE_IN()

					//adding by thiyagarajan on 11/2/09
					_with7.Parameters.Add("COMMODITY_MST_FKS_IN", "").Direction = ParameterDirection.Input;
					_with7.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;
					//end

					if (string.IsNullOrEmpty(dr["GEN_LAND_DATE"].ToString())) {
						_with7.Parameters.Add("GEN_LAND_DATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with7.Parameters.Add("GEN_LAND_DATE_IN", Convert.ToString(dr["GEN_LAND_DATE"]).Substring(0, 10).Trim()).Direction = ParameterDirection.Input;
					}
					_with7.Parameters["GEN_LAND_DATE_IN"].SourceVersion = DataRowVersion.Current;
					//RETURN_VALUE()
					_with7.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					_with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
					_with7.ExecuteNonQuery();
				}
				arrMessage.Add("All data saved successfully");
				return arrMessage;

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public ArrayList SaveSplitContainer(DataSet ds, long SplitPk, WorkFlow obj, string Status)
		{

			DataRow dr = null;

			try {
				var _with8 = obj.MyCommand;
				foreach (DataRow dr_loopVariable in ds.Tables[0].Rows) {
					dr = dr_loopVariable;
					_with8.Parameters.Clear();
					_with8.Connection = obj.MyConnection;
					_with8.CommandType = CommandType.StoredProcedure;
					if (!(Status == "EDIT")) {
						_with8.CommandText = obj.MyUserName + ".SPLIT_BL_TBL_PKG1.SPLIT_BL_TRN_CONT_INS";
					} else {
						_with8.CommandText = obj.MyUserName + ".SPLIT_BL_TBL_PKG1.SPLIT_BL_TRN_CONT_UPD";
					}
					//SPLIT_BL_FK_IN()
					_with8.Parameters.Add("SPLIT_BL_FK_IN", SplitPk).Direction = ParameterDirection.Input;
					//CONTAINER_NUMBER_IN()
					_with8.Parameters.Add("CONTAINER_NUMBER_IN", dr["CONTAINER_NUMBER"]).Direction = ParameterDirection.Input;
					_with8.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;
					//CONTAINER_TYPE_MST_FK_IN()
					_with8.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", (Convert.ToInt32(dr["CONTAINER_TYPE_MST_FK"]) == 0 ? "" : dr["CONTAINER_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
					_with8.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//SEAL_NUMBER_IN()
					_with8.Parameters.Add("SEAL_NUMBER_IN", dr["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
					_with8.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;
					//VOLUME_IN_CBM_IN()
					_with8.Parameters.Add("VOLUME_IN_CBM_IN", dr["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
					_with8.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;
					//GROSS_WEIGHT_IN()
					_with8.Parameters.Add("GROSS_WEIGHT_IN", dr["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
					_with8.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
					//NET_WEIGHT_IN()
					_with8.Parameters.Add("NET_WEIGHT_IN", dr["NET_WEIGHT"]).Direction = ParameterDirection.Input;
					_with8.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
					//CHARGEABLE_WEIGHT_IN()
					_with8.Parameters.Add("CHARGEABLE_WEIGHT_IN", dr["CHARGEABLE_WEIGHT"]).Direction = ParameterDirection.Input;
					_with8.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
					//PACK_TYPE_MST_FK_IN()
					_with8.Parameters.Add("PACK_TYPE_MST_FK_IN", (Convert.ToInt32(dr["PACK_TYPE_MST_FK"]) == 0 ? "" : dr["PACK_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
					_with8.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//PACK_COUNT_IN()
					_with8.Parameters.Add("PACK_COUNT_IN", dr["PACK_COUNT"]).Direction = ParameterDirection.Input;
					_with8.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;
					//COMMODITY_MST_FK_IN()
					if (Convert.ToInt32(dr["COMMODITY_MST_FK"]) == 0) {
						_with8.Parameters.Add("COMMODITY_MST_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with8.Parameters.Add("COMMODITY_MST_FK_IN", (Convert.ToInt32(dr["COMMODITY_MST_FK"]) == 0 ? "" : dr["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;
					}
					_with8.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//GEN_LAND_DATE_IN()
					if (string.IsNullOrEmpty(dr["GEN_LAND_DATE"].ToString())) {
						_with8.Parameters.Add("GEN_LAND_DATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with8.Parameters.Add("GEN_LAND_DATE_IN", Convert.ToString(dr["GEN_LAND_DATE"]).Substring(0, 10).Trim()).Direction = ParameterDirection.Input;
					}
					_with8.Parameters["GEN_LAND_DATE_IN"].SourceVersion = DataRowVersion.Current;
					_with8.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					_with8.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
					_with8.ExecuteNonQuery();
				}
				arrMessage.Add("All data saved successfully");
				return arrMessage;

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		#endregion
		public DataSet fun1(long pk)
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			strSql = "SELECT JOB.JOBCARD_REF_NO,";
			strSql += "JOB.VESSEL_NAME || '/' || JOB.VOYAGE_FLIGHT_NO AS VSL_VOY,";
			strSql += "POL.PORT_NAME AS POL,";
			strSql += " POD.PORT_NAME AS POD,";
			strSql += " SH.CUSTOMER_NAME AS SHIPPER,";
			strSql += "CN.CUSTOMER_NAME AS CONSIGNEE,";
			strSql += "CMMGRP.COMMODITY_GROUP_DESC,";
			strSql += "BL.REFERENCE_NO,";
			strSql += " BL.HBL_NO,";
			strSql += "PARTY.CUSTOMER_NAME,";
			strSql += "BL.MARKS_NUMBERS,";
			strSql += "BL.GOODS_DESCRIPTION,";
			strSql += "BL.WEIGHT_VOLUME,";
			strSql += "BL.REMARKS";
			strSql += " FROM JOB_CARD_TRN    JOB,";
			strSql += "PORT_MST_TBL    POL,";
			strSql += "PORT_MST_TBL            POD,";
			strSql += "CUSTOMER_MST_TBL        SH,";
			strSql += "CUSTOMER_MST_TBL        CN,";
			strSql += " CUSTOMER_MST_TBL        PARTY,";
			strSql += "COMMODITY_GROUP_MST_TBL CMMGRP,";
			strSql += " SPLIT_BL_TBL BL";
			strSql += "WHERE JOB.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)";
			strSql += " AND JOB.CONSIGNEE_CUST_MST_FK = CN.CUSTOMER_MST_PK(+)";
			strSql += " AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)";
			strSql += " AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
			strSql += " AND JOB.COMMODITY_GROUP_FK = CMMGRP.COMMODITY_GROUP_PK(+)";
			strSql += " AND JOB.JOB_CARD_TRN_PK = BL.JOB_CARD_SEA_IMP_FK(+)";
			strSql += " AND BL.PARTY_NAME = PARTY.CUSTOMER_MST_PK";
			strSql += " AND  JOB.JOB_CARD_TRN_PK =" + pk;
			strSql += " ORDER BY JOB.JOBCARD_REF_NO";

			try {
				return (objWF.GetDataSet(strSql));
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#region "Get container data"
		public DataSet GetContainerData(string jobCardPK = "0")
		{

			WorkFlow objWF = new WorkFlow();
			StringBuilder SQL = new StringBuilder();

			SQL.Append("SELECT ");
			SQL.Append("      JOB_TRN_CONT_PK,");
			SQL.Append("      container_number,");
			SQL.Append("      container_type_mst_fk,");
			SQL.Append("      seal_number,");
			SQL.Append("      volume_in_cbm,");
			SQL.Append("      gross_weight,");
			SQL.Append("      net_weight,");
			SQL.Append("      chargeable_weight,");
			SQL.Append("      pack_type_mst_fk,");
			SQL.Append("      pack_count,");
			SQL.Append("      commodity_name,");
			SQL.Append("      gen_land_date,");
			SQL.Append("      container_type_mst_id,");
			SQL.Append("      commodity_mst_fk,");
			SQL.Append("     'false' selected");
			SQL.Append("FROM");
			SQL.Append("      JOB_TRN_CONT cont_trn,");
			SQL.Append("      JOB_CARD_TRN job_card,");
			SQL.Append("      container_type_mst_tbl cont,");
			SQL.Append("      pack_type_mst_tbl pack,");
			SQL.Append("      commodity_mst_tbl comm");
			SQL.Append("WHERE");
			SQL.Append("      cont_trn.JOB_CARD_TRN_FK =" + jobCardPK);
			SQL.Append("      AND cont_trn.JOB_CARD_TRN_FK = job_card.JOB_CARD_TRN_PK");
			SQL.Append("      AND cont_trn.container_type_mst_fk = cont.container_type_mst_pk(+)");
			SQL.Append("      AND cont_trn.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
			SQL.Append("      AND cont_trn.commodity_mst_fk = comm.commodity_mst_pk(+)");

			try {
				return objWF.GetDataSet(SQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		public DataSet GetContainerDataBeforSave(string JobCardPk = "0")
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			DataTable dt = null;


			try {
				objWF.MyCommand.Parameters.Clear();
				var _with9 = objWF.MyCommand.Parameters;
				_with9.Add("JOB_CARD_SEA_IMP_PK_IN", JobCardPk).Direction = ParameterDirection.Input;
				_with9.Add("BL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				return objWF.GetDataSet("FETCH_SPLIT_BL_PKG", "JOB_CARD_CONT_DTLS_GRID");
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet GetContainerDataAfterSave(string splitPk = "0", string JobCardPk = "0")
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with10 = objWF.MyCommand.Parameters;
				_with10.Add("SPLIT_PK_IN", splitPk).Direction = ParameterDirection.Input;
				_with10.Add("JOB_CARD_SEA_IMP_PK_IN", JobCardPk).Direction = ParameterDirection.Input;
				_with10.Add("BL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataSet("FETCH_SPLIT_BL_PKG", "SPLIT_CONT_DTLS_GRID");
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		public DataSet Job_Card_PK(string txtJobCard)
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			strSql = " SELECT JSI.JOB_CARD_TRN_PK JOBPK,JSI.VERSION_NO VERSION FROM JOB_CARD_TRN JSI WHERE JSI.JOBCARD_REF_NO ='" + txtJobCard + "'";
			try {
				return (objWF.GetDataSet(strSql));
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public void deleteConjob(long jobcardPK)
		{
			try {
				WorkFlow objWF = new WorkFlow();
				objWF.ExecuteCommands("DELETE FROM JOB_TRN_CONT where JOB_CARD_TRN_FK=" + jobcardPK);
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
	}
}
