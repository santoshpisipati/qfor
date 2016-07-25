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
    public class cls_Multi_Lingual_setting : CommonFeatures
	{

		#region "Fetch Grid Details based on Country Selection "
		public DataSet FetchCaptions(long CountryPK)
		{
			WorkFlow objWF = new WorkFlow();
			string strSQL = null;
			strSQL = "select rownum as SLNR,";
			strSQL += "hdr.REPORT_CAPTION_MAPPER_PK,";
			strSQL += "hdr.REPORT_DEFAULT_CAPTION,";
			strSQL += "cntrdtl.REPORT_CAPTION_CNTRY_PK,";
			strSQL += "cntrdtl.REPORT_COUNTRY_CAPTION,";
			strSQL += "cntrdtl.FONT_NAME,cntrdtl.version_no";
			strSQL += "from report_caption_mapper_hdr hdr, report_caption_cntry_trn cntrdtl";
			strSQL += "where cntrdtl.report_capton_mapper_hdr_fk(+) = hdr.report_caption_mapper_pk";
			strSQL += "  and cntrdtl.country_mst_fk(+) = " + CountryPK;
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
		public DataSet FetchCaptions_Newmode()
		{
			WorkFlow objWF = new WorkFlow();
			string strSQL = null;
			strSQL += "SELECT ROWNUM SLNR,Q.* FROM ";
			strSQL += "(SELECT  ";
			strSQL += "hdr.REPORT_CAPTION_MAPPER_PK,";
			strSQL += "hdr.REPORT_DEFAULT_CAPTION,";
			strSQL += "'' REPORT_CAPTION_CNTRY_PK,";
			strSQL += "'' REPORT_COUNTRY_CAPTION,";
			strSQL += "'' FONT_NAME,'' version_no";
			strSQL += "from report_caption_mapper_hdr hdr ";
			strSQL += " ORDER BY REPORT_DEFAULT_CAPTION) Q  ";
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

		#region "Fetch Country Details "
		public DataSet FetchCountry(long CountryPK, long LOCPK, string adminusr)
		{
			WorkFlow objWF = new WorkFlow();
			string strSQL = null;
			strSQL = "select country.country_mst_pk,";
			strSQL += "country.country_id,";
			strSQL += "country.country_name,null FONT_NAME,";
			strSQL += "(CASE WHEN country.country_mst_pk = " + CountryPK + "  THEN";
			strSQL += "  '1' ";
			strSQL += " ELSE";
			strSQL += " null END ) Sel";
			strSQL += "from country_mst_tbl country,location_mst_tbl loc";
			strSQL += "where country.country_mst_pk  not in (select distinct dtl.country_mst_fk from report_caption_cntry_trn dtl where  dtl.country_mst_fk = " + CountryPK + " )";
			strSQL += "and country.active_flag = 1";
			if (adminusr != "1") {
				strSQL += " and loc.location_mst_pk = " + LOCPK;
			}
			strSQL += "and loc.country_mst_fk = country.country_mst_pk";
			strSQL += " union";
			strSQL += " select country.country_mst_pk,";
			strSQL += "country.country_id,";
			strSQL += "country.country_name,trn.font_name FONT_NAME,";
			strSQL += "'1' Sel";
			strSQL += "from country_mst_tbl country,report_caption_cntry_trn trn ";
			strSQL += "where country.country_mst_pk = " + CountryPK;
			strSQL += "and country.active_flag = 1 and country.country_mst_pk = trn.country_mst_fk(+)";
			strSQL += "ORDER BY COUNTRY_ID";
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

		public DataSet FetchCountry_newmode(long LOCPK, string adminusr)
		{
			WorkFlow objWF = new WorkFlow();
			string strSQL = null;
			strSQL = "select distinct country.country_mst_pk,";
			strSQL += "country.country_id,";
			strSQL += "country.country_name,null FONT_NAME,";
			strSQL += " null Sel";
			strSQL += "from country_mst_tbl country,location_mst_tbl loc";
			strSQL += "where country.active_flag = 1";
			if (adminusr == "0") {
				strSQL += " and loc.location_mst_pk = " + LOCPK;
			}
			strSQL += "and loc.country_mst_fk = country.country_mst_pk";
			strSQL += "ORDER BY COUNTRY_ID";
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

		public DataSet ExistingCountryPK()
		{
			WorkFlow objWF = new WorkFlow();
			string strSQL = null;
			strSQL += "select distinct cntrcaption.country_mst_fk";
			strSQL += "from  report_caption_cntry_trn cntrcaption  ";
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

		#region "delete"
		public bool delete(string CountryPK)
		{
			WorkFlow objWF = new WorkFlow();
			string strSQL = null;
			strSQL = "delete report_caption_cntry_trn cntrytrn where cntrytrn.country_mst_fk in(" + CountryPK + ")";
			try {
				return objWF.ExecuteCommands(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

		#endregion

		#region "Insert "
		public ArrayList Insert(DataSet M_DATASET, long CountryPK, long ConfigurationPK, string FontName)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			OracleCommand insCommand = new OracleCommand();
			try {
				for (int i = 0; i <= M_DATASET.Tables[0].Rows.Count - 1; i++) {
					var _with1 = insCommand;
					_with1.Parameters.Clear();
					_with1.Connection = objWK.MyConnection;
					_with1.CommandType = CommandType.StoredProcedure;
					_with1.CommandText = objWK.MyUserName + ".REPORT_CAPTION_CNTRY_TRN_PKG.REPORT_CAPTION_CNTRY_TRN_INS";
					var _with2 = _with1.Parameters;
					insCommand.Parameters.Add("REPORT_CAPTON_MAPPER_HDR_FK_IN", M_DATASET.Tables[0].Rows[i]["REPORT_CAPTION_MAPPER_PK"]).Direction = ParameterDirection.Input;

					insCommand.Parameters.Add("REPORT_COUNTRY_CAPTION_IN", M_DATASET.Tables[0].Rows[i]["REPORT_COUNTRY_CAPTION"]).Direction = ParameterDirection.Input;

					insCommand.Parameters.Add("created_by_fk_in", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

					insCommand.Parameters.Add("COUNTRY_MST_FK_IN", CountryPK).Direction = ParameterDirection.Input;

					if ((FontName != null)) {
						insCommand.Parameters.Add("FONT_NAME_IN", FontName).Direction = ParameterDirection.Input;
					} else {
						insCommand.Parameters.Add("FONT_NAME_IN", "").Direction = ParameterDirection.Input;
					}
					insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

					insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
					var _with3 = objWK.MyDataAdapter;
					_with3.InsertCommand = insCommand;
					_with3.InsertCommand.Transaction = TRAN;
					_with3.InsertCommand.ExecuteNonQuery();
				}
				TRAN.Commit();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				TRAN.Rollback();
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				TRAN.Rollback();
				arrMessage.Add(ex.Message);
				return arrMessage;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion

		#region "Update "
		public ArrayList Update(DataSet M_DATASET, long CountryPK, long ConfigurationPK)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			try {
				objWK.MyCommand.Transaction = TRAN;
				foreach (DataRow row in M_DATASET.Tables[0].Rows) {
					var _with4 = objWK.MyCommand;
					_with4.CommandType = CommandType.StoredProcedure;
					_with4.CommandText = objWK.MyUserName + ".REPORT_CAPTION_CNTRY_TRN_PKG.REPORT_CAPTION_CNTRY_TRN_UPD";
					var _with5 = _with4.Parameters;
					_with5.Clear();
					_with5.Add("REPORT_CAPTION_CNTRY_PK_IN", row["REPORT_CAPTION_CNTRY_PK"]).Direction = ParameterDirection.Input;

					_with5.Add("REPORT_CAPTON_MAPPER_HDR_FK_IN", row["REPORT_CAPTION_MAPPER_PK"]).Direction = ParameterDirection.Input;

					_with5.Add("REPORT_COUNTRY_CAPTION_IN", row["REPORT_COUNTRY_CAPTION"]).Direction = ParameterDirection.Input;

					_with5.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

					_with5.Add("VERSION_NO_IN", row["VERSION_NO"]).Direction = ParameterDirection.Input;

					_with5.Add("COUNTRY_MST_FK_IN", CountryPK).Direction = ParameterDirection.Input;

					_with5.Add("FONT_NAME_IN", row["FONT_NAME"]).Direction = ParameterDirection.Input;

					_with5.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

					_with5.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					_with4.ExecuteNonQuery();
				}
				TRAN.Commit();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				TRAN.Rollback();
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				TRAN.Rollback();
				arrMessage.Add(ex.Message);
				return arrMessage;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion

		#region "Other Functions"
		public bool CaptionCheckForCountry(int CountryPk)
		{
			int CaptionExist = 0;
			WorkFlow objwf = new WorkFlow();
			CaptionExist = Convert.ToInt32(objwf.ExecuteScaler("SELECT COUNT(*) FROM REPORT_CAPTION_CNTRY_TRN RC WHERE RC.COUNTRY_MST_FK=" + CountryPk));
			if (CaptionExist > 0)
				return true;
			else
				return false;
		}
		#endregion
	}
}
