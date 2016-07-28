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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_RemovalCreditNote : CommonFeatures
	{

		#region "Property"
		long lngCreditNotePk;
		public long ReturnSavePk {
			get { return lngCreditNotePk; }
			set { lngCreditNotePk = value; }
		}
		#endregion

		#region "Fetch Document Type "
		public DataSet FetchDocType()
		{
			try {
				string StrSql = null;
				WorkFlow objWF = new WorkFlow();
				StrSql = "Select  Document_Type_Mst_Fk,Document_Type  From Document_Type_Mst_Tbl";
				return objWF.GetDataSet(StrSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#endregion

		#region "For Enhance Search "
		public string Fetch_Credit_Nr(string strCond, string loc = "0")
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand cmd = new OracleCommand();
			string strReturn = null;
			Array arr = null;

			string strSearchIn = null;
			string strReq = null;

			string strbiz = null;
			string strproces = null;
			string strcredittype = null;


			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
			strSearchIn = Convert.ToString(arr.GetValue(1));
            strbiz = Convert.ToString(arr.GetValue(2));
            strproces = Convert.ToString(arr.GetValue(3));
            strcredittype = Convert.ToString(arr.GetValue(5));

            try {
				objWF.OpenConnection();
				cmd.Connection = objWF.MyConnection;
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandText = objWF.MyUserName + ".EN_CREDIT_REF_NR.GETCREDITREF_COMMON";
				var _with1 = cmd.Parameters;
				_with1.Add("SEARCH_IN", getDefault(strSearchIn, "")).Direction = ParameterDirection.Input;
				_with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with1.Add("BUSINESS_TYPE_IN", strbiz).Direction = ParameterDirection.Input;
				_with1.Add("PROCESS_TYPE_IN", strproces).Direction = ParameterDirection.Input;
				_with1.Add("credit_note_type", strcredittype).Direction = ParameterDirection.Input;
				_with1.Add("LOCFK_IN", loc).Direction = ParameterDirection.Input;
				_with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				cmd.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				cmd.ExecuteNonQuery();
				strReturn = Convert.ToString(cmd.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				cmd.Connection.Close();
			}



		}
		#endregion

		#region "Fetch_Grid_Credit_Listing"
		public DataSet Fetch_Parent_Grid_list(Int16 Credit_note_type = 0, string validFrom = "", string validTo = "", string InvRefNr = "", Int16 strCustomerPk = 0, string strcreditPk = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string Doctype = "", Int16 DocNr = 0,
		long usrLocFK = 0, Int32 flag = 0, string CreditRefNr = "")
		{


			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder strSql = new System.Text.StringBuilder();
			string strCondition = null;
			WorkFlow objWF = new WorkFlow();
			string strSQL1 = null;

			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			if (!string.IsNullOrEmpty(CreditRefNr)) {
				strCondition += " AND CNT.CREDIT_NOTE_REF_NR='" + CreditRefNr + "' ";
			}
			if (Convert.ToInt32(InvRefNr) != 0) {
				strCondition += " AND  cntt.INVOICE_TRN_FK= " + InvRefNr;
			}
			if (strCustomerPk != 0) {
				strCondition += " AND  CNT.CUSTOMER_MST_FK =" + strCustomerPk;
			}
			//strcreditPk
			if (Convert.ToInt32(strcreditPk) != 0) {
				strCondition += " AND cnt.CRN_TBL_PK =  " + strcreditPk;
			}

			if (Convert.ToInt32(Doctype) != 1) {
				strCondition += " AND  cnt.DOCUMENT_TYPE =" + Doctype;
			}


			if (validFrom.Length > 0 & validTo.Length > 0) {
				strCondition += " AND  cnt.CREDIT_NOTE_DATE  between to_date('" + validFrom + "','" + dateFormat + "') and to_date('" + validTo + "','" + dateFormat + "')";
			}


			if (flag == 0) {
				strCondition += " AND 1=2 ";
			}

			strQuery.Append("SELECT Count(*)" );
			strQuery.Append("  FROM REM_CREDIT_NOTE_TBL       CNT," );
			if (Convert.ToInt32(InvRefNr) != 0) {
				strQuery.Append("       REM_CREDIT_NOTE_TRN_TBL   cntt," );

			}
			strQuery.Append("       CUSTOMER_MST_TBL      CMT," );
			strQuery.Append("       USER_MST_TBL          UMT" );
			strQuery.Append("WHERE CNT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK" );
			strQuery.Append(" AND CNT.CREATED_BY_FK= UMT.USER_MST_PK");
			if (Convert.ToInt32(InvRefNr) != 0) {
				strQuery.Append("      and cntt.CRN_TRN_TBL_PK=cnt.CRN_TBL_PK" );

			}

			strQuery.Append("       AND cnt.CREDIT_NOTE_TYPE=" + Credit_note_type);
			strQuery.Append("       AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
			strQuery.Append("" );



			strQuery.Append(strCondition);
			strSQL1 = strQuery.ToString();


			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
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


			strQuery.Remove(0, strQuery.Length);


			strSql.Append(" select * from (" );
			strSql.Append("SELECT ROWNUM SR_NO,q.* FROM (" );

			strSql.Append("SELECT" );
			if (Convert.ToInt32(InvRefNr) != 0) {
				strSql.Append("  distinct   CNT.CUSTOMER_MST_FK Custumer_pk," );
			} else {
				strSql.Append("       CNT.CUSTOMER_MST_FK Custumer_pk," );
			}

			strSql.Append("       CMT.CUSTOMER_NAME Customer," );
			strSql.Append("       CNT.CRN_TBL_PK CRN_TBl," );
			strSql.Append("       CNT.CREDIT_NOTE_REF_NR Credit_note1," );
			strSql.Append("       TO_CHAR(CNT.CREDIT_NOTE_DATE,'" + dateFormat + "') CRedit_Note_date," );
			strSql.Append("       CNT.CRN_AMOUNT Amount," );
			strSql.Append("       ''DOCUMENT_PK," );
			strSql.Append("       ''DOCUMENT_TYPE," );
			strSql.Append("       ''DOCUMENT_REFRENCE_FK," );
			strSql.Append("       ''CURRENCY_MST_FK," );
			strSql.Append("       ''CURRENCY_NAME," );
			strSql.Append("       ''CRN_AMMOUNT," );
			strSql.Append("       'FALSE' SEL" );
			strSql.Append("  FROM REM_CREDIT_NOTE_TBL       CNT," );
			if (Convert.ToInt32(InvRefNr )!= 0) {
				strSql.Append("       REM_CREDIT_NOTE_TRN_TBL   cntt," );

			}
			strSql.Append("       CUSTOMER_MST_TBL      CMT," );
			strSql.Append("       USER_MST_TBL          UMT" );
			strSql.Append("WHERE CNT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK" );
			strSql.Append(" AND CNT.CREATED_BY_FK= UMT.USER_MST_PK");
			if (Convert.ToInt32(InvRefNr) != 0) {
				strSql.Append("      and cntt.crn_tbl_fk=cnt.crn_tbl_pk" );

			}
			strSql.Append("       AND cnt.CREDIT_NOTE_TYPE=" + Credit_note_type);
			strSql.Append("       AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
			strSql.Append("" );
			strSql.Append(strCondition);
			strSql.Append("  ORDER BY  CRN_TBL_PK  desc)Q)");
			strSql.Append(" WHERE SR_NO  Between " + start + " and " + last );

			string sql = null;
			sql = strSql.ToString();
			DataSet DS = new DataSet();

			try {
				DS = objWF.GetDataSet(sql);
				DataRelation CONTRel = null;

				DS.Tables.Add(FetchChildGrid_list(AllMasterPKs_list(DS), Credit_note_type, InvRefNr, Convert.ToString(strCustomerPk), Convert.ToInt16(strcreditPk), CurrentPage, TotalPage, Doctype, Convert.ToString(DocNr), validFrom,
				validTo, usrLocFK));

				CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CRN_TBl"], DS.Tables[1].Columns["CRNTBL"], true);
				CONTRel.Nested = true;
				DS.Relations.Add(CONTRel);
				return DS;

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}


		}

		#region " All Master Supplier PKs "
		private string AllMasterPKs_list(DataSet ds)
		{
			Int16 RowCnt = default(Int16);
			Int16 ln = default(Int16);
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			strBuilder.Append("-1,");
			for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++) {
				strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["CRN_TBl"]).Trim() + ",");
			}
			strBuilder.Remove(strBuilder.Length - 1, 1);
			return strBuilder.ToString();
		}
		#endregion


		public DataTable FetchChildGrid_list(string SUPPLIERPKs = "", Int16 Credit_note_type = 0, string InvRefNr = "", string strCustomerPk = "0", Int16 strcreditPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, string Doctype = "", string DocNr = "", string validFrom = "",
		string validTo = "", long usrLocFK = 0)
		{

			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder strSql = new System.Text.StringBuilder();
			string strCondition = null;
			WorkFlow objWF = new WorkFlow();
			int RowCnt = 0;
			int Rno = 0;
			int pk = 0;
			string strSQL1 = null;

			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);


			if (Convert.ToInt32(InvRefNr) != 0) {
				strCondition += " AND  cntt.INVOICE_TRN_FK= " + InvRefNr;
			}
			if (Convert.ToInt32(strCustomerPk) != 0) {
				strCondition += " AND  CNT.CUSTOMER_MST_FK =" + strCustomerPk;
			}
			//strcreditPk
			if (strcreditPk != 0) {
				strCondition += " AND cnt.CRN_TBL_PK =  " + strcreditPk;
			}

			if (Convert.ToInt32(Doctype) != 1) {
				strCondition += " AND  cnt.DOCUMENT_TYPE =" + Doctype;
			}


			if (validFrom.Length > 0 & validTo.Length > 0) {
				strCondition += " AND  cnt.CREDIT_NOTE_DATE  between to_date('" + validFrom + "','" + dateFormat + "') and to_date('" + validTo + "','" + dateFormat + "')";
			}


			strSql.Append(" select * from (" );
			strSql.Append("SELECT ROWNUM SR_NO,q.* FROM (" );
			strSql.Append("SELECT" );
			strSql.Append("        '' CUSTOMER_MST_FK," );
			strSql.Append("        '' CUSTOMER_NAME," );
			strSql.Append("        CNT.CRN_TBL_PK CRNTBL," );
			strSql.Append("        '' CREDIT_NOTE_REF_NR," );
			strSql.Append("        '' CREDIT_NOTE_DATE," );
			strSql.Append("        '' CRN_AMOUNT," );
			strSql.Append("       CNT.DOCUMENT_TYPE DOCTYPE1," );
			strSql.Append("       CNT.DOCUMENT_TYPE DOCTYPE," );
			strSql.Append("       CNT.DOCUMENT_REFRENCE_FK DOCREF," );
			strSql.Append("       CNT.CURRENCY_MST_FK CURRMSTFK," );
			strSql.Append("      ctmt.currency_id CURRNAM," );
			strSql.Append("       CNT.CRN_AMOUNT CRNAMT," );
			strSql.Append("       ' ' SEL" );
			strSql.Append("  FROM REM_CREDIT_NOTE_TBL       CNT," );
			if (Convert.ToInt32(InvRefNr) != 0) {
				strSql.Append("       REM_CREDIT_NOTE_TRN_TBL   cntt," );

			}
			strSql.Append("       CURRENCY_TYPE_MST_TBL CTMT," );
			strSql.Append("       USER_MST_TBL          UMT" );
			strSql.Append("WHERE CNT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK" );
			strSql.Append(" AND CNT.CREATED_BY_FK= UMT.USER_MST_PK");
			if (Convert.ToInt32(InvRefNr) != 0) {
				strSql.Append("      and cntt.crn_tbl_fk=cnt.crn_tbl_pk" );
			}

			strSql.Append("       AND cnt.credit_note_type=" + Credit_note_type);
			strSql.Append("       AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
			strSql.Append("" );


			if (SUPPLIERPKs.Trim().Length > 0) {
				strSql.Append(" AND CNT.CRN_TBL_PK  in (" + SUPPLIERPKs + ") ");
			}

			strSql.Append(strCondition);
			strSql.Append(" ORDER BY   crn_tbl_pk  desc )Q)");
			string sql = null;
			sql = strSql.ToString();
			DataTable dt = null;
			try {
				pk = -1;
				dt = objWF.GetDataTable(sql);
				for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++) {
					if (Convert.ToInt32(dt.Rows[RowCnt]["CRNTBL"]) != pk) {
						pk = Convert.ToInt32(dt.Rows[RowCnt]["CRNTBL"]);
						Rno = 0;
					}
					Rno += 1;
					dt.Rows[RowCnt]["SR_NO"] = Rno;
				}

				return dt;

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

		#region "Fetch_Grid_Credit_Entry"
		public DataSet Fetch_Parent_Grid(string intBaseDate, Int16 intBaseCurrPk, string InvRefNr = "", Int16 strCustomerPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExType = 0, Int32 flag = 0)
		{

			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder strSql = new System.Text.StringBuilder();
			string strCondition = null;
			WorkFlow objWF = new WorkFlow();
			string strSQL1 = null;

			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);

			if (!string.IsNullOrEmpty(InvRefNr)) {
				strCondition += " AND  CIT.INVOICE_REF_NO =  '" + InvRefNr + "'  ";
			}
			if (strCustomerPk != 0) {
				strCondition += " AND  CIT.CUSTOMER_MST_FK =" + strCustomerPk;
			}

			//If BlankGrid = 0 Then
			//    strCondition &= vbCrLf & " AND 1=2 "
			//End If

			strQuery.Append("SELECT Count(*)" );
			strQuery.Append("  FROM REM_M_INVOICE_TBL      CIT," );
			strQuery.Append("       CURRENCY_TYPE_MST_TBL   CTMT," );
			strQuery.Append("       CUSTOMER_MST_TBL        CMT" );
			strQuery.Append("       where CIT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK" );
			strQuery.Append("       AND CIT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK" );

			strQuery.Append("" );
			strQuery.Append(strCondition);

			strSQL1 = strQuery.ToString();


			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
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


			strQuery.Remove(0, strQuery.Length);


			strSql.Append(" select * from (" );
			strSql.Append("SELECT ROWNUM SR_NO,q.* FROM (" );


			//strSql.Append("SELECT ROWNUM SRNO," & vbCrLf)
			strSql.Append("SELECT" );
			strSql.Append("       CIT.REMOVALS_INVOICE_PK," );
			strSql.Append("       '' REMOVALS_INVOICE_FK," );
			strSql.Append("       CIT.INVOICE_REF_NO," );
			strSql.Append("       '' FREIGHT_ELEMENT_Pk," );
			strSql.Append("       '' FREIGHT_ELEMENT_NAME," );
			strSql.Append("       '' CURRENCY_MST_PK," );
			strSql.Append("       CTMT.CURRENCY_ID," );
			strSql.Append("       CIT.NET_RECEIVABLE," );
			if (ExType == 1) {
				strSql.Append("       ROUND((SELECT GET_EX_RATE( CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL),6) ROE," );
				strSql.Append("       ROUND((CIT.NET_RECEIVABLE) * (SELECT GET_EX_RATE( CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL), 2) AMTINLOC," );
				strSql.Append("       ROUND((cit.TOTAL_CREDIT_NOTE_AMT) * (SELECT GET_EX_RATE( CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL),2) Crntot ," );
			}
			strSql.Append("       '' CRNCURRENT," );
			strSql.Append("       'FALSE' SEL" );
			strSql.Append("  FROM REM_M_INVOICE_TBL      CIT," );
			strSql.Append("       CURRENCY_TYPE_MST_TBL   CTMT," );
			strSql.Append("       CUSTOMER_MST_TBL        CMT" );
			strSql.Append("       where CIT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK" );
			strSql.Append("       AND CIT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK" );
			if (flag == 0) {
				strSql.Append("       AND 1=2" );
			}

			strSql.Append("" );



			strSql.Append(strCondition);
			strSql.Append(" ORDER BY  INVOICE_REF_NO )q)");
			// strSql.Append(" WHERE SR_NO  Between " & start & " and " & last & vbCrLf)

			string sql = null;
			sql = strSql.ToString();
			DataSet DS = new DataSet();

			try {
				DS = objWF.GetDataSet(sql);
				if (DS.Tables[0].Rows.Count > 0) {
					DataRelation CONTRel = null;

					DS.Tables.Add(FetchChildGrid(AllMasterPKs(DS), intBaseDate, intBaseCurrPk, InvRefNr, Convert.ToString(strCustomerPk),Convert.ToInt32(CurrentPage), TotalPage, ExType));

					CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["REMOVALS_INVOICE_PK"], DS.Tables[1].Columns["REMOVALS_INVOICE_FK"], true);
					CONTRel.Nested = true;
					DS.Relations.Add(CONTRel);
				}
				return DS;

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}


		}

		#region " All Master Supplier PKs "
		private string AllMasterPKs(DataSet ds)
		{
			Int16 RowCnt = default(Int16);
			Int16 ln = default(Int16);
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			strBuilder.Append("-1,");
			for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++) {
				strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["REMOVALS_INVOICE_PK"]).Trim() + ",");
			}
			strBuilder.Remove(strBuilder.Length - 1, 1);
			return strBuilder.ToString();
		}
		#endregion


		public DataTable FetchChildGrid(string SUPPLIERPKs = "", string intBaseDate = "", Int16 intBaseCurrPk = 0, string InvRefNr = "", string strCustomerPk = "0", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExType = 0)
		{


			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder strSql = new System.Text.StringBuilder();
			string strCondition = null;
			WorkFlow objWF = new WorkFlow();
			int RowCnt = 0;
			int Rno = 0;
			int pk = 0;
			string strSQL1 = null;

			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);


			if (!string.IsNullOrEmpty(InvRefNr)) {
				strCondition += " AND CIT.INVOICE_REF_NO = '" + InvRefNr + "'  ";
			}
			if (Convert.ToInt32(strCustomerPk) != 0) {
				strCondition += " AND CIT.CUSTOMER_MST_FK =" + strCustomerPk;
			}


			strSql.Append("select * from(");
			strSql.Append("SELECT ROWNUM SR_NO ,Q.*FROM ( SELECT");
			strSql.Append("       citt.REMOVALS_INVOICE_TRN_PK," );
			strSql.Append("       citt.REMOVALS_INVOICE_FK ," );
			strSql.Append("       CIT.INVOICE_REF_NO," );
			strSql.Append("       FEMT.FREIGHT_ELEMENT_MST_PK," );
			strSql.Append("       FEMT.FREIGHT_ELEMENT_NAME," );
			strSql.Append("       CTMT.CURRENCY_MST_PK," );
			strSql.Append("       CTMT.CURRENCY_ID," );
			strSql.Append("       CITT.TOT_AMT totamt," );
			if (ExType == 1) {
				strSql.Append("       ROUND((SELECT GET_EX_RATE(CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL),6) ROE," );
				strSql.Append("       ROUND((CITT.TOT_AMT) * (SELECT GET_EX_RATE(CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL), 2) CRNTOTAlLOCAL," );
				// strSql.Append("       ROUND((citt.TOTAL_CREDIT_AMT) * (SELECT GET_EX_RATE( CIT.CURRENCY_MST_FK," & intBaseCurrPk & ",TO_DATE('" & intBaseDate & "',DATEFORMAT )) FROM DUAL),2) crntrntot," & vbCrLf)
				strSql.Append("      sum(ROUND((cntt.CRN_AMT_IN_CRN_CUR) * (SELECT GET_EX_RATE(cntt.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL),2)) crntrntot," );
			}
			strSql.Append("       '' CRNCURRENT," );
			strSql.Append("       'FALSE' SEL" );
			strSql.Append("  FROM REM_M_INVOICE_TBL      CIT," );
			strSql.Append("       rem_invoice_trn_tbl  CITT," );
			strSql.Append("       CURRENCY_TYPE_MST_TBL     CTMT," );
			strSql.Append("       CUSTOMER_MST_TBL          CMT," );
			strSql.Append("       FREIGHT_ELEMENT_MST_TBL FEMT," );
			strSql.Append("       rem_credit_note_tbl     cnt," );
			strSql.Append("       rem_credit_note_trn_tbl cntt" );
			strSql.Append("       where CIT.REMOVALS_INVOICE_PK = CITT.REMOVALS_INVOICE_FK" );
			strSql.Append("       and   CITT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK" );
			strSql.Append("       AND   CIT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK" );
			strSql.Append("       AND   CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK" );
			strSql.Append("       AND   cnt.crn_tbl_pk(+) = cntt.crn_tbl_fk" );
			strSql.Append("       AND   cntt.invoice_trn_fk(+) = citt.REMOVALS_INVOICE_FK" );
			strSql.Append("" );

			//If SUPPLIERPKs.Trim.Length > 0 Then
			//    strSql.Append(" AND citt.REMOVALS_INVOICE_FK in (" & SUPPLIERPKs & ") ")
			//End If

			strSql.Append(strCondition);
			strSql.Append("  group by  citt.REMOVALS_INVOICE_TRN_PK," );
			strSql.Append(" citt.REMOVALS_INVOICE_FK,CIT.INVOICE_REF_NO, " );
			strSql.Append(" FEMT.FREIGHT_ELEMENT_MST_PK,FEMT.FREIGHT_ELEMENT_NAME,CTMT.CURRENCY_MST_PK," );
			strSql.Append(" CTMT.CURRENCY_ID,CITT.TOT_AMT,CIT.CURRENCY_MST_FK" );

			strSql.Append(" ORDER BY  INVOICE_REF_NO )Q)");

			string sql = null;
			sql = strSql.ToString();
			DataTable dt = null;

			try {
				pk = -1;
				dt = objWF.GetDataTable(sql);
				for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++) {
					if (Convert.ToInt32(dt.Rows[RowCnt]["REMOVALS_INVOICE_FK"]) != pk) {
						pk = Convert.ToInt32(dt.Rows[RowCnt]["REMOVALS_INVOICE_FK"]);
						Rno = 0;
					}
					Rno += 1;
					dt.Rows[RowCnt]["SR_NO"] = Rno;
				}

				return dt;

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

		#region "Fetch_Grid_From_listing"
		public DataSet Fetch_Parent_Grid_edit(string intBaseDate, Int16 intBaseCurrPk, string Crn_pk, Int32 CurrentPage = 0, Int32 TotalPage = 0)
		{


			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder strSql = new System.Text.StringBuilder();
			string strCondition = null;
			WorkFlow objWF = new WorkFlow();
			string strSQL1 = null;

			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);

			strQuery.Append("SELECT Count(*)" );
			strQuery.Append("  FROM REM_M_INVOICE_TBL      CIT," );
			strQuery.Append("       REM_CREDIT_NOTE_TBL       CNT," );
			strQuery.Append("       REM_CREDIT_NOTE_TRN_TBL   cntt," );
			strQuery.Append("       rem_invoice_trn_tbl  CITT," );
			strQuery.Append("       CURRENCY_TYPE_MST_TBL   CTMT" );
			strQuery.Append("  where  citt.REMOVALS_INVOICE_FK = cntt.INVOICE_TRN_FK (+)" );
			strQuery.Append("       and CNT.CRN_TBL_PK = cntt.CRN_TBL_FK" );
			strQuery.Append("       and CITT.REMOVALS_INVOICE_FK = cit.REMOVALS_INVOICE_PK" );
			strQuery.Append("       and cit.CURRENCY_MST_FK = ctmt.currency_mst_pk(+)" );
			strQuery.Append("       AND CNT.CRN_TBL_PK= " + Crn_pk);
			strQuery.Append("" );


			strSQL1 = strQuery.ToString();


			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
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

			strQuery.Remove(0, strQuery.Length);

			strSql.Append(" select * from (" );
			strSql.Append("SELECT ROWNUM SR_NO,q.* FROM (" );
			strSql.Append("SELECT" );
			strSql.Append("       distinct cit.REMOVALS_INVOICE_PK," );
			strSql.Append("       '' REMOVALS_INVOICE_TRN_PK," );
			strSql.Append("        CIT.INVOICE_REF_NO," );
			strSql.Append("        '' FREIGHT_ELEMENT_Pk," );
			strSql.Append("        '' FREIGHT_ELEMENT_NAME," );
			strSql.Append("        CTMT.CURRENCY_MST_PK," );
			strSql.Append("        CTMT.CURRENCY_ID," );
			strSql.Append("        CIT.NET_RECEIVABLE," );
			strSql.Append("        CNTT.EXCHANGE_RATE ROE," );
			strSql.Append("       (CIT.NET_RECEIVABLE *  CNTT.EXCHANGE_RATE) AMTINLOC," );
			strSql.Append("       (cit.TOTAL_CREDIT_NOTE_AMT*  CNTT.EXCHANGE_RATE) Crntot," );
			strSql.Append("       '' CRNCURRENT," );
			strSql.Append("       'FALSE' SEL" );
			strSql.Append("  FROM REM_M_INVOICE_TBL        CIT," );
			strSql.Append("       REM_CREDIT_NOTE_TBL      CNT," );
			strSql.Append("       REM_CREDIT_NOTE_TRN_TBL  cntt," );
			strSql.Append("       rem_invoice_trn_tbl      CITT," );
			strSql.Append("       CURRENCY_TYPE_MST_TBL    CTMT" );
			strSql.Append("       where  citt.REMOVALS_INVOICE_FK = cntt.INVOICE_TRN_FK (+)" );
			strSql.Append("       and CNT.CRN_TBL_PK = cntt.CRN_TBL_FK" );
			strSql.Append("       and CITT.REMOVALS_INVOICE_FK = cit.REMOVALS_INVOICE_PK" );
			strSql.Append("       and cit.currency_mst_fk = ctmt.currency_mst_pk(+)" );
			strSql.Append("       AND cnt.crn_tbl_pk= " + Crn_pk);

			strSql.Append("" );
			strSql.Append(strCondition);
			strSql.Append(" ORDER BY  INVOICE_REF_NO )q)");
			strSql.Append(" WHERE SR_NO  Between " + start + " and " + last );

			string sql = null;
			sql = strSql.ToString();
			DataSet DS = new DataSet();

			try {
				DS = objWF.GetDataSet(sql);
				DataRelation CONTRel = null;

				DS.Tables.Add(FetchChildGrid_edit(AllMasterPKs_edit(DS), Crn_pk, intBaseDate, intBaseCurrPk));

				CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["REMOVALS_INVOICE_PK"], DS.Tables[1].Columns["REMOVALS_INVOICE_FK"], true);
				CONTRel.Nested = true;
				DS.Relations.Add(CONTRel);
				return DS;

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}


		}

		#region " All Master Supplier PKs "
		private string AllMasterPKs_edit(DataSet ds)
		{
			Int16 RowCnt = default(Int16);
			Int16 ln = default(Int16);
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			strBuilder.Append("-1,");
			for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++) {
				strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["REMOVALS_INVOICE_PK"]).Trim() + ",");
			}
			strBuilder.Remove(strBuilder.Length - 1, 1);
			return strBuilder.ToString();
		}
		#endregion


		public DataTable FetchChildGrid_edit(string SUPPLIERPKs = "", string Crn_pk = "", string intBaseDate = "", Int16 intBaseCurrPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
		{

			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder strSql = new System.Text.StringBuilder();
			string strCondition = null;
			WorkFlow objWF = new WorkFlow();
			int RowCnt = 0;
			int Rno = 0;
			int pk = 0;
			string strSQL1 = null;

			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);

			strSql.Append("select * from(");
			strSql.Append("SELECT ROWNUM SR_NO ,Q.*FROM ( SELECT");
			strSql.Append("       citt.REMOVALS_INVOICE_TRN_PK," );
			strSql.Append("       citt.REMOVALS_INVOICE_FK ," );
			strSql.Append("       CIT.INVOICE_REF_NO," );
			strSql.Append("       cntt.FRT_OTH_ELEMENT_FK," );
			strSql.Append("       FEMT.FREIGHT_ELEMENT_NAME," );
			strSql.Append("       cntt.CURRENCY_MST_FK," );
			strSql.Append("       CTMT.CURRENCY_ID," );
			strSql.Append("       cntt.ELEMENT_INV_AMT totamt," );
			strSql.Append("       cntt.EXCHANGE_RATE ROE," );
			strSql.Append("       (cntt.ELEMENT_INV_AMT * cntt.EXCHANGE_RATE) CRNTOTAlLOCAL ," );
			//CRNTOTAlLOCAL
			//strSql.Append("       (citt.TOTAL_CREDIT_AMT* cntt.exchange_rate) crntrntot," & vbCrLf)
			strSql.Append("       (cntt.crn_amt_in_crn_cur * cntt.EXCHANGE_RATE) crntrntot," );
			strSql.Append("       '' CRNCURRENT," );
			strSql.Append("       'TRUE' SEL" );
			strSql.Append("  FROM REM_CREDIT_NOTE_TBL       CNT," );
			strSql.Append("       REM_CREDIT_NOTE_TRN_TBL   cntt," );
			strSql.Append("       rem_invoice_trn_tbl  CITT," );
			strSql.Append("       REM_M_INVOICE_TBL      CIT," );
			strSql.Append("       CURRENCY_TYPE_MST_TBL     CTMT," );
			strSql.Append("       FREIGHT_ELEMENT_MST_TBL   FEMT" );
			strSql.Append("     where citt.REMOVALS_INVOICE_FK = cntt.INVOICE_TRN_FK (+)" );
			strSql.Append("       and cnt.crn_tbl_pk = cntt.crn_tbl_fk" );
			strSql.Append("       and CITT.REMOVALS_INVOICE_FK = cit.REMOVALS_INVOICE_PK" );
			strSql.Append("       and citt.FRT_OTH_ELEMENT_FK = femt.freight_element_mst_pk" );
			strSql.Append("       and cntt.FRT_OTH_ELEMENT_FK = femt.freight_element_mst_pk(+)" );
			strSql.Append("       and cntt.currency_mst_fk = ctmt.currency_mst_pk" );
			strSql.Append("       AND cnt.crn_tbl_pk= " + Crn_pk);
			strSql.Append("" );

			if (SUPPLIERPKs.Trim().Length > 0) {
				strSql.Append(" AND citt.REMOVALS_INVOICE_FK  in (" + SUPPLIERPKs + ") ");
			}

			strSql.Append(strCondition);

			strSql.Append(" ORDER BY  INVOICE_REF_NO )Q)");

			string sql = null;
			sql = strSql.ToString();
			DataTable dt = null;

			try {
				pk = -1;
				dt = objWF.GetDataTable(sql);
				for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++) {
					if (Convert.ToInt32(dt.Rows[RowCnt]["REMOVALS_INVOICE_FK"]) != pk) {
						pk = Convert.ToInt32(dt.Rows[RowCnt]["REMOVALS_INVOICE_FK"]);
						Rno = 0;
					}
					Rno += 1;
					dt.Rows[RowCnt]["SR_NO"] = Rno;
				}

				return dt;

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

		#region "Fetch Document refrence Nr"

		public DataSet Fetch_Doc_Ref(string DocNr)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			//ByVal docType As Int32,
			WorkFlow objWF = new WorkFlow();

			try {
				if (string.IsNullOrEmpty(DocNr)) {
					DocNr = "0";
				}
				strQuery.Append("" );
				strQuery.Append("select jcset.JOB_CARD_REF refdocnr " );
				strQuery.Append("  from REM_M_JOB_CARD_MST_TBL jcset" );
				strQuery.Append("where jcset.JOB_CARD_PK =" + DocNr);
				strQuery.Append("" );

				return objWF.GetDataSet(strQuery.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

		#region "Fetch_Customer_invoice_entry"
		public string FetchCustInvoice(string strCond, string loc = "0")
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			string[] arr = null;
			string strSERACH_IN = null;
			string strBizType = null;
			string strReq = null;
			string strcustpk = null;
			string strProcessType = null;
			string strloc = "";

			arr = strCond.Split(Convert.ToChar("~"));
			if (arr.Length > 0)
				strReq = arr[0];
			if (arr.Length > 1)
				strSERACH_IN = arr[1];
			if (arr.Length > 2)
				strBizType = arr[2];
			if (arr.Length > 3)
				strProcessType = arr[3];
			if (arr.Length > 4)
				strcustpk = arr[5];
			if (arr.Length > 5)
				strloc = arr[6];
			//strSERACH_IN = arr(1)
			//strBizType = arr(2)
			//strProcessType = arr(3)
			//strloc = arr(6)
			//strcustpk = arr(5)



			try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_CONSOL_INVOICE_JOB_PKG.GET_INVOICE_JOB";

				var _with2 = selectCommand.Parameters;
				_with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
				_with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with2.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
				_with2.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
				_with2.Add("CUSTOMER_PK_IN", getDefault(strcustpk, "")).Direction = ParameterDirection.Input;
				_with2.Add("LOCFK_IN", strloc).Direction = ParameterDirection.Input;
				_with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}
		#endregion

		#region "Fetch_Customer_invoice_lISTING"
		public string FetchCustInvoice_listing(string strCond, string loc = "0")
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			string[] arr = null;
			string strSERACH_IN = null;
			string strBizType = null;
			string strReq = null;
			string strcustpk = null;
			string strProcessType = null;
			string strloc = "";

			arr = strCond.Split(Convert.ToChar("~"));
			if (arr.Length > 0)
				strReq = arr[0];
			if (arr.Length > 1)
				strSERACH_IN = arr[1];
			if (arr.Length > 2)
				strBizType = arr[2];
			if (arr.Length > 3)
				strProcessType = arr[3];
			if (arr.Length > 5)
				strcustpk = arr[5];
			if (arr.Length > 6)
				strloc = arr[6];
			//strSERACH_IN = arr(1)
			//strBizType = arr(2)
			//strProcessType = arr(3)
			//strloc = arr(6)
			//strcustpk = arr(5)



			try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_CONSOL_INVOICE_JOB_PKG.GET_INVOICE_JOB_LIST";

				var _with3 = selectCommand.Parameters;
				_with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
				_with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with3.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
				_with3.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
				_with3.Add("CUSTOMER_PK_IN", getDefault(strcustpk, "")).Direction = ParameterDirection.Input;
				_with3.Add("LOCFK_IN", strloc).Direction = ParameterDirection.Input;
				_with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}
		#endregion

		#region "Update Console_invoice_trn_table"
		public ArrayList Update_Console_invoice_trn_tbl(string StrPk, double StrCredit, double TotalAmt, OracleCommand cmd)
		{
			WorkFlow objWK = new WorkFlow();
			Int16 exe = default(Int16);
			System.Text.StringBuilder strQuery = null;
			double strTotal = 0;
			OracleTransaction TRAN = null;
			arrMessage.Clear();

			try {
				cmd.CommandType = CommandType.Text;
				strTotal = StrCredit + TotalAmt;
				cmd.Parameters.Clear();
				strQuery = new System.Text.StringBuilder();
				strQuery.Append(" update rem_invoice_trn_tbl CITT " );
				strQuery.Append(" set CITT.TOTAL_CREDIT_AMT =" + strTotal );
				strQuery.Append(" Where CITT.REMOVALS_INVOICE_TRN_PK =" + StrPk );
				strQuery.Append("" );
				cmd.CommandText = strQuery.ToString();
				exe = Convert.ToInt16(cmd.ExecuteNonQuery());


				arrMessage.Add("All data saved successfully");
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

		#region "Update Console_invoice_table"
		public ArrayList Update_Console_invoice_tbl(string StrPk, double StrCredit, OracleCommand cmd)
		{
			WorkFlow objWK = new WorkFlow();
			Int16 exe = default(Int16);
			System.Text.StringBuilder strQuery = null;
			int strTotal = 0;

			arrMessage.Clear();

			try {
				cmd.CommandType = CommandType.Text;

				cmd.Parameters.Clear();
				strQuery = new System.Text.StringBuilder();
				strQuery.Append(" update REM_M_INVOICE_TBL cit " );
				strQuery.Append(" set cit.TOTAL_CREDIT_NOTE_AMT =" + StrCredit );
				strQuery.Append(" Where cit.REMOVALS_INVOICE_PK =" + StrPk );
				strQuery.Append("" );
				cmd.CommandText = strQuery.ToString();
				exe = Convert.ToInt16(cmd.ExecuteNonQuery());


				arrMessage.Add("All data saved successfully");
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

		#region "fetch_Console_trn_CreditLimit "
		public string Fetch_Console_trn_credit(Int32 pk)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			string CreditVal = null;
			DataSet dsCredit = new DataSet();


			try {

				strQuery.Append("select cit.TOTAL_CREDIT_AMT from  rem_invoice_trn_tbl cit" );
				strQuery.Append("  where cit.REMOVALS_INVOICE_TRN_PK  = " + pk);

				dsCredit = objWF.GetDataSet(strQuery.ToString());


				if (dsCredit.Tables[0].Rows.Count > 0) {
					CreditVal = Convert.ToString(getDefault(dsCredit.Tables[0].Rows[0]["TOTAL_CREDIT_AMT"], 0));

				}
				return CreditVal;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
		#endregion

		#region "Fetch_Console_Tbl"
		public string Fetch_Console_credit(Int32 pk)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			string CreditVal = null;
			DataSet dsCredit = new DataSet();


			try {

				strQuery.Append("select cit.TOTAL_CREDIT_NOTE_AMT from  REM_M_INVOICE_TBL cit" );
				strQuery.Append("  where cit.REMOVALS_INVOICE_PK = " + pk);

				dsCredit = objWF.GetDataSet(strQuery.ToString());


				if (dsCredit.Tables[0].Rows.Count > 0) {
					CreditVal = Convert.ToString(getDefault(dsCredit.Tables[0].Rows[0]["TOTAL_CREDIT_NOTE_AMT"], 0));

				}
				return CreditVal;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
		#endregion

		#region "Fetch Current Credit Note Value"
		public int Fetch_Current_Credit_value(string Pktran)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			string CreditVal = null;
			DataSet dsCredit = new DataSet();


			try {

				strQuery.Append("select cnt.CRN_AMT_IN_CRN_CUR from  REM_CREDIT_NOTE_TRN_TBL cnt " );
				strQuery.Append("  where cnt.CRN_TRN_TBL_PK = " + Pktran);

				dsCredit = objWF.GetDataSet(strQuery.ToString());


				if (dsCredit.Tables[0].Rows.Count > 0) {
					CreditVal = Convert.ToString(getDefault(dsCredit.Tables[0].Rows[0]["CRN_AMT_IN_CRN_CUR"], 0));

				}
				return Convert.ToInt32(CreditVal);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Update Console Invoice as rollback "
		public ArrayList Update_Console_invoice_tbl_Rollback(string StrPk, int StrCredit, OracleCommand cmd)
		{
			WorkFlow objWK = new WorkFlow();
			Int16 exe = default(Int16);
			System.Text.StringBuilder strQuery = null;
			int strTotal = 0;

			arrMessage.Clear();

			try {
				cmd.CommandType = CommandType.Text;

				cmd.Parameters.Clear();
				strQuery = new System.Text.StringBuilder();
				strQuery.Append(" update CONSOL_INVOICE_TRN_TBL CITT " );
				strQuery.Append(" set CITT.TOTAL_CREDIT_AMT =" + StrCredit );
				strQuery.Append(" Where CITT.consol_invoice_trn_pk =" + StrPk );
				strQuery.Append("" );
				cmd.CommandText = strQuery.ToString();
				exe = Convert.ToInt16(cmd.ExecuteNonQuery());


				arrMessage.Add("All data saved successfully");
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

		#region "Fetch Customer For Document refrence Nr"

		public DataSet Fetch_Cust_Doc_Ref(Int32 DocNr, Int32 docType)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				//If DocNr = 0 Then
				//    DocNr = 0
				//End If
				strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name" );
				strQuery.Append("from customer_mst_tbl cmt, REM_M_JOB_CARD_MST_TBL jcset" );
				strQuery.Append("WHERE  jcset.JOB_CARD_PK  =" + DocNr);
				strQuery.Append("and jcset.JOB_CARD_SHIPPER_FK = cmt.customer_mst_pk" );
				strQuery.Append("" );

				return objWF.GetDataSet(strQuery.ToString());

			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

		#region "Credit Limit Used"
		public void SaveCreditLimit(double CrnAmt, string Customer, double CrLimitUsed, OracleTransaction TRAN)
		{
			WorkFlow objWK = new WorkFlow();
			Int16 exe = default(Int16);
			OracleCommand cmd = new OracleCommand();
			double temp = 0;
			string strSQL = null;
			//temp = CrnAmt - CrLimitUsed 'Hided By prakash Chandra On 11/09/2008
			temp = CrnAmt + CrLimitUsed;
			//Added By prakash Chandra On 11/09/2008
			try {
				cmd.CommandType = CommandType.Text;
				cmd.Connection = TRAN.Connection;
				cmd.Transaction = TRAN;
				cmd.Parameters.Clear();
				strSQL = "update customer_mst_tbl a set a.credit_limit_used = " + temp;
				strSQL = strSQL + " where a.customer_name in ('" + Customer + "')";
				cmd.CommandText = strSQL;
				exe = Convert.ToInt16(cmd.ExecuteNonQuery());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}

		}
		#endregion

		#region "Fetch Invoice PK" 'Added by rabbani ,To implement Barcode on 12/03/07
		public int FetchInvPK(string Inv)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
			strSQL = "SELECT C.REMOVALS_INVOICE_PK FROM rem_m_invoice_tbl C" + "WHERE C.INVOICE_REF_NO= '" + Inv + "'";
			try {
				return Convert.ToInt32(objWK.ExecuteScaler(strSQL));
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Invoice Nr."
		public int FetchCount(string Inv)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
			strSQL = "SELECT COUNT(*) FROM CONSOL_INVOICE_TBL C" + "WHERE C.INVOICE_REF_NO= '" + Inv + "'";
			try {
				return Convert.ToInt32(objWK.ExecuteScaler(strSQL));
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Invoice Generated" 'Added by rabbani ,To implement Barcode on 13/03/07
		public int FetchInvGen(string Inv)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
			strSQL = "SELECT COUNT(*) FROM CONSOL_INVOICE_TBL C" + "WHERE C.INVOICE_REF_NO= '" + Inv + "'" + "AND C.INVOICE_AMT = C.TOTAL_CREDIT_NOTE_AMT";
			try {
				return Convert.ToInt32(objWK.ExecuteScaler(strSQL));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            } catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Customer PK"
		public int FetchCusPK(string InvNr)
		{
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
			strSQL = "SELECT C.CUSTOMER_MST_FK FROM  CONSOL_INVOICE_TBL C" + "WHERE C.INVOICE_REF_NO= '" + InvNr + "'";
			try {
				return Convert.ToInt32(objWK.ExecuteScaler(strSQL));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            } catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#endregion

		#region " For Print Summary "
		public DataSet CREDIT_MAIN_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
		{
			WorkFlow objWK = new WorkFlow();
			try {
				var _with4 = objWK.MyCommand.Parameters;
				_with4.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
				_with4.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
				_with4.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
				_with4.Add("CREDIT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_PRINT");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
		}
		#endregion

		#region " For Print Summary Sub Report "
		public DataSet CREDIT_Sub_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
		{
			WorkFlow objWK = new WorkFlow();
			try {
				var _with5 = objWK.MyCommand.Parameters;
				_with5.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
				_with5.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
				_with5.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
				_with5.Add("CREDIT_CUR2", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_SUB_RPT_PRINT");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
		}
		#endregion

		#region " For Print  Details  Main Report "
		public DataSet CREDIT_Main_Details_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
		{
			WorkFlow objWK = new WorkFlow();
			try {
				var _with6 = objWK.MyCommand.Parameters;
				_with6.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
				_with6.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
				_with6.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
				_with6.Add("CREDIT_CUR1", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_DETAILS_RPT_PRINT");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
		}
		#endregion

		#region " For Print Details SUB  Report "
		public DataSet CREDIT_Sub_Details_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
		{
			WorkFlow objWK = new WorkFlow();
			try {
				var _with7 = objWK.MyCommand.Parameters;
				_with7.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
				_with7.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
				_with7.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
				_with7.Add("CREDIT_CUR4", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_DETAILS_SUB_RPT_PRINT");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
		}
		#endregion

		#region "Print For Currency Details "
		public DataSet CREDIT_Curr_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
		{
			WorkFlow objWK = new WorkFlow();
			try {
				var _with8 = objWK.MyCommand.Parameters;
				_with8.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
				_with8.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
				_with8.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
				_with8.Add("CREDIT_CUR4", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_DETAILS_Curr_RPT_PRINT");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
		}
		#endregion

		#region "For Custumer Against Geeral Credit Note "
		public DataSet Fetch_General_Custumer(int CustPk)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				strQuery.Append("  " );
				strQuery.Append("  SELECT CUST.CUSTOMER_NAME," );
				strQuery.Append("         CC.ADM_ADDRESS_1," );
				strQuery.Append("         CC.ADM_ADDRESS_2," );
				strQuery.Append("         CC.ADM_ADDRESS_3," );
				strQuery.Append("         CC.ADM_ZIP_CODE," );
				strQuery.Append("         CC.ADM_CITY," );
				strQuery.Append("         CCC.COUNTRY_NAME" );
				strQuery.Append("    FROM CUSTOMER_MST_TBL      CUST," );
				strQuery.Append("         CUSTOMER_CONTACT_DTLS CC," );
				strQuery.Append("         COUNTRY_MST_TBL       CCC" );
				strQuery.Append("   WHERE CC.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK" );
				strQuery.Append("     AND CC.ADM_COUNTRY_MST_FK = CCC.COUNTRY_MST_PK" );
				strQuery.Append("     AND CUST.CUSTOMER_MST_PK =" + CustPk);
				strQuery.Append("" );
				return objWF.GetDataSet(strQuery.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}

		}
		#endregion

		public string FetchInvRefNr(string strCond)
		{
            WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSEARCH_IN = "";
			string strLOC_MST_IN = "";
			string CUSTOMER_PK = "";
			string strBusinessType = "";
			string Port = "";
			string strReq = null;
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
			strSEARCH_IN = Convert.ToString(arr.GetValue(1));
			CUSTOMER_PK = Convert.ToString(arr.GetValue(2));
			try {
				objWF.OpenConnection();
				SCM.Connection = objWF.MyConnection;
				SCM.CommandType = CommandType.StoredProcedure;
				SCM.CommandText = objWF.MyUserName + ".EN_REMOVAL_CREDITNOTE_PKG.GET_REMOVAL_INVREFNR";
				var _with9 = SCM.Parameters;
				_with9.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
				_with9.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with9.Add("CUSTOMER_PK_IN", CUSTOMER_PK).Direction = ParameterDirection.Input;
				_with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
				SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				SCM.ExecuteNonQuery();
				strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				SCM.Connection.Close();
			}
		}

		public string FetchCreditCRNNr(string strCond)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSEARCH_IN = "";
			string strLOC_MST_IN = "";
			string Credittype = "";
			string strBusinessType = "";
			string CUSTOMER_PK = "";
			string Port = "";
			string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            CUSTOMER_PK = Convert.ToString(arr.GetValue(3));
            Credittype = Convert.ToString(arr.GetValue(2));

			try {
				objWF.OpenConnection();
				SCM.Connection = objWF.MyConnection;
				SCM.CommandType = CommandType.StoredProcedure;
				SCM.CommandText = objWF.MyUserName + ".EN_REMOVAL_CREDITNOTE_PKG.GET_REMOVAL_CRNNR";
				var _with10 = SCM.Parameters;
				_with10.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
				_with10.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with10.Add("CUSTOMER_PK_IN", CUSTOMER_PK).Direction = ParameterDirection.Input;
				_with10.Add("CREDITNOTETYPE_IN", Credittype).Direction = ParameterDirection.Input;
				_with10.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
				SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				SCM.ExecuteNonQuery();
				strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				SCM.Connection.Close();
			}
		}
		public string FetchCreditDocNr(string strCond)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSEARCH_IN = "";
			string strLOC_MST_IN = "";
			string strBusinessType = "";
			string CUSTOMER_PK = "";
			string Port = "";
			string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            CUSTOMER_PK = Convert.ToString(arr.GetValue(2));

            try {
				objWF.OpenConnection();
				SCM.Connection = objWF.MyConnection;
				SCM.CommandType = CommandType.StoredProcedure;
				SCM.CommandText = objWF.MyUserName + ".EN_REMOVAL_CREDITNOTE_PKG.GET_REMOVAL_JOBCARDREFNR";
				var _with11 = SCM.Parameters;
				_with11.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
				_with11.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with11.Add("CUSTOMER_PK_IN", CUSTOMER_PK).Direction = ParameterDirection.Input;
				_with11.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
				SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				SCM.ExecuteNonQuery();
				strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				SCM.Connection.Close();
			}
		}

		#region "Fetch Header as from listing "
		public DataSet fetch_header(string crn_pk)
		{

			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();

			try {

				strQuery.Append("" );
				strQuery.Append(" SELECT CNT.CRN_TBL_PK," );
				strQuery.Append("       CNT.CREDIT_NOTE_TYPE," );
				strQuery.Append("       CNT.CREDIT_NOTE_REF_NR," );
				strQuery.Append("       CNT.CREDIT_NOTE_DATE," );
				strQuery.Append("       CNT.CURRENCY_MST_FK," );
				strQuery.Append("       CMT.CUSTOMER_ID," );
				strQuery.Append("       CTMT.CURRENCY_NAME," );
				strQuery.Append("       CNT.CUSTOMER_MST_FK," );
				strQuery.Append("       CMT.CUSTOMER_NAME," );
				strQuery.Append("       CNT.DOCUMENT_TYPE," );
				strQuery.Append("       CNT.CRN_AMOUNT," );
				strQuery.Append("       CNT.VERSION_NO," );
				strQuery.Append("       CNT.DOCUMENT_REFRENCE_FK" );
				strQuery.Append("  FROM REM_CREDIT_NOTE_TBL   CNT," );
				strQuery.Append("       CURRENCY_TYPE_MST_TBL CTMT," );
				strQuery.Append("       CUSTOMER_MST_TBL      CMT" );
				strQuery.Append(" WHERE  CNT.CRN_TBL_PK= " + crn_pk);
				strQuery.Append(" AND CNT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" );
				strQuery.Append(" AND CNT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK" );


				return objWF.GetDataSet(strQuery.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
		#endregion

		#region "fetch_Cutumer_pk "
		public DataSet fetch_Cust_pk(Int32 pk)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();

			try {

				strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name" );
				strQuery.Append("  from REM_M_INVOICE_TBL CIT, customer_mst_tbl cmt" );
				strQuery.Append(" where cit.CUSTOMER_MST_FK = cmt.customer_mst_pk" );
				strQuery.Append("   and cit.REMOVALS_INVOICE_PK = " + pk);
				strQuery.Append("" );


				return objWF.GetDataSet(strQuery.ToString());

			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
		#endregion

		#region "Fetch_Master_pk"
		public DataSet fetchpk(string pk)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();

			try {

				strQuery.Append("" );
				strQuery.Append(" select cntt.CRN_TRN_TBL_PK from  REM_CREDIT_NOTE_TRN_TBL  cntt" );
				strQuery.Append(" where cntt.CRN_TBL_FK=" + pk);

				return objWF.GetDataSet(strQuery.ToString());

			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

		#endregion

		#region "Save Function"
		public ArrayList SaveCreditNote(DataSet DsMain, DataSet dspk, long lngLocPk, long nEmpId, long nUserId, string CREDIT_NOTE_REF_NR, string strpk = "", int version = 0, double CAmt = 0, string Customer = "0",
		double CrLimit = 0, double CrLimitUsed = 0, int ExType = 1, int jobpk = 0)
		{

			WorkFlow objWK = new WorkFlow();
			Int16 exe = default(Int16);
			int i = 0;
			Int16 j = default(Int16);
			double crnAmt = 0;
			double credittrn = 0;
			string ConsTrnPk = null;
			double CrnAmtTrn = 0;
			double strCredittot = 0;
			double strConsTbl = 0;
			string strpkConsolePk = null;
			double strCreditTrn = 0;
			double strcreditConsoleInvoice = 0;
			bool chkFlag = false;

			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();

			long lngCreditNotePkTran = 0;
			Int32 RecAfct = default(Int32);
			string CreditNoteNo = null;
			Int32 Total_Amt = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			System.Text.StringBuilder strQuery = null;
			string strConsolepk = null;
			double strCurrentcredit = 0;

			objWK.MyCommand.Connection = objWK.MyConnection;
			objWK.MyCommand.Transaction = TRAN;

			//Protocol Define 
			try {
				if (string.IsNullOrEmpty(CREDIT_NOTE_REF_NR)) {
					CreditNoteNo = GenerateProtocolKey("REMOVAL CREDIT NOTE", lngLocPk, nEmpId, DateTime.Today, "", "", "", nUserId);
					if (CreditNoteNo == "Protocol Not Defined.") {
						arrMessage.Add("Protocol Not Defined.");
						return arrMessage;
					}

				} else {
					CreditNoteNo = CREDIT_NOTE_REF_NR;
				}
				//For Credit Note Master Table 
				if (!string.IsNullOrEmpty(strpk)) {
					objWK.MyCommand.Parameters.Clear();
					insCommand.Parameters.Clear();
					insCommand.CommandText = objWK.MyUserName + ".REMOVAL_CREDIT_NOTE_TBL_PKG.REMOVAL_CREDIT_NOTE_TBL_UPD";
					insCommand.Connection = objWK.MyConnection;
					insCommand.CommandType = CommandType.StoredProcedure;
					insCommand.Transaction = TRAN;
				} else {
					objWK.MyCommand.Parameters.Clear();
					insCommand.Parameters.Clear();
					insCommand.CommandText = objWK.MyUserName + ".REMOVAL_CREDIT_NOTE_TBL_PKG.REMOVAL_CREDIT_NOTE_TBL_INS";
					insCommand.Connection = objWK.MyConnection;
					insCommand.CommandType = CommandType.StoredProcedure;
					insCommand.Transaction = TRAN;
					chkFlag = true;
				}


				var _with12 = insCommand.Parameters;
				if (!string.IsNullOrEmpty(strpk)) {
					_with12.Add("crn_tbl_pk_in", Convert.ToInt64(strpk));
					_with12.Add("last_modified_by_fk_in", nUserId);
					_with12.Add("version_no_in", version);
				} else {
					_with12.Add("CREATED_BY_FK_IN", nUserId);
				}
				_with12.Add("CREDIT_NOTE_TYPE_IN", DsMain.Tables["tblMaster"].Rows[0]["CREDIT_NOTE_TYPE"]);
				_with12.Add("CREDIT_NOTE_REF_NR_IN", CreditNoteNo);
				_with12.Add("CREDIT_NOTE_DATE_IN", getDefault(Convert.ToDateTime(DsMain.Tables["tblMaster"].Rows[0]["CREDIT_NOTE_DATE"]), ""));
				_with12.Add("CURRENCY_MST_FK_IN", getDefault(DsMain.Tables["tblMaster"].Rows[0]["CURRENCY_MST_FK"], ""));
				_with12.Add("CRN_AMMOUNT_IN", DsMain.Tables["tblMaster"].Rows[0]["CRN_AMOUNT"]);
				_with12.Add("CUSTOMER_MST_FK_IN", getDefault(DsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_MST_FK"], ""));
				_with12.Add("DOCUMENT_TYPE_IN", getDefault(DsMain.Tables["tblMaster"].Rows[0]["DOCUMENT_TYPE"], ""));
				_with12.Add("DOCUMENT_REFRENCE_IN", getDefault(DsMain.Tables["tblMaster"].Rows[0]["DOCUMENT_REFRENCE_FK"], ""));
				_with12.Add("CONFIG_MST_FK_IN", ConfigurationPK);
				_with12.Add("EXCH_RATE_TYPE_FK_IN", ExType);
				if (!string.IsNullOrEmpty(strpk)) {
					_with12.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				} else {
					_with12.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CRN_TBL_PK").Direction = ParameterDirection.Output;
					insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				}




                exe = Convert.ToInt16(insCommand.ExecuteNonQuery());
				lngCreditNotePk = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);

				//adding by thiyagarajan on 27/1/09:TrackNTrace Task:VEK Req.
				//If getDefault(strpk, "") = "" Then
				//    Dim doctype As Int32 = 10
				//    objWK.MyCommand.CommandType = CommandType.StoredProcedure
				//    objWK.MyCommand.CommandText = objWK.MyUserName & ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS"
				//    With objWK.MyCommand.Parameters
				//        .Clear()
				//        .Add("REF_NO_IN", CreditNoteNo).Direction = ParameterDirection.Input
				//        .Add("REF_FK_IN", jobpk).Direction = ParameterDirection.Input
				//        .Add("LOC_IN", HttpContext.Current.Session("LOGED_IN_LOC_FK")).Direction = ParameterDirection.Input
				//        .Add("STATUS_IN", "Credit Note Generated against InvoiceNr.").Direction = ParameterDirection.Input
				//        .Add("CREATED_BY_IN", HttpContext.Current.Session("USER_PK")).Direction = ParameterDirection.Input
				//        .Add("DOCTYPE_IN", doctype).Direction = ParameterDirection.Input
				//    End With
				//    objWK.MyCommand.ExecuteNonQuery()
				//End If
				//end

				for (i = 0; i <= DsMain.Tables["tblTransaction"].Rows.Count - 1; i++) {
					//For Credit Note Transaction Table 
					if (!string.IsNullOrEmpty(strpk)) {
						objWK.MyCommand.Parameters.Clear();
						insCommand.Parameters.Clear();
						insCommand.CommandText = objWK.MyUserName + ".REMOVAL_CREDITNOTE_TRN_TBL_PKG.REMOVALCREDIT_NOTE_TRN_TBL_UPD";
						insCommand.Connection = objWK.MyConnection;
						insCommand.CommandType = CommandType.StoredProcedure;
						insCommand.Transaction = TRAN;

						var _with13 = insCommand.Parameters;
						for (j = 0; j <= dspk.Tables[0].Rows.Count - 1; j++) {
							if (!string.IsNullOrEmpty(strpk)) {
								_with13.Add("crn_trn_tbl_pk_in", dspk.Tables[0].Rows[j]["crn_trn_tbl_pk"]);
							}

							_with13.Add("CRN_TBL_FK_IN", lngCreditNotePk);
							_with13.Add("INVOICE_TRN_FK_IN", getDefault(DsMain.Tables["tblTransaction"].Rows[i]["INVOICE_TRN_FK"], ""));
							_with13.Add("FRT_OTH_ELEMENT_FK_IN", getDefault(DsMain.Tables["tblTransaction"].Rows[i]["FRT_OTH_ELEMENT_FK"], ""));
							_with13.Add("CURRENCY_MST_FK_IN", getDefault(DsMain.Tables["tblTransaction"].Rows[i]["CURRENCY_MST_FK"], ""));
							_with13.Add("ELEMENT_INV_AMT_IN", getDefault(DsMain.Tables["tblTransaction"].Rows[i]["ELEMENT_INV_AMT"], ""));
							_with13.Add("EXCHANGE_RATE_IN", getDefault(DsMain.Tables["tblTransaction"].Rows[i]["EXCHANGE_RATE"], 0));
							_with13.Add("ELE_AMT_IN_CRN_CUR_IN", getDefault(DsMain.Tables["tblTransaction"].Rows[i]["ELE_AMT_IN_CRN_CUR"], ""));
							_with13.Add("CRN_AMT_IN_CRN_CUR_IN", getDefault(DsMain.Tables["tblTransaction"].Rows[i]["CRN_AMT_IN_CRN_CUR"], ""));
							_with13.Add("CONFIG_MST_FK_IN", ConfigurationPK);
							if (!string.IsNullOrEmpty(strpk)) {
								//crn_trn_tbl_pk_in
								_with13.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
								insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
							} else {
								_with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CRN_TRN_TBL_PK").Direction = ParameterDirection.Output;
								insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
							}
							int updateval = 0;

							ConsTrnPk = Convert.ToString(DsMain.Tables["tblTransaction"].Rows[i]["INVOICE_TRN_FK"]);
							credittrn = Convert.ToDouble(Fetch_Console_trn_credit(Convert.ToInt16(ConsTrnPk)));
							crnAmt = Convert.ToDouble(DsMain.Tables["tblTransaction"].Rows[i]["CRN_AMT_IN_CRN_CUR"]) / Convert.ToDouble(DsMain.Tables["tblTransaction"].Rows[i]["EXCHANGE_RATE"]);

							strCurrentcredit = Fetch_Current_Credit_value(strpk);

							updateval = Convert.ToInt32(credittrn) - Convert.ToInt32(strCurrentcredit);



							if (Convert.ToInt32(ConsTrnPk )!= 0) {
								arrMessage = Update_Console_invoice_trn_tbl(ConsTrnPk, credittrn, crnAmt, objWK.MyCommand);
								if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0)) {
									TRAN.Rollback();
									if (chkFlag) {
										RollbackProtocolKey("REMOVAL CREDIT NOTE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), CREDIT_NOTE_REF_NR, System.DateTime.Now);
									}
									return arrMessage;
								}
							}

							strpkConsolePk = Convert.ToString(DsMain.Tables["tblTransaction"].Rows[i]["INVOICE_TRN_FK"]);
							strCreditTrn = Convert.ToDouble(Fetch_Console_credit(Convert.ToInt32(strpkConsolePk)));
							strCredittot = strCreditTrn + crnAmt;
							strConsTbl = strConsTbl + strCredittot;


							// exe = insCommand.ExecuteNonQuery()
							break; // TODO: might not be correct. Was : Exit For

						}


						// 


					} else {
						objWK.MyCommand.Parameters.Clear();
						insCommand.Parameters.Clear();
						insCommand.CommandText = objWK.MyUserName + ".REMOVAL_CREDITNOTE_TRN_TBL_PKG.REMOVALCREDIT_NOTE_TRN_TBL_INS";
						insCommand.Connection = objWK.MyConnection;
						insCommand.CommandType = CommandType.StoredProcedure;
						insCommand.Transaction = TRAN;


						var _with14 = insCommand.Parameters;


						_with14.Add("CRN_TBL_FK_IN", lngCreditNotePk);
						_with14.Add("INVOICE_TRN_FK_IN", DsMain.Tables["tblTransaction"].Rows[i]["INVOICE_TRN_FK"]);
						_with14.Add("FRT_OTH_ELEMENT_FK_IN", DsMain.Tables["tblTransaction"].Rows[i]["FRT_OTH_ELEMENT_FK"]);
						_with14.Add("CURRENCY_MST_FK_IN", DsMain.Tables["tblTransaction"].Rows[i]["CURRENCY_MST_FK"]);
						_with14.Add("ELEMENT_INV_AMT_IN", DsMain.Tables["tblTransaction"].Rows[i]["ELEMENT_INV_AMT"]);
						_with14.Add("EXCHANGE_RATE_IN", DsMain.Tables["tblTransaction"].Rows[i]["EXCHANGE_RATE"]);
						_with14.Add("ELE_AMT_IN_CRN_CUR_IN", getDefault(DsMain.Tables["tblTransaction"].Rows[i]["ELE_AMT_IN_CRN_CUR"], 0));
						_with14.Add("CRN_AMT_IN_CRN_CUR_IN", DsMain.Tables["tblTransaction"].Rows[i]["CRN_AMT_IN_CRN_CUR"]);
						_with14.Add("CONFIG_MST_FK_IN", ConfigurationPK);
						_with14.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CRN_TRN_TBL_PK").Direction = ParameterDirection.Output;
						insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


						// exe = insCommand.ExecuteNonQuery()

						ConsTrnPk = Convert.ToString(DsMain.Tables["tblTransaction"].Rows[i]["REMOVALS_INVOICE_FK"]);
                        credittrn = Convert.ToDouble(Fetch_Console_trn_credit(Convert.ToInt16(ConsTrnPk)));
						crnAmt = Convert.ToDouble(DsMain.Tables["tblTransaction"].Rows[i]["CRN_AMT_IN_CRN_CUR"]) / Convert.ToDouble(DsMain.Tables["tblTransaction"].Rows[i]["EXCHANGE_RATE"]);

						if (Convert.ToInt32(ConsTrnPk) != 0) {
							arrMessage = Update_Console_invoice_trn_tbl(ConsTrnPk, credittrn, crnAmt, objWK.MyCommand);
							if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0)) {
								TRAN.Rollback();
								if (chkFlag) {
									RollbackProtocolKey("REMOVAL CREDIT NOTE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), CREDIT_NOTE_REF_NR, System.DateTime.Now);
								}
								return arrMessage;
							}
						}

						strpkConsolePk = Convert.ToString(DsMain.Tables["tblTransaction"].Rows[i]["INVOICE_TRN_FK"]);

						if (i == 0) {
							strConsTbl = Convert.ToDouble(Fetch_Console_credit(Convert.ToInt32(strpkConsolePk)));
						}
						strCredittot = strCredittot + crnAmt;
						// strConsTbl = strConsTbl + strCredittot

					}

					exe = Convert.ToInt16(insCommand.ExecuteNonQuery());
					double totCRNAmt = 0;

					totCRNAmt = strConsTbl + strCredittot;

					if (Convert.ToInt32(strpkConsolePk) != 0) {
						arrMessage = Update_Console_invoice_tbl(strpkConsolePk, totCRNAmt, objWK.MyCommand);
						if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0)) {
							TRAN.Rollback();
							if (chkFlag) {
                                RollbackProtocolKey("REMOVAL CREDIT NOTE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), CREDIT_NOTE_REF_NR, System.DateTime.Now);
                            }
							return arrMessage;
						}
					}
				}

				lngCreditNotePkTran = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);

				if (exe > 0) {
					if (CrLimit > 0) {
						SaveCreditLimit(CAmt, Customer, CrLimitUsed, TRAN);
					}

					//To save the CreditNote in Track N Trace table '
					//SaveTrackAndTraceForCrn(TRAN, lngCreditNotePk, 4, 1, "CreditNote", "CRN-AIR-EXP", _
					//         lngLocPk, objWK, "INS", nUserId, "O")

					arrMessage.Clear();
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					//StrPk = insCommand.Parameters["RETURN_VALUE"].Value
					return arrMessage;
				} else {
					if (chkFlag) {
                        RollbackProtocolKey("REMOVAL CREDIT NOTE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), CREDIT_NOTE_REF_NR, System.DateTime.Now);
                    }
					TRAN.Rollback();
				}

			} catch (OracleException oraexp) {
				TRAN.Rollback();
				//If CREDIT_NOTE_REF_NR = "" Then
				// CreditNoteNo = GenerateProtocolKey("REMOVAL CREDIT NOTE", lngLocPk, nEmpId, Today, "", "", "", nUserId)
				if (chkFlag) {
                    RollbackProtocolKey("REMOVAL CREDIT NOTE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), CREDIT_NOTE_REF_NR, System.DateTime.Now);
                }
				arrMessage.Add(oraexp.Message);
				return arrMessage;
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
            return new ArrayList();
		}
		#endregion

		//#Region "save TrackAndTrace"
		//        Public Function SaveTrackAndTraceForCrn(ByRef TRAN As OracleTransaction, _
		//                                                        ByVal PkValue As Integer, _
		//                                                        ByVal BizType As Integer, _
		//                                                        ByVal Process As Integer, _
		//                                                        ByVal Status As String, _
		//                                                        ByVal OnStatus As String, _
		//                                                        ByVal Locationfk As Integer, _
		//                                                        ByRef objWF As WorkFlow, _
		//                                                        ByVal flagInsUpd As String, _
		//                                                        ByVal lngCreatedby As Long, _
		//                                                        ByVal PkStatus As String _
		//                                                        ) As ArrayList

		//            Dim retVal As Int32
		//            Dim RecAfct As Int32

		//            Try
		//                arrMessage.Clear()
		//                With objWF.MyCommand
		//                    .CommandType = CommandType.StoredProcedure
		//                    .CommandText = objWF.MyUserName & ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS"
		//                    .Transaction = TRAN
		//                    .Parameters.Clear()
		//                    .Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input
		//                    .Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input
		//                    .Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input
		//                    .Parameters.Add("status_in", Status).Direction = ParameterDirection.Input
		//                    .Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input
		//                    .Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input
		//                    .Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input
		//                    .Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input
		//                    .Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input
		//                    .Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input
		//                    .Parameters.Add("Return_value", OracleClient.OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output
		//                    .ExecuteNonQuery()
		//                End With
		//                arrMessage.Add("All Data Saved Successfully")
		//                Return arrMessage
		//            Catch oraexp As OracleException
		//                Throw oraexp
		//            Catch ex As Exception
		//                Throw ex
		//            End Try
		//        End Function
		//#End Region

		#region " Supporting Function "

		private object ifDBNull(object col)
		{
			if (Convert.ToString(col).Length == 0) {
				return "";
			} else {
				return col;
			}
		}

		private object removeDBNull(object col)
		{
			if (object.ReferenceEquals(col, "")) {
				return "";
			}
			return col;
		}

		#endregion
	}
}
