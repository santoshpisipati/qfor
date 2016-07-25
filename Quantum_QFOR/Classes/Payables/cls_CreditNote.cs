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
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCreditNote : CommonFeatures
    {
        #region "Property"

        /// <summary>
        /// The LNG credit note pk
        /// </summary>
        private long lngCreditNotePk;

        /// <summary>
        /// Gets or sets the return save pk.
        /// </summary>
        /// <value>
        /// The return save pk.
        /// </value>
        public long ReturnSavePk
        {
            get { return lngCreditNotePk; }
            set { lngCreditNotePk = value; }
        }

        #endregion "Property"

        #region "Fetch Document Type "

        /// <summary>
        /// Fetches the type of the document.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchDocType()
        {
            try
            {
                string StrSql = null;
                WorkFlow objWF = new WorkFlow();
                StrSql = "Select  Document_Type_Mst_Fk,Document_Type  From Document_Type_Mst_Tbl";
                return objWF.GetDataSet(StrSql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Document Type "

        #region "For Enhance Search "

        /// <summary>
        /// Fetch_s the credit_ nr.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
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

            try
            {
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
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                cmd.Connection.Close();
            }
        }

        #endregion "For Enhance Search "

        #region "Fetch_Grid_Credit_Listing"

        /// <summary>
        /// Fetch_s the parent_ grid_list.
        /// </summary>
        /// <param name="Business_Type">Type of the business_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="Credit_note_type">The credit_note_type.</param>
        /// <param name="validFrom">The valid from.</param>
        /// <param name="validTo">The valid to.</param>
        /// <param name="InvRefNr">The inv reference nr.</param>
        /// <param name="strCustomerPk">The string customer pk.</param>
        /// <param name="strcreditPk">The strcredit pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Doctype">The doctype.</param>
        /// <param name="DocNr">The document nr.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CRNStatus">The CRN status.</param>
        /// <returns></returns>
        public DataSet Fetch_Parent_Grid_list(Int16 Business_Type, Int16 Process_Type, Int16 Credit_note_type, System.DateTime validFrom, System.DateTime validTo, string InvRefNr = "", Int16 strCustomerPk = 0, string strcreditPk = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        string Doctype = "", Int16 DocNr = 0, long usrLocFK = 0, Int32 flag = 0, Int32 CRNStatus = 0)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder strSql = new System.Text.StringBuilder();
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();
            string strSQL1 = null;

            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (string.IsNullOrEmpty(InvRefNr))
                InvRefNr = "0";
            if (string.IsNullOrEmpty(strcreditPk))
                strcreditPk = "0";
            if (string.IsNullOrEmpty(Doctype))
                Doctype = "0";

            if (Convert.ToInt32(InvRefNr) != 0)
            {
                strCondition += " AND  cntt.consol_invoice_trn_fk= " + InvRefNr;
            }

            if (strCustomerPk != 0)
            {
                strCondition += " AND  CNT.CUSTOMER_MST_FK =" + strCustomerPk;
            }

            if (Convert.ToInt32(strcreditPk) != 0)
            {
                strCondition += " AND cnt.crn_tbl_pk =  " + strcreditPk;
            }

            if (Convert.ToInt32(Doctype) != 1)
            {
                strCondition += " AND  cnt.document_type =" + Doctype;
            }

            if (DocNr != 0)
            {
                strCondition += " AND cnt.document_refrence =  " + DocNr;
            }

            if (CRNStatus != 0)
            {
                strCondition += " AND  cnt.CRN_STATUS =" + CRNStatus;
            }

            if (validFrom != null & validTo != null)
            {
                strCondition += " AND  cnt.credit_note_date  between to_date('" + validFrom + "','" + dateFormat + "') and to_date('" + validTo + "','" + dateFormat + "')";
            }
            else if (validFrom != null)
            {
                strCondition += " AND  cnt.credit_note_date >= to_date('" + validFrom + "','" + dateFormat + "')";
            }
            else if (validTo != null)
            {
                strCondition += " AND  cnt.credit_note_date <= to_date('" + validTo + "','" + dateFormat + "')";
            }

            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }

            strQuery.Append("SELECT Count(*)");
            strQuery.Append("  FROM CREDIT_NOTE_TBL       CNT,");
            if (Convert.ToInt32(InvRefNr) != 0)
            {
                strQuery.Append("       credit_note_trn_tbl   cntt,");
            }
            strQuery.Append("       CUSTOMER_MST_TBL      CMT,");
            strQuery.Append("       USER_MST_TBL          UMT");
            strQuery.Append("WHERE CNT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strQuery.Append(" AND CNT.CREATED_BY_FK= UMT.USER_MST_PK");
            if (Convert.ToInt32(InvRefNr) != 0)
            {
                strQuery.Append("      and cntt.crn_tbl_fk=cnt.crn_tbl_pk");
            }
            strQuery.Append("       AND CNT.BIZ_TYPE=" + Business_Type);
            strQuery.Append("       AND CNT.PROCESS_TYPE=" + Process_Type);
            strQuery.Append("       AND cnt.credit_note_type=" + Credit_note_type);
            strQuery.Append("       AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
            strQuery.Append("");

            strQuery.Append(strCondition);
            strSQL1 = strQuery.ToString();

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
            {
                CurrentPage = 1;
            }
            if (TotalRecords == 0)
            {
                CurrentPage = 0;
            }
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            strQuery.Remove(0, strQuery.Length);

            strSql.Append(" select * from (");
            strSql.Append("SELECT ROWNUM SR_NO,q.* FROM (");

            strSql.Append("SELECT");
            if (Convert.ToInt32(InvRefNr) != 0)
            {
                strSql.Append("  distinct     CNT.CUSTOMER_MST_FK Custumer_pk,");
            }
            else
            {
                strSql.Append("       CNT.CUSTOMER_MST_FK Custumer_pk,");
            }
            strSql.Append("       CMT.CUSTOMER_NAME Customer,");
            strSql.Append("       CNT.CRN_TBL_PK CRN_TBl,");
            strSql.Append("       CNT.CREDIT_NOTE_REF_NR Credit_note1,");
            strSql.Append("       TO_DATE(CNT.CREDIT_NOTE_DATE,'" + dateFormat + "') CRedit_Note_date,");
            strSql.Append("       FN_CUR_CRN_AMT(CNT.CRN_TBL_PK) Amount,");
            strSql.Append("       ''DOCUMENT_PK,");
            strSql.Append("       ''DOCUMENT_TYPE,");
            strSql.Append("       ''DOCUMENT_REFRENCE,");
            strSql.Append("       ''CURRENCY_MST_FK,");
            strSql.Append("       ''CURRENCY_NAME,");
            strSql.Append("       ''CRN_AMMOUNT,");
            strSql.Append("        DECODE(CNT.CRN_STATUS,null,'Approved',1,'Approved',2,'Cancelled') crnstatus,");
            strSql.Append("       'FALSE' Sel");
            strSql.Append("  FROM CREDIT_NOTE_TBL       CNT,");
            if (Convert.ToInt32(InvRefNr) != 0)
            {
                strSql.Append("       credit_note_trn_tbl   cntt,");
            }
            strSql.Append("       CUSTOMER_MST_TBL      CMT,");
            strSql.Append("       USER_MST_TBL          UMT");
            strSql.Append("WHERE CNT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strSql.Append(" AND CNT.CREATED_BY_FK= UMT.USER_MST_PK");
            if (Convert.ToInt32(InvRefNr) != 0)
            {
                strSql.Append("      and cntt.crn_tbl_fk=cnt.crn_tbl_pk");
            }
            strSql.Append("       AND CNT.BIZ_TYPE=" + Business_Type);
            strSql.Append("       AND CNT.PROCESS_TYPE=" + Process_Type);
            strSql.Append("       AND cnt.credit_note_type=" + Credit_note_type);
            strSql.Append("       AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
            strSql.Append("");
            strSql.Append(strCondition);
            strSql.Append("  ORDER BY  crn_tbl_pk  desc)Q)");
            strSql.Append(" WHERE SR_NO  Between " + start + " and " + last);

            string sql = null;
            sql = strSql.ToString();
            DataSet DS = new DataSet();

            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;

                DS.Tables.Add(FetchChildGrid_list(AllMasterPKs_list(DS), Business_Type, Process_Type, Credit_note_type, InvRefNr, strCustomerPk.ToString(), Convert.ToInt16(strcreditPk), CurrentPage, TotalPage, Doctype,
                DocNr.ToString(), validFrom.ToString(), validTo.ToString(), usrLocFK));
                CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CRN_TBl"], DS.Tables[1].Columns["CRNTBL"], true);
                CONTRel.Nested = true;
                DS.Relations.Add(CONTRel);
                return DS;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch_Grid_Credit_Listing"

        #region " All Master Supplier PKs "

        /// <summary>
        /// Alls the master p ks_list.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        private string AllMasterPKs_list(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append("-1,");
            for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
            {
                strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["CRN_TBl"]).Trim() + ",");
            }
            strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion " All Master Supplier PKs "

        #region "FetchChildGrid_list "

        /// <summary>
        /// Fetches the child grid_list.
        /// </summary>
        /// <param name="SUPPLIERPKs">The supplierp ks.</param>
        /// <param name="Business_Type">Type of the business_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="Credit_note_type">The credit_note_type.</param>
        /// <param name="InvRefNr">The inv reference nr.</param>
        /// <param name="strCustomerPk">The string customer pk.</param>
        /// <param name="strcreditPk">The strcredit pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Doctype">The doctype.</param>
        /// <param name="DocNr">The document nr.</param>
        /// <param name="validFrom">The valid from.</param>
        /// <param name="validTo">The valid to.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataTable FetchChildGrid_list(string SUPPLIERPKs = "", Int16 Business_Type = 0, Int16 Process_Type = 0, Int16 Credit_note_type = 0, string InvRefNr = "", string strCustomerPk = "0", Int16 strcreditPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, string Doctype = "",
        string DocNr = "", string validFrom = "", string validTo = "", long usrLocFK = 0)
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

            if (Convert.ToInt32(InvRefNr) != 0)
            {
                strCondition += " AND  cntt.consol_invoice_trn_fk = " + InvRefNr;
            }
            if (Convert.ToInt32(strCustomerPk) != 0)
            {
                strCondition += " AND  CNT.CUSTOMER_MST_FK =" + strCustomerPk;
            }
            if (strcreditPk != 0)
            {
                strCondition += " AND cnt.crn_tbl_pk =  " + strcreditPk;
            }

            if (Convert.ToInt32(Doctype) != 1)
            {
                strCondition += " AND  cnt.document_type =" + Doctype;
            }
            if (Convert.ToInt32(DocNr) != 0)
            {
                strCondition += " AND cnt.document_refrence =  " + DocNr;
            }
            if (validFrom != "00:00:00" & validTo != "00:00:00")
            {
                strCondition += " AND  cnt.credit_note_date  between to_date('" + validFrom + "','" + dateFormat + "') and to_date('" + validTo + "','" + dateFormat + "')";
            }

            strSql.Append(" select * from (");
            strSql.Append("SELECT ROWNUM SR_NO,q.* FROM (");

            strSql.Append("SELECT");
            strSql.Append("        '' CUSTOMER_MST_FK,");
            strSql.Append("        '' CUSTOMER_NAME,");
            strSql.Append("        CNT.CRN_TBL_PK CRNTBL,");
            strSql.Append("        '' CREDIT_NOTE_REF_NR,");
            strSql.Append("        '' CREDIT_NOTE_DATE,");
            strSql.Append("        '' CRN_AMMOUNT,");
            strSql.Append("       CNT.DOCUMENT_TYPE DOCTYPE1,");
            strSql.Append("       CNT.DOCUMENT_TYPE DOCTYPE,");
            strSql.Append("       CNT.DOCUMENT_REFRENCE DOCREF,");
            strSql.Append("       CNT.CURRENCY_MST_FK CURRMSTFK,");
            strSql.Append("      ctmt.currency_id CURRNAM,");
            strSql.Append("      FN_CUR_CRN_AMT(CNT.CRN_TBL_PK) CRNAMT,");
            strSql.Append("       ' ' SEL");
            strSql.Append("  FROM CREDIT_NOTE_TBL       CNT,");
            if (Convert.ToInt32(InvRefNr) != 0)
            {
                strSql.Append("       credit_note_trn_tbl   cntt,");
            }
            strSql.Append("       CURRENCY_TYPE_MST_TBL CTMT,");
            strSql.Append("       USER_MST_TBL          UMT");
            strSql.Append("WHERE CNT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
            strSql.Append(" AND CNT.CREATED_BY_FK= UMT.USER_MST_PK");
            if (Convert.ToInt32(InvRefNr) != 0)
            {
                strSql.Append("      and cntt.crn_tbl_fk=cnt.crn_tbl_pk");
            }
            strSql.Append("       AND CNT.BIZ_TYPE=" + Business_Type);
            strSql.Append("       AND CNT.PROCESS_TYPE=" + Process_Type);
            strSql.Append("       AND cnt.credit_note_type=" + Credit_note_type);
            strSql.Append("       AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
            strSql.Append("");

            if (SUPPLIERPKs.Trim().Length > 0)
            {
                strSql.Append(" AND CNT.CRN_TBL_PK  in (" + SUPPLIERPKs + ") ");
            }

            strSql.Append(strCondition);
            strSql.Append(" ORDER BY   crn_tbl_pk  desc )Q)");
            string sql = null;
            sql = strSql.ToString();
            DataTable dt = null;

            try
            {
                pk = -1;
                dt = objWF.GetDataTable(sql);
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt]["CRNTBL"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["CRNTBL"]);
                        Rno = 0;
                    }
                    Rno += 1;
                    dt.Rows[RowCnt]["SR_NO"] = Rno;
                }

                return dt;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "FetchChildGrid_list "

        #region "Fetch_Grid_Credit_Entry"

        /// <summary>
        /// Fetch_s the parent_ grid.
        /// </summary>
        /// <param name="Business_Type">Type of the business_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="intBaseDate">The int base date.</param>
        /// <param name="intBaseCurrPk">The int base curr pk.</param>
        /// <param name="InvRefNr">The inv reference nr.</param>
        /// <param name="strCustomerPk">The string customer pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <returns></returns>
        public DataSet Fetch_Parent_Grid(Int16 Business_Type, Int16 Process_Type, string intBaseDate, Int16 intBaseCurrPk, string InvRefNr = "", Int16 strCustomerPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExType = 1)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder strSql = new System.Text.StringBuilder();
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();
            string strSQL1 = null;

            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (!string.IsNullOrEmpty(InvRefNr))
            {
                strCondition += " AND  CIT.INVOICE_REF_NO =  '" + InvRefNr + "'  ";
            }
            if (strCustomerPk != 0)
            {
                strCondition += " AND  CIT.CUSTOMER_MST_FK =" + strCustomerPk;
            }

            if (BlankGrid == 0)
            {
                strCondition += " AND 1=2 ";
            }

            strQuery.Append("SELECT Count(*)");
            strQuery.Append("  FROM CONSOL_INVOICE_TBL      CIT,");
            strQuery.Append("      CURRENCY_TYPE_MST_TBL   CTMT,");
            strQuery.Append("       CUSTOMER_MST_TBL        CMT");
            strQuery.Append("       where CIT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
            strQuery.Append("       AND CIT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strQuery.Append("       AND CIT.BUSINESS_TYPE=" + Business_Type);
            strQuery.Append("       AND CIT.PROCESS_TYPE=" + Process_Type);
            strQuery.Append("");
            strQuery.Append(strCondition);
            strSQL1 = strQuery.ToString();
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
            {
                CurrentPage = 1;
            }
            if (TotalRecords == 0)
            {
                CurrentPage = 0;
            }
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;
            strQuery.Remove(0, strQuery.Length);
            strSql.Append(" select * from (");
            strSql.Append("SELECT ROWNUM SR_NO,q.* FROM (");
            strSql.Append("SELECT");
            strSql.Append("       CIT.CONSOL_INVOICE_PK,");
            strSql.Append("       '' consol_invoice_trn_fk,");
            strSql.Append("       CIT.INVOICE_REF_NO,");
            strSql.Append("       '' FREIGHT_ELEMENT_Pk,");
            strSql.Append("       '' FREIGHT_ELEMENT_NAME,");
            strSql.Append("       '' CURRENCY_MST_PK,");
            strSql.Append("       CTMT.CURRENCY_ID,");
            strSql.Append("       CIT.NET_RECEIVABLE,");
            if (Process_Type == 2 & ExType == 3)
            {
                strSql.Append("       ROUND((SELECT GET_EX_RATE1( " + intBaseCurrPk + ",CIT.CURRENCY_MST_FK,TO_DATE('" + intBaseDate + "',DATEFORMAT )," + ExType + ") FROM DUAL),6) ROE,");
                strSql.Append("       ROUND((CIT.NET_RECEIVABLE) * (SELECT GET_EX_RATE1( " + intBaseCurrPk + ",CIT.CURRENCY_MST_FK,TO_DATE('" + intBaseDate + "',DATEFORMAT )," + ExType + ") FROM DUAL), 2) AMTINLOC,");
                strSql.Append("       ROUND((cit.total_credit_note_amt) * (SELECT GET_EX_RATE1( " + intBaseCurrPk + ",CIT.CURRENCY_MST_FK,TO_DATE('" + intBaseDate + "',DATEFORMAT )," + ExType + ") FROM DUAL),2) Crntot ,");
            }
            else
            {
                strSql.Append("       ROUND((SELECT GET_EX_RATE( CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL),6) ROE,");
                strSql.Append("       ROUND((CIT.NET_RECEIVABLE) * (SELECT GET_EX_RATE( CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL), 2) AMTINLOC,");
                strSql.Append("       ROUND((cit.total_credit_note_amt) * (SELECT GET_EX_RATE( CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL),2) Crntot ,");
            }
            strSql.Append("       '' CRNCURRENT,");
            strSql.Append("       'FALSE' SEL");
            strSql.Append("  FROM CONSOL_INVOICE_TBL      CIT,");
            strSql.Append("      CURRENCY_TYPE_MST_TBL   CTMT,");
            strSql.Append("       CUSTOMER_MST_TBL        CMT");
            strSql.Append("       where CIT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
            strSql.Append("       AND CIT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strSql.Append("       AND CIT.BUSINESS_TYPE=" + Business_Type);
            strSql.Append("       AND CIT.PROCESS_TYPE=" + Process_Type);
            strSql.Append("");
            strSql.Append(strCondition);
            strSql.Append(" ORDER BY  INVOICE_REF_NO )q)");
            strSql.Append(" WHERE SR_NO  Between " + start + " and " + last);

            string sql = null;
            sql = strSql.ToString();
            DataSet DS = new DataSet();

            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;

                DS.Tables.Add(FetchChildGrid(AllMasterPKs(DS), Business_Type, Process_Type, intBaseDate, intBaseCurrPk, InvRefNr, strCustomerPk.ToString(), CurrentPage, TotalPage, ExType));

                CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CONSOL_INVOICE_PK"], DS.Tables[1].Columns["consol_invoice_fk"], true);
                CONTRel.Nested = true;
                DS.Relations.Add(CONTRel);
                return DS;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch_Grid_Credit_Entry"

        #region " All Master Supplier PKs "

        /// <summary>
        /// Alls the master p ks.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        private string AllMasterPKs(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append("-1,");
            for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
            {
                strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["CONSOL_INVOICE_PK"]).Trim() + ",");
            }
            strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion " All Master Supplier PKs "

        #region "FetchChildGrid"

        /// <summary>
        /// Fetches the child grid.
        /// </summary>
        /// <param name="SUPPLIERPKs">The supplierp ks.</param>
        /// <param name="Business_Type">Type of the business_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="intBaseDate">The int base date.</param>
        /// <param name="intBaseCurrPk">The int base curr pk.</param>
        /// <param name="InvRefNr">The inv reference nr.</param>
        /// <param name="strCustomerPk">The string customer pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <returns></returns>
        public DataTable FetchChildGrid(string SUPPLIERPKs = "", Int16 Business_Type = 0, Int16 Process_Type = 0, string intBaseDate = "", Int16 intBaseCurrPk = 0, string InvRefNr = "", string strCustomerPk = "0", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExType = 1)
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
            if (!string.IsNullOrEmpty(InvRefNr))
            {
                strCondition += " AND CIT.INVOICE_REF_NO = '" + InvRefNr + "'  ";
            }
            if (Convert.ToInt32(strCustomerPk) != 0)
            {
                strCondition += " AND CIT.CUSTOMER_MST_FK =" + strCustomerPk;
            }
            strSql.Append("select * from(");
            strSql.Append("SELECT ROWNUM SR_NO ,Q.*FROM ( SELECT");

            strSql.Append("       citt.consol_invoice_trn_pk,");
            strSql.Append("       citt.consol_invoice_fk ,");
            strSql.Append("       CIT.INVOICE_REF_NO,");
            strSql.Append("       FEMT.FREIGHT_ELEMENT_MST_PK,");
            strSql.Append("       FEMT.FREIGHT_ELEMENT_NAME,");
            strSql.Append("       CTMT.CURRENCY_MST_PK,");
            strSql.Append("       CTMT.CURRENCY_ID,");
            strSql.Append("       CITT.TOT_AMT totamt,");
            if (Process_Type == 2 & ExType == 3)
            {
                strSql.Append("       ROUND((SELECT GET_EX_RATE1(" + intBaseCurrPk + ",CIT.CURRENCY_MST_FK,TO_DATE('" + intBaseDate + "',DATEFORMAT )," + ExType + ") FROM DUAL),6) ROE,");
                strSql.Append("       abs(ROUND((CITT.TOT_AMT) * (SELECT GET_EX_RATE1(" + intBaseCurrPk + ",CIT.CURRENCY_MST_FK,TO_DATE('" + intBaseDate + "',DATEFORMAT )," + ExType + ") FROM DUAL), 2)) CRNTOTAlLOCAL,");
                strSql.Append("       ROUND((citt.total_credit_amt) * (SELECT GET_EX_RATE1( " + intBaseCurrPk + ",CIT.CURRENCY_MST_FK,TO_DATE('" + intBaseDate + "',DATEFORMAT )," + ExType + ") FROM DUAL),2) crntrntot,");
            }
            else
            {
                strSql.Append("       ROUND((SELECT GET_EX_RATE(CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL),6) ROE,");
                strSql.Append("       abs(ROUND((CITT.TOT_AMT) * (SELECT GET_EX_RATE(CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL), 2)) CRNTOTAlLOCAL,");
                strSql.Append("       ROUND((citt.total_credit_amt) * (SELECT GET_EX_RATE(CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL),2) crntrntot,");
            }
            strSql.Append("       '' CRNCURRENT,");
            strSql.Append("       'FALSE' SEL");
            strSql.Append("  FROM CONSOL_INVOICE_TBL      CIT,");
            strSql.Append("       CONSOL_INVOICE_TRN_TBL  CITT,");
            strSql.Append("       CURRENCY_TYPE_MST_TBL   CTMT,");
            strSql.Append("       CUSTOMER_MST_TBL        CMT,");
            strSql.Append("       FREIGHT_ELEMENT_MST_TBL FEMT");
            strSql.Append("       where CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            strSql.Append("       and  CIT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
            strSql.Append("       AND CIT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strSql.Append("       AND CITT.Frt_Oth_Element_Fk = FEMT.FREIGHT_ELEMENT_MST_PK");
            strSql.Append("       AND CIT.BUSINESS_TYPE=" + Business_Type);
            strSql.Append("       AND CIT.PROCESS_TYPE=" + Process_Type);
            strSql.Append("");
            if (SUPPLIERPKs.Trim().Length > 0)
            {
                strSql.Append(" AND citt.consol_invoice_fk in (" + SUPPLIERPKs + ") ");
            }
            strSql.Append(strCondition);
            strSql.Append(" ORDER BY  INVOICE_REF_NO )Q)");

            string sql = null;
            sql = strSql.ToString();
            DataTable dt = null;

            try
            {
                pk = -1;
                dt = objWF.GetDataTable(sql);
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt]["consol_invoice_fk"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["consol_invoice_fk"]);
                        Rno = 0;
                    }
                    Rno += 1;
                    dt.Rows[RowCnt]["SR_NO"] = Rno;
                }
                return dt;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "FetchChildGrid"

        #region "Fetch_Grid_From_listing"

        /// <summary>
        /// Fetch_s the parent_ grid_edit.
        /// </summary>
        /// <param name="Business_Type">Type of the business_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="intBaseDate">The int base date.</param>
        /// <param name="intBaseCurrPk">The int base curr pk.</param>
        /// <param name="Crn_pk">The CRN_PK.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Extype">The extype.</param>
        /// <returns></returns>
        public DataSet Fetch_Parent_Grid_edit(Int16 Business_Type, Int16 Process_Type, string intBaseDate, Int16 intBaseCurrPk, string Crn_pk, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 Extype = 1)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder strSql = new System.Text.StringBuilder();
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();
            string strSQL1 = null;

            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            strQuery.Append("SELECT Count(*)");
            strQuery.Append("  FROM CONSOL_INVOICE_TBL      CIT,");
            strQuery.Append("       credit_note_tbl         CNT,");
            strQuery.Append("      credit_note_trn_tbl     cntt,");
            strQuery.Append("       CONSOL_INVOICE_TRN_TBL  CITT,");
            strQuery.Append("        CURRENCY_TYPE_MST_TBL   CTMT");
            strQuery.Append("  where  citt.consol_invoice_fk = cntt.consol_invoice_trn_fk (+)");
            strQuery.Append("       and CNT.CRN_TBL_PK = cntt.crn_tbl_fk");
            strQuery.Append("       and CITT.Consol_Invoice_Fk = cit.consol_invoice_pk");
            strQuery.Append("       and cit.currency_mst_fk = ctmt.currency_mst_pk(+)");
            strQuery.Append("       AND CIT.BUSINESS_TYPE=" + Business_Type);
            strQuery.Append("       AND CIT.PROCESS_TYPE=" + Process_Type);
            strQuery.Append("       AND cnt.crn_tbl_pk= " + Crn_pk);
            strQuery.Append("");
            strSQL1 = strQuery.ToString();
            strQuery.Remove(0, strQuery.Length);
            strSql.Append(" SELECT");
            strSql.Append("       distinct cit.consol_invoice_pk,");
            strSql.Append("       '' consol_invoice_trn_fk,");
            strSql.Append("        CIT.INVOICE_REF_NO,");
            strSql.Append("        '' FREIGHT_ELEMENT_Pk,");
            strSql.Append("       '' FREIGHT_ELEMENT_NAME,");
            strSql.Append("        CTMT.CURRENCY_MST_PK,");
            strSql.Append("       CTMT.CURRENCY_ID,");
            strSql.Append("        CIT.NET_RECEIVABLE,");
            strSql.Append(" ROUND((SELECT GET_EX_RATE1(CIT.CURRENCY_MST_FK," + intBaseCurrPk + ",TO_DATE(CNT.CREDIT_NOTE_DATE,DATEFORMAT ), " + Extype + " ) FROM DUAL),6) ROE,");
            strSql.Append("      (CIT.NET_RECEIVABLE *  CNTT.EXCHANGE_RATE) AMTINLOC,");
            strSql.Append("       (cit.total_credit_note_amt*  CNTT.EXCHANGE_RATE) Crntot,");
            strSql.Append("      FN_CUR_CRN_AMT(CNT.CRN_TBL_PK) CRNCURRENT,");
            strSql.Append("       'FALSE' SEL");
            strSql.Append("  FROM CONSOL_INVOICE_TBL      CIT,");
            strSql.Append("       credit_note_tbl         CNT,");
            strSql.Append("      credit_note_trn_tbl     cntt,");
            strSql.Append("       CONSOL_INVOICE_TRN_TBL  CITT,");
            strSql.Append("        CURRENCY_TYPE_MST_TBL   CTMT");
            strSql.Append("       where  citt.consol_invoice_fk = cntt.consol_invoice_trn_fk (+)");
            strSql.Append("       and CNT.CRN_TBL_PK = cntt.crn_tbl_fk");
            strSql.Append("       and CITT.Consol_Invoice_Fk = cit.consol_invoice_pk");
            strSql.Append("       and cit.currency_mst_fk = ctmt.currency_mst_pk(+)");
            strSql.Append("       AND CIT.BUSINESS_TYPE=" + Business_Type);
            strSql.Append("       AND CIT.PROCESS_TYPE=" + Process_Type);
            strSql.Append("       AND cnt.crn_tbl_pk= " + Crn_pk);

            strSql.Append(strCondition);

            strSQL1 = " select count(*) from (";
            strSQL1 += strSql.ToString() + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
            {
                CurrentPage = 1;
            }
            if (TotalRecords == 0)
            {
                CurrentPage = 0;
            }
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            strSql.Append(" ORDER BY  INVOICE_REF_NO ");

            strSQL1 = " select * from (SELECT ROWNUM SR_NO,q.* FROM (";
            strSQL1 += strSql.ToString();
            strSQL1 += " )q ) WHERE SR_NO Between " + start + " and " + last;

            string sql = null;
            sql = strSQL1.ToString();
            DataSet DS = new DataSet();

            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;

                DS.Tables.Add(FetchChildGrid_edit(AllMasterPKs_edit(DS), Business_Type, Process_Type, Crn_pk, intBaseDate, intBaseCurrPk));

                CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CONSOL_INVOICE_PK"], DS.Tables[1].Columns["consol_invoice_fk"], true);
                CONTRel.Nested = true;
                DS.Relations.Add(CONTRel);
                return DS;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch_Grid_From_listing"

        #region " All Master Supplier PKs "

        /// <summary>
        /// Alls the master p ks_edit.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        private string AllMasterPKs_edit(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append("-1,");
            for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
            {
                strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["CONSOL_INVOICE_PK"]).Trim() + ",");
            }
            strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion " All Master Supplier PKs "

        #region "FetchChildGrid_edit"

        /// <summary>
        /// Fetches the child grid_edit.
        /// </summary>
        /// <param name="SUPPLIERPKs">The supplierp ks.</param>
        /// <param name="Business_Type">Type of the business_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="Crn_pk">The CRN_PK.</param>
        /// <param name="intBaseDate">The int base date.</param>
        /// <param name="intBaseCurrPk">The int base curr pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataTable FetchChildGrid_edit(string SUPPLIERPKs = "", Int16 Business_Type = 0, Int16 Process_Type = 0, string Crn_pk = "", string intBaseDate = "", Int16 intBaseCurrPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
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
            strSql.Append("       DISTINCT 0 consol_invoice_trn_pk,");
            strSql.Append("       citt.consol_invoice_fk ,");
            strSql.Append("       CIT.INVOICE_REF_NO,");
            strSql.Append("       cntt.frt_oth_element_fk,");
            strSql.Append("       FEMT.FREIGHT_ELEMENT_NAME,");
            strSql.Append("       cntt.CURRENCY_MST_fK,");
            strSql.Append("       CTMT.CURRENCY_ID,");
            strSql.Append("       cntt.element_inv_amt totamt,");
            strSql.Append("       cntt.exchange_rate ROE,");
            strSql.Append("       (cntt.element_inv_amt * cntt.exchange_rate) CRNTOTAlLOCAL ,");
            strSql.Append("       (cNtt.Crn_Amt_In_Crn_Cur ) crntrntot,");
            strSql.Append("       (cNtt.Crn_Amt_In_Crn_Cur ) CRNCURRENT,");
            strSql.Append("       'TRUE' SEL");
            strSql.Append("  FROM credit_note_tbl        CNT,");
            strSql.Append("       credit_note_trn_tbl    cntt,");
            strSql.Append("       CONSOL_INVOICE_TRN_TBL  CITT,");
            strSql.Append("       CONSOL_INVOICE_TBL      CIT,");
            strSql.Append("       CURRENCY_TYPE_MST_TBL   CTMT,");
            strSql.Append("       FREIGHT_ELEMENT_MST_TBL FEMT");
            strSql.Append("     where  citt.consol_invoice_fk = cntt.consol_invoice_trn_fk (+)");
            strSql.Append("       and cnt.crn_tbl_pk = cntt.crn_tbl_fk");
            strSql.Append("       and citt.consol_invoice_fk = cit.consol_invoice_pk");
            strSql.Append("      and citt.frt_oth_element_fk = femt.freight_element_mst_pk");
            strSql.Append("       and cntt.frt_oth_element_fk = femt.freight_element_mst_pk(+)");
            strSql.Append("       and cntt.currency_mst_fk = ctmt.currency_mst_pk");
            strSql.Append("       AND CIT.BUSINESS_TYPE=" + Business_Type);
            strSql.Append("       AND CIT.PROCESS_TYPE=" + Process_Type);
            strSql.Append("       AND cnt.crn_tbl_pk= " + Crn_pk);
            strSql.Append("");

            if (SUPPLIERPKs.Trim().Length > 0)
            {
                strSql.Append(" AND citt.consol_invoice_fk  in (" + SUPPLIERPKs + ") ");
            }

            strSql.Append(strCondition);

            strSql.Append(" ORDER BY  INVOICE_REF_NO )Q)");
            string sql = null;
            sql = strSql.ToString();
            DataTable dt = null;

            try
            {
                pk = -1;
                dt = objWF.GetDataTable(sql);
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt]["consol_invoice_fk"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["consol_invoice_fk"]);
                        Rno = 0;
                    }
                    Rno += 1;
                    dt.Rows[RowCnt]["SR_NO"] = Rno;
                }
                return dt;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "FetchChildGrid_edit"

        #region "Fetch Header as from listing "

        /// <summary>
        /// Fetch_headers the specified CRN_PK.
        /// </summary>
        /// <param name="crn_pk">The CRN_PK.</param>
        /// <returns></returns>
        public DataSet fetch_header(string crn_pk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("");
                strQuery.Append("SELECT CNT.PROCESS_TYPE,");
                strQuery.Append("       CNT.BIZ_TYPE,");
                strQuery.Append("       cnt.crn_tbl_pk,");
                strQuery.Append("       CNT.CREDIT_NOTE_TYPE,");
                strQuery.Append("       CNT.CREDIT_NOTE_REF_NR,");
                strQuery.Append("       CNT.CREDIT_NOTE_DATE,");
                strQuery.Append("       CNT.CURRENCY_MST_FK,");
                strQuery.Append("       CMT.CUSTOMER_ID,");
                strQuery.Append("       CTMT.CURRENCY_NAME,");
                strQuery.Append("       CNT.CUSTOMER_MST_FK,");
                strQuery.Append("       CMT.CUSTOMER_NAME,");
                strQuery.Append("       CNT.DOCUMENT_TYPE,");
                strQuery.Append("       CNT.crn_ammount,");
                strQuery.Append("       cnt.version_no,");
                strQuery.Append("       CNT.DOCUMENT_REFRENCE,");
                strQuery.Append("       CNT.CRN_STATUS,");
                strQuery.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
                strQuery.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
                strQuery.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
                strQuery.Append("   TO_DATE(CNT.CREATED_DT) CREATED_BY_DT, ");
                strQuery.Append("   TO_DATE(CNT.LAST_MODIFIED_DT) LAST_MODIFIEDBY_DT, ");
                strQuery.Append("   TO_DATE(CNT.LAST_MODIFIED_DT) APPROVED_DT ");

                strQuery.Append("  FROM CREDIT_NOTE_TBL       CNT,");
                strQuery.Append("       CURRENCY_TYPE_MST_TBL CTMT,");
                strQuery.Append("       CUSTOMER_MST_TBL      CMT ,");

                strQuery.Append("  USER_MST_TBL UMTCRT, ");
                strQuery.Append("  USER_MST_TBL UMTUPD, ");
                strQuery.Append("  USER_MST_TBL UMTAPP ");

                strQuery.Append(" WHERE  cnt.crn_tbl_pk= " + crn_pk);
                strQuery.Append(" AND CNT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
                strQuery.Append(" AND CNT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");

                strQuery.Append(" AND UMTCRT.USER_MST_PK(+) = CNT.CREATED_BY_FK ");
                strQuery.Append(" AND UMTUPD.USER_MST_PK(+) = CNT.LAST_MODIFIED_BY_FK  ");
                strQuery.Append(" AND UMTAPP.USER_MST_PK(+) = CNT.LAST_MODIFIED_BY_FK  ");

                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Header as from listing "

        #region "Fetch_Master_pk"

        /// <summary>
        /// Fetchpks the specified pk.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet fetchpk(string pk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("");
                strQuery.Append(" select cntt.crn_trn_tbl_pk from  credit_note_trn_tbl  cntt");
                strQuery.Append(" where cntt.crn_tbl_fk=" + pk);

                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch_Master_pk"

        #region "fetch_Cutumer_pk "

        /// <summary>
        /// Fetch_s the cust_pk.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet fetch_Cust_pk(Int32 pk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                strQuery.Append("  from consol_invoice_tbl CIT, customer_mst_tbl cmt");
                strQuery.Append(" where cit.customer_mst_fk = cmt.customer_mst_pk");
                strQuery.Append("   and cit.consol_invoice_pk = " + pk);
                strQuery.Append("");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "fetch_Cutumer_pk "

        #region "Fetch Document refrence Nr"

        /// <summary>
        /// Fetch_s the doc_ reference.
        /// </summary>
        /// <param name="process">The process.</param>
        /// <param name="biz">The biz.</param>
        /// <param name="docType">Type of the document.</param>
        /// <param name="DocNr">The document nr.</param>
        /// <returns></returns>
        public DataSet Fetch_Doc_Ref(Int32 process, Int32 biz, Int32 docType, string DocNr)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (string.IsNullOrEmpty(DocNr))
                {
                    DocNr = "0";
                }
                if (process == 1 & biz == 2)
                {
                    if (docType == 2)
                    {
                        strQuery.Append("SELECT HBE.HBL_REF_NO  refdocnr  FROM HBL_EXP_TBL HBE WHERE HBE.HBL_EXP_TBL_PK =" + DocNr);
                    }
                    else
                    {
                        strQuery.Append("");
                        strQuery.Append("select jcset.jobcard_ref_no refdocnr ");
                        strQuery.Append("  from JOB_CARD_TRN jcset");
                        strQuery.Append("where jcset.JOB_CARD_TRN_PK =" + DocNr);
                        strQuery.Append("");
                    }
                }
                else if (process == 1 & biz == 1)
                {
                    if (docType == 3)
                    {
                        strQuery.Append("select  HET.HAWB_REF_NO  refdocnr from hawb_exp_tbl HET where  HET.HAWB_EXP_TBL_PK=" + DocNr);
                    }
                    else
                    {
                        strQuery.Append("select jcaet.jobcard_ref_no refdocnr ");
                        strQuery.Append("  from JOB_CARD_TRN jcaet");
                        strQuery.Append(" where jcaet.JOB_CARD_TRN_PK =" + DocNr);
                    }

                    // import sea
                }
                else if (process == 2 & biz == 2)
                {
                    strQuery.Append("");
                    strQuery.Append(" Select Case jcsit.jobcard_ref_no refdocnr ");
                    strQuery.Append("  from JOB_CARD_TRN jcsit");
                    strQuery.Append(" where jcsit.JOB_CARD_TRN_PK =" + DocNr);
                    strQuery.Append("");

                    // import air
                }
                else if (process == 2 & biz == 1)
                {
                    strQuery.Append("select jcait.jobcard_ref_no refdocnr ");
                    strQuery.Append("  from JOB_CARD_TRN jcait");
                    strQuery.Append(" where jcait.JOB_CARD_TRN_PK =" + DocNr);
                }
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Document refrence Nr"

        #region "Fetch_Customer_invoice_entry"

        /// <summary>
        /// Fetches the customer invoice.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
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
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CONSOL_INVOICE_JOB_PKG.GET_INVOICE_JOB";

                var _with6 = selectCommand.Parameters;
                _with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with6.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
                _with6.Add("CUSTOMER_PK_IN", getDefault(strcustpk, "")).Direction = ParameterDirection.Input;
                _with6.Add("LOCFK_IN", strloc).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Fetch_Customer_invoice_entry"

        #region "Fetch_Customer_invoice_lISTING"

        /// <summary>
        /// Fetches the customer invoice_listing.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
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

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CONSOL_INVOICE_JOB_PKG.GET_INVOICE_JOB_LIST";

                var _with7 = selectCommand.Parameters;
                _with7.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with7.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with7.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
                _with7.Add("CUSTOMER_PK_IN", getDefault(strcustpk, "")).Direction = ParameterDirection.Input;
                _with7.Add("LOCFK_IN", strloc).Direction = ParameterDirection.Input;
                _with7.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Fetch_Customer_invoice_lISTING"

        #region "fetch_Console_trn_CreditLimit "

        /// <summary>
        /// Fetch_s the console_trn_credit.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public string Fetch_Console_trn_credit(Int32 pk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string CreditVal = null;
            DataSet dsCredit = new DataSet();

            try
            {
                strQuery.Append("select cit.total_credit_amt from  consol_invoice_trn_tbl cit");
                strQuery.Append("  where cit.consol_invoice_trn_pk  = " + pk);

                dsCredit = objWF.GetDataSet(strQuery.ToString());

                if (dsCredit.Tables[0].Rows.Count > 0)
                {
                    CreditVal = Convert.ToString(getDefault(dsCredit.Tables[0].Rows[0]["total_credit_amt"], 0));
                }
                return CreditVal;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "fetch_Console_trn_CreditLimit "

        #region "Fetch_Console_Tbl"

        /// <summary>
        /// Fetch_s the console_credit.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public string Fetch_Console_credit(Int32 pk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string CreditVal = null;
            DataSet dsCredit = new DataSet();

            try
            {
                strQuery.Append("select cit.total_credit_note_amt from  consol_invoice_tbl cit");
                strQuery.Append("  where cit.consol_invoice_pk = " + pk);

                dsCredit = objWF.GetDataSet(strQuery.ToString());

                if (dsCredit.Tables[0].Rows.Count > 0)
                {
                    CreditVal = Convert.ToString(getDefault(dsCredit.Tables[0].Rows[0]["total_credit_note_amt"], 0));
                }
                return CreditVal;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch_Console_Tbl"

        #region "Fetch Current Credit Note Value"

        /// <summary>
        /// Fetch_s the current_ credit_value.
        /// </summary>
        /// <param name="Pktran">The pktran.</param>
        /// <returns></returns>
        public int Fetch_Current_Credit_value(string Pktran)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string CreditVal = null;
            DataSet dsCredit = new DataSet();

            try
            {
                strQuery.Append("select cnt.crn_amt_in_crn_cur from  credit_note_trn_tbl cnt ");
                strQuery.Append("  where cnt.crn_trn_tbl_pk = " + Pktran);

                dsCredit = objWF.GetDataSet(strQuery.ToString());

                if (dsCredit.Tables[0].Rows.Count > 0)
                {
                    CreditVal = Convert.ToString(getDefault(dsCredit.Tables[0].Rows[0]["crn_amt_in_crn_cur"], 0));
                }
                return Convert.ToInt32(CreditVal);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Current Credit Note Value"

        #region "Fetch Customer For Document refrence Nr"

        /// <summary>
        /// Fetch_s the cust_ doc_ reference.
        /// </summary>
        /// <param name="process">The process.</param>
        /// <param name="biz">The biz.</param>
        /// <param name="DocNr">The document nr.</param>
        /// <param name="docType">Type of the document.</param>
        /// <returns></returns>
        public DataSet Fetch_Cust_Doc_Ref(Int32 process, Int32 biz, Int32 DocNr, Int32 docType)
        {
            // Public Function Fetch_Doc_Ref(ByVal process As Int32, ByVal biz As Int32, ByVal DocNr As Int32, ByVal docType As Int32) As DataSet
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                //export sea
                if (process == 1 & biz == 2)
                {
                    //hbl
                    if (docType == 2)
                    {
                        strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                        strQuery.Append("from customer_mst_tbl cmt, HBL_EXP_TBL HBE");
                        strQuery.Append("WHERE HBE.HBL_EXP_TBL_PK =" + DocNr);
                        strQuery.Append("and hbe.shipper_cust_mst_fk = cmt.customer_mst_pk");
                        strQuery.Append("");
                    }
                    else
                    {
                        strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                        strQuery.Append("from customer_mst_tbl cmt, JOB_CARD_TRN jcset");
                        strQuery.Append("WHERE  jcset.JOB_CARD_TRN_PK  =" + DocNr);
                        strQuery.Append("and jcset.shipper_cust_mst_fk = cmt.customer_mst_pk");
                        strQuery.Append("");
                    }

                    // export air
                }
                else if (process == 1 & biz == 1)
                {
                    //hawb
                    if (docType == 3)
                    {
                        strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                        strQuery.Append("from customer_mst_tbl cmt, hawb_exp_tbl HET ");
                        strQuery.Append("WHERE   HET.HAWB_EXP_TBL_PK  =" + DocNr);
                        strQuery.Append("and HET.shipper_cust_mst_fk = cmt.customer_mst_pk");
                        strQuery.Append("");
                    }
                    else
                    {
                        strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                        strQuery.Append("from customer_mst_tbl cmt, JOB_CARD_TRN jcaet ");
                        strQuery.Append("WHERE   jcaet.JOB_CARD_TRN_PK =" + DocNr);
                        strQuery.Append("and   jcaet.shipper_cust_mst_fk = cmt.customer_mst_pk");
                        strQuery.Append("");
                    }

                    // import sea
                }
                else if (process == 2 & biz == 2)
                {
                    strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                    strQuery.Append("from customer_mst_tbl cmt, JOB_CARD_TRN jcsit ");
                    strQuery.Append("WHERE  jcsit.JOB_CARD_TRN_PK =" + DocNr);
                    strQuery.Append("and    jcsit.consignee_cust_mst_fk = cmt.customer_mst_pk");
                    strQuery.Append("");

                    // import air
                }
                else if (process == 2 & biz == 1)
                {
                    strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                    strQuery.Append("from customer_mst_tbl cmt, JOB_CARD_TRN jcait ");
                    strQuery.Append("WHERE jcait.JOB_CARD_TRN_PK =" + DocNr);
                    strQuery.Append("and    jcait.consignee_cust_mst_fk = cmt.customer_mst_pk");
                    strQuery.Append("");
                }

                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Customer For Document refrence Nr"

        #region "Fetch Invoice PK" 'Added by rabbani ,To implement Barcode on 12/03/07

        /// <summary>
        /// Fetches the inv pk.
        /// </summary>
        /// <param name="Inv">The inv.</param>
        /// <returns></returns>
        public int FetchInvPK(string Inv)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT C.CONSOL_INVOICE_PK FROM CONSOL_INVOICE_TBL C" + "WHERE C.INVOICE_REF_NO= '" + Inv + "'";
            try
            {
                return Convert.ToInt32(objWK.ExecuteScaler(strSQL));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Invoice PK" 'Added by rabbani ,To implement Barcode on 12/03/07

        #region "Fetch Invoice Nr." 'Added by rabbani ,To implement Barcode on 13/03/07

        /// <summary>
        /// Fetches the count.
        /// </summary>
        /// <param name="Inv">The inv.</param>
        /// <returns></returns>
        public int FetchCount(string Inv)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT COUNT(*) FROM CONSOL_INVOICE_TBL C" + "WHERE C.INVOICE_REF_NO= '" + Inv + "'";
            try
            {
                return Convert.ToInt32(objWK.ExecuteScaler(strSQL));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Invoice Nr." 'Added by rabbani ,To implement Barcode on 13/03/07

        #region "Fetch Invoice Generated" 'Added by rabbani ,To implement Barcode on 13/03/07

        /// <summary>
        /// Fetches the inv gen.
        /// </summary>
        /// <param name="Inv">The inv.</param>
        /// <returns></returns>
        public int FetchInvGen(string Inv)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT COUNT(*) FROM CONSOL_INVOICE_TBL C" + "WHERE C.INVOICE_REF_NO= '" + Inv + "'" + "AND C.INVOICE_AMT = C.TOTAL_CREDIT_NOTE_AMT";
            try
            {
                return Convert.ToInt32(objWK.ExecuteScaler(strSQL));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Invoice Generated" 'Added by rabbani ,To implement Barcode on 13/03/07

        #region "Fetch Customer PK" 'Added by rabbani ,To implement Barcode on 14/03/07

        /// <summary>
        /// Fetches the cus pk.
        /// </summary>
        /// <param name="InvNr">The inv nr.</param>
        /// <returns></returns>
        public int FetchCusPK(string InvNr)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT C.CUSTOMER_MST_FK FROM  CONSOL_INVOICE_TBL C" + "WHERE C.INVOICE_REF_NO= '" + InvNr + "'";
            try
            {
                return Convert.ToInt32(objWK.ExecuteScaler(strSQL));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Customer PK" 'Added by rabbani ,To implement Barcode on 14/03/07

        #region " For Print Summary "

        /// <summary>
        /// Credis the t_ mai n_ print.
        /// </summary>
        /// <param name="Crn_PK">The CRN_ pk.</param>
        /// <param name="Biz_Type">Type of the biz_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <returns></returns>
        public DataSet CREDIT_MAIN_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with8 = objWK.MyCommand.Parameters;
                _with8.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
                _with8.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with8.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with8.Add("CREDIT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " For Print Summary "

        #region " For Print Summary Sub Report "

        /// <summary>
        /// Credis the t_ sub_ print.
        /// </summary>
        /// <param name="Crn_PK">The CRN_ pk.</param>
        /// <param name="Biz_Type">Type of the biz_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <returns></returns>
        public DataSet CREDIT_Sub_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with9 = objWK.MyCommand.Parameters;
                _with9.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
                _with9.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with9.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with9.Add("CREDIT_CUR2", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_SUB_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " For Print Summary Sub Report "

        #region " For Print  Details  Main Report "

        /// <summary>
        /// Credis the t_ main_ details_ print.
        /// </summary>
        /// <param name="Crn_PK">The CRN_ pk.</param>
        /// <param name="Biz_Type">Type of the biz_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <returns></returns>
        public DataSet CREDIT_Main_Details_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with10 = objWK.MyCommand.Parameters;
                _with10.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
                _with10.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with10.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with10.Add("CREDIT_CUR1", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_DETAILS_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " For Print  Details  Main Report "

        #region " For Print Details SUB  Report "

        /// <summary>
        /// Credis the t_ sub_ details_ print.
        /// </summary>
        /// <param name="Crn_PK">The CRN_ pk.</param>
        /// <param name="Biz_Type">Type of the biz_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <returns></returns>
        public DataSet CREDIT_Sub_Details_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with11 = objWK.MyCommand.Parameters;
                _with11.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
                _with11.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with11.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with11.Add("CREDIT_CUR4", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_DETAILS_SUB_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " For Print Details SUB  Report "

        #region "Print For Currency Details "

        /// <summary>
        /// Credis the t_ curr_ print.
        /// </summary>
        /// <param name="Crn_PK">The CRN_ pk.</param>
        /// <param name="Biz_Type">Type of the biz_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <returns></returns>
        public DataSet CREDIT_Curr_PRINT(string Crn_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with12 = objWK.MyCommand.Parameters;
                _with12.Add("CRNOTE_PK_IN", Crn_PK).Direction = ParameterDirection.Input;
                _with12.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with12.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with12.Add("CREDIT_CUR4", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CREDIT_NOTE_TBL_PKG", "CREDIT_DETAILS_Curr_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Print For Currency Details "

        #region "Fetch Location"

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <param name="Loc">The loc.</param>
        /// <returns></returns>
        public DataSet FetchLocation(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Location"

        #region "For Custumer Against Geeral Credit Note "

        /// <summary>
        /// Fetch_s the general_ custumer.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <returns></returns>
        public DataSet Fetch_General_Custumer(int CustPk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("  ");
                strQuery.Append("  SELECT CUST.CUSTOMER_NAME,");
                strQuery.Append("         CC.ADM_ADDRESS_1,");
                strQuery.Append("         CC.ADM_ADDRESS_2,");
                strQuery.Append("         CC.ADM_ADDRESS_3,");
                strQuery.Append("         CC.ADM_ZIP_CODE,");
                strQuery.Append("         CC.ADM_CITY,");
                strQuery.Append("         CCC.COUNTRY_NAME");
                strQuery.Append("    FROM CUSTOMER_MST_TBL      CUST,");
                strQuery.Append("         CUSTOMER_CONTACT_DTLS CC,");
                strQuery.Append("         COUNTRY_MST_TBL       CCC");
                strQuery.Append("   WHERE CC.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                strQuery.Append("     AND CC.ADM_COUNTRY_MST_FK = CCC.COUNTRY_MST_PK");
                strQuery.Append("     AND CUST.CUSTOMER_MST_PK =" + CustPk);
                strQuery.Append("");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "For Custumer Against Geeral Credit Note "

        #region "Fetch Barcode Manager Pk"

        /// <summary>
        /// Fetches the bar code manager pk.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcType">Type of the proc.</param>
        /// <returns></returns>
        public int FetchBarCodeManagerPk(int BizType, int ProcType)
        {
            try
            {
                string StrSql = null;
                DataSet DsBarManager = null;
                int strReturn = 0;
                StringBuilder strquery = new StringBuilder();

                WorkFlow objWF = new WorkFlow();
                //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"

                strquery.Append(" Select a.bcd_mst_pk from barcode_data_mst_tbl a");
                strquery.Append("                  where a.config_id_fk='QFOR4095'");
                strquery.Append("                 and a.BCD_MST_FK= (select b.bcd_mst_pk from barcode_data_mst_tbl b ");

                //Sea & Export
                if (BizType == 2 & ProcType == 1)
                {
                    strquery.Append("                   where b.field_name='Export Documentation' ");
                    strquery.Append("                     and b.BCD_MST_FK=2 ) ");
                    //Air Export
                }
                else if (BizType == 1 & ProcType == 1)
                {
                    strquery.Append("                   where b.field_name='Export Documentation' ");
                    strquery.Append("                     and b.BCD_MST_FK=1 ) ");
                    //Air import
                }
                else if (BizType == 1 & ProcType == 2)
                {
                    strquery.Append("                   where b.field_name='Import Documentation' ");
                    strquery.Append("                     and b.BCD_MST_FK=1 ) ");

                    //sea import
                }
                else if (BizType == 2 & ProcType == 2)
                {
                    strquery.Append("                   where b.field_name='Import Documentation' ");
                    strquery.Append("                     and b.BCD_MST_FK=2 ) ");
                }

                DsBarManager = objWF.GetDataSet(strquery.ToString());
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with13 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToInt32(_with13["bcd_mst_pk"]);
                }
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Barcode Manager Pk"

        #region "Fetch Barcode Type"

        /// <summary>
        /// Fetches the bar code field.
        /// </summary>
        /// <param name="BarCodeManagerPk">The bar code manager pk.</param>
        /// <returns></returns>
        public DataSet FetchBarCodeField(int BarCodeManagerPk)
        {
            try
            {
                string StrSql = null;
                DataSet DsBarManager = null;
                int strReturn = 0;
                WorkFlow objWF = new WorkFlow();
                StringBuilder strQuery = new StringBuilder();

                strQuery.Append("select distinct bdmt.bcd_mst_pk, bdmt.field_name, bdmt.default_value");
                strQuery.Append("  from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt");
                strQuery.Append(" where bdmt.bcd_mst_pk = bddt.bcd_mst_fk(+)");
                strQuery.Append("   and bdmt.BCD_MST_FK= " + BarCodeManagerPk);
                strQuery.Append(" ORDER BY default_value desc");

                // StrSql = "select bdmt.bcd_mst_pk, bdmt.field_name ,bdmt.default_value from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt where bdmt.bcd_mst_pk=bddt.bcd_mst_fk and bdmt.BCD_MST_FK=" & BarCodeManagerPk
                DsBarManager = objWF.GetDataSet(strQuery.ToString());
                return DsBarManager;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Barcode Type"

        #region "Fetch Credit Note List Recrods"

        //GetCNLCount functionality has been written below on 05/02/2010.Sreenivas
        /// <summary>
        /// Gets the CNL count.
        /// </summary>
        /// <param name="cnlRefNr">The CNL reference nr.</param>
        /// <param name="cnlPk">The CNL pk.</param>
        /// <param name="locPk">The loc pk.</param>
        /// <returns></returns>
        public int GetCNLCount(string cnlRefNr, int cnlPk, long locPk)
        {
            try
            {
                System.Text.StringBuilder strCNLQuery = new System.Text.StringBuilder(5000);
                strCNLQuery.Append(" select cnt.crn_tbl_pk, cnt.credit_note_ref_nr");
                strCNLQuery.Append(" from credit_note_tbl cnt, user_mst_tbl umt");
                strCNLQuery.Append(" where cnt.credit_note_ref_nr like '%" + cnlRefNr + "%'");
                strCNLQuery.Append(" and cnt.created_by_fk = umt.user_mst_pk");
                strCNLQuery.Append(" and umt.default_location_fk = " + locPk);
                WorkFlow objWF = new WorkFlow();
                DataSet objCNLDS = new DataSet();
                objCNLDS = objWF.GetDataSet(strCNLQuery.ToString());
                if (objCNLDS.Tables[0].Rows.Count == 1)
                {
                    cnlPk = Convert.ToInt32(objCNLDS.Tables[0].Rows[0][0]);
                }
                return objCNLDS.Tables[0].Rows.Count;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception handling added by gangadhar on 17/09/2011 PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Credit Note List Recrods"

        #region "UpdateCRNoteStatus"

        /// <summary>
        /// Updates the cr note status.
        /// </summary>
        /// <param name="CrNotePk">The cr note pk.</param>
        /// <param name="remarks">The remarks.</param>
        /// <returns></returns>
        public string UpdateCRNoteStatus(string CrNotePk, string remarks)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand updCmd = new OracleCommand();
            Int16 intIns = default(Int16);
            try
            {
                objWF.OpenConnection();
                var _with14 = updCmd;
                _with14.Connection = objWF.MyConnection;
                _with14.CommandType = CommandType.StoredProcedure;
                _with14.CommandText = objWF.MyUserName + ".PAYMENT_CRN_CANCELLATION_PKG.CANCEL_PAYMENT_CRN";
                var _with15 = _with14.Parameters;
                updCmd.Parameters.Add("PK_IN", CrNotePk).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("TYPE_FLAG_IN", "CRNOTE").Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                intIns = Convert.ToInt16(_with14.ExecuteNonQuery());
                return Convert.ToString(updCmd.Parameters["RETURN_VALUE"].Value);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "UpdateCRNoteStatus"
    }
}