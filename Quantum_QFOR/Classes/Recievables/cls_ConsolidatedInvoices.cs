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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsConsolidatedInvoices : CommonFeatures
    {

        #region "Fetch surcharge"
        public string Fetch_Surcharge_assign(DataSet dsGrid, string Int_pk_List = "", string MAIN_TABLE = "", string TRN_TABLE = "", string MAIN_TABLE_PK = "", string PK_OUT = "", string TRN_TABLE_PK = "", Int16 FreightCol = 0, int POL_PK = 0, int POD_PK = 0)
        {
            int Rcnt = 0;
            int Rcnt1 = 0;
            DataSet Dssurcharge = null;
            int Int_pk = 0;
            Array IntPKList = null;
            int Int_I = 0;
            try
            {

                if (dsGrid.Tables.Count == 2)
                {
                    dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                    if (Int_pk_List.Length > 0)
                    {
                        IntPKList = Int_pk_List.Split(',');
                        for (Int_I = 0; Int_I <= IntPKList.Length - 1; Int_I++)
                        {
                            Int_pk = Convert.ToInt32(IntPKList.GetValue(Int_I));
                            Dssurcharge = Fetch_surcharge_fortwotable(Int_pk, MAIN_TABLE, TRN_TABLE, MAIN_TABLE_PK, PK_OUT, TRN_TABLE_PK);
                        }
                    }

                    for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++)
                    {
                        for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                        {
                            if ((dsGrid.Tables[1].Rows[Rcnt][FreightCol] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]))
                            {
                                dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                            }
                        }
                    }


                }
                else if (dsGrid.Tables.Count == 1)
                {
                    dsGrid.Tables[0].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                    if (Int_pk_List.Length > 0)
                    {
                        IntPKList = Int_pk_List.Split(',');
                        for (Int_I = 0; Int_I <= IntPKList.Length - 1; Int_I++)
                        {
                            Int_pk = Convert.ToInt32(IntPKList.GetValue(Int_I));
                            Dssurcharge = Fetch_surcharge_fortwotable(Int_pk, MAIN_TABLE, TRN_TABLE, MAIN_TABLE_PK, PK_OUT, TRN_TABLE_PK, POL_PK, POD_PK);
                        }
                    }

                    for (Rcnt = 0; Rcnt <= dsGrid.Tables[0].Rows.Count - 1; Rcnt++)
                    {
                        for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                        {
                            if ((dsGrid.Tables[0].Rows[Rcnt][FreightCol] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]))
                            {
                                dsGrid.Tables[0].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                            }
                        }
                    }

                }

                return JsonConvert.SerializeObject(dsGrid, Formatting.Indented);
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
        #endregion

        #region "Fetch surcharge"
        public DataSet Fetch_surcharge_fortwotable(int Valuepk = 0, string MAIN_TABLE = "", string TRN_TABLE = "", string MAIN_TABLE_PK = "", string PK_OUT = "", string TRN_TABLE_PK = "", int POL_PK = 0, int POD_PK = 0)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".FETCH_SURCHARGE.FETCH_SURCHARGE_DATA";
                var _with1 = selectCommand.Parameters;
                _with1.Clear();
                _with1.Add("PK_IN", Valuepk).Direction = ParameterDirection.Input;
                _with1.Add("PK_OUT_IN", PK_OUT).Direction = ParameterDirection.Input;
                _with1.Add("MAIN_TABLE_IN", MAIN_TABLE).Direction = ParameterDirection.Input;
                _with1.Add("MAIN_TABLE_PK_IN", MAIN_TABLE_PK).Direction = ParameterDirection.Input;
                _with1.Add("TRN_TABLE_IN", TRN_TABLE).Direction = ParameterDirection.Input;
                _with1.Add("TRN_TABLE_PK_IN", TRN_TABLE_PK).Direction = ParameterDirection.Input;
                _with1.Add("POL_PK_IN", POL_PK).Direction = ParameterDirection.Input;
                _with1.Add("POD_PK_IN", POD_PK).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value).Trim();

                return (objWF.GetDataSet(strReturn));
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
        #endregion

        #region "Property"
        long lngInvPk;
        public string uniqueReferenceNr;
        public long ReturnSavePk
        {
            get { return lngInvPk; }
            set { lngInvPk = value; }
        }
        #endregion

        #region "Cargo-type"
        public int FetchCargoType(string JobCardNo)
        {

            WorkFlow Objwk = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("  SELECT DISTINCT BKG.CARGO_TYPE ");
            sb.Append("  FROM JOB_CARD_TRN JC,BOOKING_MST_TBL BKG");
            sb.Append("  WHERE JC.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
            sb.Append("  AND JC.JOBCARD_REF_NO='" + JobCardNo + "'");
            try
            {
                return Convert.ToInt32(Objwk.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }
        #endregion

        #region "FetchCredteDays"
        public DataSet GetDtVat(Int32 invpk)
        {
            StringBuilder strquery = new StringBuilder();
            WorkFlow objWf = new WorkFlow();
            try
            {
                strquery.Append(" select c.invoice_date,cust.vat_no from consol_invoice_tbl c , customer_mst_tbl cust where ");
                strquery.Append(" c.customer_mst_fk=cust.customer_mst_pk and  c.consol_invoice_pk=" + invpk);
                strquery.Append("  UNION ");
                strquery.Append("SELECT C.INVOICE_DATE, OPR.VAT_NO");
                strquery.Append("  FROM CONSOL_INVOICE_TBL C, OPERATOR_MST_TBL OPR");
                strquery.Append(" WHERE C.SUPPLIER_MST_FK = OPR.OPERATOR_MST_PK");
                strquery.Append("   AND C.BUSINESS_TYPE = 2");
                strquery.Append("   AND C.PROCESS_TYPE = 1");
                strquery.Append("   AND C.CONSOL_INVOICE_PK =" + invpk);
                strquery.Append(" UNION ");
                strquery.Append("  SELECT C.INVOICE_DATE, AMT.VAT_NO");
                strquery.Append("  FROM CONSOL_INVOICE_TBL C, AIRLINE_MST_TBL AMT");
                strquery.Append(" WHERE C.SUPPLIER_MST_FK = AMT.AIRLINE_MST_PK");
                strquery.Append("   AND C.BUSINESS_TYPE = 1");
                strquery.Append("   AND C.PROCESS_TYPE = 1");
                strquery.Append("   AND C.CONSOL_INVOICE_PK =" + invpk);
                return objWf.GetDataSet(strquery.ToString());
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
        public object getbookingpk(string INVPK = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("select distinct BKS.BOOKING_MST_PK BOOKING_SEA_PK");
            sb.Append("  from JOB_CARD_TRN   JOB,");
            sb.Append("      CONSOL_INVOICE_TBL     INV,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
            sb.Append("       BOOKING_MST_TBL        BKS");
            sb.Append("     WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
            sb.Append("   AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
            sb.Append("   AND JOB.BOOKING_MST_FK = BKS.BOOKING_MST_PK");
            sb.Append("   AND INV.CONSOL_INVOICE_PK IN (" + INVPK + ") ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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
        public string GetInvCrday(string invpk, string invdate, Int32 biztype, Int32 Process)
        {
            StringBuilder strquery = new StringBuilder();
            WorkFlow objWf = new WorkFlow();
            string strqry = null;
            DataSet dsinv = new DataSet();
            Int32 i = default(Int32);
            Int32 j = default(Int32);
            try
            {
                strquery.Append(" select distinct J.BOOKING_MST_FK BOOKING_SEA_FK,cust.customer_id,cust.credit_days from JOB_CARD_TRN j,");
                strquery.Append(" CUSTOMER_MST_TBL CUST where j.JOB_CARD_TRN_PK in ");
                strquery.Append(" (select c.job_card_fk  from consol_invoice_trn_tbl c where c.consol_invoice_fk IN (" + invpk + ")");

                strquery.Append(" group by c.job_card_fk) and j.shipper_cust_mst_fk=cust.customer_mst_pk ");
                dsinv = objWf.GetDataSet(strquery.ToString());
                if (dsinv.Tables[0].Rows.Count > 1)
                {
                    strquery = strquery.Remove(0, strquery.Length - 1);
                    strquery.Append("  select to_char(c1.invoice_date+" + (string.IsNullOrEmpty(dsinv.Tables[0].Rows[0]["credit_days"].ToString()) ? 0 : dsinv.Tables[0].Rows[0]["credit_days"]) + " ,'dd/mm/yyyy') from consol_invoice_tbl c1 where c1.consol_invoice_pk in(" + invpk + ")");

                    return objWf.ExecuteScaler(strquery.ToString());
                }

                string strSQL = null;

                strSQL = " select b.credit_days,to_CHAR(con.invoice_date,'dd/mm/yyyy')invdt  , (case when b.credit_days>0 then ";
                strSQL += " to_CHAR(con.invoice_date+b.credit_days,'dd/mm/yyyy') end )crdate ";
                strSQL += " from JOB_CARD_TRN j,BOOKING_MST_TBL b,consol_invoice_tbl con,consol_invoice_trn_tbl inv ";

                if (Process == 2)
                {
                    if (biztype == 2)
                    {
                        strSQL += " , JOB_CARD_TRN impj ";
                    }
                    else
                    {
                        strSQL += " , JOB_CARD_TRN impj ";
                    }
                }
                strSQL += " where inv.consol_invoice_fk in (" + invpk + ")";

                strSQL += "  and con.consol_invoice_pk=inv.consol_invoice_fk ";
                if (Process == 1)
                {
                    strSQL += " and inv.job_card_fk=j.JOB_CARD_TRN_PK and j.BOOKING_MST_FK=b.BOOKING_MST_PK";
                }
                else
                {
                    strSQL += " and inv.job_card_fk=impj.JOB_CARD_TRN_PK and j.BOOKING_MST_FK=b.BOOKING_MST_PK";
                }
                if (Process == 2)
                {
                    strSQL += " and impj.jobcard_ref_no=j.jobcard_ref_no";
                }
                if (biztype == 1)
                {
                    //strSQL = strSQL.Replace("sea", "air")
                }
                if (Process == 2)
                {
                    //strSQL = strSQL.Replace("exp", "imp")
                }
                dsinv = objWf.GetDataSet(strSQL);
                if (dsinv.Tables[0].Rows.Count > 0)
                {
                    return Convert.ToString(getDefault(dsinv.Tables[0].Rows[0][2], ""));
                }
                else
                {
                    if (Process == 2)
                    {
                        strqry = "  select distinct (case  when cust.credit_days > 0 then  to_CHAR(con.invoice_date +cust.credit_days,'dd/mm/yyyy')  ";
                        strqry += "    end) crdate   from  consol_invoice_tbl     con,consol_invoice_trn_tbl inv, ";
                        strqry += "  JOB_CARD_TRN   impj,   customer_mst_tbl cust  where inv.consol_invoice_fk in( " + invpk + ")";
                        strqry += "  and con.consol_invoice_pk = inv.consol_invoice_fk and inv.job_card_fk = impj.JOB_CARD_TRN_PK ";
                        strqry += "  and impj.consignee_cust_mst_fk=cust.customer_mst_pk ";
                        if (biztype == 1)
                        {
                            //strqry = strqry.Replace("sea", "air")
                        }
                        return objWf.ExecuteScaler(strqry);
                    }
                    else
                    {
                        return "";
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "";
            }
        }
        public string GetVatNo(string custname)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWf = new WorkFlow();
            try
            {
                strsql.Append(" select co.vat_no from customer_mst_tbl co where co.customer_name like '" + custname + "'");
                return objWf.ExecuteScaler(strsql.ToString());
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
        #endregion

        #region "Fetch Records"
        public DataSet FetchAllJCsForFreightPayers(bool blnFetch, string strCustomer, string JCdate, string strJobNo, string strCBJC, string strTPT, string strDem, string strBLNo, string strMBLNo, short BizType,short Process, short JobType, string VoyPK_OR_FlightNo, int I_Cargotype = 0, int Polpk = 0, int Podpk = 0, string Hbl = "", string Mbl = "", string fromDate = "", string ToDate = "",Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {

            StringBuilder strBuilder = new StringBuilder();
            WorkFlow objWf = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            StringBuilder strSelectionQuery = new StringBuilder();
            Int32 TotalRecords = default(Int32);
            DataSet DsCount = new DataSet();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (BizType == 1)
            {
                I_Cargotype = 0;
            }

            if (strCustomer.Length > 0)
            {
                strSelectionQuery.Append("AND CUST.CUSTOMER_ID = '" + strCustomer + "' " );
            }
            else
            {
            }

            if (JCdate.Trim().Length > 0 && Process == 1)
            {
                strSelectionQuery.Append(" and TO_DATE(TO_CHAR(JOB.JOBCARD_DATE,DATEFORMAT),DATEFORMAT) = TO_DATE('" + JCdate + "' ,'" + dateFormat + "')");
                //" &  & "'")
            }
            else if (JCdate.Trim().Length > 0 && Process == 2)
            {
                strSelectionQuery.Append(" and TO_DATE(TO_CHAR(JOB.JOBCARD_DATE,DATEFORMAT),DATEFORMAT) = TO_DATE('" + JCdate + "' ,'" + dateFormat + "')");
                //" &  & "'")
            }

            if (strJobNo.Length > 0)
            {
                strSelectionQuery.Append(" and job.jobcard_ref_no = '" + strJobNo + "'");
            }

            if (strBLNo.Length > 0)
            {
                if (Process == 1)
                {
                    strSelectionQuery.Append(" AND HBL.HBL_REF_NO = '" + strBLNo + "'");
                }
                else
                {
                    strSelectionQuery.Append(" AND JOB.HBL_HAWB_REF_NO = '" + strBLNo + "'");
                }
            }
            if (strMBLNo.Length > 0)
            {
                if (Process == 2)
                {
                    if (BizType == 2)
                    {
                        strSelectionQuery.Append(" AND JOB.MBL_MAWB_REF_NO = '" + strMBLNo + "'");
                    }
                    else
                    {
                        strSelectionQuery.Append(" AND JOB.HBL_HAWB_REF_NO = '" + strMBLNo + "'");
                    }

                }
            }
            if (VoyPK_OR_FlightNo.Length > 0)
            {
                if (BizType == 2)
                {
                    if (Process == 1)
                    {
                        strSelectionQuery.Append(" AND JOB.VOYAGE_TRN_FK = " + VoyPK_OR_FlightNo + "");
                    }
                    else
                    {
                        strSelectionQuery.Append(" AND JOB.VOYAGE_TRN_FK = '" + VoyPK_OR_FlightNo + "' ");
                    }

                }
                else
                {
                    strSelectionQuery.Append(" AND JOB.VOYAGE_FLIGHT_NO = '" + VoyPK_OR_FlightNo + "'");
                }
            }

            if (strDem.Length > 0)
            {
                strSelectionQuery.Append(" AND 1 > 2 ");
            }

            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(fromDate, ""))))
            {
                strSelectionQuery.Append(" AND TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                strSelectionQuery.Append(" AND TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            strQuery.Append(" SELECT JOBPK,JOBCARD,JOBDATE,JOB_TYPE,SHIPPER,CONSIGNEE,CUST," );
            strQuery.Append("POL,POD,CARGO_TYPE,SUM(FRAMT) FRAMT,CUST_PK,SEL,CUSTOMER_CATEGORY FROM ( SELECT * FROM " );
            strQuery.Append("(SELECT JOB.JOB_CARD_TRN_PK JOBPK," );
            strQuery.Append("MAX(JOB.JOBCARD_REF_NO) JOBCARD," );
            strQuery.Append("MAX(JOB.JOBCARD_DATE) JOBDATE," );
            strQuery.Append("MAX(CUST.CUSTOMER_ID) SHIPPER," );
            strQuery.Append("  NVL (MAX (CNSGN.CUSTOMER_ID),MAX( TEMPCON.CUSTOMER_ID)) CONSIGNEE," );
            // strQuery.Append("MAX(CNSGN.CUSTOMER_ID) CONSIGNEE," & vbCrLf)
            strQuery.Append("MAX(POL.PORT_ID) POL," );
            strQuery.Append("MAX(POD.PORT_ID) POD," );
            if (Process == 1)
            {
                if (BizType == 2)
                {
                    strQuery.Append("  DECODE(BKG.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE," );
                }
                else
                {
                    strQuery.Append("  '' CARGO_TYPE," );
                }
            }
            else
            {
                if (BizType == 2)
                {
                    strQuery.Append("  DECODE(JOB.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE," );
                }
                else
                {
                    strQuery.Append("  '' CARGO_TYPE," );
                }
            }
            strQuery.Append("SUM( NVL(JFD.FREIGHT_AMT,0) * JFD.EXCHANGE_RATE) FRAMT," );
            strQuery.Append("MAX(FRTPYR.CUSTOMER_NAME) CUST," );
            strQuery.Append("MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK, " );
            strQuery.Append(" '' SEL, 'JobCard' JOB_TYPE,0 CUSTOMER_CATEGORY " );

            strQuery.Append("FROM JOB_TRN_FD JFD, " );
            strQuery.Append("JOB_CARD_TRN JOB," );
            strQuery.Append("MBL_EXP_TBL MBL," );
            strQuery.Append(" TEMP_CUSTOMER_TBL TEMPCON," );
            if (Process == 1)
            {
                strQuery.Append("HBL_EXP_TBL HBL," );
                strQuery.Append("BOOKING_MST_TBL BKG," );
            }
            strQuery.Append("PORT_MST_TBL POL," );
            strQuery.Append("PORT_MST_TBL POD," );
            strQuery.Append("CUSTOMER_MST_TBL CUST," );
            strQuery.Append("CUSTOMER_MST_TBL CNSGN," );
            strQuery.Append("USER_MST_TBL UMT," );
            strQuery.Append("CUSTOMER_MST_TBL FRTPYR" );

            strQuery.Append("WHERE 1 = 1" );
            strQuery.Append("AND JFD.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK" );
            strQuery.Append("AND abs((NVL(JFD.FREIGHT_AMT,0) * JFD.EXCHANGE_RATE))>0" );
            strQuery.Append("AND JFD.FRTPAYER_CUST_MST_FK = FRTPYR.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK" );
            strQuery.Append("AND JOB.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK(+)" );
            strQuery.Append("AND JOB.CONSIGNEE_CUST_MST_FK = CNSGN.CUSTOMER_MST_PK(+)" );
            strQuery.Append(" AND JOB.CONSIGNEE_CUST_MST_FK=TEMPCON.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND UMT.USER_MST_PK = JOB.CREATED_BY_FK" );

            strQuery.Append("AND (jfd.consol_invoice_trn_fk is null or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=jfd.consol_invoice_trn_fk)=2) " );

            if (Process == 1)
            {
                strQuery.Append("AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)" );
                strQuery.Append("AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK" );
                strQuery.Append("AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK" );
                strQuery.Append("AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK" );
                strQuery.Append("AND POL.LOCATION_MST_FK = " + LoggedIn_Loc_FK );
                strQuery.Append("AND BKG.Status <> 3" );
                strQuery.Append("AND JOB.JOB_CARD_STATUS <> 2" );
                strQuery.Append("AND JFD.FREIGHT_TYPE = 1" );
            }
            else
            {
                strQuery.Append("AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK" );
                strQuery.Append("AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK" );
                strQuery.Append("AND POD.LOCATION_MST_FK = " + LoggedIn_Loc_FK );
                strQuery.Append("AND JOB.JOB_CARD_STATUS <> 2" );
                strQuery.Append("AND JFD.FREIGHT_TYPE = 2" );
            }
            if (Hbl != "0" & Process == 1)
            {
                strQuery.Append("AND HBL.HBL_EXP_TBL_PK = " + Hbl );
            }
            if (!string.IsNullOrEmpty(Mbl) & Process == 1)
            {
                strQuery.Append("AND MBL.MBL_EXP_TBL_PK = " + Mbl );
            }
            if (Polpk > 0)
            {
                strQuery.Append("AND POL.Port_Mst_Pk = " + Polpk );
            }
            if (Podpk > 0)
            {
                strQuery.Append("AND POD.Port_Mst_Pk = " + Podpk );
            }
            strQuery.Append(" AND JOB.BUSINESS_TYPE=" + BizType);
            strQuery.Append(" AND JOB.PROCESS_TYPE=" + Process);
            strQuery.Append(strSelectionQuery);
            strQuery.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,JFD.FRTPAYER_CUST_MST_FK " );
            if (Process == 1)
            {
                if (BizType == 2)
                {
                    strQuery.Append(" ,BKG.CARGO_TYPE " );
                }
            }
            else
            {
                if (BizType == 2)
                {
                    strQuery.Append(" ,JOB.CARGO_TYPE " );
                }
            }
            if (BizType == 2)
            {
            }
            strQuery.Append("" );
            strQuery.Append("UNION" );
            strQuery.Append("" );

            strQuery.Append("SELECT JOB.JOB_CARD_TRN_PK JOBPK," );
            strQuery.Append("MAX(JOB.JOBCARD_REF_NO) JOBCARD," );
            strQuery.Append("MAX(JOB.JOBCARD_DATE) JOBDATE," );
            strQuery.Append("MAX(CUST.CUSTOMER_ID) SHIPPER," );
            strQuery.Append(" NVL (MAX (CNSGN.CUSTOMER_ID),MAX( TEMPCON.CUSTOMER_ID)) CONSIGNEE," );
            // strQuery.Append("MAX(CNSGN.CUSTOMER_ID) CONSIGNEE," & vbCrLf)
            strQuery.Append("MAX(POL.PORT_ID) POL," );
            strQuery.Append("MAX(POD.PORT_ID) POD," );
            if (Process == 1)
            {
                if (BizType == 2)
                {
                    strQuery.Append(" DECODE(BKG.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE," );
                }
                else
                {
                    strQuery.Append("  '' CARGO_TYPE," );
                }
            }
            else
            {
                if (BizType == 2)
                {
                    strQuery.Append("  DECODE(JOB.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE," );
                }
                else
                {
                    strQuery.Append("  '' CARGO_TYPE," );
                }
            }
            strQuery.Append("SUM( NVL(JOTH.AMOUNT,0) * JOTH.EXCHANGE_RATE) FRAMT," );
            strQuery.Append("MAX(FRTPYR.CUSTOMER_NAME) CUST," );
            strQuery.Append("MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK, " );
            strQuery.Append(" '' SEL,'JobCard' JOB_TYPE, 0 CUSTOMER_CATEGORY " );


            strQuery.Append("FROM JOB_TRN_OTH_CHRG JOTH, " );
            strQuery.Append("JOB_CARD_TRN JOB," );
            strQuery.Append(" TEMP_CUSTOMER_TBL TEMPCON," );
            strQuery.Append("MBL_EXP_TBL MBL," );
            if (Process == 1)
            {
                strQuery.Append("HBL_EXP_TBL HBL," );
                strQuery.Append("BOOKING_MST_TBL BKG," );
            }
            strQuery.Append("PORT_MST_TBL POL," );
            strQuery.Append("PORT_MST_TBL POD," );
            strQuery.Append("CUSTOMER_MST_TBL CUST," );
            strQuery.Append("CUSTOMER_MST_TBL CNSGN," );
            strQuery.Append("USER_MST_TBL UMT," );
            strQuery.Append("CUSTOMER_MST_TBL FRTPYR" );

            strQuery.Append("WHERE 1 = 1" );
            strQuery.Append("AND JOTH.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK" );
            strQuery.Append("AND abs((NVL(JOTH.AMOUNT,0) * JOTH.EXCHANGE_RATE))>0" );
            strQuery.Append("AND JOTH.FRTPAYER_CUST_MST_FK = FRTPYR.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK" );
            strQuery.Append("AND JOB.CONSIGNEE_CUST_MST_FK=TEMPCON.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND JOB.CONSIGNEE_CUST_MST_FK = CNSGN.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND UMT.USER_MST_PK = JOB.CREATED_BY_FK" );
            strQuery.Append("AND JOB.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK(+)" );

            strQuery.Append("AND (joth.consol_invoice_trn_fk is null or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=joth.consol_invoice_trn_fk)=2) " );

            if (Process == 1)
            {
                strQuery.Append("AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)" );
                strQuery.Append("AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK" );
                strQuery.Append("AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK" );
                strQuery.Append("AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK" );
                strQuery.Append("AND POL.LOCATION_MST_FK = " + LoggedIn_Loc_FK );
                strQuery.Append("AND BKG.Status <> 3" );
                strQuery.Append("AND JOB.JOB_CARD_STATUS <> 2" );
                strQuery.Append("AND JOTH.FREIGHT_TYPE = 1" );
            }
            else
            {
                strQuery.Append("AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK" );
                strQuery.Append("AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK" );
                strQuery.Append("AND POD.LOCATION_MST_FK = " + LoggedIn_Loc_FK );
                strQuery.Append("AND JOB.JOB_CARD_STATUS <> 2" );
                strQuery.Append("AND JOTH.FREIGHT_TYPE = 2" );
            }
            if (Hbl != "0" & Process == 1)
            {
                strQuery.Append("AND HBL.HBL_EXP_TBL_PK = " + Hbl );
            }
            if (!string.IsNullOrEmpty(Mbl) & Process == 1)
            {
                strQuery.Append("AND MBL.MBL_EXP_TBL_PK = " + Mbl );
            }
            if (Polpk > 0)
            {
                strQuery.Append("AND POL.Port_Mst_Pk = " + Polpk );
            }
            if (Podpk > 0)
            {
                strQuery.Append("AND POD.Port_Mst_Pk = " + Podpk );
            }
            strQuery.Append(" AND JOB.BUSINESS_TYPE=" + BizType);
            strQuery.Append(" AND JOB.PROCESS_TYPE=" + Process);
            strQuery.Append(strSelectionQuery);
            strQuery.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,JOTH.FRTPAYER_CUST_MST_FK  " );
            if (Process == 1)
            {
                if (BizType == 2)
                {
                    strQuery.Append(" ,BKG.CARGO_TYPE) " );
                }
                else
                {
                    strQuery.Append(" ) " );
                }
            }
            else
            {
                if (BizType == 2)
                {
                    strQuery.Append(" ,JOB.CARGO_TYPE) " );
                }
                else
                {
                    strQuery.Append(" ) " );
                }
            }

            strQuery.Append(" UNION ");
            strQuery.Append("SELECT CT.CBJC_PK,");
            strQuery.Append("       MAX(CT.CBJC_NO) JOBCARD,");
            strQuery.Append("       MAX(CT.CBJC_DATE) JOBDATE,");
            strQuery.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
            strQuery.Append("     NVL (MAX (CONS.CUSTOMER_NAME),MAX(TEMPCON.CUSTOMER_NAME))  CONSIGNEE,");
            // strQuery.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,")
            strQuery.Append("       MAX(POL.PORT_ID) POL,");
            strQuery.Append("       MAX(POD.PORT_ID) POD,");
            strQuery.Append("       DECODE(CT.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            strQuery.Append("       SUM(NVL(CFD.FREIGHT_AMT, 0) * CFD.EXCHANGE_RATE) FRAMT,");
            strQuery.Append("       FRTPYR.CUSTOMER_NAME CUST,");
            strQuery.Append("       FRTPYR.CUSTOMER_MST_PK CUST_PK,");
            strQuery.Append("       '' SEL, 'Customs Brokerage' JOB_TYPE,");
            strQuery.Append("       (SELECT CASE WHEN NVL((SELECT C.CUSTOMER_CATEGORY_MST_PK");
            strQuery.Append("       FROM CUSTOMER_CATEGORY_MST_TBL C, CUSTOMER_CATEGORY_TRN CTRN");
            strQuery.Append("       WHERE C.CUSTOMER_CATEGORY_MST_PK = CTRN.CUSTOMER_CATEGORY_MST_FK");
            strQuery.Append("       AND CTRN.CUSTOMER_MST_FK = FRTPYR.CUSTOMER_MST_PK");
            strQuery.Append("       AND C.CUSTOMER_CATEGORY_ID='VENDOR'),0) > 0 THEN 1 ELSE 0 END FROM DUAL) CUSTOMER_CATEGORY ");

            strQuery.Append("  FROM CBJC_TBL         CT,");
            strQuery.Append("       PORT_MST_TBL     POL,");
            strQuery.Append(" TEMP_CUSTOMER_TBL TEMPCON,");
            strQuery.Append("       PORT_MST_TBL     POD,");
            strQuery.Append("       CUSTOMER_MST_TBL SHP,");
            strQuery.Append("       CUSTOMER_MST_TBL CONS,");
            strQuery.Append("       CBJC_TRN_FD      CFD,");
            strQuery.Append("       CBJC_TRN_CONT    CCONT,");
            strQuery.Append("       CUSTOMER_MST_TBL FRTPYR");
            strQuery.Append(" WHERE CT.POL_MST_FK = POL.PORT_MST_PK");
            strQuery.Append("   AND CT.POD_MST_FK = POD.PORT_MST_PK");
            strQuery.Append("   AND CT.SHIPPER_MST_FK = SHP.CUSTOMER_MST_PK(+)");
            strQuery.Append("   AND CT.CONSIGNEE_MST_FK = CONS.CUSTOMER_MST_PK(+)");
            strQuery.Append(" AND CT.CONSIGNEE_MST_FK=TEMPCON.CUSTOMER_MST_PK(+)");
            strQuery.Append("   AND CFD.CBJC_FK = CT.CBJC_PK");
            strQuery.Append("   AND CCONT.CBJC_FK(+) = CT.CBJC_PK");
            strQuery.Append("   AND CFD.FRTPAYER_CUST_MST_FK = FRTPYR.CUSTOMER_MST_PK");
            strQuery.Append("   AND ABS((NVL(CFD.FREIGHT_AMT, 0) * CFD.EXCHANGE_RATE)) > 0");
            if (Process == 1)
            {
                strQuery.Append("   AND POL.LOCATION_MST_FK =" + LoggedIn_Loc_FK);
                strQuery.Append("   AND CFD.FREIGHT_TYPE =1");
            }
            else
            {
                strQuery.Append("   AND POD.LOCATION_MST_FK =" + LoggedIn_Loc_FK);
                strQuery.Append("   AND CFD.FREIGHT_TYPE =2");
            }
            strQuery.Append("   AND CT.CBJC_STATUS <> 3");
            strQuery.Append("   AND CT.JOB_CARD_STATUS <> 2");
            strQuery.Append("   AND (CFD.CONSOL_INVOICE_TRN_FK IS NULL or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=CFD.CONSOL_INVOICE_TRN_FK)=2) ");
            strQuery.Append("  AND (CASE");
            strQuery.Append("  WHEN (SELECT CASE");
            strQuery.Append("  WHEN NVL((SELECT C.CUSTOMER_CATEGORY_MST_PK");
            strQuery.Append("   FROM CUSTOMER_CATEGORY_MST_TBL C,");
            strQuery.Append("  CUSTOMER_CATEGORY_TRN     CTRN");
            strQuery.Append("  WHERE C.CUSTOMER_CATEGORY_MST_PK =");
            strQuery.Append("  CTRN.CUSTOMER_CATEGORY_MST_FK");
            strQuery.Append("  AND CTRN.CUSTOMER_MST_FK =");
            strQuery.Append("  FRTPYR.CUSTOMER_MST_PK");
            strQuery.Append("  AND C.CUSTOMER_CATEGORY_ID = 'VENDOR'),");
            strQuery.Append("  0) > 0 THEN");
            strQuery.Append("  1");
            strQuery.Append("  ELSE");
            strQuery.Append("   0");
            strQuery.Append("  END");
            strQuery.Append("  FROM DUAL) = 1 THEN");
            strQuery.Append("   NVL(CCONT.CONTAINER_OWNER_TYPE_FK,0)");
            strQuery.Append("  ELSE");
            strQuery.Append("  1");
            strQuery.Append("  END) <> 2 ");
            strQuery.Append("   AND CT.BIZ_TYPE = " + BizType);
            strQuery.Append("   AND CT.PROCESS_TYPE =" + Process);
            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(fromDate, ""))))
            {
                strQuery.Append(" AND TO_DATE(CT.CBJC_DATE,DATEFORMAT) >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                strQuery.Append(" AND TO_DATE(CT.CBJC_DATE,DATEFORMAT) <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (Polpk > 0)
            {
                strQuery.Append("AND POL.Port_Mst_Pk = " + Polpk );
            }
            if (Podpk > 0)
            {
                strQuery.Append("AND POD.Port_Mst_Pk = " + Podpk );
            }
            if (strBLNo.Length > 0)
            {
                strQuery.Append(" AND CT.HBL_NO = '" + strBLNo + "'");
            }
            if (strDem.Length > 0)
            {
                strQuery.Append(" AND 1 > 2 ");
            }
            if (strMBLNo.Length > 0)
            {
                strQuery.Append(" AND CT.MBL_NO = '" + strMBLNo + "'");
            }
            if (strCBJC.Length > 0)
            {
                strQuery.Append(" AND CT.CBJC_NO = '" + strCBJC + "'");
            }
            if (strCustomer.Length > 0)
            {
                strQuery.Append("AND FRTPYR.CUSTOMER_ID = '" + strCustomer + "' " );
            }
            if (VoyPK_OR_FlightNo.Length > 0)
            {
                if (BizType == 2)
                {
                    strQuery.Append(" AND CT.VOYAGE_TRN_FK = '" + VoyPK_OR_FlightNo + "' ");
                }
                else
                {
                    strQuery.Append(" AND CT.FLIGHT_NO = '" + VoyPK_OR_FlightNo + "'");
                }
            }
            strQuery.Append(" GROUP BY CT.CBJC_PK, CT.CARGO_TYPE,FRTPYR.CUSTOMER_MST_PK,FRTPYR.CUSTOMER_NAME,CT.CUSTOMER_CATEGORY");

            strQuery.Append(" UNION ");

            strQuery.Append(" SELECT TIST.TRANSPORT_INST_SEA_PK,");
            strQuery.Append("       MAX(TIST.TRANS_INST_REF_NO) JOBCARD,");
            strQuery.Append("       MAX(TIST.TRANS_INST_DATE) JOBDATE,");
            strQuery.Append("       MAX(SHP.CUSTOMER_NAME) SHIPPER,");
            strQuery.Append("   NVL (MAX (CONS.CUSTOMER_NAME),MAX(TEMPCON.CUSTOMER_NAME))  CONSIGNEE,");
            //strQuery.Append("       MAX(CONS.CUSTOMER_NAME) CONSIGNEE,")
            strQuery.Append("       POL.PORT_ID POL,");
            strQuery.Append("       POD.PORT_ID POD,");
            strQuery.Append("       DECODE(TIST.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            strQuery.Append("       SUM(NVL(TFD.FREIGHT_AMT, 0) * TFD.EXCHANGE_RATE) FRAMT,");
            strQuery.Append("       FRTPYR.CUSTOMER_NAME CUST,");
            strQuery.Append("       FRTPYR.CUSTOMER_MST_PK CUST_PK,");
            strQuery.Append("       '' SEL, 'Transport Note' JOB_TYPE,");
            strQuery.Append("       (SELECT CASE WHEN NVL((SELECT C.CUSTOMER_CATEGORY_MST_PK");
            strQuery.Append("       FROM CUSTOMER_CATEGORY_MST_TBL C, CUSTOMER_CATEGORY_TRN CTRN");
            strQuery.Append("       WHERE C.CUSTOMER_CATEGORY_MST_PK = CTRN.CUSTOMER_CATEGORY_MST_FK");
            strQuery.Append("       AND CTRN.CUSTOMER_MST_FK = FRTPYR.CUSTOMER_MST_PK");
            strQuery.Append("       AND C.CUSTOMER_CATEGORY_ID='VENDOR'),0) > 0 THEN 1 ELSE 0 END FROM DUAL) CUSTOMER_CATEGORY ");

            strQuery.Append("  FROM TRANSPORT_INST_SEA_TBL TIST,");
            strQuery.Append("       CUSTOMER_MST_TBL       SHP,");
            strQuery.Append("       CUSTOMER_MST_TBL       CONS,");
            strQuery.Append("  TEMP_CUSTOMER_TBL TEMPCON,");
            strQuery.Append("       TRANSPORT_TRN_FD       TFD,");
            strQuery.Append("       TRANSPORT_TRN_CONT     TCONT,");
            strQuery.Append("       CUSTOMER_MST_TBL       FRTPYR,");
            strQuery.Append("       USER_MST_TBL           UMT,");
            strQuery.Append("       PORT_MST_TBL           POL,");
            strQuery.Append("       PORT_MST_TBL           POD");
            strQuery.Append(" WHERE TIST.TP_SHIPPER_MST_FK = SHP.CUSTOMER_MST_PK(+)");
            strQuery.Append("   AND TIST.TP_CONSIGNEE_MST_FK = CONS.CUSTOMER_MST_PK(+)");
            strQuery.Append("   AND TIST.TP_CONSIGNEE_MST_FK=TEMPCON.CUSTOMER_MST_PK(+)");
            strQuery.Append("   AND TFD.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK");
            strQuery.Append("   AND TCONT.TRANSPORT_INST_FK(+) = TIST.TRANSPORT_INST_SEA_PK");
            strQuery.Append("   AND TFD.FRTPAYER_CUST_MST_FK = FRTPYR.CUSTOMER_MST_PK");
            strQuery.Append("   AND ABS((NVL(TFD.FREIGHT_AMT, 0) * TFD.EXCHANGE_RATE)) > 0");
            strQuery.Append("   AND UMT.USER_MST_PK = TIST.CREATED_BY_FK");
            strQuery.Append("   AND UMT.DEFAULT_LOCATION_FK =" + LoggedIn_Loc_FK);
            strQuery.Append("   AND TIST.TP_STATUS <> 3");
            strQuery.Append("   AND TIST.TP_CLOSE_STATUS <>2");
            strQuery.Append("   AND (TFD.CONSOL_INVOICE_TRN_FK IS NULL or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=TFD.CONSOL_INVOICE_TRN_FK)=2)");
            strQuery.Append("   AND TIST.POL_FK = POL.PORT_MST_PK(+)");
            strQuery.Append("   AND TIST.POD_FK = POD.PORT_MST_PK(+)");

            strQuery.Append("  AND (CASE");
            strQuery.Append("  WHEN (SELECT CASE");
            strQuery.Append("  WHEN NVL((SELECT C.CUSTOMER_CATEGORY_MST_PK");
            strQuery.Append("   FROM CUSTOMER_CATEGORY_MST_TBL C,");
            strQuery.Append("  CUSTOMER_CATEGORY_TRN     CTRN");
            strQuery.Append("  WHERE C.CUSTOMER_CATEGORY_MST_PK =");
            strQuery.Append("  CTRN.CUSTOMER_CATEGORY_MST_FK");
            strQuery.Append("  AND CTRN.CUSTOMER_MST_FK =");
            strQuery.Append("  FRTPYR.CUSTOMER_MST_PK");
            strQuery.Append("  AND C.CUSTOMER_CATEGORY_ID = 'VENDOR'),");
            strQuery.Append("  0) > 0 THEN");
            strQuery.Append("  1");
            strQuery.Append("  ELSE");
            strQuery.Append("   0");
            strQuery.Append("  END");
            strQuery.Append("  FROM DUAL) = 1 THEN");
            strQuery.Append("   NVL(TCONT.CONTAINER_OWNER_TYPE_FK,0)");
            strQuery.Append("  ELSE");
            strQuery.Append("  1");
            strQuery.Append("  END) <> 2 ");
            strQuery.Append("   AND TIST.BUSINESS_TYPE =" + BizType);
            strQuery.Append("   AND TIST.PROCESS_TYPE =" + Process);
            if (Process == 1)
            {
                strQuery.Append("   AND TFD.FREIGHT_TYPE =1");
            }
            else
            {
                strQuery.Append("   AND TFD.FREIGHT_TYPE =2");
            }
            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(fromDate, ""))))
            {
                strQuery.Append(" AND TO_DATE(TIST.TRANS_INST_DATE,DATEFORMAT) >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                strQuery.Append(" AND TO_DATE(TIST.TRANS_INST_DATE,DATEFORMAT) <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (Polpk > 0)
            {
                strQuery.Append(" AND 1=2 ");
            }
            if (Podpk > 0)
            {
                strQuery.Append(" AND 1=2 ");
            }
            if (strBLNo.Length > 0)
            {
                strQuery.Append(" AND 1=2 ");
            }
            if (strDem.Length > 0)
            {
                strQuery.Append(" AND 1 > 2 ");
            }
            if (strMBLNo.Length > 0)
            {
                strQuery.Append(" AND UPPER((SELECT T.BL_NUMBER FROM TRANSPORT_TRN_CONT T WHERE T.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM < 2)) LIKE '" + strMBLNo + "'");
            }
            if (strTPT.Length > 0)
            {
                strQuery.Append(" AND TIST.TRANS_INST_REF_NO = '" + strTPT + "'");
            }
            if (strCustomer.Length > 0)
            {
                strQuery.Append("AND FRTPYR.CUSTOMER_ID = '" + strCustomer + "' " );
            }
            if (VoyPK_OR_FlightNo.Length > 0)
            {
                if (BizType == 2)
                {
                    strQuery.Append(" AND TIST.VSL_VOY_FK = '" + VoyPK_OR_FlightNo + "' ");
                }
                else
                {
                    strQuery.Append(" AND 1=2");
                }
            }
            strQuery.Append(" GROUP BY TIST.TRANSPORT_INST_SEA_PK, TIST.CARGO_TYPE,FRTPYR.CUSTOMER_MST_PK,");
            strQuery.Append(" FRTPYR.CUSTOMER_NAME,TIST.CUSTOMER_CATEGORY,POL.PORT_ID, POD.PORT_ID ");
            ///'''''
            strQuery.Append(" UNION SELECT DCH.DEM_CALC_HDR_PK,");
            strQuery.Append("       DCH.DEM_CALC_ID JOBCARD,");
            strQuery.Append("       DCH.DEM_CALC_DATE JOBDATE,");
            strQuery.Append("       SHP.CUSTOMER_NAME SHIPPER,");
            strQuery.Append("   NVL( CON.CUSTOMER_NAME,TEMPCON.CUSTOMER_NAME) CONSIGNEE,");
            //strQuery.Append("       CON.CUSTOMER_NAME CONSIGNEE,")
            strQuery.Append("       POL.PORT_ID POL,");
            strQuery.Append("       POD.PORT_ID POD,");
            strQuery.Append("       DECODE(TPN.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            strQuery.Append("       (SELECT SUM(((NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) +");
            strQuery.Append("                   (NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0))) *");
            strQuery.Append("                   GET_EX_RATE(" + HttpContext.Current.Session["currency_mst_pk"] + ",");
            strQuery.Append("                               DCH.CURRENCY_MST_FK,");
            strQuery.Append("                               SYSDATE))");
            strQuery.Append("          FROM DEM_CALC_DTL DCD");
            strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FRAMT,");
            strQuery.Append("  NVL(CON.CUSTOMER_NAME ,TEMPCON.CUSTOMER_NAME) AS CUST, ");
            strQuery.Append("      NVL(CON.CUSTOMER_MST_PK,TEMPCON.CUSTOMER_MST_PK) CUST_PK,");
            //strQuery.Append("       CON.CUSTOMER_NAME CUST,")
            //strQuery.Append("       CON.CUSTOMER_MST_PK CUST_PK,")
            strQuery.Append("       '' SEL,");
            strQuery.Append("       'Det.&Dem.' JOB_TYPE,");
            strQuery.Append("       (SELECT CASE");
            strQuery.Append("                 WHEN NVL((SELECT C.CUSTOMER_CATEGORY_MST_PK");
            strQuery.Append("                            FROM CUSTOMER_CATEGORY_MST_TBL C,");
            strQuery.Append("                                 CUSTOMER_CATEGORY_TRN     CTRN");
            strQuery.Append("                           WHERE C.CUSTOMER_CATEGORY_MST_PK =");
            strQuery.Append("                                 CTRN.CUSTOMER_CATEGORY_MST_FK");
            strQuery.Append("                             AND CTRN.CUSTOMER_MST_FK = CON.CUSTOMER_MST_PK");
            strQuery.Append("                             AND C.CUSTOMER_CATEGORY_ID = 'VENDOR'),");
            strQuery.Append("                          0) > 0 THEN");
            strQuery.Append("                  1");
            strQuery.Append("                 ELSE");
            strQuery.Append("                  0");
            strQuery.Append("               END");
            strQuery.Append("          FROM DUAL) CUSTOMER_CATEGORY");
            strQuery.Append("  FROM DEM_CALC_HDR           DCH,");
            strQuery.Append("       TRANSPORT_INST_SEA_TBL TPN,");
            strQuery.Append("       CUSTOMER_MST_TBL       SHP,");
            strQuery.Append("       CUSTOMER_MST_TBL       CON,");
            strQuery.Append("       PORT_MST_TBL           POL,");
            strQuery.Append("       PORT_MST_TBL           POD,");
            strQuery.Append("   TEMP_CUSTOMER_TBL TEMPCON ");
            strQuery.Append(" WHERE TPN.TRANSPORT_INST_SEA_PK = DCH.DOC_REF_FK");
            strQuery.Append("   AND DCH.DOC_TYPE = 0");
            strQuery.Append("   AND SHP.CUSTOMER_MST_PK(+) = TPN.TP_SHIPPER_MST_FK");
            strQuery.Append("   AND CON.CUSTOMER_MST_PK(+) = TPN.TP_CONSIGNEE_MST_FK");
            strQuery.Append("  AND TEMPCON.CUSTOMER_MST_PK(+)=TPN.TP_CONSIGNEE_MST_FK");
            strQuery.Append("   AND POL.PORT_MST_PK(+) = TPN.POL_FK");
            strQuery.Append("   AND POD.PORT_MST_PK(+) = TPN.POD_FK");

            strQuery.Append("   AND (((SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
            strQuery.Append("          FROM DEM_CALC_DTL DCD");
            strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) > 0 ");
            strQuery.Append("   AND (DCH.DET_INVOICE_TRN_FK IS NULL or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=DCH.DET_INVOICE_TRN_FK)=2)) ");
            strQuery.Append("   OR ((SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
            strQuery.Append("          FROM DEM_CALC_DTL DCD");
            strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) > 0 ");
            strQuery.Append("   AND (DCH.DEM_INVOICE_TRN_FK IS NULL or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=DCH.DET_INVOICE_TRN_FK)=2))) ");

            strQuery.Append("   AND DCH.BIZ_TYPE = " + BizType);
            strQuery.Append("   AND DCH.PROCESS_TYPE = " + Process);

            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(fromDate, ""))))
            {
                strQuery.Append(" AND TO_DATE(DCH.DEM_CALC_DATE, DATEFORMAT) >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                strQuery.Append(" AND TO_DATE(DCH.DEM_CALC_DATE, DATEFORMAT) <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (Polpk > 0)
            {
                strQuery.Append(" AND POL.PORT_MST_PK = " + Polpk);
            }
            if (Podpk > 0)
            {
                strQuery.Append(" AND POD.PORT_MST_PK = " + Podpk);
            }
            if (strBLNo.Length > 0)
            {
                strQuery.Append(" AND 1 = 2 ");
            }
            if (strMBLNo.Length > 0)
            {
                strQuery.Append(" AND 1 = 2 ");
            }
            if (strDem.Length > 0)
            {
                strQuery.Append(" AND DCH.DEM_CALC_ID = '" + strDem + "'");
            }
            if (strCustomer.Length > 0)
            {
                strQuery.Append("AND CON.CUSTOMER_ID = '" + strCustomer + "'");
            }
            if (VoyPK_OR_FlightNo.Length > 0)
            {
                if (BizType == 2)
                {
                    strQuery.Append(" AND TPN.VSL_VOY_FK = '" + VoyPK_OR_FlightNo + "' ");
                }
                else
                {
                    strQuery.Append(" AND TPN.OPERATOR_MST_FK = '" + VoyPK_OR_FlightNo + "' ");
                }
            }

            strQuery.Append(" UNION SELECT DCH.DEM_CALC_HDR_PK,");
            strQuery.Append("       DCH.DEM_CALC_ID JOBCARD,");
            strQuery.Append("       DCH.DEM_CALC_DATE JOBDATE,");
            strQuery.Append("       SHP.CUSTOMER_NAME SHIPPER,");
            strQuery.Append("  NVL( CON.CUSTOMER_NAME,TEMPCON.CUSTOMER_NAME) CONSIGNEE,");
            //strQuery.Append("       CON.CUSTOMER_NAME CONSIGNEE,")
            strQuery.Append("       POL.PORT_ID POL,");
            strQuery.Append("       POD.PORT_ID POD,");
            strQuery.Append("       DECODE(CT.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            strQuery.Append("       (SELECT SUM(((NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) +");
            strQuery.Append("                   (NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0))) *");
            strQuery.Append("                   GET_EX_RATE(" + HttpContext.Current.Session["currency_mst_pk"] + ",");
            strQuery.Append("                               DCH.CURRENCY_MST_FK,");
            strQuery.Append("                               SYSDATE))");
            strQuery.Append("          FROM DEM_CALC_DTL DCD");
            strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FRAMT,");
            strQuery.Append("  NVL( CON.CUSTOMER_NAME,TEMPCON.CUSTOMER_NAME) CUST,");
            strQuery.Append("           NVL( CON.CUSTOMER_MST_PK,TEMPCON.CUSTOMER_MST_PK) CUST_PK,");
            //strQuery.Append("       CON.CUSTOMER_NAME CUST,")
            //strQuery.Append("       CON.CUSTOMER_MST_PK CUST_PK,")
            strQuery.Append("       '' SEL,");
            strQuery.Append("       'Det.&Dem.' JOB_TYPE,");
            strQuery.Append("       (SELECT CASE");
            strQuery.Append("                 WHEN NVL((SELECT C.CUSTOMER_CATEGORY_MST_PK");
            strQuery.Append("                            FROM CUSTOMER_CATEGORY_MST_TBL C,");
            strQuery.Append("                                 CUSTOMER_CATEGORY_TRN     CTRN");
            strQuery.Append("                           WHERE C.CUSTOMER_CATEGORY_MST_PK =");
            strQuery.Append("                                 CTRN.CUSTOMER_CATEGORY_MST_FK");
            strQuery.Append("                             AND CTRN.CUSTOMER_MST_FK = CON.CUSTOMER_MST_PK");
            strQuery.Append("                             AND C.CUSTOMER_CATEGORY_ID = 'VENDOR'),");
            strQuery.Append("                          0) > 0 THEN");
            strQuery.Append("                  1");
            strQuery.Append("                 ELSE");
            strQuery.Append("                  0");
            strQuery.Append("               END");
            strQuery.Append("          FROM DUAL) CUSTOMER_CATEGORY");
            strQuery.Append("  FROM DEM_CALC_HDR     DCH,");
            strQuery.Append("       CBJC_TBL         CT,");
            strQuery.Append("       CUSTOMER_MST_TBL SHP,");
            strQuery.Append("       CUSTOMER_MST_TBL CON,");
            strQuery.Append("       PORT_MST_TBL     POL,");
            strQuery.Append("  TEMP_CUSTOMER_TBL TEMPCON,");
            strQuery.Append("       PORT_MST_TBL     POD");
            strQuery.Append(" WHERE CT.CBJC_PK = DCH.DOC_REF_FK");
            strQuery.Append("   AND DCH.DOC_TYPE = 1");
            strQuery.Append("   AND SHP.CUSTOMER_MST_PK(+) = CT.SHIPPER_MST_FK");
            strQuery.Append("   AND CON.CUSTOMER_MST_PK(+) = CT.CONSIGNEE_MST_FK");
            strQuery.Append("  AND TEMPCON.CUSTOMER_MST_PK(+)=CT.CONSIGNEE_MST_FK");
            strQuery.Append("   AND POL.PORT_MST_PK(+) = CT.POL_MST_FK");
            strQuery.Append("   AND POD.PORT_MST_PK(+) = CT.POD_MST_FK");

            strQuery.Append("   AND (((SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
            strQuery.Append("          FROM DEM_CALC_DTL DCD");
            strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) > 0 ");
            strQuery.Append("   AND (DCH.DET_INVOICE_TRN_FK IS NULL or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=DCH.DEM_INVOICE_TRN_FK)=2)) ");
            strQuery.Append("   OR ((SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
            strQuery.Append("          FROM DEM_CALC_DTL DCD");
            strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) > 0 ");
            strQuery.Append("   AND (DCH.DEM_INVOICE_TRN_FK IS NULL or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=DCH.DEM_INVOICE_TRN_FK)=2))) ");

            strQuery.Append("   AND DCH.BIZ_TYPE = " + BizType);
            strQuery.Append("   AND DCH.PROCESS_TYPE = " + Process);

            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(fromDate, ""))))
            {
                strQuery.Append(" AND TO_DATE(DCH.DEM_CALC_DATE, DATEFORMAT) >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                strQuery.Append(" AND TO_DATE(DCH.DEM_CALC_DATE, DATEFORMAT) <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (Polpk > 0)
            {
                strQuery.Append(" AND POL.PORT_MST_PK = " + Polpk);
            }
            if (Podpk > 0)
            {
                strQuery.Append(" AND POD.PORT_MST_PK = " + Podpk);
            }
            if (strBLNo.Length > 0)
            {
                strQuery.Append(" AND 1 = 2 ");
            }
            if (strMBLNo.Length > 0)
            {
                strQuery.Append(" AND 1 = 2 ");
            }
            if (strDem.Length > 0)
            {
                strQuery.Append(" AND DCH.DEM_CALC_ID = '" + strDem + "'");
            }
            if (strCustomer.Length > 0)
            {
                strQuery.Append("AND CON.CUSTOMER_ID = '" + strCustomer + "'");
            }
            if (VoyPK_OR_FlightNo.Length > 0)
            {
                if (BizType == 2)
                {
                    strQuery.Append(" AND CT.VOYAGE_TRN_FK = '" + VoyPK_OR_FlightNo + "' ");
                }
                else
                {
                    strQuery.Append(" AND CT.OPERATOR_MST_FK = '" + VoyPK_OR_FlightNo + "' ");
                }
            }

            strQuery.Append(" ) ");
            strQuery.Append(" WHERE 1=1 ");
            if (JobType != 0)
            {
                if (JobType == 1)
                {
                    strQuery.Append(" AND JOB_TYPE = 'JobCard'");
                }
                else if (JobType == 2)
                {
                    strQuery.Append(" AND JOB_TYPE = 'Customs Brokerage'");
                }
                else if (JobType == 3)
                {
                    strQuery.Append(" AND JOB_TYPE = 'Transport Note'");
                }
                else if (JobType == 4)
                {
                    strQuery.Append(" AND JOB_TYPE = 'Det.&Dem.'");
                }
            }
            if (!string.IsNullOrEmpty(strJobNo) & !string.IsNullOrEmpty(strCBJC))
            {
                strQuery.Append(" AND JOB_TYPE <> 'Transport Note'");
            }
            else if (!string.IsNullOrEmpty(strJobNo) & !string.IsNullOrEmpty(strTPT))
            {
                strQuery.Append(" AND JOB_TYPE <> 'Customs Brokerage'");
            }
            else if (!string.IsNullOrEmpty(strCBJC) & !string.IsNullOrEmpty(strTPT))
            {
                strQuery.Append(" AND JOB_TYPE <> 'JobCard'");
            }
            else if (!string.IsNullOrEmpty(strJobNo))
            {
                strQuery.Append(" AND JOB_TYPE = 'JobCard'");
            }
            else if (!string.IsNullOrEmpty(strCBJC))
            {
                strQuery.Append(" AND JOB_TYPE = 'Customs Brokerage'");
            }
            else if (!string.IsNullOrEmpty(strTPT))
            {
                strQuery.Append(" AND JOB_TYPE = 'Transport Note'");
            }
            else if (!string.IsNullOrEmpty(strDem))
            {
                strQuery.Append(" AND JOB_TYPE = 'Det.&Dem.'");
            }
            strQuery.Append(" GROUP BY JOBPK,JOBCARD,JOBDATE,SHIPPER,CONSIGNEE,POL,POD,SEL,CUST,CUST_PK  " );
            strQuery.Append(" ,CARGO_TYPE,JOB_TYPE,CUSTOMER_CATEGORY ORDER BY JOBDATE DESC,JOBCARD DESC" );

            StringBuilder strCount = new StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + strQuery.ToString() + ")"));
            DsCount = (DataSet)objWf.GetDataSet(strCount.ToString());

            if (DsCount.Tables[0].Rows.Count > 0)
            {
                TotalRecords = Convert.ToInt32(DsCount.Tables[0].Rows[0][0]);
            }
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            StringBuilder sqlstr = new StringBuilder();
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + strQuery.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            try
            {
                return objWf.GetDataSet(sqlstr.ToString());
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
        public DataSet FetchCurrId(int invpk)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWf = new WorkFlow();
            try
            {
                strsql.Append(" select curr.currency_id CURRID ,curr.currency_mst_pk CURRPK  from consol_invoice_tbl con , currency_type_mst_tbl curr ");
                strsql.Append(" where con.currency_mst_fk=curr.currency_mst_pk");
                strsql.Append(" and con.consol_invoice_pk=" + invpk);
                return objWf.GetDataSet(strsql.ToString());
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
        public DataSet FetchAll(bool blnFetch, string strCustomer, string JCdate, string strJobNo, string strBLNo, short BizType, short Process)
        {

            StringBuilder strBuilder = new StringBuilder();
            WorkFlow objWf = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("SELECT JOB.JOB_CARD_TRN_PK JOBPK," );
            strQuery.Append("       JOB.JOBCARD_REF_NO JOBCARD," );
            strQuery.Append("       JOB.JOBCARD_DATE JOBDATE," );
            strQuery.Append("       CUST.CUSTOMER_ID SHIPPER," );
            strQuery.Append("       CNSGN.CUSTOMER_ID CONSIGNEE," );
            strQuery.Append("       POL.PORT_ID POL," );
            strQuery.Append("       POD.PORT_ID POD," );
            strQuery.Append("       NVL((NVL((SELECT SUM(NVL(JOBFD.FREIGHT_AMT, 0)*JOBFD.EXCHANGE_RATE)" );
            strQuery.Append("           FROM JOB_TRN_FD JOBFD" );
            strQuery.Append("          WHERE JOBFD.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK" );

            if (Process == 1)
            {
                strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 1" );
            }
            else
            {
                strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 2" );
            }
            strQuery.Append("            AND (JOBFD.INVOICE_TBL_FK IS NULL AND" );
            strQuery.Append("                JOBFD.CONSOL_INVOICE_TRN_FK IS NULL AND" );
            strQuery.Append("                JOBFD.INV_AGENT_TRN_FK IS NULL)),0) +" );
            strQuery.Append("       NVL((SELECT SUM(NVL(JOBOTH.AMOUNT, 0)*JOBOTH.exchange_rate)" );
            strQuery.Append("              FROM JOB_TRN_OTH_CHRG JOBOTH" );
            strQuery.Append("             WHERE JOBOTH.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK" );
            strQuery.Append("               AND (JOBOTH.INV_CUST_TRN_FK IS NULL AND" );
            strQuery.Append("                   JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL AND" );
            strQuery.Append("                   JOBOTH.INV_AGENT_TRN_FK IS NULL))," );
            strQuery.Append("            0)),0) FRAMT," );
            strQuery.Append("       0 SEL," );
            if (Process == 1)
            {
                strQuery.Append("       CUST.CUSTOMER_NAME CUST," );
                strQuery.Append("       CUST.CUSTOMER_MST_PK CUST_PK" );
            }
            else
            {
                strQuery.Append("       CNSGN.CUSTOMER_NAME CUST," );
                strQuery.Append("       CNSGN.CUSTOMER_MST_PK CUST_PK" );

            }
            strQuery.Append("  FROM JOB_CARD_TRN JOB," );
            if (Process == 1)
            {
                strQuery.Append("       BOOKING_MST_TBL      BKG," );
            }
            strQuery.Append("       PORT_MST_TBL         POL," );
            strQuery.Append("       PORT_MST_TBL         POD," );
            strQuery.Append("       CUSTOMER_MST_TBL     CUST," );
            strQuery.Append("       CUSTOMER_MST_TBL     CNSGN," );
            strQuery.Append("       USER_MST_TBL        UMT" );
            strQuery.Append(" WHERE 1 = 1" );
            if (Process == 1)
            {
                strQuery.Append("   AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK" );
            }
            strQuery.Append("   AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK" );
            strQuery.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK" );
            strQuery.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK" );
            strQuery.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CNSGN.CUSTOMER_MST_PK" );
            strQuery.Append("  AND NVL((NVL((SELECT SUM(NVL(JOBFD.FREIGHT_AMT, 0)*JOBFD.exchange_rate)" );
            strQuery.Append("           FROM JOB_TRN_FD JOBFD" );
            strQuery.Append("          WHERE JOBFD.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK" );
            if (Process == 1)
            {
                strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 1" );
            }
            else
            {
                strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 2" );
            }
            strQuery.Append("            AND (JOBFD.INVOICE_TBL_FK IS NULL AND" );
            strQuery.Append("                JOBFD.CONSOL_INVOICE_TRN_FK IS NULL AND" );
            strQuery.Append("                JOBFD.INV_AGENT_TRN_FK IS NULL)),0) +" );
            strQuery.Append("       NVL((SELECT SUM(NVL(JOBOTH.AMOUNT, 0)*JOBOTH.exchange_rate)" );
            strQuery.Append("              FROM JOB_TRN_OTH_CHRG JOBOTH" );
            strQuery.Append("             WHERE JOBOTH.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK" );
            strQuery.Append("               AND (JOBOTH.INV_CUST_TRN_FK IS NULL AND" );
            strQuery.Append("                   JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL AND" );
            strQuery.Append("                   JOBOTH.INV_AGENT_TRN_FK IS NULL))," );
            strQuery.Append("            0)),0) >0" );
            strQuery.Append(" AND UMT.USER_MST_PK=JOB.CREATED_BY_FK " );
            if (Process == 1)
            {
                strQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK );
            }
            if (BizType == 1 & Process == 1)
            {
                //strQuery.Replace("SEA", "AIR")
                //strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_EXP_FK")
            }
            else if (BizType == 1 & Process == 2)
            {
                //strQuery.Replace("SEA", "AIR")
                //strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_IMP_FK")
                //strQuery.Replace("EXP", "IMP")
                //strQuery.Replace("BKG", "JOB")
            }
            else if (BizType == 2 & Process == 2)
            {
                //strQuery.Replace("EXP", "IMP")
                //strQuery.Replace("BKG", "JOB")
            }

            if (strCustomer.Length > 0 && Process == 1)
            {
                strQuery.Append(" and cust.customer_id = '" + strCustomer + "' ");
            }
            else if (strCustomer.Length > 0)
            {
                strQuery.Append(" and CNSGN.customer_id = '" + strCustomer + "' ");
            }

            if (JCdate.Trim().Length > 0 && Process == 1)
            {
                strQuery.Append(" and TO_DATE(TO_CHAR(JOB.JOBCARD_DATE,DATEFORMAT),DATEFORMAT) = TO_DATE('" + JCdate + "' ,'" + dateFormat + "')");
                //" &  & "'")
            }

            if (strJobNo.Length > 0)
            {
                strQuery.Append(" and job.jobcard_ref_no = '" + strJobNo + "'");
            }

            if (strBLNo.Length > 0)
            {
                strQuery.Append(" ");
            }

            if (blnFetch == false)
            {
                strQuery.Append(" and   1=2 ");
            }
            strQuery.Append("   ORDER BY JOBPK DESC" );
            try
            {
                return objWf.GetDataSet(strQuery.ToString());
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

        #region "Find CargoType"
        public int FetchCargoType(string strJobpk, string strCBJCpk, string strTPTpk, string strDemPKList, int Biztype, int process)
        {
            WorkFlow objwf = new WorkFlow();
            int cargotype = 0;
            string StrSql = null;
            StrSql = string.Empty;
            DataSet Ds = new DataSet();
            try
            {
                if (string.IsNullOrEmpty(strJobpk) & !string.IsNullOrEmpty(strCBJCpk))
                {
                    strJobpk = strCBJCpk;
                }
                else if (string.IsNullOrEmpty(strJobpk) & !string.IsNullOrEmpty(strTPTpk))
                {
                    strJobpk = strTPTpk;
                }
                if (!string.IsNullOrEmpty(strJobpk))
                {
                    StrSql += " select NVL(J.cargo_type,0) from JOB_CARD_TRN j " ;
                    StrSql += " where j.JOB_CARD_TRN_PK in ( " + strJobpk + " ) " ;
                }
                else if (!string.IsNullOrEmpty(strDemPKList))
                {
                    StrSql += " SELECT DCH.CARGO_TYPE FROM DEM_CALC_HDR DCH ";
                    StrSql += " WHERE DCH.DEM_CALC_HDR_PK IN (" + strDemPKList + ")";
                }
                Ds = objwf.GetDataSet(StrSql);
                if (Ds.Tables[0].Rows.Count > 0)
                {
                    return Convert.ToInt32(Ds.Tables[0].Rows[0][0]);
                }
            }
            catch (OracleException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw;
            }
            return 0;
        }
        #endregion

        #region "FetchConsolidatable"
        public DataSet FetchConsolidatable(short BizType, short Process, string CustPk, bool Edit = false, int ExType = 1, string strJobPks = "", string strCBJCPks = "", string strTPTPks = "", string strDemPKList = "")
        {
            StringBuilder strBuilder = new StringBuilder();
            WorkFlow objWk = new WorkFlow();
            string frtType = Convert.ToString((Process == 1 ? "1" : "2"));
            if (string.IsNullOrEmpty(strJobPks))
            {
                strJobPks = "0";
            }
            if (string.IsNullOrEmpty(strCBJCPks))
            {
                strCBJCPks = "0";
            }
            if (string.IsNullOrEmpty(strTPTPks))
            {
                strTPTPks = "0";
            }
            if (string.IsNullOrEmpty(strDemPKList))
            {
                strDemPKList = "0";
            }
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT DISTINCT CFD.FREIGHT_ELEMENT_MST_FK,");
            sb.Append("                CT.CBJC_NO JOBCARD_REF_NO,");
            sb.Append("                CT.CBJC_DATE JOBCARD_DATE,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN CFD.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
            sb.Append("                   CON.CONTAINER_TYPE_MST_ID");
            sb.Append("                  ELSE");
            sb.Append("                   DECODE(CFD.BASIS_FK,");
            sb.Append("                          1,");
            sb.Append("                          'Unit',");
            sb.Append("                          2,");
            sb.Append("                          'Flat Rate',");
            sb.Append("                          3,");
            sb.Append("                          'Kgs',");
            sb.Append("                          4,");
            sb.Append("                          'MT',");
            sb.Append("                          5,");
            sb.Append("                          'CBM')");
            sb.Append("                END) UNIT,");
            sb.Append("                CFD.CBJC_TRN_FD_PK JOB_TRN_SEA_EXP_FD_PK,");
            sb.Append("                CFD.CBJC_FK JOBFK,");
            sb.Append("                CFD.CURRENCY_MST_FK,");
            sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("                DECODE(CFD.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') AS PC,");
            sb.Append("                CUMT.CURRENCY_ID,");
            sb.Append("                CFD.FREIGHT_AMT,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN (CFD.CONSOL_INVOICE_TRN_FK IS NULL) THEN");
            sb.Append("                   NULL");
            sb.Append("                  WHEN CFD.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN");
            sb.Append("                   (SELECT SUM(DISTINCT");
            sb.Append("                               ROUND((TRN.TOT_AMT / TRN.EXCHANGE_RATE), 2))");
            sb.Append("                      FROM CONSOL_INVOICE_TRN_TBL TRN , CONSOL_INVOICE_TBL CIT ");
            sb.Append("                     WHERE TRN.FRT_OTH_ELEMENT_FK =");
            sb.Append("                           CFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("                       AND TRN.FRT_OTH_ELEMENT = 1 AND CIT.CONSOL_INVOICE_PK=TRN.CONSOL_INVOICE_FK AND CIT.CHK_INVOICE<>2 ");
            sb.Append("                       AND TRN.CONSOL_INVOICE_TRN_PK =");
            sb.Append("                           CFD.CONSOL_INVOICE_TRN_FK");
            sb.Append("                       AND TRN.JOB_CARD_FK = CT.CBJC_PK)");
            sb.Append("                ");
            sb.Append("                END) INV_AMT,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN (CFD.CONSOL_INVOICE_TRN_FK IS NULL) THEN");
            sb.Append("                   'False'");
            sb.Append("                  ELSE");
            sb.Append("                   'True'");
            sb.Append("                END) CHK,");
            sb.Append("                PAR.FRT_BOF_FK,");
            sb.Append("                FMT.PREFERENCE");
            sb.Append("  FROM CBJC_TBL                CT,");
            sb.Append("       CBJC_TRN_FD             CFD,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CON,");
            sb.Append("       PARAMETERS_TBL          PAR");
            sb.Append(" WHERE CFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CFD.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb.Append("   AND CT.CBJC_PK = CFD.CBJC_FK");
            sb.Append("   AND (CFD.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
            sb.Append("       CFD.CONTAINER_TYPE_MST_FK IS NULL)");
            sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
            sb.Append("   AND CFD.FREIGHT_TYPE = " + frtType);
            sb.Append("   AND CFD.CBJC_FK IN (" + strCBJCPks + ")");
            sb.Append("   AND CFD.FRTPAYER_CUST_MST_FK IN (" + CustPk + ")");

            StringBuilder sb1 = new StringBuilder(5000);
            sb1.Append("SELECT DISTINCT TFD.FREIGHT_ELEMENT_MST_FK,");
            sb1.Append("                TIST.TRANS_INST_REF_NO JOBCARD_REF_NO,");
            sb1.Append("                TIST.TRANS_INST_DATE JOBCARD_DATE,");
            sb1.Append("                (CASE");
            sb1.Append("                  WHEN TFD.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
            sb1.Append("                   CON.CONTAINER_TYPE_MST_ID");
            sb1.Append("                  ELSE");
            sb1.Append("                   DECODE(TFD.BASIS_FK,");
            sb1.Append("                          1,");
            sb1.Append("                          'Unit',");
            sb1.Append("                          2,");
            sb1.Append("                          'Flat Rate',");
            sb1.Append("                          3,");
            sb1.Append("                          'Kgs',");
            sb1.Append("                          4,");
            sb1.Append("                          'MT',");
            sb1.Append("                          5,");
            sb1.Append("                          'CBM')");
            sb1.Append("                END) UNIT,");
            sb1.Append("                TFD.TRANSPORT_TRN_FD_PK JOB_TRN_SEA_EXP_FD_PK,");
            sb1.Append("                TFD.TRANSPORT_INST_FK JOBFK,");
            sb1.Append("                TFD.CURRENCY_MST_FK,");
            sb1.Append("                FMT.FREIGHT_ELEMENT_NAME,");
            sb1.Append("                DECODE(TFD.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') AS PC,");
            sb1.Append("                CUMT.CURRENCY_ID,");
            sb1.Append("                TFD.FREIGHT_AMT,");
            sb1.Append("                (CASE");
            sb1.Append("                  WHEN (TFD.CONSOL_INVOICE_TRN_FK IS NULL) THEN");
            sb1.Append("                   NULL");
            sb1.Append("                  WHEN TFD.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN");
            sb1.Append("                   (SELECT SUM(DISTINCT");
            sb1.Append("                               ROUND((TRN.TOT_AMT / TRN.EXCHANGE_RATE), 2))");
            sb1.Append("                      FROM CONSOL_INVOICE_TRN_TBL TRN , CONSOL_INVOICE_TBL CIT ");
            sb1.Append("                     WHERE TRN.FRT_OTH_ELEMENT_FK =");
            sb1.Append("                           TFD.FREIGHT_ELEMENT_MST_FK");
            sb1.Append("                       AND TRN.FRT_OTH_ELEMENT = 1 AND CIT.CONSOL_INVOICE_PK=TRN.CONSOL_INVOICE_FK AND CIT.CHK_INVOICE<>2 ");
            sb1.Append("                       AND TRN.CONSOL_INVOICE_TRN_PK =");
            sb1.Append("                           TFD.CONSOL_INVOICE_TRN_FK");
            sb1.Append("                       AND TRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK)");
            sb1.Append("                ");
            sb1.Append("                END) INV_AMT,");
            sb1.Append("                (CASE");
            sb1.Append("                  WHEN (TFD.CONSOL_INVOICE_TRN_FK IS NULL) THEN");
            sb1.Append("                   'False'");
            sb1.Append("                  ELSE");
            sb1.Append("                   'True'");
            sb1.Append("                END) CHK,");
            sb1.Append("                PAR.FRT_BOF_FK,");
            sb1.Append("                FMT.PREFERENCE");
            sb1.Append("  FROM TRANSPORT_INST_SEA_TBL TIST,");
            sb1.Append("       TRANSPORT_TRN_FD       TFD,");
            sb1.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
            sb1.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
            sb1.Append("       CONTAINER_TYPE_MST_TBL  CON,");
            sb1.Append("       PARAMETERS_TBL          PAR");
            sb1.Append(" WHERE TFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
            sb1.Append("   AND TFD.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb1.Append("   AND TIST.TRANSPORT_INST_SEA_PK = TFD.TRANSPORT_INST_FK");
            sb1.Append("   AND (TFD.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
            sb1.Append("       TFD.CONTAINER_TYPE_MST_FK IS NULL)");
            sb1.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
            sb1.Append("   AND TFD.FREIGHT_TYPE = " + frtType);
            sb1.Append("   AND TFD.TRANSPORT_INST_FK IN (" + strTPTPks + ")");
            sb1.Append("   AND TFD.FRTPAYER_CUST_MST_FK IN (" + CustPk + ")");
            sb1.Append(" ORDER BY UNIT, PREFERENCE");

            //If BizType = 2 And Process = 1 Then
            strBuilder.Append(" SELECT JOBCARD_REF_NO, JOBCARD_DATE, UNIT, JOB_TRN_FD_PK, JOBFK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC, CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK, FRT_BOF_FK FROM ( ");
            strBuilder.Append(" select JOBCARD_REF_NO ,JOBCARD_DATE,unit,JOB_TRN_FD_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,");
            strBuilder.Append(" CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC,CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK,FRT_BOF_FK, PREFERENCE from ");
            strBuilder.Append(" ( SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK, ");
            strBuilder.Append(" JOB.JOBCARD_REF_NO,");
            strBuilder.Append(" JOB.JOBCARD_DATE,");
            if (BizType == 2)
            {
                strBuilder.Append(" (case when jobfrt.container_type_mst_fk is not null then ");
                strBuilder.Append(" con.container_type_mst_id Else ");
                strBuilder.Append(" DECODE(JOBFRT.BASIS_FK,1,'Unit',2,'Flat Rate',3,'Kgs',4,'MT',5,'CBM') END) UNIT,");
            }
            else
            {
                strBuilder.Append(" (CASE WHEN NVL(JOBFRT.SERVICE_TYPE_FLAG,0)=1 THEN ");
                strBuilder.Append("      DECODE(JOBFRT.BASIS_FK,1,'Unit',2,'Flat Rate',3,'Kgs',4,'MT',5,'CBM') ELSE ");
                strBuilder.Append("    UPPER(JOBFRT.QUANTITY) END) UNIT,");
            }
            strBuilder.Append(" JOBFRT.JOB_TRN_FD_PK,");
            strBuilder.Append(" JOBFRT.JOB_CARD_TRN_FK JOBFK,");
            strBuilder.Append(" JOBFRT.CURRENCY_MST_FK,");
            strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
            strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'Prepaid',2,'Collect') AS PC,");
            strBuilder.Append(" CUMT.CURRENCY_ID,");
            strBuilder.Append(" JOBFRT.FREIGHT_AMT,");

            strBuilder.Append(" (CASE ");

            strBuilder.Append(" WHEN (JOBFRT.INVOICE_TBL_FK IS NULL ");
            strBuilder.Append("   AND JOBFRT.INV_AGENT_TRN_FK IS NULL");
            strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ");
            strBuilder.Append(" ) THEN NULL ");

            strBuilder.Append(" WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
            strBuilder.Append("   (SELECT SUM(DISTINCT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN , CONSOL_INVOICE_TBL CIT  ");
            strBuilder.Append("   WHERE TRN.FRT_OTH_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
            strBuilder.Append("   AND TRN.FRT_OTH_ELEMENT=1 AND CIT.CONSOL_INVOICE_PK=TRN.CONSOL_INVOICE_FK AND CIT.CHK_INVOICE<>2  ");
            strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBFRT.CONSOL_INVOICE_TRN_FK ");
            strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)");

            strBuilder.Append(" WHEN JOBFRT.INVOICE_TBL_FK IS NOT NULL THEN ");
            strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_SEA_EXP_TBL TRN ");
            strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
            strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=2  ");
            strBuilder.Append("   AND TRN.INV_CUST_TRN_SEA_EXP_PK=JOBFRT.INVOICE_TBL_FK) ");

            strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN , INV_AGENT_TBL IAT ");
            strBuilder.Append("    WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
            strBuilder.Append("    AND TRN.COST_FRT_ELEMENT=2 AND IAT.INV_AGENT_PK=TRN.INV_AGENT_FK AND IAT.CHK_INVOICE<>2 ");
            strBuilder.Append("    AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_FK) END) INV_AMT,");

            strBuilder.Append(" (CASE WHEN (JOBFRT.INVOICE_TBL_FK IS NULL ");
            strBuilder.Append("            AND JOBFRT.INV_AGENT_TRN_FK IS NULL");
            strBuilder.Append("            AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL)");
            strBuilder.Append("            THEN 'False' ELSE 'True' END) CHK,PAR.FRT_BOF_FK,FMT.PREFERENCE");

            strBuilder.Append(" FROM ");
            strBuilder.Append(" JOB_CARD_TRN JOB, ");
            strBuilder.Append(" JOB_TRN_FD JOBFRT,");
            strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
            strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,CONTAINER_TYPE_MST_TBL CON,PARAMETERS_TBL PAR");
            strBuilder.Append(" WHERE");
            strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
            strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
            strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK=JOBFRT.JOB_CARD_TRN_FK AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) ");
            if (Edit == false)
            {
                strBuilder.Append(" AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ");
            }

            strBuilder.Append(" AND JOBFRT.FREIGHT_TYPE=" + frtType);

            strBuilder.Append(" AND JOBFRT.JOB_CARD_TRN_FK IN (" + strJobPks + ")");
            strBuilder.Append(" AND JOBFRT.frtpayer_cust_mst_fk in ( " + CustPk + ")  ");
            strBuilder.Append(" UNION");
            strBuilder.Append(" " + sb.ToString() + "");
            strBuilder.Append(" UNION");
            strBuilder.Append(" " + sb1.ToString() + "");

            strBuilder.Append(" )");
            strBuilder.Append(" UNION");
            strBuilder.Append("  SELECT ");
            strBuilder.Append(" JOB.JOBCARD_REF_NO, ");
            strBuilder.Append(" JOB.JOBCARD_DATE,'Oth.Chrg' UNIT,");
            strBuilder.Append(" JOBOTH.JOB_TRN_OTH_PK,");
            strBuilder.Append(" JOBOTH.JOB_CARD_TRN_FK JOBFK,");
            strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK,");
            strBuilder.Append(" JOBOTH.CURRENCY_MST_FK,");
            strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
            strBuilder.Append(" 'Prepaid' AS PC,");
            strBuilder.Append(" CUMT.CURRENCY_ID,");
            strBuilder.Append(" JOBOTH.AMOUNT,");

            strBuilder.Append(" (CASE ");
            strBuilder.Append(" WHEN (JOBOTH.INV_CUST_TRN_FK IS NULL ");
            strBuilder.Append("    AND JOBOTH.INV_AGENT_TRN_FK IS NULL ");

            strBuilder.Append("    AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL )THEN NULL ");
            strBuilder.Append(" WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
            strBuilder.Append("   (SELECT SUM(DISTINCT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN , CONSOL_INVOICE_TBL CIT ");
            strBuilder.Append("    WHERE TRN.FRT_OTH_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
            strBuilder.Append("    AND TRN.FRT_OTH_ELEMENT=2 AND CIT.CONSOL_INVOICE_PK=TRN.CONSOL_INVOICE_FK AND CIT.CHK_INVOICE<>2  ");
            strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBOTH.CONSOL_INVOICE_TRN_FK ");
            strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)");

            strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN , INV_AGENT_TBL IAT  ");
            strBuilder.Append("      WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
            strBuilder.Append("      AND TRN.COST_FRT_ELEMENT=3 AND IAT.INV_AGENT_PK=TRN.INV_AGENT_FK AND IAT.CHK_INVOICE<>2 ");
            strBuilder.Append("      AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_FK) END) INV_AMT,");

            strBuilder.Append("(CASE WHEN (JOBOTH.INV_CUST_TRN_FK IS NULL ");
            strBuilder.Append(" AND JOBOTH.INV_AGENT_TRN_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL)");
            strBuilder.Append(" THEN 'False' ELSE 'True' END) CHK, PAR.FRT_BOF_FK, FMT.PREFERENCE ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" JOB_CARD_TRN JOB,");
            strBuilder.Append(" JOB_TRN_OTH_CHRG JOBOTH,");
            strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
            strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT, PARAMETERS_TBL PAR");
            strBuilder.Append(" WHERE");
            strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
            strBuilder.Append(" AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
            strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK= JOBOTH.JOB_CARD_TRN_FK");
            strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) ");
            strBuilder.Append(" AND JOBOTH.Freight_Type= " + frtType);
            strBuilder.Append(" AND JOBOTH.JOB_CARD_TRN_FK IN(" + strJobPks + ")");
            if (Process == 2)
            {
                strBuilder.Append("   AND JOBOTH.frtpayer_cust_mst_fk in ( " + CustPk + ") ");
            }
            else
            {
                strBuilder.Append("   AND JOBOTH.frtpayer_cust_mst_fk in ( " + CustPk + ") ORDER BY preference, unit )");
            }

            if (Process == 2)
            {
                strBuilder.Append(" UNION SELECT * FROM (SELECT DCH.DEM_CALC_ID JOBCARD_REF_NO,");
                strBuilder.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE,");
                strBuilder.Append("       'DET' UNIT,");
                strBuilder.Append("       TO_NUMBER(NULL) JOB_TRN_SEA_IMP_OTH_PK,");
                strBuilder.Append("       DCH.DEM_CALC_HDR_PK JOBFK,");
                strBuilder.Append("       FMT.FREIGHT_ELEMENT_MST_PK,");
                strBuilder.Append("       CUMT.CURRENCY_MST_PK,");
                strBuilder.Append("       FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append("       'Collect' PC,");
                strBuilder.Append("       CUMT.CURRENCY_ID,");
                strBuilder.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
                strBuilder.Append("          FROM DEM_CALC_DTL DCD");
                strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FREIGHT_AMT,");

                strBuilder.Append("       CASE WHEN DCH.DET_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
                strBuilder.Append("          FROM DEM_CALC_DTL DCD");
                strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) ");
                strBuilder.Append("       ELSE NULL END INV_AMT,");

                strBuilder.Append("       'True' CHK,");
                strBuilder.Append("       PAR.FRT_BOF_FK,");
                strBuilder.Append("       FMT.PREFERENCE");
                strBuilder.Append("  FROM DEM_CALC_HDR            DCH,");

                strBuilder.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                strBuilder.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append("       PARAMETERS_TBL          PAR");
                strBuilder.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK");

                strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DET'");
                strBuilder.Append("   AND FMT.ACTIVE_FLAG = 1");
                strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                strBuilder.Append("   AND DCH.DEM_CALC_HDR_PK IN (" + strDemPKList + ") ");

                strBuilder.Append(" UNION SELECT DCH.DEM_CALC_ID JOBCARD_REF_NO,");
                strBuilder.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE,");
                strBuilder.Append("       'DEM' UNIT,");
                strBuilder.Append("       TO_NUMBER(NULL) JOB_TRN_SEA_IMP_OTH_PK,");
                strBuilder.Append("       DCH.DEM_CALC_HDR_PK JOBFK,");
                strBuilder.Append("       FMT.FREIGHT_ELEMENT_MST_PK,");
                strBuilder.Append("       CUMT.CURRENCY_MST_PK,");
                strBuilder.Append("       FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append("       'Collect' PC,");
                strBuilder.Append("       CUMT.CURRENCY_ID,");
                strBuilder.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
                strBuilder.Append("          FROM DEM_CALC_DTL DCD");
                strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FREIGHT_AMT,");

                strBuilder.Append("       CASE WHEN DCH.DEM_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
                strBuilder.Append("          FROM DEM_CALC_DTL DCD");
                strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) ");
                strBuilder.Append("       ELSE NULL END INV_AMT,");

                strBuilder.Append("       'True' CHK,");
                strBuilder.Append("       PAR.FRT_BOF_FK,");
                strBuilder.Append("       FMT.PREFERENCE");
                strBuilder.Append("  FROM DEM_CALC_HDR            DCH,");

                strBuilder.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                strBuilder.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append("       PARAMETERS_TBL          PAR");
                strBuilder.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK");
                strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DEM'");

                strBuilder.Append("   AND FMT.ACTIVE_FLAG = 1");
                strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                strBuilder.Append("   AND DCH.DEM_CALC_HDR_PK IN (" + strDemPKList + ")) WHERE FREIGHT_AMT > 0 ");

                strBuilder.Append("  ORDER BY unit,preference  ) ");
            }
            //If BizType = 1 And Process = 1 Then
            //    strBuilder.Append(" SELECT JOBCARD_REF_NO, JOBCARD_DATE, UNIT, JOB_TRN_AIR_EXP_FD_PK, JOBFK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC, CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK, FRT_AFC_FK FROM ( ")
            //    strBuilder.Append(" select JOBCARD_REF_NO ,JOBCARD_DATE,unit,JOB_TRN_AIR_EXP_FD_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,")
            //    strBuilder.Append(" CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC,CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK,FRT_AFC_FK, PREFERENCE from ")
            //    strBuilder.Append(" ( SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK,")
            //    strBuilder.Append(" JOB.JOBCARD_REF_NO,")
            //    strBuilder.Append(" JOB.JOBCARD_DATE, ")
            //    strBuilder.Append(" (CASE WHEN NVL(JOBFRT.SERVICE_TYPE_FLAG,0)=1 THEN ")
            //    strBuilder.Append("      DECODE(JOBFRT.BASIS_FK,1,'Unit',2,'Flat Rate',3,'Kgs',4,'MT',5,'CBM') ELSE ")
            //    strBuilder.Append("    UPPER(JOBFRT.QUANTITY) END) UNIT,")
            //    strBuilder.Append(" JOBFRT.JOB_TRN_AIR_EXP_FD_PK,")
            //    strBuilder.Append(" JOBFRT.JOB_CARD_TRN_PK JOBFK,")
            //    strBuilder.Append(" JOBFRT.CURRENCY_MST_FK,")
            //    strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'Prepaid',2,'Collect') AS PC,")
            //    strBuilder.Append(" CUMT.CURRENCY_ID,")
            //    strBuilder.Append(" JOBFRT.FREIGHT_AMT,")

            //    strBuilder.Append(" (CASE ")

            //    strBuilder.Append(" WHEN (JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NULL ")
            //    strBuilder.Append("   AND JOBFRT.INV_AGENT_TRN_AIR_EXP_FK IS NULL")
            //    strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ) THEN NULL ")

            //    strBuilder.Append(" WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT SUM(DISTINCT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN ")
            //    strBuilder.Append("   WHERE TRN.FRT_OTH_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("   AND TRN.FRT_OTH_ELEMENT=1  ")
            //    strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)")

            //    strBuilder.Append(" WHEN JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_EXP_TBL TRN ")
            //    strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=2  ")
            //    strBuilder.Append("   AND TRN.INV_CUST_TRN_AIR_EXP_PK=JOBFRT.INV_CUST_TRN_AIR_EXP_FK) ")

            //    strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN")
            //    strBuilder.Append("    WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("    AND TRN.COST_FRT_ELEMENT=2 ")
            //    strBuilder.Append("    AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_AIR_EXP_FK) END) INV_AMT,")

            //    strBuilder.Append(" (CASE WHEN (JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NULL ")
            //    strBuilder.Append("            AND JOBFRT.INV_AGENT_TRN_AIR_EXP_FK IS NULL")
            //    strBuilder.Append("            AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL)")

            //    strBuilder.Append("            THEN 'False' ELSE 'True' END) CHK, PAR.FRT_AFC_FK, FMT.PREFERENCE ")

            //    strBuilder.Append(" FROM ")
            //    strBuilder.Append(" JOB_CARD_TRN JOB, ")
            //    strBuilder.Append(" JOB_TRN_FD JOBFRT,")
            //    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR")
            //    strBuilder.Append(" WHERE")
            //    strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK")
            //    strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK")
            //    strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK=JOBFRT.JOB_CARD_TRN_PK")
            //    strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_AFC_FK(+)")

            //    strBuilder.Append(" AND JOBFRT.JOB_CARD_TRN_PK IN (" & strJobPks & ")")
            //    strBuilder.Append("   AND JOBFRT.frtpayer_cust_mst_fk in ( " & CustPk & ") ")
            //    strBuilder.Append(" UNION")
            //    strBuilder.Append(" " & sb.ToString & "")
            //    strBuilder.Append(" UNION")
            //    strBuilder.Append(" " & sb1.ToString & "")
            //    strBuilder.Append(" )")
            //    strBuilder.Append(" UNION")

            //    strBuilder.Append("  SELECT ")
            //    strBuilder.Append(" JOB.JOBCARD_REF_NO, ")
            //    strBuilder.Append(" JOB.JOBCARD_DATE,'Oth.Chrg' UNIT, ")
            //    strBuilder.Append(" JOBOTH.JOB_TRN_AIR_EXP_OTH_PK,")
            //    strBuilder.Append(" JOBOTH.JOB_CARD_TRN_PK JOBFK,")
            //    strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK,")
            //    strBuilder.Append(" JOBOTH.CURRENCY_MST_FK,")
            //    strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append(" 'Prepaid' AS PC,")
            //    strBuilder.Append(" CUMT.CURRENCY_ID,")
            //    strBuilder.Append(" JOBOTH.AMOUNT,")

            //    strBuilder.Append(" (CASE ")
            //    strBuilder.Append(" WHEN (JOBOTH.INV_CUST_TRN_AIR_EXP_FK IS NULL ")
            //    strBuilder.Append("    AND JOBOTH.INV_AGENT_TRN_AIR_EXP_FK IS NULL ")

            //    strBuilder.Append("    AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL  ")
            //    strBuilder.Append(" ) THEN NULL ")

            //    strBuilder.Append(" WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT SUM(DISTINCT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN ")
            //    strBuilder.Append("    WHERE TRN.FRT_OTH_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("    AND TRN.FRT_OTH_ELEMENT=2  ")
            //    strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)")

            //    strBuilder.Append(" WHEN JOBOTH.Inv_Cust_Trn_AIR_Exp_Fk IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_EXP_TBL TRN ")
            //    strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=3  ")
            //    strBuilder.Append("   AND TRN.INV_CUST_TRN_AIR_EXP_PK=JOBOTH.Inv_Cust_Trn_AIR_Exp_Fk) ")
            //    strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN")
            //    strBuilder.Append("      WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("      AND TRN.COST_FRT_ELEMENT=3 ")
            //    strBuilder.Append("      AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_AIR_EXP_FK) END) INV_AMT,")

            //    strBuilder.Append("(CASE WHEN (JOBOTH.INV_CUST_TRN_AIR_EXP_FK IS NULL ")
            //    strBuilder.Append(" AND JOBOTH.INV_AGENT_TRN_AIR_EXP_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL)")
            //    strBuilder.Append(" THEN 'False' ELSE 'True' END) CHK, PAR.FRT_AFC_FK, FMT.PREFERENCE ")
            //    strBuilder.Append(" FROM ")
            //    strBuilder.Append(" JOB_CARD_TRN JOB,")
            //    strBuilder.Append(" JOB_TRN_OTH_CHRG JOBOTH,")
            //    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR")
            //    strBuilder.Append(" WHERE")
            //    strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK")
            //    strBuilder.Append(" AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK")
            //    strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK= JOBOTH.JOB_CARD_TRN_PK")
            //    strBuilder.Append(" AND JOBOTH.JOB_CARD_TRN_PK IN(" & strJobPks & ")")
            //    strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_AFC_FK(+)")
            //    strBuilder.Append("   AND JOBOTH.frtpayer_cust_mst_fk in ( " & CustPk & ")")
            //    strBuilder.Append(" AND JOBOTH.Freight_Type=1 ORDER BY unit,preference) ")
            //ElseIf BizType = 2 And Process = 2 Then
            //    strBuilder.Append(" SELECT JOBCARD_REF_NO, JOBCARD_DATE, UNIT, JOB_TRN_SEA_IMP_FD_PK, JOBFK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC, CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK, FRT_BOF_FK FROM ( ")
            //    strBuilder.Append(" select JOBCARD_REF_NO ,JOBCARD_DATE,unit,JOB_TRN_SEA_IMP_FD_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,")
            //    strBuilder.Append(" CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC,CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK,FRT_BOF_FK, PREFERENCE from ")
            //    strBuilder.Append(" ( SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK, ")
            //    strBuilder.Append(" JOB.JOBCARD_REF_NO,")
            //    strBuilder.Append(" JOB.JOBCARD_DATE,")
            //    strBuilder.Append(" (CASE when jobfrt.container_type_mst_fk is not null then ")
            //    strBuilder.Append(" con.container_type_mst_id Else ")
            //    strBuilder.Append(" DECODE(JOBFRT.BASIS_FK,1,'Unit',2,'Flat Rate',3,'Kgs',4,'MT',5,'CBM') END) UNIT,")
            //    strBuilder.Append(" JOBFRT.JOB_TRN_SEA_IMP_FD_PK,")
            //    strBuilder.Append(" JOBFRT.JOB_CARD_SEA_IMP_FK JOBFK,")

            //    strBuilder.Append(" JOBFRT.CURRENCY_MST_FK,")
            //    strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'Prepaid',2,'Collect') AS PC,")
            //    strBuilder.Append(" CUMT.CURRENCY_ID,")
            //    strBuilder.Append(" JOBFRT.FREIGHT_AMT,")

            //    strBuilder.Append(" (CASE ")

            //    strBuilder.Append(" WHEN (JOBFRT.INVOICE_TBL_FK IS NULL ")
            //    strBuilder.Append("   AND JOBFRT.INV_AGENT_TRN_SEA_IMP_FK IS NULL")
            //    strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ) THEN NULL ")

            //    strBuilder.Append(" WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT  SUM(DISTINCT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN ")
            //    strBuilder.Append("   WHERE TRN.FRT_OTH_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("   AND TRN.FRT_OTH_ELEMENT=1  ")
            //    strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBFRT.CONSOL_INVOICE_TRN_FK ")
            //    strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)")

            //    strBuilder.Append(" WHEN JOBFRT.INVOICE_TBL_FK IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_SEA_IMP_TBL TRN ")
            //    strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=2  ")
            //    strBuilder.Append("   AND TRN.INV_CUST_TRN_SEA_IMP_PK=JOBFRT.INVOICE_TBL_FK) ")

            //    strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN")
            //    strBuilder.Append("    WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("    AND TRN.COST_FRT_ELEMENT=2 ")
            //    strBuilder.Append("    AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_SEA_IMP_FK) END) INV_AMT,")

            //    strBuilder.Append(" (CASE WHEN (JOBFRT.INVOICE_TBL_FK IS NULL ")
            //    strBuilder.Append("            AND JOBFRT.INV_AGENT_TRN_SEA_IMP_FK IS NULL")
            //    strBuilder.Append("            AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL)")

            //    strBuilder.Append("            THEN 'False' ELSE 'True' END) CHK, PAR.FRT_BOF_FK , FMT.PREFERENCE ")

            //    strBuilder.Append(" FROM ")
            //    strBuilder.Append(" JOB_CARD_TRN JOB, ")
            //    strBuilder.Append(" JOB_TRN_FD JOBFRT,")
            //    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,CONTAINER_TYPE_MST_TBL CON,PARAMETERS_TBL PAR")
            //    strBuilder.Append(" WHERE")
            //    strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK")
            //    strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK")
            //    strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK=JOBFRT.JOB_CARD_SEA_IMP_FK AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) ")
            //    strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) AND JOBFRT.JOB_CARD_SEA_IMP_FK IN (" & strJobPks & ")")

            //    strBuilder.Append("   AND JOBFRT.frtpayer_cust_mst_fk in ( " & CustPk & ") ")
            //    strBuilder.Append(" UNION")
            //    strBuilder.Append(" " & sb.ToString & "")
            //    strBuilder.Append(" UNION")
            //    strBuilder.Append(" " & sb1.ToString & "")
            //    strBuilder.Append(" )")
            //    strBuilder.Append(" UNION")
            //    strBuilder.Append("  SELECT ")
            //    strBuilder.Append(" JOB.JOBCARD_REF_NO, ")
            //    strBuilder.Append(" JOB.JOBCARD_DATE,'Oth.Chrg' UNIT,")
            //    strBuilder.Append(" JOBOTH.JOB_TRN_SEA_IMP_OTH_PK,")
            //    strBuilder.Append(" JOBOTH.JOB_CARD_SEA_IMP_FK JOBFK,")
            //    strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK,")
            //    strBuilder.Append(" JOBOTH.CURRENCY_MST_FK,")
            //    strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append(" 'Collect' AS PC,")
            //    strBuilder.Append(" CUMT.CURRENCY_ID,")
            //    strBuilder.Append(" JOBOTH.AMOUNT,")

            //    strBuilder.Append(" (CASE ")

            //    strBuilder.Append(" WHEN (JOBOTH.INV_CUST_TRN_SEA_IMP_FK IS NULL ")
            //    strBuilder.Append("    AND JOBOTH.INV_AGENT_TRN_SEA_IMP_FK IS NULL ")

            //    strBuilder.Append("    AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL ) THEN NULL ")

            //    strBuilder.Append(" WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT SUM(DISTINCT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN ")
            //    strBuilder.Append("    WHERE TRN.FRT_OTH_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("    AND TRN.FRT_OTH_ELEMENT=2  ")
            //    strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBOTH.CONSOL_INVOICE_TRN_FK ")
            //    strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)")

            //    strBuilder.Append(" WHEN JOBOTH.Inv_Cust_Trn_Sea_IMP_Fk IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_SEA_IMP_TBL TRN ")
            //    strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=3  ")
            //    strBuilder.Append("   AND TRN.INV_CUST_TRN_SEA_IMP_PK=JOBOTH.Inv_Cust_Trn_Sea_IMP_Fk) ")

            //    strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN")
            //    strBuilder.Append("      WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("      AND TRN.COST_FRT_ELEMENT=3 ")
            //    strBuilder.Append("      AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_SEA_IMP_FK) END) INV_AMT,")

            //    strBuilder.Append("(CASE WHEN (JOBOTH.INV_CUST_TRN_SEA_IMP_FK IS NULL ")
            //    strBuilder.Append(" AND JOBOTH.INV_AGENT_TRN_SEA_IMP_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL)")
            //    strBuilder.Append(" THEN 'False' ELSE 'True' END) CHK,PAR.FRT_BOF_FK , FMT.PREFERENCE")
            //    strBuilder.Append(" FROM ")
            //    strBuilder.Append(" JOB_CARD_TRN JOB,")
            //    strBuilder.Append(" JOB_TRN_OTH_CHRG JOBOTH,")
            //    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR")
            //    strBuilder.Append(" WHERE")
            //    strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK")
            //    strBuilder.Append(" AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK")
            //    strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK= JOBOTH.JOB_CARD_SEA_IMP_FK")
            //    strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) AND JOBOTH.JOB_CARD_SEA_IMP_FK IN(" & strJobPks & ")")
            //    strBuilder.Append(" AND JOBOTH.frtpayer_cust_mst_fk in ( " & CustPk & ")")
            //    strBuilder.Append(" AND JOBOTH.Freight_Type=2 ")

            //    strBuilder.Append(" UNION SELECT * FROM (SELECT DCH.DEM_CALC_ID JOBCARD_REF_NO,")
            //    strBuilder.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE,")
            //    strBuilder.Append("       'DET' UNIT,")
            //    strBuilder.Append("       TO_NUMBER(NULL) JOB_TRN_SEA_IMP_OTH_PK,")
            //    strBuilder.Append("       DCH.DEM_CALC_HDR_PK JOBFK,")
            //    strBuilder.Append("       FMT.FREIGHT_ELEMENT_MST_PK,")
            //    strBuilder.Append("       CUMT.CURRENCY_MST_PK,")
            //    strBuilder.Append("       FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append("       'Collect' PC,")
            //    strBuilder.Append("       CUMT.CURRENCY_ID,")
            //    strBuilder.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ")
            //    strBuilder.Append("          FROM DEM_CALC_DTL DCD")
            //    strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FREIGHT_AMT,")

            //    strBuilder.Append("       CASE WHEN DCH.DET_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ")
            //    strBuilder.Append("          FROM DEM_CALC_DTL DCD")
            //    strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) ")
            //    strBuilder.Append("       ELSE NULL END INV_AMT,")

            //    strBuilder.Append("       'True' CHK,")
            //    strBuilder.Append("       PAR.FRT_BOF_FK,")
            //    strBuilder.Append("       FMT.PREFERENCE")
            //    strBuilder.Append("  FROM DEM_CALC_HDR            DCH,")

            //    strBuilder.Append("       CURRENCY_TYPE_MST_TBL   CUMT,")
            //    strBuilder.Append("       FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append("       PARAMETERS_TBL          PAR")
            //    strBuilder.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK")

            //    strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DET'")
            //    strBuilder.Append("   AND FMT.ACTIVE_FLAG = 1")
            //    strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)")
            //    strBuilder.Append("   AND DCH.DEM_CALC_HDR_PK IN (" & strDemPKList & ") ")

            //    strBuilder.Append(" UNION SELECT DCH.DEM_CALC_ID JOBCARD_REF_NO,")
            //    strBuilder.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE,")
            //    strBuilder.Append("       'DEM' UNIT,")
            //    strBuilder.Append("       TO_NUMBER(NULL) JOB_TRN_SEA_IMP_OTH_PK,")
            //    strBuilder.Append("       DCH.DEM_CALC_HDR_PK JOBFK,")
            //    strBuilder.Append("       FMT.FREIGHT_ELEMENT_MST_PK,")
            //    strBuilder.Append("       CUMT.CURRENCY_MST_PK,")
            //    strBuilder.Append("       FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append("       'Collect' PC,")
            //    strBuilder.Append("       CUMT.CURRENCY_ID,")
            //    strBuilder.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ")
            //    strBuilder.Append("          FROM DEM_CALC_DTL DCD")
            //    strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FREIGHT_AMT,")

            //    strBuilder.Append("       CASE WHEN DCH.DEM_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ")
            //    strBuilder.Append("          FROM DEM_CALC_DTL DCD")
            //    strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) ")
            //    strBuilder.Append("       ELSE NULL END INV_AMT,")

            //    strBuilder.Append("       'True' CHK,")
            //    strBuilder.Append("       PAR.FRT_BOF_FK,")
            //    strBuilder.Append("       FMT.PREFERENCE")
            //    strBuilder.Append("  FROM DEM_CALC_HDR            DCH,")

            //    strBuilder.Append("       CURRENCY_TYPE_MST_TBL   CUMT,")
            //    strBuilder.Append("       FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append("       PARAMETERS_TBL          PAR")
            //    strBuilder.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK")
            //    strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DEM'")

            //    strBuilder.Append("   AND FMT.ACTIVE_FLAG = 1")
            //    strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)")
            //    strBuilder.Append("   AND DCH.DEM_CALC_HDR_PK IN (" & strDemPKList & ")) WHERE FREIGHT_AMT > 0 ")

            //    strBuilder.Append("  ORDER BY unit,preference  ) ")

            //ElseIf BizType = 1 And Process = 2 Then
            //    strBuilder.Append(" SELECT JOBCARD_REF_NO, JOBCARD_DATE, UNIT, JOB_TRN_AIR_IMP_FD_PK, JOBFK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC, CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK, FRT_AFC_FK FROM ( ")
            //    strBuilder.Append(" select JOBCARD_REF_NO ,JOBCARD_DATE,unit,JOB_TRN_AIR_IMP_FD_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,")
            //    strBuilder.Append(" CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC,CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK,FRT_AFC_FK, PREFERENCE from ")
            //    strBuilder.Append(" ( SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK,")
            //    strBuilder.Append(" JOB.JOBCARD_REF_NO,")
            //    strBuilder.Append(" JOB.JOBCARD_DATE, ")
            //    strBuilder.Append(" (CASE WHEN NVL(JOBFRT.SERVICE_TYPE_FLAG,0)=1 THEN ")
            //    strBuilder.Append("      DECODE(JOBFRT.BASIS_FK,1,'Unit',2,'Flat Rate',3,'Kgs',4,'MT',5,'CBM') ELSE ")
            //    strBuilder.Append("    UPPER(JOBFRT.QUANTITY) END) UNIT,")
            //    strBuilder.Append(" JOBFRT.JOB_TRN_AIR_IMP_FD_PK,")
            //    strBuilder.Append(" JOBFRT.JOB_CARD_AIR_IMP_FK JOBFK,")
            //    strBuilder.Append(" JOBFRT.CURRENCY_MST_FK,")
            //    strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'Prepaid',2,'Collect') AS PC,")
            //    strBuilder.Append(" CUMT.CURRENCY_ID,")
            //    strBuilder.Append(" JOBFRT.FREIGHT_AMT,")

            //    strBuilder.Append(" (CASE ")

            //    strBuilder.Append(" WHEN (JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NULL ")
            //    strBuilder.Append("   AND JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NULL")
            //    strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ) THEN NULL ")

            //    strBuilder.Append(" WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT SUM(DISTINCT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN ")
            //    strBuilder.Append("   WHERE TRN.FRT_OTH_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("   AND TRN.FRT_OTH_ELEMENT=1  ")
            //    strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)")

            //    strBuilder.Append(" WHEN JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_IMP_TBL TRN ")
            //    strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=2  ")
            //    strBuilder.Append("   AND TRN.INV_CUST_TRN_AIR_IMP_PK=JOBFRT.INV_CUST_TRN_AIR_IMP_FK) ")

            //    strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN")
            //    strBuilder.Append("    WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("    AND TRN.COST_FRT_ELEMENT=2 ")
            //    strBuilder.Append("    AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_AIR_IMP_FK) END) INV_AMT,")

            //    strBuilder.Append(" (CASE WHEN (JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NULL ")
            //    strBuilder.Append("            AND JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NULL")
            //    strBuilder.Append("            AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL)")

            //    strBuilder.Append("            THEN 'False' ELSE 'True' END) CHK, PAR.FRT_AFC_FK , FMT.PREFERENCE")
            //    strBuilder.Append(" FROM ")
            //    strBuilder.Append(" JOB_CARD_TRN JOB, ")
            //    strBuilder.Append(" JOB_TRN_AIR_IMP_FD JOBFRT,")
            //    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR")
            //    strBuilder.Append(" WHERE")
            //    strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK")
            //    strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK")
            //    strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK=JOBFRT.JOB_CARD_AIR_IMP_FK")
            //    strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)")

            //    strBuilder.Append(" AND JOBFRT.JOB_CARD_AIR_IMP_FK IN (" & strJobPks & ")")

            //    strBuilder.Append("   AND JOBFRT.frtpayer_cust_mst_fk in ( " & CustPk & ") ")
            //    strBuilder.Append(" UNION")
            //    strBuilder.Append(" " & sb.ToString & "")
            //    strBuilder.Append(" UNION")
            //    strBuilder.Append(" " & sb1.ToString & "")
            //    strBuilder.Append(" )")
            //    strBuilder.Append(" UNION")

            //    strBuilder.Append("  SELECT ")
            //    strBuilder.Append(" JOB.JOBCARD_REF_NO, ")
            //    strBuilder.Append(" JOB.JOBCARD_DATE,'Oth.Chrg' UNIT ,")
            //    strBuilder.Append(" JOBOTH.JOB_TRN_AIR_IMP_OTH_PK,")
            //    strBuilder.Append(" JOBOTH.JOB_CARD_AIR_IMP_FK JOBFK,")
            //    strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK,")
            //    strBuilder.Append(" JOBOTH.CURRENCY_MST_FK,")
            //    strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append(" 'Collect' AS PC,")
            //    strBuilder.Append(" CUMT.CURRENCY_ID,")
            //    strBuilder.Append(" JOBOTH.AMOUNT,")

            //    strBuilder.Append(" (CASE ")

            //    strBuilder.Append(" WHEN (JOBOTH.INV_CUST_TRN_AIR_IMP_FK IS NULL ")
            //    strBuilder.Append("    AND JOBOTH.INV_AGENT_TRN_AIR_IMP_FK IS NULL ")

            //    strBuilder.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL ")
            //    strBuilder.Append(" ) THEN NULL ")
            //    strBuilder.Append(" WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT SUM(DISTINCT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN ")
            //    strBuilder.Append("    WHERE TRN.FRT_OTH_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("    AND TRN.FRT_OTH_ELEMENT=2  ")
            //    strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)")

            //    strBuilder.Append(" WHEN JOBOTH.Inv_Cust_Trn_AIR_IMP_Fk IS NOT NULL THEN ")
            //    strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_IMP_TBL TRN ")
            //    strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=3  ")
            //    strBuilder.Append("   AND TRN.INV_CUST_TRN_AIR_IMP_PK=JOBOTH.Inv_Cust_Trn_AIR_IMP_Fk) ")

            //    strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN")
            //    strBuilder.Append("      WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ")
            //    strBuilder.Append("      AND TRN.COST_FRT_ELEMENT=3 ")
            //    strBuilder.Append("      AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_AIR_IMP_FK) END) INV_AMT,")

            //    strBuilder.Append("(CASE WHEN (JOBOTH.INV_CUST_TRN_AIR_IMP_FK IS NULL ")
            //    strBuilder.Append(" AND JOBOTH.INV_AGENT_TRN_AIR_IMP_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL)")
            //    strBuilder.Append(" THEN 'False' ELSE 'True' END) CHK ,PAR.FRT_AFC_FK , FMT.PREFERENCE")
            //    strBuilder.Append(" FROM ")
            //    strBuilder.Append(" JOB_CARD_TRN JOB,")
            //    strBuilder.Append(" JOB_TRN_AIR_IMP_OTH_CHRG JOBOTH,")
            //    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR")
            //    strBuilder.Append(" WHERE")
            //    strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK")
            //    strBuilder.Append(" AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK")
            //    strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK= JOBOTH.JOB_CARD_AIR_IMP_FK")
            //    strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)")

            //    strBuilder.Append(" AND JOBOTH.Freight_Type=2 ")
            //    strBuilder.Append(" AND JOBOTH.JOB_CARD_AIR_IMP_FK IN(" & strJobPks & ")")
            //    strBuilder.Append("   AND JOBOTH.frtpayer_cust_mst_fk in ( " & CustPk & ") ")

            //    strBuilder.Append(" UNION SELECT * FROM (SELECT DCH.DEM_CALC_ID JOBCARD_REF_NO,")
            //    strBuilder.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE,")
            //    strBuilder.Append("       'DET' UNIT,")
            //    strBuilder.Append("       TO_NUMBER(NULL) JOB_TRN_SEA_IMP_OTH_PK,")
            //    strBuilder.Append("       DCH.DEM_CALC_HDR_PK JOBFK,")
            //    strBuilder.Append("       FMT.FREIGHT_ELEMENT_MST_PK,")
            //    strBuilder.Append("       CUMT.CURRENCY_MST_PK,")
            //    strBuilder.Append("       FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append("       'Collect' PC,")
            //    strBuilder.Append("       CUMT.CURRENCY_ID,")
            //    strBuilder.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ")
            //    strBuilder.Append("          FROM DEM_CALC_DTL DCD")
            //    strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FREIGHT_AMT,")

            //    strBuilder.Append("       CASE WHEN DCH.DET_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ")
            //    strBuilder.Append("          FROM DEM_CALC_DTL DCD")
            //    strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) ")
            //    strBuilder.Append("       ELSE NULL END INV_AMT,")

            //    strBuilder.Append("       'True' CHK,")
            //    strBuilder.Append("       PAR.FRT_BOF_FK,")
            //    strBuilder.Append("       FMT.PREFERENCE")
            //    strBuilder.Append("  FROM DEM_CALC_HDR            DCH,")

            //    strBuilder.Append("       CURRENCY_TYPE_MST_TBL   CUMT,")
            //    strBuilder.Append("       FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append("       PARAMETERS_TBL          PAR")
            //    strBuilder.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK")

            //    strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DET'")
            //    strBuilder.Append("   AND FMT.ACTIVE_FLAG = 1")
            //    strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)")
            //    strBuilder.Append("   AND DCH.DEM_CALC_HDR_PK IN (" & strDemPKList & ") ")

            //    strBuilder.Append(" UNION SELECT DCH.DEM_CALC_ID JOBCARD_REF_NO,")
            //    strBuilder.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE,")
            //    strBuilder.Append("       'DEM' UNIT,")
            //    strBuilder.Append("       TO_NUMBER(NULL) JOB_TRN_SEA_IMP_OTH_PK,")
            //    strBuilder.Append("       DCH.DEM_CALC_HDR_PK JOBFK,")
            //    strBuilder.Append("       FMT.FREIGHT_ELEMENT_MST_PK,")
            //    strBuilder.Append("       CUMT.CURRENCY_MST_PK,")
            //    strBuilder.Append("       FMT.FREIGHT_ELEMENT_NAME,")
            //    strBuilder.Append("       'Collect' PC,")
            //    strBuilder.Append("       CUMT.CURRENCY_ID,")
            //    strBuilder.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ")
            //    strBuilder.Append("          FROM DEM_CALC_DTL DCD")
            //    strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FREIGHT_AMT,")

            //    strBuilder.Append("       CASE WHEN DCH.DEM_INVOICE_TRN_FK IS NOT NULL THEN ")
            //    strBuilder.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ")
            //    strBuilder.Append("          FROM DEM_CALC_DTL DCD")
            //    strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) ")
            //    strBuilder.Append("       ELSE NULL END INV_AMT,")

            //    strBuilder.Append("       'True' CHK,")
            //    strBuilder.Append("       PAR.FRT_BOF_FK,")
            //    strBuilder.Append("       FMT.PREFERENCE")
            //    strBuilder.Append("  FROM DEM_CALC_HDR            DCH,")
            //    strBuilder.Append("       CURRENCY_TYPE_MST_TBL   CUMT,")
            //    strBuilder.Append("       FREIGHT_ELEMENT_MST_TBL FMT,")
            //    strBuilder.Append("       PARAMETERS_TBL          PAR")
            //    strBuilder.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK")
            //    strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DEM'")
            //    strBuilder.Append("   AND FMT.ACTIVE_FLAG = 1")
            //    strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)")
            //    strBuilder.Append("   AND DCH.DEM_CALC_HDR_PK IN (" & strDemPKList & ")) WHERE FREIGHT_AMT > 0 ")

            //    strBuilder.Append("   ORDER BY unit,preference )")
            //End If

            try
            {
                return objWk.GetDataSet(strBuilder.ToString());
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
        #endregion

        #region "FetchCreditLimit"
        public string FetchCreditLimit(string CustPk)
        {
            try
            {
                StringBuilder strQuery = new StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strQuery.Append(" select distinct nvl(cmt.credit_limit,0) from ");
                strQuery.Append("  customer_mst_tbl cmt,");
                strQuery.Append(" JOB_TRN_FD JOBFRT");
                strQuery.Append(" where  cmt.customer_mst_pk=JOBFRT.FRTPAYER_CUST_MST_FK ");
                strQuery.Append(" and JOBFRT.frtpayer_cust_mst_fk in ( " + CustPk + ")");
                return objWF.ExecuteScaler(strQuery.ToString());
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
        #endregion

        #region "FetchInvoiceData"

        #endregion
        public DataSet FetchInvoiceData(string strJobPks, string strCBJCPks, string strTPTPks, string strDemPKList, int intInvPk, int nBaseCurrPK, short BizType, short Process, int UserPk, string CustPk,
        string CreditLimit, string amount, int ExType = 1)
        {
            StringBuilder strQuery = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;
            string strsql = null;
            string vatcode = null;
            Int32 rowcunt = 0;
            Int32 Contpk = 0;
            string frtType = Convert.ToString((Process == 1 ? "1" : "2"));
            if (string.IsNullOrEmpty(strJobPks))
            {
                strJobPks = "0";
            }
            if (string.IsNullOrEmpty(strCBJCPks))
            {
                strCBJCPks = "0";
            }
            if (string.IsNullOrEmpty(strTPTPks))
            {
                strTPTPks = "0";
            }
            if (string.IsNullOrEmpty(strDemPKList))
            {
                strDemPKList = "0";
            }

            try
            {
                StringBuilder sb = new StringBuilder(5000);
                sb.Append("SELECT DISTINCT JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,");
                sb.Append("                'FREIGHT' AS TYPE,");
                sb.Append("                CT.CBJC_NO,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN JOBFRT.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
                sb.Append("                   CON.CONTAINER_TYPE_MST_ID");
                sb.Append("                  ELSE");
                sb.Append("                   DECODE(JOBFRT.BASIS_FK,");
                sb.Append("                          1,");
                sb.Append("                          'Unit',");
                sb.Append("                          2,");
                sb.Append("                          'Flat Rate',");
                sb.Append("                          3,");
                sb.Append("                          'Kgs',");
                sb.Append("                          4,");
                sb.Append("                          'MT',");
                sb.Append("                          5,");
                sb.Append("                          'CBM')");
                sb.Append("                END) UNIT,");
                sb.Append("                JOBFRT.CBJC_TRN_FD_PK AS PK,");
                sb.Append("                JOBFRT.CBJC_FK AS JOBCARD_FK,");
                sb.Append("                '1' FREIGHT_OR_OTH,");
                sb.Append("                JOBFRT.CURRENCY_MST_FK,");
                sb.Append("                FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                sb.Append("                '' AS ELEMENT,");
                sb.Append("                CUMT.CURRENCY_ID,");
                sb.Append("                '' AS CURR,");
                sb.Append("                JOBFRT.FREIGHT_AMT AS AMOUNT,");
                sb.Append("                GET_EX_RATE(JOBFRT.CURRENCY_MST_FK,");
                sb.Append("                            " + nBaseCurrPK + ",");
                sb.Append("                            TO_DATE(CT.CBJC_DATE, 'DD/MM/YYYY')) EXCHANGE_RATE,");
                sb.Append("                JOBFRT.FREIGHT_AMT *");
                sb.Append("                GET_EX_RATE(JOBFRT.CURRENCY_MST_FK,");
                sb.Append("                            " + nBaseCurrPK + ",");
                sb.Append("                            TO_DATE(CT.CBJC_DATE, 'DD/MM/YYYY')) AS INV_AMOUNT,");
                sb.Append("                (SELECT FETCH_VAT((SELECT FETCH_EU(CT.CBJC_PK, " + BizType + "," + Process + ")");
                sb.Append("                                    FROM DUAL),");
                sb.Append("                                  JOBFRT.FRTPAYER_CUST_MST_FK,");
                sb.Append("                                  " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb.Append("                                  JOBFRT.FREIGHT_TYPE,");
                sb.Append("                                  JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                                  1)");
                sb.Append("                   FROM DUAL) VAT_CODE,");
                sb.Append("                (SELECT FETCH_VAT((SELECT FETCH_EU(CT.CBJC_PK, " + BizType + "," + Process + ")");
                sb.Append("                                    FROM DUAL),");
                sb.Append("                                  JOBFRT.FRTPAYER_CUST_MST_FK,");
                sb.Append("                                  " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb.Append("                                  JOBFRT.FREIGHT_TYPE,");
                sb.Append("                                  JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                                  2)");
                sb.Append("                   FROM DUAL) VAT_PERCENT,");
                sb.Append("                ((SELECT FETCH_VAT((SELECT FETCH_EU(CT.CBJC_PK, " + BizType + "," + Process + ")");
                sb.Append("                                     FROM DUAL),");
                sb.Append("                                   JOBFRT.FRTPAYER_CUST_MST_FK,");
                sb.Append("                                   " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb.Append("                                   JOBFRT.FREIGHT_TYPE,");
                sb.Append("                                   JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                                   2)");
                sb.Append("                    FROM DUAL) *");
                sb.Append("                (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) / 100) TAX_AMOUNT,");
                sb.Append("                ((ABS((SELECT FETCH_VAT((SELECT FETCH_EU(CT.CBJC_PK, " + BizType + "," + Process + ")");
                sb.Append("                                          FROM DUAL),");
                sb.Append("                                        JOBFRT.FRTPAYER_CUST_MST_FK,");
                sb.Append("                                        " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb.Append("                                        JOBFRT.FREIGHT_TYPE,");
                sb.Append("                                        JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                                        2)");
                sb.Append("                         FROM DUAL) *");
                sb.Append("                      (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) / 100)) +");
                sb.Append("                JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) TOTAL_AMOUNT,");
                sb.Append("                '' AS REMARKS,");
                sb.Append("                'NEW' AS \"MODE1\",");
                sb.Append("                'FALSE' AS CHK,");
                sb.Append("                PAR.FRT_BOF_FK,");
                sb.Append("                FMT.PREFERENCE, '2' JOB_TYPE");
                sb.Append("  FROM CBJC_TRN_FD             JOBFRT,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                sb.Append("       CORPORATE_MST_TBL       CORP,");
                sb.Append("       CBJC_TBL                CT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL  CON,");
                sb.Append("       PARAMETERS_TBL          PAR");
                sb.Append(" WHERE JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CT.CBJC_PK = JOBFRT.CBJC_FK");
                sb.Append("   AND (JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
                sb.Append("       JOBFRT.CONTAINER_TYPE_MST_FK IS NULL)");
                sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                sb.Append("   AND JOBFRT.CBJC_FK IN (" + strCBJCPks + ")");
                sb.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("   AND JOBFRT.FREIGHT_TYPE = " + frtType);
                sb.Append("   AND JOBFRT.FREIGHT_AMT > 0");
                sb.Append("   AND JOBFRT.FRTPAYER_CUST_MST_FK IN (" + CustPk + ")");


                StringBuilder sb1 = new StringBuilder(5000);
                sb1.Append("SELECT DISTINCT JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,");
                sb1.Append("                'FREIGHT' AS TYPE,");
                sb1.Append("                TIST.TRANS_INST_REF_NO,");
                sb1.Append("                (CASE");
                sb1.Append("                  WHEN JOBFRT.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
                sb1.Append("                   CON.CONTAINER_TYPE_MST_ID");
                sb1.Append("                  ELSE");
                sb1.Append("                   DECODE(JOBFRT.BASIS_FK,");
                sb1.Append("                          1,");
                sb1.Append("                          'Unit',");
                sb1.Append("                          2,");
                sb1.Append("                          'Flat Rate',");
                sb1.Append("                          3,");
                sb1.Append("                          'Kgs',");
                sb1.Append("                          4,");
                sb1.Append("                          'MT',");
                sb1.Append("                          5,");
                sb1.Append("                          'CBM')");
                sb1.Append("                END) UNIT,");
                sb1.Append("                JOBFRT.TRANSPORT_TRN_FD_PK AS PK,");
                sb1.Append("                JOBFRT.TRANSPORT_INST_FK AS JOBCARD_FK,");
                sb1.Append("                '1' FREIGHT_OR_OTH,");
                sb1.Append("                JOBFRT.CURRENCY_MST_FK,");
                sb1.Append("                FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                sb1.Append("                '' AS ELEMENT,");
                sb1.Append("                CUMT.CURRENCY_ID,");
                sb1.Append("                '' AS CURR,");
                sb1.Append("                JOBFRT.FREIGHT_AMT AS AMOUNT,");
                sb1.Append("                GET_EX_RATE(JOBFRT.CURRENCY_MST_FK,");
                sb1.Append("                            " + nBaseCurrPK + ",");
                sb1.Append("                            TO_DATE(TIST.TRANS_INST_DATE, 'DD/MM/YYYY')) EXCHANGE_RATE,");
                sb1.Append("                JOBFRT.FREIGHT_AMT *");
                sb1.Append("                GET_EX_RATE(JOBFRT.CURRENCY_MST_FK,");
                sb1.Append("                            " + nBaseCurrPK + ",");
                sb1.Append("                            TO_DATE(TIST.TRANS_INST_DATE, 'DD/MM/YYYY')) AS INV_AMOUNT,");
                sb1.Append("                (SELECT FETCH_VAT((SELECT FETCH_EU(TIST.TRANSPORT_INST_SEA_PK,");
                sb1.Append("                                                  " + BizType + ",");
                sb1.Append("                                                  " + Process + ")");
                sb1.Append("                                    FROM DUAL),");
                sb1.Append("                                  JOBFRT.FRTPAYER_CUST_MST_FK,");
                sb1.Append("                                  " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb1.Append("                                  JOBFRT.FREIGHT_TYPE,");
                sb1.Append("                                  JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                sb1.Append("                                  1)");
                sb1.Append("                   FROM DUAL) VAT_CODE,");
                sb1.Append("                (SELECT FETCH_VAT((SELECT FETCH_EU(TIST.TRANSPORT_INST_SEA_PK,");
                sb1.Append("                                                  " + BizType + ",");
                sb1.Append("                                                  " + Process + ")");
                sb1.Append("                                    FROM DUAL),");
                sb1.Append("                                  JOBFRT.FRTPAYER_CUST_MST_FK,");
                sb1.Append("                                  " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb1.Append("                                  JOBFRT.FREIGHT_TYPE,");
                sb1.Append("                                  JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                sb1.Append("                                  2)");
                sb1.Append("                   FROM DUAL) VAT_PERCENT,");
                sb1.Append("                ((SELECT FETCH_VAT((SELECT FETCH_EU(TIST.TRANSPORT_INST_SEA_PK,");
                sb1.Append("                                                   " + BizType + ",");
                sb1.Append("                                                   " + Process + ")");
                sb1.Append("                                     FROM DUAL),");
                sb1.Append("                                   JOBFRT.FRTPAYER_CUST_MST_FK,");
                sb1.Append("                                   " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb1.Append("                                   JOBFRT.FREIGHT_TYPE,");
                sb1.Append("                                   JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                sb1.Append("                                   2)");
                sb1.Append("                    FROM DUAL) * (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) / 100) TAX_AMOUNT,");
                sb1.Append("                ((ABS((SELECT FETCH_VAT((SELECT FETCH_EU(TIST.TRANSPORT_INST_SEA_PK,");
                sb1.Append("                                                        " + BizType + ",");
                sb1.Append("                                                        " + Process + ")");
                sb1.Append("                                          FROM DUAL),");
                sb1.Append("                                        JOBFRT.FRTPAYER_CUST_MST_FK,");
                sb1.Append("                                        " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb1.Append("                                        JOBFRT.FREIGHT_TYPE,");
                sb1.Append("                                        JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                sb1.Append("                                        2)");
                sb1.Append("                         FROM DUAL) * (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) / 100)) +");
                sb1.Append("                JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) TOTAL_AMOUNT,");
                sb1.Append("                '' AS REMARKS,");
                sb1.Append("                'NEW' AS \"MODE1\",");
                sb1.Append("                'FALSE' AS CHK,");
                sb1.Append("                PAR.FRT_BOF_FK,");
                sb1.Append("                FMT.PREFERENCE, '3' JOB_TYPE");
                sb1.Append("  FROM TRANSPORT_TRN_FD        JOBFRT,");
                sb1.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb1.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                sb1.Append("       CORPORATE_MST_TBL       CORP,");
                sb1.Append("       TRANSPORT_INST_SEA_TBL  TIST,");
                sb1.Append("       CONTAINER_TYPE_MST_TBL  CON,");
                sb1.Append("       PARAMETERS_TBL          PAR");
                sb1.Append(" WHERE JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb1.Append("   AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb1.Append("   AND TIST.TRANSPORT_INST_SEA_PK = JOBFRT.TRANSPORT_INST_FK");
                sb1.Append("   AND (JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
                sb1.Append("       JOBFRT.CONTAINER_TYPE_MST_FK IS NULL)");
                sb1.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                sb1.Append("   AND JOBFRT.TRANSPORT_INST_FK IN (" + strTPTPks + ")");
                sb1.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL");
                sb1.Append("   AND JOBFRT.FREIGHT_TYPE = " + frtType);
                sb1.Append("   AND JOBFRT.FREIGHT_AMT > 0");
                sb1.Append("   AND JOBFRT.FRTPAYER_CUST_MST_FK IN (" + CustPk + ")");

                StringBuilder sb2 = new StringBuilder(5000);
                sb2.Append("SELECT DISTINCT TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK,");
                sb2.Append("                DECODE(TRN.FRT_OTH_ELEMENT_FK,");
                sb2.Append("                       1,");
                sb2.Append("                       'COST',");
                sb2.Append("                       2,");
                sb2.Append("                       'FREIGHT',");
                sb2.Append("                       3,");
                sb2.Append("                       'OTHER') AS TYPE,");
                sb2.Append("                JOB.CBJC_NO,");
                sb2.Append("                (CASE");
                sb2.Append("                  WHEN JOBFRT.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
                sb2.Append("                   CON.CONTAINER_TYPE_MST_ID");
                sb2.Append("                  ELSE");
                sb2.Append("                   DECODE(JOBFRT.BASIS_FK,");
                sb2.Append("                          1,");
                sb2.Append("                          'Unit',");
                sb2.Append("                          2,");
                sb2.Append("                          'Flat Rate',");
                sb2.Append("                          3,");
                sb2.Append("                          'Kgs',");
                sb2.Append("                          4,");
                sb2.Append("                          'MT',");
                sb2.Append("                          5,");
                sb2.Append("                          'CBM')");
                sb2.Append("                END) UNIT,");
                sb2.Append("                JOBFRT.CBJC_TRN_FD_PK AS PK,");
                sb2.Append("                TRN.JOB_CARD_FK AS JOBCARD_FK,");
                sb2.Append("                '0' FREIGHT_OR_OTH,");
                sb2.Append("                TRN.CURRENCY_MST_FK,");
                sb2.Append("                TRN.FRT_DESC AS ELEMENT_NAME,");
                sb2.Append("                '' AS ELEMENT,");
                sb2.Append("                CUMT.CURRENCY_ID,");
                sb2.Append("                '' AS CURR,");
                sb2.Append("                TRN.ELEMENT_AMT AS AMOUNT,");
                sb2.Append("                ROUND((CASE TRN.ELEMENT_AMT");
                sb2.Append("                        WHEN 0 THEN");
                sb2.Append("                         1");
                sb2.Append("                        ELSE");
                sb2.Append("                         TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT");
                sb2.Append("                      END),");
                sb2.Append("                      6) AS EXCHANGE_RATE,");
                sb2.Append("                TRN.AMT_IN_INV_CURR AS INV_AMOUNT,");
                sb2.Append("                (CASE");
                sb2.Append("                  WHEN TRN.VAT_CODE = '0' THEN");
                sb2.Append("                   ''");
                sb2.Append("                  ELSE");
                sb2.Append("                   TRN.VAT_CODE");
                sb2.Append("                END) VAT_CODE,");
                sb2.Append("                TRN.TAX_PCNT AS VAT_PERCENT,");
                sb2.Append("                TRN.TAX_AMT AS TAX_AMOUNT,");
                sb2.Append("                TRN.TOT_AMT AS TOTAL_AMOUNT,");
                sb2.Append("                TRN.REMARKS,");
                sb2.Append("                'EDIT' AS \"MODE1\",");
                sb2.Append("                'TRUE' AS CHK,");
                sb2.Append("                PAR.FRT_BOF_FK,");
                sb2.Append("                FMT.PREFERENCE,");
                sb2.Append("                '2' JOB_TYPE");
                sb2.Append("  FROM CONSOL_INVOICE_TRN_TBL  TRN,");
                sb2.Append("       CONSOL_INVOICE_TBL      HDR,");
                sb2.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                sb2.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb2.Append("       CBJC_TRN_FD             JOBFRT,");
                sb2.Append("       CONTAINER_TYPE_MST_TBL  CON,");
                sb2.Append("       PARAMETERS_TBL          PAR,");
                sb2.Append("       CBJC_TBL                JOB");
                sb2.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK");
                sb2.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb2.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                //sb2.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK = TRN.CONSOL_INVOICE_TRN_PK")
                sb2.Append("   AND JOB.CBJC_PK = TRN.JOB_CARD_FK");
                sb2.Append("   AND TRN.JOB_CARD_FK = JOBFRT.CBJC_FK");
                sb2.Append("   AND (JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
                sb2.Append("       JOBFRT.CONTAINER_TYPE_MST_FK IS NULL)");
                sb2.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK");
                sb2.Append("   AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+)");
                sb2.Append("   AND HDR.CONSOL_INVOICE_PK = " + intInvPk);

                StringBuilder sb3 = new StringBuilder(5000);
                sb3.Append("SELECT DISTINCT TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK,");
                sb3.Append("                DECODE(TRN.FRT_OTH_ELEMENT_FK,");
                sb3.Append("                       1,");
                sb3.Append("                       'COST',");
                sb3.Append("                       2,");
                sb3.Append("                       'FREIGHT',");
                sb3.Append("                       3,");
                sb3.Append("                       'OTHER') AS TYPE,");
                sb3.Append("                JOB.TRANS_INST_REF_NO,");
                sb3.Append("                (CASE");
                sb3.Append("                  WHEN JOBFRT.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
                sb3.Append("                   CON.CONTAINER_TYPE_MST_ID");
                sb3.Append("                  ELSE");
                sb3.Append("                   DECODE(JOBFRT.BASIS_FK,");
                sb3.Append("                          1,");
                sb3.Append("                          'Unit',");
                sb3.Append("                          2,");
                sb3.Append("                          'Flat Rate',");
                sb3.Append("                          3,");
                sb3.Append("                          'Kgs',");
                sb3.Append("                          4,");
                sb3.Append("                          'MT',");
                sb3.Append("                          5,");
                sb3.Append("                          'CBM')");
                sb3.Append("                END) UNIT,");
                sb3.Append("                JOBFRT.TRANSPORT_TRN_FD_PK AS PK,");
                sb3.Append("                TRN.JOB_CARD_FK AS JOBCARD_FK,");
                sb3.Append("                '0' FREIGHT_OR_OTH,");
                sb3.Append("                TRN.CURRENCY_MST_FK,");
                sb3.Append("                TRN.FRT_DESC AS ELEMENT_NAME,");
                sb3.Append("                '' AS ELEMENT,");
                sb3.Append("                CUMT.CURRENCY_ID,");
                sb3.Append("                '' AS CURR,");
                sb3.Append("                TRN.ELEMENT_AMT AS AMOUNT,");
                sb3.Append("                ROUND((CASE TRN.ELEMENT_AMT");
                sb3.Append("                        WHEN 0 THEN");
                sb3.Append("                         1");
                sb3.Append("                        ELSE");
                sb3.Append("                         TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT");
                sb3.Append("                      END),");
                sb3.Append("                      6) AS EXCHANGE_RATE,");
                sb3.Append("                TRN.AMT_IN_INV_CURR AS INV_AMOUNT,");
                sb3.Append("                (CASE");
                sb3.Append("                  WHEN TRN.VAT_CODE = '0' THEN");
                sb3.Append("                   ''");
                sb3.Append("                  ELSE");
                sb3.Append("                   TRN.VAT_CODE");
                sb3.Append("                END) VAT_CODE,");
                sb3.Append("                TRN.TAX_PCNT AS VAT_PERCENT,");
                sb3.Append("                TRN.TAX_AMT AS TAX_AMOUNT,");
                sb3.Append("                TRN.TOT_AMT AS TOTAL_AMOUNT,");
                sb3.Append("                TRN.REMARKS,");
                sb3.Append("                'EDIT' AS \"MODE1\",");
                sb3.Append("                'TRUE' AS CHK,");
                sb3.Append("                PAR.FRT_BOF_FK,");
                sb3.Append("                FMT.PREFERENCE,");
                sb3.Append("                '2' JOB_TYPE");
                sb3.Append("  FROM CONSOL_INVOICE_TRN_TBL  TRN,");
                sb3.Append("       CONSOL_INVOICE_TBL      HDR,");
                sb3.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                sb3.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb3.Append("       TRANSPORT_TRN_FD        JOBFRT,");
                sb3.Append("       CONTAINER_TYPE_MST_TBL  CON,");
                sb3.Append("       PARAMETERS_TBL          PAR,");
                sb3.Append("       TRANSPORT_INST_SEA_TBL  JOB");
                sb3.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK");
                sb3.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb3.Append("   AND JOB.TRANSPORT_INST_SEA_PK = TRN.JOB_CARD_FK");
                sb3.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                //sb3.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK = TRN.CONSOL_INVOICE_TRN_PK")
                sb3.Append("   AND TRN.JOB_CARD_FK = JOBFRT.TRANSPORT_INST_FK");
                sb3.Append("   AND (JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
                sb3.Append("       JOBFRT.CONTAINER_TYPE_MST_FK IS NULL)");
                sb3.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK");
                sb3.Append("   AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+)");
                sb3.Append("   AND HDR.CONSOL_INVOICE_PK = " + intInvPk);

                StringBuilder sb4 = new StringBuilder(5000);
                sb4.Append("SELECT TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK,");
                sb4.Append("       'FREIGHT' AS TYPE,");
                sb4.Append("       J.DEM_CALC_ID JOBCARD_REF_NO,");
                sb4.Append("       '' UNIT,");
                sb4.Append("       TRN.CONSOL_INVOICE_TRN_PK PK,");
                sb4.Append("       J.DEM_CALC_HDR_PK JOBCARD_FK,");
                sb4.Append("       TO_CHAR(TRN.FRT_OTH_ELEMENT) FREIGHT_OR_OTH,");
                sb4.Append("       TRN.CURRENCY_MST_FK CURRENCY_MST_FK,");
                sb4.Append("       TRN.FRT_DESC ELEMENT_NAME,");
                sb4.Append("       '' AS ELEMENT,");
                sb4.Append("       CUR.CURRENCY_ID,");
                sb4.Append("       '' AS CURR,");
                sb4.Append("       TRN.ELEMENT_AMT AS AMOUNT,");
                sb4.Append("       ROUND((CASE TRN.ELEMENT_AMT");
                sb4.Append("               WHEN 0 THEN");
                sb4.Append("                1");
                sb4.Append("               ELSE");
                sb4.Append("                TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT");
                sb4.Append("             END),");
                sb4.Append("             6) AS EXCHANGE_RATE,");
                sb4.Append("       TRN.AMT_IN_INV_CURR AS INV_AMOUNT,");
                sb4.Append("       TRN.VAT_CODE AS VAT_CODE,");
                sb4.Append("       TRN.TAX_PCNT AS VAT_PERCENT,");
                sb4.Append("       TRN.TAX_AMT AS TAX_AMOUNT,");
                sb4.Append("       TRN.TOT_AMT AS TOTAL_AMOUNT,");
                sb4.Append("       TRN.REMARKS,");
                sb4.Append("       'EDIT' AS \"MODE\",");
                sb4.Append("       'TRUE' AS CHK,");
                sb4.Append("       0 FRT_BOF_FK,");
                sb4.Append("       FEMT.PREFERENCE,");
                sb4.Append("       TO_CHAR(TRN.JOB_TYPE) JOB_TYPE ");
                sb4.Append("  FROM CONSOL_INVOICE_TBL      MAS,");
                sb4.Append("       CONSOL_INVOICE_TRN_TBL  TRN,");
                sb4.Append("       DEM_CALC_HDR            J,");
                sb4.Append("       CURRENCY_TYPE_MST_TBL   CUR,");
                sb4.Append("       FREIGHT_ELEMENT_MST_TBL FEMT       ");
                sb4.Append(" WHERE CUR.CURRENCY_MST_PK = TRN.CURRENCY_MST_FK");
                sb4.Append("   AND MAS.CONSOL_INVOICE_PK = " + intInvPk);
                sb4.Append("   AND TRN.CONSOL_INVOICE_FK = MAS.CONSOL_INVOICE_PK");
                sb4.Append("   AND TRN.JOB_CARD_FK = J.DEM_CALC_HDR_PK");
                sb4.Append("   AND FEMT.FREIGHT_ELEMENT_NAME = TRN.FRT_DESC");

                if (intInvPk == 0)
                {
                    strQuery.Append("SELECT Q.* FROM ( ");
                    strQuery.Append(" select type,jobcard_ref_no,unit,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id,curr, ");
                    strQuery.Append("round(amount,2) amount,exchange_rate,round(inv_amount,2) inv_amount,vat_code,vat_percent, round(abs(tax_amount),2) tax_amount,round(total_amount,2) total_amount, ");
                    strQuery.Append("remarks,mode1 as \"MODE\",chk,FRT_BOF_FK, JOB_TYPE from (");
                    strQuery.Append(" select type,jobcard_ref_no,unit,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id, ");
                    strQuery.Append("curr,amount,exchange_rate,inv_amount,vat_code,vat_percent, abs(tax_amount)tax_amount,total_amount,remarks,mode1 as \"MODE1\",chk,FRT_BOF_FK, preference, JOB_TYPE from ( ");

                    if (BizType == 1)
                    {
                        strQuery.Append(" SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,'FREIGHT' AS TYPE," );
                    }
                    else
                    {
                        strQuery.Append(" SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,'FREIGHT' AS TYPE," );
                    }
                    strQuery.Append(" JOB.JOBCARD_REF_NO,");
                    if (BizType == 1)
                    {
                        strQuery.Append(" (CASE WHEN NVL(JOBFRT.SERVICE_TYPE_FLAG,0)=1 THEN ");
                        strQuery.Append("    DECODE(JOBFRT.BASIS_FK,1,'Unit',2,'Flat Rate',3,'Kgs',4,'MT',5,'CBM') ELSE ");
                        strQuery.Append(" CON.CONTAINER_TYPE_MST_ID END) UNIT," );
                    }
                    else
                    {
                        strQuery.Append(" (CASE WHEN NVL(JOBFRT.SERVICE_TYPE_FLAG,0)=1 THEN ");
                        strQuery.Append("    DECODE(JOBFRT.BASIS_FK,1,'Unit',2,'Flat Rate',3,'Kgs',4,'MT',5,'CBM') ELSE ");
                        strQuery.Append(" (case when jobfrt.container_type_mst_fk is not null then  con.container_type_mst_id  else '' end) END) UNIT, ");
                    }

                    strQuery.Append("       JOBFRT.JOB_TRN_FD_PK AS PK," );
                    strQuery.Append("       JOBFRT.JOB_CARD_TRN_FK AS JOBCARD_FK," );
                    strQuery.Append("       '1' FREIGHT_OR_OTH," );
                    strQuery.Append("       JOBFRT.CURRENCY_MST_FK," );
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME," );
                    strQuery.Append("       '' AS ELEMENT," );
                    strQuery.Append("       CUMT.CURRENCY_ID," );
                    strQuery.Append("       '' AS CURR," );
                    strQuery.Append("       JOBFRT.FREIGHT_AMT AS AMOUNT," );
                    strQuery.Append("       GET_EX_RATE(JOBFRT.CURRENCY_MST_FK, " + nBaseCurrPK + ",TO_DATE(JOB.JOBCARD_DATE,'DD/MM/YYYY')) EXCHANGE_RATE,");
                    strQuery.Append("       JOBFRT.FREIGHT_AMT * GET_EX_RATE(JOBFRT.CURRENCY_MST_FK, " + nBaseCurrPK + ",TO_DATE(JOB.JOBCARD_DATE,'DD/MM/YYYY')) AS INV_AMOUNT," );

                    strQuery.Append("(select FETCH_VAT((select FETCH_EU(JOB.JOB_CARD_TRN_PK," + BizType + "," + Process + " ) from dual )" + ",JOBFRT.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,1) from dual) VAT_CODE,");

                    strQuery.Append("(select FETCH_VAT ((select FETCH_EU(JOB.JOB_CARD_TRN_PK," + BizType + "," + Process + " ) from dual )" + ",JOBFRT.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2) from dual) VAT_PERCENT,");

                    strQuery.Append("((select FETCH_VAT ((select FETCH_EU(JOB.JOB_CARD_TRN_PK," + BizType + "," + Process + " ) from dual )" + ",JOBFRT.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) TAX_AMOUNT,");

                    strQuery.Append("((abs((select FETCH_VAT ((select FETCH_EU(JOB.JOB_CARD_TRN_PK," + BizType + "," + Process + " ) from dual )" + ",JOBFRT.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100)) + JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) TOTAL_AMOUNT,");

                    strQuery.Append("       '' AS REMARKS," );
                    strQuery.Append("       'NEW' AS \"MODE1\"," );
                    strQuery.Append("       'FALSE' AS CHK,PAR.FRT_BOF_FK, fmt.preference, '1' JOB_TYPE" );
                    strQuery.Append("  FROM JOB_TRN_FD      JOBFRT," );
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FMT," );
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   CUMT," );
                    strQuery.Append("       CORPORATE_MST_TBL       CORP," );
                    strQuery.Append("        JOB_CARD_TRN JOB,CONTAINER_TYPE_MST_TBL  CON,PARAMETERS_TBL   PAR" );
                    strQuery.Append("        WHERE JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK" );
                    strQuery.Append("        AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK" );
                    if (BizType == 1)
                    {
                        strQuery.Append("  AND JOB.JOB_CARD_TRN_PK=JOBFRT.JOB_CARD_TRN_FK AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK" );
                    }
                    else
                    {
                        strQuery.Append("  AND JOB.JOB_CARD_TRN_PK=JOBFRT.JOB_CARD_TRN_FK AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) " );
                    }
                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) AND JOBFRT.JOB_CARD_TRN_FK IN (" + strJobPks + ")");
                    strQuery.Append("   AND JOBFRT.INVOICE_TBL_FK IS NULL" );
                    strQuery.Append("   AND (JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL " );
                    strQuery.Append("   OR (SELECT COUNT(*) " );
                    strQuery.Append("   FROM CONSOL_INVOICE_TRN_TBL CT, CONSOL_INVOICE_TBL CIT " );
                    strQuery.Append("   WHERE CT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK " );
                    strQuery.Append("   AND CT.CONSOL_INVOICE_TRN_PK = JOBFRT.CONSOL_INVOICE_TRN_FK " );
                    strQuery.Append("   AND CIT.CHK_INVOICE <> 2) = 0) ");
                    strQuery.Append("   AND JOBFRT.FREIGHT_TYPE = " + frtType );

                    strQuery.Append("   AND JOBFRT.INV_AGENT_TRN_FK IS NULL" );
                    strQuery.Append("   AND JOBFRT.FREIGHT_AMT>0");
                    strQuery.Append("   AND JOBFRT.frtpayer_cust_mst_fk in ( " + CustPk + ")");
                    // added by jitendra on 22/05/07
                    strQuery.Append(" UNION");
                    strQuery.Append(" " + sb.ToString() + "");
                    strQuery.Append(" UNION");
                    strQuery.Append(" " + sb1.ToString() + "");
                    strQuery.Append(" )");
                    strQuery.Append("UNION" );

                    strQuery.Append(" SELECT 'OTHER' AS TYPE," );
                    strQuery.Append(" JOB.JOBCARD_REF_NO ,'Oth.Chrg' AS UNIT," );
                    strQuery.Append("       JOBOTH.JOB_TRN_OTH_PK AS PK," );
                    strQuery.Append("       JOBOTH.JOB_CARD_TRN_FK AS JOBCARD_FK," );
                    strQuery.Append("       '2' FREIGHT_OR_OTH," );
                    strQuery.Append("       JOBOTH.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK," );
                    strQuery.Append("       JOBOTH.CURRENCY_MST_FK," );
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME," );
                    strQuery.Append("       '' AS ELEMENT_SEARCH," );
                    strQuery.Append("       CUMT.CURRENCY_ID," );
                    strQuery.Append("       '' AS CURR_SEARCH," );
                    strQuery.Append("       JOBOTH.AMOUNT AS AMOUNT," );
                    strQuery.Append("       GET_EX_RATE(JOBOTH.CURRENCY_MST_FK, " + nBaseCurrPK + ",TO_DATE(JOB.JOBCARD_DATE,'DD/MM/YYYY')) EXCHANGE_RATE,");
                    strQuery.Append("       ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK, " + nBaseCurrPK + ",TO_DATE(JOB.JOBCARD_DATE,'DD/MM/YYYY')),2) AS INV_AMOUNT," );

                    strQuery.Append("(select FETCH_VAT ((select  FETCH_EU(JOB.JOB_CARD_TRN_PK," + BizType + "," + Process + " ) from dual )" + ",JOBOTH.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,1) from dual) VAT_CODE,");

                    strQuery.Append("(select FETCH_VAT ((select FETCH_EU(JOB.JOB_CARD_TRN_PK," + BizType + "," + Process + " ) from dual )" + ",JOBOTH.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2) from dual) VAT_PERCENT,");

                    strQuery.Append("((select FETCH_VAT((select FETCH_EU(JOB.JOB_CARD_TRN_PK," + BizType + "," + Process + " ) from dual )" + ",JOBOTH.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) TAX_AMOUNT,");

                    strQuery.Append("((abs((select FETCH_VAT((select FETCH_EU(JOB.JOB_CARD_TRN_PK," + BizType + "," + Process + " ) from dual )" + ",JOBOTH.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100)) + JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE) TOTAL_AMOUNT,");

                    strQuery.Append("       '' AS REMARKS," );
                    strQuery.Append("       'NEW' AS \"MODE\"," );
                    strQuery.Append("       'FALSE' AS CHK,PAR.FRT_BOF_FK,  fmt.preference, '1' JOB_TYPE " );
                    strQuery.Append("  FROM JOB_TRN_OTH_CHRG JOBOTH," );
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL  FMT," );
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL    CUMT," );
                    strQuery.Append("       CORPORATE_MST_TBL        CORP," );
                    strQuery.Append("       JOB_CARD_TRN JOB,PARAMETERS_TBL  PAR" );
                    strQuery.Append(" WHERE JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK" );
                    strQuery.Append("   AND JOBOTH.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK" );
                    strQuery.Append("   AND JOB.JOB_CARD_TRN_PK = JOBOTH.JOB_CARD_TRN_FK AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_AFC_FK(+)" );
                    strQuery.Append("   AND JOBOTH.JOB_CARD_TRN_FK IN (" + strJobPks + ")");
                    strQuery.Append("   AND JOBOTH.INV_CUST_TRN_FK IS NULL" );
                    strQuery.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL" );
                    strQuery.Append("   AND JOBOTH.FREIGHT_TYPE = " + frtType );
                    strQuery.Append("   AND JOBOTH.frtpayer_cust_mst_fk in (" + CustPk + ")");
                    strQuery.Append("   AND JOBOTH.AMOUNT>0");
                    //strQuery.Append(" ORDER BY unit, preference)) Q " & vbCrLf)

                    strQuery.Append(" UNION SELECT * FROM (SELECT 'FREIGHT' AS TYPE,");
                    strQuery.Append("       DCH.DEM_CALC_ID JOBCARD_REF_NO,");
                    strQuery.Append("       'DET' UNIT,");
                    strQuery.Append("       TO_NUMBER(NULL) AS PK,");
                    strQuery.Append("       DCH.DEM_CALC_HDR_PK AS JOBCARD_FK,");
                    strQuery.Append("       '1' FREIGHT_OR_OTH,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_MST_PK AS ELEMENT_FK,");
                    strQuery.Append("       CUMT.CURRENCY_MST_PK,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                    strQuery.Append("       '' AS ELEMENT_SEARCH,");
                    strQuery.Append("       CUMT.CURRENCY_ID,");
                    strQuery.Append("       '' AS CURR_SEARCH,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) AS AMOUNT,");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) EXCHANGE_RATE,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) * ");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) AS INV_AMOUNT,");
                    strQuery.Append("       TO_CHAR(NULL) VAT_CODE,");
                    strQuery.Append("       TO_CHAR(NULL) VAT_PERCENT,");
                    strQuery.Append("       TO_NUMBER(NULL) TAX_AMOUNT,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) *");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) TOTAL_AMOUNT,");
                    strQuery.Append("       '' AS REMARKS,");
                    strQuery.Append("       'NEW' AS \"MODE\",");
                    strQuery.Append("       'FALSE' AS CHK,");
                    strQuery.Append("       PAR.FRT_BOF_FK,");
                    strQuery.Append("       FMT.PREFERENCE,");
                    strQuery.Append("       '4' JOB_TYPE");
                    strQuery.Append("  FROM DEM_CALC_HDR            DCH,");

                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                    strQuery.Append("       PARAMETERS_TBL          PAR");
                    strQuery.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK");

                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DET'");
                    strQuery.Append("   AND FMT.ACTIVE_FLAG = 1");
                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                    strQuery.Append("   AND DCH.DET_INVOICE_TRN_FK IS NULL ");
                    strQuery.Append("   AND DCH.DEM_CALC_HDR_PK = (" + strDemPKList + ")");

                    strQuery.Append(" UNION SELECT 'FREIGHT' AS TYPE,");
                    strQuery.Append("       DCH.DEM_CALC_ID JOBCARD_REF_NO,");
                    strQuery.Append("       'DEM' UNIT,");
                    strQuery.Append("       TO_NUMBER(NULL) AS PK,");
                    strQuery.Append("       DCH.DEM_CALC_HDR_PK AS JOBCARD_FK,");
                    strQuery.Append("       '1' FREIGHT_OR_OTH,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_MST_PK AS ELEMENT_FK,");
                    strQuery.Append("       CUMT.CURRENCY_MST_PK,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                    strQuery.Append("       '' AS ELEMENT_SEARCH,");
                    strQuery.Append("       CUMT.CURRENCY_ID,");
                    strQuery.Append("       '' AS CURR_SEARCH,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) AS AMOUNT,");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) EXCHANGE_RATE,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) * ");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) AS INV_AMOUNT,");
                    strQuery.Append("       TO_CHAR(NULL) VAT_CODE,");
                    strQuery.Append("       TO_CHAR(NULL) VAT_PERCENT,");
                    strQuery.Append("       TO_NUMBER(NULL) TAX_AMOUNT,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) *");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) TOTAL_AMOUNT,");
                    strQuery.Append("       '' AS REMARKS,");
                    strQuery.Append("       'NEW' AS \"MODE\",");
                    strQuery.Append("       'FALSE' AS CHK,");
                    strQuery.Append("       PAR.FRT_BOF_FK,");
                    strQuery.Append("       FMT.PREFERENCE,");
                    strQuery.Append("       '4' JOB_TYPE");
                    strQuery.Append("  FROM DEM_CALC_HDR            DCH,");

                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                    strQuery.Append("       PARAMETERS_TBL          PAR");
                    strQuery.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK");

                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DEM'");
                    strQuery.Append("   AND FMT.ACTIVE_FLAG = 1");
                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                    strQuery.Append("   AND DCH.DEM_INVOICE_TRN_FK IS NULL ");
                    strQuery.Append("   AND DCH.DEM_CALC_HDR_PK = (" + strDemPKList + ")) WHERE AMOUNT > 0 ");

                    strQuery.Append(" ) ORDER BY preference, unit) Q " );

                }
                else
                {
                    strQuery.Append(" select Q.* from ( ");
                    strQuery.Append(" SELECT TYPE, JOBCARD_REF_NO, UNIT, PK, JOBCARD_FK, FREIGHT_OR_OTH, ELEMENT_FK, CURRENCY_MST_FK, ELEMENT_NAME,ELEMENT, CURRENCY_ID, CURR, ");
                    strQuery.Append("round(amount,2) amount,exchange_rate,round(inv_amount,2) inv_amount,vat_code,vat_percent, round(abs(tax_amount),2) tax_amount,round(total_amount,2) total_amount, ");
                    strQuery.Append(" REMARKS, MODE1 AS \"MODE\", CHK, FRT_BOF_FK,JOB_TYPE FROM ( ");
                    strQuery.Append(" select type,jobcard_ref_no,unit,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id, ");
                    strQuery.Append(" curr,amount,exchange_rate,inv_amount,vat_code,VAT_PERCENT vat_percent,tax_amount,total_amount,remarks,mode1 as \"MODE1\",chk,FRT_BOF_FK, PREFERENCE,JOB_TYPE from ( ");

                    strQuery.Append(" SELECT  distinct TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK, DECODE(TRN.FRT_OTH_ELEMENT_FK, 1, 'COST', 2, 'FREIGHT', 3, 'OTHER') AS TYPE," );
                    strQuery.Append("     JOB.JOBCARD_REF_NO," );

                    if (BizType == 1)
                    {
                        strQuery.Append(" (CASE WHEN NVL(JOBFRT.SERVICE_TYPE_FLAG,0)=1 THEN ");
                        strQuery.Append("    DECODE(JOBFRT.BASIS_FK,1,'Unit',2,'Flat Rate',3,'Kgs',4,'MT',5,'CBM') ELSE ");
                        strQuery.Append(" CON.CONTAINER_TYPE_MST_ID END) UNIT," );
                    }
                    else
                    {
                        strQuery.Append(" (CASE WHEN NVL(JOBFRT.SERVICE_TYPE_FLAG,0)=1 THEN ");
                        strQuery.Append("    DECODE(JOBFRT.BASIS_FK,1,'Unit',2,'Flat Rate',3,'Kgs',4,'MT',5,'CBM') ELSE ");
                        strQuery.Append(" (case when jobfrt.container_type_mst_fk is not null then  con.container_type_mst_id  else '' end) END) UNIT, ");
                    }
                    strQuery.Append(" JOBFRT.JOB_TRN_FD_PK AS PK," );
                    strQuery.Append("       TRN.JOB_CARD_FK AS JOBCARD_FK," );
                    strQuery.Append("       '0' FREIGHT_OR_OTH," );
                    strQuery.Append("       TRN.CURRENCY_MST_FK," );
                    strQuery.Append("    trn.frt_desc AS ELEMENT_NAME," );
                    strQuery.Append("       '' AS ELEMENT," );
                    strQuery.Append("       CUMT.CURRENCY_ID," );
                    strQuery.Append("       '' AS CURR," );
                    strQuery.Append("       TRN.ELEMENT_AMT AS AMOUNT," );
                    strQuery.Append("       ROUND((CASE TRN.ELEMENT_AMT" );
                    strQuery.Append("               WHEN 0 THEN" );
                    strQuery.Append("1" );
                    strQuery.Append("               ELSE" );
                    strQuery.Append("                TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT" );
                    strQuery.Append("             END)," );
                    strQuery.Append("             6) AS EXCHANGE_RATE," );
                    strQuery.Append("       TRN.AMT_IN_INV_CURR AS INV_AMOUNT," );
                    strQuery.Append("  (CASE " );
                    strQuery.Append("  WHEN TRN.VAT_CODE = '0' THEN " );
                    strQuery.Append("    '' " );
                    strQuery.Append("   ELSE" );
                    strQuery.Append("   TRN.VAT_CODE " );
                    strQuery.Append("  END) VAT_CODE," );
                    //'
                    strQuery.Append("       TRN.TAX_PCNT AS VAT_PERCENT," );
                    strQuery.Append("       TRN.TAX_AMT AS TAX_AMOUNT," );
                    strQuery.Append("       TRN.TOT_AMT AS TOTAL_AMOUNT," );
                    strQuery.Append("       TRN.REMARKS," );
                    strQuery.Append("       'EDIT' AS \"MODE1\"," );
                    strQuery.Append("       'TRUE' AS CHK,PAR.FRT_BOF_FK, FMT.PREFERENCE, '1' JOB_TYPE " );
                    strQuery.Append("  FROM CONSOL_INVOICE_TRN_TBL TRN," );
                    strQuery.Append("       CONSOL_INVOICE_TBL     HDR," );
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT,freight_element_mst_tbl fmt,JOB_TRN_FD JOBFRT,CONTAINER_TYPE_MST_TBL CON,PARAMETERS_TBL PAR, " );
                    strQuery.Append("        JOB_CARD_TRN JOB" );
                    strQuery.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK" );
                    strQuery.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK" );
                    strQuery.Append("   AND JOB.JOB_CARD_TRN_PK=TRN.JOB_CARD_FK" );

                    strQuery.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK and jobfrt.consol_invoice_trn_fk = trn.consol_invoice_trn_pk " );
                    strQuery.Append(" AND TRN.JOB_CARD_FK  = JOBFRT.JOB_CARD_TRN_FK " );

                    if (BizType == 1)
                    {
                        strQuery.Append(" AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK " );
                    }
                    else
                    {
                        strQuery.Append("  AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) ");
                    }

                    if (rowcunt <= 0)
                    {
                        strQuery.Append(" AND JOBFRT.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK " );
                    }
                    else
                    {
                        if (Contpk > 0)
                        {
                            strQuery.Append("  and jobfrt.JOB_TRN_FD_PK in ( " + Contpk + " )");
                        }
                    }

                    strQuery.Append(" AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+) " );

                    strQuery.Append("  AND HDR.CONSOL_INVOICE_PK = " + intInvPk + "  ");
                    strQuery.Append(" UNION");
                    strQuery.Append(" " + sb2.ToString() + "");
                    strQuery.Append(" UNION");
                    strQuery.Append(" " + sb3.ToString() + "");
                    strQuery.Append(" UNION");
                    strQuery.Append(" " + sb4.ToString() + "");
                    strQuery.Append(" )");
                    strQuery.Append(" UNION ");

                    strQuery.Append("  SELECT DECODE(TRN.FRT_OTH_ELEMENT_FK, 1, 'COST', 2, 'FREIGHT', 3, 'OTHER') AS TYPE," );
                    strQuery.Append("       JOB.JOBCARD_REF_NO," );
                    strQuery.Append("       'Oth.Chrg' AS UNIT, JOBoth.Job_Trn_Oth_Pk AS PK," );
                    strQuery.Append("       TRN.JOB_CARD_FK AS JOBCARD_FK," );
                    strQuery.Append("      upper(TRN.FRT_OTH_ELEMENT),");
                    strQuery.Append("       TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK," );
                    strQuery.Append("       TRN.CURRENCY_MST_FK," );

                    strQuery.Append("      trn.frt_desc AS ELEMENT_NAME," );
                    //Added By jitendra 
                    strQuery.Append("       '' AS ELEMENT," );
                    strQuery.Append("       CUMT.CURRENCY_ID," );
                    strQuery.Append("       '' AS CURR," );
                    strQuery.Append("       TRN.ELEMENT_AMT AS AMOUNT," );
                    strQuery.Append("       ROUND((CASE TRN.ELEMENT_AMT" );
                    strQuery.Append("               WHEN 0 THEN" );
                    strQuery.Append("1" );
                    strQuery.Append("               ELSE" );
                    strQuery.Append("                TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT" );
                    strQuery.Append("             END)," );
                    strQuery.Append("             6) AS EXCHANGE_RATE," );
                    strQuery.Append("       TRN.AMT_IN_INV_CURR AS INV_AMOUNT," );
                    strQuery.Append("       TRN.VAT_CODE AS VAT_CODE," );
                    //Added by Venkata 
                    strQuery.Append("       TRN.TAX_PCNT AS VAT_PERCENT," );
                    strQuery.Append("       TRN.TAX_AMT AS TAX_AMOUNT," );
                    strQuery.Append("       TRN.TOT_AMT AS TOTAL_AMOUNT," );
                    strQuery.Append("       TRN.REMARKS," );
                    strQuery.Append("       'EDIT' AS \"MODE1\"," );
                    strQuery.Append("       'TRUE' AS CHK,PAR.FRT_BOF_FK, FMT.PREFERENCE, '1' JOB_TYPE " );
                    strQuery.Append("  FROM CONSOL_INVOICE_TRN_TBL TRN," );
                    strQuery.Append("       CONSOL_INVOICE_TBL     HDR," );
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT, FREIGHT_ELEMENT_MST_TBL  FMT, JOB_TRN_OTH_CHRG JOBoth,PARAMETERS_TBL PAR, " );
                    strQuery.Append("        JOB_CARD_TRN JOB" );
                    strQuery.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK" );
                    strQuery.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK" );
                    strQuery.Append("   AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK " );
                    strQuery.Append("   AND JOB.JOB_CARD_TRN_PK=TRN.JOB_CARD_FK" );
                    strQuery.Append(" AND TRN.JOB_CARD_FK  = JOBoth.JOB_CARD_TRN_FK " );
                    if (rowcunt <= 0)
                    {
                        strQuery.Append(" AND JOBoth.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK " );
                    }
                    else
                    {
                        if (Contpk > 0)
                        {
                            strQuery.Append("  and JOBoth.JOB_TRN_OTH_PK in (" + Contpk + " ) ");
                        }
                    }
                    //end
                    strQuery.Append(" AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+) " );
                    strQuery.Append("   AND HDR.CONSOL_INVOICE_PK = " + intInvPk + "");
                    strQuery.Append(" ) ORDER BY PREFERENCE,unit ) Q ");
                }
                if (BizType == 1)
                {
                    strQuery.Replace("CON.CONTAINER_TYPE_MST_ID", "upper(JOBFRT.QUANTITY)");
                    strQuery.Replace("FRT_BOF_FK", "FRT_AFC_FK");
                    strQuery.Replace("CONTAINER_TYPE_MST_TBL CON,", " ");
                    strQuery.Replace("AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK", " ");
                }
                //If BizType = 1 And Process = 2 Then 'Air import
                //    strQuery.Replace("SEA", "AIR")
                //    strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_IMP_FK")
                //    strQuery.Replace("EXP", "IMP")
                //    strQuery.Replace("CON.CONTAINER_TYPE_MST_ID", "upper(JOBFRT.QUANTITY)")
                //    strQuery.Replace("FRT_BOF_FK", "FRT_AFC_FK")
                //    strQuery.Replace("CONTAINER_TYPE_MST_TBL CON,", " ")
                //    strQuery.Replace("AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK", " ")
                //ElseIf BizType = 1 And Process = 1 Then  'air export
                //    strQuery.Replace("SEA", "AIR")
                //    strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_EXP_FK")
                //    strQuery.Replace("IMP", "EXP")
                //    strQuery.Replace("CON.CONTAINER_TYPE_MST_ID", "upper(JOBFRT.QUANTITY)")
                //    strQuery.Replace("FRT_BOF_FK", "FRT_AFC_FK")
                //    strQuery.Replace("CONTAINER_TYPE_MST_TBL CON,", " ")
                //    strQuery.Replace("AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK", " ")
                //ElseIf BizType = 2 And Process = 2 Then ' ImportSea
                //    strQuery.Replace("EXP", "IMP")
                //End If
                strQuery.Replace("TRANSPORT_INST_AIR_TBL", "TRANSPORT_INST_SEA_TBL");
                strQuery.Replace("TIST.TRANSPORT_INST_AIR_PK", "TIST.TRANSPORT_INST_SEA_PK");
                strQuery.Replace("JOB.TRANSPORT_INST_AIR_PK", "JOB.TRANSPORT_INST_SEA_PK");
                DS = objWK.GetDataSet(strQuery.ToString());
                return DS;
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
        //adding by thiyagarajan on 3/1/09 as "no.of palattes" col.introduced in Detention form :VEK Gap analysis
        public Int32 FetchDetFrt(int invPk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strBuilder = new StringBuilder();
            try
            {
                strBuilder.Append(" select count(*) from consol_invoice_trn_tbl contran,parameters_tbl pmt ");
                strBuilder.Append(" where contran.consol_invoice_fk=" + invPk);
                strBuilder.Append(" and contran.frt_oth_element_fk=pmt.frt_det_charge_fk");
                return Convert.ToInt32(objWK.ExecuteScaler(strBuilder.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //modifying by thiyagarajan on 3/1/09 as "no.of palattes" col.introduced in Detention form :VEK Gap analysis
        public Int32 FetchDetContPk(int Jobpk, Int32 invPk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strBuilder = new StringBuilder();
            DataSet dspk = null;
            Int32 pk = default(Int32);
            Int32 i = default(Int32);
            string strpk = "";
            try
            {
                //Changed By Snigdharani - 31/12/2009 - Detention Charges will be stored only in Other Chrges table
                strBuilder.Append(" SELECT max(oth.JOB_TRN_OTH_PK) FROM JOB_TRN_OTH_CHRG OTH WHERE  ");
                strBuilder.Append(" oth.JOB_CARD_TRN_FK in  (" + Jobpk + ")");
                //strBuilder.Append(" SELECT MAX(FD.JOB_TRN_SEA_IMP_FD_PK) FROM JOB_TRN_FD FD WHERE  ")
                //strBuilder.Append(" FD.JOB_CARD_TRN_FK IN (" & Jobpk & ")")
                pk = Convert.ToInt32(getDefault(objWK.ExecuteScaler(strBuilder.ToString()), 0));
                if (pk == 0)
                {
                    strBuilder.Remove(0, strBuilder.Length);
                    strBuilder.Append(" SELECT MAX(FD.JOB_TRN_FD_PK) FROM JOB_TRN_FD FD WHERE  ");
                    strBuilder.Append(" FD.JOB_CARD_TRN_FK IN (" + Jobpk + ")");
                    //strBuilder.Append(" SELECT max(oth.job_trn_sea_imp_oth_pk) FROM JOB_TRN_OTH_CHRG OTH WHERE  ")
                    //strBuilder.Append(" oth.job_card_sea_imp_fk in  (" & Jobpk & ")")
                    pk = Convert.ToInt32(getDefault(objWK.ExecuteScaler(strBuilder.ToString()), 0));
                }
                return pk;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //end
        public DataSet FetchHeader(int invPk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append(" select cm.customer_name cust, ct.currency_id Cur,");
            strBuilder.Append(" ci.invoice_ref_no REFNO, ");
            strBuilder.Append(" ci.invoice_date CDATE, ");
            strBuilder.Append(" ci.INVOICE_DUE_DATE, ");
            strBuilder.Append(" cm.credit_customer CRCUS, ");
            strBuilder.Append(" cm.credit_days CRDAYS, ");
            strBuilder.Append(" ci.invoice_amt INVAMT,");
            strBuilder.Append(" ci.discount_amt DISAMT, ");
            strBuilder.Append(" ci.net_receivable NET,ci.INV_UNIQUE_REF_NR, ");
            //Snigdharani - 08/01/2009 - Unique Invoice Reference Int32(VEK)
            strBuilder.Append(" ci.remarks,ci.tds_remarks,ci.aif,");
            strBuilder.Append(" ci.AUTO_MANUAL_INV,");
            strBuilder.Append(" BMT.BANK_MST_PK, BMT.BANK_ID, BMT.BANK_NAME ,");
            strBuilder.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
            strBuilder.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            strBuilder.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            strBuilder.Append("   TO_DATE(CI.CREATED_DT) CREATED_DT, ");
            strBuilder.Append("   TO_DATE(CI.LAST_MODIFIED_DT) LAST_MODIFIED_DT,ci.last_modified_by_fk,ci.chk_invoice,ci.created_by_fk ,");
            strBuilder.Append("   TO_DATE(CI.LAST_MODIFIED_DT) APPROVED_DT ");
            strBuilder.Append(" from consol_invoice_tbl ci,customer_mst_tbl cm,currency_type_mst_tbl ct, BANK_MST_TBL BMT, ");
            strBuilder.Append("  USER_MST_TBL UMTCRT, ");
            strBuilder.Append("  USER_MST_TBL UMTUPD, ");
            strBuilder.Append("  USER_MST_TBL UMTAPP ");
            strBuilder.Append(" where ci.consol_invoice_pk = " + invPk + "");
            strBuilder.Append(" and cm.customer_mst_pk = ci.customer_mst_fk and");
            strBuilder.Append(" ct.currency_mst_pk = ci.currency_mst_fk");
            strBuilder.Append(" AND BMT.BANK_MST_PK(+) = CI.BANK_MST_FK");
            strBuilder.Append(" AND UMTCRT.USER_MST_PK(+) = CI.CREATED_BY_FK ");
            strBuilder.Append(" AND UMTUPD.USER_MST_PK(+) = CI.LAST_MODIFIED_BY_FK  ");
            strBuilder.Append(" AND UMTAPP.USER_MST_PK(+) = CI.LAST_MODIFIED_BY_FK  ");
            try
            {
                return objWK.GetDataSet(strBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchHeaderFAC(int invPk, string BizType = "")
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append(" SELECT ");
            if (BizType == "2")
            {
                strBuilder.Append(" OPR.OPERATOR_NAME CUST,");
            }
            else
            {
                strBuilder.Append(" AMT.AIRLINE_NAME CUST,");
            }
            strBuilder.Append(" ct.currency_id Cur,");
            strBuilder.Append(" ci.invoice_ref_no REFNO, ");
            strBuilder.Append(" ci.invoice_date CDATE, ");
            strBuilder.Append(" ci.INVOICE_DUE_DATE, ");
            strBuilder.Append(" 0 CRCUS, ");
            strBuilder.Append(" 0 CRDAYS, ");
            strBuilder.Append(" ci.invoice_amt INVAMT,");
            strBuilder.Append(" ci.discount_amt DISAMT, ");
            strBuilder.Append(" ci.net_receivable NET,ci.INV_UNIQUE_REF_NR, ");
            //Snigdharani - 08/01/2009 - Unique Invoice Reference Int32(VEK)
            strBuilder.Append(" ci.remarks,ci.tds_remarks,ci.aif,");
            strBuilder.Append(" ci.AUTO_MANUAL_INV,");
            strBuilder.Append(" BMT.BANK_MST_PK, BMT.BANK_ID, BMT.BANK_NAME ,");
            strBuilder.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
            strBuilder.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            strBuilder.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            strBuilder.Append("   TO_DATE(CI.CREATED_DT) CREATED_DT, ");
            strBuilder.Append("   TO_DATE(CI.LAST_MODIFIED_DT) LAST_MODIFIED_DT, ");
            strBuilder.Append("   TO_DATE(CI.LAST_MODIFIED_DT) APPROVED_DT,ci.last_modified_by_fk,ci.chk_invoice ,ci.created_by_fk");
            strBuilder.Append(" from consol_invoice_tbl ci, ");
            if (BizType == "2")
            {
                strBuilder.Append(" OPERATOR_MST_TBL OPR,");
            }
            else
            {
                strBuilder.Append(" AIRLINE_MST_TBL  AMT,");
            }
            strBuilder.Append(" currency_type_mst_tbl ct,BANK_MST_TBL BMT ,");
            strBuilder.Append("  USER_MST_TBL UMTCRT, ");
            strBuilder.Append("  USER_MST_TBL UMTUPD, ");
            strBuilder.Append("  USER_MST_TBL UMTAPP ");
            strBuilder.Append(" where ci.consol_invoice_pk = " + invPk + "");
            if (BizType == "2")
            {
                strBuilder.Append(" AND OPR.OPERATOR_MST_PK = CI.SUPPLIER_MST_FK ");
            }
            else
            {
                strBuilder.Append(" AND AMT.AIRLINE_MST_PK = CI.SUPPLIER_MST_FK ");
            }
            strBuilder.Append(" AND ct.currency_mst_pk = ci.currency_mst_fk");
            strBuilder.Append(" AND BMT.BANK_MST_PK(+) = CI.BANK_MST_FK");
            strBuilder.Append(" AND CI.IS_FAC_INV = 1 ");
            strBuilder.Append(" AND UMTCRT.USER_MST_PK(+) = CI.CREATED_BY_FK ");
            strBuilder.Append(" AND UMTUPD.USER_MST_PK(+) = CI.LAST_MODIFIED_BY_FK  ");
            strBuilder.Append(" AND UMTAPP.USER_MST_PK(+) = CI.LAST_MODIFIED_BY_FK  ");
            strBuilder.Append(" ");
            try
            {
                return objWK.GetDataSet(strBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchCreditCustomer(int CustPk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append(" select ");
            strBuilder.Append(" cm.credit_customer CRCUS, ");
            strBuilder.Append(" cm.credit_days CRDAYS ");
            strBuilder.Append(" from customer_mst_tbl cm ");
            strBuilder.Append(" where cm.customer_mst_pk =" + CustPk);
            strBuilder.Append(" ");
            try
            {
                return objWK.GetDataSet(strBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public int FetchCreditDays(int CustPk, string JobPKList, int BizType, int Process)
        {
            WorkFlow objWK = new WorkFlow();
            DataSet dsCreditDays = new DataSet();
            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".CREDIT_DAYS_CALCULATION";
                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand.Parameters;
                _with2.Add("CUSTPK", getDefault(CustPk, 0)).Direction = ParameterDirection.Input;
                _with2.Add("JOBPKLIST", getDefault(JobPKList, "0")).Direction = ParameterDirection.Input;
                _with2.Add("BIZTYPE", getDefault(BizType, 1)).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS", getDefault(Process, 2)).Direction = ParameterDirection.Input;
                _with2.Add("CREDITDAYS", OracleDbType.Int32, 30).Direction = ParameterDirection.Output;
                objWK.MyCommand.ExecuteNonQuery();
                return Convert.ToInt32(objWK.MyCommand.Parameters["CREDITDAYS"].Value);

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
        #endregion

        #region "Save"
        public DataSet FetchAmount(string refno)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            StringBuilder sqlinv = new StringBuilder();
            DataSet dsinvoice = new DataSet();
            try
            {
                strsql.Append(" select con.discount_amt disamt,con.net_receivable netamt , cur.currency_id currid  ");
                strsql.Append(" from consol_invoice_tbl con ,currency_type_mst_tbl cur where con.invoice_ref_no like '" + refno + "' ");
                strsql.Append(" and con.currency_mst_fk=cur.currency_mst_pk ");
                dsinvoice.Tables.Add(objWK.GetDataTable(strsql.ToString()));
                sqlinv.Append(" select sum(cc.tax_amt) taxamt ,sum(cc.amt_in_inv_curr) invamt, c.inv_unique_ref_nr from consol_invoice_trn_tbl cc,consol_invoice_tbl c where ");
                sqlinv.Append(" c.invoice_ref_no like '" + refno + "' and c.consol_invoice_pk=cc.consol_invoice_fk group by c.inv_unique_ref_nr");
                dsinvoice.Tables.Add(objWK.GetDataTable(sqlinv.ToString()));
                return dsinvoice;
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
        public int SaveData(DataSet dsSave, object InvRefNo, long nLocationPk, string CREATED_BY_FK, string BizType, string Process, long nEmpId, double CrLimit, string Customer, double NetAmt,
        double CrLimitUsed, Int16 CheckApp = 0, int extype = 1, string uniqueRefNr = "", int InvType = 0, int IsFac = 0)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();

            bool ChkFirstSave = false;
            int intSaveSucceeded = 0;
            OracleTransaction TRAN = null;
            int intPkValue = 0;
            int intChldCnt = 0;
            Int32 cargotype = default(Int32);
            try
            {
                if (string.IsNullOrEmpty(Convert.ToString(InvRefNo)))
                {
                    InvRefNo = GenerateInvoiceNo(nLocationPk, nEmpId, Convert.ToInt64(dsSave.Tables[0].Rows[0]["CREATED_BY_FK_IN"]), objWK);
                    ChkFirstSave = true;
                    if (Convert.ToString(InvRefNo) == "Protocol Not Defined.")
                    {
                        InvRefNo = "";
                        return -1;
                    }
                }
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;
                int UNIQUE = 0;
                if (string.IsNullOrEmpty(uniqueRefNr))
                {
                    System.DateTime dt = default(System.DateTime);
                    dt = System.DateTime.Now;
                    string st = null;
                    st = Convert.ToString(dt.Day + dt.Month + dt.Year + dt.Hour + dt.Minute + dt.Second + dt.Millisecond);
                    uniqueRefNr = GetVEKInvoiceRef(0, 0, st);
                }
                uniqueReferenceNr = uniqueRefNr;

                var _with3 = dsSave.Tables[0].Rows[0];
                objWK.MyCommand.Connection = objWK.MyConnection;
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".CONSOL_INV_PKG.CONSOL_INV_HDR_INS";
                objWK.MyCommand.Parameters.Clear();
                objWK.MyCommand.Parameters.Add("PROCESS_TYPE_IN", Convert.ToInt32(_with3["PROCESS_TYPE_IN"]));
                objWK.MyCommand.Parameters.Add("BUSINESS_TYPE_IN", Convert.ToInt32(_with3["BUSINESS_TYPE_IN"]));
                objWK.MyCommand.Parameters.Add("CHK_INVOICE_IN", CheckApp);
                objWK.MyCommand.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(_with3["CUSTOMER_MST_FK_IN"].ToString()) ? DBNull.Value : _with3["CUSTOMER_MST_FK_IN"]));
                objWK.MyCommand.Parameters.Add("INVOICE_REF_NO_IN", Convert.ToString(InvRefNo));
                objWK.MyCommand.Parameters.Add("INVOICE_DATE_IN", _with3["INVOICE_DATE_IN"]);
                objWK.MyCommand.Parameters.Add("INVOICE_DUE_DATE_IN", _with3["INVOICE_DUE_DATE_IN"]);
                objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with3["CURRENCY_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("BANK_MST_FK_IN", _with3["BANK_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("INVOICE_AMT_IN", _with3["INVOICE_AMT_IN"]);
                objWK.MyCommand.Parameters.Add("DISCOUNT_AMT_IN", _with3["DISCOUNT_AMT_IN"]);
                objWK.MyCommand.Parameters.Add("NET_RECEIVABLE_IN", _with3["NET_RECEIVABLE_IN"]);
                objWK.MyCommand.Parameters.Add("REMARKS_IN", _with3["REMARKS_IN"]);
                objWK.MyCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY_FK);
                objWK.MyCommand.Parameters.Add("EXCH_RATE_TYPE_FK_IN", getDefault(extype, DBNull.Value));
                objWK.MyCommand.Parameters.Add("INV_UNIQUE_REF_NR_IN", getDefault(uniqueRefNr, DBNull.Value));
                objWK.MyCommand.Parameters.Add("INV_TYPE_IN", InvType);
                objWK.MyCommand.Parameters.Add("TDS_REMARKS_IN", _with3["TDS_REMARKS_IN"]);

                objWK.MyCommand.Parameters.Add("SUPPLIER_MST_FK_IN", _with3["SUPPLIER_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("IS_FAC_INV_IN", _with3["IS_FAC_INV"]);

                objWK.MyCommand.Parameters.Add("AIF_IN", _with3["AIF_IN"]);
                objWK.MyCommand.Parameters.Add("RETURN_VALUE", _with3["CREATED_BY_FK_IN"]).Direction = ParameterDirection.Output;
                intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                lngInvPk = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);

                if (ChkFirstSave)
                {
                    for (intChldCnt = 0; intChldCnt <= dsSave.Tables[1].Rows.Count - 1; intChldCnt++)
                    {
                        objWK.MyCommand.Parameters.Clear();
                        objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                        objWK.MyCommand.CommandText = objWK.MyUserName + ".CONSOL_INV_PKG.CONSOL_INV_DETAILS_INS";
                        var _with4 = dsSave.Tables[1].Rows[intChldCnt];
                        objWK.MyCommand.Parameters.Add("CONSOL_INVOICE_FK_IN", intPkValue);
                        objWK.MyCommand.Parameters.Add("PROCESS_TYPE_IN", Convert.ToInt32(dsSave.Tables[0].Rows[0]["PROCESS_TYPE_IN"]));
                        objWK.MyCommand.Parameters.Add("BUSINESS_TYPE_IN", Convert.ToInt32(dsSave.Tables[0].Rows[0]["BUSINESS_TYPE_IN"]));
                        objWK.MyCommand.Parameters.Add("JOB_CARD_FK_IN", _with4["JOB_CARD_FK_IN"]);
                        objWK.MyCommand.Parameters.Add("FRT_OTH_ELEMENT_IN", _with4["FRT_OTH_ELEMENT_IN"]);
                        objWK.MyCommand.Parameters.Add("FRT_OTH_ELEMENT_FK_IN", _with4["FRT_OTH_ELEMENT_FK_IN"]);
                        objWK.MyCommand.Parameters.Add("FRTS_TBL_FK_IN", _with4["FRT_TBL_FK_IN"]);
                        objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with4["CURRENCY_MST_FK_IN"]);
                        objWK.MyCommand.Parameters.Add("ELEMENT_AMT_IN", _with4["ELEMENT_AMT_IN"]);
                        objWK.MyCommand.Parameters.Add("EXCHANGE_RATE_IN", _with4["EXCHANGE_RATE_IN"]);
                        objWK.MyCommand.Parameters.Add("AMT_IN_INV_CURR_IN", _with4["AMT_IN_INV_CURR_IN"]);
                        objWK.MyCommand.Parameters.Add("VAT_CODE_IN", _with4["VAT_CODE_IN"]);
                        objWK.MyCommand.Parameters.Add("TAX_PCNT_IN", _with4["TAX_PCNT_IN"]);
                        objWK.MyCommand.Parameters.Add("TAX_AMT_IN", _with4["TAX_AMT_IN"]);
                        objWK.MyCommand.Parameters.Add("TOT_AMT_IN", _with4["TOT_AMT_IN"]);
                        objWK.MyCommand.Parameters.Add("TOT_AMT_IN_LOC_CURR_IN", _with4["TOT_AMT_IN_LOC_CURR_IN"]);
                        objWK.MyCommand.Parameters.Add("REMARKS_IN", _with4["REMARKS_IN"]);
                        objWK.MyCommand.Parameters.Add("JOBTYPE_IN", _with4["JOBTYPE_IN"]);
                        objWK.MyCommand.Parameters.Add("FRT_DESC_IN", _with4["FRT_DESC_IN"]);
                        objWK.MyCommand.Parameters.Add("LOGED_IN_LOC_FK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                        intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                    }
                }
                else
                {
                    try
                    {
                        objWK.MyCommand.Parameters.Clear();
                        objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                        objWK.MyCommand.CommandText = objWK.MyUserName + ".CONSOL_INV_PKG.CONSOL_INV_DETAILS_DEL";
                        objWK.MyCommand.Parameters.Add("CONSOL_INVOICE_FK_IN", intPkValue);
                        objWK.MyCommand.ExecuteNonQuery();

                    }
                    catch (Exception ex)
                    {
                    }
                    for (intChldCnt = 0; intChldCnt <= dsSave.Tables[1].Rows.Count - 1; intChldCnt++)
                    {
                        objWK.MyCommand.Parameters.Clear();
                        objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                        objWK.MyCommand.CommandText = objWK.MyUserName + ".CONSOL_INV_PKG.CONSOL_INV_DETAILS_INS";
                        var _with5 = dsSave.Tables[1].Rows[intChldCnt];
                        objWK.MyCommand.Parameters.Add("CONSOL_INVOICE_FK_IN", intPkValue);
                        objWK.MyCommand.Parameters.Add("PROCESS_TYPE_IN", Convert.ToInt32(dsSave.Tables[0].Rows[0]["PROCESS_TYPE_IN"]));
                        objWK.MyCommand.Parameters.Add("BUSINESS_TYPE_IN", Convert.ToInt32(dsSave.Tables[0].Rows[0]["BUSINESS_TYPE_IN"]));
                        objWK.MyCommand.Parameters.Add("JOB_CARD_FK_IN", _with5["JOB_CARD_FK_IN"]);
                        objWK.MyCommand.Parameters.Add("FRT_OTH_ELEMENT_IN", _with5["FRT_OTH_ELEMENT_IN"]);
                        objWK.MyCommand.Parameters.Add("FRT_OTH_ELEMENT_FK_IN", _with5["FRT_OTH_ELEMENT_FK_IN"]);
                        objWK.MyCommand.Parameters.Add("FRTS_TBL_FK_IN", _with5["FRT_TBL_FK_IN"]);
                        objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with5["CURRENCY_MST_FK_IN"]);
                        objWK.MyCommand.Parameters.Add("ELEMENT_AMT_IN", _with5["ELEMENT_AMT_IN"]);
                        objWK.MyCommand.Parameters.Add("EXCHANGE_RATE_IN", _with5["EXCHANGE_RATE_IN"]);
                        objWK.MyCommand.Parameters.Add("AMT_IN_INV_CURR_IN", _with5["AMT_IN_INV_CURR_IN"]);
                        objWK.MyCommand.Parameters.Add("VAT_CODE_IN", _with5["VAT_CODE_IN"]);
                        objWK.MyCommand.Parameters.Add("TAX_PCNT_IN", _with5["TAX_PCNT_IN"]);
                        objWK.MyCommand.Parameters.Add("TAX_AMT_IN", _with5["TAX_AMT_IN"]);
                        objWK.MyCommand.Parameters.Add("TOT_AMT_IN", _with5["TOT_AMT_IN"]);
                        objWK.MyCommand.Parameters.Add("TOT_AMT_IN_LOC_CURR_IN", _with5["TOT_AMT_IN_LOC_CURR_IN"]);
                        objWK.MyCommand.Parameters.Add("REMARKS_IN", _with5["REMARKS_IN"]);
                        objWK.MyCommand.Parameters.Add("JOBTYPE_IN", _with5["JOBTYPE_IN"]);
                        objWK.MyCommand.Parameters.Add("FRT_DESC_IN", _with5["FRT_DESC_IN"]);
                        objWK.MyCommand.Parameters.Add("LOGED_IN_LOC_FK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                        intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                    }
                    //'While Approved time only FIN Should Save
                    if (CheckApp == 1)
                    {
                        try
                        {
                            objWK.MyCommand.Parameters.Clear();
                            objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                            objWK.MyCommand.CommandText = objWK.MyUserName + ".CONSOL_INV_PKG.CONSOL_INV_UPDATE_FIN";
                            objWK.MyCommand.Parameters.Add("INV_PK_IN", intPkValue);
                            objWK.MyCommand.ExecuteNonQuery();

                        }
                        catch (Exception ex)
                        {
                        }

                    }
                    //'End
                }

                if (intSaveSucceeded > 0)
                {
                    TRAN.Commit();
                    //Push to financial system if realtime is selected
                    if (CheckApp == 1)
                    {
                        if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["QFINGeneral"]))
                        {
                            if (Convert.ToBoolean(ConfigurationManager.AppSettings["QFINGeneral"]) == true)
                            {
                                try
                                {
                                    TRAN = objWK.MyConnection.BeginTransaction();
                                    objWK.MyCommand.Transaction = TRAN;
                                    objWK.MyCommand.Parameters.Clear();
                                    objWK.MyCommand.CommandText = objWK.MyUserName + ".ACCOUNTING_INTEGREATION_PKG.INVOICE_APPROVE_CANCEL";
                                    objWK.MyCommand.Parameters.Add("INVOICE_TRN_FK_IN", intPkValue).Direction = ParameterDirection.Input;
                                    objWK.MyCommand.Parameters.Add("LOCAL_CUR_FK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                    objWK.MyCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                    objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                    objWK.MyCommand.ExecuteNonQuery();
                                    TRAN.Commit();

                                }
                                catch (Exception ex)
                                {
                                }
                            }
                        }
                        if (intPkValue > 0)
                        {
                            cls_Scheduler objSch = new cls_Scheduler();
                            ArrayList schDtls = null;
                            bool errGen = false;
                            if (objSch.GetSchedulerPushType() == true)
                            {
                            //    QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                            //    try
                            //    {
                            //        schDtls = objSch.FetchSchDtls();
                            //        //'Used to Fetch the Sch Dtls
                            //        objPush.UpdateTaxElement(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //        objPush.UpdatebankAcc(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //        objPush.UpdateCurrency(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //        objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, intPkValue);
                            //        if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                            //        {
                            //            objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                            //        }
                            //    }
                            //    catch (Exception ex)
                            //    {
                            //        if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                            //        {
                            //            objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                            //        }
                            //    }
                            }
                        }
                    }
                    //*****************************************************************
                    if (IsFac != 1)
                    {
                        if (ChkFirstSave)
                        {
                            if (CrLimit > 0)
                            {
                                SaveCreditLimit(NetAmt, Customer, CrLimitUsed, TRAN);
                            }
                            if (Convert.ToInt32(BizType) == 1 & Convert.ToInt32(Process) == 1)
                            {
                                SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(lngInvPk), 1, 1, "Invoice", "INV-AIR-EXP", Convert.ToInt32(nLocationPk), objWK, "INS", Convert.ToInt64(CREATED_BY_FK),"O");
                            }
                            else if (Convert.ToInt32(BizType) == 1 & Convert.ToInt32(Process) == 2)
                            {
                                SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(lngInvPk), 1, 2, "Invoice", "INV-AIR-IMP", Convert.ToInt32(nLocationPk), objWK, "INS", Convert.ToInt64(CREATED_BY_FK),"O");
                            }
                            else if (Convert.ToInt32(BizType) == 2 & Convert.ToInt32(Process) == 1)
                            {
                                SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(lngInvPk), 2, 1, "Invoice", "INV-SEA-EXP", Convert.ToInt32(nLocationPk), objWK, "INS", Convert.ToInt64(CREATED_BY_FK),"O");
                            }
                            else if (Convert.ToInt32(BizType) == 2 & Convert.ToInt32(Process) == 2)
                            {
                                SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(lngInvPk), 2, 2, "Invoice", "INV-SEA-IMP", Convert.ToInt32(nLocationPk), objWK, "INS", Convert.ToInt64(CREATED_BY_FK),"O");
                            }
                        }
                    }
                }
                else
                {
                    TRAN.Rollback();
                    RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(InvRefNo), System.DateTime.Now);
                }
                return intSaveSucceeded;
            }
            catch (OracleException OraExp)
            {
                TRAN.Rollback();
                RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(InvRefNo), System.DateTime.Now);
                throw OraExp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(InvRefNo), System.DateTime.Now);
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }
        #endregion

        #region "save TrackAndTrace"
        public ArrayList SaveTrackAndTraceForInv(OracleTransaction TRAN, int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby,
        string PkStatus)
        {

            Int32 retVal = default(Int32);
            Int32 RecAfct = default(Int32);
            objWF.OpenConnection();

            OracleTransaction TRAN1 = null;
            TRAN1 = objWF.MyConnection.BeginTransaction();
            objWF.MyCommand.Transaction = TRAN1;
            try
            {
                arrMessage.Clear();
                var _with6 = objWF.MyCommand;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
                _with6.Transaction = TRAN1;
                _with6.Parameters.Clear();
                _with6.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("Container_Data_in", DBNull.Value).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with6.ExecuteNonQuery();
                TRAN1.Commit();
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyCommand.Connection.Close();

            }
        }
        #endregion

        #region " Protocol Reference Int32"
        public string GenerateInvoiceNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null)
        {
            return GenerateProtocolKey("CONSOLIDATED INVOICE", nLocationId, nEmployeeId, DateTime.Now, "","" ,"" , nCreatedBy, ObjWK);
        }
        #endregion

        #region "Parent"
        //Created By Mani.Sureshkumar
        //Function for fetching Data in Listing Screen
        public DataSet FetchListData(string strInvRefNo = "", string strJobRefNo = "", string strHblRefNo = "", string strCustomer = "", string strVessel = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long usrLocFK = 0,
        short BizType = 1, short process = 1)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strBuilder = new StringBuilder();
            StringBuilder strCondition = new StringBuilder();
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            string a = null;
            string b = null;
            a = strCustomer;
            if (a == "0")
            {
                strCustomer = "";
            }
            else
            {
                strCustomer = strCustomer;
            }
            b = strVessel;
            if (b == "0")
            {
                strVessel = "";
            }
            else
            {
                strVessel = strVessel;
            }
            strCondition.Append(" SELECT  INV.CONSOL_INVOICE_PK PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" CMT.CUSTOMER_NAME, ");
            strCondition.Append(" INV.INVOICE_DATE,");
            strCondition.Append(" (select SUM(INV.NET_RECEIVABLE) INV_AMT  from  dual) INVAMT ,");
            strCondition.Append(" CUMT.CURRENCY_ID");
            strCondition.Append(" FROM");
            strCondition.Append(" CONSOL_INVOICE_TBL INV, ");
            strCondition.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

            //Sea Export
            if (BizType == 2 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND hbl.HBL_HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.VOYAGE_TRN_FK=" + strVessel + "");
                }

            }
            //Air Export
            if (BizType == 1 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HAWB_EXP_TBL            HAWB,");

                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND HAWB.HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND HAWB.HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.VOYAGE_FLIGHT_NO  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }

            //Sea Import
            if (BizType == 2 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
                strCondition.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.VOYAGE_TRN_FK=" + strVessel + "");
                }
            }
            //Air Import
            if (BizType == 1 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
                strCondition.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.VOYAGE_FLIGHT_NO  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }
            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            strCondition.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            strCondition.Append(" AND INV.consol_invoice_pk = INVTRN.consol_invoice_fk");
            strCondition.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
            strCondition.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");

            if (!string.IsNullOrEmpty(strJobRefNo))
            {
                strCondition.Append(" AND job.jobcard_ref_no='" + strJobRefNo + "'");
            }

            if (!string.IsNullOrEmpty(strCustomer))
            {
                strCondition.Append(" AND CMT.Customer_Mst_Pk=" + strCustomer + "");
            }

            if (!string.IsNullOrEmpty(strInvRefNo))
            {
                strCondition.Append(" AND inv.invoice_ref_no = '" + strInvRefNo + "' ");
            }

            strCondition.Append(" GROUP BY");
            strCondition.Append(" INV.CONSOL_INVOICE_PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" INV.INVOICE_DATE ,");
            strCondition.Append(" CUMT.CURRENCY_ID,CMT.CUSTOMER_NAME,");
            strCondition.Append(" INV.NET_RECEIVABLE   ORDER BY " + SortColumn + "  " + SortType + "  ");

            // Get the Count of Records
            StringBuilder strCount = new StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + strCondition.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            //Get the Total Pages
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            //Get the last page and start page
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            StringBuilder sqlstr = new StringBuilder();
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + strCondition.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            string sql = null;
            sql = sqlstr.ToString();
            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;
                DS.Tables.Add(fetchchild(AllMasterPKs(DS), strInvRefNo, strJobRefNo, strHblRefNo, strCustomer, strVessel, usrLocFK, BizType, process));
                CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["PK"], DS.Tables[1].Columns["PK"], true);
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

        #endregion

        #region "Child Table"
        public DataTable fetchchild(string CONTSpotPKs = "", string strInvRefNo = "", string strJobRefNo = "", string strHblRefNo = "", string strCustomer = "", string strVessel = "", long usrLocFK = 0, short BizType = 1, short process = 1, int extype = 1)
        {
            string a = null;
            string b = null;

            a = strCustomer;
            if (a == "0")
            {
                strCustomer = "";
            }
            else
            {
                strCustomer = strCustomer;
            }
            b = strVessel;
            if (b == "0")
            {
                strVessel = "";
            }
            else
            {
                strVessel = strVessel;
            }

            StringBuilder BuildQuery = new StringBuilder();

            string strsql = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dt = null;
            int RowCnt = 0;
            int Rno = 0;
            int pk = 0;
            BuildQuery.Append("SELECT ROWNUM \"SL.NR\", T.*");
            BuildQuery.Append("FROM (");
            BuildQuery.Append(" SELECT INV.CONSOL_INVOICE_PK PK,");
            BuildQuery.Append(" JOB.JOBCARD_REF_NO, ");

            //Sea Export
            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" HBL.HBL_REF_NO,");
                //Air Export
            }
            else if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" HAWB.HAWB_REF_NO,");
                //Sea Import
            }
            else if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" JOB.HBL_HAWB_REF_NO Hbl_REF_NO,");
                //Air Import
            }
            else if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" JOB.HBL_HAWB_REF_NO,");
            }
            //Sea  
            if (BizType == 2)
            {
                BuildQuery.Append(" (CASE");
                BuildQuery.Append(" WHEN (NVL(JOB.VESSEL_NAME, '') || '/' ||");
                BuildQuery.Append(" NVL(JOB.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                BuildQuery.Append(" ''");
                BuildQuery.Append(" ELSE");
                BuildQuery.Append(" NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO, '')");
                BuildQuery.Append(" END) AS VESVOYAGE,");
                //Air  
            }
            else if (BizType == 1)
            {
                BuildQuery.Append(" JOB.VOYAGE_FLIGHT_NO ,");
            }
            BuildQuery.Append(" INVTRN.TOT_AMT_IN_LOC_CURR,");
            BuildQuery.Append(" CUMT.CURRENCY_ID");
            BuildQuery.Append(" FROM");
            BuildQuery.Append(" CONSOL_INVOICE_TBL INV, ");
            BuildQuery.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

            //Sea Export
            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" HBL_EXP_TBL            HBL,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
                BuildQuery.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                BuildQuery.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    BuildQuery.Append(" AND hbl.hbl_ref_no='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    BuildQuery.Append(" AND JOB.VOYAGE_TRN_FK=" + strVessel + "");
                }
            }
            //Air Export
            if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" HAWB_EXP_TBL           HAWB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
                BuildQuery.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                BuildQuery.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    BuildQuery.Append(" AND HAWB.HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    BuildQuery.Append(" AND JOB.VOYAGE_FLIGHT_NO  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }
            //Sea Import
            if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
                BuildQuery.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    BuildQuery.Append(" AND JOB.VOYAGE_TRN_FK=" + strVessel + "");
                }
            }
            //Air Import
            if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
                BuildQuery.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    BuildQuery.Append(" AND JOB.VOYAGE_FLIGHT_NO  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }

            BuildQuery.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            BuildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            BuildQuery.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append(" AND INV.consol_invoice_pk = INVTRN.consol_invoice_fk");

            BuildQuery.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
            BuildQuery.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");

            if (!string.IsNullOrEmpty(strJobRefNo))
            {
                BuildQuery.Append(" AND job.jobcard_ref_no='" + strJobRefNo + "'");
            }

            if (!string.IsNullOrEmpty(strCustomer))
            {
                BuildQuery.Append(" AND CMT.Customer_Mst_Pk=" + strCustomer + "");
            }

            if (!string.IsNullOrEmpty(strInvRefNo))
            {
                BuildQuery.Append(" AND inv.invoice_ref_no = '" + strInvRefNo + "' ");
            }

            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }
            BuildQuery.Append(" )  T ");


            strsql = BuildQuery.ToString();
            try
            {
                pk = -1;
                dt = objWF.GetDataTable(strsql);
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt]["PK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["PK"]);
                        Rno = 0;
                    }
                    Rno += 1;
                    dt.Rows[RowCnt]["SL.NR"] = Rno;
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
        #endregion

        #region "GET PK"
        //adding by thiyagarajan on 10/11/08 to display PDF through JOBCARD Entry Screen :PTS Task
        public Int32 GetInvPk(string RefNo)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                Strsql = "select con.consol_invoice_pk from consol_invoice_tbl con where con.invoice_ref_no like '" + RefNo + "'";
                return Convert.ToInt32(Objwk.ExecuteScaler(Strsql));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "PK Value"
        private string AllMasterPKs(DataSet ds)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                StringBuilder strBuild = new StringBuilder();
                strBuild.Append("-1,");
                for (RowCnt = 0; RowCnt <= Convert.ToInt16(ds.Tables[0].Rows.Count - 1); RowCnt++)
                {
                    strBuild.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["PK"]).Trim() + ",");
                }
                strBuild.Remove(strBuild.Length - 1, 1);
                return strBuild.ToString();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Enhance Search"
        public string FetchInvoice(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strProcessType = null;
            string strloc = "";
            string IsWriteOff = "";
            string Pending = "";
            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strProcessType = arr[3];
            strloc = arr[4];
            if (arr.Length > 5)
                IsWriteOff = arr[5];
            if (arr.Length > 6)
                Pending = arr[6];
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                //'Snigdharani - To fetch invoices for which writeoff can be done
                if (IsWriteOff == "FROMWRITEOFF")
                {
                    selectCommand.CommandText = objWF.MyUserName + ".en_consol_invoice_pkg.GET_INVOICE_WRITEOFF";
                }
                else if (Pending == "PENDING")
                {
                    selectCommand.CommandText = objWF.MyUserName + ".en_consol_invoice_pkg.GET_PENDING_INVOICE";
                }
                else if (Pending == "COLLECTED")
                {
                    selectCommand.CommandText = objWF.MyUserName + ".en_consol_invoice_pkg.GET_COLLECTED_INVOICE";
                }
                else
                {
                    selectCommand.CommandText = objWF.MyUserName + ".en_consol_invoice_pkg.get_invoice";
                }
                //selectCommand.CommandText = objWF.MyUserName & ".en_consol_invoice_pkg.get_invoice"

                var _with7 = selectCommand.Parameters;
                _with7.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with7.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with7.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
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
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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


        #endregion

        #region "SaveCreditLimit"
        public void SaveCreditLimit(double NetAmt, string Customer, double CrLimitUsed, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();

            OracleTransaction TRAN1 = null;
            OracleCommand cmd = new OracleCommand();
            TRAN1 = objWK.MyConnection.BeginTransaction();
            cmd.Transaction = TRAN1;
            string strSQL = null;
            double temp = 0;
            //temp = CrLimitUsed + NetAmt
            temp = CrLimitUsed - NetAmt;
            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.Connection = TRAN1.Connection;
                cmd.Transaction = TRAN1;

                cmd.Parameters.Clear();
                strSQL = "update customer_mst_tbl a set a.credit_limit_used = " + temp;
                strSQL = strSQL  + " where a.customer_name in ('" + Customer + "')";
                cmd.CommandText = strSQL;
                cmd.ExecuteNonQuery();
                TRAN1.Commit();
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
        #endregion

        #region "Fetch Print Header Report "
        //        Public Function Fetch_Header_print(ByVal invPk As Integer, ByVal BizType As Integer, ByVal ProType As Integer) As DataSet
        //            Dim strQuery As New StringBuilder
        //            Dim objWF As New Business.WorkFlow

        //            Try


        //                strQuery.Append("" & vbCrLf)
        //                strQuery.Append("SELECT  distinct CIT.CONSOL_INVOICE_PK," & vbCrLf)
        //                strQuery.Append("       JCSE.JOBCARD_REF_NO," & vbCrLf)
        //                strQuery.Append("       CUST.CUSTOMER_ID," & vbCrLf)
        //                strQuery.Append("       ( SELECT PMTL.PORT_ID FROM PORT_MST_TBL PMTL WHERE PMTL.PORT_MST_PK= BST.PORT_MST_POL_FK ) POL," & vbCrLf)
        //                strQuery.Append("       ( SELECT PMTD.PORT_ID FROM PORT_MST_TBL PMTD WHERE PMTD.PORT_MST_PK= BST.PORT_MST_POD_FK ) POD," & vbCrLf)
        //                strQuery.Append("       HBL.HBL_REF_NO," & vbCrLf)
        //                strQuery.Append("       MBL.MBL_MAWB_REF_NO," & vbCrLf)
        //                strQuery.Append("       CIT.INVOICE_AMT" & vbCrLf)
        //                strQuery.Append("  FROM CONSOL_INVOICE_TBL  CIT," & vbCrLf)
        //                strQuery.Append("       CONSOL_INVOICE_TRN_TBL CITT," & vbCrLf)
        //                strQuery.Append("       JOB_CARD_TRN JCSE," & vbCrLf)
        //                strQuery.Append("       CUSTOMER_MST_TBL CUST," & vbCrLf)
        //                strQuery.Append("       BOOKING_MST_TBL BST," & vbCrLf)
        //                strQuery.Append("       HBL_EXP_TBL   HBL," & vbCrLf)
        //                strQuery.Append("       MBL_EXP_TBL MBL" & vbCrLf)
        //                strQuery.Append(" WHERE CITT.CONSOL_INVOICE_FK=CIT.CONSOL_INVOICE_PK" & vbCrLf)
        //                strQuery.Append("   AND CITT.JOB_CARD_FK= JCSE.JOB_CARD_TRN_PK(+)" & vbCrLf)
        //                strQuery.Append("   AND JCSE.CONSIGNEE_CUST_MST_FK=CUST.CUSTOMER_MST_PK" & vbCrLf)
        //                strQuery.Append("   AND JCSE.BOOKING_MST_FK=BST.BOOKING_MST_PK" & vbCrLf)
        //                strQuery.Append("   AND JCSE.HBL_HAWB_FK=HBL.HBL_EXP_TBL_PK(+)" & vbCrLf)
        //                strQuery.Append("   AND JCSE.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK(+)" & vbCrLf)
        //                strQuery.Append("   AND CIT.CONSOL_INVOICE_PK=" & invPk)
        //                strQuery.Append("   and cit.business_type=" & BizType)
        //                strQuery.Append("   and cit.process_type=" & ProType)

        //                Return objWF.GetDataSet(strQuery.ToString)

        //            Catch exp As Exception
        //                ErrorMessage = exp.Message
        //                Throw exp
        //            End Try



        //        End Function

        #endregion

        #region "Fetch Print Sub Report"
        //        Public Function Fetch_Sub_print(ByVal invPk As Integer, ByVal BizType As Integer, ByVal ProType As Integer) As DataSet
        //            Dim strQuery As New StringBuilder
        //            Dim objWF As New Business.WorkFlow

        //            Try
        //                strQuery.Append("SELECT CITT.CONSOL_INVOICE_FK," & vbCrLf)
        //                strQuery.Append("       JCSE.JOBCARD_REF_NO," & vbCrLf)
        //                strQuery.Append("       FEMT.FREIGHT_ELEMENT_NAME," & vbCrLf)
        //                strQuery.Append("       CTMT.CURRENCY_ID," & vbCrLf)
        //                strQuery.Append("       CITT.ELEMENT_AMT," & vbCrLf)
        //                strQuery.Append("       CITT.EXCHANGE_RATE," & vbCrLf)
        //                strQuery.Append("       CITT.TOT_AMT" & vbCrLf)
        //                strQuery.Append("  FROM CONSOL_INVOICE_TRN_TBL  CITT," & vbCrLf)
        //                strQuery.Append("       consol_invoice_tbl cit, " & vbCrLf)
        //                strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FEMT," & vbCrLf)
        //                strQuery.Append("       CURRENCY_TYPE_MST_TBL   CTMT," & vbCrLf)
        //                strQuery.Append("       JOB_CARD_TRN JCSE" & vbCrLf)
        //                strQuery.Append(" WHERE CITT.CONSOL_INVOICE_FK = " & invPk)
        //                strQuery.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK" & vbCrLf)
        //                strQuery.Append("   AND CITT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK" & vbCrLf)
        //                strQuery.Append("   AND CITT.JOB_CARD_FK= JCSE.JOB_CARD_TRN_PK(+)" & vbCrLf)
        //                strQuery.Append("   and citt.consol_invoice_fk=cit.consol_invoice_pk" & vbCrLf)
        //                '  strQuery.Append("   AND CIT.CONSOL_INVOICE_PK=" & invPk)
        //                strQuery.Append("   and cit.business_type=" & BizType)
        //                strQuery.Append("   and cit.process_type=" & ProType)
        //                strQuery.Append("   " & vbCrLf)

        //                Return objWF.GetDataSet(strQuery.ToString)

        //            Catch exp As Exception
        //                ErrorMessage = exp.Message
        //                Throw exp
        //            End Try
        //        End Function
        #endregion

        #region "Fetch Job Card Sea/Air Details For Report"
        public DataSet FetchJobCardSeaDetails(string jobcardpk, int process)
        {
            StringBuilder Strsql = new StringBuilder();
            WorkFlow Objwk = new WorkFlow();
            if (process == 2)
            {
                Strsql.Append(" SELECT JSI.JOB_CARD_TRN_PK AS JOBCARDPK,");
                Strsql.Append(" JSI.JOBCARD_REF_NO JOBCARDNO,");
                Strsql.Append(" JSI.UCR_NO AS UCRNO,");
                Strsql.Append(" CONSIGCUST.CUSTOMER_NAME CONSIGNAME,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_1 CONSIGADD1,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_2 CONSIGADD2,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_3 CONSIGADD3,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ZIP_CODE CONSIGZIP,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_PHONE_NO_1 CONSIGPHONE,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_CITY CONSIGCITY,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_FAX_NO CONFAX,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_EMAIL_ID CONEMAIL,");
                Strsql.Append(" CONSCOUNTRY.COUNTRY_NAME CONSCOUNTRY,");
                Strsql.Append(" SHIPPERCUST.CUSTOMER_NAME SHIPPERNAME,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_1 SHIPPERADD1,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_2 SHIPPERADD2,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_3 SHIPPERADD3,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ZIP_CODE SHIPPERZIP,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_CITY SHIPPERCITY,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_FAX_NO SHIPPERFAX,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_EMAIL_ID SHIPPEREMAIL,");
                Strsql.Append(" SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,");
                Strsql.Append(" AGENTMST.AGENT_NAME AGENTNAME,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_1 AGENTADD1,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_2 AGENTADD2,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_3 AGENTADD3,");
                Strsql.Append(" AGENTDTLS.ADM_CITY      AGENTCITY,");
                Strsql.Append(" AGENTDTLS.ADM_ZIP_CODE  AGENTZIP,");
                Strsql.Append(" AGENTDTLS.ADM_PHONE_NO_1 AGENTPHONE,");
                Strsql.Append(" AGENTDTLS.ADM_FAX_NO    AGENTFAX,");
                Strsql.Append(" AGENTDTLS.ADM_EMAIL_ID  AGENTEMAIL,");
                Strsql.Append(" AGENTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,");
                Strsql.Append(" (CASE WHEN JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                Strsql.Append(" JSI.VESSEL_NAME || '/' || JSI.VOYAGE_FLIGHT_NO");
                Strsql.Append(" ELSE");
                Strsql.Append(" JSI.VESSEL_NAME END ) VES_VOY,");
                Strsql.Append(" CTMST.COMMODITY_GROUP_CODE COMMTYPE,");
                Strsql.Append(" (CASE WHEN JSI.HBL_HAWB_REF_NO IS NOT NULL THEN");
                Strsql.Append(" JSI.HBL_HAWB_REF_NO");
                Strsql.Append("  ELSE");
                Strsql.Append(" JSI.MBL_MAWB_REF_NO END ) BLREFNO,");

                Strsql.Append("POL.PORT_ID || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=POL.country_mst_fk) POLNAME,");
                Strsql.Append("POD.PORT_ID || ','|| (select cont1.country_name from country_mst_tbl cont1 where cont1.country_mst_pk=POD.country_mst_fk) PODNAME,");

                Strsql.Append(" DELMST.PLACE_NAME DEL_PLACE_NAME,");
                Strsql.Append(" JSI.GOODS_DESCRIPTION,");
                Strsql.Append(" JSI.ETA_DATE ETA,");
                Strsql.Append(" JSI.ETD_DATE ETD,");
                Strsql.Append(" JSI.CLEARANCE_ADDRESS CLEARANCEPOINT,");
                Strsql.Append(" JSI.MARKS_NUMBERS MARKS,");
                Strsql.Append(" STMST.INCO_CODE TERMS,");
                Strsql.Append(" NVL(JSI.INSURANCE_AMT, 0) INSURANCE,");
                Strsql.Append(" JSI.PYMT_TYPE PYMT_TYPE,");
                Strsql.Append(" JSI.CARGO_TYPE CARGO_TYPE,");
                Strsql.Append(" SUM(JTSC.GROSS_WEIGHT) GROSSWEIGHT,");
                Strsql.Append("  SUM(JTSC.NET_WEIGHT) NETWEIGHT,");
                Strsql.Append("  SUM(JTSC.CHARGEABLE_WEIGHT) CHARWT,");
                Strsql.Append(" SUM(JTSC.VOLUME_IN_CBM) VOLUME");
                Strsql.Append(" from JOB_CARD_TRN JSI,");
                Strsql.Append(" CUSTOMER_MST_TBL CONSIGCUST,");
                Strsql.Append(" CUSTOMER_CONTACT_DTLS CONSIGCUSTDTLS,");
                Strsql.Append(" CUSTOMER_CONTACT_DTLS SHIPPERCUSTDTLS,");
                Strsql.Append(" CUSTOMER_MST_TBL SHIPPERCUST,");
                Strsql.Append(" AGENT_MST_TBL AGENTMST,");
                Strsql.Append(" AGENT_CONTACT_DTLS AGENTDTLS,");
                Strsql.Append(" PORT_MST_TBL POL,");
                Strsql.Append(" PORT_MST_TBL POD,");
                Strsql.Append(" JOB_TRN_CONT JTSC,");
                Strsql.Append(" SHIPPING_TERMS_MST_TBL STMST,");
                Strsql.Append(" COUNTRY_MST_TBL SHIPCOUNTRY,");
                Strsql.Append(" COUNTRY_MST_TBL CONSCOUNTRY,");
                Strsql.Append(" COUNTRY_MST_TBL AGENTCOUNTRY,");
                Strsql.Append(" PLACE_MST_TBL DELMST,");
                Strsql.Append(" JOB_TRN_SEA_IMP_TP JTSIT,");
                Strsql.Append(" COMMODITY_GROUP_MST_TBL CTMST");
                Strsql.Append(" WHERE CONSIGCUST.CUSTOMER_MST_PK(+) = JSI.CONSIGNEE_CUST_MST_FK");
                Strsql.Append(" AND   SHIPPERCUST.CUSTOMER_MST_PK(+)=JSI.SHIPPER_CUST_MST_FK");
                Strsql.Append(" AND   CONSIGCUSTDTLS.CUSTOMER_MST_FK(+)=CONSIGCUST.CUSTOMER_MST_PK");
                Strsql.Append(" AND   SHIPPERCUSTDTLS.CUSTOMER_MST_FK(+)=SHIPPERCUST.CUSTOMER_MST_PK");
                Strsql.Append(" AND   POL.PORT_MST_PK(+)=JSI.PORT_MST_POL_FK");
                Strsql.Append(" AND   POD.PORT_MST_PK(+)=JSI.PORT_MST_POD_FK");
                Strsql.Append(" AND   JTSC.JOB_CARD_TRN_FK(+)=JSI.JOB_CARD_TRN_PK");
                Strsql.Append(" AND   STMST.SHIPPING_TERMS_MST_PK(+)=JSI.SHIPPING_TERMS_MST_FK");
                Strsql.Append(" AND   CONSCOUNTRY.COUNTRY_MST_PK(+)=CONSIGCUSTDTLS.ADM_COUNTRY_MST_FK");
                Strsql.Append(" AND   SHIPCOUNTRY.COUNTRY_MST_PK(+)=SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK ");
                Strsql.Append(" AND   AGENTMST.AGENT_MST_PK(+)=JSI.POL_AGENT_MST_FK");
                Strsql.Append(" AND   AGENTDTLS.AGENT_MST_FK(+)=AGENTMST.AGENT_MST_PK");
                Strsql.Append(" AND   AGENTCOUNTRY.COUNTRY_MST_PK(+)=AGENTDTLS.ADM_COUNTRY_MST_FK");
                Strsql.Append(" AND   DELMST.PLACE_PK(+)=JSI.DEL_PLACE_MST_FK");
                Strsql.Append(" AND CTMST.COMMODITY_GROUP_PK(+)=JSI.COMMODITY_GROUP_FK");
                Strsql.Append(" AND JTSIT.JOB_CARD_TRN_FK(+)=JSI.JOB_CARD_TRN_PK");
                Strsql.Append(" AND  nvl(JTSIT.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_SEA_IMP_TP JTT WHERE JTT.JOB_CARD_TRN_FK=JTSIT.JOB_CARD_TRN_FK)");
                Strsql.Append(" AND JSI.JOB_CARD_TRN_PK IN (" + jobcardpk + ")");
                Strsql.Append(" GROUP BY JSI.JOB_CARD_TRN_PK,");
                Strsql.Append(" JSI.JOBCARD_REF_NO ,");
                Strsql.Append(" JSI.UCR_NO  ,");
                Strsql.Append(" CONSIGCUST.CUSTOMER_NAME ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_1 ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_2 ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_3 ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ZIP_CODE ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_PHONE_NO_1 ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_CITY ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_FAX_NO ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_EMAIL_ID ,");
                Strsql.Append(" CONSCOUNTRY.COUNTRY_NAME ,");
                Strsql.Append(" SHIPPERCUST.CUSTOMER_NAME ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_1 ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_2 ,");
                Strsql.Append("  SHIPPERCUSTDTLS.ADM_ADDRESS_3 ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ZIP_CODE ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_PHONE_NO_1 ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_CITY ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_FAX_NO ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_EMAIL_ID ,");
                Strsql.Append(" SHIPCOUNTRY.COUNTRY_NAME,");
                Strsql.Append(" AGENTMST.AGENT_NAME ,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_1,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_2 ,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_3,");
                Strsql.Append(" AGENTDTLS.ADM_CITY,");
                Strsql.Append(" AGENTDTLS.ADM_ZIP_CODE ,");
                Strsql.Append("  AGENTDTLS.ADM_PHONE_NO_1,");
                Strsql.Append("  AGENTDTLS.ADM_FAX_NO ,");
                Strsql.Append("  AGENTDTLS.ADM_EMAIL_ID,");
                Strsql.Append(" AGENTCOUNTRY.COUNTRY_NAME,");
                Strsql.Append(" (CASE WHEN JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                Strsql.Append(" JSI.VESSEL_NAME || '/' || JSI.VOYAGE_FLIGHT_NO");
                Strsql.Append(" ELSE");
                Strsql.Append(" JSI.VESSEL_NAME END ) ,");
                Strsql.Append(" CTMST.COMMODITY_GROUP_CODE,");
                Strsql.Append(" (CASE WHEN JSI.HBL_HAWB_REF_NO IS NOT NULL THEN");
                Strsql.Append("  JSI.HBL_HAWB_REF_NO");
                Strsql.Append("  ELSE");
                Strsql.Append("  JSI.MBL_MAWB_REF_NO END ) ,");

                Strsql.Append(" POL.PORT_ID,POL.COUNTRY_MST_FK,");
                Strsql.Append(" POD.PORT_ID,POD.COUNTRY_MST_FK,");

                Strsql.Append(" DELMST.PLACE_NAME ,");
                Strsql.Append(" JSI.GOODS_DESCRIPTION,");
                Strsql.Append(" JSI.ETA_DATE ,");
                Strsql.Append(" JSI.ETD_DATE ,");
                Strsql.Append(" JSI.CLEARANCE_ADDRESS,");
                Strsql.Append(" JSI.MARKS_NUMBERS ,");
                Strsql.Append(" STMST.INCO_CODE,");
                Strsql.Append("  NVL(JSI.INSURANCE_AMT,0),");
                Strsql.Append(" JSI.CARGO_TYPE,");
                Strsql.Append(" JSI.PYMT_TYPE");

            }
            else if (process == 1)
            {
                Strsql.Append("select (select cus.customer_name from customer_mst_tbl cus where cus.customer_mst_pk=consg.customer_mst_fk ) CONSIGNAME,");
                Strsql.Append("consg.adm_address_1 CONSIGADD1,");
                Strsql.Append("consg.adm_address_2 CONSIGADD2,");
                Strsql.Append(" consg.adm_address_3 CONSIGADD3,");
                Strsql.Append("consg.adm_zip_code CONSIGZIP,");
                Strsql.Append(" consg.adm_city CONSIGCITY,");
                Strsql.Append(" ( select ctry.country_name from country_mst_tbl ctry where ctry.country_mst_pk=consg.adm_country_mst_fk) CONSCOUNTRY,");

                Strsql.Append(" ((select pol.port_id  from port_mst_tbl pol where pol.port_mst_pk=booking.port_mst_pol_fk) || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=(select pol.country_mst_fk from port_mst_tbl pol where pol.port_mst_pk=booking.port_mst_pol_fk))) POLNAME,");
                Strsql.Append(" ((select pod.port_id  from port_mst_tbl pod where pod.port_mst_pk=booking.port_mst_pod_fk) || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=(select pod.country_mst_fk from port_mst_tbl poD where pod.port_mst_pk=booking.port_mst_pod_fk))) PODNAME,");

                Strsql.Append("(  CASE WHEN job.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                Strsql.Append(" Job.VESSEL_NAME || '/' || Job.VOYAGE_FLIGHT_NO");
                Strsql.Append(" Else ");
                Strsql.Append(" Job.VESSEL_NAME END ) VES_VOY,");

                Strsql.Append("(select pl.place_name from place_mst_tbl pl where pl.place_pk=booking.DEL_PLACE_MST_FK) DEL_PLACE_NAME,");

                Strsql.Append("(CASE WHEN job.HBL_HAWB_FK IS NOT NULL THEN");
                Strsql.Append("(select hbl.hbl_ref_no from hbl_exp_tbl hbl where hbl.hbl_exp_tbl_pk=job.HBL_HAWB_FK)");
                Strsql.Append("Else");
                Strsql.Append("(select mbl.mbl_ref_no from mbl_exp_tbl mbl where mbl.mbl_exp_tbl_pk=job.MBL_MAWB_FK)");
                Strsql.Append(" END) BLREFNO,");
                Strsql.Append(" job.goods_description, job.marks_numbers MARKS,");
                Strsql.Append(" (select ship.inco_code from shipping_terms_mst_tbl ship where ship.shipping_terms_mst_pk=job.shipping_terms_mst_fk)");
                Strsql.Append(" TERMS,job.pymt_type,NVL(job.INSURANCE_AMT, 0) INSURANCE,");

                Strsql.Append("(select sum(jstc.gross_weight) from JOB_TRN_CONT jstc where jstc.JOB_CARD_TRN_FK=job.JOB_CARD_TRN_PK) GROSSWEIGHT,");

                Strsql.Append(" (select sum(jstc.volume_in_cbm) from JOB_TRN_CONT jstc where jstc.JOB_CARD_TRN_FK=job.JOB_CARD_TRN_PK) VOLUME,");
                Strsql.Append(" (select sum(jstc.chargeable_weight) from JOB_TRN_CONT jstc where jstc.JOB_CARD_TRN_FK=job.JOB_CARD_TRN_PK) CHARWT ");

                Strsql.Append(" from JOB_CARD_TRN job ,BOOKING_MST_TBL booking, customer_contact_dtls consg");
                Strsql.Append(" where job.JOB_CARD_TRN_PK in (" + jobcardpk + ")");
                Strsql.Append(" and job.BOOKING_MST_FK=booking.BOOKING_MST_PK");
                Strsql.Append(" and job.consignee_cust_mst_fk=consg.customer_mst_fk");
            }
            try
            {
                return Objwk.GetDataSet(Strsql.ToString());
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
        #endregion

        #region "FetchJobCardAirDetails"
        public DataSet FetchJobCardAirDetails(string JobCardPK, int process)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            if (process == 2)
            {
                Strsql = "select JAI.JOB_CARD_TRN_PK AS JOBCARDPK,";
                Strsql += "JAI.JOBCARD_REF_NO JOBCARDNO,";
                Strsql += "JAI.UCR_NO AS UCRNO,";
                Strsql += "CONSIGCUST.CUSTOMER_NAME CONSIGNAME,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_1 CONSIGADD1,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_2 CONSIGADD2,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_3 CONSIGADD3,";
                Strsql += "CONSIGCUSTDTLS.ADM_ZIP_CODE CONSIGZIP,";
                Strsql += "CONSIGCUSTDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
                Strsql += "CONSIGCUSTDTLS.ADM_CITY CONSIGCITY,";
                Strsql += "CONSIGCUSTDTLS.ADM_FAX_NO CONFAX,";
                Strsql += "CONSIGCUSTDTLS.ADM_EMAIL_ID CONEMAIL,";
                Strsql += "CONSCOUNTRY.COUNTRY_NAME CONSCOUNTRY,";
                Strsql += "SHIPPERCUST.CUSTOMER_NAME SHIPPERNAME,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_1 SHIPPERADD1,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ZIP_CODE SHIPPERZIP,";
                Strsql += "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
                Strsql += "SHIPPERCUSTDTLS.ADM_CITY SHIPPERCITY,";
                Strsql += "SHIPPERCUSTDTLS.ADM_FAX_NO SHIPPERFAX,";
                Strsql += "SHIPPERCUSTDTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
                Strsql += "SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
                Strsql += "AGENTMST.AGENT_NAME AGENTNAME,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_1 AGENTADD1,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_2 AGENTADD2,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_3 AGENTADD3,";
                Strsql += "AGENTDTLS.ADM_CITY      AGENTCITY,";
                Strsql += "AGENTDTLS.ADM_ZIP_CODE  AGENTZIP,";
                Strsql += "AGENTDTLS.ADM_PHONE_NO_1 AGENTPHONE,";
                Strsql += "AGENTDTLS.ADM_FAX_NO    AGENTFAX,";
                Strsql += "AGENTDTLS.ADM_EMAIL_ID  AGENTEMAIL,";
                Strsql += "AGENTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,";
                Strsql += "JAI.VOYAGE_FLIGHT_NO  VES_VOY,";
                Strsql += "CGMST.COMMODITY_GROUP_DESC COMMTYPE,";
                Strsql += "(CASE WHEN JAI.HBL_HAWB_REF_NO IS NOT NULL THEN";
                Strsql += "JAI.HBL_HAWB_REF_NO";
                Strsql += " ELSE";
                Strsql += "JAI.HBL_HAWB_REF_NO END ) BLREFNO,";

                Strsql += "(POL.PORT_ID || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=POL.country_mst_fk)) POLNAME,";
                Strsql += "(POD.PORT_ID || ','|| (select cont1.country_name from country_mst_tbl cont1 where cont1.country_mst_pk=POD.country_mst_fk)) PODNAME,";


                Strsql += "DELMST.PLACE_NAME DEL_PLACE_NAME,";
                Strsql += "JAI.GOODS_DESCRIPTION,";
                Strsql += "JAI.ETD_DATE ETD,";
                Strsql += "JAI.ETA_DATE ETA,";
                Strsql += "JAI.CLEARANCE_ADDRESS CLEARANCEPOINT,";
                Strsql += "JAI.MARKS_NUMBERS MARKS,";
                Strsql += "STMST.INCO_CODE TERMS,";
                Strsql += "NVL(JAI.INSURANCE_AMT, 0) INSURANCE,";
                Strsql += "JAI.PYMT_TYPE PYMT_TYPE,";
                Strsql += "2 CARGO_TYPE,";
                Strsql += "SUM(JTSC.GROSS_WEIGHT) GROSSWEIGHT,";
                Strsql += " '' NETWEIGHT,";
                Strsql += " SUM(JTSC.CHARGEABLE_WEIGHT) CHARWT,";
                Strsql += "SUM(JTSC.VOLUME_IN_CBM) VOLUME";
                Strsql += "from JOB_CARD_TRN JAI,";
                Strsql += "JOB_TRN_AIR_IMP_TP JTAIT,";
                Strsql += "CUSTOMER_MST_TBL CONSIGCUST,";
                Strsql += "CUSTOMER_CONTACT_DTLS CONSIGCUSTDTLS,";
                Strsql += "CUSTOMER_CONTACT_DTLS SHIPPERCUSTDTLS,";
                Strsql += "CUSTOMER_MST_TBL SHIPPERCUST,";
                Strsql += "AGENT_MST_TBL AGENTMST,";
                Strsql += "AGENT_CONTACT_DTLS AGENTDTLS,";
                Strsql += "PORT_MST_TBL POL,";
                Strsql += "PORT_MST_TBL POD,";
                Strsql += "JOB_TRN_CONT JTSC,";
                Strsql += "SHIPPING_TERMS_MST_TBL STMST,";
                Strsql += "COUNTRY_MST_TBL SHIPCOUNTRY,";
                Strsql += "COUNTRY_MST_TBL CONSCOUNTRY,";
                Strsql += "COUNTRY_MST_TBL AGENTCOUNTRY,";
                Strsql += "PLACE_MST_TBL DELMST,";
                Strsql += "COMMODITY_GROUP_MST_TBL CGMST";
                Strsql += "WHERE CONSIGCUST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
                Strsql += " AND   JTAIT.JOB_CARD_TRN_FK(+)= JAI.JOB_CARD_TRN_PK";
                Strsql += "AND  nvl(JTAIT.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_AIR_IMP_TP JTT WHERE JTT.JOB_CARD_TRN_FK=JTAIT.JOB_CARD_TRN_FK)";
                Strsql += "AND   SHIPPERCUST.CUSTOMER_MST_PK(+)=JAI.SHIPPER_CUST_MST_FK";
                Strsql += "AND   CONSIGCUSTDTLS.CUSTOMER_MST_FK(+)=CONSIGCUST.CUSTOMER_MST_PK";
                Strsql += "AND   SHIPPERCUSTDTLS.CUSTOMER_MST_FK(+)=SHIPPERCUST.CUSTOMER_MST_PK";
                Strsql += "AND   POL.PORT_MST_PK(+)=JAI.PORT_MST_POL_FK";
                Strsql += "AND   POD.PORT_MST_PK(+)=JAI.PORT_MST_POD_FK";
                Strsql += "AND   JTSC.JOB_CARD_TRN_FK(+)=JAI.JOB_CARD_TRN_PK";
                Strsql += "AND   STMST.SHIPPING_TERMS_MST_PK(+)=JAI.SHIPPING_TERMS_MST_FK";
                Strsql += "AND   CONSCOUNTRY.COUNTRY_MST_PK(+)=CONSIGCUSTDTLS.ADM_COUNTRY_MST_FK";
                Strsql += "AND   SHIPCOUNTRY.COUNTRY_MST_PK(+)=SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK ";
                Strsql += "AND   AGENTMST.AGENT_MST_PK(+)=JAI.POL_AGENT_MST_FK";
                Strsql += "AND   AGENTDTLS.AGENT_MST_FK(+)=AGENTMST.AGENT_MST_PK";
                Strsql += "AND   AGENTCOUNTRY.COUNTRY_MST_PK(+)=AGENTDTLS.ADM_COUNTRY_MST_FK";
                Strsql += "AND   DELMST.PLACE_PK(+)=JAI.DEL_PLACE_MST_FK";
                Strsql += "AND   CGMST.COMMODITY_GROUP_PK(+)=JAI.COMMODITY_GROUP_FK";
                Strsql += "AND   JAI.JOB_CARD_TRN_PK IN (" + JobCardPK + ")";
                Strsql += "GROUP BY JAI.JOB_CARD_TRN_PK,";
                Strsql += "JAI.JOBCARD_REF_NO ,";
                Strsql += "JAI.UCR_NO  ,";
                Strsql += "CONSIGCUST.CUSTOMER_NAME ,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_1 ,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_2 ,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_3 ,";
                Strsql += " CONSIGCUSTDTLS.ADM_ZIP_CODE ,";
                Strsql += "CONSIGCUSTDTLS.ADM_PHONE_NO_1 ,";
                Strsql += "CONSIGCUSTDTLS.ADM_CITY ,";
                Strsql += "CONSIGCUSTDTLS.ADM_FAX_NO ,";
                Strsql += "CONSIGCUSTDTLS.ADM_EMAIL_ID ,";
                Strsql += "CONSCOUNTRY.COUNTRY_NAME ,";
                Strsql += "SHIPPERCUST.CUSTOMER_NAME ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_1 ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_2 ,";
                Strsql += " SHIPPERCUSTDTLS.ADM_ADDRESS_3 ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ZIP_CODE ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_CITY ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_FAX_NO ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_EMAIL_ID ,";
                Strsql += "SHIPCOUNTRY.COUNTRY_NAME,";
                Strsql += "AGENTMST.AGENT_NAME ,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_1,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_2 ,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_3,";
                Strsql += "AGENTDTLS.ADM_CITY,";
                Strsql += "AGENTDTLS.ADM_ZIP_CODE ,";
                Strsql += " AGENTDTLS.ADM_PHONE_NO_1,";
                Strsql += " AGENTDTLS.ADM_FAX_NO ,";
                Strsql += " AGENTDTLS.ADM_EMAIL_ID,";
                Strsql += "AGENTCOUNTRY.COUNTRY_NAME,";
                Strsql += "JAI.VOYAGE_FLIGHT_NO ,";
                Strsql += "CGMST.COMMODITY_GROUP_DESC,";
                Strsql += "(CASE WHEN JAI.HBL_HAWB_REF_NO IS NOT NULL THEN";
                Strsql += " JAI.HBL_HAWB_REF_NO";
                Strsql += " ELSE";
                Strsql += " JAI.MBL_MAWB_REF_NO END ) ,";

                Strsql += " POL.PORT_ID,POL.COUNTRY_MST_FK,";
                Strsql += " POD.PORT_ID,POD.COUNTRY_MST_FK,";

                Strsql += "DELMST.PLACE_NAME ,";
                Strsql += "JAI.GOODS_DESCRIPTION,";
                Strsql += "JAI.ETD_DATE ,";
                Strsql += "JAI.ETA_DATE ,";
                Strsql += "JAI.CLEARANCE_ADDRESS,";
                Strsql += "JAI.MARKS_NUMBERS ,";
                Strsql += "STMST.INCO_CODE,";
                Strsql += " NVL(JAI.INSURANCE_AMT,0),";
                Strsql += "JAI.PYMT_TYPE";

            }
            else if (process == 1)
            {
                Strsql += " select (select cus.customer_name from customer_mst_tbl cus where cus.customer_mst_pk=consg.customer_mst_fk ) CONSIGNAME,";
                Strsql += " consg.adm_address_1 CONSIGADD1,";
                Strsql += " consg.adm_address_2 CONSIGADD2,";
                Strsql += " consg.adm_address_3 CONSIGADD3,";
                Strsql += " consg.adm_zip_code CONSIGZIP,";
                Strsql += " consg.adm_city CONSIGCITY,";

                Strsql += "( select ctry.country_name from country_mst_tbl ctry where ctry.country_mst_pk=consg.adm_country_mst_fk) CONSCOUNTRY,";

                Strsql += " ((select pol.port_id  from port_mst_tbl pol where pol.port_mst_pk=booking.port_mst_pol_fk) || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=(select pol.country_mst_fk from port_mst_tbl pol where pol.port_mst_pk=booking.port_mst_pol_fk))) POLNAME,";
                Strsql += " ((select pod.port_id  from port_mst_tbl pod where pod.port_mst_pk=booking.port_mst_pod_fk) || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=(select pod.country_mst_fk from port_mst_tbl poD where pod.port_mst_pk=booking.port_mst_pod_fk))) PODNAME,";

                Strsql += "job.VOYAGE_FLIGHT_NO VES_VOY,";

                Strsql += "(select pl.place_name from place_mst_tbl pl where pl.place_pk=booking.DEL_PLACE_MST_FK) DEL_PLACE_NAME,";
                Strsql += "(CASE WHEN job.HBL_HAWB_FK IS NOT NULL THEN";
                Strsql += "(select hawb.Hawb_Ref_No from hawb_exp_tbl hawb where hawb.hawb_exp_tbl_pk=job.HBL_HAWB_FK)";
                Strsql += "Else";
                Strsql += "(select mawb.mawb_ref_no from mawb_exp_tbl mawb where mawb.mawb_exp_tbl_pk=job.MBL_MAWB_FK)";
                Strsql += " END) BLREFNO,";
                Strsql += " job.goods_description, job.marks_numbers MARKS,";
                Strsql += " (select ship.inco_code from shipping_terms_mst_tbl ship where ship.shipping_terms_mst_pk=job.shipping_terms_mst_fk)";
                Strsql += " TERMS,job.pymt_type,NVL(job.INSURANCE_AMT, 0) INSURANCE,";

                Strsql += "(select sum(jstc.gross_weight) from JOB_TRN_CONT jstc where jstc.JOB_CARD_TRN_PK=job.JOB_CARD_TRN_PK) GROSSWEIGHT,";

                Strsql += " (select sum(jstc.volume_in_cbm) from JOB_TRN_CONT jstc where jstc.JOB_CARD_TRN_PK=job.JOB_CARD_TRN_PK) VOLUME,";
                Strsql += " (select sum(jstc.chargeable_weight) from JOB_TRN_CONT jstc where jstc.JOB_CARD_TRN_PK=job.JOB_CARD_TRN_PK) CHARWT ";

                Strsql += " from JOB_CARD_TRN job ,BOOKING_MST_TBL booking, customer_contact_dtls consg";
                Strsql += " where job.JOB_CARD_TRN_PK in (" + JobCardPK + ")";
                Strsql += " and job.BOOKING_MST_FK=booking.BOOKING_MST_PK";
                Strsql += " and job.consignee_cust_mst_fk=consg.customer_mst_fk";
            }

            try
            {
                return Objwk.GetDataSet(Strsql);
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

        #endregion

        #region "Fetch Consol invoice Custumer "
        public DataSet CONSOL_INV_CUST_PRINT(int Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with8 = objWK.MyCommand.Parameters;
                _with8.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with8.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with8.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with8.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_INV_CUST_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet CONSOL_DRAFT_INV_CUST_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type, int CurrFK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with9 = objWK.MyCommand.Parameters;
                _with9.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with9.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with9.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with9.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with9.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "CONSOL_INV_CUST_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FAC_DRAFT_INV_SUP_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with10 = objWK.MyCommand.Parameters;
                _with10.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with10.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with10.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with10.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with10.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "CONSOL_FAC_SUPP_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Consol invoice Details Report "
        public DataSet CONSOL_INV_DETAIL_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with11 = objWK.MyCommand.Parameters;
                _with11.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with11.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with11.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with11.Add("USER_NAME_IN", HttpContext.Current.Session["USER_NAME"]).Direction = ParameterDirection.Input;
                _with11.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                
                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_DETAILS_MAIN_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet CONSOL_DRAFT_INV_DETAIL_MAIN_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type, int Loc_fk = 0, int CurrFK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with12 = objWK.MyCommand.Parameters;
                _with12.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with12.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with12.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with12.Add("USER_NAME_IN", HttpContext.Current.Session["USER_NAME"]).Direction = ParameterDirection.Input;
                _with12.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with12.Add("LOG_IN_LOC_FK_IN", Loc_fk).Direction = ParameterDirection.Input;
                _with12.Add("CURRENCY_FK_IN", CurrFK).Direction = ParameterDirection.Input;
                _with12.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "CONSOL_DETAILS_MAIN_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FAC_DRAFT_INV_DETAIL_MAIN_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with13 = objWK.MyCommand.Parameters;
                _with13.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with13.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with13.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with13.Add("USER_NAME_IN", HttpContext.Current.Session["USER_NAME"]).Direction = ParameterDirection.Input;
                _with13.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with13.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "CONSOL_DET_FAC_MAIN_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Consol invoice Details Report "
        public DataSet CONSOL_INV_DETAIL_MAIN_PRINTTPN(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with14 = objWK.MyCommand.Parameters;
                _with14.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with14.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with14.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with14.Add("USER_NAME_IN", HttpContext.Current.Session["USER_NAME"]).Direction = ParameterDirection.Input;
                _with14.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "TPNCONSOL_RPTPRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Consol invoice Sub Details Report "
        public DataSet CONSOL_INV_DETAIL_SUB_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with15 = objWK.MyCommand.Parameters;
                _with15.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with15.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with15.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with15.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_SUB_MAIN_RPT_PRINT");
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
        public DataSet CONSOL_DRAFT_INV_DETAIL_SUB_MAIN_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type, int Loc_fk = 0, int CurrFK = 0, string ContPKS = "", int Log_Curr_fk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with16 = objWK.MyCommand.Parameters;
                _with16.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with16.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with16.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with16.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with16.Add("LOG_IN_LOC_FK_IN", Loc_fk).Direction = ParameterDirection.Input;
                _with16.Add("CURRENCY_FK_IN", CurrFK).Direction = ParameterDirection.Input;
                _with16.Add("CONTAINER_FKS_IN", ContPKS).Direction = ParameterDirection.Input;
                _with16.Add("LOGIN_CUR_FK_IN", Log_Curr_fk).Direction = ParameterDirection.Input;
                _with16.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "CONSOL_SUB_MAIN_RPT_PRINT");
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
        public DataSet FAC_DRAFT_INV_DETAIL_SUB_MAIN_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type, int CurrFK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with17 = objWK.MyCommand.Parameters;
                _with17.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with17.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with17.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with17.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with17.Add("CURRENCY_FK_IN", CurrFK).Direction = ParameterDirection.Input;
                _with17.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "CONSOL_SUB_MAIN_FAC_RPT_PRINT");
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
        public DataSet CONSOL_DRAFT_AIF_DETAIL_SUB_MAIN_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type, int Loc_fk = 0, int CurrFK = 0, string ContPKS = "", int Log_Curr_fk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with18 = objWK.MyCommand.Parameters;
                _with18.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with18.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with18.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with18.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with18.Add("LOG_IN_LOC_FK_IN", Loc_fk).Direction = ParameterDirection.Input;
                _with18.Add("CURRENCY_FK_IN", CurrFK).Direction = ParameterDirection.Input;
                _with18.Add("CONTAINER_FKS_IN", ContPKS).Direction = ParameterDirection.Input;
                _with18.Add("LOGIN_CUR_FK_IN", Log_Curr_fk).Direction = ParameterDirection.Input;
                _with18.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "CONSOL_SUB_AIF_RPT_PRINT");
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
        public DataSet TPNCONSOL_INV_DETAIL_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with19 = objWK.MyCommand.Parameters;
                _with19.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with19.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with19.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with19.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "TPNCONSOL_SUB_PRINT");
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
        public DataSet TPNCONSOL_INV_DETAIL_AIF_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with20 = objWK.MyCommand.Parameters;
                _with20.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with20.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with20.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with20.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "TPNCONSOL_SUB_AIF_RPT_PRINT");
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
        #endregion

        #region "Fetch Consol invoice Report "
        public DataSet INV_DETAIL_PRINT(string nInvPK, int BizType, int ProcessType)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT INV.CONSOL_INVOICE_PK,");
            sb.Append("       INV.INVOICE_REF_NO,");
            sb.Append("       INV.INVOICE_DATE,");
            sb.Append("       CMST.CURRENCY_ID,");
            sb.Append("       sum(INVT.ELEMENT_AMT * INVT.EXCHANGE_RATE) TOTAMT,");
            sb.Append("       sum(NVL(INVT.TAX_AMT,0)) TAX_AMT,");
            sb.Append("       SUM(DISTINCT(NVL(INV.DISCOUNT_AMT, 0))) DICSOUNT,");
            sb.Append("       SUM(DISTINCT(NVL(INV.NET_RECEIVABLE, 0))) NET_INV_AMT,");
            sb.Append("       INVT.REMARKS,INV.INVOICE_DUE_DATE");
            sb.Append("  FROM CONSOL_INVOICE_TBL     INV,");
            sb.Append("       CURRENCY_TYPE_MST_TBL     CMST,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL INVT");
            sb.Append(" WHERE CMST.CURRENCY_MST_PK = INV.CURRENCY_MST_FK");
            sb.Append("   AND INVT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK");
            sb.Append("   AND INV.CONSOL_INVOICE_PK =" + nInvPK + "");
            sb.Append("   AND INV.BUSINESS_TYPE=" + BizType + "");
            sb.Append("   AND INV.PROCESS_TYPE=" + ProcessType + "");
            sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
            sb.Append("          INV.INVOICE_REF_NO,");
            sb.Append("          CMST.CURRENCY_ID,");
            sb.Append("          INVT.REMARKS,");
            sb.Append("          INV.INVOICE_DATE,");
            sb.Append("          INV.INVOICE_DUE_DATE");
            try
            {
                return (objWK.GetDataSet(sb.ToString()));
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
        #endregion

        #region "Fetch Consol invoice Curr Details Report "
        public DataSet CONSOL_INV_DETAIL_CURR_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with21 = objWK.MyCommand.Parameters;
                _with21.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with21.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with21.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with21.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_CUR_MAIN_RPT_PRINT");

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
        public DataSet CONSOL_DRAFT_INV_DETAIL_CURR_MAIN_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type, int CurrFK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with22 = objWK.MyCommand.Parameters;
                _with22.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with22.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with22.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with22.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with22.Add("CURRENCY_FK_IN", CurrFK).Direction = ParameterDirection.Input;
                _with22.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "CONSOL_CUR_MAIN_RPT_PRINT");

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
        public DataSet CONSOL_INV_DETAIL_AIF_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with23 = objWK.MyCommand.Parameters;
                _with23.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with23.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with23.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with23.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_SUB_AIF_RPT_PRINT");
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
        #endregion

        #region "Fetch Location"
        public DataSet FetchLocation(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK,L.VAT_NO");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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
        public DataSet FetchLocationNew(long USERPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,EMT.EMAIL_ID E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST,EMPLOYEE_MST_TBL  EMT,USER_MST_TBL      UMT");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND UMT.USER_MST_PK = " + USERPK + "");
            StrSqlBuilder.Append("  AND EMT.LOCATION_MST_FK = L.LOCATION_MST_PK(+)");
            StrSqlBuilder.Append("  AND UMT.EMPLOYEE_MST_FK=EMT.EMPLOYEE_MST_PK");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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
        #endregion

        #region "Fetch Function for Bank"
        public DataSet BankDetails(Int64 BankPK)
        {

            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                ObjWk.OpenConnection();
                ObjWk.MyCommand.Connection = ObjWk.MyConnection;
                var _with24 = ObjWk.MyCommand;
                _with24.CommandType = CommandType.StoredProcedure;
                _with24.CommandText = ObjWk.MyUserName + ".CONSOL_INV_PKG.FETCH_BANK_REPORT";
                ObjWk.MyCommand.Parameters.Clear();
                var _with25 = ObjWk.MyCommand.Parameters;
                _with25.Add("BANK_PK_IN", BankPK).Direction = ParameterDirection.Input;
                _with25.Add("BANK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                ObjWk.MyDataAdapter.SelectCommand = ObjWk.MyCommand;
                ObjWk.MyDataAdapter.Fill(dsData);
                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion


        #region "Fetch Function for BankDetailsforAgent"
        public DataSet BankDetailsforAgent(Int64 LocPK)
        {

            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                ObjWk.OpenConnection();
                ObjWk.MyCommand.Connection = ObjWk.MyConnection;
                var _with26 = ObjWk.MyCommand;
                _with26.CommandType = CommandType.StoredProcedure;
                _with26.CommandText = ObjWk.MyUserName + ".CONSOL_INV_PKG.FETCH_BANK_REPORT_AGENT";
                ObjWk.MyCommand.Parameters.Clear();
                var _with27 = ObjWk.MyCommand.Parameters;
                _with27.Add("BANK_PK_IN", LocPK).Direction = ParameterDirection.Input;
                _with27.Add("BANK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                ObjWk.MyDataAdapter.SelectCommand = ObjWk.MyCommand;
                ObjWk.MyDataAdapter.Fill(dsData);
                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Barcode Manager Pk"
        public int FetchBarCodeManagerPk(int BizType, int ProcType)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;
            StringBuilder strquery = new StringBuilder();
            try
            {
                WorkFlow objWF = new WorkFlow();

                strquery.Append(" Select a.bcd_mst_pk from barcode_data_mst_tbl a" );
                strquery.Append("                  where a.config_id_fk='QFOR4078'" );
                strquery.Append("                 and a.BCD_MST_FK= (select b.bcd_mst_pk from barcode_data_mst_tbl b " );

                if (BizType == 2 & ProcType == 1)
                {
                    strquery.Append("                   where b.field_name='Export Documentation' " );
                    strquery.Append("                     and b.BCD_MST_FK=2 ) " );
                }
                else if (BizType == 1 & ProcType == 1)
                {
                    strquery.Append("                   where b.field_name='Export Documentation' " );
                    strquery.Append("                     and b.BCD_MST_FK=1 ) " );
                }
                else if (BizType == 1 & ProcType == 2)
                {
                    strquery.Append("                   where b.field_name='Import Documentation' " );
                    strquery.Append("                     and b.BCD_MST_FK=1 ) " );


                }
                else if (BizType == 2 & ProcType == 2)
                {
                    strquery.Append("                   where b.field_name='Import Documentation' " );
                    strquery.Append("                     and b.BCD_MST_FK=2 ) " );
                }

                DsBarManager = objWF.GetDataSet(strquery.ToString());
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with28 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToInt32(_with28["bcd_mst_pk"]);
                }
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
        }
        #endregion

        #region "Fetch Barcode Type"
        public DataSet FetchBarCodeField(int BarCodeManagerPk)
        {
            try
            {
                string StrSql = null;
                DataSet DsBarManager = null;
                int strReturn = 0;
                WorkFlow objWF = new WorkFlow();
                StringBuilder strQuery = new StringBuilder();

                strQuery.Append("select distinct bdmt.bcd_mst_pk, bdmt.field_name, bdmt.default_value" );
                strQuery.Append("  from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt" );
                strQuery.Append(" where bdmt.bcd_mst_pk = bddt.bcd_mst_fk(+)" );
                strQuery.Append("   and bdmt.BCD_MST_FK= " + BarCodeManagerPk);
                strQuery.Append(" ORDER BY default_value desc" );

                DsBarManager = objWF.GetDataSet(strQuery.ToString());
                return DsBarManager;
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
        #endregion

        #region "Fetch Cus pk only"
        #endregion

        #region "Fetch Custumer pk And Jobcadpk "
        public DataSet Fetch_CustumerPk(int ConsPk)
        {
            try
            {
                DataSet DsCustPk = null;
                int strReturn = 0;
                WorkFlow objWF = new WorkFlow();
                StringBuilder strQuery = new StringBuilder();
                strQuery.Append("select distinct vb.job_card_fk, cv.customer_mst_fk, cv.CHK_INVOICE,VB.JOB_TYPE from consol_invoice_trn_tbl vb ,consol_invoice_tbl cv  " );
                strQuery.Append("where vb.consol_invoice_fk=" + ConsPk);
                strQuery.Append("and vb.consol_invoice_fk=cv.consol_invoice_pk" );
                DsCustPk = objWF.GetDataSet(strQuery.ToString());
                return DsCustPk;
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
        public DataSet FetchSuppPk(int ConsPk)
        {
            try
            {
                DataSet DsCustPk = null;
                int strReturn = 0;
                WorkFlow objWF = new WorkFlow();
                StringBuilder strQuery = new StringBuilder();
                strQuery.Append("select distinct CV.SUPPLIER_MST_FK, cv.CHK_INVOICE from consol_invoice_trn_tbl vb ,consol_invoice_tbl cv  " );
                strQuery.Append("where vb.consol_invoice_fk=" + ConsPk);
                strQuery.Append("and vb.consol_invoice_fk=cv.consol_invoice_pk" );
                DsCustPk = objWF.GetDataSet(strQuery.ToString());
                return DsCustPk;
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
        #endregion

        #region "Enhance Job Card Against Freight Payer  EXP"
        public string Enhance_JobCard_FreightPayer_EXP(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strProcessType = null;
            string strCustPk = null;

            string strloc = "";

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strCustPk = arr[4];
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_FRTPAYER_PKG.GET_JOB_FRTPAYER_EXP";

                var _with29 = selectCommand.Parameters;
                _with29.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with29.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with29.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with29.Add("LOCATION_IN", loc).Direction = ParameterDirection.Input;
                _with29.Add("CUSTOMER_PK_IN", (!string.IsNullOrEmpty(strCustPk.Trim()) ? strCustPk : "")).Direction = ParameterDirection.Input;
                _with29.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Original;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                char[] charbuff = null;
                strReturn = strReader.ReadToEnd();
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
        #endregion

        #region "Enhance Job Card Against Freight Payer IMP"
        public string Enhance_JobCard_FreightPayer_IMP(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strProcessType = null;
            string strloc = "";
            string strCustPk = null;

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strCustPk = arr[4];

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_FRTPAYER_PKG.GET_JOB_FRTPAYER_IMP";

                var _with30 = selectCommand.Parameters;
                _with30.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN :"")).Direction = ParameterDirection.Input;
                _with30.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with30.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with30.Add("LOCATION_IN", loc).Direction = ParameterDirection.Input;
                _with30.Add("CUSTOMER_PK_IN", (!string.IsNullOrEmpty(strCustPk) ? strCustPk : "")).Direction = ParameterDirection.Input;

                _with30.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Original;

                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                char[] charbuff = null;
                strReturn = strReader.ReadToEnd();

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
        #endregion

        #region "Fetch Job Card Against Freight Payer  CUSTUMER "
        public string Enhance_Custumer_FreightPayer(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            string businessType = "";
            string Import = "";
            string Consignee = "0";
            string strLOC_MST_IN = "";
            string strReq = null;
            string strJobPk = null;
            string strProcess = "";

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            if (arr.Length > 2)
                strCATEGORY_IN = arr[2];
            if (arr.Length > 3)
                strLOC_MST_IN = arr[3];
            if (arr.Length > 4)
                businessType = arr[4];

            if (arr.Length > 5)
                Consignee = "1";
            if (arr.Length > 6)
                Import = arr[6];
            if (arr.Length > 7)
                strJobPk = arr[7];
            if (arr.Length > 8)
                strProcess = arr[8];

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOBCARD_FRTPAYER_PKG.GETFRTPAYER_JOB";
                var _with31 = SCM.Parameters;
                _with31.Add("CATEGORY_IN", (!string.IsNullOrEmpty(strCATEGORY_IN) ? strCATEGORY_IN : "")).Direction = ParameterDirection.Input;
                _with31.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with31.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with31.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with31.Add("CONSIGNEE_IN", Consignee).Direction = ParameterDirection.Input;
                _with31.Add("BIZ_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with31.Add("IMPORT_IN", (!string.IsNullOrEmpty(Import.Trim()) ? Import : "")).Direction = ParameterDirection.Input;
                _with31.Add("PROCESS_TYPE_IN", strProcess).Direction = ParameterDirection.Input;
                _with31.Add("JOB_CARD_PK_IN", (!string.IsNullOrEmpty(strJobPk) ? strJobPk : "")).Direction = ParameterDirection.Input;
                _with31.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
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
                SCM.Connection.Close();
            }
        }
        #endregion

        #region "Fetch enhanced search against invoice pending exp"
        public string Enhance_Jobcard_PendingInvoice_Exp(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strProcessType = null;
            string strCustPk = null;

            string strloc = "";

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strCustPk = arr[4];

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_INVOICE_PENDING.GET_JOB_INVPNDNG_EXP";

                var _with32 = selectCommand.Parameters;
                _with32.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with32.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with32.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with32.Add("LOCATION_IN", loc).Direction = ParameterDirection.Input;
                _with32.Add("CUSTOMER_PK_IN", (!string.IsNullOrEmpty(strCustPk.Trim()) ? strCustPk : "")).Direction = ParameterDirection.Input;
                _with32.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Original;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                char[] charbuff = null;
                strReturn = strReader.ReadToEnd();
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
        #endregion

        #region "Fetch enhanced search against invoice pening imp"
        public string Enhance_Jobcard_PendingInvoice_Imp(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strProcessType = null;
            string strloc = "";
            string strCustPk = null;

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strCustPk = arr[4];

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_INVOICE_PENDING.GET_JOB_INVPNDNG_IMP";

                var _with33 = selectCommand.Parameters;
                _with33.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with33.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with33.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with33.Add("LOCATION_IN", loc).Direction = ParameterDirection.Input;
                _with33.Add("CUSTOMER_PK_IN", (!string.IsNullOrEmpty(strCustPk) ? strCustPk : "")).Direction = ParameterDirection.Input;

                _with33.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Original;

                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                char[] charbuff = null;
                strReturn = strReader.ReadToEnd();

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
        #endregion

        #region "fetch_Cutumer_pk "
        public DataSet fetch_Cust_pk(string pk, string hblpk, string process, string biztype)
        {
            StringBuilder strQuery = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                if ((pk != "0") & (!string.IsNullOrEmpty(pk)))
                {
                    strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name" );
                    if (process == "1" & biztype == "1")
                    {
                        strQuery.Append("  from JOB_CARD_TRN j, customer_mst_tbl cmt" );
                        strQuery.Append("    where j.JOB_CARD_TRN_PK = " + pk);
                        strQuery.Append(" and j.shipper_cust_mst_fk = cmt.customer_mst_pk" );
                    }
                    if (process == "2" & biztype == "1")
                    {
                        strQuery.Append("  from JOB_CARD_TRN j, customer_mst_tbl cmt" );
                        strQuery.Append("    where j.JOB_CARD_TRN_PK = " + pk);
                        strQuery.Append(" and j.consignee_cust_mst_fk = cmt.customer_mst_pk" );
                    }
                    if (process == "1" & biztype == "2")
                    {
                        strQuery.Append("  from JOB_CARD_TRN j, customer_mst_tbl cmt" );
                        strQuery.Append("    where j.JOB_CARD_TRN_PK = " + pk);
                        strQuery.Append(" and j.shipper_cust_mst_fk = cmt.customer_mst_pk" );
                    }
                    if (process == "2" & biztype == "2")
                    {
                        strQuery.Append("  from JOB_CARD_TRN j, customer_mst_tbl cmt" );
                        strQuery.Append("   where j.JOB_CARD_TRN_PK = " + pk);
                        strQuery.Append(" and j.consignee_cust_mst_fk = cmt.customer_mst_pk" );
                    }
                    strQuery.Append("" );
                }
                else
                {
                    if ((hblpk != "0") & (!string.IsNullOrEmpty(hblpk)))
                    {
                        if (process == "1" & biztype == "1")
                        {
                            strQuery.Append(" select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name" );
                            strQuery.Append(" from customer_mst_tbl cmt, hawb_exp_tbl HET " );
                            strQuery.Append(" WHERE   HET.HAWB_EXP_TBL_PK  =" + hblpk);
                            strQuery.Append(" and HET.shipper_cust_mst_fk = cmt.customer_mst_pk" );
                        }
                        if (process == "1" & biztype == "2")
                        {
                            strQuery.Append(" select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name" );
                            strQuery.Append(" from customer_mst_tbl cmt, HBL_EXP_TBL HBE" );
                            strQuery.Append(" WHERE HBE.HBL_EXP_TBL_PK =" + hblpk);
                            strQuery.Append(" and hbe.shipper_cust_mst_fk = cmt.customer_mst_pk" );

                        }
                    }

                }
                if ((!string.IsNullOrEmpty(strQuery.ToString())))
                {
                    return objWF.GetDataSet(strQuery.ToString());
                }
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
            return new DataSet();
        }
        #endregion

        #region "Export to XML"
        public DataSet Export2XML(string InvPK = "0", string PayDueDt = "", short CustOrAgent = 0, short BizType = 0, short ProcessType = 0, int IsFACInv = 0)
        {

            WorkFlow objWF = new WorkFlow();
            string InvHdrCaption = "INVOICE";
            string InvDtlCaption = "INVOICE_DETAILS";
            string JobHdrCaption = "JOBCARD";
            string JobDtlCaption = "JOBCARD_DETAILS";
            DataTable dtInv = null;
            DataTable dtInvdet = null;
            DataTable dtJcHead = null;
            DataTable dtJcDet = null;
            DataSet MainDs = new DataSet();

            try
            {
                if (IsFACInv == 1)
                {
                    dtInv = getInvHeader(InvPK, PayDueDt, BizType, ProcessType, IsFACInv);
                    dtInvdet = getInvDetailsFAC(InvPK, BizType, ProcessType);
                    dtJcHead = getJcHeader(InvPK, BizType, ProcessType, IsFACInv);
                    dtJcDet = getJcDetailsFAC(InvPK, BizType, ProcessType);
                }
                else if (CustOrAgent == 0)
                {
                    dtInv = getInvHeader(InvPK, PayDueDt, BizType, ProcessType, IsFACInv);
                    dtInvdet = getInvDetails(InvPK, BizType, ProcessType);
                    dtJcHead = getJcHeader(InvPK, BizType, ProcessType, IsFACInv);
                    dtJcDet = getJcDetails(InvPK, BizType, ProcessType);
                }
                else
                {
                    dtInv = GetInvAgentHdr(InvPK, BizType, ProcessType);
                    dtInvdet = GetInvAgentDtl(InvPK, BizType, ProcessType);
                    dtJcHead = GetAgentJobHdr(InvPK, BizType, ProcessType);
                    dtJcDet = GetAgentJobDtl(InvPK, BizType, ProcessType);
                }

                MainDs.Tables.Add(dtInv);
                MainDs.Tables.Add(dtInvdet);
                MainDs.Tables.Add(dtJcHead);
                MainDs.Tables.Add(dtJcDet);

                MainDs.Tables[0].TableName = InvHdrCaption;
                MainDs.Tables[1].TableName = InvDtlCaption;
                MainDs.Tables[2].TableName = JobHdrCaption;
                MainDs.Tables[3].TableName = JobDtlCaption;

                DataRelation relInv_InvDet = new DataRelation("relInv", new DataColumn[] {
                    MainDs.Tables[InvHdrCaption].Columns["INVPK"],
                    MainDs.Tables[InvHdrCaption].Columns["JCPK"]
                }, new DataColumn[] {
                    MainDs.Tables[InvDtlCaption].Columns["INVPK"],
                    MainDs.Tables[InvDtlCaption].Columns["JCPK"]
                });
                DataRelation relInv_Jc = new DataRelation("relInvJc", new DataColumn[] {
                    MainDs.Tables[InvHdrCaption].Columns["INVPK"],
                    MainDs.Tables[InvHdrCaption].Columns["JCPK"]
                }, new DataColumn[] {
                    MainDs.Tables[JobHdrCaption].Columns["INVPK"],
                    MainDs.Tables[JobHdrCaption].Columns["JCPK"]
                });
                DataRelation relJc_JcDet = new DataRelation("relJcJcDet", new DataColumn[] {
                    MainDs.Tables[JobHdrCaption].Columns["INVPK"],
                    MainDs.Tables[JobHdrCaption].Columns["JCPK"]
                }, new DataColumn[] {
                    MainDs.Tables[JobDtlCaption].Columns["INVPK"],
                    MainDs.Tables[JobDtlCaption].Columns["JCPK"]
                });

                relInv_InvDet.Nested = true;
                relInv_Jc.Nested = true;
                relJc_JcDet.Nested = true;

                MainDs.Relations.Add(relInv_InvDet);
                MainDs.Relations.Add(relInv_Jc);
                MainDs.Relations.Add(relJc_JcDet);

                MainDs.DataSetName = "INVOICEDETAILS";

                try
                {
                    var _with34 = MainDs;
                    _with34.Tables[InvHdrCaption].Columns["INVPK"].ColumnMapping = MappingType.Hidden;
                     _with34.Tables[InvDtlCaption].Columns["INVPK"].ColumnMapping = MappingType.Hidden;
                     _with34.Tables[JobHdrCaption].Columns["INVPK"].ColumnMapping = MappingType.Hidden;
                     _with34.Tables[JobDtlCaption].Columns["INVPK"].ColumnMapping = MappingType.Hidden;

                    _with34.Tables[InvHdrCaption].Columns["JCPK"].ColumnMapping = MappingType.Hidden;
                     _with34.Tables[InvDtlCaption].Columns["JCPK"].ColumnMapping = MappingType.Hidden;
                    _with34.Tables[JobHdrCaption].Columns["JCPK"].ColumnMapping = MappingType.Hidden;
                    _with34.Tables[JobDtlCaption].Columns["JCPK"].ColumnMapping = MappingType.Hidden;

                    _with34.Tables[JobDtlCaption].Columns["REF_NR"].ColumnMapping = MappingType.Hidden;
                    _with34.Tables[InvDtlCaption].Columns["REF_NR"].ColumnMapping = MappingType.Hidden;
                    _with34.Tables[JobHdrCaption].Columns["REF_NR"].ColumnMapping = MappingType.Hidden;
                    _with34.Tables[JobDtlCaption].Columns["REF_NR"].ColumnMapping = MappingType.Hidden;

                    _with34.Tables[InvDtlCaption].Columns["JCREF_NR"].ColumnMapping = MappingType.Hidden;
                    _with34.Tables[JobDtlCaption].Columns["JCREF_NR"].ColumnMapping = MappingType.Hidden;                   

                    _with34.AcceptChanges();
                }
                catch (Exception ex)
                {
                }
                return MainDs;
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

        public DataTable getInvHeader(string InvPK = "0", string dueDate = "", short BizType = 0, short ProcessType = 0, int IsFACInv = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string BIZ = (BizType == 2 ? "SEA" : "AIR");
            string PROCESS = (ProcessType == 1 ? "EXP" : "IMP");
            StringBuilder sb = new StringBuilder();

            sb.Append("select distinct inv.consol_invoice_pk INVPK,");
            sb.Append("                INVtr.Job_Card_Fk JCPK,");
            sb.Append("                inv.invoice_ref_no invoice_nr,inv.INV_UNIQUE_REF_NR BANK_REF_NR,");
            sb.Append("                to_char(inv.invoice_date, 'dd-MON-yyyy') invoice_date,");
            sb.Append("                to_char(nvl(jcse.departure_date, jcse.etd_date), 'dd-MON-yyyy') activity_date,");

            sb.Append("to_char(to_date('" + dueDate + "', 'dd/mm/yyyy'),'dd-MON-yyyy') invoice_actual_due_date,");
            sb.Append("to_char(to_date('" + dueDate + "', 'dd/mm/yyyy'),'dd-MON-yyyy') invoice_adjusted_due_date,");
            sb.Append("                INV_UNIQUE_REF_NR unique_ref_nr,");
            sb.Append("                '" + PROCESS + "' process_type,");
            sb.Append("                '" + BIZ + "' business_type,");
            sb.Append("                ' ' ocr_no,");
            if (IsFACInv == 1)
            {
                sb.Append("                 OPR.OPERATOR_ID Supplier,");
            }
            else
            {
                sb.Append("                cust.customer_id customer,");
            }
            sb.Append("                ' ' AGENT,");
            if (IsFACInv == 1)
            {
                sb.Append("                'SUPPLIER' PARTY_TYPE,");
            }
            else
            {
                sb.Append("                'CUSTOMER' PARTY_TYPE,");
            }
            sb.Append("                nvl(shmpt.cargo_move_code, ' ') shipping_terms,");
            sb.Append("                'AS PER CONTRACT' payment_terms,");
            sb.Append("                currcorp.currency_id base_currency,");
            sb.Append("                curr.currency_id invoice_currency,");
            sb.Append("                'CONTAINER' shipment,");
            sb.Append("                nvl(inv.remarks, ' ') remarks,");
            if (BizType == 2)
            {
                sb.Append("                jcse.vessel_name VSL_FLIGHT,");
                sb.Append("                jcse.VOYAGE_FLIGHT_NO VOYAGE_FLIGHT_NR,");
            }
            else
            {
                sb.Append("                NULL VSL_FLIGHT,");
                sb.Append("                JCSE.VOYAGE_FLIGHT_NO VOYAGE_FLIGHT_NR,");
            }
            sb.Append("                'GENERAL' roe_basis,");
            sb.Append("                'STANDARD' roe_type,");
            sb.Append("                get_ex_rate(inv.currency_mst_fk,");
            sb.Append("                            currcorp.currency_mst_pk,");
            sb.Append("                            inv.invoice_date) roe_amount,");
            sb.Append("                DECODE(INV.INV_TYPE,0,'Main Invoice',1,'Suppleimentry Invoice') invoice_type,");
            sb.Append("                'RECORD1' ref_nr");
            sb.Append("  from consol_invoice_tbl     inv,");
            sb.Append("       JOB_CARD_TRN      jcse,");
            sb.Append("       currency_type_mst_tbl     curr,");
            sb.Append("       currency_type_mst_tbl     currcorp,");
            sb.Append("       consol_invoice_trn_tbl invtr,");
            if (IsFACInv == 1)
            {
                sb.Append("       OPERATOR_MST_TBL          OPR,");
            }
            else
            {
                sb.Append("       CUSTOMER_MST_TBL          cust,");
            }
            sb.Append("       cargo_move_mst_tbl   shmpt,");
            sb.Append("       AGENT_MST_TBL             AGT,");
            sb.Append("       AGENT_MST_TBL             AGTDP");
            sb.Append("  where jcse.JOB_CARD_TRN_PK = invtr.job_card_fk");
            sb.Append("   and inv.currency_mst_fk = curr.currency_mst_pk");
            sb.Append("   and invtr.consol_invoice_fk = inv.consol_invoice_pk");
            if (IsFACInv == 1)
            {
                sb.Append("    AND OPR.OPERATOR_MST_PK = INV.SUPPLIER_MST_FK ");
            }
            else
            {
                sb.Append("   and cust.customer_mst_pk = inv.customer_mst_fk");
            }
            sb.Append("   and jcse.cargo_move_fk = shmpt.cargo_move_pk(+)");
            sb.Append("   and currcorp.currency_mst_pk=" + HttpContext.Current.Session["currency_mst_pk"]);
            sb.Append("   and inv.consol_invoice_pk in ( " + InvPK + ")");
            sb.Append("   AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");

            if (ProcessType == 1)
            {
                sb.Append("   AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
            }
            else
            {
                sb.Append("   AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
            }

            string Main_Query = sb.ToString().ToUpper();
            if (BizType == 1)
            {
                Main_Query = Main_Query.Replace("SEA", "AIR");
            }
            if (ProcessType == 2)
            {
                Main_Query = Main_Query.Replace("EXP", "IMP");
            }
            try
            {
                return objWF.GetDataTable(Main_Query);
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
        public DataTable getInvDetails(string InvPK = "0", short BizType = 0, short ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            StringBuilder VAT_Str = new StringBuilder();
            VAT_Str.Append(" NVL((select Distinct (frtv.vat_percentage)");
            VAT_Str.Append("      from frt_vat_country_tbl frtv,");
            VAT_Str.Append("       user_mst_tbl        umt,");
            VAT_Str.Append("       location_mst_tbl    loc");
            VAT_Str.Append("      where umt.default_location_fk =loc.location_mst_pk ");
            VAT_Str.Append("       and loc.country_mst_fk = frtv.country_mst_fk");
            VAT_Str.Append("       and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
            VAT_Str.Append("       and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)),");
            VAT_Str.Append("     CORP.VAT_PERCENTAGE) ");

            if (BizType == 2)
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (select distinct inv.consol_invoice_pk INVPK,");
                sb.Append("                        INVTRN.JOB_CARD_FK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                        cntr.container_type_mst_id CONTAINER_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR *");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date),");
                sb.Append("                               2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        DECODE(FMT.CHARGE_BASIS,1,'%',2,'Flat Rate',3,'Kgs',4,'Unit') CHARGE_BASIS,");
                sb.Append("                        'OPEN' COLLECT_STATUS,");
                sb.Append("                        'F' ADDITIONALCHARGES");
                sb.Append("          from consol_invoice_trn_tbl invtrn,");
                sb.Append("               consol_invoice_tbl inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               JOB_CARD_TRN jcse,");
                sb.Append("               JOB_TRN_FD JOBFRT,");
                sb.Append("               JOB_TRN_CONT jccntr,");
                sb.Append("               CORPORATE_MST_TBL CORP,");
                sb.Append("               container_type_mst_tbl cntr ");

                sb.Append("         where inv.consol_invoice_pk in( " + InvPK + ")");
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
                sb.Append("           AND invtrn.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           and jobfrt.container_type_mst_fk = cntr.container_type_mst_pk(+)");

                sb.Append("           AND JOBFRT.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT = 1");
                sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("           AND JOBFRT.JOB_CARD_TRN_FK = JCSE.JOB_CARD_TRN_PK");

                sb.Append("        union");

                sb.Append("        select distinct inv.consol_invoice_pk INVPK,");
                sb.Append("                        INVTRN.JOB_CARD_FK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                        'Other' CONTAINER_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR *");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date),");
                sb.Append("                               2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        DECODE(FMT.CHARGE_BASIS,1,'%',2,'Flat Rate',3,'Kgs',4,'Unit') CHARGE_BASIS,");
                sb.Append("                        'OPEN' COLLECT_STATUS,");
                sb.Append("                        'T' ADDITIONALCHARGES");
                sb.Append("          from consol_invoice_trn_tbl invtrn,");
                sb.Append("               consol_invoice_tbl inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               JOB_CARD_TRN jcse,");
                sb.Append("               JOB_TRN_OTH_CHRG joboth,");

                sb.Append("               CORPORATE_MST_TBL CORP");
                sb.Append("         where inv.consol_invoice_pk in ( " + InvPK + ")");
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.JOB_CARD_TRN_PK = invTRN.Job_Card_Fk");
                sb.Append("           AND invtrn.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");

                sb.Append("           AND joboth.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           AND joboth.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND JOBOTH.INV_AGENT_TRN_FK IS NULL");
                sb.Append("           AND JOBOTH.INV_CUST_TRN_FK IS NULL");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT = 2");
                sb.Append("           AND JOBOTH.Freight_Type IN (1, 2)");
                sb.Append("           AND JOBOTH.JOB_CARD_TRN_FK = JCSE.JOB_CARD_TRN_PK");

                sb.Append("           ) Q");
            }
            else
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (select distinct inv.consol_invoice_pk INVPK,");
                sb.Append("                        INVTRN.JOB_CARD_FK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                        ' ' ULD_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR *");
                sb.Append("                               GET_EX_RATE(inv.currency_mst_fk,");
                sb.Append("                                           corp.currency_mst_fk,");
                sb.Append("                                           inv.invoice_date),");
                sb.Append("                               2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        DECODE(FMT.CHARGE_BASIS,1,'%',2,'Flat Rate',3,'Kgs',4,'Unit') CHARGE_BASIS,");
                sb.Append("                        'OPEN' COLLECT_STATUS,");
                sb.Append("                        'F' ADDITIONALCHARGES");
                sb.Append("          from consol_invoice_trn_tbl invtrn,");
                sb.Append("               consol_invoice_tbl inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               JOB_CARD_TRN jcse,");
                sb.Append("               JOB_TRN_FD JOBFRT,");
                sb.Append("               CORPORATE_MST_TBL CORP ");
                sb.Append("         where inv.consol_invoice_pk in( " + InvPK + ")");
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
                sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBFRT.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT = 1");
                sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("           AND JOBFRT.JOB_CARD_TRN_FK = JCSE.JOB_CARD_TRN_PK");

                sb.Append("        union");
                sb.Append("        select distinct inv.consol_invoice_pk INVPK,");
                sb.Append("                        INVTRN.JOB_CARD_FK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                        'Other' ULD_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR *");
                sb.Append("                               GET_EX_RATE(inv.currency_mst_fk,");
                sb.Append("                                           corp.currency_mst_fk,");
                sb.Append("                                           inv.invoice_date),");
                sb.Append("                               2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        DECODE(FMT.CHARGE_BASIS,1,'%',2,'Flat Rate',3,'Kgs',4,'Unit') CHARGE_BASIS,");
                sb.Append("                        'OPEN' COLLECT_STATUS,");
                sb.Append("                        'T' ADDITIONALCHARGES");
                sb.Append("          from consol_invoice_trn_tbl invtrn,");
                sb.Append("               consol_invoice_tbl inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               JOB_CARD_TRN jcse,");
                sb.Append("               JOB_TRN_OTH_CHRG joboth,");
                sb.Append("               CORPORATE_MST_TBL CORP ");
                sb.Append("         where inv.consol_invoice_pk in ( " + InvPK + ")");
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.JOB_CARD_TRN_PK = invTRN.Job_Card_Fk");
                sb.Append("           AND invtrn.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND JOBOTH.INV_AGENT_TRN_FK IS NULL");
                sb.Append("           AND JOBOTH.INV_CUST_TRN_FK IS NULL");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT = 2");
                sb.Append("           AND JOBOTH.Freight_Type IN (1, 2)");
                sb.Append("           AND JOBOTH.JOB_CARD_TRN_FK = JCSE.JOB_CARD_TRN_PK");

                sb.Append("           ) Q");
            }

            string Main_Query = sb.ToString().ToUpper();
            if (BizType == 1)
            {
                //Main_Query = Main_Query.Replace("SEA", "AIR")
            }
            if (ProcessType == 2)
            {
                //Main_Query = Main_Query.Replace("EXP", "IMP")
            }
            try
            {
                return objWF.GetDataTable(Main_Query);
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
        public DataTable getInvDetailsFAC(string InvPK = "0", short BizType = 0, short ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            StringBuilder VAT_Str = new StringBuilder();
            VAT_Str.Append(" NVL((select Distinct (frtv.vat_percentage)");
            VAT_Str.Append("      from frt_vat_country_tbl frtv,");
            VAT_Str.Append("       user_mst_tbl        umt,");
            VAT_Str.Append("       location_mst_tbl    loc");
            VAT_Str.Append("      where umt.default_location_fk =loc.location_mst_pk ");
            VAT_Str.Append("       and loc.country_mst_fk = frtv.country_mst_fk");
            VAT_Str.Append("       and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
            VAT_Str.Append("       and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)),");
            VAT_Str.Append("     CORP.VAT_PERCENTAGE) ");

            if (BizType == 2)
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (select distinct inv.consol_invoice_pk INVPK,");
                sb.Append("                        INVTRN.JOB_CARD_FK JCPK,");
                sb.Append("                        (SELECT CEMT.COST_ELEMENT_ID  FROM COST_ELEMENT_MST_TBL CEMT, PARAMETERS_TBL P");
                sb.Append("                        WHERE CEMT.COST_ELEMENT_MST_PK = P.FRT_FAC_FK) CHARGE_CODE,");
                sb.Append("                        (SELECT CEMT.COST_ELEMENT_NAME  FROM COST_ELEMENT_MST_TBL CEMT, PARAMETERS_TBL P");
                sb.Append("                        WHERE CEMT.COST_ELEMENT_MST_PK = P.FRT_FAC_FK) CHARGE_DESC,");
                sb.Append("                        '' CONTAINER_TYPE,");

                sb.Append("'" + HttpContext.Current.Session["CURRENCY_ID"] + "' TRANSACTION_CURRENCY,");
                sb.Append("                        '' VAT_PERCENTAGE,");
                sb.Append("                        '' VAT_TYPE,");
                sb.Append("                        '' VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append(" " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
                sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR *");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append(" " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                    inv.invoice_date),");
                sb.Append("                               2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                       '' CHARGE_BASIS,");
                sb.Append("                        'OPEN' COLLECT_STATUS,");
                sb.Append("                        'F' ADDITIONALCHARGES");
                sb.Append("          from consol_invoice_trn_tbl invtrn,");
                sb.Append("               consol_invoice_tbl inv,");
                sb.Append("               JOB_CARD_TRN jcse ");
                sb.Append("         where inv.consol_invoice_pk in( " + InvPK + ")");
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
                sb.Append("           AND INV.IS_FAC_INV = 1) Q");
            }

            string Main_Query = sb.ToString().ToUpper();
            if (BizType == 1)
            {
                //Main_Query = Main_Query.Replace("SEA", "AIR")
            }
            if (ProcessType == 2)
            {
                //Main_Query = Main_Query.Replace("EXP", "IMP")
            }
            try
            {
                return objWF.GetDataTable(Main_Query);
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
        public DataTable getJcHeader(string InvPK = "0", short BizType = 0, short ProcessType = 0, int IsFACInv = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string BIZ = (BizType == 2 ? "SEA" : "AIR");
            string PROCESS = (ProcessType == 1 ? "EXP" : "IMP");
            StringBuilder sb = new StringBuilder();
            if (BizType == 2)
            {
                sb.Append("SELECT distinct INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("       INVtrn.Job_Card_Fk JCPK,");
                sb.Append("       TO_CHAR(JCSE.JOBCARD_DATE,'DD-MON-YYYY') SALES_DATE,");
                sb.Append("       to_char(nvl(jcse.departure_date, jcse.etd_date), 'dd-MON-yyyy') SALES_ACT_DATE,");
                sb.Append("       '" + BIZ + "' BUSINESS_TYPE,");
                if (ProcessType == 1)
                {
                    sb.Append("       nvl(POR.PLACE_CODE, ' ') POR,");
                }
                else
                {
                    sb.Append("       NULL POR,");
                }
                sb.Append("       nvl(POL.PORT_ID, ' ') POL,");
                sb.Append("       nvl(POD.PORT_ID, ' ') POD,");
                sb.Append("       nvl(PLD.PLACE_CODE, ' ') PFD,");
                if (IsFACInv == 1)
                {
                    sb.Append("        OPR.OPERATOR_ID  SUPPLIER, ");
                }
                else
                {
                    sb.Append("       CUST.CUSTOMER_ID CUSTOMER,");
                }
                sb.Append("       ' ' AGENT,");
                if (IsFACInv == 1)
                {
                    sb.Append("       'SUPPLIER' PARTY_TYPE,");
                }
                else
                {
                    sb.Append("       'CUSTOMER' PARTY_TYPE,");
                }
                sb.Append("       nvl(JCSE.VESSEL_NAME, ' ') VSL_FLIGHT,");
                sb.Append("       NVL(JCSE.VOYAGE_FLIGHT_NO,' ') VOYAGE_FLIGHT_NR,");
                sb.Append("       CURR.CURRENCY_ID BASE_CURRENCY,");
                sb.Append("       '" + PROCESS + "' PROCESS_TYPE,");
                if (ProcessType == 1)
                    sb.Append(" BKG.BOOKING_REF_NO BOOKING_REF_NO,");
                else
                    sb.Append(" NULL BOOKING_REF_NO, ");
                if (ProcessType == 1)
                    sb.Append(" TO_CHAR(BKG.BOOKING_DATE, 'DD-MON-YYYY') BOOKING_DATE,");
                else
                    sb.Append(" NULL BOOKING_DATE,");
                sb.Append("       HBL.HBL_REF_NO BL_REF_NO,");
                sb.Append("       to_char(HBL.HBL_DATE, 'DD-MON-YYYY') BL_DATE,");
                sb.Append("       JCSE.JOBCARD_REF_NO JOBCARD_REF_NO,");
                sb.Append("       to_char(JCSE.JOBCARD_DATE, 'DD-MON-YYYY') JOB_CARD_DATE,");
                sb.Append("       'CONTAINER' SHIPMENT_TYPE,");
                sb.Append("       NVL(SHMT.CARGO_MOVE_CODE, ' ') SHIPPING_TERMS,");
                sb.Append("       'GENERAL' ROE_BASIS,");
                sb.Append("       'INVOICED' STATUS,");
                sb.Append("       'RECORD1' REF_NR");
                sb.Append("  FROM JOB_CARD_TRN   JCSE,");
                sb.Append("       consol_invoice_tbl   INV, consol_invoice_trn_tbl invtrn,");
                if (ProcessType == 1)
                {
                    sb.Append("       BOOKING_MST_TBL        BKG,");
                    sb.Append("       PLACE_MST_TBL          POR,");
                }
                sb.Append("       HBL_EXP_TBL            HBL,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       PLACE_MST_TBL          PLD,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CURR,");
                sb.Append("       CORPORATE_MST_TBL      CORP,");
                sb.Append("       cargo_move_mst_tbl SHMT,");
                sb.Append("       AGENT_MST_TBL          AGT,");
                sb.Append("       AGENT_MST_TBL          AGTDP,");
                if (IsFACInv == 1)
                {
                    sb.Append("        OPERATOR_MST_TBL   OPR ");
                }
                else
                {
                    sb.Append("       CUSTOMER_MST_TBL    CUST ");
                }
                sb.Append(" WHERE INVtrn.Job_Card_Fk = JCSE.JOB_CARD_TRN_PK ");
                sb.Append("   AND inv.consol_invoice_pk = invtrn.consol_invoice_fk");
                sb.Append("   AND INV.Consol_Invoice_Pk in ( " + InvPK + ")");
                sb.Append("   AND CORP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND SHMT.CARGO_MOVE_PK(+) = JCSE.Cargo_Move_Fk");
                if (IsFACInv == 1)
                {
                    sb.Append("    AND INV.SUPPLIER_MST_FK = OPR.OPERATOR_MST_PK ");
                }
                else
                {
                    sb.Append("   AND JCSE.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                }
                if (ProcessType == 1)
                {
                    sb.Append("   AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSE.JOB_CARD_TRN_PK");
                    sb.Append("   AND JCSE.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                    sb.Append("   AND BKG.COL_PLACE_MST_FK = POR.PLACE_PK(+)");
                    sb.Append("   AND BKG.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                    sb.Append("   AND BKG.PORT_MST_POD_FK = POL.PORT_MST_PK");
                    sb.Append("   AND BKG.PORT_MST_POL_FK = POD.PORT_MST_PK");
                    sb.Append("   AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("   AND HBL.HBL_REF_NO(+) = JCSE.HBL_HAWB_REF_NO");
                    sb.Append("   AND JCSE.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                    sb.Append("   AND JCSE.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND JCSE.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                sb.Append("   AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");

            }
            else
            {
                sb.Append("SELECT distinct INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("       INVtrn.Job_Card_Fk JCPK,");
                sb.Append("       TO_CHAR(JCSE.JOBCARD_DATE,'DD-MON-YYYY') SALES_DATE,");
                sb.Append("       to_char(nvl(jcse.departure_date, jcse.etd_date), 'dd-MON-yyyy') SALES_ACT_DATE,");
                sb.Append("       '" + BIZ + "' BUSINESS_TYPE,");
                if (ProcessType == 1)
                {
                    sb.Append("       nvl(POR.PLACE_CODE, ' ') POR,");
                }
                else
                {
                    sb.Append("       NULL POR,");
                }
                sb.Append("       nvl(POL.PORT_ID, ' ') POL,");
                sb.Append("       nvl(POD.PORT_ID, ' ') POD,");
                sb.Append("       nvl(PLD.PLACE_CODE, ' ') PFD,");
                if (IsFACInv == 1)
                {
                    sb.Append("   OPR.OPERATOR_ID  CUSTOMER, ");
                }
                else
                {
                    sb.Append("   CUST.CUSTOMER_ID CUSTOMER,");
                }
                sb.Append("       ' ' AGENT,");
                sb.Append("       'CUSTOMER' PARTY_TYPE,");
                sb.Append("       NULL VSL_FLIGHT,");
                sb.Append("       NVL(JCSE.VOYAGE_FLIGHT_NO,' ') VOYAGE_FLIGHT_NR, ");
                sb.Append("       CURR.CURRENCY_ID BASE_CURRENCY,");
                sb.Append("       '" + PROCESS + "' PROCESS_TYPE,");
                if (ProcessType == 1)
                    sb.Append(" BKG.BOOKING_REF_NO BOOKING_REF_NO,");
                else
                    sb.Append(" NULL BOOKING_REF_NO, ");
                if (ProcessType == 1)
                    sb.Append(" to_char(BKG.BOOKING_DATE, 'DD-MON-YYYY') BOOKING_DATE,");
                else
                    sb.Append(" NULL BOOKING_DATE,");
                sb.Append("       HAWB.HAWB_REF_NO BL_REF_NO,");
                sb.Append("       to_char(HAWB.HAWB_DATE, 'DD-MON-YYYY') BL_DATE,");
                sb.Append("       JCSE.JOBCARD_REF_NO JOBCARD_REF_NO,");
                sb.Append("       to_char(JCSE.JOBCARD_DATE, 'DD-MON-YYYY') JOB_CARD_DATE,");
                sb.Append("       'CONTAINER' SHIPMENT_TYPE,");
                sb.Append("       NVL(SHMT.CARGO_MOVE_CODE, ' ') SHIPPING_TERMS,");
                sb.Append("       'GENERAL' ROE_BASIS,");
                sb.Append("       'INVOICED' STATUS,");
                sb.Append("       'RECORD1' REF_NR");
                sb.Append("  FROM JOB_CARD_TRN   JCSE,");
                sb.Append("       consol_invoice_tbl   INV, consol_invoice_trn_tbl invtrn,");
                if (ProcessType == 1)
                {
                    sb.Append("       PLACE_MST_TBL          POR,");
                    sb.Append("       BOOKING_MST_TBL        BKG,");
                }
                sb.Append("       HAWB_EXP_TBL           HAWB,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       PLACE_MST_TBL          PLD,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CURR,");
                sb.Append("       CORPORATE_MST_TBL      CORP,");
                sb.Append("       cargo_move_mst_tbl SHMT,");
                sb.Append("       AGENT_MST_TBL          AGT,");
                sb.Append("       AGENT_MST_TBL          AGTDP,");
                if (IsFACInv == 1)
                {
                    sb.Append("        OPERATOR_MST_TBL   OPR ");
                }
                else
                {
                    sb.Append("       CUSTOMER_MST_TBL    CUST ");
                }
                sb.Append(" WHERE INVtrn.Job_Card_Fk = JCSE.JOB_CARD_TRN_PK ");
                sb.Append("   AND inv.consol_invoice_pk = invtrn.consol_invoice_fk");
                sb.Append("   AND INV.Consol_Invoice_Pk in ( " + InvPK + ")");
                sb.Append("   AND CORP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND SHMT.CARGO_MOVE_PK(+) = JCSE.Cargo_Move_Fk");
                if (IsFACInv == 1)
                {
                    sb.Append("    AND INV.SUPPLIER_MST_FK = OPR.OPERATOR_MST_PK ");
                }
                else
                {
                    sb.Append("   AND JCSE.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                }
                if (ProcessType == 1)
                {
                    sb.Append("   AND HAWB.JOB_CARD_SEA_EXP_FK(+) = JCSE.JOB_CARD_TRN_PK");
                    sb.Append("   AND JCSE.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                    sb.Append("   AND BKG.COL_PLACE_MST_FK = POR.PLACE_PK(+)");
                    sb.Append("   AND BKG.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                    sb.Append("   AND BKG.PORT_MST_POD_FK = POL.PORT_MST_PK");
                    sb.Append("   AND BKG.PORT_MST_POL_FK = POD.PORT_MST_PK");
                    sb.Append("   AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("   AND HAWB.HAWB_REF_NO(+) = JCSE.HBL_HAWB_REF_NO");
                    sb.Append("   AND JCSE.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                    sb.Append("   AND JCSE.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND JCSE.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                sb.Append("   AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");

            }

            string Main_Query = sb.ToString().ToUpper();
            if (ProcessType == 2)
            {
                //Main_Query = Main_Query.Replace("EXP", "IMP")
            }
            if (BizType == 1)
            {
                //Main_Query = Main_Query.Replace("SEA", "AIR")
                //Main_Query = Main_Query.Replace("HAWB_IMP_TBL", "HAWB_EXP_TBL")
            }
            else
            {
                //Main_Query = Main_Query.Replace("HBL_IMP_TBL", "HBL_EXP_TBL")
            }

            try
            {
                return objWF.GetDataTable(Main_Query);
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
        public DataTable getJcDetails(string InvPK = "0", short BizType = 0, short ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            StringBuilder VAT_Str = new StringBuilder();
            VAT_Str.Append(" NVL((select Distinct (frtv.vat_percentage)");
            VAT_Str.Append("      from frt_vat_country_tbl frtv,");
            VAT_Str.Append("       user_mst_tbl        umt,");
            VAT_Str.Append("       location_mst_tbl    loc");
            VAT_Str.Append("      where umt.default_location_fk =loc.location_mst_pk ");
            VAT_Str.Append("       and loc.country_mst_fk = frtv.country_mst_fk");
            VAT_Str.Append("       and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
            VAT_Str.Append("       and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)),");
            VAT_Str.Append("     CORP.VAT_PERCENTAGE) ");

            if (BizType == 2)
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (select distinct INV.CONSOL_INVOICE_PK INVPK, ");
                sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        cntr.container_type_mst_id CONTAINER_TYPE,");

                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBFRT.FREIGHT_AMT *");
                sb.Append("                               GET_EX_RATE(inv.currency_mst_fk,");
                sb.Append("                                           CORP.CURRENCY_MST_FK,");
                sb.Append("                                           inv.invoice_date),");
                sb.Append("                               2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                        ' ' AGENT,");
                sb.Append("                        'CUSTOMER' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          from CONSOL_INVOICE_TRN_TBL  invtrn,");
                sb.Append("               CONSOL_INVOICE_TBL  inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               JOB_CARD_TRN jcse,");
                sb.Append("               JOB_TRN_FD JOBFRT,");
                sb.Append("               container_type_mst_tbl cntr,");
                sb.Append("               CORPORATE_MST_TBL CORP,");
                sb.Append("               CUSTOMER_MST_TBL CUST,");
                sb.Append("               AGENT_MST_TBL AGT,");
                sb.Append("               AGENT_MST_TBL AGTDP,");
                sb.Append("               LOCATION_MST_TBL LOC,");
                sb.Append("               USER_MST_TBL USR ");
                sb.Append("         where inv.consol_invoice_pk in ( " + InvPK + ")");
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.JOB_CARD_TRN_PK = invTRN.Job_Card_Fk");
                sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBFRT.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           and JOBFRT.container_type_mst_fk = cntr.container_type_mst_pk(+)");
                sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");

                sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("           AND JOBFRT.JOB_CARD_TRN_FK = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND JCSE.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                if (ProcessType == 1)
                {
                    sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("           AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
                sb.Append("           AND INV.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("        union");
                sb.Append("        select distinct INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        'Other' CONTAINER_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBOTH.AMOUNT *");
                sb.Append("                               GET_EX_RATE(inv.currency_mst_fk,");
                sb.Append("                                           CORP.CURRENCY_MST_FK,");
                sb.Append("                                           inv.invoice_date),");
                sb.Append("                               2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                        ' ' AGENT,");
                sb.Append("                        'CUSTOMER' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          from CONSOL_INVOICE_TRN_TBL  invtrn,");
                sb.Append("               CONSOL_INVOICE_TBL  inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               JOB_CARD_TRN jcse,");
                sb.Append("               JOB_TRN_OTH_CHRG joboth,");
                sb.Append("               container_type_mst_tbl cntr,");
                sb.Append("               CUSTOMER_MST_TBL CUST,");
                sb.Append("               CORPORATE_MST_TBL CORP,");
                sb.Append("               AGENT_MST_TBL AGT,");
                sb.Append("               AGENT_MST_TBL AGTDP,");
                sb.Append("               LOCATION_MST_TBL LOC,");
                sb.Append("               USER_MST_TBL USR ");

                sb.Append("         where inv.consol_invoice_pk  in ( " + InvPK + ")");
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
                sb.Append("           AND joboth.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND JOBOTH.INV_AGENT_TRN_FK IS NULL");
                sb.Append("           AND JOBOTH.INV_CUST_TRN_FK IS NULL");
                sb.Append("           AND JOBOTH.Freight_Type = 2");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT = 2");
                sb.Append("           AND JOBOTH.JOB_CARD_TRN_FK = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                if (ProcessType == 1)
                {
                    sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("           AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
                sb.Append("           AND INV.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)) Q");
            }
            else
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (SELECT DISTINCT INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        DECODE(JOBFRT.BASIS,");
                sb.Append("                               1,");
                sb.Append("                               '%',");
                sb.Append("                               2,");
                sb.Append("                               'Flat Rate',");
                sb.Append("                               3,");
                sb.Append("                               'Kgs') CONTAINER_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBFRT.FREIGHT_AMT *");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE),");
                sb.Append("                              2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                        ' ' AGENT,");
                sb.Append("                        'CUSTOMER' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          FROM CONSOL_INVOICE_TRN_TBL  INVTRN,");
                sb.Append("               CONSOL_INVOICE_TBL      INV,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL   CUMT,");
                sb.Append("               JOB_CARD_TRN    JCSE,");
                sb.Append("               JOB_TRN_FD      JOBFRT,");
                sb.Append("               CORPORATE_MST_TBL    CORP,");
                sb.Append("               CUSTOMER_MST_TBL     CUST,");
                sb.Append("               AGENT_MST_TBL        AGT,");
                sb.Append("               AGENT_MST_TBL        AGTDP,");
                sb.Append("               LOCATION_MST_TBL     LOC,");
                sb.Append("               USER_MST_TBL         USR ");
                sb.Append("         WHERE INV.CONSOL_INVOICE_PK IN (" + InvPK + ")");
                sb.Append("           AND INVTRN.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK");
                sb.Append("           AND JCSE.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
                sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBFRT.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("           AND JOBFRT.JOB_CARD_TRN_PK = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND JCSE.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                if (ProcessType == 1)
                {
                    sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("           AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
                sb.Append("           AND INV.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("        UNION");
                sb.Append("        SELECT DISTINCT INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        'Other' CONTAINER_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBOTH.AMOUNT *");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE),");
                sb.Append("                              2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                        ' ' AGENT,");
                sb.Append("                        'CUSTOMER' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          FROM CONSOL_INVOICE_TRN_TBL   INVTRN,");
                sb.Append("               CONSOL_INVOICE_TBL       INV,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL  FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("               JOB_CARD_TRN     JCSE,");
                sb.Append("               JOB_TRN_OTH_CHRG JOBOTH,");
                sb.Append("               CUSTOMER_MST_TBL         CUST,");
                sb.Append("               CORPORATE_MST_TBL        CORP,");
                sb.Append("               AGENT_MST_TBL            AGT,");
                sb.Append("               AGENT_MST_TBL            AGTDP,");
                sb.Append("               LOCATION_MST_TBL         LOC,");
                sb.Append("               USER_MST_TBL             USR ");
                sb.Append("         WHERE INV.CONSOL_INVOICE_PK IN (" + InvPK + ")");
                sb.Append("           AND INVTRN.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK");
                sb.Append("           AND JCSE.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
                sb.Append("           AND JOBOTH.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND JOBOTH.INV_AGENT_TRN_AIR_EXP_FK IS NULL");
                sb.Append("           AND JOBOTH.INV_CUST_TRN_AIR_EXP_FK IS NULL");
                sb.Append("           AND JOBOTH.FREIGHT_TYPE = 2");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT = 2");
                sb.Append("           AND JOBOTH.JOB_CARD_TRN_PK = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                if (ProcessType == 1)
                {
                    sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("           AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
                sb.Append("           AND INV.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)) Q");
            }

            string Main_Query = sb.ToString().ToUpper();

            if (ProcessType == 2)
            {
                //Main_Query = Main_Query.Replace("EXP", "IMP")
            }
            try
            {
                return objWF.GetDataTable(Main_Query);
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

        public DataTable getJcDetailsFAC(string InvPK = "0", short BizType = 0, short ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            StringBuilder VAT_Str = new StringBuilder();
            VAT_Str.Append(" NVL((select Distinct (frtv.vat_percentage)");
            VAT_Str.Append("      from frt_vat_country_tbl frtv,");
            VAT_Str.Append("       user_mst_tbl        umt,");
            VAT_Str.Append("       location_mst_tbl    loc");
            VAT_Str.Append("      where umt.default_location_fk =loc.location_mst_pk ");
            VAT_Str.Append("       and loc.country_mst_fk = frtv.country_mst_fk");
            VAT_Str.Append("       and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
            VAT_Str.Append("       and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)),");
            VAT_Str.Append("     CORP.VAT_PERCENTAGE) ");

            if (BizType == 2)
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (select distinct INV.CONSOL_INVOICE_PK INVPK, ");
                sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        (SELECT CEMT.COST_ELEMENT_ID  FROM COST_ELEMENT_MST_TBL CEMT, PARAMETERS_TBL P");
                sb.Append("                        WHERE CEMT.COST_ELEMENT_MST_PK = P.FRT_FAC_FK) CHARGE_CODE,");
                sb.Append("                        '' CONTAINER_TYPE,");
                sb.Append("'" + HttpContext.Current.Session["CURRENCY_ID"] + "' TRANSACTION_CURRENCY,");
                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(INVTRN.ELEMENT_AMT *");
                sb.Append("                               GET_EX_RATE(inv.currency_mst_fk,");
                sb.Append("                                    " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                           inv.invoice_date),");
                sb.Append("                               2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                         OPR.OPERATOR_ID SUPPLIER,");
                sb.Append("                        ' ' AGENT,");
                sb.Append("                        'SUPPLIER' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          from CONSOL_INVOICE_TRN_TBL  invtrn,");
                sb.Append("               CONSOL_INVOICE_TBL  inv,");
                sb.Append("               OPERATOR_MST_TBL       OPR,");
                sb.Append("               JOB_CARD_TRN jcse,");
                sb.Append("               AGENT_MST_TBL AGT,");
                sb.Append("               AGENT_MST_TBL AGTDP,");
                sb.Append("               LOCATION_MST_TBL LOC,");
                sb.Append("               USER_MST_TBL USR ");
                sb.Append("         where inv.consol_invoice_pk in ( " + InvPK + ")");
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.JOB_CARD_TRN_PK = invTRN.Job_Card_Fk");
                sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                if (ProcessType == 1)
                {
                    sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("           AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
                sb.Append("           AND INV.SUPPLIER_MST_FK = OPR.OPERATOR_MST_PK ");
                sb.Append("          ) Q");
            }
            else
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (SELECT DISTINCT INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        DECODE(JOBFRT.BASIS,");
                sb.Append("                               1,");
                sb.Append("                               '%',");
                sb.Append("                               2,");
                sb.Append("                               'Flat Rate',");
                sb.Append("                               3,");
                sb.Append("                               'Kgs') CONTAINER_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");
                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBFRT.FREIGHT_AMT *");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE),");
                sb.Append("                              2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                        ' ' AGENT,");
                sb.Append("                        'CUSTOMER' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          FROM CONSOL_INVOICE_TRN_TBL  INVTRN,");
                sb.Append("               CONSOL_INVOICE_TBL      INV,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL   CUMT,");
                sb.Append("               JOB_CARD_TRN    JCSE,");
                sb.Append("               JOB_TRN_FD      JOBFRT,");
                sb.Append("               CORPORATE_MST_TBL    CORP,");
                sb.Append("               CUSTOMER_MST_TBL     CUST,");
                sb.Append("               AGENT_MST_TBL        AGT,");
                sb.Append("               AGENT_MST_TBL        AGTDP,");
                sb.Append("               LOCATION_MST_TBL     LOC,");
                sb.Append("               USER_MST_TBL         USR ");
                sb.Append("         WHERE INV.CONSOL_INVOICE_PK IN (" + InvPK + ")");
                sb.Append("           AND INVTRN.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK");
                sb.Append("           AND JCSE.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
                sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBFRT.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("           AND JOBFRT.JOB_CARD_TRN_PK = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND JCSE.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                if (ProcessType == 1)
                {
                    sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("           AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
                sb.Append("           AND INV.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("        UNION");
                sb.Append("        SELECT DISTINCT INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        'Other' CONTAINER_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");
                sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
                sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
                sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBOTH.AMOUNT *");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE),");
                sb.Append("                              2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                        ' ' AGENT,");
                sb.Append("                        'CUSTOMER' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          FROM CONSOL_INVOICE_TRN_TBL   INVTRN,");
                sb.Append("               CONSOL_INVOICE_TBL       INV,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL  FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("               JOB_CARD_TRN     JCSE,");
                sb.Append("               JOB_TRN_OTH_CHRG JOBOTH,");
                sb.Append("               CUSTOMER_MST_TBL         CUST,");
                sb.Append("               CORPORATE_MST_TBL        CORP,");
                sb.Append("               AGENT_MST_TBL            AGT,");
                sb.Append("               AGENT_MST_TBL            AGTDP,");
                sb.Append("               LOCATION_MST_TBL         LOC,");
                sb.Append("               USER_MST_TBL             USR ");
                sb.Append("         WHERE INV.CONSOL_INVOICE_PK IN (" + InvPK + ")");
                sb.Append("           AND INVTRN.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK");
                sb.Append("           AND JCSE.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
                sb.Append("           AND JOBOTH.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK(+) = INVTRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("           AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND JOBOTH.INV_AGENT_TRN_AIR_EXP_FK IS NULL");
                sb.Append("           AND JOBOTH.INV_CUST_TRN_AIR_EXP_FK IS NULL");
                sb.Append("           AND JOBOTH.FREIGHT_TYPE = 2");
                sb.Append("           AND INVTRN.FRT_OTH_ELEMENT = 2");
                sb.Append("           AND JOBOTH.JOB_CARD_TRN_PK = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                if (ProcessType == 1)
                {
                    sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("           AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                }
                sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
                sb.Append("           AND INV.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)) Q");
            }

            string Main_Query = sb.ToString().ToUpper();
            if (BizType == 1)
            {
                //Main_Query = Main_Query.Replace("SEA", "AIR")
            }
            if (ProcessType == 2)
            {
                //Main_Query = Main_Query.Replace("EXP", "IMP")
            }
            try
            {
                return objWF.GetDataTable(Main_Query);
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
        #endregion

        #region "AGENT INVOICE DATA"
        public DataTable GetInvAgentHdr(string InvPks = "", short BizType = 0, short ProcessType = 0)
        {
            StringBuilder sb = new StringBuilder(5000);
            string BIZ = (BizType == 2 ? "SEA" : "AIR");
            if (BizType == 2)
                BIZ = "SEA";
            else
                BIZ = "AIR";
            string PROCESS = (ProcessType == 2 ? "EXP" : "IMP");
            if (ProcessType == 1)
                PROCESS = "EXP";
            else
                PROCESS = "IMP";
            sb.Append("SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
            sb.Append("                JOB.JOB_CARD_TRN_PK JCPK,");
            sb.Append("                INV.INVOICE_REF_NO INVOICE_NR,");
            sb.Append("                INV.INV_UNIQUE_REF_NR BANK_REF_NR,");
            sb.Append("                INV.INVOICE_DATE INVOICE_DATE,");
            sb.Append("                INV.INVOICE_DUE_DATE ACTIVITY_DATE,");
            sb.Append("                '" + BIZ + "' BUSINESS_TYPE,");
            sb.Append("                '" + PROCESS + "' PROCESS_TYPE,");
            sb.Append("                '' OCR_NO,");
            sb.Append("                '' CUSTOMER,");
            sb.Append("                AMT.AGENT_NAME AGENT,");
            sb.Append("                'AGENT' PARTY_TYPE,");
            sb.Append("                NVL(SHPMT.CARGO_MOVE_CODE, '') SHIPPING_TERMS,");
            sb.Append("                'AS PER CONTRACT' PAYMENT_TERMS,");
            sb.Append("                CURCORP.CURRENCY_ID BASE_CURRENCY,");
            sb.Append("                CUR.CURRENCY_ID INVOICE_CURRENCY,");
            sb.Append("                'CONTAINER' SHIPMENT,");
            sb.Append("                INV.REMARKS REMARKS,");
            if (BizType == 2)
            {
                sb.Append("                JOB.VESSEL_NAME VSL_FLIGHT,");
                sb.Append("                JOB.VOYAGE_FLIGHT_NO VOYAGE_FLIGHTNR,");
            }
            else
            {
                sb.Append("                NULL VSL_FLIGHT,");
                sb.Append("                JOB.VOYAGE_FLIGHT_NO VOYAGE_FLIGHTNR,");
            }
            sb.Append("                'GENERAL' ROE_BASIS,");
            sb.Append("                'STANDARD' ROE_TYPE,");
            sb.Append("                GET_EX_RATE(INV.CURRENCY_MST_FK,");
            sb.Append("                            CURCORP.CURRENCY_MST_PK,");
            sb.Append("                            INV.INVOICE_DATE) ROE_AMOUNT,");
            sb.Append("                '0' INV_TYPE");
            sb.Append("  FROM INV_AGENT_TBL     INV,");
            sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
            sb.Append("       JOB_CARD_TRN      JOB,");
            sb.Append("       CARGO_MOVE_MST_TBL        SHPMT,");
            sb.Append("       AGENT_MST_TBL             AMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL     CUR,");
            sb.Append("       CURRENCY_TYPE_MST_TBL     CURCORP");
            sb.Append(" WHERE INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
            sb.Append("   AND INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
            sb.Append("   AND JOB.CARGO_MOVE_FK = SHPMT.CARGO_MOVE_PK(+)");
            sb.Append("   AND INV.CB_AGENT_MST_FK = AMT.AGENT_MST_PK");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)");
            sb.Append("   AND CURCORP.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"]);
            if (!string.IsNullOrEmpty(InvPks))
            {
                sb.Append(" AND INV.INV_AGENT_PK IN (" + InvPks + ")");
            }
            string MainQuery = sb.ToString().ToUpper();
            if (BizType == 1)
            {
                //MainQuery = MainQuery.Replace("SEA", "AIR")
            }
            if (ProcessType == 2)
            {
                //MainQuery = MainQuery.Replace("EXP", "IMP")
            }
            WorkFlow objwf = new WorkFlow();
            return objwf.GetDataTable(MainQuery);
        }
        public DataTable GetInvAgentDtl(string InvPks = "", short BizType = 0, short ProcessType = 0)
        {
            StringBuilder sb = new StringBuilder(5000);
            StringBuilder VAT_Str = new StringBuilder();
            VAT_Str.Append(" NVL((SELECT DISTINCT (FRTV.VAT_PERCENTAGE)");
            VAT_Str.Append("  FROM FRT_VAT_COUNTRY_TBL FRTV,");
            VAT_Str.Append("   USER_MST_TBL        UMT,");
            VAT_Str.Append("   LOCATION_MST_TBL    LOC");
            VAT_Str.Append("  WHERE UMT.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK");
            VAT_Str.Append("   AND LOC.COUNTRY_MST_FK = FRTV.COUNTRY_MST_FK");
            VAT_Str.Append("   AND UMT.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"]);
            VAT_Str.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK =");
            VAT_Str.Append("   FRTV.FREIGHT_ELEMENT_MST_FK(+)),");
            VAT_Str.Append("   CORP.VAT_PERCENTAGE) ");


            if (BizType == 2)
            {
                sb.Append("SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                CNTR.CONTAINER_TYPE_MST_ID CONTAINER_TYPE,");
                sb.Append("                CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        NVL(INVTRN.TAX_PCNT,CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        NVL(INVTRN.VAT_CODE,' ') VAT_TYPE,");
                sb.Append("                        ROUND(NVL(INVTRN.TAX_AMT,0),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(NVL(INVTRN.TOT_AMT,0),2) AMOUNT_INCL_VAT,");
                sb.Append("                GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                            CORP.CURRENCY_MST_FK,");
                sb.Append("                            INV.INVOICE_DATE) ROE,");
                sb.Append("                'STANDARD' ROE_TYPE,");
                sb.Append("                ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
                sb.Append("                ROUND(INVTRN.AMT_IN_INV_CURR *");
                sb.Append("                GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                            CORP.CURRENCY_MST_FK,");
                sb.Append("                            INV.INVOICE_DATE),");
                sb.Append("                      2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN JOB.PYMT_TYPE = 1 THEN");
                sb.Append("                   'P'");
                sb.Append("                  ELSE");
                sb.Append("                   'C'");
                sb.Append("                END) PREPAID_COLLECT,");
                sb.Append("                DECODE(FMT.CHARGE_BASIS,1,'%',2,'Flat Rate',3,'Kgs',4,'Unit') CHARGE_BASIS,");
                sb.Append("                'OPEN' COLLECT_STATUS,");
                sb.Append("                'F' ADDITIONALCHARGES");
                sb.Append("  FROM INV_AGENT_TBL INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("       JOB_CARD_TRN JOB,");
                sb.Append("       JOB_TRN_FD JOBFRT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL CNTR,");
                sb.Append("       CORPORATE_MST_TBL CORP ");

                sb.Append(" WHERE INV.INV_AGENT_PK IN (" + InvPks + ") ");
                sb.Append("   AND INVTRN.INV_AGENT_FK = INV.INV_AGENT_PK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("   AND INVTRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CNTR.CONTAINER_TYPE_MST_PK(+) = JOBFRT.CONTAINER_TYPE_MST_FK");

                sb.Append("   AND INVTRN.COST_FRT_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND INVTRN.COST_FRT_ELEMENT IN (1,2)");
                sb.Append("   AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("   AND JOBFRT.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append(" UNION ");
                sb.Append(" SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                ' ' CONTAINER_TYPE,");
                sb.Append("                CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        NVL(INVTRN.TAX_PCNT,CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        NVL(INVTRN.VAT_CODE,' ') VAT_TYPE,");
                sb.Append("                        ROUND(NVL(INVTRN.TAX_AMT,0),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(NVL(INVTRN.TOT_AMT,0),2) AMOUNT_INCL_VAT,");
                sb.Append("                GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                            CORP.CURRENCY_MST_FK,");
                sb.Append("                            INV.INVOICE_DATE) ROE,");
                sb.Append("                'STANDARD' ROE_TYPE,");
                sb.Append("                ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
                sb.Append("                ROUND(INVTRN.AMT_IN_INV_CURR *");
                sb.Append("                GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                            CORP.CURRENCY_MST_FK,");
                sb.Append("                            INV.INVOICE_DATE),");
                sb.Append("                      2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN JOB.PYMT_TYPE = 1 THEN");
                sb.Append("                   'P'");
                sb.Append("                  ELSE");
                sb.Append("                   'C'");
                sb.Append("                END) PREPAID_COLLECT,");
                sb.Append("                DECODE(FMT.CHARGE_BASIS,1,'%',2,'Flat Rate',3,'Kgs',4,'Unit') CHARGE_BASIS,");
                sb.Append("                'OPEN' COLLECT_STATUS,");
                sb.Append("                'T' ADDITIONALCHARGES");
                sb.Append("  FROM INV_AGENT_TBL INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("       JOB_CARD_TRN JOB,");
                sb.Append("       JOB_TRN_OTH_CHRG JOBOTH,");
                sb.Append("       CONTAINER_TYPE_MST_TBL CNTR,");

                sb.Append("       CORPORATE_MST_TBL CORP");
                sb.Append(" WHERE INV.INV_AGENT_PK IN (" + InvPks + ")");
                sb.Append("   AND INVTRN.INV_AGENT_FK = INV.INV_AGENT_PK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("   AND INVTRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND JOBOTH.INV_CUST_TRN_FK IS NULL");
                sb.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("   AND INVTRN.COST_FRT_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND INVTRN.COST_FRT_ELEMENT = 3 ");
                sb.Append("   AND JOBOTH.FREIGHT_TYPE = 2 ");
                sb.Append("   AND JOBOTH.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
            }
            else
            {
                sb.Append("SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                ' ' ULD_TYPE,");
                sb.Append("                CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        NVL(INVTRN.TAX_PCNT,CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        NVL(INVTRN.VAT_CODE,' ') VAT_TYPE,");
                sb.Append("                        ROUND(NVL(INVTRN.TAX_AMT,0),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(NVL(INVTRN.TOT_AMT,0),2) AMOUNT_INCL_VAT,");
                sb.Append("                GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                            CORP.CURRENCY_MST_FK,");
                sb.Append("                            INV.INVOICE_DATE) ROE,");
                sb.Append("                'STANDARD' ROE_TYPE,");
                sb.Append("                ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
                sb.Append("                ROUND(INVTRN.AMT_IN_INV_CURR *");
                sb.Append("                GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                            CORP.CURRENCY_MST_FK,");
                sb.Append("                            INV.INVOICE_DATE),");
                sb.Append("                      2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN JOB.PYMT_TYPE = 1 THEN");
                sb.Append("                   'P'");
                sb.Append("                  ELSE");
                sb.Append("                   'C'");
                sb.Append("                END) PREPAID_COLLECT,");
                sb.Append("                DECODE(FMT.CHARGE_BASIS,1,'%',2,'Flat Rate',3,'Kgs',4,'Unit') CHARGE_BASIS,");
                sb.Append("                'OPEN' COLLECT_STATUS,");
                sb.Append("                'F' ADDITIONALCHARGES");
                sb.Append("  FROM INV_AGENT_TBL     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL   FMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL     CUMT,");
                sb.Append("       JOB_CARD_TRN      JOB,");
                sb.Append("       JOB_TRN_FD        JOBFRT,");
                sb.Append("       CORPORATE_MST_TBL         CORP ");
                sb.Append(" WHERE INV.INV_AGENT_PK IN (" + InvPks + ")");
                sb.Append("   AND INVTRN.INV_AGENT_FK = INV.INV_AGENT_PK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("   AND INVTRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");

                sb.Append("   AND INVTRN.COST_FRT_ELEMENT IN (1,2)");
                sb.Append("   AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("   AND JOBFRT.JOB_CARD_TRN_PK = JOB.JOB_CARD_TRN_PK");
                sb.Append(" UNION ");
                sb.Append("SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                'Other' ULD_TYPE,");
                sb.Append("                CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        NVL(INVTRN.TAX_PCNT,CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        NVL(INVTRN.VAT_CODE,' ') VAT_TYPE,");
                sb.Append("                        ROUND(NVL(INVTRN.TAX_AMT,0),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(NVL(INVTRN.TOT_AMT,0),2) AMOUNT_INCL_VAT,");
                sb.Append("                GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                            CORP.CURRENCY_MST_FK,");
                sb.Append("                            INV.INVOICE_DATE) ROE,");
                sb.Append("                'STANDARD' ROE_TYPE,");
                sb.Append("                ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
                sb.Append("                ROUND(INVTRN.AMT_IN_INV_CURR *");
                sb.Append("                GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                            CORP.CURRENCY_MST_FK,");
                sb.Append("                            INV.INVOICE_DATE),");
                sb.Append("                      2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN JOB.PYMT_TYPE = 1 THEN");
                sb.Append("                   'P'");
                sb.Append("                  ELSE");
                sb.Append("                   'C'");
                sb.Append("                END) PREPAID_COLLECT,");
                sb.Append("                DECODE(FMT.CHARGE_BASIS,1,'%',2,'Flat Rate',3,'Kgs',4,'Unit') CHARGE_BASIS,");
                sb.Append("                'OPEN' COLLECT_STATUS,");
                sb.Append("                'T' ADDITIONALCHARGES");
                sb.Append("  FROM INV_AGENT_TBL     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL   FMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL     CUMT,");
                sb.Append("       JOB_CARD_TRN      JOB,");
                sb.Append("       JOB_TRN_OTH_CHRG  JOBOTH,");
                sb.Append("       CONTAINER_TYPE_MST_TBL    CNTR,");
                sb.Append("       CORPORATE_MST_TBL         CORP ");
                sb.Append(" WHERE INV.INV_AGENT_PK IN (" + InvPks + ")");
                sb.Append("   AND INVTRN.INV_AGENT_FK = INV.INV_AGENT_PK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("   AND INVTRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");

                sb.Append("   AND JOBOTH.INV_CUST_TRN_AIR_EXP_FK IS NULL");
                sb.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("   AND INVTRN.COST_FRT_ELEMENT = 3 ");
                sb.Append("   AND JOBOTH.FREIGHT_TYPE =2");
                sb.Append("   AND JOBOTH.JOB_CARD_TRN_PK = JOB.JOB_CARD_TRN_PK");
            }
            string MainQuery = sb.ToString().ToUpper();
            if (BizType == 1)
            {
                //MainQuery = MainQuery.Replace("SEA", "AIR")
            }
            if (ProcessType == 2)
            {
                //MainQuery = MainQuery.Replace("EXP", "IMP")
            }
            WorkFlow objwf = new WorkFlow();
            return objwf.GetDataTable(MainQuery);
        }
        public DataTable GetAgentJobHdr(string InvPks = "", short BizType = 0, short ProcessType = 0)
        {
            StringBuilder sb = new StringBuilder(5000);
            string BIZ = (BizType == 2 ? "SEA" : "AIR");
            string PROCESS = (ProcessType == 1 ? "EXP" : "IMP");
            if (BizType == 2 & ProcessType == 1)
            {
                sb.Append("SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                TO_CHAR(JOB.JOBCARD_DATE, 'DD-MON-YYYY') SALES_DATE,");
                sb.Append("                TO_CHAR(NVL(JOB.DEPARTURE_DATE, JOB.ETD_DATE),");
                sb.Append("                        'dd-MON-yyyy') SALES_ACT_DATE,");
                sb.Append("                '" + BIZ + "' BUSINESS_TYPE,");
                sb.Append("                NVL(POR.PLACE_CODE, ' ') POR,");
                sb.Append("                NVL(POL.PORT_ID, ' ') POL,");
                sb.Append("                NVL(POD.PORT_ID, ' ') POD,");
                sb.Append("                NVL(PLD.PLACE_CODE, ' ') PFD,");
                sb.Append("                CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                AGT.AGENT_ID AGENT,");
                sb.Append("                'AGENT' PARTY_TYPE,");
                sb.Append("                NVL(JOB.VESSEL_NAME, ' ') VSL,");
                sb.Append("                NVL(JOB.VOYAGE_FLIGHT_NO, ' ') VOYAGE,");
                sb.Append("                CURR.CURRENCY_ID BASE_CURRENCY,");
                sb.Append("                '" + PROCESS + "' PROCESS_TYPE,");
                sb.Append("                BKG.BOOKING_REF_NO BOOKING_REF_NO,");
                sb.Append("                TO_CHAR(BKG.BOOKING_DATE, 'DD-MON-YYYY') BOOKING_DATE,");
                sb.Append("                HBL.HBL_REF_NO BL_REF_NO,");
                sb.Append("                TO_CHAR(HBL.HBL_DATE, 'DD-MON-YYYY') BL_DATE,");
                sb.Append("                JOB.JOBCARD_REF_NO JOBCARD_REF_NO,");
                sb.Append("                TO_CHAR(JOB.JOBCARD_DATE, 'DD-MON-YYYY') JOB_CARD_DATE,");
                sb.Append("                'CONTAINER' SHIPMENT_TYPE,");
                sb.Append("                NVL(SHMT.CARGO_MOVE_CODE, ' ') SHIPPING_TERMS,");
                sb.Append("                'GENERAL' ROE_BASIS,");
                sb.Append("                'INVOICED' STATUS,");
                sb.Append("                'RECORD1' REF_NR");
                sb.Append("  FROM JOB_CARD_TRN      JOB,");
                sb.Append("       INV_AGENT_TBL     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       BOOKING_MST_TBL           BKG,");
                sb.Append("       PORT_MST_TBL              POL,");
                sb.Append("       PORT_MST_TBL              POD,");
                sb.Append("       PLACE_MST_TBL             POR,");
                sb.Append("       PLACE_MST_TBL             PLD,");
                sb.Append("       CURRENCY_TYPE_MST_TBL     CURR,");
                sb.Append("       HBL_EXP_TBL               HBL,");
                sb.Append("       CORPORATE_MST_TBL         CORP,");
                sb.Append("       CARGO_MOVE_MST_TBL        SHMT,");
                sb.Append("       AGENT_MST_TBL             AGT,");
                sb.Append("       CUSTOMER_MST_TBL          CUST");
                sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("   AND INV.INV_AGENT_PK IN  (" + InvPks + ")");
                sb.Append("   AND CORP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                sb.Append("   AND HBL.JOB_CARD_SEA_EXP_FK(+) = JOB.JOB_CARD_TRN_PK");
                sb.Append("   AND SHMT.CARGO_MOVE_PK(+) = JOB.CARGO_MOVE_FK");
                sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("   AND BKG.COL_PLACE_MST_FK = POR.PLACE_PK(+)");
                sb.Append("   AND BKG.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                sb.Append("   AND BKG.PORT_MST_POD_FK = POL.PORT_MST_PK");
                sb.Append("   AND BKG.PORT_MST_POL_FK = POD.PORT_MST_PK");
                sb.Append("   AND INV.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
            }
            else if (BizType == 2 & ProcessType == 2)
            {
                sb.Append("SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                TO_CHAR(JOB.JOBCARD_DATE, 'DD-MON-YYYY') SALES_DATE,");
                sb.Append("                TO_CHAR(NVL(JOB.DEPARTURE_DATE, JOB.ETD_DATE),");
                sb.Append("                        'dd-MON-yyyy') SALES_ACT_DATE,");
                sb.Append("                '" + BIZ + "' BUSINESS_TYPE,");
                sb.Append("                ' ' POR,");
                sb.Append("                NVL(POL.PORT_ID, ' ') POL,");
                sb.Append("                NVL(POD.PORT_ID, ' ') POD,");
                sb.Append("                NVL(PLD.PLACE_CODE, ' ') PFD,");
                sb.Append("                ' ' CUSTOMER,");
                sb.Append("                AGT.AGENT_ID AGENT,");
                sb.Append("                'AGENT' PARTY_TYPE,");
                sb.Append("                NVL(JOB.VESSEL_NAME, ' ') VSL,");
                sb.Append("                NVL(JOB.VOYAGE_FLIGHT_NO, ' ') VOYAGE,");
                sb.Append("                CURR.CURRENCY_ID BASE_CURRENCY,");
                sb.Append("                '" + PROCESS + "' PROCESS_TYPE,");
                sb.Append("                ' ' BOOKING_REF_NO,");
                sb.Append("                NULL BOOKING_DATE,");
                sb.Append("                HBL.HBL_REF_NO BL_REF_NO,");
                sb.Append("                TO_CHAR(HBL.HBL_DATE, 'DD-MON-YYYY') BL_DATE,");
                sb.Append("                JOB.JOBCARD_REF_NO JOBCARD_REF_NO,");
                sb.Append("                TO_CHAR(JOB.JOBCARD_DATE, 'DD-MON-YYYY') JOB_CARD_DATE,");
                sb.Append("                'CONTAINER' SHIPMENT_TYPE,");
                sb.Append("                 NVL(SHMT.CARGO_MOVE_CODE, ' ') SHIPPING_TERMS,");
                sb.Append("                'GENERAL' ROE_BASIS,");
                sb.Append("                'INVOICED' STATUS,");
                sb.Append("                'RECORD1' REF_NR");
                sb.Append("  FROM JOB_CARD_TRN      JOB,");
                sb.Append("       INV_AGENT_TBL     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       PORT_MST_TBL              POL,");
                sb.Append("       PORT_MST_TBL              POD,");
                sb.Append("       PLACE_MST_TBL             PLD,");
                sb.Append("       CURRENCY_TYPE_MST_TBL     CURR,");
                sb.Append("       HBL_EXP_TBL               HBL,");
                sb.Append("       CORPORATE_MST_TBL         CORP,");
                sb.Append("       CARGO_MOVE_MST_TBL        SHMT,");
                sb.Append("       AGENT_MST_TBL             AGT,");
                sb.Append("       CUSTOMER_MST_TBL          CUST");
                sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("   AND INV.INV_AGENT_PK IN (" + InvPks + ")");
                sb.Append("   AND CORP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND HBL.HBL_REF_NO(+) = JOB.HBL_HAWB_REF_NO");
                sb.Append("   AND SHMT.CARGO_MOVE_PK(+) = JOB.CARGO_MOVE_FK");
                sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("   AND JOB.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                sb.Append("   AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND INV.CB_AGENT_MST_FK = AGT.AGENT_MST_PK");
            }
            else if (BizType == 1 & ProcessType == 1)
            {
                sb.Append("SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                TO_CHAR(JOB.JOBCARD_DATE, 'DD-MON-YYYY') SALES_DATE,");
                sb.Append("                TO_CHAR(NVL(JOB.DEPARTURE_DATE, JOB.ETD_DATE),");
                sb.Append("                        'dd-MON-yyyy') SALES_ACT_DATE,");
                sb.Append("                '" + BIZ + "' BUSINESS_TYPE,");
                sb.Append("                NVL(POR.PLACE_CODE, ' ') POR,");
                sb.Append("                NVL(POL.PORT_ID, ' ') POL,");
                sb.Append("                NVL(POD.PORT_ID, ' ') POD,");
                sb.Append("                NVL(PLD.PLACE_CODE, ' ') PFD,");
                sb.Append("                CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                AGT.AGENT_ID AGENT,");
                sb.Append("                'AGENT' PARTY_TYPE,");
                sb.Append("                NULL VSL,");
                sb.Append("                NVL(JOB.VOYAGE_FLIGHT_NO, ' ') VOYAGE,");
                sb.Append("                CURR.CURRENCY_ID BASE_CURRENCY,");
                sb.Append("                '" + PROCESS + "' PROCESS_TYPE,");
                sb.Append("                BKG.BOOKING_REF_NO BOOKING_REF_NO,");
                sb.Append("                TO_CHAR(BKG.BOOKING_DATE, 'DD-MON-YYYY') BOOKING_DATE,");
                sb.Append("                HAWB.HAWB_REF_NO BL_REF_NO,");
                sb.Append("                TO_CHAR(HAWB.HAWB_DATE, 'DD-MON-YYYY') BL_DATE,");
                sb.Append("                JOB.JOBCARD_REF_NO JOBCARD_REF_NO,");
                sb.Append("                TO_CHAR(JOB.JOBCARD_DATE, 'DD-MON-YYYY') JOB_CARD_DATE,");
                sb.Append("                'CONTAINER' SHIPMENT_TYPE,");
                sb.Append("                NVL(SHMT.CARGO_MOVE_CODE, ' ') SHIPPING_TERMS,");
                sb.Append("                'GENERAL' ROE_BASIS,");
                sb.Append("                'INVOICED' STATUS,");
                sb.Append("                'RECORD1' REF_NR");
                sb.Append("  FROM JOB_CARD_TRN      JOB,");
                sb.Append("       INV_AGENT_TBL     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       BOOKING_MST_TBL           BKG,");
                sb.Append("       PORT_MST_TBL              POL,");
                sb.Append("       PORT_MST_TBL              POD,");
                sb.Append("       PLACE_MST_TBL             POR,");
                sb.Append("       PLACE_MST_TBL             PLD,");
                sb.Append("       CURRENCY_TYPE_MST_TBL     CURR,");
                sb.Append("       HAWB_EXP_TBL              HAWB,");
                sb.Append("       CORPORATE_MST_TBL         CORP,");
                sb.Append("       CARGO_MOVE_MST_TBL        SHMT,");
                sb.Append("       AGENT_MST_TBL             AGT,");
                sb.Append("       CUSTOMER_MST_TBL          CUST");
                sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("   AND INV.INV_AGENT_PK IN  (" + InvPks + ")");
                sb.Append("   AND CORP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                sb.Append("   AND HAWB.HAWB_EXP_TBL_PK(+) = JOB.JOB_CARD_TRN_PK");
                sb.Append("   AND SHMT.CARGO_MOVE_PK(+) = JOB.CARGO_MOVE_FK");
                sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("   AND BKG.COL_PLACE_MST_FK = POR.PLACE_PK(+)");
                sb.Append("   AND BKG.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                sb.Append("   AND BKG.PORT_MST_POD_FK = POL.PORT_MST_PK");
                sb.Append("   AND BKG.PORT_MST_POL_FK = POD.PORT_MST_PK");
                sb.Append("   AND INV.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
            }
            else if (BizType == 1 & ProcessType == 2)
            {
                sb.Append("SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                TO_CHAR(JOB.JOBCARD_DATE, 'DD-MON-YYYY') SALES_DATE,");
                sb.Append("                TO_CHAR(NVL(JOB.DEPARTURE_DATE, JOB.ETD_DATE),");
                sb.Append("                        'dd-MON-yyyy') SALES_ACT_DATE,");
                sb.Append("                '" + BIZ + "' BUSINESS_TYPE,");
                sb.Append("                ' ' POR,");
                sb.Append("                NVL(POL.PORT_ID, ' ') POL,");
                sb.Append("                NVL(POD.PORT_ID, ' ') POD,");
                sb.Append("                NVL(PLD.PLACE_CODE, ' ') PFD,");
                sb.Append("                CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                AGT.AGENT_ID AGENT,");
                sb.Append("                'AGENT' PARTY_TYPE,");
                sb.Append("                NULL VSL,");
                sb.Append("                NVL(JOB.VOYAGE_FLIGHT_NO, ' ') VOYAGE,");
                sb.Append("                CURR.CURRENCY_ID BASE_CURRENCY,");
                sb.Append("                '" + PROCESS + "' PROCESS_TYPE,");
                sb.Append("                NULL BOOKING_REF_NO,");
                sb.Append("                NULL BOOKING_DATE,");
                sb.Append("                HAWB.HAWB_REF_NO BL_REF_NO,");
                sb.Append("                TO_CHAR(HAWB.HAWB_DATE, 'DD-MON-YYYY') BL_DATE,");
                sb.Append("                JOB.JOBCARD_REF_NO JOBCARD_REF_NO,");
                sb.Append("                TO_CHAR(JOB.JOBCARD_DATE, 'DD-MON-YYYY') JOB_CARD_DATE,");
                sb.Append("                'ULD' SHIPMENT_TYPE,");
                sb.Append("                NVL(SHMT.CARGO_MOVE_CODE, ' ') SHIPPING_TERMS,");
                sb.Append("                'GENERAL' ROE_BASIS,");
                sb.Append("                'INVOICED' STATUS,");
                sb.Append("                'RECORD1' REF_NR");
                sb.Append("  FROM JOB_CARD_TRN      JOB,");
                sb.Append("       INV_AGENT_TBL     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       PORT_MST_TBL              POL,");
                sb.Append("       PORT_MST_TBL              POD,");
                sb.Append("       PLACE_MST_TBL             PLD,");
                sb.Append("       CURRENCY_TYPE_MST_TBL     CURR,");
                sb.Append("       HAWB_EXP_TBL               HAWB,");
                sb.Append("       CORPORATE_MST_TBL         CORP,");
                sb.Append("       CARGO_MOVE_MST_TBL        SHMT,");
                sb.Append("       AGENT_MST_TBL             AGT,");
                sb.Append("       CUSTOMER_MST_TBL          CUST");
                sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("   AND INV.INV_AGENT_PK IN  (" + InvPks + ")");
                sb.Append("   AND CORP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND HAWB.HAWB_REF_NO(+) = JOB.HBL_HAWB_REF_NO");
                sb.Append("   AND SHMT.CARGO_MOVE_PK(+) = JOB.CARGO_MOVE_FK");
                sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("   AND JOB.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                sb.Append("   AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND INV.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
            }
            WorkFlow objwf = new WorkFlow();
            return objwf.GetDataTable(sb.ToString());
        }
        public DataTable GetAgentJobDtl(string InvPks = "", short BizType = 0, short ProcessType = 0)
        {
            StringBuilder sb = new StringBuilder(5000);
            StringBuilder VAT_Str = new StringBuilder();
            VAT_Str.Append(" NVL((SELECT DISTINCT (FRTV.VAT_PERCENTAGE)");
            VAT_Str.Append("  FROM FRT_VAT_COUNTRY_TBL FRTV,");
            VAT_Str.Append("   USER_MST_TBL        UMT,");
            VAT_Str.Append("   LOCATION_MST_TBL    LOC");
            VAT_Str.Append("  WHERE UMT.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK");
            VAT_Str.Append("   AND LOC.COUNTRY_MST_FK = FRTV.COUNTRY_MST_FK");
            VAT_Str.Append("   AND UMT.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"]);
            VAT_Str.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK =");
            VAT_Str.Append("   FRTV.FREIGHT_ELEMENT_MST_FK(+)),");
            VAT_Str.Append("   CORP.VAT_PERCENTAGE) ");
            if (BizType == 2)
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                        JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        CNTR.CONTAINER_TYPE_MST_ID CONTAINER_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        NVL(INVTRN.TAX_PCNT,CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        NVL(INVTRN.VAT_CODE,' ') VAT_TYPE,");
                sb.Append("                        ROUND(NVL(INVTRN.TAX_AMT,0),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(NVL(INVTRN.TOT_AMT,0),2) AMOUNT_INCL_VAT,");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBFRT.FREIGHT_AMT *");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE),");
                sb.Append("                              2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JOB.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        NULL CUSTOMER,");
                sb.Append("                        AGT.AGENT_ID AGENT,");
                sb.Append("                        'AGENT' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          FROM INV_AGENT_TBL INV,");
                sb.Append("               INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               JOB_CARD_TRN JOB,");
                sb.Append("               JOB_TRN_FD JOBFRT,");
                sb.Append("               CONTAINER_TYPE_MST_TBL CNTR,");
                sb.Append("               CORPORATE_MST_TBL CORP,");
                sb.Append("               CUSTOMER_MST_TBL CUST,");
                sb.Append("               AGENT_MST_TBL AGT,");
                sb.Append("               LOCATION_MST_TBL LOC,");
                sb.Append("               USER_MST_TBL USR ");
                sb.Append("         WHERE INV.INV_AGENT_PK IN (" + InvPks + ")");
                sb.Append("           AND INVTRN.INV_AGENT_FK = INV.INV_AGENT_PK");
                sb.Append("           AND JOB.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND CNTR.CONTAINER_TYPE_MST_PK = JOBFRT.CONTAINER_TYPE_MST_FK");
                sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");

                sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2) ");
                sb.Append("           AND INVTRN.COST_FRT_ELEMENT IN (1,2) ");
                sb.Append("           AND JOBFRT.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("           AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("           AND INV.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JOB.CREATED_BY_FK");
                sb.Append("        UNION");
                sb.Append("        SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                        JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        'Other' CONTAINER_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        NVL(INVTRN.TAX_PCNT,CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        NVL(INVTRN.VAT_CODE,' ') VAT_TYPE,");
                sb.Append("                        ROUND(NVL(INVTRN.TAX_AMT,0),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(NVL(INVTRN.TOT_AMT,0),2) AMOUNT_INCL_VAT,");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBOTH.AMOUNT *");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE),");
                sb.Append("                              2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JOB.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                        AGT.AGENT_ID AGENT,");
                sb.Append("                        'AGENT' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          FROM INV_AGENT_TBL INV,");
                sb.Append("               INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               JOB_CARD_TRN JOB,");
                sb.Append("               JOB_TRN_OTH_CHRG JOBOTH,");

                sb.Append("               CUSTOMER_MST_TBL CUST,");
                sb.Append("               CORPORATE_MST_TBL CORP,");
                sb.Append("               AGENT_MST_TBL AGT,");
                sb.Append("               LOCATION_MST_TBL LOC,");
                sb.Append("               USER_MST_TBL USR ");

                sb.Append("         WHERE INV.INV_AGENT_PK  in (" + InvPks + ")");
                sb.Append("           AND INVTRN.INV_AGENT_FK = INV.INV_AGENT_PK");
                sb.Append("           AND JOB.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("           AND JOBOTH.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("           AND JOBOTH.INV_CUST_TRN_FK IS NULL");
                sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("           AND JOBOTH.FREIGHT_TYPE = 2");
                sb.Append("           AND INVTRN.COST_FRT_ELEMENT =3 ");

                sb.Append("           AND JOBOTH.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("           AND INV.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JOB.CREATED_BY_FK");
                sb.Append("           AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)) Q");
            }
            else
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                        JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        ' ' ULD_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        NVL(INVTRN.TAX_PCNT,CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        NVL(INVTRN.VAT_CODE,' ') VAT_TYPE,");
                sb.Append("                        ROUND(NVL(INVTRN.TAX_AMT,0),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(NVL(INVTRN.TOT_AMT,0),2) AMOUNT_INCL_VAT,");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBFRT.FREIGHT_AMT *");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE),");
                sb.Append("                              2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JOB.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        NULL CUSTOMER,");
                sb.Append("                        AGT.AGENT_ID AGENT,");
                sb.Append("                        'AGENT' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          FROM INV_AGENT_TBL     INV,");
                sb.Append("               INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL   FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     CUMT,");
                sb.Append("               JOB_CARD_TRN      JOB,");
                sb.Append("               JOB_TRN_FD        JOBFRT,");

                sb.Append("               CORPORATE_MST_TBL         CORP,");
                sb.Append("               CUSTOMER_MST_TBL          CUST,");
                sb.Append("               AGENT_MST_TBL             AGT,");
                sb.Append("               LOCATION_MST_TBL          LOC,");
                sb.Append("               USER_MST_TBL              USR");
                sb.Append("         WHERE INV.INV_AGENT_PK IN (" + InvPks + ")");
                sb.Append("           AND INVTRN.INV_AGENT_FK = INV.INV_AGENT_PK");
                sb.Append("           AND JOB.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");

                sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("           AND INVTRN.COST_FRT_ELEMENT IN (1,2) ");
                sb.Append("           AND JOBFRT.JOB_CARD_TRN_PK = JOB.JOB_CARD_TRN_PK");
                sb.Append("           AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("           AND INV.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JOB.CREATED_BY_FK");
                sb.Append("        UNION ");
                sb.Append("        SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                sb.Append("                        JOB.JOB_CARD_TRN_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        'Other' ULD_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");

                sb.Append("                        NVL(INVTRN.TAX_PCNT,CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        NVL(INVTRN.VAT_CODE,' ') VAT_TYPE,");
                sb.Append("                        ROUND(NVL(INVTRN.TAX_AMT,0),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(NVL(INVTRN.TOT_AMT,0),2) AMOUNT_INCL_VAT,");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBOTH.AMOUNT *");
                sb.Append("                        GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                    CORP.CURRENCY_MST_FK,");
                sb.Append("                                    INV.INVOICE_DATE),");
                sb.Append("                              2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JOB.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        NULL CUSTOMER,");
                sb.Append("                        AGT.AGENT_ID AGENT,");
                sb.Append("                        'AGENT' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          FROM INV_AGENT_TBL     INV,");
                sb.Append("               INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL   FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     CUMT,");
                sb.Append("               JOB_CARD_TRN      JOB,");
                sb.Append("               JOB_TRN_OTH_CHRG  JOBOTH,");

                sb.Append("               CUSTOMER_MST_TBL          CUST,");
                sb.Append("               CORPORATE_MST_TBL         CORP,");
                sb.Append("               AGENT_MST_TBL             AGT,");
                sb.Append("               LOCATION_MST_TBL          LOC,");
                sb.Append("               USER_MST_TBL              USR");
                sb.Append("         WHERE INV.INV_AGENT_PK  IN (" + InvPks + ")");
                sb.Append("           AND INVTRN.INV_AGENT_FK = INV.INV_AGENT_PK");
                sb.Append("           AND JOB.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("           AND JOBOTH.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND JOBOTH.INV_CUST_TRN_AIR_EXP_FK IS NULL");
                sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("           AND JOBOTH.FREIGHT_TYPE = 2");
                sb.Append("           AND INVTRN.COST_FRT_ELEMENT =3 ");

                sb.Append("           AND JOBOTH.JOB_CARD_TRN_PK = JOB.JOB_CARD_TRN_PK");
                sb.Append("           AND INV.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JOB.CREATED_BY_FK");
                sb.Append("           AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)) Q");
            }
            string Main_Query = sb.ToString().ToUpper();
            if (ProcessType == 2)
            {
                //Main_Query = Main_Query.Replace("EXP", "IMP")
            }
            WorkFlow objwf = new WorkFlow();
            return objwf.GetDataTable(Main_Query);
        }
        #endregion

        #region "Fetch VAT % and VAT Code"
        public DataSet Fetch_VAT(int ConsPk)
        {
            try
            {
                DataSet DsVAT = null;
                WorkFlow objWF = new WorkFlow();
                StringBuilder sb = new StringBuilder();
                sb.Append("SELECT INVTRN.VAT_CODE,");
                sb.Append("       INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("       INVTRN.FRT_OTH_ELEMENT_FK FREIGHT_ELEMENT_MST_FK");
                sb.Append("  FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN");
                sb.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                sb.Append("   AND INV.CONSOL_INVOICE_PK =" + ConsPk);
                DsVAT = objWF.GetDataSet(sb.ToString());
                return DsVAT;
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
        #endregion

        #region "Fetch POL,POD,COMMODITY FOR BL Clause"
        public DataSet Fetch_POL_DETAIL(int BizType, int Process, int InvPK)
        {
            try
            {
                DataSet objDs = null;
                WorkFlow objWF = new WorkFlow();
                StringBuilder sb = new StringBuilder(5000);

                if (BizType == 1)
                {
                    if (Process == 1)
                    {
                        sb.Append("SELECT DISTINCT BST.PORT_MST_POL_FK,");
                        sb.Append("                BST.PORT_MST_POD_FK,");
                        sb.Append("                JTSE.COMMODITY_MST_FKS");
                        sb.Append("  FROM CONSOL_INVOICE_TBL     C,");
                        sb.Append("       CONSOL_INVOICE_TRN_TBL CIT,");
                        sb.Append("       JOB_CARD_TRN   JCSE,");
                        sb.Append("       JOB_TRN_CONT   JTSE,");
                        sb.Append("       BOOKING_MST_TBL        BST");
                        sb.Append(" WHERE C.CONSOL_INVOICE_PK = " + InvPK);
                        sb.Append("   AND C.BUSINESS_TYPE = " + BizType);
                        sb.Append("   AND C.PROCESS_TYPE = " + Process);
                        sb.Append("   AND C.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
                        sb.Append("   AND JCSE.JOB_CARD_TRN_PK = CIT.JOB_CARD_FK");
                        sb.Append("   AND BST.BOOKING_MST_PK = JCSE.BOOKING_MST_FK");
                        sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JTSE.JOB_CARD_TRN_FK");
                    }
                    else
                    {
                        sb.Append("SELECT DISTINCT JCSI.PORT_MST_POL_FK,");
                        sb.Append("                JCSI.PORT_MST_POD_FK,");
                        sb.Append("                JTSI.COMMODITY_MST_FK COMMODITY_MST_FKS");
                        sb.Append("  FROM CONSOL_INVOICE_TBL     C,");
                        sb.Append("       CONSOL_INVOICE_TRN_TBL CIT,");
                        sb.Append("       JOB_CARD_TRN   JCSI,");
                        sb.Append("       JOB_TRN_CONT   JTSI");
                        sb.Append(" WHERE C.CONSOL_INVOICE_PK = " + InvPK);
                        sb.Append("   AND C.BUSINESS_TYPE = " + BizType);
                        sb.Append("   AND C.PROCESS_TYPE = " + Process);
                        sb.Append("   AND C.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
                        sb.Append("   AND JCSI.JOB_CARD_TRN_PK = CIT.JOB_CARD_FK");
                        sb.Append("   AND JCSI.JOB_CARD_TRN_PK = JTSI.JOB_CARD_TRN_FK");
                    }
                }
                else
                {
                    if (Process == 1)
                    {
                        sb.Append("SELECT DISTINCT BST.PORT_MST_POL_FK,");
                        sb.Append("                BST.PORT_MST_POD_FK,");
                        sb.Append("                JTSE.COMMODITY_MST_FKS");
                        sb.Append("  FROM CONSOL_INVOICE_TBL     C,");
                        sb.Append("       CONSOL_INVOICE_TRN_TBL CIT,");
                        sb.Append("       JOB_CARD_TRN   JCSE,");
                        sb.Append("       JOB_TRN_CONT   JTSE,");
                        sb.Append("       BOOKING_MST_TBL        BST");
                        sb.Append(" WHERE C.CONSOL_INVOICE_PK = " + InvPK);
                        sb.Append("   AND C.BUSINESS_TYPE = " + BizType);
                        sb.Append("   AND C.PROCESS_TYPE = " + Process);
                        sb.Append("   AND C.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
                        sb.Append("   AND JCSE.JOB_CARD_TRN_PK = CIT.JOB_CARD_FK");
                        sb.Append("   AND BST.BOOKING_MST_PK = JCSE.BOOKING_MST_FK");
                        sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JTSE.JOB_CARD_TRN_FK");
                    }
                    else
                    {
                        sb.Append("SELECT DISTINCT JCSI.PORT_MST_POL_FK,");
                        sb.Append("                JCSI.PORT_MST_POD_FK,");
                        sb.Append("                JTSI.COMMODITY_MST_FKS");
                        sb.Append("  FROM CONSOL_INVOICE_TBL     C,");
                        sb.Append("       CONSOL_INVOICE_TRN_TBL CIT,");
                        sb.Append("       JOB_CARD_TRN   JCSI,");
                        sb.Append("       JOB_TRN_CONT   JTSI");
                        sb.Append(" WHERE C.CONSOL_INVOICE_PK = " + InvPK);
                        sb.Append("   AND C.BUSINESS_TYPE = " + BizType);
                        sb.Append("   AND C.PROCESS_TYPE = " + Process);
                        sb.Append("   AND C.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
                        sb.Append("   AND JCSI.JOB_CARD_TRN_PK = CIT.JOB_CARD_FK");
                        sb.Append("   AND JCSI.JOB_CARD_TRN_PK = JTSI.JOB_CARD_TRN_FK");
                    }
                }
                objDs = objWF.GetDataSet(sb.ToString());
                return objDs;
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
        #endregion

        #region "Fetch Location"
        public string FetchContainer(string ConsInvID)
        {

            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            DataSet ds = null;
            string contUnit = "";
            Int16 i = default(Int16);

            sb.Append("SELECT DISTINCT NVL(C.CONTAINER_NUMBER || ' / ' || CTMT.CONTAINER_TYPE_MST_ID, ' ') CONTAINERUNIT ");
            sb.Append("  FROM JOB_CARD_TRN   J,");
            sb.Append("       CONSOL_INVOICE_TBL     CON,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL INV,");
            sb.Append("       JOB_TRN_CONT   C,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMT");
            sb.Append(" WHERE CON.INVOICE_REF_NO = '" + ConsInvID + "'");
            sb.Append("   AND CON.CONSOL_INVOICE_PK = INV.CONSOL_INVOICE_FK");
            sb.Append("   AND INV.JOB_CARD_FK = J.JOB_CARD_TRN_PK");
            sb.Append("   AND C.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK");
            sb.Append("   AND CTMT.CONTAINER_TYPE_MST_PK = C.CONTAINER_TYPE_MST_FK");


            try
            {
                ds = ObjWk.GetDataSet(sb.ToString());

                for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                {
                    contUnit = contUnit + ds.Tables[0].Rows[i][0] + ", ";
                }

                return contUnit;

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
        #endregion

        #region "Fetch Location"
        public string FetchDimensions(string ConsInvID)
        {

            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            DataSet ds = null;
            string Dimension = "";
            Int16 i = default(Int16);

            sb.Append("SELECT DISTINCT NVL(JOB_CONT.PALETTE_SIZE,'') DIMENSIONS ");
            sb.Append("  FROM JOB_CARD_TRN JOB,");
            sb.Append("       JOB_TRN_CONT JOB_CONT,");
            sb.Append("       CONSOL_INVOICE_TBL CON,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL INV");
            sb.Append(" WHERE CON.INVOICE_REF_NO = '" + ConsInvID + "'");
            sb.Append("  AND CON.CONSOL_INVOICE_PK = INV.CONSOL_INVOICE_FK");
            sb.Append("   AND JOB.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_PK");
            sb.Append("    AND INV.JOB_CARD_FK= JOB.JOB_CARD_TRN_PK");


            try
            {
                ds = ObjWk.GetDataSet(sb.ToString());

                for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                {
                    Dimension = Dimension + ds.Tables[0].Rows[i][0] + ", ";
                }

                return Dimension;

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
        #endregion

        #region "Fetch Inv. and Coll. Status"
        public int FetchCollectionStatus(string ConsInvPK, string Fac = "")
        {

            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            DataSet ds = null;
            string Dimension = "";

            sb.Append("SELECT COUNT(*) COUNT");
            sb.Append("  FROM CONSOL_INVOICE_TBL INV");
            sb.Append(" WHERE  ROUND(NVL(INV.NET_RECEIVABLE *");
            sb.Append("                                  (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
            sb.Append(Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]) + ",");
            sb.Append("                                                      INV.INVOICE_DATE)");
            sb.Append("                                     FROM DUAL) -");
            sb.Append("                                  NVL(NVL((SELECT SUM(AMT) FROM (SELECT SUM(CLTTRN.RECD_AMOUNT_HDR_CURR) *");
            sb.Append("                                  (SELECT GET_EX_RATE(CLT.CURRENCY_MST_FK,");
            sb.Append(Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]) + ",");
            sb.Append("                                                       CLT.COLLECTIONS_DATE)");
            sb.Append("                                     FROM DUAL) AS AMT");
            sb.Append("                                            FROM COLLECTIONS_TBL     CLT,");
            sb.Append("                                                 COLLECTIONS_TRN_TBL CLTTRN");
            sb.Append("                                           WHERE CLT.COLLECTIONS_TBL_PK =");
            sb.Append("                                                 CLTTRN.COLLECTIONS_TBL_FK");
            sb.Append("                                             AND CLTTRN.INVOICE_REF_NR =");
            sb.Append(" (SELECT CIT.INVOICE_REF_NO FROM CONSOL_INVOICE_TBL CIT WHERE CIT.CONSOL_INVOICE_PK=" + ConsInvPK + ")");
            sb.Append("                                                 GROUP BY CLT.CURRENCY_MST_FK,CLT.COLLECTIONS_DATE)),");
            sb.Append("                                          0) + NVL((SELECT SUM(WRM.WRITEOFF_AMOUNT)");
            sb.Append("                                                     FROM WRITEOFF_MANUAL_TBL WRM");
            sb.Append("                                                    WHERE WRM.CONSOL_INVOICE_FK =");
            sb.Append("                                                          INV.CONSOL_INVOICE_PK),");
            sb.Append("                                                   0),");
            sb.Append("                                      0),");
            sb.Append("                                  0),");
            if (Fac == "FAC")
            {
                sb.Append("                              2) = 0");
            }
            else
            {
                sb.Append("                              2) <= 0");
            }

            sb.Append("   AND INV.CONSOL_INVOICE_PK =" + ConsInvPK);

            return Convert.ToInt32(ObjWk.ExecuteScaler(sb.ToString()));
        }

        #endregion

        #region " EnableTriggerForMenu "
        public void EnableTriggerForMenu(int MenuPk, short FLAG = 1)
        {
            WorkFlow objwf = new WorkFlow();
            OracleCommand SCM = new OracleCommand();

            try
            {
                objwf.OpenConnection();
                SCM.Connection = objwf.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objwf.MyUserName + ".MENU_WISE_ENABLE_TRIGGER_PKG.MENU_WISE_ENABLE_TRIGGER";

                var _with35 = SCM.Parameters;
                _with35.Clear();
                _with35.Add("MENU_PK_IN", MenuPk).Direction = ParameterDirection.Input;
                _with35.Add("FLAG", FLAG).Direction = ParameterDirection.Input;

                SCM.ExecuteNonQuery();

            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }
        #endregion

        #region "FETCH FAC INVOICE"
        public DataSet FetchConsolidatableFAC(string MBLPKs, string BIZ_TYPE, string Process, string SuppPk, bool Edit = false, int ExType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            int CurrencyPK = 0;
            string LocationPK = null;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                LocationPK = HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString();

                var _with36 = objWF.MyCommand.Parameters;
                _with36.Add("MBL_PK_IN", (string.IsNullOrEmpty(MBLPKs) ? "" : MBLPKs)).Direction = ParameterDirection.Input;
                _with36.Add("CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with36.Add("LOCATION_FK_IN", (string.IsNullOrEmpty(LocationPK) ? "" : LocationPK)).Direction = ParameterDirection.Input;
                _with36.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(BIZ_TYPE) ? "" : BIZ_TYPE)).Direction = ParameterDirection.Input;
                _with36.Add("MBL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (Convert.ToInt32(BIZ_TYPE) == 2)
                {
                    dsAll = objWF.GetDataSet("FETCH_FACINVOICE_NEW_PKG", "FETCH_FACINVOICE");
                }
                else
                {
                    dsAll = objWF.GetDataSet("FETCH_FACINVOICE_NEW_PKG", "FETCH_FACINVOICEAIR");
                }
                return dsAll;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchInvoiceDataFAC(string MBLPKs, int intInvPk, int nBaseCurrPK, string BIZ_TYPE, short Process, int UserPk, string SuppPk, string CreditLimit, string amount, int ExType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            int CurrencyPK = 0;
            string LocationPK = null;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                LocationPK = HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString();

                var _with37 = objWF.MyCommand.Parameters;
                _with37.Add("MBL_PK_IN", (string.IsNullOrEmpty(MBLPKs) ? "" : MBLPKs)).Direction = ParameterDirection.Input;
                _with37.Add("CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with37.Add("LOCATION_FK_IN", (string.IsNullOrEmpty(LocationPK) ? "" : LocationPK)).Direction = ParameterDirection.Input;
                _with37.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(BIZ_TYPE) ? "" : BIZ_TYPE)).Direction = ParameterDirection.Input;
                _with37.Add("PROCESS_TYPE_IN", (Process == 0 ? 1 : Process)).Direction = ParameterDirection.Input;
                _with37.Add("OPERATOR_MST_FK_IN", SuppPk).Direction = ParameterDirection.Input;
                _with37.Add("JOB_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_FACINVOICE_NEW_PKG", "FETCH_FACINVOICEJOB");
                return dsAll;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Consol FAC invoice Sub Details Report "
        public DataSet CONSOL_INV_DETAIL_SUB_MAIN_FAC_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with38 = objWK.MyCommand.Parameters;
                _with38.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with38.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with38.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with38.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_SUB_MAIN_FAC_RPT_PRINT");
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
        #endregion

        #region "Fetch FAC Shipment Details Report "
        public DataSet FAC_INV_SHIPMENT_DETAIL_MAIN_PRINT(string MBL_pk, int CURRENCY_FK, int LOG_IN_CURRENCY_FK, int LOCATION_FK, int Biz_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with39 = objWK.MyCommand.Parameters;
                _with39.Add("MBL_PK_IN", MBL_pk).Direction = ParameterDirection.Input;
                _with39.Add("CURRENCY_FK_IN", CURRENCY_FK).Direction = ParameterDirection.Input;
                _with39.Add("LOG_IN_LOC_CURR_FK_IN", LOG_IN_CURRENCY_FK).Direction = ParameterDirection.Input;
                _with39.Add("LOCATION_FK_IN", LOCATION_FK).Direction = ParameterDirection.Input;
                _with39.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with39.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (Biz_Type == 2)
                {
                    return objWK.GetDataSet("CONSOL_INV_PKG", "FETCH_FACINVOICESEA_PRINT");
                }
                else
                {
                    return objWK.GetDataSet("CONSOL_INV_PKG", "FETCH_FACINVOICEAIR_PRINT");
                }

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
        #endregion

        #region "Fetch FAC invoice Details Report "
        public DataSet FAC_INV_DETAIL_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type, string User_Name)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with40 = objWK.MyCommand.Parameters;
                _with40.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with40.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with40.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with40.Add("USER_NAME_IN", User_Name).Direction = ParameterDirection.Input;
                _with40.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_DET_FAC_MAIN_RPT_PRINT");
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
        #endregion

        #region "Fetch FAC Consol invoice Supplier "
        public DataSet CONSOL_FAC_SUPP_RPT_PRINT(int Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with41 = objWK.MyCommand.Parameters;
                _with41.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with41.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with41.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with41.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_FAC_SUPP_RPT_PRINT");
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
        #endregion

        #region "Fetch Record Count"
        public int FetchCollectionCount(string ConsInvPK)
        {

            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT COUNT(*)");
            sb.Append("  FROM COLLECTIONS_TBL C, COLLECTIONS_TRN_TBL CC, CONSOL_INVOICE_TBL INV");
            sb.Append(" WHERE C.COLLECTIONS_TBL_PK = CC.COLLECTIONS_TBL_FK");
            sb.Append("   AND CC.INVOICE_REF_NR = INV.INVOICE_REF_NO");
            sb.Append("   AND INV.CONSOL_INVOICE_PK =" + ConsInvPK);
            return Convert.ToInt32(ObjWk.ExecuteScaler(sb.ToString()));
        }
        #endregion

        #region "Cust Category"
        public int FetchCategory(int CustPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("Select COUNT(C.CUSTOMER_CATEGORY_ID)");
            sb.Append(" FROM CUSTOMER_CATEGORY_MST_TBL C, CUSTOMER_CATEGORY_TRN CTRN");
            sb.Append(" WHERE C.CUSTOMER_CATEGORY_MST_PK = CTRN.CUSTOMER_CATEGORY_MST_FK");
            sb.Append(" AND CTRN.CUSTOMER_MST_FK = " + CustPk);
            sb.Append(" AND C.CUSTOMER_CATEGORY_ID = 'VENDOR'");
            return Convert.ToInt32(ObjWk.ExecuteScaler(sb.ToString()));
        }
        public DataSet FetchBank()
        {
            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                ObjWk.OpenConnection();
                ObjWk.MyCommand.Connection = ObjWk.MyConnection;
                var _with42 = ObjWk.MyCommand;
                _with42.CommandType = CommandType.StoredProcedure;
                _with42.CommandText = ObjWk.MyUserName + ".CONSOL_INV_PKG.FETCH_BANK";
                ObjWk.MyCommand.Parameters.Clear();
                var _with43 = ObjWk.MyCommand.Parameters;
                _with43.Add("LOCATION_PK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with43.Add("BANK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                ObjWk.MyDataAdapter.SelectCommand = ObjWk.MyCommand;
                ObjWk.MyDataAdapter.Fill(dsData);
                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch JobType"
        public string FetchJobType(int InvPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            try
            {
                sb.Append(" SELECT ROWTOCOL('SELECT DECODE(CITT.JOB_TYPE,");
                sb.Append(" ''1'',");
                sb.Append(" ''JobCard'',");
                sb.Append(" ''2'',");
                sb.Append(" ''CustomsBrokerage'',");
                sb.Append(" ''3'',");
                sb.Append(" ''TansportNote'')from CONSOL_INVOICE_TRN_TBL CITT");
                sb.Append(" WHERE CITT.CONSOL_INVOICE_FK = " + InvPK + "') FROM DUAL");

                return ObjWk.ExecuteScaler(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }
        #endregion

        #region "Fetch Container PK"
        public string FetchContPK(string ContType)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            try
            {
                sb.Append(" SELECT ROWTOCOL('SELECT CTMT.CONTAINER_TYPE_MST_PK ");
                sb.Append(" FROM CONTAINER_TYPE_MST_TBL CTMT  ");
                sb.Append(" WHERE CTMT.CONTAINER_TYPE_MST_ID IN (" + ContType + ")') CONPKS FROM DUAL ");

                return ObjWk.ExecuteScaler(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }
        #endregion

        #region "FetchInvModifiedUserDetails"
        public DataSet FetchInvModifiedUserDetails(int invPk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append(" select ");
            strBuilder.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
            strBuilder.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            strBuilder.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            strBuilder.Append("   TO_DATE(CI.CREATED_DT) CREATED_DT, ");
            strBuilder.Append("   TO_DATE(CI.LAST_MODIFIED_DT) LAST_MODIFIED_DT,");
            strBuilder.Append("   TO_DATE(CI.LAST_MODIFIED_DT) APPROVED_DT ");
            strBuilder.Append(" from consol_invoice_tbl ci,customer_mst_tbl cm,currency_type_mst_tbl ct, BANK_MST_TBL BMT, ");
            strBuilder.Append("  USER_MST_TBL UMTCRT, ");
            strBuilder.Append("  USER_MST_TBL UMTUPD, ");
            strBuilder.Append("  USER_MST_TBL UMTAPP ");
            strBuilder.Append(" where ci.consol_invoice_pk = " + invPk + "");
            strBuilder.Append(" and cm.customer_mst_pk = ci.customer_mst_fk and");
            strBuilder.Append(" ct.currency_mst_pk = ci.currency_mst_fk");
            strBuilder.Append(" AND BMT.BANK_MST_PK(+) = CI.BANK_MST_FK");
            strBuilder.Append(" AND UMTCRT.USER_MST_PK(+) = CI.CREATED_BY_FK ");
            strBuilder.Append(" AND UMTUPD.USER_MST_PK(+) = CI.LAST_MODIFIED_BY_FK  ");
            strBuilder.Append(" AND UMTAPP.USER_MST_PK(+) = CI.LAST_MODIFIED_BY_FK  ");
            try
            {
                return objWK.GetDataSet(strBuilder.ToString());
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
        #endregion

        #region "FetchInvModifiedUserDetailsInvoice"
        public DataSet FetchInvModifiedUserDetailsInvoice(int invPk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append(" select ");
            strBuilder.Append("   DISTINCT UMTCRT.USER_NAME    AS CREATED_BY, ");
            strBuilder.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            strBuilder.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            strBuilder.Append("   TO_DATE(CI.CREATED_DT) CREATED_DT, ");
            strBuilder.Append("   TO_DATE(CI.LAST_MODIFIED_DT) LAST_MODIFIED_DT,");
            strBuilder.Append("   TO_DATE(CI.LAST_MODIFIED_DT) APPROVED_DT ");
            strBuilder.Append(" from INV_AGENT_TBL ci,customer_mst_tbl cm,currency_type_mst_tbl ct, BANK_MST_TBL BMT,INV_AGENT_TRN_TBL IATT, JOB_CARD_TRN  JCT, ");
            strBuilder.Append("  USER_MST_TBL UMTCRT, ");
            strBuilder.Append("  USER_MST_TBL UMTUPD, ");
            strBuilder.Append("  USER_MST_TBL UMTAPP ");
            strBuilder.Append(" where ci.INV_AGENT_PK = " + invPk + "");
            strBuilder.Append(" and cm.customer_mst_pk(+) = ci.AGENT_MST_FK and");
            strBuilder.Append(" ct.currency_mst_pk = ci.currency_mst_fk");
            //strBuilder.Append(" AND BMT.BANK_MST_PK(+) = CI.BANK_MST_FK")
            strBuilder.Append(" AND UMTCRT.USER_MST_PK(+) = CI.CREATED_BY_FK ");
            strBuilder.Append(" AND UMTUPD.USER_MST_PK(+) = CI.LAST_MODIFIED_BY_FK  ");
            strBuilder.Append(" AND UMTAPP.USER_MST_PK(+) = CI.LAST_MODIFIED_BY_FK  ");

            strBuilder.Append(" AND CI.INV_AGENT_PK(+) = IATT.INV_AGENT_FK ");
            strBuilder.Append(" AND UMTAPP.USER_MST_PK(+) = CI.LAST_MODIFIED_BY_FK  ");
            try
            {
                return objWK.GetDataSet(strBuilder.ToString());
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
        #endregion

        #region "FetchCurContSummary"
        public DataSet FetchCurContSummary1(int invPk)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("SELECT DISTINCT CITT.CONSOL_INVOICE_FK,");
            sb.Append("                CTMT.CONTAINER_TYPE_MST_ID,");
            sb.Append("                CTMT.CURRENCY_ID,");
            sb.Append("      (CITT.ELEMENT_AMT * CITT.EXCHANGE_RATE) TOTAMT,");
            sb.Append("    (((CITT.ELEMENT_AMT * CITT.EXCHANGE_RATE)) +(NVL(CITT.TAX_AMT, 0))) INVOICE,");
            sb.Append("      FEMT.PREFERENCE");
            sb.Append("  FROM CONSOL_INVOICE_TRN_TBL  CITT,");
            sb.Append("       CONSOL_INVOICE_TBL      CIT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT,");
            sb.Append("       JOB_TRN_FD              JCFD,");
            sb.Append("       JOB_CARD_TRN            JCSE,");
            sb.Append("       JOB_TRN_CONT            JCTC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT");
            sb.Append(" WHERE CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
            sb.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CITT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
            sb.Append("   AND CITT.JOB_CARD_FK = JCSE.JOB_CARD_TRN_PK(+)");
            sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JCFD.JOB_CARD_TRN_FK");
            sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JCTC.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND JCTC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND JCFD.FREIGHT_ELEMENT_MST_FK = CITT.FRT_OTH_ELEMENT_FK");
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append(" ORDER BY FEMT.PREFERENCE");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "ContCount"
        public DataSet FetchContCount1(int invPk)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("SELECT  CITT.CONSOL_INVOICE_FK,");
            sb.Append("                COUNT(CY.CONTAINER_TYPE_MST_ID) || ' X ' ||");
            sb.Append("                CY.CONTAINER_TYPE_MST_ID AS PACK");
            sb.Append("  FROM JOB_TRN_CONT            JC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("       CONSOL_INVOICE_TBL      CIT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL  CITT,");
            sb.Append("       JOB_CARD_TRN            JCSE");
            sb.Append(" WHERE CY.CONTAINER_TYPE_MST_PK = JC.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
            sb.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CITT.JOB_CARD_FK = JCSE.JOB_CARD_TRN_PK(+)");
            sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JC.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append(" GROUP BY CITT.CONSOL_INVOICE_FK,");
            sb.Append("          JC.CONTAINER_TYPE_MST_FK,");
            sb.Append("          CY.CONTAINER_TYPE_MST_ID");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "NewContSummary"
        public DataSet FetchContCount(int invPk)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            //sb.Append("SELECT DISTINCT CITT.CONSOL_INVOICE_FK,")
            //sb.Append("              JCSE.JOB_CARD_TRN_PK,")
            //sb.Append("              JCSE.JOBCARD_REF_NO,")
            //sb.Append("                JCSE.JOB_CARD_TRN_PK,")
            //sb.Append("                (SELECT ROWTOCOL('SELECT  COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || '' X '' ||")
            //sb.Append("                CY.CONTAINER_TYPE_MST_ID AS PACK")
            //sb.Append("  FROM JOB_TRN_CONT            JC,")
            //sb.Append("       CONTAINER_TYPE_MST_TBL  CY,")
            //sb.Append("       CONSOL_INVOICE_TBL      CIT,")
            //sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,")
            //sb.Append("       CONSOL_INVOICE_TRN_TBL  CITT,")
            //sb.Append("       JOB_CARD_TRN            JCSE")
            //sb.Append(" WHERE CY.CONTAINER_TYPE_MST_PK = JC.CONTAINER_TYPE_MST_FK")
            //sb.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK")
            //sb.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK")
            //sb.Append("   AND CITT.JOB_CARD_FK = JCSE.JOB_CARD_TRN_PK(+)")
            //sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JC.JOB_CARD_TRN_FK(+)")
            //sb.Append("   AND CIT.CONSOL_INVOICE_PK = " & invPk & "")
            //sb.Append(" GROUP BY CITT.CONSOL_INVOICE_FK,")
            //sb.Append("          JC.CONTAINER_TYPE_MST_FK,")
            //sb.Append("          CY.CONTAINER_TYPE_MST_ID')")
            //sb.Append("  FROM DUAL) P")
            //sb.Append("  FROM CONSOL_INVOICE_TBL      CIT,")
            //sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,")
            //sb.Append("       CONSOL_INVOICE_TRN_TBL  CITT,")
            //sb.Append("       JOB_CARD_TRN            JCSE,")
            //sb.Append("        JOB_TRN_CONT JCTC,")
            //sb.Append("        JOB_TRN_FD   JFD    ,")
            //sb.Append("          CURRENCY_TYPE_MST_TBL   CTMT   ")
            //sb.Append(" WHERE CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK")
            //sb.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK")
            //sb.Append("   AND CITT.JOB_CARD_FK = JCSE.JOB_CARD_TRN_PK")
            //sb.Append("   AND JFD.FREIGHT_ELEMENT_MST_FK = CITT.FRT_OTH_ELEMENT_FK")
            //sb.Append("      AND JCSE.JOB_CARD_TRN_PK = JCTC.JOB_CARD_TRN_FK(+)")
            //sb.Append("        AND CIT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK")
            //sb.Append("   AND CIT.CONSOL_INVOICE_PK = " & invPk & "")

            sb.Append("SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' || CY.CONTAINER_TYPE_MST_ID AS PACK, JCSE.JOBCARD_REF_NO");
            sb.Append("  FROM JOB_TRN_CONT            JC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("       CONSOL_INVOICE_TBL      CIT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL  CITT,");
            sb.Append("       JOB_CARD_TRN            JCSE,");
            sb.Append("       JOB_TRN_FD              JCFD");
            sb.Append(" WHERE CY.CONTAINER_TYPE_MST_PK = JC.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
            sb.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CITT.JOB_CARD_FK = JCSE.JOB_CARD_TRN_PK(+)");
            sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JC.JOB_CARD_TRN_FK(+)");
            ///''
            sb.Append("   AND JCFD.CONSOL_INVOICE_TRN_FK = CITT.CONSOL_INVOICE_TRN_PK");
            sb.Append("   AND JCFD.JOB_TRN_CONT_FK = JC.JOB_TRN_CONT_PK");
            ///''
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append(" GROUP BY CITT.CONSOL_INVOICE_FK,");
            sb.Append("          JC.CONTAINER_TYPE_MST_FK,");
            sb.Append("          CY.CONTAINER_TYPE_MST_ID,JCSE.JOBCARD_REF_NO");
            sb.Append("  UNION");
            sb.Append("  SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' || CY.CONTAINER_TYPE_MST_ID AS PACK, CT.CBJC_NO AS JOBCARD_REF_NO");
            sb.Append("  FROM CBJC_TRN_CONT            CBJC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("       CONSOL_INVOICE_TBL      CIT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL  CITT,");
            sb.Append("       CBJC_TBL            CT");
            sb.Append("  WHERE CY.CONTAINER_TYPE_MST_PK = CBJC.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
            sb.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CITT.JOB_CARD_FK = CT.CBJC_PK(+)");
            sb.Append("   AND CT.CBJC_PK = CBJC.CBJC_FK(+)");
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("   AND CITT.JOB_TYPE=2");
            sb.Append("  GROUP BY CITT.CONSOL_INVOICE_FK,");
            sb.Append("          CBJC.CONTAINER_TYPE_MST_FK,");
            sb.Append("          CY.CONTAINER_TYPE_MST_ID,CT.CBJC_NO");
            sb.Append("  UNION ");
            sb.Append("  SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' || CY.CONTAINER_TYPE_MST_ID AS PACK,TPN.TRANS_INST_REF_NO AS JOBCARD_REF_NO");
            sb.Append("  FROM TRANSPORT_TRN_CONT            TTC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("       CONSOL_INVOICE_TBL      CIT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL  CITT,");
            sb.Append("       TRANSPORT_INST_SEA_TBL  TPN");
            sb.Append(" WHERE CY.CONTAINER_TYPE_MST_PK = TTC.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
            sb.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CITT.JOB_CARD_FK = TPN.TRANSPORT_INST_SEA_PK(+)");
            sb.Append("   AND TPN.TRANSPORT_INST_SEA_PK = TTC.TRANSPORT_INST_FK(+)");
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("   AND CITT.JOB_TYPE=3");
            sb.Append(" GROUP BY CITT.CONSOL_INVOICE_FK,");
            sb.Append("          TTC.CONTAINER_TYPE_MST_FK,");
            sb.Append("          CY.CONTAINER_TYPE_MST_ID,TPN.TRANS_INST_REF_NO");
            sb.Append("  UNION ");
            //sb.Append("  SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' || CY.CONTAINER_TYPE_MST_ID AS PACK,DCH.DEM_CALC_ID AS JOBCARD_REF_NO")
            //sb.Append("              FROM TRANSPORT_TRN_CONT            TTC,")
            //sb.Append("                   CONTAINER_TYPE_MST_TBL  CY,")
            //sb.Append("                   CONSOL_INVOICE_TBL      CIT,")
            //sb.Append("                   FREIGHT_ELEMENT_MST_TBL FEMT,")
            //sb.Append("                   CONSOL_INVOICE_TRN_TBL  CITT,")
            //sb.Append("                   DEM_CALC_HDR            DCH,")
            //sb.Append("                   TRANSPORT_INST_SEA_TBL TCT1,")
            //sb.Append("                    TRANSPORT_TRN_FD        TTF")
            //sb.Append("             WHERE CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK")
            //sb.Append("                  AND CITT.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK")
            //sb.Append("                  AND CY.CONTAINER_TYPE_MST_PK = TTC.CONTAINER_TYPE_MST_FK")
            //sb.Append("                  AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK")
            //sb.Append("                 AND TTF.FREIGHT_ELEMENT_MST_FK = CITT.FRT_OTH_ELEMENT_FK   ")
            //sb.Append("                 AND TCT1.TRANSPORT_INST_SEA_PK = DCH.DOC_REF_FK")
            //sb.Append("                 AND TTC.TRANSPORT_INST_FK = TCT1.TRANSPORT_INST_SEA_PK")
            //sb.Append("     AND CIT.CONSOL_INVOICE_PK = " & invPk & "")
            //sb.Append("             GROUP BY CITT.CONSOL_INVOICE_FK,")
            //sb.Append("                      TTC.CONTAINER_TYPE_MST_FK,")
            //sb.Append("                      CY.CONTAINER_TYPE_MST_ID,DCH.DEM_CALC_ID")
            sb.Append("   SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' || CY.CONTAINER_TYPE_MST_ID AS PACK,DCH.DEM_CALC_ID AS JOBCARD_REF_NO");
            sb.Append("              FROM CONSOL_INVOICE_TBL      CIT,");
            sb.Append("                   CONSOL_INVOICE_TRN_TBL  CITT,");
            sb.Append("                   DEM_CALC_HDR            DCH,");
            sb.Append("                   TRANSPORT_INST_SEA_TBL TCT1,");
            sb.Append("                   CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("                   TRANSPORT_TRN_CONT     TTC,");
            sb.Append("                   TRANSPORT_TRN_FD        TTF");
            sb.Append("             WHERE  CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("                  AND CITT.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK");
            sb.Append("                  AND TCT1.TRANSPORT_INST_SEA_PK=TTC.TRANSPORT_INST_FK");
            sb.Append("                  AND CY.CONTAINER_TYPE_MST_PK = TTC.CONTAINER_TYPE_MST_FK");
            sb.Append("                   AND TTF.FREIGHT_ELEMENT_MST_FK = CITT.FRT_OTH_ELEMENT_FK");
            sb.Append("                   AND TTF.TRANSPORT_INST_FK = TCT1.TRANSPORT_INST_SEA_PK");
            sb.Append("      AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("                  GROUP BY CITT.CONSOL_INVOICE_FK,");
            sb.Append("                      TTC.CONTAINER_TYPE_MST_FK,");
            sb.Append("                      CY.CONTAINER_TYPE_MST_ID,DCH.DEM_CALC_ID");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "FetchCurContSummary1"
        public DataSet FetchCurContSummary(int invPk)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            //sb.Append("SELECT CTMT.CURRENCY_ID, sum(CITT.ELEMENT_AMT) AS INVOICE_AMT ")
            sb.Append("SELECT JT.JOBCARD_REF_NO,CTMT.CURRENCY_ID, SUM(CITT.TOT_AMT) AS INVOICE_AMT ");
            sb.Append("          FROM CONSOL_INVOICE_TRN_TBL CITT,");
            sb.Append("               CONSOL_INVOICE_TBL     CIT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL  CTMT,");
            sb.Append("               JOB_CARD_TRN JT");
            sb.Append("          WHERE CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("           AND CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("            AND JT.JOB_CARD_TRN_PK = CITT.JOB_CARD_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = CITT.CURRENCY_MST_FK GROUP BY CTMT.CURRENCY_ID,JT.JOBCARD_REF_NO ");
            sb.Append("  UNION ");
            sb.Append("  SELECT CB.CBJC_NO, CTMT.CURRENCY_ID, SUM(CITT.TOT_AMT) AS INVOICE_AMT");
            sb.Append("  FROM CONSOL_INVOICE_TRN_TBL CITT,");
            sb.Append("       CONSOL_INVOICE_TBL     CIT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT,");
            sb.Append("       CBJC_TBL               CB");
            sb.Append("          WHERE CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("   AND CB.CBJC_PK = CITT.JOB_CARD_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = CITT.CURRENCY_MST_FK");
            sb.Append(" GROUP BY CTMT.CURRENCY_ID, CB.CBJC_NO");
            sb.Append("  UNION");
            sb.Append("  SELECT TIST.TRANS_INST_REF_NO, CTMT.CURRENCY_ID, SUM(CITT.TOT_AMT) AS INVOICE_AMT");
            sb.Append("  FROM CONSOL_INVOICE_TRN_TBL CITT,");
            sb.Append("       CONSOL_INVOICE_TBL     CIT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT,");
            sb.Append("       TRANSPORT_INST_SEA_TBL   TIST");
            sb.Append("          WHERE CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("   AND TIST.TRANSPORT_INST_SEA_PK = CITT.JOB_CARD_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = CITT.CURRENCY_MST_FK");
            sb.Append("  GROUP BY CTMT.CURRENCY_ID, TIST.TRANS_INST_REF_NO");
            sb.Append("  UNION");
            //sb.Append("    SELECT DCH.DEM_CALC_ID,")
            //sb.Append("       CTMT.CURRENCY_ID,")
            //sb.Append("       SUM(CITT.TOT_AMT) AS INVOICE_AMT")
            //sb.Append("  FROM CONSOL_INVOICE_TRN_TBL CITT,")
            //sb.Append("       CONSOL_INVOICE_TBL     CIT,")
            //sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT,")
            //sb.Append("       DEM_CALC_HDR            DCH,")
            //sb.Append("       TRANSPORT_INST_SEA_TBL TCT")
            //sb.Append("          WHERE CIT.CONSOL_INVOICE_PK = " & invPk & "")
            //sb.Append("      AND CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK")
            //sb.Append("      AND TCT.TRANSPORT_INST_SEA_PK = DCH.DOC_REF_FK")
            //sb.Append("      AND CTMT.CURRENCY_MST_PK = CITT.CURRENCY_MST_FK")
            //sb.Append("      AND CITT.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK")
            //sb.Append("      GROUP BY CTMT.CURRENCY_ID, DCH.DEM_CALC_ID")
            sb.Append("     SELECT DCH.DEM_CALC_ID,");
            sb.Append("                   CTMT.CURRENCY_ID,");
            sb.Append("                   SUM(CITT.TOT_AMT) AS INVOICE_AMT");
            sb.Append("              FROM CONSOL_INVOICE_TRN_TBL CITT,");
            sb.Append("                   CONSOL_INVOICE_TBL     CIT,");
            sb.Append("                   CURRENCY_TYPE_MST_TBL  CTMT,");
            sb.Append("                   DEM_CALC_HDR            DCH");
            sb.Append("          WHERE CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("                  AND CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("                  AND CTMT.CURRENCY_MST_PK = CITT.CURRENCY_MST_FK");
            sb.Append("                  AND CITT.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK");
            sb.Append("                  GROUP BY CTMT.CURRENCY_ID, DCH.DEM_CALC_ID");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion


        #region "FetchCurContSummarYInvoiceToAgent"
        public DataSet FetchCurContSummaryInvoiceToAgent(int invPk)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("   SELECT JT.JOBCARD_REF_NO,CTMT.CURRENCY_ID, SUM((IATT.ELEMENT_AMT * IATT.EXCHANGE_RATE) + IATT.TAX_AMT) AS INVOICE_AMT");
            sb.Append("   FROM INV_AGENT_TBL         IGT,");
            sb.Append("       INV_AGENT_TRN_TBL     IATT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CTMT,");
            sb.Append("        JOB_CARD_TRN JT");
            sb.Append("     WHERE IGT.INV_AGENT_PK= " + invPk + "");
            sb.Append("   AND IGT.INV_AGENT_PK = IATT.INV_AGENT_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = IATT.CURRENCY_MST_FK");
            sb.Append("    AND JT.JOB_CARD_TRN_PK = IGT.JOB_CARD_FK");
            sb.Append("   GROUP BY CTMT.CURRENCY_ID,JT.JOBCARD_REF_NO");
            sb.Append("    UNION");
            sb.Append("    SELECT  CB.CBJC_NO,CTMT.CURRENCY_ID, SUM((IATT.ELEMENT_AMT * IATT.EXCHANGE_RATE) + IATT.TAX_AMT) AS INVOICE_AMT");
            sb.Append("    FROM INV_AGENT_TBL         IGT,");
            sb.Append("       INV_AGENT_TRN_TBL     IATT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CTMT,");
            sb.Append("        CBJC_TBL               CB");
            sb.Append("     WHERE IGT.INV_AGENT_PK= " + invPk + "");
            sb.Append("   AND IGT.INV_AGENT_PK = IATT.INV_AGENT_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = IATT.CURRENCY_MST_FK");
            sb.Append("    AND CB.CBJC_PK = IGT.JOB_CARD_FK");
            sb.Append("   GROUP BY CTMT.CURRENCY_ID,CB.CBJC_NO");
            sb.Append("  UNION");
            sb.Append("   SELECT TIST.TRANS_INST_REF_NO,CTMT.CURRENCY_ID, SUM((IATT.ELEMENT_AMT * IATT.EXCHANGE_RATE) + IATT.TAX_AMT) AS INVOICE_AMT");
            sb.Append("  FROM INV_AGENT_TBL         IGT,");
            sb.Append("       INV_AGENT_TRN_TBL     IATT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CTMT,");
            sb.Append("        TRANSPORT_INST_SEA_TBL   TIST");
            sb.Append("     WHERE IGT.INV_AGENT_PK= " + invPk + "");
            sb.Append("   AND IGT.INV_AGENT_PK = IATT.INV_AGENT_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = IATT.CURRENCY_MST_FK");
            sb.Append("     AND TIST.TRANSPORT_INST_SEA_PK = IGT.JOB_CARD_FK");
            sb.Append("   GROUP BY CTMT.CURRENCY_ID,TIST.TRANS_INST_REF_NO");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "NewContSummary"
        public DataSet FetchContCountInvoiceToAgent(int invPk)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            //sb.Append("SELECT DISTINCT CITT.CONSOL_INVOICE_FK,")
            //sb.Append("              JCSE.JOB_CARD_TRN_PK,")
            //sb.Append("              JCSE.JOBCARD_REF_NO,")
            //sb.Append("                JCSE.JOB_CARD_TRN_PK,")
            //sb.Append("                (SELECT ROWTOCOL('SELECT  COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || '' X '' ||")
            //sb.Append("                CY.CONTAINER_TYPE_MST_ID AS PACK")
            //sb.Append("  FROM JOB_TRN_CONT            JC,")
            //sb.Append("       CONTAINER_TYPE_MST_TBL  CY,")
            //sb.Append("       CONSOL_INVOICE_TBL      CIT,")
            //sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,")
            //sb.Append("       CONSOL_INVOICE_TRN_TBL  CITT,")
            //sb.Append("       JOB_CARD_TRN            JCSE")
            //sb.Append(" WHERE CY.CONTAINER_TYPE_MST_PK = JC.CONTAINER_TYPE_MST_FK")
            //sb.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK")
            //sb.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK")
            //sb.Append("   AND CITT.JOB_CARD_FK = JCSE.JOB_CARD_TRN_PK(+)")
            //sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JC.JOB_CARD_TRN_FK(+)")
            //sb.Append("   AND CIT.CONSOL_INVOICE_PK = " & invPk & "")
            //sb.Append(" GROUP BY CITT.CONSOL_INVOICE_FK,")
            //sb.Append("          JC.CONTAINER_TYPE_MST_FK,")
            //sb.Append("          CY.CONTAINER_TYPE_MST_ID')")
            //sb.Append("  FROM DUAL) P")
            //sb.Append("  FROM CONSOL_INVOICE_TBL      CIT,")
            //sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,")
            //sb.Append("       CONSOL_INVOICE_TRN_TBL  CITT,")
            //sb.Append("       JOB_CARD_TRN            JCSE,")
            //sb.Append("        JOB_TRN_CONT JCTC,")
            //sb.Append("        JOB_TRN_FD   JFD    ,")
            //sb.Append("          CURRENCY_TYPE_MST_TBL   CTMT   ")
            //sb.Append(" WHERE CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK")
            //sb.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK")
            //sb.Append("   AND CITT.JOB_CARD_FK = JCSE.JOB_CARD_TRN_PK")
            //sb.Append("   AND JFD.FREIGHT_ELEMENT_MST_FK = CITT.FRT_OTH_ELEMENT_FK")
            //sb.Append("      AND JCSE.JOB_CARD_TRN_PK = JCTC.JOB_CARD_TRN_FK(+)")
            //sb.Append("        AND CIT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK")
            //sb.Append("   AND CIT.CONSOL_INVOICE_PK = " & invPk & "")

            sb.Append("SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' ||");
            sb.Append("       CY.CONTAINER_TYPE_MST_ID AS PACK, JCSE.JOBCARD_REF_NO");
            sb.Append("      FROM JOB_TRN_CONT            JC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("       INV_AGENT_TBL      CIT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       INV_AGENT_TRN_TBL  CITT,");
            sb.Append("       JOB_CARD_TRN            JCSE");
            sb.Append("   WHERE CY.CONTAINER_TYPE_MST_PK = JC.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND CITT.INV_AGENT_FK = CIT.INV_AGENT_PK");
            sb.Append("   AND CITT.COST_FRT_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CIT.JOB_CARD_FK = JCSE.JOB_CARD_TRN_PK(+)");
            sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JC.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND CIT.INV_AGENT_PK = " + invPk + "");
            sb.Append("   GROUP BY CITT.INV_AGENT_FK,");
            sb.Append("          JC.CONTAINER_TYPE_MST_FK,");
            sb.Append("          CY.CONTAINER_TYPE_MST_ID,JCSE.JOBCARD_REF_NO");
            sb.Append("  UNION  ");
            sb.Append("   SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' ||");
            sb.Append("       CY.CONTAINER_TYPE_MST_ID AS PACK, CT.CBJC_NO AS JOBCARD_REF_NO");
            sb.Append("  FROM CBJC_TRN_CONT           CBJC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("       INV_AGENT_TBL      CIT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       INV_AGENT_TRN_TBL  CITT,");
            sb.Append("       CBJC_TBL           CT,");
            sb.Append("       JOB_CARD_TRN       JCT");
            sb.Append("       ");
            sb.Append("   WHERE CY.CONTAINER_TYPE_MST_PK = CBJC.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND CITT.INV_AGENT_FK = CIT.INV_AGENT_PK");
            sb.Append("   AND CITT.COST_FRT_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CIT.JOB_CARD_FK = CT.CBJC_PK(+)");
            sb.Append("   AND CT.CBJC_PK = CBJC.CBJC_FK(+)");
            sb.Append("   AND CIT.INV_AGENT_PK = " + invPk + "");
            sb.Append("  GROUP BY CITT.INV_AGENT_FK,");
            sb.Append("          CBJC.CONTAINER_TYPE_MST_FK,");
            sb.Append("          CY.CONTAINER_TYPE_MST_ID,CT.CBJC_NO");
            sb.Append("   UNION");
            sb.Append("   SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' ||");
            sb.Append("       CY.CONTAINER_TYPE_MST_ID AS PACK,TPN.TRANS_INST_REF_NO AS JOBCARD_REF_NO");
            sb.Append("  FROM TRANSPORT_TRN_CONT      TTC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("       INV_AGENT_TBL      CIT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       INV_AGENT_TRN_TBL  CITT,");
            sb.Append("       TRANSPORT_INST_SEA_TBL  TPN");
            sb.Append(" WHERE CY.CONTAINER_TYPE_MST_PK = TTC.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND CITT.INV_AGENT_FK = CIT.INV_AGENT_PK");
            sb.Append("   AND CITT.COST_FRT_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CIT.JOB_CARD_FK = TPN.TRANSPORT_INST_SEA_PK(+)");
            sb.Append("   AND TPN.TRANSPORT_INST_SEA_PK = TTC.TRANSPORT_INST_FK(+)");
            sb.Append("   AND CIT.INV_AGENT_PK = " + invPk + "");
            sb.Append("   GROUP BY CITT.INV_AGENT_FK,");
            sb.Append("   TTC.CONTAINER_TYPE_MST_FK,");
            sb.Append("   CY.CONTAINER_TYPE_MST_ID,TPN.TRANS_INST_REF_NO ");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion
        #region "FETCHCARGOTYPE"
        public DataSet FetchCargoType(int invPk)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("SELECT DISTINCT JCSE.CARGO_TYPE FROM ");
            sb.Append("                       CONSOL_INVOICE_TBL      CIT,");
            sb.Append("                       CONSOL_INVOICE_TRN_TBL CITT,");
            sb.Append("                       JOB_CARD_TRN            JCSE");
            sb.Append("                       WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("                      AND JCSE.JOB_CARD_TRN_PK=CITT.JOB_CARD_FK");
            sb.Append("                      AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("                      UNION");
            sb.Append("                      SELECT DISTINCT CT.CARGO_TYPE FROM ");
            sb.Append("                       CONSOL_INVOICE_TBL      CIT,");
            sb.Append("                       CONSOL_INVOICE_TRN_TBL CITT,");
            sb.Append("                       CBJC_TBL           CT");
            sb.Append("                 WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("                      AND CT.CBJC_PK=CITT.JOB_CARD_FK");
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("                      UNION");
            sb.Append("                      SELECT DISTINCT TP.CARGO_TYPE FROM ");
            sb.Append("                       CONSOL_INVOICE_TBL      CIT,");
            sb.Append("                       CONSOL_INVOICE_TRN_TBL CITT,");
            sb.Append("                       Transport_Inst_Sea_Tbl           TP");
            sb.Append("                 WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("                      AND TP.TRANSPORT_INST_SEA_PK=CITT.JOB_CARD_FK");
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            sb.Append("                      UNION");
            sb.Append("          SELECT DISTINCT DP.CARGO_TYPE FROM ");
            sb.Append("                       CONSOL_INVOICE_TBL      CIT,");
            sb.Append("                       CONSOL_INVOICE_TRN_TBL CITT,");
            sb.Append("                        DEM_CALC_HDR DP");
            sb.Append("                 WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("                      AND DP.DEM_CALC_HDR_PK=CITT.JOB_CARD_FK");
            sb.Append("   AND CIT.CONSOL_INVOICE_PK = " + invPk + "");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "FETCHCARGOTYPEINVG"
        public DataSet FetchCargoTypeIVG(int invPk)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append(" SELECT DISTINCT JCSE.CARGO_TYPE");
            sb.Append("  FROM INV_AGENT_TBL CIT, INV_AGENT_TRN_TBL CITT, JOB_CARD_TRN JCSE");
            sb.Append("  WHERE CIT.INV_AGENT_PK = CITT.INV_AGENT_FK");
            sb.Append("   AND JCSE.JOB_CARD_TRN_PK = CIT.JOB_CARD_FK");
            sb.Append("   AND CIT.INV_AGENT_PK = " + invPk + "");
            sb.Append("  UNION");
            sb.Append("  SELECT DISTINCT CT.CARGO_TYPE");
            sb.Append("  FROM INV_AGENT_TBL CIT, INV_AGENT_TRN_TBL CITT, CBJC_TBL CT");
            sb.Append(" WHERE CIT.INV_AGENT_PK = CITT.INV_AGENT_FK");
            sb.Append("   AND CT.CBJC_PK = CIT.JOB_CARD_FK");
            sb.Append("   AND CIT.INV_AGENT_PK = " + invPk + "");
            sb.Append("  UNION");
            sb.Append("  SELECT DISTINCT TP.CARGO_TYPE");
            sb.Append("  FROM INV_AGENT_TBL CIT, INV_AGENT_TRN_TBL CITT, TRANSPORT_INST_SEA_TBL TP");
            sb.Append(" WHERE CIT.INV_AGENT_PK = CITT.INV_AGENT_FK");
            sb.Append("   AND TP.TRANSPORT_INST_SEA_PK = CIT.JOB_CARD_FK");
            sb.Append("   AND CIT.INV_AGENT_PK = " + invPk + "");
            sb.Append("  UNION");
            sb.Append("  SELECT DISTINCT DP.CARGO_TYPE");
            sb.Append("  FROM INV_AGENT_TBL CIT, INV_AGENT_TRN_TBL CITT, DEM_CALC_HDR DP");
            sb.Append(" WHERE CIT.INV_AGENT_PK = CITT.INV_AGENT_FK");
            sb.Append("   AND DP.DEM_CALC_HDR_PK = CIT.JOB_CARD_FK");
            sb.Append("   AND CIT.INV_AGENT_PK = " + invPk + "");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion
        #region "FetchCurContSummaryDRAFT"
        public DataSet FetchCurContSummaryDRAFT(string invPk, string FrtElePKs)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("  SELECT JT.JOBCARD_REF_NO,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("      SUM(JF.FREIGHT_AMT) AS INVOICE_AMT");
            sb.Append("  FROM CURRENCY_TYPE_MST_TBL  CTMT,");
            sb.Append("       JOB_CARD_TRN           JT,");
            sb.Append("        JOB_TRN_FD             JF");
            sb.Append(" WHERE JT.JOB_CARD_TRN_PK IN  (" + invPk + ")");
            sb.Append("  AND CTMT.CURRENCY_MST_PK = JF.CURRENCY_MST_FK");
            sb.Append("  AND JT.JOB_CARD_TRN_PK=JF.JOB_CARD_TRN_FK");
            sb.Append(" AND JF.FREIGHT_ELEMENT_MST_FK IN (" + FrtElePKs + ")");
            sb.Append("  GROUP BY CTMT.CURRENCY_ID, JT.JOBCARD_REF_NO");
            sb.Append("   UNION");
            sb.Append("   SELECT CB.CBJC_NO, CTMT.CURRENCY_ID, SUM(CF.FREIGHT_AMT) AS INVOICE_AMT");
            sb.Append("  FROM CURRENCY_TYPE_MST_TBL  CTMT,");
            sb.Append("       CBJC_TBL               CB,");
            sb.Append("       CBJC_TRN_FD            CF");
            sb.Append("   WHERE CB.CBJC_PK IN  (" + invPk + ")");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = CF.CURRENCY_MST_FK");
            sb.Append("   AND CF.FREIGHT_ELEMENT_MST_FK IN (" + FrtElePKs + ")");
            sb.Append("    AND CB.CBJC_PK=CF.CBJC_FK");
            sb.Append("   GROUP BY CTMT.CURRENCY_ID, CB.CBJC_NO");
            sb.Append("  UNION");
            sb.Append("   SELECT TIST.TRANS_INST_REF_NO,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       SUM(TF.FREIGHT_AMT) AS INVOICE_AMT");
            sb.Append("  FROM CURRENCY_TYPE_MST_TBL  CTMT,");
            sb.Append("       TRANSPORT_INST_SEA_TBL TIST,");
            sb.Append("       TRANSPORT_TRN_FD   TF");
            sb.Append("  WHERE TIST.TRANSPORT_INST_SEA_PK IN  (" + invPk + ")");
            sb.Append("   AND TIST.TRANSPORT_INST_SEA_PK = TF.TRANSPORT_INST_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = TF.CURRENCY_MST_FK");
            sb.Append("   AND TF.FREIGHT_ELEMENT_MST_FK IN (" + FrtElePKs + ")");
            sb.Append("   GROUP BY CTMT.CURRENCY_ID, TIST.TRANS_INST_REF_NO");
            sb.Append("   UNION");
            sb.Append("   SELECT DCH.DEM_CALC_ID,");
            sb.Append("                  CTMT.CURRENCY_ID,");
            sb.Append("                  SUM(DCD.DET_AMOUNT) AS INVOICE_AMT");
            sb.Append("             FROM CURRENCY_TYPE_MST_TBL  CTMT,");
            sb.Append("                  DEM_CALC_HDR           DCH,");
            sb.Append("                  DEM_CALC_DTL           DCD");
            sb.Append("            WHERE DCH.DEM_CALC_HDR_PK IN  (" + invPk + ")");
            sb.Append("              AND CTMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK");
            sb.Append("            GROUP BY CTMT.CURRENCY_ID, DCH.DEM_CALC_ID");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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

        public DataSet FetchContCountDRAFT(string invPk)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("  SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' ||");
            sb.Append("       CY.CONTAINER_TYPE_MST_ID AS PACK,");
            sb.Append("       JCSE.JOBCARD_REF_NO");
            sb.Append("  FROM JOB_TRN_CONT            JC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       JOB_CARD_TRN            JCSE,");
            sb.Append("       JOB_TRN_FD              JCFD");
            sb.Append("  WHERE CY.CONTAINER_TYPE_MST_PK = JC.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND JCSE.JOB_CARD_TRN_PK = JC.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND JCFD.JOB_TRN_CONT_FK = JC.JOB_TRN_CONT_PK");
            sb.Append("   AND JCSE.JOB_CARD_TRN_PK IN  (" + invPk + ")");
            sb.Append("  GROUP BY JC.CONTAINER_TYPE_MST_FK,");
            sb.Append("          CY.CONTAINER_TYPE_MST_ID,");
            sb.Append("          JCSE.JOBCARD_REF_NO,");
            sb.Append("          JCSE.JOB_CARD_TRN_PK");
            sb.Append("  UNION");
            sb.Append("  SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' || CY.CONTAINER_TYPE_MST_ID AS PACK, CT.CBJC_NO AS JOBCARD_REF_NO");
            sb.Append("              FROM CBJC_TRN_CONT            CBJC,");
            sb.Append("                   CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("                   FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("                   CBJC_TBL            CT");
            sb.Append("              WHERE CY.CONTAINER_TYPE_MST_PK = CBJC.CONTAINER_TYPE_MST_FK");
            sb.Append("               AND CT.CBJC_PK = CBJC.CBJC_FK(+)");
            sb.Append("              AND CT.CBJC_PK IN  (" + invPk + ")");
            sb.Append("              GROUP BY CT.CBJC_PK,");
            sb.Append("                      CBJC.CONTAINER_TYPE_MST_FK,");
            sb.Append("                      CY.CONTAINER_TYPE_MST_ID,");
            sb.Append("                      CT.CBJC_NO     ");
            sb.Append("  UNION");
            sb.Append("  SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' || CY.CONTAINER_TYPE_MST_ID AS PACK,TPN.TRANS_INST_REF_NO AS JOBCARD_REF_NO");
            sb.Append("              FROM TRANSPORT_TRN_CONT            TTC,");
            sb.Append("                   CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("                   FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("                   TRANSPORT_INST_SEA_TBL  TPN");
            sb.Append("             WHERE CY.CONTAINER_TYPE_MST_PK = TTC.CONTAINER_TYPE_MST_FK");
            sb.Append("               AND TPN.TRANSPORT_INST_SEA_PK = TTC.TRANSPORT_INST_FK(+)");
            sb.Append("               AND TPN.TRANSPORT_INST_SEA_PK IN  (" + invPk + ")");
            sb.Append("               GROUP BY TTC.CONTAINER_TYPE_MST_FK,");
            sb.Append("                      CY.CONTAINER_TYPE_MST_ID,");
            sb.Append("                      TPN.TRANS_INST_REF_NO");
            sb.Append("  UNION");
            sb.Append("  SELECT COUNT(DISTINCT CY.CONTAINER_TYPE_MST_ID) || ' X ' || CY.CONTAINER_TYPE_MST_ID AS PACK,DCH.DEM_CALC_ID AS JOBCARD_REF_NO");
            sb.Append("                          FROM DEM_CALC_HDR            DCH,");
            sb.Append("                               TRANSPORT_INST_SEA_TBL TCT1,");
            sb.Append("                               CONTAINER_TYPE_MST_TBL  CY,");
            sb.Append("                               TRANSPORT_TRN_CONT     TTC,");
            sb.Append("                               TRANSPORT_TRN_FD        TTF");
            sb.Append("                         WHERE  TCT1.TRANSPORT_INST_SEA_PK=TTC.TRANSPORT_INST_FK");
            sb.Append("                              AND CY.CONTAINER_TYPE_MST_PK = TTC.CONTAINER_TYPE_MST_FK");
            sb.Append("                               AND TTF.TRANSPORT_INST_FK = TCT1.TRANSPORT_INST_SEA_PK");
            sb.Append("                             AND DCH.DEM_CALC_HDR_PK IN  (" + invPk + ")");
            sb.Append("                              GROUP BY TTC.CONTAINER_TYPE_MST_FK,");
            sb.Append("                                  CY.CONTAINER_TYPE_MST_ID,DCH.DEM_CALC_ID");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "Fetch Invoice Date"
        public DataSet FetchInvoiceDate(System.DateTime InvoiceDt)
        {
            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            try
            {
                ObjWk.OpenConnection();
                ObjWk.MyCommand.Connection = ObjWk.MyConnection;
                var _with44 = ObjWk.MyCommand;
                _with44.CommandType = CommandType.StoredProcedure;
                _with44.CommandText = ObjWk.MyUserName + ".CONSOL_INV_PKG.FETCH_INVOICE_DATE";
                ObjWk.MyCommand.Parameters.Clear();
                var _with45 = ObjWk.MyCommand.Parameters;
                _with45.Add("LOCATION_PK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                _with45.Add("INVOICE_DT_IN", InvoiceDt).Direction = ParameterDirection.Input;
                _with45.Add("INV_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ObjWk.MyDataAdapter.SelectCommand = ObjWk.MyCommand;
                ObjWk.MyDataAdapter.Fill(dsData);
                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Fetch Accounting Period"
        public DataSet FetchAccPeriod(int LocationMstPK, Int32 No_Months)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with46 = objWF.MyDataAdapter;
                _with46.SelectCommand = new OracleCommand();
                _with46.SelectCommand.Connection = objWF.MyConnection;
                _with46.SelectCommand.CommandText = objWF.MyUserName + ".MONTH_END_CLOSING_TRN_PKG.FETCH_MONTH_END_LISTING";
                _with46.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with46.SelectCommand.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32).Value = LocationMstPK;
                _with46.SelectCommand.Parameters.Add("NO_OF_MONTHS_IN", OracleDbType.Int32).Value = (No_Months < 6 ? 6 : No_Months);
                _with46.SelectCommand.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with46.Fill(DS);
                return DS;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "Update Req/App for Accounting Period"
        public ArrayList UpdateAccPeriod(DataSet M_DataSet, int LocationPK, bool isApprover = false)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand updCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            int RowCnt = 0;
            int RecAfct = 0;
            int intZero = 0;
            int intOne = 1;
            try
            {
                objWF.OpenConnection();
                TRAN = objWF.MyConnection.BeginTransaction();
                if ((M_DataSet != null))
                {
                    if (M_DataSet.Tables[0].Rows.Count > 0)
                    {
                        for (RowCnt = 0; RowCnt <= M_DataSet.Tables[0].Rows.Count - 1; RowCnt++)
                        {
                            var _with47 = updCommand;
                            _with47.Connection = objWF.MyConnection;
                            _with47.CommandType = CommandType.StoredProcedure;
                            _with47.CommandText = objWF.MyUserName + ".MONTH_END_CLOSING_TRN_PKG.UPDATE_MONTH_END_REQUEST";
                            var _with48 = _with47.Parameters;
                            _with48.Clear();
                            _with48.Add("MONTH_END_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["MONTH_END_PK"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[RowCnt]["MONTH_END_PK"])).Direction = ParameterDirection.Input;
                            _with48.Add("LOCATION_MST_FK_IN", LocationPK).Direction = ParameterDirection.Input;
                            _with48.Add("MONTH_YEAR_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["MONTH_YEAR"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[RowCnt]["MONTH_YEAR"])).Direction = ParameterDirection.Input;
                            _with48.Add("VERSION_NO_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["VERSION_NO"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[RowCnt]["VERSION_NO"])).Direction = ParameterDirection.Input;
                            _with48.Add("STATUS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["STATUS_PK"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[RowCnt]["STATUS_PK"])).Direction = ParameterDirection.Input;
                            if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["STATUS_PK"]) == 2)
                            {
                                _with48.Add("MANUAL_OPEN_IN", intOne).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with48.Add("MANUAL_OPEN_IN", intZero).Direction = ParameterDirection.Input;
                            }
                            if (isApprover == false)
                            {
                                if (M_DataSet.Tables[0].Rows[RowCnt]["SEL"] == "0")
                                {
                                    _with48.Add("REQ_TO_OPEN_IN", intZero).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with48.Add("REQ_TO_OPEN_IN", intOne).Direction = ParameterDirection.Input;
                                }
                            }
                            else
                            {
                                _with48.Add("REQ_TO_OPEN_IN", intZero).Direction = ParameterDirection.Input;
                            }
                            _with48.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                            _with48.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with48.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            

                            var _with49 = objWF.MyDataAdapter;
                            _with49.UpdateCommand = updCommand;
                            _with49.UpdateCommand.Transaction = TRAN;
                            RecAfct = RecAfct + _with49.UpdateCommand.ExecuteNonQuery();
                        }
                    }
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully.Mail has been Send");
                    return arrMessage;
                }
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion
        #region "Fetch Document PK  For Sending Mail"
        public DataTable FetchDocument(string documentId)
        {
            StringBuilder strbldrSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strbldrSQL.Append(" Select DMT.DOCUMENT_MST_PK ");
            strbldrSQL.Append(" FROM DOCUMENT_MST_TBL DMT ");
            strbldrSQL.Append(" WHERE");
            strbldrSQL.Append(" DMT.DOCUMENT_ID='" + documentId + "'");
            try
            {
                return objWF.GetDataTable(strbldrSQL.ToString());
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion
        #region "Get Acc Period Approver"
        public DataSet GetAccPeriodApprover(long Docpk, long Lockpk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with50 = objWF.MyDataAdapter;
                _with50.SelectCommand = new OracleCommand();
                _with50.SelectCommand.Connection = objWF.MyConnection;
                _with50.SelectCommand.CommandText = objWF.MyUserName + ".MONTH_END_CLOSING_TRN_PKG.FETCH_ACCPERIOD_APPROVER";
                _with50.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with50.SelectCommand.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32).Value = Lockpk;
                _with50.SelectCommand.Parameters.Add("DOCUMENT_MST_PK_IN", OracleDbType.Int32).Value = Docpk;
                _with50.SelectCommand.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with50.Fill(DS);
                return DS;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion

        public DataSet GetMessageInformation(Int64 DOCPk, Int64 LOCPk)
        {
            //goutam
            string strSql = null;
            DataSet MainDS = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();

            strSql = string.Empty ;
            strSql += " SELECT ROWNUM SR_NO,  " ;
            strSql += " 0 User_Message_Pk, " ;
            strSql += " UsrMsgTrn.Sender_Fk, " ;
            strSql += " UsrMsgTrn.Receiver_Fk, " ;
            strSql += " UsrMsgTrn.Msg_Read, " ;
            strSql += " UsrMsgTrn.Followup_Flag, " ;
            strSql += " UsrMsgTrn.Have_Attachment, " ;
            strSql += " doc.document_subject  Msg_Subject, " ;
            strSql += " doc.document_header h1, doc.document_body h2,  doc.document_footer h3, Msg_Body, DOC.DOCUMENT_HEADER Msg_header, DOC.DOCUMENT_FOOTER msg_footer," ;
            strSql += " UsrMsgTrn.Read_Receipt, " ;
            strSql += " UsrMsgTrn.Document_Mst_Fk, " ;
            strSql += " doc.message_folder_mst_fk User_Message_Folders_Fk, " ;
            strSql += " /*UsrMsgTrn.*/ sysdate Msg_Received_Dt, " ;
            strSql += " UsrMsgTrn.Version_No ,'' EMAILID " ;
            strSql += " FROM USER_MESSAGE_TRN UsrMsgTrn, " ;
            strSql += " document_mst_tbl doc " ;
            strSql += " WHERE usrmsgtrn.document_mst_fk(+) = doc.document_mst_pk " ;
            strSql += " AND UsrMsgTrn.DELETE_FLAG is null  AND doc.document_mst_pk =  " + DOCPk ;
            strSql += " AND User_Message_Pk(+) =  -1 " ;
            WorkFlow objWF = new WorkFlow();

            try
            {
                DA = objWF.GetDataAdapter(strSql.Trim());
                DA.Fill(MainDS, "MsgTrn");
                strSql = string.Empty ;
                strSql += " SELECT ROWNUM SR_NO, " ;
                strSql += " 0 User_Message_Det_Pk, " ;
                strSql += " 0 User_Message_Fk, " ;
                strSql += " '' Attachment_Caption, " ;
                strSql += " '' Attachment_Data, " ;
                strSql += " doc.attachment_url Url_Page, " ;
                strSql += " 0 Version_No " ;
                strSql += " FROM document_mst_tbl doc " ;
                strSql += " WHERE doc.Active=1 And doc.document_mst_pk = " + DOCPk ;

                DA = objWF.GetDataAdapter(strSql);
                DA.Fill(MainDS, "MsgDet");
                DataRelation DSWFMSg = new DataRelation("WFMsg", new DataColumn[] { MainDS.Tables["MsgTrn"].Columns["User_Message_Pk"] }, new DataColumn[] { MainDS.Tables["MsgDet"].Columns["User_Message_Fk"] });
                MainDS.Relations.Add(DSWFMSg);
                return MainDS;

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

        #region "Fetch UserPK"
        public DataTable GetUserInfo(long DocumentId_PK)
        {
            StringBuilder strbldrSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strbldrSQL.Append("SELECT DISTINCT ");
            strbldrSQL.Append(" WF.USER_MST_FK ");
            strbldrSQL.Append(" FROM WORKFLOW_RULES_TRN WF, DOCUMENT_MST_TBL DOC");
            strbldrSQL.Append(" WHERE  ");
            strbldrSQL.Append(" WF.DOCUMENT_MST_FK = DOC.DOCUMENT_MST_PK ");
            strbldrSQL.Append(" AND WF.DOCUMENT_MST_FK = " + DocumentId_PK);
            try
            {
                return objWF.GetDataTable(strbldrSQL.ToString());
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion
        #region "Fetch Empty Invoice Hdr"
        public DataSet FetchIT(int Inv_Pk = 0, string Flag = "")
        {
            WorkFlow objWF = new WorkFlow();
            string strSql = null;

            strSql = string.Empty ;
            if (Flag == "CON_INVOICE")
            {
                if (Inv_Pk > 0)
                {
                    strSql = "  SELECT * FROM CONSOL_INVOICE_TBL IT WHERE IT.CONSOL_INVOICE_PK = " + Inv_Pk;
                }
                else
                {
                    strSql = "  SELECT * FROM CONSOL_INVOICE_TBL IT WHERE IT.CONSOL_INVOICE_PK = 0 ";
                }

            }
            else
            {

            }

            try
            {
                return objWF.GetDataSet(strSql);
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

        public DataSet FetchData(long Docpk, string Flag = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with51 = objWF.MyDataAdapter;
                _with51.SelectCommand = new OracleCommand();
                _with51.SelectCommand.Connection = objWF.MyConnection;
                _with51.SelectCommand.CommandText = objWF.MyUserName + ".MONTH_END_CLOSING_TRN_PKG.FETCH_DATA";
                _with51.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with51.SelectCommand.Parameters.Add("FLAG_IN", OracleDbType.Varchar2).Value = Flag;
                _with51.SelectCommand.Parameters.Add("PK_IN", OracleDbType.Int32).Value = Docpk;
                _with51.SelectCommand.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with51.Fill(DS);
                return DS;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion
        #region "Fetch Dem Details for Acc Period Popup"
        public DataSet FetchDemHeader(int DemInvPK)
        {
            string strSql = null;
            Int16 RecCount = default(Int16);
            WorkFlow objWF = null;
            DataSet ds = null;
            strSql = "SELECT DCH.DEM_CALC_HDR_PK, DCH.PROCESS,DIH.DEM_INV_NR AS INVOICE_NO,DIH.DEM_INV_DATE AS INVOICE_DT FROM DEM_INVOICE_HDR DIH, DEM_CALC_HDR DCH";
            strSql = strSql + " WHERE DIH.DEM_CALC_HDR_FK = DCH.DEM_CALC_HDR_PK AND DIH.DEM_INV_HDR_PK =" + DemInvPK;
            try
            {
                ds = objWF.GetDataSet(strSql);
                return ds;
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
        #endregion

        #region "Populate Status"
        public int CheckRequestFlag(long LocPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            int val = 0;
            sb.Append(" SELECT COUNT(T.WORKFLOW_RULES_FK)");
            sb.Append("  FROM WORKFLOW_LOC_TRN T ");
            sb.Append("  WHERE T.FROM_LOC_MST_FK = " + LocPK + " ");
            val = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            if (val > 0)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }
        #endregion

        #region "Fetch Dem Details for Acc Period Popup"
        public string FetchDepositAmount(string DemPk = "0")
        {
            StringBuilder strSql = new StringBuilder(5000);
            Int16 RecCount = default(Int16);
            WorkFlow objWF = new WorkFlow();
            string Amount = null;
            strSql.Append(" SELECT SUM(Q.DEPOSIT_AMT) FROM ( ");
            strSql.Append(" SELECT  Nvl(Sum(JTC.DEPOSIT_AMT),0) DEPOSIT_AMT");
            strSql.Append(" FROM JOB_TRN_CONT JTC WHERE  JTC.JOB_CARD_TRN_FK IN  ");
            strSql.Append(" (SELECT C.JC_FK FROM CBJC_TBL C WHERE C.CBJC_PK IN (SELECT D.DOC_REF_FK FROM ");
            strSql.Append(" DEM_CALC_HDR D WHERE D.DEM_CALC_HDR_PK = " + DemPk + " AND D.DOC_TYPE=1 ))");
            strSql.Append(" UNION ");
            strSql.Append(" SELECT  Nvl(Sum(JTC.DEPOSIT_AMT),0) DEPOSIT_AMT");
            strSql.Append(" FROM JOB_TRN_CONT JTC WHERE  JTC.JOB_CARD_TRN_FK IN ( ");
            strSql.Append(" SELECT T.JOB_CARD_FK FROM TRANSPORT_INST_SEA_TBL T WHERE T.TRANSPORT_INST_SEA_PK ");
            strSql.Append(" IN (SELECT D.DOC_REF_FK FROM  DEM_CALC_HDR D WHERE D.DEM_CALC_HDR_PK = " + DemPk + " AND D.DOC_TYPE=0 ))");
            strSql.Append(" UNION ");
            strSql.Append(" SELECT  Nvl(Sum(JTC.DEPOSIT_AMT),0) DEPOSIT_AMT");
            strSql.Append(" FROM JOB_TRN_CONT JTC WHERE  JTC.JOB_CARD_TRN_FK IN ( ");
            strSql.Append(" SELECT C.JC_FK FROM CBJC_TBL C WHERE C.CBJC_PK IN ( ");
            strSql.Append(" SELECT T.JOB_CARD_FK FROM TRANSPORT_INST_SEA_TBL T WHERE T.TRANSPORT_INST_SEA_PK IN (");
            strSql.Append(" SELECT D.DOC_REF_FK FROM DEM_CALC_HDR D WHERE D.DEM_CALC_HDR_PK = " + DemPk + ")))");
            strSql.Append(" )Q ");
            try
            {
                Amount = objWF.ExecuteScaler(strSql.ToString());
                return Amount;
            }
            catch (OracleException sqlExp)
            {
                return "0";
            }
            catch (Exception exp)
            {
                return "0";
            }
        }
        #endregion

        #region "AutoInvoiceSave"
        public ArrayList AutoInvoiceSave(int JobPk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            int RecAfct = 0;
            int InvoicePk = 0;
            arrMessage.Clear();
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                var _with52 = insCommand;
                _with52.Connection = objWK.MyConnection;
                _with52.CommandType = CommandType.StoredProcedure;
                _with52.CommandText = objWK.MyUserName + ".CONSOL_AUTO_INVOICE_PKG.AUTO_CREATE_INVOICE";
                _with52.Parameters.Clear();
                var _with53 = _with52.Parameters;
                _with53.Add("JOB_TRN_PK_IN", JobPk).Direction = ParameterDirection.Input;
                _with53.Add("EMPLOYEE_MST_FK_IN", Convert.ToInt64(HttpContext.Current.Session["EMP_PK"])).Direction = ParameterDirection.Input;
                _with53.Add("LOCATION_FK_IN", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                _with53.Add("CURRENCY_MST_FK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with53.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with53.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with54 = objWK.MyDataAdapter;
                _with54.InsertCommand = insCommand;
                _with54.InsertCommand.Transaction = TRAN;
                RecAfct = _with54.InsertCommand.ExecuteNonQuery();
                InvoicePk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                if (RecAfct > 0)
                {
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }
        #endregion

    }
}