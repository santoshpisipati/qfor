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
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class cls_Collectionlist : CommonFeatures
    {
        //Parent Table
        #region "Fetch Function"
        public object Fetchlist(long Biztype, long Process, string fromDate = "", string ToDate = "", string Collectionrefno = "", string Invrefno = "", string Custpk = "", string Polfk = "", string Podfk = "", string VslVoyPK = "",
        string FlightNo = "", string DocPK = "0", string DocType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long usrLocFK = 0, string SortColumn = "", string SortType = "DESC", Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();


            strSQLBuilder.Append(" SELECT COL.COLLECTIONS_TBL_PK PK, ");
            strSQLBuilder.Append(" CMT.CUSTOMER_ID PARTY, ");
            strSQLBuilder.Append(" COL.COLLECTIONS_REF_NO RECDREFNR, ");
            strSQLBuilder.Append(" COL.COLLECTIONS_DATE COLLECTIONDATE, ");
            strSQLBuilder.Append("  CUR.CURRENCY_ID\t CURRENCY,  ");
            //strSQLBuilder.Append("  '" & Session("CURRENCY_ID") & "' CURRENCY,  ")
            strSQLBuilder.Append(" SUM(CTRN.RECD_AMOUNT_HDR_CURR) RECDAMOUNT ");
            strSQLBuilder.Append(" FROM COLLECTIONS_TBL       COL, ");
            strSQLBuilder.Append(" COLLECTIONS_TRN_TBL   CTRN,USER_MST_TBL UMT, ");
            strSQLBuilder.Append(" CUSTOMER_MST_TBL      CMT, ");
            strSQLBuilder.Append(" CURRENCY_TYPE_MST_TBL CUR ");
            if ((Process == 1) & (!string.IsNullOrEmpty(Polfk) | !string.IsNullOrEmpty(Podfk) | !string.IsNullOrEmpty(VslVoyPK) | DocPK != "0" | !string.IsNullOrEmpty(FlightNo)))
            {
                strSQLBuilder.Append(" ,CONSOL_INVOICE_TBL INV,");
                strSQLBuilder.Append("  CONSOL_INVOICE_TRN_TBL INVTRN,");
                strSQLBuilder.Append("  JOB_CARD_TRN JOB,");
                strSQLBuilder.Append("   BOOKING_MST_TBL BKG");
            }
            else if ((Process == 2) & (!string.IsNullOrEmpty(Polfk) | !string.IsNullOrEmpty(Podfk) | !string.IsNullOrEmpty(VslVoyPK) | DocPK != "0" | !string.IsNullOrEmpty(FlightNo)))
            {
                strSQLBuilder.Append(" ,CONSOL_INVOICE_TBL INV,");
                strSQLBuilder.Append("  CONSOL_INVOICE_TRN_TBL INVTRN,");
                strSQLBuilder.Append("  JOB_CARD_TRN JOB");
            }
            strSQLBuilder.Append(" WHERE COL.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK ");
            strSQLBuilder.Append(" AND COL.BUSINESS_TYPE = '" + Biztype + "'");
            strSQLBuilder.Append(" AND COL.PROCESS_TYPE = '" + Process + "'");
            strSQLBuilder.Append(" AND CMT.CUSTOMER_MST_PK = COL.CUSTOMER_MST_FK ");
            strSQLBuilder.Append(" AND CUR.CURRENCY_MST_PK = COL.CURRENCY_MST_FK ");
            strSQLBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            strSQLBuilder.Append(" AND COL.CREATED_BY_FK = UMT.USER_MST_PK ");
            if ((Process == 1) & (!string.IsNullOrEmpty(Polfk) | !string.IsNullOrEmpty(Podfk) | !string.IsNullOrEmpty(VslVoyPK) | DocPK != "0" | !string.IsNullOrEmpty(FlightNo)))
            {
                strSQLBuilder.Append("  AND INV.CONSOL_INVOICE_PK=INVTRN.CONSOL_INVOICE_FK");
                strSQLBuilder.Append("  AND CTRN.INVOICE_REF_NR=INV.INVOICE_REF_NO");
                strSQLBuilder.Append("  AND JOB.JOB_CARD_TRN_PK(+)=INVTRN.JOB_CARD_FK");
                strSQLBuilder.Append("  AND JOB.BOOKING_MST_FK=BKG.BOOKING_MST_PK");
                if (!string.IsNullOrEmpty(Polfk))
                {
                    strSQLBuilder.Append(" AND BKG.PORT_MST_POL_FK=" + Polfk + " ");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    strSQLBuilder.Append(" AND BKG.PORT_MST_POD_FK=" + Podfk + " ");
                }
                if (!string.IsNullOrEmpty(VslVoyPK))
                {
                    if (Biztype == 2)
                    {
                        strSQLBuilder.Append(" AND BKG.VESSEL_VOYAGE_FK=" + VslVoyPK + "");
                    }
                    else
                    {
                        strSQLBuilder.Append(" AND JOB.VOYAGE_flight_no  LIKE '%" + FlightNo.Trim() + "%' ");
                    }

                }
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Booking
                    if (Convert.ToInt32(DocType) == 2)
                    {
                        strSQLBuilder.Append(" AND  BKG.BOOKING_MST_PK =" + DocPK + "");
                        //'JobCard
                    }
                    else if (Convert.ToInt32(DocType) == 3)
                    {
                        strSQLBuilder.Append(" AND JOB.JOB_CARD_TRN_PK =" + DocPK + "");
                        //'Hbl
                    }
                    else if (Convert.ToInt32(DocType) == 4)
                    {
                        strSQLBuilder.Append(" AND JOB.HBL_HAWB_FK =" + DocPK + "");
                    }
                    else
                    {
                        strSQLBuilder.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }
                }
            }
            else if ((Process == 2) & (!string.IsNullOrEmpty(Polfk) | !string.IsNullOrEmpty(Podfk) | !string.IsNullOrEmpty(VslVoyPK) | DocPK != "0" | !string.IsNullOrEmpty(FlightNo)))
            {
                strSQLBuilder.Append("  AND INV.CONSOL_INVOICE_PK=INVTRN.CONSOL_INVOICE_FK");
                strSQLBuilder.Append("  AND CTRN.INVOICE_REF_NR=INV.INVOICE_REF_NO");
                strSQLBuilder.Append("  AND JOB.JOB_CARD_TRN_PK(+)=INVTRN.JOB_CARD_FK");
                if (!string.IsNullOrEmpty(Polfk))
                {
                    strSQLBuilder.Append(" AND JOB.PORT_MST_POL_FK=" + Polfk + " ");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    strSQLBuilder.Append(" AND JOB.PORT_MST_POD_FK=" + Podfk + " ");
                }
                if (!string.IsNullOrEmpty(VslVoyPK))
                {
                    if (Biztype == 2)
                    {
                        strSQLBuilder.Append(" AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
                    }
                    else
                    {
                        strSQLBuilder.Append(" AND JOB.VOYAGE_flight_no  LIKE '%" + FlightNo.Trim() + "%' ");
                    }
                }
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Jobcard
                    if (Convert.ToInt32(DocType) == 3)
                    {
                        strSQLBuilder.Append("  AND JOB.JOB_CARD_TRN_PK=" + DocPK + "");
                    }
                    else
                    {
                        strSQLBuilder.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }

                }
            }
            //'End
            if (flag == 0)
            {
                strSQLBuilder.Append(" AND 1=2 ");
            }
            if (!string.IsNullOrEmpty(Collectionrefno))
            {
                strSQLBuilder.Append(" AND UPPER(COL.COLLECTIONS_REF_NO) LIKE '%" + Collectionrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }
            if (!string.IsNullOrEmpty(Custpk))
            {
                strSQLBuilder.Append(" AND COL.CUSTOMER_MST_FK = '" + Custpk + "' ");
            }
            if (!string.IsNullOrEmpty(Invrefno))
            {
                strSQLBuilder.Append(" AND UPPER(CTRN.INVOICE_REF_NR) LIKE '%" + Invrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                strSQLBuilder.Append(" AND TO_DATE(COL.COLLECTIONS_DATE, '" + dateFormat + "') >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                strSQLBuilder.Append(" AND TO_DATE(COL.COLLECTIONS_DATE, '" + dateFormat + "') <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            strSQLBuilder.Append(" GROUP BY COL.COLLECTIONS_TBL_PK, ");
            strSQLBuilder.Append(" CMT.CUSTOMER_ID, ");
            strSQLBuilder.Append(" COL.COLLECTIONS_REF_NO, ");
            strSQLBuilder.Append(" COL.COLLECTIONS_DATE, ");
            strSQLBuilder.Append(" CUR.CURRENCY_ID   ORDER BY " + SortColumn + "  " + SortType + "  ");
            //If Biztype = 1 Then 'Air
            //    strSQLBuilder.Replace("SEA", "AIR")
            //    strSQLBuilder.Replace("HBL", "HAWB")
            //End If

            strCount.Append(" SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + strSQLBuilder.ToString() + ")");

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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

            strSQL.Append("SELECT QRY.* FROM ");
            strSQL.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            strSQL.Append("  (" + strSQLBuilder.ToString() + " ");
            strSQL.Append("  ) T ORDER BY PK DESC) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            string sql = null;
            sql = strSQL.ToString();

            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;
                DS.Tables.Add(Fetchchildlist(AllMasterPKs(DS), Biztype, Process, fromDate, ToDate, Collectionrefno, Invrefno, Custpk, usrLocFK, SortColumn,
                SortType));
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
        //Added By Koteshwari
        #region "INVOICE DETAILS"
        public object FetchlistData(long Biztype, long Process, string fromDate = "", string ToDate = "", string Invrefno = "", string Custpk = "", string Polfk = "", string Podfk = "", string VslVoy = "", string DocPK = "",
        string DocType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long usrLocFK = 0, string SortColumn = "", string SortType = "DESC", Int32 flag = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            Int32 TotalRecords = default(Int32);
            if (DocPK == "0")
            {
                DocPK = "";
            }
            else
            {
                DocPK = DocPK;
            }
            strCondition.Append(" SELECT  INV.CONSOL_INVOICE_PK PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" INV.INVOICE_DATE,");
            strCondition.Append(" CMT.CUSTOMER_NAME, ");
            strCondition.Append(" CUMT.CURRENCY_ID,");
            strCondition.Append(" INV.NET_RECEIVABLE,");
            strCondition.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
            strCondition.Append("          where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0) Recieved,");
            strCondition.Append(" NVL((INV.NET_RECEIVABLE - NVL((select sum(WMT.WRITEOFF_AMOUNT) from Writeoff_Manual_Tbl WMT");
            strCondition.Append("  where WMT.CONSOL_INVOICE_FK=INV.CONSOL_INVOICE_PK),0.00)-NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
            strCondition.Append("  where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0.00) - NVL((SELECT SUM(CTRN.TDS_AMOUNT) FROM COLLECTIONS_TRN_TBL CTRN WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),  0.00)),0) Balance,");
            strCondition.Append(" 'false' SEL,CMT.CUSTOMER_MST_PK FROM");
            strCondition.Append(" CONSOL_INVOICE_TBL INV, ");
            strCondition.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
            //Sea Export
            if (Biztype == 2 & Process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,BOOKING_MST_TBL BKS,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND JOB.BOOKING_MST_FK=BKS.BOOKING_MST_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Booking
                    if (Convert.ToInt32(DocType) == 2)
                    {
                        strCondition.Append(" AND BKS.BOOKING_MST_PK=" + DocPK + "");
                        //'Jobcard
                    }
                    else if (Convert.ToInt32(DocType) == 3)
                    {
                        strCondition.Append("  AND JOB.JOB_CARD_TRN_PK=" + DocPK + "");
                        //'HBL
                    }
                    else if (Convert.ToInt32(DocType) == 4)
                    {
                        strCondition.Append(" AND HBL.HBL_EXP_TBL_PK=" + DocPK + "");
                    }
                    else
                    {
                        strCondition.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }
                }
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    strCondition.Append(" AND  VTRN.VOYAGE LIKE '%" + VslVoy.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(Polfk))
                {
                    strCondition.Append(" AND BKS.PORT_MST_POL_FK=" + Polfk + "");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    strCondition.Append(" AND BKS.PORT_MST_POD_FK=" + Podfk + "");
                }
            }
            //Air Export
            if (Biztype == 1 & Process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,BOOKING_MST_TBL BKA,");
                strCondition.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND BKA.BOOKING_MST_PK(+) = JOB.BOOKING_MST_FK");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Booking
                    if (Convert.ToInt32(DocType) == 2)
                    {
                        strCondition.Append(" AND BKA.BOOKING_MST_PK=" + DocPK + "");
                        //'Jobcard
                    }
                    else if (Convert.ToInt32(DocType) == 3)
                    {
                        strCondition.Append("  AND JOB.JOB_CARD_TRN_PK=" + DocPK + "");
                        //'HBL
                    }
                    else if (Convert.ToInt32(DocType) == 4)
                    {
                        strCondition.Append(" AND HAWB.HAWB_EXP_TBL_PK=" + DocPK + "");
                    }
                    else
                    {
                        strCondition.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }
                }
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    strCondition.Append(" AND JOB.VOYAGE_flight_no  LIKE '%" + VslVoy.Trim() + "%' ");
                }
                if (!string.IsNullOrEmpty(Polfk))
                {
                    strCondition.Append(" AND BKA.PORT_MST_POL_FK=" + Polfk + "");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    strCondition.Append(" AND BKA.PORT_MST_POD_FK=" + Podfk + "");
                }
            }

            //Sea Import
            if (Biztype == 2 & Process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Jobcard
                    if (Convert.ToInt32(DocType) == 3)
                    {
                        strCondition.Append("  AND JOB.JOB_CARD_TRN_PK=" + DocPK + "");
                    }
                    else
                    {
                        strCondition.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }
                }
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    strCondition.Append(" AND VTRN.VOYAGE LIKE '%" + VslVoy.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(Polfk))
                {
                    strCondition.Append(" AND JOB.PORT_MST_POL_FK=" + Polfk + "");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    strCondition.Append(" AND JOB.PORT_MST_POD_FK=" + Podfk + "");
                }
            }
            //Air Import
            if (Biztype == 1 & Process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Jobcard
                    if (Convert.ToInt32(DocType) == 3)
                    {
                        strCondition.Append("  AND JOB.JOB_CARD_TRN_PK=" + DocPK + "");
                    }
                    else
                    {
                        strCondition.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }
                }
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    strCondition.Append(" AND JOB.VOYAGE_flight_no  LIKE '%" + VslVoy.Trim() + "%' ");
                }
                if (!string.IsNullOrEmpty(Polfk))
                {
                    strCondition.Append(" AND JOB.PORT_MST_POL_FK=" + Polfk + "");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    strCondition.Append(" AND JOB.PORT_MST_POD_FK=" + Podfk + "");
                }
            }
            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            strCondition.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            strCondition.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            strCondition.Append(" AND INV.PROCESS_TYPE ='" + Process + "' ");
            strCondition.Append(" AND INV.BUSINESS_TYPE ='" + Biztype + "' ");
            strCondition.Append(" AND INV.CHK_INVOICE=1 ");
            strCondition.Append(" AND INV.IS_FAC_INV<>1 ");
            //strCondition.Append(" AND (INV.NET_RECEIVABLE - NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn")
            //strCondition.Append("   where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0)) > 0")
            strCondition.Append("  AND  NVL((INV.NET_RECEIVABLE - NVL((select sum(WMT.WRITEOFF_AMOUNT) from Writeoff_Manual_Tbl WMT");
            strCondition.Append("  where WMT.CONSOL_INVOICE_FK=INV.CONSOL_INVOICE_PK),0.00)-NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
            strCondition.Append("  where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0.00) - NVL((SELECT SUM(CTRN.TDS_AMOUNT) FROM COLLECTIONS_TRN_TBL CTRN WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),  0.00)),0) > 0");
            if (!string.IsNullOrEmpty(Custpk))
            {
                strCondition.Append(" AND CMT.CUSTOMER_MST_PK=" + Custpk + "");
            }

            if (!string.IsNullOrEmpty(Invrefno))
            {
                strCondition.Append(" AND INV.INVOICE_REF_NO = '" + Invrefno + "' ");
            }
            //If getDefault(fromDate, "") <> "" And getDefault(ToDate, "") <> "" Then
            //    strCondition.Append(" AND INV.invoice_date  BETWEEN TO_DATE('" & fromDate & "', '" & dateFormat & "') AND TO_DATE('" & ToDate & "', '" & dateFormat & "')")
            //ElseIf getDefault(fromDate, "") <> "" Then
            //    strCondition.Append(" AND INV.invoice_date  >=TO_DATE('" & fromDate & "' ,'" & dateFormat & "')")
            //ElseIf getDefault(ToDate, "") <> "" Then
            //    strCondition.Append(" AND INV.invoice_date  <=TO_DATE('" & ToDate & "' ,'" & dateFormat & "')")
            //End If
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                strCondition.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                strCondition.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            strCondition.Append(" GROUP BY");
            strCondition.Append(" INV.CONSOL_INVOICE_PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" INV.INVOICE_DATE ,");
            strCondition.Append(" CUMT.CURRENCY_ID,CMT.CUSTOMER_NAME, CMT.CUSTOMER_MST_PK,");
            if (Biztype == 2 & Process == 1)
            {
                strCondition.Append(" INV.NET_RECEIVABLE,INV.CREATED_DT ORDER BY INV.CREATED_DT DESC");
            }
            else
            {
                strCondition.Append(" INV.NET_RECEIVABLE,INV.CREATED_DT ORDER BY INV.CREATED_DT DESC");
            }
            //strCondition.Append(" ORDER BY INV.CONSOL_INVOICE_PK DESC ")
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from ");
            strCount.Append((" (" + strCondition.ToString() + ")"));
            DataSet dsC = new DataSet();
            dsC = objWF.GetDataSet(strCount.ToString());
            TotalRecords = Convert.ToInt32(dsC.Tables[0].Rows[0][0]);

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

            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + strCondition.ToString() + " ");
            sqlstr.Append("  ) T ORDER BY PK DESC) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            string sql = null;
            sql = sqlstr.ToString();
            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;
                DS.Tables.Add(fetchchild(AllMasterPKs(DS), Invrefno, fromDate, ToDate, Custpk, Polfk, Podfk, VslVoy, DocPK, DocType,
                usrLocFK, Convert.ToInt16(Biztype), Convert.ToInt16(Process)));
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
        //'END
        #region "PK Value"
        private string AllMasterPKs(DataSet ds)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                System.Text.StringBuilder strBuild = new System.Text.StringBuilder();
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

        #region "Child Table For Invoice"
        public DataTable fetchchild(string CONTSpotPKs = "", string Invrefno = "", string fromDate = "", string ToDate = "", string Custpk = "", string Polfk = "", string Podfk = "", string VslVoy = "", string DocPK = "", string DocType = "",
        long usrLocFK = 0, short BizType = 1, short process = 1)
        {



            if (DocPK == "0")
            {
                DocPK = "";
            }
            else
            {
                DocPK = DocPK;
            }

            System.Text.StringBuilder BuildQuery = new System.Text.StringBuilder();
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
            BuildQuery.Append(" 'JobCard' JOB_TYPE,");
            BuildQuery.Append(" JOB.JOBCARD_DATE,");
            //Sea Export
            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" HBL.HBL_REF_NO REFNR,");
                //Air Export
            }
            else if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" HAWB.HAWB_REF_NO REFNR,");
                //Sea Import
            }
            else if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" JOB.Hbl_HAWB_REF_NO REFNR,");
                //Air Import
            }
            else if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" JOB.Hbl_HAWB_REF_NO REFNR,");
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
                BuildQuery.Append(" JOB.VOYAGE_FLIGHT_NO AS VESVOYAGE,");
            }
            BuildQuery.Append("  sum(INVTRN.TOT_AMT_IN_LOC_CURR),");
            BuildQuery.Append(" CUMT.CURRENCY_ID");
            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" ,BKS.CARGO_TYPE,");
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK JCPK");
                //Air Export
            }
            else if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" ,0 CARGO_TYPE,");
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK JCPK");
                //Sea Import
            }
            else if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" ,JOB.CARGO_TYPE,");
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK JCPK");
                //Air Import
            }
            else if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" ,0 CARGO_TYPE,");
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK JCPK");
            }
            BuildQuery.Append(" FROM");
            BuildQuery.Append(" CONSOL_INVOICE_TBL INV, ");
            BuildQuery.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
            //BuildQuery.Append(" COLLECTIONS_TRN_TBL CLT,")
            //BuildQuery.Append(" COLLECTIONS_TBL    COL,")

            //Sea Export
            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,BOOKING_MST_TBL BKS,");
                BuildQuery.Append(" HBL_EXP_TBL            HBL,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT,");
                BuildQuery.Append(" VESSEL_VOYAGE_TRN      VTRN");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                BuildQuery.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                BuildQuery.Append(" AND JOB.BOOKING_MST_FK=BKS.BOOKING_MST_PK(+)");
                BuildQuery.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                BuildQuery.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Booking
                    if (Convert.ToInt32(DocType )== 2)
                    {
                        BuildQuery.Append(" AND BKS.BOOKING_MST_PK=" + DocPK + "");
                        //'Jobcard
                    }
                    else if (Convert.ToInt32(DocType) == 3)
                    {
                        BuildQuery.Append("  AND JOB.JOB_CARD_TRN_PK=" + DocPK + "");
                        //'HBL
                    }
                    else if (Convert.ToInt32(DocType) == 4)
                    {
                        BuildQuery.Append(" AND HBL.HBL_EXP_TBL_PK=" + DocPK + "");
                    }
                    else
                    {
                        BuildQuery.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }
                }
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    BuildQuery.Append(" AND VTRN.VOYAGE LIKE '%" + VslVoy.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(Polfk))
                {
                    BuildQuery.Append(" AND BKS.PORT_MST_POL_FK=" + Polfk + "");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    BuildQuery.Append(" AND BKS.PORT_MST_POD_FK=" + Podfk + "");
                }
            }
            //Air Export
            if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,BOOKING_MST_TBL BKA,");
                BuildQuery.Append(" HAWB_EXP_TBL           HAWB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                BuildQuery.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                BuildQuery.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                BuildQuery.Append(" AND BKA.BOOKING_MST_PK(+)=JOB.BOOKING_MST_FK");
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Booking
                    if (Convert.ToInt32(DocType) == 2)
                    {
                        BuildQuery.Append(" AND BKA.BOOKING_MST_PK=" + DocPK + "");
                        //'Jobcard
                    }
                    else if (Convert.ToInt32(DocType) == 3)
                    {
                        BuildQuery.Append("  AND JOB.JOB_CARD_TRN_PK=" + DocPK + "");
                        //'HBL
                    }
                    else if (Convert.ToInt32(DocType) == 4)
                    {
                        BuildQuery.Append(" AND HAWB.HAWB_EXP_TBL_PK=" + DocPK + "");
                    }
                    else
                    {
                        BuildQuery.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }
                }
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    BuildQuery.Append(" AND JOB.VOYAGE_flight_no  LIKE '%" + VslVoy.Trim() + "%' ");
                }
                if (!string.IsNullOrEmpty(Polfk))
                {
                    BuildQuery.Append(" AND BKA.PORT_MST_POL_FK=" + Polfk + "");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    BuildQuery.Append(" AND BKA.PORT_MST_POD_FK=" + Podfk + "");
                }
            }
            //Sea Import
            if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT,");
                BuildQuery.Append(" VESSEL_VOYAGE_TRN      VTRN");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                BuildQuery.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                BuildQuery.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK");
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Jobcard
                    if (Convert.ToInt32(DocType) == 3)
                    {
                        BuildQuery.Append("  AND JOB.JOB_CARD_TRN_PK=" + DocPK + "");
                    }
                    else
                    {
                        BuildQuery.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }
                }
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    BuildQuery.Append(" AND VTRN.VOYAGE LIKE '%" + VslVoy.Trim() + "%'");
                }
                if (!string.IsNullOrEmpty(Polfk))
                {
                    BuildQuery.Append(" AND JOB.PORT_MST_POL_FK=" + Polfk + "");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    BuildQuery.Append(" AND JOB.PORT_MST_POD_FK=" + Podfk + "");
                }
            }
            //Air Import
            if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                BuildQuery.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(DocPK))
                {
                    //'Jobcard
                    if (Convert.ToInt32(DocType) == 3)
                    {
                        BuildQuery.Append("  AND JOB.JOB_CARD_TRN_PK=" + DocPK + "");
                    }
                    else
                    {
                        BuildQuery.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
                    }
                }
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    BuildQuery.Append(" AND JOB.VOYAGE_flight_no  LIKE '%" + VslVoy.Trim() + "%' ");
                }
                if (!string.IsNullOrEmpty(Polfk))
                {
                    BuildQuery.Append(" AND JOB.PORT_MST_POL_FK=" + Polfk + "");
                }
                if (!string.IsNullOrEmpty(Podfk))
                {
                    BuildQuery.Append(" AND JOB.PORT_MST_POD_FK=" + Podfk + "");
                }
            }

            BuildQuery.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            BuildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            BuildQuery.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            //BuildQuery.Append(" AND COL.COLLECTIONS_TBL_PK=CLT.COLLECTIONS_TBL_FK")
            //BuildQuery.Append(" AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO")
            //BuildQuery.Append(" AND (INV.NET_RECEIVABLE - NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn")
            //BuildQuery.Append("   where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0)) > 0")
            BuildQuery.Append("  AND NVL((INV.NET_RECEIVABLE - NVL((select sum(WMT.WRITEOFF_AMOUNT) from Writeoff_Manual_Tbl WMT");
            BuildQuery.Append("  where WMT.CONSOL_INVOICE_FK=INV.CONSOL_INVOICE_PK),0.00)-NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
            BuildQuery.Append("  where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0.00) - NVL((SELECT SUM(CTRN.TDS_AMOUNT) FROM COLLECTIONS_TRN_TBL CTRN WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),  0.00)),0) > 0");
            BuildQuery.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
            BuildQuery.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
            BuildQuery.Append(" AND INV.CHK_INVOICE=1 ");
            BuildQuery.Append(" AND INV.IS_FAC_INV<>1 ");
            BuildQuery.Append(" AND INVTRN.JOB_TYPE = 1");
            if (!string.IsNullOrEmpty(Custpk))
            {
                BuildQuery.Append(" AND CMT.CUSTOMER_MST_PK=" + Custpk + "");
            }

            if (!string.IsNullOrEmpty(Invrefno))
            {
                BuildQuery.Append(" AND INV.INVOICE_REF_NO = '" + Invrefno + "' ");
            }

            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }
            //If getDefault(fromDate, "") <> "" And getDefault(ToDate, "") <> "" Then
            //    BuildQuery.Append(" AND INV.invoice_date  BETWEEN TO_DATE('" & fromDate & "', '" & dateFormat & "') AND TO_DATE('" & ToDate & "', '" & dateFormat & "')")
            //ElseIf getDefault(fromDate, "") <> "" Then
            //    BuildQuery.Append(" AND INV.invoice_date  >=TO_DATE('" & fromDate & "' ,'" & dateFormat & "')")
            //ElseIf getDefault(ToDate, "") <> "" Then
            //    BuildQuery.Append(" AND INV.invoice_date  <=TO_DATE('" & ToDate & "' ,'" & dateFormat & "')")
            //End If
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            //Sea Export
            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" Group by INV.CONSOL_INVOICE_PK, JOB.JOBCARD_REF_NO, JOB.JOBCARD_DATE,");
                BuildQuery.Append(" HBL.HBL_REF_NO, JOB.VESSEL_NAME,JOB.VOYAGE_FLIGHT_NO,CUMT.CURRENCY_ID,BKS.CARGO_TYPE,JOB.JOB_CARD_TRN_PK ");
                //Air Export
            }
            else if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" Group by INV.CONSOL_INVOICE_PK, JOB.JOBCARD_REF_NO, JOB.JOBCARD_DATE,");
                BuildQuery.Append(" HAWB.HAWB_REF_NO, JOB.VOYAGE_FLIGHT_NO,CUMT.CURRENCY_ID,JOB.JOB_CARD_TRN_PK ");
                //Sea Import
            }
            else if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" Group by INV.CONSOL_INVOICE_PK, JOB.JOBCARD_REF_NO, JOB.JOBCARD_DATE,");
                BuildQuery.Append(" JOB.Hbl_HAWB_REF_NO, JOB.VESSEL_NAME,JOB.VOYAGE_FLIGHT_NO,CUMT.CURRENCY_ID,JOB.CARGO_TYPE,JOB.JOB_CARD_TRN_PK ");
                //Air Import
            }
            else if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" Group by INV.CONSOL_INVOICE_PK, JOB.JOBCARD_REF_NO, JOB.JOBCARD_DATE,");
                BuildQuery.Append(" JOB.Hbl_HAWB_REF_NO, JOB.VOYAGE_FLIGHT_NO,CUMT.CURRENCY_ID,JOB.JOB_CARD_TRN_PK");
            }
            BuildQuery.Append(" UNION ");
            BuildQuery.Append("  SELECT INV.CONSOL_INVOICE_PK PK,");
            BuildQuery.Append("               CT.CBJC_NO JOBCARD_REF_NO,");
            BuildQuery.Append("       'Customs Brokerage' JOB_TYPE,");
            BuildQuery.Append("       CT.CBJC_DATE JOBCARD_DATE,");
            BuildQuery.Append("               CT.HBL_NO REFNR,");
            if (BizType == 2)
            {
                BuildQuery.Append("               (CASE");
                BuildQuery.Append("                 WHEN (NVL(VVT.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '') = '/') THEN");
                BuildQuery.Append("                  ''");
                BuildQuery.Append("                 ELSE");
                BuildQuery.Append("                  NVL(VVT.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '')");
                BuildQuery.Append("               END) AS VESVOYAGE,");
            }
            else
            {
                BuildQuery.Append("               (CASE");
                BuildQuery.Append("                 WHEN (NVL(AMT.AIRLINE_NAME, '') || '/' || NVL(CT.FLIGHT_NO, '') = '/') THEN");
                BuildQuery.Append("                  ''");
                BuildQuery.Append("                 ELSE");
                BuildQuery.Append("                  NVL(AMT.AIRLINE_NAME, '') || '/' || NVL(CT.FLIGHT_NO, '')");
                BuildQuery.Append("               END) AS VESVOYAGE,");
            }

            BuildQuery.Append("               SUM(INVTRN.TOT_AMT_IN_LOC_CURR),");
            BuildQuery.Append("               CUMT.CURRENCY_ID, CT.CARGO_TYPE,");
            BuildQuery.Append("               CT.CBJC_PK JCPK");
            BuildQuery.Append("          FROM CONSOL_INVOICE_TBL     INV,");
            BuildQuery.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
            BuildQuery.Append("               CBJC_TBL   CT,");
            BuildQuery.Append("               CUSTOMER_MST_TBL       CMT,");
            BuildQuery.Append("               CURRENCY_TYPE_MST_TBL  CUMT,");
            BuildQuery.Append("               USER_MST_TBL           UMT,");
            if (BizType == 2)
            {
                BuildQuery.Append("               VESSEL_VOYAGE_TBL      VVT,");
                BuildQuery.Append("               VESSEL_VOYAGE_TRN      VTRN");
            }
            else
            {
                BuildQuery.Append("               AIRLINE_MST_TBL        AMT");
            }
            BuildQuery.Append("         WHERE INVTRN.JOB_CARD_FK = CT.CBJC_PK");
            BuildQuery.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            if (BizType == 2)
            {
                BuildQuery.Append("           AND VVT.VESSEL_VOYAGE_TBL_PK = VTRN.VESSEL_VOYAGE_TBL_FK");
                BuildQuery.Append("           AND VTRN.VOYAGE_TRN_PK = CT.VOYAGE_TRN_FK(+)");
            }
            else
            {
                BuildQuery.Append("           AND AMT.AIRLINE_MST_PK = CT.OPERATOR_MST_FK(+)");
            }
            BuildQuery.Append("           AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            BuildQuery.Append("           AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
            BuildQuery.Append("           AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            BuildQuery.Append("           AND NVL((INV.NET_RECEIVABLE -");
            BuildQuery.Append("                   NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
            BuildQuery.Append("                          FROM WRITEOFF_MANUAL_TBL WMT");
            BuildQuery.Append("                         WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
            BuildQuery.Append("                        0.00) -");
            BuildQuery.Append("                   NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            BuildQuery.Append("                          FROM COLLECTIONS_TRN_TBL CTRN");
            BuildQuery.Append("                         WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            BuildQuery.Append("                        0.00) -");
            BuildQuery.Append("                   NVL((SELECT SUM(CTRN.TDS_AMOUNT)");
            BuildQuery.Append("                          FROM COLLECTIONS_TRN_TBL CTRN");
            BuildQuery.Append("                         WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            BuildQuery.Append("                        0.00)),");
            BuildQuery.Append("                   0) > 0");
            BuildQuery.Append("           AND INV.PROCESS_TYPE = " + process);
            BuildQuery.Append("           AND INV.BUSINESS_TYPE = " + BizType);
            BuildQuery.Append("           AND INV.CHK_INVOICE = 1");
            BuildQuery.Append("           AND INV.IS_FAC_INV <> 1");
            BuildQuery.Append("   AND INVTRN.JOB_TYPE = 2");
            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(DocPK))
            {
                BuildQuery.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
            }
            BuildQuery.Append("         GROUP BY INV.CONSOL_INVOICE_PK,");
            BuildQuery.Append("                  CT.CBJC_NO,CT.CBJC_DATE,");
            BuildQuery.Append("                  CT.HBL_NO,");
            if (BizType == 2)
            {
                BuildQuery.Append("                  VVT.VESSEL_NAME,");
                BuildQuery.Append("                  VTRN.VOYAGE,");
            }
            else
            {
                BuildQuery.Append("                  AMT.AIRLINE_NAME,");
                BuildQuery.Append("                  CT.FLIGHT_NO,");
            }
            BuildQuery.Append("                  CUMT.CURRENCY_ID,CT.CARGO_TYPE,CT.CBJC_PK ");

            BuildQuery.Append(" UNION ");
            BuildQuery.Append("SELECT INV.CONSOL_INVOICE_PK PK,");
            BuildQuery.Append("               CT.TRANS_INST_REF_NO JOBCARD_REF_NO,");
            BuildQuery.Append("       'Transport Note' JOB_TYPE,");
            BuildQuery.Append("       CT.TRANS_INST_DATE JOBCARD_DATE,");
            BuildQuery.Append("               '' REFNR,");
            if (BizType == 2)
            {
                BuildQuery.Append("               (CASE");
                BuildQuery.Append("                 WHEN (NVL(VVT.VESSEL_NAME, '') || '/' ||");
                BuildQuery.Append("                      NVL(VTRN.VOYAGE, '') = '/') THEN");
                BuildQuery.Append("                  ''");
                BuildQuery.Append("                 ELSE");
                BuildQuery.Append("                  NVL(VVT.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '')");
                BuildQuery.Append("               END) AS VESVOYAGE,");
            }
            else
            {
                BuildQuery.Append("               (CASE");
                BuildQuery.Append("                 WHEN (NVL(AMT.AIRLINE_NAME, '') || '/' || NVL(CT.FLIGHT_NO, '') = '/') THEN");
                BuildQuery.Append("                  ''");
                BuildQuery.Append("                 ELSE");
                BuildQuery.Append("                  NVL(AMT.AIRLINE_NAME, '') || '/' || NVL(CT.FLIGHT_NO, '')");
                BuildQuery.Append("               END) AS VESVOYAGE,");
            }
            BuildQuery.Append("               SUM(INVTRN.TOT_AMT_IN_LOC_CURR),");
            BuildQuery.Append("               CUMT.CURRENCY_ID, CT.CARGO_TYPE,");
            BuildQuery.Append("               CT.TRANSPORT_INST_SEA_PK JCPK");
            BuildQuery.Append("          FROM CONSOL_INVOICE_TBL     INV,");
            BuildQuery.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
            BuildQuery.Append("               TRANSPORT_INST_SEA_TBL CT,");
            BuildQuery.Append("               CUSTOMER_MST_TBL       CMT,");
            BuildQuery.Append("               CURRENCY_TYPE_MST_TBL  CUMT,");
            BuildQuery.Append("               USER_MST_TBL           UMT,");
            if (BizType == 2)
            {
                BuildQuery.Append("               VESSEL_VOYAGE_TBL      VVT,");
                BuildQuery.Append("               VESSEL_VOYAGE_TRN      VTRN");
            }
            else
            {
                BuildQuery.Append("               AIRLINE_MST_TBL        AMT");
            }
            BuildQuery.Append("         WHERE INVTRN.JOB_CARD_FK = CT.TRANSPORT_INST_SEA_PK");
            BuildQuery.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            if (BizType == 2)
            {
                BuildQuery.Append("           AND VVT.VESSEL_VOYAGE_TBL_PK = VTRN.VESSEL_VOYAGE_TBL_FK(+)");
                BuildQuery.Append("           AND VVT.VESSEL_VOYAGE_TBL_PK(+) = CT.VSL_VOY_FK");
            }
            else
            {
                BuildQuery.Append("           AND AMT.AIRLINE_MST_PK = CT.OPERATOR_MST_FK(+)");
            }

            BuildQuery.Append("           AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            BuildQuery.Append("           AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
            BuildQuery.Append("           AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            BuildQuery.Append("           AND NVL((INV.NET_RECEIVABLE -");
            BuildQuery.Append("                   NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
            BuildQuery.Append("                          FROM WRITEOFF_MANUAL_TBL WMT");
            BuildQuery.Append("                         WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
            BuildQuery.Append("                        0.00) -");
            BuildQuery.Append("                   NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            BuildQuery.Append("                          FROM COLLECTIONS_TRN_TBL CTRN");
            BuildQuery.Append("                         WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            BuildQuery.Append("                        0.00) -");
            BuildQuery.Append("                   NVL((SELECT SUM(CTRN.TDS_AMOUNT)");
            BuildQuery.Append("                          FROM COLLECTIONS_TRN_TBL CTRN");
            BuildQuery.Append("                         WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            BuildQuery.Append("                        0.00)),");
            BuildQuery.Append("                   0) > 0");
            BuildQuery.Append("           AND INV.PROCESS_TYPE = " + process);
            BuildQuery.Append("           AND INV.BUSINESS_TYPE = " + BizType);
            BuildQuery.Append("           AND INV.CHK_INVOICE = 1");
            BuildQuery.Append("           AND INV.IS_FAC_INV <> 1");
            BuildQuery.Append("   AND INVTRN.JOB_TYPE = 3");
            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(DocPK))
            {
                BuildQuery.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
            }
            BuildQuery.Append("         GROUP BY INV.CONSOL_INVOICE_PK,");
            BuildQuery.Append("                  CT.TRANS_INST_REF_NO,CT.TRANS_INST_DATE,");
            if (BizType == 2)
            {
                BuildQuery.Append("                  VVT.VESSEL_NAME,");
                BuildQuery.Append("                  VTRN.VOYAGE,");
            }
            else
            {
                BuildQuery.Append("                  AMT.AIRLINE_NAME,");
                BuildQuery.Append("                  CT.FLIGHT_NO,");
            }
            BuildQuery.Append("                  CUMT.CURRENCY_ID,CT.CARGO_TYPE,CT.TRANSPORT_INST_SEA_PK ");

            ///
            BuildQuery.Append(" UNION SELECT INV.CONSOL_INVOICE_PK PK,");
            BuildQuery.Append("       DCH.DEM_CALC_ID JOBCARD_REF_NO,");
            BuildQuery.Append("       'Det.&Dem.' JOB_TYPE,");
            BuildQuery.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE,");
            BuildQuery.Append("       '' REFNR,");
            if (BizType == 2)
            {
                BuildQuery.Append("       (CASE");
                BuildQuery.Append("         WHEN (NVL(VVT.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '') = '/') THEN");
                BuildQuery.Append("          ''");
                BuildQuery.Append("         ELSE");
                BuildQuery.Append("          NVL(VVT.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '')");
                BuildQuery.Append("       END) AS VESVOYAGE,");
            }
            else
            {
                BuildQuery.Append("   TPN.FLIGHT_NO AS VESVOYAGE,");
            }
            BuildQuery.Append("       SUM(INVTRN.TOT_AMT_IN_LOC_CURR),");
            BuildQuery.Append("       CUMT.CURRENCY_ID,");
            BuildQuery.Append("       DCH.CARGO_TYPE,");
            BuildQuery.Append("       DCH.DEM_CALC_HDR_PK JCPK");
            BuildQuery.Append("  FROM CONSOL_INVOICE_TBL     INV,");
            BuildQuery.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
            BuildQuery.Append("       DEM_CALC_HDR           DCH,");
            BuildQuery.Append("       TRANSPORT_INST_SEA_TBL TPN,");
            BuildQuery.Append("       CUSTOMER_MST_TBL       CMT,");
            BuildQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT,");
            BuildQuery.Append("       USER_MST_TBL           UMT");
            if (BizType == 2)
            {
                BuildQuery.Append(", VESSEL_VOYAGE_TBL      VVT,");
                BuildQuery.Append("  VESSEL_VOYAGE_TRN      VTRN");
            }
            BuildQuery.Append(" WHERE DCH.DEM_CALC_HDR_PK = INVTRN.JOB_CARD_FK");
            BuildQuery.Append("   AND DCH.DOC_REF_FK = TPN.TRANSPORT_INST_SEA_PK");
            BuildQuery.Append("   AND DCH.DOC_TYPE = 0");
            BuildQuery.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            if (BizType == 2)
            {
                BuildQuery.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK = VTRN.VESSEL_VOYAGE_TBL_FK");
                BuildQuery.Append("   AND VTRN.VOYAGE_TRN_PK = TPN.VSL_VOY_FK(+)");
            }
            BuildQuery.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            BuildQuery.Append("   AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
            BuildQuery.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append("   AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            BuildQuery.Append("   AND NVL((INV.NET_RECEIVABLE -");
            BuildQuery.Append("           NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
            BuildQuery.Append("                  FROM WRITEOFF_MANUAL_TBL WMT");
            BuildQuery.Append("                 WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
            BuildQuery.Append("                0.00) -");
            BuildQuery.Append("           NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            BuildQuery.Append("                  FROM COLLECTIONS_TRN_TBL CTRN");
            BuildQuery.Append("                 WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            BuildQuery.Append("                0.00) -");
            BuildQuery.Append("           NVL((SELECT SUM(CTRN.TDS_AMOUNT)");
            BuildQuery.Append("                  FROM COLLECTIONS_TRN_TBL CTRN");
            BuildQuery.Append("                 WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            BuildQuery.Append("                0.00)),");
            BuildQuery.Append("           0) > 0");
            BuildQuery.Append("           AND INV.PROCESS_TYPE = " + process);
            BuildQuery.Append("           AND INV.BUSINESS_TYPE = " + BizType);
            BuildQuery.Append("   AND INV.CHK_INVOICE = 1");
            BuildQuery.Append("   AND INV.IS_FAC_INV <> 1");
            BuildQuery.Append("   AND INVTRN.JOB_TYPE = 4");

            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(DocPK))
            {
                BuildQuery.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
            }
            BuildQuery.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
            BuildQuery.Append("          DCH.DEM_CALC_ID,");
            BuildQuery.Append("          DCH.DEM_CALC_DATE,");
            if (BizType == 2)
            {
                BuildQuery.Append("          VVT.VESSEL_NAME,");
                BuildQuery.Append("          VTRN.VOYAGE,");
            }
            else
            {
                BuildQuery.Append("          TPN.FLIGHT_NO, ");
            }
            BuildQuery.Append("          CUMT.CURRENCY_ID,");
            BuildQuery.Append("          DCH.CARGO_TYPE,");
            BuildQuery.Append("          DCH.DEM_CALC_HDR_PK ");

            BuildQuery.Append(" UNION SELECT INV.CONSOL_INVOICE_PK PK,");
            BuildQuery.Append("       DCH.DEM_CALC_ID JOBCARD_REF_NO,");
            BuildQuery.Append("       'Det.&Dem.' JOB_TYPE,");
            BuildQuery.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE,");
            BuildQuery.Append("       CT.HBL_NO REFNR,");
            if (BizType == 2)
            {
                BuildQuery.Append("       (CASE");
                BuildQuery.Append("         WHEN (NVL(VVT.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '') = '/') THEN");
                BuildQuery.Append("          ''");
                BuildQuery.Append("         ELSE");
                BuildQuery.Append("          NVL(VVT.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '')");
                BuildQuery.Append("       END) AS VESVOYAGE,");
            }
            else
            {
                BuildQuery.Append("   CT.FLIGHT_NO AS VESVOYAGE,");
            }
            BuildQuery.Append("       SUM(INVTRN.TOT_AMT_IN_LOC_CURR),");
            BuildQuery.Append("       CUMT.CURRENCY_ID,");
            BuildQuery.Append("       DCH.CARGO_TYPE,");
            BuildQuery.Append("       DCH.DEM_CALC_HDR_PK JCPK");
            BuildQuery.Append("  FROM CONSOL_INVOICE_TBL     INV,");
            BuildQuery.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
            BuildQuery.Append("       DEM_CALC_HDR           DCH,");
            BuildQuery.Append("       CBJC_TBL               CT,");
            BuildQuery.Append("       CUSTOMER_MST_TBL       CMT,");
            BuildQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT,");
            BuildQuery.Append("       USER_MST_TBL           UMT");
            if (BizType == 2)
            {
                BuildQuery.Append(", VESSEL_VOYAGE_TBL      VVT,");
                BuildQuery.Append("  VESSEL_VOYAGE_TRN      VTRN");
            }
            BuildQuery.Append(" WHERE DCH.DEM_CALC_HDR_PK = INVTRN.JOB_CARD_FK");
            BuildQuery.Append("   AND DCH.DOC_REF_FK = CT.CBJC_PK");
            BuildQuery.Append("   AND DCH.DOC_TYPE = 1");
            BuildQuery.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            if (BizType == 2)
            {
                BuildQuery.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK = VTRN.VESSEL_VOYAGE_TBL_FK");
                BuildQuery.Append("   AND VTRN.VOYAGE_TRN_PK = CT.VOYAGE_TRN_FK(+)");
            }
            BuildQuery.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            BuildQuery.Append("   AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
            BuildQuery.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append("   AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            BuildQuery.Append("   AND NVL((INV.NET_RECEIVABLE -");
            BuildQuery.Append("           NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
            BuildQuery.Append("                  FROM WRITEOFF_MANUAL_TBL WMT");
            BuildQuery.Append("                 WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
            BuildQuery.Append("                0.00) -");
            BuildQuery.Append("           NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            BuildQuery.Append("                  FROM COLLECTIONS_TRN_TBL CTRN");
            BuildQuery.Append("                 WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            BuildQuery.Append("                0.00) -");
            BuildQuery.Append("           NVL((SELECT SUM(CTRN.TDS_AMOUNT)");
            BuildQuery.Append("                  FROM COLLECTIONS_TRN_TBL CTRN");
            BuildQuery.Append("                 WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            BuildQuery.Append("                0.00)),");
            BuildQuery.Append("           0) > 0");
            BuildQuery.Append("           AND INV.PROCESS_TYPE = " + process);
            BuildQuery.Append("           AND INV.BUSINESS_TYPE = " + BizType);
            BuildQuery.Append("   AND INV.CHK_INVOICE = 1");
            BuildQuery.Append("   AND INV.IS_FAC_INV <> 1");
            BuildQuery.Append("   AND INVTRN.JOB_TYPE = 4");

            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                BuildQuery.Append(" AND TO_DATE(INV.invoice_date, '" + dateFormat + "') <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(DocPK))
            {
                BuildQuery.Append(" AND INVTRN.JOB_CARD_FK = " + DocPK);
            }
            BuildQuery.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
            BuildQuery.Append("          DCH.DEM_CALC_ID,");
            BuildQuery.Append("          DCH.DEM_CALC_DATE,");
            BuildQuery.Append("          CT.HBL_NO,");
            if (BizType == 2)
            {
                BuildQuery.Append("      VVT.VESSEL_NAME,");
                BuildQuery.Append("      VTRN.VOYAGE,");
            }
            else
            {
                BuildQuery.Append("      CT.FLIGHT_NO,");
            }
            BuildQuery.Append("          CUMT.CURRENCY_ID,");
            BuildQuery.Append("          DCH.CARGO_TYPE,");
            BuildQuery.Append("          DCH.DEM_CALC_HDR_PK ");
            ///
            BuildQuery.Append(" ) T ");

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
        //Child Table
        #region "Child Table"
        public DataTable Fetchchildlist(string CONTSpotPKs = "", long Biztype = 0, long Process = 0, string fromDate = "", string ToDate = "", string Collectionrefno = "", string Invrefno = "", string Custpk = "", long usrLocFK = 0, string SortColumn = "",
        string SortType = "DESC")
        {

            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            string strsql = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dt = null;
            int RowCnt = 0;
            int Rno = 0;
            int pk = 0;

            buildQuery.Append(" SELECT ROWNUM \"SL.NR\", T.* ");
            buildQuery.Append(" FROM ");
            buildQuery.Append(" (SELECT DISTINCT COL.COLLECTIONS_TBL_PK PK, ");
            buildQuery.Append(" CTRN.INVOICE_REF_NR INVOICENR, ");
            buildQuery.Append(" CUR.CURRENCY_ID RECDAMOUNT, ");
            buildQuery.Append(" CIT.NET_RECEIVABLE CURRENCY, ");
            buildQuery.Append(" (SELECT ROWTOCOL('SELECT DECODE(CT.JOB_TYPE,");
            buildQuery.Append(" ''1'',");
            buildQuery.Append(" ''JobCard'',");
            buildQuery.Append(" ''2'',");
            buildQuery.Append(" ''CustomsBrokerage'',");
            buildQuery.Append(" ''3'',");
            buildQuery.Append(" ''TansportNote'') FROM CONSOL_INVOICE_TRN_TBL CT");
            buildQuery.Append(" WHERE CT.CONSOL_INVOICE_FK=' || CIT.CONSOL_INVOICE_PK || '') FROM DUAL) JOB_TYPE");
            //buildQuery.Append("  '" & Session("CURRENCY_ID") & "' CURRENCY  ")
            buildQuery.Append(" FROM COLLECTIONS_TBL       COL, ");
            buildQuery.Append(" COLLECTIONS_TRN_TBL   CTRN, ");
            buildQuery.Append(" CUSTOMER_MST_TBL      CMT, USER_MST_TBL UMT,");
            buildQuery.Append(" CONSOL_INVOICE_TBL     CIT,  ");
            buildQuery.Append(" CONSOL_INVOICE_TRN_TBL CITT, ");
            buildQuery.Append(" CURRENCY_TYPE_MST_TBL CUR ");
            buildQuery.Append(" WHERE ");
            buildQuery.Append(" COL.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK ");
            buildQuery.Append(" AND COL.BUSINESS_TYPE  = '" + Biztype + "' ");
            buildQuery.Append(" AND COL.PROCESS_TYPE= '" + Process + "' ");
            buildQuery.Append(" AND CMT.CUSTOMER_MST_PK = COL.CUSTOMER_MST_FK ");
            buildQuery.Append(" AND CUR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK ");
            buildQuery.Append(" AND CIT.INVOICE_REF_NO = CTRN.INVOICE_REF_NR ");
            buildQuery.Append(" AND CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK ");
            buildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            buildQuery.Append(" AND COL.CREATED_BY_FK = UMT.USER_MST_PK ");

            if (!string.IsNullOrEmpty(Custpk))
            {
                buildQuery.Append(" AND COL.CUSTOMER_MST_FK = '" + Custpk + "' ");
            }

            if (!string.IsNullOrEmpty(Collectionrefno))
            {
                buildQuery.Append(" AND UPPER(COL.COLLECTIONS_REF_NO) LIKE '%" + Collectionrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (!string.IsNullOrEmpty(Invrefno))
            {
                buildQuery.Append(" AND UPPER(CTRN.INVOICE_REF_NR) LIKE '%" + Invrefno.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            //commenting & adding by thiyagarajan on 29/4/09 

            //If (Not fromDate Is Nothing Or Not fromDate = "") And (ToDate Is Nothing Or Not ToDate = "") Then
            //    buildQuery.Append(" AND COL.COLLECTIONS_DATE  >=TO_DATE('" & fromDate & "' ,'" & dateFormat & "')")
            //End If

            //If (Not ToDate Is Nothing Or Not ToDate = "") And (fromDate Is Nothing Or Not fromDate = "") Then
            //    buildQuery.Append(" AND COL.COLLECTIONS_DATE  <=TO_DATE('" & ToDate & "' ,'" & dateFormat & "')")
            //End If

            //If (Not fromDate Is Nothing Or Not fromDate = "") And (Not ToDate Is Nothing Or Not ToDate = "") Then
            //    buildQuery.Append(" AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" & fromDate & "', '" & dateFormat & "') AND TO_DATE('" & ToDate & "', '" & dateFormat & "')")
            //End If

            //If getDefault(fromDate, "") <> "" And getDefault(ToDate, "") <> "" Then
            //    buildQuery.Append(" AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" & fromDate & "', '" & dateFormat & "') AND TO_DATE('" & ToDate & "', '" & dateFormat & "')")
            //ElseIf getDefault(fromDate, "") <> "" Then
            //    buildQuery.Append(" AND COL.COLLECTIONS_DATE  >=TO_DATE('" & fromDate & "' ,'" & dateFormat & "')")
            //ElseIf getDefault(ToDate, "") <> "" Then
            //    buildQuery.Append(" AND COL.COLLECTIONS_DATE  <=TO_DATE('" & ToDate & "' ,'" & dateFormat & "')")
            //End If
            //end
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                buildQuery.Append(" AND TO_DATE(COL.COLLECTIONS_DATE, '" + dateFormat + "') >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                buildQuery.Append(" AND TO_DATE(COL.COLLECTIONS_DATE, '" + dateFormat + "') <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                buildQuery.Append(" AND COL.COLLECTIONS_TBL_PK  in (" + CONTSpotPKs + ") ");
            }
            buildQuery.Append(")T ");

            strsql = buildQuery.ToString();
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
        //Code added by Sreenvias on 04/02/2010
        #region "Fetch Collection Listing Records"
        public int GetCLCount(string clRefNr, int clPk, long locPk)
        {
            try
            {
                System.Text.StringBuilder strCLQuery = new System.Text.StringBuilder(5000);
                strCLQuery.Append(" select ct.collections_tbl_pk, ct.collections_ref_no");
                strCLQuery.Append(" from collections_tbl ct, user_mst_tbl umt");
                strCLQuery.Append(" where ct.collections_ref_no like '%" + clRefNr + "%' ");
                strCLQuery.Append(" and ct.created_by_fk = umt.user_mst_pk");
                strCLQuery.Append(" and umt.default_location_fk=" + locPk);

                WorkFlow objWF = new WorkFlow();
                DataSet objCLDS = new DataSet();
                objCLDS = objWF.GetDataSet(strCLQuery.ToString());
                if (objCLDS.Tables[0].Rows.Count == 1)
                {
                    clPk = Convert.ToInt32(objCLDS.Tables[0].Rows[0][0]);
                }
                return objCLDS.Tables[0].Rows.Count;
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
        //End Sreenvias
        #region "Fetch Invoice Nr."
        public int FetchInvNr(short Biz, short Proc, string InvNr = "0")
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            //Sea & Export
            if (Biz == 2 & Proc == 1)
            {
                strSQL = "SELECT count(*) FROM INV_CUST_SEA_EXP_TBL  INV" +  "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
                //Air Export
            }
            else if (Biz == 1 & Proc == 1)
            {
                strSQL = "SELECT count(*) FROM INV_CUST_AIR_EXP_TBL  INV" +  "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
                //Sea Import
            }
            else if (Biz == 2 & Proc == 2)
            {
                strSQL = "SELECT count(*) FROM INV_CUST_SEA_IMP_TBL  INV" +  "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
                //Air Import
            }
            else
            {
                strSQL = "SELECT count(*) FROM INV_CUST_AIR_IMP_TBL  INV" +  "WHERE INV.INVOICE_REF_NO= '" + InvNr + "'";
            }
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

        #region "Enhance Search"
        public string Fetch_Document_Nr(string strCond, string loc = "0")
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
            string DocType = "";
            string strSTATUS = "";
            var strNull = DBNull.Value;
            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strProcessType = arr[3];
            strloc = arr[4];
            if (arr.Length > 5)
                DocType = arr[5];
            if (arr.Length > 6)
                strSTATUS = arr[6];
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COLLECTION_PKG.GET_DOCUMENT_REFNR";
                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
                _with1.Add("LOCFK_IN", strloc).Direction = ParameterDirection.Input;
                _with1.Add("DOCUMENT_TYPE_IN", (string.IsNullOrEmpty(DocType) ? "" : DocType)).Direction = ParameterDirection.Input;
                _with1.Add("STATUS_IN", (string.IsNullOrEmpty(strSTATUS) ? "" : strSTATUS)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        public string Fetch_Collection_RefNr(string strCond, string loc = "0")
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
            string DocType = "";
            string FDate = "";
            string TDate = "";
            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strProcessType = arr[3];
            strloc = arr[4];
            if (arr.Length > 5)
                DocType = arr[5];
            if (arr.Length > 6)
                FDate = arr[6];
            if (arr.Length > 7)
                TDate = arr[7];
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COLLECTION_PKG.GET_COLLECTION_REFNR";
                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN :"")).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
                _with2.Add("LOCFK_IN", strloc).Direction = ParameterDirection.Input;
                _with2.Add("FDATE_IN", (!string.IsNullOrEmpty(FDate) ? FDate : "")).Direction = ParameterDirection.Input;
                _with2.Add("TDATE_IN", (!string.IsNullOrEmpty(TDate) ? TDate : "")).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
    }
}