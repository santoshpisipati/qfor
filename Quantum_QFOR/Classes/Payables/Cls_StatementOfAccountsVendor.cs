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
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_StatementOfAccountsVendor : CommonFeatures
    {

        #region "Vendor Stmt of A/C"
        public DataSet FetchLocationInformation(string Fromdate = "", string Todate = "", string CountryPK = "", string LocPK = "", string Vendor = "", string VendorType = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb2 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb3 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb4 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb5 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb6 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb7 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb8 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb9 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb10 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb11 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb12 = new System.Text.StringBuilder(5000);
            DataSet MainDS = new DataSet();
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            int LogLocPk = Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            OracleDataAdapter DA = new OracleDataAdapter();
            //GROUPID
            try
            {
                sb1.Append("       SELECT VMT.VENDOR_MST_PK,");
                sb1.Append("               LMT.LOCATION_MST_PK,");
                sb1.Append("               IST.INVOICE_DATE REF_DATE,");
                sb1.Append("               'V. INVOICE' TRANSACTION,");
                sb1.Append("               JCSE.HBL_NO SHIPMENT_REF_NR,");
                sb1.Append("               IST.INVOICE_REF_NO,");
                sb1.Append("               (SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb1.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb1.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb1.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb1.Append("                AND IST1.APPROVED <> 3) DEBIT,");
                sb1.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb1.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb1.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb1.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb1.Append("                AND IST1.APPROVED = 3),0) CREDIT,");

                sb1.Append("               0 BALANCE");
                sb1.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb1.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb1.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb1.Append("               VENDOR_SERVICES_TRN VS,");
                sb1.Append("               CBJC_TBL JCSE,");
                sb1.Append("               CBJC_TRN_COST JTFC,");
                sb1.Append("               INV_SUPPLIER_TBL     IST,");
                sb1.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb1.Append("               LOCATION_MST_TBL     LMT");
                sb1.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb1.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb1.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb1.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb1.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb1.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb1.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb1.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb1.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb1.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb1.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb1.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb1.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb1.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb1.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb1.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb1.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb1.Append("                  LMT.LOCATION_MST_PK,");
                sb1.Append("                  IST.INVOICE_DATE,");
                sb1.Append("                  JCSE.HBL_NO,");
                sb1.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");

                sb2.Append("        SELECT VMT.VENDOR_MST_PK,");
                sb2.Append("               LMT.LOCATION_MST_PK,");
                sb2.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb2.Append("               'PAYMENT' TRANSACTION,");
                sb2.Append("               JCSE.HBL_NO SHIPMENT_REF_NR,");
                sb2.Append("               PMT.PAYMENT_REF_NO,");
                sb2.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb2.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb2.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb2.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb2.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb2.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb2.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb2.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb2.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb2.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");

                sb2.Append("               0 BALANCE");
                sb2.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb2.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb2.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb2.Append("               VENDOR_SERVICES_TRN VS,");
                sb2.Append("               CBJC_TBL JCSE,");
                sb2.Append("               CBJC_TRN_COST JTFC,");
                sb2.Append("               INV_SUPPLIER_TBL     IST,");
                sb2.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb2.Append("               PAYMENTS_TBL         PMT,");
                sb2.Append("               PAYMENT_TRN_TBL      PTT,");
                sb2.Append("               LOCATION_MST_TBL     LMT");
                sb2.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb2.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb2.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb2.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb2.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb2.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb2.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb2.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb2.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb2.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb2.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb2.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb2.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb2.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb2.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb2.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb2.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb2.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb2.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb2.Append("                  LMT.LOCATION_MST_PK,");
                sb2.Append("                  PMT.PAYMENT_DATE,");
                sb2.Append("                  JCSE.HBL_NO,");
                sb2.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");

                sb3.Append("       SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb3.Append("               LMT.LOCATION_MST_PK,");
                sb3.Append("               IST.INVOICE_DATE REF_DATE,");
                sb3.Append("               'V. INVOICE' TRANSACTION,");
                sb3.Append("               JCSE.HBL_NO SHIPMENT_REF_NR,");
                sb3.Append("               IST.INVOICE_REF_NO,");
                sb3.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb3.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb3.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb3.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb3.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb3.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb3.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb3.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb3.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb3.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb3.Append("               0 BALANCE");
                sb3.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb3.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb3.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb3.Append("               VENDOR_SERVICES_TRN VS,");
                sb3.Append("               CBJC_TBL JCSE,");
                sb3.Append("               CBJC_TRN_COST JTFC,");
                sb3.Append("               INV_SUPPLIER_TBL     IST,");
                sb3.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb3.Append("               LOCATION_MST_TBL     LMT");
                sb3.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb3.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb3.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb3.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb3.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb3.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb3.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb3.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb3.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb3.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb3.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb3.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb3.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb3.Append("           AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb3.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb3.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb3.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb3.Append("                  LMT.LOCATION_MST_PK,");
                sb3.Append("                  IST.INVOICE_DATE,");
                sb3.Append("                  JCSE.HBL_NO,");
                sb3.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");

                sb4.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb4.Append("               LMT.LOCATION_MST_PK,");
                sb4.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb4.Append("               'PAYMENT' TRANSACTION,");
                sb4.Append("               JCSE.HBL_NO SHIPMENT_REF_NR,");
                sb4.Append("               PMT.PAYMENT_REF_NO,");
                sb4.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb4.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb4.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb4.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb4.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb4.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb4.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb4.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb4.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb4.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb4.Append("               0 BALANCE");
                sb4.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb4.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb4.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb4.Append("               VENDOR_SERVICES_TRN VS,");
                sb4.Append("               CBJC_TBL JCSE,");
                sb4.Append("               CBJC_TRN_COST JTFC,");
                sb4.Append("               INV_SUPPLIER_TBL     IST,");
                sb4.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb4.Append("               PAYMENTS_TBL         PMT, ");
                sb4.Append("               PAYMENT_TRN_TBL      PTT, ");
                sb4.Append("               LOCATION_MST_TBL     LMT");
                sb4.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb4.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb4.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb4.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb4.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb4.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb4.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb4.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb4.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb4.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb4.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb4.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb4.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb4.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb4.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb4.Append("           AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb4.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb4.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb4.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb4.Append("                  LMT.LOCATION_MST_PK,");
                sb4.Append("                  PMT.PAYMENT_DATE,");
                sb4.Append("                  JCSE.HBL_NO,");
                sb4.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");

                sb5.Append("  SELECT VMT.VENDOR_MST_PK,");
                sb5.Append("               LMT.LOCATION_MST_PK,");
                sb5.Append("               IST.INVOICE_DATE REF_DATE,");
                sb5.Append("               'V. INVOICE' TRANSACTION,");
                sb5.Append("               JCSE.HBL_NO SHIPMENT_REF_NR,");
                sb5.Append("               IST.INVOICE_REF_NO,");
                sb5.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb5.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb5.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb5.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb5.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb5.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb5.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb5.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb5.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb5.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb5.Append("               0 BALANCE");
                sb5.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb5.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb5.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb5.Append("               VENDOR_SERVICES_TRN VS,");
                sb5.Append("               CBJC_TBL JCSE,");
                sb5.Append("               CBJC_TRN_COST JTFC,");
                sb5.Append("               INV_SUPPLIER_TBL     IST,");
                sb5.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb5.Append("               LOCATION_MST_TBL     LMT");
                sb5.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb5.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb5.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb5.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb5.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb5.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb5.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb5.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb5.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb5.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb5.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb5.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb5.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb5.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb5.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb5.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb5.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb5.Append("                  LMT.LOCATION_MST_PK,");
                sb5.Append("                  IST.INVOICE_DATE,");
                sb5.Append("                  JCSE.HBL_NO,");
                sb5.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");

                sb6.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb6.Append("               LMT.LOCATION_MST_PK,");
                sb6.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb6.Append("               'PAYMENT' TRANSACTION,");
                sb6.Append("               JCSE.HBL_NO HBL_REF_NO,");
                sb6.Append("               PMT.PAYMENT_REF_NO,");
                sb6.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb6.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb6.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb6.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb6.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb6.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb6.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb6.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb6.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb6.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb6.Append("               0 BALANCE");
                sb6.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb6.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb6.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb6.Append("               VENDOR_SERVICES_TRN VS,");
                sb6.Append("               CBJC_TBL JCSE,");
                sb6.Append("               CBJC_TRN_COST JTFC,");
                sb6.Append("               INV_SUPPLIER_TBL     IST,");
                sb6.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb6.Append("               PAYMENTS_TBL         PMT,");
                sb6.Append("               PAYMENT_TRN_TBL      PTT,");
                sb6.Append("               LOCATION_MST_TBL     LMT");
                sb6.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb6.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb6.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb6.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb6.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb6.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb6.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb6.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb6.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb6.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb6.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb6.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb6.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb6.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb6.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb6.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb6.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb6.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb6.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb6.Append("                  LMT.LOCATION_MST_PK,");
                sb6.Append("                  PMT.PAYMENT_DATE,");
                sb6.Append("                  JCSE.HBL_NO,");
                sb6.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");
                ///''''''''''''''''''''''''''''''''''''''''''''''''''''

                sb7.Append("       SELECT VMT.VENDOR_MST_PK,");
                sb7.Append("               LMT.LOCATION_MST_PK,");
                sb7.Append("               IST.INVOICE_DATE REF_DATE,");
                sb7.Append("               'V. INVOICE' TRANSACTION,");
                sb7.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK=JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) SHIPMENT_REF_NR,");
                sb7.Append("               IST.INVOICE_REF_NO,");
                sb7.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb7.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb7.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb7.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb7.Append("                AND IST1.APPROVED <> 3),0) DEBIT,");
                sb7.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb7.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb7.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb7.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb7.Append("                AND IST1.APPROVED = 3),0) CREDIT,");
                sb7.Append("               0 BALANCE");
                sb7.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb7.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb7.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb7.Append("               VENDOR_SERVICES_TRN VS,");
                sb7.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb7.Append("               TRANSPORT_TRN_COST JTFC,");
                sb7.Append("               INV_SUPPLIER_TBL     IST,");
                sb7.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb7.Append("               LOCATION_MST_TBL     LMT");
                sb7.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb7.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb7.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb7.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb7.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb7.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb7.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb7.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb7.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb7.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb7.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb7.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb7.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb7.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb7.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb7.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb7.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb7.Append("                  LMT.LOCATION_MST_PK,");
                sb7.Append("                  IST.INVOICE_DATE,");
                sb7.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb7.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");

                sb8.Append("        SELECT VMT.VENDOR_MST_PK,");
                sb8.Append("               LMT.LOCATION_MST_PK,");
                sb8.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb8.Append("               'PAYMENT' TRANSACTION,");
                sb8.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK=JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) SHIPMENT_REF_NR,");
                sb8.Append("               PMT.PAYMENT_REF_NO,");
                sb8.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb8.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb8.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb8.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb8.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb8.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb8.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb8.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb8.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb8.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb8.Append("               0 BALANCE");
                sb8.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb8.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb8.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb8.Append("               VENDOR_SERVICES_TRN VS,");
                sb8.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb8.Append("               TRANSPORT_TRN_COST JTFC,");
                sb8.Append("               INV_SUPPLIER_TBL     IST,");
                sb8.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb8.Append("               PAYMENTS_TBL         PMT,");
                sb8.Append("               PAYMENT_TRN_TBL      PTT,");
                sb8.Append("               LOCATION_MST_TBL     LMT");
                sb8.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb8.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb8.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb8.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb8.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb8.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb8.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb8.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb8.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb8.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb8.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb8.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb8.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb8.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb8.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb8.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb8.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb8.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb8.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb8.Append("                  LMT.LOCATION_MST_PK,");
                sb8.Append("                  PMT.PAYMENT_DATE,");
                sb8.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb8.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");

                sb9.Append("       SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb9.Append("               LMT.LOCATION_MST_PK,");
                sb9.Append("               IST.INVOICE_DATE REF_DATE,");
                sb9.Append("               'V. INVOICE' TRANSACTION,");
                sb9.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK=JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) SHIPMENT_REF_NR,");
                sb9.Append("               IST.INVOICE_REF_NO,");
                sb9.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb9.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb9.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb9.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb9.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb9.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb9.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb9.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb9.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb9.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb9.Append("               0 BALANCE");
                sb9.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb9.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb9.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb9.Append("               VENDOR_SERVICES_TRN VS,");
                sb9.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb9.Append("               TRANSPORT_TRN_COST JTFC,");
                sb9.Append("               INV_SUPPLIER_TBL     IST,");
                sb9.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb9.Append("               LOCATION_MST_TBL     LMT");
                sb9.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb9.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb9.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb9.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb9.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb9.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb9.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb9.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");

                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb9.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb9.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb9.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb9.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb9.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb9.Append("           AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb9.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb9.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb9.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb9.Append("                  LMT.LOCATION_MST_PK,");
                sb9.Append("                  IST.INVOICE_DATE,");
                sb9.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb9.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");

                sb10.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb10.Append("               LMT.LOCATION_MST_PK,");
                sb10.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb10.Append("               'PAYMENT' TRANSACTION,");
                sb10.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK=JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) SHIPMENT_REF_NR,");
                sb10.Append("               PMT.PAYMENT_REF_NO,");
                sb10.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb10.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb10.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb10.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb10.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb10.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb10.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb10.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb10.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb10.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb10.Append("               0 BALANCE");
                sb10.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb10.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb10.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb10.Append("               VENDOR_SERVICES_TRN VS,");
                sb10.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb10.Append("               TRANSPORT_TRN_COST JTFC,");
                sb10.Append("               INV_SUPPLIER_TBL     IST,");
                sb10.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb10.Append("               PAYMENTS_TBL         PMT, ");
                sb10.Append("               PAYMENT_TRN_TBL      PTT, ");
                sb10.Append("               LOCATION_MST_TBL     LMT");
                sb10.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb10.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb10.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb10.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb10.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb10.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb10.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb10.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb10.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb10.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");

                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb10.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb10.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb10.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb10.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb10.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb10.Append("           AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb10.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb10.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb10.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb10.Append("                  LMT.LOCATION_MST_PK,");
                sb10.Append("                  PMT.PAYMENT_DATE,");
                sb10.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb10.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");

                sb11.Append("  SELECT VMT.VENDOR_MST_PK,");
                sb11.Append("               LMT.LOCATION_MST_PK,");
                sb11.Append("               IST.INVOICE_DATE REF_DATE,");
                sb11.Append("               'V. INVOICE' TRANSACTION,");
                sb11.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK=JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) SHIPMENT_REF_NR,");
                sb11.Append("               IST.INVOICE_REF_NO,");
                sb11.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb11.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb11.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb11.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb11.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb11.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb11.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb11.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb11.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb11.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb11.Append("               0 BALANCE");
                sb11.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb11.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb11.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb11.Append("               VENDOR_SERVICES_TRN VS,");
                sb11.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb11.Append("               TRANSPORT_TRN_COST JTFC,");
                sb11.Append("               INV_SUPPLIER_TBL     IST,");
                sb11.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb11.Append("               LOCATION_MST_TBL     LMT");
                sb11.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb11.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb11.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb11.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb11.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb11.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb11.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb11.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb11.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb11.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb11.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb11.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb11.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb11.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb11.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb11.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb11.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb11.Append("                  LMT.LOCATION_MST_PK,");
                sb11.Append("                  IST.INVOICE_DATE,");
                sb11.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb11.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");

                sb12.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb12.Append("               LMT.LOCATION_MST_PK,");
                sb12.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb12.Append("               'PAYMENT' TRANSACTION,");
                sb12.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK=JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
                sb12.Append("               PMT.PAYMENT_REF_NO,");
                sb12.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb12.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb12.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb12.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb12.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb12.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb12.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb12.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb12.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb12.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb12.Append("               0 BALANCE");
                sb12.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb12.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb12.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb12.Append("               VENDOR_SERVICES_TRN VS,");
                sb12.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb12.Append("               TRANSPORT_TRN_COST JTFC,");
                sb12.Append("               INV_SUPPLIER_TBL     IST,");
                sb12.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb12.Append("               PAYMENTS_TBL         PMT,");
                sb12.Append("               PAYMENT_TRN_TBL      PTT,");
                sb12.Append("               LOCATION_MST_TBL     LMT");
                sb12.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb12.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb12.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb12.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb12.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb12.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb12.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb12.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb12.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb12.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb12.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb12.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb12.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb12.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb12.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb12.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb12.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb12.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb12.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb12.Append("                  LMT.LOCATION_MST_PK,");
                sb12.Append("                  PMT.PAYMENT_DATE,");
                sb12.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb12.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");

                ///''''''''''''''''''''''''''''''''''''''''''''''''''''
                sb.Append("SELECT DISTINCT GROUPID");
                sb.Append("  FROM (SELECT DISTINCT LOCATION_MST_PK, LOCATION_ID, BALANCE, GROUPID");
                sb.Append("          FROM (SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("                       LMT.LOCATION_MST_PK,");
                sb.Append("                       TO_CHAR(IST.INVOICE_DATE) REF_DATE,");
                sb.Append("                       'V. INVOICE' TRANSACTION,");
                //sb.Append("                       HBL.HBL_REF_NO,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END HBL_REF_NO,");
                sb.Append("                       IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("                       0 BALANCE,");
                sb.Append("                       LMT.LOCATION_ID,");
                sb.Append("                       '' GROUPID");
                sb.Append("                  FROM VENDOR_MST_TBL       VMT,");
                sb.Append("                       VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("                       VENDOR_TYPE_MST_TBL VT,");
                sb.Append("                       VENDOR_SERVICES_TRN VS,");
                sb.Append("                       JOB_CARD_TRN JCSE,");
                sb.Append("                       JOB_TRN_COST JTFC,");
                sb.Append("                       INV_SUPPLIER_TBL     IST,");
                sb.Append("                       INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("                       HBL_EXP_TBL          HBL,");
                sb.Append("                       HAWB_EXP_TBL         HAWB,");
                sb.Append("                       LOCATION_MST_TBL     LMT");
                sb.Append("                 WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("                   AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("                   AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("                   AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("                   AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("                   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("                   AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("                   AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("                   AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("                   AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("                   AND IST.PROCESS_TYPE = 1")
                //sb.Append("                   AND IST.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("                 GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                          LMT.LOCATION_MST_PK,");
                sb.Append("                          IST.INVOICE_DATE,");
                sb.Append("                          HBL.HBL_REF_NO, HAWB.HAWB_REF_NO,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                          IST.INVOICE_REF_NO,");
                sb.Append("                          LOCATION_ID,IST.INV_SUPPLIER_PK");
                ///'''''''''''''''
                sb.Append("                UNION ");
                sb.Append("       SELECT VMT.VENDOR_MST_PK,");
                sb.Append("                       LMT.LOCATION_MST_PK,");
                sb.Append("                       TO_CHAR(IST.INVOICE_DATE) REF_DATE,");
                sb.Append("                       'V. INVOICE' TRANSACTION,");
                sb.Append("                       JCSE.HBL_NO HBL_REF_NO,");
                sb.Append("                       IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("                       0 BALANCE,");
                sb.Append("                       LMT.LOCATION_ID,");
                sb.Append("                       '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               CBJC_TBL JCSE,");
                sb.Append("               CBJC_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  JCSE.HBL_NO,");
                sb.Append("                  IST.INVOICE_REF_NO,");
                sb.Append("                          LOCATION_ID,IST.INV_SUPPLIER_PK");
                ///''''''''''''
                sb.Append("                UNION ");
                sb.Append("       SELECT VMT.VENDOR_MST_PK,");
                sb.Append("                       LMT.LOCATION_MST_PK,");
                sb.Append("                       TO_CHAR(IST.INVOICE_DATE) REF_DATE,");
                sb.Append("                       'V. INVOICE' TRANSACTION,");
                sb.Append("                       (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
                sb.Append("                       IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("                       0 BALANCE,");
                sb.Append("                       LMT.LOCATION_ID,");
                sb.Append("                       '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb.Append("               TRANSPORT_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb.Append("                  IST.INVOICE_REF_NO,");
                sb.Append("                          LOCATION_ID,IST.INV_SUPPLIER_PK");
                ///'''''''''''''

                sb.Append("                UNION");
                sb.Append("                SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("                       LMT.LOCATION_MST_PK,");
                sb.Append("                       TO_CHAR(PMT.PAYMENT_DATE) REF_DATE,");
                sb.Append("                       'PAYMENT' TRANSACTION,");
                //sb.Append("                       HBL.HBL_REF_NO,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END HBL_REF_NO, ");
                sb.Append("                       PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("                       0 BALANCE,");
                sb.Append("                       LMT.LOCATION_ID,");
                sb.Append("                       '' GROUPID");
                sb.Append("                  FROM VENDOR_MST_TBL       VMT,");
                sb.Append("                       VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("                       VENDOR_TYPE_MST_TBL VT,");
                sb.Append("                       VENDOR_SERVICES_TRN VS,");
                sb.Append("                       JOB_CARD_TRN JCSE,");
                sb.Append("                       JOB_TRN_COST JTFC,");
                sb.Append("                       INV_SUPPLIER_TBL     IST,");
                sb.Append("                       INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("                       PAYMENTS_TBL         PMT,");
                sb.Append("                       PAYMENT_TRN_TBL      PTT,");
                sb.Append("                       HBL_EXP_TBL          HBL,");
                sb.Append("                       HAWB_EXP_TBL         HAWB,");
                sb.Append("                       LOCATION_MST_TBL     LMT");
                sb.Append("                 WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("                   AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("                   AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("                   AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("                   AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("                   AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("                   AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("                   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("                   AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("                   AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("                   AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("                   AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("                   AND PMT.PROCESS_TYPE = 1")
                //sb.Append("                   AND PMT.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("                 GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                          LMT.LOCATION_MST_PK,");
                sb.Append("                          PMT.PAYMENT_DATE,");
                sb.Append("                          HBL.HBL_REF_NO,HAWB.HAWB_REF_NO,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                          PMT.PAYMENT_REF_NO,");
                sb.Append("                          LOCATION_ID,PMT.PAYMENT_TBL_PK");
                sb.Append("                UNION ");
                ///''''''''''''
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                sb.Append("                       LMT.LOCATION_MST_PK,");
                sb.Append("                       TO_CHAR(PMT.PAYMENT_DATE) REF_DATE,");
                sb.Append("                       'PAYMENT' TRANSACTION,");
                sb.Append("                       JCSE.HBL_NO HBL_REF_NO,");
                sb.Append("                       PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("                       0 BALANCE,");
                sb.Append("                       LMT.LOCATION_ID,");
                sb.Append("                       '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               CBJC_TBL JCSE,");
                sb.Append("               CBJC_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  JCSE.HBL_NO,");
                sb.Append("                  PMT.PAYMENT_REF_NO,");
                sb.Append("                          LOCATION_ID,PMT.PAYMENT_TBL_PK");
                sb.Append("                UNION ");
                ///''''''''''''
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                sb.Append("                       LMT.LOCATION_MST_PK,");
                sb.Append("                       TO_CHAR(PMT.PAYMENT_DATE) REF_DATE,");
                sb.Append("                       'PAYMENT' TRANSACTION,");
                sb.Append("                       (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
                sb.Append("                       PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("                       0 BALANCE,");
                sb.Append("                       LMT.LOCATION_ID,");
                sb.Append("                       '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb.Append("               TRANSPORT_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb.Append("                  PMT.PAYMENT_REF_NO,");
                sb.Append("                          LOCATION_ID,PMT.PAYMENT_TBL_PK");
                ///'''''''''''''
                sb.Append(" ))");
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "GROUP");
                ///''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                ///''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                sb.Remove(0, sb.Length);
                //'LOCATION
                sb.Append("SELECT DISTINCT LOCATION_MST_PK, LOCATION_ID, BALANCE, GROUPID");
                //'SEA EXPORT
                sb.Append("  FROM (SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(IST.INVOICE_DATE) REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END HBL_REF_NO, ");
                sb.Append("               IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND IST.PROCESS_TYPE = 1")
                //sb.Append("           AND IST.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  IST.INVOICE_REF_NO,");
                sb.Append("                  LOCATION_ID,IST.INV_SUPPLIER_PK");
                ///'''''''
                sb.Append(" UNION ");
                sb.Append("       SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(IST.INVOICE_DATE) REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                sb.Append("               JCSE.HBL_NO HBL_REF_NO,");
                sb.Append("               IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               CBJC_TBL JCSE,");
                sb.Append("               CBJC_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  JCSE.HBL_NO,");
                sb.Append("                  IST.INVOICE_REF_NO,");
                sb.Append("                  LOCATION_ID,IST.INV_SUPPLIER_PK");
                ///'''''''
                sb.Append(" UNION ");
                sb.Append("       SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(IST.INVOICE_DATE) REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                sb.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
                sb.Append("               IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb.Append("               TRANSPORT_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb.Append("                  IST.INVOICE_REF_NO,");
                sb.Append("                  LOCATION_ID,IST.INV_SUPPLIER_PK");
                ///'''''''

                sb.Append("        UNION");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(PMT.PAYMENT_DATE) REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END HBL_REF_NO, ");
                sb.Append("               PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND PMT.PROCESS_TYPE = 1")
                //sb.Append("           AND PMT.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  PMT.PAYMENT_REF_NO,");
                sb.Append("                  LOCATION_ID,PMT.PAYMENT_TBL_PK");
                ///'''''''''''''
                sb.Append("        UNION ");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(PMT.PAYMENT_DATE) REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                sb.Append("               JCSE.HBL_NO HBL_REF_NO,");
                sb.Append("               PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               CBJC_TBL JCSE,");
                sb.Append("               CBJC_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  JCSE.HBL_NO,");
                sb.Append("                  PMT.PAYMENT_REF_NO,");
                sb.Append("                  LOCATION_ID,PMT.PAYMENT_TBL_PK");
                ///'''''''''''''
                sb.Append("        UNION ");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(PMT.PAYMENT_DATE) REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                sb.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
                sb.Append("               PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               '' GROUPID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb.Append("               TRANSPORT_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb.Append("                  PMT.PAYMENT_REF_NO,");
                sb.Append("                  LOCATION_ID,PMT.PAYMENT_TBL_PK");
                ///'''''''''''''
                sb.Append(" )  ");
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "LOCATION");

                ///''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                ///''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                sb.Remove(0, sb.Length);
                //'VENDOR
                sb.Append("SELECT DISTINCT LOCATION_MST_PK,VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("                VENDOR_NAME,");
                sb.Append("                '' VENDOR_ID,");
                sb.Append("                BALANCE");
                sb.Append("  FROM (SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(IST.INVOICE_DATE) REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               VMT.VENDOR_ID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND IST.PROCESS_TYPE = 1")
                //sb.Append("           AND IST.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO,JCSE.HBL_HAWB_REF_NO , JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  IST.INVOICE_REF_NO,");
                sb.Append("                  VENDOR_NAME,");
                sb.Append("                  VMT.VENDOR_ID,IST.INV_SUPPLIER_PK");
                ///'''''
                sb.Append("        UNION ");
                sb.Append("       SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(IST.INVOICE_DATE) REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                sb.Append("               JCSE.HBL_NO SHIPMENT_REF_NR,");
                sb.Append("               IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               VMT.VENDOR_ID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               CBJC_TBL JCSE,");
                sb.Append("               CBJC_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  JCSE.HBL_NO,");
                sb.Append("                  IST.INVOICE_REF_NO,");
                sb.Append("                  VENDOR_NAME,");
                sb.Append("                  VMT.VENDOR_ID,IST.INV_SUPPLIER_PK");
                ///'''''
                sb.Append("        UNION ");
                sb.Append("       SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(IST.INVOICE_DATE) REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                sb.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) SHIPMENT_REF_NR,");
                sb.Append("               IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               VMT.VENDOR_ID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb.Append("               TRANSPORT_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb.Append("                  IST.INVOICE_REF_NO,");
                sb.Append("                  VENDOR_NAME,");
                sb.Append("                  VMT.VENDOR_ID,IST.INV_SUPPLIER_PK");
                ///'''

                sb.Append("        UNION");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(PMT.PAYMENT_DATE) REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               VMT.VENDOR_ID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND PMT.PROCESS_TYPE = 1")
                //sb.Append("           AND PMT.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO,JCSE.HBL_HAWB_REF_NO , JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  PMT.PAYMENT_REF_NO,");
                sb.Append("                  VENDOR_NAME,");
                sb.Append("                  VMT.VENDOR_ID,PMT.PAYMENT_TBL_PK");
                sb.Append("        UNION");
                ///'''''''
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(PMT.PAYMENT_DATE) REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                sb.Append("               JCSE.HBL_NO SHIPMENT_REF_NR,");
                sb.Append("               PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               VMT.VENDOR_ID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               CBJC_TBL JCSE,");
                sb.Append("               CBJC_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.CBJC_PK = JTFC.CBJC_FK");
                sb.Append("           AND JTFC.CBJC_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  JCSE.HBL_NO,");
                sb.Append("                  PMT.PAYMENT_REF_NO,");
                sb.Append("                  VENDOR_NAME,");
                sb.Append("                  VMT.VENDOR_ID,PMT.PAYMENT_TBL_PK");
                sb.Append("        UNION");
                ///'''''''
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               TO_CHAR(PMT.PAYMENT_DATE) REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                sb.Append("               (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = JCSE.TRANSPORT_INST_SEA_PK AND ROWNUM=1) SHIPMENT_REF_NR,");
                sb.Append("               PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               VMT.VENDOR_ID");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               TRANSPORT_INST_SEA_TBL JCSE,");
                sb.Append("               TRANSPORT_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.TRANSPORT_INST_SEA_PK = JTFC.TRANSPORT_INST_FK");
                sb.Append("           AND JTFC.TRANSPORT_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  JCSE.TRANSPORT_INST_SEA_PK,");
                sb.Append("                  PMT.PAYMENT_REF_NO,");
                sb.Append("                  VENDOR_NAME,");
                sb.Append("                  VMT.VENDOR_ID,PMT.PAYMENT_TBL_PK");
                ///'''''''
                sb.Append(" )  ");
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "VENDOR");

                ///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                ///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                sb.Remove(0, sb.Length);

                //'TRANSACTION
                sb.Append("SELECT DISTINCT V.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("                0 LOCATION_MST_PK,");
                sb.Append("                TO_DATE('" + Fromdate + "','dd/MM/yyyy HH24:Mi:ss')  REF_DATE,");
                sb.Append("                'OPENING BALANCE',");
                sb.Append("                '' SHIPMENT_REF_NR,");
                sb.Append("                '',");
                sb.Append("                 SUM(B.CREDIT - B.DEBIT) DEBIT,");
                sb.Append("                 0 CREDIT,");
                sb.Append("                 SUM(B.CREDIT - B.DEBIT) BALANCE");
                sb.Append("  FROM VENDOR_MST_TBL V,");
                sb.Append("       (SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               IST.INVOICE_DATE REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND IST.PROCESS_TYPE = 1")
                //sb.Append("           AND IST.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  HBL.HBL_REF_NO, HAWB.HAWB_REF_NO, JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");
                sb.Append("        UNION ");
                sb.Append("        " + sb1.ToString() + "");
                sb.Append("        UNION ");
                sb.Append("        " + sb7.ToString() + "");
                ///''''''''''
                sb.Append("        UNION ");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND PMT.PROCESS_TYPE = 1")
                //sb.Append("           AND PMT.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO , JCSE.HBL_HAWB_REF_NO , JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");
                sb.Append("        UNION ");
                sb.Append("        " + sb2.ToString() + "");
                sb.Append("        UNION ");
                sb.Append("        " + sb8.ToString() + "");
                sb.Append(" ) A, ");
                ///''''''
                sb.Append("       (SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               IST.INVOICE_DATE REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND IST.PROCESS_TYPE = 1")
                //sb.Append("           AND IST.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("           AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  HBL.HBL_REF_NO, HAWB.HAWB_REF_NO, JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");
                sb.Append("        UNION ");
                sb.Append("        " + sb3.ToString() + "");
                sb.Append("        UNION ");
                sb.Append("        " + sb9.ToString() + "");

                sb.Append("        UNION ");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT, ");
                sb.Append("               PAYMENT_TRN_TBL      PTT, ");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("                HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK =  HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND PMT.PROCESS_TYPE = 1")
                //sb.Append("           AND PMT.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("           AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  HBL.HBL_REF_NO, HAWB.HAWB_REF_NO, JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");
                sb.Append("        UNION ");
                sb.Append("        " + sb4.ToString() + "");
                sb.Append("        UNION ");
                sb.Append("        " + sb10.ToString() + "");
                ///''
                sb.Append(" ) B ");
                sb.Append(" WHERE V.VENDOR_MST_PK = A.VENDOR_MST_PK");
                sb.Append("   AND A.VENDOR_MST_PK = B.VENDOR_MST_PK(+)");
                sb.Append("   GROUP BY V.VENDOR_MST_PK");
                sb.Append("  UNION ");
                //'SEA EXPORT

                sb.Append("SELECT VENDOR_MST_PK,");
                sb.Append("       LOCATION_MST_PK,");
                sb.Append("       REF_DATE,");
                sb.Append("       TRANSACTION,");
                sb.Append("       SHIPMENT_REF_NR,");
                sb.Append("       INVOICE_REF_NO,");
                sb.Append("       DEBIT,");
                sb.Append("       CREDIT,");
                sb.Append("       BALANCE");
                sb.Append("       FROM");
                sb.Append("  (SELECT VENDOR_MST_PK,");
                sb.Append("       LOCATION_MST_PK,");
                sb.Append("       REF_DATE,");
                sb.Append("       TRANSACTION,");
                sb.Append("       SHIPMENT_REF_NR,");
                sb.Append("       INVOICE_REF_NO,");
                sb.Append("       DEBIT,");
                sb.Append("       CREDIT,");
                sb.Append("       BALANCE");
                sb.Append("  FROM (SELECT VMT.VENDOR_MST_PK,");
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               IST.INVOICE_DATE REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               IST.INVOICE_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK =  HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND IST.PROCESS_TYPE = 1")
                //sb.Append("           AND IST.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  HBL.HBL_REF_NO, HAWB.HAWB_REF_NO,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");
                sb.Append("        UNION ");
                sb.Append("        " + sb5.ToString() + "");
                sb.Append("        UNION ");
                sb.Append("        " + sb11.ToString() + "");
                ///''''
                sb.Append("        UNION");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               PMT.PAYMENT_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL VT,");
                sb.Append("               VENDOR_SERVICES_TRN VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK =  HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND PMT.PROCESS_TYPE = 1")
                //sb.Append("           AND PMT.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  HBL.HBL_REF_NO, HAWB.HAWB_REF_NO ,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");
                sb.Append("        UNION ");
                sb.Append("        " + sb6.ToString() + "");
                sb.Append("        UNION ");
                sb.Append("        " + sb12.ToString() + "");
                ///''''''
                sb.Append("       )");
                sb.Append("                 ORDER BY  REF_DATE )");

                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "VENDETAILS");

                DataRelation relLocGroup_Details = new DataRelation("LOCGROUP", new DataColumn[] { MainDS.Tables[0].Columns["GROUPID"] }, new DataColumn[] { MainDS.Tables[1].Columns["GROUPID"] });
                DataRelation relLocVendor_Details = new DataRelation("LOCVENDOR", new DataColumn[] { MainDS.Tables[1].Columns["LOCATION_MST_PK"] }, new DataColumn[] { MainDS.Tables[2].Columns["LOCATION_MST_PK"] });
                DataRelation relVendor_Details = new DataRelation("VENDORDETAILS", new DataColumn[] { MainDS.Tables[2].Columns["VENDOR_MST_PK"] }, new DataColumn[] { MainDS.Tables[3].Columns["VENDOR_MST_PK"] });

                relLocGroup_Details.Nested = true;
                relLocVendor_Details.Nested = true;
                relVendor_Details.Nested = true;
                MainDS.Relations.Add(relLocGroup_Details);
                MainDS.Relations.Add(relLocVendor_Details);
                MainDS.Relations.Add(relVendor_Details);
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
                throw;
            }
        }
        #endregion

        #region "Report Information"
        public DataSet FetchReportInformation(string Fromdate = "", string Todate = "", string CountryPK = "", string LocPK = "", string Vendor = "", string VendorType = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            int LogLocPk = Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            try
            {
                sb.Append("SELECT DISTINCT V.VENDOR_MST_PK,");
                sb.Append("                L.LOCATION_MST_PK,");
                sb.Append("                V.VENDOR_ID,");
                sb.Append("                V.VENDOR_NAME,");
                sb.Append("                L.LOCATION_ID,");
                sb.Append("                L.LOCATION_NAME,");
                sb.Append("                TO_DATE('" + Fromdate + "') REF_DATE,");
                sb.Append("                'OPENING BALANCE' OPENING_BALANCE,");
                sb.Append("                '' SHIPMENT_REF_NR,");
                sb.Append("                '' DOC_REF_NO,");
                sb.Append("                NVL(SUM(B.CREDIT - B.DEBIT),0) DEBIT,");
                sb.Append("                0 CREDIT,");
                sb.Append("                NVL(SUM(B.CREDIT - B.DEBIT),0) BALANCE");
                sb.Append("  FROM VENDOR_MST_TBL V,");
                sb.Append("       LOCATION_MST_TBL L,");
                sb.Append("       (SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               VMT.VENDOR_ID,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               LMT.LOCATION_NAME,");
                sb.Append("               IST.INVOICE_DATE REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               IST.INVOICE_REF_NO DOC_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL  VT,");
                sb.Append("               VENDOR_SERVICES_TRN  VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND IST.PROCESS_TYPE = 1")
                //sb.Append("           AND IST.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  VMT.VENDOR_ID,");
                sb.Append("                  VMT.VENDOR_NAME,");
                sb.Append("                  LMT.LOCATION_ID,");
                sb.Append("                  LMT.LOCATION_NAME,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO ,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");
                ///''
                sb.Append("        UNION");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               VMT.VENDOR_ID,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               LMT.LOCATION_NAME,");
                sb.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               PMT.PAYMENT_REF_NO DOC_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL  VT,");
                sb.Append("               VENDOR_SERVICES_TRN  VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND PMT.PROCESS_TYPE = 1")
                //sb.Append("           AND PMT.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN ");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  VMT.VENDOR_ID,");
                sb.Append("                  VMT.VENDOR_NAME,");
                sb.Append("                  LMT.LOCATION_ID,");
                sb.Append("                  LMT.LOCATION_NAME,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO ,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");
                ///
                sb.Append("  ) A, ");
                sb.Append("       (SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               VMT.VENDOR_ID,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               LMT.LOCATION_NAME,");
                sb.Append("               IST.INVOICE_DATE REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               IST.INVOICE_REF_NO DOC_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL  VT,");
                sb.Append("               VENDOR_SERVICES_TRN  VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK =  HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND IST.PROCESS_TYPE = 1")
                //sb.Append("           AND IST.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("           AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  VMT.VENDOR_ID,");
                sb.Append("                  VMT.VENDOR_NAME,");
                sb.Append("                  LMT.LOCATION_ID,");
                sb.Append("                  LMT.LOCATION_NAME,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO ,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");
                ///'
                sb.Append("        UNION");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               VMT.VENDOR_ID,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               LMT.LOCATION_NAME,");
                sb.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               PMT.PAYMENT_REF_NO DOC_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL  VT,");
                sb.Append("               VENDOR_SERVICES_TRN  VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND PMT.PROCESS_TYPE = 1")
                //sb.Append("           AND PMT.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("           AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  VMT.VENDOR_ID,");
                sb.Append("                  VMT.VENDOR_NAME,");
                sb.Append("                  LMT.LOCATION_ID,");
                sb.Append("                  LMT.LOCATION_NAME,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO ,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");
                ///'
                sb.Append("   ) B ");
                sb.Append("   WHERE V.VENDOR_MST_PK = A.VENDOR_MST_PK");
                sb.Append("   AND A.VENDOR_MST_PK = B.VENDOR_MST_PK(+)");
                sb.Append("   AND L.LOCATION_MST_PK = A.LOCATION_MST_PK");
                //sb.Append("   AND L.LOCATION_MST_PK = B.LOCATION_MST_PK")
                sb.Append("   GROUP BY V.VENDOR_MST_PK,");
                sb.Append("          L.LOCATION_MST_PK,");
                sb.Append("          V.VENDOR_ID,");
                sb.Append("          V.VENDOR_NAME,");
                sb.Append("          L.LOCATION_ID,");
                sb.Append("          L.LOCATION_NAME");
                sb.Append("   UNION  ");
                ///

                sb.Append(" SELECT VENDOR_MST_PK,");
                sb.Append("               LOCATION_MST_PK,");
                sb.Append("               VENDOR_ID,");
                sb.Append("               VENDOR_NAME,");
                sb.Append("               LOCATION_ID,");
                sb.Append("               LOCATION_NAME,");
                sb.Append("               REF_DATE,");
                sb.Append("               TRANSACTION,");
                sb.Append("               SHIPMENT_REF_NR,");
                sb.Append("               DOC_REF_NO,");
                sb.Append("               DEBIT,");
                sb.Append("               CREDIT,");
                sb.Append("               BALANCE");
                sb.Append("   FROM (SELECT VENDOR_MST_PK,");
                sb.Append("       LOCATION_MST_PK,");
                sb.Append("       VENDOR_ID,");
                sb.Append("       VENDOR_NAME,");
                sb.Append("       LOCATION_ID,");
                sb.Append("       LOCATION_NAME,");
                sb.Append("       REF_DATE,");
                sb.Append("       TRANSACTION,");
                sb.Append("       SHIPMENT_REF_NR,");
                sb.Append("       DOC_REF_NO,");
                sb.Append("       DEBIT,");
                sb.Append("       CREDIT,");
                sb.Append("       BALANCE");
                sb.Append("  FROM (SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               VMT.VENDOR_ID,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               LMT.LOCATION_NAME,");
                sb.Append("               IST.INVOICE_DATE REF_DATE,");
                sb.Append("               'V. INVOICE' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO SHIPMENT_REF_NR,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               IST.INVOICE_REF_NO DOC_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED = 3),0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT ISTT1.TOTAL_COST * get_ex_rate_buy(IST1.CURRENCY_MST_FK," + BaseCurrFk + ", IST1.INVOICE_DATE))");
                sb.Append("               FROM INV_SUPPLIER_TRN_TBL ISTT1, INV_SUPPLIER_TBL IST1");
                sb.Append("                WHERE ISTT1.INV_SUPPLIER_TBL_FK = IST1.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.INV_SUPPLIER_PK = IST.INV_SUPPLIER_PK");
                sb.Append("                AND IST1.APPROVED <> 3),0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL  VT,");
                sb.Append("               VENDOR_SERVICES_TRN  VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND IST.PROCESS_TYPE = 1")
                //sb.Append("           AND IST.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(IST.INVOICE_DATE,DATEFORMAT) BETWEEN");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  VMT.VENDOR_ID,");
                sb.Append("                  VMT.VENDOR_NAME,");
                sb.Append("                  LMT.LOCATION_ID,");
                sb.Append("                  LMT.LOCATION_NAME,");
                sb.Append("                  IST.INVOICE_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO ,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  IST.INVOICE_REF_NO,IST.INV_SUPPLIER_PK");
                ///'
                sb.Append("        UNION");
                sb.Append("        SELECT VMT.VENDOR_MST_PK,");
                //'SEA EXPORT
                sb.Append("               LMT.LOCATION_MST_PK,");
                sb.Append("               VMT.VENDOR_ID,");
                sb.Append("               VMT.VENDOR_NAME,");
                sb.Append("               LMT.LOCATION_ID,");
                sb.Append("               LMT.LOCATION_NAME,");
                sb.Append("               PMT.PAYMENT_DATE REF_DATE,");
                sb.Append("               'PAYMENT' TRANSACTION,");
                //sb.Append("               HBL.HBL_REF_NO,")
                sb.Append("            CASE ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 2 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("                HBL.HBL_REF_NO ");
                sb.Append("              WHEN JCSE.BUSINESS_TYPE = 1 AND JCSE.PROCESS_TYPE = 1 THEN ");
                sb.Append("               HAWB.HAWB_REF_NO ");
                sb.Append("              ELSE ");
                sb.Append("               JCSE.HBL_HAWB_REF_NO ");
                sb.Append("                 END SHIPMENT_REF_NR, ");
                sb.Append("               PMT.PAYMENT_REF_NO DOC_REF_NO,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED <> 3), 0) DEBIT,");
                sb.Append("               NVL((SELECT SUM(DISTINCT PTT1.PAID_AMOUNT_HDR_CURR * get_ex_rate_buy(PMT1.CURRENCY_MST_FK," + BaseCurrFk + ",PMT1.PAYMENT_DATE))");
                sb.Append("               FROM PAYMENT_TRN_TBL PTT1, PAYMENTS_TBL PMT1");
                sb.Append("               WHERE PTT1.PAYMENTS_TBL_FK = PMT.PAYMENT_TBL_PK");
                sb.Append("               AND PMT1.PAYMENT_TBL_PK = PMT.PAYMENT_TBL_PK     ");
                sb.Append("               AND PMT1.APPROVED = 3), 0) CREDIT,");
                sb.Append("               0 BALANCE");
                sb.Append("          FROM VENDOR_MST_TBL       VMT,");
                sb.Append("               VENDOR_CONTACT_DTLS  VCD,");
                sb.Append("               VENDOR_TYPE_MST_TBL  VT,");
                sb.Append("               VENDOR_SERVICES_TRN  VS,");
                sb.Append("               JOB_CARD_TRN JCSE,");
                sb.Append("               JOB_TRN_COST JTFC,");
                sb.Append("               INV_SUPPLIER_TBL     IST,");
                sb.Append("               INV_SUPPLIER_TRN_TBL ISTT,");
                sb.Append("               PAYMENTS_TBL         PMT,");
                sb.Append("               PAYMENT_TRN_TBL      PTT,");
                sb.Append("               HBL_EXP_TBL          HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               LOCATION_MST_TBL     LMT");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JTFC.JOB_CARD_TRN_FK");
                sb.Append("           AND JTFC.JOB_TRN_COST_PK = ISTT.JOB_TRN_EST_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND IST.INV_SUPPLIER_PK = PTT.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND PMT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = IST.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("           AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("           AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                sb.Append("           AND VCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append("           AND JCSE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                //sb.Append("           AND PMT.PROCESS_TYPE = 1")
                //sb.Append("           AND PMT.BUSINESS_TYPE = 2")
                if (!string.IsNullOrEmpty(Vendor))
                {
                    sb.Append("               AND VMT.VENDOR_MST_PK IN( " + Vendor + ")");
                }
                if (!string.IsNullOrEmpty(VendorType))
                {
                    sb.Append("               AND VT.VENDOR_TYPE_PK  IN(" + VendorType + ")");
                }
                if (!string.IsNullOrEmpty(LocPK))
                {
                    sb.Append("               AND LMT.LOCATION_MST_PK IN(" + LocPK + ")");
                }
                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("               AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
                }
                sb.Append("    AND lmt.location_mst_pk=" + LogLocPk);
                sb.Append("                   AND TO_DATE(PMT.PAYMENT_DATE,DATEFORMAT) BETWEEN ");
                sb.Append("                       TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
                sb.Append("                       TO_DATE('" + Todate + "', '" + dateFormat + "')");
                sb.Append("         GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  LMT.LOCATION_MST_PK,");
                sb.Append("                  VMT.VENDOR_ID,");
                sb.Append("                  VMT.VENDOR_NAME,");
                sb.Append("                  LMT.LOCATION_ID,");
                sb.Append("                  LMT.LOCATION_NAME,");
                sb.Append("                  PMT.PAYMENT_DATE,");
                sb.Append("                  HBL.HBL_REF_NO,HAWB.HAWB_REF_NO ,JCSE.HBL_HAWB_REF_NO, JCSE.BUSINESS_TYPE,JCSE.PROCESS_TYPE,");
                sb.Append("                  PMT.PAYMENT_REF_NO,PMT.PAYMENT_TBL_PK");
                sb.Append(" ) ");
                sb.Append("       ORDER BY REF_DATE)");
                //sb.Append("      ORDER BY SHIPMENT_REF_NR DESC")
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
                throw;
            }
        }
        #endregion

        #region "Message Details"
        public DataSet FetchMessageDelais(string VendorPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT VMT.VENDOR_MST_PK, VMT.VENDOR_ID, VMT.VENDOR_NAME, VCD.BILL_EMAIL_ID");
                sb.Append("  FROM VENDOR_MST_TBL VMT, VENDOR_CONTACT_DTLS VCD");
                sb.Append(" WHERE VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("   AND VMT.VENDOR_MST_PK=" + VendorPK + "");
                sb.Append("   AND VMT.ACTIVE = 1");
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
                throw;
            }
        }
        #endregion

        #region "Enhance Search"
        public string FetchNewVendor(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            dynamic strNull = DBNull.Value;
            string SEARCH_FLAG_IN = "";
            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 3)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VENDOR_PKG.GETVENDOR_NEW";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        #endregion
    }
}