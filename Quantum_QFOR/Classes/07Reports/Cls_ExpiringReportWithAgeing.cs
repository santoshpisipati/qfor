#region "Comments"

//'***************************************************************************************************************
//'*  Company Name:
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By  :    Santosh on 31-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified DateTime(DD-MON-YYYY)  Modified By     Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_ExpiringReportWithAgeing : CommonFeatures
    {
        #region "Fetch Expiry Report Parent Grid Function"

        /// <summary>
        /// Fetches the parent.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="ContractOf">The contract of.</param>
        /// <param name="ContractType">Type of the contract.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="RefPk">The reference pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="SupplierPk">The supplier pk.</param>
        /// <param name="ExpiryDt">The expiry dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <param name="printPDF">The print PDF.</param>
        /// <returns></returns>
        public DataSet FetchParent(Int32 Business = 0, Int32 ContractOf = 0, Int32 ContractType = 0, Int32 LocFk = 0, string RefPk = "", string CustPk = "", string SupplierPk = "", string ExpiryDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 expExcel = 0, Int32 printPDF = 0)
        {
            DataSet functionReturnValue = null;

            WorkFlow objWF = new WorkFlow();
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            Int32 TotalRecords = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                //Sea/Supplier's/SL Contract
                if (Business == 2 & ContractOf == 1 & ContractType == 1)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CMST.CONT_MAIN_SEA_PK MAIN_PK,");
                    sb.Append("               CMST.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'SL Contract' CONTRACT_TYPE,");
                    sb.Append("               OMT.OPERATOR_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(CMST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CMST.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CMST.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CMST.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE");
                    sb.Append("          FROM CONT_MAIN_SEA_TBL CMST,");
                    sb.Append("               OPERATOR_MST_TBL  OMT,");
                    sb.Append("               USER_MST_TBL      UMT");
                    sb.Append("         WHERE OMT.OPERATOR_MST_PK = CMST.OPERATOR_MST_FK");
                    sb.Append("           AND CMST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND OMT.OPERATOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMST.CONT_MAIN_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND CMST.CONT_APPROVED = 1) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Sea/Supplier's/Warehouse Contract
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 2)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append("     COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CMDT.CONT_MAIN_DEPOT_PK MAIN_PK,");
                    sb.Append("               CMDT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Warehouse Contract' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(CMDT.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CMDT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CMDT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CMDT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM CONT_MAIN_DEPOT_TBL CMDT,");
                    sb.Append("               USER_MST_TBL        UMT,");
                    sb.Append("               VENDOR_MST_TBL      VMT");
                    sb.Append("         WHERE CMDT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ");
                    sb.Append("           AND CMDT.BUSINESS_TYPE = 2");
                    sb.Append("           AND CMDT.CONT_APPROVED = 1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMDT.CONT_MAIN_DEPOT_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMDT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMDT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND VMT.VENDOR_MST_PK = CMDT.DEPOT_MST_FK) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Sea/Supplier's/Transport Contract
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 3)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CMDT.CONT_MAIN_TRANS_PK MAIN_PK,");
                    sb.Append("               CMDT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Transport Contract' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               'LCL' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CMDT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CMDT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CMDT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM CONT_MAIN_TRANS_TBL CMDT,");
                    sb.Append("               USER_MST_TBL        UMT,");
                    sb.Append("               VENDOR_MST_TBL      VMT");
                    sb.Append("         WHERE CMDT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("         AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append("           AND CMDT.BUSINESS_TYPE = 2");
                    sb.Append("           AND CMDT.CONT_APPROVED = 1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMDT.CONT_MAIN_TRANS_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMDT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMDT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND VMT.VENDOR_MST_PK = CMDT.TRANSPORTER_MST_FK");
                    sb.Append("        UNION");
                    sb.Append("        SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CTFT.CONT_TRANS_FCL_PK MAIN_PK,");
                    sb.Append("               CTFT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Transport Contract' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               'FCL' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CTFT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CTFT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CTFT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM CONT_TRANS_FCL_TBL CTFT, USER_MST_TBL UMT, VENDOR_MST_TBL VMT");
                    sb.Append("         WHERE CTFT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("         AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append("           AND CTFT.CONT_STATUS = 1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CTFT.CONT_TRANS_FCL_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CTFT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CTFT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND VMT.VENDOR_MST_PK = CTFT.TRANSPORTER_MST_FK) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Sea/Supplier's/Sea Freight Tariff
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 4)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               TMST.TARIFF_MAIN_SEA_PK MAIN_PK,");
                    sb.Append("               TMST.TARIFF_REF_NO CONTRACT_NO,");
                    sb.Append("               'Sea Freight Tariff' CONTRACT_TYPE,");
                    sb.Append("               OMT.OPERATOR_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(TMST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(TMST.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(TMST.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(TMST.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM TARIFF_MAIN_SEA_TBL TMST,");
                    sb.Append("               OPERATOR_MST_TBL    OMT,");
                    sb.Append("               USER_MST_TBL        UMT");
                    sb.Append("         WHERE TMST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND OMT.OPERATOR_MST_PK(+) = TMST.OPERATOR_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND OMT.OPERATOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND TMST.TARIFF_MAIN_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(TMST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(TMST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Sea/Supplier's/Rent & Demuurage
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 6)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               DSMT.DETENTION_SLAB_MAIN_PK MAIN_PK,");
                    sb.Append("               DSMT.TARIFF_REF_NO CONTRACT_NO,");
                    sb.Append("               'Rent & Demurrage' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(DSMT.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(DSMT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(DSMT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(DSMT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM DETENTION_SLAB_MAIN_TBL DSMT,");
                    sb.Append("               VENDOR_MST_TBL          VMT,");
                    sb.Append("               USER_MST_TBL            UMT");
                    sb.Append("         WHERE DSMT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND VMT.VENDOR_MST_PK = DSMT.VENDOR_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND DSMT.DETENTION_SLAB_MAIN_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(DSMT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(DSMT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Sea/Supplier's/SL RFQ
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 10)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               RFQ.RFQ_MAIN_SEA_PK MAIN_PK,");
                    sb.Append("               RFQ.RFQ_REF_NO CONTRACT_NO,");
                    sb.Append("               'SL RFQ' CONTRACT_TYPE,");
                    sb.Append("               OMT.OPERATOR_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(RFQ.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(RFQ.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(RFQ.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(RFQ.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM RFQ_MAIN_SEA_TBL RFQ, OPERATOR_MST_TBL OMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE RFQ.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND OMT.OPERATOR_MST_PK = RFQ.OPERATOR_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND OMT.OPERATOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND RFQ.RFQ_MAIN_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(RFQ.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(RFQ.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Sea/Customer/SL Spot Rate
                }
                else if (Business == 2 & ContractOf == 2 & ContractType == 5)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       RFQSPOT.RFQ_SPOT_SEA_PK MAIN_PK,");
                    sb.Append("       RFQSPOT.RFQ_REF_NO      CONTRACT_NO,");
                    sb.Append("       'SL Spot Rate' CONTRACT_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("       DECODE(RFQSPOT.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       TO_CHAR(RFQSPOT.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(RFQSPOT.VALID_TO,DATEFORMAT) VALID_TO ,");
                    sb.Append("       TO_DATE(RFQSPOT.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '' VIEW_CONTRACT,");
                    sb.Append("       '' RENEWAL,");
                    sb.Append("       '' SALES_CALL");
                    sb.Append("  FROM RFQ_SPOT_RATE_SEA_TBL RFQSPOT, USER_MST_TBL UMT, CUSTOMER_MST_TBL CMT");
                    sb.Append("  WHERE CMT.CUSTOMER_MST_PK = RFQSPOT.CUSTOMER_MST_FK");
                    sb.Append(" AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append(" AND RFQSPOT.APPROVED=1");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND RFQSPOT.RFQ_SPOT_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(RFQSPOT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(RFQSPOT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append(" AND RFQSPOT.CREATED_BY_FK = UMT.USER_MST_PK) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Sea/Customer/Customer Contract
                }
                else if (Business == 2 & ContractOf == 2 & ContractType == 7)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CCST.CONT_CUST_SEA_PK MAIN_PK,");
                    sb.Append("               CCST.CONT_REF_NO CONTRACT_NO,");
                    sb.Append("               'Customer Contract' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(CCST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CCST.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CCST.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CCST.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM CONT_CUST_SEA_TBL CCST,");
                    sb.Append("               CUSTOMER_MST_TBL  CMT,");
                    sb.Append("               USER_MST_TBL      UMT");
                    sb.Append("         WHERE CCST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = CCST.CUSTOMER_MST_FK");
                    sb.Append("           AND CCST.STATUS = 2");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CCST.CONT_CUST_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CCST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CCST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Sea/Customer/Quotation
                }
                else if (Business == 2 & ContractOf == 2 & ContractType == 8)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               QST.QUOTATION_SEA_PK MAIN_PK,");
                    sb.Append("               QST.QUOTATION_REF_NO CONTRACT_NO,");
                    sb.Append("               'Quotation' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(QST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(QST.QUOTATION_DATE, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR((QST.QUOTATION_DATE + QST.VALID_FOR), DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE((QST.QUOTATION_DATE + QST.VALID_FOR), DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM QUOTATION_SEA_TBL QST, CUSTOMER_MST_TBL CMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE QST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = QST.CUSTOMER_MST_FK");
                    sb.Append("           AND QST.STATUS IN(2,4)");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND QST.QUOTATION_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(QST.QUOTATION_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE((QST.QUOTATION_DATE + QST.VALID_FOR), 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Sea/Customer/SRR
                }
                else if (Business == 2 & ContractOf == 2 & ContractType == 9)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               SST.SRR_SEA_PK MAIN_PK,");
                    sb.Append("               SST.SRR_REF_NO CONTRACT_NO,");
                    sb.Append("               'SRR' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(SST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(SST.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(SST.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(SST.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM SRR_SEA_TBL SST, CUSTOMER_MST_TBL CMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE SST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND SST.SRR_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(SST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(SST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Air/Supplier's/Airline Contract
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 1)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CMAT.CONT_MAIN_AIR_PK MAIN_PK,");
                    sb.Append("               CMAT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Airline Contract' CONTRACT_TYPE,");
                    sb.Append("               AMT.AIRLINE_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CMAT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CMAT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CMAT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM CONT_MAIN_AIR_TBL CMAT, USER_MST_TBL UMT, AIRLINE_MST_TBL AMT");
                    sb.Append("         WHERE CMAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND AMT.AIRLINE_MST_PK = CMAT.AIRLINE_MST_FK");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND AMT.AIRLINE_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMAT.CONT_MAIN_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMAT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMAT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND CMAT.CONT_APPROVED = 1) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK,");
                    sb.Append("          LMT.LOCATION_NAME");

                    //Air/Supplier's/Warehouse Contract
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 2)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CMDT.CONT_MAIN_DEPOT_PK MAIN_PK,");
                    sb.Append("               CMDT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Warehouse Contract' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(CMDT.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CMDT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CMDT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CMDT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM CONT_MAIN_DEPOT_TBL CMDT,");
                    sb.Append("               USER_MST_TBL        UMT,");
                    sb.Append("               VENDOR_MST_TBL      VMT");
                    sb.Append("         WHERE CMDT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ");
                    sb.Append("           AND CMDT.BUSINESS_TYPE = 1");
                    sb.Append("           AND CMDT.CONT_APPROVED = 1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMDT.CONT_MAIN_DEPOT_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMDT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMDT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND VMT.VENDOR_MST_PK = CMDT.DEPOT_MST_FK) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");
                    //Air/Supplier's/Transport Contract
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 3)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CMDT.CONT_MAIN_TRANS_PK MAIN_PK,");
                    sb.Append("               CMDT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Transport Contract' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CMDT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CMDT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CMDT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM CONT_MAIN_TRANS_TBL CMDT,");
                    sb.Append("               USER_MST_TBL        UMT,");
                    sb.Append("               VENDOR_MST_TBL      VMT");
                    sb.Append("         WHERE CMDT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ");
                    sb.Append("           AND CMDT.BUSINESS_TYPE = 1");
                    sb.Append("           AND CMDT.CONT_APPROVED = 1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMDT.CONT_MAIN_TRANS_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMDT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMDT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND VMT.VENDOR_MST_PK = CMDT.TRANSPORTER_MST_FK) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Air/Supplier's/Air Freight Tariff
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 4)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               TMST.TARIFF_MAIN_AIR_PK MAIN_PK,");
                    sb.Append("               TMST.TARIFF_REF_NO CONTRACT_NO,");
                    sb.Append("               'Air Freight Tariff' CONTRACT_TYPE,");
                    sb.Append("               AMT.AIRLINE_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(TMST.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(TMST.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(TMST.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM TARIFF_MAIN_AIR_TBL TMST,");
                    sb.Append("               AIRLINE_MST_TBL     AMT,");
                    sb.Append("               USER_MST_TBL        UMT");
                    sb.Append("         WHERE TMST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND AMT.AIRLINE_MST_PK(+) = TMST.AIRLINE_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND AMT.AIRLINE_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND TMST.TARIFF_MAIN_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(TMST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(TMST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Air/Supplier's/Air Freight Storage Tariff
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 6)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               DSMT.DEMURAGE_SLAB_MAIN_PK MAIN_PK,");
                    sb.Append("               DSMT.TARIFF_REF_NO CONTRACT_NO,");
                    sb.Append("               'Air Freight Storage Tariff' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(DSMT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(DSMT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(DSMT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM DEMURRAGE_SLAB_MAIN_TBL DSMT,");
                    sb.Append("               VENDOR_MST_TBL          VMT,");
                    sb.Append("               USER_MST_TBL            UMT");
                    sb.Append("         WHERE DSMT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND VMT.VENDOR_MST_PK = DSMT.DEPOT_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND DSMT.DEMURAGE_SLAB_MAIN_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(DSMT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(DSMT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Air/Supplier's/Airline RFQ
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 10)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               RFQ.RFQ_MAIN_AIR_PK MAIN_PK,");
                    sb.Append("               RFQ.RFQ_REF_NO CONTRACT_NO,");
                    sb.Append("               'AIRLINE RFQ' CONTRACT_TYPE,");
                    sb.Append("               AMT.AIRLINE_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(RFQ.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(RFQ.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(RFQ.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM RFQ_MAIN_AIR_TBL RFQ, AIRLINE_MST_TBL AMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE RFQ.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND AMT.AIRLINE_MST_PK = RFQ.AIRLINE_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND AMT.AIRLINE_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND RFQ.RFQ_MAIN_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(RFQ.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(RFQ.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Air/Customer/Airline Spot Rate
                }
                else if (Business == 1 & ContractOf == 2 & ContractType == 5)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (");
                    sb.Append("SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       RFQSPOT.RFQ_SPOT_AIR_PK MAIN_PK,");
                    sb.Append("       RFQSPOT.RFQ_REF_NO      CONTRACT_NO,");
                    sb.Append("       'Airline Spot Rate' CONTRACT_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("       '' CARGO_TYPE,");
                    sb.Append("       TO_CHAR(RFQSPOT.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(RFQSPOT.VALID_TO,DATEFORMAT) VALID_TO ,");
                    sb.Append("       TO_DATE(RFQSPOT.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '' VIEW_CONTRACT,");
                    sb.Append("       '' RENEWAL,");
                    sb.Append("       '' SALES_CALL");
                    sb.Append("  FROM RFQ_SPOT_RATE_AIR_TBL RFQSPOT, USER_MST_TBL UMT, CUSTOMER_MST_TBL CMT");
                    sb.Append("  WHERE CMT.CUSTOMER_MST_PK = RFQSPOT.CUSTOMER_MST_FK");
                    sb.Append(" AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append(" AND RFQSPOT.APPROVED=1");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND RFQSPOT.RFQ_SPOT_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(RFQSPOT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(RFQSPOT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append(" AND RFQSPOT.CREATED_BY_FK = UMT.USER_MST_PK) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Air/Customer/Customer Contract
                }
                else if (Business == 1 & ContractOf == 2 & ContractType == 7)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CCAT.CONT_CUST_AIR_PK MAIN_PK,");
                    sb.Append("               CCAT.CONT_REF_NO CONTRACT_NO,");
                    sb.Append("               'Customer Contract' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CCAT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CCAT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CCAT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM CONT_CUST_AIR_TBL CCAT,");
                    sb.Append("               CUSTOMER_MST_TBL CMT,");
                    sb.Append("               USER_MST_TBL   UMT");
                    sb.Append("         WHERE CCAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = CCAT.CUSTOMER_MST_FK");
                    sb.Append("           AND CCAT.CONT_APPROVED = 2");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CCAT.CONT_CUST_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CCAT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CCAT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Air/Customer/Quotation
                }
                else if (Business == 1 & ContractOf == 2 & ContractType == 8)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               QAT.QUOTATION_AIR_PK MAIN_PK,");
                    sb.Append("               QAT.QUOTATION_REF_NO CONTRACT_NO,");
                    sb.Append("               'Quotation' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(QAT.QUOTATION_DATE, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR((QAT.QUOTATION_DATE + QAT.VALID_FOR), DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE((QAT.QUOTATION_DATE + QAT.VALID_FOR), DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM QUOTATION_AIR_TBL QAT, CUSTOMER_MST_TBL CMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE QAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = QAT.CUSTOMER_MST_FK");
                    sb.Append("           AND QAT.STATUS IN(2,4)");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND QAT.QUOTATION_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(QAT.QUOTATION_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE((QAT.QUOTATION_DATE + QAT.VALID_FOR), 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");

                    //Air/Customer/SRR
                }
                else if (Business == 1 & ContractOf == 2 & ContractType == 9)
                {
                    sb.Append("SELECT DISTINCT LMT.LOCATION_MST_PK,");
                    sb.Append("                LMT.LOCATION_NAME,");
                    sb.Append(" COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 1 AND AGE <= 7 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE1TO7,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 8 AND AGE <= 14 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE8TO14,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 15 AND AGE <= 21 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE15TO21,");
                    sb.Append("                        COUNT(CASE");
                    sb.Append("                          WHEN AGE >= 22 AND AGE <= 28 THEN");
                    sb.Append("                           MAIN_PK");
                    sb.Append("                        END) AGE22TO28");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               SAT.SRR_AIR_PK MAIN_PK,");
                    sb.Append("               SAT.SRR_REF_NO CONTRACT_NO,");
                    sb.Append("               'SRR' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(SAT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(SAT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(SAT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '' VIEW_CONTRACT,");
                    sb.Append("               '' RENEWAL,");
                    sb.Append("               '' SALES_CALL");
                    sb.Append("          FROM SRR_AIR_TBL SAT, CUSTOMER_MST_TBL CMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE SAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = SAT.CUSTOMER_MST_FK");
                    sb.Append("           AND SAT.SRR_APPROVED=1");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND SAT.SRR_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(SAT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(SAT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ) Q,");
                    sb.Append("       LOCATION_MST_TBL LMT");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                    sb.Append("   AND LMT.LOCATION_MST_PK = Q.DEFAULT_LOCATION_FK");
                    sb.Append("   AND LMT.LOCATION_MST_PK =  " + LocFk + " ");
                    sb.Append(" GROUP BY LMT.LOCATION_MST_PK, LMT.LOCATION_NAME");
                }
                if (printPDF == 1)
                {
                    return objWF.GetDataSet(sb.ToString());
                    return functionReturnValue;
                }
                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";

                //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
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
                sb.Append(" ORDER BY LOCATION_NAME DESC");
                strSQL = " SELECT * FROM (SELECT ROWNUM SLNO, q.* FROM(";
                strSQL += sb.ToString();
                if (expExcel == 0)
                {
                    strSQL += " )q ) WHERE SLNO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " )q ) ";
                }
                //Return objWF.GetDataSet(strSQL)
                string sql = null;
                sql = strSQL;
                DataSet DS = null;
                DS = objWF.GetDataSet(strSQL);
                DataRelation CONTRel = null;
                DataTable dtChild = new DataTable();
                dtChild = FetchChild(Business, ContractOf, ContractType, LocFk, RefPk, CustPk, SupplierPk, ExpiryDt).Tables[0];
                DS.Tables.Add();
                foreach (DataColumn col in dtChild.Columns)
                {
                    DataColumn dc = new DataColumn(col.ColumnName, col.DataType);
                    DS.Tables[1].Columns.Add(dc);
                }
                foreach (DataRow row in dtChild.Rows)
                {
                    DataRow dr = null;
                    dr = DS.Tables[1].NewRow();
                    foreach (DataColumn col in dtChild.Columns)
                    {
                        dr[col.ColumnName] = row[col.ColumnName];
                    }
                    DS.Tables[1].Rows.Add(dr);
                }
                CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["LOCATION_MST_PK"], DS.Tables[1].Columns["DEFAULT_LOCATION_FK"], true);
                CONTRel.Nested = true;
                DS.Relations.Add(CONTRel);
                return DS;
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
            return functionReturnValue;
        }

        #endregion "Fetch Expiry Report Parent Grid Function"

        #region "Fetch Expiry Report Child Grid Function"

        /// <summary>
        /// Fetches the child.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="ContractOf">The contract of.</param>
        /// <param name="ContractType">Type of the contract.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="RefPk">The reference pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="SupplierPk">The supplier pk.</param>
        /// <param name="ExpiryDt">The expiry dt.</param>
        /// <returns></returns>
        public DataSet FetchChild(Int32 Business = 0, Int32 ContractOf = 0, Int32 ContractType = 0, Int32 LocFk = 0, string RefPk = "", string CustPk = "", string SupplierPk = "", string ExpiryDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                //Sea/Supplier's/SL Contract
                if (Business == 2 & ContractOf == 1 & ContractType == 1)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("        FROM (");
                    sb.Append("   SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       CMST.CONT_MAIN_SEA_PK MAIN_PK,");
                    sb.Append("       CMST.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("       'SL Contract' CONTRACT_TYPE,");
                    sb.Append("       OMT.OPERATOR_NAME SUP_CUST_NAME,");
                    sb.Append("       DECODE(CMST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       TO_CHAR(CMST.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(CMST.VALID_TO,DATEFORMAT) VALID_TO ,");
                    sb.Append("       TO_DATE(CMST.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '...' VIEW_CONTRACT,");
                    sb.Append("       '...' RENEWAL,");
                    sb.Append("       '...' SALES_CALL");
                    sb.Append("  FROM CONT_MAIN_SEA_TBL CMST, OPERATOR_MST_TBL OMT, USER_MST_TBL UMT");
                    sb.Append(" WHERE OMT.OPERATOR_MST_PK = CMST.OPERATOR_MST_FK");
                    sb.Append(" AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append(" AND CMST.CONT_APPROVED = 1");
                    sb.Append(" AND CMST.CREATED_BY_FK = UMT.USER_MST_PK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND OMT.OPERATOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMST.CONT_MAIN_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("  )WHERE (AGE >=0 AND AGE <= 28)");

                    //Sea/Supplier's/Warehouse Contract
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 2)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("        FROM (");
                    sb.Append("SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       CMDT.CONT_MAIN_DEPOT_PK MAIN_PK,");
                    sb.Append("       CMDT.CONTRACT_NO        CONTRACT_NO,");
                    sb.Append("       'Warehouse Contract' CONTRACT_TYPE,");
                    sb.Append("       VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("       DECODE(CMDT.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       TO_CHAR(CMDT.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(CMDT.VALID_TO,DATEFORMAT) VALID_TO,       ");
                    sb.Append("       TO_DATE(CMDT.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '...' VIEW_CONTRACT,");
                    sb.Append("       '...' RENEWAL,");
                    sb.Append("       '...' SALES_CALL");
                    sb.Append("  FROM CONT_MAIN_DEPOT_TBL CMDT, USER_MST_TBL UMT, VENDOR_MST_TBL VMT");
                    sb.Append("  WHERE CMDT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append("  AND CMDT.BUSINESS_TYPE=2");
                    sb.Append("  AND CMDT.CONT_APPROVED=1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMDT.CONT_MAIN_DEPOT_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMDT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMDT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("  AND VMT.VENDOR_MST_PK = CMDT.DEPOT_MST_FK)");
                    sb.Append("   WHERE (AGE >=0 AND AGE <= 28)");

                    //Sea/Supplier's/Transport Contract
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 3)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CMDT.CONT_MAIN_TRANS_PK MAIN_PK,");
                    sb.Append("               CMDT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Transport Contract' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               'LCL' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CMDT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CMDT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CMDT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM CONT_MAIN_TRANS_TBL CMDT,");
                    sb.Append("               USER_MST_TBL        UMT,");
                    sb.Append("               VENDOR_MST_TBL      VMT");
                    sb.Append("         WHERE CMDT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ");
                    sb.Append("           AND CMDT.BUSINESS_TYPE = 2");
                    sb.Append("           AND CMDT.CONT_APPROVED = 1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMDT.CONT_MAIN_TRANS_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMDT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMDT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND VMT.VENDOR_MST_PK = CMDT.TRANSPORTER_MST_FK");
                    sb.Append("        UNION");
                    sb.Append("        SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CTFT.CONT_TRANS_FCL_PK MAIN_PK,");
                    sb.Append("               CTFT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Transport Contract' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               'FCL' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CTFT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CTFT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CTFT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM CONT_TRANS_FCL_TBL CTFT, USER_MST_TBL UMT, VENDOR_MST_TBL VMT");
                    sb.Append("         WHERE CTFT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ");
                    sb.Append("           AND CTFT.CONT_STATUS = 1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CTFT.CONT_TRANS_FCL_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CTFT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CTFT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND VMT.VENDOR_MST_PK = CTFT.TRANSPORTER_MST_FK)");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Sea/Supplier's/Sea Freight Tariff
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 4)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("        FROM (");
                    sb.Append("SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       TMST.TARIFF_MAIN_SEA_PK MAIN_PK,");
                    sb.Append("       TMST.TARIFF_REF_NO CONTRACT_NO,");
                    sb.Append("       'Sea Freight Tariff' CONTRACT_TYPE,");
                    sb.Append("       OMT.OPERATOR_NAME SUP_CUST_NAME,");
                    sb.Append("       DECODE(TMST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       TO_CHAR(TMST.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(TMST.VALID_TO,DATEFORMAT) VALID_TO,       ");
                    sb.Append("       TO_DATE(TMST.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '...' VIEW_CONTRACT,");
                    sb.Append("       '...' RENEWAL,");
                    sb.Append("       '...' SALES_CALL");
                    sb.Append("  FROM TARIFF_MAIN_SEA_TBL TMST,OPERATOR_MST_TBL OMT , USER_MST_TBL UMT");
                    sb.Append("  WHERE TMST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND OMT.OPERATOR_MST_PK(+) = TMST.OPERATOR_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND OMT.OPERATOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND TMST.TARIFF_MAIN_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(TMST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(TMST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append(" AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append("   WHERE (AGE >=0 AND AGE <= 28)");

                    //Sea/Supplier's/Rent & Demuurage
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 6)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("        FROM (");
                    sb.Append("SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       DSMT.DETENTION_SLAB_MAIN_PK MAIN_PK,");
                    sb.Append("       DSMT.TARIFF_REF_NO CONTRACT_NO,");
                    sb.Append("       'Rent & Demurrage' CONTRACT_TYPE,");
                    sb.Append("       VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("       DECODE(DSMT.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       TO_CHAR(DSMT.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(DSMT.VALID_TO,DATEFORMAT) VALID_TO,       ");
                    sb.Append("       TO_DATE(DSMT.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '...' VIEW_CONTRACT,");
                    sb.Append("       '...' RENEWAL,");
                    sb.Append("       '...' SALES_CALL");
                    sb.Append("  FROM DETENTION_SLAB_MAIN_TBL DSMT, VENDOR_MST_TBL VMT , USER_MST_TBL UMT");
                    sb.Append("  WHERE DSMT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND VMT.VENDOR_MST_PK = DSMT.VENDOR_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND DSMT.DETENTION_SLAB_MAIN_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(DSMT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(DSMT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append(" AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append("   WHERE (AGE >=0 AND AGE <= 28)");

                    //Sea/Supplier's/SL RFQ
                }
                else if (Business == 2 & ContractOf == 1 & ContractType == 10)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               RFQ.RFQ_MAIN_SEA_PK MAIN_PK,");
                    sb.Append("               RFQ.RFQ_REF_NO CONTRACT_NO,");
                    sb.Append("               'SL RFQ' CONTRACT_TYPE,");
                    sb.Append("               OMT.OPERATOR_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(RFQ.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(RFQ.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(RFQ.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(RFQ.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM RFQ_MAIN_SEA_TBL RFQ, OPERATOR_MST_TBL OMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE RFQ.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND OMT.OPERATOR_MST_PK = RFQ.OPERATOR_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND OMT.OPERATOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND RFQ.RFQ_MAIN_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(RFQ.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(RFQ.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Sea/Customer/SL Spot Rate
                }
                else if (Business == 2 & ContractOf == 2 & ContractType == 5)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("        FROM (");
                    sb.Append("SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       RFQSPOT.RFQ_SPOT_SEA_PK MAIN_PK,");
                    sb.Append("       RFQSPOT.RFQ_REF_NO      CONTRACT_NO,");
                    sb.Append("       'SL Spot Rate' CONTRACT_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("       DECODE(RFQSPOT.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       TO_CHAR(RFQSPOT.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(RFQSPOT.VALID_TO,DATEFORMAT) VALID_TO ,");
                    sb.Append("       TO_DATE(RFQSPOT.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '...' VIEW_CONTRACT,");
                    sb.Append("       '...' RENEWAL,");
                    sb.Append("       '...' SALES_CALL");
                    sb.Append("  FROM RFQ_SPOT_RATE_SEA_TBL RFQSPOT, USER_MST_TBL UMT, CUSTOMER_MST_TBL CMT");
                    sb.Append("  WHERE CMT.CUSTOMER_MST_PK = RFQSPOT.CUSTOMER_MST_FK");
                    sb.Append(" AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append(" AND RFQSPOT.APPROVED=1");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND RFQSPOT.RFQ_SPOT_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(RFQSPOT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(RFQSPOT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append(" AND RFQSPOT.CREATED_BY_FK = UMT.USER_MST_PK)");
                    sb.Append(" WHERE (AGE >=0 AND AGE <= 28)");

                    //Sea/Customer/Customer Contract
                }
                else if (Business == 2 & ContractOf == 2 & ContractType == 7)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CCST.CONT_CUST_SEA_PK MAIN_PK,");
                    sb.Append("               CCST.CONT_REF_NO CONTRACT_NO,");
                    sb.Append("               'Customer Contract' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(CCST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CCST.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CCST.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CCST.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM CONT_CUST_SEA_TBL CCST,");
                    sb.Append("               CUSTOMER_MST_TBL CMT,");
                    sb.Append("               USER_MST_TBL   UMT");
                    sb.Append("         WHERE CCST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = CCST.CUSTOMER_MST_FK");
                    sb.Append("           AND CCST.STATUS = 2");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CCST.CONT_CUST_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CCST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CCST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Sea/Customer/Quotation
                }
                else if (Business == 2 & ContractOf == 2 & ContractType == 8)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               QST.QUOTATION_SEA_PK MAIN_PK,");
                    sb.Append("               QST.QUOTATION_REF_NO CONTRACT_NO,");
                    sb.Append("               'Quotation' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(QST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(QST.QUOTATION_DATE, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR((QST.QUOTATION_DATE + QST.VALID_FOR), DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE((QST.QUOTATION_DATE + QST.VALID_FOR), DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM QUOTATION_SEA_TBL QST, CUSTOMER_MST_TBL CMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE QST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = QST.CUSTOMER_MST_FK");
                    sb.Append("           AND QST.STATUS IN (2,4)");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND QST.QUOTATION_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(QST.QUOTATION_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE((QST.QUOTATION_DATE + QST.VALID_FOR), 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("     AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Sea/Customer/SRR
                }
                else if (Business == 2 & ContractOf == 2 & ContractType == 9)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               SST.SRR_SEA_PK MAIN_PK,");
                    sb.Append("               SST.SRR_REF_NO CONTRACT_NO,");
                    sb.Append("               'SRR' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               DECODE(SST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("               TO_CHAR(SST.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(SST.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(SST.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM SRR_SEA_TBL SST, CUSTOMER_MST_TBL CMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE SST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND SST.SRR_SEA_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(SST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(SST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Air/Supplier's/Airline Contract
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 1)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CMAT.CONT_MAIN_AIR_PK MAIN_PK,");
                    sb.Append("               CMAT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Airline Contract' CONTRACT_TYPE,");
                    sb.Append("               AMT.AIRLINE_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CMAT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CMAT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CMAT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM CONT_MAIN_AIR_TBL CMAT, USER_MST_TBL UMT, AIRLINE_MST_TBL AMT");
                    sb.Append("         WHERE CMAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND AMT.AIRLINE_MST_PK = CMAT.AIRLINE_MST_FK");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " ");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND AMT.AIRLINE_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMAT.CONT_MAIN_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMAT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMAT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND CMAT.CONT_APPROVED = 1)");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Air/Supplier's/Warehouse Contract
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 2)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("        FROM (");
                    sb.Append("SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       CMDT.CONT_MAIN_DEPOT_PK MAIN_PK,");
                    sb.Append("       CMDT.CONTRACT_NO        CONTRACT_NO,");
                    sb.Append("       'Warehouse Contract' CONTRACT_TYPE,");
                    sb.Append("       VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("       '' CARGO_TYPE,");
                    sb.Append("       TO_CHAR(CMDT.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(CMDT.VALID_TO,DATEFORMAT) VALID_TO,       ");
                    sb.Append("       TO_DATE(CMDT.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '...' VIEW_CONTRACT,");
                    sb.Append("       '...' RENEWAL,");
                    sb.Append("       '...' SALES_CALL");
                    sb.Append("  FROM CONT_MAIN_DEPOT_TBL CMDT, USER_MST_TBL UMT, VENDOR_MST_TBL VMT");
                    sb.Append("  WHERE CMDT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append("  AND CMDT.BUSINESS_TYPE=1");
                    sb.Append("  AND CMDT.CONT_APPROVED=1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMDT.CONT_MAIN_DEPOT_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMDT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMDT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("  AND VMT.VENDOR_MST_PK = CMDT.DEPOT_MST_FK)");
                    sb.Append("   WHERE (AGE >=0 AND AGE <= 28)");

                    //Air/Supplier's/Transport Contract
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 3)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CMDT.CONT_MAIN_TRANS_PK MAIN_PK,");
                    sb.Append("               CMDT.CONTRACT_NO CONTRACT_NO,");
                    sb.Append("               'Transport Contract' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CMDT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CMDT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CMDT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM CONT_MAIN_TRANS_TBL CMDT,");
                    sb.Append("               USER_MST_TBL        UMT,");
                    sb.Append("               VENDOR_MST_TBL      VMT");
                    sb.Append("         WHERE CMDT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("         AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append("           AND CMDT.BUSINESS_TYPE = 1");
                    sb.Append("           AND CMDT.CONT_APPROVED = 1");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CMDT.CONT_MAIN_TRANS_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CMDT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CMDT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND VMT.VENDOR_MST_PK = CMDT.TRANSPORTER_MST_FK)");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Air/Supplier's/Air Freight Tariff
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 4)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("        FROM (");
                    sb.Append("SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       TMST.TARIFF_MAIN_AIR_PK MAIN_PK,");
                    sb.Append("       TMST.TARIFF_REF_NO CONTRACT_NO,");
                    sb.Append("       'Air Freight Tariff' CONTRACT_TYPE,");
                    sb.Append("       AMT.AIRLINE_NAME SUP_CUST_NAME,");
                    sb.Append("       '' CARGO_TYPE,");
                    sb.Append("       TO_CHAR(TMST.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(TMST.VALID_TO,DATEFORMAT) VALID_TO,       ");
                    sb.Append("       TO_DATE(TMST.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '...' VIEW_CONTRACT,");
                    sb.Append("       '...' RENEWAL,");
                    sb.Append("       '...' SALES_CALL");
                    sb.Append("  FROM TARIFF_MAIN_AIR_TBL TMST,AIRLINE_MST_TBL AMT , USER_MST_TBL UMT");
                    sb.Append("  WHERE TMST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND AMT.AIRLINE_MST_PK(+) = TMST.AIRLINE_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND AMT.AIRLINE_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND TMST.TARIFF_MAIN_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(TMST.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(TMST.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append(" AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >=0 AND AGE <= 28)");

                    //Air/Supplier's/Air Freight Storage Tariff
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 6)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               DSMT.DEMURAGE_SLAB_MAIN_PK MAIN_PK,");
                    sb.Append("               DSMT.TARIFF_REF_NO CONTRACT_NO,");
                    sb.Append("               'Air Freight Storage Tariff' CONTRACT_TYPE,");
                    sb.Append("               VMT.VENDOR_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(DSMT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(DSMT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(DSMT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM DEMURRAGE_SLAB_MAIN_TBL DSMT,");
                    sb.Append("               VENDOR_MST_TBL          VMT,");
                    sb.Append("               USER_MST_TBL            UMT");
                    sb.Append("         WHERE DSMT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND VMT.VENDOR_MST_PK = DSMT.DEPOT_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND VMT.VENDOR_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND DSMT.DEMURAGE_SLAB_MAIN_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(DSMT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(DSMT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Air/Supplier's/Airline RFQ
                }
                else if (Business == 1 & ContractOf == 1 & ContractType == 10)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               RFQ.RFQ_MAIN_AIR_PK MAIN_PK,");
                    sb.Append("               RFQ.RFQ_REF_NO CONTRACT_NO,");
                    sb.Append("               'AIRLINE RFQ' CONTRACT_TYPE,");
                    sb.Append("               AMT.AIRLINE_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(RFQ.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(RFQ.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(RFQ.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM RFQ_MAIN_AIR_TBL RFQ, AIRLINE_MST_TBL AMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE RFQ.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND AMT.AIRLINE_MST_PK = RFQ.AIRLINE_MST_FK");
                    if (!string.IsNullOrEmpty(SupplierPk))
                    {
                        sb.Append(" AND AMT.AIRLINE_MST_PK = " + SupplierPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND RFQ.RFQ_MAIN_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(RFQ.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(RFQ.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Air/Customer/Airline Spot Rate
                }
                else if (Business == 1 & ContractOf == 2 & ContractType == 5)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("        FROM (");
                    sb.Append("SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("       RFQSPOT.RFQ_SPOT_AIR_PK MAIN_PK,");
                    sb.Append("       RFQSPOT.RFQ_REF_NO      CONTRACT_NO,");
                    sb.Append("       'Airline Spot Rate' CONTRACT_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("       '' CARGO_TYPE,");
                    sb.Append("       TO_CHAR(RFQSPOT.VALID_FROM,DATEFORMAT) VALID_FROM,");
                    sb.Append("       TO_CHAR(RFQSPOT.VALID_TO,DATEFORMAT) VALID_TO ,");
                    sb.Append("       TO_DATE(RFQSPOT.VALID_TO,DATEFORMAT) - TO_DATE('" + ExpiryDt + "',DATEFORMAT)+1 AGE,");
                    sb.Append("       '...' VIEW_CONTRACT,");
                    sb.Append("       '...' RENEWAL,");
                    sb.Append("       '...' SALES_CALL");
                    sb.Append("  FROM RFQ_SPOT_RATE_AIR_TBL RFQSPOT, USER_MST_TBL UMT, CUSTOMER_MST_TBL CMT");
                    sb.Append("  WHERE CMT.CUSTOMER_MST_PK = RFQSPOT.CUSTOMER_MST_FK");
                    sb.Append(" AND UMT.DEFAULT_LOCATION_FK= " + LocFk + " ");
                    sb.Append(" AND RFQSPOT.APPROVED=1");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND RFQSPOT.RFQ_SPOT_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(RFQSPOT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(RFQSPOT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append(" AND RFQSPOT.CREATED_BY_FK = UMT.USER_MST_PK)");
                    sb.Append(" WHERE (AGE >=0 AND AGE <= 28)");

                    //Air/Customer/Customer Contract
                }
                else if (Business == 1 & ContractOf == 2 & ContractType == 7)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               CCAT.CONT_CUST_AIR_PK MAIN_PK,");
                    sb.Append("               CCAT.CONT_REF_NO CONTRACT_NO,");
                    sb.Append("               'Customer Contract' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(CCAT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(CCAT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(CCAT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM CONT_CUST_AIR_TBL CCAT,");
                    sb.Append("               CUSTOMER_MST_TBL CMT,");
                    sb.Append("               USER_MST_TBL   UMT");
                    sb.Append("         WHERE CCAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = CCAT.CUSTOMER_MST_FK");
                    sb.Append("           AND CCAT.CONT_APPROVED = 2");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND CCAT.CONT_CUST_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(CCAT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(CCAT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Air/Customer/Quotation
                }
                else if (Business == 1 & ContractOf == 2 & ContractType == 8)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               QAT.QUOTATION_AIR_PK MAIN_PK,");
                    sb.Append("               QAT.QUOTATION_REF_NO CONTRACT_NO,");
                    sb.Append("               'Quotation' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(QAT.QUOTATION_DATE, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR((QAT.QUOTATION_DATE + QAT.VALID_FOR), DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE((QAT.QUOTATION_DATE + QAT.VALID_FOR), DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM QUOTATION_AIR_TBL QAT, CUSTOMER_MST_TBL CMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE QAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = QAT.CUSTOMER_MST_FK");
                    sb.Append("           AND QAT.STATUS in (2,4)");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND QAT.QUOTATION_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(QAT.QUOTATION_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE((QAT.QUOTATION_DATE + QAT.VALID_FOR), 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");

                    //Air/Customer/SRR
                }
                else if (Business == 1 & ContractOf == 2 & ContractType == 9)
                {
                    sb.Append("SELECT DEFAULT_LOCATION_FK,");
                    sb.Append("       MAIN_PK,");
                    sb.Append("       CONTRACT_NO,");
                    sb.Append("       CONTRACT_TYPE,");
                    sb.Append("       SUP_CUST_NAME,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       VALID_FROM,");
                    sb.Append("       VALID_TO,");
                    sb.Append("       AGE,");
                    sb.Append("       VIEW_CONTRACT,");
                    sb.Append("       RENEWAL,");
                    sb.Append("       SALES_CALL");
                    sb.Append("  FROM (SELECT UMT.DEFAULT_LOCATION_FK,");
                    sb.Append("               SAT.SRR_AIR_PK MAIN_PK,");
                    sb.Append("               SAT.SRR_REF_NO CONTRACT_NO,");
                    sb.Append("               'SRR' CONTRACT_TYPE,");
                    sb.Append("               CMT.CUSTOMER_NAME SUP_CUST_NAME,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               TO_CHAR(SAT.VALID_FROM, DATEFORMAT) VALID_FROM,");
                    sb.Append("               TO_CHAR(SAT.VALID_TO, DATEFORMAT) VALID_TO,");
                    sb.Append("               TO_DATE(SAT.VALID_TO, DATEFORMAT) -");
                    sb.Append("               TO_DATE('" + ExpiryDt + "', DATEFORMAT)+1 AGE,");
                    sb.Append("               '...' VIEW_CONTRACT,");
                    sb.Append("               '...' RENEWAL,");
                    sb.Append("               '...' SALES_CALL");
                    sb.Append("          FROM SRR_AIR_TBL SAT, CUSTOMER_MST_TBL CMT, USER_MST_TBL UMT");
                    sb.Append("         WHERE SAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND CMT.CUSTOMER_MST_PK = SAT.CUSTOMER_MST_FK");
                    sb.Append("           AND SAT.SRR_APPROVED=1");
                    if (!string.IsNullOrEmpty(CustPk))
                    {
                        sb.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk + " ");
                    }
                    if (!string.IsNullOrEmpty(RefPk))
                    {
                        sb.Append(" AND SAT.SRR_AIR_PK = " + RefPk + " ");
                    }
                    if (!string.IsNullOrEmpty(ExpiryDt))
                    {
                        sb.Append(" And TO_DATE(SAT.VALID_FROM, 'DD/MM/YYYY') <= TO_DATE('" + ExpiryDt + "',dateformat) ");
                        sb.Append(" And TO_DATE(SAT.VALID_TO, 'DD/MM/YYYY') >= TO_DATE('" + ExpiryDt + "',dateformat) ");
                    }
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =  " + LocFk + " )");
                    sb.Append(" WHERE (AGE >= 0 AND AGE <= 28)");
                }
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Fetch Expiry Report Child Grid Function"

        #region "Fetch List"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="ContractOf">The contract of.</param>
        /// <param name="ContractType">Type of the contract.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="RefPk">The reference pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="Supplier">The supplier.</param>
        /// <param name="ExpiryDt">The expiry dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <param name="printPDF">The print PDF.</param>
        /// <returns></returns>
        public DataSet FetchAll(string Business = "0", string ContractOf = "0", string ContractType = "", string LocFk = "0", int RefPk = 0, int CustPk = 0, string Supplier = "", string ExpiryDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 expExcel = 0, Int32 printPDF = 0)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".CONTRACT_EXPIRY_PKG.CONTRACT_EXPIRY_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand.Parameters;

                _with2.Add("BIZ_TYPE_IN", Convert.ToInt32(Business)).Direction = ParameterDirection.Input;
                _with2.Add("CONTRACT_OF_IN", Convert.ToInt32(ContractOf)).Direction = ParameterDirection.Input;
                _with2.Add("CONTRACT_TYPE_IN", (string.IsNullOrEmpty(ContractType) ? "" : ContractType)).Direction = ParameterDirection.Input;
                _with2.Add("CONTRACT_PK_IN", RefPk).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMERPK_IN", CustPk).Direction = ParameterDirection.Input;
                _with2.Add("SUPPLIERPK_IN", (string.IsNullOrEmpty(Supplier) ? "" : Supplier)).Direction = ParameterDirection.Input;
                _with2.Add("DATE_IN", (string.IsNullOrEmpty(ExpiryDt) ? "" : ExpiryDt)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_PK_IN", Convert.ToInt32(LocFk)).Direction = ParameterDirection.Input;
                _with2.Add("EXCEL_IN", expExcel).Direction = ParameterDirection.Input;
                _with2.Add("PDF_IN", printPDF).Direction = ParameterDirection.Input;
                _with2.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("DETAIL0_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("DETAIL1_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                DataRelation CONTRel = null;
                CONTRel = new DataRelation("CONTRelation", dsData.Tables[0].Columns["MAIN_PK"], dsData.Tables[1].Columns["MAIN_PK"], true);

                CONTRel.Nested = true;
                dsData.Relations.Add(CONTRel);
                TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
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

        /// <summary>
        /// Fetches the child report.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="ContractOf">The contract of.</param>
        /// <param name="ContractType">Type of the contract.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="RefPk">The reference pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="Supplier">The supplier.</param>
        /// <param name="ExpiryDt">The expiry dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <param name="printPDF">The print PDF.</param>
        /// <returns></returns>
        public DataSet FetchChildReport(string Business = "0", string ContractOf = "0", string ContractType = "", string LocFk = "0", int RefPk = 0, int CustPk = 0, string Supplier = "", string ExpiryDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 expExcel = 0, Int32 printPDF = 0)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with3 = objWK.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".CONTRACT_EXPIRY_PKG.CONTRACT_FETCH_CHILD";

                objWK.MyCommand.Parameters.Clear();
                var _with4 = objWK.MyCommand.Parameters;

                _with4.Add("BIZ_TYPE_IN", Convert.ToInt32(Business)).Direction = ParameterDirection.Input;
                _with4.Add("CONTRACT_OF_IN", Convert.ToInt32(ContractOf)).Direction = ParameterDirection.Input;
                _with4.Add("CONTRACT_TYPE_IN", (string.IsNullOrEmpty(ContractType) ? "" : ContractType)).Direction = ParameterDirection.Input;
                _with4.Add("CONTRACT_PK_IN", RefPk).Direction = ParameterDirection.Input;
                _with4.Add("CUSTOMERPK_IN", CustPk).Direction = ParameterDirection.Input;
                _with4.Add("SUPPLIERPK_IN", (string.IsNullOrEmpty(Supplier) ? "" : Supplier)).Direction = ParameterDirection.Input;
                _with4.Add("DATE_IN", (string.IsNullOrEmpty(ExpiryDt) ? "" : ExpiryDt)).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_PK_IN", Convert.ToInt32(LocFk)).Direction = ParameterDirection.Input;
                _with4.Add("EXCEL_IN", expExcel).Direction = ParameterDirection.Input;
                _with4.Add("PDF_IN", printPDF).Direction = ParameterDirection.Input;
                _with4.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with4.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("DETAIL1_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

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

        /// <summary>
        /// Fetches the parent report.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="ContractOf">The contract of.</param>
        /// <param name="ContractType">Type of the contract.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="RefPk">The reference pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="Supplier">The supplier.</param>
        /// <param name="ExpiryDt">The expiry dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <param name="printPDF">The print PDF.</param>
        /// <returns></returns>
        public DataSet FetchParentReport(string Business = "0", string ContractOf = "0", string ContractType = "", string LocFk = "0", int RefPk = 0, int CustPk = 0, string Supplier = "", string ExpiryDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 expExcel = 0, Int32 printPDF = 0)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with5 = objWK.MyCommand;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".CONTRACT_EXPIRY_PKG.CONTRACT_FETCH_PARENT";

                objWK.MyCommand.Parameters.Clear();
                var _with6 = objWK.MyCommand.Parameters;

                _with6.Add("BIZ_TYPE_IN", Convert.ToInt32(Business)).Direction = ParameterDirection.Input;
                _with6.Add("CONTRACT_OF_IN", Convert.ToInt32(ContractOf)).Direction = ParameterDirection.Input;
                _with6.Add("CONTRACT_TYPE_IN", (string.IsNullOrEmpty(ContractType) ? "" : ContractType)).Direction = ParameterDirection.Input;
                _with6.Add("CONTRACT_PK_IN", RefPk).Direction = ParameterDirection.Input;
                _with6.Add("CUSTOMERPK_IN", CustPk).Direction = ParameterDirection.Input;
                _with6.Add("SUPPLIERPK_IN", (string.IsNullOrEmpty(Supplier) ? "" : Supplier)).Direction = ParameterDirection.Input;
                _with6.Add("DATE_IN", (string.IsNullOrEmpty(ExpiryDt) ? "" : ExpiryDt)).Direction = ParameterDirection.Input;
                _with6.Add("LOCATION_PK_IN", Convert.ToInt32(LocFk)).Direction = ParameterDirection.Input;
                _with6.Add("EXCEL_IN", expExcel).Direction = ParameterDirection.Input;
                _with6.Add("PDF_IN", printPDF).Direction = ParameterDirection.Input;
                _with6.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with6.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with6.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with6.Add("DETAIL1_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

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

        #endregion "Fetch List"
    }
}