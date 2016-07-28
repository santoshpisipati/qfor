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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsApprovedVouchersAwaitingPayment : CommonFeatures
    {
        /// <summary>
        /// The object wf
        /// </summary>
        public WorkFlow objWF = new WorkFlow();

        #region "GetData"

        /// <summary>
        /// Gets the vouchers list.
        /// </summary>
        /// <param name="VendorPK">The vendor pk.</param>
        /// <param name="Status">The status.</param>
        /// <param name="Business_Type">Type of the business_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="Location">The location.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETD">The etd.</param>
        /// <param name="TradePK">The trade pk.</param>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="Vsl">The VSL.</param>
        /// <param name="VendorInvNr">The vendor inv nr.</param>
        /// <param name="InvDt">The inv dt.</param>
        /// <param name="SupplierRefNr">The supplier reference nr.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet GetVouchersList(Int32 VendorPK, Int16 Status, Int16 Business_Type, Int16 Process_Type, int Location = 0, string FromDt = "", string ToDt = "", string ETD = "", Int32 TradePK = 0, int JobPK = 0,
        string Vsl = "", string VendorInvNr = "", string InvDt = "", string SupplierRefNr = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SearchType = "S", Int32 flag = 0)
        {
            DataSet DS = null;
            //Conditional Part
            //-----------------------------------------------------------------------------------------------
            string strCondition = null;
            string strCondition1 = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string BusinessProcess = null;
            string VslFlight = null;
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

            BusinessProcess = (Business_Type == 2 ? "SEA" : "AIR");
            VslFlight = (Business_Type == 2 ? "VESSEL_NAME" : "FLIGHT_NO");
            BusinessProcess += (Process_Type == 1 ? "_EXP" : "_IMP");

            strCondition = "                           AND INVTBL.BUSINESS_TYPE = " + Business_Type;
            strCondition += "                           AND INVTBL.PROCESS_TYPE = " + Process_Type;
            if (Status >= 0)
            {
                strCondition += "                           AND INVTRNTBL.ELEMENT_APPROVED = " + Status;
            }
            if ((Location != null))
            {
                if (Location > 0)
                {
                    strCondition += "                           AND " + (Process_Type == 1 ? "PORT_POL" : "PORT_POD") + ".LOCATION_MST_FK = " + Location;
                }
            }
            strCondition1 = strCondition;

            if (((FromDt != null) & (ToDt != null)) & FromDt != " " & ToDt != " ")
            {
                strCondition += " AND TO_DATE(JOB.JOBCARD_DATE " + " ,'" + dateFormat + "') BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
            }
            else if (((FromDt != null)) & FromDt != " ")
            {
                strCondition += " AND TO_DATE(JOB.JOBCARD_DATE " + " ,'" + dateFormat + "') >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
            }
            else if (((ToDt != null)) & ToDt != " ")
            {
                strCondition += " AND TO_DATE(JOB.JOBCARD_DATE " + " ,'" + dateFormat + "') <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
            }

            if (((FromDt != null) & (ToDt != null)) & FromDt != " " & ToDt != " ")
            {
                strCondition1 += " AND TO_DATE(MJOB.MASTER_JC_DATE " + " ,'" + dateFormat + "') BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
            }
            else if (((FromDt != null)) & FromDt != " ")
            {
                strCondition1 += " AND TO_DATE(MJOB.MASTER_JC_DATE " + " ,'" + dateFormat + "') >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
            }
            else if (((ToDt != null)) & ToDt != " ")
            {
                strCondition1 += " AND TO_DATE(MJOB.MASTER_JC_DATE " + " ,'" + dateFormat + "') <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
            }

            if (VendorPK > 0 & SearchType == "C")
            {
                strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
            }
            else if (VendorPK > 0 & SearchType == "S")
            {
                strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
            }

            if (!string.IsNullOrEmpty(Vsl) & SearchType == "C")
            {
                if (Business_Type == 2)
                {
                    strCondition += " AND JOB.VESSEL_NAME ||" + "'-'" + " || JOB_EXP.VOYAGE like '%" + Vsl.Trim() + "%'";
                }
                else
                {
                    strCondition += " AND JOB.FLIGHT_NO like '%" + Vsl.Trim() + "%'";
                }
            }
            else if (!string.IsNullOrEmpty(Vsl) & SearchType == "S")
            {
                if (Business_Type == 2)
                {
                    strCondition += " AND JOB.VESSEL_NAME ||" + "'-'" + " || JOB_EXP.VOYAGE like '" + Vsl.Trim() + "%'";
                }
                else
                {
                    strCondition += " AND JOB.FLIGHT_NO like '" + Vsl.Trim() + "%'";
                }
            }

            if (VendorInvNr.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            else if (VendorInvNr.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }

            if (JobPK != 0 & SearchType == "C")
            {
                strCondition += " AND JOB.JOB_CARD_" + BusinessProcess + "_PK =" + JobPK;
            }
            else if (JobPK != 0 & SearchType == "S")
            {
                strCondition += " AND JOB.JOB_CARD_" + BusinessProcess + "_PK =" + JobPK;
            }

            if ((InvDt != null))
            {
                if (!string.IsNullOrEmpty(InvDt))
                {
                    strCondition += " AND TO_DATE(INVTBL.INVOICE_DATE,'" + dateFormat + "') = TO_DATE('" + InvDt + "','" + dateFormat + "')";
                }
            }
            if ((ETD != null))
            {
                if (!string.IsNullOrEmpty(ETD))
                {
                    strCondition += " AND TO_DATE(JOB.ETD_DATE,'" + dateFormat + "') = TO_DATE('" + ETD + "','" + dateFormat + "')";
                }
            }
            if ((ETD != null))
            {
                if (!string.IsNullOrEmpty(ETD))
                {
                    //Sea Export
                    if (Business_Type == 2 & Process_Type == 1)
                    {
                        strCondition1 += " AND TO_DATE(MJOB.POL_ETD,'" + dateFormat + "') = TO_DATE('" + ETD + "','" + dateFormat + "')";
                        //Sea Import
                    }
                    else if (Business_Type == 2 & Process_Type == 2)
                    {
                        strCondition1 += " AND TO_DATE(MJOB.POD_ETA,'" + dateFormat + "') = TO_DATE('" + ETD + "','" + dateFormat + "')";
                        //Air Export
                    }
                    else if (Business_Type == 1 & Process_Type == 1)
                    {
                        strCondition1 += " AND TO_DATE(MJOB.AOO_ETD,'" + dateFormat + "') = TO_DATE('" + ETD + "','" + dateFormat + "')";
                        //Air Import
                    }
                    else if (Business_Type == 1 & Process_Type == 2)
                    {
                        strCondition1 += " AND TO_DATE(MJOB.AOD_ETA,'" + dateFormat + "') = TO_DATE('" + ETD + "','" + dateFormat + "')";
                    }
                }
            }

            if (SupplierRefNr.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND  LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '%" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'";
            }
            else if (SupplierRefNr.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND  LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'";
            }

            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }
            //-----------------------------------------------------------------------------------------------

            //Get Columns
            //-----------------------------------------------------------------------------------------------
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append(" SELECT INVTBL.INV_SUPPLIER_PK,");
            sb.Append("                               INVTBL.INVOICE_REF_NO VOUCHERNO,");
            sb.Append("                               TO_CHAR(INVTBL.INVOICE_DATE, 'DD/MM/RRRR') VOUCHERDATE,");
            sb.Append("                               INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
            sb.Append("                               VMST.VENDOR_MST_PK SUPPLIER_PK,");
            sb.Append("                               VMST.VENDOR_ID SUPPLIER_ID,");
            sb.Append("                               VMST.VENDOR_NAME SUPPLIER_NAME,");
            sb.Append("                               PORT_POL.PORT_MST_PK POL_PK,");
            sb.Append("                               PORT_POL.PORT_ID POL_ID,");
            sb.Append("                               PORT_POD.PORT_MST_PK POD_PK,");
            sb.Append("                               PORT_POD.PORT_ID POD_ID,");
            sb.Append("                               TO_DATE(JOB.ETD_DATE,DATEFORMAT) ETD,");
            sb.Append(" (SUM(INVTRNTBL.ESTIMATED_AMT * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + BaseCurrFk + " ,INVTBL.SUPPLIER_INV_DT))) ESTIMATED_AMOUNT,");
            sb.Append(" (SUM(INVTRNTBL.PAYABLE_AMT * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + BaseCurrFk + " ,INVTBL.SUPPLIER_INV_DT))) ACTUAL_AMOUNT,");
            //'sb.Append("(SELECT TO_CHAR(SUM(NVL(TRN.ACTUAL_AMT, 0) + NVL(TRN.TAX_AMOUNT, 0)))")
            //sb.Append("(SELECT SUM(NVL(TRN.PAYABLE_AMT, 0))")
            //sb.Append("  FROM INV_SUPPLIER_TRN_TBL TRN")
            //sb.Append(" WHERE TRN.INV_SUPPLIER_TBL_FK = INVTBL.INV_SUPPLIER_PK) ACTUAL_AMOUNT,")
            sb.Append("                               TO_CHAR(INVTBL.SUPPLIER_DUE_DT,DATEFORMAT) SUPPLIER_DUE_DT,");
            sb.Append("                               TO_CHAR(INVTBL.APPROVED_DATE,DATEFORMAT) APPROVED_DATE,");
            sb.Append("                               CURR.CURRENCY_ID CUR,");
            sb.Append("                               DECODE(INVTRNTBL.ELEMENT_APPROVED,");
            sb.Append("                                      1,");
            sb.Append("                                      'Approved',");
            sb.Append("                                      2,");
            sb.Append("                                      'Reject',");
            sb.Append("                                      0,");
            sb.Append("                                      'Pending') STATUS,");
            sb.Append("                               0 SEL");
            sb.Append("                          FROM INV_SUPPLIER_TBL      INVTBL,");
            sb.Append("                               INV_SUPPLIER_TRN_TBL  INVTRNTBL,");
            sb.Append("                               VENDOR_MST_TBL        VMST,");
            sb.Append("                               JOB_CARD_" + BusinessProcess + "_TBL  JOB,");
            if (Business_Type == 2 & Process_Type == 1)
            {
                sb.Append("                               BOOKING_SEA_TBL BKG,");
            }
            else if (Business_Type == 1 & Process_Type == 1)
            {
                sb.Append("                               BOOKING_AIR_TBL BKG,");
            }
            sb.Append("                               PORT_MST_TBL PORT_POL,");
            sb.Append("                               PORT_MST_TBL PORT_POD,");
            sb.Append("                               CURRENCY_TYPE_MST_TBL CURR,");
            sb.Append("                               USER_MST_TBL          USRTBL,");
            sb.Append("                               PAYMENT_TRN_TBL       PAY");
            sb.Append("                         WHERE INVTBL.INV_SUPPLIER_PK =");
            sb.Append("                               INVTRNTBL.INV_SUPPLIER_TBL_FK ");
            sb.Append("                               AND INVTBL.INV_SUPPLIER_PK=PAY.INV_SUPPLIER_TBL_FK(+) ");
            sb.Append("                               AND PAY.PAYMENTS_TRN_PK IS NULL ");
            sb.Append("                           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
            sb.Append("                           AND INVTRNTBL.JOBCARD_REF_NO =JOB.JOBCARD_REF_NO");
            if (Business_Type == 2 & Process_Type == 1)
            {
                sb.Append("                           AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                sb.Append("                           AND BKG.PORT_MST_POL_FK = PORT_POL.PORT_MST_PK");
                sb.Append("                           AND BKG.PORT_MST_POD_FK = PORT_POD.PORT_MST_PK");
            }
            else if (Business_Type == 1 & Process_Type == 1)
            {
                sb.Append("                           AND JOB.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK");
                sb.Append("                           AND BKG.PORT_MST_POL_FK = PORT_POL.PORT_MST_PK");
                sb.Append("                           AND BKG.PORT_MST_POD_FK = PORT_POD.PORT_MST_PK");
            }
            else if (Process_Type == 2)
            {
                sb.Append("                           AND JOB.PORT_MST_POL_FK = PORT_POL.PORT_MST_PK");
                sb.Append("                           AND JOB.PORT_MST_POD_FK = PORT_POD.PORT_MST_PK");
            }
            sb.Append("                           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
            sb.Append("                           AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK ");
            sb.Append(" " + strCondition + " ");
            sb.Append(" GROUP BY INVTBL.INV_SUPPLIER_PK,");
            sb.Append("                               INVTBL.INVOICE_REF_NO,");
            sb.Append("                               INVTBL.INVOICE_DATE,");
            sb.Append("                               INVTBL.SUPPLIER_INV_NO,");
            sb.Append("                               VMST.VENDOR_MST_PK,");
            sb.Append("                               VMST.VENDOR_ID,");
            sb.Append("                               VMST.VENDOR_NAME,");
            sb.Append("                               PORT_POL.PORT_MST_PK,");
            sb.Append("                               PORT_POL.PORT_ID,");
            sb.Append("                               PORT_POD.PORT_MST_PK,");
            sb.Append("                               PORT_POD.PORT_ID,");
            sb.Append("                               JOB.ETD_DATE,");
            sb.Append("                               INVTBL.SUPPLIER_DUE_DT,");
            sb.Append("                               INVTBL.APPROVED_DATE,");
            sb.Append("                               CURR.CURRENCY_ID,");
            sb.Append("                               INVTRNTBL.ELEMENT_APPROVED ");
            sb.Append("                        UNION ");
            sb.Append("                        SELECT DISTINCT INVTBL.INV_SUPPLIER_PK,");
            sb.Append("                                        INVTBL.INVOICE_REF_NO VOUCHERNO,");
            sb.Append("                                        TO_CHAR(INVTBL.INVOICE_DATE,");
            sb.Append("                                                DATEFORMAT) VOUCHERDATE,");
            sb.Append("                                        INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
            sb.Append("                                        VMST.VENDOR_MST_PK SUPPLIER_PK,");
            sb.Append("                                        VMST.VENDOR_ID SUPPLIER_ID,");
            sb.Append("                                        VMST.VENDOR_NAME SUPPLIER_NAME,");
            sb.Append("                                        PORT_POL.PORT_MST_PK POL_PK,");
            sb.Append("                                        PORT_POL.PORT_ID POL_ID,");
            sb.Append("                                        PORT_POD.PORT_MST_PK POD_PK,");
            sb.Append("                                        PORT_POD.PORT_ID POD_ID,");
            if (Business_Type == 2 & Process_Type == 1)
            {
                sb.Append("                                        TO_DATE(MJOB.POL_ETD,DATEFORMAT) ETD,");
            }
            else if (Business_Type == 2 & Process_Type == 2)
            {
                sb.Append("                                        TO_DATE(MJOB.POD_ETA,DATEFORMAT) ETD,");
            }
            else if (Business_Type == 1 & Process_Type == 1)
            {
                sb.Append("                                        TO_DATE(MJOB.AOO_ETD,DATEFORMAT) ETD,");
            }
            else if (Business_Type == 1 & Process_Type == 2)
            {
                sb.Append("                                        TO_DATE(MJOB.AOD_ETA,DATEFORMAT) ETD,");
            }
            sb.Append(" (SUM(INVTRNTBL.ESTIMATED_AMT * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + BaseCurrFk + " ,INVTBL.SUPPLIER_INV_DT))) ESTIMATED_AMOUNT,");
            sb.Append(" (SUM(INVTRNTBL.PAYABLE_AMT * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + BaseCurrFk + " ,INVTBL.SUPPLIER_INV_DT))) ACTUAL_AMOUNT,");
            //'sb.Append("(SELECT TO_CHAR(SUM(NVL(TRN.ACTUAL_AMT, 0) + NVL(TRN.TAX_AMOUNT, 0)), 2)")
            //sb.Append("(SELECT SUM(NVL(TRN.PAYABLE_AMT, 0))")
            //sb.Append("  FROM INV_SUPPLIER_TRN_TBL TRN")
            //sb.Append(" WHERE TRN.INV_SUPPLIER_TBL_FK = INVTBL.INV_SUPPLIER_PK) ACTUAL_AMOUNT,")
            sb.Append("                                        TO_CHAR(INVTBL.SUPPLIER_DUE_DT,DATEFORMAT) SUPPLIER_DUE_DT,");
            sb.Append("                                        TO_CHAR(INVTBL.APPROVED_DATE,DATEFORMAT) APPROVED_DATE,");
            sb.Append("                                        CURR.CURRENCY_ID CUR,");
            sb.Append("                                        DECODE(INVTRNTBL.ELEMENT_APPROVED,");
            sb.Append("                                               1,");
            sb.Append("                                               'Approved',");
            sb.Append("                                               2,");
            sb.Append("                                               'Reject',");
            sb.Append("                                               0,");
            sb.Append("                                               'Pending') STATUS,");
            sb.Append("                                        0 SEL");
            sb.Append("                          FROM INV_SUPPLIER_TBL      INVTBL,");
            sb.Append("                               INV_SUPPLIER_TRN_TBL  INVTRNTBL,");
            sb.Append("                               VENDOR_MST_TBL        VMST,");
            sb.Append("                               PORT_MST_TBL PORT_POL,");
            sb.Append("                               PORT_MST_TBL PORT_POD,");
            sb.Append("                               CURRENCY_TYPE_MST_TBL CURR,");
            sb.Append("                               MASTER_JC_" + BusinessProcess + "_TBL MJOB,");
            sb.Append("                               USER_MST_TBL          USRTBL,");
            sb.Append("                               PAYMENT_TRN_TBL       PAY");
            sb.Append("                         WHERE INVTBL.INV_SUPPLIER_PK =");
            sb.Append("                               INVTRNTBL.INV_SUPPLIER_TBL_FK ");
            sb.Append("                               AND INVTBL.INV_SUPPLIER_PK=PAY.INV_SUPPLIER_TBL_FK(+) ");
            sb.Append("                               AND PAY.PAYMENTS_TRN_PK IS NULL ");
            sb.Append("                           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
            sb.Append("                           AND INVTRNTBL.JOBCARD_REF_NO =MJOB.MASTER_JC_REF_NO");
            sb.Append("                           AND MJOB.PORT_MST_POL_FK = PORT_POL.PORT_MST_PK");
            sb.Append("                           AND MJOB.PORT_MST_POD_FK = PORT_POD.PORT_MST_PK    ");
            sb.Append("                           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
            sb.Append("                           AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
            sb.Append(" " + strCondition1 + " ");
            sb.Append(" GROUP BY INVTBL.INV_SUPPLIER_PK,");
            sb.Append("                               INVTBL.INVOICE_REF_NO,");
            sb.Append("                               INVTBL.INVOICE_DATE,");
            sb.Append("                               INVTBL.SUPPLIER_INV_NO,");
            sb.Append("                               VMST.VENDOR_MST_PK,");
            sb.Append("                               VMST.VENDOR_ID,");
            sb.Append("                               VMST.VENDOR_NAME,");
            sb.Append("                               PORT_POL.PORT_MST_PK,");
            sb.Append("                               PORT_POL.PORT_ID,");
            sb.Append("                               PORT_POD.PORT_MST_PK,");
            sb.Append("                               PORT_POD.PORT_ID,");
            if (Business_Type == 2 & Process_Type == 1)
            {
                sb.Append("                                        MJOB.POL_ETD,");
            }
            else if (Business_Type == 2 & Process_Type == 2)
            {
                sb.Append("                                        MJOB.POD_ETA,");
            }
            else if (Business_Type == 1 & Process_Type == 1)
            {
                sb.Append("                                        MJOB.AOO_ETD,");
            }
            else if (Business_Type == 1 & Process_Type == 2)
            {
                sb.Append("                                        MJOB.AOD_ETA,");
            }
            sb.Append("                               INVTBL.SUPPLIER_DUE_DT,");
            sb.Append("                               INVTBL.APPROVED_DATE,");
            sb.Append("                               CURR.CURRENCY_ID,");
            sb.Append("                               INVTRNTBL.ELEMENT_APPROVED ");

            //Record Calculation
            //---------------------------------------------------------------------------------
            DS = objWF.GetDataSet("SELECT * FROM ( " + sb.ToString() + " )");
            TotalRecords = (Int32)DS.Tables[0].Rows.Count;
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
            //---------------------------------------------------------------------------------
            string QryString = sb.ToString();
            //"SELECT * FROM ( " & sb.ToString & " ) ORDER BY TO_DATE(APPROVED_DATE,DATEFORMAT) DESC, VOUCHERNO DESC "
            sb = new StringBuilder();
            sb.Append("SELECT * FROM ( ");
            sb.Append(" SELECT ROWNUM SrNO , MAIN.* FROM ( ");
            sb.Append("   SELECT Q.INV_SUPPLIER_PK,");
            sb.Append("       Q.VOUCHERNO,");
            sb.Append("       Q.VOUCHERDATE,");
            sb.Append("       Q.SUPPLIER_REF_NR,");
            sb.Append("       Q.SUPPLIER_PK,");
            sb.Append("       Q.SUPPLIER_ID,");
            sb.Append("       Q.SUPPLIER_NAME,");
            sb.Append("       Q.POL_PK,");
            sb.Append("       Q.POL_ID,");
            sb.Append("       Q.POD_PK,");
            sb.Append("       Q.POD_ID,");
            sb.Append("       Q.ETD,");
            sb.Append("       Q.ESTIMATED_AMOUNT,");
            sb.Append("       Q.ACTUAL_AMOUNT,");
            sb.Append("       Q.SUPPLIER_DUE_DT,");
            sb.Append("       Q.APPROVED_DATE,");
            sb.Append("       Q.CUR,");
            sb.Append("       Q.STATUS,");
            sb.Append("       Q.SEL FROM( " + QryString + "  ");
            sb.Append("     ) Q ORDER BY TO_DATE(Q.APPROVED_DATE, DATEFORMAT) DESC, Q.VOUCHERNO ) MAIN ) ");
            sb.Append(" WHERE SrNO  Between " + start + " and " + last);

            try
            {
                DS = objWF.GetDataSet(sb.ToString());
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
            return DS;
        }

        #endregion "GetData"

        #region "FETCH DATA"

        /// <summary>
        /// Fetches the voucher data.
        /// </summary>
        /// <param name="VendorFK">The vendor fk.</param>
        /// <param name="Business_Type">Type of the business_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="Location">The location.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchVoucherData(string VendorFK = "", string Business_Type = "", string Process_Type = "", int Location = 0, string FromDt = "", string ToDt = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("VENDOR_MST_FK_IN", (string.IsNullOrEmpty(VendorFK) ? "" : VendorFK)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_PK_IN", Location).Direction = ParameterDirection.Input;
                _with1.Add("CURRENCY_FK_IN", BaseCurrFk).Direction = ParameterDirection.Input;
                _with1.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with1.Add("TODATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", Business_Type).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with1.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_VOU_PENDING_FORPAY_PKG", "FETCH_VOU_PENDING_FORPAY");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FETCH DATA"

        #region "Fetch Location of User Login"

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
                //Manjunath  PTS ID:Sep-02  26/09/2011
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

        #endregion "Fetch Location of User Login"

        #region "FETCH REC VS PAYABLE DATA"

        /// <summary>
        /// Fetches the record pay data.
        /// </summary>
        /// <param name="ChkONLD">The CHK onld.</param>
        /// <param name="FROM_DATE">The fro m_ date.</param>
        /// <param name="TO_DATE">The t o_ date.</param>
        /// <param name="POL_POD">The po l_ pod.</param>
        /// <param name="CUSTOMER">The customer.</param>
        /// <param name="LOCATION">The location.</param>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Excel">The excel.</param>
        /// <returns></returns>
        public DataSet FetchRecPayData(Int32 ChkONLD, string FROM_DATE = "", string TO_DATE = "", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string Jobpk = "", string BizType = "", string Process = "", Int32 CurrentPage = 1,
        Int32 TotalPage = 0, int Excel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;
                _with2.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;
                _with2.Add("CURRENCY_IN", BaseCurrFk).Direction = ParameterDirection.Input;
                _with2.Add("JCPK_IN", (string.IsNullOrEmpty(Jobpk) ? "" : Jobpk)).Direction = ParameterDirection.Input;
                _with2.Add("FROM_DATE", (string.IsNullOrEmpty(FROM_DATE) ? "" : FROM_DATE)).Direction = ParameterDirection.Input;
                _with2.Add("TO_DATE", (string.IsNullOrEmpty(TO_DATE) ? "" : TO_DATE)).Direction = ParameterDirection.Input;
                _with2.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with2.Add("EXCEL_IN", Excel).Direction = ParameterDirection.Input;
                _with2.Add("ONPAGELD_IN", ChkONLD).Direction = ParameterDirection.Input;
                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("RECPAY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_REC_PAYABLES_PKG", "FETCH_REC_PAY");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FETCH REC VS PAYABLE DATA"

        #region "FETCH POP UP DETAILS "

        /// <summary>
        /// Fetches the pop updetails.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="PopUpType">Type of the pop up.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <returns></returns>
        public DataSet FetchPopUpdetails(string Jobpk = "", string BizType = "", string Process = "", string PopUpType = "", string CustPK = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("JCPK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with3.Add("CURRENCY_IN", BaseCurrFk).Direction = ParameterDirection.Input;
                _with3.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with3.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with3.Add("POPUP_TYPE_IN", PopUpType).Direction = ParameterDirection.Input;
                _with3.Add("CUSTOMER_PK_IN", CustPK).Direction = ParameterDirection.Input;
                _with3.Add("RECPAY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_REC_PAYABLES_PKG", "FETCH_POPUP_DETAILS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FETCH POP UP DETAILS "

        #region "FETCH POP UP DETAILS "

        /// <summary>
        /// Fetches the current details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchCurDetails(string Jobpk = "", string BizType = "", string Process = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("JCPK_IN", (string.IsNullOrEmpty(Jobpk) ? "" : Jobpk)).Direction = ParameterDirection.Input;
                _with4.Add("CURRENCY_IN", BaseCurrFk).Direction = ParameterDirection.Input;
                _with4.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with4.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with4.Add("RECPAY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_REC_PAYABLES_PKG", "FETCH_CUR_DETAILS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FETCH POP UP DETAILS "
    }
}