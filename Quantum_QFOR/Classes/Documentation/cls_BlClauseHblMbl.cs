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
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_BlClauseForHblMbl : CommonFeatures
    {

        #region "FetchBlClausesForHBL Function"
        public DataSet FetchBlClausesForHBL(string strBlClause = "", Int32 ClauseTypeFlag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, string PodPk = "0", string CmmPk = "0", long HblPk = 0, string QuotationDt = "", string FormFlag = "")
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            //1-GENERAL 
            //2-Quotation
            //3-Booking
            //4-HBL/HAWB
            //5-MBL/MAWB''M.MBL_EXP_TBL_PK
            //6-CAN
            //7-DO
            //8-Invoice
            //9-Invoice To CB Agent
            //10-Invoice To Load Agent
            //11-Invoice To DP Agent
            //12-Customer Contract
            //13-SRR
            //'Quotation
            if (ClauseTypeFlag == 2)
            {
                strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                strCondition += " BL_CLAUSE_TBL     BLMST,";
                strCondition += " QUOTATION_MST_TBL QST";
                strCondition += " WHERE QST.QUOTATION_MST_PK = HBL.HBL_EXP_TBL_FK";
                strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 2";
                //'3-Booking
            }
            else if (ClauseTypeFlag == 3)
            {
                strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                strCondition += " BL_CLAUSE_TBL     BLMST,";
                strCondition += " BOOKING_MST_TBL   B";
                strCondition += " WHERE B.BOOKING_MST_PK = HBL.HBL_EXP_TBL_FK";
                strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 3";
                //'4-HBL/HAWB
            }
            else if (ClauseTypeFlag == 4)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " HBL_EXP_TBL HET";
                    strCondition += " WHERE HET.HBL_EXP_TBL_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 4";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " HAWB_EXP_TBL HWET";
                    strCondition += " WHERE HWET.HAWB_EXP_TBL_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 4";
                }
                //'5-MBL/MAWB
            }
            else if (ClauseTypeFlag == 5)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " MBL_EXP_TBL M";
                    strCondition += " WHERE M.MBL_EXP_TBL_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 5";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " MAWB_EXP_TBL M";
                    strCondition += " WHERE M.MAWB_EXP_TBL_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 5";
                }
                //'6-CAN
            }
            else if (ClauseTypeFlag == 6)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " JOB_CARD_TRN JC";
                    strCondition += " WHERE JC.JOB_CARD_TRN_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 6";
                    strCondition += " AND JC.BUSINESS_TYPE = 2 AND JC.PROCESS_TYPE = 2 ";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " JOB_CARD_TRN JC";
                    strCondition += " WHERE JC.JOB_CARD_TRN_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 6 ";
                    strCondition += " AND JC.BUSINESS_TYPE = 1 AND JC.PROCESS_TYPE = 2 ";
                }
                //'7-DO
            }
            else if (ClauseTypeFlag == 7)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " DELIVERY_ORDER_MST_TBL D";
                    strCondition += " WHERE D.DELIVERY_ORDER_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 7";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " DELIVERY_ORDER_MST_TBL D";
                    strCondition += " WHERE D.DELIVERY_ORDER_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 7";
                }
                //'8-Invoice
            }
            else if (ClauseTypeFlag == 8)
            {
                strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                strCondition += " BL_CLAUSE_TBL     BLMST,";
                strCondition += " CONSOL_INVOICE_TBL C";
                strCondition += " WHERE C.CONSOL_INVOICE_PK = HBL.HBL_EXP_TBL_FK";
                strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 8";

                //'9-Invoice To CB Agent
            }
            else if (ClauseTypeFlag == 9)
            {
                if (FormFlag == "SeaCBExp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_TBL I";
                    strCondition += " WHERE I.INV_AGENT_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 9";

                }
                else if (FormFlag == "SeaCBImp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_TBL I";
                    strCondition += " WHERE I.INV_AGENT_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 9";

                }
                else if (FormFlag == "AirCBExp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_TBL I";
                    strCondition += " WHERE I.INV_AGENT_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 9";

                }
                else if (FormFlag == "AirCBImp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_TBL I";
                    strCondition += " WHERE I.INV_AGENT_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 9";
                }
                //'10-Invoice To  Load Agent IMP
            }
            else if (ClauseTypeFlag == 10)
            {
                if (FormFlag == "SeaLAImp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_TBL I";
                    strCondition += " WHERE I.INV_AGENT_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 10";
                }
                else if (FormFlag == "AirLAImp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_TBL I";
                    strCondition += " WHERE I.INV_AGENT_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 10";
                }
                //'11-Invoice To DP Agent
            }
            else if (ClauseTypeFlag == 11)
            {
                //'Sea
                if (FormFlag == "SeaDPExp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_TBL I";
                    strCondition += " WHERE I.INV_AGENT_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 11";

                    //'11-AirDPExp
                }
                else if (FormFlag == "AirDPExp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_TBL I";
                    strCondition += " WHERE I.INV_AGENT_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 11";
                }

                //'12-Customer Contract
            }
            else if (ClauseTypeFlag == 12)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " CONT_CUST_SEA_TBL C";
                    strCondition += " WHERE C.CONT_CUST_SEA_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 12";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " CONT_CUST_AIR_TBL C";
                    strCondition += " WHERE C.CONT_CUST_AIR_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 12";
                }
                //'13-SRR
            }
            else if (ClauseTypeFlag == 13)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " SRR_SEA_TBL S";
                    strCondition += " WHERE S.SRR_SEA_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 13";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " SRR_AIR_TBL S";
                    strCondition += " WHERE S.SRR_AIR_PK  = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 13";
                }
            }

            //'2-Quotation
            if (HblPk > 0 & ClauseTypeFlag == 2)
            {
                strCondition += "  AND QST.QUOTATION_MST_PK = " + HblPk;
                //'3-Booking
            }
            else if (HblPk > 0 & ClauseTypeFlag == 3)
            {
                strCondition += "  AND B.BOOKING_MST_PK  = " + HblPk;
                //'4-HBL/HAWB
            }
            else if (HblPk > 0 & ClauseTypeFlag == 4)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND HET.HBL_EXP_TBL_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND HWET.HAWB_EXP_TBL_PK = " + HblPk;
                }
                //'5-MBL/MAWB
            }
            else if (HblPk > 0 & ClauseTypeFlag == 5)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND M.MBL_EXP_TBL_PK  = " + HblPk;
                }
                else
                {
                    strCondition += "  AND M.MAWB_EXP_TBL_PK  = " + HblPk;
                }
                //'6-CAN
            }
            else if (HblPk > 0 & ClauseTypeFlag == 6)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND JC.JOB_CARD_TRN_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND JC.JOB_CARD_TRN_PK = " + HblPk;
                }
                //'7-CAN
            }
            else if (HblPk > 0 & ClauseTypeFlag == 7)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND D.DELIVERY_ORDER_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND D.DELIVERY_ORDER_PK = " + HblPk;
                }

                //'8-Invoice
            }
            else if (HblPk > 0 & ClauseTypeFlag == 8)
            {
                strCondition += "  AND C.CONSOL_INVOICE_PK = " + HblPk;

            }
            else if (HblPk > 0 & ClauseTypeFlag == 9)
            {
                if (FormFlag == "SeaCBExp")
                {
                    strCondition += "  AND I.INV_AGENT_PK = " + HblPk;
                }
                else if (FormFlag == "SeaCBImp")
                {
                    strCondition += "  AND I.INV_AGENT_PK = " + HblPk;
                }
                else if (FormFlag == "AirCBExp")
                {
                    strCondition += "  AND I.INV_AGENT_PK = " + HblPk;
                }
                else if (FormFlag == "AirCBImp")
                {
                    strCondition += "  AND I.INV_AGENT_PK = " + HblPk;
                }
            }
            else if (HblPk > 0 & ClauseTypeFlag == 10)
            {
                if (FormFlag == "SeaLAImp")
                {
                    strCondition += "  AND I.INV_AGENT_PK = " + HblPk;
                }
                else if (FormFlag == "AirLAImp")
                {
                    strCondition += "  AND I.INV_AGENT_PK = " + HblPk;
                }

            }
            else if (HblPk > 0 & ClauseTypeFlag == 11)
            {
                if (FormFlag == "SeaDPExp")
                {
                    strCondition += "  AND I.INV_AGENT_PK = " + HblPk;
                }
                else if (FormFlag == "AirDPExp")
                {
                    strCondition += "  AND I.INV_AGENT_PK = " + HblPk;
                }

                //'12-Customer Contarct
            }
            else if (HblPk > 0 & ClauseTypeFlag == 12)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND C.CONT_CUST_SEA_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND C.CONT_CUST_AIR_PK = " + HblPk;
                }
                //'13-SRR
            }
            else if (HblPk > 0 & ClauseTypeFlag == 13)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND S.SRR_SEA_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND S.SRR_AIR_PK = " + HblPk;
                }
            }
            if (strBlClause != "FormLookup" & !string.IsNullOrEmpty(strBlClause))
            {
                if (strBlClause.Trim().Length > 0)
                {
                    strCondition += " AND (UPPER(HBL.BL_DESCRIPTION) LIKE '%" + strBlClause.ToUpper().Replace("'", "''") + "%' OR UPPER(HBL.REFERENCE_NR) LIKE '%" + strBlClause.ToUpper().Replace("'", "''") + "%')";
                }
            }
            ///'''''''''''
            sb.Append("SELECT COUNT(*)");
            sb.Append("  FROM (SELECT ROWNUM SR_NO, Q.*");
            sb.Append("          FROM (SELECT HBL.HBL_BL_CLAUSE_TBL_PK,");
            sb.Append("                        HBL.REFERENCE_NR,");
            sb.Append("                        HBL.BL_DESCRIPTION,");
            sb.Append("                        HBL.PRIORITY,");
            sb.Append("                        HBL.BL_CLAUSE_FK         \"PK\",");
            sb.Append("                        CASE WHEN HBL.ACTIVE_FLAG = 1 THEN ");
            sb.Append("                        'true' ELSE 'false' END \"sel\"");
            sb.Append("" + strCondition + "");
            sb.Append("                ");
            if (string.IsNullOrEmpty(strBlClause))
            {
                //' For Print Report not to display all the clause
                sb.Append("   AND HBL.ACTIVE_FLAG = 1");
            }
            else
            {
                sb.Append("                UNION");
                sb.Append("                ");
                sb.Append("                SELECT 0 HBL_BL_CLAUSE_TBL_PK,");
                sb.Append("                       BLCT.REFERENCE_NR,");
                sb.Append("                       BLCT.BL_DESCRIPTION,");
                sb.Append("                       BLCT.PRIORITY,");
                sb.Append("                       BLCT.BL_CLAUSE_PK   \"PK\",");
                sb.Append("                       'false'             \"sel\"");
                sb.Append("                  FROM BL_CLAUSE_TBL BLCT");
                sb.Append("                 WHERE BLCT.ACTIVE_FLAG = '1'");
                if (!string.IsNullOrEmpty(QuotationDt) & QuotationDt != "null")
                {
                    sb.Append("                AND TO_DATE('" + QuotationDt + "','dd/MM/yyyy') BETWEEN BLCT.VALID_FROM AND BLCT.VALID_TO ");
                }
                if (strBlClause != "FormLookup" & !string.IsNullOrEmpty(strBlClause))
                {
                    if (strBlClause.Trim().Length > 0)
                    {
                        sb.Append("               AND (UPPER(BLCT.BL_DESCRIPTION) LIKE '%" + strBlClause.ToUpper().Replace("'", "''") + "%' OR UPPER(BLCT.REFERENCE_NR) LIKE '%" + strBlClause.ToUpper().Replace("'", "''") + "%')");
                    }
                }
                sb.Append("                   AND (BLCT.CLAUSES_TYPE_MST_FK = 1 ");
                sb.Append("                       OR BLCT.CLAUSES_TYPE_MST_FK = " + ClauseTypeFlag + ")");
                sb.Append("                   AND (BLCT.BL_CLAUSE_PK IN");
                sb.Append("                       (SELECT DISTINCT BL.BL_CLAUSE_FK");
                sb.Append("                           FROM BL_CLAUSE_TRN BL");
                sb.Append("                          WHERE BL.PORT_MST_FK IN (" + PodPk + ")");
                sb.Append("                            AND BL.BL_CLAUSE_FK NOT IN");
                sb.Append("                                (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                                   FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                                  WHERE H.HBL_EXP_TBL_FK = " + HblPk + ")) OR");
                sb.Append("                       BLCT.BL_CLAUSE_PK IN");
                sb.Append("                       (SELECT DISTINCT BC.CLAUSE_MST_FK");
                sb.Append("                           FROM BL_CLAUSE_COMM_TRN BC");
                sb.Append("                          WHERE BC.COMMODITY_MST_FK IN (" + CmmPk + ")");
                sb.Append("                            AND BC.CLAUSE_MST_FK NOT IN");
                sb.Append("                                (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                                   FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                                  WHERE H.HBL_EXP_TBL_FK = " + HblPk + ")) ");
                sb.Append("                     AND(BLCT.CLAUSES_TYPE_MST_FK = 1 OR");
                sb.Append("                       BLCT.CLAUSES_TYPE_MST_FK = " + ClauseTypeFlag + ") ");
                sb.Append("                     AND BLCT.BL_CLAUSE_PK NOT IN");
                sb.Append("                       (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                           FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                          WHERE H.HBL_EXP_TBL_FK = " + HblPk + "))ORDER BY \"sel\", BL_DESCRIPTION ");
            }
            sb.Append("         ) Q)");
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            TotalPage = TotalRecords / M_MasterPageSize;

            if (TotalRecords % M_MasterPageSize != 0)
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

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            sb.Length = 0;
            sb.Append("SELECT * ");
            sb.Append("  FROM (SELECT ROWNUM SR_NO, Q.*");
            sb.Append("          FROM (SELECT HBL.HBL_BL_CLAUSE_TBL_PK,");
            sb.Append("                        HBL.REFERENCE_NR,");
            sb.Append("                        HBL.BL_DESCRIPTION,");
            sb.Append("                        HBL.PRIORITY,");
            sb.Append("                        HBL.BL_CLAUSE_FK         \"PK\",");
            //sb.Append("                        HBL.ACTIVE_FLAG          ""sel""")
            sb.Append("                        CASE WHEN HBL.ACTIVE_FLAG = 1 THEN ");
            sb.Append("                        'true' ELSE 'false' END \"sel\"");
            sb.Append("" + strCondition + "");
            sb.Append("                ");
            if (string.IsNullOrEmpty(strBlClause))
            {
                //' For Print Report not to display all the clause
                sb.Append("   AND HBL.ACTIVE_FLAG = 1");
            }
            else
            {
                sb.Append("                UNION");
                sb.Append("                ");
                sb.Append("                SELECT 0 HBL_BL_CLAUSE_TBL_PK,");
                sb.Append("                       BLCT.REFERENCE_NR,");
                sb.Append("                       BLCT.BL_DESCRIPTION,");
                sb.Append("                       BLCT.PRIORITY,");
                sb.Append("                       BLCT.BL_CLAUSE_PK   \"PK\",");
                sb.Append("                       'false'             \"sel\"");
                sb.Append("                  FROM BL_CLAUSE_TBL BLCT");
                sb.Append("                 WHERE BLCT.ACTIVE_FLAG = '1'");
                if (!string.IsNullOrEmpty(QuotationDt) & QuotationDt != "null")
                {
                    sb.Append("                AND TO_DATE('" + QuotationDt + "','dd/MM/yyyy') BETWEEN BLCT.VALID_FROM AND BLCT.VALID_TO ");
                }
                if (strBlClause != "FormLookup" & !string.IsNullOrEmpty(strBlClause))
                {
                    if (strBlClause.Trim().Length > 0)
                    {
                        sb.Append("               AND (UPPER(BLCT.BL_DESCRIPTION) LIKE '%" + strBlClause.ToUpper().Replace("'", "''") + "%' OR UPPER(BLCT.REFERENCE_NR) LIKE '%" + strBlClause.ToUpper().Replace("'", "''") + "%')");
                    }
                }
                sb.Append("                   AND (BLCT.CLAUSES_TYPE_MST_FK = 1 ");
                sb.Append("                       OR BLCT.CLAUSES_TYPE_MST_FK = " + ClauseTypeFlag + ")");
                sb.Append("                   AND (BLCT.BL_CLAUSE_PK IN");
                sb.Append("                       (SELECT DISTINCT BL.BL_CLAUSE_FK");
                sb.Append("                           FROM BL_CLAUSE_TRN BL");
                sb.Append("                          WHERE BL.PORT_MST_FK IN (" + PodPk + ")");
                sb.Append("                            AND BL.BL_CLAUSE_FK NOT IN");
                sb.Append("                                (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                                   FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                                  WHERE H.HBL_EXP_TBL_FK = " + HblPk + ")) OR");
                sb.Append("                       BLCT.BL_CLAUSE_PK IN");
                sb.Append("                       (SELECT DISTINCT BC.CLAUSE_MST_FK");
                sb.Append("                           FROM BL_CLAUSE_COMM_TRN BC");
                sb.Append("                          WHERE BC.COMMODITY_MST_FK IN (" + CmmPk + ")");
                sb.Append("                            AND BC.CLAUSE_MST_FK NOT IN");
                sb.Append("                                (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                                   FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                                  WHERE H.HBL_EXP_TBL_FK = " + HblPk + ")) ");
                sb.Append("                     AND(BLCT.CLAUSES_TYPE_MST_FK = 1 OR");
                sb.Append("                       BLCT.CLAUSES_TYPE_MST_FK = " + ClauseTypeFlag + ") ");
                sb.Append("                     AND BLCT.BL_CLAUSE_PK NOT IN");
                sb.Append("                       (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                           FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                          WHERE H.HBL_EXP_TBL_FK = " + HblPk + "))ORDER BY \"sel\", BL_DESCRIPTION ");

            }
            sb.Append("      ) Q) WHERE SR_NO  BETWEEN " + start + " AND " + last);

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
        #endregion

        #region "FetchBlClausesForMblWithHblChecked Function"
        public DataSet FetchBlClausesForMblWithHblChecked(string strBlClause = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string PodPk = "", long HblPk = 0, long RefPk = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;

            if (strBlClause.Trim().Length > 0)
            {
                strCondition += " AND (UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                strCondition += " or UPPER(MBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%')";
            }


            ///'''''''''''
            strSQL = " select count(*) from (";
            strSQL += " SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL += " (SELECT ";
            strSQL += " 0 MBL_BL_CLAUSE_TBL_PK,";
            strSQL += " '' REFERENCE_NR,";
            strSQL += " BLMST.BL_DESCRIPTION,";
            strSQL += " 0 PRIORITY,";
            strSQL += " BLMST.BL_CLAUSE_PK \"PK\",";
            strSQL += " 'false' \"sel\"";
            strSQL += " FROM";
            strSQL += " BL_CLAUSE_TBL BLMST,";
            strSQL += " BL_CLAUSE_TRN BLTRN,";
            strSQL += " PORT_MST_TBL POD";
            strSQL += " WHERE";
            strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
            strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
            strSQL += " AND POD.PORT_MST_PK = " + PodPk;
            strSQL += " AND BLMST.ACTIVE_FLAG = 1";
            strSQL += " UNION";
            strSQL += " SELECT";
            strSQL += " MBL.MBL_BL_CLAUSE_TBL_PK,";
            strSQL += " '' REFERENCE_NR,";
            strSQL += " MBL.BL_DESCRIPTION,";
            strSQL += " 0 PRIORITY,";
            strSQL += " MBL.BL_CLAUSE_FK \"PK\",";
            strSQL += " 'true' \"sel\"";
            strSQL += " FROM";
            strSQL += " MBL_BL_CLAUSE_TBL MBL,";
            strSQL += " BL_CLAUSE_TBL BLMST,";
            strSQL += " BL_CLAUSE_TRN BLTRN,";
            strSQL += " PORT_MST_TBL POD";
            strSQL += " WHERE";
            strSQL += " MBL.BL_CLAUSE_FK(+) = BLMST.BL_CLAUSE_PK";
            strSQL += " and blmst.Active_Flag =1";
            strSQL += " AND BLTRN.PORT_MST_FK = pod.port_mst_pk";
            strSQL += " and POD.PORT_MST_PK = " + PodPk;
            strSQL += " AND bltrn.bl_clause_fk= MBL.BL_CLAUSE_FK";
            strSQL += " And blmst.bl_clause_pk = bltrn.bl_clause_fk ";
            if (HblPk > 0)
            {
                strSQL += "  and MBL.MBL_EXP_TBL_FK(+)=" + HblPk;
            }
            strSQL += strCondition;
            strSQL += " UNION";
            strSQL += " SELECT";
            strSQL += " MBL.MBL_BL_CLAUSE_TBL_PK,";
            strSQL += " '' REFERENCE_NR,";
            strSQL += " MBL.BL_DESCRIPTION,";
            strSQL += " 0 PRIORITY,";
            strSQL += " MBL.BL_CLAUSE_FK \"PK\",";
            strSQL += " 'true' \"sel\"";
            strSQL += " FROM";
            strSQL += " MBL_BL_CLAUSE_TBL MBL";
            strSQL += " WHERE";
            strSQL += " MBL.MBL_EXP_TBL_FK=" + HblPk;
            strSQL += " order by \"sel\",BL_DESCRIPTION ";
            strSQL += " )q)";
            ///'''''''''''
            //strSQL &= vbCrLf & strCondition

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;

            if (TotalRecords % M_MasterPageSize != 0)
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

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            if (HblPk > 0)
            {
                strSQL = " select * from (";
                strSQL += " SELECT ROWNUM SR_NO, q.* FROM ";
                strSQL += " (SELECT ";
                strSQL += " 0 MBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " BLMST.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " BLMST.BL_CLAUSE_PK \"PK\",";
                strSQL += " 'false' \"sel\"";
                strSQL += " FROM";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
                strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
                strSQL += " AND POD.PORT_MST_PK = " + PodPk;
                strSQL += " AND BLMST.ACTIVE_FLAG = 1";
                if (HblPk > 0)
                {
                    strSQL += " and blmst.bl_clause_pk not in(select MBL.BL_CLAUSE_FK from MBL_BL_CLAUSE_TBL MBL where MBL.bl_clause_fk is not null and MBL.MBL_EXP_TBL_FK=" + HblPk;
                    strSQL += ")";
                }
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " AND UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }

                strSQL += " UNION";
                strSQL += " SELECT distinct";
                strSQL += " MBL.MBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " MBL.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " MBL.BL_CLAUSE_FK \"PK\",";
                strSQL += " 'true' \"sel\"";
                strSQL += " FROM";
                strSQL += " MBL_BL_CLAUSE_TBL MBL,";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " MBL.BL_CLAUSE_FK(+) = BLMST.BL_CLAUSE_PK";
                strSQL += " and blmst.Active_Flag =1";
                strSQL += " And blmst.bl_clause_pk = bltrn.bl_clause_fk ";
                strSQL += " AND BLTRN.PORT_MST_FK = pod.port_mst_pk";
                strSQL += " and POD.PORT_MST_PK = " + PodPk;
                if (HblPk > 0)
                {
                    strSQL += " and MBL.MBL_EXP_TBL_FK=" + HblPk;
                }
                strSQL += strCondition;

                strSQL += " UNION";
                strSQL += " SELECT ";
                strSQL += " MBL.MBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " MBL.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " MBL.BL_CLAUSE_FK \"PK\",";
                strSQL += " 'true' \"sel\"";
                strSQL += " FROM";
                strSQL += " MBL_BL_CLAUSE_TBL MBL";
                strSQL += " WHERE";
                if (HblPk > 0)
                {
                    strSQL += " MBL.MBL_EXP_TBL_FK=" + HblPk;
                }
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " and  UPPER(MBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }

                //strSQL &= vbCrLf & strCondition
                strSQL += " order by \"sel\" desc,BL_DESCRIPTION ";
                strSQL += " )q) WHERE SR_NO  Between " + start + " and " + last;



            }
            else if (HblPk == 0 & RefPk > 0)
            {
                strSQL = " select * from (";
                strSQL += " SELECT ROWNUM SR_NO, q.* FROM ";
                strSQL += " (SELECT ";
                strSQL += " 0 HBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " BLMST.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " BLMST.BL_CLAUSE_PK \"PK\",";
                strSQL += " 'false' \"sel\"";
                strSQL += " FROM";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
                strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
                strSQL += " AND POD.PORT_MST_PK =" + PodPk;
                strSQL += " AND BLMST.ACTIVE_FLAG = 1";
                strSQL += " AND BLMST.BL_CLAUSE_PK NOT IN (Select h.bl_clause_fk from hbl_bl_clause_tbl h where h.bl_clause_fk is not null and h.hbl_exp_tbl_fk =" + RefPk;
                strSQL += ")";
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " and  UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }

                strSQL += " UNION";

                strSQL += " SELECT ";
                strSQL += " HBL.HBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " HBL.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " HBL.BL_CLAUSE_FK \"PK\",";
                strSQL += " 'TRUE' \"sel\"";
                strSQL += " FROM";
                strSQL += " HBL_BL_CLAUSE_TBL HBL,";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
                strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
                strSQL += " AND POD.PORT_MST_PK =" + PodPk;
                strSQL += " AND BLMST.ACTIVE_FLAG = 1";
                strSQL += " AND HBL.HBL_EXP_TBL_FK=" + RefPk;
                strSQL += " AND BLMST.BL_CLAUSE_PK = BLTRN.BL_CLAUSE_FK";
                strSQL += " AND HBL.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " AND (UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                    strSQL += " or UPPER(HBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%')";
                }

                strSQL += " UNION";

                strSQL += " SELECT ";
                strSQL += " HBL.HBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " HBL.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " HBL.BL_CLAUSE_FK \"PK\",";
                strSQL += " 'TRUE' \"sel\"";
                strSQL += " FROM";
                strSQL += " HBL_BL_CLAUSE_TBL HBL";
                strSQL += " WHERE";
                strSQL += " HBL.HBL_EXP_TBL_FK =" + RefPk;
                strSQL += " AND HBL.BL_CLAUSE_FK IS NULL";
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " and  UPPER(HBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }

                strSQL += " order by \"sel\" desc,BL_DESCRIPTION )q)";

            }
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #region "FetchBlClausesForMblWithJobCardChecked Function"
        public DataSet FetchBlClausesForMblWithJobCardChecked(string strBlClause = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string PodPk = "", long HblPk = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;

            if (strBlClause.Trim().Length > 0)
            {
                strCondition += " AND (UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                strCondition += " or UPPER(MBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%')";
            }


            ///'''''''''''
            strSQL = " select count(*) from (";
            strSQL += " SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL += " (SELECT ";
            strSQL += " 0 MBL_BL_CLAUSE_TBL_PK,";
            strSQL += " '' REFERENCE_NR,";
            strSQL += " BLMST.BL_DESCRIPTION,";
            strSQL += " 0 PRIORITY,";
            strSQL += " BLMST.BL_CLAUSE_PK \"PK\",";
            strSQL += " 'false' \"sel\"";
            strSQL += " FROM";
            strSQL += " BL_CLAUSE_TBL BLMST,";
            strSQL += " BL_CLAUSE_TRN BLTRN,";
            strSQL += " PORT_MST_TBL POD";
            strSQL += " WHERE";
            strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
            strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
            strSQL += " AND POD.PORT_MST_PK = " + PodPk;
            strSQL += " AND BLMST.ACTIVE_FLAG = 1";
            if (strBlClause.Trim().Length > 0)
            {
                strSQL += " AND UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
            }

            strSQL += " UNION";
            strSQL += " SELECT";
            strSQL += " MBL.MBL_BL_CLAUSE_TBL_PK,";
            strSQL += " '' REFERENCE_NR,";
            strSQL += " MBL.BL_DESCRIPTION,";
            strSQL += " 0 PRIORITY,";
            strSQL += " MBL.BL_CLAUSE_FK \"PK\",";
            strSQL += " 'true' \"sel\"";
            strSQL += " FROM";
            strSQL += " MBL_BL_CLAUSE_TBL MBL,";
            strSQL += " BL_CLAUSE_TBL BLMST,";
            strSQL += " BL_CLAUSE_TRN BLTRN,";
            strSQL += " PORT_MST_TBL POD";
            strSQL += " WHERE";
            strSQL += " MBL.BL_CLAUSE_FK(+) = BLMST.BL_CLAUSE_PK";
            strSQL += " and blmst.Active_Flag =1";
            strSQL += " AND BLTRN.PORT_MST_FK = pod.port_mst_pk";
            strSQL += " and POD.PORT_MST_PK = " + PodPk;
            strSQL += " AND bltrn.bl_clause_fk= MBL.BL_CLAUSE_FK";
            strSQL += " And blmst.bl_clause_pk = bltrn.bl_clause_fk ";
            if (HblPk > 0)
            {
                strSQL += "  and MBL.MBL_EXP_TBL_FK(+)=" + HblPk;
            }
            strSQL += strCondition;

            strSQL += " UNION";
            strSQL += " SELECT";
            strSQL += " MBL.MBL_BL_CLAUSE_TBL_PK,";
            strSQL += " '' REFERENCE_NR,";
            strSQL += " MBL.BL_DESCRIPTION,";
            strSQL += " 0 PRIORITY,";
            strSQL += " MBL.BL_CLAUSE_FK \"PK\",";
            strSQL += " 'true' \"sel\"";
            strSQL += " FROM";
            strSQL += " MBL_BL_CLAUSE_TBL MBL";
            strSQL += " WHERE";
            strSQL += " MBL.MBL_EXP_TBL_FK=" + HblPk;
            if (strBlClause.Trim().Length > 0)
            {
                strSQL += " AND UPPER(MBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
            }
            strSQL += " order by \"sel\",BL_DESCRIPTION ";
            strSQL += " )q)";
            ///'''''''''''
            //strSQL &= vbCrLf & strCondition

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;

            if (TotalRecords % M_MasterPageSize != 0)
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

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            if (HblPk > 0)
            {
                strSQL = " select * from (";
                strSQL += " SELECT ROWNUM SR_NO, q.* FROM ";
                strSQL += " (SELECT ";
                strSQL += " 0 MBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " BLMST.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " BLMST.BL_CLAUSE_PK \"PK\",";
                strSQL += " 'false' \"sel\"";
                strSQL += " FROM";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
                strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
                strSQL += " AND POD.PORT_MST_PK = " + PodPk;
                strSQL += " AND BLMST.ACTIVE_FLAG = 1";
                if (HblPk > 0)
                {
                    strSQL += " and blmst.bl_clause_pk not in(select MBL.BL_CLAUSE_FK from MBL_BL_CLAUSE_TBL MBL where MBL.bl_clause_fk is not null and MBL.MBL_EXP_TBL_FK=" + HblPk;
                    strSQL += ")";
                }
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " AND UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }

                strSQL += " UNION";
                strSQL += " SELECT distinct";
                strSQL += " MBL.MBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " MBL.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " MBL.BL_CLAUSE_FK \"PK\",";
                strSQL += " 'true' \"sel\"";
                strSQL += " FROM";
                strSQL += " MBL_BL_CLAUSE_TBL MBL,";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " MBL.BL_CLAUSE_FK(+) = BLMST.BL_CLAUSE_PK";
                strSQL += " and blmst.Active_Flag =1";
                strSQL += " And blmst.bl_clause_pk = bltrn.bl_clause_fk ";
                strSQL += " AND BLTRN.PORT_MST_FK = pod.port_mst_pk";
                strSQL += " and POD.PORT_MST_PK = " + PodPk;
                //strSQL &= vbCrLf & " AND bltrn.bl_clause_fk= HBL.BL_CLAUSE_FK"
                if (HblPk > 0)
                {
                    strSQL += " and MBL.MBL_EXP_TBL_FK=" + HblPk;
                }
                strSQL += strCondition;

                strSQL += " UNION";
                strSQL += " SELECT ";
                strSQL += " MBL.MBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " MBL.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " MBL.BL_CLAUSE_FK \"PK\",";
                strSQL += " 'true' \"sel\"";
                strSQL += " FROM";
                strSQL += " MBL_BL_CLAUSE_TBL MBL";
                strSQL += " WHERE";
                if (HblPk > 0)
                {
                    strSQL += " MBL.MBL_EXP_TBL_FK=" + HblPk;
                }
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " AND UPPER(MBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }

                //strSQL &= vbCrLf & strCondition
                strSQL += " order by \"sel\" desc,BL_DESCRIPTION ";
                strSQL += " )q) WHERE SR_NO  Between " + start + " and " + last;



            }
            else if (HblPk == 0)
            {
                strSQL = " select * from (";
                strSQL += " SELECT ROWNUM SR_NO, q.* FROM ";
                strSQL += " (SELECT ";
                strSQL += " 0 HBL_BL_CLAUSE_TBL_PK,";
                strSQL += " '' REFERENCE_NR,";
                strSQL += " BLMST.BL_DESCRIPTION,";
                strSQL += " 0 PRIORITY,";
                strSQL += " BLMST.BL_CLAUSE_PK \"PK\",";
                strSQL += " 'false' \"sel\"";
                strSQL += " FROM";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
                strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
                strSQL += " AND POD.PORT_MST_PK = " + PodPk;
                strSQL += " AND BLMST.ACTIVE_FLAG = 1";
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " AND UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }
                strSQL += " order by \"sel\"  desc,BL_DESCRIPTION )q)";
            }
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #region "FetchBlClausesForMblWithJobCardChecked Function"
        public DataSet FetchBlClausesForDeliverOrder(string strBlClause = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string PodPk = "", long HblPk = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;

            if (strBlClause.Trim().Length > 0)
            {
                strCondition += " AND (UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                strCondition += " or UPPER(HBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%')";
            }

            if (PodPk == null | string.IsNullOrEmpty(PodPk))
            {
                PodPk = "0";
            }
            ///'''''''''''
            strSQL = " select count(*) from (";
            strSQL += " SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL += " (SELECT ";
            strSQL += " 0 HBL_BL_CLAUSE_TBL_PK,";
            strSQL += " BLMST.REFERENCE_NR REFERENCE_NR,";
            strSQL += " BLMST.BL_DESCRIPTION,";
            strSQL += " BLMST.PRIORITY,";
            strSQL += " BLMST.BL_CLAUSE_PK \"PK\",";
            strSQL += " 'false' \"sel\"";
            strSQL += " FROM";
            strSQL += " BL_CLAUSE_TBL BLMST,";
            strSQL += " BL_CLAUSE_TRN BLTRN,";
            strSQL += " PORT_MST_TBL POD";
            strSQL += " WHERE";
            strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
            strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
            strSQL += " AND POD.PORT_MST_PK = " + PodPk;
            strSQL += " AND BLMST.ACTIVE_FLAG = 1";
            strSQL += " UNION";
            strSQL += " SELECT";
            strSQL += " HBL.HBL_BL_CLAUSE_TBL_PK,";
            strSQL += " HBL.REFERENCE_NR REFERENCE_NR,";
            strSQL += " HBL.BL_DESCRIPTION,";
            strSQL += " HBL.PRIORITY,";
            strSQL += " HBL.BL_CLAUSE_FK \"PK\",";
            strSQL += " 'true' \"sel\"";
            strSQL += " FROM";
            strSQL += " HBL_BL_CLAUSE_TBL HBL,";
            strSQL += " BL_CLAUSE_TBL BLMST,";
            strSQL += " BL_CLAUSE_TRN BLTRN,";
            strSQL += " PORT_MST_TBL POD";
            strSQL += " WHERE";
            strSQL += " HBL.BL_CLAUSE_FK(+) = BLMST.BL_CLAUSE_PK";
            strSQL += " and blmst.Active_Flag =1";
            strSQL += " AND BLTRN.PORT_MST_FK = pod.port_mst_pk";
            strSQL += " and POD.PORT_MST_PK = " + PodPk;
            strSQL += " AND bltrn.bl_clause_fk= HBL.BL_CLAUSE_FK";
            strSQL += " And blmst.bl_clause_pk = bltrn.bl_clause_fk ";
            if (HblPk > 0)
            {
                strSQL += " and hbl.hbl_exp_tbl_fk=" + HblPk;
            }
            strSQL += strCondition;
            strSQL += " UNION";
            strSQL += " SELECT";
            strSQL += " HBL.HBL_BL_CLAUSE_TBL_PK,";
            strSQL += " HBL.REFERENCE_NR REFERENCE_NR,";
            strSQL += " HBL.BL_DESCRIPTION,";
            strSQL += " HBL.PRIORITY,";
            strSQL += " HBL.BL_CLAUSE_FK \"PK\",";
            strSQL += " 'true' \"sel\"";
            strSQL += " FROM";
            strSQL += " HBL_BL_CLAUSE_TBL HBL";
            strSQL += " WHERE";
            strSQL += " hbl.hbl_exp_tbl_fk =" + HblPk;

            strSQL += " order by \"sel\",BL_DESCRIPTION ";
            strSQL += " )q)";
            ///'''''''''''
            //strSQL &= vbCrLf & strCondition

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;

            if (TotalRecords % M_MasterPageSize != 0)
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

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            if (HblPk > 0)
            {
                strSQL = " select * from (";
                strSQL += " SELECT ROWNUM SR_NO, q.* FROM ";
                strSQL += " (SELECT ";
                strSQL += " 0 HBL_BL_CLAUSE_TBL_PK,";
                strSQL += " BLMST.REFERENCE_NR,";
                strSQL += " BLMST.BL_DESCRIPTION,";
                strSQL += " BLMST.PRIORITY,";
                strSQL += " BLMST.BL_CLAUSE_PK \"PK\",";
                strSQL += " 'false' \"sel\"";
                strSQL += " FROM";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
                strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
                strSQL += " AND POD.PORT_MST_PK = " + PodPk;
                strSQL += " AND BLMST.ACTIVE_FLAG = 1";
                if (HblPk > 0)
                {
                    strSQL += " and blmst.bl_clause_pk not in(select HBL.BL_CLAUSE_FK from HBL_BL_CLAUSE_TBL hbl where  hbl.bl_clause_fk is not null and hbl.hbl_exp_tbl_fk=" + HblPk;
                    strSQL += ")";
                }
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " AND UPPER(BLMST.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }

                strSQL += " UNION";
                strSQL += " SELECT";
                strSQL += " HBL.HBL_BL_CLAUSE_TBL_PK,";
                strSQL += " HBL.REFERENCE_NR,";
                strSQL += " HBL.BL_DESCRIPTION,";
                strSQL += " HBL.PRIORITY,";
                strSQL += " HBL.BL_CLAUSE_FK \"PK\",";
                strSQL += " 'true' \"sel\"";
                strSQL += " FROM";
                strSQL += " HBL_BL_CLAUSE_TBL HBL,";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " HBL.BL_CLAUSE_FK(+) = BLMST.BL_CLAUSE_PK";
                strSQL += " And blmst.bl_clause_pk = bltrn.bl_clause_fk";
                strSQL += " and blmst.Active_Flag =1";
                strSQL += " AND BLTRN.PORT_MST_FK = pod.port_mst_pk";
                strSQL += " and POD.PORT_MST_PK = " + PodPk;
                //strSQL &= vbCrLf & " AND bltrn.bl_clause_fk= HBL.BL_CLAUSE_FK"
                if (HblPk > 0)
                {
                    strSQL += " and hbl.hbl_exp_tbl_fk =" + HblPk;
                }
                strSQL += strCondition;

                strSQL += " UNION";
                strSQL += " SELECT";
                strSQL += " HBL.HBL_BL_CLAUSE_TBL_PK,";
                strSQL += " HBL.REFERENCE_NR,";
                strSQL += " HBL.BL_DESCRIPTION,";
                strSQL += " HBL.PRIORITY,";
                strSQL += " HBL.BL_CLAUSE_FK \"PK\",";
                strSQL += " 'true' \"sel\"";
                strSQL += " FROM";
                strSQL += " HBL_BL_CLAUSE_TBL HBL";
                strSQL += " WHERE";
                strSQL += " hbl.hbl_exp_tbl_fk = " + HblPk;
                if (strBlClause.Trim().Length > 0)
                {
                    strSQL += " and  UPPER(HBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }
                strSQL += " order by \"sel\" Desc,BL_DESCRIPTION ";
                strSQL += " )q) WHERE SR_NO  Between " + start + " and " + last;

            }
            else if (HblPk == 0)
            {
                strSQL = " select * from (";
                strSQL += " SELECT ROWNUM SR_NO, q.* FROM ";
                strSQL += " (SELECT ";
                strSQL += " 0 HBL_BL_CLAUSE_TBL_PK,";
                strSQL += " BLMST.REFERENCE_NR,";
                strSQL += " BLMST.BL_DESCRIPTION,";
                strSQL += " BLMST.PRIORITY,";
                strSQL += " BLMST.BL_CLAUSE_PK \"PK\",";
                strSQL += " 'false' \"sel\"";
                strSQL += " FROM";
                strSQL += " BL_CLAUSE_TBL BLMST,";
                strSQL += " BL_CLAUSE_TRN BLTRN,";
                strSQL += " PORT_MST_TBL POD";
                strSQL += " WHERE";
                strSQL += " BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK";
                strSQL += " And blmst.bl_clause_pk = bltrn.bl_clause_fk ";
                strSQL += " AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK";
                strSQL += " AND POD.PORT_MST_PK = " + PodPk;
                strSQL += " AND BLMST.ACTIVE_FLAG = 1";
                if (strBlClause.Trim().Length > 0)
                {
                    // strSQL &= vbCrLf & " AND (UPPER(BLMST.BL_DESCRIPTION) LIKE '" & strBlClause.ToUpper.Replace("'", "''") & "%'"
                    //Added by Hemalatha -bracket'
                    strSQL += " AND (UPPER(BLMST.BL_DESCRIPTION)) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";

                    //Ended by Hemalatha
                }
                strSQL += " order by \"sel\" Desc,BL_DESCRIPTION )q)";
            }
            try
            {
                return objWF.GetDataSet(strSQL);
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

        //Added by Gangadhar on 02/07/2011, PTS ID:Jun-20          
        #region "Save BLClause"
        public ArrayList SaveBLClause(DataSet M_DataSet, int ClauseTypeFlag = 0)
        {
            WorkFlow objWK = new WorkFlow();
            cls_BlClauseForHblMbl objclause = new cls_BlClauseForHblMbl();
            string Protocol = null;

            OracleTransaction Tran = null;
            OracleCommand InsCommand = new OracleCommand();
            OracleCommand UpdCommand = new OracleCommand();
            OracleCommand DelCommand = new OracleCommand();
            //Dim ClauseDescription, ClauseHdr As String
            int intPKVal = 0;
            int intPKVal1 = 0;
            int ClausePK = 0;
            Int32 i = default(Int32);
            Int32 RowCnt = default(Int32);
            Int32 RecAfct = default(Int32);
            System.DBNull StrNull = null;
            int BL_CLAUSE_FK = 0;
            int HBL_EXP_TBL_FK = 0;
            int HBL_BL_CLAUSE_TBL_PK = 0;
            int ACTIVE_FLAG = 0;
            string BL_DESCRIPTION = null;

            try
            {
                objWK.OpenConnection();
                Tran = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = Tran;
                //Protocol = GenerateClauseNr(CLng(Session("LOGED_IN_LOC_FK")), CLng(Session("EMP_PK")), M_CREATED_BY_FK, objWK)
                var _with1 = InsCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".HBL_BL_CLAUSE_TBL_PKG.HBL_BL_CLAUSE_TBL_INS";
                _with1.Transaction = Tran;
                var _with2 = UpdCommand;
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = objWK.MyUserName + ".HBL_BL_CLAUSE_TBL_PKG.HBL_BL_CLAUSE_TBL_UPD";
                _with2.Transaction = Tran;
                for (i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++)
                {
                    if ((Convert.ToInt32(M_DataSet.Tables[0].Rows[i]["HBL_BL_CLAUSE_TBL_PK"]) == 0))
                    {
                        var _with3 = InsCommand;
                        _with3.Parameters.Clear();
                        _with3.Parameters.Add("BL_DESCRIPTION_IN", M_DataSet.Tables[0].Rows[i]["BL_DESCRIPTION"]).Direction = ParameterDirection.Input;
                        if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["REFERENCE_NR"].ToString()))
                        {
                            Protocol = GenerateClauseNr(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), M_CREATED_BY_FK, objWK);
                            _with3.Parameters.Add("REFERENCE_NR_IN", Protocol).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with3.Parameters.Add("REFERENCE_NR_IN", M_DataSet.Tables[0].Rows[i]["REFERENCE_NR"]).Direction = ParameterDirection.Input;
                        }
                        _with3.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                        _with3.Parameters.Add("BL_CLAUSE_FK_IN", M_DataSet.Tables[0].Rows[i]["BL_CLAUSE_FK"]).Direction = ParameterDirection.Input;
                        _with3.Parameters.Add("HBL_EXP_TBL_FK_IN", M_DataSet.Tables[0].Rows[i]["HBL_EXP_TBL_FK"]).Direction = ParameterDirection.Input;
                        _with3.Parameters.Add("CLAUSE_TYPE_FLAG_IN", ClauseTypeFlag).Direction = ParameterDirection.Input;
                        _with3.Parameters.Add("ACTIVE_FLAG_IN", M_DataSet.Tables[0].Rows[i]["ACTIVE_FLAG"]).Direction = ParameterDirection.Input;
                        //.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
                        _with3.Parameters.Add("PRIORITY_IN", M_DataSet.Tables[0].Rows[i]["PRIORITY"]).Direction = ParameterDirection.Input;
                        _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_BL_CLAUSE_TBL_PK").Direction = ParameterDirection.Output;
                        _with3.ExecuteNonQuery();
                        intPKVal = Convert.ToInt32(InsCommand.Parameters["RETURN_VALUE"].Value);

                    }
                    else
                    {
                        var _with4 = UpdCommand;
                        _with4.Parameters.Clear();
                        _with4.Parameters.Add("HBL_BL_CLAUSE_TBL_PK_IN", M_DataSet.Tables[0].Rows[i]["HBL_BL_CLAUSE_TBL_PK"]).Direction = ParameterDirection.Input;
                        _with4.Parameters.Add("BL_DESCRIPTION_IN", M_DataSet.Tables[0].Rows[i]["BL_DESCRIPTION"]).Direction = ParameterDirection.Input;
                        _with4.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                        //.Parameters.Add("VERSION_NO_IN", M_DataSet.Tables(0).Rows(i).Item("VERSION_NO")).Direction = ParameterDirection.Input
                        _with4.Parameters.Add("BL_CLAUSE_FK_IN", M_DataSet.Tables[0].Rows[i]["BL_CLAUSE_FK"]).Direction = ParameterDirection.Input;
                        _with4.Parameters.Add("HBL_EXP_TBL_FK_IN", M_DataSet.Tables[0].Rows[i]["HBL_EXP_TBL_FK"]).Direction = ParameterDirection.Input;
                        _with4.Parameters.Add("CLAUSE_TYPE_FLAG_IN", ClauseTypeFlag).Direction = ParameterDirection.Input;
                        _with4.Parameters.Add("ACTIVE_FLAG_IN", M_DataSet.Tables[0].Rows[i]["ACTIVE_FLAG"]).Direction = ParameterDirection.Input;
                        //.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
                        _with4.Parameters.Add("PRIORITY_IN", M_DataSet.Tables[0].Rows[i]["PRIORITY"]).Direction = ParameterDirection.Input;
                        _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_BL_CLAUSE_TBL_PK").Direction = ParameterDirection.Output;
                        _with4.ExecuteNonQuery();
                        intPKVal = Convert.ToInt32(UpdCommand.Parameters["RETURN_VALUE"].Value);
                    }
                }
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    Tran.Commit();
                    return arrMessage;

                }

            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                arrMessage.Add(oraexp.Message);
                Tran.Rollback();
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                Tran.Rollback();
                return arrMessage;

            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
        }
        public string GenerateClauseNr(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey("CLAUSE MASTER", nLocationId, nEmployeeId, DateTime.Now, "","" ,"" , nCreatedBy, objWK);
            return functionReturnValue;
            return functionReturnValue;
        }
        #endregion
    }

    //Ended by Gangadhar
}