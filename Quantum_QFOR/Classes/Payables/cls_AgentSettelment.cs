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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsAgentSettelment : CommonFeatures
    {
        /// <summary>
        /// The pk value
        /// </summary>
        public int PkVal;

        /// <summary>
        /// The coll reference nr
        /// </summary>
        public string CollRefNr;

        #region "Fetch Listing Function"

        /// <summary>
        /// Fetchlists the specified biztype.
        /// </summary>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="Process">The process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="SettRefNo">The sett reference no.</param>
        /// <param name="AgentPK">The agent pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public object Fetchlist(int Biztype = 0, int Process = 0, string fromDate = "", string ToDate = "", string SettRefNo = "", string AgentPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long usrLocFK = 0, string SortColumn = "",
        string SortType = "DESC", Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder sb = new StringBuilder(5000);
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();
            DataSet DS = null;
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

            sb.Append(" SELECT ASMT.AGENT_SETT_MST_PK,");
            sb.Append("       ASMT.SETTLEMENT_NO,");
            sb.Append("       ASMT.SETTLEMENT_DATE,");
            sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = ASMT.BUSINESS_TYPE) BUSINESS_TYPE,");
            sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'PROCESS_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = ASMT.PROCESS_TYPE) PROCESS_TYPE,");
            sb.Append("       AMT.AGENT_NAME,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       ASMT.FROM_DATE,");
            sb.Append("       ASMT.TO_DATE,");
            sb.Append("       NVL(ASMT.TOTAL_AMOUNT,0) TOTAL_AMOUNT,");
            sb.Append("       '' CHGFLAG,");
            sb.Append("       '' DELFLAG");
            sb.Append("       FROM AGENT_SETT_MST_TBL    ASMT,");
            sb.Append("       AGENT_MST_TBL         AMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CTMT");
            sb.Append("       WHERE ASMT.AGENT_MST_FK = AMT.AGENT_MST_PK");
            sb.Append("       AND ASMT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");

            if (!string.IsNullOrEmpty(AgentPK))
            {
                sb.Append(" AND ASMT.AGENT_MST_FK = " + AgentPK + " ");
            }
            if (!string.IsNullOrEmpty(SettRefNo))
            {
                sb.Append(" AND UPPER(ASMT.SETTLEMENT_NO) LIKE '%" + SettRefNo.Trim().ToUpper().Replace("'", "''") + "%' ");
            }

            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(fromDate, ""))) & !string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                sb.Append(" AND ASMT.SETTLEMENT_DATE  BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(Convert.ToString(getDefault(fromDate, ""))))
            {
                sb.Append(" AND ASMT.SETTLEMENT_DATE   >=TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(Convert.ToString(getDefault(ToDate, ""))))
            {
                sb.Append(" AND ASMT.SETTLEMENT_DATE  <=TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            if (Biztype != 0)
            {
                if (Biztype != 3)
                {
                    sb.Append(" AND ASMT.BUSINESS_TYPE in ( " + Biztype + ", 3) ");
                }
            }

            if (Process != 0)
            {
                sb.Append(" AND ASMT.PROCESS_TYPE = " + Process + " ");
            }
            sb.Append("  ORDER BY ASMT.SETTLEMENT_DATE DESC, ASMT.SETTLEMENT_NO DESC");

            strCount.Append(" SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + sb.ToString() + ")");

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
            strSQL.Append("(SELECT ROWNUM \"SL_NR\", T.*  FROM ");
            strSQL.Append("  (" + sb.ToString() + " ");
            strSQL.Append("  ) T) QRY  WHERE \"SL_NR\"  BETWEEN " + start + " AND " + last);

            try
            {
                DS = objWF.GetDataSet(strSQL.ToString());
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

        #endregion "Fetch Listing Function"

        #region "Fetch New Data "

        /// <summary>
        /// Fetches the data.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="intAgentPk">The int agent pk.</param>
        /// <param name="SettRefPK">The sett reference pk.</param>
        /// <param name="intBaseCurrPk">The int base curr pk.</param>
        /// <param name="lngLocPk">The LNG loc pk.</param>
        /// <param name="strFromDt">The string from dt.</param>
        /// <param name="strToDt">The string to dt.</param>
        /// <returns></returns>
        public DataSet FetchData(short BizType, short Process, int intAgentPk, int SettRefPK, int intBaseCurrPk, long lngLocPk, string strFromDt = "", string strToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            DataSet MainDS = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();
            intBaseCurrPk = Convert.ToInt32(intBaseCurrPk == 0 ? HttpContext.Current.Session["CURRENCY_MST_PK"] : intBaseCurrPk);
            int BaseCurrFk = intBaseCurrPk;
            try
            {
                // Parent Row
                // Biz Type = Sea -2
                // Process  = Export -1
                sb.Append(" SELECT ROWNUM SL_NR, QRY.*");
                sb.Append("  FROM (SELECT DISTINCT 0 AGENT_SETT_TRN_PK,");
                sb.Append("                        0 AGENT_SETT_MST_FK,");
                sb.Append("                        JOB.JOB_CARD_TRN_PK  JOB_PK,");
                sb.Append("                        JOB.JOBCARD_REF_NO       REF_NO,");
                if (BizType == 2 & Process == 1)
                {
                    sb.Append("                        NVL(HBL.HBL_EXP_TBL_PK,0)       HBL_PK,");
                    sb.Append("                        NVL(HBL.HBL_REF_NO,'')           HBL_NO,");
                }
                else if (BizType == 1 & Process == 1)
                {
                    sb.Append("                        NVL(HAWB.HAWB_EXP_TBL_PK,0)    HBL_PK,");
                    sb.Append("                        NVL(HAWB.HAWB_REF_NO,'')         HBL_NO,");
                }
                else
                {
                    sb.Append("                        JOB.JOB_CARD_TRN_PK  HBL_PK,");
                    sb.Append("                        NVL(JOB.HBL_HAWB_REF_NO,'')    HBL_NO,");
                }
                sb.Append("                        INV.INV_AGENT_PK INV_PK,");
                sb.Append("                        INV.INVOICE_REF_NO       INV_NO,");
                sb.Append("                        INV.CURRENCY_MST_FK      INV_CURR_PK,");
                sb.Append("                        CTMT.CURRENCY_ID         INV_CURR,");
                sb.Append("                        INV.NET_INV_AMT          INV_AMT,");
                sb.Append("                        CLN.COLLECTIONS_TBL_PK   COLL_FK,");
                sb.Append("                        CMOD.CURRENCY_MST_FK     COLL_CURR_PK,");
                sb.Append("                        CTMT.CURRENCY_ID         COLL_CURR,");
                sb.Append("                        CMOD.RECD_AMOUNT         COLL_AMT,");
                sb.Append("                        0 PROFIT_SHARING,");
                sb.Append("                '' SELFLAG,");
                sb.Append("                '' CHGFLAG,");
                sb.Append("                '' DELFLAG");
                sb.Append("          FROM AGENT_MST_TBL             AMT,");
                sb.Append("               INV_AGENT_TBL     INV,");
                sb.Append("               INV_AGENT_TRN_TBL INVFRT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     CUMT,");
                sb.Append("               COLLECTIONS_TBL           CLN,");
                sb.Append("               COLLECTIONS_TRN_TBL       CTRN,");
                sb.Append("               COLLECTIONS_MODE_TRN_TBL  CMOD,");
                sb.Append("               JOB_CARD_TRN      JOB,");
                sb.Append("               JOB_TRN_COST      JOBTRN,");
                sb.Append("               HBL_EXP_TBL               HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     CTMT");
                sb.Append("         WHERE INV.INV_AGENT_PK = INVFRT.INV_AGENT_FK");
                sb.Append("           AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("           AND INV.AGENT_MST_FK = AMT.AGENT_MST_PK");
                sb.Append("           AND INV.AGENT_MST_FK = AMT.AGENT_MST_PK");
                sb.Append("           AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("           AND CMOD.COLLECTIONS_TBL_FK = CLN.COLLECTIONS_TBL_PK");
                sb.Append("           AND CMOD.RECD_AMOUNT = INV.NET_INV_AMT");
                sb.Append("           AND JOB.JOB_CARD_TRN_PK = JOBTRN.JOB_CARD_TRN_FK");
                sb.Append("           AND INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("           AND HBL.JOB_CARD_SEA_EXP_FK(+) = JOB.JOB_CARD_TRN_PK");
                sb.Append("           AND HAWB.JOB_CARD_AIR_EXP_FK(+) = JOB.JOB_CARD_TRN_PK");
                sb.Append("           AND INV.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("           AND CLN.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("           AND JOB.BUSINESS_TYPE=" + BizType);
                sb.Append("           AND JOB.PROCESS_TYPE=" + Process);
                sb.Append("           AND INV.INV_AGENT_PK NOT IN(SELECT A.INVOICE_FK FROM AGENT_SETT_TRN_TBL A)");
                sb.Append(" AND INV.AGENT_MST_FK = " + intAgentPk + " ");

                if (strFromDt.Length > 0 & !(strToDt.Length > 0))
                {
                    sb.Append(" AND INV.INVOICE_DATE >= TO_DATE('" + strFromDt + "', '" + dateFormat + "')");
                }
                else if (!(strFromDt.Length > 0) & strToDt.Length > 0)
                {
                    sb.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                }
                else if (strFromDt.Length > 0 & strToDt.Length > 0)
                {
                    sb.Append(" AND INV.INVOICE_DATE BETWEEN TO_DATE('" + strFromDt + "', '" + dateFormat + "') AND");
                    sb.Append(" TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                }

                sb.Append(" ) QRY");
                sb.Append("");

                DA = objWF.GetDataAdapter(" SELECT * FROM ( " + sb.ToString() + ")");
                DA.Fill(MainDS, "Parent");

                //'----------------------------------Child Grid------------------------------------------------
                sb.Length = 0;
                // Child Row
                // Biz Type = Sea -2
                // Process  = Export -1
                sb.Append(" SELECT QRY.AGENT_SETT_FRT_DTL_PK,");
                sb.Append("       QRY.AGENT_SETT_TRN_FK,");
                sb.Append("       QRY.JOB_PK,");
                sb.Append("       QRY.INV_PK,");
                sb.Append("       QRY.FRT_PK,");
                sb.Append("       QRY.FRT_ID,");
                sb.Append("       QRY.FRT_NAME,");
                sb.Append("       QRY.E_CURR_PK,");
                sb.Append("       QRY.E_CURR_ID,");
                sb.Append("       QRY.E_AMOUNT,");
                sb.Append("       QRY.E_ROE,");
                sb.Append("       QRY.E_VALUE,");
                sb.Append("       QRY.R_CURR_PK,");
                sb.Append("       QRY.R_CURR_ID,");
                sb.Append("       QRY.R_AMOUNT,");
                sb.Append("       QRY.R_ROE,");
                sb.Append("       QRY.R_VALUE,");
                sb.Append("       QRY.PROFIT,");
                sb.Append("       QRY.P_SHARE,");
                sb.Append("       ((PROFIT / 100) * P_SHARE) P_VALUE,");
                sb.Append("       '' CHGFLAG,");
                sb.Append("       '' DELFLAG");
                sb.Append("  FROM (SELECT 0 AGENT_SETT_FRT_DTL_PK,");
                sb.Append("               0 AGENT_SETT_TRN_FK,");
                sb.Append("               JCSE.JOB_CARD_TRN_PK JOB_PK,");
                sb.Append("               INVTRN.INV_AGENT_FK INV_PK,");
                sb.Append("               ACEP.FREIGHT_ELEMENT_MST_FK FRT_PK,");
                sb.Append("               FEMT.FREIGHT_ELEMENT_ID FRT_ID,");
                sb.Append("               FEMT.FREIGHT_ELEMENT_NAME FRT_NAME,");
                sb.Append("               JCONT.CURRENCY_MST_FK E_CURR_PK,");
                sb.Append("               JCURR.CURRENCY_ID E_CURR_ID,");
                sb.Append("               NVL(JCONT.ESTIMATED_COST, 0) E_AMOUNT,");
                sb.Append("               GET_EX_RATE_BUY(JCONT.CURRENCY_MST_FK, " + intBaseCurrPk + ", JCSE.JOBCARD_DATE) E_ROE,");
                sb.Append("               (NVL(JCONT.ESTIMATED_COST, 0) *");
                sb.Append("               GET_EX_RATE_BUY(JCONT.CURRENCY_MST_FK, " + intBaseCurrPk + ", JCSE.JOBCARD_DATE)) E_VALUE,");
                sb.Append("               INVTRN.CURRENCY_MST_FK R_CURR_PK,");
                sb.Append("               ICURR.CURRENCY_ID R_CURR_ID,");
                sb.Append("               NVL(INVTRN.AMT_IN_INV_CURR, 0) R_AMOUNT,");
                sb.Append("               GET_EX_RATE(INVTRN.CURRENCY_MST_FK, " + intBaseCurrPk + ", INV.INVOICE_DATE) R_ROE,");
                sb.Append("               (NVL(INVTRN.AMT_IN_INV_CURR, 0) *");
                sb.Append("               GET_EX_RATE(INVTRN.CURRENCY_MST_FK, " + intBaseCurrPk + ", INV.INVOICE_DATE)) R_VALUE,");
                sb.Append("               ((NVL(INVTRN.AMT_IN_INV_CURR, 0) *");
                sb.Append("               GET_EX_RATE(INVTRN.CURRENCY_MST_FK, " + intBaseCurrPk + ", INV.INVOICE_DATE)) -");
                sb.Append("               (NVL(JCONT.ESTIMATED_COST, 0) *");
                sb.Append("               GET_EX_RATE_BUY(JCONT.CURRENCY_MST_FK, " + intBaseCurrPk + ", JCSE.JOBCARD_DATE))) PROFIT,");
                sb.Append("               AMT.EXP_PROFIT_PER P_SHARE");
                sb.Append("          FROM AGENT_MST_TBL             AMT,");
                sb.Append("               AGENT_CNT_EXP_PS_ELEM     ACEP,");
                sb.Append("               INV_AGENT_TBL     INV,");
                sb.Append("               INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("               JOB_CARD_TRN      JCSE,");
                sb.Append("               JOB_TRN_COST      JCONT,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL   FEMT,");
                sb.Append("               COLLECTIONS_TBL           CLN,");
                sb.Append("               COLLECTIONS_TRN_TBL       CTRN,");
                sb.Append("               COLLECTIONS_MODE_TRN_TBL  CMOD,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     ICURR,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     JCURR,");
                sb.Append("               HBL_EXP_TBL               HBL,");
                sb.Append("               HAWB_EXP_TBL HAWB");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JCONT.JOB_CARD_TRN_FK");
                sb.Append("           AND JCSE.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("           AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("           AND AMT.AGENT_MST_PK = INV.AGENT_MST_FK");
                sb.Append("           AND AMT.AGENT_MST_PK = ACEP.AGENT_MST_FK");
                sb.Append("           AND FEMT.FREIGHT_ELEMENT_MST_PK = ACEP.FREIGHT_ELEMENT_MST_FK");
                sb.Append("           AND ACEP.FREIGHT_ELEMENT_MST_FK = INVTRN.COST_FRT_ELEMENT_FK");
                sb.Append("           AND INVTRN.COST_FRT_ELEMENT_FK = JCONT.COST_ELEMENT_MST_FK");
                sb.Append("           AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("           AND CMOD.COLLECTIONS_TBL_FK = CLN.COLLECTIONS_TBL_PK");
                sb.Append("           AND CMOD.RECD_AMOUNT = INV.NET_INV_AMT");
                sb.Append("           AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("           AND INVTRN.CURRENCY_MST_FK = ICURR.CURRENCY_MST_PK");
                sb.Append("           AND JCONT.CURRENCY_MST_FK = JCURR.CURRENCY_MST_PK");
                sb.Append("           AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND HAWB.JOB_CARD_AIR_EXP_FK(+) = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND JCSE.BUSINESS_TYPE=" + BizType);
                sb.Append("           AND JCSE.PROCESS_TYPE=" + Process);
                sb.Append("           AND INV.INV_AGENT_PK NOT IN");
                sb.Append("               (SELECT A.INVOICE_FK FROM AGENT_SETT_TRN_TBL A)");
                sb.Append("           AND INV.AGENT_MST_FK = " + intAgentPk + " ");

                if (strFromDt.Length > 0 & !(strToDt.Length > 0))
                {
                    sb.Append(" AND INV.INVOICE_DATE >= TO_DATE('" + strFromDt + "', '" + dateFormat + "')");
                }
                else if (!(strFromDt.Length > 0) & strToDt.Length > 0)
                {
                    sb.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                }
                else if (strFromDt.Length > 0 & strToDt.Length > 0)
                {
                    sb.Append(" AND INV.INVOICE_DATE BETWEEN TO_DATE('" + strFromDt + "', '" + dateFormat + "') AND");
                    sb.Append(" TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                }

                sb.Append("        UNION");
                sb.Append("        SELECT 0 AGENT_SETT_FRT_DTL_PK,");
                sb.Append("               0 AGENT_SETT_TRN_FK,");
                sb.Append("               JCSE.JOB_CARD_TRN_PK JOB_PK,");
                sb.Append("               INVTRN.INV_AGENT_FK INV_PK,");
                sb.Append("               ACEP.FREIGHT_ELEMENT_MST_FK FRT_PK,");
                sb.Append("               FEMT.FREIGHT_ELEMENT_ID FRT_ID,");
                sb.Append("               FEMT.FREIGHT_ELEMENT_NAME FRT_NAME,");
                sb.Append("                JCONT.CURRENCY_MST_FK E_CURR_PK,");
                sb.Append("               JCURR.CURRENCY_ID E_CURR_ID,");
                sb.Append("               NVL(JCONT.ESTIMATED_COST, 0) E_AMOUNT,");
                sb.Append("               GET_EX_RATE_BUY(JCONT.CURRENCY_MST_FK, " + intBaseCurrPk + ", JCSE.JOBCARD_DATE) E_ROE,");
                sb.Append("               (NVL(JCONT.ESTIMATED_COST, 0) *");
                sb.Append("               GET_EX_RATE_BUY(JCONT.CURRENCY_MST_FK, " + intBaseCurrPk + ", JCSE.JOBCARD_DATE)) E_VALUE,");
                sb.Append("               0 R_CURR_PK,");
                sb.Append("               '' R_CURR_ID,");
                sb.Append("               0 R_AMOUNT,");
                sb.Append("               0 R_ROE,");
                sb.Append("               0 R_VALUE,");
                sb.Append("               ");
                sb.Append("               (0 -");
                sb.Append("               (NVL(JCONT.ESTIMATED_COST, 0) *");
                sb.Append("               GET_EX_RATE_BUY(JCONT.CURRENCY_MST_FK, " + intBaseCurrPk + ", JCSE.JOBCARD_DATE))) PROFIT,");
                sb.Append("               ");
                sb.Append("               AMT.EXP_PROFIT_PER P_SHARE");
                sb.Append("          FROM AGENT_MST_TBL             AMT,");
                sb.Append("               AGENT_CNT_EXP_PS_ELEM     ACEP,");
                sb.Append("               INV_AGENT_TBL     INV,");
                sb.Append("               INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("               JOB_CARD_TRN      JCSE,");
                sb.Append("               JOB_TRN_COST      JCONT,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL   FEMT,");
                sb.Append("               COLLECTIONS_TBL           CLN,");
                sb.Append("               COLLECTIONS_TRN_TBL       CTRN,");
                sb.Append("               COLLECTIONS_MODE_TRN_TBL  CMOD,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     ICURR,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     JCURR,");
                sb.Append("                 HBL_EXP_TBL               HBL,");
                sb.Append("                HAWB_EXP_TBL HAWB");
                sb.Append("         WHERE  JCSE.JOB_CARD_TRN_PK = JCONT.JOB_CARD_TRN_FK");
                sb.Append("           AND JCSE.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("           AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("           AND AMT.AGENT_MST_PK = INV.AGENT_MST_FK");
                sb.Append("           AND AMT.AGENT_MST_PK = ACEP.AGENT_MST_FK");
                sb.Append("           AND FEMT.FREIGHT_ELEMENT_MST_PK = ACEP.FREIGHT_ELEMENT_MST_FK");
                sb.Append("           AND ACEP.FREIGHT_ELEMENT_MST_FK = INVTRN.COST_FRT_ELEMENT_FK");
                sb.Append("           AND INVTRN.COST_FRT_ELEMENT_FK <> JCONT.COST_ELEMENT_MST_FK");
                sb.Append("           AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("           AND CMOD.COLLECTIONS_TBL_FK = CLN.COLLECTIONS_TBL_PK");
                sb.Append("           AND CMOD.RECD_AMOUNT = INV.NET_INV_AMT");
                sb.Append("           AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("           AND INVTRN.CURRENCY_MST_FK = ICURR.CURRENCY_MST_PK");
                sb.Append("           AND JCONT.CURRENCY_MST_FK = JCURR.CURRENCY_MST_PK");
                sb.Append("           AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND HAWB.JOB_CARD_AIR_EXP_FK(+) = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND JCSE.BUSINESS_TYPE=" + BizType);
                sb.Append("           AND JCSE.PROCESS_TYPE=" + Process);
                sb.Append("           AND INV.INV_AGENT_PK NOT IN");
                sb.Append("               (SELECT A.INVOICE_FK FROM AGENT_SETT_TRN_TBL A)");
                sb.Append("           AND INV.AGENT_MST_FK = " + intAgentPk + " ");

                if (strFromDt.Length > 0 & !(strToDt.Length > 0))
                {
                    sb.Append(" AND INV.INVOICE_DATE >= TO_DATE('" + strFromDt + "', '" + dateFormat + "')");
                }
                else if (!(strFromDt.Length > 0) & strToDt.Length > 0)
                {
                    sb.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                }
                else if (strFromDt.Length > 0 & strToDt.Length > 0)
                {
                    sb.Append(" AND INV.INVOICE_DATE BETWEEN TO_DATE('" + strFromDt + "', '" + dateFormat + "') AND");
                    sb.Append(" TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                }

                sb.Append("        UNION");
                sb.Append("        SELECT 0 AGENT_SETT_FRT_DTL_PK,");
                sb.Append("               0 AGENT_SETT_TRN_FK,");
                sb.Append("                JCSE.JOB_CARD_TRN_PK JOB_PK,");
                sb.Append("               INVTRN.INV_AGENT_FK INV_PK,");
                sb.Append("               ACEP.FREIGHT_ELEMENT_MST_FK FRT_PK,");
                sb.Append("               FEMT.FREIGHT_ELEMENT_ID FRT_ID,");
                sb.Append("               FEMT.FREIGHT_ELEMENT_NAME FRT_NAME,");
                sb.Append("               0 E_CURR_PK,");
                sb.Append("               '' E_CURR_ID,");
                sb.Append("               0 E_AMOUNT,");
                sb.Append("               0 E_ROE,");
                sb.Append("               0 E_VALUE,");
                sb.Append("               INVTRN.CURRENCY_MST_FK R_CURR_PK,");
                sb.Append("               ICURR.CURRENCY_ID R_CURR_ID,");
                sb.Append("               NVL(INVTRN.AMT_IN_INV_CURR, 0) R_AMOUNT,");
                sb.Append("               GET_EX_RATE(INVTRN.CURRENCY_MST_FK, " + intBaseCurrPk + ", INV.INVOICE_DATE) R_ROE,");
                sb.Append("               (NVL(INVTRN.AMT_IN_INV_CURR, 0) *");
                sb.Append("               GET_EX_RATE(INVTRN.CURRENCY_MST_FK, " + intBaseCurrPk + ", INV.INVOICE_DATE)) R_VALUE,");
                sb.Append("               ");
                sb.Append("               ((NVL(INVTRN.AMT_IN_INV_CURR, 0) *");
                sb.Append("               GET_EX_RATE(INVTRN.CURRENCY_MST_FK, " + intBaseCurrPk + ", INV.INVOICE_DATE)) - 0) PROFIT,");
                sb.Append("               AMT.EXP_PROFIT_PER P_SHARE");
                sb.Append("          FROM AGENT_MST_TBL             AMT,");
                sb.Append("               AGENT_CNT_EXP_PS_ELEM     ACEP,");
                sb.Append("               INV_AGENT_TBL     INV,");
                sb.Append("               INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("               JOB_CARD_TRN      JCSE,");
                sb.Append("               JOB_TRN_COST      JCONT,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL   FEMT,");
                sb.Append("               COLLECTIONS_TBL           CLN,");
                sb.Append("               COLLECTIONS_TRN_TBL       CTRN,");
                sb.Append("               COLLECTIONS_MODE_TRN_TBL  CMOD,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     ICURR,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     JCURR,");
                sb.Append("               HBL_EXP_TBL              HBL,");
                sb.Append("                HAWB_EXP_TBL HAWB");
                sb.Append("         WHERE JCSE.JOB_CARD_TRN_PK = JCONT.JOB_CARD_TRN_FK");
                sb.Append("           AND JCSE.JOB_CARD_TRN_PK = INV.JOB_CARD_FK");
                sb.Append("           AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("           AND AMT.AGENT_MST_PK = INV.AGENT_MST_FK");
                sb.Append("           AND AMT.AGENT_MST_PK = ACEP.AGENT_MST_FK");
                sb.Append("           AND FEMT.FREIGHT_ELEMENT_MST_PK = ACEP.FREIGHT_ELEMENT_MST_FK");
                sb.Append("           AND ACEP.FREIGHT_ELEMENT_MST_FK = INVTRN.COST_FRT_ELEMENT_FK");
                sb.Append("           AND INVTRN.COST_FRT_ELEMENT_FK <> JCONT.COST_ELEMENT_MST_FK");
                sb.Append("           AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("           AND CMOD.COLLECTIONS_TBL_FK = CLN.COLLECTIONS_TBL_PK");
                sb.Append("           AND CMOD.RECD_AMOUNT = INV.NET_INV_AMT");
                sb.Append("           AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("           AND INVTRN.CURRENCY_MST_FK = ICURR.CURRENCY_MST_PK");
                sb.Append("           AND JCONT.CURRENCY_MST_FK = JCURR.CURRENCY_MST_PK");
                sb.Append("           AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND HAWB.JOB_CARD_AIR_EXP_FK(+) = JCSE.JOB_CARD_TRN_PK");
                sb.Append("           AND JCSE.BUSINESS_TYPE=" + BizType);
                sb.Append("           AND JCSE.PROCESS_TYPE=" + Process);
                sb.Append("           AND INV.INV_AGENT_PK NOT IN");
                sb.Append("               (SELECT A.INVOICE_FK FROM AGENT_SETT_TRN_TBL A)");
                sb.Append("           AND INV.AGENT_MST_FK = " + intAgentPk + " ");

                if (strFromDt.Length > 0 & !(strToDt.Length > 0))
                {
                    sb.Append(" AND INV.INVOICE_DATE >= TO_DATE('" + strFromDt + "', '" + dateFormat + "')");
                }
                else if (!(strFromDt.Length > 0) & strToDt.Length > 0)
                {
                    sb.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                }
                else if (strFromDt.Length > 0 & strToDt.Length > 0)
                {
                    sb.Append(" AND INV.INVOICE_DATE BETWEEN TO_DATE('" + strFromDt + "', '" + dateFormat + "') AND");
                    sb.Append(" TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                }
                sb.Append(" ) QRY ORDER BY FRT_ID");

                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "Child");

                DataRelation RELParent = new DataRelation("P_REL", new DataColumn[] {
                    MainDS.Tables[0].Columns["JOB_PK"],
                    MainDS.Tables[0].Columns["INV_PK"]
                }, new DataColumn[] {
                    MainDS.Tables[1].Columns["JOB_PK"],
                    MainDS.Tables[1].Columns["INV_PK"]
                });
                MainDS.Relations.Add(RELParent);
                return MainDS;
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

        #endregion "Fetch New Data "

        #region "Fetch Invoice to Agent Entry Hdr"

        /// <summary>
        /// Fetch_s the agt_ sett_ entry_ HDR.
        /// </summary>
        /// <param name="AGT_SETT_MST_PK">The ag t_ set t_ ms t_ pk.</param>
        /// <returns></returns>
        public object Fetch_Agt_Sett_Entry_Hdr(int AGT_SETT_MST_PK = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT ASMT.AGENT_SETT_MST_PK,");
            sb.Append("       ASMT.SETTLEMENT_NO,");
            sb.Append("       ASMT.AGENT_MST_FK,");
            sb.Append("       AMT.AGENT_ID,");
            sb.Append("       AMT.AGENT_NAME,");
            sb.Append("       ASMT.SETTLEMENT_DATE,");
            sb.Append("       ASMT.BUSINESS_TYPE,");
            sb.Append("       ASMT.PROCESS_TYPE,");
            sb.Append("       ASMT.CURRENCY_MST_FK,");
            sb.Append("       ASMT.FROM_DATE,");
            sb.Append("       ASMT.TO_DATE,");
            sb.Append("       ASMT.TOTAL_AMOUNT");
            sb.Append("  FROM AGENT_SETT_MST_TBL ASMT, AGENT_MST_TBL AMT");
            sb.Append(" WHERE ASMT.AGENT_MST_FK = AMT.AGENT_MST_PK");
            sb.Append("   AND ASMT.AGENT_SETT_MST_PK = " + AGT_SETT_MST_PK);
            sb.Append("");
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

        #endregion "Fetch Invoice to Agent Entry Hdr"

        #region "FetchSavedData"

        /// <summary>
        /// Fetches the saved data.
        /// </summary>
        /// <param name="SettRefPK">The sett reference pk.</param>
        /// <returns></returns>
        public DataSet FetchSavedData(int SettRefPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            DataSet MainDS = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();
            try
            {
                sb.Append("SELECT ROWNUM SL_NR, QRY.*");
                sb.Append("    FROM( SELECT DISTINCT ASTT.AGENT_SETT_TRN_PK,");
                sb.Append("                ASTT.AGENT_SETT_MST_FK,");
                sb.Append("                ASTT.JOB_CARD_FK JOB_PK,");
                sb.Append("                ASTT.J_REF_NO REF_NO,");
                sb.Append("                ASTT.HBL_FK HBL_PK,");
                sb.Append("                ASTT.HBL_NO HBL_NO,");
                sb.Append("                ASTT.INVOICE_FK INV_PK,");
                sb.Append("                ASTT.INV_NO INV_NO,");
                sb.Append("                ASTT.INV_CURR_FK INV_CURR_PK,");
                sb.Append("                I_CURR.CURRENCY_ID INV_CURR,");
                sb.Append("                ASTT.INVOICE_AMOUNT INV_AMT,");
                sb.Append("                ASTT.COLL_FK,");
                sb.Append("                ASTT.COLL_CURR_FK COLL_CURR_PK,");
                sb.Append("                C_CURR.CURRENCY_ID COLL_CURR,");
                sb.Append("                ASTT.COLL_AMOUNT COLL_AMT,");
                sb.Append("                ASTT.PROFIT_SHARING_AMOUNT PROFIT_SHARING,");
                sb.Append("                '' SELFLAG,");
                sb.Append("                '' CHGFLAG,");
                sb.Append("                '' DELFLAG");
                sb.Append("  FROM AGENT_SETT_MST_TBL     ASMT,");
                sb.Append("       AGENT_SETT_TRN_TBL     ASTT,");
                sb.Append("       AGENT_SETT_FRT_DTL_TBL ASFT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  I_CURR,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  C_CURR");
                sb.Append(" WHERE ASMT.AGENT_SETT_MST_PK = ASTT.AGENT_SETT_MST_FK");
                sb.Append("   AND ASTT.AGENT_SETT_TRN_PK = ASFT.AGENT_SETT_TRN_FK(+)");
                sb.Append("   AND ASTT.INV_CURR_FK = I_CURR.CURRENCY_MST_PK");
                sb.Append("   AND ASTT.COLL_CURR_FK = C_CURR.CURRENCY_MST_PK");
                sb.Append("   AND ASMT.AGENT_SETT_MST_PK = " + SettRefPK);

                sb.Append(" ) QRY");
                sb.Append("");

                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "Parent");

                sb.Length = 0;
                sb.Append("SELECT ASFT.AGENT_SETT_FRT_DTL_PK,");
                sb.Append("       ASFT.AGENT_SETT_TRN_FK,");
                sb.Append("       ASFT.JOB_PK,");
                sb.Append("       ASFT.INV_PK,");
                sb.Append("       ASFT.FREIGHT_ELEMENT_MST_FK FRT_PK,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_ID FRT_ID,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME FRT_NAME,");
                sb.Append("       ASFT.E_CURRENCY_MST_FK E_CURR_PK,");
                sb.Append("       E_CURR.CURRENCY_ID E_CURR_ID,");
                sb.Append("       ASFT.E_AMOUNT E_AMOUNT,");
                sb.Append("       ASFT.E_ROE E_ROE,");
                sb.Append("       ASFT.E_VALUE E_VALUE,");
                sb.Append("       ASFT.R_CURRENCY_MST_FK R_CURR_PK,");
                sb.Append("       R_CURR.CURRENCY_ID R_CURR_ID,");
                sb.Append("       ASFT.R_AMOUNT R_AMOUNT,");
                sb.Append("       ASFT.R_ROE R_ROE,");
                sb.Append("       ASFT.R_VALUE R_VALUE,");
                sb.Append("       ASFT.P_AMOUNT PROFIT,");
                sb.Append("       ASFT.P_SHARE P_SHARE,");
                sb.Append("       ASFT.P_NET_TOTAL P_VALUE,");
                sb.Append("       '' CHGFLAG,");
                sb.Append("       '' DELFLAG");
                sb.Append("  FROM AGENT_SETT_MST_TBL      ASMT,");
                sb.Append("       AGENT_SETT_TRN_TBL      ASTT,");
                sb.Append("       AGENT_SETT_FRT_DTL_TBL  ASFT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   E_CURR,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   R_CURR,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT");
                sb.Append(" WHERE ASMT.AGENT_SETT_MST_PK = ASTT.AGENT_SETT_MST_FK");
                sb.Append("   AND ASTT.AGENT_SETT_TRN_PK = ASFT.AGENT_SETT_TRN_FK(+)");
                sb.Append("   AND ASFT.E_CURRENCY_MST_FK = E_CURR.CURRENCY_MST_PK(+)");
                sb.Append("   AND ASFT.R_CURRENCY_MST_FK = R_CURR.CURRENCY_MST_PK(+)");
                sb.Append("   AND ASFT.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND ASMT.AGENT_SETT_MST_PK = " + SettRefPK);
                sb.Append("");

                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "Child");

                DataRelation RELParent = new DataRelation("P_REL", new DataColumn[] {
                    MainDS.Tables[0].Columns["JOB_PK"],
                    MainDS.Tables[0].Columns["INV_PK"]
                }, new DataColumn[] {
                    MainDS.Tables[1].Columns["JOB_PK"],
                    MainDS.Tables[1].Columns["INV_PK"]
                });
                MainDS.Relations.Add(RELParent);
                return MainDS;
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

        #endregion "FetchSavedData"

        #region " Save"

        /// <summary>
        /// Saves the settlment.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="PROCESS">The process.</param>
        /// <returns></returns>
        public ArrayList SaveSettlment(DataSet M_DataSet, DataSet dsGrid, int PROCESS = 1)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            long AgentSettPK = 0;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            try
            {
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".AGENT_SETT_MST_TBL_PKG.AGENT_SETT_MST_TBL_INS";
                var _with2 = _with1.Parameters;
                _with2.Add("SETTLEMENT_NO_IN", M_DataSet.Tables[0].Rows[0]["SETTLEMENT_NO"]).Direction = ParameterDirection.Input;
                _with2.Add("AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["AGENT_MST_FK"]).Direction = ParameterDirection.Input;
                _with2.Add("SETTLEMENT_DATE_IN", M_DataSet.Tables[0].Rows[0]["SETTLEMENT_DATE"]).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", M_DataSet.Tables[0].Rows[0]["BUSINESS_TYPE"]).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_TYPE_IN", M_DataSet.Tables[0].Rows[0]["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                _with2.Add("CURRENCY_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                _with2.Add("FROM_DATE_IN", M_DataSet.Tables[0].Rows[0]["FROM_DATE"]).Direction = ParameterDirection.Input;
                _with2.Add("TO_DATE_IN", M_DataSet.Tables[0].Rows[0]["TO_DATE"]).Direction = ParameterDirection.Input;
                _with2.Add("TOTAL_AMOUNT_IN", M_DataSet.Tables[0].Rows[0]["TOTAL_AMOUNT"]).Direction = ParameterDirection.Input;
                _with2.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                _with2.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                var _with3 = updCommand;
                updCommand.Parameters.Clear();
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".AGENT_SETT_MST_TBL_PKG.AGENT_SETT_MST_TBL_UPD";
                var _with4 = _with3.Parameters;
                _with4.Add("AGENT_SETT_MST_PK_IN", M_DataSet.Tables[0].Rows[0]["AGENT_SETT_MST_PK"]).Direction = ParameterDirection.Input;
                _with4.Add("SETTLEMENT_NO_IN", M_DataSet.Tables[0].Rows[0]["SETTLEMENT_NO"]).Direction = ParameterDirection.Input;
                _with4.Add("AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["AGENT_MST_FK"]).Direction = ParameterDirection.Input;
                _with4.Add("SETTLEMENT_DATE_IN", M_DataSet.Tables[0].Rows[0]["SETTLEMENT_DATE"]).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", M_DataSet.Tables[0].Rows[0]["BUSINESS_TYPE"]).Direction = ParameterDirection.Input;
                _with4.Add("PROCESS_TYPE_IN", M_DataSet.Tables[0].Rows[0]["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                _with4.Add("CURRENCY_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                _with4.Add("FROM_DATE_IN", M_DataSet.Tables[0].Rows[0]["FROM_DATE"]).Direction = ParameterDirection.Input;
                _with4.Add("TO_DATE_IN", M_DataSet.Tables[0].Rows[0]["TO_DATE"]).Direction = ParameterDirection.Input;
                _with4.Add("TOTAL_AMOUNT_IN", M_DataSet.Tables[0].Rows[0]["TOTAL_AMOUNT"]).Direction = ParameterDirection.Input;
                _with4.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with4.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                var _with5 = objWK.MyDataAdapter;
                if (Convert.ToString(M_DataSet.Tables[0].Rows[0]["AGENT_SETT_MST_PK"]) != null)
                {
                    _with5.InsertCommand = insCommand;
                    _with5.InsertCommand.Transaction = TRAN;
                    RecAfct = _with5.InsertCommand.ExecuteNonQuery();
                    AgentSettPK = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);
                }
                else
                {
                    _with5.UpdateCommand = updCommand;
                    _with5.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with5.UpdateCommand.ExecuteNonQuery();
                    AgentSettPK = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);
                }

                if (RecAfct > 0)
                {
                    //arrMessage = SaveSettTrnDetails(Convert.ToInt32(AgentSettPK), dsGrid, TRAN);

                    if (arrMessage.Count == 0)
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        arrMessage.Add(AgentSettPK);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                else
                {
                    arrMessage.Add("Error");
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException oraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(oraEx.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
        }

        #endregion " Save"

        #region " Protocol Reference Int32"

        /// <summary>
        /// Generates the sett no protocol.
        /// </summary>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmployeeId">The n employee identifier.</param>
        /// <param name="ObjWK">The object wk.</param>
        /// <returns></returns>
        public string GenerateSettNoProtocol(long nLocationId, long nEmployeeId, WorkFlow ObjWK = null)
        {
            string GenerateSettNo = null;
            try
            {
                GenerateSettNo = GenerateProtocolKey("AGENT SETTLEMENT", nLocationId, nEmployeeId, DateTime.Now, "", "", "", 0, ObjWK);
                return GenerateSettNo;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Protocol Reference Int32"
    }
}