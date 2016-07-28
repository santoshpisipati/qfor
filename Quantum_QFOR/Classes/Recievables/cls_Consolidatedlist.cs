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
    public class cls_Consolidatedlist : CommonFeatures
    {

        #region "Fetch All Consol List Records"
        public int GetCLCount(string clRefNr, int clsPk, long locPk)
        {
            System.Text.StringBuilder strCLQuery = new System.Text.StringBuilder(5000);
            strCLQuery.Append(" select cit.consol_invoice_pk, cit.invoice_ref_no");
            strCLQuery.Append(" from consol_invoice_tbl cit , user_mst_tbl umt");
            strCLQuery.Append(" where cit.invoice_ref_no like '%" + clRefNr + "%'");
            strCLQuery.Append(" and cit.created_by_fk = umt.user_mst_pk");
            strCLQuery.Append(" and umt.default_location_fk=" + locPk);
            WorkFlow objWF = new WorkFlow();
            DataSet objCLDS = new DataSet();
            objCLDS = objWF.GetDataSet(strCLQuery.ToString());
            if (objCLDS.Tables[0].Rows.Count == 1)
            {
                clsPk = Convert.ToInt32(objCLDS.Tables[0].Rows[0][0]);
            }
            return objCLDS.Tables[0].Rows.Count;
        }
        #endregion

        #region "Customer Invoice Data"
        #region "Parent"
        public Int32 FetchBusinessType(Int32 userpk)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strsql.Append("select users.business_type from user_mst_tbl users where users.user_mst_pk=" + userpk);
                return Convert.ToInt32(objWF.ExecuteScaler(strsql.ToString()));
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
        public DataSet FetchListData(string strInvRefNo = "", string strJobRefNo = "", string strCBJC = "", string strTPTNr = "", string strDemPKList = "", string strHblRefNo = "", string strCustomer = "", string strVessel = "", string SortColumn = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, int InvType = 0, string SortType = " DESC ", long usrLocFK = 0, short BizType = 1, short process = 1, short JobType = 0, string uniqueRefNr = "", Int32 flag = 0, bool SuppInv = false,
        int Polpk = 0, int Podpk = 0, string Mbl = "", string MblNr = "", string currencypk = "", bool active = false, string fromDate = "", string ToDate = "", bool EDI = false, short EDI_STATUS = -1,
        short AGENT_INV_FLAG = -1, short SHOW_INV_ANYWAY = 0, short AutoManual = 2)
        {


            if (AGENT_INV_FLAG == 1)
            {
                DataSet DS_Agent = new DataSet();
                DS_Agent = FetchInvAgentListData(strInvRefNo, strJobRefNo, strHblRefNo, strCustomer, strVessel, SortColumn, CurrentPage, TotalPage, InvType, SortType,
                usrLocFK, BizType, process, uniqueRefNr, flag, SuppInv, Polpk, Podpk, Mbl, currencypk,
                active, fromDate, ToDate, EDI, EDI_STATUS);
                return DS_Agent;
            }

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
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
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK PK,");
            sb.Append("                INV.INVOICE_REF_NO,");
            sb.Append("                INV.INVOICE_DATE,");
            sb.Append("                CMT.CUSTOMER_NAME,");
            sb.Append("          INV.INVOICE_AMT,");
            sb.Append("          INV.Discount_Amt,");
            sb.Append("                NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("                      FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("                     WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            sb.Append("                    0) RECIEVED,");
            sb.Append("                NVL((INV.NET_RECEIVABLE - NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
            sb.Append("                                                FROM WRITEOFF_MANUAL_TBL WMT");
            sb.Append("                                               WHERE WMT.CONSOL_INVOICE_FK =");
            sb.Append("                                                     INV.CONSOL_INVOICE_PK),");
            sb.Append("                                              0.00) -");
            sb.Append("                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("                           FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("                          WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            sb.Append("                         0.00)),");
            sb.Append("                    0) BALANCE,");
            sb.Append("                CUMT.CURRENCY_ID,");
            sb.Append("                INV.INV_UNIQUE_REF_NR,");
            sb.Append("                DECODE(INV.AUTO_MANUAL_INV, '0','MANUAL','1','AUTO') AUTO_MANUAL_INV,");
            sb.Append(" DECODE(INV.CHK_INVOICE,0,'Pending',1,'Approved',2,'Cancelled') CHK_INVOICE, ");
            if (BizType == 1)
            {
                sb.Append("                1 CARGO_TYPE,");
            }
            else
            {
                sb.Append("                CASE");
                sb.Append("                  WHEN CT.CARGO_TYPE = 1 OR CT.CARGO_TYPE = 2 THEN");
                sb.Append("                   1");
                sb.Append("                  ELSE");
                sb.Append("                   4");
                sb.Append("                END CARGO_TYPE,");
            }
            sb.Append("                DECODE(INV.EDI_STATUS,");
            sb.Append("                       '0',");
            sb.Append("                       'Not Generated',");
            sb.Append("                       '1',");
            sb.Append("                       'Transmitted') EDI_STATUS,");
            sb.Append("                '' SEL,");
            sb.Append("                CMT.CUSTOMER_MST_PK AGENT_CUST_FK,");
            sb.Append("                INV.EXCH_RATE_TYPE_FK CB_OR_DP, 'Customs Brokerage' JOBTYPE");
            sb.Append("  FROM CONSOL_INVOICE_TBL     INV,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
            sb.Append("       CBJC_TBL               CT,");
            sb.Append("       CUSTOMER_MST_TBL       CMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL  CUMT,");
            sb.Append("       USER_MST_TBL           UMT,");
            sb.Append("       VESSEL_VOYAGE_TRN      VTRN,");
            sb.Append("       PORT_MST_TBL           POL,");
            sb.Append("       PORT_MST_TBL           POD");
            sb.Append(" WHERE INVTRN.JOB_CARD_FK = CT.CBJC_PK");
            sb.Append("   AND CT.POL_MST_FK = POL.PORT_MST_PK(+)");
            sb.Append("   AND CT.POD_MST_FK = POD.PORT_MST_PK(+)");
            sb.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CT.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            sb.Append("   AND INV.IS_FAC_INV = 0 AND INVTRN.JOB_TYPE=2");
            if (process == 1)
            {
                sb.Append("   AND POL.LOCATION_MST_FK = " + usrLocFK);
            }
            else
            {
                sb.Append("   AND POD.LOCATION_MST_FK = " + usrLocFK);
            }
            sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            sb.Append("   AND INV.PROCESS_TYPE = " + process);
            sb.Append("   AND INV.BUSINESS_TYPE = " + BizType);
            if (active == true)
            {
                sb.Append("   AND (INV.NET_RECEIVABLE -");
                sb.Append("       NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("              FROM COLLECTIONS_TRN_TBL CTRN");
                sb.Append("             WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
                sb.Append("            0)) > 0");
            }
            else
            {
                sb.Append("   AND (INV.NET_RECEIVABLE -");
                sb.Append("       NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("              FROM COLLECTIONS_TRN_TBL CTRN");
                sb.Append("             WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
                sb.Append("            0)) <= 0");
            }

            if (Polpk > 0)
            {
                sb.Append("AND POL.Port_Mst_Pk = " + Polpk );
            }
            if (Podpk > 0)
            {
                sb.Append("AND POD.Port_Mst_Pk = " + Podpk );
            }
            if (!string.IsNullOrEmpty(strHblRefNo))
            {
                sb.Append(" AND CT.hbl_no='" + strHblRefNo + "'");
            }
            if (!string.IsNullOrEmpty(MblNr))
            {
                sb.Append(" AND CT.mbl_no='" + MblNr + "'");
            }
            if (!string.IsNullOrEmpty(strVessel) & strVessel != "0")
            {
                if (BizType == 2)
                {
                    sb.Append(" AND CT.VOYAGE_TRN_FK = " + strVessel );
                }
                else
                {
                    sb.Append(" AND CT.flight_no  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }
            if (!string.IsNullOrEmpty(currencypk))
            {
                sb.Append(" AND CUMT.CURRENCY_MST_PK='" + currencypk + "'");
            }
            if (!string.IsNullOrEmpty(strCBJC))
            {
                sb.Append(" AND CT.CBJC_NO='" + strCBJC + "'");
            }
            if (!string.IsNullOrEmpty(strCustomer))
            {
                sb.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "");
            }
            if (!string.IsNullOrEmpty(strInvRefNo))
            {
                sb.Append(" AND INV.INVOICE_REF_NO = '" + strInvRefNo + "' ");
            }
            if (!string.IsNullOrEmpty(uniqueRefNr))
            {
                sb.Append(" AND INV.INV_UNIQUE_REF_NR LIKE '%" + uniqueRefNr.Replace("'", "''") + "%' ");
            }
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                sb.Append(" AND TO_DATE(INV.INVOICE_DATE,DATEFORMAT) >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                sb.Append(" AND TO_DATE(INV.INVOICE_DATE,DATEFORMAT) <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (InvType > 0)
            {
                if (InvType == 1)
                {
                    sb.Append(" AND NVL(INV.CHK_INVOICE,0) = 1");
                    //'Approved
                }
                else if (InvType == 2)
                {
                    sb.Append(" AND NVL(INV.CHK_INVOICE,0) = 0");
                    //'Pending
                }
                else
                {
                    sb.Append(" AND NVL(INV.CHK_INVOICE,0) = 2");
                    //'cancelled
                }
            }
            if (AutoManual < 2)
            {
                sb.Append("   AND INV.AUTO_MANUAL_INV = " + AutoManual);
            }
            if (EDI_STATUS > -1)
            {
                sb.Append(" AND NVL(INV.EDI_STATUS,0) = " + EDI_STATUS);
            }
            if (JobType != 0)
            {
                sb.Append("   AND INVTRN.JOB_TYPE = " + JobType);
            }
            sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
            sb.Append("          INV.INVOICE_REF_NO,");
            sb.Append("          INV.INVOICE_DATE,");
            sb.Append("          CUMT.CURRENCY_ID,");
            sb.Append("          CMT.CUSTOMER_NAME,");
            sb.Append("          INV.EDI_STATUS,");
            sb.Append("          INV.AUTO_MANUAL_INV,");
            sb.Append("          CMT.CUSTOMER_MST_PK,");
            sb.Append("          INV.EXCH_RATE_TYPE_FK,");
            sb.Append("          INV.NET_RECEIVABLE,");
            sb.Append("          INV.INVOICE_AMT,");
            sb.Append("          INV.Discount_Amt,");
            sb.Append("          INV.CREATED_DT,");
            sb.Append("          INV.INV_UNIQUE_REF_NR,");
            sb.Append("          INV.CHK_INVOICE,");
            sb.Append("          CT.CARGO_TYPE");

            System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
            sb1.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK PK,");
            sb1.Append("                INV.INVOICE_REF_NO,");
            sb1.Append("                INV.INVOICE_DATE,");
            sb1.Append("                CMT.CUSTOMER_NAME,");
            sb1.Append("                  INV.INVOICE_AMT,");
            sb1.Append("                  INV.Discount_Amt,");
            sb1.Append("                NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb1.Append("                      FROM COLLECTIONS_TRN_TBL CTRN");
            sb1.Append("                     WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            sb1.Append("                    0) RECIEVED,");
            sb1.Append("                NVL((INV.NET_RECEIVABLE - NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
            sb1.Append("                                                FROM WRITEOFF_MANUAL_TBL WMT");
            sb1.Append("                                               WHERE WMT.CONSOL_INVOICE_FK =");
            sb1.Append("                                                     INV.CONSOL_INVOICE_PK),");
            sb1.Append("                                              0.00) -");
            sb1.Append("                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb1.Append("                           FROM COLLECTIONS_TRN_TBL CTRN");
            sb1.Append("                          WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
            sb1.Append("                         0.00)),");
            sb1.Append("                    0) BALANCE,");
            sb1.Append("                CUMT.CURRENCY_ID,");
            sb1.Append("                INV.INV_UNIQUE_REF_NR,");
            sb1.Append("                DECODE(INV.AUTO_MANUAL_INV, '0','MANUAL','1','AUTO') AUTO_MANUAL_INV,");
            sb1.Append("                DECODE(INV.CHK_INVOICE,0,'Pending',1,'Approved',2,'Cancelled') CHK_INVOICE, ");
            if (BizType == 1)
            {
                sb1.Append("                1 CARGO_TYPE,");
            }
            else
            {
                sb1.Append("                CASE");
                sb1.Append("                  WHEN CT.CARGO_TYPE = 1 OR CT.CARGO_TYPE = 2 THEN");
                sb1.Append("                   1");
                sb1.Append("                  ELSE");
                sb1.Append("                   4");
                sb1.Append("                END CARGO_TYPE,");
            }
            sb1.Append("                DECODE(INV.EDI_STATUS,");
            sb1.Append("                       '0',");
            sb1.Append("                       'Not Generated',");
            sb1.Append("                       '1',");
            sb1.Append("                       'Transmitted') EDI_STATUS,");
            sb1.Append("                '' SEL,");
            sb1.Append("                CMT.CUSTOMER_MST_PK AGENT_CUST_FK,");
            sb1.Append("                INV.EXCH_RATE_TYPE_FK CB_OR_DP,'Transport Note' JOBTYPE");
            sb1.Append("  FROM CONSOL_INVOICE_TBL     INV,");
            sb1.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
            sb1.Append("       TRANSPORT_INST_SEA_TBL               CT,");
            sb1.Append("       CUSTOMER_MST_TBL       CMT,");
            sb1.Append("       CURRENCY_TYPE_MST_TBL  CUMT,");
            sb1.Append("       USER_MST_TBL           UMT,");
            sb1.Append("       VESSEL_VOYAGE_TRN      VTRN");
            sb1.Append(" WHERE INVTRN.JOB_CARD_FK = CT.TRANSPORT_INST_SEA_PK");
            sb1.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            sb1.Append("   AND CT.VSL_VOY_FK = VTRN.VOYAGE_TRN_PK(+)");
            sb1.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            sb1.Append("   AND INV.IS_FAC_INV = 0 AND INVTRN.JOB_TYPE=3");
            sb1.Append("   AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK);
            sb1.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb1.Append("   AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            sb1.Append("   AND INV.PROCESS_TYPE = " + process);
            sb1.Append("   AND INV.BUSINESS_TYPE = " + BizType);
            if (active == true)
            {
                sb1.Append("   AND (INV.NET_RECEIVABLE -");
                sb1.Append("       NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                sb1.Append("              FROM COLLECTIONS_TRN_TBL CTRN");
                sb1.Append("             WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
                sb1.Append("            0)) > 0");
            }
            else
            {
                sb1.Append("   AND (INV.NET_RECEIVABLE -");
                sb1.Append("       NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                sb1.Append("              FROM COLLECTIONS_TRN_TBL CTRN");
                sb1.Append("             WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
                sb1.Append("            0)) <= 0");
            }

            if (Polpk > 0)
            {
                sb1.Append("AND 1=2 ");
            }
            if (Podpk > 0)
            {
                sb1.Append("AND 1=2 ");
            }
            if (!string.IsNullOrEmpty(MblNr))
            {
                sb1.Append(" AND UPPER((SELECT T.BL_NUMBER");
                sb1.Append(" FROM TRANSPORT_TRN_CONT T");
                sb1.Append(" WHERE T.TRANSPORT_INST_FK = CT.TRANSPORT_INST_SEA_PK AND ROWNUM < 2)) LIKE  UPPER('%" + MblNr + "%')");
            }
            if (!string.IsNullOrEmpty(strVessel) & strVessel != "0")
            {
                if (BizType == 2)
                {
                    sb1.Append(" AND CT.VSL_VOY_FK = " + strVessel );
                }
                else
                {
                    sb1.Append(" AND 1=2 ");
                }
            }
            if (!string.IsNullOrEmpty(currencypk))
            {
                sb1.Append(" AND CUMT.CURRENCY_MST_PK='" + currencypk + "'");
            }
            if (!string.IsNullOrEmpty(strTPTNr))
            {
                sb1.Append(" AND CT.TRANS_INST_REF_NO='" + strTPTNr + "'");
            }
            if (!string.IsNullOrEmpty(strCustomer))
            {
                sb1.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "");
            }
            if (!string.IsNullOrEmpty(strInvRefNo))
            {
                sb1.Append(" AND INV.INVOICE_REF_NO = '" + strInvRefNo + "' ");
            }
            if (!string.IsNullOrEmpty(uniqueRefNr))
            {
                sb1.Append(" AND INV.INV_UNIQUE_REF_NR LIKE '%" + uniqueRefNr.Replace("'", "''") + "%' ");
            }
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                sb1.Append(" AND TO_DATE(INV.INVOICE_DATE,DATEFORMAT) >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                sb1.Append(" AND TO_DATE(INV.INVOICE_DATE,DATEFORMAT) <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }
            if (InvType > 0)
            {
                if (InvType == 1)
                {
                    sb1.Append(" AND NVL(INV.CHK_INVOICE,0) = 1");
                    //'Approved
                }
                else if (InvType == 2)
                {
                    sb1.Append(" AND NVL(INV.CHK_INVOICE,0) = 0");
                    //'Pending
                }
                else
                {
                    sb1.Append(" AND NVL(INV.CHK_INVOICE,0) = 2");
                    //'cancelled
                }
            }
            if (AutoManual < 2)
            {
                sb1.Append("   AND INV.AUTO_MANUAL_INV = " + AutoManual);
            }
            if (EDI_STATUS > -1)
            {
                sb1.Append(" AND NVL(INV.EDI_STATUS,0) = " + EDI_STATUS);
            }

            if (JobType != 0)
            {
                sb1.Append("   AND INVTRN.JOB_TYPE = " + JobType);
            }
            sb1.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
            sb1.Append("          INV.INVOICE_REF_NO,");
            sb1.Append("          INV.INVOICE_DATE,");
            sb1.Append("          CUMT.CURRENCY_ID,");
            sb1.Append("          CMT.CUSTOMER_NAME,");
            sb1.Append("          INV.EDI_STATUS,");
            sb1.Append("          INV.AUTO_MANUAL_INV,");
            sb1.Append("          CMT.CUSTOMER_MST_PK,");
            sb1.Append("          INV.EXCH_RATE_TYPE_FK,");
            sb1.Append("          INV.INVOICE_AMT,");
            sb1.Append("          INV.Discount_Amt,");
            sb1.Append("          INV.NET_RECEIVABLE,");
            sb1.Append("          INV.CREATED_DT,");
            sb1.Append("          INV.INV_UNIQUE_REF_NR,");
            sb1.Append("          INV.CHK_INVOICE,");
            sb1.Append("          CT.CARGO_TYPE");

            strCondition.Append(" SELECT DISTINCT PK,");
            strCondition.Append(" INVOICE_REF_NO,");
            strCondition.Append(" INVOICE_DATE,");
            strCondition.Append(" CUSTOMER_NAME,");

            strCondition.Append(" INVOICE_AMT,");
            strCondition.Append(" Discount_Amt,");

            strCondition.Append(" RECIEVED,");
            strCondition.Append(" BALANCE,");
            strCondition.Append(" CURRENCY_ID,");
            strCondition.Append(" INV_UNIQUE_REF_NR,");
            strCondition.Append(" AUTO_MANUAL_INV,");
            strCondition.Append(" CHK_INVOICE,");
            strCondition.Append(" CARGO_TYPE,");
            strCondition.Append(" EDI_STATUS,");
            strCondition.Append(" SEL,");
            strCondition.Append(" AGENT_CUST_FK,");
            strCondition.Append(" CB_OR_DP, '' CONSOL_INVOICE_TRN_PK");
            strCondition.Append(" FROM (SELECT T.*");
            strCondition.Append(" FROM (");

            strCondition.Append(" SELECT DISTINCT INV.CONSOL_INVOICE_PK PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" INV.INVOICE_DATE,");
            strCondition.Append(" CMT.CUSTOMER_NAME,");

            strCondition.Append(" INV.INVOICE_AMT,");
            strCondition.Append(" INV.Discount_Amt,");

            strCondition.Append("NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
            strCondition.Append("          where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0) Recieved,");

            strCondition.Append("NVL((INV.NET_RECEIVABLE - NVL((select sum(WMT.WRITEOFF_AMOUNT) from Writeoff_Manual_Tbl WMT");
            strCondition.Append("  where WMT.CONSOL_INVOICE_FK=INV.CONSOL_INVOICE_PK),0.00)-NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
            strCondition.Append("  where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0.00)),0) Balance,");
            strCondition.Append(" CUMT.CURRENCY_ID,INV.INV_UNIQUE_REF_NR, ");
            strCondition.Append(" DECODE(INV.AUTO_MANUAL_INV,'0','MANUAL','1','AUTO') AUTO_MANUAL_INV, ");
            strCondition.Append(" DECODE(INV.CHK_INVOICE,0,'Pending',1,'Approved',2,'Cancelled') CHK_INVOICE ");
            if (BizType == 2)
            {
                if (process == 1)
                {
                    strCondition.Append(" , CASE WHEN BKS.CARGO_TYPE = 1 OR BKS.CARGO_TYPE = 2 THEN ");
                    strCondition.Append("  1 ELSE 4 END CARGO_TYPE,");
                }
                else
                {
                    strCondition.Append(" , CASE WHEN JOB.CARGO_TYPE = 1 OR JOB.CARGO_TYPE = 2 THEN ");
                    strCondition.Append("  1 ELSE 4 END CARGO_TYPE,");
                }
            }
            else
            {
                strCondition.Append(" ,1 CARGO_TYPE, ");
            }
            strCondition.Append(" DECODE(INV.EDI_STATUS,'0','Not Generated','1','Transmitted') EDI_STATUS, ");
            strCondition.Append(" '' SEL, ");
            strCondition.Append(" CMT.CUSTOMER_MST_PK AGENT_CUST_FK, ");
            strCondition.Append(" INV.EXCH_RATE_TYPE_FK CB_OR_DP,'JobCard' JOBTYPE");
            strCondition.Append(" FROM ");
            strCondition.Append(" CONSOL_INVOICE_TBL INV, ");
            strCondition.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

            if (BizType == 2 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,BOOKING_MST_TBL BKS,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN,");
                strCondition.Append("PORT_MST_TBL    POL,");
                strCondition.Append("PORT_MST_TBL    POD,");
                strCondition.Append("MBL_EXP_TBL MBL");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");

                strCondition.Append(" AND INVTRN.JOB_TYPE=1");

                strCondition.Append(" AND JOB.BOOKING_MST_FK=BKS.BOOKING_MST_PK");
                strCondition.Append(" AND JOB.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND BKS.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                strCondition.Append(" AND BKS.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND hbl.hbl_ref_no='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(MblNr))
                {
                    strCondition.Append("AND MBL.MBL_REF_NO = '" + MblNr + "'");
                }
                if (Polpk > 0)
                {
                    strCondition.Append("AND POL.Port_Mst_Pk = " + Polpk );
                }
                if (Podpk > 0)
                {
                    strCondition.Append("AND POD.Port_Mst_Pk = " + Podpk );
                }

                try
                {
                    if (!string.IsNullOrEmpty(strVessel) & strVessel != "0")
                    {
                        strCondition.Append(" AND JOB.VOYAGE_TRN_FK = " + strVessel );
                    }
                }
                catch (Exception ex)
                {
                }
            }

            if (BizType == 1 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" BOOKING_MST_TBL       BKA,");
                strCondition.Append("PORT_MST_TBL    POL,");
                strCondition.Append("PORT_MST_TBL    POD,");
                strCondition.Append(" HAWB_EXP_TBL  HAWB,");
                strCondition.Append(" MAWB_EXP_TBL MBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND JOB.MBL_MAWB_FK=MBL.MAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND JOB.BOOKING_MST_FK = BKA.BOOKING_MST_PK(+)");
                strCondition.Append(" AND BKA.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                strCondition.Append(" AND BKA.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND INVTRN.JOB_TYPE=1");

                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND HAWB.HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (Polpk > 0)
                {
                    strCondition.Append("AND POL.Port_Mst_Pk = " + Polpk );
                }
                if (Podpk > 0)
                {
                    strCondition.Append("AND POD.Port_Mst_Pk = " + Podpk );
                }
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND HAWB.HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(MblNr))
                {
                    strCondition.Append("AND MBL.MAWB_REF_NO = '" + MblNr + "'");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.VOYAGE_FLIGHT_NO  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }

            if (BizType == 2 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append("PORT_MST_TBL    POL,");
                strCondition.Append("PORT_MST_TBL    POD,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                strCondition.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                strCondition.Append(" AND INVTRN.JOB_TYPE=1");

                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                if (!string.IsNullOrEmpty(strVessel) & strVessel != "0")
                {
                    strCondition.Append(" AND JOB.VOYAGE_TRN_FK = " + strVessel );
                }
                if (Polpk > 0)
                {
                    strCondition.Append(" AND POL.Port_Mst_Pk = " + Polpk );
                }
                if (Podpk > 0)
                {
                    strCondition.Append(" AND POD.Port_Mst_Pk = " + Podpk );
                }
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND JOB.HBL_HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(MblNr))
                {
                    strCondition.Append("AND JOB.MBL_MAWB_REF_NO = '" + MblNr + "'");
                }

                if (BlankGrid == 0)
                {
                }
            }

            if (BizType == 1 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" PORT_MST_TBL    POL,");
                strCondition.Append(" PORT_MST_TBL    POD,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                strCondition.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND INVTRN.JOB_TYPE=1");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.VOYAGE_FLIGHT_NO  LIKE '%" + strVessel.Trim() + "%' ");
                }
                if (Polpk > 0)
                {
                    strCondition.Append(" AND POL.Port_Mst_Pk = " + Polpk );
                }
                if (Podpk > 0)
                {
                    strCondition.Append(" AND POD.Port_Mst_Pk = " + Podpk );
                }
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND JOB.HBL_HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(MblNr))
                {
                    strCondition.Append("AND JOB.MBL_MAWB_REF_NO = '" + MblNr + "'");
                }
                if (BlankGrid == 0)
                {
                }
            }

            if (flag == 0 & SHOW_INV_ANYWAY == 0)
            {
                strCondition.Append(" AND 1=2 ");
            }
            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            strCondition.Append(" AND INV.IS_FAC_INV = 0 ");
            if (process == 1)
            {
                strCondition.Append(" AND  pol.location_mst_fk = " + usrLocFK + " ");
            }
            else
            {
                strCondition.Append(" AND  pod.location_mst_fk = " + usrLocFK + " ");
            }

            strCondition.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            strCondition.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            strCondition.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
            strCondition.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
            if (SuppInv == true)
            {
                strCondition.Append(" AND INV.INV_TYPE = 1 ");
            }
            if (!EDI & SHOW_INV_ANYWAY == 0)
            {
                if (active == true)
                {
                    strCondition.Append(" AND (INV.NET_RECEIVABLE - NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
                    strCondition.Append("   where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0)) > 0");
                }
                else
                {
                    strCondition.Append(" AND (INV.NET_RECEIVABLE - NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
                    strCondition.Append("   where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0)) <= 0");
                }
            }

            if (!string.IsNullOrEmpty(currencypk))
            {
                strCondition.Append(" AND CUMT.CURRENCY_MST_PK='" + currencypk + "'");
            }
            if (!string.IsNullOrEmpty(strJobRefNo))
            {
                strCondition.Append(" AND JOB.JOBCARD_REF_NO='" + strJobRefNo + "'");
            }

            if (!string.IsNullOrEmpty(strCustomer))
            {
                strCondition.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "");
            }

            if (!string.IsNullOrEmpty(strInvRefNo))
            {
                strCondition.Append(" AND INV.INVOICE_REF_NO = '" + strInvRefNo + "' ");
            }

            if (!string.IsNullOrEmpty(uniqueRefNr))
            {
                strCondition.Append(" AND INV.INV_UNIQUE_REF_NR LIKE '%" + uniqueRefNr.Replace("'", "''") + "%' ");
            }

            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                strCondition.Append(" AND INV.INVOICE_DATE  >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                strCondition.Append(" AND INV.INVOICE_DATE  <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            if (EDI_STATUS > -1)
            {
                strCondition.Append(" AND NVL(INV.EDI_STATUS,0) = " + EDI_STATUS);
            }
            if (AutoManual < 2)
            {
                strCondition.Append("   AND INV.AUTO_MANUAL_INV = " + AutoManual);
            }
            if (JobType != 0)
            {
                strCondition.Append("   AND INVTRN.JOB_TYPE = " + JobType);
            }
            if (InvType > 0)
            {
                if (InvType == 1)
                {
                    strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 1");
                }
                else if (InvType == 2)
                {
                    strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 0");
                }
                else
                {
                    strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 2");
                }
            }

            strCondition.Append(" GROUP BY");
            strCondition.Append(" INV.CONSOL_INVOICE_PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" INV.INVOICE_DATE ,");
            strCondition.Append(" CUMT.CURRENCY_ID,CMT.CUSTOMER_NAME,");
            strCondition.Append(" INV.EDI_STATUS,");
            strCondition.Append(" INV.AUTO_MANUAL_INV,");
            strCondition.Append(" CMT.CUSTOMER_MST_PK,");
            strCondition.Append(" INV.EXCH_RATE_TYPE_FK,");
            strCondition.Append(" INV.INVOICE_AMT,");
            strCondition.Append(" INV.Discount_Amt,");
            strCondition.Append(" INV.NET_RECEIVABLE,INV.CREATED_DT,INV.INV_UNIQUE_REF_NR, INV.CHK_INVOICE ");

            if (BizType == 2)
            {
                if (process == 1)
                {
                    strCondition.Append(" ,BKS.CARGO_TYPE ");
                }
                else
                {
                    strCondition.Append(" ,JOB.CARGO_TYPE ");
                }
            }
            strCondition.Append(" UNION ");
            strCondition.Append(" " + sb.ToString() + " ");
            strCondition.Append(" UNION ");
            strCondition.Append(" " + sb1.ToString() + " ");

            ///''
            if (process == 2)
            {
                strCondition.Append(" UNION SELECT DISTINCT INV.CONSOL_INVOICE_PK PK,");
                strCondition.Append("                INV.INVOICE_REF_NO,");
                strCondition.Append("                INV.INVOICE_DATE,");
                strCondition.Append("                CMT.CUSTOMER_NAME,");
                strCondition.Append("                INV.INVOICE_AMT,");
                strCondition.Append("                INV.Discount_Amt,");

                strCondition.Append("                NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                strCondition.Append("                      FROM COLLECTIONS_TRN_TBL CTRN");
                strCondition.Append("                     WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
                strCondition.Append("                    0) RECIEVED,");
                strCondition.Append("                NVL((INV.NET_RECEIVABLE - NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
                strCondition.Append("                                                FROM WRITEOFF_MANUAL_TBL WMT");
                strCondition.Append("                                               WHERE WMT.CONSOL_INVOICE_FK =");
                strCondition.Append("                                                     INV.CONSOL_INVOICE_PK),");
                strCondition.Append("                                              0.00) -");
                strCondition.Append("                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                strCondition.Append("                           FROM COLLECTIONS_TRN_TBL CTRN");
                strCondition.Append("                          WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
                strCondition.Append("                         0.00)),");
                strCondition.Append("                    0) BALANCE,");
                strCondition.Append("                CUMT.CURRENCY_ID,");
                strCondition.Append("                INV.INV_UNIQUE_REF_NR,");
                strCondition.Append("    DECODE(INV.AUTO_MANUAL_INV,'0','MANUAL','1','AUTO') AUTO_MANUAL_INV, ");
                strCondition.Append("      DECODE(INV.CHK_INVOICE,0,'Pending',1,'Approved',2,'Cancelled') CHK_INVOICE, ");
                strCondition.Append("                CASE");
                strCondition.Append("                  WHEN DCH.CARGO_TYPE = 1 OR DCH.CARGO_TYPE = 2 THEN");
                strCondition.Append("                   1");
                strCondition.Append("                  ELSE");
                strCondition.Append("                   4");
                strCondition.Append("                END CARGO_TYPE,");
                strCondition.Append("                DECODE(INV.EDI_STATUS,");
                strCondition.Append("                       '0',");
                strCondition.Append("                       'Not Generated',");
                strCondition.Append("                       '1',");
                strCondition.Append("                       'Transmitted') EDI_STATUS,");
                strCondition.Append("                '' SEL,");
                strCondition.Append("                CMT.CUSTOMER_MST_PK AGENT_CUST_FK,");
                strCondition.Append("                INV.EXCH_RATE_TYPE_FK CB_OR_DP,");
                strCondition.Append("                'Det.&Dem.' JOBTYPE");
                strCondition.Append("  FROM CONSOL_INVOICE_TBL     INV,");
                strCondition.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
                strCondition.Append("       DEM_CALC_HDR           DCH,");
                strCondition.Append("       TRANSPORT_INST_SEA_TBL TPN,");
                strCondition.Append("       CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append("       PORT_MST_TBL           POD,");
                strCondition.Append("       CUSTOMER_MST_TBL       CMT");
                strCondition.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                strCondition.Append("   AND INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK");
                strCondition.Append("   AND DCH.DOC_REF_FK = TPN.TRANSPORT_INST_SEA_PK");
                strCondition.Append("   AND DCH.DOC_TYPE = 0");
                strCondition.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                strCondition.Append("   AND POD.PORT_MST_PK = TPN.POD_FK");

                if (flag == 0 & SHOW_INV_ANYWAY == 0)
                {
                    strCondition.Append(" AND 1 = 2 ");
                }
                strCondition.Append(" AND INV.IS_FAC_INV = 0 ");

                strCondition.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
                strCondition.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
                if (SuppInv == true)
                {
                    strCondition.Append(" AND INV.INV_TYPE = 1 ");
                }
                if (!EDI & SHOW_INV_ANYWAY == 0)
                {
                    if (active == true)
                    {
                        strCondition.Append(" AND (INV.NET_RECEIVABLE - NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
                        strCondition.Append("   where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0)) > 0");
                    }
                    else
                    {
                        strCondition.Append(" AND (INV.NET_RECEIVABLE - NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
                        strCondition.Append("   where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0)) <= 0");
                    }
                }

                if (!string.IsNullOrEmpty(currencypk))
                {
                    strCondition.Append(" AND CUMT.CURRENCY_MST_PK = '" + currencypk + "'");
                }
                if (!string.IsNullOrEmpty(strDemPKList))
                {
                    strCondition.Append(" AND DCH.DEM_CALC_ID = '" + strDemPKList + "'");
                }
                if (!string.IsNullOrEmpty(strCustomer))
                {
                    strCondition.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    strCondition.Append(" AND INV.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }

                if (!string.IsNullOrEmpty(uniqueRefNr))
                {
                    strCondition.Append(" AND INV.INV_UNIQUE_REF_NR LIKE '%" + uniqueRefNr.Replace("'", "''") + "%' ");
                }

                if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
                {
                    strCondition.Append(" AND INV.INVOICE_DATE  >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
                }
                if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
                {
                    strCondition.Append(" AND INV.INVOICE_DATE  <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
                }

                if (EDI_STATUS > -1)
                {
                    strCondition.Append(" AND NVL(INV.EDI_STATUS,0) = " + EDI_STATUS);
                }
                if (JobType != 0)
                {
                    strCondition.Append("   AND INVTRN.JOB_TYPE = " + JobType);
                }
                if (AutoManual < 2)
                {
                    strCondition.Append("   AND INV.AUTO_MANUAL_INV = " + AutoManual);
                }
                if (InvType > 0)
                {
                    if (InvType == 1)
                    {
                        strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 1");
                    }
                    else if (InvType == 2)
                    {
                        strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 0");
                    }
                    else
                    {
                        strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 2");
                    }
                }

                strCondition.Append(" UNION SELECT DISTINCT INV.CONSOL_INVOICE_PK PK,");
                strCondition.Append("                INV.INVOICE_REF_NO,");
                strCondition.Append("                INV.INVOICE_DATE,");
                strCondition.Append("                CMT.CUSTOMER_NAME,");
                strCondition.Append("               INV.INVOICE_AMT,");
                strCondition.Append("                INV.Discount_Amt,");

                strCondition.Append("                NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                strCondition.Append("                      FROM COLLECTIONS_TRN_TBL CTRN");
                strCondition.Append("                     WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
                strCondition.Append("                    0) RECIEVED,");
                strCondition.Append("                NVL((INV.NET_RECEIVABLE - NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
                strCondition.Append("                                                FROM WRITEOFF_MANUAL_TBL WMT");
                strCondition.Append("                                               WHERE WMT.CONSOL_INVOICE_FK =");
                strCondition.Append("                                                     INV.CONSOL_INVOICE_PK),");
                strCondition.Append("                                              0.00) -");
                strCondition.Append("                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                strCondition.Append("                           FROM COLLECTIONS_TRN_TBL CTRN");
                strCondition.Append("                          WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO),");
                strCondition.Append("                         0.00)),");
                strCondition.Append("                    0) BALANCE,");
                strCondition.Append("                CUMT.CURRENCY_ID,");
                strCondition.Append("                INV.INV_UNIQUE_REF_NR,");
                strCondition.Append("     DECODE(INV.AUTO_MANUAL_INV,'0','MANUAL','1','AUTO') AUTO_MANUAL_INV, ");
                strCondition.Append("      DECODE(INV.CHK_INVOICE,0,'Pending',1,'Approved',2,'Cancelled') CHK_INVOICE, ");

                strCondition.Append("                CASE");
                strCondition.Append("                  WHEN DCH.CARGO_TYPE = 1 OR DCH.CARGO_TYPE = 2 THEN");
                strCondition.Append("                   1");
                strCondition.Append("                  ELSE");
                strCondition.Append("                   4");
                strCondition.Append("                END CARGO_TYPE,");
                strCondition.Append("                DECODE(INV.EDI_STATUS,");
                strCondition.Append("                       '0',");
                strCondition.Append("                       'Not Generated',");
                strCondition.Append("                       '1',");
                strCondition.Append("                       'Transmitted') EDI_STATUS,");
                strCondition.Append("                '' SEL,");
                strCondition.Append("                CMT.CUSTOMER_MST_PK AGENT_CUST_FK,");
                strCondition.Append("                INV.EXCH_RATE_TYPE_FK CB_OR_DP,");
                strCondition.Append("                'Det.&Dem.' JOBTYPE");
                strCondition.Append("  FROM CONSOL_INVOICE_TBL     INV,");
                strCondition.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
                strCondition.Append("       DEM_CALC_HDR           DCH,");
                strCondition.Append("       CBJC_TBL               CT,");
                strCondition.Append("       CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append("       PORT_MST_TBL           POD,");
                strCondition.Append("       CUSTOMER_MST_TBL       CMT");
                strCondition.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                strCondition.Append("   AND INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK");
                strCondition.Append("   AND DCH.DOC_REF_FK = CT.CBJC_PK");
                strCondition.Append("   AND DCH.DOC_TYPE = 1");
                strCondition.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                strCondition.Append("   AND POD.PORT_MST_PK = CT.POD_MST_FK");

                if (flag == 0 & SHOW_INV_ANYWAY == 0)
                {
                    strCondition.Append(" AND 1 = 2 ");
                }
                strCondition.Append(" AND INV.IS_FAC_INV = 0 ");

                strCondition.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
                strCondition.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
                if (SuppInv == true)
                {
                    strCondition.Append(" AND INV.INV_TYPE = 1 ");
                }
                if (!EDI & SHOW_INV_ANYWAY == 0)
                {
                    if (active == true)
                    {
                        strCondition.Append(" AND (INV.NET_RECEIVABLE - NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
                        strCondition.Append("   where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0)) > 0");
                    }
                    else
                    {
                        strCondition.Append(" AND (INV.NET_RECEIVABLE - NVL((select sum(ctrn.recd_amount_hdr_curr) from collections_trn_tbl ctrn");
                        strCondition.Append("   where ctrn.invoice_ref_nr like inv.invoice_ref_no), 0)) <= 0");
                    }
                }

                if (!string.IsNullOrEmpty(currencypk))
                {
                    strCondition.Append(" AND CUMT.CURRENCY_MST_PK='" + currencypk + "'");
                }

                if (!string.IsNullOrEmpty(strDemPKList))
                {
                    strCondition.Append(" AND DCH.DEM_CALC_ID = '" + strDemPKList + "'");
                }

                if (!string.IsNullOrEmpty(strCustomer))
                {
                    strCondition.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "");
                }

                if (!string.IsNullOrEmpty(strInvRefNo))
                {
                    strCondition.Append(" AND INV.INVOICE_REF_NO = '" + strInvRefNo + "' ");
                }

                if (!string.IsNullOrEmpty(uniqueRefNr))
                {
                    strCondition.Append(" AND INV.INV_UNIQUE_REF_NR LIKE '%" + uniqueRefNr.Replace("'", "''") + "%' ");
                }

                if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
                {
                    strCondition.Append(" AND INV.INVOICE_DATE  >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
                }
                if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
                {
                    strCondition.Append(" AND INV.INVOICE_DATE  <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
                }

                if (EDI_STATUS > -1)
                {
                    strCondition.Append(" AND NVL(INV.EDI_STATUS,0) = " + EDI_STATUS);
                }
                if (JobType != 0)
                {
                    strCondition.Append("   AND INVTRN.JOB_TYPE = " + JobType);
                }
                if (AutoManual < 2)
                {
                    strCondition.Append("   AND INV.AUTO_MANUAL_INV = " + AutoManual);
                }
                if (InvType > 0)
                {
                    if (InvType == 1)
                    {
                        strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 1");
                    }
                    else if (InvType == 2)
                    {
                        strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 0");
                    }
                    else
                    {
                        strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 2");
                    }
                }
            }

            strCondition.Append(" ) T");
            strCondition.Append(" WHERE 1 = 1");
            if (!string.IsNullOrEmpty(strJobRefNo) & !string.IsNullOrEmpty(strCBJC))
            {
                strCondition.Append(" AND JOBTYPE <> 'Transport Note'");
            }
            else if (!string.IsNullOrEmpty(strJobRefNo) & !string.IsNullOrEmpty(strTPTNr))
            {
                strCondition.Append(" AND JOBTYPE <> 'Customs Brokerage'");
            }
            else if (!string.IsNullOrEmpty(strCBJC) & !string.IsNullOrEmpty(strTPTNr))
            {
                strCondition.Append(" AND JOBTYPE <> 'JobCard'");
            }
            else if (!string.IsNullOrEmpty(strJobRefNo))
            {
                strCondition.Append(" AND JOBTYPE = 'JobCard'");
            }
            else if (!string.IsNullOrEmpty(strCBJC))
            {
                strCondition.Append(" AND JOBTYPE = 'Customs Brokerage'");
            }
            else if (!string.IsNullOrEmpty(strTPTNr))
            {
                strCondition.Append(" AND JOBTYPE = 'Transport Note'");
            }
            else if (!string.IsNullOrEmpty(strDemPKList))
            {
                strCondition.Append(" AND JOBTYPE = 'Det.&Dem.'");
            }
            strCondition.Append(" )");
            strCondition.Append(" ORDER BY INVOICE_DATE DESC,INVOICE_REF_NO DESC ");

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + strCondition.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

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

            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL_NR\", T.*  FROM ");
            sqlstr.Append("  (" + strCondition.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL_NR\"  BETWEEN " + start + " AND " + last);

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
        public DataTable fetchchild(string CONTSpotPKs = "", string strInvRefNo = "", string strJobRefNo = "", string strHblRefNo = "", string strCustomer = "", string strVessel = "", long usrLocFK = 0, short BizType = 1, short process = 1)
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

            System.Text.StringBuilder BuildQuery = new System.Text.StringBuilder();

            string strsql = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dt = null;
            int RowCnt = 0;
            int Rno = 0;
            int pk = 0;
            BuildQuery.Append("SELECT ROWNUM \"SL_NR\", T.*");
            BuildQuery.Append("FROM (");
            BuildQuery.Append(" SELECT INV.CONSOL_INVOICE_PK PK,");
            BuildQuery.Append(" JOB.JOBCARD_REF_NO, ");
            BuildQuery.Append(" 'JobCard' JOB_TYPE,");
            BuildQuery.Append("JOB.JOBCARD_DATE,");

            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" HBL.HBL_REF_NO HBL_REF_NO,");
            }
            else if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" HAWB.HAWB_REF_NO HBL_REF_NO,");
            }
            else if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" JOB.HBL_HAWB_REF_NO HBL_REF_NO,");
            }
            else if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" JOB.HBL_HAWB_REF_NO HBL_REF_NO,");
            }

            if (BizType == 2)
            {
                BuildQuery.Append(" (CASE");
                BuildQuery.Append(" WHEN (NVL(JOB.VESSEL_NAME, '') || '/' ||");
                BuildQuery.Append(" NVL(JOB.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                BuildQuery.Append(" ''");
                BuildQuery.Append(" ELSE");
                BuildQuery.Append(" NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO, '')");
                BuildQuery.Append(" END) AS VESVOYAGE,");
            }
            else if (BizType == 1)
            {
                BuildQuery.Append(" JOB.VOYAGE_FLIGHT_NO VESVOYAGE,");
            }
            BuildQuery.Append(" CUMT.CURRENCY_ID,");
            BuildQuery.Append("  sum(INVTRN.TOT_AMT_IN_LOC_CURR) INVAMT,");
            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK JOBCARD_PK,");
                BuildQuery.Append(" JOB.BOOKING_MST_FK BOOKING_FK,");
                BuildQuery.Append(" JOB.JOB_CARD_STATUS,");
                BuildQuery.Append(" JOB.HBL_HAWB_FK HBL_FK,");
                BuildQuery.Append(" JOB.MBL_MAWB_FK MBL_FK, ");
                BuildQuery.Append(" (SELECT M.MBL_REF_NO FROM MBL_EXP_TBL M WHERE M.MBL_EXP_TBL_PK=");
                BuildQuery.Append("  JOB.MBL_MAWB_FK) MBL_REF_NO, ");
                BuildQuery.Append(" (SELECT H.HBL_STATUS FROM HBL_EXP_TBL H WHERE H.HBL_EXP_TBL_PK=");
                BuildQuery.Append("  JOB.HBL_HAWB_FK) HBL_STATUS ");
            }
            else if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK JOBCARD_PK,");
                BuildQuery.Append(" NULL BOOKING_FK,");
                BuildQuery.Append(" JOB.JOB_CARD_STATUS,");
                BuildQuery.Append(" NULL HBL_FK,");
                BuildQuery.Append(" JOB.MBL_MAWB_FK MBL_FK, ");
                BuildQuery.Append(" (SELECT M.MBL_REF_NO FROM MBL_IMP_TBL M WHERE M.MBL_IMP_TBL_PK=");
                BuildQuery.Append("  JOB.MBL_MAWB_FK) MBL_REF_NO, ");
                BuildQuery.Append(" (SELECT H.HBL_STATUS FROM HBL_EXP_TBL H WHERE UPPER(H.HBL_REF_NO)=");
                BuildQuery.Append("  UPPER(JOB.HBL_HAWB_REF_NO)) HBL_STATUS ");
                //JOB.HBL_HAWB_REF_NO
            }
            else if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK JOBCARD_PK,");
                BuildQuery.Append(" JOB.BOOKING_MST_FK BOOKING_FK,");
                BuildQuery.Append(" JOB.JOB_CARD_STATUS,");
                BuildQuery.Append(" JOB.HBL_HAWB_FK HBL_FK,");
                BuildQuery.Append(" JOB.MBL_MAWB_FK MBL_FK, ");
                BuildQuery.Append(" (SELECT M.MAWB_REF_NO FROM MAWB_EXP_TBL M WHERE M.MAWB_EXP_TBL_PK= ");
                BuildQuery.Append("  JOB.MBL_MAWB_FK) MBL_REF_NO, ");
                BuildQuery.Append(" (SELECT H.HAWB_STATUS FROM HAWB_EXP_TBL H WHERE H.HAWB_EXP_TBL_PK=");
                BuildQuery.Append("  JOB.HBL_HAWB_FK) HBL_STATUS ");
            }
            else if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK JOBCARD_PK,");
                BuildQuery.Append(" NULL BOOKING_FK,");
                BuildQuery.Append(" JOB.JOB_CARD_STATUS,");
                BuildQuery.Append(" NULL HBL_FK,");
                BuildQuery.Append(" JOB.MBL_MAWB_FK MBL_FK, ");
                BuildQuery.Append(" (SELECT M.MBL_REF_NO FROM MBL_IMP_TBL M WHERE M.MBL_IMP_TBL_PK=");
                BuildQuery.Append("  JOB.MBL_MAWB_FK) MBL_REF_NO, ");
                BuildQuery.Append(" (SELECT H.HAWB_STATUS FROM HAWB_EXP_TBL H WHERE UPPER(H.HAWB_REF_NO)=");
                BuildQuery.Append("  UPPER(JOB.HBL_HAWB_REF_NO)) HBL_STATUS ");
            }
            BuildQuery.Append(" FROM ");
            BuildQuery.Append(" CONSOL_INVOICE_TBL INV, ");
            BuildQuery.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" HBL_EXP_TBL            HBL,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT,");
                BuildQuery.Append(" VESSEL_VOYAGE_TRN      VTRN");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK ");
                BuildQuery.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                BuildQuery.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                BuildQuery.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");

            }

            if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" HAWB_EXP_TBL           HAWB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK ");
                BuildQuery.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                BuildQuery.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");

            }

            if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT,");
                BuildQuery.Append(" VESSEL_VOYAGE_TRN      VTRN");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK ");
                BuildQuery.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                BuildQuery.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");

            }
            if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK ");
                BuildQuery.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");

            }

            BuildQuery.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            BuildQuery.Append(" AND INV.IS_FAC_INV = 0 ");
            BuildQuery.Append(" AND INVTRN.JOB_TYPE = 1");
            BuildQuery.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");


            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }

            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" Group by INV.CONSOL_INVOICE_PK, JOB.JOBCARD_REF_NO,JOB.JOBCARD_DATE, ");
                BuildQuery.Append(" HBL.HBL_REF_NO, JOB.VESSEL_NAME,JOB.VOYAGE_FLIGHT_NO,CUMT.CURRENCY_ID,");
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK,JOB.BOOKING_MST_FK, ");
                BuildQuery.Append(" JOB.JOB_CARD_STATUS,JOB.HBL_HAWB_FK,JOB.MBL_MAWB_FK ");
            }
            else if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" Group by INV.CONSOL_INVOICE_PK, JOB.JOBCARD_REF_NO,JOB.JOBCARD_DATE, ");
                BuildQuery.Append(" HAWB.HAWB_REF_NO, JOB.VOYAGE_FLIGHT_NO,CUMT.CURRENCY_ID,");
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK,JOB.BOOKING_MST_FK,");
                BuildQuery.Append(" JOB.JOB_CARD_STATUS,JOB.HBL_HAWB_FK,JOB.MBL_MAWB_FK ");
            }
            else if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" Group by INV.CONSOL_INVOICE_PK, JOB.JOBCARD_REF_NO,JOB.JOBCARD_DATE, ");
                BuildQuery.Append(" JOB.HBL_HAWB_REF_NO, JOB.VESSEL_NAME,JOB.VOYAGE_FLIGHT_NO,CUMT.CURRENCY_ID,");
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK,JOB.JOB_CARD_STATUS,JOB.MBL_MAWB_FK ");
            }
            else if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" Group by INV.CONSOL_INVOICE_PK, JOB.JOBCARD_REF_NO,JOB.JOBCARD_DATE, ");
                BuildQuery.Append(" JOB.HBL_HAWB_REF_NO, JOB.VOYAGE_FLIGHT_NO,CUMT.CURRENCY_ID,");
                BuildQuery.Append(" JOB.JOB_CARD_TRN_PK,JOB.JOB_CARD_STATUS,JOB.MBL_MAWB_FK ");
            }
            BuildQuery.Append(" UNION ");
            BuildQuery.Append(" SELECT INV.CONSOL_INVOICE_PK PK,");
            BuildQuery.Append("       CT.CBJC_NO JOBCARD_REF_NO,");
            BuildQuery.Append("       'Customs Brokerage' JOB_TYPE,");
            BuildQuery.Append("       CT.CBJC_DATE CDATE,");
            BuildQuery.Append("       CT.HBL_NO HBL_REF_NO,");
            BuildQuery.Append(" (CASE");
            BuildQuery.Append(" WHEN (NVL(VES.VESSEL_NAME, '') || '/' ||");
            BuildQuery.Append(" NVL(VTRN.VOYAGE, '') = '/') THEN");
            BuildQuery.Append(" ''");
            BuildQuery.Append(" ELSE");
            BuildQuery.Append(" NVL(VES.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '')");
            BuildQuery.Append(" END) AS VESVOYAGE,");
            BuildQuery.Append("       CUMT.CURRENCY_ID,");
            BuildQuery.Append("       SUM(INVTRN.TOT_AMT_IN_LOC_CURR) INVAMT,");
            BuildQuery.Append("       CT.CBJC_PK JOBCARD_PK,");
            BuildQuery.Append("       0 BOOKING_FK,");
            BuildQuery.Append("       CT.JOB_CARD_STATUS,");
            BuildQuery.Append("       0 HBL_FK,");
            BuildQuery.Append("       0 MBL_FK,");
            BuildQuery.Append("       CT.MBL_NO MBL_REF_NO,");
            BuildQuery.Append("       0 HBL_STATUS");
            BuildQuery.Append("  FROM CONSOL_INVOICE_TBL     INV,");
            BuildQuery.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
            BuildQuery.Append("       CBJC_TBL               CT,");
            BuildQuery.Append("       CUSTOMER_MST_TBL       CMT,");
            BuildQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT,");
            BuildQuery.Append("       USER_MST_TBL           UMT,");
            BuildQuery.Append("       VESSEL_VOYAGE_TBL     VES,");
            BuildQuery.Append("       VESSEL_VOYAGE_TRN      VTRN");
            BuildQuery.Append(" WHERE INVTRN.JOB_CARD_FK = CT.CBJC_PK");
            BuildQuery.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            BuildQuery.Append("   AND VES.VESSEL_VOYAGE_TBL_PK(+) = VTRN.VESSEL_VOYAGE_TBL_FK");
            BuildQuery.Append("   AND CT.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
            BuildQuery.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            BuildQuery.Append("   AND INV.IS_FAC_INV = 0 AND INVTRN.JOB_TYPE=2");
            BuildQuery.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append("   AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }
            BuildQuery.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
            BuildQuery.Append("          CT.MBL_NO,");
            BuildQuery.Append("          CT.CBJC_NO,");
            BuildQuery.Append("          CT.HBL_NO,");
            BuildQuery.Append("          CT.CBJC_PK,");
            BuildQuery.Append("          CT.JOB_CARD_STATUS,");
            BuildQuery.Append("          CT.CBJC_DATE,");
            BuildQuery.Append("          VES.VESSEL_NAME,");
            BuildQuery.Append("          VTRN.VOYAGE,");
            BuildQuery.Append("          CUMT.CURRENCY_ID");

            BuildQuery.Append(" UNION ");
            BuildQuery.Append("SELECT INV.CONSOL_INVOICE_PK PK,");
            BuildQuery.Append("       CT.TRANS_INST_REF_NO JOBCARD_REF_NO,");
            BuildQuery.Append("       'Transport Note' JOB_TYPE,");
            BuildQuery.Append("       CT.TRANS_INST_DATE CDATE,");
            BuildQuery.Append("       '' HBL_REF_NO,");
            BuildQuery.Append(" (CASE");
            BuildQuery.Append(" WHEN (NVL(VES.VESSEL_NAME, '') || '/' ||");
            BuildQuery.Append(" NVL(VTRN.VOYAGE, '') = '/') THEN");
            BuildQuery.Append(" ''");
            BuildQuery.Append(" ELSE");
            BuildQuery.Append(" NVL(VES.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '')");
            BuildQuery.Append(" END) AS VESVOYAGE,");
            BuildQuery.Append("       CUMT.CURRENCY_ID,");
            BuildQuery.Append("       SUM(INVTRN.TOT_AMT_IN_LOC_CURR) INVAMT,");
            BuildQuery.Append("       CT.TRANSPORT_INST_SEA_PK JOBCARD_PK,");
            BuildQuery.Append("       0 BOOKING_FK,");
            BuildQuery.Append("       CT.TP_CLOSE_STATUS,");
            BuildQuery.Append("       0 HBL_FK,");
            BuildQuery.Append("       0 MBL_FK,");
            BuildQuery.Append("       '' MBL_REF_NO,");
            BuildQuery.Append("       0 HBL_STATUS");
            BuildQuery.Append("  FROM CONSOL_INVOICE_TBL     INV,");
            BuildQuery.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
            BuildQuery.Append("       TRANSPORT_INST_SEA_TBL CT,");
            BuildQuery.Append("       CUSTOMER_MST_TBL       CMT,");
            BuildQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT,");
            BuildQuery.Append("       USER_MST_TBL           UMT,");
            BuildQuery.Append("       VESSEL_VOYAGE_TBL     VES,");
            BuildQuery.Append("       VESSEL_VOYAGE_TRN      VTRN");
            BuildQuery.Append(" WHERE INVTRN.JOB_CARD_FK = CT.TRANSPORT_INST_SEA_PK");
            BuildQuery.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            BuildQuery.Append("   AND VES.VESSEL_VOYAGE_TBL_PK = VTRN.VESSEL_VOYAGE_TBL_FK(+)");
            BuildQuery.Append("   AND VES.VESSEL_VOYAGE_TBL_PK(+) = CT.VSL_VOY_FK");
            BuildQuery.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            BuildQuery.Append("   AND INV.IS_FAC_INV = 0");
            BuildQuery.Append("   AND INVTRN.JOB_TYPE = 3");
            BuildQuery.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append("   AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }
            BuildQuery.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
            BuildQuery.Append("          CT.TRANS_INST_REF_NO,");
            BuildQuery.Append("          CT.TP_CLOSE_STATUS,");
            BuildQuery.Append("          VES.VESSEL_NAME,");
            BuildQuery.Append("          VTRN.VOYAGE,");
            BuildQuery.Append("          CT.TRANSPORT_INST_SEA_PK,");
            BuildQuery.Append("          CT.TRANS_INST_DATE,");
            BuildQuery.Append("          CUMT.CURRENCY_ID ");

            if (process == 2)
            {
                BuildQuery.Append(" UNION SELECT INV.CONSOL_INVOICE_PK PK,");
                BuildQuery.Append("       DCH.DEM_CALC_ID JOBCARD_REF_NO,");
                BuildQuery.Append("       'Det.&Dem.' JOB_TYPE,");
                BuildQuery.Append("       DCH.DEM_CALC_DATE CDATE,");
                BuildQuery.Append("       '' HBL_REF_NO,");
                if (BizType == 2)
                {
                    BuildQuery.Append("       (CASE");
                    BuildQuery.Append("         WHEN (NVL(VES.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '') = '/') THEN");
                    BuildQuery.Append("          ''");
                    BuildQuery.Append("         ELSE");
                    BuildQuery.Append("          NVL(VES.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '')");
                    BuildQuery.Append("       END) AS VESVOYAGE,");
                }
                else
                {
                    //    BuildQuery.Append("   TPN.VOYAGE AS VESVOYAGE,")
                    BuildQuery.Append("   '' VESVOYAGE,");
                }
                BuildQuery.Append("       CUMT.CURRENCY_ID,");
                BuildQuery.Append("       SUM(INVTRN.TOT_AMT_IN_LOC_CURR) INVAMT,");
                BuildQuery.Append("       DCH.DEM_CALC_HDR_PK JOBCARD_PK,");
                BuildQuery.Append("       0 BOOKING_FK,");
                BuildQuery.Append("       TPN.TP_CLOSE_STATUS,");
                BuildQuery.Append("       0 HBL_FK,");
                BuildQuery.Append("       0 MBL_FK,");
                BuildQuery.Append("       '' MBL_REF_NO,");
                BuildQuery.Append("       0 HBL_STATUS");
                BuildQuery.Append("  FROM CONSOL_INVOICE_TBL     INV,");
                BuildQuery.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
                BuildQuery.Append("       DEM_CALC_HDR           DCH,");
                BuildQuery.Append("       TRANSPORT_INST_SEA_TBL TPN,");
                BuildQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT");
                if (BizType == 2)
                {
                    BuildQuery.Append(", VESSEL_VOYAGE_TBL      VES,");
                    BuildQuery.Append("  VESSEL_VOYAGE_TRN      VTRN");
                }
                BuildQuery.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                BuildQuery.Append("   AND INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK");
                BuildQuery.Append("   AND DCH.DOC_REF_FK = TPN.TRANSPORT_INST_SEA_PK");
                BuildQuery.Append("   AND DCH.DOC_TYPE = 0");
                if (BizType == 2)
                {
                    BuildQuery.Append("   AND VES.VESSEL_VOYAGE_TBL_PK = VTRN.VESSEL_VOYAGE_TBL_FK(+)");
                    BuildQuery.Append("   AND VES.VESSEL_VOYAGE_TBL_PK(+) = TPN.VSL_VOY_FK");
                }
                BuildQuery.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                BuildQuery.Append("   AND INV.IS_FAC_INV = 0");
                BuildQuery.Append("   AND INVTRN.JOB_TYPE = 4");

                if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
                {
                    BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
                }

                BuildQuery.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                BuildQuery.Append("          DCH.DEM_CALC_ID,");
                BuildQuery.Append("          DCH.DEM_CALC_DATE,");
                if (BizType == 2)
                {
                    BuildQuery.Append("      VES.VESSEL_NAME,");
                    BuildQuery.Append("      VTRN.VOYAGE,");
                }
                else
                {
                    BuildQuery.Append("      TPN.FLIGHT_NO,");
                }
                BuildQuery.Append("          CUMT.CURRENCY_ID,");
                BuildQuery.Append("          DCH.DEM_CALC_HDR_PK,");
                BuildQuery.Append("          TPN.TP_CLOSE_STATUS ");

                BuildQuery.Append(" UNION SELECT INV.CONSOL_INVOICE_PK PK,");
                BuildQuery.Append("       DCH.DEM_CALC_ID JOBCARD_REF_NO,");
                BuildQuery.Append("       'Det.&Dem.' JOB_TYPE,");
                BuildQuery.Append("       DCH.DEM_CALC_DATE CDATE,");
                BuildQuery.Append("       '' HBL_REF_NO,");
                if (BizType == 2)
                {
                    BuildQuery.Append("       (CASE");
                    BuildQuery.Append("         WHEN (NVL(VES.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '') = '/') THEN");
                    BuildQuery.Append("          ''");
                    BuildQuery.Append("         ELSE");
                    BuildQuery.Append("          NVL(VES.VESSEL_NAME, '') || '/' || NVL(VTRN.VOYAGE, '')");
                    BuildQuery.Append("       END) AS VESVOYAGE,");
                }
                else
                {
                    BuildQuery.Append("   CT.FLIGHT_NO AS VESVOYAGE,");
                }
                BuildQuery.Append("       CUMT.CURRENCY_ID,");
                BuildQuery.Append("       SUM(INVTRN.TOT_AMT_IN_LOC_CURR) INVAMT,");
                BuildQuery.Append("       DCH.DEM_CALC_HDR_PK JOBCARD_PK,");
                BuildQuery.Append("       0 BOOKING_FK,");
                BuildQuery.Append("       CT.CBJC_STATUS TP_CLOSE_STATUS,");
                BuildQuery.Append("       0 HBL_FK,");
                BuildQuery.Append("       0 MBL_FK,");
                BuildQuery.Append("       '' MBL_REF_NO,");
                BuildQuery.Append("       0 HBL_STATUS");
                BuildQuery.Append("  FROM CONSOL_INVOICE_TBL     INV,");
                BuildQuery.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
                BuildQuery.Append("       DEM_CALC_HDR           DCH,");
                BuildQuery.Append("       CBJC_TBL               CT,");
                BuildQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT ");
                if (BizType == 2)
                {
                    BuildQuery.Append(", VESSEL_VOYAGE_TBL      VES,");
                    BuildQuery.Append("  VESSEL_VOYAGE_TRN      VTRN");
                }
                BuildQuery.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                BuildQuery.Append("   AND INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK");
                BuildQuery.Append("   AND DCH.DOC_REF_FK = CT.CBJC_PK");
                BuildQuery.Append("   AND DCH.DOC_TYPE = 1");
                if (BizType == 2)
                {
                    BuildQuery.Append("   AND VES.VESSEL_VOYAGE_TBL_PK = VTRN.VESSEL_VOYAGE_TBL_FK(+)");
                    BuildQuery.Append("   AND VTRN.VOYAGE_TRN_PK = CT.VOYAGE_TRN_FK ");
                }
                BuildQuery.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                BuildQuery.Append("   AND INV.IS_FAC_INV = 0");
                BuildQuery.Append("   AND INVTRN.JOB_TYPE = 4");

                if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
                {
                    BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
                }

                BuildQuery.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                BuildQuery.Append("          DCH.DEM_CALC_ID,");
                BuildQuery.Append("          DCH.DEM_CALC_DATE,");
                if (BizType == 2)
                {
                    BuildQuery.Append("      VES.VESSEL_NAME,");
                    BuildQuery.Append("      VTRN.VOYAGE,");
                }
                else
                {
                    BuildQuery.Append("      CT.FLIGHT_NO,");
                }
                BuildQuery.Append("          CUMT.CURRENCY_ID,");
                BuildQuery.Append("          DCH.DEM_CALC_HDR_PK,");
                BuildQuery.Append("          CT.CBJC_STATUS ");
            }

            BuildQuery.Append(" )T ");
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
                    dt.Rows[RowCnt]["SL_NR"] = Rno;
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
        #endregion

        #region "Agent Invoice Data"
        #region "Parent"
        public DataSet FetchInvAgentListData(string strInvRefNo = "", string strJobRefNo = "", string strHblRefNo = "", string strCustomer = "", string strVessel = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int InvType = 0, string SortType = " DESC ",
        long usrLocFK = 0, short BizType = 1, short Process = 1, string uniqueRefNr = "", Int32 flag = 0, bool SuppInv = false, int Polpk = 0, int Podpk = 0, string Mbl = "", string currencypk = "",
        bool active = false, string fromDate = "", string ToDate = "", bool EDI = false, short EDI_STATUS = 0, short AGENT_INV_FLAG = -1)
        {


            if (AGENT_INV_FLAG == 0)
            {
                DataSet DS_Cust = new DataSet();
                DS_Cust = FetchInvAgentListData(strInvRefNo, strJobRefNo, strHblRefNo, strCustomer, strVessel, SortColumn, CurrentPage, TotalPage, InvType, SortType,
                usrLocFK, BizType, Process, uniqueRefNr, flag, SuppInv, Polpk, Podpk, Mbl, currencypk,
                active, fromDate, ToDate, EDI, EDI_STATUS);
                return DS_Cust;
            }
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            StringBuilder strCondition = new StringBuilder();
            string Main_Query = "";
            if (BizType == 2 & Process == 1)
            {
                Main_Query = InvAgentSeaExpHDRQry(strHblRefNo, Mbl, strVessel, Polpk, Podpk);
            }
            else if (BizType == 2 & Process == 2)
            {
                Main_Query = InvAgentSeaImpHDRQry(strHblRefNo, Mbl, strVessel, Polpk, Podpk);
            }
            else if (BizType == 1 & Process == 1)
            {
                Main_Query = InvAgentAirExpHDRQry(strHblRefNo, Mbl, strVessel, Polpk, Podpk);
            }
            else if (BizType == 1 & Process == 2)
            {
                Main_Query = InvAgentAirImpHDRQry(strHblRefNo, Mbl, strVessel, Polpk, Podpk);
            }
            if (Process == 1)
            {
                strCondition.Append(" AND PORT_POL.LOCATION_MST_FK= " + usrLocFK + " ");
            }
            else
            {
                strCondition.Append(" AND PORT_POD.LOCATION_MST_FK= " + usrLocFK + " ");
            }

            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            strCondition.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            if (EDI_STATUS > -1)
            {
                strCondition.Append(" AND NVL(INV.EDI_STATUS,0) = " + EDI_STATUS);
            }

            try
            {
                currencypk = Convert.ToString(getDefault(currencypk, 0));
                if (Convert.ToInt32(currencypk) > 0)
                {
                    strCondition.Append(" AND CUMT.CURRENCY_MST_PK= " + currencypk + " ");
                }
            }
            catch (Exception ex)
            {
            }

            if (!string.IsNullOrEmpty(strJobRefNo))
            {
                strCondition.Append(" AND JOB.JOBCARD_REF_NO='" + strJobRefNo + "'");
            }

            try
            {
                strCustomer = Convert.ToString(strCustomer);
                if (Convert.ToInt32(strCustomer) > 0)
                {
                    strCondition.Append(" AND INV.CB_AGENT_MST_FK=" + strCustomer + "");
                }
            }
            catch (Exception ex)
            {
            }

            if (!string.IsNullOrEmpty(strInvRefNo))
            {
                strCondition.Append(" AND INV.INVOICE_REF_NO = '" + strInvRefNo + "' ");
            }

            if (!string.IsNullOrEmpty(uniqueRefNr))
            {
                strCondition.Append(" AND INV.INV_UNIQUE_REF_NR LIKE '%" + uniqueRefNr.Replace("'", "''") + "%' ");
            }
            if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()) & !string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                strCondition.Append(" AND INV.INVOICE_DATE BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(fromDate, "").ToString()))
            {
                strCondition.Append(" AND INV.INVOICE_DATE  >= TO_DATE('" + fromDate + "' ,'" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(getDefault(ToDate, "").ToString()))
            {
                strCondition.Append(" AND INV.INVOICE_DATE  <= TO_DATE('" + ToDate + "' ,'" + dateFormat + "')");
            }

            if (InvType > 0)
            {
                if (InvType == 1)
                {
                    strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 1");
                }
                else if (InvType == 2)
                {
                    strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 0");
                }
                else
                {
                    strCondition.Append(" AND NVL(INV.CHK_INVOICE,0) = 2");
                }
            }

            if (flag == 0)
            {
                strCondition.Append(" AND 1=2 ");
            }

            Main_Query = Main_Query + " " + strCondition.ToString();
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + Main_Query + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

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

            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL_NR\", T.*  FROM ");
            sqlstr.Append("  (" + Main_Query + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL_NR\"  BETWEEN " + start + " AND " + last);

            string sql = null;
            sql = sqlstr.ToString();
            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;
                if (BizType == 2 & Process == 1)
                {
                    DS.Tables.Add(objWF.GetDataTable(InvAgentSeaExpChildQuery(AllMasterPKs(DS))));
                }
                else if (BizType == 2 & Process == 2)
                {
                    DS.Tables.Add(objWF.GetDataTable(InvAgentSeaImpChildQuery(AllMasterPKs(DS))));
                }
                else if (BizType == 1 & Process == 1)
                {
                    DS.Tables.Add(objWF.GetDataTable(InvAgentAirExpChildQuery(AllMasterPKs(DS))));
                }
                else if (BizType == 1 & Process == 2)
                {
                    DS.Tables.Add(objWF.GetDataTable(InvAgentAirImpChildQuery(AllMasterPKs(DS))));
                }
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
        private string InvAgentSeaExpHDRQry(string HblRefNo = "", string MblRefNo = "", string Vessel = "", int Polpk = 0, int Podpk = 0)
        {

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK PK,");
            sb.Append("       INV.INVOICE_REF_NO INVOICE_REF_NO,");
            sb.Append("       CMT.AGENT_NAME CUSTOMER_NAME,");
            sb.Append("       INV.INVOICE_DATE INVOICE_DATE,");
            sb.Append("       INV.NET_INV_AMT NET_RECEIVABLE,");
            sb.Append("       NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("             FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("            WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
            sb.Append("              AND CTRN.FROM_INV_OR_CONSOL_INV = 3),");
            sb.Append("           0) RECIEVED,");
            sb.Append("       NVL((INV.NET_INV_AMT -");
            sb.Append("           NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("                  FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("                 WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
            sb.Append("                   AND CTRN.FROM_INV_OR_CONSOL_INV = 3),");
            sb.Append("                0.00)),");
            sb.Append("           0) BALANCE,");
            sb.Append("       CUMT.CURRENCY_ID,");
            sb.Append("       INV.INV_UNIQUE_REF_NR,");
            sb.Append("       CASE");
            sb.Append("         WHEN INV.CHK_INVOICE = 0 THEN");
            sb.Append("          'Pending'");
            sb.Append("         ELSE");
            sb.Append("          'Approved'");
            sb.Append("       END CHK_INVOICE,");
            sb.Append("       BKS.CARGO_TYPE CARGO_TYPE,");
            sb.Append("       NVL(DECODE(INV.EDI_STATUS, '0', 'Not Generated', '1', 'Trasmitted'),'Not Generated') EDI_STATUS,");
            sb.Append("       '' SEL,");
            sb.Append("       INV.AGENT_MST_FK AGENT_CUST_FK,");
            sb.Append("       DECODE(INV.CB_DP_LOAD_AGENT,1,'CB',2,'DP',3,'LA') CB_OR_DP ");
            sb.Append("  FROM INV_AGENT_TBL INV,");
            sb.Append("       JOB_CARD_TRN  JOB,");
            sb.Append("       BOOKING_MST_TBL       BKS,");
            sb.Append("       HBL_EXP_TBL           HBL,");
            sb.Append("       MBL_EXP_TBL           MBL,");
            sb.Append("       AGENT_MST_TBL         CMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
            sb.Append("       USER_MST_TBL          UMT,");
            sb.Append("       PORT_MST_TBL          PORT_POL,");
            sb.Append("       PORT_MST_TBL          PORT_POD");
            sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");

            sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND JOB.JOB_CARD_TRN_PK = HBL.JOB_CARD_SEA_EXP_FK(+)");
            sb.Append("   AND JOB.BOOKING_MST_FK = BKS.BOOKING_MST_PK(+)");
            sb.Append("   AND BKS.PORT_MST_POL_FK = PORT_POL.PORT_MST_PK");
            sb.Append("   AND BKS.PORT_MST_POD_FK = PORT_POD.PORT_MST_PK");
            sb.Append("   AND JOB.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND INV.AGENT_MST_FK = CMT.AGENT_MST_PK(+)");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+) ");
            if (!string.IsNullOrEmpty(HblRefNo))
            {
                sb.Append(" AND hbl.hbl_ref_no='" + HblRefNo + "'");
            }
            if (!string.IsNullOrEmpty(MblRefNo))
            {
                sb.Append(" AND MBL.MBL_EXP_TBL_PK = " + MblRefNo );
            }
            if (Polpk > 0)
            {
                sb.Append(" AND BKS.PORT_MST_POL_FK = " + Polpk );
            }
            if (Podpk > 0)
            {
                sb.Append(" AND BKS.PORT_MST_POD_FK = " + Podpk );
            }
            try
            {
                Vessel = Vessel.Trim();
                if (!string.IsNullOrEmpty(Vessel) & Vessel != "0")
                {
                    sb.Append(" AND JOB.VOYAGE_TRN_FK = " + Vessel );
                }
            }
            catch (Exception ex)
            {
            }
            return sb.ToString();
        }
        private string InvAgentSeaImpHDRQry(string HblRefNo = "", string MblRefNo = "", string Vessel = "", int Polpk = 0, int Podpk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK PK,");
            sb.Append("       INV.INVOICE_REF_NO INVOICE_REF_NO,");
            sb.Append("       CMT.AGENT_NAME CUSTOMER_NAME,");
            sb.Append("       INV.INVOICE_DATE INVOICE_DATE,");
            sb.Append("       INV.NET_INV_AMT NET_RECEIVABLE,");
            sb.Append("       NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("             FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("            WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
            sb.Append("              AND CTRN.FROM_INV_OR_CONSOL_INV = 3),");
            sb.Append("           0) RECIEVED,");
            sb.Append("       NVL((INV.NET_INV_AMT -");
            sb.Append("           NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("                  FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("                 WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
            sb.Append("                   AND CTRN.FROM_INV_OR_CONSOL_INV = 3),");
            sb.Append("                0.00)),");
            sb.Append("           0) BALANCE,");
            sb.Append("       CUMT.CURRENCY_ID,");
            sb.Append("       INV.INV_UNIQUE_REF_NR,");
            sb.Append("       CASE");
            sb.Append("         WHEN INV.CHK_INVOICE = 0 THEN");
            sb.Append("          'Pending'");
            sb.Append("         ELSE");
            sb.Append("          'Approved'");
            sb.Append("       END CHK_INVOICE,");
            sb.Append("       JOB.Cargo_Type CARGO_TYPE,");
            sb.Append("       NVL(DECODE(INV.EDI_STATUS, '0', 'Not Generated', '1', 'Trasmitted'),'Not Generated') EDI_STATUS,");
            sb.Append("       '' SEL,");
            sb.Append("       INV.AGENT_MST_FK AGENT_CUST_FK,");
            sb.Append("       DECODE(INV.CB_DP_LOAD_AGENT,1,'CB',2,'DP',3,'LA') CB_OR_DP ");
            sb.Append("  FROM INV_AGENT_TBL INV,");
            sb.Append("       JOB_CARD_TRN  JOB,");
            sb.Append("       AGENT_MST_TBL         CMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
            sb.Append("       USER_MST_TBL          UMT,");
            sb.Append("       VESSEL_VOYAGE_TRN      VTRN, ");
            sb.Append("       PORT_MST_TBL          PORT_POL, ");
            sb.Append("       PORT_MST_TBL          PORT_POD ");
            sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");

            sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND INV.AGENT_MST_FK = CMT.AGENT_MST_PK(+)");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+) ");
            sb.Append("   AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
            sb.Append("   AND JOB.PORT_MST_POL_FK = PORT_POL.PORT_MST_PK ");
            sb.Append("   AND JOB.PORT_MST_POD_FK = PORT_POD.PORT_MST_PK ");
            Vessel = Vessel.Trim();
            if (!string.IsNullOrEmpty(Vessel) & Vessel != "0")
            {
                sb.Append(" AND VTRN.VOYAGE LIKE '%" + Vessel.Trim() + "%'");
            }
            if (Polpk > 0)
            {
                sb.Append(" AND JOB.PORT_MST_POL_FK = " + Polpk );
            }
            if (Podpk > 0)
            {
                sb.Append(" AND JOB.PORT_MST_POD_FK = " + Podpk );
            }
            return sb.ToString();
        }
        private string InvAgentAirExpHDRQry(string HAWBRefNo = "", string MAWBRefNo = "", string Vessel = "", int Polpk = 0, int Podpk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK PK,");
            sb.Append("       INV.INVOICE_REF_NO INVOICE_REF_NO,");
            sb.Append("       CMT.AGENT_NAME CUSTOMER_NAME,");
            sb.Append("       INV.INVOICE_DATE INVOICE_DATE,");
            sb.Append("       INV.NET_INV_AMT NET_RECEIVABLE,");
            sb.Append("       NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("             FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("            WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
            sb.Append("              AND CTRN.FROM_INV_OR_CONSOL_INV = 3),");
            sb.Append("           0) RECIEVED,");
            sb.Append("       NVL((INV.NET_INV_AMT -");
            sb.Append("           NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("                  FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("                 WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
            sb.Append("                   AND CTRN.FROM_INV_OR_CONSOL_INV = 3),");
            sb.Append("                0.00)),");
            sb.Append("           0) BALANCE,");
            sb.Append("       CUMT.CURRENCY_ID,");
            sb.Append("       INV.INV_UNIQUE_REF_NR,");
            sb.Append("       CASE");
            sb.Append("         WHEN INV.CHK_INVOICE = 0 THEN");
            sb.Append("          'Pending'");
            sb.Append("         ELSE");
            sb.Append("          'Approved'");
            sb.Append("       END CHK_INVOICE,");
            sb.Append("       0 CARGO_TYPE,");
            sb.Append("       NVL(DECODE(INV.EDI_STATUS, '0', 'Not Generated', '1', 'Trasmitted'),'Not Generated') EDI_STATUS,");
            sb.Append("       '' SEL,");
            sb.Append("       INV.AGENT_MST_FK AGENT_CUST_FK,");
            sb.Append("       DECODE(INV.CB_DP_LOAD_AGENT,1,'CB',2,'DP',3,'LA') CB_OR_DP ");
            sb.Append("  FROM INV_AGENT_TBL INV,");
            sb.Append("       JOB_CARD_TRN  JOB,");
            sb.Append("       BOOKING_MST_TBL       BKS,");
            sb.Append("       HAWB_EXP_TBL          HBL,");
            sb.Append("       MAWB_EXP_TBL          MBL,");
            sb.Append("       AGENT_MST_TBL         CMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
            sb.Append("       USER_MST_TBL          UMT, ");
            sb.Append("       PORT_MST_TBL          PORT_POL, ");
            sb.Append("       PORT_MST_TBL          PORT_POD ");
            sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
            sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND JOB.JOB_CARD_TRN_PK = HBL.JOB_CARD_AIR_EXP_FK(+)");
            sb.Append("   AND JOB.BOOKING_MST_FK = BKS.BOOKING_MST_PK(+)");
            sb.Append("   AND BKS.PORT_MST_POL_FK = PORT_POL.PORT_MST_PK");
            sb.Append("   AND BKS.PORT_MST_POD_FK = PORT_POD.PORT_MST_PK");
            sb.Append("   AND JOB.MBL_MAWB_FK = MBL.MAWB_EXP_TBL_PK(+)");
            sb.Append("   AND INV.AGENT_MST_FK = CMT.AGENT_MST_PK(+)");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK(+)");
            sb.Append("   ");
            if (!string.IsNullOrEmpty(HAWBRefNo))
            {
                sb.Append(" AND HBL.HBL_REF_NO LIKE '%" + HAWBRefNo + "%' ");
            }
            if (!string.IsNullOrEmpty(MAWBRefNo))
            {
                sb.Append(" AND MBL.MAWB_REF_NO LIKE '%" + MAWBRefNo + "%' ");
            }
            if (Polpk > 0)
            {
                sb.Append(" AND BKS.PORT_MST_POL_FK = " + Polpk );
            }
            if (Podpk > 0)
            {
                sb.Append(" AND BKS.PORT_MST_POD_FK = " + Podpk );
            }
            Vessel = Vessel.Trim();
            if (!string.IsNullOrEmpty(Vessel) & Vessel != "0")
            {
                sb.Append(" AND JOB.VOYAGE_FLIGHT_NO  LIKE '%" + Vessel.Trim() + "%' ");
            }
            return sb.ToString();
        }
        private string InvAgentAirImpHDRQry(string HAWBRefNo = "", string MAWBRefNo = "", string Vessel = "", int Polpk = 0, int Podpk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK PK,");
            sb.Append("       INV.INVOICE_REF_NO INVOICE_REF_NO,");
            sb.Append("       CMT.AGENT_NAME CUSTOMER_NAME,");
            sb.Append("       INV.INVOICE_DATE INVOICE_DATE,");
            sb.Append("       INV.NET_INV_AMT NET_RECEIVABLE,");
            sb.Append("       NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("             FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("            WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
            sb.Append("              AND CTRN.FROM_INV_OR_CONSOL_INV = 3),");
            sb.Append("           0) RECIEVED,");
            sb.Append("       NVL((INV.NET_INV_AMT -");
            sb.Append("           NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("                  FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("                 WHERE CTRN.INVOICE_REF_NR LIKE INV.INVOICE_REF_NO");
            sb.Append("                   AND CTRN.FROM_INV_OR_CONSOL_INV = 3),");
            sb.Append("                0.00)),");
            sb.Append("           0) BALANCE,");
            sb.Append("       CUMT.CURRENCY_ID,");
            sb.Append("       INV.INV_UNIQUE_REF_NR,");
            sb.Append("       CASE");
            sb.Append("         WHEN INV.CHK_INVOICE = 0 THEN");
            sb.Append("          'Pending'");
            sb.Append("         ELSE");
            sb.Append("          'Approved'");
            sb.Append("       END CHK_INVOICE,");
            sb.Append("       0 CARGO_TYPE,");
            sb.Append("       NVL(DECODE(INV.EDI_STATUS, '0', 'Not Generated', '1', 'Trasmitted'),'Not Generated') EDI_STATUS,");
            sb.Append("       '' SEL,");
            sb.Append("       INV.AGENT_MST_FK AGENT_CUST_FK,");
            sb.Append("       DECODE(INV.CB_DP_LOAD_AGENT,1,'CB',2,'DP',3,'DP') CB_OR_DP ");
            sb.Append("  FROM INV_AGENT_TBL INV,");
            sb.Append("       JOB_CARD_TRN  JOB,");
            sb.Append("       AGENT_MST_TBL         CMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
            sb.Append("       USER_MST_TBL          UMT, ");
            sb.Append("       PORT_MST_TBL          PORT_POL, ");
            sb.Append("       PORT_MST_TBL          PORT_POD ");
            sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");

            sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND INV.CB_AGENT_MST_FK = CMT.AGENT_MST_PK(+)");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+) ");
            sb.Append("   AND JOB.PORT_MST_POL_FK = PORT_POL.PORT_MST_PK ");
            sb.Append("   AND JOB.PORT_MST_POD_FK = PORT_POD.PORT_MST_PK ");
            if (!string.IsNullOrEmpty(HAWBRefNo))
            {
                sb.Append("   AND JOB.HBL_HAWB_REF_NO LIKE '%" + HAWBRefNo + "%'");
            }
            if (!string.IsNullOrEmpty(MAWBRefNo))
            {
                sb.Append("   AND JOB.MBL_MAWB_REF_NO LIKE '%" + MAWBRefNo + "%'");
            }
            Vessel = Vessel.Trim();
            if (!string.IsNullOrEmpty(Vessel) & Vessel != "0")
            {
                sb.Append(" AND JOB.VOYAGE_FLIGHT_NO  LIKE '%" + Vessel.Trim() + "%' ");
            }
            if (Polpk > 0)
            {
                sb.Append(" AND JOB.PORT_MST_POL_FK = " + Polpk );
            }
            if (Podpk > 0)
            {
                sb.Append(" AND JOB.PORT_MST_POL_FK = " + Podpk );
            }
            return sb.ToString();
        }
        #endregion

        #region "Child"
        private string InvAgentSeaExpChildQuery(string InvPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK PK,");
            sb.Append("       JOB.JOBCARD_REF_NO,");
            sb.Append("       HBL.HBL_REF_NO,");
            sb.Append("       (CASE");
            sb.Append("         WHEN (NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO, '') = '/') THEN");
            sb.Append("          ''");
            sb.Append("         ELSE");
            sb.Append("          NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO, '')");
            sb.Append("       END) AS VESVOYAGE,");
            sb.Append("       SUM(INVTRN.AMT_IN_INV_CURR) INVAMT,");
            sb.Append("       CUMT.CURRENCY_ID, ");
            sb.Append("       JOB.JOB_CARD_TRN_PK JOBCARD_PK,");
            sb.Append("       JOB.BOOKING_MST_FK BOOKING_FK,");
            sb.Append("       JOB.JOB_CARD_STATUS,");
            sb.Append("       JOB.HBL_HAWB_FK HBL_FK,");
            sb.Append("       JOB.MBL_MAWB_FK MBL_FK,");
            sb.Append("       (SELECT M.MBL_REF_NO FROM MBL_EXP_TBL M WHERE M.MBL_EXP_TBL_PK=");
            sb.Append("        JOB.MBL_MAWB_FK) MBL_REF_NO, ");
            sb.Append("       (SELECT H.HBL_STATUS FROM HBL_EXP_TBL H WHERE H.HBL_EXP_TBL_PK=");
            sb.Append("        JOB.HBL_HAWB_FK) HBL_STATUS ");
            sb.Append("  FROM INV_AGENT_TBL     INV,");
            sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
            sb.Append("       JOB_CARD_TRN      JOB,");
            sb.Append("       HBL_EXP_TBL               HBL,");
            sb.Append("       AGENT_MST_TBL             AMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL     CUMT,");
            sb.Append("       USER_MST_TBL              UMT,");
            sb.Append("       VESSEL_VOYAGE_TRN         VTRN");
            sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
            sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
            sb.Append("   AND INV.AGENT_MST_FK = AMT.AGENT_MST_PK(+)");
            sb.Append("   AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK(+)");
            if (!string.IsNullOrEmpty(InvPks))
            {
                sb.Append("   AND INV.INV_AGENT_PK IN (" + InvPks + ")");
            }
            sb.Append(" GROUP BY INV.INV_AGENT_PK,");
            sb.Append("          JOB.JOBCARD_REF_NO,");
            sb.Append("          HBL.HBL_REF_NO,");
            sb.Append("          JOB.VESSEL_NAME,");
            sb.Append("          JOB.VOYAGE_FLIGHT_NO,");
            sb.Append("          CUMT.CURRENCY_ID,");
            sb.Append("       JOB.JOB_CARD_TRN_PK,");
            sb.Append("       JOB.BOOKING_MST_FK,");
            sb.Append("       JOB.JOB_CARD_STATUS,");
            sb.Append("       JOB.HBL_HAWB_FK,");
            sb.Append("       JOB.MBL_MAWB_FK ");
            return sb.ToString();
        }
        private string InvAgentSeaImpChildQuery(string InvPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK PK,");
            sb.Append("       JOB.JOBCARD_REF_NO,");
            sb.Append("       JOB.HBL_HAWB_REF_NO,");
            sb.Append("       (CASE");
            sb.Append("         WHEN (NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO, '') = '/') THEN");
            sb.Append("          ''");
            sb.Append("         ELSE");
            sb.Append("          NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO, '')");
            sb.Append("       END) AS VESVOYAGE,");
            sb.Append("       SUM(INVTRN.AMT_IN_INV_CURR) INVAMT,");
            sb.Append("       CUMT.CURRENCY_ID,");
            sb.Append("       JOB.JOB_CARD_TRN_PK JOBCARD_PK,");
            sb.Append("       NULL BOOKING_FK,");
            sb.Append("       JOB.JOB_CARD_STATUS,");
            sb.Append("       NULL HBL_FK,");
            sb.Append("       JOB.MBL_MAWB_FK MBL_FK,");
            sb.Append("       (SELECT M.MBL_REF_NO FROM MBL_IMP_TBL M WHERE M.MBL_IMP_TBL_PK=");
            sb.Append("        JOB.MBL_MAWB_FK) MBL_REF_NO, ");
            sb.Append("       (SELECT H.HBL_STATUS FROM HBL_EXP_TBL H WHERE UPPER(H.HBL_REF_NO) =");
            sb.Append("        UPPER(JOB.HBL_HAWB_REF_NO)) HBL_STATUS ");
            sb.Append("  FROM INV_AGENT_TBL     INV,");
            sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
            sb.Append("       JOB_CARD_TRN      JOB,");
            sb.Append("       AGENT_MST_TBL             AMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL     CUMT,");
            sb.Append("       USER_MST_TBL              UMT");
            sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
            sb.Append("   AND INV.AGENT_MST_FK = AMT.AGENT_MST_PK(+)");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK(+)");
            if (!string.IsNullOrEmpty(InvPks))
            {
                sb.Append("   AND INV.INV_AGENT_PK IN (" + InvPks + ")");
            }
            sb.Append(" GROUP BY INV.INV_AGENT_PK,");
            sb.Append("          JOB.JOBCARD_REF_NO,");
            sb.Append("          JOB.HBL_HAWB_REF_NO,");
            sb.Append("          JOB.VOYAGE_FLIGHT_NO,");
            sb.Append("          JOB.VESSEL_NAME,");
            sb.Append("          CUMT.CURRENCY_ID,");
            sb.Append("       JOB.JOB_CARD_TRN_PK,");
            sb.Append("       JOB.JOB_CARD_STATUS,");
            sb.Append("       JOB.MBL_MAWB_FK ");
            return sb.ToString();
        }
        private string InvAgentAirExpChildQuery(string InvPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK PK,");
            sb.Append("       JOB.JOBCARD_REF_NO,");
            sb.Append("       HBL.HAWB_REF_NO,");
            sb.Append("       (CASE");
            sb.Append("         WHEN (NVL(AIRLINE.AIRLINE_NAME, '') || '/' ||");
            sb.Append("              NVL(JOB.VOYAGE_FLIGHT_NO, '') = '/') THEN");
            sb.Append("          ''");
            sb.Append("         ELSE");
            sb.Append("          NVL(AIRLINE.AIRLINE_NAME, '') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO, '')");
            sb.Append("       END) AS VESVOYAGE,");
            sb.Append("       SUM(INVTRN.AMT_IN_INV_CURR) INVAMT,");
            sb.Append("       CUMT.CURRENCY_ID,");
            sb.Append("       JOB.JOB_CARD_TRN_PK JOBCARD_PK,");
            sb.Append("       JOB.BOOKING_MST_FK BOOKING_FK,");
            sb.Append("       JOB.JOB_CARD_STATUS,");
            sb.Append("       JOB.HBL_HAWB_FK HBL_FK,");
            sb.Append("       JOB.MBL_MAWB_FK MBL_FK, ");
            sb.Append("       (SELECT M.MBL_MAWB_REF_NO FROM MAWB_EXP_TBL M WHERE M.MAWB_EXP_TBL_PK= ");
            sb.Append("        JOB.MBL_MAWB_FK) MBL_REF_NO, ");
            sb.Append("       (SELECT H.HAWB_STATUS FROM HAWB_EXP_TBL H WHERE H.HAWB_EXP_TBL_PK=");
            sb.Append("        JOB.HBL_HAWB_FK) HBL_STATUS ");
            sb.Append("  FROM INV_AGENT_TBL     INV,");
            sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
            sb.Append("       JOB_CARD_TRN      JOB,");
            sb.Append("       BOOKING_MST_TBL           BAT,");
            sb.Append("       HAWB_EXP_TBL              HBL,");
            sb.Append("       AGENT_MST_TBL             AMT,");
            sb.Append("       AIRLINE_MST_TBL           AIRLINE,");
            sb.Append("       CURRENCY_TYPE_MST_TBL     CUMT,");
            sb.Append("       USER_MST_TBL              UMT");
            sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
            sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK(+)");
            sb.Append("   AND INV.AGENT_MST_FK = AMT.AGENT_MST_PK(+)");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            sb.Append("   AND JOB.BOOKING_MST_FK = BAT.BOOKING_MST_PK");
            sb.Append("   AND BAT.CARRIER_MST_FK = AIRLINE.AIRLINE_MST_PK(+)");
            sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK(+)");
            if (!string.IsNullOrEmpty(InvPks))
            {
                sb.Append("   AND INV.INV_AGENT_PK IN (" + InvPks + ")");
            }
            sb.Append(" GROUP BY INV.INV_AGENT_PK,");
            sb.Append("          JOB.JOBCARD_REF_NO,");
            sb.Append("          HBL.HAWB_REF_NO,");
            sb.Append("          AIRLINE.AIRLINE_NAME,");
            sb.Append("          JOB.VOYAGE_FLIGHT_NO,");
            sb.Append("          CUMT.CURRENCY_ID,");
            sb.Append("       JOB.JOB_CARD_TRN_PK,");
            sb.Append("       JOB.BOOKING_MST_FK,");
            sb.Append("       JOB.JOB_CARD_STATUS,");
            sb.Append("       JOB.HBL_HAWB_FK,");
            sb.Append("       JOB.MBL_MAWB_FK ");
            return sb.ToString();
        }
        private string InvAgentAirImpChildQuery(string InvPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK PK,");
            sb.Append("       JOB.JOBCARD_REF_NO,");
            sb.Append("       JOB.HBL_HAWB_REF_NO,");
            sb.Append("       (CASE");
            sb.Append("         WHEN (NVL(AIRLINE.AIRLINE_NAME, '') || '/' ||");
            sb.Append("              NVL(JOB.VOYAGE_FLIGHT_NO, '') = '/') THEN");
            sb.Append("          ''");
            sb.Append("         ELSE");
            sb.Append("          NVL(AIRLINE.AIRLINE_NAME, '') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO, '')");
            sb.Append("       END) AS VESVOYAGE,");
            sb.Append("       SUM(INVTRN.AMT_IN_INV_CURR) INVAMT,");
            sb.Append("       CUMT.CURRENCY_ID,");
            sb.Append("       JOB.JOB_CARD_TRN_PK JOBCARD_PK,");
            sb.Append("       NULL BOOKING_FK,");
            sb.Append("       JOB.JOB_CARD_STATUS,");
            sb.Append("       NULL HBL_FK,");
            sb.Append("       JOB.MBL_MAWB_FK MBL_FK,");
            sb.Append("       (SELECT M.MBL_REF_NO FROM MBL_IMP_TBL M WHERE M.MBL_IMP_TBL_PK=");
            sb.Append("        JOB.MBL_MAWB_FK) MBL_REF_NO, ");
            sb.Append("       (SELECT H.HAWB_STATUS FROM HAWB_EXP_TBL H WHERE UPPER(H.HAWB_REF_NO) =");
            sb.Append("        UPPER(JOB.HBL_HAWB_REF_NO)) HBL_STATUS ");
            sb.Append("  FROM INV_AGENT_TBL     INV,");
            sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
            sb.Append("       JOB_CARD_TRN      JOB,");
            sb.Append("       AGENT_MST_TBL             AMT,");
            sb.Append("       AIRLINE_MST_TBL           AIRLINE,");
            sb.Append("       CURRENCY_TYPE_MST_TBL     CUMT,");
            sb.Append("       USER_MST_TBL              UMT");
            sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
            sb.Append("   AND INV.AGENT_MST_FK = AMT.AGENT_MST_PK(+)");
            sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            sb.Append("   AND JOB.CARRIER_MST_FK = AIRLINE.AIRLINE_MST_PK(+)");
            sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK(+)");
            if (!string.IsNullOrEmpty(InvPks))
            {
                sb.Append("   AND INV.INV_AGENT_PK IN (" + InvPks + ")");
            }
            sb.Append(" GROUP BY INV.INV_AGENT_PK,");
            sb.Append("          JOB.JOBCARD_REF_NO,");
            sb.Append("          JOB.HBL_HAWB_REF_NO,");
            sb.Append("          AIRLINE.AIRLINE_NAME,");
            sb.Append("          JOB.VOYAGE_FLIGHT_NO,");
            sb.Append("          CUMT.CURRENCY_ID,");
            sb.Append("          JOB.JOB_CARD_TRN_PK,");
            sb.Append("          JOB.JOB_CARD_STATUS,");
            sb.Append("          JOB.MBL_MAWB_FK ");
            return sb.ToString();
        }
        #endregion
        #endregion

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

        public string Fetchcustpk(string custid)
        {
            string strSQL = null;
            strSQL = "select c.customer_mst_pk from customer_mst_tbl c where c.customer_name = '" + custid + "'";
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string FetchEXpk(string PK)
        {
            string strSQL = null;
            strSQL = "select T.EXCH_RATE_TYPE_FK from consol_invoice_tbl t  where t.consol_invoice_pk = " + PK;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        #region "Take User Biz Type"
        public string FetchBizType(int UsrPk)
        {
            string strSQL = null;
            strSQL = "select usr.business_type from user_mst_tbl usr, role_mst_tbl r where r.role_mst_tbl_pk = usr.role_mst_fk and usr.user_mst_pk = " + UsrPk;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        #region "Export_To_QFOR"

        public void Export_To_QFOR(string INVOICE_REF_NR, string strRetVal)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand cmd = new OracleCommand();

            objWK.OpenConnection();


            try
            {
                var _with1 = cmd;

                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".DB_INTEGRATION.EXP_INVOICES";
                var _with2 = _with1.Parameters;
                _with2.Add("INVOICE_REF_NR", INVOICE_REF_NR).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;


                cmd.ExecuteNonQuery();
                strRetVal = (string.IsNullOrEmpty(cmd.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : cmd.Parameters["RETURN_VALUE"].Value.ToString());

                objWK.CloseConnection();
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

        #region "UpdateInvEDIStatus"
        public bool UpdateInvEDIStatus(string InvoicePkS, short BizType, short ProcessType, short CUST_OR_AGENT = 0)
        {
            WorkFlow objwf = new WorkFlow();
            string UpdateQuery = "";
            if (CUST_OR_AGENT == 0)
            {
                UpdateQuery = "UPDATE CONSOL_INVOICE_TBL INV SET INV.EDI_STATUS=1 ";
                UpdateQuery += " WHERE INV.CONSOL_INVOICE_PK IN (" + InvoicePkS + ")";
            }
            else
            {
                UpdateQuery = "UPDATE INV_AGENT_TBL INV SET INV.EDI_STATUS=1 ";
                UpdateQuery += " WHERE INV.INV_AGENT_PK IN (" + InvoicePkS + ")";

                if (BizType == 1)
                {
                    UpdateQuery = UpdateQuery.Replace("SEA", "AIR");
                }
                if (ProcessType == 2)
                {
                    UpdateQuery = UpdateQuery.Replace("EXP", "IMP");
                }
            }

            try
            {
                objwf.ExecuteCommands(UpdateQuery);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
        #endregion

        #region "UpdateInvStatus"
        public string UpdateInvStatus(string InvoicePkS, short BizType, short ProcessType, string remarks, short CUST_OR_AGENT = 0)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand updCmd = new OracleCommand();
            DataSet dsPia = null;
            string str = null;
            string strIns = null;
            string Ret = null;
            Int16 intDel = default(Int16);
            Int16 intIns = default(Int16);
            try
            {
                objWF.OpenConnection();
                var _with3 = updCmd;
                _with3.Connection = objWF.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWF.MyUserName + ".INVOICE_CANCELLATION_PKG.CANCEL_INVOICE";
                var _with4 = _with3.Parameters;
                updCmd.Parameters.Add("INVOICE_PK_IN", InvoicePkS).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                if (CUST_OR_AGENT == 0)
                {
                    updCmd.Parameters.Add("INV_TYPE_FLAG_IN", "CONSOL_INV").Direction = ParameterDirection.Input;
                }
                else
                {
                    if (BizType == 1 & ProcessType == 2)
                    {
                        updCmd.Parameters.Add("INV_TYPE_FLAG_IN", "INV_AGENT_AIR_EXP").Direction = ParameterDirection.Input;
                    }
                    else if (BizType == 2 & ProcessType == 2)
                    {
                        updCmd.Parameters.Add("INV_TYPE_FLAG_IN", "INV_AGENT_SEA_EXP").Direction = ParameterDirection.Input;
                    }
                    else if (BizType == 1 & ProcessType == 1)
                    {
                        updCmd.Parameters.Add("INV_TYPE_FLAG_IN", "INV_AGENT_AIR_IMP").Direction = ParameterDirection.Input;
                    }
                    else if (BizType == 2 & ProcessType == 2)
                    {
                        updCmd.Parameters.Add("INV_TYPE_FLAG_IN", "INV_AGENT_SEA_IMP").Direction = ParameterDirection.Input;
                    }
                }
                updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                intIns = Convert.ToInt16(_with3.ExecuteNonQuery());
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
        #endregion

        #region "UpdateBkgEDIStatus"
        public bool UpdateBkgEDIStatus(string InvoicePkS, short BizType, short CUST_OR_AGENT = 0)
        {
            WorkFlow objwf = new WorkFlow();
            string UpdateQuery = "";
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (CUST_OR_AGENT == 0)
            {
                sb.Append("SELECT DISTINCT BKG.BOOKING_MST_PK");
                sb.Append("  FROM BOOKING_MST_TBL        BKG,");
                sb.Append("       JOB_CARD_TRN   JOB,");
                sb.Append("       CONSOL_INVOICE_TBL     INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL TRN");
                sb.Append(" WHERE INV.CONSOL_INVOICE_PK = TRN.CONSOL_INVOICE_FK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = TRN.JOB_CARD_FK");
                sb.Append("   AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                sb.Append("   AND INV.CONSOL_INVOICE_PK IN (" + InvoicePkS + ")");
            }
            else
            {
                sb.Append("SELECT DISTINCT BKG.BOOKING_MST_PK");
                sb.Append("  FROM BOOKING_MST_TBL       BKG,");
                sb.Append("       JOB_CARD_TRN  JOB,");
                sb.Append("       INV_AGENT_TBL INV");
                sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("   AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                sb.Append("   AND INV.INV_AGENT_PK IN (" + InvoicePkS + ")");
            }
            UpdateQuery = sb.ToString();
            UpdateQuery = "UPDATE BOOKING_MST_TBL B SET B.EDI_STATUS=1 WHERE B.BOOKING_MST_PK IN (" + UpdateQuery + ")";
            if (BizType == 1)
            {
                UpdateQuery = UpdateQuery.Replace("SEA", "AIR");
            }
            try
            {
                objwf.ExecuteCommands(UpdateQuery);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
        #endregion


    }
}