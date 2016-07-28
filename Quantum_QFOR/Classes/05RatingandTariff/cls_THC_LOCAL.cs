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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_THC_LOCAL : CommonFeatures
    {
        #region "Enum.."

        private enum Header
        {
            SLNO = 0,
            PK = 1,
            OPTFLAG = 2,
            POL_ID = 3,
            POL_FK = 4,
            POL_NAME = 5,
            OPTFLAGVALUE = 6,
            COMMODITY_NAME = 7,
            COMMODITY_MST_PK = 8,
            RATE = 9
        }

        private enum HeaderEntry
        {
            SLNO = 0,
            OPTFLAG = 1,
            PK = 2,
            OPER_FK = 3,
            OPER_ID = 4,
            PORT_FK = 5,
            PORT_ID = 6,
            TER_PK = 7,
            TER_ID = 8,
            PROCESS_PK = 9,
            PROCESS_ID = 10,
            CNTRTYPE_PK = 11,
            CNTRTYPE_ID = 12,
            COMMGRP_PK = 13,
            COMMGRP_ID = 14,
            COMM_PK = 15,
            COMM_ID = 16,
            CHARGE_PK = 17,
            CHARGE_ID = 18,
            BASIS_PK = 19,
            BASIS_ID = 20,
            CURRENCY_PK = 21,
            CURRENCY_ID = 22,
            BL_RATE = 23,
            CONT1_PK = 24,
            CONT1_RATE = 25,
            CONT2_PK = 26,
            CONT2_RATE = 27,
            CONT3_PK = 28,
            CONT3_RATE = 29,
            CONT4_PK = 30,
            CONT4_RATE = 31,
            CONT5_PK = 32,
            CONT5_RATE = 33,
            CONT6_PK = 34,
            CONT6_RATE = 35,
            OTH_CONT = 36,
            VALID_FROM = 37,
            VALID_TO = 38,
            FRT_PRIORITY = 39,
            DELFLAG = 40,
            CHKFLAG = 41
        }

        protected enum COUNTRYPORT
        {
            COUNTRY_PK = 0,
            COUNTRY_ID = 1,
            COUNTRY_NAME = 2,
            PORT_PK = 3,
            PORT_ID = 4,
            PORT_NAME = 5
        }

        protected enum CHARGES
        {
            SLNO = 0,
            OPTFLAG = 1,
            PK = 2,
            CHARGE_PK = 3,
            CHARGE_ID = 4,
            CHARGE_DESC = 5,
            BIZ_PK = 6,
            BIZ_ID = 7,
            CARGO_TYPE = 8,
            PROCESS_PK = 9,
            PROCESS_ID = 10,
            COMMGRP_PK = 11,
            COMMGRP_ID = 12,
            COMM_PK = 13,
            COMM_ID = 14,
            BASIS_PK = 15,
            BASIS_ID = 16,
            CURRENCY_PK = 17,
            CURRENCY_ID = 18,
            CNTRTYPE_PK = 19,
            CNTRTYPE_ID = 20,
            MIN_AMT = 21,
            SHIPMENT_AMT = 22,
            BL_RATE = 23,
            CONT_20_AMT = 24,
            CONT_40_AMT = 25,
            OTH_CONT = 26,
            VALID_FROM = 27,
            VALID_TO = 28,
            FRT_PRIORITY = 29,
            COUNTRY_FK = 30,
            PORT_FK = 31,
            PORT_ID = 32,
            TER_PK = 33,
            TER_ID = 34,
            CREDIT = 35
        }

        #endregion "Enum.."

        #region "List of Properties"

        private Int64 M_Protocol_Mst_Pk;
        private string M_Protocol_NAME;
        private string M_Protocol_VALUE;
        private int M_LINE_MST_FK = 0;

        public Int64 Protocol_Mst_Pk
        {
            get { return M_Protocol_Mst_Pk; }
            set { M_Protocol_Mst_Pk = value; }
        }

        public string Protocol_Id
        {
            get { return M_Protocol_NAME; }
            set { M_Protocol_NAME = value; }
        }

        public string Protocol_Name
        {
            get { return M_Protocol_VALUE; }
            set { M_Protocol_VALUE = value; }
        }

        public Int64 Created_By_Fk
        {
            get { return M_CREATED_BY_FK; }
            set { M_CREATED_BY_FK = value; }
        }

        public DateTime Created_Dt
        {
            get { return M_CREATED_DT; }
            set { M_CREATED_DT = value; }
        }

        public Int64 Last_Modified_By_FK
        {
            get { return M_LAST_MODIFIED_BY_FK; }
            set { M_LAST_MODIFIED_BY_FK = value; }
        }

        public DateTime Last_Modified_Dt
        {
            get { return M_LAST_MODIFIED_DT; }
            set { M_LAST_MODIFIED_DT = value; }
        }

        #endregion "List of Properties"

        public object FetchCommodityGroup(bool IncludeAll = false)
        {
            WorkFlow objWF = new WorkFlow();
            string Sql = null;
            DataSet _DataSet = new DataSet();
            if (IncludeAll)
            {
                Sql += " SELECT COMMODITY_GROUP_PK,COMMODITY_GROUP_CODE,COMMODITY_GROUP_DESC,VERSION_NO";
                Sql += " FROM (select 0 COMMODITY_GROUP_PK,";
                Sql += " ' ' COMMODITY_GROUP_CODE, ";
                Sql += " ' ' COMMODITY_GROUP_DESC, ";
                Sql += " 0 VERSION_NO,0 PREFERENCE from dual UNION ";
            }
            else
            {
                Sql += " SELECT COMMODITY_GROUP_PK,COMMODITY_GROUP_CODE,COMMODITY_GROUP_DESC,VERSION_NO";
                Sql += " FROM (select 0 COMMODITY_GROUP_PK,";
                Sql += " ' ' COMMODITY_GROUP_CODE, ";
                Sql += " ' ' COMMODITY_GROUP_DESC, ";
                Sql += " 0 VERSION_NO,0 PREFERENCE from dual UNION ";
            }

            Sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            Sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO,CG.PREFERENCE ";
            Sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            Sql += " WHERE CG.ACTIVE_FLAG=1) ";
            Sql += " ORDER BY PREFERENCE ";
            try
            {
                return objWF.GetDataSet(Sql);
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

        #region "Local Charges New"

        #region "Fetch"

        public DataSet FetchCountryPortHeader(string CountryMstPks, string PortMstPks, string ChargeFK, int TerFK, int CommGrpFK, int CommFK, string strFromdate, string strToDate, int CurFK, int sessionLoged,
        DataSet dynCntnr = null, bool chkAct = false, bool isadminusr = false, Int16 Basis = 0, Int16 Process = 0, Int16 BizType = 0, Int16 CargoType = 0, Int16 CntrApp = 0, int ChkonLoad = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 FromGenerate = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sbMain = new System.Text.StringBuilder(5000);
            DataSet dsTHC = new DataSet();
            DataTable dtTHC = new DataTable();
            string CountryFks = "";
            string PortFks = "";
            System.DateTime strFdate = default(System.DateTime);
            System.DateTime strTdate = default(System.DateTime);
            try
            {
                //----------------------------------main query starts-------------------------------------
                sbMain.Append("                         SELECT DISTINCT TT.TARIFF_PK PK,");
                sbMain.Append("                                        to_char(0) OPTFLAG,");
                sbMain.Append("                                        FRT.BUSINESS_TYPE BIZ_PK,");
                sbMain.Append("                                        DECODE(FRT.BUSINESS_TYPE,");
                sbMain.Append("                                               1,");
                sbMain.Append("                                               'Air',");
                sbMain.Append("                                               2,");
                sbMain.Append("                                               'Sea',");
                sbMain.Append("                                               3,");
                sbMain.Append("                                               'Both') BIZ_ID,");
                sbMain.Append("                                        DECODE(TT.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE,");
                sbMain.Append("                                        COUNTRY.COUNTRY_MST_PK COUNTRY_FK,");
                sbMain.Append("                                        COUNTRY.COUNTRY_ID,");
                sbMain.Append("                                        COUNTRY.COUNTRY_NAME,");
                sbMain.Append("                                        PMT.PORT_MST_PK PORT_FK,");
                sbMain.Append("                                        PMT.PORT_ID,");
                sbMain.Append("                                        PMT.PORT_NAME,");
                sbMain.Append("                                        TT.POL_TERMINAL TER_PK,");
                sbMain.Append("                                        TMT.TERMINAL_ID TER_ID,");
                sbMain.Append("                                        TT.PROCESS_TYPE PROCESS_PK,");
                sbMain.Append("                                        DECODE(TT.PROCESS_TYPE,");
                sbMain.Append("                                               0,");
                sbMain.Append("                                               'BOTH',");
                sbMain.Append("                                               1,");
                sbMain.Append("                                               'EXPORT',");
                sbMain.Append("                                               2,");
                sbMain.Append("                                               'IMPORT',");
                sbMain.Append("                                               'BOTH') PROCESS_ID,");
                sbMain.Append("                                        TT.CNTR_APPLICABLE AS CNTRTYPE_PK,");
                sbMain.Append("                                        CASE");
                sbMain.Append("                                          WHEN TT.TARIFF_BASIS = 0 THEN");
                sbMain.Append("                                           DECODE(TT.CNTR_APPLICABLE,");
                sbMain.Append("                                                  0,");
                sbMain.Append("                                                  'BOTH',");
                sbMain.Append("                                                  1,");
                sbMain.Append("                                                  'COC',");
                sbMain.Append("                                                  2,");
                sbMain.Append("                                                  'SOC')");
                sbMain.Append("                                          ELSE");
                sbMain.Append("                                           ''");
                sbMain.Append("                                        END CNTRTYPE_ID,");
                sbMain.Append("                                        NVL(CGMT.COMMODITY_GROUP_PK,0) COMMGRP_PK,");
                sbMain.Append("                                        CGMT.COMMODITY_GROUP_CODE COMMGRP_ID,");
                sbMain.Append("                                        CMT.COMMODITY_MST_PK         COMM_PK,");
                sbMain.Append("                                        CMT.COMMODITY_NAME COMM_ID,");
                sbMain.Append("                                        FRT.FREIGHT_ELEMENT_MST_PK CHARGE_PK,");
                sbMain.Append("                                        FRT.FREIGHT_ELEMENT_ID CHARGE_ID,");
                sbMain.Append("                                        FRT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sbMain.Append("                                        TT.TARIFF_BASIS BASIS_PK,");
                sbMain.Append("                                        D.DDID BASIS_ID,");
                sbMain.Append("                                        CUR.CURRENCY_MST_PK CURRENCY_PK,");
                sbMain.Append("                                        CUR.CURRENCY_ID CURRENCY_ID,");
                sbMain.Append("                                        TT.RATE MIN_AMT,");
                sbMain.Append("                                        TT.SHIPMENT_AMT,");
                sbMain.Append("                                        CASE");
                sbMain.Append("                                          WHEN TT.TARIFF_BASIS = 1 THEN");
                sbMain.Append("                                           TT.RATE");
                sbMain.Append("                                          ELSE");
                sbMain.Append("                                           0");
                sbMain.Append("                                        END BL_RATE,");
                sbMain.Append("                                        TT.CONT_20_AMT,");
                sbMain.Append("                                        TT.CONT_40_AMT,");
                sbMain.Append("                                        '' OTH_CONT,");
                sbMain.Append("                                        TO_DATE(TT.EFFECTIVE_FROM, DATEFORMAT) VALID_FROM,");
                sbMain.Append("                                        TO_DATE(TT.EFFECTIVE_TO, DATEFORMAT) VALID_TO,");
                sbMain.Append("                                        FRT.PREFERENCE AS FRT_PRIORITY,");
                sbMain.Append("                                        NVL(FRT.CREDIT,0) CREDIT");
                sbMain.Append("                          FROM TARIFF_TRN              TT,");
                sbMain.Append("                               PORT_MST_TBL            PMT,");
                sbMain.Append("                               COUNTRY_MST_TBL         COUNTRY,");
                sbMain.Append("                               TERMINAL_MST_TBL        TMT,");
                sbMain.Append("                               FREIGHT_ELEMENT_MST_TBL FRT,");
                sbMain.Append("                               FREIGHT_CONFIG_TRN_TBL  FCT,");
                sbMain.Append("                               COMMODITY_MST_TBL       CMT,");
                sbMain.Append("                               COMMODITY_GROUP_MST_TBL CGMT,");
                sbMain.Append("                               CURRENCY_TYPE_MST_TBL   CUR,");
                sbMain.Append("                       (SELECT TO_NUMBER(DD.DD_VALUE) DDVALUE, DD.DD_ID DDID");
                sbMain.Append("                          FROM QFOR_DROP_DOWN_TBL DD");
                sbMain.Append("                         WHERE DD.DD_FLAG = 'BASIS'");
                sbMain.Append("                           AND DD.CONFIG_ID = 'QFOR4458') D");
                sbMain.Append("                         WHERE TT.POL_FK = PMT.PORT_MST_PK");
                sbMain.Append("                           AND FRT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
                sbMain.Append("                           AND PMT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK(+)");
                sbMain.Append("                           AND TT.FREIGHT_ELEMENT_MST_FK =");
                sbMain.Append("                               FRT.FREIGHT_ELEMENT_MST_PK");
                sbMain.Append("                           AND TT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                sbMain.Append("                           AND TT.COMMODITY_GROUP_MST_FK =");
                sbMain.Append("                               CGMT.COMMODITY_GROUP_PK(+)");
                sbMain.Append("                           AND TT.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)");
                sbMain.Append("                           AND TT.POD_FK = 0");
                sbMain.Append("                           AND FRT.CHARGE_BASIS = D.DDVALUE(+)");
                if (!string.IsNullOrEmpty(PortMstPks.Trim()) & PortMstPks.Trim() != "0")
                {
                    sbMain.Append("                           AND PMT.PORT_MST_PK IN (" + PortMstPks + ")");
                }
                if (!string.IsNullOrEmpty(CountryMstPks.Trim()) & CountryMstPks.Trim() != "0")
                {
                    sbMain.Append("                           AND COUNTRY.COUNTRY_MST_PK IN (" + CountryMstPks + ")");
                }
                sbMain.Append("                           AND TT.POL_TERMINAL = TMT.TERMINAL_MST_PK(+)");
                sbMain.Append("                           AND PMT.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(ChargeFK))
                {
                    sbMain.Append(" AND TT.FREIGHT_ELEMENT_MST_FK IN (" + ChargeFK + ")");
                }

                if (TerFK > 0)
                {
                    sbMain.Append(" AND TT.POL_TERMINAL=" + TerFK);
                }

                ///'''Genaerate Time It should check with Blank commodityGroup
                if (FromGenerate == 1)
                {
                    sbMain.Append(" AND TT.COMMODITY_GROUP_MST_FK=" + CommGrpFK);
                }
                else
                {
                    if ((CommGrpFK > 0))
                    {
                        sbMain.Append(" AND TT.COMMODITY_GROUP_MST_FK=" + CommGrpFK);
                    }
                }

                if (CurFK > 0)
                {
                    sbMain.Append(" AND TT.CURRENCY_MST_FK = " + CurFK);
                }

                if (Basis > 0)
                {
                    sbMain.Append(" AND TT.TARIFF_BASIS=" + Basis);
                }

                if (Process > 0)
                {
                    sbMain.Append(" AND TT.PROCESS_TYPE IN (0," + Process + ") ");
                }

                if (BizType == 1 | BizType == 2)
                {
                    sbMain.Append(" AND FRT.BUSINESS_TYPE IN (" + BizType + ",3,0)");
                }
                else
                {
                    sbMain.Append(" AND FRT.BUSINESS_TYPE IN (1,2,3,0) ");
                }

                if ((CargoType == 1 | CargoType == 2 | CargoType == 4) & (BizType == 2 | BizType == 3))
                {
                    sbMain.Append(" AND NVL(TT.CARGO_TYPE,0) IN (0," + CargoType + ") ");
                }

                if (CommFK > 0)
                {
                    sbMain.Append(" AND TT.COMMODITY_MST_FK=" + CommFK);
                }

                if (CntrApp > 0)
                {
                    sbMain.Append(" AND (TT.CNTR_APPLICABLE=0 OR TT.CNTR_APPLICABLE=" + CntrApp + ")");
                }
                if (!string.IsNullOrEmpty(strFromdate) & string.IsNullOrEmpty(strToDate))
                {
                    strFdate = Convert.ToDateTime(strFromdate);
                    sbMain.Append(" AND TO_DATE('" + System.String.Format("{0:dd-MMM-yyyy}", strFdate) + "',DATEFORMAT)");
                    sbMain.Append(" BETWEEN TO_DATE(TT.EFFECTIVE_FROM,DATEFORMAT) and TO_DATE(NVL(TT.EFFECTIVE_TO,SYSDATE),DATEFORMAT)");
                }
                if (string.IsNullOrEmpty(strFromdate) & !string.IsNullOrEmpty(strToDate))
                {
                    strTdate = Convert.ToDateTime(strToDate);
                    sbMain.Append(" AND TO_DATE('" + System.String.Format("{0:dd-MMM-yyyy}", strTdate) + "',DATEFORMAT)");
                    sbMain.Append(" BETWEEN TO_DATE(TT.EFFECTIVE_FROM,DATEFORMAT) AND TO_DATE(NVL(TT.EFFECTIVE_TO,SYSDATE),DATEFORMAT)");
                }
                if (!string.IsNullOrEmpty(strFromdate) & !string.IsNullOrEmpty(strToDate))
                {
                    strFdate = Convert.ToDateTime(strFromdate);
                    strTdate = Convert.ToDateTime(strToDate);
                    sbMain.Append(" AND (TO_DATE('" + System.String.Format("{0:dd-MMM-yyyy}", strFdate) + "',DATEFORMAT)");
                    sbMain.Append(" BETWEEN TO_DATE(TT.EFFECTIVE_FROM,DATEFORMAT) and TO_DATE(NVL(TT.EFFECTIVE_TO,SYSDATE),DATEFORMAT)");
                    sbMain.Append(" OR ");
                    sbMain.Append(" TO_DATE('" + System.String.Format("{0:dd-MMM-yyyy}", strTdate) + "',DATEFORMAT)");
                    sbMain.Append(" BETWEEN TO_DATE(TT.EFFECTIVE_FROM,DATEFORMAT) AND TO_DATE(NVL(TT.EFFECTIVE_TO,SYSDATE),DATEFORMAT) )");
                }

                if (chkAct == true)
                {
                    sbMain.Append("AND (nvl(TT.EFFECTIVE_TO,'01-Jan-2100') >='" + System.String.Format("{0:dd-MMM-yyyy}", (DateTime)DateTime.Today) + "') ");
                }
                sbMain.Append("     AND TT.CHARGE_TYPE=2 ");
                //----------------------------------main query ends-------------------------------------

                sb.Append("SELECT DISTINCT QRY.COUNTRY_FK COUNTRY_PK,");
                sb.Append("       QRY.COUNTRY_ID,");
                sb.Append("       QRY.COUNTRY_NAME,");
                sb.Append("       QRY.PORT_FK,");
                sb.Append("       QRY.PORT_ID,");
                sb.Append("       QRY.PORT_NAME FROM ");
                sb.Append("(" + sbMain.ToString() + ") QRY");

                Int32 last = default(Int32);
                Int32 start = default(Int32);
                Int32 TotalRecords = default(Int32);
                string Sql = null;
                Sql = "select count(*) from ( " + sb.ToString() + ")";
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(Sql));
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

                Sql = "select * from (" + sb.ToString() + ") WHERE ROWNUM  BETWEEN " + start + " AND " + last;
                objWF.MyConnection.Open();
                objWF.MyDataAdapter = new OracleDataAdapter(Sql, objWF.MyConnection);
                objWF.MyDataAdapter.Fill(dtTHC);
                dsTHC.Tables.Add(dtTHC);
                objWF.MyDataAdapter.Dispose();
                sb.Clear();
                CountryFks = "0";
                PortFks = "0";
                foreach (DataRow dr in dtTHC.Rows)
                {
                    if (string.IsNullOrEmpty(CountryFks))
                    {
                        CountryFks = dr["COUNTRY_PK"].ToString();
                    }
                    else
                    {
                        CountryFks += "," + dr["COUNTRY_PK"].ToString();
                    }
                    if (string.IsNullOrEmpty(PortFks))
                    {
                        PortFks = dr["PORT_FK"].ToString();
                    }
                    else
                    {
                        PortFks += "," + dr["PORT_FK"].ToString();
                    }
                }
                //-------------------------------header query ends------------------------------------------------------
                sb.Append("SELECT * ");
                sb.Append("  FROM (SELECT ROWNUM SLNO, Q.*");
                sb.Append("          FROM (SELECT QRY.OPTFLAG,");
                sb.Append("                       QRY.PK,");
                sb.Append("                       QRY.CHARGE_PK,");
                sb.Append("                       QRY.CHARGE_ID,");
                sb.Append("                       QRY.CHARGE_DESC,");
                sb.Append("                       QRY.BIZ_PK,");
                sb.Append("                       QRY.BIZ_ID,");
                sb.Append("                       QRY.CARGO_TYPE,");
                sb.Append("                       QRY.PROCESS_PK,");
                sb.Append("                       QRY.PROCESS_ID,");
                sb.Append("                       QRY.COMMGRP_PK,");
                sb.Append("                       QRY.COMMGRP_ID,");
                sb.Append("                       QRY.COMM_PK,");
                sb.Append("                       QRY.COMM_ID,");
                sb.Append("                       QRY.BASIS_PK,");
                sb.Append("                       QRY.BASIS_ID,");
                sb.Append("                       QRY.CURRENCY_PK,");
                sb.Append("                       QRY.CURRENCY_ID,");
                sb.Append("                       QRY.CNTRTYPE_PK,");
                sb.Append("                       QRY.CNTRTYPE_ID,");
                sb.Append("                       SUM(QRY.MIN_AMT) MIN_AMT,");
                sb.Append("                       SUM(QRY.SHIPMENT_AMT) SHIPMENT_AMT,");
                sb.Append("                       SUM(QRY.BL_RATE) BL_RATE,");
                sb.Append("                       SUM(QRY.CONT_20_AMT) AS CONT_20_AMT,");
                sb.Append("                       SUM(QRY.CONT_40_AMT) AS CONT_40_AMT,");
                sb.Append("                       QRY.OTH_CONT,");
                sb.Append("                       QRY.VALID_FROM,");
                sb.Append("                       QRY.VALID_TO,");
                sb.Append("                       QRY.FRT_PRIORITY,");
                sb.Append("                       QRY.COUNTRY_FK,");
                sb.Append("                       QRY.PORT_FK,");
                sb.Append("                       QRY.PORT_ID,");
                sb.Append("                       QRY.TER_PK,");
                sb.Append("                       QRY.TER_ID,");
                sb.Append("                       QRY.CREDIT");
                sb.Append("                  FROM ( ");
                //--------------------main query append-------------------------
                sb.Append("(" + sbMain.ToString() + ") ");
                //--------------------------------------------------------------
                sb.Append("                  ) QRY WHERE QRY.PORT_FK IN (" + PortFks + ") ");
                sb.Append("                 GROUP BY QRY.OPTFLAG,");
                sb.Append("                          QRY.PK,");
                sb.Append("                          QRY.CHARGE_PK,");
                sb.Append("                          QRY.CHARGE_ID,");
                sb.Append("                          QRY.CHARGE_DESC,");
                sb.Append("                          QRY.BIZ_PK,");
                sb.Append("                          QRY.BIZ_ID,");
                sb.Append("                          QRY.CARGO_TYPE,");
                sb.Append("                          QRY.PROCESS_PK,");
                sb.Append("                          QRY.PROCESS_ID,");
                sb.Append("                          QRY.COMMGRP_PK,");
                sb.Append("                          QRY.COMMGRP_ID,");
                sb.Append("                          QRY.COMM_PK,");
                sb.Append("                          QRY.COMM_ID,");
                sb.Append("                          QRY.BASIS_PK,");
                sb.Append("                          QRY.BASIS_ID,");
                sb.Append("                          QRY.CURRENCY_PK,");
                sb.Append("                          QRY.CURRENCY_ID,");
                sb.Append("                          QRY.CNTRTYPE_PK,");
                sb.Append("                          QRY.CNTRTYPE_ID,");
                sb.Append("                          QRY.OTH_CONT,");
                sb.Append("                          QRY.VALID_FROM,");
                sb.Append("                          QRY.VALID_TO,");
                sb.Append("                          QRY.FRT_PRIORITY,");
                sb.Append("                          QRY.COUNTRY_FK,");
                sb.Append("                          QRY.PORT_FK,");
                sb.Append("                          QRY.PORT_ID,");
                sb.Append("                          QRY.TER_PK,");
                sb.Append("                          QRY.TER_ID,");
                sb.Append("                          QRY.CREDIT");
                sb.Append("                 ORDER BY FRT_PRIORITY,TO_DATE(VALID_FROM, DATEFORMAT) DESC,");
                sb.Append("                          TO_DATE(VALID_TO, DATEFORMAT) DESC,PORT_ID) Q)");

                dtTHC = new DataTable();
                if (objWF.MyConnection.State != ConnectionState.Open)
                    objWF.MyConnection.Open();
                objWF.MyDataAdapter = new OracleDataAdapter(sb.ToString(), objWF.MyConnection);
                objWF.MyDataAdapter.Fill(dtTHC);
                dsTHC.Tables.Add(dtTHC);
                DataRelation rel = new DataRelation("rl_PORT_CHARGES", new DataColumn[] {
                    dsTHC.Tables[0].Columns["COUNTRY_PK"],
                    dsTHC.Tables[0].Columns["PORT_FK"]
                }, new DataColumn[] {
                    dsTHC.Tables[1].Columns["COUNTRY_FK"],
                    dsTHC.Tables[1].Columns["PORT_FK"]
                });
                dsTHC.Relations.Clear();
                dsTHC.Relations.Add(rel);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return dsTHC;
        }

        #endregion "Fetch"

        #region "Generate"

        public DataSet FetchRecordOnGenerateTHC(string FrtElementFK, int PortPK, int TerPK, int CommGrpPK, int CommPK, int CurrencyPK, int Basis, int Process, int CntrApp, string frmDate,
        string ToDate, int CargoType = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            DataSet dsTHC = new DataSet();
            DataSet dsTHCExisting = new DataSet();
            DataTable dtTHC = new DataTable();
            string exChargePks = "";

            try
            {
                //dsTHCExisting = FetchCountryPortHeader("", PortPK, FrtElementFK, 0, CommGrpPK, CommPK, frmDate, ToDate, CurrencyPK, HttpContext.Current.Session("LOGED_IN_LOC_FK"), , True, , , , , , , , , )
                dsTHCExisting = FetchCountryPortHeader("",Convert.ToString(PortPK), FrtElementFK, 0, CommGrpPK, 0, frmDate, ToDate, 0, Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]),new DataSet() ,false ,false ,0 ,0 ,0 ,0 ,0 ,0 , CurrentPage,
                TotalPage, 1);
                foreach (DataRow _row in dsTHCExisting.Tables[1].Rows)
                {
                    if (string.IsNullOrEmpty(exChargePks))
                    {
                        exChargePks = Convert.ToString(_row[Convert.ToInt32(CHARGES.CHARGE_PK)]);
                    }
                    else
                    {
                        exChargePks += "," + Convert.ToString(_row[Convert.ToInt32(CHARGES.CHARGE_PK)]);
                    }
                }
                if (exChargePks.Split(',').Length == FrtElementFK.Split(',').Length & !string.IsNullOrEmpty(exChargePks))
                {
                    dsTHC = dsTHCExisting;
                }
                else
                {
                    sb.Append("SELECT COUNTRY.COUNTRY_MST_PK COUNTRY_PK,");
                    sb.Append("       COUNTRY.COUNTRY_ID,");
                    sb.Append("       COUNTRY.COUNTRY_NAME,");
                    sb.Append("       PMT.PORT_MST_PK PORT_FK,");
                    sb.Append("       PMT.PORT_ID,");
                    sb.Append("       PMT.PORT_NAME");
                    sb.Append("  FROM COUNTRY_MST_TBL COUNTRY, PORT_MST_TBL PMT");
                    sb.Append(" WHERE COUNTRY.COUNTRY_MST_PK = PMT.COUNTRY_MST_FK");
                    sb.Append("   AND COUNTRY.ACTIVE_FLAG = 1");
                    sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                    sb.Append("   AND PMT.PORT_MST_PK IN (" + PortPK + ")");

                    objWF.MyDataAdapter = new OracleDataAdapter(sb.ToString(), objWF.MyConnection.ConnectionString);
                    objWF.MyDataAdapter.Fill(dtTHC);
                    dsTHC.Tables.Add(dtTHC);
                    objWF.MyDataAdapter.Dispose();
                    sb.Clear();

                    sb.Append("SELECT ROWNUM SLNO, Q.*");
                    sb.Append("  FROM (SELECT QRY.OPTFLAG,");
                    sb.Append("               QRY.PK,");
                    sb.Append("               QRY.CHARGE_PK,");
                    sb.Append("               QRY.CHARGE_ID,");
                    sb.Append("               QRY.CHARGE_DESC,");
                    sb.Append("               QRY.BIZ_PK,");
                    sb.Append("               QRY.BIZ_ID,");
                    sb.Append("               QRY.CARGO_TYPE,");
                    sb.Append("               QRY.PROCESS_PK,");
                    sb.Append("               QRY.PROCESS_ID,");
                    sb.Append("               QRY.COMMGRP_PK,");
                    sb.Append("               QRY.COMMGRP_ID,");
                    sb.Append("               QRY.COMM_PK,");
                    sb.Append("               QRY.COMM_ID,");
                    sb.Append("               QRY.BASIS_PK,");
                    sb.Append("               QRY.BASIS_ID,");
                    sb.Append("               QRY.CURRENCY_PK,");
                    sb.Append("               QRY.CURRENCY_ID,");
                    sb.Append("               QRY.CNTRTYPE_PK,");
                    sb.Append("               QRY.CNTRTYPE_ID,");
                    sb.Append("               NULL MIN_AMT,");
                    sb.Append("               NULL SHIPMENT_AMT,");
                    sb.Append("               NULL BL_RATE,");
                    sb.Append("               NULL CONT_20_AMT,");
                    sb.Append("               NULL CONT_40_AMT,");
                    sb.Append("               QRY.OTH_CONT,");
                    sb.Append("               QRY.VALID_FROM,");
                    sb.Append("               QRY.VALID_TO,");
                    sb.Append("               QRY.FRT_PRIORITY,");
                    sb.Append("               QRY.COUNTRY_FK,");
                    sb.Append("               QRY.PORT_FK,");
                    sb.Append("               QRY.PORT_ID,");
                    sb.Append("               QRY.TER_PK,");
                    sb.Append("               QRY.TER_ID,");
                    sb.Append("               QRY.CREDIT");
                    sb.Append("          FROM (SELECT DISTINCT 0 PK,");
                    sb.Append("                                to_char(0) OPTFLAG,");
                    sb.Append("                                FRT.BUSINESS_TYPE BIZ_PK,");
                    sb.Append("                                DECODE(FRT.BUSINESS_TYPE,");
                    sb.Append("                                       1,");
                    sb.Append("                                       'Air',");
                    sb.Append("                                       2,");
                    sb.Append("                                       'Sea',");
                    sb.Append("                                       3,");
                    sb.Append("                                       'Both') BIZ_ID,");
                    if (CargoType > 0)
                    {
                        sb.Append("                                '" + (Convert.ToString(CargoType) == "1" ? "FCL" : (CargoType == 2 ? "LCL" : "BBC")) + "' CARGO_TYPE,");
                    }
                    else
                    {
                        sb.Append("                                '' CARGO_TYPE,");
                    }
                    sb.Append("                                PMT.COUNTRY_MST_FK COUNTRY_FK,");
                    sb.Append("                                PMT.PORT_MST_PK PORT_FK,");
                    sb.Append("                                PMT.PORT_ID,");
                    sb.Append("                                PMT.PORT_NAME,");
                    sb.Append("                                NULL TER_PK,");
                    sb.Append("                                NULL TER_ID,");
                    //sb.Append("                                " & Process & " PROCESS_PK,")
                    sb.Append("                                FCT.CHARGE_TYPE PROCESS_PK,");
                    sb.Append("                                DECODE(FCT.CHARGE_TYPE,1,'EXPORT',2,'IMPORT',3,'BOTH') PROCESS_ID,");
                    //sb.Append("'" & IIf(Process = 1, "EXPORT", IIf(Process = 2, "IMPORT", "BOTH")) & "' PROCESS_ID,")
                    sb.Append("                                '' CNTRTYPE_PK,");
                    sb.Append("                                '' CNTRTYPE_ID,");
                    if (CommGrpPK > 0)
                    {
                        sb.Append("                                CGMT.COMMODITY_GROUP_PK COMMGRP_PK,");
                        sb.Append("                                CGMT.COMMODITY_GROUP_CODE COMMGRP_ID,");
                    }
                    else
                    {
                        sb.Append("                                0 COMMGRP_PK,");
                        sb.Append("                                '' COMMGRP_ID,");
                    }

                    if (CommPK > 0)
                    {
                        sb.Append("                                CMT.COMMODITY_MST_PK COMM_PK,");
                        sb.Append("                                CMT.COMMODITY_NAME COMM_ID,");
                    }
                    else
                    {
                        sb.Append("                                '' COMM_PK,'' COMM_ID,");
                    }
                    sb.Append("                                FRT.FREIGHT_ELEMENT_MST_PK CHARGE_PK,");
                    sb.Append("                                FRT.FREIGHT_ELEMENT_ID CHARGE_ID,");
                    sb.Append("                                FRT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                    sb.Append("                                FRT.CHARGE_BASIS BASIS_PK,");
                    sb.Append("                                D.DDID BASIS_ID,");
                    sb.Append("                                CUR.CURRENCY_MST_PK CURRENCY_PK,");
                    sb.Append("                                CUR.CURRENCY_ID CURRENCY_ID,");
                    sb.Append("                                '' MIN_AMT,");
                    sb.Append("                                '' SHIPMENT_AMT,");
                    sb.Append("                                '' BL_RATE,");
                    sb.Append("                                '' CONT_20_AMT,");
                    sb.Append("                                '' CONT_40_AMT,");
                    sb.Append("                                '' OTH_CONT,");
                    if (!string.IsNullOrEmpty(frmDate))
                    {
                        sb.Append("                                TO_DATE('" + Convert.ToDateTime(frmDate).ToString("dd/MM/yyyy") + "',DATEFORMAT) VALID_FROM,");
                    }
                    else
                    {
                        sb.Append("                                TO_DATE(NULL) VALID_FROM,");
                    }
                    if (!string.IsNullOrEmpty(ToDate))
                    {
                        sb.Append("                                TO_DATE('" + Convert.ToDateTime(ToDate).ToString("dd/MM/yyyy") + "',DATEFORMAT) VALID_TO,");
                    }
                    else
                    {
                        sb.Append("                                TO_DATE(NULL) VALID_TO,");
                    }
                    sb.Append("                                FRT.PREFERENCE AS FRT_PRIORITY,");
                    sb.Append("                                NVL(FRT.CREDIT,0) CREDIT");
                    sb.Append("                  FROM PORT_MST_TBL            PMT,");
                    sb.Append("                       COUNTRY_MST_TBL         COUNTRY,");
                    sb.Append("                       FREIGHT_ELEMENT_MST_TBL FRT,");
                    sb.Append("                       FREIGHT_CONFIG_TRN_TBL  FCT,");
                    if (CommPK > 0)
                    {
                        sb.Append("                       COMMODITY_MST_TBL       CMT,");
                    }
                    if (CommGrpPK > 0)
                    {
                        sb.Append("                       COMMODITY_GROUP_MST_TBL CGMT,");
                    }
                    sb.Append("                       CURRENCY_TYPE_MST_TBL   CUR,");
                    sb.Append("                       (SELECT TO_NUMBER(DD.DD_VALUE) DDVALUE, DD.DD_ID DDID");
                    sb.Append("                          FROM QFOR_DROP_DOWN_TBL DD");
                    sb.Append("                         WHERE DD.DD_FLAG = 'BASIS'");
                    sb.Append("                           AND DD.CONFIG_ID = 'QFOR4458') D");
                    sb.Append("                 WHERE PMT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK(+)");
                    sb.Append("                   AND FRT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
                    sb.Append("                   AND FRT.CHARGE_BASIS = D.DDVALUE(+)");
                    if (!string.IsNullOrEmpty(exChargePks.Trim()))
                    {
                        sb.Append("                   AND FRT.FREIGHT_ELEMENT_MST_PK NOT IN (" + exChargePks.Trim() + ")");
                    }
                    sb.Append("                   AND FRT.FREIGHT_ELEMENT_MST_PK IN (" + FrtElementFK + ")");
                    sb.Append("                   AND PMT.PORT_MST_PK IN (" + PortPK + ")");
                    sb.Append("                   AND CUR.CURRENCY_MST_PK IN (" + CurrencyPK + ")");
                    if (CommGrpPK > 0)
                    {
                        sb.Append("                   AND CGMT.COMMODITY_GROUP_PK IN (" + CommGrpPK + ")");
                    }

                    if (CommPK > 0 & CommGrpPK > 0)
                    {
                        sb.Append("                   AND CMT.COMMODITY_GROUP_FK = " + CommGrpPK);
                        sb.Append("                   AND CMT.COMMODITY_MST_PK = " + CommPK);
                    }
                    sb.Append(" ) QRY");
                    sb.Append("         ORDER BY FRT_PRIORITY) Q");
                    dtTHC = new DataTable();
                    objWF.MyDataAdapter = new OracleDataAdapter(sb.ToString(), objWF.MyConnection.ConnectionString);
                    objWF.MyDataAdapter.Fill(dtTHC);
                    dsTHC.Tables.Add(dtTHC);
                    DataRelation rel = new DataRelation("rl_PORT_CHARGES", new DataColumn[] {
                        dsTHC.Tables[0].Columns["COUNTRY_PK"],
                        dsTHC.Tables[0].Columns["PORT_FK"]
                    }, new DataColumn[] {
                        dsTHC.Tables[1].Columns["COUNTRY_FK"],
                        dsTHC.Tables[1].Columns["PORT_FK"]
                    });
                    dsTHC.Relations.Clear();
                    dsTHC.Relations.Add(rel);
                    if (dsTHCExisting.Tables[0].Rows.Count > 0)
                    {
                        foreach (DataRow _row in dsTHC.Tables[1].Rows)
                        {
                            DataRow dr = dsTHCExisting.Tables[1].NewRow();
                            for (int _colIndex = 0; _colIndex <= Enum.GetNames(typeof(CHARGES)).Length - 1; _colIndex++)
                            {
                                dr[_colIndex] = _row[_colIndex];
                            }
                            dsTHCExisting.Tables[1].Rows.Add(dr);
                        }
                        dsTHC = dsTHCExisting;
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dsTHC;
        }

        #endregion "Generate"

        #region "Save"

        public ArrayList SaveLocalChargesEntryNew(object UWG1, long containerStatus = 2, DataSet ContDs = null, int CargoType = 0, bool IsNewEntry = true)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction insertTrans = null;
            OracleCommand insCommand = new OracleCommand();
            int tblCnt = 0;
            Int32 RecAfct = default(Int32);
            Int32 RowCnt = default(Int32);
            Int32 RowCntContainer = default(Int32);
            Int32 RowCnt3 = default(Int32);
            Int16 versionNo = 0;
            var strNull = "";
            string flag = "N";
            Int32 initValue = 0;
            DateTime enteredToDate = default(DateTime);
            long standardFrtPk = 0;
            long selectedRows = 0;
            int zero = 0;
            int strOne = 1;
            objWK.OpenConnection();
            insertTrans = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                //for (RowCnt = 0; RowCnt <= UWG1.Rows.Count() - 1; RowCnt++)
                //{
                //    for (int chargeIndex = 0; chargeIndex <= UWG1.Rows(RowCnt).Rows.Count - 1; chargeIndex++)
                //    {
                //        if (UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.OPTFLAG).value == "1" | UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.OPTFLAG).value == "true")
                //        {
                //            _with1.Parameters.Clear();
                //            if (Conversion.Val(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.PK).value) > 0)
                //            {
                //                IsNewEntry = false;
                //            }
                //            else
                //            {
                //                IsNewEntry = true;
                //            }

                //            if (IsNewEntry)
                //            {
                //                _with1.CommandText = objWK.MyUserName + ".TARIFF_TRN_PKG.TARIFF_TRN_INS_LOCAL";
                //            }
                //            else
                //            {
                //                _with1.CommandText = objWK.MyUserName + ".TARIFF_TRN_PKG.TARIFF_TRN_UPD_LOCAL";
                //            }
                //            var _with2 = _with1.Parameters;
                //            _with2.Add("TARIFF_PK_IN", UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.PK).value).Direction = ParameterDirection.Input;
                //            _with2.Add("STANDARD_FREIGHT_CODE_IN", strNull.Value).Direction = ParameterDirection.Input;
                //            _with2.Add("POL_FK_IN", UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.PORT_FK).value).Direction = ParameterDirection.Input;
                //            _with2.Add("POD_FK_IN", zero).Direction = ParameterDirection.Input;
                //            _with2.Add("SERVICE_MST_FK_IN", strNull.Value).Direction = ParameterDirection.Input;
                //            _with2.Add("CARRIER_MST_FK_IN", M_LINE_MST_FK).Direction = ParameterDirection.Input;
                //            _with2.Add("POL_TER_FK_IN", (UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.TER_PK).value != null ? UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.TER_PK).value : strNull.Value)).Direction = ParameterDirection.Input;
                //            _with2.Add("POD_TER_FK_IN", strNull.Value).Direction = ParameterDirection.Input;
                //            _with2.Add("COMMODITY_GROUP_MST_FK_IN", UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.COMMGRP_PK).value).Direction = ParameterDirection.Input;
                //            _with2.Add("COMMODITY_MST_FK_IN", (UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.COMM_PK).value != null ? UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.COMM_PK).value : strNull.Value)).Direction = ParameterDirection.Input;
                //            _with2.Add("FREIGHT_ELEMENT_MST_FK_IN", UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.CHARGE_PK).value).Direction = ParameterDirection.Input;
                //            _with2.Add("PERCENTAGE_AMOUNT_IN", strOne).Direction = ParameterDirection.Input;
                //            _with2.Add("CURRENCY_MST_FK_IN", UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.CURRENCY_PK).value).Direction = ParameterDirection.Input;
                //            _with2.Add("CONTAINTER_TYPE_MST_FK_IN", zero).Direction = ParameterDirection.Input;
                //            _with2.Add("RATE_IN", getDefault(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.MIN_AMT).value, "")).Direction = ParameterDirection.Input;
                //            _with2.Add("SHIPMENT_AMT_IN", getDefault(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.SHIPMENT_AMT).value, "")).Direction = ParameterDirection.Input;
                //            _with2.Add("CONT_20_AMT_IN", getDefault(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.CONT_20_AMT).value, "")).Direction = ParameterDirection.Input;
                //            _with2.Add("CONT_40_AMT_IN", getDefault(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.CONT_40_AMT).value, "")).Direction = ParameterDirection.Input;
                //            if (Information.IsDate(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.VALID_FROM).value) == true)
                //            {
                //                _with2.Add("EFFECTIVE_FROM_IN", System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.VALID_FROM).value))).Direction = ParameterDirection.Input;
                //            }
                //            else
                //            {
                //                _with2.Add("EFFECTIVE_FROM_IN", strNull.Value).Direction = ParameterDirection.Input;
                //            }
                //            if (Information.IsDate(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.VALID_TO).value) == true)
                //            {
                //                _with2.Add("EFFECTIVE_TO_IN", System.String.Format("{0:dd-MMM-yyyy}", Convert.ToDateTime(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.VALID_TO).value))).Direction = ParameterDirection.Input;
                //            }
                //            else
                //            {
                //                _with2.Add("EFFECTIVE_TO_IN", strNull.Value).Direction = ParameterDirection.Input;
                //            }

                //            if (!IsNewEntry)
                //            {
                //                if (Information.IsDate(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.VALID_TO).value))
                //                {
                //                    Date changedToDate = default(Date);
                //                    changedToDate = Convert.ToDateTime(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.VALID_TO).value);
                //                    _with2.Add("CHANGED_TO_DT", (Strings.Trim(changedToDate) != "00:00:00" ? System.String.Format("{0:dd-MMM-yyyy}", changedToDate) : strNull.Value)).Direction = ParameterDirection.Input;
                //                }
                //                else
                //                {
                //                    _with2.Add("CHANGED_TO_DT", "").Direction = ParameterDirection.Input;
                //                }
                //            }

                //            if ((UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.COMMGRP_PK).value != null) & !Information.IsDBNull(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.COMMGRP_PK).value))
                //            {
                //                if (Strings.UCase(UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.COMMGRP_ID).value) == "EMPTY")
                //                {
                //                    containerStatus = 1;
                //                }
                //                else
                //                {
                //                    containerStatus = 2;
                //                }
                //            }
                //            else
                //            {
                //                containerStatus = 2;
                //            }

                //            _with2.Add("CONTAINER_STATUS_IN", containerStatus).Direction = ParameterDirection.Input;
                //            _with2.Add("PORT_GROUP_IN", zero).Direction = ParameterDirection.Input;
                //            _with2.Add("ITEM_TYPE_IN", strOne).Direction = ParameterDirection.Input;
                //            _with2.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                //            _with2.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //            _with2.Add("SR_NO_IN", UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.SLNO).value).Direction = ParameterDirection.Input;
                //            _with2.Add("BASIS_IN", UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.BASIS_PK).value).Direction = ParameterDirection.Input;
                //            _with2.Add("PROCESS_IN", UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.PROCESS_PK).value).Direction = ParameterDirection.Input;

                //            string _cg = UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.CARGO_TYPE).value;
                //            if ((_cg == null))
                //                _cg = "";
                //            else
                //                _cg = _cg.ToUpper();
                //            if (_cg == "FCL" | _cg == "1")
                //            {
                //                _cg = "1";
                //            }
                //            else if (_cg == "LCL" | _cg == "2")
                //            {
                //                _cg = "2";
                //            }
                //            else if (_cg == "BBC" | _cg == "4")
                //            {
                //                _cg = "4";
                //            }
                //            else
                //            {
                //                _cg = CargoType;
                //            }
                //            _with2.Add("CARGO_TYPE_IN", _cg).Direction = ParameterDirection.Input;

                //            _with2.Add("CTRTYPE_IN", UWG1.Rows(RowCnt).Rows(chargeIndex).Cells(CHARGES.CNTRTYPE_PK).value).Direction = ParameterDirection.Input;
                //            _with2.Add("CHARGE_TYPE_IN", 2).Direction = ParameterDirection.Input;
                //            //1-Tariff,2-Local Charges

                //            _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //            insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                //            var _with3 = objWK.MyDataAdapter;
                //            _with3.InsertCommand = insCommand;
                //            _with3.InsertCommand.Transaction = insertTrans;
                //            _with3.InsertCommand.ExecuteNonQuery();
                //        }
                //    }
                //}
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
            }
            if (arrMessage.Count == 0)
            {
                insertTrans.Commit();
                arrMessage.Add("All Data Saved Successfully");
            }
            else
            {
                insertTrans.Rollback();
            }
            return arrMessage;
        }

        #endregion "Save"

        #endregion "Local Charges New"
    }
}