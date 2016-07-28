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
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_Transport_Note : CommonFeatures
    {
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
        private DataSet CMRDS = new DataSet();
        private int CMRPK = new int();
        public System.Text.StringBuilder StrCondition = new System.Text.StringBuilder(5000);

        public string sbNew;

        #region "Update Collection/Delivery Address"

        public bool UpdateAddress(string CollAddress, string DelAddress, string RefNo, long UserType, bool Export)
        {
            WorkFlow ObjWk = new WorkFlow();
            OracleTransaction Tran = null;
            string Strsql = null;
            ObjWk.OpenConnection();
            Tran = ObjWk.MyConnection.BeginTransaction();
            if (Export == true)
            {
                if (UserType == 2)
                {
                    Strsql = "UPDATE BOOKING_MST_TBL B SET  B.COL_ADDRESS='" + CollAddress + "',B.DEL_ADDRESS='" + DelAddress + "' WHERE B.BOOKING_REF_NO='" + RefNo + "'";
                }
                else if (UserType == 1)
                {
                    Strsql = "UPDATE BOOKING_MST_TBL B SET  B.COL_ADDRESS='" + CollAddress + "',B.DEL_ADDRESS='" + DelAddress + "' WHERE B.BOOKING_REF_NO='" + RefNo + "'";
                }
            }
            else
            {
                if (UserType == 2)
                {
                    Strsql = "update JOB_CARD_TRN set DEL_ADDRESS='" + DelAddress + "' where UPPER(JOBCARD_REF_NO)='" + RefNo + "'";
                }
                else if (UserType == 1)
                {
                    Strsql = "update JOB_CARD_TRN set DEL_ADDRESS='" + DelAddress + "' where UPPER(JOBCARD_REF_NO)='" + RefNo + "'";
                }
            }

            try
            {
                var _with1 = ObjWk.MyCommand;
                _with1.CommandText = Strsql;
                _with1.CommandType = CommandType.Text;
                _with1.Transaction = Tran;

                ObjWk.MyCommand.ExecuteNonQuery();
                Tran.Commit();
                return true;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return false;
            }
            finally
            {
                ObjWk.CloseConnection();
            }
        }

        #endregion "Update Collection/Delivery Address"

        #region "Fetch Containers"

        public DataSet FetchContainers(string strSql)
        {
            try
            {
                WorkFlow objwf = new WorkFlow();
                return objwf.GetDataSet(strSql);
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

        #endregion "Fetch Containers"

        #region "Fetch Grid Rates History"

        public object FetchforGridRates(string validFrom, string validTo, string Loc, int CargoType, string ReferType, int BizType, string Locpk = "", string polpk = "", string podpk = "", string Customer = "",
        Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition1 = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            if ((Convert.ToInt32(Loc) != 0))
            {
                StrCondition.Append(" and USR.DEFAULT_LOCATION_FK IN (  " + Loc + ") ");
            }
            if ((!string.IsNullOrEmpty(polpk) & polpk != "0"))
            {
                StrCondition.Append(" and pol.port_mst_pk =  " + polpk + " ");
            }

            if ((!string.IsNullOrEmpty(podpk) & podpk != "0"))
            {
                StrCondition.Append(" and pod.port_mst_pk =  " + podpk + " ");
            }

            if ((!string.IsNullOrEmpty(Customer)))
            {
                StrCondition.Append(" and cmt.customer_mst_pk =  " + Customer + " ");
            }

            if ((BizType == 2) & ReferType == "SRR" & CargoType == 1)
            {
                forGridSeaSRRFCL(validFrom, validTo);
            }

            if ((BizType == 2) & ReferType == "SRR" & CargoType == 2)
            {
                forGridSeaSRRLCL(validFrom, validTo);
            }

            if ((BizType == 2) & ReferType == "QTN" & CargoType == 1)
            {
                forGridQtnSeaFCL(validFrom, validTo);
            }

            if ((BizType == 2) & ReferType == "QTN" & CargoType == 2)
            {
                forGridQtnSeaLCL(validFrom, validTo);
            }

            if ((BizType == 2) & ReferType == "QTN" & CargoType == 4)
            {
                forGridQtnSeaBBC(validFrom, validTo);
            }

            if (BizType == 2 & ReferType == "BOTH" & CargoType == 1)
            {
                forGridSeaSRRAndQtnFCL(validFrom, validTo);
            }

            if (BizType == 2 & ReferType == "BOTH" & CargoType == 2)
            {
                forGridSeaSRRAndQtnLCL(validFrom, validTo);
            }

            if ((BizType == 1) & ReferType == "SRR")
            {
                forGridSRRAir(validFrom, validTo);
            }

            if ((BizType == 1) & ReferType == "QTN")
            {
                forGridQtnAir(validFrom, validTo);
            }

            if (BizType == 1 & ReferType == "BOTH")
            {
                forGridAirSRRAndQtn(validFrom, validTo);
            }

            if (BizType == 0 & ReferType == "SRR")
            {
                forGridAirSeaSRR(validFrom, validTo);
            }
            if (BizType == 0 & ReferType == "QTN")
            {
                forGridAirSeaQua(validFrom, validTo);
            }
            if (BizType == 0 & ReferType == "BOTH")
            {
                forGridAirSeaBoth(validFrom, validTo);
            }

            try
            {
                System.Text.StringBuilder strCount = new System.Text.StringBuilder();
                System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
                strCount.Append(" SELECT COUNT(*)  from  ");
                strCount.Append(("(" + sbNew.ToString() + ""));

                strCount.Append(" )");
                TotalRecords = Convert.ToInt32(ObjWk.ExecuteScaler(strCount.ToString()));
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
                strCount.Remove(0, strCount.Length);

                sqlstr2.Append(" Select * from (");
                sqlstr2.Append(" SELECT ROWNUM SL_NO, q.*  FROM ( ");
                sqlstr2.Append("  (" + sbNew.ToString() + " ");

                sqlstr2.Append(" ) q )) ");
                sqlstr2.Append("   WHERE \"SL_NO\"  BETWEEN " + start + " AND " + last + "");
                strSql = sqlstr2.ToString();
                return objWF.GetDataSet(strSql);
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

        #endregion "Fetch Grid Rates History"

        #region "forGridSRRFCL()"

        public void forGridSeaSRRFCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                 sst.srr_sea_pk as \"REF PK\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("              SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\",");
                sb.Append("          'SEA' \"BIZType\",");
                sb.Append("          'FCL' \"CargoType\",");
                sb.Append("          sst.status as \"StausType\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.CARGO_TYPE = 1");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append("  AND (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') )  ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     sst.srr_sea_pk,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("    LMT.LOCATION_NAME,");
                sb.Append("    POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO,");
                sb.Append("    sst.status");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSRRFCL()"

        #region "forGridSRRLCL()"

        public void forGridSeaSRRLCL(string validFrom, string validTo)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                 sst.srr_sea_pk as \"REF PK\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\",");
                sb.Append("                  'SEA' \"BIZType\",");
                sb.Append("                  'LCL' \"CargoType\",");
                sb.Append("                 sst.status as \"StausType\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.CARGO_TYPE = 2");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    sst.srr_sea_pk,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO,");
                sb.Append("     sst.status");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSRRLCL()"

        #region "forGridQtnFCL()"

        public void forGridQtnSeaFCL(string validFrom, string validTo)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("              qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("                 'SEA' \"BIZType\",");
                sb.Append("                 'FCL' \"CargoType\",");
                sb.Append("                  0  \"StausType\"");
                sb.Append("  FROM QUOTATION_DTL_TBL  QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN     QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("  AND  QTNS.CARGO_TYPE=1");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK AND QTNS.BIZ_TYPE=2");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date  <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     qtns.QUOTATION_MST_PK,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridQtnFCL()"

        #region "forGridQtnLCL()"

        public void forGridQtnSeaLCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                  qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("              'SEA' \"BIZType\",");
                sb.Append("              'LCL' \"CargoType\",");
                sb.Append("                  0   \"StausType\"");
                sb.Append("  FROM QUOTATION_DTL_TBL  QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN    QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND  QTNS.CARGO_TYPE=2 AND QTNS.BIZ_TYPE=2");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("   qtns.QUOTATION_MST_PK,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridQtnLCL()"

        #region "forGridQtnSeaBBC()"

        public void forGridQtnSeaBBC(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                 qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("               'SEA' \"BIZType\",");
                sb.Append("                 'BBC' \"CargoType\",");
                sb.Append("                  0   \"StausType\"");
                sb.Append("  FROM QUOTATION_DTL_TBL  QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN    QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND QTNS.CARGO_TYPE=4 AND QTNS.BIZ_TYPE=2");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date <= to_date('" + validTo + "','DD/MM/YYYY')  ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    qtns.QUOTATION_MST_PK,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");
                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridQtnSeaBBC()"

        #region "forGridSRRQtnFCL()"

        public void forGridSeaSRRAndQtnFCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                 sst.srr_sea_pk as \"REF PK\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("               ");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\",");
                sb.Append("             'SEA' \"BIZType\",");
                sb.Append("             'FCL' \"CargoType\",");
                sb.Append("             sst.status as \"StausType\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.CARGO_TYPE=1");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    sst.srr_sea_pk,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO,");
                sb.Append("    sst.status");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("               qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("                  'SEA' \"BIZType\",");
                sb.Append("                 'FCL' \"CargoType\",");
                sb.Append("                  0 \"StausType\"");
                sb.Append("  FROM QUOTATION_DTL_TBL  QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN    QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND QTNS.CARGO_TYPE=1 AND QTNS.BIZ_TYPE=2");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    qtns.QUOTATION_MST_PK,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSRRQtnFCL()"

        #region "forGridBothLCL()"

        public void forGridSeaSRRAndQtnLCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                 sst.srr_sea_pk as \"REF PK\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("               ");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\",");
                sb.Append("               'SEA' \"BIZType\",");
                sb.Append("              'LCL' \"CargoType\",");
                sb.Append("              sst.status as \"StausType\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.CARGO_TYPE=2");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     sst.srr_sea_pk,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO,");
                sb.Append("   sst.status");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                  qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("                  'SEA' \"BIZType\",");
                sb.Append("                   'LCL' \"CargoType\",");
                sb.Append("                   0 \"StausType\"");
                sb.Append("  FROM QUOTATION_DTL_TBL  QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN   QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND QTNS.CARGO_TYPE=2 AND QTNS.BIZ_TYPE=2");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    qtns.QUOTATION_MST_PK,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridBothLCL()"

        #region "forGridSRRAir()"

        public void forGridSRRAir(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                 SRRAIR.srr_air_pk as \"REF PK\",");
                sb.Append("                SRRAIR.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                SUM(NVL(SAFT.MIN_AMOUNT, 0)) RATE,");
                sb.Append("                SRRAIR.SRR_DATE AS \"Issued\",");
                sb.Append("                SRRAIR.VALID_TO AS \"Validity\",");
                sb.Append("                  'AIR' \"BIZType\",");
                sb.Append("                 '' \"CargoType\",");
                sb.Append("                 SRRAIR.Srr_Approved as \"StausType\"");
                sb.Append("  FROM SRR_TRN_AIR_TBL        SRRTRN,");
                sb.Append("       SRR_AIR_TBL            SRRAIR,");
                sb.Append("       SRR_AIR_FREIGHT_TBL   SAFT,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("         CURRENCY_TYPE_MST_TBL  CURR");
                sb.Append(" WHERE SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("   AND SAFT.SRR_TRN_AIR_FK = SRRTRN.SRR_TRN_AIR_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SRRAIR.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   AND SAFT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (SRRAIR.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SRRAIR.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SRRAIR.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     SRRAIR.srr_air_pk,");
                sb.Append("      SRRAIR.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("    SAFT.MIN_AMOUNT,");
                sb.Append("     SRRAIR.SRR_DATE,");
                sb.Append("    SRRAIR.VALID_TO,");
                sb.Append("    SRRAIR.Srr_Approved");
                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSRRAir()"

        #region "forGridQtnAir()"

        public void forGridQtnAir(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(FTRAN.QUOTED_RATE * get_ex_rate(FTRAN.CURRENCY_MST_FK,MAIN1.base_currency_fk,MAIN1.quotation_date), 0))) RATE,");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("               'AIR' \"BIZType\",");
                sb.Append("              ' ' \"CargoType\",");
                sb.Append("       MAIN1.QUOTATION_TYPE as \"StausType\"");
                sb.Append("  FROM QUOTATION_MST_TBL          MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL          TRAN,");
                sb.Append("       QUOTATION_FREIGHT_TRN     FTRAN,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CURR");
                sb.Append(" WHERE TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND FTRAN.QUOTATION_DTL_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = MAIN1.CUSTOMER_MST_FK");
                sb.Append("   AND MAIN1.base_currency_fk = CURR.CURRENCY_MST_PK AND MAIN1.BIZ_TYPE=1");
                sb.Append("   AND (MAIN1.STATUS = 2 OR MAIN1.STATUS=4)");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (MAIN1.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and main1.quotation_date <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND MAIN1.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND MAIN1.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( MAIN1.quotation_date +  MAIN1.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     MAIN1.QUOTATION_MST_PK,");
                sb.Append("     MAIN1.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("     MAIN1.QUOTATION_DATE,");
                sb.Append("     MAIN1.VALID_FOR,");
                sb.Append("       MAIN1.QUOTATION_TYPE");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridQtnAir()"

        #region "forGridAirSRRAndQtn()"

        public void forGridAirSRRAndQtn(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("             SRRAIR.srr_air_pk as \"REF PK\",");
                sb.Append("                SRRAIR.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                SUM(NVL(SAFT.MIN_AMOUNT, 0)) RATE,");
                sb.Append("                SRRAIR.SRR_DATE AS \"Issued\",");
                sb.Append("                SRRAIR.VALID_TO AS \"Validity\",");
                sb.Append("                  'AIR' \"BIZType\",");
                sb.Append("                  '' \"CargoType\",");
                sb.Append("                 SRRAIR.Srr_Approved as \"StausType\"");
                sb.Append("  FROM SRR_TRN_AIR_TBL        SRRTRN,");
                sb.Append("       SRR_AIR_TBL            SRRAIR,");
                sb.Append("       SRR_AIR_FREIGHT_TBL   SAFT,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("         CURRENCY_TYPE_MST_TBL  CURR");
                sb.Append(" WHERE SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("   AND SAFT.SRR_TRN_AIR_FK = SRRTRN.SRR_TRN_AIR_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SRRAIR.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   AND SAFT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (SRRAIR.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SRRAIR.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SRRAIR.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     SRRAIR.srr_air_pk,");
                sb.Append("      SRRAIR.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("    SAFT.MIN_AMOUNT,");
                sb.Append("     SRRAIR.SRR_DATE,");
                sb.Append("    SRRAIR.VALID_TO,");
                sb.Append("    SRRAIR.Srr_Approved");

                sb.Append(" union ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(FTRAN.QUOTED_RATE * get_ex_rate(FTRAN.CURRENCY_MST_FK,MAIN1.base_currency_fk,MAIN1.quotation_date), 0))) RATE,");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("                 'AIR' \"BIZType\",");
                sb.Append("                 '' \"CargoType\",");
                sb.Append("       MAIN1.QUOTATION_TYPE as \"StausType\"");
                sb.Append("  FROM QUOTATION_MST_TBL          MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL          TRAN,");
                sb.Append("       QUOTATION_FREIGHT_TRN     FTRAN,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CURR");
                sb.Append(" WHERE TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND FTRAN.QUOTATION_DTL_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = MAIN1.CUSTOMER_MST_FK");
                sb.Append("   AND MAIN1.base_currency_fk = CURR.CURRENCY_MST_PK AND MAIN1.BIZ_TYPE=1");
                sb.Append("   AND (MAIN1.STATUS = 2 OR MAIN1.STATUS=4)");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (MAIN1.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and main1.quotation_date <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND MAIN1.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND MAIN1.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( MAIN1.quotation_date +  MAIN1.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("    GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    MAIN1.QUOTATION_MST_PK,");
                sb.Append("     MAIN1.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("     MAIN1.QUOTATION_DATE,");
                sb.Append("     MAIN1.VALID_FOR,");
                sb.Append("      MAIN1.QUOTATION_TYPE");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridAirSRRAndQtn()"

        #region "forGridAirSeaSRR"

        public void forGridAirSeaSRR(string validFrom, string validTo, Int32 flag = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                string StrCondition2 = null;
                if (Convert.ToInt32(StrCondition2) == 0)
                {
                    if (flag == 0)
                    {
                        StrCondition2 = " AND 1=2";
                    }
                }

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("              sst.srr_sea_pk as \"REF PK\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\",");
                sb.Append("                 'SEA' \"BIZType\",");
                sb.Append("                 '' \"CargoType\",");
                sb.Append("       sst.status as \"StausType\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    sst.srr_sea_pk,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO,");
                sb.Append("       sst.status");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                  SRRAIR.srr_air_pk as \"REF PK\",");
                sb.Append("                SRRAIR.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"AOO\",");
                sb.Append("                POD.PORT_NAME AS \"AOD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                SUM(NVL(SAFT.MIN_AMOUNT, 0)) RATE,");
                sb.Append("                SRRAIR.SRR_DATE AS \"Issued\",");
                sb.Append("                SRRAIR.VALID_TO AS \"Validity\",");
                sb.Append("                 'AIR' \"BIZType\",");
                sb.Append("                 '' \"CargoType\",");
                sb.Append("       SRRAIR.Srr_Approved as \"StausType\"");
                sb.Append("  FROM SRR_TRN_AIR_TBL        SRRTRN,");
                sb.Append("       SRR_AIR_TBL            SRRAIR,");
                sb.Append("       SRR_AIR_FREIGHT_TBL   SAFT,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("         CURRENCY_TYPE_MST_TBL  CURR");
                sb.Append(" WHERE SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("   AND SAFT.SRR_TRN_AIR_FK = SRRTRN.SRR_TRN_AIR_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SRRAIR.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   AND SAFT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (SRRAIR.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SRRAIR.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SRRAIR.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("   SRRAIR.srr_air_pk,");
                sb.Append("     SRRAIR.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("    SAFT.MIN_AMOUNT,");
                sb.Append("     SRRAIR.SRR_DATE,");
                sb.Append("    SRRAIR.VALID_TO,");
                sb.Append("    SRRAIR.Srr_Approved");
                sb.Append(" ORDER BY \"Issued\" DESC");

                sbNew = sb.ToString();
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

        #endregion "forGridAirSeaSRR"

        #region "forGridAirSeaQua"

        public void forGridAirSeaQua(string validFrom, string validTo, Int32 flag = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                string StrCondition2 = null;
                if (Convert.ToInt32(StrCondition2) == 0)
                {
                    if (flag == 0)
                    {
                        StrCondition2 = " AND 1=2";
                    }
                }

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                 qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * get_ex_rate(qtsfd.currency_mst_fk,qtns.base_currency_fk,qtns.quotation_date), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("                 'SEA' \"BIZType\",");
                sb.Append("                DECODE(qtns.cargo_type, 1, 'FCL', 2, 'LCL',4,'BBC') As \"CargoType\",");
                sb.Append("            0  \"StausType\"");
                sb.Append("  FROM QUOTATION_DTL_TBL         QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN     QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.base_currency_fk  = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK AND QTNS.BIZ_TYPE=2");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date  <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    qtns.QUOTATION_MST_PK,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR,");
                sb.Append("   qtns.cargo_type");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO AS \"REF No\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"AOO\",");
                sb.Append("                POD.PORT_NAME AS \"AOD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(FTRAN.QUOTED_RATE * get_ex_rate(FTRAN.CURRENCY_MST_FK,MAIN1.base_currency_fk,MAIN1.quotation_date), 0))) RATE,");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("                 'AIR' \"BIZType\",");
                sb.Append("                 '' \"CargoType\",");
                sb.Append("       MAIN1.QUOTATION_TYPE as \"StausType\"");
                sb.Append("  FROM QUOTATION_MST_TBL          MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL          TRAN,");
                sb.Append("       QUOTATION_FREIGHT_TRN     FTRAN,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CURR");
                sb.Append(" WHERE TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND FTRAN.QUOTATION_DTL_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = MAIN1.CUSTOMER_MST_FK");
                sb.Append("   AND MAIN1.base_currency_fk = CURR.CURRENCY_MST_PK AND MAIN1.BIZ_TYPE=1");
                sb.Append("   AND (MAIN1.STATUS = 2 OR MAIN1.STATUS=4)");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (MAIN1.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and main1.quotation_date <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND MAIN1.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND MAIN1.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( MAIN1.quotation_date +  MAIN1.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    MAIN1.QUOTATION_MST_PK,");
                sb.Append("     MAIN1.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("     MAIN1.QUOTATION_DATE,");
                sb.Append("     MAIN1.VALID_FOR,");
                sb.Append("       MAIN1.QUOTATION_TYPE");
                sb.Append("  order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridAirSeaQua"

        #region "forGridAirSeaBoth"

        public void forGridAirSeaBoth(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                 sst.srr_sea_pk as \"REF PK\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("               ");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\",");
                sb.Append("            'SEA' \"BIZType\",");
                sb.Append("          '' \"CargoType\",");
                sb.Append("       sst.status as \"StausType\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append(" and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    sst.srr_sea_pk,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO,");
                sb.Append("       sst.status");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                 qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * get_ex_rate(qtsfd.currency_mst_fk,qtns.base_currency_fk,qtns.quotation_date), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("                'SEA' \"BIZType\",");
                sb.Append("                DECODE(qtns.cargo_type, 1, 'FCL', 2, 'LCL',4,'BBC') As \"CargoType\",");
                sb.Append("                   0  \"StausType\"");
                sb.Append("  FROM QUOTATION_DTL_TBL           QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN      QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND qtns.base_currency_fk = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK AND QTNS.BIZ_TYPE=2");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append(" and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    qtns.QUOTATION_MST_PK,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR,");
                sb.Append("   qtns.cargo_type");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SRRAIR.srr_air_pk as \"REF PK\",");
                sb.Append("                SRRAIR.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"AOO\",");
                sb.Append("                POD.PORT_NAME AS \"AOD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                SUM(NVL(SAFT.MIN_AMOUNT, 0)) RATE,");
                sb.Append("                SRRAIR.SRR_DATE AS \"Issued\",");
                sb.Append("                SRRAIR.VALID_TO AS \"Validity\",");
                sb.Append("               'AIR' \"BIZType\",");
                sb.Append("               '' \"CargoType\",");
                sb.Append("              SRRAIR.Srr_Approved as \"StausType\"");
                sb.Append("  FROM SRR_TRN_AIR_TBL        SRRTRN,");
                sb.Append("       SRR_AIR_TBL            SRRAIR,");
                sb.Append("       SRR_AIR_FREIGHT_TBL   SAFT,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("         CURRENCY_TYPE_MST_TBL  CURR");
                sb.Append(" WHERE SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("   AND SAFT.SRR_TRN_AIR_FK = SRRTRN.SRR_TRN_AIR_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SRRAIR.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   AND SAFT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (SRRAIR.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SRRAIR.SRR_DATE <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append(" and to_date(SRRAIR.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     SRRAIR.srr_air_pk,");
                sb.Append("      SRRAIR.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("    SAFT.MIN_AMOUNT,");
                sb.Append("     SRRAIR.SRR_DATE,");
                sb.Append("    SRRAIR.VALID_TO,");
                sb.Append("    SRRAIR.Srr_Approved");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"AOO\",");
                sb.Append("                POD.PORT_NAME AS \"AOD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(FTRAN.QUOTED_RATE * get_ex_rate(FTRAN.CURRENCY_MST_FK,MAIN1.base_currency_fk,MAIN1.quotation_date), 0))) RATE,");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Validity\",");
                sb.Append("              'AIR' \"BIZType\",");
                sb.Append("              '' \"CargoType\",");
                sb.Append("             MAIN1.QUOTATION_TYPE as \"StausType\"");
                sb.Append("  FROM QUOTATION_MST_TBL          MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL          TRAN,");
                sb.Append("       QUOTATION_FREIGHT_TRN FTRAN,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CURR");
                sb.Append(" WHERE TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND FTRAN.QUOTATION_DTL_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = MAIN1.CUSTOMER_MST_FK AND MAIN1.BIZ_TYPE=1");
                sb.Append("   AND MAIN1.base_currency_fk = CURR.CURRENCY_MST_PK");
                sb.Append("   AND (MAIN1.STATUS = 2 OR MAIN1.STATUS=4)");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (MAIN1.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and main1.quotation_date <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND MAIN1.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND MAIN1.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append(" and to_date( MAIN1.quotation_date +  MAIN1.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    MAIN1.QUOTATION_MST_PK,");
                sb.Append("     MAIN1.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("     MAIN1.QUOTATION_DATE,");
                sb.Append("    MAIN1.VALID_FOR,");
                sb.Append("    MAIN1.QUOTATION_TYPE");
                sb.Append("  order by \"Issued\" desc");

                sbNew = sb.ToString();
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

        #endregion "forGridAirSeaBoth"

        #region "Fetch Rating History"

        public object FetchforReportRating(string validFrom, string validTo, string Loc, int CargoType, string ReferType, int BizType, string Locpk = "", string polpk = "", string podpk = "", string Custpk = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition1 = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            if ((Convert.ToInt32(Loc) != 0))
            {
                StrCondition.Append(" and USR.DEFAULT_LOCATION_FK IN ( " + Loc + ") ");
            }

            if ((!string.IsNullOrEmpty(polpk) & polpk != "0"))
            {
                StrCondition.Append(" and pol.port_mst_pk =  " + polpk + " ");
            }

            if ((!string.IsNullOrEmpty(podpk) & podpk != "0"))
            {
                StrCondition.Append(" and pod.port_mst_pk =  " + podpk + " ");
            }

            if ((!string.IsNullOrEmpty(Custpk)))
            {
                StrCondition.Append(" and cmt.customer_mst_pk =  " + Custpk + " ");
            }

            if ((BizType == 2) & ReferType == "SRR" & CargoType == 1)
            {
                fnSeaSRRFCL(validFrom, validTo);
            }

            if ((BizType == 2) & ReferType == "SRR" & CargoType == 2)
            {
                fnSeaSRRLCL(validFrom, validTo);
            }

            if ((BizType == 2) & ReferType == "QTN" & CargoType == 1)
            {
                fnQtnSeaFCL(validFrom, validTo);
            }

            if ((BizType == 2) & ReferType == "QTN" & CargoType == 2)
            {
                fnQtnSeaLCL(validFrom, validTo);
            }

            if ((BizType == 2) & ReferType == "QTN" & CargoType == 4)
            {
                fnQtnSeaBBC(validFrom, validTo);
            }

            if (BizType == 2 & ReferType == "BOTH" & CargoType == 1)
            {
                fnSeaSRRAndQtnFCL(validFrom, validTo);
            }

            if (BizType == 2 & ReferType == "BOTH" & CargoType == 2)
            {
                fnSeaSRRAndQtnLCL(validFrom, validTo);
            }

            if ((BizType == 1) & ReferType == "SRR")
            {
                fnSRRAir(validFrom, validTo);
            }

            if ((BizType == 1) & ReferType == "QTN")
            {
                fnQtnAir(validFrom, validTo);
            }

            if (BizType == 1 & ReferType == "BOTH")
            {
                fnAirSRRAndQtn(validFrom, validTo);
            }

            if (BizType == 0 & ReferType == "SRR")
            {
                frmAirSeaSRR(validFrom, validTo);
            }
            if (BizType == 0 & ReferType == "QTN")
            {
                frmAirSeaQua(validFrom, validTo);
            }
            if (BizType == 0 & ReferType == "BOTH")
            {
                frmAirSeaBoth(validFrom, validTo);
            }

            try
            {
                return ObjWk.GetDataSet(sbNew.ToString());
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

        #endregion "Fetch Rating History"

        #region "fnSeaSRRFCL()"

        public void fnSeaSRRFCL(string validFrom, string validTo)
        {
            try
            {
                int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("              SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.CARGO_TYPE = 1");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SST.SRR_DATE and SST.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SST.SRR_DATE and SST.valid_to or (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("    LMT.LOCATION_NAME,");
                sb.Append("    POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnSeaSRRFCL()"

        #region "fnSeaSRRLCL()"

        public void fnSeaSRRLCL(string validFrom, string validTo)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.CARGO_TYPE = 2");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SST.SRR_DATE and SST.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SST.SRR_DATE and SST.valid_to or (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnSeaSRRLCL()"

        #region "fnQtnSeaFCL()"

        public void fnQtnSeaFCL(string validFrom, string validTo)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_DTL_TBL           QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN  QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND  QTNS.CARGO_TYPE=1");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or  to_date('" + validTo + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date +  qtns.VALID_FOR <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnQtnSeaFCL()"

        #region "fnQtnSeaLCL()"

        public void fnQtnSeaLCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_DTL_TBL           QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN      QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND  QTNS.CARGO_TYPE=2");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or  to_date('" + validTo + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date +  qtns.VALID_FOR <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnQtnSeaLCL()"

        #region "fnQtnSeaBBC()"

        public void fnQtnSeaBBC(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_DTL_TBL           QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN      QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND QTNS.CARGO_TYPE=4");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or  to_date('" + validTo + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date +  qtns.VALID_FOR <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");
                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnQtnSeaBBC()"

        #region "fnSeaSRRAndQtnFCL()"

        public void fnSeaSRRAndQtnFCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("               ");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.CARGO_TYPE=1");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SST.SRR_DATE and SST.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SST.SRR_DATE and SST.valid_to or (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_DTL_TBL         QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN      QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND QTNS.CARGO_TYPE=1");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or  to_date('" + validTo + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date +  qtns.VALID_FOR <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnSeaSRRAndQtnFCL()"

        #region "fnSeaSRRAndQtnLCL()"

        public void fnSeaSRRAndQtnLCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("               ");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.CARGO_TYPE=2");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SST.SRR_DATE and SST.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SST.SRR_DATE and SST.valid_to or (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_DTL_TBL  QTRN,");
                sb.Append("       QUOTATION_MST_TBL  QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND QTNS.CARGO_TYPE=2");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or  to_date('" + validTo + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date +  qtns.VALID_FOR <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnSeaSRRAndQtnLCL()"

        #region "fnSRRAir()"

        public void fnSRRAir(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SRRAIR.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                SUM(NVL(SAFT.MIN_AMOUNT, 0)) RATE,");
                sb.Append("                SRRAIR.SRR_DATE AS \"Issued\",");
                sb.Append("                SRRAIR.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_AIR_TBL        SRRTRN,");
                sb.Append("       SRR_AIR_TBL            SRRAIR,");
                sb.Append("       SRR_AIR_FREIGHT_TBL   SAFT,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("         CURRENCY_TYPE_MST_TBL  CURR");
                sb.Append(" WHERE SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("   AND SAFT.SRR_TRN_AIR_FK = SRRTRN.SRR_TRN_AIR_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SRRAIR.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   AND SAFT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SRRAIR.SRR_DATE and SRRAIR.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SRRAIR.SRR_DATE and SRRAIR.valid_to or (SRRAIR.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SRRAIR.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SRRAIR.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("      SRRAIR.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("    SAFT.MIN_AMOUNT,");
                sb.Append("     SRRAIR.SRR_DATE,");
                sb.Append("    SRRAIR.VALID_TO");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnSRRAir()"

        #region "fnQtnAir()"

        public void fnQtnAir(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO AS \"Ref No\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(FTRAN.QUOTED_RATE * get_ex_rate(FTRAN.CURRENCY_MST_FK,MAIN1.base_currency_fk,MAIN1.quotation_date), 0))) RATE,");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_MST_TBL          MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL          TRAN,");
                sb.Append("       QUOTATION_FREIGHT_TRN FTRAN,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CURR");
                sb.Append(" WHERE TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND FTRAN.QUOTATION_DTL_FK = MAIN1.QUOTE_DTL_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = MAIN1.CUSTOMER_MST_FK");
                sb.Append("   AND MAIN1.base_currency_fk = CURR.CURRENCY_MST_PK");
                sb.Append("   AND (MAIN1.STATUS = 2 OR MAIN1.STATUS=4)");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between MAIN1.quotation_date and main1.quotation_date + main1.valid_for or  to_date('" + validTo + "','DD/MM/YYYY') between MAIN1.quotation_date and main1.quotation_date + main1.valid_for or (MAIN1.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and main1.quotation_date + main1.valid_for <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND MAIN1.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND MAIN1.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( MAIN1.quotation_date +  MAIN1.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     MAIN1.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("     MAIN1.QUOTATION_DATE,");
                sb.Append("     MAIN1.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnQtnAir()"

        #region "fnAirSRRAndQtn()"

        public void fnAirSRRAndQtn(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SRRAIR.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                SUM(NVL(SAFT.MIN_AMOUNT, 0)) RATE,");
                sb.Append("                SRRAIR.SRR_DATE AS \"Issued\",");
                sb.Append("                SRRAIR.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_AIR_TBL        SRRTRN,");
                sb.Append("       SRR_AIR_TBL            SRRAIR,");
                sb.Append("       SRR_AIR_FREIGHT_TBL   SAFT,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("         CURRENCY_TYPE_MST_TBL  CURR");
                sb.Append(" WHERE SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("   AND SAFT.SRR_TRN_AIR_FK = SRRTRN.SRR_TRN_AIR_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SRRAIR.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   AND SAFT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SRRAIR.SRR_DATE and SRRAIR.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SRRAIR.SRR_DATE and SRRAIR.valid_to or (SRRAIR.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SRRAIR.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SRRAIR.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("      SRRAIR.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("    SAFT.MIN_AMOUNT,");
                sb.Append("     SRRAIR.SRR_DATE,");
                sb.Append("    SRRAIR.VALID_TO");

                sb.Append(" union ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO AS \"Ref No\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(FTRAN.QUOTED_RATE * get_ex_rate(FTRAN.CURRENCY_MST_FK,MAIN1.base_currency_fk,MAIN1.quotation_date), 0))) RATE,");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_MST_TBL          MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL          TRAN,");
                sb.Append("       QUOTATION_FREIGHT_TRN      FTRAN,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CURR");
                sb.Append(" WHERE TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND FTRAN.QUOTATION_DTL_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = MAIN1.CUSTOMER_MST_FK");
                sb.Append("   AND MAIN1.base_currency_fk = CURR.CURRENCY_MST_PK");
                sb.Append("   AND (MAIN1.STATUS = 2 OR MAIN1.STATUS=4)");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between MAIN1.quotation_date and main1.quotation_date + main1.valid_for or  to_date('" + validTo + "','DD/MM/YYYY') between MAIN1.quotation_date and main1.quotation_date + main1.valid_for or (MAIN1.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and main1.quotation_date + main1.valid_for <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND MAIN1.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND MAIN1.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( MAIN1.quotation_date +  MAIN1.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("    GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     MAIN1.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("     MAIN1.QUOTATION_DATE,");
                sb.Append("     MAIN1.VALID_FOR");

                sb.Append("   order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnAirSRRAndQtn()"

        #region "frmAirSeaSRR"

        public void frmAirSeaSRR(string validFrom, string validTo, Int32 flag = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                string StrCondition2 = null;
                if (Convert.ToInt32(StrCondition2) == 0)
                {
                    if (flag == 0)
                    {
                        StrCondition2 = " AND 1=2";
                    }
                }

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SST.SRR_DATE and SST.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SST.SRR_DATE and SST.valid_to or (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SRRAIR.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"AOO\",");
                sb.Append("                POD.PORT_NAME AS \"AOD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                SUM(NVL(SAFT.MIN_AMOUNT, 0)) RATE,");
                sb.Append("                SRRAIR.SRR_DATE AS \"Issued\",");
                sb.Append("                SRRAIR.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_AIR_TBL        SRRTRN,");
                sb.Append("       SRR_AIR_TBL            SRRAIR,");
                sb.Append("       SRR_AIR_FREIGHT_TBL   SAFT,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("         CURRENCY_TYPE_MST_TBL  CURR");
                sb.Append(" WHERE SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("   AND SAFT.SRR_TRN_AIR_FK = SRRTRN.SRR_TRN_AIR_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SRRAIR.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   AND SAFT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SRRAIR.SRR_DATE and SRRAIR.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SRRAIR.SRR_DATE and SRRAIR.valid_to or (SRRAIR.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SRRAIR.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SRRAIR.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("      SRRAIR.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("    SAFT.MIN_AMOUNT,");
                sb.Append("     SRRAIR.SRR_DATE,");
                sb.Append("    SRRAIR.VALID_TO");

                sb.Append(" ORDER BY \"Issued\" DESC");

                sbNew = sb.ToString();
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

        #endregion "frmAirSeaSRR"

        #region "fnAirSeaQua"

        public void frmAirSeaQua(string validFrom, string validTo, Int32 flag = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                string StrCondition2 = null;
                if (Convert.ToInt32(StrCondition2) == 0)
                {
                    if (flag == 0)
                    {
                        StrCondition2 = " AND 1=2";
                    }
                }

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");

                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_DTL_TBL         QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN      QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or  to_date('" + validTo + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date +  qtns.VALID_FOR <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO AS \"Ref No\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"AOO\",");
                sb.Append("                POD.PORT_NAME AS \"AOD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(FTRAN.QUOTED_RATE * get_ex_rate(FTRAN.CURRENCY_MST_FK,MAIN1.base_currency_fk,MAIN1.quotation_date), 0))) RATE,");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_MST_TBL          MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL          TRAN,");
                sb.Append("       QUOTATION_FREIGHT_TRN      FTRAN,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CURR");
                sb.Append(" WHERE TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND FTRAN.QUOTATION_DTL_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = MAIN1.CUSTOMER_MST_FK");
                sb.Append("   AND MAIN1.base_currency_fk = CURR.CURRENCY_MST_PK");
                sb.Append("   AND (MAIN1.STATUS = 2 OR MAIN1.STATUS=4)");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between MAIN1.quotation_date and main1.quotation_date + main1.valid_for or  to_date('" + validTo + "','DD/MM/YYYY') between MAIN1.quotation_date and main1.quotation_date + main1.valid_for or (MAIN1.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and main1.quotation_date + main1.valid_for <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND MAIN1.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND MAIN1.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( MAIN1.quotation_date +  MAIN1.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     MAIN1.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("     MAIN1.QUOTATION_DATE,");
                sb.Append("     MAIN1.VALID_FOR");

                sb.Append("  order by \"Issued\" desc");
                sbNew = sb.ToString();
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

        #endregion "fnAirSeaQua"

        #region "fnAirSeaBoth"

        public void frmAirSeaBoth(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SST.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("               ");
                sb.Append("                 SUM(NVL(S.APPROVED_BOF_RATE, 0)) RATE,");
                sb.Append("                SST.SRR_DATE AS \"Issued\",");
                sb.Append("                SST.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_SEA_TBL        S,");
                sb.Append("       SRR_SEA_TBL            SST,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL C,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CTMT");
                sb.Append(" WHERE SST.SRR_SEA_PK = S.SRR_SEA_FK");
                sb.Append("   AND S.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND S.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND S.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SST.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SST.CUSTOMER_MST_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND S.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND SST.STATUS = 1");
                sb.Append("   AND SST.ACTIVE = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SST.SRR_DATE and SST.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SST.SRR_DATE and SST.valid_to or (SST.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SST.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SST.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SST.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SST.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     SST.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    S.APPROVED_BOF_RATE,");
                sb.Append("    SST.SRR_DATE,");
                sb.Append("    SST.VALID_TO");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                QTNS.QUOTATION_REF_NO AS \"REF NO\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"POL\",");
                sb.Append("                POD.PORT_NAME AS \"POD\",");
                sb.Append("                C.CONTAINER_TYPE_MST_ID AS \"Ctr. Type\",");
                sb.Append("                CTMT.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(QTSFD.QUOTED_RATE * GET_EX_RATE(QTSFD.CURRENCY_MST_FK,QTNS.BASE_CURRENCY_FK,QTNS.QUOTATION_DATE), 0))) RATE,");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(QTNS.QUOTATION_DATE + QTNS.VALID_FOR, 'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_DTL_TBL         QTRN,");
                sb.Append("       QUOTATION_MST_TBL          QTNS,");
                sb.Append("       QUOTATION_FREIGHT_TRN      QTSFD,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL     C,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CTMT");
                sb.Append(" WHERE QTRN.CONTAINER_TYPE_MST_FK = C.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND QTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND QTSFD.QUOTATION_DTL_FK = QTRN.QUOTE_DTL_PK");
                sb.Append("   AND QTNS.BASE_CURRENCY_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND QTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND QTNS.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = QTNS.CUSTOMER_MST_FK");
                sb.Append("   AND (QTNS.STATUS = 2 OR QTNS.STATUS=4)");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND QTRN.QUOTATION_MST_FK = QTNS.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or  to_date('" + validTo + "','DD/MM/YYYY') between qtns.quotation_date and qtns.quotation_date +  qtns.VALID_FOR or (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date +  qtns.VALID_FOR <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND qtns.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND qtns.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("    QTNS.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("   C.CONTAINER_TYPE_MST_ID,");
                sb.Append("    CTMT.CURRENCY_ID,");
                sb.Append("    QTNS.QUOTATION_DATE,");
                sb.Append("   QTNS.VALID_FOR");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                SRRAIR.SRR_REF_NO AS \"REF NO\",");
                sb.Append("                'Special Rate Request' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"AOO\",");
                sb.Append("                POD.PORT_NAME AS \"AOD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                SUM(NVL(SAFT.MIN_AMOUNT, 0)) RATE,");
                sb.Append("                SRRAIR.SRR_DATE AS \"Issued\",");
                sb.Append("                SRRAIR.VALID_TO AS \"Validity\"");
                sb.Append("  FROM SRR_TRN_AIR_TBL        SRRTRN,");
                sb.Append("       SRR_AIR_TBL            SRRAIR,");
                sb.Append("       SRR_AIR_FREIGHT_TBL   SAFT,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       USER_MST_TBL           USR,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("         CURRENCY_TYPE_MST_TBL  CURR");
                sb.Append(" WHERE SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("   AND SAFT.SRR_TRN_AIR_FK = SRRTRN.SRR_TRN_AIR_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND SRRTRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND SRRAIR.CREATED_BY_FK = USR.USER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   AND SAFT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between SRRAIR.SRR_DATE and SRRAIR.VALID_TO or  to_date('" + validTo + "','DD/MM/YYYY') between SRRAIR.SRR_DATE and SRRAIR.valid_to or (SRRAIR.SRR_DATE >= to_date('" + validFrom + "','DD/MM/YYYY') and SRRAIR.valid_to <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND SRRAIR.SRR_DATE  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date(SRRAIR.valid_to,'dd/mm/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append(StrCondition);
                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("      SRRAIR.SRR_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("    SAFT.MIN_AMOUNT,");
                sb.Append("     SRRAIR.SRR_DATE,");
                sb.Append("    SRRAIR.VALID_TO");

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_NAME AS \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO AS \"Ref No\",");
                sb.Append("                'Quotation' AS \"REF Type\",");
                sb.Append("                USR.USER_ID AS \"User ID\",");
                sb.Append("                LMT.LOCATION_NAME AS \"Location\",");
                sb.Append("                POL.PORT_NAME AS \"AOO\",");
                sb.Append("                POD.PORT_NAME AS \"AOD\",");
                sb.Append("                '' CONTTYPE,");
                sb.Append("                CURR.CURRENCY_ID AS \"Curr.\",");
                sb.Append("                (SUM(NVL(FTRAN.QUOTED_RATE * get_ex_rate(FTRAN.CURRENCY_MST_FK,MAIN1.base_currency_fk,MAIN1.quotation_date), 0))) RATE,");

                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Issued\",");
                sb.Append("                TO_DATE(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Validity\"");
                sb.Append("  FROM QUOTATION_MST_TBL          MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL          TRAN,");
                sb.Append("       QUOTATION_FREIGHT_TRN      FTRAN,");
                sb.Append("       PORT_MST_TBL               POL,");
                sb.Append("       PORT_MST_TBL               POD,");
                sb.Append("       USER_MST_TBL               USR,");
                sb.Append("       LOCATION_MST_TBL           LMT,");
                sb.Append("       CUSTOMER_MST_TBL           CMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL      CURR");
                sb.Append(" WHERE TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND FTRAN.QUOTATION_DTL_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK");
                sb.Append("   AND USR.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = MAIN1.CUSTOMER_MST_FK");
                sb.Append("   AND MAIN1.base_currency_fk = CURR.CURRENCY_MST_PK");
                sb.Append("   AND (MAIN1.STATUS = 2 OR MAIN1.STATUS=4)");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between MAIN1.quotation_date and main1.quotation_date + main1.valid_for or  to_date('" + validTo + "','DD/MM/YYYY') between MAIN1.quotation_date and main1.quotation_date + main1.valid_for or (MAIN1.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and main1.quotation_date + main1.valid_for <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND MAIN1.quotation_date  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND MAIN1.quotation_date  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( MAIN1.quotation_date +  MAIN1.VALID_FOR, 'dd/MM/yyyy') <= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);

                sb.Append("   GROUP BY CMT.CUSTOMER_NAME,");
                sb.Append("     MAIN1.QUOTATION_REF_NO,");
                sb.Append("    USR.USER_ID,");
                sb.Append("   LMT.LOCATION_NAME,");
                sb.Append("   POL.PORT_NAME,");
                sb.Append("    POD.PORT_NAME,");
                sb.Append("    CURR.CURRENCY_ID,");
                sb.Append("     MAIN1.QUOTATION_DATE,");
                sb.Append("    MAIN1.VALID_FOR");
                sb.Append("  order by \"Issued\" desc");

                sbNew = sb.ToString();
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

        #endregion "fnAirSeaBoth"

        #region "Fetch Job Card Details (Air)"

        public void Fetch_Job_Card(long JobCardPk, int BizType, DataSet objDS, int process = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                if (BizType == 1)
                {
                    if (process == 1)
                    {
                        strQuery.Append(" SELECT DISTINCT D.PACK_TYPE_MST_PK,D.PACK_TYPE_DESC,SUM(JCT.PACK_COUNT)PACK_COUNT,W.MAWB_REF_NO FROM JOB_TRN_CONT JCT , PACK_TYPE_MST_TBL D , JOB_CARD_TRN K , MAWB_EXP_TBL W");
                        strQuery.Append(" WHERE JCT.PACK_TYPE_MST_FK = D.PACK_TYPE_MST_PK ");
                        strQuery.Append(" AND K.MBL_MAWB_FK=W.MAWB_EXP_TBL_PK(+)");
                        strQuery.Append(" AND  JCT.JOB_CARD_TRN_FK=K.JOB_CARD_TRN_PK");
                        strQuery.Append(" AND  JCT.JOB_CARD_TRN_FK=" + JobCardPk);
                        strQuery.Append("    GROUP BY D.PACK_TYPE_MST_PK, D.PACK_TYPE_DESC,W.MAWB_REF_NO  ");
                        strQuery.Append("");
                    }
                    else
                    {
                        strQuery.Append(" SELECT DISTINCT D.PACK_TYPE_MST_PK, D.PACK_TYPE_DESC, SUM(JCT.PACK_COUNT)PACK_COUNT,K.MBL_MAWB_REF_NO MAWB_REF_NO");
                        strQuery.Append("   FROM JOB_TRN_CONT JCT,");
                        strQuery.Append("        PACK_TYPE_MST_TBL    D,");
                        strQuery.Append("        JOB_CARD_TRN K");
                        strQuery.Append("  WHERE JCT.PACK_TYPE_MST_FK = D.PACK_TYPE_MST_PK");
                        strQuery.Append("    AND JCT.JOB_CARD_TRN_FK = K.JOB_CARD_TRN_PK");
                        strQuery.Append("    AND JCT.JOB_CARD_TRN_FK= " + JobCardPk);
                        strQuery.Append("    GROUP BY D.PACK_TYPE_MST_PK, D.PACK_TYPE_DESC,K.MBL_MAWB_REF_NO ");
                        strQuery.Append("");
                    }
                }
                else
                {
                    if (process == 1)
                    {
                        strQuery.Append(" SELECT  BST.CARGO_TYPE,");
                        strQuery.Append("         JSE.GOODS_DESCRIPTION,");
                        strQuery.Append("         SUM(JCT.GROSS_WEIGHT)as GROSSW,");
                        strQuery.Append("         SUM(JCT.NET_WEIGHT)As NETW,");
                        strQuery.Append("         SUM(JCT.VOLUME_IN_CBM)as VOLINCBM ");
                        strQuery.Append("    FROM JOB_TRN_CONT JCT,");
                        strQuery.Append("         JOB_CARD_TRN JSE,");
                        strQuery.Append("         BOOKING_MST_TBL      BST");
                        strQuery.Append("   WHERE BST.BOOKING_MST_PK = JSE.BOOKING_MST_FK");
                        strQuery.Append("     AND JCT.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK");
                        strQuery.Append("     AND JSE.JOB_CARD_TRN_PK = " + JobCardPk);
                        strQuery.Append("   GROUP BY BST.CARGO_TYPE,");
                        strQuery.Append("            BST.COL_ADDRESS,");
                        strQuery.Append("            BST.DEL_ADDRESS,");
                        strQuery.Append("            JSE.GOODS_DESCRIPTION ");
                        strQuery.Append("");
                    }
                    else
                    {
                        strQuery.Append("            SELECT  jse.cargo_type,");
                        strQuery.Append("                             JSE.GOODS_DESCRIPTION,");
                        strQuery.Append("                             SUM(JCT.GROSS_WEIGHT)as GROSSW,");
                        strQuery.Append("                             SUM(JCT.NET_WEIGHT)As NETW,");
                        strQuery.Append("                             SUM(JCT.VOLUME_IN_CBM)as VOLINCBM ");
                        strQuery.Append("                        FROM JOB_TRN_CONT JCT,");
                        strQuery.Append("                             JOB_CARD_TRN JSE");
                        strQuery.Append("                       WHERE  JCT.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK");
                        strQuery.Append("                         AND JSE.JOB_CARD_TRN_PK =  " + JobCardPk);
                        strQuery.Append("                       GROUP BY JSE.GOODS_DESCRIPTION ,");
                        strQuery.Append("                              jse.cargo_type,");
                        strQuery.Append("                             JSE.GOODS_DESCRIPTION,");
                        strQuery.Append("                             JCT.GROSS_WEIGHT,");
                        strQuery.Append("                             JCT.NET_WEIGHT");
                    }
                }
                objDS.Tables.Add(ObjWk.GetDataTable(strQuery.ToString()));

                ObjWk.MyCommand = new OracleCommand();

                var _with2 = ObjWk.MyCommand.Parameters;
                _with2.Add("JOBCARD_PK", JobCardPk).Direction = ParameterDirection.Input;
                _with2.Add("TRAN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                if (BizType == 1)
                {
                    if (process == 1)
                    {
                        objDS.Tables.Add(ObjWk.GetDataTable("FETCH_TRANSPORTER_NOTE", "AIR_JOB_CARD_FETCH"));
                    }
                    else
                    {
                        objDS.Tables.Add(ObjWk.GetDataTable("FETCH_TRANSPORTER_NOTE", "AIR_JOB_CARD_FETCH_IMP"));
                    }
                }
                else
                {
                    if (process == 1)
                    {
                        objDS.Tables.Add(ObjWk.GetDataTable("FETCH_TRANSPORTER_NOTE", "SEA_JOB_CARD_FETCH"));
                    }
                    else
                    {
                        objDS.Tables.Add(ObjWk.GetDataTable("FETCH_TRANSPORTER_NOTE", "SEA_JOB_CARD_FETCH_IMP"));
                    }
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

        #endregion "Fetch Job Card Details (Air)"

        #region "Check For Job Card In Transport Air Note"

        public int Fetch_Transport_Note(long JobCardPk, int process)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            DataSet objDsTrans = new DataSet();
            int str = 0;

            try
            {
                strSql = "select * from transport_inst_air_tb vb ";
                strSql += "where vb.job_card_fk=" + JobCardPk;
                strSql += "and vb.process_type=" + process;
                objDsTrans.Tables.Add(ObjWk.GetDataTable(strSql));

                if (objDsTrans.Tables[0].Rows.Count == 0)
                {
                    str = 1;
                    return str;
                }
                else
                {
                    str = 0;
                    return str;
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

        #endregion "Check For Job Card In Transport Air Note"

        #region "If Check Is True Then Fetch Transport Note"

        public DataSet Fetch_Transport_Note_Check_True(long JobCardPk, int process)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                if (process == 1)
                {
                    strQuery.Append("SELECT VB.JOB_CARD_FK,");
                    strQuery.Append("       VB.TRANSPORT_INST_AIR_PK,");
                    strQuery.Append("       VB.TRANS_INS_REF_NO,");
                    strQuery.Append("       VB.TRANS_INST_DATE,");
                    strQuery.Append("       VB.TRANSPORTER_MST_FK,");
                    strQuery.Append("       VB.COL_REF_NO,");
                    strQuery.Append("       VB.COL_ADDRESS,");
                    strQuery.Append("       VB.DEL_REF_NO,");
                    strQuery.Append("       VB.DEL_ADDRESS,");
                    strQuery.Append("       VB.GOODS_DESCRIPTION,");
                    strQuery.Append("       VB.SPECIAL_INSTRUCTIONS,");
                    strQuery.Append("       PTMT.PACK_TYPE_DESC,");
                    strQuery.Append("       VB.PACK_COUNT,");
                    strQuery.Append("       VB.PACK_TYPE_MST_FK,");
                    strQuery.Append("       VB.GROSS_WEIGHT,");
                    strQuery.Append("       VB.CHRG_WEIGHT,");
                    strQuery.Append("       VB.TRANSPORTER_ZONES_FK,");
                    strQuery.Append("       TZT.ZONE_NAME,");
                    strQuery.Append("       VB.TRANS_COST,");
                    strQuery.Append("       VB.CURRENCY_MST_FK,");
                    strQuery.Append("       CURR.CURRENCY_ID,");
                    strQuery.Append("       VB.VERSION_NO,");
                    strQuery.Append("       VB.VOLUME,");
                    strQuery.Append("       HBL.HAWB_REF_NO,");
                    strQuery.Append("       BAT.BOOKING_REF_NO,");
                    strQuery.Append("       MET.MAWB_REF_NO,");
                    strQuery.Append("       JCT.JOBCARD_REF_NO,");
                    strQuery.Append("       VMT.VENDOR_ID,");
                    strQuery.Append("       VMT.VENDOR_NAME,");
                    strQuery.Append("       CMR.TRPT_CMR_PRINT_PK,");
                    strQuery.Append("       JCT.SHIPPER_CUST_MST_FK,");
                    strQuery.Append("       JCT.CONSIGNEE_CUST_MST_FK,");
                    strQuery.Append("       NVL(VB.TRANS_MODE,1) TRANS_MODE");
                    strQuery.Append("  FROM TRANSPORT_INST_AIR_TB VB,");
                    strQuery.Append("       HAWB_EXP_TBL          HBL,");
                    strQuery.Append("       BOOKING_MST_TBL       BAT,");
                    strQuery.Append("       JOB_CARD_TRN  JCT,");
                    strQuery.Append("       MAWB_EXP_TBL          MET,");
                    strQuery.Append("       VENDOR_MST_TBL        VMT,");
                    strQuery.Append("       PACK_TYPE_MST_TBL     PTMT,");
                    strQuery.Append("       TRANSPORTER_ZONES_TBL TZT,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL CURR,TRPT_CMR_PRINT_TBL     CMR");
                    strQuery.Append("  WHERE VB.JOB_CARD_FK = JCT.JOB_CARD_TRN_PK(+)");
                    strQuery.Append("   AND JCT.BOOKING_MST_FK = BAT.BOOKING_MST_PK");
                    strQuery.Append("   AND JCT.HBL_HAWB_FK = MET.MAWB_EXP_TBL_PK(+)");
                    strQuery.Append("   AND VB.JOB_CARD_FK = HBL.JOB_CARD_AIR_EXP_FK(+)");
                    strQuery.Append("   AND VB.TRANSPORTER_MST_FK = VMT.VENDOR_MST_PK(+)");
                    strQuery.Append("   AND VB.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
                    strQuery.Append("   AND VB.TRANSPORTER_ZONES_FK = TZT.TRANSPORTER_ZONES_PK(+)");
                    strQuery.Append("   AND VB.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                    strQuery.Append("   AND CMR.TRANSPORT_NOTE_FK(+) = VB.TRANSPORT_INST_AIR_PK");
                    strQuery.Append("   AND VB.PROCESS_TYPE=" + process);
                    strQuery.Append("   AND VB.JOB_CARD_FK = " + JobCardPk);
                    strQuery.Append("");
                }
                else
                {
                    strQuery.Append("SELECT VB.JOB_CARD_FK,");
                    strQuery.Append("       VB.TRANSPORT_INST_AIR_PK,");
                    strQuery.Append("       VB.TRANS_INS_REF_NO,");
                    strQuery.Append("       VB.TRANS_INST_DATE,");
                    strQuery.Append("       VB.TRANSPORTER_MST_FK,");
                    strQuery.Append("       VB.COL_REF_NO,");
                    strQuery.Append("       VB.COL_ADDRESS,");
                    strQuery.Append("       VB.DEL_REF_NO,");
                    strQuery.Append("       VB.DEL_ADDRESS,");
                    strQuery.Append("       VB.GOODS_DESCRIPTION,");
                    strQuery.Append("       VB.SPECIAL_INSTRUCTIONS,");
                    strQuery.Append("       PTMT.PACK_TYPE_DESC,");
                    strQuery.Append("       VB.PACK_COUNT,");
                    strQuery.Append("       VB.PACK_TYPE_MST_FK,");
                    strQuery.Append("       VB.GROSS_WEIGHT,");
                    strQuery.Append("       VB.CHRG_WEIGHT,");
                    strQuery.Append("       VB.TRANSPORTER_ZONES_FK,");
                    strQuery.Append("       TZT.ZONE_NAME,");
                    strQuery.Append("       VB.TRANS_COST,");
                    strQuery.Append("       VB.CURRENCY_MST_FK,");
                    strQuery.Append("       CURR.CURRENCY_ID,");
                    strQuery.Append("       VB.VERSION_NO,");
                    strQuery.Append("       VB.VOLUME,");
                    strQuery.Append("       JCT.HBL_HAWB_REF_NO HAWB_REF_NO,");
                    strQuery.Append("       JCT.MBL_MAWB_REF_NO MAWB_REF_NO,");
                    strQuery.Append("       JCT.JOBCARD_REF_NO,");
                    strQuery.Append("       VMT.VENDOR_ID,");
                    strQuery.Append("       VMT.VENDOR_NAME,");
                    strQuery.Append("       CMR.TRPT_CMR_PRINT_PK,");
                    strQuery.Append("       JCT.SHIPPER_CUST_MST_FK,");
                    strQuery.Append("       JCT.CONSIGNEE_CUST_MST_FK,");
                    strQuery.Append("       NVL(VB.TRANS_MODE,1) TRANS_MODE");
                    strQuery.Append("  FROM TRANSPORT_INST_AIR_TB VB,");
                    strQuery.Append("       HAWB_EXP_TBL          HBL,");
                    strQuery.Append("       JOB_CARD_TRN  JCT,");
                    strQuery.Append("       MAWB_EXP_TBL          MET,");
                    strQuery.Append("       VENDOR_MST_TBL        VMT,");
                    strQuery.Append("       PACK_TYPE_MST_TBL     PTMT,");
                    strQuery.Append("       TRANSPORTER_ZONES_TBL TZT,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL CURR,TRPT_CMR_PRINT_TBL     CMR");
                    strQuery.Append("  WHERE VB.JOB_CARD_FK = JCT.JOB_CARD_TRN_PK(+)");
                    strQuery.Append("    AND JCT.MBL_MAWB_REF_NO = MET.MAWB_REF_NO(+)");
                    strQuery.Append("   AND VB.JOB_CARD_FK = HBL.JOB_CARD_AIR_EXP_FK(+)");
                    strQuery.Append("   AND VB.TRANSPORTER_MST_FK = VMT.VENDOR_MST_PK(+)");
                    strQuery.Append("   AND VB.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
                    strQuery.Append("   AND VB.TRANSPORTER_ZONES_FK = TZT.TRANSPORTER_ZONES_PK(+)");
                    strQuery.Append("   AND VB.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                    strQuery.Append("   AND CMR.TRANSPORT_NOTE_FK(+) = VB.TRANSPORT_INST_AIR_PK");
                    strQuery.Append("   AND VB.PROCESS_TYPE=" + process);
                    strQuery.Append("   AND VB.JOB_CARD_FK = " + JobCardPk);
                    strQuery.Append("");
                }
                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "If Check Is True Then Fetch Transport Note"

        #region "Check For Job Card In Transport Sea Note"

        public int Fetch_Transport_Sea_Note(long JobCardPk, long process)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            DataSet objDsTrans = new DataSet();
            int str = 0;

            try
            {
                strSql = "select * from TRANSPORT_INST_SEA_TBL ";
                strSql += "where job_card_fk='" + JobCardPk + "'";
                strSql += "and process_type=" + process;
                objDsTrans.Tables.Add(ObjWk.GetDataTable(strSql));

                if (objDsTrans.Tables[0].Rows.Count == 0)
                {
                    str = 1;
                    return str;
                }
                else
                {
                    str = 0;
                    return str;
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

        #endregion "Check For Job Card In Transport Sea Note"

        #region "Fetch HBL/HAWB Details"

        public void Fetch_HBL_HAWB(long HBL_HAWB_Pk, int BizType, DataSet objDS)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            try
            {
                if (BizType == 1)
                {
                    strSql = " SELECT D.PACK_TYPE_DESC,JCT.PACK_COUNT,W.MAWB_REF_NO ,K.JOB_CARD_TRN_PK FROM JOB_TRN_CONT JCT , PACK_TYPE_MST_TBL D ,JOB_CARD_TRN K ,MAWB_EXP_TBL W,HAWB_EXP_TBL HH ";
                    strSql += " WHERE JCT.PACK_TYPE_MST_FK = D.PACK_TYPE_MST_PK ";
                    strSql += " AND K.MBL_MAWB_FK=W.MAWB_EXP_TBL_PK";
                    strSql += " AND  JCT.JOB_CARD_TRN_FK=K.JOB_CARD_TRN_PK";
                    strSql += " AND HH.JOB_CARD_AIR_EXP_FK=JCT.JOB_CARD_TRN_FK ";
                    strSql += " AND HH.HAWB_EXP_TBL_PK=" + HBL_HAWB_Pk;
                }
                else
                {
                    strSql = "      SELECT JC.CONTAINER_NUMBER   FROM JOB_TRN_CONT JC,HBL_EXP_TBL H";
                    strSql += "    WHERE H.HBL_EXP_TBL_PK=" + HBL_HAWB_Pk;
                    strSql += "    AND  H.JOB_CARD_SEA_EXP_FK=JC.JOB_CARD_TRN_FK ";
                }
                objDS.Tables.Add(ObjWk.GetDataTable(strSql));

                ObjWk.MyCommand = new OracleCommand();

                var _with3 = ObjWk.MyCommand.Parameters;
                _with3.Add("HAWB_HBL_PK", HBL_HAWB_Pk).Direction = ParameterDirection.Input;
                _with3.Add("TRAN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                if (BizType == 1)
                {
                    objDS.Tables.Add(ObjWk.GetDataTable("FETCH_TRANSPORTER_NOTE", "HAWB_FETCH"));
                }
                else
                {
                    objDS.Tables.Add(ObjWk.GetDataTable("FETCH_TRANSPORTER_NOTE", "HBL_FETCH"));
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

        #endregion "Fetch HBL/HAWB Details"

        #region "If Check Is True Then Fetch Transport Sea Note EXP"

        public int Fetch_mode(long JobCardPk, long process)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            DataSet objDsTrans = new DataSet();

            try
            {
                strSql = " select TI.TP_MODE from TRANSPORT_INST_SEA_TBL TI ";
                strSql += " where job_card_fk='" + JobCardPk + "'";
                strSql += " and process_type=" + process;
                return Convert.ToInt32(ObjWk.ExecuteScaler(strSql));
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

        public DataSet Fetch_Transport_Note_Sea_Check_True(long JobCardPk, long process, int Business_type = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                if (process == 1)
                {
                    strQuery.Append("  SELECT TIST.BUSINESS_TYPE,");
                    strQuery.Append("         TIST.TRANSPORT_INST_SEA_PK,");
                    strQuery.Append("         TIST.TRANSPORTER_MST_FK,");
                    strQuery.Append("         BST.BOOKING_REF_NO,");
                    strQuery.Append("         JCSE.JOBCARD_REF_NO,");
                    strQuery.Append("         TIST.JOB_CARD_FK,");
                    strQuery.Append("         TIST.CARGO_TYPE,");
                    strQuery.Append("         TIST.TRANS_INST_REF_NO,");
                    strQuery.Append("         TIST.TRANS_INST_DATE,");
                    strQuery.Append("         VMT.VENDOR_ID,");
                    strQuery.Append("         VMT.VENDOR_NAME,");
                    strQuery.Append("         TIST.MT_CTR_PICKUP_REF,");
                    strQuery.Append("         TIST.MT_CTR_PICKUP_ADDRESS,");
                    strQuery.Append("         TIST.MT_CTR_PICKUP_BY,");
                    strQuery.Append("         TIST.CARGO_PICKUP_REF_NO,");
                    strQuery.Append("         TIST.CARGO_PICKUP_ADDRESS,");
                    strQuery.Append("         TIST.CARGO_PICKUP_BY,");
                    strQuery.Append("         TIST.DELIVERY_REF_NO,");
                    strQuery.Append("         TIST.DELIVERY_OFF_ADDRESS,");
                    strQuery.Append("         TIST.DELIVERY_BY,");
                    strQuery.Append("         TIST.GOODS_DESCRIPTION,");
                    strQuery.Append("         TIST.SPECIAL_INSTRUCTIONS,");
                    strQuery.Append("         TIST.GROSS_WEIGHT,");
                    strQuery.Append("         TIST.NET_WEIGHT,");
                    strQuery.Append("         TIST.VOLUME,");
                    strQuery.Append("         TIST.VERSION_NO,");
                    strQuery.Append("         CMR.TRPT_CMR_PRINT_PK,");
                    strQuery.Append("       JCSE.SHIPPER_CUST_MST_FK,");
                    strQuery.Append("       JCSE.CONSIGNEE_CUST_MST_FK");
                    strQuery.Append("   FROM TRANSPORT_INST_SEA_TBL TIST, VENDOR_MST_TBL VMT, BOOKING_MST_TBL BST ");
                    //Snigdharani
                    strQuery.Append("   ,JOB_CARD_TRN JCSE,TRPT_CMR_PRINT_TBL     CMR");
                    strQuery.Append("   WHERE VMT.VENDOR_MST_PK = TIST.TRANSPORTER_MST_FK");
                    strQuery.Append("   AND BST.BOOKING_MST_PK = JCSE.BOOKING_MST_FK");
                    strQuery.Append("   AND TIST.JOB_CARD_FK =JCSE.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND CMR.TRANSPORT_NOTE_FK(+) = TIST.TRANSPORT_INST_SEA_PK");
                    strQuery.Append("   AND TIST.JOB_CARD_FK = " + JobCardPk);
                    strQuery.Append("   AND TIST.PROCESS_TYPE=" + process);
                    strQuery.Append("");
                }
                else if (Business_type == 3)
                {
                    strQuery.Append("  SELECT TIST.BUSINESS_TYPE,");
                    strQuery.Append("         TIST.TRANSPORT_INST_SEA_PK,");
                    strQuery.Append("         TIST.TRANSPORTER_MST_FK,");
                    strQuery.Append("         JCSE.JOBCARD_REF_NO,");
                    strQuery.Append("         TIST.JOB_CARD_FK,");
                    strQuery.Append("         TIST.CARGO_TYPE,");
                    strQuery.Append("         TIST.TRANS_INST_REF_NO,");
                    strQuery.Append("         TIST.TRANS_INST_DATE,");
                    strQuery.Append("         VMT.VENDOR_ID,");
                    strQuery.Append("         VMT.VENDOR_NAME,");
                    strQuery.Append("         TIST.MT_CTR_PICKUP_REF,");
                    strQuery.Append("         TIST.MT_CTR_PICKUP_ADDRESS,");
                    strQuery.Append("         TIST.MT_CTR_PICKUP_BY,");
                    strQuery.Append("         TIST.CARGO_PICKUP_REF_NO,");
                    strQuery.Append("         TIST.CARGO_PICKUP_ADDRESS,");
                    strQuery.Append("         TIST.CARGO_PICKUP_BY,");
                    strQuery.Append("         TIST.DELIVERY_REF_NO,");
                    strQuery.Append("         TIST.DELIVERY_OFF_ADDRESS,");
                    strQuery.Append("         TIST.DELIVERY_BY,");
                    strQuery.Append("         TIST.GOODS_DESCRIPTION,");
                    strQuery.Append("         TIST.SPECIAL_INSTRUCTIONS,");
                    strQuery.Append("         TIST.GROSS_WEIGHT,");
                    strQuery.Append("         TIST.NET_WEIGHT,");
                    strQuery.Append("         TIST.VOLUME,");
                    strQuery.Append("         TIST.trans_inst_date,");
                    strQuery.Append("         TIST.VERSION_NO,");
                    strQuery.Append("         CMR.TRPT_CMR_PRINT_PK,");
                    strQuery.Append("       JCSE.SHIPPER_CUST_MST_FK,");
                    strQuery.Append("       JCSE.CONSIGNEE_CUST_MST_FK");
                    strQuery.Append("      , POL.PORT_MST_PK POL_PK,POL.PORT_ID POL_ID,POL.PORT_NAME POL_NAME,  ");
                    strQuery.Append("      POD.PORT_MST_PK POD_PK,POD.PORT_ID POD_ID,POD.PORT_NAME POD_NAME,  ");
                    strQuery.Append("      TIST.TP_MODE,TIST.ETA,TIST.ETD,TIST.VSL_VOY_FK,V.VESSEL_NAME,VVT.VOYAGE,V.VESSEL_ID  ");
                    strQuery.Append("   FROM TRANSPORT_INST_SEA_TBL TIST, VENDOR_MST_TBL VMT,");
                    strQuery.Append("   JOB_CARD_TRN JCSE, TRPT_CMR_PRINT_TBL     CMR, PORT_MST_TBL POL,PORT_MST_TBL POD, OPERATOR_MST_TBL OPT,  VESSEL_VOYAGE_TBL V,VESSEL_VOYAGE_TRN VVT ");
                    strQuery.Append("   WHERE ");

                    strQuery.Append("   VMT.VENDOR_MST_PK = TIST.TRANSPORTER_MST_FK ");
                    strQuery.Append("   AND VMT.VENDOR_ID = OPT.OPERATOR_ID ");

                    strQuery.Append("   AND TIST.POL_FK = POL.PORT_MST_PK(+) ");
                    strQuery.Append("   AND TIST.POD_FK = POD.PORT_MST_PK(+) ");
                    strQuery.Append("   AND TIST.JOB_CARD_FK =JCSE.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND CMR.TRANSPORT_NOTE_FK(+) = TIST.TRANSPORT_INST_SEA_PK AND TIST.VSL_VOY_FK = VVT.VOYAGE_TRN_PK AND  VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK  ");
                    strQuery.Append("   AND TIST.JOB_CARD_FK = " + JobCardPk);
                    strQuery.Append("   AND TIST.PROCESS_TYPE=" + process);
                    strQuery.Append("");
                }
                else
                {
                    strQuery.Append("  SELECT TIST.BUSINESS_TYPE,");
                    strQuery.Append("         TIST.TRANSPORT_INST_SEA_PK,");
                    strQuery.Append("         TIST.TRANSPORTER_MST_FK,");
                    strQuery.Append("         JCSE.JOBCARD_REF_NO,");
                    strQuery.Append("         TIST.JOB_CARD_FK,");
                    strQuery.Append("         TIST.CARGO_TYPE,");
                    strQuery.Append("         TIST.TRANS_INST_REF_NO,");
                    strQuery.Append("         TIST.TRANS_INST_DATE,");
                    strQuery.Append("         VMT.VENDOR_ID,");
                    strQuery.Append("         VMT.VENDOR_NAME,");
                    strQuery.Append("         TIST.MT_CTR_PICKUP_REF,");
                    strQuery.Append("         TIST.MT_CTR_PICKUP_ADDRESS,");
                    strQuery.Append("         TIST.MT_CTR_PICKUP_BY,");
                    strQuery.Append("         TIST.CARGO_PICKUP_REF_NO,");
                    strQuery.Append("         TIST.CARGO_PICKUP_ADDRESS,");
                    strQuery.Append("         TIST.CARGO_PICKUP_BY,");
                    strQuery.Append("         TIST.DELIVERY_REF_NO,");
                    strQuery.Append("         TIST.DELIVERY_OFF_ADDRESS,");
                    strQuery.Append("         TIST.DELIVERY_BY,");
                    strQuery.Append("         TIST.GOODS_DESCRIPTION,");
                    strQuery.Append("         TIST.SPECIAL_INSTRUCTIONS,");
                    strQuery.Append("         TIST.GROSS_WEIGHT,");
                    strQuery.Append("         TIST.NET_WEIGHT,");
                    strQuery.Append("         TIST.VOLUME,");
                    strQuery.Append("         TIST.trans_inst_date,");
                    strQuery.Append("         TIST.VERSION_NO,");
                    strQuery.Append("         CMR.TRPT_CMR_PRINT_PK,");
                    strQuery.Append("       JCSE.SHIPPER_CUST_MST_FK,");
                    strQuery.Append("       JCSE.CONSIGNEE_CUST_MST_FK");
                    strQuery.Append("      , POL.PORT_MST_PK POL_PK,POL.PORT_ID POL_ID,POL.PORT_NAME POL_NAME,  ");
                    strQuery.Append("      POD.PORT_MST_PK POD_PK,POD.PORT_ID POD_ID,POD.PORT_NAME POD_NAME,  ");
                    strQuery.Append("      TIST.TP_MODE,''ETA,''ETD,null VSL_VOY_FK,'' VESSEL_NAME,'' VOYAGE,''VESSEL_ID  ");
                    strQuery.Append("   FROM TRANSPORT_INST_SEA_TBL TIST, VENDOR_MST_TBL VMT,");
                    strQuery.Append("   JOB_CARD_TRN JCSE, TRPT_CMR_PRINT_TBL     CMR, PORT_MST_TBL POL,PORT_MST_TBL POD ");
                    strQuery.Append("   WHERE VMT.VENDOR_MST_PK = TIST.TRANSPORTER_MST_FK");
                    strQuery.Append("   AND TIST.POL_FK = POL.PORT_MST_PK(+) ");
                    strQuery.Append("   AND TIST.POD_FK = POD.PORT_MST_PK(+) ");
                    strQuery.Append("   AND TIST.JOB_CARD_FK =JCSE.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND CMR.TRANSPORT_NOTE_FK(+) = TIST.TRANSPORT_INST_SEA_PK");
                    strQuery.Append("   AND TIST.JOB_CARD_FK = " + JobCardPk);
                    strQuery.Append("   AND TIST.PROCESS_TYPE=" + process);
                    strQuery.Append("");
                }
                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "If Check Is True Then Fetch Transport Sea Note EXP"

        #region "Fetch Booking Details"

        public void Fetch_Booking(long BookingPk, int BizType, DataSet objDS)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            try
            {
                if (BizType == 1)
                {
                    strSql = " SELECT D.PACK_TYPE_DESC,JCT.PACK_COUNT,W.MAWB_REF_NO ,K.JOB_CARD_TRN_PK FROM JOB_TRN_CONT JCT , PACK_TYPE_MST_TBL D , JOB_CARD_TRN K , MAWB_EXP_TBL W ,BOOKING_MST_TBL BAT";
                    strSql += " WHERE JCT.PACK_TYPE_MST_FK = D.PACK_TYPE_MST_PK ";
                    strSql += " AND K.MBL_MAWB_FK=W.MAWB_EXP_TBL_PK";
                    strSql += " AND JCT.JOB_CARD_TRN_FK=K.JOB_CARD_TRN_PK";
                    strSql += " AND BAT.BOOKING_MST_PK=K.BOOKING_MST_FK";
                    strSql += " AND BAT.BOOKING_MST_PK = " + BookingPk;
                }
                else
                {
                    strSql = "  select jse.job_card_TRN_pk from JOB_CARD_TRN JSE";
                    strSql += "  where jse.booking_MST_fk= " + BookingPk;
                }
                objDS.Tables.Add(ObjWk.GetDataTable(strSql));

                ObjWk.MyCommand = new OracleCommand();

                var _with4 = ObjWk.MyCommand.Parameters;
                _with4.Add("BOOKING_PK", BookingPk).Direction = ParameterDirection.Input;
                _with4.Add("TRAN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                if (BizType == 1)
                {
                    objDS.Tables.Add(ObjWk.GetDataTable("FETCH_TRANSPORTER_NOTE", "AIR_BOOKING_FETCH"));
                }
                else
                {
                    objDS.Tables.Add(ObjWk.GetDataTable("FETCH_TRANSPORTER_NOTE", " SEA_BOOKING_FETCH"));
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

        #endregion "Fetch Booking Details"

        #region "GET CROPK"

        public int GetCroPK(string JOBPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append(" SELECT BCR.CRO_PK");
            StrSqlBuilder.Append(" FROM BOOKING_MST_TBL BOOK, JOB_CARD_TRN JOB, BOOKING_CRO_TBL BCR");
            StrSqlBuilder.Append(" WHERE BOOK.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
            StrSqlBuilder.Append(" AND BOOK.BOOKING_MST_PK = BCR.BOOKING_MST_FK");
            StrSqlBuilder.Append("  AND JOB.JOB_CARD_TRN_PK = " + JOBPK);
            try
            {
                return Convert.ToInt32(ObjWk.ExecuteScaler(StrSqlBuilder.ToString()));
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

        #endregion "GET CROPK"

        #region "Fetch Location of User Login"

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
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Location of User Login"

        #region "FetchLogAddressDtl"

        public DataSet FetchLogAddressDtl(long LOC)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.CITY,");
            StrSqlBuilder.Append("  L.TELE_PHONE_NO,L.Fax_No,L.E_MAIL_ID ");
            StrSqlBuilder.Append("  from location_mst_tbl L");
            StrSqlBuilder.Append("  where L.LOCATION_MST_PK = " + LOC + "");

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

        #endregion "FetchLogAddressDtl"

        #region "Fetch Transporter Of User Login"

        public DataSet FetchTransporter(long Transporter)
        {
            WorkFlow ObjWk = new WorkFlow();
            string Strsql = null;
            Strsql = "   SELECT T.TRANSPORTER_MST_PK,T.TRANSPORTER_ID,T.TRANSPORTER_NAME,TD.ADM_ADDRESS_1,";
            Strsql += "  TD.ADM_ADDRESS_2, TD.ADM_ADDRESS_3, TD.ADM_CITY, TD.ADM_ZIP_CODE, CC.COUNTRY_NAME,";
            Strsql += "  TD.ADM_PHONE_NO_1,TD.ADM_FAX_NO,TD.ADM_EMAIL_ID";
            Strsql += "  FROM  TRANSPORTER_MST_TBL T,TRANSPORTER_CONTACT_DTLS TD,COUNTRY_MST_TBL CC";
            Strsql += "  WHERE CC.COUNTRY_MST_PK = TD.ADM_COUNTRY_MST_FK";
            Strsql += "  AND TD.TRANSPORTER_MST_FK=T.TRANSPORTER_MST_PK";
            Strsql += "  AND T.TRANSPORTER_MST_PK=" + Transporter;
            try
            {
                return ObjWk.GetDataSet(Strsql);
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

        #endregion "Fetch Transporter Of User Login"

        #region "Fetch Transporter Import Details"

        public DataSet FetchTransporterAirImp(long JOBPK, string Truck = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            string Strsql = null;
            Strsql = "SELECT JAI.JOB_CARD_TRN_PK JOBPK,";
            Strsql += " JAI.JOBCARD_REF_NO  JOBNO,";
            Strsql += " JAI.VOYAGE_FLIGHT_NO VSE_VOY,";
            Strsql += "(CASE WHEN JAI.HBL_HAWB_REF_NO IS NOT NULL THEN";
            Strsql += "  JAI.HBL_HAWB_REF_NO";
            Strsql += " ELSE";
            Strsql += " JAI.MBL_MAWB_REF_NO END ) BLREFNO,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME, ";
            Strsql += " PMST.PLACE_NAME DELNAME,";
            Strsql += " '' COLNAME,";
            Strsql += " TO_CHAR(JAI.ETD_DATE,'" + dateFormat + "')  ETD,";
            Strsql += " JAI.MARKS_NUMBERS MARKS,";
            Strsql += " JAI.GOODS_DESCRIPTION GOODS,";
            Strsql += " STMST.INCO_CODE TERMS,";
            Strsql += " JAI.PYMT_TYPE,";
            Strsql += " NVL(JAI.INSURANCE_AMT,0) INSURANCE,";
            Strsql += " CMST.COMMODITY_GROUP_DESC COMMCODE,";
            Strsql += " 2 CARGO_TYPE,";
            Strsql += "  TO_CHAR(JAI.ETA_DATE,'" + dateFormat + "') ETA,";
            Strsql += " '' WORKORDER_NR,";
            Strsql += " TTT.DD_DRIVER1_NAME DRIVER_NAME,";
            Strsql += " TTT.DD_CONTACT_NUMBER1 MOBILE_NR,";
            Strsql += " TTT.TD_TRUCK_NUMBER TRUCK_NR,";
            Strsql += " (SELECT VMT.VENDOR_NAME FROM VENDOR_MST_TBL VMT WHERE VMT.VENDOR_MST_PK = TTT.TRANSPORTER_MST_FK) TRANSPORTER_NAME";
            Strsql += " FROM TRANSPORT_INST_AIR_TB TIST, TRANSPORT_TRN_CONT CONT, TRANSPORT_TRN_TRUCK TTT, JOB_CARD_TRN JAI,";
            Strsql += " SHIPPING_TERMS_MST_TBL STMST,";
            Strsql += " PLACE_MST_TBL PMST,";
            Strsql += " PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,";
            Strsql += "COMMODITY_GROUP_MST_TBL CMST";
            Strsql += " WHERE TIST.JOB_CARD_FK IN (JAI.JOB_CARD_TRN_PK(+))";
            Strsql += " AND TIST.TRANSPORT_INST_AIR_PK = CONT.TRANSPORT_INST_FK";
            Strsql += " AND CONT.TRANSPORT_TRN_CONT_PK = TTT.TRANSPORT_TRN_CONT_FK";
            Strsql += " AND JAI.SHIPPING_TERMS_MST_FK=STMST.SHIPPING_TERMS_MST_PK(+)";
            Strsql += " AND  PMST.PLACE_PK(+)=JAI.DEL_PLACE_MST_FK";
            Strsql += " AND  POL.PORT_MST_PK(+) = JAI.PORT_MST_POL_FK";
            Strsql += " AND  POD.PORT_MST_PK(+) = JAI.PORT_MST_POD_FK";
            Strsql += " AND JAI.COMMODITY_GROUP_FK=CMST.COMMODITY_GROUP_PK(+)";
            Strsql += " AND  TIST.TRANSPORT_INST_AIR_PK = " + JOBPK;
            Strsql += " AND TTT.TD_TRUCK_NUMBER= '" + Truck + "' ";

            try
            {
                return ObjWk.GetDataSet(Strsql);
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

        public DataSet FetchTransporterSeaImp(long JOBPK, string Truck = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            string Strsql = null;
            Strsql = "SELECT JSI.JOB_CARD_TRN_PK JOBPK,";
            Strsql += " JSI.JOBCARD_REF_NO  JOBNO,";
            Strsql += " (CASE WHEN JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
            Strsql += " JSI.VESSEL_NAME || '-' || JSI.VOYAGE_FLIGHT_NO";
            Strsql += " ELSE";
            Strsql += " JSI.VESSEL_NAME END ) VSE_VOY,";
            Strsql += "(CASE WHEN JSI.HBL_HAWB_REF_NO IS NOT NULL THEN";
            Strsql += "  JSI.HBL_HAWB_REF_NO";
            Strsql += " ELSE";
            Strsql += " JSI.MBL_MAWB_REF_NO END ) BLREFNO,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " CASE WHEN TIST.POD_FK = NULL THEN POD.PORT_NAME ELSE TP_POD.PORT_NAME END PODNAME, ";
            //Modified by Faheem
            Strsql += " PMST.PLACE_NAME DELNAME,";
            Strsql += " '' COLNAME,";
            Strsql += " TO_CHAR(JSI.ETD_DATE,'" + dateFormat + "')  ETD,";
            Strsql += " JSI.MARKS_NUMBERS MARKS,";
            Strsql += " JSI.GOODS_DESCRIPTION GOODS,";
            Strsql += " STMST.INCO_CODE TERMS,";
            Strsql += " JSI.PYMT_TYPE,";
            Strsql += " NVL(JSI.INSURANCE_AMT,0) INSURANCE,";
            Strsql += " CMST.COMMODITY_GROUP_DESC COMMCODE,";
            Strsql += " JSI.CARGO_TYPE CARGO_TYPE,";
            Strsql += " TO_CHAR(JSI.ETA_DATE,'" + dateFormat + "') ETA,";
            Strsql += " '' WORKORDER_NR,";
            Strsql += " TTT.DD_DRIVER1_NAME DRIVER_NAME,";
            Strsql += " TTT.DD_CONTACT_NUMBER1 MOBILE_NR,";
            Strsql += " TTT.TD_TRUCK_NUMBER TRUCK_NR,";
            Strsql += " (SELECT VMT.VENDOR_NAME FROM VENDOR_MST_TBL VMT WHERE VMT.VENDOR_MST_PK = TTT.TRANSPORTER_MST_FK) TRANSPORTER_NAME";
            Strsql += " FROM TRANSPORT_INST_SEA_TBL  TIST, TRANSPORT_TRN_CONT CONT, TRANSPORT_TRN_TRUCK TTT,JOB_CARD_TRN JSI,";
            Strsql += " SHIPPING_TERMS_MST_TBL STMST,";
            Strsql += " PLACE_MST_TBL PMST,";
            Strsql += " PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,";
            Strsql += "COMMODITY_GROUP_MST_TBL CMST";
            Strsql += " ,PORT_MST_TBL TP_POD ";
            Strsql += " WHERE TIST.JOB_CARD_FK IN (JSI.JOB_CARD_TRN_PK(+)) ";
            Strsql += " AND TIST.TRANSPORT_INST_SEA_PK = CONT.TRANSPORT_INST_FK";
            Strsql += " AND CONT.TRANSPORT_TRN_CONT_PK = TTT.TRANSPORT_TRN_CONT_FK";
            Strsql += " AND JSI.SHIPPING_TERMS_MST_FK=STMST.SHIPPING_TERMS_MST_PK(+)";
            Strsql += " AND  PMST.PLACE_PK(+)=JSI.DEL_PLACE_MST_FK";
            Strsql += " AND  POL.PORT_MST_PK(+) = JSI.PORT_MST_POL_FK";
            Strsql += " AND  POD.PORT_MST_PK(+) = JSI.PORT_MST_POD_FK";
            Strsql += " AND  TP_POD.PORT_MST_PK(+) = TIST.POD_FK";
            Strsql += " AND JSI.COMMODITY_GROUP_FK=CMST.COMMODITY_GROUP_PK(+)";
            Strsql += " AND  TIST.TRANSPORT_INST_SEA_PK = " + JOBPK;
            Strsql += " AND TTT.TD_TRUCK_NUMBER= '" + Truck + "' ";

            try
            {
                return ObjWk.GetDataSet(Strsql);
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

        public DataSet FetchTransporterSeaExp(string JOBNO, string Truck = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            string Strsql = null;
            Strsql = "SELECT JSE.JOB_CARD_TRN_PK JOBPK,";
            Strsql += " JSE.JOBCARD_REF_NO  JOBNO,";
            Strsql += " (CASE WHEN JSE.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
            Strsql += " JSE.VESSEL_NAME || '-' || JSE.VOYAGE_FLIGHT_NO";
            Strsql += " ELSE";
            Strsql += " JSE.VESSEL_NAME END ) VSE_VOY,";
            Strsql += " (CASE WHEN JSE.HBL_HAWB_FK IS NOT NULL THEN";
            Strsql += " HBL.HBL_REF_NO";
            Strsql += " ELSE";
            Strsql += " MBL.MBL_REF_NO END ) BLREFNO,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME, ";
            Strsql += " PMST.PLACE_NAME DELNAME,";
            Strsql += " COLMST.PLACE_NAME COLNAME,";
            Strsql += " TO_CHAR(JSE.ETD_DATE,'" + dateFormat + "')  ETD,";
            Strsql += " JSE.MARKS_NUMBERS MARKS,";
            Strsql += " JSE.GOODS_DESCRIPTION GOODS,";
            Strsql += " STMST.INCO_CODE TERMS,";
            Strsql += " JSE.PYMT_TYPE,";
            Strsql += " NVL(JSE.INSURANCE_AMT,0) INSURANCE,";
            Strsql += " CMST.COMMODITY_GROUP_DESC COMMCODE,";
            Strsql += " BST.CARGO_TYPE CARGO_TYPE,";
            Strsql += " TO_CHAR(JSE.ETA_DATE,'" + dateFormat + "')  ETA,";
            Strsql += " '' WORKORDER_NR,";
            Strsql += " TTT.DD_DRIVER1_NAME DRIVER_NAME,";
            Strsql += " TTT.DD_CONTACT_NUMBER1 MOBILE_NR,";
            Strsql += " TTT.TD_TRUCK_NUMBER TRUCK_NR,";
            Strsql += " (SELECT VMT.VENDOR_NAME FROM VENDOR_MST_TBL VMT WHERE VMT.VENDOR_MST_PK = TTT.TRANSPORTER_MST_FK) TRANSPORTER_NAME";
            Strsql += " FROM TRANSPORT_INST_SEA_TBL  TIST, TRANSPORT_TRN_CONT CONT, TRANSPORT_TRN_TRUCK TTT,JOB_CARD_TRN JSE,";
            Strsql += " SHIPPING_TERMS_MST_TBL STMST,";
            Strsql += " PLACE_MST_TBL PMST,";
            Strsql += " PLACE_MST_TBL COLMST,";
            Strsql += " PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,";
            Strsql += " BOOKING_MST_TBL BST,";
            Strsql += " COMMODITY_GROUP_MST_TBL CMST,";
            Strsql += " HBL_EXP_TBL HBL,";
            Strsql += " MBL_EXP_TBL MBL";
            Strsql += " WHERE TIST.JOB_CARD_FK IN (JSE.JOB_CARD_TRN_PK(+))";
            Strsql += " AND TIST.TRANSPORT_INST_SEA_PK = CONT.TRANSPORT_INST_FK";
            Strsql += " AND CONT.TRANSPORT_TRN_CONT_PK = TTT.TRANSPORT_TRN_CONT_FK";
            Strsql += " AND JSE.SHIPPING_TERMS_MST_FK=STMST.SHIPPING_TERMS_MST_PK(+)";
            Strsql += " AND   BST.BOOKING_MST_PK(+)=JSE.BOOKING_MST_FK";
            Strsql += " AND  PMST.PLACE_PK(+)=BST.DEL_PLACE_MST_FK";
            Strsql += " AND  POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK";
            Strsql += " AND  POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK";
            Strsql += " AND JSE.COMMODITY_GROUP_FK=CMST.COMMODITY_GROUP_PK(+)";
            Strsql += " AND  JSE.HBL_HAWB_FK=HBL.HBL_EXP_TBL_PK(+)";
            Strsql += " AND  JSE.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK(+)";
            Strsql += " AND  BST.COL_PLACE_MST_FK=COLMST.PLACE_PK(+)";
            Strsql += " AND  TIST.TRANSPORT_INST_SEA_PK  = " + JOBNO;
            Strsql += " AND TTT.TD_TRUCK_NUMBER= '" + Truck + "' ";
            Strsql += " GROUP BY";
            Strsql += " JSE.JOB_CARD_TRN_PK ,";
            Strsql += " JSE.JOBCARD_REF_NO ,";

            Strsql += " (CASE WHEN JSE.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
            Strsql += " JSE.VESSEL_NAME || '-' || JSE.VOYAGE_FLIGHT_NO";
            Strsql += " ELSE";
            Strsql += " JSE.VESSEL_NAME END ),";
            Strsql += " (CASE WHEN JSE.HBL_HAWB_FK IS NOT NULL THEN";
            Strsql += " HBL.HBL_REF_NO";
            Strsql += " ELSE";
            Strsql += " MBL.MBL_REF_NO END ),";
            Strsql += " POL.PORT_NAME ,";
            Strsql += " POD.PORT_NAME, ";
            Strsql += " PMST.PLACE_NAME,";
            Strsql += " COLMST.PLACE_NAME,";
            Strsql += " JSE.ETD_DATE,";
            Strsql += " JSE.MARKS_NUMBERS,";
            Strsql += " JSE.GOODS_DESCRIPTION,";
            Strsql += " STMST.INCO_CODE ,";
            Strsql += " JSE.PYMT_TYPE,";
            Strsql += " NVL(JSE.INSURANCE_AMT,0) ,";
            Strsql += " CMST.COMMODITY_GROUP_DESC ,";
            Strsql += " BST.CARGO_TYPE,JSE.ETA_DATE,";
            Strsql += " TTT.DD_DRIVER1_NAME,";
            Strsql += " TTT.DD_CONTACT_NUMBER1,";
            Strsql += "  TTT.TD_TRUCK_NUMBER,";
            Strsql += " TTT.TRANSPORTER_MST_FK";
            try
            {
                return ObjWk.GetDataSet(Strsql);
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

        public DataSet FetchTransporterBookingAirExp(string BKGNO)
        {
            WorkFlow ObjWk = new WorkFlow();
            string Strsql = null;
            try
            {
                Strsql = "SELECT BAT.BOOKING_MST_PK JOBPK,";
                Strsql += "  BAT.BOOKING_REF_NO  JOBNO,";
                Strsql += "  BAT.VOYAGE_FLIGHT_NO VSE_VOY,";
                Strsql += "  '' BLREFNO,";
                Strsql += "  POL.PORT_NAME POLNAME,";
                Strsql += "  POD.PORT_NAME PODNAME, ";
                Strsql += "  PMST.PLACE_NAME DELNAME,";
                Strsql += "  COLMST.PLACE_NAME COLNAME,";

                Strsql += "  TO_CHAR(BAT.ETD_DATE,'" + dateFormat + "')  ETD,";
                Strsql += "  '' MARKS,";
                Strsql += "  '' GOODS,";
                Strsql += "  '' TERMS,";
                Strsql += "   BAT.PYMT_TYPE,";
                Strsql += "  0 INSURANCE,";
                Strsql += "  CMST.COMMODITY_GROUP_DESC COMMCODE,";
                Strsql += "  2 CARGO_TYPE,";
                Strsql += "  TO_CHAR(BAT.ETA_DATE,'" + dateFormat + "') ETA";
                Strsql += "  FROM  BOOKING_MST_TBL BAT,";

                Strsql += "  PLACE_MST_TBL PMST,";
                Strsql += "  PLACE_MST_TBL COLMST,";
                Strsql += "  PORT_MST_TBL POL,";
                Strsql += "  PORT_MST_TBL POD,";

                Strsql += "  COMMODITY_GROUP_MST_TBL CMST";
                Strsql += "  WHERE";
                Strsql += "  PMST.PLACE_PK(+)=BAT.DEL_PLACE_MST_FK";
                Strsql += "  AND  POL.PORT_MST_PK(+) = BAT.PORT_MST_POL_FK";
                Strsql += "  AND  POD.PORT_MST_PK(+) = BAT.PORT_MST_POD_FK";
                Strsql += "  AND BAT.COMMODITY_GROUP_FK=CMST.COMMODITY_GROUP_PK(+)";

                Strsql += "  AND  BAT.COL_PLACE_MST_FK=COLMST.PLACE_PK(+)";
                Strsql += "  AND  BAT.BOOKING_REF_NO ='" + BKGNO + "'";
                return ObjWk.GetDataSet(Strsql);
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

        public DataSet FetchTransporterBookingSeaExp(string BKGNO)
        {
            WorkFlow ObjWk = new WorkFlow();
            string Strsql = null;
            try
            {
                Strsql = "SELECT BST.BOOKING_MST_PK JOBPK,";
                Strsql += "  BST.BOOKING_REF_NO  JOBNO,";
                Strsql += "  (CASE WHEN BST.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
                Strsql += " (BST.VESSEL_NAME || '-' || BST.VOYAGE_FLIGHT_NO) ";
                Strsql += " ELSE";
                Strsql += " BST.VESSEL_NAME END) VSE_VOY,";
                Strsql += "  '' BLREFNO,";
                Strsql += "  POL.PORT_NAME POLNAME,";
                Strsql += "  POD.PORT_NAME PODNAME, ";
                Strsql += "  PMST.PLACE_NAME DELNAME,";
                Strsql += "  COLMST.PLACE_NAME COLNAME,";

                Strsql += "   TO_CHAR(BST.ETD_DATE,'" + dateFormat + "')  ETD,";
                Strsql += "  '' MARKS,";
                Strsql += "  '' GOODS,";
                Strsql += "  STMST.INCO_CODE TERMS,";
                Strsql += "   BST.PYMT_TYPE,";
                Strsql += "  0 INSURANCE,";
                Strsql += "  CMST.COMMODITY_GROUP_DESC COMMCODE,";
                Strsql += "  BST.CARGO_TYPE CARGO_TYPE,";
                Strsql += "  TO_CHAR(BST.ETA_DATE,'" + dateFormat + "') ETA";
                Strsql += "  FROM  BOOKING_MST_TBL BST,";
                Strsql += "  SHIPPING_TERMS_MST_TBL STMST,";
                Strsql += "  PLACE_MST_TBL PMST,";
                Strsql += "  PLACE_MST_TBL COLMST,";
                Strsql += "  PORT_MST_TBL POL,";
                Strsql += "  PORT_MST_TBL POD,";

                Strsql += "  COMMODITY_GROUP_MST_TBL CMST";
                Strsql += "  WHERE BST.SHIPPING_TERMS_MST_FK=STMST.SHIPPING_TERMS_MST_PK(+)";
                Strsql += "  AND PMST.PLACE_PK(+)=BST.DEL_PLACE_MST_FK";
                Strsql += "  AND  POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK";
                Strsql += "  AND  POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK";
                Strsql += "  AND BST.COMMODITY_GROUP_FK=CMST.COMMODITY_GROUP_PK(+)";

                Strsql += "  AND  BST.COL_PLACE_MST_FK=COLMST.PLACE_PK(+)";
                Strsql += "  AND  BST.BOOKING_REF_NO ='" + BKGNO + "'";
                return ObjWk.GetDataSet(Strsql);
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

        public DataSet FetchTransporterAirExp(string JOBNO, string Truck = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            string Strsql = null;
            Strsql = "SELECT JAE.JOB_CARD_TRN_PK JOBPK,";
            Strsql += " JAE.JOBCARD_REF_NO  JOBNO,";
            Strsql += " JAE.VOYAGE_FLIGHT_NO VSE_VOY,";
            Strsql += " (CASE WHEN JAE.HBL_HAWB_FK IS NOT NULL THEN";
            Strsql += " HAWB.HAWB_REF_NO";
            Strsql += " ELSE";
            Strsql += " MAWB.MAWB_REF_NO END ) BLREFNO,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME, ";
            Strsql += " PMST.PLACE_NAME DELNAME,";
            Strsql += " COLMST.PLACE_NAME COLNAME,";
            Strsql += " TO_CHAR(JAE.ETD_DATE,'" + dateFormat + "')  ETD,";
            Strsql += " JAE.MARKS_NUMBERS MARKS,";
            Strsql += " JAE.GOODS_DESCRIPTION GOODS,";
            Strsql += " STMST.INCO_CODE TERMS,";
            Strsql += " JAE.PYMT_TYPE,";
            Strsql += " NVL(JAE.INSURANCE_AMT,0) INSURANCE,";
            Strsql += " CMST.COMMODITY_GROUP_DESC COMMCODE,";
            Strsql += " 2 CARGO_TYPE,";
            Strsql += " TO_CHAR(JAE.ETA_DATE,'" + dateFormat + "')  ETA,";
            Strsql += " '' WORKORDER_NR,";
            Strsql += " TTT.DD_DRIVER1_NAME DRIVER_NAME,";
            Strsql += " TTT.DD_CONTACT_NUMBER1 MOBILE_NR,";
            Strsql += " TTT.TD_TRUCK_NUMBER TRUCK_NR,";
            Strsql += " (SELECT VMT.VENDOR_NAME FROM VENDOR_MST_TBL VMT WHERE VMT.VENDOR_MST_PK = TTT.TRANSPORTER_MST_FK) TRANSPORTER_NAME";
            Strsql += " FROM TRANSPORT_INST_AIR_TB TIST, TRANSPORT_TRN_CONT CONT, TRANSPORT_TRN_TRUCK TTT,JOB_CARD_TRN JAE,";
            Strsql += " SHIPPING_TERMS_MST_TBL STMST,";
            Strsql += " PLACE_MST_TBL PMST,";
            Strsql += " PLACE_MST_TBL COLMST,";
            Strsql += " PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,";
            Strsql += " BOOKING_MST_TBL BAT,";
            Strsql += " COMMODITY_GROUP_MST_TBL CMST,";
            Strsql += " HAWB_EXP_TBL HAWB,";
            Strsql += " MAWB_EXP_TBL MAWB";
            Strsql += " WHERE TIST.JOB_CARD_FK IN (JAE.JOB_CARD_TRN_PK(+))";
            Strsql += " AND TIST.TRANSPORT_INST_AIR_PK = CONT.TRANSPORT_INST_FK";
            Strsql += " AND CONT.TRANSPORT_TRN_CONT_PK = TTT.TRANSPORT_TRN_CONT_FK";
            Strsql += " AND JAE.SHIPPING_TERMS_MST_FK=STMST.SHIPPING_TERMS_MST_PK(+)";
            Strsql += " AND BAT.BOOKING_MST_PK(+)=JAE.BOOKING_MST_FK";
            Strsql += " AND PMST.PLACE_PK(+)=BAT.DEL_PLACE_MST_FK";
            Strsql += " AND POL.PORT_MST_PK(+) = BAT.PORT_MST_POL_FK";
            Strsql += " AND  POD.PORT_MST_PK(+) = BAT.PORT_MST_POD_FK";
            Strsql += " AND JAE.COMMODITY_GROUP_FK=CMST.COMMODITY_GROUP_PK(+)";
            Strsql += " AND  JAE.HBL_HAWB_FK=HAWB.HAWB_EXP_TBL_PK(+)";
            Strsql += " AND  JAE.MBL_MAWB_FK=MAWB.MAWB_EXP_TBL_PK(+)";
            Strsql += " AND  BAT.COL_PLACE_MST_FK=COLMST.PLACE_PK(+)";
            Strsql += "  AND  TIST.TRANSPORT_INST_AIR_PK  = " + JOBNO;
            Strsql += " GROUP BY";
            Strsql += " JAE.JOB_CARD_TRN_PK ,";
            Strsql += " JAE.JOBCARD_REF_NO ,";

            Strsql += " JAE.VOYAGE_FLIGHT_NO,";
            Strsql += " (CASE WHEN JAE.HBL_HAWB_FK IS NOT NULL THEN";
            Strsql += " HAWB.HAWB_REF_NO";
            Strsql += " ELSE";
            Strsql += " MAWB.MAWB_REF_NO END ),";
            Strsql += " POL.PORT_NAME ,";
            Strsql += " POD.PORT_NAME, ";
            Strsql += " PMST.PLACE_NAME,";
            Strsql += " COLMST.PLACE_NAME,";
            Strsql += " JAE.ETD_DATE,";
            Strsql += " JAE.MARKS_NUMBERS ,";
            Strsql += " JAE.GOODS_DESCRIPTION,";
            Strsql += " STMST.INCO_CODE ,";
            Strsql += " JAE.PYMT_TYPE,";
            Strsql += " NVL(JAE.INSURANCE_AMT,0) ,";
            Strsql += " CMST.COMMODITY_GROUP_DESC ,";
            Strsql += " BAT.CARGO_TYPE,JAE.ETA_DATE,";
            Strsql += " TTT.DD_DRIVER1_NAME,";
            Strsql += " TTT.DD_CONTACT_NUMBER1,";
            Strsql += " TTT.TD_TRUCK_NUMBER,";
            Strsql += " TTT.TRANSPORTER_MST_FK";
            try
            {
                return ObjWk.GetDataSet(Strsql);
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

        #endregion "Fetch Transporter Import Details"

        #region "Fetch Report Transport Note"

        public DataSet FetchTransporterSeaExpNew(int TPTPK, int BizType, int Process, string Truck = "")
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
                _with5.CommandText = objWK.MyUserName + ".CUSTOMS_TRANSPORTNOTE_PKG.TRUCK_REPORT_PARAMETERS";

                objWK.MyCommand.Parameters.Clear();
                var _with6 = objWK.MyCommand.Parameters;

                _with6.Add("TPTPK_IN", TPTPK).Direction = ParameterDirection.Input;
                _with6.Add("TRUCK_IN", Truck).Direction = ParameterDirection.Input;
                _with6.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with6.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
                _with6.Add("TRUCK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Fetch Report Transport Note"

        #region "Certificate of Insurance -- Exports Sea"

        public DataSet FetchCISExpSea(Int32 JobPk, Int32 BizType)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();

            if (BizType == 2)
            {
                Strsql = "  SELECT  JS.JOB_CARD_TRN_PK AS JOBPK,";
                Strsql += " JS.JOBCARD_REF_NO AS JOBREFNO,";
                Strsql += " JS.VESSEL_NAME AS CONVEYANCE,";
                Strsql += " '' FRM,";
                Strsql += " DEL.PLACE_NAME AS VIATO,";
                Strsql += " NVL(JS.INSURANCE_AMT,0) AS INSUREDVALUE, ";
                Strsql += " C.CURRENCY_NAME,";
                Strsql += " H.MARKS_NUMBERS,";
                Strsql += " H.GOODS_DESCRIPTION AS INTEREST ,";
                Strsql += " SHP.CUSTOMER_NAME AS SHIPPER";
                Strsql += " FROM JOB_CARD_TRN JS,";
                Strsql += " HBL_EXP_TBL H,";
                Strsql += " PLACE_MST_TBL DEL,";
                Strsql += " CURRENCY_TYPE_MST_TBL C,";
                Strsql += " CUSTOMER_MST_TBL SHP";
                Strsql += " WHERE";
                Strsql += " H.JOB_CARD_SEA_EXP_FK(+)=JS.JOB_CARD_TRN_PK";
                Strsql += " AND DEL.PLACE_PK(+)=JS.Del_Place_Mst_Fk";
                Strsql += " AND C.CURRENCY_MST_PK(+)=JS.INSURANCE_CURRENCY";
                Strsql += " AND SHP.CUSTOMER_MST_PK(+)=JS.SHIPPER_CUST_MST_FK";
                Strsql += " AND JS.JOB_CARD_TRN_PK=" + JobPk;
            }
            else
            {
                Strsql = "  SELECT  JS.JOB_CARD_TRN_PK AS JOBPK,";
                Strsql += " JS.JOBCARD_REF_NO AS JOBREFNO,";
                Strsql += " JS.VOYAGE_FLIGHT_NO AS CONVEYANCE,";
                Strsql += " '' FRM,";
                Strsql += " DEL.PLACE_NAME AS VIATO,";
                Strsql += " NVL(JS.INSURANCE_AMT,0) AS INSUREDVALUE, ";
                Strsql += " C.CURRENCY_NAME,";
                Strsql += " H.MARKS_NUMBERS,";
                Strsql += " H.GOODS_DESCRIPTION AS INTEREST ,";
                Strsql += " SHP.CUSTOMER_NAME AS SHIPPER";
                Strsql += " FROM JOB_CARD_TRN JS,";
                Strsql += " HAWB_EXP_TBL H,";
                Strsql += " PLACE_MST_TBL DEL,";
                Strsql += " CURRENCY_TYPE_MST_TBL C,";
                Strsql += " CUSTOMER_MST_TBL SHP";
                Strsql += " WHERE";
                Strsql += " H.JOB_CARD_AIR_EXP_FK(+)=JS.JOB_CARD_TRN_PK";
                Strsql += " AND DEL.PLACE_PK(+)=JS.Del_Place_Mst_Fk";
                Strsql += " AND C.CURRENCY_MST_PK(+)=JS.INSURANCE_CURRENCY";
                Strsql += " AND SHP.CUSTOMER_MST_PK(+)=JS.SHIPPER_CUST_MST_FK";
                Strsql += " AND JS.JOB_CARD_TRN_PK=" + JobPk;
            }

            try
            {
                return ObjWk.GetDataSet(Strsql);
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

        #endregion "Certificate of Insurance -- Exports Sea"

        #region "Track And Trace Functionality"

        public void SaveTrackNTraceExp(string PkValue, short BizType, string nLocationFk, string PkStatus, long CREATED_BY)
        {
            try
            {
                WorkFlow objWK = new WorkFlow();
                objWK.OpenConnection();
                if (BizType == 1)
                {
                    arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(PkValue), BizType, 1, "Transport Note Issued", "TRA-NOTE-PRNT-AIR-EXP", Convert.ToInt32(nLocationFk), objWK, "INS", CREATED_BY, PkStatus);
                }
                else
                {
                    arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(PkValue), BizType, 1, "Transport Note Issued", "TRA-NOTE-PRNT-SEA-EXP", Convert.ToInt32(nLocationFk), objWK, "INS", CREATED_BY, PkStatus);
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

        public void SaveTrackNTraceImp(int PkValue, int nlocationFk, int BizType)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            string Onstatus = null;
            try
            {
                if (BizType == 1)
                {
                    Onstatus = "TRNS-NOTE-PRNT-AIR-IMP";
                }
                else
                {
                    Onstatus = "TRNS-NOTE-PRINT-SEA-IMP";
                }
                arrMessage = objTrackNTrace.SaveTrackAndTrace(PkValue, BizType, 2, "Transport Note Issued", Onstatus, nlocationFk, objWK, "INS", CREATED_BY, "O");
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

        #endregion "Track And Trace Functionality"

        #region "save"

        public ArrayList Save(DataSet dsMain, string Measure, string Wt, string Divfac, string strPk, int PROCESS_TYPE, string BUSINESS_TYPE, string JOB_CARD_FK, object TRANS_INS_REF_NO, string TRANS_INST_DATE,
        string TRANSPORTER_MST_FK, string COL_REF_NO, string COL_ADDRESS, string GOODS_DESCRIPTION, string SPECIAL_INSTRUCTIONS, string PACK_COUNT, string PACK_TYPE_MST_FK, string GROSS_WEIGHT_IN, string CHRG_WEIGHT, string VOLUME,
        string DEL_REF_NO, string DEL_ADDRESS, long lngLocPk, long nEmpId, long nUserId, string ZoneFK, string TransCost, string COST_TPC_FK, string VENDOR_KEY, string CurrencyFK,
        string VendorMstFK, bool flag = true, int version = 0, int TransMode = 1)
        {
            WorkFlow objWK = new WorkFlow();
            Int16 exe = default(Int16);
            bool IsUpdate = false;

            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            long lngTransportNotePk = 0;
            Int32 RecAfct = default(Int32);
            string TransportNoteNo = null;
            OracleCommand insCommand = new OracleCommand();

            objWK.MyCommand.Connection = objWK.MyConnection;
            objWK.MyCommand.Transaction = TRAN;
            if ((strPk == null))
            {
                strPk = "0";
            }
            if (Convert.ToInt32(strPk) != 0)
            {
                objWK.MyCommand.Parameters.Clear();

                insCommand.CommandText = objWK.MyUserName + ".TRANSPORT_INST_AIR_TB_PKG.TRANSPORT_INST_AIR_TB_UPD";
                insCommand.Connection = objWK.MyConnection;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.Transaction = TRAN;
            }
            else
            {
                objWK.MyCommand.Parameters.Clear();

                insCommand.CommandText = objWK.MyUserName + ".TRANSPORT_INST_AIR_TB_PKG.TRANSPORT_INST_AIR_TB_INS";
                insCommand.Connection = objWK.MyConnection;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.Transaction = TRAN;
            }

            try
            {
                if (PROCESS_TYPE == 1)
                {
                    if (string.IsNullOrEmpty(Convert.ToString(TRANS_INS_REF_NO)))
                    {
                        TransportNoteNo = GenerateProtocolKey("TRANSPORT INSTRUCTION AIR EXPORT", lngLocPk, nEmpId, DateTime.Today, "", "", "", nUserId);
                        if (TransportNoteNo == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        TransportNoteNo = Convert.ToString(TRANS_INS_REF_NO);
                    }
                }
                else if (PROCESS_TYPE == 2)
                {
                    if (string.IsNullOrEmpty(Convert.ToString(TRANS_INS_REF_NO)))
                    {
                        TransportNoteNo = GenerateProtocolKey("TRANSPORT INSTRUCTION AIR IMPORT", lngLocPk, nEmpId, DateTime.Today, "", "", "", nUserId);
                        if (TransportNoteNo == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        TransportNoteNo = Convert.ToString(TRANS_INS_REF_NO);
                    }
                }
                if (TransCost.Length > 0)
                {
                    TransCost = Convert.ToString(TransCost);
                }
                var _with7 = insCommand.Parameters;
                if (Convert.ToInt32(strPk) != 0)
                {
                    _with7.Add("TRANSPORT_INST_AIR_PK_IN", Convert.ToInt64(strPk));
                    _with7.Add("LAST_MODIFIED_BY_FK_IN", nUserId);
                    _with7.Add("VERSION_NO_IN", version);
                }
                else
                {
                    _with7.Add("CREATED_BY_FK_IN", nUserId);
                }
                _with7.Add("PROCESS_TYPE_IN", PROCESS_TYPE);
                _with7.Add("BUSINESS_TYPE_IN", BUSINESS_TYPE);
                _with7.Add("JOB_CARD_FK_IN", getDefault(JOB_CARD_FK, ""));
                _with7.Add("TRANS_INS_REF_NO_IN", TransportNoteNo);
                _with7.Add("TRANS_INST_DATE_IN", getDefault(Convert.ToDateTime(TRANS_INST_DATE), ""));
                _with7.Add("TRANSPORTER_MST_FK_IN", getDefault(TRANSPORTER_MST_FK, ""));
                _with7.Add("COL_REF_NO_IN", COL_REF_NO);
                _with7.Add("COL_ADDRESS_IN", getDefault(COL_ADDRESS, ""));
                _with7.Add("GOODS_DESCRIPTION_IN", getDefault(GOODS_DESCRIPTION, ""));
                _with7.Add("SPECIAL_INSTRUCTIONS_IN", getDefault(SPECIAL_INSTRUCTIONS, ""));
                _with7.Add("PACK_COUNT_IN", getDefault(PACK_COUNT, ""));
                _with7.Add("PACK_TYPE_MST_FK_IN", getDefault(PACK_TYPE_MST_FK, ""));
                _with7.Add("GROSS_WEIGHT_IN", getDefault(GROSS_WEIGHT_IN, ""));
                _with7.Add("CHRG_WEIGHT_IN", getDefault(CHRG_WEIGHT, ""));
                _with7.Add("VOLUME_IN", getDefault(VOLUME, ""));
                _with7.Add("TRANSPORTER_ZONES_FK_IN", getDefault(ZoneFK, ""));
                _with7.Add("TRANS_COST_IN", getDefault(TransCost, ""));
                _with7.Add("CURRENCY_MST_FK_IN", CurrencyFK);
                _with7.Add("CONFIG_MST_FK_IN", ConfigurationPK);
                _with7.Add("DEL_REF_NO_IN", DEL_REF_NO);
                _with7.Add("DEL_ADDRESS_IN", getDefault(DEL_ADDRESS, ""));
                _with7.Add("TRANS_MODE_IN", getDefault(TransMode, ""));
                if (Convert.ToInt32(strPk) != 0)
                {
                    _with7.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                }
                else
                {
                    _with7.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRANSPORT_INST_AIR_PK").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                }

                exe = Convert.ToInt16(insCommand.ExecuteNonQuery());
                lngTransportNotePk = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);

                arrMessage = SaveTransportCargoCal(dsMain, lngTransportNotePk, objWK.MyCommand, IsUpdate, Measure, Wt, Divfac, TRAN);

                if (Convert.ToInt32(strPk) == 0 & !string.IsNullOrEmpty(JOB_CARD_FK))
                {
                    arrMessage = SaveJobCardPIAIns(JOB_CARD_FK, COST_TPC_FK, VENDOR_KEY, CurrencyFK, TransCost, PROCESS_TYPE, VendorMstFK, objWK.MyCommand, TRAN);
                }

                if (exe > 0)
                {
                    arrMessage.Clear();
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    strPk = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value);
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException oraexp)
            {
                if (Convert.ToInt32(strPk) == 0)
                {
                    if (PROCESS_TYPE == 1)
                    {
                        RollbackProtocolKey("TRANSPORT INSTRUCTION AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNo, System.DateTime.Now);
                    }
                    else if (PROCESS_TYPE == 2)
                    {
                        RollbackProtocolKey("TRANSPORT INSTRUCTION AIR IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNo, System.DateTime.Now);
                    }
                }

                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                if (Convert.ToInt32(strPk) == 0)
                {
                    if (PROCESS_TYPE == 1)
                    {
                        RollbackProtocolKey("TRANSPORT INSTRUCTION AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNo, System.DateTime.Now);
                    }
                    else if (PROCESS_TYPE == 2)
                    {
                        RollbackProtocolKey("TRANSPORT INSTRUCTION AIR IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNo, System.DateTime.Now);
                    }
                }
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "save"

        #region "Save function for sea"

        public ArrayList save_sea(string strPk, int PROCESS_TYPE, string BUSINESS_TYPE, string JOB_CARD_FK, string CARGO_TYPE, object TRANS_INS_REF_NO, string TRANS_INST_DATE, string TRANSPORTER_MST_FK, string MT_CTR_PICKUP_REF, string MT_CTR_PICKUP_ADDRESS,
        System.DateTime MT_CTR_PICKUP_BY, string PRINT_MT_CTR_PKP_ADD, string CARGO_PICKUP_REF_NO, string CARGO_PICKUP_ADDRESS, System.DateTime CARGO_PICKUP_BY, string PRINT_CARGO_PKP_ADD, string DELIVERY_REF_NO, string DELIVERY_OFF_ADDRESS, System.DateTime DELIVERY_BY, string PRINT_DELIVERY_ADD,
        string GOODS_DESCRIPTION, string SPECIAL_INSTRUCTIONS, string GROSS_WEIGHT_IN, string NET_WEIGHT, string VOLUME, long lngLocPk, long nEmpId, long nUserId, string ETD, string ETA,
        bool flag = true, int version = 0, int POLPK = 0, int PODPK = 0, int VSLVOYPK = 0, int MODE = 0)
        {
            WorkFlow objWK = new WorkFlow();
            Int16 exe = default(Int16);
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            DateTime dETA = default(DateTime);
            DateTime dETD = default(DateTime);
            int Chk_Eta = 0;
            int chk_Etd = 0;
            long lngTransportNoteSeaPk = 0;
            Int32 RecAfct = default(Int32);
            string TransportNoteNoSea = null;
            OracleCommand insCommand = new OracleCommand();

            if (!string.IsNullOrEmpty(ETA))
            {
                dETA = Convert.ToDateTime(ETA);
            }
            else
            {
                dETA = DateTime.MinValue;
                Chk_Eta = 1;
            }

            if (!string.IsNullOrEmpty(ETD))
            {
                dETD = Convert.ToDateTime(ETD);
            }
            else
            {
                dETD = DateTime.MinValue;
                chk_Etd = 1;
            }

            objWK.MyCommand.Connection = objWK.MyConnection;
            objWK.MyCommand.Transaction = TRAN;

            if (Convert.ToInt32(strPk) != 0)
            {
                objWK.MyCommand.Parameters.Clear();
                insCommand.CommandText = objWK.MyUserName + ".TRANSPORT_INST_SEA_TBL_PKG.TRANSPORT_INST_SEA_TBL_UPD";
                insCommand.Connection = objWK.MyConnection;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.Transaction = TRAN;
            }
            else
            {
                objWK.MyCommand.Parameters.Clear();
                insCommand.CommandText = objWK.MyUserName + ".TRANSPORT_INST_SEA_TBL_PKG.TRANSPORT_INST_SEA_TBL_INS";
                insCommand.Connection = objWK.MyConnection;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.Transaction = TRAN;
            }

            try
            {
                if (PROCESS_TYPE == 1)
                {
                    if (string.IsNullOrEmpty(Convert.ToString(TRANS_INS_REF_NO)))
                    {
                        TransportNoteNoSea = GenerateProtocolKey("TRANSPORT INSTRUCTION SEA EXPORT", lngLocPk, nEmpId, DateTime.Today, "", "", "", nUserId);
                        if (TransportNoteNoSea == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        TransportNoteNoSea = Convert.ToString(TRANS_INS_REF_NO);
                    }
                }
                else if (PROCESS_TYPE == 2)
                {
                    if (string.IsNullOrEmpty(Convert.ToString(TRANS_INS_REF_NO)))
                    {
                        TransportNoteNoSea = GenerateProtocolKey("TRANSPORT INSTRUCTION SEA IMPORT", lngLocPk, nEmpId, DateTime.Today, "", "", "", nUserId);
                        if (TransportNoteNoSea == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        TransportNoteNoSea = Convert.ToString(TRANS_INS_REF_NO);
                    }
                }

                var _with8 = insCommand.Parameters;
                if (Convert.ToInt32(strPk) != 0)
                {
                    _with8.Add("transport_inst_sea_pk_in", Convert.ToInt64(strPk));
                    _with8.Add("last_modified_by_fk_in", nUserId);
                    _with8.Add("version_no_in", version);
                }
                else
                {
                    _with8.Add("created_by_fk_in", nUserId);
                }

                _with8.Add("process_type_in", PROCESS_TYPE);
                _with8.Add("business_type_in", BUSINESS_TYPE);
                _with8.Add("job_card_fk_in", getDefault(JOB_CARD_FK, ""));
                _with8.Add("cargo_type_in", CARGO_TYPE);
                _with8.Add("trans_inst_ref_no_in", TransportNoteNoSea);
                _with8.Add("trans_inst_date_in", getDefault(Convert.ToDateTime(TRANS_INST_DATE), ""));
                _with8.Add("transporter_mst_fk_in", TRANSPORTER_MST_FK);
                _with8.Add("mt_ctr_pickup_ref_in", getDefault(MT_CTR_PICKUP_REF, ""));
                _with8.Add("mt_ctr_pickup_address_in", getDefault(MT_CTR_PICKUP_ADDRESS, ""));
                _with8.Add("mt_ctr_pickup_by_in", MT_CTR_PICKUP_BY);
                _with8.Add("print_mt_ctr_pkp_add_in", PRINT_MT_CTR_PKP_ADD);
                _with8.Add("cargo_pickup_ref_no_in", CARGO_PICKUP_REF_NO);
                _with8.Add("cargo_pickup_address_in", getDefault(CARGO_PICKUP_ADDRESS, ""));
                _with8.Add("cargo_pickup_by_in", CARGO_PICKUP_BY);
                _with8.Add("print_cargo_pkp_add_in", PRINT_CARGO_PKP_ADD);
                _with8.Add("delivery_ref_no_in", DELIVERY_REF_NO);
                _with8.Add("delivery_off_address_in", getDefault(DELIVERY_OFF_ADDRESS, ""));
                _with8.Add("delivery_by_in", DELIVERY_BY);
                _with8.Add("print_delivery_add_in", PRINT_DELIVERY_ADD);
                _with8.Add("goods_description_in", getDefault(GOODS_DESCRIPTION, ""));
                _with8.Add("special_instructions_in", getDefault(SPECIAL_INSTRUCTIONS, ""));
                _with8.Add("GROSS_WEIGHT_IN", Convert.ToDouble(getDefault(GROSS_WEIGHT_IN, "")));
                _with8.Add("net_weight_in", Convert.ToDouble(getDefault(NET_WEIGHT, "")));
                _with8.Add("volume_in", Convert.ToDouble(getDefault(VOLUME, "")));
                _with8.Add("TP_MODE_IN", MODE);
                _with8.Add("POL_FK_IN", POLPK);
                _with8.Add("POD_FK_IN", PODPK);
                _with8.Add("VSL_VOY_FK_IN", getDefault(VSLVOYPK, ""));
                if (Chk_Eta == 1)
                {
                    _with8.Add("ETD_IN", "");
                }
                else
                {
                    _with8.Add("ETD_IN", dETD);
                }

                if (chk_Etd == 1)
                {
                    _with8.Add("ETA_IN", "");
                }
                else
                {
                    _with8.Add("ETA_IN", dETD);
                }

                _with8.Add("config_mst_fk_in", ConfigurationPK);
                if (Convert.ToInt32(strPk) != 0)
                {
                    _with8.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                }
                else
                {
                    _with8.Add("return_value", OracleDbType.Int32, 10, "TRANSPORT_INST_SEA_PK").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                }

                exe = Convert.ToInt16(insCommand.ExecuteNonQuery());
                lngTransportNoteSeaPk = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);

                if (exe > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    strPk = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value);
                    return arrMessage;
                }
                else
                {
                    if (Convert.ToInt32(strPk) == 0)
                    {
                        if (PROCESS_TYPE == 1)
                        {
                            RollbackProtocolKey("TRANSPORT INSTRUCTION SEA EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNoSea, System.DateTime.Now);
                        }
                        else if (PROCESS_TYPE == 2)
                        {
                            RollbackProtocolKey("TRANSPORT INSTRUCTION SEA IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNoSea, System.DateTime.Now);
                        }
                    }
                    TRAN.Rollback();
                }
            }
            catch (OracleException oraexp)
            {
                if (Convert.ToInt32(strPk) == 0)
                {
                    if (PROCESS_TYPE == 1)
                    {
                        RollbackProtocolKey("TRANSPORT INSTRUCTION SEA EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNoSea, System.DateTime.Now);
                    }
                    else if (PROCESS_TYPE == 2)
                    {
                        RollbackProtocolKey("TRANSPORT INSTRUCTION SEA IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNoSea, System.DateTime.Now);
                    }
                }
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                if (Convert.ToInt32(strPk) == 0)
                {
                    if (PROCESS_TYPE == 1)
                    {
                        RollbackProtocolKey("TRANSPORT INSTRUCTION SEA EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNoSea, System.DateTime.Now);
                    }
                    else if (PROCESS_TYPE == 2)
                    {
                        RollbackProtocolKey("TRANSPORT INSTRUCTION SEA IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), TransportNoteNoSea, System.DateTime.Now);
                    }
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "Save function for sea"

        #region "Cargo Calculator"

        #region "Save CargoCalculator"

        public ArrayList SaveTransportCargoCal(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string Measure, string Wt, string Divfac, OracleTransaction TRAN = null)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            try
            {
                if (!IsUpdate)
                {
                    objWK.OpenConnection();
                    objWK.MyConnection = TRAN.Connection;

                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblCDimension"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with9 = SelectCommand;
                        _with9.CommandType = CommandType.StoredProcedure;
                        _with9.CommandText = objWK.MyUserName + ".TRANSPORT_INST_AIR_TB_PKG.TRANS_CARGO_CALC_INS";
                        SelectCommand.Parameters.Clear();

                        _with9.Parameters.Add("TRANSPORT_INST_AIR_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with9.Parameters.Add("CARGO_NOP_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_NOP"]).Direction = ParameterDirection.Input;
                        _with9.Parameters["CARGO_NOP_IN"].SourceVersion = DataRowVersion.Current;

                        _with9.Parameters.Add("CARGO_LENGTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_LENGTH"]).Direction = ParameterDirection.Input;
                        _with9.Parameters["CARGO_LENGTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with9.Parameters.Add("CARGO_WIDTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_WIDTH"]).Direction = ParameterDirection.Input;
                        _with9.Parameters["CARGO_WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with9.Parameters.Add("CARGO_HEIGHT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_HEIGHT"]).Direction = ParameterDirection.Input;
                        _with9.Parameters["CARGO_HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with9.Parameters.Add("CARGO_CUBE_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_CUBE"]).Direction = ParameterDirection.Input;
                        _with9.Parameters["CARGO_CUBE_IN"].SourceVersion = DataRowVersion.Current;

                        _with9.Parameters.Add("CARGO_VOLUME_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_VOLUME_WT"]).Direction = ParameterDirection.Input;
                        _with9.Parameters["CARGO_VOLUME_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with9.Parameters.Add("CARGO_ACTUAL_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_ACTUAL_WT"]).Direction = ParameterDirection.Input;
                        _with9.Parameters["CARGO_ACTUAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with9.Parameters.Add("CARGO_DENSITY_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_DENSITY"]).Direction = ParameterDirection.Input;
                        _with9.Parameters["CARGO_DENSITY_IN"].SourceVersion = DataRowVersion.Current;

                        _with9.Parameters.Add("CARGO_MEASURE_IN", (string.IsNullOrEmpty(Measure) ? "0" : Measure)).Direction = ParameterDirection.Input;
                        _with9.Parameters.Add("CARGO_WT_IN", (string.IsNullOrEmpty(Wt) ? "0" : Wt)).Direction = ParameterDirection.Input;
                        _with9.Parameters.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(Divfac) ? "0" : Divfac)).Direction = ParameterDirection.Input;

                        _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        var _with10 = objWK.MyDataAdapter;
                        _with10.InsertCommand = SelectCommand;
                        _with10.InsertCommand.Transaction = TRAN;
                        _with10.InsertCommand.ExecuteNonQuery();
                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblCDimension"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with11 = SelectCommand;
                        _with11.CommandType = CommandType.StoredProcedure;
                        _with11.CommandText = objWK.MyUserName + ".TRANSPORT_INST_AIR_TB_PKG.TRANS_CARGO_CALC_UPD";
                        SelectCommand.Parameters.Clear();

                        _with11.Parameters.Add("TRANS_INS_AIR_CARGO_CAL_PK_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["TRANS_INST_AIR_CARGO_CALC_PK"]).Direction = ParameterDirection.Input;
                        _with11.Parameters["TRANS_INS_AIR_CARGO_CAL_PK_IN"].SourceVersion = DataRowVersion.Current;

                        _with11.Parameters.Add("TRANSPORT_INST_AIR_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with11.Parameters.Add("CARGO_NOP_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_NOP"]).Direction = ParameterDirection.Input;
                        _with11.Parameters["CARGO_NOP_IN"].SourceVersion = DataRowVersion.Current;

                        _with11.Parameters.Add("CARGO_LENGTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_LENGTH"]).Direction = ParameterDirection.Input;
                        _with11.Parameters["CARGO_LENGTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with11.Parameters.Add("CARGO_WIDTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_WIDTH"]).Direction = ParameterDirection.Input;
                        _with11.Parameters["CARGO_WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with11.Parameters.Add("CARGO_HEIGHT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_HEIGHT"]).Direction = ParameterDirection.Input;
                        _with11.Parameters["CARGO_HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with11.Parameters.Add("CARGO_CUBE_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_CUBE"]).Direction = ParameterDirection.Input;
                        _with11.Parameters["CARGO_CUBE_IN"].SourceVersion = DataRowVersion.Current;

                        _with11.Parameters.Add("CARGO_VOLUME_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_VOLUME_WT"]).Direction = ParameterDirection.Input;
                        _with11.Parameters["CARGO_VOLUME_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with11.Parameters.Add("CARGO_ACTUAL_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_ACTUAL_WT"]).Direction = ParameterDirection.Input;
                        _with11.Parameters["CARGO_ACTUAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with11.Parameters.Add("CARGO_DENSITY_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_DENSITY"]).Direction = ParameterDirection.Input;
                        _with11.Parameters["CARGO_DENSITY_IN"].SourceVersion = DataRowVersion.Current;

                        _with11.Parameters.Add("CARGO_MEASURE_IN", (string.IsNullOrEmpty(Measure) ? "0" : Measure)).Direction = ParameterDirection.Input;
                        _with11.Parameters.Add("CARGO_WT_IN", (string.IsNullOrEmpty(Wt) ? "0" : Wt)).Direction = ParameterDirection.Input;
                        _with11.Parameters.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(Divfac) ? "0" : Divfac)).Direction = ParameterDirection.Input;

                        _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        var _with12 = objWK.MyDataAdapter;
                        _with12.InsertCommand = SelectCommand;
                        _with12.InsertCommand.Transaction = TRAN;
                        _with12.InsertCommand.ExecuteNonQuery();
                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
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

        #endregion "Save CargoCalculator"

        #region "Save TRN in JobCard PIA Table"

        public ArrayList SaveJobCardPIAIns(string JOB_CARD_FK, string COST_TPC_FK, string VENDOR_KEY, string CurrencyFK, string TransCost, int PROCESS_TYPE, string VendorMstFK, OracleCommand SelectCommand, OracleTransaction TRAN = null)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            Int32 RecAfct = default(Int32);

            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;
                if (PROCESS_TYPE == 1)
                {
                    var _with13 = SelectCommand;
                    _with13.CommandType = CommandType.StoredProcedure;
                    _with13.CommandText = objWK.MyUserName + ".TRANSPORT_INST_AIR_TB_PKG.JOB_CARD_AIR_EXP_PIA_INS";
                    SelectCommand.Parameters.Clear();

                    _with13.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JOB_CARD_FK).Direction = ParameterDirection.Input;

                    _with13.Parameters.Add("COST_ELEMENT_MST_FK_IN", COST_TPC_FK).Direction = ParameterDirection.Input;

                    _with13.Parameters.Add("VENDOR_KEY_IN", VENDOR_KEY).Direction = ParameterDirection.Input;

                    _with13.Parameters.Add("CURRENCY_MST_FK_IN", CurrencyFK).Direction = ParameterDirection.Input;

                    _with13.Parameters.Add("ESTIMATED_AMT_IN", getDefault(TransCost, "")).Direction = ParameterDirection.Input;

                    _with13.Parameters.Add("VENDOR_MST_FK_IN", VendorMstFK).Direction = ParameterDirection.Input;

                    _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    var _with14 = objWK.MyDataAdapter;
                    _with14.InsertCommand = SelectCommand;
                    _with14.InsertCommand.Transaction = TRAN;
                    _with14.InsertCommand.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                else if (PROCESS_TYPE == 2)
                {
                    var _with15 = SelectCommand;
                    _with15.CommandType = CommandType.StoredProcedure;
                    _with15.CommandText = objWK.MyUserName + ".TRANSPORT_INST_AIR_TB_PKG.JOB_CARD_AIR_IMP_PIA_INS";
                    SelectCommand.Parameters.Clear();

                    _with15.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JOB_CARD_FK).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("COST_ELEMENT_MST_FK_IN", COST_TPC_FK).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("VENDOR_KEY_IN", VENDOR_KEY).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("CURRENCY_MST_FK_IN", CurrencyFK).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("ESTIMATED_AMT_IN", getDefault(TransCost, "")).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("VENDOR_MST_FK_IN", VendorMstFK).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    var _with16 = objWK.MyDataAdapter;
                    _with16.InsertCommand = SelectCommand;
                    _with16.InsertCommand.Transaction = TRAN;
                    _with16.InsertCommand.ExecuteNonQuery();

                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }

        #endregion "Save TRN in JobCard PIA Table"

        #region "Fetch Zone"

        public string FetchTransporterZone(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            string transFK = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            transFK = Convert.ToString(arr.GetValue(2));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".TRANSPORT_INST_AIR_TB_PKG.GET_TRANSPORTER_ZONE";

                var _with17 = selectCommand.Parameters;
                _with17.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with17.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with17.Add("TRANSPORT_INST_AIR_PK_IN", transFK).Direction = ParameterDirection.Input;
                _with17.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion "Fetch Zone"

        #region "GetCorpCurrency()"

        public DataSet GetCorpCurrency()
        {
            string strSQL = null;
            strSQL = "SELECT CMT.CURRENCY_MST_FK,CUMT.CURRENCY_ID FROM CORPORATE_MST_TBL CMT,CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += "WHERE CMT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            try
            {
                DataSet DS = null;
                DS = (new WorkFlow()).GetDataSet(strSQL);
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

        #endregion "GetCorpCurrency()"

        #region "Fetch Rate"

        public DataSet FetchApplicableRate(string TransPK, string ZonePK, string ChegWt, System.DateTime TransDate)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            string strReturn = "";

            strSQL = " SELECT DECODE(CMT.RATE_APPLICABILITY_TYPE, ";
            strSQL += " 0,( CTT.RANGE_RATE * " + ChegWt + "), ";
            strSQL += " 1, ";
            strSQL += " ( CASE ";
            strSQL += " WHEN (CTT.RANGE_RATE * " + ChegWt + ") > CMT.MIN_RATE THEN (CTT.RANGE_RATE * " + ChegWt + ") ";
            strSQL += " WHEN (CTT.RANGE_RATE * " + ChegWt + ") < CMT.MIN_RATE THEN CMT.MIN_RATE ";
            strSQL += " END) , ";
            strSQL += " 2, ((CTT.RANGE_RATE * " + ChegWt + ") + CMT.BASE_RATE ) ) TRANS_COST, CMT.CURRENCY_MST_FK CURR_PK, CURR.CURRENCY_ID CURR_ID ";
            strSQL += " FROM CONT_MAIN_TRANS_TBL CMT, CONT_TRN_TRANS CTT, CURRENCY_TYPE_MST_TBL CURR ";
            strSQL += " WHERE CMT.CONT_MAIN_TRANS_PK = CTT.CONT_MAIN_TRANS_FK ";
            strSQL += " AND CMT.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
            strSQL += " AND CMT.CONT_APPROVED = 1 ";
            strSQL += " AND CMT.ACTIVE = 1";
            strSQL += " AND TO_DATE(' " + TransDate + "','" + dateFormat + "') BETWEEN CMT.VALID_FROM AND nvl(CMT.VALID_TO,NULL_DATE_FORMAT)";
            if (!string.IsNullOrEmpty(TransPK))
            {
                strSQL += " AND CMT.TRANSPORTER_MST_FK = " + TransPK + "";
            }
            strSQL += " AND CTT.TRANSPORTER_ZONES_FK = " + ZonePK + "";
            strSQL += " AND " + ChegWt + " BETWEEN CTT.FROM_WEIGHT AND CTT.TO_WEIGHT";
            strSQL += " AND CMT.BUSINESS_TYPE = 1 ";
            strSQL += "";

            try
            {
                return objWK.GetDataSet(strSQL);
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

        public DataSet FetchRateType(string TransPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();

            strSQL = "SELECT M.RATE_APPLICABILITY_TYPE \"Rate_Type\", M.BASE_RATE \"Base_Rate\", M.MIN_RATE \"Min_Rate\"";
            strSQL += "FROM CONT_MAIN_TRANS_TBL M";
            strSQL += "WHERE M.TRANSPORTER_MST_FK = " + TransPK + "";

            try
            {
                return objWK.GetDataSet(strSQL.ToString());
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

        #endregion "Fetch Rate"

        #region " Fetch ROE"

        public DataSet Fetch_ROE(string CurrencyPK)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK,(select c.currency_mst_fk from corporate_mst_tbl c),round(sysdate - .5)),6) AS ROE");
                strSQL.Append("FROM");
                strSQL.Append("    CURRENCY_TYPE_MST_TBL CURR");
                strSQL.Append("WHERE");
                strSQL.Append("    CURR.ACTIVE_FLAG = 1");
                strSQL.Append(" AND CURR.CURRENCY_MST_PK = '" + CurrencyPK + "'");

                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch ROE"

        #region "Fetch Cargo Details"

        public object FetchCDimension(DataTable dtMain, long lngABEPK)
        {
            string strSqlCDimension = "";
            WorkFlow objwf = new WorkFlow();

            strSqlCDimension = "SELECT " + "ROWNUM AS SNO, " + "BACC.CARGO_NOP AS NOP, " + "BACC.CARGO_LENGTH AS LENGTH, " + "BACC.CARGO_WIDTH AS WIDTH, " + "BACC.CARGO_HEIGHT AS HEIGHT, " + "BACC.CARGO_CUBE AS CUBE, " + "BACC.CARGO_VOLUME_WT AS VOLWEIGHT, " + "BACC.CARGO_ACTUAL_WT AS ACTWEIGHT, " + "BACC.CARGO_DENSITY AS DENSITY, " + "BACC.BOOKING_CARGO_CALC_PK AS PK, " + "BACC.BOOKING_MST_FK " + "FROM " + "BOOKING_CARGO_CALC BACC " + "WHERE " + "BACC.BOOKING_MST_FK= " + lngABEPK;
            try
            {
                dtMain = objwf.GetDataTable(strSqlCDimension);
                return dtMain;
            }
            catch (OracleException oraExp)
            {
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public object FetchCDTrans(DataTable dtMain, long transPK)
        {
            string strSqlCDimension = "";
            WorkFlow objwf = new WorkFlow();

            strSqlCDimension = "SELECT " + "ROWNUM AS SNO, " + "TIA.CARGO_NOP AS NOP, " + "TIA.CARGO_LENGTH AS LENGTH, " + "TIA.CARGO_WIDTH AS WIDTH, " + "TIA.CARGO_HEIGHT AS HEIGHT, " + "TIA.CARGO_CUBE AS CUBE, " + "TIA.CARGO_VOLUME_WT AS VOLWEIGHT, " + "TIA.CARGO_ACTUAL_WT AS ACTWEIGHT, " + "TIA.CARGO_DENSITY AS DENSITY, " + "TIA.TRANS_INST_AIR_CARGO_CALC_PK AS PK, " + "TIA.TRANSPORT_INST_AIR_FK " + "FROM " + "TRANS_INST_AIR_CARGO_CALC_TBL TIA " + "WHERE " + "TIA.TRANSPORT_INST_AIR_FK= " + transPK;
            try
            {
                dtMain = objwf.GetDataTable(strSqlCDimension);
                return dtMain;
            }
            catch (OracleException oraExp)
            {
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Cargo Details"

        #region "Fetch Booking PK"

        public string FetchBookingPK(string JobCardPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            string strReturn = "";

            strSQL = " select je.BOOKING_MST_FK BOOKING_MST_FK from JOB_CARD_TRN je ";
            strSQL += " where je.JOB_CARD_TRN_PK = " + JobCardPK + "";
            strSQL += " ";

            objWK.MyDataReader = objWK.GetDataReader(strSQL);
            try
            {
                while (objWK.MyDataReader.Read())
                {
                    strReturn = Convert.ToString(objWK.MyDataReader[0]);
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                string e = ex.Message;
            }
            finally
            {
                objWK.MyDataReader.Close();
                objWK.MyConnection.Close();
            }
            return strReturn;
        }

        #endregion "Fetch Booking PK"

        #region "Fetch COSTTPC"

        public string FetchCostTPCFK()
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            string strReturn = "";

            strSQL = " select param.cost_tpc_fk ";
            strSQL += " from parameters_tbl param, cost_element_mst_tbl cmt ";
            strSQL += " where param.cost_tpc_fk = cmt.cost_element_mst_pk ";
            strSQL += "";

            objWK.MyDataReader = objWK.GetDataReader(strSQL);
            try
            {
                while (objWK.MyDataReader.Read())
                {
                    strReturn = Convert.ToString(objWK.MyDataReader[0]);
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                string e = ex.Message;
            }
            finally
            {
                objWK.MyDataReader.Close();
                objWK.MyConnection.Close();
            }
            return strReturn;
        }

        #endregion "Fetch COSTTPC"

        #endregion "Cargo Calculator"

        #region "Fetch Grid"

        public DataSet FetchGrid_Sea(long JobCardPk = 0, int process1 = 0, int cargotype = 0, int charges = 0)
        {
            DataSet ds = new DataSet();
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string strque = null;
            try
            {
                if (cargotype == 1)
                {
                    strque = "select *  from JOB_TRN_CONT where container_type_mst_fk is null and JOB_CARD_TRN_FK=" + JobCardPk;
                    ds = ObjWk.GetDataSet(strque);
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        cargotype = 1;
                        charges = 1;
                    }
                }

                if (process1 == 1)
                {
                    if (cargotype == 1)
                    {
                        strQuery.Append("SELECT rownum,");
                        strQuery.Append("       JSE.CONTAINER_NUMBER,");
                        strQuery.Append("       JSE.CONTAINER_NUMBER,");
                        strQuery.Append(" jse.container_type_mst_fk ,");
                        strQuery.Append("       PTMT.PACK_TYPE_DESC,");
                        strQuery.Append("       JSE.PACK_COUNT,");
                        strQuery.Append("       JSE.PACK_TYPE_MST_FK,");
                        strQuery.Append("       JSE.GROSS_WEIGHT,");
                        strQuery.Append("       JSE.CHARGEABLE_WEIGHT,");
                        strQuery.Append("       JSE.VOLUME_IN_CBM");
                        strQuery.Append("  FROM JOB_TRN_CONT   JSE,");
                        strQuery.Append("       PACK_TYPE_MST_TBL    PTMT");
                        strQuery.Append("  WHERE PTMT.PACK_TYPE_MST_PK(+)=JSE.Pack_Type_Mst_Fk");
                        strQuery.Append("   AND JSE.JOB_CARD_TRN_FK=" + JobCardPk);
                        strQuery.Append("");
                    }
                    else
                    {
                        strQuery.Append("SELECT rownum,");
                        strQuery.Append("       JSE.CONTAINER_NUMBER,");
                        strQuery.Append("       JSE.CONTAINER_NUMBER,");
                        strQuery.Append("       CTM.container_type_mst_id,");
                        strQuery.Append("       PTMT.PACK_TYPE_DESC,");
                        strQuery.Append("       JSE.PACK_COUNT,");
                        strQuery.Append("       JSE.PACK_TYPE_MST_FK,");
                        strQuery.Append("       JSE.GROSS_WEIGHT,");
                        if (charges == 1)
                        {
                            strQuery.Append("       JSE.CHARGEABLE_WEIGHT,");
                        }
                        else
                        {
                            strQuery.Append("       JSE.NET_WEIGHT,");
                        }
                        strQuery.Append("       JSE.VOLUME_IN_CBM");
                        strQuery.Append("  FROM JOB_TRN_CONT   JSE,");
                        strQuery.Append("       CONTAINER_TYPE_MST_TBL CTM,");
                        strQuery.Append("       JOB_CARD_TRN   JTSE,");
                        strQuery.Append("       PACK_TYPE_MST_TBL      PTMT,");
                        strQuery.Append("       JOB_TRN_COMMODITY  JCD");
                        strQuery.Append(" WHERE CTM.CONTAINER_TYPE_MST_PK =JSE.CONTAINER_TYPE_MST_FK");
                        strQuery.Append("   AND JTSE.JOB_CARD_TRN_PK = JSE.JOB_CARD_TRN_FK");
                        strQuery.Append("   AND JCD.JOB_TRN_CONT_FK(+) = JSE.JOB_TRN_CONT_PK");
                        strQuery.Append("   AND PTMT.PACK_TYPE_MST_PK(+) = JCD.PACK_TYPE_FK");
                        strQuery.Append("   AND JTSE.JOB_CARD_TRN_PK=" + JobCardPk);
                        strQuery.Append("   ORDER BY CTM.PREFERENCES");
                    }
                }
                else
                {
                    strQuery.Append(" SELECT rownum,");
                    strQuery.Append("                           JSE.CONTAINER_NUMBER,");
                    strQuery.Append("                           JSE.CONTAINER_NUMBER,");
                    strQuery.Append("                           CTM.container_type_mst_id,");
                    strQuery.Append("                           PTMT.PACK_TYPE_DESC,");
                    strQuery.Append("                           JSE.PACK_COUNT,");
                    strQuery.Append("                           JSE.PACK_TYPE_MST_FK,");
                    strQuery.Append("                           JSE.GROSS_WEIGHT,");
                    strQuery.Append("                           JSE.NET_WEIGHT,");
                    strQuery.Append("                           JSE.VOLUME_IN_CBM");
                    strQuery.Append("                      FROM JOB_TRN_CONT   JSE,");
                    strQuery.Append("                           CONTAINER_TYPE_MST_TBL CTM,");
                    strQuery.Append("                           JOB_CARD_TRN   JTSE,");
                    strQuery.Append("                           PACK_TYPE_MST_TBL      PTMT,");
                    strQuery.Append("                           JOB_TRN_COMMODITY JCD");
                    strQuery.Append("                     WHERE CTM.CONTAINER_TYPE_MST_PK =JSE.CONTAINER_TYPE_MST_FK");
                    strQuery.Append("                       AND JTSE.JOB_CARD_TRN_PK = JSE.JOB_CARD_TRN_FK");
                    strQuery.Append("                       AND JCD.JOB_TRN_CONT_FK(+) = JSE.JOB_TRN_CONT_PK ");
                    strQuery.Append("                       AND PTMT.PACK_TYPE_MST_PK(+) = JCD.PACK_TYPE_FK");
                    strQuery.Append("                       AND JTSE.JOB_CARD_TRN_PK=" + JobCardPk);
                    strQuery.Append("                       and JSE.CONTAINER_NUMBER is not null");
                    strQuery.Append("                       ORDER BY CTM.PREFERENCES");
                }

                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "Fetch Grid"

        #region "Fetch Chargable Weight And Net Weight "

        public DataSet Chargable_net_weight(long JobCardPk, int BizType, DataSet objDS, int process = 0, int CargoType = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                if (BizType == 1)
                {
                }
                else
                {
                    if (CargoType == 1)
                    {
                        if (process == 1)
                        {
                            strQuery.Append(" SELECT  BST.CARGO_TYPE,");
                            strQuery.Append("         JSE.GOODS_DESCRIPTION,");
                            strQuery.Append("         SUM(JCT.GROSS_WEIGHT)as GROSSW,");
                            strQuery.Append("         SUM(JCT.NET_WEIGHT)As NETW,");
                            strQuery.Append("         SUM(JCT.VOLUME_IN_CBM)as VOLINCBM ");
                            strQuery.Append("    FROM JOB_TRN_CONT JCT,");
                            strQuery.Append("         JOB_CARD_TRN JSE,");
                            strQuery.Append("         BOOKING_MST_TBL      BST");
                            strQuery.Append("   WHERE BST.BOOKING_MST_PK = JSE.BOOKING_MST_FK");
                            strQuery.Append("     AND JCT.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK");
                            strQuery.Append("     AND JSE.JOB_CARD_TRN_PK = " + JobCardPk);
                            strQuery.Append("   GROUP BY BST.CARGO_TYPE,");
                            strQuery.Append("            BST.COL_ADDRESS,");
                            strQuery.Append("            BST.DEL_ADDRESS,");
                            strQuery.Append("            JSE.GOODS_DESCRIPTION ");
                            strQuery.Append("");
                        }
                        else
                        {
                            strQuery.Append("            SELECT  jse.cargo_type,");
                            strQuery.Append("                             JSE.GOODS_DESCRIPTION,");
                            strQuery.Append("                             SUM(JCT.GROSS_WEIGHT)as GROSSW,");
                            strQuery.Append("                             SUM(JCT.NET_WEIGHT)As NETW,");
                            strQuery.Append("                             SUM(JCT.VOLUME_IN_CBM)as VOLINCBM ");
                            strQuery.Append("                        FROM JOB_TRN_CONT JCT,");
                            strQuery.Append("                             JOB_CARD_TRN JSE");
                            strQuery.Append("                       WHERE  JCT.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK");
                            strQuery.Append("                         AND JSE.JOB_CARD_TRN_PK =  " + JobCardPk);
                            strQuery.Append("                       GROUP BY JSE.GOODS_DESCRIPTION ,");
                            strQuery.Append("                              jse.cargo_type,");
                            strQuery.Append("                             JSE.GOODS_DESCRIPTION,");
                            strQuery.Append("                             JCT.GROSS_WEIGHT,");
                            strQuery.Append("                             JCT.NET_WEIGHT");
                        }
                    }
                    else
                    {
                        if (process == 1)
                        {
                            strQuery.Append(" SELECT  BST.CARGO_TYPE,");
                            strQuery.Append("         JSE.GOODS_DESCRIPTION,");
                            strQuery.Append("         SUM(JCT.GROSS_WEIGHT)as GROSSW,");
                            strQuery.Append("         SUM(jct.chargeable_weight)As NETW,");
                            strQuery.Append("         SUM(JCT.VOLUME_IN_CBM)as VOLINCBM ");
                            strQuery.Append("    FROM JOB_TRN_CONT JCT,");
                            strQuery.Append("         JOB_CARD_TRN JSE,");
                            strQuery.Append("         BOOKING_MST_TBL      BST");
                            strQuery.Append("   WHERE BST.BOOKING_MST_PK = JSE.BOOKING_MST_FK");
                            strQuery.Append("     AND JCT.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK");
                            strQuery.Append("     AND JSE.JOB_CARD_TRN_PK = " + JobCardPk);
                            strQuery.Append("   GROUP BY BST.CARGO_TYPE,");
                            strQuery.Append("            BST.COL_ADDRESS,");
                            strQuery.Append("            BST.DEL_ADDRESS,");
                            strQuery.Append("            JSE.GOODS_DESCRIPTION ");
                            strQuery.Append("");
                        }
                        else
                        {
                            strQuery.Append("            SELECT  jse.cargo_type,");
                            strQuery.Append("                             JSE.GOODS_DESCRIPTION,");
                            strQuery.Append("                             SUM(JCT.GROSS_WEIGHT)as GROSSW,");
                            strQuery.Append("                             SUM(jct.chargeable_weight)As NETW,");
                            strQuery.Append("                             SUM(JCT.VOLUME_IN_CBM)as VOLINCBM ");
                            strQuery.Append("                        FROM JOB_TRN_CONT JCT,");
                            strQuery.Append("                             JOB_CARD_TRN JSE");
                            strQuery.Append("                       WHERE  JCT.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK");
                            strQuery.Append("                         AND JSE.JOB_CARD_TRN_PK =  " + JobCardPk);
                            strQuery.Append("                       GROUP BY JSE.GOODS_DESCRIPTION ,");
                            strQuery.Append("                              jse.cargo_type,");
                            strQuery.Append("                             JSE.GOODS_DESCRIPTION,");
                            strQuery.Append("                             JCT.GROSS_WEIGHT,");
                            strQuery.Append("                             JCT.NET_WEIGHT");
                        }
                    }
                }
                objDS.Tables.Add(ObjWk.GetDataTable(strQuery.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new DataSet();
        }

        #endregion "Fetch Chargable Weight And Net Weight "

        #region "FETCH LOCATION WISE "

        public string FetchTransporter(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            int strBIZ_TYPE_IN = 0;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBIZ_TYPE_IN = Convert.ToInt32(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_TRANSPORTER_PKG.GETTRANSPORTER_LOCATION_WISE";

                var _with18 = selectCommand.Parameters;
                _with18.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with18.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with18.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with18.Add("BIZ_TYPE_IN", strBIZ_TYPE_IN).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        public string NEWFetchTransporter(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            int strBIZ_TYPE_IN = 0;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBIZ_TYPE_IN = Convert.ToInt32(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_TRANSPORTER_PKG.NEW_TRANSPORTER";

                var _with19 = selectCommand.Parameters;
                _with19.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with19.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with19.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with19.Add("BIZ_TYPE_IN", strBIZ_TYPE_IN).Direction = ParameterDirection.Input;
                _with19.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion "FETCH LOCATION WISE "

        #region "Fetch and Save CMR Details"

        #region "Fetch CMR Details"

        public DataSet FetchTRPTCMR(int TRPTPK, int BizType, int Process)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                objWF.MyCommand.Parameters.Clear();
                var _with20 = objWF.MyCommand.Parameters;
                _with20.Add("TRANSPORT_PK_IN", TRPTPK).Direction = ParameterDirection.Input;
                _with20.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with20.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with20.Add("TRANSPORT_CMR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                CMRDS = objWF.GetDataSet("TRANSPORT_INST_SEA_TBL_PKG", "FETCH_TRANSPORT_CMR");
                return CMRDS;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch CMR Details"

        #region "Save CMR Details"

        public ArrayList SaveCMR(DataSet CMRDS, bool IsEditing)
        {
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            WorkFlow objWF = new WorkFlow();
            OracleCommand insCMRCommand = new OracleCommand();
            OracleCommand updCMRCommand = new OracleCommand();
            try
            {
                objWF.OpenConnection();
                TRAN = objWF.MyConnection.BeginTransaction();
                var _with21 = insCMRCommand;
                _with21.Connection = objWF.MyConnection;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWF.MyUserName + ".TRANSPORT_INST_SEA_TBL_PKG.INS_TRANSPORT_CMR";
                var _with22 = _with21.Parameters;
                insCMRCommand.Parameters.Add("TRANSPORT_NOTE_FK_IN", OracleDbType.Int32, 10, "TRANSPORT_NOTE_FK").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRANSPORT_NOTE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_SHIPPER_NAME_IN", OracleDbType.Varchar2, 50, "TRPT_CMR_SHIPPER_NAME").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_SHIPPER_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_SHIPPER_ADD_IN", OracleDbType.Varchar2, 200, "TRPT_CMR_SHIPPER_ADD").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_SHIPPER_ADD_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_CONSIGNEE_NAME_IN", OracleDbType.Varchar2, 50, "TRPT_CMR_CONSIGNEE_NAME").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_CONSIGNEE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_CONSIGNEE_ADD_IN", OracleDbType.Varchar2, 200, "TRPT_CMR_CONSIGNEE_ADD").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_CONSIGNEE_ADD_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_DOC_INVOICE_IN", OracleDbType.Varchar2, 1, "TRPT_CMR_DOC_INVOICE").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_DOC_INVOICE_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_DOC_INS_COST_IN", OracleDbType.Int32, 1, "TRPT_CMR_DOC_INS_COST").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_DOC_INS_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_DOC_CUST_INV_IN", OracleDbType.Int32, 1, "TRPT_CMR_DOC_CUST_INV").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_DOC_CUST_INV_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_DOC_PACK_LIST_IN", OracleDbType.Int32, 1, "TRPT_CMR_DOC_PACK_LIST").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_DOC_PACK_LIST_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_PL_ORI_COPY_IN", OracleDbType.Int32, 1, "TRPT_CMR_PL_ORI_COPY").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_PL_ORI_COPY_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_DOC_CERT_ORIGIN_IN", OracleDbType.Int32, 1, "TRPT_CMR_DOC_CERT_ORIGIN").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_DOC_CERT_ORIGIN_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_CMR_CO_ORI_COPY_IN", OracleDbType.Int32, 1, "TRPT_CMR_CO_ORI_COPY").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_CMR_CO_ORI_COPY_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_BIZ_TYPE_IN", OracleDbType.Int32, 1, "TRPT_BIZ_TYPE").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("TRPT_PROCESS_IN", OracleDbType.Int32, 1, "TRPT_PROCESS").Direction = ParameterDirection.Input;
                insCMRCommand.Parameters["TRPT_PROCESS_IN"].SourceVersion = DataRowVersion.Current;

                insCMRCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "TRPT_CMR_PRINT_PK").Direction = ParameterDirection.Output;
                insCMRCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with23 = updCMRCommand;
                _with23.Connection = objWF.MyConnection;
                _with23.CommandType = CommandType.StoredProcedure;
                _with23.CommandText = objWF.MyUserName + ".TRANSPORT_INST_SEA_TBL_PKG.UPD_TRANSPORT_CMR";
                var _with24 = _with23.Parameters;
                updCMRCommand.Parameters.Add("TRPT_CMR_PRINT_PK_IN", OracleDbType.Int32, 10, "TRPT_CMR_PRINT_PK").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_PRINT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRANSPORT_NOTE_FK_IN", OracleDbType.Int32, 10, "TRANSPORT_NOTE_FK").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRANSPORT_NOTE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_SHIPPER_NAME_IN", OracleDbType.Varchar2, 50, "TRPT_CMR_SHIPPER_NAME").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_SHIPPER_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_SHIPPER_ADD_IN", OracleDbType.Varchar2, 200, "TRPT_CMR_SHIPPER_ADD").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_SHIPPER_ADD_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_CONSIGNEE_NAME_IN", OracleDbType.Varchar2, 50, "TRPT_CMR_CONSIGNEE_NAME").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_CONSIGNEE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_CONSIGNEE_ADD_IN", OracleDbType.Varchar2, 200, "TRPT_CMR_CONSIGNEE_ADD").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_CONSIGNEE_ADD_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_DOC_INVOICE_IN", OracleDbType.Varchar2, 1, "TRPT_CMR_DOC_INVOICE").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_DOC_INVOICE_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_DOC_INS_COST_IN", OracleDbType.Int32, 1, "TRPT_CMR_DOC_INS_COST").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_DOC_INS_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_DOC_CUST_INV_IN", OracleDbType.Int32, 1, "TRPT_CMR_DOC_CUST_INV").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_DOC_CUST_INV_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_DOC_PACK_LIST_IN", OracleDbType.Int32, 1, "TRPT_CMR_DOC_PACK_LIST").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_DOC_PACK_LIST_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_PL_ORI_COPY_IN", OracleDbType.Int32, 1, "TRPT_CMR_PL_ORI_COPY").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_PL_ORI_COPY_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_DOC_CERT_ORIGIN_IN", OracleDbType.Int32, 1, "TRPT_CMR_DOC_CERT_ORIGIN").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_DOC_CERT_ORIGIN_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("TRPT_CMR_CO_ORI_COPY_IN", OracleDbType.Int32, 1, "TRPT_CMR_CO_ORI_COPY").Direction = ParameterDirection.Input;
                updCMRCommand.Parameters["TRPT_CMR_CO_ORI_COPY_IN"].SourceVersion = DataRowVersion.Current;

                updCMRCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "TRPT_CMR_PRINT_PK").Direction = ParameterDirection.Output;
                updCMRCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with25 = objWF.MyDataAdapter;
                _with25.InsertCommand = insCMRCommand;
                _with25.InsertCommand.Transaction = TRAN;

                _with25.UpdateCommand = updCMRCommand;
                _with25.UpdateCommand.Transaction = TRAN;

                RecAfct = _with25.Update(CMRDS);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    if (IsEditing == false)
                    {
                        CMRPK = Convert.ToInt32(insCMRCommand.Parameters["RETURN_VALUE"].Value);
                    }
                    else
                    {
                        CMRPK = Convert.ToInt32(CMRDS.Tables[0].Rows[0]["TRPT_CMR_PRINT_PK"]);
                    }
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
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
            finally
            {
                objWF.CloseConnection();
            }
        }

        #endregion "Save CMR Details"

        #endregion "Fetch and Save CMR Details"

        #region "CMRReportMainQry"

        public DataSet fetchCMRreport(int PK, long Loc, Int16 Process = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                if (Process == 1)
                {
                    sb.Append("select j.JOB_CARD_TRN_PK JOB_CARD_FK,");
                    sb.Append("       lmt.location_name ISSUEDAT,");
                    sb.Append("       cship.customer_name JC_CMR_SHIPPER_NAME,");
                    sb.Append("       contship.adm_address_1 JC_CMR_SHIPPER_ADD,");
                    sb.Append("       j.jobcard_ref_no JOB_CARD_REF,");
                    sb.Append("       ccon.customer_name JC_CMR_CONSIGNEE_NAME,");
                    sb.Append("       contcon.adm_address_1 JC_CMR_CONSIGNEE_ADD,");
                    sb.Append("       JCITDE.TRANSPORT_INST_SEA_PK JOB_CARD_PFD_FK,");
                    sb.Append("       JCITDE.DELIVERY_OFF_ADDRESS JOB_CARD_PFD_ADDR1,");
                    sb.Append("       '' JOB_CARD_PFD_ADDR2,");
                    sb.Append("       '' JOB_CARD_PFD_CITY,");
                    sb.Append("       '' JOB_CARD_PFD_ZIP,");
                    sb.Append("       '' JOB_CARD_PFD_COUNTRY_FK,");
                    sb.Append("       CMT.COUNTRY_NAME,");
                    sb.Append("       op.operator_id CARRIER_ID,");
                    sb.Append("       '' VECHICLEREG_NO,");
                    sb.Append("       TO_CHAR(j.arrival_date, DATEFORMAT) DATE_DEPARTURE,");
                    sb.Append("       DECODE(JCTRDE.TRANSPORT_MODE, '1', 'Air', '2', 'Sea', '3', 'Land') TRANS_MODE,");
                    sb.Append("       pol.Port_mst_pk POLPK,");
                    sb.Append("       (CASE");
                    sb.Append("         WHEN JCTRDE.TRANSPORT_MODE = 3 THEN");
                    sb.Append("          PLR.PORT_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          POL.PORT_ID");
                    sb.Append("       END) POL,");
                    sb.Append("       pod.Port_mst_pk PODPK,");
                    sb.Append("       (CASE");
                    sb.Append("         WHEN JCTRDE.TRANSPORT_MODE = 3 THEN");
                    sb.Append("          PFD.PORT_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          POD.PORT_ID");
                    sb.Append("       END) POD,");
                    sb.Append("       '' VESSEL,");
                    sb.Append("       POD.PORT_NAME FINALDESTINATION,");
                    sb.Append("       '' TERMSOF_TRANSPORT,");
                    sb.Append("       SUM(JCITDE.NET_WEIGHT) WEIGHT,");
                    sb.Append("       SUM(JCITDE.VOLUME) VOLUME,");
                    sb.Append("       '' JC_CARRIER_INST,");
                    sb.Append("       '' JC_MVMT_CERT_NR,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_INVOICE JC_CMR_DOC_INVOICE,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_INS_COST JC_CMR_DOC_INS_COST,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_CUST_INV JC_CMR_DOC_CUST_INV,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_PACK_LIST JC_CMR_DOC_PACK_LIST,");
                    sb.Append("       JCCMR.TRPT_CMR_PL_ORI_COPY JC_CMR_PL_ORI_COPY,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_CERT_ORIGIN JC_CMR_DOC_CERT_ORIGIN,");
                    sb.Append("       JCCMR.TRPT_CMR_CO_ORI_COPY JC_CMR_CO_ORI_COPY");
                    sb.Append(" from JOB_CARD_TRN   j,");
                    sb.Append("       BOOKING_MST_TBL        b,");
                    sb.Append("       customer_mst_tbl       cship,");
                    sb.Append("       customer_mst_tbl       ccon,");
                    sb.Append("       customer_contact_dtls  contship,");
                    sb.Append("       customer_contact_dtls  contcon,");
                    sb.Append("       TRANSPORT_INST_SEA_TBL JCITDE,");
                    sb.Append("       TRPT_CMR_PRINT_TBL     JCCMR,");
                    sb.Append("       country_mst_tbl        cmt,");
                    sb.Append("       CONT_TRANS_FCL_TBL     JCTRDE,");
                    sb.Append("       port_mst_tbl           plr,");
                    sb.Append("       port_mst_tbl           pfd,");
                    sb.Append("       port_mst_tbl           pol,");
                    sb.Append("       port_mst_tbl           pod,");
                    sb.Append("       operator_mst_tbl       op,");
                    sb.Append("       location_mst_tbl       lmt,");
                    sb.Append("       user_mst_tbl           u");
                    sb.Append(" where b.BOOKING_MST_PK = j.BOOKING_MST_FK");
                    sb.Append("   and b.cust_customer_mst_fk = cship.customer_mst_pk");
                    sb.Append("   and ccon.customer_mst_pk = J.CONSIGNEE_CUST_MST_FK");
                    sb.Append("   and ccon.customer_mst_pk = contcon.customer_mst_fk");
                    sb.Append("   and cship.customer_mst_pk = contship.customer_mst_fk");
                    sb.Append("   and JCITDE.JOB_CARD_FK = j.JOB_CARD_TRN_PK");
                    sb.Append("   and JCITDE.BUSINESS_TYPE = 2");
                    sb.Append("   and JCITDE.PROCESS_TYPE = 1");
                    sb.Append("   and jccmr.transport_note_fk = JCITDE.TRANSPORT_INST_SEA_PK");
                    sb.Append("   and cship.country_mst_fk = cmt.country_mst_pk(+)");
                    sb.Append("   and JCTRDE.TRANSPORTER_MST_FK = JCITDE.TRANSPORTER_MST_FK");
                    sb.Append("   and plr.port_mst_pk(+) = b.col_place_mst_fk");
                    sb.Append("   and pfd.port_mst_pk(+) = b.del_place_mst_fk");
                    sb.Append("   and pol.port_mst_pk(+) = b.port_mst_pol_fk");
                    sb.Append("   and pod.port_mst_pk(+) = b.port_mst_pod_fk");
                    sb.Append("   and op.operator_mst_pk(+) = b.CARRIER_MST_FK");
                    sb.Append("   and u.user_mst_pk = JCITDE.CREATED_BY_FK");
                    sb.Append("   and u.default_location_fk = lmt.location_mst_pk");
                    sb.Append("   and j.JOB_CARD_TRN_PK = " + PK + "");
                    sb.Append(" group by j.JOB_CARD_TRN_PK,");
                    sb.Append("          j.jobcard_ref_no,");
                    sb.Append("          cship.customer_name,");
                    sb.Append("          ccon.customer_name,");
                    sb.Append("          contship.adm_address_1,");
                    sb.Append("          contcon.adm_address_1,");
                    sb.Append("          JCITDE.TRANSPORT_INST_SEA_PK,");
                    sb.Append("          JCITDE.DELIVERY_OFF_ADDRESS,");
                    sb.Append("          CMT.COUNTRY_NAME,");
                    sb.Append("          JCTRDE.TRANSPORT_MODE,");
                    sb.Append("          pol.Port_mst_pk,");
                    sb.Append("          op.operator_id,");
                    sb.Append("          j.arrival_date,");
                    sb.Append("          POD.PORT_NAME,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_INVOICE,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_INS_COST,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_CUST_INV,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_PACK_LIST,");
                    sb.Append("          JCCMR.TRPT_CMR_PL_ORI_COPY,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_CERT_ORIGIN,");
                    sb.Append("          JCCMR.TRPT_CMR_CO_ORI_COPY,");
                    sb.Append("          PLR.PORT_NAME,");
                    sb.Append("          POL.PORT_ID,");
                    sb.Append("          pod.Port_mst_pk,");
                    sb.Append("          PFD.PORT_NAME,");
                    sb.Append("          POD.PORT_ID,");
                    sb.Append("          lmt.location_name");
                }
                else
                {
                    sb.Append("select j.JOB_CARD_TRN_PK JOB_CARD_FK,");
                    sb.Append("       lmt.location_name ISSUEDAT,");
                    sb.Append("       cship.customer_name JC_CMR_SHIPPER_NAME,");
                    sb.Append("       contship.adm_address_1 JC_CMR_SHIPPER_ADD,");
                    sb.Append("       j.jobcard_ref_no JOB_CARD_REF,");
                    sb.Append("       ccon.customer_name JC_CMR_CONSIGNEE_NAME,");
                    sb.Append("       contcon.adm_address_1 JC_CMR_CONSIGNEE_ADD,");
                    sb.Append("       JCITDE.TRANSPORT_INST_SEA_PK JOB_CARD_PFD_FK,");
                    sb.Append("       JCITDE.DELIVERY_OFF_ADDRESS JOB_CARD_PFD_ADDR1,");
                    sb.Append("       '' JOB_CARD_PFD_ADDR2,");
                    sb.Append("       '' JOB_CARD_PFD_CITY,");
                    sb.Append("       '' JOB_CARD_PFD_ZIP,");
                    sb.Append("       '' JOB_CARD_PFD_COUNTRY_FK,");
                    sb.Append("       CMT.COUNTRY_NAME,");
                    sb.Append("       OP.OPERATOR_NAME CARRIER_ID,");
                    sb.Append("       '' VECHICLEREG_NO,");
                    sb.Append("       TO_CHAR(j.arrival_date, DATEFORMAT) DATE_DEPARTURE,");
                    sb.Append("       DECODE(JCTRDE.TRANSPORT_MODE, '1', 'Air', '2', 'Sea', '3', 'Land') TRANS_MODE,");
                    sb.Append("       pol.Port_mst_pk POLPK,");
                    sb.Append("       (CASE");
                    sb.Append("         WHEN JCTRDE.TRANSPORT_MODE = 3 THEN");
                    sb.Append("          POL.PORT_ID");
                    sb.Append("         ELSE");
                    sb.Append("          POL.PORT_ID");
                    sb.Append("       END) POL,");
                    sb.Append("       pod.Port_mst_pk PODPK,");
                    sb.Append("       (CASE");
                    sb.Append("         WHEN JCTRDE.TRANSPORT_MODE = 3 THEN");
                    sb.Append("          PFD.PORT_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          POD.PORT_ID");
                    sb.Append("       END) POD,");
                    sb.Append("       '' VESSEL,");
                    sb.Append("       POD.PORT_NAME FINALDESTINATION,");
                    sb.Append("       '' TERMSOF_TRANSPORT,");
                    sb.Append("       SUM(DISTINCT JCITDE.NET_WEIGHT) WEIGHT,");
                    sb.Append("       SUM(DISTINCT JCITDE.VOLUME) VOLUME,");
                    sb.Append("       '' JC_CARRIER_INST,");
                    sb.Append("       '' JC_MVMT_CERT_NR,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_INVOICE JC_CMR_DOC_INVOICE,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_INS_COST JC_CMR_DOC_INS_COST,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_CUST_INV JC_CMR_DOC_CUST_INV,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_PACK_LIST JC_CMR_DOC_PACK_LIST,");
                    sb.Append("       JCCMR.TRPT_CMR_PL_ORI_COPY JC_CMR_PL_ORI_COPY,");
                    sb.Append("       JCCMR.TRPT_CMR_DOC_CERT_ORIGIN JC_CMR_DOC_CERT_ORIGIN,");
                    sb.Append("       JCCMR.TRPT_CMR_CO_ORI_COPY JC_CMR_CO_ORI_COPY");
                    sb.Append("  from JOB_CARD_TRN   j,");
                    sb.Append("       customer_mst_tbl       cship,");
                    sb.Append("       customer_mst_tbl       ccon,");
                    sb.Append("       customer_mst_tbl       CPFD,");
                    sb.Append("       customer_contact_dtls  contship,");
                    sb.Append("       customer_contact_dtls  contcon,");
                    sb.Append("       customer_contact_dtls  contPFD,");
                    sb.Append("       TRANSPORT_INST_SEA_TBL JCITDE,");
                    sb.Append("       TRPT_CMR_PRINT_TBL     JCCMR,");
                    sb.Append("       country_mst_tbl        cmt,");
                    sb.Append("       CONT_TRANS_FCL_TBL     JCTRDE,");
                    sb.Append("       port_mst_tbl           pfd,");
                    sb.Append("       port_mst_tbl           pol,");
                    sb.Append("       port_mst_tbl           pod,");
                    sb.Append("       operator_mst_tbl       op,");
                    sb.Append("       location_mst_tbl       lmt,");
                    sb.Append("       user_mst_tbl           u");
                    sb.Append(" where j.cust_customer_mst_fk = cship.customer_mst_pk");
                    sb.Append("   and ccon.customer_mst_pk = j.consignee_cust_mst_fk");
                    sb.Append("   and ccon.customer_mst_pk = contcon.customer_mst_fk");
                    sb.Append("   and ccon.customer_mst_pk = J.CONSIGNEE_CUST_MST_FK");
                    sb.Append("   and cship.customer_mst_pk = contship.customer_mst_fk");
                    sb.Append("   and JCITDE.JOB_CARD_FK = j.JOB_CARD_TRN_PK");
                    sb.Append("   and JCITDE.BUSINESS_TYPE = 2");
                    sb.Append("   and JCITDE.PROCESS_TYPE = 2");
                    sb.Append("   and jccmr.transport_note_fk = JCITDE.TRANSPORT_INST_SEA_PK");
                    sb.Append("   and cship.country_mst_fk = cmt.country_mst_pk(+)");
                    sb.Append("   and JCTRDE.TRANSPORTER_MST_FK(+) = JCITDE.TRANSPORTER_MST_FK");
                    sb.Append("   and pfd.port_mst_pk(+) = j.del_place_mst_fk");
                    sb.Append("   and pol.port_mst_pk(+) = j.port_mst_pol_fk");
                    sb.Append("   and pod.port_mst_pk(+) = j.port_mst_pod_fk");
                    sb.Append("   and op.operator_mst_pk(+) =j.CARRIER_MST_FK");
                    sb.Append("   and u.user_mst_pk = JCITDE.CREATED_BY_FK");
                    sb.Append("   and u.default_location_fk = lmt.location_mst_pk");
                    sb.Append("   and j.JOB_CARD_TRN_PK = " + PK + "");
                    sb.Append("   group by j.JOB_CARD_TRN_PK,");
                    sb.Append("          j.jobcard_ref_no,");
                    sb.Append("          cship.customer_name,");
                    sb.Append("          ccon.customer_name,");
                    sb.Append("          contship.adm_address_1,");
                    sb.Append("          contcon.adm_address_1,");
                    sb.Append("          JCITDE.TRANSPORT_INST_SEA_PK,");
                    sb.Append("          JCITDE.DELIVERY_OFF_ADDRESS,");
                    sb.Append("          CMT.COUNTRY_NAME,");
                    sb.Append("          JCTRDE.TRANSPORT_MODE,");
                    sb.Append("          pol.Port_mst_pk,");
                    sb.Append("          OP.OPERATOR_NAME,");
                    sb.Append("          j.arrival_date,");
                    sb.Append("          POD.PORT_NAME,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_INVOICE,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_INS_COST,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_CUST_INV,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_PACK_LIST,");
                    sb.Append("          JCCMR.TRPT_CMR_PL_ORI_COPY,");
                    sb.Append("          JCCMR.TRPT_CMR_DOC_CERT_ORIGIN,");
                    sb.Append("          JCCMR.TRPT_CMR_CO_ORI_COPY,");
                    sb.Append("          POL.PORT_ID,");
                    sb.Append("          pod.Port_mst_pk,");
                    sb.Append("          PFD.PORT_NAME,");
                    sb.Append("          POD.PORT_ID,");
                    sb.Append("          lmt.location_name");
                }
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "CMRReportMainQry"

        #region "fetchMarksNos"

        public DataSet fetchMarksNos(int PK, int ViaMode, string ItemViaMode, bool CMRPrint = false, Int16 Process = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                if (Process == 1)
                {
                    sb.Append(" SELECT distinct DECODE(JCTRDE.TRANSPORT_MODE, '1', 'Air', '2', 'Sea', '3', 'Both') TRANS_MODE,");
                    sb.Append("       '' VIA_MODE,");
                    sb.Append("       JCMST.MARKS_NUMBERS JC_MARKS_NOS,");
                    sb.Append("       JCONT.PACK_TYPE_MST_FK PACK_TYPE_FK,");
                    sb.Append("       PTMT.PACK_TYPE_ID TYPEPACKAGES,");
                    sb.Append("       JCONT.PACK_COUNT NOOFPACKAGES,");
                    sb.Append("       JCMST.GOODS_DESCRIPTION GOODDESC");
                    sb.Append("  FROM JOB_CARD_TRN   JCMST,");
                    sb.Append("       CONT_TRANS_FCL_TBL     JCTRDE,");
                    sb.Append("       TRANSPORT_INST_SEA_TBL JCITDE,");
                    sb.Append("       PACK_TYPE_MST_TBL       PTMT,");
                    sb.Append("       JOB_TRN_CONT   JCONT");
                    sb.Append(" WHERE JCMST.JOB_CARD_TRN_PK = JCITDE.JOB_CARD_FK");
                    sb.Append("   AND JCTRDE.TRANSPORTER_MST_FK = JCITDE.TRANSPORTER_MST_FK");
                    sb.Append("   AND JCITDE.JOB_CARD_FK  = JCMST.JOB_CARD_TRN_PK");
                    sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCMST.JOB_CARD_TRN_PK");
                    sb.Append("   AND JCONT.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK");
                    sb.Append("   and (to_date(JCTRDE.CONTRACT_DATE,'" + dateFormat + "')  between to_date(JCTRDE.Valid_From,'" + dateFormat + "') and to_date(JCTRDE.Valid_To,'" + dateFormat + "')");
                    sb.Append("   or  (JCTRDE.Valid_To is null))");
                    sb.Append("   AND JCMST.JOB_CARD_TRN_PK = " + PK + "");
                }
                else
                {
                    sb.Append("SELECT distinct DECODE(JCTRDE.TRANSPORT_MODE,");
                    sb.Append("                       '1',");
                    sb.Append("                       'Air',");
                    sb.Append("                       '2',");
                    sb.Append("                       'Sea',");
                    sb.Append("                       '3',");
                    sb.Append("                       'Both') TRANS_MODE,");
                    sb.Append("                '' VIA_MODE,");
                    sb.Append("                JCMST.MARKS_NUMBERS JC_MARKS_NOS,");
                    sb.Append("                JCONT.PACK_TYPE_MST_FK PACK_TYPE_FK,");
                    sb.Append("                PTMT.PACK_TYPE_ID TYPEPACKAGES,");
                    sb.Append("                JCONT.PACK_COUNT NOOFPACKAGES,");
                    sb.Append("                JCMST.GOODS_DESCRIPTION GOODDESC");
                    sb.Append("  FROM JOB_CARD_TRN   JCMST,");
                    sb.Append("       CONT_TRANS_FCL_TBL     JCTRDE,");
                    sb.Append("       TRANSPORT_INST_SEA_TBL JCITDE,");
                    sb.Append("       PACK_TYPE_MST_TBL      PTMT,");
                    sb.Append("       JOB_TRN_CONT   JCONT");
                    sb.Append(" WHERE JCMST.JOB_CARD_TRN_PK = JCITDE.JOB_CARD_FK");
                    sb.Append("  AND JCTRDE.TRANSPORTER_MST_FK = JCITDE.TRANSPORTER_MST_FK");
                    sb.Append("   AND JCITDE.JOB_CARD_FK = JCMST.JOB_CARD_TRN_PK");
                    sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCMST.JOB_CARD_TRN_PK");
                    sb.Append("   AND JCONT.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK");
                    sb.Append("   and (to_date(JCTRDE.CONTRACT_DATE, 'dd/MM/yyyy') between");
                    sb.Append("       to_date(JCTRDE.Valid_From, 'dd/MM/yyyy') and");
                    sb.Append("       to_date(JCTRDE.Valid_To, 'dd/MM/yyyy') or (JCTRDE.Valid_To is null))");
                    sb.Append("   AND JCMST.JOB_CARD_TRN_PK = " + PK + "");
                }

                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "fetchMarksNos"

        #region "FetchClauseCMRSubReport"

        public DataSet FetchClauseCMRSubReport(int PK, Int16 Process = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                if (Process == 1)
                {
                    sb.Append("");
                    sb.Append("SELECT BLMST.BL_CLAUSE_PK   CUSTOM_CLAUSE_MST_PK,");
                    sb.Append("       BLMST.BL_DESCRIPTION CUSTOM_CLAUSE_DESCRIPTION");
                    sb.Append("  FROM BL_CLAUSE_TBL BLMST, BL_CLAUSE_TRN BLTRN, PORT_MST_TBL POD");
                    sb.Append(" WHERE BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK");
                    sb.Append("   AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK");
                    sb.Append("   AND POD.PORT_MST_PK in");
                    sb.Append("       (select bkg.port_mst_pod_fk");
                    sb.Append("          from BOOKING_MST_TBL bkg, JOB_CARD_TRN JC");
                    sb.Append("         where jc.BOOKING_MST_FK = bkg.BOOKING_MST_PK");
                    sb.Append("           and jc.JOB_CARD_TRN_PK = " + PK + ")");
                    sb.Append("AND BLMST.ACTIVE_FLAG = 1");
                    sb.Append(" UNION ");
                    sb.Append("SELECT MBL.BL_CLAUSE_FK   CUSTOM_CLAUSE_MST_PK,");
                    sb.Append("       MBL.BL_DESCRIPTION CUSTOM_CLAUSE_DESCRIPTION");
                    sb.Append("  FROM MBL_BL_CLAUSE_TBL MBL, BL_CLAUSE_TBL BLMST, JOB_CARD_TRN JC");
                    sb.Append(" WHERE JC.MBL_MAWB_FK = MBL.MBL_EXP_TBL_FK");
                    sb.Append("   and JC.JOB_CARD_TRN_PK = " + PK + "");
                    sb.Append(" UNION ");
                    sb.Append("SELECT MBL.BL_CLAUSE_FK   CUSTOM_CLAUSE_MST_PK,");
                    sb.Append("       MBL.BL_DESCRIPTION CUSTOM_CLAUSE_DESCRIPTION");
                    sb.Append("  FROM MBL_BL_CLAUSE_TBL MBL, JOB_CARD_TRN JC");
                    sb.Append(" WHERE jc.HBL_HAWB_FK = MBL.MBL_EXP_TBL_FK");
                    sb.Append("   and JC.JOB_CARD_TRN_PK = " + PK + "");
                    sb.Append(" order by CUSTOM_CLAUSE_DESCRIPTION");
                }
                else
                {
                    sb.Append("SELECT BLMST.BL_CLAUSE_PK   CUSTOM_CLAUSE_MST_PK,");
                    sb.Append("       BLMST.BL_DESCRIPTION CUSTOM_CLAUSE_DESCRIPTION");
                    sb.Append("  FROM BL_CLAUSE_TBL BLMST, BL_CLAUSE_TRN BLTRN, PORT_MST_TBL POD");
                    sb.Append(" WHERE BLTRN.BL_CLAUSE_FK = BLMST.BL_CLAUSE_PK");
                    sb.Append("   AND BLTRN.PORT_MST_FK = POD.PORT_MST_PK");
                    sb.Append("   AND POD.PORT_MST_PK in");
                    sb.Append("       (select JC.Port_Mst_Pod_Fk");
                    sb.Append("          from  JOB_CARD_TRN JC");
                    sb.Append("         where jc.JOB_CARD_TRN_PK = " + PK + ")");
                    sb.Append("   AND BLMST.ACTIVE_FLAG = 1");
                    sb.Append(" UNION ");
                    sb.Append("SELECT MBL.BL_CLAUSE_FK   CUSTOM_CLAUSE_MST_PK,");
                    sb.Append("       MBL.BL_DESCRIPTION CUSTOM_CLAUSE_DESCRIPTION");
                    sb.Append("  FROM MBL_BL_CLAUSE_TBL MBL, BL_CLAUSE_TBL BLMST, JOB_CARD_TRN JC");
                    sb.Append(" WHERE JC.MBL_MAWB_FK = MBL.MBL_EXP_TBL_FK");
                    sb.Append("   and JC.JOB_CARD_TRN_PK = " + PK + "");
                }

                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "FetchClauseCMRSubReport"

        #region "Fetch Containers"

        public DataSet FetchContainersNew(int BizType, int Process, int JobPK, string truck)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                if (BizType == 2)
                {
                    sb.Append("  SELECT CONT.CONTAINER_NO");
                }
                else
                {
                    sb.Append("  SELECT CONT.PALETTE_SIZE CONTAINER_NO");
                }
                sb.Append("     FROM TRANSPORT_INST_SEA_TBL TIST, TRANSPORT_TRN_CONT CONT , TRANSPORT_TRN_TRUCK TTT");
                sb.Append("    WHERE TIST.TRANSPORT_INST_SEA_PK=" + JobPK);
                sb.Append("     AND TIST.TRANSPORT_INST_SEA_PK = CONT.TRANSPORT_INST_FK");
                sb.Append("     AND CONT.TRANSPORT_TRN_CONT_PK = TTT.TRANSPORT_TRN_CONT_FK(+)");
                sb.Append("     AND TTT.TD_TRUCK_NUMBER='" + truck + "'");
                sb.Append("     AND TIST.BUSINESS_TYPE= " + BizType);

                WorkFlow objwf = new WorkFlow();
                return objwf.GetDataSet(sb.ToString());
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

        public DataSet FetchCustomerNew(int CustPK)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with26 = objWK.MyCommand;
                _with26.CommandType = CommandType.StoredProcedure;
                _with26.CommandText = objWK.MyUserName + ".CUSTOMS_TRANSPORTNOTE_PKG.TRUCK_REPORT_CUSTOMER";

                objWK.MyCommand.Parameters.Clear();
                var _with27 = objWK.MyCommand.Parameters;

                _with27.Add("CUSTPK_IN", CustPK).Direction = ParameterDirection.Input;
                _with27.Add("LOCATION_PK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with27.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Fetch Containers"

        #region "GET JOBPK"

        public int GetJCPK(string BKGPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append(" SELECT T.JOB_CARD_TRN_PK FROM ");
            StrSqlBuilder.Append(" JOB_CARD_TRN T WHERE T.BOOKING_MST_FK =" + BKGPK);
            try
            {
                return Convert.ToInt32(ObjWk.ExecuteScaler(StrSqlBuilder.ToString()));
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

        #endregion "GET JOBPK"

        #region "Fetch Location of User Login"

        public DataSet FetchGoodsDesc1(Int32 chkFlag)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ROWNUM SL_NO,B.MARKS,");
            sb.Append("       B.CARTTON_NR,");
            sb.Append("       B.QUANTITTY,");
            sb.Append("       B.GOODS_DECRIPTION,");
            sb.Append("       C.COUNTRY_ID,");
            sb.Append("       B.LENGTH ||'X' || B.HEIGHT || 'X' || B.WIDTH DIMENSIONCM,");
            sb.Append("       B.HEIGHT,");
            sb.Append("       B.WIDTH,");
            sb.Append("       B.NET_WEIGHT,");
            sb.Append("      B.GROSS_WEIGHT,");
            sb.Append("       B.VOLUME");
            sb.Append("  FROM BOOKING_TRN_COMMINV_DTL B ,COUNTRY_MST_TBL C");
            if (chkFlag == 1)
            {
                sb.Append("  WHERE C.COUNTRY_MST_PK = B.COUNTRY_MST_FK ORDER BY SL_NO ASC");
            }
            else
            {
                sb.Append("  WHERE C.COUNTRY_MST_PK = B.COUNTRY_MST_FK ORDER BY B.CARTTON_NR ASC");
            }
            //sb.Append("  WHERE C.COUNTRY_MST_PK = B.COUNTRY_MST_FK ORDER BY B.CARTTON_NR ASC")
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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

        public DataSet ComInvoiceGoodsDesc(long BookingPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            sb.Append(" SELECT ROWNUM SLNR, Q.* FROM (");
            sb.Append(" SELECT  BTRN.BOOKING_TRN_COMMINV_PK,");
            sb.Append("             BTRN.GOODS_DECRIPTION,");
            sb.Append("             BTRN.COUNTRY_MST_FK,");
            sb.Append("             CONT.COUNTRY_ID,");
            //sb.Append("             BTRN.QUANTITTY ||'   '|| QDDT.DD_ID QUANTITTY,")
            sb.Append("             BTRN.QUANTITTY QUANTITTY,");
            sb.Append("             QDDT.DD_ID DD_ID,");
            sb.Append("             BTRN.UNIT_PRICE,");
            sb.Append("             (BTRN.QUANTITTY * BTRN.UNIT_PRICE) *");
            sb.Append("             GET_EX_RATE(BTRN.CURRENCY_MST_FK,");
            sb.Append("                         '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "',");
            sb.Append("                         BTRN.COMM_INV_DT) AMOUNT");
            sb.Append("            ");
            sb.Append("        FROM BOOKING_TRN_COMMINV_DTL BTRN,");
            sb.Append("             BOOKING_MST_TBL         BMT,");
            sb.Append("             COUNTRY_MST_TBL         CONT,");
            sb.Append("             QFOR_DROP_DOWN_TBL      QDDT");
            sb.Append("       WHERE BMT.BOOKING_MST_PK = BTRN.BOOKING_MST_FK");
            sb.Append("         AND BTRN.COUNTRY_MST_FK = CONT.COUNTRY_MST_PK");
            sb.Append("         AND BTRN.DIMENTION_UNIT_MST_FK = QDDT.DD_VALUE");
            sb.Append("         AND QDDT.DD_FLAG = 'UNIT'");
            sb.Append("         AND QDDT.CONFIG_ID = 'QFOR4459'");
            sb.Append("         AND BTRN.BOOKING_MST_FK = " + BookingPK);
            sb.Append("        ORDER BY BTRN.GOODS_DECRIPTION) Q");
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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

        public DataSet PackageInvoiceGoodsDesc(long BookingPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ROWNUM SLNR,");
            sb.Append("             BTRN.BOOKING_TRN_COMMINV_PK,");
            sb.Append("             BTRN.MARKS,");
            sb.Append("             BTRN.CARTTON_NR,");
            sb.Append("             BTRN.NO_OF_CARTONS,");
            sb.Append("             BTRN.GOODS_DECRIPTION,");
            sb.Append("             BTRN.COUNTRY_MST_FK,");
            sb.Append("             CONT.COUNTRY_ID,");
            //sb.Append("             BTRN.QUANTITTY ||'   '|| QDDT.DD_ID QUANTITTY,")
            sb.Append("             BTRN.QUANTITTY QUANTITTY,");
            sb.Append("             QDDT.DD_ID DD_ID,");
            sb.Append("             BTRN.NET_WEIGHT,");
            sb.Append("             BTRN.GROSS_WEIGHT,");

            sb.Append("             CASE      ");
            sb.Append("             WHEN  BTRN.LENGTH IS NOT NULL AND BTRN.HEIGHT IS NOT NULL AND BTRN.WIDTH IS NOT NULL THEN BTRN.LENGTH || 'X' || BTRN.HEIGHT || 'X' || BTRN.WIDTH  ");
            sb.Append("             WHEN  BTRN.LENGTH IS NOT NULL AND BTRN.HEIGHT IS NOT NULL THEN BTRN.LENGTH || 'X' || BTRN.HEIGHT  ");
            sb.Append("             WHEN  BTRN.LENGTH IS NOT NULL AND BTRN.WIDTH IS NOT NULL THEN BTRN.LENGTH || 'X' || BTRN.WIDTH  ");
            sb.Append("             WHEN  BTRN.HEIGHT IS NOT NULL AND BTRN.WIDTH  IS NOT NULL THEN  BTRN.HEIGHT || 'X' ||  BTRN.WIDTH  ");
            sb.Append("             WHEN  BTRN.LENGTH IS NOT NULL  THEN TO_CHAR(BTRN.LENGTH)  ");
            sb.Append("             WHEN  BTRN.HEIGHT IS NOT NULL  THEN     TO_CHAR(BTRN.HEIGHT)  ");
            sb.Append("             WHEN  BTRN.WIDTH IS NOT NULL  THEN  TO_CHAR(BTRN.WIDTH)  ");
            sb.Append("             ELSE  ");
            sb.Append("             ''  ");
            sb.Append("             END DIMENSIONCM,  ");
            //sb.Append("             BTRN.LENGTH || 'X' || BTRN.HEIGHT || 'X' || BTRN.WIDTH DIMENSIONCM,")
            sb.Append("             BTRN.VOLUME            ");
            sb.Append("        FROM BOOKING_TRN_COMMINV_DTL BTRN,");
            sb.Append("             BOOKING_MST_TBL         BMT,");
            sb.Append("             COUNTRY_MST_TBL         CONT,");
            sb.Append("             QFOR_DROP_DOWN_TBL      QDDT");
            sb.Append("       WHERE BMT.BOOKING_MST_PK = BTRN.BOOKING_MST_FK");
            sb.Append("         AND BTRN.COUNTRY_MST_FK = CONT.COUNTRY_MST_PK");
            sb.Append("         AND QDDT.DD_FLAG = 'UNIT'");
            sb.Append("         AND QDDT.CONFIG_ID = 'QFOR4459'");
            sb.Append("         AND BTRN.BOOKING_MST_FK = " + BookingPK);
            //sb.Append("         AND BTRN.DIMENTION_UNIT_MST_FK = QDDT.DD_VALUE ORDER BY CARTTON_NR ASC")
            sb.Append("         AND BTRN.DIMENTION_UNIT_MST_FK = QDDT.DD_VALUE  ORDER BY BTRN.CARTTON_NR,BTRN.GOODS_DECRIPTION");

            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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

        #region "Fetch PackingHeader"

        public DataSet PackingHeaderTest()
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT BBT.BOOKING_REF_NO AS PACKAGENO,");
            sb.Append("       CONSIGNEE.CUSTOMER_NAME AS CONSIGNEE,");
            sb.Append("       '' AS NOTIFY,");
            sb.Append("       BBT.BOOKING_REF_NO,");
            sb.Append("       CONSIGNEE.CREDIT_DAYS,");
            sb.Append("       POL.PORT_ID || ',' || POL.PORT_NAME AS POL,");
            sb.Append("       POD.PORT_ID || ',' || POD.PORT_NAME AS POD,");
            sb.Append("       TO_DATE(BBT.CREATED_DT) CREATED_DT,");
            sb.Append("       BBT.VESSEL_NAME || ' ' || BBT.VOYAGE_FLIGHT_NO AS VOYAGE_FLIGHT_NO,");
            sb.Append("       UMTCRT.USER_NAME,");
            sb.Append("       '" + HttpContext.Current.Session["USER_NAME"] + "' PRINTED_BY,");
            sb.Append("       (SELECT ROWTOCOL('SELECT  JC.CONTAINER_NUMBER");
            sb.Append("  FROM JOB_TRN_CONT JC, JOB_CARD_TRN JT,booking_mst_tbl bbt");
            sb.Append("  WHERE JT.JOB_CARD_TRN_PK = JC.JOB_CARD_TRN_FK");
            sb.Append("       AND jc.container_number IS NOT NULL");
            sb.Append("       AND bbt.booking_mst_pk=jt.booking_mst_fk(+)");
            sb.Append("      AND BBT.BOOKING_MST_PK = 3303') AS CONTAINER_NUMBER");
            sb.Append("          FROM DUAL) CONTAINER_NUMBER");
            sb.Append("  FROM BOOKING_MST_TBL  BBT,");
            sb.Append("       CUSTOMER_MST_TBL CONSIGNEE,");
            sb.Append("       PORT_MST_TBL     POD,");
            sb.Append("       PORT_MST_TBL     POL,");
            sb.Append("       USER_MST_TBL     UMTCRT,");
            sb.Append("       JOB_CARD_TRN     JT1,");
            sb.Append("       JOB_TRN_CONT     JC1");
            sb.Append("  WHERE BBT.CONS_CUSTOMER_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
            sb.Append("   AND POL.PORT_MST_PK = BBT.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BBT.PORT_MST_POD_FK");
            sb.Append("   AND UMTCRT.USER_MST_PK(+) = BBT.CREATED_BY_FK");
            sb.Append("   AND BBT.BOOKING_MST_PK = JT1.BOOKING_MST_FK(+)");
            sb.Append("   AND JT1.JOB_CARD_TRN_PK = JC1.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND BBT.BOOKING_MST_PK = 668");
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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

        public DataSet PackingHeader(long BookingPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT BBT.BOOKING_REF_NO AS PACKAGENO,");
            sb.Append("       BBT.BOOKING_MST_PK,");
            sb.Append("       CONSIGNEE.CUSTOMER_NAME AS CONSIGNEE,");
            sb.Append("       CCD.ADM_ADDRESS_1,");
            sb.Append("       CCD.ADM_ADDRESS_2,");
            sb.Append("       CCD.ADM_ADDRESS_3,");
            sb.Append("       CCD.ADM_PHONE_NO_1,");
            sb.Append("       CCD.ADM_FAX_NO,");
            sb.Append("       NOTIFY.CUSTOMER_NAME NOTIFY,");
            sb.Append("       NCD.ADM_ADDRESS_1,");
            sb.Append("       NCD.ADM_ADDRESS_2,");
            sb.Append("       NCD.ADM_ADDRESS_3,");
            sb.Append("       NCD.ADM_PHONE_NO_1,");
            sb.Append("       NCD.ADM_FAX_NO,    ");
            sb.Append("       BBT.BOOKING_REF_NO,");
            sb.Append("       CONSIGNEE.CREDIT_DAYS,");
            sb.Append("       POL.PORT_ID || ',' || POL.PORT_NAME AS POL,");
            sb.Append("       POD.PORT_ID || ',' || POD.PORT_NAME AS POD,");
            sb.Append("       TO_DATE(BBT.CREATED_DT) CREATED_DT,");
            sb.Append("       BBT.VESSEL_NAME || '/' || BBT.VOYAGE_FLIGHT_NO AS VOYAGE_FLIGHT_NO,");
            sb.Append("       UMTCRT.USER_NAME,");
            sb.Append("       (SELECT ROWTOCOL('SELECT  JC.CONTAINER_NUMBER");
            sb.Append("              FROM JOB_TRN_CONT JC, JOB_CARD_TRN JT,BOOKING_MST_TBL BBT");
            sb.Append("              WHERE JT.JOB_CARD_TRN_PK = JC.JOB_CARD_TRN_FK");
            //sb.Append("                 --  AND JC.CONTAINER_NUMBER IS NOT NULL")
            sb.Append("                   AND BBT.BOOKING_MST_PK=JT.BOOKING_MST_FK");
            sb.Append("                  AND BBT.BOOKING_MST_PK =' ||");
            sb.Append("                        BBT.BOOKING_MST_PK) AS CONTAINER_NUMBER");
            sb.Append("          FROM DUAL) CONTAINER_NUMBER");
            sb.Append("   FROM BOOKING_MST_TBL  BBT,");
            sb.Append("       CUSTOMER_MST_TBL CONSIGNEE,");
            sb.Append("       CUSTOMER_MST_TBL NOTIFY,");
            sb.Append("       PORT_MST_TBL     POD,");
            sb.Append("       PORT_MST_TBL     POL,");
            sb.Append("       USER_MST_TBL     UMTCRT,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
            sb.Append("        CUSTOMER_CONTACT_DTLS NCD");
            sb.Append(" WHERE BBT.CONS_CUSTOMER_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND BBT.NOTIFY1_CUST_MST_FK = NOTIFY.CUSTOMER_MST_PK(+)");
            sb.Append("   AND POL.PORT_MST_PK = BBT.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BBT.PORT_MST_POD_FK");
            sb.Append("   AND UMTCRT.USER_MST_PK(+) = BBT.CREATED_BY_FK");
            sb.Append("   AND CCD.CUSTOMER_MST_FK(+) = CONSIGNEE.CUSTOMER_MST_PK");
            sb.Append("   AND NCD.CUSTOMER_MST_FK(+) = NOTIFY.CUSTOMER_MST_PK");
            sb.Append("  AND BBT.BOOKING_MST_PK = " + BookingPK);
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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

        public DataSet FetchLocationWithVat(long Loc)
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

        public DataSet PackingSummary(long BookingPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ROWNUM SLNR, Q.*");
            sb.Append("  FROM (SELECT QDDT.DD_ID, SUM(BTRN.QUANTITTY) QUANTITTY");
            sb.Append("          FROM BOOKING_TRN_COMMINV_DTL BTRN,");
            sb.Append("               BOOKING_MST_TBL         BMT,");
            sb.Append("               COUNTRY_MST_TBL         CONT,");
            sb.Append("               QFOR_DROP_DOWN_TBL      QDDT");
            sb.Append("         WHERE BMT.BOOKING_MST_PK = BTRN.BOOKING_MST_FK");
            sb.Append("           AND BTRN.COUNTRY_MST_FK = CONT.COUNTRY_MST_PK");
            sb.Append("           AND BTRN.DIMENTION_UNIT_MST_FK = QDDT.DD_VALUE");
            sb.Append("           AND QDDT.DD_FLAG = 'UNIT'");
            sb.Append("           AND QDDT.CONFIG_ID = 'QFOR4459'");
            sb.Append("          AND BMT.BOOKING_MST_PK = " + BookingPK);
            sb.Append("         GROUP BY QDDT.DD_ID ORDER BY QDDT.DD_ID ASC ) Q");
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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

        public DataSet InvPackingSummary(long BookingPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ROWNUM SLNR, Q.*");
            sb.Append("  FROM (SELECT SUM(BTRN.NO_OF_CARTONS) NCARTONS,");
            sb.Append("               SUM(BTRN.NET_WEIGHT) NET_WT,");
            sb.Append("               SUM(BTRN.GROSS_WEIGHT) GROSS_WT,");
            sb.Append("               SUM(BTRN.VOLUME) VOLUME");
            sb.Append("          FROM BOOKING_TRN_COMMINV_DTL BTRN,");
            sb.Append("               BOOKING_MST_TBL         BMT,");
            sb.Append("               COUNTRY_MST_TBL         CONT");
            sb.Append("         WHERE BMT.BOOKING_MST_PK = BTRN.BOOKING_MST_FK");
            sb.Append("            AND BMT.BOOKING_MST_PK = " + BookingPK);
            sb.Append("           AND BTRN.COUNTRY_MST_FK = CONT.COUNTRY_MST_PK) Q");
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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

        #endregion "Fetch PackingHeader"
    }
}