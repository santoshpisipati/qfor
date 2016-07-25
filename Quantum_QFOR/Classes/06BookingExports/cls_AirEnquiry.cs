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

using Oracle.DataAccess.Client;
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
    public class clsAirEnquiry : CommonFeatures
    {
        #region "Fetch Enquiry Records "

        //This function is called to fetch the list of enquiry records
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="strEnqPK">The string enq pk.</param>
        /// <param name="strCustPK">The string customer pk.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="lngUserLocFk">The LNG user loc fk.</param>
        /// <param name="lblPOLEnqPK">The label pol enq pk.</param>
        /// <param name="lblPODEnqPK">The label pod enq pk.</param>
        /// <param name="txtExecuteEnqPK">The text execute enq pk.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="ddlCommGroupEnq">The DDL comm group enq.</param>
        /// <param name="ddlEnqTypeEnq">The DDL enq type enq.</param>
        /// <returns></returns>
        public DataSet FetchAll(string strEnqPK = "", string strCustPK = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long lngUserLocFk = 0, string lblPOLEnqPK = "0", string lblPODEnqPK = "0", string txtExecuteEnqPK = "",
        string toDate = "", string fromDate = "", string ddlCommGroupEnq = "", string ddlEnqTypeEnq = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCondition = "FROM";
            strCondition += "ENQUIRY_BKG_AIR_TBL ENQ,";
            strCondition += "ENQUIRY_TRN_AIR TRN,";
            strCondition += "CUSTOMER_CATEGORY_MST_TBL CCMT,";
            strCondition += "USER_MST_TBL UMT";
            strCondition += "WHERE";
            strCondition += "ENQ.CUSTOMER_CATEGORY_FK=CCMT.CUSTOMER_CATEGORY_MST_PK (+)";
            strCondition += " AND ENQ.ENQUIRY_BKG_AIR_PK = TRN.ENQUIRY_MAIN_AIR_FK(+)";
            strCondition += "AND ENQ.EXECUTED_BY= UMT.USER_MST_PK(+)";
            strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + lngUserLocFk + " ";
            strCondition += "AND ENQ.CREATED_BY_FK = UMT.USER_MST_PK ";

            if (!string.IsNullOrEmpty(strEnqPK.Trim()))
            {
                strCondition += "AND ENQ.ENQUIRY_BKG_AIR_PK=" + strEnqPK.Trim();
            }

            if (!string.IsNullOrEmpty(strCustPK.Trim()))
            {
                strCondition += "AND ENQ.CUSTOMER_MST_FK=" + strCustPK.Trim();
            }

            if (!string.IsNullOrEmpty(fromDate))
            {
                strCondition = strCondition + " And TO_DATE(ENQ.ENQUIRY_DATE,DATEFORMAT) >= TO_DATE('" + fromDate + "',dateformat)";
            }
            if (!string.IsNullOrEmpty(toDate))
            {
                strCondition = strCondition + " And TO_DATE(ENQ.ENQUIRY_DATE,DATEFORMAT) <= TO_DATE('" + toDate + "',dateformat)";
            }
            if (lblPOLEnqPK.Trim() != "0" & !string.IsNullOrEmpty(lblPOLEnqPK.Trim()))
            {
                strCondition += "AND TRN.PORT_MST_POL_FK=" + lblPOLEnqPK.Trim();
            }
            if (lblPODEnqPK.Trim() != "0" & !string.IsNullOrEmpty(lblPODEnqPK.Trim()))
            {
                strCondition += "AND TRN.PORT_MST_POD_FK=" + lblPODEnqPK.Trim();
            }
            if (!string.IsNullOrEmpty(txtExecuteEnqPK.Trim()))
            {
                strCondition += "AND UMT.USER_MST_PK =" + txtExecuteEnqPK.Trim();
            }
            if (ddlEnqTypeEnq.Trim() != "0")
            {
                strCondition += "AND TRN.TRANS_REFERED_FROM=" + ddlEnqTypeEnq.Trim();
            }
            if (ddlCommGroupEnq.Trim() != "0")
            {
                strCondition += "AND TRN.COMMODITY_GROUP_FK=" + ddlCommGroupEnq.Trim();
            }
            string strCount = null;
            strCount = "SELECT COUNT(*)  ";
            strCount += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount));
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

            strSQL = " SELECT * FROM (";
            strSQL += "SELECT ROWNUM SR_NO, q.* FROM (";
            strSQL += "SELECT";
            strSQL += "ENQ.ENQUIRY_BKG_AIR_PK,";
            strSQL += "ENQ.ENQUIRY_REF_NO,";
            strSQL += "TO_CHAR(ENQ.ENQUIRY_DATE,'' || DATEFORMAT || '') AS \"ENQDATE\",";
            strSQL += "(CASE CUST_TYPE WHEN 0 THEN (SELECT CUMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CUMT WHERE CUMT.CUSTOMER_MST_PK=ENQ.CUSTOMER_MST_FK) WHEN 1 THEN  (SELECT CUMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL CUMT WHERE CUMT.CUSTOMER_MST_PK=ENQ.CUSTOMER_MST_FK) END) CUSTOMER_NAME,";
            strSQL += "CCMT.CUSTOMER_CATEGORY_DESC,";
            strSQL += "UMT.USER_NAME AS \"EXECUTEDBY\"";
            strSQL += strCondition;
            if (SortColumn == "ENQDATE")
            {
                SortColumn = "ENQ.ENQUIRY_DATE";
            }
            strSQL += " ORDER BY " + SortColumn + SortType + ",ENQ.ENQUIRY_BKG_AIR_PK " + SortType + " ) q  ) ";
            strSQL += " WHERE SR_NO  BETWEEN " + start + " AND " + last;

            DataSet DS = null;
            DS = objWF.GetDataSet(strSQL);
            try
            {
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

        #endregion "Fetch Enquiry Records "

        #region "Fetch Reates"

        //This function is called to fetch the rates from operator tariff
        //Called for Enquiry on Rates
        /// <summary>
        /// Fetches the rates.
        /// </summary>
        /// <param name="nPOLPk">The n pol pk.</param>
        /// <param name="nPODPk">The n pod pk.</param>
        /// <param name="nAirlinePk">The n airline pk.</param>
        /// <param name="strDate">The string date.</param>
        /// <param name="nCommPk">The n comm pk.</param>
        /// <returns></returns>
        public DataSet FetchRates(long nPOLPk, long nPODPk, long nAirlinePk, string strDate, long nCommPk)
        {
            try
            {
                string strSQL = null;

                strSQL = "SELECT Q.* FROM (";
                strSQL = strSQL + " SELECT ";
                strSQL = strSQL + " HDR.TARIFF_REF_NO,POL.PORT_ID AS \"POL\",";
                strSQL = strSQL + " POD.PORT_ID AS \"POD\",";
                strSQL = strSQL + " HDR.AIRLINE_MST_FK AS \"AIRLINE_PK\",";
                strSQL = strSQL + " AIR.AIRLINE_NAME AS \"AIRLINE\",";
                strSQL = strSQL + " FMT.FREIGHT_ELEMENT_ID AS \"FRTELEMENT\",";
                strSQL = strSQL + " CUMT.CURRENCY_ID AS \"CURRENCY\",";
                strSQL = strSQL + " 'Minimum Rate' AS \"DESCRIPTION\",";
                strSQL = strSQL + " 'KGS' AS BASIS,";
                strSQL = strSQL + " FRT.Min_Amount AS \"RATE\",";
                strSQL = strSQL + " com.commodity_group_code AS \"COMMODITY_GROUP\",";
                strSQL = strSQL + " TO_CHAR(HDR.VALID_FROM, DATEFORMAT) AS \"VALID_FROM\",";
                strSQL = strSQL + " TO_CHAR(HDR.VALID_TO, DATEFORMAT) AS \"VALID_TO\",";
                strSQL = strSQL + " 1 AS \"AllInRt\",";
                strSQL = strSQL + " null AS \"SEQUENCE_NO\", ";
                strSQL = strSQL + " 1 AS ListOrder,";
                strSQL = strSQL + " HDR.TARIFF_MAIN_AIR_PK AS \"PK\"";
                strSQL = strSQL + " FROM ";
                strSQL = strSQL + " TARIFF_MAIN_AIR_TBL HDR,";
                strSQL = strSQL + " TARIFF_TRN_AIR_TBL TRN,";
                strSQL = strSQL + " TARIFF_TRN_AIR_FREIGHT_TBL FRT,";
                strSQL = strSQL + " PORT_MST_TBL POL,";
                strSQL = strSQL + " PORT_MST_TBL POD,";
                strSQL = strSQL + " commodity_group_mst_tbl com, ";
                strSQL = strSQL + " AIRLINE_MST_TBL AIR,";
                strSQL = strSQL + " FREIGHT_ELEMENT_MST_TBL FMT,";
                strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CUMT ";
                strSQL = strSQL + " WHERE ";
                strSQL = strSQL + " HDR.TARIFF_MAIN_AIR_PK = TRN.TARIFF_MAIN_AIR_FK";
                strSQL = strSQL + " AND FRT.TARIFF_TRN_AIR_FK=TRN.TARIFF_TRN_AIR_PK";
                strSQL = strSQL + " AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK";
                strSQL = strSQL + " AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK";
                strSQL = strSQL + " AND HDR.COMMODITY_GROUP_FK = com.commodity_group_pk(+)";
                strSQL = strSQL + " AND HDR.AIRLINE_MST_FK=AIR.AIRLINE_MST_PK (+)";
                strSQL = strSQL + " AND FRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
                strSQL = strSQL + " AND FRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK";
                strSQL = strSQL + " AND HDR.ACTIVE=1";
                strSQL = strSQL + " AND HDR.TARIFF_TYPE IN (1,2)";

                strSQL = strSQL + "AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk);
                strSQL = strSQL + "AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk);

                if (nCommPk != 0)
                {
                    strSQL = strSQL + "AND HDR.COMMODITY_GROUP_FK=" + Convert.ToString(nCommPk);
                    strSQL = strSQL + " AND com.commodity_group_pk=" + Convert.ToString(nCommPk);
                }
                if (nAirlinePk != 0)
                {
                    strSQL = strSQL + "AND HDR.AIRLINE_MST_FK=" + Convert.ToString(nAirlinePk);
                }
                strSQL = strSQL + "AND fmt.charge_basis=(select f1.charge_basis from freight_element_mst_tbl";
                strSQL = strSQL + "f1 where f1.freight_element_mst_pk=frt.freight_element_mst_fk)";
                strSQL = strSQL + " AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))";
                strSQL = strSQL + "UNION";
                strSQL = strSQL + " SELECT HDR.TARIFF_REF_NO,POL.PORT_ID AS \"POL\",";
                strSQL = strSQL + "POD.PORT_ID AS \"POD\",";
                strSQL = strSQL + "HDR.AIRLINE_MST_FK AS \"AIRLINE_PK\",";
                strSQL = strSQL + "AIR.AIRLINE_NAME AS \"AIRLINE\",";
                strSQL = strSQL + "FMT.FREIGHT_ELEMENT_ID AS \"FRTELEMENT\",";
                strSQL = strSQL + "CUMT.CURRENCY_ID AS \"CURRENCY\",";
                strSQL = strSQL + "AST.BREAKPOINT_ID AS \"DESCRIPTION\",";
                strSQL = strSQL + "DECODE(AST.BASIS,1,'KGS',2,'FLAT') AS \"BASIS\",";
                strSQL = strSQL + "BRK.TARIFF_RATE AS \"RATE\",";

                strSQL = strSQL + " com.commodity_group_code AS \"COMMODITY_GROUP\",";

                strSQL = strSQL + "TO_CHAR(HDR.VALID_FROM, DATEFORMAT) AS \"VALID_FROM\",";
                strSQL = strSQL + "TO_CHAR(HDR.VALID_TO, DATEFORMAT) AS \"VALID_TO\",";
                strSQL = strSQL + "1 AS \"AllInRt\",";
                strSQL = strSQL + " AST.SEQUENCE_NO,";
                strSQL = strSQL + " 2 AS ListOrder, ";
                strSQL = strSQL + " HDR.TARIFF_MAIN_AIR_PK AS \"PK\"";
                strSQL = strSQL + "FROM";
                strSQL = strSQL + "TARIFF_MAIN_AIR_TBL HDR,";
                strSQL = strSQL + "TARIFF_TRN_AIR_TBL TRN,";
                strSQL = strSQL + "TARIFF_TRN_AIR_FREIGHT_TBL FRT,";
                strSQL = strSQL + "TARIFF_AIR_BREAKPOINTS BRK,";
                strSQL = strSQL + "AIRFREIGHT_SLABS_TBL AST,";
                strSQL = strSQL + "PORT_MST_TBL POL,";
                strSQL = strSQL + "PORT_MST_TBL POD,";

                strSQL = strSQL + " commodity_group_mst_tbl com, ";

                strSQL = strSQL + "AIRLINE_MST_TBL AIR,";
                strSQL = strSQL + "FREIGHT_ELEMENT_MST_TBL FMT,";
                strSQL = strSQL + "CURRENCY_TYPE_MST_TBL CUMT";
                strSQL = strSQL + "WHERE";
                strSQL = strSQL + "HDR.TARIFF_MAIN_AIR_PK = TRN.TARIFF_MAIN_AIR_FK";
                strSQL = strSQL + "AND FRT.TARIFF_TRN_AIR_FK=TRN.TARIFF_TRN_AIR_PK";
                strSQL = strSQL + "AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK";
                strSQL = strSQL + "AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK";

                strSQL = strSQL + " AND HDR.COMMODITY_GROUP_FK = com.commodity_group_pk(+)";

                strSQL = strSQL + "AND HDR.AIRLINE_MST_FK=AIR.AIRLINE_MST_PK (+)";
                strSQL = strSQL + "AND FRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
                strSQL = strSQL + "AND FRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK";
                strSQL = strSQL + "AND FRT.TARIFF_TRN_FREIGHT_PK=BRK.TARIFF_TRN_FREIGHT_FK";
                strSQL = strSQL + "AND HDR.ACTIVE=1";
                strSQL = strSQL + "AND HDR.TARIFF_TYPE IN (1,2)";
                strSQL = strSQL + "AND BRK.AIRFREIGHT_SLABS_TBL_FK=AST.AIRFREIGHT_SLABS_TBL_PK";
                strSQL = strSQL + "AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk);
                strSQL = strSQL + "AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk);

                if (nCommPk != 0)
                {
                    strSQL = strSQL + "AND HDR.COMMODITY_GROUP_FK=" + Convert.ToString(nCommPk);
                    strSQL = strSQL + " AND com.commodity_group_pk=" + Convert.ToString(nCommPk);
                }
                if (nAirlinePk != 0)
                {
                    strSQL = strSQL + "AND HDR.AIRLINE_MST_FK=" + Convert.ToString(nAirlinePk);
                }
                strSQL = strSQL + " AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))";
                strSQL = strSQL + "UNION";

                strSQL = strSQL + " SELECT   HDR.TARIFF_REF_NO,POL.PORT_ID AS \"POL\",";
                strSQL = strSQL + "POD.PORT_ID AS \"POD\",";
                strSQL = strSQL + "HDR.AIRLINE_MST_FK AS \"AIRLINE_PK\",";
                strSQL = strSQL + "AIR.AIRLINE_NAME AS \"AIRLINE\",";
                strSQL = strSQL + "FMT.FREIGHT_ELEMENT_ID AS \"FRTELEMENT\",";
                strSQL = strSQL + "CUMT.CURRENCY_ID AS \"CURRENCY\",";
                strSQL = strSQL + "'SURCHARGE' AS \"DESCRIPTION\",";
                strSQL = strSQL + "DECODE(SUR.CHARGE_BASIS,1,'%',2,'FLAT',3,'KGS',4,'UNIT') AS \"BASIS\",";
                strSQL = strSQL + "SUR.TARIFF_RATE AS \"RATE\",";

                strSQL = strSQL + " com.commodity_group_code AS \"COMMODITY_GROUP\",";

                strSQL = strSQL + "TO_CHAR(HDR.VALID_FROM, DATEFORMAT) AS \"VALID_FROM\",";
                strSQL = strSQL + "TO_CHAR(HDR.VALID_TO, DATEFORMAT) AS \"VALID_TO\",";
                strSQL = strSQL + "1 AS \"AllInRt\",";
                strSQL = strSQL + "null as  \"SEQUENCE_NO\",";
                strSQL = strSQL + "3 As ListOrder,";
                strSQL = strSQL + " HDR.TARIFF_MAIN_AIR_PK AS \"PK\"";
                strSQL = strSQL + "FROM";
                strSQL = strSQL + "TARIFF_MAIN_AIR_TBL HDR,";
                strSQL = strSQL + "TARIFF_TRN_AIR_TBL TRN,";
                strSQL = strSQL + "TARIFF_TRN_AIR_SURCHARGE SUR,";
                strSQL = strSQL + "PORT_MST_TBL POL,";
                strSQL = strSQL + "PORT_MST_TBL POD,";

                strSQL = strSQL + " commodity_group_mst_tbl com, ";

                strSQL = strSQL + "AIRLINE_MST_TBL AIR,";
                strSQL = strSQL + "FREIGHT_ELEMENT_MST_TBL FMT,";
                strSQL = strSQL + "CURRENCY_TYPE_MST_TBL CUMT";
                strSQL = strSQL + "WHERE";
                strSQL = strSQL + "HDR.TARIFF_MAIN_AIR_PK = TRN.TARIFF_MAIN_AIR_FK";
                strSQL = strSQL + "AND SUR.TARIFF_TRN_AIR_FK=TRN.TARIFF_TRN_AIR_PK";
                strSQL = strSQL + "AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK";
                strSQL = strSQL + "AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK";

                strSQL = strSQL + " AND HDR.COMMODITY_GROUP_FK = com.commodity_group_pk(+)";
                strSQL = strSQL + "AND HDR.AIRLINE_MST_FK=AIR.AIRLINE_MST_PK (+) ";
                strSQL = strSQL + "AND SUR.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
                strSQL = strSQL + "AND SUR.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK";
                strSQL = strSQL + "AND HDR.ACTIVE=1";
                strSQL = strSQL + "AND HDR.TARIFF_TYPE IN (1,2)";
                strSQL = strSQL + "AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk);
                strSQL = strSQL + "AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk);

                if (nCommPk != 0)
                {
                    strSQL = strSQL + "AND HDR.COMMODITY_GROUP_FK=" + Convert.ToString(nCommPk);
                    strSQL = strSQL + " AND com.commodity_group_pk=" + Convert.ToString(nCommPk);
                }
                if (nAirlinePk != 0)
                {
                    strSQL = strSQL + "AND HDR.AIRLINE_MST_FK=" + Convert.ToString(nAirlinePk);
                }
                strSQL = strSQL + " AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))";

                strSQL = strSQL + "UNION";

                strSQL = strSQL + "SELECT HDR.TARIFF_REF_NO,POL.PORT_ID AS \"POL\",";
                strSQL = strSQL + "POD.PORT_ID AS \"POD\",";
                strSQL = strSQL + "HDR.AIRLINE_MST_FK AS \"AIRLINE_PK\",";
                strSQL = strSQL + "AIR.AIRLINE_NAME AS \"AIRLINE\",";
                strSQL = strSQL + "FMT.FREIGHT_ELEMENT_ID AS \"FRTELEMENT\",";
                strSQL = strSQL + "CUMT.CURRENCY_ID AS \"CURRENCY\",";
                strSQL = strSQL + "'Minimum Rate' AS \"DESCRIPTION\",";
                strSQL = strSQL + "DECODE(OTH.CHARGE_BASIS,1,'%',2,'FLAT',3,'KGS',4,'UNIT') AS \"BASIS\",";
                strSQL = strSQL + "OTH.TARIFF_RATE AS \"RATE\",";

                strSQL = strSQL + " com.commodity_group_code AS \"COMMODITY_GROUP\",";

                strSQL = strSQL + "TO_CHAR(HDR.VALID_FROM, DATEFORMAT) AS \"VALID_FROM\",";
                strSQL = strSQL + "TO_CHAR(HDR.VALID_TO, DATEFORMAT) AS \"VALID_TO\",";
                strSQL = strSQL + "1 AS \"AllInRt\",";
                strSQL = strSQL + "null AS \"SEQUENCE_NO\",";
                strSQL = strSQL + "4 AS \"ListOrder\",";
                strSQL = strSQL + " HDR.TARIFF_MAIN_AIR_PK AS \"PK\"";
                strSQL = strSQL + "FROM";
                strSQL = strSQL + "TARIFF_MAIN_AIR_TBL HDR,";
                strSQL = strSQL + "TARIFF_TRN_AIR_TBL TRN,";
                strSQL = strSQL + "TARIFF_TRN_AIR_OTH_CHRG OTH,";
                strSQL = strSQL + "PORT_MST_TBL POL,";
                strSQL = strSQL + "PORT_MST_TBL POD,";

                strSQL = strSQL + " commodity_group_mst_tbl com, ";
                strSQL = strSQL + "AIRLINE_MST_TBL AIR,";
                strSQL = strSQL + "FREIGHT_ELEMENT_MST_TBL FMT,";
                strSQL = strSQL + "CURRENCY_TYPE_MST_TBL CUMT";
                strSQL = strSQL + "WHERE";
                strSQL = strSQL + "HDR.TARIFF_MAIN_AIR_PK = TRN.TARIFF_MAIN_AIR_FK";
                strSQL = strSQL + "AND OTH.TARIFF_TRN_AIR_FK=TRN.TARIFF_TRN_AIR_PK";
                strSQL = strSQL + "AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK";
                strSQL = strSQL + "AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK";

                strSQL = strSQL + " AND HDR.COMMODITY_GROUP_FK = com.commodity_group_pk(+)";
                strSQL = strSQL + "AND HDR.AIRLINE_MST_FK=AIR.AIRLINE_MST_PK (+) ";
                strSQL = strSQL + "AND OTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
                strSQL = strSQL + "AND OTH.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK";
                strSQL = strSQL + "AND HDR.ACTIVE=1";
                strSQL = strSQL + "AND HDR.TARIFF_TYPE IN (1,2) ";
                strSQL = strSQL + "AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk);
                strSQL = strSQL + "AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk);

                if (nCommPk != 0)
                {
                    strSQL = strSQL + "AND HDR.COMMODITY_GROUP_FK=" + Convert.ToString(nCommPk);
                    strSQL = strSQL + " AND com.commodity_group_pk=" + Convert.ToString(nCommPk);
                }
                if (nAirlinePk != 0)
                {
                    strSQL = strSQL + "AND HDR.AIRLINE_MST_FK=" + Convert.ToString(nAirlinePk);
                }
                strSQL = strSQL + " AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))";
                strSQL = strSQL + ")Q";
                strSQL = strSQL + "order by q.tariff_ref_no,q.airline_pk,q.commodity_group,Q.FRTELEMENT";

                return (new WorkFlow()).GetDataSet(strSQL);
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

        #endregion "Fetch Reates"

        #region "FetchNewBooking"

        //This function is called to fetch the rates from spot rate, customer contract,quotation and operator tariff
        //Called for Enquiry on New Booking

        /// <summary>
        /// Fetches the new booking.
        /// </summary>
        /// <param name="nCustomerPk">The n customer pk.</param>
        /// <param name="nCommGrp">The n comm GRP.</param>
        /// <param name="dtDate">The dt date.</param>
        /// <param name="strSearch">The string search.</param>
        /// <param name="nTransType">Type of the n trans.</param>
        /// <returns></returns>
        public DataSet FetchNewBooking(long nCustomerPk, long nCommGrp, System.DateTime dtDate, string strSearch, Int16 nTransType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            object objCur = new object();
            try
            {
                objWK.OpenConnection();
                objCommand.Connection = objWK.MyConnection;
                var _with1 = objCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".TEMP_ENQ_NEW_BKG_AIR_PKG.GET_RATES";
                var _with2 = objCommand.Parameters;

                _with2.Add("CUSTOMER_MST_FK_IN", nCustomerPk).Direction = ParameterDirection.Input;
                _with2.Add("COMMODITY_GROUP_FK_IN", nCommGrp).Direction = ParameterDirection.Input;
                _with2.Add("SHIPMENT_DATE_IN", dtDate).Direction = ParameterDirection.Input;
                _with2.Add("PORTPAIR_IN", strSearch).Direction = ParameterDirection.Input;
                _with2.Add("TRANS_TYPE_IN", nTransType).Direction = ParameterDirection.Input;
                _with2.Add("RATE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("COMMODITY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("AIRLINE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("OTH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objCommand;
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

        #endregion "FetchNewBooking"

        #region "Save"

        //This function is called to save the enquiry
        //Called for Enquiry on New Booking
        /// <summary>
        /// Saves the specified HDR ds.
        /// </summary>
        /// <param name="hdrDS">The HDR ds.</param>
        /// <param name="trnDS">The TRN ds.</param>
        /// <param name="nConfigPK">The n configuration pk.</param>
        /// <param name="remarks">The remarks.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet hdrDS, DataSet trnDS, long nConfigPK, string remarks = "")
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;

            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            bool chkflag = false;

            try
            {
                var _with3 = insCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_AIR_TBL_PKG.ENQUIRY_BKG_AIR_TBL_INS";

                _with3.Parameters.Add("ENQUIRY_REF_NO_IN", OracleDbType.Varchar2, 50, "ENQUIRY_REF_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["ENQUIRY_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("ENQUIRY_DATE_IN", OracleDbType.Date, 10, "ENQUIRY_DATE").Direction = ParameterDirection.Input;
                _with3.Parameters["ENQUIRY_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("CUSTOMER_CATEGORY_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_CATEGORY_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["CUSTOMER_CATEGORY_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("EXECUTED_BY_IN", OracleDbType.Int32, 10, "EXECUTED_BY").Direction = ParameterDirection.Input;
                _with3.Parameters["EXECUTED_BY_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("SHIPMENT_DATE_IN", OracleDbType.Date, 10, "SHIPMENT_DATE").Direction = ParameterDirection.Input;
                _with3.Parameters["SHIPMENT_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("CUST_TYPE_IN", OracleDbType.Int32, 1, "CUST_TYPE").Direction = ParameterDirection.Input;
                _with3.Parameters["CUST_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("CREATED_BY_FK_IN", OracleDbType.Int32, 10, "CREATED_BY_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("REMARKS_IN", getDefault(remarks, "")).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ENQUIRY_BKG_AIR_PK").Direction = ParameterDirection.Output;
                _with3.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with4 = updCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_AIR_TBL_PKG.ENQUIRY_BKG_AIR_TBL_UPD";

                _with4.Parameters.Add("ENQUIRY_BKG_AIR_PK_IN", OracleDbType.Int32, 10, "ENQUIRY_BKG_AIR_PK").Direction = ParameterDirection.Input;
                _with4.Parameters["ENQUIRY_BKG_AIR_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("ENQUIRY_REF_NO_IN", OracleDbType.Varchar2, 50, "ENQUIRY_REF_NO").Direction = ParameterDirection.Input;
                _with4.Parameters["ENQUIRY_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("ENQUIRY_DATE_IN", OracleDbType.Date, 10, "ENQUIRY_DATE").Direction = ParameterDirection.Input;
                _with4.Parameters["ENQUIRY_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("CUSTOMER_CATEGORY_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_CATEGORY_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["CUSTOMER_CATEGORY_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("EXECUTED_BY_IN", OracleDbType.Int32, 10, "EXECUTED_BY").Direction = ParameterDirection.Input;
                _with4.Parameters["EXECUTED_BY_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("SHIPMENT_DATE_IN", OracleDbType.Date, 10, "SHIPMENT_DATE").Direction = ParameterDirection.Input;
                _with4.Parameters["SHIPMENT_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("CUST_TYPE_IN", OracleDbType.Int32, 1, "CUST_TYPE").Direction = ParameterDirection.Input;
                _with4.Parameters["CUST_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("LAST_MODIFIED_BY_FK_IN", OracleDbType.Int32, 10, "LAST_MODIFIED_BY_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
                _with4.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("REMARKS_IN", getDefault(remarks, "")).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500).Direction = ParameterDirection.Output;
                _with4.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                arrMessage.Clear();
                TRAN = objWK.MyConnection.BeginTransaction();

                var _with5 = objWK.MyDataAdapter;
                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;
                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;
                if ((hdrDS.GetChanges(DataRowState.Added) != null))
                {
                    chkflag = true;
                }
                else
                {
                    chkflag = false;
                }
                RecAfct = _with5.Update(hdrDS.Tables[0]);
                if (RecAfct > 0)
                {
                    //SaveTrn(trnDS, TRAN, hdrDS.Tables[0].Rows[0][0]);
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    if (chkflag)
                    {
                        RollbackProtocolKey("ENQUIRYBKGAIR", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(hdrDS.Tables[0].Rows[0]["ENQUIRY_REF_NO"]), DateTime.Now);
                    }
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                if ((TRAN != null))
                {
                    if (TRAN.Connection.State == ConnectionState.Open)
                    {
                        TRAN.Rollback();
                        if (chkflag)
                        {
                            RollbackProtocolKey("ENQUIRYBKGAIR", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(hdrDS.Tables[0].Rows[0]["ENQUIRY_REF_NO"]), DateTime.Now);
                        }
                        TRAN = null;
                    }
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                if ((TRAN != null))
                {
                    if (TRAN.Connection.State == ConnectionState.Open)
                    {
                        TRAN.Rollback();
                        if (chkflag)
                        {
                            RollbackProtocolKey("ENQUIRYBKGAIR", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(hdrDS.Tables[0].Rows[0]["ENQUIRY_REF_NO"]), DateTime.Now);
                        }
                        TRAN = null;
                    }
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
                string CurrFKs = "0";
                System.DateTime ContractDt = default(System.DateTime);
                cls_Operator_Contract objContract = new cls_Operator_Contract();
                ContractDt = Convert.ToDateTime(hdrDS.Tables[0].Rows[0]["ENQUIRY_DATE"]);
                for (int nRowCnt = 0; nRowCnt <= trnDS.Tables[1].Rows.Count - 1; nRowCnt++)
                {
                    CurrFKs += "," + trnDS.Tables[1].Rows[nRowCnt]["CURRENCY_MST_FK"];
                }
                UpdateTempExRate(Convert.ToInt64(hdrDS.Tables[0].Rows[0][0]), CurrFKs, ContractDt, "AIRENQUIRY");
            }
        }

        #endregion "Save"

        #region "Update Temp ExchangeRate"

        /// <summary>
        /// Updates the temporary ex rate.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="CurrFks">The curr FKS.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="FromFlag">From flag.</param>
        /// <returns></returns>
        public ArrayList UpdateTempExRate(long PkValue, string CurrFks, System.DateTime FromDt, string FromFlag = "")
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            arrMessage.Clear();
            try
            {
                var _with6 = objWK.MyCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.TEMP_EX_RATE_TRN_UPD";
                _with6.Parameters.Clear();
                _with6.Parameters.Add("REF_FK_IN", PkValue).Direction = ParameterDirection.Input;
                _with6.Parameters["REF_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("BASE_CURRENCY_FK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with6.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("CURRENCY_FKS_IN", CurrFks).Direction = ParameterDirection.Input;
                _with6.Parameters["CURRENCY_FKS_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("FROM_DATE_IN", FromDt).Direction = ParameterDirection.Input;
                _with6.Parameters["FROM_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("FROM_FLAG_IN", FromFlag).Direction = ParameterDirection.Input;
                _with6.Parameters["FROM_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with6.ExecuteNonQuery();
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Update Temp ExchangeRate"

        #region "Save TRN"

        //This function is called to save the enquiry transaction details
        //Called for Enquiry on New Booking
        /// <summary>
        /// Saves the TRN.
        /// </summary>
        /// <param name="trnDS">The TRN ds.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="nEnqMainAirPK">The n enq main air pk.</param>
        private void SaveTrn(DataSet trnDS, OracleTransaction TRAN, long nEnqMainAirPK)
        {
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();

            try
            {
                objWK.MyConnection = TRAN.Connection;
                var _with7 = delCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.Text;
                _with7.Transaction = TRAN;
                _with7.CommandText = "DELETE FROM ENQUIRY_TRN_AIR WHERE ENQUIRY_MAIN_AIR_FK=" + Convert.ToString(nEnqMainAirPK);
                _with7.ExecuteNonQuery();
                var _with8 = insCommand;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_AIR_TBL_PKG.ENQUIRY_TRN_AIR_INS";

                _with8.Parameters.Add("ENQUIRY_MAIN_AIR_FK_IN", nEnqMainAirPK).Direction = ParameterDirection.Input;
                _with8.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POL_FK").Direction = ParameterDirection.Input;
                _with8.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POD_FK").Direction = ParameterDirection.Input;
                _with8.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("TRANS_REFERED_FROM_IN", OracleDbType.Int32, 10, "TRANS_REFERED_FROM").Direction = ParameterDirection.Input;
                _with8.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("TRANS_REF_NO_IN", OracleDbType.Varchar2, 50, "TRANS_REF_NO").Direction = ParameterDirection.Input;
                _with8.Parameters["TRANS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "AIRLINE_MST_FK").Direction = ParameterDirection.Input;
                _with8.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("EXPECTED_VOLUME_CBM_IN", OracleDbType.Int32, 10, "EXPECTED_VOLUME").Direction = ParameterDirection.Input;
                _with8.Parameters["EXPECTED_VOLUME_CBM_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("EXPECTED_SHIPMENT_IN", OracleDbType.Date, 10, "EXPECTED_SHIPMENT").Direction = ParameterDirection.Input;
                _with8.Parameters["EXPECTED_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
                _with8.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "COMMODITY_MST_FK").Direction = ParameterDirection.Input;
                _with8.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("ALL_IN_TARIFF_IN", OracleDbType.Int32, 10, "ALL_IN_TARIFF").Direction = ParameterDirection.Input;
                _with8.Parameters["ALL_IN_TARIFF_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("EXP_CHARGEABLE_WT_IN", OracleDbType.Int32, 10, "EXP_CHARGEABLE_WT").Direction = ParameterDirection.Input;
                _with8.Parameters["EXP_CHARGEABLE_WT_IN"].SourceVersion = DataRowVersion.Current;
                _with8.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 5, "QUANTITY").Direction = ParameterDirection.Input;
                _with8.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                _with8.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ENQUIRY_TRN_AIR_PK").Direction = ParameterDirection.Output;
                _with8.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with9 = updCommand;
                _with9.Connection = TRAN.Connection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_AIR_TBL_PKG.ENQUIRY_TRN_AIR_UPD";

                _with9.Parameters.Add("ENQUIRY_TRN_AIR_PK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_AIR_PK").Direction = ParameterDirection.Input;
                _with9.Parameters["ENQUIRY_TRN_AIR_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("ENQUIRY_MAIN_AIR_FK_IN", OracleDbType.Int32, 10, "ENQUIRY_MAIN_AIR_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["ENQUIRY_MAIN_AIR_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POL_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POD_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("TRANS_REFERED_FROM_IN", OracleDbType.Int32, 10, "TRANS_REFERED_FROM").Direction = ParameterDirection.Input;
                _with9.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("TRANS_REF_NO_IN", OracleDbType.Varchar2, 50, "TRANS_REF_NO").Direction = ParameterDirection.Input;
                _with9.Parameters["TRANS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "AIRLINE_MST_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("EXPECTED_VOLUME_CBM_IN", OracleDbType.Int32, 10, "EXPECTED_VOLUME").Direction = ParameterDirection.Input;
                _with9.Parameters["EXPECTED_VOLUME_CBM_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("EXPECTED_SHIPMENT_IN", OracleDbType.Date, 10, "EXPECTED_SHIPMENT").Direction = ParameterDirection.Input;
                _with9.Parameters["EXPECTED_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "COMMODITY_MST_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("ALL_IN_TARIFF_IN", OracleDbType.Int32, 10, "ALL_IN_TARIFF").Direction = ParameterDirection.Input;
                _with9.Parameters["ALL_IN_TARIFF_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("EXP_CHARGEABLE_WT_IN", OracleDbType.Int32, 10, "EXP_CHARGEABLE_WT").Direction = ParameterDirection.Input;
                _with9.Parameters["EXP_CHARGEABLE_WT_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 5, "QUANTITY").Direction = ParameterDirection.Input;
                _with9.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;
                _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500).Direction = ParameterDirection.Output;
                _with9.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with10 = objWK.MyDataAdapter;
                _with10.InsertCommand = insCommand;
                _with10.InsertCommand.Transaction = TRAN;
                _with10.UpdateCommand = updCommand;
                _with10.UpdateCommand.Transaction = TRAN;
                RecAfct = _with10.Update(trnDS.Tables[0]);
                if (trnDS.Tables[0].Rows.Count == RecAfct)
                {
                    var _with11 = insCommand;
                    _with11.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_AIR_TBL_PKG.ENQUIRY_TRN_AIR_FRT_DTLS_INS";
                    _with11.Parameters.Clear();
                    _with11.Parameters.Add("ENQUIRY_TRN_AIR_FK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_AIR_FK").Direction = ParameterDirection.Input;
                    _with11.Parameters["ENQUIRY_TRN_AIR_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                    _with11.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", OracleDbType.Int32, 1, "CHECK_FOR_ALL_IN_RT").Direction = ParameterDirection.Input;
                    _with11.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    _with11.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("TARIFF_RATE_IN", OracleDbType.Varchar2, 50, "TARIFF_RATE").Direction = ParameterDirection.Input;
                    _with11.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 10, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                    _with11.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("BASIS_RATE_IN", OracleDbType.Int32, 10, "BASIS_RATE").Direction = ParameterDirection.Input;
                    _with11.Parameters["BASIS_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("AIRFREIGHT_SLABS_FK_IN", OracleDbType.Int32, 10, "AIRFREIGHT_SLABS_FK").Direction = ParameterDirection.Input;
                    _with11.Parameters["AIRFREIGHT_SLABS_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ENQUIRY_TRN_AIR_FRT_PK").Direction = ParameterDirection.Output;
                    _with11.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with12 = updCommand;
                    _with12.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_AIR_TBL_PKG.ENQUIRY_TRN_AIR_FRT_DTLS_UPD";
                    _with12.Parameters.Clear();
                    _with12.Parameters.Add("ENQUIRY_TRN_AIR_FRT_PK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_AIR_FRT_PK").Direction = ParameterDirection.Input;
                    _with12.Parameters["ENQUIRY_TRN_AIR_FRT_PK_IN"].SourceVersion = DataRowVersion.Current;
                    _with12.Parameters.Add("ENQUIRY_TRN_AIR_FK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_AIR_FK").Direction = ParameterDirection.Input;
                    _with12.Parameters["ENQUIRY_TRN_AIR_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with12.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                    _with12.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with12.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", OracleDbType.Int32, 1, "CHECK_FOR_ALL_IN_RT").Direction = ParameterDirection.Input;
                    _with12.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;
                    _with12.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    _with12.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with12.Parameters.Add("TARIFF_RATE_IN", OracleDbType.Varchar2, 50, "TARIFF_RATE").Direction = ParameterDirection.Input;
                    _with12.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    _with12.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 10, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                    _with12.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;
                    _with12.Parameters.Add("BASIS_RATE_IN", OracleDbType.Int32, 10, "BASIS_RATE").Direction = ParameterDirection.Input;
                    _with12.Parameters["BASIS_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    _with12.Parameters.Add("AIRFREIGHT_SLABS_FK_IN", OracleDbType.Int32, 10, "AIRFREIGHT_SLABS_FK").Direction = ParameterDirection.Input;
                    _with12.Parameters["AIRFREIGHT_SLABS_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500).Direction = ParameterDirection.Output;
                    _with12.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    var _with13 = objWK.MyDataAdapter;
                    _with13.InsertCommand = insCommand;
                    _with13.InsertCommand.Transaction = TRAN;
                    _with13.UpdateCommand = updCommand;
                    _with13.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with13.Update(trnDS.Tables[1]);
                    if (RecAfct != trnDS.Tables[1].Rows.Count)
                    {
                        arrMessage.Add("Save not successful");
                        return;
                    }

                    var _with14 = insCommand;
                    _with14.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_AIR_TBL_PKG.ENQUIRY_AIR_OTH_CHRG_INS";
                    _with14.Parameters.Clear();
                    _with14.Parameters.Add("ENQUIRY_TRN_AIR_FK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_AIR_FK").Direction = ParameterDirection.Input;
                    _with14.Parameters["ENQUIRY_TRN_AIR_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with14.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                    _with14.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with14.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    _with14.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with14.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 10, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                    _with14.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;
                    _with14.Parameters.Add("BASIS_RATE_IN", OracleDbType.Int32, 10, "BASIS_RATE").Direction = ParameterDirection.Input;
                    _with14.Parameters["BASIS_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    _with14.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "AMOUNT").Direction = ParameterDirection.Input;
                    _with14.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                    _with14.Parameters.Add("AIRFREIGHT_SLABS_FK_IN", OracleDbType.Int32, 10, "AIRFREIGHT_SLABS_FK").Direction = ParameterDirection.Input;
                    _with14.Parameters["AIRFREIGHT_SLABS_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with14.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ENQUIRY_AIR_OTH_CHRG_PK").Direction = ParameterDirection.Output;
                    _with14.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with15 = updCommand;
                    _with15.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_AIR_TBL_PKG.ENQUIRY_AIR_OTH_CHRG_UPD";
                    _with15.Parameters.Clear();
                    _with15.Parameters.Add("ENQUIRY_AIR_OTH_CHRG_PK_IN", OracleDbType.Int32, 10, "ENQUIRY_AIR_OTH_CHRG_PK").Direction = ParameterDirection.Input;
                    _with15.Parameters["ENQUIRY_AIR_OTH_CHRG_PK_IN"].SourceVersion = DataRowVersion.Current;
                    _with15.Parameters.Add("ENQUIRY_TRN_AIR_FK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_AIR_FK").Direction = ParameterDirection.Input;
                    _with15.Parameters["ENQUIRY_TRN_AIR_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with15.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                    _with15.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with15.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    _with15.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with15.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 10, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                    _with15.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;
                    _with15.Parameters.Add("BASIS_RATE_IN", OracleDbType.Int32, 10, "BASIS_RATE").Direction = ParameterDirection.Input;
                    _with15.Parameters["BASIS_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    _with15.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "AMOUNT").Direction = ParameterDirection.Input;
                    _with15.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                    _with15.Parameters.Add("AIRFREIGHT_SLABS_FK_IN", OracleDbType.Int32, 10, "AIRFREIGHT_SLABS_FK").Direction = ParameterDirection.Input;
                    _with15.Parameters["AIRFREIGHT_SLABS_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500).Direction = ParameterDirection.Output;
                    _with15.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    var _with16 = objWK.MyDataAdapter;
                    _with16.InsertCommand = insCommand;
                    _with16.InsertCommand.Transaction = TRAN;
                    _with16.UpdateCommand = updCommand;
                    _with16.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with16.Update(trnDS.Tables[2]);
                    if (RecAfct != trnDS.Tables[2].Rows.Count)
                    {
                        arrMessage.Add("Save not successful");
                        return;
                    }
                }
                else
                {
                    arrMessage.Add("Save not successful");
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
            }
        }

        #endregion "Save TRN"

        #region "Generate Key"

        //This function is called to generate the enquiry reference no.
        //Called for Enquiry on New Booking
        /// <summary>
        /// Generates the key.
        /// </summary>
        /// <param name="strName">Name of the string.</param>
        /// <param name="nLocPK">The n loc pk.</param>
        /// <param name="nEmpPK">The n emp pk.</param>
        /// <param name="dtDate">The dt date.</param>
        /// <param name="nUserID">The n user identifier.</param>
        /// <returns></returns>
        public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
        {
            return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
        }

        #endregion "Generate Key"

        #region "Fetch HDR"

        //This function is called to fetch the header details of a given enquiry
        /// <summary>
        /// Fetches the HDR.
        /// </summary>
        /// <param name="nEnqPK">The n enq pk.</param>
        /// <returns></returns>
        public DataSet FetchHDR(long nEnqPK)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT * FROM ENQUIRY_BKG_AIR_TBL WHERE ENQUIRY_BKG_AIR_PK=" + Convert.ToString(nEnqPK);

                return (new WorkFlow()).GetDataSet(strSQL);
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

        #endregion "Fetch HDR"

        #region "Fetch Existing enquiury"

        /// <summary>
        /// Fetches the existing enquiry.
        /// </summary>
        /// <param name="nEnqPK">The n enq pk.</param>
        /// <param name="nCommGrpPK">The n comm GRP pk.</param>
        /// <returns></returns>
        public DataSet FetchExistingEnquiry(long nEnqPK, long nCommGrpPK)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            object objCur = new object();
            try
            {
                objWK.OpenConnection();
                objCommand.Connection = objWK.MyConnection;
                var _with17 = objCommand;
                _with17.CommandType = CommandType.StoredProcedure;
                _with17.CommandText = objWK.MyUserName + ".TEMP_ENQ_NEW_BKG_AIR_PKG.GET_ENQ";
                var _with18 = objCommand.Parameters;
                _with18.Add("ENQUIRY_PK_IN", nEnqPK).Direction = ParameterDirection.Input;
                _with18.Add("RATE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with18.Add("COMMODITY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with18.Add("AIRLINE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with18.Add("OTH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with18.Add("COMMODITY_GROUP_FK_OUT", OracleDbType.Int32).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objCommand;
                objWK.MyDataAdapter.Fill(dsData);
                nCommGrpPK = Convert.ToInt32(objCommand.Parameters["COMMODITY_GROUP_FK_OUT"].Value);
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

        #endregion "Fetch Existing enquiury"

        #region "Fetch existing Booking"

        //This function is called to fetch the details of existing booking records
        //Called for Enquiry on Existing Booking
        /// <summary>
        /// Fetches the existing booking.
        /// </summary>
        /// <param name="nCustPK">The n customer pk.</param>
        /// <param name="strBookingPK">The string booking pk.</param>
        /// <param name="strJCPK">The string JCPK.</param>
        /// <param name="strPOLPK">The string polpk.</param>
        /// <param name="strPODPK">The string podpk.</param>
        /// <param name="strValidFrom">The string valid from.</param>
        /// <param name="strValidTo">The string valid to.</param>
        /// <param name="UsrLocFk">The usr loc fk.</param>
        /// <param name="commGroup">The comm group.</param>
        /// <param name="CustType">Type of the customer.</param>
        /// <returns></returns>
        public DataSet FetchExistingBooking(long nCustPK = 0, string strBookingPK = "", string strJCPK = "", string strPOLPK = "", string strPODPK = "", string strValidFrom = "", string strValidTo = "", long UsrLocFk = 0, string commGroup = "", int CustType = 0)
        {
            try
            {
                string strSQL = null;

                strSQL = "SELECT ROWNUM AS \"SNO\", Q.* FROM";
                strSQL = strSQL + " (SELECT DISTINCT BKG.BOOKING_MST_PK,";
                strSQL = strSQL + "BKG.CUST_CUSTOMER_MST_FK,";
                strSQL = strSQL + "BKG.CONS_CUSTOMER_MST_FK,";
                strSQL = strSQL + "BKGTRN.COMMODITY_MST_FK,";
                strSQL = strSQL + "BKG.PORT_MST_POL_FK,";
                strSQL = strSQL + "BKG.PORT_MST_POD_FK,";
                strSQL = strSQL + "BKG.BOOKING_REF_NO,";
                //strSQL = strSQL & vbCrLf & "TO_CHAR(BKG.BOOKING_DATE,'" & dateFormat & "') AS BOOKING_DATE,"
                strSQL = strSQL + "BKG.BOOKING_DATE BOOKING_DATE,";
                strSQL = strSQL + "CASE WHEN SH.CUSTOMER_MST_PK IS NOT NULL THEN";
                strSQL = strSQL + "SH.CUSTOMER_ID";
                strSQL = strSQL + "ELSE (SELECT T.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL T, BOOKING_MST_TBL BT";
                strSQL = strSQL + "WHERE BT.CUST_CUSTOMER_MST_FK = T.CUSTOMER_MST_PK(+)";
                strSQL = strSQL + "AND BT.BOOKING_MST_PK = BKG.BOOKING_MST_PK)";
                strSQL = strSQL + "END AS \"Shipper\",";
                strSQL = strSQL + "CASE WHEN JOB.BOOKING_MST_FK IS NULL THEN";
                strSQL = strSQL + "CON.CUSTOMER_ID";
                strSQL = strSQL + "ELSE (SELECT C.CUSTOMER_ID FROM CUSTOMER_MST_TBL C, JOB_CARD_TRN J ";
                strSQL = strSQL + "WHERE J.CONSIGNEE_CUST_MST_FK=C.CUSTOMER_MST_PK(+)";
                strSQL = strSQL + "AND J.JOB_CARD_TRN_PK=JOB.JOB_CARD_TRN_PK)";
                strSQL = strSQL + "END AS \"Consignee\",";
                strSQL = strSQL + "CGM.COMMODITY_GROUP_CODE AS \"commodity_group\",";
                strSQL = strSQL + "POL.PORT_ID AS \"POL\",";
                strSQL = strSQL + "POD.PORT_ID AS \"POD\",";
                strSQL = strSQL + "DECODE(BKG.STATUS,0,'',1,'Provisional',2,'Confirmed',3,'Cancelled',6,'Shipped') AS \"Status\",";
                strSQL = strSQL + "JOB.JOBCARD_REF_NO,";
                strSQL = strSQL + "JOB.JOB_CARD_TRN_PK";
                strSQL = strSQL + "FROM";
                strSQL = strSQL + "BOOKING_MST_TBL BKG,";
                strSQL = strSQL + "BOOKING_TRN BKGTRN,";
                strSQL = strSQL + "JOB_CARD_TRN JOB,";
                strSQL = strSQL + "CUSTOMER_MST_TBL SH,";
                strSQL = strSQL + "CUSTOMER_MST_TBL CON,";
                strSQL = strSQL + "COMMODITY_GROUP_MST_TBL CGM,";
                strSQL = strSQL + "COMMODITY_MST_TBL COMM,";
                strSQL = strSQL + "PORT_MST_TBL POL,";
                strSQL = strSQL + "PORT_MST_TBL POD,USER_MST_TBL UMT";
                strSQL = strSQL + "WHERE";
                strSQL = strSQL + "BKG.BOOKING_MST_PK = BKGTRN.BOOKING_MST_FK";
                strSQL = strSQL + "AND BKG.BOOKING_MST_PK=JOB.BOOKING_MST_FK(+)";
                strSQL = strSQL + "AND BKG.CUST_CUSTOMER_MST_FK=SH.CUSTOMER_MST_PK(+)";
                strSQL = strSQL + "AND BKG.CONS_CUSTOMER_MST_FK=CON.CUSTOMER_MST_PK(+)";
                strSQL = strSQL + "AND BKGTRN.COMMODITY_GROUP_FK=CGM.COMMODITY_GROUP_PK(+)";
                strSQL = strSQL + "AND BKGTRN.COMMODITY_MST_FK=COMM.COMMODITY_MST_PK(+)";
                strSQL = strSQL + "AND BKG.PORT_MST_POL_FK=POL.PORT_MST_PK";
                strSQL = strSQL + "AND BKG.PORT_MST_POD_FK=POD.PORT_MST_PK";
                strSQL = strSQL + "AND UMT.DEFAULT_LOCATION_FK = " + UsrLocFk + " ";
                if (!string.IsNullOrEmpty(commGroup) | commGroup != "0")
                {
                    strSQL = strSQL + "AND cgm.commodity_group_pk= " + commGroup + " ";
                }
                strSQL = strSQL + "AND BKG.CREATED_BY_FK = UMT.USER_MST_PK ";

                if (nCustPK != 0)
                {
                    if (CustType == 0)
                    {
                        strSQL = strSQL + "AND nvl(BKG.CUST_CUSTOMER_MST_FK,0)=" + Convert.ToString(nCustPK);
                    }
                    else
                    {
                        strSQL = strSQL + "AND nvl(BKG.CONS_CUSTOMER_MST_FK,NVL(CONSIGNEE_CUST_MST_FK,0))=" + Convert.ToString(nCustPK);
                    }
                }
                if (!string.IsNullOrEmpty(strBookingPK.Trim()) & strBookingPK.Trim() != "0")
                {
                    strSQL = strSQL + "AND BKG.BOOKING_MST_PK=" + strBookingPK.Trim();
                }
                if (!string.IsNullOrEmpty(strJCPK.Trim()) & strJCPK.Trim() != "0")
                {
                    strSQL = strSQL + "AND JOB.JOB_CARD_TRN_PK=" + strJCPK.Trim();
                }
                if (!string.IsNullOrEmpty(strPOLPK.Trim()) & strPOLPK.Trim() != "0")
                {
                    strSQL = strSQL + "AND BKG.PORT_MST_POL_FK=" + strPOLPK.Trim();
                }
                if (!string.IsNullOrEmpty(strPODPK.Trim()) & strPODPK.Trim() != "0")
                {
                    strSQL = strSQL + "AND BKG.PORT_MST_POD_FK=" + strPODPK.Trim();
                }
                DateTime ValidfrmDate = new DateTime();
                if (DateTime.TryParse(strValidFrom, out ValidfrmDate))
                {
                    strSQL = strSQL + "AND BKG.BOOKING_DATE>='" + strValidFrom.Trim() + "'";
                }
                DateTime ValidToDate = new DateTime();
                if (DateTime.TryParse(strValidTo, out ValidToDate))
                {
                    strSQL = strSQL + "AND BKG.BOOKING_DATE<='" + strValidTo.Trim() + "'";
                }
                strSQL = strSQL + " AND BKG.BUSINESS_TYPE = 1 ";
                strSQL = strSQL + "ORDER BY TO_DATE(BOOKING_DATE,'" + dateFormat + "') DESC,BOOKING_REF_NO DESC)Q ";

                return (new WorkFlow()).GetDataSet(strSQL);
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

        #endregion "Fetch existing Booking"

        #region "Get Customer"

        /// <summary>
        /// Gets the customer.
        /// </summary>
        /// <param name="nCustPK">The n customer pk.</param>
        /// <returns></returns>
        public DataTable GetCustomer(long nCustPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            DataTable DS = null;

            strQuery.Append("SELECT C.CUSTOMER_MST_PK AS \"PK\",");
            strQuery.Append("       C.CUSTOMER_ID AS \"ID\",");
            strQuery.Append("       C.CUSTOMER_NAME AS \"Customer Name\",");
            strQuery.Append("       CDTL.ADM_ADDRESS_1 || case when( CDTL.ADM_ADDRESS_2 is not null ) then CHR(13) || CDTL.ADM_ADDRESS_2 else CDTL.ADM_ADDRESS_2 end || ");
            strQuery.Append("       case when ( CDTL.ADM_ADDRESS_3 is not null ) then chr(13) || CDTL.ADM_ADDRESS_3 else CDTL.ADM_ADDRESS_3  end  || ");
            strQuery.Append("       CHR(13) || CDTL.ADM_CITY || '-' ||");
            strQuery.Append("       CDTL.ADM_ZIP_CODE || CHR(13) || CMT.COUNTRY_NAME AS \"ADDRESS\",");
            strQuery.Append("       CDTL.ADM_CONTACT_PERSON AS \"PERSON\",");
            strQuery.Append("       CDTL.ADM_PHONE_NO_1 AS \"PHON\",");
            strQuery.Append("       CDTL.ADM_FAX_NO AS \"FAX\",");
            strQuery.Append("       CDTL.ADM_EMAIL_ID AS \"EMAIL\",");
            strQuery.Append("       CDTL.ADM_SALUTATION AS \"SAL\"");
            strQuery.Append("  FROM CUSTOMER_MST_TBL C, CUSTOMER_CONTACT_DTLS CDTL, COUNTRY_MST_TBL CMT");
            strQuery.Append(" WHERE C.CUSTOMER_MST_PK = CDTL.CUSTOMER_MST_FK");
            strQuery.Append("   AND CDTL.ADM_COUNTRY_MST_FK = CMT.COUNTRY_MST_PK(+)");
            strQuery.Append("   AND C.CUSTOMER_MST_PK=");
            strQuery.Append(nCustPK);
            try
            {
                DS = (new WorkFlow()).GetDataTable(strQuery.ToString());
                return DS;
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

        #endregion "Get Customer"

        #region "Quotation Printing - Export AIR LCL"

        /// <summary>
        /// Fetches the quotation air main.
        /// </summary>
        /// <param name="Qpk">The QPK.</param>
        /// <returns></returns>
        public DataSet FetchQuotationAirMain(Int32 Qpk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "  SELECT QA.QUOTATION_MST_PK,QA.QUOTATION_REF_NO,C.CUSTOMER_NAME,CDET.ADM_ADDRESS_1,CDET.ADM_ADDRESS_2,";
            Strsql += "  CDET.ADM_ADDRESS_3,CDET.ADM_ZIP_CODE,'NA' AS NOOFPIECES,'NA' AS DIMENSIONS,";
            Strsql += "  QAA.EXPECTED_WEIGHT AS WEIGHT,QAA.EXPECTED_VOLUME AS CUBE1,";
            Strsql += "  COM.COMMODITY_NAME,'' AS TERMS,PL.PORT_NAME AS COLLECTIONPOINT,PD.PORT_NAME AS DELIVERPOINT,";
            Strsql += "  P.PLACE_NAME AS DESTINATION,'Air' AS MODEOFTRANSPORT,'LCL' AS SERVICE,QA.VALID_FOR";
            Strsql += "  FROM QUOTATION_MST_TBL QA,QUOTATION_DTL_TBL QAA,QUOTATION_FREIGHT_TRN QFA,CUSTOMER_MST_TBL C, ";
            Strsql += "  CUSTOMER_CONTACT_DTLS CDET,COMMODITY_MST_TBL COM,PORT_MST_TBL PL,PORT_MST_TBL PD,";
            Strsql += "  PLACE_MST_TBL P WHERE QA.QUOTATION_MST_PK=" + Qpk;
            Strsql += "  AND QA.CUSTOMER_MST_FK(+)=C.CUSTOMER_MST_PK";
            Strsql += "  AND CDET.CUSTOMER_MST_FK(+)=C.CUSTOMER_MST_PK";
            Strsql += "  AND QA.QUOTATION_MST_PK=QAA.QUOTATION_MST_FK";
            Strsql += "  AND QAA.QUOTE_DTL_PK=QFA.QUOTATION_DTL_FK";
            Strsql += " AND COM.COMMODITY_MST_PK(+)=QAA.COMMODITY_MST_FK ";
            Strsql += "  AND QAA.PORT_MST_POL_FK=PL.PORT_MST_PK";
            Strsql += "  AND QAA.PORT_MST_POD_FK=PD.PORT_MST_PK";
            Strsql += "  AND P.PLACE_PK(+)=QA.DEL_PLACE_MST_FK";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the quotation air fright.
        /// </summary>
        /// <param name="Qpk">The QPK.</param>
        /// <returns></returns>
        public DataSet FetchQuotationAirFright(Int32 Qpk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = " SELECT QA.QUOTATION_MST_PK,QA.QUOTATION_REF_NO,FE.FREIGHT_ELEMENT_NAME AS DESCRPTION,";
            Strsql += " CC.CURRENCY_ID,QFA.QUOTED_RATE AS AMOUNT,DU.DIMENTION_ID AS COMMENTS,QA.SPECIAL_INSTRUCTIONS";
            Strsql += " FROM QUOTATION_MST_TBL QA,QUOTATION_DTL_TBL QAA,QUOTATION_FREIGHT_TRN QFA,FREIGHT_ELEMENT_MST_TBL FE,";
            Strsql += " CURRENCY_TYPE_MST_TBL CC,DIMENTION_UNIT_MST_TBL DU";

            Strsql += " WHERE QA.QUOTATION_MST_PK=" + Qpk;
            Strsql += " AND QA.QUOTATION_MST_PK=QAA.QUOTATION_MST_FK";
            Strsql += " AND QAA.QUOTE_DTL_PK=QFA.QUOTATION_DTL_FK";
            Strsql += " AND CC.CURRENCY_MST_PK(+)=QFA.CURRENCY_MST_FK";
            Strsql += " AND DU.DIMENTION_UNIT_MST_PK(+)=FE.UOM_MST_FK";
            Strsql += " AND DU.DIMENTION_UNIT_MST_PK(+)=FE.UOM_MST_FK";
            Strsql += " AND FE.FREIGHT_ELEMENT_MST_PK=QFA.FREIGHT_ELEMENT_MST_FK";
            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        #endregion "Quotation Printing - Export AIR LCL"

        #region "AIR FREIGHT SLABS"

        /// <summary>
        /// Fetches the air freight slabs.
        /// </summary>
        /// <param name="SlabType">Type of the slab.</param>
        /// <returns></returns>
        public DataTable FetchAirFreightSlabs(int SlabType = 0)
        {
            System.Text.StringBuilder sqlBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            sqlBuilder.Append("SELECT AIR.AIRFREIGHT_SLABS_TBL_PK, ");
            sqlBuilder.Append("AIR.BREAKPOINT_ID FROM AIRFREIGHT_SLABS_TBL AIR ");
            sqlBuilder.Append("  WHERE AIR.ACTIVE_FLAG =1 ");
            if (SlabType > 0)
            {
                sqlBuilder.Append("   AND AIR.BREAKPOINT_TYPE=" + SlabType);
            }
            sqlBuilder.Append("  ORDER BY AIR.SEQUENCE_NO");
            try
            {
                return objWF.GetDataTable(sqlBuilder.ToString());
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
        /// Fetches the contract.
        /// </summary>
        /// <param name="Polpk">The polpk.</param>
        /// <param name="Podpk">The podpk.</param>
        /// <returns></returns>
        public string FetchContract(int Polpk = 0, int Podpk = 0)
        {
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            buildCondition.Append("Select count(*) from (select main.contract_no ");
            buildCondition.Append(" from CONT_MAIN_air_TBL MAIN, CONT_TRN_AIR_LCL f ");
            buildCondition.Append("  where f.cont_main_air_fk = MAIN.cont_main_air_pk ");

            buildCondition.Append("AND ((TO_DATE(SYSDATE , '" + dateFormat + "') BETWEEN");
            buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR");
            buildCondition.Append("    (TO_DATE(SYSDATE , '" + dateFormat + "') BETWEEN");
            buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR");
            buildCondition.Append("    (MAIN.VALID_TO IS NULL))");
            buildCondition.Append(" AND main.ACTIVE = 1 AND main.CONT_APPROVED = 1 ");
            buildCondition.Append(" AND ( ");
            buildCondition.Append("        main.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ");
            buildCondition.Append("       OR main.VALID_TO IS NULL ");
            buildCondition.Append("     ) ");
            buildCondition.Append("   AND f.port_mst_pol_fk=" + Polpk);
            buildCondition.Append("   AND f.port_mst_pod_fk=" + Podpk);
            buildCondition.Append("     ) ");
            try
            {
                return objWF.ExecuteScaler(buildCondition.ToString());
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

        #endregion "AIR FREIGHT SLABS"

        #region "DropDownValue"

        /// <summary>
        /// Fetches the drop down values.
        /// </summary>
        /// <param name="Flag">The flag.</param>
        /// <param name="ConfigID">The configuration identifier.</param>
        /// <returns></returns>
        public DataSet FetchDropDownValues(string Flag, string ConfigID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT T.DD_VALUE, T.DD_ID");
            sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
            sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
            sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
            sb.Append(" ORDER BY T.DROPDOWN_PK ");
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

        #endregion "DropDownValue"

        #region "ViewSchedule"

        /// <summary>
        /// Fetches the schedule.
        /// </summary>
        /// <param name="AOOFK">The aoofk.</param>
        /// <param name="AODFK">The aodfk.</param>
        /// <param name="CarrierPK">The carrier pk.</param>
        /// <param name="Flag">The flag.</param>
        /// <param name="DEPDATE">The depdate.</param>
        /// <param name="DEPFROMDATE">The depfromdate.</param>
        /// <param name="DEPTODATE">The deptodate.</param>
        /// <param name="ARRDATE">The arrdate.</param>
        /// <param name="ARRFROMDATE">The arrfromdate.</param>
        /// <param name="ARRTODATE">The arrtodate.</param>
        /// <param name="TransitFrom">The transit from.</param>
        /// <param name="TransitTo">The transit to.</param>
        /// <param name="Day">The day.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public object FetchSchedule(long AOOFK, long AODFK, long CarrierPK, int Flag, string DEPDATE, string DEPFROMDATE, string DEPTODATE, string ARRDATE, string ARRFROMDATE, string ARRTODATE,
        string TransitFrom, string TransitTo, string Day, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            System.Text.StringBuilder Str = new System.Text.StringBuilder();

            DataSet Ds = new DataSet();
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            object objCur = new object();
            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;
                var _with19 = objWK.MyCommand;
                _with19.CommandType = CommandType.StoredProcedure;
                _with19.CommandText = objWK.MyUserName + ".AIRLINE_SCHEDULE_PKG.FETCH_AIRLINE_SCHEDULE_ENQUIRY";

                var _with20 = objWK.MyCommand.Parameters;
                _with20.Add("CARRIER_FK_IN", (CarrierPK == 0 ? 0 : CarrierPK)).Direction = ParameterDirection.Input;
                _with20.Add("AOO_FK_IN", (AOOFK == 0 ? 0 : AOOFK)).Direction = ParameterDirection.Input;
                _with20.Add("AOD_FK_IN", (AODFK == 0 ? 0 : AODFK)).Direction = ParameterDirection.Input;
                _with20.Add("FLAG_IN", (Flag == 0 ? 0 : Flag)).Direction = ParameterDirection.Input;
                _with20.Add("DEP_DATE_IN", (string.IsNullOrEmpty(DEPDATE) ? "" : DEPDATE)).Direction = ParameterDirection.Input;
                _with20.Add("DEP_FROM_DATE_IN", (string.IsNullOrEmpty(DEPFROMDATE) ? "" : DEPFROMDATE)).Direction = ParameterDirection.Input;
                _with20.Add("DEP_TO_DATE_IN", (string.IsNullOrEmpty(DEPTODATE) ? "" : DEPTODATE)).Direction = ParameterDirection.Input;
                _with20.Add("ARR_DATE_IN", (string.IsNullOrEmpty(ARRDATE) ? "" : ARRDATE)).Direction = ParameterDirection.Input;
                _with20.Add("ARR_FROM_DATE_IN", (string.IsNullOrEmpty(ARRFROMDATE) ? "" : ARRFROMDATE)).Direction = ParameterDirection.Input;
                _with20.Add("ARR_TO_DATE_IN", (string.IsNullOrEmpty(ARRTODATE) ? "" : ARRTODATE)).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(TransitTo) | TransitTo == "  :  :  ")
                {
                    _with20.Add("TRANS_TIME_TO_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with20.Add("TRANS_TIME_TO_IN", TransitTo).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(TransitFrom) | TransitFrom == "  :  :  ")
                {
                    _with20.Add("TRANS_TIME_FROM_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with20.Add("TRANS_TIME_FROM_IN", TransitFrom).Direction = ParameterDirection.Input;
                }
                _with20.Add("P_DAY", (string.IsNullOrEmpty(Day) ? "" : Day)).Direction = ParameterDirection.Input;
                _with20.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with20.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with20.Add("PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.InputOutput;
                _with20.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

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

        #endregion "ViewSchedule"
    }
}