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
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_CreditPolicy : CommonFeatures
    {
        #region " Fetch Trade Sea "

        /// <summary>
        /// Fetches the trade.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchTrade(string CustPk, string BizType)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" select trd.trade_mst_pk,trd.trade_code,tr.credit_limit,tr.credit_days,tr.credit_used ");
            strQuery.Append(" from trade_wise_cust_cr_policy tr ,trade_mst_tbl trd where trd.trade_mst_pk = tr.trade_mst_fk(+) ");
            strQuery.Append(" union select trd.trade_mst_pk,trd.trade_code,tr.credit_limit,tr.credit_days,tr.credit_used ");
            strQuery.Append(" from trade_wise_cust_cr_policy tr, trade_mst_tbl trd,customer_mst_tbl cs ");
            strQuery.Append(" where trd.trade_mst_pk(+) = tr.trade_mst_fk and tr.customer_mst_fk = cs.customer_mst_pk ");
            //strQuery.Append(" and cs.business_type in (3," & BizType & ") ")
            strQuery.Append(" and cs.customer_mst_pk in " + CustPk + " ");
            strQuery.Append(" order by credit_limit,trade_code ");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Trade Sea "

        #region " Fetch Open Credit Booking Sea Export "

        /// <summary>
        /// Fetches the open CRD BKG exp sea.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet FetchOpenCrdBkgExpSea(string CustPk, int BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT BOOKING_REF_NO,JOBCARD_REF_NO,AIRSEA,POL,POD,CURRENCY_ID,SUM(NVL(get_ex_rate(CURRENCY_MST_FK," + BaseCurrency + ",JOBCARD_DATE),1)) * AMT  \"AMT\" ,CARGO_TYPE,JOBPK,BOOKINGPK");
            strQuery.Append(" FROM(SELECT B.BOOKING_REF_NO,J.JOBCARD_REF_NO,'SEA' AIRSEA,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\", ");
            strQuery.Append(" SUM(JFD.FREIGHT_AMT) \"AMT\",JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE ,B.CARGO_TYPE, J.JOB_CARD_TRN_PK JOBPK,B.BOOKING_MST_PK BOOKINGPK FROM JOB_CARD_TRN J, ");
            strQuery.Append(" BOOKING_MST_TBL B,PORT_MST_TBL POL,PORT_MST_TBL POD,JOB_TRN_FD JFD,CURRENCY_TYPE_MST_TBL CURR ");
            strQuery.Append(" WHERE J.BOOKING_MST_FK = B.BOOKING_MST_PK AND B.PORT_MST_POL_FK = POL.PORT_MST_PK  AND B.PORT_MST_POD_FK = POD.PORT_MST_PK ");
            strQuery.Append(" AND J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK AND JFD.FREIGHT_TYPE = 1  AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND B.STATUS<>3 ");
            strQuery.Append(" AND J.SHIPPER_CUST_MST_FK = " + CustPk + " GROUP BY BOOKING_REF_NO, JOBCARD_REF_NO,POL.PORT_ID,POD.PORT_ID, ");
            strQuery.Append(" JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,B.CARGO_TYPE,J.JOB_CARD_TRN_PK,B.BOOKING_MST_PK ");
            strQuery.Append(" UNION ");
            strQuery.Append(" SELECT B.BOOKING_REF_NO,J.JOBCARD_REF_NO,'SEA' AIRSEA,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\",SUM(JOTH.AMOUNT) \"AMT\", ");
            strQuery.Append(" JOTH.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,B.CARGO_TYPE,J.JOB_CARD_TRN_PK JOBPK,B.BOOKING_MST_PK BOOKINGPK FROM JOB_CARD_TRN J,BOOKING_MST_TBL B, ");
            strQuery.Append(" PORT_MST_TBL POL,PORT_MST_TBL POD,JOB_TRN_OTH_CHRG JOTH,CURRENCY_TYPE_MST_TBL CURR ");
            strQuery.Append(" WHERE J.BOOKING_MST_FK = B.BOOKING_MST_PK AND B.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            strQuery.Append(" AND B.PORT_MST_POD_FK = POD.PORT_MST_PK AND J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JOTH.FREIGHT_TYPE = 1 AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND B.STATUS<>3");
            strQuery.Append(" AND J.SHIPPER_CUST_MST_FK = " + CustPk + " GROUP BY BOOKING_REF_NO,JOBCARD_REF_NO, ");
            strQuery.Append(" POL.PORT_ID,POD.PORT_ID,JOTH.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,B.CARGO_TYPE,J.JOB_CARD_TRN_PK,B.BOOKING_MST_PK ");
            //'
            strQuery.Append(" UNION ");
            strQuery.Append(" SELECT B.BOOKING_REF_NO,J.JOBCARD_REF_NO,'AIR' AIRSEA,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\", ");
            strQuery.Append(" SUM(JFD.FREIGHT_AMT) \"AMT\",JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,0 CARGO_TYPE,J.JOB_CARD_TRN_PK JOBPK,B.BOOKING_MST_PK BOOKINGPK  FROM JOB_CARD_TRN J, ");
            strQuery.Append(" BOOKING_MST_TBL B,PORT_MST_TBL POL,PORT_MST_TBL POD,JOB_TRN_FD JFD,CURRENCY_TYPE_MST_TBL CURR ");
            strQuery.Append(" WHERE J.BOOKING_MST_FK = B.BOOKING_MST_PK AND B.PORT_MST_POL_FK = POL.PORT_MST_PK  AND B.PORT_MST_POD_FK = POD.PORT_MST_PK ");
            strQuery.Append(" AND J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK AND JFD.FREIGHT_TYPE = 1  AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND B.STATUS<>3 ");
            strQuery.Append(" AND J.SHIPPER_CUST_MST_FK = " + CustPk + " GROUP BY BOOKING_REF_NO, JOBCARD_REF_NO,POL.PORT_ID,POD.PORT_ID, ");
            strQuery.Append(" JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE, J.JOB_CARD_TRN_PK,B.BOOKING_MST_PK");
            strQuery.Append(" UNION ");
            strQuery.Append(" SELECT B.BOOKING_REF_NO,J.JOBCARD_REF_NO,'AIR' AIRSEA,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\",SUM(JOTH.AMOUNT) \"AMT\", ");
            strQuery.Append(" JOTH.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,0 CARGO_TYPE,J.JOB_CARD_TRN_PK JOBPK,B.BOOKING_MST_PK BOOKINGPK  FROM JOB_CARD_TRN J,BOOKING_MST_TBL B, ");
            strQuery.Append(" PORT_MST_TBL POL,PORT_MST_TBL POD,JOB_TRN_OTH_CHRG JOTH,CURRENCY_TYPE_MST_TBL CURR ");
            strQuery.Append(" WHERE J.BOOKING_MST_FK = B.BOOKING_MST_PK AND B.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            strQuery.Append(" AND B.PORT_MST_POD_FK = POD.PORT_MST_PK AND J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JOTH.FREIGHT_TYPE = 1 AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND B.STATUS<>3 ");
            strQuery.Append(" AND J.SHIPPER_CUST_MST_FK = " + CustPk + " GROUP BY BOOKING_REF_NO,JOBCARD_REF_NO, ");
            strQuery.Append(" POL.PORT_ID,POD.PORT_ID,JOTH.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,J.JOB_CARD_TRN_PK,B.BOOKING_MST_PK) ");
            //'
            strQuery.Append(" GROUP BY BOOKING_REF_NO,JOBCARD_REF_NO,POL,POD,CURRENCY_ID, AMT,AIRSEA ,CARGO_TYPE,JOBPK,BOOKINGPK");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Open Credit Booking Sea Export "

        #region " Fetch Open Credit Booking Air Import "

        /// <summary>
        /// Fetches the open CRD BKG imp air.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet FetchOpenCrdBkgImpAir(string CustPk, int BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT JOBCARD_REF_NO,POL,POD,CURRENCY_ID,SUM(NVL(get_ex_rate(CURRENCY_MST_FK," + BaseCurrency + ", JOBCARD_DATE), 1)) * AMT  \"AMT\" ");
            strQuery.Append(" FROM (SELECT J.JOBCARD_REF_NO,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\",SUM(JFD.FREIGHT_AMT) \"AMT\", ");
            strQuery.Append(" JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE FROM JOB_CARD_TRN J,PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            strQuery.Append(" JOB_TRN_FD JFD,CURRENCY_TYPE_MST_TBL CURR WHERE J.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            strQuery.Append(" AND J.PORT_MST_POD_FK = POD.PORT_MST_PK  AND J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JFD.FREIGHT_TYPE = 2 AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + " ");
            strQuery.Append(" GROUP BY JOBCARD_REF_NO,POL.PORT_ID,POD.PORT_ID,JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE ");
            strQuery.Append(" UNION ");
            strQuery.Append(" SELECT J.JOBCARD_REF_NO,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\",SUM(JOTH.AMOUNT) \"AMT\",JOTH.CURRENCY_MST_FK, ");
            strQuery.Append(" curr.currency_id,J.JOBCARD_DATE  FROM JOB_CARD_TRN J,PORT_MST_TBL POL,PORT_MST_TBL POD,JOB_TRN_OTH_CHRG JOTH, ");
            strQuery.Append(" CURRENCY_TYPE_MST_TBL CURR WHERE J.PORT_MST_POL_FK = POL.PORT_MST_PK AND J.PORT_MST_POD_FK = POD.PORT_MST_PK ");
            strQuery.Append(" AND J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK  AND JOTH.FREIGHT_TYPE = 2  AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ");
            strQuery.Append(" AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + " GROUP BY JOBCARD_REF_NO,POL.PORT_ID,POD.PORT_ID,JOTH.CURRENCY_MST_FK, ");
            strQuery.Append(" curr.currency_id,J.JOBCARD_DATE)GROUP BY JOBCARD_REF_NO, POL, POD,CURRENCY_ID, AMT ");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Open Credit Booking Air Import "

        #region " Fetch Open Credit Booking Sea Import "

        /// <summary>
        /// Fetches the open CRD BKG imp sea.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet FetchOpenCrdBkgImpSea(string CustPk, int BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT JOBCARD_REF_NO,AIRSEA,POL,POD,CURRENCY_ID,SUM(NVL(get_ex_rate(CURRENCY_MST_FK," + BaseCurrency + ", JOBCARD_DATE), 1)) * AMT  \"AMT\" ,CARGO_TYPE,JOBPK");
            strQuery.Append(" FROM (SELECT J.JOBCARD_REF_NO,'SEA' AIRSEA,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\",SUM(JFD.FREIGHT_AMT) \"AMT\", ");
            strQuery.Append(" JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,J.CARGO_TYPE,J.JOB_CARD_TRN_PK JOBPK FROM JOB_CARD_TRN J,PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            strQuery.Append(" JOB_TRN_FD JFD,CURRENCY_TYPE_MST_TBL CURR WHERE J.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            strQuery.Append(" AND J.PORT_MST_POD_FK = POD.PORT_MST_PK  AND J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JFD.FREIGHT_TYPE = 2 AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + " ");
            strQuery.Append(" GROUP BY JOBCARD_REF_NO,POL.PORT_ID,POD.PORT_ID,JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,J.CARGO_TYPE,J.JOB_CARD_TRN_PK ");
            strQuery.Append(" UNION ");
            strQuery.Append(" SELECT J.JOBCARD_REF_NO,'SEA' AIRSEA,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\",SUM(JOTH.AMOUNT) \"AMT\",JOTH.CURRENCY_MST_FK, ");
            strQuery.Append(" curr.currency_id, J.JOBCARD_DATE,J.CARGO_TYPE,J.JOB_CARD_TRN_PK JOBPK  FROM JOB_CARD_TRN J,PORT_MST_TBL POL,PORT_MST_TBL POD,JOB_TRN_OTH_CHRG JOTH, ");
            strQuery.Append(" CURRENCY_TYPE_MST_TBL CURR WHERE J.PORT_MST_POL_FK = POL.PORT_MST_PK AND J.PORT_MST_POD_FK = POD.PORT_MST_PK ");
            strQuery.Append(" AND J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK  AND JOTH.FREIGHT_TYPE = 2  AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ");
            strQuery.Append(" AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + " GROUP BY JOBCARD_REF_NO,POL.PORT_ID,POD.PORT_ID,JOTH.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,J.CARGO_TYPE,J.JOB_CARD_TRN_PK ");
            //'
            strQuery.Append(" UNION ");
            strQuery.Append(" SELECT J.JOBCARD_REF_NO,'AIR' AIRSEA,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\",SUM(JFD.FREIGHT_AMT) \"AMT\", ");
            strQuery.Append(" JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,0 CARGO_TYPE,J.JOB_CARD_TRN_PK JOBPK FROM JOB_CARD_TRN J,PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            strQuery.Append(" JOB_TRN_FD JFD,CURRENCY_TYPE_MST_TBL CURR WHERE J.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            strQuery.Append(" AND J.PORT_MST_POD_FK = POD.PORT_MST_PK  AND J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JFD.FREIGHT_TYPE = 2 AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + " ");
            strQuery.Append(" GROUP BY JOBCARD_REF_NO,POL.PORT_ID,POD.PORT_ID,JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,J.JOB_CARD_TRN_PK ");
            strQuery.Append(" UNION ");
            strQuery.Append(" SELECT J.JOBCARD_REF_NO,'AIR' AIRSEA,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\",SUM(JOTH.AMOUNT) \"AMT\",JOTH.CURRENCY_MST_FK, ");
            strQuery.Append(" curr.currency_id,J.JOBCARD_DATE,0 CARGO_TYPE,J.JOB_CARD_TRN_PK JOBPK  FROM JOB_CARD_TRN J,PORT_MST_TBL POL,PORT_MST_TBL POD,JOB_TRN_OTH_CHRG JOTH, ");
            strQuery.Append(" CURRENCY_TYPE_MST_TBL CURR WHERE J.PORT_MST_POL_FK = POL.PORT_MST_PK AND J.PORT_MST_POD_FK = POD.PORT_MST_PK ");
            strQuery.Append(" AND J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK  AND JOTH.FREIGHT_TYPE = 2  AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ");
            strQuery.Append(" AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + " GROUP BY JOBCARD_REF_NO,POL.PORT_ID,POD.PORT_ID,JOTH.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE,J.JOB_CARD_TRN_PK) ");
            //'
            strQuery.Append(" GROUP BY JOBCARD_REF_NO, POL, POD,CURRENCY_ID, AMT ,AIRSEA,CARGO_TYPE,JOBPK");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Open Credit Booking Sea Import "

        #region " Fetch Open Credit Booking Air Export "

        /// <summary>
        /// Fetches the open CRD BKG exp air.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet FetchOpenCrdBkgExpAir(string CustPk, int BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT BOOKING_REF_NO,JOBCARD_REF_NO,POL,POD,CURRENCY_ID,SUM(NVL(get_ex_rate(CURRENCY_MST_FK," + BaseCurrency + ",JOBCARD_DATE),1)) * AMT  \"AMT\" ");
            strQuery.Append(" FROM(SELECT B.BOOKING_REF_NO,J.JOBCARD_REF_NO,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\", ");
            strQuery.Append(" SUM(JFD.FREIGHT_AMT) \"AMT\",JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE FROM JOB_CARD_TRN J, ");
            strQuery.Append(" BOOKING_MST_TBL B,PORT_MST_TBL POL,PORT_MST_TBL POD,JOB_TRN_FD JFD,CURRENCY_TYPE_MST_TBL CURR ");
            strQuery.Append(" WHERE J.BOOKING_MST_FK = B.BOOKING_MST_PK AND B.PORT_MST_POL_FK = POL.PORT_MST_PK  AND B.PORT_MST_POD_FK = POD.PORT_MST_PK ");
            strQuery.Append(" AND J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK AND JFD.FREIGHT_TYPE = 1  AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ");
            strQuery.Append(" AND J.SHIPPER_CUST_MST_FK = " + CustPk + " GROUP BY BOOKING_REF_NO, JOBCARD_REF_NO,POL.PORT_ID,POD.PORT_ID, ");
            strQuery.Append(" JFD.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE ");
            strQuery.Append(" UNION ");
            strQuery.Append(" SELECT B.BOOKING_REF_NO,J.JOBCARD_REF_NO,POL.PORT_ID \"POL\",POD.PORT_ID \"POD\",SUM(JOTH.AMOUNT) \"AMT\", ");
            strQuery.Append(" JOTH.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE FROM JOB_CARD_TRN J,BOOKING_MST_TBL B, ");
            strQuery.Append(" PORT_MST_TBL POL,PORT_MST_TBL POD,JOB_TRN_OTH_CHRG JOTH,CURRENCY_TYPE_MST_TBL CURR ");
            strQuery.Append(" WHERE J.BOOKING_MST_FK = B.BOOKING_MST_PK AND B.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            strQuery.Append(" AND B.PORT_MST_POD_FK = POD.PORT_MST_PK AND J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JOTH.FREIGHT_TYPE = 1 AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ");
            strQuery.Append(" AND J.SHIPPER_CUST_MST_FK = " + CustPk + " GROUP BY BOOKING_REF_NO,JOBCARD_REF_NO, ");
            strQuery.Append(" POL.PORT_ID,POD.PORT_ID,JOTH.CURRENCY_MST_FK,curr.currency_id,J.JOBCARD_DATE) ");
            strQuery.Append(" GROUP BY BOOKING_REF_NO,JOBCARD_REF_NO,POL,POD,CURRENCY_ID, AMT ");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Open Credit Booking Air Export "

        #region " Fetch Open Credit Booking "

        /// <summary>
        /// Fetches the open CRD BKG.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="basecurrency">The basecurrency.</param>
        /// <param name="biztype">The biztype.</param>
        /// <returns></returns>
        public DataSet FetchOpenCrdBkg(string CustPk, string basecurrency, string biztype)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("CUSTOMER_PK", CustPk).Direction = ParameterDirection.Input;
                _with1.Add("BASECURRENCY", basecurrency).Direction = ParameterDirection.Input;
                _with1.Add("BIZTYPE", biztype).Direction = ParameterDirection.Input;
                _with1.Add("CUST_INV_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_CUSTOMER_INV");
                //Catch sqlExp As Exception
                //    Throw sqlExp
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Open Credit Booking "

        #region " Fetch Cust Details Sea "

        /// <summary>
        /// Fetches the customer det sea.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchCustDetSea(string CustPk, string BizType)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" select cus.sea_credit_days,cus.sea_credit_limit,cus.sea_credit_used,cus.credit_type, ");
            strQuery.Append(" cus.sea_rate_of_interest,cus.sea_int_per_period,cus.sea_flat_penalty,cus.Int_Penalty_Flag,cus.Expiry_Date ");
            strQuery.Append(" from customer_mst_tbl cus where cus.business_type in (3," + BizType + ")  ");
            strQuery.Append(" and cus.customer_mst_pk = " + CustPk + " ");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Cust Details Sea "

        #region " Fetch Cust Details Sea "

        /// <summary>
        /// Fetches the customer det air.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <returns></returns>
        public DataSet FetchCustDetAir(string CustPk)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" select cus.air_credit_days,cus.air_credit_limit,cus.air_credit_used, ");
            strQuery.Append(" cus.air_rate_of_interest,cus.air_int_per_period,cus.air_flat_penalty ");
            strQuery.Append(" from customer_mst_tbl cus where cus.business_type in (3,1) ");
            strQuery.Append(" and cus.customer_mst_pk = " + CustPk + " ");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Cust Details Sea "

        #region " Fetch Cust Details Sea "

        /// <summary>
        /// Fetches the credit used.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="CrdExpUsd">The CRD exp usd.</param>
        /// <param name="CrdImpUsd">The CRD imp usd.</param>
        /// <returns></returns>
        public DataSet FetchCreditUsed(string CustPk, string CrdExpUsd = "0", string CrdImpUsd = "0")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT (NVL(SUM(" + Convert.ToDouble(CrdExpUsd) + " + " + Convert.ToDouble(CrdImpUsd) + "),0))as SUM");
            strQuery.Append(" FROM customer_mst_tbl cus where cus.customer_mst_pk = " + CustPk + "");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Cust Details Sea "

        #region " Fetch Cust Details "

        /// <summary>
        /// Fetches the customer details.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <returns></returns>
        public DataSet FetchCustDetails(string CustPk)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" Select cust.business_type,cust.credit_limit,cust.credit_customer,cust.credit_type,cust.currency_mst_fk,cust.version_no, ");
            strQuery.Append(" cust.customer_id,cust.customer_name,cust.credit_policy_flag,cust.priority");
            strQuery.Append(" from customer_mst_tbl cust");
            strQuery.Append(" where cust.customer_mst_pk = " + CustPk + " ");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion " Fetch Cust Details "

        #region "Delete Trade Wise Policy"

        /// <summary>
        /// Deletes the trade wise policy.
        /// </summary>
        /// <returns></returns>
        public string DeleteTradeWisePolicy()
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " DELETE FROM TRADE_WISE_CUST_CR_POLICY ";
            try
            {
                Objwk.ExecuteScaler(Strsql);
                //Modified by Manjunath  PTS ID:Sep-02  13/09/2011
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
            return "";
        }

        #endregion "Delete Trade Wise Policy"

        #region "Fetch Credit Used Export Sea"

        /// <summary>
        /// Fetches the CRD usd exp sea.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public string FetchCrdUsdExpSea(string CustPk, Int64 BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery1 = new StringBuilder();
            strQuery1.Append(" SELECT ROUND(NVL(SUM(INV_AMT - PAID),0),2) FROM (SELECT NVL(SUM(get_ex_rate(CON.CURRENCY_MST_FK, " + BaseCurrency + ", CON.INVOICE_DATE) * ");
            strQuery1.Append(" NVL(CONS_TRN.TOT_AMT, 1)),0) \"INV_AMT\",(select NVL(SUM(get_ex_rate(COL.CURRENCY_MST_FK, " + BaseCurrency + ", ");
            strQuery1.Append(" COL.COLLECTIONS_DATE) * NVL(COLN.RECD_AMOUNT_HDR_CURR, 1)),0) from COLLECTIONS_TRN_TBL COLN, COLLECTIONS_TBL COL ");
            strQuery1.Append(" where COLN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK ");
            strQuery1.Append(" AND CON.INVOICE_REF_NO = COLN.INVOICE_REF_NR");
            strQuery1.Append(" AND COL.CUSTOMER_MST_FK = " + CustPk + " ) \"PAID\" FROM CONSOL_INVOICE_TBL CON,CONSOL_INVOICE_TRN_TBL CONS_TRN, ");
            strQuery1.Append(" JOB_CARD_TRN JOB,CURRENCY_TYPE_MST_TBL CURR,BOOKING_MST_TBL BKG  WHERE CON.CONSOL_INVOICE_PK = CONS_TRN.CONSOL_INVOICE_FK ");
            strQuery1.Append(" AND CONS_TRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK AND CURR.CURRENCY_MST_PK = " + BaseCurrency + "  ");
            strQuery1.Append(" AND CON.PROCESS_TYPE = 1 AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK  AND BKG.STATUS <> 3 AND CON.CUSTOMER_MST_FK = " + CustPk + " ");
            strQuery1.Append(" GROUP BY JOB.JOBCARD_REF_NO, CON.INVOICE_REF_NO ");
            strQuery1.Append(" UNION ");
            strQuery1.Append(" SELECT NVL(SUM(get_ex_rate(CON.CURRENCY_MST_FK, " + BaseCurrency + ", CON.INVOICE_DATE) * ");
            strQuery1.Append(" NVL(CONS_TRN.TOT_AMT, 1)),0) \"INV_AMT\",(select NVL(SUM(get_ex_rate(COL.CURRENCY_MST_FK, " + BaseCurrency + ", ");
            strQuery1.Append(" COL.COLLECTIONS_DATE) * NVL(COLN.RECD_AMOUNT_HDR_CURR, 1)),0) from COLLECTIONS_TRN_TBL COLN, COLLECTIONS_TBL COL ");
            strQuery1.Append(" where COLN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK ");
            strQuery1.Append(" AND CON.INVOICE_REF_NO = COLN.INVOICE_REF_NR");
            strQuery1.Append(" AND COL.CUSTOMER_MST_FK = " + CustPk + " ) \"PAID\" FROM CONSOL_INVOICE_TBL CON,CONSOL_INVOICE_TRN_TBL CONS_TRN, ");
            strQuery1.Append(" JOB_CARD_TRN JOB,CURRENCY_TYPE_MST_TBL CURR,BOOKING_MST_TBL  BKG WHERE CON.CONSOL_INVOICE_PK = CONS_TRN.CONSOL_INVOICE_FK ");
            strQuery1.Append(" AND CONS_TRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK AND CURR.CURRENCY_MST_PK = " + BaseCurrency + "  ");
            strQuery1.Append(" AND CON.PROCESS_TYPE = 1 AND BKG.BOOKING_MST_PK = JOB.JOB_CARD_TRN_PK  AND BKG.STATUS <> 3 AND CON.CUSTOMER_MST_FK = " + CustPk + " ");
            strQuery1.Append(" GROUP BY JOB.JOBCARD_REF_NO, CON.INVOICE_REF_NO) ");
            try
            {
                return objWF.ExecuteScaler(strQuery1.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion "Fetch Credit Used Export Sea"

        #region "Fetch Credit Used Import Sea"

        /// <summary>
        /// Fetches the CRD usd imp sea.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public string FetchCrdUsdImpSea(string CustPk, int BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT ROUND(NVL(SUM(INV_AMT - PAID),0),2) FROM (SELECT NVL(SUM(get_ex_rate(CON.CURRENCY_MST_FK, " + BaseCurrency + ", CON.INVOICE_DATE) * ");
            strQuery.Append(" NVL(CONS_TRN.TOT_AMT, 1)),0) \"INV_AMT\",(select NVL(SUM(get_ex_rate(COL.CURRENCY_MST_FK, " + BaseCurrency + ", ");
            strQuery.Append(" COL.COLLECTIONS_DATE) * NVL(COLN.RECD_AMOUNT_HDR_CURR, 0)),0) from COLLECTIONS_TRN_TBL COLN, COLLECTIONS_TBL COL ");
            strQuery.Append(" where COLN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK ");
            strQuery.Append(" AND CON.INVOICE_REF_NO = COLN.INVOICE_REF_NR ");
            strQuery.Append(" AND COL.CUSTOMER_MST_FK = " + CustPk + " ) \"PAID\" FROM CONSOL_INVOICE_TBL CON,CONSOL_INVOICE_TRN_TBL CONS_TRN, ");
            strQuery.Append(" JOB_CARD_TRN JOB,CURRENCY_TYPE_MST_TBL CURR WHERE CON.CONSOL_INVOICE_PK = CONS_TRN.CONSOL_INVOICE_FK ");
            strQuery.Append(" AND CONS_TRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK AND CURR.CURRENCY_MST_PK = " + BaseCurrency + "  ");
            strQuery.Append(" AND CON.PROCESS_TYPE = 2 AND CON.CUSTOMER_MST_FK = " + CustPk + " ");
            strQuery.Append(" GROUP BY JOB.JOBCARD_REF_NO, CON.INVOICE_REF_NO ");
            strQuery.Append(" UNION ");
            strQuery.Append("  SELECT NVL(SUM(get_ex_rate(CON.CURRENCY_MST_FK, " + BaseCurrency + ", CON.INVOICE_DATE) * ");
            strQuery.Append(" NVL(CONS_TRN.TOT_AMT, 1)),0) \"INV_AMT\",(select NVL(SUM(get_ex_rate(COL.CURRENCY_MST_FK, " + BaseCurrency + ", ");
            strQuery.Append(" COL.COLLECTIONS_DATE) * NVL(COLN.RECD_AMOUNT_HDR_CURR, 0)),0) from COLLECTIONS_TRN_TBL COLN, COLLECTIONS_TBL COL ");
            strQuery.Append(" where COLN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK ");
            strQuery.Append(" AND CON.INVOICE_REF_NO = COLN.INVOICE_REF_NR ");
            strQuery.Append(" AND COL.CUSTOMER_MST_FK = " + CustPk + " ) \"PAID\" FROM CONSOL_INVOICE_TBL CON,CONSOL_INVOICE_TRN_TBL CONS_TRN, ");
            strQuery.Append(" JOB_CARD_TRN JOB,CURRENCY_TYPE_MST_TBL CURR WHERE CON.CONSOL_INVOICE_PK = CONS_TRN.CONSOL_INVOICE_FK ");
            strQuery.Append(" AND CONS_TRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK AND CURR.CURRENCY_MST_PK = " + BaseCurrency + "  ");
            strQuery.Append(" AND CON.PROCESS_TYPE = 2 AND CON.CUSTOMER_MST_FK = " + CustPk + " ");
            strQuery.Append(" GROUP BY JOB.JOBCARD_REF_NO, CON.INVOICE_REF_NO) ");
            try
            {
                return objWF.ExecuteScaler(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion "Fetch Credit Used Import Sea"

        #region "Fetch Credit Used Import Sea"

        /// <summary>
        /// Fetches the CRD usd imp air.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public string FetchCrdUsdImpAir(string CustPk, int BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT NVL(SUM(INV_AMT - PAID),0) FROM (SELECT NVL(SUM(get_ex_rate(CON.CURRENCY_MST_FK, " + BaseCurrency + ", CON.INVOICE_DATE) * ");
            strQuery.Append(" NVL(CONS_TRN.Amt_In_Inv_Curr, 1)),0) \"INV_AMT\",(select NVL(SUM(get_ex_rate(COL.CURRENCY_MST_FK, " + BaseCurrency + ", ");
            strQuery.Append(" COL.COLLECTIONS_DATE) * NVL(COLN.RECD_AMOUNT_HDR_CURR, 0)),0)  from COLLECTIONS_TRN_TBL COLN, COLLECTIONS_TBL COL ");
            strQuery.Append(" where COLN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK ");
            strQuery.Append(" AND COL.CUSTOMER_MST_FK = " + CustPk + " ) \"PAID\" FROM CONSOL_INVOICE_TBL CON,CONSOL_INVOICE_TRN_TBL CONS_TRN, ");
            strQuery.Append(" JOB_CARD_TRN JOB,CURRENCY_TYPE_MST_TBL CURR WHERE CON.CONSOL_INVOICE_PK = CONS_TRN.CONSOL_INVOICE_FK ");
            strQuery.Append(" AND CONS_TRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK AND CURR.CURRENCY_MST_PK = " + BaseCurrency + "  ");
            strQuery.Append(" AND CON.PROCESS_TYPE = 2 AND CON.CUSTOMER_MST_FK = " + CustPk + " ");
            strQuery.Append(" GROUP BY JOB.JOBCARD_REF_NO, CON.INVOICE_REF_NO) ");
            try
            {
                return objWF.ExecuteScaler(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion "Fetch Credit Used Import Sea"

        #region "Fetch Credit Used Import Sea"

        /// <summary>
        /// Fetches the CRD usd exp air.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public string FetchCrdUsdExpAir(string CustPk, int BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT NVL(SUM(INV_AMT - PAID),0) FROM (SELECT NVL(SUM(get_ex_rate(CON.CURRENCY_MST_FK, " + BaseCurrency + ", CON.INVOICE_DATE) * ");
            strQuery.Append(" NVL(CONS_TRN.Amt_In_Inv_Curr, 1)),0) \"INV_AMT\",(select NVL(SUM(get_ex_rate(COL.CURRENCY_MST_FK, " + BaseCurrency + ", ");
            strQuery.Append(" COL.COLLECTIONS_DATE) * NVL(COLN.RECD_AMOUNT_HDR_CURR, 0)),0) from COLLECTIONS_TRN_TBL COLN, COLLECTIONS_TBL COL ");
            strQuery.Append(" where COLN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK ");
            strQuery.Append(" AND COL.CUSTOMER_MST_FK = " + CustPk + " ) \"PAID\" FROM CONSOL_INVOICE_TBL CON,CONSOL_INVOICE_TRN_TBL CONS_TRN, ");
            strQuery.Append(" JOB_CARD_TRN JOB,CURRENCY_TYPE_MST_TBL CURR WHERE CON.CONSOL_INVOICE_PK = CONS_TRN.CONSOL_INVOICE_FK ");
            strQuery.Append(" AND CONS_TRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK AND CURR.CURRENCY_MST_PK = " + BaseCurrency + "  ");
            strQuery.Append(" AND CON.PROCESS_TYPE = 1 AND CON.CUSTOMER_MST_FK = " + CustPk + " ");
            strQuery.Append(" GROUP BY JOB.JOBCARD_REF_NO, CON.INVOICE_REF_NO) ");
            try
            {
                return objWF.ExecuteScaler(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion "Fetch Credit Used Import Sea"

        #region "Fetch Credit Balance Sea"

        /// <summary>
        /// Fetches the CRD bal sea.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="CrdExpUsd">The CRD exp usd.</param>
        /// <param name="CrdImpUsd">The CRD imp usd.</param>
        /// <returns></returns>
        public string FetchCrdBalSea(string CustPk, string CrdExpUsd = "0", string CrdImpUsd = "0")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT (NVL(CUST.SEA_CREDIT_LIMIT,0) - " + Convert.ToDouble(CrdExpUsd) + " - " + Convert.ToDouble(CrdImpUsd) + ") FROM ");
            strQuery.Append(" CUSTOMER_MST_TBL CUST WHERE CUST.CUSTOMER_MST_PK = " + CustPk + "");
            try
            {
                return objWF.ExecuteScaler(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion "Fetch Credit Balance Sea"

        #region "Fetch Credit Balance Air"

        /// <summary>
        /// Fetches the CRD bal air.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="CrdExpUsd">The CRD exp usd.</param>
        /// <param name="CrdImpUsd">The CRD imp usd.</param>
        /// <returns></returns>
        public string FetchCrdBalAir(string CustPk, string CrdExpUsd = "0", string CrdImpUsd = "0")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT (NVL(CUST.AIR_CREDIT_LIMIT,0) - " + CrdExpUsd + " - " + CrdImpUsd + ") FROM ");
            strQuery.Append(" CUSTOMER_MST_TBL CUST WHERE CUST.CUSTOMER_MST_PK = " + CustPk + "");
            try
            {
                return objWF.ExecuteScaler(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion "Fetch Credit Balance Air"

        #region "Fetch Credit Booking Amt Air"

        /// <summary>
        /// Fetches the CRD bok amt air.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public string FetchCrdBokAmtAir(string CustPk, int BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT NVL(SUM(AMT), 0) FROM (SELECT SUM(JFD.FREIGHT_AMT * GET_EX_RATE(JFD.CURRENCY_MST_FK, 1, J.JOBCARD_DATE)) AMT, ");
            strQuery.Append(" JFD.CURRENCY_MST_FK,J.JOBCARD_DATE FROM JOB_CARD_TRN  J,JOB_TRN_FD    JFD, ");
            strQuery.Append(" CURRENCY_TYPE_MST_TBL CURR WHERE J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK AND JFD.FREIGHT_TYPE  IN(1,2) ");
            strQuery.Append(" AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + "  GROUP BY JFD.CURRENCY_MST_FK, ");
            strQuery.Append(" J.JOBCARD_DATE UNION SELECT SUM(JOTH.AMOUNT * GET_EX_RATE(JOTH.CURRENCY_MST_FK, 1, J.JOBCARD_DATE)) AMT,JOTH.CURRENCY_MST_FK,J.JOBCARD_DATE FROM JOB_CARD_TRN J, ");
            strQuery.Append(" JOB_TRN_OTH_CHRG JOTH,CURRENCY_TYPE_MST_TBL CURR WHERE J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JOTH.FREIGHT_TYPE IN(1,2) AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + "  ");
            strQuery.Append(" GROUP BY JOTH.CURRENCY_MST_FK,J.JOBCARD_DATE )");
            //strQuery.Append(" UNION SELECT SUM(JFD.FREIGHT_AMT * GET_EX_RATE(JFD.CURRENCY_MST_FK, 1, J.JOBCARD_DATE)) AMT,JFD.CURRENCY_MST_FK,J.JOBCARD_DATE FROM JOB_CARD_TRN  J, ")
            //strQuery.Append(" JOB_TRN_FD JFD,CURRENCY_TYPE_MST_TBL CURR WHERE J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK ")
            //strQuery.Append(" AND JFD.FREIGHT_TYPE = 1 AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.SHIPPER_CUST_MST_FK = " & CustPk & " ")
            //strQuery.Append(" GROUP BY JFD.CURRENCY_MST_FK,J.JOBCARD_DATE UNION  SELECT SUM(JOTH.AMOUNT * GET_EX_RATE(JOTH.CURRENCY_MST_FK, 1, J.JOBCARD_DATE)) AMT,JOTH.CURRENCY_MST_FK, ")
            //strQuery.Append(" J.JOBCARD_DATE FROM JOB_CARD_TRN J,JOB_TRN_OTH_CHRG JOTH,CURRENCY_TYPE_MST_TBL  CURR ")
            //strQuery.Append(" WHERE J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK AND JOTH.FREIGHT_TYPE = 1 ")
            //strQuery.Append(" AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.SHIPPER_CUST_MST_FK = " & CustPk & "  ")
            //strQuery.Append(" GROUP BY JOTH.CURRENCY_MST_FK,J.JOBCARD_DATE) ")
            try
            {
                return objWF.ExecuteScaler(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion "Fetch Credit Booking Amt Air"

        #region "Fetch Credit Booking Amt Sea"

        /// <summary>
        /// Fetches the CRD bok amt sea.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet FetchCrdBokAmtSea(string CustPk, int BaseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT ROUND(NVL(SUM(AMT), 0),2) AMOUNT FROM (SELECT ROUND(SUM(JFD.FREIGHT_AMT * GET_EX_RATE(JFD.CURRENCY_MST_FK, " + BaseCurrency + ", J.JOBCARD_DATE)),2) AMT,");
            strQuery.Append(" JFD.CURRENCY_MST_FK,J.JOBCARD_DATE FROM JOB_CARD_TRN  J,JOB_TRN_FD    JFD, ");
            strQuery.Append(" CURRENCY_TYPE_MST_TBL CURR WHERE J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + "  GROUP BY JFD.CURRENCY_MST_FK, ");
            strQuery.Append(" J.JOBCARD_DATE UNION SELECT SUM(JOTH.AMOUNT * GET_EX_RATE(JOTH.CURRENCY_MST_FK," + BaseCurrency + ", J.JOBCARD_DATE)) AMT,JOTH.CURRENCY_MST_FK,J.JOBCARD_DATE FROM JOB_CARD_TRN J, ");
            strQuery.Append(" JOB_TRN_OTH_CHRG JOTH,CURRENCY_TYPE_MST_TBL CURR WHERE J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.CONSIGNEE_CUST_MST_FK = " + CustPk + "  ");
            strQuery.Append(" GROUP BY JOTH.CURRENCY_MST_FK,J.JOBCARD_DATE ");
            strQuery.Append(" UNION SELECT ROUND(SUM(JFD.FREIGHT_AMT * GET_EX_RATE(JFD.CURRENCY_MST_FK, " + BaseCurrency + ", J.JOBCARD_DATE)),2) AMT,JFD.CURRENCY_MST_FK,J.JOBCARD_DATE FROM JOB_CARD_TRN  J, ");
            strQuery.Append(" JOB_TRN_FD JFD,CURRENCY_TYPE_MST_TBL CURR,BOOKING_MST_TBL BKG  WHERE J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK ");
            strQuery.Append(" AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND BKG.BOOKING_MST_PK=J.BOOKING_MST_FK AND BKG.STATUS<>3 AND J.SHIPPER_CUST_MST_FK = " + CustPk + " ");
            strQuery.Append(" GROUP BY JFD.CURRENCY_MST_FK,J.JOBCARD_DATE UNION  SELECT SUM(JOTH.AMOUNT * GET_EX_RATE(JOTH.CURRENCY_MST_FK, " + BaseCurrency + ", J.JOBCARD_DATE)) AMT,JOTH.CURRENCY_MST_FK, ");
            strQuery.Append(" J.JOBCARD_DATE FROM JOB_CARD_TRN J,JOB_TRN_OTH_CHRG JOTH,CURRENCY_TYPE_MST_TBL  CURR,BOOKING_MST_TBL BKG ");
            strQuery.Append(" WHERE J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK AND BKG.BOOKING_MST_PK=J.BOOKING_MST_FK AND BKG.STATUS<>3 ");
            strQuery.Append(" AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.SHIPPER_CUST_MST_FK = " + CustPk + "  ");
            strQuery.Append(" GROUP BY JOTH.CURRENCY_MST_FK,J.JOBCARD_DATE) ");
            //'
            //strQuery.Append(" /* UNION ")
            //strQuery.Append(" SELECT ROUND(SUM(JFD.FREIGHT_AMT * GET_EX_RATE(JFD.CURRENCY_MST_FK, " & BaseCurrency & ", J.JOBCARD_DATE)),2) AMT,")
            //strQuery.Append(" JFD.CURRENCY_MST_FK,J.JOBCARD_DATE FROM JOB_CARD_TRN  J,JOB_TRN_FD    JFD, ")
            //strQuery.Append(" CURRENCY_TYPE_MST_TBL CURR WHERE J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK ")
            //strQuery.Append(" AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.CONSIGNEE_CUST_MST_FK = " & CustPk & "  GROUP BY JFD.CURRENCY_MST_FK, ")
            //strQuery.Append(" J.JOBCARD_DATE UNION SELECT SUM(JOTH.AMOUNT * GET_EX_RATE(JOTH.CURRENCY_MST_FK," & BaseCurrency & ", J.JOBCARD_DATE)) AMT,JOTH.CURRENCY_MST_FK,J.JOBCARD_DATE FROM JOB_CARD_TRN J, ")
            //strQuery.Append(" JOB_TRN_OTH_CHRG JOTH,CURRENCY_TYPE_MST_TBL CURR WHERE J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK ")
            //strQuery.Append(" AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.CONSIGNEE_CUST_MST_FK = " & CustPk & "  ")
            //strQuery.Append(" GROUP BY JOTH.CURRENCY_MST_FK,J.JOBCARD_DATE ")
            //strQuery.Append(" UNION SELECT ROUND(SUM(JFD.FREIGHT_AMT * GET_EX_RATE(JFD.CURRENCY_MST_FK, " & BaseCurrency & ", J.JOBCARD_DATE)),2) AMT,JFD.CURRENCY_MST_FK,J.JOBCARD_DATE FROM JOB_CARD_TRN  J, ")
            //strQuery.Append(" JOB_TRN_FD JFD,CURRENCY_TYPE_MST_TBL CURR ,BOOKING_MST_TBL BKG WHERE J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK ")
            //strQuery.Append(" AND JFD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND BKG.BOOKING_MST_PK=J.BOOKING_MST_FK AND J.SHIPPER_CUST_MST_FK = " & CustPk & " ")
            //strQuery.Append(" GROUP BY JFD.CURRENCY_MST_FK,J.JOBCARD_DATE UNION  SELECT SUM(JOTH.AMOUNT * GET_EX_RATE(JOTH.CURRENCY_MST_FK, " & BaseCurrency & ", J.JOBCARD_DATE)) AMT,JOTH.CURRENCY_MST_FK, ")
            //strQuery.Append(" J.JOBCARD_DATE FROM JOB_CARD_TRN J,JOB_TRN_OTH_CHRG JOTH,CURRENCY_TYPE_MST_TBL  CURR ,BOOKING_MST_TBL BKG")
            //strQuery.Append(" WHERE J.JOB_CARD_TRN_PK = JOTH.JOB_CARD_TRN_FK AND BKG.BOOKING_MST_PK=J.BOOKING_MST_FK ")
            //strQuery.Append(" AND JOTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK AND J.SHIPPER_CUST_MST_FK = " & CustPk & "  ")
            //strQuery.Append(" GROUP BY JOTH.CURRENCY_MST_FK,J.JOBCARD_DATE )*/ ")
            //'
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception EX)
            {
                throw EX;
            }
        }

        #endregion "Fetch Credit Booking Amt Sea"

        #region " Save Air Data set "

        /// <summary>
        /// Saves the trade data set.
        /// </summary>
        /// <param name="nCustPk">The n customer pk.</param>
        /// <param name="dsTradeSeaData">The ds trade sea data.</param>
        /// <param name="dsTradeAirData">The ds trade air data.</param>
        /// <param name="SelectCommand">The select command.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public ArrayList SaveTradeDataSet(int nCustPk, DataSet dsTradeSeaData, DataSet dsTradeAirData, OracleCommand SelectCommand, int BizType)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();

            try
            {
                for (nRowCnt = 0; nRowCnt <= dsTradeSeaData.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    var _with4 = SelectCommand;
                    _with4.CommandType = CommandType.StoredProcedure;
                    _with4.CommandText = objWK.MyUserName + ".CREDIT_MANAGER_TBL_PKG.TRADE_WISE_CUST_CR_POLICY_INS";
                    SelectCommand.Parameters.Clear();

                    _with4.Parameters.Add("CUSTOMER_MST_FK_IN", nCustPk).Direction = ParameterDirection.Input;
                    _with4.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with4.Parameters.Add("BUSINESS_TYPE_IN", 2).Direction = ParameterDirection.Input;
                    _with4.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with4.Parameters.Add("TRADE_MST_FK_IN", dsTradeSeaData.Tables[0].Rows[nRowCnt]["TRADE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with4.Parameters["TRADE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with4.Parameters.Add("CREDIT_LIMIT_IN", dsTradeSeaData.Tables[0].Rows[nRowCnt]["CREDIT_LIMIT"]).Direction = ParameterDirection.Input;
                    _with4.Parameters["CREDIT_LIMIT_IN"].SourceVersion = DataRowVersion.Current;

                    _with4.Parameters.Add("CREDIT_USED_IN", dsTradeSeaData.Tables[0].Rows[nRowCnt]["CREDIT_USED"]).Direction = ParameterDirection.Input;
                    _with4.Parameters["CREDIT_USED_IN"].SourceVersion = DataRowVersion.Current;

                    _with4.Parameters.Add("CREDIT_DAYS_IN", dsTradeSeaData.Tables[0].Rows[nRowCnt]["CREDIT_DAYS"]).Direction = ParameterDirection.Input;
                    _with4.Parameters["CREDIT_DAYS_IN"].SourceVersion = DataRowVersion.Current;

                    _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with4.ExecuteNonQuery();
                }
                arrMessage.Clear();
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            try
            {
                for (nRowCnt = 0; nRowCnt <= dsTradeSeaData.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    var _with5 = SelectCommand;
                    _with5.CommandType = CommandType.StoredProcedure;
                    _with5.CommandText = objWK.MyUserName + ".CREDIT_MANAGER_TBL_PKG.TRADE_WISE_CUST_CR_POLICY_INS";
                    SelectCommand.Parameters.Clear();

                    _with5.Parameters.Add("CUSTOMER_MST_FK_IN", nCustPk).Direction = ParameterDirection.Input;
                    _with5.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("BUSINESS_TYPE_IN", 1).Direction = ParameterDirection.Input;
                    _with5.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("TRADE_MST_FK_IN", dsTradeSeaData.Tables[0].Rows[nRowCnt]["TRADE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["TRADE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CREDIT_LIMIT_IN", dsTradeSeaData.Tables[0].Rows[nRowCnt]["CREDIT_LIMIT"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["CREDIT_LIMIT_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CREDIT_USED_IN", dsTradeSeaData.Tables[0].Rows[nRowCnt]["CREDIT_USED"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["CREDIT_USED_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("CREDIT_DAYS_IN", dsTradeSeaData.Tables[0].Rows[nRowCnt]["CREDIT_DAYS"]).Direction = ParameterDirection.Input;
                    _with5.Parameters["CREDIT_DAYS_IN"].SourceVersion = DataRowVersion.Current;

                    _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with5.ExecuteNonQuery();
                }
                arrMessage.Clear();
                arrMessage.Add("All data saved successfully");
                return arrMessage;
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

        #endregion " Save Air Data set "

        #region "Currency Drop Down"

        /// <summary>
        /// Fills the currency combo.
        /// </summary>
        /// <returns></returns>
        public DataSet FillCurrencyCombo()
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("select c.currency_mst_pk,c.currency_id from currency_type_mst_tbl c where c.active_flag =1 order by currency_id");
            try
            {
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

        #endregion "Currency Drop Down"

        #region "Fetch Grid Data"

        /// <summary>
        /// Fetches the grid.
        /// </summary>
        /// <param name="LocPK">The loc pk.</param>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <param name="LoginLocPK">The login loc pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Flag">The flag.</param>
        /// <param name="IsActive">The is active.</param>
        /// <returns></returns>
        public DataSet FetchGrid(string LocPK = "", string CustomerPK = "", int LoginLocPK = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 Flag = 0, Int16 IsActive = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            Int32 TotalRecords = default(Int32);
            if (string.IsNullOrEmpty(LocPK))
            {
                LocPK = Convert.ToString(LoginLocPK);
            }
            sb.Append("SELECT NVL(CMT.CREDIT_ACTIVE_FLAG, 0) ACTIVE_FLAG,");
            sb.Append("       CMT.CUSTOMER_MST_PK,");
            sb.Append("       CMT.CUSTOMER_ID,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       DECODE(CMT.BUSINESS_TYPE, 1, 'Air', 2, 'Sea', 3, 'Both') BUSINESS_TYPE,");
            sb.Append("       DECODE(NVL(CMT.CREDIT_TYPE, 0), 0, 'General', 1, 'Trade') CREDIT_TYPE");
            sb.Append("  FROM CUSTOMER_MST_TBL      CMT,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
            sb.Append("       LOCATION_MST_TBL      LMT");
            sb.Append(" WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
            sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND CMT.CREDIT_CUSTOMER = 1");
            sb.Append("   AND CMT.ACTIVE_FLAG = 1");
            sb.Append("   AND CMT.CREDIT_POLICY_FLAG = 1");
            sb.Append("   AND LMT.LOCATION_MST_PK IN");
            sb.Append("       (SELECT L.LOCATION_MST_PK");
            sb.Append("          FROM LOCATION_MST_TBL L");
            sb.Append("         START WITH L.LOCATION_MST_PK = " + LocPK);
            sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");
            //If LocPK <> "" Then
            //    sb.Append(" AND CCD.ADM_LOCATION_MST_FK=" & LocPK)
            //End If
            if (!string.IsNullOrEmpty(CustomerPK))
            {
                sb.Append(" AND CMT.CUSTOMER_MST_PK=" + CustomerPK);
            }
            if (IsActive == 1)
            {
                sb.Append("  AND CMT.CREDIT_ACTIVE_FLAG = 1");
            }
            if (Flag == 0)
            {
                sb.Append(" AND 1=2");
            }
            strSQL = " select count(*) from (";
            strSQL += sb.ToString() + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
            strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
            strSQL += sb.ToString();
            strSQL += " ORDER BY CUSTOMER_ID  )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Grid Data"

        #region "Get Location ID"

        /// <summary>
        /// Gets the location identifier.
        /// </summary>
        /// <param name="LocPK">The loc pk.</param>
        /// <returns></returns>
        public DataSet GetLocationID(Int32 LocPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append(" SELECT LMT.LOCATION_ID, LMT.LOCATION_NAME");
            sb.Append(" FROM LOCATION_MST_TBL LMT");
            sb.Append("   WHERE LMT.LOCATION_MST_PK = " + LocPK);
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

        #endregion "Get Location ID"

        #region "Fetch Cust Details From Cr.Manager"

        /// <summary>
        /// Fetches the cr manager details.
        /// </summary>
        /// <param name="LocPK">The loc pk.</param>
        /// <returns></returns>
        public object FetchCrManagerDetails(Int32 LocPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT CMT.CR_MANAGER_TBL_PK,");
            sb.Append("       CMT.CR_LIMIT_EDITABLE,");
            sb.Append("       CMT.CR_DAYS_EDITABLE,");
            sb.Append("       CMT.INTEREST_EDITABLE,");
            sb.Append("       CMT.PENALTY_EDITABLE,");
            sb.Append("       CMT.SEA_CREDIT_LIMIT,");
            sb.Append("       CMT.SEA_CREDIT_DAYS,");
            sb.Append("       CMT.SEA_RATE_OF_INTEREST,");
            sb.Append("       CMT.SEA_INT_PER_PERIOD,");
            sb.Append("       CMT.SEA_FLAT_PENALTY,");
            sb.Append("       CMT.EXPIRY_DATE,");
            sb.Append("       CMT.EXPIRED_ON_EDITABLE");
            sb.Append("  FROM CREDIT_MANAGER_TBL CMT");
            sb.Append(" WHERE CMT.LOCATION_MST_FK = " + LocPK);
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

        #endregion "Fetch Cust Details From Cr.Manager"
    }
}