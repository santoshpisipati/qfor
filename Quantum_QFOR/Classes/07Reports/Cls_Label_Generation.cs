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

using Oracle.DataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_Label_Generation : CommonFeatures
    {
        #region "Fetch For AIR/SEA Import"

        /// <summary>
        /// Fetches the mawbdata.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchMAWBDATA(long JobPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JAE.JOB_CARD_AIR_EXP_PK = " + JobPk;
            }

            Strsql = "SELECT COUNT(*)";
            Strsql += "FROM JOB_CARD_AIR_EXP_TBL JAE,";
            Strsql += "  HAWB_EXP_TBL  HAWB, ";
            Strsql += "MAWB_EXP_TBL  MAWB, ";
            Strsql += "  PORT_MST_TBL POL,";
            Strsql += "  PORT_MST_TBL POD";
            Strsql += " WHERE";
            Strsql += " JAE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK ";
            Strsql += " AND JAE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+) ";
            Strsql += " AND   POL.PORT_MST_PK(+)=MAWB.PORT_MST_POL_FK";
            Strsql += " AND   POD.PORT_MST_PK(+)=MAWB.PORT_MST_POD_FK";
            Strsql += " AND   JAE.JOB_CARD_STATUS = 1";
            Strsql += strCondition;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(Strsql));
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

            Strsql = " select  * from (";
            Strsql += " SELECT ROWNUM AS SLNO,Q.* FROM (";
            Strsql += "SELECT JAE.JOB_CARD_AIR_EXP_PK JOBPK,";
            Strsql += "JAE.JOBCARD_REF_NO JOBNO,";
            Strsql += "HAWB.HAWB_EXP_TBL_PK HBPK,";
            Strsql += "HAWB.HAWB_REF_NO HBNO,";
            Strsql += "MAWB.MAWB_EXP_TBL_PK MBPK,";
            Strsql += "MAWB.MAWB_REF_NO MBNO,";
            Strsql += "MAWB.SHIPPER_NAME SHIPPERNAME,";
            Strsql += "MAWB.CONSIGNEE_NAME CONSIGNEENAME,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME";
            Strsql += "FROM JOB_CARD_AIR_EXP_TBL JAE,";
            Strsql += "  HAWB_EXP_TBL  HAWB, ";
            Strsql += "MAWB_EXP_TBL  MAWB, ";
            Strsql += "  PORT_MST_TBL POL,";
            Strsql += "  PORT_MST_TBL POD";
            Strsql += " WHERE";
            Strsql += " JAE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK ";
            Strsql += " AND JAE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+) ";
            Strsql += " AND   POL.PORT_MST_PK(+)=MAWB.PORT_MST_POL_FK";
            Strsql += " AND   POD.PORT_MST_PK(+)=MAWB.PORT_MST_POD_FK";
            Strsql += " AND   JAE.JOB_CARD_STATUS = 1";
            Strsql += strCondition;

            Strsql += " )q) WHERE SLNO  Between " + start + " and " + last;
            try
            {
                return Objwk.GetDataSet(Strsql);
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

        /// <summary>
        /// Fetches the hawbdata.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchHAWBDATA(long JobPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JAE.JOB_CARD_AIR_EXP_PK = " + JobPk;
            }

            Strsql = "SELECT COUNT(*)";
            Strsql += "FROM JOB_CARD_AIR_EXP_TBL JAE,";
            Strsql += "HAWB_EXP_TBL  HAWB, ";
            Strsql += "MAWB_EXP_TBL  MAWB,";
            Strsql += "CUSTOMER_MST_TBL SHIPPERMST,";
            Strsql += "CUSTOMER_MST_TBL CONSIGNEEMST,";
            Strsql += "BOOKING_AIR_TBL BAT,";
            Strsql += "PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD";
            Strsql += "WHERE";

            Strsql += "JAE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK ";
            Strsql += "AND JAE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+) ";
            Strsql += "AND   HAWB.SHIPPER_CUST_MST_FK=SHIPPERMST.CUSTOMER_MST_PK(+)";
            Strsql += "AND HAWB.CONSIGNEE_CUST_MST_FK=CONSIGNEEMST.CUSTOMER_MST_PK(+)";
            Strsql += "AND   BAT.BOOKING_AIR_PK(+)=JAE.BOOKING_AIR_FK";
            Strsql += "AND   POL.PORT_MST_PK(+)=BAT.PORT_MST_POL_FK";
            Strsql += "AND   POD.PORT_MST_PK(+)=BAT.PORT_MST_POD_FK";
            Strsql += "AND   JAE.JOB_CARD_STATUS = 1";
            Strsql += strCondition;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(Strsql)); ;
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

            Strsql = " select  * from (";
            Strsql += " SELECT ROWNUM AS SLNO,Q.* FROM (";
            Strsql += "SELECT JAE.JOB_CARD_AIR_EXP_PK JOBPK,";
            Strsql += "JAE.JOBCARD_REF_NO JOBNO,";
            Strsql += " HAWB.HAWB_EXP_TBL_PK HBPK,";
            Strsql += "HAWB.HAWB_REF_NO HBNO,";
            Strsql += "MAWB.MAWB_EXP_TBL_PK MBPK,";
            Strsql += "MAWB.MAWB_REF_NO MBNO,";
            Strsql += "SHIPPERMST.CUSTOMER_NAME SHIPPERNAME,";
            Strsql += "CONSIGNEEMST.CUSTOMER_NAME CONSIGNEENAME,";

            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME";
            Strsql += "FROM JOB_CARD_AIR_EXP_TBL JAE,";
            Strsql += "HAWB_EXP_TBL  HAWB, ";
            Strsql += "MAWB_EXP_TBL  MAWB,";
            Strsql += "CUSTOMER_MST_TBL SHIPPERMST,";
            Strsql += "CUSTOMER_MST_TBL CONSIGNEEMST,";
            Strsql += "BOOKING_AIR_TBL BAT,";
            Strsql += "PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD";
            Strsql += "WHERE";

            Strsql += "JAE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK ";
            Strsql += "AND JAE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+) ";
            Strsql += "AND   HAWB.SHIPPER_CUST_MST_FK=SHIPPERMST.CUSTOMER_MST_PK(+)";
            Strsql += "AND HAWB.CONSIGNEE_CUST_MST_FK=CONSIGNEEMST.CUSTOMER_MST_PK(+)";
            Strsql += "AND   BAT.BOOKING_AIR_PK(+)=JAE.BOOKING_AIR_FK";
            Strsql += "AND   POL.PORT_MST_PK(+)=BAT.PORT_MST_POL_FK";
            Strsql += "AND   POD.PORT_MST_PK(+)=BAT.PORT_MST_POD_FK";
            Strsql += "AND   JAE.JOB_CARD_STATUS = 1";

            Strsql += strCondition;

            Strsql += " )q) WHERE SLNO  Between " + start + " and " + last;
            try
            {
                return Objwk.GetDataSet(Strsql);
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

        #endregion "Fetch For AIR/SEA Import"

        #region "Fetch Report Data"

        /// <summary>
        /// Fetches the hawb report data.
        /// </summary>
        /// <param name="HBPK">The HBPK.</param>
        /// <returns></returns>
        public DataSet FetchHAWBReportData(string HBPK)
        {
            string Strsql = null;
            WorkFlow objWF = new WorkFlow();
            if (string.IsNullOrEmpty(HBPK))
            {
                HBPK = "0";
            }

            Strsql = "SELECT JAE.JOB_CARD_TRN_PK JOBPK,";
            Strsql += "JAE.JOBCARD_REF_NO JOBNO,";
            Strsql += "HAWB.HAWB_EXP_TBL_PK HBPK,";
            Strsql += "HAWB.HAWB_REF_NO HBREFNO,";
            Strsql += "MAWB.MAWB_EXP_TBL_PK MBPK,";
            Strsql += "MAWB.MAWB_REF_NO MBREFNO,";
            Strsql += "HAWB.TOTAL_PACK_COUNT PIECES,";
            Strsql += "HAWB.TOTAL_GROSS_WEIGHT WEIGHT,";
            Strsql += "SHIPPERMST.CUSTOMER_NAME SHIPPER,";
            Strsql += "SHIPPERDTLS.ADM_ADDRESS_1 SHIPPERADDD1,";
            Strsql += "SHIPPERDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
            Strsql += "SHIPPERDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
            Strsql += "SHIPPERDTLS.ADM_CITY SHIPPERCITY,";
            Strsql += "SHIPPERCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            Strsql += "SHIPPERDTLS.ADM_ZIP_CODE  SHIPPERZIP,";
            Strsql += "SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            Strsql += "SHIPPERDTLS.ADM_FAX_NO     SHIPPERFAX,";
            Strsql += "SHIPPERDTLS.ADM_EMAIL_ID   SHIPPEREMAIL,";
            Strsql += "CONSIGNEEMST.CUSTOMER_NAME CONSIGNEE,";
            Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,";
            Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,";
            Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,";
            Strsql += "CONSIGNEEDTLS.ADM_CITY CONSIGNEECITY,";
            Strsql += "CONSIGNEECOUNTRY.COUNTRY_NAME CONSIGCOUNTRY,";
            Strsql += "CONSIGNEEDTLS.ADM_ZIP_CODE  CONSIGNEEZIP,";
            Strsql += " CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,";
            Strsql += " CONSIGNEEDTLS.ADM_FAX_NO    CONSIGNEEFAX,";
            Strsql += " CONSIGNEEDTLS.ADM_EMAIL_ID  CONSIGNEEEMAIL,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME";
            Strsql += " FROM ";
            Strsql += " HAWB_EXP_TBL  HAWB, ";
            Strsql += " MAWB_EXP_TBL  MAWB, ";
            Strsql += " JOB_CARD_TRN JAE,";
            Strsql += " CUSTOMER_MST_TBL SHIPPERMST,";
            Strsql += " CUSTOMER_CONTACT_DTLS SHIPPERDTLS,";
            Strsql += " COUNTRY_MST_TBL  SHIPPERCOUNTRY,";
            Strsql += " CUSTOMER_MST_TBL CONSIGNEEMST,";
            Strsql += " CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
            Strsql += " COUNTRY_MST_TBL  CONSIGNEECOUNTRY,";
            Strsql += " BOOKING_MST_TBL BAT,";
            Strsql += " PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD";
            Strsql += " WHERE";

            Strsql += " JAE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK";
            Strsql += " AND   MAWB.MAWB_EXP_TBL_PK(+)=JAE.MBL_MAWB_FK";
            Strsql += "  AND   HAWB.SHIPPER_CUST_MST_FK=SHIPPERMST.CUSTOMER_MST_PK(+)";
            Strsql += "  AND   SHIPPERDTLS.CUSTOMER_MST_FK(+)=SHIPPERMST.CUSTOMER_MST_PK";
            Strsql += "  AND   SHIPPERDTLS.ADM_COUNTRY_MST_FK=SHIPPERCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "  AND   CONSIGNEEMST.CUSTOMER_MST_PK(+)=HAWB.CONSIGNEE_CUST_MST_FK";
            Strsql += "   AND   CONSIGNEEDTLS.CUSTOMER_MST_FK(+)=CONSIGNEEMST.CUSTOMER_MST_PK";
            Strsql += "   AND   CONSIGNEEDTLS.ADM_COUNTRY_MST_FK=CONSIGNEECOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "   AND   BAT.BOOKING_MST_PK(+)=JAE.BOOKING_MST_FK";
            Strsql += "   AND   POL.PORT_MST_PK(+)=BAT.PORT_MST_POL_FK";
            Strsql += "   AND   POD.PORT_MST_PK(+)=BAT.PORT_MST_POD_FK";
            Strsql += "   AND   JAE.JOB_CARD_STATUS = 1";
            Strsql += "   AND   HAWB.HAWB_EXP_TBL_PK IN (" + HBPK + ")";

            try
            {
                return objWF.GetDataSet(Strsql);
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

        /// <summary>
        /// Fetches the HBL report data.
        /// </summary>
        /// <param name="HBPK">The HBPK.</param>
        /// <returns></returns>
        public DataSet FetchHBLReportData(string HBPK)
        {
            string Strsql = null;
            if (string.IsNullOrEmpty(HBPK))
            {
                HBPK = "0";
            }
            WorkFlow objWF = new WorkFlow();

            Strsql = "SELECT JSE.JOB_CARD_TRN_PK JOBPK,";
            Strsql += "JSE.JOBCARD_REF_NO JOBNO,";
            Strsql += "HBL.HBL_EXP_TBL_PK HBPK,";
            Strsql += "HBL.HBL_REF_NO HBREFNO,";
            Strsql += "MBL.MBL_EXP_TBL_PK MBPK,";
            Strsql += "MBL.MBL_REF_NO MBREFNO,";
            Strsql += "HBL.TOTAL_PACK_COUNT PIECES,";
            Strsql += "HBL.TOTAL_GROSS_WEIGHT WEIGHT,";
            Strsql += "SHIPPERMST.CUSTOMER_NAME SHIPPER,";
            Strsql += "SHIPPERDTLS.ADM_ADDRESS_1 SHIPPERADDD1,";
            Strsql += "SHIPPERDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
            Strsql += "SHIPPERDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
            Strsql += "SHIPPERDTLS.ADM_CITY SHIPPERCITY,";
            Strsql += "SHIPPERCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            Strsql += "SHIPPERDTLS.ADM_ZIP_CODE  SHIPPERZIP,";
            Strsql += "SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            Strsql += "SHIPPERDTLS.ADM_FAX_NO     SHIPPERFAX,";
            Strsql += "SHIPPERDTLS.ADM_EMAIL_ID   SHIPPEREMAIL,";
            Strsql += "CONSIGNEEMST.CUSTOMER_NAME CONSIGNEE,";
            Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,";
            Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,";
            Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,";
            Strsql += "CONSIGNEEDTLS.ADM_CITY CONSIGNEECITY,";
            Strsql += "CONSIGNEECOUNTRY.COUNTRY_NAME CONSIGCOUNTRY,";
            Strsql += "CONSIGNEEDTLS.ADM_ZIP_CODE  CONSIGNEEZIP,";
            Strsql += " CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,";
            Strsql += " CONSIGNEEDTLS.ADM_FAX_NO    CONSIGNEEFAX,";
            Strsql += " CONSIGNEEDTLS.ADM_EMAIL_ID  CONSIGNEEEMAIL,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME";
            Strsql += " FROM";
            Strsql += "HBL_EXP_TBL  HBL, ";
            Strsql += "MBL_EXP_TBL  MBL, ";
            Strsql += " JOB_CARD_TRN JSE,";
            Strsql += " CUSTOMER_MST_TBL SHIPPERMST,";
            Strsql += " CUSTOMER_CONTACT_DTLS SHIPPERDTLS,";
            Strsql += " COUNTRY_MST_TBL  SHIPPERCOUNTRY,";
            Strsql += " CUSTOMER_MST_TBL CONSIGNEEMST,";
            Strsql += " CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
            Strsql += " COUNTRY_MST_TBL  CONSIGNEECOUNTRY,";
            Strsql += "  BOOKING_MST_TBL BST,";
            Strsql += "  PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD";
            Strsql += " WHERE";

            Strsql += " JSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK ";
            Strsql += " AND   MBL.MBL_EXP_TBL_PK(+)=JSE.MBL_MAWB_FK ";
            Strsql += " AND   HBL.SHIPPER_CUST_MST_FK=SHIPPERMST.CUSTOMER_MST_PK(+) ";
            Strsql += " AND   SHIPPERDTLS.CUSTOMER_MST_FK(+)=SHIPPERMST.CUSTOMER_MST_PK ";
            Strsql += " AND   SHIPPERDTLS.ADM_COUNTRY_MST_FK=SHIPPERCOUNTRY.COUNTRY_MST_PK(+) ";
            Strsql += " AND   CONSIGNEEMST.CUSTOMER_MST_PK(+)=HBL.CONSIGNEE_CUST_MST_FK ";
            Strsql += " AND   CONSIGNEEDTLS.CUSTOMER_MST_FK(+)=CONSIGNEEMST.CUSTOMER_MST_PK ";
            Strsql += " AND   CONSIGNEEDTLS.ADM_COUNTRY_MST_FK=CONSIGNEECOUNTRY.COUNTRY_MST_PK(+) ";
            Strsql += " AND   BST.BOOKING_MST_PK(+)=JSE.BOOKING_MST_FK ";
            Strsql += " AND   POL.PORT_MST_PK(+)=BST.PORT_MST_POL_FK ";
            Strsql += " AND   POD.PORT_MST_PK(+)=BST.PORT_MST_POD_FK ";
            Strsql += " AND   JSE.JOB_CARD_STATUS = 1 ";
            Strsql += " AND   HBL.HBL_EXP_TBL_PK IN (" + HBPK + ") ";

            try
            {
                return objWF.GetDataSet(Strsql);
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

        /// <summary>
        /// Fetches the mawb report data.
        /// </summary>
        /// <param name="HBPK">The HBPK.</param>
        /// <returns></returns>
        public DataSet FetchMAWBReportData(string HBPK)
        {
            string Strsql = null;
            if (string.IsNullOrEmpty(HBPK))
            {
                HBPK = "0";
            }
            WorkFlow objWF = new WorkFlow();
            Strsql = "SELECT JAE.JOB_CARD_TRN_PK JOBPK,";
            Strsql += "JAE.JOBCARD_REF_NO JOBNO,";
            Strsql += " HAWB.HAWB_EXP_TBL_PK HBPK,";
            Strsql += "HAWB.HAWB_REF_NO HBREFNO,";
            Strsql += "MAWB.MAWB_EXP_TBL_PK MBPK,";
            Strsql += "MAWB.MAWB_REF_NO MBREFNO,";
            Strsql += "MAWB.TOTAL_PACK_COUNT PIECES,";
            Strsql += "MAWB.TOTAL_GROSS_WEIGHT WEIGHT,";
            Strsql += "AMST.AIRLINE_NAME SHIPPER,";
            Strsql += "ADTLS.ADM_ADDRESS_1  SHIPPERADDD1,";
            Strsql += "ADTLS.ADM_ADDRESS_2 SHIPPERADD2,";
            Strsql += "ADTLS.ADM_ADDRESS_3 SHIPPERADD3,";
            Strsql += "ADTLS.ADM_CITY  SHIPPERCITY,";
            Strsql += "ACOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            Strsql += "ADTLS.ADM_ZIP_CODE  SHIPPERZIP,";
            Strsql += "ADTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            Strsql += "ADTLS.ADM_FAX_NO SHIPPERFAX,";
            Strsql += "ADTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
            Strsql += "MAWB.AGENT_NAME CONSIGNEE,";
            Strsql += "MAWB.AGENT_ADDRESS  CONSIGNEEADD1,";
            Strsql += "'' CONSIGNEEADD2,";
            Strsql += "'' CONSIGNEEADD3,";
            Strsql += "''  CONSIGNEECITY,";
            Strsql += "'' CONSIGCOUNTRY,";
            Strsql += "'' CONSIGNEEZIP,";
            Strsql += "'' CONSIGNEEPHONE,";
            Strsql += "'' CONSIGNEEFAX,";
            Strsql += "''CONSIGNEEEMAIL,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME";
            Strsql += "FROM JOB_CARD_TRN JAE,";
            Strsql += "  HAWB_EXP_TBL  HAWB, ";
            Strsql += "MAWB_EXP_TBL  MAWB, ";
            Strsql += "AIRLINE_MST_TBL AMST,";
            Strsql += "AIRLINE_CONTACT_DTLS ADTLS,";
            Strsql += "COUNTRY_MST_TBL ACOUNTRY,";
            //Strsql &= vbCrLf & "AGENT_MST_TBL DPMST,"
            //Strsql &= vbCrLf & "AGENT_CONTACT_DTLS DPDTLS,"
            //Strsql &= vbCrLf & "COUNTRY_MST_TBL DPCOUNTRY,"
            Strsql += "  PORT_MST_TBL POL,";
            Strsql += "  PORT_MST_TBL POD";
            Strsql += " WHERE";
            Strsql += " JAE.MBL_MAWB_FK(+) = MAWB.MAWB_EXP_TBL_PK ";
            Strsql += " AND JAE.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+) ";
            Strsql += " AND   POL.PORT_MST_PK(+)=MAWB.PORT_MST_POL_FK";
            Strsql += " AND   POD.PORT_MST_PK(+)=MAWB.PORT_MST_POD_FK";
            //Strsql &= vbCrLf & " AND MAWB.DP_AGENT_MST_FK = DPMST.AGENT_MST_PK(+)"
            //Strsql &= vbCrLf & "AND DPMST.AGENT_MST_PK = DPDTLS.AGENT_MST_FK(+)"
            //Strsql &= vbCrLf & " AND DPCOUNTRY.COUNTRY_MST_PK(+) = DPDTLS.ADM_COUNTRY_MST_FK"
            Strsql += " AND AMST.AIRLINE_MST_PK(+) = MAWB.AIRLINE_MST_FK";
            Strsql += " AND ADTLS.AIRLINE_MST_FK(+) = AMST.AIRLINE_MST_PK";
            Strsql += "AND ADTLS.ADM_COUNTRY_MST_FK = ACOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += " AND   JAE.JOB_CARD_STATUS(+) = 1";
            Strsql += "   AND   MAWB.MAWB_EXP_TBL_PK IN (" + HBPK + ")";

            try
            {
                return objWF.GetDataSet(Strsql);
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

        /// <summary>
        /// Fetches the MBL report data.
        /// </summary>
        /// <param name="HBPK">The HBPK.</param>
        /// <returns></returns>
        public DataSet FetchMBLReportData(string HBPK)
        {
            string Strsql = null;
            WorkFlow objWF = new WorkFlow();
            if (string.IsNullOrEmpty(HBPK))
            {
                HBPK = "0";
            }
            Strsql = "SELECT JSE.JOB_CARD_TRN_PK JOBPK,";
            Strsql += "JSE.JOBCARD_REF_NO JOBNO,";
            Strsql += "HBL.HBL_EXP_TBL_PK HBPK,";
            Strsql += "HBL.HBL_REF_NO HBREFNO,";
            Strsql += "MBL.MBL_EXP_TBL_PK MBPK,";
            Strsql += "MBL.MBL_REF_NO MBREFNO,";
            Strsql += "MBL.TOTAL_PACK_COUNT PIECES,";
            Strsql += "MBL.TOTAL_GROSS_WEIGHT WEIGHT,";
            Strsql += "OMST.OPERATOR_NAME SHIPPER,";
            Strsql += "ODTLS.ADM_ADDRESS_1  SHIPPERADDD1,";
            Strsql += "ODTLS.ADM_ADDRESS_2 SHIPPERADD2,";
            Strsql += "ODTLS.ADM_ADDRESS_3 SHIPPERADD3,";
            Strsql += "ODTLS.ADM_CITY  SHIPPERCITY,";
            Strsql += "ACOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            Strsql += "ODTLS.ADM_ZIP_CODE  SHIPPERZIP,";
            Strsql += "ODTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            Strsql += "ODTLS.ADM_FAX_NO SHIPPERFAX,";
            Strsql += "ODTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
            Strsql += "MBL.AGENT_NAME CONSIGNEE,";
            Strsql += " MBL.AGENT_ADDRESS CONSIGNEEADD1,";
            Strsql += "'' CONSIGNEEADD2,";
            Strsql += "'' CONSIGNEEADD3,";
            Strsql += "'' CONSIGNEECITY,";
            Strsql += "'' CONSIGCOUNTRY,";
            Strsql += "'' CONSIGNEEZIP,";
            Strsql += "'' CONSIGNEEPHONE,";
            Strsql += "'' CONSIGNEEFAX,";
            Strsql += "'' CONSIGNEEEMAIL,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME";
            Strsql += "FROM JOB_CARD_TRN JSE,";
            Strsql += "  HBL_EXP_TBL  HBL, ";
            Strsql += "MBL_EXP_TBL  MBL, ";
            Strsql += "OPERATOR_MST_TBL OMST,";
            Strsql += "OPERATOR_CONTACT_DTLS ODTLS,";
            Strsql += "COUNTRY_MST_TBL ACOUNTRY,";
            //Strsql &= vbCrLf & "AGENT_MST_TBL DPAGENT,"
            //Strsql &= vbCrLf & "AGENT_CONTACT_DTLS DPADTLS,"
            //Strsql &= vbCrLf & "COUNTRY_MST_TBL DPCOUNTRY,"
            Strsql += "  PORT_MST_TBL POL,";
            Strsql += "  PORT_MST_TBL POD";
            Strsql += " WHERE";
            Strsql += " JSE.MBL_MAWB_FK(+) = MBL.MBL_EXP_TBL_PK  ";
            //Strsql &= vbCrLf & " AND DPAGENT.AGENT_MST_PK(+)=MBL.DP_AGENT_MST_FK "
            //Strsql &= vbCrLf & "  AND DPAGENT.AGENT_MST_PK =DPADTLS.AGENT_MST_FK(+)"
            //Strsql &= vbCrLf & " AND DPCOUNTRY.COUNTRY_MST_PK(+) = DPADTLS.ADM_COUNTRY_MST_FK"
            Strsql += " AND OMST.OPERATOR_MST_PK(+) = MBL.OPERATOR_MST_FK";
            Strsql += "AND ODTLS.OPERATOR_MST_FK = OMST.OPERATOR_MST_PK";
            Strsql += "AND ODTLS.ADM_COUNTRY_MST_FK = ACOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "  AND JSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)";
            Strsql += " AND POL.PORT_MST_PK(+)=MBL.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK(+)=MBL.PORT_MST_POD_FK";
            Strsql += " AND JSE.JOB_CARD_STATUS(+) = 1";
            Strsql += "  AND   MBL.MBL_EXP_TBL_PK IN(" + HBPK + ")";

            try
            {
                return objWF.GetDataSet(Strsql);
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

        #endregion "Fetch Report Data"

        #region "Enhance Search & Lookup Search Block "

        //Pls do the impact the analysis before changing as this function
        //as might be accesed by other forms also.
        /// <summary>
        /// Fetches the exp label job no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchExpLabelJobNo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_LABEL_GENERATION_JOB_NUMBER.LABEL_GENERA_JOB_NUMBER_INS";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "Enhance Search & Lookup Search Block "
    }
}