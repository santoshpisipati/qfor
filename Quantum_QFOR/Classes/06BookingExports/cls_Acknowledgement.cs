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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Acknowledgement : CommonFeatures
    {
        #region "Fetch Booking Details"

        /// <summary>
        /// Fetches the booking details.
        /// </summary>
        /// <param name="SeaBkgPK">The sea BKG pk.</param>
        /// <returns></returns>
        public DataSet FetchBookingDetails(int SeaBkgPK)
        {
            string strSql = null;
            WorkFlow Objwk = new WorkFlow();
            strSql = "SELECT JSE.JOB_CARD_TRN_PK JOBPK, ";
            strSql += "JSE.JOBCARD_REF_NO JOBREFNO, ";
            strSql += "BST.BOOKING_MST_PK BKGPK, ";
            strSql += "BST.BOOKING_REF_NO BKGREFNO, ";
            strSql += "BST.BOOKING_DATE BKGDATE, ";
            strSql += "(CASE WHEN BST.VOYAGE_FLIGHT_NO IS NOT NULL THEN ";
            strSql += "BST.VESSEL_NAME ||'-' || BST.VOYAGE_FLIGHT_NO ";
            strSql += "ELSE";
            strSql += "BST.VESSEL_NAME END ) VESFLIGHT,";
            strSql += "HBL.HBL_REF_NO HBLREFNO,";
            strSql += " MBL.MBL_REF_NO  MBLREFNO,";
            strSql += " JSE.MARKS_NUMBERS MARKS,";

            strSql += " JSE.GOODS_DESCRIPTION GOODS,";
            strSql += "BST.CARGO_TYPE,";
            strSql += "BST.REMARKS_NEW UCRN0,";
            strSql += "'' CLEARANCEPOINT,";
            strSql += " BST.ETD_DATE ETD,";
            strSql += "BST.CUST_CUSTOMER_MST_FK,";
            strSql += "CMST.CUSTOMER_NAME SHIPNAME,";
            strSql += "CMST.CUST_REG_NO SHIPREFNO,";
            strSql += "CDTLS.ADM_ADDRESS_1 SHIPADD1,";
            strSql += "CDTLS.ADM_ADDRESS_2 SHIPADD2,";
            strSql += "CDTLS.ADM_ADDRESS_3 SHIPADD3,";
            strSql += "CDTLS.ADM_CITY SHIPCITY,";
            strSql += "CDTLS.ADM_ZIP_CODE SHIPZIP,";
            strSql += "CDTLS.ADM_EMAIL_ID AS SHIPEMAIL,";
            strSql += "CDTLS.ADM_PHONE_NO_1 SHIPPHONE,";
            strSql += "CDTLS.ADM_FAX_NO SHIPFAX,";
            strSql += " SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            strSql += " CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGADD1,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGADD2,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGADD3,";
            strSql += "CONSIGNEEDTLS.ADM_CITY CONSIGCITY,";
            strSql += "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGZIP,";
            strSql += "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGEMAIL,";
            strSql += "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
            strSql += "CONSIGNEEDTLS.ADM_FAX_NO CONSIGFAX,";
            strSql += " CONSIGCOUNTRY.COUNTRY_NAME CONSICOUNTRY,";
            strSql += " POL.PORT_NAME POLNAME,";
            strSql += " POD.PORT_NAME PODNAME,";
            strSql += " CASE WHEN  PLD.PLACE_NAME IS NOT  NULL  THEN  PLD.PLACE_NAME  ELSE  POD.PORT_NAME  END DELNAME,";
            strSql += " COLPLD.PLACE_NAME COLNAME,";

            strSql += " DBAMST.AGENT_MST_PK DBAGENTPK,";
            strSql += " DBAMST.AGENT_NAME  DBAGENTNAME,";
            strSql += " DBADTLS.ADM_ADDRESS_1  DBAGENTADD1,";
            strSql += " DBADTLS.ADM_ADDRESS_2  DBAGENTADD2,";
            strSql += " DBADTLS.ADM_ADDRESS_3  DBAGENTADD3,";
            strSql += " DBADTLS.ADM_CITY  DBAGENTCITY,";
            strSql += " DBADTLS.ADM_ZIP_CODE DBAGENTZIP,";
            strSql += " DBADTLS.ADM_EMAIL_ID DBAGENTEMAIL,";
            strSql += " DBADTLS.ADM_PHONE_NO_1 DBAGENTPHONE,";
            strSql += " DBADTLS.ADM_FAX_NO DBAGENTFAX,";
            strSql += " DBCOUNTRY.COUNTRY_NAME DBCOUNTRY,";
            strSql += "STMST.INCO_CODE TERMS,";
            strSql += " NVL(JSE.INSURANCE_AMT,0) INSURANCE,";
            strSql += " BST.PYMT_TYPE ,";
            strSql += " CGMST.commodity_group_desc COMMCODE,";
            strSql += " BST.ETA_DATE ETA,";
            strSql += " BST.GROSS_WEIGHT GROSS,";
            strSql += " BST.CHARGEABLE_WEIGHT CHARWT,";
            strSql += " BST.NET_WEIGHT NETWT,";
            strSql += " BST.VOLUME_IN_CBM VOLUME";

            strSql += "FROM   JOB_CARD_TRN JSE,";
            strSql += " BOOKING_MST_TBL BST,";
            strSql += " CUSTOMER_MST_TBL CMST,";
            strSql += " CUSTOMER_MST_TBL CONSIGNEE,";
            strSql += " CUSTOMER_CONTACT_DTLS CDTLS,";
            strSql += " CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
            strSql += " COUNTRY_MST_TBL SHIPCOUNTRY,";
            strSql += " COUNTRY_MST_TBL CONSIGCOUNTRY,";
            strSql += " PORT_MST_TBL POL,";
            strSql += " PORT_MST_TBL POD,";
            strSql += " PLACE_MST_TBL PLD,";
            strSql += " PLACE_MST_TBL COLPLD,";
            strSql += "AGENT_MST_TBL DBAMST,";
            strSql += "AGENT_CONTACT_DTLS DBADTLS,";
            strSql += "COUNTRY_MST_TBL DBCOUNTRY,";
            strSql += "SHIPPING_TERMS_MST_TBL STMST,";
            strSql += " COMMODITY_GROUP_MST_TBL CGMST,";
            strSql += "HBL_EXP_TBL HBL,";
            strSql += "MBL_EXP_TBL MBL";

            strSql += "WHERE BST.BOOKING_MST_PK IN ('" + SeaBkgPK + "')";
            strSql += "AND JSE.HBL_HAWB_FK=HBL.HBL_EXP_TBL_PK(+)";
            strSql += "AND JSE.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK(+)";
            strSql += "AND   CMST.CUSTOMER_MST_PK(+)=BST.CUST_CUSTOMER_MST_FK";
            strSql += "AND   CONSIGNEE.CUSTOMER_MST_PK(+)=BST.CONS_CUSTOMER_MST_FK";
            strSql += "AND   CDTLS.CUSTOMER_MST_FK(+)=CMST.CUSTOMER_MST_PK";
            strSql += "AND CONSIGNEE.CUSTOMER_MST_PK=CONSIGNEEDTLS.CUSTOMER_MST_FK(+)";
            strSql += " AND SHIPCOUNTRY.COUNTRY_MST_PK(+)=CDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CONSIGNEEDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND   JSE.BOOKING_MST_FK(+)=BST.BOOKING_MST_PK";
            strSql += " AND   BST.PORT_MST_POL_FK=POL.PORT_MST_PK(+)";
            strSql += " AND   BST.PORT_MST_POD_FK=POD.PORT_MST_PK(+)";
            strSql += " AND   BST.DEL_PLACE_MST_FK=PLD.PLACE_PK(+)";
            strSql += " AND   BST.COL_PLACE_MST_FK=COLPLD.PLACE_PK(+)";
            strSql += " AND   BST.DP_AGENT_MST_FK=DBAMST.AGENT_MST_PK(+)";
            strSql += " AND   DBAMST.AGENT_MST_PK=DBADTLS.AGENT_MST_FK(+)";
            strSql += "AND DBCOUNTRY.COUNTRY_MST_PK(+)=DBADTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND  STMST.SHIPPING_TERMS_MST_PK(+)=BST.SHIPPING_TERMS_MST_FK";
            strSql += " AND  BST.COMMODITY_GROUP_FK=CGMST.COMMODITY_GROUP_PK(+)";
            try
            {
                return Objwk.GetDataSet(strSql);
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
        /// Fetches the sea containers.
        /// </summary>
        /// <param name="BkgPK">The BKG pk.</param>
        /// <returns></returns>
        public DataSet FetchSeaContainers(string BkgPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT BST.BOOKING_MST_PK BKGPK, JSE.CONTAINER_NUMBER CONTAINER";
            Strsql += "FROM JOB_TRN_CONT JSE,BOOKING_MST_TBL BST,JOB_CARD_TRN JS";
            Strsql += "WHERE BST.BOOKING_MST_PK = JS.BOOKING_MST_FK";
            Strsql += "AND JSE.JOB_CARD_TRN_FK=JS.JOB_CARD_TRN_PK";
            Strsql += " AND BST.BOOKING_MST_PK IN (" + BkgPK + ")";
            try
            {
                return Objwk.GetDataSet(Strsql);
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
        /// Fetches the air ack containers.
        /// </summary>
        /// <param name="BkgPK">The BKG pk.</param>
        /// <returns></returns>
        public DataSet FetchAirAckContainers(string BkgPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT BST.BOOKING_MST_PK BKGPK, JSE.PALETTE_SIZE CONTAINER";
            Strsql += "FROM JOB_TRN_CONT JSE,BOOKING_MST_TBL BST,JOB_CARD_TRN JS";
            Strsql += "WHERE BST.BOOKING_MST_PK = JS.BOOKING_MST_FK";
            Strsql += "AND JSE.JOB_CARD_TRN_FK=JS.JOB_CARD_TRN_PK";
            Strsql += " AND BST.BOOKING_MST_PK IN (" + BkgPK + ")";
            try
            {
                return Objwk.GetDataSet(Strsql);
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
        /// Fetches the air booking details.
        /// </summary>
        /// <param name="AIRBkgPK">The air BKG pk.</param>
        /// <returns></returns>
        public DataSet FetchAirBookingDetails(int AIRBkgPK)
        {
            string strSql = null;
            WorkFlow Objwk = new WorkFlow();
            strSql = "SELECT JSE.JOB_CARD_TRN_PK JOBPK,";
            strSql += "JSE.JOBCARD_REF_NO JOBREFNO,";
            strSql += "BST.BOOKING_MST_PK BKGPK,";
            strSql += "BST.BOOKING_REF_NO BKGREFNO,";
            strSql += "BST.BOOKING_DATE BKGDATE,";
            strSql += "BST.VOYAGE_FLIGHT_NO VESFLIGHT,";
            strSql += "HAWB.HAWB_REF_NO HBLREFNO,";
            strSql += "MAWB.MAWB_REF_NO MBLREFNO,";
            strSql += "JSE.MARKS_NUMBERS MARKS,";
            strSql += "JSE.GOODS_DESCRIPTION GOODS,";
            strSql += "BST.CARGO_TYPE,";
            strSql += "BST.REMARKS_NEW UCRN0,";
            strSql += "'' CLEARANCEPOINT,";
            strSql += "BST.ETD_DATE ETD,";
            strSql += "BST.CUST_CUSTOMER_MST_FK,";
            strSql += "CMST.CUSTOMER_NAME SHIPNAME,";
            strSql += "CMST.CUST_REG_NO SHIPREFNO,";
            strSql += "CDTLS.ADM_ADDRESS_1 SHIPADD1,";
            strSql += "CDTLS.ADM_ADDRESS_2 SHIPADD2,";
            strSql += "CDTLS.ADM_ADDRESS_3 SHIPADD3,";
            strSql += "CDTLS.ADM_CITY SHIPCITY,";
            strSql += "CDTLS.ADM_ZIP_CODE SHIPZIP,";
            strSql += "CDTLS.ADM_EMAIL_ID AS SHIPEMAIL,";
            strSql += "CDTLS.ADM_PHONE_NO_1 SHIPPHONE,";
            strSql += "CDTLS.ADM_FAX_NO SHIPFAX,";
            strSql += "SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            strSql += " (CASE  WHEN CONSIGNEE.CUSTOMER_NAME IS NULL THEN ";
            strSql += " (SELECT CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL";
            strSql += "  WHERE CUSTOMER_MST_PK = BST.CONS_CUSTOMER_MST_FK) ";
            strSql += " ELSE CONSIGNEE.CUSTOMER_NAME END) CONSIGNEENAME,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGADD1,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGADD2,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGADD3,";
            strSql += "CONSIGNEEDTLS.ADM_CITY CONSIGCITY,";
            strSql += "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGZIP,";
            strSql += "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGEMAIL,";
            strSql += " CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
            strSql += "CONSIGNEEDTLS.ADM_FAX_NO CONSIGFAX,";
            strSql += "CONSIGCOUNTRY.COUNTRY_NAME CONSICOUNTRY,";
            strSql += "POL.PORT_NAME POLNAME,";
            strSql += "POD.PORT_NAME PODNAME,";
            strSql += " PLD.PLACE_NAME DELNAME,";
            strSql += " COLPLD.PLACE_NAME COLNAME,";

            strSql += "DBAMST.AGENT_MST_PK DBAGENTPK,";
            strSql += "DBAMST.AGENT_NAME  DBAGENTNAME,";
            strSql += "DBADTLS.ADM_ADDRESS_1  DBAGENTADD1,";
            strSql += "DBADTLS.ADM_ADDRESS_2  DBAGENTADD2,";
            strSql += "DBADTLS.ADM_ADDRESS_3  DBAGENTADD3,";
            strSql += "DBADTLS.ADM_CITY  DBAGENTCITY,";
            strSql += "DBADTLS.ADM_ZIP_CODE DBAGENTZIP,";
            strSql += "DBADTLS.ADM_EMAIL_ID DBAGENTEMAIL,";
            strSql += "DBADTLS.ADM_PHONE_NO_1 DBAGENTPHONE,";
            strSql += "DBADTLS.ADM_FAX_NO DBAGENTFAX,";
            strSql += "DBCOUNTRY.COUNTRY_NAME DBCOUNTRY,";
            strSql += "STMST.INCO_CODE TERMS,";
            strSql += "NVL(JSE.INSURANCE_AMT,0) INSURANCE,";
            strSql += "BST.PYMT_TYPE ,";
            strSql += "CGMST.commodity_group_desc COMMCODE,";
            strSql += "BST.ETA_DATE ETA,";
            strSql += "BST.GROSS_WEIGHT GROSS,";
            strSql += "BST.CHARGEABLE_WEIGHT CHARWT,";
            strSql += "'' NETWT,";
            strSql += "BST.VOLUME_IN_CBM VOLUME";

            strSql += "FROM   JOB_CARD_TRN JSE,";
            strSql += "BOOKING_MST_TBL BST,";
            strSql += "CUSTOMER_MST_TBL CMST,";
            strSql += "CUSTOMER_MST_TBL CONSIGNEE,";
            strSql += "CUSTOMER_CONTACT_DTLS CDTLS,";
            strSql += "CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
            strSql += "COUNTRY_MST_TBL SHIPCOUNTRY,";
            strSql += "COUNTRY_MST_TBL CONSIGCOUNTRY,";
            strSql += "PORT_MST_TBL POL,";
            strSql += "PORT_MST_TBL POD,";
            strSql += "PLACE_MST_TBL PLD,";
            strSql += "PLACE_MST_TBL COLPLD,";
            strSql += "AGENT_MST_TBL DBAMST,";
            strSql += "AGENT_CONTACT_DTLS DBADTLS,";
            strSql += "COUNTRY_MST_TBL DBCOUNTRY,";
            strSql += "SHIPPING_TERMS_MST_TBL STMST,";
            strSql += "COMMODITY_GROUP_MST_TBL CGMST,";
            strSql += "HAWB_EXP_TBL HAWB,";
            strSql += "MAWB_EXP_TBL MAWB";

            strSql += "WHERE";
            strSql += "BST.BOOKING_MST_PK IN (" + AIRBkgPK + ")";
            strSql += " AND ";
            strSql += "JSE.HBL_HAWB_FK=HAWB.HAWB_EXP_TBL_PK(+)";
            strSql += " AND JSE.MBL_MAWB_FK=MAWB.MAWB_EXP_TBL_PK(+)";
            strSql += "AND   CMST.CUSTOMER_MST_PK(+)=BST.CUST_CUSTOMER_MST_FK";
            strSql += "AND   CONSIGNEE.CUSTOMER_MST_PK(+)=BST.CONS_CUSTOMER_MST_FK";
            strSql += "AND   CDTLS.CUSTOMER_MST_FK(+)=CMST.CUSTOMER_MST_PK";
            strSql += "AND CONSIGNEE.CUSTOMER_MST_PK=CONSIGNEEDTLS.CUSTOMER_MST_FK(+)";
            strSql += "AND SHIPCOUNTRY.COUNTRY_MST_PK(+)=CDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CONSIGNEEDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND   JSE.BOOKING_MST_FK(+)=BST.BOOKING_MST_PK";
            strSql += "AND   BST.PORT_MST_POL_FK=POL.PORT_MST_PK(+)";
            strSql += "AND   BST.PORT_MST_POD_FK=POD.PORT_MST_PK(+)";
            strSql += "AND   BST.DEL_PLACE_MST_FK=PLD.PLACE_PK(+)";
            strSql += "AND   BST.COL_PLACE_MST_FK=COLPLD.PLACE_PK(+)";
            strSql += "AND   BST.DP_AGENT_MST_FK=DBAMST.AGENT_MST_PK(+)";
            strSql += "AND   DBAMST.AGENT_MST_PK=DBADTLS.AGENT_MST_FK(+)";
            strSql += "AND DBCOUNTRY.COUNTRY_MST_PK(+)=DBADTLS.ADM_COUNTRY_MST_FK";
            strSql += "AND  STMST.SHIPPING_TERMS_MST_PK(+)=BST.CUST_CUSTOMER_MST_FK";
            strSql += "AND  BST.COMMODITY_GROUP_FK=CGMST.COMMODITY_GROUP_PK(+)";
            try
            {
                return Objwk.GetDataSet(strSql);
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
        /// Fetches the air containers.
        /// </summary>
        /// <param name="BkgPK">The BKG pk.</param>
        /// <returns></returns>
        public DataSet FetchAirContainers(string BkgPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT BAT.BOOKING_MST_PK BKGPK, JAE.PALETTE_SIZE CONTAINER";
            Strsql += "FROM JOB_TRN_CONT JAE,BOOKING_MST_TBL BAT,JOB_CARD_TRN JS";
            Strsql += "WHERE BAT.BOOKING_MST_PK = JS.BOOKING_MST_FK";
            Strsql += "AND JAE.JOB_CARD_TRN_FK=JS.JOB_CARD_TRN_PK";
            Strsql += " AND BAT.BOOKING_MST_PK IN (" + BkgPK + ")";
            try
            {
                return Objwk.GetDataSet(Strsql);
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
    }
}