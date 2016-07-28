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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsInvoiceListAir : CommonFeatures
    {
        #region "Fetch All"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="strInvPK">The string inv pk.</param>
        /// <param name="strJobPK">The string job pk.</param>
        /// <param name="strHBLPK">The string HBLPK.</param>
        /// <param name="strMBLPK">The string MBLPK.</param>
        /// <param name="strCustPK">The string customer pk.</param>
        /// <param name="strVoyage">The string voyage.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string strInvPK = "", string strJobPK = "", string strHBLPK = "", string strMBLPK = "", string strCustPK = "", string strVoyage = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ",
        long usrLocFK = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCondition = "FROM ";
            strCondition += "INV_CUST_AIR_EXP_TBL INV,";
            strCondition += "JOB_CARD_TRN JOB,";
            strCondition += "HAWB_EXP_TBL HAWB,";
            strCondition += "MAWB_EXP_TBL MAWB,";
            strCondition += "CUSTOMER_MST_TBL CMT,";
            strCondition += "CURRENCY_TYPE_MST_TBL CUMT,";
            strCondition += "USER_MST_TBL UMT";
            strCondition += "WHERE";
            strCondition += "INV.JOB_CARD_AIR_EXP_FK = JOB.JOB_CARD_TRN_PK";
            strCondition += "AND JOB.JOB_CARD_TRN_PK=HAWB.JOB_CARD_AIR_EXP_FK (+)";
            strCondition += "AND JOB.MBL_MAWB_FK=MAWB.MAWB_EXP_TBL_PK (+)";
            strCondition += "AND JOB.SHIPPER_CUST_MST_FK=CMT.CUSTOMER_MST_PK (+)";
            strCondition += "AND INV.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ";
            strCondition += "AND INV.CREATED_BY_FK = UMT.USER_MST_PK ";
            //If BlankGrid = 0 Then
            //    strCondition &= vbCrLf & " AND 1=2 "
            //End If
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (!string.IsNullOrEmpty(strInvPK.Trim()))
            {
                strCondition += "AND UPPER(INV.INVOICE_REF_NO) LIKE '%" + strInvPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strJobPK.Trim()))
            {
                strCondition += "AND UPPER(JOB.JOBCARD_REF_NO) LIKE '%" + strJobPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strHBLPK.Trim()))
            {
                strCondition += "AND UPPER(HAWB.HAWB_REF_NO) LIKE '%" + strHBLPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strMBLPK.Trim()))
            {
                strCondition += "AND UPPER(MAWB.MAWB_REF_NO) LIKE '%" + strMBLPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strCustPK.Trim()))
            {
                strCondition += "AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + strCustPK.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strVoyage.Trim()))
            {
                strCondition += "AND JOB.VOYAGE_FLIGHT_NO LIKE '%" + strVoyage.Trim() + "%'";
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
            strSQL += "SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL += "( SELECT ";
            strSQL += "INV.INV_CUST_AIR_EXP_PK,";
            strSQL += "INV.JOB_CARD_AIR_EXP_FK,";
            strSQL += "JOB.HBL_HAWB_FK,";
            strSQL += "JOB.MBL_MAWB_FK,";
            strSQL += "JOB.SHIPPER_CUST_MST_FK,";
            strSQL += "INV.CURRENCY_MST_FK,";
            strSQL += "INV.INVOICE_REF_NO,";
            strSQL += "INV.INVOICE_DATE AS INVDATE,";
            strSQL += "JOB.JOBCARD_REF_NO,";
            strSQL += "HAWB.HAWB_REF_NO,";
            strSQL += "MAWB.MAWB_REF_NO,";
            strSQL += "CMT.CUSTOMER_NAME,";
            strSQL += "JOB.VOYAGE_FLIGHT_NO,";
            strSQL += "CUMT.CURRENCY_ID,";
            strSQL += "INV.NET_PAYABLE";
            strSQL += strCondition;
            if (SortColumn == "INVDATE")
            {
                SortColumn = "INV.INVOICE_DATE";
            }
            strSQL += " ORDER BY " + SortColumn + SortType + "  ,INVOICE_REF_NO DESC   ) q  ) ";
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

        /// <summary>
        /// Fetches the uninvoiced jc.
        /// </summary>
        /// <param name="strJobPK">The string job pk.</param>
        /// <param name="strHBLPK">The string HBLPK.</param>
        /// <param name="strMBLPK">The string MBLPK.</param>
        /// <param name="strCustPK">The string customer pk.</param>
        /// <param name="strVoyage">The string voyage.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet FetchUninvoicedJC(string strJobPK = "", string strHBLPK = "", string strMBLPK = "", string strCustPK = "", string strVoyage = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC   ", long usrLocFK = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCondition = "FROM ";
            strCondition += "JOB_CARD_TRN JOB,";
            strCondition += "HAWB_EXP_TBL HAWB,";
            strCondition += "MAWB_EXP_TBL MAWB,";
            strCondition += "CUSTOMER_MST_TBL CMT,";
            strCondition += "USER_MST_TBL UMT";
            strCondition += "WHERE JOB.JOB_CARD_STATUS=1";
            strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + "";
            strCondition += "AND JOB.CREATED_BY_FK = UMT.USER_MST_PK ";
            strCondition += "AND JOB.HBL_HAWB_FK=HAWB.HAWB_EXP_TBL_PK (+)";
            strCondition += "AND JOB.MBL_MAWB_FK=MAWB.MAWB_EXP_TBL_PK (+)";
            strCondition += "AND JOB.SHIPPER_CUST_MST_FK=CMT.CUSTOMER_MST_PK (+)";
            if (BlankGrid == 0)
            {
                strCondition += " AND 1=2 ";
            }
            if (!string.IsNullOrEmpty(strJobPK.Trim()))
            {
                strCondition += "AND JOB.JOB_CARD_TRN_PK=" + strJobPK.Trim();
            }

            if (!string.IsNullOrEmpty(strHBLPK.Trim()))
            {
                strCondition += "AND JOB.HBL_HAWB_FK=" + strHBLPK.Trim();
            }

            if (!string.IsNullOrEmpty(strMBLPK.Trim()))
            {
                strCondition += "AND JOB.MBL_MAWB_FK=" + strMBLPK.Trim();
            }

            if (!string.IsNullOrEmpty(strCustPK.Trim()))
            {
                strCondition += "AND JOB.SHIPPER_CUST_MST_FK=" + strCustPK.Trim();
            }

            if (!string.IsNullOrEmpty(strVoyage.Trim()))
            {
                strCondition += "AND JOB.VOYAGE_FLIGHT_NO LIKE '%" + strVoyage.Trim() + "%'";
            }

            strSQL = " SELECT SR_NO,INV_CUST_AIR_EXP_PK,JOB_CARD_AIR_EXP_FK,HBL_HAWB_FK,";
            strSQL += "MBL_MAWB_FK,SHIPPER_CUST_MST_FK,CURRENCY_MST_FK,";
            strSQL += "INVOICE_REF_NO,INVDATE,JOBCARD_REF_NO,HAWB_REF_NO,";
            strSQL += "MAWB_REF_NO,CUSTOMER_NAME, VOYAGE_FLIGHT_NO, CURRENCY_ID, NET_PAYABLE";
            strSQL += "FROM (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            //strSQL &= vbCrLf & "( SELECT "
            strSQL += "(SELECT  * FROM (SELECT ";
            strSQL += "0 INV_CUST_AIR_EXP_PK,";
            strSQL += "JOB.JOB_CARD_TRN_PK JOB_CARD_AIR_EXP_FK,";
            strSQL += "JOB.HBL_HAWB_FK,";
            strSQL += "JOB.MBL_MAWB_FK,";
            strSQL += "JOB.SHIPPER_CUST_MST_FK,";
            strSQL += "0 CURRENCY_MST_FK,";
            strSQL += "'' INVOICE_REF_NO,";
            strSQL += "NULL AS INVDATE,";
            strSQL += "JOB.JOBCARD_REF_NO,";
            strSQL += "HAWB.HAWB_REF_NO,";
            strSQL += "MAWB.MAWB_REF_NO,";
            strSQL += "CMT.CUSTOMER_NAME,";
            strSQL += "JOB.VOYAGE_FLIGHT_NO,";
            strSQL += "'' CURRENCY_ID,";
            strSQL += "NULL NET_PAYABLE,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_PIA PIA WHERE PIA.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK AND PIA.INV_CUST_TRN_FK IS NULL AND PIA.INV_AGENT_TRN_FK IS NULL) AS PC,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_FD FD WHERE FD.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK AND FD.INV_CUST_TRN_FK IS NULL AND FD.INV_AGENT_TRN_FK IS NULL) AS FC,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_OTH_CHRG OTH WHERE OTH.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK AND OTH.INV_CUST_TRN_FK IS NULL AND OTH.INV_AGENT_TRN_FK IS NULL) AS OC,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_PIA PIA WHERE PIA.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK) AS PC1,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_FD FD WHERE FD.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK) AS FC1,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_OTH_CHRG OTH WHERE OTH.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK) AS OC1";
            strSQL += strCondition;
            if (SortColumn == "INVDATE")
            {
                //SortColumn = "INV.INVOICE_DATE"
            }
            //strSQL &= vbCrLf & " ORDER BY " & SortColumn & SortType & " ) q  ) "
            //strSQL &= vbCrLf & " WHERE  (PC>0 OR FC>0 OR OC>0 OR (PC1=0 AND FC1=0 AND OC1=0))"

            strSQL += " ORDER BY " + SortColumn + SortType + " ))q ";
            strSQL += " WHERE  (PC>0 OR FC>0 OR OC>0 OR (PC1=0 AND FC1=0 AND OC1=0)))";

            string strCount = null;
            strCount = "SELECT COUNT(*) FROM ( ";
            strCount += strSQL;
            strCount += ")";
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

            //strSQL &= vbCrLf & " AND (SR_NO BETWEEN " & start & " AND " & last & ")"
            strSQL += " where SR_NO BETWEEN " + start + " AND " + last + " ";

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

        #endregion "Fetch All"

        #region "Invoice To Customer -- Exports AIR --  Reports Section"

        /// <summary>
        /// Fetches the inv to customer exp air main.
        /// </summary>
        /// <param name="InvPK">The inv pk.</param>
        /// <returns></returns>
        public DataSet FetchInvToCustExpAirMain(Int32 InvPK)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = " SELECT INVCUSTEXP.CONSOL_INVOICE_PK INVPK,";
            Strsql += "INVCUSTEXP.INVOICE_REF_NO INVREFNO,";
            Strsql += "NVL(INVCUSTEXP.DISCOUNT_AMT, 0) DICSOUNT,";
            Strsql += "INVTAGTEXP.Tax_Pcnt VATPCT,";
            Strsql += "INVTAGTEXP.Tax_Amt VATAMT,";
            Strsql += "JAE.JOB_CARD_TRN_PK JOBPK,";
            Strsql += "JAE.JOBCARD_REF_NO JOBREFNO,";
            Strsql += "'' CLEARANCEPOINT,";
            Strsql += "JAE.ETD_DATE ETD,";
            Strsql += "JAE.ETA_DATE ETA,";
            Strsql += "1 CARGOTYPE,";
            Strsql += "SHIPMST.CUSTOMER_NAME SHIPPER,";
            Strsql += "BAT.CUSTOMER_REF_NO  SHIPPERREFNO,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_1 SHIPPERADD1,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
            Strsql += "SHIPDTLS.ADM_CITY SHIPPERCITY,";
            Strsql += "SHIPDTLS.ADM_ZIP_CODE SHIPPERZIP,";
            Strsql += "SHIPDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            Strsql += "SHIPDTLS.ADM_FAX_NO SHIPPERFAX,";
            Strsql += "SHIPDTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,";
            Strsql += "SHIPMST.VAT_NO VATNO,";
            Strsql += "SHIPMST.CREDIT_DAYS PAYMENTDAYS,";
            Strsql += "CONSMST.CUSTOMER_NAME CONSIGNEE,";
            Strsql += "CONSDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,";
            Strsql += "CONSDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,";
            Strsql += "CONSDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,";
            Strsql += "CONSDTLS.ADM_CITY CONSIGNEECITY,";
            Strsql += "CONSDTLS.ADM_ZIP_CODE CONSIGNEEZIP,";
            Strsql += "CONSDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,";
            Strsql += "CONSDTLS.ADM_FAX_NO CONSIGNEEFAX,";
            Strsql += "CONSDTLS.ADM_EMAIL_ID CONSIGNEEMAIL,";
            Strsql += "CONSCOUNTRY.COUNTRY_NAME CONSIGNEEOUNTRY,";
            Strsql += "FEMST.FREIGHT_ELEMENT_NAME FREIGHTNAME,";
            Strsql += "nvl(INVTAGTEXP.AMT_IN_INV_CURR,0) FREIGHTAMT,";
            Strsql += "INVTAGTEXP.TAX_PCNT FRETAXPCNT,";
            Strsql += "INVTAGTEXP.TAX_AMT FRETAXANT,";
            Strsql += "CURRMST.CURRENCY_ID CURRID,";
            Strsql += "CURRMST.CURRENCY_NAME CURRNAME,";
            Strsql += "JAE.VOYAGE_FLIGHT_NO VES_FLIGHT,";
            Strsql += "JAE.PYMT_TYPE PYMT,";
            Strsql += "JAE.GOODS_DESCRIPTION GOODS,";
            Strsql += " JAE.MARKS_NUMBERS MARKS,";
            Strsql += "NVL(JAE.INSURANCE_AMT, 0) INSURANCE,";
            Strsql += "STMST.INCO_CODE TERMS,";
            Strsql += "COLMST.PLACE_NAME COLPLACE,";
            Strsql += "DELMST.PLACE_NAME DELPLACE,";
            Strsql += "POLMST.PORT_NAME POL,";
            Strsql += "PODMST.PORT_NAME POD,";
            Strsql += "HAWB.HAWB_REF_NO HBLREFNO ,";
            Strsql += " MAWB.MAWB_REF_NO MBLREFNO ,";
            Strsql += "CGMST.COMMODITY_GROUP_DESC COMMODITY,";
            Strsql += "SUM(JAEC.VOLUME_IN_CBM) VOLUME,";
            Strsql += "SUM(JAEC.GROSS_WEIGHT) GROSS,";
            Strsql += "'' NETWT,";
            Strsql += "SUM(JAEC.CHARGEABLE_WEIGHT) CHARWT ,";
            Strsql += "INVCUSTEXP.INVOICE_DATE ";
            Strsql += "FROM CONSOL_INVOICE_TBL     INVCUSTEXP,";
            Strsql += "CURRENCY_TYPE_MST_TBL    CURRMST,";
            Strsql += "CONSOL_INVOICE_TRN_TBL INVTAGTEXP,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL  FEMST,";
            Strsql += "JOB_CARD_TRN     JAE,";
            Strsql += "JOB_TRN_CONT     JAEC,";
            Strsql += " SHIPPING_TERMS_MST_TBL   STMST,";
            Strsql += "BOOKING_MST_TBL          BAT,";
            Strsql += "PLACE_MST_TBL            COLMST,";
            Strsql += "PLACE_MST_TBL            DELMST,";
            Strsql += "PORT_MST_TBL             POLMST,";
            Strsql += "PORT_MST_TBL             PODMST,";
            Strsql += "HAWB_EXP_TBL              HAWB,";
            Strsql += "MAWB_EXP_TBL              MAWB,";
            Strsql += "COMMODITY_GROUP_MST_TBL  CGMST,";
            Strsql += "CUSTOMER_MST_TBL         SHIPMST,";
            Strsql += "CUSTOMER_CONTACT_DTLS    SHIPDTLS,";
            Strsql += "COUNTRY_MST_TBL          SHIPCOUNTRY,";
            Strsql += "CUSTOMER_MST_TBL         CONSMST,";
            Strsql += "CUSTOMER_CONTACT_DTLS    CONSDTLS,";
            Strsql += " COUNTRY_MST_TBL CONSCOUNTRY";
            Strsql += "WHERE(INVTAGTEXP.JOB_CARD_FK = JAE.JOB_CARD_TRN_PK)";
            Strsql += "AND CURRMST.CURRENCY_MST_PK(+) = INVCUSTEXP.CURRENCY_MST_FK";
            Strsql += "AND INVTAGTEXP.CONSOL_INVOICE_FK = INVCUSTEXP.CONSOL_INVOICE_PK";
            Strsql += "AND FEMST.FREIGHT_ELEMENT_MST_PK(+) = INVTAGTEXP.Frt_Oth_Element_Fk";
            Strsql += "AND JAE.JOB_CARD_TRN_PK = JAEC.JOB_CARD_TRN_FK(+)";
            Strsql += "AND STMST.SHIPPING_TERMS_MST_PK(+) = JAE.SHIPPING_TERMS_MST_FK";
            Strsql += "AND BAT.BOOKING_MST_PK(+) = JAE.BOOKING_MST_FK";
            Strsql += "AND COLMST.PLACE_PK(+) = BAT.COL_PLACE_MST_FK";
            Strsql += "AND DELMST.PLACE_PK(+) = BAT.DEL_PLACE_MST_FK";
            Strsql += "AND POLMST.PORT_MST_PK = BAT.PORT_MST_POL_FK";
            Strsql += "AND PODMST.PORT_MST_PK = BAT.PORT_MST_POD_FK";
            Strsql += "AND HAWB.HAWB_EXP_TBL_PK(+) = JAE.HBL_HAWB_FK";
            Strsql += "AND MAWB.MAWB_EXP_TBL_PK(+) = JAE.MBL_MAWB_FK";
            Strsql += "AND CGMST.COMMODITY_GROUP_PK(+) = JAE.COMMODITY_GROUP_FK";
            Strsql += "AND CONSMST.CUSTOMER_MST_PK(+) = JAE.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND CONSDTLS.CUSTOMER_MST_FK(+) = CONSMST.CUSTOMER_MST_PK";
            Strsql += "AND CONSDTLS.ADM_COUNTRY_MST_FK = CONSCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "AND SHIPMST.CUSTOMER_MST_PK(+) = JAE.SHIPPER_CUST_MST_FK";
            Strsql += "AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK";
            Strsql += "AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "AND INVTAGTEXP.CONSOL_INVOICE_TRN_PK =" + InvPK;
            Strsql += "GROUP BY INVCUSTEXP.CONSOL_INVOICE_PK,";
            Strsql += "INVCUSTEXP.INVOICE_REF_NO,";
            Strsql += "INVCUSTEXP.DISCOUNT_AMT,";
            Strsql += " INVTAGTEXP.Tax_Pcnt,";
            Strsql += " INVTAGTEXP.Tax_Amt,";
            Strsql += " JAE.JOB_CARD_TRN_PK,";
            Strsql += " JAE.JOBCARD_REF_NO,";
            Strsql += "JAE.ETD_DATE,";
            Strsql += "JAE.ETA_DATE,";
            Strsql += "SHIPMST.CUSTOMER_NAME,";
            Strsql += "BAT.CUSTOMER_REF_NO ,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_1,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_2,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_3,";
            Strsql += "SHIPDTLS.ADM_CITY,";
            Strsql += "SHIPDTLS.ADM_ZIP_CODE,";
            Strsql += "SHIPDTLS.ADM_PHONE_NO_1,";
            Strsql += "SHIPDTLS.ADM_FAX_NO,";
            Strsql += "SHIPDTLS.ADM_EMAIL_ID,";
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME,";
            Strsql += " SHIPMST.VAT_NO,";
            Strsql += "SHIPMST.CREDIT_DAYS,";
            Strsql += "CONSMST.CUSTOMER_NAME,";
            Strsql += "CONSDTLS.ADM_ADDRESS_1,";
            Strsql += "CONSDTLS.ADM_ADDRESS_2,";
            Strsql += "CONSDTLS.ADM_ADDRESS_3,";
            Strsql += "CONSDTLS.ADM_CITY,";
            Strsql += "CONSDTLS.ADM_ZIP_CODE,";
            Strsql += "CONSDTLS.ADM_PHONE_NO_1,";
            Strsql += "CONSDTLS.ADM_FAX_NO,";
            Strsql += "CONSDTLS.ADM_EMAIL_ID,";
            Strsql += "CONSCOUNTRY.COUNTRY_NAME,";
            Strsql += "FEMST.FREIGHT_ELEMENT_NAME,";
            Strsql += "INVTAGTEXP.AMT_IN_INV_CURR,";
            Strsql += "INVTAGTEXP.TAX_PCNT,";
            Strsql += "INVTAGTEXP.TAX_AMT,";
            Strsql += " CURRMST.CURRENCY_ID,";
            Strsql += "CURRMST.CURRENCY_NAME,";
            Strsql += "JAE.VOYAGE_FLIGHT_NO,";
            Strsql += "JAE.PYMT_TYPE,";
            Strsql += "JAE.GOODS_DESCRIPTION,";
            Strsql += "JAE.MARKS_NUMBERS,";
            Strsql += "JAE.INSURANCE_AMT,";
            Strsql += "STMST.INCO_CODE,";
            Strsql += "COLMST.PLACE_NAME,";
            Strsql += "DELMST.PLACE_NAME,";
            Strsql += "POLMST.PORT_NAME,";
            Strsql += "PODMST.PORT_NAME,";
            Strsql += "HAWB.HAWB_REF_NO,";
            Strsql += "MAWB.MAWB_REF_NO,";
            Strsql += "INVCUSTEXP.INVOICE_DATE,";
            Strsql += "CGMST.COMMODITY_GROUP_DESC";

            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(Int64 nInvPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT ICSE.CONSOL_INVOICE_TRN_PK ,JTEC.PALETTE_SIZE CONTAINER";
            strSQL += "FROM CONSOL_INVOICE_TRN_TBL ICSE,";
            strSQL += "JOB_CARD_TRN  JSE,";
            strSQL += "JOB_TRN_CONT JTEC";
            strSQL += "WHERE(ICSE.JOB_CARD_FK = JSE.JOB_CARD_TRN_PK)";
            strSQL += "AND JTEC.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK";
            strSQL += "AND ICSE.CONSOL_INVOICE_TRN_PK = " + nInvPK;
            try
            {
                return (objWK.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the inv to cus exp air details.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
        public DataSet FetchInvToCusExpAirDetails(Int32 JobPk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = "  SELECT J.JOB_CARD_TRN_PK,  J.JOBCARD_REF_NO,  '' HBL_REF_NO, ";
            Strsql += "  J.MARKS_NUMBERS,   J.GOODS_DESCRIPTION,  SUM(HH.VOLUME_IN_CBM) AS CUBEM3, ";
            Strsql += "  SUM(HH.GROSS_WEIGHT) AS GROSS,   SUM(HH.CHARGEABLE_WEIGHT) AS NET  FROM JOB_CARD_TRN J, JOB_TRN_CONT HH ";
            Strsql += "  WHERE J.JOB_CARD_TRN_PK=" + JobPk;
            Strsql += "  AND HH.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK  ";
            Strsql += "  GROUP BY J.JOB_CARD_TRN_PK, J.JOBCARD_REF_NO, J.MARKS_NUMBERS, J.GOODS_DESCRIPTION ";

            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the inv to cus exp air desc.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="InvRefNo">The inv reference no.</param>
        /// <returns></returns>
        public DataSet FetchInvToCusExpAirDesc(Int32 JobPk, string InvRefNo)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = "  SELECT JA.JOB_CARD_TRN_PK,JA.JOBCARD_REF_NO,ICT.COST_FRT_ELEMENT,ICT.COST_FRT_ELEMENT_FK,";
            Strsql += "  DECODE(ICT.COST_FRT_ELEMENT,1,";
            Strsql += "  (SELECT CE.COST_ELEMENT_NAME FROM COST_ELEMENT_MST_TBL CE  ";
            Strsql += "  WHERE CE.COST_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK) ,2,";
            Strsql += "  (SELECT FE.FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL FE WHERE";
            Strsql += "   FE.FREIGHT_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK),3,";
            Strsql += " (SELECT FE.FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL FE WHERE";
            Strsql += "   FE.FREIGHT_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK)) AS DESCRIPTION,";
            Strsql += "  ICT.AMT_IN_INV_CURR AS CHARGES,ICT.TAX_AMT AS TAX,";
            Strsql += "    '' AS CODE, '' AS TAXCODE,ICT.TOT_AMT AS TOTCHARGE,NVL(ICA.VAT_AMT,0) AS VAT_AMT,ICA.INVOICE_REF_NO,CT.CURRENCY_ID,CT.CURRENCY_NAME";
            Strsql += "   ,ICA.VAT_PCNT,ICA.VAT_AMT,NVL(ICA.DISCOUNT_AMT,0) AS DISCOUNT_AMT,(ICT.TOT_AMT+ICA.VAT_AMT-ICA.DISCOUNT_AMT) AS TOTAMOUNTDUE";
            Strsql += "  FROM JOB_CARD_TRN JA,INV_CUST_AIR_EXP_TBL ICA,INV_CUST_TRN_AIR_EXP_TBL ICT,CURRENCY_TYPE_MST_TBL CT";
            Strsql += "  WHERE JA.JOB_CARD_TRN_PK=" + JobPk;
            Strsql += "  AND ICA.JOB_CARD_AIR_EXP_FK=JA.JOB_CARD_TRN_PK";
            Strsql += "  AND ICA.INV_CUST_AIR_EXP_PK=ICT.INV_CUST_AIR_EXP_FK";
            Strsql += "  AND ICA.JOB_CARD_AIR_EXP_FK=JA.JOB_CARD_TRN_PK";
            Strsql += "  AND ICA.INVOICE_REF_NO='" + InvRefNo + "'";
            Strsql += "  AND CT.CURRENCY_MST_PK=ICA.CURRENCY_MST_FK";

            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Invoice To Customer -- Exports AIR --  Reports Section"

        #region "Invoice To Cusomer -- Imports -- Reports Section"

        /// <summary>
        /// Fetches the inv to customer imp main.
        /// </summary>
        /// <param name="InvPk">The inv pk.</param>
        /// <returns></returns>
        public DataSet FetchInvToCustImpMain(Int32 InvPk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();

            Strsql = " SELECT INVCUSTEXP.CONSOL_INVOICE_PK INVPK,";
            Strsql += "INVCUSTEXP.INVOICE_REF_NO INVREFNO,";
            Strsql += "NVL(INVCUSTEXP.DISCOUNT_AMT, 0) DICSOUNT,";
            Strsql += "INVTAGTEXP.TAX_PCNT VATPCT,";
            Strsql += "INVTAGTEXP.TAX_AMT VATAMT,";
            Strsql += "JAI.JOB_CARD_TRN_PK JOBPK,";
            Strsql += "JAI.JOBCARD_REF_NO JOBREFNO,";
            Strsql += "JAI.CLEARANCE_ADDRESS CLEARANCEPOINT,";
            Strsql += "JAI.ETD_DATE ETD,";
            Strsql += "JAI.ETA_DATE ETA,";
            Strsql += "1 CARGOTYPE,";
            Strsql += "SHIPMST.CUSTOMER_NAME SHIPPER,";
            Strsql += "''     SHIPPERREFNO,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_1 SHIPPERADD1,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
            Strsql += "SHIPDTLS.ADM_CITY SHIPPERCITY,";
            Strsql += "SHIPDTLS.ADM_ZIP_CODE SHIPPERZIP,";
            Strsql += "SHIPDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            Strsql += "SHIPDTLS.ADM_FAX_NO SHIPPERFAX,";
            Strsql += "SHIPDTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,";
            Strsql += "SHIPMST.VAT_NO VATNO,";
            Strsql += "SHIPMST.CREDIT_DAYS PAYMENTDAYS,";
            Strsql += "CONSMST.CUSTOMER_NAME CONSIGNEE,";
            Strsql += "CONSDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,";
            Strsql += "CONSDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,";
            Strsql += "CONSDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,";
            Strsql += "CONSDTLS.ADM_CITY CONSIGNEECITY,";
            Strsql += "CONSDTLS.ADM_ZIP_CODE CONSIGNEEZIP,";
            Strsql += "CONSDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,";
            Strsql += "CONSDTLS.ADM_FAX_NO CONSIGNEEFAX,";
            Strsql += "CONSDTLS.ADM_EMAIL_ID CONSIGNEEMAIL,";
            Strsql += "CONSCOUNTRY.COUNTRY_NAME CONSIGNEEOUNTRY,";
            Strsql += "FEMST.FREIGHT_ELEMENT_NAME FREIGHTNAME,";
            Strsql += "nvl(INVTAGTEXP.AMT_IN_INV_CURR,0) FREIGHTAMT,";
            Strsql += "INVTAGTEXP.TAX_PCNT FRETAXPCNT,";
            Strsql += "INVTAGTEXP.TAX_AMT FRETAXANT,";
            Strsql += "CURRMST.CURRENCY_ID CURRID,";
            Strsql += "CURRMST.CURRENCY_NAME CURRNAME,";
            Strsql += "JAI.VOYAGE_FLIGHT_NO VES_FLIGHT,";
            Strsql += "JAI.PYMT_TYPE PYMT,";
            Strsql += "JAI.GOODS_DESCRIPTION GOODS,";
            Strsql += "JAI.MARKS_NUMBERS MARKS,";
            Strsql += "NVL(JAI.INSURANCE_AMT, 0) INSURANCE,";
            Strsql += "STMST.INCO_CODE TERMS,";

            Strsql += "DELMST.PLACE_NAME DELPLACE,";
            Strsql += "POLMST.PORT_NAME POL,";
            Strsql += "PODMST.PORT_NAME POD,";
            Strsql += "JAI.MAWB_REF_NO MBLREFNO, ";
            Strsql += "JAI.HAWB_REF_NO HBLREFNO ,";
            Strsql += " CGMST.COMMODITY_GROUP_DESC COMMODITY,";
            Strsql += "SUM(JAIC.VOLUME_IN_CBM) VOLUME,";
            Strsql += "SUM(JAIC.GROSS_WEIGHT) GROSS,";
            Strsql += " '' NETWT,";
            Strsql += "SUM(JAIC.CHARGEABLE_WEIGHT) CHARWT ,";
            Strsql += "INVCUSTEXP.INVOICE_DATE ";
            Strsql += "FROM CONSOL_INVOICE_TBL     INVCUSTEXP,";
            Strsql += "CURRENCY_TYPE_MST_TBL    CURRMST,";
            Strsql += "CONSOL_INVOICE_TRN_TBL INVTAGTEXP,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL  FEMST,";
            Strsql += "JOB_CARD_TRN     JAI,";
            Strsql += "JOB_TRN_CONT     JAIC,";
            Strsql += "SHIPPING_TERMS_MST_TBL   STMST,";

            Strsql += "PLACE_MST_TBL            DELMST,";
            Strsql += "PORT_MST_TBL             POLMST,";
            Strsql += "PORT_MST_TBL             PODMST,";

            Strsql += "COMMODITY_GROUP_MST_TBL  CGMST,";
            Strsql += "CUSTOMER_MST_TBL         SHIPMST,";
            Strsql += "CUSTOMER_CONTACT_DTLS    SHIPDTLS,";
            Strsql += "COUNTRY_MST_TBL          SHIPCOUNTRY,";
            Strsql += "CUSTOMER_MST_TBL         CONSMST,";
            Strsql += "CUSTOMER_CONTACT_DTLS    CONSDTLS,";
            Strsql += "COUNTRY_MST_TBL CONSCOUNTRY";
            Strsql += "WHERE(INVTAGTEXP.JOB_CARD_FK = JAI.JOB_CARD_TRN_PK)";
            Strsql += "AND CURRMST.CURRENCY_MST_PK(+) = INVCUSTEXP.CURRENCY_MST_FK";
            Strsql += "AND INVTAGTEXP.CONSOL_INVOICE_FK(+) = INVCUSTEXP.CONSOL_INVOICE_PK";
            Strsql += "AND FEMST.FREIGHT_ELEMENT_MST_PK(+) = INVTAGTEXP.FRT_OTH_ELEMENT_FK";
            Strsql += "AND JAI.JOB_CARD_TRN_PK = JAIC.JOB_CARD_TRN_FK(+)";
            Strsql += "AND STMST.SHIPPING_TERMS_MST_PK(+) = JAI.SHIPPING_TERMS_MST_FK";

            Strsql += "AND DELMST.PLACE_PK(+) = JAI.DEL_PLACE_MST_FK";
            Strsql += "AND POLMST.PORT_MST_PK = JAI.PORT_MST_POL_FK";
            Strsql += "AND PODMST.PORT_MST_PK = JAI.PORT_MST_POD_FK";

            Strsql += "AND CGMST.COMMODITY_GROUP_PK(+) = JAI.COMMODITY_GROUP_FK";
            Strsql += "AND CONSMST.CUSTOMER_MST_PK(+) = JAI.SHIPPER_CUST_MST_FK";
            Strsql += "AND CONSDTLS.CUSTOMER_MST_FK(+) = CONSMST.CUSTOMER_MST_PK";
            Strsql += "AND CONSDTLS.ADM_COUNTRY_MST_FK = CONSCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "AND SHIPMST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK";
            Strsql += "AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "AND INVTAGTEXP.CONSOL_INVOICE_TRN_PK = " + InvPk;
            Strsql += "GROUP BY INVCUSTEXP.CONSOL_INVOICE_PK,";
            Strsql += "INVCUSTEXP.INVOICE_REF_NO,";
            Strsql += "INVCUSTEXP.DISCOUNT_AMT,";
            Strsql += "INVTAGTEXP.TAX_PCNT,";
            Strsql += "INVTAGTEXP.TAX_AMT,";
            Strsql += "JAI.JOB_CARD_TRN_PK,";
            Strsql += "JAI.JOBCARD_REF_NO,";
            Strsql += "JAI.CLEARANCE_ADDRESS,";
            Strsql += "JAI.ETD_DATE,";
            Strsql += "JAI.ETA_DATE,";
            Strsql += "SHIPMST.CUSTOMER_NAME,";

            Strsql += "SHIPDTLS.ADM_ADDRESS_1,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_2,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_3,";
            Strsql += "SHIPDTLS.ADM_CITY,";
            Strsql += "SHIPDTLS.ADM_ZIP_CODE,";
            Strsql += "SHIPDTLS.ADM_PHONE_NO_1,";
            Strsql += "SHIPDTLS.ADM_FAX_NO,";
            Strsql += "SHIPDTLS.ADM_EMAIL_ID,";
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME,";
            Strsql += "SHIPMST.VAT_NO,";
            Strsql += "SHIPMST.CREDIT_DAYS,";
            Strsql += "CONSMST.CUSTOMER_NAME,";
            Strsql += "CONSDTLS.ADM_ADDRESS_1,";
            Strsql += "CONSDTLS.ADM_ADDRESS_2,";
            Strsql += "CONSDTLS.ADM_ADDRESS_3,";
            Strsql += "CONSDTLS.ADM_CITY,";
            Strsql += "CONSDTLS.ADM_ZIP_CODE,";
            Strsql += "CONSDTLS.ADM_PHONE_NO_1,";
            Strsql += "CONSDTLS.ADM_FAX_NO,";
            Strsql += "CONSDTLS.ADM_EMAIL_ID,";
            Strsql += "CONSCOUNTRY.COUNTRY_NAME,";
            Strsql += "FEMST.FREIGHT_ELEMENT_NAME,";
            Strsql += "INVTAGTEXP.AMT_IN_INV_CURR,";
            Strsql += "INVTAGTEXP.TAX_PCNT,";
            Strsql += "INVTAGTEXP.TAX_AMT,";
            Strsql += "CURRMST.CURRENCY_ID,";
            Strsql += "CURRMST.CURRENCY_NAME,";
            Strsql += "JAI.VOYAGE_FLIGHT_NO,";
            Strsql += "JAI.PYMT_TYPE,";
            Strsql += "JAI.GOODS_DESCRIPTION,";
            Strsql += "JAI.MARKS_NUMBERS,";
            Strsql += "JAI.INSURANCE_AMT,";
            Strsql += "STMST.INCO_CODE,";

            Strsql += "DELMST.PLACE_NAME,";
            Strsql += "POLMST.PORT_NAME,";
            Strsql += "PODMST.PORT_NAME,";
            Strsql += "JAI.MAWB_REF_NO,";
            Strsql += "JAI.HAWB_REF_NO,";
            Strsql += "INVCUSTEXP.INVOICE_DATE,";
            Strsql += "CGMST.COMMODITY_GROUP_DESC";

            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the inv to customer sea imp main.
        /// </summary>
        /// <param name="InvPk">The inv pk.</param>
        /// <returns></returns>
        public DataSet FetchInvToCustSeaImpMain(Int32 InvPk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();

            Strsql = " SELECT INVCUSTEXP.CONSOL_INVOICE_PK INVPK,";
            Strsql += "INVCUSTEXP.INVOICE_REF_NO       INVREFNO,";

            Strsql += "NVL(INVCUSTEXP.DISCOUNT_AMT,0)         DICSOUNT,";
            Strsql += "INVTAGTEXP.Tax_Pcnt             VATPCT,";
            Strsql += "INVTAGTEXP.Tax_Amt              VATAMT,";
            Strsql += "JSI.JOB_CARD_TRN_PK        JOBPK,";
            Strsql += "JSI.JOBCARD_REF_NO             JOBREFNO,";
            Strsql += "JSI.CLEARANCE_ADDRESS CLEARANCEPOINT,";
            Strsql += "JSI.ETD_DATE ETD,";
            Strsql += "JSI.ETA_DATE ETA,";
            Strsql += "2 CARGOTYPE, ";
            Strsql += "SHIPMST.CUSTOMER_NAME    SHIPPER,";
            Strsql += "''  SHIPPERREFNO,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_1   SHIPPERADD1,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_2   SHIPPERADD2,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_3   SHIPPERADD3,";
            Strsql += "SHIPDTLS.ADM_CITY        SHIPPERCITY,";
            Strsql += "SHIPDTLS.ADM_ZIP_CODE    SHIPPERZIP,";
            Strsql += "SHIPDTLS.ADM_PHONE_NO_1  SHIPPERPHONE,";
            Strsql += "SHIPDTLS.ADM_FAX_NO      SHIPPERFAX,";
            Strsql += "SHIPDTLS.ADM_EMAIL_ID    SHIPPEREMAIL,";
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,";
            Strsql += "SHIPMST.VAT_NO VATNO,";
            Strsql += "SHIPMST.CREDIT_DAYS PAYMENTDAYS,";

            Strsql += "CONSMST.CUSTOMER_NAME    CONSIGNEE,";
            Strsql += "CONSDTLS.ADM_ADDRESS_1   CONSIGNEEADD1,";
            Strsql += "CONSDTLS.ADM_ADDRESS_2   CONSIGNEEADD2,";
            Strsql += "CONSDTLS.ADM_ADDRESS_3   CONSIGNEEADD3,";
            Strsql += "CONSDTLS.ADM_CITY        CONSIGNEECITY,";
            Strsql += "CONSDTLS.ADM_ZIP_CODE    CONSIGNEEZIP,";
            Strsql += "CONSDTLS.ADM_PHONE_NO_1  CONSIGNEEPHONE,";
            Strsql += "CONSDTLS.ADM_FAX_NO      CONSIGNEEFAX,";
            Strsql += "CONSDTLS.ADM_EMAIL_ID    CONSIGNEEMAIL,";
            Strsql += "CONSCOUNTRY.COUNTRY_NAME CONSIGNEEOUNTRY,";

            Strsql += "FEMST.FREIGHT_ELEMENT_NAME FREIGHTNAME,";
            Strsql += "nvl(INVTAGTEXP.AMT_IN_INV_CURR ,0)    FREIGHTAMT,";
            Strsql += "INVTAGTEXP.TAX_PCNT        FRETAXPCNT,";
            Strsql += "INVTAGTEXP.TAX_AMT         FRETAXANT,";

            Strsql += "CURRMST.CURRENCY_ID CURRID,";
            Strsql += "CURRMST.CURRENCY_NAME CURRNAME,";
            Strsql += "(CASE WHEN JSI.VOYAGE IS NOT NULL THEN";
            Strsql += "JSI.VESSEL_NAME || '-' || JSI.VOYAGE";
            Strsql += "ELSE";
            Strsql += " JSI.VESSEL_NAME END) VES_FLIGHT,";
            Strsql += "JSI.PYMT_TYPE PYMT,";
            Strsql += "JSI.GOODS_DESCRIPTION GOODS,";
            Strsql += "JSI.MARKS_NUMBERS MARKS,";
            Strsql += "NVL(JSI.INSURANCE_AMT, 0) INSURANCE,";
            Strsql += "STMST.INCO_CODE TERMS,";

            Strsql += "DELMST.PLACE_NAME DELPLACE,";
            Strsql += "POLMST.PORT_NAME POL,";
            Strsql += "PODMST.PORT_NAME POD,";
            Strsql += "JSI.HBL_REF_NO HBLREFNO,";
            Strsql += "JSI.MBL_REF_NO MBLREFNO,";
            Strsql += "CGMST.COMMODITY_GROUP_DESC COMMODITY,";
            Strsql += " SUM(JSIC.VOLUME_IN_CBM) VOLUME,";
            Strsql += "SUM(JSIC.GROSS_WEIGHT) GROSS,";
            Strsql += "SUM(JSIC.NET_WEIGHT) NETWT,";
            Strsql += "SUM(JSIC.CHARGEABLE_WEIGHT) CHARWT ,";
            Strsql += "INVCUSTEXP.INVOICE_DATE ";
            Strsql += "FROM CONSOL_INVOICE_TBL INVCUSTEXP,";
            Strsql += "CURRENCY_TYPE_MST_TBL CURRMST,";

            Strsql += "CONSOL_INVOICE_TRN_TBL INVTAGTEXP,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL   FEMST,";

            Strsql += "JOB_CARD_TRN    JSI,";
            Strsql += "JOB_TRN_CONT    JSIC,";

            Strsql += "SHIPPING_TERMS_MST_TBL  STMST,";

            Strsql += "PLACE_MST_TBL           DELMST,";
            Strsql += "PORT_MST_TBL            POLMST,";
            Strsql += "PORT_MST_TBL            PODMST,";

            Strsql += "COMMODITY_GROUP_MST_TBL CGMST,";

            Strsql += "CUSTOMER_MST_TBL      SHIPMST,";
            Strsql += "CUSTOMER_CONTACT_DTLS SHIPDTLS,";
            Strsql += "COUNTRY_MST_TBL       SHIPCOUNTRY,";

            Strsql += "CUSTOMER_MST_TBL      CONSMST,";
            Strsql += "CUSTOMER_CONTACT_DTLS CONSDTLS,";
            Strsql += "COUNTRY_MST_TBL CONSCOUNTRY";

            Strsql += "WHERE(INVTAGTEXP.JOB_CARD_FK = JSI.JOB_CARD_TRN_PK)";
            Strsql += "AND CURRMST.CURRENCY_MST_PK(+) = INVCUSTEXP.CURRENCY_MST_FK";

            Strsql += "AND INVTAGTEXP.CONSOL_INVOICE_FK = INVCUSTEXP.CONSOL_INVOICE_PK";
            Strsql += "AND FEMST.FREIGHT_ELEMENT_MST_PK(+) = INVTAGTEXP.FRT_OTH_ELEMENT_FK";

            Strsql += "AND JSI.JOB_CARD_TRN_PK = JSIC.JOB_CARD_TRN_FK(+)";

            Strsql += "AND STMST.SHIPPING_TERMS_MST_PK(+) = JSI.SHIPPING_TERMS_MST_FK";

            Strsql += "AND DELMST.PLACE_PK(+) = JSI.DEL_PLACE_MST_FK";
            Strsql += "AND POLMST.PORT_MST_PK = JSI.PORT_MST_POL_FK";
            Strsql += "AND PODMST.PORT_MST_PK = JSI.PORT_MST_POD_FK";

            Strsql += "AND CGMST.COMMODITY_GROUP_PK(+) = JSI.COMMODITY_GROUP_FK";

            Strsql += "AND CONSMST.CUSTOMER_MST_PK(+) = JSI.SHIPPER_CUST_MST_FK";
            Strsql += "AND CONSDTLS.CUSTOMER_MST_FK(+) = CONSMST.CUSTOMER_MST_PK";
            Strsql += "AND CONSDTLS.ADM_COUNTRY_MST_FK = CONSCOUNTRY.COUNTRY_MST_PK(+)";

            Strsql += "AND SHIPMST.CUSTOMER_MST_PK(+) = JSI.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK";
            Strsql += "AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "AND INVTAGTEXP.CONSOL_INVOICE_TRN_PK=" + InvPk;
            Strsql += "GROUP BY INVCUSTEXP.CONSOL_INVOICE_PK,";
            Strsql += "INVCUSTEXP.INVOICE_REF_NO,";

            Strsql += "INVCUSTEXP.DISCOUNT_AMT,";
            Strsql += " INVTAGTEXP.Tax_Pcnt,";
            Strsql += "INVTAGTEXP.Tax_Amt,";
            Strsql += "JSI.JOB_CARD_TRN_PK,";
            Strsql += "JSI.JOBCARD_REF_NO,";
            Strsql += "JSI.CLEARANCE_ADDRESS,";
            Strsql += " JSI.ETD_DATE,";
            Strsql += " JSI.ETA_DATE,";
            Strsql += "SHIPMST.CUSTOMER_NAME,";

            Strsql += "SHIPDTLS.ADM_ADDRESS_1,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_2,";
            Strsql += "SHIPDTLS.ADM_ADDRESS_3,";
            Strsql += "SHIPDTLS.ADM_CITY,";
            Strsql += "SHIPDTLS.ADM_ZIP_CODE,";
            Strsql += "SHIPDTLS.ADM_PHONE_NO_1,";
            Strsql += "SHIPDTLS.ADM_FAX_NO,";
            Strsql += "SHIPDTLS.ADM_EMAIL_ID,";
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME,";
            Strsql += "SHIPMST.VAT_NO ,";
            Strsql += "SHIPMST.CREDIT_DAYS,";

            Strsql += "CONSMST.CUSTOMER_NAME    ,";
            Strsql += "CONSDTLS.ADM_ADDRESS_1  ,";
            Strsql += "CONSDTLS.ADM_ADDRESS_2 ,";
            Strsql += "CONSDTLS.ADM_ADDRESS_3 ,";
            Strsql += "CONSDTLS.ADM_CITY   ,";
            Strsql += "CONSDTLS.ADM_ZIP_CODE ,";
            Strsql += "CONSDTLS.ADM_PHONE_NO_1 ,";
            Strsql += "CONSDTLS.ADM_FAX_NO  ,";
            Strsql += "CONSDTLS.ADM_EMAIL_ID  ,";
            Strsql += "CONSCOUNTRY.COUNTRY_NAME ,";

            Strsql += "FEMST.FREIGHT_ELEMENT_NAME,";
            Strsql += "INVTAGTEXP.AMT_IN_INV_CURR,";
            Strsql += "INVTAGTEXP.TAX_PCNT,";
            Strsql += "INVTAGTEXP.TAX_AMT,";
            Strsql += "CURRMST.CURRENCY_ID,";
            Strsql += "CURRMST.CURRENCY_NAME,";
            Strsql += "(CASE WHEN JSI.VOYAGE IS NOT NULL THEN";
            Strsql += "JSI.VESSEL_NAME || '-' || JSI.VOYAGE_FLIGHT_NO";
            Strsql += "ELSE";
            Strsql += "JSI.VESSEL_NAME END),";
            Strsql += "JSI.PYMT_TYPE,";
            Strsql += "JSI.GOODS_DESCRIPTION,";
            Strsql += "JSI.MARKS_NUMBERS,";
            Strsql += "JSI.INSURANCE_AMT,";
            Strsql += "STMST.INCO_CODE,";

            Strsql += "DELMST.PLACE_NAME,";
            Strsql += " POLMST.PORT_NAME,";
            Strsql += "PODMST.PORT_NAME,";
            Strsql += "JSI.HBL_REF_NO,";
            Strsql += "JSI.MBL_REF_NO,";
            Strsql += "INVCUSTEXP.INVOICE_DATE,";
            Strsql += "CGMST.COMMODITY_GROUP_DESC";

            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the air imp container details.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public DataSet FetchAirImpContainerDetails(Int64 nInvPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT ICSE.CONSOL_INVOICE_TRN_PK ,JTEC.PALETTE_SIZE CONTAINER";
            strSQL += "FROM CONSOL_INVOICE_TRN_TBL ICSE,";
            strSQL += "JOB_CARD_TRN  JSE,";
            strSQL += "JOB_TRN_CONT JTEC";
            strSQL += "WHERE(ICSE.JOB_CARD_FK = JSE.JOB_CARD_TRN_PK)";
            strSQL += "AND JTEC.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK";
            strSQL += "AND ICSE.CONSOL_INVOICE_TRN_PK = " + nInvPK;
            try
            {
                return (objWK.GetDataSet(strSQL));
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the sea imp container details.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public DataSet FetchSeaImpContainerDetails(Int64 nInvPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT ICSE.CONSOL_INVOICE_TRN_PK ,JTEC.CONTAINER_NUMBER CONTAINER";
            strSQL += "FROM CONSOL_INVOICE_TRN_TBL ICSE,";
            strSQL += "JOB_CARD_TRN  JSE,";
            strSQL += "JOB_TRN_CONT JTEC";
            strSQL += "WHERE(ICSE.JOB_CARD_FK = JSE.JOB_CARD_TRN_PK)";
            strSQL += "AND JTEC.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK";
            strSQL += "AND ICSE.CONSOL_INVOICE_TRN_PK = " + nInvPK;
            try
            {
                return (objWK.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the inv to cus imp details.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="Sea">if set to <c>true</c> [sea].</param>
        /// <returns></returns>
        public DataSet FetchInvToCusImpDetails(Int32 JobPk, bool Sea)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            if (Sea == true)
            {
                Strsql = "  SELECT J.JOB_CARD_TRN_PK AS JOBPK,J.JOBCARD_REF_NO AS JOBREFNO,J.HBL_REF_NO AS HREFNO,J.MARKS_NUMBERS,J.GOODS_DESCRIPTION,";
                Strsql += "  SUM(JT.VOLUME_IN_CBM) AS CUBEM3,SUM(JT.GROSS_WEIGHT) AS GROSS, CASE WHEN (SELECT J1.CARGO_TYPE FROM JOB_CARD_TRN J1 WHERE J1.JOB_CARD_TRN_PK = " + JobPk + ")=1 THEN SUM(JT.NET_WEIGHT) ELSE SUM(JT.CHARGEABLE_WEIGHT) END AS NET";
                Strsql += " FROM JOB_CARD_TRN J,JOB_TRN_CONT JT";
                Strsql += "  WHERE J.JOB_CARD_TRN_PK=" + JobPk;
                Strsql += "  AND J.JOB_CARD_TRN_PK=JT.JOB_CARD_TRN_FK";
                Strsql += "  GROUP BY J.JOB_CARD_TRN_PK,J.JOBCARD_REF_NO,J.HBL_REF_NO,J.MARKS_NUMBERS,J.GOODS_DESCRIPTION";
            }
            else
            {
                Strsql = "  SELECT J.JOB_CARD_TRN_PK AS JOBPK,J.JOBCARD_REF_NO AS JOBREFNO,J.HAWB_REF_NO AS HREFNO,J.MARKS_NUMBERS,J.GOODS_DESCRIPTION  ";
                Strsql += "  ,SUM(JT.VOLUME_IN_CBM) AS CUBEM3,SUM(JT.GROSS_WEIGHT) AS GROSS,SUM(JT.CHARGEABLE_WEIGHT) AS NET";
                Strsql += "  FROM JOB_CARD_TRN J,JOB_TRN_CONT JT";
                Strsql += "  WHERE J.JOB_CARD_TRN_PK=" + JobPk;
                Strsql += "  AND JT.JOB_CARD_TRN_FK=J.JOB_CARD_TRN_PK";
                Strsql += "  GROUP BY J.JOB_CARD_TRN_PK,J.JOBCARD_REF_NO,J.HAWB_REF_NO,J.MARKS_NUMBERS,J.GOODS_DESCRIPTION";
            }

            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the inv to cus imp desc.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="InvRefNo">The inv reference no.</param>
        /// <param name="Sea">if set to <c>true</c> [sea].</param>
        /// <returns></returns>
        public DataSet FetchInvToCusImpDesc(Int32 JobPk, string InvRefNo, bool Sea)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            if (Sea == true)
            {
                Strsql = "  SELECT JS.JOB_CARD_TRN_PK AS JOBPK,JS.JOBCARD_REF_NO AS JOBREFNO,ICT.COST_FRT_ELEMENT,ICT.COST_FRT_ELEMENT_FK,";
                Strsql += "  DECODE(ICT.COST_FRT_ELEMENT,1,";
                Strsql += "  (SELECT CE.COST_ELEMENT_NAME FROM COST_ELEMENT_MST_TBL CE  ";
                Strsql += "  WHERE CE.COST_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK) ,2,";
                Strsql += "  (SELECT FE.FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL FE WHERE";
                Strsql += "   FE.FREIGHT_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK),3,";
                Strsql += " (SELECT FE.FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL FE WHERE";
                Strsql += "   FE.FREIGHT_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK)) AS DESCRIPTION,";
                Strsql += "  ICT.AMT_IN_INV_CURR AS CHARGES,ICT.TAX_AMT AS TAX,";
                Strsql += "   '' AS CODE, '' AS TAXCODE,ICT.TOT_AMT AS TOTCHARGE,NVL(ICS.VAT_AMT,0) AS VAT_AMT ,ICS.INVOICE_REF_NO,CT.CURRENCY_ID,CT.CURRENCY_NAME";
                Strsql += "    ,ICS.VAT_PCNT,ICS.VAT_AMT,NVL(ICS.DISCOUNT_AMT,0) AS DISCOUNT_AMT ,(ICT.TOT_AMT+ICS.VAT_AMT-ICS.DISCOUNT_AMT) AS TOTAMOUNTDUE";
                Strsql += "  FROM JOB_CARD_TRN JS,INV_CUST_SEA_IMP_TBL ICS,INV_CUST_TRN_SEA_IMP_TBL ICT,CURRENCY_TYPE_MST_TBL CT";
                Strsql += "  WHERE JS.JOB_CARD_TRN_PK=" + JobPk;
                Strsql += "  AND ICS.JOB_CARD_TRN_FK=JS.JOB_CARD_TRN_PK";
                Strsql += "  AND ICS.INV_CUST_SEA_IMP_PK=ICT.INV_CUST_SEA_IMP_FK";
                Strsql += " AND ICS.JOB_CARD_TRN_FK=JS.JOB_CARD_TRN_PK";
                Strsql += "  AND ICS.INVOICE_REF_NO='" + InvRefNo + "'";
                Strsql += " AND CT.CURRENCY_MST_PK=ICS.CURRENCY_MST_FK";
            }
            else
            {
                Strsql = "  SELECT JA.JOB_CARD_TRN_PK AS JOBPK,JA.JOBCARD_REF_NO AS JOBREFNO,ICT.COST_FRT_ELEMENT,ICT.COST_FRT_ELEMENT_FK,";
                Strsql += "  DECODE(ICT.COST_FRT_ELEMENT,1,";
                Strsql += "  (SELECT CE.COST_ELEMENT_NAME FROM COST_ELEMENT_MST_TBL CE  ";
                Strsql += "  WHERE CE.COST_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK) ,2,";
                Strsql += "  (SELECT FE.FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL FE WHERE";
                Strsql += "   FE.FREIGHT_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK),3,";
                Strsql += " (SELECT FE.FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL FE WHERE";
                Strsql += "   FE.FREIGHT_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK)) AS DESCRIPTION,";
                Strsql += " ICT.AMT_IN_INV_CURR AS CHARGES,ICT.TAX_AMT AS TAX,";
                Strsql += "   '' AS CODE, '' AS TAXCODE,ICT.TOT_AMT AS TOTCHARGE,NVL(ICA.VAT_AMT,0) AS VAT_AMT ,ICA.INVOICE_REF_NO,CT.CURRENCY_ID,CT.CURRENCY_NAME";
                Strsql += "     ,ICA.VAT_PCNT, ICA.VAT_AMT ,NVL(ICA.DISCOUNT_AMT,0) AS DISCOUNT_AMT ,(ICT.TOT_AMT+ICA.VAT_AMT-ICA.DISCOUNT_AMT) AS TOTAMOUNTDUE";
                Strsql += "  FROM JOB_CARD_TRN JA,INV_CUST_AIR_IMP_TBL ICA,INV_CUST_TRN_AIR_IMP_TBL ICT,CURRENCY_TYPE_MST_TBL CT";
                Strsql += "  WHERE JA.JOB_CARD_TRN_PK=" + JobPk;
                Strsql += "  AND ICA.JOB_CARD_TRN_FK=JA.JOB_CARD_TRN_PK";
                Strsql += " AND ICA.INV_CUST_AIR_IMP_PK=ICT.INV_CUST_AIR_IMP_FK";
                Strsql += " AND ICA.JOB_CARD_TRN_FK=JA.JOB_CARD_TRN_PK";
                Strsql += "  AND ICA.INVOICE_REF_NO='" + InvRefNo + "'";
                Strsql += " AND CT.CURRENCY_MST_PK=ICA.CURRENCY_MST_FK";
            }

            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Invoice To Cusomer -- Imports -- Reports Section"
    }
}