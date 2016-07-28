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
using Oracle.ManagedDataAccess.Types;
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
    public class cls_JobCardView : CommonFeatures
    {
        /// <summary>
        /// The _ pk value trans
        /// </summary>
        private long _PkValueTrans;

        /// <summary>
        /// The object track n trace
        /// </summary>
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        /// <summary>
        /// The object vessel voyage
        /// </summary>
        private cls_SeaBookingEntry objVesselVoyage = new cls_SeaBookingEntry();

        #region "GetESi Status"

        /// <summary>
        /// Esi_s the fetch.
        /// </summary>
        /// <param name="StrcurrentBookingID">The strcurrent booking identifier.</param>
        /// <param name="StrcurrentJobCardID">The strcurrent job card identifier.</param>
        /// <returns></returns>
        public DataSet Esi_Fetch(int StrcurrentBookingID = 0, int StrcurrentJobCardID = 0)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder();

            sb.Append(" SELECT ESI.*");
            sb.Append("  FROM JOB_CARD_TRN JC,");
            sb.Append("       SYN_EBKG_ESI_HDR_TBL ESI,");
            sb.Append("       BOOKING_MST_TBL      BST");
            sb.Append("");
            sb.Append("  WHERE BST.BOOKING_SEA_PK = ESI.BOOKING_SEA_FK");
            sb.Append("   AND BST.BOOKING_SEA_PK = JC.BOOKING_SEA_FK");

            sb.Append("");
            if (StrcurrentBookingID != 0)
            {
                sb.Append("   AND BST.BOOKING_SEA_PK = " + StrcurrentBookingID);
            }
            else if (StrcurrentJobCardID != 0)
            {
                sb.Append("  AND JC.JOB_CARD_TRN_PK = " + StrcurrentJobCardID);
            }

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

        #endregion "GetESi Status"

        #region "Fetch Location"

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <param name="Loc">The loc.</param>
        /// <returns></returns>
        public DataSet FetchLocation(long Loc)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet LocDS = null;
            objWF.MyCommand.Parameters.Clear();
            var _with1 = objWF.MyCommand.Parameters;
            _with1.Add("LOC_MST_PK_IN", Loc).Direction = ParameterDirection.Input;
            _with1.Add("LOCATION_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            LocDS = objWF.GetDataSet("PKG_COMMON_TASKS", "GET_LOC_ADDRESS");
            try
            {
                return LocDS;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Location"

        #region "Fetch Main ESI"

        /// <summary>
        /// Fetches the main esi.
        /// </summary>
        /// <param name="bookingpk">The bookingpk.</param>
        /// <returns></returns>
        public DataSet FetchMainESI(int bookingpk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            //sb.Append("SELECT DISTINCT BOOK.BOOKING_SEA_PK,")
            //sb.Append("                EST.ESI_HDR_PK,")
            //sb.Append("                JC.JOB_CARD_TRN_PK,")
            //sb.Append("                BOOK.BOOKING_REF_NO,")
            //sb.Append("                BOOK.BOOKING_DATE,")
            //sb.Append("                BOOK.STATUS,")
            //sb.Append("                JC.UCR_NO CUSTOMERREFERENE,")
            //sb.Append("                JC.INSURANCE_AMT,")
            //sb.Append("                JC.INSURANCE_CURRENCY,")
            //sb.Append("                EST.ESI_REF_NR,")
            //sb.Append("                CMT.CUSTOMER_NAME SHIPPER,")
            //sb.Append("                CCD.ADM_ADDRESS_1 || '' || CCD.ADM_ADDRESS_2 || '' ||")
            //sb.Append("                CCD.ADM_ADDRESS_3 SHIPPER_ADRESS,")
            //sb.Append("                CMT.VAT_NO,")
            //sb.Append("                CMT1.CUSTOMER_NAME CONSIGNNAME,")
            //sb.Append("                CC.ADM_ADDRESS_1 || '' || CC.ADM_ADDRESS_2 || '' ||")
            //sb.Append("                CC.ADM_ADDRESS_3 CONSIGN_ADRESS,")
            //sb.Append("                VES.VESSEL_NAME ""vessel_name"",")
            //sb.Append("                VES.VESSEL_ID || '/' || VOY.VOYAGE VSL_VOY,")
            //sb.Append("                POL.PORT_ID POL,")
            //sb.Append("                POD.PORT_ID POD,")
            //sb.Append("                COL_PLACE.LOCATION_MST_ID PORID,")
            //sb.Append("                DEL_PLACE.LOCATION_MST_ID PFDID,")
            //sb.Append("                SUM(FD.FREIGHT_AMT) FRTAMT,")
            //sb.Append("                DECODE(BOOK.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENTTYPE,")
            //sb.Append("                (CASE")
            //sb.Append("                  WHEN BOOK.PYMT_TYPE = 1 THEN")
            //sb.Append("                   'A'")
            //sb.Append("                  ELSE")
            //sb.Append("                   'B'")
            //sb.Append("                END) TYPE")
            //sb.Append("  FROM SYN_QCOR_M_SEA_BOOKING      BOOK,")
            //sb.Append("       EBKG_ESI_HDR_TBL            EST,")
            //sb.Append("       CUSTOMER_CONTACT_DTLS       CCD,")
            //sb.Append("       CUSTOMER_CONTACT_DTLS       CC,")
            //sb.Append("       CUSTOMER_MST_TBL            CMT,")
            //sb.Append("       CUSTOMER_MST_TBL            CMT1,")
            //sb.Append("       QCOR_M_JOB_CARD_SEA_EXP_TBL JC,")
            //sb.Append("       QCOR_MST_M_VESSEL           VES,")
            //sb.Append("       QCOR_MST_M_VSL_VOY          VOY,")
            //sb.Append("       QCOR_MST_M_PORT             POL,")
            //sb.Append("       QCOR_MST_M_PORT             POD,")
            //sb.Append("       QCOR_MST_M_LOCATION         DEL_PLACE,")
            //sb.Append("       QCOR_MST_M_LOCATION         COL_PLACE,")
            //sb.Append("       QCOR_T_JOB_SEA_EXP_FD       FD")
            //sb.Append(" WHERE BOOK.BOOKING_SEA_PK = EST.BOOKING_SEA_FK")
            //sb.Append("   AND JC.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)")
            //sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CMT1.CUSTOMER_MST_PK(+)")
            //sb.Append("   AND BOOK.CUST_CUSTOMER_MST_FK = CCD.CUSTOMER_MST_FK(+)")
            //sb.Append("   AND BOOK.CONS_CUSTOMER_MST_FK = CC.CUSTOMER_MST_FK(+)")
            //sb.Append("   AND JC.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)")
            //sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CMT1.CUSTOMER_MST_PK(+)")
            //sb.Append("   AND JC.BOOKING_SEA_FK(+) = BOOK.BOOKING_SEA_PK")
            //sb.Append("   AND JC.VOYAGE_TRN_FK = VOY.VSL_VOYAGE_PK")
            //sb.Append("   AND VES.VESSEL_MST_PK(+) = VOY.VESSEL_MST_FK")
            //sb.Append("   AND POL.PORT_MST_PK(+) = BOOK.PORT_MST_POL_FK")
            //sb.Append("   AND POD.PORT_MST_PK(+) = BOOK.PORT_MST_POD_FK")
            //sb.Append("   AND BOOK.DEL_PLACE_MST_FK = DEL_PLACE.LOCATION_MST_PK(+)")
            //sb.Append("   AND BOOK.COL_PLACE_MST_FK = COL_PLACE.LOCATION_MST_PK(+)")
            //sb.Append("   AND FD.JOB_CARD_SEA_EXP_FK = JC.JOB_CARD_TRN_PK")
            //sb.Append("   AND BOOK.BOOKING_SEA_PK = " & bookingpk & "")
            //sb.Append(" GROUP BY BOOKING_SEA_PK,")
            //sb.Append("          ESI_HDR_PK,")
            //sb.Append("          JOB_CARD_TRN_PK,")
            //sb.Append("          BOOKING_REF_NO,")
            //sb.Append("          BOOKING_DATE,")
            //sb.Append("          STATUS,")
            //sb.Append("          UCR_NO,")
            //sb.Append("          INSURANCE_AMT,")
            //sb.Append("          INSURANCE_CURRENCY,")
            //sb.Append("          ESI_REF_NR,")
            //sb.Append("          CMT.CUSTOMER_NAME,")
            //sb.Append("          BOOK.PYMT_TYPE,")
            //sb.Append("          CCD.ADM_ADDRESS_1,")
            //sb.Append("          CCD.ADM_ADDRESS_2,")
            //sb.Append("          CCD.ADM_ADDRESS_3,")
            //sb.Append("          VES.VESSEL_NAME,")
            //sb.Append("          CMT.VAT_NO,")
            //sb.Append("          CMT1.CUSTOMER_NAME,")
            //sb.Append("          CC.ADM_ADDRESS_1,")
            //sb.Append("          CC.ADM_ADDRESS_2,")
            //sb.Append("          CC.ADM_ADDRESS_3,")
            //sb.Append("          VES.VESSEL_NAME,")
            //sb.Append("          VES.VESSEL_ID,")
            //sb.Append("          VOY.VOYAGE,")
            //sb.Append("          POL.PORT_ID,")
            //sb.Append("          POD.PORT_ID,")
            //sb.Append("          COL_PLACE.LOCATION_MST_ID,")
            //sb.Append("          DEL_PLACE.LOCATION_MST_ID")
            sb.Append(" SELECT DISTINCT BOOK.BOOKING_SEA_PK,");
            sb.Append("                EST.ESI_HDR_PK,");
            sb.Append("                JC.JOB_CARD_TRN_PK,");
            sb.Append("                BOOK.BOOKING_REF_NO,");
            sb.Append("                BOOK.BOOKING_DATE,");
            sb.Append("                BOOK.STATUS,");
            sb.Append("                JC.UCR_NO CUSTOMERREFERENE,");
            sb.Append("                JC.INSURANCE_AMT,");
            sb.Append("                JC.INSURANCE_CURRENCY,");
            sb.Append("                EST.ESI_REF_NR,");
            sb.Append("                CMT.CUSTOMER_NAME SHIPPER,");
            sb.Append("                CCD.ADM_ADDRESS_1 || '' || CCD.ADM_ADDRESS_2 || '' ||");
            sb.Append("                CCD.ADM_ADDRESS_3 SHIPPER_ADRESS,");
            sb.Append("                CMT.VAT_NO,");
            sb.Append("                CMT1.CUSTOMER_NAME CONSIGNNAME,");
            sb.Append("                CC.ADM_ADDRESS_1 || '' || CC.ADM_ADDRESS_2 || '' ||");
            sb.Append("                CC.ADM_ADDRESS_3 CONSIGN_ADRESS,");
            sb.Append("                VES.VESSEL_NAME \"vessel_name\",");
            sb.Append("                VES.VESSEL_ID || '/' || VOY.VOYAGE VSL_VOY,");
            sb.Append("                POL.PORT_ID POL,");
            sb.Append("                POD.PORT_ID POD,");
            sb.Append("                COL_PLACE.PLACE_CODE PORID,");
            sb.Append("                DEL_PLACE.PLACE_CODE PFDID,");
            sb.Append("                SUM(FD.FREIGHT_AMT) FRTAMT,");
            sb.Append("                DECODE(BOOK.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENTTYPE,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN BOOK.PYMT_TYPE = 1 THEN");
            sb.Append("                   'A'");
            sb.Append("                  ELSE");
            sb.Append("                   'B'");
            sb.Append("                END) TYPE");
            sb.Append("   FROM BOOKING_MST_TBL       BOOK,");
            sb.Append("       SYN_EBKG_ESI_HDR_TBL  EST,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CC,");
            sb.Append("       CUSTOMER_MST_TBL      CMT,");
            sb.Append("       CUSTOMER_MST_TBL      CMT1,");
            sb.Append("       ");
            sb.Append("       JOB_CARD_TRN JC,");
            sb.Append("       ");
            sb.Append("       VESSEL_VOYAGE_TBL VES,");
            sb.Append("       ");
            sb.Append("       VESSEL_VOYAGE_TRN VOY,");
            sb.Append("       ");
            sb.Append("       PORT_MST_TBL       POL,");
            sb.Append("       PORT_MST_TBL       POD,");
            sb.Append("       PLACE_MST_TBL      DEL_PLACE,");
            sb.Append("       PLACE_MST_TBL      COL_PLACE,");
            sb.Append("       JOB_TRN_FD FD");
            sb.Append("");
            sb.Append("  WHERE BOOK.BOOKING_SEA_PK = EST.BOOKING_SEA_FK");
            sb.Append("   AND JC.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CMT1.CUSTOMER_MST_PK(+)");
            sb.Append("   AND BOOK.CUST_CUSTOMER_MST_FK = CCD.CUSTOMER_MST_FK(+)");
            sb.Append("   AND BOOK.CONS_CUSTOMER_MST_FK = CC.CUSTOMER_MST_FK(+)");
            sb.Append("   AND JC.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CMT1.CUSTOMER_MST_PK(+)");
            sb.Append("   AND JC.BOOKING_SEA_FK(+) = BOOK.BOOKING_SEA_PK");
            sb.Append("   AND JC.VOYAGE_TRN_FK = VOY.VOYAGE_TRN_PK");
            sb.Append("   AND VES.VESSEL_VOYAGE_TBL_PK(+) = VOY.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND POL.PORT_MST_PK(+) = BOOK.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK(+) = BOOK.PORT_MST_POD_FK");
            sb.Append("   AND BOOK.DEL_PLACE_MST_FK = DEL_PLACE.PLACE_PK(+)");
            sb.Append("   AND BOOK.COL_PLACE_MST_FK = COL_PLACE.PLACE_PK(+)");
            sb.Append("   AND FD.JOB_CARD_SEA_EXP_FK = JC.JOB_CARD_TRN_PK");
            sb.Append("   AND BOOK.BOOKING_SEA_PK = " + bookingpk);
            sb.Append("  GROUP BY BOOKING_SEA_PK,");
            sb.Append("          ESI_HDR_PK,");
            sb.Append("          JOB_CARD_TRN_PK,");
            sb.Append("          BOOKING_REF_NO,");
            sb.Append("          BOOKING_DATE,");
            sb.Append("          STATUS,");
            sb.Append("          UCR_NO,");
            sb.Append("          INSURANCE_AMT,");
            sb.Append("          INSURANCE_CURRENCY,");
            sb.Append("          ESI_REF_NR,");
            sb.Append("          CMT.CUSTOMER_NAME,");
            sb.Append("          BOOK.PYMT_TYPE,");
            sb.Append("          CCD.ADM_ADDRESS_1,");
            sb.Append("          CCD.ADM_ADDRESS_2,");
            sb.Append("          CCD.ADM_ADDRESS_3,");
            sb.Append("          VES.VESSEL_NAME,");
            sb.Append("          CMT.VAT_NO,");
            sb.Append("          CMT1.CUSTOMER_NAME,");
            sb.Append("          CC.ADM_ADDRESS_1,");
            sb.Append("          CC.ADM_ADDRESS_2,");
            sb.Append("          CC.ADM_ADDRESS_3,");
            sb.Append("          VES.VESSEL_NAME,");
            sb.Append("          VES.VESSEL_ID,");
            sb.Append("          VOY.VOYAGE,");
            sb.Append("          POL.PORT_ID,");
            sb.Append("          POD.PORT_ID,");
            sb.Append("          COL_PLACE.PLACE_CODE,");
            sb.Append("          DEL_PLACE.PLACE_CODE");
            sb.Append("");

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Main ESI"

        #region "Fetch Detail ESI"

        /// <summary>
        /// Fetches the detail esi.
        /// </summary>
        /// <param name="ESIPK">The esipk.</param>
        /// <returns></returns>
        public object FetchDetailESI(int ESIPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            //sb.Append("SELECT DISTINCT QCOMM.COMMODITY_ID,")
            //sb.Append("                QCOMM.COMMODITY_NAME,")
            //sb.Append("                CNT.CNTR_DTL_TBL_PK,")
            //sb.Append("                CONT.CONTAINER_TYPE_MST_PK,")
            //sb.Append("                CONT.CONTAINER_TYPE_MST_ID,")
            //sb.Append("                CNT.CONTAINER_NR,")
            //sb.Append("                PCK.PACK_TYPE_MST_PK PACK_TYPE_PK,")
            //sb.Append("                PCK.PACK_TYPE_ID,")
            //sb.Append("                CNT.PACK_COUNT QUANTITY,")
            //sb.Append("                nvl(CNT.GROSS_WEIGHT, 0) GROSS_WEIGHT,")
            //sb.Append("                nvl(CNT.NET_WEIGHT, 0) NET_WEIGHT,")
            //sb.Append("                CNT.VOLUME_IN_CBM,")
            //sb.Append("                nvl(CNT.CHARGEABLE_WEIGHT, 0) CHARGEABLE_WEIGHT,")
            //sb.Append("                CNT.COMMODITY_MST_FK,")
            //sb.Append("                CNT.COMMODITY_MST_FKS,")
            //sb.Append("                CONT.CONTAINER_TAREWEIGHT_TONE,")
            //sb.Append("                NULL NO_OF_BOXES,")
            //sb.Append("                CNT.SEAL_NUMBER SEAL_NR,")
            //sb.Append("                CNT.GOODS_DESCRIPTION,")
            //sb.Append("                CNT.MARKS_NUMBERS,")
            //sb.Append("                CNT.REMARKS")
            //sb.Append("  FROM EBKG_ESI_HDR_TBL          ESI,")
            //sb.Append("       EBKG_ESI_CNTR_TBL         CNT,")
            //sb.Append("       EBKG_ESI_COMM_TBL         COMM,")
            //sb.Append("       QCOR_MST_M_CONTAINER_TYPE CONT,")
            //sb.Append("       QCOR_MST_M_PACK_TYPE      PCK,")
            //sb.Append("       QCOR_MST_M_COMMODITY      QCOMM")
            //sb.Append(" WHERE ESI.ESI_HDR_PK = CNT.ESI_HDR_FK")
            //sb.Append("   AND ESI.ESI_HDR_PK = COMM.ESI_HDR_FK(+)")
            //sb.Append("   AND CNT.CONTAINER_TYPE_FK = CONT.CONTAINER_TYPE_MST_PK")
            //sb.Append("   AND CNT.PACK_TYPE_MST_FK = PCK.PACK_TYPE_MST_PK")
            //sb.Append("   AND COMM.COMMODITY_FK = QCOMM.COMMODITY_MST_PK(+)")
            sb.Append(" SELECT DISTINCT QCOMM.COMMODITY_ID,");
            sb.Append("                QCOMM.COMMODITY_NAME,");
            sb.Append("                CNT.CNTR_DTL_TBL_PK,");
            sb.Append("                CONT.CONTAINER_TYPE_MST_PK,");
            sb.Append("                CONT.CONTAINER_TYPE_MST_ID,");
            sb.Append("                CNT.CONTAINER_NR,");
            sb.Append("                PCK.PACK_TYPE_MST_PK PACK_TYPE_PK,");
            sb.Append("                PCK.PACK_TYPE_ID,");
            sb.Append("                CNT.PACK_COUNT QUANTITY,");
            sb.Append("                NVL(CNT.GROSS_WEIGHT, 0) GROSS_WEIGHT,");
            sb.Append("                NVL(CNT.NET_WEIGHT, 0) NET_WEIGHT,");
            sb.Append("                CNT.VOLUME_IN_CBM,");
            sb.Append("                NVL(CNT.CHARGEABLE_WEIGHT, 0) CHARGEABLE_WEIGHT,");
            sb.Append("                CNT.COMMODITY_MST_FK,");
            sb.Append("                CNT.COMMODITY_MST_FKS,");
            sb.Append("                CONT.CONTAINER_TAREWEIGHT_TONE,");
            sb.Append("                NULL NO_OF_BOXES,");
            sb.Append("                CNT.SEAL_NUMBER SEAL_NR,");
            sb.Append("                CNT.GOODS_DESCRIPTION,");
            sb.Append("                CNT.MARKS_NUMBERS,");
            sb.Append("                CNT.REMARKS");
            sb.Append("   FROM SYN_EBKG_ESI_HDR_TBL  ESI,");
            sb.Append("       SYN_EBKG_ESI_CNTR_TBL CNT,");
            sb.Append("       SYN_EBKG_ESI_COMM_TBL COMM,       ");
            sb.Append("       CONTAINER_TYPE_MST_TBL CONT,");
            sb.Append("       PACK_TYPE_MST_TBL      PCK,");
            sb.Append("       COMMODITY_MST_TBL      QCOMM");
            sb.Append("");
            sb.Append("  WHERE ESI.ESI_HDR_PK = CNT.ESI_HDR_FK");
            sb.Append("   AND ESI.ESI_HDR_PK = COMM.ESI_HDR_FK(+)");
            sb.Append("   AND CNT.CONTAINER_TYPE_FK = CONT.CONTAINER_TYPE_MST_PK");
            sb.Append("   AND CNT.PACK_TYPE_MST_FK = PCK.PACK_TYPE_MST_PK");
            sb.Append("   AND COMM.COMMODITY_FK = QCOMM.COMMODITY_MST_PK(+)      ");
            sb.Append("   AND ESI.ESI_HDR_PK = " + ESIPK + "");

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Detail ESI"

        /// <summary>
        /// Jobs the crad_ update_ esi.
        /// </summary>
        /// <param name="StrcurrentBookingID">The strcurrent booking identifier.</param>
        /// <param name="StrcurrentJobCardID">The strcurrent job card identifier.</param>
        /// <returns></returns>
        public ArrayList JobCrad_Update_ESI(int StrcurrentBookingID = 0, int StrcurrentJobCardID = 0)
        {
            //'Update the Esiin Job card
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder();
            StringBuilder sb1 = new StringBuilder();
            DataSet dsContainerData = null;
            int IRcnt = 0;
            int Int_Container_pk = 0;
            int k = 0;
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            string[] ArrSealnr = null;
            string Slnr = null;
            DataSet ds = null;
            string StrcurrentBookingpk = null;

            try
            {
                DataSet dsContainer = null;
                sb.Append("  SELECT JC.BOOKING_SEA_FK, SEC.JOB_TRN_CONT_PK,SEC.JOB_CARD_SEA_EXP_FK, SEC.CONTAINER_TYPE_MST_FK");
                sb.Append("  FROM JOB_CARD_TRN JC, JOB_TRN_CONT SEC");
                sb.Append("  WHERE JC.JOB_CARD_TRN_PK = SEC.JOB_CARD_SEA_EXP_FK(+)");
                sb.Append("   AND SEC.JOB_CARD_SEA_EXP_FK = " + StrcurrentJobCardID);
                sb.Append("  ORDER BY SEC.container_type_mst_fk ASC ");
                dsContainer = objWF.GetDataSet(sb.ToString());
                sb.Remove(0, sb.Length);

                sb.Append("");
                sb.Append(" SELECT ESI.*, EEC.*, MY_CONCADINATE_FUN(NVL(ESI.ESI_HDR_PK, 0), 1) MARKS_NUMBERS_IN1, MY_CONCADINATE_FUN(NVL(ESI.ESI_HDR_PK, 0), 2) GOODS_DESCRIPTION_IN1, MY_CONCADINATE_FUN(NVL(ESI.ESI_HDR_PK, 0), 3) REMARKS1,EEC.PACK_COUNT as PACK_COUNT1,");
                sb.Append(" EEC.VOLUME_IN_CBM VOLUME_IN_CBM1,EEC.GROSS_WEIGHT GROSS_WEIGHT1,EEC.NET_WEIGHT NET_WEIGHT1");
                sb.Append("  FROM SYN_EBKG_ESI_HDR_TBL ESI, SYN_EBKG_ESI_CNTR_TBL EEC");
                sb.Append(" WHERE ESI.ESI_HDR_PK = EEC.ESI_HDR_FK");
                if (StrcurrentBookingID != 0)
                {
                    sb.Append("   AND ESI.BOOKING_SEA_FK = " + StrcurrentBookingID);
                }
                else if (StrcurrentJobCardID != 0)
                {
                    sb1.Append("  SELECT J.BOOKING_SEA_FK");
                    sb1.Append("  FROM JOB_CARD_TRN J");
                    sb1.Append("  WHERE J.JOB_CARD_TRN_PK =" + StrcurrentJobCardID);
                    if (StrcurrentJobCardID != 0)
                    {
                        ds = objWF.GetDataSet(sb1.ToString());
                    }

                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        StrcurrentBookingpk = Convert.ToString(ds.Tables[0].Rows[0]["BOOKING_SEA_FK"]);
                    }
                    sb.Append("   AND ESI.BOOKING_SEA_FK = " + StrcurrentBookingpk);
                }
                sb.Append(" ORDER BY eec.container_type_fk ASC ");

                dsContainerData = objWF.GetDataSet(sb.ToString());

                if (dsContainerData.Tables[0].Rows.Count > 0)
                {
                    TRAN = objWK.MyConnection.BeginTransaction();
                    OracleCommand insCommand = new OracleCommand();
                    OracleCommand updCommand = new OracleCommand();
                    if (dsContainerData.Tables[0].Rows.Count == dsContainer.Tables[0].Rows.Count)
                    {
                        for (IRcnt = 0; IRcnt <= dsContainerData.Tables[0].Rows.Count - 1; IRcnt++)
                        {
                            if ((string.IsNullOrEmpty(dsContainer.Tables[0].Rows[IRcnt]["JOB_TRN_CONT_PK"].ToString())))
                            {
                                //'Nothing to Insert , Only Update the Things
                            }
                            else if (string.IsNullOrEmpty(dsContainerData.Tables[0].Rows[IRcnt]["CONTAINER_TYPE_FK"].ToString()))
                            {
                                var _with2 = updCommand;
                                updCommand.Parameters.Clear();
                                _with2.Connection = objWK.MyConnection;
                                _with2.CommandType = CommandType.StoredProcedure;
                                _with2.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_ESI_TRN_SEA_EXP_CONT_UPD";
                                var _with3 = _with2.Parameters;
                                _with3.Add("JOB_TRN_SEA_EXP_CONT_PK_IN", dsContainer.Tables[0].Rows[IRcnt]["JOB_TRN_CONT_PK"]).Direction = ParameterDirection.Input;
                                _with3.Add("JOB_CARD_SEA_EXP_FK_IN", dsContainer.Tables[0].Rows[IRcnt]["JOB_CARD_SEA_EXP_FK"]).Direction = ParameterDirection.Input;

                                _with3.Add("CONTAINER_NUMBER_IN", dsContainerData.Tables[0].Rows[IRcnt]["CONTAINER_NR"]).Direction = ParameterDirection.Input;
                                _with3.Add("CONTAINER_TYPE_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["CONTAINER_TYPE_FK"]).Direction = ParameterDirection.Input;

                                if (!string.IsNullOrEmpty(dsContainerData.Tables[0].Rows[IRcnt]["SEAL_NUMBER"].ToString()))
                                {
                                    Slnr = Convert.ToString(dsContainerData.Tables[0].Rows[IRcnt]["SEAL_NUMBER"]);
                                    if (!string.IsNullOrEmpty(Slnr))
                                    {
                                        ArrSealnr = Slnr.Split('$');
                                    }
                                    if (Convert.ToInt32(ArrSealnr[3]) == 1)
                                    {
                                        Slnr = ArrSealnr[0];
                                    }
                                    else if (Convert.ToInt32(ArrSealnr[3]) == 2)
                                    {
                                        Slnr = ArrSealnr[1];
                                    }
                                    else if (Convert.ToInt32(ArrSealnr[3]) == 3)
                                    {
                                        Slnr = ArrSealnr[2];
                                    }
                                    else
                                    {
                                        Slnr = ArrSealnr[1];
                                    }
                                }
                                else
                                {
                                    Slnr = "";
                                }

                                _with3.Add("SEAL_NUMBER_IN", (string.IsNullOrEmpty(Slnr) ? "" : Slnr)).Direction = ParameterDirection.Input;

                                _with3.Add("VOLUME_IN_CBM_IN", dsContainerData.Tables[0].Rows[IRcnt]["VOLUME_IN_CBM1"]).Direction = ParameterDirection.Input;
                                _with3.Add("GROSS_WEIGHT_IN", dsContainerData.Tables[0].Rows[IRcnt]["GROSS_WEIGHT1"]).Direction = ParameterDirection.Input;
                                _with3.Add("NET_WEIGHT_IN", dsContainerData.Tables[0].Rows[IRcnt]["NET_WEIGHT1"]).Direction = ParameterDirection.Input;

                                // .Add("CHARGEABLE_WEIGHT_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("JOB_TRN_CONT_PK")).Direction = ParameterDirection.Input
                                _with3.Add("PACK_TYPE_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                                _with3.Add("PACK_COUNT_IN", dsContainerData.Tables[0].Rows[IRcnt]["PACK_COUNT1"]).Direction = ParameterDirection.Input;
                                _with3.Add("COMMODITY_MST_FKS_IN", dsContainerData.Tables[0].Rows[IRcnt]["COMMODITY_MST_FKS"]).Direction = ParameterDirection.Input;

                                //  .Add("CONTAINER_PK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("JOB_CARD_TRN_PK")).Direction = ParameterDirection.Input
                                _with3.Add("MARKS_NUMBERS_IN", dsContainerData.Tables[0].Rows[IRcnt]["MARKS_NUMBERS1"]).Direction = ParameterDirection.Input;
                                _with3.Add("GOODS_DESCRIPTION_IN", dsContainerData.Tables[0].Rows[IRcnt]["GOODS_DESCRIPTION1"]).Direction = ParameterDirection.Input;

                                //'added
                                _with3.Add("QUANTITY_IN", dsContainerData.Tables[0].Rows[IRcnt]["PACK_COUNT1"]).Direction = ParameterDirection.Input;
                                _with3.Add("REMARKS_IN", dsContainerData.Tables[0].Rows[IRcnt]["REMARKS1"]).Direction = ParameterDirection.Input;
                                _with3.Add("PYMT_TYPE_IN", dsContainerData.Tables[0].Rows[IRcnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                                _with3.Add("CARGO_MOVE_FK_IN", (Convert.ToInt32(dsContainerData.Tables[0].Rows[IRcnt]["CARGO_MOVE_FK"].ToString()) == 0 ? "" : dsContainerData.Tables[0].Rows[IRcnt]["CARGO_MOVE_FK"].ToString())).Direction = ParameterDirection.Input;
                                //.Add("SHIPPING_TERMS_MST_FK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("PYMT_TERMS")).Direction = ParameterDirection.Input
                                _with3.Add("SHIPPING_TERMS_MST_FK_IN", (Convert.ToInt32(dsContainerData.Tables[0].Rows[IRcnt]["PYMT_TERMS"].ToString()) == 0 ? "" : dsContainerData.Tables[0].Rows[IRcnt]["PYMT_TERMS"].ToString())).Direction = ParameterDirection.Input;
                                _with3.Add("NOTIFY1_CUST_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["NOTIFYPARTY_FK1"]).Direction = ParameterDirection.Input;

                                _with3.Add("NOTIFY2_CUST_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["NOTIFYPARTY_FK2"]).Direction = ParameterDirection.Input;
                                // .Add("CONSIGNEE_CUST_MST_FK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("CONSIGN_MST_FK")).Direction = ParameterDirection.Input
                                // .Add("DEL_PLACE_MST_FK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("PFD_FK")).Direction = ParameterDirection.Input
                                // .Add("COL_PLACE_MST_FK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("POR_FK")).Direction = ParameterDirection.Input
                                _with3.Add("CONSIGNEE_CUST_MST_FK_IN", (Convert.ToInt32(dsContainerData.Tables[0].Rows[IRcnt]["CONSIGN_MST_FK"].ToString()) == 0 ? "" : dsContainerData.Tables[0].Rows[IRcnt]["CONSIGN_MST_FK"].ToString())).Direction = ParameterDirection.Input;
                                if (string.IsNullOrEmpty(dsContainerData.Tables[0].Rows[IRcnt]["PFD_FK"].ToString()))
                                {
                                    _with3.Add("DEL_PLACE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with3.Add("DEL_PLACE_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["PFD_FK"]).Direction = ParameterDirection.Input;
                                }
                                if (string.IsNullOrEmpty(dsContainerData.Tables[0].Rows[IRcnt]["POR_FK"].ToString()))
                                {
                                    _with3.Add("COL_PLACE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with3.Add("COL_PLACE_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["POR_FK"]).Direction = ParameterDirection.Input;
                                }
                                _with3.Add("ESI_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                _with3.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                                var _with4 = objWK.MyDataAdapter;
                                _with4.UpdateCommand = updCommand;
                                _with4.UpdateCommand.Transaction = TRAN;
                                _with4.UpdateCommand.ExecuteNonQuery();
                                Int_Container_pk = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
                            }
                            else if (dsContainer.Tables[0].Rows[IRcnt]["CONTAINER_TYPE_MST_FK"] == dsContainerData.Tables[0].Rows[IRcnt]["CONTAINER_TYPE_FK"])
                            {
                                var _with5 = updCommand;
                                updCommand.Parameters.Clear();
                                _with5.Connection = objWK.MyConnection;
                                _with5.CommandType = CommandType.StoredProcedure;
                                _with5.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_ESI_TRN_SEA_EXP_CONT_UPD";
                                var _with6 = _with5.Parameters;
                                _with6.Add("JOB_TRN_SEA_EXP_CONT_PK_IN", dsContainer.Tables[0].Rows[IRcnt]["JOB_TRN_CONT_PK"]).Direction = ParameterDirection.Input;
                                _with6.Add("JOB_CARD_SEA_EXP_FK_IN", dsContainer.Tables[0].Rows[IRcnt]["JOB_CARD_SEA_EXP_FK"]).Direction = ParameterDirection.Input;

                                _with6.Add("CONTAINER_NUMBER_IN", dsContainerData.Tables[0].Rows[IRcnt]["CONTAINER_NR"]).Direction = ParameterDirection.Input;
                                _with6.Add("CONTAINER_TYPE_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["CONTAINER_TYPE_FK"]).Direction = ParameterDirection.Input;

                                if (!string.IsNullOrEmpty(dsContainerData.Tables[0].Rows[IRcnt]["SEAL_NUMBER"].ToString()))
                                {
                                    Slnr = Convert.ToString(dsContainerData.Tables[0].Rows[IRcnt]["SEAL_NUMBER"]);
                                    if (!string.IsNullOrEmpty(Slnr))
                                    {
                                        ArrSealnr = Slnr.Split('$');
                                    }
                                    if (Convert.ToInt32(ArrSealnr[3]) == 1)
                                    {
                                        Slnr = ArrSealnr[0];
                                    }
                                    else if (Convert.ToInt32(ArrSealnr[3]) == 2)
                                    {
                                        Slnr = ArrSealnr[1];
                                    }
                                    else if (Convert.ToInt32(ArrSealnr[3]) == 3)
                                    {
                                        Slnr = ArrSealnr[2];
                                    }
                                    else
                                    {
                                        Slnr = ArrSealnr[1];
                                    }
                                }
                                else
                                {
                                    Slnr = "";
                                }

                                _with6.Add("SEAL_NUMBER_IN", (string.IsNullOrEmpty(Slnr) ? "" : Slnr)).Direction = ParameterDirection.Input;

                                _with6.Add("VOLUME_IN_CBM_IN", dsContainerData.Tables[0].Rows[IRcnt]["VOLUME_IN_CBM1"]).Direction = ParameterDirection.Input;
                                _with6.Add("GROSS_WEIGHT_IN", dsContainerData.Tables[0].Rows[IRcnt]["GROSS_WEIGHT1"]).Direction = ParameterDirection.Input;
                                _with6.Add("NET_WEIGHT_IN", dsContainerData.Tables[0].Rows[IRcnt]["NET_WEIGHT1"]).Direction = ParameterDirection.Input;

                                // .Add("CHARGEABLE_WEIGHT_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("JOB_TRN_CONT_PK")).Direction = ParameterDirection.Input
                                _with6.Add("PACK_TYPE_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                                _with6.Add("PACK_COUNT_IN", dsContainerData.Tables[0].Rows[IRcnt]["PACK_COUNT1"]).Direction = ParameterDirection.Input;
                                _with6.Add("COMMODITY_MST_FKS_IN", dsContainerData.Tables[0].Rows[IRcnt]["COMMODITY_MST_FKS"]).Direction = ParameterDirection.Input;

                                //  .Add("CONTAINER_PK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("JOB_CARD_TRN_PK")).Direction = ParameterDirection.Input
                                _with6.Add("MARKS_NUMBERS_IN", dsContainerData.Tables[0].Rows[IRcnt]["MARKS_NUMBERS1"]).Direction = ParameterDirection.Input;
                                _with6.Add("GOODS_DESCRIPTION_IN", dsContainerData.Tables[0].Rows[IRcnt]["GOODS_DESCRIPTION1"]).Direction = ParameterDirection.Input;

                                //'added
                                _with6.Add("QUANTITY_IN", dsContainerData.Tables[0].Rows[IRcnt]["PACK_COUNT1"]).Direction = ParameterDirection.Input;
                                _with6.Add("REMARKS_IN", dsContainerData.Tables[0].Rows[IRcnt]["REMARKS1"]).Direction = ParameterDirection.Input;
                                _with6.Add("PYMT_TYPE_IN", dsContainerData.Tables[0].Rows[IRcnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                                _with6.Add("CARGO_MOVE_FK_IN", (Convert.ToInt32(dsContainerData.Tables[0].Rows[IRcnt]["CARGO_MOVE_FK"].ToString()) == 0 ? "" : dsContainerData.Tables[0].Rows[IRcnt]["CARGO_MOVE_FK"].ToString())).Direction = ParameterDirection.Input;
                                //.Add("SHIPPING_TERMS_MST_FK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("PYMT_TERMS")).Direction = ParameterDirection.Input
                                _with6.Add("SHIPPING_TERMS_MST_FK_IN", (Convert.ToInt32(dsContainerData.Tables[0].Rows[IRcnt]["PYMT_TERMS"].ToString()) == 0 ? "" : dsContainerData.Tables[0].Rows[IRcnt]["PYMT_TERMS"].ToString())).Direction = ParameterDirection.Input;
                                _with6.Add("NOTIFY1_CUST_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["NOTIFYPARTY_FK1"]).Direction = ParameterDirection.Input;

                                _with6.Add("NOTIFY2_CUST_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["NOTIFYPARTY_FK2"]).Direction = ParameterDirection.Input;
                                // .Add("CONSIGNEE_CUST_MST_FK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("CONSIGN_MST_FK")).Direction = ParameterDirection.Input
                                // .Add("DEL_PLACE_MST_FK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("PFD_FK")).Direction = ParameterDirection.Input
                                // .Add("COL_PLACE_MST_FK_IN", dsContainerData.Tables(0).Rows(IRcnt).Item("POR_FK")).Direction = ParameterDirection.Input
                                _with6.Add("CONSIGNEE_CUST_MST_FK_IN", (Convert.ToInt32(dsContainerData.Tables[0].Rows[IRcnt]["CONSIGN_MST_FK"].ToString()) == 0 ? "" : dsContainerData.Tables[0].Rows[IRcnt]["CONSIGN_MST_FK"].ToString())).Direction = ParameterDirection.Input;
                                if (string.IsNullOrEmpty(dsContainerData.Tables[0].Rows[IRcnt]["PFD_FK"].ToString()))
                                {
                                    _with6.Add("DEL_PLACE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with6.Add("DEL_PLACE_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["PFD_FK"]).Direction = ParameterDirection.Input;
                                }
                                if (string.IsNullOrEmpty(dsContainerData.Tables[0].Rows[IRcnt]["POR_FK"].ToString()))
                                {
                                    _with6.Add("COL_PLACE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with6.Add("COL_PLACE_MST_FK_IN", dsContainerData.Tables[0].Rows[IRcnt]["POR_FK"]).Direction = ParameterDirection.Input;
                                }
                                _with6.Add("ESI_FLAG_IN", 1).Direction = ParameterDirection.Input;
                                _with6.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                                var _with7 = objWK.MyDataAdapter;
                                _with7.UpdateCommand = updCommand;
                                _with7.UpdateCommand.Transaction = TRAN;
                                _with7.UpdateCommand.ExecuteNonQuery();
                                Int_Container_pk = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
                            }
                            //'If dsContainer.Tables(0).Rows(IRcnt).Item("CONTAINER_TYPE_MST_FK") = dsContainerData.Tables(0).Rows(IRcnt).Item("CONTAINER_TYPE_FK") Then
                            //End If ''(IsDBNull(dsContainer.Tables(0).Rows(IRcnt).Item("JOB_TRN_CONT_PK"))) Then
                        }
                        ///' For IRcnt = 0 To dsContainerData.Tables(0).Rows.Count - 1
                    }
                    //' dsContainerData.Tables(0).Rows.Count = dsContainer.Tables(0).Rows.Count Then
                }
                //'dsContainerData.Tables(0).Rows.Count > 0 Then

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
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
                throw;
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

        #region "GetMainBookingData"

        //Function gets the Booking data for particular BookingPK
        /// <summary>
        /// Gets the main booking data.
        /// </summary>
        /// <param name="bookingPK">The booking pk.</param>
        /// <returns></returns>
        public DataSet GetMainBookingData(string bookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("      DISTINCT bst.booking_sea_pk,");
            SQL.Append("      bst.booking_ref_no,");
            SQL.Append("      bst.cargo_type,cust.customer_id,");
            SQL.Append("      cust.customer_name, cust.del_address,");
            SQL.Append("      bst.cust_customer_mst_fk,");
            SQL.Append("      col_place.place_name AS \"CollectionPlace\",");
            SQL.Append("      bst.col_place_mst_fk,");
            SQL.Append("      del_place.place_name AS \"DeliveryPlace\",");
            SQL.Append("      bst.del_place_mst_fk,");
            SQL.Append("      POL.port_name as \"POL\",");
            SQL.Append("      bst.port_mst_pol_fk AS \"POLPK\",");
            SQL.Append("      POD.port_name as \"POD\",");
            SQL.Append("      bst.port_mst_pod_fk AS \"PODPK\",");
            SQL.Append("      oprator.operator_id,");
            SQL.Append("      oprator.operator_name,");
            SQL.Append("      bst.operator_mst_fk,");
            //SQL.Append(vbCrLf & "      bst.vessel_name,")
            //SQL.Append(vbCrLf & "      bst.voyage,")
            SQL.Append("      VVT.VOYAGE_TRN_PK \"VoyagePK\",");
            SQL.Append("      V.VESSEL_NAME,");
            SQL.Append("      VVT.VOYAGE,");
            SQL.Append("      bst.eta_date,");
            SQL.Append("      bst.etd_date,");

            SQL.Append("      (   CASE WHEN bst.cust_customer_mst_fk IN ");
            SQL.Append("                                         (SELECT");
            SQL.Append("                                                 C.CUSTOMER_MST_PK ");
            SQL.Append("                                          FROM");
            SQL.Append("                                                 CUSTOMER_MST_TBL C,");
            SQL.Append("                                                 CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_CATEGORY_TRN CCT ");
            SQL.Append("                                          WHERE");
            SQL.Append("                                                 C.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
            SQL.Append("                                                 AND CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ");
            SQL.Append("                                                 AND UPPER(CC.CUSTOMER_CATEGORY_ID) LIKE 'SHIPPER'");
            SQL.Append("                                         )");
            SQL.Append("          THEN cust.customer_id ELSE '' END");
            SQL.Append("       ) \"Shipper\",");

            SQL.Append("      (   CASE WHEN bst.cust_customer_mst_fk IN ");
            SQL.Append("                                         (SELECT");
            SQL.Append("                                                 C.CUSTOMER_MST_PK ");
            SQL.Append("                                          FROM");
            SQL.Append("                                                 CUSTOMER_MST_TBL C,");
            SQL.Append("                                                 CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_CATEGORY_TRN CCT ");
            SQL.Append("                                          WHERE");
            SQL.Append("                                                 C.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
            SQL.Append("                                                 AND CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ");
            SQL.Append("                                                 AND UPPER(CC.CUSTOMER_CATEGORY_ID) LIKE 'SHIPPER'");
            SQL.Append("                                         )");
            SQL.Append("          THEN cust.customer_name ELSE '' END");
            SQL.Append("       ) \"ShipperName\",");

            SQL.Append("      (   CASE WHEN bst.cust_customer_mst_fk IN ");
            SQL.Append("                                         (SELECT");
            SQL.Append("                                                 C.CUSTOMER_MST_PK ");
            SQL.Append("                                          FROM");
            SQL.Append("                                                 CUSTOMER_MST_TBL C,");
            SQL.Append("                                                 CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_CATEGORY_TRN CCT ");
            SQL.Append("                                          WHERE");
            SQL.Append("                                                 C.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
            SQL.Append("                                                 AND CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ");
            SQL.Append("                                                 AND UPPER(CC.CUSTOMER_CATEGORY_ID) LIKE 'SHIPPER'");
            SQL.Append("                                         )");
            SQL.Append("          THEN TO_CHAR(cust.customer_mst_pk) ELSE ''END");
            SQL.Append("       ) \"shipper_cust_mst_fk\",");

            SQL.Append("      consignee.customer_id AS \"Consignee\",");
            SQL.Append("      consignee.customer_name AS \"ConsigneeName\",");
            SQL.Append("      bst.cons_customer_mst_fk consignee_cust_mst_fk,");
            SQL.Append("      bst.cb_agent_mst_fk,");
            SQL.Append("      cbagnt.agent_id \"cbAgent\",");
            SQL.Append("      cbagnt.agent_name \"cbAgentName\",");
            SQL.Append("      bst.cl_agent_mst_fk,");
            SQL.Append("      clagnt.agent_id \"clAgent\",");
            SQL.Append("      clagnt.agent_name \"clAgentName\",");
            SQL.Append("      bst.cargo_move_fk,");
            SQL.Append("      bst.pymt_type,");
            SQL.Append("      bst.commodity_group_fk,");
            SQL.Append("      comm.commodity_group_desc,");
            SQL.Append("      '' GOODS_DESCRIPTION,");
            SQL.Append("      '' MARKS_NUMBERS,");
            SQL.Append("      0 CHK_NOMINATED,");
            SQL.Append("      1 CHK_CSR,");
            SQL.Append("      0 SALES_EXEC_FK,");
            SQL.Append("      '' SALES_EXEC_ID,");
            SQL.Append("      '' SALES_EXEC_NAME ");
            SQL.Append(" FROM ");
            SQL.Append("      BOOKING_MST_TBL bst,");
            SQL.Append("      BOOKING_TRN btrn,");
            SQL.Append("      port_mst_tbl POD,");
            SQL.Append("      port_mst_tbl POL,");
            SQL.Append("      customer_mst_tbl cust,");
            SQL.Append("      customer_mst_tbl consignee,");
            SQL.Append("      place_mst_tbl col_place,");
            SQL.Append("      place_mst_tbl del_place,");
            SQL.Append("      operator_mst_tbl oprator,");
            SQL.Append("      agent_mst_tbl clagnt,");
            SQL.Append("      agent_mst_tbl cbagnt,");
            SQL.Append("      commodity_group_mst_tbl comm,");
            SQL.Append("      VESSEL_VOYAGE_TBL V,");
            SQL.Append("      VESSEL_VOYAGE_TRN VVT");

            SQL.Append("WHERE ");
            SQL.Append("      bst.cust_customer_mst_fk = cust.customer_mst_pk");
            SQL.Append("      AND bst.col_place_mst_fk = col_place.place_pk (+)");
            SQL.Append("      AND bst.del_place_mst_fk = del_place.place_pk (+)");
            SQL.Append("      AND bst.cb_agent_mst_fk = cbagnt.agent_mst_pk(+)");
            SQL.Append("      AND bst.cl_agent_mst_fk = clagnt.agent_mst_pk(+)");
            SQL.Append("      AND bst.port_mst_pol_fk = POL.port_mst_pk");
            SQL.Append("      AND bst.port_mst_pod_fk = POD.port_mst_pk");
            SQL.Append("      AND bst.operator_mst_fk = oprator.operator_mst_pk(+)");
            SQL.Append("      AND bst.booking_sea_pk =" + bookingPK);
            SQL.Append("      AND bst.cons_customer_mst_fk = consignee.customer_mst_pk(+)");
            SQL.Append("      AND bst.status = 2");
            // only for the confirmed booking.
            SQL.Append("      AND bst.commodity_group_fk = comm.commodity_group_pk(+)");
            SQL.Append("      AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK");
            SQL.Append("      AND BST.VESSEL_VOYAGE_FK = VVT.VOYAGE_TRN_PK");

            try
            {
                return objWF.GetDataSet(SQL.ToString());
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
        /// Gets the booking container data.
        /// </summary>
        /// <param name="bookingPK">The booking pk.</param>
        /// <returns></returns>
        public DataSet GetBookingContainerData(string bookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("      btrn.container_type_mst_fk,");
            SQL.Append("      cont.container_type_mst_id,");
            SQL.Append("      btrn.no_of_boxes,");
            SQL.Append("      com.commodity_name,");
            SQL.Append("      btrn.commodity_mst_fk");
            SQL.Append("FROM");
            SQL.Append("      BOOKING_MST_TBL bst,");
            SQL.Append("      BOOKING_TRN btrn,");
            SQL.Append("      container_type_mst_tbl cont,");
            SQL.Append("      commodity_mst_tbl com");
            SQL.Append("WHERE");
            SQL.Append("      btrn.booking_sea_fk =  bst.booking_sea_pk");
            SQL.Append("      AND btrn.container_type_mst_fk = cont.container_type_mst_pk");
            SQL.Append("      AND btrn.booking_sea_fk =" + bookingPK);
            SQL.Append("      AND btrn.commodity_mst_fk =com.commodity_mst_pk(+) ");
            SQL.Append("      AND bst.status = 2");
            // only for the confirmed booking.
            SQL.Append(" ORDER BY cont.Preferences");

            try
            {
                return objWF.GetDataSet(SQL.ToString());
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
        /// Gets the booking container LCL data.
        /// </summary>
        /// <param name="bookingPK">The booking pk.</param>
        /// <returns></returns>
        public DataSet GetBookingContainerLCLData(string bookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("select");
            SQL.Append("0 JOB_TRN_CONT_PK,");
            //modified by latha
            SQL.Append("bkg_trn.container_type_mst_fk,");
            SQL.Append("'' container_id,");
            SQL.Append("'' container_number,");
            SQL.Append("'' seal_number,");
            SQL.Append("bkg.volume_in_cbm volume_in_cbm,");
            SQL.Append("bkg.gross_weight gross_weight,");
            SQL.Append("bkg.net_weight net_weight,");
            SQL.Append("bkg.chargeable_weight,");
            SQL.Append("bkg.pack_typ_mst_fk pack_type_mst_fk,");
            SQL.Append("bkg.pack_count pack_count,");
            SQL.Append("bkg_trn.commodity_mst_fk,");
            SQL.Append("'' load_date");
            SQL.Append(", '0' CONTAINER_PK");
            SQL.Append("from");
            SQL.Append("BOOKING_MST_TBL bkg,");
            SQL.Append("BOOKING_TRN bkg_trn,");
            SQL.Append("commodity_mst_tbl com ");
            SQL.Append("where");
            SQL.Append("bkg_trn.booking_sea_fk = bkg.booking_sea_pk");
            SQL.Append("AND bkg_trn.booking_sea_fk =" + bookingPK);
            SQL.Append("AND bkg_trn.commodity_mst_fk =com.commodity_mst_pk(+) ");
            SQL.Append("AND bkg.status = 2");

            try
            {
                return objWF.GetDataSet(SQL.ToString());
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
        /// Gets the booking freight data.
        /// </summary>
        /// <param name="bookingPK">The booking pk.</param>
        /// <param name="baseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet GetBookingFreightData(string bookingPK, Int64 baseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("     '0' job_trn_sea_exp_fd_pk,");
            SQL.Append("     btrn.container_type_mst_fk,");
            SQL.Append("     frt.freight_element_id,");
            SQL.Append("     frt.freight_element_name,");
            SQL.Append("     frt.freight_element_mst_pk,");
            SQL.Append("     basis,");
            SQL.Append("     quantity,");
            SQL.Append("     DECODE(bfrt.pymt_type,1,'Prepaid',2,'Collect') freight_type,");
            SQL.Append("     nvl(bfrt.tariff_rate,1)*nvl(btrn.no_of_boxes,1) freight_amt,");
            SQL.Append("     bfrt.currency_mst_fk,");
            SQL.Append("     ROUND(GET_EX_RATE(bfrt.currency_mst_fk," + baseCurrency + ",round(sysdate - .5)),4) AS ROE ,");
            SQL.Append("     'false' \"Delete\", 1 \"Print\" ");
            SQL.Append("FROM");
            SQL.Append("     BOOKING_MST_TBL bst,");
            SQL.Append("     BOOKING_TRN btrn,");
            SQL.Append("     BOOKING_TRN_FRT_DTLS bfrt,");
            SQL.Append("     container_type_mst_tbl cont,");
            SQL.Append("     freight_element_mst_tbl frt,");
            SQL.Append("     currency_type_mst_tbl curr");
            SQL.Append("WHERE");
            SQL.Append("     bfrt.freight_element_mst_fk = frt.freight_element_mst_pk");
            SQL.Append("     AND bfrt.booking_trn_sea_fk = btrn.booking_trn_sea_pk");
            SQL.Append("     AND btrn.booking_sea_fk = bst.booking_sea_pk");
            SQL.Append("     AND btrn.container_type_mst_fk = cont.container_type_mst_pk");
            SQL.Append("     AND bst.booking_sea_pk = " + bookingPK);
            SQL.Append("     AND bfrt.currency_mst_fk = curr.currency_mst_pk");
            SQL.Append("     AND bst.status = 2");
            // only for the confirmed booking.
            SQL.Append("     ORDER BY cont.container_type_mst_id,frt.freight_element_id ");

            try
            {
                return objWF.GetDataSet(SQL.ToString());
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
        /// Gets the booking freight LCL data.
        /// </summary>
        /// <param name="bookingPK">The booking pk.</param>
        /// <param name="baseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet GetBookingFreightLCLData(string bookingPK, Int64 baseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("     '0' job_trn_sea_exp_fd_pk,");
            SQL.Append("     btrn.container_type_mst_fk,");
            SQL.Append("     frt.freight_element_id,");
            SQL.Append("     frt.freight_element_name,");
            SQL.Append("     frt.freight_element_mst_pk,");
            SQL.Append("     DECODE(btrn.basis,0,' ',1,'%',2,'Flat rate',3,'Kgs',4,'Unit') basis,");
            SQL.Append("     btrn.quantity,");
            SQL.Append("     DECODE(bfrt.pymt_type,1,'Prepaid',2,'Collect') freight_type,");
            SQL.Append("     nvl(bfrt.tariff_rate,1)*nvl(btrn.quantity,1) freight_amt,");
            SQL.Append("     bfrt.currency_mst_fk,");
            SQL.Append("     ROUND(GET_EX_RATE(bfrt.currency_mst_fk," + baseCurrency + ",round(sysdate - .5)),4) AS ROE ,");
            SQL.Append("     'false' \"Delete\", 1 \"Print\" ");
            SQL.Append("     FROM");
            SQL.Append("     BOOKING_MST_TBL bst,");
            SQL.Append("     BOOKING_TRN btrn,");
            SQL.Append("     BOOKING_TRN_FRT_DTLS bfrt,");
            SQL.Append("     freight_element_mst_tbl frt,");
            SQL.Append("     currency_type_mst_tbl curr");
            SQL.Append("     WHERE");
            SQL.Append("     bfrt.freight_element_mst_fk = frt.freight_element_mst_pk");
            SQL.Append("     AND bfrt.booking_trn_sea_fk = btrn.booking_trn_sea_pk");
            SQL.Append("     AND btrn.booking_sea_fk = bst.booking_sea_pk");
            SQL.Append("     AND bst.booking_sea_pk =" + bookingPK);
            SQL.Append("     AND bfrt.currency_mst_fk = curr.currency_mst_pk");
            SQL.Append("     AND bst.status = 2");
            SQL.Append("     ORDER BY frt.freight_element_id");

            try
            {
                return objWF.GetDataSet(SQL.ToString());
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
        /// Gets the base currency.
        /// </summary>
        /// <returns></returns>
        public Int64 GetBaseCurrency()
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append(" SELECT c.currency_mst_fk FROM corporate_mst_tbl c ");

            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(SQL.ToString()));
            }
            catch (OracleException sqlExp)
            {
                return 0;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "GetMainBookingData"

        #region "Property"

        /// <summary>
        /// The COM GRP
        /// </summary>
        private int ComGrp;

        /// <summary>
        /// Gets or sets the commodity group.
        /// </summary>
        /// <value>
        /// The commodity group.
        /// </value>
        public int CommodityGroup
        {
            get { return ComGrp; }
            set { ComGrp = value; }
        }

        #endregion "Property"

        #region "Commodity"

        /// <summary>
        /// Fetches the commodity.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCommodity()
        {
            string strSQL = null;
            strSQL = "SELECT c.commodity_mst_pk,c.commodity_name  FROM commodity_mst_tbl c WHERE c.active_flag =1 ORDER BY c.commodity_name";
            WorkFlow objWF = new WorkFlow();
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

        #endregion "Commodity"

        #region "Spacial Request"

        /// <summary>
        /// Saves the transaction hz SPCL.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionHZSpcl(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            try
            {
                if (!string.IsNullOrEmpty(strSpclRequest))
                {
                    arrMessage.Clear();
                    string[] strParam = null;
                    strParam = strSpclRequest.Split('~');
                    try
                    {
                        var _with8 = SCM;
                        _with8.CommandType = CommandType.StoredProcedure;
                        _with8.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_HAZ_SPL_REQ_INS";
                        var _with9 = _with8.Parameters;
                        _with9.Clear();
                        //BKG_TRN_SEA_FK_IN()
                        _with9.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        //OUTER_PACK_TYPE_MST_FK_IN()
                        _with9.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                        //INNER_PACK_TYPE_MST_FK_IN()
                        _with9.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], "")).Direction = ParameterDirection.Input;
                        //MIN_TEMP_IN()
                        _with9.Add("MIN_TEMP_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                        //MIN_TEMP_UOM_IN()
                        _with9.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? "" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                        //MAX_TEMP_IN()
                        _with9.Add("MAX_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                        //MAX_TEMP_UOM_IN()
                        _with9.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                        //FLASH_PNT_TEMP_IN()
                        _with9.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                        //FLASH_PNT_TEMP_UOM_IN()
                        _with9.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                        //IMDG_CLASS_CODE_IN()
                        _with9.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                        //UN_NO_IN()
                        _with9.Add("UN_NO_IN", getDefault(strParam[9], "")).Direction = ParameterDirection.Input;
                        //IMO_SURCHARGE_IN()
                        _with9.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                        //SURCHARGE_AMT_IN()
                        _with9.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                        //IS_MARINE_POLLUTANT_IN()
                        _with9.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                        //EMS_NUMBER_IN()
                        _with9.Add("EMS_NUMBER_IN", getDefault(strParam[13], "")).Direction = ParameterDirection.Input;
                        _with9.Add("PROPER_SHIPPING_NAME_IN", getDefault(strParam[14], "")).Direction = ParameterDirection.Input;
                        _with9.Add("PACK_CLASS_TYPE_IN", getDefault(strParam[15], "")).Direction = ParameterDirection.Input;
                        //RETURN_VALUE()
                        _with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with8.ExecuteNonQuery();
                        arrMessage.Add("All data saved successfully");
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
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the SPCL req.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataTable fetchSpclReq(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT JOB_TRN_SEA_SPL_PK,");
                    strQuery.Append("JOB_TRN_SEA_EXP_CONT_FK,");
                    strQuery.Append("OUTER_PACK_TYPE_MST_FK,");
                    strQuery.Append("INNER_PACK_TYPE_MST_FK,");
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("FLASH_PNT_TEMP,");
                    strQuery.Append("FLASH_PNT_TEMP_UOM,");
                    strQuery.Append("IMDG_CLASS_CODE,");
                    strQuery.Append("UN_NO,");
                    strQuery.Append("IMO_SURCHARGE,");
                    strQuery.Append("SURCHARGE_AMT,");
                    strQuery.Append("IS_MARINE_POLLUTANT,");
                    strQuery.Append("Q.PROPER_SHIPPING_NAME, ");
                    strQuery.Append("Q.PACK_CLASS_TYPE, ");
                    strQuery.Append("EMS_NUMBER FROM JOB_TRN_SPL_REQ Q");
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.JOB_TRN_SEA_EXP_CONT_FK=" + strPK);
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Saves the transaction reefer.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionReefer(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with10 = SCM;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_REF_SPL_REQ_INS";
                    var _with11 = _with10.Parameters;
                    _with11.Clear();
                    //BOOKING_TRN_SEA_FK_IN()
                    _with11.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //VENTILATION_IN()
                    _with11.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    //AIR_COOL_METHOD_IN()
                    _with11.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    //HUMIDITY_FACTOR_IN()
                    _with11.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //IS_PERSHIABLE_GOODS_IN()
                    _with11.Add("IS_PERSHIABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with11.Add("MIN_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with11.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with11.Add("MAX_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with11.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with11.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with11.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    _with11.Add("REF_VENTILATION_IN", getDefault(strParam[10], "")).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with10.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
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
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        /// <summary>
        /// Fetches the SPCL req reefer.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataTable fetchSpclReqReefer(string strPK)
        {
            if (!string.IsNullOrEmpty(strPK))
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                strQuery.Append("SELECT JOB_TRN_SEA_SPL_PK,");
                strQuery.Append("JOB_TRN_SEA_EXP_CONT_FK,");
                strQuery.Append("VENTILATION,");
                strQuery.Append("AIR_COOL_METHOD,");
                strQuery.Append("HUMIDITY_FACTOR,");
                strQuery.Append("IS_PERSHIABLE_GOODS,");
                strQuery.Append("MIN_TEMP,");
                strQuery.Append("MIN_TEMP_UOM,");
                strQuery.Append("MAX_TEMP,");
                strQuery.Append("MAX_TEMP_UOM,");
                strQuery.Append("PACK_TYPE_MST_FK,");
                strQuery.Append("Q.PACK_COUNT, ");
                strQuery.Append("Q.REF_VENTILATION ");
                strQuery.Append("FROM JOB_TRN_SPL_REQ Q");
                strQuery.Append("WHERE ");
                strQuery.Append("Q.JOB_TRN_SEA_EXP_CONT_FK=" + strPK);
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Saves the transaction odc.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionODC(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with12 = SCM;
                    _with12.CommandType = CommandType.StoredProcedure;
                    _with12.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_ODC_SPL_REQ_INS";
                    var _with13 = _with12.Parameters;
                    _with13.Clear();
                    //BKG_TRN_SEA_FK_IN()
                    _with13.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with13.Add("LENGTH_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with13.Add("LENGTH_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with13.Add("HEIGHT_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with13.Add("HEIGHT_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with13.Add("WIDTH_IN", getDefault(strParam[1], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with13.Add("WIDTH_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with13.Add("WEIGHT_IN", getDefault(strParam[3], "")).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with13.Add("WEIGHT_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with13.Add("VOLUME_IN", "").Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with13.Add("VOLUME_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with13.Add("SLOT_LOSS_IN", "").Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with13.Add("LOSS_QUANTITY_IN", "").Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with13.Add("APPR_REQ_IN", "").Direction = ParameterDirection.Input;
                    if (Convert.ToBoolean(strParam[4]) == true)
                    {
                        _with13.Add("STOWAGE_IN", 1).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with13.Add("STOWAGE_IN", 2).Direction = ParameterDirection.Input;
                    }
                    _with13.Add("HAND_INST_IN", (string.IsNullOrEmpty(strParam[6]) ? "" : strParam[6])).Direction = ParameterDirection.Input;
                    _with13.Add("LASH_INST_IN", (string.IsNullOrEmpty(strParam[7]) ? "" : strParam[7])).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with12.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
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
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        /// <summary>
        /// Fetches the SPCL req odc.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataTable fetchSpclReqODC(string strPK)
        {
            if (!string.IsNullOrEmpty(strPK))
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                strQuery.Append("SELECT ");
                strQuery.Append("JOB_TRN_SEA_SPL_PK,");
                strQuery.Append("JOB_TRN_SEA_EXP_CONT_FK,");
                strQuery.Append("LENGTH,");
                strQuery.Append("LENGTH_UOM_MST_FK,");
                strQuery.Append("HEIGHT,");
                strQuery.Append("HEIGHT_UOM_MST_FK,");
                strQuery.Append("WIDTH,");
                strQuery.Append("WIDTH_UOM_MST_FK,");
                strQuery.Append("WEIGHT,");
                strQuery.Append("WEIGHT_UOM_MST_FK,");
                strQuery.Append("VOLUME,");
                strQuery.Append("VOLUME_UOM_MST_FK,");
                strQuery.Append("SLOT_LOSS,");
                strQuery.Append("LOSS_QUANTITY,");
                strQuery.Append("APPR_REQ, ");
                strQuery.Append("STOWAGE, ");
                strQuery.Append("HANDLING_INSTR, ");
                strQuery.Append("LASHING_INSTR ");
                strQuery.Append("FROM JOB_TRN_SPL_REQ Q");
                strQuery.Append("WHERE ");
                strQuery.Append("Q.JOB_TRN_SEA_EXP_CONT_FK=" + strPK);
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
            }
            else
            {
                return null;
            }
        }

        #endregion "Spacial Request"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="dsContainerData">The ds container data.</param>
        /// <param name="dsTPDetails">The ds tp details.</param>
        /// <param name="dsFreightDetails">The ds freight details.</param>
        /// <param name="dsPurchaseInventory">The ds purchase inventory.</param>
        /// <param name="dsCostDetails">The ds cost details.</param>
        /// <param name="dsPickUpDetails">The ds pick up details.</param>
        /// <param name="dsDropDetails">The ds drop details.</param>
        /// <param name="Update">if set to <c>true</c> [update].</param>
        /// <param name="isEdting">if set to <c>true</c> [is edting].</param>
        /// <param name="ucrNo">The ucr no.</param>
        /// <param name="jobCardRefNumber">The job card reference number.</param>
        /// <param name="userLocation">The user location.</param>
        /// <param name="employeeID">The employee identifier.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsOtherCharges">The ds other charges.</param>
        /// <param name="strBookingRefNo">The string booking reference no.</param>
        /// <param name="strOperatorPk">The string operator pk.</param>
        /// <param name="intIsUpdate">The int is update.</param>
        /// <param name="hdColPlace">The hd col place.</param>
        /// <param name="hdDelPlace">The hd delete place.</param>
        /// <param name="AddVATOSFLAG">The add vatosflag.</param>
        /// <param name="CheckESI">The check esi.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <param name="dsDoc">The ds document.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, DataSet dsContainerData, DataSet dsTPDetails, DataSet dsFreightDetails, DataSet dsPurchaseInventory, DataSet dsCostDetails, DataSet dsPickUpDetails, DataSet dsDropDetails, bool Update, bool isEdting,
        object ucrNo, string jobCardRefNumber, string userLocation, string employeeID, long JobCardPK, DataSet dsOtherCharges, string strBookingRefNo, string strOperatorPk, Int16 intIsUpdate, string hdColPlace = "",
        string hdDelPlace = "", int AddVATOSFLAG = 0, Int32 CheckESI = 0, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, DataSet dsDoc = null)
        {
            int strVoyagepk = Convert.ToInt32(getDefault(M_DataSet.Tables[0].Rows[0]["VOYAGEPK"], 0));
            // By Amit
            objVesselVoyage.ConfigurationPK = M_Configuration_PK;
            objVesselVoyage.CREATED_BY = M_CREATED_BY_FK;
            ///'
            WorkFlow objWK = new WorkFlow();

            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            OracleTransaction TRAN1 = null;

            // To save in Vessel/Voyage
            //
            if (Update == true)
            {
                strVoyagepk = 0;
            }
            TRAN = objWK.MyConnection.BeginTransaction();
            TRAN1 = TRAN;
            //If strVoyagepk = "0" Then
            if (Convert.ToString(strVoyagepk) == "0" & !string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"].ToString()))
            {
                //commented by suryaprasad for implementing tran roll back
                //TRAN1 = objWK.MyConnection.BeginTransaction()
                //end

                objWK.MyCommand.Transaction = TRAN1;
                objWK.MyCommand.Connection = objWK.MyConnection;

                arrMessage = objVesselVoyage.SaveVesselMaster(strVoyagepk, getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"], "").ToString(), Convert.ToInt64(getDefault(M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"], 0)), getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"], "").ToString(), getDefault(M_DataSet.Tables[0].Rows[0]["VOYAGE"], "").ToString(), objWK.MyCommand, Convert.ToInt64(getDefault(M_DataSet.Tables[0].Rows[0]["PORT_MST_POL_FK"], 0)), Convert.ToString(M_DataSet.Tables[0].Rows[0]["PORT_MST_POD_FK"]), DateTime.MinValue, Convert.ToDateTime(getDefault(Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["ETD_DATE"], DateTime.MinValue)), null)),
                DateTime.MinValue, Convert.ToDateTime(getDefault(Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["ETA_DATE"], DateTime.MinValue)), null)), Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["departure_date"], null)), Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["arrival_date"], null)));

                M_DataSet.Tables[0].Rows[0]["VOYAGEPK"] = strVoyagepk;
                if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                {
                    TRAN1.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Clear();
                }
            }

            Int32 nRowCnt = default(Int32);
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            OracleCommand insContainerDetails = new OracleCommand();
            OracleCommand updContainerDetails = new OracleCommand();

            OracleCommand insTPDetails = new OracleCommand();
            OracleCommand updTPDetails = new OracleCommand();
            OracleCommand delTPDetails = new OracleCommand();

            OracleCommand insPickUpDetails = new OracleCommand();
            OracleCommand updPickUpDetails = new OracleCommand();

            OracleCommand insDropDetails = new OracleCommand();
            OracleCommand updDropDetails = new OracleCommand();

            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();
            OracleCommand delFreightDetails = new OracleCommand();

            OracleCommand insPurchaseInvDetails = new OracleCommand();
            OracleCommand updPurchaseInvDetails = new OracleCommand();
            OracleCommand delPurchaseInvDetails = new OracleCommand();

            OracleCommand insCostDetails = new OracleCommand();
            //'Added By Koteshwari
            OracleCommand updCostDetails = new OracleCommand();
            OracleCommand delCostDetails = new OracleCommand();

            OracleCommand insOtherChargesDetails = new OracleCommand();
            OracleCommand updOtherChargesDetails = new OracleCommand();
            OracleCommand delOtherChargesDetails = new OracleCommand();

            OracleCommand insIncomeChargeDetails = new OracleCommand();
            OracleCommand updIncomeChargeDetails = new OracleCommand();
            OracleCommand delIncomeChargeDetails = new OracleCommand();

            OracleCommand insExpenseChargeDetails = new OracleCommand();
            OracleCommand updExpenseChargeDetails = new OracleCommand();
            OracleCommand delExpenseChargeDetails = new OracleCommand();

            OracleCommand updESIContainerDetails = new OracleCommand();
            OracleCommand updESICommand = new OracleCommand();

            DataSet dsTrackNTrace = new DataSet();
            int Int_I = 0;
            int Int_J = 0;
            DataSet dsESI = new DataSet();
            DataSet dsESIContainerData = new DataSet();
            dsESI = M_DataSet.Copy();
            dsESI.AcceptChanges();
            dsESIContainerData = dsContainerData.Copy();
            dsESIContainerData.AcceptChanges();

            for (Int_I = 0; Int_I <= M_DataSet.Tables[0].Rows.Count - 1; Int_I++)
            {
                for (Int_J = 0; Int_J <= M_DataSet.Tables[0].Columns.Count - 1; Int_J++)
                {
                    dsESI.Tables[0].Rows[Int_I][Int_J] = M_DataSet.Tables[0].Rows[Int_I][Int_J];
                }
            }

            for (Int_I = 0; Int_I <= dsContainerData.Tables[0].Rows.Count - 1; Int_I++)
            {
                for (Int_J = 0; Int_J <= dsContainerData.Tables[0].Columns.Count - 1; Int_J++)
                {
                    dsESIContainerData.Tables[0].Rows[Int_I][Int_J] = dsContainerData.Tables[0].Rows[Int_I][Int_J];
                }
            }
            //If isEdting = False Then
            //    jobCardRefNumber = GenerateProtocolKey("JOB CARD EXP (SEA)", userLocation, employeeID, DateTime.Now, , , , M_LAST_MODIFIED_BY_FK)
            //End If

            ucrNo = ucrNo + jobCardRefNumber;

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;

                dsTrackNTrace = dsContainerData.Copy();
                var _with14 = insCommand;
                _with14.Connection = objWK.MyConnection;
                _with14.CommandType = CommandType.StoredProcedure;
                _with14.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_CARD_SEA_EXP_TBL_INS";
                var _with15 = _with14.Parameters;

                insCommand.Parameters.Add("BOOKING_SEA_FK_IN", OracleDbType.Int32, 10, "booking_sea_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["BOOKING_SEA_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("UCR_NO_IN", ucrNo).Direction = ParameterDirection.Input;
                //insCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current

                insCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "job_card_status").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_XML_STATUS_IN", OracleDbType.Int32, 1, "WIN_XML_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_XML_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "remarks").Direction = ParameterDirection.Input;
                insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                insCommand.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("VOYAGE_FK_IN", OracleClient.OracleDbType.Varchar2, 10, "VoyagePK").Direction = ParameterDirection.Input
                insCommand.Parameters.Add("VOYAGE_FK_IN", getDefault(strVoyagepk, "")).Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("ETA_DATE_IN", OracleClient.OracleDbType.Date, 25, "eta_date").Direction = ParameterDirection.Input
                //insCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETA_DATE"].ToString()))
                {
                    insCommand.Parameters.Add("ETA_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ETA_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;
                //Added By Sivachandran on 12-08-2009

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Date"].ToString()))
                {
                    insCommand.Parameters.Add("SURVEY_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("SURVEY_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["Survey_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["SURVEY_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"].ToString()))
                {
                    insCommand.Parameters.Add("SI_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("SI_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["SI_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["RFS_Date"].ToString()))
                {
                    insCommand.Parameters.Add("RFS_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("RFS_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["RFS_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["RFS_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["STF_Date"].ToString()))
                {
                    insCommand.Parameters.Add("STF_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("STF_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["STF_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["STF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SURVEY_REF_NR_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Ref_Nr"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["Survey_Ref_Nr"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("SURVEY_REMARKS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Remarks"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["Survey_Remarks"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("SURVEYOR_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["Survey_PK"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("SI_IN", OracleDbType.Int32, 1, "SHIPPING_INST_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RFS_IN", OracleDbType.Int32, 1, "RFS").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("STF_IN", OracleDbType.Int32, 1, "STF").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("SURVEY_COMPLETED_IN", OracleDbType.Int32, 1, "SURVEY_COMPLETED").Direction = ParameterDirection.Input;

                //end

                //insCommand.Parameters.Add("ETD_DATE_IN", OracleClient.OracleDbType.Date, 25, "etd_date").Direction = ParameterDirection.Input
                //insCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETD_DATE"].ToString()))
                {
                    insCommand.Parameters.Add("ETD_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ETD_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleClient.OracleDbType.Date, 25, "arrival_date").Direction = ParameterDirection.Input
                //insCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["arrival_date"].ToString()))
                {
                    insCommand.Parameters.Add("ARRIVAL_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ARRIVAL_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["arrival_date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleClient.OracleDbType.Date, 25, "departure_date").Direction = ParameterDirection.Input
                //insCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["departure_date"].ToString()))
                {
                    insCommand.Parameters.Add("DEPARTURE_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("DEPARTURE_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["departure_date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SEC_VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "sec_vessel_name").Direction = ParameterDirection.Input;
                insCommand.Parameters["SEC_VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SEC_VOYAGE_IN", OracleDbType.Varchar2, 10, "sec_voyage").Direction = ParameterDirection.Input;
                insCommand.Parameters["SEC_VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SEC_ETA_DATE_IN", OracleDbType.Date, 20, "sec_eta_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["SEC_ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SEC_ETD_DATE_IN", OracleDbType.Date, 20, "sec_etd_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["SEC_ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "shipper_cust_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["SHIPPER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", OracleDbType.Int32, 10, "consignee_cust_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CONSIGNEE_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify1_cust_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["NOTIFY1_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("NOTIFY2_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify2_cust_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["NOTIFY2_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CB_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cb_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DP_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "dp_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cl_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 10, "cargo_move_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PYMT_TYPE_IN", OracleDbType.Int32, 1, "pymt_type").Direction = ParameterDirection.Input;
                insCommand.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", OracleDbType.Int32, 10, "shipping_terms_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("INSURANCE_AMT_IN", OracleDbType.Int32, 10, "insurance_amt").Direction = ParameterDirection.Input;
                insCommand.Parameters["INSURANCE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("INSURANCE_CURRENCY_IN", OracleDbType.Int32, 10, "insurance_currency").Direction = ParameterDirection.Input;
                insCommand.Parameters["INSURANCE_CURRENCY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TRANSPORTER_DEPOT_FK_IN", OracleDbType.Int32, 10, "depot_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANSPORTER_DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TRANSPORTER_CARRIER_FK_IN", OracleDbType.Int32, 10, "carrier_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANSPORTER_CARRIER_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COUNTRY_ORIGIN_FK_IN", OracleDbType.Int32, 10, "country_mst_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["COUNTRY_ORIGIN_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DA_NUMBER_IN", OracleDbType.Int32, 10, "da_number").Direction = ParameterDirection.Input;
                insCommand.Parameters["DA_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200, "del_address").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                //Added By Rijesh To Incorporate Cargo Details On March -30 2006
                //*******************
                insCommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "GOODS_DESCRIPTION").Direction = ParameterDirection.Input;
                insCommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
                insCommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;
                //***********************

                insCommand.Parameters.Add("master_jc_sea_exp_fk_in", OracleDbType.Int32, 10, "MASTER_JC_SEA_EXP_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["master_jc_sea_exp_fk_in"].SourceVersion = DataRowVersion.Current;

                //Code Added By Anil on 17 Aug 09
                insCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                insCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("STUFF_LOC_IN", OracleDbType.Varchar2, 40, "stuff_loc").Direction = ParameterDirection.Input;
                insCommand.Parameters["STUFF_LOC_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

                //nomination parameters
                insCommand.Parameters.Add("CHK_NOMINATED_IN", OracleDbType.Int32, 1, "CHK_NOMINATED").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_NOMINATED_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHK_CSR_IN", OracleDbType.Int32, 1, "CHK_CSR").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_CSR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", OracleDbType.Int32, 10, "SALES_EXEC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["EXECUTIVE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //-----------------------------------------------------------------------------------

                insCommand.Parameters.Add("CC_REQ_IN", OracleDbType.Int32, 1, "cc_req").Direction = ParameterDirection.Input;
                insCommand.Parameters["CC_REQ_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CC_IE_IN", OracleDbType.Int32, 1, "cc_ie").Direction = ParameterDirection.Input;
                insCommand.Parameters["CC_IE_IN"].SourceVersion = DataRowVersion.Current;

                //Raghavenra added
                insCommand.Parameters.Add("PRC_FK_IN", OracleDbType.Int32, 1, "PRC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PRC_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ONC_FK_IN", OracleDbType.Int32, 1, "ONC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["ONC_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PRC_MODE_FK_IN", OracleDbType.Int32, 1, "PRC_MODE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PRC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ONC_MODE_FK_IN", OracleDbType.Int32, 1, " ONC_MODE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["ONC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;
                //End
                //Manjunath for Total, Received and Balance Quantity and Weight
                insCommand.Parameters.Add("WIN_TOTAL_QTY_IN", OracleDbType.Int32, 10, "WIN_TOTAL_QTY").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_TOTAL_QTY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_REC_QTY_IN", OracleDbType.Int32, 10, "WIN_REC_QTY").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_REC_QTY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_BALANCE_QTY_IN", OracleDbType.Int32, 10, "WIN_BALANCE_QTY").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_BALANCE_QTY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_TOTAL_WT_IN", OracleDbType.Int32, 10, "WIN_TOTAL_WT").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_TOTAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_REC_WT_IN", OracleDbType.Int32, 10, "WIN_REC_WT").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_REC_WT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_BALANCE_WT_IN", OracleDbType.Int32, 10, "WIN_BALANCE_WT").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_BALANCE_WT_IN"].SourceVersion = DataRowVersion.Current;
                //End
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_TRN_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with16 = updCommand;
                _with16.Connection = objWK.MyConnection;
                _with16.CommandType = CommandType.StoredProcedure;
                _with16.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_CARD_SEA_EXP_TBL_UPD";
                var _with17 = _with16.Parameters;

                updCommand.Parameters.Add("JOB_CARD_SEA_EXP_PK_IN", OracleDbType.Int32, 10, "JOB_CARD_TRN_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_SEA_EXP_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BOOKING_SEA_FK_IN", OracleDbType.Int32, 10, "booking_sea_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["BOOKING_SEA_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "ucr_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "job_card_status").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_XML_STATUS_IN", OracleDbType.Int32, 1, "WIN_XML_STATUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_XML_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "remarks").Direction = ParameterDirection.Input;
                updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                updCommand.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                updCommand.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                //updCommand.Parameters.Add("VOYAGE_FK_IN", OracleClient.OracleDbType.Varchar2, 10, "VoyagePK").Direction = ParameterDirection.Input
                updCommand.Parameters.Add("VOYAGE_FK_IN", getDefault(strVoyagepk, "")).Direction = ParameterDirection.Input;
                updCommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleDbType.Date, 20, "arrival_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleDbType.Date, 20, "departure_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SEC_VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "sec_vessel_name").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEC_VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SEC_VOYAGE_IN", OracleDbType.Varchar2, 10, "sec_voyage").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEC_VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SEC_ETA_DATE_IN", OracleDbType.Date, 20, "sec_eta_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEC_ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SEC_ETD_DATE_IN", OracleDbType.Date, 20, "sec_etd_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEC_ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "shipper_cust_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["SHIPPER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", OracleDbType.Int32, 10, "consignee_cust_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONSIGNEE_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify1_cust_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["NOTIFY1_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NOTIFY2_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify2_cust_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["NOTIFY2_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CB_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cb_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DP_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "dp_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cl_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "version_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 10, "cargo_move_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PYMT_TYPE_IN", OracleDbType.Int32, 1, "pymt_type").Direction = ParameterDirection.Input;
                updCommand.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", OracleDbType.Int32, 10, "shipping_terms_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("INSURANCE_AMT_IN", OracleDbType.Int32, 10, "insurance_amt").Direction = ParameterDirection.Input;
                updCommand.Parameters["INSURANCE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("INSURANCE_CURRENCY_IN", OracleDbType.Int32, 10, "insurance_currency").Direction = ParameterDirection.Input;
                updCommand.Parameters["INSURANCE_CURRENCY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TRANSPORTER_DEPOT_FK_IN", OracleDbType.Int32, 10, "depot_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSPORTER_DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TRANSPORTER_CARRIER_FK_IN", OracleDbType.Int32, 10, "carrier_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSPORTER_CARRIER_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COUNTRY_ORIGIN_FK_IN", OracleDbType.Int32, 10, "country_mst_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["COUNTRY_ORIGIN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DA_NUMBER_IN", OracleDbType.Int32, 10, "da_number").Direction = ParameterDirection.Input;
                updCommand.Parameters["DA_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200, "del_address").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                //<<<<<<<<<<<-----------------Jagadeesh on 15-Dec-06-----------------
                //updCommand.Parameters.Add("COL_PLACE_MST_FK_IN", OracleClient.OracleDbType.Int32, 10, hdColPlace).Direction = ParameterDirection.Input
                //updCommand.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current

                //updCommand.Parameters.Add("DEL_PLACE_MST_FK_IN", OracleClient.OracleDbType.Int32, 10, hdDelPlace).Direction = ParameterDirection.Input
                //updCommand.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                //<<<<<<<<<<<--------------------------------------------------------
                //Added By Rijesh To Incorporate Cargo Details  on March 30  2006
                //****************************************************************
                updCommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "GOODS_DESCRIPTION").Direction = ParameterDirection.Input;
                updCommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
                updCommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;
                //*****************************************************************

                updCommand.Parameters.Add("MASTER_JC_SEA_EXP_FK_IN", OracleDbType.Int32, 10, "MASTER_JC_SEA_EXP_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MASTER_JC_SEA_EXP_FK_IN"].SourceVersion = DataRowVersion.Current;

                //Added By Sivachandran on 12-08-2009

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Date"].ToString()))
                {
                    updCommand.Parameters.Add("SURVEY_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("SURVEY_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["Survey_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["SURVEY_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"].ToString()))
                {
                    updCommand.Parameters.Add("SI_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("SI_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["SI_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["RFS_Date"].ToString()))
                {
                    updCommand.Parameters.Add("RFS_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("RFS_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["RFS_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["RFS_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["STF_Date"].ToString()))
                {
                    updCommand.Parameters.Add("STF_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("STF_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["STF_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["STF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SURVEY_REF_NR_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Ref_Nr"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["Survey_Ref_Nr"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SURVEY_REMARKS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Remarks"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["Survey_Remarks"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SURVEYOR_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["Survey_PK"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SI_IN", OracleDbType.Int32, 1, "SHIPPING_INST_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RFS_IN", OracleDbType.Int32, 1, "RFS").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("STF_IN", OracleDbType.Int32, 1, "STF").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SURVEY_COMPLETED_IN", OracleDbType.Int32, 1, "SURVEY_COMPLETED").Direction = ParameterDirection.Input;

                //end

                //Code Added By Anil on 17 Aug 09
                updCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("STUFF_LOC_IN", OracleDbType.Varchar2, 40, "stuff_loc").Direction = ParameterDirection.Input;
                updCommand.Parameters["STUFF_LOC_IN"].SourceVersion = DataRowVersion.Current;
                //End By Anil
                //Added by Faheem
                //updCommand.Parameters.Add("ADDVATOS_FLAG_IN", OracleClient.OracleDbType.Int32, 1, AddVATOSFLAG).Direction = ParameterDirection.Input
                //updCommand.Parameters["ADDVATOS_FLAG_IN"].SourceVersion = DataRowVersion.Current
                updCommand.Parameters.Add("ADDVATOS_FLAG_IN", AddVATOSFLAG).Direction = ParameterDirection.Input;
                //End

                updCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

                //nomination parameters
                updCommand.Parameters.Add("CHK_NOMINATED_IN", OracleDbType.Int32, 1, "CHK_NOMINATED").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHK_NOMINATED_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHK_CSR_IN", OracleDbType.Int32, 1, "CHK_CSR").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHK_CSR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", OracleDbType.Int32, 10, "SALES_EXEC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["EXECUTIVE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //------------------------------------------------------------------------------------

                updCommand.Parameters.Add("CC_REQ_IN", OracleDbType.Int32, 1, "cc_req").Direction = ParameterDirection.Input;
                updCommand.Parameters["CC_REQ_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CC_IE_IN", OracleDbType.Int32, 1, "cc_ie").Direction = ParameterDirection.Input;
                updCommand.Parameters["CC_IE_IN"].SourceVersion = DataRowVersion.Current;

                //Raghavendra

                updCommand.Parameters.Add("PRC_FK_IN", OracleDbType.Int32, 1, "PRC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PRC_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ONC_FK_IN", OracleDbType.Int32, 1, "ONC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["ONC_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PRC_MODE_FK_IN", OracleDbType.Int32, 1, "PRC_MODE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PRC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ONC_MODE_FK_IN", OracleDbType.Int32, 1, "ONC_MODE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["ONC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                //Manjunath for Total, Received and Balance Quantity and Weight
                updCommand.Parameters.Add("WIN_TOTAL_QTY_IN", OracleDbType.Int32, 10, "WIN_TOTAL_QTY").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_TOTAL_QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_REC_QTY_IN", OracleDbType.Int32, 10, "WIN_REC_QTY").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_REC_QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_BALANCE_QTY_IN", OracleDbType.Int32, 10, "WIN_BALANCE_QTY").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_BALANCE_QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_TOTAL_WT_IN", OracleDbType.Int32, 10, "WIN_TOTAL_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_TOTAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_REC_WT_IN", OracleDbType.Int32, 10, "WIN_REC_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_REC_WT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_BALANCE_WT_IN", OracleDbType.Int32, 10, "WIN_BALANCE_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_BALANCE_WT_IN"].SourceVersion = DataRowVersion.Current;
                //End
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                //End

                var _with18 = objWK.MyDataAdapter;

                _with18.InsertCommand = insCommand;
                _with18.InsertCommand.Transaction = TRAN;

                _with18.UpdateCommand = updCommand;
                _with18.UpdateCommand.Transaction = TRAN;

                RecAfct = _with18.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    if (isEdting == false)
                    {
                        JobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    }
                }

                //Manjunath for E-comm
                if (CheckESI == 1)
                {
                    //dsESI = M_DataSet.Copy()
                    var _with19 = updESICommand;
                    _with19.Connection = objWK.MyConnection;
                    _with19.CommandType = CommandType.StoredProcedure;
                    _with19.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_CARD_ESI_SEA_EXP_TBL_UPD";
                    var _with20 = _with19.Parameters;

                    updESICommand.Parameters.Add("JOB_CARD_SEA_EXP_PK_IN", OracleDbType.Int32, 10, "JOB_CARD_TRN_PK").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["JOB_CARD_SEA_EXP_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("BOOKING_SEA_FK_IN", OracleDbType.Int32, 10, "booking_sea_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["BOOKING_SEA_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "ucr_no").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

                    updESICommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "job_card_status").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "remarks").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("VOYAGE_FK_IN", getDefault(strVoyagepk, "")).Direction = ParameterDirection.Input;
                    updESICommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("ARRIVAL_DATE_IN", OracleDbType.Date, 20, "arrival_date").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("DEPARTURE_DATE_IN", OracleDbType.Date, 20, "departure_date").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("SEC_VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "sec_vessel_name").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["SEC_VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("SEC_VOYAGE_IN", OracleDbType.Varchar2, 10, "sec_voyage").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["SEC_VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("SEC_ETA_DATE_IN", OracleDbType.Date, 20, "sec_eta_date").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["SEC_ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("SEC_ETD_DATE_IN", OracleDbType.Date, 20, "sec_etd_date").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["SEC_ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "shipper_cust_mst_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["SHIPPER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", OracleDbType.Int32, 10, "consignee_cust_mst_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["CONSIGNEE_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify1_cust_mst_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["NOTIFY1_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("NOTIFY2_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify2_cust_mst_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["NOTIFY2_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("CB_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cb_agent_mst_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("DP_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "dp_agent_mst_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("CL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cl_agent_mst_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "version_no").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 10, "cargo_move_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("PYMT_TYPE_IN", OracleDbType.Int32, 1, "pymt_type").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", OracleDbType.Int32, 10, "shipping_terms_mst_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("INSURANCE_AMT_IN", OracleDbType.Int32, 10, "insurance_amt").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["INSURANCE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("INSURANCE_CURRENCY_IN", OracleDbType.Int32, 10, "insurance_currency").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["INSURANCE_CURRENCY_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("TRANSPORTER_DEPOT_FK_IN", OracleDbType.Int32, 10, "depot_pk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["TRANSPORTER_DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("TRANSPORTER_CARRIER_FK_IN", OracleDbType.Int32, 10, "carrier_pk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["TRANSPORTER_CARRIER_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("COUNTRY_ORIGIN_FK_IN", OracleDbType.Int32, 10, "country_mst_pk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["COUNTRY_ORIGIN_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("DA_NUMBER_IN", OracleDbType.Int32, 10, "da_number").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["DA_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                    updESICommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                    updESICommand.Parameters.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200, "del_address").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "GOODS_DESCRIPTION").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;
                    //*****************************************************************

                    updESICommand.Parameters.Add("MASTER_JC_SEA_EXP_FK_IN", OracleDbType.Int32, 10, "MASTER_JC_SEA_EXP_FK").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["MASTER_JC_SEA_EXP_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsESI.Tables[0].Rows[0]["Survey_Date"].ToString()))
                    {
                        updESICommand.Parameters.Add("SURVEY_DATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updESICommand.Parameters.Add("SURVEY_DATE_IN", Convert.ToDateTime(dsESI.Tables[0].Rows[0]["Survey_Date"])).Direction = ParameterDirection.Input;
                    }
                    updESICommand.Parameters["SURVEY_DATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsESI.Tables[0].Rows[0]["RFS_Date"].ToString()))
                    {
                        updESICommand.Parameters.Add("RFS_DATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updESICommand.Parameters.Add("RFS_DATE_IN", Convert.ToDateTime(dsESI.Tables[0].Rows[0]["RFS_Date"])).Direction = ParameterDirection.Input;
                    }
                    updESICommand.Parameters["RFS_DATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsESI.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                    {
                        updESICommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updESICommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(dsESI.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                    }
                    updESICommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsESI.Tables[0].Rows[0]["STF_Date"].ToString()))
                    {
                        updESICommand.Parameters.Add("STF_DATE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updESICommand.Parameters.Add("STF_DATE_IN", Convert.ToDateTime(dsESI.Tables[0].Rows[0]["STF_Date"])).Direction = ParameterDirection.Input;
                    }
                    updESICommand.Parameters["STF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("SURVEY_REF_NR_IN", (string.IsNullOrEmpty(dsESI.Tables[0].Rows[0]["Survey_Ref_Nr"].ToString()) ? "" : dsESI.Tables[0].Rows[0]["Survey_Ref_Nr"])).Direction = ParameterDirection.Input;
                    updESICommand.Parameters.Add("SURVEY_REMARKS_IN", (string.IsNullOrEmpty(dsESI.Tables[0].Rows[0]["Survey_Remarks"].ToString()) ? "" : dsESI.Tables[0].Rows[0]["Survey_Remarks"])).Direction = ParameterDirection.Input;
                    updESICommand.Parameters.Add("SURVEYOR_FK_IN", (string.IsNullOrEmpty(dsESI.Tables[0].Rows[0]["Survey_PK"].ToString()) ? "" : dsESI.Tables[0].Rows[0]["Survey_PK"])).Direction = ParameterDirection.Input;
                    updESICommand.Parameters.Add("RFS_IN", OracleDbType.Int32, 1, "RFS").Direction = ParameterDirection.Input;
                    updESICommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;
                    updESICommand.Parameters.Add("STF_IN", OracleDbType.Int32, 1, "STF").Direction = ParameterDirection.Input;
                    updESICommand.Parameters.Add("SURVEY_COMPLETED_IN", OracleDbType.Int32, 1, "SURVEY_COMPLETED").Direction = ParameterDirection.Input;

                    //end

                    //Code Added By Anil on 17 Aug 09
                    updESICommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("STUFF_LOC_IN", OracleDbType.Varchar2, 40, "stuff_loc").Direction = ParameterDirection.Input;
                    updESICommand.Parameters["STUFF_LOC_IN"].SourceVersion = DataRowVersion.Current;

                    updESICommand.Parameters.Add("ADDVATOS_FLAG_IN", AddVATOSFLAG).Direction = ParameterDirection.Input;

                    updESICommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updESICommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with21 = objWK.MyDataAdapter;

                    _with21.UpdateCommand = updESICommand;
                    _with21.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with21.Update(dsESI);

                    if (arrMessage.Count == 0)
                    {
                        //goto 21;
                    }

                    if (string.Compare(Convert.ToString(arrMessage[0]).ToLower(), "saved") > 0)
                    {
                        arrMessage.Clear();
                    }
                    //21:

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                    // End
                }

                var _with22 = insContainerDetails;
                _with22.Connection = objWK.MyConnection;
                _with22.CommandType = CommandType.StoredProcedure;
                _with22.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_CONT_INS";
                var _with23 = _with22.Parameters;

                insContainerDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //Added By Prakash chandra on 5/1/2009 for pts: multiple commodity selection
                insContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                //Snigdharani - 21/08/2009
                insContainerDetails.Parameters.Add("CONTAINER_PK_IN", OracleDbType.Int32, 20, "CONTAINER_PK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_PK_IN"].SourceVersion = DataRowVersion.Current;
                //insContainerDetails.Parameters.Add("LOAD_DATE_IN", loaddate).Direction = ParameterDirection.Input

                insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_CONT_PK").Direction = ParameterDirection.Output;
                insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with24 = updContainerDetails;
                _with24.Connection = objWK.MyConnection;
                _with24.CommandType = CommandType.StoredProcedure;
                _with24.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_CONT_UPD";
                var _with25 = _with24.Parameters;

                updContainerDetails.Parameters.Add("JOB_TRN_SEA_EXP_CONT_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_CONT_PK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["JOB_TRN_SEA_EXP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //Added By prakash chandra on 6/1/2009 for pts:Docs - Job Card – Provision to capture Multiple Commodities
                updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;
                //Snigdharani - 21/08/2009
                updContainerDetails.Parameters.Add("CONTAINER_PK_IN", OracleDbType.Int32, 20, "CONTAINER_PK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_PK_IN"].SourceVersion = DataRowVersion.Current;

                //updContainerDetails.Parameters.Add("LOAD_DATE_IN", loaddate).Direction = ParameterDirection.Input

                updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with26 = objWK.MyDataAdapter;

                _with26.InsertCommand = insContainerDetails;
                _with26.InsertCommand.Transaction = TRAN;

                _with26.UpdateCommand = updContainerDetails;
                _with26.UpdateCommand.Transaction = TRAN;
                RecAfct = _with26.Update(dsContainerData.Tables[0]);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                    //Else
                    //    TRAN.Commit()
                }
                objWK.MyCommand.Transaction = TRAN;
                // Amit 22-Dec-06 TaskID "DOC-DEC-001"
                int rowCnt = 0;
                if (dsContainerData.Tables[0].Rows.Count > 0)
                {
                    try
                    {
                        for (rowCnt = 0; rowCnt <= dsContainerData.Tables[0].Rows.Count - 1; rowCnt++)
                        {
                            string CntType = null;
                            CntType = Convert.ToString(((System.Data.DataRow)dsContainerData.Tables[0].Rows[rowCnt]).ItemArray[3]);
                            string strSql = null;
                            string drCntKind = null;
                            strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE UPPER(C.CONTAINER_TYPE_MST_ID)= '" + CntType.ToUpper() + "'";

                            //drCntKind = objWK.ExecuteScaler(strSql)
                            //objWK.OpenConnection()
                            var _with27 = objWK.MyCommand;
                            _with27.Parameters.Clear();
                            _with27.CommandType = CommandType.Text;
                            _with27.CommandText = strSql;
                            drCntKind = Convert.ToString(_with27.ExecuteScalar());
                            objWK.MyCommand.Parameters.Clear();
                            //objWK.MyCommand.Connection = objWK.MyConnection
                            //TRAN = objWK.MyConnection.BeginTransaction()
                            //objWK.MyCommand.Transaction = TRAN
                            if (CommodityGroup == HAZARDOUS)
                            {
                                if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                                {
                                    arrMessage = SaveTransactionODC(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                                }
                                else
                                {
                                    arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                                }
                            }
                            else if (CommodityGroup == REEFER)
                            {
                                if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                                {
                                    arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                                }
                                else
                                {
                                    arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                                }
                            }
                            else if (CommodityGroup == ODC)
                            {
                                if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                                {
                                    arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                                }
                            }
                            else
                            {
                                if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
                                {
                                    arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                }
                // End

                //Manjunath for E-comm
                if (CheckESI == 1)
                {
                    var _with28 = updESIContainerDetails;
                    _with28.Connection = objWK.MyConnection;
                    _with28.CommandType = CommandType.StoredProcedure;
                    _with28.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_ESI_SEA_EXP_CONT_UPD";
                    var _with29 = _with28.Parameters;

                    updESIContainerDetails.Parameters.Add("JOB_TRN_SEA_EXP_CONT_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_CONT_PK").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["JOB_TRN_SEA_EXP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    updESIContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("CONTAINER_PK_IN", OracleDbType.Int32, 20, "CONTAINER_PK").Direction = ParameterDirection.Input;
                    updESIContainerDetails.Parameters["CONTAINER_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updESIContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updESIContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with30 = objWK.MyDataAdapter;

                    _with30.UpdateCommand = updESIContainerDetails;
                    _with30.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with30.Update(dsESIContainerData.Tables[0]);
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                    objWK.MyCommand.Transaction = TRAN;
                }

                var _with31 = insTPDetails;
                _with31.Connection = objWK.MyConnection;
                _with31.CommandType = CommandType.StoredProcedure;
                _with31.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_TP_INS";
                var _with32 = _with31.Parameters;

                insTPDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insTPDetails.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 10, "port_mst_fk").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["PORT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("TRANSHIPMENT_NO_IN", OracleDbType.Int32, 10, "transhipment_no").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["TRANSHIPMENT_NO_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("VOYAGE_FK_IN", OracleDbType.Varchar2, 10, "GridVoyagePK").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("AGENT_FK_IN", OracleDbType.Varchar2, 10, "AgentPK").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["AGENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_TP_PK").Direction = ParameterDirection.Output;
                insTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with33 = updTPDetails;
                _with33.Connection = objWK.MyConnection;
                _with33.CommandType = CommandType.StoredProcedure;
                _with33.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_TP_UPD";
                var _with34 = _with33.Parameters;

                updTPDetails.Parameters.Add("JOB_TRN_SEA_EXP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_tp_pk").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["JOB_TRN_SEA_EXP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updTPDetails.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 10, "port_mst_fk").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["PORT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("TRANSHIPMENT_NO_IN", OracleDbType.Int32, 10, "transhipment_no").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["TRANSHIPMENT_NO_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("VOYAGE_FK_IN", OracleDbType.Varchar2, 10, "GridVoyagePK").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("AGENT_FK_IN", OracleDbType.Varchar2, 10, "AgentPK").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["AGENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with35 = delTPDetails;
                _with35.Connection = objWK.MyConnection;
                _with35.CommandType = CommandType.StoredProcedure;
                _with35.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_TP_DEL";

                delTPDetails.Parameters.Add("JOB_TRN_SEA_EXP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_tp_pk").Direction = ParameterDirection.Input;
                delTPDetails.Parameters["JOB_TRN_SEA_EXP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                delTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with36 = objWK.MyDataAdapter;

                _with36.InsertCommand = insTPDetails;
                _with36.InsertCommand.Transaction = TRAN;

                _with36.UpdateCommand = updTPDetails;
                _with36.UpdateCommand.Transaction = TRAN;

                _with36.DeleteCommand = delTPDetails;
                _with36.DeleteCommand.Transaction = TRAN;
                RecAfct = _with36.Update(dsTPDetails.Tables[0]);
                // Amit 22-Dec-06 TaskID "DOC-DEC-001"
                if (arrMessage.Count == 0)
                {
                    //goto 20;
                }

                if (string.Compare(Convert.ToString(arrMessage[0]).ToLower(), "saved") > 0)
                {
                    arrMessage.Clear();
                }
                //20:

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                // End

                //Manjunath for Cargo Pick up & Drop Address
                if ((dsPickUpDetails != null))
                {
                    var _with37 = insPickUpDetails;
                    _with37.Connection = objWK.MyConnection;
                    _with37.CommandType = CommandType.StoredProcedure;
                    _with37.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

                    _with37.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with37.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with37.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with37.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with37.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with37.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with37.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with37.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with37.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with37.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with37.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with37.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with37.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with37.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with37.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with37.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with37.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with37.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with37.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with37.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with37.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with37.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with38 = updPickUpDetails;
                    _with38.Connection = objWK.MyConnection;
                    _with38.CommandType = CommandType.StoredProcedure;
                    _with38.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with38.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with38.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with38.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with38.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with38.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with38.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with38.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with38.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with38.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with38.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with38.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with38.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with38.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with38.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with38.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with38.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with38.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with38.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with38.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with38.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with38.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with38.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with39 = objWK.MyDataAdapter;

                    _with39.InsertCommand = insPickUpDetails;
                    _with39.InsertCommand.Transaction = TRAN;

                    _with39.UpdateCommand = updPickUpDetails;
                    _with39.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with39.Update(dsPickUpDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

                if ((dsDropDetails != null))
                {
                    var _with40 = insDropDetails;
                    _with40.Connection = objWK.MyConnection;
                    _with40.CommandType = CommandType.StoredProcedure;
                    _with40.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

                    _with40.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with40.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with40.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with40.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with40.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with40.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with40.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with40.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with40.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with40.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with40.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with40.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with40.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with40.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with40.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with40.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with40.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with40.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with40.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with40.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with40.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with40.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with41 = updDropDetails;
                    _with41.Connection = objWK.MyConnection;
                    _with41.CommandType = CommandType.StoredProcedure;
                    _with41.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with41.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with41.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with41.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with41.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with41.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with41.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with41.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with41.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with41.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with41.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with41.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with41.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with41.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with41.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with41.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with41.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with41.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with41.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with41.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with41.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with41.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with41.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with42 = objWK.MyDataAdapter;

                    _with42.InsertCommand = insDropDetails;
                    _with42.InsertCommand.Transaction = TRAN;

                    _with42.UpdateCommand = updDropDetails;
                    _with42.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with42.Update(dsDropDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                //End Manjunath

                //--------------------------Freight Details-----------------------------
                var _with43 = insFreightDetails;
                _with43.Connection = objWK.MyConnection;
                _with43.CommandType = CommandType.StoredProcedure;
                _with43.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_INS";
                var _with44 = _with43.Parameters;

                insFreightDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                // By Amit Singh on 23-May-07
                insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // End

                insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                //'Rateperbasis
                insFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                //'surcharge
                insFreightDetails.Parameters.Add("surcharge_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["surcharge_IN"].SourceVersion = DataRowVersion.Current;
                ///surcharge

                insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;
                //'added by subhransu
                insFreightDetails.Parameters.Add("job_trn_sea_exp_cont_fk_in", "").Direction = ParameterDirection.Input;

                // Added Suresh Kumar 30.03.2006 - Print Check box for MBL Print
                insFreightDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;
                //end
                insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_FD_PK").Direction = ParameterDirection.Output;
                insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with45 = updFreightDetails;
                _with45.Connection = objWK.MyConnection;
                _with45.CommandType = CommandType.StoredProcedure;
                _with45.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_UPD";
                var _with46 = _with45.Parameters;

                updFreightDetails.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                //'Rateperbasis
                updFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                //'surcharge
                updFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;
                ///surcharge

                // By Amit Singh on 23-May-07
                updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // End

                updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                //' Added Suresh Kumar 30.03.2006 - Print Check box for MBL Print
                updFreightDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;
                //end

                updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with47 = delFreightDetails;
                _with47.Connection = objWK.MyConnection;
                _with47.CommandType = CommandType.StoredProcedure;
                _with47.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_DEL";

                delFreightDetails.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                delFreightDetails.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with48 = objWK.MyDataAdapter;

                _with48.InsertCommand = insFreightDetails;
                _with48.InsertCommand.Transaction = TRAN;

                _with48.UpdateCommand = updFreightDetails;
                _with48.UpdateCommand.Transaction = TRAN;

                _with48.DeleteCommand = delFreightDetails;
                _with48.DeleteCommand.Transaction = TRAN;

                RecAfct = _with48.Update(dsFreightDetails);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                if (!SaveSecondaryServices(objWK, TRAN, Convert.ToInt32(JobCardPK), dsIncomeChargeDetails, dsExpenseChargeDetails))
                {
                    arrMessage.Add("Error while saving secondary service details");
                    return arrMessage;
                }

                var _with49 = insPurchaseInvDetails;
                _with49.Connection = objWK.MyConnection;
                _with49.CommandType = CommandType.StoredProcedure;
                _with49.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_PIA_INS";
                var _with50 = _with49.Parameters;

                insPurchaseInvDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insPurchaseInvDetails.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "cost_element_mst_fk").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "vendor_key").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("INVOICE_NUMBER_IN", OracleDbType.Varchar2, 20, "invoice_number").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["INVOICE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("INVOICE_DATE_IN", OracleDbType.Date, 20, "invoice_date").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("INVOICE_AMT_IN", OracleDbType.Int32, 10, "invoice_amt").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["INVOICE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("TAX_PERCENTAGE_IN", OracleDbType.Int32, 5, "tax_percentage").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["TAX_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 10, "tax_amt").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("ESTIMATED_AMT_IN", OracleDbType.Int32, 10, "estimated_amt").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "vendor_mst_fk").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("ATTACHED_FILE_NAME_IN", OracleDbType.Varchar2, 50, "attached_file_name").Direction = ParameterDirection.Input;
                insPurchaseInvDetails.Parameters["ATTACHED_FILE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_PIA_PK").Direction = ParameterDirection.Output;
                insPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with51 = updPurchaseInvDetails;
                _with51.Connection = objWK.MyConnection;
                _with51.CommandType = CommandType.StoredProcedure;
                _with51.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_PIA_UPD";
                var _with52 = _with51.Parameters;

                updPurchaseInvDetails.Parameters.Add("JOB_TRN_SEA_EXP_PIA_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_pia_pk").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["JOB_TRN_SEA_EXP_PIA_PK_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updPurchaseInvDetails.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "cost_element_mst_fk").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "vendor_key").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("INVOICE_NUMBER_IN", OracleDbType.Varchar2, 20, "invoice_number").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["INVOICE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("INVOICE_DATE_IN", OracleDbType.Date, 20, "invoice_date").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("INVOICE_AMT_IN", OracleDbType.Int32, 10, "invoice_amt").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["INVOICE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("TAX_PERCENTAGE_IN", OracleDbType.Int32, 5, "tax_percentage").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["TAX_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 10, "tax_amt").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("ESTIMATED_AMT_IN", OracleDbType.Int32, 10, "estimated_amt").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "vendor_mst_fk").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("ATTACHED_FILE_NAME_IN", OracleDbType.Varchar2, 50, "attached_file_name").Direction = ParameterDirection.Input;
                updPurchaseInvDetails.Parameters["ATTACHED_FILE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with53 = delPurchaseInvDetails;
                _with53.Connection = objWK.MyConnection;
                _with53.CommandType = CommandType.StoredProcedure;
                _with53.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_PIA_DEL";

                delPurchaseInvDetails.Parameters.Add("JOB_TRN_SEA_EXP_PIA_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_pia_pk").Direction = ParameterDirection.Input;
                delPurchaseInvDetails.Parameters["JOB_TRN_SEA_EXP_PIA_PK_IN"].SourceVersion = DataRowVersion.Current;

                delPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with54 = objWK.MyDataAdapter;

                _with54.InsertCommand = insPurchaseInvDetails;
                _with54.InsertCommand.Transaction = TRAN;

                _with54.UpdateCommand = updPurchaseInvDetails;
                _with54.UpdateCommand.Transaction = TRAN;

                _with54.DeleteCommand = delPurchaseInvDetails;
                _with54.DeleteCommand.Transaction = TRAN;

                RecAfct = _with54.Update(dsPurchaseInventory.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //'Added By Koteshwari on 22/4/2011
                var _with55 = insCostDetails;
                _with55.Connection = objWK.MyConnection;
                _with55.CommandType = CommandType.StoredProcedure;
                _with55.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_INS";
                var _with56 = _with55.Parameters;
                insCostDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                //'surcharge
                insCostDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Output;
                insCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with57 = updCostDetails;
                _with57.Connection = objWK.MyConnection;
                _with57.CommandType = CommandType.StoredProcedure;
                _with57.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_UPD";
                var _with58 = _with57.Parameters;

                updCostDetails.Parameters.Add("JOB_TRN_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["JOB_TRN_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                //'surcharge
                updCostDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with59 = delCostDetails;
                _with59.Connection = objWK.MyConnection;
                _with59.CommandType = CommandType.StoredProcedure;
                _with59.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_DEL";

                delCostDetails.Parameters.Add("JOB_TRN_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Input;
                delCostDetails.Parameters["JOB_TRN_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with60 = objWK.MyDataAdapter;

                _with60.InsertCommand = insCostDetails;
                _with60.InsertCommand.Transaction = TRAN;

                _with60.UpdateCommand = updCostDetails;
                _with60.UpdateCommand.Transaction = TRAN;

                _with60.DeleteCommand = delCostDetails;
                _with60.DeleteCommand.Transaction = TRAN;

                RecAfct = _with60.Update(dsCostDetails.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //'End Koteshwari

                foreach (DataRow _rOth in dsOtherCharges.Tables[0].Rows)
                {
                    string PayType = Convert.ToString(_rOth["Payment_Type"]);
                    if (PayType.ToUpper() == "PREPAID")
                    {
                        _rOth["Payment_Type"] = "1";
                    }
                    else if (PayType.ToUpper() == "COLLECT")
                    {
                        _rOth["Payment_Type"] = "2";
                    }
                }
                var _with61 = insOtherChargesDetails;
                _with61.Connection = objWK.MyConnection;
                _with61.CommandType = CommandType.StoredProcedure;
                _with61.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_INS";

                _with61.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with61.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                _with61.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with61.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                _with61.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with61.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                _with61.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with61.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                _with61.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with61.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                _with61.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with61.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                _with61.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with61.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                _with61.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with61.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                _with61.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                _with61.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_OTH_PK").Direction = ParameterDirection.Output;
                _with61.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with62 = updOtherChargesDetails;
                _with62.Connection = objWK.MyConnection;
                _with62.CommandType = CommandType.StoredProcedure;
                _with62.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_UPD";

                _with62.Parameters.Add("JOB_TRN_SEA_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_oth_pk").Direction = ParameterDirection.Input;
                _with62.Parameters["JOB_TRN_SEA_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with62.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with62.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                _with62.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with62.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                _with62.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with62.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                _with62.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with62.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                _with62.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with62.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                _with62.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with62.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                _with62.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with62.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                _with62.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with62.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                _with62.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                _with62.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with62.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with63 = delOtherChargesDetails;
                _with63.Connection = objWK.MyConnection;
                _with63.CommandType = CommandType.StoredProcedure;
                _with63.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_DEL";

                _with63.Parameters.Add("JOB_TRN_SEA_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_oth_pk").Direction = ParameterDirection.Input;
                _with63.Parameters["JOB_TRN_SEA_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with63.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with63.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with64 = objWK.MyDataAdapter;

                _with64.InsertCommand = insOtherChargesDetails;
                _with64.InsertCommand.Transaction = TRAN;

                _with64.UpdateCommand = updOtherChargesDetails;
                _with64.UpdateCommand.Transaction = TRAN;

                _with64.DeleteCommand = delOtherChargesDetails;
                _with64.DeleteCommand.Transaction = TRAN;

                RecAfct = _with64.Update(dsOtherCharges);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    if (intIsUpdate == 1)
                    {
                        if (!string.IsNullOrEmpty(getDefault(strOperatorPk, "").ToString()))
                        {
                            arrMessage = (ArrayList)funUpStreamUpdationBookingOpr(strBookingRefNo, strOperatorPk, TRAN);

                            if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                            {
                                TRAN.Rollback();
                                return arrMessage;
                            }
                        }
                    }
                    arrMessage = (ArrayList)SaveJobCardDoc(JobCardPK.ToString(), TRAN, dsDoc, 2, 1);
                    //Biztype -2(Sea),Process Type -1(Export)
                    if (arrMessage.Count > 0)
                    {
                        if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                        {
                            TRAN.Rollback();
                            return arrMessage;
                        }
                    }
                    arrMessage = (ArrayList)SaveTrackAndTrace(Convert.ToInt32(JobCardPK), TRAN, M_DataSet, Convert.ToInt32(userLocation), isEdting, dsTrackNTrace);
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Commit();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                    }
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        /// <summary>
        /// Saves the secondary services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool SaveSecondaryServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["JOB_TRN_SEA_EXP_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with65 = objWK.MyCommand;
                    _with65.Parameters.Clear();
                    _with65.Transaction = TRAN;
                    _with65.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with65.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_UPD";
                        _with65.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", ri["JOB_TRN_SEA_EXP_FD_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with65.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_INS";
                        _with65.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                        _with65.Parameters.Add("JOB_TRN_SEA_EXP_CONT_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    _with65.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], "")).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("PRINT_ON_MBL_IN", 1).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("RATE_PERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with65.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            _with65.Parameters.Clear();
                            _with65.CommandType = CommandType.StoredProcedure;
                            _with65.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_FRT_SEQ_CURRVAL";
                            _with65.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
                            _with65.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with65.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with65.ExecuteNonQuery();
                            Frt_Pk = Convert.ToInt32(_with65.Parameters["RETURN_VALUE"].Value);
                            ri["JOB_TRN_SEA_EXP_FD_PK"] = Frt_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            //----------------------------------Expense Charge Details----------------------------------
            if ((dsExpenseChargeDetails != null))
            {
                foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                {
                    int Cost_Pk = 0;
                    try
                    {
                        Cost_Pk = Convert.ToInt32(re["JOB_TRN_SEA_EXP_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with66 = objWK.MyCommand;
                    _with66.Parameters.Clear();
                    _with66.Transaction = TRAN;
                    _with66.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with66.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_UPD";
                        _with66.Parameters.Add("JOB_TRN_EST_PK_IN", re["JOB_TRN_SEA_EXP_COST_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with66.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_INS";
                        _with66.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }

                    _with66.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("COST_ELEMENT_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("LOCATION_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("VENDOR_KEY_IN", re["SUPPLIER_MST_ID"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("PTMT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with66.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with66.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            _with66.Parameters.Clear();
                            _with66.CommandType = CommandType.StoredProcedure;
                            _with66.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_COST_SEQ_CURRVAL";
                            _with66.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
                            _with66.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with66.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with66.ExecuteNonQuery();
                            Cost_Pk = Convert.ToInt32(_with66.Parameters["RETURN_VALUE"].Value);
                            re["JOB_TRN_SEA_EXP_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            ClearRemovedServices(objWK, TRAN, JobCardPK, dsIncomeChargeDetails, dsExpenseChargeDetails);
            return true;
        }

        /// <summary>
        /// Clears the removed services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool ClearRemovedServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            //Dim objwk As New WorkFlow
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (JobCardPK > 0)
            {
                //objwk.OpenConnection()
                //Dim TRAN As OracleTransaction
                //TRAN = objwk.MyConnection.BeginTransaction()
                try
                {
                    foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedFrtPks))
                        {
                            SelectedFrtPks = Convert.ToString(getDefault(ri["JOB_TRN_SEA_EXP_FD_PK"], 0));
                        }
                        else
                        {
                            SelectedFrtPks += "," + getDefault(ri["JOB_TRN_SEA_EXP_FD_PK"], 0);
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = Convert.ToString(getDefault(re["JOB_TRN_SEA_EXP_COST_PK"], 0));
                        }
                        else
                        {
                            SelectedCostPks += "," + getDefault(re["JOB_TRN_SEA_EXP_COST_PK"], 0);
                        }
                    }

                    var _with67 = objWK.MyCommand;
                    _with67.Transaction = TRAN;
                    _with67.CommandType = CommandType.StoredProcedure;
                    _with67.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_SEA_EXP_SEC_CHG_EXCEPT";
                    _with67.Parameters.Clear();
                    _with67.Parameters.Add("JOB_CARD_SEA_EXP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with67.Parameters.Add("JOB_TRN_SEA_EXP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with67.Parameters.Add("JOB_TRN_SEA_EXP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with67.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with67.ExecuteNonQuery();
                    //TRAN.Commit()
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                    //Throw oraexp
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                    //Throw ex
                }
                finally
                {
                    //objwk.CloseConnection()
                }
            }
            return false;
        }

        #endregion "Save Function"

        #region "Fetch Vessel/Voyage Detail"

        /// <summary>
        /// Fetches the voyage detail.
        /// </summary>
        /// <param name="VoyagePk">The voyage pk.</param>
        /// <returns></returns>
        public DataSet FetchVoyageDetail(string VoyagePk)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT");
            strSQL.Append("      VVT.VESSEL_ID,");
            strSQL.Append("      VVT.VESSEL_NAME,");
            strSQL.Append("      VTR.VOYAGE");
            strSQL.Append(" FROM");
            strSQL.Append("      VESSEL_VOYAGE_TBL VVT,");
            strSQL.Append("      VESSEL_VOYAGE_TRN VTR");
            strSQL.Append(" WHERE");
            strSQL.Append("      VVT.VESSEL_VOYAGE_TBL_PK = VTR.VESSEL_VOYAGE_TBL_FK");
            strSQL.Append(" AND  VTR.VOYAGE_TRN_PK=" + VoyagePk);

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

        #endregion "Fetch Vessel/Voyage Detail"

        #region "CustomerID"

        //To Get Customer ID
        //By Amit on 23-May-07
        /// <summary>
        /// Gets the customer identifier.
        /// </summary>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <returns></returns>
        public DataSet GetCustomerID(string CustomerPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("SELECT CMT.CUSTOMER_ID");
                strQuery.Append("   FROM CUSTOMER_MST_TBL CMT");
                strQuery.Append("  WHERE CMT.CUSTOMER_MST_PK= '" + CustomerPK + "'");
                strQuery.Append("");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //End

        #endregion "CustomerID"

        #region " Save Track And Trace "

        /// <summary>
        /// Saves the track and trace.
        /// </summary>
        /// <param name="jobPk">The job pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="nlocationfk">The nlocationfk.</param>
        /// <param name="IsEditing">if set to <c>true</c> [is editing].</param>
        /// <param name="dsContainer">The ds container.</param>
        /// <returns></returns>
        public object SaveTrackAndTrace(int jobPk, OracleTransaction TRAN, DataSet dsMain, int nlocationfk, bool IsEditing, DataSet dsContainer)
        {
            int Cnt = 0;
            int a = 0;
            bool UpdATD = false;
            bool UpdLDdate = false;
            string strContData = null;
            if (!string.IsNullOrEmpty((dsMain.Tables[0].Rows[Cnt]["departure_date"].ToString())))
            {
                UpdATD = true;
            }
            objTrackNTrace.DeleteOnSaveTraceExportOnATDLDUpd(jobPk, 2, 1, "Vessel Voyage", "LD-DT-DATA-DEL-SEA-EXP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
            "Null");

            for (Cnt = 0; Cnt <= dsContainer.Tables[0].Rows.Count - 1; Cnt++)
            {
                if (!string.IsNullOrEmpty((dsContainer.Tables[0].Rows[Cnt]["load_date"].ToString())))
                {
                    UpdLDdate = true;
                    var _with68 = dsContainer.Tables[0].Rows[Cnt];
                    // Updated by Amit on 05-Jan-07 For Task DTS-1833
                    if (string.IsNullOrEmpty((dsMain.Tables[0].Rows[0]["vessel_name"].ToString())))
                    {
                        strContData = "Loaded Container " + _with68["container_number"] + "~" + _with68["load_date"];
                    }
                    else
                    {
                        strContData = "Loaded Container " + _with68["container_number"] + " On " + dsMain.Tables[0].Rows[0]["vessel_name"] + "/" + dsMain.Tables[0].Rows[0]["voyage"] + "~" + _with68["load_date"];
                    }
                    // End
                    arrMessage = objTrackNTrace.SaveTrackAndTraceExportOnLDUpd(jobPk, 2, 1, "Vessel Voyage", "LD-DT-UPD-JOB-SEA-EXP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                    strContData);
                }
            }

            //If UpdLDdate = True Then
            //    arrMessage = objTrackNTrace.SaveTrackAndTraceExportOnATDLDUpd(jobPk, 2, 1, "Vessel Voyage", "LD-DT-UPD-JOB-SEA-EXP", nlocationfk, TRAN, "INS", "O")
            //End If

            if (UpdATD == true & IsEditing == true)
            {
                //Added by Venkata to get status like "sailed from XXX POL"
                for (Cnt = 0; Cnt <= dsMain.Tables[0].Rows.Count - 1; Cnt++)
                {
                    if (!string.IsNullOrEmpty((dsMain.Tables[0].Rows[Cnt]["departure_date"].ToString())))
                    {
                        //UpdATD = True
                        var _with69 = dsMain.Tables[0].Rows[Cnt];
                        strContData = "Sailed from " + _with69["POL"] + "~" + _with69["departure_date"];
                        arrMessage = objTrackNTrace.SaveTrackAndTraceExportOnATDUpd(jobPk, 2, 1, "Sail", "ATD-UPD-JC-SEA-EXP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                        strContData);
                    }
                }
                //end Added
            }
            else
            {
                arrMessage.Clear();
                arrMessage.Add("All Data Saved Successfully");
            }
            return arrMessage;
        }

        #endregion " Save Track And Trace "

        #region "Fetch Main Jobcard for export"

        /// <summary>
        /// Fetches the main job card data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchMainJobCardDataExp(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            strSQL.Append("SELECT ");
            strSQL.Append("    job_exp.JOB_CARD_TRN_PK, ");
            strSQL.Append("    job_exp.booking_sea_fk,  ");
            strSQL.Append("    bst.BOOKING_DATE,  ");
            strSQL.Append("    job_exp.jobcard_ref_no, ");
            strSQL.Append("    bst.booking_ref_no, ");
            strSQL.Append("    bst.cargo_type, cust.customer_id,");
            strSQL.Append("    bst.cust_customer_mst_fk,");
            strSQL.Append("    cust.customer_name, job_exp.del_address,");
            strSQL.Append(" CASE WHEN BST.CARGO_TYPE =1 THEN BST.POO_FK");
            strSQL.Append("  ELSE BST.COL_PLACE_MST_FK END COL_PLACE_MST_FK,  ");
            strSQL.Append("  CASE WHEN BST.CARGO_TYPE =1 THEN POO.PORT_NAME  ");
            strSQL.Append("  ELSE FRMPLC.PLACE_NAME END \"CollectionPlace\" , ");
            strSQL.Append("    bst.port_mst_pol_fk, ");
            strSQL.Append("    pol.port_name \"POL\",");
            strSQL.Append("    bst.port_mst_pod_fk, ");
            strSQL.Append("    pod.port_name \"POD\",");
            //strSQL.Append(vbCrLf & "    bst.del_place_mst_fk, ")
            //strSQL.Append(vbCrLf & "    del_place.place_name ""DeliveryPlace"",")
            strSQL.Append(" CASE WHEN BST.CARGO_TYPE =1 THEN BST.PFD_FK");
            strSQL.Append("  ELSE BST.PFD_FK END DEL_PLACE_MST_FK,  ");
            strSQL.Append("   CASE WHEN BST.CARGO_TYPE =1 THEN PFD.PORT_NAME  ");
            strSQL.Append("  ELSE TOPLC.PLACE_NAME END \"DeliveryPlace\", ");

            strSQL.Append("    job_card_status,     ");
            strSQL.Append("    job_card_closed_on,  ");
            strSQL.Append("    job_exp.WIN_XML_STATUS,");
            strSQL.Append("    job_exp.WIN_TOTAL_QTY,");
            strSQL.Append("    job_exp.WIN_REC_QTY,");
            strSQL.Append("    job_exp.WIN_BALANCE_QTY,");
            strSQL.Append("    job_exp.WIN_TOTAL_WT,");
            strSQL.Append("    job_exp.WIN_REC_WT,");
            strSQL.Append("    job_exp.WIN_BALANCE_WT,");
            strSQL.Append("    bst.operator_mst_fk, ");
            strSQL.Append("    oprator.operator_id \"operator_id\",");
            strSQL.Append("    oprator.operator_name \"operator_name\",");
            //strSQL.Append(vbCrLf & "    job_exp.vessel_name ""vessel_name"",  ")
            //strSQL.Append(vbCrLf & "    job_exp.voyage ""voyage"",   ")
            strSQL.Append("    VVT.VOYAGE_TRN_PK \"VoyagePK\",  ");
            strSQL.Append("    V.VESSEL_ID \"vessel_id\",   ");
            strSQL.Append("    V.VESSEL_NAME \"vessel_name\",  ");
            strSQL.Append("    VVT.VOYAGE \"voyage\",   ");
            strSQL.Append("    TO_CHAR(job_exp.eta_date,DATETIMEFORMAT24)eta_date , ");
            strSQL.Append("    TO_CHAR(job_exp.etd_date,DATETIMEFORMAT24) etd_date, ");
            strSQL.Append("    TO_CHAR(job_exp.arrival_date,DATETIMEFORMAT24) arrival_date, ");
            strSQL.Append("    TO_CHAR(job_exp.departure_date,DATETIMEFORMAT24) departure_date, ");
            strSQL.Append("    job_exp.sec_vessel_name, ");
            strSQL.Append("    job_exp.sec_voyage,  ");
            strSQL.Append("    TO_CHAR(job_exp.sec_eta_date,DATEFORMAT) sec_eta_date, ");
            strSQL.Append("    TO_CHAR(job_exp.sec_etd_date,DATEFORMAT) sec_etd_date, ");
            strSQL.Append("    job_exp.shipper_cust_mst_fk, ");
            strSQL.Append("    shipper.customer_id \"Shipper\", ");
            strSQL.Append("    shipper.customer_name \"ShipperName\", ");
            strSQL.Append("    job_exp.consignee_cust_mst_fk, ");
            strSQL.Append("    consignee.customer_id \"Consignee\",");
            strSQL.Append("    consignee.customer_name \"ConsigneeName\",");
            strSQL.Append("    job_exp.notify1_cust_mst_fk, ");
            strSQL.Append("    notify1.customer_id \"Notify1\" ,");
            strSQL.Append("    notify1.customer_name \"Notify1Name\" ,");
            strSQL.Append("    job_exp.notify2_cust_mst_fk, ");
            strSQL.Append("    notify2.customer_id \"Notify2\",");
            strSQL.Append("    notify2.customer_name \"Notify2Name\",");
            strSQL.Append("    job_exp.cb_agent_mst_fk, ");
            strSQL.Append("    cbagnt.agent_id \"cbAgent\",");
            strSQL.Append("    cbagnt.agent_name \"cbAgentName\",");
            strSQL.Append("    job_exp.dp_agent_mst_fk, ");
            strSQL.Append("    dpagnt.agent_id \"dpAgent\",");
            strSQL.Append("    dpagnt.agent_name \"dpAgentName\",");
            strSQL.Append("    job_exp.cl_agent_mst_fk, ");
            strSQL.Append("    clagnt.agent_id \"clAgent\",");
            strSQL.Append("    clagnt.agent_name \"clAgentName\",");
            strSQL.Append("    job_exp.remarks,  ");
            strSQL.Append("    job_exp.version_no,  ");
            strSQL.Append("    job_exp.jobcard_date,");
            strSQL.Append("    job_exp.ucr_no,");
            strSQL.Append("    job_exp.cargo_move_fk,");
            strSQL.Append("    job_exp.pymt_type,");
            strSQL.Append("    job_exp.shipping_terms_mst_fk,");
            strSQL.Append("    job_exp.insurance_amt,");
            strSQL.Append("    job_exp.insurance_currency,");
            strSQL.Append("    job_exp.commodity_group_fk,");
            strSQL.Append("    comm.commodity_group_desc,");
            //fields are added after the UAT..
            //strSQL.Append(vbCrLf & "    depot.vendor_id ""depot_id"",")
            //strSQL.Append(vbCrLf & "    depot.vendor_name ""depot_name"",")
            //strSQL.Append(vbCrLf & "    depot.vendor_mst_pk ""depot_pk"",")
            strSQL.Append("    DPT.PORT_ID \"carrier_id\",");
            strSQL.Append("    DPT.PORT_NAME \"carrier_name\",");
            strSQL.Append("    DPT.PORT_MST_PK \"carrier_pk\",");

            //strSQL.Append(vbCrLf & "    carrier.vendor_id ""carrier_id"",")
            //strSQL.Append(vbCrLf & "    carrier.vendor_name ""carrier_name"",")
            //strSQL.Append(vbCrLf & "    carrier.vendor_mst_pk ""carrier_pk"",")
            strSQL.Append("    carrier.vendor_id \"depot_id\",");
            strSQL.Append("    carrier.vendor_name \"depot_name\",");
            strSQL.Append("    carrier.vendor_mst_pk \"depot_pk\",");
            strSQL.Append("    country.country_id \"country_id\",");
            strSQL.Append("    country.country_name \"country_name\",");
            strSQL.Append("    country.country_mst_pk \"country_mst_pk\",");
            strSQL.Append("    job_exp.da_number \"da_number\",");
            strSQL.Append("    job_exp.hbl_exp_tbl_fk, ");
            strSQL.Append("    job_exp.mbl_exp_tbl_fk, ");
            strSQL.Append("    job_exp.master_jc_sea_exp_fk, ");
            strSQL.Append("    mst.master_jc_ref_no, ");
            strSQL.Append("    mst.MASTER_JC_DATE, ");
            strSQL.Append("    job_exp.GOODS_DESCRIPTION,");
            strSQL.Append("    job_exp.MARKS_NUMBERS,");
            strSQL.Append("    hbl.hbl_ref_no hbl_no,");
            strSQL.Append("    hbl.hbl_date,");
            strSQL.Append("    mbl.mbl_ref_no mbl_no,");
            strSQL.Append("    mbl.mbl_date,");
            strSQL.Append("    job_exp.SHIPPING_INST_FLAG,");
            strSQL.Append("    job_exp.RFS,");
            strSQL.Append("    job_exp.CRQ,");
            strSQL.Append("    job_exp.STF,");
            strSQL.Append("    job_exp.SHIPPING_INST_DT,");
            strSQL.Append("    job_exp.RFS_DATE,");
            strSQL.Append("    job_exp.CRQ_DATE,");
            strSQL.Append("    job_exp.STF_DATE,");
            strSQL.Append("    job_exp.SURVEY_COMPLETED,");
            strSQL.Append("    job_exp.SURVEY_REF_NR,");
            strSQL.Append("    job_exp.SURVEY_DATE,");
            strSQL.Append("    job_exp.SURVEYOR_FK \"Survey_PK\",");
            strSQL.Append("    job_exp.SURVEY_REMARKS,");
            strSQL.Append("    Surveyor.vendor_id,");
            strSQL.Append("    Surveyor.vendor_name,");
            strSQL.Append("    job_exp.sb_number,job_exp.sb_date, ");
            strSQL.Append("    job_exp.cha_agent_mst_fk, ");
            strSQL.Append("    CHAAGNT.VENDOR_ID \"CHAAgentID\",");
            strSQL.Append("    CHAAGNT.VENDOR_NAME \"CHAAgentName\",job_exp.stuff_loc,");
            strSQL.Append("    curr.currency_id,HBL.HBL_STATUS ,job_exp.ADDVATOS_FLAG, bst.shipment_date,job_exp.LC_SHIPMENT,");
            strSQL.Append("    NVL(JOB_EXP.CHK_NOMINATED,0) CHK_NOMINATED,");
            strSQL.Append("    NVL(JOB_EXP.CHK_CSR,1) CHK_CSR,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_MST_PK,NVL(SHP_SE.EMPLOYEE_MST_PK,0)) SALES_EXEC_FK,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_ID,SHP_SE.EMPLOYEE_ID) SALES_EXEC_ID,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_NAME,SHP_SE.EMPLOYEE_NAME) SALES_EXEC_NAME ,");
            strSQL.Append("    job_exp.cc_req,job_exp.cc_ie,job_exp.PRC_FK,job_exp.ONC_FK,job_exp.PRC_MODE_FK,job_exp.ONC_MODE_FK,");
            strSQL.Append("   JOB_EXP.CHA_AGENT_MST_FK,");
            strSQL.Append("   CHAAGNT.VENDOR_ID \"CHAAgentID\",");
            strSQL.Append("   CHAAGNT.VENDOR_NAME \"CHAAgentName\" ");
            strSQL.Append("   FROM ");
            strSQL.Append("    JOB_CARD_TRN job_exp,");
            strSQL.Append("    BOOKING_MST_TBL bst,");
            strSQL.Append("    port_mst_tbl POD,");
            strSQL.Append("    port_mst_tbl POL,");
            strSQL.Append("    customer_mst_tbl cust,");
            strSQL.Append("    customer_mst_tbl consignee,");
            strSQL.Append("    customer_mst_tbl shipper,");
            strSQL.Append("    customer_mst_tbl notify1,");
            strSQL.Append("    customer_mst_tbl notify2,");
            strSQL.Append("    place_mst_tbl FRMPLC,");
            strSQL.Append("    place_mst_tbl TOPLC,");
            strSQL.Append("    PORT_MST_TBL POO,");
            strSQL.Append("    PORT_MST_TBL PFD,");
            strSQL.Append("    operator_mst_tbl oprator,");
            strSQL.Append("    agent_mst_tbl clagnt, ");
            strSQL.Append("    agent_mst_tbl dpagnt, ");
            strSQL.Append("    agent_mst_tbl cbagnt, ");
            strSQL.Append("    VENDOR_MST_TBL chaagnt, ");
            strSQL.Append("    commodity_group_mst_tbl comm, ");
            strSQL.Append("    VESSEL_VOYAGE_TBL V,  ");
            strSQL.Append("    VESSEL_VOYAGE_TRN VVT, ");
            strSQL.Append("    PORT_MST_TBL DPT,");
            strSQL.Append("    vendor_mst_tbl  carrier,");
            strSQL.Append("    vendor_mst_tbl  Surveyor,");
            strSQL.Append("    country_mst_tbl country,");
            strSQL.Append("    master_jc_sea_exp_tbl mst,");
            strSQL.Append("    hbl_exp_tbl hbl,");
            strSQL.Append("    mbl_exp_tbl mbl,");
            strSQL.Append("    currency_type_mst_tbl   curr,");
            strSQL.Append("    EMPLOYEE_MST_TBL        EMP, ");
            strSQL.Append("    EMPLOYEE_MST_TBL        SHP_SE ");
            //SHIPPER SALES PERSON
            strSQL.Append(" WHERE ");
            strSQL.Append("    job_exp.JOB_CARD_TRN_PK = " + jobCardPK);
            strSQL.Append("    AND job_exp.booking_sea_fk           =  bst.booking_sea_pk");
            strSQL.Append("    AND bst.port_mst_pol_fk              =  pol.port_mst_pk");
            strSQL.Append("    AND bst.port_mst_pod_fk              =  pod.port_mst_pk");
            strSQL.Append("    AND bst.col_place_mst_fk             =  FRMPLC.place_pk(+)");
            strSQL.Append("    AND bst.del_place_mst_fk             =  TOPLC.place_pk(+)");
            strSQL.Append("    AND bst.POO_FK             =  POO.PORT_MST_PK(+)");
            strSQL.Append("    AND bst.PFD_FK             =  PFD.PORT_MST_PK(+)");
            strSQL.Append("    AND bst.cust_customer_mst_fk         =  cust.customer_mst_pk(+) ");
            strSQL.Append("    AND bst.operator_mst_fk              =  oprator.operator_mst_pk(+)");
            strSQL.Append("    AND job_exp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.cha_agent_mst_fk         =  CHAAGNT.VENDOR_MST_PK(+) ");
            strSQL.Append("    AND job_exp.cb_agent_mst_fk          =  cbagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.dp_agent_mst_fk          =  dpagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.commodity_group_fk       =  comm.commodity_group_pk(+)");
            strSQL.Append("    AND JOB_EXP.TRANSPORTER_DEPOT_FK = DPT.PORT_MST_PK(+) ");
            strSQL.Append("    AND job_exp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.surveyor_fk              =  Surveyor.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.country_origin_fk        =  country.country_mst_pk(+)");
            strSQL.Append("    AND VVT.VESSEL_VOYAGE_TBL_FK         =  V.VESSEL_VOYAGE_TBL_PK(+)  ");
            strSQL.Append("    AND JOB_EXP.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
            strSQL.Append("    AND job_exp.master_jc_sea_exp_fk     =  mst.master_jc_sea_exp_pk(+)");

            strSQL.Append("    AND shipper.REP_EMP_MST_FK=SHP_SE.EMPLOYEE_MST_PK(+) ");
            strSQL.Append("    AND JOB_EXP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
            strSQL.Append("    and hbl.hbl_exp_tbl_pk(+) = job_exp.hbl_exp_tbl_fk");
            strSQL.Append("    and mbl.mbl_exp_tbl_pk(+) = job_exp.mbl_exp_tbl_fk");
            strSQL.Append("    and curr.currency_mst_pk(+) = job_exp.base_currency_mst_fk");
            WorkFlow objWF = new WorkFlow();
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

        #endregion "Fetch Main Jobcard for export"

        #region " Fetch Container data export"

        /// <summary>
        /// Fetches the container data export.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDataExport(string jobCardPK = "0", string MJCPK = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //by thiyagarajan for displaying container details in frmcargodetails.aspx which has link of booking sea.
                //26/2/08
                strSQL.Append("SELECT");
                strSQL.Append("    JOB_TRN_CONT_PK,");
                strSQL.Append("    container_number,");
                strSQL.Append("    cont.container_type_mst_id,");
                strSQL.Append("    container_type_mst_fk,");
                strSQL.Append("    seal_number,");
                strSQL.Append("    volume_in_cbm,");
                strSQL.Append("    gross_weight,");
                strSQL.Append("    net_weight,");
                strSQL.Append("    chargeable_weight,");
                strSQL.Append("    pack_type_mst_fk,");
                strSQL.Append("    pack_count,");
                strSQL.Append("    commodity_mst_fk,");
                if (string.IsNullOrEmpty(MJCPK))
                {
                    strSQL.Append("    TO_CHAR(job_trn_cont.load_date ,DATETIMEFORMAT24) load_date ");
                    // for now..
                }
                else
                {
                    strSQL.Append("    TO_CHAR(job_exp.departure_date ,DATETIMEFORMAT24) load_date ");
                }
                //Snigdharani - 21/08/2009
                strSQL.Append("     , job_trn_cont.CONTAINER_PK CONTAINER_PK");
                //strSQL.Append(vbCrLf & "    job_trn_cont.load_date load_date ")
                strSQL.Append("FROM");
                strSQL.Append("    JOB_TRN_CONT job_trn_cont,");
                strSQL.Append("    pack_type_mst_tbl pack,");
                strSQL.Append("    commodity_mst_tbl comm,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    JOB_CARD_TRN job_exp");
                strSQL.Append("WHERE ");
                strSQL.Append("    job_trn_cont.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.commodity_mst_fk = comm.commodity_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);

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

        #endregion " Fetch Container data export"

        #region " Fetch Container data export"

        /// <summary>
        /// Fetches the container data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDataExp(string jobCardPK = "0", string MJCPK = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT Q.JOB_TRN_CONT_PK,Q.CONTAINER_TYPE_MST_FK,Q.CONTAINER_NUMBER,Q.CONTAINER_TYPE_MST_ID, ");
                sb.Append("Q.SEAL_NUMBER,Q.FETCH_COMM,Q.PACK_TYPE_MST_FK,Q.PACK_COUNT, Q.NET_WEIGHT,Q.GROSS_WEIGHT, Q.VOLUME_IN_CBM,Q.CHARGEABLE_WEIGHT,");
                sb.Append("Q.COMMODITY_MST_FK, Q.LOAD_DATE, Q.COMMODITY_MST_FKS, Q.CONTAINER_PK FROM(");
                sb.Append("SELECT JOB_TRN_CONT_PK,");
                sb.Append("       JOB_TRN_CONT.CONTAINER_TYPE_MST_FK,");
                sb.Append("       JOB_TRN_CONT.CONTAINER_NUMBER,");
                sb.Append("       CONT.CONTAINER_TYPE_MST_ID,");
                sb.Append("       JOB_TRN_CONT.SEAL_NUMBER,");
                //sb.Append("       ' ' FETCH_COMM,")
                sb.Append("       (SELECT REPLACE(ROWTOCOL('SELECT DISTINCT CMD.COMMODITY_NAME||'';''");
                sb.Append("           FROM JOB_TRN_CONT JOB_CONT, JOBCARD_COMMODITY_DTL JCD,COMMODITY_MST_TBL CMD");
                sb.Append("          WHERE JOB_CONT.JOB_TRN_CONT_PK = JCD.JOB_TRN_CONT_FK AND JCD.COMMODITY_MST_FK=CMD.COMMODITY_MST_PK ");
                sb.Append("          AND JOB_CONT.JOB_TRN_CONT_PK='||JOB_TRN_CONT.JOB_TRN_CONT_PK),';,',';') FROM DUAL) FETCH_COMM,");
                //sb.Append("     CASE WHEN BST.CARGO_TYPE=1 THEN ")
                sb.Append(" (SELECT ROWTOCOL('SELECT PT.PACK_TYPE_MST_PK FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (");
                sb.Append("     SELECT DISTINCT JC.PACK_TYPE_FK FROM JOB_TRN_CONT JOB,JOBCARD_COMMODITY_DTL JC ");
                sb.Append("     WHERE JOB.JOB_TRN_CONT_PK=JC.JOB_TRN_CONT_FK ");
                sb.Append("     AND JOB.JOB_TRN_CONT_PK='||JOB_TRN_CONT.JOB_TRN_CONT_PK||')') FROM DUAL) PACK_TYPE_MST_FK, ");
                //sb.Append("       ELSE TO_CHAR(JOB_TRN_CONT.PACK_TYPE_MST_FK) END PACK_TYPE_MST_FK,")
                sb.Append("       DECODE(JOB_TRN_CONT.PACK_COUNT,");
                sb.Append("              0,");
                sb.Append("              SUM(JCD.PACK_COUNT),");
                sb.Append("              JOB_TRN_CONT.PACK_COUNT) PACK_COUNT,");
                sb.Append("       DECODE(JOB_TRN_CONT.NET_WEIGHT,");
                sb.Append("              0,");
                sb.Append("              SUM(JCD.NET_WEIGHT),");
                sb.Append("              JOB_TRN_CONT.NET_WEIGHT) NET_WEIGHT,");
                sb.Append("       DECODE(JOB_TRN_CONT.GROSS_WEIGHT,");
                sb.Append("              0,");
                sb.Append("              (SUM(JCD.NET_WEIGHT)+CONT.CONTAINER_TAREWEIGHT_TONE),");
                sb.Append("              JOB_TRN_CONT.GROSS_WEIGHT) GROSS_WEIGHT,");
                sb.Append("       ");
                sb.Append("       DECODE(JOB_TRN_CONT.VOLUME_IN_CBM,");
                sb.Append("              0,");
                sb.Append("              SUM(JCD.VOLUME_IN_CBM),");
                sb.Append("              JOB_TRN_CONT.VOLUME_IN_CBM) VOLUME_IN_CBM,");
                sb.Append("       JOB_TRN_CONT.CHARGEABLE_WEIGHT,");
                sb.Append("       JOB_TRN_CONT.COMMODITY_MST_FK,");
                if (string.IsNullOrEmpty(MJCPK))
                {
                    sb.Append("       TO_CHAR(JOB_TRN_CONT.LOAD_DATE, DATETIMEFORMAT24) LOAD_DATE,");
                }
                else
                {
                    sb.Append("       TO_CHAR(JOB_EXP.DEPARTURE_DATE, DATETIMEFORMAT24) LOAD_DATE,");
                }
                sb.Append("       JOB_TRN_CONT.COMMODITY_MST_FKS,");
                sb.Append("       JOB_TRN_CONT.CONTAINER_PK CONTAINER_PK,");
                sb.Append("       NVL(cont.Preferences,-1) AS preference");
                sb.Append("  FROM JOB_TRN_CONT   JOB_TRN_CONT,");
                sb.Append("       PACK_TYPE_MST_TBL      PACK,");
                sb.Append("       COMMODITY_MST_TBL      COMM,");
                sb.Append("       CONTAINER_TYPE_MST_TBL CONT,");
                sb.Append("       JOB_CARD_TRN   JOB_EXP,");
                sb.Append("       BOOKING_MST_TBL        BST,");
                sb.Append("       JOBCARD_COMMODITY_DTL  JCD");
                sb.Append(" WHERE JOB_TRN_CONT.PACK_TYPE_MST_FK = PACK.PACK_TYPE_MST_PK(+)");
                sb.Append("   AND JOB_EXP.BOOKING_MST_FK= BST.BOOKING_MST_PK ");
                sb.Append("   AND JOB_TRN_CONT.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND JOB_TRN_CONT.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK(+)");
                sb.Append("   AND JOB_TRN_CONT.JOB_TRN_CONT_PK = JCD.JOB_TRN_CONT_FK(+)");
                sb.Append("   AND JOB_TRN_CONT.job_card_trn_fk = JOB_EXP.JOB_CARD_TRN_PK");
                sb.Append("   AND JOB_EXP.JOB_CARD_TRN_PK = " + jobCardPK);
                sb.Append(" GROUP BY JOB_TRN_CONT.JOB_TRN_CONT_PK,");
                sb.Append("          JOB_TRN_CONT.CONTAINER_TYPE_MST_FK,");
                sb.Append("          BST.CARGO_TYPE,PACK.PACK_TYPE_DESC,");
                sb.Append("          JOB_TRN_CONT.CONTAINER_NUMBER,");
                sb.Append("          CONT.CONTAINER_TAREWEIGHT_TONE,");
                sb.Append("          CONT.CONTAINER_TYPE_MST_ID,");
                sb.Append("          JOB_TRN_CONT.SEAL_NUMBER,");
                sb.Append("          JOB_TRN_CONT.PACK_TYPE_MST_FK,");
                sb.Append("          JOB_TRN_CONT.PACK_COUNT,");
                sb.Append("          JOB_TRN_CONT.COMMODITY_MST_FK,");
                sb.Append("          JOB_TRN_CONT.COMMODITY_MST_FKS,");
                sb.Append("          JOB_TRN_CONT.CONTAINER_PK,");
                sb.Append("          JOB_TRN_CONT.NET_WEIGHT,");
                sb.Append("          JOB_TRN_CONT.GROSS_WEIGHT,");
                sb.Append("          JOB_TRN_CONT.VOLUME_IN_CBM,");
                sb.Append("          JOB_TRN_CONT.CHARGEABLE_WEIGHT,NVL(cont.Preferences,-1),");
                if (string.IsNullOrEmpty(MJCPK))
                {
                    sb.Append("          JOB_TRN_CONT.LOAD_DATE");
                }
                else
                {
                    sb.Append("          JOB_EXP.DEPARTURE_DATE");
                }
                sb.Append(" ORDER BY NVL(cont.Preferences,-1))Q");

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

        /// <summary>
        /// Fetches the container data exp booking.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDataExpBooking(string jobCardPK = "0", string MJCPK = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT jc.job_trn_cont_pk,");
                sb.Append("       jc.container_number,");
                sb.Append("       cont.container_type_mst_id,");
                sb.Append("       jc.container_type_mst_fk,");
                sb.Append("       jc.seal_number,");
                sb.Append("       jc.volume_in_cbm,");
                sb.Append("       jc.gross_weight,");
                sb.Append("       jc.net_weight,");
                sb.Append("       jc.chargeable_weight,");
                sb.Append("       (SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (     SELECT DISTINCT JC.PACK_TYPE_FK FROM JOB_TRN_CONT JOB,JOBCARD_COMMODITY_DTL JC      WHERE JOB.JOB_TRN_CONT_PK=JC.JOB_TRN_CONT_FK      AND JOB.JOB_TRN_CONT_PK=' ||");
                sb.Append("                        jc.job_trn_cont_pk || ')')");
                sb.Append("          FROM DUAL) pack_type_mst_fk,");
                sb.Append("       pack_count,");
                sb.Append("       commodity_mst_fk,");
                sb.Append("       ' ' fetch_comm,");
                if (string.IsNullOrEmpty(MJCPK))
                {
                    sb.Append("    TO_CHAR(jc.load_date ,DATETIMEFORMAT24) load_date, ");
                }
                else
                {
                    sb.Append("    TO_CHAR(job_exp.departure_date ,DATETIMEFORMAT24) load_date, ");
                }
                sb.Append("       (SELECT ROWTOCOL('SELECT J.COMMODITY_MST_FK FROM JOBCARD_COMMODITY_DTL J WHERE J.JOB_TRN_CONT_FK=' ||");
                sb.Append("                        jc.job_trn_cont_pk || '')");
                sb.Append("          FROM DUAL) COMMODITY_MST_FKS,");
                sb.Append("       jc.container_pk");
                sb.Append("  FROM job_trn_cont           jc,");
                sb.Append("       pack_type_mst_tbl      pack,");
                sb.Append("       commodity_mst_tbl      comm,");
                sb.Append("       container_type_mst_tbl cont,");
                sb.Append("       job_card_trn           job_exp");
                sb.Append(" WHERE jc.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
                sb.Append("   AND jc.container_type_mst_fk = cont.container_type_mst_pk(+)");
                sb.Append("   AND jc.commodity_mst_fk = comm.commodity_mst_pk(+)");
                sb.Append("   AND jc.job_card_trn_fk = job_exp.job_card_trn_pk");
                sb.Append("   AND job_exp.job_card_trn_pk = " + jobCardPK);

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

        #endregion " Fetch Container data export"

        #region "Frieght Element"

        /// <summary>
        /// Fetches the cost det.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchCostDet(int jobcardpk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT JOBCOST.JOB_TRN_SEA_EXP_COST_PK");
                sb.Append("  FROM JOB_CARD_TRN JOB,");
                sb.Append("       JOB_TRN_COST JOBCOST,");
                sb.Append("       INV_SUPPLIER_TBL     INV,");
                sb.Append("       INV_SUPPLIER_TRN_TBL INVTRN");
                sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JOBCOST.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + jobcardpk);
                sb.Append("   AND INVTRN.JOB_TRN_EST_FK = JOBCOST.JOB_TRN_SEA_EXP_COST_PK");
                sb.Append("   AND INV.INV_SUPPLIER_PK = INVTRN.INV_SUPPLIER_TBL_FK");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the fret.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchFret(int jobcardpk)
        {
            try
            {
                string strsql = null;
                WorkFlow objWF = new WorkFlow();
                ///strsql = "select * from  CONSOL_INVOICE_TRN_TBl where JOB_CARD_FK = " & jobcardpk & " AND FRT_OTH_ELEMENT = 1"
                strsql = "select con.FRT_OTH_ELEMENT_FK,cont.container_type_mst_pk from  CONSOL_INVOICE_TRN_TBl con , ";
                strsql = strsql + " JOB_TRN_FD job_trn_fd, container_type_mst_tbl cont ";
                strsql = strsql + " where con.FRT_OTH_ELEMENT = 1 AND job_trn_fd.container_type_mst_fk = cont.container_type_mst_pk(+)";
                strsql = strsql + " and con.frt_oth_element_fk=job_trn_fd.freight_element_mst_fk ";
                strsql = strsql + "  and con.JOB_CARD_FK = " + jobcardpk;
                strsql = strsql + " and con.consol_invoice_trn_pk=job_trn_fd.consol_invoice_trn_fk ";
                return objWF.GetDataSet(strsql);
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
        /// Fetches the agent fret.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchAgentFret(int jobcardpk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT INVTRN.COST_FRT_ELEMENT_FK, CONT.CONTAINER_TYPE_MST_PK");
                sb.Append("  FROM inv_agent_tbl     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       JOB_TRN_FD        JOB_TRN_FD,");
                sb.Append("       CONTAINER_TYPE_MST_TBL    CONT");
                sb.Append(" WHERE INVTRN.COST_FRT_ELEMENT = 2");
                sb.Append("   AND JOB_TRN_FD.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND INV.JOB_CARD_SEA_EXP_FK = JOB_TRN_FD.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("   AND INVTRN.COST_FRT_ELEMENT_FK = JOB_TRN_FD.FREIGHT_ELEMENT_MST_FK");
                sb.Append("   AND INV.JOB_CARD_FK = " + jobcardpk);
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Frieght Element"

        #region " Fetch Freight data export"

        //added by manoharan 2/11/2006 for disable the entry in Fre. Data when already invoiced

        /// <summary>
        /// Fetches the fre det.
        /// </summary>
        /// <param name="jcpk">The JCPK.</param>
        /// <returns></returns>
        public DataSet FetchFreDet(string jcpk)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_fd.invoice_sea_tbl_fk,");
                strSQL.Append("    job_trn_fd.inv_agent_trn_fk,");
                strSQL.Append("    job_trn_fd.consol_invoice_trn_fk");
                strSQL.Append("    FROM");
                strSQL.Append("    JOB_TRN_FD job_trn_fd,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    freight_element_mst_tbl freight,");
                strSQL.Append("    JOB_CARD_TRN job_exp");
                strSQL.Append("    WHERE");
                strSQL.Append("    job_trn_fd.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append("    AND job_trn_fd.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append("    AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk");
                strSQL.Append("    AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jcpk);
                strSQL.Append("    ORDER BY cont.container_type_mst_id,freight.freight_element_id ");

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

        /// <summary>
        /// Fetches the freight data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="jobProfit">The job profit.</param>
        /// <param name="BaseCurrFk">The base curr fk.</param>
        /// <returns></returns>
        public DataSet FetchFreightDataExp(string jobCardPK = "0", Int64 jobProfit = 0, string BaseCurrFk = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" SELECT QRY.JOB_TRN_FD_PK,");
                strSQL.Append(" QRY.CONTAINER_TYPE_MST_FK,");
                strSQL.Append(" QRY.FREIGHT_ELEMENT_ID,");
                strSQL.Append(" QRY.FREIGHT_ELEMENT_NAME,");
                strSQL.Append(" QRY.FREIGHT_ELEMENT_MST_PK,");
                strSQL.Append(" QRY.BASIS,");
                strSQL.Append(" QRY.QUANTITY,");
                strSQL.Append(" QRY.FREIGHT_TYPE,");
                strSQL.Append(" QRY.LOCATION_MST_FK,");
                strSQL.Append(" QRY.LOCATION_ID,");
                strSQL.Append(" QRY.FRTPAYER_CUST_MST_FK,");
                strSQL.Append(" QRY.CUSTOMER_ID,");
                strSQL.Append(" QRY.CURRENCY_MST_FK,");
                strSQL.Append(" QRY.RATEPERBASIS,");
                strSQL.Append(" QRY.FREIGHT_AMT,");
                strSQL.Append(" QRY.ROE,");
                strSQL.Append(" QRY.TOTAL_AMT,");
                strSQL.Append(" QRY.\"Delete\",");
                strSQL.Append(" QRY.\"Print\",");
                strSQL.Append(" QRY.CREDIT,");
                strSQL.Append(" QRY.DIMENTION_ID ");

                strSQL.Append(" FROM(");

                strSQL.Append(" SELECT Q.* FROM (");
                strSQL.Append(" SELECT");
                strSQL.Append("    jfd.job_trn_fd_pk,\t");
                strSQL.Append("    container_type_mst_fk,");
                strSQL.Append("    freight.freight_element_id,");
                strSQL.Append("    freight.freight_element_name,");
                strSQL.Append("    freight.freight_element_mst_pk,\t");
                strSQL.Append("    jfd.basis,");
                strSQL.Append("    jfd.quantity,");
                if (jobProfit == 1)
                {
                    strSQL.Append("   jfd.FREIGHT_TYPE,");
                }
                else
                {
                    strSQL.Append("   CASE WHEN FREIGHT.FREIGHT_ELEMENT_ID = 'THD' THEN 'Collect' ");
                    strSQL.Append("   ELSE DECODE(jfd.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') ");
                    strSQL.Append("  END FREIGHT_TYPE, ");
                }
                strSQL.Append("    jfd.location_mst_fk, ");
                strSQL.Append("    lmt.location_id ,");
                strSQL.Append("    jfd.frtpayer_cust_mst_fk,");
                strSQL.Append("    cmt.customer_id, ");
                //strSQL.Append(vbCrLf & "    curr.currency_id currency_mst_fk,")
                strSQL.Append("    jfd.Currency_Mst_Fk,");
                strSQL.Append("  nvl(jfd.Rateperbasis, 0) Rateperbasis,");
                strSQL.Append("  nvl(jfd.freight_amt, 0) freight_amt,");
                if (Convert.ToInt32(BaseCurrFk) != 0)
                {
                    strSQL.Append("       NVL(GET_EX_RATE(jfd.CURRENCY_MST_FK, " + BaseCurrFk + ", job_exp.JOBCARD_DATE),0) AS ROE,");
                    strSQL.Append("    (jfd.FREIGHT_AMT* NVL(GET_EX_RATE(jfd.CURRENCY_MST_FK, " + BaseCurrFk + ", job_exp.JOBCARD_DATE),0)) total_amt,");
                }
                else
                {
                    strSQL.Append("    jfd.exchange_rate AS ROE ,");
                    strSQL.Append("    (jfd.FREIGHT_AMT*jfd.EXCHANGE_RATE) total_amt,");
                }
                strSQL.Append("    'false' as \"Delete\", jfd.PRINT_ON_MBL \"Print\",FREIGHT.Credit , dmt.dimention_id");
                strSQL.Append("    FROM");
                strSQL.Append("    job_trn_fd jfd,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    freight_element_mst_tbl freight,");
                strSQL.Append("    parameters_tbl prm,");
                strSQL.Append("    job_card_trn job_exp,");
                strSQL.Append("    location_mst_tbl lmt,");
                strSQL.Append("    customer_mst_tbl cmt, dimention_unit_mst_tbl dmt");
                strSQL.Append("    WHERE");
                strSQL.Append("    jfd.job_card_trn_fk = job_exp.job_card_trn_pk");
                strSQL.Append("    AND jfd.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append("    AND jfd.Currency_Mst_Fk = curr.currency_mst_pk");
                strSQL.Append("    AND jfd.freight_element_mst_fk = freight.freight_element_mst_pk");
                strSQL.Append("    AND jfd.freight_element_mst_fk = prm.frt_bof_fk");
                strSQL.Append("   AND jfd.location_mst_fk = lmt.location_mst_pk (+)");
                strSQL.Append("   AND jfd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append("    and  dmt.dimention_unit_mst_pk(+) = jfd.basis");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);
                strSQL.Append("    AND NVL(jfd.SERVICE_TYPE_FLAG,0) <>1");
                strSQL.Append("    ) Q");

                strSQL.Append(" union all ");

                strSQL.Append("  SELECT Q1.* FROM (");
                strSQL.Append(" SELECT");
                strSQL.Append("    jfd.job_trn_fd_pk,\t");
                strSQL.Append("    container_type_mst_fk,");
                strSQL.Append("    freight.freight_element_id,");
                strSQL.Append("    freight.freight_element_name,");
                strSQL.Append("    freight.freight_element_mst_pk,\t");
                strSQL.Append("    jfd.basis,");
                strSQL.Append("    jfd.quantity,");
                if (jobProfit == 1)
                {
                    strSQL.Append("   jfd.FREIGHT_TYPE,");
                }
                else
                {
                    strSQL.Append("   CASE WHEN FREIGHT.FREIGHT_ELEMENT_ID = 'THD' THEN 'Collect' ");
                    strSQL.Append("   ELSE DECODE(jfd.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') ");
                    strSQL.Append("  END FREIGHT_TYPE, ");
                }
                strSQL.Append("    jfd.location_mst_fk, ");
                strSQL.Append("    lmt.location_id ,");
                strSQL.Append("    jfd.frtpayer_cust_mst_fk,");
                strSQL.Append("    cmt.customer_id, ");
                //strSQL.Append(vbCrLf & "    curr.currency_id currency_mst_fk,")
                strSQL.Append("    jfd.Currency_Mst_Fk,");
                strSQL.Append("  nvl(jfd.Rateperbasis, 0) Rateperbasis,");
                strSQL.Append("  nvl(jfd.freight_amt, 0) freight_amt,");
                if (Convert.ToInt32(BaseCurrFk) != 0)
                {
                    strSQL.Append("        NVL(GET_EX_RATE(jfd.CURRENCY_MST_FK, " + BaseCurrFk + ", job_exp.JOBCARD_DATE),0) AS ROE,");
                    strSQL.Append("    (jfd.FREIGHT_AMT* NVL(GET_EX_RATE(jfd.CURRENCY_MST_FK, " + BaseCurrFk + ", job_exp.JOBCARD_DATE),0)) total_amt,");
                }
                else
                {
                    strSQL.Append("    jfd.exchange_rate AS ROE ,");
                    strSQL.Append("    (jfd.FREIGHT_AMT*jfd.EXCHANGE_RATE) total_amt,");
                }

                strSQL.Append("    'false' as \"Delete\", jfd.PRINT_ON_MBL \"Print\",freight.Credit,dmt.dimention_id");
                strSQL.Append("    FROM");
                strSQL.Append("    job_trn_fd jfd,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    freight_element_mst_tbl freight,");
                strSQL.Append("    parameters_tbl prm,");
                strSQL.Append("    job_card_trn job_exp,");
                strSQL.Append("    location_mst_tbl lmt,");
                strSQL.Append("    customer_mst_tbl cmt,dimention_unit_mst_tbl dmt");
                strSQL.Append("    WHERE");
                strSQL.Append("    jfd.job_card_trn_fk = job_exp.job_card_trn_pk");
                strSQL.Append("    AND jfd.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append("    AND jfd.Currency_Mst_Fk = curr.currency_mst_pk");
                strSQL.Append("    AND jfd.freight_element_mst_fk = freight.freight_element_mst_pk");
                strSQL.Append("    AND jfd.freight_element_mst_fk not in  prm.frt_bof_fk");
                strSQL.Append("   AND jfd.location_mst_fk = lmt.location_mst_pk (+)");
                strSQL.Append("   AND jfd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append("    and  dmt.dimention_unit_mst_pk(+) = jfd.basis");
                strSQL.Append("    AND job_exp.job_card_trn_pk =" + jobCardPK);
                strSQL.Append("    AND NVL(jfd.SERVICE_TYPE_FLAG,0) <>1");
                strSQL.Append("    ) Q1");
                strSQL.Append("   )Qry,freight_element_mst_tbl femt,container_type_mst_tbl ctmt");
                strSQL.Append("   WHERE qry.container_type_mst_fk=ctmt.container_type_mst_pk(+)");
                strSQL.Append("   AND qry.freight_element_mst_pk=femt.freight_element_mst_pk");
                strSQL.Append("   ORDER BY ctmt.preferences,femt.preference");
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

        #endregion " Fetch Freight data export"

        #region " Fetch Purchase Inventory data export"

        /// <summary>
        /// Fetches the purchase inv data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchPurchaseInvDataExp(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT distinct");
                strSQL.Append("    job_trn_sea_exp_pia_pk,");
                strSQL.Append("    vendor_key,\t");
                strSQL.Append("    cost_element_id,\t");
                strSQL.Append("    invoice_number,\t");
                strSQL.Append("    to_char(job_trn_pia.invoice_date, dateformat) invoice_date,");
                strSQL.Append("    currency_mst_fk,");
                strSQL.Append("    invoice_amt,\t");
                strSQL.Append("    tax_percentage,");
                strSQL.Append("    tax_amt,\t");
                strSQL.Append("    estimated_amt,");
                //strSQL.Append(vbCrLf & "    NVL(invoice_amt,0) - NVL(estimated_amt,0) diff_amt,")
                strSQL.Append("    invoice_amt - NVL(estimated_amt,0) diff_amt,");
                strSQL.Append("    vendor_mst_fk, ");
                strSQL.Append("    cost_element_mst_fk,");
                strSQL.Append("    job_trn_pia.attached_file_name,'false' as \"Delete\", MJC_TRN_SEA_EXP_PIA_FK ");

                strSQL.Append("FROM");
                strSQL.Append("    job_trn_sea_exp_pia  job_trn_pia,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    cost_element_mst_tbl cost_ele,");
                strSQL.Append("    JOB_CARD_TRN job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_pia.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append("    AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
                strSQL.Append("    AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);
                strSQL.Append("    AND (SELECT ist.approved from inv_supplier_tbl ist , inv_supplier_trn_tbl ISTR where ISTR.INV_SUPPLIER_TBL_FK = IST.INV_SUPPLIER_PK AND ISTR.JOB_CARD_PIA_FK  = job_trn_pia.job_card_sea_exp_fk) <>2");
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

        /// <summary>
        /// Fetches the pia.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchPIA(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    INV_CUST_TRN_FK invoice_sea_tbl_fk,");
                strSQL.Append("    inv_agent_trn_fk,");
                strSQL.Append("    inv_supplier_fk");
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_pia  job_trn_pia,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    cost_element_mst_tbl cost_ele,");
                strSQL.Append("    job_card_trn job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_pia.job_card_trn_fk = job_exp.job_card_trn_pk");
                strSQL.Append("    AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
                strSQL.Append("    AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
                strSQL.Append("    AND job_exp.job_card_trn_pk =" + jobCardPK);
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

        #endregion " Fetch Purchase Inventory data export"

        #region " Fetch Cost details data export"

        /// <summary>
        /// Fetches the cost detail data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="basecurrency">The basecurrency.</param>
        /// <returns></returns>
        public DataSet FetchCostDetailDataExp(string jobCardPK = "0", int basecurrency = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT JEC.JOB_TRN_COST_PK,");
                strSQL.Append("       JEC.job_card_trn_fk,");
                strSQL.Append("       VMT.VENDOR_MST_PK,");
                strSQL.Append("       CMT.COST_ELEMENT_MST_PK,");
                strSQL.Append("       JEC.VENDOR_KEY,");
                strSQL.Append("       CMT.COST_ELEMENT_ID,");
                strSQL.Append("       CMT.COST_ELEMENT_NAME,");
                strSQL.Append("       DECODE(JEC.PTMT_TYPE,1,'Prepaid',2,'Collect')PTMT_TYPE,");
                strSQL.Append("       LMT.LOCATION_ID,");
                strSQL.Append("       CURR.CURRENCY_ID,");
                strSQL.Append("       JEC.ESTIMATED_COST,");
                strSQL.Append("       ROUND(GET_EX_RATE_BUY(JEC.CURRENCY_MST_FK, " + basecurrency + ", round(TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT) - .5)), 6) AS ROE,");
                strSQL.Append("       JEC.TOTAL_COST,");
                strSQL.Append("       ''DEL_FLAG,");
                strSQL.Append("       'true' SEL_FLAG,");
                strSQL.Append("       JEC.LOCATION_MST_FK,");
                strSQL.Append("       JEC.CURRENCY_MST_FK");
                strSQL.Append("  FROM JOB_TRN_COST      JEC,");
                strSQL.Append("       JOB_CARD_TRN  JOB,");
                strSQL.Append("       VENDOR_MST_TBL        VMT,");
                strSQL.Append("       COST_ELEMENT_MST_TBL  CMT,");
                strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR,");
                strSQL.Append("       LOCATION_MST_TBL      LMT");
                strSQL.Append(" WHERE JEC.job_card_trn_fk = JOB.JOB_CARD_TRN_PK");
                strSQL.Append("   AND JEC.COST_ELEMENT_MST_FK = CMT.COST_ELEMENT_MST_PK");
                strSQL.Append("   AND JEC.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                strSQL.Append("   AND JEC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                strSQL.Append("   AND JEC.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                strSQL.Append("   AND JOB.JOB_CARD_TRN_PK = " + jobCardPK);
                strSQL.Append("   AND NVL(JEC.SERVICE_TYPE_FLAG,0)<>1");
                strSQL.Append("   ORDER BY CMT.PREFERENCE");
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

        #endregion " Fetch Cost details data export"

        #region " Fetch TP data export"

        /// <summary>
        /// Fetches the tp data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchTPDataExp(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_tp.job_trn_sea_exp_tp_pk,");
                strSQL.Append("    job_trn_tp.transhipment_no,");
                strSQL.Append("    job_trn_tp.port_mst_fk,");
                strSQL.Append("    port.port_id,");
                strSQL.Append("    port.port_name,");
                strSQL.Append("    job_trn_tp.vessel_name,");
                strSQL.Append("    job_trn_tp.voyage,");
                strSQL.Append("    TO_CHAR(job_trn_tp.eta_date,DATEFORMAT ) eta_date,");
                strSQL.Append("    TO_CHAR(job_trn_tp.etd_date,DATEFORMAT ) etd_date,");
                strSQL.Append("    agt.agent_id,");
                strSQL.Append("    agt.agent_name,");
                strSQL.Append("    'false' \"Delete\",");
                strSQL.Append("    job_trn_tp.voyage_trn_fk \"GridVoyagePK\", ");
                strSQL.Append("    job_trn_tp.agent_fk \"AgentPK\" ");
                strSQL.Append(" FROM");
                strSQL.Append("    job_trn_sea_exp_tp  job_trn_tp,");
                strSQL.Append("    port_mst_tbl port,");
                strSQL.Append("    agent_mst_tbl agt,");
                strSQL.Append("    location_mst_tbl lmt,");
                strSQL.Append("    vessel_voyage_trn vvt,");
                strSQL.Append("    JOB_CARD_TRN job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_tp.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append("    AND job_trn_tp.port_mst_fk = port.port_mst_pk");
                strSQL.Append("    AND agt.location_mst_fk = lmt.location_mst_pk");
                strSQL.Append("    AND lmt.location_mst_pk = port.location_mst_fk");
                strSQL.Append("    AND JOB_TRN_TP.AGENT_FK = AGT.AGENT_MST_PK");
                strSQL.Append("    AND job_trn_tp.voyage_trn_fk = vvt.voyage_trn_pk(+)");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);
                strSQL.Append("    ORDER BY transhipment_no");

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

        #endregion " Fetch TP data export"

        #region "Fetch For Transhipment"

        /// <summary>
        /// Fetches the agent pk.
        /// </summary>
        /// <param name="AgentPK">The agent pk.</param>
        /// <param name="TEU">The teu.</param>
        /// <returns></returns>
        public DataSet FetchAgentPK(string AgentPK, decimal TEU)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT '' JOB_TRN_SEA_EXP_PIA_PK,");
                sb.Append("       AMT.AGENT_ID VENDOR_KEY,");
                sb.Append("       CEM.COST_ELEMENT_ID COST_ELEMENT_ID,");
                sb.Append("       '' INVOICE_NUMBER,");
                sb.Append("       '' INVOICE_DATE,");
                sb.Append("       A.CURRENCY_TYPE_MST_FK CURRENCY_MST_FK,");
                sb.Append("       A.AMOUNT INVOICE_AMT,");
                sb.Append("       '' TAX_PERCENTAGE,");
                sb.Append("       '' TAX_AMT,");
                sb.Append("       NVL((A.AMOUNT * " + TEU + "), 0) ESTIMATED_AMT,");
                sb.Append("       NVL(A.AMOUNT - NVL((A.AMOUNT * " + TEU + "), 0), 0) DIFF_AMT,");
                sb.Append("       A.AGENT_MST_FK VENDOR_MST_FK,");
                sb.Append("       A.COST_ELEMENT_MST_FK COST_ELEMENT_MST_FK,");
                sb.Append("       '' ATTACHED_FILE_NAME,");
                sb.Append("       '0' AS \"Delete\",");
                sb.Append("       '' MJC_TRN_SEA_EXP_PIA_FK,");
                sb.Append("       '' UpdFlag");
                sb.Append("  FROM AGENT_TRANSHIP_TRN A, AGENT_MST_TBL AMT, COST_ELEMENT_MST_TBL CEM");
                sb.Append(" WHERE A.AGENT_MST_FK = " + AgentPK);
                sb.Append("   AND AMT.AGENT_MST_PK = A.AGENT_MST_FK");
                sb.Append("   AND A.COST_ELEMENT_MST_FK = CEM.COST_ELEMENT_MST_PK");

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

        #endregion "Fetch For Transhipment"

        #region "Calculate_TAX" ''Added by subhransu for tax calculation

        /// <summary>
        /// Calculate_s the tax.
        /// </summary>
        /// <param name="jobCardID">The job card identifier.</param>
        /// <returns></returns>
        public object Calculate_TAX(string jobCardID = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append(" SELECT  NVL(SUM(JP.TAX_AMT), 0) AS COST_TAX");
                sb.Append("   FROM JOB_CARD_TRN   JC,");
                sb.Append("        JOB_TRN_SEA_EXP_PIA    JP");
                sb.Append("  WHERE ");
                sb.Append("     JC.JOB_CARD_TRN_PK = " + jobCardID + "");
                sb.Append("    AND JC.JOB_CARD_TRN_PK = JP.JOB_CARD_SEA_EXP_FK(+)");

                return (objWF.GetDataSet(sb.ToString()));
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
        /// Calculate_s the ta x_ cost.
        /// </summary>
        /// <param name="jobCardID">The job card identifier.</param>
        /// <returns></returns>
        public object Calculate_TAX_Cost(string jobCardID = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append(" SELECT NVL(SUM(CI.TAX_AMT), 0) AS REVENUE_TAX");
                sb.Append("   FROM JOB_CARD_TRN   JC,");
                sb.Append("        CONSOL_INVOICE_TRN_TBL CI");

                sb.Append("  WHERE JC.JOB_CARD_TRN_PK = CI.JOB_CARD_FK(+)");
                sb.Append("    AND JC.JOB_CARD_TRN_PK = " + jobCardID + "");

                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion "Calculate_TAX" ''Added by subhransu for tax calculation

        #region "GetRevenueDetails"

        /// <summary>
        /// Gets the revenue details.
        /// </summary>
        /// <param name="actualCost">The actual cost.</param>
        /// <param name="actualRevenue">The actual revenue.</param>
        /// <param name="estimatedCost">The estimated cost.</param>
        /// <param name="estimatedRevenue">The estimated revenue.</param>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="LocationPK">The location pk.</param>
        /// <returns></returns>
        public DataSet GetRevenueDetails(decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, string jobCardPK, int LocationPK = 0)
        {
            //Dim SQL As New System.Text.StringBuilder
            WorkFlow objWF = new WorkFlow();
            //Snigdharani - 10/11/2008 - making the values same as consolidation screen.
            try
            {
                DataSet DS = new DataSet();
                var _with70 = objWF.MyCommand.Parameters;
                _with70.Add("JCPK", jobCardPK).Direction = ParameterDirection.Input;
                _with70.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with70.Add("JOB_EXP_SEA", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "FETCH_JOBCARD_EXP_SEA");
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
            //End Snigdharani
            //Dim actualEstimatedCost As String
            //Dim costArray As String()
            //Dim temporary As String
            //Dim oraReader As OracleDataReader

            //SQL.Append(vbCrLf & "SELECT")
            //SQL.Append(vbCrLf & "      sum(ROUND(q.EstimatedCost * ")
            //SQL.Append(vbCrLf & "      (case when ")
            //SQL.Append(vbCrLf & "                           q.currency_mst_pk = corp.currency_mst_fk")
            //SQL.Append(vbCrLf & "            then")
            //SQL.Append(vbCrLf & "                           1")
            //SQL.Append(vbCrLf & "            else")
            //SQL.Append(vbCrLf & "                           (select")
            //SQL.Append(vbCrLf & "                                   exch.exchange_rate")
            //SQL.Append(vbCrLf & "                            from")
            //SQL.Append(vbCrLf & "                                   exchange_rate_trn exch")
            //SQL.Append(vbCrLf & "                            where")
            //SQL.Append(vbCrLf & "                                   q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //SQL.Append(vbCrLf & "                                   AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //SQL.Append(vbCrLf & "                            )end")
            //SQL.Append(vbCrLf & "         ),4))""Estimated Cost"", ")
            //SQL.Append(vbCrLf & "      sum(ROUND(ActualCost * ")
            //SQL.Append(vbCrLf & "      (case when ")
            //SQL.Append(vbCrLf & "                            q.currency_mst_pk = corp.currency_mst_fk")
            //SQL.Append(vbCrLf & "           then")
            //SQL.Append(vbCrLf & "                            1")
            //SQL.Append(vbCrLf & "           else")
            //SQL.Append(vbCrLf & "                            (select")
            //SQL.Append(vbCrLf & "                                    exch.exchange_rate")
            //SQL.Append(vbCrLf & "                             from")
            //SQL.Append(vbCrLf & "                                    exchange_rate_trn exch")
            //SQL.Append(vbCrLf & "                             where")
            //SQL.Append(vbCrLf & "                                    q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //SQL.Append(vbCrLf & "                                    AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //SQL.Append(vbCrLf & "                            )end ")
            //SQL.Append(vbCrLf & "    ),4)) ""Actual Cost"" ")
            //SQL.Append(vbCrLf & "FROM")
            //SQL.Append(vbCrLf & "    (SELECT")
            //SQL.Append(vbCrLf & "           job_exp.jobcard_date,")
            //SQL.Append(vbCrLf & "           curr.currency_mst_pk,")
            //SQL.Append(vbCrLf & "           SUM(job_trn_pia.Estimated_Amt) EstimatedCost,")
            //SQL.Append(vbCrLf & "           SUM(job_trn_pia.Invoice_Amt) ActualCost")
            //SQL.Append(vbCrLf & "     FROM")
            //SQL.Append(vbCrLf & "           job_trn_sea_exp_pia  job_trn_pia,")
            //SQL.Append(vbCrLf & "           currency_type_mst_tbl curr,")
            //SQL.Append(vbCrLf & "           cost_element_mst_tbl cost_ele,")
            //SQL.Append(vbCrLf & "           JOB_CARD_TRN job_exp")
            //SQL.Append(vbCrLf & "     WHERE")
            //SQL.Append(vbCrLf & "           job_trn_pia.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK")
            //SQL.Append(vbCrLf & "           AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk")
            //SQL.Append(vbCrLf & "           AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk")
            //SQL.Append(vbCrLf & "           AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK)

            //'by Thiyagarajan on 28/3/08 for location based currency : PTS TASK GEN-FEB-003
            //'SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp")
            //SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,")
            //SQL.Append(vbCrLf & "  (select country.currency_mst_fk from country_mst_tbl country where country.country_mst_pk in (select loc.country_mst_fk from ")
            //SQL.Append(vbCrLf & "  location_mst_tbl loc where loc.location_mst_pk=" & LocationPK & ")) corp ")
            //'end

            //oraReader = objWF.GetDataReader(SQL.ToString())

            //While oraReader.Read
            //    If Not (oraReader(0) Is "") Then
            //        estimatedCost = oraReader(0)
            //    End If

            //    If Not (oraReader(1) Is "") Then
            //        actualCost = oraReader(1)
            //    End If

            //End While

            //oraReader.Close()

            //SQL = New System.Text.StringBuilder

            //SQL.Append(vbCrLf & "SELECT  ")
            //SQL.Append(vbCrLf & "   sum(ROUND(q.freight_amt * ")
            //SQL.Append(vbCrLf & "   (case when ")
            //SQL.Append(vbCrLf & "           q.currency_mst_pk = corp.currency_mst_fk")
            //SQL.Append(vbCrLf & "         then ")
            //SQL.Append(vbCrLf & "           1")
            //SQL.Append(vbCrLf & "         else ")
            //SQL.Append(vbCrLf & "           (select ")
            //SQL.Append(vbCrLf & "               exch.exchange_rate")
            //SQL.Append(vbCrLf & "            from")
            //SQL.Append(vbCrLf & "               exchange_rate_trn exch")
            //SQL.Append(vbCrLf & "            where")
            //SQL.Append(vbCrLf & "               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //SQL.Append(vbCrLf & "               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //SQL.Append(vbCrLf & "           )end ")
            //SQL.Append(vbCrLf & "   ),4)) ""Estimated Revenue""")
            //SQL.Append(vbCrLf & "FROM")
            //SQL.Append(vbCrLf & "   (SELECT")
            //SQL.Append(vbCrLf & "       job_exp.jobcard_date,")
            //SQL.Append(vbCrLf & "       curr.currency_mst_pk,")
            //SQL.Append(vbCrLf & "       sum(job_trn_fd.freight_amt) freight_amt")
            //SQL.Append(vbCrLf & "    FROM")
            //SQL.Append(vbCrLf & "       JOB_TRN_FD  job_trn_fd,")
            //SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
            //SQL.Append(vbCrLf & "       JOB_CARD_TRN job_exp")
            //SQL.Append(vbCrLf & "    WHERE")
            //SQL.Append(vbCrLf & "       job_trn_fd.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK")
            //SQL.Append(vbCrLf & "       AND job_trn_fd.currency_mst_fk =curr.currency_mst_pk")
            //SQL.Append(vbCrLf & "       AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK)

            //'by Thiyagarajan on 28/3/08 for location based currency : PTS TASK GEN-FEB-003
            //'SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp")
            //SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,")
            //SQL.Append(vbCrLf & "  (select country.currency_mst_fk from country_mst_tbl country where country.country_mst_pk in (select loc.country_mst_fk from ")
            //SQL.Append(vbCrLf & "  location_mst_tbl loc where loc.location_mst_pk=" & LocationPK & ")) corp ")
            //'end

            //oraReader = objWF.GetDataReader(SQL.ToString())
            //While oraReader.Read
            //    If Not (oraReader(0) Is "") Then
            //        estimatedRevenue = oraReader(0)
            //    End If
            //End While
            //oraReader.Close()

            //'()

            //SQL = New System.Text.StringBuilder

            //SQL.Append(vbCrLf & "SELECT  ")
            //SQL.Append(vbCrLf & "   sum(ROUND(q.freight_amt * ")
            //SQL.Append(vbCrLf & "   (case when ")
            //SQL.Append(vbCrLf & "           q.currency_mst_pk = corp.currency_mst_fk")
            //SQL.Append(vbCrLf & "         then ")
            //SQL.Append(vbCrLf & "           1")
            //SQL.Append(vbCrLf & "         else ")
            //SQL.Append(vbCrLf & "           (select ")
            //SQL.Append(vbCrLf & "               exch.exchange_rate")
            //SQL.Append(vbCrLf & "            from")
            //SQL.Append(vbCrLf & "               exchange_rate_trn exch")
            //SQL.Append(vbCrLf & "            where")
            //SQL.Append(vbCrLf & "               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //SQL.Append(vbCrLf & "               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //SQL.Append(vbCrLf & "           )end ")
            //SQL.Append(vbCrLf & "   ),4)) ""Estimated Revenue""")
            //SQL.Append(vbCrLf & "FROM")
            //SQL.Append(vbCrLf & "   (SELECT")
            //SQL.Append(vbCrLf & "       job_exp.jobcard_date,")
            //SQL.Append(vbCrLf & "       curr.currency_mst_pk,")
            //SQL.Append(vbCrLf & "       sum(job_trn_othr.amount) freight_amt")
            //SQL.Append(vbCrLf & "    FROM")
            //SQL.Append(vbCrLf & "       job_trn_oth_chrg  job_trn_othr,")
            //SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
            //SQL.Append(vbCrLf & "       JOB_CARD_TRN job_exp")
            //SQL.Append(vbCrLf & "    WHERE")
            //SQL.Append(vbCrLf & "       job_trn_othr.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK")
            //SQL.Append(vbCrLf & "       AND job_trn_othr.currency_mst_fk =curr.currency_mst_pk")
            //SQL.Append(vbCrLf & "       AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK)

            //'by Thiyagarajan on 28/3/08 for location based currency : PTS TASK GEN-FEB-003
            //'SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp")
            //SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,")
            //SQL.Append(vbCrLf & "  (select country.currency_mst_fk from country_mst_tbl country where country.country_mst_pk in (select loc.country_mst_fk from ")
            //SQL.Append(vbCrLf & "  location_mst_tbl loc where loc.location_mst_pk=" & LocationPK & ")) corp ")
            //'end

            //oraReader = objWF.GetDataReader(SQL.ToString())
            //Dim Temp As Int32
            //While oraReader.Read
            //    If Not (oraReader(0) Is "") Then
            //        Temp = oraReader(0)
            //        estimatedRevenue = estimatedRevenue + oraReader(0)
            //    End If
            //End While
            //oraReader.Close()

            //'SQL = New System.Text.StringBuilder

            //'SQL.Append(vbCrLf & "SELECT  ")
            //'SQL.Append(vbCrLf & "   sum(ROUND(q.actual_revenue * ")
            //'SQL.Append(vbCrLf & "   (case when")
            //'SQL.Append(vbCrLf & "           q.currency_mst_pk = corp.currency_mst_fk")
            //'SQL.Append(vbCrLf & "    then")
            //'SQL.Append(vbCrLf & "           1")
            //'SQL.Append(vbCrLf & "    else")
            //'SQL.Append(vbCrLf & "           (select")
            //'SQL.Append(vbCrLf & "                  exch.exchange_rate")
            //'SQL.Append(vbCrLf & "            from")
            //'SQL.Append(vbCrLf & "                  exchange_rate_trn exch")
            //'SQL.Append(vbCrLf & "            where")
            //'SQL.Append(vbCrLf & "                  q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //'SQL.Append(vbCrLf & "                  AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //'SQL.Append(vbCrLf & "           )end ")
            //'SQL.Append(vbCrLf & "   ),4)) ""Actual Revenue""")
            //'SQL.Append(vbCrLf & "FROM")
            //'SQL.Append(vbCrLf & "   (SELECT")
            //'SQL.Append(vbCrLf & "       job_exp.jobcard_date,")
            //'SQL.Append(vbCrLf & "       curr.currency_mst_pk,")
            //'SQL.Append(vbCrLf & "       sum (nvl(inv_cust.invoice_amt,0) + nvl(inv_cust.vat_amt,0) - nvl(inv_cust.discount_amt,0) ) actual_revenue")
            //'SQL.Append(vbCrLf & "   FROM")
            //'SQL.Append(vbCrLf & "       inv_cust_sea_exp_tbl inv_cust,")
            //'SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
            //'SQL.Append(vbCrLf & "       JOB_CARD_TRN job_exp")
            //'SQL.Append(vbCrLf & "   WHERE")
            //'SQL.Append(vbCrLf & "       inv_cust.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK")
            //'SQL.Append(vbCrLf & "       AND inv_cust.currency_mst_fk =curr.currency_mst_pk")
            //'SQL.Append(vbCrLf & "       AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK)
            //'SQL.Append(vbCrLf & "   GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp")
            //Try
            //    objWF.MyCommand.Parameters.Clear()
            //    With objWF.MyCommand.Parameters
            //        .Add("JOBCARD_PK", jobCardPK).Direction = ParameterDirection.Input
            //        '.Add("LocationsPk", LocationPK).Direction = ParameterDirection.Input 'adding by Thiyagarajan for location based curr.
            //        'Commented By ANAND AS Location Based Currency is not moved to eqa
            //        .Add("JOB_SEA_EXP_CUR", OracleClient.OracleDbType.RefCursor).Direction = ParameterDirection.Output
            //    End With
            //    'Return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_EXP")
            //Catch sqlExp As Exception
            //    Throw sqlExp
            //End Try

            //oraReader = objWF.GetDataReader("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_EXP_ACTREV")
            //While oraReader.Read
            //    If Not (oraReader(0) Is "") Then
            //        actualRevenue = oraReader(0)
            //    End If
            //End While
            //oraReader.Close()

            //'temporary = objWF.ExecuteScaler(SQL.ToString)
            //'If temporary <> "" Then
            //'    actualRevenue = CDec(temporary)
            //'End If

            //Try
            //    Return True
            //Catch sqlExp As OracleException
            //    ErrorMessage = sqlExp.Message
            //    Throw sqlExp
            //Catch exp As Exception
            //    ErrorMessage = exp.Message
            //    Throw exp
            //End Try
        }

        #endregion "GetRevenueDetails"

        #region "Get Invoice PK"

        //0 - means no invoice exists.
        //-1 - means more then one invoice extists
        //pk value of invoice

        //invice Type: 1-Invoice to customer.
        //invice Type: 2-Invoice to CB Agent.
        //invice Type: 3-Invoice to DP Agent.
        /// <summary>
        /// Gets the customer invoice.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="invoiceType">Type of the invoice.</param>
        /// <returns></returns>
        public long GetCustInvoice(string jobCardPK, Int16 invoiceType = 1)
        {
            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;
            int invoiceCount = 0;
            long invoicePK = 0;

            if (invoiceType == 1)
            {
                SQL.Append("select i.inv_cust_sea_exp_pk from inv_cust_sea_exp_tbl i where i.job_card_sea_exp_fk = " + jobCardPK);
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK");
                sb.Append("  FROM JOB_CARD_TRN   JOB,");
                sb.Append("       CONSOL_INVOICE_TBL     INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN ");
                sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
                sb.Append("   AND INVTRN.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK");
                sb.Append("   AND INV.BUSINESS_TYPE = 2 AND INV.PROCESS_TYPE = 1");
                sb.Append("   AND INVTRN.JOB_TYPE=1");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + jobCardPK);
                SQL.Append(" UNION " + sb.ToString());
            }
            else if (invoiceType == 2)
            {
                SQL.Append("select i.inv_agent_pk from inv_agent_tbl i where i.CB_DP_LOAD_AGENT=1 AND  i.job_card_fk in (" + jobCardPK + ")");
            }
            else if (invoiceType == 3)
            {
                SQL.Append("select i.inv_agent_pk from inv_agent_tbl i where  i.CB_DP_LOAD_AGENT=2 AND i.job_card_fk = " + jobCardPK);
            }

            oraReader = objWF.GetDataReader(SQL.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    invoicePK = Convert.ToInt64(oraReader[0]);
                    invoiceCount += 1;
                }
            }

            if (invoiceCount == 0)
            {
                return 0;
            }
            else if (invoiceCount > 1)
            {
                return -1;
            }
            else
            {
                return invoicePK;
            }

            oraReader.Close();

            try
            {
                return invoicePK;
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
        /// Updates the places reference delete.
        /// </summary>
        /// <param name="SQLQuery">The SQL query.</param>
        public void UpdatePlacesRefDel(string SQLQuery)
        {
            WorkFlow objWF = new WorkFlow();
            objWF.ExecuteScaler(SQLQuery);
        }

        /// <summary>
        /// Gets the customer invoice CBJC.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <returns></returns>
        public long GetCustInvoiceCBJC(string jobCardPK, Int16 JobType)
        {
            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;
            int invoiceCount = 0;
            long invoicePK = 0;

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK");
            if (JobType == 2)
            {
                sb.Append("  FROM CBJC_TBL   JOB,");
            }
            else if (JobType == 3)
            {
                sb.Append("  FROM TRANSPORT_INST_SEA_TBL   JOB,");
            }
            sb.Append("       CONSOL_INVOICE_TBL     INV,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN ");
            sb.Append(" WHERE INVTRN.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK");
            if (JobType == 2)
            {
                sb.Append("   AND JOB.CBJC_PK = INVTRN.JOB_CARD_FK");
                sb.Append("   AND INVTRN.JOB_TYPE=2");
                sb.Append("   AND JOB.CBJC_PK = " + jobCardPK);
            }
            else if (JobType == 3)
            {
                sb.Append("   AND JOB.TRANSPORT_INST_SEA_PK = INVTRN.JOB_CARD_FK");
                sb.Append("   AND INVTRN.JOB_TYPE=3");
                sb.Append("   AND JOB.TRANSPORT_INST_SEA_PK = " + jobCardPK);
            }

            oraReader = objWF.GetDataReader(sb.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    invoicePK = Convert.ToInt64(oraReader[0].ToString());
                    invoiceCount += 1;
                }
            }

            if (invoiceCount == 0)
            {
                return 0;
            }
            else if (invoiceCount > 1)
            {
                return -1;
            }
            else
            {
                return invoicePK;
            }

            oraReader.Close();

            try
            {
                return invoicePK;
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

        #endregion "Get Invoice PK"

        #region "Fill Combo"

        /// <summary>
        /// Fills the cargo move code.
        /// </summary>
        /// <returns></returns>
        public DataSet FillCargoMoveCode()
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT");
            strSQL.Append("      c.cargo_move_pk,");
            strSQL.Append("      c.cargo_move_code");
            strSQL.Append(" FROM");
            strSQL.Append("      cargo_move_mst_tbl c");
            strSQL.Append(" WHERE");
            strSQL.Append("      c.active_flag = 1");
            strSQL.Append(" ORDER BY cargo_move_code");

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

        /// <summary>
        /// Fills the shipping terms combo.
        /// </summary>
        /// <returns></returns>
        public DataSet FillShippingTermsCombo()
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("SELECT");
            strSQL.Append("      s.shipping_terms_mst_pk,");
            strSQL.Append("      s.inco_code ");
            strSQL.Append(" FROM");
            strSQL.Append("      shipping_terms_mst_tbl s ");
            strSQL.Append(" WHERE");
            strSQL.Append("      s.active_flag = 1 ");
            strSQL.Append(" ORDER BY inco_code");

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

        #endregion "Fill Combo"

        #region "Fetch Revenue data export"

        /// <summary>
        /// Fetches the revenue data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchRevenueData(string jobCardPK = "0")
        {
            //Dim strSQL As StringBuilder = New StringBuilder
            WorkFlow objWF = new WorkFlow();

            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with71 = objWF.MyCommand.Parameters;
                _with71.Add("JOBCARD_PK", jobCardPK).Direction = ParameterDirection.Input;
                _with71.Add("JOB_SEA_EXP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_EXP");
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Revenue data export"

        #region "GenerateUCRNumber"

        /// <summary>
        /// Generates the ucr number.
        /// </summary>
        /// <param name="customerID">The customer identifier.</param>
        /// <returns></returns>
        public string GenerateUCRNumber(string customerID)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL.Append("SELECT (SELECT TO_CHAR(SYSDATE,'yy')  FROM dual)||country.country_id||cmt.vat_no");
                strSQL.Append("    FROM");
                strSQL.Append("    customer_contact_dtls cust_det,");
                strSQL.Append("    customer_mst_tbl cmt,");
                strSQL.Append("    country_mst_tbl country,");
                strSQL.Append("    location_mst_tbl lmt");
                strSQL.Append("    WHERE");
                strSQL.Append("    cmt.customer_mst_pk =" + customerID);
                strSQL.Append("    AND cust_det.customer_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append("    AND cust_det.adm_location_mst_fk = lmt.location_mst_pk(+)");
                strSQL.Append("    AND lmt.country_mst_fk = country.country_mst_pk(+)");

                return Convert.ToString(objWF.ExecuteScaler(strSQL.ToString()));
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
        /// Fills the container type data set.
        /// </summary>
        /// <param name="isBooking">if set to <c>true</c> [is booking].</param>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet FillContainerTypeDataSet(bool isBooking, string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            if (isBooking)
            {
                strSQL.Append(" SELECT  distinct");
                strSQL.Append(" cont.container_type_mst_pk,");
                strSQL.Append(" cont.container_type_mst_id,");
                strSQL.Append(" cont.preferences");
                strSQL.Append(" FROM");
                strSQL.Append(" BOOKING_MST_TBL book,");
                strSQL.Append(" BOOKING_TRN booking_trn,");
                strSQL.Append(" container_type_mst_tbl cont");
                strSQL.Append(" WHERE");
                strSQL.Append(" booking_trn.booking_sea_fk = " + pk);
                strSQL.Append(" AND booking_trn.container_type_mst_fk = cont.container_type_mst_pk");
                strSQL.Append(" AND book.booking_sea_pk = booking_trn.booking_sea_fk");
                strSQL.Append(" ORDER BY cont.preferences");
            }
            else
            {
                strSQL.Append(" SELECT distinct");
                strSQL.Append(" cont.container_type_mst_pk,");
                strSQL.Append(" cont.container_type_mst_id,");
                strSQL.Append(" cont.preferences");
                strSQL.Append(" FROM");
                strSQL.Append(" JOB_TRN_CONT job_trn,");
                strSQL.Append(" container_type_mst_tbl cont");
                strSQL.Append(" WHERE");
                strSQL.Append(" job_trn.container_type_mst_fk = cont.container_type_mst_pk");
                strSQL.Append(" and job_trn.job_card_sea_exp_fk =" + pk);
                strSQL.Append(" ORDER BY cont.preferences");
            }

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

        /// <summary>
        /// Fills the booking other charges data set.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet FillBookingOtherChargesDataSet(string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("SELECT");
            strSQL.Append("         '' JOB_TRN_SEA_EXP_OTH_PK,");
            strSQL.Append("         frt.freight_element_mst_pk,");
            strSQL.Append("         frt.freight_element_id,");
            strSQL.Append("         frt.freight_element_name,");
            strSQL.Append("         curr.currency_mst_pk, '' \"ROE\",");
            strSQL.Append("         oth_chrg.amount amount,");
            strSQL.Append("         'false' \"Delete\", 1 \"Print\" ");
            strSQL.Append("FROM");
            strSQL.Append("         BOOKING_TRN_OTH_CHRG oth_chrg,");
            strSQL.Append("         BOOKING_MST_TBL  booking_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr");
            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.booking_sea_fk = booking_mst.booking_sea_pk");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.booking_sea_fk         = " + pk);
            strSQL.Append("ORDER BY freight_element_id ");

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

        /// <summary>
        /// Fills the job card other charges data set.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="baseCurrency">The base currency.</param>
        /// <param name="CheckBkgJC">The check BKG jc.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <returns></returns>
        public DataSet FillJobCardOtherChargesDataSet(string pk = "0", Int64 baseCurrency = 1, Int16 CheckBkgJC = 0, string BKGPK = "")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            if (CheckBkgJC == 0)
            {
                strSQL.Append("         SELECT");
                strSQL.Append("         oth_chrg.job_trn_oth_pk,");
                strSQL.Append("         frt.freight_element_mst_pk,");
                strSQL.Append("         frt.freight_element_id,");
                strSQL.Append("         frt.freight_element_name,");
                strSQL.Append("         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') Payment_Type, ");
                strSQL.Append("         oth_chrg.location_mst_fk,");
                strSQL.Append("         lmt.location_id ,");
                strSQL.Append("         oth_chrg.frtpayer_cust_mst_fk,");
                strSQL.Append("         cmt.customer_id,");
                strSQL.Append("         curr.currency_id currency_mst_pk, ");
                strSQL.Append("         oth_chrg.exchange_rate ROE, ");
                strSQL.Append("         oth_chrg.amount amount,");
                strSQL.Append("         'false' \"Delete\", oth_chrg.PRINT_ON_MBL \"Print\" ");
                strSQL.Append("FROM");
                strSQL.Append("         job_trn_oth_chrg oth_chrg,");
                strSQL.Append("         JOB_CARD_TRN jobcard_mst,");
                strSQL.Append("         freight_element_mst_tbl frt,");
                strSQL.Append("         currency_type_mst_tbl curr,");
                strSQL.Append("         location_mst_tbl lmt,");
                strSQL.Append("         customer_mst_tbl cmt");
                strSQL.Append("WHERE");
                strSQL.Append("         oth_chrg.job_card_trn_fk = jobcard_mst.JOB_CARD_TRN_PK");
                strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
                strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
                strSQL.Append("         AND oth_chrg.location_mst_fk = lmt.location_mst_pk (+)");
                strSQL.Append("         AND oth_chrg.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append("         AND oth_chrg.job_card_trn_fk    = " + pk);
                strSQL.Append("ORDER BY freight_element_id ");
            }
            else
            {
                strSQL.Append("         SELECT");
                strSQL.Append("         '' JOB_TRN_OTH_PK,");
                strSQL.Append("         frt.freight_element_mst_pk,");
                strSQL.Append("         frt.freight_element_id,");
                strSQL.Append("         frt.freight_element_name,");
                strSQL.Append("         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') Payment_Type, ");
                strSQL.Append("         '' location_mst_fk,");
                strSQL.Append("         '' location_id ,");
                strSQL.Append("         booking_mst.cust_customer_mst_fk frtpayer_cust_mst_fk,");
                strSQL.Append("         cmt.customer_id,");
                strSQL.Append("         curr.currency_id currency_mst_pk, ");
                strSQL.Append("    ROUND(GET_EX_RATE(oth_chrg.currency_mst_fk," + baseCurrency + ",round(sysdate - .5)),4) AS ROE ,");
                strSQL.Append("         oth_chrg.amount amount,");
                strSQL.Append("         'false' \"Delete\", 1 \"Print\" ");
                strSQL.Append("FROM");
                strSQL.Append("         BOOKING_TRN_OTH_CHRG oth_chrg,");
                strSQL.Append("         BOOKING_MST_TBL booking_mst,");
                strSQL.Append("         freight_element_mst_tbl frt,");
                strSQL.Append("         currency_type_mst_tbl curr,");
                strSQL.Append("         customer_mst_tbl cmt");
                strSQL.Append("WHERE");
                strSQL.Append("         oth_chrg.booking_mst_fk = booking_mst.booking_mst_pk");
                strSQL.Append("         and booking_mst.cust_customer_mst_fk=cmt.customer_mst_pk");
                strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
                strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
                strSQL.Append("         AND oth_chrg.booking_mst_fk    = " + BKGPK);
                strSQL.Append("ORDER BY freight_element_id ");
            }
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

        /// <summary>
        /// Gets the currency details.
        /// </summary>
        /// <param name="Currency">The currency.</param>
        /// <returns></returns>
        public DataSet GetCurrencyDetails(string Currency = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWf = new WorkFlow();
            sb.Append("SELECT C.CURRENCY_MST_PK,");
            sb.Append("       C.CURRENCY_ID,");
            sb.Append("       C.CURRENCY_NAME,");
            sb.Append("       C.ACTIVE_FLAG,");
            sb.Append("       C.CREATED_BY_FK,");
            sb.Append("       C.CREATED_DT,");
            sb.Append("       C.LAST_MODIFIED_BY_FK,");
            sb.Append("       C.LAST_MODIFIED_DT,");
            sb.Append("       C.VERSION_NO");
            sb.Append("  FROM CURRENCY_TYPE_MST_TBL C");
            sb.Append("     WHERE 1=1 ");
            try
            {
                Currency = Convert.ToString(Currency);
                sb.Append("     AND C.CURRENCY_MST_PK = " + Currency);
            }
            catch (Exception ex)
            {
                sb.Append("     AND UPPER(C.CURRENCY_ID) ='" + Currency.ToUpper() + "' ");
            }
            try
            {
                return objWf.GetDataSet(sb.ToString());
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        /// <summary>
        /// Fills the jc oth CHRG.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet fillJcOthChrg(string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("         SELECT");
            strSQL.Append("         oth_chrg.inv_cust_trn_sea_exp_fk,");
            strSQL.Append("         oth_chrg.inv_agent_trn_fk,");
            strSQL.Append("         oth_chrg.consol_invoice_trn_fk");
            strSQL.Append("FROM");
            strSQL.Append("         job_trn_oth_chrg oth_chrg,");
            strSQL.Append("         JOB_CARD_TRN jobcard_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr");
            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.job_card_sea_exp_fk = jobcard_mst.JOB_CARD_TRN_PK");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.job_card_sea_exp_fk    = " + pk);
            strSQL.Append("ORDER BY freight_element_id ");

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

        //added by surya prasad for implementing multiple commodity task
        /// <summary>
        /// Fetches the booking pk.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <returns></returns>
        public string FetchBookingPk(string Jobpk)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append(" select bkg.booking_sea_pk from ");
            strSQL.Append(" BOOKING_MST_TBL bkg, ");
            strSQL.Append(" JOB_CARD_TRN  jsea ");
            strSQL.Append(" where jsea.booking_sea_fk = bkg.booking_sea_pk ");
            strSQL.Append(" and jsea.JOB_CARD_TRN_PK = " + Jobpk + " ");
            try
            {
                return objWF.ExecuteScaler(strSQL.ToString());
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
        /// Fetches the esi FLG.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <returns></returns>
        public int FetchEsiFlg(string Jobpk)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append("SELECT JC.ESI_FLAG ");
            strSQL.Append(" FROM JOB_CARD_TRN JC WHERE JC.JOB_CARD_TRN_PK =" + Jobpk + "  ");
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
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

        //end
        /// <summary>
        /// Fetches the cost ele desc.
        /// </summary>
        /// <param name="CostElefk">The cost elefk.</param>
        /// <returns></returns>
        public string FetchCostEleDesc(string CostElefk)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append("SELECT CMT.COST_ELEMENT_NAME ");
            strSQL.Append(" FROM COST_ELEMENT_MST_TBL CMT");
            strSQL.Append(" WHERE CMT.COST_ELEMENT_MST_PK= " + CostElefk + "");
            try
            {
                return objWF.ExecuteScaler(strSQL.ToString());
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

        #endregion "GenerateUCRNumber"

        #region "Certificate of Insurance -- Exports Sea"

        /// <summary>
        /// Fetches the cis exp sea.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
        public DataSet FetchCISExpSea(Int32 JobPk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();

            Strsql = "  SELECT JS.JOB_CARD_TRN_PK AS JOBPK,";
            Strsql += " JS.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += " JS.VESSEL_NAME AS CONVEYANCE,";
            Strsql += " COLL.PLACE_NAME AS FRM,";
            Strsql += " DEL.PLACE_NAME AS VIATO,";
            Strsql += " NVL(JS.INSURANCE_AMT,0) AS INSUREDVALUE, ";
            Strsql += " C.CURRENCY_NAME,";
            Strsql += " H.MARKS_NUMBERS,";
            Strsql += " H.GOODS_DESCRIPTION AS INTEREST ,";
            Strsql += " SHP.CUSTOMER_NAME AS SHIPPER";
            Strsql += " FROM JOB_CARD_TRN JS,";
            Strsql += " BOOKING_MST_TBL BS,";
            Strsql += " HBL_EXP_TBL H,";
            Strsql += " PLACE_MST_TBL COLL,";
            Strsql += " PLACE_MST_TBL DEL,";
            Strsql += " CURRENCY_TYPE_MST_TBL C,";
            Strsql += " CUSTOMER_MST_TBL SHP";
            Strsql += " WHERE JS.BOOKING_SEA_FK = BS.BOOKING_SEA_PK";
            Strsql += " AND H.JOB_CARD_SEA_EXP_FK(+)=JS.JOB_CARD_TRN_PK";
            Strsql += " AND COLL.PLACE_PK(+)=BS.COL_PLACE_MST_FK";
            Strsql += " AND DEL.PLACE_PK(+)=BS.DEL_PLACE_MST_FK";
            Strsql += " AND C.CURRENCY_MST_PK(+)=JS.INSURANCE_CURRENCY";
            Strsql += " AND SHP.CUSTOMER_MST_PK(+)=JS.SHIPPER_CUST_MST_FK";
            Strsql += " AND JS.JOB_CARD_TRN_PK=" + JobPk;
            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Certificate of Insurance -- Exports Sea"

        #region " Fetch base currency Exchange rate export"

        /// <summary>
        /// Fetches the roe.
        /// </summary>
        /// <param name="baseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet FetchROE(Int64 baseCurrency)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    CURR.CURRENCY_MST_PK,");
                strSQL.Append("    CURR.CURRENCY_ID,");
                //"corporate_mst_tbl" removed by thiyagarajan on 21/11/08 for location based currency task
                strSQL.Append("    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(sysdate - .5)),6) AS ROE");
                strSQL.Append("FROM");
                strSQL.Append("    CURRENCY_TYPE_MST_TBL CURR");
                strSQL.Append("WHERE");
                strSQL.Append("    CURR.ACTIVE_FLAG = 1");

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

        #endregion " Fetch base currency Exchange rate export"

        #region " Fetch base currency Exchange rate export"

        /// <summary>
        /// Fetches the vessel voyage roe.
        /// </summary>
        /// <param name="voyage">The voyage.</param>
        /// <returns></returns>
        public DataSet FetchVesselVoyageROE(Int64 voyage)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                //modified by thiyagarajan on 2/12/08 for location based currency task
                strSQL = " SELECT C.CURRENCY_MST_PK," + " 1  ROE" + " FROM CURRENCY_TYPE_MST_TBL C" + " WHERE C.CURRENCY_MST_PK =" + HttpContext.Current.Session["currency_mst_pk"] + " AND C.ACTIVE_FLAG = 1" + " UNION" + " SELECT CURR.CURRENCY_MST_PK," + " EXCHANGE_RATE ROE" + " FROM CURRENCY_TYPE_MST_TBL CURR, Exchange_Rate_Trn EXC" + " WHERE EXC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK" + " AND CURR.ACTIVE_FLAG = 1" + " AND EXC.Voyage_Trn_Fk is not null" + " AND exc.voyage_trn_fk = " + voyage + " AND EXC.CURRENCY_MST_BASE_FK =" + HttpContext.Current.Session["currency_mst_pk"] + " AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK" + " UNION" + " SELECT T.CURRENCY_MST_PK," + " TO_NUMBER(NULL) ROE" + " FROM CURRENCY_TYPE_MST_TBL T " + " WHERE T.CURRENCY_MST_PK NOT IN" + " ( SELECT CURRENCY_MST_PK" + " FROM CURRENCY_TYPE_MST_TBL , Exchange_Rate_Trn " + " WHERE CURRENCY_MST_FK = CURRENCY_MST_PK" + " AND ACTIVE_FLAG = 1" + " AND voyage_trn_fk = " + voyage + " AND CURRENCY_MST_BASE_FK = " + HttpContext.Current.Session["currency_mst_pk"] + " AND CURRENCY_MST_BASE_FK <> CURRENCY_MST_FK )" + " AND T.CURRENCY_MST_PK <> " + HttpContext.Current.Session["currency_mst_pk"];
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

        #endregion " Fetch base currency Exchange rate export"

        #region "Fetch Freight Type"

        /// <summary>
        /// Fetches the type of the FRT.
        /// </summary>
        /// <param name="baseFrt">The base FRT.</param>
        /// <returns></returns>
        public DataSet FetchFrtType(Int64 baseFrt = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    STMT.SHIPPING_TERMS_MST_PK, STMT.FREIGHT_TYPE");
                strSQL.Append("FROM");
                strSQL.Append("    SHIPPING_TERMS_MST_TBL STMT");

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

        #endregion "Fetch Freight Type"

        #region " Fetch Data for Standard Shipping Note"

        /// <summary>
        /// Fetches the SSN.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchSSN(string JOBPK)
        {
            string Strsql = null;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " SELECT ";
                Strsql += " JAE.JOB_CARD_TRN_PK JOBPK,                     ";
                Strsql += " JAE.JOBCARD_REF_NO JOBREFNO,                       ";
                Strsql += " BAT.BOOKING_MST_PK BKGPK,                          ";
                Strsql += " BAT.BOOKING_REF_NO BKGREFNO,                       ";
                Strsql += " BAT.BOOKING_DATE BKGDATE, ";
                Strsql += " JAE.SHIPPER_CUST_MST_FK EXPORTERPK,";
                Strsql += " CUSTOMER.CUSTOMER_NAME EXPORTERNAME,";
                Strsql += " CUSTDTLS.ADM_ADDRESS_1 EXPORTERADD1,";
                Strsql += " CUSTDTLS.ADM_ADDRESS_2 EXPORTERADD2,";
                Strsql += " CUSTDTLS.ADM_ADDRESS_3 EXPORTERADD3,";
                Strsql += " CUSTDTLS.ADM_CITY EXPORTERCITY,";
                Strsql += " CUSTDTLS.ADM_LOCATION_MST_FK EXPORTERLOCFK,";
                Strsql += " EXPORTERLOC.LOCATION_NAME EXPORTERLOCNAME,";
                Strsql += " CUSTDTLS.ADM_COUNTRY_MST_FK EXPORTERCOUNTRYFK,";
                Strsql += " EXPORTERCOUNTRY.COUNTRY_NAME EXPORTERCOUNTRYNAME,";
                Strsql += " BAT.CUSTOMER_REF_NO EXPORTERSREF,";
                Strsql += " CUSTDTLS.ADM_ZIP_CODE EXPORTERZIP,";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_1 EXPORTERPHONE1,";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_2 EXPORTERPHONE2,";
                Strsql += " CUSTDTLS.ADM_FAX_NO     EXPORTERFAX,";
                Strsql += " CUSTDTLS.ADM_EMAIL_ID   EXPORTEREMAIL,";
                Strsql += " CUSTDTLS.ADM_URL        EXPORTERURL,";
                Strsql += " ' ' CUSTOMSFK,                 ";
                Strsql += " JAE.UCR_NO  CUSTOMSCODE,       ";
                Strsql += " BAT.CUSTOMER_REF_NO EXPORTERREF,                                   ";
                Strsql += " CORP.CORPORATE_NAME CORPNAME,                      ";
                Strsql += " CORP.ADDRESS_LINE1 CORPADD1,                       ";
                Strsql += " CORP.ADDRESS_LINE2 CORPADD2,                       ";
                Strsql += " CORP.ADDRESS_LINE3 CORPADD3,                       ";
                Strsql += " CORP.CITY          CORPCITY,  ";
                Strsql += " CORP.STATE_MST_FK CORPSTATEFK,";
                Strsql += " STATE.STATE_NAME CORPSTATENAME,                         ";
                Strsql += " CORP.COUNTRY_MST_FK CORPCOUNTRYFK,";
                Strsql += " COUNTRY.COUNTRY_NAME CORPCOUNTRY,                  ";
                Strsql += " CORP.POST_CODE       CORPZIP, ";
                Strsql += " CORP.PHONE           CORPPHONE,";
                Strsql += " CORP.FAX             CORPFAX,";
                Strsql += " CORP.EMAIL           CORPEMAIL,";
                Strsql += " CORP.HOME_PAGE       CORPURL,                     ";
                Strsql += " BAT.CARRIER_MST_FK INTLCARRFK,                     ";
                Strsql += " OPERAT.OPERATOR_NAME INTLCARRNAME,                 ";
                Strsql += " ' ' OTHUKTRANS,                                    ";
                Strsql += " (JAE.VESSEL_NAME || '/' || JAE.VOYAGE_FLIGHT_NO) VSL_OR_FLIGHT_NO,                    ";
                Strsql += " TO_CHAR(JAE.ETD_DATE,'" + dateFormat + "') VSL_OR_FLIGHT_DATE ,                           ";
                Strsql += " BAT.PORT_MST_POL_FK PORTOFLANDFK ,                 ";
                Strsql += " PORTOFLANDING.PORT_NAME PORTOFLANDNAME,            ";
                Strsql += " BAT.PORT_MST_POD_FK PORTOFDISCHFK,                 ";
                Strsql += " PORTOFDISCHARGE.PORT_NAME PORTOFDISCHNAME,         ";
                Strsql += " BAT.DEL_PLACE_MST_FK DELPLACEFK,                   ";
                Strsql += " PLD.PLACE_NAME DELPLACENAME,                       ";
                Strsql += " (CASE                                              ";
                Strsql += " WHEN JAE.MARKS_NUMBERS IS NOT NULL THEN          ";
                Strsql += " jAE.MARKS_NUMBERS";
                Strsql += " ELSE                                               ";
                Strsql += " JAE.MARKS_NUMBERS";
                Strsql += " END) MARKS,                                        ";
                Strsql += " (CASE                                              ";
                Strsql += " WHEN JAE.GOODS_DESCRIPTION IS NOT NULL THEN          ";
                Strsql += " JAE.GOODS_DESCRIPTION";
                Strsql += " ELSE                                               ";
                Strsql += " JAE.GOODS_DESCRIPTION";
                Strsql += " END) GOODS,                                        ";
                Strsql += " SUM(JTAEC.GROSS_WEIGHT) GROSSWT,                   ";
                Strsql += " SUM(JTAEC.VOLUME_IN_CBM) VOL_IN_CBM,               ";
                Strsql += " SUM(JTAEC.GROSS_WEIGHT) TOTGROSSWT,                ";
                Strsql += " SUM(JTAEC.VOLUME_IN_CBM) TOT_VOL_IN_CBM,";
                Strsql += " JTAEC.CONTAINER_NUMBER CONTAINERNO,                    ";
                Strsql += " JTAEC.SEAL_NUMBER SEALNO,";
                Strsql += " COUNT(CONTAINER.CONTAINER_TYPE_MST_ID) || ' X ' || CONTAINER.CONTAINER_TYPE_MST_ID CONTAINERSIZE, ";
                Strsql += " CONTAINER.CONTAINER_TAREWEIGHT_TONE TAREWT,                                        ";
                Strsql += " JTAEC.PACK_COUNT TOTOFBOXES,                       ";
                Strsql += " CORP.CORPORATE_NAME NAMEOFCOMP,                    ";
                Strsql += " TO_CHAR(SYSDATE,'" + dateFormat + "') TODAYSDATE ,         ";
                Strsql += " JAE.TRANSPORTER_CARRIER_FK HAULIERFK,              ";
                Strsql += " TRANSPORTER.VENDOR_NAME HAULIERNAME,               ";
                Strsql += " ' ' VEHICLEREGNO                                   ";
                Strsql += " FROM JOB_CARD_TRN    JAE,                  ";
                Strsql += " JOB_TRN_CONT    JTAEC,                     ";
                Strsql += " BOOKING_MST_TBL         BAT,                       ";
                Strsql += " PLACE_MST_TBL           PLD,                       ";
                Strsql += " COMMODITY_GROUP_MST_TBL CGMST,                     ";
                Strsql += " CORPORATE_MST_TBL       CORP,                      ";
                Strsql += " COUNTRY_MST_TBL         COUNTRY,                   ";
                Strsql += " PORT_MST_TBL            PORTOFLANDING,             ";
                Strsql += " PORT_MST_TBL            PORTOFDISCHARGE,           ";
                Strsql += " VENDOR_MST_TBL TRANSPORTER,";
                Strsql += " STATE_MST_TBL STATE,";
                Strsql += " CUSTOMER_MST_TBL CUSTOMER,";
                Strsql += " CUSTOMER_CONTACT_DTLS CUSTDTLS,";
                Strsql += " LOCATION_MST_TBL EXPORTERLOC,";
                Strsql += " COUNTRY_MST_TBL EXPORTERCOUNTRY,";
                Strsql += " OPERATOR_MST_TBL OPERAT,";
                Strsql += " CONTAINER_TYPE_MST_TBL CONTAINER";
                Strsql += " WHERE JAE.JOB_CARD_TRN_PK IN(" + JOBPK + " )   ";
                Strsql += " AND JTAEC.JOB_CARD_TRN_FK(+) = JAE.JOB_CARD_TRN_PK";
                Strsql += " AND JAE.BOOKING_MST_FK = BAT.BOOKING_MST_PK(+)";
                Strsql += " AND BAT.CARRIER_MST_FK = OPERAT.OPERATOR_MST_PK(+)";
                Strsql += " AND JAE.COMMODITY_GROUP_FK = CGMST.COMMODITY_GROUP_PK(+)";
                Strsql += " AND CORP.COUNTRY_MST_FK     = COUNTRY.COUNTRY_MST_PK(+)";
                Strsql += " AND BAT.PORT_MST_POL_FK     = PORTOFLANDING.PORT_MST_PK(+)";
                Strsql += " AND BAT.PORT_MST_POD_FK     = PORTOFDISCHARGE.PORT_MST_PK(+)";
                Strsql += " AND BAT.DEL_PLACE_MST_FK    = PLD.PLACE_PK(+)";
                Strsql += " AND JAE.TRANSPORTER_CARRIER_FK = TRANSPORTER.VENDOR_MST_PK(+)";
                Strsql += " AND CORP.STATE_MST_FK          = STATE.STATE_MST_PK(+)";
                Strsql += " AND JAE.SHIPPER_CUST_MST_FK    = CUSTOMER.CUSTOMER_MST_PK";
                Strsql += " AND CUSTDTLS.CUSTOMER_MST_FK   = CUSTOMER.CUSTOMER_MST_PK(+)";
                Strsql += " AND CUSTDTLS.ADM_LOCATION_MST_FK = EXPORTERLOC.LOCATION_MST_PK(+)";
                Strsql += " AND CUSTDTLS.ADM_COUNTRY_MST_FK  = EXPORTERCOUNTRY.COUNTRY_MST_PK(+)";
                Strsql += " AND JTAEC.CONTAINER_TYPE_MST_FK  = CONTAINER.CONTAINER_TYPE_MST_PK(+)";
                Strsql += " and CGMST.COMMODITY_GROUP_CODE NOT LIKE 'DGS'   ";
                Strsql += " GROUP BY JAE.JOB_CARD_TRN_PK,               ";
                Strsql += " JAE.JOBCARD_REF_NO,                             ";
                Strsql += " BAT.BOOKING_MST_PK,                             ";
                Strsql += " BAT.BOOKING_REF_NO,                             ";
                Strsql += " BAT.BOOKING_DATE,  ";
                Strsql += " JAE.SHIPPER_CUST_MST_FK ,      ";
                Strsql += " CUSTOMER.CUSTOMER_NAME ,       ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_1 ,       ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_2 ,       ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_3 ,       ";
                Strsql += " CUSTDTLS.ADM_CITY ,            ";
                Strsql += " JAE.UCR_NO,                    ";
                Strsql += " CUSTDTLS.ADM_LOCATION_MST_FK , ";
                Strsql += " EXPORTERLOC.LOCATION_NAME ,    ";
                Strsql += " CUSTDTLS.ADM_COUNTRY_MST_FK ,  ";
                Strsql += " EXPORTERCOUNTRY.COUNTRY_NAME , ";
                Strsql += " BAT.CUSTOMER_REF_NO,          ";
                Strsql += " CUSTDTLS.ADM_ZIP_CODE ,    ";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_1 ,  ";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_2 ,  ";
                Strsql += " CUSTDTLS.ADM_FAX_NO     ,  ";
                Strsql += " CUSTDTLS.ADM_EMAIL_ID   ,  ";
                Strsql += " CUSTDTLS.ADM_URL,                              ";
                Strsql += " ' ',                                           ";
                Strsql += " ' ' ,                                          ";
                Strsql += " ' ' ,                                          ";
                Strsql += " CORP.CORPORATE_NAME ,                          ";
                Strsql += " CORP.ADDRESS_LINE1 ,                           ";
                Strsql += " CORP.ADDRESS_LINE2 ,                           ";
                Strsql += " CORP.ADDRESS_LINE3 ,                           ";
                Strsql += " CORP.CITY          ,                           ";
                Strsql += " CORP.STATE_MST_FK ,                            ";
                Strsql += " STATE.STATE_NAME ,                             ";
                Strsql += " CORP.COUNTRY_MST_FK ,                          ";
                Strsql += " COUNTRY.COUNTRY_NAME ,                         ";
                Strsql += " CORP.POST_CODE       ,                         ";
                Strsql += " CORP.PHONE           ,                         ";
                Strsql += " CORP.FAX             ,                         ";
                Strsql += " CORP.EMAIL           ,                         ";
                Strsql += " CORP.HOME_PAGE       ,                         ";
                Strsql += " BAT.CARRIER_MST_FK ,                          ";
                Strsql += " OPERAT.OPERATOR_NAME ,                         ";
                Strsql += " ' ',                                           ";
                Strsql += " (JAE.VESSEL_NAME || '/'|| JAE.VOYAGE_FLIGHT_NO) ,        ";
                Strsql += " TO_CHAR(JAE.ETD_DATE,'" + dateFormat + "'),            ";
                Strsql += " BAT.PORT_MST_POL_FK,                           ";
                Strsql += " PORTOFLANDING.PORT_NAME,                       ";
                Strsql += " BAT.PORT_MST_POD_FK ,                          ";
                Strsql += " PORTOFDISCHARGE.PORT_NAME,                     ";
                Strsql += " BAT.DEL_PLACE_MST_FK,                          ";
                Strsql += " PLD.PLACE_NAME,                                ";
                Strsql += " (CASE                                          ";
                Strsql += " WHEN JAE.MARKS_NUMBERS IS NOT NULL THEN        ";
                Strsql += " JAE.MARKS_NUMBERS                              ";
                Strsql += " ELSE                                           ";
                Strsql += " JAE.MARKS_NUMBERS                              ";
                Strsql += " END),                                          ";
                Strsql += " (CASE                                          ";
                Strsql += " WHEN JAE.GOODS_DESCRIPTION IS NOT NULL THEN    ";
                Strsql += " JAE.GOODS_DESCRIPTION                          ";
                Strsql += " ELSE                                           ";
                Strsql += " JAE.GOODS_DESCRIPTION                          ";
                Strsql += " END),                                          ";
                Strsql += " JTAEC.CONTAINER_NUMBER,                        ";
                Strsql += " JTAEC.SEAL_NUMBER,                             ";
                Strsql += " CONTAINER.CONTAINER_TYPE_MST_ID,                ";
                Strsql += " CONTAINER.CONTAINER_TAREWEIGHT_TONE,            ";
                Strsql += " JTAEC.PACK_COUNT,                               ";
                Strsql += " CORP.CORPORATE_NAME,                            ";
                Strsql += " TO_CHAR(SYSDATE,'" + dateFormat + "') ,                 ";
                Strsql += " JAE.TRANSPORTER_CARRIER_FK ,                    ";
                Strsql += " TRANSPORTER.VENDOR_NAME ,                       ";
                Strsql += "  ' '                                            ";

                return ObjWF.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch Data for Standard Shipping Note"

        #region "Fetch Volume and Gross Weight"

        /// <summary>
        /// Fetches the volume.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public Int32 FetchVolume(string JOBPK)
        {
            string Strsql = null;
            Int32 TotVol = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " select sum(nvl(j.volume_in_cbm,0)) ";
                Strsql += " from JOB_TRN_CONT j where j.JOB_CARD_TRN_FK=" + JOBPK;
                TotVol = Convert.ToInt32(ObjWF.ExecuteScaler(Strsql));
                return TotVol;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the gross wt.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public Int32 FetchGrossWt(string JOBPK)
        {
            string Strsql = null;
            Int32 TotGrossWt = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " select sum(nvl(j.gross_weight,0)) ";
                Strsql += " from JOB_TRN_CONT j where j.JOB_CARD_TRN_FK=" + JOBPK;
                TotGrossWt = Convert.ToInt32(ObjWF.ExecuteScaler(Strsql));
                return TotGrossWt;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Volume and Gross Weight"

        #region "Enhance Search Function"

        /// <summary>
        /// Fetches the master job card air.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMasterJobCardAir(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            string POL = null;
            string POD = null;
            string operatorPK = null;

            var strNull = "";

            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            POL = Convert.ToString(arr.GetValue(2));
            POD = Convert.ToString(arr.GetValue(3));
            operatorPK = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_MASTER_PKG.GET_MASTERJOBCARDSEA";

                var _with72 = selectCommand.Parameters;
                _with72.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with72.Add("LOOKUP_VALUE_IN", (!string.IsNullOrEmpty(strReq) ? strReq : strNull)).Direction = ParameterDirection.Input;
                _with72.Add("POL_IN", (!string.IsNullOrEmpty(POL) ? POL : strNull)).Direction = ParameterDirection.Input;
                _with72.Add("POD_IN", (!string.IsNullOrEmpty(POD) ? POD : strNull)).Direction = ParameterDirection.Input;
                _with72.Add("OPERATOR_IN", (!string.IsNullOrEmpty(operatorPK) ? operatorPK : strNull)).Direction = ParameterDirection.Input;
                _with72.Add("LOC_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                //Snigdharani - 15/12/2008
                _with72.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                selectCommand.ExecuteNonQuery();
                //strReturn = CStr(selectCommand.Parameters["RETURN_VALUE"].Value)
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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

        #endregion "Enhance Search Function"

        #region " Fetch Max Contract No."

        /// <summary>
        /// Fetches the job card reference no.
        /// </summary>
        /// <param name="strMasterJobCardNo">The string master job card no.</param>
        /// <returns></returns>
        public string FetchJobCardReferenceNo(string strMasterJobCardNo)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                string strResult = null;

                strSQL = " SELECT NVL(MAX(T.jobcard_ref_no),0) FROM JOB_CARD_TRN T " + " WHERE t.master_jc_sea_exp_fk = " + strMasterJobCardNo + " ORDER BY T.jobcard_ref_no ";

                strResult = objWF.ExecuteScaler(strSQL);

                if (strResult == "0")
                {
                    strSQL = " select m.master_jc_ref_no from master_jc_sea_exp_tbl m " + " where m.master_jc_sea_exp_pk = " + strMasterJobCardNo;
                    strResult = objWF.ExecuteScaler(strSQL);
                }

                return strResult;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch Max Contract No."

        #region " Fetch VAT percentage "

        /// <summary>
        /// Fetches the vat percentage.
        /// </summary>
        /// <returns></returns>
        public double FetchVATPercentage()
        {
            try
            {
                string strSQL = "select nvl(c.vat_percentage,0) from corporate_mst_tbl c";
                WorkFlow objWK = new WorkFlow();
                return Convert.ToDouble(objWK.ExecuteScaler(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch VAT percentage "

        #region "Fetch EnableDisableOprStatus & Save Operator Booking"

        /// <summary>
        /// Funs the e disable opr status.
        /// </summary>
        /// <param name="strBookingRefNo">The string booking reference no.</param>
        /// <returns></returns>
        public string funEDisableOprStatus(string strBookingRefNo)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strReturn = null;
            strBuilder.Append(" SELECT ");
            strBuilder.Append(" BST.BOOKING_SEA_PK");
            strBuilder.Append(" FROM");
            strBuilder.Append(" BOOKING_MST_TBL BST ,");
            strBuilder.Append(" JOB_CARD_TRN JHDR");
            strBuilder.Append(" WHERE");
            strBuilder.Append(" BST.OPR_UPDATE_STATUS=1 ");
            strBuilder.Append(" AND JHDR.BOOKING_SEA_FK=BST.BOOKING_SEA_PK");
            strBuilder.Append(" AND (JHDR.HBL_EXP_TBL_FK IS NULL AND JHDR.MBL_EXP_TBL_FK IS NULL)");
            strBuilder.Append(" AND BST.BOOKING_REF_NO='" + strBookingRefNo + "'");
            try
            {
                strReturn = objWF.ExecuteScaler(strBuilder.ToString());
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Funs up stream updation booking opr.
        /// </summary>
        /// <param name="strBookingRefNo">The string booking reference no.</param>
        /// <param name="strOperatorPk">The string operator pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        private object funUpStreamUpdationBookingOpr(string strBookingRefNo, string strOperatorPk, OracleTransaction TRAN)
        {
            try
            {
                arrMessage.Clear();
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                OracleCommand OCUpdCmd = new OracleCommand();
                Int16 intReturn = default(Int16);
                var _with73 = OCUpdCmd;
                _with73.CommandType = CommandType.StoredProcedure;
                _with73.CommandText = objWF.MyUserName + ".JOB_CARD_SEA_PKG.UPDATE_UPSTREAM_BOOKINGOPR";
                _with73.Connection = TRAN.Connection;
                _with73.Transaction = TRAN;

                _with73.Parameters.Clear();
                _with73.Parameters.Add("BOOKING_REFNO_IN", strBookingRefNo).Direction = ParameterDirection.Input;
                _with73.Parameters.Add("OPERATOR_FK_IN", strOperatorPk).Direction = ParameterDirection.Input;
                _with73.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                intReturn = Convert.ToInt16(_with73.ExecuteNonQuery());

                if (intReturn == 1)
                {
                    arrMessage.Add("All Data Saved Successfully");
                }
                else
                {
                    arrMessage.Add("Upstream updation failed, Check Operator");
                }
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

        #endregion "Fetch EnableDisableOprStatus & Save Operator Booking"

        #region "Fetch MJCPK"

        //Added by rabbani
        /// <summary>
        /// Fetches the MJCPK.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public string FetchMJCPK(string jobCardPK = "0")
        {
            string strSQL = null;
            strSQL = "select job_exp.master_jc_sea_exp_fk" + "from JOB_CARD_TRN job_exp" + "where job_exp.JOB_CARD_TRN_PK=" + jobCardPK;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch MJCPK"

        #region "Export to XML"         ' Manoharan 04June2008 for Qfor-Qfin

        /// <summary>
        /// Export2s the XML.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet Export2XML(string jobCardPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtSale = null;
            DataTable dtFrt = null;
            DataTable dtCost = null;
            DataSet MainDs = new DataSet();

            try
            {
                dtSale = getSalesHeader(jobCardPK);
                dtFrt = getSalesFreight(jobCardPK);
                dtCost = getSalesCost(jobCardPK);
                MainDs.Tables.Add(dtSale);
                MainDs.Tables.Add(dtFrt);
                MainDs.Tables.Add(dtCost);

                MainDs.Tables[0].TableName = "SALES";
                MainDs.Tables[1].TableName = "CHARGEDETAILS";
                MainDs.Tables[2].TableName = "COSTDETAILS";

                DataRelation relJc_Frt = new DataRelation("relJcFrt", new DataColumn[] { MainDs.Tables["SALES"].Columns["JOB_PK"] }, new DataColumn[] { MainDs.Tables["CHARGEDETAILS"].Columns["JOB_PK"] });
                DataRelation relJc_Cost = new DataRelation("relJcCost", new DataColumn[] { MainDs.Tables["SALES"].Columns["JOB_PK"] }, new DataColumn[] { MainDs.Tables["COSTDETAILS"].Columns["JOB_PK"] });

                relJc_Frt.Nested = true;
                relJc_Cost.Nested = true;

                MainDs.Relations.Add(relJc_Frt);
                MainDs.Relations.Add(relJc_Cost);
                MainDs.DataSetName = "JOBCARDSALES";

                return MainDs;
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
        /// Gets the sales header.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataTable getSalesHeader(string jobCardPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT  ");
            strSQL.Append(" ' ' AS \"SALENR\",' ' AS \"SALEDT\",' ' AS \"ACTIVITYDT\",' ' AS \"ROEBASIS\", ");
            strSQL.Append(" Q.jobcard_ref_no as \"JCNUMBER\", Q.jobcard_date as \"JCDATE\", Q.booking_ref_no as \"BKGNUMBER\", ");
            strSQL.Append(" Q.booking_date AS \"BKGDATE\", Q.hbl_ref_no as \"BLNUMBER\", Q.hbl_date as \"BLDATE\", ");
            strSQL.Append(" Q.customer_id  as \"PARTYID\", Q.customer_name  as \"PARTYNAME\", 'CUSTOMER' AS \"PARTYFLAG\", ");
            strSQL.Append(" 'SEA' AS \"BIZTYPE\", Q.place_code  as \"POR\", Q.POL as \"POL\", Q.POD as \"POD\", ");
            strSQL.Append(" Q.PFD as \"PFD\", 'EXPORT' AS \"PROCESSTYPE\", Q.cargo_move_code as \"SHIPMENTTERMS\" , ");
            strSQL.Append(" Q.VSL_VOYAGE AS \"VSLVOYAGE\", 'CONTAINER' AS \"SHIPMENTTYPE\", Q.JOB_CARD_TRN_PK AS \"JOB_PK\" FROM (SELECT ");
            strSQL.Append(" job_exp.JOB_CARD_TRN_PK, job_exp.booking_sea_fk, job_exp.jobcard_ref_no , ");
            strSQL.Append(" to_char(job_exp.jobcard_date,'dd-Mon-yyyy') jobcard_date, bst.booking_ref_no, to_char(bst.booking_date,'dd-Mon-yyyy') booking_date, ");
            strSQL.Append(" job_exp.hbl_exp_tbl_fk, nvl(hbl.hbl_ref_no,' ') hbl_ref_no, nvl(to_char(hbl.hbl_date, 'dd-Mon-yyyy'), ' ') hbl_date, job_exp.shipper_cust_mst_fk, shipper.customer_id, ");
            strSQL.Append(" shipper.customer_name,bst.col_place_mst_fk, nvl(col_place.place_code,' ') place_code, bst.port_mst_pol_fk, ");
            strSQL.Append(" pol.port_id as \"POL\",bst.port_mst_pod_fk, pod.port_id as \"POD\",bst.del_place_mst_fk, ");
            strSQL.Append(" nvl(del_place.place_code,' ')  as \"PFD\", job_exp.cargo_move_fk, stm.cargo_move_code, ");
            strSQL.Append(" VVT.VOYAGE_TRN_PK \"VoyagePK\", CASE WHEN V.VESSEL_ID IS NULL THEN ' ' ELSE ");
            strSQL.Append(" V.VESSEL_ID || '/' || VVT.VOYAGE END AS \"VSL_VOYAGE\" ");
            strSQL.Append(" FROM ");
            strSQL.Append(" JOB_CARD_TRN job_exp,BOOKING_MST_TBL bst,port_mst_tbl POD,port_mst_tbl POL, ");
            strSQL.Append(" cargo_move_mst_tbl stm,customer_mst_tbl shipper,place_mst_tbl col_place, ");
            strSQL.Append(" place_mst_tbl del_place,VESSEL_VOYAGE_TBL V,  VESSEL_VOYAGE_TRN VVT, hbl_exp_tbl hbl ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" job_exp.JOB_CARD_TRN_PK = " + jobCardPK);
            strSQL.Append(" AND job_exp.booking_sea_fk   =  bst.booking_sea_pk ");
            strSQL.Append(" AND bst.port_mst_pol_fk  =  pol.port_mst_pk ");
            strSQL.Append(" AND bst.port_mst_pod_fk  =  pod.port_mst_pk ");
            strSQL.Append(" AND bst.col_place_mst_fk =  col_place.place_pk(+) ");
            strSQL.Append(" AND bst.del_place_mst_fk =  del_place.place_pk(+) ");
            strSQL.Append(" AND job_exp.shipper_cust_mst_fk  =  shipper.customer_mst_pk(+) ");
            strSQL.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK =  V.VESSEL_VOYAGE_TBL_PK(+)  ");
            strSQL.Append(" AND JOB_EXP.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+) ");
            strSQL.Append(" and hbl.hbl_exp_tbl_pk(+) = job_exp.hbl_exp_tbl_fk ");
            strSQL.Append(" and job_exp.cargo_move_fk = stm.cargo_move_pk(+)) Q ");
            try
            {
                return objWF.GetDataTable(strSQL.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the sales freight.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataTable getSalesFreight(string jobCardPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            //Dim AmountFormat As String = ""

            strSQL.Append(" SELECT ");
            strSQL.Append(" Q.freight_element_id  AS \"CHARGECODE\",' ' AS \"PACKAGETYPE\", ");
            strSQL.Append(" Q.container_type_mst_id AS \"CONTAINERTYPE\",Q.currency_id AS \"CURRENCY\", ");

            //strSQL.Append(" Q.QUANTITY AS ""QUANTITY"",Q.freight_amt/Q.QUANTITY AS ""RATE"", ")
            strSQL.Append(" Q.QUANTITY AS \"QUANTITY\",");
            strSQL.Append(" case when (Q.QUANTITY=0 or Q.freight_amt=0) then 0 ");
            strSQL.Append(" else ");
            strSQL.Append(" ROUND(Q.freight_amt/Q.QUANTITY,2) ");
            strSQL.Append(" END \"RATE\",");

            strSQL.Append(" Q.freight_type AS \"PCFLAG\",Q.freight_amt AS \"AMOUNT\", ");
            strSQL.Append(" ' ' AS \"VATPERCENTAGE\",' ' AS \"VATAMOUNT\", ");
            strSQL.Append(" Q.location_id AS \"COLLECTLOCATION\",Q.customer_id AS \"COLLECTPARTY\", ");
            strSQL.Append(" ' ' AS \"ROE\",' ' AS \"ROEBASIS\",' ' AS \"AMT_IN_BASE\",' ' as \"STATUS\", Q.job_card_sea_exp_fk AS \"JOB_PK\" ");
            strSQL.Append(" FROM (SELECT ");
            strSQL.Append(" job_trn_sea_exp_fd_pk, job_card_sea_exp_fk,\tcontainer_type_mst_fk, freight.freight_element_id, ");
            strSQL.Append(" freight.freight_element_mst_pk,\tcont.container_type_mst_id, job_trn_fd.currency_mst_fk, ");
            strSQL.Append(" curr.currency_id, (select Count(*) FROM JOB_TRN_CONT job_trn_cont ");
            strSQL.Append(" where job_trn_cont.job_card_sea_exp_fk = " + jobCardPK);
            strSQL.Append(" and job_trn_cont.container_type_mst_fk = container_type_mst_fk) as \"QUANTITY\", ");
            strSQL.Append(" DECODE(job_trn_fd.freight_type,1,'Prepaid',2,'Collect') freight_type,  ");
            strSQL.Append(" job_trn_fd.freight_amt, job_trn_fd.location_mst_fk, lmt.location_id , ");
            strSQL.Append(" job_trn_fd.frtpayer_cust_mst_fk, cmt.customer_id ");
            strSQL.Append(" FROM ");
            strSQL.Append(" JOB_TRN_FD job_trn_fd, container_type_mst_tbl cont, currency_type_mst_tbl curr, ");
            strSQL.Append(" freight_element_mst_tbl freight, parameters_tbl prm, JOB_CARD_TRN job_exp, ");
            strSQL.Append(" location_mst_tbl lmt, customer_mst_tbl cmt ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" job_trn_fd.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK ");
            strSQL.Append(" AND job_trn_fd.container_type_mst_fk = cont.container_type_mst_pk(+) ");
            strSQL.Append(" AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk ");
            strSQL.Append(" AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk ");
            strSQL.Append(" AND job_trn_fd.freight_element_mst_fk = prm.frt_bof_fk ");
            strSQL.Append(" AND job_trn_fd.location_mst_fk = lmt.location_mst_pk (+) ");
            strSQL.Append(" AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+) ");
            strSQL.Append(" AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);
            strSQL.Append("  union all  ");
            strSQL.Append("  SELECT ");
            strSQL.Append(" job_trn_sea_exp_fd_pk, job_card_sea_exp_fk, container_type_mst_fk, freight.freight_element_id, ");
            strSQL.Append(" freight.freight_element_mst_pk,\tcont.container_type_mst_id, job_trn_fd.currency_mst_fk, ");
            strSQL.Append(" curr.currency_id, (select Count(*) FROM JOB_TRN_CONT job_trn_cont ");
            strSQL.Append(" where job_trn_cont.job_card_sea_exp_fk = " + jobCardPK);
            strSQL.Append(" and job_trn_cont.container_type_mst_fk = container_type_mst_fk) as \"QUANTITY\", ");
            strSQL.Append(" DECODE(job_trn_fd.freight_type,1,'Prepaid',2,'Collect') freight_type, ");
            strSQL.Append(" job_trn_fd.freight_amt, job_trn_fd.location_mst_fk, lmt.location_id , ");
            strSQL.Append(" job_trn_fd.frtpayer_cust_mst_fk, cmt.customer_id ");
            strSQL.Append(" FROM ");
            strSQL.Append(" JOB_TRN_FD job_trn_fd, container_type_mst_tbl cont, currency_type_mst_tbl curr, ");
            strSQL.Append(" freight_element_mst_tbl freight, parameters_tbl prm, JOB_CARD_TRN job_exp, ");
            strSQL.Append(" location_mst_tbl lmt, customer_mst_tbl cmt ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" job_trn_fd.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK ");
            strSQL.Append(" AND job_trn_fd.container_type_mst_fk = cont.container_type_mst_pk(+) ");
            strSQL.Append(" AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk ");
            strSQL.Append(" AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk ");
            strSQL.Append(" AND job_trn_fd.freight_element_mst_fk not in  prm.frt_bof_fk ");
            strSQL.Append("  AND job_trn_fd.location_mst_fk = lmt.location_mst_pk (+) ");
            strSQL.Append(" AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+) ");
            strSQL.Append(" AND job_exp.JOB_CARD_TRN_PK = " + jobCardPK);
            strSQL.Append(" )Q");
            try
            {
                return objWF.GetDataTable(strSQL.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the sales cost.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataTable getSalesCost(string jobCardPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT ");
            strSQL.Append(" Q.vendor_key AS \"PARTYID\", Q.VENDOR_NAME AS \"PARTYNAME\", 'VENDOR' AS \"PARTYTYPE\", ");
            strSQL.Append(" Q.cost_element_id AS \"COSTCODE\", Q.LOCATION_ID AS \"COSTCENTER\", Q.CONT_TYPE AS \"CONTAINERTYPE\", ");
            //
            strSQL.Append(" ' ' AS \"PACKAGETYPE\", Q.CURRENCY_ID AS \"CURRENCY\" , Q.QUANTITY AS \"QUANTITY\", ");

            //strSQL.Append(" (Q.estimated_amt/QUANTITY) AS ""RATE"", Q.estimated_amt AS ""AMOUNT"", ")
            strSQL.Append(" case when (Q.estimated_amt=0 or QUANTITY=0) then 0 ");
            strSQL.Append(" else ");
            strSQL.Append(" Q.estimated_amt/QUANTITY");
            strSQL.Append(" END \"RATE\",");

            strSQL.Append(" Q.tax_percentage AS \"VATPERCENTAGE\", Q.tax_amt AS \"VATAMOUNT\", Q.invoice_amt AS \"TOTALAMOUNT\", ");
            strSQL.Append(" ' ' AS \"ROE\", ' ' AS \"ROEBASIS\", ' ' AS \"AMT_IN_BASE\", ");
            strSQL.Append(" Q.job_card_sea_exp_fk AS \"JOB_PK\" ");
            strSQL.Append(" FROM (SELECT ");
            strSQL.Append(" job_trn_sea_exp_pia_pk, job_card_sea_exp_fk, vendor_mst_fk, vendor_key, ");
            strSQL.Append(" VENDOR.VENDOR_NAME, cost_element_mst_fk, cost_element_id, LMT.LOCATION_ID,");
            //
            strSQL.Append(" (select DISTINCT CONT.CONTAINER_TYPE_MST_ID FROM JOB_TRN_CONT job_trn_cont, ");
            strSQL.Append(" container_type_mst_tbl cont where ");
            strSQL.Append(" CONT.CONTAINER_TYPE_MST_PK = job_trn_cont.container_type_mst_fk ");
            strSQL.Append(" AND job_trn_cont.job_card_sea_exp_fk= " + jobCardPK);
            strSQL.Append(" ) AS \"CONT_TYPE\", ");
            strSQL.Append(" currency_mst_fk, CURR.CURRENCY_ID, (select Count(*) FROM JOB_TRN_CONT job_trn_cont, ");
            strSQL.Append(" container_type_mst_tbl cont where ");
            strSQL.Append(" CONT.CONTAINER_TYPE_MST_PK = job_trn_cont.container_type_mst_fk ");
            strSQL.Append(" AND job_trn_cont.job_card_sea_exp_fk = " + jobCardPK);
            strSQL.Append(" ) as \"QUANTITY\", ");
            strSQL.Append(" estimated_amt, tax_percentage, tax_amt,\tinvoice_amt ");
            strSQL.Append(" FROM ");
            strSQL.Append(" job_trn_sea_exp_pia  job_trn_pia, ");
            strSQL.Append(" currency_type_mst_tbl curr, ");
            strSQL.Append(" cost_element_mst_tbl cost_ele, ");
            strSQL.Append(" JOB_CARD_TRN job_exp, USER_MST_TBL  UMT, LOCATION_MST_TBL      LMT,");
            //
            strSQL.Append(" VENDOR_MST_TBL VENDOR ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" job_trn_pia.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK ");
            strSQL.Append(" AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk ");
            strSQL.Append(" AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk ");
            strSQL.Append(" AND VENDOR.VENDOR_MST_PK = job_trn_pia.Vendor_Mst_Fk AND JOB_EXP.CREATED_BY_FK = UMT.USER_MST_PK AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
            //
            strSQL.Append(" AND job_exp.JOB_CARD_TRN_PK = " + jobCardPK);
            strSQL.Append(" ) Q");
            try
            {
                return objWF.GetDataTable(strSQL.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //adding by thiyagarajan on 10/11/08 to display PDF through JOBCARD Entry Screen :PTS Task
        /// <summary>
        /// Agents the type.
        /// </summary>
        /// <param name="Refno">The refno.</param>
        /// <param name="process">The process.</param>
        /// <param name="biztype">The biztype.</param>
        /// <returns></returns>
        public string AgentType(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" select decode(inv.CB_DP_LOAD_AGENT,1,'CB',2,'DP',3,'LA') from inv_agent_tbl inv where ");
                strSQL.Append(" inv.invoice_ref_no like '" + Refno + "' ");
                //If process = 2 Then
                //    strSQL.Replace("EXP", "IMP")
                //    strSQL.Replace("cb_or_dp_agent", "CB_OR_LOAD_AGENT")
                //End If
                if (biztype == 1)
                {
                    strSQL.Replace("SEA", "AIR");
                }
                return objWF.ExecuteScaler(strSQL.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Credits the type.
        /// </summary>
        /// <param name="Refno">The refno.</param>
        /// <param name="process">The process.</param>
        /// <param name="biztype">The biztype.</param>
        /// <returns></returns>
        public ArrayList CreditType(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            ArrayList result = new ArrayList();
            OracleDataReader cmdreader = null;
            try
            {
                strSQL.Append(" select CR.CR_AGENT_PK PK,decode(CR.CB_DP_LOAD_AGENT,1,'CB',2,'DP') CRType ");
                strSQL.Append(" from CR_AGENT_TBL CR WHERE CR.CREDIT_NOTE_REF_NO LIKE '" + Refno + "' ");
                //If process = 2 Then
                //    strSQL.Replace("EXP", "IMP")
                //    strSQL.Replace("CB_DP_LOAD_AGENT", "CB_DP_LOAD_AGENT")
                //End If
                //If biztype = 1 Then
                //    strSQL.Replace("SEA", "AIR")
                //End If
                cmdreader = objWF.GetDataReader(strSQL.ToString());
                while (cmdreader.Read())
                {
                    result.Add(cmdreader[0]);
                    result.Add(cmdreader[1]);
                }
                cmdreader.Close();
                objWF.MyConnection.Close();
                return result;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Crs the customer.
        /// </summary>
        /// <param name="Refno">The refno.</param>
        /// <param name="process">The process.</param>
        /// <param name="biztype">The biztype.</param>
        /// <returns></returns>
        public Int32 CrCustomer(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            int CRPK = 0;
            try
            {
                strSQL.Append(" select cust.cr_cust_sea_exp_pk from CR_CUST_SEA_EXP_TBL cust where cust.credit_note_ref_no like '" + Refno + "' ");
                if (biztype == 1)
                {
                    strSQL.Replace("SEA", "AIR");
                }
                CRPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return CRPK;
        }

        /// <summary>
        /// Crs the customer.
        /// </summary>
        /// <param name="Refno">The refno.</param>
        /// <returns></returns>
        public Int32 CrCustomer(string Refno)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            int CRPK = 0;
            try
            {
                strSQL = "SELECT CRN.CRN_TBL_PK FROM CREDIT_NOTE_TBL CRN WHERE UPPER(CRN.CREDIT_NOTE_REF_NR)=UPPER('" + Refno + "')";
                CRPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return CRPK;
        }

        /// <summary>
        /// Crs the type of the agent.
        /// </summary>
        /// <param name="Refno">The refno.</param>
        /// <param name="process">The process.</param>
        /// <param name="biztype">The biztype.</param>
        /// <returns></returns>
        public string CRAgentType(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" select decode(inv.CB_DP_LOAD_AGENT,1,'CB',2,'DP') from CR_AGENT_TBL inv where ");
                strSQL.Append(" inv.credit_note_ref_no like '" + Refno + "' ");
                //If process = 2 Then
                //    strSQL.Replace("EXP", "IMP")
                //    strSQL.Replace("cb_or_dp_agent", "CB_OR_LOAD_AGENT")
                //End If
                //If biztype = 1 Then
                //    strSQL.Replace("SEA", "AIR")
                //End If
                return objWF.ExecuteScaler(strSQL.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the cr pk.
        /// </summary>
        /// <param name="Refno">The refno.</param>
        /// <param name="process">The process.</param>
        /// <param name="biztype">The biztype.</param>
        /// <returns></returns>
        public Int32 GetCRPk(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" select inv.cr_agent_pk from CR_AGENT_TBL inv where ");
                strSQL.Append(" inv.credit_note_ref_no like '" + Refno + "' ");
                //If process = 2 Then
                //    strSQL.Replace("EXP", "IMP")
                //End If
                //If biztype = 1 Then
                //    strSQL.Replace("SEA", "AIR")
                //End If
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //end by thiyagarajan

        #endregion "Export to XML"         ' Manoharan 04June2008 for Qfor-Qfin

        #region "CLP Report MainDS"

        /// <summary>
        /// Fetches the RPT ds.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchRptDS(string JOBPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = "select rownum as slnr,";
                strSQL += "job_exp.JOB_CARD_TRN_PK,";
                strSQL += "job_exp.booking_sea_fk,";
                strSQL += "job_exp.jobcard_ref_no,";
                strSQL += "job_exp.vessel_name,";
                strSQL += "job_exp.voyage,";
                strSQL += "job_exp.survey_ref_nr,";
                strSQL += "job_exp.survey_date,";
                strSQL += "job_exp.stuff_loc,";
                strSQL += "chaagent.agent_id,";
                strSQL += "chaagent.agent_name,";
                strSQL += "job_cont.container_number,";
                strSQL += "cont_type.container_type_mst_id,";
                strSQL += "cont_type.container_type_name,";
                strSQL += "cont_type.container_tareweight_tone,";
                strSQL += "'' agent_seal_no,";
                strSQL += "job_cont.seal_number customs_seal_no,";
                strSQL += "pod.port_name,";
                strSQL += "del_place.place_code,";
                strSQL += "del_place.place_name,";
                strSQL += "job_exp.sb_number,";
                strSQL += "job_exp.sb_date,";
                strSQL += "shipper.customer_name shipper,";
                strSQL += "job_cont.pack_count,";
                strSQL += "job_cont.gross_weight,";
                strSQL += "job_exp.marks_numbers,";
                strSQL += "job_exp.goods_description,";
                strSQL += "consignee.customer_name consignee,";
                strSQL += "job_cont.volume_in_cbm,";
                strSQL += "chaagent.agent_name chaagent,";
                strSQL += "'' csno,'' grno,";
                strSQL += "pod.port_id as sbpod,";
                strSQL += "job_exp.remarks,";
                strSQL += "'' invnr";
                strSQL += "from JOB_CARD_TRN   job_exp,";
                strSQL += "agent_mst_tbl          chaagent,";
                strSQL += "JOB_TRN_CONT   job_cont,";
                strSQL += "container_type_mst_tbl cont_type,";
                strSQL += "port_mst_tbl           pod,";
                strSQL += "BOOKING_MST_TBL        bkg_sea,";
                strSQL += "place_mst_tbl          del_place,";
                strSQL += "customer_mst_tbl       shipper,";
                strSQL += "customer_mst_tbl       consignee";
                strSQL += "where job_exp.JOB_CARD_TRN_PK IN( " + JOBPK + " )";
                strSQL += "and job_exp.cha_agent_mst_fk = chaagent.agent_mst_pk(+)";
                strSQL += "and job_exp.JOB_CARD_TRN_PK(+) = job_cont.job_card_sea_exp_fk";
                strSQL += "and cont_type.container_type_mst_pk(+) = job_cont.container_type_mst_fk";
                strSQL += "and job_exp.booking_sea_fk = bkg_sea.booking_sea_pk";
                strSQL += "and bkg_sea.port_mst_pod_fk = pod.port_mst_pk";
                strSQL += "and bkg_sea.del_place_mst_fk = del_place.place_pk(+)";
                strSQL += "and job_exp.shipper_cust_mst_fk =  shipper.customer_mst_pk(+)";
                strSQL += "and job_exp.consignee_cust_mst_fk =  consignee.customer_mst_pk(+)";
                return (objWF.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "CLP Report MainDS"

        #region "Stuffing Report"

        /// <summary>
        /// Fetches the BKG pk.
        /// </summary>
        /// <param name="BookingNr">The booking nr.</param>
        /// <returns></returns>
        public DataSet FetchBkgPk(string BookingNr)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = " SELECT BST.BOOKING_SEA_PK ";
                strSQL += " FROM BOOKING_MST_TBL BST ";
                strSQL += " WHERE BST.BOOKING_REF_NO = '" + BookingNr + "'";
                return (objWF.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

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
            strSql += "BST.BOOKING_SEA_PK BKGPK, ";
            strSql += "BST.BOOKING_REF_NO BKGREFNO, ";
            strSql += "BST.BOOKING_DATE BKGDATE, ";
            //strSql &= vbCrLf & "(CASE WHEN BST.VOYAGE IS NOT NULL THEN "
            //strSql &= vbCrLf & "BST.VESSEL_NAME ||'-' || BST.VOYAGE "
            //strSql &= vbCrLf & "ELSE"
            //strSql &= vbCrLf & "BST.VESSEL_NAME END ) VESFLIGHT,"
            strSql += "(CASE";
            strSql += "WHEN JSE.VOYAGE_TRN_FK IS NOT NULL THEN";
            strSql += "JSE.VESSEL_NAME || '-' || JSE.VOYAGE";
            strSql += "ELSE";
            strSql += "JSE.VESSEL_NAME ";
            strSql += "END) VESFLIGHT,";
            strSql += "HBL.HBL_REF_NO HBLREFNO,";
            strSql += " MBL.MBL_REF_NO  MBLREFNO,";
            strSql += " JSE.MARKS_NUMBERS MARKS,";

            strSql += " JSE.GOODS_DESCRIPTION GOODS,";
            strSql += "BST.CARGO_TYPE,";
            //strSql &= vbCrLf & "JSE.UCR_NO UCRN0,"
            strSql += "OPTA.OPERATOR_NAME UCRN0,";
            strSql += "'' CLEARANCEPOINT,";
            //strSql &= vbCrLf & " BST.ETD_DATE ETD,"
            strSql += " JSE.ETD_DATE ETD,";
            strSql += "BST.CUST_CUSTOMER_MST_FK,";
            strSql += "CMST.CUSTOMER_NAME SHIPNAME,";
            strSql += "CMST.ACCOUNT_NO SHIPREFNO,";
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
            strSql += " PLD.PLACE_NAME DELNAME,";
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
            //strSql &= vbCrLf & " BST.ETA_DATE ETA,"
            strSql += " JSE.ETA_DATE ETA,";
            strSql += " BST.GROSS_WEIGHT GROSS,";
            strSql += " BST.CHARGEABLE_WEIGHT CHARWT,";
            strSql += " BST.NET_WEIGHT NETWT,";
            strSql += " BST.VOLUME_IN_CBM VOLUME";

            strSql += "FROM   JOB_CARD_TRN JSE,";
            strSql += " BOOKING_MST_TBL BST,";
            strSql += " CUSTOMER_MST_TBL CMST,";
            strSql += " OPERATOR_MST_TBL        OPTA,";
            strSql += " VESSEL_VOYAGE_TBL       VVT,";
            strSql += " VESSEL_VOYAGE_TRN       VVTT,";
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

            strSql += "WHERE BST.BOOKING_SEA_PK IN ('" + SeaBkgPK + "')";
            strSql += "AND JSE.HBL_EXP_TBL_FK=HBL.HBL_EXP_TBL_PK(+)";
            strSql += "AND JSE.MBL_EXP_TBL_FK=MBL.MBL_EXP_TBL_PK(+)";
            strSql += "AND   CMST.CUSTOMER_MST_PK(+)=BST.CUST_CUSTOMER_MST_FK";
            strSql += "AND   (CONSIGNEE.CUSTOMER_MST_PK=BST.CONS_CUSTOMER_MST_FK OR CONSIGNEE.CUSTOMER_MST_PK=JSE.CONSIGNEE_CUST_MST_FK)";
            strSql += "AND   CDTLS.CUSTOMER_MST_FK(+)=CMST.CUSTOMER_MST_PK";
            strSql += "AND CONSIGNEE.CUSTOMER_MST_PK=CONSIGNEEDTLS.CUSTOMER_MST_FK(+)";
            strSql += " AND SHIPCOUNTRY.COUNTRY_MST_PK(+)=CDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CONSIGNEEDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND   JSE.BOOKING_SEA_FK(+)=BST.BOOKING_SEA_PK";
            strSql += " AND JSE.VOYAGE_TRN_FK = VVTT.VOYAGE_TRN_PK(+)";
            strSql += " AND VVTT.VESSEL_VOYAGE_TBL_FK= VVT.VESSEL_VOYAGE_TBL_PK(+)";
            strSql += " AND VVT.OPERATOR_MST_FK = OPTA.OPERATOR_MST_PK(+)";
            strSql += " AND   BST.PORT_MST_POL_FK=POL.PORT_MST_PK(+)";
            strSql += " AND   BST.PORT_MST_POD_FK=POD.PORT_MST_PK(+)";
            strSql += " AND   BST.DEL_PLACE_MST_FK=PLD.PLACE_PK(+)";
            strSql += " AND   BST.COL_PLACE_MST_FK=COLPLD.PLACE_PK(+)";
            strSql += " AND   BST.DP_AGENT_MST_FK=DBAMST.AGENT_MST_PK(+)";
            strSql += " AND   DBAMST.AGENT_MST_PK=DBADTLS.AGENT_MST_FK(+)";
            strSql += "AND DBCOUNTRY.COUNTRY_MST_PK(+)=DBADTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND  STMST.SHIPPING_TERMS_MST_PK(+)=BST.CUST_CUSTOMER_MST_FK";
            strSql += " AND  BST.COMMODITY_GROUP_FK=CGMST.COMMODITY_GROUP_PK(+)";
            // strSql &= vbCrLf & "AND  BST.BOOKING_SEA_PK IN ('" & SeaBkgPK & "')"
            try
            {
                return Objwk.GetDataSet(strSql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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
            Strsql = " SELECT BST.BOOKING_SEA_PK BKGPK, JSE.CONTAINER_NUMBER CONTAINER";
            Strsql += "FROM JOB_TRN_CONT JSE,BOOKING_MST_TBL BST,JOB_CARD_TRN JS";
            Strsql += "WHERE BST.BOOKING_SEA_PK = JS.BOOKING_SEA_FK";
            Strsql += "AND JSE.JOB_CARD_SEA_EXP_FK=JS.JOB_CARD_TRN_PK";
            Strsql += " AND BST.BOOKING_SEA_PK IN (" + BkgPK + ")";
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Get_s the con det.
        /// </summary>
        /// <param name="JBPk">The jb pk.</param>
        /// <returns></returns>
        public DataSet Get_ConDet(string JBPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                Strsql = "SELECT J.JOB_CARD_SEA_EXP_FK,";
                Strsql += " J.CONTAINER_NUMBER,";
                Strsql += " CTYPE.CONTAINER_TYPE_MST_ID,";
                Strsql += " CMT.COMMODITY_NAME,";
                Strsql += " J.SEAL_NUMBER , ";
                Strsql += " PTMT.PACK_TYPE_ID,";
                Strsql += " J.PACK_COUNT, ";
                Strsql += " J.GROSS_WEIGHT, ";
                Strsql += " J.VOLUME_IN_CBM,";
                Strsql += " J.NET_WEIGHT ";
                Strsql += "FROM JOB_TRN_CONT   J,JOB_CARD_TRN   JS,";
                Strsql += " COMMODITY_MST_TBL      CMT,       PACK_TYPE_MST_TBL      PTMT,";
                Strsql += "CONTAINER_TYPE_MST_TBL CTYPE";
                Strsql += "WHERE JS.JOB_CARD_TRN_PK = J.JOB_CARD_SEA_EXP_FK";
                Strsql += "AND CTYPE.CONTAINER_TYPE_MST_PK(+) = J.CONTAINER_TYPE_MST_FK";
                Strsql += "AND J.COMMODITY_MST_FK= CMT.COMMODITY_MST_PK(+)";
                Strsql += "AND J.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)";
                Strsql += "AND J.JOB_CARD_SEA_EXP_FK in(" + JBPk + ")";
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //End
        }

        /// <summary>
        /// Fetches the transport note.
        /// </summary>
        /// <param name="JBPk">The jb pk.</param>
        /// <returns></returns>
        public DataSet FetchTransportNote(string JBPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                Strsql = "SELECT JCSE.JOB_CARD_TRN_PK,";
                Strsql += "TIST.TRANSPORT_INST_SEA_PK,";
                Strsql += "TIST.TRANS_INST_REF_NO,";
                Strsql += "TIST.MT_CTR_PICKUP_REF,";
                Strsql += "TIST.MT_CTR_PICKUP_BY,";
                Strsql += "TIST.CARGO_PICKUP_REF_NO,";
                Strsql += "TIST.CARGO_PICKUP_BY,";
                Strsql += "TIST.DELIVERY_REF_NO,";
                Strsql += "TIST.DELIVERY_BY";
                Strsql += "FROM TRANSPORT_INST_SEA_TBL TIST, JOB_CARD_TRN JCSE";
                Strsql += "  WHERE TIST.JOB_CARD_FK = TO_CHAR(JCSE.JOB_CARD_TRN_PK) ";
                Strsql += " AND JCSE.JOB_CARD_TRN_PK in(" + JBPk + ")";

                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Stuffing Report"

        #region "FETCH FREIGHT"

        /// <summary>
        /// Fetches the freight elemet.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <returns></returns>
        public DataSet FetchFreightElemet(string JOBPK, string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT JFD.FREIGHT_ELEMENT_MST_FK, QFT.CHECK_ADVATOS");
                sb.Append("  FROM JOB_TRN_FD         JFD,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL    FMT,");
                sb.Append("       QUOTATION_TRN_SEA_FRT_DTLS QFT,");
                sb.Append("       QUOTATION_SEA_TBL          QST,");
                sb.Append("       QUOTATION_TRN_SEA_FCL_LCL  QTS,");
                sb.Append("       JOB_CARD_TRN JOB,");
                sb.Append("       BOOKING_MST_TBL            BKG,");
                sb.Append("       BOOKING_TRN    BTS");
                sb.Append(" WHERE JFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND QFT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK=JFD.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND JOB.BOOKING_SEA_FK=BKG.BOOKING_SEA_PK");
                sb.Append("   AND BTS.TRANS_REF_NO = QST.QUOTATION_REF_NO");
                sb.Append("   AND QST.QUOTATION_SEA_PK = QTS.QUOTATION_SEA_FK");
                sb.Append("   AND QFT.QUOTE_TRN_SEA_FK = QTS.QUOTE_TRN_SEA_PK");
                sb.Append("   AND BTS.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                sb.Append("   AND JFD.JOB_CARD_SEA_EXP_FK = " + JOBPK);
                sb.Append("   AND QFT.CHECK_ADVATOS = 1");
                sb.Append("   AND JFD.ADVATOS_FLAG = 0 ");
                sb.Append("   AND BKG.BOOKING_SEA_PK=" + BKGPK);
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

        #region "FETCH FREIGHT AMOUNT"

        /// <summary>
        /// Fetches the freight amount.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <param name="Cargo_Type">Type of the cargo_.</param>
        /// <param name="PolFK">The pol fk.</param>
        /// <param name="PodFK">The pod fk.</param>
        /// <param name="ContFK">The cont fk.</param>
        /// <param name="BasisFK">The basis fk.</param>
        /// <param name="RefFrom">The reference from.</param>
        /// <returns></returns>
        public DataSet FetchFreightAmount(string JOBPK, string BKGPK, int Cargo_Type, int PolFK, int PodFK, int ContFK, int BasisFK, int RefFrom = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                //'For Opr.Tariff and Gen.Tariff
                if ((RefFrom == 5 | RefFrom == 6))
                {
                    sb.Append("SELECT DISTINCT '' SLNO,");
                    sb.Append("                JOB.JOB_CARD_TRN_PK,");
                    sb.Append("                TNS.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
                    sb.Append("                TNS.CURRENCY_MST_FK,");
                    sb.Append("                CMT.CURRENCY_ID,");
                    //'FCL
                    if (Cargo_Type == 1)
                    {
                        sb.Append("   TFT.FCL_REQ_RATE RATE,");
                        //'LCL
                    }
                    else
                    {
                        sb.Append("   TNS.LCL_TARIFF_RATE RATE,");
                    }
                    sb.Append("                '' SEL");
                    sb.Append("  FROM QUOTATION_SEA_TBL         QST,");
                    sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTN,");
                    sb.Append("       TARIFF_MAIN_SEA_TBL       TMS,");
                    sb.Append("       TARIFF_TRN_SEA_FCL_LCL    TNS,");
                    //'FCL
                    if (Cargo_Type == 1)
                    {
                        sb.Append("       TARIFF_TRN_SEA_CONT_DTL TFT,");
                    }
                    sb.Append("       BOOKING_MST_TBL           BKG,");
                    sb.Append("       BOOKING_TRN   BTF,");
                    sb.Append("       JOB_CARD_TRN      JOB,");
                    sb.Append("       JOB_TRN_FD        JFD,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL   FMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL     CMT");
                    sb.Append(" WHERE BTF.TRANS_REF_NO = QST.QUOTATION_REF_NO");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = BTF.BOOKING_SEA_FK");
                    sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_SEA_EXP_FK");
                    sb.Append("   AND QST.QUOTATION_SEA_PK = QTN.QUOTATION_SEA_FK");
                    sb.Append("   AND TMS.TARIFF_REF_NO = QTN.TRANS_REF_NO");
                    sb.Append("   AND TNS.TARIFF_MAIN_SEA_FK = TMS.TARIFF_MAIN_SEA_PK");
                    if (Cargo_Type == 1)
                    {
                        sb.Append("  AND TNS.TARIFF_TRN_SEA_PK=TFT.TARIFF_TRN_SEA_FK");
                    }
                    sb.Append("   AND CMT.CURRENCY_MST_PK = TNS.CURRENCY_MST_FK");
                    sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = TNS.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = " + BKGPK + "");
                    sb.Append("   AND TNS.PORT_MST_POL_FK = " + PolFK + "");
                    sb.Append("   AND TNS.PORT_MST_POD_FK = " + PodFK + "");
                    //Fcl
                    if (Cargo_Type == 1)
                    {
                        if (ContFK != 0)
                        {
                            sb.Append("   AND TFT.CONTAINER_TYPE_MST_FK = " + ContFK + "");
                        }
                        //'Lcl
                    }
                    else
                    {
                        if (BasisFK != 0)
                        {
                            sb.Append("   AND TNS.LCL_BASIS=" + BasisFK + "");
                        }
                    }

                    //For Spot Rate
                }
                else if ((RefFrom == 1))
                {
                    sb.Append("SELECT DISTINCT '' SLNO,");
                    sb.Append("                JOB.JOB_CARD_TRN_PK,");
                    sb.Append("                RFQ.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
                    sb.Append("                RFQ.CURRENCY_MST_FK,");
                    sb.Append("                CMT.CURRENCY_ID,");
                    //Fcl
                    if (Cargo_Type == 1)
                    {
                        sb.Append("    RFD.FCL_APP_RATE RATE,");
                        //Lcl
                    }
                    else
                    {
                        sb.Append("    RFQ.LCL_APPROVED_RATE RATE,");
                    }
                    sb.Append("                '' SEL");
                    sb.Append("  FROM QUOTATION_SEA_TBL         QST,");
                    sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTN,");
                    sb.Append("       RFQ_SPOT_RATE_SEA_TBL RFM, ");
                    sb.Append("       RFQ_SPOT_TRN_SEA_FCL_LCL   RFQ,");
                    if (Cargo_Type == 1)
                    {
                        sb.Append("       RFQ_SPOT_TRN_SEA_CONT_DET RFD,");
                    }
                    sb.Append("       BOOKING_MST_TBL           BKG,");
                    sb.Append("       BOOKING_TRN   BTF,");
                    sb.Append("       JOB_CARD_TRN      JOB,");
                    sb.Append("       JOB_TRN_FD        JFD,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL   FMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL     CMT");
                    sb.Append(" WHERE BTF.TRANS_REF_NO = QST.QUOTATION_REF_NO");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = BTF.BOOKING_SEA_FK");
                    sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_SEA_EXP_FK");
                    sb.Append("   AND QST.QUOTATION_SEA_PK = QTN.QUOTATION_SEA_FK");
                    sb.Append("   AND RFM.RFQ_REF_NO = QTN.TRANS_REF_NO");
                    sb.Append("   AND RFM.RFQ_SPOT_SEA_PK = RFQ.RFQ_SPOT_SEA_FK");
                    sb.Append("   AND CMT.CURRENCY_MST_PK = RFQ.CURRENCY_MST_FK");
                    sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = RFQ.FREIGHT_ELEMENT_MST_FK");
                    if (Cargo_Type == 1)
                    {
                        sb.Append("   AND RFD.RFQ_SPOT_SEA_TRN_FK=RFQ.RFQ_SPOT_SEA_TRN_PK ");
                    }
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = " + BKGPK + "");

                    //For Customer Contract
                }
                else if ((RefFrom == 2))
                {
                    sb.Append("SELECT DISTINCT '' SLNO,");
                    sb.Append("                JOB.JOB_CARD_TRN_PK,");
                    sb.Append("                JFD.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
                    sb.Append("                CTN.CURRENCY_MST_FK,");
                    sb.Append("                CMT.CURRENCY_ID,");
                    sb.Append("                (Case");
                    sb.Append("                  When NVL(CTN.APPROVED_ALL_IN_RATE, 0) > 0 Then");
                    sb.Append("                   CTN.CURRENT_BOF_RATE");
                    sb.Append("                  else");
                    sb.Append("                   CTN.APPROVED_BOF_RATE");
                    sb.Append("                End) RATE,");
                    sb.Append("                '' SEL");
                    sb.Append("  FROM QUOTATION_SEA_TBL         QST,");
                    sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTN,");
                    sb.Append("       CONT_CUST_SEA_TBL         CST,");
                    sb.Append("       CONT_CUST_TRN_SEA_TBL     CTN,");
                    sb.Append("       BOOKING_MST_TBL           BKG,");
                    sb.Append("       BOOKING_TRN   BTF,");
                    sb.Append("       JOB_CARD_TRN      JOB,");
                    sb.Append("       JOB_TRN_FD        JFD,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL   FMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL     CMT");
                    sb.Append(" WHERE BTF.TRANS_REF_NO = QST.QUOTATION_REF_NO");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = BTF.BOOKING_SEA_FK");
                    sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_SEA_EXP_FK");
                    sb.Append("   AND QST.QUOTATION_SEA_PK = QTN.QUOTATION_SEA_FK");
                    sb.Append("   AND CST.CONT_REF_NO = QTN.TRANS_REF_NO");
                    sb.Append("   AND CST.CONT_CUST_SEA_PK = CTN.CONT_CUST_SEA_FK");
                    sb.Append("   AND CMT.CURRENCY_MST_PK = CTN.CURRENCY_MST_FK");
                    sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = JFD.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = " + BKGPK + "");

                    //'Getting Freight Amount from Gen.Tariff...
                }
                else
                {
                    sb.Append("SELECT DISTINCT '' SLNO,");
                    sb.Append("                JOB.JOB_CARD_TRN_PK,");
                    sb.Append("                TNS.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
                    sb.Append("                TNS.CURRENCY_MST_FK,");
                    sb.Append("                CMT.CURRENCY_ID,");
                    if (Cargo_Type == 1)
                    {
                        sb.Append("     TNF.FCL_REQ_RATE,");
                    }
                    else
                    {
                        sb.Append("     TNS.LCL_TARIFF_RATE,");
                    }
                    sb.Append("                '' SEL");
                    sb.Append("  FROM TARIFF_MAIN_SEA_TBL     TMS,");
                    sb.Append("       TARIFF_TRN_SEA_FCL_LCL  TNS,");
                    if (Cargo_Type == 1)
                    {
                        sb.Append("       TARIFF_TRN_SEA_CONT_DTL TNF,");
                    }
                    sb.Append("       BOOKING_MST_TBL         BKG,");
                    sb.Append("       BOOKING_TRN BTF,");
                    sb.Append("       JOB_CARD_TRN    JOB,");
                    sb.Append("       JOB_TRN_FD      JFD,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL   CMT");
                    sb.Append(" WHERE BKG.BOOKING_SEA_PK = BTF.BOOKING_SEA_FK");
                    sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_SEA_EXP_FK");
                    sb.Append("   AND TNS.TARIFF_MAIN_SEA_FK = TMS.TARIFF_MAIN_SEA_PK");
                    sb.Append("   AND CMT.CURRENCY_MST_PK = TNS.CURRENCY_MST_FK");
                    sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = TNS.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("   AND TMS.ACTIVE = 1");
                    sb.Append("   AND TMS.CARGO_TYPE =" + Cargo_Type);
                    sb.Append("   AND TMS.TARIFF_TYPE = 2");
                    if (Cargo_Type == 1)
                    {
                        sb.Append("   AND TNF.TARIFF_TRN_SEA_FK = TNS.TARIFF_TRN_SEA_PK");
                    }
                    sb.Append("   AND TNS.PORT_MST_POL_FK = " + PolFK + "");
                    sb.Append("   AND TNS.PORT_MST_POD_FK = " + PodFK + "");
                    //Fcl
                    if (Cargo_Type == 1)
                    {
                        if (ContFK != 0)
                        {
                            sb.Append("   AND TNF.CONTAINER_TYPE_MST_FK = " + ContFK + "");
                        }
                        //'Lcl
                    }
                    else
                    {
                        if (BasisFK != 0)
                        {
                            sb.Append("   AND TNS.LCL_BASIS=" + BasisFK + "");
                        }
                    }
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK + "");
                    sb.Append("  AND BKG.BOOKING_SEA_PK = " + BKGPK + "");
                }
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

        /// <summary>
        /// Gets the header details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <param name="RefFrom">The reference from.</param>
        /// <param name="PolFK">The pol fk.</param>
        /// <param name="PodFK">The pod fk.</param>
        /// <param name="ContFK">The cont fk.</param>
        /// <param name="BasisFK">The basis fk.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <returns></returns>
        public DataSet GetHeaderDetails(string JOBPK, string BKGPK, int RefFrom, int PolFK, int PodFK, int ContFK, int BasisFK, int Cargotype)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                //'For SL tariff && Gen.Tariff
                if ((RefFrom == 5 | RefFrom == 6))
                {
                    sb.Append("SELECT DISTINCT QTN.TRANS_REF_NO REFNR,");
                    sb.Append("                TMS.TARIFF_DATE REFDATE,");
                    sb.Append("                TMS.VALID_FROM,");
                    sb.Append("                TMS.VALID_TO,");
                    sb.Append("                QTN.TRANS_REFERED_FROM");
                    sb.Append("  FROM QUOTATION_SEA_TBL         QST,");
                    sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTN,");
                    sb.Append("       TARIFF_MAIN_SEA_TBL       TMS,");
                    sb.Append("       BOOKING_MST_TBL           BKG,");
                    sb.Append("       BOOKING_TRN   BTF,");
                    sb.Append("       JOB_CARD_TRN      JOB,");
                    sb.Append("       JOB_TRN_FD        JFD");
                    sb.Append(" WHERE BTF.TRANS_REF_NO = QST.QUOTATION_REF_NO");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = BTF.BOOKING_SEA_FK");
                    sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_SEA_EXP_FK");
                    sb.Append("   AND QST.QUOTATION_SEA_PK = QTN.QUOTATION_SEA_FK");
                    sb.Append("   AND TMS.TARIFF_REF_NO = QTN.TRANS_REF_NO");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = " + BKGPK + "");

                    //For Spot Rate
                }
                else if (RefFrom == 1)
                {
                    sb.Append("SELECT DISTINCT QTN.TRANS_REF_NO REFNR,");
                    sb.Append("                RFQ.RFQ_DATE REFDATE,");
                    sb.Append("                RFQ.VALID_FROM,");
                    sb.Append("                RFQ.VALID_TO,");
                    sb.Append("                QTN.TRANS_REFERED_FROM");
                    sb.Append("  FROM QUOTATION_SEA_TBL         QST,");
                    sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTN,");
                    sb.Append("        RFQ_SPOT_RATE_SEA_TBL     RFQ,");
                    sb.Append("       BOOKING_MST_TBL           BKG,");
                    sb.Append("       BOOKING_TRN   BTF,");
                    sb.Append("       JOB_CARD_TRN      JOB,");
                    sb.Append("       JOB_TRN_FD        JFD");
                    sb.Append(" WHERE BTF.TRANS_REF_NO = QST.QUOTATION_REF_NO");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = BTF.BOOKING_SEA_FK");
                    sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_SEA_EXP_FK");
                    sb.Append("   AND QST.QUOTATION_SEA_PK = QTN.QUOTATION_SEA_FK");
                    sb.Append("   AND RFQ.RFQ_REF_NO = QTN.TRANS_REF_NO");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = " + BKGPK + "");
                    //'For Customer Contract
                }
                else if (RefFrom == 2)
                {
                    sb.Append("SELECT DISTINCT QTN.TRANS_REF_NO REFNR,");
                    sb.Append("                CST.CONT_DATE REFDATE,");
                    sb.Append("                CST.VALID_FROM,");
                    sb.Append("                CST.VALID_TO,");
                    sb.Append("                QTN.TRANS_REFERED_FROM");
                    sb.Append("  FROM QUOTATION_SEA_TBL         QST,");
                    sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTN,");
                    sb.Append("       CONT_CUST_SEA_TBL         CST,");
                    sb.Append("       BOOKING_MST_TBL           BKG,");
                    sb.Append("       BOOKING_TRN   BTF,");
                    sb.Append("       JOB_CARD_TRN      JOB,");
                    sb.Append("       JOB_TRN_FD        JFD");
                    sb.Append(" WHERE BTF.TRANS_REF_NO = QST.QUOTATION_REF_NO");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = BTF.BOOKING_SEA_FK");
                    sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_SEA_EXP_FK");
                    sb.Append("   AND QST.QUOTATION_SEA_PK = QTN.QUOTATION_SEA_FK");
                    sb.Append("   AND CST.CONT_REF_NO = QTN.TRANS_REF_NO");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_SEA_PK = " + BKGPK + "");

                    //'Getting Detalis From Gen.Tariff
                }
                else
                {
                    sb.Append("SELECT DISTINCT TMS.TARIFF_REF_NO REFNR,");
                    sb.Append("                TMS.TARIFF_DATE REFDATE,");
                    sb.Append("                TMS.VALID_FROM,");
                    sb.Append("                TMS.VALID_TO");
                    sb.Append("  FROM TARIFF_MAIN_SEA_TBL     TMS,");
                    sb.Append("       TARIFF_TRN_SEA_FCL_LCL  TNS");
                    if (Cargotype == 1)
                    {
                        sb.Append("    ,TARIFF_TRN_SEA_CONT_DTL TNF");
                    }
                    sb.Append(" WHERE TMS.TARIFF_MAIN_SEA_PK = TNS.TARIFF_MAIN_SEA_FK");
                    if (Cargotype == 1)
                    {
                        sb.Append("     AND TNS.TARIFF_TRN_SEA_PK = TNF.TARIFF_TRN_SEA_FK");
                    }
                    sb.Append("     AND TMS.ACTIVE=1");
                    if (Cargotype != 0)
                    {
                        sb.Append("     AND TMS.CARGO_TYPE=" + Cargotype);
                    }
                    sb.Append("     AND TMS.TARIFF_TYPE=2");
                    sb.Append("     AND (TMS.VALID_TO >= TO_DATE(SYSDATE,'" + dateFormat + "') OR TMS.VALID_TO IS NULL)");
                    if (PolFK != 0)
                    {
                        sb.Append("   AND TNS.PORT_MST_POL_FK = " + PolFK + "");
                    }
                    if (PodFK != 0)
                    {
                        sb.Append("   AND TNS.PORT_MST_POD_FK = " + PodFK + "");
                    }

                    //Fcl
                    if (Cargotype == 1)
                    {
                        if (ContFK != 0)
                        {
                            sb.Append("   AND TNF.CONTAINER_TYPE_MST_FK = " + ContFK + "");
                        }
                        //'Lcl
                    }
                    else
                    {
                        if (BasisFK != 0)
                        {
                            sb.Append("   AND TNS.LCL_BASIS=" + BasisFK + "");
                        }
                    }
                }
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

        /// <summary>
        /// Gets the details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <returns></returns>
        public DataSet GetDetails(string JOBPK, string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT QTN.TRANS_REF_NO,");
                sb.Append("           QTN.TRANS_REFERED_FROM");
                sb.Append("  FROM QUOTATION_SEA_TBL         QST,");
                sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTN,");
                sb.Append("       BOOKING_MST_TBL           BKG,");
                sb.Append("       BOOKING_TRN   BTF,");
                sb.Append("       JOB_CARD_TRN      JOB,");
                sb.Append("       JOB_TRN_FD        JFD");
                sb.Append(" WHERE BTF.TRANS_REF_NO = QST.QUOTATION_REF_NO");
                sb.Append("   AND BKG.BOOKING_SEA_PK = BTF.BOOKING_SEA_FK");
                sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND QST.QUOTATION_SEA_PK = QTN.QUOTATION_SEA_FK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK + "");
                sb.Append("   AND BKG.BOOKING_SEA_PK = " + BKGPK + "");
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

        /// <summary>
        /// Gets the job details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <returns></returns>
        public DataSet GetJobDetails(string JOBPK, string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT BKG.PORT_MST_POD_FK,");
                sb.Append("                BKG.PORT_MST_POL_FK,");
                sb.Append("                JCT.CONTAINER_TYPE_MST_FK,");
                sb.Append("                JFD.BASIS");
                sb.Append("  FROM BOOKING_MST_TBL      BKG,");
                sb.Append("       JOB_CARD_TRN JOB,");
                sb.Append("       JOB_TRN_FD   JFD,");
                sb.Append("       JOB_TRN_CONT JCT");
                sb.Append(" WHERE JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = JCT.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK + "");
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

        #endregion "FETCH FREIGHT AMOUNT"

        #endregion "FETCH FREIGHT"

        /// <summary>
        /// Updates the advatos.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="FrtPK">The FRT pk.</param>
        public void UpdateADVATOS(string JobPK, int FrtPK)
        {
            try
            {
                WorkFlow ObjWk = new WorkFlow();
                OracleCommand objCommand = new OracleCommand();
                OracleTransaction TRAN = null;
                int nRecAfct = 0;
                string strSQL = null;
                string strSQL1 = null;
                Int16 upd = default(Int16);
                if (string.IsNullOrEmpty(JobPK) | string.IsNullOrEmpty(JobPK))
                {
                    return;
                }

                ObjWk.OpenConnection();
                TRAN = ObjWk.MyConnection.BeginTransaction();
                var _with74 = objCommand;
                _with74.Connection = ObjWk.MyConnection;
                _with74.CommandType = CommandType.Text;
                _with74.CommandText = strSQL;
                _with74.Transaction = TRAN;
                //nRecAfct = .ExecuteNonQuery()
                strSQL1 = "update JOB_TRN_FD J set J.ADVATOS_FLAG = 1 where J.JOB_CARD_SEA_EXP_FK= " + JobPK;
                strSQL1 += " AND J.FREIGHT_ELEMENT_MST_FK=" + FrtPK;
                _with74.CommandText = strSQL1;
                upd = Convert.ToInt16(_with74.ExecuteNonQuery());
                if (upd > 0)
                {
                    TRAN.Commit();
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                //ObjWk.MyConnection.Close()
            }
        }

        #region "Fetch Tariff for VATOS"

        /// <summary>
        /// Fetches the operator tariff.
        /// </summary>
        /// <param name="OprPk">The opr pk.</param>
        /// <param name="CommdityPk">The commdity pk.</param>
        /// <param name="Cargo">The cargo.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public string FetchOperatorTariff(Int32 OprPk, Int32 CommdityPk, Int32 Cargo, string strCondition)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string ValidFrom = null;
            string ValidTo = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".JOB_CARD_SEA_PKG.GETOPERATORTARIFF_SEA_VATOS";
                var _with75 = selectCommand.Parameters;
                _with75.Add("OPR_PK_IN", OprPk).Direction = ParameterDirection.Input;
                _with75.Add("CARGO_TYPE_IN", Cargo).Direction = ParameterDirection.Input;
                _with75.Add("COMMODITY_GROUP_PK_IN", CommdityPk).Direction = ParameterDirection.Input;
                //.Add("VALID_FROM", ValidFrom).Direction = ParameterDirection.Input
                //.Add("VALID_TO", IIf(ValidTo = "n", "", ValidTo)).Direction = ParameterDirection.Input
                _with75.Add("LOOKUP_VALUE_IN", "E").Direction = ParameterDirection.Input;
                _with75.Add("CONDITION_IN", strCondition).Direction = ParameterDirection.Input;
                _with75.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                return "~" + ex.Message;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        /// <summary>
        /// Fetches the operator tariff rates.
        /// </summary>
        /// <param name="TariffPK">The tariff pk.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchOperatorTariffRates(Int64 TariffPK, string strCondition, Int64 CargoType)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strCondition = "(" + strCondition + ")";
                if (CargoType == 1)
                {
                    sb.Append("SELECT CTMT.CONTAINER_TYPE_MST_PK,FRT.FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("       FRT.FREIGHT_ELEMENT_ID,");
                    sb.Append("       CURR.CURRENCY_ID,");
                    sb.Append("       CTMT.CONTAINER_TYPE_MST_ID AS CONT_BASIS,");
                    sb.Append("       NVL(CONT.FCL_REQ_RATE, 0.00) AS APPROVED_RATE,");
                    sb.Append("       CURR.CURRENCY_MST_PK,");
                    sb.Append("       GET_EX_RATE(CURR.CURRENCY_MST_PK, 1, SYSDATE) ROE,");
                    sb.Append("       (NVL(CONT.FCL_REQ_RATE, 0.00) *");
                    sb.Append("       GET_EX_RATE(CURR.CURRENCY_MST_PK, 1, SYSDATE)) FINAL_RATE");
                    sb.Append("  FROM TARIFF_TRN_SEA_FCL_LCL  T,");
                    sb.Append("       TARIFF_TRN_SEA_CONT_DTL CONT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL   CURR,");
                    sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL FRT");
                    sb.Append(" WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPK);
                    sb.Append("   AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                    sb.Append("   AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK");
                    sb.Append("   AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK");
                    sb.Append("   AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK");
                    sb.Append("   AND FRT.FREIGHT_ELEMENT_ID <> 'BOF'");
                    sb.Append("   AND CONT.FCL_REQ_RATE > 0");
                    sb.Append("   AND (T.PORT_MST_POL_FK, T.PORT_MST_POD_FK, CONT.CONTAINER_TYPE_MST_FK) IN");
                    sb.Append("      " + strCondition + "  ");
                    sb.Append(" ORDER BY FRT.PREFERENCE");
                }
                else
                {
                    sb.Append("SELECT UOM.DIMENTION_UNIT_MST_PK CONTAINER_TYPE_MST_PK,");
                    sb.Append("       FRT.FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("       FRT.FREIGHT_ELEMENT_ID,");
                    sb.Append("       CURR.CURRENCY_ID,");
                    sb.Append("       UOM.DIMENTION_ID AS CONT_BASIS,");
                    sb.Append("       NVL(T.LCL_TARIFF_RATE, 0.00) AS APPROVED_RATE,");
                    sb.Append("       CURR.CURRENCY_MST_PK,");
                    sb.Append("       GET_EX_RATE(CURR.CURRENCY_MST_PK, 1, SYSDATE) ROE,");
                    sb.Append("       (NVL(T.LCL_TARIFF_RATE, 0.00) *");
                    sb.Append("       GET_EX_RATE(CURR.CURRENCY_MST_PK, 1, SYSDATE)) FINAL_RATE");
                    sb.Append("  FROM TARIFF_TRN_SEA_FCL_LCL  T,");
                    sb.Append("       DIMENTION_UNIT_MST_TBL  UOM,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL   CURR,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL FRT");
                    sb.Append(" WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPK);
                    sb.Append("   AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                    sb.Append("   AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK");
                    sb.Append("   AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK");
                    sb.Append("   AND FRT.FREIGHT_ELEMENT_ID <> 'BOF'");
                    sb.Append("   AND T.LCL_BASIS > 0");
                    sb.Append("   AND (T.PORT_MST_POL_FK, T.PORT_MST_POD_FK, T.LCL_BASIS) IN");
                    sb.Append("      " + strCondition + "  ");
                }
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the basis.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <returns></returns>
        public DataSet FetchBasis(Int64 JobPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append(" SELECT DISTINCT F.BASIS FROM JOB_TRN_FD F ");
                sb.Append(" WHERE F.JOB_CARD_SEA_EXP_FK = " + JobPK);
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the jobcard close status.
        /// </summary>
        /// <param name="JobCardPk">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchJobcardCloseStatus(int JobCardPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT (CASE");
            sb.Append("         WHEN J.COLLECTION_DATE > J.PAYEMENT_DATE THEN");
            sb.Append("         ");
            sb.Append("          TO_CHAR(J.COLLECTION_DATE, 'dd/MM/yyyy')");
            sb.Append("         ELSE");
            sb.Append("         ");
            sb.Append("          TO_CHAR(J.PAYEMENT_DATE, 'dd/MM/yyyy')");
            sb.Append("       END) JOCARDCLOSEDATE");
            sb.Append("  FROM JOB_CARD_TRN J");
            sb.Append(" WHERE J.JOB_CARD_TRN_PK = " + JobCardPk);
            sb.Append("   AND J.COLLECTION_STATUS = 1");
            sb.Append("   AND J.PAYEMENT_STATUS = 1");
            sb.Append("");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the quot status.
        /// </summary>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <returns></returns>
        public int FetchQuotStatus(int JobCardPK)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT CASE");
            sb.Append("         WHEN QTN_DATE < SYSDATE THEN");
            sb.Append("          1");
            sb.Append("         ELSE");
            sb.Append("          0");
            sb.Append("       END QUOTFLAG");
            sb.Append("  FROM (SELECT J.JOB_CARD_TRN_PK,");
            sb.Append("               F.TRANS_REFERED_FROM,");
            sb.Append("               NVL((SELECT Q.EXPECTED_SHIPMENT_DT + Q.VALID_FOR");
            sb.Append("                     FROM QUOTATION_SEA_TBL Q");
            sb.Append("                    WHERE Q.QUOTATION_REF_NO = F.TRANS_REF_NO),");
            sb.Append("                   '') QTN_DATE");
            sb.Append("          FROM JOB_CARD_TRN    J,");
            sb.Append("               BOOKING_MST_TBL         B,");
            sb.Append("               BOOKING_TRN F");
            sb.Append("         WHERE J.BOOKING_SEA_FK = B.BOOKING_SEA_PK");
            sb.Append("           AND B.BOOKING_SEA_PK = F.BOOKING_SEA_FK");
            sb.Append("           AND J.JOB_CARD_TRN_PK =  " + JobCardPK);
            sb.Append(" ) ");
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Tariff for VATOS"

        #region "Fetch Consol Inv Pk"

        /// <summary>
        /// Checks the inv.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <returns></returns>
        public DataSet CheckInv(Int64 JobPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append(" SELECT COUNT(*) ");
                sb.Append(" FROM (SELECT JF.FREIGHT_ELEMENT_MST_FK");
                sb.Append(" FROM JOB_CARD_TRN J, JOB_TRN_FD JF");
                sb.Append(" WHERE J.JOB_CARD_TRN_PK =" + JobPK);
                sb.Append(" AND JF.JOB_CARD_SEA_EXP_FK = J.JOB_CARD_TRN_PK");
                sb.Append(" AND JF.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append(" UNION");
                sb.Append(" SELECT JO.FREIGHT_ELEMENT_MST_FK");
                sb.Append(" FROM JOB_CARD_TRN J, job_trn_oth_chrg JO");
                sb.Append(" WHERE J.JOB_CARD_TRN_PK = " + JobPK);
                sb.Append(" AND J.JOB_CARD_TRN_PK = JO.JOB_CARD_SEA_EXP_FK");
                sb.Append(" AND JO.CONSOL_INVOICE_TRN_FK IS NULL) ");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Consol Inv Pk"

        #region "Get HBL"

        /// <summary>
        /// Gets the HBL.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <returns></returns>
        public object GetHBL(int JobPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append(" SELECT HBL.HBL_EXP_TBL_PK");
                sb.Append(" FROM JOB_CARD_TRN JOB, HBL_EXP_TBL HBL ");
                sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = HBL.JOB_CARD_SEA_EXP_FK");
                sb.Append(" AND JOB.JOB_CARD_TRN_PK =" + JobPK);
                sb.Append(" AND HBL.HBL_STATUS IN (1, 2)");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Get HBL"

        #region "Fetch Grid Details"

        /// <summary>
        /// Fecthes the grid details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FecthGridDetails(string Jobpk, string BizType, string ProcessType, string CargoType)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                var _with76 = objWF.MyCommand.Parameters;
                _with76.Add("JOB_CARD_SEA_EXP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with76.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with76.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with76.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with76.Add("BASE_CURR_FK_IN", Convert.ToInt64(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                _with76.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_JOBCARD_PROFITABILITY", "FETCH_GRIDDETAILS");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Fetch Grid Details"

        #region "Fetch Header Details"

        /// <summary>
        /// Gets the header details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcType">Type of the proc.</param>
        /// <returns></returns>
        public DataSet GetHeaderDetails(string JOBPK, string BizType, string ProcType)
        {
            StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT JOB.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT)JOBCARD_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME   SHIPPER,");
                sb.Append("       CONSG.CUSTOMER_NAME CONSIGNEE,");
                sb.Append("       OPR.OPERATOR_NAME,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       JOB.SHIPPER_CUST_MST_FK");
                sb.Append("  FROM JOB_CARD_TRN JOB,");
                sb.Append("       BOOKING_MST_TBL      BOOK,");
                sb.Append("       CUSTOMER_MST_TBL     CMT,");
                sb.Append("       CUSTOMER_MST_TBL     CONSG,");
                sb.Append("       OPERATOR_MST_TBL     OPR,");
                sb.Append("       AGENT_MST_TBL        AMT");
                sb.Append(" WHERE JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CONSG.CUSTOMER_MST_PK(+)");
                sb.Append("   AND JOB.BOOKING_SEA_FK = BOOK.BOOKING_SEA_PK");
                sb.Append("   AND BOOK.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
                if (Convert.ToInt32(ProcType) == 1)
                {
                    sb.Append("   AND JOB.DP_AGENT_MST_FK = AMT.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("   AND JOB.CB_AGENT_MST_FK = AMT.AGENT_MST_PK(+)");
                }
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK);
                if (Convert.ToInt32(BizType) == 1)
                {
                    sb.Replace("SEA", "AIR");
                }
                if (Convert.ToInt32(ProcType) == 2)
                {
                    sb.Replace("EXP", "IMP");
                }
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

        #endregion "Fetch Header Details"

        #region "Save JobDetails"

        /// <summary>
        /// Saves the job details.
        /// </summary>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsFreightDetails">The ds freight details.</param>
        /// <param name="dsCostDetails">The ds cost details.</param>
        /// <returns></returns>
        public ArrayList SaveJobDetails(string JobCardPK, DataSet dsFreightDetails, DataSet dsCostDetails)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();

            OracleCommand insCostDetails = new OracleCommand();
            OracleCommand updCostDetails = new OracleCommand();

            try
            {
                var _with77 = insFreightDetails;
                _with77.Connection = objWK.MyConnection;
                _with77.CommandType = CommandType.StoredProcedure;
                _with77.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_INS";
                var _with78 = _with77.Parameters;

                insFreightDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("surcharge_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["surcharge_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;
                insFreightDetails.Parameters.Add("job_trn_sea_exp_cont_fk_in", "").Direction = ParameterDirection.Input;

                insFreightDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_FD_PK").Direction = ParameterDirection.Output;
                insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with79 = updFreightDetails;
                _with79.Connection = objWK.MyConnection;
                _with79.CommandType = CommandType.StoredProcedure;
                _with79.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_UPD";
                var _with80 = _with79.Parameters;

                updFreightDetails.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with81 = objWK.MyDataAdapter;

                _with81.InsertCommand = insFreightDetails;
                _with81.InsertCommand.Transaction = TRAN;
                _with81.UpdateCommand = updFreightDetails;
                _with81.UpdateCommand.Transaction = TRAN;

                RecAfct = _with81.Update(dsFreightDetails);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //'
                var _with82 = insCostDetails;
                _with82.Connection = objWK.MyConnection;
                _with82.CommandType = CommandType.StoredProcedure;
                _with82.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_INS";
                var _with83 = _with82.Parameters;
                insCostDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Output;
                insCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with84 = updCostDetails;
                _with84.Connection = objWK.MyConnection;
                _with84.CommandType = CommandType.StoredProcedure;
                _with84.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_UPD";
                var _with85 = _with84.Parameters;

                updCostDetails.Parameters.Add("JOB_TRN_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["JOB_TRN_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with86 = objWK.MyDataAdapter;
                _with86.InsertCommand = insCostDetails;
                _with86.InsertCommand.Transaction = TRAN;
                _with86.UpdateCommand = updCostDetails;
                _with86.UpdateCommand.Transaction = TRAN;

                RecAfct = _with86.Update(dsCostDetails.Tables[0]);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
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
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
                //added by suryaprasad for implementing session management
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save JobDetails"

        #region "Fetch Income and Expense Details"

        /// <summary>
        /// Fetches the sec ser income details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="CurFK">The current fk.</param>
        /// <returns></returns>
        public DataSet FetchSecSerIncomeDetails(string Jobpk, int CurFK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTotalAmt = new DataTable();
            DataTable dtChargeDet = new DataTable();
            DataSet dsIncomeDet = new DataSet();
            int CurrencyPK = 0;
            if (CurFK > 0)
            {
                CurrencyPK = CurFK;
            }
            else
            {
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            }
            //Parent Details
            try
            {
                var _with87 = objWF.MyCommand.Parameters;
                _with87.Clear();
                _with87.Add("JOB_CARD_SEA_EXP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with87.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with87.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "INCOME_MAIN_SEA_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }

            //Child Details
            try
            {
                var _with88 = objWF.MyCommand.Parameters;
                _with88.Clear();
                _with88.Add("JOB_CARD_SEA_EXP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with88.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with88.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "INCOME_CHILD_SEA_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }

            try
            {
                dsIncomeDet.Tables.Add(dtTotalAmt);
                dsIncomeDet.Tables.Add(dtChargeDet);
                dsIncomeDet.Tables[0].TableName = "TOTAL_AMOUNT";
                dsIncomeDet.Tables[1].TableName = "CHARGE_DETAILS";
                var rel_TotAmtAndCharge = new DataRelation("rel1", dsIncomeDet.Tables["TOTAL_AMOUNT"].Columns["FLAG"], dsIncomeDet.Tables["CHARGE_DETAILS"].Columns["FLAG"]);
                dsIncomeDet.Relations.Add(rel_TotAmtAndCharge);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return dsIncomeDet;
        }

        /// <summary>
        /// Fetches the sec ser expense details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="CurFK">The current fk.</param>
        /// <returns></returns>
        public DataSet FetchSecSerExpenseDetails(string Jobpk, int CurFK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTotalAmt = new DataTable();
            DataTable dtChargeDet = new DataTable();
            DataSet dsExpenseDet = new DataSet();
            int CurrencyPK = 0;
            if (CurFK > 0)
            {
                CurrencyPK = CurFK;
            }
            else
            {
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            }

            //Parent Details
            try
            {
                var _with89 = objWF.MyCommand.Parameters;
                _with89.Add("JOB_CARD_SEA_EXP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with89.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with89.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "EXPENSE_MAIN_SEA_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }

            //Child Details
            try
            {
                var _with90 = objWF.MyCommand.Parameters;
                _with90.Clear();
                _with90.Add("JOB_CARD_SEA_EXP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with90.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with90.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "EXPENSE_CHILD_SEA_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }

            try
            {
                dsExpenseDet.Tables.Add(dtTotalAmt);
                dsExpenseDet.Tables.Add(dtChargeDet);
                dsExpenseDet.Tables[0].TableName = "TOTAL_AMOUNT";
                dsExpenseDet.Tables[1].TableName = "CHARGE_DETAILS";
                var rel_TotAmtAndCharge = new DataRelation("rel1", dsExpenseDet.Tables["TOTAL_AMOUNT"].Columns["FLAG"], dsExpenseDet.Tables["CHARGE_DETAILS"].Columns["FLAG"]);
                dsExpenseDet.Relations.Add(rel_TotAmtAndCharge);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dsExpenseDet;
        }

        /// <summary>
        /// Fetches the services.
        /// </summary>
        /// <param name="From">From.</param>
        /// <returns></returns>
        public DataSet FetchServices(string From = "")
        {
            WorkFlow objWF = new WorkFlow();
            string sql = null;
            sql = "SELECT SMT.SERVICE_MST_PK,SMT.SERVICE_ID,SMT.SERVICE_NAME FROM SERVICES_MST_TBL SMT";
            try
            {
                return objWF.GetDataSet(sql);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the service wise charges.
        /// </summary>
        /// <param name="SERVICE_TYPE_FK">The servic e_ typ e_ fk.</param>
        /// <param name="CHARGE_MST_PKS">The charg e_ ms t_ PKS.</param>
        /// <param name="SHOW_RECORDS">if set to <c>true</c> [sho w_ records].</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <returns></returns>
        public DataSet FetchServiceWiseCharges(int SERVICE_TYPE_FK = 0, string CHARGE_MST_PKS = "", bool SHOW_RECORDS = true, short BIZ_TYPE = 2)
        {
            return FetchSelectedServices(SERVICE_TYPE_FK, CHARGE_MST_PKS, SHOW_RECORDS, BIZ_TYPE);
        }

        /// <summary>
        /// Fetches the selected services.
        /// </summary>
        /// <param name="SERVICE_TYPE_FK">The servic e_ typ e_ fk.</param>
        /// <param name="CHARGE_MST_PKS">The charg e_ ms t_ PKS.</param>
        /// <param name="SHOW_RECORDS">if set to <c>true</c> [sho w_ records].</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <returns></returns>
        public DataSet FetchSelectedServices(int SERVICE_TYPE_FK = 0, string CHARGE_MST_PKS = "", bool SHOW_RECORDS = true, short BIZ_TYPE = 2)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsChargesDet = new DataSet();
            try
            {
                var _with91 = objWF.MyCommand.Parameters;
                _with91.Add("SERVICE_TYPE_FK_IN", (SERVICE_TYPE_FK > 0 ? SERVICE_TYPE_FK : 0)).Direction = ParameterDirection.Input;
                _with91.Add("CHARGE_MST_PKS_IN", (string.IsNullOrEmpty(CHARGE_MST_PKS.Trim()) ? "" : CHARGE_MST_PKS)).Direction = ParameterDirection.Input;
                _with91.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
                _with91.Add("SHOW_RECORDS_IN", (SHOW_RECORDS ? 1 : 0)).Direction = ParameterDirection.Input;
                _with91.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsChargesDet = objWF.GetDataSet("JOBCARD_SEC_SERVICE_PKG", "FETCH_SERVICEWISE_CHARGES");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            try
            {
                return dsChargesDet;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Income and Expense Details"

        #region "Get ESI PK"

        /// <summary>
        /// Fetches the esipk.
        /// </summary>
        /// <param name="BkgPK">The BKG pk.</param>
        /// <returns></returns>
        public string FetchESIPK(int BkgPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append(" SELECT ESI.ESI_HDR_PK");
                sb.Append(" FROM SYN_EBKG_ESI_HDR_TBL ESI, BOOKING_MST_TBL BKG ");
                sb.Append(" WHERE BKG.BOOKING_SEA_PK = ESI.BOOKING_SEA_FK");
                sb.Append(" AND BKG.BOOKING_SEA_PK=" + BkgPK);
                return Convert.ToString(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Get ESI PK"

        #region "GET FAC INVOICE"

        /// <summary>
        /// Gets the fac inv count.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public int GetFacInvCnt(string JOBPK, int BizType)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append("SELECT COUNT(INV.INVOICE_REF_NO) CNT");
                sb.Append("  FROM CONSOL_INVOICE_TBL INV, CONSOL_INVOICE_TRN_TBL INVTRN");
                sb.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                sb.Append("   AND INV.IS_FAC_INV = 1");
                sb.Append("   AND INV.BUSINESS_TYPE = " + BizType);
                sb.Append("   AND INV.PROCESS_TYPE = 1");
                sb.Append("   AND INVTRN.JOB_CARD_FK =" + JOBPK);
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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
        /// Gets the fac col count.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public int GetFacColCnt(string JOBPK, int BizType)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append("SELECT DISTINCT COUNT(COL.COLLECTIONS_REF_NO)COLCNT  ");
                sb.Append("  FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
                sb.Append("       COLLECTIONS_TBL        COL,");
                sb.Append("       COLLECTIONS_TRN_TBL    COLTRN");
                sb.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                sb.Append("   AND COL.COLLECTIONS_TBL_PK = COLTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND COLTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("   AND INV.IS_FAC_INV = 1");
                sb.Append("   AND INV.BUSINESS_TYPE = " + BizType);
                sb.Append("   AND INV.PROCESS_TYPE = 1");
                sb.Append("    AND INVTRN.JOB_CARD_FK =" + JOBPK);
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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
        /// Gets the fac set up.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public int GetFacSetUp(string JOBPK, int BizType)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append("SELECT OPR.FAC_INVOICE");
                sb.Append("  FROM OPERATOR_MST_TBL OPR, BOOKING_MST_TBL BKG, JOB_CARD_TRN JOB");
                sb.Append(" WHERE OPR.OPERATOR_MST_PK = BKG.OPERATOR_MST_FK");
                sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK);
                if (BizType == 1)
                {
                    sb.Replace("SEA", "AIR");
                    sb.Replace("OPERATOR", "AIRLINE");
                }
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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
        /// Gets the fac amt.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public string GetFacAmt(string JOBPK, int BizType)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append("SELECT SUM(NVL(GET_EX_RATE_BUY(JCOST.CURRENCY_MST_FK, " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", JOB.JOBCARD_DATE) *");
                sb.Append("           ((FAC.COMMISSION / 100) * JCOST.ESTIMATED_COST),0)) FACAMT");
                sb.Append("  FROM JOB_CARD_TRN JOB,");
                sb.Append("       JOB_TRN_COST JCOST,");
                sb.Append("       FAC_SETUP_TBL        FAC,");
                sb.Append("       BOOKING_MST_TBL      BKG,");
                sb.Append("       OPERATOR_MST_TBL     OPR");
                sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JCOST.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND JCOST.COST_ELEMENT_MST_FK = FAC.COST_ELEMENT_FK");
                sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                sb.Append("   AND BKG.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
                sb.Append("   AND FAC.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
                //sb.Append("   AND OPR.FAC_INVOICE = 0")
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBPK);
                if (BizType == 1)
                {
                    sb.Replace("SEA", "AIR");
                    sb.Replace("OPERATOR", "AIRLINE");
                }
                return Convert.ToString(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "GET FAC INVOICE"

        #region "Get Doc List"

        /// <summary>
        /// Fetches the document list.
        /// </summary>
        /// <param name="JC_TRN_FK">The j c_ tr n_ fk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchDocList(int JC_TRN_FK, int BizType, int ProcessType)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with92 = objWF.MyDataAdapter;
                _with92.SelectCommand = new OracleCommand();
                _with92.SelectCommand.Connection = objWF.MyConnection;
                _with92.SelectCommand.CommandText = objWF.MyUserName + ".JOB_DOC_TRN_PKG.JOB_DOC_TRN_LIST";
                _with92.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with92.SelectCommand.Parameters.Add("JOB_TRN_FK_IN", JC_TRN_FK).Direction = ParameterDirection.Input;
                _with92.SelectCommand.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with92.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with92.SelectCommand.Parameters.Add("BAND0_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with92.Fill(ds);

                return ds;
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

        #endregion "Get Doc List"

        #region "Update Document to Job Card"

        /// <summary>
        /// Saves the job card document.
        /// </summary>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dsDoc">The ds document.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public object SaveJobCardDoc(string JobCardPK, OracleTransaction TRAN, DataSet dsDoc, int BizType, int ProcessType)
        {
            try
            {
                arrMessage.Clear();
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                OracleCommand insCommand = new OracleCommand();
                OracleCommand updCommand = new OracleCommand();
                OracleCommand delCommand = new OracleCommand();
                int RowCnt = 0;
                int intReturn = 0;
                intReturn = 1;

                if ((dsDoc != null))
                {
                    for (RowCnt = 0; RowCnt <= dsDoc.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["JC_DOC_PK"].ToString()))
                        {
                            dsDoc.Tables[0].Rows[RowCnt]["JC_DOC_PK"] = 0;
                        }
                        if (Convert.ToInt32(dsDoc.Tables[0].Rows[RowCnt]["JC_DOC_PK"]) == 0 & (Convert.ToInt32(dsDoc.Tables[0].Rows[RowCnt]["DELFLAG"]) == 0 | Convert.ToBoolean(dsDoc.Tables[0].Rows[RowCnt]["DELFLAG"]) == false))
                        {
                            var _with93 = insCommand;
                            _with93.Connection = TRAN.Connection;
                            _with93.Transaction = TRAN;
                            _with93.CommandType = CommandType.StoredProcedure;
                            _with93.CommandText = objWF.MyUserName + ".JOB_DOC_TRN_PKG.JOB_DOC_TRN_INS";
                            _with93.Parameters.Clear();
                            _with93.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_REF_NO_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_REF_NO"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_REF_NO"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("CB_DOC_MST_FK_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_TYPE_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_TYPE_FK"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_DATE_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_DATE"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_DATE"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_IDENTITY_FK_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_IDENTITY_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_IDENTITY_FK"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_CURRENCY_FK_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_CUR_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_CUR_FK"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_AMOUNT_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_AMOUNT"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_AMOUNT"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_REC_FROM_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_FROM"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_FROM"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_REC_THROUGH_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_THROUGH_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_THROUGH_FK"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_REC_BY_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_BY_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_BY_FK"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_REC_ON_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_ON"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_ON"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("DOC_REC_REMARKS_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["REMARKS"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["REMARKS"])).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                            _with93.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with93.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            var _with94 = objWF.MyDataAdapter;
                            _with94.InsertCommand = insCommand;
                            _with94.InsertCommand.Transaction = TRAN;
                            intReturn = _with94.InsertCommand.ExecuteNonQuery();
                        }
                        else if (Convert.ToInt32(dsDoc.Tables[0].Rows[RowCnt]["JC_DOC_PK"]) > 0 & (Convert.ToInt32(dsDoc.Tables[0].Rows[RowCnt]["DELFLAG"]) == 0 | Convert.ToBoolean(dsDoc.Tables[0].Rows[RowCnt]["DELFLAG"]) == false))
                        {
                            var _with95 = updCommand;
                            _with95.Connection = TRAN.Connection;
                            _with95.Transaction = TRAN;
                            _with95.CommandType = CommandType.StoredProcedure;
                            _with95.CommandText = objWF.MyUserName + ".JOB_DOC_TRN_PKG.JOB_DOC_TRN_UPD";
                            _with95.Parameters.Clear();
                            _with95.Parameters.Add("JOB_TRN_DOC_PK_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["JC_DOC_PK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["JC_DOC_PK"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_REF_NO_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_REF_NO"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_REF_NO"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("CB_DOC_MST_FK_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_TYPE_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_TYPE_FK"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_DATE_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_DATE"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_DATE"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_IDENTITY_FK_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_IDENTITY_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_IDENTITY_FK"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_CURRENCY_FK_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_CUR_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_CUR_FK"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_AMOUNT_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["DOC_AMOUNT"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["DOC_AMOUNT"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_REC_FROM_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_FROM"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_FROM"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_REC_THROUGH_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_THROUGH_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_THROUGH_FK"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_REC_BY_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_BY_FK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_BY_FK"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_REC_ON_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_ON"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["RECEIVED_ON"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("DOC_REC_REMARKS_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["REMARKS"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["REMARKS"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("VERSION_NO_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["VERSION_NO"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["VERSION_NO"])).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with95.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            var _with96 = objWF.MyDataAdapter;
                            _with96.UpdateCommand = updCommand;
                            _with96.UpdateCommand.Transaction = TRAN;
                            intReturn = _with96.UpdateCommand.ExecuteNonQuery();
                        }
                        else
                            if (Convert.ToInt32(dsDoc.Tables[0].Rows[RowCnt]["JC_DOC_PK"]) > 0 & (Convert.ToInt32(dsDoc.Tables[0].Rows[RowCnt]["DELFLAG"]) == 1 | Convert.ToBoolean(dsDoc.Tables[0].Rows[RowCnt]["DELFLAG"]) == true))
                        {
                            var _with97 = delCommand;
                            _with97.Connection = TRAN.Connection;
                            _with97.Transaction = TRAN;
                            _with97.CommandType = CommandType.StoredProcedure;
                            _with97.CommandText = objWF.MyUserName + ".JOB_DOC_TRN_PKG.JOB_DOC_TRN_DEL";
                            _with97.Parameters.Clear();
                            _with97.Parameters.Add("JOB_TRN_DOC_PK_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["JC_DOC_PK"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["JC_DOC_PK"])).Direction = ParameterDirection.Input;
                            _with97.Parameters.Add("VERSION_NO_IN", (string.IsNullOrEmpty(dsDoc.Tables[0].Rows[RowCnt]["VERSION_NO"].ToString()) ? "" : dsDoc.Tables[0].Rows[RowCnt]["VERSION_NO"])).Direction = ParameterDirection.Input;
                            _with97.Parameters.Add("DELETED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            _with97.Parameters.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                            _with97.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with97.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            var _with98 = objWF.MyDataAdapter;
                            _with98.DeleteCommand = delCommand;
                            _with98.DeleteCommand.Transaction = TRAN;
                            intReturn = _with98.DeleteCommand.ExecuteNonQuery();
                        }
                    }
                }

                if (intReturn == 1 & arrMessage.Count == 0)
                {
                    arrMessage.Add("All Data Saved Successfully");
                }
                else
                {
                    arrMessage.Add("Error while Updating Document Details");
                }
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

        #endregion "Update Document to Job Card"

        #region "Fetch IMDG and UNO"

        /// <summary>
        /// Commodits the y_ imd g_ fetch.
        /// </summary>
        /// <param name="CommPKs">The comm p ks.</param>
        /// <returns></returns>
        public DataSet COMMODITY_IMDG_FETCH(string CommPKs = "")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder();

            sb.Append(" SELECT IMDG_CLASS_CODE,UN_NO");
            sb.Append("  FROM COMMODITY_MST_TBL CM");
            sb.Append("  WHERE CM.COMMODITY_MST_PK in (" + CommPKs + ")");
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

        #endregion "Fetch IMDG and UNO"

        /// <summary>
        /// Saves the ack update.
        /// </summary>
        /// <param name="dsAckXML">The ds ack XML.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="fiName">Name of the fi.</param>
        /// <returns></returns>
        public ArrayList SaveAckUpdate(DataSet dsAckXML, int BizType, int ProcessType, string fiName)
        {
            WorkFlow objWf = new WorkFlow();
            OracleTransaction TRAN = null;
            objWf.OpenConnection();
            TRAN = objWf.MyConnection.BeginTransaction();
            objWf.MyCommand.Transaction = TRAN;
            objWf.MyCommand.Connection = objWf.MyConnection;
            int RESULT = 0;

            try
            {
                var _with99 = objWf.MyCommand.Parameters;
                objWf.MyCommand.Parameters.Clear();
                objWf.MyCommand.CommandType = CommandType.StoredProcedure;
                objWf.MyCommand.CommandText = objWf.MyUserName + ".ACKNOWLEDGEMENT_RECEVIED_PKG.ACKNOWLEDGEMENT_RECEVIED";
                _with99.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with99.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with99.Add("RECEIVED_MESSAGENAME_IN", dsAckXML.Tables["Details"].Rows[0]["Received_MessageName"]).Direction = ParameterDirection.Input;
                _with99.Add("MESSAGE_ID_IN", dsAckXML.Tables["Details"].Rows[0]["Message_ID"]).Direction = ParameterDirection.Input;
                if (dsAckXML.Tables["Header"].Columns.Contains("Unique_ReferenceID"))
                {
                    if (!string.IsNullOrEmpty(dsAckXML.Tables["Header"].Rows[0]["Unique_ReferenceID"].ToString()))
                    {
                        _with99.Add("UNIQUE_REFERENCEID_IN", dsAckXML.Tables["Header"].Rows[0]["Unique_ReferenceID"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with99.Add("UNIQUE_REFERENCEID_IN", "").Direction = ParameterDirection.Input;
                    }
                }
                else
                {
                    _with99.Add("UNIQUE_REFERENCEID_IN", "").Direction = ParameterDirection.Input;
                }
                _with99.Add("FILE_NAME_IN", fiName).Direction = ParameterDirection.Input;
                _with99.Add("RESPONSE_TIME_IN", dsAckXML.Tables["Details"].Rows[0]["Response_Time"]).Direction = ParameterDirection.Input;
                _with99.Add("TRANSACTION_FLAG_IN", dsAckXML.Tables["Details"].Rows[0]["Transaction_Flag"]).Direction = ParameterDirection.Input;
                _with99.Add("SYSTEM_MESSAGE_IN", dsAckXML.Tables["Details"].Rows[0]["System_Message"]).Direction = ParameterDirection.Input;

                RESULT = objWf.MyCommand.ExecuteNonQuery();
                arrMessage.Add("All data saved successfully");
                TRAN.Commit();
                return arrMessage;
            }
            catch (Exception sqlExp)
            {
                TRAN.Rollback();
                ErrorMessage = sqlExp.Message;
                arrMessage.Add(sqlExp.Message);
                return arrMessage;
            }
        }
    }
}