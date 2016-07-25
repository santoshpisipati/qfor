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

using Oracle.DataAccess.Client;
using System;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_EDItoCustomsImport : CommonFeatures
    {
        #region "Fetch Grid Details"

        /// <summary>
        /// Fetches the grid details.
        /// </summary>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="Flag">The flag.</param>
        /// <param name="VslTrnPk">The VSL TRN pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <returns></returns>
        public DataSet FetchGridDetails(Int32 TotalPage = 0, Int32 CurrentPage = 0, int Flag = 0, string VslTrnPk = "", string PodPK = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            Int32 TotalRecords = default(Int32);
            try
            {
                sb.Append("SELECT JCSIT.JOB_CARD_TRN_PK JCPK,");
                sb.Append("               JCSIT.JOBCARD_REF_NO,");
                sb.Append("               JCSIT.HBL_HAWB_REF_NO HBL_REF_NO,");
                sb.Append("               JCSIT.MBL_MAWB_REF_NO MBL_REF_NO,");
                sb.Append("               POL.PORT_NAME POL,");
                sb.Append("               POD.PORT_NAME POD,");
                sb.Append("               SUM(NVL(CTMT.TEU_FACTOR, 0)) TEU_FACTOR,");
                sb.Append("               SUM(NVL(JTSIC.PACK_COUNT, 0)) PACK_COUNT,");
                sb.Append("               (CASE");
                sb.Append("                 WHEN JCSIT.CARGO_TYPE = 1 THEN");
                sb.Append("                  SUM(NVL(JTSIC.GROSS_WEIGHT, 0))");
                sb.Append("                 ELSE");
                sb.Append("                  SUM(NVL(JTSIC.CHARGEABLE_WEIGHT, 0))");
                sb.Append("               END) GROSS_WEIGHT,");
                sb.Append("               SUM(NVL(JTSIC.VOLUME_IN_CBM, 0)) VOLUME_IN_CBM");
                sb.Append("          FROM JOB_CARD_TRN   JCSIT,");
                sb.Append("               JOB_TRN_CONT   JTSIC,");
                sb.Append("               PORT_MST_TBL           POL,");
                sb.Append("               PORT_MST_TBL           POD,");
                sb.Append("               CONTAINER_TYPE_MST_TBL CTMT");
                sb.Append("         WHERE JCSIT.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("           AND JCSIT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("           AND JTSIC.JOB_CARD_TRN_FK = JCSIT.JOB_CARD_TRN_PK");
                sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK = JTSIC.CONTAINER_TYPE_MST_FK");
                sb.Append("           AND JCSIT.VOYAGE_TRN_FK IS NOT NULL");
                sb.Append("           AND JCSIT.PROCESS_TYPE = 2");
                sb.Append("           AND POD.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                if (Flag != 0)
                {
                    sb.Append(" AND 1=1 ");
                }
                else
                {
                    sb.Append(" AND 1=2 ");
                }
                if (!string.IsNullOrEmpty(VslTrnPk))
                {
                    sb.Append(" AND JCSIT.VOYAGE_TRN_FK=" + VslTrnPk);
                }
                if (!string.IsNullOrEmpty(PodPK))
                {
                    sb.Append("  AND JCSIT.PORT_MST_POD_FK =" + PodPK);
                }
                sb.Append("         GROUP BY JCSIT.JOB_CARD_TRN_PK,");
                sb.Append("                  JCSIT.JOBCARD_REF_NO,");
                sb.Append("                  JCSIT.HBL_HAWB_REF_NO,");
                sb.Append("                  JCSIT.MBL_MAWB_REF_NO,");
                sb.Append("                  POL.PORT_NAME,");
                sb.Append("                  POD.PORT_NAME,");
                sb.Append("                  JCSIT.CARGO_TYPE");

                strCount.Append(" select count(*) from ");
                strSQL = sb.ToString();
                strCount.Append((" (" + sb.ToString() + ")"));
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
                strCount.Remove(0, strCount.Length);

                System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
                sqlstr2.Append(" SELECT ROWNUM SLNR, Q.* FROM ");
                sqlstr2.Append(" (" + sb.ToString() + "");
                sqlstr2.Append(" ORDER BY HBL_REF_NO DESC) Q");
                return objWF.GetDataSet(sqlstr2.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Fetch Grid Details"

        #region "Get Vessel Details"

        /// <summary>
        /// Gets the vessel details.
        /// </summary>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <returns></returns>
        public object getVesselDetails(string VesselPK = "", string PodPK = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT VVT.VOYAGE_TRN_PK,");
                sb.Append("                VT.VESSEL_ID,");
                sb.Append("                VT.VESSEL_NAME,");
                sb.Append("                VVT.VOYAGE,");
                sb.Append("                POL.PORT_ID         POL,");
                sb.Append("                POD.PORT_ID         POD,");
                sb.Append("                LPORT.PORT_ID       LASTPORT,");
                sb.Append("                VVT.CAPTAIN_NAME,");
                sb.Append("                VVT.GRT,");
                sb.Append("                VVT.NRT,");
                sb.Append("                VVT.ATA_POD,");
                sb.Append("     (SELECT COUNT(DISTINCT JOB.HBL_HAWB_REF_NO)");
                sb.Append("                   FROM JOB_CARD_TRN JOB");
                sb.Append("                  WHERE JOB.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK) TOTALITEMS,");
                sb.Append("             OPR.OPERATOR_ID, LMT.OFFICE_NAME,");
                sb.Append("            LMT.ADDRESS_LINE1 || LMT.ADDRESS_LINE2 || LMT.ADDRESS_LINE3 ADDRESS");
                sb.Append(" FROM VESSEL_VOYAGE_TBL    VT,");
                sb.Append("       VESSEL_VOYAGE_TRN    VVT,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL         POD,");
                sb.Append("       PORT_MST_TBL         LPORT,");
                sb.Append("       JOB_CARD_TRN JCSIT,");
                sb.Append("       OPERATOR_MST_TBL     OPR,");
                sb.Append("       LOCATION_MST_TBL     LMT,");
                sb.Append("       USER_MST_TBL         UMT");
                sb.Append(" WHERE JCSIT.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JCSIT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND VT.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK");
                sb.Append("   AND LPORT.PORT_MST_PK(+) = VVT.PORT_CALL_MST_FK");
                sb.Append("   AND VT.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
                sb.Append("   AND JCSIT.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("    AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
                sb.Append("    AND JCSIT.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK =" + VesselPK);
                sb.Append("   AND POD.PORT_MST_PK=" + PodPK);
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Get Vessel Details"

        #region "Get Cargo Details"

        /// <summary>
        /// Gets the cargo details.
        /// </summary>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <returns></returns>
        public object GetCargoDetails(string VesselPK = "", string PodPK = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT  SUBSTR(JCSIT.MBL_MAWB_REF_NO, 0, 20) MBL_REF_NO");
                sb.Append("  FROM JOB_CARD_TRN    JCSIT,");
                sb.Append("       JOB_TRN_CONT    JTSIC,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            PFD,");
                sb.Append("       VESSEL_VOYAGE_TBL       VT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VVT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS   CCD,");
                sb.Append("       CUSTOMER_MST_TBL        NOTIFY,");
                sb.Append("       CUSTOMER_CONTACT_DTLS   NTCCD,");
                sb.Append("       PACK_TYPE_MST_TBL       PMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM,");
                sb.Append("       COMMODITY_MST_TBL       COMT");
                sb.Append(" WHERE JCSIT.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JCSIT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND JCSIT.PFD_FK = PFD.PORT_MST_PK(+)");
                sb.Append("   AND JTSIC.JOB_CARD_TRN_FK = JCSIT.JOB_CARD_TRN_PK");
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK IS NOT NULL");
                sb.Append("   AND VT.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND JCSIT.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND VVT.VOYAGE_TRN_PK = JCSIT.VOYAGE_TRN_FK");
                sb.Append("   AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND NOTIFY.CUSTOMER_MST_PK = NTCCD.CUSTOMER_MST_FK(+)");
                sb.Append("   AND JCSIT.NOTIFY1_CUST_MST_FK = NOTIFY.CUSTOMER_MST_PK(+)");
                sb.Append("   AND JTSIC.PACK_TYPE_MST_FK = PMT.PACK_TYPE_MST_PK(+)");
                sb.Append("   AND JCSIT.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND JTSIC.COMMODITY_MST_FK = COMT.COMMODITY_MST_PK(+)");
                sb.Append("   AND JCSIT.PROCESS_TYPE = 2");
                sb.Append("   AND JCSIT.MBL_MAWB_REF_NO IS NOT NULL");
                sb.Append("   AND POD.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK = " + VesselPK);
                sb.Append("   AND POD.PORT_MST_PK=" + PodPK);
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Gets the HBL details.
        /// </summary>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <returns></returns>
        public object GetHBLDetails(string VesselPK = "", string PodPK = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT JCSIT.JOB_CARD_TRN_PK,");
                sb.Append("                JCSIT.JOBCARD_REF_NO,");
                sb.Append("                SUBSTR(JCSIT.HBL_HAWB_REF_NO, 0, 20) HBL_REF_NO,");
                sb.Append("                SUBSTR(JCSIT.MBL_MAWB_REF_NO, 0, 20) MBL_REF_NO,");
                sb.Append("                SUBSTR(VT.VESSEL_ID, 0, 7) VESSEL_ID,");
                sb.Append("                SUBSTR(VVT.VOYAGE, 0, 10) VOYAGE,");
                sb.Append("                SUBSTR(POL.PORT_ID, 0, 6) POL,");
                sb.Append("                SUBSTR(POD.PORT_ID, 0, 6) POD,");
                sb.Append("                TO_CHAR(JCSIT.HBL_HAWB_DATE, 'DDMMYYYY') HBLDATE,");
                sb.Append("                TO_CHAR(JCSIT.MBL_MAWB_DATE, 'DDMMYYYY') MBLDATE,");
                sb.Append("                SUBSTR(CMT.CUSTOMER_NAME, 0, 35) CONSIGNEE,");
                sb.Append("                SUBSTR(REPLACE(CCD.ADM_ADDRESS_1, CHR(13), ''), 0, 35) CONGADD1,");
                sb.Append("                SUBSTR(REPLACE(CCD.ADM_ADDRESS_2, CHR(13), ''), 36, 70) CONGADD2,");
                sb.Append("                SUBSTR(REPLACE(CCD.ADM_ADDRESS_3, CHR(13), ''), 71, 105) CONGADD3,");
                sb.Append("                SUBSTR(NOTIFY.CUSTOMER_NAME, 0, 35) NOTIFY,");
                sb.Append("                SUBSTR(REPLACE(NTCCD.ADM_ADDRESS_1, CHR(13), ''), 0, 35) NOTIFYADD1,");
                sb.Append("                SUBSTR(REPLACE(NTCCD.ADM_ADDRESS_2, CHR(13), ''), 36, 70) NOTIFYADD2,");
                sb.Append("                SUBSTR(REPLACE(NTCCD.ADM_ADDRESS_3, CHR(13), ''), 71, 105) NOTIFYADD3,");
                sb.Append("                JCSIT.NOTIFY1_CUST_MST_FK,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN JCSIT.CARGO_TYPE = 1 THEN");
                sb.Append("                   'C'");
                sb.Append("                  WHEN JCSIT.CARGO_TYPE = 2 THEN");
                sb.Append("                   'P'");
                sb.Append("                  ELSE");
                sb.Append("                   'DB'");
                sb.Append("                END) NATUREOFCARGO,");
                sb.Append("                SUM(NVL(SUBSTR(JTSIC.PACK_COUNT, 0, 8), 0)) PACK_COUNT,");
                sb.Append("                COUNT(JTSIC.PACK_TYPE_MST_FK) PACKTYPE_COUNT,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN COUNT(JTSIC.PACK_TYPE_MST_FK) > 1 THEN");
                sb.Append("                   'PCS'");
                sb.Append("                  ELSE");
                sb.Append("                    MAX(PMT.PACK_TYPE_ID)");
                sb.Append("                END) PACKTYPE,");
                sb.Append("                SUM(NVL(SUBSTR(JTSIC.GROSS_WEIGHT, 0, 16), 0)) GROSS_WEIGHT,");
                sb.Append("                SUM(NVL(SUBSTR(JTSIC.VOLUME_IN_CBM, 0, 16), 0)) VOLUME_IN_CBM,");
                sb.Append("                SUBSTR(REPLACE(JCSIT.MARKS_NUMBERS, CHR(13),''), 0, 300) MARKS_NUMBERS,");
                sb.Append("                SUBSTR(REPLACE(JCSIT.GOODS_DESCRIPTION,CHR(13),''), 0, 350) GOODS_DESCRIPTION,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN (POD.PORT_ID = PFD.PORT_ID) OR PFD.PORT_ID IS NULL THEN");
                sb.Append("                   'LC'");
                sb.Append("                  WHEN (POD.PORT_ID <> PFD.PORT_ID) AND");
                sb.Append("                       PFD.PORT_ID IS NOT NULL AND");
                sb.Append("                       (POD.COUNTRY_MST_FK <> PFD.COUNTRY_MST_FK) THEN");
                sb.Append("                   'TC'");
                sb.Append("                  ELSE");
                sb.Append("                   'TI'");
                sb.Append("                END) CARGOMOVE,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN COMM.COMMODITY_GROUP_CODE = 'HAZARDOUS' THEN");
                sb.Append("                   COMT.UN_NO");
                sb.Append("                  ELSE");
                sb.Append("                   'ZZZZ'");
                sb.Append("                END) UNOCODE,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN COMM.COMMODITY_GROUP_CODE = 'HAZARDOUS' THEN");
                sb.Append("                   COMT.IMDG_CLASS_CODE");
                sb.Append("                  ELSE");
                sb.Append("                   'ZZZ'");
                sb.Append("                END) IMOCODE");
                sb.Append("  FROM JOB_CARD_TRN    JCSIT,");
                sb.Append("       JOB_TRN_CONT    JTSIC,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            PFD,");
                sb.Append("       VESSEL_VOYAGE_TBL       VT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VVT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS   CCD,");
                sb.Append("       CUSTOMER_MST_TBL        NOTIFY,");
                sb.Append("       CUSTOMER_CONTACT_DTLS   NTCCD,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM,");
                sb.Append("       COMMODITY_MST_TBL       COMT,");
                sb.Append("       PACK_TYPE_MST_TBL       PMT");
                sb.Append(" WHERE JCSIT.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JCSIT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND JCSIT.PFD_FK = PFD.PORT_MST_PK(+)");
                sb.Append("   AND JTSIC.JOB_CARD_TRN_FK = JCSIT.JOB_CARD_TRN_PK");
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK IS NOT NULL");
                sb.Append("   AND VT.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND JCSIT.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND VVT.VOYAGE_TRN_PK = JCSIT.VOYAGE_TRN_FK");
                sb.Append("   AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND NOTIFY.CUSTOMER_MST_PK = NTCCD.CUSTOMER_MST_FK(+)");
                sb.Append("   AND JCSIT.NOTIFY1_CUST_MST_FK = NOTIFY.CUSTOMER_MST_PK(+)");
                sb.Append("   AND JCSIT.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND JTSIC.COMMODITY_MST_FK = COMT.COMMODITY_MST_PK(+)");
                sb.Append("   AND JTSIC.PACK_TYPE_MST_FK = PMT.PACK_TYPE_MST_PK(+)");
                sb.Append("    AND JCSIT.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK = " + VesselPK);
                sb.Append("   AND POD.PORT_MST_PK = " + PodPK);
                sb.Append(" GROUP BY JCSIT.JOB_CARD_TRN_PK,");
                sb.Append("          JCSIT.JOBCARD_REF_NO,");
                sb.Append("          JCSIT.HBL_HAWB_REF_NO,");
                sb.Append("          JCSIT.MBL_MAWB_REF_NO,");
                sb.Append("          VT.VESSEL_ID,");
                sb.Append("          VVT.VOYAGE,");
                sb.Append("          POL.PORT_ID,");
                sb.Append("          POD.PORT_ID,");
                sb.Append("          JCSIT.HBL_HAWB_DATE,MBL_MAWB_DATE,");
                sb.Append("          CMT.CUSTOMER_NAME,");
                sb.Append("          CCD.ADM_ADDRESS_1,");
                sb.Append("          CCD.ADM_ADDRESS_2,");
                sb.Append("          CCD.ADM_ADDRESS_3,");
                sb.Append("          NOTIFY.CUSTOMER_NAME,");
                sb.Append("          NTCCD.ADM_ADDRESS_1,");
                sb.Append("          NTCCD.ADM_ADDRESS_2,");
                sb.Append("          NTCCD.ADM_ADDRESS_3,");
                sb.Append("          JCSIT.NOTIFY1_CUST_MST_FK,");
                sb.Append("          JCSIT.CARGO_TYPE,");
                sb.Append("          POD.PORT_ID,");
                sb.Append("          PFD.PORT_ID,");
                sb.Append("          POD.COUNTRY_MST_FK,");
                sb.Append("          PFD.COUNTRY_MST_FK,");
                sb.Append("          COMM.COMMODITY_GROUP_CODE,");
                sb.Append("          COMT.UN_NO,");
                sb.Append("          COMT.IMDG_CLASS_CODE,");
                sb.Append("          JCSIT.MARKS_NUMBERS,");
                sb.Append("          JCSIT.GOODS_DESCRIPTION");
                sb.Append("   ORDER BY HBL_REF_NO DESC");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Get Cargo Details"

        #region "Get Container Details"

        /// <summary>
        /// Gets the container details.
        /// </summary>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <returns></returns>
        public object GetContainerDetails(string VesselPK = "", string PodPK = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT JCSIT.JOB_CARD_TRN_PK,");
                sb.Append("                JCSIT.JOBCARD_REF_NO,");
                sb.Append("                SUBSTR(JCSIT.HBL_HAWB_REF_NO, 0, 20) HBL_REF_NO,");
                sb.Append("                SUBSTR(JCSIT.MBL_MAWB_REF_NO, 0, 20) MBL_REF_NO,");
                sb.Append("                SUBSTR(VT.VESSEL_ID, 0, 7) VESSEL_ID,");
                sb.Append("                SUBSTR(VVT.VOYAGE, 0, 10) VOYAGE,");
                sb.Append("                JTSIC.CONTAINER_NUMBER,");
                sb.Append("                JTSIC.SEAL_NUMBER,");
                sb.Append("                JTSIC.PACK_COUNT,");
                sb.Append("               (CASE");
                sb.Append("                  WHEN JCSIT.CARGO_TYPE = 1 THEN");
                sb.Append("                   'FCL'");
                sb.Append("                  ELSE");
                sb.Append("                   'LCL'");
                sb.Append("                END) CONTSTATUS,");
                sb.Append("                JTSIC.GROSS_WEIGHT,");
                sb.Append("                CTMT.ISO_NUMBER,");
                sb.Append("                 POD.PORT_ID POD,");
                sb.Append("               (CASE");
                sb.Append("                  WHEN PFD.PORT_ID IS NULL THEN");
                sb.Append("                   POD.PORT_ID");
                sb.Append("                  ELSE");
                sb.Append("                   PFD.PORT_ID");
                sb.Append("                END) PFD,");
                sb.Append("                COMM.COMMODITY_GROUP_CODE,COMT.IMDG_CLASS_CODE");
                sb.Append("  FROM JOB_CARD_TRN   JCSIT,");
                sb.Append("       JOB_TRN_CONT   JTSIC,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       PORT_MST_TBL            PFD,");
                sb.Append("       VESSEL_VOYAGE_TBL      VT,");
                sb.Append("       VESSEL_VOYAGE_TRN      VVT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL CTMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM,");
                sb.Append("       COMMODITY_MST_TBL COMT ");
                sb.Append(" WHERE JCSIT.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JCSIT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND JTSIC.JOB_CARD_TRN_FK = JCSIT.JOB_CARD_TRN_PK");
                sb.Append("   AND VT.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK");
                sb.Append("   AND JTSIC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK");
                sb.Append("   AND JCSIT.PFD_FK = PFD.PORT_MST_PK(+)");
                sb.Append("   AND JCSIT.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND JTSIC.COMMODITY_MST_FK=COMT.COMMODITY_MST_PK(+)");
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK IS NOT NULL");
                sb.Append("    AND JCSIT.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK =" + VesselPK);
                sb.Append("   AND POD.PORT_MST_PK = " + PodPK);
                sb.Append(" ORDER BY  HBL_REF_NO DESC");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Gets the HBLS.
        /// </summary>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <returns></returns>
        public DataSet GetHbls(string VesselPK = "", string PodPK = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT");
                sb.Append("        SUBSTR(JCSIT.HBL_HAWB_REF_NO, 0, 20) HBL_REF_NO");
                //sb.Append("                JTSIC.CONTAINER_NUMBER ")
                sb.Append("  FROM JOB_CARD_TRN   JCSIT,");
                sb.Append("       JOB_TRN_CONT   JTSIC,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       VESSEL_VOYAGE_TBL      VT,");
                sb.Append("       VESSEL_VOYAGE_TRN      VVT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL CTMT");
                sb.Append(" WHERE JCSIT.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JCSIT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND JTSIC.JOB_CARD_TRN_FK = JCSIT.JOB_CARD_TRN_PK");
                sb.Append("   AND VT.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK");
                sb.Append("   AND JTSIC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK");
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK IS NOT NULL");
                sb.Append("    AND JCSIT.PROCESS_TYPE = 2");
                sb.Append("    AND JCSIT.HBL_HAWB_REF_NO IS NOT NULL");
                sb.Append("   AND POD.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK =" + VesselPK);
                sb.Append("   AND POD.PORT_MST_PK = " + PodPK);
                sb.Append(" ORDER BY  HBL_REF_NO DESC");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Get Container Details"
    }
}