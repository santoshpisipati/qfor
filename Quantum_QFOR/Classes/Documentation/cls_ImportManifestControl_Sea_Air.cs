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

using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class ClsImportManifestControl_Sea_Air : CommonFeatures
    {
        #region " FetchAllImpManifest "

        /// <summary>
        /// Fetches all imp manifest.
        /// </summary>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="PolPK">The pol pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <param name="txtHbl">The text HBL.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="loc">The loc.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="SearchFlg">The search FLG.</param>
        /// <returns></returns>
        public DataSet FetchAllImpManifest(string VesselPK = "", int JobPK = 0, int PolPK = 0, int PodPK = 0, string txtHbl = "", string RefType = "", string RefNr = "", Int32 Export = 0, int CurrentPage = 0, int TotalPage = 0,
        int loc = 0, int BizType = 0, string SearchFlg = "S")
        {
            Int32 Last = default(Int32);
            Int32 Start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("SELECT *");
            sb.Append("  FROM (SELECT ROWNUM SL_NO, Q.*");
            sb.Append("     FROM ((SELECT QRY.JCIPK,");
            sb.Append("                        QRY.JCRNR,");
            sb.Append("                        to_date(QRY.JCDT,dateformat) JCDT, ");
            sb.Append("                        BIZ_TYPE,");
            sb.Append("                        QRY.HRNR,");
            sb.Append("                        QRY.HDATE,");
            sb.Append("                        QRY.HSURR,");
            sb.Append("                        QRY.POL,");
            sb.Append("                        QRY.POD,");
            sb.Append("                        QRY.VF_STATUS,");
            sb.Append("                        QRY.ATA_ETA,");
            sb.Append("                        QRY.MANIFEST,");
            sb.Append("                        QRY.CAN,");
            sb.Append("                        QRY.DISCHARGE,");
            sb.Append("                        QRY.INV,");
            sb.Append("                        QRY.CIPK,");
            sb.Append("                        QRY.INRNR,");
            sb.Append("                        (CASE");
            sb.Append("                          WHEN QRY.CTPK > 0 THEN");
            sb.Append("                           'a'");
            sb.Append("                          ELSE");
            sb.Append("                           'r'");
            sb.Append("                        END) COLL,");
            sb.Append("                        QRY.CTPK,");
            sb.Append("                        QRY.COLNR,");
            sb.Append("                        QRY.CARGO_TYPE,");
            sb.Append("                        QRY.JCTYPE,");
            sb.Append("                        BIZ_TYPE_VALUE");
            sb.Append("                   FROM (SELECT DISTINCT JOB.JOB_CARD_TRN_PK JCIPK,");
            sb.Append("                                         JOB.JOBCARD_REF_NO JCRNR,");
            sb.Append("                                         JOB.JOBCARD_DATE JCDT,");
            sb.Append("                                         (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE, ");
            sb.Append("                                         JOB.HBL_HAWB_REF_NO HRNR,");
            sb.Append("                                         TO_DATE(JOB.HBL_HAWB_DATE, DATEFORMAT) HDATE,");
            sb.Append("                                         CASE");
            sb.Append("                                           WHEN JOB.HBL_HAWB_SURRDT IS NOT NULL THEN");
            sb.Append("                                            'a'");
            sb.Append("                                           ELSE");
            sb.Append("                                            'r'");
            sb.Append("                                         END HSURR,");
            sb.Append("                                         POL.PORT_ID POL,");
            sb.Append("                                         POD.PORT_ID POD,");
            sb.Append("                                         CASE");
            sb.Append("                                           WHEN JOB.ETA_DATE IS NOT NULL THEN");
            sb.Append("                                            'Arrived'");
            sb.Append("                                           ELSE");
            sb.Append("                                            'Not Arrived'");
            sb.Append("                                         END VF_STATUS,");
            sb.Append("                                         CASE");
            sb.Append("                                             WHEN JOB.ETA_DATE IS NOT NULL THEN");
            sb.Append("                                            JOB.ETA_DATE");
            sb.Append("                                           ELSE");
            sb.Append("                                            NULL");
            sb.Append("                                         END ATA_ETA,");
            sb.Append("                                         'r' MANIFEST,");
            sb.Append("                                         CASE");
            sb.Append("                                           WHEN CMT.BL_REF_NO IS NOT NULL THEN");
            sb.Append("                                            'a'");
            sb.Append("                                           ELSE");
            sb.Append("                                            'r'");
            sb.Append("                                         END CAN,");
            sb.Append("                                        CASE WHEN DOMT.DELIVERY_ORDER_DATE IS NOT NULL THEN");
            sb.Append("                                         TO_CHAR(DOMT.DELIVERY_ORDER_DATE,");
            sb.Append("                                                 DATEFORMAT) ELSE NULL END DISCHARGE,");
            sb.Append("                                         CASE");
            sb.Append("                                           WHEN INVTRN.JOB_CARD_FK IS NOT NULL THEN");
            sb.Append("                                            'a'");
            sb.Append("                                           ELSE");
            sb.Append("                                            'r'");
            sb.Append("                                         END INV,");
            sb.Append("                                        CASE WHEN CON.CONSOL_INVOICE_PK IS NOT NULL THEN");
            sb.Append("                                         CON.CONSOL_INVOICE_PK ELSE 0 END CIPK,");
            sb.Append("                                        CASE WHEN CON.INVOICE_REF_NO IS NOT NULL THEN");
            sb.Append("                                         CON.INVOICE_REF_NO ELSE '' END INRNR,");
            sb.Append("                                         NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("                                               FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("                                              WHERE CTRN.INVOICE_REF_NR LIKE");
            sb.Append("                                                    CON.INVOICE_REF_NO),");
            sb.Append("                                             0) RECIEVED,");
            sb.Append("                                        CASE WHEN COL.COLLECTIONS_TBL_PK IS NOT NULL THEN");
            sb.Append("                                         COL.COLLECTIONS_TBL_PK ELSE 0 END CTPK,");
            sb.Append("                                        CASE WHEN COL.COLLECTIONS_REF_NO IS NOT NULL THEN");
            sb.Append("                                         COL.COLLECTIONS_REF_NO ELSE '' END COLNR,");
            sb.Append("                                         JOB.CARGO_TYPE CARGO_TYPE,");
            sb.Append("                                         JOB.JC_AUTO_MANUAL JCTYPE,");
            sb.Append("                                         JOB.BUSINESS_TYPE BIZ_TYPE_VALUE ");
            sb.Append("                           FROM JOB_CARD_TRN   JOB,");
            sb.Append("                                CONSOL_INVOICE_TBL     CON,");
            sb.Append("                                CONSOL_INVOICE_TRN_TBL INVTRN,");
            sb.Append("                                COLLECTIONS_TBL        COL,");
            sb.Append("                                COLLECTIONS_TRN_TBL    COLTRN,");
            sb.Append("                                PORT_MST_TBL           POL,");
            sb.Append("                                PORT_MST_TBL           POD,");
            sb.Append("                                CAN_MST_TBL            CMT,");
            sb.Append("                                DELIVERY_ORDER_MST_TBL DOMT,");
            sb.Append("                                VESSEL_VOYAGE_TBL VVT,");
            sb.Append("                                VESSEL_VOYAGE_TRN VVTRN,");
            sb.Append("                                CUSTOMER_MST_TBL       CONS,");
            sb.Append("                                CUSTOMER_MST_TBL       SHIP,");
            if (RefType == "5")
            {
                if (!string.IsNullOrEmpty(RefNr))
                {
                    sb.Append("       (SELECT JCONT.JOB_CARD_TRN_FK");
                    sb.Append("          FROM JOB_TRN_CONT JCONT");
                    sb.Append("         WHERE JCONT.COMMODITY_MST_FKS IN");
                    sb.Append("               (SELECT C.COMMODITY_MST_PK || '' FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_ID LIKE '" + (SearchFlg == "C" ? "%" + RefNr.ToUpper().Replace("'", "''") : RefNr.ToUpper().Replace("'", "''")) + "%')) JOB_CONT,");
                }
            }
            sb.Append("                                COMMODITY_GROUP_MST_TBL COMG");
            sb.Append("                          WHERE CON.CONSOL_INVOICE_PK(+) =");
            sb.Append("                                INVTRN.CONSOL_INVOICE_FK");
            sb.Append("                            AND JOB.JOB_CARD_TRN_PK =");
            sb.Append("                                INVTRN.JOB_CARD_FK(+)");
            sb.Append("                            AND COL.COLLECTIONS_TBL_PK(+) =");
            sb.Append("                                COLTRN.COLLECTIONS_TBL_FK");
            sb.Append("                            AND CON.INVOICE_REF_NO = COLTRN.INVOICE_REF_NR(+)");
            sb.Append("                            AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("                            AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("                            AND DOMT.JOB_CARD_MST_FK(+) = JOB.JOB_CARD_TRN_PK");
            sb.Append("                            AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VVTRN.VESSEL_VOYAGE_TBL_FK");
            sb.Append("                            AND VVTRN.VOYAGE_TRN_PK(+) = JOB.VOYAGE_TRN_FK");
            sb.Append("                             AND JOB.SHIPPER_CUST_MST_FK=SHIP.CUSTOMER_MST_PK");
            sb.Append("                             AND JOB.CONSIGNEE_CUST_MST_FK=CONS.CUSTOMER_MST_PK");
            sb.Append("                             AND JOB.COMMODITY_GROUP_FK=COMG.COMMODITY_GROUP_PK(+)");
            sb.Append("                             AND JOB.VOYAGE_TRN_FK IS NOT NULL");
            sb.Append("     AND POD.PORT_MST_PK IN (SELECT LWPT.PORT_MST_FK FROM LOCATION_WORKING_PORTS_TRN LWPT");
            sb.Append("     WHERE LWPT.ACTIVE = 1");
            sb.Append("     AND LWPT.LOCATION_MST_FK = " + loc + ")");

            if (!string.IsNullOrEmpty(RefNr))
            {
                ///Shipper
                if (RefType == "1")
                {
                    sb.Append(" AND UPPER(SHIP.CUSTOMER_ID) LIKE '" + (SearchFlg == "C" ? "%" + RefNr.ToUpper().Replace("'", "''") : RefNr.ToUpper().Replace("'", "''")) + "%'");
                    /// Consignee
                }
                else if (RefType == "2")
                {
                    sb.Append(" AND UPPER(CONS.CUSTOMER_ID) LIKE '" + (SearchFlg == "C" ? "%" + RefNr.ToUpper().Replace("'", "''") : RefNr.ToUpper().Replace("'", "''")) + "%'");
                    /// Cargo Type
                }
                else if (RefType == "3")
                {
                    if (RefNr == "FCL" | RefNr == "fcl")
                    {
                        sb.Append(" AND  JOB.CARGO_TYPE = 1");
                    }
                    else if (RefNr == "LCL" | RefNr == "lcl")
                    {
                        sb.Append(" AND  JOB.CARGO_TYPE = 2");
                    }
                    else if (RefNr == "BBC" | RefNr == "Break Bulk")
                    {
                        sb.Append(" AND  JOB.CARGO_TYPE = 4");
                    }
                    else
                    {
                        sb.Append(" AND 1 = 2 ");
                    }
                    /// Commodity Grp
                }
                else if (RefType == "4")
                {
                    sb.Append(" AND UPPER(COMG.COMMODITY_GROUP_DESC) LIKE '" + (SearchFlg == "C" ? "%" + RefNr.ToUpper().Replace("'", "''") : RefNr.ToUpper().Replace("'", "''")) + "%'");
                    /// Commodity
                }
                else if (RefType == "5")
                {
                    sb.Append("   AND JOB_CONT.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                    /// Invoice Nr.
                }
                else if (RefType == "6")
                {
                    sb.Append(" AND UPPER(CON.INVOICE_REF_NO) LIKE '" + (SearchFlg == "C" ? "%" + RefNr.ToUpper().Replace("'", "''") : RefNr.ToUpper().Replace("'", "''")) + "%'");
                    /// Collection Nr.
                }
                else if (RefType == "7")
                {
                    sb.Append(" AND UPPER(COL.COLLECTIONS_REF_NO) LIKE '" + (SearchFlg == "C" ? "%" + RefNr.ToUpper().Replace("'", "''") : RefNr.ToUpper().Replace("'", "''")) + "%'");
                }
            }
            if (Convert.ToInt32(VesselPK) > 0)
            {
                if (BizType == 2)
                {
                    sb.Append(" AND VVTRN.VOYAGE_TRN_PK = " + VesselPK + "");
                }
                else if (BizType == 1)
                {
                    sb.Append(" AND JOB.CARRIER_MST_FK =  " + VesselPK + " ");
                }
                else
                {
                    sb.Append(" AND (VVTRN.VOYAGE_TRN_PK = " + VesselPK + "  OR  JOB.CARRIER_MST_FK =  " + VesselPK + " )");
                }
            }

            if (JobPK > 0)
            {
                sb.Append("  AND JOB.JOB_CARD_TRN_PK = " + JobPK + "");
            }

            if (PolPK > 0)
            {
                sb.Append("  AND POL.PORT_MST_PK = " + PolPK + "");
            }

            if (PodPK > 0)
            {
                sb.Append("  AND POD.PORT_MST_PK = " + PodPK + "");
            }

            if (!string.IsNullOrEmpty(txtHbl.Trim()))
            {
                sb.Append(" AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + txtHbl.Trim().ToUpper() + "%'");
            }

            sb.Append("  AND JOB.JOB_CARD_TRN_PK = CMT.JOB_CARD_FK(+) ");
            sb.Append("  AND JOB.PROCESS_TYPE = 2 ");
            if (BizType > 0)
            {
                sb.Append(" AND  JOB.BUSINESS_TYPE=" + BizType);
            }
            sb.Append("  ) QRY) Q)) ");

            try
            {
                DataTable tbl = new DataTable();
                tbl = objWF.GetDataTable(sb.ToString());
                TotalRecords = (Int32)tbl.Rows.Count;
                TotalPage = TotalRecords / RecordsPerPage;
                if (TotalRecords % RecordsPerPage != 0)
                {
                    TotalPage += 1;
                }
                if (CurrentPage > TotalPage)
                    CurrentPage = 1;
                if (TotalRecords == 0)
                    CurrentPage = 0;
                Last = CurrentPage * RecordsPerPage;
                Start = (CurrentPage - 1) * RecordsPerPage + 1;

                if (Export == 0)
                {
                    sb.Append("  WHERE SL_NO  Between " + Start + " and " + Last + "");
                }

                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion " FetchAllImpManifest "

        #region "Fetch On Search for Import Manifest Air"

        /// <summary>
        /// Fetches all imp air manifest.
        /// </summary>
        /// <param name="AirLinePK">The air line pk.</param>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="AOOPK">The aoopk.</param>
        /// <param name="AODPK">The aodpk.</param>
        /// <param name="txtHawbNr">The text hawb nr.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BCurrencyPK">The b currency pk.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public DataSet FetchAllImpAirManifest(int AirLinePK = 0, int JobPK = 0, int AOOPK = 0, int AODPK = 0, string txtHawbNr = "", string RefType = "", string RefNr = "", Int32 Export = 0, int CurrentPage = 0, int TotalPage = 0,
        int BCurrencyPK = 0, int loc = 0)
        {
            Int32 Last = default(Int32);
            Int32 Start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("SELECT *");
            sb.Append("  FROM (SELECT ROWNUM SL_NO, Q.*");
            sb.Append("     FROM ((SELECT QRY.JCIPK,");
            sb.Append("                        QRY.JCRNR,");
            sb.Append("                        QRY.JCDT,");
            sb.Append("                        QRY.HRNR,");
            sb.Append("                        QRY.HDATE,");
            sb.Append("                        QRY.HSURR,");
            sb.Append("                        QRY.POL,");
            sb.Append("                        QRY.POD,");
            sb.Append("                        QRY.VF_STATUS,");
            sb.Append("                        QRY.ATA_ETA,");
            sb.Append("                        QRY.MANIFEST,");
            sb.Append("                        QRY.CAN,");
            sb.Append("                        QRY.DISCHARGE,");
            sb.Append("                        QRY.INV,");
            sb.Append("                        QRY.CIPK,");
            sb.Append("                        QRY.INRNR,");
            sb.Append("                        (CASE");
            sb.Append("                          WHEN QRY.RECIEVED >= '0' THEN");
            sb.Append("                           'a'");
            sb.Append("                          ELSE");
            sb.Append("                           'r'");
            sb.Append("                        END) COLL,");
            sb.Append("                        QRY.CTPK,");
            sb.Append("                        QRY.COLNR,");
            sb.Append("                        '' CARGO_TYPE,");
            sb.Append("                         QRY.JCTYPE");
            sb.Append("                   FROM (SELECT DISTINCT JOB.JOB_CARD_AIR_IMP_PK JCIPK,");
            sb.Append("                                         JOB.JOBCARD_REF_NO JCRNR,");
            sb.Append("                                         JOB.JOBCARD_DATE JCDT,");
            sb.Append("                                         JOB.HAWB_REF_NO HRNR,");
            sb.Append("                                         TO_CHAR(JOB.HAWB_DATE, DATEFORMAT) HDATE,");
            sb.Append("                                         CASE");
            sb.Append("                                           WHEN JOB.HAWB_SURRDT IS NOT NULL THEN");
            sb.Append("                                            'a'");
            sb.Append("                                           ELSE");
            sb.Append("                                            'r'");
            sb.Append("                                         END HSURR,");
            sb.Append("                                         POL.PORT_ID POL,");
            sb.Append("                                         POD.PORT_ID POD,");
            sb.Append("                                         CASE");
            sb.Append("                                           WHEN JOB.ETA_DATE  IS NOT NULL THEN");
            sb.Append("                                            'Arrived'");
            sb.Append("                                           ELSE");
            sb.Append("                                            'Not Arrived'");
            sb.Append("                                         END VF_STATUS,");
            sb.Append("                                         CASE");
            sb.Append("                                           WHEN JOB.ETA_DATE  IS NOT NULL THEN");
            sb.Append("                                            JOB.ETA_DATE ");
            sb.Append("                                           ELSE");
            sb.Append("                                            Null");
            sb.Append("                                         END ATA_ETA,");
            sb.Append("                                        'r' MANIFEST,");
            sb.Append("                                         CASE");
            sb.Append("                                           WHEN CMT.BL_REF_NO IS NOT NULL THEN");
            sb.Append("                                            'a'");
            sb.Append("                                           ELSE");
            sb.Append("                                            'r'");
            sb.Append("                                         END CAN,");
            sb.Append("                                         TO_CHAR(DOMT.DELIVERY_ORDER_DATE,");
            sb.Append("                                                 DATEFORMAT) DISCHARGE,");
            sb.Append("                                         CASE");
            sb.Append("                                           WHEN INVTRN.JOB_CARD_FK IS NOT NULL THEN");
            sb.Append("                                            'a'");
            sb.Append("                                           ELSE");
            sb.Append("                                            'r'");
            sb.Append("                                         END INV,");
            sb.Append("                                         CON.CONSOL_INVOICE_PK CIPK,");
            sb.Append("                                         CON.INVOICE_REF_NO INRNR,");
            sb.Append("                                         NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
            sb.Append("                                               FROM COLLECTIONS_TRN_TBL CTRN");
            sb.Append("                                              WHERE CTRN.INVOICE_REF_NR LIKE");
            sb.Append("                                                    CON.INVOICE_REF_NO),");
            sb.Append("                                             0) RECIEVED,");
            sb.Append("                                         COL.COLLECTIONS_TBL_PK CTPK,");
            sb.Append("                                         COL.COLLECTIONS_REF_NO COLNR,");
            sb.Append("                                         JOB.JC_AUTO_MANUAL JCTYPE");
            sb.Append("                           FROM JOB_CARD_AIR_IMP_TBL   JOB,");
            sb.Append("                                CONSOL_INVOICE_TBL     CON,");
            sb.Append("                                CONSOL_INVOICE_TRN_TBL INVTRN,");
            sb.Append("                                COLLECTIONS_TBL        COL,");
            sb.Append("                                COLLECTIONS_TRN_TBL    COLTRN,");
            sb.Append("                                PORT_MST_TBL           POL,");
            sb.Append("                                PORT_MST_TBL           POD,");
            sb.Append("                                CAN_MST_TBL            CMT,");
            sb.Append("                                JOB_TRN_AIR_IMP_CONT JTRN,");
            sb.Append("                                DELIVERY_ORDER_MST_TBL DOMT,");
            sb.Append("                                CUSTOMER_MST_TBL       CONS,");
            sb.Append("                                CUSTOMER_MST_TBL       SHIP,");
            //sb.Append("                                USER_MST_TBL UMT,")
            if (RefType == "4")
            {
                if (!string.IsNullOrEmpty(RefNr))
                {
                    sb.Append("       (SELECT JCONT.JOB_CARD_AIR_IMP_FK");
                    sb.Append("          FROM JOB_TRN_AIR_IMP_CONT JCONT");
                    sb.Append("         WHERE JCONT.COMMODITY_MST_FK IN");
                    sb.Append("               (SELECT ROWTOCOL('SELECT C.COMMODITY_MST_PK FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_ID IN (''" + RefNr + "'') ')");
                    sb.Append("                  FROM DUAL)) JOB_CONT,");
                }
            }
            sb.Append("                                COMMODITY_GROUP_MST_TBL COMG");
            sb.Append("                          WHERE CON.CONSOL_INVOICE_PK =");
            sb.Append("                                INVTRN.CONSOL_INVOICE_FK");
            sb.Append("                            AND JOB.JOB_CARD_AIR_IMP_PK =");
            sb.Append("                                INVTRN.JOB_CARD_FK(+)");
            sb.Append("                            AND COL.COLLECTIONS_TBL_PK =");
            sb.Append("                                COLTRN.COLLECTIONS_TBL_FK");
            sb.Append("                            AND CON.INVOICE_REF_NO = COLTRN.INVOICE_REF_NR(+)");
            sb.Append("                            AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("                            AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("                            AND DOMT.JOB_CARD_MST_FK(+) =");
            sb.Append("                                JOB.JOB_CARD_AIR_IMP_PK");
            sb.Append("                              AND  JOB.JOB_CARD_AIR_IMP_PK=JTRN.JOB_CARD_AIR_IMP_FK");
            sb.Append("                             AND JOB.SHIPPER_CUST_MST_FK=SHIP.CUSTOMER_MST_PK");
            sb.Append("                             AND JOB.CONSIGNEE_CUST_MST_FK=CONS.CUSTOMER_MST_PK");
            sb.Append("                             AND JOB.COMMODITY_GROUP_FK=COMG.COMMODITY_GROUP_PK(+)");
            //sb.Append("                             AND JOB.CREATED_BY_FK=UMT.USER_MST_PK")
            //sb.Append("                             AND POD.location_mst_fk=" & loc)
            sb.Append("     AND POD.PORT_MST_PK IN (SELECT LWPT.PORT_MST_FK FROM LOCATION_WORKING_PORTS_TRN LWPT");
            sb.Append("     WHERE LWPT.ACTIVE = 1");
            sb.Append("     AND LWPT.LOCATION_MST_FK = " + loc + ")");

            if (Convert.ToInt32(RefType) > 0)
            {
                if (!string.IsNullOrEmpty(RefNr))
                {
                    if (RefType == "1")
                    {
                        sb.Append(" AND UPPER(SHIP.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (RefType == "2")
                    {
                        sb.Append(" AND UPPER(CONS.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (RefType == "3")
                    {
                        sb.Append(" AND UPPER(COMG.COMMODITY_GROUP_DESC) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
                    }
                    else
                    {
                        sb.Append("   AND JOB_CONT.JOB_CARD_AIR_IMP_FK(+) = JCAET.JOB_CARD_AIR_IMP_PK");
                    }
                }
            }

            if (AirLinePK != 0)
            {
                sb.Append("  and JOB.airline_mst_fk =  " + AirLinePK + " ");
            }

            if (JobPK != 0)
            {
                sb.Append("  AND JOB.JOB_CARD_AIR_IMP_PK = " + JobPK + "");
            }

            if (AOOPK != 0)
            {
                sb.Append("  AND POL.PORT_MST_PK = " + AOOPK + "");
            }

            if (AODPK != 0)
            {
                sb.Append("  AND POD.PORT_MST_PK = " + AODPK + "");
            }

            if (!string.IsNullOrEmpty(txtHawbNr.Trim()))
            {
                sb.Append(" AND UPPER(JOB.HAWB_REF_NO) LIKE '%" + txtHawbNr.Trim().ToUpper() + "%'");
            }

            sb.Append(" AND JOB.JOB_CARD_AIR_IMP_PK = CMT.JOB_CARD_FK) QRY) Q))");

            try
            {
                DataTable tbl = new DataTable();
                tbl = objWF.GetDataTable(sb.ToString());
                TotalRecords = (Int32)tbl.Rows.Count;
                TotalPage = TotalRecords / RecordsPerPage;
                if (TotalRecords % RecordsPerPage != 0)
                {
                    TotalPage += 1;
                }
                if (CurrentPage > TotalPage)
                    CurrentPage = 1;
                if (TotalRecords == 0)
                    CurrentPage = 0;
                Last = CurrentPage * RecordsPerPage;
                Start = (CurrentPage - 1) * RecordsPerPage + 1;
                if (Export == 0)
                {
                    sb.Append("  WHERE SL_NO  Between " + Start + " and " + Last + "");
                }
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch On Search for Import Manifest Air"
    }
}