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
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_Freight_Corrector_Notice : CommonFeatures
	{
        #region "List of Properties"
        /// <summary>
        /// The object wf
        /// </summary>
        WorkFlow objWF = new WorkFlow();

        /// <summary>
        /// The m_ protocol_ MST_ pk
        /// </summary>
        private Int64 M_Protocol_Mst_Pk;
        /// <summary>
        /// The m_ protocol_ name
        /// </summary>
        private string M_Protocol_NAME;

        /// <summary>
        /// The m_ protocol_ value
        /// </summary>
        private string M_Protocol_VALUE;
        /// <summary>
        /// Gets or sets the protocol_ MST_ pk.
        /// </summary>
        /// <value>
        /// The protocol_ MST_ pk.
        /// </value>
        public Int64 Protocol_Mst_Pk {
			get { return M_Protocol_Mst_Pk; }
			set { M_Protocol_Mst_Pk = value; }
		}

        /// <summary>
        /// Gets or sets the protocol_ identifier.
        /// </summary>
        /// <value>
        /// The protocol_ identifier.
        /// </value>
        public string Protocol_Id {
			get { return M_Protocol_NAME; }
			set { M_Protocol_NAME = value; }
		}

        /// <summary>
        /// Gets or sets the name of the protocol_.
        /// </summary>
        /// <value>
        /// The name of the protocol_.
        /// </value>
        public string Protocol_Name {
			get { return M_Protocol_VALUE; }
			set { M_Protocol_VALUE = value; }
		}

        /// <summary>
        /// Gets or sets the created_ by_ fk.
        /// </summary>
        /// <value>
        /// The created_ by_ fk.
        /// </value>
        public Int64 Created_By_Fk {
			get { return M_CREATED_BY_FK; }
			set { M_CREATED_BY_FK = value; }
		}

        /// <summary>
        /// Gets or sets the created_ dt.
        /// </summary>
        /// <value>
        /// The created_ dt.
        /// </value>
        public DateTime Created_Dt {
			get { return M_CREATED_DT; }
			set { M_CREATED_DT = value; }
		}

        /// <summary>
        /// Gets or sets the last_ modified_ by_ fk.
        /// </summary>
        /// <value>
        /// The last_ modified_ by_ fk.
        /// </value>
        public Int64 Last_Modified_By_FK {
			get { return M_LAST_MODIFIED_BY_FK; }
			set { M_LAST_MODIFIED_BY_FK = value; }
		}

        /// <summary>
        /// Gets or sets the last_ modified_ dt.
        /// </summary>
        /// <value>
        /// The last_ modified_ dt.
        /// </value>
        public DateTime Last_Modified_Dt {
			get { return M_LAST_MODIFIED_DT; }
			set { M_LAST_MODIFIED_DT = value; }
		}

        #endregion

        #region "Fech Freight Corrector Notice Listing"
        /// <summary>
        /// Feches the lsting.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="blpk">The BLPK.</param>
        /// <param name="bookingpk">The bookingpk.</param>
        /// <param name="comshd">The comshd.</param>
        /// <param name="pol_pk">The pol_pk.</param>
        /// <param name="pod_pk">The pod_pk.</param>
        /// <param name="status">The status.</param>
        /// <param name="fromdate">The fromdate.</param>
        /// <param name="todate">The todate.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="SupInvNr">The sup inv nr.</param>
        /// <param name="CrNoteNr">The cr note nr.</param>
        /// <param name="SearchFlg">The search FLG.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public object FechLsting(Int64 BizType = 0, int blpk = 0, int bookingpk = 0, int comshd = 0, string pol_pk = "", string pod_pk = "", Int64 status = 0, string fromdate = "", string todate = "", int CargoType = 0,
		string SupInvNr = "", string CrNoteNr = "", string SearchFlg = "C", Int32 CurrentPage = 0, Int32 TotalPage = 0)
		{

			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			int Location = Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
			Int32 last = 0;
			Int32 start = 0;
			Int32 TotalRecords = default(Int32);
			string strSQL = null;

			sb.Append(" SELECT ROWNUM SLNO, Q.* FROM ( ");
			sb.Append("SELECT * FROM (SELECT DISTINCT ");
			sb.Append("                HBL.HBL_REF_NO SERVICE_BL_NO,");
			sb.Append("                FREIGHT_CORRECTOR_HDR.VERSION,");
			sb.Append("                TO_DATE(HBL.HBL_DATE, 'dd/MM/yyyy') BL_DATE,");
			sb.Append("                BKG.BOOKING_REF_NO BOOKING_ID,");
			sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE, ");
			sb.Append("                DECODE(BKG.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
			sb.Append("                HBL.VESSEL_NAME || '-' || HBL.VOYAGE AS VSL,");
			sb.Append("                POL.PORT_ID POL,");
			sb.Append("                POD.PORT_ID POD,");
			//sb.Append("                '' INVOICE_NO,")
			//sb.Append("                '' CR_NOTE_ID,")

			sb.Append("(SELECT DISTINCT CIN.INVOICE_REF_NO");
			sb.Append("                 FROM CONSOL_INVOICE_TBL     CIN,");
			sb.Append("                 CONSOL_INVOICE_TRN_TBL CIT ");
			sb.Append("                 WHERE CIN.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
			sb.Append("                 AND CIT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
			//sb.Append("                 AND CIN.BUSINESS_TYPE(+) = " & BizType)
			sb.Append("                 AND CIN.INV_TYPE=1) INVOICE_NO,");

			sb.Append("(SELECT DISTINCT CT.CREDIT_NOTE_REF_NR");
			sb.Append("                 FROM CREDIT_NOTE_TBL     CT,");
			sb.Append("                      CREDIT_NOTE_TRN_TBL     CNT,");
			sb.Append("                      CONSOL_INVOICE_TBL     CIT,");
			sb.Append("                      CONSOL_INVOICE_TRN_TBL CINT ");
			sb.Append("                 WHERE CT.CRN_TBL_PK = CNT.CRN_TBL_FK");
			sb.Append("                 AND CINT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
			sb.Append("                 AND CIT.CONSOL_INVOICE_PK = CNT.CONSOL_INVOICE_TRN_FK");
			sb.Append("                 AND JOB.JOB_CARD_TRN_PK = CINT.JOB_CARD_FK");
			sb.Append("                 AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK) CR_NOTE_ID,");

			sb.Append("                CASE");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 1 THEN");
			sb.Append("                   'Requested'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 2 THEN");
			sb.Append("                   'Approved'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 3 THEN");
			sb.Append("                   'Rejected'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 0 THEN");
			sb.Append("                   'WIP'");
			sb.Append("                END STATUS, ");
			sb.Append("                HBL.HBL_EXP_TBL_PK BOOKING_BL_PK,");
			sb.Append("                BKG.BOOKING_MST_PK BOOKING_TRN_PK,");
			sb.Append("                FREIGHT_CORRECTOR_HDR.FREIGHT_CORRECTOR_HDR_PK, 2 BIZ_TYPE_VALUE ");
			sb.Append("  FROM BOOKING_MST_TBL       BKG,");
			sb.Append("       JOB_CARD_TRN  JOB,");
			sb.Append("       HBL_EXP_TBL           HBL,");
			sb.Append("       PORT_MST_TBL          POL,");
			sb.Append("       PORT_MST_TBL          POD,");
			sb.Append("       FREIGHT_CORRECTOR_HDR,");
			sb.Append("       LOCATION_WORKING_PORTS_TRN LWPT");
			sb.Append(" WHERE BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
			sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
			sb.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
			sb.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
			sb.Append("   AND HBL.HBL_EXP_TBL_PK = FREIGHT_CORRECTOR_HDR.HBL_FK");
			sb.Append("   AND LWPT.PORT_MST_FK = POL.PORT_MST_PK");

			///' AIR
			if (BizType == 1) {
				sb.Append(" And 1 = 2 ");
			}
			if (CargoType > 0) {
				sb.Append("   AND BKG.CARGO_TYPE = " + CargoType);
			}
			if (status != 0) {
				sb.Append("   AND FREIGHT_CORRECTOR_HDR.STATUS = " + status);
			}
			if (!string.IsNullOrEmpty(fromdate)) {
				sb.Append("  AND to_date(HBL.HBL_DATE,DATEFORMAT) BETWEEN TO_DATE('" + fromdate + "',DATEFORMAT) and to_date('" + fromdate + "',DATEFORMAT) ");
			}
			if (comshd != 0) {
				sb.Append("   AND HBL.VOYAGE_TRN_FK = " + comshd);
			}
			if (blpk != 0) {
				sb.Append("   AND HBL.HBL_EXP_TBL_PK = " + blpk);
			}
			if (bookingpk != 0) {
				sb.Append("   AND BKG.BOOKING_MST_PK = " + bookingpk);
			}
			if (!string.IsNullOrEmpty(pol_pk)) {
				sb.Append(" AND POL.PORT_MST_PK  IN ('" + pol_pk.ToUpper().Replace(",", "','") + "')");
			}
			if (!string.IsNullOrEmpty(pod_pk)) {
				sb.Append(" AND POD.PORT_MST_PK  IN ('" + pod_pk.ToUpper().Replace(",", "','") + "')");
			}

			sb.Append("   AND LWPT.LOCATION_MST_FK IN");
			sb.Append("       (SELECT L.LOCATION_MST_PK");
			sb.Append("          FROM LOCATION_MST_TBL L");
			sb.Append("         START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
			sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");
			sb.Append(" UNION ");
			sb.Append("SELECT DISTINCT ");
			sb.Append("                HBL.HBL_REF_NO SERVICE_BL_NO,");
			sb.Append("                FREIGHT_CORRECTOR_HDR.VERSION,");
			sb.Append("                TO_DATE(HBL.HBL_DATE, 'dd/MM/yyyy') BL_DATE,");
			sb.Append("                BKG.BOOKING_REF_NO BOOKING_ID,");
			sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE,");
			sb.Append("                DECODE(BKG.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
			sb.Append("                HBL.VESSEL_NAME || '-' || HBL.VOYAGE AS VSL,");
			sb.Append("                POL.PORT_ID POL,");
			sb.Append("                POD.PORT_ID POD,");
			sb.Append("(SELECT DISTINCT CIN.INVOICE_REF_NO");
			sb.Append("                 FROM CONSOL_INVOICE_TBL     CIN,");
			sb.Append("                 CONSOL_INVOICE_TRN_TBL CIT ");
			sb.Append("                 WHERE CIN.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
			sb.Append("                 AND CIT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
			//sb.Append("                 AND CIN.BUSINESS_TYPE(+) = " & BizType)
			sb.Append("                 AND CIN.INV_TYPE=0) INVOICE_NO,");

			sb.Append("(SELECT DISTINCT CT.CREDIT_NOTE_REF_NR");
			sb.Append("                 FROM CREDIT_NOTE_TBL     CT,");
			sb.Append("                      CREDIT_NOTE_TRN_TBL     CNT,");
			sb.Append("                      CONSOL_INVOICE_TBL     CIT,");
			sb.Append("                      CONSOL_INVOICE_TRN_TBL CINT ");
			sb.Append("                 WHERE CT.CRN_TBL_PK = CNT.CRN_TBL_FK");
			sb.Append("                 AND CINT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
			sb.Append("                 AND CIT.CONSOL_INVOICE_PK = CNT.CONSOL_INVOICE_TRN_FK");
			sb.Append("                 AND JOB.JOB_CARD_TRN_PK = CINT.JOB_CARD_FK");
			sb.Append("                 AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK) CR_NOTE_ID,");
			sb.Append("                CASE");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 1 THEN");
			sb.Append("                   'Requested'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 2 THEN");
			sb.Append("                   'Approved'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 3 THEN");
			sb.Append("                   'Rejected'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 0 THEN");
			sb.Append("                   'WIP'");
			sb.Append("                END STATUS, ");
			sb.Append("                HBL.HBL_EXP_TBL_PK BOOKING_BL_PK,");
			sb.Append("                BKG.BOOKING_MST_PK BOOKING_TRN_PK,");
			sb.Append("                FREIGHT_CORRECTOR_HDR.FREIGHT_CORRECTOR_HDR_PK, 2 BIZ_TYPE_VALUE ");
			sb.Append("  FROM BOOKING_MST_TBL       BKG,");
			sb.Append("       JOB_CARD_TRN  JOB,");
			sb.Append("       HBL_EXP_TBL           HBL,");
			sb.Append("       PORT_MST_TBL          POL,");
			sb.Append("       PORT_MST_TBL          POD,");
			sb.Append("       FREIGHT_CORRECTOR_HDR,");
			sb.Append("       LOCATION_WORKING_PORTS_TRN LWPT");
			sb.Append(" WHERE BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
			sb.Append("   AND JOB.JOB_CARD_TRN_PK = HBL.NEW_JOB_CARD_SEA_EXP_FK");
			sb.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
			sb.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
			sb.Append("   AND HBL.HBL_EXP_TBL_PK = FREIGHT_CORRECTOR_HDR.HBL_FK");

			///' AIR
			if (BizType == 1) {
				sb.Append(" And 1 = 2 ");
			}
			if (CargoType > 0) {
				sb.Append("   AND BKG.CARGO_TYPE = " + CargoType);
			}
			if (status != 0) {
				sb.Append("   AND FREIGHT_CORRECTOR_HDR.STATUS = " + status);
			}
			if (!string.IsNullOrEmpty(fromdate)) {
				sb.Append("  AND to_date(HBL.HBL_DATE,DATEFORMAT) BETWEEN TO_DATE('" + fromdate + "',DATEFORMAT) and to_date('" + fromdate + "',DATEFORMAT) ");
			}
			if (comshd != 0) {
				sb.Append("   AND HBL.VOYAGE_TRN_FK = " + comshd);
			}
			if (blpk != 0) {
				sb.Append("   AND HBL.HBL_EXP_TBL_PK = " + blpk);
			}
			if (bookingpk != 0) {
				sb.Append("   AND BKG.BOOKING_MST_PK = " + bookingpk);
			}
			if (!string.IsNullOrEmpty(pol_pk)) {
				sb.Append(" AND POL.PORT_MST_PK  IN ('" + pol_pk.ToUpper().Replace(",", "','") + "')");
			}
			if (!string.IsNullOrEmpty(pod_pk)) {
				sb.Append(" AND POD.PORT_MST_PK  IN ('" + pod_pk.ToUpper().Replace(",", "','") + "')");
			}

			sb.Append("   AND LWPT.PORT_MST_FK = POL.PORT_MST_PK");
			sb.Append("   AND LWPT.LOCATION_MST_FK IN");
			sb.Append("       (SELECT L.LOCATION_MST_PK");
			sb.Append("          FROM LOCATION_MST_TBL L");
			sb.Append("         START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
			sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ");

			sb.Append(" UNION SELECT DISTINCT ");
			sb.Append("                HBL.HAWB_REF_NO SERVICE_BL_NO,");
			sb.Append("                FREIGHT_CORRECTOR_HDR.VERSION,");
			sb.Append("                TO_DATE(HBL.HAWB_DATE, 'dd/MM/yyyy') BL_DATE,");
			sb.Append("                BKG.BOOKING_REF_NO BOOKING_ID,");
			sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
			//sb.Append("                '' CARGO_TYPE,")
			sb.Append("                DECODE(BKG.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGO_TYPE,");
			sb.Append("                AM.AIRLINE_NAME || '-' || HBL.FLIGHT_NO AS VSL,");
			sb.Append("                POL.PORT_ID POL,");
			sb.Append("                POD.PORT_ID POD,");
			sb.Append("(SELECT DISTINCT CIN.INVOICE_REF_NO");
			sb.Append("                 FROM CONSOL_INVOICE_TBL     CIN,");
			sb.Append("                 CONSOL_INVOICE_TRN_TBL CIT ");
			sb.Append("                 WHERE CIN.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
			sb.Append("                 AND CIT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
			//sb.Append("                 AND CIN.BUSINESS_TYPE(+) = " & BizType)
			sb.Append("                 AND CIN.INV_TYPE=0) INVOICE_NO,");

			sb.Append("(SELECT DISTINCT CT.CREDIT_NOTE_REF_NR");
			sb.Append("                 FROM CREDIT_NOTE_TBL     CT,");
			sb.Append("                      CREDIT_NOTE_TRN_TBL     CNT,");
			sb.Append("                      CONSOL_INVOICE_TBL     CIT,");
			sb.Append("                      CONSOL_INVOICE_TRN_TBL CINT ");
			sb.Append("                 WHERE CT.CRN_TBL_PK = CNT.CRN_TBL_FK");
			sb.Append("                 AND CINT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
			sb.Append("                 AND CIT.CONSOL_INVOICE_PK = CNT.CONSOL_INVOICE_TRN_FK");
			sb.Append("                 AND JOB.JOB_CARD_TRN_PK = CINT.JOB_CARD_FK");
			sb.Append("                 AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK) CR_NOTE_ID,");
			sb.Append("                CASE");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 1 THEN");
			sb.Append("                   'Requested'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 2 THEN");
			sb.Append("                   'Approved'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 3 THEN");
			sb.Append("                   'Rejected'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 0 THEN");
			sb.Append("                   'WIP'");
			sb.Append("                END STATUS,");
			sb.Append("                HBL.HAWB_EXP_TBL_PK BOOKING_BL_PK,");
			sb.Append("                BKG.BOOKING_MST_PK BOOKING_TRN_PK,");
			sb.Append("                FREIGHT_CORRECTOR_HDR.FREIGHT_CORRECTOR_HDR_PK, 1 BIZ_TYPE_VALUE ");
			sb.Append("  FROM BOOKING_MST_TBL            BKG,");
			sb.Append("       AIRLINE_MST_TBL            AM,");
			sb.Append("       JOB_CARD_TRN       JOB,");
			sb.Append("       HAWB_EXP_TBL               HBL,");
			sb.Append("       PORT_MST_TBL               POL,");
			sb.Append("       PORT_MST_TBL               POD,");
			sb.Append("       FREIGHT_CORRECTOR_HDR,");
			sb.Append("       LOCATION_WORKING_PORTS_TRN LWPT");
			sb.Append(" WHERE BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
			sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK");
			sb.Append("   AND AM.AIRLINE_MST_PK = BKG.CARRIER_MST_FK");
			sb.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
			sb.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
			sb.Append("   AND HBL.HAWB_EXP_TBL_PK = FREIGHT_CORRECTOR_HDR.HAWB_FK");

			///' SEA
			if (BizType == 2) {
				sb.Append(" And 1 = 2 ");
			}
			if (status != 0) {
				sb.Append("   AND FREIGHT_CORRECTOR_HDR.STATUS = " + status);
			}
			if (!string.IsNullOrEmpty(fromdate)) {
				sb.Append("  AND to_date(HBL.HAWB_DATE,DATEFORMAT) BETWEEN TO_DATE('" + fromdate + "',DATEFORMAT) and to_date('" + fromdate + "',DATEFORMAT) ");
			}
			if (comshd != 0) {
				sb.Append("   AND AM.AIRLINE_MST_PK = " + comshd);
			}
			if (blpk != 0) {
				sb.Append("   AND HBL.HAWB_EXP_TBL_PK = " + blpk);
			}
			if (bookingpk != 0) {
				sb.Append("   AND BKG.BOOKING_MST_PK = " + bookingpk);
			}
			if (!string.IsNullOrEmpty(pol_pk)) {
				sb.Append(" AND POL.PORT_MST_PK  IN ('" + pol_pk.ToUpper().Replace(",", "','") + "')");
			}
			if (!string.IsNullOrEmpty(pod_pk)) {
				sb.Append(" AND POD.PORT_MST_PK  IN ('" + pod_pk.ToUpper().Replace(",", "','") + "')");
			}

			sb.Append("   AND LWPT.PORT_MST_FK = POL.PORT_MST_PK");
			sb.Append("   AND LWPT.LOCATION_MST_FK IN");
			sb.Append("       (SELECT L.LOCATION_MST_PK");
			sb.Append("          FROM LOCATION_MST_TBL L");
			sb.Append("         START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
			sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");
			sb.Append(" UNION SELECT DISTINCT ");
			sb.Append("                HBL.HAWB_REF_NO SERVICE_BL_NO,");
			sb.Append("                FREIGHT_CORRECTOR_HDR.VERSION,");
			sb.Append("                TO_DATE(HBL.HAWB_DATE, 'dd/MM/yyyy') BL_DATE,");
			sb.Append("                BKG.BOOKING_REF_NO BOOKING_ID,");
			sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
			sb.Append("                '' CARGO_TYPE,");
			sb.Append("                AM.AIRLINE_NAME || '-' || HBL.FLIGHT_NO AS VSL,");
			sb.Append("                POL.PORT_ID POL,");
			sb.Append("                POD.PORT_ID POD,");
			sb.Append("(SELECT DISTINCT CIN.INVOICE_REF_NO");
			sb.Append("                 FROM CONSOL_INVOICE_TBL     CIN,");
			sb.Append("                 CONSOL_INVOICE_TRN_TBL CIT ");
			sb.Append("                 WHERE CIN.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
			sb.Append("                 AND CIT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
			//sb.Append("                 AND CIN.BUSINESS_TYPE(+) = " & BizType)
			sb.Append("                 AND CIN.INV_TYPE=0) INVOICE_NO,");

			sb.Append("(SELECT DISTINCT CT.CREDIT_NOTE_REF_NR");
			sb.Append("                 FROM CREDIT_NOTE_TBL     CT,");
			sb.Append("                      CREDIT_NOTE_TRN_TBL     CNT,");
			sb.Append("                      CONSOL_INVOICE_TBL     CIT,");
			sb.Append("                      CONSOL_INVOICE_TRN_TBL CINT ");
			sb.Append("                 WHERE CT.CRN_TBL_PK = CNT.CRN_TBL_FK");
			sb.Append("                 AND CINT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
			sb.Append("                 AND CIT.CONSOL_INVOICE_PK = CNT.CONSOL_INVOICE_TRN_FK");
			sb.Append("                 AND JOB.JOB_CARD_TRN_PK = CINT.JOB_CARD_FK");
			sb.Append("                 AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK) CR_NOTE_ID,");
			sb.Append("                CASE");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 1 THEN");
			sb.Append("                   'Requested'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 2 THEN");
			sb.Append("                   'Approved'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 3 THEN");
			sb.Append("                   'Rejected'");
			sb.Append("                  WHEN FREIGHT_CORRECTOR_HDR.STATUS = 0 THEN");
			sb.Append("                   'WIP'");
			sb.Append("                END STATUS,");
			sb.Append("                HBL.HAWB_EXP_TBL_PK BOOKING_BL_PK,");
			sb.Append("                BKG.BOOKING_MST_PK BOOKING_TRN_PK,");
			sb.Append("                FREIGHT_CORRECTOR_HDR.FREIGHT_CORRECTOR_HDR_PK, 1 BIZ_TYPE_VALUE ");
			sb.Append("  FROM BOOKING_MST_TBL            BKG,");
			sb.Append("       AIRLINE_MST_TBL            AM,");
			sb.Append("       JOB_CARD_TRN       JOB,");
			sb.Append("       HAWB_EXP_TBL               HBL,");
			sb.Append("       PORT_MST_TBL               POL,");
			sb.Append("       PORT_MST_TBL               POD,");
			sb.Append("       FREIGHT_CORRECTOR_HDR,");
			sb.Append("       LOCATION_WORKING_PORTS_TRN LWPT ");
			sb.Append(" WHERE BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
			sb.Append("   AND HBL.NEW_JOB_CARD_AIR_EXP_FK=JOB.JOB_CARD_TRN_PK");
			sb.Append("   AND AM.AIRLINE_MST_PK = BKG.CARRIER_MST_FK");
			sb.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
			sb.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
			sb.Append("   AND HBL.HAWB_EXP_TBL_PK = FREIGHT_CORRECTOR_HDR.HAWB_FK");
			sb.Append("   AND LWPT.PORT_MST_FK = POL.PORT_MST_PK");

			///' SEA
			if (BizType == 2) {
				sb.Append(" And 1 = 2 ");
			}
			if (status != 0) {
				sb.Append("   AND FREIGHT_CORRECTOR_HDR.STATUS = " + status);
			}
			if (!string.IsNullOrEmpty(fromdate)) {
				sb.Append("  AND to_date(HBL.HAWB_DATE,DATEFORMAT) BETWEEN TO_DATE('" + fromdate + "',DATEFORMAT) and to_date('" + fromdate + "',DATEFORMAT) ");
			}
			if (comshd != 0) {
				sb.Append("   AND AM.AIRLINE_MST_PK = " + comshd);
			}
			if (blpk != 0) {
				sb.Append("   AND HBL.HAWB_EXP_TBL_PK = " + blpk);
			}
			if (bookingpk != 0) {
				sb.Append("   AND BKG.BOOKING_MST_PK = " + bookingpk);
			}
			if (!string.IsNullOrEmpty(pol_pk)) {
				sb.Append(" AND POL.PORT_MST_PK  IN ('" + pol_pk.ToUpper().Replace(",", "','") + "')");
			}
			if (!string.IsNullOrEmpty(pod_pk)) {
				sb.Append(" AND POD.PORT_MST_PK  IN ('" + pod_pk.ToUpper().Replace(",", "','") + "')");
			}
			sb.Append("   AND LWPT.LOCATION_MST_FK IN");
			sb.Append("       (SELECT L.LOCATION_MST_PK");
			sb.Append("          FROM LOCATION_MST_TBL L");
			sb.Append("         START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
			sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)) WHERE 1 = 1 ");

			if (!string.IsNullOrEmpty(SupInvNr)) {
				sb.Append(" AND UPPER(INVOICE_NO) LIKE '" + (SearchFlg == "C" ? "%" + SupInvNr.ToUpper().Trim() + "%" : SupInvNr.ToUpper().Trim() + "%") + "'");
			}

			if (!string.IsNullOrEmpty(CrNoteNr)) {
				sb.Append(" AND UPPER(CR_NOTE_ID) LIKE '" + (SearchFlg == "C" ? "%" + CrNoteNr.ToUpper().Trim() + "%" : CrNoteNr.ToUpper().Trim() + "%") + "'");
			}

			sb.Append(" ORDER BY BL_DATE DESC, SERVICE_BL_NO DESC ) Q  ");

			strSQL = " SELECT COUNT(*) FROM (";
			strSQL += sb.ToString() + ")QRY";

			TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;

			strSQL = " SELECT * FROM (SELECT ROWNUM SLNO, Q1.* FROM(";
			strSQL += sb.ToString();
			strSQL += " ) Q1 ) WHERE SLNO Between " + start + " and " + last;

			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Cargo Details"
        /// <summary>
        /// Gets the bl cargo details.
        /// </summary>
        /// <param name="BLPK">The BLPK.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet getBLCargoDetails(long BLPK, int CargoType, int BizType = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			//'For SEA
			if (BizType == 2) {
				if (CargoType == 1) {
					sb.Append("SELECT ROWNUM SLNO,CTY.CONTAINER_TYPE_MST_PK,");
					sb.Append("       CTY.CONTAINER_TYPE_MST_ID CONTAINER_TYPE,");
					sb.Append("       JCT.CONTAINER_NUMBER CONTAINER_NR,");
					sb.Append("       JCT.SEAL_NUMBER SEAL_NR,");
					sb.Append("       JCT.VOLUME_IN_CBM VOLUME,");
					sb.Append("       JCT.GROSS_WEIGHT GROSS_WT,");
					sb.Append("       JCT.NET_WEIGHT NET_WT,");
					sb.Append("       PCK.PACK_TYPE_ID PACKTYPE,");
					sb.Append("       JCT.PACK_COUNT QUANTITY,");
					sb.Append("       '' COMMODITY,");
					sb.Append("       HBL.HBL_EXP_TBL_PK HBL_FK,");
					sb.Append("       JCT.JOB_TRN_CONT_PK,");
					sb.Append("       JCT.COMMODITY_MST_FK,");
					sb.Append("       JCT.COMMODITY_MST_FKS");
					sb.Append("  FROM JOB_CARD_TRN   JOB,");
					sb.Append("       JOB_TRN_CONT   JCT,");
					sb.Append("       BOOKING_MST_TBL        BKG,");
					sb.Append("       HBL_EXP_TBL            HBL,");
					sb.Append("       PACK_TYPE_MST_TBL      PCK,");
					sb.Append("       CONTAINER_TYPE_MST_TBL CTY");
					sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JCT.JOB_CARD_TRN_FK");
					sb.Append("   AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					sb.Append("   AND HBL.HBL_EXP_TBL_PK = JOB.HBL_HAWB_FK");
					sb.Append("   AND PCK.PACK_TYPE_MST_PK(+) = JCT.PACK_TYPE_MST_FK");
					sb.Append("   AND CTY.CONTAINER_TYPE_MST_PK = JCT.CONTAINER_TYPE_MST_FK");
					sb.Append("  AND HBL.HBL_EXP_TBL_PK = " + BLPK + "");
				///LCL
				} else if (CargoType == 2) {
					sb.Append(" SELECT ROWNUM SLNO,CTY.CONTAINER_TYPE_MST_PK,");
					sb.Append("       CTY.CONTAINER_TYPE_MST_ID CONTAINER_TYPE,");
					sb.Append("       JCT.CONTAINER_NUMBER CONTAINER_NR,");
					sb.Append("       JCT.SEAL_NUMBER SEAL_NR,");
					sb.Append("       JCT.VOLUME_IN_CBM VOLUME,");
					sb.Append("       JCT.GROSS_WEIGHT GROSS_WT,");
					sb.Append("       JCT.CHARGEABLE_WEIGHT CHRG_WT,");
					sb.Append("       PCK.PACK_TYPE_ID PACKTYPE,");
					sb.Append("       JCT.PACK_COUNT QUANTITY,");
					sb.Append("       '' COMMODITY,");
					sb.Append("       HBL.HBL_EXP_TBL_PK HBL_FK,");
					sb.Append("       JCT.JOB_TRN_CONT_PK,");
					sb.Append("       JCT.COMMODITY_MST_FK,");
					sb.Append("       JCT.COMMODITY_MST_FKS");
					sb.Append("  FROM JOB_CARD_TRN   JOB,");
					sb.Append("       JOB_TRN_CONT   JCT,");
					sb.Append("       BOOKING_MST_TBL        BKG,");
					sb.Append("       HBL_EXP_TBL            HBL,");
					sb.Append("       PACK_TYPE_MST_TBL      PCK,");
					sb.Append("       CONTAINER_TYPE_MST_TBL CTY");
					sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JCT.JOB_CARD_TRN_FK");
					sb.Append("   AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					sb.Append("   AND HBL.HBL_EXP_TBL_PK = JOB.HBL_HAWB_FK");
					sb.Append("   AND PCK.PACK_TYPE_MST_PK = JCT.PACK_TYPE_MST_FK");
					sb.Append("   AND CTY.CONTAINER_TYPE_MST_PK(+) = JCT.CONTAINER_TYPE_MST_FK");
					sb.Append("  AND HBL.HBL_EXP_TBL_PK = " + BLPK + "");
				} else if (CargoType == 4) {
					sb.Append("SELECT ROWNUM SLNO,");
					sb.Append("       COMM.COMMODITY_MST_PK COMMODITY_GROUP_FK,");
					sb.Append("       CGM.COMMODITY_GROUP_CODE COMMODITY_GRP_NAME,");
					sb.Append("       JCT.COMMODITY_MST_FK,");
					sb.Append("       COMM.COMMODITY_NAME,");
					sb.Append("       PCK.PACK_TYPE_ID PACKTYPE,");
					sb.Append("       DM.DIMENTION_ID BASIS,");
					sb.Append("       JCT.PACK_COUNT QUANTITY,");
					sb.Append("       JCT.CHARGEABLE_WEIGHT GROSS_WT,");
					sb.Append("       JCT.VOLUME_IN_CBM VOLUME,");
					sb.Append("       HBL.HBL_EXP_TBL_PK HBL_FK,");
					sb.Append("       JCT.JOB_TRN_CONT_PK");
					sb.Append("  FROM JOB_CARD_TRN    JOB,");
					sb.Append("       JOB_TRN_CONT    JCT,");
					sb.Append("       BOOKING_MST_TBL         BKG,");
					sb.Append("       HBL_EXP_TBL             HBL,");
					sb.Append("       PACK_TYPE_MST_TBL       PCK,");
					sb.Append("       COMMODITY_MST_TBL       COMM,");
					sb.Append("       COMMODITY_GROUP_MST_TBL CGM,");
					sb.Append("       DIMENTION_UNIT_MST_TBL  DM");
					sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JCT.JOB_CARD_TRN_FK");
					sb.Append("   AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					sb.Append("   AND HBL.HBL_EXP_TBL_PK = JOB.HBL_HAWB_FK");
					sb.Append("   AND PCK.PACK_TYPE_MST_PK = JCT.PACK_TYPE_MST_FK");
					sb.Append("   AND JCT.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK");
					sb.Append("   AND COMM.COMMODITY_GROUP_FK = CGM.COMMODITY_GROUP_PK");
					sb.Append("   AND DM.DIMENTION_UNIT_MST_PK = JCT.BASIS_FK");
					sb.Append("   AND HBL.HBL_EXP_TBL_PK = " + BLPK);
				}
			///'For AIR
			} else {
				sb.Append("SELECT ROWNUM  SLNO,");
				sb.Append("       JOB_TRN_CONT_PK,");
				sb.Append("       JOB_TRN_CONT.PALETTE_SIZE,");
				sb.Append("       JOB_TRN_CONT.AIRFREIGHT_SLABS_TBL_FK SLAB_FK,");
				sb.Append("       AIRFREIGHT.BREAKPOINT_ID,");
				sb.Append("       JOB_TRN_CONT.ULD_NUMBER,");
				sb.Append("       VOLUME_IN_CBM,");
				sb.Append("       GROSS_WEIGHT,");
				sb.Append("       CHARGEABLE_WEIGHT,");
				sb.Append("       PACK_TYPE_MST_FK,");
				sb.Append("       PACK_COUNT,");
				sb.Append("       COMMODITY_MST_FK,");
				sb.Append("       COMM.COMMODITY_NAME                  COMMODITY,");
				sb.Append("       HAWB.HAWB_EXP_TBL_PK                 HAWB_FK");
				sb.Append("  FROM JOB_TRN_CONT JOB_TRN_CONT,");
				sb.Append("       PACK_TYPE_MST_TBL    PACK,");
				sb.Append("       AIRFREIGHT_SLABS_TBL AIRFREIGHT,");
				sb.Append("       COMMODITY_MST_TBL    COMM,");
				sb.Append("       JOB_CARD_TRN JOB_EXP,");
				sb.Append("       HAWB_EXP_TBL         HAWB");
				sb.Append(" WHERE JOB_TRN_CONT.PACK_TYPE_MST_FK = PACK.PACK_TYPE_MST_PK(+)");
				sb.Append("   AND JOB_TRN_CONT.AIRFREIGHT_SLABS_TBL_FK =");
				sb.Append("       AIRFREIGHT.AIRFREIGHT_SLABS_TBL_PK(+)");
				sb.Append("   AND JOB_TRN_CONT.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK(+)");
				sb.Append("   AND JOB_TRN_CONT.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
				sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
				sb.Append("   AND HAWB.HAWB_EXP_TBL_PK = " + BLPK);
			}

			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "BLFreight Details.."
        /// <summary>
        /// Bls the freight details.
        /// </summary>
        /// <param name="BLPK">The BLPK.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="FREIGHT_CORRECTOR_HDR_PK">The freigh t_ correcto r_ hd r_ pk.</param>
        /// <param name="STATUS">The status.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet BLFreightDetails(long BLPK, int CargoType, Int16 flag = 0, string FREIGHT_CORRECTOR_HDR_PK = "", string STATUS = "", int BizType = 0)
		{
			string strSql = null;
			Int16 i = default(Int16);
			Int16 j = default(Int16);
			DataRow rowCnt = null;
			long cntPk = 0;
			long cntCount = 0;
			System.Data.OleDb.OleDbDataAdapter objAdp = new System.Data.OleDb.OleDbDataAdapter();
			DataTable dttable = new DataTable();
			DataSet dsData = new DataSet();
			WorkFlow objWF = new WorkFlow();
			DataSet ds = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
			//'For SEA
			if (BizType == 2) {
				if (string.IsNullOrEmpty(FREIGHT_CORRECTOR_HDR_PK)) {
					sb.Append("SELECT JOB_TRN_FD_PK,");
					sb.Append("       JOB_CARD_TRN_PK,");
					sb.Append("       CONTAINER_TYPE_MST_PK,");
					sb.Append("       CONTAINER_TYPE_MST_ID,");
					sb.Append("       FREIGHT_ELEMENT_MST_PK,");
					sb.Append("       FREIGHT_ELEMENT_ID,");
					sb.Append("       FREIGHT_ELEMENT_NAME,");
					sb.Append("       PMT_TYPE,");
					sb.Append("       LOCATION_MST_PK,");
					sb.Append("       LOCATION_ID,");
					sb.Append("       CUSTOMER_MST_PK,");
					sb.Append("       FRT_PAYER,");
					sb.Append("       RATEPERBASIS,");
					sb.Append("       FREIGHT_AMT,");
					sb.Append("       CURRENCY_MST_PK,");
					sb.Append("       CURRENCY_ID,");
					sb.Append("       EXCHANGE_RATE,");
					sb.Append("       TOTAL,");
					sb.Append("       QTY,");
					sb.Append("       WEIGHT,");
					sb.Append("       VOLUME,");
					sb.Append("       BASIS,CREDIT,SURCHARGE");
					sb.Append("  FROM (SELECT DISTINCT JF.JOB_TRN_FD_PK,");
					sb.Append("                        JC.JOB_CARD_TRN_PK,");
					sb.Append("                        CASE");
					sb.Append("                          WHEN BK.CARGO_TYPE = 4 THEN");
					sb.Append("                           CMT.COMMODITY_MST_PK");
					sb.Append("                          ELSE");
					sb.Append("                           CTY.CONTAINER_TYPE_MST_PK");
					sb.Append("                        END CONTAINER_TYPE_MST_PK,");
					sb.Append("                        CASE");
					sb.Append("                          WHEN BK.CARGO_TYPE = 4 THEN");
					sb.Append("                           CMT.COMMODITY_NAME");
					sb.Append("                          ELSE");
					sb.Append("                           CTY.CONTAINER_TYPE_MST_ID");
					sb.Append("                        END CONTAINER_TYPE_MST_ID,");
					sb.Append("                        FM.FREIGHT_ELEMENT_MST_PK,");
					sb.Append("                        FM.FREIGHT_ELEMENT_ID,");
					sb.Append("                        FM.FREIGHT_ELEMENT_NAME,");
					sb.Append("                        DECODE(JF.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') PMT_TYPE,");
					sb.Append("                        LM.LOCATION_MST_PK,");
					sb.Append("                        LM.LOCATION_ID,");
					sb.Append("                        CM.CUSTOMER_MST_PK,");
					sb.Append("                        CM.CUSTOMER_NAME FRT_PAYER,");
					sb.Append("                        NVL(JF.RATEPERBASIS, 0) RATEPERBASIS,");
					sb.Append("                        JF.FREIGHT_AMT,");
					sb.Append("                        CY.CURRENCY_MST_PK,");
					sb.Append("                        CY.CURRENCY_ID,");
					sb.Append("                        JF.EXCHANGE_RATE,");
					sb.Append("                        (JF.FREIGHT_AMT * JF.EXCHANGE_RATE) TOTAL,");
					if (CargoType == 4) {
						sb.Append("                        JT.PACK_COUNT QTY,");
						sb.Append("                        JT.CHARGEABLE_WEIGHT WEIGHT,");
						sb.Append("                        JT.VOLUME_IN_CBM VOLUME,");
						sb.Append("                        UOM.DIMENTION_ID BASIS,FM.CREDIT,");
					} else {
						if (CargoType == 2) {
							sb.Append("                        JF.QUANTITY QTY,");
						} else {
							sb.Append("(SELECT COUNT(*)");
							sb.Append("                   FROM JOB_TRN_CONT JT");
							sb.Append("                  WHERE JT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
							sb.Append("                    AND JT.CONTAINER_TYPE_MST_FK = CTY.CONTAINER_TYPE_MST_PK) QTY,");
						}
						sb.Append("                        0 WEIGHT,");
						sb.Append("                        0 VOLUME,");
						sb.Append("                        UOM.DIMENTION_ID BASIS,FM.CREDIT,");
					}
					sb.Append("                        FM.PREFERENCE");
					sb.Append(" , CASE ");
					sb.Append(" WHEN FM.CHARGE_BASIS = 1 AND");
					sb.Append("   FM.FREIGHT_ELEMENT_ID <> 'BOF' THEN");
					sb.Append("   (GET_BASIS_VALUE(BK.PORT_MST_POL_FK,");
					sb.Append("   BK.PORT_MST_POD_FK,");
					sb.Append("   FM.FREIGHT_ELEMENT_MST_PK) ||");
					sb.Append("   '% of' ||");
					sb.Append("   CONCADINATE_FUN_FREIGHTELEMENT(FM.FREIGHT_ELEMENT_MST_PK,1,BK.PORT_MST_POL_FK, BK.PORT_MST_POD_FK))");
					sb.Append("   ELSE NULL  END SURCHARGE");
					if (CargoType == 4) {
						sb.Append("                   ,CMT.COMMODITY_MST_PK");
					}
					sb.Append("          FROM JOB_CARD_TRN    JC,");
					sb.Append("               BOOKING_MST_TBL         BK,");
					sb.Append("               HBL_EXP_TBL             HBL,");
					sb.Append("               JOB_TRN_CONT    JT,");
					sb.Append("               JOB_TRN_FD      JF,");
					sb.Append("               FREIGHT_ELEMENT_MST_TBL FM,");
					sb.Append("               CURRENCY_TYPE_MST_TBL   CY,");
					sb.Append("               LOCATION_MST_TBL        LM,");
					sb.Append("               CUSTOMER_MST_TBL        CM,");
					sb.Append("               CONTAINER_TYPE_MST_TBL  CTY,");
					sb.Append("               COMMODITY_MST_TBL       CMT,DIMENTION_UNIT_MST_TBL UOM");
					sb.Append("         WHERE JC.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK");
					sb.Append("           AND HBL.HBL_EXP_TBL_PK = JC.HBL_HAWB_FK");
					if (CargoType == 4) {
						sb.Append("       AND JT.JOB_TRN_CONT_PK = JF.JOB_CARD_TRN_FK ");
					}
					sb.Append("           AND JC.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
					sb.Append("           AND UOM.DIMENTION_UNIT_MST_PK(+)= JT.BASIS_FK");
					sb.Append("           AND JF.FREIGHT_ELEMENT_MST_FK = FM.FREIGHT_ELEMENT_MST_PK");
					sb.Append("           AND JF.LOCATION_MST_FK = LM.LOCATION_MST_PK");
					sb.Append("           AND JF.FRTPAYER_CUST_MST_FK = CM.CUSTOMER_MST_PK");
					if (CargoType == 2) {
						sb.Append("           AND JT.CONTAINER_TYPE_MST_FK = CTY.CONTAINER_TYPE_MST_PK(+)");
					} else {
						sb.Append("           AND JF.CONTAINER_TYPE_MST_FK = CTY.CONTAINER_TYPE_MST_PK(+)");
					}
					sb.Append("           AND JF.CURRENCY_MST_FK = CY.CURRENCY_MST_PK");
					sb.Append("           AND CMT.COMMODITY_MST_PK(+) = JT.COMMODITY_MST_FK");
					sb.Append("           AND BK.BOOKING_MST_PK = JC.BOOKING_MST_FK");
					sb.Append("           AND HBL.HBL_EXP_TBL_PK = " + BLPK);
					if (CargoType == 4) {
						sb.Append("         ORDER BY CMT.COMMODITY_MST_PK,FM.PREFERENCE) Q");
					} else {
						sb.Append("         ORDER BY CONTAINER_TYPE_MST_PK,FM.PREFERENCE) Q");
					}

				} else if (!string.IsNullOrEmpty(FREIGHT_CORRECTOR_HDR_PK) & flag == 2) {
					sb.Append("SELECT JOB_TRN_FD_PK,");
					sb.Append("       JOB_CARD_TRN_PK,");
					sb.Append("       CONTAINER_TYPE_MST_PK,");
					sb.Append("       CONTAINER_TYPE_MST_ID,");
					sb.Append("       FREIGHT_ELEMENT_MST_PK,");
					sb.Append("       FREIGHT_ELEMENT_ID,");
					sb.Append("       FREIGHT_ELEMENT_NAME,");
					sb.Append("       PMT_TYPE,");
					sb.Append("       LOCATION_MST_PK,");
					sb.Append("       LOCATION_ID,");
					sb.Append("       CUSTOMER_MST_PK,");
					sb.Append("       FRT_PAYER,");
					sb.Append("       RATEPERBASIS,");
					sb.Append("       FREIGHT_AMT,");
					sb.Append("       CURRENCY_MST_PK,");
					sb.Append("       CURRENCY_ID,");
					sb.Append("       EXCHANGE_RATE,");
					sb.Append("       TOTAL,");
					sb.Append("       QTY,");
					sb.Append("       WEIGHT,");
					sb.Append("       VOLUME,");
					sb.Append("       BASIS,CREDIT,SURCHARGE");
					sb.Append("  FROM ( SELECT DISTINCT FCT.FREIGHT_CRCTR_TRN_PK JOB_TRN_FD_PK,");
					sb.Append("                JC.JOB_CARD_TRN_PK,");
					sb.Append("                CASE");
					sb.Append("                  WHEN BK.CARGO_TYPE = 4 THEN");
					sb.Append("                   CMT.COMMODITY_MST_PK");
					sb.Append("                  ELSE");
					sb.Append("                   CTY.CONTAINER_TYPE_MST_PK");
					sb.Append("                END CONTAINER_TYPE_MST_PK,");
					sb.Append("                CASE");
					sb.Append("                  WHEN BK.CARGO_TYPE = 4 THEN");
					sb.Append("                   CMT.COMMODITY_NAME");
					sb.Append("                  ELSE");
					sb.Append("                   CTY.CONTAINER_TYPE_MST_ID");
					sb.Append("                END CONTAINER_TYPE_MST_ID,");
					sb.Append("                FM.FREIGHT_ELEMENT_MST_PK,");
					sb.Append("                FM.FREIGHT_ELEMENT_ID,");
					sb.Append("                FM.FREIGHT_ELEMENT_NAME,");
					sb.Append("                DECODE(FCT.PMT_TYPE, 1, 'Prepaid', 2, 'Collect') PMT_TYPE,");
					sb.Append("                LM.LOCATION_MST_PK,");
					sb.Append("                LM.LOCATION_ID,");
					sb.Append("                CM.CUSTOMER_MST_PK,");
					sb.Append("                CM.CUSTOMER_NAME FRT_PAYER,");
					sb.Append("                NVL(FCT.CORRECTED_FREIGHT, 0) RATEPERBASIS,");
					sb.Append("                NVL(FCT.TOTAL_CORRECTED_FREIGHT, 0) FREIGHT_AMT,");
					sb.Append("                CY.CURRENCY_MST_PK,");
					sb.Append("                CY.CURRENCY_ID,");
					sb.Append("                FCT.EXCHANGE_RATE,");
					sb.Append("                (FCT.TOTAL_CORRECTED_FREIGHT * FCT.EXCHANGE_RATE) TOTAL,");
					if (CargoType == 4) {
						sb.Append("                JT.PACK_COUNT QTY,");
						sb.Append("                JT.CHARGEABLE_WEIGHT WEIGHT,");
						sb.Append("                JT.VOLUME_IN_CBM VOLUME,");
					} else {
						sb.Append("                0 QTY,");
						sb.Append("                0 WEIGHT,");
						sb.Append("                0 VOLUME,");
					}
					sb.Append("                UOM.DIMENTION_ID BASIS,");
					sb.Append("                FM.CREDIT,");
					sb.Append("                FM.PREFERENCE,");
					if (CargoType == 4) {
						sb.Append("                CMT.COMMODITY_MST_PK");
					} else {
						sb.Append("                0 COMMODITY_MST_PK");
					}
					sb.Append(" , CASE ");
					sb.Append(" WHEN FM.CHARGE_BASIS = 1 AND");
					sb.Append("   FM.FREIGHT_ELEMENT_ID <> 'BOF' THEN");
					sb.Append("   (GET_BASIS_VALUE(BK.PORT_MST_POL_FK,");
					sb.Append("   BK.PORT_MST_POD_FK,");
					sb.Append("   FM.FREIGHT_ELEMENT_MST_PK) ||");
					sb.Append("   '% of' ||");
					sb.Append("   CONCADINATE_FUN_FREIGHTELEMENT(FM.FREIGHT_ELEMENT_MST_PK,1,BK.PORT_MST_POL_FK, BK.PORT_MST_POD_FK))");
					sb.Append("   ELSE NULL  END SURCHARGE");

					sb.Append("  FROM JOB_CARD_TRN    JC,");
					sb.Append("       BOOKING_MST_TBL         BK,");
					sb.Append("       HBL_EXP_TBL             HBL,");
					sb.Append("       JOB_TRN_CONT    JT,");
					sb.Append("       JOB_TRN_FD      JF,");
					sb.Append("       FREIGHT_CORRECTOR_HDR   FCH,");
					sb.Append("       FREIGHT_CRCTR_TRN       FCT,");
					sb.Append("       FREIGHT_ELEMENT_MST_TBL FM,");
					sb.Append("       CURRENCY_TYPE_MST_TBL   CY,");
					sb.Append("       LOCATION_MST_TBL        LM,");
					sb.Append("       CUSTOMER_MST_TBL        CM,");
					sb.Append("       CONTAINER_TYPE_MST_TBL  CTY,");
					sb.Append("       COMMODITY_MST_TBL       CMT,DIMENTION_UNIT_MST_TBL UOM");
					sb.Append(" WHERE JC.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK");
					if (STATUS == "APPROVED" | STATUS == "Approved") {
						sb.Append("   AND HBL.NEW_JOB_CARD_SEA_EXP_FK = JC.JOB_CARD_TRN_PK");
					} else {
						sb.Append("   AND HBL.HBL_EXP_TBL_PK = JC.HBL_HAWB_FK");
					}
					if (CargoType == 4) {
						sb.Append("   AND JT.JOB_TRN_CONT_PK = JF.JOB_CARD_TRN_FK");
					}
					sb.Append("   AND JF.JOB_TRN_FD_PK = FCT.JOB_TRN_SEA_EXP_FD_FK");
					sb.Append("   AND FCH.HBL_FK = HBL.HBL_EXP_TBL_PK");
					sb.Append("   AND FCT.FREIGHT_CORRECTOR_HDR_FK = FCH.FREIGHT_CORRECTOR_HDR_PK");
					sb.Append("   AND JC.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
					sb.Append("   AND FCT.FREIGHT_ELEMENT_MST_FK = FM.FREIGHT_ELEMENT_MST_PK");
					sb.Append("   AND FCT.INVOICE_LOCATION_FK = LM.LOCATION_MST_PK");
					sb.Append("   AND FCT.INVOICE_CUSTOMER_FK = CM.CUSTOMER_MST_PK");
					sb.Append("  AND UOM.DIMENTION_UNIT_MST_PK(+)= JT.BASIS_FK");
					if (CargoType == 2) {
						sb.Append("   AND JT.CONTAINER_TYPE_MST_FK = CTY.CONTAINER_TYPE_MST_PK(+)");
					} else {
						sb.Append("   AND JF.CONTAINER_TYPE_MST_FK = CTY.CONTAINER_TYPE_MST_PK(+)");
						//sb.Append("   AND JT.JOB_TRN_CONT_PK = JF.JOB_CARD_TRN_FK")
					}
					sb.Append("   AND FCT.CORRECTED_CURR_FK = CY.CURRENCY_MST_PK");
					sb.Append("   AND CMT.COMMODITY_MST_PK(+) = JT.COMMODITY_MST_FK");
					sb.Append("   AND BK.BOOKING_MST_PK = JC.BOOKING_MST_FK");
					sb.Append("   AND FCH.FREIGHT_CORRECTOR_HDR_PK = " + FREIGHT_CORRECTOR_HDR_PK);
					if (CargoType != 4) {
						sb.Append("         ORDER BY CONTAINER_TYPE_MST_ID,FM.PREFERENCE) Q");
					} else {
						sb.Append("         ORDER BY CMT.COMMODITY_MST_PK, FM.PREFERENCE) Q");
					}
				} else {
					sb.Append("SELECT JOB_TRN_FD_PK,");
					sb.Append("       JOB_CARD_TRN_PK,");
					sb.Append("       CONTAINER_TYPE_MST_PK,");
					sb.Append("       CONTAINER_TYPE_MST_ID,");
					sb.Append("       FREIGHT_ELEMENT_MST_PK,");
					sb.Append("       FREIGHT_ELEMENT_ID,");
					sb.Append("       FREIGHT_ELEMENT_NAME,");
					sb.Append("       PMT_TYPE,");
					sb.Append("       LOCATION_MST_PK,");
					sb.Append("       LOCATION_ID,");
					sb.Append("       CUSTOMER_MST_PK,");
					sb.Append("       FRT_PAYER,");
					sb.Append("       RATEPERBASIS,");
					sb.Append("       FREIGHT_AMT,");
					sb.Append("       CURRENCY_MST_PK,");
					sb.Append("       CURRENCY_ID,");
					sb.Append("       EXCHANGE_RATE,");
					sb.Append("       TOTAL,");
					sb.Append("       QTY,");
					sb.Append("       WEIGHT,");
					sb.Append("       VOLUME,");
					sb.Append("       BASIS,CREDIT,SURCHARGE");
					sb.Append("  FROM ( SELECT DISTINCT JF.JOB_TRN_FD_PK,");
					sb.Append("                JC.JOB_CARD_TRN_PK,");
					sb.Append("                CASE");
					sb.Append("                  WHEN BK.CARGO_TYPE = 4 THEN");
					sb.Append("                   CMT.COMMODITY_MST_PK");
					sb.Append("                  ELSE");
					sb.Append("                   CTY.CONTAINER_TYPE_MST_PK");
					sb.Append("                END CONTAINER_TYPE_MST_PK,");
					sb.Append("                CASE");
					sb.Append("                  WHEN BK.CARGO_TYPE = 4 THEN");
					sb.Append("                   CMT.COMMODITY_NAME");
					sb.Append("                  ELSE");
					sb.Append("                   CTY.CONTAINER_TYPE_MST_ID");
					sb.Append("                END CONTAINER_TYPE_MST_ID,");
					sb.Append("                FM.FREIGHT_ELEMENT_MST_PK,");
					sb.Append("                FM.FREIGHT_ELEMENT_ID,");
					sb.Append("                FM.FREIGHT_ELEMENT_NAME,");
					sb.Append("                DECODE(FCT.PMT_TYPE, 1, 'Prepaid', 2, 'Collect') PMT_TYPE,");
					sb.Append("                LM.LOCATION_MST_PK,");
					sb.Append("                LM.LOCATION_ID,");
					sb.Append("                CM.CUSTOMER_MST_PK,");
					sb.Append("                CM.CUSTOMER_NAME FRT_PAYER,");
					sb.Append("                NVL(FCT.BL_FREIGHT, 0) RATEPERBASIS,");
					sb.Append("                NVL(FCT.TOTAL_FREIGHT, 0) FREIGHT_AMT,");
					sb.Append("                CY.CURRENCY_MST_PK,");
					sb.Append("                CY.CURRENCY_ID,");
					sb.Append("                FCT.EXCHANGE_RATE,");
					sb.Append("                (FCT.TOTAL_FREIGHT * FCT.EXCHANGE_RATE) TOTAL,");
					if (CargoType == 4) {
						sb.Append("                JT.PACK_COUNT QTY,");
						sb.Append("                JT.CHARGEABLE_WEIGHT WEIGHT,");
						sb.Append("                JT.VOLUME_IN_CBM VOLUME,");
					} else {
						sb.Append("                0 QTY,");
						sb.Append("                0 WEIGHT,");
						sb.Append("                0 VOLUME,");
					}
					sb.Append("                UOM.DIMENTION_ID BASIS,");
					sb.Append("                FM.CREDIT,");
					sb.Append("                FM.PREFERENCE,");
					if (CargoType == 4) {
						sb.Append("                CMT.COMMODITY_MST_PK");
					} else {
						sb.Append("                0 COMMODITY_MST_PK");
					}
					sb.Append(" , CASE ");
					sb.Append(" WHEN FM.CHARGE_BASIS = 1 AND");
					sb.Append("   FM.FREIGHT_ELEMENT_ID <> 'BOF' THEN");
					sb.Append("   (GET_BASIS_VALUE(BK.PORT_MST_POL_FK,");
					sb.Append("   BK.PORT_MST_POD_FK,");
					sb.Append("   FM.FREIGHT_ELEMENT_MST_PK) ||");
					sb.Append("   '% of' ||");
					sb.Append("   CONCADINATE_FUN_FREIGHTELEMENT(FM.FREIGHT_ELEMENT_MST_PK,1,BK.PORT_MST_POL_FK,                                                           BK.PORT_MST_POD_FK))");
					sb.Append("   ELSE NULL  END SURCHARGE");

					sb.Append("  FROM JOB_CARD_TRN    JC,");
					sb.Append("       BOOKING_MST_TBL         BK,");
					sb.Append("       HBL_EXP_TBL             HBL,");
					sb.Append("       JOB_TRN_CONT    JT,");
					sb.Append("       JOB_TRN_FD      JF,");
					sb.Append("       FREIGHT_CORRECTOR_HDR   FCH,");
					sb.Append("       FREIGHT_BL_FCN          FCT,");
					sb.Append("       FREIGHT_ELEMENT_MST_TBL FM,");
					sb.Append("       CURRENCY_TYPE_MST_TBL   CY,");
					sb.Append("       LOCATION_MST_TBL        LM,");
					sb.Append("       CUSTOMER_MST_TBL        CM,");
					sb.Append("       CONTAINER_TYPE_MST_TBL  CTY,");
					sb.Append("       COMMODITY_MST_TBL       CMT,DIMENTION_UNIT_MST_TBL UOM");
					sb.Append(" WHERE JC.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK");
					if (STATUS == "APPROVED" | STATUS == "Approved") {
						sb.Append("   AND HBL.NEW_JOB_CARD_SEA_EXP_FK = JC.JOB_CARD_TRN_PK");
					} else {
						sb.Append("   AND HBL.HBL_EXP_TBL_PK = JC.HBL_HAWB_FK");
					}
					if (CargoType == 4) {
						sb.Append("       AND JT.JOB_TRN_CONT_PK = JF.JOB_CARD_TRN_FK ");
					}
					sb.Append("   AND JF.JOB_TRN_FD_PK = FCT.FREIGHT_PK");
					sb.Append("   AND FCH.HBL_FK = HBL.HBL_EXP_TBL_PK");
					sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK(+)= JT.BASIS_FK");
					sb.Append("   AND FCT.FREIGHT_CORRECTOR_HDR_FK = FCH.FREIGHT_CORRECTOR_HDR_PK");
					sb.Append("   AND JC.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
					sb.Append("   AND FCT.FREIGHT_ELEMENT_MST_FK = FM.FREIGHT_ELEMENT_MST_PK");
					sb.Append("   AND FCT.INVOICE_LOCATION_FK = LM.LOCATION_MST_PK");
					sb.Append("   AND FCT.INVOICE_CUSTOMER_FK = CM.CUSTOMER_MST_PK");
					if (CargoType == 2) {
						sb.Append("   AND JT.CONTAINER_TYPE_MST_FK = CTY.CONTAINER_TYPE_MST_PK(+)");
					} else {
						sb.Append("   AND JF.CONTAINER_TYPE_MST_FK = CTY.CONTAINER_TYPE_MST_PK(+)");
						//sb.Append("       AND JT.JOB_TRN_CONT_PK = JF.JOB_CARD_TRN_FK ")
					}
					sb.Append("   AND FCT.BL_CURRENCY_FK = CY.CURRENCY_MST_PK");
					sb.Append("   AND CMT.COMMODITY_MST_PK(+) = JT.COMMODITY_MST_FK");
					sb.Append("   AND BK.BOOKING_MST_PK = JC.BOOKING_MST_FK");
					sb.Append("   AND HBL.HBL_EXP_TBL_PK = " + BLPK);
					sb.Append("   AND FCH.FREIGHT_CORRECTOR_HDR_PK = " + FREIGHT_CORRECTOR_HDR_PK);
					if (CargoType != 4) {
						sb.Append("         ORDER BY CONTAINER_TYPE_MST_ID,FM.PREFERENCE) Q");
					} else {
						sb.Append("         ORDER BY CMT.COMMODITY_MST_PK, FM.PREFERENCE) Q");
					}
				}
			//'For AIR
			} else {
				if (string.IsNullOrEmpty(FREIGHT_CORRECTOR_HDR_PK)) {
					sb.Append("SELECT DISTINCT JF.JOB_TRN_FD_PK JOB_TRN_FD_PK,");
					sb.Append("                JC.JOB_CARD_TRN_PK JOB_CARD_TRN_PK,");
					sb.Append("                CMT.COMMODITY_MST_PK  CONTAINER_TYPE_MST_PK,");
					sb.Append("                CMT.COMMODITY_NAME CONTAINER_TYPE_MST_ID,");
					sb.Append("                FM.FREIGHT_ELEMENT_MST_PK,");
					sb.Append("                FM.FREIGHT_ELEMENT_ID,");
					sb.Append("                FM.FREIGHT_ELEMENT_NAME,");
					sb.Append("                DECODE(JF.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') PMT_TYPE,");
					sb.Append("                LM.LOCATION_MST_PK,");
					sb.Append("                LM.LOCATION_ID,");
					sb.Append("                CM.CUSTOMER_MST_PK,");
					sb.Append("                CM.CUSTOMER_NAME FRT_PAYER,");
					sb.Append("                NVL(JF.RATEPERBASIS, 0) RATEPERBASIS,");
					sb.Append("                JF.FREIGHT_AMT,");
					sb.Append("                CY.CURRENCY_MST_PK,");
					sb.Append("                CY.CURRENCY_ID,");
					sb.Append("                JF.EXCHANGE_RATE,");
					sb.Append("                (JF.FREIGHT_AMT * JF.EXCHANGE_RATE) TOTAL,");
					sb.Append("                BK.NO_OF_BOXES QTY,");
					sb.Append("                (SELECT DISTINCT SUM(CT.CHARGEABLE_WEIGHT)");
					sb.Append("                   FROM JOB_TRN_CONT CT");
					sb.Append("                  WHERE CT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) WEIGHT,");
					sb.Append("                ");
					sb.Append("                (SELECT DISTINCT SUM(CT.VOLUME_IN_CBM)");
					sb.Append("                   FROM JOB_TRN_CONT CT");
					sb.Append("                  WHERE CT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) VOLUME,");
					sb.Append("                ");
					sb.Append("                JF.BASIS,");
					sb.Append("                FM.CREDIT,");
					sb.Append("                '' SURCHARGE");
					sb.Append("  FROM JOB_CARD_TRN    JC,");
					sb.Append("       BOOKING_MST_TBL         BK,");
					sb.Append("       HAWB_EXP_TBL            HBL,");
					sb.Append("       JOB_TRN_CONT    JT,");
					sb.Append("       JOB_TRN_FD      JF,");
					sb.Append("       FREIGHT_ELEMENT_MST_TBL FM,");
					sb.Append("       CURRENCY_TYPE_MST_TBL   CY,");
					sb.Append("       LOCATION_MST_TBL        LM,");
					sb.Append("       CUSTOMER_MST_TBL        CM,");
					sb.Append("       COMMODITY_MST_TBL       CMT");
					sb.Append(" WHERE JC.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK");
					sb.Append("   AND HBL.HAWB_EXP_TBL_PK = JC.HBL_HAWB_FK");
					sb.Append("   AND JC.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
					sb.Append("   AND JF.FREIGHT_ELEMENT_MST_FK = FM.FREIGHT_ELEMENT_MST_PK");
					sb.Append("   AND JF.LOCATION_MST_FK = LM.LOCATION_MST_PK");
					sb.Append("   AND JF.FRTPAYER_CUST_MST_FK = CM.CUSTOMER_MST_PK");
					sb.Append("   AND JF.CURRENCY_MST_FK = CY.CURRENCY_MST_PK");
					sb.Append("   AND CMT.COMMODITY_MST_PK(+) = JT.COMMODITY_MST_FK");
					sb.Append("   AND BK.BOOKING_MST_PK = JC.BOOKING_MST_FK");
					sb.Append("   AND HBL.HAWB_EXP_TBL_PK = " + BLPK);
					sb.Append("   ORDER BY FM.FREIGHT_ELEMENT_ID");
				} else if (!string.IsNullOrEmpty(FREIGHT_CORRECTOR_HDR_PK) & flag == 2) {
					sb.Append("SELECT JOB_TRN_FD_PK JOB_TRN_FD_PK ,");
					sb.Append("       JOB_CARD_TRN_PK,");
					sb.Append("       CONTAINER_TYPE_MST_PK,");
					sb.Append("       CONTAINER_TYPE_MST_ID,");
					sb.Append("       FREIGHT_ELEMENT_MST_PK,");
					sb.Append("       FREIGHT_ELEMENT_ID,");
					sb.Append("       FREIGHT_ELEMENT_NAME,");
					sb.Append("       PMT_TYPE,");
					sb.Append("       LOCATION_MST_PK,");
					sb.Append("       LOCATION_ID,");
					sb.Append("       CUSTOMER_MST_PK,");
					sb.Append("       FRT_PAYER,");
					sb.Append("       RATEPERBASIS,");
					sb.Append("       FREIGHT_AMT,");
					sb.Append("       CURRENCY_MST_PK,");
					sb.Append("       CURRENCY_ID,");
					sb.Append("       EXCHANGE_RATE,");
					sb.Append("       TOTAL,");
					sb.Append("       QTY,");
					sb.Append("       WEIGHT,");
					sb.Append("       VOLUME,");
					sb.Append("       BASIS,");
					sb.Append("       CREDIT,");
					sb.Append("       SURCHARGE");
					sb.Append("  FROM (SELECT DISTINCT FCT.FREIGHT_CRCTR_TRN_PK JOB_TRN_FD_PK,");
					sb.Append("                        JC.JOB_CARD_TRN_PK,");
					sb.Append("                        CMT.COMMODITY_MST_PK CONTAINER_TYPE_MST_PK,");
					sb.Append("                        CMT.COMMODITY_NAME CONTAINER_TYPE_MST_ID,");
					sb.Append("                        FM.FREIGHT_ELEMENT_MST_PK,");
					sb.Append("                        FM.FREIGHT_ELEMENT_ID,");
					sb.Append("                        FM.FREIGHT_ELEMENT_NAME,");
					sb.Append("                        DECODE(FCT.PMT_TYPE, 1, 'Prepaid', 2, 'Collect') PMT_TYPE,");
					sb.Append("                        LM.LOCATION_MST_PK,");
					sb.Append("                        LM.LOCATION_ID,");
					sb.Append("                        CM.CUSTOMER_MST_PK,");
					sb.Append("                        CM.CUSTOMER_NAME FRT_PAYER,");
					sb.Append("                        NVL(FCT.CORRECTED_FREIGHT, 0) RATEPERBASIS,");
					sb.Append("                        NVL(FCT.TOTAL_CORRECTED_FREIGHT, 0) FREIGHT_AMT,");
					sb.Append("                        CY.CURRENCY_MST_PK,");
					sb.Append("                        CY.CURRENCY_ID,");
					sb.Append("                        FCT.EXCHANGE_RATE,");
					sb.Append("                        (FCT.TOTAL_CORRECTED_FREIGHT * FCT.EXCHANGE_RATE) TOTAL,");
					sb.Append("                        BK.NO_OF_BOXES QTY,");
					sb.Append("                        (SELECT DISTINCT SUM(CT.CHARGEABLE_WEIGHT)");
					sb.Append("                           FROM JOB_TRN_CONT CT");
					sb.Append("                          WHERE CT.JOB_CARD_TRN_FK =");
					sb.Append("                                JC.JOB_CARD_TRN_PK) WEIGHT,");
					sb.Append("                        (SELECT DISTINCT SUM(CT.VOLUME_IN_CBM)");
					sb.Append("                           FROM JOB_TRN_CONT CT");
					sb.Append("                          WHERE CT.JOB_CARD_TRN_FK =");
					sb.Append("                                JC.JOB_CARD_TRN_PK) VOLUME,");
					sb.Append("                        JF.BASIS BASIS,");
					sb.Append("                        FM.CREDIT,");
					sb.Append("                        FM.PREFERENCE,");
					sb.Append("                        CMT.COMMODITY_MST_PK,");
					sb.Append("                        CASE");
					sb.Append("                          WHEN FM.CHARGE_BASIS = 1 AND");
					sb.Append("                               FM.FREIGHT_ELEMENT_ID <> 'BOF' THEN");
					sb.Append("                           (FM.BASIS_VALUE || '% of' ||");
					sb.Append("                           CONCADINATE_FUN_FREIGHTELEMENT(FM.FREIGHT_ELEMENT_MST_PK,");
					sb.Append("                                                           1,BK.PORT_MST_POL_FK,BK.PORT_MST_POD_FK))");
					sb.Append("                          ELSE");
					sb.Append("                           NULL");
					sb.Append("                        END SURCHARGE");
					sb.Append("          FROM JOB_CARD_TRN    JC,");
					sb.Append("               BOOKING_MST_TBL         BK,");
					sb.Append("               HAWB_EXP_TBL            HBL,");
					sb.Append("               JOB_TRN_CONT    JT,");
					sb.Append("               JOB_TRN_FD      JF,");
					sb.Append("               FREIGHT_CORRECTOR_HDR   FCH,");
					sb.Append("               FREIGHT_CRCTR_TRN       FCT,");
					sb.Append("               FREIGHT_ELEMENT_MST_TBL FM,");
					sb.Append("               CURRENCY_TYPE_MST_TBL   CY,");
					sb.Append("               LOCATION_MST_TBL        LM,");
					sb.Append("               CUSTOMER_MST_TBL        CM,");
					sb.Append("               COMMODITY_MST_TBL       CMT");
					sb.Append("         WHERE JC.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK");
					if (STATUS == "APPROVED" | STATUS == "Approved") {
						sb.Append("           AND HBL.NEW_JOB_CARD_AIR_EXP_FK = JC.JOB_CARD_TRN_PK");
					} else {
						sb.Append("           AND HBL.HAWB_EXP_TBL_PK = JC.HBL_HAWB_FK");
					}
					sb.Append("           AND JF.JOB_TRN_FD_PK = FCT.JOB_TRN_AIR_EXP_FD_FK");
					sb.Append("           AND FCH.HAWB_FK = HBL.HAWB_EXP_TBL_PK");
					sb.Append("           AND FCT.FREIGHT_CORRECTOR_HDR_FK = FCH.FREIGHT_CORRECTOR_HDR_PK");
					sb.Append("           AND JC.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
					sb.Append("           AND FCT.FREIGHT_ELEMENT_MST_FK = FM.FREIGHT_ELEMENT_MST_PK");
					sb.Append("           AND FCT.INVOICE_LOCATION_FK = LM.LOCATION_MST_PK");
					sb.Append("           AND FCT.INVOICE_CUSTOMER_FK = CM.CUSTOMER_MST_PK");
					sb.Append("           AND FCT.CORRECTED_CURR_FK = CY.CURRENCY_MST_PK");
					sb.Append("           AND CMT.COMMODITY_MST_PK(+) = JT.COMMODITY_MST_FK");
					sb.Append("           AND BK.BOOKING_MST_PK = JC.BOOKING_MST_FK");
					sb.Append("           AND FCH.FREIGHT_CORRECTOR_HDR_PK = " + FREIGHT_CORRECTOR_HDR_PK);
					sb.Append("         ORDER BY FM.FREIGHT_ELEMENT_ID) Q");
					sb.Append("");
				} else {
					sb.Append("SELECT JOB_TRN_FD_PK JOB_TRN_FD_PK,");
					sb.Append("       JOB_CARD_TRN_PK,");
					sb.Append("       CONTAINER_TYPE_MST_PK,");
					sb.Append("       CONTAINER_TYPE_MST_ID,");
					sb.Append("       FREIGHT_ELEMENT_MST_PK,");
					sb.Append("       FREIGHT_ELEMENT_ID,");
					sb.Append("       FREIGHT_ELEMENT_NAME,");
					sb.Append("       PMT_TYPE,");
					sb.Append("       LOCATION_MST_PK,");
					sb.Append("       LOCATION_ID,");
					sb.Append("       CUSTOMER_MST_PK,");
					sb.Append("       FRT_PAYER,");
					sb.Append("       RATEPERBASIS,");
					sb.Append("       FREIGHT_AMT,");
					sb.Append("       CURRENCY_MST_PK,");
					sb.Append("       CURRENCY_ID,");
					sb.Append("       EXCHANGE_RATE,");
					sb.Append("       TOTAL,");
					sb.Append("       QTY,");
					sb.Append("       WEIGHT,");
					sb.Append("       VOLUME,");
					sb.Append("       BASIS,");
					sb.Append("       CREDIT,");
					sb.Append("       SURCHARGE");
					sb.Append("  FROM (SELECT DISTINCT JF.JOB_TRN_FD_PK,");
					sb.Append("                        JC.JOB_CARD_TRN_PK,");
					sb.Append("                        CMT.COMMODITY_MST_PK CONTAINER_TYPE_MST_PK,");
					sb.Append("                        CMT.COMMODITY_NAME CONTAINER_TYPE_MST_ID,");
					sb.Append("                        FM.FREIGHT_ELEMENT_MST_PK,");
					sb.Append("                        FM.FREIGHT_ELEMENT_ID,");
					sb.Append("                        FM.FREIGHT_ELEMENT_NAME,");
					sb.Append("                        DECODE(FCT.PMT_TYPE, 1, 'Prepaid', 2, 'Collect') PMT_TYPE,");
					sb.Append("                        LM.LOCATION_MST_PK,");
					sb.Append("                        LM.LOCATION_ID,");
					sb.Append("                        CM.CUSTOMER_MST_PK,");
					sb.Append("                        CM.CUSTOMER_NAME FRT_PAYER,");
					sb.Append("                        NVL(FCT.BL_FREIGHT, 0) RATEPERBASIS,");
					sb.Append("                        NVL(FCT.TOTAL_FREIGHT, 0) FREIGHT_AMT,");
					sb.Append("                        CY.CURRENCY_MST_PK,");
					sb.Append("                        CY.CURRENCY_ID,");
					sb.Append("                        FCT.EXCHANGE_RATE,");
					sb.Append("                        (FCT.TOTAL_FREIGHT * FCT.EXCHANGE_RATE) TOTAL,");
					sb.Append("                        BK.NO_OF_BOXES QTY,");
					sb.Append("                        ");
					sb.Append("                        (SELECT DISTINCT SUM(CT.CHARGEABLE_WEIGHT)");
					sb.Append("                           FROM JOB_TRN_CONT CT");
					sb.Append("                          WHERE CT.JOB_CARD_TRN_FK =");
					sb.Append("                                JC.JOB_CARD_TRN_PK) WEIGHT,");
					sb.Append("                        (SELECT DISTINCT SUM(CT.VOLUME_IN_CBM)");
					sb.Append("                           FROM JOB_TRN_CONT CT");
					sb.Append("                          WHERE CT.JOB_CARD_TRN_FK =");
					sb.Append("                                JC.JOB_CARD_TRN_PK) VOLUME,");
					sb.Append("                        JF.BASIS BASIS,");
					sb.Append("                        FM.CREDIT,");
					sb.Append("                        FM.PREFERENCE,");
					sb.Append("                        CMT.COMMODITY_MST_PK,");
					sb.Append("                        '' SURCHARGE");
					sb.Append("          FROM JOB_CARD_TRN    JC,");
					sb.Append("               BOOKING_MST_TBL         BK,");
					sb.Append("               HAWB_EXP_TBL            HBL,");
					sb.Append("               JOB_TRN_CONT    JT,");
					sb.Append("               JOB_TRN_FD      JF,");
					sb.Append("               FREIGHT_CORRECTOR_HDR   FCH,");
					sb.Append("               FREIGHT_BL_FCN          FCT,");
					sb.Append("               FREIGHT_ELEMENT_MST_TBL FM,");
					sb.Append("               CURRENCY_TYPE_MST_TBL   CY,");
					sb.Append("               LOCATION_MST_TBL        LM,");
					sb.Append("               CUSTOMER_MST_TBL        CM,");
					sb.Append("               COMMODITY_MST_TBL       CMT");
					sb.Append("         WHERE JC.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK");
					if (STATUS == "APPROVED" | STATUS == "Approved") {
						sb.Append("           AND HBL.NEW_JOB_CARD_AIR_EXP_FK = JC.JOB_CARD_TRN_PK");
					} else {
						sb.Append("           AND HBL.HAWB_EXP_TBL_PK = JC.HBL_HAWB_FK");
					}
					sb.Append("           AND JF.JOB_TRN_FD_PK = FCT.FREIGHT_PK");
					sb.Append("           AND FCH.HAWB_FK = HBL.HAWB_EXP_TBL_PK");
					sb.Append("           AND FCT.FREIGHT_CORRECTOR_HDR_FK = FCH.FREIGHT_CORRECTOR_HDR_PK");
					sb.Append("           AND JC.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
					sb.Append("           AND FCT.FREIGHT_ELEMENT_MST_FK = FM.FREIGHT_ELEMENT_MST_PK");
					sb.Append("           AND FCT.INVOICE_LOCATION_FK = LM.LOCATION_MST_PK");
					sb.Append("           AND FCT.INVOICE_CUSTOMER_FK = CM.CUSTOMER_MST_PK");
					sb.Append("           AND FCT.BL_CURRENCY_FK = CY.CURRENCY_MST_PK");
					sb.Append("           AND CMT.COMMODITY_MST_PK(+) = JT.COMMODITY_MST_FK");
					sb.Append("           AND BK.BOOKING_MST_PK = JC.BOOKING_MST_FK");
					sb.Append("           AND HBL.HAWB_EXP_TBL_PK = " + BLPK);
					sb.Append("           AND FCH.FREIGHT_CORRECTOR_HDR_PK = " + FREIGHT_CORRECTOR_HDR_PK);
					sb.Append("         ORDER BY FM.FREIGHT_ELEMENT_ID) Q");
					sb.Append("");
				}
			}

			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

        #endregion

        #region "BLHeader Details.."
        /// <summary>
        /// Feches the header.
        /// </summary>
        /// <param name="BLpk">The b LPK.</param>
        /// <param name="BOOKPK">The bookpk.</param>
        /// <param name="fcnpk">The FCNPK.</param>
        /// <param name="STATUS">The status.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public object FechHeader(long BLpk = 0, long BOOKPK = 0, int fcnpk = 0, string STATUS = "", int BizType = 0)
		{

			System.Data.OleDb.OleDbDataAdapter objAdp = new System.Data.OleDb.OleDbDataAdapter();
			WorkFlow objWF = new WorkFlow();
			DataSet dsData = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			if (BizType == 2) {
				if (fcnpk == 0) {
					sb.Append("SELECT DISTINCT HBL.HBL_EXP_TBL_PK BOOKING_BL_PK,");
					sb.Append("                HBL.HBL_REF_NO SERVICE_BL_NO,");
					sb.Append("                TO_CHAR(HBL.HBL_DATE, 'dd/mm/yyyy') AS BL_DATE,");
					sb.Append("                BKG.BOOKING_REF_NO BOOKING_ID,");
					sb.Append("                BKG.BOOKING_MST_PK BOOKING_TRN_PK,");
					sb.Append("                0 COMMERCIAL_SCHEDULE_HDR_PK,");
					sb.Append("                HBL.VESSEL_NAME VESSEL_NAME,");
					sb.Append("                HBL.VOYAGE VOYAGE_NO,");
					sb.Append("                NVL(CONSIGEE.CUSTOMER_MST_PK, 0) CONSIGEEPK,");
					sb.Append("                NVL(CONSIGEE.CUSTOMER_NAME, ' ') CONSIGEENAME,");
					sb.Append("                SHIPPER.CUSTOMER_MST_PK SHIPPERPK,");
					sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPERNAME,");
					sb.Append("                POL.PORT_ID POL,");
					sb.Append("                POL.PORT_NAME POLNAME,");
					sb.Append("                POL.PORT_MST_PK POLPK,");
					sb.Append("                POD.PORT_MST_PK PODPK,");
					sb.Append("                POD.PORT_ID PODID,");
					sb.Append("                POD.PORT_NAME PODNAME,");
					sb.Append("                POO.PORT_MST_PK POOPK,");
					sb.Append("                POO.PORT_ID POOID,");
					sb.Append("                POO.PORT_NAME POONAME,");
					sb.Append("                PFD.PORT_ID PFDID,");
					sb.Append("                PFD.PORT_NAME PFDNAME,");
					sb.Append("                PFD.PORT_MST_PK PFDPK,");
					sb.Append("                BKG.CARGO_TYPE");
					sb.Append("  FROM JOB_CARD_TRN       JOB,");
					sb.Append("       BOOKING_MST_TBL            BKG,");
					sb.Append("       HBL_EXP_TBL                HBL,");
					sb.Append("       PORT_MST_TBL               POL,");
					sb.Append("       PORT_MST_TBL               POD,");
					sb.Append("       CUSTOMER_MST_TBL           CONSIGEE,");
					sb.Append("       CUSTOMER_MST_TBL           SHIPPER,");
					sb.Append("       LOCATION_WORKING_PORTS_TRN LOC,");
					sb.Append("       PORT_MST_TBL               POO,");
					sb.Append("       PORT_MST_TBL               PFD");
					sb.Append(" WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
					sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK");
					sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CONSIGEE.CUSTOMER_MST_PK");
					sb.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
					sb.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
					sb.Append("   AND POL.PORT_MST_PK = LOC.PORT_MST_FK(+)");
					sb.Append("   AND BKG.POO_FK = POO.PORT_MST_PK(+)");
					sb.Append("   AND BKG.PFD_FK = PFD.PORT_MST_PK(+)");
					sb.Append("   AND HBL.HBL_EXP_TBL_PK = " + BLpk);
				} else {
					sb.Append("SELECT DISTINCT HBL.HBL_EXP_TBL_PK BOOKING_BL_PK,");
					sb.Append("                HBL.HBL_REF_NO SERVICE_BL_NO,");
					sb.Append("                TO_CHAR(HBL.HBL_DATE, 'dd/mm/yyyy') AS BL_DATE,");
					sb.Append("                BKG.BOOKING_REF_NO BOOKING_ID,");
					sb.Append("                BKG.BOOKING_MST_PK BOOKING_TRN_PK,");
					sb.Append("                0 COMMERCIAL_SCHEDULE_HDR_PK,");
					sb.Append("                HBL.VESSEL_NAME VESSEL_NAME,");
					sb.Append("                HBL.VOYAGE VOYAGE_NO,");
					sb.Append("                NVL(CONSIGEE.CUSTOMER_MST_PK, 0) CONSIGEEPK,");
					sb.Append("                NVL(CONSIGEE.CUSTOMER_NAME, ' ') CONSIGEENAME,");
					sb.Append("                SHIPPER.CUSTOMER_MST_PK SHIPPERPK,");
					sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPERNAME,");
					sb.Append("                POL.PORT_ID POL,");
					sb.Append("                POL.PORT_NAME POLNAME,");
					sb.Append("                POL.PORT_MST_PK POLPK,");
					sb.Append("                POD.PORT_MST_PK PODPK,");
					sb.Append("                POD.PORT_ID PODID,");
					sb.Append("                POD.PORT_NAME PODNAME,");
					sb.Append("                POO.PORT_MST_PK POOPK,");
					sb.Append("                POO.PORT_ID POOID,");
					sb.Append("                POO.PORT_NAME POONAME,");
					sb.Append("                PFD.PORT_ID PFDID,");
					sb.Append("                PFD.PORT_NAME PFDNAME,");
					sb.Append("                PFD.PORT_MST_PK PFDPK,");
					sb.Append("                BKG.CARGO_TYPE,FCH.NOTES,FCH.VERSION");
					sb.Append("  FROM JOB_CARD_TRN       JOB,");
					sb.Append("       BOOKING_MST_TBL            BKG,");
					sb.Append("       HBL_EXP_TBL                HBL,");
					sb.Append("       PORT_MST_TBL               POL,");
					sb.Append("       PORT_MST_TBL               POD,");
					sb.Append("       CUSTOMER_MST_TBL           CONSIGEE,");
					sb.Append("       CUSTOMER_MST_TBL           SHIPPER,");
					sb.Append("       LOCATION_WORKING_PORTS_TRN LOC,");
					sb.Append("       PORT_MST_TBL               POO,");
					sb.Append("       PORT_MST_TBL               PFD,FREIGHT_CORRECTOR_HDR FCH");
					sb.Append(" WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					if (STATUS == "APPROVED" | STATUS == "Approved") {
						sb.Append("   AND JOB.JOB_CARD_TRN_PK=HBL.NEW_JOB_CARD_SEA_EXP_FK");
					} else {
						sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
					}
					sb.Append("   AND FCH.HBL_FK = HBL.HBL_EXP_TBL_PK");
					sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK");
					sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CONSIGEE.CUSTOMER_MST_PK");
					sb.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
					sb.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
					sb.Append("   AND POL.PORT_MST_PK = LOC.PORT_MST_FK(+)");
					sb.Append("   AND BKG.POO_FK = POO.PORT_MST_PK(+)");
					sb.Append("   AND BKG.PFD_FK = PFD.PORT_MST_PK(+)");
					sb.Append("   AND HBL.HBL_EXP_TBL_PK = " + BLpk);
				}
			///For AIR
			} else {
				if (fcnpk == 0) {
					sb.Append("SELECT DISTINCT HBL.HAWB_EXP_TBL_PK BOOKING_BL_PK,");
					sb.Append("                HBL.HAWB_REF_NO SERVICE_BL_NO,");
					sb.Append("                TO_CHAR(HBL.HAWB_DATE, 'dd/mm/yyyy') AS BL_DATE,");
					sb.Append("                BKG.BOOKING_REF_NO BOOKING_ID,");
					sb.Append("                BKG.BOOKING_MST_PK BOOKING_TRN_PK,");
					sb.Append("                0 COMMERCIAL_SCHEDULE_HDR_PK,");
					sb.Append("                AM.AIRLINE_NAME VESSEL_NAME,");
					sb.Append("                HBL.FLIGHT_NO VOYAGE_NO,");
					sb.Append("                NVL(CONSIGEE.CUSTOMER_MST_PK, 0) CONSIGEEPK,");
					sb.Append("                NVL(CONSIGEE.CUSTOMER_NAME, ' ') CONSIGEENAME,");
					sb.Append("                SHIPPER.CUSTOMER_MST_PK SHIPPERPK,");
					sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPERNAME,");
					sb.Append("                POL.PORT_ID POL,");
					sb.Append("                POL.PORT_NAME POLNAME,");
					sb.Append("                POL.PORT_MST_PK POLPK,");
					sb.Append("                POD.PORT_MST_PK PODPK,");
					sb.Append("                POD.PORT_ID PODID,");
					sb.Append("                POD.PORT_NAME PODNAME,");
					sb.Append("                POO.PLACE_PK POOPK,");
					sb.Append("                POO.PLACE_CODE POOID,");
					sb.Append("                POO.PLACE_NAME POONAME,");
					sb.Append("                PFD.PLACE_CODE PFDID,");
					sb.Append("                PFD.PLACE_NAME PFDNAME,");
					sb.Append("                PFD.PLACE_PK PFDPK,");
					sb.Append("                0 CARGO_TYPE");
					sb.Append("  FROM JOB_CARD_TRN       JOB,");
					sb.Append("       BOOKING_MST_TBL            BKG,");
					sb.Append("       HAWB_EXP_TBL                HBL,");
					sb.Append("       AIRLINE_MST_TBL AM,");
					sb.Append("       PORT_MST_TBL               POL,");
					sb.Append("       PORT_MST_TBL               POD,");
					sb.Append("       CUSTOMER_MST_TBL           CONSIGEE,");
					sb.Append("       CUSTOMER_MST_TBL           SHIPPER,");
					sb.Append("       LOCATION_WORKING_PORTS_TRN LOC,");
					sb.Append("       PLACE_MST_TBL               POO,");
					sb.Append("       PLACE_MST_TBL               PFD");
					sb.Append(" WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK");
					sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK");
					sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CONSIGEE.CUSTOMER_MST_PK");
					sb.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
					sb.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
					sb.Append("   AND AM.AIRLINE_MST_PK=BKG.CARRIER_MST_FK");
					sb.Append("   AND POL.PORT_MST_PK = LOC.PORT_MST_FK(+)");
					sb.Append("   AND BKG.COL_PLACE_MST_FK = POO.PLACE_PK(+)");
					sb.Append("   AND BKG.DEL_PLACE_MST_FK = PFD.PLACE_PK(+)");
					sb.Append("   AND HBL.HAWB_EXP_TBL_PK = " + BLpk);
				} else {
					sb.Append("SELECT DISTINCT HBL.HAWB_EXP_TBL_PK BOOKING_BL_PK,");
					sb.Append("                HBL.HAWB_REF_NO SERVICE_BL_NO,");
					sb.Append("                TO_CHAR(HBL.HAWB_DATE, 'dd/mm/yyyy') AS BL_DATE,");
					sb.Append("                BKG.BOOKING_REF_NO BOOKING_ID,");
					sb.Append("                BKG.BOOKING_MST_PK BOOKING_TRN_PK,");
					sb.Append("                0 COMMERCIAL_SCHEDULE_HDR_PK,");
					sb.Append("                AM.AIRLINE_NAME VESSEL_NAME,");
					sb.Append("                HBL.FLIGHT_NO VOYAGE_NO,");
					sb.Append("                NVL(CONSIGEE.CUSTOMER_MST_PK, 0) CONSIGEEPK,");
					sb.Append("                NVL(CONSIGEE.CUSTOMER_NAME, ' ') CONSIGEENAME,");
					sb.Append("                SHIPPER.CUSTOMER_MST_PK SHIPPERPK,");
					sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPERNAME,");
					sb.Append("                POL.PORT_ID POL,");
					sb.Append("                POL.PORT_NAME POLNAME,");
					sb.Append("                POL.PORT_MST_PK POLPK,");
					sb.Append("                POD.PORT_MST_PK PODPK,");
					sb.Append("                POD.PORT_ID PODID,");
					sb.Append("                POD.PORT_NAME PODNAME,");
					sb.Append("                POO.PLACE_PK POOPK,");
					sb.Append("                POO.PLACE_CODE POOID,");
					sb.Append("                POO.PLACE_NAME POONAME,");
					sb.Append("                PFD.PLACE_CODE PFDID,");
					sb.Append("                PFD.PLACE_NAME PFDNAME,");
					sb.Append("                PFD.PLACE_PK PFDPK,");
					sb.Append("                0 CARGO_TYPE,FCH.NOTES,FCH.VERSION");
					sb.Append("  FROM JOB_CARD_TRN       JOB,");
					sb.Append("       BOOKING_MST_TBL            BKG,");
					sb.Append("       HAWB_EXP_TBL                HBL,");
					sb.Append("       AIRLINE_MST_TBL AM,");
					sb.Append("       PORT_MST_TBL               POL,");
					sb.Append("       PORT_MST_TBL               POD,");
					sb.Append("       CUSTOMER_MST_TBL           CONSIGEE,");
					sb.Append("       CUSTOMER_MST_TBL           SHIPPER,");
					sb.Append("       LOCATION_WORKING_PORTS_TRN LOC,");
					sb.Append("       PLACE_MST_TBL               POO,");
					sb.Append("       PLACE_MST_TBL               PFD,FREIGHT_CORRECTOR_HDR FCH");
					sb.Append(" WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("   AND FCH.HAWB_FK = HBL.HAWB_EXP_TBL_PK");
					if (STATUS == "APPROVED" | STATUS == "Approved") {
						sb.Append("   AND JOB.JOB_CARD_TRN_PK = HBL.NEW_JOB_CARD_AIR_EXP_FK");
					} else {
						sb.Append("   AND JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK");
					}
					sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK");
					sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CONSIGEE.CUSTOMER_MST_PK");
					sb.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
					sb.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
					sb.Append("   AND AM.AIRLINE_MST_PK=BKG.CARRIER_MST_FK");
					sb.Append("   AND POL.PORT_MST_PK = LOC.PORT_MST_FK(+)");
					sb.Append("   AND BKG.COL_PLACE_MST_FK = POO.PLACE_PK(+)");
					sb.Append("   AND BKG.DEL_PLACE_MST_FK = PFD.PLACE_PK(+)");
					sb.Append("   AND HBL.HAWB_EXP_TBL_PK = " + BLpk);
				}
			}
			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion


        #region "Update The Request "
        /// <summary>
        /// Updates the request FCN.
        /// </summary>
        /// <param name="Freight_correct_pk">The freight_correct_pk.</param>
        /// <param name="Status">The status.</param>
        /// <param name="note">The note.</param>
        /// <param name="reject">The reject.</param>
        /// <returns></returns>
        public object UpdateRequestFcn(int Freight_correct_pk = 0, long Status = 0, string note = "", int reject = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWK = new WorkFlow();
			OracleCommand UpdCmd = new OracleCommand();
			OracleTransaction tran = null;
			int rowAffected = 0;
			if (string.IsNullOrEmpty(note) & reject == 0) {
				if (Freight_correct_pk != 0 & Status != 0) {
					sb.Append(" update   freight_corrector_hdr FCH set FCH.Status='" + Status + "' where FCH.FREIGHT_CORRECTOR_HDR_PK='" + Freight_correct_pk + "'");
				}

			} else if (!string.IsNullOrEmpty(note.Trim()) & Freight_correct_pk != 0) {
				sb.Append(" update   freight_corrector_hdr FCH set FCH.Notes='" + note + "' where FCH.FREIGHT_CORRECTOR_HDR_PK='" + Freight_correct_pk + "'");
			} else if (reject != 0 & Freight_correct_pk != 0) {
				sb.Append(" update   freight_corrector_hdr FCH set FCH.Status='" + reject + "' where FCH.FREIGHT_CORRECTOR_HDR_PK='" + Freight_correct_pk + "'");
			}
			objWK.OpenConnection();
			objWK.MyCommand.CommandType = CommandType.Text;
			objWK.MyCommand.CommandText = sb.ToString();
			try {
				rowAffected = objWK.MyCommand.ExecuteNonQuery();
				if (rowAffected > 0) {
					arrMessage.Clear();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}
			} catch (Exception ex) {
				tran.Rollback();
				throw ex;
			} finally {
				if (objWK.MyConnection.State == ConnectionState.Open) {
					objWK.MyConnection.Close();
				}
			}
            return new object();
		}
        #endregion

        #region "FECH THE REQUEST FOR APPOVAL"
        /// <summary>
        /// Feches the request bl corrector.
        /// </summary>
        /// <param name="BLPK">The BLPK.</param>
        /// <param name="FROMDATE">The fromdate.</param>
        /// <param name="TODATE">The todate.</param>
        /// <param name="FCH_PK">The fc h_ pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public object FechRequestBlCorrector(int BLPK = 0, string FROMDATE = "", string TODATE = "", int FCH_PK = 0, int BizType = 0, int CargoType = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string strCondition = null;
			int locPK = 0;
			locPK = Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
			//SEA
			if (BizType == 2) {
				sb.Append("SELECT ROWNUM SLNO, Q.*");
				sb.Append("  FROM(SELECT * FROM (SELECT DISTINCT '' OPTFLAG,");
				sb.Append("                        HBL.HBL_EXP_TBL_PK BL_PK,");
				sb.Append("                        HBL.HBL_REF_NO BL_NO,");
				sb.Append("                        FCH.VERSION,");
				sb.Append("                        TO_DATE(HBL.HBL_DATE, DATEFORMAT) BL_DATE,");
				sb.Append("                        BKG.BOOKING_MST_PK BOOKING_PK,");
				sb.Append("                        BKG.BOOKING_REF_NO BOOKING_NO,");
				sb.Append("                        'SEA' BIZ_TYPE,");
				sb.Append("                        DECODE(BKG.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC')CARGO_TYPE,");
				sb.Append("                        HBL.VESSEL_NAME VESSEL_ID,");
				sb.Append("                        HBL.VOYAGE VOYAGE_NO,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN FCH.STATUS = 1 THEN");
				sb.Append("                           'REQUESTED'");
				sb.Append("                          WHEN FCH.STATUS = 2 THEN");
				sb.Append("                           'APPROVED'");
				sb.Append("                          WHEN FCH.STATUS = 3 THEN");
				sb.Append("                           'REJECTED'");
				sb.Append("                        END STATUS,");
				sb.Append("                        FCH.FREIGHT_CORRECTOR_HDR_PK,");
				sb.Append("                        BKG.PORT_MST_POL_FK POLPK,");
				sb.Append("                        BKG.PORT_MST_POD_FK PODPK,JOB.JOB_CARD_TRN_PK JOB_CARD_PK");
				sb.Append("                       ,CMT.CUSTOMER_MST_PK,CMT.CUSTOMER_NAME ");
				sb.Append("          FROM HBL_EXP_TBL                HBL,");
				sb.Append("               JOB_CARD_TRN       JOB,");
				sb.Append("               BOOKING_MST_TBL            BKG,");
				sb.Append("               FREIGHT_CORRECTOR_HDR      FCH,");
				sb.Append("               LOCATION_WORKING_PORTS_TRN LWPT,CUSTOMER_MST_TBL CMT");
				sb.Append("         WHERE (HBL.HBL_EXP_TBL_PK = JOB.HBL_HAWB_FK OR JOB.JOB_CARD_TRN_PK=HBL.NEW_JOB_CARD_SEA_EXP_FK)");
				sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
				sb.Append("           AND FCH.HBL_FK = HBL.HBL_EXP_TBL_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK=JOB.SHIPPER_CUST_MST_FK ");
				//Conditions
				if (BLPK != 0) {
					sb.Append("  AND HBL.HBL_EXP_TBL_PK =" + BLPK);
				}
				if (FCH_PK != 0) {
					sb.Append("   AND FCH.FREIGHT_CORRECTOR_HDR_PK =" + FCH_PK);
				}
				if (FROMDATE.ToString().Trim().Length > 0) {
					sb.Append("           AND HBL.HBL_DATE >= to_date('" + FROMDATE + "' ,'" + dateFormat + "')");
				}
				if (TODATE.ToString().Trim().Length > 0) {
					sb.Append("           AND HBL.HBL_DATE <= to_date('" + TODATE + "' ,'" + dateFormat + "')");
				}
				if (CargoType > 0) {
					sb.Append("   AND BKG.CARGO_TYPE = " + CargoType);
				}
				sb.Append("   AND LWPT.PORT_MST_FK=BKG.PORT_MST_POL_FK ");
				sb.Append("           AND LWPT.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
				sb.Append("                CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");

				//sb.Append("  UNION ")

				//sb.Append("  SELECT DISTINCT '' OPTFLAG,")
				//sb.Append("                        HBL.HBL_EXP_TBL_PK BL_PK,")
				//sb.Append("                        HBL.HBL_REF_NO BL_NO,")
				//sb.Append("                        FCH.VERSION,")
				//sb.Append("                        TO_CHAR(HBL.HBL_DATE, DATEFORMAT) BL_DATE,")
				//sb.Append("                        BKG.BOOKING_MST_PK BOOKING_PK,")
				//sb.Append("                        BKG.BOOKING_REF_NO BOOKING_NO,")
				//sb.Append("                        'Sea' BIZ_TYPE,")
				//sb.Append("                        DECODE(BKG.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC')CARGO_TYPE,")
				//sb.Append("                        HBL.VESSEL_NAME VESSEL_ID,")
				//sb.Append("                        HBL.VOYAGE VOYAGE_NO,")
				//sb.Append("                        CASE")
				//sb.Append("                          WHEN FCH.STATUS = 1 THEN")
				//sb.Append("                           'REQUESTED'")
				//sb.Append("                          WHEN FCH.STATUS = 2 THEN")
				//sb.Append("                           'APPROVED'")
				//sb.Append("                          WHEN FCH.STATUS = 3 THEN")
				//sb.Append("                           'REJECTED'")
				//sb.Append("                        END STATUS,")
				//sb.Append("                        FCH.FREIGHT_CORRECTOR_HDR_PK,")
				//sb.Append("                        BKG.PORT_MST_POL_FK POLPK,")
				//sb.Append("                        BKG.PORT_MST_POD_FK PODPK,JOB.JOB_CARD_TRN_PK JOB_CARD_PK")
				//sb.Append("                       ,CMT.CUSTOMER_MST_PK,CMT.CUSTOMER_NAME")
				//sb.Append("          FROM HBL_EXP_TBL                HBL,")
				//sb.Append("               JOB_CARD_TRN       JOB,")
				//sb.Append("               BOOKING_MST_TBL            BKG,")
				//sb.Append("               FREIGHT_CORRECTOR_HDR      FCH,")
				//sb.Append("               LOCATION_WORKING_PORTS_TRN,CUSTOMER_MST_TBL CMT")
				//sb.Append("         WHERE JOB.JOB_CARD_TRN_PK=HBL.NEW_JOB_CARD_SEA_EXP_FK")
				//sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK")
				//sb.Append("           AND FCH.HBL_FK = HBL.HBL_EXP_TBL_PK")
				//sb.Append("           AND CMT.CUSTOMER_MST_PK=JOB.SHIPPER_CUST_MST_FK ")
				//'Conditions
				//'Conditions
				//If BLPK <> 0 Then
				//    sb.Append("  AND HBL.HBL_EXP_TBL_PK =" & BLPK)
				//End If
				//If FCH_PK <> 0 Then
				//    sb.Append("  AND  FCH.FREIGHT_CORRECTOR_HDR_PK =" & FCH_PK)
				//End If
				//If FROMDATE.ToString.Trim.Length > 0 Then
				//    sb.Append("           AND HBL.HBL_DATE >= to_date('" & FROMDATE & "' ,'" & dateFormat & "')")
				//End If
				//If TODATE.ToString.Trim.Length > 0 Then
				//    sb.Append("           AND HBL.HBL_DATE <= to_date('" & TODATE & "' ,'" & dateFormat & "')")
				//End If
				//If CargoType > 0 Then
				//    sb.Append("   AND BKG.CARGO_TYPE = " & CargoType)
				//End If
				//'End
				//sb.Append("           AND LOCATION_WORKING_PORTS_TRN.LOCATION_MST_FK IN")
				//sb.Append("               (SELECT L.LOCATION_MST_PK")
				//sb.Append("                  FROM LOCATION_MST_TBL L")
				//sb.Append("                 START WITH L.LOCATION_MST_PK = " & HttpContext.Current.Session("LOGED_IN_LOC_FK"))
				//sb.Append("                CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)")
				//sb.Append(strCondition) ' order by BL_DATE asc
				sb.Append("     ) ORDER BY TO_DATE(BL_DATE,DATEFORMAT) DESC, BL_NO DESC)Q ");
			//'AIR
			} else if (BizType == 1) {
				sb.Append("SELECT ROWNUM SLNO, Q.*");
				sb.Append(" FROM (SELECT * FROM( SELECT DISTINCT '' OPTFLAG,");
				sb.Append("                HBL.HAWB_EXP_TBL_PK BL_PK,");
				sb.Append("                HBL.HAWB_REF_NO BL_NO,");
				sb.Append("                FCH.VERSION,");
				sb.Append("                TO_DATE(HBL.HAWB_DATE,DATEFORMAT) BL_DATE,");
				sb.Append("                BKG.BOOKING_MST_PK BOOKING_PK,");
				sb.Append("                BKG.BOOKING_REF_NO BOOKING_NO,");
				sb.Append("                'AIR' BIZ_TYPE,");
				sb.Append("                'KGS/ULD' CARGO_TYPE,");
				sb.Append("                AM.AIRLINE_NAME VESSEL_ID,");
				sb.Append("                HBL.FLIGHT_NO VOYAGE_NO,");
				sb.Append("                CASE");
				sb.Append("                  WHEN FCH.STATUS = 1 THEN");
				sb.Append("                   'REQUESTED'");
				sb.Append("                  WHEN FCH.STATUS = 2 THEN");
				sb.Append("                   'APPROVED'");
				sb.Append("                  WHEN FCH.STATUS = 3 THEN");
				sb.Append("                   'REJECTED'");
				sb.Append("                END STATUS,");
				sb.Append("                FCH.FREIGHT_CORRECTOR_HDR_PK,");
				sb.Append("                BKG.PORT_MST_POL_FK POLPK,");
				sb.Append("                BKG.PORT_MST_POD_FK PODPK,");
				sb.Append("                JOB.JOB_CARD_TRN_PK JOB_CARD_PK,");
				sb.Append("                CMT.CUSTOMER_MST_PK,");
				sb.Append("                CMT.CUSTOMER_NAME");
				sb.Append("  FROM HAWB_EXP_TBL               HBL,");
				sb.Append("       JOB_CARD_TRN       JOB,");
				sb.Append("       BOOKING_MST_TBL            BKG,");
				sb.Append("       AIRLINE_MST_TBL            AM,");
				sb.Append("       FREIGHT_CORRECTOR_HDR      FCH,");
				sb.Append("       LOCATION_WORKING_PORTS_TRN LWPT,");
				sb.Append("       CUSTOMER_MST_TBL           CMT");
				sb.Append(" WHERE (HBL.HAWB_EXP_TBL_PK = JOB.HBL_HAWB_FK OR HBL.NEW_JOB_CARD_AIR_EXP_FK=JOB.JOB_CARD_TRN_PK) ");
				sb.Append("   AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
				sb.Append("   AND AM.AIRLINE_MST_PK = BKG.CARRIER_MST_FK");
				sb.Append("   AND FCH.HAWB_FK = HBL.HAWB_EXP_TBL_PK");
				sb.Append("   AND CMT.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK");
				//Conditions
				if (BLPK != 0) {
					sb.Append("  AND HBL.HAWB_EXP_TBL_PK =" + BLPK);
				}
				if (FCH_PK != 0) {
					sb.Append("  AND  FCH.FREIGHT_CORRECTOR_HDR_PK =" + FCH_PK);
				}
				if (FROMDATE.ToString().Trim().Length > 0) {
					sb.Append("           AND HBL.HAWB_DATE >= to_date('" + FROMDATE + "' ,'" + dateFormat + "')");
				}
				if (TODATE.ToString().Trim().Length > 0) {
					sb.Append("           AND HBL.HAWB_DATE <= to_date('" + TODATE + "' ,'" + dateFormat + "')");
				}
				sb.Append("   AND LWPT.PORT_MST_FK=BKG.PORT_MST_POL_FK ");
				sb.Append("   AND LWPT.LOCATION_MST_FK IN");
				sb.Append("       (SELECT L.LOCATION_MST_PK");
				sb.Append("          FROM LOCATION_MST_TBL L");
				sb.Append("                 START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
				sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");
				//sb.Append("   UNION ")
				//sb.Append("  SELECT DISTINCT '' OPTFLAG,")
				//sb.Append("                HBL.HAWB_EXP_TBL_PK BL_PK,")
				//sb.Append("                HBL.HAWB_REF_NO BL_NO,")
				//sb.Append("                FCH.VERSION,")
				//sb.Append("                TO_CHAR(HBL.HAWB_DATE,DATEFORMAT) BL_DATE,")
				//sb.Append("                BKG.BOOKING_MST_PK BOOKING_PK,")
				//sb.Append("                BKG.BOOKING_REF_NO BOOKING_NO,")
				//sb.Append("                'Air' BIZ_TYPE,")
				//sb.Append("                'AIR' CARGO_TYPE,")
				//sb.Append("                AM.AIRLINE_NAME VESSEL_ID,")
				//sb.Append("                HBL.FLIGHT_NO VOYAGE_NO,")
				//sb.Append("                CASE")
				//sb.Append("                  WHEN FCH.STATUS = 1 THEN")
				//sb.Append("                   'REQUESTED'")
				//sb.Append("                  WHEN FCH.STATUS = 2 THEN")
				//sb.Append("                   'APPROVED'")
				//sb.Append("                  WHEN FCH.STATUS = 3 THEN")
				//sb.Append("                   'REJECTED'")
				//sb.Append("                END STATUS,")
				//sb.Append("                FCH.FREIGHT_CORRECTOR_HDR_PK,")
				//sb.Append("                BKG.PORT_MST_POL_FK POLPK,")
				//sb.Append("                BKG.PORT_MST_POD_FK PODPK,")
				//sb.Append("                JOB.JOB_CARD_TRN_PK JOB_CARD_PK,")
				//sb.Append("                CMT.CUSTOMER_MST_PK,")
				//sb.Append("                CMT.CUSTOMER_NAME")
				//sb.Append("  FROM HAWB_EXP_TBL               HBL,")
				//sb.Append("       JOB_CARD_TRN       JOB,")
				//sb.Append("       BOOKING_MST_TBL            BKG,")
				//sb.Append("       AIRLINE_MST_TBL            AM,")
				//sb.Append("       FREIGHT_CORRECTOR_HDR      FCH,")
				//sb.Append("       LOCATION_WORKING_PORTS_TRN,")
				//sb.Append("       CUSTOMER_MST_TBL           CMT")
				//sb.Append(" WHERE HBL.NEW_JOB_CARD_AIR_EXP_FK=JOB.JOB_CARD_TRN_PK ")
				//sb.Append("   AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK")
				//sb.Append("   AND AM.AIRLINE_MST_PK = BKG.CARRIER_MST_FK")
				//sb.Append("   AND FCH.HAWB_FK = HBL.HAWB_EXP_TBL_PK")
				//'Conditions
				//If BLPK <> 0 Then
				//    sb.Append("  AND HBL.HAWB_EXP_TBL_PK =" & BLPK)
				//End If
				//If FCH_PK <> 0 Then
				//    sb.Append("   AND FCH.FREIGHT_CORRECTOR_HDR_PK =" & FCH_PK)
				//End If
				//If FROMDATE.ToString.Trim.Length > 0 Then
				//    sb.Append("           AND HBL.HAWB_DATE >= to_date('" & FROMDATE & "' ,'" & dateFormat & "')")
				//End If
				//If TODATE.ToString.Trim.Length > 0 Then
				//    sb.Append("           AND HBL.HAWB_DATE <= to_date('" & TODATE & "' ,'" & dateFormat & "')")
				//End If
				//'End
				//sb.Append("   AND CMT.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK")
				//sb.Append("   AND LOCATION_WORKING_PORTS_TRN.LOCATION_MST_FK IN")
				//sb.Append("       (SELECT L.LOCATION_MST_PK")
				//sb.Append("          FROM LOCATION_MST_TBL L")
				//sb.Append("                 START WITH L.LOCATION_MST_PK = " & HttpContext.Current.Session("LOGED_IN_LOC_FK"))
				//sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)")

				sb.Append("   ) ORDER BY TO_DATE(BL_DATE,DATEFORMAT) DESC, BL_NO DESC)Q ");
			} else {
				sb.Append("SELECT ROWNUM SLNO, Q.*");
				sb.Append("  FROM (SELECT * FROM (SELECT DISTINCT '' OPTFLAG,");
				sb.Append("                        HBL.HBL_EXP_TBL_PK BL_PK,");
				sb.Append("                        HBL.HBL_REF_NO BL_NO,");
				sb.Append("                        FCH.VERSION,");
				sb.Append("                        TO_DATE(HBL.HBL_DATE, DATEFORMAT) BL_DATE,");
				sb.Append("                        BKG.BOOKING_MST_PK BOOKING_PK,");
				sb.Append("                        BKG.BOOKING_REF_NO BOOKING_NO,");
				sb.Append("                        'SEA' BIZ_TYPE,");
				sb.Append("                        DECODE(BKG.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC')CARGO_TYPE,");
				sb.Append("                        HBL.VESSEL_NAME VESSEL_ID,");
				sb.Append("                        HBL.VOYAGE VOYAGE_NO,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN FCH.STATUS = 1 THEN");
				sb.Append("                           'REQUESTED'");
				sb.Append("                          WHEN FCH.STATUS = 2 THEN");
				sb.Append("                           'APPROVED'");
				sb.Append("                          WHEN FCH.STATUS = 3 THEN");
				sb.Append("                           'REJECTED'");
				sb.Append("                        END STATUS,");
				sb.Append("                        FCH.FREIGHT_CORRECTOR_HDR_PK,");
				sb.Append("                        BKG.PORT_MST_POL_FK POLPK,");
				sb.Append("                        BKG.PORT_MST_POD_FK PODPK,JOB.JOB_CARD_TRN_PK JOB_CARD_PK");
				sb.Append("                       ,CMT.CUSTOMER_MST_PK,CMT.CUSTOMER_NAME");
				sb.Append("          FROM HBL_EXP_TBL                HBL,");
				sb.Append("               JOB_CARD_TRN       JOB,");
				sb.Append("               BOOKING_MST_TBL            BKG,");
				sb.Append("               FREIGHT_CORRECTOR_HDR      FCH,");
				sb.Append("               LOCATION_WORKING_PORTS_TRN LWPT,CUSTOMER_MST_TBL CMT");
				sb.Append("         WHERE (HBL.HBL_EXP_TBL_PK = JOB.HBL_HAWB_FK OR JOB.JOB_CARD_TRN_PK=HBL.NEW_JOB_CARD_SEA_EXP_FK) ");
				sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
				sb.Append("           AND FCH.HBL_FK = HBL.HBL_EXP_TBL_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK=JOB.SHIPPER_CUST_MST_FK ");
				//Conditions
				if (BLPK != 0) {
					sb.Append("  AND HBL.HBL_EXP_TBL_PK =" + BLPK);
				}
				if (FCH_PK != 0) {
					sb.Append("   AND FCH.FREIGHT_CORRECTOR_HDR_PK =" + FCH_PK);
				}
				if (FROMDATE.ToString().Trim().Length > 0) {
					sb.Append("           AND HBL.HBL_DATE >= to_date('" + FROMDATE + "' ,'" + dateFormat + "')");
				}
				if (TODATE.ToString().Trim().Length > 0) {
					sb.Append("           AND HBL.HBL_DATE <= to_date('" + TODATE + "' ,'" + dateFormat + "')");
				}
				if (CargoType > 0) {
					sb.Append("   AND BKG.CARGO_TYPE = " + CargoType);
				}
				sb.Append("   AND LWPT.PORT_MST_FK=BKG.PORT_MST_POL_FK ");
				sb.Append("           AND LWPT.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
				sb.Append("                CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");

				//sb.Append("  UNION ")

				//sb.Append("  SELECT DISTINCT '' OPTFLAG,")
				//sb.Append("                        HBL.HBL_EXP_TBL_PK BL_PK,")
				//sb.Append("                        HBL.HBL_REF_NO BL_NO,")
				//sb.Append("                        FCH.VERSION,")
				//sb.Append("                        TO_CHAR(HBL.HBL_DATE, DATEFORMAT) BL_DATE,")
				//sb.Append("                        BKG.BOOKING_MST_PK BOOKING_PK,")
				//sb.Append("                        BKG.BOOKING_REF_NO BOOKING_NO,")
				//sb.Append("                        'Sea' BIZ_TYPE,")
				//sb.Append("                        DECODE(BKG.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC')CARGO_TYPE,")
				//sb.Append("                        HBL.VESSEL_NAME VESSEL_ID,")
				//sb.Append("                        HBL.VOYAGE VOYAGE_NO,")
				//sb.Append("                        CASE")
				//sb.Append("                          WHEN FCH.STATUS = 1 THEN")
				//sb.Append("                           'REQUESTED'")
				//sb.Append("                          WHEN FCH.STATUS = 2 THEN")
				//sb.Append("                           'APPROVED'")
				//sb.Append("                          WHEN FCH.STATUS = 3 THEN")
				//sb.Append("                           'REJECTED'")
				//sb.Append("                        END STATUS,")
				//sb.Append("                        FCH.FREIGHT_CORRECTOR_HDR_PK,")
				//sb.Append("                        BKG.PORT_MST_POL_FK POLPK,")
				//sb.Append("                        BKG.PORT_MST_POD_FK PODPK,JOB.JOB_CARD_TRN_PK JOB_CARD_PK")
				//sb.Append("                       ,CMT.CUSTOMER_MST_PK,CMT.CUSTOMER_NAME")
				//sb.Append("          FROM HBL_EXP_TBL                HBL,")
				//sb.Append("               JOB_CARD_TRN       JOB,")
				//sb.Append("               BOOKING_MST_TBL            BKG,")
				//sb.Append("               FREIGHT_CORRECTOR_HDR      FCH,")
				//sb.Append("               LOCATION_WORKING_PORTS_TRN,CUSTOMER_MST_TBL CMT")
				//sb.Append("         WHERE JOB.JOB_CARD_TRN_PK=HBL.NEW_JOB_CARD_SEA_EXP_FK")
				//sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK")
				//sb.Append("           AND FCH.HBL_FK = HBL.HBL_EXP_TBL_PK")
				//sb.Append("           AND CMT.CUSTOMER_MST_PK=JOB.SHIPPER_CUST_MST_FK ")
				//'Conditions
				//'Conditions
				//If BLPK <> 0 Then
				//    sb.Append("  AND HBL.HBL_EXP_TBL_PK =" & BLPK)
				//End If
				//If FCH_PK <> 0 Then
				//    sb.Append("  AND  FCH.FREIGHT_CORRECTOR_HDR_PK =" & FCH_PK)
				//End If
				//If FROMDATE.ToString.Trim.Length > 0 Then
				//    sb.Append("           AND HBL.HBL_DATE >= to_date('" & FROMDATE & "' ,'" & dateFormat & "')")
				//End If
				//If TODATE.ToString.Trim.Length > 0 Then
				//    sb.Append("           AND HBL.HBL_DATE <= to_date('" & TODATE & "' ,'" & dateFormat & "')")
				//End If
				//If CargoType > 0 Then
				//    sb.Append("   AND BKG.CARGO_TYPE = " & CargoType)
				//End If
				//'End
				//sb.Append("           AND LOCATION_WORKING_PORTS_TRN.LOCATION_MST_FK IN")
				//sb.Append("               (SELECT L.LOCATION_MST_PK")
				//sb.Append("                  FROM LOCATION_MST_TBL L")
				//sb.Append("                 START WITH L.LOCATION_MST_PK = " & HttpContext.Current.Session("LOGED_IN_LOC_FK"))
				//sb.Append("                CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)")

				sb.Append("  UNION ");

				sb.Append(" SELECT DISTINCT '' OPTFLAG,");
				sb.Append("                HBL.HAWB_EXP_TBL_PK BL_PK,");
				sb.Append("                HBL.HAWB_REF_NO BL_NO,");
				sb.Append("                FCH.VERSION,");
				sb.Append("                TO_DATE(HBL.HAWB_DATE, DATEFORMAT) BL_DATE,");
				sb.Append("                BKG.BOOKING_MST_PK BOOKING_PK,");
				sb.Append("                BKG.BOOKING_REF_NO BOOKING_NO,");
				sb.Append("                'AIR' BIZ_TYPE,");
				sb.Append("                'KGS/ULD' CARGO_TYPE,");
				sb.Append("                AM.AIRLINE_NAME VESSEL_ID,");
				sb.Append("                HBL.FLIGHT_NO VOYAGE_NO,");
				sb.Append("                CASE");
				sb.Append("                  WHEN FCH.STATUS = 1 THEN");
				sb.Append("                   'REQUESTED'");
				sb.Append("                  WHEN FCH.STATUS = 2 THEN");
				sb.Append("                   'APPROVED'");
				sb.Append("                  WHEN FCH.STATUS = 3 THEN");
				sb.Append("                   'REJECTED'");
				sb.Append("                END STATUS,");
				sb.Append("                FCH.FREIGHT_CORRECTOR_HDR_PK,");
				sb.Append("                BKG.PORT_MST_POL_FK POLPK,");
				sb.Append("                BKG.PORT_MST_POD_FK PODPK,");
				sb.Append("                JOB.JOB_CARD_TRN_PK JOB_CARD_PK,");
				sb.Append("                CMT.CUSTOMER_MST_PK,");
				sb.Append("                CMT.CUSTOMER_NAME");
				sb.Append("  FROM HAWB_EXP_TBL               HBL,");
				sb.Append("       JOB_CARD_TRN       JOB,");
				sb.Append("       BOOKING_MST_TBL            BKG,");
				sb.Append("       AIRLINE_MST_TBL            AM,");
				sb.Append("       FREIGHT_CORRECTOR_HDR      FCH,");
				sb.Append("       LOCATION_WORKING_PORTS_TRN LWPT,");
				sb.Append("       CUSTOMER_MST_TBL           CMT");
				sb.Append(" WHERE (HBL.HAWB_EXP_TBL_PK = JOB.HBL_HAWB_FK OR HBL.NEW_JOB_CARD_AIR_EXP_FK=JOB.JOB_CARD_TRN_PK)");
				sb.Append("   AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
				sb.Append("   AND AM.AIRLINE_MST_PK = BKG.CARRIER_MST_FK");
				sb.Append("   AND FCH.HAWB_FK = HBL.HAWB_EXP_TBL_PK");
				sb.Append("   AND CMT.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK");
				//Conditions
				if (BLPK != 0) {
					sb.Append("  AND HBL.HAWB_EXP_TBL_PK =" + BLPK);
				}
				if (FCH_PK != 0) {
					sb.Append("  AND  FCH.FREIGHT_CORRECTOR_HDR_PK =" + FCH_PK);
				}
				if (FROMDATE.ToString().Trim().Length > 0) {
					sb.Append("           AND HBL.HAWB_DATE >= to_date('" + FROMDATE + "' ,'" + dateFormat + "')");
				}
				if (TODATE.ToString().Trim().Length > 0) {
					sb.Append("           AND HBL.HAWB_DATE <= to_date('" + TODATE + "' ,'" + dateFormat + "')");
				}
				sb.Append("   AND LWPT.PORT_MST_FK=BKG.PORT_MST_POL_FK ");
				sb.Append("   AND LWPT.LOCATION_MST_FK IN");
				sb.Append("       (SELECT L.LOCATION_MST_PK");
				sb.Append("          FROM LOCATION_MST_TBL L");
				sb.Append("                 START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
				sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");

				//sb.Append("   UNION ")

				//sb.Append("  SELECT DISTINCT '' OPTFLAG,")
				//sb.Append("                HBL.HAWB_EXP_TBL_PK BL_PK,")
				//sb.Append("                HBL.HAWB_REF_NO BL_NO,")
				//sb.Append("                FCH.VERSION,")
				//sb.Append("                TO_CHAR(HBL.HAWB_DATE, DATEFORMAT) BL_DATE,")
				//sb.Append("                BKG.BOOKING_MST_PK BOOKING_PK,")
				//sb.Append("                BKG.BOOKING_REF_NO BOOKING_NO,")
				//sb.Append("                'Air' BIZ_TYPE,")
				//sb.Append("                'AIR' CARGO_TYPE,")
				//sb.Append("                AM.AIRLINE_NAME VESSEL_ID,")
				//sb.Append("                HBL.FLIGHT_NO VOYAGE_NO,")
				//sb.Append("                CASE")
				//sb.Append("                  WHEN FCH.STATUS = 1 THEN")
				//sb.Append("                   'REQUESTED'")
				//sb.Append("                  WHEN FCH.STATUS = 2 THEN")
				//sb.Append("                   'APPROVED'")
				//sb.Append("                  WHEN FCH.STATUS = 3 THEN")
				//sb.Append("                   'REJECTED'")
				//sb.Append("                END STATUS,")
				//sb.Append("                FCH.FREIGHT_CORRECTOR_HDR_PK,")
				//sb.Append("                BKG.PORT_MST_POL_FK POLPK,")
				//sb.Append("                BKG.PORT_MST_POD_FK PODPK,")
				//sb.Append("                JOB.JOB_CARD_TRN_PK JOB_CARD_PK,")
				//sb.Append("                CMT.CUSTOMER_MST_PK,")
				//sb.Append("                CMT.CUSTOMER_NAME")
				//sb.Append("  FROM HAWB_EXP_TBL               HBL,")
				//sb.Append("       JOB_CARD_TRN       JOB,")
				//sb.Append("       BOOKING_MST_TBL            BKG,")
				//sb.Append("       AIRLINE_MST_TBL            AM,")
				//sb.Append("       FREIGHT_CORRECTOR_HDR      FCH,")
				//sb.Append("       LOCATION_WORKING_PORTS_TRN,")
				//sb.Append("       CUSTOMER_MST_TBL           CMT")
				//sb.Append(" WHERE HBL.NEW_JOB_CARD_AIR_EXP_FK=JOB.JOB_CARD_TRN_PK ")
				//sb.Append("   AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK")
				//sb.Append("   AND AM.AIRLINE_MST_PK = BKG.CARRIER_MST_FK")
				//sb.Append("   AND FCH.HAWB_FK = HBL.HAWB_EXP_TBL_PK")
				//'Conditions
				//If BLPK <> 0 Then
				//    sb.Append("  AND HBL.HAWB_EXP_TBL_PK =" & BLPK)
				//End If
				//If FCH_PK <> 0 Then
				//    sb.Append("   AND FCH.FREIGHT_CORRECTOR_HDR_PK =" & FCH_PK)
				//End If
				//If FROMDATE.ToString.Trim.Length > 0 Then
				//    sb.Append("           AND HBL.HAWB_DATE >= to_date('" & FROMDATE & "' ,'" & dateFormat & "')")
				//End If
				//If TODATE.ToString.Trim.Length > 0 Then
				//    sb.Append("           AND HBL.HAWB_DATE <= to_date('" & TODATE & "' ,'" & dateFormat & "')")
				//End If
				//'End
				//sb.Append("   AND CMT.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK")
				//sb.Append("   AND LOCATION_WORKING_PORTS_TRN.LOCATION_MST_FK IN")
				//sb.Append("       (SELECT L.LOCATION_MST_PK")
				//sb.Append("          FROM LOCATION_MST_TBL L")
				//sb.Append("                 START WITH L.LOCATION_MST_PK = " & HttpContext.Current.Session("LOGED_IN_LOC_FK"))
				//sb.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)")

				sb.Append("   ) ORDER BY TO_DATE(BL_DATE,DATEFORMAT) DESC, BL_NO DESC)Q ");
			}
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        #endregion

        #region "Check The Bl Is Invoice or Not"
        /// <summary>
        /// Checks the bl invoice.
        /// </summary>
        /// <param name="JOBCARD_PK">The jobcar d_ pk.</param>
        /// <param name="creditnote">The creditnote.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public object CheckBlInvoice(int JOBCARD_PK = 0, int creditnote = 0, int BizType = 0)
		{
			DataSet dsData = new DataSet();
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			if (BizType == 2) {
				sb.Append("SELECT DISTINCT CON.CONSOL_INVOICE_PK");
				sb.Append("  FROM CONSOL_INVOICE_TBL     CON,");
				sb.Append("       CONSOL_INVOICE_TRN_TBL CIT,");
				sb.Append("       JOB_CARD_TRN   JOB");
				sb.Append(" WHERE CON.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
				sb.Append("   AND CIT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
				sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBCARD_PK);
				sb.Append("   AND CON.BUSINESS_TYPE = " + BizType);
			} else {
				sb.Append("SELECT DISTINCT CON.CONSOL_INVOICE_PK, JOB.JOBCARD_REF_NO");
				sb.Append("  FROM CONSOL_INVOICE_TBL     CON,");
				sb.Append("       CONSOL_INVOICE_TRN_TBL CIT,");
				sb.Append("       JOB_CARD_TRN   JOB");
				sb.Append(" WHERE CON.CONSOL_INVOICE_PK = CIT.CONSOL_INVOICE_FK");
				sb.Append("   AND CIT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
				sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JOBCARD_PK);
				sb.Append("   AND CON.BUSINESS_TYPE = " + BizType);
			}
			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Bljobcard deatils"
        /// <summary>
        /// Bls the job card details.
        /// </summary>
        /// <param name="BlFreightPk">The bl freight pk.</param>
        /// <param name="bookingblpk">The bookingblpk.</param>
        /// <returns></returns>
        public object BLJobCardDetails(string BlFreightPk = "", int bookingblpk = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet dsData = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("");
			sb.Append("SELECT QRY1.BOOKING_FREIGHT_PK,");
			sb.Append("       QRY1.CARGO_TYPE_ID,");
			sb.Append("       QRY1.COMMODITY_GROUP_CODE,");
			sb.Append("       QRY1.COMMODITY_NAME,");
			sb.Append("       QRY1.FREIGHT_ELEMENT_ID,");
			sb.Append("       QRY1.LOCAL_CHARGES,");
			sb.Append("       QRY1.BOOKING_CURRENCY_FK,");
			sb.Append("       QRY1.CURRENCY_ID,");
			sb.Append("       QRY1.BOOKING_FREIGHT,");
			sb.Append("       QRY1.TOTAL_FREIGHT,");
			sb.Append("       QRY1.VAT_CODE,");
			sb.Append("       QRY1.VAT_FK,");
			sb.Append("       QRY1.VAT_PERCENTAGE,");
			sb.Append("       QRY1.VAT_AMOUNT,");
			sb.Append("       QRY1.NET_AMT,");
			sb.Append("       QRY1.COLLECT_FLAG,");
			sb.Append("       QRY1.COLLECT_FLAG_DESC,");
			sb.Append("       '' SEL,");
			sb.Append("       ");
			sb.Append("       QRY1.INVOICE_LOCATION_FK,");
			sb.Append("       QRY1.LOCATION_ID,");
			sb.Append("       QRY1.INVOICE_CUSTOMER_FK,");
			sb.Append("       QRY1.CUSTOMER_ID,");
			sb.Append("       '' BTN,");
			sb.Append("       QRY1.INVOICED INVOICED,");
			sb.Append("       QRY1.JOB_CARD_FK,");
			sb.Append("       QRY1.INVOICE_VOYAGE_FK,");
			sb.Append("       QRY1.ROE,");
			sb.Append("       QRY1.INVOICE_AMOUNT,");
			sb.Append("       QRY1.INVOICE_CURENCY_FK,");
			sb.Append("       QRY1.LLIMIT,");
			sb.Append("       QRY1.ULIMIT,");
			sb.Append("       QRY1.FREIGHT_ELEMENT_MST_PK,");
			sb.Append("       QRY1.VERSION_NO, ");
			sb.Append("       'n' JC_Trn_Upd , ");
			sb.Append("       '0' invoice_pk, ");
			sb.Append("       '0' JC_frt_pk  ");

			sb.Append("  FROM (SELECT QRY.BOOKING_FREIGHT_PK,");
			sb.Append("               QRY.CARGO_TYPE_ID,");
			sb.Append("               QRY.COMMODITY_GROUP_CODE,");
			sb.Append("               QRY.COMMODITY_NAME,");
			sb.Append("               QRY.FREIGHT_ELEMENT_ID,");
			sb.Append("               QRY.LOCAL_CHARGES,");
			sb.Append("               QRY.BOOKING_CURRENCY_FK,");
			sb.Append("               QRY.CURRENCY_ID,");
			sb.Append("               QRY.BOOKING_FREIGHT,");
			sb.Append("               QRY.TOTAL_FREIGHT,");
			sb.Append("               QRY.VAT_CODE,");
			sb.Append("               QRY.VAT_FK,");
			sb.Append("               QRY.VAT_PERCENTAGE,");
			sb.Append("               QRY.VAT_AMOUNT,");
			sb.Append("               QRY.NET_AMT,");
			sb.Append("               QRY.COLLECT_FLAG,");
			sb.Append("               QRY.COLLECT_FLAG_DESC,");
			sb.Append("               '' SEL,");
			sb.Append("               ");
			sb.Append("               QRY.INVOICE_LOCATION_FK,");
			sb.Append("               QRY.LOCATION_ID,");
			sb.Append("               QRY.INVOICE_CUSTOMER_FK,");
			sb.Append("               QRY.CUSTOMER_ID,");
			sb.Append("               '' BTN,");
			sb.Append("               QRY.INVOICED INVOICED,");
			sb.Append("               QRY.JOB_CARD_FK,");
			sb.Append("               QRY.INVOICE_VOYAGE_FK,");
			sb.Append("               QRY.ROE,");
			sb.Append("               QRY.INVOICE_AMOUNT ,");
			sb.Append("               QRY.INVOICE_CURENCY_FK,");
			sb.Append("               QRY.LLIMIT,");
			sb.Append("               QRY.ULIMIT,");
			sb.Append("               QRY.FREIGHT_ELEMENT_MST_PK,");
			sb.Append("               QRY.VERSION_NO,");
			sb.Append("               PRINTING_PRIORITY");
			sb.Append("          FROM (SELECT distinct  bfct.FREIGHT_ELEMENT_MST_FK BOOKING_FREIGHT_PK,");
			sb.Append("                                CARGO_TYPE_MST_TBL.CARGO_TYPE_ID,");
			sb.Append("                                COMMODITY_GROUP_MST_TBL.COMMODITY_GROUP_CODE,");
			sb.Append("                                COMMODITY_MST_TBL.COMMODITY_NAME,");
			sb.Append("                                 FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_MST_PK FREIGHT_ELEMENT_ID, ");
			sb.Append("                                FREIGHT_ELEMENT_MST_TBL.LOCAL_CHARGES,");
			sb.Append("                                bfct.corrected_curr_fk BOOKING_CURRENCY_FK,");
			sb.Append("                                CURRENCY_TYPE_MST_TBL.CURRENCY_ID,");
			sb.Append("                                (bfct.corrected_freight-bl.bl_freight) BOOKING_FREIGHT,");
			sb.Append("                                (SELECT VAT_CODE");
			sb.Append("                                   FROM COUNTRY_VAT_CODE_TRN CVT");
			sb.Append("                                  WHERE CVT.COUNTRY_VAT_CODE_TRN_PK =");
			sb.Append("                                        JC_FREIGHT_TRN.COUNTRY_VAT_CODE_FK) VAT_CODE,");
			sb.Append("                                JC_FREIGHT_TRN.COUNTRY_VAT_CODE_FK VAT_FK,");
			sb.Append("                                JC_FREIGHT_TRN.VAT_PERCENTAGE,");
			sb.Append("                                (((NVL(bfct.total_corrected_freight-(bl.total_freight), 0)) *");
			sb.Append("                                NVL(JC_FREIGHT_TRN.VAT_PERCENTAGE, 0)) / 100) VAT_AMOUNT,");
			sb.Append("                                (NVL(bfct.total_corrected_freight-(bl.total_freight), 0) +");
			sb.Append("                                ((NVL(bfct.total_corrected_freight-(bl.total_freight), 0) *");
			sb.Append("                                NVL(JC_FREIGHT_TRN.VAT_PERCENTAGE, 0)) / 100)) NET_AMT,");
			sb.Append("                                (bfct.total_corrected_freight-(bl.total_freight)) TOTAL_FREIGHT,");
			sb.Append("                                bfct.COLLECT_FLAG,");
			sb.Append("                                Decode(bfct.COLLECT_FLAG,");
			sb.Append("                                       1,");
			sb.Append("                                       'Prepaid',");
			sb.Append("                                       2,");
			sb.Append("                                       'Collect',");
			sb.Append("                                       'Foreign') COLLECT_FLAG_DESC,");
			sb.Append("                                bfct.INVOICE_LOCATION_FK,");
			sb.Append("                                LOCATION_MST_TBL.LOCATION_ID,");
			sb.Append("                                bfct.INVOICE_CUSTOMER_FK,");
			sb.Append("                                CUSTOMER_MST_TBL.CUSTOMER_ID,");
			sb.Append("                                  Decode(bfct.COLLECT_FLAG,");
			sb.Append("                                       1,");
			sb.Append("                                       '1',");
			sb.Append("                                       2,");
			sb.Append("                                       '2',");
			sb.Append("                                       '3') INVOICED,");
			sb.Append("                               ");
			sb.Append("                                JC_FREIGHT_TRN.jc_freight_pk JOB_CARD_FK,");
			sb.Append("                                '' INVOICE_VOYAGE_FK,");
			sb.Append("                                  NVL(JC_FREIGHT_TRN.ROE,0) Roe ,");
			sb.Append("              (JC_FREIGHT_TRN.ROE*(bfct.total_corrected_freight-(bl.total_freight))) INVOICE_AMOUNT,");
			sb.Append("                                bfct.corrected_curr_fk INVOICE_CURENCY_FK,");
			sb.Append("                                '' ULIMIT,");
			sb.Append("                                '' LLIMIT,");
			sb.Append("                                FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_MST_PK,");
			sb.Append("                                NVL(JC_FREIGHT_TRN.VERSION_NO,0) VERSION_NO,  ");
			sb.Append("                                '' SEL,");
			sb.Append("                                FREIGHT_ELEMENT_MST_TBL.PRINTING_PRIORITY from CARGO_TYPE_MST_TBL,");
			sb.Append("       COMMODITY_GROUP_MST_TBL,");
			sb.Append("       COMMODITY_MST_TBL,");
			sb.Append("       CURRENCY_TYPE_MST_TBL,");
			sb.Append("       LOCATION_MST_TBL,");
			sb.Append("       CUSTOMER_MST_TBL,");
			sb.Append("       FREIGHT_ELEMENT_MST_TBL,");
			sb.Append("       bl_freight_trn bl,");
			sb.Append("       bl_freight_crctr_trn bfct,");
			sb.Append("       JC_FREIGHT_TRN");
			sb.Append("");
			sb.Append(" where bl.bl_freight_pk = bfct.bl_freight_trn_fk");
			sb.Append("   AND FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_MST_PK =");
			sb.Append("       BL.FREIGHT_ELEMENT_MST_FK");
			sb.Append("   AND JC_FREIGHT_TRN.BL_FREIGHT_FK = bfct.bl_freight_trn_fk");
			sb.Append("    AND COMMODITY_MST_TBL.COMMODITY_MST_PK = BL.COMMODITY_MST_FK");
			sb.Append("   AND COMMODITY_GROUP_MST_TBL.COMMODITY_GROUP_PK =");
			sb.Append("       BL.COMMODITY_GROUP_MST_FK");
			sb.Append("   AND CARGO_TYPE_MST_TBL.CARGO_TYPE_MST_PK = BL.CARGO_TYPE_MST_FK");
			sb.Append("      ");
			sb.Append("   and CURRENCY_TYPE_MST_TBL.CURRENCY_MST_PK = BFCT.CORRECTED_CURR_FK");
			sb.Append("   and LOCATION_MST_TBL.LOCATION_MST_PK = BFCT.INVOICE_LOCATION_FK");
			sb.Append("   AND CUSTOMER_MST_TBL.CUSTOMER_MST_PK = BFCT.INVOICE_CUSTOMER_FK");
			sb.Append("   AND FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_MST_PK =");
			sb.Append("       BFCT.FREIGHT_ELEMENT_MST_FK");
			sb.Append("  AND bfct.bl_freight_crctr_trn_pk IN ( " + BlFreightPk + "");
			sb.Append("   )  ");
			sb.Append("   AND bfct.collect_flag = 1");
			sb.Append("");
			sb.Append(" ORDER BY FREIGHT_ELEMENT_MST_TBL.PRINTING_PRIORITY) QRY");
			sb.Append("         order by PRINTING_PRIORITY) QRY1");

			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        #endregion

        #region "Location Currency"
        /// <summary>
        /// Feches the currency.
        /// </summary>
        /// <param name="locationPk">The location pk.</param>
        /// <returns></returns>
        public object FechCurrency(int locationPk = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet dsData = new DataSet();
			WorkFlow objWF = new WorkFlow();
			sb.Append("      select distinct ctt.currency_mst_pk ");
			sb.Append("         from location_mst_tbl      lmt,");
			sb.Append("              country_mst_tbl       cmt,");
			sb.Append("              currency_type_mst_tbl ctt");
			sb.Append("        where lmt.country_mst_fk = cmt.country_mst_pk");
			sb.Append("          and ctt.currency_mst_pk = cmt.currency_mst_fk");
			sb.Append("   AND lmt.location_mst_pk = " + locationPk + "");

			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

        #endregion

        #region "To Check the Customer detail for BL"
        /// <summary>
        /// Bls the customer detail.
        /// </summary>
        /// <param name="FCNPK">The FCNPK.</param>
        /// <returns></returns>
        public object BLCustomerDetail(int FCNPK = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet dsData = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("");
			sb.Append("SELECT CMT.CUSTOMER_MST_PK,CMT.CUSTOMER_ID");
			sb.Append("  FROM CUSTOMER_MST_TBL      CMT,");
			sb.Append("       FREIGHT_CORRECTOR_HDR FCN,");
			sb.Append("       BOOKING_BL_TRN        BBT,");
			sb.Append("       BOOKING_TRN           BT");
			sb.Append("       ");
			sb.Append("       WHERE CMT.CUSTOMER_MST_PK=BT.CUSTOMER_MST_FK");
			sb.Append("       AND BBT.BOOKING_BL_PK=FCN.BOOKING_BL_FK");
			sb.Append("       AND BBT.BOOKING_TRN_FK=BT.BOOKING_TRN_PK");
			sb.Append("     AND FCN.FREIGHT_CORRECTOR_HDR_PK= " + FCNPK + "");

			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Credit Detail For BL"
        /// <summary>
        /// Credits the detail bl.
        /// </summary>
        /// <param name="FCNPK">The FCNPK.</param>
        /// <param name="BL_FREIGHT_CRCTR_TRN_PK">The b l_ freigh t_ CRCT r_ tr n_ pk.</param>
        /// <param name="currency">The currency.</param>
        /// <returns></returns>
        public object CreditDetailBl(int FCNPK = 0, string BL_FREIGHT_CRCTR_TRN_PK = "", int currency = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet dsData = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT DISTINCT INVOICE_TRN.INVOICE_TRN_PK,");
			sb.Append("                INVOICE_TRN.INVOICE_NO,");
			sb.Append("                FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_MST_PK,");
			sb.Append("                FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_ID,");
			sb.Append("                INVOICE_TRN.CURRENCY_MST_FK,");
			sb.Append("                CURRENCY_TYPE_MST_TBL.CURRENCY_ID,");
			sb.Append("                (nvl(BL_FREIGHT_CRCTR_TRN.Total_Corrected_Freight, 0) +");
			sb.Append("               (nvl(INVOICE_FREIGHT_TRN.Vat_Percentage, 0)*BL_FREIGHT_CRCTR_TRN.Total_Corrected_Freight/100) ) *");
			sb.Append("                INVOICE_FREIGHT_TRN.Roe Inv_Amount,");
			sb.Append("                ");
			sb.Append("                                convertcurrency(INVOICE_TRN.CURRENCY_MST_FK,");
			sb.Append("                                '" + currency + "',");
			sb.Append("                                (nvl(BL_FREIGHT_CRCTR_TRN.Total_Corrected_Freight,");
			sb.Append("                                     0) +");
			sb.Append("                                 (nvl(INVOICE_FREIGHT_TRN.Vat_Percentage, 0)*BL_FREIGHT_CRCTR_TRN.Total_Corrected_Freight/100)) *");
			sb.Append("                                INVOICE_FREIGHT_TRN.Roe) Loc_Amount,");
			sb.Append("                (select sum(b.cr_note_frt_amt)");
			sb.Append("                   from cr_note_dtl_tbl b");
			sb.Append("                  where b.invoice_freight_fk =");
			sb.Append("                        INVOICE_FREIGHT_TRN.Invoice_Freight_Pk) Prev_CrAmount,");
			sb.Append("                null Current_CrAmount,");
			sb.Append("                '' Sel,");
			sb.Append("                null BkFrtFK,");
			sb.Append("                null BkOthFrtFK,");
			sb.Append("                '0' version_no,");
			sb.Append("                null JC_FREIGHT_FK,");
			sb.Append("                null JC_OTHER_FREIGHT_FK,");
			sb.Append("                INVOICE_FREIGHT_TRN.Invoice_Freight_Pk INVOICE_FREIGHT_FK,");
			sb.Append("                null INVOICE_OTHER_FREIGHT_FK,");
			sb.Append("                null BL_FREIGHT_FK,");
			sb.Append("                null BL_OTHER_FREIGHT_FK,");
			sb.Append("                BL_FREIGHT_CRCTR_TRN.BL_FREIGHT_CRCTR_TRN_PK");
			sb.Append("  FROM JOB_CARD_TRN,");
			sb.Append("       INVOICE_JOB_CARD_TRN,");
			sb.Append("       CURRENCY_TYPE_MST_TBL,");
			sb.Append("       FREIGHT_ELEMENT_MST_TBL,");
			sb.Append("       BOOKING_BL_TRN,");
			sb.Append("       FREIGHT_CORRECTOR_HDR,");
			sb.Append("       BL_FREIGHT_CRCTR_TRN,");
			sb.Append("       INVOICE_TRN,");
			sb.Append("       INVOICE_FREIGHT_TRN");
			sb.Append(" WHERE JOB_CARD_TRN.JOB_CARD_PK = INVOICE_JOB_CARD_TRN.JOB_CARD_FK");
			sb.Append("   AND BOOKING_BL_TRN.BOOKING_BL_PK = FREIGHT_CORRECTOR_HDR.BOOKING_BL_FK");
			sb.Append("   AND FREIGHT_CORRECTOR_HDR.FREIGHT_CORRECTOR_HDR_PK =");
			sb.Append("       BL_FREIGHT_CRCTR_TRN.FREIGHT_CORRECTOR_FK");
			sb.Append("   AND JOB_CARD_TRN.BOOKING_BL_FK = FREIGHT_CORRECTOR_HDR.BOOKING_BL_FK");
			sb.Append("   AND JOB_CARD_TRN.JOB_CARD_PK = INVOICE_JOB_CARD_TRN.JOB_CARD_FK");
			sb.Append("   AND INVOICE_TRN.INVOICE_TRN_PK = INVOICE_JOB_CARD_TRN.INVOICE_TRN_FK");
			sb.Append("   AND FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_MST_PK =");
			sb.Append("       BL_FREIGHT_CRCTR_TRN.FREIGHT_ELEMENT_MST_FK");
			sb.Append("   AND INVOICE_TRN.CURRENCY_MST_FK = CURRENCY_TYPE_MST_TBL.CURRENCY_MST_PK");
			sb.Append("   AND INVOICE_FREIGHT_TRN.INVOICE_TRN_FK = INVOICE_TRN.INVOICE_TRN_PK");
			sb.Append("   AND INVOICE_FREIGHT_TRN.FREIGHT_ELEMENT_MST_FK =");
			sb.Append("       FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_MST_PK");
			sb.Append("    AND FREIGHT_CORRECTOR_HDR.FREIGHT_CORRECTOR_HDR_PK = " + FCNPK + "");
			sb.Append("  AND BL_FREIGHT_CRCTR_TRN.BL_FREIGHT_CRCTR_TRN_PK IN ( " + BL_FREIGHT_CRCTR_TRN_PK + "");
			sb.Append("   ) ");
			sb.Append("   AND INVOICE_TRN.INVOICE_TYPE = 0");
			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Pod Location Details For B/l "
        /// <summary>
        /// Pods the location detail.
        /// </summary>
        /// <param name="PODpk">The po DPK.</param>
        /// <returns></returns>
        public object PodLocationDetail(int PODpk = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			DataSet dsData = new DataSet();
			sb.Append("SELECT UMT.USER_MST_PK,");
			sb.Append("       UMT.USER_NAME,");
			sb.Append("       UMT.DEFAULT_LOCATION_FK,");
			sb.Append("       LMT.LOCATION_ID,");
			sb.Append("       LMT.LOCATION_NAME");
			sb.Append("  FROM USER_MST_TBL UMT, LOCATION_MST_TBL LMT");
			sb.Append("");
			sb.Append(" WHERE UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
			sb.Append("      ");
			sb.Append("   AND LMT.LOCATION_MST_PK IN");
			sb.Append("       (SELECT LPT.LOCATION_MST_FK");
			sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LPT,");
			sb.Append("               PORT_MST_TBL PMT WHERE   LPT.PORT_MST_FK = PMT.PORT_MST_PK");
			sb.Append("               ");
			sb.Append("               ");
			sb.Append("    AND PMT.PORT_MST_PK = " + PODpk + "");
			sb.Append("  ) ");
			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fech Data For Fill Control"
        /// <summary>
        /// Feches the fill control.
        /// </summary>
        /// <param name="BLpk">The b LPK.</param>
        /// <param name="bookingpk">The bookingpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public object FechFillControl(int BLpk = 0, int bookingpk = 0, int BizType = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet dsData = new DataSet();
			WorkFlow objWF = new WorkFlow();
			if (BizType == 2) {
				sb.Append("SELECT DISTINCT HBL.HBL_EXP_TBL_PK AS \"HIDDEN1\",");
				sb.Append("                HBL.HBL_REF_NO AS \"BL No\",");
				sb.Append("                TO_CHAR(HBL.HBL_DATE, 'dd/MM/yyyy') \"BL_DATE\",");
				sb.Append("                BKG.BOOKING_REF_NO AS \"BookingID\",");
				sb.Append("                BKG.BOOKING_MST_PK AS \"Hidden2\",");
				sb.Append("                0 AS \"HIDDEN3\",");
				sb.Append("                HBL.VESSEL_NAME || '-' || HBL.VOYAGE AS \"Vsl / Voy\",");
				sb.Append("                NVL(CON.CUSTOMER_MST_PK, 0) AS \"Hidden4\",");
				sb.Append("                NVL(CON.CUSTOMER_NAME, ' ') AS \"COSGINEE NAME\",");
				sb.Append("                SHP.CUSTOMER_MST_PK AS \"Hidden5\",");
				sb.Append("                SHP.CUSTOMER_NAME AS \"SHIPPER NAME\",");
				sb.Append("                POL.PORT_ID \"POL\",");
				sb.Append("                POL.PORT_NAME \"Hidden6\",");
				sb.Append("                POL.PORT_MST_PK AS \"Hidden7\",");
				sb.Append("                POD.PORT_MST_PK AS \"Hidden8\",");
				sb.Append("                POD.PORT_ID \"Hidden9\",");
				sb.Append("                POD.PORT_NAME \"Hidden10\",");
				sb.Append("                POO.PORT_MST_PK AS \"Hidden11\",");
				sb.Append("                POO.PORT_ID \"Hidden12\",");
				sb.Append("                POO.PORT_NAME \"Hidden13\",");
				sb.Append("                PFD.PORT_MST_PK AS \"Hidden14\",");
				sb.Append("                PFD.PORT_ID \"Hidden15\",");
				sb.Append("                PFD.PORT_NAME \"Hidden16\",");
				sb.Append("                BKG.CARGO_TYPE,JC.COMMODITY_GROUP_FK ");
				sb.Append("  FROM JOB_CARD_TRN JC,");
				sb.Append("       HBL_EXP_TBL          HBL,");
				sb.Append("       BOOKING_MST_TBL      BKG,");
				sb.Append("       PORT_MST_TBL         POL,");
				sb.Append("       PORT_MST_TBL         POD,");
				sb.Append("       PORT_MST_TBL         POO,");
				sb.Append("       PORT_MST_TBL         PFD,");
				sb.Append("       CUSTOMER_MST_TBL     SHP,");
				sb.Append("       CUSTOMER_MST_TBL     CON");
				sb.Append(" WHERE JC.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
				sb.Append("   AND BKG.BOOKING_MST_PK = JC.BOOKING_MST_FK");
				sb.Append("   AND POL.PORT_MST_PK = BKG.PORT_MST_POL_FK");
				sb.Append("   AND POD.PORT_MST_PK = BKG.PORT_MST_POD_FK");
				sb.Append("   AND POO.PORT_MST_PK(+) = BKG.POO_FK");
				sb.Append("   AND PFD.PORT_MST_PK(+) = BKG.PFD_FK");
				sb.Append("   AND JC.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK");
				sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CON.CUSTOMER_MST_PK");
				sb.Append("   AND HBL.HBL_EXP_TBL_PK = " + BLpk);
			} else {
				sb.Append("SELECT DISTINCT HBL.HAWB_EXP_TBL_PK AS \"HIDDEN1\",");
				sb.Append("                HBL.HAWB_REF_NO AS \"BL No\",");
				sb.Append("                TO_CHAR(HBL.HAWB_DATE, 'dd/MM/yyyy') \"BL_DATE\",");
				sb.Append("                BKG.BOOKING_REF_NO AS \"BookingID\",");
				sb.Append("                BKG.BOOKING_MST_PK AS \"Hidden2\",");
				sb.Append("                0 AS \"HIDDEN3\",");
				sb.Append("                AM.AIRLINE_ID || '-' || HBL.FLIGHT_NO AS \"Vsl / Voy\",");
				sb.Append("                NVL(CON.CUSTOMER_MST_PK, 0) AS \"Hidden4\",");
				sb.Append("                NVL(CON.CUSTOMER_NAME, ' ') AS \"COSGINEE NAME\",");
				sb.Append("                SHP.CUSTOMER_MST_PK AS \"Hidden5\",");
				sb.Append("                SHP.CUSTOMER_NAME AS \"SHIPPER NAME\",");
				sb.Append("                POL.PORT_ID \"POL\",");
				sb.Append("                POL.PORT_NAME \"Hidden6\",");
				sb.Append("                POL.PORT_MST_PK AS \"Hidden7\",");
				sb.Append("                POD.PORT_MST_PK AS \"Hidden8\",");
				sb.Append("                POD.PORT_ID \"Hidden9\",");
				sb.Append("                POD.PORT_NAME \"Hidden10\",");
				sb.Append("                POO.PLACE_PK AS \"Hidden11\",");
				sb.Append("                POO.PLACE_CODE \"Hidden12\",");
				sb.Append("                POO.PLACE_NAME \"Hidden13\",");
				sb.Append("                PFD.PLACE_PK AS \"Hidden14\",");
				sb.Append("                PFD.PLACE_CODE \"Hidden15\",");
				sb.Append("                PFD.PLACE_NAME \"Hidden16\",");
				sb.Append("                BKG.CARGO_TYPE,");
				sb.Append("                JC.COMMODITY_GROUP_FK");
				sb.Append("  FROM JOB_CARD_TRN JC,");
				sb.Append("       HAWB_EXP_TBL          HBL,");
				sb.Append("       BOOKING_MST_TBL      BKG,");
				sb.Append("       AIRLINE_MST_TBL AM,");
				sb.Append("       PORT_MST_TBL         POL,");
				sb.Append("       PORT_MST_TBL         POD,");
				sb.Append("       PLACE_MST_TBL        POO,");
				sb.Append("       PLACE_MST_TBL        PFD,");
				sb.Append("       CUSTOMER_MST_TBL     SHP,");
				sb.Append("       CUSTOMER_MST_TBL     CON");
				sb.Append(" WHERE JC.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK");
				sb.Append("   AND BKG.BOOKING_MST_PK = JC.BOOKING_MST_FK");
				sb.Append("   AND AM.AIRLINE_MST_PK=BKG.CARRIER_MST_FK");
				sb.Append("   AND POL.PORT_MST_PK = BKG.PORT_MST_POL_FK");
				sb.Append("   AND POD.PORT_MST_PK = BKG.PORT_MST_POD_FK");
				sb.Append("   AND POO.PLACE_PK(+) = BKG.COL_PLACE_MST_FK");
				sb.Append("   AND PFD.PLACE_PK(+) = BKG.DEL_PLACE_MST_FK");
				sb.Append("   AND JC.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK");
				sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CON.CUSTOMER_MST_PK");
				sb.Append("   AND HBL.HAWB_EXP_TBL_PK = " + BLpk);
			}
			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

        #endregion

        #region "Select InVoice Nr"
        /// <summary>
        /// Feches the invoice nr.
        /// </summary>
        /// <param name="jobcard">The jobcard.</param>
        /// <returns></returns>
        public object FechInvoiceNr(int jobcard = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet dsData = new DataSet();
			WorkFlow objWF = new WorkFlow();
			sb.Append("select it.invoice_no");
			sb.Append("  from invoice_trn IT, invoice_job_card_trn ijct, job_card_trn jc");
			sb.Append("");
			sb.Append(" where it.invoice_trn_pk = ijct.invoice_trn_fk");
			sb.Append("      ");
			sb.Append("   AND ijct.job_card_fk = jc.job_card_pk");
			sb.Append("        ");
			sb.Append("   AND IT.Invoice_Type = 0");
			sb.Append("   AND it.consolidated_inv_trn_fk is null");
			sb.Append("   AND jc.job_card_pk  =   " + jobcard + "");

			try {
				dsData = objWF.GetDataSet(sb.ToString());
				return dsData;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "FCNRetroFeedBl"
        /// <summary>
        /// FCNs the retro feed bl.
        /// </summary>
        /// <param name="Freight_correct_pk">The freight_correct_pk.</param>
        /// <returns></returns>
        public ArrayList FCNRetroFeedBl(int Freight_correct_pk = 0)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand insCommand = new OracleCommand();
			OracleTransaction insertTrans = null;
			objWK.OpenConnection();
			insertTrans = objWK.MyConnection.BeginTransaction();
			try {
				var _with12 = insCommand;
				_with12.Connection = objWK.MyConnection;
				_with12.CommandType = CommandType.StoredProcedure;
				_with12.CommandText = objWK.MyUserName + ".bl_corrected_freight_pkg.retro_feed_bl_corrected";
				_with12.Parameters.Clear();
				var _with13 = _with12.Parameters;
				_with13.Add("freight_corrector_hdr_In", Freight_correct_pk).Direction = ParameterDirection.Input;
				_with13.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with14 = objWK.MyDataAdapter;
				_with14.InsertCommand = insCommand;
				_with14.InsertCommand.Transaction = insertTrans;
				_with14.InsertCommand.ExecuteNonQuery();
				if (arrMessage.Count > 0) {
					insertTrans.Rollback();
					return arrMessage;
				} else {
					insertTrans.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				insertTrans.Rollback();
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				insertTrans.Rollback();
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
		}
        #endregion

        #region "CheckFreightCorrector"
        /// <summary>
        /// Checks the freight corrector.
        /// </summary>
        /// <returns></returns>
        public DataSet CheckFreightCorrector()
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();

			try {
				strSQL = " SELECT ROWTOCOL('SELECT FET.FREIGHT_ELEMENT_ID FROM FREIGHT_ELEMENT_MST_TBL FET WHERE FET.CHARGE_BASIS = 2') AS FREIGHT_ELEMENT_ID FROM DUAL " ;
				return objWK.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Booking Location"
        /// <summary>
        /// Fetches the booking location.
        /// </summary>
        /// <param name="Booking_ID">The booking_ identifier.</param>
        /// <returns></returns>
        public DataSet FetchBookingLocation(string Booking_ID)
		{
			WorkFlow objWF = new WorkFlow();
			string strSql = null;

			strSql = string.Empty ;
			if (!string.IsNullOrEmpty(Booking_ID)) {
				strSql = " SELECT * FROM BOOKING_TRN BT WHERE BT.BOOKING_ID = '" + Booking_ID + "'";
			} else {
				strSql = " SELECT * FROM BOOKING_TRN BT WHERE BT.BOOKING_ID = '0' ";
			}
			try {
				return objWF.GetDataSet(strSql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Document PK  For Sending Mail"
        /// <summary>
        /// Fetches the document.
        /// </summary>
        /// <param name="documentId">The document identifier.</param>
        /// <returns></returns>
        public DataTable FetchDocument(string documentId)
		{
			System.Text.StringBuilder strbldrSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();

			strbldrSQL.Append(" SELECT DMT.DOCUMENT_MST_PK ");
			strbldrSQL.Append(" FROM DOCUMENT_MST_TBL DMT, ");
			strbldrSQL.Append(" DOCUMENT_NAME_MST_TBL DN ");
			strbldrSQL.Append(" WHERE");
			strbldrSQL.Append(" DN.DOCUMENT_NAME='" + documentId + "'");
			strbldrSQL.Append(" AND DN.DOCUMENT_NAME_MST_PK = DMT.DOCUMENT_NAME_MST_FK ");
			try {
				return objWF.GetDataTable(strbldrSQL.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception exp) {
				throw exp;
			}
		}
        #endregion

        #region "Invoice Header Data"
        /// <summary>
        /// Fetches the inv header.
        /// </summary>
        /// <param name="FrtCrctTrnPk">The FRT CRCT TRN pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchInvHeader(string FrtCrctTrnPk, int BizType)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet DSInv = new DataSet();
			if (BizType == 2) {
				sb.Append("SELECT DISTINCT 1 PROCESS_TYPE_IN,");
				sb.Append("                2 BUSINESS_TYPE_IN,");
				sb.Append("                FCT.INVOICE_CUSTOMER_FK CUSTOMER_MST_FK_IN,");
				sb.Append("                SYSDATE INVOICE_DATE_IN,");
				sb.Append("                NULL INVOICE_DUE_DATE_IN,");
				sb.Append("                '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' CURRENCY_MST_FK_IN,");
				sb.Append("                (select b.bank_mst_pk from bank_mst_tbl b where b.invoice=1 ");
				sb.Append("                and b.location_mst_fk=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ") BANK_MST_FK_IN,");
				sb.Append("                NVL(SUM((((SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + "," + BizType + ", 1)");
				sb.Append("                                          FROM DUAL),");
				sb.Append("                                        FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                                        '" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + "',");
				sb.Append("                                        FCT.PMT_TYPE,");
				sb.Append("                                        FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                                        2)");
				sb.Append("                         FROM DUAL) *");
				sb.Append("                    ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("                    FCT.EXCHANGE_RATE) / 100) +");
				sb.Append("                    (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("                    FCT.EXCHANGE_RATE)),0) INVOICE_AMT_IN,");
				sb.Append("                ");
				sb.Append("                0 DISCOUNT_AMT_IN,");
				sb.Append("                ");
				sb.Append("                NVL(SUM((((SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1)");
				sb.Append("                                          FROM DUAL),");
				sb.Append("                                        FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                                        " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                                        FCT.PMT_TYPE,");
				sb.Append("                                        FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                                        2)");
				sb.Append("                         FROM DUAL) *");
				sb.Append("                    ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("                    FCT.EXCHANGE_RATE) / 100) +");
				sb.Append("                    (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("                    FCT.EXCHANGE_RATE)),0) NET_RECEIVABLE_IN,");
				sb.Append("                'Supplementry Invoice' REMARKS_IN, '' TDS_REMARKS_IN, 0 SUPPLIER_MST_FK_IN, 0 IS_FAC_INV, 0 AIF_IN, ");
				sb.Append("                1 EXCH_RATE_TYPE_FK_IN,JOB.CREATED_BY_FK CREATED_BY_FK_IN");
				sb.Append("  FROM JOB_CARD_TRN    JOB,");
				sb.Append("       HBL_EXP_TBL             HBL,");
				sb.Append("       JOB_TRN_FD      JF,");
				sb.Append("       FREIGHT_CRCTR_TRN       FCT,");
				sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
				sb.Append("       COMMODITY_MST_TBL       CMT,");
				sb.Append("       CURRENCY_TYPE_MST_TBL   CUR,");
				sb.Append("       PARAMETERS_TBL          PAR");
				sb.Append(" WHERE JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
				sb.Append("   AND FCT.JOB_TRN_SEA_EXP_FD_FK = JF.JOB_TRN_FD_PK");
				sb.Append("   AND JOB.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
				sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = JF.FREIGHT_ELEMENT_MST_FK");
				sb.Append("   AND FCT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
				sb.Append("   AND CUR.CURRENCY_MST_PK = FCT.CORRECTED_CURR_FK");
				sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
				sb.Append("   AND FCT.FREIGHT_CRCTR_TRN_PK IN(" + FrtCrctTrnPk + ") ");
				sb.Append(" GROUP BY FCT.INVOICE_CUSTOMER_FK,JOB.CREATED_BY_FK");
			//'AIR
			} else {
				sb.Append("SELECT DISTINCT 1 PROCESS_TYPE_IN,");
				sb.Append("                1 BUSINESS_TYPE_IN,");
				sb.Append("                FCT.INVOICE_CUSTOMER_FK CUSTOMER_MST_FK_IN,");
				sb.Append("                SYSDATE INVOICE_DATE_IN,");
				sb.Append("                NULL INVOICE_DUE_DATE_IN,");
				sb.Append("                " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " CURRENCY_MST_FK_IN,");
				sb.Append("                (select b.bank_mst_pk from bank_mst_tbl b where b.invoice=1 ");
				sb.Append("                and b.location_mst_fk=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ") BANK_MST_FK_IN,");
				sb.Append("                NVL(SUM((((SELECT FETCH_VAT((SELECT FETCH_EU(" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1)");
				sb.Append("                                          FROM DUAL),");
				sb.Append("                                        FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                                        " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                                        FCT.PMT_TYPE,");
				sb.Append("                                        FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                                        2)");
				sb.Append("                         FROM DUAL) *");
				sb.Append("                    ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("                    FCT.EXCHANGE_RATE) / 100) +");
				sb.Append("                    (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("                    FCT.EXCHANGE_RATE)),0) INVOICE_AMT_IN,");
				sb.Append("                0 DISCOUNT_AMT_IN,");
				sb.Append("               NVL( SUM((((SELECT FETCH_VAT((SELECT FETCH_EU(" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1)");
				sb.Append("                                          FROM DUAL),");
				sb.Append("                                        FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                                         " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                                        FCT.PMT_TYPE,");
				sb.Append("                                        FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                                        2)");
				sb.Append("                         FROM DUAL) *");
				sb.Append("                    ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("                    FCT.EXCHANGE_RATE) / 100) +");
				sb.Append("                    (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("                    FCT.EXCHANGE_RATE)),0) NET_RECEIVABLE_IN,");
				sb.Append("                'Supplementry Invoice' REMARKS_IN, '' TDS_REMARKS_IN, 0 SUPPLIER_MST_FK_IN, 0 IS_FAC_INV, 0 AIF_IN, ");
				sb.Append("                1 EXCH_RATE_TYPE_FK_IN, ");
				sb.Append("                JOB.CREATED_BY_FK CREATED_BY_FK_IN");
				sb.Append("  FROM JOB_CARD_TRN    JOB,");
				sb.Append("       HAWB_EXP_TBL             HBL,");
				sb.Append("       JOB_TRN_FD      JF,");
				sb.Append("       FREIGHT_CRCTR_TRN       FCT,");
				sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
				sb.Append("       COMMODITY_MST_TBL       CMT,");
				sb.Append("       CURRENCY_TYPE_MST_TBL   CUR,");
				sb.Append("       PARAMETERS_TBL          PAR");
				sb.Append(" WHERE JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK");
				sb.Append("   AND FCT.JOB_TRN_AIR_EXP_FD_FK = JF.JOB_TRN_FD_PK");
				sb.Append("   AND JOB.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
				sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = JF.FREIGHT_ELEMENT_MST_FK");
				sb.Append("   AND FCT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
				sb.Append("   AND CUR.CURRENCY_MST_PK = FCT.CORRECTED_CURR_FK");
				sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
				sb.Append("   AND FCT.FREIGHT_CRCTR_TRN_PK IN(" + FrtCrctTrnPk + ") ");
				sb.Append(" GROUP BY FCT.INVOICE_CUSTOMER_FK, JOB.CREATED_BY_FK ");
			}
			DSInv = objWF.GetDataSet(sb.ToString());
			DSInv.Tables.Add(objWF.GetDataTable(FetchInvDtls(FrtCrctTrnPk, BizType)));
			try {
				return DSInv;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception exp) {
				throw exp;
			}
		}
        #endregion

        #region "Credit Note Data"
        /// <summary>
        /// Fetches the CRN header.
        /// </summary>
        /// <param name="FrtCrctTrnPk">The FRT CRCT TRN pk.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchCRNHeader(string FrtCrctTrnPk, int JOBPK, int BizType)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet DSInv = new DataSet();
			sb.Append("SELECT DISTINCT 1 PROCESS_TYPE,");
			sb.Append("                " + BizType + " BIZ_TYPE,");
			sb.Append("                1 CREDIT_NOTE_TYPE,");
			sb.Append("                SYSDATE CREDIT_NOTE_DATE,");
			sb.Append("                CM.CURRENCY_MST_FK,");
			sb.Append("                CM.INVOICE_AMT CRN_AMMOUNT,");
			sb.Append("                JS.SHIPPER_CUST_MST_FK CUSTOMER_MST_FK,");
			sb.Append("                1 DOCUMENT_TYPE,NULL DOCUMENT_REFRENCE,");
			sb.Append("                '' REMARKS,");
			sb.Append("                0 VERSION_NO");
			sb.Append("  FROM CONSOL_INVOICE_TBL     CM,");
			sb.Append("       CONSOL_INVOICE_TRN_TBL CT,");
			sb.Append("       JOB_CARD_TRN   JS");
			sb.Append(" WHERE CT.CONSOL_INVOICE_FK = CM.CONSOL_INVOICE_PK");
			sb.Append("   AND JS.JOB_CARD_TRN_PK = CT.JOB_CARD_FK");
			sb.Append("   AND CT.JOB_CARD_FK = " + JOBPK);
			sb.Append("   AND CT.FRT_OTH_ELEMENT_FK IN");
			sb.Append("       (SELECT FC.FREIGHT_ELEMENT_MST_FK");
			sb.Append("          FROM FREIGHT_CRCTR_TRN FC");
			sb.Append("         WHERE FC.FREIGHT_CRCTR_TRN_PK IN(" + FrtCrctTrnPk + "))");

			DSInv = objWF.GetDataSet(sb.ToString());
			DSInv.Tables[0].TableName = "tblMaster";
			DSInv.Tables.Add(objWF.GetDataTable(FetchCRNDtls(FrtCrctTrnPk, BizType)));
			DSInv.Tables[1].TableName = "tblTransaction";
			try {
				return DSInv;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception exp) {
				throw exp;
			}
		}
        /// <summary>
        /// Fetches the CRN DTLS.
        /// </summary>
        /// <param name="FrtCrctTrnPk">The FRT CRCT TRN pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public string FetchCRNDtls(string FrtCrctTrnPk, int BizType)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			//SEA
			if (BizType == 2) {
				sb.Append("SELECT CIT.CONSOL_INVOICE_PK CONSOL_INVOICE_FK,");
				sb.Append("       CIT.CONSOL_INVOICE_PK CONSOL_INVOICE_TRN_FK,");
				sb.Append("       JF.FREIGHT_ELEMENT_MST_FK FRT_OTH_ELEMENT_FK,");
				sb.Append("       JF.CURRENCY_MST_FK,");
				sb.Append("       JF.FREIGHT_AMT ELEMENT_INV_AMT,");
				sb.Append("       JF.EXCHANGE_RATE,");
				sb.Append("       (JF.FREIGHT_AMT * JF.EXCHANGE_RATE) ELE_AMT_IN_CRN_CUR,");
				sb.Append("       ((JF.FREIGHT_AMT - FCT.TOTAL_CORRECTED_FREIGHT) * JF.EXCHANGE_RATE) CRN_AMT_IN_CRN_CUR,");
				sb.Append("       JF.EXCHANGE_RATE ROE");
				sb.Append("  FROM FREIGHT_CRCTR_TRN      FCT,");
				sb.Append("       JOB_TRN_FD     JF,");
				sb.Append("       CONSOL_INVOICE_TBL     CIT,");
				sb.Append("       CONSOL_INVOICE_TRN_TBL CITT");
				sb.Append(" WHERE JF.JOB_TRN_FD_PK = FCT.JOB_TRN_SEA_EXP_FD_FK");
				sb.Append("   AND JF.CONSOL_INVOICE_TRN_FK = CITT.CONSOL_INVOICE_TRN_PK");
				sb.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
				sb.Append("   AND FCT.FREIGHT_CRCTR_TRN_PK IN(" + FrtCrctTrnPk + ") ");
			//AIR
			} else {
				sb.Append("SELECT CIT.CONSOL_INVOICE_PK CONSOL_INVOICE_FK,");
				sb.Append("       CIT.CONSOL_INVOICE_PK CONSOL_INVOICE_TRN_FK,");
				sb.Append("       JF.FREIGHT_ELEMENT_MST_FK FRT_OTH_ELEMENT_FK,");
				sb.Append("       JF.CURRENCY_MST_FK,");
				sb.Append("       JF.FREIGHT_AMT ELEMENT_INV_AMT,");
				sb.Append("       JF.EXCHANGE_RATE,");
				sb.Append("       (JF.FREIGHT_AMT * JF.EXCHANGE_RATE) ELE_AMT_IN_CRN_CUR,");
				sb.Append("       ((JF.FREIGHT_AMT - FCT.TOTAL_CORRECTED_FREIGHT) * JF.EXCHANGE_RATE) CRN_AMT_IN_CRN_CUR,");
				sb.Append("       JF.EXCHANGE_RATE ROE");
				sb.Append("  FROM FREIGHT_CRCTR_TRN      FCT,");
				sb.Append("       JOB_TRN_FD     JF,");
				sb.Append("       CONSOL_INVOICE_TBL     CIT,");
				sb.Append("       CONSOL_INVOICE_TRN_TBL CITT");
				sb.Append(" WHERE JF.JOB_TRN_FD_PK = FCT.JOB_TRN_AIR_EXP_FD_FK");
				sb.Append("   AND JF.CONSOL_INVOICE_TRN_FK = CITT.CONSOL_INVOICE_TRN_PK");
				sb.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK");
				sb.Append("   AND FCT.FREIGHT_CRCTR_TRN_PK IN(" + FrtCrctTrnPk + ") ");
			}
			try {
				return sb.ToString();
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception exp) {
				throw exp;
			}
		}
        #endregion

        #region "Invoice Detail Data"
        /// <summary>
        /// Fetches the inv DTLS.
        /// </summary>
        /// <param name="FrtCrctTrnPk">The FRT CRCT TRN pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public string FetchInvDtls(string FrtCrctTrnPk, int BizType)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			//SEA
			if (BizType == 2) {
				sb.Append("SELECT FMT.FREIGHT_ELEMENT_MST_PK FRT_OTH_ELEMENT_FK_IN,");
				sb.Append("       'FREIGHT' AS TYPE,");
				sb.Append("       JOB.JOBCARD_REF_NO,");
				sb.Append("       '1' UNIT,");
				sb.Append("       JF.JOB_TRN_FD_PK FRT_TBL_FK_IN,");
				sb.Append("       JOB.JOB_CARD_TRN_PK JOB_CARD_FK_IN,");
				sb.Append("       1 FRT_OTH_ELEMENT_IN,");
				sb.Append("       nvl(CMT.COMMODITY_MST_PK,0) COMMODITY_MST_FK_IN,");
				sb.Append("       FMT.FREIGHT_ELEMENT_NAME AS FRT_DESC_IN,");
				sb.Append("       '' AS ELEMENT,");
				sb.Append("       FCT.CORRECTED_CURR_FK CURRENCY_MST_FK_IN,");
				sb.Append("       CUR.CURRENCY_ID,");
				sb.Append("       '' AS CURR,");
				sb.Append("       (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) ELEMENT_AMT_IN,");
				sb.Append("       FCT.EXCHANGE_RATE EXCHANGE_RATE_IN,");
				sb.Append("       ROUND((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("             FCT.EXCHANGE_RATE,");
				sb.Append("             2) AMT_IN_INV_CURR_IN,");
				sb.Append("       (SELECT FETCH_VAT((SELECT BB_FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1) FROM DUAL),");
				sb.Append("                         FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                          " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                         FCT.PMT_TYPE,");
				sb.Append("                         FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                         1)");
				sb.Append("          FROM DUAL) VAT_CODE_IN,");
				sb.Append("       (SELECT FETCH_VAT((SELECT BB_FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1) FROM DUAL),");
				sb.Append("                         FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                          " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                         FCT.PMT_TYPE,");
				sb.Append("                         FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                         2)");
				sb.Append("          FROM DUAL) TAX_PCNT_IN,");
				sb.Append("       ");
				sb.Append("       ((SELECT FETCH_VAT((SELECT BB_FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1) FROM DUAL),");
				sb.Append("                          FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                           " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                          FCT.PMT_TYPE,");
				sb.Append("                          FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                          2)");
				sb.Append("           FROM DUAL) *");
				sb.Append("       ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) * FCT.EXCHANGE_RATE) / 100) TAX_AMT_IN,");
				sb.Append("       ");
				sb.Append("       NVL((((SELECT FETCH_VAT((SELECT BB_FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1) FROM DUAL),");
				sb.Append("                           FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                            " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                           FCT.PMT_TYPE,");
				sb.Append("                           FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                           2)");
				sb.Append("            FROM DUAL) * ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("       FCT.EXCHANGE_RATE) / 100) +");
				sb.Append("       (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) * FCT.EXCHANGE_RATE),0) TOT_AMT_IN,");
				sb.Append("       NVL((((SELECT FETCH_VAT((SELECT BB_FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " , " + BizType + ", 1) FROM DUAL),");
				sb.Append("                           FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                            " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                           FCT.PMT_TYPE,");
				sb.Append("                           FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                           2)");
				sb.Append("            FROM DUAL) * ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("       FCT.EXCHANGE_RATE) / 100) +");
				sb.Append("       (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) * FCT.EXCHANGE_RATE),0) TOT_AMT_IN_LOC_CURR_IN,");
				sb.Append("       '' AS REMARKS_IN,");
				sb.Append("       'NEW' AS \"MODE1\",");
				sb.Append("        1  AS \"JOBTYPE_IN\",");
				sb.Append("       'FALSE' AS CHK,");
				sb.Append("       nvl(PAR.FRT_BOF_FK,0)FRT_BOF_FK,JOB.CREATED_BY_FK CREATED_BY_FK_IN");
				sb.Append("  FROM JOB_CARD_TRN    JOB,");
				sb.Append("       HBL_EXP_TBL             HBL,");
				sb.Append("       JOB_TRN_FD      JF,");
				sb.Append("       FREIGHT_CRCTR_TRN       FCT,");
				sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
				sb.Append("       COMMODITY_MST_TBL       CMT,");
				sb.Append("       CURRENCY_TYPE_MST_TBL   CUR,");
				sb.Append("       PARAMETERS_TBL          PAR");
				sb.Append(" WHERE JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
				sb.Append("   AND FCT.JOB_TRN_SEA_EXP_FD_FK = JF.JOB_TRN_FD_PK");
				sb.Append("   AND JOB.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
				sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = JF.FREIGHT_ELEMENT_MST_FK");
				sb.Append("   AND FCT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
				sb.Append("   AND CUR.CURRENCY_MST_PK = FCT.CORRECTED_CURR_FK");
				sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
				sb.Append("   AND FCT.FREIGHT_CRCTR_TRN_PK IN(" + FrtCrctTrnPk + ") ");
			//AIR
			} else {
				sb.Append("SELECT FMT.FREIGHT_ELEMENT_MST_PK FRT_OTH_ELEMENT_FK_IN,");
				sb.Append("       'FREIGHT' AS TYPE,");
				sb.Append("       JOB.JOBCARD_REF_NO,");
				sb.Append("       '1' UNIT,");
				sb.Append("       JF.JOB_TRN_FD_PK FRT_TBL_FK_IN,");
				sb.Append("       JOB.JOB_CARD_TRN_PK JOB_CARD_FK_IN,");
				sb.Append("       1 FRT_OTH_ELEMENT_IN,");
				sb.Append("       nvl(CMT.COMMODITY_MST_PK,0) COMMODITY_MST_FK_IN,");
				sb.Append("       FMT.FREIGHT_ELEMENT_NAME AS FRT_DESC_IN,");
				sb.Append("       '' AS ELEMENT,");
				sb.Append("       FCT.CORRECTED_CURR_FK CURRENCY_MST_FK_IN,");
				sb.Append("       CUR.CURRENCY_ID,");
				sb.Append("       '' AS CURR,");
				sb.Append("       (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) ELEMENT_AMT_IN,");
				sb.Append("       FCT.EXCHANGE_RATE EXCHANGE_RATE_IN,");
				sb.Append("       ROUND((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("             FCT.EXCHANGE_RATE,");
				sb.Append("             2) AMT_IN_INV_CURR_IN,");
				sb.Append("       (SELECT FETCH_VAT((SELECT FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1) FROM DUAL),");
				sb.Append("                         FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                          " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                         FCT.PMT_TYPE,");
				sb.Append("                         FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                         1)");
				sb.Append("          FROM DUAL) VAT_CODE_IN,");
				sb.Append("       (SELECT FETCH_VAT((SELECT FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1) FROM DUAL),");
				sb.Append("                         FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                          " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                         FCT.PMT_TYPE,");
				sb.Append("                         FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                         2)");
				sb.Append("          FROM DUAL) TAX_PCNT_IN,");
				sb.Append("       ((SELECT FETCH_VAT((SELECT FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1) FROM DUAL),");
				sb.Append("                          FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                           " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                          FCT.PMT_TYPE,");
				sb.Append("                          FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                          2)");
				sb.Append("           FROM DUAL) *");
				sb.Append("       ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) * FCT.EXCHANGE_RATE) / 100) TAX_AMT_IN,");
				sb.Append("       NVL((((SELECT FETCH_VAT((SELECT FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1) FROM DUAL),");
				sb.Append("                           FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                            " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                           FCT.PMT_TYPE,");
				sb.Append("                           FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                           2)");
				sb.Append("            FROM DUAL) * ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("       FCT.EXCHANGE_RATE) / 100) +");
				sb.Append("       (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) * FCT.EXCHANGE_RATE),0) TOT_AMT_IN,");
				sb.Append("       NVL((((SELECT FETCH_VAT((SELECT FETCH_EU( " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ", " + BizType + ", 1) FROM DUAL),");
				sb.Append("                           FCT.INVOICE_CUSTOMER_FK,");
				sb.Append("                            " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
				sb.Append("                           FCT.PMT_TYPE,");
				sb.Append("                           FCT.FREIGHT_ELEMENT_MST_FK,");
				sb.Append("                           2)");
				sb.Append("            FROM DUAL) * ((FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) *");
				sb.Append("       FCT.EXCHANGE_RATE) / 100) +");
				sb.Append("       (FCT.TOTAL_CORRECTED_FREIGHT - JF.FREIGHT_AMT) * FCT.EXCHANGE_RATE),0) TOT_AMT_IN_LOC_CURR_IN,");
				sb.Append("       '' AS REMARKS_IN,");
				sb.Append("       'NEW' AS \"MODE1\",");
				sb.Append("        1  AS \"JOBTYPE_IN\",");
				sb.Append("       'FALSE' AS CHK,");
				sb.Append("       nvl(PAR.FRT_BOF_FK,0)FRT_BOF_FK,");
				sb.Append("       JOB.CREATED_BY_FK CREATED_BY_FK_IN");
				sb.Append("  FROM JOB_CARD_TRN    JOB,");
				sb.Append("       HAWB_EXP_TBL             HBL,");
				sb.Append("       JOB_TRN_FD      JF,");
				sb.Append("       FREIGHT_CRCTR_TRN       FCT,");
				sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
				sb.Append("       COMMODITY_MST_TBL       CMT,");
				sb.Append("       CURRENCY_TYPE_MST_TBL   CUR,");
				sb.Append("       PARAMETERS_TBL          PAR");
				sb.Append(" WHERE JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK");
				sb.Append("   AND FCT.JOB_TRN_AIR_EXP_FD_FK = JF.JOB_TRN_FD_PK");
				sb.Append("   AND JOB.JOB_CARD_TRN_PK = JF.JOB_CARD_TRN_FK");
				sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = JF.FREIGHT_ELEMENT_MST_FK");
				sb.Append("   AND FCT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
				sb.Append("   AND CUR.CURRENCY_MST_PK = FCT.CORRECTED_CURR_FK");
				sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
				sb.Append("   AND FCT.FREIGHT_CRCTR_TRN_PK IN(" + FrtCrctTrnPk + ") ");
			}
			try {
				return sb.ToString();
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception exp) {
				throw exp;
			}
		}
        #endregion

        #region "Get Job Freight"
        /// <summary>
        /// Fetches the job FRT.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchJobFrt(int JOBPK, int BizType)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT JF.FREIGHT_ELEMENT_MST_FK,");
			sb.Append("       JF.RATEPERBASIS,");
			sb.Append("       JF.FREIGHT_AMT,");
			sb.Append("       JF.FREIGHT_TYPE,JF.JOB_TRN_FD_PK");
			sb.Append("  FROM JOB_TRN_FD JF");
			sb.Append(" WHERE JF.JOB_CARD_TRN_FK = " + JOBPK);
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Corrected Freight"
        /// <summary>
        /// Fetches the CRCT FRT.
        /// </summary>
        /// <param name="FCHPK">The FCHPK.</param>
        /// <returns></returns>
        public DataSet FetchCrctFrt(int FCHPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("  SELECT FCT.FREIGHT_ELEMENT_MST_FK,");
			sb.Append("         FCT.CORRECTED_FREIGHT,");
			sb.Append("         FCT.TOTAL_CORRECTED_FREIGHT,");
			sb.Append("         FCT.PMT_TYPE,FCT.CORRECTED_CURR_FK,FCT.EXCHANGE_RATE");
			sb.Append("          FROM FREIGHT_CRCTR_TRN FCT");
			sb.Append("         WHERE FCT.FREIGHT_CORRECTOR_HDR_FK = " + FCHPK);
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Generate New HBL & HAWB Function"

        /// <summary>
        /// Generates the HBL.
        /// </summary>
        /// <param name="HBLPK">The HBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="FCHPK">The FCHPK.</param>
        /// <param name="VERSION">The version.</param>
        /// <returns></returns>
        public ArrayList GenerateHBL(int HBLPK, int JOBPK, int FCHPK, long VERSION)
		{
			WorkFlow objWK = new WorkFlow();

			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();

			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			int HBL_PK = 0;
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			arrMessage.Clear();

			try {
				var _with15 = insCommand;
				_with15.Connection = objWK.MyConnection;
				_with15.CommandType = CommandType.StoredProcedure;
				_with15.CommandText = objWK.MyUserName + ".HBL_EXP_TBL_PKG.GENERATE_NEW_HBL";
				var _with16 = _with15.Parameters;
				_with16.Add("HBL_FK_IN", HBLPK).Direction = ParameterDirection.Input;
				_with16.Add("JOB_FK_IN", JOBPK).Direction = ParameterDirection.Input;
				_with16.Add("CRCTR_FK_IN", FCHPK).Direction = ParameterDirection.Input;
				_with16.Add("VERSION_IN", VERSION).Direction = ParameterDirection.Input;
				_with16.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "HBL_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with17 = objWK.MyDataAdapter;
				_with17.InsertCommand = insCommand;
				_with17.InsertCommand.Transaction = TRAN;
				_with17.InsertCommand.ExecuteNonQuery();
				HBL_PK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
				} else {
					TRAN.Commit();
					arrMessage.Add(HBL_PK);
					return arrMessage;
				}

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				if (objWK.MyConnection.State == ConnectionState.Open) {
					objWK.MyConnection.Close();
				}
			}
            return new ArrayList();
		}
        /// <summary>
        /// Generates the hawb.
        /// </summary>
        /// <param name="HBLPK">The HBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="FCHPK">The FCHPK.</param>
        /// <param name="VERSION">The version.</param>
        /// <returns></returns>
        public ArrayList GenerateHAWB(int HBLPK, int JOBPK, int FCHPK, long VERSION)
		{
			WorkFlow objWK = new WorkFlow();

			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();

			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			int HBL_PK = 0;
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			arrMessage.Clear();

			try {
				var _with18 = insCommand;
				_with18.Connection = objWK.MyConnection;
				_with18.CommandType = CommandType.StoredProcedure;
				_with18.CommandText = objWK.MyUserName + ".HAWB_EXP_TBL_PKG.GENERATE_NEW_HAWB";
				var _with19 = _with18.Parameters;
				_with19.Add("HBL_FK_IN", HBLPK).Direction = ParameterDirection.Input;
				_with19.Add("JOB_FK_IN", JOBPK).Direction = ParameterDirection.Input;
				_with19.Add("CRCTR_FK_IN", FCHPK).Direction = ParameterDirection.Input;
				_with19.Add("VERSION_IN", VERSION).Direction = ParameterDirection.Input;
				_with19.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "HBL_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with20 = objWK.MyDataAdapter;
				_with20.InsertCommand = insCommand;
				_with20.InsertCommand.Transaction = TRAN;
				_with20.InsertCommand.ExecuteNonQuery();
				HBL_PK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
				} else {
					TRAN.Commit();
					arrMessage.Add(HBL_PK);
					return arrMessage;
				}

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				if (objWK.MyConnection.State == ConnectionState.Open) {
					objWK.MyConnection.Close();
				}
			}return new ArrayList();
		}
        #endregion

        #region "GET BIZ TYPE BY FREIGHT CORRECTOR PK "
        /// <summary>
        /// Gets the FRT CRCT biz type by pk.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <returns></returns>
        public short GetFrtCrctBizTypeByPk(int PK)
		{
			WorkFlow objWK = new WorkFlow();
			try {
                Int16 Count = Convert.ToInt16(objWK.ExecuteScaler("SELECT COUNT(*) FROM FREIGHT_CORRECTOR_HDR FCH WHERE FCH.HBL_FK IS NULL AND FCH.FREIGHT_CORRECTOR_HDR_PK=" + PK));
				if (Count > 0) {
					return 1;
				} else {
					return 2;
				}
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
	}
}
