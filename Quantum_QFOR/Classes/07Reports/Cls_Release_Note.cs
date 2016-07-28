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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_Release_Note : CommonFeatures
    {
        /// <summary>
        /// The object track n trace
        /// </summary>
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        #region "Enum Header"

        /// <summary>
        /// 
        /// </summary>
        private enum Header
        {
            /// <summary>
            /// The slno
            /// </summary>
            SLNO = 0,
            /// <summary>
            /// The jobcardpk
            /// </summary>
            JOBCARDPK = 1,
            /// <summary>
            /// The jobrefno
            /// </summary>
            JOBREFNO = 2,
            /// <summary>
            /// The hb l_ hawbrefno
            /// </summary>
            HBL_HAWBREFNO = 3,
            /// <summary>
            /// The mb l_ mawbrefno
            /// </summary>
            MBL_MAWBREFNO = 4,
            /// <summary>
            /// The consigneeid
            /// </summary>
            CONSIGNEEID = 5,
            /// <summary>
            /// The consignee
            /// </summary>
            CONSIGNEE = 6,
            /// <summary>
            /// The vs l_ flight
            /// </summary>
            VSL_FLIGHT = 7,
            /// <summary>
            /// The polid
            /// </summary>
            POLID = 8,
            /// <summary>
            /// The polname
            /// </summary>
            POLNAME = 9,
            /// <summary>
            /// The podid
            /// </summary>
            PODID = 10,
            /// <summary>
            /// The podname
            /// </summary>
            PODNAME = 11,
            /// <summary>
            /// The warehouse pk
            /// </summary>
            WarehousePK = 12,
            /// <summary>
            /// The warehouse
            /// </summary>
            Warehouse = 13,
            /// <summary>
            /// The arrdate
            /// </summary>
            ARRDATE = 14,
            /// <summary>
            /// The SPL inst data
            /// </summary>
            SplInstData = 15,
            /// <summary>
            /// The SPL inst
            /// </summary>
            SplInst = 16,
            /// <summary>
            /// The carg o_ type
            /// </summary>
            CARGO_TYPE = 17,
            /// <summary>
            /// The arrivaldate
            /// </summary>
            ARRIVALDATE = 18,
            /// <summary>
            /// The sel
            /// </summary>
            SEL = 19,
            /// <summary>
            /// The flag
            /// </summary>
            Flag = 20
        }

        #endregion "Enum Header"

        #region "Fetch For AIR/SEA Import"

        /// <summary>
        /// Fetches the air user import.
        /// </summary>
        /// <param name="Ves_Flight">The ves_ flight.</param>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="PodPk">The pod pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Loc">The loc.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAirUserImport(string Ves_Flight = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, int Loc = 0, Int32 flag = 0)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (Ves_Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND UPPER(JAI.VOYAGE_FLIGHT_NO) LIKE '%" + Ves_Flight.ToUpper() + "%'";
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JAI.JOB_CARD_TRN_PK = " + JobPk;
            }

            if (PolPk > 0)
            {
                strCondition = strCondition + " And JAI.PORT_MST_POL_FK = " + PolPk;
            }

            if (PodPk > 0)
            {
                strCondition = strCondition + " And JAI.PORT_MST_POD_FK = " + PodPk;
            }

            if (CustPk > 0)
            {
                strCondition = strCondition + " AND CMST.CUSTOMER_MST_PK=" + CustPk;
            }
            strCondition = strCondition + "  and JAI.process_type = 2 ";
            strCondition = strCondition + "  and JAI.business_type = 1 ";

            Strsql = "select count(*) from (select JAI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += "JAI.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += "JAI.HBL_HAWB_REF_NO AS HBL_HAWBREFNO,";
            Strsql += "JAI.MBL_MAWB_REF_NO AS MBL_MAWBREFNO,";
            Strsql += "CMST.CUSTOMER_ID AS CONSIGNEEID,";
            Strsql += "CMST.CUSTOMER_NAME AS CONSIGNEE,";
            Strsql += "JAI.VOYAGE_FLIGHT_NO AS VSL_FLIGHT,";
            Strsql += "POL.PORT_ID POLID,";
            Strsql += "POL.PORT_NAME POLNAME,";
            Strsql += "POD.PORT_ID PODID,";
            Strsql += "POD.PORT_NAME PODNAME,";
            Strsql += "DEPOT.DEPOT_MST_PK AS WarehousePK,";
            Strsql += "DEPOT.DEPOT_NAME AS Warehouse,";
            Strsql += "DOCS.SPECIAL_INSTRUCTION AS SplInstData,";
            Strsql += " '' SplInst,";
            Strsql += "2 CARGO_TYPE,";
            Strsql += " TO_DATE(JAI.ARRIVAL_DATE, '" + dateFormat + "') ARRDATE, ";
            Strsql += "JAI.ARRIVAL_DATE AS ARRIVALDATE";
            Strsql += " from JOB_CARD_TRN  JAI,";
            Strsql += " CUSTOMER_MST_TBL     CMST,";
            Strsql += " PORT_MST_TBL         POL,";
            Strsql += " PORT_MST_TBL         POD,";
            Strsql += " DOCS_PRINT_DTL_TBL   DOCS,";
            Strsql += " user_mst_tbl umt ,";
            Strsql += " DEPOT_MST_TBL DEPOT ";
            Strsql += "WHERE CMST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND POL.PORT_MST_PK = JAI.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK = JAI.PORT_MST_POD_FK";
            Strsql += "AND DOCS.JOB_CARD_REF_FK(+) = JAI.JOB_CARD_TRN_PK";
            Strsql += "AND DEPOT.DEPOT_MST_PK(+) = DOCS.DEPOT_MST_FK";
            Strsql += "AND DOCS.DOC_NUMBER(+) LIKE 'RELEASE%'";
            Strsql += "AND JAI.ARRIVAL_DATE IS NOT NULL";
            Strsql += " and (jai.created_by_fk = umt.user_mst_pk OR";
            Strsql += "  jai.Last_Modified_By_Fk = UMT.USER_MST_PK) ";
            Strsql += " and umt.default_location_fk=" + Loc;
            Strsql += "and docs.doc_number in";
            Strsql += "(select max(d1.doc_number)";
            Strsql += "from DOCS_PRINT_DTL_TBL D1";
            Strsql += "where d1.job_card_ref_fk = jAi.job_card_TRN_pk and D1.DOC_NUMBER(+) LIKE 'RELEASE%')";
            Strsql += strCondition;
            Strsql += " union";

            Strsql += "select distinct JAI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += " JAI.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += " JAI.HBL_HAWB_REF_NO AS HBL_HAWBREFNO,";
            Strsql += " JAI.MBL_MAWB_REF_NO AS MBL_MAWBREFNO,";
            Strsql += " CMST.CUSTOMER_ID AS CONSIGNEEID,";
            Strsql += " CMST.CUSTOMER_NAME AS CONSIGNEE,";
            Strsql += " JAI.VOYAGE_FLIGHT_NO AS VSL_FLIGHT,";
            Strsql += " POL.PORT_ID POLID,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_ID PODID,";
            Strsql += " POD.PORT_NAME PODNAME,";
            Strsql += " null WarehousePK,";
            Strsql += " null Warehouse,";
            Strsql += "   null AS SplInstData,";
            Strsql += " '' SplInst,";
            Strsql += " 2 CARGO_TYPE,";
            Strsql += "  TO_DATE(JAI.ARRIVAL_DATE, '" + dateFormat + "') ARRDATE,";
            Strsql += " JAI.ARRIVAL_DATE AS ARRIVALDATE ";
            Strsql += " from JOB_CARD_TRN JAI,";
            Strsql += " CUSTOMER_MST_TBL     CMST,";
            Strsql += " PORT_MST_TBL         POL,";
            Strsql += " user_mst_tbl umt ,";
            Strsql += " PORT_MST_TBL POD";

            Strsql += "WHERE CMST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
            Strsql += " AND POL.PORT_MST_PK = JAI.PORT_MST_POL_FK";
            Strsql += " AND POD.PORT_MST_PK = JAI.PORT_MST_POD_FK";
            Strsql += " AND JAI.ARRIVAL_DATE IS NOT NULL";
            Strsql += " and (jai.created_by_fk = umt.user_mst_pk OR";
            Strsql += "  jai.Last_Modified_By_Fk = UMT.USER_MST_PK) ";
            Strsql += " and umt.default_location_fk=" + Loc;
            // jitu
            Strsql += " and jAi.job_card_TRN_pk in";
            Strsql += "(select jc.JOB_CARD_TRN_PK";
            Strsql += "  from job_card_TRN jc";
            Strsql += "   where(jc.arrival_date Is Not null)";
            Strsql += "   and jc.job_card_TRN_pk not in";
            Strsql += "  (select d1.job_card_ref_fk";
            Strsql += "  from DOCS_PRINT_DTL_TBL D1";
            Strsql += " where d1.doc_number lIKE 'RELEASE%'))";
            Strsql += strCondition + ")";

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

            Strsql = " select  * from ";
            Strsql += " (SELECT ROWNUM AS SLNO,Q.* FROM ";
            Strsql += "(select JAI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += "JAI.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += "JAI.HBL_HAWB_REF_NO AS HBL_HAWBREFNO,";
            Strsql += "JAI.MBL_MAWB_REF_NO AS MBL_MAWBREFNO,";
            Strsql += "CMST.CUSTOMER_ID AS CONSIGNEEID,";
            Strsql += "CMST.CUSTOMER_NAME AS CONSIGNEE,";
            Strsql += "JAI.VOYAGE_FLIGHT_NO AS VSL_FLIGHT,";
            Strsql += "POL.PORT_ID POLID,";
            Strsql += "POL.PORT_NAME POLNAME,";
            Strsql += "POD.PORT_ID PODID,";
            Strsql += "POD.PORT_NAME PODNAME,";
            Strsql += "DEPOT.DEPOT_MST_PK AS WarehousePK,";
            Strsql += "DEPOT.DEPOT_NAME AS Warehouse,";
            Strsql += "TO_DATE(JAI.ARRIVAL_DATE, '" + dateFormat + "') ARRDATE,";
            Strsql += "DOCS.SPECIAL_INSTRUCTION AS SplInstData,";
            Strsql += " '' SplInst,";
            Strsql += "2 CARGO_TYPE,";
            Strsql += " JAI.ARRIVAL_DATE AS ARRIVALDATE ";
            Strsql += " from JOB_CARD_TRN    JAI,";
            Strsql += " CUSTOMER_MST_TBL     CMST,";
            Strsql += " PORT_MST_TBL         POL,";
            Strsql += " PORT_MST_TBL         POD,";
            Strsql += " DOCS_PRINT_DTL_TBL   DOCS,";
            Strsql += " user_mst_tbl umt ,";
            Strsql += "DEPOT_MST_TBL DEPOT";
            Strsql += "WHERE CMST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND POL.PORT_MST_PK = JAI.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK = JAI.PORT_MST_POD_FK";
            Strsql += "AND DOCS.JOB_CARD_REF_FK(+) = JAI.JOB_CARD_TRN_PK";
            Strsql += "AND DEPOT.DEPOT_MST_PK(+) = DOCS.DEPOT_MST_FK";
            Strsql += " and (jai.created_by_fk = umt.user_mst_pk OR";
            Strsql += "  jai.Last_Modified_By_Fk = UMT.USER_MST_PK) ";
            Strsql += " and umt.default_location_fk=" + Loc;
            Strsql += "AND DOCS.DOC_NUMBER(+) LIKE 'RELEASE%'";
            Strsql += "AND JAI.ARRIVAL_DATE IS NOT NULL";
            Strsql += "and docs.doc_number in";
            Strsql += "(select max(d1.doc_number)";
            Strsql += " from DOCS_PRINT_DTL_TBL D1";
            Strsql += " where d1.job_card_ref_fk = jAi.job_card_TRN_pk and D1.DOC_NUMBER(+) LIKE 'RELEASE%')";
            Strsql += strCondition;
            Strsql += " union";

            Strsql += "select distinct JAI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += " JAI.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += " JAI.HBL_HAWB_REF_NO AS HBL_HAWBREFNO,";
            Strsql += " JAI.MBL_MAWB_REF_NO AS MBL_MAWBREFNO,";
            Strsql += " CMST.CUSTOMER_ID AS CONSIGNEEID,";
            Strsql += " CMST.CUSTOMER_NAME AS CONSIGNEE,";
            Strsql += "  JAI.VOYAGE_FLIGHT_NO AS VSL_FLIGHT,";
            Strsql += "  POL.PORT_ID POLID,";
            Strsql += "  POL.PORT_NAME POLNAME,";
            Strsql += "  POD.PORT_ID PODID,";
            Strsql += "  POD.PORT_NAME PODNAME,";
            Strsql += "   null WarehousePK,";
            Strsql += "   null Warehouse,";
            Strsql += "  TO_DATE(JAI.ARRIVAL_DATE, '" + dateFormat + "') ARRDATE,";
            Strsql += "   null AS SplInstData,";
            Strsql += " '' SplInst,";
            Strsql += " 2 CARGO_TYPE, ";
            Strsql += " JAI.ARRIVAL_DATE AS ARRIVALDATE ";
            Strsql += " from JOB_CARD_TRN   JAI,";
            Strsql += " CUSTOMER_MST_TBL     CMST,";
            Strsql += " PORT_MST_TBL         POL,";
            Strsql += " user_mst_tbl umt ,";
            Strsql += " PORT_MST_TBL POD";

            Strsql += " WHERE CMST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND POL.PORT_MST_PK = JAI.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK = JAI.PORT_MST_POD_FK";
            Strsql += "AND JAI.ARRIVAL_DATE IS NOT NULL";
            Strsql += " and (jai.created_by_fk = umt.user_mst_pk OR";
            Strsql += "  jai.Last_Modified_By_Fk = UMT.USER_MST_PK) ";
            Strsql += " and umt.default_location_fk=" + Loc;
            Strsql += "and jAi.job_card_TRN_pk in";
            Strsql += " (select jc.JOB_CARD_TRN_PK";
            Strsql += "  from job_card_TRN jc";
            Strsql += "   where(jc.arrival_date Is Not null)";
            Strsql += "  and jc.job_card_TRN_pk not in";
            Strsql += " (select d1.job_card_ref_fk";
            Strsql += "   from DOCS_PRINT_DTL_TBL D1";
            Strsql += "   where d1.doc_number lIKE 'RELEASE%'))";
            Strsql += strCondition;
            Strsql += "ORDER BY ARRIVALDATE DESC,JOBREFNO DESC";

            Strsql += ")q) WHERE SLNO  Between " + start + " and " + last;

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
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the sea user import.
        /// </summary>
        /// <param name="Ves_Flight">The ves_ flight.</param>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="PodPk">The pod pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Loc">The loc.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="VoyageTrnFk">The voyage TRN fk.</param>
        /// <returns></returns>
        public DataSet FetchSeaUserImport(string Ves_Flight = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, int Loc = 0, Int32 flag = 0, Int32 VoyageTrnFk = 0)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (VoyageTrnFk > 0)
            {
                strCondition = strCondition + " AND JSI.VOYAGE_TRN_FK=" + VoyageTrnFk;
            }
            else if (Ves_Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND (JSI.VESSEL_NAME ||" + "'-'" + " || JSI.VOYAGE_FLIGHT_NO) = '" + Ves_Flight + "'";
            }

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JSI.Job_Card_Trn_Pk = " + JobPk;
            }

            if (PolPk > 0)
            {
                strCondition = strCondition + " And JSI.PORT_MST_POL_FK = " + PolPk;
            }

            if (PodPk > 0)
            {
                strCondition = strCondition + " And JSI.PORT_MST_POD_FK = " + PodPk;
            }

            if (CustPk > 0)
            {
                strCondition = strCondition + " AND CMST.CUSTOMER_MST_PK=" + CustPk;
            }

            Strsql = "select count(*) from (select JSI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += "JSI.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += "JSI.HBL_HAWB_REF_NO AS HBL_HAWBREFNO,";
            Strsql += "JSI.MBL_MAWB_REF_NO AS MBL_MAWBREFNO,";
            Strsql += "(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "(SELECT CMST.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.REL_PARTY_CUST_MST_FK)";
            Strsql += "ELSE";
            Strsql += "(SELECT CMST.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.CONSIGNEE_CUST_MST_FK) END) CONSIGNEEID,";

            Strsql += "(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "(SELECT CMST.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.REL_PARTY_CUST_MST_FK)";
            Strsql += "ELSE";
            Strsql += "(SELECT CMST.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.CONSIGNEE_CUST_MST_FK) END) CONSIGNEE,";

            Strsql += "JSI.VESSEL_NAME AS VSL_FLIGHT,";
            Strsql += "POL.PORT_ID POLID,";
            Strsql += "POL.PORT_NAME POLNAME,";
            Strsql += "POD.PORT_ID PODID,";
            Strsql += "POD.PORT_NAME PODNAME,";
            Strsql += "(CASE WHEN JSI.CARGO_TYPE= 1 THEN";
            Strsql += "OMST.OPERATOR_MST_PK ELSE";
            Strsql += "VMST.VENDOR_MST_PK END ) WarehousePK,";
            Strsql += "(CASE WHEN JSI.CARGO_TYPE= 1 THEN";
            Strsql += "OMST.OPERATOR_NAME";
            Strsql += "ELSE VMST.VENDOR_NAME END )  Warehouse,";
            Strsql += "TO_DATE(JSI.ARRIVAL_DATE, '" + dateFormat + "') ARRDATE,";

            Strsql += "DOCS.SPECIAL_INSTRUCTION AS SplInstData,";
            Strsql += " '' SplInst,";
            Strsql += "JSI.CARGO_TYPE CARGO_TYPE, ";
            Strsql += "JSI.ARRIVAL_DATE AS ARRIVALDATE";
            Strsql += "from JOB_CARD_TRN    JSI,";
            Strsql += "CUSTOMER_MST_TBL     CMST,";
            Strsql += "PORT_MST_TBL         POL,";
            Strsql += "PORT_MST_TBL         POD,";
            Strsql += " OPERATOR_MST_TBL OMST,";
            Strsql += "DOCS_PRINT_DTL_TBL   DOCS,";
            Strsql += "user_mst_tbl umt ,";
            Strsql += " VENDOR_MST_TBL VMST";
            Strsql += " WHERE CMST.CUSTOMER_MST_PK(+) = JSI.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND POL.PORT_MST_PK = JSI.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK = JSI.PORT_MST_POD_FK";
            Strsql += " AND OMST.OPERATOR_MST_PK(+) = JSI.CARRIER_MST_FK ";
            Strsql += "AND DOCS.JOB_CARD_REF_FK(+) = JSI.JOB_CARD_TRN_PK";
            Strsql += "AND VMST.VENDOR_MST_PK(+) = DOCS.DEPOT_MST_FK";

            Strsql += " and (jsi.created_by_fk = umt.user_mst_pk OR";
            Strsql += "  JSI.Last_Modified_By_Fk = UMT.USER_MST_PK) ";
            Strsql += " and umt.default_location_fk=" + Loc;
            Strsql += "  and jsi.process_type = 2 ";
            Strsql += "  and jsi.business_type = 2 ";
            Strsql += "AND DOCS.DOC_NUMBER(+) LIKE 'RELEASE%'";
            Strsql += "AND JSI.ARRIVAL_DATE IS NOT NULL";
            Strsql += "and docs.doc_number in";
            Strsql += "(select max(d1.doc_number)";
            Strsql += "from DOCS_PRINT_DTL_TBL D1";
            Strsql += "where d1.job_card_ref_fk = jsi.job_card_TRN_pk and D1.DOC_NUMBER(+) LIKE 'RELEASE%')";
            Strsql += strCondition;
            Strsql += " union";

            Strsql += "select distinct JSI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += " JSI.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += " JSI.HBL_HAWB_REF_NO AS HBL_HAWBREFNO,";
            Strsql += " JSI.MBL_MAWB_REF_NO AS MBL_MAWBREFNO,";
            Strsql += "(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "(SELECT CMST.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.REL_PARTY_CUST_MST_FK)";
            Strsql += "ELSE";
            Strsql += "(SELECT CMST.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.CONSIGNEE_CUST_MST_FK) END) CONSIGNEEID,";

            Strsql += "(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "(SELECT CMST.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.REL_PARTY_CUST_MST_FK)";
            Strsql += "ELSE";
            Strsql += "(SELECT CMST.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.CONSIGNEE_CUST_MST_FK) END) CONSIGNEE,";

            Strsql += " JSI.VESSEL_NAME AS VSL_FLIGHT,";
            Strsql += " POL.PORT_ID POLID,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_ID PODID,";
            Strsql += " POD.PORT_NAME PODNAME,";
            Strsql += "(CASE WHEN JSI.CARGO_TYPE= 1 THEN";
            Strsql += "OMST.OPERATOR_MST_PK ELSE";
            Strsql += "NULL END ) WarehousePK,";
            Strsql += " (CASE WHEN JSI.CARGO_TYPE= 1 THEN";
            Strsql += " OMST.OPERATOR_NAME";
            Strsql += " ELSE null  END ) Warehouse,";
            Strsql += "  TO_DATE(JSI.ARRIVAL_DATE, '" + dateFormat + "') ARRDATE,";
            Strsql += "   null AS SplInstData,";
            Strsql += " '' SplInst,";
            Strsql += " JSI.CARGO_TYPE CARGO_TYPE,";
            Strsql += " JSI.ARRIVAL_DATE AS ARRIVALDATE";
            Strsql += " from JOB_CARD_TRN JSI,";
            Strsql += " CUSTOMER_MST_TBL     CMST,";
            Strsql += " PORT_MST_TBL         POL,";
            Strsql += " PORT_MST_TBL POD,";
            Strsql += "user_mst_tbl umt ,";
            Strsql += " OPERATOR_MST_TBL OMST";
            Strsql += "WHERE CMST.CUSTOMER_MST_PK(+) = JSI.CONSIGNEE_CUST_MST_FK";
            Strsql += "  AND POL.PORT_MST_PK = JSI.PORT_MST_POL_FK";
            Strsql += " AND POD.PORT_MST_PK = JSI.PORT_MST_POD_FK";
            Strsql += " AND OMST.OPERATOR_MST_PK(+) = JSI.CARRIER_MST_FK";
            Strsql += " AND JSI.ARRIVAL_DATE IS NOT NULL";
            Strsql += " and (jsi.created_by_fk = umt.user_mst_pk OR";
            Strsql += "  JSI.Last_Modified_By_Fk = UMT.USER_MST_PK) ";
            Strsql += " and umt.default_location_fk=" + Loc;
            Strsql += "  and jsi.process_type = 2 ";
            Strsql += "  and jsi.business_type = 2 ";
            Strsql += " and jsi.job_card_TRN_pk in";
            Strsql += "(select jc.JOB_CARD_TRN_PK";
            Strsql += "  from job_card_TRN  jc";
            Strsql += "   where(jc.arrival_date Is Not null)";
            Strsql += "   and jc.job_card_TRN_pk not in";
            Strsql += "  (select d1.job_card_ref_fk";
            Strsql += "  from DOCS_PRINT_DTL_TBL D1";
            Strsql += " where d1.doc_number lIKE 'RELEASE%'))";
            Strsql += strCondition + ")";

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

            Strsql = " select  * from ";
            Strsql += " (SELECT ROWNUM AS SLNO,Q.* FROM ";
            Strsql += "(select JSI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += "JSI.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += "JSI.HBL_HAWB_REF_NO AS HBL_HAWBREFNO,";
            Strsql += "JSI.MBL_MAWB_REF_NO AS MBL_MAWBREFNO,";
            Strsql += "(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "(SELECT CMST.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.REL_PARTY_CUST_MST_FK)";
            Strsql += "ELSE";
            Strsql += "(SELECT CMST.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.CONSIGNEE_CUST_MST_FK) END) CONSIGNEEID,";

            Strsql += "(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "(SELECT CMST.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.REL_PARTY_CUST_MST_FK)";
            Strsql += "ELSE";
            Strsql += "(SELECT CMST.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.CONSIGNEE_CUST_MST_FK) END) CONSIGNEE,";

            Strsql += "(CASE WHEN JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
            Strsql += " JSI.VESSEL_NAME || '-' || JSI.VOYAGE_FLIGHT_NO";
            Strsql += "ELSE JSI.VESSEL_NAME END) AS VSL_FLIGHT,";
            Strsql += "POL.PORT_ID POLID,";
            Strsql += "POL.PORT_NAME POLNAME,";
            Strsql += "POD.PORT_ID PODID,";
            Strsql += "POD.PORT_NAME PODNAME,";
            Strsql += "  VMST.VENDOR_MST_PK   WarehousePK,";
            Strsql += " VMST.VENDOR_NAME   Warehouse,";

            Strsql += "TO_DATE(JSI.ARRIVAL_DATE, '" + dateFormat + "') ARRDATE,";

            Strsql += "DOCS.SPECIAL_INSTRUCTION AS SplInstData,";
            Strsql += " '' SplInst,";
            Strsql += "JSI.CARGO_TYPE CARGO_TYPE,";
            Strsql += "JSI.ARRIVAL_DATE AS ARRIVALDATE ";

            Strsql += " from JOB_CARD_TRN    JSI,";
            Strsql += " CUSTOMER_MST_TBL     CMST,";
            Strsql += " PORT_MST_TBL         POL,";
            Strsql += " PORT_MST_TBL         POD,";
            Strsql += " OPERATOR_MST_TBL OMST,";
            Strsql += "DOCS_PRINT_DTL_TBL   DOCS,";
            Strsql += "user_mst_tbl umt ,";
            Strsql += " VENDOR_MST_TBL VMST";
            Strsql += "WHERE CMST.CUSTOMER_MST_PK(+) = JSI.CONSIGNEE_CUST_MST_FK";
            Strsql += " AND POL.PORT_MST_PK = JSI.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK = JSI.PORT_MST_POD_FK";
            Strsql += " AND OMST.OPERATOR_MST_PK(+) = JSI.CARRIER_MST_FK";
            Strsql += "AND DOCS.JOB_CARD_REF_FK(+) = JSI.JOB_CARD_TRN_PK";
            Strsql += "AND jsi.transporter_depot_fk = VMST.VENDOR_MST_PK(+)";
            Strsql += " AND( JSI.CREATED_BY_FK=UMT.USER_MST_PK OR JSI.LAST_MODIFIED_BY_FK=UMT.USER_MST_PK )";
            Strsql += " and umt.default_location_fk=" + Loc;

            Strsql += "AND DOCS.DOC_NUMBER(+) LIKE 'RELEASE%'";
            Strsql += "AND JSI.ARRIVAL_DATE IS NOT NULL";
            Strsql += "and docs.doc_number in";
            Strsql += "(select max(d1.doc_number)";
            Strsql += " from DOCS_PRINT_DTL_TBL D1";
            Strsql += " where d1.job_card_ref_fk = jsi.job_card_TRN_pk and D1.DOC_NUMBER(+) LIKE 'RELEASE%')";
            Strsql += strCondition;
            Strsql += "  and jsi.process_type = 2 ";
            Strsql += "  and jsi.business_type = 2 ";
            Strsql += " union";

            Strsql += "select distinct JSI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += " JSI.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += " JSI.HBL_HAWB_REF_NO AS HBL_HAWBREFNO,";
            Strsql += " JSI.MBL_MAWB_REF_NO AS MBL_MAWBREFNO,";
            Strsql += "(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "(SELECT CMST.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.REL_PARTY_CUST_MST_FK)";
            Strsql += "ELSE";
            Strsql += "(SELECT CMST.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.CONSIGNEE_CUST_MST_FK) END) CONSIGNEEID,";

            Strsql += "(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "(SELECT CMST.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.REL_PARTY_CUST_MST_FK)";
            Strsql += "ELSE";
            Strsql += "(SELECT CMST.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK(+)=JSI.CONSIGNEE_CUST_MST_FK) END) CONSIGNEE,";
            Strsql += "(CASE WHEN JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
            Strsql += " JSI.VESSEL_NAME || '-' || JSI.VOYAGE_FLIGHT_NO";
            Strsql += "ELSE JSI.VESSEL_NAME END) AS VSL_FLIGHT,";
            Strsql += "  POL.PORT_ID POLID,";
            Strsql += "  POL.PORT_NAME POLNAME,";
            Strsql += "  POD.PORT_ID PODID,";
            Strsql += "  POD.PORT_NAME PODNAME,";
            Strsql += "  VMST.VENDOR_MST_PK   WarehousePK,";
            Strsql += " VMST.VENDOR_NAME   Warehouse,";
            Strsql += " TO_DATE(JSI.ARRIVAL_DATE, '" + dateFormat + "') ARRDATE,";

            Strsql += " null AS SplInstData,";
            Strsql += " '' SplInst,";
            Strsql += "JSI.CARGO_TYPE CARGO_TYPE,";
            Strsql += "JSI.ARRIVAL_DATE AS ARRIVALDATE ";
            Strsql += "from JOB_CARD_TRN     JSI,";
            Strsql += " CUSTOMER_MST_TBL     CMST,";
            Strsql += " PORT_MST_TBL         POL,";
            Strsql += " PORT_MST_TBL POD,";
            Strsql += " OPERATOR_MST_TBL OMST,";
            Strsql += "user_mst_tbl umt ,";
            Strsql += " VENDOR_MST_TBL VMST";
            Strsql += " WHERE  CMST.CUSTOMER_MST_PK(+) = JSI.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND POL.PORT_MST_PK = JSI.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK = JSI.PORT_MST_POD_FK";
            Strsql += " AND OMST.OPERATOR_MST_PK(+) = JSI.CARRIER_MST_FK";
            Strsql += "AND jsi.transporter_depot_fk = VMST.VENDOR_MST_PK(+)";
            Strsql += "AND JSI.ARRIVAL_DATE IS NOT NULL";
            Strsql += " and (jsi.created_by_fk=umt.user_mst_pk OR JSI.Last_Modified_By_Fk=UMT.USER_MST_PK)";
            Strsql += " and umt.default_location_fk=" + Loc;
            Strsql += "and jsi.job_card_TRN_pk in";
            Strsql += " (select jc.Job_Card_Trn_Pk";
            Strsql += "  from JOB_CARD_TRN jc";
            Strsql += "   where(jc.arrival_date Is Not null)";
            Strsql += "  and jc.Job_Card_Trn_Pk not in";
            Strsql += " (select d1.job_card_ref_fk";
            Strsql += "   from DOCS_PRINT_DTL_TBL D1";
            Strsql += "   where d1.doc_number lIKE 'RELEASE%'))";
            Strsql += "  and jsi.process_type = 2 ";
            Strsql += "  and jsi.business_type = 2 ";
            Strsql += strCondition;

            Strsql += " ORDER BY ARRIVALDATE DESC,JOBREFNO DESC ";

            Strsql += ")q)WHERE SLNO  Between " + start + " and " + last;
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        #endregion "Fetch For AIR/SEA Import"

        #region "generate protocol for Sea/Air"

        /// <summary>
        /// Generates the sea_ ref_ no.
        /// </summary>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <param name="sPOL">The s pol.</param>
        /// <returns></returns>
        public string GenerateSea_Ref_No(Int64 ILocationId, Int64 IEmployeeId, string sPOL)
        {
            string functionReturnValue = null;
            try
            {
                CREATED_BY = this.CREATED_BY;
                functionReturnValue = GenerateProtocolKey("RELEASE NOTE SEA IMP", ILocationId, IEmployeeId, DateTime.Now, "", "", "", CREATED_BY);
                return functionReturnValue;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        /// <summary>
        /// Generates the air_ ref_ no.
        /// </summary>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <param name="sPOL">The s pol.</param>
        /// <returns></returns>
        public string GenerateAir_Ref_No(Int64 ILocationId, Int64 IEmployeeId, string sPOL)
        {
            string functionReturnValue = null;
            try
            {
                CREATED_BY = this.CREATED_BY;
                functionReturnValue = GenerateProtocolKey("RELEASE NOTE AIR IMP", ILocationId, IEmployeeId, DateTime.Now, "", "", "", CREATED_BY);
                return functionReturnValue;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        #endregion "generate protocol for Sea/Air"

        #region "Save Spl Instr + Warehouse  in DOCS_PRINT_DTL_TBL"

        /// <summary>
        /// Updates the details.
        /// </summary>
        /// <param name="Uwg1">The uwg1.</param>
        /// <param name="ProtocolNo">The protocol no.</param>
        /// <param name="Busitype">The busitype.</param>
        /// <returns></returns>
        public bool UpdateDetails(object Uwg1, string ProtocolNo, int Busitype)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Int16 I = default(Int16);
            OracleTransaction Tran = null;
            try
            {
                ObjWk.OpenConnection();
                Tran = ObjWk.MyConnection.BeginTransaction();
                //for (int I = 0; I <= Uwg1.Rows.Count() - 1; I++)
                //{
                //    if (Uwg1.Rows(I).Cells(Header.SEL].Value) {
                //    var _with1 = ObjWk.MyCommand;

                //    _with1.CommandText = ObjWk.MyUserName + ".RELEASE_NOTE_REP_PKG.DOCS_PRINT_DTL_TBL_INS";
                //    _with1.CommandType = CommandType.StoredProcedure;
                //    _with1.Transaction = Tran;
                //    _with1.Parameters.Clear();

                //    _with1.Parameters.Add("BUSINESS_TYPE_IN", Busitype).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("MODE_IN", 2).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("JOBCARD_REF_PK_IN", Convert.ToInt32(Uwg1.rows(I).cells(Header.JOBCARDPK).value)).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("PROTOCOL_NO_IN", ProtocolNo).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("DEPOT_FK_IN", Convert.ToInt32(Uwg1.rows(I).cells(Header.WarehousePK).value)).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("SPL_INS_IN", (((Uwg1.rows(I).cells(Header.SplInstData).value == null) | Information.IsDBNull(Uwg1.rows(I).cells(Header.SplInstData).value)) ? "" : Uwg1.rows(I).cells(Header.SplInstData).value)).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DOCS_PRINT_DTL_TBL_PK").Direction = ParameterDirection.Output;
                //    _with1.ExecuteNonQuery();

                //}
                //}

                Tran.Commit();
                return true;
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
                ObjWk.CloseConnection();
            }
        }

        #endregion "Save Spl Instr + Warehouse  in DOCS_PRINT_DTL_TBL"

        #region "Save Track N Trace"

        /// <summary>
        /// Saves the track and trace.
        /// </summary>
        /// <param name="jobref">The jobref.</param>
        /// <param name="nlocationfk">The nlocationfk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CREATED_BY">The create d_ by.</param>
        /// <param name="PodID">The pod identifier.</param>
        /// <param name="WareHouse">The ware house.</param>
        /// <returns></returns>
        public object SaveTrackAndTrace(string jobref, int nlocationfk, short BizType, long CREATED_BY, string PodID, string WareHouse)
        {
            string jobPk = null;
            int Cnt = 0;
            string strContData = null;
            string PORTID = null;
            string WAREHOUS = null;
            Array arrJobPk = null;
            Array arrPodID = null;
            Array arrWHouse = null;
            WorkFlow objWk = new WorkFlow();
            objWk.OpenConnection();
            int i = 0;
            int j = 0;
            string onStatus = null;
            try
            {
                onStatus = (BizType == 1 ? "RLS-NOTE-PRNT-AIR-IMP" : "RLS-NOTE-PRNT-SEA-IMP");
                arrJobPk = jobref.Split(',');
                arrPodID = PodID.Split(',');
                arrWHouse = WareHouse.Split(',');
                for (i = 0; i <= arrJobPk.Length - 1; i++)
                {
                    jobPk = Convert.ToString(arrJobPk.GetValue(i));
                    PORTID = Convert.ToString(arrPodID.GetValue(i));
                    WAREHOUS = Convert.ToString(arrWHouse.GetValue(i));
                    if (!string.IsNullOrEmpty(WAREHOUS))
                    {
                        strContData = "Consignment Released at " + WAREHOUS;
                    }
                    else
                    {
                        strContData = "Consignment Released at " + PORTID;
                    }
                    arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(jobPk), BizType, 2, strContData, onStatus, nlocationfk, objWk, "INS", CREATED_BY, "O");
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
            return new object();
        }

        #endregion "Save Track N Trace"

        #region "Change customer from Temp to Permanent "

        /// <summary>
        /// Fetches the temporary customer sea.
        /// </summary>
        /// <param name="JCPk">The jc pk.</param>
        /// <returns></returns>
        public DataSet fetchTempCustSea(string JCPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = "select j.consignee_cust_mst_fk, j.notify1_cust_mst_fk, j.notify2_cust_mst_fk";
            Strsql += " from JOB_CARD_TRN j where j.Job_Card_Trn_Pk in (" + JCPk + ")";
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
        /// Fetches the temporary customer air.
        /// </summary>
        /// <param name="JCPk">The jc pk.</param>
        /// <returns></returns>
        public DataSet fetchTempCustAir(string JCPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select j.consignee_cust_mst_fk, j.notify1_cust_mst_fk, j.notify2_cust_mst_fk";
            Strsql += " from job_card_trn j where j.job_card_trn_pk in (" + JCPk + ")";
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
        /// Fetches the arr date air.
        /// </summary>
        /// <param name="JCNr">The jc nr.</param>
        /// <returns></returns>
        public DataSet fetchArrDateAir(string JCNr)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select t.arrival_date from JOB_CARD_TRN t where t.jobcard_ref_no='" + JCNr + "'";
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
        /// Fetches the arr date sea.
        /// </summary>
        /// <param name="JCNr">The jc nr.</param>
        /// <returns></returns>
        public DataSet fetchArrDateSea(string JCNr)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select t.arrival_date from JOB_CARD_TRN t where t.jobcard_ref_no='" + JCNr + "'";
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

        #endregion "Change customer from Temp to Permanent "

        #region "fetchCommDetails"

        /// <summary>
        /// Fetches the comm details.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet fetchCommDetails(string jobcardpk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow Objwk = new WorkFlow();

            sb.Append(" SELECT CM.COMMODITY_NAME,JT.VOLUME_IN_CBM,");
            sb.Append("       jt.pack_count as GROSS_WEIGHT,");
            sb.Append("       JT.NET_WEIGHT,");
            sb.Append("       JT.CHARGEABLE_WEIGHT");
            sb.Append("   FROM JOB_CARD_TRN JC,");
            sb.Append("       JOB_TRN_CONT JT,");
            sb.Append("       COMMODITY_MST_TBL    CM");
            sb.Append("  WHERE JC.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK      ");
            sb.Append("   AND CM.COMMODITY_MST_PK = JT.COMMODITY_MST_FK");

            sb.Append("   AND JC.JOB_CARD_TRN_PK IN (" + jobcardpk + ")");
            sb.Append("");

            try
            {
                return Objwk.GetDataSet(sb.ToString());
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

        #endregion "fetchCommDetails"

        #region "Fetch data for Sea/Air Release note"

        /// <summary>
        /// Fetches the job card sea details.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <param name="protocol">The protocol.</param>
        /// <returns></returns>
        public DataSet FetchJobCardSeaDetails(string jobcardpk, string protocol)
        {
            System.Text.StringBuilder Strsql = new System.Text.StringBuilder();
            WorkFlow Objwk = new WorkFlow();
            Strsql.Append(" select  JSI.JOB_CARD_TRN_PK AS JOBCARDPK,");
            Strsql.Append(" JSI.JOBCARD_REF_NO AS JOBCARDREFNO,");
            Strsql.Append(" JSI.UCR_NO AS UCRNO,");
            Strsql.Append(" JSI.VESSEL_NAME AS VSL_FLIGHT,");
            Strsql.Append(" JSI.HBL_HAWB_REF_NO AS HBL_HAWB_REFNO,");
            Strsql.Append(" JSI.MBL_MAWB_REF_NO AS MBL_MAWB_REFNO,");
            Strsql.Append(" POL.PORT_NAME AS POLNAME,");
            Strsql.Append(" POD.PORT_NAME AS PODNAME,");

            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASE_PARTY.CUSTOMER_NAME");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUST.CUSTOMER_NAME  END) CONSIGNAME,");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_ADDRESS_1");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUSTDTLS.ADM_ADDRESS_1 END) ADDCONSIG1,");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_ADDRESS_2");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUSTDTLS.ADM_ADDRESS_2 END) ADDCONSIG2,");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_ADDRESS_3");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUSTDTLS.ADM_ADDRESS_3 END) ADDCONSIG3,");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_CITY");
            Strsql.Append("  ELSE");
            Strsql.Append("   CUSTDTLS.ADM_CITY END) CITY,");
            Strsql.Append("  (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_ZIP_CODE");
            Strsql.Append("  ELSE");
            Strsql.Append(" CUSTDTLS.ADM_ZIP_CODE END) ZIPCONSIG,");

            Strsql.Append("(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_PHONE_NO_1");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUSTDTLS.ADM_PHONE_NO_1 END) CONSIGPHONE,");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_FAX_NO");
            Strsql.Append("  ELSE");
            Strsql.Append(" CUSTDTLS.ADM_FAX_NO END) CONSIGFAX,");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_EMAIL_ID");
            Strsql.Append("  ELSE");
            Strsql.Append("  CUSTDTLS.ADM_EMAIL_ID END) CONSIGEMAIL,");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RCOUNTRY.COUNTRY_NAME");
            Strsql.Append(" ELSE");
            Strsql.Append("  CONSIGCOUNTRY.COUNTRY_NAME END) CONSIGCOUNTRY,");
            Strsql.Append(" VMST.VENDOR_NAME DEPOTNAME,");
            Strsql.Append("       VDTLS.ADM_ADDRESS_1 DADDRESS1,");
            Strsql.Append("       VDTLS.ADM_ADDRESS_2 DADDRESS2,");
            Strsql.Append("       VDTLS.ADM_ADDRESS_3 DADDRESS3,");
            Strsql.Append("       VDTLS.ADM_CITY DCITY,");
            Strsql.Append("       VDTLS.ADM_ZIP_CODE DZIP,");
            Strsql.Append("       VDTLS.ADM_PHONE DPHONE,");
            Strsql.Append("       VDTLS.ADM_FAX_NO DFAX,");
            Strsql.Append("       VDTLS.ADM_EMAIL_ID DEMAIL,");
            Strsql.Append("       VCOUNTRY.COUNTRY_NAME DCOUNTRY,");

            Strsql.Append(" DOCS.SPECIAL_INSTRUCTION AS SPE_INS,");
            Strsql.Append(" JSI.GOODS_DESCRIPTION GOODS,");
            Strsql.Append(" JC.CONTAINER_NUMBER,");
            Strsql.Append(" TO_CHAR(JSI.CARGO_TYPE, '9') AS CONTAINER_TYPE_MST_ID,");
            Strsql.Append(" CMST.CONTAINER_TYPE_NAME,");
            Strsql.Append(" SUM(JC.VOLUME_IN_CBM) AS VOLUME,");
            Strsql.Append(" SUM(JC.GROSS_WEIGHT) AS GROSSWT");
            Strsql.Append(" from JOB_CARD_TRN JSI,");
            Strsql.Append(" JOB_TRN_CONT JC,");
            Strsql.Append(" PORT_MST_TBL POL,");
            Strsql.Append(" PORT_MST_TBL POD,");
            Strsql.Append(" CUSTOMER_MST_TBL CUST,");
            Strsql.Append(" CUSTOMER_CONTACT_DTLS CUSTDTLS,");
            Strsql.Append(" CUSTOMER_MST_TBL RELEASE_PARTY,");
            Strsql.Append(" CUSTOMER_CONTACT_DTLS RELEASEDTLS,");
            Strsql.Append(" COUNTRY_MST_TBL RCOUNTRY,");
            Strsql.Append(" DOCS_PRINT_DTL_TBL DOCS,");

            Strsql.Append(" VENDOR_MST_TBL VMST,");
            Strsql.Append(" VENDOR_CONTACT_DTLS VDTLS,");
            Strsql.Append(" COUNTRY_MST_TBL VCOUNTRY,");
            Strsql.Append(" COUNTRY_MST_TBL CONSIGCOUNTRY,");
            Strsql.Append(" CONTAINER_TYPE_MST_TBL CMST");
            Strsql.Append(" WHERE POL.PORT_MST_PK = JSI.PORT_MST_POL_FK");
            Strsql.Append(" AND CMST.CONTAINER_TYPE_MST_PK(+) = JC.CONTAINER_TYPE_MST_FK");
            Strsql.Append(" AND JSI.JOB_CARD_TRN_PK=JC.JOB_CARD_TRN_FK(+)");

            Strsql.Append(" AND DOCS.JOB_CARD_REF_FK=JSI.JOB_CARD_TRN_PK");
            Strsql.Append(" AND VMST.VENDOR_MST_PK(+) = DOCS.DEPOT_MST_FK");
            Strsql.Append(" AND VDTLS.VENDOR_MST_FK(+)=VMST.VENDOR_MST_PK");
            Strsql.Append(" AND VCOUNTRY.COUNTRY_MST_PK(+)=VDTLS.ADM_COUNTRY_MST_FK");
            Strsql.Append(" AND POD.PORT_MST_PK=JSI.PORT_MST_POD_FK");
            Strsql.Append(" AND CUST.CUSTOMER_MST_PK(+)=JSI.CONSIGNEE_CUST_MST_FK");
            Strsql.Append(" AND CUSTDTLS.CUSTOMER_MST_FK(+)=CUST.CUSTOMER_MST_PK");
            Strsql.Append(" AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CUSTDTLS.ADM_COUNTRY_MST_FK");
            Strsql.Append(" AND JSI.REL_PARTY_CUST_MST_FK=RELEASE_PARTY.CUSTOMER_MST_PK(+)");
            Strsql.Append(" AND RELEASE_PARTY.CUSTOMER_MST_PK=RELEASEDTLS.CUSTOMER_MST_FK(+)");
            Strsql.Append(" AND RELEASEDTLS.ADM_COUNTRY_MST_FK=RCOUNTRY.COUNTRY_MST_PK(+)");
            Strsql.Append(" AND DOCS.DOC_NUMBER='" + protocol + "'");
            Strsql.Append(" AND JSI.JOB_CARD_TRN_PK IN (" + jobcardpk + ")");
            Strsql.Append(" GROUP BY");
            Strsql.Append(" JSI.JOB_CARD_TRN_PK ,");
            Strsql.Append(" JSI.JOBCARD_REF_NO ,");
            Strsql.Append(" JSI.UCR_NO,");
            Strsql.Append(" JSI.VESSEL_NAME,");
            Strsql.Append(" JSI.HBL_HAWB_REF_NO,");
            Strsql.Append(" JSI.MBL_MAWB_REF_NO,");
            Strsql.Append(" POL.PORT_NAME,");
            Strsql.Append(" POD.PORT_NAME,");

            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASE_PARTY.CUSTOMER_NAME");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUST.CUSTOMER_NAME  END),");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_ADDRESS_1");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUSTDTLS.ADM_ADDRESS_1 END),");
            Strsql.Append("(CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_ADDRESS_2");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUSTDTLS.ADM_ADDRESS_2 END),");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_ADDRESS_3");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUSTDTLS.ADM_ADDRESS_3 END),");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_CITY");
            Strsql.Append("  ELSE");
            Strsql.Append("  CUSTDTLS.ADM_CITY END),");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_ZIP_CODE");
            Strsql.Append("  ELSE");
            Strsql.Append(" CUSTDTLS.ADM_ZIP_CODE END),");

            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_PHONE_NO_1");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUSTDTLS.ADM_PHONE_NO_1 END) ,");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RELEASEDTLS.ADM_FAX_NO");
            Strsql.Append("  ELSE");
            Strsql.Append(" CUSTDTLS.ADM_FAX_NO END),");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append("  RELEASEDTLS.ADM_EMAIL_ID");
            Strsql.Append(" ELSE");
            Strsql.Append(" CUSTDTLS.ADM_EMAIL_ID END),");
            Strsql.Append(" (CASE WHEN JSI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN");
            Strsql.Append(" RCOUNTRY.COUNTRY_NAME");
            Strsql.Append("  ELSE");
            Strsql.Append("  CONSIGCOUNTRY.COUNTRY_NAME END),");
            Strsql.Append(" VMST.VENDOR_NAME,");
            Strsql.Append("          VDTLS.ADM_ADDRESS_1,");
            Strsql.Append("          VDTLS.ADM_ADDRESS_2,");
            Strsql.Append("          VDTLS.ADM_ADDRESS_3,");
            Strsql.Append("          VDTLS.ADM_CITY,");
            Strsql.Append("          VDTLS.ADM_ZIP_CODE,");
            Strsql.Append("          VDTLS.ADM_PHONE,");
            Strsql.Append("          VDTLS.ADM_FAX_NO,");
            Strsql.Append("          VDTLS.ADM_EMAIL_ID,");
            Strsql.Append("          VCOUNTRY.COUNTRY_NAME,");

            Strsql.Append(" DOCS.SPECIAL_INSTRUCTION ,");
            Strsql.Append(" JSI.GOODS_DESCRIPTION,");
            Strsql.Append(" JC.CONTAINER_NUMBER,");
            Strsql.Append(" JSI.CARGO_TYPE,");
            Strsql.Append(" CMST.CONTAINER_TYPE_NAME");

            try
            {
                return Objwk.GetDataSet(Strsql.ToString());
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
        /// Fetches the job card air details.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <param name="protocol">The protocol.</param>
        /// <returns></returns>
        public DataSet FetchJobCardAirDetails(string jobcardpk, string protocol)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select  JAI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += " JAI.JOBCARD_REF_NO AS JOBCARDREFNO,";
            Strsql += " JAI.UCR_NO AS UCRNO,";
            Strsql += " JAI.VOYAGE_FLIGHT_NO AS VSL_FLIGHT,";
            Strsql += " JAI.HBL_HAWB_REF_NO AS HBL_HAWB_REFNO,";
            Strsql += " JAI.MBL_MAWB_REF_NO AS MBL_MAWB_REFNO,";
            Strsql += " POL.PORT_NAME AS POLNAME,";
            Strsql += " POD.PORT_NAME AS PODNAME,";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += " RELEASE_PARTY.CUSTOMER_NAME";
            Strsql += "ELSE";
            Strsql += " CUST.CUSTOMER_NAME  END) CONSIGNAME,";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += " RELEASEDTLS.ADM_ADDRESS_1";
            Strsql += " ELSE";
            Strsql += " CUSTDTLS.ADM_ADDRESS_1 END) ADDCONSIG1,";
            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_ADDRESS_2";
            Strsql += " ELSE";
            Strsql += " CUSTDTLS.ADM_ADDRESS_2 END) ADDCONSIG2,";
            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_ADDRESS_3";
            Strsql += "ELSE";
            Strsql += "CUSTDTLS.ADM_ADDRESS_3 END) ADDCONSIG3,";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += " RELEASEDTLS.ADM_CITY";
            Strsql += " ELSE";
            Strsql += "  CUSTDTLS.ADM_CITY END) CITY,";
            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_ZIP_CODE";
            Strsql += "  ELSE";
            Strsql += " CUSTDTLS.ADM_ZIP_CODE END) ZIPCONSIG,";

            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_PHONE_NO_1";
            Strsql += " ELSE";
            Strsql += " CUSTDTLS.ADM_PHONE_NO_1 END) CONSIGPHONE,";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_FAX_NO";
            Strsql += " ELSE";
            Strsql += "CUSTDTLS.ADM_FAX_NO END) CONSIGFAX,";
            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "  RELEASEDTLS.ADM_EMAIL_ID";
            Strsql += " ELSE";
            Strsql += " CUSTDTLS.ADM_EMAIL_ID END) CONSIGEMAIL,";
            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RCOUNTRY.COUNTRY_NAME";
            Strsql += "ELSE";
            Strsql += "CONSIGCOUNTRY.COUNTRY_NAME END) CONSIGCOUNTRY,";

            Strsql += "VMST.VENDOR_NAME AS DEPOTNAME,";
            Strsql += "VDTLS.ADM_ADDRESS_1 AS DADDRESS1,";
            Strsql += "VDTLS.ADM_ADDRESS_2 AS DADDRESS2,";
            Strsql += "VDTLS.ADM_ADDRESS_3 AS DADDRESS3,";
            Strsql += "VDTLS.ADM_CITY AS DCITY,";
            Strsql += "VDTLS.ADM_ZIP_CODE AS DZIP,";
            Strsql += "VDTLS.ADM_PHONE AS DPHONE,";
            Strsql += "VDTLS.ADM_FAX_NO   AS DFAX,";
            Strsql += "VDTLS.ADM_EMAIL_ID AS DEMAIL,";
            Strsql += "VCOUNTRY.COUNTRY_NAME DCOUNTRY,";
            Strsql += "DOCS.SPECIAL_INSTRUCTION AS SPE_INS,";
            Strsql += " JAI.GOODS_DESCRIPTION GOODS,";
            Strsql += "(SELECT ROWTOCOL('SELECT C.COMMODITY_NAME FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_MST_PK IN (' || NVL(JAIC.COMMODITY_MST_FKS, -1) || ')') FROM DUAL) CONTAINER_NUMBER,";
            Strsql += "'' CONTAINER_TYPE_MST_ID,";
            Strsql += " JAIC.PALETTE_SIZE CONTAINER_TYPE_NAME,";
            Strsql += "SUM(JAIC.VOLUME_IN_CBM) VOLUME,";
            Strsql += "SUM(JAIC.GROSS_WEIGHT) GROSSWT";
            Strsql += " from JOB_CARD_TRN JAI,";
            Strsql += "JOB_TRN_CONT JAIC,";
            Strsql += "PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD,";
            Strsql += "CUSTOMER_MST_TBL CUST,";
            Strsql += "CUSTOMER_CONTACT_DTLS CUSTDTLS,";
            Strsql += "DOCS_PRINT_DTL_TBL DOCS,";
            Strsql += "VENDOR_MST_TBL VMST,";
            Strsql += "VENDOR_CONTACT_DTLS VDTLS,";
            Strsql += "COUNTRY_MST_TBL VCOUNTRY,";
            Strsql += " COUNTRY_MST_TBL CONSIGCOUNTRY,";
            Strsql += "CUSTOMER_MST_TBL RELEASE_PARTY,";
            Strsql += "CUSTOMER_CONTACT_DTLS RELEASEDTLS,";
            Strsql += "COUNTRY_MST_TBL RCOUNTRY";
            Strsql += " WHERE POL.PORT_MST_PK = JAI.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK=JAI.PORT_MST_POD_FK";
            Strsql += "AND CUST.CUSTOMER_MST_PK(+)=JAI.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND CUSTDTLS.CUSTOMER_MST_FK(+)=CUST.CUSTOMER_MST_PK";
            Strsql += "AND DOCS.JOB_CARD_REF_FK=JAI.JOB_CARD_TRN_PK";
            Strsql += "AND VMST.VENDOR_MST_PK=DOCS.DEPOT_MST_FK";
            Strsql += "AND VDTLS.VENDOR_MST_FK(+)=VMST.VENDOR_MST_PK";
            Strsql += "AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CUSTDTLS.ADM_COUNTRY_MST_FK";
            Strsql += "AND VCOUNTRY.COUNTRY_MST_PK(+)=VDTLS.ADM_COUNTRY_MST_FK";
            Strsql += "AND JAIC.JOB_CARD_TRN_FK(+)=JAI.JOB_CARD_TRN_PK";
            Strsql += "AND JAI.REL_PARTY_CUST_MST_FK=RELEASE_PARTY.CUSTOMER_MST_PK(+)";
            Strsql += " AND RELEASE_PARTY.CUSTOMER_MST_PK=RELEASEDTLS.CUSTOMER_MST_FK(+)";
            Strsql += " AND RELEASEDTLS.ADM_COUNTRY_MST_FK=RCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "AND DOCS.DOC_NUMBER='" + protocol + "'";
            Strsql += "AND JAI.JOB_CARD_TRN_PK IN (" + jobcardpk + ")";
            Strsql += "GROUP BY";
            Strsql += "JAI.Job_Card_Trn_Pk ,";
            Strsql += "JAI.JOBCARD_REF_NO,";
            Strsql += "JAI.UCR_NO,";
            Strsql += "JAI.VOYAGE_FLIGHT_NO,";
            Strsql += "JAI.HBL_HAWB_REF_NO ,";
            Strsql += "JAI.MBL_MAWB_REF_NO,";
            Strsql += "POL.PORT_NAME,";
            Strsql += "POD.PORT_NAME,";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASE_PARTY.CUSTOMER_NAME";
            Strsql += "ELSE";
            Strsql += "CUST.CUSTOMER_NAME  END),";
            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_ADDRESS_1";
            Strsql += "ELSE";
            Strsql += "CUSTDTLS.ADM_ADDRESS_1 END) ,";
            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_ADDRESS_2";
            Strsql += "ELSE";
            Strsql += " CUSTDTLS.ADM_ADDRESS_2 END) ,";
            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_ADDRESS_3";
            Strsql += " ELSE";
            Strsql += " CUSTDTLS.ADM_ADDRESS_3 END),";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += " RELEASEDTLS.ADM_CITY";
            Strsql += "  ELSE";
            Strsql += "  CUSTDTLS.ADM_CITY END),";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += " RELEASEDTLS.ADM_ZIP_CODE";
            Strsql += " ELSE";
            Strsql += "CUSTDTLS.ADM_ZIP_CODE END),";

            Strsql += "(CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_PHONE_NO_1";
            Strsql += " ELSE";
            Strsql += " CUSTDTLS.ADM_PHONE_NO_1 END),";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += " RELEASEDTLS.ADM_FAX_NO";
            Strsql += "  ELSE";
            Strsql += " CUSTDTLS.ADM_FAX_NO END) ,";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RELEASEDTLS.ADM_EMAIL_ID";
            Strsql += "  ELSE";
            Strsql += "  CUSTDTLS.ADM_EMAIL_ID END),";
            Strsql += " (CASE WHEN JAI.REL_PARTY_CUST_MST_FK IS NOT NULL THEN";
            Strsql += "RCOUNTRY.COUNTRY_NAME";
            Strsql += " ELSE";
            Strsql += " CONSIGCOUNTRY.COUNTRY_NAME END) ,";

            Strsql += " VMST.VENDOR_NAME,";
            Strsql += " VDTLS.ADM_ADDRESS_1,";
            Strsql += "VDTLS.ADM_ADDRESS_2,";
            Strsql += "VDTLS.ADM_ADDRESS_3 ,";
            Strsql += "VDTLS.ADM_CITY,";
            Strsql += "VDTLS.ADM_ZIP_CODE,";
            Strsql += "VDTLS.ADM_PHONE,";
            Strsql += "VDTLS.ADM_FAX_NO ,";
            Strsql += "VDTLS.ADM_EMAIL_ID,";
            Strsql += "VCOUNTRY.COUNTRY_NAME,";
            Strsql += "  DOCS.SPECIAL_INSTRUCTION,";
            Strsql += "JAI.GOODS_DESCRIPTION,";
            Strsql += "JAIC.PALETTE_SIZE, JAIC.COMMODITY_MST_FKS ";

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

        #endregion "Fetch data for Sea/Air Release note"

        #region "Fetch Sea/Air Container Details"

        /// <summary>
        /// Fetches the sea container DTLS.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchSeaContainerDtls(string jobcardpk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select ROWNUM SNO,JSIC.JOB_TRN_CONT_PK JOBCARDCONTPK,";
            Strsql += "JSIC.CONTAINER_NUMBER,";
            Strsql += "jsic.job_card_TRN_fk JOBSPK,";
            Strsql += "CMST.CONTAINER_TYPE_MST_ID,";
            Strsql += "CMST.CONTAINER_TYPE_NAME";
            Strsql += "from   JOB_TRN_CONT JSIC,";
            Strsql += "CONTAINER_TYPE_MST_TBL CMST";
            Strsql += "WHERE CMST.CONTAINER_TYPE_MST_PK = JSIC.CONTAINER_TYPE_MST_FK";
            Strsql += "AND JSIC.JOB_CARD_TRN_FK IN (" + jobcardpk + ") ";
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        /// <summary>
        /// Fetches the air container DTLS.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchAirContainerDtls(string jobcardpk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select ROWNUM SNO,JAIC.JOB_TRN_CONT_PK JOBCARDCONTPK,";
            Strsql += "JAIC.PALETTE_SIZE CONTAINER_NUMBER,";
            Strsql += "jAic.job_card_TRN_fk JOBSPK,";
            Strsql += "'' CONTAINER_TYPE_MST_ID,";
            Strsql += "'' CONTAINER_TYPE_NAME";
            Strsql += "from JOB_TRN_CONT JAIC";
            Strsql += "WHERE";
            Strsql += " JAIC.JOB_CARD_TRN_FK IN (" + jobcardpk + ") ";
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        #endregion "Fetch Sea/Air Container Details"

        #region "FetchSeaSummary"

        /// <summary>
        /// Fetches the sea summary.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchSeaSummary(string jobcardpk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT SUM(JC.VOLUME_IN_CBM) AS VOLUME,SUM(JC.GROSS_WEIGHT) AS GROSSWT,JS.JOB_CARD_TRN_PK AS JOBPK";
            Strsql += " FROM JOB_TRN_CONT JC,JOB_CARD_TRN JS";
            Strsql += " WHERE JC.JOB_CARD_TRN_FK IN (" + jobcardpk + ")";
            Strsql += " AND JS.JOB_CARD_TRN_PK=JC.JOB_CARD_TRN_FK";
            Strsql += " GROUP BY JS.JOB_CARD_TRN_PK";
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchSeaSummary"

        #region "FetchAirSummary"

        /// <summary>
        /// Fetches the air summary.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchAirSummary(string jobcardpk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT SUM(JC.VOLUME_IN_CBM) AS VOLUME,SUM(JC.GROSS_WEIGHT) AS GROSSWT,JA.JOB_CARD_TRN_PK AS JOBPK";
            Strsql += " FROM JOB_TRN_CONT JC,JOB_CARD_TRN JA";
            Strsql += " WHERE JC.JOB_CARD_TRN_FK IN (" + jobcardpk + ")";
            Strsql += " AND JA.JOB_CARD_TRN_PK=JC.JOB_CARD_TRN_FK";
            Strsql += " GROUP BY JA.JOB_CARD_TRN_PK";
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchAirSummary"

        #region "FetchSeaDesc"

        /// <summary>
        /// Fetches the sea desc.
        /// </summary>
        /// <param name="JobRefNos">The job reference nos.</param>
        /// <returns></returns>
        public DataSet FetchSeaDesc(string JobRefNos)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT JSI.JOB_CARD_TRN_PK JOBPK,JSI.GOODS_DESCRIPTION,JSI.JOBCARD_REF_NO";
            Strsql += "FROM JOB_CARD_TRN JSI";
            Strsql += "WHERE JSI.JOB_CARD_TRN_PK IN(" + JobRefNos + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchSeaDesc"

        #region "FetchAirDesc"

        /// <summary>
        /// Fetches the air desc.
        /// </summary>
        /// <param name="JobRefNos">The job reference nos.</param>
        /// <returns></returns>
        public DataSet FetchAirDesc(string JobRefNos)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT JAI.JOB_CARD_TRN_PK JOBPK, JAI.GOODS_DESCRIPTION, JAI.JOBCARD_REF_NO";
            Strsql += "FROM JOB_CARD_TRN JAI";
            Strsql += "WHERE JAI.JOB_CARD_TRN_PK IN(" + JobRefNos + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchAirDesc"
    }
}