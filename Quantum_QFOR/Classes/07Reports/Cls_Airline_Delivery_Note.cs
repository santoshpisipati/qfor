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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_Airline_Delivery_Note : CommonFeatures
    {
        /// <summary>
        /// The object track n trace
        /// </summary>
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        #region "Fetch For AIR Export"

        /// <summary>
        /// Fetches the air user export.
        /// </summary>
        /// <param name="Flight">The flight.</param>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="PodPk">The pod pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="strLocPk">The string loc pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAirUserExport(string Flight = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, long strLocPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND UPPER(JA.VOYAGE_FLIGHT_NO) LIKE '%" + Flight.ToUpper() + "%'";
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JA.JOB_CARD_TRN_PK = " + JobPk;
            }

            if (PolPk > 0)
            {
                strCondition = strCondition + " And BA.PORT_MST_POl_FK = " + PolPk;
            }

            if (PodPk > 0)
            {
                strCondition = strCondition + " And BA.PORT_MST_POD_FK = " + PodPk;
            }

            if (CustPk > 0)
            {
                strCondition = strCondition + " AND C.CUSTOMER_MST_PK=" + CustPk;
            }

            if (strLocPk > 0)
            {
                strCondition = strCondition + " AND LMT.LOCATION_MST_PK=" + strLocPk;
            }
            strCondition = strCondition + " and ja.BUSINESS_TYPE = 1 ";

            Strsql = " SELECT Count(*) FROM(";
            Strsql += " SELECT JA.JOB_CARD_TRN_PK AS JOBCARDPK ,";
            Strsql += " JA.BOOKING_MST_FK AS BOOKINGPK,JA.JOBCARD_REF_NO AS JOBREF, ";
            Strsql += " C.CUSTOMER_MST_PK CPK,";
            Strsql += " C.CUSTOMER_NAME CUSTOMER,";
            Strsql += " JA.VOYAGE_FLIGHT_NO AS FLIGHT,";
            Strsql += " HA.HAWB_EXP_TBL_PK AS HAWB_PK,HA.HAWB_REF_NO AS HAWB_REF,";
            Strsql += " MA.MAWB_EXP_TBL_PK AS MAWB_PK,MA.MAWB_REF_NO AS MAWB_REF,";
            Strsql += " '' AS DEPOT_PK,'' AS DEPOT_NAME,";
            Strsql += "  '' AS TRANSPORTER_PK,'' AS TRANSPORTER_NAME,'' AS SPL_INSTR,";
            Strsql += " PO.PORT_ID AS POD,'' AS SEL,DP.SPECIAL_INSTRUCTION AS SI";
            Strsql += " FROM JOB_CARD_TRN JA,HAWB_EXP_TBL HA,MAWB_EXP_TBL MA,";
            Strsql += " BOOKING_MST_TBL BA,PORT_MST_TBL POL,";
            Strsql += "  PORT_MST_TBL PO,CUSTOMER_MST_TBL C,DOCS_PRINT_DTL_TBL DP";
            Strsql += "  ,VENDOR_MST_TBL T,VENDOR_MST_TBL D, USER_MST_TBL UMT, LOCATION_MST_TBL LMT";
            Strsql += " WHERE  JA.BOOKING_MST_FK=BA.BOOKING_MST_PK";
            Strsql += " AND HA.JOB_CARD_AIR_EXP_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += "  AND JA.MBL_MAWB_FK = MA.MAWB_EXP_TBL_PK(+) ";
            Strsql += " AND PO.PORT_MST_PK=BA.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK=BA.PORT_MST_POL_FK";
            Strsql += " AND C.CUSTOMER_MST_PK(+)=JA.CONSIGNEE_CUST_MST_FK ";
            Strsql += " AND JA.TRANSPORTER_DEPOT_FK=T.VENDOR_MST_PK(+)";
            Strsql += " AND DP.DEPOT_MST_FK=D.VENDOR_MST_PK(+)";
            Strsql += " AND DP.JOB_CARD_REF_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += " AND JA.CREATED_BY_FK = UMT.USER_MST_PK";
            Strsql += " AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK";
            Strsql += " AND DP.DOC_NUMBER(+) LIKE 'AIRLINE%' ";
            Strsql += strCondition;
            Strsql += " and dP.doc_number in";
            Strsql += "(select max(d1.doc_number)";
            Strsql += " from DOCS_PRINT_DTL_TBL D1";
            Strsql += " where d1.job_card_ref_fk = jA.JOB_CARD_TRN_PK and D1.DOC_NUMBER(+) LIKE 'AIRLINE%')";

            Strsql += " union";
            Strsql += " SELECT JA.JOB_CARD_TRN_PK AS JOBCARDPK ,";
            Strsql += " JA.BOOKING_MST_FK AS BOOKINGPK,JA.JOBCARD_REF_NO AS JOBREF, ";
            Strsql += " C.CUSTOMER_MST_PK CPK,";
            Strsql += " C.CUSTOMER_NAME CUSTOMER,";
            Strsql += " JA.VOYAGE_FLIGHT_NO AS FLIGHT,";
            Strsql += " HA.HAWB_EXP_TBL_PK AS HAWB_PK,HA.HAWB_REF_NO AS HAWB_REF,";
            Strsql += " MA.MAWB_EXP_TBL_PK AS MAWB_PK,MA.MAWB_REF_NO AS MAWB_REF,";
            Strsql += " '' AS DEPOT_PK,'' AS DEPOT_NAME,";
            Strsql += "  '' AS TRANSPORTER_PK,'' AS TRANSPORTER_NAME,'' AS SPL_INSTR,";
            Strsql += " PO.PORT_ID AS POD,'' AS SEL,DP.SPECIAL_INSTRUCTION AS SI";
            Strsql += " FROM JOB_CARD_TRN JA,HAWB_EXP_TBL HA,MAWB_EXP_TBL MA,";
            Strsql += " BOOKING_MST_TBL BA,PORT_MST_TBL POL,";
            Strsql += "  PORT_MST_TBL PO,CUSTOMER_MST_TBL C,DOCS_PRINT_DTL_TBL DP";
            Strsql += "  ,VENDOR_MST_TBL T,VENDOR_MST_TBL D,USER_MST_TBL UMT, LOCATION_MST_TBL LMT";
            Strsql += " WHERE  JA.BOOKING_MST_FK=BA.BOOKING_MST_PK";
            Strsql += " AND HA.JOB_CARD_AIR_EXP_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += "  AND JA.MBL_MAWB_FK = MA.MAWB_EXP_TBL_PK(+) ";
            Strsql += " AND PO.PORT_MST_PK=BA.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK=BA.PORT_MST_POL_FK";
            Strsql += " AND C.CUSTOMER_MST_PK(+)=JA.CONSIGNEE_CUST_MST_FK ";
            Strsql += " AND JA.TRANSPORTER_DEPOT_FK=T.VENDOR_MST_PK(+)";
            Strsql += " AND DP.DEPOT_MST_FK=D.VENDOR_MST_PK(+)";
            Strsql += " AND DP.JOB_CARD_REF_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += " AND JA.CREATED_BY_FK = UMT.USER_MST_PK";
            Strsql += " AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK";
            Strsql += " AND DP.DOC_NUMBER(+) LIKE 'AIRLINE%' ";
            Strsql += "and jA.JOB_CARD_TRN_PK in";
            Strsql += "(select jc.JOB_CARD_TRN_PK";
            Strsql += " from JOB_CARD_TRN jc";
            Strsql += "where";
            Strsql += "   jc.JOB_CARD_TRN_PK not in";
            Strsql += " (select d1.job_card_ref_fk";
            Strsql += "  from DOCS_PRINT_DTL_TBL D1";
            Strsql += " where d1.doc_number lIKE 'AIRLINE%')";
            Strsql += "  )";
            Strsql += strCondition;
            Strsql += "  )";
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
            Strsql += " SELECT JA.JOB_CARD_TRN_PK AS JOBCARDPK ,";
            Strsql += " JA.BOOKING_MST_FK AS BOOKINGPK,JA.JOBCARD_REF_NO AS JOBREF, ";
            Strsql += " HA.HAWB_EXP_TBL_PK AS HAWB_PK,HA.HAWB_REF_NO AS HAWB_REF,";
            Strsql += " MA.MAWB_EXP_TBL_PK AS MAWB_PK,MA.MAWB_REF_NO AS MAWB_REF,";
            Strsql += " C.CUSTOMER_MST_PK CPK,";
            Strsql += " C.CUSTOMER_NAME CUSTOMER,";
            Strsql += " JA.VOYAGE_FLIGHT_NO AS FLIGHT,";
            Strsql += " PO.PORT_ID AS POD,";
            Strsql += " '' AS DEPOT_PK,'' AS DEPOT_NAME,";
            Strsql += "  ''AS TRANSPORTER_PK,'' AS TRANSPORTER_NAME,'' AS SPL_INSTR,";
            Strsql += " '' AS SEL,DP.SPECIAL_INSTRUCTION AS SI";
            Strsql += " FROM JOB_CARD_TRN JA,HAWB_EXP_TBL HA,MAWB_EXP_TBL MA,";
            Strsql += " BOOKING_MST_TBL BA,PORT_MST_TBL POL,";
            Strsql += "  PORT_MST_TBL PO,CUSTOMER_MST_TBL C,DOCS_PRINT_DTL_TBL DP";
            Strsql += "  ,VENDOR_MST_TBL T,VENDOR_MST_TBL D,USER_MST_TBL UMT, LOCATION_MST_TBL LMT";
            Strsql += " WHERE  JA.BOOKING_MST_FK=BA.BOOKING_MST_PK";
            Strsql += " AND HA.JOB_CARD_AIR_EXP_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += "  AND JA.MBL_MAWB_FK = MA.MAWB_EXP_TBL_PK(+) ";
            Strsql += " AND PO.PORT_MST_PK=BA.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK=BA.PORT_MST_POL_FK";
            Strsql += " AND C.CUSTOMER_MST_PK(+)=JA.CONSIGNEE_CUST_MST_FK ";
            Strsql += " AND JA.TRANSPORTER_DEPOT_FK=T.VENDOR_MST_PK(+)";
            Strsql += " AND DP.DEPOT_MST_FK=D.VENDOR_MST_PK(+)";
            Strsql += " AND DP.JOB_CARD_REF_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += " AND JA.CREATED_BY_FK = UMT.USER_MST_PK";
            Strsql += " AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK";
            Strsql += " AND DP.DOC_NUMBER(+) LIKE 'AIRLINE%' ";
            Strsql += strCondition;
            Strsql += " and dP.doc_number in";
            Strsql += "(select max(d1.doc_number)";
            Strsql += " from DOCS_PRINT_DTL_TBL D1";
            Strsql += " where d1.job_card_ref_fk = jA.JOB_CARD_TRN_PK and D1.DOC_NUMBER(+) LIKE 'AIRLINE%')";

            Strsql += " union";
            Strsql += " SELECT JA.JOB_CARD_TRN_PK AS JOBCARDPK ,";
            Strsql += " JA.BOOKING_MST_FK AS BOOKINGPK,JA.JOBCARD_REF_NO AS JOBREF, ";
            Strsql += " HA.HAWB_EXP_TBL_PK AS HAWB_PK,HA.HAWB_REF_NO AS HAWB_REF,";
            Strsql += " MA.MAWB_EXP_TBL_PK AS MAWB_PK,MA.MAWB_REF_NO AS MAWB_REF,";
            Strsql += " C.CUSTOMER_MST_PK CPK,";
            Strsql += " C.CUSTOMER_NAME CUSTOMER,";
            Strsql += " JA.VOYAGE_FLIGHT_NO AS FLIGHT,";
            Strsql += " PO.PORT_ID AS POD,";
            Strsql += " '' AS DEPOT_PK,'' AS DEPOT_NAME,";
            Strsql += "  '' AS TRANSPORTER_PK,'' AS TRANSPORTER_NAME,'' AS SPL_INSTR,";
            Strsql += " '' AS SEL,DP.SPECIAL_INSTRUCTION AS SI";
            Strsql += " FROM JOB_CARD_TRN JA,HAWB_EXP_TBL HA,MAWB_EXP_TBL MA,";
            Strsql += " BOOKING_MST_TBL BA,PORT_MST_TBL POL,";
            Strsql += "  PORT_MST_TBL PO,CUSTOMER_MST_TBL C,DOCS_PRINT_DTL_TBL DP";
            Strsql += "  ,VENDOR_MST_TBL T,VENDOR_MST_TBL D,USER_MST_TBL UMT, LOCATION_MST_TBL LMT";
            Strsql += " WHERE  JA.BOOKING_MST_FK=BA.BOOKING_MST_PK";
            Strsql += " AND HA.JOB_CARD_AIR_EXP_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += "  AND JA.MBL_MAWB_FK = MA.MAWB_EXP_TBL_PK(+) ";
            Strsql += " AND PO.PORT_MST_PK=BA.PORT_MST_POD_FK";
            Strsql += " AND POL.PORT_MST_PK=BA.PORT_MST_POL_FK";
            Strsql += " AND C.CUSTOMER_MST_PK(+)=JA.CONSIGNEE_CUST_MST_FK ";
            Strsql += " AND JA.TRANSPORTER_DEPOT_FK=T.VENDOR_MST_PK(+)";
            Strsql += " AND DP.DEPOT_MST_FK=D.VENDOR_MST_PK(+)";
            Strsql += " AND DP.JOB_CARD_REF_FK(+)=JA.JOB_CARD_TRN_PK";
            Strsql += " AND JA.CREATED_BY_FK = UMT.USER_MST_PK";
            Strsql += " AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK";
            Strsql += " AND DP.DOC_NUMBER(+) LIKE 'AIRLINE%' ";
            Strsql += "and jA.JOB_CARD_TRN_PK in";
            Strsql += "(select jc.JOB_CARD_TRN_PK";
            Strsql += " from JOB_CARD_TRN jc";
            Strsql += "where";
            Strsql += "   jc.JOB_CARD_TRN_PK not in";
            Strsql += " (select d1.job_card_ref_fk";
            Strsql += "  from DOCS_PRINT_DTL_TBL D1";
            Strsql += " where d1.doc_number lIKE 'AIRLINE%')";
            Strsql += "  )";
            Strsql += strCondition;

            Strsql += " )q) WHERE SLNO  Between " + start + " and " + last;

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

        #endregion "Fetch For AIR Export"

        #region "Define ENUM Variables"

        /// <summary>
        /// 
        /// </summary>
        private enum Export
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
            /// The bookingpk
            /// </summary>
            BOOKINGPK = 2,
            /// <summary>
            /// The jobref
            /// </summary>
            JOBREF = 3,
            /// <summary>
            /// The haw b_ pk
            /// </summary>
            HAWB_PK = 4,
            /// <summary>
            /// The haw b_ reference
            /// </summary>
            HAWB_REF = 5,
            /// <summary>
            /// The maw b_ pk
            /// </summary>
            MAWB_PK = 6,
            /// <summary>
            /// The maw b_ reference
            /// </summary>
            MAWB_REF = 7,
            /// <summary>
            /// </summary>
            CPK = 8,
            /// <summary>
            /// The customer
            /// </summary>
            CUSTOMER = 9,
            /// <summary>
            /// The flight
            /// </summary>
            FLIGHT = 10,
            /// <summary>
            /// The pod
            /// </summary>
            POD = 11,
            /// <summary>
            /// The depo t_ pk
            /// </summary>
            DEPOT_PK = 12,
            /// <summary>
            /// The depo t_ name
            /// </summary>
            DEPOT_NAME = 13,
            /// <summary>
            /// The transporte r_ pk
            /// </summary>
            TRANSPORTER_PK = 14,
            /// <summary>
            /// The transporte r_ name
            /// </summary>
            TRANSPORTER_NAME = 15,
            /// <summary>
            /// The sp l_ instr
            /// </summary>
            SPL_INSTR = 16,
            /// <summary>
            /// The sel
            /// </summary>
            SEL = 17,
            /// <summary>
            /// The si
            /// </summary>
            SI = 18
        }

        #endregion "Define ENUM Variables"

        #region "Save Spl Instr + Warehouse + Transporter in DOCS_PRINT_DTL_TBL"

        /// <summary>
        /// Updates the details.
        /// </summary>
        /// <param name="Uwg1">The uwg1.</param>
        /// <param name="ProtocolNo">The protocol no.</param>
        /// <returns></returns>
        public bool UpdateDetails(object Uwg1, string ProtocolNo)
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
                //    if (Uwg1.Rows(I).Cells(Export.SEL].Value == "true") {
                //    var _with1 = ObjWk.MyCommand;

                //    _with1.CommandText = ObjWk.MyUserName + ".AIRLINE_DELIVERY_NOTE_REP_PKG.DOCS_PRINT_DTL_TBL_INS";
                //    _with1.CommandType = CommandType.StoredProcedure;
                //    _with1.Transaction = Tran;
                //    _with1.Parameters.Clear();

                //    _with1.Parameters.Add("BUSINESS_TYPE_IN", 1).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("MODE_IN", 1).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("JOBCARD_REF_PK_IN", Convert.ToInt32(Uwg1.rows(I).cells(Export.JOBCARDPK).value)).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("PROTOCOL_NO_IN", ProtocolNo).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("DEPOT_FK_IN", System."").Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("TRANSPORTER_FK_IN", System."").Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("SPL_INS_IN", (((Uwg1.rows(I).cells(Export.SI).value == null) | string.IsNullOrEmpty(Uwg1.rows(I).cells(Export.SI).value)) ? System."" : Uwg1.rows(I).cells(Export.SI).value).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                //    _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //    _with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

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
                Tran.Rollback();
                return false;
            }
            finally
            {
                ObjWk.CloseConnection();
            }
        }

        #endregion "Save Spl Instr + Warehouse + Transporter in DOCS_PRINT_DTL_TBL"

        #region "generate protocol"

        /// <summary>
        /// Generate_s the ref_ no.
        /// </summary>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <param name="sPOL">The s pol.</param>
        /// <returns></returns>
        public string Generate_Ref_No(Int64 ILocationId, Int64 IEmployeeId, string sPOL)
        {
            string functionReturnValue = null;
            try
            {
                CREATED_BY = this.CREATED_BY;
                functionReturnValue = GenerateProtocolKey("AIRLINE DELIVERY NOTE AIR EXP", ILocationId, IEmployeeId, DateTime.Now, "", "", "", CREATED_BY);
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

        #endregion "generate protocol"

        #region "Query For Main Body"

        /// <summary>
        /// Fetches the main.
        /// </summary>
        /// <param name="DocNo">The document no.</param>
        /// <param name="JobRefs">The job refs.</param>
        /// <returns></returns>
        public DataSet FetchMain(string DocNo, string JobRefs)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();

            Strsql = "  SELECT ";
            Strsql += " HA.HAWB_REF_NO,JAA.VOYAGE_FLIGHT_NO FLIGHT_NO,MA.MAWB_REF_NO, POL.PORT_NAME AS POL, POD.PORT_NAME AS POD, ";
            Strsql += "  JAA.JOBCARD_REF_NO,JAA.JOB_CARD_TRN_PK JOB_CARD_AIR_EXP_PK,MA.MAWB_DATE MAWBDATE,";
            Strsql += "   TMST.VENDOR_NAME TRANSPORTER_NAME,TDTLS.ADM_ADDRESS_1 TA1,TDTLS.ADM_ADDRESS_2 AS TA2,TDTLS.ADM_ADDRESS_3 AS TA3,";
            Strsql += "  TDTLS.ADM_CITY AS TCITY,TDTLS.ADM_ZIP_CODE AS TZIP,";
            Strsql += " TDTLS.ADM_PHONE TPHONE,";
            Strsql += "  TDTLS.ADM_FAX_NO  TFAX,";
            Strsql += "  TDTLS.ADM_EMAIL_ID TEMAIL,";
            Strsql += "  TCOUNTRY.COUNTRY_NAME TPCOUNTRY,";
            Strsql += "  DMST.VENDOR_NAME DEPOT_NAME,DDTLS.ADM_ADDRESS_1,DDTLS.ADM_ADDRESS_2,DDTLS.ADM_ADDRESS_3 ";
            Strsql += "  ,DDTLS.ADM_CITY,DDTLS.ADM_ZIP_CODE AS DZIP,";
            Strsql += " DDTLS.ADM_PHONE DPHONE,";
            Strsql += " DDTLS.ADM_FAX_NO DFAX,";
            Strsql += " DDTLS.ADM_EMAIL_ID DEMAIL,";
            Strsql += " DCOUNTRY.COUNTRY_NAME DPCOUNTRY,";
            Strsql += "  DP.SPECIAL_INSTRUCTION,";
            Strsql += "BS.DEL_ADDRESS DEL_ADDRESS,";
            Strsql += " SUM(JTAE.GROSS_WEIGHT) GROSSWT, ";
            Strsql += " SUM(JTAE.VOLUME_IN_CBM) VOLUMEWT,";
            Strsql += "SUM(JTAE.PACK_COUNT) PACKCOUNT";
            Strsql += "  FROM HAWB_EXP_TBL HA,JOB_CARD_TRN JAA,DOCS_PRINT_DTL_TBL DP,";
            Strsql += "  MAWB_EXP_TBL MA,BOOKING_MST_TBL BS,PORT_MST_TBL POL,PORT_MST_TBL POD";
            Strsql += "   ,VENDOR_MST_TBL  DMST,VENDOR_MST_TBL  TMST,VENDOR_CONTACT_DTLS DDTLS,VENDOR_CONTACT_DTLS TDTLS,";
            Strsql += "COUNTRY_MST_TBL TCOUNTRY,COUNTRY_MST_TBL DCOUNTRY,JOB_TRN_CONT JTAE";
            Strsql += "  WHERE  JAA.JOB_CARD_TRN_PK IN (" + JobRefs + ")";
            Strsql += " AND JTAE.JOB_CARD_TRN_FK(+)=JAA.JOB_CARD_TRN_PK";
            Strsql += " AND HA.JOB_CARD_AIR_EXP_FK(+)=JAA.JOB_CARD_TRN_PK ";
            Strsql += " AND JAA.MBL_MAWB_FK = MA.MAWB_EXP_TBL_PK(+) ";
            Strsql += "  AND JAA.BOOKING_MST_FK=BS.BOOKING_MST_PK";
            Strsql += "  AND POL.PORT_MST_PK=BS.PORT_MST_POL_FK";
            Strsql += "  AND POD.PORT_MST_PK=BS.PORT_MST_POD_FK";
            Strsql += "   AND TMST.VENDOR_MST_PK(+) = JAA.TRANSPORTER_DEPOT_FK ";
            Strsql += "   AND DMST.VENDOR_MST_PK(+) = DP.DEPOT_MST_FK";
            Strsql += "  AND DP.JOB_CARD_REF_FK(+)=JAA.JOB_CARD_TRN_PK";
            Strsql += " AND TMST.VENDOR_MST_PK = TDTLS.VENDOR_MST_FK(+) ";
            Strsql += "   AND DMST.VENDOR_MST_PK = DDTLS.VENDOR_MST_FK(+)";
            Strsql += "AND DCOUNTRY.COUNTRY_MST_PK(+)=DDTLS.ADM_COUNTRY_MST_FK";
            Strsql += " AND TCOUNTRY.COUNTRY_MST_PK(+)=TDTLS.ADM_COUNTRY_MST_FK";
            Strsql += "  AND DP.DOC_NUMBER='" + DocNo + "'";
            Strsql += "GROUP BY";
            Strsql += "HA.HAWB_REF_NO,";
            Strsql += "JAA.VOYAGE_FLIGHT_NO,";
            Strsql += "MA.MAWB_REF_NO,";
            Strsql += "POL.PORT_NAME ,";
            Strsql += "POD.PORT_NAME ,";
            Strsql += "JAA.JOBCARD_REF_NO,";
            Strsql += "JAA.JOB_CARD_TRN_PK,";
            Strsql += "MA.MAWB_DATE,";
            Strsql += " TMST.VENDOR_NAME,";
            Strsql += "TDTLS.ADM_ADDRESS_1 ,";
            Strsql += "TDTLS.ADM_ADDRESS_2,";
            Strsql += "TDTLS.ADM_ADDRESS_3 ,";
            Strsql += "TDTLS.ADM_CITY ,";
            Strsql += "TDTLS.ADM_ZIP_CODE ,";
            Strsql += "TDTLS.ADM_PHONE ,";
            Strsql += "TDTLS.ADM_FAX_NO,";
            Strsql += "TDTLS.ADM_EMAIL_ID ,";
            Strsql += "TCOUNTRY.COUNTRY_NAME ,";
            Strsql += "DMST.VENDOR_NAME,";
            Strsql += "DDTLS.ADM_ADDRESS_1,";
            Strsql += "DDTLS.ADM_ADDRESS_2,";
            Strsql += "DDTLS.ADM_ADDRESS_3,";
            Strsql += " DDTLS.ADM_CITY,";
            Strsql += "DDTLS.ADM_ZIP_CODE ,";
            Strsql += "DDTLS.ADM_PHONE,";
            Strsql += "DDTLS.ADM_FAX_NO ,";
            Strsql += "DDTLS.ADM_EMAIL_ID,";
            Strsql += "DCOUNTRY.COUNTRY_NAME ,";
            Strsql += "DP.SPECIAL_INSTRUCTION,";
            Strsql += "BS.DEL_ADDRESS";

            try
            {
                return ObjWk.GetDataSet(Strsql);
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

        #endregion "Query For Main Body"

        #region "Summary"

        /// <summary>
        /// Fetches the summary.
        /// </summary>
        /// <param name="Jobref">The jobref.</param>
        /// <returns></returns>
        public DataSet FetchSummary(string Jobref)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = " SELECT SUM(JA.GROSS_WEIGHT), SUM(JA.VOLUME_IN_CBM),SUM(JA.PACK_COUNT),JA.JOB_CARD_AIR_EXP_FK";
            Strsql += "  FROM JOB_TRN_AIR_EXP_CONT JA";
            Strsql += "  WHERE  JA.JOB_CARD_AIR_EXP_FK IN (" + Jobref + ")";
            Strsql += "   GROUP BY JOB_CARD_AIR_EXP_FK  ";
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

        #endregion "Summary"

        #region "Save Track And Trace"

        /// <summary>
        /// Saves the track and trace.
        /// </summary>
        /// <param name="jobref">The jobref.</param>
        /// <param name="nlocationfk">The nlocationfk.</param>
        /// <param name="CREATED_BY">The create d_ by.</param>
        public void SaveTrackAndTrace(string jobref, int nlocationfk, int CREATED_BY)
        {
            string jobPk = null;

            Array arrJobPk = null;
            WorkFlow objWk = new WorkFlow();
            try
            {
                objWk.OpenConnection();
                int i = 0;
                arrJobPk = jobref.Split(',');
                for (i = 0; i <= arrJobPk.Length - 1; i++)
                {
                    jobPk = Convert.ToString(arrJobPk.GetValue(i));
                    arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(jobPk), 1, 1, "Goods Transferred to shed", "DEL-NOTE-PRNT-AIR-EXP", nlocationfk, objWk, "INS", CREATED_BY, "O");
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
        }

        #endregion "Save Track And Trace"

        #region "Fetch Airline Delivery Address"

        /// <summary>
        /// Fetches the delete address.
        /// </summary>
        /// <param name="Jobrefs">The jobrefs.</param>
        /// <returns></returns>
        public DataSet FetchDelAddress(string Jobrefs)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "  SELECT J.JOB_CARD_TRN_PK,B.DEL_ADDRESS CARGO_DEL_ADDRESS";
            Strsql += " FROM JOB_CARD_TRN J,BOOKING_MST_TBL B ";
            Strsql += " WHERE J.JOB_CARD_TRN_PK IN (" + Jobrefs + ")";
            Strsql += " AND J.BOOKING_MST_FK=B.BOOKING_MST_PK";
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

        #endregion "Fetch Airline Delivery Address"

        #region "Enhance Search For Flight"

        /// <summary>
        /// Fetches the exp airline flight.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchExpAirlineFlight(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EXP_VESFLIGHT_PKG.GET_AIRLINE_FLIGHT_COMMON";

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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

        /// <summary>
        /// Fetches the exp airline job number.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchExpAirlineJobNumber(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strLocPK = "";
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strLocPK = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EXP_VESFLIGHT_PKG.GET_AIRLINE_JOBNUMBER_COMMON";

                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", strLocPK).Direction = ParameterDirection.Input;
                // By Amit on 25-May-07, To fetch JobCard of Logged in Location
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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

        #endregion "Enhance Search For Flight"

        #region "Fetch"

        /// <summary>
        /// Fetches the tariff data.
        /// </summary>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="Type">if set to <c>true</c> [type].</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="AIRLINEPK">The airlinepk.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <returns></returns>
        public DataSet FetchTariffData(Int64 POLPK, bool Type, Int64 PODPK = 0, Int64 AIRLINEPK = 0, Int64 ActiveFlag = 0)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "SELECT TMAIN.TARIFF_MAIN_AIR_PK TPK, ";
            Strsql += "TMAIN.TARIFF_REF_NO TARIFF_NO,";
            Strsql += "ASLABS.SEQUENCE_NO SEQUENCENO,";
            Strsql += "FRT_FRT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT,";
            Strsql += "TTAT.PORT_MST_POL_FK POLFK,";
            Strsql += "TTAT.PORT_MST_POD_FK PODFK,";
            Strsql += "AMST.AIRLINE_ID CARRIERID,";
            Strsql += "AMST.AIRLINE_NAME CARRIERName,";

            Strsql += "AOO.PORT_ID AOOID ,";
            Strsql += " AOO.PORT_NAME AOONAME ,";
            Strsql += "AOD.PORT_ID AODID,";
            Strsql += "AOD.PORT_NAME AODNAME,";
            Strsql += "ASLABS.BREAKPOINT_ID SLABS,";
            Strsql += "NVL(TABRK.TARIFF_RATE,0) TARIFF_RATE,";
            Strsql += "NVL(TTAFRE.MIN_AMOUNT,0)MIN_AMOUNT";
            Strsql += "FROM TARIFF_MAIN_AIR_TBL TMAIN, ";
            Strsql += "TARIFF_TRN_AIR_TBL TTAT,";
            Strsql += "PORT_MST_TBL AOO,";
            Strsql += "PORT_MST_TBL AOD,";
            Strsql += "TARIFF_AIR_BREAKPOINTS TABRK,";
            Strsql += "AIRFREIGHT_SLABS_TBL ASLABS,";
            Strsql += " TARIFF_TRN_AIR_FREIGHT_TBL TTAFRE,";
            Strsql += " AIRLINE_MST_TBL AMST,";
            Strsql += "  FREIGHT_ELEMENT_MST_TBL  FRT_FRT";

            Strsql += "WHERE TTAT.TARIFF_MAIN_AIR_FK = TMAIN.TARIFF_MAIN_AIR_PK";
            Strsql += "AND FRT_FRT.FREIGHT_ELEMENT_MST_PK = TTAFRE.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND AOO.PORT_MST_PK=TTAT.PORT_MST_POL_FK";
            Strsql += "AND AOD.PORT_MST_PK=TTAT.PORT_MST_POD_FK";
            Strsql += "AND TABRK.AIRFREIGHT_SLABS_TBL_FK=ASLABS.AIRFREIGHT_SLABS_TBL_PK";
            Strsql += "AND TTAFRE.TARIFF_TRN_FREIGHT_PK=TABRK.TARIFF_TRN_FREIGHT_FK";
            Strsql += "AND TTAFRE.TARIFF_TRN_AIR_FK=TTAT.TARIFF_TRN_AIR_PK";
            Strsql += "AND AMST.AIRLINE_MST_PK(+)=TMAIN.AIRLINE_MST_FK";
            Strsql += " AND TTAT.PORT_MST_POL_FK=" + POLPK;
            if (PODPK > 0)
            {
                Strsql += " AND TTAT.PORT_MST_POD_FK=" + PODPK;
            }
            if (Type == true)
            {
                Strsql += " AND TMAIN.AIRLINE_MST_FK=" + AIRLINEPK;
            }
            if (Type == true)
            {
                Strsql += "AND TMAIN.TARIFF_TYPE = 1";
            }
            if (Type == false)
            {
                Strsql += "AND TMAIN.TARIFF_TYPE = 2";
            }
            if (ActiveFlag > 0)
            {
                Strsql += "AND TMAIN.Active = 1";
            }
            try
            {
                return Objwk.GetDataSet(Strsql);
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
        /// Fetches the fre data.
        /// </summary>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="Type">if set to <c>true</c> [type].</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="AIRLINEPK">The airlinepk.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <returns></returns>
        public DataSet FetchFreData(Int64 POLPK, bool Type, Int64 PODPK = 0, Int64 AIRLINEPK = 0, Int64 ActiveFlag = 0)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = "  SELECT";
            Strsql += "HDR.TARIFF_MAIN_AIR_PK TPK,";
            Strsql += "HDR.TARIFF_REF_NO TARIFF_NO,";
            Strsql += "FRT_FRT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT,";
            Strsql += "TRN.PORT_MST_POL_FK POLFK,";
            Strsql += "TRN.PORT_MST_POD_FK PODFK,";
            Strsql += "AMST.AIRLINE_ID CARRIERID,";
            Strsql += "AMST.AIRLINE_NAME CARRIERNAME,";
            Strsql += "AOO.PORT_ID AOOID ,";
            Strsql += "AOO.PORT_NAME AOONAME ,";
            Strsql += "AOD.PORT_ID AODID,";
            Strsql += "AOD.PORT_NAME AODNAME,";
            Strsql += "SUR_FRT.FREIGHT_ELEMENT_ID ||";
            Strsql += "DECODE(SUR.CHARGE_BASIS, 1, ' (%)', 2, ' (Flat)', 3, ' (Kgs)') AS SUR,";
            Strsql += "SUR.FREIGHT_ELEMENT_MST_FK AS SUR_FRT_FK,";
            Strsql += "NVL(SUR.CURRENT_RATE,0) SUR_CURRENT,";
            Strsql += "NVL(SUR.TARIFF_RATE,0) SUR_TARIFF,";
            Strsql += "NVL(TTAFRE.MIN_AMOUNT,0) MIN_AMOUNT ,";
            Strsql += "SUR.CHARGE_BASIS AS SUR_CHARGE_BASIS,";
            Strsql += "SUR_FRT.FREIGHT_ELEMENT_NAME AS SUR_FRT_NAME";
            Strsql += "FROM TARIFF_MAIN_AIR_TBL HDR,";
            Strsql += "TARIFF_TRN_AIR_TBL TRN,";
            Strsql += "TARIFF_TRN_AIR_SURCHARGE SUR,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL SUR_FRT,";
            Strsql += "PORT_MST_TBL AOO,";
            Strsql += "PORT_MST_TBL AOD,";
            Strsql += " AIRLINE_MST_TBL AMST,";
            Strsql += " TARIFF_TRN_AIR_FREIGHT_TBL TTAFRE,";
            Strsql += " FREIGHT_ELEMENT_MST_TBL  FRT_FRT";

            Strsql += "WHERE HDR.TARIFF_MAIN_AIR_PK = TRN.TARIFF_MAIN_AIR_FK";
            Strsql += "AND FRT_FRT.FREIGHT_ELEMENT_MST_PK = TTAFRE.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND TRN.TARIFF_TRN_AIR_PK = SUR.TARIFF_TRN_AIR_FK";
            Strsql += "AND SUR_FRT.FREIGHT_ELEMENT_MST_PK = SUR.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND AMST.AIRLINE_MST_PK(+)=HDR.AIRLINE_MST_FK";
            Strsql += "AND AOO.PORT_MST_PK=TRN.PORT_MST_POL_FK";
            Strsql += "AND AOD.PORT_MST_PK=TRN.PORT_MST_POD_FK";
            Strsql += "AND TTAFRE.TARIFF_TRN_AIR_FK=TRN.TARIFF_TRN_AIR_PK";
            Strsql += "AND TRN.PORT_MST_POL_FK=" + POLPK;
            if (PODPK > 0)
            {
                Strsql += "AND TRN.PORT_MST_POD_FK=" + PODPK;
            }
            if (Type == true)
            {
                Strsql += "AND HDR.AIRLINE_MST_FK=" + AIRLINEPK;
            }
            if (Type == true)
            {
                Strsql += "AND HDR.TARIFF_TYPE = 1";
            }
            if (Type == false)
            {
                Strsql += "AND HDR.TARIFF_TYPE = 2";
            }
            if (ActiveFlag > 0)
            {
                Strsql += "AND HDR.Active = 1";
            }
            Strsql += " AND ROUND(SYSDATE - .5) BETWEEN TRN.VALID_FROM AND DECODE(TRN.VALID_TO,NULL, NULL_DATE_FORMAT, TRN.VALID_TO) ";
            Strsql += "ORDER BY  HDR.TARIFF_MAIN_AIR_PK";

            try
            {
                return Objwk.GetDataSet(Strsql);
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
        /// Fetches the base currency.
        /// </summary>
        /// <returns></returns>
        public string FetchBaseCurrency()
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "SELECT CURR.CURRENCY_ID CURRENCY FROM CORPORATE_MST_TBL CMST,CURRENCY_TYPE_MST_TBL CURR";
            Strsql += "WHERE CURR.CURRENCY_MST_PK(+)=CMST.CURRENCY_MST_FK";
            try
            {
                return Objwk.ExecuteScaler(Strsql);
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

        #endregion "Fetch"

        #region "Other Charges SubReport"

        /// <summary>
        /// Fetches the other charges.
        /// </summary>
        /// <param name="TrnPk">The TRN pk.</param>
        /// <param name="PolPK">The pol pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <returns></returns>
        public object FetchOtherCharges(string TrnPk, string PolPK, string PodPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = "SELECT DISTINCT HDR.TARIFF_MAIN_AIR_PK,";
            Strsql += "TRN.TARIFF_TRN_AIR_PK,";
            Strsql += "FMT.FREIGHT_ELEMENT_ID,";
            Strsql += "CTMT.CURRENCY_ID,";
            Strsql += "DECODE(TRNOTH.CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,";
            Strsql += "TRNOTH.CURRENT_RATE,";
            Strsql += "TRNOTH.TARIFF_RATE";

            Strsql += "FROM TARIFF_MAIN_AIR_TBL  HDR,";
            Strsql += "TARIFF_TRN_AIR_TBL TRN,";
            Strsql += "TARIFF_TRN_AIR_OTH_CHRG  TRNOTH,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FMT,";
            Strsql += "CURRENCY_TYPE_MST_TBL   CTMT,";
            Strsql += "FREIGHT_CONFIG_TRN_TBL  FCT";
            Strsql += " WHERE HDR.TARIFF_MAIN_AIR_PK = TRN.TARIFF_MAIN_AIR_FK";
            Strsql += "AND TRN.TARIFF_TRN_AIR_PK = TRNOTH.TARIFF_TRN_AIR_FK";
            Strsql += "AND CTMT.CURRENCY_MST_PK = TRNOTH.CURRENCY_MST_FK";
            Strsql += "AND FMT.FREIGHT_ELEMENT_MST_PK = TRNOTH.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND TRN.TARIFF_TRN_AIR_PK IN (" + TrnPk + ")";
            Strsql += "AND FMT.BUSINESS_TYPE IN (1, 3)";
            Strsql += "AND  FCT.PORT_MST_FK IN(" + PolPK + "," + PodPK + " )";

            try
            {
                return Objwk.GetDataSet(Strsql);
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

        #endregion "Other Charges SubReport"

        #region "Fetch Transaction"

        //This function fetch the Airline Tariff Entry from the database against the supplied ATE Pk
        /// <summary>
        /// Fetches the ate.
        /// </summary>
        /// <param name="pol_pk">The pol_pk.</param>
        /// <param name="pod_pk">The pod_pk.</param>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        /// <param name="Air_PK">The air_ pk.</param>
        /// <param name="rbValue">The rb value.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveOnly">The active only.</param>
        public void FetchATE(Int64 pol_pk, Int64 pod_pk, DataSet dsGrid, string ChargeBasis, string AirSuchargeToolTip, Int64 Air_PK, Int64 rbValue, Int64 CurrentPage, Int64 TotalPage, int ActiveOnly = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtDataTable = null;
            DataTable dtPort = null;
            DataTable dtOtherCharge = null;
            try
            {
                ChargeBasis = "";
                AirSuchargeToolTip = "";

                Create_Static_Column(dsGrid);
                objWF.MyCommand.Parameters.Clear();
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("POL_PK_IN", pol_pk).Direction = ParameterDirection.Input;
                _with4.Add("POD_PK_IN", pod_pk).Direction = ParameterDirection.Input;
                _with4.Add("AIR_IN", Air_PK).Direction = ParameterDirection.Input;
                _with4.Add("RBVAL_IN", rbValue).Direction = ParameterDirection.Input;
                _with4.Add("ACTIVE_IN", ActiveOnly).Direction = ParameterDirection.Input;
                _with4.Add("TARIFF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtDataTable = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "NEW_TARIFF_FREIGHT_AIR");

                Make_Column(dsGrid.Tables["Child"], dtDataTable, true);
                Populate_Child(dsGrid.Tables["Child"], dtDataTable);

                dsGrid.Tables["Parent"].Columns.Add("Oth. Chrg", typeof(string));
                dsGrid.Tables["Parent"].Columns.Add("Oth_Chrg_Val", typeof(string));

                dsGrid.Tables["Parent"].Columns.Add("Routing", typeof(string));
                objWF.MyCommand.Parameters.Clear();
                var _with5 = objWF.MyCommand.Parameters;
                _with5.Add("POL_PK_IN", pol_pk).Direction = ParameterDirection.Input;
                _with5.Add("POD_PK_IN", pod_pk).Direction = ParameterDirection.Input;
                _with5.Add("AIR_IN", Air_PK).Direction = ParameterDirection.Input;
                _with5.Add("RBVAL_IN", rbValue).Direction = ParameterDirection.Input;
                _with5.Add("ACTIVE_IN", ActiveOnly).Direction = ParameterDirection.Input;
                _with5.Add("TARIFF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtPort = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "NEW_TARIFF_PORT_AIR");
                objWF.MyCommand.Parameters.Clear();
                var _with6 = objWF.MyCommand.Parameters;
                _with6.Add("POL_PK_IN", pol_pk).Direction = ParameterDirection.Input;
                _with6.Add("POD_PK_IN", pod_pk).Direction = ParameterDirection.Input;
                _with6.Add("ACTIVE_IN", ActiveOnly).Direction = ParameterDirection.Input;
                _with6.Add("TARIFF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtOtherCharge = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "NEW_TARIFF_OTH_CHARGES");

                Populate_Parent(dsGrid.Tables["Parent"], dtPort, dtDataTable, dtOtherCharge);
                DataRelation rel = new DataRelation("rl_POL_POD", new DataColumn[] {
                    dsGrid.Tables["Parent"].Columns["TRNPK"],
                    dsGrid.Tables["Parent"].Columns["POLPK"],
                    dsGrid.Tables["Parent"].Columns["PODPK"]
                }, new DataColumn[] {
                    dsGrid.Tables["Child"].Columns["TRNPK"],
                    dsGrid.Tables["Child"].Columns["POLPK"],
                    dsGrid.Tables["Child"].Columns["PODPK"]
                });
                dsGrid.Relations.Add(rel);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                dtDataTable.Dispose();
                dtPort.Dispose();
                dtOtherCharge.Dispose();
                objWF = null;
            }
        }

        #endregion "Fetch Transaction"

        #region "Private Variables"

        /// <summary>
        /// The _ static_ col
        /// </summary>
        private int _Static_Col;
        /// <summary>
        /// The _ col_ incr
        /// </summary>
        private int _Col_Incr;
        /// <summary>
        /// The _ from date
        /// </summary>
        private string _FromDate = "";
        /// <summary>
        /// The _ todate
        /// </summary>
        private string _Todate = "";

        #endregion "Private Variables"

        /// <summary>
        /// The _ air line_ tariff_ cols
        /// </summary>
        private const int _AirLine_Tariff_Cols = 7;

        #region " Supporting Functions "

        /// <summary>
        /// Create_s the static_ column.
        /// </summary>
        /// <param name="dsGrid">The ds grid.</param>
        /// Creates Static column to be shown in the grid.
        public void Create_Static_Column(DataSet dsGrid)
        {
            dsGrid.Tables.Add("Parent");
            dsGrid.Tables["Parent"].Columns.Add("TRNPK", typeof(long));
            dsGrid.Tables["Parent"].Columns.Add("POLPK", typeof(long));
            dsGrid.Tables["Parent"].Columns.Add("AOO", typeof(string));
            dsGrid.Tables["Parent"].Columns.Add("PODPK", typeof(long));
            dsGrid.Tables["Parent"].Columns.Add("AOD", typeof(string));
            dsGrid.Tables["Parent"].Columns.Add("Valid From", typeof(string));
            dsGrid.Tables["Parent"].Columns.Add("Valid To", typeof(string));

            if (_Static_Col > _AirLine_Tariff_Cols)
            {
                dsGrid.Tables["Parent"].Columns.Add("Expected_Wt", typeof(double));
                dsGrid.Tables["Parent"].Columns.Add("Expected_Vol", typeof(double));
            }
            dsGrid.Tables.Add("Child");
            dsGrid.Tables["Child"].Columns.Add("TRNPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("POLPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("PODPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("FRTPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("Frt. Ele.", typeof(string));
            dsGrid.Tables["Child"].Columns.Add("FREIGHT_ELEMENT_MST_PK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("CURRENCY_MST_PK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("Curr.", typeof(string));
            dsGrid.Tables["Child"].Columns.Add("Minimum", typeof(double));
        }

        /// <summary>
        /// Make_s the column.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="dtTable">The dt table.</param>
        /// <param name="IsSlab">if set to <c>true</c> [is slab].</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        public void Make_Column(DataTable dtMain, DataTable dtTable, bool IsSlab, string ChargeBasis = "", string AirSuchargeToolTip = "")
        {
            int nRowCnt = 0;
            long nFrt = 0;
            bool bFirstTime = true;

            if (!IsSlab)
            {
                if (dtTable.Rows.Count > 0)
                {
                    nFrt = Convert.ToInt64(dtTable.Rows[0]["SUR_FRT_FK"]);
                }
            }
            else
            {
                if (dtTable.Rows.Count > 0)
                {
                    nFrt = Convert.ToInt64(dtTable.Rows[0]["FRTPK"]);
                }
            }

            for (nRowCnt = 0; nRowCnt <= dtTable.Rows.Count - 1; nRowCnt++)
            {
                if (!IsSlab)
                {
                    if (nFrt == Convert.ToInt64(dtTable.Rows[nRowCnt]["SUR_FRT_FK"]) & !bFirstTime)
                    {
                        return;
                    }

                    if (_Col_Incr == 3)
                    {
                        CheckColumn(dtMain, dtTable.Rows[nRowCnt]["SUR_FRT_FK"].ToString(), dtTable.Rows[nRowCnt]["SUR_EXTRA"].ToString(), dtTable.Rows[nRowCnt]["SUR"].ToString());
                    }
                    else
                    {
                        CheckColumn(dtMain, dtTable.Rows[nRowCnt]["SUR_FRT_FK"].ToString(), dtTable.Rows[nRowCnt]["SUR"].ToString());
                    }

                    ChargeBasis += dtTable.Rows[nRowCnt]["SUR_CHARGE_BASIS"].ToString() + ",";
                    AirSuchargeToolTip += dtTable.Rows[nRowCnt]["SUR_FRT_NAME"].ToString() + ",";
                    bFirstTime = false;
                }
                else
                {
                    //If nFrt <> CLng(dtTable.Rows(nRowCnt)["FRTPK")) Then
                    //    Exit Sub
                    //End If

                    if (_Col_Incr == 3)
                    {
                        CheckColumn(dtMain, dtTable.Rows[nRowCnt]["SLABS_FK"].ToString(), dtTable.Rows[nRowCnt]["SLABS_EXTRA"].ToString(), dtTable.Rows[nRowCnt]["SLABS"].ToString());
                    }
                    else
                    {
                        CheckColumn(dtMain, dtTable.Rows[nRowCnt]["SLABS_FK"].ToString(), dtTable.Rows[nRowCnt]["SLABS"].ToString());

                        AirSuchargeToolTip += dtTable.Rows[nRowCnt]["SLABS"].ToString() + ",";
                    }
                }
            }

            ChargeBasis = ChargeBasis.TrimEnd(Convert.ToChar(","));
            AirSuchargeToolTip = AirSuchargeToolTip.TrimEnd(Convert.ToChar(","));
        }

        /// <summary>
        /// Checks the column.
        /// </summary>
        /// <param name="dtTable">The dt table.</param>
        /// <param name="ColumnName">Name of the column.</param>
        /// Actually creates column in datatabla equal to the number of arguments in paramarray
        public void CheckColumn(DataTable dtTable, params string[] ColumnName)
        {
            try
            {
                int i = 0;
                for (i = 0; i <= ColumnName.Length - 1; i++)
                {
                    if (dtTable.Columns.Contains(ColumnName[i]) == true)
                    {
                    }
                    else
                    {
                        dtTable.Columns.Add(ColumnName[i], typeof(double));
                    }
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
        }

        /// <summary>
        /// Populate_s the child.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="dtTable">The dt table.</param>
        /// Populates child table for airfreight slabs
        public void Populate_Child(DataTable dsMain, DataTable dtTable)
        {
            int nRowCnt = 0;
            DataRow drRow = dsMain.NewRow();
            int nColPopulated = 0;
            int nTotalCol = dsMain.Columns.Count - 1;
            string strRatesToBeShown = null;
            try
            {
                if (_Col_Incr == 3)
                {
                    strRatesToBeShown = "SLAB_APPROVED";
                }
                else
                {
                    strRatesToBeShown = "SLAB_TARIFF";
                }
                for (nRowCnt = 0; nRowCnt <= dtTable.Rows.Count - 1; nRowCnt++)
                {
                    var _with7 = dtTable.Rows[nRowCnt];

                    if (string.IsNullOrEmpty(drRow["TRNPK"].ToString()))
                    {
                        drRow["TRNPK"] = _with7["TRN_AIR_PK"];
                        nColPopulated = 0;
                    }

                    if (string.IsNullOrEmpty(drRow["POLPK"].ToString()))
                    {
                        drRow["POLPK"] = _with7["PORT_MST_POL_FK"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["PODPK"].ToString()))
                    {
                        drRow["PODPK"] = _with7["PORT_MST_POD_FK"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["FRTPK"].ToString()))
                    {
                        drRow["FRTPK"] = _with7["FRTPK"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["Frt. Ele."].ToString()))
                    {
                        drRow["Frt. Ele."] = _with7["FRT_FRT"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["FREIGHT_ELEMENT_MST_PK"].ToString()))
                    {
                        drRow["FREIGHT_ELEMENT_MST_PK"] = _with7["FRT_FRT_FK"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["CURRENCY_MST_PK"].ToString()))
                    {
                        drRow["CURRENCY_MST_PK"] = _with7["FRT_CURR"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["Curr."].ToString()))
                    {
                        drRow["Curr."] = _with7["FRT_CURRID"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["Minimum"].ToString()))
                    {
                        drRow["Minimum"] = _with7["MIN_AMOUNT"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow[_with7["SLABS"].ToString()].ToString()))
                    {
                        drRow[_with7["SLABS"].ToString()] = _with7[strRatesToBeShown];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow[_with7["SLABS_FK"].ToString()].ToString()))
                    {
                        drRow[_with7["SLABS_FK"].ToString()] = _with7["SLAB_CURRENT"];
                        nColPopulated += 1;
                    }

                    if (_Col_Incr == 3)
                    {
                        if (string.IsNullOrEmpty(drRow[_with7["SLABS_EXTRA"].ToString()].ToString()))
                        {
                            drRow[_with7["SLABS_EXTRA"].ToString()] = _with7["SLAB_TARIFF"];
                            nColPopulated += 1;
                        }
                    }

                    if (nTotalCol == nColPopulated)
                    {
                        nColPopulated = 0;
                        dsMain.Rows.Add(drRow);
                        drRow = dsMain.NewRow();
                    }
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
        }

        /// <summary>
        /// Populate_s the parent.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="dtPort">The dt port.</param>
        /// <param name="dtAirSurchage">The dt air surchage.</param>
        /// <param name="dtOtherCharges">The dt other charges.</param>
        /// Populates Parent table for sector,air surcharges and other charges
        public void Populate_Parent(DataTable dsMain, DataTable dtPort, DataTable dtAirSurchage, DataTable dtOtherCharges)
        {
            int nPortRowCnt = 0;
            int nAirRowCnt = 0;
            int nOthRowCnt = 0;
            DataRow drMain = null;
            bool boolFirstLoop = true;
            string strRatestoBeShown = null;
            try
            {
                if (_Col_Incr == 3)
                {
                    strRatestoBeShown = "SUR_APPROVED";
                }
                else
                {
                    strRatestoBeShown = "SUR_TARIFF";
                }
                for (nPortRowCnt = 0; nPortRowCnt <= dtPort.Rows.Count - 1; nPortRowCnt++)
                {
                    drMain = dsMain.NewRow();
                    var _with8 = dtPort.Rows[nPortRowCnt];
                    drMain["TRNPK"] = _with8["TRN_AIR_PK"];
                    drMain["POLPK"] = _with8["PORT_MST_POL_FK"];
                    drMain["AOO"] = _with8["AOO"];
                    drMain["PODPK"] = _with8["PORT_MST_POD_FK"];
                    drMain["AOD"] = _with8["AOD"];
                    drMain["Valid From"] = (Convert.ToBoolean(_FromDate.TrimEnd().Length > 0) ? _FromDate : _with8["VALID_FROM"]);
                    drMain["Valid To"] = (Convert.ToBoolean(_Todate.TrimEnd().Length > 0) ? _Todate : _with8["VALID_TO"].ToString());
                    for (nOthRowCnt = 0; nOthRowCnt <= dtOtherCharges.Rows.Count - 1; nOthRowCnt++)
                    {
                        var _with9 = dtOtherCharges.Rows[nOthRowCnt];
                        if (Convert.ToInt64(_with9["TRN_AIR_PK"]) == Convert.ToInt64(dtPort.Rows[nPortRowCnt]["TRN_AIR_PK"]))
                        {
                            if (!(_with9.IsNull("OTH_CHRG_FRT_FRT_FK")))
                            {
                                if (_Col_Incr == 3)
                                {
                                    drMain["Oth_Chrg_Val"] = (drMain.IsNull("Oth_Chrg_Val") ? "" : drMain["Oth_Chrg_Val"]).ToString() + _with9["OTH_CHRG_FRT_FRT_FK"].ToString() + "~" + _with9["OTH_CHRG_CURR"].ToString() + "~" + _with9["OTH_CHRG_BASIS"].ToString() + "~" + _with9["OTH_CHRG_CURRENT"].ToString() + "~" + _with9["OTH_CHRG_TARIFF"].ToString() + "~" + _with9["OTH_CHRG_APPROVED"].ToString() + "^";
                                }
                                else
                                {
                                    drMain["Oth_Chrg_Val"] = (drMain.IsNull("Oth_Chrg_Val") ? "" : drMain["Oth_Chrg_Val"]).ToString() + _with9["OTH_CHRG_FRT_FRT_FK"].ToString() + "~" + _with9["OTH_CHRG_CURR"].ToString() + "~" + _with9["OTH_CHRG_BASIS"].ToString() + "~" + _with9["OTH_CHRG_CURRENT"].ToString() + "~" + _with9["OTH_CHRG_TARIFF"].ToString() + "^";
                                }
                            }
                        }
                    }
                    dsMain.Rows.Add(drMain);
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
        }

        #endregion " Supporting Functions "
    }
}