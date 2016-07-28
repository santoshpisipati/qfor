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
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_BOOKING_CONTAINER_MOVES : CommonFeatures
    {
        public DataSet dsOthBooking;
        public DataSet dsMoveHdr;
        public long lngEmpty_Hdr;
        public long lngFull_Hdr;
        public long Departure_Voyage_Fk;

        public string sTxt;
        #region "Generate Protocol Key"
        public string GenerateProtocol(string sProtocolName, Int64 ILocationId, Int64 IEmployeeId, System.DateTime ProtocolDate, string sVSL = "", string sVOY = "", string sPOL = "", Int64 sUserid = 0)
        {
            return GenerateProtocolKey(sProtocolName, ILocationId, IEmployeeId, ProtocolDate, sVSL, sVOY, sPOL, sUserid);
        }

        public string GenerateProto(string sProtocolName, Int64 ILocationId, Int64 IEmployeeId)
        {
            return GenerateProtocolKey(sProtocolName, ILocationId, IEmployeeId, DateTime.Now);
        }
        #endregion

        #region "EDI Function"
        public string SegmentUNB()
        {
            string syntaxidentifier = null;
            string Syntaxversion = null;
            string sendercode = null;
            string ReceiverCode = null;
            string DateTime1 = null;
            string InterchangeRef = null;
            syntaxidentifier = "UNOA";
            Syntaxversion = "2";
            sendercode = "TRP:ZZZ";
            ReceiverCode = "UTS:ZZZ";
            DateTime1 = DateTime.Now.ToString("YYMMDD:hhmm");
            InterchangeRef = "1";
            sTxt = "UNB+" + syntaxidentifier + ":" + Syntaxversion + "+" + sendercode + "+" + DateTime1 + "+" + InterchangeRef;
            return sTxt;
        }
        public string SegmentUNH()
        {
            string msgrefno = null;
            string msgIdentifier = null;
            string msgversion = null;
            string msgtypereleaseno = null;
            string ControllingAgency = null;
            string AssocAssignedCode = null;
            msgrefno = "2";
            msgIdentifier = "COPRAR";
            msgversion = "D";
            msgtypereleaseno = "95B";
            ControllingAgency = "UN";
            AssocAssignedCode = "ITG12";
            sTxt = "UNH+" + msgrefno + "+" + msgIdentifier + ":" + msgversion + ":" + msgtypereleaseno + ":";
            return sTxt;
        }
        public string SegamentBGM()
        {
            string TransLI = null;
            TransLI = "45";
            sTxt = "BGM+" + "+" + TransLI + "+";
            return sTxt;
        }
        public string SegamentRFF()
        {
            string RefQualifier = null;
            RefQualifier = "XXX:1";
            sTxt = "RFF+" + "+" + RefQualifier;
            return sTxt;
        }
        #endregion

        #region "Fetch Gate-In"
        public DataSet FetchGateInMoves()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " select m.movecode_pk MOVECODE_FK from movecode_mst_tbl m where m.movecode_id in ('OFO','OEO')";
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
        public int GetPod(string bookID)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "  select  bt.pod_fk from booking_trn bt where BT.BOOKING_ID ='" + bookID + "'";
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
        public DataSet GetPodID(string bookID)
        {
            string strSQL = null;
            int podPk = 0;
            WorkFlow objWF = new WorkFlow();
            podPk = GetPod(bookID);
            strSQL = "select p.port_id,p.port_mst_pk from port_mst_tbl p where p.port_mst_pk=" + podPk + "";
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
        #endregion

        #region "FetchForGateIn"
        public DataSet FetchForGateIn(long Commercial_Sch_trn_Pk, Int16 clientval, Int64 PortPK, bool iTranshipment, string sType = "A", Int32 iBusiness = 0, string ContNo = "", string Customer = "", string CRO = "", long Pol = 0,
        string Pod = "", string BkgNo = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool display = false)
        {

            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CRO = CRO.Replace("'", "");
            ContNo = ContNo.Replace("'", "");
            BkgNo = BkgNo.Replace("'", "");

            strSQL = " SELECT COUNT(*)  from " ;
            strSQL += "    (select BCT.BOOKING_CONTAINERS_PK " ;
            strSQL += "     FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT,PORT_MST_TBL PLOAD," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT,COMMERCIAL_SCHEDULE_HDR CSH,COMMERCIAL_SCHEDULE_TRN CST,VESSEL_MST_TBL VMT,CRO_TRN CT" ;
            strSQL += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            strSQL += "       AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            strSQL += "       AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            strSQL += "       and bct.cro_trn_fk = ct.cro_trn_pk" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BT.POD_FK=PMT.PORT_MST_PK AND BT.POL_FK=PLOAD.PORT_MST_PK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "    AND BT.SPLITTING_STATUS IS NULL  ";
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            if (sType == "P")
            {
                strSQL += "       AND BCT.GATE_IN_STATUS=0 " ;
            }
            if (iTranshipment)
            {
                strSQL += "       AND(BT.SHIPMENT_TYPE=1 OR BT.SHIPMENT_TYPE=2)" ;
            }
            else
            {
                strSQL += "       AND BT.SHIPMENT_TYPE=1" ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            if (Commercial_Sch_trn_Pk != 0)
            {
                strSQL += "       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk ;
            }
            else
            {
                strSQL += " AND BR.COMMERCIAL_SCHEDULE_TRN_FK =0 ";
            }
            if (!string.IsNullOrEmpty(ContNo))
            {
                strSQL += "   AND BCT.CONTAINER_NO LIKE '" + "%" + ContNo.ToUpper() + "%" + "'";
            }
            if (!string.IsNullOrEmpty(Customer))
            {
                strSQL += " AND CMT.CUSTOMER_ID LIKE '" + Customer + "%" + "'";
            }
            if (!string.IsNullOrEmpty(CRO))
            {
                strSQL += "  AND CT.CRO_NO LIKE '" + "%" + CRO.ToUpper() + "%" + "'";
            }
            if (Pol > 0)
            {
                strSQL += " AND PLOAD.PORT_MST_PK =" + Pol;
            }
            if (!string.IsNullOrEmpty(Pod))
            {
                strSQL += " AND PMT.PORT_ID LIKE '" + Pod + "%" + "'";
            }
            if (!string.IsNullOrEmpty(BkgNo))
            {
                strSQL += "  AND BT.BOOKING_ID LIKE '" + "%" + BkgNo.ToUpper() + "%" + "'";
            }

            strSQL += "    AND CST.COMMERCIAL_SCHEDULE_HDR_FK=CSH.COMMERCIAL_SCHEDULE_HDR_PK";
            strSQL += "    AND CSH.VESSEL_MST_FK=VMT.VESSEL_MST_PK";
            strSQL += "    AND CST.COMMERCIAL_SCHEDULE_TRN_PK=BR.COMMERCIAL_SCHEDULE_TRN_FK";
            strSQL += "    union ";
            strSQL += "    select BCT.BOOKING_CONTAINERS_PK " ;
            strSQL += "     FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT,PORT_MST_TBL PLOAD," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT,COMMERCIAL_SCHEDULE_HDR CSH,COMMERCIAL_SCHEDULE_TRN CST,VESSEL_MST_TBL VMT ,CRO_TRN CT " ;
            strSQL += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            strSQL += "       AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            strSQL += "       AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            strSQL += "       and bct.cro_trn_fk = ct.cro_trn_pk(+)" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BT.POD_FK=PMT.PORT_MST_PK AND BT.POL_FK=PLOAD.PORT_MST_PK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;

            strSQL += "    AND BT.SPLITTING_STATUS IS NULL  ";

            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            if (sType == "P")
            {
                strSQL += "       AND BCT.GATE_IN_STATUS=0 " ;
            }
            if (iTranshipment)
            {
                strSQL += "       AND(BT.SHIPMENT_TYPE=1 OR BT.SHIPMENT_TYPE=2)" ;
            }
            else
            {
                strSQL += "       AND BT.SHIPMENT_TYPE=1" ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            if (Commercial_Sch_trn_Pk != 0)
            {
                strSQL += "       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk ;
            }
            else
            {
                strSQL += " AND BR.COMMERCIAL_SCHEDULE_TRN_FK =0 ";
            }
            if (!string.IsNullOrEmpty(ContNo))
            {
                strSQL += "   AND upper(BCT.CONTAINER_NO) LIKE '" + "%" + ContNo.ToUpper() + "%" + "'";
            }
            if (!string.IsNullOrEmpty(Customer))
            {
                strSQL += " AND CMT.CUSTOMER_ID LIKE '" + Customer + "%" + "'";
            }
            if (Pol > 0)
            {
                strSQL += " AND PLOAD.PORT_MST_PK =" + Pol;
            }
            if (!string.IsNullOrEmpty(Pod))
            {
                strSQL += " AND PMT.PORT_ID LIKE '" + Pod + "%" + "'";
            }
            if (!string.IsNullOrEmpty(BkgNo))
            {
                strSQL += "  AND upper(BT.BOOKING_ID) LIKE '" + "%" + BkgNo.ToUpper() + "%" + "'";
            }
            if (!string.IsNullOrEmpty(CRO))
            {
                strSQL += "  AND upper(CT.CRO_NO) LIKE '" + "%" + CRO.ToUpper() + "%" + "'";
            }
            strSQL += "    AND CST.COMMERCIAL_SCHEDULE_HDR_FK=CSH.COMMERCIAL_SCHEDULE_HDR_PK";
            strSQL += "   AND CSH.VESSEL_MST_FK=VMT.VESSEL_MST_PK";
            strSQL += "  AND CST.COMMERCIAL_SCHEDULE_TRN_PK=BR.COMMERCIAL_SCHEDULE_TRN_FK";
            strSQL += " )";
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;

            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            strSQL = " SELECT * FROM(" ;
            strSQL += " SELECT ROWNUM SL," ;
            strSQL += " q.* FROM (SELECT  SUBQRY.* FROM(" ;
            strSQL += " SELECT " ;
            strSQL += "       BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += "       BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += "       BCT.CONTAINER_NO, " ;
            strSQL += "       CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += "       PLOAD.PORT_ID POL,PMT.PORT_ID POD," ;
            strSQL += "       BT.BOOKING_ID," ;
            strSQL += "       CMT.CUSTOMER_NAME," ;
            strSQL += "       BCT.CONTAINER_STATUS," ;
            strSQL += "       DECODE(BCT.CONTAINER_STATUS, 1, 'Empty',2,'Full') ST," ;
            strSQL += "       CT.CRO_NO," ;
            strSQL += "       TO_CHAR(CT.CRO_DT,'" + M_DateFormat + "')CRO_DT, " ;
            strSQL += "       TO_CHAR(BCT.GATE_IN_STATUS) GATE_IN_STATUS, " ;
            strSQL += "       TO_CHAR(BCT.GATE_IN_STATUS) PREV_GATE_IN_STATUS, " ;
            strSQL += "       to_char(BCT.GATE_IN_DATE,'" + M_DateTimeFormat + "') GATE_IN_DATE ," ;
            strSQL += "  bct.stowage_position STOWAGE_POS," ;
            strSQL += "  to_char(bct.departure_date,'" + M_DateTimeFormat + "') LOAD_DT," ;
            strSQL += "       BR.SL_NO ROUTING_ID," ;
            strSQL += "       DECODE(BR.SL_NO,1,'LC','TS') LCTS," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_PK," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += "       CIT.LAST_MOVE_CODE_FK," ;
            strSQL += "       to_char(BCT.DSO_DATE,'dd/MM/yyyy hh:mm') DSO_DATE, " ;
            strSQL += "       BCT.DEPARTURE_DATE," ;
            strSQL += "       BCT.ARRIVAL_DT," ;
            strSQL += "       BCT.VERSION_NO, " ;
            strSQL += "       BT.SHIPMENT_TYPE, " ;
            strSQL += "      CST.COMMERCIAL_SCHEDULE_TRN_PK,BR.BOOKING_TRN_FK,";
            strSQL += "       TO_CHAR(CST.ETA,'" + M_DateFormat + "') ETA," ;
            strSQL += " CTM.Preference" ;
            strSQL += "     FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT, PORT_MST_TBL PLOAD," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT,COMMERCIAL_SCHEDULE_HDR CSH,COMMERCIAL_SCHEDULE_TRN CST,VESSEL_MST_TBL VMT,CRO_TRN CT" ;
            strSQL += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            if (clientval == 1)
            {
                strSQL += "       AND CMT.TEMP_CUSTOMER=1 " ;
            }
            strSQL += "       AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            strSQL += "       AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            strSQL += "       and bct.cro_trn_fk = ct.cro_trn_pk" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BT.POD_FK=PMT.PORT_MST_PK AND BT.POL_FK=PLOAD.PORT_MST_PK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            if (sType == "P")
            {
                strSQL += "       AND BCT.GATE_IN_STATUS=0 " ;
            }
            if (iTranshipment)
            {
                strSQL += "       AND(BT.SHIPMENT_TYPE=1 OR BT.SHIPMENT_TYPE=2)" ;
            }
            else
            {
                strSQL += "       AND BT.SHIPMENT_TYPE=1" ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            if (Commercial_Sch_trn_Pk > 0)
            {
                strSQL += "       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk ;
            }
            else
            {
                strSQL += " AND BR.COMMERCIAL_SCHEDULE_TRN_FK =0 ";
            }
            if (!string.IsNullOrEmpty(ContNo))
            {
                strSQL += "   AND BCT.CONTAINER_NO LIKE '" + "%" + ContNo.ToUpper() + "%" + "'";
            }
            if (!string.IsNullOrEmpty(Customer))
            {
                strSQL += " AND CMT.CUSTOMER_ID LIKE '" + Customer + "%" + "'";
            }
            if (!string.IsNullOrEmpty(CRO))
            {
                strSQL += "  AND CT.CRO_NO LIKE '" + "%" + CRO.ToUpper() + "%" + "'";
            }
            if (Pol > 0)
            {
                strSQL += " AND PLOAD.PORT_MST_PK =" + Pol;
            }
            if (!string.IsNullOrEmpty(Pod))
            {
                strSQL += " AND PMT.PORT_ID LIKE '" + Pod + "%" + "'";
            }
            if (!string.IsNullOrEmpty(BkgNo))
            {
                strSQL += "  AND BT.BOOKING_ID LIKE '" + "%" + BkgNo.ToUpper() + "%" + "'";
            }
            if (display == true)
            {
                strSQL += "  AND PLOAD.PORT_MST_PK = 0 ";
            }
            strSQL += "    AND CST.COMMERCIAL_SCHEDULE_HDR_FK=CSH.COMMERCIAL_SCHEDULE_HDR_PK";
            strSQL += "   AND CSH.VESSEL_MST_FK=VMT.VESSEL_MST_PK";
            strSQL += "  AND CST.COMMERCIAL_SCHEDULE_TRN_PK=BR.COMMERCIAL_SCHEDULE_TRN_FK";

            strSQL += "    AND BT.SPLITTING_STATUS IS NULL  ";
            strSQL += "  Union ";
            strSQL += " SELECT " ;
            strSQL += "       BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += "       BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += "       BCT.CONTAINER_NO, " ;
            strSQL += "       CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += "       PLOAD.PORT_ID POL,PMT.PORT_ID POD," ;
            strSQL += "       BT.BOOKING_ID," ;
            strSQL += "       CMT.CUSTOMER_NAME," ;
            strSQL += "       BCT.CONTAINER_STATUS," ;
            strSQL += "       DECODE(BCT.CONTAINER_STATUS, 1, 'Empty',2,'Full') ST," ;
            strSQL += "       CT.CRO_NO," ;
            strSQL += "       TO_CHAR(CT.CRO_DT,'" + M_DateFormat + "')CRO_DT, " ;
            strSQL += "       TO_CHAR(BCT.GATE_IN_STATUS) GATE_IN_STATUS, " ;
            strSQL += "       TO_CHAR(BCT.GATE_IN_STATUS) PREV_GATE_IN_STATUS, " ;
            strSQL += "       to_char(BCT.GATE_IN_DATE,'" + M_DateTimeFormat + "') GATE_IN_DATE ," ;
            strSQL += "  bct.stowage_position STOWAGE_POS," ;
            strSQL += "  to_char(bct.departure_date,'" + M_DateTimeFormat + "') LOAD_DT," ;
            strSQL += "       BR.SL_NO ROUTING_ID," ;
            strSQL += "       DECODE(BR.SL_NO,1,'LC','TS') LCTS," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_PK," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += "       CIT.LAST_MOVE_CODE_FK," ;
            strSQL += "       to_char(BCT.DSO_DATE,'dd/MM/yyyy hh:mm') DSO_DATE, " ;
            strSQL += "       BCT.DEPARTURE_DATE," ;
            strSQL += "       BCT.ARRIVAL_DT," ;
            strSQL += "       BCT.VERSION_NO, " ;
            strSQL += "       BT.SHIPMENT_TYPE, " ;
            strSQL += "      CST.COMMERCIAL_SCHEDULE_TRN_PK,BR.BOOKING_TRN_FK,";
            strSQL += "       TO_CHAR(CST.ETA,'" + M_DateFormat + "') ETA," ;
            strSQL += "     CTM.Preference" ;
            strSQL += "     FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT, PORT_MST_TBL PLOAD," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT,COMMERCIAL_SCHEDULE_HDR CSH,COMMERCIAL_SCHEDULE_TRN CST,VESSEL_MST_TBL VMT  ,CRO_TRN CT " ;
            strSQL += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            if (clientval == 1)
            {
                strSQL += "       AND CMT.TEMP_CUSTOMER=1 " ;
            }
            strSQL += "       AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            strSQL += "       AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            strSQL += "       and bct.cro_trn_fk = ct.cro_trn_pk(+)" ;
            strSQL += "         AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BT.POD_FK=PMT.PORT_MST_PK AND BT.POL_FK=PLOAD.PORT_MST_PK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            if (sType == "P")
            {
                strSQL += "       AND BCT.GATE_IN_STATUS=0 " ;
            }
            if (iTranshipment)
            {
                strSQL += "       AND(BT.SHIPMENT_TYPE=1 OR BT.SHIPMENT_TYPE=2)" ;
            }
            else
            {
                strSQL += "       AND BT.SHIPMENT_TYPE=1" ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            if (Commercial_Sch_trn_Pk > 0)
            {
                strSQL += "       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk ;
            }
            else
            {
                strSQL += " AND BR.COMMERCIAL_SCHEDULE_TRN_FK =0 ";
            }
            if (!string.IsNullOrEmpty(ContNo))
            {
                strSQL += "   AND upper(BCT.CONTAINER_NO) LIKE '" + "%" + ContNo.ToUpper() + "%" + "'";
            }
            if (!string.IsNullOrEmpty(Customer))
            {
                strSQL += " AND CMT.CUSTOMER_ID LIKE '" + Customer + "%" + "'";
            }
            if (!string.IsNullOrEmpty(CRO))
            {
                strSQL += "  AND upper(CT.CRO_NO) LIKE '" + "%" + CRO.ToUpper() + "%" + "'";
            }
            if (Pol > 0)
            {
                strSQL += " AND PLOAD.PORT_MST_PK =" + Pol;
            }
            if (!string.IsNullOrEmpty(Pod))
            {
                strSQL += " AND PMT.PORT_ID LIKE '" + Pod + "%" + "'";
            }
            if (!string.IsNullOrEmpty(BkgNo))
            {
                strSQL += "  AND upper(BT.BOOKING_ID) LIKE '" + "%" + BkgNo.ToUpper() + "%" + "'";
            }
            if (display == true)
            {
                strSQL += "  AND PLOAD.PORT_MST_PK = 0 ";
            }
            strSQL += "    AND CST.COMMERCIAL_SCHEDULE_HDR_FK=CSH.COMMERCIAL_SCHEDULE_HDR_PK";
            strSQL += "   AND CSH.VESSEL_MST_FK=VMT.VESSEL_MST_PK";
            strSQL += "  AND CST.COMMERCIAL_SCHEDULE_TRN_PK=BR.COMMERCIAL_SCHEDULE_TRN_FK";

            strSQL += "            AND BT.SPLITTING_STATUS IS NULL    ";

            strSQL += "  )SUBQRY order by SUBQRY.preference)q) WHERE SL  Between " + start + " and " + last ;

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
        #endregion

        #region "FetchBookingDetails"
        public DataSet FetchBookingDetails(Int64 BookingPk)
        {
            string Strsql = null;
            WorkFlow objWF = new WorkFlow();
            Strsql = Strsql  + " SELECT DISTINCT";
            Strsql = Strsql  + " BT.BOOKING_TRN_PK,";
            Strsql = Strsql  + " BRT.COMMERCIAL_SCHEDULE_TRN_FK,";
            Strsql = Strsql  + " POL.PORT_ID ,";
            Strsql = Strsql  + " VM.VESSEL_ID,";
            Strsql = Strsql  + " VM.VESSEL_NAME,";
            Strsql = Strsql  + " CHDR.VOYAGE_NO,";
            Strsql = Strsql  + " BT.BUSINESS_MODEL,";
            Strsql = Strsql  + " TO_CHAR(CTRN.ETA,'" + M_DateTimeFormat + "') ETA,";
            Strsql = Strsql  + " BRT.POL_MST_FK,";
            Strsql = Strsql  + " CST.CUSTOMER_ID CUSTID,";
            Strsql = Strsql  + " BT.BOOKING_ID BOOKNO,";
            Strsql = Strsql  + " POD.PORT_ID PODID";
            Strsql = Strsql  + " From";
            Strsql = Strsql  + " BOOKING_TRN BT,";
            Strsql = Strsql  + " BOOKING_ROUTING_TRN BRT,";
            Strsql = Strsql  + " COMMERCIAL_SCHEDULE_HDR CHDR,";
            Strsql = Strsql  + " COMMERCIAL_SCHEDULE_TRN CTRN,";
            Strsql = Strsql  + " VESSEL_MST_TBL VM,";
            Strsql = Strsql  + " PORT_MST_TBL POL,CUSTOMER_MST_TBL CST,";
            Strsql = Strsql  + " COMMODITY_MST_TBL CMT,";
            Strsql = Strsql  + " COMMODITY_GROUP_MST_TBL CG,";
            Strsql = Strsql  + " CUSTOMER_TYPE_MST_TBL CMT,";
            Strsql = Strsql  + " LOCATION_WORKING_PORTS_TRN LPT,";
            Strsql = Strsql  + " LOCATION_MST_TBL LMT,";
            Strsql = Strsql  + " BOOKING_CONTAINERS_TRN BCT ,";
            Strsql = Strsql  + " PORT_MST_TBL POD";
            Strsql = Strsql  + " Where BRT.BOOKING_TRN_FK = BT.BOOKING_TRN_PK";
            Strsql = Strsql  + " AND BRT.COMMERCIAL_SCHEDULE_TRN_FK=CTRN.COMMERCIAL_SCHEDULE_TRN_PK";
            Strsql = Strsql  + " AND CTRN.COMMERCIAL_SCHEDULE_HDR_FK=CHDR.COMMERCIAL_SCHEDULE_HDR_PK";
            Strsql = Strsql  + " AND CHDR.VESSEL_MST_FK=VM.VESSEL_MST_PK";
            Strsql = Strsql  + " AND BT.CUSTOMER_MST_FK=CST.CUSTOMER_MST_PK";
            Strsql = Strsql  + " AND BT.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+)";
            Strsql = Strsql  + " AND BT.COMMODITY_GROUP_FK=CG.COMMODITY_GROUP_PK";
            Strsql = Strsql  + " AND BT.POL_FK=POL.PORT_MST_PK";
            Strsql = Strsql  + " AND CMT.CUSTOMER_TYPE_PK = BT.BUSINESS_MODEL";
            Strsql = Strsql  + " AND BT.POL_FK = LPT.PORT_MST_FK";
            Strsql = Strsql  + " AND LPT.LOCATION_MST_FK = LMT.LOCATION_MST_PK";
            Strsql = Strsql  + " AND BCT.BOOKING_TRN_FK(+) = BT.BOOKING_TRN_PK";
            Strsql = Strsql  + " AND  BT.Pod_Fk = POD.PORT_MST_PK";
            Strsql = Strsql  + " AND BT.BOOKING_TRN_PK = " + BookingPk ;

            try
            {
                return objWF.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }
        #endregion

        #region "FetchPol"
        public DataSet FetchPol(Int64 LocPk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT 0 Port_Mst_Pk,' ' Port_Id FROM DUAL";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + " Select Prt.Port_Mst_Pk,Prt.Port_Id";
            strSQL = strSQL + " FROM Port_Mst_Tbl Prt,Location_Working_Ports_Trn Wrk,Country_Mst_Tbl Cntry";
            strSQL = strSQL + " WHERE Wrk.Port_Mst_Fk = Prt.Port_Mst_Pk";
            strSQL = strSQL + "   AND Prt.Country_Mst_Fk = Cntry.Country_Mst_Pk";
            strSQL = strSQL + "   and Wrk.active=1";
            strSQL = strSQL + "  and Wrk.Location_Mst_Fk in (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L    START WITH L.LOCATION_MST_PK = " + LocPk;
            strSQL = strSQL + "      CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK )  ";
            strSQL = strSQL + "  order by port_id";
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
        #endregion

        #region "VesselNameToolTip"
        public OracleDataReader VesselNameToolTip(string sqlstr)
        {
            WorkFlow objwk = new WorkFlow();
            try
            {
                return objwk.GetDataReader(sqlstr);
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
        public OracleDataReader CustomerToolTip(string sqlstr)
        {
            WorkFlow objwk = new WorkFlow();
            try
            {
                return objwk.GetDataReader(sqlstr);
            }
            catch (Exception sqlexp)
            {
                ErrorMessage = sqlexp.Message;
                throw sqlexp;
            }
        }
        #endregion

        #region "New Save Function for Gate-IN"
        public int InsertMoveHeader(string l_Move_Ref_No, string l_Move_Dt, long Locpk, string TimeDiff, Int64 l_Move_code_Fk)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            long lngMoveHdr_Pk = 0;
            DateTime EnteredDate = default(DateTime);
            EnteredDate = Convert.ToDateTime(String.Format("{0:" + M_DateFormat + "}", Convert.ToDateTime(l_Move_Dt)));
            try
            {
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with1 = objWK.MyCommand;
                _with1.Parameters.Add("MOVE_REF_NO_IN", l_Move_Ref_No).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("MOVE_DT_IN", EnteredDate).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("LOCATION_FK_IN", Locpk).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("DMT_DT_IN", TimeDiff).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("MOVE_CODE_FK_IN", l_Move_code_Fk).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".CONTAINER_MOVEMENT_PKG.CONTAINER_MOVE_HDR_INS";
                if (objWK.ExecuteCommands() == true)
                {
                    lngMoveHdr_Pk = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                    if (lngMoveHdr_Pk == 0)
                    {
                        lngMoveHdr_Pk = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                    }
                    return Convert.ToInt32(lngMoveHdr_Pk);
                }
                else
                {
                    return -1;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
            }
            return 0;
        }
        #endregion

        #region "SaveGateInData"
        public ArrayList SaveGateInData(Int64 IBooking_Containers_PK, Int64 IContainer_Inventry_Fk, Int64 IContainer_Move_HDR_fk, Int16 IContainer_Status, Int16 Ibusiness_Model, Int64 IPrev_Gate_n_Status, Int64 IGateIn_Status, System.DateTime DtGateId_dt, Int64 IVersion_no, long Locpk,
        string TimeDiff)
        {
            OracleCommand Cmd = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            int RAF = 0;
            objWK.OpenConnection();
            string SRet_Value = null;
            arrMessage.Clear();
            try
            {
                var _with2 = Cmd;
                _with2.Parameters.Clear();
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = objWK.MyUserName + ".CONTAINER_MOVEMENT_PKG.UPDATE_GATE_IN";
                var _with3 = _with2.Parameters;
                _with3.Add("BOOKING_CONTAINERS_PK_IN", IBooking_Containers_PK).Direction = ParameterDirection.Input;
                _with3.Add("CONTAINER_INVENTORY_TRN_FK_IN", IContainer_Inventry_Fk).Direction = ParameterDirection.Input;
                _with3.Add("CONTAINER_MOVE_HDR_FK_IN", IContainer_Move_HDR_fk).Direction = ParameterDirection.Input;
                _with3.Add("CONTAINER_STATUS_IN", IContainer_Status).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_MODEL_IN", Ibusiness_Model).Direction = ParameterDirection.Input;
                _with3.Add("PREV_GATE_IN_STATUS_IN", IPrev_Gate_n_Status).Direction = ParameterDirection.Input;
                _with3.Add("GATE_IN_STATUS_IN", IGateIn_Status).Direction = ParameterDirection.Input;
                _with3.Add("GATE_IN_DATE_IN", DtGateId_dt).Direction = ParameterDirection.Input;
                _with3.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with3.Add("VERSION_NO_IN", IVersion_no).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", Locpk).Direction = ParameterDirection.Input;
                _with3.Add("TIME_DIFF_IN", TimeDiff).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, SRet_Value).Direction = ParameterDirection.Output;
                RAF = Cmd.ExecuteNonQuery();

            }
            catch (OracleException oraexp)
            {
                if (oraexp.ErrorCode == 20999)
                {
                    arrMessage.Add("20999");
                }
                else
                {
                    arrMessage.Add(oraexp.Message);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            if (arrMessage.Count > 0)
            {
                return arrMessage;
            }
            else
            {
                arrMessage.Add("Saved");
                return arrMessage;
            }
        }
        #endregion

        #region "SaveArrivalData"
        public string SaveArrivalData(Int64 IBooking_Containers_PK, Int64 IContainer_Inventry_Fk, Int64 IContainer_Move_HDR_fk, Int64 IRouting_id_in, Int16 IContainer_Status, Int16 Ibusiness_Model, Int64 IPrev_Disch_Status, Int64 IDisch_Status, System.DateTime DtGateId_dt, Int64 IVersion_no,
        Int64 Location_mst_fk_in, Int64 Consignee_mst_fk_in)
        {
            OracleCommand Cmd = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            int RAF = 0;
            objWK.OpenConnection();
            string SRet_Value = null;
            arrMessage.Clear();
            try
            {
                var _with4 = Cmd;
                _with4.Parameters.Clear();
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".CONTAINER_MOVEMENT_PKG.UPDATE_DISCHARGE_CONFIRMATOIN";
                var _with5 = _with4.Parameters;
                _with5.Add("BOOKING_CONTAINERS_PK_IN", IBooking_Containers_PK).Direction = ParameterDirection.Input;
                _with5.Add("CONTAINER_INVENTORY_TRN_FK_IN", IContainer_Inventry_Fk).Direction = ParameterDirection.Input;
                _with5.Add("CONTAINER_MOVE_HDR_FK_IN", IContainer_Move_HDR_fk).Direction = ParameterDirection.Input;
                _with5.Add("ROUTING_ID_IN", IRouting_id_in).Direction = ParameterDirection.Input;
                _with5.Add("CONTAINER_STATUS_IN", IContainer_Status).Direction = ParameterDirection.Input;
                _with5.Add("BUSINESS_MODEL_IN", Ibusiness_Model).Direction = ParameterDirection.Input;
                _with5.Add("PREV_DISCH_STATUS_IN", IPrev_Disch_Status).Direction = ParameterDirection.Input;
                _with5.Add("DISCH_STATUS_IN", IDisch_Status).Direction = ParameterDirection.Input;
                _with5.Add("DISCH_DATE_IN", DtGateId_dt).Direction = ParameterDirection.Input;
                _with5.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with5.Add("LOCATION_MST_FK_IN", Location_mst_fk_in).Direction = ParameterDirection.Input;
                _with5.Add("CONSIGNEE_MST_FK_IN", Consignee_mst_fk_in).Direction = ParameterDirection.Input;
                _with5.Add("VERSION_NO_IN", IVersion_no).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, SRet_Value).Direction = ParameterDirection.Output;
                RAF = Cmd.ExecuteNonQuery();
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return "";
        }
        #endregion

        #region "Save For Gate In"
        public object SaveGateIn(DataSet dsBooking)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;

                OracleCommand insCommand = new OracleCommand();

                var _with6 = insCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".CONTAINER_MOVE_DTL_PKG.BOOKING_CONTAINERS_GATEIN";
                var _with7 = _with6.Parameters;
                insCommand.Parameters.Add("BOOKING_CONTAINERS_PK_IN", OracleDbType.Int32, 10, "BOOKING_CONTAINERS_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["BOOKING_CONTAINERS_PK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("GATE_IN_STATUS_IN", OracleDbType.Int32, 1, "GATE_IN_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["GATE_IN_STATUS_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("GATE_IN_DATE_IN", OracleDbType.Varchar2, 12, "GATE_IN_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["GATE_IN_DATE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;                

                var _with8 = objWK.MyDataAdapter;
                _with8.InsertCommand = insCommand;
                _with8.InsertCommand.Transaction = TRAN;
                RecAfct = _with8.Update(dsBooking.Tables["Booking"]);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    long lngHdrPk = 0;
                    arrMessage = Save_GateIn_Load_Arr_Hdr(TRAN);
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
                objWK.MyCommand.Connection.Close();
            }
        }
        #endregion

        #region "Save_GateIn_Load_Arr_Hdr"
        public ArrayList Save_GateIn_Load_Arr_Hdr(OracleTransaction Trn)
        {
            WorkFlow objWK = new WorkFlow();
            long lngMoveHdr_Pk = 0;
            objWK.MyConnection = Trn.Connection;
            Int16 i = default(Int16);
            try
            {
                objWK.MyCommand.Connection = objWK.MyConnection;
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".CONTAINER_MOVE_HDR_PKG.CONTAINER_MOVE_HDR_INS";
                objWK.MyCommand.Transaction = Trn;
                for (i = 0; i <= dsMoveHdr.Tables[0].Rows.Count - 1; i++)
                {
                    var _with9 = objWK.MyCommand;
                    var _with10 = _with9.Parameters;
                    _with10.Clear();
                    _with10.Add("MOVE_REF_NO_IN", dsMoveHdr.Tables[0].Rows[i]["MOVE_REF_NO_IN"]).Direction = ParameterDirection.Input;
                    _with10.Add("MOVE_DT_IN", dsMoveHdr.Tables[0].Rows[i]["MOVE_DT_IN"]).Direction = ParameterDirection.Input;
                    _with10.Add("MOVE_CODE_FK_IN", dsMoveHdr.Tables[0].Rows[i]["MOVE_CODE_FK_IN"]).Direction = ParameterDirection.Input;
                    _with10.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with10.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with10.Add("RETURN_VALUE", lngMoveHdr_Pk).Direction = ParameterDirection.Output;
                    try
                    {
                        if (!(objWK.MyCommand.ExecuteNonQuery() == 1))
                        {
                            arrMessage.Add("ERROR");
                        }
                        else
                        {
                            if (Convert.ToInt32(dsMoveHdr.Tables[0].Rows[i]["CHK"]) == 1)
                            {
                                lngEmpty_Hdr = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                            }
                            else
                            {
                                lngFull_Hdr = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        arrMessage.Add(ex.Message);
                    }
                }
                if (arrMessage.Count > 0)
                {
                    Trn.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage = Save_GateIn_Load_Arr_Dtl(Trn);
                    return arrMessage;
                }
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
        #endregion

        #region "Save_GateIn_Load_Arr_Dtl"
        public ArrayList Save_GateIn_Load_Arr_Dtl(OracleTransaction Trn1)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.MyConnection = Trn1.Connection;
            OracleCommand insCommand = new OracleCommand();
            Int64 RecAfct = default(Int64);

            var _with11 = insCommand;
            _with11.Connection = objWK.MyConnection;
            _with11.CommandType = CommandType.StoredProcedure;
            _with11.CommandText = objWK.MyUserName + ".CONTAINER_MOVE_DTL_PKG.CONTAINER_MOVE_GATEIN_LOAD_ARR";
            var _with12 = _with11.Parameters;
            insCommand.Parameters.Add("CONTAINER_INVENTORY_TRN_FK_IN", OracleDbType.Int32, 10, "CONTAINER_INVENTORY_TRN_FK").Direction = ParameterDirection.Input;
            insCommand.Parameters["CONTAINER_INVENTORY_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

            insCommand.Parameters.Add("CONTAINER_STATUS_IN", OracleDbType.Int32, 1, "CONTAINER_STATUS").Direction = ParameterDirection.Input;
            insCommand.Parameters["CONTAINER_STATUS_IN"].SourceVersion = DataRowVersion.Current;

            insCommand.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
            insCommand.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

            insCommand.Parameters.Add("STATUS", OracleDbType.Int32, 1, "STATUS").Direction = ParameterDirection.Input;
            insCommand.Parameters["STATUS"].SourceVersion = DataRowVersion.Current;

            insCommand.Parameters.Add("BOOKING_TRN_FK_IN", OracleDbType.Int32, 10, "BOOKING_TRN_FK").Direction = ParameterDirection.Input;
            insCommand.Parameters["BOOKING_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

            insCommand.Parameters.Add("LAST_MOVE_DT_IN", OracleDbType.Varchar2, 18, "MOVE_DATE").Direction = ParameterDirection.Input;
            insCommand.Parameters["LAST_MOVE_DT_IN"].SourceVersion = DataRowVersion.Current;

            insCommand.Parameters.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
            insCommand.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

            insCommand.Parameters.Add("MOVE_CODE_IN", OracleDbType.Int32, 10, "MOVECODE_FK").Direction = ParameterDirection.Input;
            insCommand.Parameters["MOVE_CODE_IN"].SourceVersion = DataRowVersion.Current;

            insCommand.Parameters.Add("FULL_MOVE_HDR", lngFull_Hdr).Direction = ParameterDirection.Input;
            insCommand.Parameters.Add("EMPTY_MOVE_HDR", lngEmpty_Hdr).Direction = ParameterDirection.Input;

            insCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
            insCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
            insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
            insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
            insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
            insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;            

            try
            {
                var _with13 = objWK.MyDataAdapter;
                _with13.InsertCommand = insCommand;
                _with13.InsertCommand.Transaction = Trn1;
                RecAfct = _with13.Update(dsOthBooking.Tables["COCBOOKING"]);
                if (arrMessage.Count > 0)
                {
                    Trn1.Rollback();
                    return arrMessage;
                }
                else
                {
                    Trn1.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
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
        #endregion

        #region "Fetch for Load Confirm"
        public DataSet FetchForLoadConfirm(string Vessel_Name, string Voyage, string sType = "A", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool PostBack = true)
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT COUNT(*) FROM (" ;
            strSQL += " SELECT DISTINCT JCSET.JOB_CARD_TRN_PK AS BOOKING_CONTAINERS_PK," ;
            strSQL += "     0 AS CONTAINER_INVENTORY_FK," ;
            strSQL += "     JTSEC.CONTAINER_NUMBER CONTAINER_NO," ;
            strSQL += "     JCSET.JOBCARD_REF_NO AS STOWAGE_POSITION," ;
            strSQL += "     CTMT.CONTAINER_TYPE_MST_ID," ;
            strSQL += "     'FULL' AS CONTAINER_STATUS," ;
            strSQL += "     '' AS ROUTING_ID," ;
            strSQL += "     'Local' AS SHIP_TYPE," ;
            strSQL += "     'FULL' AS STATUS," ;
            strSQL += "      CMT.CUSTOMER_NAME AS CUSTOMER_NAME," ;
            strSQL += "      POD.PORT_ID AS POD," ;
            strSQL += "     DECODE(HBL.HBL_STATUS,NULL,0,0,0,1,0,2,1) AS DEPARTURE_STATUS," ;
            strSQL += "     '' AS PREV_DEPARTURE_STATUS," ;
            strSQL += "     TO_CHAR(JTSEC.LOAD_DATE,DATETIMEFORMAT24) AS DEPARTURE_DATE," ;
            strSQL += "      0 AS CUSTOMER_TYPE_PK," ;
            strSQL += "     'SEA' AS BIZ_MODEL," ;
            strSQL += "      0 AS LAST_MOVE_CODE_FK," ;
            strSQL += "      0 AS BOOKING_ROUTING_PK," ;
            strSQL += "      POD.PORT_MST_PK AS POD_MST_FK," ;
            strSQL += "     '' AS DSO_DATE," ;
            strSQL += "     '' AS GATE_IN_DATE," ;
            strSQL += "      JCSET.ARRIVAL_DATE AS ARRIVAL_DT," ;
            strSQL += "      JCSET.VERSION_NO" ;
            strSQL += " FROM JOB_CARD_TRN   JCSET," ;
            strSQL += "      JOB_TRN_CONT   JTSEC," ;
            strSQL += "      CONTAINER_TYPE_MST_TBL CTMT," ;
            strSQL += "      CUSTOMER_MST_TBL       CMT," ;
            strSQL += "      BOOKING_MST_TBL        BST," ;
            strSQL += "      HBL_EXP_TBL            HBL," ;
            strSQL += "      PORT_MST_TBL           POL," ;
            strSQL += "      PORT_MST_TBL           POD" ;
            strSQL += "      WHERE JCSET.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK" ;
            strSQL += "      AND JTSEC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK" ;
            strSQL += "      AND JCSET.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK" ;
            strSQL += "      AND JCSET.BOOKING_MST_FK = BST.BOOKING_MST_PK" ;
            strSQL += "      AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSET.JOB_CARD_TRN_PK" ;
            strSQL += "      AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK" ;
            strSQL += "      AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK" ;
            strSQL += "      AND JCSET.VESSEL_NAME IS NOT NULL" ;
            strSQL += "      AND JCSET.VOYAGE_FLIGHT_NO IS NOT NULL" ;
            strSQL += "      AND JTSEC.CONTAINER_NUMBER IS NOT NULL " ;
            strSQL += "      AND BST.CARGO_TYPE=1" ;
            strSQL += "      AND BST.STATUS <> 3 " ;
            if (PostBack == false)
            {
                strSQL += "      AND JCSET.VESSEL_NAME='" + 0 + "'" ;
            }
            else
            {
                if (!string.IsNullOrEmpty(Vessel_Name))
                {
                    strSQL += "      AND JCSET.VESSEL_NAME='" + Vessel_Name + "'" ;
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strSQL += "      AND JCSET.VOYAGE_FLIGHT_NO='" + Voyage + "'" ;
                }
                if (sType == "P")
                {
                    strSQL += "      AND HBL.HBL_STATUS IN (0,1)" ;
                }
            }
            strSQL += ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            strSQL = "      SELECT * FROM ( " ;
            strSQL += "     SELECT ROWNUM SL_NO, QRY.* FROM (SELECT DISTINCT JCSET.JOB_CARD_TRN_PK AS BOOKING_CONTAINERS_PK," ;
            strSQL += "     0 AS CONTAINER_INVENTORY_FK," ;
            strSQL += "     JTSEC.CONTAINER_NUMBER CONTAINER_NO," ;
            strSQL += "     JCSET.JOBCARD_REF_NO AS STOWAGE_POSITION," ;
            strSQL += "     CTMT.CONTAINER_TYPE_MST_ID," ;
            strSQL += "     'FULL' AS CONTAINER_STATUS," ;
            strSQL += "     '' AS ROUTING_ID," ;
            strSQL += "     'Local' AS SHIP_TYPE," ;
            strSQL += "     'FULL' AS STATUS," ;
            strSQL += "      CMT.CUSTOMER_NAME AS CUSTOMER_NAME," ;
            strSQL += "      POD.PORT_ID AS POD," ;
            strSQL += "     DECODE(HBL.HBL_STATUS,NULL,0,0,0,1,0,2,1) AS DEPARTURE_STATUS," ;
            strSQL += "     '' AS PREV_DEPARTURE_STATUS," ;
            strSQL += "     TO_CHAR(JTSEC.LOAD_DATE,DATETIMEFORMAT24) AS DEPARTURE_DATE," ;
            strSQL += "      0 AS CUSTOMER_TYPE_PK," ;
            strSQL += "     'SEA' AS BIZ_MODEL," ;
            strSQL += "      0 AS LAST_MOVE_CODE_FK," ;
            strSQL += "      0 AS BOOKING_ROUTING_PK," ;
            strSQL += "      POD.PORT_MST_PK AS POD_MST_FK," ;
            strSQL += "     '' AS DSO_DATE," ;
            strSQL += "     '' AS GATE_IN_DATE," ;
            strSQL += "      JCSET.ARRIVAL_DATE AS ARRIVAL_DT," ;
            strSQL += "      JCSET.VERSION_NO" ;
            strSQL += "       FROM JOB_CARD_TRN   JCSET," ;
            strSQL += "      JOB_TRN_CONT   JTSEC," ;
            strSQL += "      CONTAINER_TYPE_MST_TBL CTMT," ;
            strSQL += "      CUSTOMER_MST_TBL       CMT," ;
            strSQL += "      HBL_EXP_TBL            HBL," ;
            strSQL += "      BOOKING_MST_TBL        BST," ;
            strSQL += "      PORT_MST_TBL           POL," ;
            strSQL += "      PORT_MST_TBL           POD" ;
            strSQL += "        WHERE JCSET.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK" ;
            strSQL += "      AND JTSEC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK" ;
            strSQL += "      AND JCSET.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK" ;
            strSQL += "      AND JCSET.BOOKING_MST_FK = BST.BOOKING_MST_PK" ;
            strSQL += "      AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSET.JOB_CARD_TRN_PK" ;
            strSQL += "      AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK" ;
            strSQL += "      AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK" ;
            strSQL += "      AND JCSET.VESSEL_NAME IS NOT NULL" ;
            strSQL += "      AND JCSET.VOYAGE_FLIGHT_NO IS NOT NULL" ;
            strSQL += "      AND JTSEC.CONTAINER_NUMBER IS NOT NULL" ;
            strSQL += "      AND BST.CARGO_TYPE=1" ;
            strSQL += "      AND BST.STATUS <> 3 " ;
            if (PostBack == false)
            {
                strSQL += "      AND JCSET.VESSEL_NAME='" + 0 + "'" ;
            }
            else
            {
                if (!string.IsNullOrEmpty(Vessel_Name))
                {
                    strSQL += "      AND JCSET.VESSEL_NAME='" + Vessel_Name + "'" ;
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strSQL += "      AND JCSET.VOYAGE_FLIGHT_NO='" + Voyage + "'" ;
                }
                if (sType == "P")
                {
                    strSQL += "      AND HBL.HBL_STATUS IN (0,1)" ;
                }
            }
            strSQL += "    ORDER BY CONTAINER_NO  ";
            strSQL += ")  QRY ) WHERE SL_NO  Between " + start + " and " + last;

            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
            }
            return new DataSet();
        }
        #endregion

        #region "FetchForLoadConfirmReport"
        public DataSet FetchForLoadConfirmReport(string Vessel_Name, string Voyage, string sType = "A", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool PostBack = true)
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = "      SELECT * FROM ( " ;
            strSQL += "     SELECT ROWNUM SL_NO, QRY.* FROM (SELECT DISTINCT JCSET.JOB_CARD_TRN_PK AS BOOKING_CONTAINERS_PK,     " ;
            strSQL += " BST.BOOKING_REF_NO AS BOOKING_REF_NO," ;
            strSQL += "        HBL.HBL_REF_NO AS HBL_REF_NO," ;
            strSQL += "        JCSET.JOBCARD_REF_NO AS JOBCARDREF_NO," ;
            strSQL += "     CMT.CUSTOMER_NAME AS CUSTOMER_NAME," ;
            strSQL += "        JTSEC.CONTAINER_NUMBER CONTAINER_NO," ;
            strSQL += "        JTSEC.SEAL_NUMBER," ;
            strSQL += "        COMT.COMMODITY_ID," ;
            strSQL += "        CTMT.CONTAINER_TYPE_MST_ID," ;
            strSQL += "        JTSEC.GROSS_WEIGHT," ;
            strSQL += "        JTSEC.NET_WEIGHT," ;
            strSQL += "        POL.PORT_NAME AS POL," ;
            strSQL += "        POD.PORT_NAME AS POD," ;
            strSQL += "     'FULL' AS CONTAINER_STATUS," ;
            strSQL += "        JTSEC.LOAD_DATE AS LOAD_DATE" ;
            strSQL += "     FROM JOB_CARD_TRN   JCSET," ;
            strSQL += "       JOB_TRN_CONT   JTSEC," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CUSTOMER_MST_TBL       CMT," ;
            strSQL += "       BOOKING_MST_TBL        BST," ;
            strSQL += "       HBL_EXP_TBL            HBL," ;
            strSQL += "       COMMODITY_MST_TBL      COMT," ;
            strSQL += "     PORT_MST_TBL           POL," ;
            strSQL += "     PORT_MST_TBL POD" ;
            strSQL += "     WHERE JCSET.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK" ;
            strSQL += "       AND JTSEC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND JCSET.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND JCSET.BOOKING_MST_FK = BST.BOOKING_MST_PK" ;
            strSQL += "       AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSET.JOB_CARD_TRN_PK" ;
            strSQL += "       AND COMT.COMMODITY_MST_PK(+) = JTSEC.COMMODITY_MST_FK" ;
            strSQL += "        AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK" ;
            strSQL += "       AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK" ;
            strSQL += "       AND JCSET.VESSEL_NAME IS NOT NULL" ;
            strSQL += "       AND JCSET.VOYAGE_FLIGHT_NO IS NOT NULL" ;
            strSQL += "       AND JTSEC.CONTAINER_NUMBER IS NOT NULL " ;
            strSQL += "       AND BST.CARGO_TYPE=1" ;
            strSQL += "  AND BST.STATUS <> 3 " ;

            if (PostBack == false)
            {
                strSQL += "      AND JCSET.VESSEL_NAME='" + 0 + "'" ;
            }
            else
            {
                if (!string.IsNullOrEmpty(Vessel_Name))
                {
                    strSQL += "      AND JCSET.VESSEL_NAME='" + Vessel_Name + "'" ;
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strSQL += "      AND JCSET.VOYAGE_FLIGHT_NO ='" + Voyage + "'" ;
                }

            }
            strSQL += " ORDER BY JTSEC.CONTAINER_NUMBER ";
            strSQL += ")  QRY ) ";

            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
            }
            return new DataSet();
        }
        #endregion

        #region "FetchForLoadConfirmTotal"
        public DataSet FetchForLoadConfirmTotal(string Vessel_Name, string Voyage, string sType = "A", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool PostBack = true)
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = "     SELECT CONTAINER_TYPE AS CONTAINERTYPE," ;
            strSQL += "   COUNT(CONTAINER_NUMBER) AS TOTAL," ;
            strSQL += "  SUM(GROSS_WEIGHT) AS TOTAL_GR_WEIGHT," ;
            strSQL += "        POD AS DISCHARGEPORT" ;
            strSQL += "       FROM (SELECT DISTINCT JTSEC.CONTAINER_NUMBER, JCSET.JOBCARD_REF_NO, " ;
            strSQL += "       CTMT.CONTAINER_TYPE_MST_ID AS CONTAINER_TYPE," ;
            strSQL += "      CTMT.CONTAINER_TYPE_MST_ID AS CONT_COUNT," ;
            strSQL += "     JTSEC.GROSS_WEIGHT         AS GROSS_WEIGHT," ;
            strSQL += "    POD.PORT_NAME                AS POD" ;
            strSQL += "   FROM JOB_CARD_TRN   JCSET," ;
            strSQL += "  JOB_TRN_CONT   JTSEC," ;
            strSQL += "        CONTAINER_TYPE_MST_TBL CTMT," ;
            strSQL += "        CUSTOMER_MST_TBL       CMT," ;
            strSQL += "       BOOKING_MST_TBL        BST," ;
            strSQL += "      HBL_EXP_TBL            HBL," ;
            strSQL += "     COMMODITY_MST_TBL      COMT," ;
            strSQL += "    PORT_MST_TBL           POL," ;
            strSQL += "   PORT_MST_TBL POD" ;
            strSQL += " WHERE JCSET.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK" ;
            strSQL += "     AND JTSEC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK" ;
            strSQL += "    AND JCSET.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK" ;
            strSQL += "   AND JCSET.BOOKING_MST_FK = BST.BOOKING_MST_PK" ;
            strSQL += "  AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSET.JOB_CARD_TRN_PK" ;
            strSQL += " AND COMT.COMMODITY_MST_PK(+) = JTSEC.COMMODITY_MST_FK" ;
            strSQL += "      AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK" ;
            strSQL += "     AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK" ;
            strSQL += "    AND JCSET.VESSEL_NAME IS NOT NULL" ;
            strSQL += "   AND JCSET.VOYAGE_FLIGHT_NO IS NOT NULL" ;
            strSQL += "  AND JTSEC.CONTAINER_NUMBER IS NOT NULL" ;
            strSQL += " AND BST.CARGO_TYPE = 1" ;
            strSQL += "  AND BST.STATUS <> 3 " ;

            if (PostBack == false)
            {
                strSQL += "      AND JCSET.VESSEL_NAME='" + 0 + "'" ;
            }
            else
            {
                if (!string.IsNullOrEmpty(Vessel_Name))
                {
                    strSQL += "      AND JCSET.VESSEL_NAME='" + Vessel_Name + "'" ;
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strSQL += "      AND JCSET.VOYAGE_FLIGHT_NO='" + Voyage + "'" ;
                }

            }
            strSQL += ") GROUP BY CONTAINER_TYPE, POD";

            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
            }
            return new DataSet();
        }
        #endregion

        #region "fetchLoadConformationExport"
        public DataSet FetchForLoadConfirmExport(long Commercial_Sch_trn_Pk, Int64 PortPK, string sType = "A", Int32 iBusiness = 0, bool iTranshipment = true, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT COUNT(*)" ;
            strSQL += " FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT" ;
            strSQL += " WHERE BR.BOOKING_TRN_FK=BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BC.BOOKING_TRN_FK=BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.BOOKING_CTYPE_FK=BC.BOOKING_CTYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BT.POD_FK=PMT.PORT_MST_PK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += "         AND (BCT.GATE_IN_STATUS = 1 OR BCT.DSO_STATUS = 1 or BCT.GATE_IN_STATUS=0) " ;
            if (sType == "P")
            {
                strSQL += "       AND BCT.DEPARTURE_STATUS=0 " ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            strSQL += "       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk ;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            strSQL = " SELECT * FROM ( " ;
            strSQL += " SELECT ROWNUM SL_NO, QRY.* FROM ( SELECT " ;
            strSQL += "       BCT.BOOKING_CONTAINERS_PK , " ;
            strSQL += "       BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += "       BCT.CONTAINER_NO, " ;
            strSQL += "       BCT.STOWAGE_POSITION, " ;
            strSQL += "       CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += "       BCT.CONTAINER_STATUS," ;
            strSQL += "       BR.SL_NO ROUTING_ID," ;
            strSQL += "       DECODE(BR.SL_NO,1,'Local','T/S') SHIP_TYPE," ;
            strSQL += "       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            strSQL += "       CMT.CUSTOMER_NAME," ;
            strSQL += "       PMT.PORT_ID POD," ;
            strSQL += "       TO_CHAR(DECODE(BR.SL_NO,1,BCT.DEPARTURE_STATUS, BCT.TS_DEP_STATUS)) DEPARTURE_STATUS, " ;
            strSQL += "       DECODE(BR.SL_NO,1,BCT.DEPARTURE_STATUS, BCT.TS_DEP_STATUS) PREV_DEPARTURE_STATUS, " ;
            strSQL += "       TO_CHAR(BCT.DEPARTURE_DATE,'" + M_DateTimeFormat + "') DEPARTURE_DATE, " ;
            strSQL += "       CTMT.CUSTOMER_TYPE_PK," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += "       CIT.LAST_MOVE_CODE_FK," ;
            strSQL += "       BR.BOOKING_ROUTING_PK," ;
            strSQL += "       BR.POD_MST_FK, " ;
            strSQL += "       TO_CHAR( BCT.DSO_DATE,'" + M_DateTimeFormat + "') DSO_DATE, " ;
            strSQL += "       TO_CHAR(BCT.GATE_IN_DATE,'" + M_DateTimeFormat + "') GATE_IN_DATE, " ;
            strSQL += "       TO_CHAR(BCT.ARRIVAL_DT, '" + M_DateFormat + "') ARRIVAL_DT, " ;
            strSQL += "       BCT.VERSION_NO " ;
            strSQL += " FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT" ;
            strSQL += " WHERE BR.BOOKING_TRN_FK=BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BC.BOOKING_TRN_FK=BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.BOOKING_CTYPE_FK=BC.BOOKING_CTYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BT.POD_FK=PMT.PORT_MST_PK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += "         AND (BCT.GATE_IN_STATUS = 1 OR BCT.DSO_STATUS = 1 or BCT.GATE_IN_STATUS=0) " ;
            //..GAP-USS-043
            if (sType == "P")
            {
                strSQL += "       AND BCT.DEPARTURE_STATUS=0 " ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            strSQL += "       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk ;
            strSQL += " ORDER BY BCT.GATE_IN_DATE)  QRY )" ;
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
            return new DataSet();
        }
        #endregion

        #region "FetchForLoadConfirm2"
        public DataSet FetchForLoadConfirm2(long Commercial_Sch_trn_Pk, Int64 PortPK, string sType = "A", Int32 iBusiness = 0, bool iTranshipment = true, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT * FROM ( " ;
            strSQL += " SELECT ROWNUM SL_NO, QRY.* FROM ( SELECT " ;
            strSQL += "       BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += "       BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += "       BCT.CONTAINER_NO, " ;
            strSQL += "       BCT.STOWAGE_POSITION, " ;
            strSQL += "       CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += "       BCT.CONTAINER_STATUS," ;
            strSQL += "       BR.SL_NO ROUTING_ID," ;
            strSQL += "       DECODE(BR.SL_NO,1,'Local','T/S') SHIP_TYPE," ;
            strSQL += "       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            strSQL += "       CMT.CUSTOMER_ID," ;
            strSQL += "       PMT.PORT_ID POD," ;
            strSQL += "       TO_CHAR(DECODE(BR.SL_NO,1,BCT.DEPARTURE_STATUS, BCT.TS_DEP_STATUS)) DEPARTURE_STATUS, " ;
            strSQL += "       DECODE(BR.SL_NO,1,BCT.DEPARTURE_STATUS, BCT.TS_DEP_STATUS) PREV_DEPARTURE_STATUS, " ;
            strSQL += "       TO_CHAR(BCT.DEPARTURE_DATE, '" + M_DateTimeFormat + "') DEPARTURE_DATE, " ;
            //modified by pavan..26/12/06
            strSQL += "       CTMT.CUSTOMER_TYPE_PK," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += "       CIT.LAST_MOVE_CODE_FK," ;
            strSQL += "       BR.BOOKING_ROUTING_PK," ;
            strSQL += "       BR.POD_MST_FK, " ;
            strSQL += "       TO_CHAR( BCT.DSO_DATE,'" + M_DateTimeFormat + "') DSO_DATE, " ;
            //modified by pavan..26/12/06
            strSQL += "       TO_CHAR(BCT.GATE_IN_DATE,'" + M_DateTimeFormat + "') GATE_IN_DATE, " ;
            //modified by pavan..26/12/06
            strSQL += "       TO_CHAR(BCT.ARRIVAL_DT, '" + M_DateFormat + "') ARRIVAL_DT, " ;
            strSQL += "       BCT.VERSION_NO " ;
            strSQL += " FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT" ;
            strSQL += " WHERE BR.BOOKING_TRN_FK=BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BC.BOOKING_TRN_FK=BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.BOOKING_CTYPE_FK=BC.BOOKING_CTYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BR.POD_MST_FK=PMT.PORT_MST_PK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += "       AND BCT.GATE_IN_STATUS=1 " ;
            if (sType == "P")
            {
                strSQL += "       AND BCT.DEPARTURE_STATUS=0 " ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            strSQL += "       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk ;
            strSQL += " ORDER BY BCT.GATE_IN_DATE)  QRY )" ;

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
        #endregion

        #region "FetchForLoadConfirm1"
        public DataSet FetchForLoadConfirm1(long Commercial_Sch_trn_Pk, string sType, Int32 iBusiness = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT  ROWNUM SR_NO," ;
            strSQL += " BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += " BCT.CONTAINER_NO, " ;
            strSQL += " CTM.CONTAINER_TYPE_MST_PK," ;
            strSQL += " CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += " BR.SL_NO ROUTE_ID," ;
            strSQL += " DECODE(BR.SL_NO,1,'Local','T/S') SHIPMENT_TYPE,  " ;
            strSQL += " DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            strSQL += " BT.CUSTOMER_MST_FK," ;
            strSQL += " CMT.CUSTOMER_ID," ;
            strSQL += " PMT.PORT_ID POD," ;
            strSQL += " TO_CHAR(BCT.DEPARTURE_STATUS) DEPARTURE_STATUS," ;
            strSQL += " BCT.DEPARTURE_DATE," ;
            strSQL += " CTMT.CUSTOMER_TYPE_ID BIZMODEL," ;
            strSQL += " BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += " CIT.LAST_MOVE_CODE_FK," ;
            strSQL += " MMT.MOVECODE_ID," ;
            strSQL += " BCT.VERSION_NO BCT_VERSION," ;
            strSQL += " CIT.VERSION_NO CIT_VERSION,BCT.SHIPMENT_TYPE,DEPARTURE_STATUS TEMP_STATUS,CTMT.CUSTOMER_TYPE_PK,BT.BOOKING_TRN_PK,BT.LOCATION_MST_FK,BCT.STOWAGE_POSITION,BCT.DEPARTURE_VOYAGE_FK " ;

            strSQL += " FROM BOOKING_TRN BT," ;
            strSQL += " BOOKING_ROUTING_TRN BR," ;
            strSQL += " BOOKING_CTYPE_TRN BC," ;
            strSQL += " BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += " CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += " CUSTOMER_MST_TBL CMT," ;
            strSQL += " PORT_MST_TBL PMT," ;
            strSQL += " CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += " CONTAINER_INVENTORY_TRN CIT," ;
            strSQL += " MOVECODE_MST_TBL MMT " ;

            strSQL += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            strSQL += " AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            strSQL += " AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            strSQL += " AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += " AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += " AND BT.POD_FK=PMT.PORT_MST_PK" ;
            strSQL += " AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += " AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += " AND MMT.MOVECODE_PK(+)=CIT.LAST_MOVE_CODE_FK AND BCT.GATE_IN_STATUS=1 " ;
            strSQL += " AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk;

            if (sType == "P")
            {
                strSQL += " AND BCT.DEPARTURE_STATUS=0 " ;
            }
            if (iBusiness > 0)
            {
                strSQL += " AND BT.BUSINESS_MODEL= " + iBusiness ;
            }

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
        #endregion

        #region "Fetch forAdvancedLoad Confirm"
        public DataSet FetchForAdvancedLoadConfirm(long Commercial_Sch_trn_Pk, Int16 clientval, Int64 PortPK, string sType = "A", Int32 iBusiness = 0, bool iTranshipment = true, Int32 CurrentPage = 0, Int32 TotalPage = 0, string From = "E", Int32 mvcode = 0)
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            strSQL += " SELECT COUNT(*) from (" ;
            strSQL += " select distinct bct.booking_containers_pk," ;
            strSQL += " bct.container_inventory_fk," ;
            strSQL += " bct.container_no," ;
            strSQL += " CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += " bt.booking_id BookingNr," ;
            strSQL += " bbt.service_bl_no," ;
            strSQL += " CMT.CUSTOMER_NAME," ;
            strSQL += " bct.container_status," ;
            strSQL += " CTMT.CUSTOMER_TYPE_PK," ;
            strSQL += " CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += " BRt.Sl_No ROUTING_ID," ;
            strSQL += " PMT.PORT_ID POD," ;
            strSQL += " DECODE(BRt.SL_NO, 1, 'Local', 'T/S') SHIP_TYPE," ;
            strSQL += " DECODE(bct.CONTAINER_STATUS, 1, 'EMPTY', 2, 'FULL') STATUS," ;
            strSQL += " TO_CHAR(DECODE(BRt.SL_NO," ;
            strSQL += " 1," ;
            strSQL += " bct.DEPARTURE_STATUS," ;
            strSQL += " bct.TS_DEP_STATUS)) DEPARTURE_STATUS," ;
            strSQL += " DECODE(BRt.SL_NO," ;
            strSQL += " 1," ;
            strSQL += " bct.DEPARTURE_STATUS," ;
            strSQL += " bct.TS_DEP_STATUS) PREV_DEPARTURE_STATUS," ;
            strSQL += " bct.DEPARTURE_DATE," ;
            strSQL += " BRt.BOOKING_ROUTING_PK," ;
            strSQL += " BRt.POD_MST_FK," ;
            strSQL += " CIT.LAST_MOVE_CODE_FK," ;
            strSQL += " bct.DSO_DATE," ;
            strSQL += "  bct.GATE_IN_DATE," ;
            strSQL += " bct.ARRIVAL_DT," ;
            strSQL += " bct.VERSION_NO " ;
            strSQL += "  from booking_trn             bt," ;
            strSQL += "  booking_containers_trn  bct," ;
            strSQL += "  booking_routing_trn     brt," ;
            strSQL += " container_inventory_trn cit," ;
            strSQL += " commercial_schedule_hdr csh," ;
            strSQL += " commercial_schedule_trn cst," ;
            strSQL += " vessel_mst_tbl          vmt," ;
            strSQL += "  booking_bl_trn          bbt," ;
            strSQL += " CONTAINER_TYPE_MST_TBL  CTM," ;
            strSQL += " CUSTOMER_MST_TBL        CMT," ;
            strSQL += " Port_Mst_Tbl            Pmt," ;
            strSQL += " customer_type_mst_tbl ctmt " ;
            strSQL += " where bt.booking_trn_pk = brt.booking_trn_fk " ;
            strSQL += " and bt.booking_trn_pk = bct.booking_trn_fk" ;
            strSQL += " AND bct.GATE_IN_STATUS = 1" ;
            strSQL += " AND Bt.SPLIT_STATUS(+) = 'O'" ;
            strSQL += " and cit.container_inventory_pk = bct.container_inventory_fk" ;
            strSQL += " and CIT.LAST_MOVE_CODE_FK in" ;
            strSQL += " (select m.movecode_pk" ;
            strSQL += " from movecode_mst_tbl m" ;
            strSQL += " where m.movecode_id IN ('OFO', 'TSM', 'TSF'))" ;
            strSQL += " and cst.commercial_schedule_trn_pk=brt.commercial_schedule_trn_fk" ;
            strSQL += " and csh.commercial_schedule_hdr_pk = cst.commercial_schedule_hdr_fk" ;
            strSQL += " and csh.vessel_mst_fk = vmt.vessel_mst_pk" ;
            strSQL += " and bbt.booking_trn_fk(+) = bt.booking_trn_pk" ;
            strSQL += " and (BBT.BL_STATUS IS NULL OR BBT.BL_STATUS <> 5)" ;
            strSQL += " and ctm.container_type_mst_pk = bct.container_type_mst_fk" ;
            strSQL += " and cmt.customer_mst_pk = bt.customer_mst_fk" ;
            strSQL += " and ctmt.customer_type_pk = 4" ;
            strSQL += " and pmt.port_mst_pk = brt.pod_mst_fk" ;
            strSQL += " AND brt.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk;
            strSQL += " ) " ;


            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            strSQL = " SELECT distinct * FROM ( " ;
            strSQL += " SELECT distinct  ROWNUM SL_NO, QRY.* FROM ( SELECT Distinct " ;
            strSQL += " bct.booking_containers_pk," ;
            strSQL += " bct.container_inventory_fk," ;
            strSQL += " bct.container_no," ;
            strSQL += " CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += " bt.booking_id BookingNr," ;
            strSQL += " bbt.service_bl_no," ;
            strSQL += " bct.container_status," ;
            strSQL += " BRt.Sl_No ROUTING_ID," ;
            strSQL += " DECODE(BRt.SL_NO, 1, 'Local', 'T/S') SHIP_TYPE," ;
            strSQL += " DECODE(bct.CONTAINER_STATUS, 1, 'EMPTY', 2, 'FULL') STATUS," ;
            strSQL += " CMT.CUSTOMER_NAME," ;
            strSQL += " PMT.PORT_ID POD," ;
            strSQL += " TO_CHAR(DECODE(BRt.SL_NO," ;
            strSQL += " 1," ;
            strSQL += " bct.DEPARTURE_STATUS," ;
            strSQL += " bct.TS_DEP_STATUS)) DEPARTURE_STATUS," ;
            strSQL += " DECODE(BRt.SL_NO," ;
            strSQL += " 1," ;
            strSQL += " bct.DEPARTURE_STATUS," ;
            strSQL += " bct.TS_DEP_STATUS) PREV_DEPARTURE_STATUS," ;
            strSQL += " bct.DEPARTURE_DATE," ;
            strSQL += " CTMT.CUSTOMER_TYPE_PK," ;
            strSQL += " CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += " CIT.LAST_MOVE_CODE_FK," ;
            strSQL += " BRt.BOOKING_ROUTING_PK," ;

            strSQL += " BRt.POD_MST_FK," ;
            strSQL += " bct.DSO_DATE," ;
            strSQL += " bct.GATE_IN_DATE," ;

            strSQL += " bct.ARRIVAL_DT," ;
            strSQL += " bct.VERSION_NO " ;
            strSQL += " from booking_trn             bt," ;
            strSQL += " booking_containers_trn  bct," ;
            strSQL += " booking_routing_trn     brt," ;
            strSQL += " container_inventory_trn cit," ;
            strSQL += " commercial_schedule_hdr csh," ;
            strSQL += " commercial_schedule_trn cst," ;
            strSQL += " vessel_mst_tbl          vmt," ;
            strSQL += " booking_bl_trn          bbt," ;
            strSQL += " CONTAINER_TYPE_MST_TBL  CTM," ;
            strSQL += " CUSTOMER_MST_TBL        CMT," ;
            strSQL += " Port_Mst_Tbl            Pmt," ;
            strSQL += " customer_type_mst_tbl ctmt" ;
            strSQL += " where bt.booking_trn_pk = brt.booking_trn_fk " ;
            strSQL += " and bt.booking_trn_pk = bct.booking_trn_fk" ;
            strSQL += " AND bct.GATE_IN_STATUS = 1" ;
            strSQL += " AND Bt.SPLIT_STATUS(+) = 'O'" ;
            strSQL += " and cit.container_inventory_pk = bct.container_inventory_fk" ;
            strSQL += " and CIT.LAST_MOVE_CODE_FK in" ;
            strSQL += " (select m.movecode_pk" ;
            strSQL += " from movecode_mst_tbl m" ;
            strSQL += " where m.movecode_id IN ('OFO', 'TSM', 'TSF'))" ;
            strSQL += " and csh.commercial_schedule_hdr_pk = cst.commercial_schedule_hdr_fk" ;
            strSQL += " and csh.vessel_mst_fk = vmt.vessel_mst_pk" ;
            if (clientval == 1)
            {
                strSQL += "     AND CMT.Temp_Customer=1";
            }
            strSQL += " and bbt.booking_trn_fk(+) = bt.booking_trn_pk" ;
            strSQL += " and (BBT.BL_STATUS IS NULL OR BBT.BL_STATUS <> 5)" ;
            strSQL += " and ctm.container_type_mst_pk = bct.container_type_mst_fk" ;
            strSQL += " and cmt.customer_mst_pk = bt.customer_mst_fk" ;
            strSQL += " and ctmt.customer_type_pk = 4" ;
            strSQL += " and pmt.port_mst_pk = brt.pod_mst_fk" ;
            strSQL += "       AND BRT.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk;
            strSQL += " ORDER BY BCT.GATE_IN_DATE)  QRY )" ;
            strSQL += " WHERE SL_NO  Between " + start + " and " + last;
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
        #endregion

        #region "Fetch Import"
        public DataSet FetchImport(int loc, int voyage, string searchType = "", int PortPK = 0, int status = 0, int conttype = 0, int shipment = 0, int commodity = 0, int servicepk = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, string VesselID = "")
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string Condition = null;

            if (VesselID.ToString().Trim().Length > 0)
            {
                if (searchType == "C")
                {
                    Condition +=  "and upper(VMT.VESSEL_ID) like '%" + VesselID.Replace("'", "''") + "%'";
                }
                else
                {
                    Condition +=  "and upper(VMT.VESSEL_ID) like '" + VesselID.Replace("'", "''") + "%'";
                }
            }

            if (voyage > 0)
            {
                Condition +=  " and CST.COMMERCIAL_SCHEDULE_TRN_PK =  " + voyage;
            }

            if (PortPK > 0)
            {
                Condition +=  " AND bbt.POL_MST_FK = " + PortPK;
            }

            if (status > 0)
            {
                Condition +=  " AND BKCNT.CONTAINER_STATUS = " + status;
            }

            if (conttype > 0)
            {
                Condition +=  " And CTM.CONTAINER_TYPE_MST_PK  = " + conttype;
            }

            if (shipment > 1)
            {
                Condition +=  "  and BKCNT.BOOKING_CONTAINERS_PK in (select BOOKING_CONTAINERS_PK from BOOKING_CONTAINERS_TRN where SHIPMENT_TYPE=2) ";
            }
            else
            {
                Condition +=  " and BKCNT.BOOKING_CONTAINERS_PK in (select BOOKING_CONTAINERS_PK from BOOKING_CONTAINERS_TRN where SHIPMENT_TYPE=1 or SHIPMENT_TYPE is null) ";
            }

            if (commodity > 0)
            {
                Condition +=  " AND bktrn.commodity_group_fk = " + commodity;
            }
            strSQL += " SELECT";
            strSQL += " count(*)";
            strSQL += " FROM CONTAINER_INVENTORY_TRN CIT,";
            strSQL += " CONTAINER_MOVE_DTL      CMD,";
            strSQL += " CONTAINER_MOVE_HDR      CMH,";
            strSQL += " MOVECODE_MST_TBL        MMT,";
            strSQL += " CONTAINER_TYPE_MST_TBL  CTM,";
            strSQL += " CUSTOMER_MST_TBL        CMT,";
            strSQL += " DEPOT_MST_TBL           DMT,";
            strSQL += " COMMERCIAL_SCHEDULE_TRN CST,";
            strSQL += " COMMERCIAL_SCHEDULE_HDR CSH,";
            strSQL += " VESSEL_MST_TBL          VMT,";
            strSQL += " BOOKING_CONTAINERS_TRN  BKCNT,";
            strSQL += " BOOKING_TRN             BKTRN,";
            strSQL += " terminal_mst_tbl        tmt,";
            strSQL += " booking_bl_trn          bbt,";
            strSQL += " booking_routing_trn  brt";

            strSQL += " WHERE CIT.LAST_MOVE_CODE_FK in";
            strSQL += " (select m.movecode_pk";
            strSQL += " from movecode_mst_tbl m";
            strSQL += " where m.movecode_id IN ('BFF', 'BEM'))";
            strSQL += " AND CIT.CONTAINER_INVENTORY_PK = CMD.CONTAINER_INVENTORY_TRN_FK";
            strSQL += " AND CMD.CONTAINER_MOVE_HDR_FK = CMH.CONTAINER_MOVE_PK";
            strSQL += " AND CMH.MOVE_CODE_FK = MMT.MOVECODE_PK";
            strSQL += " AND CIT.CONTAINER_TYPE_FK = CTM.CONTAINER_TYPE_MST_PK";
            strSQL += " AND CIT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)";
            strSQL += " AND CIT.DEPOT_MST_FK = DMT.DEPOT_MST_PK(+)";
            strSQL += " and brt.booking_trn_fk = BKTRN.BOOKING_TRN_PK";
            strSQL += " AND cst.arrival_voygae_fk = CSH.COMMERCIAL_SCHEDULE_HDR_PK";
            strSQL += " AND brt.arrival_voyage_fk = cst.commercial_schedule_trn_pk";
            strSQL += " AND CSH.VESSEL_MST_FK = VMT.VESSEL_MST_PK";
            strSQL += " AND BKCNT.CONTAINER_INVENTORY_FK = CIT.CONTAINER_INVENTORY_PK(+)";
            strSQL += " AND BKCNT.BOOKING_TRN_FK = BKTRN.BOOKING_TRN_PK(+)";
            strSQL += " AND BKTRN.SPLIT_STATUS(+) = 'O'";
            strSQL += " and bbt.booking_trn_fk(+) = bktrn.booking_trn_pk";
            strSQL += " AND TMT.TERMINAL_MST_PK(+) = BkCnT.DESTINATION_TERMINAL_MST_FK";
            strSQL += " AND BKTRN.BOOKING_TRN_PK IS NOT NULL";
            strSQL += " AND cmd.container_move_dtl_pk =";
            strSQL += " (SELECT Max(cd.container_move_dtl_pk)";
            strSQL += " from Container_move_Dtl Cd";
            strSQL += " where cd.container_inventory_trn_fk =";
            strSQL += " cmd.container_inventory_trn_fk";
            strSQL += " group by cd.container_inventory_trn_fk)";

            strSQL +=  Condition;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;


            strSQL = " SELECT * FROM ( ";
            strSQL += " SELECT ROWNUM SLNO, QRY.* FROM (";
            strSQL += " SELECT";
            strSQL += " BKCNT.BOOKING_CONTAINERS_PK BOOKING_CONTAINERS_PK,";
            strSQL += " BKCNT.CONTAINER_NO CONTAINER_NO,";
            strSQL += " CTM.CONTAINER_TYPE_MST_ID CONTAINER_TYPE_MST_ID,";
            strSQL += " DECODE(BKCNT.CONTAINER_STATUS, 1, 'EMPTY', 2, 'FULL') CONTAINER_STATUS,";
            strSQL += " DECODE(BKCNT.SHIPMENT_TYPE, 2, 'T/S', 'Local') SHIPMENT_TYPE,";
            strSQL += " BBT.SERVICE_BL_NO SERVICE_BL_NO,";
            strSQL += " BBT.POL POL,";
            strSQL += " BBT.POL_MST_FK POL_MST_FK,";
            strSQL += " CST.COMMERCIAL_SCHEDULE_TRN_PK COMMERCIAL_SCHEDULE_TRN_PK,";
            strSQL += " bktrn.commodity_group_fk COMMODITY_GROUP_PK,";
            strSQL += " CTM.CONTAINER_TYPE_MST_PK CONTAINER_TYPE_MST_PK,";
            strSQL += " BKCNT.DESTINATION_TERMINAL_MST_FK TERMINAL_PK,";
            strSQL += " tmt.terminal_id TERMINAL_ID,";
            strSQL += "dmt.depot_mst_pk DEPOT_PK,";
            strSQL += " dmt.depot_id DEPOT_ID,";
            strSQL += " BKCNT.VERSION_NO VERSION_NO,";
            strSQL += " BBT.POD_MST_FK POD_MST_FK";
            strSQL += " FROM CONTAINER_INVENTORY_TRN CIT,";
            strSQL += " CONTAINER_MOVE_DTL      CMD,";
            strSQL += " CONTAINER_MOVE_HDR      CMH,";
            strSQL += " MOVECODE_MST_TBL        MMT,";
            strSQL += " CONTAINER_TYPE_MST_TBL  CTM,";
            strSQL += " CUSTOMER_MST_TBL        CMT,";
            strSQL += " DEPOT_MST_TBL           DMT,";
            strSQL += " COMMERCIAL_SCHEDULE_TRN CST,";
            strSQL += " COMMERCIAL_SCHEDULE_HDR CSH,";
            strSQL += " VESSEL_MST_TBL          VMT,";
            strSQL += " BOOKING_CONTAINERS_TRN  BKCNT,";
            strSQL += " BOOKING_TRN             BKTRN,";
            strSQL += " terminal_mst_tbl        tmt,";
            strSQL += " booking_bl_trn          bbt,";
            strSQL += " booking_routing_trn     brt";
            strSQL += " WHERE CIT.LAST_MOVE_CODE_FK in";
            strSQL += " (select m.movecode_pk";
            strSQL += " from movecode_mst_tbl m";
            strSQL += " where m.movecode_id IN ('BFF', 'BEM'))";
            strSQL += " AND CIT.CONTAINER_INVENTORY_PK = CMD.CONTAINER_INVENTORY_TRN_FK";
            strSQL += " AND CMD.CONTAINER_MOVE_HDR_FK = CMH.CONTAINER_MOVE_PK";
            strSQL += " AND CMH.MOVE_CODE_FK = MMT.MOVECODE_PK";
            strSQL += " AND CIT.CONTAINER_TYPE_FK = CTM.CONTAINER_TYPE_MST_PK";
            strSQL += " AND CIT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)";
            strSQL += "and bkcnt.destination_depot_fk=dmt.depot_mst_pk(+)";
            strSQL += " and brt.booking_trn_fk = BKTRN.BOOKING_TRN_PK";
            strSQL += " AND cst.arrival_voygae_fk = CSH.COMMERCIAL_SCHEDULE_HDR_PK";
            strSQL += " AND brt.arrival_voyage_fk = cst.commercial_schedule_trn_pk";
            strSQL += " AND CSH.VESSEL_MST_FK = VMT.VESSEL_MST_PK";
            strSQL += " AND BKCNT.CONTAINER_INVENTORY_FK = CIT.CONTAINER_INVENTORY_PK(+)";
            strSQL += " AND BKCNT.BOOKING_TRN_FK = BKTRN.BOOKING_TRN_PK(+)";
            strSQL += " AND BKTRN.SPLIT_STATUS(+) = 'O'";
            strSQL += " and bbt.booking_trn_fk(+) = bktrn.booking_trn_pk";
            strSQL += " AND TMT.TERMINAL_MST_PK(+) = BkCnT.DESTINATION_TERMINAL_MST_FK";
            strSQL += " AND BKTRN.BOOKING_TRN_PK IS NOT NULL";
            strSQL += " AND cmd.container_move_dtl_pk =";
            strSQL += " (SELECT Max(cd.container_move_dtl_pk)";
            strSQL += " from Container_move_Dtl Cd";
            strSQL += " where cd.container_inventory_trn_fk =";
            strSQL += " cmd.container_inventory_trn_fk";
            strSQL += " group by cd.container_inventory_trn_fk)";
            strSQL +=  Condition;
            strSQL += " )QRY )";
            strSQL += "WHERE SLNO  Between " + start + " and " + last;
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
        #endregion

        #region "Save Function for Advance Import"
        public ArrayList SaveImport(DataSet M_DataSet)
        {
            Int32 i = 0;
            WorkFlow objWK = new WorkFlow();
            Int32 RecAfct = default(Int32);
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            OracleCommand updCommand = new OracleCommand();
            try
            {
                var _with14 = objWK.MyCommand;
                for (i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    _with14.CommandType = CommandType.StoredProcedure;
                    _with14.CommandText = objWK.MyUserName + ".BOOKING_CONTAINERS_TRN_PKG.UPDATE_DEST_DEPOT";
                    if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["DESTINATION_DEPOT_FK"].ToString()))
                    {
                        _with14.Parameters.Add("DEPOT_FK_IN", M_DataSet.Tables[0].Rows[i]["DESTINATION_DEPOT_FK"]).Direction = ParameterDirection.Input;
                        _with14.Parameters["DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        _with14.Parameters.Add("DEPOT_FK_IN", System.DBNull.Value).Direction = ParameterDirection.Input;
                        _with14.Parameters["DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["DESTINATION_TERMINAL_MST_FK"].ToString()))
                    {
                        if ((Convert.ToInt32(M_DataSet.Tables[0].Rows[i]["DESTINATION_TERMINAL_MST_FK"]) > 0))
                        {
                            _with14.Parameters.Add("TERMINAL_FK_IN", M_DataSet.Tables[0].Rows[i]["DESTINATION_TERMINAL_MST_FK"]).Direction = ParameterDirection.Input;
                            _with14.Parameters["TERMINAL_FK_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            _with14.Parameters.Add("TERMINAL_FK_IN", System.DBNull.Value).Direction = ParameterDirection.Input;
                            _with14.Parameters["TERMINAL_FK_IN"].SourceVersion = DataRowVersion.Current;
                        }
                    }
                    else
                    {
                        _with14.Parameters.Add("TERMINAL_FK_IN", System.DBNull.Value).Direction = ParameterDirection.Input;
                        _with14.Parameters["TERMINAL_FK_IN"].SourceVersion = DataRowVersion.Current;
                    }

                    _with14.Parameters.Add("BOOKING_CONTAINERS_PK_IN", M_DataSet.Tables[0].Rows[i]["BOOKING_CONTAINERS_PK"]).Direction = ParameterDirection.Input;
                    _with14.Parameters["BOOKING_CONTAINERS_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;

                    _with14.Parameters.Add("VERSION_NO_IN", M_DataSet.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                    _with14.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                    _with14.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with14.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    RecAfct = _with14.ExecuteNonQuery();
                }
                if (RecAfct > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                }

            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();

            }
            return new ArrayList();
        }
        #endregion

        #region "Fetch Port"
        public DataSet FetchPolloc(Int32 LocPk)
        {

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT ";
            strSQL += " 0 PORT_MST_PK,";
            strSQL += " ' ' PORT_ID FROM ";
            strSQL += " DUAL";
            strSQL += " UNION";
            strSQL += "  SELECT PMT.PORT_MST_PK as PORT_MST_PK, PMT.PORT_ID as PORT_ID";
            strSQL += "  FROM LOC_PORT_MAPPING_TRN LPMT, PORT_MST_TBL PMT" ;
            strSQL += "  WHERE LPMT.PORT_MST_FK = PMT.PORT_MST_PK";
            strSQL += "  AND LPMT.LOCATION_MST_FK not in " + LocPk ;
            strSQL += "  AND LPMT.BRANCH_WORKING_PORT = 1 ORDER BY PORT_ID" ;
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
        #endregion

        #region "Fetch Container Type"
        public DataSet Fetchcontainertype()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " select";
            strSQL += " 0 container_type_mst_pk,";
            strSQL += " ' ' container_type_mst_id,0 preference";
            strSQL += " From";
            strSQL += " Dual";
            strSQL += " Union";
            strSQL += " select ctm.container_type_mst_pk as container_type_mst_pk,";
            strSQL += " ctm.container_type_mst_id as container_type_mst_id,";
            strSQL += " ctm.preference as preference";
            strSQL += " from container_type_mst_tbl ctm where ctm.active_flag=1 order by preference";
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
        #endregion

        #region "Fetch Commodity"
        public DataSet FetchGroup()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = " select";
            strSQL += " 0 commodity_group_pk,";
            strSQL += "  ' ' commodity_group_code";
            strSQL += " From";
            strSQL += " Dual";
            strSQL += " Union";
            strSQL += " select cgm.commodity_group_pk as commodity_group_pk,";
            strSQL += " cgm.commodity_group_code as commodity_group_code";
            strSQL += " from commodity_group_mst_tbl cgm";
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
        #endregion

        #region "Fetch report for Advanced load list"
        public DataSet FetchReport(string VesselPK, string voyage, string port, string From = "E", Int32 MVCODE = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            try
            {
                strSQL +=  " select distinct bct.booking_containers_pk,";
                strSQL +=  "  bct.container_inventory_fk,";
                strSQL +=  " VMT.VESSEL_NAME,";
                strSQL +=  " csh.voyage_no,";
                strSQL +=  " cst.etd ETD,";
                strSQL +=  "  bt.booking_id,";
                strSQL +=  " bbt.service_bl_no BLNr,";
                strSQL +=  " NVL(PFD.PORT_ID, Pmt.PORT_ID) PFD, ";
                strSQL +=  "  bct.container_no,";
                strSQL +=  " bct.STOWAGE_POSITION,";
                strSQL +=  " CTM.CONTAINER_TYPE_MST_ID,";
                strSQL +=  " bct.container_status,";
                strSQL +=  " BRt.Sl_No ROUTING_ID,";
                strSQL +=  " DECODE(BRt.SL_NO, 1, 'Local', 'T/S') SHIP_TYPE,";
                strSQL +=  " DECODE(bct.CONTAINER_STATUS, 1, 'EMPTY', 2, 'FULL') STATUS,";
                strSQL +=  " CMT.CUSTOMER_MST_PK,";
                strSQL +=  " CMT.CUSTOMER_NAME,";
                strSQL +=  " POL.PORT_ID POLNAME,";
                strSQL +=  " Pmt.PORT_ID PODNAME, ";
                strSQL +=  "  bct.gate_in_date LOAD_DATE,";
                strSQL +=  "  CTMT.CUSTOMER_TYPE_ID BIZ_MODEL,";
                strSQL +=  "  bt.POO_FK,";
                strSQL +=  "  POO.PORT_ID POONAME";
                strSQL +=  " from booking_trn             bt,";
                strSQL +=  "  booking_containers_trn  bct,";
                strSQL +=  " booking_routing_trn     brt,";
                strSQL +=  " container_inventory_trn cit,";
                strSQL +=  " commercial_schedule_hdr csh,";
                strSQL +=  " commercial_schedule_trn cst,";
                strSQL +=  "  vessel_mst_tbl          vmt,";
                strSQL +=  "  booking_bl_trn          bbt,";
                strSQL +=  "  CONTAINER_TYPE_MST_TBL  CTM,";
                strSQL +=  " CUSTOMER_MST_TBL        CMT,";
                strSQL +=  "  Port_Mst_Tbl            Pmt,";
                strSQL +=  "  Port_Mst_Tbl            Poo,";
                strSQL +=  "   Port_Mst_Tbl            Pol,";
                strSQL +=  "  Port_Mst_Tbl            Pfd,";
                strSQL +=  "  Port_Mst_Tbl            Pod,";
                strSQL +=  "  customer_type_mst_tbl ctmt";
                strSQL +=  " where(bt.booking_trn_pk = brt.booking_trn_fk)";
                strSQL +=  " and bt.booking_trn_pk = bct.booking_trn_fk";
                strSQL +=  " AND bct.GATE_IN_STATUS = 1";
                strSQL +=  " AND Bt.SPLIT_STATUS(+) = 'O'";
                strSQL +=  " and cit.container_inventory_pk = bct.container_inventory_fk";
                strSQL +=  " and CIT.LAST_MOVE_CODE_FK in";
                strSQL +=  " (select m.movecode_pk";
                strSQL +=  " from movecode_mst_tbl m";
                strSQL +=  " where m.movecode_id IN ('OFO', 'TSM', 'TSF'))";
                strSQL +=  " and cst.commercial_schedule_trn_pk=brt.commercial_schedule_trn_fk";
                strSQL +=  " and csh.commercial_schedule_hdr_pk = cst.commercial_schedule_hdr_fk";
                strSQL +=  " and csh.vessel_mst_fk = vmt.vessel_mst_pk";
                strSQL +=  " and bbt.booking_trn_fk(+) = bt.booking_trn_pk";
                strSQL +=  " and (BBT.BL_STATUS IS NULL OR BBT.BL_STATUS <> 5)";
                strSQL +=  " and ctm.container_type_mst_pk = bct.container_type_mst_fk";
                strSQL +=  " and cmt.customer_mst_pk = bt.customer_mst_fk";
                strSQL +=  " and ctmt.customer_type_pk = 4";
                strSQL +=  " AND BRt.COMMERCIAL_SCHEDULE_TRN_FK = " + VesselPK + "";
                strSQL +=  " and pod.port_mst_pk = bt.pod_fk";
                strSQL +=  " and poo.port_mst_pk(+)=bt.poo_fk";
                strSQL +=  " and pol.port_mst_pk=bt.pol_fk";
                strSQL +=  " and pfd.port_mst_pk(+)=bt.pfd_fk";
                strSQL +=  " and pmt.port_mst_pk=brt.pod_mst_fk";
                strSQL +=  " order by bct.gate_in_date";
                return objWF.GetDataSet(strSQL);
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
        #endregion

        #region "Get ETD for Vessel"
        public string FetchVesselETD(int intvesselpk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = " select cst.etd from commercial_schedule_trn cst ";
            strSQL += " where cst.commercial_schedule_trn_pk=" + intvesselpk + "";
            try
            {
                return objWF.ExecuteScaler(strSQL);
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

        #endregion

        #region "Fetch Empty & Full Move Codes For Load Confirmation"
        public DataSet FetchLoadMoves()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " select m.movecode_pk MOVECODE_FK from movecode_mst_tbl m where m.movecode_id in ('BEM','BFF')";
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
        #endregion

        #region "Save For Load Confirmation"
        public object SaveLoad(DataSet dsBooking)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;

                OracleCommand insCommand = new OracleCommand();

                var _with15 = insCommand;
                _with15.Connection = objWK.MyConnection;
                _with15.CommandType = CommandType.StoredProcedure;
                _with15.CommandText = objWK.MyUserName + ".CONTAINER_MOVE_DTL_PKG.BOOKING_CONTAINERS_LOAD";
                var _with16 = _with15.Parameters;
                insCommand.Parameters.Add("BOOKING_CONTAINERS_PK_IN", OracleDbType.Int32, 10, "BOOKING_CONTAINERS_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["BOOKING_CONTAINERS_PK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("DEPARTURE_STATUS_IN", OracleDbType.Int32, 1, "DEPARTURE_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEPARTURE_STATUS_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("STOWAGE_POS_IN", OracleDbType.Varchar2, 7, "STOWAGE_POS").Direction = ParameterDirection.Input;
                insCommand.Parameters["STOWAGE_POS_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleDbType.Varchar2, 12, "DEPARTURE_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("DEPARTURE_VOYAGE_FK_IN", Departure_Voyage_Fk).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;                

                var _with17 = objWK.MyDataAdapter;
                _with17.InsertCommand = insCommand;
                _with17.InsertCommand.Transaction = TRAN;
                RecAfct = _with17.Update(dsBooking.Tables["Booking"]);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    long lngHdrPk = 0;
                    arrMessage = Save_GateIn_Load_Arr_Hdr(TRAN);
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
        }
        #endregion

        #region "New Save Function for Load Confirmation"
        public ArrayList SaveLoadConfirm(int Booking_containers_pk, int Container_inventory_fk, int Move_Hdr_pk, int Cont_Status, int Routing_id, int SHIP_TYPE, int IBusinessModel, int Prev_Depart_Stat, int Depart_Stat, System.DateTime Depart_Date,
        string Stowage_pos, int BOOKING_ROUTING, int POT_FK, int Depart_Voy_fk, int IVersionNo, long Locpk, string TimeDiff)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();
            Int32 inti = default(Int32);
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            try
            {
                var _with18 = updCommand;
                _with18.Parameters.Clear();
                _with18.Connection = objWK.MyConnection;
                _with18.CommandType = CommandType.StoredProcedure;
                _with18.CommandText = objWK.MyUserName + ".CONTAINER_MOVEMENT_PKG.UPDATE_LOAD_CONFIRMATION";

                var _with19 = _with18.Parameters;
                _with19.Add("BOOKING_CONTAINERS_PK_IN", Booking_containers_pk).Direction = ParameterDirection.Input;

                _with19.Add("CONTAINER_INVENTORY_TRN_FK_IN", Container_inventory_fk).Direction = ParameterDirection.Input;

                _with19.Add("CONTAINER_MOVE_HDR_FK_IN", Move_Hdr_pk).Direction = ParameterDirection.Input;

                _with19.Add("CONTAINER_STATUS_IN", Cont_Status).Direction = ParameterDirection.Input;

                _with19.Add("ROUTING_ID_IN", Routing_id).Direction = ParameterDirection.Input;

                _with19.Add("SHIPMENT_TYPE_IN", SHIP_TYPE).Direction = ParameterDirection.Input;

                _with19.Add("BUSINESS_MODEL_IN", IBusinessModel).Direction = ParameterDirection.Input;

                _with19.Add("PREV_DEP_STATUS_IN", Prev_Depart_Stat).Direction = ParameterDirection.Input;

                _with19.Add("DEP_STATUS_IN", Depart_Stat).Direction = ParameterDirection.Input;

                _with19.Add("DEP_DATE_IN", Depart_Date).Direction = ParameterDirection.Input;

                _with19.Add("STOWAGE_POSITION_IN", Stowage_pos).Direction = ParameterDirection.Input;

                _with19.Add("BOOKING_ROUTING_FK_IN", BOOKING_ROUTING).Direction = ParameterDirection.Input;

                _with19.Add("POT_FK_IN", POT_FK).Direction = ParameterDirection.Input;

                _with19.Add("DEPARTURE_VOYAGE_FK_IN", Depart_Voy_fk).Direction = ParameterDirection.Input;

                _with19.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with19.Add("VERSION_NO_IN", IVersionNo).Direction = ParameterDirection.Input;
                _with19.Add("LOCATION_MST_FK_IN", Locpk).Direction = ParameterDirection.Input;
                _with19.Add("TIME_DIFF_IN", TimeDiff).Direction = ParameterDirection.Input;

                _with19.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                

                var _with20 = objWK.MyDataAdapter;

                _with20.UpdateCommand = updCommand;
                _with20.UpdateCommand.Transaction = TRAN;
                _with20.UpdateCommand.ExecuteNonQuery();

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //Next

                if (arrMessage.Count == 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }
        #endregion

        #region "Fetch for Arrival Confirm"
        public DataSet FetchArrivalConfirm(long Commercial_Sch_trn_Pk, Int64 PortPK, string sType = "A", Int32 iBusiness = 0, bool iTranshipment = true, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 Location = 0, Int64 intPodPk = 0, Int16 intStatus = 0,
        string strContainerno = "", string arrivaldt = "")
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strTmpTSDecode = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT COUNT(*)" ;
            strSQL += " FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT," ;
            strSQL += "       LOCATION_WORKING_PORTS_TRN LWP," ;
            strSQL += "       BOOKING_BL_TRN BL" ;

            strSQL += "      WHERE BR.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BR.POD_MST_FK = PMT.PORT_MST_PK" ;
            strSQL += "       AND CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK = CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += "       AND LWP.PORT_MST_FK=BR.POD_MST_FK " ;
            strSQL += "       AND LWP.ACTIVE = 1 " ;
            strSQL += "       AND BL.BOOKING_TRN_FK(+) = BT.BOOKING_TRN_PK " ;
            if (sType == "P")
            {
                strSQL += "       AND DECODE(BR.SL_NO,1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS) =0 " ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            strSQL += "       AND LWP.LOCATION_MST_FK in (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L    START WITH L.LOCATION_MST_PK =  " + (Int64)objPage.Session["LOGED_IN_LOC_FK"] ;
            strSQL += "     CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK )" ;
            //vijays
            strSQL += "       AND BCT.GATE_IN_STATUS=1 " ;
            strSQL += "       AND BCT.DEPARTURE_STATUS=1 " ;
            if (intStatus == 0)
            {
                strSQL += "       AND (BCT.ARRIVAL_VOYAGE_FK = " + 0 ;
                strSQL += "       OR BCT.TS_ARRIVAL_VOYAGE_FK = " + 0 + " ) " ;
            }
            if (Commercial_Sch_trn_Pk > 0)
            {
                strSQL += "       AND (BCT.ARRIVAL_VOYAGE_FK = " + Commercial_Sch_trn_Pk ;
                strSQL += "       OR BCT.TS_ARRIVAL_VOYAGE_FK = " + Commercial_Sch_trn_Pk + " ) " ;
            }
            if (intPodPk > 0)
            {
                strSQL += "       AND BR.POD_MST_FK  = " + intPodPk + " ";
            }
            if (PortPK > 0)
            {
                strSQL += "       AND BR.POL_MST_FK  = " + PortPK + " ";
            }

            if (strContainerno.Trim().Length > 0)
            {
                strSQL += "       AND BCT.CONTAINER_NO LIKE '%" + strContainerno.Trim().Replace("'", "''") + "%'";
            }
            if ((arrivaldt != null))
            {
                string arrDt = System.String.Format("{0:" + M_DateFormat + "}", Convert.ToInt32(arrivaldt));
                strSQL += "and ((bct.arrival_dt <=  to_date('" + arrDt.ToUpper() + "','" + M_DateFormat + "')) or (BCT.TS_ARRIVAL_DT<= to_date('" + arrDt.ToUpper() + "','" + M_DateFormat + "'))) ";
            }
            strSQL += " ORDER BY ARRIVAL_DT ";


            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            strTmpTSDecode = "decode((Select Count(*) from Booking_Routing_Trn y where y.pol_mst_fk = " + PortPK + " and y.booking_trn_fk = br.booking_trn_fk),0,1,2)";

            strSQL = " SELECT * FROM(" ;
            strSQL += " SELECT ROWNUM SL_NO," ;
            strSQL += " SUBQRY.* FROM(" ;
            strSQL += " SELECT " ;
            strSQL += "       DISTINCT BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += "       BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += "       BCT.CONTAINER_NO, " ;
            strSQL += "       CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += "       BCT.CONTAINER_STATUS," ;
            strSQL += strTmpTSDecode + " ROUTING_ID, ";
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,'Local','T/S') SHIP_TYPE," ;
            strSQL += "       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            strSQL += "       BL.CONSIGNEE_MST_FK, " ;
            strSQL += "       CMT.CUSTOMER_ID," ;
            strSQL += "       PMT.PORT_ID POL," ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,BCT.STOWAGE_POSITION,BCT.TS_STOWAGE_POSITION) STOWAGE_POSITION," ;
            strSQL += "       TO_CHAR(DECODE(" + strTmpTSDecode + ",1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS)) ARRIVAL_STATUS, " ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS) PREV_ARRIVAL_STATUS, " ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,BCT.ARRIVAL_DT,BCT.TS_ARRIVAL_DT) ARRIVAL_DT," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_PK," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += "       CIT.LAST_MOVE_CODE_FK," ;
            strSQL += "       LWP.location_mst_fk LOCATION_MST_FK, " ;
            strSQL += "       BCT.VERSION_NO " ;
            strSQL += " FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT," ;
            strSQL += "       BOOKING_BL_TRN BL, " ;
            strSQL += "       LOCATION_WORKING_PORTS_TRN LWP" ;
            strSQL += " WHERE BR.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BR.POD_MST_FK = PMT.PORT_MST_PK" ;
            strSQL += "       AND CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK = CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += "       AND LWP.PORT_MST_FK=BR.POD_MST_FK " ;
            strSQL += "       AND LWP.ACTIVE = 1 " ;
            strSQL += "       AND BL.BOOKING_TRN_FK(+) = BT.BOOKING_TRN_PK " ;
            if (sType == "P")
            {
                strSQL += "       AND DECODE(BR.SL_NO,1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS) =0 " ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }

            if (Location > 0)
            {
                strSQL += "       AND LWP.LOCATION_MST_FK  in (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L    START WITH L.LOCATION_MST_PK =   " + Location + "    CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK )" ;
            }
            else
            {
                strSQL += "       AND LWP.LOCATION_MST_FK in (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L    START WITH L.LOCATION_MST_PK =  " + (Int64)objPage.Session["LOGED_IN_LOC_FK"] ;
                strSQL += "      CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK )" ;
            }

            strSQL += "       AND BCT.GATE_IN_STATUS=1 " ;
            strSQL += "       AND BCT.DEPARTURE_STATUS=1 " ;
            if (intStatus == 0)
            {
                strSQL += "       AND (BCT.ARRIVAL_VOYAGE_FK = " + 0 ;
                strSQL += "       OR BCT.TS_ARRIVAL_VOYAGE_FK = " + 0 + " ) " ;
            }
            if (Commercial_Sch_trn_Pk > 0)
            {
                strSQL += "       AND (BCT.ARRIVAL_VOYAGE_FK = " + Commercial_Sch_trn_Pk ;
                strSQL += "       OR BCT.TS_ARRIVAL_VOYAGE_FK = " + Commercial_Sch_trn_Pk + " ) " ;
            }

            if (PortPK > 0)
            {
                strSQL += "       AND BR.POL_MST_FK  = " + PortPK + " ";
            }

            if (intPodPk > 0)
            {
                strSQL += "       AND BR.POD_MST_FK  = " + intPodPk + " ";
            }
            if (strContainerno.Trim().Length > 0)
            {
                strSQL += "       AND BCT.CONTAINER_NO LIKE '%" + strContainerno.Trim().Replace("'", "''") + "%'";
            }

            if ((arrivaldt != null))
            {
                string arrDt = System.String.Format("{0:" + M_DateFormat + "}", Convert.ToInt32(arrivaldt));
                strSQL += "AND ((bct.arrival_dt =  to_date('" + arrDt.ToUpper() + "','" + M_DateFormat + "')) or (BCT.TS_ARRIVAL_DT = to_date('" + arrDt.ToUpper() + "','" + M_DateFormat + "'))) ";
            }
            strSQL += " ORDER BY ARRIVAL_DT)SUBQRY)WHERE SL_NO  Between " + start + " and " + last;

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
        #endregion

        #region "PromptBal"
        public DataSet PromptBal(long Commercial_Sch_trn_Pk, Int64 PortPK, bool iTranshipment = true)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT * FROM(" ;
            strSQL += " SELECT ROWNUM SL_NO," ;
            strSQL += " SUBQRY.* FROM(" ;
            strSQL += " SELECT " ;
            strSQL += "       BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += "       LWP.LOCATION_MST_FK, " ;
            strSQL += "       BCT.VERSION_NO " ;
            strSQL += " FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT," ;
            strSQL += "       BOOKING_BL_TRN BL," ;
            strSQL += "       LOCATION_WORKING_PORTS_TRN LWP," ;
            strSQL += "       BL_CONTAINERS_TRN BLC" ;
            strSQL += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            strSQL += "       AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            strSQL += "       AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BL.CONSIGNEE_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BT.POD_FK=PMT.PORT_MST_PK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BL.BOOKING_BL_PK=BLC.BOOKING_BL_FK " ;
            strSQL += "       AND BLC.BOOKING_CONTAINERS_FK=BCT.BOOKING_CONTAINERS_PK  " ;
            strSQL += "       AND LWP.PORT_MST_FK=BR.POD_MST_FK " ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += "       AND (BCT.ARRIVAL_VOYAGE_FK = " + Commercial_Sch_trn_Pk ;
            strSQL += "       OR BCT.TS_ARRIVAL_VOYAGE_FK = " + Commercial_Sch_trn_Pk + " ) " ;
            strSQL += " ORDER BY ARRIVAL_DT)SUBQRY)ORDER BY SL_NO  " ;

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
        #endregion

        #region "PromptBalDetails1"
        public DataSet PromptBalDetails1(long Commercial_Sch_trn_Pk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT SUM(C20GP) GP20,SUM(C40GP) GP40,SUM(C20RF) RF20,SUM(C40RF) RF40,SUM(C45GP) GP45 FROM (SELECT Q.* FROM (SELECT" ;
            strSQL += " SUM( CASE WHEN BC.CONTAINER_TYPE_MST_FK=4 THEN 1 ELSE 0 END ) C20GP, " ;
            strSQL += "       SUM( CASE WHEN BC.CONTAINER_TYPE_MST_FK=82 THEN 1 ELSE 0 END ) C40GP," ;
            strSQL += "       SUM( CASE WHEN BC.CONTAINER_TYPE_MST_FK=322 THEN 1 ELSE 0 END ) C20RF," ;
            strSQL += "       SUM( CASE WHEN BC.CONTAINER_TYPE_MST_FK=101 THEN 1 ELSE 0 END ) C40RF," ;
            strSQL += "       SUM( CASE WHEN BC.CONTAINER_TYPE_MST_FK=284 THEN 1 ELSE 0 END ) C45GP" ;
            strSQL += "       FROM BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC, COMMERCIAL_SCHEDULE_TRN CST," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTM," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CT" ;
            strSQL += "WHERE  BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            strSQL += "       AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            strSQL += "       AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTM.CUSTOMER_TYPE_PK(+)";
            strSQL += "       AND BC.CONTAINER_TYPE_MST_FK=CT.CONTAINER_TYPE_MST_PK AND CST.COMMERCIAL_SCHEDULE_TRN_PK = BR.COMMERCIAL_SCHEDULE_TRN_FK" ;
            strSQL += "       AND BCT.DEPARTURE_STATUS = 1" ;
            strSQL += "       AND CST.COMMERCIAL_SCHEDULE_TRN_PK = " + Commercial_Sch_trn_Pk ;
            strSQL += "GROUP BY CTM.CUSTOMER_TYPE_ID, BC.CONTAINER_STATUS) Q) " ;
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
        #endregion

        #region "FetchArrivalConfirm1"
        public DataSet FetchArrivalConfirm1(long Commercial_Sch_trn_Pk, string sType, Int32 iBusiness = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT  ROWNUM SR_NO," ;
            strSQL += " BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += " BCT.CONTAINER_NO, " ;
            strSQL += " CTM.CONTAINER_TYPE_MST_PK," ;
            strSQL += " CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += " DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            strSQL += " BT.CUSTOMER_MST_FK," ;
            strSQL += " CMT.CUSTOMER_ID," ;
            strSQL += " PMT.PORT_ID POD," ;
            strSQL += " TO_CHAR(BCT.ARRIVAL_STATUS) ARRIVAL_STATUS," ;
            strSQL += " BCT.ARRIVAL_DT," ;
            strSQL += " CTMT.CUSTOMER_TYPE_ID BIZMODEL," ;
            strSQL += " BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += " CIT.LAST_MOVE_CODE_FK," ;
            strSQL += " MMT.MOVECODE_ID," ;
            strSQL += " BCT.VERSION_NO BCT_VERSION," ;
            strSQL += " CIT.VERSION_NO CIT_VERSION,BCT.SHIPMENT_TYPE,ARRIVAL_STATUS TEMP_STATUS,CTMT.CUSTOMER_TYPE_PK,BT.BOOKING_TRN_PK,BT.LOCATION_MST_FK,BCT.STOWAGE_POSITION " ;
            strSQL += " FROM BOOKING_TRN BT," ;
            strSQL += " BOOKING_ROUTING_TRN BR," ;
            strSQL += " BOOKING_CTYPE_TRN BC," ;
            strSQL += " BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += " CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += " CUSTOMER_MST_TBL CMT," ;
            strSQL += " PORT_MST_TBL PMT," ;
            strSQL += " CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += " CONTAINER_INVENTORY_TRN CIT," ;
            strSQL += " MOVECODE_MST_TBL MMT " ;

            strSQL += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            strSQL += " AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            strSQL += " AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            strSQL += " AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += " AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += " AND BT.POD_FK=PMT.PORT_MST_PK" ;
            strSQL += " AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += " AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += " AND MMT.MOVECODE_PK(+)=CIT.LAST_MOVE_CODE_FK AND BCT.DEPARTURE_STATUS=1 " ;
            strSQL += " AND BCT.Arrival_Voyage_Fk=" + Commercial_Sch_trn_Pk;
            if (sType == "P")
            {
                strSQL += " AND BCT.ARRIVAL_STATUS=0 " ;
            }

            if (iBusiness > 0)
            {
                strSQL += " AND BT.BUSINESS_MODEL= " + iBusiness ;
            }

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
        #endregion

        #region "Fetch Empty & Full Move Codes For Arrival Confirmation"
        public DataSet FetchArrivalMoves()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " select m.movecode_pk MOVECODE_FK from movecode_mst_tbl m where m.movecode_id in ('EMD','IFD')";
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
        #endregion

        #region "Save For Load Confirmation"
        public object SaveArrival(DataSet dsBooking)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;

                OracleCommand insCommand = new OracleCommand();

                var _with21 = insCommand;
                _with21.Connection = objWK.MyConnection;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWK.MyUserName + ".CONTAINER_MOVE_DTL_PKG.BOOKING_CONTAINERS_ARRIVAL";
                var _with22 = _with21.Parameters;
                insCommand.Parameters.Add("BOOKING_CONTAINERS_PK_IN", OracleDbType.Int32, 10, "BOOKING_CONTAINERS_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["BOOKING_CONTAINERS_PK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("ARRIVAL_STATUS_IN", OracleDbType.Int32, 1, "ARRIVAL_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["ARRIVAL_STATUS_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleDbType.Varchar2, 12, "ARRIVAL_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;                

                var _with23 = objWK.MyDataAdapter;
                _with23.InsertCommand = insCommand;
                _with23.InsertCommand.Transaction = TRAN;
                RecAfct = _with23.Update(dsBooking.Tables["Booking"]);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    long lngHdrPk = 0;
                    arrMessage = Save_GateIn_Load_Arr_Hdr(TRAN);
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
        }
        #endregion

        #region "Save Move Header"
        public int SaveMoveHeader(DataSet M_Dataset)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleCommand inscommand = new OracleCommand();
            OracleTransaction TRAN = null;
            System.DBNull StrNUll = null;
            Int32 inti = default(Int32);
            Int64 PKValue = default(Int64);
            long lngMoveHdr_Pk = 0;
            try
            {
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with24 = objWK.MyCommand;
                _with24.Parameters.Add("MOVE_REF_NO_IN", M_Dataset.Tables[0].Rows[inti][1]).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("MOVE_DT_IN", M_Dataset.Tables[0].Rows[inti][2]).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("MOVE_CODE_FK_IN", M_Dataset.Tables[0].Rows[inti][3]).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                objWK.MyCommand.CommandText = objWK.MyUserName + ".CONTAINER_MOVEMENT_PKG.CONTAINER_MOVE_HDR_INS";


                if (objWK.ExecuteCommands() == true)
                {
                    lngMoveHdr_Pk = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                    if (lngMoveHdr_Pk == 0)
                    {
                        lngMoveHdr_Pk = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                    }

                    return Convert.ToInt32(lngMoveHdr_Pk);
                }
                else
                {
                    return -1;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
            }
            return 0;
        }
        #endregion

        #region "Fetch All GateIn Records"
        public DataSet AllGateInRecords(long Commercial_Sch_trn_Pk, Int64 PortPK, bool iTranshipment, string sType = "A", Int32 iBusiness = 0)
        {
            string StrSql = null;
            WorkFlow Objwk = new WorkFlow();

            StrSql = " SELECT * FROM(" ;
            StrSql += " SELECT ROWNUM SL_NO," ;
            StrSql += " SUBQRY.* FROM(" ;
            StrSql += " SELECT " ;
            StrSql += "       BCT.BOOKING_CONTAINERS_PK, " ;
            StrSql += "       BCT.CONTAINER_INVENTORY_FK," ;
            StrSql += "       BCT.CONTAINER_NO, " ;
            StrSql += "       CTM.CONTAINER_TYPE_MST_ID," ;
            StrSql += "       BCT.CONTAINER_STATUS," ;
            StrSql += "       BR.SL_NO ROUTING_ID," ;
            StrSql += "       DECODE(BR.SL_NO,1,'Local','T/S') SHIP_TYPE," ;
            StrSql += "       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            StrSql += "       CMT.CUSTOMER_ID," ;
            StrSql += "       PMT.PORT_ID POD," ;
            StrSql += "       TO_CHAR(BCT.GATE_IN_STATUS) GATE_IN_STATUS, " ;
            StrSql += "       TO_CHAR(BCT.GATE_IN_STATUS) PREV_GATE_IN_STATUS, " ;
            StrSql += "       BCT.GATE_IN_DATE," ;
            StrSql += "       CTMT.CUSTOMER_TYPE_PK," ;
            StrSql += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            StrSql += "       CIT.LAST_MOVE_CODE_FK," ;
            StrSql += "       BCT.VERSION_NO, " ;
            StrSql += "       BT.SHIPMENT_TYPE " ;
            StrSql += " FROM  BOOKING_TRN BT," ;
            StrSql += "       BOOKING_ROUTING_TRN BR," ;
            StrSql += "       BOOKING_CTYPE_TRN BC," ;
            StrSql += "       BOOKING_CONTAINERS_TRN BCT," ;
            StrSql += "       CONTAINER_TYPE_MST_TBL CTM," ;
            StrSql += "       CUSTOMER_MST_TBL CMT," ;
            StrSql += "       PORT_MST_TBL PMT," ;
            StrSql += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            StrSql += "       CONTAINER_INVENTORY_TRN CIT" ;
            StrSql += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            StrSql += "       AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            StrSql += "       AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            StrSql += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            StrSql += "       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            StrSql += "       AND BT.POD_FK=PMT.PORT_MST_PK" ;
            StrSql += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            StrSql += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            if (sType == "P")
            {
                StrSql += "       AND BCT.GATE_IN_STATUS=0 " ;
            }
            if (iTranshipment)
            {
                StrSql += "       AND(BT.SHIPMENT_TYPE=1 OR BT.SHIPMENT_TYPE=2)" ;
            }
            else
            {
                StrSql += "       AND BT.SHIPMENT_TYPE=1" ;
            }
            if (iBusiness > 0)
            {
                StrSql += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            if (Commercial_Sch_trn_Pk > 0)
            {
                StrSql += "       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk ;
            }

            StrSql += " ORDER BY BCT.GATE_IN_DATE)SUBQRY) ";
            try
            {
                return Objwk.GetDataSet(StrSql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }
        #endregion

        #region "GetExcelColumn + Save Excel Data + Release Function After Export  "
        public string SaveExcelData(DataSet M_Dataset, Int64 Full_Move_HDR_fk, Int64 Empty_Move_HDR_fk)
        {
            OracleCommand Cmd = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            int I = 0;
            string SRet_Value = null;
            System.DateTime GateInDate = default(System.DateTime);
            objWK.OpenConnection();

            try
            {

                objWK.MyCommand.Connection = objWK.MyConnection;
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".CONTAINER_MOVEMENT_PKG.UPDATE_GATE_IN";
                for (I = 0; I <= M_Dataset.Tables[0].Rows.Count - 1; I++)
                {
                    if (Convert.ToInt32(M_Dataset.Tables[0].Rows[I]["GATE_IN_STATUS"]) == 1 | !string.IsNullOrEmpty(M_Dataset.Tables[0].Rows[I]["GATE_IN_DATE"].ToString()))
                    {
                        var _with25 = objWK.MyCommand.Parameters;
                        _with25.Add("BOOKING_CONTAINERS_PK_IN", M_Dataset.Tables[0].Rows[I]["BOOKING_CONTAINERS_PK"]).Direction = ParameterDirection.Input;
                        _with25.Add("CONTAINER_INVENTORY_TRN_FK_IN", M_Dataset.Tables[0].Rows[I]["CONTAINER_INVENTORY_FK"]).Direction = ParameterDirection.Input;
                        if (Convert.ToInt32(M_Dataset.Tables[0].Rows[I]["CONTAINER_STATUS"]) == 2)
                        {
                            _with25.Add("CONTAINER_MOVE_HDR_FK_IN", Full_Move_HDR_fk).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with25.Add("CONTAINER_MOVE_HDR_FK_IN", Empty_Move_HDR_fk).Direction = ParameterDirection.Input;
                        }

                        _with25.Add("CONTAINER_STATUS_IN", M_Dataset.Tables[0].Rows[I]["CONTAINER_STATUS"]).Direction = ParameterDirection.Input;
                        _with25.Add("BUSINESS_MODEL_IN", M_Dataset.Tables[0].Rows[I]["CUSTOMER_TYPE_PK"]).Direction = ParameterDirection.Input;
                        _with25.Add("PREV_GATE_IN_STATUS_IN", M_Dataset.Tables[0].Rows[I]["PREV_GATE_IN_STATUS"]).Direction = ParameterDirection.Input;
                        _with25.Add("GATE_IN_STATUS_IN", M_Dataset.Tables[0].Rows[I]["GATE_IN_STATUS"]).Direction = ParameterDirection.Input;
                        GateInDate = Convert.ToDateTime(M_Dataset.Tables[0].Rows[I]["GATE_IN_DATE"]);
                        _with25.Add("GATE_IN_DATE_IN", GateInDate).Direction = ParameterDirection.Input;
                        _with25.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                        _with25.Add("VERSION_NO_IN", M_Dataset.Tables[0].Rows[I]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        _with25.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, SRet_Value).Direction = ParameterDirection.Output;
                        if (objWK.ExecuteCommands() == true)
                        {
                        }
                        else
                        {
                            return Convert.ToString(-1);
                        }
                        objWK.MyCommand.Parameters.Clear();
                    }
                }

            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return "";
        }
        public void ReleaseComObject(object Reference)
        {
            try
            {
                while (!(System.Runtime.InteropServices.Marshal.ReleaseComObject(Reference) <= 0))
                {
                }
            }
            catch
            {
            }
            finally
            {
                Reference = null;
                System.GC.Collect();
                System.GC.WaitForPendingFinalizers();
            }
        }
        public string GetExcelColumn(int col)
        {
            string result = null;
            switch (col)
            {
                case 0:
                    // first column
                    result = "A";
                    break;
                case 1:
                    result = "B";
                    break;
                case 2:
                    result = "C";
                    break;
                case 3:
                    result = "D";
                    break;
                case 4:
                    result = "E";
                    break;
                case 5:
                    result = "F";
                    break;
                case 6:
                    result = "G";
                    break;
                case 7:
                    result = "H";
                    break;
                case 8:
                    result = "I";
                    break;
                case 9:
                    result = "J";
                    break;
                case 10:
                    result = "K";
                    break;
                case 11:
                    result = "L";
                    break;
                case 12:
                    result = "M";
                    break;
                case 13:
                    result = "N";
                    break;
                case 14:
                    result = "O";
                    break;
                case 15:
                    result = "P";
                    break;
                case 16:
                    result = "Q";
                    break;
                case 17:
                    result = "R";
                    break;
                case 18:
                    result = "S";
                    break;
                case 19:
                    result = "T";
                    break;
                case 20:
                    result = "U";
                    break;
                case 21:
                    result = "V";
                    break;
                case 22:
                    result = "W";
                    break;
                case 23:
                    result = "X";
                    break;
                case 24:
                    result = "Y";
                    break;
                case 25:
                    result = "Z";
                    break;
            }
            return result;
        }
        public string SaveLoadConfirmationExcel(DataSet M_Dataset, Int64 Full_Move_HDR_fk, Int64 Empty_Move_HDR_fk, int Depart_Voy_fk, Int64 SHIP_TYPE)
        {
            OracleCommand Cmd = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            int I = 0;
            string RETURN_VALUE = null;
            objWK.OpenConnection();

            try
            {
                objWK.MyCommand.Connection = objWK.MyConnection;
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".CONTAINER_MOVEMENT_PKG.UPDATE_LOAD_CONFIRMATION";
                for (I = 0; I <= M_Dataset.Tables[0].Rows.Count - 1; I++)
                {
                    var _with26 = objWK.MyCommand.Parameters;


                    _with26.Add("BOOKING_CONTAINERS_PK_IN", M_Dataset.Tables[0].Rows[I]["Booking_containers_pk"]).Direction = ParameterDirection.Input;

                    _with26.Add("CONTAINER_INVENTORY_TRN_FK_IN", M_Dataset.Tables[0].Rows[I]["Container_inventory_fk"]).Direction = ParameterDirection.Input;
                    if (Convert.ToInt32(M_Dataset.Tables[0].Rows[I]["CONTAINER_STATUS"]) == 2)
                    {
                        _with26.Add("CONTAINER_MOVE_HDR_FK_IN", Full_Move_HDR_fk).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with26.Add("CONTAINER_MOVE_HDR_FK_IN", Empty_Move_HDR_fk).Direction = ParameterDirection.Input;
                    }


                    _with26.Add("CONTAINER_STATUS_IN", M_Dataset.Tables[0].Rows[I]["CONTAINER_STATUS"]).Direction = ParameterDirection.Input;

                    _with26.Add("ROUTING_ID_IN", M_Dataset.Tables[0].Rows[I]["ROUTING_ID"]).Direction = ParameterDirection.Input;

                    _with26.Add("SHIPMENT_TYPE_IN", SHIP_TYPE).Direction = ParameterDirection.Input;

                    _with26.Add("BUSINESS_MODEL_IN", M_Dataset.Tables[0].Rows[I]["CUSTOMER_TYPE_PK"]).Direction = ParameterDirection.Input;

                    _with26.Add("PREV_DEP_STATUS_IN", M_Dataset.Tables[0].Rows[I]["PREV_DEPARTURE_STATUS"]).Direction = ParameterDirection.Input;

                    _with26.Add("DEP_STATUS_IN", M_Dataset.Tables[0].Rows[I]["DEPARTURE_STATUS"]).Direction = ParameterDirection.Input;

                    _with26.Add("DEP_DATE_IN", M_Dataset.Tables[0].Rows[I]["DEPARTURE_DATE"]).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(Convert.ToString(M_Dataset.Tables[0].Rows[I]["STOWAGE_POSITION"])))
                    {
                        M_Dataset.Tables[0].Rows[I]["STOWAGE_POSITION"] = "";
                    }

                    if (Convert.ToBoolean(Convert.ToString(M_Dataset.Tables[0].Rows[I]["STOWAGE_POSITION"]).Trim().Length))
                    {
                        _with26.Add("STOWAGE_POSITION_IN", M_Dataset.Tables[0].Rows[I]["STOWAGE_POSITION"].ToString().Trim()).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with26.Add("STOWAGE_POSITION_IN", System.DBNull.Value).Direction = ParameterDirection.Input;
                    }


                    _with26.Add("BOOKING_ROUTING_FK_IN", M_Dataset.Tables[0].Rows[I]["BOOKING_ROUTING_PK"]).Direction = ParameterDirection.Input;

                    _with26.Add("POT_FK_IN", M_Dataset.Tables[0].Rows[I]["POD_MST_FK"]).Direction = ParameterDirection.Input;

                    _with26.Add("DEPARTURE_VOYAGE_FK_IN", Depart_Voy_fk).Direction = ParameterDirection.Input;

                    _with26.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                    _with26.Add("VERSION_NO_IN", M_Dataset.Tables[0].Rows[I]["Version_No"]).Direction = ParameterDirection.Input;

                    _with26.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    if (objWK.ExecuteCommands() == true)
                    {
                    }
                    else
                    {
                        // Return -1
                    }
                    objWK.MyCommand.Parameters.Clear();
                }

            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return "";
        }
        public string SaveArrivalConfirmExcel(DataSet M_Dataset, Int64 Full_Move_HDR_fk, Int64 Empty_Move_HDR_fk)
        {
            WorkFlow ObjWk = new WorkFlow();
            int i = 0;
            string SRet_Value = null;
            try
            {
                ObjWk.MyCommand.Connection = ObjWk.MyConnection;
                ObjWk.MyCommand.CommandType = CommandType.StoredProcedure;
                ObjWk.MyCommand.CommandText = ObjWk.MyUserName + ".CONTAINER_MOVEMENT_PKG.UPDATE_DISCHARGE_CONFIRMATOIN";
                for (i = 0; i <= M_Dataset.Tables[0].Rows.Count - 1; i++)
                {
                    var _with27 = ObjWk.MyCommand.Parameters;
                    _with27.Add("BOOKING_CONTAINERS_PK_IN", M_Dataset.Tables[0].Rows[i]["BOOKING_CONTAINERS_PK"]).Direction = ParameterDirection.Input;
                    _with27.Add("CONTAINER_INVENTORY_TRN_FK_IN", M_Dataset.Tables[0].Rows[i]["CONTAINER_INVENTORY_FK"]).Direction = ParameterDirection.Input;
                    if (Convert.ToInt32(M_Dataset.Tables[0].Rows[i]["CONTAINER_STATUS"] )== 2)
                    {
                        _with27.Add("CONTAINER_MOVE_HDR_FK_IN", Full_Move_HDR_fk).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Add("CONTAINER_MOVE_HDR_FK_IN", Empty_Move_HDR_fk).Direction = ParameterDirection.Input;
                    }
                    _with27.Add("ROUTING_ID_IN", M_Dataset.Tables[0].Rows[i]["ROUTING_ID"]).Direction = ParameterDirection.Input;
                    _with27.Add("CONTAINER_STATUS_IN", M_Dataset.Tables[0].Rows[i]["CONTAINER_STATUS"]).Direction = ParameterDirection.Input;
                    _with27.Add("BUSINESS_MODEL_IN", M_Dataset.Tables[0].Rows[i]["CUSTOMER_TYPE_PK"]).Direction = ParameterDirection.Input;
                    _with27.Add("PREV_DISCH_STATUS_IN", M_Dataset.Tables[0].Rows[i]["PREV_ARRIVAL_STATUS"]).Direction = ParameterDirection.Input;
                    _with27.Add("DISCH_STATUS_IN", M_Dataset.Tables[0].Rows[i]["ARRIVAL_STATUS"]).Direction = ParameterDirection.Input;
                    _with27.Add("DISCH_DATE_IN", M_Dataset.Tables[0].Rows[i]["ARRIVAL_DT"]).Direction = ParameterDirection.Input;
                    _with27.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with27.Add("LOCATION_MST_FK_IN", M_Dataset.Tables[0].Rows[i]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with27.Add("CONSIGNEE_MST_FK_IN", M_Dataset.Tables[0].Rows[i]["CONSIGNEE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with27.Add("VERSION_NO_IN", M_Dataset.Tables[0].Rows[i]["Version_No"]).Direction = ParameterDirection.Input;
                    _with27.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, SRet_Value).Direction = ParameterDirection.Output;
                    if (ObjWk.ExecuteCommands() == true)
                    {
                    }
                    else
                    {
                    }
                    ObjWk.MyCommand.Parameters.Clear();
                }
            }
            catch (OracleException oraexp)
            {
                ErrorMessage = oraexp.Message;
                throw oraexp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return "";
        }
        #endregion

        #region "Fetch All LoadConfirmation Records"
        public DataSet AllLoadConfirmation(long Commercial_Sch_trn_Pk, Int64 PortPK, string sType = "A", Int32 iBusiness = 0)
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT * FROM ( " ;
            strSQL += " SELECT ROWNUM SL_NO, QRY.* FROM ( SELECT " ;
            strSQL += "       BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += "       BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += "       BCT.CONTAINER_NO, " ;
            strSQL += "       BCT.STOWAGE_POSITION, " ;
            strSQL += "       CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += "       BCT.CONTAINER_STATUS," ;
            strSQL += "       BR.SL_NO ROUTING_ID," ;
            strSQL += "       DECODE(BR.SL_NO,1,'Local','T/S') SHIP_TYPE," ;
            strSQL += "       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            strSQL += "       CMT.CUSTOMER_ID," ;
            strSQL += "       PMT.PORT_ID POD," ;
            strSQL += "       TO_CHAR(DECODE(BR.SL_NO,1,BCT.DEPARTURE_STATUS, BCT.TS_DEP_STATUS)) DEPARTURE_STATUS, " ;
            strSQL += "       DECODE(BR.SL_NO,1,BCT.DEPARTURE_STATUS, BCT.TS_DEP_STATUS) PREV_DEPARTURE_STATUS, " ;
            strSQL += "       BCT.DEPARTURE_DATE," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_PK," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += "       CIT.LAST_MOVE_CODE_FK," ;
            strSQL += "       BR.BOOKING_ROUTING_PK," ;
            strSQL += "       BR.POD_MST_FK, " ;
            strSQL += "       BCT.VERSION_NO " ;
            strSQL += " FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT" ;
            strSQL += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            strSQL += "       AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            strSQL += "       AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BR.POD_MST_FK=PMT.PORT_MST_PK" ;
            strSQL += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += "       AND BCT.GATE_IN_STATUS=1 " ;
            if (sType == "P")
            {
                strSQL += "       AND BCT.DEPARTURE_STATUS=0 " ;
            }
            if (iBusiness > 0)
            {
                strSQL += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            strSQL += "       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = " + Commercial_Sch_trn_Pk ;
            strSQL += " ORDER BY BCT.GATE_IN_DATE)  QRY )" ;

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
        #endregion

        #region "All Arrival Confirmation Records"
        public DataSet FetchAllArrivalConfirm(long Commercial_Sch_trn_Pk, Int64 PortPK, string sType = "A", Int32 iBusiness = 0, bool iTranshipment = true)
        {
            string StrSql = null;
            WorkFlow Objwk = new WorkFlow();

            StrSql = " SELECT * FROM(" ;
            StrSql += " SELECT ROWNUM SL_NO," ;
            StrSql += " SUBQRY.* FROM(" ;
            StrSql += " SELECT " ;
            StrSql += "       BCT.BOOKING_CONTAINERS_PK, " ;
            StrSql += "       BCT.CONTAINER_INVENTORY_FK," ;
            StrSql += "       BCT.CONTAINER_NO, " ;
            StrSql += "       CTM.CONTAINER_TYPE_MST_ID," ;
            StrSql += "       BCT.CONTAINER_STATUS," ;
            StrSql += "       BR.SL_NO ROUTING_ID," ;
            StrSql += "       DECODE(BR.SL_NO,1,'Local','T/S') SHIP_TYPE," ;
            StrSql += "       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            StrSql += "       BL.CONSIGNEE_MST_FK, " ;
            StrSql += "       CMT.CUSTOMER_ID," ;
            StrSql += "       PMT.PORT_ID POD," ;
            StrSql += "       DECODE(BR.SL_NO,1,BCT.STOWAGE_POSITION,BCT.TS_STOWAGE_POSITION) STOWAGE_POSITION," ;
            StrSql += "       TO_CHAR(DECODE(BR.SL_NO,1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS)) ARRIVAL_STATUS, " ;
            StrSql += "       DECODE(BR.SL_NO,1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS) PREV_ARRIVAL_STATUS, " ;
            StrSql += "       DECODE(BR.SL_NO,1,BCT.ARRIVAL_DT,BCT.TS_ARRIVAL_DT) ARRIVAL_DT," ;
            StrSql += "       CTMT.CUSTOMER_TYPE_PK," ;
            StrSql += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            StrSql += "       CIT.LAST_MOVE_CODE_FK," ;
            StrSql += "       LWP.LOCATION_MST_FK, " ;
            StrSql += "       BCT.VERSION_NO " ;
            StrSql += " FROM  BOOKING_TRN BT," ;
            StrSql += "       BOOKING_ROUTING_TRN BR," ;
            StrSql += "       BOOKING_CTYPE_TRN BC," ;
            StrSql += "       BOOKING_CONTAINERS_TRN BCT," ;
            StrSql += "       CONTAINER_TYPE_MST_TBL CTM," ;
            StrSql += "       CUSTOMER_MST_TBL CMT," ;
            StrSql += "       PORT_MST_TBL PMT," ;
            StrSql += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            StrSql += "       CONTAINER_INVENTORY_TRN CIT," ;
            StrSql += "       BOOKING_BL_TRN BL," ;
            StrSql += "       LOCATION_WORKING_PORTS_TRN LWP," ;
            StrSql += "       BL_CONTAINERS_TRN BLC" ;
            StrSql += " WHERE BT.BOOKING_TRN_PK=BR.BOOKING_TRN_FK" ;
            StrSql += "       AND BT.BOOKING_TRN_PK=BC.BOOKING_TRN_FK" ;
            StrSql += "       AND BC.BOOKING_CTYPE_PK=BCT.BOOKING_CTYPE_FK" ;
            StrSql += "       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK" ;
            StrSql += "       AND BL.CONSIGNEE_MST_FK=CMT.CUSTOMER_MST_PK" ;
            StrSql += "       AND BT.POD_FK=PMT.PORT_MST_PK" ;
            StrSql += "       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK" ;
            StrSql += "       AND BL.BOOKING_BL_PK=BLC.BOOKING_BL_FK " ;
            StrSql += "       AND LWP.PORT_MST_FK=BR.POD_MST_FK " ;
            StrSql += "       AND BLC.BOOKING_CONTAINERS_FK=BCT.BOOKING_CONTAINERS_PK  " ;
            StrSql += "       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)" ;

            if (sType == "P")
            {
                StrSql += "       AND DECODE(BR.SL_NO,1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS) =0 " ;
            }
            if (iBusiness > 0)
            {
                StrSql += "       AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            StrSql += "       AND (BCT.ARRIVAL_VOYAGE_FK = " + Commercial_Sch_trn_Pk ;
            StrSql += "       OR BCT.TS_ARRIVAL_VOYAGE_FK = " + Commercial_Sch_trn_Pk + " ) " ;
            StrSql += " ORDER BY ARRIVAL_DT)SUBQRY)";
            try
            {
                return Objwk.GetDataSet(StrSql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }
        #endregion

        #region "Fetch ETD And ETA"
        public string GetETD(Int64 Compk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = " SELECT TO_CHAR(CST.ETD,'" + M_DateTimeFormat + "')";
            Strsql += " FROM COMMERCIAL_SCHEDULE_TRN CST WHERE CST.COMMERCIAL_SCHEDULE_TRN_PK= " + Compk;
            try
            {
                return ObjWk.ExecuteScaler(Strsql);
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
        public string GetETA(Int64 Compk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = " SELECT TO_CHAR(CST.ETA,'" + M_DateTimeFormat + "')";
            Strsql += " FROM COMMERCIAL_SCHEDULE_TRN CST WHERE CST.COMMERCIAL_SCHEDULE_TRN_PK= " + Compk;
            try
            {
                return ObjWk.ExecuteScaler(Strsql);
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
        #endregion

        #region "Load Confirmation Function"
        public int GetPKValue(Int64 PKvalue, string ContainerNr)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT JOB_TRN_SEA_EXP_CONT_PK FROM");
            sb.Append("        JOB_TRN_SEA_EXP_CONT N ");
            sb.Append(" WHERE n.job_card_sea_exp_fk=" + PKvalue);
            sb.Append(" AND N.container_number='" + ContainerNr + "'");
            try
            {
                return Convert.ToInt32(ObjWk.ExecuteScaler(sb.ToString()));
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
        public ArrayList UpdateLoadConfirm(System.DateTime LOAD_DATE, int JOB_CARD_SEA_CONT_PK = 0, int JOB_CARD_SEA_EXP_FK = 0, string COTAINER_NR = "")
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            OracleCommand UpdateCommand = new OracleCommand();
            Int32 inti = default(Int32);
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();

            try
            {
                var _with28 = UpdateCommand;
                _with28.Parameters.Clear();
                _with28.Connection = objWK.MyConnection;
                _with28.CommandType = CommandType.StoredProcedure;
                _with28.CommandText = objWK.MyUserName + ".LOAD_CONFIRMDATE_PKG.LOAD_CONFIRMDATE_UPDATE";
                var _with29 = _with28.Parameters;
                _with29.Add("JOB_TRN_SEA_EXP_CONT_PK_IN", JOB_CARD_SEA_CONT_PK).Direction = ParameterDirection.Input;
                _with29.Add("JOB_CARD_SEA_EXP_FK_IN", JOB_CARD_SEA_EXP_FK).Direction = ParameterDirection.Input;
                _with29.Add("CONTAINER_NUMBER_IN", COTAINER_NR).Direction = ParameterDirection.Input;
                _with29.Add("LOAD_DATE_IN", LOAD_DATE).Direction = ParameterDirection.Input;
                _with29.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with29.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with29.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with30 = objWK.MyDataAdapter;

                _with30.UpdateCommand = UpdateCommand;
                _with30.UpdateCommand.Transaction = TRAN;
                _with30.UpdateCommand.ExecuteNonQuery();

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                if (arrMessage.Count == 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }
        #endregion

        #region "Fill Address Details"
        public DataSet FetchAddressDetails(int LogPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            try
            {
                strSQL +=  " select distinct locmst.address_line1,locmst.address_line2, ";
                strSQL +=  "locmst.address_line3, locmst.zip, locmst.city, countryMst.Country_Name, ";
                strSQL +=  "locmst.tele_phone_no, locmst.fax_no, locmst.e_mail_id,locmst.Location_name ";
                strSQL +=  "from location_mst_tbl locmst,country_mst_tbl countryMst where ";
                strSQL +=  "locmst.location_mst_pk = " + LogPK;
                strSQL +=  "AND countryMst.Country_Mst_Pk=locmst.country_mst_fk";

                return (objWF.GetDataSet(strSQL));
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
        #endregion       

        #region "FetchConfirmReport"
        public DataSet FetchConfirmReport(string VesselPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strsql = null;
            try
            {
                strsql += "       SELECT " ;
                strsql += "       BCT.BOOKING_CONTAINERS_PK, " ;
                strsql += "       BCT.CONTAINER_INVENTORY_FK," ;
                strsql += "       BCT.CONTAINER_NO, " ;
                strsql += "       CTM.CONTAINER_TYPE_MST_ID," ;
                strsql += "       BCT.CONTAINER_STATUS," ;
                strsql += "       BR.SL_NO ROUTING_ID," ;
                strsql += "       DECODE(BR.SL_NO,1,'Local','T/S') SHIP_TYPE," ;
                strsql += "       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
                strsql += "       BL.CONSIGNEE_MST_FK, " ;
                strsql += "       CMT.CUSTOMER_ID," ;
                strsql += "       CMT.CUSTOMER_NAME," ;
                strsql += "       PMT.PORT_ID POL," ;
                strsql += "       DECODE(BR.SL_NO,1,BCT.STOWAGE_POSITION,BCT.TS_STOWAGE_POSITION) STOWAGE_POSITION," ;
                strsql += "       TO_CHAR(DECODE(BR.SL_NO,1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS)) ARRIVAL_STATUS, " ;
                strsql += "       DECODE(BR.SL_NO,1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS) PREV_ARRIVAL_STATUS, " ;
                strsql += "       DECODE(BR.SL_NO,1,BCT.ARRIVAL_DT,BCT.TS_ARRIVAL_DT) ARRIVAL_DT," ;
                strsql += "       CTMT.CUSTOMER_TYPE_PK," ;
                strsql += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
                strsql += "       CIT.LAST_MOVE_CODE_FK," ;
                strsql += "       LWP.LOCATION_MST_FK, " ;
                strsql += "       BCT.VERSION_NO " ;
                strsql += " FROM  BOOKING_TRN BT," ;
                strsql += "       BOOKING_ROUTING_TRN BR," ;
                strsql += "       BOOKING_CTYPE_TRN BC," ;
                strsql += "       BOOKING_CONTAINERS_TRN BCT," ;
                strsql += "       CONTAINER_TYPE_MST_TBL CTM," ;
                strsql += "       CUSTOMER_MST_TBL CMT," ;
                strsql += "       PORT_MST_TBL PMT," ;
                strsql += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
                strsql += "       CONTAINER_INVENTORY_TRN CIT," ;
                strsql += "       BOOKING_BL_TRN BL," ;
                strsql += "       LOCATION_WORKING_PORTS_TRN LWP" ;
                strsql += " WHERE BR.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
                strsql += "       AND BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
                strsql += "       AND BCT.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
                strsql += "       AND BCT.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK" ;
                strsql += "       AND BT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK" ;
                strsql += "       AND BR.POD_MST_FK = PMT.PORT_MST_PK" ;
                strsql += "       AND CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK" ;
                strsql += "       AND BCT.CONTAINER_INVENTORY_FK = CIT.CONTAINER_INVENTORY_PK(+)" ;
                strsql += "       AND LWP.PORT_MST_FK=BR.POD_MST_FK " ;
                strsql += "       AND BL.BOOKING_TRN_FK(+) = BT.BOOKING_TRN_PK " ;
                strsql += "       AND BCT.GATE_IN_STATUS=1 " ;
                strsql += "       AND BCT.DEPARTURE_STATUS=1 " ;

                return (objWF.GetDataSet(strsql));
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
        #endregion

        #region "feTCH FOR DISCHARGE LIST"
        public DataSet FETCHDISCHARGELIST(long VESSEL, string CONTAINER = "", string BLNO = "", string CUSTOMER = "", long POLPK = 0, long PODPK = 0)
        {

            WorkFlow Objwk = new WorkFlow();
            System.Text.StringBuilder StrSql = new System.Text.StringBuilder();

            try
            {
                StrSql.Append(" SELECT * ");
                StrSql.Append(" FROM (SELECT ROWNUM SL_NO, SUBQRY.* ");
                StrSql.Append(" FROM (SELECT DISTINCT BCT.BOOKING_CONTAINERS_PK,");
                StrSql.Append(" BCT.CONTAINER_INVENTORY_FK,");
                StrSql.Append(" BCT.CONTAINER_NO,");
                StrSql.Append(" CTM.CONTAINER_TYPE_MST_ID,");
                StrSql.Append(" BCT.SEAL_NO,");
                StrSql.Append(" BCT.CONTAINER_STATUS,");
                StrSql.Append(" poo.port_id POOID,");
                StrSql.Append(" BLTRN.CONSIGNEE_MST_FK,");
                StrSql.Append("  CMT.CUSTOMER_NAME CUSTOMER_ID,");
                StrSql.Append("  PMT.PORT_ID POLID,");
                StrSql.Append("  POD.PORT_ID PODID,");
                StrSql.Append(" bt.booking_id,");
                StrSql.Append(" BLTRN.SERVICE_BL_NO BLNr,");
                StrSql.Append("  PFD.PORT_ID PFDID,");
                StrSql.Append(" VMT.VESSEL_NAME,");
                StrSql.Append("  VMT.VOYAGE_NO,");
                StrSql.Append("  VMT.ETD,");
                StrSql.Append(" VMT.ETA,");
                StrSql.Append("    decode((Select Count(*)");
                StrSql.Append("      from Booking_Routing_Trn y ");
                StrSql.Append("  where y.pol_mst_fk =" + POLPK);
                StrSql.Append(" And y.booking_trn_fk =  br.booking_trn_fk),0,1,2) ROUTING_ID,");
                StrSql.Append("      DECODE(decode((Select Count(*) from Booking_Routing_Trn y  ");
                StrSql.Append(" where y.pol_mst_fk = 721");
                StrSql.Append("     and y.booking_trn_fk =br.booking_trn_fk),0,1,2),1,'Local','T/S') SHIP_TYPE,");
                StrSql.Append("   DECODE(BCT.CONTAINER_STATUS,1,'E',2, 'F') STATUS,");
                StrSql.Append("   DECODE(decode((Select Count(*)");
                StrSql.Append("           from Booking_Routing_Trn y");
                StrSql.Append("   where y.pol_mst_fk = " + POLPK);
                StrSql.Append("          and y.booking_trn_fk =br.booking_trn_fk),0,1,2),1,");
                StrSql.Append("        BCT.STOWAGE_POSITION,");
                StrSql.Append("     BCT.TS_STOWAGE_POSITION) STOWAGE_POSITION,");
                StrSql.Append("     TO_CHAR(DECODE(decode((Select Count(*)");
                StrSql.Append("          from Booking_Routing_Trn y");
                StrSql.Append("    where y.pol_mst_fk = " + POLPK);
                StrSql.Append("        and y.booking_trn_fk =");
                StrSql.Append("              br.booking_trn_fk),0,1,2),1,");
                StrSql.Append("     BCT.ARRIVAL_STATUS,");
                StrSql.Append("    BCT.TS_ARRIVAL_STATUS)) ARRIVAL_STATUS,");
                StrSql.Append("   DECODE(decode((Select Count(*)");
                StrSql.Append("            from Booking_Routing_Trn y");
                StrSql.Append("   where y.pol_mst_fk = " + POLPK);
                StrSql.Append("    and y.booking_trn_fk =");
                StrSql.Append("      br.booking_trn_fk), 0,1,2),1,");
                StrSql.Append("     BCT.ARRIVAL_STATUS,");
                StrSql.Append("    BCT.TS_ARRIVAL_STATUS) PREV_ARRIVAL_STATUS,");
                StrSql.Append("    DECODE(decode((Select Count(*)");
                StrSql.Append("     from Booking_Routing_Trn y");
                StrSql.Append("   where y.pol_mst_fk = " + POLPK);
                StrSql.Append("         and y.booking_trn_fk =");
                StrSql.Append("    br.booking_trn_fk),0,1,2),1,");
                StrSql.Append("           BCT.ARRIVAL_DT,");
                StrSql.Append("       BCT.TS_ARRIVAL_DT) ARRIVAL_DT,");
                StrSql.Append("    CTMT.CUSTOMER_TYPE_PK,");
                StrSql.Append("   CIT.LAST_MOVE_CODE_FK,");
                StrSql.Append(" (SELECT tmt.terminal_id from Terminal_Mst_Tbl tmt ");
                StrSql.Append(" where tmt.terminal_mst_pk = bct.destination_terminal_mst_fk) terminal_id ");
                StrSql.Append("   FROM BOOKING_TRN BT,");
                StrSql.Append("   BOOKING_ROUTING_TRN BR,");
                StrSql.Append("   BOOKING_CTYPE_TRN BC,");
                StrSql.Append("   BOOKING_CONTAINERS_TRN BCT,");
                StrSql.Append("   CONTAINER_TYPE_MST_TBL CTM,");
                StrSql.Append("   CUSTOMER_MST_TBL CMT,");
                StrSql.Append("   PORT_MST_TBL PMT,");
                StrSql.Append("   CUSTOMER_TYPE_MST_TBL CTMT,");
                StrSql.Append("   CONTAINER_INVENTORY_TRN CIT,");
                StrSql.Append("   Succeeding_Moves_Mst_Tbl SCC,");
                StrSql.Append("   Movecode_Mst_Tbl         MOV,");
                StrSql.Append("    BOOKING_BL_TRN BLTRN,");
                StrSql.Append("   bl_containers_trn blc,");
                StrSql.Append("    PORT_MST_TBL PFD,");
                StrSql.Append("     PORT_MST_TBL POO,");
                StrSql.Append("      PORT_MST_TBL POD,");
                StrSql.Append("      (SELECT DISTINCT CST.COMMERCIAL_SCHEDULE_TRN_PK,");
                StrSql.Append("       VMT.VESSEL_NAME,");
                StrSql.Append("        CSH.VOYAGE_NO,");
                StrSql.Append("        CST.ETD ETD,");
                StrSql.Append("        DECODE(DMT.ATA_DT,' ',TO_CHAR(CST.ETA),TO_CHAR(DMT.ATA_DT || ' ' || DMT.ATA_TIME)) ETA");
                StrSql.Append("         FROM VESSEL_MST_TBL          VMT,");
                StrSql.Append("               COMMERCIAL_SCHEDULE_HDR CSH, DEP_MESSAGE_TRN         DMT,");
                StrSql.Append("    COMMERCIAL_SCHEDULE_TRN CST");
                StrSql.Append("   where CSH.VESSEL_MST_FK = VMT.VESSEL_MST_PK");
                StrSql.Append("           AND CST.COMMERCIAL_SCHEDULE_HDR_FK =");
                StrSql.Append("   CSH.COMMERCIAL_SCHEDULE_HDR_PK ");
                StrSql.Append("   AND CST.COMMERCIAL_SCHEDULE_TRN_PK = DMT.COMMERCIAL_SCHEDULE_TRN_FK(+)");
                StrSql.Append("               and CST.COMMERCIAL_SCHEDULE_TRN_PK =" + VESSEL + ")VMT");

                StrSql.Append("    WHERE BR.BOOKING_TRN_FK = BT.BOOKING_TRN_PK ");
                StrSql.Append("    AND BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK");
                StrSql.Append("    AND BCT.BOOKING_TRN_FK = BT.BOOKING_TRN_PK");
                StrSql.Append("     AND BCT.BOOKING_CTYPE_FK = BC.BOOKING_CTYPE_PK");
                StrSql.Append("     AND BCT.CONTAINER_TYPE_MST_FK =CTM.CONTAINER_TYPE_MST_PK");
                StrSql.Append("      AND BLTRN.CONSIGNEE_MST_FK= CMT.CUSTOMER_MST_PK");
                StrSql.Append("  and br.pol_mst_fk =PMT.PORT_MST_PK ");

                StrSql.Append("     AND CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK");

                StrSql.Append("     AND BCT.CONTAINER_INVENTORY_FK =");
                StrSql.Append("         CIT.CONTAINER_INVENTORY_PK(+)");

                StrSql.Append(" AND BLTRN.BOOKING_TRN_FK = BT.BOOKING_TRN_PK");
                StrSql.Append(" AND bltrn.booking_bl_pk=blc.booking_bl_fk");
                StrSql.Append(" AND blc.booking_containers_fk=BCT.BOOKING_CONTAINERS_PK");
                StrSql.Append("  and bltrn.booking_trn_fk = bct.booking_trn_fk");
                StrSql.Append("  and SCC.MOVECODE_FK = CIT.LAST_MOVE_CODE_FK");
                StrSql.Append("     AND SCC.NEXT_MOVECODE_FK = MOV.MOVECODE_PK");
                StrSql.Append("     AND MOV.MOVECODE_ID IN ('IFD', 'EMD', 'TSF', 'TSM')");
                StrSql.Append("   AND BT.BUSINESS_MODEL = 4");
                StrSql.Append("   AND BCT.GATE_IN_STATUS = 1");
                StrSql.Append("   AND BCT.DEPARTURE_STATUS = 1");
                StrSql.Append("   AND BCT.ARRIVAL_STATUS=1");
                StrSql.Append("   and bltrn.bl_status <> 5");
                StrSql.Append("   AND VMT.COMMERCIAL_SCHEDULE_TRN_PK = BCT.ARRIVAL_VOYAGE_FK  ");
                StrSql.Append("   AND BT.POO_FK = POO.PORT_MST_PK(+)");
                StrSql.Append("   AND BT.PFD_FK = PFD.PORT_MST_PK(+)");
                StrSql.Append("  AND BR.POD_MST_FK = POD.PORT_MST_PK");
                if (!string.IsNullOrEmpty(CONTAINER))
                {
                    StrSql.Append("   AND BCT.CONTAINER_NO LIKE '" + "%" + CONTAINER.ToUpper() + "%" + "'");
                }
                if (!string.IsNullOrEmpty(CUSTOMER))
                {
                    StrSql.Append("   AND CMT.CUSTOMER_ID LIKE '" + CUSTOMER + "%" + "'");
                }
                if (!string.IsNullOrEmpty(BLNO))
                {
                    StrSql.Append(" AND  BLTRN.SERVICE_BL_NO='" + BLNO + "'");
                }
                if (POLPK != 0)
                {
                    StrSql.Append(" AND PMT.PORT_MST_PK=" + POLPK);
                }
                if (PODPK != 0)
                {
                    StrSql.Append(" AND POD.PORT_MST_PK=" + PODPK);
                }

                if (VESSEL > 0)
                {
                    StrSql.Append("  AND (BCT.ARRIVAL_VOYAGE_FK = " + VESSEL + " ");
                    StrSql.Append("  OR BCT.TS_ARRIVAL_VOYAGE_FK = " + VESSEL + " )");
                }
                StrSql.Append("  ORDER BY ARRIVAL_DT) SUBQRY)");

                return Objwk.GetDataSet(StrSql.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Counting Total No. of Container Types"
        public DataSet FetchNoContainer(long VESSEL)
        {
            WorkFlow Objwk = new WorkFlow();
            System.Text.StringBuilder StrSql = new System.Text.StringBuilder();
            try
            {
                StrSql.Append(" SELECT ");
                StrSql.Append(" CTM.CONTAINER_TYPE_MST_ID,");
                StrSql.Append(" case when CTM.CONTAINER_TYPE_MST_ID in ");
                StrSql.Append(" (select Container_Type_Mst_ID from Container_Type_Mst_tbl where Active_Flag = 1) then   ");
                StrSql.Append(" count(CTM.CONTAINER_TYPE_MST_ID)");
                StrSql.Append(" else");
                StrSql.Append(" 0");
                StrSql.Append(" End CCount");
                StrSql.Append(" from BOOKING_CONTAINERS_TRN BCT,");
                StrSql.Append(" CONTAINER_TYPE_MST_TBL CTM,");
                StrSql.Append(" (SELECT DISTINCT CST.COMMERCIAL_SCHEDULE_TRN_PK,");
                StrSql.Append(" VMT.VESSEL_NAME,");
                StrSql.Append(" CSH.VOYAGE_NO,");
                StrSql.Append(" TO_CHAR(CST.ETD, 'dd/MM/yyyy') ETD,");
                StrSql.Append(" to_char(cst.eta, 'dd/MM/yyyy') ETA");
                StrSql.Append(" FROM VESSEL_MST_TBL          VMT,");
                StrSql.Append(" COMMERCIAL_SCHEDULE_HDR CSH,");
                StrSql.Append(" COMMERCIAL_SCHEDULE_TRN CST");
                StrSql.Append(" where CSH.VESSEL_MST_FK = VMT.VESSEL_MST_PK");
                StrSql.Append(" AND CST.COMMERCIAL_SCHEDULE_HDR_FK =");
                StrSql.Append(" CSH.COMMERCIAL_SCHEDULE_HDR_PK and CST.COMMERCIAL_SCHEDULE_TRN_PK =" + VESSEL + ")VMT");
                StrSql.Append(" where BCT.GATE_IN_STATUS = 1");
                StrSql.Append(" AND BCT.DEPARTURE_STATUS = 1");
                StrSql.Append(" AND BCT.ARRIVAL_STATUS = 1");
                StrSql.Append(" and bct.container_type_mst_fk = ctm.container_type_mst_pk");
                StrSql.Append(" AND VMT.COMMERCIAL_SCHEDULE_TRN_PK = BCT.ARRIVAL_VOYAGE_FK");
                StrSql.Append(" group by CTM.CONTAINER_TYPE_MST_ID");
                return Objwk.GetDataSet(StrSql.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Terminal base on the POL"
        public DataSet FetchTerminal(long POLPK = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = strSQL + " (select ";
            strSQL = strSQL + " '' TERMINAL_ID,";
            strSQL = strSQL + " -1 TERMINAL_MST_PK,";
            strSQL = strSQL + " '' TERMINAL_NAME";

            strSQL = strSQL + " from dual";
            strSQL = strSQL + " union ";
            strSQL = strSQL + " select DISTINCT tmt.terminal_id,tmt.terminal_mst_pk,tmt.terminal_name ";
            strSQL = strSQL + " from Terminal_Mst_Tbl tmt ,Port_Mst_Tbl pmt";
            strSQL = strSQL + " where tmt.port_mst_fk=pmt.Port_Mst_Pk";
            strSQL = strSQL + " And pmt.PORT_MST_PK =" + POLPK;
            strSQL = strSQL + " union ";

            strSQL = strSQL + " select ";
            strSQL = strSQL + " '<ALL>' TERMINAL_ID,";
            strSQL = strSQL + " 0 TERMINAL_MST_PK,";
            strSQL = strSQL + " '' TERMINAL_NAME";
            strSQL = strSQL + " from dual) ORDER BY TERMINAL_MST_PK ";

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
        #endregion

        #region "FetchUpdateTerminal"
        public DataSet FetchUpdateTerminal(long POLPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = strSQL + " (select ";
            strSQL = strSQL + " '' TERMINAL_ID,";
            strSQL = strSQL + " 0 TERMINAL_MST_PK,";
            strSQL = strSQL + " '' TERMINAL_NAME";

            strSQL = strSQL + " from dual";
            strSQL = strSQL + " union ";
            strSQL = strSQL + " select DISTINCT tmt.terminal_id,tmt.terminal_mst_pk,tmt.terminal_name ";
            strSQL = strSQL + " from Terminal_Mst_Tbl tmt ,Port_Mst_Tbl pmt";
            strSQL = strSQL + " where tmt.port_mst_fk=pmt.Port_Mst_Pk";
            strSQL = strSQL + " And pmt.PORT_MST_PK =" + POLPK ;
            strSQL = strSQL + " )Order By Terminal_Mst_Pk";
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
        #endregion

        #region "FetchUpdateTerminal"
        public DataSet FetchATADate(long comhdrpk = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = strSQL + " Select dmt.ata_dt || ' ' || dmt.ata_time ata_dt";
            strSQL = strSQL + " from commercial_schedule_trn cst,";
            strSQL = strSQL + " dep_message_trn dmt,";
            strSQL = strSQL + " commercial_schedule_hdr csh";
            strSQL = strSQL + " where dmt.commercial_schedule_trn_fk = cst.commercial_schedule_trn_pk";
            strSQL = strSQL + " and cst.commercial_schedule_hdr_fk=csh.commercial_schedule_hdr_pk";
            strSQL = strSQL + " and cst.commercial_schedule_trn_pk =" + comhdrpk;
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
        #endregion

        #region "FetchATDDate"
        public DataSet FetchATDDate(long comtrnpk = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = strSQL + " Select dmt.atd_dt || ' ' || dmt.atd_time atd_dt";
            strSQL = strSQL + " from commercial_schedule_hdr csh,";
            strSQL = strSQL + " commercial_schedule_trn cst,";
            strSQL = strSQL + " dep_message_trn dmt";
            strSQL = strSQL + " where dmt.commercial_schedule_trn_fk = cst.commercial_schedule_trn_pk";
            strSQL = strSQL + " and cst.commercial_schedule_hdr_fk=csh.commercial_schedule_hdr_pk";
            strSQL = strSQL + " and cst.commercial_schedule_trn_pk=" + comtrnpk;
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
        #endregion

        #region "FetchArrivalConfirmNew"
        public DataSet FetchArrivalConfirmNew(long Commercial_Sch_trn_Pk, Int16 clientval = 0, string sType = "AP", Int16 iBusiness = 0, bool iTranshipment = true, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 Location = 0, Int64 PortPk = 0, string strContainerno = "",
        string strBillNo = "", Int32 TPK = 0, Int32 comhdr = 0)
        {

            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strTmpTSDecode = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            WorkFlow objWF = new WorkFlow();
            strTmpTSDecode = "decode((Select Count(*) from Booking_Routing_Trn y where y.pod_mst_fk = " + PortPk + " and y.booking_trn_fk = br.booking_trn_fk),0,1,2)";
            strSQL = " SELECT COUNT(*)" ;
            strSQL += " FROM(" ;
            strSQL += " SELECT " ;
            strSQL += " SUBQRY.* FROM(" ;
            strSQL += " SELECT DISTINCT " ;
            strSQL += " nvl((select TMT.TERMINAL_ID FROM   TERMINAL_MST_TBL TMT WHERE TMT.TERMINAL_MST_PK=BCT.DESTINATION_TERMINAL_MST_FK),NULL) TERMINAL_ID,";
            strSQL += "        BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += "       BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += "       BCT.CONTAINER_NO, " ;
            strSQL += "       CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += "       BCT.CONTAINER_STATUS," ;
            strSQL += " '' VESSEL, " ;
            strSQL += " '' VOYAGE," ;
            strSQL += "  BCT.DESTINATION_TERMINAL_MST_FK  TERMINAL_MST_PK," ;
            strSQL += strTmpTSDecode + " ROUTING_ID, ";
            strSQL += "       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,'Local','T/S') SHIP_TYPE," ;
            strSQL += "       BL.CONSIGNEE_MST_FK, " ;
            strSQL += "       PMT.PORT_ID POL," ;
            strSQL += "       CMT.CUSTOMER_NAME," ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,BCT.STOWAGE_POSITION,BCT.TS_STOWAGE_POSITION) STOWAGE_POSITION," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += "  DECODE(BCT.ARRIVAL_STATUS,1,'Discharged',0,'Not Discharged') DISCHARGE_STATUS," ;
            strSQL += "       TO_CHAR(DECODE(" + strTmpTSDecode + ",1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS)) ARRIVAL_STATUS, " ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS) PREV_ARRIVAL_STATUS, " ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,TO_CHAR(BCT.ARRIVAL_DT,'" + M_DateTimeFormat + "'),TO_CHAR(BCT.TS_ARRIVAL_DT,'HH24:MI')) ARRIVAL_DT," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_PK," ;
            strSQL += "       CIT.LAST_MOVE_CODE_FK," ;
            strSQL += "   cmt.location_mst_fk," ;
            strSQL += "       BCT.VERSION_NO, " ;
            strSQL += "       TO_CHAR(0) SEL";
            strSQL += " FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT," ;
            strSQL += "       BOOKING_BL_TRN BL, " ;
            strSQL += "      Succeeding_Moves_Mst_Tbl  SCC,  " ;
            strSQL += "       Movecode_Mst_Tbl          MOV  " ;
            strSQL += " WHERE BR.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND BL.Consignee_Mst_Fk = CMT.CUSTOMER_MST_PK" ;
            strSQL += "       AND BR.POl_MST_FK = PMT.PORT_MST_PK" ;
            strSQL += "       AND CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK = CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += "       AND BL.BOOKING_TRN_FK(+) = BT.BOOKING_TRN_PK " ;
            strSQL += "       and SCC.MOVECODE_FK = CIT.LAST_MOVE_CODE_FK  " ;
            strSQL += "       AND SCC.NEXT_MOVECODE_FK = MOV.MOVECODE_PK " ;
            strSQL += "        AND MOV.MOVECODE_ID IN ('IFD', 'EMD', 'TSF', 'TSM') " ;
            strSQL += "       AND NVL(BL.BL_STATUS,0) <> 5 " ;
            //to avoid cancelled BLs.
            if (iBusiness > 0)
            {
                strSQL += "  AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            else if (iBusiness == 0)
            {
                strSQL += "  AND BT.BUSINESS_MODEL in (1,4)" ;
            }

            strSQL += "       AND BCT.GATE_IN_STATUS=1 " ;
            strSQL += "       AND BCT.DEPARTURE_STATUS=1 " ;
            if (sType == "A")
            {
                strSQL += "       AND    BCT.ARRIVAL_STATUS =1 " ;
            }
            else if (sType == "P")
            {
                strSQL += "       AND   BCT.ARRIVAL_STATUS =0 " ;
            }
            else if (sType == "AP")
            {
                strSQL += "       AND    BCT.ARRIVAL_STATUS IN(0,1) " ;
            }
            strSQL += "  and bct.arrival_voyage_fk =" + Commercial_Sch_trn_Pk.ToString();
            if (strContainerno.Trim().Length > 0)
            {
                strSQL += "       AND BCT.CONTAINER_NO LIKE '%" + strContainerno.Trim().Replace("'", "''").ToUpper() + "%'";
            }
            if (strBillNo.Trim().Length > 0)
            {
                strSQL += "       AND      BL.SERVICE_BL_NO LIKE '%" + strBillNo.Trim().Replace("'", "''").ToUpper() + "%'";
            }
            if (TPK > 0)
            {
                strSQL += " and BCT.DESTINATION_TERMINAL_MST_FK =" + TPK ;
            }
            strSQL += "  )SUBQRY)";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            strTmpTSDecode = "decode((Select Count(*) from Booking_Routing_Trn y where y.pod_mst_fk = " + PortPk + " and y.booking_trn_fk = br.booking_trn_fk),0,1,2)";
            strSQL = " SELECT * FROM(" ;
            strSQL += " SELECT ROWNUM SL_NO," ;
            strSQL += " SUBQRY.* FROM(" ;
            strSQL += " SELECT DISTINCT " ;
            strSQL += " nvl((select TMT.TERMINAL_ID FROM   TERMINAL_MST_TBL TMT WHERE TMT.TERMINAL_MST_PK=BCT.DESTINATION_TERMINAL_MST_FK),NULL) TERMINAL_ID,";
            strSQL += "        BCT.BOOKING_CONTAINERS_PK, " ;
            strSQL += "       BCT.CONTAINER_INVENTORY_FK," ;
            strSQL += "       BCT.CONTAINER_NO, " ;
            strSQL += "       CTM.CONTAINER_TYPE_MST_ID," ;
            strSQL += "       BCT.CONTAINER_STATUS," ;
            strSQL += " '' VESSEL, " ;
            strSQL += " '' VOYAGE," ;
            strSQL += "  BCT.DESTINATION_TERMINAL_MST_FK  TERMINAL_MST_PK," ;
            strSQL += strTmpTSDecode + " ROUTING_ID, ";
            strSQL += "       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS," ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,'Local','T/S') SHIP_TYPE," ;
            strSQL += "       BL.CONSIGNEE_MST_FK, " ;
            strSQL += "       PMT.PORT_ID POL," ;
            strSQL += "       CMT.CUSTOMER_NAME," ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,BCT.STOWAGE_POSITION,BCT.TS_STOWAGE_POSITION) STOWAGE_POSITION," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_ID BIZ_MODEL," ;
            strSQL += "  DECODE(BCT.ARRIVAL_STATUS,1,'Discharged',0,'Not Discharged') DISCHARGE_STATUS," ;
            strSQL += "       TO_CHAR(DECODE(" + strTmpTSDecode + ",1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS)) ARRIVAL_STATUS, " ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,BCT.ARRIVAL_STATUS,BCT.TS_ARRIVAL_STATUS) PREV_ARRIVAL_STATUS, " ;
            strSQL += "       DECODE(" + strTmpTSDecode + ",1,TO_CHAR(BCT.ARRIVAL_DT,'" + M_DateTimeFormat + "'),TO_CHAR(BCT.TS_ARRIVAL_DT,'HH24:MI')) ARRIVAL_DT," ;
            strSQL += "       CTMT.CUSTOMER_TYPE_PK," ;

            strSQL += "       CIT.LAST_MOVE_CODE_FK," ;
            strSQL += "      cmt.location_mst_fk,  " ;
            strSQL += "       BCT.VERSION_NO, " ;
            strSQL += "       TO_CHAR(0) SEL";
            strSQL += " FROM  BOOKING_TRN BT," ;
            strSQL += "       BOOKING_ROUTING_TRN BR," ;
            strSQL += "       BOOKING_CTYPE_TRN BC," ;
            strSQL += "       BOOKING_CONTAINERS_TRN BCT," ;
            strSQL += "       CONTAINER_TYPE_MST_TBL CTM," ;
            strSQL += "       CUSTOMER_MST_TBL CMT," ;
            strSQL += "       PORT_MST_TBL PMT," ;
            strSQL += "       CUSTOMER_TYPE_MST_TBL CTMT," ;
            strSQL += "       CONTAINER_INVENTORY_TRN CIT," ;
            strSQL += "       BOOKING_BL_TRN BL, " ;
            strSQL += "      Succeeding_Moves_Mst_Tbl  SCC,  " ;
            strSQL += "       Movecode_Mst_Tbl          MOV  " ;
            strSQL += " WHERE BR.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            if (clientval == 1)
            {
                strSQL += "   AND CMT.TEMP_CUSTOMER=1 " ;
            }
            strSQL += "       AND BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.BOOKING_TRN_FK = BT.BOOKING_TRN_PK" ;
            strSQL += "       AND BCT.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK" ;
            strSQL += "       AND Bl.Consignee_Mst_Fk = CMT.CUSTOMER_MST_PK" ;
            strSQL += "        AND BR.POl_MST_FK = PMT.PORT_MST_PK" ;
            strSQL += "       AND CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK" ;
            strSQL += "       AND BCT.CONTAINER_INVENTORY_FK = CIT.CONTAINER_INVENTORY_PK(+)" ;
            strSQL += "       AND BL.BOOKING_TRN_FK(+) = BT.BOOKING_TRN_PK " ;

            strSQL += "       and SCC.MOVECODE_FK = CIT.LAST_MOVE_CODE_FK  " ;
            strSQL += "       AND SCC.NEXT_MOVECODE_FK = MOV.MOVECODE_PK " ;
            strSQL += "        AND MOV.MOVECODE_ID IN ('IFD', 'EMD', 'TSF', 'TSM') " ;
            strSQL += "        AND NVL(BL.BL_STATUS,0) <> 5 " ;
            if (iBusiness > 0)
            {
                strSQL += "  AND BT.BUSINESS_MODEL= " + iBusiness ;
            }
            else if (iBusiness == 0)
            {
                strSQL += "  AND BT.BUSINESS_MODEL in (1,4)" ;
            }

            strSQL += "       AND BCT.GATE_IN_STATUS=1 " ;
            strSQL += "       AND BCT.DEPARTURE_STATUS=1 " ;
            if (sType == "A")
            {
                strSQL += "       AND    BCT.ARRIVAL_STATUS =1 " ;
            }
            else if (sType == "P")
            {
                strSQL += "       AND   BCT.ARRIVAL_STATUS =0 " ;
            }
            else if (sType == "AP")
            {
                strSQL += "       AND    BCT.ARRIVAL_STATUS IN(0,1) " ;
            }
            strSQL += "  and bct.arrival_voyage_fk =" + Commercial_Sch_trn_Pk.ToString();
            if (strContainerno.Trim().Length > 0)
            {
                strSQL += "       AND BCT.CONTAINER_NO LIKE '%" + strContainerno.Trim().Replace("'", "''").ToUpper() + "%'";
            }
            if (strBillNo.Trim().Length > 0)
            {
                strSQL += "       AND      BL.SERVICE_BL_NO LIKE '%" + strBillNo.Trim().Replace("'", "''").ToUpper() + "%'";
            }
            if (TPK > 0)
            {
                strSQL += " and BCT.DESTINATION_TERMINAL_MST_FK =" + TPK ;
            }
            strSQL += "  ORDER BY TERMINAL_Id,POL,CTM.CONTAINER_TYPE_MST_ID,CONTAINER_NO,ARRIVAL_DT)SUBQRY)WHERE SL_NO  Between " + start + " and " + last;
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
        #endregion

        #region "SaveArrivalDataNew"
        public ArrayList SaveArrivalDataNew(DataSet Discharge_DataSet, int DischargeStatus, string TimeDiff)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();
            Int32 inti = default(Int32);
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            Int16 SHIP_TYPE = 1;

            int I = 0;
            updCommand.Connection = objWK.MyConnection;
            try
            {
                for (I = 0; I <= Discharge_DataSet.Tables[0].Rows.Count - 1; I++)
                {
                    updCommand.Parameters.Clear();

                    updCommand.CommandType = CommandType.StoredProcedure;
                    updCommand.CommandText = objWK.MyUserName + ".CONTAINER_MOVEMENT_PKG.UPDATE_DISCHARGE_CONFIRMATOIN";
                    updCommand.Transaction = TRAN;
                    updCommand.Parameters.Add("BOOKING_CONTAINERS_PK_IN", Discharge_DataSet.Tables[0].Rows[I]["BOOKING_CONTAINERS_PK"]).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("CONTAINER_INVENTORY_TRN_FK_IN", Discharge_DataSet.Tables[0].Rows[I]["CONTAINER_INVENTORY_FK"]).Direction = ParameterDirection.Input;
                    if (!string.IsNullOrEmpty(Discharge_DataSet.Tables[0].Rows[I]["Is_Full_Container_Present"].ToString()))
                    {
                        if (Convert.ToInt32(Discharge_DataSet.Tables[0].Rows[I]["Is_Full_Container_Present"]) == 1)
                        {
                            updCommand.Parameters.Add("CONTAINER_MOVE_HDR_FK_IN", Discharge_DataSet.Tables[0].Rows[I]["full_Move_Hdr_pk"]).Direction = ParameterDirection.Input;
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(Discharge_DataSet.Tables[0].Rows[I]["Is_Empty_Container_Present"].ToString()))
                        {
                            if (Convert.ToInt32(Discharge_DataSet.Tables[0].Rows[I]["Is_Empty_Container_Present"]) == 1)
                            {
                                updCommand.Parameters.Add("CONTAINER_MOVE_HDR_FK_IN", Discharge_DataSet.Tables[0].Rows[I]["Empty_Move_Hdr_pk"]).Direction = ParameterDirection.Input;
                            }
                        }
                    }
                    updCommand.Parameters.Add("ROUTING_ID_IN", Discharge_DataSet.Tables[0].Rows[I]["ROUTING_ID"]).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("CONTAINER_STATUS_IN", Discharge_DataSet.Tables[0].Rows[I]["CONTAINER_STATUS"]).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("BUSINESS_MODEL_IN", Discharge_DataSet.Tables[0].Rows[I]["CUSTOMER_TYPE_PK"]).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("PREV_DISCH_STATUS_IN", Discharge_DataSet.Tables[0].Rows[I]["PREV_ARRIVAL_STATUS"]).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("DISCH_STATUS_IN", DischargeStatus).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("DISCH_DATE_IN", Convert.ToDateTime(Discharge_DataSet.Tables[0].Rows[I]["ARRIVAL_DT"])).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("LOCATION_MST_FK_IN", Discharge_DataSet.Tables[0].Rows[I]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("CONSIGNEE_MST_FK_IN", Discharge_DataSet.Tables[0].Rows[I]["CONSIGNEE_MST_FK"]).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("VERSION_NO_IN", Discharge_DataSet.Tables[0].Rows[I]["VERSION_NO"]).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("TERMINAL_FK_IN", Discharge_DataSet.Tables[0].Rows[I]["TERMINAL_MST_PK"]).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("TIME_DIFF_IN", TimeDiff).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    updCommand.ExecuteNonQuery();
                }
                TRAN.Commit();
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();

            }
        }
        #endregion

        #region "GetBookingPk"
        public long GetBookingPk(long cropk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " Select bktrn.booking_trn_pk from Booking_Trn BKTRN,Cro_Trn CROTRN Where " ;
            strSQL += " CROTRN.BOOKING_TRN_FK = BKTRN.BOOKING_TRN_PK And crotrn.cro_trn_pk = " + cropk;
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
        public DataSet GetDetail(long bookpk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " select distinct comm.port_mst_fk,v.vessel_id,v.vessel_name,po.port_name,comh.voyage_no,comm.commercial_schedule_trn_pk" ;
            strSQL += "from booking_containers_trn b,commercial_schedule_trn comm," ;
            strSQL += "commercial_schedule_hdr comh,vessel_mst_tbl v,port_mst_tbl po " ;
            strSQL += "where b.departure_voyage_fk = comm.commercial_schedule_trn_pk " ;
            strSQL += "and comh.vessel_mst_fk=v.vessel_mst_pk " ;
            strSQL += "and comm.commercial_schedule_hdr_fk=comh.commercial_schedule_hdr_pk " ;
            strSQL += "and po.port_mst_pk=comm.port_mst_fk " ;
            strSQL += "and b.booking_trn_fk=" + bookpk;

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
        #endregion

        #region "Update Stowage Position For Load Confirmation"
        public bool UpdateStow_Pos(int Booking_containers_pk, string Stowage_pos, System.DateTime Dept_Date, int IVersionNo)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            long version_no = 0;
            version_no = IVersionNo + 1;
            strSQL = " UPDATE BOOKING_CONTAINERS_TRN BCT  set ";
            if (!string.IsNullOrEmpty(Stowage_pos))
            {
                strSQL +=  " BCT.STOWAGE_POSITION = '" + Stowage_pos + "', ";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(Dept_Date)))
            {
                strSQL +=  " BCT.Departure_Date =  to_date('" + Dept_Date + "','dd/MM/yyyy hh24:mi:ss'), ";
            }
            strSQL +=  " BCT.VERSION_NO = " + version_no;
            strSQL +=  "WHERE BCT.BOOKING_CONTAINERS_PK = " + Booking_containers_pk;
            strSQL +=  "AND BCT.VERSION_NO =  " + IVersionNo;
            try
            {
                return objWK.ExecuteCommands(strSQL);
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
        #endregion

        #region " Update Load Date For Gate_In"
        public ArrayList Update_GateIN(int Booking_containers_pk, DateTime load_dt, int IVersionNo)
        {

            string strRet_Value = null;
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            var _with31 = updCommand;
            _with31.Connection = objWK.MyConnection;
            _with31.CommandType = CommandType.StoredProcedure;
            _with31.CommandText = objWK.MyUserName + ".CONTAINER_MOVEMENT_PKG.Update_GateIN";

            _with31.Parameters.Add("BOOKING_CONTAINERS_PK_IN", Booking_containers_pk).Direction = ParameterDirection.Input;
            _with31.Parameters.Add("Load_Date_IN", load_dt).Direction = ParameterDirection.Input;
            _with31.Parameters.Add("Version_No_IN", IVersionNo).Direction = ParameterDirection.Input;
            _with31.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
            _with31.Parameters["return_value"].SourceVersion = DataRowVersion.Current;
            var _with32 = objWK.MyDataAdapter;
            _with32.UpdateCommand = updCommand;
            _with32.UpdateCommand.Transaction = TRAN;
            RecAfct = _with32.UpdateCommand.ExecuteNonQuery();
            if (RecAfct > 0)
            {
                TRAN.Commit();
                arrMessage.Add("Saved");
            }

            try
            {
            }
            catch (OracleException sqlexp)
            {
                if (sqlexp.ErrorCode == 20999)
                {
                    arrMessage.Add("20999");
                }
                else
                {
                    arrMessage.Add(sqlexp.Message);
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            if (arrMessage.Count > 0)
            {
                return arrMessage;
            }
            else
            {
                arrMessage.Add("Saved");
                return arrMessage;
            }
        }
        #endregion

        #region "Update Load Date for Gate-In"
        public bool Upd_Gate_In(int Booking_containers_pk, DateTime Load_Dt, int IVersionNo)
        {
            string strSql = null;
            string strSql1 = null;
            WorkFlow objwk = new WorkFlow();
            long ver_no = 0;
            ver_no = IVersionNo + 1;
            strSql1 = "NLS_DATE_LANGUAGE=AMERICAN";
            strSql = " UPDATE BOOKING_CONTAINERS_TRN BCT  set BCT.Departure_Date = to_date('" + Load_Dt + "','" + strSql1 + "'),";
            strSql +=  " BCT.VERSION_NO = " + ver_no;
            strSql +=  "WHERE BCT.BOOKING_CONTAINERS_PK = " + Booking_containers_pk;
            strSql +=  "AND BCT.VERSION_NO =  " + IVersionNo;

            try
            {
                return objwk.ExecuteCommands(strSql);
            }
            catch (OracleException sqlexp)
            {
                ErrorMessage = sqlexp.Message;
                throw sqlexp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }
        public bool Upd_Gate_In_Stow_Pos(int Booking_containers_pk, string stowage_pos, System.DateTime load_dt, int IVersionNo)
        {
            string strSql = null;
            WorkFlow objwk = new WorkFlow();
            long ver_no = 0;
            ver_no = IVersionNo + 1;
            string loadDt = null;
            strSql = " UPDATE BOOKING_CONTAINERS_TRN BCT  set BCT.Departure_Date = to_date('" + loadDt + "','dd/MM/yyyy hh24:mi'),";
            strSql +=  " BCT.STOWAGE_POSITION = '" + stowage_pos + "', BCT.VERSION_NO = " + ver_no;
            strSql +=  "WHERE BCT.BOOKING_CONTAINERS_PK = " + Booking_containers_pk;
            strSql +=  "AND BCT.VERSION_NO =  " + IVersionNo;

            try
            {
                return objwk.ExecuteCommands(strSql);
            }
            catch (OracleException sqlexp)
            {
                ErrorMessage = sqlexp.Message;
                throw sqlexp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }
        #endregion

        #region "UpdateStowPos"
        public bool UpdateStowPos(int Booking_containers_pk, string Stowage_pos, int IVersionNo)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            long version_no = 0;
            version_no = IVersionNo + 1;

            strSQL = " UPDATE BOOKING_CONTAINERS_TRN BCT  set BCT.STOWAGE_POSITION = '" + Stowage_pos + "', ";
            strSQL +=  " BCT.VERSION_NO = " + version_no;
            strSQL +=  "WHERE BCT.BOOKING_CONTAINERS_PK = " + Booking_containers_pk;
            strSQL +=  "AND BCT.VERSION_NO =  " + IVersionNo;

            try
            {
                return objWK.ExecuteCommands(strSQL);
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
        #endregion

        #region "Get Server Date"
        public string GetServerDtTime()
        {
            string StrSql = null;
            string strTim = null;
            DataRow Dr = null;
            StrSql = "SELECT to_char(SYSDATE,'mm/dd/yyyy hh24:mi:ss') CurDate FROM dual";
            WorkFlow ObjWs = new WorkFlow();
            ObjWs.OpenConnection();
            ObjWs.MyCommand.CommandType = CommandType.Text;
            ObjWs.MyCommand.CommandText = StrSql;
            try
            {
                return Convert.ToString(ObjWs.MyCommand.ExecuteScalar());
            }
            catch (OracleException sqlexp)
            {
                ErrorMessage = sqlexp.Message;
                throw sqlexp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        public string GetServerTime()
        {
            string StrSql = null;
            string strTim = null;
            DataRow Dr = null;
            StrSql = "SELECT to_char(SYSDATE,'hh24:mi:ss') CurDate FROM dual";
            WorkFlow ObjWs = new WorkFlow();
            ObjWs.OpenConnection();
            ObjWs.MyCommand.CommandType = CommandType.Text;
            ObjWs.MyCommand.CommandText = StrSql;
            try
            {
                return Convert.ToString(ObjWs.MyCommand.ExecuteScalar());
            }
            catch (OracleException sqlexp)
            {
                ErrorMessage = sqlexp.Message;
                throw sqlexp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }
        #endregion

        #region "Check For New Pickup Entry"
        public DataSet FetchGateInUSS(long voyagePk, string pol, string ServerDate, string inventoryPks, string ssvrdt)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("");
            sb.Append(" SELECT ROWNUM SL_NO, QRY.* FROM ( SELECT ");
            sb.Append("       BCT.BOOKING_CONTAINERS_PK, ");
            sb.Append("       BCT.CONTAINER_INVENTORY_FK,");
            sb.Append("       BCT.CONTAINER_NO, ");
            sb.Append("       CTM.CONTAINER_TYPE_MST_ID,");
            sb.Append("'");
            sb.Append(pol);
            sb.Append("'");
            sb.Append("        POL,");
            sb.Append("       PMT.PORT_ID POD,");
            sb.Append("       BCT.CONTAINER_STATUS,");
            sb.Append("       DECODE(BCT.CONTAINER_STATUS, 1, 'EMPTY',2,'FULL') STATUS,");
            sb.Append("       CTMT.CUSTOMER_TYPE_PK,");
            sb.Append("'");
            sb.Append(ssvrdt);
            sb.Append("' GATE_IN_DATE,");
            sb.Append("        TO_CHAR(BCT.GATE_IN_STATUS) GATE_IN_STATUS,");
            sb.Append("         TO_CHAR(BCT.GATE_IN_STATUS) PREV_GATE_IN_STATUS,");
            sb.Append("         TO_CHAR(BCT.DSO_DATE,'" + M_DateTimeFormat + "') DSO_DATE,");
            sb.Append("         BCT.DEPARTURE_DATE,");
            sb.Append("         BCT.ARRIVAL_DT ARRIVAL_DATE,");
            sb.Append("         BCT.VERSION_NO ");
            sb.Append(" FROM  BOOKING_TRN BT,");
            sb.Append("       BOOKING_ROUTING_TRN BR,");
            sb.Append("       BOOKING_CTYPE_TRN BC,");
            sb.Append("       BOOKING_CONTAINERS_TRN BCT,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTM,");
            sb.Append("       CUSTOMER_MST_TBL CMT,");
            sb.Append("       PORT_MST_TBL PMT,");
            sb.Append("       CUSTOMER_TYPE_MST_TBL CTMT,");
            sb.Append("       CONTAINER_INVENTORY_TRN CIT");
            sb.Append(" WHERE BR.BOOKING_TRN_FK=BT.BOOKING_TRN_PK");
            sb.Append("       AND BC.BOOKING_TRN_FK=BT.BOOKING_TRN_PK");
            sb.Append("       AND BCT.BOOKING_CTYPE_FK=BC.BOOKING_CTYPE_PK");
            sb.Append("       AND BCT.CONTAINER_TYPE_MST_FK=CTM.CONTAINER_TYPE_MST_PK");
            sb.Append("       AND BT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
            sb.Append("       AND BR.POD_MST_FK=PMT.PORT_MST_PK");
            sb.Append("       AND BT.BUSINESS_MODEL=CTMT.CUSTOMER_TYPE_PK");
            sb.Append("       AND BCT.CONTAINER_INVENTORY_FK=CIT.CONTAINER_INVENTORY_PK(+)");
            sb.Append("       AND (BCT.GATE_IN_STATUS=0 AND BCT.DSO_STATUS=1 OR BCT.DSO_STATUS=0)");
            sb.Append("       AND BT.BUSINESS_MODEL= 4");
            sb.Append("       AND BR.COMMERCIAL_SCHEDULE_TRN_FK = ");
            sb.Append(voyagePk);
            sb.Append("       AND BCT.CONTAINER_INVENTORY_FK IN (" + inventoryPks + ") ");
            sb.Append(" ORDER BY BCT.GATE_IN_DATE)  QRY ");

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #endregion

        #region "Check For VersionNo"
        public DataSet VersionNo(int Booking_containers_pk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();

            sb.Append("");
            sb.Append(" SELECT BCT.VERSION_NO ");
            sb.Append("  FROM BOOKING_CONTAINERS_TRN BCT ");
            sb.Append(" WHERE BCT.BOOKING_CONTAINERS_PK = " + Booking_containers_pk);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #endregion
        #region " Fetch Container data export"
        public DataSet FetchDetailsLoad(string VoyagePk = "0", string customerpk = "0", string Polpk = "0", string podpk = "0", string bookingpk = "0", string mblpk = "0", string comoditypk = "", string status = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with33 = objWF.MyDataAdapter;
                _with33.SelectCommand = new OracleCommand();
                _with33.SelectCommand.Connection = objWF.MyConnection;
                _with33.SelectCommand.CommandText = objWF.MyUserName + ".LOAD_LIST_PKG.FETCH_LOAD_LIST";
                _with33.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with33.SelectCommand.Parameters.Add("VOYAGE_TRN_PK_IN", VoyagePk).Direction = ParameterDirection.Input;
                _with33.SelectCommand.Parameters.Add("CUSTOMER_PK_IN", customerpk).Direction = ParameterDirection.Input;
                _with33.SelectCommand.Parameters.Add("POL_PK_IN", Polpk).Direction = ParameterDirection.Input;
                _with33.SelectCommand.Parameters.Add("POD_PK_IN", podpk).Direction = ParameterDirection.Input;
                _with33.SelectCommand.Parameters.Add("BOOKING_PK_IN", bookingpk).Direction = ParameterDirection.Input;
                _with33.SelectCommand.Parameters.Add("MBL_PK_IN", mblpk).Direction = ParameterDirection.Input;
                _with33.SelectCommand.Parameters.Add("COMMODITY_GROUP_PK_IN", comoditypk).Direction = ParameterDirection.Input;
                _with33.SelectCommand.Parameters.Add("STATUS_IN", status).Direction = ParameterDirection.Input;

                _with33.SelectCommand.Parameters.Add("LOAD_LIST_CUT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with33.Fill(ds);


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

        #endregion
        #region "Fetch POD"
        public object fetchpod(string VoyagePk = "0")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with34 = objWF.MyDataAdapter;
                _with34.SelectCommand = new OracleCommand();
                _with34.SelectCommand.Connection = objWF.MyConnection;
                _with34.SelectCommand.CommandText = objWF.MyUserName + ".LOAD_LIST_PKG.FETCH_POD";
                _with34.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with34.SelectCommand.Parameters.Add("VOYAGE_TRN_PK_IN", VoyagePk).Direction = ParameterDirection.Input;
                _with34.SelectCommand.Parameters.Add("LOAD_LIST_CUT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with34.Fill(ds);


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
        #endregion
    }
}