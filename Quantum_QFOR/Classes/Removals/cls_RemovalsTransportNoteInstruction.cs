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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class clsRemovalsTransportNoteInstruction : CommonFeatures
    {
        #region " Enhance Search & Lookup Search Block FOR HBL"

        public string Fetch_Removal_JobCard_RefNr(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strReq = null;
            string strLOCATION_IN = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOCATION_IN = Convert.ToString(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_HBL_REF_NO_PKG.GET_REM_JOBCARD_REF_NO";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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
                SCM.Connection.Close();
            }
        }

        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        #endregion " Enhance Search & Lookup Search Block FOR HBL"

        #region "Fetch TransportNote detail"

        // Fetch from Removal Job card Master Table to generate New Transport Note Instruction.
        public void Fetch_RemTranspNote(long RemJobPk, DataSet objDS)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            try
            {
                strSql = " SELECT RJOB.JOB_CARD_PK,";
                strSql += " RTRNDTL.JC_TRANSP_DTLS_PK,";
                strSql += " RJOB.JOB_CARD_PARTY_FK,";
                strSql += " RJOB.JOB_CARD_SHIPPER_FK AS SHIPPK,";
                strSql += " RJOB.JOB_CARD_CONSINEE_FK AS CONPK,";
                strSql += " RTRNDTL.CARRIER_ID AS TRANSPORTER_NAME,";
                strSql += " RTRNDTL.CARRIER_NR AS TRANSPORTER_NUMBER,";
                strSql += " RTRNDTL.TRANS_MODE,";
                strSql += " 'Land' AS TRANSPORTMODE,";
                strSql += " CSHIP.CUSTOMER_ID AS SHIPPER,";
                strSql += " SHIPADDR.ADM_ADDRESS_1 || ' ' || SHIPADDR.ADM_ADDRESS_2 || ' ' ||";
                strSql += " SHIPADDR.ADM_ADDRESS_3 || ' ' || SHIPADDR.ADM_CITY || ' ' ||";
                strSql += " SHIPADDR.ADM_ZIP_CODE || ' ' || SHIPCON.COUNTRY_ID AS SHIPADDR,";
                strSql += " CCONS.CUSTOMER_ID AS CONSIGNEE,";
                strSql += " CONADDR.ADM_ADDRESS_1 || ' ' || CONADDR.ADM_ADDRESS_2 || ' ' ||";
                strSql += " CONADDR.ADM_ADDRESS_3 || ' ' || CONADDR.ADM_CITY || ' ' ||";
                strSql += " CONADDR.ADM_ZIP_CODE || ' ' || CONSCON.COUNTRY_ID AS CONSADDR,";
                strSql += " CSHIP.COL_ADDRESS,";
                strSql += " CSHIP.DEL_ADDRESS,";
                strSql += " RSPL.JC_GOODS_DESC,";
                strSql += " RTRNDTL.NO_OF_PCS AS QUANTITY,";
                strSql += " RTRNDTL.MOVE_DATE AS PICKUPDATE,";
                strSql += " RTRNDTL.DEL_DATE AS DELIVERYDATE,";
                strSql += " RTRNDTL.WEIGHT,";
                strSql += " RTRNDTL.VOLUME";
                strSql += " FROM";
                strSql += " REM_M_JOB_CARD_MST_TBL RJOB,";
                strSql += " REM_T_JC_TRANSP_DTLS_TBL RTRNDTL,";
                strSql += " CUSTOMER_MST_TBL CSHIP,";
                strSql += " CUSTOMER_MST_TBL CCONS,";
                strSql += " REM_T_JC_SPL_INST_TBL RSPL,";
                strSql += " CUSTOMER_CONTACT_DTLS SHIPADDR,";
                strSql += " CUSTOMER_CONTACT_DTLS CONADDR,";
                strSql += " COUNTRY_MST_TBL SHIPCON,";
                strSql += " COUNTRY_MST_TBL CONSCON";
                strSql += " WHERE ";
                strSql += " RJOB.JOB_CARD_PK=RTRNDTL.JOB_CARD_FK";
                strSql += " AND RJOB.JOB_CARD_SHIPPER_FK=CSHIP.CUSTOMER_MST_PK";
                strSql += " AND RJOB.JOB_CARD_CONSINEE_FK=CCONS.CUSTOMER_MST_PK";
                ///strSql &= " AND RJOB.JOB_CARD_PK=RSPL.JOB_CARD_FK"
                strSql += " AND RTRNDTL.JC_TRANSP_DTLS_PK=RSPL.JC_TRANSP_DTLS_FK(+)";
                strSql += " AND SHIPADDR.CUSTOMER_MST_FK=RJOB.JOB_CARD_SHIPPER_FK";
                strSql += " AND CONADDR.CUSTOMER_MST_FK=RJOB.JOB_CARD_CONSINEE_FK";
                strSql += " AND SHIPCON.COUNTRY_MST_PK=SHIPADDR.ADM_COUNTRY_MST_FK";
                strSql += " AND CONSCON.COUNTRY_MST_PK=CONADDR.ADM_COUNTRY_MST_FK";
                strSql += "  AND RTRNDTL.JOB_CARD_FK=" + RemJobPk;
                strSql += " AND RTRNDTL.TRANS_MODE=3";
                objDS.Tables.Add(ObjWk.GetDataTable(strSql));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Fetch TransportNote detail"

        #region "Save"

        public ArrayList Save(long Remjobpk, string TransportPk, string TrnspName, System.DateTime PickUpDate, string RefNo, System.DateTime Deldate, string TrnspNr = "", string TrnspMode = "", string TrnspAddr = "", string ShipperName = "",
        string ShipperAddr = "", string ConsName = "", string ConsAddr = "", string PickUpAddr = "", string DeliveryAddr = "", string GoodsDesc = "", string Qty = "", string Weight = "", string Volume = "", long CreatedBy = 0,
        string Notes = "", string DelRefNr = "", string PickUpRefNr = "", long lngLocPk = 0, long nEmpId = 0, long nUserId = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            Int16 exe = default(Int16);
            OracleTransaction TRAN = null;
            OracleCommand insCommand = new OracleCommand();
            string TransportNoteNo = null;
            try
            {
                ObjWk.OpenConnection();
                TRAN = ObjWk.MyConnection.BeginTransaction();
                insCommand.Connection = ObjWk.MyConnection;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.Transaction = TRAN;
                if (Convert.ToInt32(TransportPk) != 0)
                {
                    insCommand.CommandText = ObjWk.MyUserName + ".REM_TRANSPORT_NOTE_INS_TBL_PKG.REM_TRANSPORT_NOTE_INS_TBL_UPD";
                }
                else
                {
                    insCommand.CommandText = ObjWk.MyUserName + ".REM_TRANSPORT_NOTE_INS_TBL_PKG.REM_TRANSPORT_NOTE_INS_TBL_INS";
                    TransportNoteNo = GenerateProtocolKey("REMOVALS TRANSPORT INSTRUCTION NOTE", lngLocPk, nEmpId, DateTime.Today, "", "", "", nUserId);
                    if (TransportNoteNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                    }
                }
                RefNo = TransportNoteNo;
                ObjWk.MyCommand.Parameters.Clear();
                var _with2 = insCommand.Parameters;
                if (Convert.ToInt32(TransportPk) != 0)
                {
                    _with2.Add("REM_TRNSP_INST_PK_IN", Convert.ToInt64(TransportPk));
                    _with2.Add("LAST_MODIFIED_BY_FK_IN", nUserId);
                }
                else
                {
                    _with2.Add("TRNSP_INST_REF_NO_IN", TransportNoteNo);
                    _with2.Add("CREATED_BY_FK_IN", CreatedBy);
                }
                _with2.Add("TRNSP_NAME_IN", TrnspName);
                _with2.Add("TRNSP_NUMBER_IN", (!string.IsNullOrEmpty(TrnspNr) ? TrnspNr : ""));
                _with2.Add("REM_JOB_CARD_FK_IN", Remjobpk);
                _with2.Add("SHIPPER_NAME_IN", ShipperName);
                _with2.Add("SHIPPER_ADDRESS_IN", (!string.IsNullOrEmpty(ShipperAddr) ? ShipperAddr : ""));
                _with2.Add("CONSIGNEE_NAME_IN", ConsName);
                _with2.Add("CONSIGNEE_ADDRESS_IN", (!string.IsNullOrEmpty(ConsAddr) ? ConsAddr : ""));
                _with2.Add("TRNSP_MODE_IN", (!string.IsNullOrEmpty(TrnspMode) ? TrnspMode : ""));
                _with2.Add("TRNSP_ADDR_IN", (!string.IsNullOrEmpty(TrnspAddr) ? TrnspAddr : ""));
                _with2.Add("PICKUP_ADDRESS_IN", PickUpAddr);
                _with2.Add("PICKUP_REF_NR_IN", PickUpRefNr);
                _with2.Add("PICKUP_DATE_IN", (PickUpDate != DateTime.MinValue ? PickUpDate : DateTime.MinValue));
                _with2.Add("DELIVERY_ADDRESS_IN", DeliveryAddr);
                _with2.Add("DEL_REF_NR_IN", DelRefNr);
                _with2.Add("DELIVERY_DATE_IN", (Deldate != DateTime.MinValue ? Deldate : DateTime.MinValue));
                _with2.Add("GOODS_DESCRIPTION_IN", (!string.IsNullOrEmpty(GoodsDesc) ? GoodsDesc : ""));
                _with2.Add("NOTES_IN", (!string.IsNullOrEmpty(Notes) ? Notes : ""));
                _with2.Add("QUANTITY_IN", (!string.IsNullOrEmpty(Qty) ? Qty : ""));
                _with2.Add("WEIGHT_IN", Weight);
                _with2.Add("VOLUME_IN", Volume);
                _with2.Add("CONFIG_MST_FK_IN", ConfigurationPK);
                _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                exe = Convert.ToInt16(insCommand.ExecuteNonQuery());
                if (exe > 0)
                {
                    //adding by thiyagarajan on 13/2/09:Removal TrackNTrace Task
                    if (string.IsNullOrEmpty(getDefault(TransportPk, "").ToString()))
                    {
                        SaveInTrackNTrace(TransportNoteNo, Convert.ToInt32(Remjobpk), "Transport Note Generated", 11, TRAN);
                    }
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
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                ObjWk.CloseConnection();
            }
            return new ArrayList();
        }

        //adding by thiyagarajan on 13/2/09:TrackNTrace Task:VEK Req.
        public void SaveInTrackNTrace(string refno, Int32 refpk, string status, Int32 Doctype, OracleTransaction TRAN)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            bool chk = false;
            Int32 Return_value = default(Int32);
            Int32 i = default(Int32);
            Int32 j = default(Int32);
            string stuts = null;
            OracleCommand insCommand = new OracleCommand();
            string modes = null;
            try
            {
                objWF.OpenConnection();
                objWF.MyConnection = TRAN.Connection;
                insCommand.Connection = objWF.MyConnection;
                insCommand.Transaction = TRAN;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.CommandText = objWF.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
                var _with3 = insCommand.Parameters;
                _with3.Clear();
                _with3.Add("REF_NO_IN", refno).Direction = ParameterDirection.Input;
                _with3.Add("REF_FK_IN", refpk).Direction = ParameterDirection.Input;
                _with3.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with3.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
                _with3.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with3.Add("DOCTYPE_IN", Doctype).Direction = ParameterDirection.Input;
                insCommand.ExecuteNonQuery();
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        //end

        #endregion "Save"

        #region "Check record"

        //To check Transport Note Already Generated for a job card or not.
        public int Check_Record(long Rem_Jc_Pk)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            int TotalRecords = 0;
            try
            {
                strSql = "select Count(*) from rem_transport_note_ins_tbl ";
                strSql += "where REM_JOB_CARD_FK=" + Rem_Jc_Pk;
                TotalRecords = Convert.ToInt32(ObjWk.ExecuteScaler(strSql));
                if (TotalRecords > 0)
                {
                    TotalRecords = 1;
                    return TotalRecords;
                }
                else
                {
                    TotalRecords = 0;
                    return TotalRecords;
                }
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Check record"

        #region "Fetch From Removals Transport Note Instruction table"

        public void FetchFrom_Rem_T_Ins_Tbl(long Rem_Jc_Pk, DataSet objDS)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            try
            {
                strSql = "SELECT RTN.REM_TRNSP_INST_PK,RJC.JOB_CARD_REF,";
                strSql += " RTN.TRNSP_INST_REF_NO,RTN.TRNSP_INST_DATE, ";
                strSql += " RTN.TRNSP_NAME,RTN.TRNSP_NUMBER,";
                strSql += " RTN.SHIPPER_NAME,RTN.SHIPPER_ADDRESS,";
                strSql += " RTN.CONSIGNEE_NAME,RTN.CONSIGNEE_ADDRESS,";
                strSql += " RTN.TRNSP_MODE,RTN.TRNSP_ADDR,";
                strSql += " RTN.PICKUP_ADDRESS,RTN.PICKUP_REF_NR,";
                strSql += " RTN.PICKUP_DATE,RTN.DELIVERY_ADDRESS,";
                strSql += " RTN.DEL_REF_NR,RTN.DELIVERY_DATE,";
                strSql += " RTN.GOODS_DESCRIPTION,RTN.NOTES,";
                strSql += " RTN.QUANTITY,RTN.WEIGHT,";
                strSql += " RTN.VOLUME,RTN.VERSION_NO";
                strSql += " FROM REM_TRANSPORT_NOTE_INS_TBL RTN,REM_M_JOB_CARD_MST_TBL RJC";
                strSql += " WHERE REM_JOB_CARD_FK= " + Rem_Jc_Pk;
                strSql += " AND RJC.JOB_CARD_PK=RTN.REM_JOB_CARD_FK ";
                objDS.Tables.Add(ObjWk.GetDataTable(strSql));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Fetch From Removals Transport Note Instruction table"

        #region "Fetch For Report"

        public DataSet FetchLocation(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public DataSet fetchTransportreport(long Rem_Jc_Pk)
        {
            try
            {
                string strSql = null;
                strSql = strSql + "SELECT DISTINCT ";
                strSql = strSql + "RTN.REM_TRNSP_INST_PK,";
                strSql = strSql + "RJC.JOB_CARD_REF,";
                strSql = strSql + "RTN.TRNSP_INST_REF_NO,";
                strSql = strSql + "TO_CHAR(JCTRDE.ATD)TRNSP_INST_DATE,";
                strSql = strSql + "RTN.TRNSP_NAME CARRIERNAME,";
                strSql = strSql + "RTN.TRNSP_NUMBER CARRIERNUMBER,";
                strSql = strSql + "RTN.SHIPPER_NAME,";
                strSql = strSql + "RTN.SHIPPER_ADDRESS,";
                strSql = strSql + "RTN.CONSIGNEE_NAME,";
                strSql = strSql + "RTN.CONSIGNEE_ADDRESS,";
                strSql = strSql + "RTN.TRNSP_MODE,";
                strSql = strSql + "RTN.TRNSP_ADDR CARRIERADD,";
                strSql = strSql + "RTN.PICKUP_ADDRESS collectionaddress,";
                strSql = strSql + "RTN.PICKUP_REF_NR,";
                strSql = strSql + "TO_CHAR(RTN.PICKUP_DATE) PICKUPDATE,";
                strSql = strSql + "RTN.DELIVERY_ADDRESS deliveryadd,";
                strSql = strSql + "RTN.TRNSP_MODE,";
                strSql = strSql + "JCTRDE.POL_FK POLPK,";
                strSql = strSql + "(CASE WHEN JCTRDE.TRANS_MODE = 3 THEN PLR.PLACE_CODE END) POLID,";
                strSql = strSql + "JCTRDE.POD_FK PODPK,";
                strSql = strSql + "(CASE WHEN JCTRDE.TRANS_MODE = 3 THEN PFD.PLACE_CODE END) PODID,";
                strSql = strSql + "(CASE WHEN JCTRDE.TRANS_MODE = 3 THEN PLR.PLACE_NAME END) POLNAME,";
                strSql = strSql + "JCTRDE.POD_FK PODPK,";
                strSql = strSql + "(CASE WHEN JCTRDE.TRANS_MODE = 3 THEN PFD.PLACE_NAME END) PODNAME,";
                strSql = strSql + "PFD.PLACE_NAME FINALDESTINATION,";
                strSql = strSql + "JCSPINST.JC_TERMS_DELIVERY TERMSOF_TRANSPORT,";
                strSql = strSql + " RTN.Weight WEIGHT,";
                strSql = strSql + " RTN.Volume VOLUME ";
                strSql = strSql + " FROM REM_TRANSPORT_NOTE_INS_TBL RTN,";
                strSql = strSql + " REM_M_JOB_CARD_MST_TBL RJC,";
                strSql = strSql + " REM_T_JC_TRANSP_DTLS_TBL  JCTRDE,";
                strSql = strSql + " PLACE_MST_TBL              PLR,";
                strSql = strSql + " PLACE_MST_TBL              PFD,";
                strSql = strSql + " REM_T_JC_ITEM_DTLS_TBL    JCITDE,";
                strSql = strSql + " REM_T_JC_SPL_INST_TBL     JCSPINST";
                strSql = strSql + " WHERE RJC.JOB_CARD_PK = JCTRDE.JOB_CARD_FK";
                strSql = strSql + " AND RJC.JOB_CARD_PK = JCITDE.JOB_CARD_FK";
                // strSql = strSql & vbCrLf & " AND RJC.JOB_CARD_PK = JCSPINST.JOB_CARD_FK"
                strSql = strSql + " AND RJC.JOB_CARD_PK = RTN.REM_JOB_CARD_FK ";
                strSql = strSql + " AND RJC.JOB_CARD_PLR_FK = PLR.PLACE_PK(+)";
                strSql = strSql + " AND RJC.JOB_CARD_PFD_FK = PFD.PLACE_PK(+)";
                strSql = strSql + " AND JCTRDE.JC_TRANSP_DTLS_PK = JCSPINST.JC_TRANSP_DTLS_FK(+)";
                strSql = strSql + " AND REM_JOB_CARD_FK= " + Rem_Jc_Pk;
                strSql = strSql + " AND JCTRDE.TRANS_MODE = 3 ";
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(strSql);
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public DataSet fetchMarksNos(long Rem_Jc_Pk)
        {
            try
            {
                string strSql = null;
                strSql = strSql + "SELECT ";
                strSql = strSql + "JCSPINST.JC_MARKS_NOS,";
                strSql = strSql + "JCSPINST.JC_GOODS_DESC GOODDESC ";
                strSql = strSql + " FROM REM_T_JC_SPL_INST_TBL  JCSPINST ";
                strSql = strSql + " WHERE JCSPINST.JOB_CARD_FK= " + Rem_Jc_Pk;
                strSql = strSql + " AND JCSPINST.TRANS_MODE = 3 ";
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(strSql);
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Fetch For Report"
    }
}