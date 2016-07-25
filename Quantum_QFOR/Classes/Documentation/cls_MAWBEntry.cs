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
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_MAWBEntry : CommonFeatures
    {
        private string strSql;
        private long _Airway_Bill_Pk;

        #region "Properties"

        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        public long Airway_Bill_Pk
        {
            get { return _Airway_Bill_Pk; }
        }

        #endregion "Properties"

        #region " Fetch After Save "

        public DataSet FetchAllAfterSave(string strPkMAWB)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();

            strQuery.Append("   SELECT M.MAWB_EXP_TBL_PK,");
            strQuery.Append("   M.AIRLINE_MST_FK,");
            strQuery.Append("   M.MAWB_MADE_FROM,");
            strQuery.Append("   M.MAWB_REF_NO,");
            strQuery.Append("   M.MAWB_DATE,");
            strQuery.Append("   A.AIRLINE_ID,");
            strQuery.Append("   M.PORT_MST_POL_FK,");
            strQuery.Append("   POL.PORT_ID POL,");
            strQuery.Append("   M.PORT_MST_POD_FK,");
            strQuery.Append("   POD.PORT_ID POD,");
            strQuery.Append("   M.CARGO_MOVE_FK,");
            strQuery.Append("   MOV.CARGO_MOVE_CODE MOVECODE,");
            strQuery.Append("   M.COMMODITY_GROUP_FK,");
            strQuery.Append("   COM.COMMODITY_GROUP_CODE,");
            strQuery.Append("   M.SHIPPER_NAME,");
            strQuery.Append("   M.SHIPPER_ADDRESS,");
            strQuery.Append("   M.CONSIGNEE_NAME,");
            strQuery.Append("   M.CONSIGNEE_ADDRESS,");
            strQuery.Append("   M.NOTIFY_NAME, ");
            strQuery.Append("   M.NOTIFY_ADDRESS,");
            strQuery.Append("   M.VERSION_NO,");
            strQuery.Append("   M.AGENT_NAME,");
            strQuery.Append("   M.AGENT_ADDRESS,");
            strQuery.Append("   M.SHIPPING_TERMS_MST_FK,");
            strQuery.Append("   SH.INCO_CODE,");
            strQuery.Append("   M.PYMT_TYPE,");
            strQuery.Append("   M.MARKS_NUMBERS,");
            strQuery.Append("   M.DEL_PLACE,");
            strQuery.Append("   M.GOODS_DESCRIPTION,");

            strQuery.Append("   M.DECL_VAL_FOR_CUSTOMS,");
            strQuery.Append("   M.DP_AGENT_MST_FK,");
            strQuery.Append("   M.BL_CLAUSE_FK, ");
            strQuery.Append("   M.COLLECT_CHARGES,");
            strQuery.Append("   M.PREPAID_CHARGES,");
            strQuery.Append("   M.ASSIGN_TO_AGENT,");
            strQuery.Append("  M.LINER_TERMS_FK,");
            strQuery.Append("   M.INSURANCE_AMT,M.AIRFREIGHT_SLABS_TBL_FK,M.ULD_NUMBER,");
            //added to retrieve
            strQuery.Append("   M.DECL_VAL_FOR_CARRIAGE,");
            strQuery.Append("  (SELECT JOB.JOBCARD_REF_NO");
            strQuery.Append("   FROM JOB_CARD_TRN JOB");
            strQuery.Append("   WHERE JOB.MBL_MAWB_FK = " + strPkMAWB + " ");
            strQuery.Append("   AND ROWNUM <=1) JOB_REF,");
            strQuery.Append("  (SELECT JOB1.JOB_CARD_TRN_PK");
            strQuery.Append("   FROM JOB_CARD_TRN JOB1");
            strQuery.Append("   WHERE JOB1.MBL_MAWB_FK = " + strPkMAWB + " ");
            strQuery.Append("   AND ROWNUM <=1) JOB_REF_PK,");
            //added for cargo type
            strQuery.Append("  (SELECT BKG.cargo_type");
            strQuery.Append("   FROM JOB_CARD_TRN JOB, BOOKING_MST_TBL bkg");
            strQuery.Append("   WHERE JOB.BOOKING_MST_FK = bkg.BOOKING_MST_PK AND JOB.MBL_MAWB_FK = " + strPkMAWB + " ");
            strQuery.Append("   AND ROWNUM <=1) CARGO_TYPE");
            //
            //adding by thiyagarajan on 3/8/08 for pts task
            strQuery.Append(" , DELPL.PLACE_CODE AS DELPL, BKG.DEL_PLACE_MST_FK,COLPL.PLACE_CODE AS COLPL,BKG.COL_PLACE_MST_FK, M.SURRENDER_DT ");
            //end

            strQuery.Append("   FROM MAWB_EXP_TBL M,");

            //adding by thiyagarajan on 3/8/08 for pts task
            strQuery.Append("   JOB_CARD_TRN JOB, BOOKING_MST_TBL  BKG,PLACE_MST_TBL  DELPL, PLACE_MST_TBL  COLPL,");
            //end

            strQuery.Append("        AIRLINE_MST_TBL A, ");
            strQuery.Append("        PORT_MST_TBL POL, ");
            strQuery.Append("        PORT_MST_TBL POD, ");
            strQuery.Append("        CARGO_MOVE_MST_TBL MOV, ");
            strQuery.Append("        COMMODITY_GROUP_MST_TBL COM, ");
            strQuery.Append("        SHIPPING_TERMS_MST_TBL SH ");

            strQuery.Append("   WHERE A.AIRLINE_MST_PK(+) = M.AIRLINE_MST_FK");
            strQuery.Append(" AND POL.PORT_MST_PK(+) = M.PORT_MST_POL_FK");
            strQuery.Append("  AND POD.PORT_MST_PK(+) = M.PORT_MST_POD_FK");
            strQuery.Append(" AND MOV.CARGO_MOVE_PK(+) = M.CARGO_MOVE_FK");
            strQuery.Append(" AND COM.COMMODITY_GROUP_PK(+) = M.COMMODITY_GROUP_FK");
            strQuery.Append(" AND SH.SHIPPING_TERMS_MST_PK(+) = M.SHIPPING_TERMS_MST_FK");
            //adding by thiyagarajan on 3/8/08 for pts task
            strQuery.Append(" AND JOB.MBL_MAWB_FK(+)=M.MAWB_EXP_TBL_PK   AND BKG.BOOKING_MST_PK(+)=JOB.BOOKING_MST_FK");
            strQuery.Append(" AND DELPL.PLACE_PK(+)=BKG.DEL_PLACE_MST_FK   AND COLPL.PLACE_PK(+)=BKG.COL_PLACE_MST_FK ");
            //end

            strQuery.Append(" AND M.MAWB_EXP_TBL_PK =" + strPkMAWB + " ");
            try
            {
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

        #endregion " Fetch After Save "

        #region " Function to Find Mawb Pk"

        public int FindMawbPk(string RefNo)
        {
            //Added by rabbani on 8/12/06
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select m.mawb_exp_tbl_pk from mawb_exp_tbl m where m.mawb_ref_no = '" + RefNo + "' ";
                //strSQL = " SELECT JOB.MBL_MAWB_FK FROM JOB_CARD_TRN JOB WHERE JOB.JOB_CARD_TRN_PK = " & JobCardPK
                string MBLPK = null;
                MBLPK = objWF.ExecuteScaler(strSQL);
                return Convert.ToInt32(MBLPK);
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

        //Ended by rabbani

        #endregion " Function to Find Mawb Pk"

        #region " Fetch In Grid After Save"

        public object FetchInGridAfterSave(string strPkMAWB)
        {
            WorkFlow objWF = new WorkFlow();
            strSql = " SELECT ROWNUM SR_NO,q.* FROM (SELECT " + "MT.PALETTE_SIZE," + "MT.VOLUME_IN_CBM," + "MT.GROSS_WEIGHT," + "MT.CHARGEABLE_WEIGHT," + "MT.PACK_TYPE_MST_FK," + "PK.PACK_TYPE_DESC," + "MT.PACK_COUNT," + "MT.COMMODITY_MST_FKS COMMODITY_MST_FK," + "(SELECT rowtocol('SELECT c.commodity_name FROM commodity_mst_tbl c WHERE c.commodity_mst_pk IN ('|| NVL(MT.COMMODITY_MST_FKS,-1) ||')') FROM dual) " + "AS COMMODITY_ID" + "FROM MAWB_TRN_EXP_CONTAINER MT," + "PACK_TYPE_MST_TBL PK," + "COMMODITY_MST_TBL COM" + "WHERE" + "PK.PACK_TYPE_MST_PK(+)=MT.PACK_TYPE_MST_FK AND" + "COM.COMMODITY_MST_PK(+)=MT.COMMODITY_MST_FK AND" + "MT.MAWB_EXP_TBL_FK=" + strPkMAWB + ") q";
            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion " Fetch In Grid After Save"

        #region " Fetch Hawb After Save"

        public DataSet FetchHAWBsafterSave(string strPkMAWB, string type)
        {
            WorkFlow objWF = new WorkFlow();
            if (type == "1")
            {
                strSql = "SELECT H.HAWB_REF_NO , H.HAWB_EXP_TBL_PK FROM HAWB_EXP_TBL H WHERE H.MAWB_EXP_TBL_FK =" + strPkMAWB;
            }
            else
            {
                strSql = "SELECT j.JOB_CARD_TRN_PK, j.jobcard_ref_no FROM JOB_CARD_TRN j WHERE j.MBL_MAWB_FK=" + strPkMAWB;
                //Try
            }
            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion " Fetch Hawb After Save"

        #region " Fetch All"

        public DataTable fetchAll(string strPk = "0", string TYPE = "0")
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (TYPE == "0")
                {
                    objWF.MyCommand.Parameters.Clear();
                    var _with4 = objWF.MyCommand.Parameters;
                    _with4.Add("HAWB_PK", Convert.ToInt64(strPk)).Direction = ParameterDirection.Input;
                    _with4.Add("MAWB_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                    return objWF.GetDataTable("FETCH_MAWB_DATA", "FETCH_HAWB");
                }
                else
                {
                    objWF.MyCommand.Parameters.Clear();
                    var _with5 = objWF.MyCommand.Parameters;
                    _with5.Add("JOBCARD_PK", Convert.ToInt64(strPk)).Direction = ParameterDirection.Input;
                    _with5.Add("MAWB_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                    return objWF.GetDataTable("FETCH_MAWB_DATA", "FETCH_JOBCARD");
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

        //Fetching Agent Address To Fill the Agent Address Details
        public DataSet FetchCongAgtMAWB(long Jobpk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT AGT.AGENT_NAME,AGT_DTLS.ADM_ADDRESS_1,AGT_DTLS.ADM_ADDRESS_2,AGT_DTLS.ADM_ADDRESS_3,");
            StrSqlBuilder.Append("  AGT_DTLS.ADM_PHONE_NO_1,AGT_DTLS.ADM_FAX_NO,AGT_DTLS.ADM_EMAIL_ID FROM JOB_CARD_TRN JOB,");
            StrSqlBuilder.Append("  AGENT_CONTACT_DTLS   AGT_DTLS,AGENT_MST_TBL AGT WHERE AGT.AGENT_MST_PK = AGT_DTLS.AGENT_MST_FK");
            StrSqlBuilder.Append("  AND AGT.AGENT_MST_PK = JOB.DP_AGENT_MST_FK AND JOB.JOB_CARD_TRN_PK = " + Jobpk + "");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        public DataSet FetchShipAgtMAWB(long Jobpk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  select cm.customer_name, cc.adm_address_1, cc.adm_address_2, cc.adm_address_3,");
            StrSqlBuilder.Append("  cc.adm_phone_no_1, cc.cor_fax_no, cc.adm_email_id from JOB_CARD_TRN j, customer_contact_dtls cc, customer_mst_tbl cm");
            StrSqlBuilder.Append("  where cc.customer_mst_fk = cm.customer_mst_pk and j.shipper_cust_mst_fk = cm.customer_mst_pk and j.JOB_CARD_TRN_PK= " + Jobpk);

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        #endregion " Fetch All"

        #region " Fetch Temp Customer" 'Manoharan 21Feb07: to fetch Temp customer in HBL / JC

        public DataSet fetchTempCust(string RefNr, Int16 Type)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //from HAWB
                if (Type == 1)
                {
                    strSQL.Append("SELECT H.SHIPPER_CUST_MST_FK FROM ");
                    strSQL.Append(" HAWB_EXP_TBL H WHERE H.HAWB_REF_NO='" + RefNr + "'");
                    //from JC
                }
                else if (Type == 2)
                {
                    strSQL.Append("select j.shipper_cust_mst_fk from ");
                    strSQL.Append(" JOB_CARD_TRN J WHERE J.JOBCARD_REF_NO='" + RefNr + "'");
                }
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

        #endregion " Fetch Temp Customer" 'Manoharan 21Feb07: to fetch Temp customer in HBL / JC

        #region " Fetch In Grid"

        public DataSet FetchInGrid(string STRPK = "0", string hawbPKs = "")
        {
            WorkFlow objWF = new WorkFlow();

            strSql = " SELECT ROWNUM SR_NO,q.* FROM (SELECT " + "JT.PALETTE_SIZE," + "JT.VOLUME_IN_CBM," + "JT.GROSS_WEIGHT," + "JT.CHARGEABLE_WEIGHT," + "(SELECT ROWTOCOL('SELECT PT.PACK_TYPE_MST_PK FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (     SELECT DISTINCT JOB.PACK_TYPE_MST_FK FROM JOB_TRN_CONT JOB WHERE JOB.JOB_TRN_CONT_PK=' ||" + "JT.JOB_TRN_CONT_PK || ')')" + "FROM DUAL) PACK_TYPE_MST_FK," + "(SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (    SELECT DISTINCT JOB.PACK_TYPE_MST_FK FROM JOB_TRN_CONT JOB WHERE JOB.JOB_TRN_CONT_PK=' ||" + "JT.JOB_TRN_CONT_PK || ')')" + "FROM DUAL) PACK_TYPE_DESC," + "JT.PACK_COUNT," + "NVL(JT.COMMODITY_MST_FKS,JT.COMMODITY_MST_FK) COMMODITY_MST_FK," + "(SELECT rowtocol('SELECT c.commodity_name FROM commodity_mst_tbl c WHERE c.commodity_mst_pk IN ('|| NVL(JT.COMMODITY_MST_FKS,-1) ||')') FROM dual) " + "AS COMMODITY_ID" + "FROM JOB_TRN_CONT JT," + "PACK_TYPE_MST_TBL PK,";
            if (hawbPKs.Length > 0)
            {
                strSql += " HAWB_EXP_TBL H, ";
            }
            strSql += " COMMODITY_MST_TBL COM " + "WHERE" + "PK.PACK_TYPE_MST_PK(+)=JT.PACK_TYPE_MST_FK AND " + "COM.COMMODITY_MST_PK(+)=JT.COMMODITY_MST_FK AND ";
            if (hawbPKs.Length > 0)
            {
                strSql += " H.JOB_CARD_AIR_EXP_FK=JT.JOB_CARD_TRN_FK AND";
                strSql += " H.HAWB_EXP_TBL_PK IN (" + hawbPKs + ")";
            }
            else
            {
                strSql += "JT.JOB_CARD_TRN_FK=" + STRPK;
                // strSql &= "JT.JOB_CARD_AIR_EXP_FK in (" & STRPK
            }
            strSql += ") q";
            //strSql &= ")) q"
            try
            {
                return objWF.GetDataSet(strSql);
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

        public string FetchCommodity(string MJCNr)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append("SELECT NVL(ROWTOCOL('SELECT DISTINCT JT.COMMODITY_MST_FKS");
            sb.Append("  FROM MASTER_JC_AIR_EXP_TBL MJ,");
            sb.Append("       JOB_CARD_TRN  JC,");
            sb.Append("       JOB_TRN_CONT  JT");
            sb.Append(" WHERE MJ.MASTER_JC_AIR_EXP_PK = JC.MASTER_JC_FK");
            sb.Append("   AND JC.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK");
            sb.Append("   AND MJ.MASTER_JC_REF_NO = ''" + MJCNr + "'' ");
            sb.Append(" '),'null') ");
            sb.Append("  FROM DUAL");
            try
            {
                return objWF.ExecuteScaler(sb.ToString());
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

        #endregion " Fetch In Grid"

        #region " Fetch in DropDownList Of ULDType"

        public DataSet FetchULDType()
        {
            WorkFlow objWF = new WorkFlow();

            strSql = " select 0 airfreight_slabs_tbl_pk, 'Select' breakpoint_id from airfreight_slabs_tbl " + "union" + "select airfreight_slabs_tbl_pk, breakpoint_id from airfreight_slabs_tbl where breakpoint_type='2' and active_flag = 1";

            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion " Fetch in DropDownList Of ULDType"

        #region " Fetch in DropDownList Of ULDType for listing"

        public DataSet FetchULDTypelist()
        {
            WorkFlow objWF = new WorkFlow();

            strSql = " select 0 airfreight_slabs_tbl_pk, 'All' breakpoint_id from airfreight_slabs_tbl " + "union" + "select airfreight_slabs_tbl_pk, breakpoint_id from airfreight_slabs_tbl where breakpoint_type='2' and active_flag = 1";

            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion " Fetch in DropDownList Of ULDType for listing"

        #region " Fetch Agent"

        public DataSet FETCHagent()
        {
            WorkFlow objWF = new WorkFlow();
            strSql = "SELECT C.CORPORATE_NAME, C.ADDRESS_LINE1, C.ADDRESS_LINE2, C.ADDRESS_LINE3" + "FROM CORPORATE_MST_TBL C";
            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion " Fetch Agent"

        #region "Fetch JobCards"

        public Int16 findJCused_inMJC(string Jcpks)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSqlBuilder = new StringBuilder();

            strSqlBuilder.Append(" select H.HAWB_EXP_TBL_PK, ");
            strSqlBuilder.Append("h.HAWB_REF_NO,H.JOB_CARD_AIR_EXP_FK,");
            strSqlBuilder.Append("h.marks_numbers,");

            try
            {
                return Convert.ToInt16(objWF.ExecuteScaler(strSqlBuilder.ToString()));
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

        #endregion "Fetch JobCards"

        #region " Fetch from consolidation"

        public DataSet fetchByConsolidationHAWB(string HAWBs)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSqlBuilder = new StringBuilder();
            strSqlBuilder.Append(" select H.HAWB_EXP_TBL_PK, ");
            strSqlBuilder.Append("h.HAWB_REF_NO,H.JOB_CARD_AIR_EXP_FK,");
            strSqlBuilder.Append("h.marks_numbers,");
            strSqlBuilder.Append("H.GOODS_DESCRIPTION,");
            strSqlBuilder.Append("H.DECL_VAL_FOR_CUSTOMS,");
            strSqlBuilder.Append("B.DEL_PLACE_MST_FK,");
            strSqlBuilder.Append("P.PLACE_NAME,");

            strSqlBuilder.Append(" nvl(round((get_ex_rate(j.insurance_currency,cm.currency_mst_fk,sysdate) * j.insurance_amt),2),0) insurance_amt ,");

            strSqlBuilder.Append(" (Select round(sum(IMSUM), 2) ");
            strSqlBuilder.Append("  from (SELECT SUM(FD.FREIGHT_AMT * fd.exchange_rate) IMSUM");
            strSqlBuilder.Append("  FROM JOB_TRN_FD FD, JOB_CARD_TRN job");
            strSqlBuilder.Append("  ");
            strSqlBuilder.Append(" WHERE FD.FREIGHT_TYPE = 1 ");
            strSqlBuilder.Append("     AND FD.JOB_CARD_TRN_FK =");
            strSqlBuilder.Append("  JOB.JOB_CARD_TRN_PK");
            strSqlBuilder.Append("     and job.HBL_HAWB_FK in (" + HAWBs + ")");

            strSqlBuilder.Append("     union select sum(jo.exchange_rate * jo.amount)");
            strSqlBuilder.Append("   from JOB_TRN_OTH_CHRG jo,");
            strSqlBuilder.Append("            JOB_CARD_TRN job ");
            strSqlBuilder.Append("      where jo.JOB_CARD_TRN_FK = ");
            strSqlBuilder.Append("    job.JOB_CARD_TRN_PK");
            strSqlBuilder.Append("     and job.HBL_HAWB_FK In (" + HAWBs + " )");
            strSqlBuilder.Append("     and jo.FREIGHT_TYPE = 1 ");
            strSqlBuilder.Append("   )) PREPAID, ");

            strSqlBuilder.Append(" (Select round(sum(COLSUM), 2) from ");
            strSqlBuilder.Append(" (SELECT SUM(FD.FREIGHT_AMT * FD.EXCHANGE_RATE) COLSUM");
            strSqlBuilder.Append("   FROM JOB_TRN_FD FD, ");
            strSqlBuilder.Append("  JOB_CARD_TRN job");
            strSqlBuilder.Append("             WHERE FD.FREIGHT_TYPE = 2");
            strSqlBuilder.Append("  AND FD.JOB_CARD_TRN_FK = job.JOB_CARD_TRN_PK ");
            strSqlBuilder.Append("     and job.HBL_HAWB_FK in (" + HAWBs + ")");

            strSqlBuilder.Append("     union select sum(jo.exchange_rate * jo.amount) ");
            strSqlBuilder.Append("   from JOB_TRN_OTH_CHRG jo,");
            strSqlBuilder.Append("            JOB_CARD_TRN job ");
            strSqlBuilder.Append("      where jo.JOB_CARD_TRN_FK = ");
            strSqlBuilder.Append("    job.JOB_CARD_TRN_PK");
            strSqlBuilder.Append("     and job.HBL_HAWB_FK In (" + HAWBs + " )");
            strSqlBuilder.Append("     and jo.FREIGHT_TYPE = 2 ");
            strSqlBuilder.Append("   ))  COLL ");

            strSqlBuilder.Append("    FROM HAWB_EXP_TBL         H,");
            strSqlBuilder.Append("   JOB_CARD_TRN j,");
            strSqlBuilder.Append(" BOOKING_MST_TBL      B,");
            strSqlBuilder.Append(" PLACE_MST_TBL P,");
            strSqlBuilder.Append(" CORPORATE_MST_TBL CM");

            strSqlBuilder.Append("  WHERE H.JOB_CARD_AIR_EXP_FK = J.JOB_CARD_TRN_PK ");
            strSqlBuilder.Append(" AND J.BOOKING_MST_FK = B.BOOKING_MST_PK");
            strSqlBuilder.Append("  AND B.DEL_PLACE_MST_FK = P.PLACE_PK(+)");
            strSqlBuilder.Append(" AND H.HAWB_EXP_TBL_PK IN (" + HAWBs + ")");
            try
            {
                return objWF.GetDataSet(strSqlBuilder.ToString());
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

        public DataSet fetchByConsolidationJOBCARD(string JOB_Pks)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSqlBuilder = new StringBuilder();
            strSqlBuilder.Append("SELECT H.HAWB_EXP_TBL_PK,");
            strSqlBuilder.Append("J.JOBCARD_REF_NO,");
            strSqlBuilder.Append("J.JOB_CARD_TRN_PK,");
            strSqlBuilder.Append("J.MARKS_NUMBERS, ");
            strSqlBuilder.Append("J.GOODS_DESCRIPTION, ");
            strSqlBuilder.Append("H.DECL_VAL_FOR_CUSTOMS,");
            strSqlBuilder.Append("B.DEL_PLACE_MST_FK,");
            strSqlBuilder.Append("P.PLACE_NAME,");

            //strSqlBuilder.Append(" round(get_ex_rate(j.insurance_currency,cm.currency_mst_fk,sysdate) * sum(j.insurance_amt),2) insurance_amt ,")
            strSqlBuilder.Append(" nvl(round((get_ex_rate(j.insurance_currency,cm.currency_mst_fk,sysdate) * j.insurance_amt),2),0) insurance_amt ,");
            strSqlBuilder.Append(" (Select round(sum(IMSUM), 2) ");
            strSqlBuilder.Append("  from (SELECT SUM(FD.FREIGHT_AMT * fd.exchange_rate) IMSUM");
            strSqlBuilder.Append("  FROM JOB_TRN_FD FD, ");
            strSqlBuilder.Append("  JOB_CARD_TRN job");
            strSqlBuilder.Append(" WHERE FD.FREIGHT_TYPE = 1 ");
            strSqlBuilder.Append("     AND FD.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK ");
            strSqlBuilder.Append("     and job.JOB_CARD_TRN_PK in (" + JOB_Pks + ")");

            strSqlBuilder.Append("     union select sum(jo.exchange_rate * jo.amount)");
            strSqlBuilder.Append("   from JOB_TRN_OTH_CHRG jo,");
            strSqlBuilder.Append("            JOB_CARD_TRN job ");
            strSqlBuilder.Append("      where jo.JOB_CARD_TRN_FK = ");
            strSqlBuilder.Append("    job.JOB_CARD_TRN_PK");
            strSqlBuilder.Append("     and job.JOB_CARD_TRN_PK In (" + JOB_Pks + " )");
            strSqlBuilder.Append("     and jo.FREIGHT_TYPE = 1 ");
            strSqlBuilder.Append("   )) PREPAID, ");

            strSqlBuilder.Append(" (Select round(sum(COLSUM), 2) ");
            strSqlBuilder.Append(" from (SELECT SUM(FD.FREIGHT_AMT * FD.EXCHANGE_RATE) COLSUM");
            strSqlBuilder.Append("   FROM JOB_TRN_FD FD,");
            strSqlBuilder.Append("  JOB_CARD_TRN job");
            strSqlBuilder.Append("             WHERE FD.FREIGHT_TYPE = 2");
            strSqlBuilder.Append(" AND FD.JOB_CARD_TRN_FK = job.JOB_CARD_TRN_PK ");
            strSqlBuilder.Append("     and job.JOB_CARD_TRN_PK in (" + JOB_Pks + ")");
            strSqlBuilder.Append("     union select sum(jo.exchange_rate * jo.amount)");
            strSqlBuilder.Append("   from JOB_TRN_OTH_CHRG jo,");
            strSqlBuilder.Append("            JOB_CARD_TRN job ");
            strSqlBuilder.Append("      where jo.JOB_CARD_TRN_FK = ");
            strSqlBuilder.Append("    job.JOB_CARD_TRN_PK");
            strSqlBuilder.Append("     and job.JOB_CARD_TRN_PK in (" + JOB_Pks + ")");
            strSqlBuilder.Append("     and jo.FREIGHT_TYPE = 2 ");
            strSqlBuilder.Append("   )) COLL ");

            strSqlBuilder.Append("  FROM HAWB_EXP_TBL         H, ");
            strSqlBuilder.Append("      JOB_CARD_TRN J, ");
            strSqlBuilder.Append("     BOOKING_MST_TBL      B,");
            strSqlBuilder.Append("  PLACE_MST_TBL P , ");
            strSqlBuilder.Append(" CORPORATE_MST_TBL CM");

            strSqlBuilder.Append(" WHERE H.JOB_CARD_AIR_EXP_FK(+) = J.JOB_CARD_TRN_PK ");
            strSqlBuilder.Append(" AND J.BOOKING_MST_FK = B.BOOKING_MST_PK ");
            strSqlBuilder.Append(" AND B.DEL_PLACE_MST_FK = P.PLACE_PK(+)");
            strSqlBuilder.Append(" AND J.JOB_CARD_TRN_PK IN  (" + JOB_Pks + ")");
            try
            {
                return objWF.GetDataSet(strSqlBuilder.ToString());
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

        #endregion " Fetch from consolidation"

        #region " Other Functions"

        public DataSet agent(string PK)
        {
            WorkFlow objWF = new WorkFlow();
            strSql = "select AG.AGENT_NAME," + "AGD.ADM_ADDRESS_1," + "AGD.ADM_ADDRESS_2," + "AGD.ADM_ADDRESS_3" + "from agent_mst_tbl ag, AGENT_CONTACT_DTLS AGD" + "WHERE AG.AGENT_MST_PK =" + PK;
            strSql += " AND AG.AGENT_MST_PK = AGD.AGENT_MST_FK";
            try
            {
                return objWF.GetDataSet(strSql);
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

        public DataSet COMMODITY(string PK)
        {
            WorkFlow objWF = new WorkFlow();
            strSql = " SELECT C.COMMODITY_GROUP_CODE FROM COMMODITY_GROUP_MST_TBL C " + "WHERE C.COMMODITY_GROUP_PK=" + PK;
            try
            {
                return objWF.GetDataSet(strSql);
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
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

        private object ifDouble(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return Convert.ToDouble(col);
            }
        }

        private object ifDateNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return Convert.ToDateTime(col);
            }
        }

        #endregion " Other Functions"

        #region " Fetch MAWB Header Document Report Data"

        //        Public Function FetchMAWBHeaderData(ByVal MAWBPK As Integer) As DataSet
        //            Dim objWF As New WorkFlow
        //            Dim strSQL As String
        //            strSQL = "select JobCardairExp.JOB_CARD_TRN_PK JOBPK,"
        //            strSQL &= vbCrLf & "mawb.mawb_exp_tbl_pk HBPK,"
        //            strSQL &= vbCrLf & "JobCardairExp.Jobcard_Ref_No JOBNO,"
        //            strSQL &= vbCrLf & "JobCardairExp.Ucr_No UCRNO,"

        //            strSQL &= vbCrLf & "mawb.mawb_ref_no HBNO,"
        //            strSQL &= vbCrLf & "JobCardairExp.Flight_No VES_FLIGHT,"
        //            strSQL &= vbCrLf & "'' voyage,"
        //            strSQL &= vbCrLf & "POL.PORT_NAME POL,"
        //            strSQL &= vbCrLf & "POD.PORT_NAME POD,"
        //            strSQL &= vbCrLf & "MAWB.DEL_PLACE PLD,"
        //            strSQL &= vbCrLf & "mawb.Shipper_Name Shipper,"
        //            strSQL &= vbCrLf & "mawb.Shipper_Address ShiAddress1,"
        //            strSQL &= vbCrLf & "'' ShiAddress2,"
        //            strSQL &= vbCrLf & "'' ShiAddress3,"
        //            strSQL &= vbCrLf & " '' ShiCity,"
        //            strSQL &= vbCrLf & "mawb.Consignee_Name Consignee,"
        //            strSQL &= vbCrLf & "mawb.Consignee_Address ConsiAddress1,"
        //            strSQL &= vbCrLf & "  '' ConsiAddress2,"
        //            strSQL &= vbCrLf & "  '' ConsiAddress3,"
        //            strSQL &= vbCrLf & "  '' ConsiCity,"

        //            strSQL &= vbCrLf & "mawb.AGENT_NAME Agent_Name,"
        //            strSQL &= vbCrLf & "mawb.agent_address AgtAddress1,"
        //            strSQL &= vbCrLf & " '' AgtAddress2,"
        //            strSQL &= vbCrLf & " '' AgtAddress3,"
        //            strSQL &= vbCrLf & " '' AgtCity,"
        //            strSQL &= vbCrLf & " mawb.Goods_Description "
        //            strSQL &= vbCrLf & "from  JOB_CARD_TRN JobCardairExp,"
        //            strSQL &= vbCrLf & "MAWB_EXP_TBL MAWB,"
        //            strSQL &= vbCrLf & " PORT_MST_TBL POL,"
        //            strSQL &= vbCrLf & " PORT_MST_TBL POD"
        //            strSQL &= vbCrLf & "where JobcardairExp.MBL_MAWB_FK(+) = mawb.mawb_exp_tbl_pk"
        //            strSQL &= vbCrLf & "and   POL.PORT_MST_PK(+)=MAWB.PORT_MST_POL_FK"
        //            strSQL &= vbCrLf & " and   POD.PORT_MST_PK(+)=MAWB.PORT_MST_POD_FK"
        //            strSQL &= vbCrLf & " and   MAWB.MAWB_EXP_TBL_PK=" & MAWBPK
        //            Try
        //                Return (objWF.GetDataSet(strSQL))
        //            Catch sqlExp As Exception
        //                ErrorMessage = sqlExp.Message
        //                Throw sqlExp
        //            End Try
        //        End Function

        #endregion " Fetch MAWB Header Document Report Data"

        #region "Fetch MAWB Header Document Report Data"

        public DataSet FetchMAWBData(int MAWBPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT MAWB.MAWB_EXP_TBL_PK MAPK,";
            strSQL += "MAWB.SHIPPER_NAME SHIPPER,";
            strSQL += "MAWB.SHIPPER_ADDRESS SHIPPERADDRESS,";
            strSQL += "MAWB.CONSIGNEE_NAME CONSIGNEE,";
            strSQL += "MAWB.CONSIGNEE_ADDRESS CONSIGNEEADDRESS,";
            strSQL += "POL.PORT_NAME POLNAME,";
            strSQL += "POL.PORT_ID POLID,";
            strSQL += "POD.PORT_NAME PODNAME,";
            strSQL += "POD.PORT_ID PODID,";
            strSQL += "AMST.AIRLINE_NAME AIRLINE,";
            strSQL += "MAWB.MAWB_MADE_FROM FLIGHT,";
            strSQL += "AMST.CARGO_DEL_ADDRESS AIRLINEADDRESS,";
            strSQL += "AMST.ACCOUNT_NO AIRLINE_AC_NO,";
            strSQL += "MAWB.DECL_VAL_FOR_CUSTOMS DECLVALCUSTOMS,";
            strSQL += "MAWB.DECL_VAL_FOR_CARRIAGE DECLVALCARRIAGE,";
            strSQL += "MAWB.INSURANCE_AMT INSURANCE_AMT,";
            strSQL += "MAWB.TOTAL_PACK_COUNT NO_OF_PIECES,";
            strSQL += "MAWB.TOTAL_GROSS_WEIGHT GROSS_WEIGHT,";
            strSQL += "MAWB.TOTAL_CHARGEABLE_WEIGHT CHARGE_WEIGHT,";
            strSQL += "MAWB.GOODS_DESCRIPTION GOODS,";
            strSQL += "MAWB.PREPAID_CHARGES PREPAID_CHARGES,";
            strSQL += "MAWB.COLLECT_CHARGES COLLECT_CHARGES,";
            strSQL += "COP.CORPORATE_NAME CORPORATE_NAME,";
            strSQL += "COP.ADDRESS_LINE1 COP_ADD1,";
            strSQL += "COP.ADDRESS_LINE2 COP_ADD2,";
            strSQL += "COP.ADDRESS_LINE3 COP_ADD3,";
            strSQL += "COP.CITY COP_CITY,";
            strSQL += "COUNTRY_MST.COUNTRY_NAME COUNTRY_NAME,";
            strSQL += "COP.PHONE COP_PHONE,";
            strSQL += "COP.FAX COP_FAX,";
            strSQL += "COP.EMAIL COP_EMAIL,";
            strSQL += "COP.IATA_CODE IATA_CODE,";
            strSQL += "COP.ACCOUNT_NO ACCOUNT_NO,";
            strSQL += "CURR_MST.CURRENCY_NAME CURRENCY_NAME,";
            strSQL += "AGENT_MST.ACCOUNT_NO CONSIGNEE_AC_NO,";
            strSQL += "CSMT.CUSTOMS_STATUS_CODE CUST_STATUS";
            strSQL += "FROM MAWB_EXP_TBL MAWB,";
            strSQL += "PORT_MST_TBL POL,";
            strSQL += "PORT_MST_TBL POD,";
            strSQL += "AIRLINE_MST_TBL AMST,";
            strSQL += "HAWB_EXP_TBL HAWB,";
            strSQL += "CORPORATE_MST_TBL COP,";
            strSQL += "CURRENCY_TYPE_MST_TBL CURR_MST,";
            strSQL += "COUNTRY_MST_TBL COUNTRY_MST,";
            strSQL += "AGENT_MST_TBL AGENT_MST,";
            strSQL += "BOOKING_MST_TBL BAT,";
            strSQL += "JOB_CARD_TRN JAE,";
            strSQL += "CUSTOMS_STATUS_MST_TBL CSMT";
            strSQL += "WHERE POL.PORT_MST_PK(+)=MAWB.PORT_MST_POL_FK";
            strSQL += "AND   POD.PORT_MST_PK(+)=MAWB.PORT_MST_POD_FK  ";
            strSQL += "AND   AMST.AIRLINE_MST_PK(+)=MAWB.AIRLINE_MST_FK   ";
            strSQL += "AND   HAWB.MAWB_EXP_TBL_FK(+)=MAWB.MAWB_EXP_TBL_PK";
            strSQL += "AND   CURR_MST.CURRENCY_MST_PK(+)=COP.CURRENCY_MST_FK";
            strSQL += "AND   COUNTRY_MST.COUNTRY_MST_PK(+)=COP.COUNTRY_MST_FK";
            strSQL += "AND   AGENT_MST.AGENT_MST_PK(+)=MAWB.DP_AGENT_MST_FK";
            strSQL += "AND JAE.BOOKING_MST_FK=BAT.BOOKING_MST_PK(+)";
            strSQL += "AND JAE.MBL_MAWB_FK(+)=MAWB.MAWB_EXP_TBL_PK";
            strSQL += "AND CSMT.CUSTOMS_CODE_MST_PK(+)=BAT.CUSTOMS_CODE_MST_FK";
            strSQL += "AND   MAWB.MAWB_EXP_TBL_PK =" + MAWBPK;
            strSQL += "group by";
            strSQL += "MAWB.MAWB_EXP_TBL_PK ,";
            strSQL += "MAWB.SHIPPER_NAME ,";
            strSQL += "MAWB.SHIPPER_ADDRESS ,";
            strSQL += "MAWB.CONSIGNEE_NAME ,";
            strSQL += "MAWB.CONSIGNEE_ADDRESS ,";
            strSQL += "POL.PORT_NAME ,";
            strSQL += " POL.PORT_ID ,";
            strSQL += " POD.PORT_NAME ,";
            strSQL += " POD.PORT_ID,";
            strSQL += " AMST.AIRLINE_NAME ,";
            strSQL += "MAWB.MAWB_MADE_FROM,";
            strSQL += " AMST.CARGO_DEL_ADDRESS,";
            strSQL += " AMST.ACCOUNT_NO,";
            strSQL += " MAWB.DECL_VAL_FOR_CUSTOMS ,";
            strSQL += " MAWB.DECL_VAL_FOR_CARRIAGE,";
            strSQL += " MAWB.INSURANCE_AMT ,";
            strSQL += " MAWB.TOTAL_PACK_COUNT,";
            strSQL += " MAWB.TOTAL_GROSS_WEIGHT,";
            strSQL += " MAWB.TOTAL_CHARGEABLE_WEIGHT,";
            strSQL += " MAWB.GOODS_DESCRIPTION,";
            strSQL += " MAWB.PREPAID_CHARGES,";
            strSQL += " MAWB.COLLECT_CHARGES,";
            strSQL += " COP.CORPORATE_NAME,";
            strSQL += " COP.ADDRESS_LINE1,";
            strSQL += " COP.ADDRESS_LINE2,";
            strSQL += " COP.ADDRESS_LINE3 ,";
            strSQL += " COP.CITY,";
            strSQL += " COUNTRY_MST.COUNTRY_NAME ,";
            strSQL += " COP.PHONE,";
            strSQL += "  COP.FAX ,";
            strSQL += "  COP.EMAIL,";
            strSQL += "  COP.IATA_CODE,";
            strSQL += "  COP.ACCOUNT_NO,";
            strSQL += "  CURR_MST.CURRENCY_NAME,";
            strSQL += "  AGENT_MST.ACCOUNT_NO,";
            strSQL += "CSMT.CUSTOMS_STATUS_CODE";

            try
            {
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

        public DataSet FetchJFLIGHT(int MAWBPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "Select J.FLIGHT_NO FLIGHT";
            strSQL += "FROM MAWB_EXP_TBL M, JOB_CARD_TRN J";
            strSQL += "WHERE ";
            strSQL += " M.MAWB_EXP_TBL_PK = J.MBL_MAWB_FK(+)";
            strSQL += "AND M.MAWB_EXP_TBL_PK = " + MAWBPK;
            try
            {
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

        public DataSet FetchHFLIGHT(int MAWBPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "Select H.FLIGHT_NO FLIGHT";
            strSQL += "FROM MAWB_EXP_TBL M, HAWB_EXP_TBL H";
            strSQL += "WHERE ";
            strSQL += " M.MAWB_EXP_TBL_PK = H.MAWB_EXP_TBL_FK(+)";
            strSQL += "AND M.MAWB_EXP_TBL_PK = " + MAWBPK;
            try
            {
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

        #endregion "Fetch MAWB Header Document Report Data"

        #region " ENHANCE FOR MAWB"

        public string FetchForJobRefMAWB(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            string strLoc = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strLoc = loc;
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            //Business Type to identify the user belongs to AIR/SEA
            //If arr.Length > 3 Then strBusiType = arr(3)
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MAWB_PKG.GET_ACTIVE_JOB_REF_COMMON_MAWB";
                var _with6 = SCM.Parameters;
                _with6.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                // .Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
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
                SCM.Connection.Close();
            }
        }

        public string FetchForHAWBRefMAWB(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            string strLoc = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strLoc = loc;
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            //Business Type to identify the user belongs to AIR/SEA
            //If arr.Length > 3 Then strBusiType = arr(3)
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MAWB_PKG.GET_HAWB_REF_FOR_MAWB";
                var _with7 = SCM.Parameters;
                _with7.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                // .Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input
                _with7.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with7.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
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
                SCM.Connection.Close();
            }
        }

        #endregion " ENHANCE FOR MAWB"

        #region "Generate Reference Int32"

        //
        // This function returns reference number generated for airway
        // also it returns airway bill master table pk
        public string GenerateReference_Number(long Pk, int type, long Airpk, long Locpk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                string ReferenceNo = "";
                if (Pk > 0)
                {
                    objWF.OpenConnection();
                    objWF.MyCommand.Connection = objWF.MyConnection;
                    objWF.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWF.MyCommand.CommandText = objWF.MyUserName + "." + "MAWB_REF_NO";

                    var _with8 = objWF.MyCommand.Parameters;
                    _with8.Add("PK", Pk).Direction = ParameterDirection.Input;
                    _with8.Add("HAWB_JOB", type).Direction = ParameterDirection.Input;
                    _with8.Add("AIR_PK", Airpk).Direction = ParameterDirection.Input;
                    _with8.Add("LOC_PK", Locpk).Direction = ParameterDirection.Input;
                    _with8.Add("AIRWAY_BILL_PK", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with8.Add("MAWB_REF_NO", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                    objWF.MyCommand.ExecuteNonQuery();
                    _Airway_Bill_Pk = Convert.ToInt64(objWF.MyCommand.Parameters["AIRWAY_BILL_PK"].Value);
                    ReferenceNo = Convert.ToString(objWF.MyCommand.Parameters["MAWB_REF_NO"].Value);
                    if (string.Compare(ReferenceNo, "not") > 0)
                    {
                        throw new Exception("Airway bill number not found");
                    }
                    else
                    {
                        return ReferenceNo;
                    }
                }
                else
                {
                    return ReferenceNo;
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

        #endregion "Generate Reference Int32"

        //Akhilesh ... Start [29-May-06] [EFS]
        //Reason: HAWB Printing

        #region " Print MAWB "

        public DataSet MAWB_Print(long MAWB_PK, long Loged_In_Loc)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with9 = objWK.MyCommand.Parameters;
                _with9.Add("MAWB_PK_IN", MAWB_PK).Direction = ParameterDirection.Input;
                _with9.Add("LOGGED_IN_LOC_FK", Loged_In_Loc).Direction = ParameterDirection.Input;
                _with9.Add("MAWB_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("HAWB_MAWB_PRINT_PKG", "MAWB_PRINT");
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

        public DataSet MAWB_FRT(long MAWB_PK)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with10 = objWK.MyCommand.Parameters;
                _with10.Add("MAWB_PK_IN", MAWB_PK).Direction = ParameterDirection.Input;
                _with10.Add("MAWB_ITEM", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("HAWB_MAWB_PRINT_PKG", "MAWB_ITEMWISE_DTLS");
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

        #endregion " Print MAWB "

        #region "Packing List Print"

        //Added by rabbani on 18/12/06 To print Packing List Report
        public DataSet FetchPackingListDetails(string From = "MBL", string MAWBPK = "", string MJOBPK = "", long Loc = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                strQuery.Append("select");
                strQuery.Append(" JOB.JOB_CARD_TRN_PK,");
                strQuery.Append(" MST.MASTER_JC_AIR_EXP_PK,");
                strQuery.Append(" MAWB.MAWB_EXP_TBL_PK,");
                strQuery.Append(" HAWB.HAWB_EXP_TBL_PK,");
                strQuery.Append(" MST.MASTER_JC_REF_NO,");
                strQuery.Append(" MAWB.MAWB_REF_NO,");
                strQuery.Append(" HAWB.HAWB_REF_NO,");
                strQuery.Append(" MAWBPOL.PORT_NAME POL,");
                strQuery.Append(" MAWBPOD.PORT_NAME POD,");
                strQuery.Append(" MAWBCNTPOL.COUNTRY_NAME CNTPOL,");
                strQuery.Append(" MAWBCNTPOD.COUNTRY_NAME CNTPOD,");
                strQuery.Append(" FLGAIR.VOYAGE FLIGHT,");
                strQuery.Append(" FLGAIR.ETA_DATE ETDAOO,");
                strQuery.Append(" FLGAIR.ETD_DATE ETDAOD,");
                strQuery.Append(" MAWB.AGENT_NAME MAWBAGENT_NAME,");
                strQuery.Append(" MAWB.AGENT_ADDRESS MAWBAGENT_ADDRESS,");
                strQuery.Append(" MAWB.SHIPPER_NAME MAWBSHIPPER_NAME,");
                strQuery.Append(" MAWB.SHIPPER_ADDRESS MAWBSHIPPER_ADDRESS,");
                strQuery.Append(" MAWB.GOODS_DESCRIPTION MAWBGOODS_DESCRIPTION,");
                strQuery.Append(" MAWB.MARKS_NUMBERS MAWBMARKS_NUMBERS,");
                strQuery.Append(" MAWB.TOTAL_GROSS_WEIGHT MAWBWGT,");
                strQuery.Append(" MAWB.TOTAL_VOLUME MAWBVLM,");
                strQuery.Append(" HAWBCUS.CUSTOMER_NAME HAWBSHIPPER_NAME,");
                strQuery.Append(" HAWBADD.ADM_ADDRESS_1 HAWBSHIPPER_ADD1,");
                strQuery.Append(" HAWBADD.ADM_ADDRESS_2 HAWBSHIPPER_ADD2,");
                strQuery.Append(" HAWBADD.ADM_ADDRESS_3 HAWBSHIPPER_ADD3,");
                strQuery.Append(" HAWBADD.ADM_CITY HAWBSHIPPER_CITY,");
                strQuery.Append(" HAWBADD.ADM_ZIP_CODE HAWBSHIPPER_ZIP,");
                strQuery.Append(" HAWBADD.ADM_PHONE_NO_1 HAWBSHIPPER_PHONE,");
                strQuery.Append(" HAWBADD.ADM_FAX_NO HAWBSHIPPER_FAX,");
                strQuery.Append(" HAWBADD.ADM_EMAIL_ID HAWBSHIPPER_EMAIL,");
                strQuery.Append(" HAWB.GOODS_DESCRIPTION HAWBGOODS_DESCRIPTION,");
                strQuery.Append(" HAWB.MARKS_NUMBERS HAWBMARKS_NUMBERS,");
                strQuery.Append(" HAWB.TOTAL_GROSS_WEIGHT HAWBWGT,");
                strQuery.Append(" HAWB.TOTAL_VOLUME HAWBVLM");

                strQuery.Append("  from JOB_CARD_TRN   JOB,");
                //strQuery.Append("       JOB_CARD_TRN   JOB," & vbCrLf)
                strQuery.Append("       HAWB_EXP_TBL           HAWB,");
                strQuery.Append("       MAWB_EXP_TBL           MAWB,");
                strQuery.Append("       MASTER_JC_AIR_EXP_TBL  MST,");
                strQuery.Append("       PORT_MST_TBL           MAWBPOL,");
                strQuery.Append("       PORT_MST_TBL           MAWBPOD,");
                strQuery.Append("       COUNTRY_MST_TBL        MAWBCNTPOL,");
                strQuery.Append("       COUNTRY_MST_TBL        MAWBCNTPOD,");
                strQuery.Append("       JOB_TRN_TP     FLGAIR,");
                strQuery.Append("       CUSTOMER_MST_TBL       HAWBCUS,");
                strQuery.Append("       CUSTOMER_CONTACT_DTLS  HAWBADD,");
                strQuery.Append("       LOCATION_MST_TBL       L  ");

                strQuery.Append(" WHERE");
                strQuery.Append(" JOB.HBL_HAWB_FK IS NOT NULL");
                strQuery.Append(" AND JOB.MBL_MAWB_FK IS NOT NULL");
                strQuery.Append(" AND HAWB.HAWB_EXP_TBL_PK(+) = JOB.HBL_HAWB_FK");
                strQuery.Append(" AND MAWB.MAWB_EXP_TBL_PK(+) = JOB.MBL_MAWB_FK");
                strQuery.Append(" AND MST.MASTER_JC_AIR_EXP_PK(+) = JOB.MASTER_JC_FK");
                strQuery.Append(" AND MAWBPOL.PORT_MST_PK(+) = MAWB.PORT_MST_POL_FK");
                strQuery.Append(" AND MAWBPOD.PORT_MST_PK(+) = MAWB.PORT_MST_POD_FK");
                strQuery.Append(" AND MAWBCNTPOL.COUNTRY_MST_PK(+) = MAWBPOL.COUNTRY_MST_FK");
                strQuery.Append(" AND MAWBCNTPOD.COUNTRY_MST_PK(+) = MAWBPOD.COUNTRY_MST_FK");
                strQuery.Append(" AND JOB.JOB_CARD_TRN_PK = FLGAIR.JOB_CARD_TRN_FK(+)");
                strQuery.Append(" AND HAWBCUS.CUSTOMER_MST_PK(+) = HAWB.SHIPPER_CUST_MST_FK");
                strQuery.Append(" AND HAWBCUS.CUSTOMER_MST_PK = HAWBADD.CUSTOMER_MST_FK(+)");
                strQuery.Append(" AND L.LOCATION_MST_PK=" + Loc);

                if (From == "MAWB")
                {
                    strQuery.Append(" AND JOB.MBL_MAWB_FK IN (" + MAWBPK + ")");
                }
                else
                {
                    strQuery.Append(" AND JOB.MASTER_JC_FK =" + MJOBPK);
                }

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

        //Ended by rabbani on 18/12/06

        #endregion "Packing List Print"

        #region "HAWB Count"

        //Added by rabbani on 19/12/06 To get the count of HAWBS against Selected MAWB or MSJobCard
        public int HAWBCount(string MAWBPK = "", string MSJobPK = "", string @from = "MBL")
        {
            try
            {
                string strSQL = null;
                Int32 totRecords = 0;
                WorkFlow objTotRecCount = new WorkFlow();
                strSQL = "SELECT COUNT(JOB.JOB_CARD_TRN_PK) FROM JOB_CARD_TRN JOB";
                strSQL += "WHERE JOB.HBL_HAWB_FK IS NOT NULL";
                strSQL += "AND JOB.MBL_MAWB_FK IS NOT NULL";
                //If MAWBPK <> "" Or MSJobPK <> "" Then
                if (@from == "MAWB")
                {
                    strSQL += "AND JOB.MBL_MAWB_FK =" + MAWBPK;
                }
                else
                {
                    strSQL += "AND JOB.MASTER_JC_FK =" + MSJobPK;
                }
                //End If
                totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSQL.ToString()));
                return totRecords;
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

        //Ended by rabbani on 19/12/06

        #endregion "HAWB Count"

        #region "fetch MaWB Nr"

        public DataSet Fetch_MAwbNr(string BkgNr, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("   select abn.airway_bill_no");
                strQuery.Append("     from airway_bill_trn abn, airway_bill_mst_tbl am");
                strQuery.Append("    where abn.reference_no = '" + BkgNr + "'");
                //add by latha
                strQuery.Append(" and am.location_mst_fk =" + usrLocFK);
                strQuery.Append("");

                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "fetch MaWB Nr"

        #region "Update Airway Bill Trn"

        public ArrayList Update_Airway_Bill_Trn(string txtrefno, string AirwayBillNo, string AirwayPk, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            Int16 exe = default(Int16);
            OracleCommand cmd = new OracleCommand();
            System.Text.StringBuilder strQuery = null;
            arrMessage.Clear();
            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.Connection = TRAN.Connection;
                cmd.Transaction = TRAN;
                //add by latha for updating  the mst table for cancelled records

                cmd.Parameters.Clear();

                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_mst_tbl AMT ");
                strQuery.Append("   set AMT.total_nos_used = (AMT.total_nos_used - 1),  ");
                strQuery.Append("   AMT.total_nos_cancelled = (AMT.total_nos_cancelled + 1)  ");
                strQuery.Append(" Where AMT.Airway_Bill_Mst_Pk in (select abt.airway_bill_mst_fk  from airway_bill_trn ABT Where ABT.REFERENCE_NO = '" + txtrefno + "') ");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();
               
                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();
                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_trn ABT ");
                strQuery.Append(" set ABT.Status = 0, ABT.Used_At = 0, ABT.Reference_No = Null ");
                strQuery.Append(" Where ABT.REFERENCE_NO = '" + txtrefno + "'");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                //cmd.Parameters.Clear()
                //strQuery = New Text.StringBuilder
                //strQuery.Append(" update airway_bill_trn ABT " & vbCrLf)
                //strQuery.Append(" set ABT.Status = 0, ABT.Used_At = 0, ABT.Reference_No = Null " & vbCrLf)
                //strQuery.Append(" Where ABT.REFERENCE_NO = '" & jobcardNo & "'" & vbCrLf)
                //strQuery.Append("" & vbCrLf)
                //cmd.CommandText = strQuery.ToString

                //exe = cmd.ExecuteNonQuery()
                if (AirwayPk != null)
                {
                    cmd.Parameters.Clear();

                    strQuery = new System.Text.StringBuilder();
                    strQuery.Append(" update airway_bill_trn ABT ");
                    strQuery.Append("   set ABT.Status       = 3, ");
                    strQuery.Append("       ABT.Used_At      = 4, ");
                    strQuery.Append("       ABT.Reference_No = '" + txtrefno + "'");
                    strQuery.Append(" Where ABT.Airway_Bill_Mst_Fk = " + AirwayPk);
                    strQuery.Append("   And ABT.AIRWAY_BILL_NO = '" + AirwayBillNo + "'");
                    strQuery.Append("");
                    cmd.CommandText = strQuery.ToString();

                    exe = Convert.ToInt16(cmd.ExecuteNonQuery());
                    //add by latha for updating the totalno used in the mst table
                    cmd.Parameters.Clear();

                    strQuery = new System.Text.StringBuilder();
                    strQuery.Append(" update airway_bill_mst_tbl AMT ");
                    strQuery.Append(" set AMT.total_nos_used = ( select count(*) + 1 from airway_bill_trn trn ");
                    strQuery.Append(" where(trn.reference_no Is Not null) and trn.airway_bill_mst_fk= " + AirwayPk);
                    strQuery.Append(") Where AMT.Airway_Bill_Mst_Pk = " + AirwayPk);
                    strQuery.Append("");
                    cmd.CommandText = strQuery.ToString();

                    exe = Convert.ToInt16(cmd.ExecuteNonQuery());
                }

                //    arrMessage.Add("All data saved successfully")
                //    Return arrMessage

                //Catch oraexp As OracleException
                //    arrMessage.Add(oraexp.Message)
                //    Return arrMessage
                //Catch ex As Exception
                //    arrMessage.Add(ex.Message)
                //    Return arrMessage
                //End Try
                if (exe == 1)
                {
                    arrMessage.Add("All Data Saved Successfully");
                }
                else
                {
                    arrMessage.Add("Upstream updation failed, Check MAWB Int32");
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

        #endregion "Update Airway Bill Trn"

        #region "Fetch Aiwaybill MST Fk "

        public DataSet fecth_Airway_mst_Fk(string ref_nr, string Air_Pk, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("select a.airway_bill_mst_fk");
                strQuery.Append("from airway_bill_trn a, airway_bill_mst_tbl am");
                strQuery.Append("where am.airway_bill_mst_pk = a.airway_bill_mst_fk");
                //add by latha
                strQuery.Append("and am.location_mst_fk =" + usrLocFK);
                strQuery.Append("and am.airline_mst_fk=" + Air_Pk);
                strQuery.Append("and a.airway_bill_no='" + ref_nr + "'");
                strQuery.Append("");

                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "Fetch Aiwaybill MST Fk "

        #region "Fetch MAWB Pk  "

        public DataSet fecth_Mawbpk(string mawbrefno)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("SELECT MAWB.MAWB_EXP_TBL_PK,mawb.version_no FROM MAWB_EXP_TBL MAWB");
                //strQuery.Append("WHERE MAWB.MAWB_REF_NO =" & vbCrLf)
                //strQuery.Append("where am.airway_bill_mst_pk = a.airway_bill_mst_fk" & vbCrLf)
                //strQuery.Append("and am.airline_mst_fk=" & Air_Pk & vbCrLf)
                strQuery.Append("WHERE MAWB.MAWB_REF_NO =" + mawbrefno);
                //strQuery.Append("" & vbCrLf)
                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "Fetch MAWB Pk  "

        #region "Update Container"

        //The Following Code Added By ANand to reflect the container number to other Master Job Cards
        public string UpdateContainer(Int32 MstPk, string Palette)
        {
            try
            {
                string sqlStr = "";
                string strReturn = "";

                WorkFlow objWF = new WorkFlow();

                sqlStr = "update JOB_TRN_CONT f set f.palette_size = '" + Palette + "'" + "where f.JOB_CARD_TRN_FK in (select cnt.JOB_CARD_TRN_PK from JOB_CARD_TRN cnt" + "where cnt.MASTER_JC_FK = '" + MstPk + "')";
                strReturn = objWF.ExecuteScaler(sqlStr);
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

        #endregion "Update Container"

        #region "MAWB Standard Print Main"

        public object MAWB_MainPrint(string MAWBPk = "0", int LocPk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT M.MAWB_EXP_TBL_PK,");
                sb.Append("                M.MAWB_MADE_FROM,");
                sb.Append("                M.MAWB_REF_NO,");
                sb.Append("                M.MAWB_DATE,");
                sb.Append("                (SELECT BKG.BOOKING_REF_NO");
                sb.Append("                   FROM JOB_CARD_TRN JOB, BOOKING_MST_TBL BKG");
                sb.Append("                  WHERE JOB.MBL_MAWB_FK = " + MAWBPk);
                sb.Append("                    AND BKG.BOOKING_MST_PK(+) = JOB.BOOKING_MST_FK");
                sb.Append("                    AND ROWNUM <= 1) BOOKING_REF_NO,");
                sb.Append("                A.AIRLINE_NAME AIRLINE_ID,");
                sb.Append("                M.PORT_MST_POL_FK,");
                sb.Append("                M.PORT_MST_POD_FK,");
                sb.Append("                POL.PORT_ID POL,");
                sb.Append("                POD.PORT_ID POD,");
                sb.Append("                COM.COMMODITY_GROUP_CODE,");
                sb.Append("                M.SHIPPER_NAME,");
                sb.Append("                M.SHIPPER_ADDRESS,");
                sb.Append("                M.CONSIGNEE_NAME,");
                sb.Append("                M.CONSIGNEE_ADDRESS,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                M.AGENT_NAME,");
                sb.Append("                M.AGENT_ADDRESS,");
                sb.Append("                SH.INCO_CODE,");
                sb.Append("                DECODE(M.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PYMT_TYPE,");
                sb.Append("                MOV.CARGO_MOVE_CODE MOVECODE,");
                sb.Append("                M.COLLECT_CHARGES,");
                sb.Append("                M.PREPAID_CHARGES,");
                sb.Append("                M.INSURANCE_AMT,");
                sb.Append("                M.DECL_VAL_FOR_CARRIAGE,");
                sb.Append("                M.DECL_VAL_FOR_CUSTOMS,");
                sb.Append("                (SELECT ROWTOCOL('SELECT J.JOBCARD_REF_NO FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK='||" + MAWBPk + ")");
                sb.Append("                   FROM DUAL) JOB_REF,");
                sb.Append("                (SELECT JOB1.JOB_CARD_TRN_PK");
                sb.Append("                   FROM JOB_CARD_TRN JOB1");
                sb.Append("                  WHERE JOB1.MBL_MAWB_FK = " + MAWBPk);
                sb.Append("                    AND ROWNUM <= 1) JOB_REF_PK,");
                sb.Append("                (SELECT BKG.CARGO_TYPE");
                sb.Append("                   FROM JOB_CARD_TRN JOB, BOOKING_MST_TBL BKG");
                sb.Append("                  WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                sb.Append("                    AND JOB.MBL_MAWB_FK = " + MAWBPk);
                sb.Append("                    AND ROWNUM <= 1) CARGO_TYPE,");
                sb.Append("                JOB.VOYAGE_FLIGHT_NO CARGO_TYPE,");
                sb.Append("                DELPL.PLACE_NAME AS DELPL,");
                sb.Append("                COLPL.PLACE_NAME AS COLPL,");
                sb.Append("                M.MARKS_NUMBERS,");
                sb.Append("                M.GOODS_DESCRIPTION,");
                sb.Append("                MT.PALETTE_SIZE,");
                sb.Append("                PK.PACK_TYPE_DESC PACK_TYPE_ID,");
                sb.Append("                MT.PACK_COUNT,");
                sb.Append("                MT.GROSS_WEIGHT,");
                sb.Append("                MT.CHARGEABLE_WEIGHT,");
                sb.Append("                MT.VOLUME_IN_CBM,");
                sb.Append("                CM.COMMODITY_ID,");
                sb.Append("                M.ULD_NUMBER,");
                sb.Append("                M.AIRFREIGHT_SLABS_TBL_FK,");
                sb.Append("                AST.BREAKPOINT_ID,");
                sb.Append("                SHP.CUST_REG_NO,");
                sb.Append("                CNS.CUST_REG_NO,");
                sb.Append("                (SELECT AMT.IATA_CODE");
                sb.Append("                   FROM AGENT_MST_TBL AMT");
                sb.Append("                  WHERE AMT.LOCATION_MST_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("                    AND ROWNUM = 1) AGENT_IATA_CODE,");
                sb.Append("                (SELECT AMT.ACCOUNT_NO");
                sb.Append("                   FROM AGENT_MST_TBL AMT");
                sb.Append("                  WHERE AMT.LOCATION_MST_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("                    AND ROWNUM = 1) AGENT_ACC_NR,");
                sb.Append("                (CCD.ADM_ADDRESS_1 || CHR(13) || CCD.ADM_ADDRESS_2 ||");
                sb.Append("                CHR(13) || CCD.ADM_ADDRESS_3) NOTIFY_ADDRESS,");
                sb.Append("                CTMT.CURRENCY_ID,");
                sb.Append("                (SELECT SUM(JTAEF.FREIGHT_AMT *");
                sb.Append("                                GET_EX_RATE(JTAEF.CURRENCY_MST_FK,");
                sb.Append("                                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                            JOB.JOBCARD_DATE))");
                sb.Append("                   FROM JOB_TRN_FD JTAEF");
                sb.Append("                  WHERE JTAEF.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + ")");
                sb.Append("                    AND JTAEF.FREIGHT_TYPE = 1) PREPAID_FREIGHT,");
                sb.Append("                (SELECT SUM(JTAEF.FREIGHT_AMT *");
                sb.Append("                                GET_EX_RATE(JTAEF.CURRENCY_MST_FK,");
                sb.Append("                                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                            JOB.JOBCARD_DATE))");
                sb.Append("                   FROM JOB_TRN_FD JTAEF");
                sb.Append("                  WHERE JTAEF.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + ")");
                sb.Append("                    AND JTAEF.FREIGHT_TYPE = 2) COLLECT_FREIGHT,");
                sb.Append("                (SELECT SUM(JTAEOC.AMOUNT *");
                sb.Append("                                GET_EX_RATE(JTAEOC.CURRENCY_MST_FK,");
                sb.Append("                                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                            JOB.JOBCARD_DATE))");
                sb.Append("                   FROM JOB_TRN_OTH_CHRG JTAEOC");
                sb.Append("                  WHERE JTAEOC.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + ")");
                sb.Append("                    AND JTAEOC.FREIGHT_TYPE = 1) PREPAID_OTH,");
                sb.Append("                (SELECT SUM(JTAEOC.AMOUNT *");
                sb.Append("                                GET_EX_RATE(JTAEOC.CURRENCY_MST_FK,");
                sb.Append("                                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                            JOB.JOBCARD_DATE))");
                sb.Append("                   FROM JOB_TRN_OTH_CHRG JTAEOC");
                sb.Append("                  WHERE JTAEOC.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + ")");
                sb.Append("                    AND JTAEOC.FREIGHT_TYPE = 2) COLLECT_OTH,");
                sb.Append("                CASE");
                sb.Append("                  WHEN JOB.DEPARTURE_DATE IS NULL THEN");
                sb.Append("                   JOB.ETD_DATE");
                sb.Append("                  ELSE");
                sb.Append("                   JOB.DEPARTURE_DATE");
                sb.Append("                END ATD,");
                sb.Append("                CASE");
                sb.Append("                  WHEN AST.BREAKPOINT_ID IS NULL THEN");
                sb.Append("                   'M'");
                sb.Append("                  WHEN INSTR(AST.BREAKPOINT_ID, '<') > 0 THEN");
                sb.Append("                   'N'");
                sb.Append("                  ELSE");
                sb.Append("                   'Q'");
                sb.Append("                END RATE_CLASS,");
                //sb.Append("                ROWTOCOL('SELECT CMT.COMMODITY_ID FROM COMMODITY_MST_TBL CMT WHERE CMT.COMMODITY_MST_PK IN  ")
                //sb.Append("                (SELECT JTAEC.COMMODITY_MST_FKS")
                //sb.Append("                   FROM JOB_TRN_CONT JTAEC")
                //sb.Append("                  WHERE JTAEC.JOB_CARD_AIR_EXP_FK ='||JOB_CARD_TRN_PK||')') COMMODITY_CODE,")
                sb.Append("  (SELECT ROWTOCOL('SELECT CMT.COMMODITY_ID FROM COMMODITY_MST_TBL CMT WHERE CMT.COMMODITY_MST_PK IN (' ||");
                sb.Append("                                 NVL(JT.COMMODITY_MST_FKS, 0) || ')') COMMODITY_CODE");
                sb.Append("                   FROM JOB_TRN_CONT JT");
                sb.Append("                  WHERE JOB.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK");
                sb.Append("                    AND JT.COMMODITY_MST_FKS IS NOT NULL AND ROWNUM=1) COMMODITY_CODE,");
                sb.Append("                   (SELECT SUM(JTAEF.FREIGHT_AMT *");
                sb.Append("                                GET_EX_RATE(JTAEF.CURRENCY_MST_FK,");
                sb.Append("                                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                            JOB.JOBCARD_DATE))");
                sb.Append("                   FROM JOB_TRN_FD JTAEF, FREIGHT_ELEMENT_MST_TBL FEMT");
                sb.Append("                  WHERE JTAEF.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + ")");
                sb.Append("                    AND FEMT.FREIGHT_ELEMENT_MST_PK = JTAEF.FREIGHT_ELEMENT_MST_FK");
                sb.Append("                    AND JTAEF.FREIGHT_TYPE = 1");
                sb.Append("                    AND FEMT.CHARGE_TYPE=1");
                sb.Append("                     ) FREIGHT_PREPAID_FREIGHT,");
                sb.Append("                  (SELECT SUM(JTAEF.FREIGHT_AMT *");
                sb.Append("                                GET_EX_RATE(JTAEF.CURRENCY_MST_FK,");
                sb.Append("                                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                            JOB.JOBCARD_DATE))");
                sb.Append("                   FROM JOB_TRN_FD JTAEF, FREIGHT_ELEMENT_MST_TBL FEMT");
                sb.Append("                  WHERE JTAEF.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + ")");
                sb.Append("                    AND FEMT.FREIGHT_ELEMENT_MST_PK = JTAEF.FREIGHT_ELEMENT_MST_FK");
                sb.Append("                    AND JTAEF.FREIGHT_TYPE = 1");
                sb.Append("                    AND FEMT.CHARGE_TYPE=2");
                sb.Append("                     ) SURCHARGE_PREPAID_FREIGHT,");
                sb.Append("                 (SELECT SUM(JTAEF.FREIGHT_AMT *");
                sb.Append("                                GET_EX_RATE(JTAEF.CURRENCY_MST_FK,");
                sb.Append("                                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                            JOB.JOBCARD_DATE))");
                sb.Append("                   FROM JOB_TRN_FD JTAEF, FREIGHT_ELEMENT_MST_TBL FEMT");
                sb.Append("                  WHERE JTAEF.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + ")");
                sb.Append("                    AND FEMT.FREIGHT_ELEMENT_MST_PK = JTAEF.FREIGHT_ELEMENT_MST_FK");
                sb.Append("                    AND JTAEF.FREIGHT_TYPE = 2");
                sb.Append("                    AND FEMT.CHARGE_TYPE=1");
                sb.Append("                     ) FREIGHT_COLLECT_FREIGHT,");
                sb.Append("                     ");
                sb.Append("                  (SELECT SUM(JTAEF.FREIGHT_AMT *");
                sb.Append("                                GET_EX_RATE(JTAEF.CURRENCY_MST_FK,");
                sb.Append("                                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                            JOB.JOBCARD_DATE))");
                sb.Append("                   FROM JOB_TRN_FD JTAEF, FREIGHT_ELEMENT_MST_TBL FEMT");
                sb.Append("                  WHERE JTAEF.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + ")");
                sb.Append("                    AND FEMT.FREIGHT_ELEMENT_MST_PK = JTAEF.FREIGHT_ELEMENT_MST_FK");
                sb.Append("                    AND JTAEF.FREIGHT_TYPE = 2");
                sb.Append("                    AND FEMT.CHARGE_TYPE=2");
                sb.Append("                     ) SURCHARGE_COLLECT_FREIGHT,");
                sb.Append("                  '' OTH_AGENT_PREPAID,");
                sb.Append("                  '' OTH_AGENT_COLLECT,");
                sb.Append("  (SELECT SUM(COST.TOTAL_COST * ");
                sb.Append("                  GET_EX_RATE(COST.CURRENCY_MST_FK,");
                sb.Append("                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                            JOB.JOBCARD_DATE))");
                sb.Append("    FROM JOB_TRN_COST COST");
                sb.Append("  WHERE(COST.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + "))");
                sb.Append("     AND COST.PTMT_TYPE = 1) OTH_CARRIER_PREPAID,");
                sb.Append("  (SELECT SUM(COST.TOTAL_COST * ");
                sb.Append("                  GET_EX_RATE(COST.CURRENCY_MST_FK,");
                sb.Append("                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                            JOB.JOBCARD_DATE))");
                sb.Append("    FROM JOB_TRN_COST COST");
                sb.Append("  WHERE(COST.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK FROM JOB_CARD_TRN J WHERE J.MBL_MAWB_FK=" + MAWBPk + "))");
                sb.Append("     AND COST.PTMT_TYPE = 2) OTH_CARRIER_COLLECT,");
                sb.Append("                  A.AIRLINE_ID TOTAL_PREPAID,");
                sb.Append("                  '' TOTAL_COLLECT,");
                sb.Append("   (SELECT NVL(FD.RATEPERBASIS, 0)");
                sb.Append("                   FROM JOB_TRN_FD FD, FREIGHT_ELEMENT_MST_TBL FEMT");
                sb.Append("                  WHERE (FD.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK)");
                sb.Append("                    AND FEMT.FREIGHT_ELEMENT_MST_PK =");
                sb.Append("                        FD.FREIGHT_ELEMENT_MST_FK");
                sb.Append("                    AND FEMT.FREIGHT_ELEMENT_ID = 'AFC') RATE_CHARGE,");
                sb.Append("                ");
                sb.Append("                ROWTOCOL('(SELECT FRT.FREIGHT_ELEMENT_ID ||'' '' || ROUND(NVL(OTH.AMOUNT,0),2)");
                sb.Append("                   FROM FREIGHT_ELEMENT_MST_TBL  FRT,");
                sb.Append("                        JOB_TRN_OTH_CHRG OTH");
                sb.Append("                  WHERE FRT.FREIGHT_ELEMENT_MST_PK = OTH.FREIGHT_ELEMENT_MST_FK");
                sb.Append("                    AND OTH.JOB_CARD_TRN_FK(+) = ' ||");
                sb.Append("                         JOB.JOB_CARD_TRN_PK || ')') FREIGHT_ELEMENT_ID,");
                sb.Append("                (SELECT MIN(PMT.PORT_ID)");
                sb.Append("                   FROM JOB_TRN_TP JTP, PORT_MST_TBL PMT");
                sb.Append("                  WHERE JTP.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("                    AND JTP.PORT_MST_FK = PMT.PORT_MST_PK) FIRST_TO,");
                sb.Append("                (SELECT MIN(A.AIRLINE_ID)");
                sb.Append("                   FROM JOB_TRN_TP JTP, AIRLINE_MST_TBL A");
                sb.Append("                  WHERE JTP.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("                    AND JTP.VOYAGE_AIRLINE_TRN_FK = A.AIRLINE_MST_PK) FIRST_BY,");
                sb.Append("                ");
                sb.Append("                (SELECT MAX(PMT.PORT_ID)");
                sb.Append("                   FROM JOB_TRN_TP TP, PORT_MST_TBL PMT");
                sb.Append("                  WHERE TP.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("                    AND TP.PORT_MST_FK = PMT.PORT_MST_PK) SECOND_TO,");
                sb.Append("                ");
                sb.Append("                (SELECT MAX(A.AIRLINE_ID)");
                sb.Append("                   FROM JOB_TRN_TP JTP, AIRLINE_MST_TBL A");
                sb.Append("                  WHERE JTP.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("                    AND JTP.VOYAGE_AIRLINE_TRN_FK = A.AIRLINE_MST_PK) SECOND_BY");

                sb.Append("  FROM MAWB_EXP_TBL            M,");
                sb.Append("       MAWB_TRN_EXP_CONTAINER  MT,");
                sb.Append("       JOB_CARD_TRN    JOB,");
                sb.Append("       BOOKING_MST_TBL         BKG,");
                sb.Append("       PLACE_MST_TBL           DELPL,");
                sb.Append("       PLACE_MST_TBL           COLPL,");
                sb.Append("       AIRLINE_MST_TBL         A,");
                sb.Append("       PACK_TYPE_MST_TBL       PK,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CARGO_MOVE_MST_TBL      MOV,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COM,");
                sb.Append("       COMMODITY_MST_TBL       CM,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS   CCD,");
                sb.Append("       CUSTOMER_MST_TBL        SHP,");
                sb.Append("       CUSTOMER_MST_TBL        CNS,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       COUNTRY_MST_TBL         COUN,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT,");
                sb.Append("       SHIPPING_TERMS_MST_TBL  SH,");
                sb.Append("       AIRFREIGHT_SLABS_TBL    AST");
                sb.Append(" WHERE A.AIRLINE_MST_PK(+) = M.AIRLINE_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK(+) = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK(+) = M.PORT_MST_POD_FK");
                sb.Append("   AND MOV.CARGO_MOVE_PK(+) = M.CARGO_MOVE_FK");
                sb.Append("   AND COM.COMMODITY_GROUP_PK(+) = M.COMMODITY_GROUP_FK");
                sb.Append("   AND SH.SHIPPING_TERMS_MST_PK(+) = M.SHIPPING_TERMS_MST_FK");
                sb.Append("   AND JOB.MBL_MAWB_FK(+) = M.MAWB_EXP_TBL_PK");
                sb.Append("   AND BKG.BOOKING_MST_PK(+) = JOB.BOOKING_MST_FK");
                sb.Append("   AND JOB.NOTIFY1_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");

                sb.Append("   AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) ");
                sb.Append("   AND SHP.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK ");
                sb.Append("   AND CNS.CUSTOMER_MST_PK(+) = JOB.CONSIGNEE_CUST_MST_FK ");
                sb.Append("   AND POL.COUNTRY_MST_FK = COUN.COUNTRY_MST_PK");
                sb.Append("   AND COUN.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");

                sb.Append("   AND DELPL.PLACE_PK(+) = BKG.DEL_PLACE_MST_FK");
                sb.Append("   AND COLPL.PLACE_PK(+) = BKG.COL_PLACE_MST_FK");
                sb.Append("   AND PK.PACK_TYPE_MST_PK(+) = MT.PACK_TYPE_MST_FK");
                sb.Append("   AND MT.MAWB_EXP_TBL_FK = M.MAWB_EXP_TBL_PK");
                sb.Append("   AND CM.COMMODITY_MST_PK(+) = MT.COMMODITY_MST_FK");
                sb.Append("   AND M.AIRFREIGHT_SLABS_TBL_FK = AST.AIRFREIGHT_SLABS_TBL_PK(+)");
                sb.Append("   AND M.MAWB_EXP_TBL_PK =" + MAWBPk);
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion "MAWB Standard Print Main"

        #region "MAWB Standard Print Freight"

        public object MAWB_FREIGHT(string MAWBPk = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("  SELECT FEMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       FEMT.PREFERENCE,");
            sb.Append("       CASE WHEN JFD.FREIGHT_TYPE=1 THEN");
            sb.Append("       SUM(JFD.FREIGHT_AMT) ");
            sb.Append("       END PREPAID,");
            sb.Append("       CASE WHEN JFD.FREIGHT_TYPE=2 THEN");
            sb.Append("       SUM(JFD.FREIGHT_AMT) ");
            sb.Append("       END COLLECT");
            sb.Append("  FROM MAWB_EXP_TBL H,");
            sb.Append("       JOB_CARD_TRN J,");
            sb.Append("       JOB_TRN_FD JFD,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT");
            sb.Append("  WHERE J.MBL_MAWB_FK = H.MAWB_EXP_TBL_PK");
            sb.Append("   AND J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK");
            sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK = JFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = JFD.CURRENCY_MST_FK");
            sb.Append("   AND H.MAWB_EXP_TBL_PK = " + MAWBPk);
            sb.Append("  GROUP BY FEMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       JFD.FREIGHT_TYPE,");
            sb.Append("       FEMT.PREFERENCE");
            sb.Append(" ORDER BY FEMT.PREFERENCE");

            try
            {
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion "MAWB Standard Print Freight"

        #region "MAWB PALETTE"

        public object MAWB_PALETTE(string MAWBPk = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT MT.PALETTE_SIZE");
            sb.Append("     FROM MAWB_EXP_TBL M, ");
            sb.Append("      MAWB_TRN_EXP_CONTAINER MT");
            sb.Append("  WHERE MT.MAWB_EXP_TBL_FK = M.MAWB_EXP_TBL_PK");
            sb.Append(" AND M.MAWB_EXP_TBL_PK =" + MAWBPk);
            try
            {
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion "MAWB PALETTE"

        #region "GET AGENT DETAILS"

        public object GetDpAgentFK(string MAWBPk = "0", int BizType = 2)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (BizType == 1)
            {
                sb.Append("SELECT DISTINCT nvl(JC.DP_AGENT_MST_FK,0)DP_AGENT_MST_FK,JC.CARRIER_MST_FK, AMT.AIRLINE_NAME,NVL(VMT.VENDOR_MST_PK,0) VENDOR_MST_PK ");
                sb.Append("     FROM JOB_CARD_TRN  JC , AIRLINE_MST_TBL AMT,VENDOR_MST_TBL VMT ");
                sb.Append("  WHERE JC.MBL_MAWB_FK = " + MAWBPk);
                sb.Append(" AND JC.BUSINESS_TYPE=" + BizType);
                sb.Append("   AND JC.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append(" AND AMT.AIRLINE_ID = VMT.VENDOR_ID(+)");
            }
            else
            {
                sb.Append("SELECT DISTINCT nvl(JC.DP_AGENT_MST_FK,0)DP_AGENT_MST_FK , JC.CARRIER_MST_FK, OPR.OPERATOR_NAME ,NVL(VMT.VENDOR_MST_PK,0) VENDOR_MST_PK ");
                sb.Append("     FROM JOB_CARD_TRN  JC , OPERATOR_MST_TBL OPR,VENDOR_MST_TBL VMT ");
                sb.Append("  WHERE JC.MBL_MAWB_FK = " + MAWBPk);
                sb.Append(" AND JC.BUSINESS_TYPE=" + BizType);
                sb.Append("  AND JC.CARRIER_MST_FK = OPR.OPERATOR_MST_PK ");
                sb.Append(" AND OPR.OPERATOR_ID = VMT.VENDOR_ID(+)");
            }

            try
            {
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion "GET AGENT DETAILS"
    }
}