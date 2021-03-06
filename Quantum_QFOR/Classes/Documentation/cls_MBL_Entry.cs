﻿#region "Comments"

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
using System.Text;

namespace Quantum_QFOR
{
    public class cls_MBL_Entry : CommonFeatures
    {
        private WorkFlow objWF = new WorkFlow();
        private static DataSet M_ShipDataSet = new DataSet();
        private static DataSet M_FreightTermsDataset = new DataSet();
        private static DataSet M_MoveCodeDataset = new DataSet();
        private static DataSet M_LinerTermsDS = new DataSet();
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        #region "Properties"

        public static DataSet ShipDataSet
        {
            get { return M_ShipDataSet; }
        }

        public static DataSet FreightDataSet
        {
            get { return M_FreightTermsDataset; }
        }

        public static DataSet MoveCodeDataSet
        {
            get { return M_MoveCodeDataset; }
        }

        //Public Shared ReadOnly Property LinerTermsDS() As DataSet
        //    Get
        //        Return M_LinerTermsDS
        //    End Get
        //End Property

        #endregion "Properties"

        public DataSet FetchCommodityFks(int IntMasterJobpk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append("  SELECT JCNT.COMMODITY_MST_FKS, JCNT.COMMODITY_MST_FK,JC.JOBCARD_REF_NO,JCNT.CONTAINER_TYPE_MST_FK");
            sb.Append("  FROM JOB_CARD_TRN JC, JOB_TRN_CONT JCNT");
            sb.Append("  WHERE JC.JOB_CARD_TRN_PK = JCNT.JOB_CARD_TRN_FK");
            if (IntMasterJobpk > 0)
            {
                sb.Append("   AND JC.MASTER_JC_FK = " + IntMasterJobpk + "");
            }
            else
            {
                sb.Append("   AND 1=2");
            }
            try
            {
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

        #region "Constructor"

        public cls_MBL_Entry()
        {
            string strShipSQL = null;
            string strFreightSQL = null;
            string strMoveCodeSQL = null;
            strShipSQL = "SELECT 0 SHIPPING_TERMS_MST_PK, ' ' INCO_CODE  FROM  DUAL UNION ";
            strShipSQL += " SELECT SHIPPING_TERMS_MST_PK,INCO_CODE  FROM SHIPPING_TERMS_MST_TBL WHERE ACTIVE_FLAG = 1 ORDER BY INCO_CODE ";
            //strShipSQL &= " SELECT SHIPPING_TERMS_MST_PK,INCO_CODE  FROM SHIPPING_TERMS_MST_TBL "
            strFreightSQL = "SELECT FREIGHT_TERMS_MST_PK,FRIEGHT_TERMS  FROM FREIGHT_TERMS_MST_TBL ";
            strMoveCodeSQL = "SELECT 0 CARGO_MOVE_PK,'' CARGO_MOVE_CODE FROM DUAL UNION ";
            strMoveCodeSQL += "SELECT CARGO_MOVE_PK,CARGO_MOVE_CODE FROM CARGO_MOVE_MST_TBL WHERE ACTIVE_FLAG = 1";
            //strMoveCodeSQL &= "SELECT CARGO_MOVE_PK,CARGO_MOVE_CODE FROM CARGO_MOVE_MST_TBL "
            try
            {
                M_ShipDataSet = (new WorkFlow()).GetDataSet(strShipSQL);
                M_FreightTermsDataset = (new WorkFlow()).GetDataSet(strFreightSQL);
                M_MoveCodeDataset = (new WorkFlow()).GetDataSet(strMoveCodeSQL);
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

        #endregion "Constructor"

        #region " Fetch Freight Type"

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

        #endregion " Fetch Freight Type"

        #region " Fetch If From Master Job Card"

        public DataSet FetchMJC(Int64 MBLPK)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            try
            {
                sb.Append("SELECT MJ.MASTER_JC_SEA_EXP_PK, MJ.MASTER_JC_REF_NO, MJ.CARGO_TYPE FROM MBL_EXP_TBL M, MASTER_JC_SEA_EXP_TBL MJ");
                sb.Append(" WHERE M.MBL_EXP_TBL_PK = MJ.MBL_FK");
                sb.Append(" AND M.MBL_EXP_TBL_PK =" + MBLPK);

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

        #endregion " Fetch If From Master Job Card"

        #region " Fetch Temp Customer" 'Manoharan 19Feb07: to fetch Temp customer in HBL / JC

        public DataSet fetchTempCust(string RefNr, Int16 Type)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //from HBL
                if (Type == 1)
                {
                    strSQL.Append("select h.shipper_cust_mst_fk from ");
                    strSQL.Append(" HBL_EXP_TBL h where h.hbl_ref_no='" + RefNr + "'");
                    //from JC
                }
                else if (Type == 2)
                {
                    strSQL.Append("select j.shipper_cust_mst_fk from ");
                    strSQL.Append(" JOB_CARD_TRN j where j.jobcard_ref_no='" + RefNr + "'");
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

        #endregion " Fetch Temp Customer" 'Manoharan 19Feb07: to fetch Temp customer in HBL / JC

        #region " Fetch From Consolidation"

        //Filling Controls on Consolidation
        public DataSet FetchByConsolidation(string HBLs)
        {
            StringBuilder strSQl = new StringBuilder();
            strSQl.Append(" SELECT HBL.HBL_EXP_TBL_PK,  ");
            strSQl.Append(" HBL.HBL_REF_NO,");
            strSQl.Append(" HBL.JOB_CARD_TRN_PK,");
            strSQl.Append(" HBL.MARKS_NUMBERS,");
            strSQl.Append(" HBL.GOODS_DESCRIPTION,");
            strSQl.Append(" BOOK.DEL_PLACE_MST_FK,");
            strSQl.Append(" PL.PLACE_NAME PLACE_code ");
            strSQl.Append(" FROM HBL_EXP_TBL HBL,");
            strSQl.Append(" JOB_CARD_TRN JOB,");
            strSQl.Append(" BOOKING_MST_TBL BOOK,");
            strSQl.Append(" PLACE_MST_TBL PL");
            strSQl.Append(" WHERE HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK ");
            strSQl.Append(" AND JOB.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
            strSQl.Append(" AND BOOK.DEL_PLACE_MST_FK = PL.PLACE_PK(+)");
            strSQl.Append(" AND HBL.HBL_EXP_TBL_PK IN (" + HBLs + ")");
            try
            {
                return objWF.GetDataSet(strSQl.ToString());
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

        #endregion " Fetch From Consolidation"

        #region " Fetch for Filling the HBL Listing Control"

        // To Fill the List Controls
        public object FillHBLListControl(int mblPk, int mblMadeFrom)
        {
            StringBuilder strSQl = new StringBuilder();
            if (mblMadeFrom == 3)
            {
                strSQl.Append("SELECT HBL.HBL_REF_NO from HBL_EXP_TBL HBL where HBL.mbl_exp_tbl_fk = " + mblPk + "");
            }
            if (mblMadeFrom == 5)
            {
                strSQl.Append("select H.HBL_REF_NO from HBL_EXP_TBL H,JOB_CARD_TRN j where J.JOB_CARD_TRN_PK = H.JOB_CARD_SEA_EXP_FK AND j.MBL_MAWB_FK =" + mblPk + "");
            }
            try
            {
                return objWF.GetDataSet(strSQl.ToString());
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

        #endregion " Fetch for Filling the HBL Listing Control"

        #region " Fetch All On New"

        //Fetch Data From Upstream Tables to fill the MBL Screen Controls On New mode
        //On Basis of HBL or Job Card
        public DataSet fetchAll(string strPk = "0", string TYPE = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" select distinct ");
                if (TYPE == "0")
                {
                    strSQL.Append(" H.HBL_EXP_TBL_PK, ");
                    strSQL.Append(" H.JOB_CARD_SEA_EXP_FK, ");
                }
                else
                {
                    strSQL.Append(" J.JOB_CARD_TRN_PK, ");
                    strSQL.Append(" J.JOBCARD_REF_NO, ");
                }
                strSQL.Append(" H.HBL_REF_NO, ");
                strSQL.Append(" B.CARRIER_MST_FK, ");
                strSQL.Append(" O.OPERATOR_NAME,");
                strSQL.Append(" B.PORT_MST_POL_FK, ");
                strSQL.Append(" POL.PORT_NAME POL, ");
                strSQL.Append(" B.PORT_MST_POD_FK, ");
                strSQL.Append(" POD.PORT_NAME POD, ");
                strSQL.Append(" J.CARGO_MOVE_FK, ");
                strSQL.Append(" MOV.CARGO_MOVE_CODE MOVECODE, ");
                strSQL.Append(" B.COMMODITY_GROUP_FK, ");
                strSQL.Append(" COM.COMMODITY_GROUP_CODE, ");
                strSQL.Append(" J.SHIPPING_TERMS_MST_FK, ");
                strSQL.Append(" SH.INCO_CODE, ");
                strSQL.Append(" J.PYMT_TYPE, ");
                if (TYPE == "0")
                {
                    strSQL.Append(" H.MARKS_NUMBERS, ");
                    strSQL.Append(" H.GOODS_DESCRIPTION, ");
                    strSQL.Append(" nvl(H.DP_AGENT_MST_FK,0) DP_AGENT_MST_FK, ");
                    strSQL.Append(" FIRSTVSL.VESSEL_ID, ");
                    strSQL.Append(" FIRSTVOY.POL_ETD, ");
                    strSQL.Append(" FIRSTVSL.vessel_name, ");
                    strSQL.Append(" FIRSTVOY.voyage,");
                    strSQL.Append(" H.voyage_trn_fk, ");
                    strSQL.Append(" H.shipper_address, ");
                    strSQL.Append(" H.consignee_address, ");
                    strSQL.Append(" H.dp_agent_address, ");
                }
                else
                {
                    strSQL.Append(" J.MARKS_NUMBERS, ");
                    strSQL.Append(" J.GOODS_DESCRIPTION, ");
                    strSQL.Append(" NVL(J.DP_AGENT_MST_FK,0) DP_AGENT_MST_FK, ");
                    strSQL.Append(" FIRSTVSL.VESSEL_ID, ");
                    strSQL.Append(" FIRSTVOY.POL_ETD, ");
                    strSQL.Append(" FIRSTVSL.vessel_name, ");
                    strSQL.Append(" FIRSTVOY.voyage, ");
                    strSQL.Append(" j.voyage_trn_fk, ");

                    strSQL.Append(" AGD.ADM_ADDRESS_1 AS AGENTADD1, ");
                    strSQL.Append(" AGD.ADM_ADDRESS_2 AS AGENTADD2, ");
                    strSQL.Append(" AGD.ADM_ADDRESS_3 AS AGENTADD3, ");

                    strSQL.Append(" CTD.ADM_ADDRESS_1 AS CONSIGNEEADD1, ");
                    strSQL.Append(" CTD.ADM_ADDRESS_2 AS CONSIGNEEADD2, ");
                    strSQL.Append(" CTD.ADM_ADDRESS_3 AS CONSIGNEEADD3, ");
                    strSQL.Append(" CTSD.ADM_ADDRESS_1 AS SHIPPERADD1, ");
                    strSQL.Append(" CTSD.ADM_ADDRESS_2 AS SHIPPERADD2, ");
                    strSQL.Append(" CTSD.ADM_ADDRESS_3 AS SHIPPERADD3, ");
                }
                strSQL.Append(" PL.PLACE_CODE AS place_name, ");
                strSQL.Append(" B.DEL_PLACE_MST_FK AS place_of_delivery_fk, ");
                strSQL.Append(" B.COL_PLACE_MST_FK AS PLACE_OF_COL_FK, COLPL.PLACE_CODE AS COLPLACE_NAME,");
                strSQL.Append(" AG.AGENT_NAME AS AGENTID, ");
                strSQL.Append(" CT.CUSTOMER_NAME AS CONSIGNEEID, ");
                strSQL.Append(" CTS.CUSTOMER_NAME AS SHIPPERID, ");
                if (TYPE == "0")
                {
                    strSQL.Append("    NOTIFYHBL.CUSTOMER_NAME as notifyname,");
                    strSQL.Append("  ( NOTIFYADDHBL.ADM_ADDRESS_1 || '  ' || NOTIFYADDHBL.ADM_ADDRESS_2 || '  ' ||  NOTIFYADDHBL.ADM_ADDRESS_3  ) NOTIFYADDRESS ");
                }
                else
                {
                    strSQL.Append("    NOTIFY.CUSTOMER_NAME as notifyname ,");
                    strSQL.Append("   ( NOTIFYADD.ADM_ADDRESS_1 || '  ' || NOTIFYADD.ADM_ADDRESS_2 || '  ' ||  NOTIFYADD.ADM_ADDRESS_3  ) NOTIFYADDRESS ");
                }

                strSQL.Append(" FROM BOOKING_MST_TBL    B, ");
                strSQL.Append(" HBL_EXP_TBL    H, ");
                strSQL.Append(" JOB_CARD_TRN    J, ");
                strSQL.Append(" OPERATOR_MST_TBL         O,");
                strSQL.Append(" PORT_MST_TBL            POL, ");
                strSQL.Append(" PORT_MST_TBL            POD,    ");
                strSQL.Append(" CARGO_MOVE_MST_TBL      MOV,    ");
                strSQL.Append(" COMMODITY_GROUP_MST_TBL COM,    ");
                strSQL.Append(" SHIPPING_TERMS_MST_TBL  SH, ");
                strSQL.Append(" PLACE_MST_TBL   PL,  PLACE_MST_TBL COLPL,");
                strSQL.Append(" AGENT_MST_TBL AG,   ");
                strSQL.Append(" AGENT_CONTACT_DTLS      AGD,    ");
                strSQL.Append(" CUSTOMER_MST_TBL CT,    ");
                strSQL.Append(" CUSTOMER_CONTACT_DTLS  CTD, ");
                strSQL.Append(" CUSTOMER_MST_TBL      CTS,  ");
                strSQL.Append(" CUSTOMER_CONTACT_DTLS CTSD,  ");

                if (TYPE == "0")
                {
                    strSQL.Append(" customer_mst_tbl NOTIFYHBL,");
                    strSQL.Append("  CUSTOMER_CONTACT_DTLS   NOTIFYADDHBL,");
                }
                else
                {
                    strSQL.Append("customer_mst_tbl       NOTIFY, ");
                    strSQL.Append("CUSTOMER_CONTACT_DTLS NOTIFYADD, ");
                }

                strSQL.Append(" VESSEL_VOYAGE_TBL FIRSTVSL,  ");
                strSQL.Append(" VESSEL_VOYAGE_TRN FIRSTVOY   ");
                strSQL.Append(" WHERE   ");
                strSQL.Append(" O.OPERATOR_MST_PK(+)=B.CARRIER_MST_FK AND  ");
                strSQL.Append(" POL.PORT_MST_PK(+)= B.PORT_MST_POL_FK AND   ");
                strSQL.Append(" POD.PORT_MST_PK(+)= B.PORT_MST_POD_FK AND   ");
                strSQL.Append("  MOV.CARGO_MOVE_PK(+) = J.CARGO_MOVE_FK AND ");
                strSQL.Append(" COM.COMMODITY_GROUP_PK(+)=B.COMMODITY_GROUP_FK AND ");
                strSQL.Append(" SH.SHIPPING_TERMS_MST_PK(+)=J.SHIPPING_TERMS_MST_FK AND ");
                strSQL.Append(" PL.PLACE_PK(+)=B.DEL_PLACE_MST_FK ");
                strSQL.Append(" AND COLPL.PLACE_PK(+)=B.COL_PLACE_MST_FK ");
                strSQL.Append(" AND AGD.AGENT_MST_FK(+) = AG.AGENT_MST_PK  ");
                strSQL.Append(" AND J.BOOKING_MST_FK = B.BOOKING_MST_PK ");
                strSQL.Append(" AND H.JOB_CARD_SEA_EXP_FK(+)=J.JOB_CARD_TRN_PK ");
                if (TYPE == "0")
                {
                    strSQL.Append(" AND AG.AGENT_MST_PK(+)=H.DP_AGENT_MST_FK ");
                }
                else
                {
                    strSQL.Append(" AND AG.AGENT_MST_PK(+)=J.DP_AGENT_MST_FK ");
                }
                if (TYPE == "0")
                {
                    strSQL.Append(" AND CT.CUSTOMER_MST_PK(+)= H.CONSIGNEE_CUST_MST_FK  ");
                    strSQL.Append(" AND CTD.CUSTOMER_MST_FK(+) = CT.CUSTOMER_MST_PK ");
                    strSQL.Append(" AND CTS.CUSTOMER_MST_PK(+)= H.SHIPPER_CUST_MST_FK ");
                    strSQL.Append(" AND CTSD.CUSTOMER_MST_FK(+) = CTS.CUSTOMER_MST_PK ");
                    strSQL.Append(" AND H.VOYAGE_TRN_FK       = FIRSTVOY.VOYAGE_TRN_PK(+)");
                    strSQL.Append(" AND FIRSTVOY.VESSEL_VOYAGE_TBL_FK = FIRSTVSL.VESSEL_VOYAGE_TBL_PK(+)");
                    strSQL.Append("   AND NOTIFYHBL.CUSTOMER_MST_PK=NOTIFYADDHBL.CUSTOMER_MST_FK(+)");
                    strSQL.Append("  AND h.notify1_cust_mst_fk =NOTIFYHBL.CUSTOMER_MST_PK(+)");
                    strSQL.Append(" AND H.HBL_EXP_TBL_PK=" + strPk + "");
                }
                else
                {
                    strSQL.Append(" AND CT.CUSTOMER_MST_PK(+)= J.CONSIGNEE_CUST_MST_FK ");
                    strSQL.Append(" AND CTD.CUSTOMER_MST_FK(+) = CT.CUSTOMER_MST_PK  ");
                    strSQL.Append(" AND CTS.CUSTOMER_MST_PK(+)= J.SHIPPER_CUST_MST_FK ");
                    strSQL.Append(" AND CTSD.CUSTOMER_MST_FK(+) = CTS.CUSTOMER_MST_PK ");
                    strSQL.Append(" AND J.VOYAGE_TRN_FK       = FIRSTVOY.VOYAGE_TRN_PK(+)");
                    strSQL.Append("  AND J.NOTIFY1_CUST_MST_FK =NOTIFY.CUSTOMER_MST_PK(+)");
                    strSQL.Append("   AND NOTIFY.CUSTOMER_MST_PK=NOTIFYADD.CUSTOMER_MST_FK(+)");
                    strSQL.Append(" AND FIRSTVOY.VESSEL_VOYAGE_TBL_FK = FIRSTVSL.VESSEL_VOYAGE_TBL_PK(+)");
                    strSQL.Append(" AND J.JOB_CARD_TRN_PK =" + strPk + "");
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

        #endregion " Fetch All On New"

        #region "Get Consignee Currency ID"

        //adding by thiyagarajan on 22/12/08 to display currency for collect chrg.as per consignee location
        public string FetchConsCurrId(string Pk, string Type)
        {
            StringBuilder strSQl = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataSet dscurr = null;
            try
            {
                if (Type == "1" | Type == "2")
                {
                    strSQl.Append("select mbl.mbl_exp_tbl_pk pk , curr.currency_id currid from mbl_exp_tbl mbl, JOB_CARD_TRN job, ");
                    strSQl.Append(" PORT_MST_TBL PMT, LOCATION_MST_TBL LMT, COUNTRY_MST_TBL CMT,currency_type_mst_tbl curr,BOOKING_MST_TBL BST where 1=1 ");
                    if (Type == "2")
                    {
                        strSQl.Append(" and mbl.mbl_exp_tbl_pk=job.MBL_MAWB_FK ");
                        strSQl.Append(" and mbl.mbl_exp_tbl_pk=" + Pk + "  ");
                        strSQl.Append(" AND PMT.PORT_MST_PK = MBL.PORT_MST_POD_FK  ");
                        strSQl.Append(" AND LMT.LOCATION_MST_PK = PMT.LOCATION_MST_FK ");
                        strSQl.Append(" AND CMT.COUNTRY_MST_PK = LMT.COUNTRY_MST_FK ");
                        strSQl.Append(" AND CURR.CURRENCY_MST_PK = CMT.CURRENCY_MST_FK ");
                        strSQl.Append(" AND BST.BOOKING_MST_PK = JOB.BOOKING_MST_FK ");
                        //new MBL
                    }
                    else
                    {
                        strSQl.Append(" and mbl.mbl_exp_tbl_pk(+)=job.MBL_MAWB_FK ");
                        strSQl.Append(" and job.JOB_CARD_TRN_PK =" + Pk + "  ");
                        strSQl.Append(" AND PMT.PORT_MST_PK = BST.PORT_MST_POD_FK  ");
                        strSQl.Append(" AND LMT.LOCATION_MST_PK = PMT.LOCATION_MST_FK ");
                        strSQl.Append(" AND CMT.COUNTRY_MST_PK = LMT.COUNTRY_MST_FK ");
                        strSQl.Append(" AND CURR.CURRENCY_MST_PK = CMT.CURRENCY_MST_FK ");
                        strSQl.Append(" AND BST.BOOKING_MST_PK = JOB.BOOKING_MST_FK ");
                    }
                    strSQl.Append(" group by mbl.mbl_exp_tbl_pk,  ");
                    strSQl.Append(" curr.currency_id ");
                }
                else
                {
                    strSQl.Append(" select hbl.hbl_exp_tbl_pk pk,curr.currency_id currid from hbl_exp_tbl hbl, currency_type_mst_tbl curr, ");
                    strSQl.Append(" BOOKING_MST_TBL BST, PORT_MST_TBL PMT, LOCATION_MST_TBL LMT, JOB_CARD_TRN  JSET, COUNTRY_MST_TBL       CMT ");
                    strSQl.Append(" where hbl.hbl_exp_tbl_pk= " + Pk + "");
                    strSQl.Append(" AND JSET.JOB_CARD_TRN_PK = HBL.JOB_CARD_SEA_EXP_FK ");
                    strSQl.Append(" AND BST.BOOKING_MST_PK = JSET.BOOKING_MST_FK ");
                    strSQl.Append(" AND PMT.PORT_MST_PK = BST.PORT_MST_POD_FK ");
                    strSQl.Append(" AND LMT.LOCATION_MST_PK = PMT.LOCATION_MST_FK ");
                    strSQl.Append(" AND CMT.COUNTRY_MST_PK = LMT.COUNTRY_MST_FK ");
                    strSQl.Append(" AND CURR.CURRENCY_MST_PK = CMT.CURRENCY_MST_FK ");
                }
                dscurr = objWF.GetDataSet(strSQl.ToString());
                if (dscurr.Tables[0].Rows.Count > 0)
                {
                    return Convert.ToString(dscurr.Tables[0].Rows[0]["currid"]);
                }
                else
                {
                    return "";
                }
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

        #endregion "Get Consignee Currency ID"

        #region " Fetch Called On Consolidation"

        //Called When Consolidating Multiple HBL's or Job Cards
        public DataSet FetchForConsolidation(string HBLPks, int DPAgentPk)
        {
            StringBuilder strsql = new StringBuilder();
            strsql.Append("   SELECT HBL.HBL_EXP_TBL_PK, ");
            strsql.Append("   JOB.JOB_CARD_TRN_PK,BOOK.CARRIER_MST_FK, ");
            strsql.Append("   OPR.OPERATOR_NAME,");
            strsql.Append("   BOOK.PORT_MST_POL_FK,");
            strsql.Append("   POL.PORT_ID POL,");
            strsql.Append("   BOOK.PORT_MST_POD_FK,");
            strsql.Append("   POD.PORT_ID POD,");
            strsql.Append("   BOOK.CARGO_MOVE_FK,");
            strsql.Append("   MOV.CARGO_MOVE_CODE MOVECODE,");
            strsql.Append("   BOOKT.COMMODITY_GROUP_FK,");
            strsql.Append("   COM.COMMODITY_GROUP_CODE,");
            strsql.Append("   JOB.SHIPPING_TERMS_MST_FK,");
            strsql.Append("   SH.FREIGHT_TYPE,");
            strsql.Append("   BOOK.PYMT_TYPE,");
            strsql.Append("   HBL.MARKS_NUMBERS,");
            strsql.Append("   HBL.GOODS_DESCRIPTION,");
            strsql.Append("   PL.PLACE_NAME,");
            strsql.Append("   BOOK.DEL_PLACE_MST_FK,");
            strsql.Append("   AG.AGENT_ID,");
            strsql.Append("   AGD.ADM_ADDRESS_1,");
            strsql.Append("   AGD.ADM_ADDRESS_2,");
            strsql.Append("   AGD.ADM_ADDRESS_3,");
            strsql.Append("   sum(HBL.TOTAL_VOLUME), ");
            strsql.Append("   sum(HBL.TOTAL_CHARGE_WEIGHT),");
            strsql.Append("   sum(HBL.Total_Gross_Weight),");
            strsql.Append("   sum(HBL.Total_Pack_Count),");
            strsql.Append("   sum(HBL.Total_Net_Weight),");
            strsql.Append("   sum(HBL.Total_Net_Weight) ");

            strsql.Append("   FROM BOOKING_MST_TBL BOOK,");
            strsql.Append("   HBL_EXP_TBL  HBL,");
            strsql.Append("   JOB_CARD_TRN JOB,");
            strsql.Append("   BOOKING_TRN BOOKT ,");
            strsql.Append("   OPERATOR_MST_TBL  OPR ,");
            strsql.Append("   PORT_MST_TBL   POL,");
            strsql.Append("   PORT_MST_TBL   POD,");
            strsql.Append("   CARGO_MOVE_MST_TBL  MOV,");
            strsql.Append("   COMMODITY_GROUP_MST_TBL COM,");
            strsql.Append("   SHIPPING_TERMS_MST_TBL  SH,");
            strsql.Append("   PLACE_MST_TBL  PL,");
            strsql.Append("   AGENT_MST_TBL AG,");
            strsql.Append("   AGENT_CONTACT_DTLS  AGD");

            strsql.Append("  WHERE");
            strsql.Append("   OPR.OPERATOR_MST_PK(+)= BOOK.CARRIER_MST_FK AND");
            strsql.Append("   POL.PORT_MST_PK(+)= BOOK.PORT_MST_POL_FK AND");
            strsql.Append("   POD.PORT_MST_PK(+)= BOOK.PORT_MST_POD_FK AND");
            strsql.Append("   MOV.CARGO_MOVE_PK(+)= BOOK.CARGO_MOVE_FK AND");
            strsql.Append("   COM.COMMODITY_GROUP_PK(+)=BOOKT.COMMODITY_GROUP_FK AND");
            strsql.Append("   SH.SHIPPING_TERMS_MST_PK(+)= JOB.SHIPPING_TERMS_MST_FK AND");
            strsql.Append("   PL.PLACE_PK(+) = BOOK.DEL_PLACE_MST_FK AND");
            strsql.Append("   AG.AGENT_MST_PK(+)= " + DPAgentPk + " ");
            strsql.Append("   AND AGD.AGENT_MST_FK(+) = AG.AGENT_MST_PK");
            strsql.Append("   AND JOB.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
            strsql.Append("   AND HBL.JOB_CARD_SEA_EXP_FK =JOB.JOB_CARD_TRN_PK");
            strsql.Append("   AND HBL.HBL_EXP_TBL_PK IN " + "(" + HBLPks + ")");
            strsql.Append("   GROUP BY ");
            strsql.Append("   HBL.HBL_EXP_TBL_PK,");
            strsql.Append("   JOB.JOB_CARD_TRN_PK,BOOK.CARRIER_MST_FK,");
            strsql.Append("   OPR.OPERATOR_NAME, ");
            strsql.Append("   BOOK.PORT_MST_POL_FK, ");
            strsql.Append("   POL.PORT_ID, ");
            strsql.Append("   BOOK.PORT_MST_POD_FK,");
            strsql.Append("   POD.PORT_ID ,");
            strsql.Append("   BOOK.CARGO_MOVE_FK,");
            strsql.Append("   MOV.CARGO_MOVE_CODE,");
            strsql.Append("   BOOKT.COMMODITY_GROUP_FK,");
            strsql.Append("   COM.COMMODITY_GROUP_CODE,");
            strsql.Append("   JOB.SHIPPING_TERMS_MST_FK,");
            strsql.Append("   SH.FREIGHT_TYPE, ");
            strsql.Append("   BOOK.PYMT_TYPE, ");
            strsql.Append("   HBL.MARKS_NUMBERS, ");
            strsql.Append("   HBL.GOODS_DESCRIPTION, ");
            strsql.Append("   PL.PLACE_NAME, ");
            strsql.Append("   BOOK.DEL_PLACE_MST_FK, ");
            strsql.Append("   HBL.DP_AGENT_MST_FK, ");
            strsql.Append("   AG.AGENT_ID, ");
            strsql.Append("   AGD.ADM_ADDRESS_1, ");
            strsql.Append("   AGD.ADM_ADDRESS_2, ");
            strsql.Append("   AGD.ADM_ADDRESS_3 ");
            try
            {
                return objWF.GetDataSet(strsql.ToString());
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

        #endregion " Fetch Called On Consolidation"

        #region " Fetch All on Edit Master BL "

        //Fetching All Data On Edit Mode to Fill the Controls
        public DataSet FetchAllOnEdit(string mblPk)
        {
            StringBuilder strSQl = new StringBuilder();
            strSQl.Append("  SELECT M.MBL_EXP_TBL_PK,");
            strSQl.Append("  HBL.HBL_REF_NO, ");
            strSQl.Append("  JOB.JOBCARD_REF_NO,");
            strSQl.Append("  M.OPERATOR_MST_FK,");
            strSQl.Append("  M.MBL_MADE_FROM,");
            strSQl.Append("  M.MBL_REF_NO,");
            strSQl.Append("  M.MBL_DATE,");
            strSQl.Append("  M.CARGO_TYPE, ");
            strSQl.Append("  A.OPERATOR_NAME,");
            strSQl.Append("  M.PORT_MST_POL_FK,");
            strSQl.Append("  POL.PORT_NAME POL,");
            strSQl.Append("  M.PORT_MST_POD_FK,");
            strSQl.Append("  POD.PORT_NAME POD,");
            strSQl.Append("  M.CARGO_MOVE_FK,");
            strSQl.Append("  MOV.CARGO_MOVE_CODE MOVECODE,");
            strSQl.Append("  M.COMMODITY_GROUP_FK,");
            strSQl.Append("  COM.COMMODITY_GROUP_CODE,");
            strSQl.Append("  M.SHIPPER_NAME,");
            strSQl.Append("  M.SHIPPER_ADDRESS,");
            strSQl.Append("  M.CONSIGNEE_NAME,");
            strSQl.Append("  M.CONSIGNEE_ADDRESS,");
            strSQl.Append("  M.VERSION_NO,");
            strSQl.Append("  M.AGENT_NAME,");
            strSQl.Append("  M.AGENT_ADDRESS,");
            strSQl.Append("  M.SHIPPING_TERMS_MST_FK,");
            strSQl.Append("  SH.INCO_CODE,");
            strSQl.Append("  M.PYMT_TYPE,");
            strSQl.Append("  M.MARKS_NUMBERS,");
            strSQl.Append("  M.GOODS_DESCRIPTION, ");
            strSQl.Append("  M.INSURANCE_AMT, ");
            strSQl.Append("  M.PREPAID_CHARGES, ");
            strSQl.Append("  M.COLLECT_CHARGES, ");
            strSQl.Append("  M.VOYAGE_TRN_FK, ");
            strSQl.Append("  V.VESSEL_ID, ");
            strSQl.Append("  VT.POL_ETD, ");
            strSQl.Append("  V.VESSEL_NAME, ");
            strSQl.Append("  VT.VOYAGE,");
            strSQl.Append("  M.LINER_TERMS_FK,");
            strSQl.Append(" m.status,");
            strSQl.Append("m.released_dt,");
            //adding & modifying by thiyagarajan on 3/8/08 for pts task
            strSQl.Append("  BOOK.Del_Place_Mst_Fk as PLACE_OF_DELIVERY_FK, ");
            strSQl.Append("  PL.PLACE_CODE as PLACE_NAME,");
            strSQl.Append("  BOOK.COL_PLACE_MST_FK AS PLACE_OF_COL_FK,COLPL.PLACE_CODE AS COLPLACE_NAME, M.SURRENDER_DT,");

            strSQl.Append("   M.NOTIFY_NAME as notifyname,");
            strSQl.Append("   M.NOTIFY_ADDRESS NOTIFYADDRESS ");

            //end

            strSQl.Append("  FROM MBL_EXP_TBL M,");
            strSQl.Append("  vessel_voyage_tbl v,");
            strSQl.Append("  Vessel_Voyage_Trn vt,");
            strSQl.Append("  OPERATOR_MST_TBL A, ");
            strSQl.Append("  PORT_MST_TBL POL, ");

            strSQl.Append("  JOB_CARD_TRN            J, ");
            strSQl.Append("   customer_mst_tbl       NOTIFY, ");
            strSQl.Append("  CUSTOMER_CONTACT_DTLS NOTIFYADD, ");

            strSQl.Append("  PORT_MST_TBL POD, ");
            strSQl.Append("  CARGO_MOVE_MST_TBL MOV, ");
            strSQl.Append("  COMMODITY_GROUP_MST_TBL COM, ");
            strSQl.Append("  SHIPPING_TERMS_MST_TBL SH, ");
            strSQl.Append("  PLACE_MST_TBL PL,  PLACE_MST_TBL COLPL,");
            //adding by thiyagarajan on 3/8/08
            strSQl.Append("  JOB_CARD_TRN JOB, ");
            strSQl.Append("  BOOKING_MST_TBL BOOK, ");
            strSQl.Append("  HBL_EXP_TBL    HBL ");
            strSQl.Append("  WHERE A.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK");
            strSQl.Append("  and v.vessel_voyage_tbl_pk(+) = vt.vessel_voyage_tbl_fk");
            strSQl.Append("  and vt.voyage_trn_pk(+)= m.voyage_trn_fk");
            strSQl.Append("  and   BOOK.BOOKING_MST_PK= JOB.BOOKING_MST_FK");
            strSQl.Append("  AND POL.PORT_MST_PK(+) = M.PORT_MST_POL_FK");
            strSQl.Append("  AND POD.PORT_MST_PK(+) = M.PORT_MST_POD_FK");
            strSQl.Append("  AND MOV.CARGO_MOVE_PK(+) = M.CARGO_MOVE_FK");
            strSQl.Append("  AND COM.COMMODITY_GROUP_PK(+) = M.COMMODITY_GROUP_FK");
            strSQl.Append("  AND SH.SHIPPING_TERMS_MST_PK(+) = M.SHIPPING_TERMS_MST_FK");
            strSQl.Append("  AND PL.PLACE_PK(+)=BOOK.Del_Place_Mst_Fk");
            strSQl.Append("  AND COLPL.PLACE_PK(+)=BOOK.COL_PLACE_MST_FK ");
            strSQl.Append("  AND JOB.MBL_MAWB_FK = M.MBL_EXP_TBL_PK ");

            strSQl.Append("   AND J.NOTIFY1_CUST_MST_FK =NOTIFY.CUSTOMER_MST_PK(+)");
            strSQl.Append(" AND NOTIFY.CUSTOMER_MST_PK=NOTIFYADD.CUSTOMER_MST_FK(+)");
            strSQl.Append("  and j.MBL_MAWB_FK= M.MBL_EXP_TBL_PK");

            strSQl.Append("  AND HBL.Mbl_Exp_Tbl_Fk(+) = M.MBL_EXP_TBL_PK ");
            strSQl.Append("  AND M.MBL_EXP_TBL_PK =" + mblPk + "");
            try
            {
                return objWF.GetDataSet(strSQl.ToString());
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

        #endregion " Fetch All on Edit Master BL "

        #region " Fetch Grid After Save"

        // Fetching the data In Grid from MBL's Trnsaction Table
        // After saving the data. Made Value is stored in DB to Identify from
        // Where MBL has been Created
        public object FetchInGridAfterSave(string strMblPk, int intMadeValue)
        {
            StringBuilder strSQl = new StringBuilder();
            strSQl.Append(" SELECT ROWNUM SR_NO,q.* FROM (SELECT ");
            strSQl.Append(" MT.CONTAINER_NUMBER, ");
            strSQl.Append(" CONT.CONTAINER_TYPE_MST_ID, ");
            strSQl.Append(" MT.CONTAINER_TYPE_MST_FK, ");
            strSQl.Append(" MT.SEAL_NUMBER, ");
            strSQl.Append(" MT.VOLUME_IN_CBM, ");
            strSQl.Append(" MT.GROSS_WEIGHT, ");
            strSQl.Append(" MT.NET_WEIGHT, ");
            strSQl.Append(" MT.CHARGEABLE_WEIGHT, ");
            strSQl.Append(" MT.PACK_TYPE_MST_FK, ");
            strSQl.Append(" PK.PACK_TYPE_DESC PACK_TYPE_ID, ");
            strSQl.Append(" MT.PACK_COUNT, ");
            strSQl.Append(" MT.COMMODITY_MST_FK, ");

            if (intMadeValue == 4)
            {
                strSQl.Append(" COM.COMMODITY_GROUP_CODE  COMMODITY_ID,");
            }
            else
            {
                strSQl.Append(" ''  COMMODITY_ID, ");
            }
            strSQl.Append(" MT.MBL_TRN_EXP_CONT_PK, ");
            strSQl.Append(" NVL(MT.COMMODITY_MST_FKS,'')COMMODITY_MST_FKS,MT.JOBCARD_REF_NR JOBCARDREFNO ");
            //Manoharan 11June2009: to introduce multiple commodity selection in MBL as like JobCard
            strSQl.Append(" FROM MBL_TRN_EXP_CONTAINER MT,");
            strSQl.Append(" PACK_TYPE_MST_TBL PK,");

            if (intMadeValue == 4)
            {
                strSQl.Append(" COMMODITY_GROUP_MST_TBL COM,");
            }
            else
            {
                strSQl.Append(" COMMODITY_MST_TBL COM,");
            }
            strSQl.Append(" CONTAINER_TYPE_MST_TBL CONT");
            strSQl.Append(" WHERE");
            strSQl.Append(" PK.PACK_TYPE_MST_PK(+)=MT.PACK_TYPE_MST_FK AND");
            if (intMadeValue == 4)
            {
                strSQl.Append(" COM.COMMODITY_GROUP_PK(+)=MT.COMMODITY_MST_FK AND ");
            }
            else
            {
                strSQl.Append(" COM.COMMODITY_MST_PK(+)=MT.COMMODITY_MST_FK AND ");
            }
            strSQl.Append(" CONT.CONTAINER_TYPE_MST_PK(+) = MT.CONTAINER_TYPE_MST_FK AND");
            strSQl.Append(" MT.MBL_EXP_TBL_FK=" + strMblPk + " ");
            strSQl.Append(") q");
            try
            {
                return objWF.GetDataSet(strSQl.ToString());
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

        #endregion " Fetch Grid After Save"

        #region " Fetch To Grid"

        public DataSet FetchTrans(string JobPk = "0")
        {
            //Fetch TRansaction Details(Container Details from Job Card) Which
            //fills the Grid in MBL
            StringBuilder strSqlBuilder = new StringBuilder();
            strSqlBuilder.Append(" SELECT ROWNUM SR_NO,q.* FROM (SELECT ");
            strSqlBuilder.Append(" JOBT.CONTAINER_NUMBER,");
            strSqlBuilder.Append(" CONT.CONTAINER_TYPE_MST_ID, ");
            strSqlBuilder.Append("CONT.CONTAINER_TYPE_MST_PK,");
            strSqlBuilder.Append(" JOBT.SEAL_NUMBER,");
            strSqlBuilder.Append(" JOBT.VOLUME_IN_CBM,");
            strSqlBuilder.Append(" JOBT.GROSS_WEIGHT,");
            strSqlBuilder.Append(" JOBT.NET_WEIGHT,");
            strSqlBuilder.Append(" JOBT.CHARGEABLE_WEIGHT,");
            strSqlBuilder.Append(" JOBT.PACK_TYPE_MST_FK,");
            strSqlBuilder.Append(" PACK.PACK_TYPE_ID,");
            strSqlBuilder.Append(" JOBT.PACK_COUNT,");
            strSqlBuilder.Append(" JOBT.COMMODITY_MST_FK,");
            strSqlBuilder.Append(" '' COMMODITY_ID,");
            strSqlBuilder.Append(" '' MBL_TRN_EXP_CONT_PK, JOBT.COMMODITY_MST_FKS,");
            strSqlBuilder.Append(" JOBT.JOB_TRN_CONT_PK JOBCARDREFNO");
            strSqlBuilder.Append(" FROM JOB_TRN_CONT JOBT,");
            strSqlBuilder.Append(" PACK_TYPE_MST_TBL PACK,");
            strSqlBuilder.Append(" COMMODITY_MST_TBL COM,");
            strSqlBuilder.Append(" CONTAINER_TYPE_MST_TBL CONT");
            strSqlBuilder.Append(" WHERE");
            strSqlBuilder.Append(" PACK.PACK_TYPE_MST_PK(+)=JOBT.PACK_TYPE_MST_FK AND");
            strSqlBuilder.Append(" COM.COMMODITY_MST_PK(+)=JOBT.COMMODITY_MST_FK AND");
            strSqlBuilder.Append(" CONT.CONTAINER_TYPE_MST_PK(+) = JOBT.CONTAINER_TYPE_MST_FK AND");
            strSqlBuilder.Append(" JOBT.JOB_CARD_TRN_FK=" + JobPk + "");
            strSqlBuilder.Append(" ) q");
            try
            {
                return objWF.GetDataSet(strSqlBuilder.ToString());
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

        #endregion " Fetch To Grid"

        #region " Save And Update "

        public ArrayList SaveAll(long nLocationfk, ref string MblPk, int mblmadefrom, string mblrefno, string mbldate, int operatorfk, int polfk, int podfk, string VESSEL_ID, string FirstVoyageFk,
        string FirstVessel, string FirstVoyage, string ETD_DATE, int commodityfk, string shippername, string shipperaddress, string consigneename, string consigneeaddress, int totalpackcount, double totalgrossweight,
        double totalnetweight, double totalchargeableweight, double totalvolume, string marksnumbers, string goodsdescription, string agentname, string agentaddress, long cargomovefk, short pymttypeInt, long shippingtermsmstfk,
        string hblexptblfk, string jobcardfk, string Version, string InsuranceAmt, string PrepaidCharges, string CollectCharges, int DPAgentFk, ref DataSet GridDs, int CargoType, string strTransaction = "",
        int VoyageFk = 0, int pldfk = 0, Int32 PLRFK = 0, int MJCPk = 0, int AssignedToAgt = 0, int ddlLTerms = 0, string SurrDt = "", int Status_mbl = 0, string Released_date = "", string notifyname = "",
        string notifyaddress = "")
        {
            try
            {
                WorkFlow objWK = new WorkFlow();
                cls_SeaBookingEntry objSBE = new cls_SeaBookingEntry();
                string strpk = null;
                Int16 Execute = default(Int16);
                objWK.OpenConnection();
                OracleTransaction TRAN = default(OracleTransaction);
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;
                string strVoyagepk = "";
                strVoyagepk = FirstVoyageFk;
                Int32 RecAfct = default(Int32);
                ConfigurationPK = 3010;
                Int32 status = default(Int32);
                //adding by thiyagarajan on 10/10/08 to save the MBL into Track N Trace while insert only

                if ((string.IsNullOrEmpty(FirstVoyageFk) || FirstVoyageFk == "0") & !string.IsNullOrEmpty(VESSEL_ID))
                {
                    FirstVoyageFk = "0";
                    objSBE.CREATED_BY = CREATED_BY;
                    objSBE.ConfigurationPK = ConfigurationPK;
                    if (!string.IsNullOrEmpty(FirstVessel) & !string.IsNullOrEmpty(VESSEL_ID) & !string.IsNullOrEmpty(FirstVoyage))
                    {
                        arrMessage = objSBE.SaveVesselMaster(
                            Convert.ToInt64(FirstVoyageFk),
                            getDefault(FirstVessel, "").ToString(),
                            Convert.ToInt64(getDefault(operatorfk, 0)),
                            getDefault(VESSEL_ID, "").ToString(),
                            getDefault(FirstVoyage, "").ToString(),
                            objWK.MyCommand,
                            Convert.ToInt64(getDefault(polfk, 0)),
                            Convert.ToString(getDefault(podfk, 0)),
                            DateTime.MinValue,
                            Convert.ToDateTime(getDefault(ETD_DATE,null)),
                            DateTime.MinValue, DateTime.MinValue, DateTime.MinValue,
                            DateTime.MinValue);
                        strVoyagepk = FirstVoyageFk;
                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }
                        else
                        {
                            arrMessage.Clear();
                        }
                    }
                }

                var _with1 = objWK.MyCommand;
                short deleteflag = 0;
                if (strTransaction.Trim().Length > 0)
                {
                    deleteflag = 1;
                }
                //modifying by thiyagarajan on 10/10/08 to save the MBL into Track N Trace while insert only
                if (MblPk == "0")
                {
                    status = 1;
                    _with1.CommandText = objWK.MyUserName + ".MBL_EXP_TBL_PKG.MBL_EXP_TBL_INS";
                }
                else
                {
                    status = 0;
                    _with1.CommandText = objWK.MyUserName + ".MBL_EXP_TBL_PKG.MBL_EXP_TBL_UPD";
                }
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.Transaction = TRAN;
                var _with2 = _with1.Parameters;
                if (MblPk != "0")
                {
                    _with2.Add("MBL_EXP_TBL_PK_IN", MblPk);
                    _with2.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY);
                    _with2.Add("VERSION_NO_IN", Version);
                    _with2.Add("DELETE_CLAUSE_FLAG_IN", deleteflag);
                }
                else
                {
                    _with2.Add("MJC_PK_IN", MJCPk);
                    _with2.Add("HBL_EXP_TBL_FK_IN", ifDBNull(hblexptblfk));
                    _with2.Add("JOB_CARD_FK_IN", ifDBNull(jobcardfk));
                    _with2.Add("CREATED_BY_FK_IN", CREATED_BY);
                    _with2.Add("PLACE_OF_RECEIPT_FK_IN", (PLRFK == 0 ? 0 : PLRFK)).Direction = ParameterDirection.Input;
                    _with2.Add("MBL_MADE_FROM_IN", mblmadefrom).Direction = ParameterDirection.Input;
                }

                if (mblrefno.Trim().Length > 0)
                {
                    _with2.Add("MBL_REF_NO_IN", mblrefno).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with2.Add("MBL_REF_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                _with2.Add("MBL_DATE_IN", mbldate).Direction = ParameterDirection.Input;
                _with2.Add("OPERATOR_MST_FK_IN", getDefault(operatorfk, DBNull.Value)).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with2.Add("PORT_MST_POL_FK_IN", polfk).Direction = ParameterDirection.Input;
                _with2.Add("PORT_MST_POD_FK_IN", podfk).Direction = ParameterDirection.Input;
                _with2.Add("COMMODITY_GROUP_FK_IN", commodityfk).Direction = ParameterDirection.Input;
                _with2.Add("SHIPPER_NAME_IN", (string.IsNullOrEmpty(shippername) ? "" : shippername)).Direction = ParameterDirection.Input;
                _with2.Add("SHIPPER_ADDRESS_IN", (string.IsNullOrEmpty(shipperaddress) ? "" : shipperaddress)).Direction = ParameterDirection.Input;
                _with2.Add("CONSIGNEE_NAME_IN", (string.IsNullOrEmpty(consigneename) ? "" : consigneename)).Direction = ParameterDirection.Input;
                _with2.Add("CONSIGNEE_ADDRESS_IN", (string.IsNullOrEmpty(consigneeaddress) ? "" : consigneeaddress)).Direction = ParameterDirection.Input;
                _with2.Add("NOTIFY_NAME_IN", (string.IsNullOrEmpty(notifyname) ? "" : notifyname)).Direction = ParameterDirection.Input;
                _with2.Add("NOTIFY_ADDRESS_IN", (string.IsNullOrEmpty(notifyaddress) ? "" : notifyaddress)).Direction = ParameterDirection.Input;
                _with2.Add("TOTAL_PACK_COUNT_IN", totalpackcount).Direction = ParameterDirection.Input;
                _with2.Add("TOTAL_GROSS_WEIGHT_IN", totalgrossweight).Direction = ParameterDirection.Input;
                _with2.Add("TOTAL_NET_WEIGHT_IN", totalnetweight).Direction = ParameterDirection.Input;
                _with2.Add("TOTAL_VOLUME_IN", totalvolume).Direction = ParameterDirection.Input;
                //.Add("PLD_IN", IIf(pld = "", "", pld)).Direction = ParameterDirection.Input
                _with2.Add("MARKS_NUMBERS_IN", (string.IsNullOrEmpty(marksnumbers) ? "" : marksnumbers)).Direction = ParameterDirection.Input;
                _with2.Add("GOODS_DESCRIPTION_IN", (string.IsNullOrEmpty(goodsdescription) ? "" : goodsdescription)).Direction = ParameterDirection.Input;
                _with2.Add("AGENT_NAME_IN", (string.IsNullOrEmpty(agentname) ? "" : agentname)).Direction = ParameterDirection.Input;
                _with2.Add("AGENT_ADDRESS_IN", (string.IsNullOrEmpty(agentaddress) ? "" : agentaddress)).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_MOVE_FK_IN", (cargomovefk == 0 ? 0 : cargomovefk)).Direction = ParameterDirection.Input;
                _with2.Add("PYMT_TYPE_IN", pymttypeInt).Direction = ParameterDirection.Input;
                _with2.Add("SHIPPING_TERMS_MST_FK_IN", (shippingtermsmstfk == 0 ? 0 : shippingtermsmstfk)).Direction = ParameterDirection.Input;
                _with2.Add("INSURANCE_AMT_IN", ifDBNull(Convert.ToDouble(InsuranceAmt))).Direction = ParameterDirection.Input;
                _with2.Add("PREPAID_CHARGES_IN", ifDBNull(Convert.ToDouble(PrepaidCharges))).Direction = ParameterDirection.Input;
                _with2.Add("COLLECT_CHARGES_IN", ifDBNull(Convert.ToDouble(CollectCharges))).Direction = ParameterDirection.Input;
                _with2.Add("DP_AGENT_MST_FK_IN", (DPAgentFk == 0 ? 0 : DPAgentFk)).Direction = ParameterDirection.Input;
                _with2.Add("VOYAGE_TRN_FK_IN", (strVoyagepk == "0" ? "" : strVoyagepk)).Direction = ParameterDirection.Input;
                _with2.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with2.Add("PLACE_OF_DELIVERY_FK_IN", (pldfk == 0 ? 0 : pldfk)).Direction = ParameterDirection.Input;
                _with2.Add("ASSIGN_TO_AGENT_IN", AssignedToAgt).Direction = ParameterDirection.Input;
                _with2.Add("LINER_TERMS_IN", ddlLTerms).Direction = ParameterDirection.Input;
                _with2.Add("SURRENDER_DT_IN", getDefault(SurrDt, ""));
                _with2.Add("STATUS_IN", Status_mbl).Direction = ParameterDirection.Input;
                _with2.Add("RELEASED_DT_IN", getDefault(Released_date, ""));
                _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MBL_EXP_TBL_PK").Direction = ParameterDirection.Output;
                try
                {
                    Execute = Convert.ToInt16(_with1.ExecuteNonQuery());
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                    throw oraexp;
                }
                string CurMblPk = null;
                CurMblPk = Convert.ToString(_with1.Parameters["RETURN_VALUE"].Value);
                // added by gopi for save seal no and pack type on 03/05/2007
                if (MblPk == "0")
                {
                    _with1.CommandText = objWK.MyUserName + ".MBL_EXP_TBL_PKG.MBL_TRN_EXP_CONTAINER_INS";
                }
                else
                {
                    _with1.CommandText = objWK.MyUserName + ".MBL_EXP_TBL_PKG.MBL_TRN_EXP_CONTAINER_UPD";
                }
                DataRow dr = null;

                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                try
                {
                    foreach (DataRow dr_loopVariable in GridDs.Tables[0].Rows)
                    {
                        dr = dr_loopVariable;
                        var _with3 = _with1.Parameters;
                        _with3.Clear();
                        if (MblPk != "0")
                        {
                            _with3.Add("MBL_TRN_EXP_CONT_PK_IN", dr["MBL_TRN_EXP_CONT_PK"]);
                            _with3.Add("CONTAINER_TYPE_MST_FK_IN", dr["CONTAINER_TYPE_MST_FK"]);
                        }
                        else
                        {
                            _with3.Add("CONTAINER_TYPE_MST_FK_IN", dr["CONTAINER_TYPE_MST_PK"]);
                        }
                        _with3.Add("MBL_EXP_TBL_FK_IN", CurMblPk);
                        _with3.Add("CONTAINER_NUMBER_IN", (string.IsNullOrEmpty(dr["CONTAINER_NUMBER"].ToString()) ? "" : dr["CONTAINER_NUMBER"]));
                        _with3.Add("SEAL_NUMBER_IN", (string.IsNullOrEmpty(dr["SEAL_NUMBER"].ToString()) ? "" : dr["SEAL_NUMBER"]));

                        _with3.Add("JOBCARD_REF_NR_IN", (string.IsNullOrEmpty(dr["JOBCARDREFNO"].ToString()) ? DBNull.Value : dr["JOBCARDREFNO"]));
                        //'ADEED FOR THE DTS:6793
                        _with3.Add("VOLUME_IN_CBM_IN", dr["VOLUME_IN_CBM"]);
                        _with3.Add("GROSS_WEIGHT_IN", dr["GROSS_WEIGHT"]);
                        _with3.Add("NET_WEIGHT_IN", dr["NET_WEIGHT"]);
                        _with3.Add("CHARGEABLE_WEIGHT_IN", dr["CHARGEABLE_WEIGHT"]);
                        _with3.Add("PACK_TYPE_MST_FK_IN", dr["PACK_TYPE_MST_FK"]);
                        _with3.Add("PACK_COUNT_IN", dr["PACK_COUNT"]);
                        _with3.Add("COMMODITY_MST_FK_IN", dr["COMMODITY_MST_FK"]);
                        if ((string.IsNullOrEmpty(dr["COMMODITY_MST_FKS"].ToString())))
                        {
                            _with3.Add("COMMODITY_MST_FKS_IN", DBNull.Value);
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(dr["COMMODITY_MST_FKS"].ToString()))
                            {
                                _with3.Add("COMMODITY_MST_FKS_IN", DBNull.Value);
                            }
                            else
                            {
                                _with3.Add("COMMODITY_MST_FKS_IN", dr["COMMODITY_MST_FKS"]);
                            }
                        }
                        _with3.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MBL_TRN_EXP_CONT_PK").Direction = ParameterDirection.Output;
                        Execute = Convert.ToInt16(_with1.ExecuteNonQuery());
                    }
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                    throw oraexp;
                }

                if (status == 1)
                {
                    arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(CurMblPk), 2, 1, "MBL", "MBL-INS", Convert.ToInt32(nLocationfk), objWK, "INS", CREATED_BY, "O");
                }
                // End
                MblPk = CurMblPk;
                if (Execute > 0)
                {
                    if (strTransaction.Trim().Length > 0)
                    {
                        if (strTransaction != "undefined")
                        {
                            arrMessage = (ArrayList)SaveTransaction(strTransaction, objWK.MyCommand, Convert.ToInt32(MblPk));
                        }
                        else if (strTransaction.Trim().Length == 0 | strTransaction == "undefined")
                        {
                            arrMessage.Add("All Data Saved Successfully");
                        }
                    }
                    else
                    {
                        arrMessage.Add("All Data Saved Successfully");
                    }

                    if (arrMessage.Count >= 1)
                    {
                        TRAN.Commit();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                    }
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            return new ArrayList();
        }

        public bool CheckUniqueMBL(string MBLno, string MBLpk)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                string strSql = null;
                int count = 0;

                if (MBLno.Trim().Length > 0 & MBLpk == "0")
                {
                    strSql = "SELECT COUNT(*) FROM mbl_exp_tbl m WHERE m.mbl_ref_no='" + MBLno + "'";
                    count = Convert.ToInt16(objWK.ExecuteScaler(strSql));
                    if (count > 0)
                    {
                        return false;
                    }
                    else
                    {
                        return true;
                    }
                }
                else
                {
                    return true;
                }
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

        #endregion " Save And Update "

        #region " Save BL Clause"

        private object SaveTransaction(string str, OracleCommand InsertCmd, int Strpk)
        {
            Array arrRow = null;
            Array arrMain = null;
            string strRow = null;
           DataSet dsTran = new DataSet();
            DataRow dRow = null;
            DataTable dtTran = null;
            int nStart = 0;
            string nEnd = null;
            Int32 RecAfct = default(Int32);
            nEnd = Convert.ToString(str.Length);
            str = str.Substring(0, Convert.ToInt32(nEnd) - 2);
            dsTran.Tables.Add("dtTran");
            dsTran.Tables["dtTran"].Columns.Add("MBL_EXP_TBL_FK");
            dsTran.Tables["dtTran"].Columns.Add("BL_CLAUSE_FK");
            dsTran.Tables["dtTran"].Columns.Add("BL_DESCRIPTION");
            arrMain = str.Split('#');
            for (int arrLen = 0; arrLen <= arrMain.Length - 1; arrLen++)
            {
                strRow = Convert.ToString(arrMain.GetValue(arrLen));
                arrRow = strRow.Split('^');
                dRow = dsTran.Tables["dtTran"].NewRow();
                if (Convert.ToString(arrRow.GetValue(0)) == "null")
                {
                    dRow["MBL_EXP_TBL_FK"] = System.DBNull.Value;
                }
                else
                {
                    dRow["MBL_EXP_TBL_FK"] = Convert.ToString(arrRow.GetValue(0));
                }

                dRow["BL_DESCRIPTION"] = arrRow.GetValue(1);

                if (Convert.ToString(arrRow.GetValue(2)) == "null")
                {
                    dRow["BL_CLAUSE_FK"] = System.DBNull.Value;
                }
                else
                {
                    dRow["BL_CLAUSE_FK"] = arrRow.GetValue(2);
                }
                dsTran.Tables["dtTran"].Rows.Add(dRow);
            }

            try
            {
                WorkFlow objWK = new WorkFlow();
                var _with4 = InsertCmd;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".MBL_BL_CLAUSE_TBL_PKG.MBL_BL_CLAUSE_TBL_INS";
                _with4.Parameters.Clear();
                _with4.Parameters.Add("MBL_EXP_TBL_FK_IN", Strpk);
                _with4.Parameters.Add("BL_DESCRIPTION_IN", OracleDbType.NVarchar2, 500, "BL_DESCRIPTION");
                _with4.Parameters.Add("BL_CLAUSE_FK_IN", OracleDbType.Int32, 10, "BL_CLAUSE_FK");
                _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MBL_BL_CLAUSE_TBL_PK").Direction = ParameterDirection.Output;
                
                var _with5 = objWK.MyDataAdapter;
                _with5.InsertCommand = InsertCmd;
                RecAfct = _with5.Update(dsTran.Tables["dtTran"]);
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
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

        #endregion " Save BL Clause"

        #region " Other Functions"

        public int FindMBLPk(string RefNo)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select m.mbl_exp_tbl_pk from mbl_exp_tbl m where m.mbl_ref_no = '" + RefNo + "' ";
                string MBLPK = null;
                MBLPK = objWF.ExecuteScaler(strSQL);
                return Convert.ToInt32(MBLPK);
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

        public DataSet FetchShippingTerms()
        {
            try
            {
                string strSQL = null;
                DataSet dsTerms = new DataSet();
                WorkFlow objWF = new WorkFlow();
                strSQL = "SELECT SHIPPING_TERMS_MST_PK,INCO_CODE  FROM SHIPPING_TERMS_MST_TBL WHERE ACTIVE_FLAG = 1";
                dsTerms = objWF.GetDataSet(strSQL);
                return dsTerms;
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

        public DataSet FetchFreightTerms()
        {
            try
            {
                string strSQL = null;
                DataSet dsTerms = new DataSet();
                WorkFlow objWF = new WorkFlow();
                strSQL = "SELECT FREIGHT_TERMS_MST_PK,FREIGHT_TERMS  FROM FREIGHT_TERMS_MST_TBL WHERE ACTIVE_FLAG = 1";
                dsTerms = objWF.GetDataSet(strSQL);
                return dsTerms;
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

        public DataSet FetchHBLRef(string HblKey)
        {
            try
            {
                string strSQL = null;
                DataSet dsHbl = new DataSet();
                WorkFlow objWF = new WorkFlow();
                strSQL = "select HBL_REF_NO from HBL_EXP_TBL where HBL_EXP_TBL_PK IN " + "(" + HblKey + ")";
                dsHbl = objWF.GetDataSet(strSQL);
                return dsHbl;
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

        public DataSet FetchMJCRef(string MjcFKey)
        {
            try
            {
                string strSQL = null;
                DataSet dsHbl = new DataSet();
                WorkFlow objWF = new WorkFlow();
                strSQL = "select H.HBL_REF_NO from HBL_EXP_TBL H,JOB_CARD_TRN j where J.JOB_CARD_TRN_PK = H.JOB_CARD_SEA_EXP_FK AND j.MASTER_JC_FK In " + "(" + MjcFKey + ")";
                dsHbl = objWF.GetDataSet(strSQL);
                return dsHbl;
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

        public DataSet FetchMRef(string MjcFKey)
        {
            try
            {
                string strSQL = null;
                DataSet dsHbl = new DataSet();
                WorkFlow objWF = new WorkFlow();
                strSQL = "SELECT H.HBL_REF_NO FROM HBL_EXP_TBL H, JOB_CARD_TRN J WHERE J.JOB_CARD_TRN_PK = H.JOB_CARD_SEA_EXP_FK AND J.MBL_MAWB_FK In " + "(" + MjcFKey + ")";
                dsHbl = objWF.GetDataSet(strSQL);
                return dsHbl;
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

        public DataSet FetchJRef(string MjcFKey)
        {
            try
            {
                string strSQL = null;
                DataSet dsHbl = new DataSet();
                WorkFlow objWF = new WorkFlow();
                strSQL = "select JOB.JOBCARD_REF_NO from JOB_CARD_TRN JOB  where JOB.JOB_CARD_TRN_PK In " + "(" + MjcFKey + ")";
                dsHbl = objWF.GetDataSet(strSQL);
                return dsHbl;
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

        public string FetchPlace(int pldfk)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                //add by latha
                strSQL = "select PMT.PLACE_NAME from PLACE_MST_TBL PMT where PMT.PLACE_PK  In " + "(" + pldfk + ")";
                string Place = null;
                Place = objWF.ExecuteScaler(strSQL);
                return Place;
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

        //For Clubbling the Marks and Numbers and Goods Description
        // when Job Cards are Consolidated
        public DataSet FetchMarksNrsAndGoodsDesc(string pks)
        {
            try
            {
                string strSQL = null;
                DataSet dsMarksNrs = new DataSet();
                WorkFlow objWF = new WorkFlow();
                strSQL = "select c.jobcard_ref_no, c.marks_numbers, c.goods_description" + "from JOB_CARD_TRN c" + "where c.MASTER_JC_FK in(" + pks + ") order by jobcard_ref_no";
                dsMarksNrs = objWF.GetDataSet(strSQL);
                return dsMarksNrs;
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

        //Fetch Shipper Address and
        //      Consignee Address   while Fetching through Consolidation
        public DataSet FetchShipperAndConsignee(string pk)
        {
            string strSql = null;
            try
            {
                strSql = "select a.customer_mst_pk," + "a.customer_name," + "c.adm_address_1," + "c.adm_address_2," + "c.adm_address_3," + "c.adm_city," + "c.adm_zip_code," + "c.adm_country_mst_fk" + "from customer_mst_tbl a, customer_contact_dtls c" + "where a.customer_mst_pk = c.customer_mst_fk" + "and a.customer_mst_pk = " + pk + "UNION" + "select a.customer_mst_pk," + "a.customer_name," + "c.adm_address_1," + "c.adm_address_2," + "c.adm_address_3," + "c.adm_city," + "c.adm_zip_code," + "c.adm_country_mst_fk" + "from TEMP_CUSTOMER_TBL a, TEMP_CUSTOMER_CONTACT_DTLS c" + "where a.customer_mst_pk = c.customer_mst_fk" + "and a.customer_mst_pk = " + pk ;

                return objWF.GetDataSet(strSql);
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

        //Fetch DP Agent Address
        //DP agent Pk Should be passed from consolidation
        public DataSet FetchDPAgent(string pk)
        {
            string strSql = null;

            try
            {
                strSql = "select a.agent_mst_pk," + "a.agent_name," + "c.adm_address_1," + "c.adm_address_2," + "c.adm_address_3," + "c.adm_city," + "c.adm_zip_code," + "c.adm_country_mst_fk" + "from agent_mst_tbl a, agent_contact_dtls c" + "where a.agent_mst_pk = c.agent_mst_fk" + "and a.agent_mst_pk = " + pk ;

                return objWF.GetDataSet(strSql);
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

        //Fetching Agent Address To Fill the Agent Address Details
        public DataSet FetchShipAgt(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID");
            StrSqlBuilder.Append("  FROM LOCATION_MST_TBL L");
            StrSqlBuilder.Append("  WHERE  L.LOCATION_MST_PK = " + Loc + "");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        //Fetching Agent Address To Fill the Agent Address Details
        public DataSet FETCHagent(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID");
            StrSqlBuilder.Append("  FROM LOCATION_MST_TBL L");
            StrSqlBuilder.Append("  WHERE  L.LOCATION_MST_PK = " + Loc + "");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        //Fetching Agent Address To Fill the Agent Address Details
        public DataSet FetchCongAgt(long Jobpk)
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
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Fetching Location Wise Adderess When User Want to Change the Address of Agent,... to EFS
        //This facility is Enabled on Consolidation of multiple HBL's(Job Cards)
        public DataSet FETCHPOLEFS(ref int pol)
        {
            string strSql = null;
            strSql = "SELECT DISTINCT LOC.ADDRESS_LINE1,LOC.ADDRESS_LINE2,LOC.ADDRESS_LINE3,Loc.Office_Name" + " FROM LOCATION_MST_TBL LOC, PORT_MST_TBL POL " + " WHERE(pol.LOCATION_MST_FK = Loc.LOCATION_MST_PK)" + "AND POL.PORT_MST_PK=" + pol;
            try
            {
                return objWF.GetDataSet(strSql);
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

        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        private object ifDateNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return Convert.ToDateTime(col);
            }
        }

        #endregion " Other Functions"

        #region " Enhance Search Functionality"

        public string FetchForHblRefInMbl(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strLoc = loc;
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_FOR_MBL_PKG.GET_HBL_REF_FOR_MBL";
                var _with6 = SCM.Parameters;
                _with6.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
            finally
            {
                SCM.Connection.Close();
            }
        }

        //Added By Rijesh for Job Card Fetch In MBL Entry Screen.
        //This is unique for MBL Entry.
        public string FetchJobInMblEntry(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strLoc = loc;
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_MBL_REF_NO_PKG.GET_JOB_REF_MBL";
                var _with7 = SCM.Parameters;
                _with7.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
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
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
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

        //Added by Faheem
        public string FetchMJCInMblEntry(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            string From = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            From = Convert.ToString(arr.GetValue(4));
            strLoc = loc;
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_MBL_REF_NO_PKG.GET_MJC_REF_MBL";
                var _with8 = SCM.Parameters;
                _with8.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with8.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with8.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with8.Add("FROM_IN", From).Direction = ParameterDirection.Input;
                _with8.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
            finally
            {
                SCM.Connection.Close();
            }
        }

        //End

        #endregion " Enhance Search Functionality"

        #region " Enhance search for MHAWB JOB REF NO"

        public string FetchForMAWBJobRef(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_MBL_REF_NO_PKG.GET_JOB_MBL_REF_COMMON";
                var _with9 = SCM.Parameters;
                _with9.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with9.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
            finally
            {
                SCM.Connection.Close();
            }
        }

        #endregion " Enhance search for MHAWB JOB REF NO"

        #region " MBL Instruction"

        public DataSet FetchMblInstructionData(int MBLPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT DISTINCT MBL.MBL_EXP_TBL_PK MBLPK,";
            strSQL += "MBL.MBL_REF_NO MBLNO,";
            strSQL += "MBL.MBL_DATE MBLDATE,";
            strSQL += "MBL.SHIPPER_NAME SHIPPER,";
            strSQL += "MBL.SHIPPER_ADDRESS SHIPPERADDRESS,";
            strSQL += "MBL.CONSIGNEE_NAME CONSIGNEE,";
            strSQL += "MBL.CONSIGNEE_ADDRESS CONSIGNEEADDRESS,";
            strSQL += "OMST.OPERATOR_NAME OPERATORNAME,";
            strSQL += "OMSTDTLS.ADM_CONTACT_PERSON CONTACTPERSON,";
            strSQL += "OMSTDTLS.ADM_FAX_NO FAX,";
            strSQL += "POL.PORT_NAME POLNAME,";
            strSQL += "POD.PORT_NAME PODNAME,";
            strSQL += "JSE.JOB_CARD_TRN_PK JOBPK,";
            strSQL += "JSE.JOBCARD_REF_NO JOBNO,";
            strSQL += "(VVTBL.VESSEL_NAME || '-' || VVTRN.VOYAGE) VESSEL,";
            strSQL += " JSE.UCR_NO UCRNO,";
            strSQL += "MBL.goods_description GOODS,";

            strSQL += "(MBL.NOTIFY_NAME || chr(10) || MBL.NOTIFY_ADDRESS)  NOTIFY1_DETAILS,";
            strSQL += "(NOT2.CUSTOMER_NAME || CASE WHEN NOT2.CUSTOMER_NAME IS NULL THEN '' ELSE ',' END|| NOTCON2.ADM_ADDRESS_1 ||CASE WHEN NOTCON2.ADM_ADDRESS_1 IS NULL THEN '' ELSE ',' END|| NOTCON2.ADM_ADDRESS_2||CASE WHEN NOTCON2.ADM_ADDRESS_2 IS NULL THEN '' ELSE ',' END|| NOTCON2.ADM_ADDRESS_3) NOTIFY2_DETAILS,";
            strSQL += "MBL.PYMT_TYPE PAYMENT_TYPE,";
            strSQL += "HBL.HBL_REF_NO BILL_INSTRUCTIONS,";
            strSQL += "MBL.marks_numbers MARKS";
            strSQL += " FROM MBL_EXP_TBL MBL,";
            strSQL += "JOB_CARD_TRN JSE,";
            strSQL += "JOB_TRN_CONT JTSE,";
            strSQL += "PORT_MST_TBL POL,";
            strSQL += "PORT_MST_TBL POD,";
            strSQL += "HBL_EXP_TBL HBL,";
            strSQL += "OPERATOR_MST_TBL OMST,";
            strSQL += "OPERATOR_CONTACT_DTLS OMSTDTLS,";
            strSQL += "VESSEL_VOYAGE_TRN VVTRN,";
            strSQL += "VESSEL_VOYAGE_TBL VVTBL,";

            strSQL += "CUSTOMER_MST_TBL   NOT1,";
            strSQL += "CUSTOMER_MST_TBL   NOT2,";
            strSQL += "CUSTOMER_CONTACT_DTLS NOTCON1,";
            strSQL += "CUSTOMER_CONTACT_DTLS NOTCON2";
            strSQL += "WHERE POL.PORT_MST_PK(+)=MBL.PORT_MST_POL_FK";
            strSQL += "AND POD.PORT_MST_PK(+)=MBL.PORT_MST_POD_FK ";
            strSQL += "AND JSE.MBL_MAWB_FK(+)=MBL.MBL_EXP_TBL_PK ";
            strSQL += "AND JTSE.JOB_CARD_TRN_FK(+)=JSE.JOB_CARD_TRN_PK  ";
            strSQL += "AND HBL.JOB_CARD_SEA_EXP_FK(+)=JSE.JOB_CARD_TRN_PK ";
            strSQL += "AND OMST.OPERATOR_MST_PK(+)=MBL.OPERATOR_MST_FK";
            strSQL += "AND OMSTDTLS.OPERATOR_MST_FK(+)=OMST.OPERATOR_MST_PK";
            strSQL += "AND MBL.VOYAGE_TRN_FK = VVTRN.VOYAGE_TRN_PK(+)";
            strSQL += "AND VVTRN.VESSEL_VOYAGE_TBL_FK= VVTBL.VESSEL_VOYAGE_TBL_PK(+)";

            strSQL += "AND NOT1.CUSTOMER_MST_PK(+) = JSE.NOTIFY1_CUST_MST_FK";
            strSQL += "AND NOT2.CUSTOMER_MST_PK(+) = JSE.NOTIFY2_CUST_MST_FK";
            strSQL += "AND NOT1.CUSTOMER_MST_PK = NOTCON1.CUSTOMER_MST_FK(+)";
            strSQL += "AND NOT2.CUSTOMER_MST_PK = NOTCON2.CUSTOMER_MST_FK(+)";

            strSQL += "AND MBL.MBL_EXP_TBL_PK=" + MBLPK;
            try
            {
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

        public DataSet FetchBlClauses(Int32 MBLPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT ";
            Strsql += " MBL.BL_DESCRIPTION";
            Strsql += " FROM";
            Strsql += " MBL_BL_CLAUSE_TBL MBL";
            Strsql += " WHERE";
            Strsql += " MBL.MBL_EXP_TBL_FK =" + MBLPK;
            try
            {
                return Objwk.GetDataSet(Strsql);
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

        public DataSet FetchLogAddressDtl(long LOC)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.CITY,");
            StrSqlBuilder.Append("  L.TELE_PHONE_NO,L.Fax_No,L.E_MAIL_ID,L.ZIP,CTY.COUNTRY_NAME ");
            StrSqlBuilder.Append("  from location_mst_tbl L,country_mst_tbl cty");
            StrSqlBuilder.Append("  where L.LOCATION_MST_PK = " + LOC + "");
            StrSqlBuilder.Append("  and cty.country_mst_pk = l.country_mst_fk ");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        public DataSet FetchMblCntDetailsData(int MBLPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT CNTNUMBER,";
            strSQL += "SEALNUMBER,";
            strSQL += "CNTYPE,";
            strSQL += "NET_WEIGHT,";
            strSQL += "GROSS_WEIGHT,";
            strSQL += "VOLUME_IN_CBM";
            strSQL += " FROM ( SELECT DISTINCT CONTAINER.CONTAINER_NUMBER CNTNUMBER,";
            strSQL += "CONTAINER.SEAL_NUMBER SEALNUMBER,";
            strSQL += "CONTYPE.CONTAINER_TYPE_MST_ID CNTYPE,";
            strSQL += "CONTAINER.NET_WEIGHT,";
            strSQL += "CONTAINER.GROSS_WEIGHT,";
            strSQL += "CONTAINER.VOLUME_IN_CBM,";
            strSQL += "CONTYPE.PREFERENCES";
            strSQL += " FROM";
            strSQL += "CONTAINER_TYPE_MST_TBL CONTYPE,";
            strSQL += "MBL_TRN_EXP_CONTAINER CONTAINER";
            strSQL += "WHERE CONTAINER.MBL_EXP_TBL_FK =" + MBLPK;
            strSQL += "AND CONTYPE.CONTAINER_TYPE_MST_PK = CONTAINER.CONTAINER_TYPE_MST_FK";
            strSQL += " ORDER BY CONTYPE.PREFERENCES)";
            try
            {
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

        #endregion " MBL Instruction"

        #region "Fetch Amount Fields "

        public object FetchInsAmtsFromJCthruHbl(string hblKeys, int BaseCurrency)
        {
            try
            {
                StringBuilder Str = new StringBuilder();
                DataSet dsInsureAmt = new DataSet();
                Str.Append(" select round(get_ex_rate(jc.insurance_currency," + BaseCurrency + ",sysdate) * sum(jc.insurance_amt),2) ");
                Str.Append("  from JOB_CARD_TRN jc  Where ");
                Str.Append(" jc.HBL_HAWB_FK in ( " + hblKeys + ") ");
                Str.Append("  group by jc.insurance_currency ");
                dsInsureAmt = objWF.GetDataSet(Str.ToString());
                return dsInsureAmt;
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

        public object FetchPrePaidChgsThruHbl(string hblKeys)
        {
            StringBuilder str = new StringBuilder();
            DataSet dsPrePaidChrgs = new DataSet();
            try
            {
                str.Append(" Select round(sum(amt), 2) from ");
                str.Append(" (select sum(jd.exchange_rate * jd.freight_amt) as amt ");
                str.Append(" from JOB_TRN_FD jd,JOB_CARD_TRN jb ");
                str.Append(" where jd.freight_type = 1 ");
                str.Append(" and jb.HBL_HAWB_FK in (" + hblKeys + " ) ");
                str.Append(" and jb.JOB_CARD_TRN_PK = jd.JOB_CARD_TRN_FK ");
                str.Append(" union ");
                str.Append(" select sum(jo.exchange_rate * jo.amount) as amt  ");
                str.Append(" from JOB_TRN_OTH_CHRG jo ,JOB_CARD_TRN jb1 ");
                str.Append(" where jb1.HBL_HAWB_FK in (" + hblKeys + " ) ");
                str.Append(" and jb1.JOB_CARD_TRN_PK = jo.JOB_CARD_TRN_FK  ");
                str.Append(" and jo.freight_type = 1 ) ");
                dsPrePaidChrgs = objWF.GetDataSet(str.ToString());
                return dsPrePaidChrgs;
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

        public object FetchCollectChrgsThruHbl(string hblKeys)
        {
            try
            {
                StringBuilder str = new StringBuilder();
                DataSet dsCollectChrgs = new DataSet();
                str.Append(" Select round(sum(amt), 2) from ");
                str.Append(" (select sum(jd.freight_amt) as amt ");
                str.Append(" from JOB_TRN_FD jd,JOB_CARD_TRN jb ");
                str.Append(" where jd.freight_type = 2 ");
                str.Append(" and jb.HBL_HAWB_FK in (" + hblKeys + " ) ");
                str.Append(" and jb.JOB_CARD_TRN_PK = jd.JOB_CARD_TRN_FK ");
                str.Append(" union ");
                str.Append(" select sum(jo.amount) as amt  ");
                str.Append(" from JOB_TRN_OTH_CHRG jo ,JOB_CARD_TRN jb1 ");
                str.Append(" where jb1.HBL_HAWB_FK in (" + hblKeys + " ) ");
                str.Append(" and jb1.JOB_CARD_TRN_PK = jo.JOB_CARD_TRN_FK  ");
                str.Append(" and jo.freight_type = 2 ) ");
                dsCollectChrgs = objWF.GetDataSet(str.ToString());
                return dsCollectChrgs;
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

        public DataSet FetchInsureAmtsFromJobCard(string jobPks, int BaseCurrency, string MblDate)
        {
            try
            {
                StringBuilder Str = new StringBuilder();
                DataSet dsInsureAmt = new DataSet();
                Str.Append(" select round(get_ex_rate(jc.insurance_currency," + BaseCurrency + ",sysdate) * sum(jc.insurance_amt),2) ");
                Str.Append("  from JOB_CARD_TRN jc  Where ");
                Str.Append(" jc.JOB_CARD_TRN_PK in ( " + jobPks + ") ");
                Str.Append("  group by jc.insurance_currency ");
                dsInsureAmt = objWF.GetDataSet(Str.ToString());
                return dsInsureAmt;
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

        public DataSet FetchPrePaidChgs(string JobPks, int BaseCurrency)
        {
            try
            {
                StringBuilder str = new StringBuilder();
                DataSet dsPrePaidChrgs = new DataSet();
                str.Append("  Select round(sum(amt), 2) ");
                str.Append("from (select sum(jd.exchange_rate * ");
                str.Append("     jd.freight_amt) as amt ");
                str.Append("  from JOB_TRN_FD jd ");
                str.Append(" where jd.freight_type = 1 ");
                str.Append("  and jd.JOB_CARD_TRN_FK in (" + JobPks + ")  ");
                str.Append(" union ");
                str.Append("  select sum(jo.exchange_rate * jo.amount) as amt ");
                str.Append("  from JOB_TRN_OTH_CHRG jo ");
                str.Append("    where jo.JOB_CARD_TRN_FK in (" + JobPks + ") ");
                str.Append("    and jo.freight_type = 1 ) ");
                dsPrePaidChrgs = objWF.GetDataSet(str.ToString());
                return dsPrePaidChrgs;
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

        public object FetchCollectChrgs(string JobPks, int BaseCurrency)
        {
            try
            {
                StringBuilder str = new StringBuilder();
                DataSet dsCollectChrgs = new DataSet();
                str.Append("  Select round(sum(amt), 2) ");
                str.Append("from (select sum(jd.freight_amt) as amt ");
                str.Append("  from JOB_TRN_FD jd ");
                str.Append(" where(jd.freight_type = 2) ");
                str.Append("  and jd.JOB_CARD_TRN_FK in (" + JobPks + ")  ");
                str.Append(" union ");
                str.Append("  select sum(jo.amount) as amt ");
                str.Append("  from JOB_TRN_OTH_CHRG jo ");
                str.Append("    where jo.JOB_CARD_TRN_FK in (" + JobPks + ") ");
                str.Append("    and jo.freight_type = 2 ) ");
                dsCollectChrgs = objWF.GetDataSet(str.ToString());
                return dsCollectChrgs;
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

        public DataSet FetchMJCPrePaidChgs(string MJCPK)
        {
            try
            {
                DataSet dsPrePaidChrgs = new DataSet();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append(" SELECT ROUND(SUM(AMT),2) AMT FROM (");
                sb.Append(" SELECT SUM(JF.EXCHANGE_RATE*JF.FREIGHT_AMT) AS AMT FROM MASTER_JC_SEA_EXP_TBL M, JOB_CARD_TRN J, JOB_TRN_FD JF ");
                sb.Append(" where m.master_jc_sea_exp_pk in (" + MJCPK + ") ");
                sb.Append(" AND J.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK");
                sb.Append(" AND JF.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK");
                sb.Append(" AND JF.FREIGHT_TYPE=1");
                sb.Append(" UNION");
                sb.Append(" SELECT SUM(JOTH.EXCHANGE_RATE*JOTH.AMOUNT) AS AMT FROM MASTER_JC_SEA_EXP_TBL M, JOB_CARD_TRN J, JOB_TRN_OTH_CHRG JOTH ");
                sb.Append(" where m.master_jc_sea_exp_pk in (" + MJCPK + ") ");
                sb.Append(" AND J.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK");
                sb.Append(" AND JOTH.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK");
                sb.Append(" AND JOTH.FREIGHT_TYPE=1)");

                dsPrePaidChrgs = objWF.GetDataSet(sb.ToString());
                return dsPrePaidChrgs;
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

        public DataSet FetchMJCCollectChrgs(string MJCPK)
        {
            try
            {
                DataSet dsPrePaidChrgs = new DataSet();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append(" SELECT ROUND(SUM(AMT),2) AMT FROM (");
                sb.Append(" SELECT SUM(JF.EXCHANGE_RATE*JF.FREIGHT_AMT) AS AMT FROM MASTER_JC_SEA_EXP_TBL M, JOB_CARD_TRN J, JOB_TRN_FD JF ");
                sb.Append(" where m.master_jc_sea_exp_pk in (" + MJCPK + ") ");
                sb.Append(" AND J.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK");
                sb.Append(" AND JF.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK");
                sb.Append(" AND JF.FREIGHT_TYPE=2");
                sb.Append(" UNION");
                sb.Append(" SELECT SUM(JOTH.EXCHANGE_RATE*JOTH.AMOUNT) AS AMT FROM MASTER_JC_SEA_EXP_TBL M, JOB_CARD_TRN J, JOB_TRN_OTH_CHRG JOTH ");
                sb.Append(" where m.master_jc_sea_exp_pk in (" + MJCPK + ") ");
                sb.Append(" AND J.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK");
                sb.Append(" AND JOTH.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK");
                sb.Append(" AND JOTH.FREIGHT_TYPE=2)");

                dsPrePaidChrgs = objWF.GetDataSet(sb.ToString());
                return dsPrePaidChrgs;
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

        public string FetchCurrId()
        {
            try
            {
                StringBuilder str = new StringBuilder();
                string CurrId = null;
                WorkFlow objWF = new WorkFlow();
                str.Append(" select cm.currency_id from corporate_mst_tbl cr,currency_type_mst_tbl cm where cr.currency_mst_fk = cm.currency_mst_pk");
                CurrId = objWF.ExecuteScaler(str.ToString());
                return CurrId;
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

        #endregion "Fetch Amount Fields "

        #region "Packing List Report"

        //Added by rabbani on 19/12/06 To print Packing List Report
        public DataSet FetchPackingListDetails(string From = "MBL", string MBLPK = "", string MJOBPK = "", long Loc = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                strQuery.Append("select");
                strQuery.Append(" JS.JOB_CARD_TRN_PK Job_Card_Sea_Exp_Pk,");
                strQuery.Append(" MBL.MBL_EXP_TBL_PK,");
                strQuery.Append(" HBL.HBL_EXP_TBL_PK,");
                strQuery.Append(" MSTSEA.MASTER_JC_SEA_EXP_PK,");
                strQuery.Append(" MBL.MBL_REF_NO,");
                strQuery.Append(" HBL.HBL_REF_NO,");
                strQuery.Append(" MSTSEA.MASTER_JC_REF_NO,");
                strQuery.Append(" MBLPOL.PORT_NAME POL,");
                strQuery.Append(" MBLPOD.PORT_NAME POD,");
                strQuery.Append(" MBLCNTPOL.COUNTRY_NAME CNTPOL,");
                strQuery.Append(" MBLCNTPOD.COUNTRY_NAME CNTPOD,");
                strQuery.Append(" VES.VESSEL_NAME VESSEL,");
                strQuery.Append(" VOY.VOYAGE,");
                strQuery.Append(" VOY.POL_ETD ETDPOL,");
                strQuery.Append(" VOY.POD_ETA ETAPOD,");
                strQuery.Append(" CONTAINER.CONTAINER_NUMBER,");
                strQuery.Append(" CONTYPE.CONTAINER_TYPE_MST_ID,");
                strQuery.Append(" CASE WHEN MBL.AGENT_NAME IS NULL THEN MBL.SHIPPER_NAME ELSE MBL.AGENT_NAME END AGENT_NAME, ");
                strQuery.Append(" CASE WHEN MBL.AGENT_ADDRESS IS NULL THEN MBL.SHIPPER_ADDRESS ELSE MBL.AGENT_ADDRESS END AGENT_ADDRESS, ");
                strQuery.Append(" MBL.SHIPPER_NAME MBLSHIPPER_NAME,");
                strQuery.Append(" MBL.SHIPPER_ADDRESS MBLSHIPPER_ADDRESS,");
                strQuery.Append(" MBL.GOODS_DESCRIPTION MBLGOODS_DESCRIPTION,");
                strQuery.Append(" MBL.MARKS_NUMBERS MBLMARKS_NUMBERS,");
                strQuery.Append(" MBL.TOTAL_GROSS_WEIGHT MBLGRWT,");
                strQuery.Append(" MBL.TOTAL_VOLUME MBLVLM,");
                strQuery.Append(" HBLCUS.CUSTOMER_NAME HBLSHIPPER_NAME,");
                strQuery.Append(" HBLADD.ADM_ADDRESS_1 HBLSHIPPER_ADD1,");
                strQuery.Append(" HBLADD.ADM_ADDRESS_2 HBLSHIPPER_ADD2,");
                strQuery.Append(" HBLADD.ADM_ADDRESS_3 HBLSHIPPER_ADD3,");
                strQuery.Append(" HBLADD.ADM_CITY HBLSHIPPER_CITY,");
                strQuery.Append(" HBLADD.ADM_ZIP_CODE HBLSHIPPER_ZIP,");
                strQuery.Append(" HBLADD.ADM_PHONE_NO_1 HBLSHIPPER_PHONE,");
                strQuery.Append(" HBLADD.ADM_FAX_NO HBLSHIPPER_FAX,");
                strQuery.Append(" HBLADD.ADM_EMAIL_ID HBLSHIPPER_EMAIL,");
                strQuery.Append(" HBL.GOODS_DESCRIPTION HBLGOODS_DESCRIPTION,");
                strQuery.Append(" HBL.MARKS_NUMBERS HBLMARKS_NUMBERS,");
                strQuery.Append(" HBL.TOTAL_GROSS_WEIGHT HBLGRWT,");
                strQuery.Append(" HBL.TOTAL_VOLUME HBLVLM");

                strQuery.Append("  from JOB_CARD_TRN JS,");
                strQuery.Append("       MBL_EXP_TBL            MBL,");
                strQuery.Append("       HBL_EXP_TBL            HBL,");
                strQuery.Append("       MASTER_JC_SEA_EXP_TBL  MSTSEA,");
                strQuery.Append("       PORT_MST_TBL           MBLPOL,");
                strQuery.Append("       PORT_MST_TBL           MBLPOD,");
                strQuery.Append("       COUNTRY_MST_TBL        MBLCNTPOL,");
                strQuery.Append("       COUNTRY_MST_TBL        MBLCNTPOD,");
                strQuery.Append("       VESSEL_VOYAGE_TRN      VOY,");
                strQuery.Append("       VESSEL_VOYAGE_TBL      VES,");
                strQuery.Append("       MBL_TRN_EXP_CONTAINER  CONTAINER,");
                strQuery.Append("       CONTAINER_TYPE_MST_TBL CONTYPE,");
                strQuery.Append("       CUSTOMER_MST_TBL       HBLCUS,");
                strQuery.Append("       CUSTOMER_CONTACT_DTLS  HBLADD,");
                strQuery.Append("       LOCATION_MST_TBL       L  ");

                strQuery.Append(" WHERE");
                strQuery.Append(" JS.HBL_HAWB_FK IS NOT NULL");
                strQuery.Append(" AND JS.MBL_MAWB_FK IS NOT NULL");
                strQuery.Append(" AND MBL.MBL_EXP_TBL_PK(+) = JS.MBL_MAWB_FK");
                strQuery.Append(" AND HBL.HBL_EXP_TBL_PK(+) = JS.HBL_HAWB_FK");
                strQuery.Append(" AND MSTSEA.MASTER_JC_SEA_EXP_PK(+) = JS.MASTER_JC_FK");
                strQuery.Append(" AND MBLPOL.PORT_MST_PK(+) = MBL.PORT_MST_POL_FK");
                strQuery.Append(" AND MBLPOD.PORT_MST_PK(+) = MBL.PORT_MST_POD_FK");
                strQuery.Append(" AND MBLCNTPOL.COUNTRY_MST_PK(+) = MBLPOL.COUNTRY_MST_FK");
                strQuery.Append(" AND MBLCNTPOD.COUNTRY_MST_PK(+) = MBLPOD.COUNTRY_MST_FK");
                strQuery.Append(" AND VES.VESSEL_VOYAGE_TBL_PK(+) = VOY.VESSEL_VOYAGE_TBL_FK");
                strQuery.Append(" AND MBL.VOYAGE_TRN_FK = VOY.VOYAGE_TRN_PK(+)");
                strQuery.Append(" AND MBL.MBL_EXP_TBL_PK = CONTAINER.MBL_EXP_TBL_FK ");
                strQuery.Append(" AND CONTYPE.CONTAINER_TYPE_MST_PK(+) = CONTAINER.CONTAINER_TYPE_MST_FK");
                strQuery.Append(" AND HBLCUS.CUSTOMER_MST_PK(+) = HBL.SHIPPER_CUST_MST_FK");
                strQuery.Append(" AND HBLCUS.CUSTOMER_MST_PK = HBLADD.CUSTOMER_MST_FK(+)");
                strQuery.Append(" AND L.LOCATION_MST_PK= " + Loc);
                if (From == "MBL")
                {
                    strQuery.Append(" AND JS.MBL_MAWB_FK IN  (" + MBLPK + ")");
                }
                else
                {
                    strQuery.Append(" AND JS.MASTER_JC_FK = " + MJOBPK);
                }

                return objWF.GetDataSet(strQuery.ToString());
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

        #endregion "Packing List Report"

        #region "HBL Count"

        public int HBLCount(string MBLPK = "0", string MSJobPK = "0", string @from = "MBL")
        {
            try
            {
                string strSQL = null;
                Int32 totRecords = 0;
                WorkFlow objTotRecCount = new WorkFlow();
                strSQL = "SELECT COUNT(JS.JOB_CARD_TRN_PK) FROM JOB_CARD_TRN JS";
                strSQL += "WHERE JS.HBL_HAWB_FK IS NOT NULL";
                strSQL += "AND JS.MBL_MAWB_FK IS NOT NULL";
                if (@from == "MBL")
                {
                    strSQL += "AND JS.MBL_MAWB_FK  =" + MBLPK;
                }
                else
                {
                    strSQL += "AND JS.MASTER_JC_FK =" + MSJobPK;
                }
                //End If
                totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSQL.ToString()));
                return totRecords;
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

        #endregion "HBL Count"

        #region "Fetch Barcode Manager Pk"

        public int FetchBarCodeManagerPk(string Configid)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;

            try
            {
                WorkFlow objWF = new WorkFlow();
                //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"

                StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt" + "where bdmt.config_id_fk= '" + Configid + "'";
                DsBarManager = objWF.GetDataSet(StrSql);
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with10 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToInt32(_with10["bcd_mst_pk"]);
                }
                return strReturn;
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

        #endregion "Fetch Barcode Manager Pk"

        #region "Fetch Barcode Type"

        public DataSet FetchBarCodeField(int BarCodeManagerPk)
        {
            try
            {
                string StrSql = null;
                DataSet DsBarManager = null;
                int strReturn = 0;
                WorkFlow objWF = new WorkFlow();
                StringBuilder strQuery = new StringBuilder();

                strQuery.Append("select bdmt.bcd_mst_pk, bdmt.field_name, bdmt.default_value");
                strQuery.Append("  from barcode_data_mst_tbl bdmt");
                //, barcode_doc_data_tbl bddt
                //strQuery.Append(" where bdmt.bcd_mst_pk = bddt.bcd_mst_fk and" & vbCrLf)
                strQuery.Append(" where bdmt.BCD_MST_FK= " + BarCodeManagerPk);
                strQuery.Append(" and bdmt.default_value = 1 ORDER BY default_value desc");

                // StrSql = "select bdmt.bcd_mst_pk, bdmt.field_name ,bdmt.default_value from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt where bdmt.bcd_mst_pk=bddt.bcd_mst_fk and bdmt.BCD_MST_FK=" & BarCodeManagerPk
                DsBarManager = objWF.GetDataSet(strQuery.ToString());
                return DsBarManager;
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

        #endregion "Fetch Barcode Type"

        #region "Fetch Booking Nr"

        public string FetchBookingNr(string MblRefnr)
        {
            try
            {
                string strQuery = null;
                DataSet DsBarManager = null;
                string strReturn = null;
                WorkFlow objWF = new WorkFlow();

                strQuery = "select bok.booking_ref_no from  mbl_exp_tbl hbl, JOB_CARD_TRN job,BOOKING_MST_TBL bok" + "where  job.MBL_MAWB_FK = hbl.mbl_exp_tbl_pk " + "and job.BOOKING_MST_FK = bok.BOOKING_MST_PK" + "and hbl.mbl_ref_no ='" + MblRefnr + "'";

                DsBarManager = objWF.GetDataSet(strQuery);
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with11 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToString(_with11["booking_ref_no"]);
                }
                return strReturn;
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

        #endregion "Fetch Booking Nr"

        #region "Fetch jobcard Nr"

        public string FetchJobCardNr(string MblPk)
        {
            try
            {
                string strQuery = null;
                DataSet DsBarManager = null;
                string strReturn = null;

                WorkFlow objWF = new WorkFlow();
                strQuery = "select job.jobcard_ref_no from  mbl_exp_tbl hbl, JOB_CARD_TRN job,BOOKING_MST_TBL bok" + "where  job.MBL_MAWB_FK = hbl.mbl_exp_tbl_pk " + "and job.BOOKING_MST_FK = bok.BOOKING_MST_PK" + "and mbl_exp_tbl_pk ='" + MblPk + "'";

                DsBarManager = objWF.GetDataSet(strQuery);
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with12 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToString(_with12["jobcard_ref_no"]);
                }
                return strReturn;
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

        #endregion "Fetch jobcard Nr"

        #region "Update Container"

        //The Following Code Added By ANand to reflect the container number to other Master Job Cards
        public string UpdateContainer(Int32 MstPk, string Container, string SealNumber, string ContainerPk, string JobcardRefNo)
        {
            try
            {
                string sqlStr = "";
                string strReturn = "";
                Int16 ContPk = 0;
                WorkFlow objWF = new WorkFlow();
                if (string.IsNullOrEmpty(ContainerPk))
                {
                    ContPk = 0;
                    sqlStr = "update JOB_TRN_CONT f set f.container_number = '" + Container + "', f.seal_number = '" + SealNumber + "'" + "where f.JOB_CARD_TRN_FK in (select cnt.JOB_CARD_TRN_PK from JOB_CARD_TRN cnt" + "where cnt.JOBCARD_REF_NO = '" + JobcardRefNo + "')";
                    strReturn = objWF.ExecuteScaler(sqlStr);
                }
                else
                {
                    ContPk = Convert.ToInt16(ContainerPk);
                    sqlStr = "update JOB_TRN_CONT f set f.container_number = '" + Container + "',CONTAINER_TYPE_MST_FK='" + ContPk + "', f.seal_number = '" + SealNumber + "'" + "where f.JOB_CARD_TRN_FK in (select cnt.JOB_CARD_TRN_PK from JOB_CARD_TRN cnt" + "where cnt.JOBCARD_REF_NO = '" + JobcardRefNo + "')";
                    strReturn = objWF.ExecuteScaler(sqlStr);
                }
                return strReturn;
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

        #endregion "Update Container"

        //------Added for Import Side MBL
        //added by mani for Imp. MBL
        public string FetchForHblRefInMbl_IMP(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strLoc = loc;
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_FOR_MBL_PKG.GET_HBL_REF_FOR_MBL_IMP";
                var _with13 = SCM.Parameters;
                _with13.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with13.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with13.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with13.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
            finally
            {
                SCM.Connection.Close();
            }
        }

        //end

        public string FetchJobInMblEntry_IMP(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strLoc = loc;
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_MBL_REF_NO_PKG.GET_JOB_REF_MBL_IMP";
                var _with14 = SCM.Parameters;
                _with14.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with14.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with14.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
            finally
            {
                SCM.Connection.Close();
            }
        }

        //----- END----
        //Added by Faheem

        #region " Fetch From Master JobCard"

        public DataSet FetchMJCMain(string MJCPk = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT O.OPERATOR_MST_PK,");
            sb.Append("       O.OPERATOR_NAME,");
            sb.Append("       POL.PORT_MST_PK      POLPK,");
            sb.Append("       POL.PORT_ID          POL,");
            sb.Append("       POD.PORT_MST_PK      PODPK,");
            sb.Append("       POD.PORT_ID POD ,");
            sb.Append("       V.VOYAGE_TRN_PK,");
            sb.Append("       VY.VESSEL_ID,");
            sb.Append("       VY.VESSEL_NAME,");
            sb.Append("       V.VOYAGE,");
            sb.Append("       V.POL_ETD,");
            sb.Append("       M.COMMODITY_GROUP_FK,M.DP_AGENT_MST_FK");
            sb.Append("  FROM MASTER_JC_SEA_EXP_TBL M,");
            sb.Append("       OPERATOR_MST_TBL      O,");
            sb.Append("       PORT_MST_TBL          POL,");
            sb.Append("       PORT_MST_TBL          POD,");
            sb.Append("       VESSEL_VOYAGE_TRN     V,");
            sb.Append("       VESSEL_VOYAGE_TBL     VY");
            sb.Append(" WHERE M.OPERATOR_MST_FK = O.OPERATOR_MST_PK");
            sb.Append("   AND M.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND M.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND V.VOYAGE_TRN_PK(+) = M.VOYAGE_TRN_FK");
            sb.Append("   AND V.VESSEL_VOYAGE_TBL_FK = VY.VESSEL_VOYAGE_TBL_PK(+)");
            sb.Append(" AND M.MASTER_JC_SEA_EXP_PK =" + MJCPk + "");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        public DataSet FetchMJCTrans(string MJCPk = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT ROWNUM SR_NO, Q.*");
            sb.Append("  FROM (SELECT JOBT.CONTAINER_NUMBER,");
            sb.Append("               CONT.CONTAINER_TYPE_MST_ID,");
            sb.Append("               CONT.CONTAINER_TYPE_MST_PK,");
            sb.Append("               JOBT.SEAL_NUMBER,");
            sb.Append("               JOBT.VOLUME_IN_CBM,");
            sb.Append("               JOBT.GROSS_WEIGHT,");
            sb.Append("               JOBT.NET_WEIGHT,");
            sb.Append("               JOBT.CHARGEABLE_WEIGHT,");
            sb.Append("               (CASE");
            sb.Append("                 WHEN BKG.CARGO_TYPE = 1 THEN");
            sb.Append("                  (SELECT PTMT.PACK_TYPE_MST_PK");
            sb.Append("                     FROM JOB_TRN_COMMODITY JCOMM, PACK_TYPE_MST_TBL PTMT");
            sb.Append("                    WHERE JOBT.JOB_TRN_CONT_PK = JCOMM.JOB_TRN_CONT_FK");
            sb.Append("                      AND PTMT.PACK_TYPE_MST_PK = JCOMM.PACK_TYPE_FK");
            sb.Append("                      AND ROWNUM = 1)");
            sb.Append("                 ELSE");
            sb.Append("                  JOBT.PACK_TYPE_MST_FK");
            sb.Append("               END) PACK_TYPE_MST_FK,");
            sb.Append("               (CASE");
            sb.Append("                 WHEN BKG.CARGO_TYPE = 1 THEN");
            sb.Append("                  (SELECT PTMT.PACK_TYPE_DESC");
            sb.Append("                     FROM JOB_TRN_COMMODITY JCOMM, PACK_TYPE_MST_TBL PTMT");
            sb.Append("                    WHERE JOBT.JOB_TRN_CONT_PK = JCOMM.JOB_TRN_CONT_FK");
            sb.Append("                      AND PTMT.PACK_TYPE_MST_PK = JCOMM.PACK_TYPE_FK");
            sb.Append("                      AND ROWNUM = 1)");
            sb.Append("                 ELSE");
            sb.Append("                  PACK.PACK_TYPE_DESC");
            sb.Append("               END) PACK_TYPE_ID,");
            sb.Append("               JOBT.PACK_COUNT,");
            sb.Append("               JOBT.COMMODITY_MST_FK,");
            sb.Append("               '' COMMODITY_ID,");
            sb.Append("               '' MBL_TRN_EXP_CONT_PK,");
            sb.Append("               JOBT.COMMODITY_MST_FKS,''JOBCARDREFNO");
            sb.Append("          FROM JOB_CARD_TRN   JOB,");
            sb.Append("               BOOKING_MST_TBL        BKG,");
            sb.Append("               JOB_TRN_CONT   JOBT,");
            sb.Append("               PACK_TYPE_MST_TBL      PACK,");
            sb.Append("               COMMODITY_MST_TBL      COM,");
            sb.Append("               CONTAINER_TYPE_MST_TBL CONT");
            sb.Append("         WHERE JOB.JOB_CARD_TRN_PK = JOBT.JOB_CARD_TRN_FK");
            sb.Append("           AND PACK.PACK_TYPE_MST_PK(+) = JOBT.PACK_TYPE_MST_FK");
            sb.Append("           AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
            sb.Append("           AND COM.COMMODITY_MST_PK(+) = JOBT.COMMODITY_MST_FK");
            sb.Append("           AND CONT.CONTAINER_TYPE_MST_PK(+) = JOBT.CONTAINER_TYPE_MST_FK");
            sb.Append(" AND JOB.MASTER_JC_FK =" + MJCPk + "");
            sb.Append(" ) Q");
            try
            {
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

        #endregion " Fetch From Master JobCard"

        //End
    }
}