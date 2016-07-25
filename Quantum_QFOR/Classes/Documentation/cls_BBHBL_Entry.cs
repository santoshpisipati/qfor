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

using Oracle.DataAccess.Client;
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
    #region "Class Variables"
    public class cls_BBHBL_Entry : CommonFeatures
    {
        WorkFlow objWF = new WorkFlow();
        cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
        private static DataSet M_ShipDataSet = new DataSet();
        private static DataSet M_FreightTermsDataset = new DataSet();
        #endregion
        private static DataSet M_MoveCodeDataset = new DataSet();

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
        #endregion

        #region "Constructor"
        public cls_BBHBL_Entry()
        {
            string strShipSQL = null;
            string strFreightSQL = null;
            string strMoveCodeSQL = null;
            strShipSQL = "SELECT 0 SHIPPING_TERMS_MST_PK, ' ' INCO_CODE  FROM  DUAL UNION ";
            strShipSQL += " SELECT SHIPPING_TERMS_MST_PK,INCO_CODE  FROM SHIPPING_TERMS_MST_TBL WHERE ACTIVE_FLAG = 1";
            strFreightSQL = "SELECT FREIGHT_TERMS_MST_PK,FRIEGHT_TERMS  FROM FREIGHT_TERMS_MST_TBL ";
            strMoveCodeSQL = "SELECT 0 CARGO_MOVE_PK,'' CARGO_MOVE_CODE FROM DUAL UNION ";
            strMoveCodeSQL += "SELECT CARGO_MOVE_PK,CARGO_MOVE_CODE FROM CARGO_MOVE_MST_TBL WHERE ACTIVE_FLAG = 1";
            try
            {
                M_ShipDataSet = (new WorkFlow()).GetDataSet(strShipSQL);
                M_FreightTermsDataset = (new WorkFlow()).GetDataSet(strFreightSQL);
                M_MoveCodeDataset = (new WorkFlow()).GetDataSet(strMoveCodeSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "print"
        //adding by thiyagarajan on 25/5/09
        public DataSet getfrightdtls(string jobrefno)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT NVL(COMMODITY_NAME, 'Other Charges') as \"COMMODITY NAME\",");
                sb.Append("       freight_element_name,");
                sb.Append("       job_card_sea_exp_pk,");
                sb.Append("       prepaid,");
                sb.Append("       collect,");
                sb.Append("       currency_id,");
                sb.Append("       sortcol");
                sb.Append("  FROM (select CMT.COMMODITY_NAME,");
                sb.Append("               fright.freight_element_name,");
                sb.Append("               j.JOB_CARD_TRN_PK job_card_sea_exp_pk,");
                sb.Append("               (case");
                sb.Append("                 when fd.freight_type = 1 then");
                sb.Append("                  fd.freight_amt");
                sb.Append("                 else");
                sb.Append("                  0.00");
                sb.Append("               end) prepaid,");
                sb.Append("               (case");
                sb.Append("                 when fd.freight_type = 2 then");
                sb.Append("                  fd.freight_amt");
                sb.Append("                 else");
                sb.Append("                  0.00");
                sb.Append("               end) collect,");
                sb.Append("               curr.currency_id,");
                sb.Append("               1 sortcol");
                sb.Append("          from JOB_CARD_TRN    j,");
                sb.Append("               JOB_TRN_FD      fd,");
                sb.Append("               freight_element_mst_tbl fright,");
                sb.Append("               currency_type_mst_tbl   curr,");
                sb.Append("               JOB_TRN_CONT    JC,");
                sb.Append("               COMMODITY_MST_TBL       CMT");
                sb.Append("               where j.jobcard_ref_no like '" + jobrefno + "'");
                sb.Append("           and j.JOB_CARD_TRN_PK = fd.JOB_CARD_TRN_FK");
                sb.Append("           AND JC.JOB_TRN_CONT_PK = FD.JOB_TRN_CONT_FK");
                sb.Append("           AND CMT.COMMODITY_MST_PK = JC.COMMODITY_MST_FK");
                sb.Append("           and fd.freight_element_mst_fk = fright.freight_element_mst_pk");
                sb.Append("           and curr.currency_mst_pk = fd.currency_mst_fk");
                sb.Append("           and fd.print_on_mbl = 1");
                sb.Append("        ");
                sb.Append("        UNION ALL");
                sb.Append("        select NULL COMMODITY_NAME,");
                sb.Append("               fright.freight_element_name,");
                sb.Append("               j.JOB_CARD_TRN_PK job_card_sea_exp_pk,");
                sb.Append("               (case");
                sb.Append("                 when oth.freight_type = 1 then");
                sb.Append("                  oth.amount");
                sb.Append("                 else");
                sb.Append("                  0.00");
                sb.Append("               end) prepaid,");
                sb.Append("               (case");
                sb.Append("                 when oth.freight_type = 2 then");
                sb.Append("                  oth.amount");
                sb.Append("                 else");
                sb.Append("                  0.00");
                sb.Append("               end) collect,");
                sb.Append("               curr.currency_id,");
                sb.Append("               2 sortcol");
                sb.Append("          from JOB_CARD_TRN     j,");
                sb.Append("               JOB_TRN_OTH_CHRG OTH,");
                sb.Append("               freight_element_mst_tbl  fright,");
                sb.Append("               currency_type_mst_tbl    curr");
                sb.Append("               where j.jobcard_ref_no like '" + jobrefno + "'");
                sb.Append("           and j.JOB_CARD_TRN_PK = oth.JOB_CARD_TRN_FK");
                sb.Append("           and oth.freight_element_mst_fk = fright.freight_element_mst_pk");
                sb.Append("           and curr.currency_mst_pk = oth.currency_mst_fk");
                sb.Append("           and oth.print_on_mbl = 1");
                sb.Append("         ) Q order by commodity_name,job_card_sea_exp_pk ");
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
        public DataSet getamt(string jobrefno)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = " select sum(preamt)amt ,1 sortcol from ( ";
                str += " select sum (fd.freight_amt*fd.exchange_rate)preamt ";
                str += " from JOB_CARD_TRN j,job_trn_fd fd,freight_element_mst_tbl fright,currency_type_mst_tbl curr ";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=fd.JOB_CARD_TRN_FK and fd.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=fd.currency_mst_fk and fd.print_on_mbl=1 and fd.freight_type=1 ";

                str += "  UNION ";

                str += " select sum(oth.amount*oth.exchange_rate)preamt ";
                str += " from JOB_CARD_TRN j,job_trn_oth_chrg OTH,freight_element_mst_tbl fright,currency_type_mst_tbl curr";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=oth.JOB_CARD_TRN_FK and oth.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=oth.currency_mst_fk and oth.print_on_mbl=1 and oth.freight_type=1 )main1 ";

                str += " UNION select sum(collamt) amt, 2 sortcol from ( select sum ( fd.freight_amt*fd.exchange_rate)collamt ";
                str += " from JOB_CARD_TRN j,job_trn_fd fd,freight_element_mst_tbl fright,currency_type_mst_tbl curr ";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=fd.JOB_CARD_TRN_FK and fd.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=fd.currency_mst_fk and fd.print_on_mbl=1 and fd.freight_type=2 ";

                str += "  UNION ";

                str += " select sum(oth.amount*oth.exchange_rate)collamt ";
                str += " from JOB_CARD_TRN j,job_trn_oth_chrg OTH,freight_element_mst_tbl fright,currency_type_mst_tbl curr";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=oth.JOB_CARD_TRN_FK and oth.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=oth.currency_mst_fk and oth.print_on_mbl=1 and oth.freight_type=2 )main2 order by sortcol asc ";
                return objWF.GetDataSet(str);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public string fetchjob(string pk)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = "select j.jobcard_ref_no  from JOB_CARD_TRN j where j.JOB_CARD_TRN_PK= " + pk;
                return objWF.ExecuteScaler(str);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Enhance Search Functions"
        public string FetchForJobRef(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            var strNull = DBNull.Value;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            //Business Type to identify the user belongs to AIR/SEA
            // If arr.Length > 3 Then strBusiType = arr(3)
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_JOB_REF_COMMON";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //  .Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input
                _with1.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
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
        #endregion

        #region " Enhance Search & Lookup Search Block FOR HBL"
        //Pls do the impact analysis before changing as this function
        //as might be accesed by other forms also. 
        //Using in Transporter Note Report
        public string FetchForHblRef(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOCATION_IN = null;
            string strSERACH_IN = null;
            int strBUSINESS_MODEL_IN = 0;
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            //  Business Type Added to identify AIR/SEA/Both based on USER LOGIN
            if (arr.Length > 2)
                strBUSINESS_MODEL_IN = Convert.ToInt32(arr.GetValue(2));
            if (arr.Length > 3)
                strLOCATION_IN = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_PKG.GET_HBL_REF_COMMON";
                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBUSINESS_MODEL_IN).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_IN", (!string.IsNullOrEmpty(loc) ? loc : "")).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
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
        #endregion

        #region " Fetch sOperator Details After Job Card Is Selected"
        public DataSet FetchOperatorDetails(string JOBPk = "0")
        {
            string strSQl = null;
            strSQl = " select distinct" ;
            strSQl += " BOOK.CARGO_TYPE,OPR.OPERATOR_ID, " ;
            strSQl += " OPR.OPERATOR_NAME, " ;
            strSQl += " opr.operator_mst_pk," ;
            strSQl += " Po2.PORT_ID AS POL," ;
            strSQl += " po2.port_mst_pk as \"POLPK\"," ;
            strSQl += " po1.port_mst_pk as \"PODPK\"," ;
            strSQl += " po1.PORT_ID AS POD," ;
            strSQl += " CASE WHEN pld.PLACE_NAME IS NOT NULL THEN pld.PLACE_NAME ELSE po1.PORT_ID END DELPLACE, " ;
            strSQl += " CASE WHEN plr.PLACE_NAME IS NOT NULL THEN plr.PLACE_NAME ELSE Po2.PORT_ID END RPLACE," ;
            strSQl += " CUST.CUSTOMER_NAME AS CUSTOMER," ;
            strSQl += " FIRSTVOY.VOYAGE_TRN_PK \"FIRSTVOYPK\"," ;
            strSQl += " FIRSTVSL.VESSEL_NAME," ;
            strSQl += " FIRSTVSL.VESSEL_ID," ;
            strSQl += " FIRSTVOY.pod_eta," ;
            strSQl += " FIRSTVOY.pol_etd," ;
            strSQl += " JOB.ARRIVAL_DATE, " ;
            strSQl += " JOB.DEPARTURE_DATE, " ;
            strSQl += " JOB.SEC_ETA_DATE, " ;
            strSQl += " JOB.SEC_ETD_DATE, " ;
            strSQl += " JOB.jobcard_ref_no, " ;
            strSQl += " JOB.GOODS_DESCRIPTION, " ;
            strSQl += " JOB.MARKS_NUMBERS, " ;
            strSQl += " SH.inco_code, " ;
            strSQl += " MOV.cargo_move_code, " ;
            strSQl += " JOB.PYMT_TYPE, " ;
            strSQl += " SECVOY.VOYAGE_TRN_PK \"SECVOYPK\"," ;
            strSQl += " SECVSL.VESSEL_NAME \"SEC_VESSEL_NAME\"," ;
            strSQl += " SECVSL.VESSEL_ID \"SEC_VESSEL_ID\"," ;
            strSQl += " FIRSTVOY.VOYAGE," ;
            strSQl += " SECVOY.VOYAGE \"SEC_VOYAGE\", " ;
            strSQl += " CL.AGENT_MST_PK AS CLAGENTPK ," ;
            strSQl += " CL.agent_name AS CLAGENT ," ;
            strSQl += " CB.AGENT_MST_PK AS CBAGENTPK ," ;
            strSQl += " CB.agent_name AS CBAGENT," ;
            strSQl += " DP.AGENT_MST_PK AS DPAGENTPK ," ;
            strSQl += " DP.agent_name AS DPAGENT," ;



            strSQl += "         TEMP_CONS.CUSTOMER_MST_PK AS CONSIGNEEPK_TEMP," ;
            strSQl += "      TMPNOTIFY1.CUSTOMER_MST_PK AS NOTIFY1PK_TEMP," ;
            strSQl += "       TMPNOTIFY2.CUSTOMER_MST_PK AS NOTIFY2PK_TEMP," ;



            strSQl += "     nvl(  CONSIGNEE.CUSTOMER_MST_PK,TEMP_CONS.CUSTOMER_MST_PK) AS CONSIGNEEPK," ;
            strSQl += "     nvl(  CONSIGNEE.CUSTOMER_NAME,TEMP_CONS.CUSTOMER_NAME) AS CONSIGNEE," ;

            //strSQl &= " CONSIGNEE.CUSTOMER_MST_PK AS CONSIGNEEPK," & vbCrLf
            //strSQl &= " CONSIGNEE.customer_name AS CONSIGNEE," & vbCrLf




            strSQl += " SHIPPER.CUSTOMER_MST_PK AS SHIPPERPK," ;
            strSQl += " SHIPPER.customer_name AS SHIPPER," ;
            strSQl += " case when NVL(JOB.sac_n1, 0) = '0' then nvl(NOTIFY1.CUSTOMER_MST_PK, TMPNOTIFY1.CUSTOMER_MST_PK) else NULL END as NOTIFY1PK," ;
            strSQl += " (case when NVL(JOB.sac_n1, 0) = '0' then nvl(NOTIFY1.customer_name, TMPNOTIFY1.CUSTOMER_NAME)  else 'SAME AS CONSIGNEE' end) as NOTIFY1," ;
            strSQl += " case when NVL(JOB.sac_n2, 0) = '0' then nvl(NOTIFY2.CUSTOMER_MST_PK, TMPNOTIFY2.CUSTOMER_MST_PK)  else NULL END as NOTIFY2PK," ;
            strSQl += " (case when NVL(JOB.sac_n2, 0) = '0' then nvl(NOTIFY2.customer_name, TMPNOTIFY2.CUSTOMER_NAME)  else 'SAME AS CONSIGNEE' end) as NOTIFY2," ;
            strSQl += " JOB.LC_SHIPMENT, (SELECT LMT.LOCATION_NAME FROM LOCATION_MST_TBL LMT WHERE LMT.LOCATION_MST_PK = " + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + ") PLACE_ISSUE, " ;
            strSQl += "  PO1.PORT_NAME || ',' || POLCONT.COUNTRY_NAME PODCONTY," ;
            strSQl += "                PO2.PORT_NAME || ',' || PODCONT.COUNTRY_NAME POLCONTY," ;
            strSQl += "                CASE" ;
            strSQl += "                  WHEN PLR.PLACE_NAME IS NOT NULL THEN" ;
            strSQl += "                   PLR.PLACE_NAME || ',' || PLRCONT.COUNTRY_NAME" ;
            strSQl += "                  ELSE" ;
            strSQl += "                   ''" ;
            strSQl += "                END PLRCONTY," ;
            strSQl += "                CASE" ;
            strSQl += "                  WHEN PLD.PLACE_NAME IS NOT NULL THEN" ;
            strSQl += "                   PLD.PLACE_NAME || ',' || PFDCONT.COUNTRY_NAME" ;
            strSQl += "                  ELSE" ;
            strSQl += "                   ''" ;
            strSQl += "                END PLDCONTY " ;
            strSQl += " FROM " ;



            strSQl += "      TEMP_CUSTOMER_TBL TMPNOTIFY2," ;
            strSQl += "     TEMP_CUSTOMER_TBL TMPNOTIFY1," ;
            strSQl += "   TEMP_CUSTOMER_TBL TEMP_CONS," ;


            strSQl += " BOOKING_MST_TBL book, " ;
            strSQl += " JOB_CARD_TRN JOB, " ;
            strSQl += " CUSTOMER_MST_TBL SHIPPER," ;
            strSQl += " CUSTOMER_MST_TBL CUST, " ;
            strSQl += " AGENT_MST_TBL CL," ;
            strSQl += " AGENT_MST_TBL CB," ;
            strSQl += " AGENT_MST_TBL DP," ;
            strSQl += " CUSTOMER_MST_TBL CONSIGNEE,";
            strSQl += " CUSTOMER_MST_TBL NOTIFY1," ;
            strSQl += " CUSTOMER_MST_TBL NOTIFY2, ";
            strSQl += " PLACE_MST_TBL PLR," ;
            strSQl += " PLACE_MST_TBL PLD," ;
            strSQl += " PORT_MST_TBL PO1," ;
            strSQl += " PORT_MST_TBL PO2," ;
            strSQl += " OPERATOR_MST_TBL OPR," ;
            strSQl += " VESSEL_VOYAGE_TBL FIRSTVSL," ;
            strSQl += " VESSEL_VOYAGE_TBL SECVSL," ;
            strSQl += " VESSEL_VOYAGE_TRN FIRSTVOY," ;
            strSQl += " VESSEL_VOYAGE_TRN SECVOY," ;
            strSQl += " CARGO_MOVE_MST_TBL MOV," ;
            strSQl += " SHIPPING_TERMS_MST_TBL SH," ;
            strSQl += "       COUNTRY_MST_TBL        PLRCONT," ;
            strSQl += "       COUNTRY_MST_TBL        POLCONT," ;
            strSQl += "       COUNTRY_MST_TBL        PODCONT," ;
            strSQl += "       COUNTRY_MST_TBL        PFDCONT," ;
            strSQl += "       LOCATION_MST_TBL       LMTPLR," ;
            strSQl += "       LOCATION_MST_TBL       LMTPFD " ;
            strSQl += " WHERE " ;
            strSQl += " cl.agent_mst_pk(+) =JOB.CL_AGENT_MST_FK  and" ;
            strSQl += " cb.agent_mst_pk(+) =JOB.CB_AGENT_MST_FK  and" ;
            strSQl += " dp.agent_mst_pk(+) =JOB.DP_AGENT_MST_FK   and" ;
            strSQl += " consignee.customer_mst_pk(+) =JOB.Consignee_Cust_Mst_Fk  and" ;
            strSQl += " Shipper.customer_mst_pk(+) =JOB.Shipper_Cust_Mst_Fk   and" ;
            strSQl += " Notify1.customer_mst_pk(+) =JOB.Notify1_Cust_Mst_Fk   and" ;
            strSQl += " Notify2.customer_mst_pk(+) = JOB.Notify2_Cust_Mst_Fk  and" ;
            strSQl += " cust.customer_mst_pk(+) =  Book.Cust_Customer_Mst_Fk and" ;
            strSQl += " book.port_mst_pod_fk(+) = po1.PORT_MST_PK and " ;
            strSQl += " book.port_mst_pol_fk = po2.port_mst_pk and " ;
            strSQl += " MOV.CARGO_MOVE_PK(+) = JOB.CARGO_MOVE_FK AND " ;
            strSQl += " SH.SHIPPING_TERMS_MST_PK(+)=job.SHIPPING_TERMS_MST_FK and " ;
            strSQl += " book.col_place_mst_fk = plr.place_pk(+) and " ;
            strSQl += " Book.del_place_mst_fk = pld.place_pk(+) and ";
            strSQl += " book.CARRIER_MST_FK = opr.operator_mst_pk(+)  and " ;
            strSQl += " job.BOOKING_MST_FK = book.BOOKING_MST_PK " ;
            strSQl += " AND FIRSTVOY.VESSEL_VOYAGE_TBL_FK = FIRSTVSL.VESSEL_VOYAGE_TBL_PK(+) " ;
            strSQl += " AND SECVOY.VESSEL_VOYAGE_TBL_FK = SECVSL.VESSEL_VOYAGE_TBL_PK(+) " ;
            strSQl += " AND JOB.VOYAGE_TRN_FK = FIRSTVOY.VOYAGE_TRN_PK(+) " ;
            strSQl += " AND JOB.SEC_VOYAGE_TRN_FK = SECVOY.VOYAGE_TRN_PK(+) " ;
            strSQl += "   AND PO1.COUNTRY_MST_FK = POLCONT.COUNTRY_MST_PK(+)" ;
            strSQl += "   AND PO2.COUNTRY_MST_FK = PODCONT.COUNTRY_MST_PK(+)" ;
            strSQl += "   AND PLR.LOCATION_MST_FK = LMTPLR.LOCATION_MST_PK(+)" ;
            strSQl += "   AND LMTPLR.COUNTRY_MST_FK = PLRCONT.COUNTRY_MST_PK(+)" ;
            strSQl += "   AND PLD.LOCATION_MST_FK = LMTPFD.LOCATION_MST_PK(+)" ;
            strSQl += "   AND LMTPFD.COUNTRY_MST_FK = PFDCONT.COUNTRY_MST_PK(+)" ;
            strSQl += "     AND JOB.CONSIGNEE_CUST_MST_FK = TEMP_CONS.CUSTOMER_MST_PK(+)" ;
            strSQl += "  AND JOB.NOTIFY1_CUST_MST_FK=TMPNOTIFY1.CUSTOMER_MST_PK(+)" ;
            strSQl += "    AND JOB.NOTIFY2_CUST_MST_FK=TMPNOTIFY2.CUSTOMER_MST_PK(+)" ;
            strSQl += "  and JOB.JOB_CARD_TRN_PK=" + JOBPk;
            try
            {
                return (objWF.GetDataSet(strSQl));
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

        #region " Fetch Container Type"
        public DataSet FetchContainerType(string jobPk = "0")
        {
            string strSqlpl = null;
            strSqlpl = " SELECT JOBT.JOB_CARD_TRN_FK,UPPER(JOBT.CONTAINER_NUMBER) CONTAINER_NUMBER,JOBT.SEAL_NUMBER,CON.CONTAINER_TYPE_MST_ID, ";
            strSqlpl += " JOBT.VOLUME_IN_CBM,JOBT.GROSS_WEIGHT,JOBT.PACK_COUNT,PK.PACK_TYPE_ID ";
            strSqlpl += " FROM JOB_TRN_CONT JOBT, CONTAINER_TYPE_MST_TBL CON,PACK_TYPE_MST_TBL PK ";
            strSqlpl += " WHERE CON.CONTAINER_TYPE_MST_PK = JOBT.CONTAINER_TYPE_MST_FK ";
            strSqlpl += " AND JOBT.PACK_TYPE_MST_FK = PK.PACK_TYPE_MST_PK(+) ";
            strSqlpl += " AND JOBT.JOB_CARD_TRN_FK = " + jobPk;
            try
            {
                return (objWF.GetDataSet(strSqlpl));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        ///Added by Koteshwari for break bulk implementation
        #region " Fetch Data"
        public DataSet FetchAllData(System.DateTime CutOffdate, string HBLRefNo = "", string Shipperid = "", short CargoType = 4, string POLID = "", string PODID = "", string POLname = "", string PODname = "", string HBLdate = "", string sOperator = "",
        string Vessel = "", string Voyage = "", string Consineeid = "", string Status = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", long lngUsrLocFk = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder strBuildCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strBuildCondition.Append(" AND 1=2 ");
            }
            if (HBLRefNo.Length > 0)
            {
                strBuildCondition.Append(" AND UPPER(HBL.HBL_REF_NO) LIKE '%" + HBLRefNo.ToUpper().Replace("'", "''") + "%'  ");
            }

            if (Shipperid.Length > 0)
            {
                strBuildCondition.Append(" AND CUST.CUSTOMER_ID LIKE '%" + Shipperid.ToUpper().Replace("'", "''") + "%'  ");
            }

            if (Consineeid.Length > 0)
            {
                strBuildCondition.Append(" AND CONS.CUSTOMER_ID LIKE '%" + Consineeid.ToUpper().Replace("'", "''") + "%'  ");
            }

            if (!(CutOffdate == null))
            {
                strBuildCondition.Append(" AND VVT.POL_CUT_OFF_DATE >= TO_DATE('" + CutOffdate + "','& dd/MM/yyyy  HH24:MI:SS &')   ");
            }

            if (HBLdate.Length > 0)
            {
                strBuildCondition.Append(" AND HBL.HBL_DATE = TO_DATE('" + HBLdate + "','" + dateFormat + "')   ");
            }
            if (sOperator.Length > 0)
            {
                strBuildCondition.Append(" AND OPR.OPERATOR_ID LIKE '%" + sOperator.ToUpper().Replace("'", "''") + "%'  ");
            }

            if (Vessel.Length > 0)
            {
                strBuildCondition.Append(" AND UPPER(HBL.VESSEL_NAME) LIKE '%" + Vessel.ToUpper().Replace("'", "''") + "%'   ");
            }

            if (Voyage.Length > 0)
            {
                strBuildCondition.Append(" AND UPPER(VVT.VOYAGE) LIKE '%" + Voyage.ToUpper().Replace("'", "''") + "%'   ");
            }

            if (Convert.ToInt32(Status) != 4)
            {
                if (Status.Length > 0)
                {
                    strBuildCondition.Append(" AND HBL.HBL_STATUS =" + Status + "");
                }
            }
            if (PODID.Length > 0)
            {
                strBuildCondition.Append(" AND BOOK.PORT_MST_POD_FK IN");
                strBuildCondition.Append(" (SELECT P.PORT_MST_PK FROM PORT_MST_TBL P ");
                strBuildCondition.Append(" WHERE P.PORT_ID LIKE '%" + PODID.ToUpper().Replace("'", "''") + "%')");
            }

            if (POLID.Length > 0)
            {
                strBuildCondition.Append(" AND BOOK.PORT_MST_POL_FK IN");
                strBuildCondition.Append(" (SELECT P.PORT_MST_PK FROM PORT_MST_TBL P ");
                strBuildCondition.Append(" WHERE P.PORT_ID LIKE '%" + POLID.ToUpper().Replace("'", "''") + "%')");
            }
            if (CargoType > 0)
            {
                strBuildCondition.Append(" AND BOOK.CARGO_TYPE = " + CargoType + "");
            }
            strBuildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
            strBuildCondition.Append(" AND HBL.CREATED_BY_FK = UMT.USER_MST_PK ");
            strBuildCondition.AppendLine(" AND JOB.BUSINESS_TYPE=2 ");
            strBuildCondition.AppendLine(" AND (HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK OR HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK OR HBL.HBL_EXP_TBL_PK=JOB.HBL_HAWB_FK)");

            StrBuilder.Append(" SELECT COUNT(*) FROM ( ");
            StrBuilder.Append("  SELECT HBL.HBL_EXP_TBL_PK FROM ");
            StrBuilder.Append("  HBL_EXP_TBL HBL,");
            StrBuilder.Append(" JOB_CARD_TRN JOB, ");
            StrBuilder.Append(" CUSTOMER_MST_TBL CUST, ");
            StrBuilder.Append(" CUSTOMER_MST_TBL     CONS,");
            //'
            StrBuilder.Append(" BOOKING_MST_TBL BOOK, ");
            StrBuilder.Append(" PORT_MST_TBL PO,");
            StrBuilder.Append(" OPERATOR_MST_TBL OPR,USER_MST_TBL UMT, ");
            StrBuilder.Append(" PORT_MST_TBL PO1, VESSEL_VOYAGE_TBL V, VESSEL_VOYAGE_TRN VVT ");
            StrBuilder.Append(" WHERE ");
            StrBuilder.Append(" OPR.OPERATOR_MST_PK  = BOOK.CARRIER_MST_FK ");
            StrBuilder.Append(" AND    CUST.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK");
            StrBuilder.Append(" AND CONS.CUSTOMER_MST_PK = JOB.CONSIGNEE_CUST_MST_FK ");
            //'
            StrBuilder.Append(" AND    JOB.BOOKING_MST_FK   = BOOK.BOOKING_MST_PK");
            StrBuilder.Append(" AND    BOOK.PORT_MST_POL_FK = PO.PORT_MST_PK ");
            StrBuilder.Append(" AND    BOOK.PORT_MST_POD_FK = PO1.PORT_MST_PK ");
            StrBuilder.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK (+) ");
            StrBuilder.Append(" AND HBL.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK (+) ");
            StrBuilder.Append(strBuildCondition.ToString());
            //StrBuilder.Append("  UNION ")
            //StrBuilder.Append("  SELECT DISTINCT HBL.HBL_EXP_TBL_PK FROM ")
            //StrBuilder.Append("  HBL_EXP_TBL HBL,")
            //StrBuilder.Append(" JOB_CARD_TRN JOB, ")
            //StrBuilder.Append(" CUSTOMER_MST_TBL CUST, ")
            //StrBuilder.Append(" CUSTOMER_MST_TBL     CONS,") ''
            //StrBuilder.Append(" BOOKING_MST_TBL BOOK, ")
            //StrBuilder.Append(" PORT_MST_TBL PO,")
            //StrBuilder.Append(" OPERATOR_MST_TBL OPR,USER_MST_TBL UMT, ")
            //StrBuilder.Append(" PORT_MST_TBL PO1, VESSEL_VOYAGE_TBL V, VESSEL_VOYAGE_TRN VVT ")
            //StrBuilder.Append(" WHERE ")
            //StrBuilder.Append(" OPR.OPERATOR_MST_PK  = BOOK.CARRIER_MST_FK ")
            //StrBuilder.Append(" AND    CUST.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK")
            //StrBuilder.Append(" AND CONS.CUSTOMER_MST_PK = JOB.CONSIGNEE_CUST_MST_FK ") ''
            //StrBuilder.Append(" AND    JOB.BOOKING_MST_FK   = BOOK.BOOKING_MST_PK")
            //StrBuilder.Append(" AND    BOOK.PORT_MST_POL_FK = PO.PORT_MST_PK ")
            //StrBuilder.Append(" AND    BOOK.PORT_MST_POD_FK = PO1.PORT_MST_PK  ")
            //StrBuilder.Append(" AND    HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK ")
            //StrBuilder.Append(strBuildCondition.ToString)
            //StrBuilder.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK (+) ")
            //StrBuilder.Append(" AND HBL.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK (+) ")
            StrBuilder.AppendLine(" ) ");

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrBuilder.ToString()));
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

            StrBuilder = new System.Text.StringBuilder();
            StrBuilder.Append(" SELECT * FROM (");
            StrBuilder.Append(" SELECT ROWNUM SR_NO,q.* FROM (  ");
            StrBuilder.Append(" SELECT HBL.HBL_EXP_TBL_PK, ");
            StrBuilder.Append(" HBL.HBL_REF_NO, ");
            StrBuilder.Append(" HBL.HBL_DATE, ");
            StrBuilder.Append(" CUST.CUSTOMER_ID, ");
            StrBuilder.Append("  CONS.CUSTOMER_ID AS CONSIGNEE,");
            //'
            StrBuilder.Append(" PO.PORT_ID AS POL,PO1.PORT_ID AS POD, ");
            StrBuilder.Append(" OPR.OPERATOR_ID,");

            StrBuilder.Append(" V.VESSEL_NAME,");
            StrBuilder.Append("  VVT.VOYAGE,");
            //'
            StrBuilder.Append("  HBL.DEPARTURE_DATE ATD_POL,");
            StrBuilder.Append("  VVT.POL_CUT_OFF_DATE,");
            StrBuilder.Append("  VVT.POL_ETD,");
            //'
            StrBuilder.Append(" DECODE(BOOK.CARGO_TYPE, '1','FCL','2','LCL','4','BBC' ) CARGO_TYPE, ");
            StrBuilder.Append(" DECODE(HBL.HBL_STATUS, '0','Draft','1','Confirmed','2','Released','3','Cancelled', '4','All') STATUS,'' SEL");
            StrBuilder.Append(" FROM HBL_EXP_TBL HBL,");
            StrBuilder.Append(" JOB_CARD_TRN JOB, ");
            StrBuilder.Append(" CUSTOMER_MST_TBL CUST,");
            StrBuilder.Append(" CUSTOMER_MST_TBL     CONS,");
            //'
            StrBuilder.Append(" BOOKING_MST_TBL BOOK, ");
            StrBuilder.Append(" PORT_MST_TBL PO, ");
            StrBuilder.Append(" OPERATOR_MST_TBL OPR,");
            StrBuilder.Append(" PORT_MST_TBL PO1,USER_MST_TBL UMT,");
            StrBuilder.Append(" VESSEL_VOYAGE_TBL V,");
            StrBuilder.Append(" VESSEL_VOYAGE_TRN VVT ");
            StrBuilder.Append(" WHERE ");
            StrBuilder.Append(" OPR.OPERATOR_MST_PK       =  BOOK.CARRIER_MST_FK ");
            StrBuilder.Append(" AND CUST.CUSTOMER_MST_PK  =  JOB.SHIPPER_CUST_MST_FK ");
            StrBuilder.Append("  AND CONS.CUSTOMER_MST_PK = JOB.CONSIGNEE_CUST_MST_FK ");
            //'
            StrBuilder.Append(" AND JOB.BOOKING_MST_FK    =  BOOK.BOOKING_MST_PK ");
            StrBuilder.Append(" AND BOOK.PORT_MST_POL_FK  =  PO.PORT_MST_PK");
            StrBuilder.Append(" AND BOOK.PORT_MST_POD_FK  =  PO1.PORT_MST_PK");
            //StrBuilder.Append(" AND HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK ")
            StrBuilder.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK (+) ");
            StrBuilder.Append(" AND HBL.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK (+) ");
            StrBuilder.Append(strBuildCondition.ToString());
            //StrBuilder.Append(" UNION ")
            //StrBuilder.Append(" SELECT DISTINCT HBL.HBL_EXP_TBL_PK, ")
            //StrBuilder.Append(" HBL.HBL_REF_NO, ")
            //StrBuilder.Append(" HBL.HBL_DATE, ")
            //StrBuilder.Append(" CUST.CUSTOMER_ID, ")
            //StrBuilder.Append("  CONS.CUSTOMER_ID AS CONSIGNEE,") ''
            //StrBuilder.Append(" PO.PORT_ID AS POL,PO1.PORT_ID AS POD, ")
            //StrBuilder.Append(" OPR.OPERATOR_ID,")

            //StrBuilder.Append(" V.VESSEL_NAME,")
            //StrBuilder.Append("  VVT.VOYAGE,") ''
            //StrBuilder.Append("  VVT.ATD_POL,")
            //StrBuilder.Append("  VVT.POL_CUT_OFF_DATE,")
            //StrBuilder.Append("  VVT.POL_ETD,") ''
            //StrBuilder.Append(" DECODE(BOOK.CARGO_TYPE, '1','FCL','2','LCL','4','BBC' ) CARGO_TYPE, ")
            //StrBuilder.Append(" DECODE(HBL.HBL_STATUS, '0','Draft','1','Confirmed','2','Released','3','Cancelled','4','All' ) STATUS,'' SEL")
            //StrBuilder.Append(" FROM HBL_EXP_TBL HBL,")
            //StrBuilder.Append(" JOB_CARD_TRN JOB, ")
            //StrBuilder.Append(" CUSTOMER_MST_TBL CUST,")
            //StrBuilder.Append(" CUSTOMER_MST_TBL     CONS,") ''
            //StrBuilder.Append(" BOOKING_MST_TBL BOOK, ")
            //StrBuilder.Append(" PORT_MST_TBL PO, ")
            //StrBuilder.Append(" OPERATOR_MST_TBL OPR,")
            //StrBuilder.Append(" PORT_MST_TBL PO1,USER_MST_TBL UMT,")
            //StrBuilder.Append(" VESSEL_VOYAGE_TBL V,")
            //StrBuilder.Append(" VESSEL_VOYAGE_TRN VVT ")
            //StrBuilder.Append(" WHERE ")
            //StrBuilder.Append(" OPR.OPERATOR_MST_PK       =  BOOK.CARRIER_MST_FK ")
            //StrBuilder.Append(" AND CUST.CUSTOMER_MST_PK  =  JOB.SHIPPER_CUST_MST_FK ")
            //StrBuilder.Append("  AND CONS.CUSTOMER_MST_PK = JOB.CONSIGNEE_CUST_MST_FK ") ''
            //StrBuilder.Append(" AND JOB.BOOKING_MST_FK    =  BOOK.BOOKING_MST_PK ")
            //StrBuilder.Append(" AND BOOK.PORT_MST_POL_FK  =  PO.PORT_MST_PK")
            //StrBuilder.Append(" AND BOOK.PORT_MST_POD_FK  =  PO1.PORT_MST_PK")
            //StrBuilder.Append(" AND HBL.NEW_JOB_CARD_SEA_EXP_FK=JOB.JOB_CARD_TRN_PK ")
            //StrBuilder.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK (+) ")
            //StrBuilder.Append(" AND HBL.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK (+) ")
            //StrBuilder.Append(strBuildCondition.ToString)
            //StrBuilder.Append("  ORDER BY " & SortColumn & SortType & " ,HBL_REF_NO DESC ")
            StrBuilder.Append("  ORDER BY HBL_DATE ,HBL_REF_NO DESC ");
            StrBuilder.Append("  ) q  ) WHERE SR_NO  Between " + start + " and " + last + "");

            DataSet DS = null;
            DS = objWF.GetDataSet(StrBuilder.ToString());
            try
            {
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
        }
        #endregion

        #region "Fetch HBLCommodityDetails"
        public DataSet GetHBLCommodity(string jobPk = "0", int Flag = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT ROWNUM SLNO,");
            sb.Append("                CG.COMMODITY_GROUP_PK,");
            sb.Append("                CG.COMMODITY_GROUP_CODE,");
            sb.Append("                CMT.COMMODITY_MST_PK,");
            sb.Append("                CMT.COMMODITY_NAME,");
            sb.Append("                PK.PACK_TYPE_MST_PK,");
            sb.Append("                PK.PACK_TYPE_ID,");
            sb.Append("                UM.DIMENTION_UNIT_MST_PK,");
            sb.Append("                UM.DIMENTION_ID,");
            sb.Append("                JTR.PACK_COUNT,");
            sb.Append("                JTR.VOLUME_IN_CBM,");
            sb.Append("                JTR.CHARGEABLE_WEIGHT,");
            sb.Append("                JTR.NET_WEIGHT,JTR.JOB_TRN_CONT_PK, '0' SEL");
            sb.Append("  FROM JOB_CARD_TRN    JC,");
            sb.Append("       JOB_TRN_CONT    JTR,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CG,");
            sb.Append("       COMMODITY_MST_TBL       CMT,");
            sb.Append("       DIMENTION_UNIT_MST_TBL  UM,");
            sb.Append("       PACK_TYPE_MST_TBL       PK");
            sb.Append(" WHERE JC.JOB_CARD_TRN_PK = JTR.JOB_CARD_TRN_FK");
            sb.Append("   AND JTR.COMMODITY_MST_FK  = CMT.COMMODITY_MST_PK");
            sb.Append("   AND CMT.COMMODITY_GROUP_FK = CG.COMMODITY_GROUP_PK(+)");
            sb.Append("   AND JTR.BASIS_FK = UM.DIMENTION_UNIT_MST_PK(+)");
            sb.Append("   AND JTR.PACK_TYPE_MST_FK = PK.PACK_TYPE_MST_PK(+)");
            if ((!string.IsNullOrEmpty(jobPk)))
            {
                sb.Append("   AND JC.JOB_CARD_TRN_PK = " + jobPk + "");
            }
            else
            {
                sb.Append("  AND 1=2");
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
        #endregion

        #region "Get Commodity Details for Print"
        public object GetCommDetails(string jobpk = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT JC.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_PK,");
            sb.Append("       CMT.COMMODITY_MST_PK,");
            sb.Append("       CMT.COMMODITY_NAME,");
            sb.Append("       JT.CHARGEABLE_WEIGHT,");
            sb.Append("       JT.GROSS_WEIGHT,");
            sb.Append("       JT.VOLUME_IN_CBM");
            sb.Append("  FROM JOB_CARD_TRN    JC,");
            sb.Append("       JOB_TRN_CONT    JT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGM,");
            sb.Append("       COMMODITY_MST_TBL       CMT");
            sb.Append(" WHERE JC.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK");
            sb.Append("   AND JT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK");
            sb.Append("   AND CGM.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK");
            if ((!string.IsNullOrEmpty(jobpk)))
            {
                sb.Append("   AND JC.JOB_CARD_TRN_PK = " + jobpk + "");
            }
            sb.Append("  ORDER BY COMMODITY_NAME");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "GetHeader Details"
        public object GetHeaderGeatils(string jobpk = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT JC.JOBCARD_REF_NO,");
            sb.Append("       HBL.HBL_REF_NO,");
            sb.Append("       POL.PORT_ID POLID,");
            sb.Append("       POD.PORT_ID PODID,");
            sb.Append("       PLR.PLACE_CODE PLRID,");
            sb.Append("       PLD.PLACE_CODE PLDID");
            sb.Append("  FROM JOB_CARD_TRN JC,");
            sb.Append("       HBL_EXP_TBL          HBL,");
            sb.Append("       BOOKING_MST_TBL      BOOK,");
            sb.Append("       PORT_MST_TBL         POL,");
            sb.Append("       PORT_MST_TBL         POD,");
            sb.Append("       PLACE_MST_TBL        PLR,");
            sb.Append("       PLACE_MST_TBL        PLD");
            sb.Append(" WHERE JC.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
            sb.Append("  AND HBL.JOB_CARD_SEA_EXP_FK(+) = JC.JOB_CARD_TRN_PK");
            sb.Append("  AND POL.PORT_MST_PK=BOOK.PORT_MST_POD_FK");
            sb.Append("  AND POD.PORT_MST_PK=BOOK.PORT_MST_POL_FK");
            sb.Append("  AND PLR.PLACE_PK(+)=BOOK.COL_PLACE_MST_FK");
            sb.Append("  AND PLD.PLACE_PK(+)=BOOK.DEL_PLACE_MST_FK");

            if ((!string.IsNullOrEmpty(jobpk)))
            {
                sb.Append("   AND JC.JOB_CARD_TRN_PK = " + jobpk + "");
            }
            else
            {
                sb.Append("  AND 1=2");
            }
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "Fetch Freight Details"
        public object FetchFreightDetails(string jobpk = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT *");
            sb.Append("  FROM (SELECT DISTINCT ROWNUM SLN0,");
            sb.Append("     job_trn_fd.JOB_TRN_FD_PK JOB_TRN_SEA_PK,");
            sb.Append("               freight.freight_element_mst_pk FREIGHT_ELE_MST_PK,");
            sb.Append("               COMM.COMMODITY_NAME COMMODITY_NAME,");
            sb.Append("               freight.freight_element_id FREIGHT_ELEMENT_ID,");
            sb.Append("               DIM.DIMENTION_ID BASIS,");
            sb.Append("               job_trn_fd.quantity QUANTITY,");
            sb.Append("               DECODE(job_trn_fd.freight_type, 1, 'Prepaid', 2, 'Collect') FREIGHT_TYPE,");
            sb.Append("               job_trn_fd.freight_amt FREIGHT_AMT,");
            sb.Append("               curr.currency_id CURRENCY_ID,");
            sb.Append("               job_trn_fd.exchange_rate ROE,");
            sb.Append("               lmt.location_name LOCATION,");
            sb.Append("               cmt.customer_name CUSTOMER");
            sb.Append("          FROM JOB_TRN_FD      job_trn_fd,");
            sb.Append("               container_type_mst_tbl  cont,");
            sb.Append("               currency_type_mst_tbl   curr,");
            sb.Append("               freight_element_mst_tbl freight,");
            sb.Append("               parameters_tbl          prm,");
            sb.Append("               JOB_CARD_TRN    job_exp,");
            sb.Append("               location_mst_tbl        lmt,");
            sb.Append("               customer_mst_tbl        cmt,");
            sb.Append("               JOB_TRN_CONT    CNT,");
            sb.Append("               DIMENTION_UNIT_MST_TBL DIM,");
            sb.Append("               COMMODITY_MST_TBL       COMM");
            sb.Append("         WHERE job_trn_fd.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            sb.Append("           AND job_trn_fd.container_type_mst_fk =");
            sb.Append("               cont.container_type_mst_pk(+)");
            sb.Append("           AND CNT.JOB_TRN_CONT_PK =");
            sb.Append("               JOB_TRN_FD.JOB_TRN_CONT_FK");
            sb.Append("           AND COMM.COMMODITY_MST_PK = CNT.COMMODITY_MST_FK");
            sb.Append("           AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk");
            sb.Append("           AND job_trn_fd.freight_element_mst_fk =");
            sb.Append("               freight.freight_element_mst_pk");
            sb.Append("           AND DIM.DIMENTION_UNIT_MST_PK=job_trn_fd.Basis");
            sb.Append("           AND job_trn_fd.freight_element_mst_fk = prm.frt_bof_fk");
            sb.Append("           AND job_trn_fd.location_mst_fk = lmt.location_mst_pk(+)");
            sb.Append("           AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
            if ((!string.IsNullOrEmpty(jobpk)))
            {
                sb.Append("   AND job_exp.JOB_CARD_TRN_PK = " + jobpk + "");
            }
            sb.Append("        union all");
            sb.Append("        SELECT DISTINCT ROWNUM SLN0,");
            sb.Append("          job_trn_fd.JOB_TRN_FD_PK JOB_TRN_SEA_PK,");
            sb.Append("               freight.freight_element_mst_pk FREIGHT_ELE_MST_PK,");
            sb.Append("               COMM.COMMODITY_NAME COMMODITY_NAME,");
            sb.Append("               freight.freight_element_id FREIGHT_ELEMENT_ID,");
            sb.Append("               DIM.DIMENTION_ID BASIS,");
            sb.Append("               job_trn_fd.quantity QUANTITY,");
            sb.Append("               DECODE(job_trn_fd.freight_type, 1, 'Prepaid', 2, 'Collect') FREIGHT_TYPE,");
            sb.Append("               job_trn_fd.freight_amt FREIGHT_AMT,");
            sb.Append("               curr.currency_id CURRENCY_ID,");
            sb.Append("               job_trn_fd.exchange_rate ROE,");
            sb.Append("               lmt.location_name LOCATION,");
            sb.Append("               cmt.customer_name CUSTOMER");
            sb.Append("          FROM JOB_TRN_FD      job_trn_fd,");
            sb.Append("               container_type_mst_tbl  cont,");
            sb.Append("               currency_type_mst_tbl   curr,");
            sb.Append("               freight_element_mst_tbl freight,");
            sb.Append("               parameters_tbl          prm,");
            sb.Append("               JOB_CARD_TRN    job_exp,");
            sb.Append("               location_mst_tbl        lmt,");
            sb.Append("               customer_mst_tbl        cmt,");
            sb.Append("               JOB_TRN_CONT    CNT,");
            sb.Append("                DIMENTION_UNIT_MST_TBL DIM,");
            sb.Append("               COMMODITY_MST_TBL       COMM");
            sb.Append("         WHERE job_trn_fd.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            sb.Append("           AND job_trn_fd.container_type_mst_fk =");
            sb.Append("               cont.container_type_mst_pk(+)");
            sb.Append("           AND CNT.JOB_TRN_CONT_PK =");
            sb.Append("               JOB_TRN_FD.JOB_TRN_CONT_FK");
            sb.Append("           AND COMM.COMMODITY_MST_PK = CNT.COMMODITY_MST_FK");
            sb.Append("           AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk");
            sb.Append("           AND job_trn_fd.freight_element_mst_fk =");
            sb.Append("               freight.freight_element_mst_pk");
            sb.Append("           AND DIM.DIMENTION_UNIT_MST_PK=job_trn_fd.Basis");
            sb.Append("           AND job_trn_fd.freight_element_mst_fk not in prm.frt_bof_fk");
            sb.Append("           AND job_trn_fd.location_mst_fk = lmt.location_mst_pk(+)");
            sb.Append("           AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
            if ((!string.IsNullOrEmpty(jobpk)))
            {
                sb.Append("   AND job_exp.JOB_CARD_TRN_PK = " + jobpk + ")");
            }
            sb.Append(" ORDER BY COMMODITY_NAME");
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
        #endregion

        #region "Get Other Charge Details"
        public object GetOthChargeDetails(string jobpk = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("SELECT OTH_CHRG.JOB_TRN_OTH_PK JOB_TRN_SEA_EXP_OTH_PK,");
            sb.Append("       FRT.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("       FRT.FREIGHT_ELEMENT_ID,");
            sb.Append("       OTH_CHRG.AMOUNT AMOUNT,");
            sb.Append("       DECODE(OTH_CHRG.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE,");
            sb.Append("       CURR.CURRENCY_ID,");
            sb.Append("       OTH_CHRG.EXCHANGE_RATE ROE,");
            sb.Append("       LMT.LOCATION_NAME,");
            sb.Append("       CMT.CUSTOMER_NAME");
            sb.Append("  FROM JOB_TRN_OTH_CHRG OTH_CHRG,");
            sb.Append("       JOB_CARD_TRN     JOBCARD_MST,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL  FRT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL    CURR,");
            sb.Append("       LOCATION_MST_TBL         LMT,");
            sb.Append("       CUSTOMER_MST_TBL         CMT");
            sb.Append(" WHERE OTH_CHRG.JOB_CARD_TRN_FK = JOBCARD_MST.JOB_CARD_TRN_PK");
            sb.Append("   AND OTH_CHRG.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("   AND OTH_CHRG.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)");
            sb.Append("   AND OTH_CHRG.LOCATION_MST_FK = LMT.LOCATION_MST_PK(+)");
            sb.Append("   AND OTH_CHRG.FRTPAYER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            if ((!string.IsNullOrEmpty(jobpk)))
            {
                sb.Append("   AND OTH_CHRG.JOB_CARD_TRN_FK = " + jobpk + "");
            }
            sb.Append(" ORDER BY FREIGHT_ELEMENT_ID");
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
        #endregion

        ///End
        #region "Fetch Temp Customer" 'Manoharan 19Feb07: If HBL is released, update temp customer to permanent
        public DataSet fetchTempCust(string jobNr = "")
        {
            string strSqlpl = null;
            strSqlpl = "select t.cust_customer_mst_fk from BOOKING_MST_TBL t ";
            strSqlpl += " where t.BOOKING_MST_PK =";
            strSqlpl += " (select j.BOOKING_MST_FK from JOB_CARD_TRN j where j.jobcard_ref_no = '" + jobNr + "'  and j.booking_mst_fk is not null)";

            try
            {
                return (objWF.GetDataSet(strSqlpl));
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
        #endregion

        #region " Fetch Volume & Weight"
        public DataSet FetchVolWeight(string jobPk = "0")
        {
            string strSql = null;
            strSql = " SELECT SUM(VOLUME_IN_CBM) as VOLUME, " ;
            strSql = strSql + " SUM(GROSS_WEIGHT) AS GWEIGHT, " ;
            strSql = strSql + " SUM(CHARGEABLE_WEIGHT) AS CWEIGHT, " ;
            strSql = strSql + " SUM(NET_WEIGHT) AS NWEIGHT, " ;
            strSql = strSql + " SUM(PACK_COUNT) AS PACKCOUNT " ;
            strSql = strSql + " FROM JOB_TRN_CONT JOBT " ;
            strSql = strSql + " WHERE JOBT.JOB_CARD_TRN_FK = " + jobPk ;
            try
            {
                return (objWF.GetDataSet(strSql));
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
        #endregion

        #region " Fetch Volume & Weight"
        public long CalulateTransAmt(string jobPk = "0")
        {
            string strSql = null;
            strSql = "  SELECT sum(ROUND(q.AMT * (case " ;
            strSql = strSql + "  when q.CURRENCY_MST_FK = corp.currency_mst_fk then " ;
            strSql = strSql + "   1 else (select exch.exchange_rate from exchange_rate_trn exch " ;
            strSql = strSql + "  where q.jobcard_date BETWEEN exch.from_date AND " ;
            strSql = strSql + "  NVL(exch.to_date, round(sysdate - .5)) " ;
            strSql = strSql + "  AND q.CURRENCY_MST_FK = exch.currency_mst_fk) " ;
            strSql = strSql + "  end), 2)) \"AMOUNT\" " ;
            strSql = strSql + "  FROM (SELECT JFD.FREIGHT_AMT \"AMT\", " ;
            strSql = strSql + "  FROM (SELECT JFD.FREIGHT_AMT \"AMT\",JFD.CURRENCY_MST_FK,J.JOBCARD_DATE  FROM HBL_EXP_TBL H, " ;
            strSql = strSql + "  JOB_CARD_TRN  J,JOB_TRN_FD    JFD,CURRENCY_TYPE_MST_TBL CUR " ;
            strSql = strSql + "  WHERE J.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK" ;
            strSql = strSql + "  AND JFD.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK " ;
            strSql = strSql + "  AND JFD.FREIGHT_TYPE = 1" ;
            strSql = strSql + "  AND J.JOB_CARD_TRN_PK = " + jobPk ;
            strSql = strSql + "  UNION " ;
            strSql = strSql + "  SELECT JOC.AMOUNT \"AMT\", JOC.CURRENCY_MST_FK, J.JOBCARD_DATE FROM HBL_EXP_TBL H, " ;
            strSql = strSql + "  JOB_CARD_TRN  J,JOB_TRN_OTH_CHRG JOC,CURRENCY_TYPE_MST_TBL  CUR " ;
            strSql = strSql + "  WHERE J.JOB_CARD_TRN_PK = JOC.JOB_CARD_TRN_FK " ;
            strSql = strSql + "  AND JOC.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK AND JOC.FREIGHT_TYPE = 1 " ;
            strSql = strSql + "  AND J.JOB_CARD_TRN_PK = " + jobPk ;
            strSql = strSql + "  ) q,corporate_mst_tbl corp; " ;
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(strSql));
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
        #endregion

        #region " Fetch All With Hbl Pk"
        public DataSet FetchAll(string HBLPk = "0", int Status = 0)
        {
            string strSQl = null;
            strSQl = "SELECT " ;
            strSQl += " BOOK.CARGO_TYPE, " ;
            strSQl += " HBL.HBL_EXP_TBL_PK," ;
            strSQl += " JOB.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_FK," ;
            strSQl += " JOB.JOBCARD_REF_NO," ;
            strSQl += " HBL.HBL_REF_NO," ;
            strSQl += " HBL.HBL_DATE," ;
            strSQl += " HBL.VOYAGE_TRN_FK \"FIRSTVOYFK\"," ;
            strSQl += " HBL.VESSEL_NAME," ;
            strSQl += " HBL.VESSEL_ID," ;
            strSQl += " OPR.OPERATOR_ID, OPR.OPERATOR_NAME," ;
            strSQl += " opr.operator_mst_pk," ;
            strSQl += " HBL.VOYAGE, " ;
            strSQl += " FIRSTVOY.pod_eta," ;
            strSQl += " FIRSTVOY.pol_etd," ;
            strSQl += " HBL.ARRIVAL_DATE," ;
            strSQl += " HBL.DEPARTURE_DATE," ;
            strSQl += " HBL.SEC_VOYAGE_TRN_FK \"SECVOYFK\"," ;
            strSQl += " SECVSL.VESSEL_NAME \"SEC_VESSEL_NAME\"," ;
            strSQl += " SECVSL.VESSEL_ID \"SEC_VESSEL_ID\"," ;
            strSQl += " SECVOY.VOYAGE \"SEC_VOYAGE\"," ;
            strSQl += " HBL.SEC_ETA_DATE," ;
            strSQl += " HBL.SEC_ETD_DATE," ;
            strSQl += " HBL.SHIPPER_CUST_MST_FK," ;
            strSQl += " SH.customer_name AS SHIPPER," ;
            strSQl += "         TEMP_CONS.CUSTOMER_MST_PK AS CONSIGNEEPK_TEMP," ;
            strSQl += "      TMPNOTIFY1.CUSTOMER_MST_PK AS NOTIFY1PK_TEMP," ;
            strSQl += "       TMPNOTIFY2.CUSTOMER_MST_PK AS NOTIFY2PK_TEMP," ;
            strSQl += " nvl(HBL.CONSIGNEE_CUST_MST_FK,TEMP_CONS.CUSTOMER_MST_PK)as CONSIGNEE_CUST_MST_FK," ;
            //  strSQl &= " HBL.CONSIGNEE_CUST_MST_FK," & vbCrLf


            strSQl += " (case when hbl.is_to_order = '0' then nvl(CO.customer_name,TEMP_CONS.customer_name) else 'To Order' end) as CONSIGNEE_NAME, " ;
            strSQl += " nvl( hbl.consignee_name,TEMP_CONS.customer_name) AS CONSIGNEE," ;
            strSQl += " nvl(hbl.consignee_name,TEMP_CONS.customer_name) AS CONSIGNEE_TOORDER," ;


            //strSQl &= " (case when hbl.is_to_order = '0' then CO.customer_name else 'To Order' end) as CONSIGNEE_NAME, " & vbCrLf
            //strSQl &= " CO.customer_name AS CONSIGNEE," & vbCrLf
            //strSQl &= " hbl.consignee_name AS CONSIGNEE_TOORDER," & vbCrLf
            strSQl += " HBL.IS_TO_ORDER, " ;

            strSQl += " case  when NVL( hbl.sac_n1,0) = '0' then nvl( HBL.NOTIFY1_CUST_MST_FK,TMPNOTIFY1.CUSTOMER_MST_PK) else null end as NOTIFY1_CUST_MST_FK ," ;
            strSQl += "(case  when NVL( hbl.sac_n1,0) = '0' then   nvl(  N1.customer_name,TMPNOTIFY1.customer_name)  else 'SAME AS CONSIGNEE' end) as NOTIFY1," ;

            strSQl += " case  when NVL( hbl.sac_n2,0) = '0' then  nvl( HBL.NOTIFY2_CUST_MST_FK,TMPNOTIFY2.CUSTOMER_MST_PK) else null end as  NOTIFY2_CUST_MST_FK," ;
            strSQl += "       (case when NVL(hbl.sac_n2,0) = '0' then nvl(N2.customer_name,TMPNOTIFY2.customer_name) else 'SAME AS CONSIGNEE' end) as NOTIFY2," ;

            //  strSQl &= " HBL.NOTIFY1_CUST_MST_FK," & vbCrLf
            //  strSQl &= " N1.customer_name AS NOTIFY1," & vbCrLf
            //  strSQl &= " HBL.NOTIFY2_CUST_MST_FK," & vbCrLf
            //  strSQl &= " N2.customer_name AS NOTIFY2," & vbCrLf
            strSQl += " HBL.CB_AGENT_MST_FK," ;
            strSQl += " CB.agent_name as CBAGENT," ;
            strSQl += " HBL.DP_AGENT_MST_FK," ;
            strSQl += " DP.agent_name AS DPAGENT," ;
            strSQl += " HBL.CL_AGENT_MST_FK," ;
            strSQl += " CL.agent_name AS CLAGENT," ;
            strSQl += " POL.PORT_ID AS POL," ;
            strSQl += " POD.port_mst_pk as \"PODPK\"," ;
            strSQl += " POL.port_mst_pk as \"POLPK\"," ;
            strSQl += " POD.PORT_ID AS POD," ;
            strSQl += " CASE WHEN COLP.PLACE_CODE IS NOT NULL THEN COLP.PLACE_CODE ELSE POL.PORT_ID END RECP,";
            strSQl += " CASE WHEN DELP.PLACE_CODE IS NOT NULL THEN DELP.PLACE_CODE ELSE POD.PORT_ID END DELP,";
            strSQl += " HBL.GOODS_DESCRIPTION,";
            strSQl += " HBL.TOTAL_VOLUME, " ;
            strSQl += " HBL.TOTAL_CHARGE_WEIGHT," ;
            strSQl += " HBL.TOTAL_GROSS_WEIGHT," ;
            strSQl += " HBL.TOTAL_PACK_COUNT, " ;
            strSQl += " HBL.TOTAL_NET_WEIGHT, " ;
            strSQl += " HBL.HBL_ORIGINAL_PRINTS, " ;
            strSQl += " HBL.MARKS_NUMBERS," ;
            strSQl += " CUST.CUSTOMER_NAME," ;
            strSQl += " SH.inco_code, " ;
            strSQl += " MOV.cargo_move_code, " ;
            strSQl += " JOB.PYMT_TYPE, " ;
            strSQl += " HBL.HBL_STATUS," ;
            strSQl += " job.job_card_status," ;
            strSQl += " HBL.LETTER_OF_CREDIT," ;
            strSQl += " HBL.VERSION_NO," ;

            //GOPI
            strSQl += " HBL.SHIPPER_ADDRESS," ;
            strSQl += " HBL.CONSIGNEE_ADDRESS," ;
            //strSQl &= " case  when NVL( hbl.sac_n1,0) = '0' THEN HBL.NOTIFY1_ADDRESS else null end NOTIFY1_ADDRESS," & vbCrLf
            //strSQl &= " case  when NVL( hbl.sac_n2,0) = '0' THEN HBL.NOTIFY2_ADDRESS else null end NOTIFY2_ADDRESS," & vbCrLf


            strSQl += "   hbl.NOTIFY1_ADDRESS," ;
            strSQl += "  HBL.NOTIFY2_ADDRESS," ;

            strSQl += " HBL.CB_AGENT_ADDRESS," ;
            strSQl += " HBL.CL_AGENT_ADDRESS," ;
            strSQl += " HBL.DP_AGENT_ADDRESS," ;
            strSQl += "  HBL.LC_NUMBER,";
            strSQl += "  HBL.LC_DATE,";
            strSQl += "  HBL.LC_EXPIRES_ON, HBL.PLACE_ISSUE, ";
            strSQl += " CASE WHEN COLP.PLACE_NAME IS NOT NULL THEN COLP.PLACE_NAME ELSE POL.PORT_NAME END PLRCONTY,";
            strSQl += " CASE WHEN DELP.PLACE_NAME IS NOT NULL THEN DELP.PLACE_NAME ELSE POD.PORT_NAME END PLDCONTY,";
            strSQl += " POL.PORT_NAME AS POLCONTY," ;
            strSQl += " POD.PORT_NAME AS PODCONTY," ;
            strSQl += " HBL.SURRENDER_DT, ";
            strSQl += " BOOK.BOOKING_DATE ";

            strSQl += " FROM HBL_EXP_TBL HBL," ;


            strSQl += "   TEMP_CUSTOMER_TBL      TMPNOTIFY2," ;
            strSQl += "     TEMP_CUSTOMER_TBL      TMPNOTIFY1," ;
            strSQl += "      TEMP_CUSTOMER_TBL      TEMP_CONS," ;
            strSQl += " OPERATOR_MST_TBL OPR," ;
            strSQl += " JOB_CARD_TRN JOB," ;
            strSQl += " BOOKING_MST_TBL BOOK," ;
            strSQl += " CUSTOMER_MST_TBL SH," ;
            strSQl += " CUSTOMER_MST_TBL CO," ;
            strSQl += " CUSTOMER_MST_TBL N1," ;
            strSQl += " CUSTOMER_MST_TBL N2," ;
            strSQl += " AGENT_MST_TBL CB," ;
            strSQl += " AGENT_MST_TBL DP," ;
            strSQl += " AGENT_MST_TBL CL," ;
            strSQl += " PORT_MST_TBL POL," ;
            strSQl += " PORT_MST_TBL POD," ;
            strSQl += " CUSTOMER_MST_TBL CUST," ;
            strSQl += " PLACE_MST_TBL COLP," ;
            strSQl += " PLACE_MST_TBL DELP," ;
            strSQl += " VESSEL_VOYAGE_TRN FIRSTVOY," ;
            strSQl += " VESSEL_VOYAGE_TRN SECVOY, " ;
            strSQl += " VESSEL_VOYAGE_TBL FIRSTVSL," ;
            strSQl += " VESSEL_VOYAGE_TBL SECVSL," ;
            strSQl += " CARGO_MOVE_MST_TBL MOV," ;
            strSQl += " SHIPPING_TERMS_MST_TBL SH" ;

            strSQl += " WHERE" ;
            strSQl += " JOB.BOOKING_MST_FK      =  BOOK.BOOKING_MST_PK " ;
            strSQl += " AND OPR.OPERATOR_MST_PK(+)  = BOOK.CARRIER_MST_FK " ;
            strSQl += " AND SH.CUSTOMER_MST_PK(+)  =  HBL.SHIPPER_CUST_MST_FK" ;
            strSQl += " AND CO.CUSTOMER_MST_PK(+)  =  HBL.CONSIGNEE_CUST_MST_FK " ;
            strSQl += " AND N1.CUSTOMER_MST_PK(+)  =  HBL.NOTIFY1_CUST_MST_FK" ;
            strSQl += " AND N2.CUSTOMER_MST_PK(+)  =  HBL.NOTIFY2_CUST_MST_FK" ;
            strSQl += " AND CB.AGENT_MST_PK(+)     =  HBL.CB_AGENT_MST_FK" ;
            strSQl += " AND DP.AGENT_MST_PK(+)     =  HBL.DP_AGENT_MST_FK" ;
            strSQl += " AND CL.AGENT_MST_PK(+)     =  HBL.CL_AGENT_MST_FK" ;
            strSQl += " AND POL.PORT_MST_PK(+)     =  BOOK.PORT_MST_POL_FK" ;
            strSQl += " AND POD.PORT_MST_PK(+)     =  BOOK.PORT_MST_POD_FK" ;
            strSQl += " and MOV.CARGO_MOVE_PK(+) = JOB.CARGO_MOVE_FK  " ;
            strSQl += " and SH.SHIPPING_TERMS_MST_PK(+)=job.SHIPPING_TERMS_MST_FK  " ;
            strSQl += " AND COLP.PLACE_PK(+)       =  BOOK.COL_PLACE_MST_FK" ;
            strSQl += " AND DELP.PLACE_PK(+)       =  BOOK.DEL_PLACE_MST_FK" ;
            strSQl += " AND CUST.CUSTOMER_MST_PK(+) =  BOOK.CUST_CUSTOMER_MST_FK" ;
            strSQl += "    AND HBL.consignee_cust_mst_fk = TEMP_CONS.CUSTOMER_MST_PK(+)" ;
            strSQl += "   AND HBL.notify1_cust_mst_fk=TMPNOTIFY1.CUSTOMER_MST_PK(+)" ;
            strSQl += "   AND HBL.notify2_cust_mst_fk=TMPNOTIFY2.CUSTOMER_MST_PK(+)" ;
            if (Status == 3)
            {
                strSQl += " AND HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK " ;
            }
            else
            {
                strSQl += " AND HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK " ;
            }
            strSQl += " AND HBL.SEC_VOYAGE_TRN_FK   = SECVOY.VOYAGE_TRN_PK(+)" ;
            strSQl += " AND HBL.VOYAGE_TRN_FK       = FIRSTVOY.VOYAGE_TRN_PK(+)" ;
            strSQl += " AND FIRSTVOY.VESSEL_VOYAGE_TBL_FK = FIRSTVSL.VESSEL_VOYAGE_TBL_PK(+)" ;
            strSQl += " AND SECVOY.VESSEL_VOYAGE_TBL_FK = SECVSL.VESSEL_VOYAGE_TBL_PK(+)" ;
            strSQl += " AND HBL.HBL_EXP_TBL_PK= " + HBLPk;
            try
            {
                return (objWF.GetDataSet(strSQl));
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
        #endregion

        #region " Generate HBL No"
        public string genrateHBLNo(long Location, long emp, long User)
        {
            return GenerateProtocolKey("HBL", Location, emp, DateTime.Today, "", "", "", User, new WorkFlow());
        }
        #endregion

        #region " Save Main"
        public ArrayList save(DataTable dtMain, long nLocationfk, long nUserfk, string strPk, string JobPk, string HBLNo, string CBAgentPK, string CLAgentPK, string ConsigneePK, string DPAgentPK,
        string VESSEL_ID, string FirstVoyageFk, string FirstVessel, string FirstVoyage, string GoodDesc, string Marks, string Notify1PK, string Notify2PK, string OperatorPk, string sOperator,
        string SecondVoyageFk, string SecondVessel, string SecondVoyage, string ShipperPK, string Shipperaddress, string Consigneeaddress, string Notify1address, string Notify2address, string CBAgentaddress, string DPAgentaddress,
        string CLAgentaddress, string PODPk, string POLPk, string PackCount, string Gweight, string NWeight, string CWeight, string TotVol, string HDate, string ETA_DATE,
        string ETD_DATE, string SEC_ETA_DATE, string SEC_ETD_DATE, string ARRIVAL_DATE, string DEPARTURE_DATE, string cargo_move, string pymt_type, string shipping_terms, string EmpPk, bool Is_To_Order,
        string ConsigneeName, string status = "0", string Version = "0", string Letter = "", string TotalPrints = "", bool fclFlag = true, string strTransaction = "", string hdnadd = "", string Corrector = "", string LCNumber = "",
        string LCDate = "", string LCExpiresOn = "", string PLACE_ISSUE = "", string PLRCountry = "", string POLCountry = "", string PODCountry = "", string PFDCountry = "", string SurrDt = "", int SAC_N1 = 0, int SAC_N2 = 0,
        string sid = "", string polid = "", string podid = "", string COD = "", int pfdfk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            cls_SeaBookingEntry objSBE = new cls_SeaBookingEntry();
            cls_HBL_Entry objHBLEntry = new cls_HBL_Entry();
            Int16 exe = default(Int16);
            objWK.OpenConnection();
            OracleTransaction TRAN = null;

            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            int intPKVal = 0;
            long lngI = 0;
            string strVoyagepk = "";
            strVoyagepk = FirstVoyageFk;
            Int32 RecAfct = default(Int32);
            bool chkFlag = false;
            int CrctrBLPK = 0;
            string Cversion = null;
            string NewHBLnr = null;
            int a = 0;
            int b = 0;

            intPKVal = Convert.ToInt32(strPk);


            try
            {
                if (((string.IsNullOrEmpty(FirstVoyageFk) || FirstVoyageFk == "0") & !string.IsNullOrEmpty(VESSEL_ID)) | COD == "YES")
                {
                    FirstVoyageFk = "0";
                    objSBE.CREATED_BY = CREATED_BY;
                    objSBE.ConfigurationPK = ConfigurationPK;
                    if (!string.IsNullOrEmpty(FirstVessel) & !string.IsNullOrEmpty(VESSEL_ID) & !string.IsNullOrEmpty(FirstVoyage))
                    {
                        arrMessage = objSBE.SaveVesselMaster(Convert.ToInt64(FirstVoyageFk), Convert.ToString(getDefault(FirstVessel, "")), Convert.ToInt64(getDefault(OperatorPk, 0)), Convert.ToString(getDefault(VESSEL_ID, "")), Convert.ToString(getDefault(FirstVoyage, "")), objWK.MyCommand, Convert.ToInt64(getDefault(POLPk, 0)), Convert.ToString(PODPk), DateTime.MinValue, Convert.ToDateTime(getDefault(ETD_DATE, null)),DateTime.MinValue, Convert.ToDateTime(getDefault(ETA_DATE, null)), DateTime.MinValue, DateTime.MinValue);
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

                if (string.IsNullOrEmpty(strPk) | strPk == "0")
                {
                    HBLNo = objHBLEntry.genrateHBLNo(nLocationfk, Convert.ToInt64(EmpPk), nUserfk, sid, polid, podid);
                    strPk = "0";
                }
                //Added by Faheem
                if (Corrector == "YES" | COD == "YES")
                {
                    CrctrBLPK = Convert.ToInt32(strPk);
                    strPk = "0";
                    if (string.Compare(HBLNo, "-V") > 0)
                    {
                        a = string.Compare(HBLNo, "-V");
                        b = Convert.ToInt32(HBLNo.Substring(a + 1, 1));
                        Cversion = string.Concat("V", b + 1);
                        NewHBLnr = HBLNo.Replace(string.Concat("V", b), Cversion);
                        HBLNo = NewHBLnr;
                    }
                    else
                    {
                        Cversion = "-V1";
                        HBLNo = string.Concat(HBLNo, Cversion);
                    }
                }
                //End
                var _with3 = objWK.MyCommand;
                if (Convert.ToInt32(strPk) != 0)
                {
                    _with3.CommandText = objWK.MyUserName + ".BB_HBL_EXP_TBL_PKG.HBL_EXP_TBL_UPD";
                }
                else
                {
                    _with3.CommandText = objWK.MyUserName + ".BB_HBL_EXP_TBL_PKG.HBL_EXP_TBL_INS";
                    chkFlag = true;
                    //added by surya prasad for protocol rolllback
                }
                if (fclFlag == true)
                {
                    CWeight = "0";
                }
                else
                {
                    NWeight = "0";
                }
                short deleteflag = 0;
                if (strTransaction.Trim().Length > 0)
                {
                    deleteflag = 1;
                }

                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.Transaction = TRAN;
                var _with4 = _with3.Parameters;
                if (strPk != "0")
                {
                    _with4.Add("HBL_EXP_TBL_PK_IN", Convert.ToInt64(strPk));
                    _with4.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY);
                    //        
                    _with4.Add("VERSION_NO_IN", Convert.ToInt64(Version));
                    _with4.Add("DELETE_CLAUSE_FLAG_IN", deleteflag);
                }
                else
                {
                    _with4.Add("CREATED_BY_FK_IN", CREATED_BY);
                    //Added by Faheem
                    if (Corrector == "YES")
                    {
                        _with4.Add("CORRECTOR_HBL_FK_IN", CrctrBLPK);
                    }
                    //End
                    if (COD == "YES")
                    {
                        _with4.Add("CORRECTOR_HBL_FK_IN", CrctrBLPK);
                        _with4.Add("POD_MST_FK_IN", PODPk);
                        _with4.Add("PFD_MST_FK_IN", pfdfk);
                        _with4.Add("OPERATOR_FK_IN", OperatorPk);
                    }
                }
                _with4.Add("JOB_CARD_SEA_EXP_FK_IN", (string.IsNullOrEmpty(JobPk) ? 0 : Convert.ToInt64(JobPk)));
                _with4.Add("HBL_REF_NO_IN", getDefault((HBLNo), DBNull.Value));
                _with4.Add("HBL_DATE_IN", getDefault(HDate, DBNull.Value));
                _with4.Add("FIRST_VSLVOY_FK_IN", getDefault((strVoyagepk), DBNull.Value));
                _with4.Add("VESSEL_NAME_IN", getDefault((FirstVessel), DBNull.Value));
                _with4.Add("VESSEL_ID_IN", getDefault((VESSEL_ID), DBNull.Value));
                _with4.Add("VOYAGE_IN", getDefault((FirstVoyage), DBNull.Value));
                _with4.Add("ETA_DATE_IN", getDefault(ETA_DATE, DBNull.Value));
                _with4.Add("ETD_DATE_IN", getDefault(ETD_DATE, DBNull.Value));
                _with4.Add("ARRIVAL_DATE_IN", getDefault(ARRIVAL_DATE, DBNull.Value));
                _with4.Add("DEPARTURE_DATE_IN", getDefault(DEPARTURE_DATE, DBNull.Value));
                _with4.Add("SECOND_VSLVOY_FK_IN", getDefault((SecondVoyageFk), DBNull.Value));
                _with4.Add("SEC_VESSEL_NAME_IN", getDefault((SecondVessel), DBNull.Value));
                _with4.Add("SEC_VOYAGE_IN", getDefault((SecondVoyage), DBNull.Value));
                _with4.Add("SEC_ETA_DATE_IN", getDefault(SEC_ETA_DATE, DBNull.Value));
                _with4.Add("SEC_ETD_DATE_IN", getDefault(SEC_ETD_DATE, DBNull.Value));
                _with4.Add("SHIPPER_CUST_MST_FK_IN", getDefault((ShipperPK), DBNull.Value));
                _with4.Add("CONSIGNEE_CUST_MST_FK_IN", getDefault((ConsigneePK), DBNull.Value));
                _with4.Add("NOTIFY1_CUST_MST_FK_IN", getDefault((Notify1PK), DBNull.Value));
                _with4.Add("NOTIFY2_CUST_MST_FK_IN", getDefault((Notify2PK), DBNull.Value));
                _with4.Add("CB_AGENT_MST_FK_IN", getDefault((CBAgentPK), DBNull.Value));
                _with4.Add("DP_AGENT_MST_FK_IN", getDefault((DPAgentPK), DBNull.Value));
                _with4.Add("CL_AGENT_MST_FK_IN", getDefault((CLAgentPK), DBNull.Value));
                _with4.Add("SHIPPER_ADDRESS_IN", (string.IsNullOrEmpty(Shipperaddress) ? "" : Shipperaddress));
                //If Is_To_Order = True Then
                //    .Add("CONSIGNEE_ADDRESS_IN", IIf(hdnadd = "", DBNull.Value, hdnadd))
                //Else
                //    .Add("CONSIGNEE_ADDRESS_IN", IIf(Consigneeaddress = "", DBNull.Value, Consigneeaddress))
                //End If
                _with4.Add("CONSIGNEE_ADDRESS_IN", (string.IsNullOrEmpty(Consigneeaddress) ? "" : Consigneeaddress));
                _with4.Add("NOTIFY1_ADDRESS_IN", (string.IsNullOrEmpty(Notify1address) ? "" : Notify1address));
                _with4.Add("NOTIFY2_ADDRESS_IN", (string.IsNullOrEmpty(Notify2address) ? "" : Notify2address));
                _with4.Add("CB_AGENT_ADDRESS_IN", (string.IsNullOrEmpty(CBAgentaddress) ? "" : CBAgentaddress));
                _with4.Add("DP_AGENT_ADDRESS_IN", (string.IsNullOrEmpty(DPAgentaddress) ? "" : DPAgentaddress));
                _with4.Add("CL_AGENT_ADDRESS_IN", (string.IsNullOrEmpty(CLAgentaddress) ? "" : CLAgentaddress));

                _with4.Add("MARKS_NUMBERS_IN", getDefault((Marks), DBNull.Value));
                _with4.Add("GOODS_DESCRIPTION_IN", getDefault((GoodDesc), DBNull.Value));
                //added by gopi
                _with4.Add("CARGO_MOVE_IN", (string.IsNullOrEmpty(cargo_move) ? "" : cargo_move));
                _with4.Add("PYMT_TYPE_IN", (pymt_type == "Collect" ? "2" : "1"));
                //ifDBNull(pymt_type))
                _with4.Add("SHIPPING_TERMS_IN", (string.IsNullOrEmpty(shipping_terms) ? "" : shipping_terms));
                if (Is_To_Order == true)
                {
                    _with4.Add("CONSIGNEE_NAME_IN", DBNull.Value);
                }
                else
                {
                    _with4.Add("CONSIGNEE_NAME_IN", getDefault((ConsigneeName), DBNull.Value));
                    // added by gopi for saving consignee name 
                }
                _with4.Add("Is_To_Order_IN", (Is_To_Order == true ? "1" : "0"));

                _with4.Add("HBL_STATUS_IN", Convert.ToInt64(status));
                _with4.Add("MBL_EXP_TBL_FK_IN", DBNull.Value);
                //.Add("TOTAL_VOLUME_IN", ifDBNull(CDbl(TotVol)))
                _with4.Add("TOTAL_VOLUME_IN", DBNull.Value);
                //.Add("TOTAL_GROSS_WEIGHT_IN", ifDBNull(CDbl(Gweight)))
                _with4.Add("TOTAL_GROSS_WEIGHT_IN", DBNull.Value);
                if ((string.IsNullOrEmpty(NWeight)))
                {
                    _with4.Add("TOTAL_NET_WEIGHT_IN", DBNull.Value);
                }
                else
                {
                    _with4.Add("TOTAL_NET_WEIGHT_IN", ifDBNull(Convert.ToDouble(NWeight)));
                }
                //.Add("TOTAL_NET_WEIGHT_IN", IIf(NWeight = "", DBNull.Value, CDbl(NWeight))) 'ifDBNull(CDbl(NWeight)))
                if ((string.IsNullOrEmpty(CWeight)))
                {
                    _with4.Add("TOTAL_CHARGE_WEIGHT_IN", DBNull.Value);
                }
                else
                {
                    _with4.Add("TOTAL_CHARGE_WEIGHT_IN", ifDBNull(Convert.ToDouble(CWeight)));
                }
                //.Add("TOTAL_CHARGE_WEIGHT_IN", ifDBNull(CDbl(CWeight)))
                _with4.Add("TOTAL_PACK_COUNT_IN", getDefault((PackCount), DBNull.Value));
                _with4.Add("HBL_ORIGINAL_PRINTS_IN", getDefault((TotalPrints), DBNull.Value));
                _with4.Add("REMARKS_IN", getDefault((HBLNo), DBNull.Value));
                _with4.Add("CONFIG_MST_PK_IN", Convert.ToInt64(ConfigurationPK));
                //'
                _with4.Add("LETTER_OF_CREDIT_IN", getDefault((Letter), DBNull.Value));
                _with4.Add("LC_NUMBER_IN", getDefault((LCNumber), DBNull.Value));
                _with4.Add("LC_DATE_IN", getDefault((LCDate), DBNull.Value));
                _with4.Add("LC_EXPIRES_ON_IN", getDefault((LCExpiresOn), DBNull.Value));
                _with4.Add("PLACE_ISSUE_IN", getDefault((PLACE_ISSUE), DBNull.Value));
                _with4.Add("PLR_COUNTRY_IN", getDefault((PLRCountry), DBNull.Value));
                _with4.Add("POL_COUNTRY_IN", getDefault((POLCountry), DBNull.Value));
                _with4.Add("POD_COUNTRY_IN", getDefault((PODCountry), DBNull.Value));
                _with4.Add("PFD_COUNTRY_IN", getDefault((PFDCountry), DBNull.Value));
                //'
                _with4.Add("SURRENDER_DT_IN", getDefault(SurrDt, DBNull.Value));

                _with4.Add("SAC_N1_IN", SAC_N1);
                _with4.Add("SAC_N2_IN", SAC_N2);

                _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_EXP_TBL_PK").Direction = ParameterDirection.Output;

                exe = Convert.ToInt16(_with3.ExecuteNonQuery());
                strPk = Convert.ToString(_with3.Parameters["RETURN_VALUE"].Value);
                //If strPk = 0 Then
                //    TRAN.Rollback()
                //    arrMessage.Add("HBL Already Exists")
                //    Return arrMessage
                //End If
                if (exe > 0)
                {
                    //*****Save BBC Commodity******
                    int CommPk = 0;
                    foreach (DataRow _row in dtMain.Rows)
                    {
                        CommPk = 0;
                        try
                        {
                            CommPk = Convert.ToInt32(_row["JOB_TRN_CONT_PK"]);
                        }
                        catch (Exception ex)
                        {
                        }
                        if (CommPk > 0)
                        {
                            var _with5 = objWK.MyCommand;
                            _with5.CommandType = CommandType.StoredProcedure;
                            _with5.Parameters.Clear();
                            _with5.CommandText = objWK.MyUserName + ".BOOKING_COMMODITY_DTL_PKG.HBL_BBC_COMMODITY_DTL_UPD";

                            _with5.Parameters.Add("JOB_TRN_CONT_PK_IN", _row["JOB_TRN_CONT_PK"]).Direction = ParameterDirection.Input;
                            _with5.Parameters["JOB_TRN_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                            _with5.Parameters.Add("PACK_TYPE_FK_IN", _row["PACK_TYPE_MST_PK"]).Direction = ParameterDirection.Input;
                            _with5.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                            _with5.Parameters.Add("PACK_COUNT_IN", _row["PACK_COUNT"]).Direction = ParameterDirection.Input;
                            _with5.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                            if (string.IsNullOrEmpty(_row["CHARGEABLE_WEIGHT"].ToString()))
                            {
                                _with5.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                _with5.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                            }
                            else
                            {
                                _with5.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(_row["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;
                                _with5.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                            }
                            _with5.Parameters.Add("VOLUME_IN_CBM_IN", _row["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                            _with5.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;
                            _with5.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                            _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with5.ExecuteNonQuery();
                            CommPk = Convert.ToInt32(string.IsNullOrEmpty(_with5.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with5.Parameters["RETURN_VALUE"].Value.ToString());
                        }
                    }
                    //*****************************
                    //Callinf Track And Trace Functionality
                    arrMessage.Add("All Data Saved Successfully");
                    if (strTransaction.Trim().Length > 0)
                    {
                        if (strTransaction != "undefined")
                        {
                            arrMessage = (ArrayList)SaveTransaction(strTransaction, objWK.MyCommand, Convert.ToInt32(strPk));
                        }
                        else if (strTransaction.Trim().Length == 0 | strTransaction == "undefined")
                        {
                        }
                    }
                    if (Corrector == "YES")
                    {
                        arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(strPk), 2, 1, "HBL", "HBL-INS", Convert.ToInt32(nLocationfk), objWK, "INS", CREATED_BY, "O");
                    }
                    else
                    {
                        if (intPKVal == 0)
                        {
                            arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(strPk), 2, 1, "HBL", "HBL-INS", Convert.ToInt32(nLocationfk), objWK, "INS", CREATED_BY, "O");
                        }
                    }
                    //If intPKVal = 0 Then
                    //    arrMessage = objTrackNTrace.SaveBBTrackAndTrace(strPk, 2, 1, "HBL", "HBL-INS", nLocationfk, objWK, "INS", CREATED_BY, "O")
                    //End If
                    //To update the booking with the new values in case of cod.
                    if (COD == "YES")
                    {
                        OracleCommand updBKGCommand = new OracleCommand();
                        var _with6 = updBKGCommand;
                        _with6.Parameters.Clear();
                        _with6.Connection = objWK.MyConnection;
                        _with6.CommandType = CommandType.StoredProcedure;
                        _with6.Transaction = TRAN;
                        _with6.CommandText = objWK.MyUserName + ".HBL_EXP_TBL_PKG.HBL_BKG_UPD";
                        var _with7 = _with6.Parameters;
                        _with7.Add("JOB_CARD_SEA_EXP_FK_IN", (string.IsNullOrEmpty(JobPk) ? 0 : Convert.ToInt64(JobPk)));
                        _with7.Add("FIRST_VSLVOY_FK_IN", getDefault((strVoyagepk), DBNull.Value));
                        _with7.Add("ETA_DATE_IN", getDefault(ETA_DATE, DBNull.Value));
                        _with7.Add("ETD_DATE_IN", getDefault(ETD_DATE, DBNull.Value));
                        _with7.Add("ARRIVAL_DATE_IN", getDefault(ARRIVAL_DATE, DBNull.Value));
                        _with7.Add("DEPARTURE_DATE_IN", getDefault(DEPARTURE_DATE, DBNull.Value));
                        _with7.Add("VESSEL_NAME_IN", getDefault((FirstVessel), DBNull.Value));
                        _with7.Add("VOYAGE_IN", getDefault((FirstVoyage), DBNull.Value));
                        _with7.Add("CONSIGNEE_CUST_MST_FK_IN", getDefault((ConsigneePK), DBNull.Value));
                        _with7.Add("NOTIFY1_CUST_MST_FK_IN", getDefault((Notify1PK), DBNull.Value));
                        _with7.Add("NOTIFY2_CUST_MST_FK_IN", getDefault((Notify2PK), DBNull.Value));
                        _with7.Add("DP_AGENT_MST_FK_IN", getDefault((DPAgentPK), DBNull.Value));
                        _with7.Add("CONSIGNEE_ADDRESS_IN", (string.IsNullOrEmpty(Consigneeaddress) ? "" : Consigneeaddress));
                        _with7.Add("NOTIFY1_ADDRESS_IN", (string.IsNullOrEmpty(Notify1address) ? "" : Notify1address));
                        _with7.Add("NOTIFY2_ADDRESS_IN", (string.IsNullOrEmpty(Notify2address) ? "" : Notify2address));
                        _with7.Add("DP_AGENT_ADDRESS_IN", (string.IsNullOrEmpty(DPAgentaddress) ? "" : DPAgentaddress));
                        _with7.Add("POD_MST_FK_IN", PODPk);
                        _with7.Add("PFD_MST_FK_IN", pfdfk);
                        _with7.Add("OPERATOR_FK_IN", OperatorPk);
                        _with7.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_EXP_TBL_PK").Direction = ParameterDirection.Output;
                        _with6.ExecuteNonQuery();
                    }
                    if (arrMessage.Count >= 1)
                    {
                        TRAN.Commit();
                        //Push to financial system if realtime is selected
                        string JCPKs = "0";
                        JCPKs = GetImportJCPKs(HBLNo);
                        if (JCPKs != "0")
                        {
                            cls_Scheduler objSch = new cls_Scheduler();
                            ArrayList schDtls = null;
                            bool errGen = false;
                            if (objSch.GetSchedulerPushType() == true)
                            {
                                //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                                //try
                                //{
                                //    schDtls = objSch.FetchSchDtls();
                                //    //'Used to Fetch the Sch Dtls
                                //    objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , JCPKs);
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                                //    }
                                //}
                                //catch (Exception ex)
                                //{
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                                //    }
                                //}
                            }
                        }
                        //*****************************************************************
                        return arrMessage;
                    }
                    else
                    {
                        //added by suryaprasad for protocol rollback
                        if (chkFlag)
                        {
                            RollbackProtocolKey("HBL", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), HBLNo, System.DateTime.Now);
                        }
                        TRAN.Rollback();
                    }
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                //added by suryaprasad for protocol rollback
                if (chkFlag)
                {
                    RollbackProtocolKey("HBL", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), HBLNo, System.DateTime.Now);
                }
                arrMessage.Clear();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                //added by suryaprasad for protocol rollback
                if (chkFlag)
                {
                    RollbackProtocolKey("HBL", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), HBLNo, System.DateTime.Now);
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }
        #endregion

        #region "Save Transaction"
        private object SaveTransaction(string str, OracleCommand InsertCmd, int Strpk)
        {
            Array arrRow = null;
            Array arrMain = null;
            string strRow = null;
            int arrLen = 0;
            DataSet dsTran = new DataSet();
            DataRow dRow = null;
            DataTable dtTran = null;
            int nStart = 0;
            string nEnd = null;
            Int32 RecAfct = default(Int32);

            nEnd = Convert.ToString(str.Length);
            str = str.Substring(0, Convert.ToInt32(nEnd) - 2);
            dsTran.Tables.Add("dtTran");
            dsTran.Tables["dtTran"].Columns.Add("HBL_EXP_TBL_FK");
            dsTran.Tables["dtTran"].Columns.Add("BL_CLAUSE_FK");
            dsTran.Tables["dtTran"].Columns.Add("BL_DESCRIPTION");
            arrMain = str.Split('#');
            for (arrLen = 0; arrLen <= arrMain.Length - 1; arrLen++)
            {
                strRow = Convert.ToString(arrMain.GetValue(arrLen));
                arrRow = strRow.Split('^');
                dRow = dsTran.Tables["dtTran"].NewRow();
                if (Convert.ToString(arrRow.GetValue(0)) == "null")
                {
                    dRow["HBL_EXP_TBL_FK"] = System.DBNull.Value;
                }
                else
                {
                    dRow["HBL_EXP_TBL_FK"] = Convert.ToString(arrRow.GetValue(0));
                }

                dRow["BL_DESCRIPTION"] = Convert.ToInt32(arrRow.GetValue(1));

                if (Convert.ToString(arrRow.GetValue(2)) == "null")
                {
                    dRow["BL_CLAUSE_FK"] = System.DBNull.Value;
                }
                else
                {
                    dRow["BL_CLAUSE_FK"] = Convert.ToString(arrRow.GetValue(2));
                }
                dsTran.Tables["dtTran"].Rows.Add(dRow);
            }

            try
            {
                WorkFlow objWK = new WorkFlow();

                var _with8 = InsertCmd;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".HBL_BL_CLAUSE_TBL_PKG.HBL_BL_CLAUSE_TBL_INS";
                _with8.Parameters.Clear();
                _with8.Parameters.Add("HBL_EXP_TBL_FK_IN", Strpk);
                _with8.Parameters.Add("BL_DESCRIPTION_IN", OracleDbType.Varchar2, 500, "BL_DESCRIPTION");
                _with8.Parameters.Add("BL_CLAUSE_FK_IN", OracleDbType.Int32, 10, "BL_CLAUSE_FK");
                _with8.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_BL_CLAUSE_TBL_PK").Direction = ParameterDirection.Output;
                var _with9 = objWK.MyDataAdapter;
                _with9.InsertCommand = InsertCmd;
                RecAfct = _with9.Update(dsTran.Tables["dtTran"]);
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
        #endregion

        #region " Other Functions"
        public int FindHblPk(string RefNo)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select h.hbl_exp_tbl_pk from hbl_exp_tbl h where h.hbl_ref_no = '" + RefNo + "' ";
                string HBLPK = null;
                HBLPK = objWF.ExecuteScaler(strSQL);
                return Convert.ToInt32(HBLPK);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
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

        #endregion

        #region " Fetch HBL Report Data"
        public DataSet FetchHBLData(int HBLPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "select JObCardSeaExp.JOB_CARD_TRN_PK JOBPK,";
            strSQL += "hbl.hbl_exp_tbl_pk HBPK,";
            strSQL += "JobCardSeaExp.Jobcard_Ref_No JOBNO,";
            strSQL += "JobCardSeaExp.Ucr_No UCRNO,";
            strSQL += "hbl.hbl_ref_no HBNO,";
            strSQL += "hbl.vessel_name VES_FLIGHT,";
            strSQL += "hbl.voyage voyage,";
            strSQL += "POL.PORT_NAME POL,";
            strSQL += "POD.PORT_NAME POD,";
            strSQL += "PMT.PLACE_NAME PLD,";
            strSQL += "CustMstShipper.Customer_Name Shipper,";
            strSQL += "CustShipperDtls.Adm_Address_1 ShiAddress1,";
            strSQL += "CustShipperDtls.Adm_Address_2 ShiAddress2,";
            strSQL += "CustShipperDtls.Adm_Address_3 ShiAddress3,";
            strSQL += "CustShipperDtls.Adm_City ShiCity,";
            strSQL += "CustMstConsignee.Customer_Name Consignee,";
            strSQL += "CustConsigneeDtls.Adm_Address_1 ConsiAddress1,";
            strSQL += "CustConsigneeDtls.Adm_Address_2 ConsiAddress2,";
            strSQL += "CustConsigneeDtls.Adm_Address_3 ConsiAddress3,";
            strSQL += "CustConsigneeDtls.Adm_City ConsiCity,";
            strSQL += "AgentMst.Agent_Name,";
            strSQL += "AgentDtls.Adm_Address_1 AgtAddress1,";
            strSQL += "AgentDtls.Adm_Address_2 AgtAddress2,";
            strSQL += "AgentDtls.Adm_Address_3 AgtAddress3,";
            strSQL += "AgentDtls.Adm_City AgtCity,";
            strSQL += "HBL.GOODS_DESCRIPTION";
            strSQL += "from JOB_CARD_TRN JobCardSeaExp,";
            strSQL += "hbl_exp_tbl HBL,";
            strSQL += " BOOKING_MST_TBL BkgSea,";
            strSQL += " Port_Mst_Tbl POL,";
            strSQL += " Port_Mst_Tbl POD,";
            strSQL += " Place_Mst_Tbl PMT,";
            strSQL += "Customer_Mst_Tbl CustMstShipper,";
            strSQL += "Customer_Mst_Tbl CustMstConsignee,";
            strSQL += " Agent_Mst_Tbl AgentMst,";
            strSQL += "Customer_Contact_Dtls CustShipperDtls,";
            strSQL += " Customer_Contact_Dtls CustConsigneeDtls,";
            strSQL += "  Agent_Contact_Dtls AgentDtls";
            strSQL += " where JobCardSeaExp.JOB_CARD_TRN_PK = hbl.job_card_sea_exp_fk";
            strSQL += "and   JobCardSeaExp.BOOKING_MST_FK=BkgSea.BOOKING_MST_PK";
            strSQL += "and   POL.PORT_MST_PK(+)=BkgSea.Port_Mst_Pol_Fk";
            strSQL += "and   POD.PORT_MST_PK(+)=BkgSea.Port_Mst_Pod_Fk";
            strSQL += "and   PMT.PLACE_PK(+)=BkgSea.Del_Place_Mst_Fk";
            strSQL += "and   HBL.Shipper_Cust_Mst_Fk=CustMstShipper.Customer_Mst_Pk(+)";
            strSQL += "and   HBL.Consignee_Cust_Mst_Fk=CustMstConsignee.Customer_Mst_Pk(+)";
            strSQL += "and   HBL.Dp_Agent_Mst_Fk=AgentMst.Agent_Mst_Pk(+)";
            strSQL += "and   CustMstShipper.Customer_Mst_Pk=CustShipperDtls.Customer_Mst_Fk(+)";
            strSQL += "and   CustMstConsignee.Customer_Mst_Pk=CustConsigneeDtls.Customer_Mst_Fk(+)";
            strSQL += "and   AgentMst.Agent_Mst_Pk=AgentDtls.Agent_Mst_Fk(+)";
            strSQL += "and   hbl.hbl_exp_tbl_pk=" + HBLPK;

            try
            {
                return (objWF.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }
        #endregion

        #region " save letter of credit"
        public ArrayList saveLetter(string strPk, string Letter, string version)
        {
            WorkFlow objWK = new WorkFlow();
            Int16 exe = default(Int16);
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand insCommand = new OracleCommand();
            insCommand.CommandText = objWK.MyUserName + ".HBL_EXP_TBL_PKG.HBL_EXP_TBL_CREDIT_UPD";
            try
            {
                insCommand.Connection = objWK.MyConnection;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.Transaction = TRAN;
                var _with10 = insCommand.Parameters;
                _with10.Add("HBL_EXP_TBL_PK_IN", strPk);
                _with10.Add("VERSION_NO_IN", version);
                // = Version_No + 1
                _with10.Add("LETTER_OF_CREDIT_IN", ifDBNull(Letter));
                _with10.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_EXP_TBL_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                exe = Convert.ToInt16(insCommand.ExecuteNonQuery());
                if (exe > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    strPk = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value);
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
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }
        #endregion

        ///Modified by koteswari on 24/1/2011 
        #region " Enhance Search & Lookup Search Block FOR HBL"
        public string FetchForJobBBHblRef(string strCond)
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
                //SCM.CommandText = objWF.MyUserName & ".EN_JOB_HBL_REF_NO_PKG.GET_JOB_HBL_REF_COMMON"
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_HBL_REF_NO_PKG.GET_JOB_BBHBL_REF_COMMON";
                var _with11 = SCM.Parameters;
                _with11.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with11.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with11.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with11.Add("RETURN_VALUE", OracleDbType.Varchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion

        // Added By Jitendra For Barcode 
        #region "Fetch Barcode Manager Pk"
        public int FetchBarCodeManagerPk(string Configid)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;

            WorkFlow objWF = new WorkFlow();
            //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"

            StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt"  + "where bdmt.config_id_fk= '" + Configid + "'";
            DsBarManager = objWF.GetDataSet(StrSql);
            if (DsBarManager.Tables[0].Rows.Count > 0)
            {
                var _with12 = DsBarManager.Tables[0].Rows[0];
                strReturn = Convert.ToInt32(_with12["bcd_mst_pk"]);
            }
            return strReturn;
        }
        #endregion

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

                strQuery.Append("select bdmt.bcd_mst_pk, bdmt.field_name, bdmt.default_value" );
                strQuery.Append("  from barcode_data_mst_tbl bdmt" );
                //, barcode_doc_data_tbl bddt
                //strQuery.Append(" where bdmt.bcd_mst_pk = bddt.bcd_mst_fk and" & vbCrLf)
                strQuery.Append("   where bdmt.BCD_MST_FK= " + BarCodeManagerPk);
                strQuery.Append(" and bdmt.default_value = 1 ORDER BY default_value desc" );

                // StrSql = "select bdmt.bcd_mst_pk, bdmt.field_name ,bdmt.default_value from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt where bdmt.bcd_mst_pk=bddt.bcd_mst_fk and bdmt.BCD_MST_FK=" & BarCodeManagerPk
                DsBarManager = objWF.GetDataSet(strQuery.ToString());
                return DsBarManager;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Booking Nr"
        public string FetchBookingNr(string hblRefnr)
        {
            try
            {
                string strQuery = null;
                DataSet DsBarManager = null;
                string strReturn = null;

                WorkFlow objWF = new WorkFlow();
                //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"
                strQuery = "select bok.booking_ref_no from hbl_exp_tbl hbl, JOB_CARD_TRN job,BOOKING_MST_TBL bok"  + "where hbl.job_card_sea_exp_fk = job.JOB_CARD_TRN_PK"  + "and job.BOOKING_MST_FK = bok.BOOKING_MST_PK"  + "and hbl.hbl_ref_no ='" + hblRefnr + "'";


                DsBarManager = objWF.GetDataSet(strQuery);
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with13 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToString(_with13["booking_ref_no"]);
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
        #endregion

        #region "HBL VALIDATION"
        public DataTable FetchCollInfo(string JOBPk, string locpk)
        {

            StringBuilder strSqlBuilder = new StringBuilder();

            strSqlBuilder.Append("         SELECT QRY.* ");
            strSqlBuilder.Append("         FROM (SELECT T.* ");
            strSqlBuilder.Append("              FROM (SELECT INV.CONSOL_INVOICE_PK PK, ");
            strSqlBuilder.Append("                           INV.INVOICE_REF_NO, ");
            strSqlBuilder.Append("                           CMT.CUSTOMER_NAME, ");
            strSqlBuilder.Append("                           INV.INVOICE_DATE, ");
            strSqlBuilder.Append("                           INV.NET_RECEIVABLE, ");
            strSqlBuilder.Append("                           NVL((select sum(ctrn.recd_amount_hdr_curr) ");
            strSqlBuilder.Append("                                 from collections_trn_tbl ctrn ");
            strSqlBuilder.Append("                        where ctrn.invoice_ref_nr like inv.invoice_ref_no), ");
            strSqlBuilder.Append("                               0) Recieved, ");
            strSqlBuilder.Append("                           NVL((INV.NET_RECEIVABLE - ");
            strSqlBuilder.Append("                               NVL((select sum(ctrn.recd_amount_hdr_curr) ");
            strSqlBuilder.Append("                                      from collections_trn_tbl ctrn ");
            strSqlBuilder.Append("                                     where ctrn.invoice_ref_nr like ");
            strSqlBuilder.Append("                                           inv.invoice_ref_no), ");
            strSqlBuilder.Append("                                    0.00)), ");
            strSqlBuilder.Append("                               0) Balance, ");
            strSqlBuilder.Append("                           CUMT.CURRENCY_ID, ");
            strSqlBuilder.Append("                           INV.INV_UNIQUE_REF_NR ");
            strSqlBuilder.Append("                      FROM CONSOL_INVOICE_TBL     INV, ");
            strSqlBuilder.Append("                           CONSOL_INVOICE_TRN_TBL INVTRN, ");
            strSqlBuilder.Append("                           JOB_CARD_TRN   JOB, ");
            strSqlBuilder.Append("                           HBL_EXP_TBL           HBL, ");
            strSqlBuilder.Append("                           CUSTOMER_MST_TBL       CMT, ");
            strSqlBuilder.Append("                           CURRENCY_TYPE_MST_TBL  CUMT, ");
            strSqlBuilder.Append("                           USER_MST_TBL           UMT ");
            strSqlBuilder.Append("                     WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
            strSqlBuilder.Append("                       AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)  ");
            strSqlBuilder.Append("                       AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) ");
            strSqlBuilder.Append("                       AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+) ");
            strSqlBuilder.Append("                       AND UMT.DEFAULT_LOCATION_FK = 1 ");
            strSqlBuilder.Append("                       AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");
            strSqlBuilder.Append("                       AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+) ");
            strSqlBuilder.Append("                       AND INV.PROCESS_TYPE = '1' ");
            strSqlBuilder.Append("                       AND INV.BUSINESS_TYPE = '2' ");
            strSqlBuilder.Append("                       AND JOB.JOB_CARD_TRN_PK = " + JOBPk + " ");
            strSqlBuilder.Append("                     GROUP BY INV.CONSOL_INVOICE_PK, ");
            strSqlBuilder.Append("                              INV.INVOICE_REF_NO, ");
            strSqlBuilder.Append("                              INV.INVOICE_DATE, ");
            strSqlBuilder.Append("                              CUMT.CURRENCY_ID, ");
            strSqlBuilder.Append("                              CMT.CUSTOMER_NAME, ");
            strSqlBuilder.Append("                              INV.NET_RECEIVABLE, ");
            strSqlBuilder.Append("                              INV.CREATED_DT, ");
            strSqlBuilder.Append("                              INV.INV_UNIQUE_REF_NR ");
            strSqlBuilder.Append("                     ORDER BY INV.CREATED_DT DESC) T) QRY ");
            try
            {
                return (objWF.GetDataTable(strSqlBuilder.ToString()));
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
        public DataTable FetchInvoiceInfo(string JOBPk, string locpk)
        {

            StringBuilder strSqlBuilder = new StringBuilder();

            strSqlBuilder.Append("        select distinct jcf.freight_element_mst_fk, ");
            strSqlBuilder.Append("                     ft.freight_element_id, ");
            strSqlBuilder.Append("                     jc.jobcard_ref_no ");
            strSqlBuilder.Append("        from JOB_CARD_TRN    jc, ");
            strSqlBuilder.Append("            JOB_TRN_FD      jcf, ");
            strSqlBuilder.Append("            freight_element_mst_tbl ft ");
            strSqlBuilder.Append("        where jc.JOB_CARD_TRN_PK = jcf.JOB_CARD_TRN_FK ");
            strSqlBuilder.Append("        and ft.freight_element_mst_pk = jcf.freight_element_mst_fk ");
            strSqlBuilder.Append("         and jcf.freight_type = 1 ");
            strSqlBuilder.Append("        and jcf.freight_element_mst_fk not in ");
            strSqlBuilder.Append("            (select jfrt.freight_element_mst_fk ");
            strSqlBuilder.Append("               from JOB_CARD_TRN    jcimp, ");
            strSqlBuilder.Append("                    JOB_TRN_FD      jfrt, ");
            strSqlBuilder.Append("                    consol_invoice_tbl      cinv, ");
            strSqlBuilder.Append("                    consol_invoice_trn_tbl  cintrn, ");
            strSqlBuilder.Append("                    freight_element_mst_tbl frt ");
            strSqlBuilder.Append("              where jcimp.JOB_CARD_TRN_PK = jfrt.JOB_CARD_TRN_FK ");
            strSqlBuilder.Append("                and cinv.consol_invoice_pk = cintrn.consol_invoice_fk ");
            strSqlBuilder.Append("                and cintrn.job_card_fk = jcimp.JOB_CARD_TRN_PK ");
            strSqlBuilder.Append("                and cintrn.frt_oth_element_fk = jfrt.freight_element_mst_fk ");
            strSqlBuilder.Append("                and cinv.process_type = 1 ");
            strSqlBuilder.Append("                and cinv.business_type = 2 ");
            strSqlBuilder.Append("                and frt.freight_element_mst_pk = jfrt.freight_element_mst_fk ");
            strSqlBuilder.Append("                and jcimp.JOB_CARD_TRN_PK = jc.JOB_CARD_TRN_PK) ");
            strSqlBuilder.Append("        and jc.JOB_CARD_TRN_PK = " + JOBPk + " ");
            strSqlBuilder.Append("        union ");
            strSqlBuilder.Append("        select distinct jcf1.freight_element_mst_fk, ");
            strSqlBuilder.Append("                     ft.freight_element_id, ");
            strSqlBuilder.Append("                     jc.jobcard_ref_no ");
            strSqlBuilder.Append("        from JOB_CARD_TRN    jc, ");
            strSqlBuilder.Append("            JOB_TRN_OTH_CHRG  jcf1, ");
            strSqlBuilder.Append("            freight_element_mst_tbl ft ");
            strSqlBuilder.Append("        where jc.JOB_CARD_TRN_PK = jcf1.JOB_CARD_TRN_FK ");
            strSqlBuilder.Append("        and ft.freight_element_mst_pk = jcf1.freight_element_mst_fk ");
            strSqlBuilder.Append("         and jcf1.freight_type = 1 ");
            strSqlBuilder.Append("         and jcf1.FREIGHT_ELEMENT_MST_FK not in ");
            strSqlBuilder.Append("            (select jfrt1.freight_element_mst_fk ");
            strSqlBuilder.Append("               from JOB_CARD_TRN    jcimp, ");
            strSqlBuilder.Append("                    JOB_TRN_OTH_CHRG   jfrt1, ");
            strSqlBuilder.Append("                    consol_invoice_tbl      cinv, ");
            strSqlBuilder.Append("                    consol_invoice_trn_tbl  cintrn, ");
            strSqlBuilder.Append("                    freight_element_mst_tbl frt ");
            strSqlBuilder.Append("              where jcimp.JOB_CARD_TRN_PK = jfrt1.JOB_CARD_TRN_FK ");
            strSqlBuilder.Append("                and cinv.consol_invoice_pk = cintrn.consol_invoice_fk ");
            strSqlBuilder.Append("                and cintrn.job_card_fk = jcimp.JOB_CARD_TRN_PK ");
            strSqlBuilder.Append("                and cintrn.frt_oth_element_fk = jfrt1.FREIGHT_ELEMENT_MST_FK ");
            strSqlBuilder.Append("                and cinv.process_type = 1 ");
            strSqlBuilder.Append("                and cinv.business_type = 2 ");
            strSqlBuilder.Append("                and frt.freight_element_mst_pk = jfrt1.freight_element_mst_fk ");
            strSqlBuilder.Append("                and jcimp.JOB_CARD_TRN_PK = jc.JOB_CARD_TRN_PK) ");
            strSqlBuilder.Append("        and jc.JOB_CARD_TRN_PK = " + JOBPk + " ");
            try
            {
                return (objWF.GetDataTable(strSqlBuilder.ToString()));
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

        #region "Fetch Booking Status"

        public Int32 fetchBkgStatus(long PKVal)
        {
            try
            {
                System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
                string Status = null;
                WorkFlow objwf = new WorkFlow();
                //strSQL = " "
                strSQL.Append(" select bkg.status from BOOKING_MST_TBL bkg, ");
                strSQL.Append("  JOB_CARD_TRN jc ");
                strSQL.Append("  where bkg.BOOKING_MST_PK = jc.BOOKING_MST_FK ");
                strSQL.Append("  and jc.JOB_CARD_TRN_PK=" + PKVal);

                Status = objwf.ExecuteScaler(strSQL.ToString());
                if (!string.IsNullOrEmpty(Status))
                {
                    return Convert.ToInt32(Status);
                }
                else
                {
                    return 0;
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
        #endregion

        #region "Credit Control"
        public DataSet FetchCreditDetails(string JOBPk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            WorkFlow objwf = new WorkFlow();

            strSQL.Append("SELECT JOB.JOB_CARD_TRN_PK,");
            strSQL.Append(" JOB.SHIPPER_CUST_MST_FK,");
            strSQL.Append("CMT.SEA_CREDIT_LIMIT,");
            strSQL.Append("JOB.JOBCARD_REF_NO,");
            strSQL.Append("SUM(NVL(JFD.EXCHANGE_RATE * JFD.FREIGHT_AMT, 0)) AS TOTAL ");

            strSQL.Append(" FROM JOB_TRN_FD   JFD,");
            strSQL.Append(" JOB_CARD_TRN JOB,");
            strSQL.Append(" CUSTOMER_MST_TBL     CMT");
            strSQL.Append(" WHERE JOB.JOB_CARD_TRN_PK = JFD.JOB_CARD_TRN_FK");
            strSQL.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
            strSQL.Append("  AND CMT.CREDIT_CUSTOMER =1 ");
            if (!string.IsNullOrEmpty(JOBPk))
            {
                strSQL.Append(" AND JOB.JOB_CARD_TRN_PK = " + JOBPk);
            }
            strSQL.Append(" GROUP BY JOB_CARD_TRN_PK,");
            strSQL.Append(" SHIPPER_CUST_MST_FK,");
            strSQL.Append(" SEA_CREDIT_LIMIT,");
            strSQL.Append(" JOB.JOBCARD_REF_NO");

            try
            {
                return (objwf.GetDataSet(strSQL.ToString()));
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

        #region "Fetching CreditPolicy Details based on Shipper"
        public object FetchCreditPolicy(string ShipperPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT CMT.CREDIT_LIMIT,");
            strSQL.Append(" cmt.customer_name,");
            strSQL.Append(" CMT.CREDIT_DAYS,");
            strSQL.Append(" CMT.SEA_APP_BOOKING,");
            strSQL.Append(" CMT.SEA_APP_BL_RELEASE,");
            strSQL.Append(" CMT.SEA_APP_RELEASE_ODR");
            strSQL.Append(" FROM CUSTOMER_MST_TBL CMT");
            strSQL.Append(" WHERE CMT.CUSTOMER_MST_PK = " + ShipperPK);

            try
            {
                return (objWF.GetDataSet(strSQL.ToString()));
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

        #region "Get Import Job Card Pks "
        public string GetImportJCPKs(string HBLNr)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT ROWTOCOL('");
                strSQL.Append(" SELECT 0 AS JCPK FROM DUAL");
                strSQL.Append(" UNION");
                strSQL.Append(" SELECT JCSIT.JOB_CARD_SEA_IMP_PK AS JCPK FROM JOB_CARD_SEA_IMP_TBL JCSIT");
                strSQL.Append(" WHERE JCSIT.HBL_REF_NO=''" + HBLNr + "'' AND JCSIT.JC_AUTO_MANUAL=1')");
                strSQL.Append(" FROM DUAL");

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
        #endregion
    }
}