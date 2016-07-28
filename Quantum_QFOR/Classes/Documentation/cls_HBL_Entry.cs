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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />

    #region "Class Variables"

    public class cls_HBL_Entry : CommonFeatures
    {
        /// <summary>
        /// The object wf
        /// </summary>
        private WorkFlow objWF = new WorkFlow();

        /// <summary>
        /// The object track n trace
        /// </summary>
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        /// <summary>
        /// The m_ ship data set
        /// </summary>
        private static DataSet M_ShipDataSet = new DataSet();

        /// <summary>
        /// The m_ freight terms dataset
        /// </summary>
        private static DataSet M_FreightTermsDataset = new DataSet();

        #endregion "Class Variables"

        /// <summary>
        /// The m_ move code dataset
        /// </summary>
        private static DataSet M_MoveCodeDataset = new DataSet();

        #region "Properties"

        /// <summary>
        /// Gets the ship data set.
        /// </summary>
        /// <value>
        /// The ship data set.
        /// </value>
        public static DataSet ShipDataSet
        {
            get { return M_ShipDataSet; }
        }

        /// <summary>
        /// Gets the freight data set.
        /// </summary>
        /// <value>
        /// The freight data set.
        /// </value>
        public static DataSet FreightDataSet
        {
            get { return M_FreightTermsDataset; }
        }

        /// <summary>
        /// Gets the move code data set.
        /// </summary>
        /// <value>
        /// The move code data set.
        /// </value>
        public static DataSet MoveCodeDataSet
        {
            get { return M_MoveCodeDataset; }
        }

        #endregion "Properties"

        #region "Constructor"

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_HBL_Entry"/> class.
        /// </summary>
        public cls_HBL_Entry()
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
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Constructor"

        #region "print"

        //adding by thiyagarajan on 25/5/09
        /// <summary>
        /// Getfrightdtlses the specified jobrefno.
        /// </summary>
        /// <param name="jobrefno">The jobrefno.</param>
        /// <returns></returns>
        public DataSet getfrightdtls(string jobrefno)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = "  SELECT CASE WHEN FD.SURCHARGE IS NULL THEN FRIGHT.FREIGHT_ELEMENT_NAME ELSE (FRIGHT.FREIGHT_ELEMENT_NAME || ' ( ' || FD.SURCHARGE || '  ) ' || '') END FREIGHT_ELEMENT_NAME, ";
                str += " (case when fd.freight_type=1 then fd.freight_amt else 0.00 end)prepaid, (case when fd.freight_type=2 then ";
                str += " fd.freight_amt else 0.00 end)collect,curr.currency_id,1 sortcol,fright.preference ";
                str += " from JOB_CARD_TRN j,JOB_TRN_FD fd,freight_element_mst_tbl fright,currency_type_mst_tbl curr ";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=fd.JOB_CARD_TRN_FK and fd.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=fd.currency_mst_fk and fd.print_on_mbl=1";
                str += "  UNION ALL";

                str += " select fright.freight_element_name, (case when oth.freight_type=1 then ";
                str += " oth.amount else 0.00 end)prepaid,(case when oth.freight_type=2 then ";
                str += " oth.amount else 0.00 end)collect,curr.currency_id,2 sortcol,fright.preference ";
                str += " from JOB_CARD_TRN j,JOB_TRN_OTH_CHRG OTH,freight_element_mst_tbl fright,currency_type_mst_tbl curr";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=oth.JOB_CARD_TRN_FK and oth.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=oth.currency_mst_fk and oth.print_on_mbl=1 order by sortcol ,preference";

                return objWF.GetDataSet(str);
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

        /// <summary>
        /// Getamts the specified jobrefno.
        /// </summary>
        /// <param name="jobrefno">The jobrefno.</param>
        /// <returns></returns>
        public DataSet getamt(string jobrefno)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = " select sum(preamt)amt ,1 sortcol from ( ";
                str += " select sum (fd.freight_amt*fd.exchange_rate)preamt ";
                str += " from JOB_CARD_TRN j,JOB_TRN_FD fd,freight_element_mst_tbl fright,currency_type_mst_tbl curr ";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=fd.JOB_CARD_TRN_FK and fd.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=fd.currency_mst_fk and fd.print_on_mbl=1 and fd.freight_type=1 ";

                str += "  UNION ";

                str += " select sum(oth.amount*oth.exchange_rate)preamt ";
                str += " from JOB_CARD_TRN j,JOB_TRN_OTH_CHRG OTH,freight_element_mst_tbl fright,currency_type_mst_tbl curr";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=oth.JOB_CARD_TRN_FK and oth.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=oth.currency_mst_fk and oth.print_on_mbl=1 and oth.freight_type=1 )main1 ";

                str += " UNION select sum(collamt) amt, 2 sortcol from ( select sum ( fd.freight_amt*fd.exchange_rate)collamt ";
                str += " from JOB_CARD_TRN j,JOB_TRN_FD fd,freight_element_mst_tbl fright,currency_type_mst_tbl curr ";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=fd.JOB_CARD_TRN_FK and fd.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=fd.currency_mst_fk and fd.print_on_mbl=1 and fd.freight_type=2 ";

                str += "  UNION ";

                str += " select sum(oth.amount*oth.exchange_rate)collamt ";
                str += " from JOB_CARD_TRN j,JOB_TRN_OTH_CHRG OTH,freight_element_mst_tbl fright,currency_type_mst_tbl curr";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=oth.JOB_CARD_TRN_FK and oth.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=oth.currency_mst_fk and oth.print_on_mbl=1 and oth.freight_type=2 )main2 order by sortcol asc ";
                return objWF.GetDataSet(str);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetchjobs the specified pk.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public string fetchjob(string pk)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = "select j.jobcard_ref_no  from JOB_CARD_TRN j where j.JOB_CARD_TRN_PK= " + pk;
                return objWF.ExecuteScaler(str);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Getfrtdtlses the specified jobrefno.
        /// </summary>
        /// <param name="jobrefno">The jobrefno.</param>
        /// <returns></returns>
        public DataSet getfrtdtls(string jobrefno)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = "  select FREIGHT_ELEMENT_NAME,currency_id,preference,prepaid,collect,ctrtype from( ";
                str += " SELECT CASE WHEN FD.SURCHARGE IS NULL THEN FRIGHT.FREIGHT_ELEMENT_NAME ELSE (FRIGHT.FREIGHT_ELEMENT_NAME || ' ( ' || FD.SURCHARGE || '  ) ' || '') END FREIGHT_ELEMENT_NAME,";
                str += " curr.currency_id,fright.preference ,(case when fd.freight_type=1 then fd.freight_amt else 0.00 end)prepaid, (case when fd.freight_type=2 then ";
                str += " fd.freight_amt else 0.00 end)collect,ctmt.container_type_mst_id ctrtype,ctmt.preferences ";
                str += "  from JOB_CARD_TRN j,JOB_TRN_FD fd,freight_element_mst_tbl fright,currency_type_mst_tbl curr,container_type_mst_tbl ctmt";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=fd.JOB_CARD_TRN_FK and fd.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and fd.container_type_mst_fk=ctmt.container_type_mst_pk(+)";
                str += " and curr.currency_mst_pk=fd.currency_mst_fk and fd.print_on_mbl=1";
                str += "  UNION ALL";

                str += " select fright.freight_element_name, curr.currency_id,fright.preference,(case when oth.freight_type=1 then ";
                str += " oth.amount else 0.00 end)prepaid,(case when oth.freight_type=2 then  ";
                str += " oth.amount else 0.00 end)collect,null ctrtype,999 preferences";
                str += " from JOB_CARD_TRN j,JOB_TRN_OTH_CHRG OTH,freight_element_mst_tbl fright,currency_type_mst_tbl curr";
                str += " where j.jobcard_ref_no like '" + jobrefno + "'";
                str += " and j.JOB_CARD_TRN_PK=oth.JOB_CARD_TRN_FK and oth.freight_element_mst_fk=fright.freight_element_mst_pk ";
                str += " and curr.currency_mst_pk=oth.currency_mst_fk and oth.print_on_mbl=1 order by preferences ,preference)";

                return objWF.GetDataSet(str);
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

        #endregion "print"

        #region " Enhance Search Functions"

        /// <summary>
        /// Fetches for job reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
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
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion " Enhance Search Functions"

        #region " Enhance Search & Lookup Search Block FOR HBL"

        //Pls do the impact analysis before changing as this function
        //as might be accesed by other forms also.
        //Using in Transporter Note Report
        /// <summary>
        /// Fetches for HBL reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
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
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search & Lookup Search Block FOR HBL"

        #region " Enhance Search & Lookup Search Block FOR HBL"

        //Pls do the impact analysis before changing as this function
        //as might be accesed by other forms also.
        //Using in Transporter Note Report
        /// <summary>
        /// Fetches for HBL reference mawb.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchForHblRefMAWB(string strCond, string loc = "")
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
                selectCommand.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_PKG.GET_HBL_REF_MAWB";
                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBUSINESS_MODEL_IN).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_IN", (!string.IsNullOrEmpty(loc) ? loc : "")).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search & Lookup Search Block FOR HBL"

        #region " Fetch sOperator Details After Job Card Is Selected"

        /// <summary>
        /// Fetches the operator details.
        /// </summary>
        /// <param name="JOBPk">The job pk.</param>
        /// <param name="Booking_Ref_NR">The booking_ ref_ nr.</param>
        /// <param name="HBl_Esi_status">The h BL_ esi_status.</param>
        /// <returns></returns>
        public DataSet FetchOperatorDetails(string JOBPk = "0", string Booking_Ref_NR = "0", string HBl_Esi_status = "false")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            if (HBl_Esi_status == "true")
            {
                sb.Append("   SELECT JC.JOB_CARD_TRN_PK,");
                sb.Append("       BOOK.CARGO_TYPE,");
                sb.Append("       OPR.OPERATOR_ID,");
                sb.Append("       OPR.OPERATOR_NAME,");
                sb.Append("       OPR.OPERATOR_MST_PK,");
                sb.Append("       PO2.PORT_ID AS POL, ");
                sb.Append("       PO2.PORT_MST_PK AS \"POLPK\", ");
                sb.Append("       PO1.PORT_MST_PK AS \"PODPK\",");
                sb.Append("       PO1.PORT_ID AS POD,");
                sb.Append("       CASE WHEN PLD.PLACE_NAME IS NOT NULL THEN  PLD.PLACE_NAME ELSE PO1.PORT_ID END RPLACE,");
                sb.Append("       CASE WHEN PLR.PLACE_NAME IS NOT NULL THEN  PLR.PLACE_NAME ELSE PO2.PORT_ID END DELPLACE,");
                sb.Append("            ");
                sb.Append("       CUST.CUSTOMER_NAME AS CUSTOMER,");
                sb.Append("       FIRSTVOY.VOYAGE_TRN_PK \"FIRSTVOYPK\",");
                sb.Append("          FIRSTVOY.VOYAGE,");
                sb.Append("       FIRSTVSL.VESSEL_NAME,");
                sb.Append("       FIRSTVSL.VESSEL_ID,");
                sb.Append("        FIRSTVOY.POD_ETA,");
                sb.Append("                FIRSTVOY.POL_ETD,");
                sb.Append(" ");
                sb.Append("        JC.ARRIVAL_DATE,");
                sb.Append("                JC.DEPARTURE_DATE,");
                sb.Append("                JC.SEC_ETA_DATE,");
                sb.Append("                JC.SEC_ETD_DATE,");
                sb.Append("                ");
                sb.Append("       ");
                sb.Append("       JC.JOBCARD_REF_NO,");
                sb.Append("      ");
                sb.Append("      ( SELECT MY_CONCADINATE_FUN(ESI.ESI_HDR_PK,1) FROM DUAL ) AS GOODS_DESCRIPTION,");
                sb.Append(" ( SELECT MY_CONCADINATE_FUN(ESI.ESI_HDR_PK,1) FROM DUAL ) AS MARKS_NUMBERS,");
                sb.Append("       SH.INCO_CODE,");
                sb.Append("       MOV.CARGO_MOVE_CODE,");
                sb.Append("       ESI.PYMT_TYPE,");
                sb.Append("       SECVOY.VOYAGE_TRN_PK      \"SECVOYPK\",");
                sb.Append("                SECVSL.VESSEL_NAME        \"SEC_VESSEL_NAME\",");
                sb.Append("                SECVSL.VESSEL_ID          \"SEC_VESSEL_ID\",             ");
                sb.Append("                SECVOY.VOYAGE             \"SEC_VOYAGE\",");
                sb.Append("        CL.AGENT_MST_PK           AS CLAGENTPK,");
                sb.Append("                CL.AGENT_NAME             AS CLAGENT,");
                sb.Append("          CB.AGENT_MST_PK           AS CBAGENTPK,");
                sb.Append("                CB.AGENT_NAME             AS CBAGENT,");
                sb.Append("       ");
                sb.Append("       DP.AGENT_MST_PK           AS DPAGENTPK,");
                sb.Append("        DP.AGENT_NAME             AS DPAGENT,");
                sb.Append("       CASE WHEN  ESI.PYMT_TYPE=2THEN");
                sb.Append("         ESI.CONSIGN_MST_FK");
                sb.Append("         ELSE");
                sb.Append("           CONSIGNEE.CUSTOMER_MST_PK");
                sb.Append("         END CONSIGNEEPK,");
                sb.Append("            CASE WHEN  ESI.PYMT_TYPE=2THEN");
                sb.Append("        ");
                sb.Append("      (SELECT  NVL(CONSIGNEE1.CUSTOMER_NAME,'')");
                sb.Append("          FROM CUSTOMER_MST_TBL CONSIGNEE1, SYN_EBKG_ESI_HDR_TBL ESI1");
                sb.Append("         WHERE CONSIGNEE1.CUSTOMER_MST_PK = ESI1.CONSIGN_MST_FK)");
                sb.Append("         ELSE");
                sb.Append("            CONSIGNEE.CUSTOMER_NAME ");
                sb.Append("         END CONSIGNEE,");
                sb.Append("          CASE WHEN  ESI.PYMT_TYPE=2THEN");
                sb.Append("        ");
                sb.Append("      (SELECT  NVL(ESI2.CONSIGN_ADRESS,'')");
                sb.Append("          FROM CUSTOMER_MST_TBL CONSIGNEE2, SYN_EBKG_ESI_HDR_TBL ESI2");
                sb.Append("         WHERE CONSIGNEE2.CUSTOMER_MST_PK = ESI.CONSIGN_MST_FK)");
                sb.Append("         ELSE");
                sb.Append("          ''");
                sb.Append("         END CONSIGNEEAdress,");
                sb.Append("   ");
                sb.Append("     ");
                sb.Append("       SHIPPER.CUSTOMER_MST_PK AS SHIPPERPK,");
                sb.Append("       SHIPPER.CUSTOMER_NAME AS SHIPPER,");
                sb.Append("       NOTIFY1.CUSTOMER_MST_PK AS NOTIFY1PK,");

                sb.Append("  CASE WHEN JOB.SAC_N1=0  THEN notify1.customer_name ");
                sb.Append(" Else  'SAME AS CONSIGNEE' END  NOTIFY1, ");

                // sb.Append("       NOTIFY1.CUSTOMER_NAME AS NOTIFY1,")
                sb.Append("       ESI.NOTIFYPARTY1_ASRESS,");
                sb.Append("       NOTIFY2.CUSTOMER_MST_PK AS NOTIFY2PK,");
                //  sb.Append("       NOTIFY2.CUSTOMER_NAME AS NOTIFY2,")
                sb.Append("    CASE WHEN job.SAC_N2=0  THEN notify2.customer_name ");
                sb.Append("    Else  'SAME AS CONSIGNEE' END  NOTIFY2,");
                sb.Append("       ESI.NOTIFYPARTY2_ASRESS,");
                sb.Append("       JC.LC_SHIPMENT, (SELECT LMT.LOCATION_NAME FROM LOCATION_MST_TBL LMT WHERE LMT.LOCATION_MST_PK = " + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + ") PLACE_ISSUE, ");
                sb.Append("  PO1.PORT_NAME || ',' || POLCONT.COUNTRY_NAME POLCONTY,");
                sb.Append("                PO2.PORT_NAME || ',' || PODCONT.COUNTRY_NAME PODCONTY,");
                sb.Append("                CASE");
                sb.Append("                  WHEN PLR.PLACE_NAME IS NOT NULL THEN");
                sb.Append("                   PLR.PLACE_NAME || ',' || PLRCONT.COUNTRY_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   ''");
                sb.Append("                END PLRCONTY,");
                sb.Append("                CASE");
                sb.Append("                  WHEN PLD.PLACE_NAME IS NOT NULL THEN");
                sb.Append("                   PLD.PLACE_NAME || ',' || PFDCONT.COUNTRY_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   ''");
                sb.Append("                END PLDCONTY ");
                sb.Append("    FROM BOOKING_MST_TBL        BOOK,");
                sb.Append("       JOB_CARD_TRN   JC,");
                sb.Append("       SYN_EBKG_ESI_HDR_TBL   ESI,");
                sb.Append("       OPERATOR_MST_TBL       OPR,");
                sb.Append("       PORT_MST_TBL           PO2,");
                sb.Append("       PORT_MST_TBL           PO1,");
                sb.Append("       PLACE_MST_TBL          PLR,");
                sb.Append("       PLACE_MST_TBL          PLD,");
                sb.Append("       CUSTOMER_MST_TBL       CUST,");
                sb.Append("       VESSEL_VOYAGE_TRN      FIRSTVOY,");
                sb.Append("       VESSEL_VOYAGE_TBL      FIRSTVSL,");
                sb.Append("       SHIPPING_TERMS_MST_TBL SH,");
                sb.Append("       CARGO_MOVE_MST_TBL     MOV,");
                sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
                sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
                sb.Append("       CUSTOMER_MST_TBL       NOTIFY1,");
                sb.Append("       CUSTOMER_MST_TBL       NOTIFY2,");
                sb.Append("       AGENT_MST_TBL          DP,");
                sb.Append("         AGENT_MST_TBL          CB,");
                sb.Append("         AGENT_MST_TBL          CL,");
                sb.Append("          VESSEL_VOYAGE_TRN      SECVOY,");
                sb.Append("            VESSEL_VOYAGE_TBL      SECVSL,");
                sb.Append("       COUNTRY_MST_TBL        PLRCONT,");
                sb.Append("       COUNTRY_MST_TBL        POLCONT,");
                sb.Append("       COUNTRY_MST_TBL        PODCONT,");
                sb.Append("       COUNTRY_MST_TBL        PFDCONT,");
                sb.Append("       LOCATION_MST_TBL       LMTPLR,");
                sb.Append("       LOCATION_MST_TBL       LMTPFD");
                sb.Append("   WHERE OPR.OPERATOR_MST_PK(+) = BOOK.CARRIER_MST_FK");
                sb.Append("   AND ESI.BOOKING_SEA_FK = JC.BOOKING_MST_FK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = PO2.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK(+) = PO1.PORT_MST_PK");
                sb.Append("   AND ESI.Por_Fk = PLR.PLACE_PK(+)");
                sb.Append("   AND ESI.Pfd_Fk = PLD.PLACE_PK(+)");
                sb.Append("   AND CUST.CUSTOMER_MST_PK(+) = BOOK.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND FIRSTVOY.VESSEL_VOYAGE_TBL_FK = FIRSTVSL.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JC.VOYAGE_TRN_FK = FIRSTVOY.VOYAGE_TRN_PK(+)");
                sb.Append("   AND SH.SHIPPING_TERMS_MST_PK(+) = ESI.PYMT_TERMS");
                sb.Append("   AND MOV.CARGO_MOVE_PK(+) = ESI.CARGO_MOVE_FK");
                sb.Append("   AND  CONSIGNEE.CUSTOMER_MST_PK(+) = JC.CONSIGNEE_CUST_MST_FK");
                sb.Append("   AND SHIPPER.CUSTOMER_MST_PK(+) = JC.SHIPPER_CUST_MST_FK");
                sb.Append("   AND NOTIFY1.CUSTOMER_MST_PK(+) = ESI.NOTIFYPARTY_FK1");
                sb.Append("   AND NOTIFY2.CUSTOMER_MST_PK(+) = ESI.NOTIFYPARTY_FK2");
                sb.Append("   AND DP.AGENT_MST_PK(+) = JC.DP_AGENT_MST_FK");
                sb.Append("     AND CB.AGENT_MST_PK(+) = JC.CB_AGENT_MST_FK");
                sb.Append("   AND  CL.AGENT_MST_PK(+) = JC.CL_AGENT_MST_FK");
                sb.Append("   AND JC.SEC_VOYAGE_TRN_FK = SECVOY.VOYAGE_TRN_PK(+)");
                sb.Append("   AND SECVOY.VESSEL_VOYAGE_TBL_FK = SECVSL.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND PO1.COUNTRY_MST_FK = POLCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PO2.COUNTRY_MST_FK = PODCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PLR.LOCATION_MST_FK = LMTPLR.LOCATION_MST_PK(+)");
                sb.Append("   AND LMTPLR.COUNTRY_MST_FK = PLRCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PLD.LOCATION_MST_FK = LMTPFD.LOCATION_MST_PK(+)");
                sb.Append("   AND LMTPFD.COUNTRY_MST_FK = PFDCONT.COUNTRY_MST_PK(+)");
                sb.Append("    AND shipper.REP_EMP_MST_FK=SHP_SE.EMPLOYEE_MST_PK(+) ");
                sb.Append("   AND JOB_EXP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
                sb.Append(" AND job_exp.consignee_cust_mst_fk = TEMP_CONS.CUSTOMER_MST_PK(+)  ");

                if (Booking_Ref_NR != "0")
                {
                    sb.Append("   AND BOOK.BOOKING_REF_NO ='" + Booking_Ref_NR + "'");
                }

                if (JOBPk != "0")
                {
                    sb.Append("   AND JC.JOB_CARD_TRN_PK=" + JOBPk);
                }

                sb.Append("");
            }
            else
            {
                sb.Append("     SELECT DISTINCT  ");
                sb.Append("                        BOOK.CARGO_TYPE,");
                sb.Append("       OPR.OPERATOR_ID,");
                sb.Append("       OPR.OPERATOR_NAME,");
                sb.Append("       OPR.OPERATOR_MST_PK,");
                sb.Append("       PO2.PORT_ID               AS POL,");
                sb.Append("       PO2.PORT_MST_PK           AS \"POLPK\",");
                sb.Append("       PO1.PORT_MST_PK           AS \"PODPK\",");
                sb.Append("       PO1.PORT_ID               AS POD,");
                sb.Append("       CASE WHEN BOOK.CARGO_TYPE = 1 AND BOOK.BUSINESS_TYPE=2 THEN POO.PORT_ID ELSE PLD.PLACE_NAME END RPLACE,");
                sb.Append("       CASE WHEN BOOK.CARGO_TYPE = 1 AND BOOK.BUSINESS_TYPE=2 THEN PFD.PORT_ID ELSE PLR.PLACE_NAME END DELPLACE,");
                sb.Append("       CUST.CUSTOMER_NAME        AS CUSTOMER,");
                sb.Append("       FIRSTVOY.VOYAGE_TRN_PK    \"FIRSTVOYPK\",");
                sb.Append("       FIRSTVSL.VESSEL_NAME,");
                sb.Append("       FIRSTVSL.VESSEL_ID,");
                sb.Append("       FIRSTVOY.POD_ETA,");
                sb.Append("       FIRSTVOY.POL_ETD,");
                sb.Append("       JOB.ARRIVAL_DATE,");
                sb.Append("       JOB.DEPARTURE_DATE,");
                sb.Append("       JOB.SEC_ETA_DATE,");
                sb.Append("       JOB.SEC_ETD_DATE,");
                sb.Append("       JOB.JOBCARD_REF_NO,");
                sb.Append("       JOB.GOODS_DESCRIPTION,");
                sb.Append("       JOB.MARKS_NUMBERS,");
                sb.Append("       SH.INCO_CODE,");
                sb.Append("       MOV.CARGO_MOVE_CODE,");
                sb.Append("       JOB.PYMT_TYPE,");
                sb.Append("       SECVOY.VOYAGE_TRN_PK      \"SECVOYPK\",");
                sb.Append("       SECVSL.VESSEL_NAME        \"SEC_VESSEL_NAME\",");
                sb.Append("       SECVSL.VESSEL_ID          \"SEC_VESSEL_ID\",");
                sb.Append("       FIRSTVOY.VOYAGE,");
                sb.Append("       SECVOY.VOYAGE             \"SEC_VOYAGE\",");
                sb.Append("       CL.AGENT_MST_PK           AS CLAGENTPK,");
                sb.Append("       CL.AGENT_NAME             AS CLAGENT,");
                sb.Append("       CB.AGENT_MST_PK           AS CBAGENTPK,");
                sb.Append("       CB.AGENT_NAME             AS CBAGENT,");
                sb.Append("       DP.AGENT_MST_PK           AS DPAGENTPK,");
                sb.Append("       DP.AGENT_NAME             AS DPAGENT,");

                sb.Append("          TEMP_CONS.CUSTOMER_MST_PK AS CONSIGNEEPK_TEMP,");
                sb.Append("      TMPNOTIFY1.CUSTOMER_MST_PK AS NOTIFY1PK_TEMP,");
                sb.Append("        TMPNOTIFY2.CUSTOMER_MST_PK AS NOTIFY2PK_TEMP,");

                sb.Append("     nvl(  CONSIGNEE.CUSTOMER_MST_PK,TEMP_CONS.CUSTOMER_MST_PK) AS CONSIGNEEPK,");
                sb.Append("      nvl(  CONSIGNEE.CUSTOMER_NAME,TEMP_CONS.CUSTOMER_NAME) AS CONSIGNEE,");

                //sb.Append("       CONSIGNEE.CUSTOMER_MST_PK AS CONSIGNEEPK,")
                //sb.Append("       CONSIGNEE.CUSTOMER_NAME   AS CONSIGNEE,")
                sb.Append("       SHIPPER.CUSTOMER_MST_PK   AS SHIPPERPK,");
                sb.Append("       SHIPPER.CUSTOMER_NAME     AS SHIPPER,");
                sb.Append("      nvl( NOTIFY1.CUSTOMER_MST_PK,TMPNOTIFY1.CUSTOMER_MST_PK ) AS NOTIFY1PK,");
                //sb.Append("       NOTIFY1.CUSTOMER_MST_PK   AS NOTIFY1PK,")
                sb.Append("  CASE WHEN JOB.SAC_N1=0  THEN NVL( notify1.customer_name,TMPNOTIFY1.CUSTOMER_NAME) ");
                sb.Append(" Else  'SAME AS CONSIGNEE' END  NOTIFY1, ");
                //  sb.Append("       NOTIFY1.CUSTOMER_NAME     AS NOTIFY1,")
                sb.Append("     nvl(   NOTIFY2.CUSTOMER_MST_PK,TMPNOTIFY2.CUSTOMER_MST_PK) AS NOTIFY2PK,");
                //s                b.Append("       NOTIFY2.CUSTOMER_MST_PK   AS NOTIFY2PK,")
                // sb.Append("       NOTIFY2.CUSTOMER_NAME     AS NOTIFY2,")
                sb.Append("    CASE WHEN job.SAC_N2=0  THEN NVL(notify2.customer_name,TMPNOTIFY2.CUSTOMER_NAME) ");
                sb.Append("    Else  'SAME AS CONSIGNEE' END  NOTIFY2,");
                sb.Append("       JOB.LC_SHIPMENT, (SELECT LMT.LOCATION_NAME FROM LOCATION_MST_TBL LMT WHERE LMT.LOCATION_MST_PK = " + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + ") PLACE_ISSUE, ");
                sb.Append("  PO1.PORT_NAME || ',' || POLCONT.COUNTRY_NAME PODCONTY ,");
                sb.Append("                PO2.PORT_NAME || ',' || PODCONT.COUNTRY_NAME POLCONTY,");
                sb.Append("CASE WHEN BOOK.CARGO_TYPE = 1 THEN");
                sb.Append("                   CASE");
                sb.Append("                     WHEN POO.PORT_ID IS NOT NULL THEN");
                sb.Append("                      POO.PORT_NAME || ',' || POOCONT.COUNTRY_NAME");
                sb.Append("                     ELSE");
                sb.Append("                      ''");
                sb.Append("                   END");
                sb.Append("                  ELSE");
                sb.Append("                   CASE");
                sb.Append("                     WHEN PLR.PLACE_NAME IS NOT NULL THEN");
                sb.Append("                      PLR.PLACE_NAME || ',' || PLRCONT.COUNTRY_NAME");
                sb.Append("                     ELSE");
                sb.Append("                      ''");
                sb.Append("                   END");
                sb.Append("                END PLRCONTY,");
                sb.Append("                CASE");
                sb.Append("                  WHEN BOOK.CARGO_TYPE = 1 THEN");
                sb.Append("                   CASE");
                sb.Append("                     WHEN PFD.PORT_ID IS NOT NULL THEN");
                sb.Append("                      PFD.PORT_NAME || ',' || PLDCONT.COUNTRY_NAME");
                sb.Append("                     ELSE");
                sb.Append("                      ''");
                sb.Append("                   END");
                sb.Append("                  ELSE");
                sb.Append("                   CASE");
                sb.Append("                     WHEN PLD.PLACE_NAME IS NOT NULL THEN");
                sb.Append("                      PLD.PLACE_NAME || ',' || PFDCONT.COUNTRY_NAME");
                sb.Append("                     ELSE");
                sb.Append("                      ''");
                sb.Append("                   END                   ");
                sb.Append("                END PLDCONTY");
                sb.Append("    FROM BOOKING_MST_TBL        BOOK,");

                sb.Append("        TEMP_CUSTOMER_TBL TMPNOTIFY2,");
                sb.Append("     TEMP_CUSTOMER_TBL TMPNOTIFY1,");
                sb.Append("    TEMP_CUSTOMER_TBL TEMP_CONS,");
                sb.Append("       JOB_CARD_TRN   JOB,");
                sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
                sb.Append("       CUSTOMER_MST_TBL       CUST,");
                sb.Append("       AGENT_MST_TBL          CL,");
                sb.Append("       AGENT_MST_TBL          CB,");
                sb.Append("       AGENT_MST_TBL          DP,");
                sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
                sb.Append("       CUSTOMER_MST_TBL       NOTIFY1,");
                sb.Append("       CUSTOMER_MST_TBL       NOTIFY2,");
                sb.Append("       PORT_MST_TBL POO, ");
                sb.Append("       PORT_MST_TBL PFD, ");
                sb.Append("       PLACE_MST_TBL          PLR,");
                sb.Append("       PLACE_MST_TBL          PLD,");
                sb.Append("       PORT_MST_TBL           PO1,");
                sb.Append("       PORT_MST_TBL           PO2,");
                sb.Append("       OPERATOR_MST_TBL       OPR,");
                sb.Append("       VESSEL_VOYAGE_TBL      FIRSTVSL,");
                sb.Append("       VESSEL_VOYAGE_TBL      SECVSL,");
                sb.Append("       VESSEL_VOYAGE_TRN      FIRSTVOY,");
                sb.Append("       VESSEL_VOYAGE_TRN      SECVOY,");
                sb.Append("       CARGO_MOVE_MST_TBL     MOV,");
                sb.Append("       SHIPPING_TERMS_MST_TBL SH,");
                sb.Append("       COUNTRY_MST_TBL        PLRCONT,");
                sb.Append("       COUNTRY_MST_TBL        POLCONT,");
                sb.Append("       COUNTRY_MST_TBL        PODCONT,");
                sb.Append("       COUNTRY_MST_TBL        PFDCONT,");
                sb.Append("       LOCATION_MST_TBL       LMTPLR,");
                sb.Append("       LOCATION_MST_TBL       LMTPFD,");
                sb.Append("       COUNTRY_MST_TBL        POOCONT, ");
                sb.Append("       COUNTRY_MST_TBL        PLDCONT");
                sb.Append("   WHERE CL.AGENT_MST_PK(+) = JOB.CL_AGENT_MST_FK");
                sb.Append("   AND CB.AGENT_MST_PK(+) = JOB.CB_AGENT_MST_FK");
                sb.Append("   AND DP.AGENT_MST_PK(+) = JOB.DP_AGENT_MST_FK");
                sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK(+) = JOB.CONSIGNEE_CUST_MST_FK");
                sb.Append("   AND SHIPPER.CUSTOMER_MST_PK(+) = JOB.SHIPPER_CUST_MST_FK");
                sb.Append("   AND NOTIFY1.CUSTOMER_MST_PK(+) = JOB.NOTIFY1_CUST_MST_FK");
                sb.Append("   AND NOTIFY2.CUSTOMER_MST_PK(+) = JOB.NOTIFY2_CUST_MST_FK");
                sb.Append("   AND CUST.CUSTOMER_MST_PK(+) = BOOK.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK(+) = PO1.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = PO2.PORT_MST_PK");
                sb.Append("   AND MOV.CARGO_MOVE_PK(+) = JOB.CARGO_MOVE_FK");
                sb.Append("   AND SH.SHIPPING_TERMS_MST_PK(+) = JOB.SHIPPING_TERMS_MST_FK");
                sb.Append("   AND BOOK.POO_FK = PLR.PLACE_PK(+)");
                sb.Append("   AND BOOK.PFD_FK = PLD.PLACE_PK(+)");
                sb.Append("   AND BOOK.CARRIER_MST_FK = OPR.OPERATOR_MST_PK(+)");
                sb.Append("   AND JOB.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
                sb.Append("   AND FIRSTVOY.VESSEL_VOYAGE_TBL_FK = FIRSTVSL.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND SECVOY.VESSEL_VOYAGE_TBL_FK = SECVSL.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JOB.VOYAGE_TRN_FK = FIRSTVOY.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JOB.SEC_VOYAGE_TRN_FK = SECVOY.VOYAGE_TRN_PK(+)");
                sb.Append("   AND PO1.COUNTRY_MST_FK = POLCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PO2.COUNTRY_MST_FK = PODCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PLR.LOCATION_MST_FK = LMTPLR.LOCATION_MST_PK(+)");
                sb.Append("   AND LMTPLR.COUNTRY_MST_FK = PLRCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PLD.LOCATION_MST_FK = LMTPFD.LOCATION_MST_PK(+)");
                sb.Append("   AND LMTPFD.COUNTRY_MST_FK = PFDCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND BOOK.POO_FK = POO.PORT_MST_PK(+)");
                sb.Append("   AND BOOK.PFD_FK = PFD.PORT_MST_PK(+)");
                sb.Append("   AND POO.COUNTRY_MST_FK = POOCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PFD.COUNTRY_MST_FK = PLDCONT.COUNTRY_MST_PK(+)");
                sb.Append("     AND JOB.CONSIGNEE_CUST_MST_FK = TEMP_CONS.CUSTOMER_MST_PK(+)");
                sb.Append("   AND JOB.NOTIFY1_CUST_MST_FK=TMPNOTIFY1.CUSTOMER_MST_PK(+)");
                sb.Append("    AND JOB.NOTIFY2_CUST_MST_FK=TMPNOTIFY2.CUSTOMER_MST_PK(+)");
                if (JOBPk != "0")
                {
                    sb.Append("and JOB.JOB_CARD_TRN_PK=" + JOBPk);
                }
            }

            try
            {
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion " Fetch sOperator Details After Job Card Is Selected"

        #region " Fetch Container Type"

        /// <summary>
        /// Fetches the type of the container.
        /// </summary>
        /// <param name="jobPk">The job pk.</param>
        /// <param name="HblEsiStatus">The HBL esi status.</param>
        /// <returns></returns>
        public DataSet FetchContainerType(string jobPk = "0", string HblEsiStatus = "false")
        {
            string strSqlpl = null;
            if (HblEsiStatus == "true")
            {
                strSqlpl = " SELECT DISTINCT TRN.CNTR_DTL_TBL_PK,";
                strSqlpl += "  UPPER(TRN.CONTAINER_NR) CONTAINER_NUMBER,";
                strSqlpl += "     TRN.SEAL_NUMBER,";
                strSqlpl += "     CON.CONTAINER_TYPE_MST_ID,";
                strSqlpl += "     TRN.VOLUME_IN_CBM,";
                strSqlpl += "     TRN.GROSS_WEIGHT,";
                strSqlpl += "     TRN.PACK_COUNT,";
                strSqlpl += "     PK.PACK_TYPE_ID";
                strSqlpl += "  FROM BOOKING_MST_TBL        BOOK,";
                strSqlpl += "       JOB_CARD_TRN   JC,";
                strSqlpl += "       SYN_EBKG_ESI_HDR_TBL   ESIHDR,";
                strSqlpl += "       SYN_EBKG_ESI_CNTR_TBL  TRN,";
                strSqlpl += "       CONTAINER_TYPE_MST_TBL CON,";
                strSqlpl += "       PACK_TYPE_MST_TBL      PK";
                strSqlpl += " WHERE CON.CONTAINER_TYPE_MST_PK(+) = TRN.CONTAINER_TYPE_FK";
                strSqlpl += "   AND TRN.PACK_TYPE_MST_FK = PK.PACK_TYPE_MST_PK(+)";
                strSqlpl += "   AND ESIHDR.BOOKING_SEA_FK = BOOK.BOOKING_MST_PK";
                strSqlpl += "   AND ESIHDR.ESI_HDR_PK = TRN.ESI_HDR_FK";
                strSqlpl += "   AND BOOK.BOOKING_MST_PK = JC.BOOKING_MST_FK";
                if (jobPk != "0")
                {
                    strSqlpl += "    AND JC.JOB_CARD_TRN_PK = " + jobPk;
                }
            }
            else
            {
                strSqlpl = " SELECT JOBT.JOB_CARD_TRN_FK,UPPER(JOBT.CONTAINER_NUMBER) CONTAINER_NUMBER,JOBT.SEAL_NUMBER,CON.CONTAINER_TYPE_MST_ID, ";
                strSqlpl += " JOBT.VOLUME_IN_CBM,JOBT.GROSS_WEIGHT,JOBT.PACK_COUNT,PK.PACK_TYPE_ID ";
                strSqlpl += " FROM JOB_TRN_CONT JOBT, CONTAINER_TYPE_MST_TBL CON,PACK_TYPE_MST_TBL PK ";
                strSqlpl += " WHERE CON.CONTAINER_TYPE_MST_PK = JOBT.CONTAINER_TYPE_MST_FK ";
                strSqlpl += " AND JOBT.PACK_TYPE_MST_FK = PK.PACK_TYPE_MST_PK(+) ";
                strSqlpl += " AND JOBT.JOB_CARD_TRN_FK = " + jobPk;
            }

            try
            {
                return (objWF.GetDataSet(strSqlpl));
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

        #endregion " Fetch Container Type"

        #region "Fetch Temp Customer" 'Manoharan 19Feb07: If HBL is released, update temp customer to permanent

        /// <summary>
        /// Fetches the temporary customer.
        /// </summary>
        /// <param name="jobNr">The job nr.</param>
        /// <returns></returns>
        public DataSet fetchTempCust(string jobNr = "")
        {
            string strSqlpl = null;
            strSqlpl = "select t.cust_customer_mst_fk from BOOKING_MST_TBL t ";
            strSqlpl += " where t.BOOKING_MST_PK =";
            strSqlpl += " (select j.BOOKING_MST_FK from JOB_CARD_TRN j where j.Booking_Mst_Fk IS NOT NULL AND j.jobcard_ref_no = '" + jobNr + "')";

            try
            {
                return (objWF.GetDataSet(strSqlpl));
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

        #endregion "Fetch Temp Customer" 'Manoharan 19Feb07: If HBL is released, update temp customer to permanent

        #region " Fetch Volume & Weight"

        /// <summary>
        /// Fetches the vol weight.
        /// </summary>
        /// <param name="jobPk">The job pk.</param>
        /// <param name="HBl_Esi_status">The h BL_ esi_status.</param>
        /// <returns></returns>
        public DataSet FetchVolWeight(string jobPk = "0", string HBl_Esi_status = "false")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            if (HBl_Esi_status == "true")
            {
                sb.Append("  SELECT DISTINCT SUM(TRN.VOLUME_IN_CBM) AS VOLUME,");
                sb.Append("       SUM(TRN.GROSS_WEIGHT) AS GWEIGHT,");
                sb.Append("       SUM(TRN.CHARGEABLE_WEIGHT) AS CWEIGHT,");
                sb.Append("       SUM(TRN.NET_WEIGHT) AS NWEIGHT,");
                sb.Append("       SUM(TRN.PACK_COUNT) AS PACKCOUNT");
                sb.Append("");
                sb.Append("   FROM BOOKING_MST_TBL       BOOK,");
                sb.Append("       SYN_EBKG_ESI_HDR_TBL  ESIHDR,");
                sb.Append("       SYN_EBKG_ESI_CNTR_TBL TRN,");
                sb.Append("       JOB_CARD_TRN  JC");
                sb.Append("");
                sb.Append("  WHERE ESIHDR.BOOKING_SEA_FK = BOOK.BOOKING_MST_PK");
                sb.Append("   AND ESIHDR.ESI_HDR_PK = TRN.ESI_HDR_FK");
                sb.Append("   AND BOOK.BOOKING_MST_PK = JC.BOOKING_MST_FK");
                if (jobPk != "0")
                {
                    sb.Append("  AND JC.JOB_CARD_TRN_PK=" + jobPk);
                }
            }
            else
            {
                string strSql = null;
                sb.Append("  SELECT SUM(VOLUME_IN_CBM) AS VOLUME,");
                sb.Append("       SUM(GROSS_WEIGHT) AS GWEIGHT,");
                sb.Append("       SUM(CHARGEABLE_WEIGHT) AS CWEIGHT,");
                sb.Append("       SUM(NET_WEIGHT) AS NWEIGHT,");
                sb.Append("       SUM(PACK_COUNT) AS PACKCOUNT");
                sb.Append("    FROM  JOB_TRN_CONT JOBT");
                if (jobPk != "0")
                {
                    sb.Append("  WHERE  JOBT.JOB_CARD_TRN_FK=" + jobPk);
                }
            }

            try
            {
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion " Fetch Volume & Weight"

        #region " Fetch Volume & Weight"

        /// <summary>
        /// Calulates the trans amt.
        /// </summary>
        /// <param name="jobPk">The job pk.</param>
        /// <returns></returns>
        public long CalulateTransAmt(string jobPk = "0")
        {
            string strSql = null;
            strSql = "  SELECT sum(ROUND(q.AMT * (case ";
            strSql = strSql + "  when q.CURRENCY_MST_FK = corp.currency_mst_fk then ";
            strSql = strSql + "   1 else (select exch.exchange_rate from exchange_rate_trn exch ";
            strSql = strSql + "  where q.jobcard_date BETWEEN exch.from_date AND ";
            strSql = strSql + "  NVL(exch.to_date, round(sysdate - .5)) ";
            strSql = strSql + "  AND q.CURRENCY_MST_FK = exch.currency_mst_fk) ";
            strSql = strSql + "  end), 2)) \"AMOUNT\" ";
            strSql = strSql + "  FROM (SELECT JFD.FREIGHT_AMT \"AMT\", ";
            strSql = strSql + "  FROM (SELECT JFD.FREIGHT_AMT \"AMT\",JFD.CURRENCY_MST_FK,J.JOBCARD_DATE  FROM HBL_EXP_TBL H, ";
            strSql = strSql + "  JOB_CARD_SEA_EXP_TBL  J,JOB_TRN_SEA_EXP_FD    JFD,CURRENCY_TYPE_MST_TBL CUR ";
            strSql = strSql + "  WHERE J.JOB_CARD_SEA_EXP_PK = JFD.JOB_CARD_SEA_EXP_FK";
            strSql = strSql + "  AND JFD.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK ";
            strSql = strSql + "  AND JFD.FREIGHT_TYPE = 1";
            strSql = strSql + "  AND J.JOB_CARD_SEA_EXP_PK = " + jobPk;
            strSql = strSql + "  UNION ";
            strSql = strSql + "  SELECT JOC.AMOUNT \"AMT\", JOC.CURRENCY_MST_FK, J.JOBCARD_DATE FROM HBL_EXP_TBL H, ";
            strSql = strSql + "  JOB_CARD_SEA_EXP_TBL  J,JOB_TRN_SEA_EXP_OTH_CHRG JOC,CURRENCY_TYPE_MST_TBL  CUR ";
            strSql = strSql + "  WHERE J.JOB_CARD_SEA_EXP_PK = JOC.JOB_CARD_SEA_EXP_FK ";
            strSql = strSql + "  AND JOC.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK AND JOC.FREIGHT_TYPE = 1 ";
            strSql = strSql + "  AND J.JOB_CARD_SEA_EXP_PK = " + jobPk;
            strSql = strSql + "  ) q,corporate_mst_tbl corp; ";
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

        #endregion " Fetch Volume & Weight"

        #region " Fetch All With Hbl Pk"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="HBLPk">The HBL pk.</param>
        /// <param name="Status">The status.</param>
        /// <returns></returns>
        public DataSet FetchAll(string HBLPk = "0", int Status = 0)
        {
            string strSQl = null;
            strSQl = "SELECT ";
            strSQl += " BOOK.CARGO_TYPE, ";
            strSQl += " HBL.HBL_EXP_TBL_PK,";
            strSQl += " JOB.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_FK,";
            strSQl += " JOB.JOBCARD_REF_NO,";
            strSQl += " HBL.HBL_REF_NO,";
            strSQl += " HBL.HBL_DATE,";
            strSQl += " HBL.VOYAGE_TRN_FK \"FIRSTVOYFK\",";
            //strSQl &= " FIRSTVSL.VESSEL_NAME," & vbCrLf
            //strSQl &= " FIRSTVSL.VESSEL_ID," & vbCrLf
            strSQl += " HBL.VESSEL_NAME,";
            strSQl += " HBL.VESSEL_ID,";
            strSQl += " OPR.OPERATOR_ID, OPR.OPERATOR_NAME,";
            strSQl += " opr.operator_mst_pk,";
            strSQl += " HBL.VOYAGE, ";
            strSQl += " FIRSTVOY.pod_eta,";
            strSQl += " FIRSTVOY.pol_etd,";
            strSQl += " HBL.ARRIVAL_DATE,";
            strSQl += " HBL.DEPARTURE_DATE,";
            strSQl += " HBL.SEC_VOYAGE_TRN_FK \"SECVOYFK\",";
            strSQl += " SECVSL.VESSEL_NAME \"SEC_VESSEL_NAME\",";
            strSQl += " SECVSL.VESSEL_ID \"SEC_VESSEL_ID\",";
            strSQl += " SECVOY.VOYAGE \"SEC_VOYAGE\",";
            strSQl += " HBL.SEC_ETA_DATE,";
            strSQl += " HBL.SEC_ETD_DATE,";
            strSQl += " HBL.SHIPPER_CUST_MST_FK,";
            strSQl += " SH.customer_name AS SHIPPER,";

            strSQl += " nvl(HBL.CONSIGNEE_CUST_MST_FK,TEMP_CONS.CUSTOMER_MST_PK)as CONSIGNEE_CUST_MST_FK,";
            strSQl += " (case when hbl.is_to_order = '0' then nvl(CO.customer_name,TEMP_CONS.customer_name) else 'To Order' end) as CONSIGNEE_NAME, ";
            strSQl += " nvl( hbl.consignee_name,TEMP_CONS.customer_name) AS CONSIGNEE,";
            strSQl += " nvl(hbl.consignee_name,TEMP_CONS.customer_name) AS CONSIGNEE_TOORDER,";
            strSQl += " HBL.IS_TO_ORDER, ";

            strSQl += " nvl( HBL.NOTIFY1_CUST_MST_FK,TMPNOTIFY1.CUSTOMER_MST_PK) as NOTIFY1_CUST_MST_FK ,";
            strSQl += "(case  when NVL( hbl.sac_n1,0) = '0' then  nvl(  N1.customer_name,TMPNOTIFY1.customer_name)  else 'SAME AS CONSIGNEE' end) as NOTIFY1,";
            //  strSQl &= " HBL.NOTIFY1_CUST_MST_FK," & vbCrLf
            //  strSQl &= " N1.customer_name AS NOTIFY1," & vbCrLf
            //  strSQl &= " HBL.NOTIFY2_CUST_MST_FK," & vbCrLf
            strSQl += " nvl( HBL.NOTIFY2_CUST_MST_FK,TMPNOTIFY2.CUSTOMER_MST_PK) as  NOTIFY2_CUST_MST_FK,";
            strSQl += "       (case when NVL(hbl.sac_n2,0) = '0' then nvl(N2.customer_name,TMPNOTIFY2.customer_name) else 'SAME AS CONSIGNEE' end) as NOTIFY2,";
            //' strSQl &= " N2.customer_name AS NOTIFY2," & vbCrLf
            strSQl += " HBL.CB_AGENT_MST_FK,";
            strSQl += " CB.agent_name as CBAGENT,";
            strSQl += " HBL.DP_AGENT_MST_FK,";
            strSQl += " DP.agent_name AS DPAGENT,";
            strSQl += " HBL.CL_AGENT_MST_FK,";
            strSQl += " CL.agent_name AS CLAGENT,";
            strSQl += " POL.PORT_ID AS POL,";
            strSQl += " POD.port_mst_pk as \"PODPK\",";
            strSQl += " POL.port_mst_pk as \"POLPK\",";
            strSQl += " POD.PORT_ID AS POD,";
            strSQl += " CASE WHEN BOOK.CARGO_TYPE=1 AND BOOK.BUSINESS_TYPE=2 THEN CASE WHEN COLPORT.PORT_ID IS NOT NULL THEN COLPORT.PORT_ID ELSE POL.PORT_ID END ELSE CASE WHEN COLP.PLACE_CODE IS NOT NULL THEN COLP.PLACE_CODE ELSE POL.PORT_ID END END RECP,";
            strSQl += " CASE WHEN BOOK.CARGO_TYPE=1 AND BOOK.BUSINESS_TYPE=2 THEN CASE WHEN DELPORT.PORT_ID IS NOT NULL THEN DELPORT.PORT_ID ELSE POD.PORT_ID END ELSE CASE WHEN DELP.PLACE_CODE IS NOT NULL THEN DELP.PLACE_CODE ELSE POD.PORT_ID END END DELP,";
            strSQl += " HBL.GOODS_DESCRIPTION,";
            strSQl += " HBL.TOTAL_VOLUME, ";
            strSQl += " HBL.TOTAL_CHARGE_WEIGHT,";
            strSQl += " HBL.TOTAL_GROSS_WEIGHT,";
            strSQl += " HBL.TOTAL_PACK_COUNT, ";
            strSQl += " HBL.TOTAL_NET_WEIGHT, ";
            strSQl += " HBL.HBL_ORIGINAL_PRINTS, ";
            strSQl += " HBL.MARKS_NUMBERS,";
            strSQl += " CUST.CUSTOMER_NAME,";
            strSQl += " SH.inco_code, ";
            strSQl += " MOV.cargo_move_code, ";
            strSQl += " JOB.PYMT_TYPE, ";
            strSQl += " HBL.HBL_STATUS,";
            strSQl += " job.job_card_status,";
            strSQl += " HBL.LETTER_OF_CREDIT,";
            strSQl += " HBL.VERSION_NO,";

            strSQl += " NVL(JOB.SHIPPER_ADDRESS,HBL.SHIPPER_ADDRESS) SHIPPER_ADDRESS,";
            strSQl += " NVL(JOB.CONSIGNEE_ADDRESS,HBL.CONSIGNEE_ADDRESS) CONSIGNEE_ADDRESS,";
            //strSQl &= " HBL.CONSIGNEE_ADDRESS," & vbCrLf
            strSQl += " (case when NVL(hbl.sac_n1,0) = 0 then NVL(JOB.NOTIFY_ADDRESS,HBL.NOTIFY1_ADDRESS) else  ' ' end) as NOTIFY1_ADDRESS,";
            //strSQl &= " HBL.NOTIFY1_ADDRESS," & vbCrLf
            //strSQl &= " HBL.NOTIFY2_ADDRESS," & vbCrLf
            strSQl += "  (case when NVL(hbl.sac_n2,0) = 0 then HBL.NOTIFY2_ADDRESS else ' ' end) as NOTIFY2_ADDRESS,";
            strSQl += " HBL.CB_AGENT_ADDRESS,";
            strSQl += " HBL.CL_AGENT_ADDRESS,";
            strSQl += " HBL.DP_AGENT_ADDRESS,";
            strSQl += "  HBL.LC_NUMBER,";
            strSQl += "  HBL.LC_DATE,";
            strSQl += "  HBL.LC_EXPIRES_ON, HBL.PLACE_ISSUE, ";
            strSQl += " CASE WHEN BOOK.CARGO_TYPE=1 AND BOOK.BUSINESS_TYPE=2 THEN CASE WHEN COLPORT.PORT_NAME IS NOT NULL THEN COLPORT.PORT_NAME ELSE POL.PORT_NAME END ELSE CASE WHEN COLP.PLACE_NAME IS NOT NULL THEN COLP.PLACE_NAME ELSE POL.PORT_NAME END END PLRCONTY,";
            strSQl += " CASE WHEN BOOK.CARGO_TYPE=1 AND BOOK.BUSINESS_TYPE=2 THEN CASE WHEN DELPORT.PORT_NAME IS NOT NULL THEN DELPORT.PORT_NAME ELSE POD.PORT_NAME END ELSE CASE WHEN DELP.PLACE_NAME IS NOT NULL THEN DELP.PLACE_NAME ELSE POD.PORT_NAME END END PLDCONTY,";
            //strSQl &= "  HBL.PLR_COUNTRY PLRCONTY,"
            //strSQl &= " HBL.PFD_COUNTRY PLDCONTY, "
            //strSQl &= " HBL.POL_COUNTRY POLCONTY,"
            //strSQl &= " HBL.POD_COUNTRY PODCONTY,"
            strSQl += " POL.PORT_NAME AS POLCONTY,";
            strSQl += " POD.PORT_NAME AS PODCONTY,";
            strSQl += " HBL.SURRENDER_DT, ";
            strSQl += " BOOK.BOOKING_DATE ";
            strSQl += " FROM HBL_EXP_TBL HBL,";
            strSQl += " TEMP_CUSTOMER_TBL      TMPNOTIFY2,";
            strSQl += " TEMP_CUSTOMER_TBL      TMPNOTIFY1,";
            strSQl += " TEMP_CUSTOMER_TBL      TEMP_CONS,";
            strSQl += " OPERATOR_MST_TBL OPR,";
            strSQl += " JOB_CARD_TRN JOB,";
            strSQl += " BOOKING_MST_TBL BOOK,";
            strSQl += " CUSTOMER_MST_TBL SH,";
            strSQl += " CUSTOMER_MST_TBL CO,";
            strSQl += " CUSTOMER_MST_TBL N1,";
            strSQl += " CUSTOMER_MST_TBL N2,";
            strSQl += " AGENT_MST_TBL CB,";
            strSQl += " AGENT_MST_TBL DP,";
            strSQl += " AGENT_MST_TBL CL,";
            strSQl += " PORT_MST_TBL POL,";
            strSQl += " PORT_MST_TBL POD,";
            strSQl += " CUSTOMER_MST_TBL CUST,";
            strSQl += " PLACE_MST_TBL COLP,";
            strSQl += " PLACE_MST_TBL DELP,";
            strSQl += " PORT_MST_TBL COLPORT,";
            strSQl += " PORT_MST_TBL DELPORT,";
            strSQl += " VESSEL_VOYAGE_TRN FIRSTVOY,";
            strSQl += " VESSEL_VOYAGE_TRN SECVOY, ";
            strSQl += " VESSEL_VOYAGE_TBL FIRSTVSL,";
            strSQl += " VESSEL_VOYAGE_TBL SECVSL,";
            strSQl += " CARGO_MOVE_MST_TBL MOV,";
            strSQl += " SHIPPING_TERMS_MST_TBL SH";

            strSQl += " WHERE";
            strSQl += " JOB.BOOKING_MST_FK      =  BOOK.BOOKING_MST_PK ";
            strSQl += " AND OPR.OPERATOR_MST_PK(+)  = BOOK.CARRIER_MST_FK ";
            strSQl += " AND SH.CUSTOMER_MST_PK(+)  =  HBL.SHIPPER_CUST_MST_FK";
            strSQl += " AND CO.CUSTOMER_MST_PK(+)  =  HBL.CONSIGNEE_CUST_MST_FK ";
            strSQl += " AND N1.CUSTOMER_MST_PK(+)  =  HBL.NOTIFY1_CUST_MST_FK";
            strSQl += " AND N2.CUSTOMER_MST_PK(+)  =  HBL.NOTIFY2_CUST_MST_FK";
            strSQl += " AND CB.AGENT_MST_PK(+)     =  HBL.CB_AGENT_MST_FK";
            strSQl += " AND DP.AGENT_MST_PK(+)     =  HBL.DP_AGENT_MST_FK";
            strSQl += " AND CL.AGENT_MST_PK(+)     =  HBL.CL_AGENT_MST_FK";
            strSQl += " AND POL.PORT_MST_PK(+)     =  BOOK.PORT_MST_POL_FK";
            strSQl += " AND POD.PORT_MST_PK(+)     =  BOOK.PORT_MST_POD_FK";
            strSQl += " and MOV.CARGO_MOVE_PK(+) = JOB.CARGO_MOVE_FK  ";
            strSQl += " and SH.SHIPPING_TERMS_MST_PK(+)=job.SHIPPING_TERMS_MST_FK  ";
            strSQl += " AND COLP.PLACE_PK(+)       =  BOOK.COL_PLACE_MST_FK";
            strSQl += " AND DELP.PLACE_PK(+)       =  BOOK.DEL_PLACE_MST_FK";
            strSQl += " AND COLPORT.PORT_MST_PK(+)       =  BOOK.COL_PLACE_MST_FK";
            strSQl += " AND DELPORT.PORT_MST_PK(+)       =  BOOK.DEL_PLACE_MST_FK";
            strSQl += " AND CUST.CUSTOMER_MST_PK(+) =  BOOK.CUST_CUSTOMER_MST_FK";
            strSQl += " AND JOB.CONSIGNEE_CUST_MST_FK = TEMP_CONS.CUSTOMER_MST_PK(+)";
            strSQl += " AND JOB.NOTIFY1_CUST_MST_FK=TMPNOTIFY1.CUSTOMER_MST_PK(+)";
            strSQl += " AND JOB.NOTIFY2_CUST_MST_FK=TMPNOTIFY2.CUSTOMER_MST_PK(+)";

            if (Status == 3)
            {
                strSQl += " AND HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK ";
            }
            else
            {
                strSQl += " AND HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK ";
            }
            strSQl += " AND HBL.SEC_VOYAGE_TRN_FK   = SECVOY.VOYAGE_TRN_PK(+)";
            strSQl += " AND HBL.VOYAGE_TRN_FK       = FIRSTVOY.VOYAGE_TRN_PK(+)";
            strSQl += " AND FIRSTVOY.VESSEL_VOYAGE_TBL_FK = FIRSTVSL.VESSEL_VOYAGE_TBL_PK(+)";
            strSQl += " AND SECVOY.VESSEL_VOYAGE_TBL_FK = SECVSL.VESSEL_VOYAGE_TBL_PK(+)";
            strSQl += " AND HBL.HBL_EXP_TBL_PK= " + HBLPk;
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

        #endregion " Fetch All With Hbl Pk"

        #region " Generate HBL No"

        /// <summary>
        /// Genrates the HBL no.
        /// </summary>
        /// <param name="Location">The location.</param>
        /// <param name="emp">The emp.</param>
        /// <param name="User">The user.</param>
        /// <param name="SID">The sid.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="PODID">The podid.</param>
        /// <returns></returns>
        public string genrateHBLNo(long Location, long emp, long User, string SID = "", string POLID = "", string PODID = "")
        {
            return GenerateProtocolKey("HBL", Location, emp, DateTime.Today, "", "", POLID, User, new WorkFlow(), SID, PODID);
        }

        #endregion " Generate HBL No"

        #region "To Fetch JobcardHBLreleased"

        /// <summary>
        /// Fetches the HBL released.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
        public object FetchHBLReleased(string JobPk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("select DISTINCT JOB.JOB_CARD_TRN_PK,JOB.JOBCARD_REF_NO");
                sb.Append(" from HBL_EXP_TBL HBL,JOB_CARD_TRN JOB ");
                sb.Append("where HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append(" AND HBL.JOB_CARD_SEA_EXP_FK='" + JobPk + "'");
                sb.Append(" AND HBL.HBL_STATUS='2'");
                return objWF.GetDataSet(sb.ToString());
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
        /// Updates the HBL status.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
        public ArrayList UpdateHBLStatus(string JobPk)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;

                str = "UPDATE JOB_CARD_TRN  j SET ";
                str += "   j.HBL_RELEASED_STATUS = 1";
                str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;

                var _with4 = updCmdUser;
                _with4.Connection = objWK.MyConnection;
                _with4.Transaction = TRAN;
                _with4.CommandType = CommandType.Text;
                _with4.CommandText = str;
                intIns = Convert.ToInt16(_with4.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Protocol Generated Succesfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        /// <summary>
        /// Checkstatuses the specified job pk.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="dsAgtCollection">The ds agt collection.</param>
        /// <returns></returns>
        public object checkstatus(string JobPk, DataSet dsAgtCollection)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 i = default(Int32);
            bool IsDPAgent = false;
            bool IsCBAgent = false;
            try
            {
                if (dsAgtCollection.Tables.Count > 0)
                {
                    if (dsAgtCollection.Tables[0].Rows.Count > 0)
                    {
                        for (i = 0; i <= dsAgtCollection.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"].ToString()))
                            {
                                if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 2)
                                {
                                    IsDPAgent = true;
                                }
                                else if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 1)
                                {
                                    IsCBAgent = true;
                                }
                            }
                        }
                    }
                }

                if (IsDPAgent == true & IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append("  AND JOB.COLLECTION_STATUS=1");
                    sb.Append("  AND JOB.PAYEMENT_STATUS=1");
                    sb.Append("  AND JOB.HBL_RELEASED_STATUS=1");
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                }
                else if (IsDPAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append(" AND JOB.COLLECTION_STATUS=1");
                    sb.Append(" AND JOB.PAYEMENT_STATUS=1");
                    sb.Append(" AND JOB.HBL_RELEASED_STATUS=1");
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                }
                else if (IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append("  AND JOB.COLLECTION_STATUS=1");
                    sb.Append("  AND JOB.PAYEMENT_STATUS=1");
                    sb.Append("  AND JOB.HBL_RELEASED_STATUS=1");
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                }
                else
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append(" AND JOB.COLLECTION_STATUS=1");
                    sb.Append(" AND JOB.PAYEMENT_STATUS=1");
                    sb.Append(" AND JOB.HBL_RELEASED_STATUS=1");
                }
                return objWF.GetDataSet(sb.ToString());
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
        /// Updatejobcarddates the specified job pk.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
        public ArrayList updatejobcarddate(string JobPk)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            short intIns = default(short);
            try
            {
                updCmdUser.Transaction = TRAN;

                str = "UPDATE JOB_CARD_TRN  j SET ";
                str += "   j.JOB_CARD_STATUS = 2, j.JOB_CARD_CLOSED_ON = SYSDATE";
                str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;
                var _with5 = updCmdUser;
                _with5.Connection = objWK.MyConnection;
                _with5.Transaction = TRAN;
                _with5.CommandType = CommandType.Text;
                _with5.CommandText = str;
                intIns = Convert.ToInt16(_with5.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Protocol Generated Succesfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "To Fetch JobcardHBLreleased"

        #region " Save Main"

        public ArrayList save(long nLocationfk, long nUserfk, string strPk, string JobPk, string HBLNo, string CBAgentPK, string CLAgentPK, string ConsigneePK, string DPAgentPK, string VESSEL_ID,
        string FirstVoyageFk, string FirstVessel, string FirstVoyage, string GoodDesc, string Marks, string Notify1PK, string Notify2PK, string OperatorPk, string sOperator, string SecondVoyageFk,
        string SecondVessel, string SecondVoyage, string ShipperPK, string Shipperaddress, string Consigneeaddress, string Notify1address, string Notify2address, string CBAgentaddress, string DPAgentaddress, string CLAgentaddress,
        string PODPk, string POLPk, string PackCount, string Gweight, string NWeight, string CWeight, string TotVol, string HDate, string ETA_DATE, string ETD_DATE,
        string SEC_ETA_DATE, string SEC_ETD_DATE, string ARRIVAL_DATE, string DEPARTURE_DATE, string cargo_move, string pymt_type, string shipping_terms, string EmpPk, bool Is_To_Order, string ConsigneeName,
        string status = "0", string Version = "0", string Letter = "", string TotalPrints = "", bool fclFlag = true, string strTransaction = "", string hdnadd = "", string Corrector = "", string LCNumber = "", string LCDate = "",
        string LCExpiresOn = "", string PLACE_ISSUE = "", string PLRCountry = "", string POLCountry = "", string PODCountry = "", string PFDCountry = "", string SurrDt = "", int SAC_N1 = 0, int SAC_N2 = 0, string sid = "",
        string polid = "", string podid = "", string COD = "", int pfdfk = 0)
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
                        arrMessage = objSBE.SaveVesselMaster(Convert.ToInt64(FirstVoyageFk), Convert.ToString(getDefault(FirstVessel, "")),
                            Convert.ToInt32(getDefault(OperatorPk, 0)), Convert.ToString(getDefault(VESSEL_ID, "")), Convert.ToString(getDefault(FirstVoyage, "")), 
                            objWK.MyCommand, Convert.ToInt32(getDefault(POLPk, 0)), Convert.ToString(PODPk),DateTime.Now, Convert.ToDateTime(getDefault(ETD_DATE, null)),
                            DateTime.Now,
                            Convert.ToDateTime(getDefault(ETA_DATE, null)), 
                            DateTime.Now,
                            DateTime.Now);
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
                var _with6 = objWK.MyCommand;
                if (Convert.ToInt32(strPk) != 0)
                {
                    _with6.CommandText = objWK.MyUserName + ".HBL_EXP_TBL_PKG.HBL_EXP_TBL_UPD";
                }
                else
                {
                    _with6.CommandText = objWK.MyUserName + ".HBL_EXP_TBL_PKG.HBL_EXP_TBL_INS";
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

                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.Transaction = TRAN;
                var _with7 = _with6.Parameters;
                if (strPk != "0")
                {
                    _with7.Add("HBL_EXP_TBL_PK_IN", Convert.ToInt64(strPk));
                    _with7.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY);
                    //
                    _with7.Add("VERSION_NO_IN", Convert.ToInt64(Version));
                    _with7.Add("DELETE_CLAUSE_FLAG_IN", deleteflag);
                }
                else
                {
                    _with7.Add("CREATED_BY_FK_IN", CREATED_BY);

                    if (Corrector == "YES")
                    {
                        _with7.Add("CORRECTOR_HBL_FK_IN", CrctrBLPK);
                    }
                    if (COD == "YES")
                    {
                        _with7.Add("CORRECTOR_HBL_FK_IN", CrctrBLPK);
                        _with7.Add("POD_MST_FK_IN", PODPk);
                        _with7.Add("PFD_MST_FK_IN", pfdfk);
                        _with7.Add("OPERATOR_FK_IN", OperatorPk);
                    }
                }
                _with7.Add("JOB_CARD_SEA_EXP_FK_IN", (string.IsNullOrEmpty(JobPk) ? "" : Convert.ToString(JobPk)));
                _with7.Add("HBL_REF_NO_IN", getDefault((HBLNo), DBNull.Value));
                if (Corrector == "YES")
                {
                    _with7.Add("HBL_DATE_IN", System.DateTime.Now.ToString("dd/MM/yyyy"));
                }
                else
                {
                    _with7.Add("HBL_DATE_IN", getDefault(HDate, DBNull.Value));
                }
                _with7.Add("FIRST_VSLVOY_FK_IN", getDefault((strVoyagepk), DBNull.Value));
                _with7.Add("VESSEL_NAME_IN", getDefault((FirstVessel), DBNull.Value));
                _with7.Add("VESSEL_ID_IN", getDefault((VESSEL_ID), DBNull.Value));
                _with7.Add("VOYAGE_IN", getDefault((FirstVoyage), DBNull.Value));
                _with7.Add("ETA_DATE_IN", getDefault(ETA_DATE, DBNull.Value));
                _with7.Add("ETD_DATE_IN", getDefault(ETD_DATE, DBNull.Value));
                _with7.Add("ARRIVAL_DATE_IN", getDefault(ARRIVAL_DATE, DBNull.Value));
                _with7.Add("DEPARTURE_DATE_IN", getDefault(DEPARTURE_DATE, DBNull.Value));
                _with7.Add("SECOND_VSLVOY_FK_IN", getDefault((SecondVoyageFk), DBNull.Value));
                _with7.Add("SEC_VESSEL_NAME_IN", getDefault((SecondVessel), DBNull.Value));
                _with7.Add("SEC_VOYAGE_IN", getDefault((SecondVoyage), DBNull.Value));
                _with7.Add("SEC_ETA_DATE_IN", getDefault(SEC_ETA_DATE, DBNull.Value));
                _with7.Add("SEC_ETD_DATE_IN", getDefault(SEC_ETD_DATE, DBNull.Value));
                _with7.Add("SHIPPER_CUST_MST_FK_IN", getDefault((ShipperPK), DBNull.Value));
                _with7.Add("CONSIGNEE_CUST_MST_FK_IN", getDefault((ConsigneePK), DBNull.Value));
                _with7.Add("NOTIFY1_CUST_MST_FK_IN", getDefault((Notify1PK), DBNull.Value));
                _with7.Add("NOTIFY2_CUST_MST_FK_IN", getDefault((Notify2PK), DBNull.Value));
                _with7.Add("CB_AGENT_MST_FK_IN", getDefault((CBAgentPK), DBNull.Value));
                _with7.Add("DP_AGENT_MST_FK_IN", getDefault((DPAgentPK), DBNull.Value));
                _with7.Add("CL_AGENT_MST_FK_IN", getDefault((CLAgentPK), DBNull.Value));
                _with7.Add("SHIPPER_ADDRESS_IN", (string.IsNullOrEmpty(Shipperaddress) ?"" : Shipperaddress));
                //If Is_To_Order = True Then
                //    .Add("CONSIGNEE_ADDRESS_IN", IIf(hdnadd = "", DBNull.Value, hdnadd))
                //Else
                //    .Add("CONSIGNEE_ADDRESS_IN", IIf(Consigneeaddress = "", DBNull.Value, Consigneeaddress))
                //End If
                _with7.Add("CONSIGNEE_ADDRESS_IN", (string.IsNullOrEmpty(Consigneeaddress) ? "" : Consigneeaddress));
                _with7.Add("NOTIFY1_ADDRESS_IN", (string.IsNullOrEmpty(Notify1address) ? "" : Notify1address));
                _with7.Add("NOTIFY2_ADDRESS_IN", (string.IsNullOrEmpty(Notify2address) ? "" : Notify2address));
                _with7.Add("CB_AGENT_ADDRESS_IN", (string.IsNullOrEmpty(CBAgentaddress) ? "": CBAgentaddress));
                _with7.Add("DP_AGENT_ADDRESS_IN", (string.IsNullOrEmpty(DPAgentaddress) ?"" : DPAgentaddress));
                _with7.Add("CL_AGENT_ADDRESS_IN", (string.IsNullOrEmpty(CLAgentaddress) ? "" : CLAgentaddress));

                _with7.Add("MARKS_NUMBERS_IN", getDefault((Marks), DBNull.Value));
                _with7.Add("GOODS_DESCRIPTION_IN", getDefault((GoodDesc), DBNull.Value));
                //added by gopi
                _with7.Add("CARGO_MOVE_IN", (string.IsNullOrEmpty(cargo_move) ? "" : cargo_move));
                _with7.Add("PYMT_TYPE_IN", (pymt_type == "Collect" ? "2" : "1"));
                //ifDBNull(pymt_type))
                _with7.Add("SHIPPING_TERMS_IN", (string.IsNullOrEmpty(shipping_terms) ? "" : shipping_terms));
                if (Is_To_Order == true)
                {
                    _with7.Add("CONSIGNEE_NAME_IN", DBNull.Value);
                    // added by gopi for saving consignee name
                }
                else
                {
                    _with7.Add("CONSIGNEE_NAME_IN", getDefault((ConsigneeName), DBNull.Value));
                    // added by gopi for saving consignee name
                }

                _with7.Add("Is_To_Order_IN", (Is_To_Order == true ? "1" : "0"));

                _with7.Add("HBL_STATUS_IN", Convert.ToInt64(status));
                _with7.Add("MBL_EXP_TBL_FK_IN", DBNull.Value);
                _with7.Add("TOTAL_VOLUME_IN", ifDBNull(Convert.ToDouble(TotVol)));
                _with7.Add("TOTAL_GROSS_WEIGHT_IN", ifDBNull(Convert.ToDouble(Gweight)));
                if ((string.IsNullOrEmpty(NWeight)))
                {
                    _with7.Add("TOTAL_NET_WEIGHT_IN", DBNull.Value);
                }
                else
                {
                    _with7.Add("TOTAL_NET_WEIGHT_IN", ifDBNull(Convert.ToDouble(NWeight)));
                }
                //.Add("TOTAL_NET_WEIGHT_IN", IIf(NWeight = "", DBNull.Value, CDbl(NWeight))) 'ifDBNull(CDbl(NWeight)))
                if ((string.IsNullOrEmpty(CWeight)))
                {
                    _with7.Add("TOTAL_CHARGE_WEIGHT_IN", DBNull.Value);
                }
                else
                {
                    _with7.Add("TOTAL_CHARGE_WEIGHT_IN", ifDBNull(Convert.ToDouble(CWeight)));
                }
                //.Add("TOTAL_CHARGE_WEIGHT_IN", ifDBNull(CDbl(CWeight)))
                _with7.Add("TOTAL_PACK_COUNT_IN", getDefault((PackCount), DBNull.Value));
                _with7.Add("HBL_ORIGINAL_PRINTS_IN", getDefault((TotalPrints), DBNull.Value));
                _with7.Add("REMARKS_IN", getDefault((HBLNo), DBNull.Value));
                _with7.Add("CONFIG_MST_PK_IN", Convert.ToInt64(ConfigurationPK));
                //'
                _with7.Add("LETTER_OF_CREDIT_IN", getDefault((Letter), DBNull.Value));
                _with7.Add("LC_NUMBER_IN", getDefault((LCNumber), DBNull.Value));
                _with7.Add("LC_DATE_IN", getDefault((LCDate), DBNull.Value));
                _with7.Add("LC_EXPIRES_ON_IN", getDefault((LCExpiresOn), DBNull.Value));
                _with7.Add("PLACE_ISSUE_IN", getDefault((PLACE_ISSUE), DBNull.Value));
                _with7.Add("PLR_COUNTRY_IN", getDefault((PLRCountry), DBNull.Value));
                _with7.Add("POL_COUNTRY_IN", getDefault((POLCountry), DBNull.Value));
                _with7.Add("POD_COUNTRY_IN", getDefault((PODCountry), DBNull.Value));
                _with7.Add("PFD_COUNTRY_IN", getDefault((PFDCountry), DBNull.Value));
                _with7.Add("SURRENDER_DT_IN", getDefault(SurrDt, DBNull.Value));

                _with7.Add("SAC_N1_IN", SAC_N1);
                _with7.Add("SAC_N2_IN", SAC_N2);

                _with7.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_EXP_TBL_PK").Direction = ParameterDirection.Output;

                exe = Convert.ToInt16(_with6.ExecuteNonQuery());
                strPk = Convert.ToString(_with6.Parameters["RETURN_VALUE"].Value);
                //If strPk = 0 Then
                //    TRAN.Rollback()
                //    arrMessage.Add("HBL Already Exists")
                //    Return arrMessage
                //End If
                if (exe > 0)
                {
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
                    //Manjunath for HBL status update
                    int ESIflag = 0;
                    ESIflag = objHBLEntry.ESIflagCheck(Convert.ToInt32(strPk));
                    if (ESIflag == 1)
                    {
                        if (Convert.ToInt32(status) == 1)
                        {
                            OracleCommand updESICommand = new OracleCommand();
                            var _with8 = updESICommand;
                            _with8.Parameters.Clear();
                            _with8.Connection = objWK.MyConnection;
                            _with8.CommandType = CommandType.StoredProcedure;
                            _with8.Transaction = TRAN;
                            _with8.CommandText = objWK.MyUserName + ".HBL_EXP_TBL_PKG.HBL_ESI_EXP_TBL_UPD";
                            var _with9 = _with8.Parameters;
                            _with9.Add("HBL_EXP_TBL_PK_IN", Convert.ToInt64(strPk));
                            _with9.Add("HBL_STATUS_IN", Convert.ToInt64(status));
                            _with9.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "HBL_EXP_TBL_PK").Direction = ParameterDirection.Output;
                            _with8.ExecuteNonQuery();
                        }
                    }
                    //End
                    //To update the booking with the new values in case of cod.
                    if (COD == "YES")
                    {
                        OracleCommand updBKGCommand = new OracleCommand();
                        var _with10 = updBKGCommand;
                        _with10.Parameters.Clear();
                        _with10.Connection = objWK.MyConnection;
                        _with10.CommandType = CommandType.StoredProcedure;
                        _with10.Transaction = TRAN;
                        _with10.CommandText = objWK.MyUserName + ".HBL_EXP_TBL_PKG.HBL_BKG_UPD";
                        var _with11 = _with10.Parameters;
                        _with11.Add("JOB_CARD_SEA_EXP_FK_IN", (string.IsNullOrEmpty(JobPk) ? 0 : Convert.ToInt64(JobPk)));
                        _with11.Add("FIRST_VSLVOY_FK_IN", getDefault((strVoyagepk), DBNull.Value));
                        _with11.Add("ETA_DATE_IN", getDefault(ETA_DATE, DBNull.Value));
                        _with11.Add("ETD_DATE_IN", getDefault(ETD_DATE, DBNull.Value));
                        _with11.Add("ARRIVAL_DATE_IN", getDefault(ARRIVAL_DATE, DBNull.Value));
                        _with11.Add("DEPARTURE_DATE_IN", getDefault(DEPARTURE_DATE, DBNull.Value));
                        _with11.Add("VESSEL_NAME_IN", getDefault((FirstVessel), DBNull.Value));
                        _with11.Add("VOYAGE_IN", getDefault((FirstVoyage), DBNull.Value));
                        _with11.Add("CONSIGNEE_CUST_MST_FK_IN", getDefault((ConsigneePK), DBNull.Value));
                        _with11.Add("NOTIFY1_CUST_MST_FK_IN", getDefault((Notify1PK), DBNull.Value));
                        _with11.Add("NOTIFY2_CUST_MST_FK_IN", getDefault((Notify2PK), DBNull.Value));
                        _with11.Add("DP_AGENT_MST_FK_IN", getDefault((DPAgentPK), DBNull.Value));
                        _with11.Add("CONSIGNEE_ADDRESS_IN", (string.IsNullOrEmpty(Consigneeaddress) ? "" : Consigneeaddress));
                        _with11.Add("NOTIFY1_ADDRESS_IN", (string.IsNullOrEmpty(Notify1address) ? "" : Notify1address));
                        _with11.Add("NOTIFY2_ADDRESS_IN", (string.IsNullOrEmpty(Notify2address) ? "" : Notify2address));
                        _with11.Add("DP_AGENT_ADDRESS_IN", (string.IsNullOrEmpty(DPAgentaddress) ? "" : DPAgentaddress));
                        _with11.Add("POD_MST_FK_IN", PODPk);
                        _with11.Add("PFD_MST_FK_IN", pfdfk);
                        _with11.Add("OPERATOR_FK_IN", OperatorPk);
                        _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_EXP_TBL_PK").Direction = ParameterDirection.Output;
                        _with10.ExecuteNonQuery();
                    }
                    if (arrMessage.Count >= 1)
                    {
                        TRAN.Commit();
                        return arrMessage;
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
                                //    objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), "", "", JCPKs);
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 1, Session["USER_PK"]);
                                //    }
                                //}
                                //catch (Exception ex)
                                //{
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 2, Session["USER_PK"]);
                                //    }
                                //}
                            }
                        }
                        //*****************************************************************
                    }
                    else
                    {
                        //added by suryaprasad for protocol rollback
                        if (chkFlag)
                        {
                            RollbackProtocolKey("HBL", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), HBLNo, DateTime.Now);
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
                    RollbackProtocolKey("HBL", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), HBLNo, DateTime.Now);

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
                    RollbackProtocolKey("HBL", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), HBLNo, DateTime.Now);
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion " Save Main"

        #region "ESIflag"

        public int ESIflagCheck(int HBLPk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT J.ESI_FLAG FROM JOB_CARD_TRN J,HBL_EXP_TBL H");
            sb.Append(" WHERE H.HBL_EXP_TBL_PK=J.HBL_HAWB_FK");
            sb.Append(" AND H.HBL_EXP_TBL_PK=" + HBLPk);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "ESIflag"

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

            nEnd = str.Length.ToString();
            str = str.Substring(0, Convert.ToInt32(nEnd) - 2);
            dsTran.Tables.Add("dtTran");
            dsTran.Tables["dtTran"].Columns.Add("HBL_EXP_TBL_FK");
            dsTran.Tables["dtTran"].Columns.Add("BL_CLAUSE_FK");
            dsTran.Tables["dtTran"].Columns.Add("BL_DESCRIPTION");
            arrMain = str.Split('#');
            for (arrLen = 0; arrLen <= arrMain.Length - 1; arrLen++)
            {
                strRow = arrMain.GetValue(arrLen).ToString();
                arrRow = strRow.Split('^');
                dRow = dsTran.Tables["dtTran"].NewRow();
                if (arrRow.GetValue(0) == "null")
                {
                    dRow["HBL_EXP_TBL_FK"] = System.DBNull.Value;
                }
                else
                {
                    dRow["HBL_EXP_TBL_FK"] = arrRow.GetValue(0);
                }

                dRow["BL_DESCRIPTION"] = arrRow.GetValue(1);

                if (arrRow.GetValue(2) == "null")
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

                var _with12 = InsertCmd;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = objWK.MyUserName + ".HBL_BL_CLAUSE_TBL_PKG.HBL_BL_CLAUSE_TBL_INS";
                _with12.Parameters.Clear();
                _with12.Parameters.Add("HBL_EXP_TBL_FK_IN", Strpk);
                _with12.Parameters.Add("BL_DESCRIPTION_IN", OracleDbType.Varchar2, 500, "BL_DESCRIPTION");
                _with12.Parameters.Add("BL_CLAUSE_FK_IN", OracleDbType.Int32, 10, "BL_CLAUSE_FK");
                _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_BL_CLAUSE_TBL_PK").Direction = ParameterDirection.Output;
                var _with13 = objWK.MyDataAdapter;
                _with13.InsertCommand = InsertCmd;
                RecAfct = _with13.Update(dsTran.Tables["dtTran"]);
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

        #endregion "Save Transaction"

        #region " Other Functions"

        public int FindHblPk(string RefNo)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "select h.hbl_exp_tbl_pk from hbl_exp_tbl h where h.hbl_ref_no = '" + RefNo + "' ";
            string HBLPK = null;
            HBLPK = objWF.ExecuteScaler(strSQL);
            return Convert.ToInt32(HBLPK);
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

        #endregion " Other Functions"

        #region " Fetch HBL Report Data"

        public DataSet FetchHBLData(int HBLPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "select JObCardSeaExp.Job_Card_Sea_Exp_Pk JOBPK,";
            strSQL +=  "hbl.hbl_exp_tbl_pk HBPK,";
            strSQL +=  "JobCardSeaExp.Jobcard_Ref_No JOBNO,";
            strSQL +=  "JobCardSeaExp.Ucr_No UCRNO,";
            strSQL +=  "hbl.hbl_ref_no HBNO,";
            strSQL +=  "hbl.vessel_name VES_FLIGHT,";
            strSQL +=  "hbl.voyage voyage,";
            strSQL +=  "POL.PORT_NAME POL,";
            strSQL +=  "POD.PORT_NAME POD,";
            strSQL +=  "PMT.PLACE_NAME PLD,";
            strSQL +=  "CustMstShipper.Customer_Name Shipper,";
            strSQL +=  "CustShipperDtls.Adm_Address_1 ShiAddress1,";
            strSQL +=  "CustShipperDtls.Adm_Address_2 ShiAddress2,";
            strSQL +=  "CustShipperDtls.Adm_Address_3 ShiAddress3,";
            strSQL +=  "CustShipperDtls.Adm_City ShiCity,";
            strSQL +=  "CustMstConsignee.Customer_Name Consignee,";
            strSQL +=  "CustConsigneeDtls.Adm_Address_1 ConsiAddress1,";
            strSQL +=  "CustConsigneeDtls.Adm_Address_2 ConsiAddress2,";
            strSQL +=  "CustConsigneeDtls.Adm_Address_3 ConsiAddress3,";
            strSQL +=  "CustConsigneeDtls.Adm_City ConsiCity,";
            strSQL +=  "AgentMst.Agent_Name,";
            strSQL +=  "AgentDtls.Adm_Address_1 AgtAddress1,";
            strSQL +=  "AgentDtls.Adm_Address_2 AgtAddress2,";
            strSQL +=  "AgentDtls.Adm_Address_3 AgtAddress3,";
            strSQL +=  "AgentDtls.Adm_City AgtCity,";
            strSQL +=  "HBL.GOODS_DESCRIPTION";
            strSQL +=  "from job_card_SEA_exp_tbl JobCardSeaExp,";
            strSQL +=  "hbl_exp_tbl HBL,";
            strSQL +=  " Booking_Sea_Tbl BkgSea,";
            strSQL +=  " Port_Mst_Tbl POL,";
            strSQL +=  " Port_Mst_Tbl POD,";
            strSQL +=  " Place_Mst_Tbl PMT,";
            strSQL +=  "Customer_Mst_Tbl CustMstShipper,";
            strSQL +=  "Customer_Mst_Tbl CustMstConsignee,";
            strSQL +=  " Agent_Mst_Tbl AgentMst,";
            strSQL +=  "Customer_Contact_Dtls CustShipperDtls,";
            strSQL +=  " Customer_Contact_Dtls CustConsigneeDtls,";
            strSQL +=  "  Agent_Contact_Dtls AgentDtls";
            strSQL +=  " where JobCardSeaExp.Job_Card_Sea_Exp_Pk = hbl.job_card_sea_exp_fk";
            strSQL +=  "and   JobCardSeaExp.Booking_Sea_Fk=BkgSea.Booking_Sea_Pk";
            strSQL +=  "and   POL.PORT_MST_PK(+)=BkgSea.Port_Mst_Pol_Fk";
            strSQL +=  "and   POD.PORT_MST_PK(+)=BkgSea.Port_Mst_Pod_Fk";
            strSQL +=  "and   PMT.PLACE_PK(+)=BkgSea.Del_Place_Mst_Fk";
            strSQL +=  "and   HBL.Shipper_Cust_Mst_Fk=CustMstShipper.Customer_Mst_Pk(+)";
            strSQL +=  "and   HBL.Consignee_Cust_Mst_Fk=CustMstConsignee.Customer_Mst_Pk(+)";
            strSQL +=  "and   HBL.Dp_Agent_Mst_Fk=AgentMst.Agent_Mst_Pk(+)";
            strSQL +=  "and   CustMstShipper.Customer_Mst_Pk=CustShipperDtls.Customer_Mst_Fk(+)";
            strSQL +=  "and   CustMstConsignee.Customer_Mst_Pk=CustConsigneeDtls.Customer_Mst_Fk(+)";
            strSQL +=  "and   AgentMst.Agent_Mst_Pk=AgentDtls.Agent_Mst_Fk(+)";
            strSQL +=  "and   hbl.hbl_exp_tbl_pk=" + HBLPK;

            try
            {
                return (objWF.GetDataSet(strSQL));
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

        #endregion " Fetch HBL Report Data"

        #region " save letter of credit"

        public ArrayList saveLetter(string strPk, string Letter, string version, string LcNumber, string LCDate, string LCExpiresOn)
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
                var _with14 = insCommand.Parameters;
                _with14.Add("HBL_EXP_TBL_PK_IN", strPk);
                _with14.Add("VERSION_NO_IN", version);
                // = Version_No + 1
                _with14.Add("LETTER_OF_CREDIT_IN", ifDBNull(Letter));
                _with14.Add("LC_NUMBER_IN", ifDBNull(LcNumber));
                _with14.Add("LC_DATE_IN", ifDBNull(LCDate));
                _with14.Add("LC_EXPIRES_ON_IN", ifDBNull(LCExpiresOn));
                _with14.Add("RETURN_VALUE", OracleDbType.Int32, 10, "HBL_EXP_TBL_PK").Direction = ParameterDirection.Output;
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

        #endregion " save letter of credit"

        #region " Enhance Search & Lookup Search Block FOR HBL"

        public string FetchForJobHblRef(string strCond)
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
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_HBL_REF_NO_PKG.GET_JOB_HBL_REF_COMMON";
                var _with15 = SCM.Parameters;
                _with15.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with15.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with15.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN :"")).Direction = ParameterDirection.Input;
                _with15.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion " Enhance Search & Lookup Search Block FOR HBL"

        // Added By Jitendra For Barcode

        #region "Fetch Barcode Manager Pk"

        public int FetchBarCodeManagerPk(string Configid)
        {
            try
            {
                string StrSql = null;
                DataSet DsBarManager = null;
                int strReturn = 0;

                WorkFlow objWF = new WorkFlow();
                //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"

                StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt" + "where bdmt.config_id_fk= '" + Configid + "'";
                DsBarManager = objWF.GetDataSet(StrSql);
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with16 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToInt32(_with16["bcd_mst_pk"]);
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
                strQuery.Append("   where bdmt.BCD_MST_FK= " + BarCodeManagerPk);
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

        public string FetchBookingNr(string hblRefnr)
        {
            try
            {
                string strQuery = null;
                DataSet DsBarManager = null;
                string strReturn = null;

                WorkFlow objWF = new WorkFlow();
                //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"
                strQuery = "select bok.booking_ref_no from hbl_exp_tbl hbl, job_card_sea_exp_tbl job,booking_sea_tbl bok" + "where hbl.job_card_sea_exp_fk = job.job_card_sea_exp_pk" + "and job.booking_sea_fk = bok.booking_sea_pk" + "and hbl.hbl_ref_no ='" + hblRefnr + "'";

                DsBarManager = objWF.GetDataSet(strQuery);
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with17 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToString(_with17["booking_ref_no"]);
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

        #endregion "Fetch Booking Nr"

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
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
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
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "HBL VALIDATION"

        #region "For Fetching Booking Status"

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

        #endregion "For Fetching Booking Status"

        #region "Get Master JobCard Pk"

        public Int32 GetMJCPK(long PKVal)
        {
            try
            {
                System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
                string MJCPK = null;
                WorkFlow objwf = new WorkFlow();
                //strSQL = " "
                strSQL.Append(" SELECT NVL(JC.MASTER_JC_FK,0) MJCPK ");
                strSQL.Append("  FROM JOB_CARD_TRN JC ");
                strSQL.Append("  WHERE JC.JOB_CARD_TRN_PK =" + PKVal);

                MJCPK = objwf.ExecuteScaler(strSQL.ToString());
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    return Convert.ToInt32(MJCPK);
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

        #endregion "Get Master JobCard Pk"

        #region "Get Customer Details Enhance Search"

        public string FetchForShipperAndConsigneeHBL(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            string businessType = "";
            string Import = "";
            string Consignee = "0";
            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strCATEGORY_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                businessType = Convert.ToString(arr.GetValue(4));
            //new condition added by vimlesh kumar for checking consignee
            //in place of location we need to pass pod pk
            //this condition gives the consignee of pod location.
            if (arr.Length > 5)
                Consignee = "1";
            //added by gopi in import side we need show all shippper
            if (arr.Length > 6)
                Import = Convert.ToString(arr.GetValue(6));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_CATEGORY_HBL";
                var _with18 = SCM.Parameters;
                _with18.Add("CATEGORY_IN", ifDBNull(strCATEGORY_IN)).Direction = ParameterDirection.Input;
                _with18.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with18.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with18.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with18.Add("CONSIGNEE_IN", Consignee).Direction = ParameterDirection.Input;
                _with18.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with18.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion "Get Customer Details Enhance Search"

        //'Vasava For PTS:Oct-006 Credit Control Management

        #region "Fetching JobPk based on HBL"

        public object FetchJobpk(string HblPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            WorkFlow objwf = new WorkFlow();
            strSQL.Append(" SELECT H.HBL_EXP_TBL_PK,");
            strSQL.Append(" JOB.JOB_CARD_SEA_EXP_PK");
            strSQL.Append(" FROM HBL_EXP_TBL H,");
            strSQL.Append(" JOB_CARD_SEA_EXP_TBL JOB");
            strSQL.Append("  WHERE H.HBL_EXP_TBL_PK = JOB.HBL_EXP_TBL_FK");
            strSQL.Append("  AND H.HBL_EXP_TBL_PK =" + HblPK);
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

        #endregion "Fetching JobPk based on HBL"

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

            strSQL.Append(" FROM JOB_TRN_FD  JFD,");
            strSQL.Append(" JOB_CARD_TRN  JOB,");
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

        #endregion "Credit Control"

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

        #endregion "Fetching CreditPolicy Details based on Shipper"

        #region " Fetch Container data export"

        public DataSet FetchContainerDataExp(string jobCardPK = "0", string MJCPK = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append( "SELECT");
                strSQL.Append( "    JOB_TRN_CONT_PK,");
                strSQL.Append( "    container_type_mst_fk,");
                strSQL.Append( "    cont.container_type_mst_id,");
                strSQL.Append( "    container_number,");
                strSQL.Append( "    seal_number,");
                strSQL.Append( "    volume_in_cbm,");
                strSQL.Append( "    gross_weight,");

                strSQL.Append( "     CASE WHEN NET_WEIGHT IS NULL THEN");
                strSQL.Append( "        CHARGEABLE_WEIGHT");
                strSQL.Append( "        ELSE NET_WEIGHT ");
                strSQL.Append( "          END NET_WEIGHT,");
                strSQL.Append( "    CASE WHEN CHARGEABLE_WEIGHT IS NULL THEN");
                strSQL.Append( "          NET_WEIGHT");
                strSQL.Append( "        ELSE CHARGEABLE_WEIGHT");
                strSQL.Append( "         END CHARGEABLE_WEIGHT,");
                //'
                strSQL.Append( "    PACK_TYPE_DESC,");
                strSQL.Append( "    pack_count,");
                strSQL.Append( "     job_exp.commodity_group_fk commodity_mst_fk,");

                strSQL.Append( "    ' ' fetch_comm,");
                strSQL.Append( " (SELECT ROWTOCOL('SELECT DISTINCT JCOMM.COMMODITY_MST_FK");
                strSQL.Append( "          FROM JOB_TRN_COMMODITY JCOMM");
                strSQL.Append( "         WHERE JCOMM.JOB_TRN_CONT_FK =' ||");
                strSQL.Append( "                        JOB_TRN_CONT.JOB_TRN_CONT_PK)");
                strSQL.Append( "          FROM DUAL) COMMODITY_MST_FKS ");
                strSQL.Append( "     , job_trn_cont.CONTAINER_PK CONTAINER_PK, Job_Card_Trn_Pk, '1' SEL ");
                strSQL.Append( "FROM");
                strSQL.Append( "    JOB_TRN_CONT job_trn_cont,");
                strSQL.Append( "    pack_type_mst_tbl pack,");
                strSQL.Append( "    commodity_mst_tbl comm,");
                strSQL.Append( "    container_type_mst_tbl cont,");
                strSQL.Append( "    JOB_CARD_TRN job_exp");
                strSQL.Append( "WHERE ");
                strSQL.Append( "    job_trn_cont.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
                strSQL.Append( "    AND job_trn_cont.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append( "    AND job_trn_cont.commodity_mst_fk = comm.commodity_mst_pk(+)");
                strSQL.Append( "    AND job_trn_cont.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append( "    AND job_exp.Job_Card_Trn_Pk =" + jobCardPK);

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

        #endregion " Fetch Container data export"

        #region "Get Import Job Card Pks "

        public string GetImportJCPKs(string HBLNr)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append( "SELECT ROWTOCOL('");
                strSQL.Append( " SELECT 0 AS JCPK FROM DUAL");
                strSQL.Append( " UNION");
                strSQL.Append( " SELECT JCSIT.JOB_CARD_SEA_IMP_PK AS JCPK FROM JOB_CARD_SEA_IMP_TBL JCSIT");
                strSQL.Append( " WHERE JCSIT.HBL_REF_NO=''" + HBLNr + "'' AND JCSIT.JC_AUTO_MANUAL=1')");
                strSQL.Append( " FROM DUAL");

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

        #endregion "Get Import Job Card Pks "

        #region " Fetch Freight Deatils Export "

        public DataSet FetchFreightDetailsExport(int JobCardPK)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with19 = objWF.MyDataAdapter;
                _with19.SelectCommand = new OracleCommand();
                _with19.SelectCommand.Connection = objWF.MyConnection;
                _with19.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_HBL_HAWB_PKG.FETCH_HBL_HAWB";
                _with19.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with19.SelectCommand.Parameters.Add("JOB_CARD_TRN_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                _with19.SelectCommand.Parameters.Add("LOCATION_MST_PK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with19.SelectCommand.Parameters.Add("GET_DS1", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with19.SelectCommand.Parameters.Add("GET_DS2", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with19.SelectCommand.Parameters.Add("GET_DS3", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with19.Fill(ds);

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

        #endregion " Fetch Freight Deatils Export "

        #region " Fetch Export Captions "

        public DataSet FetchExportCaptions(string CONFIG_ID, int BizType, int CargoType)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with20 = objWF.MyDataAdapter;
                _with20.SelectCommand = new OracleCommand();
                _with20.SelectCommand.Connection = objWF.MyConnection;
                _with20.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_HBL_HAWB_PKG.FETCH_HBL_HAWB_EXCEL_CAPTIONS";
                _with20.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with20.SelectCommand.Parameters.Add("CONFIG_ID_IN", CONFIG_ID).Direction = ParameterDirection.Input;
                _with20.SelectCommand.Parameters.Add("ENVIRONMENT_TBL_FK_IN", HttpContext.Current.Session["ENVIRONMENT_PK"]).Direction = ParameterDirection.Input;
                _with20.SelectCommand.Parameters.Add("BIZ_TYPE_IN", (BizType <= 0 ? 0 : BizType)).Direction = ParameterDirection.Input;
                _with20.SelectCommand.Parameters.Add("CARGO_TYPE_IN", (CargoType <= 0 ? 0 : CargoType)).Direction = ParameterDirection.Input;

                _with20.SelectCommand.Parameters.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with20.Fill(ds);

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

        #endregion " Fetch Export Captions "

        #region " Fetch Route Details"

        public DataSet FetchRouteDetails(int JOBPk = 0, int HBLPK = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            if (HBLPK == 0)
            {
                sb.Append("     SELECT DISTINCT  ");
                sb.Append("       PO2.PORT_ID               AS POL,");
                sb.Append("       PO2.PORT_MST_PK           AS \"POLPK\",");
                sb.Append("       PO1.PORT_MST_PK           AS \"PODPK\",");
                sb.Append("       PO1.PORT_ID               AS POD,");
                sb.Append("       PLD.PLACE_NAME            AS DELPLACE,");
                sb.Append("       PLR.PLACE_NAME            AS RPLACE,");
                sb.Append("       FIRSTVOY.VOYAGE_TRN_PK    \"FIRSTVOYPK\",");
                sb.Append("       FIRSTVSL.VESSEL_NAME,");
                sb.Append("       FIRSTVSL.VESSEL_ID,");
                sb.Append("       FIRSTVOY.VOYAGE,");
                sb.Append("       SECVOY.VOYAGE_TRN_PK      \"SECVOYPK\",");
                sb.Append("       SECVSL.VESSEL_NAME        \"SEC_VESSEL_NAME\",");
                sb.Append("       SECVSL.VESSEL_ID          \"SEC_VESSEL_ID\",");
                sb.Append("       SECVOY.VOYAGE             \"SEC_VOYAGE\",");
                sb.Append("  PO1.PORT_NAME || ',' || POLCONT.COUNTRY_NAME PODCONTY ,");
                sb.Append("                PO2.PORT_NAME || ',' || PODCONT.COUNTRY_NAME POLCONTY,");
                sb.Append("                CASE");
                sb.Append("                  WHEN PLR.PLACE_NAME IS NOT NULL THEN");
                sb.Append("                   PLR.PLACE_NAME || ',' || PLRCONT.COUNTRY_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   ''");
                sb.Append("                END PLRCONTY,");
                sb.Append("                CASE");
                sb.Append("                  WHEN PLD.PLACE_NAME IS NOT NULL THEN");
                sb.Append("                   PLD.PLACE_NAME || ',' || PFDCONT.COUNTRY_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   ''");
                sb.Append("                END PLDCONTY ");
                sb.Append("    FROM BOOKING_MST_TBL        BOOK,");
                sb.Append("       JOB_CARD_TRN   JOB,");
                sb.Append("       PLACE_MST_TBL          PLR,");
                sb.Append("       PLACE_MST_TBL          PLD,");
                sb.Append("       PORT_MST_TBL           PO1,");
                sb.Append("       PORT_MST_TBL           PO2,");
                sb.Append("       VESSEL_VOYAGE_TBL      FIRSTVSL,");
                sb.Append("       VESSEL_VOYAGE_TBL      SECVSL,");
                sb.Append("       VESSEL_VOYAGE_TRN      FIRSTVOY,");
                sb.Append("       VESSEL_VOYAGE_TRN      SECVOY,");
                sb.Append("       COUNTRY_MST_TBL        PLRCONT,");
                sb.Append("       COUNTRY_MST_TBL        POLCONT,");
                sb.Append("       COUNTRY_MST_TBL        PODCONT,");
                sb.Append("       COUNTRY_MST_TBL        PFDCONT,");
                sb.Append("       LOCATION_MST_TBL       LMTPLR,");
                sb.Append("       LOCATION_MST_TBL       LMTPFD");
                sb.Append("   WHERE ");
                sb.Append("   AND BOOK.PORT_MST_POD_FK(+) = PO1.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = PO2.PORT_MST_PK");
                sb.Append("   AND BOOK.COL_PLACE_MST_FK = PLR.PLACE_PK(+)");
                sb.Append("   AND BOOK.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                sb.Append("   AND JOB.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
                sb.Append("   AND FIRSTVOY.VESSEL_VOYAGE_TBL_FK = FIRSTVSL.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND SECVOY.VESSEL_VOYAGE_TBL_FK = SECVSL.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JOB.VOYAGE_TRN_FK = FIRSTVOY.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JOB.SEC_VOYAGE_TRN_FK = SECVOY.VOYAGE_TRN_PK(+)");
                sb.Append("   AND PO1.COUNTRY_MST_FK = POLCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PO2.COUNTRY_MST_FK = PODCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PLR.LOCATION_MST_FK = LMTPLR.LOCATION_MST_PK(+)");
                sb.Append("   AND LMTPLR.COUNTRY_MST_FK = PLRCONT.COUNTRY_MST_PK(+)");
                sb.Append("   AND PLD.LOCATION_MST_FK = LMTPFD.LOCATION_MST_PK(+)");
                sb.Append("   AND LMTPFD.COUNTRY_MST_FK = PFDCONT.COUNTRY_MST_PK(+)");
                if (Convert.ToString(JOBPk) != "0")
                {
                    sb.Append("and JOB.JOB_CARD_TRN_PK=" + JOBPk);
                }
            }
            else
            {
                sb.Append(" SELECT DISTINCT HBL.VESSEL_NAME, ");
                sb.Append(" HBL.VESSEL_ID,");
                sb.Append(" HBL.VOYAGE,");
                sb.Append(" HBL.POD_COUNTRY PODCONTY,");
                sb.Append(" HBL.POL_COUNTRY POLCONTY,");
                sb.Append(" HBL.PLR_COUNTRY PLRCONTY,");
                sb.Append(" HBL.PFD_COUNTRY PLDCONTY ");
                sb.Append(" FROM JOB_CARD_TRN JOB,HBL_EXP_TBL HBL ");
                sb.Append(" WHERE HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK ");
                sb.Append(" AND HBL.HBL_EXP_TBL_PK = " + HBLPK);
            }
            try
            {
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion " Fetch Route Details"

        #region " Save Container"

        public ArrayList saveJCContainer(DataTable dtMain)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            Int16 RecAfect = default(Int16);
            try
            {
                //*****Save JC Container******
                int ContainerPk = 0;
                foreach (DataRow _row in dtMain.Rows)
                {
                    ContainerPk = 0;
                    try
                    {
                        ContainerPk = Convert.ToInt32(_row["JOB_TRN_CONT_PK"]);
                    }
                    catch (Exception ex)
                    {
                    }
                    if (ContainerPk > 0)
                    {
                        var _with21 = objWK.MyCommand;
                        _with21.CommandType = CommandType.StoredProcedure;
                        _with21.Parameters.Clear();
                        _with21.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_CONT_UPD_FROM_HBL";

                        _with21.Parameters.Add("JOB_TRN_CONT_PK_IN", _row["JOB_TRN_CONT_PK"]).Direction = ParameterDirection.Input;
                        _with21.Parameters.Add("JOB_CARD_TRN_FK_IN", _row["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with21.Parameters.Add("VOLUME_IN_CBM_IN", _row["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                        if (string.IsNullOrEmpty(_row["GROSS_WEIGHT"].ToString()))
                        {
                            _with21.Parameters.Add("GROSS_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with21.Parameters.Add("GROSS_WEIGHT_IN", Convert.ToDouble(_row["GROSS_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(_row["CHARGEABLE_WEIGHT"].ToString()))
                        {
                            _with21.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with21.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(_row["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        _with21.Parameters.Add("PACK_COUNT_IN", _row["PACK_COUNT"]).Direction = ParameterDirection.Input;
                        _with21.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                        _with21.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150).Direction = ParameterDirection.Output;
                        RecAfect = Convert.ToInt16(_with21.ExecuteNonQuery());
                    }
                }
                TRAN.Commit();
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
                //*****************************
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
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion " Save Container"

        #region "Get HBL Details"

        public DataSet HBLDetails(string HblRefNr, int BizType)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (BizType == 2)
            {
                sb.Append("SELECT HBL.HBL_EXP_TBL_PK,");
                sb.Append("       HBL.HBL_DATE,");
                sb.Append("       HBL.PLACE_ISSUE,");
                sb.Append("       JC.JOB_CARD_TRN_PK,");
                sb.Append("       JC.JOBCARD_REF_NO,");
                sb.Append("       JC.PORT_MST_POL_FK,");
                sb.Append("       JC.PORT_MST_POD_FK,DECODE(HBL.PYMT_TYPE,1,'Prepaid',2,'Collect')PAYTYPE,");
                sb.Append("        DECODE(JC.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC')CARGOTYPE,HBL.IS_TO_ORDER, HBL.CARGO_MOVE ");
                sb.Append("  FROM HBL_EXP_TBL HBL, JOB_CARD_TRN JC");
                sb.Append(" WHERE JC.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append(" AND HBL.HBL_REF_NO='" + HblRefNr + "'");
            }
            else
            {
                sb.Append("SELECT HAWB.HAWB_EXP_TBL_PK,");
                sb.Append("       HAWB.HAWB_DATE,");
                sb.Append("       HAWB.PLACE_ISSUE,");
                sb.Append("       JC.JOB_CARD_TRN_PK,");
                sb.Append("       JC.JOBCARD_REF_NO,");
                sb.Append("       JC.PORT_MST_POL_FK,");
                sb.Append("       JC.PORT_MST_POD_FK,DECODE(HAWB.PYMT_TYPE,1,'Prepaid',2,'Collect')PAYTYPE ");
                sb.Append("  FROM HAWB_EXP_TBL HAWB, JOB_CARD_TRN JC");
                sb.Append(" WHERE JC.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append(" AND HAWB.HAWB_REF_NO='" + HblRefNr + "'");
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

        public DataSet MBLDetails(string MBLPK, int BizType)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (BizType == 2)
            {
                sb.Append("SELECT MBL.MBL_EXP_TBL_PK,");
                sb.Append("       MBL.MBL_REF_NO,");
                sb.Append("       MBL.MBL_DATE,");
                sb.Append("       JC.PORT_MST_POL_FK,");
                sb.Append("       JC.PORT_MST_POD_FK,NVL(JC.DP_AGENT_MST_FK,0)DP_AGENT_MST_FK ");
                sb.Append("  FROM MBL_EXP_TBL MBL, JOB_CARD_TRN JC");
                sb.Append(" WHERE JC.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK");
                sb.Append(" AND MBL.MBL_EXP_TBL_PK=" + MBLPK);
            }
            else
            {
                sb.Append("SELECT MAWB.MAWB_EXP_TBL_PK,");
                sb.Append("       MAWB.MAWB_REF_NO,");
                sb.Append("       MAWB.MAWB_DATE,");
                sb.Append("       JC.PORT_MST_POL_FK,");
                sb.Append("       JC.PORT_MST_POD_FK,NVL(JC.DP_AGENT_MST_FK,0)DP_AGENT_MST_FK ");
                sb.Append("  FROM MAWB_EXP_TBL MAWB, JOB_CARD_TRN JC");
                sb.Append(" WHERE JC.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK");
                sb.Append(" AND MAWB.MAWB_EXP_TBL_PK=" + MBLPK);
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

        #endregion "Get HBL Details"

        #region "Get Mail Details"

        public DataSet GetMsgDetails()
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with22 = objWF.MyDataAdapter;
                _with22.SelectCommand = new OracleCommand();
                _with22.SelectCommand.Connection = objWF.MyConnection;
                _with22.SelectCommand.CommandText = objWF.MyUserName + ".EMAIL_STANDARD_PROTOCOL_PKG.FETCH_MAIL_DTLS";
                _with22.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with22.SelectCommand.Parameters.Add("DOC_ID_IN", "DOC000589").Direction = ParameterDirection.Input;
                _with22.SelectCommand.Parameters.Add("DOC_OUT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with22.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion "Get Mail Details"

        #region "Get Consignee Details"

        public DataSet GetConsigneeDetails(int ConsigneePK)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with23 = objWF.MyDataAdapter;
                _with23.SelectCommand = new OracleCommand();
                _with23.SelectCommand.Connection = objWF.MyConnection;
                _with23.SelectCommand.CommandText = objWF.MyUserName + ".EMAIL_STANDARD_PROTOCOL_PKG.FETCH_CONSIGNEE_DATA";
                _with23.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with23.SelectCommand.Parameters.Add("CUST_MST_PK_IN", ConsigneePK).Direction = ParameterDirection.Input;
                _with23.SelectCommand.Parameters.Add("CON_OUT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with23.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion "Get Consignee Details"

        #region "Get DP Agent Details"

        public DataSet GetDPAgentDetails(int DPAgentPK)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with24 = objWF.MyDataAdapter;
                _with24.SelectCommand = new OracleCommand();
                _with24.SelectCommand.Connection = objWF.MyConnection;
                _with24.SelectCommand.CommandText = objWF.MyUserName + ".EMAIL_STANDARD_PROTOCOL_PKG.FETCH_DPAGENT_DATA";
                _with24.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with24.SelectCommand.Parameters.Add("AGENT_MST_PK_IN", DPAgentPK).Direction = ParameterDirection.Input;
                _with24.SelectCommand.Parameters.Add("AGENT_OUT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with24.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion "Get DP Agent Details"
    }
}