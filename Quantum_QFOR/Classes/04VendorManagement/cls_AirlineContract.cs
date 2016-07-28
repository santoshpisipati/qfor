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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_AirlineContract : CommonFeatures
    {
        /// <summary>
        /// The _ pk value
        /// </summary>
        private long _PkValue;

        #region "Property"

        /// <summary>
        /// Gets the pk value.
        /// </summary>
        /// <value>
        /// The pk value.
        /// </value>
        public long PkValue
        {
            get { return _PkValue; }
        }

        #endregion "Property"

        #region "Fetch function for creating New Airline Tariff Entry "

        /// <summary>
        /// Fetches the header new.
        /// </summary>
        /// <param name="strPolPk">The string pol pk.</param>
        /// <param name="strPodPk">The string pod pk.</param>
        /// <param name="Mode">The mode.</param>
        /// <param name="strTFDate">The string tf date.</param>
        /// <param name="airlinepk">The airlinepk.</param>
        /// <param name="strToDate">The string to date.</param>
        /// <returns></returns>
        public DataTable FetchHeaderNew(string strPolPk, string strPodPk, string Mode, string strTFDate = "", string airlinepk = "0", string strToDate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = null;
            DataTable dtKgs = null;
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = "";
            string strNewModeCondition = null;
            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');
            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }
            if (!(Mode == "EDIT"))
            {
                strNewModeCondition = " AND POL.BUSINESS_TYPE = 1";
                strNewModeCondition += " AND POD.BUSINESS_TYPE = 1";
            }
            str = " SELECT 0 PK,POL.PORT_MST_PK AS \"POLPK\",POL.PORT_ID AS \"POL\",POL.PORT_NAME AS \"POL_NAME\",";
            str += " POD.PORT_MST_PK AS \"PODPK\",POD.PORT_ID AS \"POD\",POD.PORT_NAME AS \"POD_NAME\",";
            str += " '" + strTFDate + "' AS \"Valid From\",'" + strToDate + "' AS \"Valid To\"";
            str += " FROM PORT_MST_TBL POL, PORT_MST_TBL POD";
            str += " WHERE (1=1)";
            str += " AND (";
            str += strCondition + ")";
            str += strNewModeCondition;
            str += " GROUP BY POL.PORT_ID,POL.PORT_NAME,POL.PORT_MST_PK,POD.PORT_ID,POD.PORT_NAME,POD.PORT_MST_PK";
            str += " HAVING POL.PORT_ID<>POD.PORT_ID";
            str += " ORDER BY POL.PORT_ID";
            try
            {
                dtMain = objWF.GetDataTable(str);

                dtKgs = FetchKgFreightNew(Mode, airlinepk);

                for (i = 0; i <= dtKgs.Rows.Count - 1; i++)
                {
                    dtMain.Columns.Add(dtKgs.Rows[i][0].ToString(), typeof(decimal));
                    dtMain.Columns.Add(dtKgs.Rows[i][1].ToString(), typeof(decimal));
                    dtMain.Columns.Add(dtKgs.Rows[i][2].ToString(), typeof(decimal));
                }
                TransferAirlineFreightData(dtMain, dtKgs);
                dtMain.Columns.Add("Other_Charges");
                dtMain.Columns.Add("OtherCharges");
                dtMain.Columns.Add("Routing");
                return dtMain;
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

        /// <summary>
        /// Fetches the freight new.
        /// </summary>
        /// <param name="strPolPk">The string pol pk.</param>
        /// <param name="strPodPk">The string pod pk.</param>
        /// <param name="Mode">The mode.</param>
        /// <returns></returns>
        public DataTable FetchFreightNew(string strPolPk, string strPodPk, string Mode)
        {
            System.Text.StringBuilder str = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = null;
            DataTable dtSlabs = null;
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = "";
            string strNewModeCondition = null;
            //Spliting the POL and POD Pk's
            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');

            //Making condition as the record should have only selected POL and POD
            //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
            //is the selected sector.
            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }

            if (!(Mode == "EDIT"))
            {
                strNewModeCondition = " AND POL.BUSINESS_TYPE = 1";
                //BUSINESS_TYPE = 1 :- Is the business type for Air
                strNewModeCondition += " AND POD.BUSINESS_TYPE = 1";
                //BUSINESS_TYPE = 1 :- Is the business type for Air
                strNewModeCondition += " AND FMT.ACTIVE_FLAG = 1";
                strNewModeCondition += " AND FMT.charge_Type=1";
                strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (1,3)";
                strNewModeCondition += " AND FMT.BY_DEFAULT=1";
            }

            //Making query with the condition added
            //modified by thiyagarajan on 24/11/08 for location based currency task
            str.Append(" SELECT 0 FK, 0 PK,POL.PORT_MST_PK \"POLPK\",POD.PORT_MST_PK \"PODPK\", ");
            str.Append(" FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.FREIGHT_ELEMENT_NAME,");
            str.Append("  NVL((SELECT DISTINCT CY.CURRENCY_MST_PK ");
            str.Append("  FROM FREIGHT_CONFIG_TRN_TBL FT, CURRENCY_TYPE_MST_TBL CY ");
            str.Append("  WHERE CY.CURRENCY_MST_PK = FT.CURRENCY_MST_FK ");
            str.Append("  AND FT.FREIGHT_ELEMENT_FK= FMT.FREIGHT_ELEMENT_MST_PK),CURR.CURRENCY_MST_PK) CURRENCY_MST_PK, ");
            str.Append("  0 \"MINIMUM_RATE\" ");
            str.Append(" FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,");
            str.Append(" CURRENCY_TYPE_MST_TBL CURR");
            str.Append(" WHERE (1=1)");
            str.Append(" AND (");
            str.Append(strCondition + ")");
            str.Append(strNewModeCondition);
            str.Append(" AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
            str.Append(" GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,");
            str.Append(" FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.FREIGHT_ELEMENT_NAME,");
            str.Append(" CURR.CURRENCY_MST_PK");
            str.Append(" HAVING POL.PORT_ID <> POD.PORT_ID");
            str.Append(" ORDER BY FMT.FREIGHT_ELEMENT_ID");

            try
            {
                dtMain = objWF.GetDataTable(str.ToString());

                dtSlabs = FetchAirFreightSlabsNew(Mode);

                for (i = 0; i <= dtSlabs.Rows.Count - 1; i++)
                {
                    //Current Rate-freight pk
                    dtMain.Columns.Add(dtSlabs.Rows[i][0].ToString(), typeof(decimal));
                    //Approved rate Rate -freight id
                    dtMain.Columns.Add(dtSlabs.Rows[i][1].ToString(), typeof(decimal));
                    //Request Ratename
                    dtMain.Columns.Add("'" + dtSlabs.Rows[i][2].ToString() + "'", typeof(decimal));
                }
                return dtMain;
            }
            catch (Exception SQLEX)
            {
                throw SQLEX;
            }
        }

        /// <summary>
        /// Fetches the kg freight new.
        /// </summary>
        /// <param name="Mode">The mode.</param>
        /// <param name="airlinePK">The airline pk.</param>
        /// <returns></returns>
        public DataTable FetchKgFreightNew(string Mode, string airlinePK = "0")
        {
            System.Text.StringBuilder sqlBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strNewModeCondition = null;

            if (string.IsNullOrEmpty(airlinePK))
            {
                sqlBuilder.Append(" SELECT FR.FREIGHT_ELEMENT_MST_PK,");
                sqlBuilder.Append("     FR.FREIGHT_ELEMENT_ID || '(' ||");
                sqlBuilder.Append("     decode(FR.CHARGE_BASIS, 1, '%', 2, 'Flat', 3, 'Kgs',4,'Unit') || ')',");
                sqlBuilder.Append("     FR.FREIGHT_ELEMENT_NAME,");
                sqlBuilder.Append("     FR.basis_value");
                sqlBuilder.Append(" FROM FREIGHT_ELEMENT_MST_TBL FR");
                sqlBuilder.Append(" WHERE FR.charge_Type = 2");
                sqlBuilder.Append("     and fr.active_flag = 1");
                sqlBuilder.Append("     and fr.business_type = 1");
                sqlBuilder.Append(" ORDER BY FR.FREIGHT_ELEMENT_ID");
            }
            else
            {
                sqlBuilder.Append("SELECT FR.FREIGHT_ELEMENT_MST_PK,");
                sqlBuilder.Append("       FR.FREIGHT_ELEMENT_ID || '(' || decode(SUR.CHARGE_BASIS,1,'%',2,'Flat',3,'Kgs',4,'Unit') || ')',");
                sqlBuilder.Append("       FR.FREIGHT_ELEMENT_NAME,sur.basis_value");
                sqlBuilder.Append("  FROM FREIGHT_ELEMENT_MST_TBL FR,airline_mst_tbl airline,airline_surcharges_tbl sur");
                sqlBuilder.Append(" WHERE FR.charge_Type = 2");
                sqlBuilder.Append("     and fr.active_flag = 1");
                sqlBuilder.Append("   and airline.airline_mst_pk =" + airlinePK);
                sqlBuilder.Append("   and sur.freight_element_mst_fk = fr.freight_element_mst_pk");
                sqlBuilder.Append("   AND SUR.AIRLINE_MST_FK = airline.airline_mst_pk");
                sqlBuilder.Append(" ORDER BY FR.FREIGHT_ELEMENT_ID");
            }

            try
            {
                return objWF.GetDataTable(sqlBuilder.ToString());
            }
            catch (Exception SQLEX)
            {
                throw SQLEX;
            }
        }

        /// <summary>
        /// Fetches the air freight slabs new.
        /// </summary>
        /// <param name="Mode">The mode.</param>
        /// <returns></returns>
        public DataTable FetchAirFreightSlabsNew(string Mode)
        {
            System.Text.StringBuilder sqlBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strNewModeCondition = null;

            if (!(Mode == "EDIT"))
            {
                strNewModeCondition = "WHERE AIR.ACTIVE_FLAG =1";
            }

            sqlBuilder.Append("SELECT AIR.AIRFREIGHT_SLABS_TBL_PK, AIR.BREAKPOINT_ID,AIR.BREAKPOINT_DESC ");
            // sqlBuilder.Append("to_number(null) current_rate,to_number(null) request_rate,to_number(null) approved_rate")
            sqlBuilder.Append(" FROM AIRFREIGHT_SLABS_TBL AIR ");
            sqlBuilder.Append(strNewModeCondition);
            sqlBuilder.Append("ORDER BY AIR.SEQUENCE_NO");

            try
            {
                return objWF.GetDataTable(sqlBuilder.ToString());
            }
            catch (Exception SQLEX)
            {
                throw SQLEX;
            }
        }

        #region " Transfer Data [ kg ] "

        /// <summary>
        /// Transfers the airline freight data.
        /// </summary>
        /// <param name="GridDt">The grid dt.</param>
        /// <param name="SlabDt">The slab dt.</param>
        private void TransferAirlineFreightData(DataTable GridDt, DataTable SlabDt)
        {
            Int16 GRowCnt = default(Int16);
            Int16 SRowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            string frtpk = null;
            string Flat = "10";
            DataRow[] drArray = null;

            for (SRowCnt = 0; SRowCnt <= GridDt.Rows.Count - 1; SRowCnt++)
            {
                for (ColCnt = 9; ColCnt <= GridDt.Columns.Count - 1; ColCnt += 3)
                {
                    drArray = SlabDt.Select("FREIGHT_ELEMENT_MST_PK = " + GridDt.Columns[ColCnt].Caption);

                    if (Convert.ToString(drArray[0]["FREIGHT_ELEMENT_MST_PK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                    {
                        GridDt.Rows[SRowCnt][ColCnt] = drArray[0]["basis_value"];
                        GridDt.Rows[SRowCnt][ColCnt + 2] = drArray[0]["basis_value"];
                        GridDt.Rows[SRowCnt][ColCnt + 1] = drArray[0]["basis_value"];
                    }
                }
            }
        }

        #endregion " Transfer Data [ kg ] "

        #endregion "Fetch function for creating New Airline Tariff Entry "

        #region " Fetch Data from aircontract table "

        #region "FetchMainData"

        /// <summary>
        /// Fetches the one.
        /// </summary>
        /// <param name="GridDS">The grid ds.</param>
        /// <param name="contPk">The cont pk.</param>
        /// <returns></returns>
        public DataSet FetchOne(DataSet GridDS, string contPk = "")
        {
            try
            {
                string ContainerPKs = null;
                bool NewRecord = true;
                //If rfqSpotRatePK.Trim.Length > 0 Then NewRecord = False

                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                DataSet ds = null;
                DataTable otherCharges = new DataTable();
                GridDS.Tables.Clear();
                // Now GridDS is Empty

                strSQL = "SELECT MAIN_CONT.CONT_MAIN_AIR_PK," + "      MAIN_CONT.AIRLINE_MST_FK, " + "      AIRLINE.AIRLINE_ID," + "      AIRLINE.AIRLINE_NAME," + "      RFQ.RFQ_REF_NO," + "      RFQ.RFQ_MAIN_AIR_PK," + "      MAIN_CONT.CONTRACT_NO," + "      MAIN_CONT.CONTRACT_DATE," + "      MAIN_CONT.COMMODITY_GROUP_FK," + "      TO_CHAR(MAIN_CONT.VALID_FROM,DATEFORMAT) VALID_FROM ," + "      TO_CHAR(MAIN_CONT.VALID_TO,DATEFORMAT) VALID_TO," + "      MAIN_CONT.ACTIVE," + "      MAIN_CONT.CONT_APPROVED," + "      MAIN_CONT.APPROVED_BY APPROVED_BY_FK," + "      USER_MST.USER_NAME APPROVED_BY_NAME, " + "      MAIN_CONT.APPROVED_DATE APPROVED_DATE,MAIN_CONT.VERSION_NO,MAIN_CONT.WORKFLOW_STATUS " + "FROM  CONT_MAIN_AIR_TBL MAIN_CONT," + "      AIRLINE_MST_TBL AIRLINE," + "      RFQ_MAIN_AIR_TBL RFQ," + "      COMMODITY_GROUP_MST_TBL COMM," + "      USER_MST_TBL USER_MST" + "WHERE" + "      MAIN_CONT.CONT_MAIN_AIR_PK = " + contPk + "      AND MAIN_CONT.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK(+)" + "      AND MAIN_CONT.RFQ_MAIN_AIR_FK = RFQ.RFQ_MAIN_AIR_PK(+)" + "      AND MAIN_CONT.APPROVED_BY = USER_MST.USER_MST_PK(+)" + "      AND MAIN_CONT.AIRLINE_MST_FK = AIRLINE.AIRLINE_MST_PK(+)";

                ds = objWF.GetDataSet(strSQL);

                // Child Record :==========[ POL and POD ]====

                strSQL = "    Select  main.cont_trn_air_pk,                                 " + "       main.PORT_MST_POL_FK,                                       " + "       PORTPOL.PORT_ID                             PORTPOL_ID,     " + "       PORTPOL.PORT_NAME                           PORTPOL_NAME,   " + "       main.PORT_MST_POD_FK,                                       " + "       PORTPOD.PORT_ID                             PORTPOD_ID,     " + "       PORTPOD.PORT_NAME                           PORTPOD_NAME,   " + "       TO_CHAR(VALID_FROM,DATEFORMAT) VALID_FROM,                                                 " + "       TO_CHAR(VALID_TO,DATEFORMAT) VALID_TO                                             " + "      from                                                         " + "       CONT_TRN_AIR_LCL                main,                       " + "       PORT_MST_TBL                    PORTPOL,                    " + "       PORT_MST_TBL                    PORTPOD                     " + "      where  CONT_MAIN_AIR_FK      = " + contPk + "                " + "       AND PORT_MST_POL_FK         =   PORTPOL.PORT_MST_PK         " + "       AND PORT_MST_POD_FK         =   PORTPOD.PORT_MST_PK         ";

                DataTable ChildDt = null;
                ChildDt = objWF.GetDataTable(strSQL);

                // Adding KGFreights Columns [ Charge Basis = 3 ]
                DataTable KGFrtDt = null;

                strSQL = "Select" + "      main.cont_trn_air_pk," + "      tran.cont_air_surcharge_pk,          " + "      tran.FREIGHT_ELEMENT_MST_FK,      " + "      FREIGHT_ELEMENT_ID || '(' || decode(tran.charge_basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') || ')' FREIGHT_ELEMENT_ID ," + "      FREIGHT_ELEMENT_NAME,       " + "      tran.CURRENCY_MST_FK,       " + "      CURRENCY_ID,        " + "      CURRENCY_NAME,        " + "      tran.current_rate,   " + "      tran.request_rate,   " + "      tran.approved_rate   " + "from          " + "      CONT_TRN_AIR_LCL  main,      " + "      CONT_AIR_SURCHARGE  tran,     " + "      FREIGHT_ELEMENT_MST_TBL,      " + "      CURRENCY_TYPE_MST_TBL       " + "where CONT_MAIN_AIR_FK  = " + contPk + "      AND  CONT_TRN_AIR_FK  = main.CONT_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID  ";

                KGFrtDt = objWF.GetDataTable(strSQL);
                string KGFreights = null;

                KGFreights = getStrFreights(KGFrtDt);

                AddColumnsFreights(ChildDt, KGFreights);
                // KGFreights Columns

                TransferKGFreightsData(ChildDt, KGFrtDt);
                //    KGFreights Data in ChildDt now..

                ChildDt.Columns.Add("Other_Charges");
                ChildDt.Columns.Add("OtherCharges");
                ChildDt.Columns.Add("Routing");

                // Other Charge Information need to be fetched======================================Importent
                // Child Records FOR Freights..>

                strSQL = "Select" + "      tran.cont_trn_air_fk," + "      frt.freight_element_mst_pk," + "      curr.currency_mst_pk," + "      tran.charge_basis," + "      tran.current_rate," + "      tran.request_rate," + "      tran.approved_rate" + "from" + "      CONT_TRN_AIR_LCL  main,      " + "      CONT_AIR_OTH_CHRG  tran,     " + "      FREIGHT_ELEMENT_MST_TBL frt, " + "      CURRENCY_TYPE_MST_TBL curr      " + "where CONT_MAIN_AIR_FK  = " + contPk + "      AND  CONT_TRN_AIR_FK  = main.CONT_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID ";

                otherCharges = objWF.GetDataTable(strSQL);

                TransferOtherFreightsData(ChildDt, otherCharges);

                strSQL = "Select" + "      tran.cont_trn_air_fk," + "      tran.cont_air_freight_pk,           " + "      main.port_mst_pol_fk,           " + "      main.port_mst_pod_fk,           " + "      FREIGHT_ELEMENT_MST_FK,       " + "      FREIGHT_ELEMENT_ID,        " + "      FREIGHT_ELEMENT_NAME,       " + "      tran.CURRENCY_MST_FK,              " + "      MIN_AMOUNT " + "FROM" + "      CONT_TRN_AIR_LCL     main,   " + "      CONT_AIR_FREIGHT_TBL    tran,   " + "      FREIGHT_ELEMENT_MST_TBL,      " + "      CURRENCY_TYPE_MST_TBL" + "WHERE CONT_MAIN_AIR_FK = " + contPk + "      AND  CONT_TRN_AIR_FK  = main.CONT_TRN_AIR_PK " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "      Order By FREIGHT_ELEMENT_ID    ";

                DataTable GRFrtDt = null;
                GRFrtDt = objWF.GetDataTable(strSQL);

                strSQL = "Select" + "      AIRFREIGHT_SLABS_TBL_PK," + "      bpnt.cont_air_freight_fk,               " + "      slab.breakpoint_id,        " + "      BREAKPOINT_DESC,        " + "      CURRENT_RATE,   " + "      REQUEST_RATE,   " + "      APPROVED_RATE" + "FROM" + "      CONT_TRN_AIR_LCL    main,    " + "      CONT_AIR_FREIGHT_TBL   tran,    " + "      CONT_AIR_BREAKPOINTS   bpnt,    " + "      AIRFREIGHT_SLABS_TBL slab" + "where CONT_MAIN_AIR_FK = " + contPk + "      AND tran.CONT_TRN_AIR_FK  = main.CONT_TRN_AIR_PK  " + "      AND bpnt.CONT_AIR_FREIGHT_FK  = tran.CONT_AIR_FREIGHT_PK   " + "      AND bpnt.AIRFREIGHT_SLABS_FK  = AIRFREIGHT_SLABS_TBL_PK  " + "Order By SEQUENCE_NO";

                DataTable SlbDt = null;
                SlbDt = objWF.GetDataTable(strSQL);

                string AirSlabs = null;
                AirSlabs = getStrSlabs(SlbDt);
                AddColumnsSlabs(GRFrtDt, AirSlabs);
                // Slabs Columns
                TransferSlabsData(GRFrtDt, SlbDt);
                //

                GridDS.Tables.Add(ChildDt);
                GridDS.Tables.Add(GRFrtDt);
                DataRelation REL = new DataRelation("RFQRelation", new DataColumn[] { GridDS.Tables[0].Columns["cont_trn_air_pk"] }, new DataColumn[] { GridDS.Tables[1].Columns["cont_trn_air_fk"] });
                GridDS.Relations.Add(REL);
                return ds;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchMainData"

        //'Vasava For Fetching the data for Fetching Existing Freight Elements and New Frieght ELements while Amending

        #region "FetchAmendData"

        /// <summary>
        /// Fetches the one all.
        /// </summary>
        /// <param name="strPolPk">The string pol pk.</param>
        /// <param name="strPodPk">The string pod pk.</param>
        /// <param name="GridDS">The grid ds.</param>
        /// <param name="contPk">The cont pk.</param>
        /// <returns></returns>
        public DataSet FetchOneAll(string strPolPk, string strPodPk, DataSet GridDS, string contPk = "")
        {
            try
            {
                string ContainerPKs = null;
                bool NewRecord = true;
                //If rfqSpotRatePK.Trim.Length > 0 Then NewRecord = False

                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                DataSet ds = null;
                Int16 i = default(Int16);
                DataTable otherCharges = new DataTable();
                Array arrPolPk = null;
                Array arrPodPk = null;
                string strCondition = null;
                string strNewModeCondition = null;
                //Spliting the POL and POD Pk's
                arrPolPk = strPolPk.Split(',');
                arrPodPk = strPodPk.Split(',');

                GridDS.Tables.Clear();
                // Now GridDS is Empty

                strSQL = "SELECT MAIN_CONT.CONT_MAIN_AIR_PK," + "      MAIN_CONT.AIRLINE_MST_FK, " + "      AIRLINE.AIRLINE_ID," + "      AIRLINE.AIRLINE_NAME," + "      RFQ.RFQ_REF_NO," + "      RFQ.RFQ_MAIN_AIR_PK," + "      MAIN_CONT.CONTRACT_NO," + "      MAIN_CONT.CONTRACT_DATE," + "      MAIN_CONT.COMMODITY_GROUP_FK," + "      TO_CHAR(MAIN_CONT.VALID_FROM,DATEFORMAT) VALID_FROM ," + "      TO_CHAR(MAIN_CONT.VALID_TO,DATEFORMAT) VALID_TO," + "      MAIN_CONT.ACTIVE," + "      MAIN_CONT.CONT_APPROVED," + "      MAIN_CONT.APPROVED_BY APPROVED_BY_FK," + "      USER_MST.USER_NAME APPROVED_BY_NAME, " + "      MAIN_CONT.APPROVED_DATE APPROVED_DATE,MAIN_CONT.VERSION_NO,MAIN_CONT.WORKFLOW_STATUS " + "FROM  CONT_MAIN_AIR_TBL MAIN_CONT," + "      AIRLINE_MST_TBL AIRLINE," + "      RFQ_MAIN_AIR_TBL RFQ," + "      COMMODITY_GROUP_MST_TBL COMM," + "      USER_MST_TBL USER_MST" + "WHERE" + "      MAIN_CONT.CONT_MAIN_AIR_PK = " + contPk + "      AND MAIN_CONT.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK(+)" + "      AND MAIN_CONT.RFQ_MAIN_AIR_FK = RFQ.RFQ_MAIN_AIR_PK(+)" + "      AND MAIN_CONT.APPROVED_BY = USER_MST.USER_MST_PK(+)" + "      AND MAIN_CONT.AIRLINE_MST_FK = AIRLINE.AIRLINE_MST_PK(+)";

                ds = objWF.GetDataSet(strSQL);

                // Child Record :==========[ POL and POD ]====

                strSQL = "    Select  main.cont_trn_air_pk,                                 " + "       main.PORT_MST_POL_FK,                                       " + "       PORTPOL.PORT_ID                             PORTPOL_ID,     " + "       PORTPOL.PORT_NAME                           PORTPOL_NAME,   " + "       main.PORT_MST_POD_FK,                                       " + "       PORTPOD.PORT_ID                             PORTPOD_ID,     " + "       PORTPOD.PORT_NAME                           PORTPOD_NAME,   " + "       TO_CHAR(VALID_FROM,DATEFORMAT) VALID_FROM,                                                 " + "       TO_CHAR(VALID_TO,DATEFORMAT) VALID_TO                                             " + "      from                                                         " + "       CONT_TRN_AIR_LCL                main,                       " + "       PORT_MST_TBL                    PORTPOL,                    " + "       PORT_MST_TBL                    PORTPOD                     " + "      where  CONT_MAIN_AIR_FK      = " + contPk + "                " + "       AND PORT_MST_POL_FK         =   PORTPOL.PORT_MST_PK         " + "       AND PORT_MST_POD_FK         =   PORTPOD.PORT_MST_PK         ";

                DataTable ChildDt = null;
                ChildDt = objWF.GetDataTable(strSQL);

                // Adding KGFreights Columns [ Charge Basis = 3 ]
                DataTable KGFrtDt = null;

                strSQL = "Select" + "      main.cont_trn_air_pk," + "      tran.cont_air_surcharge_pk,          " + "      tran.FREIGHT_ELEMENT_MST_FK,      " + "      FREIGHT_ELEMENT_ID || '(' || decode(tran.charge_basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') || ')' FREIGHT_ELEMENT_ID ," + "      FREIGHT_ELEMENT_NAME,       " + "      tran.CURRENCY_MST_FK,       " + "      CURRENCY_ID,        " + "      CURRENCY_NAME,        " + "      tran.current_rate,   " + "      tran.request_rate,   " + "      tran.approved_rate   " + "from          " + "      CONT_TRN_AIR_LCL  main,      " + "      CONT_AIR_SURCHARGE  tran,     " + "      FREIGHT_ELEMENT_MST_TBL,      " + "      CURRENCY_TYPE_MST_TBL       " + "where CONT_MAIN_AIR_FK  = " + contPk + "      AND  CONT_TRN_AIR_FK  = main.CONT_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID  ";

                KGFrtDt = objWF.GetDataTable(strSQL);
                string KGFreights = null;

                KGFreights = getStrFreights(KGFrtDt);

                AddColumnsFreights(ChildDt, KGFreights);
                // KGFreights Columns

                TransferKGFreightsData(ChildDt, KGFrtDt);
                //    KGFreights Data in ChildDt now..

                ChildDt.Columns.Add("Other_Charges");
                ChildDt.Columns.Add("OtherCharges");
                ChildDt.Columns.Add("Routing");

                // Other Charge Information need to be fetched======================================Importent
                // Child Records FOR Freights..>

                strSQL = "Select" + "      tran.cont_trn_air_fk," + "      frt.freight_element_mst_pk," + "      curr.currency_mst_pk," + "      tran.charge_basis," + "      tran.current_rate," + "      tran.request_rate," + "      tran.approved_rate" + "from" + "      CONT_TRN_AIR_LCL  main,      " + "      CONT_AIR_OTH_CHRG  tran,     " + "      FREIGHT_ELEMENT_MST_TBL frt, " + "      CURRENCY_TYPE_MST_TBL curr      " + "where CONT_MAIN_AIR_FK  = " + contPk + "      AND  CONT_TRN_AIR_FK  = main.CONT_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID ";

                otherCharges = objWF.GetDataTable(strSQL);

                TransferOtherFreightsData(ChildDt, otherCharges);

                strSQL = "Select" + "      tran.cont_trn_air_fk," + "      tran.cont_air_freight_pk,           " + "      main.port_mst_pol_fk,           " + "      main.port_mst_pod_fk,           " + "      FREIGHT_ELEMENT_MST_FK,       " + "      FREIGHT_ELEMENT_ID,        " + "      FREIGHT_ELEMENT_NAME,       " + "      tran.CURRENCY_MST_FK,              " + "      MIN_AMOUNT " + "FROM" + "      CONT_TRN_AIR_LCL     main,   " + "      CONT_AIR_FREIGHT_TBL    tran,   " + "      FREIGHT_ELEMENT_MST_TBL,      " + "      CURRENCY_TYPE_MST_TBL" + "WHERE CONT_MAIN_AIR_FK = " + contPk + "      AND  CONT_TRN_AIR_FK  = main.CONT_TRN_AIR_PK " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "     UNION " + "     SELECT 0   CONT_TRN_AIR_FK,  " + "     0         CONT_AIR_FREIGHT_PK, " + "     POL.PORT_MST_PK    \"PORT_MST_POL_FK\", " + "     POD.PORT_MST_PK            \"PORT_MST_POD_FK\", " + "     FMT.FREIGHT_ELEMENT_MST_PK, " + "     FMT.FREIGHT_ELEMENT_ID, " + "     FMT.FREIGHT_ELEMENT_NAME," + "     CURR.CURRENCY_MST_PK   \"CURRENCY_MST_FK\"," + "     0           \"MINIMUM_RATE\" " + "FROM" + "      FREIGHT_ELEMENT_MST_TBL FMT,  " + "      PORT_MST_TBL            POL,   " + "      PORT_MST_TBL            POD,   " + "      CURRENCY_TYPE_MST_TBL   CURR   " + " WHERE (1 = 1)" + " AND (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")" + "  AND POL.BUSINESS_TYPE = 1 AND POD.BUSINESS_TYPE = 1 " + "  AND FMT.ACTIVE_FLAG = 1 AND FMT.CHARGE_TYPE = 1 " + "  AND FMT.BUSINESS_TYPE IN (1, 3) " + "  AND FMT.BY_DEFAULT = 1 " + "  AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + "  AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN " + "  (SELECT FTMT.FREIGHT_ELEMENT_MST_PK FROM  " + " CONT_TRN_AIR_LCL        MAIN1," + " CONT_AIR_FREIGHT_TBL    TRAN1," + " FREIGHT_ELEMENT_MST_TBL FTMT," + " CURRENCY_TYPE_MST_TBL " + " WHERE CONT_MAIN_AIR_FK = " + contPk + " AND CONT_TRN_AIR_FK = MAIN1.CONT_TRN_AIR_PK " + " AND TRAN1.FREIGHT_ELEMENT_MST_FK = FTMT.FREIGHT_ELEMENT_MST_PK " + "  AND CURRENCY_MST_FK = CURRENCY_MST_PK) " + "  GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID, " + "  FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.FREIGHT_ELEMENT_NAME,CURR.CURRENCY_MST_PK" + " HAVING POL.PORT_ID <> POD.PORT_ID " + " ORDER BY FREIGHT_ELEMENT_ID ";

                DataTable GRFrtDt = null;
                GRFrtDt = objWF.GetDataTable(strSQL);

                strSQL = "Select" + "      AIRFREIGHT_SLABS_TBL_PK," + "      bpnt.cont_air_freight_fk,               " + "      slab.breakpoint_id,        " + "      BREAKPOINT_DESC,        " + "      CURRENT_RATE,   " + "      REQUEST_RATE,   " + "      APPROVED_RATE" + "FROM" + "      CONT_TRN_AIR_LCL    main,    " + "      CONT_AIR_FREIGHT_TBL   tran,    " + "      CONT_AIR_BREAKPOINTS   bpnt,    " + "      AIRFREIGHT_SLABS_TBL slab" + "where CONT_MAIN_AIR_FK = " + contPk + "      AND tran.CONT_TRN_AIR_FK  = main.CONT_TRN_AIR_PK  " + "      AND bpnt.CONT_AIR_FREIGHT_FK  = tran.CONT_AIR_FREIGHT_PK   " + "      AND bpnt.AIRFREIGHT_SLABS_FK  = AIRFREIGHT_SLABS_TBL_PK  " + "Order By SEQUENCE_NO";

                DataTable SlbDt = null;
                SlbDt = objWF.GetDataTable(strSQL);

                string AirSlabs = null;
                AirSlabs = getStrSlabs(SlbDt);
                AddColumnsSlabs(GRFrtDt, AirSlabs);
                // Slabs Columns
                TransferSlabsData(GRFrtDt, SlbDt);
                //

                GridDS.Tables.Add(ChildDt);
                GridDS.Tables.Add(GRFrtDt);
                DataRelation REL = new DataRelation("RFQRelation", new DataColumn[] {
                    GridDS.Tables[0].Columns["PORT_MST_POL_FK"],
                    GridDS.Tables[0].Columns["PORT_MST_POD_FK"]
                }, new DataColumn[] {
                    GridDS.Tables[1].Columns["port_mst_pol_fk"],
                    GridDS.Tables[1].Columns["port_mst_pod_fk"]
                });
                GridDS.Relations.Add(REL);
                return ds;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchAmendData"

        //'End

        #region " Transfer Data [ Other charges Freights ] "

        /// <summary>
        /// Transfers the other freights data.
        /// </summary>
        /// <param name="GridDt">The grid dt.</param>
        /// <param name="FrtDt">The FRT dt.</param>
        private void TransferOtherFreightsData(DataTable GridDt, DataTable FrtDt)
        {
            Int16 RowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            Int16 frtNumber = default(Int16);
            DataRow[] drArray = null;
            string strOtherCharges = "";
            try
            {
                for (RowCnt = 0; RowCnt <= GridDt.Rows.Count - 1; RowCnt++)
                {
                    drArray = FrtDt.Select(" cont_trn_air_fk = " + GridDt.Rows[RowCnt]["cont_trn_air_pk"]);
                    strOtherCharges = "";
                    for (frtNumber = 0; frtNumber <= drArray.Length - 1; frtNumber++)
                    {
                        strOtherCharges = strOtherCharges + drArray[frtNumber]["freight_element_mst_pk"] + "~" + drArray[frtNumber]["currency_mst_pk"] + "~" + drArray[frtNumber]["charge_basis"] + "~" + drArray[frtNumber]["current_rate"] + "~" + drArray[frtNumber]["request_rate"] + "~" + drArray[frtNumber]["approved_rate"] + "^";
                    }

                    if (!string.IsNullOrEmpty(strOtherCharges))
                    {
                        //adding by thiyagarajan on 26/2/09:fetch saved oth.chrg
                        // GridDt.Columns.Count - 1 - Routing col. ,
                        // GridDt.Columns.Count(-2) - oth.chrg col.
                        //GridDt.Rows(RowCnt).Item(GridDt.Columns.Count - 1) = strOtherCharges
                        GridDt.Rows[RowCnt][GridDt.Columns.Count - 2] = strOtherCharges;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Transfer Data [ Other charges Freights ] "

        #region " Transfer Data [ Kg Freights ] "

        /// <summary>
        /// Transfers the kg freights data.
        /// </summary>
        /// <param name="GridDt">The grid dt.</param>
        /// <param name="FrtDt">The FRT dt.</param>
        private void TransferKGFreightsData(DataTable GridDt, DataTable FrtDt)
        {
            Int16 RowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            Int16 frtNumber = default(Int16);
            DataRow[] drArray = null;
            try
            {
                for (RowCnt = 0; RowCnt <= GridDt.Rows.Count - 1; RowCnt++)
                {
                    drArray = FrtDt.Select(" cont_trn_air_pk = " + GridDt.Rows[RowCnt]["cont_trn_air_pk"]);

                    for (frtNumber = 0; frtNumber <= drArray.Length - 1; frtNumber++)
                    {
                        for (ColCnt = 9; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(drArray[frtNumber]["FREIGHT_ELEMENT_MST_FK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[RowCnt][ColCnt] = drArray[frtNumber]["CURRENT_RATE"];
                                GridDt.Rows[RowCnt][ColCnt + 1] = drArray[frtNumber]["APPROVED_RATE"];
                                GridDt.Rows[RowCnt][ColCnt + 2] = drArray[frtNumber]["REQUEST_RATE"];
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                    }
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Transfer Data [ Kg Freights ] "

        #region " Transfer Data [ Slabs ] "

        //RFQ_SPOT_AIR_FRT_FK, RFQ_SPOT_TRN_FREIGHT_PK
        /// <summary>
        /// Transfers the slabs data.
        /// </summary>
        /// <param name="GridDt">The grid dt.</param>
        /// <param name="SlabDt">The slab dt.</param>
        private void TransferSlabsData(DataTable GridDt, DataTable SlabDt)
        {
            Int16 GRowCnt = default(Int16);
            Int16 SRowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            string frtpk = null;
            DataRow[] drArray = null;
            try
            {
                for (SRowCnt = 0; SRowCnt <= GridDt.Rows.Count - 1; SRowCnt++)
                {
                    drArray = SlabDt.Select(" cont_air_freight_fk = " + GridDt.Rows[SRowCnt]["cont_air_freight_pk"]);

                    for (GRowCnt = 0; GRowCnt <= drArray.Length - 1; GRowCnt++)
                    {
                        for (ColCnt = 9; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(drArray[GRowCnt]["AIRFREIGHT_SLABS_TBL_PK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[SRowCnt][ColCnt] = drArray[GRowCnt]["CURRENT_RATE"];
                                GridDt.Rows[SRowCnt][ColCnt + 2] = drArray[GRowCnt]["REQUEST_RATE"];
                                GridDt.Rows[SRowCnt][ColCnt + 1] = drArray[GRowCnt]["APPROVED_RATE"];
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                    }
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Transfer Data [ Slabs ] "

        #region " Column Add "

        /// <summary>
        /// Adds the columns freights.
        /// </summary>
        /// <param name="DT">The dt.</param>
        /// <param name="FRTs">The fr ts.</param>
        private void AddColumnsFreights(DataTable DT, string FRTs)
        {
            Array CHeads = null;
            string hed = null;
            try
            {
                CHeads = FRTs.Split(',');
                Int16 i = default(Int16);
                for (i = 0; i <= CHeads.Length - 3; i += 3)
                {
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 1)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 2)), typeof(decimal));
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Column Add "

        #region " Column Add "

        /// <summary>
        /// Adds the columns slabs.
        /// </summary>
        /// <param name="DT">The dt.</param>
        /// <param name="FRTs">The fr ts.</param>
        private void AddColumnsSlabs(DataTable DT, string FRTs)
        {
            Array CHeads = null;
            string hed = null;
            try
            {
                CHeads = FRTs.Split(',');
                Int16 i = default(Int16);
                for (i = 0; i <= CHeads.Length - 3; i += 3)
                {
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 1)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 2)), typeof(decimal));
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Column Add "

        #region " Getting Freights "

        /// <summary>
        /// Gets the string freights.
        /// </summary>
        /// <param name="DT">The dt.</param>
        /// <returns></returns>
        private string getStrFreights(DataTable DT)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                string frpk = "-1";
                strBuilder.Append("");
                if (DT.Rows.Count > 0)
                {
                    frpk = "-1";
                }

                for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                {
                    if (DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"] != frpk)
                    {
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"])).Trim() + ",");
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_ID"])).Trim() + ",");
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_NAME"])).Trim() + ".,");
                        frpk = Convert.ToString(DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"]);
                    }
                }
                if (DT.Rows.Count > 0)
                    strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Getting Freights "

        #region " Getting Slabs "

        /// <summary>
        /// Gets the string slabs.
        /// </summary>
        /// <param name="DT">The dt.</param>
        /// <returns></returns>
        private string getStrSlabs(DataTable DT)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                string frpk = "-1";
                string slpk = "-1";
                strBuilder.Append("");

                if (DT.Rows.Count == 0)
                {
                    //    frpk = CStr(DT.Rows(0).Item("cont_air_freight_fk"))
                    //Else
                    return strBuilder.ToString();
                }

                for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                {
                    // If frpk <> CStr(DT.Rows(RowCnt).Item("cont_air_freight_fk")) Then Exit For

                    if (slpk != Convert.ToString(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"]))
                    {
                        if (strBuilder.ToString().IndexOf(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"])).Trim()) == -1)
                        {
                            strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"])).Trim() + ",");
                            strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["BREAKPOINT_ID"])).Trim() + ",");
                            strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["BREAKPOINT_DESC"])).Trim() + ".,");
                            slpk = Convert.ToString(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"]);
                        }
                    }
                }
                if (DT.Rows.Count > 0)
                    strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Getting Slabs "

        #endregion " Fetch Data from aircontract table "

        #region "Save"

        //This region save the Contract in database
        //Here first the data is entered into the Header Table (CONT_MAIN_AIR_TBL) then taking the PkValue of the
        //header the transaction table is filled (CONT_TRN_AIR_LCL)
        //Transaction control is take care as the OracleCommand itself is sent as a
        //parameter to the function filling transaction table

        /// <summary>
        /// Generates the contract no.
        /// </summary>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmployeeId">The n employee identifier.</param>
        /// <param name="nCreatedBy">The n created by.</param>
        /// <param name="objWK">The object wk.</param>
        /// <param name="SID">The sid.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="PODID">The podid.</param>
        /// <returns></returns>
        public string GenerateContractNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK, string SID = "", string POLID = "", string PODID = "")
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey("AIRLINE CONTRACT", nLocationId, nEmployeeId, DateTime.Now, "", "", POLID, nCreatedBy, objWK, SID,
            PODID);
            return functionReturnValue;
        }

        /// <summary>
        /// Deactivates the specified contract pk.
        /// </summary>
        /// <param name="ContractPk">The contract pk.</param>
        /// <returns></returns>
        public ArrayList Deactivate(long ContractPk)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;

            strSQL = "UPDATE CONT_MAIN_AIR_TBL T " + "SET T.ACTIVE = 0," + "T.LAST_MODIFIED_BY_FK =" + M_CREATED_BY_FK + "," + "T.LAST_MODIFIED_DT = SYSDATE," + "T.VERSION_NO = T.VERSION_NO + 1" + "WHERE T.CONT_MAIN_AIR_PK =" + ContractPk;
            objWK.MyCommand.CommandType = CommandType.Text;
            objWK.MyCommand.CommandText = strSQL;
            try
            {
                objWK.MyCommand.ExecuteScalar();
                _PkValue = ContractPk;
                arrMessage.Add("All data saved successfully");
                TRAN.Commit();
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
                return arrMessage;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        #endregion "Save"

        #region "Fetch Contract"

        /// <summary>
        /// Fetches the contract.
        /// </summary>
        /// <param name="nRFQPk">The n RFQ pk.</param>
        /// <returns></returns>
        public DataSet FetchContract(long nRFQPk)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT AIR.AIRLINE_MST_PK, AIR.AIRLINE_ID , AIR.AIRLINE_NAME," + "RFQ.RFQ_REF_NO,CONTHDR.CONTRACT_NO,CONTHDR.CONTRACT_DATE," + "CONTHDR.CONT_APPROVED,CONTHDR.VALID_FROM, CONTHDR.VALID_TO,UOM.DIMENTION_ID," + "CONTTRN.PORT_MST_POL_FK,CONTTRN.PORT_MST_POD_FK,CONTTRN.FREIGHT_ELEMENT_MST_FK," + "CONTTRN.CHECK_FOR_ALL_IN_RT,CONTTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID," + "CONTTRN.LCL_BASIS,CONTHDR.COMMODITY_GROUP_FK,CONTTRN.LCL_CURRENT_RATE," + "CONTTRN.LCL_REQUEST_RATE,CONTTRN.LCL_APPROVED_RATE,CONTHDR.VERSION_NO," + "CONTTRN.VALID_FROM AS \"P_VALID_FROM\",CONTTRN.VALID_TO AS \"P_VALID_TO\", " + "CONTHDR.ACTIVE,CREATED.USER_ID AS \"CREATED\", " + "TO_CHAR(CONTHDR.CREATED_DT,'" + dateFormat + "') CREATED_DT,  " + "MODIFIED.USER_ID AS \"MODIFIED\", " + "TO_CHAR(CONTHDR.LAST_MODIFIED_DT,'" + dateFormat + "') LAST_MODIFIED_DT " + "FROM AIRLINE_MST_TBL AIR," + "CONT_MAIN_AIR_TBL CONTHDR," + "CONT_TRN_AIR_LCL CONTTRN," + "RFQ_MAIN_AIR_TBL RFQ," + "DIMENTION_UNIT_MST_TBL UOM," + "CURRENCY_TYPE_MST_TBL CURR," + "USER_MST_TBL CREATED," + "USER_MST_TBL MODIFIED" + "WHERE" + "CONTHDR.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK " + "AND CONTHDR.RFQ_MAIN_AIR_FK = RFQ.RFQ_MAIN_AIR_PK (+)" + "AND CONTHDR.CONT_MAIN_AIR_PK = CONTTRN.CONT_MAIN_AIR_FK" + "AND CONTTRN.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK" + "AND CONTTRN.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK" + "AND CREATED.USER_MST_PK = CONTHDR.CREATED_BY_FK" + "AND MODIFIED.USER_MST_PK (+)= CONTHDR.LAST_MODIFIED_BY_FK" + "AND CONTHDR.CONT_MAIN_AIR_PK = " + nRFQPk;

                return (new WorkFlow()).GetDataSet(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Contract"

        #region " Fetch Max Contract No."

        /// <summary>
        /// Fetches the contract no.
        /// </summary>
        /// <param name="strContractNo">The string contract no.</param>
        /// <returns></returns>
        public string FetchContractNo(string strContractNo)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT NVL(MAX(T.CONTRACT_NO),0) FROM CONT_MAIN_AIR_TBL T " + "WHERE T.CONTRACT_NO LIKE '" + strContractNo + "/%'" + "ORDER BY T.CONTRACT_NO ";
                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        #endregion " Fetch Max Contract No."

        #region "Update File Name"

        /// <summary>
        /// Updates the name of the file.
        /// </summary>
        /// <param name="AirContractPk">The air contract pk.</param>
        /// <param name="strFileName">Name of the string file.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public bool UpdateFileName(long AirContractPk, string strFileName, Int16 Flag)
        {
            if (strFileName.Trim().Length > 0)
            {
                string RemQuery = null;
                WorkFlow objwk = new WorkFlow();
                if (Flag == 1)
                {
                    RemQuery = " UPDATE CONT_MAIN_AIR_TBL CMAT SET CMAT.ATTACHED_FILE_NAME='" + strFileName + "'";
                    RemQuery += " WHERE CMAT.CONT_MAIN_AIR_PK = " + AirContractPk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (OracleException oraexp)
                    {
                        throw oraexp;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                        return false;
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
                else if (Flag == 2)
                {
                    RemQuery = " UPDATE CONT_MAIN_AIR_TBL CMAT SET CMAT.ATTACHED_FILE_NAME='" + "" + "'";
                    RemQuery += " WHERE CMAT.CONT_MAIN_AIR_PK = " + AirContractPk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (OracleException oraexp)
                    {
                        throw oraexp;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                        return false;
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Fetches the name of the file.
        /// </summary>
        /// <param name="AirContractPk">The air contract pk.</param>
        /// <returns></returns>
        public string FetchFileName(long AirContractPk)
        {
            string strSQL = null;
            string strUpdFileName = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT ";
            strSQL += " CMAT.ATTACHED_FILE_NAME FROM CONT_MAIN_AIR_TBL CMAT WHERE CMAT.CONT_MAIN_AIR_PK = " + AirContractPk;
            try
            {
                DataSet ds = null;
                strUpdFileName = objWF.ExecuteScaler(strSQL);
                return strUpdFileName;
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

        #endregion "Update File Name"

        #region " Supporting Function "

        /// <summary>
        /// Ifs the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private new object ifDBNull(object col)
        {
            if (Convert.ToString(col).Trim().Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        /// <summary>
        /// Removes the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private new object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, ""))
            {
                return "";
            }
            return col;
        }

        #endregion " Supporting Function "

        #region " Fetch Data from airRFQ table "

        #region "FetchMainData"

        /// <summary>
        /// RFQs the fetch one.
        /// </summary>
        /// <param name="GridDS">The grid ds.</param>
        /// <param name="contPk">The cont pk.</param>
        /// <returns></returns>
        public DataSet RFQFetchOne(DataSet GridDS, string contPk = "")
        {
            try
            {
                string ContainerPKs = null;
                bool NewRecord = true;
                //If rfqSpotRatePK.Trim.Length > 0 Then NewRecord = False

                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                DataSet ds = null;
                DataTable otherCharges = new DataTable();
                GridDS.Tables.Clear();
                // Now GridDS is Empty

                strSQL = " SELECT MAIN_RFQ.RFQ_MAIN_AIR_PK," + "       MAIN_RFQ.AIRLINE_MST_FK," + "       AIRLINE.AIRLINE_ID," + "       AIRLINE.AIRLINE_NAME," + "       MAIN_RFQ.RFQ_REF_NO," + "       MAIN_RFQ.COMMODITY_MST_FK," + "       TO_CHAR(MAIN_RFQ.VALID_FROM,DATEFORMAT) VALID_FROM," + "       TO_CHAR(MAIN_RFQ.VALID_TO,DATEFORMAT) VALID_TO,'0' WORKFLOW_STATUS" + " FROM RFQ_MAIN_AIR_TBL       MAIN_RFQ," + "       AIRLINE_MST_TBL         AIRLINE," + "       COMMODITY_GROUP_MST_TBL COMM" + " WHERE MAIN_RFQ.RFQ_MAIN_AIR_PK = " + contPk + "       AND MAIN_RFQ.COMMODITY_MST_FK =COMM.COMMODITY_GROUP_PK(+)" + "       AND MAIN_RFQ.Airline_Mst_Fk = AIRLINE.AIRLINE_MST_PK(+)";

                ds = objWF.GetDataSet(strSQL);

                // Child Record :==========[ POL and POD ]====

                strSQL = "    Select main.RFQ_trn_air_pk,                                 " + "       main.PORT_MST_POL_FK,                                       " + "       PORTPOL.PORT_ID                             PORTPOL_ID,     " + "       PORTPOL.PORT_NAME                           PORTPOL_NAME,   " + "       main.PORT_MST_POD_FK,                                       " + "       PORTPOD.PORT_ID                             PORTPOD_ID,     " + "       PORTPOD.PORT_NAME                           PORTPOD_NAME,   " + "       TO_CHAR(VALID_FROM,DATEFORMAT) VALID_FROM,                                                 " + "       TO_CHAR(VALID_TO,DATEFORMAT) VALID_TO                   " + "      from                                                         " + "       RFQ_TRN_AIR_LCL                main,                       " + "       PORT_MST_TBL                    PORTPOL,                    " + "       PORT_MST_TBL                    PORTPOD                     " + "      where  RFQ_MAIN_AIR_FK      = " + contPk + "                " + "       AND PORT_MST_POL_FK         =   PORTPOL.PORT_MST_PK         " + "       AND PORT_MST_POD_FK         =   PORTPOD.PORT_MST_PK         ";

                DataTable ChildDt = null;
                ChildDt = objWF.GetDataTable(strSQL);

                // Adding KGFreights Columns [ Charge Basis = 3 ]
                DataTable KGFrtDt = null;

                strSQL = "Select" + "      main.rfq_trn_air_pk," + "      tran.rfq_air_surcharge_pk,          " + "      tran.FREIGHT_ELEMENT_MST_FK,      " + "      FREIGHT_ELEMENT_ID || '(' || decode(tran.charge_basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') || ')' FREIGHT_ELEMENT_ID ," + "      FREIGHT_ELEMENT_NAME,       " + "      tran.CURRENCY_MST_FK,       " + "      CURRENCY_ID,        " + "      CURRENCY_NAME,        " + "      tran.current_rate,   " + "      tran.requested_rate   " + "from          " + "      rfq_TRN_AIR_LCL  main,      " + "      rfq_AIR_SURCHARGE  tran,     " + "      FREIGHT_ELEMENT_MST_TBL,      " + "      CURRENCY_TYPE_MST_TBL       " + "where rfq_MAIN_AIR_FK  = " + contPk + "      AND  rfq_TRN_AIR_FK  = main.rfq_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID  ";

                KGFrtDt = objWF.GetDataTable(strSQL);
                string KGFreights = null;

                KGFreights = RFQgetStrFreights(KGFrtDt);

                RFQAddColumnsFreights(ChildDt, KGFreights);
                // KGFreights Columns

                RFQTransferKGFreightsData(ChildDt, KGFrtDt);
                //    KGFreights Data in ChildDt now..

                ChildDt.Columns.Add("Other_Charges");
                ChildDt.Columns.Add("OtherCharges");
                ChildDt.Columns.Add("Routing");
                //Snigdharani - 02/12/2008
                // Other Charge Information need to be fetched======================================Importent
                // Child Records FOR Freights..>

                strSQL = "Select" + "      tran.rfq_trn_air_fk," + "      frt.freight_element_mst_pk," + "      curr.currency_mst_pk," + "      tran.charge_basis," + "      tran.current_rate," + "      tran.requested_rate" + "from" + "      rfq_TRN_AIR_LCL  main,      " + "      rfq_AIR_OTH_CHRG  tran,     " + "      FREIGHT_ELEMENT_MST_TBL frt, " + "      CURRENCY_TYPE_MST_TBL curr      " + "where rfq_MAIN_AIR_FK  = " + contPk + "      AND  rfq_TRN_AIR_FK  = main.rfq_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID ";

                otherCharges = objWF.GetDataTable(strSQL);

                RFQTransferOtherFreightsData(ChildDt, otherCharges);

                strSQL = "Select" + "      tran.rfq_trn_air_fk," + "      tran.rfq_air_freight_pk,           " + "      main.port_mst_pol_fk,           " + "      main.port_mst_pod_fk,           " + "      FREIGHT_ELEMENT_MST_FK,       " + "      FREIGHT_ELEMENT_ID,        " + "      FREIGHT_ELEMENT_NAME,       " + "      tran.CURRENCY_MST_FK,              " + "      MIN_AMOUNT " + "FROM" + "      rfq_TRN_AIR_LCL     main,   " + "      rfq_AIR_FREIGHT_TBL    tran,   " + "      FREIGHT_ELEMENT_MST_TBL,      " + "      CURRENCY_TYPE_MST_TBL" + "WHERE rfq_MAIN_AIR_FK = " + contPk + "      AND  rfq_TRN_AIR_FK  = main.rfq_TRN_AIR_PK " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "      Order By FREIGHT_ELEMENT_ID    ";

                DataTable GRFrtDt = null;
                GRFrtDt = objWF.GetDataTable(strSQL);

                strSQL = "Select" + "      AIRFREIGHT_SLABS_TBL_PK," + "      bpnt.RFQ_air_freight_fk,               " + "      slab.breakpoint_id,        " + "      BREAKPOINT_DESC,        " + "      CURRENT_RATE,   " + "      REQUESTED_RATE   " + "FROM" + "      rfq_TRN_AIR_LCL    main,    " + "      rfq_AIR_FREIGHT_TBL   tran,    " + "      rfq_AIR_BREAKPOINTS   bpnt,    " + "      AIRFREIGHT_SLABS_TBL slab" + "where rfq_MAIN_AIR_FK = " + contPk + "      AND tran.rfq_TRN_AIR_FK  = main.rfq_TRN_AIR_PK  " + "      AND bpnt.rfq_AIR_FREIGHT_FK  = tran.rfq_AIR_FREIGHT_PK   " + "      AND bpnt.AIRFREIGHT_SLABS_FK  = AIRFREIGHT_SLABS_TBL_PK  " + "Order By SEQUENCE_NO";

                DataTable SlbDt = null;
                SlbDt = objWF.GetDataTable(strSQL);

                string AirSlabs = null;
                AirSlabs = RFQgetStrSlabs(SlbDt);
                RFQAddColumnsSlabs(GRFrtDt, AirSlabs);
                // Slabs Columns
                RFQTransferSlabsData(GRFrtDt, SlbDt);
                //

                GridDS.Tables.Add(ChildDt);
                GridDS.Tables.Add(GRFrtDt);
                DataRelation REL = new DataRelation("RFQRelation", new DataColumn[] { GridDS.Tables[0].Columns["RFQ_trn_air_pk"] }, new DataColumn[] { GridDS.Tables[1].Columns["RFQ_trn_air_fk"] });
                GridDS.Relations.Add(REL);
                return ds;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchMainData"

        #region " Transfer Data [ Other charges Freights ] "

        /// <summary>
        /// RFQs the transfer other freights data.
        /// </summary>
        /// <param name="GridDt">The grid dt.</param>
        /// <param name="FrtDt">The FRT dt.</param>
        private void RFQTransferOtherFreightsData(DataTable GridDt, DataTable FrtDt)
        {
            Int16 RowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            Int16 frtNumber = default(Int16);
            DataRow[] drArray = null;
            string strOtherCharges = "";
            try
            {
                for (RowCnt = 0; RowCnt <= GridDt.Rows.Count - 1; RowCnt++)
                {
                    drArray = FrtDt.Select(" rfq_trn_air_fk = " + GridDt.Rows[RowCnt]["rfq_trn_air_pk"]);
                    strOtherCharges = "";
                    for (frtNumber = 0; frtNumber <= drArray.Length - 1; frtNumber++)
                    {
                        strOtherCharges = strOtherCharges + drArray[frtNumber]["freight_element_mst_pk"] + "~" + drArray[frtNumber]["currency_mst_pk"] + "~" + drArray[frtNumber]["charge_basis"] + "~" + drArray[frtNumber]["current_rate"] + "~" + drArray[frtNumber]["requested_rate"] + "~" + drArray[frtNumber]["requested_rate"] + "^";
                    }

                    if (!string.IsNullOrEmpty(strOtherCharges))
                    {
                        GridDt.Rows[RowCnt][GridDt.Columns.Count - 2] = strOtherCharges;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Transfer Data [ Other charges Freights ] "

        #region " Transfer Data [ Kg Freights ] "

        /// <summary>
        /// RFQs the transfer kg freights data.
        /// </summary>
        /// <param name="GridDt">The grid dt.</param>
        /// <param name="FrtDt">The FRT dt.</param>
        private void RFQTransferKGFreightsData(DataTable GridDt, DataTable FrtDt)
        {
            Int16 RowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            Int16 frtNumber = default(Int16);
            DataRow[] drArray = null;
            try
            {
                for (RowCnt = 0; RowCnt <= GridDt.Rows.Count - 1; RowCnt++)
                {
                    drArray = FrtDt.Select(" rfq_trn_air_pk = " + GridDt.Rows[RowCnt]["rfq_trn_air_pk"]);

                    for (frtNumber = 0; frtNumber <= drArray.Length - 1; frtNumber++)
                    {
                        for (ColCnt = 9; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(drArray[frtNumber]["FREIGHT_ELEMENT_MST_FK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[RowCnt][ColCnt] = drArray[frtNumber]["CURRENT_RATE"];
                                GridDt.Rows[RowCnt][ColCnt + 1] = drArray[frtNumber]["REQUESTED_RATE"];
                                GridDt.Rows[RowCnt][ColCnt + 2] = drArray[frtNumber]["REQUESTED_RATE"];
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                    }
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Transfer Data [ Kg Freights ] "

        #region " Transfer Data [ Slabs ] "

        //RFQ_SPOT_AIR_FRT_FK, RFQ_SPOT_TRN_FREIGHT_PK
        /// <summary>
        /// RFQs the transfer slabs data.
        /// </summary>
        /// <param name="GridDt">The grid dt.</param>
        /// <param name="SlabDt">The slab dt.</param>
        private void RFQTransferSlabsData(DataTable GridDt, DataTable SlabDt)
        {
            Int16 GRowCnt = default(Int16);
            Int16 SRowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            string frtpk = null;
            DataRow[] drArray = null;
            try
            {
                for (SRowCnt = 0; SRowCnt <= GridDt.Rows.Count - 1; SRowCnt++)
                {
                    drArray = SlabDt.Select(" rfq_air_freight_fk = " + GridDt.Rows[SRowCnt]["rfq_air_freight_pk"]);

                    for (GRowCnt = 0; GRowCnt <= drArray.Length - 1; GRowCnt++)
                    {
                        for (ColCnt = 9; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(drArray[GRowCnt]["AIRFREIGHT_SLABS_TBL_PK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[SRowCnt][ColCnt] = drArray[GRowCnt]["CURRENT_RATE"];
                                GridDt.Rows[SRowCnt][ColCnt + 2] = drArray[GRowCnt]["REQUESTED_RATE"];
                                GridDt.Rows[SRowCnt][ColCnt + 1] = drArray[GRowCnt]["REQUESTED_RATE"];
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                    }
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Transfer Data [ Slabs ] "

        #region " Column Add "

        /// <summary>
        /// RFQs the add columns freights.
        /// </summary>
        /// <param name="DT">The dt.</param>
        /// <param name="FRTs">The fr ts.</param>
        private void RFQAddColumnsFreights(DataTable DT, string FRTs)
        {
            try
            {
                Array CHeads = null;
                string hed = null;
                CHeads = FRTs.Split(',');
                Int16 i = default(Int16);
                for (i = 0; i <= CHeads.Length - 3; i += 3)
                {
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 1)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 2)), typeof(decimal));
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Column Add "

        #region " Column Add "

        /// <summary>
        /// RFQs the add columns slabs.
        /// </summary>
        /// <param name="DT">The dt.</param>
        /// <param name="FRTs">The fr ts.</param>
        private void RFQAddColumnsSlabs(DataTable DT, string FRTs)
        {
            try
            {
                Array CHeads = null;
                string hed = null;
                CHeads = FRTs.Split(',');
                Int16 i = default(Int16);
                for (i = 0; i <= CHeads.Length - 3; i += 3)
                {
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 1)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 2)), typeof(decimal));
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Column Add "

        #region " Getting Freights "

        /// <summary>
        /// Rfs the qget string freights.
        /// </summary>
        /// <param name="DT">The dt.</param>
        /// <returns></returns>
        private string RFQgetStrFreights(DataTable DT)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                string frpk = "-1";
                strBuilder.Append("");
                if (DT.Rows.Count > 0)
                {
                    frpk = "-1";
                }

                for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                {
                    if (DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"] != frpk)
                    {
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"])).Trim() + ",");
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_ID"])).Trim() + ",");
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_NAME"])).Trim() + ".,");
                        frpk = Convert.ToString(DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"]);
                    }
                }
                if (DT.Rows.Count > 0)
                    strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Getting Freights "

        #region " Getting Slabs "

        /// <summary>
        /// Rfs the qget string slabs.
        /// </summary>
        /// <param name="DT">The dt.</param>
        /// <returns></returns>
        private string RFQgetStrSlabs(DataTable DT)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                string frpk = "-1";
                string slpk = "-1";
                strBuilder.Append("");

                if (DT.Rows.Count == 0)
                {
                    //    frpk = CStr(DT.Rows(0).Item("cont_air_freight_fk"))
                    //Else
                    return strBuilder.ToString();
                }

                for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                {
                    if (slpk != Convert.ToString(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"]))
                    {
                        if (strBuilder.ToString().IndexOf(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"])).Trim()) == -1)
                        {
                            strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"])).Trim() + ",");
                            strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["BREAKPOINT_ID"])).Trim() + ",");
                            strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["BREAKPOINT_DESC"])).Trim() + ".,");
                            slpk = Convert.ToString(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"]);
                        }
                    }
                }
                if (DT.Rows.Count > 0)
                    strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Getting Slabs "

        #endregion " Fetch Data from airRFQ table "

        #region "Fetch Details"

        /// <summary>
        /// Fetches the report detail.
        /// </summary>
        /// <param name="CustContractNr">The customer contract nr.</param>
        /// <returns></returns>
        public DataSet FetchReportDetail(string CustContractNr)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWK = new WorkFlow();
                sb.Append("select AMT.AIRLINE_NAME,");
                sb.Append("       ACD.ADM_ADDRESS_1,");
                sb.Append("       ACD.ADM_ADDRESS_2,");
                sb.Append("       ACD.ADM_ADDRESS_3,");
                sb.Append("       ACD.ADM_CITY,");
                sb.Append("       ACD.ADM_ZIP_CODE,");
                sb.Append("       CMT.COUNTRY_NAME,");
                sb.Append("       'AIR' BIZ_TYPE,");
                sb.Append("       DECODE(CMAT.CONT_APPROVED, 1, 'APPROVED', 0, 'PENDING', 2, 'REJECTED') CONT_APPROVED,");
                sb.Append("       DECODE(CMAT.cont_approved,");
                sb.Append("              0,");
                sb.Append("              CUMT.USER_NAME,");
                sb.Append("              1,");
                sb.Append("              LUMT.USER_NAME,");
                sb.Append("              2,");
                sb.Append("              LUMT.USER_ID) USER_ID,");
                sb.Append("       RMAT.RFQ_REF_NO,");
                sb.Append("       CMAT.CONTRACT_NO,");
                sb.Append("       CMAT.CONTRACT_DATE,");
                sb.Append("       CMAT.VALID_FROM,");
                sb.Append("       CMAT.VALID_TO,");
                sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
                sb.Append("       CMAT.APPROVED_DATE,");
                sb.Append("       case when CUMT.USER_NAME is null then");
                sb.Append("         lumt.user_name");
                sb.Append("       else");
                sb.Append("         cumt.user_name");
                sb.Append("       end USER_NAME");
                sb.Append("  from CONT_MAIN_AIR_TBL       CMAT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       AIRLINE_CONTACT_DTLS    ACD,");
                sb.Append("       COUNTRY_MST_TBL         CMT,");
                sb.Append("       RFQ_MAIN_AIR_TBL        RMAT,");
                sb.Append("       USER_MST_TBL            CUMT,");
                sb.Append("       USER_MST_TBL            LUMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE CMAT.CONTRACT_NO = '" + CustContractNr + "'");
                sb.Append("   AND CMAT.AIRLINE_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND ACD.AIRLINE_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND ACD.ADM_COUNTRY_MST_FK = CMT.COUNTRY_MST_PK(+)");
                sb.Append("   AND CMAT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND CMAT.RFQ_MAIN_AIR_FK = RMAT.RFQ_MAIN_AIR_PK(+)");
                sb.Append("   AND CMAT.CREATED_BY_FK = CUMT.USER_MST_PK(+)");
                sb.Append("   AND CMAT.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+) ");
                return objWK.GetDataSet(sb.ToString());
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

        #endregion "Fetch Details"

        #region "Fetch Freight Details"

        /// <summary>
        /// Fetches the freight details.
        /// </summary>
        /// <param name="CustContractNr">The customer contract nr.</param>
        /// <returns></returns>
        public DataSet FetchFreightDetails(string CustContractNr)
        {
            try
            {
                WorkFlow objWK = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT CMAT.CONT_MAIN_AIR_PK,");
                sb.Append("       AOO.PORT_NAME PORT_ID,");
                sb.Append("       AOD.PORT_NAME PORT_ID,");
                sb.Append("       CTAL.VALID_FROM,");
                sb.Append("       CTAL.VALID_TO,");
                sb.Append("       CAFT.MIN_AMOUNT,");
                sb.Append("       CTM.CURRENCY_ID,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT_ID,");
                sb.Append("       CASE");
                sb.Append("         WHEN CABP.APPROVED_RATE IS NULL THEN");
                sb.Append("          CABP.REQUEST_RATE");
                sb.Append("         ELSE");
                sb.Append("          CABP.APPROVED_RATE");
                sb.Append("       END RATE,");
                sb.Append("       ASLABS.BREAKPOINT_ID SLABS,");
                sb.Append("       ASLABS.SEQUENCE_NO");
                sb.Append("  FROM CONT_MAIN_AIR_TBL       CMAT,");
                sb.Append("       CONT_TRN_AIR_LCL        CTAL,");
                sb.Append("       AIRFREIGHT_SLABS_TBL    ASLABS,");
                sb.Append("       CONT_AIR_FREIGHT_TBL    CAFT,");
                sb.Append("       CONT_AIR_BREAKPOINTS    CABP,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CTM,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT");
                sb.Append(" WHERE CTAL.CONT_MAIN_AIR_FK = CMAT.CONT_MAIN_AIR_PK");
                sb.Append("   AND CAFT.CONT_AIR_FREIGHT_PK = CABP.CONT_AIR_FREIGHT_FK");
                sb.Append("   AND CABP.AIRFREIGHT_SLABS_FK = ASLABS.AIRFREIGHT_SLABS_TBL_PK");
                sb.Append("   AND CAFT.CONT_TRN_AIR_FK = CTAL.CONT_TRN_AIR_PK");
                sb.Append("   AND CAFT.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND CAFT.CURRENCY_MST_FK = CTM.CURRENCY_MST_PK");
                sb.Append("   AND AOO.PORT_MST_PK = CTAL.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK = CTAL.PORT_MST_POD_FK");
                sb.Append("   AND CMAT.CONTRACT_NO = '" + CustContractNr + "'");
                sb.Append("   ORDER BY ASLABS.SEQUENCE_NO ");
                return objWK.GetDataSet(sb.ToString());
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

        #endregion "Fetch Freight Details"

        #region "File If Exist"

        //This function is created by Ashish Arya on 22nd Sept 2011
        //to check that particular file does already exist in database for a DepotPk
        //Returns true if exist else false
        /// <summary>
        /// Checks the file existence.
        /// </summary>
        /// <param name="AirContractPk">The air contract pk.</param>
        /// <param name="strFileName">Name of the string file.</param>
        /// <returns></returns>
        public bool CheckFileExistence(long AirContractPk, string strFileName)
        {
            WorkFlow objwk = new WorkFlow();
            bool Chk = false;
            string RemQuery = "";
            RemQuery = " SELECT COUNT(*) FROM CONT_MAIN_AIR_TBL CMAT ";
            RemQuery += " WHERE CMAT.CONT_MAIN_AIR_PK = " + AirContractPk + " AND CMAT.ATTACHED_FILE_NAME='" + strFileName + "'";
            try
            {
                objwk.OpenConnection();
                if (Convert.ToInt32(objwk.ExecuteScaler(RemQuery)) > 0)
                {
                    Chk = true;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
                return false;
            }
            finally
            {
                objwk.MyCommand.Connection.Close();
            }
            return Chk;
        }

        #endregion "File If Exist"
    }
}