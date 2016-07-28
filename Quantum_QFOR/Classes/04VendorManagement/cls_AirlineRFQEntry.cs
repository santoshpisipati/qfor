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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_AirlineRFQEntry : CommonFeatures
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
            string strCondition = null;
            string strNewModeCondition = null;

            //Spliting the POL and POD Pk's
            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');

            //Making condition as the record should have only selected POL and POD
            //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk.GetValue
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
            }

            //Making query with the condition added
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
                    //current rate~slabpk
                    dtMain.Columns.Add(dtKgs.Rows[i][0].ToString(), typeof(decimal));
                    //approved rate~id
                    dtMain.Columns.Add(dtKgs.Rows[i][1].ToString(), typeof(decimal));
                    //'request rate~name
                    //dtMain.Columns.Add(dtKgs.Rows(i).Item(2).ToString, GetType(Decimal))
                }

                TransferAirlineFreightData(dtMain, dtKgs);
                dtMain.Columns.Add("Other_Charges");
                dtMain.Columns.Add("OtherCharges");
                return dtMain;
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
            string strCondition = null;
            string strNewModeCondition = null;
            //Spliting the POL and POD Pk's
            arrPolPk = strPolPk.Split(',');
            arrPodPk = strPodPk.Split(',');

            //Making condition as the record should have only selected POL and POD
            //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk.GetValue
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
                strNewModeCondition += " AND FMT.CHARGE_Type=1";
                strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (1,3)";
                strNewModeCondition += " AND FMT.BY_DEFAULT=1";
            }

            //Making query with the condition added
            str.Append(" SELECT 0 FK, 0 PK,POL.PORT_MST_PK \"POLPK\",POD.PORT_MST_PK \"PODPK\", ");
            str.Append(" FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,FMT.FREIGHT_ELEMENT_NAME,");
            str.Append(" CURR.CURRENCY_MST_PK, 0 \"MINIMUM_RATE\" ");
            str.Append(" FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,");
            //Snigdharani - 09/12/2008 - Location based currency...
            str.Append(" CURRENCY_TYPE_MST_TBL CURR");
            // ,CORPORATE_MST_TBL CMT")
            str.Append(" WHERE (1=1)");
            str.Append(" AND (");
            str.Append(strCondition + ")");
            str.Append(strNewModeCondition);
            str.Append(" AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
            // CMT.CURRENCY_MST_FK")
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
                    //'Request Ratename
                    //dtMain.Columns.Add("'" + dtSlabs.Rows(i).Item(2).ToString + "'", GetType(Decimal))
                }
                return dtMain;
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

        /// <summary>
        /// Fetches the kg freight new.
        /// </summary>
        /// <param name="Mode">The mode.</param>
        /// <param name="airlinePK">The airline pk.</param>
        /// <param name="RFQPK">The RFQPK.</param>
        /// <returns></returns>
        public DataTable FetchKgFreightNew(string Mode, string airlinePK = "0", string RFQPK = "0")
        {
            System.Text.StringBuilder sqlBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strNewModeCondition = null;
            if (string.IsNullOrEmpty(RFQPK))
            {
                if (string.IsNullOrEmpty(airlinePK))
                {
                    sqlBuilder.Append(" SELECT FR.FREIGHT_ELEMENT_MST_PK,");
                    sqlBuilder.Append("     FR.FREIGHT_ELEMENT_ID || '(' ||");
                    sqlBuilder.Append("     decode(FR.CHARGE_BASIS, 1, '%', 2, 'Flat', 3, 'Kgs') || ')',");
                    sqlBuilder.Append("     FR.FREIGHT_ELEMENT_NAME,");
                    sqlBuilder.Append("     FR.basis_value");
                    sqlBuilder.Append(" FROM FREIGHT_ELEMENT_MST_TBL FR");
                    sqlBuilder.Append(" WHERE FR.CHARGE_Type = 2");
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
                    sqlBuilder.Append(" WHERE FR.CHARGE_Type = 2");
                    sqlBuilder.Append("   and airline.airline_mst_pk =" + airlinePK);
                    sqlBuilder.Append("   and sur.freight_element_mst_fk = fr.freight_element_mst_pk");
                    sqlBuilder.Append("   AND SUR.AIRLINE_MST_FK = airline.airline_mst_pk");
                    sqlBuilder.Append(" ORDER BY FR.FREIGHT_ELEMENT_ID");
                    //sqlBuilder.Append(" SELECT FR.FREIGHT_ELEMENT_MST_PK,")
                    //sqlBuilder.Append("     FR.FREIGHT_ELEMENT_ID || '(' ||")
                    //sqlBuilder.Append("     decode(FR.CHARGE_BASIS, 1, '%', 2, 'Flat', 3, 'Kgs') || ')',")
                    //sqlBuilder.Append("     FR.FREIGHT_ELEMENT_NAME,")
                    //sqlBuilder.Append("     FR.basis_value")
                    //sqlBuilder.Append(" FROM FREIGHT_ELEMENT_MST_TBL FR")
                    //sqlBuilder.Append(" WHERE FR.CHARGE_Type = 2")
                    //sqlBuilder.Append("     and fr.active_flag = 1")
                    //sqlBuilder.Append("     and fr.business_type = 1")
                    //sqlBuilder.Append(" ORDER BY FR.FREIGHT_ELEMENT_ID")
                }
            }
            else
            {
                sqlBuilder.Append("Select ");
                sqlBuilder.Append("      tran.FREIGHT_ELEMENT_MST_FK,      " + "");
                sqlBuilder.Append("      FREIGHT_ELEMENT_ID || '(' || decode(tran.charge_basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') || ')' FREIGHT_ELEMENT_ID ," + "");
                sqlBuilder.Append("      FREIGHT_ELEMENT_NAME,       " + "");
                sqlBuilder.Append("      tran.requested_rate,   " + "");
                sqlBuilder.Append("      tran.CURRENCY_MST_FK,       " + "");
                sqlBuilder.Append("      CURRENCY_ID,        " + "");
                sqlBuilder.Append("      CURRENCY_NAME,        " + "");
                sqlBuilder.Append("      main.RFQ_trn_air_pk," + "");
                sqlBuilder.Append("      tran.RFQ_air_surcharge_pk,          " + "");
                sqlBuilder.Append("      tran.current_rate   " + "");

                sqlBuilder.Append(" from          " + "");
                sqlBuilder.Append("       RFQ_TRN_AIR_LCL main,      " + "");
                sqlBuilder.Append("     RFQ_AIR_SURCHARGE tran,     " + "");
                sqlBuilder.Append("      FREIGHT_ELEMENT_MST_TBL,      " + "");
                sqlBuilder.Append("      CURRENCY_TYPE_MST_TBL       " + "");
                sqlBuilder.Append("where RFQ_MAIN_AIR_FK = " + RFQPK + "");
                sqlBuilder.Append("      AND  RFQ_TRN_AIR_FK = main.RFQ_TRN_AIR_PK  " + "");
                sqlBuilder.Append("      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "");
                sqlBuilder.Append("      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "");
                sqlBuilder.Append("Order By FREIGHT_ELEMENT_ID  ");
            }
            try
            {
                return objWF.GetDataTable(sqlBuilder.ToString());
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
            try
            {
                for (SRowCnt = 0; SRowCnt <= GridDt.Rows.Count - 1; SRowCnt++)
                {
                    for (ColCnt = 9; ColCnt <= GridDt.Columns.Count - 1; ColCnt += 2)
                    {
                        drArray = SlabDt.Select("FREIGHT_ELEMENT_MST_PK = " + GridDt.Columns[ColCnt].Caption);

                        if (Convert.ToString(drArray[0]["FREIGHT_ELEMENT_MST_PK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                        {
                            GridDt.Rows[SRowCnt][ColCnt] = drArray[0]["basis_value"];
                            //GridDt.Rows(SRowCnt).Item(ColCnt + 2) = drArray(0).Item("basis_value")
                            GridDt.Rows[SRowCnt][ColCnt + 1] = drArray[0]["basis_value"];
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

        #endregion " Transfer Data [ kg ] "

        #endregion "Fetch function for creating New Airline Tariff Entry "

        #region "Fetch Called by Select Container/Sector"

        //This function returns all the active sectors from the database.
        //If the given POL and POD are present then the value will come as checked.
        /// <summary>
        /// Actives the sector.
        /// </summary>
        /// <param name="LocationPk">The location pk.</param>
        /// <param name="strPOLPk">The string pol pk.</param>
        /// <param name="strPODPk">The string pod pk.</param>
        /// <returns></returns>
        public DataTable ActiveSector(long LocationPk, string strPOLPk = "", string strPODPk = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;
            //Spliting the POL and POD Pk's
            arrPolPk = strPOLPk.Split(',');
            arrPodPk = strPODPk.Split(',');
            //Making condition as the record should have only selected POL and POD
            //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk.GetValue
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

            //Creating the sql if the user has already selected one port pair in calling form
            //incase of veiwing also then that port pair will come and active port pair in the grid.
            //BUSINESS_TYPE = 1 :- Is the business type for AIR
            strSQL = "SELECT POL.PORT_MST_PK ,POL.PORT_ID AS \"POL\", " + "POD.PORT_MST_PK,POD.PORT_ID,'1' CHK " + "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  " + "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " + "AND POL.BUSINESS_TYPE = 1 " + "AND POD.BUSINESS_TYPE = 1 " + "AND ( " + strCondition + " ) " + "UNION " + "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, " + "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK " + "FROM SECTOR_MST_TBL SMT, " + "PORT_MST_TBL POL, " + "PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM " + "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + "AND   LPM.PORT_MST_FK = POL.PORT_MST_PK " + "AND   POL.BUSINESS_TYPE = 1 " + "AND   POD.BUSINESS_TYPE = 1 " + "AND   LPM.LOCATION_MST_FK =" + LocationPk + "AND   SMT.ACTIVE = 1 " + "AND   SMT.BUSINESS_TYPE = 1 " + "ORDER BY CHK DESC,POL";
            try
            {
                return objWF.GetDataTable(strSQL);
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

        /// <summary>
        /// Actives the sector with ch wt.
        /// </summary>
        /// <param name="LocationPk">The location pk.</param>
        /// <param name="strPOLPk">The string pol pk.</param>
        /// <param name="strPODPk">The string pod pk.</param>
        /// <returns></returns>
        public DataTable ActiveSectorWithChWt(long LocationPk, string strPOLPk = "", string strPODPk = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;
            //Spliting the POL and POD Pk's
            arrPolPk = strPOLPk.Split(',');
            arrPodPk = strPODPk.Split(',');
            //Making condition as the record should have only selected POL and POD
            //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk.GetValue
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

            //Creating the sql if the user has already selected one port pair in calling form
            //incase of veiwing also then that port pair will come and active port pair in the grid.
            //BUSINESS_TYPE = 1 :- Is the business type for AIR
            strSQL = "SELECT POL.PORT_MST_PK ,POL.PORT_ID AS \"POL\", " + "POD.PORT_MST_PK,POD.PORT_ID,NULL AS CHARGEABLE_WEIGHT,NULL AS SLAB,NULL AS QTY,'1' CHK " + "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  " + "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " + "AND POL.BUSINESS_TYPE = 1 " + "AND POD.BUSINESS_TYPE = 1 " + "AND ( " + strCondition + " ) " + "UNION " + "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD,NULL AS CHARGEABLE_WEIGHT,NULL AS SLAB,NULL AS QTY, " + "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK " + "FROM SECTOR_MST_TBL SMT, " + "PORT_MST_TBL POL, " + "PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM " + "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + "AND   LPM.PORT_MST_FK = POL.PORT_MST_PK " + "AND   POL.BUSINESS_TYPE = 1 " + "AND   POD.BUSINESS_TYPE = 1 " + "AND   LPM.LOCATION_MST_FK =" + LocationPk + "AND   SMT.ACTIVE = 1 " + "AND   SMT.BUSINESS_TYPE = 1 " + "ORDER BY CHK DESC,POL";
            try
            {
                return objWF.GetDataTable(strSQL);
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

        #endregion "Fetch Called by Select Container/Sector"

        #region " Fetch Data from airrfq table "

        #region "FetchMainData"

        /// <summary>
        /// Fetches the one.
        /// </summary>
        /// <param name="GridDS">The grid ds.</param>
        /// <param name="rfqPK">The RFQ pk.</param>
        /// <returns></returns>
        public DataSet FetchOne(DataSet GridDS, string rfqPK = "")
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

                strSQL = "SELECT RFQ.RFQ_MAIN_AIR_PK," + "      RFQ.AIRLINE_MST_FK," + "      AIRLINE.AIRLINE_ID," + "      AIRLINE.AIRLINE_NAME," + "      RFQ.RFQ_REF_NO," + "      RFQ.RFQ_DATE," + "      RFQ.COMMODITY_MST_FK," + "      TO_CHAR(RFQ.VALID_FROM,DATEFORMAT) VALID_FROM," + "      TO_CHAR(RFQ.VALID_TO,DATEFORMAT) VALID_TO," + "      RFQ.VERSION_NO" + "FROM  AIRLINE_MST_TBL AIRLINE," + "      RFQ_MAIN_AIR_TBL RFQ," + "      COMMODITY_GROUP_MST_TBL COMM" + "WHERE" + "      RFQ.RFQ_MAIN_AIR_PK=" + rfqPK + "      AND RFQ.COMMODITY_MST_FK = COMM.COMMODITY_GROUP_PK(+)" + "      AND RFQ.AIRLINE_MST_FK = AIRLINE.AIRLINE_MST_PK(+)";

                ds = objWF.GetDataSet(strSQL);

                // Child Record :==========[ POL and POD ]====

                strSQL = "  SELECT MAIN.RFQ_TRN_AIR_PK,                                 " + "       MAIN.PORT_MST_POL_FK,                                       " + "       PORTPOL.PORT_ID                             PORTPOL_ID,     " + "       PORTPOL.PORT_NAME                           PORTPOL_NAME,   " + "       main.PORT_MST_POD_FK,                                       " + "       PORTPOD.PORT_ID                             PORTPOD_ID,     " + "       PORTPOD.PORT_NAME                           PORTPOD_NAME,   " + "       TO_CHAR(main.VALID_FROM,DATEFORMAT) VALID_FROM,                                                 " + "       TO_CHAR(main.VALID_TO,DATEFORMAT) VALID_TO                                                    " + "      from                                                         " + "       RFQ_TRN_AIR_LCL                 main,                       " + "       PORT_MST_TBL                    PORTPOL,                    " + "       PORT_MST_TBL                    PORTPOD                     " + "      where  RFQ_MAIN_AIR_FK       = " + rfqPK + "                " + "       AND PORT_MST_POL_FK         =   PORTPOL.PORT_MST_PK         " + "       AND PORT_MST_POD_FK         =   PORTPOD.PORT_MST_PK         ";

                DataTable ChildDt = null;
                ChildDt = objWF.GetDataTable(strSQL);

                // Adding KGFreights Columns [ Charge Basis = 3 ]
                DataTable KGFrtDt = null;

                strSQL = "Select" + "      main.RFQ_trn_air_pk," + "      tran.RFQ_air_surcharge_pk,          " + "      tran.FREIGHT_ELEMENT_MST_FK,      " + "      FREIGHT_ELEMENT_ID || '(' || decode(tran.charge_basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') || ')' FREIGHT_ELEMENT_ID ," + "      FREIGHT_ELEMENT_NAME,       " + "      tran.CURRENCY_MST_FK,       " + "      CURRENCY_ID,        " + "      CURRENCY_NAME,        " + "      tran.current_rate,   " + "      tran.requested_rate   " + "from          " + "      RFQ_TRN_AIR_LCL main,      " + "      RFQ_AIR_SURCHARGE tran,     " + "      FREIGHT_ELEMENT_MST_TBL,      " + "      CURRENCY_TYPE_MST_TBL       " + "where RFQ_MAIN_AIR_FK = " + rfqPK + "      AND  RFQ_TRN_AIR_FK = main.RFQ_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID  ";

                KGFrtDt = objWF.GetDataTable(strSQL);
                string KGFreights = null;

                KGFreights = getStrFreights(KGFrtDt);

                AddColumnsFreights(ChildDt, KGFreights);
                // KGFreights Columns

                TransferKGFreightsData(ChildDt, KGFrtDt);
                //    KGFreights Data in ChildDt now..

                ChildDt.Columns.Add("Other_Charges");
                ChildDt.Columns.Add("OtherCharges");

                // Other Charge Information need to be fetched======================================Importent
                // Child Records FOR Freights..>

                strSQL = "Select" + "      tran.RFQ_trn_air_fk," + "      frt.freight_element_mst_pk," + "      curr.currency_mst_pk," + "      tran.charge_basis," + "      tran.current_rate," + "      tran.requested_rate" + "from" + "      RFQ_TRN_AIR_LCL  main,      " + "      RFQ_AIR_OTH_CHRG  tran,     " + "      FREIGHT_ELEMENT_MST_TBL frt, " + "      CURRENCY_TYPE_MST_TBL curr      " + "where RFQ_MAIN_AIR_FK  = " + rfqPK + "      AND  RFQ_TRN_AIR_FK  = main.RFQ_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID ";

                otherCharges = objWF.GetDataTable(strSQL);

                TransferOtherFreightsData(ChildDt, otherCharges);

                strSQL = "Select" + "      tran.rfq_trn_air_fk," + "      tran.rfq_air_freight_pk,           " + "      main.port_mst_pol_fk,           " + "      main.port_mst_pod_fk,           " + "      FREIGHT_ELEMENT_MST_FK,       " + "      FREIGHT_ELEMENT_ID,        " + "      FREIGHT_ELEMENT_NAME,       " + "      tran.CURRENCY_MST_FK,              " + "      MIN_AMOUNT " + "FROM" + "      rfq_TRN_AIR_LCL     main,   " + "      rfq_AIR_FREIGHT_TBL    tran,   " + "      FREIGHT_ELEMENT_MST_TBL,      " + "      CURRENCY_TYPE_MST_TBL" + "WHERE rfq_MAIN_AIR_FK = " + rfqPK + "      AND  rfq_TRN_AIR_FK  = main.rfq_TRN_AIR_PK " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "      Order By FREIGHT_ELEMENT_ID    ";

                DataTable GRFrtDt = null;
                GRFrtDt = objWF.GetDataTable(strSQL);

                strSQL = "SELECT" + "      AIRFREIGHT_SLABS_TBL_PK," + "      BPNT.rfq_AIR_FREIGHT_FK,               " + "      SLAB.BREAKPOINT_ID,        " + "      BREAKPOINT_DESC,        " + "      CURRENT_RATE,   " + "      REQUESTED_RATE   " + "FROM" + "      RFQ_TRN_AIR_LCL    MAIN,    " + "      RFQ_AIR_FREIGHT_TBL   TRAN,    " + "      RFQ_AIR_BREAKPOINTS   BPNT,    " + "      AIRFREIGHT_SLABS_TBL SLAB" + "WHERE RFQ_MAIN_AIR_FK = " + rfqPK + "      AND TRAN.RFQ_TRN_AIR_FK  = MAIN.RFQ_TRN_AIR_PK  " + "      AND BPNT.RFQ_AIR_FREIGHT_FK  = TRAN.RFQ_AIR_FREIGHT_PK   " + "      AND BPNT.AIRFREIGHT_SLABS_FK  = AIRFREIGHT_SLABS_TBL_PK  " + "ORDER BY SEQUENCE_NO";

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
                DataRelation REL = new DataRelation("RFQRelation", new DataColumn[] { GridDS.Tables[0].Columns["rfq_trn_air_pk"] }, new DataColumn[] { GridDS.Tables[1].Columns["rfq_trn_air_fk"] });
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
                    drArray = FrtDt.Select(" rfq_trn_air_fk = " + GridDt.Rows[RowCnt]["rfq_trn_air_pk"]);
                    strOtherCharges = "";
                    for (frtNumber = 0; frtNumber <= drArray.Length - 1; frtNumber++)
                    {
                        strOtherCharges = strOtherCharges + drArray[frtNumber]["freight_element_mst_pk"] + "~" + drArray[frtNumber]["currency_mst_pk"] + "~" + drArray[frtNumber]["charge_basis"] + "~" + drArray[frtNumber]["current_rate"] + "~" + drArray[frtNumber]["requested_rate"] + "^";
                        //drArray(frtNumber).Item("approved_rate") & "^"
                    }

                    if (!string.IsNullOrEmpty(strOtherCharges))
                    {
                        GridDt.Rows[RowCnt][GridDt.Columns.Count - 1] = strOtherCharges;
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
                    drArray = FrtDt.Select(" rfq_trn_air_pk = " + GridDt.Rows[RowCnt]["rfq_trn_air_pk"]);

                    for (frtNumber = 0; frtNumber <= drArray.Length - 1; frtNumber++)
                    {
                        for (ColCnt = 9; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(drArray[frtNumber]["FREIGHT_ELEMENT_MST_FK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[RowCnt][ColCnt] = drArray[frtNumber]["CURRENT_RATE"];
                                //GridDt.Rows(RowCnt).Item(ColCnt + 1) = drArray(frtNumber).Item("APPROVED_RATE")
                                GridDt.Rows[RowCnt][ColCnt + 1] = drArray[frtNumber]["REQUESTED_RATE"];
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
                    drArray = SlabDt.Select(" rfq_air_freight_fk = " + GridDt.Rows[SRowCnt]["rfq_air_freight_pk"]);

                    for (GRowCnt = 0; GRowCnt <= drArray.Length - 1; GRowCnt++)
                    {
                        for (ColCnt = 9; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(drArray[GRowCnt]["AIRFREIGHT_SLABS_TBL_PK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[SRowCnt][ColCnt] = drArray[GRowCnt]["CURRENT_RATE"];
                                GridDt.Rows[SRowCnt][ColCnt + 1] = drArray[GRowCnt]["REQUESTED_RATE"];
                                //GridDt.Rows(SRowCnt).Item(ColCnt + 1) = drArray(GRowCnt).Item("APPROVED_RATE")
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
            try
            {
                Array CHeads = null;
                string hed = null;
                CHeads = FRTs.Split(',');
                Int16 i = default(Int16);
                for (i = 0; i <= CHeads.Length - 2; i += 2)
                {
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 1)), typeof(decimal));
                    //DT.Columns.Add(CStr(CHeads.GetValue(i + 2)), GetType(Decimal))
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
            try
            {
                Array CHeads = null;
                string hed = null;
                CHeads = FRTs.Split(',');
                Int16 i = default(Int16);
                for (i = 0; i <= CHeads.Length - 2; i += 2)
                {
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 1)), typeof(decimal));
                    // DT.Columns.Add(CStr(CHeads.GetValue(i + 2)), GetType(Decimal))
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
                        //strBuilder.Append(CStr(removeDBNull(DT.Rows(RowCnt).Item("FREIGHT_ELEMENT_NAME"))).Trim + ".,")
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
                            // strBuilder.Append(CStr(removeDBNull(DT.Rows(RowCnt).Item("BREAKPOINT_DESC"))).Trim + ".,")
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

        #endregion " Fetch Data from airrfq table "

        #region "Fetch RFQ"

        //This function fetch the RFQ from the database against the supplied RFQ Pk
        /// <summary>
        /// Fetches the RFQ.
        /// </summary>
        /// <param name="nRFQPk">The n RFQ pk.</param>
        /// <returns></returns>
        public DataSet FetchRFQ(long nRFQPk)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT AIR.AIRLINE_MST_PK,AIR.AIRLINE_ID,AIR.AIRLINE_NAME,  " + "RFQHDR.RFQ_REF_NO,RFQHDR.RFQ_DATE, " + "RFQHDR.COMMODITY_MST_FK, RFQHDR.VALID_FROM,  " + "RFQHDR.VALID_TO,RFQHDR.VERSION_NO,  " + "RFQTRN.PORT_MST_POL_FK,RFQTRN.PORT_MST_POD_FK,RFQTRN.CHECK_FOR_ALL_IN_RT,  " + "RFQTRN.FREIGHT_ELEMENT_MST_FK,RFQTRN.CURRENCY_MST_FK,CURR.CURRENCY_ID,  " + "TO_CHAR(RFQTRN.VALID_FROM,'" + dateFormat + "') AS P_VALID_FROM,  " + "TO_CHAR(RFQTRN.VALID_TO,'" + dateFormat + "') AS P_VALID_TO,  " + "RFQTRN.LCL_BASIS,RFQTRN.LCL_CURRENT_RATE,  " + "RFQTRN.LCL_REQUEST_RATE, DMT.DIMENTION_ID " + "FROM AIRLINE_MST_TBL AIR,  " + "RFQ_TRN_AIR_LCL RFQTRN,RFQ_MAIN_AIR_TBL RFQHDR,  " + "CURRENCY_TYPE_MST_TBL CURR,DIMENTION_UNIT_MST_TBL DMT  " + "WHERE RFQHDR.RFQ_MAIN_AIR_PK = RFQTRN.RFQ_MAIN_AIR_FK " + "AND AIR.AIRLINE_MST_PK = RFQHDR.AIRLINE_MST_FK " + "AND CURR.CURRENCY_MST_PK = RFQTRN.CURRENCY_MST_FK  " + "AND DMT.DIMENTION_UNIT_MST_PK = RFQTRN.LCL_BASIS  " + "AND RFQHDR.RFQ_MAIN_AIR_PK =" + nRFQPk;
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

        #endregion "Fetch RFQ"

        #region " Fetch Max RFQ No."

        /// <summary>
        /// Fetches the RFQ no.
        /// </summary>
        /// <param name="strRFQNo">The string RFQ no.</param>
        /// <returns></returns>
        public string FetchRFQNo(string strRFQNo)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT NVL(MAX(T.RFQ_REF_NO),0) FROM RFQ_MAIN_AIR_TBL T " + "WHERE T.RFQ_REF_NO LIKE '" + strRFQNo + "/%' " + "ORDER BY T.RFQ_REF_NO";
                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        #endregion " Fetch Max RFQ No."

        #region "Fetch Contract"

        /// <summary>
        /// Fetches the contract.
        /// </summary>
        /// <param name="AirlineFk">The airline fk.</param>
        /// <param name="CommodityGrpFk">The commodity GRP fk.</param>
        /// <param name="POLPk">The pol pk.</param>
        /// <param name="PODPk">The pod pk.</param>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="strTFDate">The string tf date.</param>
        /// <param name="strToDate">The string to date.</param>
        public void FetchContract(long AirlineFk, long CommodityGrpFk, string POLPk, string PODPk, DataSet dsMain, string strTFDate = "", string strToDate = "")
        {
            try
            {
                System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
                // to hold the SQL statement
                Int16 i = default(Int16);
                Int16 j = default(Int16);
                Int16 colCount = default(Int16);
                // Loop index
                Array arrPolPk = null;
                // Array to hold the POL
                Array arrPodPk = null;
                // Array to hold the POD
                string contractPK = "";
                OracleDataReader oraReader = null;
                WorkFlow objWF = new WorkFlow();
                string strOtherCharges = "";

                DataTable dtOtherCharge = new DataTable();
                DataTable dtBreakPoints = new DataTable();
                DataTable dtSurCharges = new DataTable();
                DataRow[] drArray = null;
                string strSQL1 = null;
                // It holds the POL and POD in the form (POL1,POD1),(POL2,POD2) etc
                string strPOLPOD = "";
                DataTable dtOperatorCharges = null;

                DataTable dtMinimumAmount = new DataTable();

                //Spliting the POL and POD Pk's
                arrPolPk = POLPk.Split(',');
                arrPodPk = PODPk.Split(',');

                for (i = 0; i <= arrPolPk.Length - 1; i++)
                {
                    if (string.IsNullOrEmpty(strPOLPOD))
                    {
                        strPOLPOD = "(" + arrPolPk.GetValue(i) + "," + arrPodPk.GetValue(i) + ")";
                    }
                    else
                    {
                        strPOLPOD = strPOLPOD + "," + "(" + arrPolPk.GetValue(i) + "," + arrPodPk.GetValue(i) + ")";
                    }
                }

                strSQL1 = "SELECT FR.FREIGHT_ELEMENT_MST_PK," + "       FR.FREIGHT_ELEMENT_ID || '(' || decode(SUR.CHARGE_BASIS,1,'%',2,'Flat',3,'Kgs',4,'Unit') || ')'," + "       FR.FREIGHT_ELEMENT_NAME,sur.basis_value" + "  FROM FREIGHT_ELEMENT_MST_TBL FR,airline_mst_tbl airline,airline_surcharges_tbl sur" + " WHERE FR.CHARGE_Type = 2" + "   and airline.airline_mst_pk =" + AirlineFk + "   and sur.freight_element_mst_fk = fr.freight_element_mst_pk" + "   AND SUR.AIRLINE_MST_FK = airline.airline_mst_pk" + " ORDER BY FR.FREIGHT_ELEMENT_ID";

                dtOperatorCharges = objWF.GetDataTable(strSQL1);
                //TransferKGFreightsData(GridDS.Tables(0), KGFrtDt) '    KGFreights Data in ChildDt now..
                for (i = 0; i <= dsMain.Tables[0].Rows.Count - 1; i++)
                {
                    for (colCount = 9; colCount <= dsMain.Tables[0].Columns.Count - 3; colCount += 2)
                    {
                        for (j = 0; j <= dtOperatorCharges.Rows.Count - 1; j++)
                        {
                            if (dsMain.Tables[0].Columns[colCount].Caption == dtOperatorCharges.Rows[j]["FREIGHT_ELEMENT_MST_PK"])
                            {
                                dsMain.Tables[0].Rows[i][colCount] = dtOperatorCharges.Rows[j]["basis_value"];
                                dsMain.Tables[0].Rows[i][colCount + 1] = dtOperatorCharges.Rows[j]["basis_value"];
                            }
                        }
                    }
                }

                strSQL.Append(" SELECT MAX(MAIN_CONT.CONT_MAIN_AIR_PK),");
                strSQL.Append("     CONT_TRN.PORT_MST_POL_FK,");
                strSQL.Append("     CONT_TRN.PORT_MST_POD_FK,");
                strSQL.Append("     MAX(MAIN_CONT.VALID_FROM)");
                strSQL.Append(" FROM CONT_MAIN_AIR_TBL       MAIN_CONT,");
                strSQL.Append("     CONT_TRN_AIR_LCL        CONT_TRN,");
                strSQL.Append("     AIRLINE_MST_TBL         AIRLINE,");
                strSQL.Append("     COMMODITY_GROUP_MST_TBL COMM");
                strSQL.Append(" WHERE MAIN_CONT.ACTIVE = 1");
                //check for active contract
                strSQL.Append("     AND MAIN_CONT.CONT_APPROVED = 1");
                //check for approved contract
                strSQL.Append("     AND MAIN_CONT.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK(+)");
                strSQL.Append("     AND MAIN_CONT.AIRLINE_MST_FK = AIRLINE.AIRLINE_MST_PK(+)");
                strSQL.Append("     AND MAIN_CONT.AIRLINE_MST_FK = " + AirlineFk.ToString());
                // check for particular airline
                strSQL.Append("     AND CONT_TRN.CONT_MAIN_AIR_FK = MAIN_CONT.CONT_MAIN_AIR_PK ");
                strSQL.Append("     AND TO_CHAR(MAIN_CONT.VALID_TO,DATEFORMAT) >= TO_CHAR(SYSDATE,DATEFORMAT) ");
                strSQL.Append("     AND (CONT_TRN.PORT_MST_POL_FK, CONT_TRN.PORT_MST_POD_FK) IN");
                strSQL.Append("     (" + strPOLPOD + ")");
                strSQL.Append("     AND MAIN_CONT.COMMODITY_GROUP_FK = " + CommodityGrpFk.ToString());
                strSQL.Append(" GROUP BY CONT_TRN.PORT_MST_POL_FK,");
                strSQL.Append("     CONT_TRN.PORT_MST_POD_FK");

                oraReader = objWF.GetDataReader(strSQL.ToString());
                while (oraReader.Read())
                {
                    if (string.IsNullOrEmpty(contractPK))
                    {
                        contractPK = Convert.ToString(oraReader[0]);
                    }
                    else
                    {
                        contractPK = contractPK + "," + oraReader[0];
                    }
                }

                strSQL = new System.Text.StringBuilder();

                if (!string.IsNullOrEmpty(contractPK))
                {
                    strSQL.Append(" SELECT DISTINCT MAIN.PORT_MST_POL_FK,");
                    strSQL.Append("     MAIN.PORT_MST_POD_FK,");
                    strSQL.Append("     TRAN.FREIGHT_ELEMENT_MST_FK,");
                    strSQL.Append("     TRAN.APPROVED_RATE");
                    strSQL.Append(" FROM CONT_TRN_AIR_LCL MAIN,");
                    strSQL.Append("     CONT_AIR_SURCHARGE TRAN,");
                    strSQL.Append("     FREIGHT_ELEMENT_MST_TBL,");
                    strSQL.Append("     CURRENCY_TYPE_MST_TBL");
                    strSQL.Append(" WHERE CONT_MAIN_AIR_FK IN (" + contractPK + ")");
                    strSQL.Append("     AND CONT_TRN_AIR_FK = MAIN.CONT_TRN_AIR_PK");
                    strSQL.Append("     AND FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK");
                    strSQL.Append("     AND (MAIN.PORT_MST_POL_FK, MAIN.PORT_MST_POD_FK) IN");
                    strSQL.Append("     (" + strPOLPOD + ")");
                    strSQL.Append(" ORDER BY MAIN.PORT_MST_POL_FK, MAIN.PORT_MST_POD_FK");

                    dtSurCharges = objWF.GetDataTable(strSQL.ToString());

                    for (i = 0; i <= dsMain.Tables[0].Rows.Count - 1; i++)
                    {
                        drArray = dtSurCharges.Select("PORT_MST_POL_FK=" + dsMain.Tables[0].Rows[i]["POLPK"] + " AND PORT_MST_POD_FK=" + dsMain.Tables[0].Rows[i]["PODPK"]);
                        for (colCount = 9; colCount <= dsMain.Tables[0].Columns.Count - 3; colCount += 2)
                        {
                            for (j = 0; j <= drArray.Length - 1; j++)
                            {
                                if (dsMain.Tables[0].Columns[colCount].Caption == drArray[j]["FREIGHT_ELEMENT_MST_FK"])
                                {
                                    dsMain.Tables[0].Rows[i][colCount] = drArray[j]["APPROVED_RATE"];
                                    dsMain.Tables[0].Rows[i][colCount + 1] = drArray[j]["APPROVED_RATE"];
                                }
                            }
                        }
                    }

                    strSQL = new System.Text.StringBuilder();

                    strSQL.Append(" SELECT DISTINCT MAIN.PORT_MST_POL_FK,");
                    strSQL.Append("     MAIN.PORT_MST_POD_FK,");
                    strSQL.Append("     TRAN.FREIGHT_ELEMENT_MST_FK,");
                    strSQL.Append("     BPNT.AIRFREIGHT_SLABS_FK,");
                    strSQL.Append("     APPROVED_RATE,");
                    strSQL.Append("     TRAN.CURRENCY_MST_FK");
                    strSQL.Append(" FROM CONT_TRN_AIR_LCL     MAIN,");
                    strSQL.Append("     CONT_AIR_FREIGHT_TBL TRAN,");
                    strSQL.Append("     CONT_AIR_BREAKPOINTS BPNT,");
                    strSQL.Append("     AIRFREIGHT_SLABS_TBL SLAB ");
                    strSQL.Append(" WHERE CONT_MAIN_AIR_FK IN (" + contractPK + ")");
                    strSQL.Append("     AND TRAN.CONT_TRN_AIR_FK = MAIN.CONT_TRN_AIR_PK");
                    strSQL.Append("     AND BPNT.CONT_AIR_FREIGHT_FK =");
                    strSQL.Append("     TRAN.CONT_AIR_FREIGHT_PK");
                    strSQL.Append("     AND BPNT.AIRFREIGHT_SLABS_FK =");
                    strSQL.Append("     AIRFREIGHT_SLABS_TBL_PK");
                    strSQL.Append("     AND (MAIN.PORT_MST_POL_FK, MAIN.PORT_MST_POD_FK) IN");
                    strSQL.Append("     (" + strPOLPOD + ")");
                    if (!string.IsNullOrEmpty(strTFDate.ToString()))
                    {
                        strSQL.Append("  AND  TO_DATE(MAIN.VALID_TO,DATEFORMAT) >= TO_DATE('" + strTFDate.ToString() + "',DATEFORMAT) ");
                    }
                    strSQL.Append(" ORDER BY MAIN.PORT_MST_POL_FK,");
                    strSQL.Append("     MAIN.PORT_MST_POD_FK,");
                    strSQL.Append("     TRAN.FREIGHT_ELEMENT_MST_FK");

                    dtBreakPoints = objWF.GetDataTable(strSQL.ToString());

                    for (i = 0; i <= dsMain.Tables[1].Rows.Count - 1; i++)
                    {
                        drArray = dtBreakPoints.Select("PORT_MST_POL_FK=" + dsMain.Tables[1].Rows[i]["POLPK"] + " AND PORT_MST_POD_FK=" + dsMain.Tables[1].Rows[i]["PODPK"] + " AND FREIGHT_ELEMENT_MST_FK=" + dsMain.Tables[1].Rows[i]["FREIGHT_ELEMENT_MST_PK"]);

                        for (colCount = 9; colCount <= dsMain.Tables[1].Columns.Count - 2; colCount += 2)
                        {
                            for (j = 0; j <= drArray.Length - 1; j++)
                            {
                                if (dsMain.Tables[1].Columns[colCount].Caption == drArray[j]["AIRFREIGHT_SLABS_FK"])
                                {
                                    dsMain.Tables[1].Rows[i][7] = drArray[j]["CURRENCY_MST_FK"];
                                    //currency
                                    dsMain.Tables[1].Rows[i][colCount] = drArray[j]["APPROVED_RATE"];
                                    dsMain.Tables[1].Rows[i][colCount + 1] = drArray[j]["APPROVED_RATE"];
                                }
                            }
                        }
                    }

                    strSQL = new System.Text.StringBuilder();

                    strSQL.Append(" SELECT DISTINCT MAIN.PORT_MST_POL_FK,");
                    strSQL.Append("     MAIN.PORT_MST_POD_FK,");
                    strSQL.Append("     FRT.FREIGHT_ELEMENT_MST_PK,");
                    strSQL.Append("     TRAN.CURRENCY_MST_FK,");
                    strSQL.Append("     TRAN.CHARGE_BASIS,");
                    strSQL.Append("     nvl(TRAN.APPROVED_RATE,0) APPROVED_RATE");
                    strSQL.Append(" FROM CONT_TRN_AIR_LCL        MAIN,");
                    strSQL.Append("     CONT_AIR_OTH_CHRG       TRAN,");
                    strSQL.Append("     FREIGHT_ELEMENT_MST_TBL FRT");
                    strSQL.Append(" WHERE CONT_MAIN_AIR_FK IN (" + contractPK + ")");
                    strSQL.Append("     AND CONT_TRN_AIR_FK = MAIN.CONT_TRN_AIR_PK");
                    strSQL.Append("     AND FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK");
                    strSQL.Append("     AND (MAIN.PORT_MST_POL_FK, MAIN.PORT_MST_POD_FK) IN");
                    strSQL.Append("     (" + strPOLPOD + ")");

                    dtOtherCharge = objWF.GetDataTable(strSQL.ToString());

                    for (i = 0; i <= dsMain.Tables[0].Rows.Count - 1; i++)
                    {
                        strOtherCharges = "";
                        drArray = dtOtherCharge.Select("PORT_MST_POL_FK=" + dsMain.Tables[0].Rows[i]["POLPK"] + " AND PORT_MST_POD_FK=" + dsMain.Tables[0].Rows[i]["PODPK"]);
                        for (j = 0; j <= drArray.Length - 1; j++)
                        {
                            strOtherCharges = strOtherCharges + drArray[j]["FREIGHT_ELEMENT_MST_PK"].ToString() + "~" + drArray[j]["CURRENCY_MST_FK"].ToString() + "~" + drArray[j]["CHARGE_BASIS"].ToString() + "~" + drArray[j]["APPROVED_RATE"].ToString() + "~" + drArray[j]["APPROVED_RATE"].ToString() + "^";
                        }
                        dsMain.Tables[0].Rows[i][dsMain.Tables[0].Columns.Count - 1] = (string.IsNullOrEmpty(strOtherCharges) ? "" : strOtherCharges);
                    }

                    strSQL1 = " select main.port_mst_pol_fk," + "      main.port_mst_pod_fk," + "      tran.freight_element_mst_fk," + "      tran.min_amount" + " from cont_trn_air_lcl main," + "      cont_air_freight_tbl tran," + "      freight_element_mst_tbl," + "      currency_type_mst_tbl" + " where cont_main_air_fk in (" + contractPK + ")" + "      and cont_trn_air_fk = main.cont_trn_air_pk" + "      and freight_element_mst_fk = freight_element_mst_pk" + "      and currency_mst_fk = currency_mst_pk" + " order by freight_element_id";

                    dtMinimumAmount = objWF.GetDataTable(strSQL1);

                    for (i = 0; i <= dsMain.Tables[1].Rows.Count - 1; i++)
                    {
                        drArray = dtMinimumAmount.Select("PORT_MST_POL_FK=" + dsMain.Tables[1].Rows[i]["POLPK"] + " AND PORT_MST_POD_FK=" + dsMain.Tables[1].Rows[i]["PODPK"] + " AND FREIGHT_ELEMENT_MST_FK=" + dsMain.Tables[1].Rows[i]["FREIGHT_ELEMENT_MST_PK"]);
                        if (drArray.Length == 1)
                        {
                            dsMain.Tables[1].Rows[i][8] = drArray[0]["min_amount"];
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

        #endregion "Fetch Contract"

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

        #region "FetchMainData"

        /// <summary>
        /// Fetches the one copy.
        /// </summary>
        /// <param name="GridDS">The grid ds.</param>
        /// <param name="rfqPK">The RFQ pk.</param>
        /// <param name="airlinePK">The airline pk.</param>
        /// <returns></returns>
        public DataSet FetchOneCopy(DataSet GridDS, string rfqPK = "", string airlinePK = "")
        {
            try
            {
                string ContainerPKs = null;
                bool NewRecord = true;
                //If rfqSpotRatePK.Trim.Length > 0 Then NewRecord = False
                string strSQL = null;
                // to hold the SQL statement
                Int16 i = default(Int16);
                Int16 j = default(Int16);
                Int16 colCount = default(Int16);
                // Loop index
                Array arrPolPk = null;
                // Array to hold the POL
                Array arrPodPk = null;
                // Array to hold the POD
                string contractPK = "";
                OracleDataReader oraReader = null;
                WorkFlow objWF = new WorkFlow();
                string strOtherCharges = "";

                DataTable dtOtherCharge = new DataTable();
                DataTable dtBreakPoints = new DataTable();
                DataTable dtSurCharges = new DataTable();
                DataTable dtOperatorCharges = new DataTable();
                DataTable dtMinimumAmount = new DataTable();

                DataRow[] drArray = null;

                DataTable otherCharges = new DataTable();

                // Adding KGFreights Columns [ Charge Basis = 3 ]
                DataTable KGFrtDt = null;

                strSQL = "SELECT FR.FREIGHT_ELEMENT_MST_PK," + "       FR.FREIGHT_ELEMENT_ID || '(' || decode(SUR.CHARGE_BASIS,1,'%',2,'Flat',3,'Kgs',4,'Unit') || ')'," + "       FR.FREIGHT_ELEMENT_NAME,sur.basis_value" + "  FROM FREIGHT_ELEMENT_MST_TBL FR,airline_mst_tbl airline,airline_surcharges_tbl sur" + " WHERE FR.CHARGE_Type = 2" + "   and airline.airline_mst_pk =" + airlinePK + "   and sur.freight_element_mst_fk = fr.freight_element_mst_pk" + "   AND SUR.AIRLINE_MST_FK = airline.airline_mst_pk" + " ORDER BY FR.FREIGHT_ELEMENT_ID";

                dtOperatorCharges = objWF.GetDataTable(strSQL);
                //TransferKGFreightsData(GridDS.Tables(0), KGFrtDt) '    KGFreights Data in ChildDt now..
                for (i = 0; i <= GridDS.Tables[0].Rows.Count - 1; i++)
                {
                    for (colCount = 9; colCount <= GridDS.Tables[0].Columns.Count - 3; colCount += 2)
                    {
                        for (j = 0; j <= dtOperatorCharges.Rows.Count - 1; j++)
                        {
                            if (GridDS.Tables[0].Columns[colCount].Caption == dtOperatorCharges.Rows[j]["FREIGHT_ELEMENT_MST_PK"])
                            {
                                GridDS.Tables[0].Rows[i][colCount] = dtOperatorCharges.Rows[j]["basis_value"];
                                GridDS.Tables[0].Rows[i][colCount + 1] = dtOperatorCharges.Rows[j]["basis_value"];
                            }
                        }
                    }
                }

                strSQL = " Select main.port_mst_pol_fk," + "      main.port_mst_pod_fk," + "      tran.FREIGHT_ELEMENT_MST_FK," + "      tran.min_amount" + " from RFQ_TRN_AIR_LCL main," + "      Rfq_Air_Freight_Tbl tran," + "      FREIGHT_ELEMENT_MST_TBL," + "      CURRENCY_TYPE_MST_TBL" + " where RFQ_MAIN_AIR_FK = " + rfqPK + "      AND RFQ_TRN_AIR_FK = main.RFQ_TRN_AIR_PK" + "      AND FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK" + "      AND CURRENCY_MST_FK = CURRENCY_MST_PK" + " Order By FREIGHT_ELEMENT_ID";

                dtMinimumAmount = objWF.GetDataTable(strSQL);

                for (i = 0; i <= GridDS.Tables[1].Rows.Count - 1; i++)
                {
                    drArray = dtMinimumAmount.Select("PORT_MST_POL_FK=" + GridDS.Tables[1].Rows[i]["POLPK"] + " AND PORT_MST_POD_FK=" + GridDS.Tables[1].Rows[i]["PODPK"] + " AND FREIGHT_ELEMENT_MST_FK=" + GridDS.Tables[1].Rows[i]["FREIGHT_ELEMENT_MST_PK"]);
                    if (drArray.Length == 1)
                    {
                        GridDS.Tables[1].Rows[i][8] = drArray[0]["min_amount"];
                    }
                }

                strSQL = "Select" + "       main.port_mst_pol_fk," + "       main.port_mst_pod_fk,          " + "      tran.FREIGHT_ELEMENT_MST_FK,      " + "      FREIGHT_ELEMENT_ID || '(' || decode(tran.charge_basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') || ')' FREIGHT_ELEMENT_ID ," + "      FREIGHT_ELEMENT_NAME,       " + "      tran.current_rate,   " + "      tran.requested_rate   " + "from          " + "      RFQ_TRN_AIR_LCL main,      " + "      RFQ_AIR_SURCHARGE tran,     " + "      FREIGHT_ELEMENT_MST_TBL,      " + "      CURRENCY_TYPE_MST_TBL       " + "where RFQ_MAIN_AIR_FK = " + rfqPK + "      AND  RFQ_TRN_AIR_FK = main.RFQ_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID  ";

                KGFrtDt = objWF.GetDataTable(strSQL);
                string KGFreights = null;

                //TransferKGFreightsData(GridDS.Tables(0), KGFrtDt) '    KGFreights Data in ChildDt now..
                for (i = 0; i <= GridDS.Tables[0].Rows.Count - 1; i++)
                {
                    drArray = KGFrtDt.Select("PORT_MST_POL_FK=" + GridDS.Tables[0].Rows[i]["POLPK"] + " AND PORT_MST_POD_FK=" + GridDS.Tables[0].Rows[i]["PODPK"]);
                    for (colCount = 9; colCount <= GridDS.Tables[0].Columns.Count - 3; colCount += 2)
                    {
                        for (j = 0; j <= drArray.Length - 1; j++)
                        {
                            if (GridDS.Tables[0].Columns[colCount].Caption == drArray[j]["FREIGHT_ELEMENT_MST_FK"])
                            {
                                GridDS.Tables[0].Rows[i][colCount] = drArray[j]["CURRENT_RATE"];
                                GridDS.Tables[0].Rows[i][colCount + 1] = drArray[j]["REQUESTED_RATE"];
                            }
                        }
                    }
                }

                // Other Charge Information need to be fetched======================================Importent
                // Child Records FOR Freights..>

                strSQL = "Select" + "      main.port_mst_pol_fk," + "      main.port_mst_pod_fk," + "      frt.freight_element_mst_pk," + "      curr.currency_mst_pk," + "      tran.charge_basis," + "      tran.current_rate," + "      tran.requested_rate" + "from" + "      RFQ_TRN_AIR_LCL  main,      " + "      RFQ_AIR_OTH_CHRG  tran,     " + "      FREIGHT_ELEMENT_MST_TBL frt, " + "      CURRENCY_TYPE_MST_TBL curr      " + "where RFQ_MAIN_AIR_FK  = " + rfqPK + "      AND  RFQ_TRN_AIR_FK  = main.RFQ_TRN_AIR_PK  " + "      AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " + "      AND  CURRENCY_MST_FK  = CURRENCY_MST_PK   " + "Order By FREIGHT_ELEMENT_ID ";

                otherCharges = objWF.GetDataTable(strSQL);

                //TransferOtherFreightsData(GridDS.Tables(0), otherCharges)

                for (i = 0; i <= GridDS.Tables[0].Rows.Count - 1; i++)
                {
                    strOtherCharges = "";
                    drArray = otherCharges.Select("PORT_MST_POL_FK=" + GridDS.Tables[0].Rows[i]["POLPK"] + " AND PORT_MST_POD_FK=" + GridDS.Tables[0].Rows[i]["PODPK"]);
                    for (j = 0; j <= drArray.Length - 1; j++)
                    {
                        strOtherCharges = strOtherCharges + drArray[j]["FREIGHT_ELEMENT_MST_PK"].ToString() + "~" + drArray[j]["currency_mst_pk"].ToString() + "~" + drArray[j]["CHARGE_BASIS"].ToString() + "~" + drArray[j]["current_RATE"].ToString() + "~" + drArray[j]["requested_RATE"].ToString() + "^";
                    }
                    GridDS.Tables[0].Rows[i][GridDS.Tables[0].Columns.Count - 1] = (string.IsNullOrEmpty(strOtherCharges) ? "" : strOtherCharges);
                }

                strSQL = "SELECT" + "      main.port_mst_pol_fk," + "      main.port_mst_pod_fk,               " + "      tran.freight_element_mst_fk,               " + "      tran.currency_mst_fk,               " + "      airfreight_slabs_tbl_pk,               " + "      SLAB.BREAKPOINT_ID,        " + "      CURRENT_RATE,   " + "      REQUESTED_RATE   " + "FROM" + "      RFQ_TRN_AIR_LCL    MAIN,    " + "      RFQ_AIR_FREIGHT_TBL   TRAN,    " + "      RFQ_AIR_BREAKPOINTS   BPNT,    " + "      AIRFREIGHT_SLABS_TBL SLAB" + "WHERE RFQ_MAIN_AIR_FK = " + rfqPK + "      AND TRAN.RFQ_TRN_AIR_FK  = MAIN.RFQ_TRN_AIR_PK  " + "      AND BPNT.RFQ_AIR_FREIGHT_FK  = TRAN.RFQ_AIR_FREIGHT_PK   " + "      AND BPNT.AIRFREIGHT_SLABS_FK  = AIRFREIGHT_SLABS_TBL_PK  " + "ORDER BY SEQUENCE_NO";

                DataTable SlbDt = null;
                SlbDt = objWF.GetDataTable(strSQL);

                for (i = 0; i <= GridDS.Tables[1].Rows.Count - 1; i++)
                {
                    drArray = SlbDt.Select("PORT_MST_POL_FK=" + GridDS.Tables[1].Rows[i]["POLPK"] + " AND PORT_MST_POD_FK=" + GridDS.Tables[1].Rows[i]["PODPK"] + " AND FREIGHT_ELEMENT_MST_FK=" + GridDS.Tables[1].Rows[i]["FREIGHT_ELEMENT_MST_PK"]);

                    for (colCount = 9; colCount <= GridDS.Tables[1].Columns.Count - 2; colCount += 2)
                    {
                        for (j = 0; j <= drArray.Length - 1; j++)
                        {
                            if (GridDS.Tables[1].Columns[colCount].Caption == drArray[j]["airfreight_slabs_tbl_pk"])
                            {
                                GridDS.Tables[1].Rows[i][7] = drArray[j]["CURRENCY_MST_FK"];
                                //currency
                                GridDS.Tables[1].Rows[i][colCount] = drArray[j]["CURRENT_RATE"];
                                GridDS.Tables[1].Rows[i][colCount + 1] = drArray[j]["REQUESTED_RATE"];
                            }
                        }
                    }
                }

                return GridDS;
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
    }
}