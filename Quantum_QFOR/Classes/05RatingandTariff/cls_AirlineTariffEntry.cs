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
    public class cls_AirlineTariffEntry : CommonFeatures
    {
        #region "Private Variables"

        /// <summary>
        /// The _ pk value
        /// </summary>
        private long _PkValue;

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

        /// <summary>
        /// The _ use_ extra_ cols
        /// </summary>
        private bool _Use_Extra_Cols;

        #endregion "Private Variables"

        /// <summary>
        /// The _ air line_ tariff_ cols
        /// </summary>
        private const int _AirLine_Tariff_Cols = 7;

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

        //
        /// <summary>
        /// Initializes a new instance of the <see cref="cls_AirlineTariffEntry"/> class.
        /// </summary>
        /// <param name="Static_Col">The static_ col.</param>
        /// <param name="Col_Increment">The col_ increment.</param>
        /// <param name="Use_Extra_Cols">if set to <c>true</c> [use_ extra_ cols].</param>
        public cls_AirlineTariffEntry(int Static_Col, int Col_Increment, bool Use_Extra_Cols = false)
        {
            _Static_Col = Static_Col;
            _Col_Incr = Col_Increment;
            _Use_Extra_Cols = Use_Extra_Cols;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_AirlineTariffEntry"/> class.
        /// </summary>
        /// <param name="Static_Col">The static_ col.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="Todate">The todate.</param>
        /// <param name="Use_Extra_Cols">if set to <c>true</c> [use_ extra_ cols].</param>
        /// This constructor is used when fromdate and todate is required.
        public cls_AirlineTariffEntry(int Static_Col, string FromDate, string Todate, bool Use_Extra_Cols = false)
        {
            _Static_Col = Static_Col;
            _FromDate = FromDate;
            _Todate = Todate;
            _Use_Extra_Cols = Use_Extra_Cols;
        }

        //
        /// <summary>
        /// Fetches the atehdr.
        /// </summary>
        /// <param name="strPolPk">The string pol pk.</param>
        /// <param name="strPodPk">The string pod pk.</param>
        /// <param name="Mode">The mode.</param>
        /// <param name="IsAirlineTariff">if set to <c>true</c> [is airline tariff].</param>
        /// <param name="lngAirlinePk">The LNG airline pk.</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        /// <param name="strFDate">The string f date.</param>
        /// <param name="strTDate">The string t date.</param>
        /// <param name="Group">The group.</param>
        /// <returns></returns>
        public DataTable FetchATEHDR(string strPolPk, string strPodPk, string Mode, bool IsAirlineTariff, long lngAirlinePk, string ChargeBasis, string AirSuchargeToolTip, string strFDate = "", string strTDate = "", int Group = 0)
        {
            string str = null;
            string strCondition = null;
            string strNewModeCondition = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = null;
            DataTable dtKgs = null;
            int intdtMain_Arr_RowCnt = 0;
            int intdtMainColCnt = 0;
            int intdtKgsRowCnt = 0;
            int Static_Col = 0;
            string[] arrPolPk = null;
            string[] arrPodPk = null;

            ChargeBasis = "";
            AirSuchargeToolTip = "";
            //Spliting the POL and POD Pk's
            arrPolPk = strPolPk.Split(Convert.ToChar(","));
            arrPodPk = strPodPk.Split(Convert.ToChar(","));

            if (Group == 1 | Group == 2)
            {
                //Making query with the condition added
                str = " SELECT 0 AS TRANPK,POLGP.PORT_GRP_MST_PK AS \"POLPK\",POLGP.PORT_GRP_ID AS \"AOO\",";
                str += " PODGP.PORT_GRP_MST_PK AS \"PODPK\",PODGP.PORT_GRP_ID AS \"AOD\",";
                str += " '" + strFDate + "' AS \"Valid From\",'" + strTDate + "' AS \"Valid To\"";
                str += " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP";
                str += " WHERE (1=1)";
                str += " AND POLGP.PORT_GRP_MST_PK IN (" + strPolPk + ")";
                str += " AND PODGP.PORT_GRP_MST_PK IN (" + strPodPk + ")";
                str += " GROUP BY POLGP.PORT_GRP_MST_PK, POLGP.PORT_GRP_ID, PODGP.PORT_GRP_ID, PODGP.PORT_GRP_MST_PK";
                str += " HAVING POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK";
                str += " ORDER BY POLGP.PORT_GRP_ID";
            }
            else
            {
                for (intdtMain_Arr_RowCnt = 0; intdtMain_Arr_RowCnt <= arrPolPk.Length - 1; intdtMain_Arr_RowCnt++)
                {
                    if (string.IsNullOrEmpty(strCondition))
                    {
                        strCondition = " (POL.PORT_MST_PK =" + arrPolPk[intdtMain_Arr_RowCnt] + " AND POD.PORT_MST_PK =" + arrPodPk[intdtMain_Arr_RowCnt] + ")";
                    }
                    else
                    {
                        strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk[intdtMain_Arr_RowCnt] + " AND POD.PORT_MST_PK =" + arrPodPk[intdtMain_Arr_RowCnt] + ")";
                    }
                }
                if (!(Mode == "EDIT"))
                {
                    strNewModeCondition = " AND POL.BUSINESS_TYPE = 1";
                    strNewModeCondition += " AND POD.BUSINESS_TYPE = 1";
                }

                //Making query with the condition added
                str = " SELECT 0 AS TRANPK,POL.PORT_MST_PK AS \"POLPK\",POL.PORT_ID AS \"AOO\",";
                str += " POD.PORT_MST_PK AS \"PODPK\",POD.PORT_ID AS \"AOD\",";
                str += " '" + strFDate + "' AS \"Valid From\",'" + strTDate + "' AS \"Valid To\"";
                str += " FROM PORT_MST_TBL POL, PORT_MST_TBL POD";
                str += " WHERE (1=1)";
                str += " AND (";
                str += strCondition + ")";
                str += strNewModeCondition;
                str += " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_ID,POD.PORT_MST_PK";
                str += " HAVING POL.PORT_ID<>POD.PORT_ID";
                str += " ORDER BY POL.PORT_ID";
            }

            try
            {
                dtMain = objWF.GetDataTable(str);
                if (_Static_Col > _AirLine_Tariff_Cols)
                {
                    dtMain.Columns.Add("Expected_Wt");
                    dtMain.Columns.Add("Expected_Vol");
                }

                dtKgs = FetchKgFreight(Mode, IsAirlineTariff, lngAirlinePk);
                for (intdtKgsRowCnt = 0; intdtKgsRowCnt <= dtKgs.Rows.Count - 1; intdtKgsRowCnt++)
                {
                    //Current Rate
                    dtMain.Columns.Add(dtKgs.Rows[intdtKgsRowCnt]["FREIGHT_ELEMENT_MST_PK"].ToString());
                    //Tariff Rate
                    dtMain.Columns.Add(dtKgs.Rows[intdtKgsRowCnt]["FREIGHT_ELEMENT_ID"].ToString());
                    //Charge Basis
                    ChargeBasis += dtKgs.Rows[intdtKgsRowCnt]["CHARGE_BASIS"].ToString() + ",";
                    //Air Surcharge Tool tip
                    AirSuchargeToolTip += dtKgs.Rows[intdtKgsRowCnt]["FREIGHT_ELEMENT_NAME"].ToString() + ",";
                }

                dtMain.Columns.Add("Oth. Chrg");
                dtMain.Columns.Add("Oth_Chrg_Val");
                dtMain.Columns.Add("Routing");

                intdtMainColCnt = dtMain.Columns.Count - 3;

                for (intdtMain_Arr_RowCnt = 0; intdtMain_Arr_RowCnt <= dtMain.Rows.Count - 1; intdtMain_Arr_RowCnt++)
                {
                    Static_Col = _Static_Col;

                    while (Static_Col <= intdtMainColCnt)
                    {
                        for (intdtKgsRowCnt = 0; intdtKgsRowCnt <= dtKgs.Rows.Count - 1; intdtKgsRowCnt++)
                        {
                            if (dtMain.Columns[Static_Col].ColumnName == dtKgs.Rows[intdtKgsRowCnt]["FREIGHT_ELEMENT_MST_PK"].ToString())
                            {
                                dtMain.Rows[intdtMain_Arr_RowCnt][Static_Col] = dtKgs.Rows[intdtKgsRowCnt]["BASIS_VALUE"].ToString();

                                dtMain.Rows[intdtMain_Arr_RowCnt][Static_Col + 1] = dtKgs.Rows[intdtKgsRowCnt]["BASIS_VALUE"].ToString();
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                        //For loop for Rows in dtKgs

                        Static_Col = Static_Col + _Col_Incr;
                    }
                }
                //For loop for Rows in dtMain

                ChargeBasis = ChargeBasis.TrimEnd(Convert.ToChar(","));
                AirSuchargeToolTip = AirSuchargeToolTip.TrimEnd(Convert.ToChar(","));

                return dtMain;
            }
            catch (OracleException SQLEX)
            {
                throw SQLEX;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the freight.
        /// </summary>
        /// <param name="strPolPk">The string pol pk.</param>
        /// <param name="strPodPk">The string pod pk.</param>
        /// <param name="Mode">The mode.</param>
        /// <param name="Group">The group.</param>
        /// <returns></returns>
        /// This function return datatable for child grid. It contains Freight form Freight Table
        /// having freight type of "Freight" and Air Freight Slabs defined with default values.
        public DataTable FetchFreight(string strPolPk, string strPodPk, string Mode, int Group = 0)
        {
            System.Text.StringBuilder str = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = null;
            DataTable dtSlabs = null;
            int i = 0;
            string[] arrPolPk = null;
            string[] arrPodPk = null;
            string strCondition = null;
            string strNewModeCondition = null;
            //Spliting the POL and POD Pk's
            arrPolPk = strPolPk.Split(Convert.ToChar(","));
            arrPodPk = strPodPk.Split(Convert.ToChar(","));

            if (Group == 1 | Group == 2)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (PGL.PORT_GRP_MST_PK IN (" + strPolPk + ") AND PGD.PORT_GRP_MST_PK IN (" + strPodPk + "))";
                }
                else
                {
                    strCondition += " OR (PGL.PORT_GRP_MST_PK IN (" + strPolPk + ")AND PGD.PORT_GRP_MST_PK IN(" + strPodPk + "))";
                }

                if (!(Mode == "EDIT"))
                {
                    strNewModeCondition = " AND PGL.BIZ_TYPE = 1";
                    strNewModeCondition += " AND PGD.BIZ_TYPE = 1";
                    strNewModeCondition += " AND FMT.ACTIVE_FLAG = 1";
                    strNewModeCondition += " AND FMT.CHARGE_TYPE = 1";
                    strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (1,3)";
                    strNewModeCondition += " AND FMT.BY_DEFAULT=1";
                }

                //Making query with the condition added
                //modified by thiyagarajan on 24/11/08 for location based currency task
                str.Append(" SELECT 0 AS FRTPK, PGL.PORT_GRP_MST_PK \"POLPK\", PGD.PORT_GRP_MST_PK \"PODPK\", ");
                str.Append(" FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID AS \"Frt. Ele.\",");
                str.Append(" CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID AS \"Curr.\", 0.00 AS \"Min.\" ");
                str.Append(" FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_GRP_MST_TBL PGL, PORT_GRP_MST_TBL PGD,");
                str.Append(" CURRENCY_TYPE_MST_TBL CURR");
                str.Append(" WHERE (1=1)");
                str.Append(" AND (");
                str.Append(strCondition + ")");
                str.Append(strNewModeCondition);
                str.Append(" AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                str.Append(" GROUP BY PGL.PORT_GRP_MST_PK, PGL.PORT_GRP_ID, PGD.PORT_GRP_MST_PK, PGD.PORT_GRP_ID,");
                str.Append(" FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,");
                str.Append(" CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID");
                str.Append(" HAVING PGL.PORT_GRP_ID<>PGD.PORT_GRP_ID");
                str.Append(" ORDER BY FMT.FREIGHT_ELEMENT_ID");
            }
            else
            {
                for (i = 0; i <= arrPolPk.Length - 1; i++)
                {
                    if (string.IsNullOrEmpty(strCondition))
                    {
                        strCondition = " (POL.PORT_MST_PK =" + Convert.ToString(arrPolPk[i]) + " AND POD.PORT_MST_PK =" + arrPodPk[i].ToString() + ")";
                    }
                    else
                    {
                        strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk[i].ToString() + " AND POD.PORT_MST_PK =" + arrPodPk[i].ToString() + ")";
                    }
                }

                if (!(Mode == "EDIT"))
                {
                    strNewModeCondition = " AND POL.BUSINESS_TYPE = 1";
                    //BUSINESS_TYPE = 1 :- Is the business type for Air
                    strNewModeCondition += " AND POD.BUSINESS_TYPE = 1";
                    //BUSINESS_TYPE = 1 :- Is the business type for Air
                    strNewModeCondition += " AND FMT.ACTIVE_FLAG = 1";
                    strNewModeCondition += " AND FMT.CHARGE_TYPE = 1";
                    strNewModeCondition += " AND FMT.BUSINESS_TYPE IN (1,3)";
                    strNewModeCondition += " AND FMT.BY_DEFAULT=1";
                }

                //Making query with the condition added
                //modified by thiyagarajan on 24/11/08 for location based currency task
                str.Append(" SELECT 0 AS FRTPK,POL.PORT_MST_PK \"POLPK\",POD.PORT_MST_PK \"PODPK\", ");
                str.Append(" FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID AS \"Frt. Ele.\",");
                str.Append(" CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID AS \"Curr.\", 0.00 AS \"Min.\" ");
                str.Append(" FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,");
                str.Append(" CURRENCY_TYPE_MST_TBL CURR");
                str.Append(" WHERE (1=1)");
                str.Append(" AND (");
                str.Append(strCondition + ")");
                str.Append(strNewModeCondition);
                str.Append(" AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                str.Append(" GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,");
                str.Append(" FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,");
                str.Append(" CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID");
                str.Append(" HAVING POL.PORT_ID<>POD.PORT_ID");
                str.Append(" ORDER BY FMT.FREIGHT_ELEMENT_ID");
            }

            try
            {
                dtMain = objWF.GetDataTable(str.ToString());

                dtSlabs = FetchAirFreightSlabs(Mode);
                for (i = 0; i <= dtSlabs.Rows.Count - 1; i++)
                {
                    //Current Rate
                    dtMain.Columns.Add(dtSlabs.Rows[i][0].ToString());
                    //Tariff Rate
                    dtMain.Columns.Add(dtSlabs.Rows[i][1].ToString());
                }
                return dtMain;
            }
            catch (OracleException SQLEX)
            {
                throw SQLEX;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the kg freight.
        /// </summary>
        /// <param name="Mode">The mode.</param>
        /// <param name="IsAirlineTariff">if set to <c>true</c> [is airline tariff].</param>
        /// <param name="lngAirlinePk">The LNG airline pk.</param>
        /// <returns></returns>
        /// This function is called from FetchATEHDR(). It returns datatable for containing
        /// Air Surchages avialable for the selected airline. If airline is not present then
        /// it takes form the default freights along with its default values.
        public DataTable FetchKgFreight(string Mode, bool IsAirlineTariff, long lngAirlinePk)
        {
            System.Text.StringBuilder sqlBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            if (!IsAirlineTariff)
            {
                sqlBuilder.Append("SELECT FR.FREIGHT_ELEMENT_MST_PK, FR.FREIGHT_ELEMENT_ID ");
                sqlBuilder.Append("|| DECODE(FR.CHARGE_BASIS,1,' (%)',2,' (Flat)',3,' (Kgs)') AS FREIGHT_ELEMENT_ID, ");
                sqlBuilder.Append("NVL(FR.BASIS_VALUE, 0.00) BASIS_VALUE, ");
                sqlBuilder.Append("FR.CHARGE_BASIS, ");
                sqlBuilder.Append("FR.FREIGHT_ELEMENT_NAME ");
                sqlBuilder.Append("FROM FREIGHT_ELEMENT_MST_TBL FR ");
                sqlBuilder.Append("WHERE FR.CHARGE_TYPE = 2 ");
                sqlBuilder.Append("AND FR.BY_DEFAULT = 1 ");
                sqlBuilder.Append("AND FR.BUSINESS_TYPE IN (1, 3) ");
                //BUSINESS_TYPE = 1 :- Business Type for Air
                sqlBuilder.Append("AND FR.ACTIVE_FLAG = 1 ");
                sqlBuilder.Append("ORDER BY FREIGHT_ELEMENT_ID");
            }
            else
            {
                sqlBuilder.Append("SELECT FR.FREIGHT_ELEMENT_MST_PK, ");
                sqlBuilder.Append("       FR.FREIGHT_ELEMENT_ID ");
                sqlBuilder.Append("|| DECODE(AIR.CHARGE_BASIS,1,' (%)',2,' (Flat)',3,' (Kgs)') AS FREIGHT_ELEMENT_ID, ");
                sqlBuilder.Append("       NVL(AIR.BASIS_VALUE, 0.00) BASIS_VALUE, ");
                sqlBuilder.Append("       AIR.CHARGE_BASIS, ");
                sqlBuilder.Append("       FR.FREIGHT_ELEMENT_NAME ");
                sqlBuilder.Append("  FROM FREIGHT_ELEMENT_MST_TBL FR, AIRLINE_SURCHARGES_TBL AIR ");
                sqlBuilder.Append(" WHERE FR.FREIGHT_ELEMENT_MST_PK = AIR.FREIGHT_ELEMENT_MST_FK ");
                sqlBuilder.Append("   AND AIR.BY_DEFAULT = 1 ");
                sqlBuilder.Append("   AND AIR.AIRLINE_MST_FK = " + lngAirlinePk);
                sqlBuilder.Append(" ORDER BY FREIGHT_ELEMENT_ID ");
            }
            try
            {
                return objWF.GetDataTable(sqlBuilder.ToString());
            }
            catch (OracleException SQLEX)
            {
                throw SQLEX;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the air freight slabs.
        /// </summary>
        /// <param name="Mode">The mode.</param>
        /// <returns></returns>
        /// This function is called from FetchFreight(). It returns datatable for containing
        /// Air Freight Slabs avialable along with defined default values.
        public DataTable FetchAirFreightSlabs(string Mode)
        {
            System.Text.StringBuilder sqlBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strNewModeCondition = null;

            if (!(Mode == "EDIT"))
            {
                strNewModeCondition = "WHERE AIR.ACTIVE_FLAG =1 ";
            }

            sqlBuilder.Append("SELECT AIR.AIRFREIGHT_SLABS_TBL_PK, ");
            sqlBuilder.Append("AIR.BREAKPOINT_ID FROM AIRFREIGHT_SLABS_TBL AIR ");
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

        #region "Fetch Called by Select Container/Sector"

        //This function returns all the active sectors from the database.
        //If the given POL and POD are present then the value will come as checked.
        /// <summary>
        /// Actives the sector.
        /// </summary>
        /// <param name="strPOLPk">The string pol pk.</param>
        /// <param name="strPODPk">The string pod pk.</param>
        /// <returns></returns>
        public DataTable ActiveSector(string strPOLPk = "", string strPODPk = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            int i = 0;
            string[] arrPolPk = null;
            string[] arrPodPk = null;
            string strCondition = null;
            //Spliting the POL and POD Pk's
            arrPolPk = strPOLPk.Split(Convert.ToChar(","));
            arrPodPk = strPODPk.Split(Convert.ToChar(","));
            //Making condition as the record should have only selected POL and POD
            //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
            //is the selected sector.
            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk[i] + " AND POD.PORT_MST_PK =" + arrPodPk[i] + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk[i] + " AND POD.PORT_MST_PK =" + arrPodPk[i] + ")";
                }
            }

            //Creating the sql if the user has already selected one port pair in calling form
            //incase of veiwing also then that port pair will come and active port pair in the grid.
            //BUSINESS_TYPE = 1 :- Is the business type for AIR
            strSQL = "SELECT POL.PORT_MST_PK ,POL.PORT_ID AS \"POL\", " + "POD.PORT_MST_PK,POD.PORT_ID,'1' CHK " + "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  " + "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " + "AND POL.BUSINESS_TYPE = 1 " + "AND POD.BUSINESS_TYPE = 1 " + "AND ( " + strCondition + " ) " + "UNION " + "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, " + "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK " + "FROM SECTOR_MST_TBL SMT, " + "PORT_MST_TBL POL, " + "PORT_MST_TBL POD " + "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + "AND   POL.BUSINESS_TYPE = 1 " + "AND   POD.BUSINESS_TYPE = 1 " + "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + "AND   SMT.ACTIVE = 1 " + "AND   SMT.BUSINESS_TYPE = 1 " + "ORDER BY CHK DESC,POL";
            try
            {
                return objWF.GetDataTable(strSQL);
            }
            catch (Exception SQLEX)
            {
                throw SQLEX;
            }
        }

        #endregion "Fetch Called by Select Container/Sector"

        #region "Fetch ATE"

        #region "Fetch Transaction"

        //This function fetch the Airline Tariff Entry from the database against the supplied ATE Pk
        /// <summary>
        /// Fetches the ate.
        /// </summary>
        /// <param name="nATEPk">The n ate pk.</param>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="ContractPk">The contract pk.</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        /// <param name="oGroup">The o group.</param>
        public void FetchATE(long nATEPk, DataSet dsGrid, string ContractPk, string ChargeBasis, string AirSuchargeToolTip, int oGroup = 0)
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

                //***************************************************************************************'
                // Fetching Airferight Slabs.
                // Creating columns and populating Child table acc. to the number of air freight slabs.
                //***************************************************************************************'
                objWF.MyCommand.Parameters.Clear();
                var _with8 = objWF.MyCommand.Parameters;
                _with8.Add("TARIFF_MAIN_AIR_PK_IN", nATEPk).Direction = ParameterDirection.Input;
                _with8.Add("TARIFF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (oGroup == 0)
                {
                    dtDataTable = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "TARIFF_FREIGHT");
                }
                else
                {
                    dtDataTable = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "TARIFF_FREIGHT_GROUP");
                }

                Make_Column(dsGrid.Tables["Child"], dtDataTable, true);
                Populate_Child(dsGrid.Tables["Child"], dtDataTable);

                //***************************************************************************************'
                // Fetching Air Surcharges
                // Creating columns in parent table acc. to the number of air surcharges
                //***************************************************************************************'
                objWF.MyCommand.Parameters.Clear();
                var _with9 = objWF.MyCommand.Parameters;
                _with9.Add("TARIFF_MAIN_AIR_PK_IN", nATEPk).Direction = ParameterDirection.Input;
                _with9.Add("TARIFF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (oGroup == 0)
                {
                    dtDataTable = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "TARIFF_AIR_SURCHAGE");
                }
                else
                {
                    dtDataTable = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "TARIFF_AIR_SURCHAGE_GROUP");
                }

                Make_Column(dsGrid.Tables["Parent"], dtDataTable, false, ChargeBasis, AirSuchargeToolTip);
                dsGrid.Tables["Parent"].Columns.Add("Oth. Chrg", typeof(string));
                dsGrid.Tables["Parent"].Columns.Add("Oth_Chrg_Val", typeof(string));

                //adding by thiyagarajan for routing col in grid on 11/3/08
                dsGrid.Tables["Parent"].Columns.Add("Routing", typeof(string));
                //end

                //***************************************************************************************'
                // Fetching Ports and its validity
                //***************************************************************************************'
                objWF.MyCommand.Parameters.Clear();
                var _with10 = objWF.MyCommand.Parameters;
                _with10.Add("TARIFF_MAIN_AIR_PK_IN", nATEPk).Direction = ParameterDirection.Input;
                _with10.Add("TARIFF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                if (oGroup == 0)
                {
                    dtPort = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "TARIFF_PORT");
                }
                else
                {
                    dtPort = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "TARIFF_PORT_GROUP");
                }

                //***************************************************************************************'
                // Fetching Air Other Charges
                // Populating parent table with  Ports, Air surcharges and Other Charges
                //***************************************************************************************'
                objWF.MyCommand.Parameters.Clear();
                var _with11 = objWF.MyCommand.Parameters;
                _with11.Add("TARIFF_MAIN_AIR_PK_IN", nATEPk).Direction = ParameterDirection.Input;
                _with11.Add("TARIFF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (oGroup == 0)
                {
                    dtOtherCharge = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "TARIFF_OTH_CHARGES");
                }
                else
                {
                    dtOtherCharge = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "TARIFF_OTH_CHARGES_GROUP");
                }

                Populate_Parent(dsGrid.Tables["Parent"], dtPort, dtDataTable, dtOtherCharge, ContractPk);

                //***************************************************************************************'
                // Creating Relations
                //***************************************************************************************'
                DataRelation rel = new DataRelation("rl_POL_POD", new DataColumn[] {
                    dsGrid.Tables["Parent"].Columns["POLPK"],
                    dsGrid.Tables["Parent"].Columns["PODPK"]
                }, new DataColumn[] {
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

        #region "Fetch Header"

        /// <summary>
        /// Fetches the air tariff.
        /// </summary>
        /// <param name="nTariffPK">The n tariff pk.</param>
        /// <param name="dtTable">The dt table.</param>
        public void FetchAirTariff(Int64 nTariffPK, DataTable dtTable)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with12 = objWF.MyCommand.Parameters;
                _with12.Add("TARIFF_MAIN_AIR_PK_IN", nTariffPK).Direction = ParameterDirection.Input;
                _with12.Add("TARIFF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTable = objWF.GetDataTable("FETCH_TARIFF_AIR_PKG", "TARIFF_MAIN");
            }
            catch (OracleException ex)
            {
                throw ex;
            }
            catch (Exception generalEx)
            {
                throw generalEx;
            }
        }

        #endregion "Fetch Header"

        #endregion "Fetch ATE"

        //
        /// <summary>
        /// This region creates static column, makes dynamic columns in datatable for
        /// airfreight slabs and  air surcharges and then populates Parent and Child
        /// table accordingly.
        /// </summary>
        /// <param name="dsGrid">The ds grid.</param>
        /// <sub name="Create_Static_Column">
        /// Creates all the static columns required.
        /// </sub>
        /// <sub name="Make_Column">
        /// Creates columns for the dsMain acc. to rows in dtDataTable accoding to IsSlab variable.
        /// If IsSlab is true the columns created for slabs else created for
        /// Air surcharges
        /// </sub>
        /// <sub name="CheckColumn">
        /// This function acutally add column in datatable equal
        /// to the names provided in its param array.
        /// </sub>
        /// <sub name="Populate_Child">
        /// Populates child table for airfreight slabs.
        /// </sub>
        /// <sub name="Populate_Parent">
        /// Populates Parent table for sector,air surcharges and other charges.
        /// </sub>
        /// Creates Static column to be shown in the grid.
        public void Create_Static_Column(DataSet dsGrid)
        {
            //***************************************************************************************'
            // Creating Parent Table
            //***************************************************************************************'
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
            //Two columns are remainig for Other charges
            //Create these two column at the end of fetcing and creating air surcharges column

            //***************************************************************************************'
            // Creating Child Table
            //***************************************************************************************'
            dsGrid.Tables.Add("Child");
            dsGrid.Tables["Child"].Columns.Add("FRTPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("POLPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("PODPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("FREIGHT_ELEMENT_MST_PK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("Frt. Ele.", typeof(string));
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
        /// Makes column from dtTable to dsMain. actually does a transpose.
        /// If IsSlab then
        /// Create air freight slabs
        /// Else
        /// Creates air surcharges
        /// End If
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

            //Making dynamic columns
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
                        //Current,Requested,Approved
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
                    if (nFrt != Convert.ToInt64(dtTable.Rows[nRowCnt]["FRTPK"]))
                    {
                    }

                    if (_Col_Incr == 3)
                    {
                        //Current,Requested,Approved
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
                    if (!dtTable.Columns.Contains(ColumnName[i]))
                    {
                        dtTable.Columns.Add(ColumnName[i], typeof(double));
                    }
                }
                //Manjunath  PTS ID:Sep-02  04/10/2011
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
                    var _with13 = dtTable.Rows[nRowCnt];

                    if (string.IsNullOrEmpty(drRow["FRTPK"].ToString()))
                    {
                        drRow["FRTPK"] = _with13["FRTPK"];
                        nColPopulated = 0;
                    }

                    if (string.IsNullOrEmpty(drRow["POLPK"].ToString()))
                    {
                        drRow["POLPK"] = _with13["PORT_MST_POL_FK"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["PODPK"].ToString()))
                    {
                        drRow["PODPK"] = _with13["PORT_MST_POD_FK"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["FREIGHT_ELEMENT_MST_PK"].ToString()))
                    {
                        drRow["FREIGHT_ELEMENT_MST_PK"] = _with13["FRT_FRT_FK"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["Frt. Ele."].ToString()))
                    {
                        drRow["Frt. Ele."] = _with13["FRT_FRT"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["CURRENCY_MST_PK"].ToString()))
                    {
                        drRow["CURRENCY_MST_PK"] = _with13["FRT_CURR"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["Curr."].ToString()))
                    {
                        drRow["Curr."] = _with13["FRT_CURRID"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["Minimum"].ToString()))
                    {
                        drRow["Minimum"] = _with13["MIN_AMOUNT"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["SLABS_FK"].ToString()))
                    {
                        drRow["SLABS_FK"] = _with13["SLAB_CURRENT"];
                        nColPopulated += 1;
                    }

                    if (string.IsNullOrEmpty(drRow["SLABS"].ToString()))
                    {
                        drRow["SLABS"] = _with13[strRatesToBeShown];
                        nColPopulated += 1;
                    }

                    if (_Col_Incr == 3)
                    {
                        if (string.IsNullOrEmpty(drRow["SLABS_EXTRA"].ToString()))
                        {
                            drRow["SLABS_EXTRA"] = _with13["SLAB_TARIFF"];
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
                //Manjunath  PTS ID:Sep-02  04/10/2011
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
        /// <param name="strContractPk">The string contract pk.</param>
        /// Populates Parent table for sector,air surcharges and other charges
        public void Populate_Parent(DataTable dsMain, DataTable dtPort, DataTable dtAirSurchage, DataTable dtOtherCharges, string strContractPk = null)
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
                    var _with14 = dtPort.Rows[nPortRowCnt];
                    drMain["TRNPK"] = _with14["TRN_AIR_PK"];
                    drMain["POLPK"] = _with14["PORT_MST_POL_FK"];
                    drMain["AOO"] = _with14["AOO"];
                    drMain["PODPK"] = _with14["PORT_MST_POD_FK"];
                    drMain["AOD"] = _with14["AOD"];
                    drMain["Valid From"] = (Convert.ToBoolean(_FromDate.TrimEnd().Length > 0) ? _FromDate : _with14["VALID_FROM"]);
                    drMain["Valid To"] = (Convert.ToBoolean(_Todate.TrimEnd().Length > 0) ? _Todate : _with14["VALID_TO"]);
                    if (_Static_Col > _AirLine_Tariff_Cols & _Use_Extra_Cols)
                    {
                        drMain["Expected_Wt"] = _with14["EXPECTED_WEIGHT"];
                        drMain["Expected_Vol"] = _with14["EXPECTED_VOLUME"];
                    }
                    if (_Static_Col <= _AirLine_Tariff_Cols)
                    {
                        if ((strContractPk != null) & boolFirstLoop)
                        {
                            if (!_with14.IsNull("CONT_MAIN_AIR_FK"))
                            {
                                strContractPk = _with14["CONT_MAIN_AIR_FK"].ToString();
                            }
                            boolFirstLoop = false;
                            //ElseIf Not boolFirstLoop Then
                            //    If Not .IsNull("CONT_MAIN_AIR_FK") Then
                            //        strContractPk = strContractPk & "," & ["CONT_MAIN_AIR_FK").ToString
                            //    End If
                        }
                    }
                    try
                    {
                        for (nAirRowCnt = 0; nAirRowCnt <= dtAirSurchage.Rows.Count - 1; nAirRowCnt++)
                        {
                            var _with15 = dtAirSurchage.Rows[nAirRowCnt];

                            if (Convert.ToInt64(_with15["TRN_AIR_PK"]) == Convert.ToInt64(dtPort.Rows[nPortRowCnt]["TRN_AIR_PK"]) | Convert.ToInt64(dtPort.Rows[nPortRowCnt]["TRN_AIR_PK"]) == 0)
                            {
                                drMain["SUR_FRT_FK"] = _with15["SUR_CURRENT"];
                                drMain["SUR"] = _with15[strRatestoBeShown];

                                if (_Col_Incr == 3)
                                {
                                    drMain["SUR_EXTRA"] = _with15["SUR_TARIFF"];
                                }
                            }
                        }
                        //Air Surcharge loop
                    }
                    catch (Exception ex)
                    {
                    }
                    //FreightElementPk ~ CurrencyPk ~ CurrentRate ~ RequestRate ^
                    for (nOthRowCnt = 0; nOthRowCnt <= dtOtherCharges.Rows.Count - 1; nOthRowCnt++)
                    {
                        var _with16 = dtOtherCharges.Rows[nOthRowCnt];
                        if (Convert.ToInt64(_with16["TRN_AIR_PK"]) == Convert.ToInt64(dtPort.Rows[nPortRowCnt]["TRN_AIR_PK"]))
                        {
                            if (!(_with16.IsNull("OTH_CHRG_FRT_FRT_FK")))
                            {
                                if (_Col_Incr == 3)
                                {
                                    drMain["Oth_Chrg_Val"] = (drMain.IsNull("Oth_Chrg_Val") ? "" : drMain["Oth_Chrg_Val"]).ToString() + _with16["OTH_CHRG_FRT_FRT_FK"].ToString() + "~" + _with16["OTH_CHRG_CURR"].ToString() + "~" + _with16["OTH_CHRG_BASIS"].ToString() + "~" + _with16["OTH_CHRG_CURRENT"].ToString() + "~" + _with16["OTH_CHRG_TARIFF"].ToString() + "~" + _with16["OTH_CHRG_APPROVED"].ToString() + "^";
                                }
                                else
                                {
                                    drMain["Oth_Chrg_Val"] = (drMain.IsNull("Oth_Chrg_Val") ? "" : drMain["Oth_Chrg_Val"]).ToString() + _with16["OTH_CHRG_FRT_FRT_FK"].ToString() + "~" + _with16["OTH_CHRG_CURR"].ToString() + "~" + _with16["OTH_CHRG_BASIS"].ToString() + "~" + _with16["OTH_CHRG_CURRENT"].ToString() + "~" + _with16["OTH_CHRG_TARIFF"].ToString() + "^";
                                }
                            }
                        }
                    }
                    //Air Surcharge loop
                    dsMain.Rows.Add(drMain);
                }
                //Port table loop
                //Manjunath  PTS ID:Sep-02  04/10/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
        }

        #region "Find Contract"

        //This fun fetch the contract from the database against the supplied Airline Pk,Pol,Pod,From DateTime,To DateTime
        /// <summary>
        /// Finds the contract.
        /// </summary>
        /// <param name="strPolPk">The string pol pk.</param>
        /// <param name="strPodPk">The string pod pk.</param>
        /// <param name="nAirlinePk">The n airline pk.</param>
        /// <param name="strCommGroup">The string comm group.</param>
        /// <param name="strFDate">The string f date.</param>
        /// <param name="strTDate">The string t date.</param>
        /// <param name="ContPk">The cont pk.</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        /// <returns></returns>
        public DataSet findContract(string strPolPk, string strPodPk, string nAirlinePk, long strCommGroup, string strFDate, string strTDate, string ContPk, string ChargeBasis, string AirSuchargeToolTip)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsGrid = new DataSet();
            //Dim dtDataTable As New DataSet
            int i = 0;
            System.Text.StringBuilder sqlBuilder = new System.Text.StringBuilder();
            string[] arrPolPk = null;
            string[] arrPodPk = null;
            DataTable dtDataTable = null;
            DataTable dtPort = null;
            DataTable dtOtherCharge = null;
            string strSectorCondition = "";
            try
            {
                ContPk = "";
                ChargeBasis = "";
                AirSuchargeToolTip = "";
                //Spliting the POL and POD Pk's
                arrPolPk = strPolPk.Split(Convert.ToChar(","));
                arrPodPk = strPodPk.Split(Convert.ToChar(","));

                for (i = 0; i <= arrPolPk.Length - 1; i++)
                {
                    strSectorCondition += "(" + Convert.ToInt32(arrPolPk[i]) + "," + Convert.ToInt32(arrPodPk[i]) + "),";
                }
                strSectorCondition = strSectorCondition.TrimEnd(Convert.ToChar(","));

                //Making query with the condition added
                sqlBuilder.Append(" SELECT HDR.CONT_MAIN_AIR_PK FROM");
                sqlBuilder.Append(" CONT_MAIN_AIR_TBL HDR,");
                sqlBuilder.Append(" CONT_TRN_AIR_LCL TRN,");
                sqlBuilder.Append(" PORT_MST_TBL POL,");
                sqlBuilder.Append(" PORT_MST_TBL POD ");
                sqlBuilder.Append(" WHERE TRN.CONT_MAIN_AIR_FK = HDR.CONT_MAIN_AIR_PK");
                sqlBuilder.Append(" AND HDR.AIRLINE_MST_FK =" + nAirlinePk);
                sqlBuilder.Append(" AND HDR.COMMODITY_GROUP_FK =" + strCommGroup);
                sqlBuilder.Append(" AND HDR.ACTIVE = 1");
                sqlBuilder.Append(" AND HDR.CONT_APPROVED = 1");
                sqlBuilder.Append(" AND POL.PORT_MST_PK = TRN.PORT_MST_POL_FK");
                sqlBuilder.Append(" AND POD.PORT_MST_PK = TRN.PORT_MST_POD_FK");
                sqlBuilder.Append(" AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");

                //
                /// When we create an Airline specific tariff
                /// Validity period of the same should fall in the upstream contract validity

                sqlBuilder.Append(" AND TRN.VALID_FROM <= NVL(TO_DATE('" + strFDate + "','" + dateFormat + "'),NULL_DATE_FORMAT)");
                if (strTDate.Length > 0)
                {
                    sqlBuilder.Append(" AND NVL(TRN.VALID_TO,NULL_DATE_FORMAT) >= NVL(TO_DATE('" + strTDate + "','" + dateFormat + "'),NULL_DATE_FORMAT)");
                }
                else
                {
                    sqlBuilder.Append(" AND NVL(TRN.VALID_TO,NULL_DATE_FORMAT) >= NVL(TO_DATE('" + strFDate + "','" + dateFormat + "'),NULL_DATE_FORMAT)");
                }
                //AND NVL(TRN.VALID_TO, NULL_DATE_FORMAT) >= NVL(TO_DATE('01/02/2007', '" & dateFormat & "'), NULL_DATE_FORMAT)
                //sqlBuilder.Append(" AND TRN.VALID_FROM <= NVL(TO_DATE('" & strTDate & "','" & dateFormat & "'),NULL_DATE_FORMAT)")
                //sqlBuilder.Append(" AND NVL(TRN.VALID_TO,NULL_DATE_FORMAT) >= TO_DATE('" & strFDate & "','" & dateFormat & "')")

                sqlBuilder.Append(" ORDER BY POL.PORT_ID,POD.PORT_ID");

                DataSet ds = objWF.GetDataSet(sqlBuilder.ToString());

                //  For i = 0 To dtDataTable.Rows.Count - 1
                //ContPk &= dtDataTable.Rows(i)[0).ToString & ","
                for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                {
                    ContPk += ds.Tables[0].Rows[i][0].ToString() + ",";
                }
                ContPk = ContPk.TrimEnd(Convert.ToChar(","));

                if (ContPk.Trim().Length > 0)
                {
                    try
                    {
                        Create_Static_Column(dsGrid);

                        //***************************************************************************************'
                        // Fetching Airferight Slabs.
                        // Creating columns and populating child table acc. to the number of air freight slabs.
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("      TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("      FRT_FRT.FREIGHT_ELEMENT_ID AS FRT_FRT,");
                        sqlBuilder.Append("      FRT.CONT_AIR_FREIGHT_PK  AS FRTPK,");
                        sqlBuilder.Append("      FRT.FREIGHT_ELEMENT_MST_FK AS FRT_FRT_FK,");
                        sqlBuilder.Append("      FRT.CURRENCY_MST_FK AS FRT_CURR,");
                        sqlBuilder.Append("      FRT_CURR.CURRENCY_ID AS FRT_CURRID,");
                        sqlBuilder.Append("      FRT.MIN_AMOUNT,");
                        sqlBuilder.Append("      AIR_FRT.BREAKPOINT_ID AS SLABS,");
                        sqlBuilder.Append("      BRK.AIRFREIGHT_SLABS_FK AS SLABS_FK,");
                        sqlBuilder.Append("      BRK.APPROVED_RATE AS SLAB_CURRENT,");
                        sqlBuilder.Append("      BRK.APPROVED_RATE AS SLAB_TARIFF");
                        sqlBuilder.Append(" FROM CONT_MAIN_AIR_TBL       HDR,");
                        sqlBuilder.Append("      CONT_TRN_AIR_LCL        TRN,");
                        sqlBuilder.Append("      CONT_AIR_FREIGHT_TBL    FRT,");
                        sqlBuilder.Append("      CONT_AIR_BREAKPOINTS    BRK,");
                        sqlBuilder.Append("      CURRENCY_TYPE_MST_TBL   FRT_CURR,");
                        sqlBuilder.Append("      FREIGHT_ELEMENT_MST_TBL FRT_FRT,");
                        sqlBuilder.Append("      AIRFREIGHT_SLABS_TBL    AIR_FRT ");
                        sqlBuilder.Append("WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("  AND TRN.CONT_TRN_AIR_PK = FRT.CONT_TRN_AIR_FK");
                        sqlBuilder.Append("  AND FRT.CONT_AIR_FREIGHT_PK = BRK.CONT_AIR_FREIGHT_FK");
                        sqlBuilder.Append("  AND FRT_CURR.CURRENCY_MST_PK = FRT.CURRENCY_MST_FK");
                        sqlBuilder.Append("  AND FRT_FRT.FREIGHT_ELEMENT_MST_PK = FRT.FREIGHT_ELEMENT_MST_FK");
                        sqlBuilder.Append("  AND AIR_FRT.AIRFREIGHT_SLABS_TBL_PK = BRK.AIRFREIGHT_SLABS_FK");
                        sqlBuilder.Append("  AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");
                        sqlBuilder.Append("  AND HDR.CONT_MAIN_AIR_PK IN (" + ContPk + ") ");
                        sqlBuilder.Append("ORDER BY TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("         TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("         FRT_FRT.FREIGHT_ELEMENT_ID,");
                        sqlBuilder.Append("         AIR_FRT.SEQUENCE_NO");

                        dtDataTable = objWF.GetDataTable(sqlBuilder.ToString());
                        Make_Column(dsGrid.Tables["Child"], dtDataTable, true);
                        Populate_Child(dsGrid.Tables["Child"], dtDataTable);

                        //***************************************************************************************'
                        // Fetching Air Surcharges
                        // Creating columns in parent table acc. to the number of air surcharges
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.CONT_TRN_AIR_PK AS TRN_AIR_PK,");
                        sqlBuilder.Append("      SUR_FRT.FREIGHT_ELEMENT_ID ");
                        sqlBuilder.Append(" || DECODE(SUR.CHARGE_BASIS,1,' (%)',2,' (Flat)',3,' (Kgs)') AS SUR,");
                        sqlBuilder.Append("      SUR.FREIGHT_ELEMENT_MST_FK AS SUR_FRT_FK,");
                        sqlBuilder.Append("      SUR.APPROVED_RATE AS SUR_CURRENT,");
                        sqlBuilder.Append("      SUR.APPROVED_RATE AS SUR_TARIFF,");
                        sqlBuilder.Append("      SUR.CHARGE_BASIS AS SUR_CHARGE_BASIS,");
                        sqlBuilder.Append("      SUR_FRT.FREIGHT_ELEMENT_NAME AS SUR_FRT_NAME");
                        sqlBuilder.Append(" FROM CONT_MAIN_AIR_TBL       HDR,");
                        sqlBuilder.Append("      CONT_TRN_AIR_LCL        TRN,");
                        sqlBuilder.Append("      CONT_AIR_SURCHARGE      SUR,");
                        sqlBuilder.Append("      FREIGHT_ELEMENT_MST_TBL SUR_FRT ");
                        sqlBuilder.Append("WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("  AND TRN.CONT_TRN_AIR_PK = SUR.CONT_TRN_AIR_FK");
                        sqlBuilder.Append("  AND SUR_FRT.FREIGHT_ELEMENT_MST_PK = SUR.FREIGHT_ELEMENT_MST_FK");
                        sqlBuilder.Append("  AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");
                        sqlBuilder.Append("  AND HDR.CONT_MAIN_AIR_PK IN (" + ContPk + ") ");
                        sqlBuilder.Append("ORDER BY TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("         TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("         SUR_FRT.FREIGHT_ELEMENT_ID");

                        dtDataTable = objWF.GetDataTable(sqlBuilder.ToString());
                        Make_Column(dsGrid.Tables["Parent"], dtDataTable, false, ChargeBasis, AirSuchargeToolTip);
                        dsGrid.Tables["Parent"].Columns.Add("Oth. Chrg", typeof(string));
                        dsGrid.Tables["Parent"].Columns.Add("Oth_Chrg_Val", typeof(string));
                        dsGrid.Tables["Parent"].Columns.Add("Routing", typeof(string));
                        //***************************************************************************************'
                        // Fetching Ports and its validity
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.CONT_TRN_AIR_PK AS TRN_AIR_PK,");
                        sqlBuilder.Append("       TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("       POL.PORT_ID AS AOO,");
                        sqlBuilder.Append("       TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("       POD.PORT_ID AS AOD,");
                        sqlBuilder.Append("       '" + strFDate + "' AS VALID_FROM,");
                        sqlBuilder.Append("       '" + strTDate + "' AS VALID_TO");
                        sqlBuilder.Append("  FROM CONT_MAIN_AIR_TBL HDR,");
                        sqlBuilder.Append("       CONT_TRN_AIR_LCL  TRN,");
                        sqlBuilder.Append("       PORT_MST_TBL      POL,");
                        sqlBuilder.Append("       PORT_MST_TBL      POD ");
                        sqlBuilder.Append(" WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("   AND POL.PORT_MST_PK = TRN.PORT_MST_POL_FK");
                        sqlBuilder.Append("   AND POD.PORT_MST_PK = TRN.PORT_MST_POD_FK");
                        sqlBuilder.Append("   AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");
                        sqlBuilder.Append("   AND HDR.CONT_MAIN_AIR_PK IN (" + ContPk + ") ");
                        sqlBuilder.Append(" ORDER BY POL.PORT_ID,POD.PORT_ID");

                        dtPort = objWF.GetDataTable(sqlBuilder.ToString());

                        //***************************************************************************************'
                        // Fetching Air Other Charges
                        // Populating parent table with  Ports, Air surcharges and Other Charges
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.CONT_TRN_AIR_PK AS TRN_AIR_PK,");
                        sqlBuilder.Append("       OTH_CHRG_FRT.FREIGHT_ELEMENT_ID AS OTH_CHRG_FRT,");
                        sqlBuilder.Append("       OTH_CHRG.FREIGHT_ELEMENT_MST_FK AS OTH_CHRG_FRT_FRT_FK,");
                        sqlBuilder.Append("       OTH_CHRG_CURR.CURRENCY_ID AS OTH_CHRG_CURRID,");
                        sqlBuilder.Append("       OTH_CHRG.CURRENCY_MST_FK AS OTH_CHRG_CURR,");
                        sqlBuilder.Append("       OTH_CHRG.APPROVED_RATE AS OTH_CHRG_CURRENT,");
                        sqlBuilder.Append("       OTH_CHRG.APPROVED_RATE AS OTH_CHRG_TARIFF,");
                        sqlBuilder.Append("       OTH_CHRG.CHARGE_BASIS AS OTH_CHRG_BASIS");
                        sqlBuilder.Append("  FROM CONT_MAIN_AIR_TBL       HDR,");
                        sqlBuilder.Append("       CONT_TRN_AIR_LCL        TRN,");
                        sqlBuilder.Append("       CONT_AIR_OTH_CHRG       OTH_CHRG,");
                        sqlBuilder.Append("       CURRENCY_TYPE_MST_TBL   OTH_CHRG_CURR,");
                        sqlBuilder.Append("       FREIGHT_ELEMENT_MST_TBL OTH_CHRG_FRT");
                        sqlBuilder.Append(" WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("   AND TRN.CONT_TRN_AIR_PK = OTH_CHRG.CONT_TRN_AIR_FK(+)");
                        sqlBuilder.Append("   AND OTH_CHRG_CURR.CURRENCY_MST_PK(+) = OTH_CHRG.CURRENCY_MST_FK");
                        sqlBuilder.Append("   AND OTH_CHRG_FRT.FREIGHT_ELEMENT_MST_PK(+) =");
                        sqlBuilder.Append("       OTH_CHRG.FREIGHT_ELEMENT_MST_FK");
                        sqlBuilder.Append("   AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");
                        sqlBuilder.Append("   AND HDR.CONT_MAIN_AIR_PK IN (" + ContPk + ") ");
                        sqlBuilder.Append(" ORDER BY TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("          TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("          OTH_CHRG_FRT.FREIGHT_ELEMENT_ID");

                        dtOtherCharge = objWF.GetDataTable(sqlBuilder.ToString());
                        Populate_Parent(dsGrid.Tables["Parent"], dtPort, dtDataTable, dtOtherCharge);

                        //***************************************************************************************'
                        // Creating Relations
                        //***************************************************************************************'
                        DataRelation rel = new DataRelation("rl_POL_POD", new DataColumn[] {
                            dsGrid.Tables["Parent"].Columns["POLPK"],
                            dsGrid.Tables["Parent"].Columns["PODPK"]
                        }, new DataColumn[] {
                            dsGrid.Tables["Child"].Columns["POLPK"],
                            dsGrid.Tables["Child"].Columns["PODPK"]
                        });
                        dsGrid.Relations.Add(rel);
                    }
                    catch (Exception sqlExp)
                    {
                        throw sqlExp;
                    }
                    finally
                    {
                        dtPort.Dispose();
                        dtOtherCharge.Dispose();
                    }
                }
                return dsGrid;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                dtDataTable = objWF.GetDataTable(sqlBuilder.ToString());
                dtDataTable.Dispose();
                objWF = null;
            }
        }

        #endregion "Find Contract"

        #region "Fetch Port ID's"

        /// <summary>
        /// Rets the pid.
        /// </summary>
        /// <param name="strPortID">The string port identifier.</param>
        /// <returns></returns>
        public string retPID(string strPortID)
        {
            DataSet ds = null;
            string strPOLPK = null;
            string strPODPK = null;
            string strSql = null;
            string strCondition = null;
            string strPortsID = null;
            string[] arrPID = null;
            string[] arrPOLPK = null;
            string[] arrPODPK = null;
            Int32 i = default(Int32);
            WorkFlow objWF = new WorkFlow();

            arrPID = strPortID.Split(Convert.ToChar("~"));
            strPOLPK = arrPID[0];
            strPODPK = arrPID[1];
            arrPOLPK = strPOLPK.Split(Convert.ToChar(","));
            arrPODPK = strPODPK.Split(Convert.ToChar(","));
            try
            {
                for (i = 0; i <= arrPOLPK.Length - 1; i++)
                {
                    if (string.IsNullOrEmpty(strCondition))
                    {
                        strCondition = " (POL.PORT_MST_PK =" + arrPOLPK[i] + " AND POD.PORT_MST_PK =" + arrPODPK[i] + ")";
                    }
                    else
                    {
                        strCondition += " OR (POL.PORT_MST_PK =" + arrPOLPK[i] + " AND POD.PORT_MST_PK =" + arrPODPK[i] + ")";
                    }
                }

                strSql = "SELECT POL.PORT_ID AS POL,POD.PORT_ID AS POD FROM " + "PORT_MST_TBL POl,PORT_MST_TBL POD WHERE " + strCondition;

                ds = objWF.GetDataSet(strSql);

                for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                {
                    if (i == 0)
                    {
                        strPortsID = ds.Tables[0].Rows[i]["POL"].ToString() + "-" + ds.Tables[0].Rows[i]["POD"].ToString();
                    }
                    else
                    {
                        strPortsID += "," + ds.Tables[0].Rows[i]["POL"].ToString() + "-" + ds.Tables[0].Rows[i]["POD"].ToString();
                    }
                }
                return strPortsID;
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "Fetch Port ID's"

        #region "Fetch Contract"

        //Added by rabbani on 15/12/06 To Fetch Header data from AirLine Contract form to AirLine Tariff form
        /// <summary>
        /// Fetches the contract.
        /// </summary>
        /// <param name="ContractPk">The contract pk.</param>
        /// <returns></returns>
        public DataSet FetchContract(long ContractPk)
        {
            try
            {
                string strSQL = null;

                strSQL = "SELECT MAIN_CONT.CONT_MAIN_AIR_PK," + "      MAIN_CONT.AIRLINE_MST_FK, " + "      AIRLINE.AIRLINE_ID," + "      AIRLINE.AIRLINE_NAME," + "      RFQ.RFQ_REF_NO," + "      RFQ.RFQ_MAIN_AIR_PK," + "      MAIN_CONT.CONTRACT_NO," + "      MAIN_CONT.CONTRACT_DATE," + "      MAIN_CONT.COMMODITY_GROUP_FK," + "      TO_CHAR(MAIN_CONT.VALID_FROM,DATEFORMAT) VALID_FROM ," + "      TO_CHAR(MAIN_CONT.VALID_TO,DATEFORMAT) VALID_TO," + "      MAIN_CONT.ACTIVE," + "      MAIN_CONT.CONT_APPROVED," + "      MAIN_CONT.APPROVED_BY APPROVED_BY_FK," + "      USER_MST.USER_NAME APPROVED_BY_NAME, " + "      MAIN_CONT.APPROVED_DATE APPROVED_DATE,MAIN_CONT.VERSION_NO " + "FROM  CONT_MAIN_AIR_TBL MAIN_CONT," + "      AIRLINE_MST_TBL AIRLINE," + "      RFQ_MAIN_AIR_TBL RFQ," + "      COMMODITY_GROUP_MST_TBL COMM," + "      USER_MST_TBL USER_MST" + "WHERE" + "      MAIN_CONT.CONT_MAIN_AIR_PK = " + ContractPk + "      AND MAIN_CONT.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK(+)" + "      AND MAIN_CONT.RFQ_MAIN_AIR_FK = RFQ.RFQ_MAIN_AIR_PK(+)" + "      AND MAIN_CONT.APPROVED_BY = USER_MST.USER_MST_PK(+)" + "      AND MAIN_CONT.AIRLINE_MST_FK = AIRLINE.AIRLINE_MST_PK(+)";

                return (new WorkFlow()).GetDataSet(strSQL);
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        //Ended by rabbani on 15/12/06

        #endregion "Fetch Contract"

        #region "From Contract"

        //Added by rabbani on 14/12/06 To populate Grid data from AirLine Contract form to AirLine Tariff form
        /// <summary>
        /// Froms the contract.
        /// </summary>
        /// <param name="GridDS">The grid ds.</param>
        /// <param name="strFDate">The string f date.</param>
        /// <param name="strTDate">The string t date.</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        /// <param name="ContPk">The cont pk.</param>
        /// <returns></returns>
        public DataSet FromContract(DataSet GridDS, string strFDate, string strTDate, string ChargeBasis, string AirSuchargeToolTip, string ContPk = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsGrid = new DataSet();
            int i = 0;
            System.Text.StringBuilder sqlBuilder = new System.Text.StringBuilder();
            string[] arrPolPk = null;
            string[] arrPodPk = null;
            DataTable dtDataTable = null;
            DataTable dtPort = null;
            DataTable dtOtherCharge = null;
            string strSectorCondition = "";

            try
            {
                //Making query with the condition added
                sqlBuilder.Append(" SELECT HDR.CONT_MAIN_AIR_PK FROM");
                sqlBuilder.Append(" CONT_MAIN_AIR_TBL HDR,");
                sqlBuilder.Append(" CONT_TRN_AIR_LCL TRN,");
                sqlBuilder.Append(" PORT_MST_TBL POL,");
                sqlBuilder.Append(" PORT_MST_TBL POD ");
                sqlBuilder.Append(" WHERE TRN.CONT_MAIN_AIR_FK = HDR.CONT_MAIN_AIR_PK");
                sqlBuilder.Append(" AND HDR.CONT_MAIN_AIR_PK =" + ContPk);
                sqlBuilder.Append(" AND HDR.ACTIVE = 1");
                sqlBuilder.Append(" AND HDR.CONT_APPROVED = 1");
                sqlBuilder.Append(" AND POL.PORT_MST_PK = TRN.PORT_MST_POL_FK");
                sqlBuilder.Append(" AND POD.PORT_MST_PK = TRN.PORT_MST_POD_FK");
                sqlBuilder.Append(" AND TRN.VALID_FROM <= NVL(TO_DATE('" + strFDate + "','" + dateFormat + "'),NULL_DATE_FORMAT)");
                sqlBuilder.Append(" AND NVL(TRN.VALID_TO,NULL_DATE_FORMAT) >= TO_DATE('" + strTDate + "','" + dateFormat + "')");
                sqlBuilder.Append(" ORDER BY POL.PORT_ID,POD.PORT_ID");

                dtDataTable = objWF.GetDataTable(sqlBuilder.ToString());

                //For i = 0 To dtDataTable.Rows.Count - 1
                //ContPk = dtDataTable.Rows(0)[0).ToString
                //Next
                //ContPk = ContPk.TrimEnd(CChar(","))

                if (ContPk.Trim().Length > 0)
                {
                    try
                    {
                        Create_Static_Column(dsGrid);

                        //***************************************************************************************'
                        // Fetching Airferight Slabs.
                        // Creating columns and populating child table acc. to the number of air freight slabs.
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("      TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("      FRT_FRT.FREIGHT_ELEMENT_ID AS FRT_FRT,");
                        sqlBuilder.Append("      FRT.CONT_AIR_FREIGHT_PK  AS FRTPK,");
                        sqlBuilder.Append("      FRT.FREIGHT_ELEMENT_MST_FK AS FRT_FRT_FK,");
                        sqlBuilder.Append("      FRT.CURRENCY_MST_FK AS FRT_CURR,");
                        sqlBuilder.Append("      FRT_CURR.CURRENCY_ID AS FRT_CURRID,");
                        sqlBuilder.Append("      FRT.MIN_AMOUNT,");
                        sqlBuilder.Append("      AIR_FRT.BREAKPOINT_ID AS SLABS,");
                        sqlBuilder.Append("      BRK.AIRFREIGHT_SLABS_FK AS SLABS_FK,");
                        sqlBuilder.Append("      BRK.APPROVED_RATE AS SLAB_CURRENT,");
                        sqlBuilder.Append("      BRK.APPROVED_RATE AS SLAB_TARIFF");
                        sqlBuilder.Append(" FROM CONT_MAIN_AIR_TBL       HDR,");
                        sqlBuilder.Append("      CONT_TRN_AIR_LCL        TRN,");
                        sqlBuilder.Append("      CONT_AIR_FREIGHT_TBL    FRT,");
                        sqlBuilder.Append("      CONT_AIR_BREAKPOINTS    BRK,");
                        sqlBuilder.Append("      CURRENCY_TYPE_MST_TBL   FRT_CURR,");
                        sqlBuilder.Append("      FREIGHT_ELEMENT_MST_TBL FRT_FRT,");
                        sqlBuilder.Append("      AIRFREIGHT_SLABS_TBL    AIR_FRT ");
                        sqlBuilder.Append("WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("  AND TRN.CONT_TRN_AIR_PK = FRT.CONT_TRN_AIR_FK");
                        sqlBuilder.Append("  AND FRT.CONT_AIR_FREIGHT_PK = BRK.CONT_AIR_FREIGHT_FK");
                        sqlBuilder.Append("  AND FRT_CURR.CURRENCY_MST_PK = FRT.CURRENCY_MST_FK");
                        sqlBuilder.Append("  AND FRT_FRT.FREIGHT_ELEMENT_MST_PK = FRT.FREIGHT_ELEMENT_MST_FK");
                        sqlBuilder.Append("  AND AIR_FRT.AIRFREIGHT_SLABS_TBL_PK = BRK.AIRFREIGHT_SLABS_FK");
                        //sqlBuilder.Append("  AND HDR.CONT_MAIN_AIR_PK IN (" & ContPk & ") ")
                        sqlBuilder.Append("  AND TRN.CONT_MAIN_AIR_FK IN (" + ContPk + ") ");
                        sqlBuilder.Append("ORDER BY TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("         TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("         FRT_FRT.FREIGHT_ELEMENT_ID,");
                        sqlBuilder.Append("         AIR_FRT.SEQUENCE_NO");

                        dtDataTable = objWF.GetDataTable(sqlBuilder.ToString());
                        Make_Column(dsGrid.Tables["Child"], dtDataTable, true);
                        Populate_Child(dsGrid.Tables["Child"], dtDataTable);

                        //***************************************************************************************'
                        // Fetching Air Surcharges
                        // Creating columns in parent table acc. to the number of air surcharges
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.CONT_TRN_AIR_PK AS TRN_AIR_PK,");
                        sqlBuilder.Append("      SUR_FRT.FREIGHT_ELEMENT_ID ");
                        sqlBuilder.Append(" || DECODE(SUR.CHARGE_BASIS,1,' (%)',2,' (Flat)',3,' (Kgs)') AS SUR,");
                        sqlBuilder.Append("      SUR.FREIGHT_ELEMENT_MST_FK AS SUR_FRT_FK,");
                        sqlBuilder.Append("      SUR.APPROVED_RATE AS SUR_CURRENT,");
                        sqlBuilder.Append("      SUR.APPROVED_RATE AS SUR_TARIFF,");
                        sqlBuilder.Append("      SUR.CHARGE_BASIS AS SUR_CHARGE_BASIS,");
                        sqlBuilder.Append("      SUR_FRT.FREIGHT_ELEMENT_NAME AS SUR_FRT_NAME");
                        sqlBuilder.Append(" FROM CONT_MAIN_AIR_TBL       HDR,");
                        sqlBuilder.Append("      CONT_TRN_AIR_LCL        TRN,");
                        sqlBuilder.Append("      CONT_AIR_SURCHARGE      SUR,");
                        sqlBuilder.Append("      FREIGHT_ELEMENT_MST_TBL SUR_FRT ");
                        sqlBuilder.Append("WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("  AND TRN.CONT_TRN_AIR_PK = SUR.CONT_TRN_AIR_FK");
                        sqlBuilder.Append("  AND SUR_FRT.FREIGHT_ELEMENT_MST_PK = SUR.FREIGHT_ELEMENT_MST_FK");
                        //sqlBuilder.Append("  AND HDR.CONT_MAIN_AIR_PK IN (" & ContPk & ") ")
                        sqlBuilder.Append("  AND TRN.CONT_MAIN_AIR_FK IN (" + ContPk + ") ");
                        sqlBuilder.Append("ORDER BY TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("         TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("         SUR_FRT.FREIGHT_ELEMENT_ID");

                        dtDataTable = objWF.GetDataTable(sqlBuilder.ToString());
                        //Make_Column(dsGrid.Tables("Parent"), dtDataTable, False)
                        Make_Column(dsGrid.Tables["Parent"], dtDataTable, false, ChargeBasis, AirSuchargeToolTip);
                        dsGrid.Tables["Parent"].Columns.Add("Oth. Chrg", typeof(string));
                        dsGrid.Tables["Parent"].Columns.Add("Oth_Chrg_Val", typeof(string));

                        //Adding by Thiyagarjan for adding a Routing & TT col. in the Grid.
                        dsGrid.Tables["Parent"].Columns.Add("Routing", typeof(string));
                        //end

                        //***************************************************************************************'
                        // Fetching Ports and its validity
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.CONT_TRN_AIR_PK AS TRN_AIR_PK,");
                        sqlBuilder.Append("       TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("       POL.PORT_ID AS AOO,");
                        sqlBuilder.Append("       TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("       POD.PORT_ID AS AOD,");
                        sqlBuilder.Append("       '" + strFDate + "' AS VALID_FROM,");
                        sqlBuilder.Append("       '" + strTDate + "' AS VALID_TO");
                        sqlBuilder.Append("  FROM CONT_MAIN_AIR_TBL HDR,");
                        sqlBuilder.Append("       CONT_TRN_AIR_LCL  TRN,");
                        sqlBuilder.Append("       PORT_MST_TBL      POL,");
                        sqlBuilder.Append("       PORT_MST_TBL      POD ");
                        sqlBuilder.Append(" WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("   AND POL.PORT_MST_PK = TRN.PORT_MST_POL_FK");
                        sqlBuilder.Append("   AND POD.PORT_MST_PK = TRN.PORT_MST_POD_FK");
                        //sqlBuilder.Append("   AND HDR.CONT_MAIN_AIR_PK IN (" & ContPk & ") ")
                        sqlBuilder.Append("   AND TRN.CONT_MAIN_AIR_FK IN (" + ContPk + ") ");
                        //rabbani
                        sqlBuilder.Append(" ORDER BY POL.PORT_ID,POD.PORT_ID");

                        dtPort = objWF.GetDataTable(sqlBuilder.ToString());

                        //***************************************************************************************'
                        // Fetching Air Other Charges
                        // Populating parent table with  Ports, Air surcharges and Other Charges
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.CONT_TRN_AIR_PK AS TRN_AIR_PK,");
                        sqlBuilder.Append("       OTH_CHRG_FRT.FREIGHT_ELEMENT_ID AS OTH_CHRG_FRT,");
                        sqlBuilder.Append("       OTH_CHRG.FREIGHT_ELEMENT_MST_FK AS OTH_CHRG_FRT_FRT_FK,");
                        sqlBuilder.Append("       OTH_CHRG_CURR.CURRENCY_ID AS OTH_CHRG_CURRID,");
                        sqlBuilder.Append("       OTH_CHRG.CURRENCY_MST_FK AS OTH_CHRG_CURR,");
                        sqlBuilder.Append("       OTH_CHRG.APPROVED_RATE AS OTH_CHRG_CURRENT,");
                        sqlBuilder.Append("       OTH_CHRG.APPROVED_RATE AS OTH_CHRG_TARIFF,");
                        sqlBuilder.Append("       OTH_CHRG.CHARGE_BASIS AS OTH_CHRG_BASIS");
                        sqlBuilder.Append("  FROM CONT_MAIN_AIR_TBL       HDR,");
                        sqlBuilder.Append("       CONT_TRN_AIR_LCL        TRN,");
                        sqlBuilder.Append("       CONT_AIR_OTH_CHRG       OTH_CHRG,");
                        sqlBuilder.Append("       CURRENCY_TYPE_MST_TBL   OTH_CHRG_CURR,");
                        sqlBuilder.Append("       FREIGHT_ELEMENT_MST_TBL OTH_CHRG_FRT");
                        sqlBuilder.Append(" WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("   AND TRN.CONT_TRN_AIR_PK = OTH_CHRG.CONT_TRN_AIR_FK(+)");
                        sqlBuilder.Append("   AND OTH_CHRG_CURR.CURRENCY_MST_PK(+) = OTH_CHRG.CURRENCY_MST_FK");
                        sqlBuilder.Append("   AND OTH_CHRG_FRT.FREIGHT_ELEMENT_MST_PK(+) =");
                        sqlBuilder.Append("       OTH_CHRG.FREIGHT_ELEMENT_MST_FK");
                        //sqlBuilder.Append("   AND HDR.CONT_MAIN_AIR_PK IN (" & ContPk & ") ")
                        sqlBuilder.Append("   AND TRN.CONT_MAIN_AIR_FK IN (" + ContPk + ") ");
                        sqlBuilder.Append(" ORDER BY TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("          TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("          OTH_CHRG_FRT.FREIGHT_ELEMENT_ID");

                        dtOtherCharge = objWF.GetDataTable(sqlBuilder.ToString());
                        Populate_Parent(dsGrid.Tables["Parent"], dtPort, dtDataTable, dtOtherCharge);

                        //***************************************************************************************'
                        // Creating Relations
                        //***************************************************************************************'

                        DataRelation rel = new DataRelation("rl_POL_POD", new DataColumn[] {
                            dsGrid.Tables["Parent"].Columns["POLPK"],
                            dsGrid.Tables["Parent"].Columns["PODPK"]
                        }, new DataColumn[] {
                            dsGrid.Tables["Child"].Columns["POLPK"],
                            dsGrid.Tables["Child"].Columns["PODPK"]
                        });
                        dsGrid.Relations.Add(rel);
                    }
                    catch (Exception sqlExp)
                    {
                        throw sqlExp;
                    }
                    finally
                    {
                        dtPort.Dispose();
                        dtOtherCharge.Dispose();
                    }
                }
                return dsGrid;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                dtDataTable.Dispose();
                objWF = null;
            }
        }

        //Ended by rabbani on 14/12/06
        //by thiyagarjan
        /// <summary>
        /// Fetches the contract pk.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public int FetchContractPK(int pk)
        {
            WorkFlow objWF = new WorkFlow();
            string strsql = null;
            try
            {
                strsql = "select tariff.cont_main_air_fk from tariff_trn_air_tbl tariff where tariff.tariff_main_air_fk = " + pk;
                return Convert.ToInt32(getDefault(objWF.ExecuteScaler(strsql), 0));
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        //end

        #endregion "From Contract"

        #region "FetchPrtGroup"

        /// <summary>
        /// Fetches the PRT group.
        /// </summary>
        /// <param name="TariffMstPK">The tariff MST pk.</param>
        /// <returns></returns>
        public string FetchPrtGroup(int TariffMstPK = 0)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT NVL(T.PORT_GROUP,0) PORT_GROUP FROM TARIFF_MAIN_AIR_TBL T WHERE T.TARIFF_MAIN_AIR_PK = " + TariffMstPK;
                return objWF.ExecuteScaler(strSQL);
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

        #endregion "FetchPrtGroup"

        #region "PortGroup"

        /// <summary>
        /// Fetches from port group.
        /// </summary>
        /// <param name="TariffMstPK">The tariff MST pk.</param>
        /// <param name="PortGrpPK">The port GRP pk.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <returns></returns>
        public DataSet FetchFromPortGroup(int TariffMstPK = 0, int PortGrpPK = 0, string POLPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (TariffMstPK != 0)
                {
                    sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID, T.POL_GRP_FK POL_GRP_FK,T.TARIFF_TRN_AIR_PK,T.PORT_MST_POD_FK");
                    sb.Append("  FROM PORT_MST_TBL P, TARIFF_TRN_AIR_TBL T");
                    sb.Append(" WHERE P.PORT_MST_PK = T.PORT_MST_POL_FK");
                    sb.Append("   AND T.TARIFF_MAIN_AIR_FK = " + TariffMstPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POL_GRP_FK,0  TARIFF_TRN_AIR_PK,0 PORT_MST_POD_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 1 AND P.ACTIVE_FLAG = 1 ");
                    if (POLPK != "0" & !string.IsNullOrEmpty(POLPK))
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + POLPK + ") ");
                    }
                }

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

        /// <summary>
        /// Fetches to port group.
        /// </summary>
        /// <param name="TariffMstPK">The tariff MST pk.</param>
        /// <param name="PortGrpPK">The port GRP pk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <returns></returns>
        public DataSet FetchToPortGroup(int TariffMstPK = 0, int PortGrpPK = 0, string PODPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (TariffMstPK != 0)
                {
                    sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK POD_GRP_FK ");
                    sb.Append("  FROM PORT_MST_TBL P, TARIFF_TRN_AIR_TBL T");
                    sb.Append(" WHERE P.PORT_MST_PK = T.PORT_MST_POD_FK");
                    sb.Append("   AND T.TARIFF_MAIN_AIR_FK = " + TariffMstPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POD_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 1 AND P.ACTIVE_FLAG = 1");
                    if (PODPK != "0" & !string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + PODPK + ") ");
                    }
                }
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

        /// <summary>
        /// Fetches the tariff GRP.
        /// </summary>
        /// <param name="TariffMstPK">The tariff MST pk.</param>
        /// <param name="PortPOLGrpPK">The port pol GRP pk.</param>
        /// <param name="PortPODGrpPK">The port pod GRP pk.</param>
        /// <param name="TariffPK">The tariff pk.</param>
        /// <returns></returns>
        public DataSet FetchTariffGrp(int TariffMstPK = 0, int PortPOLGrpPK = 0, int PortPODGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (TariffMstPK != 0)
                {
                    sb.Append(" SELECT POL.PORT_MST_PK   POL_PK,");
                    sb.Append("       POL.PORT_ID       POL_ID,");
                    sb.Append("       POD.PORT_MST_PK   POD_PK,");
                    sb.Append("       POD.PORT_ID       POD_ID,");
                    sb.Append("       T.POL_GRP_FK      POL_GRP_FK,");
                    sb.Append("       T.PORT_MST_POD_FK POD_GRP_FK,");
                    sb.Append("       T.TARIFF_GRP_FK   TARIFF_GRP_MST_PK");
                    sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, TARIFF_TRN_AIR_TBL T");
                    sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND T.TARIFF_MAIN_AIR_FK =" + TariffMstPK);
                }
                else
                {
                    sb.Append(" SELECT POL.PORT_MST_PK       POL_PK,");
                    sb.Append("       POL.PORT_ID           POL_ID,");
                    sb.Append("       POD.PORT_MST_PK       POD_PK,");
                    sb.Append("       POD.PORT_ID           POD_ID,");
                    sb.Append("       TGM.POL_GRP_MST_FK    POL_GRP_FK,");
                    sb.Append("       TGM.POD_GRP_MST_FK    POD_GRP_FK,");
                    sb.Append("       TGM.TARIFF_GRP_MST_PK");
                    sb.Append("  FROM PORT_MST_TBL       POL,");
                    sb.Append("       PORT_MST_TBL       POD,");
                    sb.Append("       TARIFF_GRP_TRN_TBL TGT,");
                    sb.Append("       TARIFF_GRP_MST_TBL TGM");
                    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                    sb.Append("   AND POL.PORT_MST_PK = TGT.POL_MST_FK");
                    sb.Append("   AND POD.PORT_MST_PK = TGT.POD_MST_FK");
                    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                    sb.Append("   AND POL.BUSINESS_TYPE = 1");
                    sb.Append("   AND POL.ACTIVE_FLAG = 1");
                }
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

        /// <summary>
        /// Fetches the saved group.
        /// </summary>
        /// <param name="TariffMstPK">The tariff MST pk.</param>
        /// <returns></returns>
        public DataSet FetchSavedGroup(int TariffMstPK = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (TariffMstPK != 0)
                {
                    sb.Append("SELECT POL.PORT_MST_PK POL_PK,");
                    sb.Append("       POL.PORT_ID     POD_ID,");
                    sb.Append("       T.POL_GRP_FK,");
                    sb.Append("       POd.PORT_MST_PK POD_PK,");
                    sb.Append("       POD.PORT_ID     POD_ID,");
                    sb.Append("       T.POD_GRP_FK,");
                    sb.Append("       T.TARIFF_GRP_FK");
                    sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, TARIFF_TRN_SEA_FCL_LCL T");
                    sb.Append(" WHERE POL.PORT_MST_PK = T.PORT_MST_POL_FK");
                    sb.Append("   AND POD.PORT_MST_PK = T.PORT_MST_POD_FK");
                    sb.Append("   AND T.TARIFF_MAIN_SEA_FK = " + TariffMstPK);
                }
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

        #endregion "PortGroup"

        #region "Fetch Max Tariff Nr"

        /// <summary>
        /// Fetches the tariff nr.
        /// </summary>
        /// <param name="strTariffNo">The string tariff no.</param>
        /// <returns></returns>
        public string FetchTariffNr(string strTariffNo)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT NVL(MAX(T.TARIFF_REF_NO),0) FROM TARIFF_MAIN_AIR_TBL T " + "WHERE T.TARIFF_REF_NO LIKE '" + strTariffNo + "/%' " + "ORDER BY T.TARIFF_REF_NO";
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

        #endregion "Fetch Max Tariff Nr"
    }
}