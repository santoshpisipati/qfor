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
    public class Cls_AirCustomerContract : CommonFeatures
    {
        #region " Private Variables "

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
        private int _Col_Incr = 2;

        /// <summary>
        /// The _ from date
        /// </summary>
        private string _FromDate;

        /// <summary>
        /// The _ todate
        /// </summary>
        private string _Todate;

        #endregion " Private Variables "

        /// <summary>
        /// The _ use_ extra_ cols
        /// </summary>
        private bool _Use_Extra_Cols;

        #region " Property "

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

        #endregion " Property "

        #region " Constuctors "

        /// <summary>
        /// Initializes a new instance of the <see cref="Cls_AirCustomerContract"/> class.
        /// </summary>
        /// <param name="Static_Col">The static_ col.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="Todate">The todate.</param>
        /// <param name="Use_Extra_Cols">if set to <c>true</c> [use_ extra_ cols].</param>
        public Cls_AirCustomerContract(int Static_Col, string FromDate, string Todate, bool Use_Extra_Cols)
        {
            _Static_Col = Static_Col;
            _FromDate = FromDate;
            _Todate = Todate;
            _Use_Extra_Cols = Use_Extra_Cols;
        }

        #endregion " Constuctors "

        #region " Fetch Queries "

        #region " Fetch Transaction "

        /// <summary>
        /// Fetch_s the contract.
        /// </summary>
        /// <param name="ContractPk">The contract pk.</param>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        /// <param name="oGroup">The o group.</param>
        public void Fetch_Contract(long ContractPk, DataSet dsGrid, string ChargeBasis, string AirSuchargeToolTip, int oGroup = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtDataTable = null;
            DataTable dtPort = null;
            DataTable dtOtherCharge = null;
            Quantum_QFOR.cls_AirlineTariffEntry objTariff = null;

            if (_FromDate.TrimEnd().Length <= 0 & _Todate.TrimEnd().Length <= 0)
            {
                objTariff = new Quantum_QFOR.cls_AirlineTariffEntry(_Static_Col, _Col_Incr, _Use_Extra_Cols);
            }
            else
            {
                objTariff = new Quantum_QFOR.cls_AirlineTariffEntry(_Static_Col, _FromDate, _Todate, _Use_Extra_Cols);
            }

            try
            {
                ChargeBasis = "";
                AirSuchargeToolTip = "";

                objTariff.Create_Static_Column(dsGrid);

                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("CONT_CUST_AIR_PK_IN", ContractPk).Direction = ParameterDirection.Input;
                _with1.Add("CONT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (oGroup == 0)
                {
                    dtDataTable = objWF.GetDataTable("FETCH_CUST_CONT_AIR_PKG", "CONT_FREIGHT");
                }
                else
                {
                    dtDataTable = objWF.GetDataTable("FETCH_CUST_CONT_AIR_PKG", "CONT_FREIGHT_GROUP");
                }

                objTariff.Make_Column(dsGrid.Tables["Child"], dtDataTable, true);
                objTariff.Populate_Child(dsGrid.Tables["Child"], dtDataTable);

                objWF.MyCommand.Parameters.Clear();
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("CONT_CUST_AIR_PK_IN", ContractPk).Direction = ParameterDirection.Input;
                _with2.Add("CONT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (oGroup == 0)
                {
                    dtDataTable = objWF.GetDataTable("FETCH_CUST_CONT_AIR_PKG", "CONT_AIR_SURCHAGE");
                }
                else
                {
                    dtDataTable = objWF.GetDataTable("FETCH_CUST_CONT_AIR_PKG", "CONT_AIR_SURCHAGE_GROUP");
                }

                objTariff.Make_Column(dsGrid.Tables["Parent"], dtDataTable, false, ChargeBasis, AirSuchargeToolTip);
                dsGrid.Tables["Parent"].Columns.Add("Oth. Chrg", typeof(string));
                dsGrid.Tables["Parent"].Columns.Add("Oth_Chrg_Val", typeof(string));

                objWF.MyCommand.Parameters.Clear();
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("CONT_CUST_AIR_PK_IN", ContractPk).Direction = ParameterDirection.Input;
                _with3.Add("CONT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (oGroup == 0)
                {
                    dtPort = objWF.GetDataTable("FETCH_CUST_CONT_AIR_PKG", "CONT_PORT");
                }
                else
                {
                    dtPort = objWF.GetDataTable("FETCH_CUST_CONT_AIR_PKG", "CONT_PORT_GROUP");
                }

                objWF.MyCommand.Parameters.Clear();
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("CONT_CUST_AIR_PK_IN", ContractPk).Direction = ParameterDirection.Input;
                _with4.Add("CONT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtOtherCharge = objWF.GetDataTable("FETCH_CUST_CONT_AIR_PKG", "CONT_OTH_CHARGES");

                objTariff.Populate_Parent(dsGrid.Tables["Parent"], dtPort, dtDataTable, dtOtherCharge, "");

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

        #endregion " Fetch Transaction "

        #region " Fetch Header "

        /// <summary>
        /// Fetch_s the contract_ header.
        /// </summary>
        /// <param name="intContractPK">The int contract pk.</param>
        /// <param name="dtTable">The dt table.</param>
        public void Fetch_Contract_Header(long intContractPK, DataTable dtTable)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with5 = objWF.MyCommand.Parameters;
                _with5.Add("CONT_CUST_AIR_PK_IN", intContractPK).Direction = ParameterDirection.Input;
                _with5.Add("CONT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTable = objWF.GetDataTable("FETCH_CUST_CONT_AIR_PKG", "CONT_MAIN");
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

        #endregion " Fetch Header "

        #endregion " Fetch Queries "

        #region "Fetch Header Details"

        /// <summary>
        /// Fetches the header.
        /// </summary>
        /// <param name="CustContractPk">The customer contract pk.</param>
        /// <returns></returns>
        public DataSet FetchHeader(int CustContractPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT CMT.CUSTOMER_NAME,");
                sb.Append("       CCD.ADM_ADDRESS_1,");
                sb.Append("       CCD.ADM_ADDRESS_2,");
                sb.Append("       CCD.ADM_ADDRESS_3,");
                sb.Append("       CCD.ADM_ZIP_CODE,");
                sb.Append("       CMT.COUNTRY_NAME,");
                sb.Append("       OMT.AIRLINE_NAME,");
                sb.Append("       'AIR' BIZ_TYPE,");
                sb.Append("       CCST.CONT_REF_NO,");
                sb.Append("       CCST.CONT_DATE,");
                sb.Append("       CCST.VALID_FROM,");
                sb.Append("       CCST.VALID_TO,");
                sb.Append("       DECODE(CMT.BUSINESS_TYPE,3,'Both',1,'Air',2,'Sea')BUSINESS_TYPE,");
                sb.Append("       CT.COMMODITY_NAME,");
                sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
                sb.Append("        DECODE(CCST.cont_approved,0,'Work In Progress', 1,'Internal Approved',2,'Customer Approved',3,'Internal Rejected')cont_approved,");
                sb.Append("       CCST.CREDIT_PERIOD,");
                sb.Append("       DECODE(CCST.cont_approved,0,CUMT.USER_NAME,1,LUMT.USER_NAME,2,LUMT.USER_NAME) USER_ID,");
                sb.Append("       PLMT.LOCATION_NAME,");
                sb.Append("       TMS.TARIFF_REF_NO, ");
                sb.Append("       AUMT.USER_NAME APPBY_ID,");
                sb.Append("       CCST.APPROVED_DATE,");
                sb.Append("        CCST.CONT_CLAUSE ");
                sb.Append("  FROM CONT_CUST_AIR_TBL     CCST,");
                sb.Append("       CUSTOMER_MST_TBL      CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
                sb.Append("       LOCATION_MST_TBL      LMT,");
                sb.Append("       COUNTRY_MST_TBL       CMT,");
                sb.Append("       LOCATION_MST_TBL      PLMT,");
                sb.Append("       AIRLINE_MST_TBL      OMT,");
                sb.Append("       COMMODITY_MST_TBL     CT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       USER_MST_TBL            CUMT,");
                sb.Append("       USER_MST_TBL            LUMT,");
                sb.Append("       USER_MST_TBL            AUMT,");
                sb.Append("       TARIFF_MAIN_AIR_TBL   TMS");
                sb.Append(" WHERE CCST.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND LMT.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
                sb.Append("   AND CCST.AIRLINE_MST_FK = OMT.AIRLINE_MST_PK(+)");
                sb.Append("   AND CCST.COMMODITY_MST_FK = CT.COMMODITY_MST_PK(+)");
                sb.Append("   AND CCST.COMMODITY_GROUP_MST_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND CCST.TARIFF_MAIN_AIR_FK=TMS.TARIFF_MAIN_AIR_PK");
                sb.Append("   AND CCST.CREATED_BY_FK = CUMT.USER_MST_PK");
                sb.Append("   AND CCST.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+)");
                sb.Append("   AND CCST.APPROVED_BY = AUMT.USER_MST_PK(+)");
                sb.Append("   AND CCST.PYMT_LOCATION_MST_FK = PLMT.LOCATION_MST_PK(+)");
                sb.Append("   AND CCST.CONT_CUST_AIR_PK =" + CustContractPk);
                return ObjWk.GetDataSet(sb.ToString());
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

        #endregion "Fetch Header Details"

        #region "Fetch FreightDetails"

        /// <summary>
        /// Fetches the freight details.
        /// </summary>
        /// <param name="CustContractAirPk">The customer contract air pk.</param>
        /// <returns></returns>
        public DataSet FetchFreightDetails(long CustContractAirPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT POL.PORT_NAME  POL,");
                sb.Append("       POD.PORT_NAME  POD,");
                sb.Append("       CCTA.VALID_FROM,");
                sb.Append("       CCTA.VALID_TO,");
                sb.Append("       CCTA.EXPECTED_VOLUME,");
                sb.Append("       CCTA.EXPECTED_WEIGHT,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT_ID,");
                sb.Append("       CTMT.CURRENCY_ID,");
                sb.Append("       CCAB.CURRENT_RATE,");
                sb.Append("       CCAB.APPROVED_RATE,");
                sb.Append("       CCAF.MIN_AMOUNT,");
                sb.Append("       AST.BREAKPOINT_ID,");
                sb.Append("       AST.SEQUENCE_NO");
                sb.Append("  FROM CONT_CUST_AIR_TBL         CCAT,");
                sb.Append("       CONT_CUST_TRN_AIR_TBL     CCTA,");
                sb.Append("       CONT_CUST_AIR_FREIGHT_TBL CCAF,");
                sb.Append("       CONT_CUST_AIR_BREAKPOINTS CCAB,");
                sb.Append("       AIRFREIGHT_SLABS_TBL      AST,");
                sb.Append("       PORT_MST_TBL              POL,");
                sb.Append("       PORT_MST_TBL              POD,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL   FEMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL     CTMT");
                sb.Append(" WHERE CCTA.CONT_CUST_AIR_FK = CCAT.CONT_CUST_AIR_PK");
                sb.Append("   AND CCAF.CONT_CUST_TRN_AIR_FK = CCTA.CONT_CUST_TRN_AIR_PK");
                sb.Append("   AND CCTA.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND CCTA.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND CCAB.CONT_CUST_AIR_FRT_FK = CCAF.CONT_CUST_AIR_FREIGHT_PK");
                sb.Append("   AND AST.AIRFREIGHT_SLABS_TBL_PK = CCAB.AIRFREIGHT_SLABS_FK");
                sb.Append("   AND CCAF.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND CCAF.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND CCAT.CONT_CUST_AIR_PK =" + CustContractAirPk);
                sb.Append("   ORDER BY AST.SEQUENCE_NO ");
                return objWK.GetDataSet(sb.ToString());
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
        /// Fetches the freight details port GRP.
        /// </summary>
        /// <param name="CustContractAirPk">The customer contract air pk.</param>
        /// <returns></returns>
        public DataSet FetchFreightDetailsPortGrp(long CustContractAirPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT POL.PORT_GRP_ID  POL,");
                sb.Append("       POD.PORT_GRP_ID  POD,");
                sb.Append("       CCTA.VALID_FROM,");
                sb.Append("       CCTA.VALID_TO,");
                sb.Append("       CCTA.EXPECTED_VOLUME,");
                sb.Append("       CCTA.EXPECTED_WEIGHT,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT_ID,");
                sb.Append("       CTMT.CURRENCY_ID,");
                sb.Append("       CCAB.CURRENT_RATE,");
                sb.Append("       CCAB.APPROVED_RATE,");
                sb.Append("       CCAF.MIN_AMOUNT,");
                sb.Append("       AST.BREAKPOINT_ID,");
                sb.Append("       AST.SEQUENCE_NO");
                sb.Append("  FROM CONT_CUST_AIR_TBL         CCAT,");
                sb.Append("       CONT_CUST_TRN_AIR_TBL     CCTA,");
                sb.Append("       CONT_CUST_AIR_FREIGHT_TBL CCAF,");
                sb.Append("       CONT_CUST_AIR_BREAKPOINTS CCAB,");
                sb.Append("       AIRFREIGHT_SLABS_TBL      AST,");
                sb.Append("       PORT_GRP_MST_TBL              POL,");
                sb.Append("       PORT_GRP_MST_TBL              POD,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL   FEMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL     CTMT");
                sb.Append(" WHERE CCTA.CONT_CUST_AIR_FK = CCAT.CONT_CUST_AIR_PK");
                sb.Append("   AND CCAF.CONT_CUST_TRN_AIR_FK = CCTA.CONT_CUST_TRN_AIR_PK");
                sb.Append("   AND CCTA.POL_GRP_FK = POL.PORT_GRP_MST_PK");
                sb.Append("   AND CCTA.POD_GRP_FK = POD.PORT_GRP_MST_PK");
                sb.Append("   AND CCAB.CONT_CUST_AIR_FRT_FK = CCAF.CONT_CUST_AIR_FREIGHT_PK");
                sb.Append("   AND AST.AIRFREIGHT_SLABS_TBL_PK = CCAB.AIRFREIGHT_SLABS_FK");
                sb.Append("   AND CCAF.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND CCAF.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND CCAT.CONT_CUST_AIR_PK =" + CustContractAirPk);
                sb.Append("   ORDER BY AST.SEQUENCE_NO ");
                return objWK.GetDataSet(sb.ToString());
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
        /// Fetches the freight details tariff GRP.
        /// </summary>
        /// <param name="CustContractAirPk">The customer contract air pk.</param>
        /// <returns></returns>
        public DataSet FetchFreightDetailsTariffGrp(long CustContractAirPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT TRF.TARIFF_GRP_NAME  POL,");
                sb.Append("       '' POD,");
                sb.Append("       CCTA.VALID_FROM,");
                sb.Append("       CCTA.VALID_TO,");
                sb.Append("       CCTA.EXPECTED_VOLUME,");
                sb.Append("       CCTA.EXPECTED_WEIGHT,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT_ID,");
                sb.Append("       CTMT.CURRENCY_ID,");
                sb.Append("       CCAB.CURRENT_RATE,");
                sb.Append("       CCAB.APPROVED_RATE,");
                sb.Append("       CCAF.MIN_AMOUNT,");
                sb.Append("       AST.BREAKPOINT_ID,");
                sb.Append("       AST.SEQUENCE_NO");
                sb.Append("  FROM CONT_CUST_AIR_TBL         CCAT,");
                sb.Append("       CONT_CUST_TRN_AIR_TBL     CCTA,");
                sb.Append("       CONT_CUST_AIR_FREIGHT_TBL CCAF,");
                sb.Append("       CONT_CUST_AIR_BREAKPOINTS CCAB,");
                sb.Append("       AIRFREIGHT_SLABS_TBL      AST,");
                sb.Append("       TARIFF_GRP_MST_TBL      TRF,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL   FEMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL     CTMT");
                sb.Append(" WHERE CCTA.CONT_CUST_AIR_FK = CCAT.CONT_CUST_AIR_PK");
                sb.Append("   AND CCAF.CONT_CUST_TRN_AIR_FK = CCTA.CONT_CUST_TRN_AIR_PK");
                sb.Append("   AND CCTA.TARIFF_GRP_FK = TRF.TARIFF_GRP_MST_PK");
                sb.Append("   AND CCAB.CONT_CUST_AIR_FRT_FK = CCAF.CONT_CUST_AIR_FREIGHT_PK");
                sb.Append("   AND AST.AIRFREIGHT_SLABS_TBL_PK = CCAB.AIRFREIGHT_SLABS_FK");
                sb.Append("   AND CCAF.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND CCAF.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
                sb.Append("   AND CCAT.CONT_CUST_AIR_PK =" + CustContractAirPk);
                sb.Append("   ORDER BY AST.SEQUENCE_NO ");
                return objWK.GetDataSet(sb.ToString());
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

        #endregion "Fetch FreightDetails"

        #region "Fetch OtherChargeDetails"

        /// <summary>
        /// Fetches the other charge details.
        /// </summary>
        /// <param name="CustContractOtherPk">The customer contract other pk.</param>
        /// <returns></returns>
        public DataSet FetchOtherChargeDetails(long CustContractOtherPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT_ID,");
                sb.Append("       CTMT.CURRENCY_ID,");
                sb.Append("       DECODE(CAOC.CHARGE_BASIS,1,'%',2,'Flat rate',3,'Kgs',4,'Unit') CHARGE_BASIS,");
                sb.Append("       CAOC.CURRENT_RATE,");
                sb.Append("       CAOC.APPROVED_RATE REQ_RATE");
                sb.Append("  FROM CONT_CUST_AIR_TBL   CCAT,");
                sb.Append("       CONT_CUST_TRN_AIR_TBL    CTAL,");
                sb.Append("        CONT_CUST_AIR_OTH_CHRG   CAOC,");
                sb.Append("        FREIGHT_ELEMENT_MST_TBL  FEMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT");
                sb.Append(" WHERE CCAT.CONT_CUST_AIR_PK = CTAL.CONT_CUST_AIR_FK");
                sb.Append("   AND CAOC.CONT_CUST_TRN_AIR_FK = CTAL.CONT_CUST_TRN_AIR_PK");
                sb.Append("   AND CAOC.FREIGHT_ELEMENT_MST_FK= FEMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("   AND CAOC.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
                sb.Append("   AND CCAT.CONT_CUST_AIR_PK =" + CustContractOtherPk);
                return objWK.GetDataSet(sb.ToString());
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

        #endregion "Fetch OtherChargeDetails"

        #region "Filter Data"

        /// <summary>
        /// Filters the data.
        /// </summary>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="strPol">The string pol.</param>
        /// <param name="strPod">The string pod.</param>
        public void FilterData(DataSet dsGrid, string strPol, string strPod)
        {
            string[] arrPol = null;
            string[] arrPod = null;
            DataSet dsGridClone = new DataSet();
            DataRow drClone = null;
            bool boolParentEntered = false;
            int intRowCnt = 0;
            int intGridRowCnt = 0;
            int intColCnt = 0;
            try
            {
                dsGridClone = dsGrid.Clone();
                arrPol = strPol.Split(Convert.ToChar(","));
                arrPod = strPod.Split(Convert.ToChar(","));

                for (intRowCnt = 0; intRowCnt <= arrPol.Length - 1; intRowCnt++)
                {
                    boolParentEntered = false;

                    for (intGridRowCnt = 0; intGridRowCnt <= dsGrid.Tables["Parent"].Rows.Count - 1; intGridRowCnt++)
                    {
                        if (Convert.ToInt64(arrPol[intRowCnt]) == Convert.ToInt64(dsGrid.Tables["Parent"].Rows[intGridRowCnt]["POLPK"]) & Convert.ToInt64(arrPod[intRowCnt]) == Convert.ToInt64(dsGrid.Tables["Parent"].Rows[intGridRowCnt]["PODPK"]))
                        {
                            boolParentEntered = true;
                            drClone = dsGridClone.Tables["Parent"].NewRow();

                            for (intColCnt = 0; intColCnt <= dsGrid.Tables["Parent"].Columns.Count - 1; intColCnt++)
                            {
                                drClone[intColCnt] = dsGrid.Tables["Parent"].Rows[intGridRowCnt][intColCnt];
                            }

                            dsGridClone.Tables["Parent"].Rows.Add(drClone);
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }

                    if (boolParentEntered)
                    {
                        for (intGridRowCnt = 0; intGridRowCnt <= dsGrid.Tables["Child"].Rows.Count - 1; intGridRowCnt++)
                        {
                            if (Convert.ToInt64(arrPol[intRowCnt]) == Convert.ToInt64(dsGrid.Tables["Child"].Rows[intGridRowCnt]["POLPK"]) & Convert.ToInt64(arrPod[intRowCnt]) == Convert.ToInt64(dsGrid.Tables["Child"].Rows[intGridRowCnt]["PODPK"]))
                            {
                                drClone = dsGridClone.Tables["Child"].NewRow();

                                for (intColCnt = 0; intColCnt <= dsGrid.Tables["Child"].Columns.Count - 1; intColCnt++)
                                {
                                    drClone[intColCnt] = dsGrid.Tables["Child"].Rows[intGridRowCnt][intColCnt];
                                }

                                dsGridClone.Tables["Child"].Rows.Add(drClone);
                            }
                        }
                    }
                }
                dsGrid = dsGridClone;
                dsGridClone = null;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Filter Data"

        #region "Fetch Current Status"

        /// <summary>
        /// Fetches the status.
        /// </summary>
        /// <param name="PK_VAL">The p k_ value.</param>
        /// <returns></returns>
        public int FetchStatus(int PK_VAL)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append(" SELECT CT.CONT_APPROVED ");
                sb.Append("  FROM CONT_CUST_AIR_TBL CT ");
                sb.Append("   WHERE CT.CONT_CUST_AIR_PK =  " + PK_VAL);
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "Fetch Current Status"

        #region "PortGroup"

        /// <summary>
        /// Fetches from port group.
        /// </summary>
        /// <param name="QuotPK">The quot pk.</param>
        /// <param name="PortGrpPK">The port GRP pk.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <returns></returns>
        public DataSet FetchFromPortGroup(int QuotPK = 0, int PortGrpPK = 0, string POLPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append("SELECT POL.PORT_MST_PK,");
                    sb.Append("       POL.PORT_ID,");
                    sb.Append("       T.POL_GRP_FK ");
                    sb.Append("  FROM CONT_CUST_TRN_AIR_TBL T, PORT_MST_TBL POL");
                    sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND T.CONT_CUST_AIR_FK = " + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POL_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 1 AND P.ACTIVE_FLAG = 1 ");
                    if (POLPK != "0")
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
        /// <param name="QuotPK">The quot pk.</param>
        /// <param name="PortGrpPK">The port GRP pk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <returns></returns>
        public DataSet FetchToPortGroup(int QuotPK = 0, int PortGrpPK = 0, string PODPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append("SELECT POD.PORT_MST_PK,");
                    sb.Append("       POD.PORT_ID,");
                    sb.Append("       T.POD_GRP_FK ");
                    sb.Append("  FROM CONT_CUST_TRN_AIR_TBL T, PORT_MST_TBL POD");
                    sb.Append(" WHERE T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND T.CONT_CUST_AIR_FK = " + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POD_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 1 AND P.ACTIVE_FLAG = 1");
                    if (PODPK != "0")
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
        /// <param name="QuotPK">The quot pk.</param>
        /// <param name="PortGrpPK">The port GRP pk.</param>
        /// <param name="TariffPK">The tariff pk.</param>
        /// <returns></returns>
        public DataSet FetchTariffGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append("SELECT POL.PORT_MST_PK POL_PK,");
                    sb.Append("       POL.PORT_ID POL_ID,");
                    sb.Append("       POD.PORT_MST_PK POD_PK,");
                    sb.Append("       POD.PORT_ID POD_ID,");
                    sb.Append("       T.POL_GRP_FK,");
                    sb.Append("       T.POD_GRP_FK,T.TARIFF_GRP_FK");
                    sb.Append("  FROM CONT_CUST_TRN_AIR_TBL T, PORT_MST_TBL POL, PORT_MST_TBL POD");
                    sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND T.CONT_CUST_TRN_AIR_PK = " + QuotPK);
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
        /// Fetches the tariff pod GRP.
        /// </summary>
        /// <param name="QuotPK">The quot pk.</param>
        /// <param name="PortGrpPK">The port GRP pk.</param>
        /// <param name="TariffPK">The tariff pk.</param>
        /// <returns></returns>
        public DataSet FetchTariffPODGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK, T.TARIFF_GRP_FK FROM QUOTATION_TRN_SEA_FCL_LCL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.TARIFF_GRP_FK = " + TariffPK);
                    sb.Append(" AND T.QUOTATION_SEA_FK =" + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK,");
                    sb.Append("        P.PORT_ID,");
                    sb.Append("        TGM.POD_GRP_MST_FK POD_GRP_FK,");
                    sb.Append("        TGM.TARIFF_GRP_MST_PK");
                    sb.Append("  FROM PORT_MST_TBL P, TARIFF_GRP_TRN_TBL TGT, TARIFF_GRP_MST_TBL TGM");
                    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                    sb.Append("   AND P.PORT_MST_PK = TGT.POD_MST_FK");
                    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                    sb.Append("   AND P.BUSINESS_TYPE = 1");
                    sb.Append("   AND P.ACTIVE_FLAG = 1");
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
        /// Fetches the PRT group.
        /// </summary>
        /// <param name="ContPK">The cont pk.</param>
        /// <returns></returns>
        public string FetchPrtGroup(int ContPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT NVL(C.PORT_GROUP,0) PORT_GROUP FROM CONT_CUST_AIR_TBL C WHERE C.CONT_CUST_AIR_PK = " + ContPK;
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

        /// <summary>
        /// Fetches the freight grid pk.
        /// </summary>
        /// <param name="CCPK">The CCPK.</param>
        /// <param name="CCTrnFK">The cc TRN fk.</param>
        /// <param name="CCFreightFK">The cc freight fk.</param>
        /// <returns></returns>
        public long FetchFreightGridPK(string CCPK, int CCTrnFK, int CCFreightFK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT CFT.CONT_SUR_CHRG_SEA_PK ");
                sb.Append("  FROM CONT_SUR_CHRG_SEA_TBL CFT,");
                sb.Append("       CONT_CUST_SEA_TBL     CMT,");
                sb.Append("       CONT_CUST_TRN_SEA_TBL CTT");
                sb.Append(" WHERE CMT.CONT_CUST_SEA_PK = CTT.CONT_CUST_SEA_FK");
                sb.Append("   AND CTT.CONT_CUST_TRN_SEA_PK = CFT.CONT_CUST_TRN_SEA_FK");
                sb.Append("   AND CMT.CONT_CUST_SEA_PK = " + CCPK);
                sb.Append("   AND CTT.CONT_CUST_TRN_SEA_PK = " + CCTrnFK);
                sb.Append("   AND CFT.FREIGHT_ELEMENT_MST_FK = " + CCFreightFK);

                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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
        /// Fetches the TRN grid pk.
        /// </summary>
        /// <param name="CCPK">The CCPK.</param>
        /// <param name="CCPOLFK">The ccpolfk.</param>
        /// <param name="CCPODFK">The ccpodfk.</param>
        /// <returns></returns>
        public long FetchTrnGridPK(string CCPK, int CCPOLFK, int CCPODFK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append("SELECT CTT.CONT_CUST_TRN_AIR_PK");
                sb.Append("  FROM CONT_CUST_AIR_TBL CMT, CONT_CUST_TRN_AIR_TBL CTT");
                sb.Append(" WHERE CMT.CONT_CUST_AIR_PK = CTT.CONT_CUST_AIR_FK");
                sb.Append("   AND CMT.CONT_CUST_AIR_PK = " + CCPK);
                sb.Append("   AND CTT.PORT_MST_POL_FK = " + CCPOLFK);
                sb.Append("   AND CTT.PORT_MST_POD_FK = " + CCPODFK);

                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #region "Fetch COntract Nmber"

        /// <summary>
        /// Fetches the contract.
        /// </summary>
        /// <param name="strRFQNo">The string RFQ no.</param>
        /// <returns></returns>
        public string FetchContract(string strRFQNo)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT NVL(MAX(CCAT.CONT_REF_NO),0) FROM CONT_CUST_AIR_TBL CCAT " + "WHERE CCAT.CONT_REF_NO LIKE '" + strRFQNo + "/%' " + "ORDER BY CCAT.CONT_REF_NO";
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

        #endregion "Fetch COntract Nmber"

        #region "Deactive Existing Cntract"

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

            strSQL = "UPDATE CONT_CUST_AIR_TBL T " + "SET T.CONT_APPROVED=4,T.ACTIVE=0,T.VALID_TO = TO_DATE(SYSDATE,'dd/mm/yyyy')," + "T.LAST_MODIFIED_BY_FK =" + M_CREATED_BY_FK + "," + "T.LAST_MODIFIED_DT = SYSDATE," + "T.VERSION_NO = T.VERSION_NO + 1" + "WHERE T.CONT_CUST_AIR_PK =" + ContractPk;
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

        #endregion "Deactive Existing Cntract"
    }
}