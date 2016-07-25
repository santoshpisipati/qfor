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
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_SRRAirContract : CommonFeatures
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
        private int _Col_Incr;

        #endregion " Private Variables "

        /// <summary>
        /// The _ approved_ rate
        /// </summary>
        private int _Approved_Rate;

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

        #region " Constructor "

        /// <summary>
        /// Initializes a new instance of the <see cref="Cls_SRRAirContract"/> class.
        /// </summary>
        /// <param name="Static_Col">The static_ col.</param>
        /// <param name="Col_Incr">The col_ incr.</param>
        /// <param name="Approved_Rate">The approved_ rate.</param>
        public Cls_SRRAirContract(int Static_Col, int Col_Incr, int Approved_Rate)
        {
            _Static_Col = Static_Col;
            _Col_Incr = Col_Incr;
            _Approved_Rate = Approved_Rate;
        }

        #endregion " Constructor "

        #region " Enhance Search Function for AIRLINE TARIFF "

        /// <summary>
        /// Fetches the airline tariff.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public static string FetchAirlineTariff(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string[] arr = null;
            long AirPk = 0;
            long CommdityPk = 0;
            string Pol = null;
            string Pod = null;
            string LookUp_Value = null;
            string ValidTo = null;
            string ValidFrom = null;
            string strReturn = null;
            string strCondition = null;
            arr = strCond.Split(Convert.ToChar("~"));
            LookUp_Value = arr[0];
            AirPk = Convert.ToInt64(arr[1]);
            CommdityPk = Convert.ToInt64(arr[2]);
            ValidFrom = arr[3];
            ValidTo = arr[4];
            Pol = arr[5];
            if (!(Pol == "n") & !string.IsNullOrEmpty(Pol) & !(Pol == "undefined"))
            {
                Pod = arr[6];
            }
            else
            {
                Pod = "";
            }
            strCondition = MakePortPairString(Pol, Pod);
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_OPERATOR_AIR_TARIFF.GETOPERATORTARIFF_AIR";

                var _with1 = selectCommand.Parameters;
                _with1.Add("AIR_PK_IN", (AirPk <= 0 ? 0 : AirPk)).Direction = ParameterDirection.Input;
                _with1.Add("COMMODITY_GROUP_PK_IN", CommdityPk).Direction = ParameterDirection.Input;
                _with1.Add("VALID_FROM", ValidFrom).Direction = ParameterDirection.Input;
                _with1.Add("VALID_TO", (ValidTo == "n" ? "" : ValidTo)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", LookUp_Value).Direction = ParameterDirection.Input;
                _with1.Add("CONDITION_IN", strCondition).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "~" + ex.Message;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search Function for AIRLINE TARIFF "

        #region " Enhance Search Function for Customer Contract(Air) "

        /// <summary>
        /// Fetch_s the customer_ contract_ air.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public static string Fetch_Customer_Contract_Air(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string[] arr = null;
            long intAirPk = 0;
            long intCustomerPk = 0;
            long intCommodityGroupPk = 0;
            string Pol = null;
            string Pod = null;
            string LookUp_Value = null;
            string ValidTo = null;
            string ValidFrom = null;
            string strReturn = null;
            string strCondition = null;
            arr = strCond.Split(Convert.ToChar("~"));
            LookUp_Value = arr[0];
            intAirPk = Convert.ToInt64(arr[1]);
            intCustomerPk = Convert.ToInt64(arr[2]);
            intCommodityGroupPk = Convert.ToInt64(arr[3]);
            ValidFrom = arr[4];
            ValidTo = arr[5];
            Pol = arr[6];

            if (!(Pol == "n") & !string.IsNullOrEmpty(Pol) & !(Pol == "undefined"))
            {
                Pod = arr[7];
            }
            else
            {
                Pod = "";
            }

            strCondition = MakePortPairString(Pol, Pod);

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CONT_REF_NO_PKG.GET_CUSTOMER_CONTRACT";

                var _with2 = selectCommand.Parameters;
                _with2.Add("AIRLINE_MST_FK_IN", intAirPk).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMER_MST_FK_IN", intCustomerPk).Direction = ParameterDirection.Input;
                _with2.Add("COMMODITY_GROUP_MST_FK_IN", intCommodityGroupPk).Direction = ParameterDirection.Input;
                _with2.Add("VALID_FROM_IN", ValidFrom).Direction = ParameterDirection.Input;
                _with2.Add("VALID_TO_IN", ValidTo).Direction = ParameterDirection.Input;
                _with2.Add("PORT_PAIR", strCondition).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", LookUp_Value).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "~" + ex.Message;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search Function for Customer Contract(Air) "

        #region " Enhanced seardh function for Spot Rate "

        /// <summary>
        /// Fetch_s the spot rate.
        /// </summary>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public static string Fetch_SpotRate(string strCondition)
        {
            string[] arr = null;
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = objWF.MyCommand;
            arr = strCondition.Split(Convert.ToChar("~"));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_RFQ_REF_NO_PKG.AVIALABLE_RFQ_SPOT_RATE";

                var _with3 = selectCommand.Parameters;

                _with3.Add("LOOKUP_VALUE_IN", arr[0]).Direction = ParameterDirection.Input;
                _with3.Add("AIRLINE_MST_FK_IN", Convert.ToInt64(arr[1])).Direction = ParameterDirection.Input;
                _with3.Add("CUSTOMER_MST_FK_IN", Convert.ToInt64(arr[2])).Direction = ParameterDirection.Input;
                _with3.Add("POL_FK_IN", Convert.ToInt64(arr[3])).Direction = ParameterDirection.Input;
                _with3.Add("POD_FK_IN", Convert.ToInt64(arr[4])).Direction = ParameterDirection.Input;
                _with3.Add("VALID_FROM_IN", arr[5]).Direction = ParameterDirection.Input;
                _with3.Add("VALID_TO_IN", arr[6]).Direction = ParameterDirection.Input;
                _with3.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(arr[7])).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1500, "RETURN_VALUE").Direction = ParameterDirection.Output;

                selectCommand.ExecuteNonQuery();
                return Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "~" + ex.Message;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhanced seardh function for Spot Rate "

        #region " Fetch Queries "

        #region " Fetch Transaction "

        /// <summary>
        /// Fetch_s the SRR.
        /// </summary>
        /// <param name="lngSrrPK">The LNG SRR pk.</param>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        /// <param name="oGroup">The o group.</param>
        public void Fetch_Srr(long lngSrrPK, DataSet dsGrid, string ChargeBasis, string AirSuchargeToolTip, int oGroup = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtDataTable = null;
            DataTable dtPort = null;
            DataTable dtOtherCharge = null;
            Quantum_QFOR.cls_AirlineTariffEntry objTariff = new Quantum_QFOR.cls_AirlineTariffEntry(_Static_Col, _Col_Incr, true);
            try
            {
                ChargeBasis = "";
                AirSuchargeToolTip = "";

                objTariff.Create_Static_Column(dsGrid);
                objWF.MyCommand.Parameters.Clear();
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("SRR_AIR_PK_IN", lngSrrPK).Direction = ParameterDirection.Input;
                _with4.Add("SHOW_APPROVED", _Approved_Rate).Direction = ParameterDirection.Input;
                _with4.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (oGroup == 0)
                {
                    dtDataTable = objWF.GetDataTable("FETCH_SRR_AIR_PKG", "SRR_FREIGHT");
                }
                else
                {
                    dtDataTable = objWF.GetDataTable("FETCH_SRR_AIR_PKG", "SRR_FREIGHT_GROUP");
                }

                objTariff.Make_Column(dsGrid.Tables["Child"], dtDataTable, true);
                objTariff.Populate_Child(dsGrid.Tables["Child"], dtDataTable);

                objWF.MyCommand.Parameters.Clear();
                var _with5 = objWF.MyCommand.Parameters;
                _with5.Add("SRR_AIR_PK_IN", lngSrrPK).Direction = ParameterDirection.Input;
                _with5.Add("SHOW_APPROVED", _Approved_Rate).Direction = ParameterDirection.Input;
                _with5.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (oGroup == 0)
                {
                    dtDataTable = objWF.GetDataTable("FETCH_SRR_AIR_PKG", "SRR_AIR_SURCHAGE");
                }
                else
                {
                    dtDataTable = objWF.GetDataTable("FETCH_SRR_AIR_PKG", "SRR_AIR_SURCHAGE_GROUP");
                }

                objTariff.Make_Column(dsGrid.Tables["Parent"], dtDataTable, false, ChargeBasis, AirSuchargeToolTip);
                dsGrid.Tables["Parent"].Columns.Add("Oth. Chrg", typeof(string));
                dsGrid.Tables["Parent"].Columns.Add("Oth_Chrg_Val", typeof(string));
                objWF.MyCommand.Parameters.Clear();
                var _with6 = objWF.MyCommand.Parameters;
                _with6.Add("SRR_AIR_PK_IN", lngSrrPK).Direction = ParameterDirection.Input;
                _with6.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (oGroup == 0)
                {
                    dtPort = objWF.GetDataTable("FETCH_SRR_AIR_PKG", "SRR_PORT");
                }
                else
                {
                    dtPort = objWF.GetDataTable("FETCH_SRR_AIR_PKG", "SRR_PORT_GROUP");
                }
                objWF.MyCommand.Parameters.Clear();
                var _with7 = objWF.MyCommand.Parameters;
                _with7.Add("SRR_AIR_PK_IN", lngSrrPK).Direction = ParameterDirection.Input;
                _with7.Add("SHOW_APPROVED", _Approved_Rate).Direction = ParameterDirection.Input;
                _with7.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtOtherCharge = objWF.GetDataTable("FETCH_SRR_AIR_PKG", "SRR_OTH_CHARGES");

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
        /// Fetch_s the SRR_ header.
        /// </summary>
        /// <param name="lngSrrPK">The LNG SRR pk.</param>
        /// <param name="dtTable">The dt table.</param>
        public void Fetch_Srr_Header(long lngSrrPK, DataTable dtTable)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with8 = objWF.MyCommand.Parameters;
                _with8.Add("SRR_AIR_PK_IN", lngSrrPK).Direction = ParameterDirection.Input;
                _with8.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTable = objWF.GetDataTable("FETCH_SRR_AIR_PKG", "SRR_MAIN");
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

        #region " Fetch SpotRate "

        /// <summary>
        /// Fetch_s the spot rate.
        /// </summary>
        /// <param name="lngSrrPK">The LNG SRR pk.</param>
        /// <param name="dtTable">The dt table.</param>
        public void Fetch_SpotRate(long lngSrrPK, DataTable dtTable)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with9 = objWF.MyCommand.Parameters;
                _with9.Add("SRR_AIR_PK_IN", lngSrrPK).Direction = ParameterDirection.Input;
                _with9.Add("SRR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTable = objWF.GetDataTable("FETCH_SRR_AIR_PKG", "SRR_SPOT_RATE");
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

        #endregion " Fetch SpotRate "

        #region "Fetch BL Clause etc "

        /// <summary>
        /// Fetches the clause.
        /// </summary>
        /// <param name="intCustContPk">The int customer cont pk.</param>
        /// <returns></returns>
        public DataSet FetchClause(long intCustContPk)
        {
            StringBuilder str = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataSet dsClause = new DataSet();
            try
            {
                str.Append(" select cust.cont_clause, cust.credit_period, ");
                str.Append(" loc.location_mst_pk,loc.location_id,loc.location_name ");
                str.Append(" from cont_cust_air_tbl cust, ");
                str.Append(" location_mst_tbl Loc  ");
                str.Append(" where loc.location_mst_pk(+) = cust.pymt_location_mst_fk ");
                str.Append(" and cust.cont_cust_air_pk = " + intCustContPk + " ");
                dsClause = objWF.GetDataSet(str.ToString());
                return dsClause;
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

        #endregion "Fetch BL Clause etc "

        #endregion " Fetch Queries "

        #region " Save "

        /// <summary>
        /// Saves the HDR.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="txtSRRRefNo">The text SRR reference no.</param>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmpId">The n emp identifier.</param>
        /// <param name="Mode">The mode.</param>
        /// <param name="Remarks">The remarks.</param>
        /// <param name="Restrict">The restrict.</param>
        /// <param name="sid">The sid.</param>
        /// <param name="polid">The polid.</param>
        /// <param name="podid">The podid.</param>
        /// <returns></returns>
        public ArrayList SaveHDR(DataSet dsMain, System.Web.UI.WebControls.TextBox txtSRRRefNo, long nLocationId, long nEmpId, string Mode, string Remarks = "", Int16 Restrict = 0, string sid = "", string polid = "", string podid = "")
        {
            string SrrRefNo = null;
            bool IsUpdate = false;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            string strCondition = null;
            string[] arr = null;

            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                if (string.IsNullOrEmpty(txtSRRRefNo.Text))
                {
                    SrrRefNo = GenerateSRRNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, sid, polid, podid);
                    if (SrrRefNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                    }
                }
                else
                {
                    SrrRefNo = txtSRRRefNo.Text;
                }
                objWK.MyCommand.Parameters.Clear();
                strCondition = dsMain.Tables["tblMaster"].Rows[0]["STRCONDITION"].ToString();
                arr = strCondition.Split(Convert.ToChar("~"));
                strCondition = Cls_SRRAirContract.MakePortPairString(arr[0], arr[1]);

                var _with10 = objWK.MyCommand;

                if (Mode == "NEW" | Mode == "FETCHED")
                {
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = objWK.MyUserName + ".SRR_AIR_TBL_PKG.SRR_AIR_TBL_INS";
                    _with10.Parameters.Clear();
                    _with10.Parameters.Add("SRR_REF_NO_IN", SrrRefNo).Direction = ParameterDirection.Input;

                    _with10.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    _with10.Parameters.Add("CONT_CUST_AIR_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["CONT_CUST_AIR_FK"]).Direction = ParameterDirection.Input;
                }
                else
                {
                    IsUpdate = true;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = objWK.MyUserName + ".SRR_AIR_TBL_PKG.SRR_AIR_TBL_UPD";
                    _with10.Parameters.Clear();
                    _with10.Parameters.Add("SRR_AIR_PK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["SRR_AIR_PK"])).Direction = ParameterDirection.Input;

                    _with10.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    _with10.Parameters.Add("VERSION_NO_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["VERSION_NO"])).Direction = ParameterDirection.Input;
                }
                _with10.Parameters.Add("AIRLINE_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["AIRLINE_MST_FK"]).Direction = ParameterDirection.Input;

                //CUSTOMER_MST_FK_IN
                _with10.Parameters.Add("CUSTOMER_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_MST_FK"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblMaster"].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("STRCONDITION", strCondition).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("COMMODITY_GROUP_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_MST_FK"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("COMMODITY_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("SRR_CLAUSE_IN", dsMain.Tables["tblMaster"].Rows[0]["SRR_CLAUSE"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("SRR_REMARKS_IN", dsMain.Tables["tblMaster"].Rows[0]["SRR_REMARKS"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("COL_ADDRESS_IN", dsMain.Tables["tblMaster"].Rows[0]["COL_ADDRESS"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("PYMT_LOCATION_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["PYMT_LOCATION_MST_FK"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("CREDIT_PERIOD_IN", dsMain.Tables["tblMaster"].Rows[0]["CREDIT_PERIOD"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("APPROVED_BY_IN", dsMain.Tables["tblMaster"].Rows[0]["APPROVED_BY"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("APPROVED_DATE_IN", dsMain.Tables["tblMaster"].Rows[0]["APPROVED_DATE"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("SRR_APPROVED_IN", dsMain.Tables["tblMaster"].Rows[0]["SRR_APPROVED"]).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("RESTRICTED_IN", Restrict).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with10.Parameters.Add("ACTIVE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["ACTIVE"])).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("PORT_GROUP_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["PORT_GROUP"])).Direction = ParameterDirection.Input;
                _with10.Parameters.Add("SRR_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["SRR_TYPE"])).Direction = ParameterDirection.Input;
                _with10.ExecuteNonQuery();

                if (string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "SRR") > 0 | string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "modified") > 0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    if (!IsUpdate)
                    {
                        RollbackProtocolKey("SRR AIR", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), SrrRefNo, System.DateTime.Now);
                    }
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }

                arrMessage = SaveSrrTRN(dsMain, objWK, IsUpdate);

                if ((HttpContext.Current.Session["SessionLocalCharges"] != null))
                {
                    SaveLocalCharges(objWK.MyCommand, objWK.MyUserName, (DataSet)HttpContext.Current.Session["SessionLocalCharges"], _PkValue, 1);
                }

                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        txtSRRRefNo.Text = SrrRefNo;
                        return arrMessage;
                    }
                    else
                    {
                        if (!IsUpdate)
                        {
                            RollbackProtocolKey("SRR AIR", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), SrrRefNo, System.DateTime.Now);
                        }
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                if (!IsUpdate)
                {
                    RollbackProtocolKey("SRR AIR", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), SrrRefNo, System.DateTime.Now);
                }
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
            return new ArrayList();
        }

        /// <summary>
        /// Saves the SRR TRN.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="objWK">The object wk.</param>
        /// <param name="IsUpdate">if set to <c>true</c> [is update].</param>
        /// <returns></returns>
        private ArrayList SaveSrrTRN(DataSet dsMain, WorkFlow objWK, bool IsUpdate)
        {
            Int32 nRowCnt = default(Int32);
            long intCurrentPk = 0;
            objWK.MyCommand.Parameters.Clear();
            try
            {
                if (!IsUpdate)
                {
                    var _with11 = objWK.MyCommand;

                    _with11.CommandType = CommandType.StoredProcedure;
                    _with11.CommandText = objWK.MyUserName + ".SRR_AIR_TBL_PKG.SRR_TRN_AIR_TBL_INS";

                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        _with11.Parameters.Clear();

                        //SRR_AIR_FK_IN
                        _with11.Parameters.Add("SRR_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;

                        //PORT_MST_POL_FK_IN
                        _with11.Parameters.Add("PORT_MST_POL_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POL_FK"]).Direction = ParameterDirection.Input;

                        //PORT_MST_POD_FK_IN
                        _with11.Parameters.Add("PORT_MST_POD_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PORT_MST_POD_FK"]).Direction = ParameterDirection.Input;

                        //POL_GRP_FK_IN
                        _with11.Parameters.Add("POL_GRP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POL_GRP_FK"]).Direction = ParameterDirection.Input;

                        //POD_GRP_FK_IN
                        _with11.Parameters.Add("POD_GRP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["POD_GRP_FK"]).Direction = ParameterDirection.Input;

                        //TARIFF_GRP_FK_IN
                        _with11.Parameters.Add("TARIFF_GRP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TARIFF_GRP_FK"]).Direction = ParameterDirection.Input;

                        //CURRENCY_MST_FK_IN
                        _with11.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;

                        //Valid From date
                        _with11.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_FROM"]).Direction = ParameterDirection.Input;

                        //VALID_TO_IN
                        _with11.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"]).Direction = ParameterDirection.Input;

                        //AIR_OTH_CHRG_IN
                        _with11.Parameters.Add("AIR_OTH_CHRG_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_OTH_CHRG"]).Direction = ParameterDirection.Input;

                        //AIR_BRK_PNTS_IN
                        _with11.Parameters.Add("AIR_BRK_PNTS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_BRK_PNTS"]).Direction = ParameterDirection.Input;

                        //AIR_SURCHRG_IN
                        //.Parameters.Add("AIR_SURCHRG_IN", _
                        //dsMain.Tables("tblTransaction").Rows(nRowCnt).Item("AIR_SURCHRG")).Direction = _
                        //ParameterDirection.Input
                        if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_SURCHRG"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_SURCHRG"])))
                            {
                                _with11.Parameters.Add("AIR_SURCHRG_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_SURCHRG"]).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with11.Parameters.Add("AIR_SURCHRG_IN", "").Direction = ParameterDirection.Input;
                            }
                        }
                        else
                        {
                            _with11.Parameters.Add("AIR_SURCHRG_IN", "").Direction = ParameterDirection.Input;
                        }

                        //AIR_FREIGHT_IN
                        _with11.Parameters.Add("AIR_FREIGHT_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_FREIGHT"]).Direction = ParameterDirection.Input;

                        //EXPECTED_VOLUME_IN
                        _with11.Parameters.Add("EXPECTED_VOLUME_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["EXPECTED_VOLUME"]).Direction = ParameterDirection.Input;

                        //EXPECTED_WEIGHT_IN
                        _with11.Parameters.Add("EXPECTED_WEIGHT_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["EXPECTED_WEIGHT"]).Direction = ParameterDirection.Input;

                        //Return value of the proc.
                        _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with11.ExecuteNonQuery();
                    }
                }
                else
                {
                    var _with12 = objWK.MyCommand;

                    _with12.CommandType = CommandType.StoredProcedure;
                    _with12.CommandText = objWK.MyUserName + ".SRR_AIR_TBL_PKG.SRR_TRN_AIR_TBL_UPD";

                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        _with12.Parameters.Clear();

                        _with12.Parameters.Add("SRR_TRN_AIR_PK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SRR_TRN_AIR_PK"]).Direction = ParameterDirection.Input;

                        //SPOT_RATE_FK_IN
                        _with12.Parameters.Add("SPOT_RATE_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SPOT_RATE_FK"]).Direction = ParameterDirection.Input;

                        //CURRENCY_MST_FK_IN
                        _with12.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;

                        //COUNT_IN
                        if (intCurrentPk == Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SRR_TRN_AIR_PK"]))
                        {
                            _with12.Parameters.Add("COUNT_IN", Convert.ToInt64(1)).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            intCurrentPk = Convert.ToInt64(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SRR_TRN_AIR_PK"]);

                            _with12.Parameters.Add("COUNT_IN", Convert.ToInt64(0)).Direction = ParameterDirection.Input;
                        }

                        //Valid From date
                        _with12.Parameters.Add("VALID_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_FROM"]).Direction = ParameterDirection.Input;

                        //VALID_TO_IN
                        _with12.Parameters.Add("VALID_TO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VALID_TO"]).Direction = ParameterDirection.Input;

                        //AIR_OTH_CHRG_IN
                        _with12.Parameters.Add("AIR_OTH_CHRG_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_OTH_CHRG"]).Direction = ParameterDirection.Input;

                        //AIR_BRK_PNTS_IN
                        _with12.Parameters.Add("AIR_BRK_PNTS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_BRK_PNTS"]).Direction = ParameterDirection.Input;

                        if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_SURCHRG"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_SURCHRG"])))
                            {
                                _with12.Parameters.Add("AIR_SURCHRG_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_SURCHRG"]).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with12.Parameters.Add("AIR_SURCHRG_IN", "").Direction = ParameterDirection.Input;
                            }
                        }
                        else
                        {
                            _with12.Parameters.Add("AIR_SURCHRG_IN", "").Direction = ParameterDirection.Input;
                        }

                        //AIR_FREIGHT_IN
                        _with12.Parameters.Add("AIR_FREIGHT_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["AIR_FREIGHT"]).Direction = ParameterDirection.Input;

                        //EXPECTED_VOLUME_IN
                        _with12.Parameters.Add("EXPECTED_VOLUME_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["EXPECTED_VOLUME"]).Direction = ParameterDirection.Input;

                        //EXPECTED_WEIGHT_IN
                        _with12.Parameters.Add("EXPECTED_WEIGHT_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["EXPECTED_WEIGHT"]).Direction = ParameterDirection.Input;

                        //RETURN_VALUE
                        _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with12.ExecuteNonQuery();
                    }
                }

                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        /// <summary>
        /// Generates the SRR no.
        /// </summary>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmployeeId">The n employee identifier.</param>
        /// <param name="nCreatedBy">The n created by.</param>
        /// <param name="objWK">The object wk.</param>
        /// <param name="SID">The sid.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="PODID">The podid.</param>
        /// <returns></returns>
        public string GenerateSRRNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK, string SID = "", string POLID = "", string PODID = "")
        {
            string functionReturnValue = null;
            try
            {
                functionReturnValue = GenerateProtocolKey("SRR AIR", nLocationId, nEmployeeId, DateTime.Now, "", "", POLID, nCreatedBy, objWK, SID,
                PODID);
                return functionReturnValue;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        #endregion " Save "

        #region " Shared String for Sector "

        /// <summary>
        /// Makes the port pair string.
        /// </summary>
        /// <param name="strPol">The string pol.</param>
        /// <param name="strPod">The string pod.</param>
        /// <param name="OperPKs">The oper p ks.</param>
        /// <returns></returns>
        public static string MakePortPairString(string strPol, string strPod, string OperPKs = "")
        {
            string[] arr = null;
            int intPOLCnt = 0;
            int intPODCnt = 0;
            System.Text.StringBuilder builderCondition = new System.Text.StringBuilder();
            string[] Pol = null;
            string[] Pod = null;
            string[] OperPK = null;
            try
            {
                if (string.IsNullOrEmpty(OperPKs))
                {
                    if (!(strPol == "n") & !string.IsNullOrEmpty(strPol) & !(strPol == "undefined"))
                    {
                        Pol = strPol.Split(Convert.ToChar(","));
                        Pod = strPod.Split(Convert.ToChar(","));
                        for (intPOLCnt = 0; intPOLCnt <= Pol.Length - 1; intPOLCnt++)
                        {
                            for (intPODCnt = 0; intPODCnt <= Pod.Length - 1; intPODCnt++)
                            {
                                builderCondition.Append("(" + Pol[intPOLCnt] + "," + Pod[intPODCnt] + "),");
                            }
                        }
                    }
                }
                else
                {
                    if (!(strPol == "n") & !string.IsNullOrEmpty(strPol) & !(strPol == "undefined"))
                    {
                        Pol = strPol.Split(Convert.ToChar(","));
                        Pod = strPod.Split(Convert.ToChar(","));
                        OperPK = OperPKs.Split(',');
                        for (intPOLCnt = 0; intPOLCnt <= Pol.Length - 1; intPOLCnt++)
                        {
                            for (intPODCnt = 0; intPODCnt <= Pod.Length - 1; intPODCnt++)
                            {
                                for (int k = 0; k <= OperPK.Length - 1; k++)
                                {
                                    builderCondition.Append("(" + Pol[intPOLCnt] + "," + Pod[intPODCnt] + "," + OperPK[k] + "),");
                                }
                            }
                        }
                    }
                }
                return builderCondition.ToString().TrimEnd(Convert.ToChar(","));
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

        #endregion " Shared String for Sector "

        #region "Fetch Header Details"

        /// <summary>
        /// Fetches the header.
        /// </summary>
        /// <param name="SRRAirPk">The SRR air pk.</param>
        /// <returns></returns>
        public DataSet FetchHeader(long SRRAirPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("       SELECT CMT.CUSTOMER_NAME,");
            sb.Append("       CCD.ADM_ADDRESS_1,");
            sb.Append("       CCD.ADM_ADDRESS_2,");
            sb.Append("       CCD.ADM_ADDRESS_3,");
            sb.Append("       CCD.ADM_ZIP_CODE,");
            sb.Append("       CMT.COUNTRY_NAME,");
            sb.Append("       OMT.AIRLINE_NAME,");
            sb.Append("       'AIR' BIZ_TYPE,");
            sb.Append("       CASE WHEN SAT.SRR_TYPE=0 THEN CAT.CONT_REF_NO ELSE TARIFF.Tariff_Ref_No END AS CONT_REF_NO,");
            sb.Append("       SAT.VALID_FROM,");
            sb.Append("       SAT.VALID_TO,");
            sb.Append("       SAT.COL_ADDRESS APPROVED_BY,");
            sb.Append("      DECODE(SAT.SRR_APPROVED,0,'Requested',1,'Internal Approval',2,'Customer Approval')SRR_APPROVED,");
            sb.Append("       DECODE(SAT.SRR_APPROVED,0,CUMT.USER_NAME,1,LUMT.USER_NAME,2,LUMT.USER_NAME) USER_ID,");
            sb.Append("       CT.COMMODITY_NAME,");
            sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
            sb.Append("       CASE WHEN SAT.SRR_TYPE=0 THEN CAT.CONT_DATE ELSE TARIFF.tariff_date END AS CONT_DATE,");
            sb.Append("       SAT.CREDIT_PERIOD,");
            sb.Append("       LMT.LOCATION_NAME,");
            sb.Append("       SAT.SRR_REF_NO, ");
            sb.Append("       AUMT.USER_NAME APPBY_ID,");
            sb.Append("       SAT.APPROVED_DATE,");
            sb.Append("        SAT.SRR_CLAUSE ");
            sb.Append("  FROM SRR_AIR_TBL     SAT,");
            sb.Append("       CUSTOMER_MST_TBL      CMT,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
            sb.Append("       LOCATION_MST_TBL      LMT,");
            sb.Append("       COUNTRY_MST_TBL       CMT,");
            sb.Append("       AIRLINE_MST_TBL      OMT,");
            sb.Append("       COMMODITY_MST_TBL     CT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       USER_MST_TBL            CUMT,");
            sb.Append("       USER_MST_TBL            LUMT,");
            sb.Append("       USER_MST_TBL            AUMT,");
            sb.Append("       CONT_CUST_AIR_TBL       CAT,TARIFF_MAIN_AIR_TBL TARIFF");
            sb.Append(" WHERE SAT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            sb.Append("   AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND LMT.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
            sb.Append("   AND SAT.AIRLINE_MST_FK = OMT.AIRLINE_MST_PK(+)");
            sb.Append("   AND SAT.COMMODITY_MST_FK = CT.COMMODITY_MST_PK(+)");
            sb.Append("   AND SAT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
            sb.Append("   AND SAT.CONT_CUST_AIR_FK =CAT.CONT_CUST_AIR_PK(+) AND SAT.CONT_CUST_AIR_FK = TARIFF.TARIFF_MAIN_AIR_PK(+)");
            sb.Append("   AND SAT.CREATED_BY_FK = CUMT.USER_MST_PK");
            sb.Append("   AND SAT.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+)");
            sb.Append("   AND SAT.APPROVED_BY = AUMT.USER_MST_PK(+)");
            sb.Append("   AND SAT.SRR_AIR_PK = " + SRRAirPk);
            try
            {
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
        /// <param name="SRRAirPk">The SRR air pk.</param>
        /// <returns></returns>
        public DataSet FetchFreightDetails(long SRRAirPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT POL.PORT_NAME   POL,");
            sb.Append("       POD.PORT_NAME   POD,");
            sb.Append("       STAT.VALID_FROM,");
            sb.Append("       STAT.VALID_TO,");
            sb.Append("       STAT.EXPECTED_WEIGHT,");
            sb.Append("       STAT.EXPECTED_VOLUME,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT_ID,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       SAFT.MIN_AMOUNT,");
            sb.Append("       AST.BREAKPOINT_ID,");
            sb.Append("       SAB.CURRENT_RATE,");
            sb.Append("       SAB.REQUEST_RATE,");
            sb.Append("       SAB.APPROVED_RATE,");
            sb.Append("       AST.SEQUENCE_NO");
            sb.Append("  FROM SRR_AIR_TBL             SAT,");
            sb.Append("       SRR_TRN_AIR_TBL         STAT,");
            sb.Append("       SRR_AIR_FREIGHT_TBL     SAFT,");
            sb.Append("       SRR_AIR_BREAKPOINTS     SAB,");
            sb.Append("       AIRFREIGHT_SLABS_TBL    AST,");
            sb.Append("       PORT_MST_TBL            POL,");
            sb.Append("       PORT_MST_TBL            POD,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT");
            sb.Append(" WHERE SAT.SRR_AIR_PK = STAT.SRR_AIR_FK");
            sb.Append("   AND STAT.SRR_TRN_AIR_PK = SAFT.SRR_TRN_AIR_FK");
            sb.Append("   AND SAB.SRR_AIR_FRT_FK = SAFT.SRR_AIR_FRT_PK");
            sb.Append("   AND AST.AIRFREIGHT_SLABS_TBL_PK = SAB.AIRFREIGHT_SLABS_FK");
            sb.Append("   AND POL.PORT_MST_PK = STAT.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = STAT.PORT_MST_POD_FK");
            sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK = SAFT.FREIGHT_ELEMENT_MST_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = SAFT.CURRENCY_MST_FK");
            sb.Append("   AND SAT.SRR_AIR_PK = " + SRRAirPk);
            sb.Append("   order by AST.SEQUENCE_NO");
            try
            {
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
        /// <param name="SRRAirPk">The SRR air pk.</param>
        /// <returns></returns>
        public DataSet FetchFreightDetailsPortGrp(long SRRAirPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT POL.PORT_GRP_ID   POL,");
            sb.Append("       POD.PORT_GRP_ID   POD,");
            sb.Append("       STAT.VALID_FROM,");
            sb.Append("       STAT.VALID_TO,");
            sb.Append("       STAT.EXPECTED_WEIGHT,");
            sb.Append("       STAT.EXPECTED_VOLUME,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT_ID,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       SAFT.MIN_AMOUNT,");
            sb.Append("       AST.BREAKPOINT_ID,");
            sb.Append("       SAB.CURRENT_RATE,");
            sb.Append("       SAB.REQUEST_RATE,");
            sb.Append("       SAB.APPROVED_RATE,");
            sb.Append("       AST.SEQUENCE_NO");
            sb.Append("  FROM SRR_AIR_TBL             SAT,");
            sb.Append("       SRR_TRN_AIR_TBL         STAT,");
            sb.Append("       SRR_AIR_FREIGHT_TBL     SAFT,");
            sb.Append("       SRR_AIR_BREAKPOINTS     SAB,");
            sb.Append("       AIRFREIGHT_SLABS_TBL    AST,");
            sb.Append("       PORT_GRP_MST_TBL            POL,");
            sb.Append("       PORT_GRP_MST_TBL            POD,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT");
            sb.Append(" WHERE SAT.SRR_AIR_PK = STAT.SRR_AIR_FK");
            sb.Append("   AND STAT.SRR_TRN_AIR_PK = SAFT.SRR_TRN_AIR_FK");
            sb.Append("   AND SAB.SRR_AIR_FRT_FK = SAFT.SRR_AIR_FRT_PK");
            sb.Append("   AND AST.AIRFREIGHT_SLABS_TBL_PK = SAB.AIRFREIGHT_SLABS_FK");
            sb.Append("   AND POL.PORT_GRP_MST_PK = STAT.POL_GRP_FK");
            sb.Append("   AND POD.PORT_GRP_MST_PK = STAT.POD_GRP_FK");
            sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK = SAFT.FREIGHT_ELEMENT_MST_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = SAFT.CURRENCY_MST_FK");
            sb.Append("   AND SAT.SRR_AIR_PK = " + SRRAirPk);
            sb.Append("   order by AST.SEQUENCE_NO");
            try
            {
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
        /// <param name="SRRAirPk">The SRR air pk.</param>
        /// <returns></returns>
        public DataSet FetchFreightDetailsTariffGrp(long SRRAirPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT TRF.TARIFF_GRP_NAME   POL,");
            sb.Append("       '' POD,");
            sb.Append("       STAT.VALID_FROM,");
            sb.Append("       STAT.VALID_TO,");
            sb.Append("       STAT.EXPECTED_WEIGHT,");
            sb.Append("       STAT.EXPECTED_VOLUME,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT_ID,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       SAFT.MIN_AMOUNT,");
            sb.Append("       AST.BREAKPOINT_ID,");
            sb.Append("       SAB.CURRENT_RATE,");
            sb.Append("       SAB.REQUEST_RATE,");
            sb.Append("       SAB.APPROVED_RATE,");
            sb.Append("       AST.SEQUENCE_NO");
            sb.Append("  FROM SRR_AIR_TBL             SAT,");
            sb.Append("       SRR_TRN_AIR_TBL         STAT,");
            sb.Append("       SRR_AIR_FREIGHT_TBL     SAFT,");
            sb.Append("       SRR_AIR_BREAKPOINTS     SAB,");
            sb.Append("       AIRFREIGHT_SLABS_TBL    AST,");
            sb.Append("       TARIFF_GRP_MST_TBL      TRF,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT");
            sb.Append(" WHERE SAT.SRR_AIR_PK = STAT.SRR_AIR_FK");
            sb.Append("   AND STAT.SRR_TRN_AIR_PK = SAFT.SRR_TRN_AIR_FK");
            sb.Append("   AND SAB.SRR_AIR_FRT_FK = SAFT.SRR_AIR_FRT_PK");
            sb.Append("   AND AST.AIRFREIGHT_SLABS_TBL_PK = SAB.AIRFREIGHT_SLABS_FK");
            sb.Append("   AND TRF.TARIFF_GRP_MST_PK = STAT.TARIFF_GRP_FK");
            sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK = SAFT.FREIGHT_ELEMENT_MST_FK");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = SAFT.CURRENCY_MST_FK");
            sb.Append("   AND SAT.SRR_AIR_PK = " + SRRAirPk);
            sb.Append("   order by AST.SEQUENCE_NO");
            try
            {
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
        /// <param name="SRRContractOtherPk">The SRR contract other pk.</param>
        /// <returns></returns>
        public DataSet FetchOtherChargeDetails(long SRRContractOtherPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT_ID,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       DECODE(SAOC.CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,");
            sb.Append("       SAOC.CURRENT_RATE,");
            sb.Append("       SAOC.REQUESTED_RATE");
            sb.Append("  FROM SRR_AIR_TBL   SAT,");
            sb.Append("    SRR_TRN_AIR_TBL    STAT,");
            sb.Append("    SRR_AIR_OTH_CHRG   SAOC,");
            sb.Append("    FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("    CURRENCY_TYPE_MST_TBL  CTMT");
            sb.Append("  WHERE SAT.SRR_AIR_PK = STAT.SRR_AIR_FK");
            sb.Append("     AND SAOC.FREIGHT_ELEMENT_MST_FK=FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("     AND SAOC.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
            sb.Append("     AND STAT.SRR_TRN_AIR_PK = SAOC.SRR_TRN_AIR_FK  ");
            sb.Append("     AND SAT.SRR_AIR_PK =" + SRRContractOtherPk);
            try
            {
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
                    sb.Append("  FROM SRR_TRN_AIR_TBL T, PORT_MST_TBL POL");
                    sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND T.SRR_AIR_FK = " + QuotPK);
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
                    sb.Append("  FROM SRR_TRN_AIR_TBL T, PORT_MST_TBL POD");
                    sb.Append(" WHERE T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND T.SRR_AIR_FK = " + QuotPK);
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
                strSQL = " SELECT NVL(C.PORT_GROUP,0) PORT_GROUP FROM SRR_AIR_TBL C WHERE C.SRR_AIR_PK = " + ContPK;
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
        /// Fetches the commodity.
        /// </summary>
        /// <param name="CommPK">The comm pk.</param>
        /// <returns></returns>
        public string FetchCommodity(int CommPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT C.COMMODITY_NAME FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_MST_PK= " + CommPK;
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

                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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
                sb.Append("SELECT CTT.SRR_TRN_AIR_PK");
                sb.Append("  FROM SRR_AIR_TBL CMT, SRR_TRN_AIR_TBL CTT");
                sb.Append(" WHERE CMT.SRR_AIR_PK = CTT.SRR_AIR_FK");
                sb.Append("   AND CMT.SRR_AIR_PK = " + CCPK);
                sb.Append("   AND CTT.PORT_MST_POL_FK = " + CCPOLFK);
                sb.Append("   AND CTT.PORT_MST_POD_FK = " + CCPODFK);

                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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

        /// <summary>
        /// Saves the local charges.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="dsLocalChrgs">The ds local CHRGS.</param>
        /// <param name="Quotation_Mst_fk">The quotation_ MST_FK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="AmendFlg">if set to <c>true</c> [amend FLG].</param>
        /// <param name="FromFlg">From FLG.</param>
        /// <param name="Qout_Type">Type of the qout_.</param>
        /// <param name="QuotOrBkgFlag">The quot or BKG flag.</param>
        /// <returns></returns>
        public ArrayList SaveLocalCharges(OracleCommand SCM, string UserName, DataSet dsLocalChrgs, long Quotation_Mst_fk = 0, int BizType = 2, bool AmendFlg = false, int FromFlg = -1, int Qout_Type = 0, int QuotOrBkgFlag = 4)
        {
            int Rcnt = 0;
            DataRow Odr = null;
            int delFlage = 0;
            string QoutLocDtlPKs = "0";

            try
            {
                if (dsLocalChrgs.Tables[0].Rows.Count > 0)
                {
                    for (Rcnt = 0; Rcnt <= dsLocalChrgs.Tables[0].Rows.Count - 1; Rcnt++)
                    {
                        if (AmendFlg == true)
                        {
                            dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"] = "";
                        }
                        if (!string.IsNullOrEmpty(dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"].ToString()) & (dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"] != null))
                        {
                            if (dsLocalChrgs.Tables[0].Rows[Rcnt]["SEL"] == "TRUE" | Convert.ToBoolean(dsLocalChrgs.Tables[0].Rows[Rcnt]["SEL"]) == true)
                            {
                                QoutLocDtlPKs += "," + dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"];
                                SCM.CommandType = CommandType.StoredProcedure;
                                SCM.CommandText = UserName + ".QUOTATION_LOCAL_CHRG_PKG.QUOTATION_LOCAL_TRN_UPD";
                                var _with13 = SCM.Parameters;
                                _with13.Clear();
                                _with13.Add("QUOTATION_LOCAL_PK_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"]);
                                _with13.Add("TARIFF_TRN_FK_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["TARIFF_PK"]);
                                _with13.Add("MINIMUM_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_MINAMT"]);
                                _with13.Add("SHIPMENT_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_SHIPMENT_AMT"]);
                                _with13.Add("CONT_20_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_CONT_20_AMT"]);
                                _with13.Add("CONT_40_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_CONT_40_AMT"]).Direction = ParameterDirection.Input;
                                _with13.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                                _with13.Add("PROCESS_TYPE_IN", (FromFlg == 0 ? 1 : 2)).Direction = ParameterDirection.Input;
                                _with13.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                                _with13.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                                _with13.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                SCM.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            if (dsLocalChrgs.Tables[0].Rows[Rcnt]["SEL"] == "TRUE" | Convert.ToBoolean(dsLocalChrgs.Tables[0].Rows[Rcnt]["SEL"]) == true)
                            {
                                SCM.CommandType = CommandType.StoredProcedure;
                                SCM.CommandText = UserName + ".QUOTATION_LOCAL_CHRG_PKG.QUOTATION_LOCAL_TRN_INS";
                                var _with14 = SCM.Parameters;
                                _with14.Clear();
                                _with14.Add("QUOTATION_MST_FK_IN", Quotation_Mst_fk);
                                _with14.Add("TARIFF_TRN_FK_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["TARIFF_PK"]);
                                _with14.Add("MINIMUM_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_MINAMT"]);
                                _with14.Add("SHIPMENT_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_SHIPMENT_AMT"]);
                                _with14.Add("CONT_20_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_CONT_20_AMT"]);
                                _with14.Add("CONT_40_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_CONT_40_AMT"]).Direction = ParameterDirection.Input;
                                _with14.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                                _with14.Add("PROCESS_TYPE_IN", (FromFlg == 0 ? 1 : 2)).Direction = ParameterDirection.Input;
                                _with14.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                                _with14.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                                _with14.Add("FROM_FLAG_IN", QuotOrBkgFlag).Direction = ParameterDirection.Input;
                                _with14.Add("QUOT_TYPE_IN", Qout_Type).Direction = ParameterDirection.Input;
                                _with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                SCM.ExecuteNonQuery();
                                QoutLocDtlPKs += "," + SCM.Parameters["RETURN_VALUE"].Value;
                            }
                        }
                    }
                    if (QoutLocDtlPKs.Length > 0)
                    {
                        SCM.CommandType = CommandType.StoredProcedure;
                        SCM.CommandText = UserName + ".QUOTATION_LOCAL_CHRG_PKG.QUOTATION_LOCAL_TRN_DEL";
                        var _with15 = SCM.Parameters;
                        _with15.Clear();
                        _with15.Add("QUOTATION_LOCAL_PKS_IN", QoutLocDtlPKs);
                        _with15.Add("QUOTATION_MST_FK_IN", Quotation_Mst_fk);
                        _with15.Add("QUOT_TYPE_IN", Qout_Type).Direction = ParameterDirection.Input;
                        _with15.Add("FROM_FLAG_IN", QuotOrBkgFlag).Direction = ParameterDirection.Input;
                        _with15.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        SCM.ExecuteNonQuery();
                    }
                    arrMessage.Add("saved");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            return new ArrayList();
        }
    }
}