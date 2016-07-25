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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_OperatorRFQSpotRate : CommonFeatures
    {

        private long _PkValue;
        #region " Update "
        public ArrayList Update(DataSet MasterDS, DataSet GridDS, DataSet CalcDS, string Measure, string Wt, string DivFac, object RFQNo = null, long nLocationId = 0, long nEmpId = 0, bool ForFCL = true,Int16 Active = 1, Int16 Restricted = 0)
        {

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int16 RowAffacted = 0;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;

            arrMessage.Clear();
            objWK.MyCommand.Parameters.Clear();
            try
            {
                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".RFQ_SPOT_RATE_SEA_TBL_PKG.RFQ_SPOT_RATE_SEA_TBL_UPD";
                var _with2 = _with1.Parameters;
                _with2.Add("RFQ_SPOT_SEA_PK_IN", MasterDS.Tables[0].Rows[0]["RFQ_SPOT_SEA_PK"]).Direction = ParameterDirection.Input;
                _with2.Add("APPROVED_IN", MasterDS.Tables[0].Rows[0]["APPROVED"]).Direction = ParameterDirection.Input;
                _with2.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(MasterDS.Tables[0].Rows[0]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                if ((object.ReferenceEquals(MasterDS.Tables[0].Rows[0]["VALID_TO"], DBNull.Value)))
                {
                    _with2.Add("VALID_TO_IN", MasterDS.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with2.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(MasterDS.Tables[0].Rows[0]["VALID_TO"]))).Direction = ParameterDirection.Input;
                }
                if (!(object.ReferenceEquals(MasterDS.Tables[0].Rows[0]["COMMODITY_MST_FK"], DBNull.Value) | MasterDS.Tables[0].Rows[0]["COMMODITY_MST_FK"] == null))
                {
                    _with2.Add("COMMODITY_MAST_FK_IN", MasterDS.Tables[0].Rows[0]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with2.Add("COMMODITY_MAST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                // New Parameter

                _with2.Add("TEUS_VOL_IN", MasterDS.Tables[0].Rows[0]["TEUS_VOL"]).Direction = ParameterDirection.Input;
                //MODIFIED FOR NEW PRAMETER
                _with2.Add("GROSS_WEIGHT_IN", MasterDS.Tables[0].Rows[0]["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;

                _with2.Add("VOLUME_IN_CBM_IN", MasterDS.Tables[0].Rows[0]["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;

                _with2.Add("PACK_COUNT_IN", MasterDS.Tables[0].Rows[0]["PACK_COUNT"]).Direction = ParameterDirection.Input;

                _with2.Add("CHARGEABLE_WEIGHT_IN", MasterDS.Tables[0].Rows[0]["CHARGEABLE_WEIGHT"]).Direction = ParameterDirection.Input;

                _with2.Add("VOLUME_WEIGHT_IN", MasterDS.Tables[0].Rows[0]["VOLUME_WEIGHT"]).Direction = ParameterDirection.Input;

                _with2.Add("DENSITY_IN", MasterDS.Tables[0].Rows[0]["DENSITY"]).Direction = ParameterDirection.Input;

                _with2.Add("COL_ADDRESS_IN", getDefault(MasterDS.Tables[0].Rows[0]["COL_ADDRESS"], DBNull.Value)).Direction = ParameterDirection.Input;

                //'

                if (Convert.ToInt32(MasterDS.Tables[0].Rows[0]["APPROVED"]) == 1)
                {
                    _with2.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with2.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(0)).Direction = ParameterDirection.Input;
                }

                _with2.Add("ACTIVE_IN", Active).Direction = ParameterDirection.Input;

                _with2.Add("VERSION_NO_IN", MasterDS.Tables[0].Rows[0]["VERSION_NO"]).Direction = ParameterDirection.Input;

                _with2.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with2.Add("RESTRICTED_IN", Restricted).Direction = ParameterDirection.Input;

                _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RowAffacted = Convert.ToInt16(_with1.ExecuteNonQuery());
                _PkValue = Convert.ToInt64(MasterDS.Tables[0].Rows[0]["RFQ_SPOT_SEA_PK"]);
                if (RowAffacted == 0)
                {
                    arrMessage.Add("ORA-20999: Record(s) Already Modified.");
                }
                else
                {
                    arrMessage = UpdateTransaction(MasterDS, GridDS, _PkValue, objWK.MyCommand, objWK.MyUserName, ForFCL);
                    arrMessage = SaveCargoCalculator(CalcDS, _PkValue, objWK.MyCommand, objWK.MyUserName, Measure, Wt, DivFac);
                }
                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved")>0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }
        #endregion

        #region " Update "
        private ArrayList UpdateTransaction(DataSet MasterDS, DataSet Gridds, long PkValue, OracleCommand UCM, string UserName, bool ForFCL)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            Int16 AllInRate = default(Int16);
            arrMessage.Clear();
            try
            {
                var _with3 = UCM;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = UserName + ".RFQ_SPOT_RATE_SEA_TBL_PKG.RFQ_SPOT_TRN_SEA_FCL_LCL_UPD";

                for (nRowCnt = 0; nRowCnt <= Gridds.Tables[1].Rows.Count - 1; nRowCnt++)
                {
                    var _with4 = _with3.Parameters;
                    DR = Gridds.Tables[1].Rows[nRowCnt];
                    _with4.Clear();
                    _with4.Add("RFQ_SPOT_SEA_FK_IN", Gridds.Tables[0].Rows[0]["RFQ_SPOT_SEA_FK"]).Direction = ParameterDirection.Input;

                    _with4.Add("SURCHARGE_IN", Gridds.Tables[1].Rows[nRowCnt]["SURCHARGE"]).Direction = ParameterDirection.Input;


                    if (!ForFCL)
                    {
                        _with4.Add("FREIGHT_ELEMENT_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;

                        AllInRate = Convert.ToInt16((Gridds.Tables[1].Rows[nRowCnt]["SELECTED"] == "true" ? 1 : 0));

                        _with4.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;

                        _with4.Add("CURRENCY_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        if (string.IsNullOrEmpty(Gridds.Tables[0].Rows[0]["LCL_BASIS"].ToString()))
                        {
                            _with4.Add("LCL_BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Add("LCL_BASIS_IN", Gridds.Tables[0].Rows[0]["LCL_BASIS"]).Direction = ParameterDirection.Input;
                        }
                        _with4.Add("LCL_CURRENT_RATE_IN", Gridds.Tables[1].Rows[nRowCnt]["LCL_CURRENT_RATE"]).Direction = ParameterDirection.Input;
                        _with4.Add("LCL_REQUEST_RATE_IN", Gridds.Tables[1].Rows[nRowCnt]["LCL_REQUEST_RATE"]).Direction = ParameterDirection.Input;
                        _with4.Add("LCL_APPROVED_RATE_IN", Gridds.Tables[1].Rows[nRowCnt]["LCL_APPROVED_RATE"]).Direction = ParameterDirection.Input;

                        _with4.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                        if (object.ReferenceEquals(Gridds.Tables[0].Rows[0]["VALID_TO"], DBNull.Value))
                        {
                            _with4.Add("VALID_TO_IN", Gridds.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                        }
                        else if (Gridds.Tables[0].Rows[0]["VALID_TO"] == "  /  /    ")
                        {
                            _with4.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_TO"]))).Direction = ParameterDirection.Input;
                        }

                        _with4.Add("CONTAINER_DTL_FCL_IN", DBNull.Value).Direction = ParameterDirection.Input;

                        //Added by rabbani reason USS Gap
                        if (string.IsNullOrEmpty(Gridds.Tables[1].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"].ToString()))
                        {
                            _with4.Add("LCL_CURRENT_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Add("LCL_CURRENT_MIN_RATE_IN", Convert.ToDouble(Gridds.Tables[1].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"])).Direction = ParameterDirection.Input;
                        }

                        //Added by rabbani reason USS Gap
                        if (string.IsNullOrEmpty(Gridds.Tables[1].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"].ToString()))
                        {
                            _with4.Add("LCL_REQUEST_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Add("LCL_REQUEST_MIN_RATE_IN", Convert.ToDouble(Gridds.Tables[1].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"])).Direction = ParameterDirection.Input;
                        }

                        //Added by rabbani reason USS Gap
                        if (string.IsNullOrEmpty(Gridds.Tables[1].Rows[nRowCnt]["LCL_APPROVED_MIN_RATE"].ToString()))
                        {
                            _with4.Add("LCL_APPROVED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Add("LCL_APPROVED_MIN_RATE_IN", Convert.ToDouble(Gridds.Tables[1].Rows[nRowCnt]["LCL_APPROVED_MIN_RATE"])).Direction = ParameterDirection.Input;
                        }

                    }
                    else
                    {
                        _with4.Add("FREIGHT_ELEMENT_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["FrElmtFk"]).Direction = ParameterDirection.Input;

                        AllInRate = Convert.ToInt16(Gridds.Tables[1].Rows[nRowCnt]["Selected"] == "true" ? 1 : 0);

                        _with4.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;

                        _with4.Add("CURRENCY_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["CurrencyFk"]).Direction = ParameterDirection.Input;
                        if (string.IsNullOrEmpty(Gridds.Tables[0].Rows[0]["LCL_BASIS"].ToString()))
                        {
                            _with4.Add("LCL_BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Add("LCL_BASIS_IN", Gridds.Tables[0].Rows[0]["LCL_BASIS"]).Direction = ParameterDirection.Input;
                        }
                        _with4.Add("LCL_CURRENT_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with4.Add("LCL_REQUEST_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with4.Add("LCL_APPROVED_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                        _with4.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                        //If Gridds.Tables(0).Rows(0)["VALID_TO") = "  /  /    " Then
                        //    Gridds.Tables(0).Rows(0)["VALID_TO") = DBNull.Value
                        //End If

                        if (object.ReferenceEquals(Gridds.Tables[0].Rows[0]["VALID_TO"], DBNull.Value))
                        {
                            _with4.Add("VALID_TO_IN", Gridds.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                        }
                        else if (Gridds.Tables[0].Rows[0]["VALID_TO"] == "  /  /    ")
                        {
                            _with4.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_TO"]))).Direction = ParameterDirection.Input;
                        }

                        _with4.Add("CONTAINER_DTL_FCL_IN", GetContainerDtlFcl(Gridds.Tables[1].Rows[nRowCnt])).Direction = ParameterDirection.Input;

                        _with4.Add("LCL_CURRENT_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                        _with4.Add("LCL_REQUEST_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                        _with4.Add("LCL_APPROVED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with3.ExecuteNonQuery();
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
        #endregion

        #region "Fetch surcharge"
        public DataSet Fetch_Surcharge_assign(DataSet dsGrid, int Int_pk = 0, string MAIN_TABLE = "", string TRN_TABLE = "", string MAIN_TABLE_PK = "", string PK_OUT = "", string TRN_TABLE_PK = "", bool FclFlag = false, Int16 LclFlag = 0)
        {
            int Rcnt = 0;
            int Rcnt1 = 0;
            int RowCnt = 0;
            DataSet Dssurcharge = null;
            try
            {
                if (MAIN_TABLE == "RFQ_SPOT_RATE_SEA_TBL")
                {
                    for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE")))
                        {
                            dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                        }
                        Dssurcharge = Fetch_surcharge_fortwotable(Int_pk, MAIN_TABLE, TRN_TABLE, MAIN_TABLE_PK, PK_OUT, TRN_TABLE_PK, Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"]), Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"]));
                        if (Dssurcharge.Tables[0].Rows.Count == 0)
                        {
                            Dssurcharge = Fetch_surcharge_fortwotable(0, MAIN_TABLE, TRN_TABLE, MAIN_TABLE_PK, PK_OUT, TRN_TABLE_PK,
                                Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"]),
                                Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"]));
                        }

                        if (FclFlag == true)
                        {
                            for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++)
                            {
                                for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                                {
                                    if ((dsGrid.Tables[1].Rows[Rcnt]["FrElmtFk"] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"] & dsGrid.Tables[1].Rows[Rcnt]["POL"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["POD"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"]))
                                    {
                                        dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                                    }
                                }
                            }
                        }
                        else
                        {
                            for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++)
                            {
                                for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                                {
                                    if ((dsGrid.Tables[1].Rows[Rcnt]["FREIGHT_ELEMENT_MST_FK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]))
                                    {
                                        dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                                    }
                                }
                            }
                        }
                    }
                }
                else
                {
                    dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                    Dssurcharge = Fetch_surcharge_fortwotable(Int_pk, MAIN_TABLE, TRN_TABLE, MAIN_TABLE_PK, PK_OUT, TRN_TABLE_PK);

                    if (Dssurcharge.Tables[0].Rows.Count == 0)
                    {
                        Dssurcharge = Fetch_surcharge_fortwotable(0, MAIN_TABLE, TRN_TABLE, MAIN_TABLE_PK, PK_OUT, TRN_TABLE_PK);
                    }

                    if (Convert.ToInt32(FclFlag) >= 1)
                    {
                        for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++)
                        {
                            for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                            {
                                if ((dsGrid.Tables[1].Rows[Rcnt]["FrElmtFk"] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]))
                                {
                                    dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                                }
                            }
                        }

                    }

                    if (LclFlag >= 1)
                    {
                        for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++)
                        {
                            for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                            {
                                if ((dsGrid.Tables[1].Rows[Rcnt]["FREIGHT_ELEMENT_MST_FK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]))
                                {
                                    dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                                }
                            }
                        }

                    }
                }

                return (dsGrid);
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
        #endregion

        #region "Fetch surcharge"
        public DataSet Fetch_surcharge_fortwotable(int Valuepk = 0, string MAIN_TABLE = "", string TRN_TABLE = "", string MAIN_TABLE_PK = "", string PK_OUT = "", string TRN_TABLE_PK = "", int POL_PK = 0, int POD_PK = 0)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            //'for 2-table

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".FETCH_SURCHARGE.FETCH_SURCHARGE_DATA";
                var _with5 = selectCommand.Parameters;
                _with5.Clear();
                _with5.Add("PK_IN", Valuepk).Direction = ParameterDirection.Input;
                _with5.Add("PK_OUT_IN", PK_OUT).Direction = ParameterDirection.Input;
                _with5.Add("MAIN_TABLE_IN", MAIN_TABLE).Direction = ParameterDirection.Input;
                _with5.Add("MAIN_TABLE_PK_IN", MAIN_TABLE_PK).Direction = ParameterDirection.Input;
                _with5.Add("TRN_TABLE_IN", TRN_TABLE).Direction = ParameterDirection.Input;
                _with5.Add("TRN_TABLE_PK_IN", TRN_TABLE_PK).Direction = ParameterDirection.Input;
                _with5.Add("POL_PK_IN", POL_PK).Direction = ParameterDirection.Input;
                _with5.Add("POD_PK_IN", POD_PK).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.Varchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value).Trim();

                return (objWF.GetDataSet(strReturn));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        #region "SaveSurcharge"
        public ArrayList SaveSurcharge(DataSet dsMain, long PkValue, int TrnPk_colnumber, OracleCommand SelectCommand, int Freight_Elepk_colno, string TRN_SEA_PK_COL = "", string TRN_SEA_FREIGHT_COL = "", string MAIN_SEA_COL = "", string TRN_TABLE_IN = "")
        {
            //'saving the Surcharge

            WorkFlow objWF = new WorkFlow();
            arrMessage.Clear();
            Int32 nRowCnt = default(Int32);
            Int32 IntReturn = default(Int32);
            Int32 nRowCnt1 = default(Int32);

            try
            {
                var _with6 = SelectCommand;
                if (dsMain.Tables["tblTransaction"].Rows.Count > 0)
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        _with6.CommandType = CommandType.StoredProcedure;
                        _with6.CommandText = objWF.MyUserName + ".RFQ_MAIN_SEA_TBL_PKG.SURCHARGE_UPD";
                        var _with7 = _with6.Parameters;
                        _with7.Clear();

                        _with7.Add("TRN_TABLE_IN", TRN_TABLE_IN).Direction = ParameterDirection.Input;
                        _with7.Add("TRN_SEA_PK_IN", TrnPk_colnumber).Direction = ParameterDirection.Input;
                        _with7.Add("TRN_SEA_PK_COL_IN", TRN_SEA_PK_COL).Direction = ParameterDirection.Input;

                        _with7.Add("TRN_SEA_FREIGHT_COL_IN", TRN_SEA_FREIGHT_COL).Direction = ParameterDirection.Input;
                        _with7.Add("TRN_SEA_FREIGHT_VAL_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt][Freight_Elepk_colno]).Direction = ParameterDirection.Input;
                        _with7.Add("MAIN_SEA_COL_IN", MAIN_SEA_COL).Direction = ParameterDirection.Input;
                        _with7.Add("MAIN_SEA_PK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with7.Add("SURCHARGE_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["SURCHARGE"]).Direction = ParameterDirection.Input;
                        _with7.Add("RETURN_VALUE", OracleDbType.Varchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with6.ExecuteNonQuery();
                        IntReturn = Convert.ToInt32(_with6.Parameters["RETURN_VALUE"].Value);
                        //Exit For
                    }
                }

                arrMessage.Add("All Data Saved Successfully");
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
        #endregion

        #region "FetchRFQSpotRateInsertStatus "
        public string FetchRFQSpotRateInsertStatus(string strCond)
        {
            Array arr = null;
            arr = strCond.Split('~');
            long Operator1 = Convert.ToInt64(arr.GetValue(1));
            long Customer = Convert.ToInt64(arr.GetValue(2));
            string Fdate = Convert.ToString(arr.GetValue(3));
            string TDate = (Convert.ToString(arr.GetValue(4)).Length == 0 ? null : arr.GetValue(4).ToString());
            string CargoType = Convert.ToString(arr.GetValue(5));
            object polfk = Convert.ToString(arr.GetValue(6));
            object podfk = Convert.ToString(arr.GetValue(7));
            object Commodityfk = Convert.ToString(arr.GetValue(8));
            object CommGrpPk = Convert.ToString(arr.GetValue(9));
            string CtrIds = Convert.ToString(arr.GetValue(10));
            try
            {
                return GetInsertStatus(Operator1, Customer, Fdate, TDate, CargoType, polfk, podfk, Commodityfk, CommGrpPk, CtrIds);
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
        #endregion

        #region " Get Insert Status "
        public string GetInsertStatus(long Operator1, long Customer, string Fdate, string TDate, string CargoType, object polfk, object podfk, object Commfk, object CommGrp, string CtrIds)
        {

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Parameters.Clear();
            try
            {
                var _with8 = objWK.MyCommand;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".RFQ_SPOT_RATE_SEA_TBL_PKG.RFQ_SPOT_RATE_SEA_TBL_STATUS";
                var _with9 = _with8.Parameters;
                _with9.Add("OPERATOR_MST_FK_IN", Operator1).Direction = ParameterDirection.Input;
                _with9.Add("CUSTOMER_MST_FK_IN", Customer).Direction = ParameterDirection.Input;
                _with9.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(Fdate))).Direction = ParameterDirection.Input;
                _with9.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(TDate))).Direction = ParameterDirection.Input;
                _with9.Add("CARGO_TYPE_IN", Convert.ToInt32(CargoType)).Direction = ParameterDirection.Input;
                _with9.Add("POL_IN", polfk).Direction = ParameterDirection.Input;
                _with9.Add("POD_IN", podfk).Direction = ParameterDirection.Input;
                _with9.Add("COMMODITY_IN", getDefault(Commfk, DBNull.Value)).Direction = ParameterDirection.Input;
                _with9.Add("COMMODITY_GRP_IN", getDefault(CommGrp, DBNull.Value)).Direction = ParameterDirection.Input;
                //Manoharan
                _with9.Add("container_type_in", getDefault(CtrIds, DBNull.Value)).Direction = ParameterDirection.Input;

                _with9.Add("RFQ_RETURN", OracleDbType.Varchar2, 20, "RFQ_RETURN").Direction = ParameterDirection.Output;

                _with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with8.ExecuteNonQuery();
                string strReturn = null;
                strReturn = Convert.ToString(_with8.Parameters["RETURN_VALUE"].Value);
                strReturn += "~" + Convert.ToString(_with8.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
        #endregion

        #region " Save "
        public ArrayList Save(DataSet MasterDS, DataSet GridDS, DataSet CalcDS, long lngPkValue, Int16 Active, string Measure, string Wt, string DivFac, bool NewRecord = false, string RFQNo = null,
        long nLocationId = 0, long nEmpId = 0, bool ForFCL = true, Int16 InsertStatus = 0, string container_type_id = "", int Restricted = 0, string sid = "", string polid = "", string podid = "")
        {
            if (!NewRecord)
            {
                return Update(MasterDS, GridDS, CalcDS, Measure, Wt, DivFac, RFQNo, nLocationId, nEmpId, ForFCL,
                Active, Convert.ToInt16(Restricted));
            }
            cls_AirlineRFQEntry objrfqAir = new cls_AirlineRFQEntry();
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                if (string.IsNullOrEmpty(RFQNo))
                {
                    RFQNo = GenerateRFQNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, sid, polid, podid);
                    if (RFQNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                    }
                    MasterDS.Tables[0].Rows[0]["RFQ_REF_NO"] = RFQNo;
                }
                objWK.MyCommand.Parameters.Clear();
                var _with10 = objWK.MyCommand;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".RFQ_SPOT_RATE_SEA_TBL_PKG.RFQ_SPOT_RATE_SEA_TBL_INS";
                var _with11 = _with10.Parameters;
                _with11.Add("OPERATOR_MST_FK_IN", MasterDS.Tables[0].Rows[0]["OPERATOR_MST_FK"]).Direction = ParameterDirection.Input;
                _with11.Add("CONT_MAIN_SEA_FK_IN", MasterDS.Tables[0].Rows[0]["CONT_MAIN_SEA_FK"]).Direction = ParameterDirection.Input;
                _with11.Add("CUSTOMER_MST_FK_IN", MasterDS.Tables[0].Rows[0]["CUSTOMER_MST_FK"]).Direction = ParameterDirection.Input;
                _with11.Add("RFQ_REF_NO_IN", MasterDS.Tables[0].Rows[0]["RFQ_REF_NO"]).Direction = ParameterDirection.Input;
                _with11.Add("RFQ_DATE_IN", Convert.ToString(Convert.ToDateTime(MasterDS.Tables[0].Rows[0]["RFQ_DATE"]))).Direction = ParameterDirection.Input;
                _with11.Add("APPROVED_IN", MasterDS.Tables[0].Rows[0]["APPROVED"]).Direction = ParameterDirection.Input;
                _with11.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(MasterDS.Tables[0].Rows[0]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                if ((object.ReferenceEquals(MasterDS.Tables[0].Rows[0]["VALID_TO"], DBNull.Value)))
                {
                    _with11.Add("VALID_TO_IN", MasterDS.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with11.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(MasterDS.Tables[0].Rows[0]["VALID_TO"]))).Direction = ParameterDirection.Input;
                }

                _with11.Add("CARGO_TYPE_IN", Convert.ToInt32(MasterDS.Tables[0].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
                _with11.Add("ACTIVE_IN", Convert.ToInt32(Active)).Direction = ParameterDirection.Input;
                _with11.Add("COMMODITY_GROUP_FK_IN", MasterDS.Tables[0].Rows[0]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;

                _with11.Add("COMMODITY_MST_FK_IN", getDefault(MasterDS.Tables[0].Rows[0]["COMMODITY_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;

                _with11.Add("POL_IN", GridDS.Tables[0].Rows[0]["PORT_MST_POL_FK"]).Direction = ParameterDirection.Input;
                _with11.Add("POD_IN", GridDS.Tables[0].Rows[0]["PORT_MST_POD_FK"]).Direction = ParameterDirection.Input;
                _with11.Add("STATUS_IN", InsertStatus).Direction = ParameterDirection.Input;

                _with11.Add("TEUS_VOL_IN", getDefault(MasterDS.Tables[0].Rows[0]["TEUS_VOL"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with11.Add("GROSS_WEIGHT_IN", getDefault(MasterDS.Tables[0].Rows[0]["GROSS_WEIGHT"], DBNull.Value)).Direction = ParameterDirection.Input;

                _with11.Add("VOLUME_IN_CBM_IN", getDefault(MasterDS.Tables[0].Rows[0]["VOLUME_IN_CBM"], DBNull.Value)).Direction = ParameterDirection.Input;

                _with11.Add("PACK_COUNT_IN", getDefault(MasterDS.Tables[0].Rows[0]["PACK_COUNT"], DBNull.Value)).Direction = ParameterDirection.Input;

                _with11.Add("CHARGEABLE_WEIGHT_IN", getDefault(MasterDS.Tables[0].Rows[0]["CHARGEABLE_WEIGHT"], DBNull.Value)).Direction = ParameterDirection.Input;

                _with11.Add("VOLUME_WEIGHT_IN", getDefault(MasterDS.Tables[0].Rows[0]["VOLUME_WEIGHT"], DBNull.Value)).Direction = ParameterDirection.Input;

                _with11.Add("DENSITY_IN", getDefault(MasterDS.Tables[0].Rows[0]["DENSITY"], DBNull.Value)).Direction = ParameterDirection.Input;


                _with11.Add("COL_ADDRESS_IN", getDefault(MasterDS.Tables[0].Rows[0]["COL_ADDRESS"], DBNull.Value)).Direction = ParameterDirection.Input;

                //'
                _with11.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with11.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                if ((Convert.ToInt32(MasterDS.Tables[0].Rows[0]["CARGO_TYPE"]) == 1))
                {
                    _with11.Add("CONTAINER_TYPE_IN", container_type_id).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with11.Add("CONTAINER_TYPE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                _with11.Add("base_currency_fk_in", getDefault(MasterDS.Tables[0].Rows[0]["base_currency_fk"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with11.Add("RESTRICTED_IN", Restricted).Direction = ParameterDirection.Input;
                _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with10.ExecuteNonQuery();
                _PkValue = Convert.ToInt64(_with10.Parameters["RETURN_VALUE"].Value);
                MasterDS.Tables[0].Rows[0]["RFQ_SPOT_SEA_PK"] = _PkValue;
                GridDS.Tables[0].Rows[0]["RFQ_SPOT_SEA_FK"] = _PkValue;
                lngPkValue = _PkValue;
                arrMessage = SaveTransaction(MasterDS, GridDS, _PkValue, objWK.MyCommand, objWK.MyUserName, ForFCL);
                if (CalcDS.Tables.Count > 0)
                {
                    arrMessage = SaveCargoCalculator(CalcDS, _PkValue, objWK.MyCommand, objWK.MyUserName, Measure, Wt, DivFac);
                }
                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved")>0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        return arrMessage;
                    }
                    else
                    {
                        if (NewRecord)
                        {
                            RollbackProtocolKey("OPERATOR RFQ SPOT RATE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]),
                                Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RFQNo, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                if (NewRecord)
                {
                    RollbackProtocolKey("OPERATOR RFQ SPOT RATE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RFQNo, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                if (NewRecord)
                {
                    RollbackProtocolKey("OPERATOR RFQ SPOT RATE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RFQNo, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }
        #endregion

        #region "SaveTransaction"
        public ArrayList SaveTransaction(DataSet MasterDS, DataSet Gridds, long PkValue, OracleCommand SCM, string UserName, bool ForFCL)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            long TranPk = 0;
            Int16 AllInRate = default(Int16);
            arrMessage.Clear();
            try
            {
                var _with12 = SCM;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = UserName + ".RFQ_SPOT_RATE_SEA_TBL_PKG.RFQ_SPOT_TRN_SEA_FCL_LCL_INS";

                for (nRowCnt = 0; nRowCnt <= Gridds.Tables[1].Rows.Count - 1; nRowCnt++)
                {
                    DR = Gridds.Tables[1].Rows[nRowCnt];
                    if (ForUpdate(DR, 8, Convert.ToInt16((ForFCL ? -Convert.ToInt16(1) : Convert.ToInt16(10)))))
                    {
                        var _with13 = _with12.Parameters;
                        _with13.Clear();
                        _with13.Add("RFQ_SPOT_SEA_FK_IN", Gridds.Tables[0].Rows[0]["RFQ_SPOT_SEA_FK"]).Direction = ParameterDirection.Input;

                        _with13.Add("PORT_MST_POL_FK_IN", Gridds.Tables[0].Rows[0]["PORT_MST_POL_FK"]).Direction = ParameterDirection.Input;
                        _with13.Add("PORT_MST_POD_FK_IN", Gridds.Tables[0].Rows[0]["PORT_MST_POD_FK"]).Direction = ParameterDirection.Input;
                        _with13.Add("SURCHARGE_IN", Gridds.Tables[1].Rows[nRowCnt]["SURCHARGE"]).Direction = ParameterDirection.Input;


                        if (!ForFCL)
                        {
                            _with13.Add("FREIGHT_ELEMENT_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;

                            AllInRate = Convert.ToInt16(Gridds.Tables[1].Rows[nRowCnt]["SELECTED"] == "true" ? 1 : 0);

                            _with13.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;
                            _with13.Add("CURRENCY_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;

                            _with13.Add("LCL_BASIS_IN", Gridds.Tables[0].Rows[0]["LCL_BASIS"]).Direction = ParameterDirection.Input;
                            _with13.Add("LCL_CURRENT_RATE_IN", Gridds.Tables[1].Rows[nRowCnt]["LCL_CURRENT_RATE"]).Direction = ParameterDirection.Input;

                            _with13.Add("LCL_REQUEST_RATE_IN", Gridds.Tables[1].Rows[nRowCnt]["LCL_REQUEST_RATE"]).Direction = ParameterDirection.Input;

                            _with13.Add("LCL_APPROVED_RATE_IN", Gridds.Tables[1].Rows[nRowCnt]["LCL_REQUEST_RATE"]).Direction = ParameterDirection.Input;

                            _with13.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                            if ((object.ReferenceEquals(Gridds.Tables[0].Rows[0]["VALID_TO"], DBNull.Value)))
                            {
                                _with13.Add("VALID_TO_IN", Gridds.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                            }
                            else if (Gridds.Tables[0].Rows[0]["VALID_TO"] == "  /  /    ")
                            {
                                _with13.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with13.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_TO"]))).Direction = ParameterDirection.Input;
                            }

                            _with13.Add("CONTAINER_DTL_FCL_IN", DBNull.Value).Direction = ParameterDirection.Input;

                            //Added by rabbani reason USS Gap
                            if (string.IsNullOrEmpty(Gridds.Tables[1].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"].ToString()))
                            {
                                _with13.Add("LCL_CURRENT_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with13.Add("LCL_CURRENT_MIN_RATE_IN", Convert.ToDouble(Gridds.Tables[1].Rows[nRowCnt]["LCL_CURRENT_MIN_RATE"])).Direction = ParameterDirection.Input;
                            }

                            //Added by rabbani reason USS Gap
                            if (string.IsNullOrEmpty(Gridds.Tables[1].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"].ToString()))
                            {
                                _with13.Add("LCL_REQUEST_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with13.Add("LCL_REQUEST_MIN_RATE_IN", Convert.ToDouble(Gridds.Tables[1].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"])).Direction = ParameterDirection.Input;
                            }

                            //Added by rabbani reason USS Gap
                            if (string.IsNullOrEmpty(Gridds.Tables[1].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"].ToString()))
                            {
                                _with13.Add("LCL_APPROVED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with13.Add("LCL_APPROVED_MIN_RATE_IN", Convert.ToDouble(Gridds.Tables[1].Rows[nRowCnt]["LCL_REQUEST_MIN_RATE"])).Direction = ParameterDirection.Input;
                            }
                        }
                        else
                        {
                            _with13.Add("LCL_CURRENT_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            _with13.Add("LCL_REQUEST_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            _with13.Add("LCL_APPROVED_MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            _with13.Add("FREIGHT_ELEMENT_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["FrElmtFk"]).Direction = ParameterDirection.Input;

                            AllInRate = Convert.ToInt16(Gridds.Tables[1].Rows[nRowCnt]["Selected"] == "true" ? 1 : 0);

                            _with13.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;

                            _with13.Add("CURRENCY_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["CurrencyFk"]).Direction = ParameterDirection.Input;

                            _with13.Add("LCL_BASIS_IN", Gridds.Tables[0].Rows[0]["LCL_BASIS"]).Direction = ParameterDirection.Input;
                            _with13.Add("LCL_CURRENT_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            _with13.Add("LCL_REQUEST_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            _with13.Add("LCL_APPROVED_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            _with13.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                            if (object.ReferenceEquals(Gridds.Tables[0].Rows[0]["VALID_TO"], DBNull.Value))
                            {
                                _with13.Add("VALID_TO_IN", Gridds.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                            }
                            else if (Gridds.Tables[0].Rows[0]["VALID_TO"] == "  /  /    ")
                            {
                                _with13.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with13.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_TO"]))).Direction = ParameterDirection.Input;
                            }

                            _with13.Add("CONTAINER_DTL_FCL_IN", GetContainerDtlFcl(Gridds.Tables[1].Rows[nRowCnt],0 , true)).Direction = ParameterDirection.Input;
                        }
                        _with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with12.ExecuteNonQuery();

                    }

                }
                arrMessage.Add("All data saved successfully");
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
        #endregion

        #region "GenerateRFQNo"
        public new string GenerateRFQNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null, string SLID = "", string POLID = "", string PODID = "")
        {
            string functionReturnValue = null;
            try
            {
                functionReturnValue = GenerateProtocolKey("OPERATOR RFQ SPOT RATE", nLocationId, nEmployeeId, DateTime.Now,"" ,"" , POLID, nCreatedBy, ObjWK, SLID,
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
        #endregion

        #region "GetContainerDtlFcl"
        private string GetContainerDtlFcl(DataRow dr, Int16 StartFrom = 8, bool NewRecord = false)
        {
            string functionReturnValue = null;
            Int16 nColCnt = default(Int16);
            for (nColCnt = StartFrom; nColCnt <= dr.Table.Columns.Count - 3; nColCnt += 3)
            {
                if (NewRecord)
                {
                    functionReturnValue += dr.Table.Columns[nColCnt + 1].Caption + "~" + Convert.ToString(dr[nColCnt]).Trim() + "~" + Convert.ToString(dr[nColCnt + 1]).Trim() + "~" + Convert.ToString(dr[nColCnt + 1]).Trim() + "$";
                }
                else
                {
                    functionReturnValue += dr.Table.Columns[nColCnt + 1].Caption + "~" + Convert.ToString(dr[nColCnt]).Trim() + "~" + Convert.ToString(dr[nColCnt + 1]).Trim() + "~" + Convert.ToString(dr[nColCnt + 2]).Trim() + "$";
                }
            }
            return functionReturnValue;
        }

        private bool ForUpdate(DataRow dr, Int16 StartFrom = 8, Int16 UpTo = -1)
        {
            Int16 i = default(Int16);
            if (UpTo < StartFrom)
                UpTo = Convert.ToInt16(dr.Table.Columns.Count - 1);
            for (i = StartFrom; i <= UpTo; i++)
            {
                if ((!object.ReferenceEquals(dr[i], DBNull.Value)))
                {
                    if (Convert.ToString(dr[i]).Trim().Length != 0)
                    {
                        Int32 value = Convert.ToInt32(dr[i].ToString());
                        if (value > 0)
                        {
                            if (Convert.ToDouble(dr[i]) != 0)
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }
        #endregion

        #region " Cargo Calculator Save "
        private ArrayList SaveCargoCalculator(DataSet DS, long PkValue, OracleCommand SCM, string UserName, string Measure, string Wt, string Divfac)
        {
            Int16 nRowCnt = default(Int16);
            long TranPk = 0;
            DataTable DT = DS.Tables[0];

            arrMessage.Clear();
            try
            {
                var _with14 = SCM;
                _with14.CommandType = CommandType.Text;
                _with14.CommandText = " Delete RFQ_SPOT_SEA_CARGO_CALC where RFQ_SPOT_SEA_FK = " + PkValue;
                _with14.Parameters.Clear();
                _with14.ExecuteNonQuery();

                _with14.CommandType = CommandType.StoredProcedure;
                for (nRowCnt = 0; nRowCnt <= DT.Rows.Count - 1; nRowCnt++)
                {
                    if (Convert.ToInt32(getDefault(DT.Rows[nRowCnt]["NOP"], 0)) > 0)
                    {
                        _with14.CommandText = UserName + ".RFQ_SPOT_RATE_SEA_TBL_PKG.RFQ_SPOT_SEA_CARGO_CALC_INS";
                        var _with15 = _with14.Parameters;
                        _with15.Clear();
                        _with15.Add("RFQ_SPOT_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with15.Add("CARGO_NOP_IN", DT.Rows[nRowCnt]["NOP"]).Direction = ParameterDirection.Input;

                        _with15.Add("CARGO_LENGTH_IN", DT.Rows[nRowCnt]["Length"]).Direction = ParameterDirection.Input;

                        _with15.Add("CARGO_WIDTH_IN", DT.Rows[nRowCnt]["Width"]).Direction = ParameterDirection.Input;
                        _with15.Add("CARGO_HEIGHT_IN", DT.Rows[nRowCnt]["Height"]).Direction = ParameterDirection.Input;

                        _with15.Add("CARGO_CUBE_IN", DT.Rows[nRowCnt]["Cube"]).Direction = ParameterDirection.Input;


                        _with15.Add("CARGO_VOLUME_WT_IN", DT.Rows[nRowCnt]["VolWeight"]).Direction = ParameterDirection.Input;

                        _with15.Add("CARGO_ACTUAL_WT_IN", DT.Rows[nRowCnt]["ActWeight"]).Direction = ParameterDirection.Input;
                        _with15.Add("CARGO_DENSITY_IN", DT.Rows[nRowCnt]["Density"]).Direction = ParameterDirection.Input;

                        _with15.Add("CARGO_MEASURE_IN", (string.IsNullOrEmpty(Measure) ? "0" : Measure)).Direction = ParameterDirection.Input;
                        _with15.Add("CARGO_WT_IN", (string.IsNullOrEmpty(Wt) ? "0" : Wt)).Direction = ParameterDirection.Input;
                        _with15.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(Divfac) ? "0" : Divfac)).Direction = ParameterDirection.Input;

                        _with15.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with14.ExecuteNonQuery();
                        TranPk = Convert.ToInt64(_with14.Parameters["RETURN_VALUE"].Value);
                    }
                }
                arrMessage.Add("All data saved successfully");
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
        #endregion

        #region " Fetch One "
        public DataSet FetchOne(DataSet GridDS, string rfqSpotRatePK = "", bool isFCL = true, bool forApproval = false, string containerIDs = "'20RF','20GP','40GP','40RF','45GP'", string rfqNo = "", string rfqDate = "", string operatorPk = "", string operatorID = "", string operatorName = "",
        string customerPk = "", string customerID = "", string customerName = "", string contPk = "", string contractNo = "", string polPk = "", string polID = "", string polName = "", string podPk = "", string podID = "",
        string podName = "", string fdate = "", string tdate = "", string commodityPk = "", string commodityID = "", string commodityName = "", string GROSS_WEIGHT = "", string VOLUME_IN_CBM = "", string PACK_COUNT = "", string CHARGEABLE_WEIGHT = "",
        string VOLUME_WEIGHT = "", string DENSITY = "", string CollectionAddress = "", DataSet CalcDS = null, int CommodityGroupFk = 0)
        {
            string ContainerPKs = null;
            bool NewRecord = true;
            if (rfqSpotRatePK.Trim().Length > 0)
                NewRecord = false;

            string strSQL = null;
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            DataSet ds = null;
            GridDS.Tables.Clear();
            try
            {
                strCondition = " AND 1 = 2 ";
                if (NewRecord == false)
                    strCondition = " AND main.RFQ_SPOT_SEA_PK = " + rfqSpotRatePK + " ";

                buildQuery.Append("    Select ");
                buildQuery.Append("       RFQ_SPOT_SEA_PK, ");
                buildQuery.Append("       NVL(main.ACTIVE, 0) ACTIVE, ");
                buildQuery.Append("       RFQ_REF_NO, ");
                buildQuery.Append("       to_Char(main.RFQ_DATE, '" + dateFormat + "') RFQ_DATE, ");
                buildQuery.Append("       main.OPERATOR_MST_FK, ");
                buildQuery.Append("       OPERATOR_ID, ");
                buildQuery.Append("       OPERATOR_NAME, ");
                buildQuery.Append("       main.CUSTOMER_MST_FK, ");
                buildQuery.Append("       CUSTOMER_ID, ");
                buildQuery.Append("       CUSTOMER_NAME, ");
                buildQuery.Append("       main.CONT_MAIN_SEA_FK, ");
                buildQuery.Append("       CONTRACT_NO, ");
                buildQuery.Append("       decode(main.CARGO_TYPE, 1, 'FCL',2, 'LCL') CARGO_TYPE, ");
                buildQuery.Append("       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
                buildQuery.Append("       to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
                buildQuery.Append("       main.COMMODITY_MST_FK, ");
                buildQuery.Append("       COMMODITY_ID, ");
                buildQuery.Append("       COMMODITY_NAME, ");
                buildQuery.Append("       main.APPROVED , ");
                buildQuery.Append("       NVL(main.ACTIVE, 0) ACTIVE,");
                buildQuery.Append("       main.LAST_MODIFIED_BY_FK, ");
                buildQuery.Append("       USER_NAME, ");
                buildQuery.Append("       to_Char(main.LAST_MODIFIED_DT, '" + dateFormat + "') LAST_MODIFIED_DT, ");
                // 21
                buildQuery.Append("       main.VERSION_NO, ");
                // 22
                buildQuery.Append("       NVL(main.TEUS_VOL, 0) TEUS_VOL, ");
                // 23
                buildQuery.Append("       NVL(main.GROSS_WEIGHT, 0) GROSS_WEIGHT, ");
                // 24
                buildQuery.Append("       NVL(main.VOLUME_IN_CBM, 0) VOLUME_IN_CBM, ");
                // 25
                buildQuery.Append("       NVL(main.PACK_COUNT, 0) PACK_COUNT, ");
                // 26
                buildQuery.Append("       NVL(main.CHARGEABLE_WEIGHT, 0) CHARGEABLE_WEIGHT, ");
                // 27
                buildQuery.Append("       NVL(main.VOLUME_WEIGHT, 0) VOLUME_WEIGHT, ");
                // 28
                buildQuery.Append("       NVL(main.DENSITY, 0) DENSITY, ");
                // 29
                buildQuery.Append("       main.COL_ADDRESS,");
                //30
                buildQuery.Append("       main.COMMODITY_GROUP_FK,");
                //31
                buildQuery.Append("       main.base_currency_fk,");
                buildQuery.Append("       curr.currency_id");
                //31
                buildQuery.Append("       from ");
                buildQuery.Append("       RFQ_SPOT_RATE_SEA_TBL main, ");
                buildQuery.Append("       OPERATOR_MST_TBL, ");
                buildQuery.Append("       CUSTOMER_MST_TBL, ");
                buildQuery.Append("       CONT_MAIN_SEA_TBL, ");
                buildQuery.Append("       COMMODITY_MST_TBL, ");
                buildQuery.Append("       USER_MST_TBL, ");
                buildQuery.Append("       CURRENCY_TYPE_MST_TBL CURR ");
                buildQuery.Append("      where 1 = 1 " + strCondition);
                buildQuery.Append("       AND main.OPERATOR_MST_FK  = OPERATOR_MST_PK ");
                buildQuery.Append("       AND main.CUSTOMER_MST_FK  = CUSTOMER_MST_PK (+) ");
                buildQuery.Append("       AND main.CONT_MAIN_SEA_FK = CONT_MAIN_SEA_PK(+) ");
                buildQuery.Append("       AND main.COMMODITY_MST_FK = COMMODITY_MST_PK(+) ");
                buildQuery.Append("       AND main.LAST_MODIFIED_BY_FK = USER_MST_PK(+) ");
                buildQuery.Append("       AND main.base_currency_fk = curr.currency_mst_pk(+) ");
                buildQuery.Append("   ");
                strSQL = buildQuery.ToString();

                ds = objWF.GetDataSet(strSQL);
                if (!NewRecord)
                    isFCL = (ds.Tables[0].Rows[0]["CARGO_TYPE"] == "FCL" ? true : false);
                if (NewRecord == true)
                {
                    ds.Tables[0].Rows.Add(ds.Tables[0].NewRow());
                    var _with16 = ds.Tables[0].Rows[0];
                    _with16["RFQ_SPOT_SEA_PK"] = 0;
                    _with16["ACTIVE"] = 1;
                    _with16["RFQ_REF_NO"] = rfqNo;
                    _with16["RFQ_DATE"] = rfqDate;
                    _with16["OPERATOR_MST_FK"] = operatorPk;
                    _with16["OPERATOR_ID"] = operatorID;
                    _with16["OPERATOR_NAME"] = operatorName;
                    _with16["CUSTOMER_MST_FK"] = customerPk;
                    _with16["CUSTOMER_ID"] = customerID;
                    _with16["CUSTOMER_NAME"] = customerName;
                    _with16["CONT_MAIN_SEA_FK"] = ifDBNull(contPk);
                    _with16["CONTRACT_NO"] = ifDBNull(contractNo);
                    _with16["CARGO_TYPE"] = (isFCL == true ? "FCL" : "LCL");
                    _with16["VALID_FROM"] = fdate;
                    _with16["VALID_TO"] = tdate;
                    _with16["COMMODITY_MST_FK"] = ifDBNull(commodityPk);
                    _with16["COMMODITY_ID"] = ifDBNull(commodityID);
                    _with16["COMMODITY_NAME"] = ifDBNull(commodityName);
                    _with16["APPROVED"] = 0;
                    _with16["ACTIVE"] = 0;
                    _with16["TEUS_VOL"] = 0;
                    _with16["GROSS_WEIGHT"] = 0;
                    _with16["VOLUME_IN_CBM"] = 0;
                    _with16["PACK_COUNT"] = 0;
                    _with16["CHARGEABLE_WEIGHT"] = 0;
                    _with16["VOLUME_WEIGHT"] = 0;
                    _with16["DENSITY"] = 0;
                    _with16["COL_ADDRESS"] = ifDBNull(CollectionAddress);
                    _with16["base_currency_fk"] = HttpContext.Current.Session["CURRENCY_MST_PK"];
                }
                strCondition = " AND 1 = 2 ";
                if (NewRecord == false)
                    strCondition = " AND RFQ_SPOT_SEA_FK = " + rfqSpotRatePK + " ";

                buildQuery.Remove(0, buildQuery.Length);

                buildQuery.Append("    Select DISTINCT ");
                buildQuery.Append("       main.RFQ_SPOT_SEA_FK, ");
                // 0
                buildQuery.Append("       main.PORT_MST_POL_FK, ");
                // 1
                buildQuery.Append("       PORTPOL.PORT_ID PORTPOL_ID, ");
                // 2
                buildQuery.Append("       PORTPOL.PORT_NAME PORTPOL_NAME, ");
                // 3
                buildQuery.Append("       main.PORT_MST_POD_FK, ");
                // 4
                buildQuery.Append("       PORTPOD.PORT_ID PORTPOD_ID, ");
                // 5
                buildQuery.Append("       PORTPOD.PORT_NAME PORTPOD_NAME, ");
                // 6
                buildQuery.Append("       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
                // 7
                buildQuery.Append("       to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
                // 8
                buildQuery.Append("       NVL(main.LCL_BASIS,0) LCL_BASIS, ");
                // 9
                buildQuery.Append("       NVL(DIMENTION_ID,'') DIMENTION_ID ");
                // 10

                buildQuery.Append("      from ");
                buildQuery.Append("       RFQ_SPOT_TRN_SEA_FCL_LCL main, ");
                buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
                buildQuery.Append("       PORT_MST_TBL PORTPOD,");
                buildQuery.Append("       DIMENTION_UNIT_MST_TBL ");
                buildQuery.Append("      where 1 = 1 " + strCondition);
                buildQuery.Append("       AND PORT_MST_POL_FK = PORTPOL.PORT_MST_PK ");
                buildQuery.Append("       AND PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
                buildQuery.Append("       AND LCL_BASIS = DIMENTION_UNIT_MST_PK(+) ");

                strSQL = buildQuery.ToString();

                DataTable newChild = null;
                string str1sql = null;
                str1sql = "select P.UOM_KG, d.dimention_id FROM PARAMETERS_TBL P, dimention_unit_mst_tbl D WHERE P.UOM_KG=D.DIMENTION_UNIT_MST_PK";
                newChild = objWF.GetDataTable(str1sql);
                DataTable ChildDt = null;
                ChildDt = objWF.GetDataTable(strSQL);
                if (NewRecord == true)
                {
                    ChildDt.Rows.Add(ChildDt.NewRow());
                    var _with17 = ChildDt.Rows[0];
                    _with17["RFQ_SPOT_SEA_FK"] = 0;
                    _with17["PORT_MST_POL_FK"] = polPk;
                    _with17["PORTPOL_ID"] = polID;
                    _with17["PORTPOL_NAME"] = polName;
                    _with17["PORT_MST_POD_FK"] = podPk;
                    _with17["PORTPOD_ID"]= podID;
                    _with17["PORTPOD_NAME"] = podName;
                    _with17["VALID_FROM"] = fdate;
                    _with17["VALID_TO"] = tdate;
                    if (!isFCL)
                    {
                        _with17["LCL_BASIS"] = newChild.Rows[0][0];
                        _with17["DIMENTION_ID"] = newChild.Rows[0][1];
                    }
                }

                strCondition = " AND 1 = 2 ";
                if (NewRecord == false)
                    strCondition = " AND RFQ_SPOT_SEA_FK = " + rfqSpotRatePK + " ";

                buildQuery.Remove(0, buildQuery.Length);

                if (isFCL == true)
                {
                    buildQuery.Append("    Select ");
                    buildQuery.Append("       main.FREIGHT_ELEMENT_MST_FK, ");
                    // 0
                    buildQuery.Append("       FREIGHT_ELEMENT_ID, ");
                    // 1
                    buildQuery.Append("       FREIGHT_ELEMENT_NAME, ");
                    // 2
                    buildQuery.Append("       DECODE(CHECK_FOR_ALL_IN_RT, 0,'false','true') SELECTED, ");
                    // 3
                    buildQuery.Append("       main.CURRENCY_MST_FK, ");
                    // 4
                    buildQuery.Append("       CURRENCY_ID, ");
                    // 5
                    buildQuery.Append("       CURRENCY_NAME, ");
                    // 6
                    buildQuery.Append("       CONT.CONTAINER_TYPE_MST_FK, ");
                    // 7
                    buildQuery.Append("       CONTAINER_TYPE_MST_ID, ");
                    // 8
                    buildQuery.Append("       CONTAINER_TYPE_NAME, ");
                    // 9
                    buildQuery.Append("       NVL(CONT.FCL_CURRENT_RATE,0) FCL_CURRENT_RATE, ");
                    // 10
                    buildQuery.Append("       NVL(CONT.FCL_REQ_RATE,0) FCL_REQ_RATE, ");
                    // 11
                    buildQuery.Append("       NVL(CONT.FCL_APP_RATE,0) FCL_APP_RATE, ");
                    // 12
                    buildQuery.Append("       main.PORT_MST_POL_FK, ");
                    // 13
                    buildQuery.Append("       main.PORT_MST_POD_FK ");
                    // 14
                    buildQuery.Append("      from ");
                    buildQuery.Append("       RFQ_SPOT_TRN_SEA_FCL_LCL main, ");
                    buildQuery.Append("       CONTAINER_TYPE_MST_TBL, ");
                    buildQuery.Append("       RFQ_SPOT_TRN_SEA_CONT_DET CONT, ");
                    buildQuery.Append("       FREIGHT_ELEMENT_MST_TBL, ");
                    buildQuery.Append("       CURRENCY_TYPE_MST_TBL ");
                    buildQuery.Append("      where 1 = 1 " + strCondition);
                    buildQuery.Append("       AND FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK ");
                    buildQuery.Append("       AND CONT.RFQ_SPOT_SEA_TRN_FK=main.RFQ_SPOT_SEA_TRN_PK ");
                    buildQuery.Append("       AND FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1 ");
                    buildQuery.Append("       AND CURRENCY_MST_FK = CURRENCY_MST_PK ");
                    buildQuery.Append("       AND FREIGHT_TYPE >= 0 ");
                    buildQuery.Append("       AND CONT.CONTAINER_TYPE_MST_FK = CONTAINER_TYPE_MST_PK(+) ");
                    buildQuery.Append("      Order By PREFERENCE,FREIGHT_ELEMENT_ID desc,container_type_mst_tbl.preferences");
                    //added by purnanand
                    // If LCL
                }
                else
                {
                    buildQuery.Append("       SELECT ");
                    buildQuery.Append("       QRY.FREIGHT_ELEMENT_MST_FK, ");
                    //0
                    buildQuery.Append("       QRY.FREIGHT_ELEMENT_ID, ");
                    // 1
                    buildQuery.Append("       QRY.FREIGHT_ELEMENT_NAME, ");
                    // 2
                    buildQuery.Append("       QRY.SELECTED, ");
                    // 3
                    buildQuery.Append("       QRY.CURRENCY_MST_FK, ");
                    // 4
                    buildQuery.Append("       QRY.CURRENCY_ID, ");
                    // 5
                    buildQuery.Append("       QRY.CURRENCY_NAME, ");
                    // 6
                    buildQuery.Append("       QRY.LCL_BASIS, ");
                    // 7
                    buildQuery.Append("       QRY.DIMENTION_ID, ");
                    // 8
                    buildQuery.Append("       QRY.LCL_CURRENT_MIN_RATE, ");
                    //9 
                    buildQuery.Append("       QRY.LCL_CURRENT_RATE, ");
                    // 10
                    buildQuery.Append("       QRY.LCL_REQUEST_MIN_RATE, ");
                    //11 
                    buildQuery.Append("       QRY.LCL_REQUEST_RATE, ");
                    // 12
                    buildQuery.Append("       QRY.LCL_APPROVED_MIN_RATE, ");
                    // 13 
                    buildQuery.Append("       QRY.LCL_APPROVED_RATE, ");
                    // 14
                    buildQuery.Append("       QRY.PORT_MST_POL_FK, ");
                    // 15    
                    buildQuery.Append("       QRY.PORT_MST_POD_FK FROM (");
                    // 16
                    //END
                    buildQuery.Append("       Select ");
                    buildQuery.Append("       main.FREIGHT_ELEMENT_MST_FK, ");
                    // 0
                    buildQuery.Append("       FRE.FREIGHT_ELEMENT_ID, ");
                    // 1
                    buildQuery.Append("       FRE.FREIGHT_ELEMENT_NAME, ");
                    // 2
                    buildQuery.Append("       DECODE(main.CHECK_FOR_ALL_IN_RT, 1,'true','false') SELECTED, ");
                    // 3
                    buildQuery.Append("       main.CURRENCY_MST_FK, ");
                    // 4
                    buildQuery.Append("       CURR.CURRENCY_ID, ");
                    // 5
                    buildQuery.Append("       CURR.CURRENCY_NAME, ");
                    // 6
                    buildQuery.Append("       main.LCL_BASIS, ");
                    // 7
                    buildQuery.Append("       DIM.DIMENTION_ID, ");
                    // 8
                    buildQuery.Append("       main.LCL_CURRENT_MIN_RATE, ");
                    //9 'Added rabbani reason USS Gap
                    buildQuery.Append("       main.LCL_CURRENT_RATE, ");
                    // 10
                    buildQuery.Append("       main.LCL_REQUEST_MIN_RATE, ");
                    //11 'Added rabbani reason USS Gap
                    buildQuery.Append("       main.LCL_REQUEST_RATE, ");
                    // 12
                    buildQuery.Append("       main.LCL_APPROVED_MIN_RATE, ");
                    // 13 'Added rabbani reason USS Gap
                    buildQuery.Append("       main.LCL_APPROVED_RATE, ");
                    // 14
                    buildQuery.Append("       main.PORT_MST_POL_FK, ");
                    // 15    
                    buildQuery.Append("       main.PORT_MST_POD_FK ");
                    // 16
                    buildQuery.Append("       ,FRE.PREFERENCE ");
                    // SNIGDHARANI - 10/12/2008
                    buildQuery.Append("       from ");
                    buildQuery.Append("       RFQ_SPOT_TRN_SEA_FCL_LCL main, ");
                    buildQuery.Append("       FREIGHT_ELEMENT_MST_TBL FRE, ");
                    buildQuery.Append("       CURRENCY_TYPE_MST_TBL CURR, ");
                    buildQuery.Append("       DIMENTION_UNIT_MST_TBL DIM ");
                    //buildQuery.Append(vbCrLf & "       CONT_TRN_SEA_FCL_LCL CONT ")
                    buildQuery.Append("       where 1 = 1 " + strCondition);
                    // JOIN CONDITION
                    buildQuery.Append("       AND main.FREIGHT_ELEMENT_MST_FK = FRE.FREIGHT_ELEMENT_MST_PK ");
                    //buildQuery.Append(vbCrLf & "       AND FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1 ")
                    buildQuery.Append("       AND FRE.ACTIVE_FLAG = 1 ");
                    buildQuery.Append("       AND main.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ");
                    //modified  by latha for fetching the freight elements THD,THL... having the FREIGHT TYPE AS 0. 
                    buildQuery.Append("       AND FRE.FREIGHT_TYPE >= 0 ");
                    buildQuery.Append("       AND main.LCL_BASIS = DIM.DIMENTION_UNIT_MST_PK ");
                    buildQuery.Append("       AND FRE.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)");
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column.
                    //buildQuery.Append(vbCrLf & "       AND CONT.FREIGHT_ELEMENT_MST_FK = FRE.FREIGHT_ELEMENT_MST_PK ")
                    //buildQuery.Append(vbCrLf & "      Order By FRE.FREIGHT_ELEMENT_ID ")

                    buildQuery.Append("    UNION ALL");
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column.
                    buildQuery.Append("       Select ");
                    buildQuery.Append("       main.FREIGHT_ELEMENT_MST_FK, ");
                    // 0
                    buildQuery.Append("       FRE.FREIGHT_ELEMENT_ID, ");
                    // 1
                    buildQuery.Append("       FRE.FREIGHT_ELEMENT_NAME, ");
                    // 2
                    //buildQuery.Append(vbCrLf & "       NVL(main.CHECK_FOR_ALL_IN_RT, 0) SELECTED, ") ' 3
                    buildQuery.Append("       DECODE(main.CHECK_FOR_ALL_IN_RT, 1,'true','false') SELECTED, ");
                    // 3
                    buildQuery.Append("       main.CURRENCY_MST_FK, ");
                    // 4
                    buildQuery.Append("       CURR.CURRENCY_ID, ");
                    // 5
                    buildQuery.Append("       CURR.CURRENCY_NAME, ");
                    // 6
                    buildQuery.Append("       main.LCL_BASIS, ");
                    // 7
                    buildQuery.Append("       DIM.DIMENTION_ID, ");
                    // 8
                    //buildQuery.Append(vbCrLf & "       CONT.LCL_APPROVED_MIN_RATE, ") '9 'RRR
                    buildQuery.Append("       main.LCL_CURRENT_MIN_RATE, ");
                    //9 'Added rabbani reason USS Gap
                    buildQuery.Append("       main.LCL_CURRENT_RATE, ");
                    // 10
                    buildQuery.Append("       main.LCL_REQUEST_MIN_RATE, ");
                    //11 'Added rabbani reason USS Gap
                    buildQuery.Append("       main.LCL_REQUEST_RATE, ");
                    // 12
                    buildQuery.Append("       main.LCL_APPROVED_MIN_RATE, ");
                    // 13 'Added rabbani reason USS Gap
                    buildQuery.Append("       main.LCL_APPROVED_RATE, ");
                    // 14
                    buildQuery.Append("       main.PORT_MST_POL_FK, ");
                    // 15    
                    buildQuery.Append("       main.PORT_MST_POD_FK ");
                    // 16
                    buildQuery.Append("       ,FRE.PREFERENCE");
                    //SNIGDHARANI - 10/12/2008
                    buildQuery.Append("       from ");
                    buildQuery.Append("       RFQ_SPOT_TRN_SEA_FCL_LCL main, ");
                    buildQuery.Append("       FREIGHT_ELEMENT_MST_TBL FRE, ");
                    buildQuery.Append("       CURRENCY_TYPE_MST_TBL CURR, ");
                    buildQuery.Append("       DIMENTION_UNIT_MST_TBL DIM ");
                    //buildQuery.Append(vbCrLf & "       CONT_TRN_SEA_FCL_LCL CONT ")
                    buildQuery.Append("       where 1 = 1 " + strCondition);
                    // JOIN CONDITION
                    buildQuery.Append("       AND main.FREIGHT_ELEMENT_MST_FK = FRE.FREIGHT_ELEMENT_MST_PK ");
                    //buildQuery.Append(vbCrLf & "       AND FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1 ")
                    buildQuery.Append("       AND FRE.ACTIVE_FLAG = 1 ");
                    buildQuery.Append("       AND main.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ");
                    //modified  by latha for fetching the freight elements THD,THL... having the FREIGHT TYPE AS 0. 
                    buildQuery.Append("       AND FRE.FREIGHT_TYPE >= 0 ");
                    buildQuery.Append("       AND main.LCL_BASIS = DIM.DIMENTION_UNIT_MST_PK ");
                    buildQuery.Append("       AND FRE.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)");
                    //buildQuery.Append(vbCrLf & "       AND CONT.FREIGHT_ELEMENT_MST_FK = FRE.FREIGHT_ELEMENT_MST_PK ")
                    //buildQuery.Append(vbCrLf & "       Order By FRE.FREIGHT_ELEMENT_ID ")
                    buildQuery.Append("       ) QRY ORDER BY QRY.PREFERENCE ,FREIGHT_ELEMENT_ID desc");
                    //SNIGDHARANI - 10/12/2008
                }
                //End by rabbani on 24/3/07
                strSQL = buildQuery.ToString();

                DataTable FrtDt = null;
                FrtDt = objWF.GetDataTable(strSQL);

                DataTable VryTbl = new DataTable();
                if (isFCL == true)
                {
                    if (!NewRecord)
                        containerIDs = getContainerIDs(FrtDt);
                    // Container IDs

                    VryTbl.Columns.Add("POL", ChildDt.Columns[1].DataType);
                    VryTbl.Columns.Add("POD", ChildDt.Columns[4].DataType);
                    VryTbl.Columns.Add("FrElmtFk", FrtDt.Columns[0].DataType);
                    VryTbl.Columns.Add("FrElmtID");
                    VryTbl.Columns.Add("FrElmtName");
                    VryTbl.Columns.Add("Selected", FrtDt.Columns[3].DataType);
                    VryTbl.Columns.Add("CurrencyFk", FrtDt.Columns[4].DataType);
                    VryTbl.Columns.Add("CurrencyID");

                    AddContainerColumnToMaster(ChildDt, containerIDs);
                    AddContainerColumnsToChild(VryTbl, containerIDs);

                    if (!NewRecord)
                        TransferData(FrtDt, VryTbl);

                    if (NewRecord)
                    {
                        string CurrID = null;
                        string CurrFk = null;
                        CurrID = Convert.ToString(HttpContext.Current.Session["CURRENCY_ID"]);
                        CurrFk = Convert.ToString(HttpContext.Current.Session["CURRENCY_MST_PK"]);

                        MakeRowsForAllFreightElements(VryTbl, polPk, podPk, CurrFk, CurrID, fdate, commodityPk, commodityID, contPk, contractNo,
                        operatorPk, CommodityGroupFk);
                    }

                    GridDS.Tables.Add(ChildDt);
                    GridDS.Tables.Add(VryTbl);

                    DataRelation REL = new DataRelation("RFQRelation", new DataColumn[] {
                        GridDS.Tables[0].Columns["PORT_MST_POL_FK"],
                        GridDS.Tables[0].Columns["PORT_MST_POD_FK"]
                    }, new DataColumn[] {
                        GridDS.Tables[1].Columns["POL"],
                        GridDS.Tables[1].Columns["POD"]
                    });
                    GridDS.Relations.Add(REL);
                    // If LCL
                }
                else
                {
                    if (NewRecord)
                    {
                        string CurrID = null;
                        string CurrName = null;
                        string CurrFk = null;
                        CurrID = Convert.ToString(HttpContext.Current.Session["CURRENCY_ID"]);
                        CurrFk = Convert.ToString(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                        CurrName = Convert.ToString(HttpContext.Current.Session["CURRENCY_NAME"]);
                        //end

                        if (tdate == null)
                            tdate = "";
                        if (object.ReferenceEquals(tdate, DBNull.Value))
                            tdate = "";
                        MakeRowsForAllFreightElements(ChildDt, FrtDt, polPk, podPk, CurrFk, CurrID, CurrName, rfqDate, commodityPk, commodityID,
                        contPk, contractNo, operatorPk, CommodityGroupFk);
                    }
                    AddColumnToMaster(ChildDt);
                    // Adding Columns to parent grid
                    GridDS.Tables.Add(ChildDt);
                    GridDS.Tables.Add(FrtDt);
                    DataRelation REL = new DataRelation("RFQRelation", new DataColumn[] {
                        GridDS.Tables[0].Columns["PORT_MST_POL_FK"],
                        GridDS.Tables[0].Columns["PORT_MST_POD_FK"]
                    }, new DataColumn[] {
                        GridDS.Tables[1].Columns["PORT_MST_POL_FK"],
                        GridDS.Tables[1].Columns["PORT_MST_POD_FK"]
                    });
                    GridDS.Relations.Add(REL);
                }
                // Cargo Calculator Information
                if (!NewRecord)
                {
                    strSQL = "    Select                                                    " + "        ROWNUM                             SNo,                " + "        CARGO_NOP                          NOP,                " + "        CARGO_LENGTH                       Length,             " + "        CARGO_WIDTH                        Width,              " + "        CARGO_HEIGHT                       Height,             " + "        CARGO_CUBE                         Cube,               " + "        CARGO_VOLUME_WT                    VolWeight,          " + "        CARGO_ACTUAL_WT                    ActWeight,          " + "        CARGO_DENSITY                      Density,            " + "        RFQ_SPOT_SEA_FK                     FK                 " + "         FROM                                                  " + "        RFQ_SPOT_SEA_CARGO_CALC                                " + "         WHERE                                                 " + "        RFQ_SPOT_SEA_FK                = " + rfqSpotRatePK + "     ";

                    strSQL = strSQL.Replace("   ", " ");
                    strSQL = strSQL.Replace("  ", " ");

                    CalcDS = objWF.GetDataSet(strSQL);
                }
                return ds;
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
        public DataSet getDivFacMW(string Pk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                if ((Pk != null))
                {
                    if (!string.IsNullOrEmpty(Pk))
                    {
                        strQuery.Append("select distinct cargo_measurement, cargo_weight_in, cargo_division_fact " );
                        strQuery.Append("from RFQ_SPOT_SEA_CARGO_CALC where " );
                        strQuery.Append("rfq_spot_sea_fk = " + Pk);
                        return objWF.GetDataSet(strQuery.ToString());
                    }
                }
                return null;
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

        #endregion

        #region " Getting Container IDs in case of existing FCL records "
        private string getContainerIDs(DataTable DT)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            string frpk = null;
            strBuilder.Append("");
            try
            {
                if (DT.Rows.Count > 0)
                {
                    frpk = Convert.ToString(DT.Rows[0]["FREIGHT_ELEMENT_MST_FK"]);
                }
                for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                {
                    if (frpk != DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"])
                        break; // TODO: might not be correct. Was : Exit For
                    strBuilder.Append("'" + removeDBNull(DT.Rows[RowCnt]["CONTAINER_TYPE_MST_ID"]).ToString().Trim() + "',");
                }
                if (DT.Rows.Count > 0)
                    strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
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
        #endregion

        #region " FCL Column Add "
        private void AddContainerColumnToMaster(DataTable DT, string CIDs)
        {
            Array CHeads = null;
            CHeads = CIDs.Split(',');
            Int16 i = default(Int16);
            for (i = 0; i <= CHeads.Length - 1; i++)
            {
                DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)).Replace("'", ""), typeof(decimal));
            }
        }

        // FCL
        private void AddContainerColumnsToChild(DataTable DT, string CIDs)
        {
            Array CHeads = null;
            string hed = null;
            CHeads = CIDs.Split(',');
            Int16 i = default(Int16);
            for (i = 0; i <= CHeads.Length - 1; i++)
            {
                DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)), typeof(decimal));
                DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)).Replace("'", ""), typeof(decimal));
                DT.Columns.Add();
            }
        }
        #endregion

        #region " LCL Column Add "
        // LCL
        private void AddColumnToMaster(DataTable DT)
        {
            DT.Columns.Add("Current Rate", typeof(decimal));
            DT.Columns.Add("Requested Rate", typeof(decimal));
            DT.Columns.Add("Approved Rate", typeof(decimal));
        }

        // LCL
        private void AddColumnsToChild(DataTable DT)
        {
            DT.Columns.Add("Current Minimum Rate", typeof(decimal));
            DT.Columns.Add("Current Rate", typeof(decimal));
            DT.Columns.Add("Requested Minimum Rate", typeof(decimal));
            DT.Columns.Add("Requested Rate", typeof(decimal));
            DT.Columns.Add("Approved Minimum Rate", typeof(decimal));
            DT.Columns.Add("Approved Rate", typeof(decimal));
        }
        #endregion

        #region " Transfer Data from rows to columns FCL Only "
        // FCL
        private void TransferData(DataTable fromDT, DataTable toDT)
        {

            string strFR = "";
            Int16 RowCnt = default(Int16);
            Int16 toDTRowCnt = -1;
            Int16 toDTColCnt = default(Int16);
            try
            {
                for (RowCnt = 0; RowCnt <= fromDT.Rows.Count - 1; RowCnt++)
                {
                    if (fromDT.Rows[RowCnt]["FREIGHT_ELEMENT_ID"] == strFR)
                    {
                        toDT.Rows[toDTRowCnt][toDTColCnt + 1] = fromDT.Rows[RowCnt]["FCL_CURRENT_RATE"];
                        toDT.Rows[toDTRowCnt][toDTColCnt + 2] = fromDT.Rows[RowCnt]["FCL_REQ_RATE"];
                        toDT.Rows[toDTRowCnt][toDTColCnt + 3] = fromDT.Rows[RowCnt]["FCL_APP_RATE"];
                        toDTColCnt += 3;
                    }
                    else
                    {
                        toDTRowCnt += 1;
                        toDTColCnt = -1;
                        DataRow Dr = null;
                        Dr = toDT.NewRow();
                        Dr["POL"] = fromDT.Rows[RowCnt]["PORT_MST_POL_FK"];
                        Dr["POD"] = fromDT.Rows[RowCnt]["PORT_MST_POD_FK"];
                        Dr["FrElmtFk"] = fromDT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"];
                        strFR = Convert.ToString(fromDT.Rows[RowCnt]["FREIGHT_ELEMENT_ID"]);
                        Dr["FrElmtID"] = strFR;
                        Dr["FrElmtName"] = fromDT.Rows[RowCnt]["FREIGHT_ELEMENT_NAME"];
                        Dr["Selected"] = fromDT.Rows[RowCnt]["SELECTED"];
                        Dr["CurrencyFk"] = fromDT.Rows[RowCnt]["CURRENCY_MST_FK"];
                        Dr["CurrencyID"] = fromDT.Rows[RowCnt]["CURRENCY_ID"];
                        toDTColCnt += 8;
                        Dr[toDTColCnt + 1] = fromDT.Rows[RowCnt]["FCL_CURRENT_RATE"];
                        Dr[toDTColCnt + 2] = fromDT.Rows[RowCnt]["FCL_REQ_RATE"];
                        Dr[toDTColCnt + 3] = fromDT.Rows[RowCnt]["FCL_APP_RATE"];
                        toDTColCnt += 3;
                        toDT.Rows.Add(Dr);
                    }
                }
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
        #endregion

        #region " New Record Freight elements for FCL "
        private void MakeRowsForAllFreightElements(DataTable DT, string polFk, string podFk, string currencyFk, string currencyID, string RFQdate, string CommPk, string CommId, object ContPK, object ContractNo,
        object operator1, int CommodityGroupFk = 0)
        {
            DataTable frtDt = null;
            string FrtPks = "-1";
            DataRow Dr = null;
            DataTable ActiveContractData = null;
            try
            {
                ActiveContractData = FetchActiveContractInformation(polFk, podFk, RFQdate, true, CommPk, CommId, Convert.ToString(CommodityGroupFk),"" , Convert.ToString(operator1));
                foreach (DataRow Dr_loopVariable in ActiveContractData.Rows)
                {
                    Dr = Dr_loopVariable;
                    FrtPks += "," + Dr["FREIGHT_ELEMENT_MST_FK"];
                }
                string strSQL = " Select " + " FREIGHT_ELEMENT_MST_PK, FREIGHT_ELEMENT_ID, FREIGHT_ELEMENT_NAME " + " from FREIGHT_ELEMENT_MST_TBL where ACTIVE_FLAG = 1 " + " AND BUSINESS_TYPE in (2,3) " + " AND CHARGE_TYPE IN (1, 2) ";
                ///& " AND BY_DEFAULT = 1 " & vbCrLf 

                if (FrtPks != "-1")
                {
                    strSQL += " AND FREIGHT_ELEMENT_MST_PK in (" + FrtPks + ")";
                }
                strSQL += " Order By PREFERENCE ";

                frtDt = (new WorkFlow()).GetDataTable(strSQL);
                Int16 RowCnt = default(Int16);
                for (RowCnt = 0; RowCnt <= frtDt.Rows.Count - 1; RowCnt++)
                {
                    DataRow ContRow = null;
                    Dr = DT.NewRow();
                    Dr["POL"] = polFk;
                    Dr["POD"] = podFk;
                    Dr["FrElmtFk"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"];
                    Dr["FrElmtID"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_ID"];
                    Dr["FrElmtName"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_NAME"];
                    Dr["Selected"] = 0;

                    //Commented By Rijesh
                    Dr["CurrencyFk"] = currencyFk;
                    Dr["CurrencyID"] = currencyID;
                    Int16 ColCnt = 8;
                    for (ColCnt = 8; ColCnt <= DT.Columns.Count - 3; ColCnt += 3)
                    {
                        double CurrRate = 0;
                        double ExchRate = 1;
                        foreach (DataRow ContRow_loopVariable in ActiveContractData.Rows)
                        {
                            ContRow = ContRow_loopVariable;
                            if (ContRow["FREIGHT_ELEMENT_MST_FK"] == frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"])
                            {
                                if (ContRow["CONTAINER_TYPE_MST_ID"] == DT.Columns[ColCnt + 1].Caption)
                                {
                                    CurrRate = Convert.ToDouble(getDefault(ContRow["FCL_APP_RATE"], 0));
                                    ExchRate = Convert.ToDouble(getDefault(ContRow["EXCHANGE_RATE"], 1));
                                    Dr["CurrencyFk"] = ContRow["CURRENCY_MST_FK"];
                                    Dr["CurrencyID"] = ContRow["CURRENCY_ID"];
                                    break; // TODO: might not be correct. Was : Exit For
                                }
                            }
                        }
                        Dr[ColCnt] = CurrRate;
                        Dr[ColCnt + 1] = CurrRate;
                        Dr[ColCnt + 2] = CurrRate;
                    }

                    DT.Rows.Add(Dr);
                }

                if (ActiveContractData.Rows.Count > 0)
                {
                    ContPK = ActiveContractData.Rows[0]["CONT_MAIN_SEA_PK"];
                    ContractNo = ActiveContractData.Rows[0]["CONTRACT_NO"];
                }
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

        #endregion

        #region " New Record Fright elements for LCL "
        private void MakeRowsForAllFreightElements(DataTable Mdt, DataTable DT, string polFk, string podFk, string currencyFk, string currencyID, string currencyName, string RFQdate, string CommPk, string CommId,
        // LCL New Record
        object ContPK, string ContractNo, string operator1, int CommodityGroupFk = 0)
        {
            DataTable frtDt = null;
            DataTable ActiveContractData = null;
            DataRow Dr = null;
            try
            {
                ActiveContractData = FetchActiveContractInformation(polFk, podFk, RFQdate, false, CommPk, CommId, Convert.ToString(CommodityGroupFk), "", operator1);
                string FrtPks = "-1";
                foreach (DataRow Dr_loopVariable in ActiveContractData.Rows)
                {
                    Dr = Dr_loopVariable;
                    FrtPks += "," + Dr["FREIGHT_ELEMENT_MST_FK"];
                }
                string strSQL = null;

                strSQL = "SELECT q.FREIGHT_ELEMENT_MST_PK,q.FREIGHT_ELEMENT_ID,q.FREIGHT_ELEMENT_NAME FROM(SELECT FREIGHT_ELEMENT_MST_PK, FREIGHT_ELEMENT_ID, FREIGHT_ELEMENT_NAME,PREFERENCE ";
                strSQL += " from FREIGHT_ELEMENT_MST_TBL where ACTIVE_FLAG = 1 ";
                strSQL += " AND BUSINESS_TYPE in (2,3)";
                strSQL += " AND BY_DEFAULT = 1 ";
                strSQL += " AND nvl(FREIGHT_TYPE,0) >= 0 ";
                strSQL += " AND FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)";
                if (FrtPks != "-1")
                {
                    strSQL += " AND FREIGHT_ELEMENT_MST_PK in (" + FrtPks + ")";
                }
                strSQL += " UNION ALL ";
                strSQL += " SELECT FREIGHT_ELEMENT_MST_PK, FREIGHT_ELEMENT_ID, FREIGHT_ELEMENT_NAME,PREFERENCE ";
                strSQL += " from FREIGHT_ELEMENT_MST_TBL where ACTIVE_FLAG = 1 ";
                strSQL += " AND BUSINESS_TYPE in (2,3)";
                strSQL += " AND BY_DEFAULT = 1 ";
                strSQL += " AND nvl(FREIGHT_TYPE,0) >= 0 ";
                strSQL += " AND FREIGHT_ELEMENT_MST_TBL.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)";
                if (FrtPks != "-1")
                {
                    strSQL += " AND FREIGHT_ELEMENT_MST_PK in (" + FrtPks + ")";
                }
                strSQL += " )q ORDER BY q.PREFERENCE ";
                frtDt = (new WorkFlow()).GetDataTable(strSQL);

                Int16 RowCnt = default(Int16);
                for (RowCnt = 0; RowCnt <= frtDt.Rows.Count - 1; RowCnt++)
                {
                    DataRow ContRow = null;
                    Dr = DT.NewRow();
                    Dr["FREIGHT_ELEMENT_MST_FK"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"];
                    Dr["FREIGHT_ELEMENT_ID"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_ID"];
                    Dr["FREIGHT_ELEMENT_NAME"] = frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_NAME"];
                    Dr["SELECTED"] = 0;
                    Dr["CURRENCY_MST_FK"] = currencyFk;
                    Dr["CURRENCY_ID"] = currencyID;
                    Dr["CURRENCY_NAME"] = currencyName;
                    Dr["LCL_CURRENT_MIN_RATE"] = 0;
                    //Added by rabbani reason USS Gap
                    Dr["LCL_CURRENT_RATE"] = 0;
                    Dr["LCL_REQUEST_MIN_RATE"] = 0;
                    //Added by rabbani reason USS Gap
                    Dr["LCL_REQUEST_RATE"] = 0;
                    Dr["LCL_APPROVED_MIN_RATE"] = 0;
                    //Added by rabbani reason USS Gap
                    Dr["LCL_APPROVED_RATE"] = 0;
                    Dr["PORT_MST_POL_FK"] = polFk;
                    Dr["PORT_MST_POD_FK"] = podFk;
                    double CurrRate = 0;
                    double ExchRate = 1;
                    double CurrMinRate = 0;
                    double ReqMinRate = 0;
                    object LCLBasis = DBNull.Value;
                    foreach (DataRow ContRow_loopVariable in ActiveContractData.Rows)
                    {
                        ContRow = ContRow_loopVariable;
                        if (ContRow["FREIGHT_ELEMENT_MST_FK"] == frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"])
                        {
                            CurrRate = Convert.ToDouble(getDefault(ContRow["LCL_APPROVED_RATE"], 0));
                            ExchRate = Convert.ToDouble(getDefault(ContRow["EXCHANGE_RATE"], 1));
                            LCLBasis = ContRow["LCL_BASIS"];
                            Dr["CURRENCY_MST_FK"] = ContRow["CURRENCY_MST_FK"];
                            Dr["CURRENCY_ID"] = ContRow["CURRENCY_ID"];
                            CurrMinRate = Convert.ToDouble(getDefault(ContRow["LCL_APPROVED_MIN_RATE"], 0));
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                    Dr["LCL_CURRENT_MIN_RATE"] = CurrMinRate;
                    Dr["LCL_CURRENT_RATE"] = CurrRate;
                    Dr["LCL_REQUEST_MIN_RATE"] = CurrMinRate;
                    Dr["LCL_REQUEST_RATE"] = CurrRate;
                    Dr["LCL_APPROVED_MIN_RATE"] = CurrMinRate;
                    Dr["LCL_APPROVED_RATE"] = CurrRate;
                    Dr["LCL_BASIS"] = LCLBasis;
                    DT.Rows.Add(Dr);
                }

                if (ActiveContractData.Rows.Count > 0)
                {
                    ContPK = ActiveContractData.Rows[0]["CONT_MAIN_SEA_PK"];
                    ContractNo = Convert.ToString(ActiveContractData.Rows[0]["CONTRACT_NO"]);
                    Mdt.Rows[0]["LCL_BASIS"] = ActiveContractData.Rows[0]["LCL_BASIS"];
                    Mdt.Rows[0]["DIMENTION_ID"] = ActiveContractData.Rows[0]["LCL_BASIS"];
                }
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
        #endregion

        #region " Get Base Currency "
        public static string GetBaseCurrency(string ID = "", string Name = "")
        {
            DataTable dt = null;
            try
            {
                dt = (new WorkFlow()).GetDataTable("Select CURRENCY_MST_FK, CURRENCY_ID, CURRENCY_NAME from CORPORATE_MST_TBL, CURRENCY_TYPE_MST_TBL where CURRENCY_MST_FK = CURRENCY_MST_PK ");
                ID = Convert.ToString(dt.Rows[0]["CURRENCY_ID"]);
                Name = Convert.ToString(dt.Rows[0]["CURRENCY_NAME"]);
                return Convert.ToString(dt.Rows[0]["CURRENCY_MST_FK"]);
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
        #endregion

        #region " Fetch Active Contract Information "

        public DataTable FetchActiveContractInformation(string POLFk = "-1", string PODFk = "-1", string RFQDate = "", bool iSFCL = true, string CommodityFk = "", string CommodityID = "", string CommodityGroupFk = "", string CommodityGroupID = "", string Operator1 = "")
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            DataTable dt = null;
            try
            {
                //============= Getting Commodity Group Pk Value
                if (string.IsNullOrEmpty(CommodityGroupFk) & string.IsNullOrEmpty(CommodityGroupID))
                {
                    if (!string.IsNullOrEmpty(CommodityFk))
                    {
                        dt = (new WorkFlow()).GetDataTable("Select COMMODITY_GROUP_FK from COMMODITY_MST_TBL where " + "COMMODITY_MST_PK = " + CommodityFk);
                    }
                    else if (!string.IsNullOrEmpty(CommodityID))
                    {
                        dt = (new WorkFlow()).GetDataTable("Select COMMODITY_GROUP_FK from COMMODITY_MST_TBL where " + "COMMODITY_ID = " + CommodityID);
                    }
                }
                else if (string.IsNullOrEmpty(CommodityGroupFk) & !string.IsNullOrEmpty(CommodityGroupID))
                {
                    dt = (new WorkFlow()).GetDataTable("Select COMMODITY_GROUP_PK from COMMODITY_GROUP_MST_TBL where " + "COMMODITY_GROUP_CODE = " + CommodityGroupID);
                }
                if ((dt != null))
                {
                    if (dt.Rows.Count > 0)
                    {
                        CommodityGroupFk = Convert.ToString(dt.Rows[0][0]);
                    }
                }

                buildQuery.Append("    Select ");
                buildQuery.Append("       main.CONT_MAIN_SEA_PK, ");
                // 0
                buildQuery.Append("       main.CONTRACT_NO, ");
                // 1
                buildQuery.Append("       to_Char(main.CONTRACT_DATE, '" + dateFormat + "') CONTRACT_DATE, ");
                // 2
                buildQuery.Append("       trn.PORT_MST_POL_FK, ");
                // 3
                buildQuery.Append("       trn.PORT_MST_POD_FK, ");
                // 4
                buildQuery.Append("       trn.FREIGHT_ELEMENT_MST_FK, ");
                // 5
                buildQuery.Append("       frt.FREIGHT_ELEMENT_ID, ");
                // 6
                buildQuery.Append("       trn.CURRENCY_MST_FK, ");
                // 7 'Added By Rijesh 20-Apr
                buildQuery.Append("       cur.CURRENCY_ID, ");
                // 7 'Added By Rijesh 20-Apr
                buildQuery.Append("       trn.LCL_BASIS, ");
                // 8
                buildQuery.Append("       round(trn.LCL_APPROVED_RATE,2) LCL_APPROVED_RATE, ");
                // 9
                buildQuery.Append("       CONT.CONTAINER_TYPE_MST_FK, ");
                // 10
                buildQuery.Append("       contmain.CONTAINER_TYPE_MST_ID, ");
                // 11
                buildQuery.Append("       NVL(CONT.FCL_APP_RATE, 0) FCL_APP_RATE, ");
                // 12
                buildQuery.Append("       NVL(EXC.EXCHANGE_RATE, 1) EXCHANGE_RATE, ");
                // 13
                buildQuery.Append("       round(trn.lcl_approved_min_rate,2) LCL_APPROVED_MIN_RATE");
                //14 'Added by rabbani reason USS Gap
                buildQuery.Append("      from  CONT_MAIN_SEA_TBL       main, ");
                buildQuery.Append("            FREIGHT_ELEMENT_MST_TBL frt, ");
                buildQuery.Append("            CONT_TRN_SEA_FCL_LCL    trn, ");
                buildQuery.Append("            CONTAINER_TYPE_MST_TBL  contmain, ");
                buildQuery.Append("            currency_type_mst_tbl      CUR, ");
                //'Added By Rijesh
                buildQuery.Append("            V_EXCHANGE_RATE         EXC, ");
                buildQuery.Append("            CONT_TRN_SEA_FCL_RATES CONT ");
                buildQuery.Append("      where main.CONT_APPROVED = 1 ");
                buildQuery.Append("            AND main.ACTIVE = 1 AND EXC.EXCH_RATE_TYPE_FK = 1 ");
                buildQuery.Append("            AND trn.CONT_TRN_SEA_PK = CONT.CONT_TRN_SEA_FK(+) ");
                //Snigdharani
                buildQuery.Append("            AND frt.FREIGHT_TYPE >= 0 ");
                buildQuery.Append("            AND CUR.CURRENCY_MST_PK = trn.Currency_Mst_Fk ");
                buildQuery.Append("       AND main.CONT_MAIN_SEA_PK = trn.CONT_MAIN_SEA_FK ");
                buildQuery.Append("       AND main.VALID_FROM <= TO_DATE('" + RFQDate + "','" + dateFormat + "') ");
                buildQuery.Append("       AND ( main.VALID_TO >= TO_DATE('" + RFQDate + "','" + dateFormat + "') ");
                buildQuery.Append("             OR main.VALID_TO IS NULL ");
                buildQuery.Append("           ) ");
                buildQuery.Append("       AND main.CONTRACT_DATE between ");
                buildQuery.Append("           EXC.FROM_DATE and NVL(EXC.TO_DATE, ROUND(SYSDATE-0.5)) ");
                if (!string.IsNullOrEmpty(Operator1))
                {
                    buildQuery.Append("   AND main.OPERATOR_MST_FK = " + Operator1);
                }
                if (!string.IsNullOrEmpty(CommodityGroupFk))
                {
                    buildQuery.Append("   AND COMMODITY_GROUP_FK = " + CommodityGroupFk);
                }
                buildQuery.Append("       AND main.CARGO_TYPE = " + (iSFCL ? "1" : "2"));
                // Cargo Type 
                buildQuery.Append("       AND trn.PORT_MST_POL_FK = " + POLFk);
                buildQuery.Append("       AND trn.PORT_MST_POD_FK = " + PODFk);
                // Freight Element condition
                buildQuery.Append("       AND trn.FREIGHT_ELEMENT_MST_FK = frt.FREIGHT_ELEMENT_MST_PK ");
                // Frght Cond.
                // Exchange Rate Condition
                buildQuery.Append("       AND trn.CURRENCY_MST_FK = EXC.CURRENCY_MST_FK ");
                //Added By Rijesh
                buildQuery.Append("       AND CONT.CONTAINER_TYPE_MST_FK = contmain.CONTAINER_TYPE_MST_PK(+) ");
                //Container Cond.
                //buildQuery.Append(vbCrLf & "       AND EXC.CURRENCY_MST_BASE_FK =" & HttpContext.Current.Session("currency_mst_pk")) 'adding by thiyagarajan on 11/10/08
                buildQuery.Append("      Order By frt.FREIGHT_ELEMENT_ID, contmain.PREFERENCES ");

                return (new WorkFlow()).GetDataTable(buildQuery.ToString());
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

        public object CommodityGroupFk(object CommodityFk)
        {
            if (CommodityFk == null)
                return 0;
            if (object.ReferenceEquals(CommodityFk, DBNull.Value))
                return 0;
            DataTable dt = null;
            try
            {
                dt = (new WorkFlow()).GetDataTable("Select COMMODITY_GROUP_FK from COMMODITY_MST_TBL where " + "COMMODITY_MST_PK = " + CommodityFk);
                return dt.Rows[0][0];
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }
        #endregion

        #region " Supporting Function "
        private new object ifDBNull(object col)
        {
            if (Convert.ToString(col).Trim().Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        private new object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }

        #endregion

        #region " Fetch Active Container Method "
        public new DataTable FetchActiveCont()
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();

            str = " SELECT CTMT.CONTAINER_TYPE_MST_ID " + " FROM CONTAINER_TYPE_MST_TBL CTMT WHERE CTMT.ACTIVE_FLAG = 1 " + " ORDER BY CTMT.PREFERENCES";
            try
            {
                return objWF.GetDataTable(str);
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

        #endregion

    }
}