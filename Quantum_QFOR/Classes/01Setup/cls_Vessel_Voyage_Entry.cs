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
using System.Text;

using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsVesselVoyageEntry : CommonFeatures
    {
        #region "Fetch All"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="shippingID">The shipping identifier.</param>
        /// <param name="VoyagePKS">The voyage PKS.</param>
        /// <param name="LinePKs">The line p ks.</param>
        /// <returns></returns>
        public DataSet fetchAll(string shippingID = "", string VoyagePKS = "", string LinePKs = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            string strCondition1 = null;
            string strCondition2 = null;
            Int32 TotalRecords = default(Int32);

            WorkFlow objWF = new WorkFlow();
            string strQuery = null;

            if (!string.IsNullOrEmpty(shippingID))
            {
                strCondition = "AND v.vessel_voyage_tbl_pk in (" + shippingID + ") ";
            }
            else
            {
                strCondition = "AND v.vessel_voyage_tbl_pk = '" + shippingID + "' ";
            }

            if (!string.IsNullOrEmpty(VoyagePKS))
            {
                if (!string.IsNullOrEmpty(VoyagePKS))
                {
                    strCondition1 = "AND vt.VOYAGE_TRN_PK in (" + VoyagePKS + ") ";
                }
                else
                {
                    strCondition1 = "AND vt.VOYAGE_TRN_PK = '" + VoyagePKS + "' ";
                }
            }

            if (!string.IsNullOrEmpty(LinePKs))
            {
                if (!string.IsNullOrEmpty(LinePKs))
                {
                    strCondition2 = "AND OP.OPERATOR_MST_PK in (" + LinePKs + ") ";
                }
                else
                {
                    strCondition2 = "AND OP.OPERATOR_MST_PK = '" + LinePKs + "' ";
                }
            }

            strSQL = "SELECT Count(*) from vessel_voyage_tbl V,VESSEL_VOYAGE_TRN VT,OPERATOR_MST_TBL OP";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
            strSQL = strSQL + "  and OP.OPERATOR_MST_PK = V.OPERATOR_MST_FK";
            strSQL = strSQL + "    AND V.VESSEL_VOYAGE_TBL_PK = VT.VESSEL_VOYAGE_TBL_FK";
            strSQL += strCondition;
            strSQL += strCondition1;
            strSQL += strCondition2;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            //'TotalPage = TotalRecords \ M_MasterPageSize
            //If TotalRecords Mod M_MasterPageSize <> 0 Then
            //    'TotalPage += 1
            //End If
            //If CurrentPage > TotalPage Then
            //    CurrentPage = 1
            //End If
            //If TotalRecords = 0 Then
            //    CurrentPage = 0
            //End If

            //last = CurrentPage * M_MasterPageSize
            //start = (CurrentPage - 1) * M_MasterPageSize + 1

            strSQL = "SELECT * from (SELECT rownum as SLNO ,A.* FROM (SELECT OP.ACTIVE_FLAG,";
            strSQL = strSQL + "  OP.OPERATOR_ID,";
            strSQL = strSQL + "  OP.OPERATOR_NAME,";
            strSQL = strSQL + "  V.VESSEL_ID,";
            strSQL = strSQL + "  V.VESSEL_NAME,";
            strSQL = strSQL + " VT.VOYAGE AS VOYAGE,";
            strSQL = strSQL + " POL.PORT_ID AS POL, ";
            strSQL = strSQL + " POD.PORT_ID AS POD, ";
            strSQL = strSQL + "  '' AS POC, ";
            strSQL = strSQL + "  to_Char(VT.POL_ETA, 'dd/MM/yyyy HH24:mi')ETA_POL, ";
            //strSQL = strSQL & vbCrLf & "       to_Char(VT.POL_ETD, '" & dateFormat & "')  ETD_POL, "
            strSQL = strSQL + "       to_Char(VT.POL_ETD, 'dd/MM/yyyy HH24:mi')ETD_POL, ";
            strSQL = strSQL + "      to_Char(VT.POL_CUT_OFF_DATE, 'dd/MM/yyyy HH24:mi')CUT_OFF_POL, ";
            strSQL = strSQL + "        to_Char(VT.POD_ETA, 'dd/MM/yyyy HH24:mi')ETA_POD, ";
            //ADD BY LATHA ON APRIL 2
            strSQL = strSQL + "        to_Char(VT.ATD_POL, 'dd/MM/yyyy HH24:mi')ATD_POL, ";
            strSQL = strSQL + "        to_Char(VT.ATA_POD, 'dd/MM/yyyy HH24:mi')ATA_POD, ";
            //END BY LATHA
            strSQL = strSQL + " '' AS EXCHRATE, ";
            strSQL = strSQL + "        V.VESSEL_VOYAGE_TBL_PK AS VOYAGEPK,";
            strSQL = strSQL + "        POL.PORT_MST_PK POLPK,";
            strSQL = strSQL + "        POD.PORT_MST_PK PODPK,";
            strSQL = strSQL + "        '' AS VAL,vt.version_no,";
            strSQL = strSQL + "        Vt.VOYAGE_TRN_PK AS VOYAGEtrnPK,";
            strSQL = strSQL + "     VT.CUSTOMS_CALL,";
            strSQL = strSQL + "     VT.CAPTAIN_NAME,";
            strSQL = strSQL + "     PCALL.PORT_ID AS PORTCALL,";
            strSQL = strSQL + "     VT.GRT,";
            strSQL = strSQL + "     VT.NRT,";
            strSQL = strSQL + "     VT.PORT_CALL_MST_FK,";
            strSQL = strSQL + "   CASE WHEN ((SELECT COUNT(*)  FROM BOOKING_MST_TBL BST WHERE BST.VESSEL_VOYAGE_FK = VT.VOYAGE_TRN_PK) +";
            strSQL = strSQL + "    (SELECT COUNT(*) FROM JOB_CARD_TRN JCT WHERE JCT.VOYAGE_TRN_FK =  VT.VOYAGE_TRN_PK)) > 0 THEN 1 ELSE 0 END EDITABLE";
            strSQL = strSQL + "     , '' SEL ";
            strSQL = strSQL + "     ,  OP.OPERATOR_MST_PK ";
            strSQL = strSQL + "   FROM VESSEL_VOYAGE_TBL    V,";
            strSQL = strSQL + "        VESSEL_VOYAGE_TRN    VT,";
            strSQL = strSQL + "        OPERATOR_MST_TBL     OP,";
            strSQL = strSQL + "        PORT_MST_TBL         POL,";
            strSQL = strSQL + "        PORT_MST_TBL POD,";
            strSQL = strSQL + "        PORT_MST_TBL PCALL";
            strSQL = strSQL + "  Where OP.OPERATOR_MST_PK = V.OPERATOR_MST_FK";
            strSQL = strSQL + "    AND V.VESSEL_VOYAGE_TBL_PK = VT.VESSEL_VOYAGE_TBL_FK";
            strSQL = strSQL + "    AND VT.PORT_MST_POL_FK = POL.PORT_MST_PK";
            strSQL = strSQL + "    AND VT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            strSQL = strSQL + "    AND VT.PORT_CALL_MST_FK = PCALL.PORT_MST_PK(+)";
            strSQL = strSQL + strCondition;
            strSQL = strSQL + strCondition1;
            strSQL = strSQL + strCondition2;
            strSQL = strSQL + "  ORDER BY VT.POL_ETA)A)";

            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "Fetch All"

        #region "FetchafterSave"

        /// <summary>
        /// Fetches the save.
        /// </summary>
        /// <param name="shipping">The shipping.</param>
        /// <param name="vesselname">The vesselname.</param>
        /// <returns></returns>
        public DataSet fetchSave(string shipping, string vesselname)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;

            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            strCondition = " AND op.operator_name ='" + shipping + "' and v.vessel_name= '" + vesselname + "'  ";
            strSQL = strSQL + " SELECT rownum as SLNO, OP.OPERATOR_NAME,";
            strSQL = strSQL + "  V.VESSEL_NAME,";
            strSQL = strSQL + " VT.VOYAGE AS VOYAGE,";
            strSQL = strSQL + " POL.PORT_ID AS POL, ";
            strSQL = strSQL + " POD.PORT_ID AS POD, ";
            strSQL = strSQL + "  '' AS POC, ";
            strSQL = strSQL + "  VT.POL_ETA AS ETA_POL, ";
            strSQL = strSQL + " VT.POL_ETD AS ETD_POL, ";
            strSQL = strSQL + " VT.POL_CUT_OFF_DATE AS CUT_OFF_POL, ";
            strSQL = strSQL + " VT.POD_ETA AS ETA_POD, ";
            //ADD BY LATHA ON APRIL 2
            strSQL = strSQL + " VT.ATD_POL AS ATD_POL, ";
            strSQL = strSQL + " VT.ATA_POD AS ATA_POD, ";
            //END BY LATHA
            strSQL = strSQL + " '' AS EXCHRATE, ";
            strSQL = strSQL + "        V.VESSEL_VOYAGE_TBL_PK AS VOYAGEPK,";
            strSQL = strSQL + "        POL.PORT_MST_PK,";
            strSQL = strSQL + "        POD.PORT_MST_PK,";
            strSQL = strSQL + "        '' AS VAL,vt.version_no";
            strSQL = strSQL + "   FROM VESSEL_VOYAGE_TBL    V,";
            strSQL = strSQL + "        VESSEL_VOYAGE_TRN    VT,";
            strSQL = strSQL + "        OPERATOR_MST_TBL     OP,";
            strSQL = strSQL + "        PORT_MST_TBL         POL,";
            strSQL = strSQL + "        PORT_MST_TBL POD";
            strSQL = strSQL + "  Where OP.OPERATOR_MST_PK = V.OPERATOR_MST_FK";
            strSQL = strSQL + "    AND V.VESSEL_VOYAGE_TBL_PK = VT.VESSEL_VOYAGE_TBL_FK";
            strSQL = strSQL + "    AND VT.PORT_MST_POL_FK = POL.PORT_MST_PK";
            strSQL = strSQL + "    AND VT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            strSQL = strSQL + strCondition;
            strSQL = strSQL + "  ORDER BY VT.POL_ETA";

            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "FetchafterSave"

        #region "Operator"

        //for getting the data to the operator drop down list
        /// <summary>
        /// Operator1s the specified opkey.
        /// </summary>
        /// <param name="OPKEY">The opkey.</param>
        /// <returns></returns>
        public DataSet Operator1(long OPKEY = 0)
        {
            // Function Operator() As DataSet
            string strSQL = null;
            string strconditon = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = strSQL + "select 0,' ',' ' from dual ";
            strSQL = strSQL + "union";
            strSQL = "select operator.operator_mst_pk, operator.operator_id,operator.operator_name";
            strSQL = strSQL + "from operator_mst_tbl operator ";
            if (OPKEY > 0)
            {
                strSQL = strSQL + "where operator.operator_mst_pk=" + OPKEY;
            }
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "Operator"

        #region " SaveVesselMaster"

        //'This function will be used to Insert/update the Vessel Master Details to "vessel_voyage_tbl"
        //' If inserted/Updated then the PK Value will be Returned
        //' If the Insrtion of Updation Failed, then 0 will be passed
        /// <summary>
        /// Saves the vessel master.
        /// </summary>
        /// <param name="dblVesselPK">The double vessel pk.</param>
        /// <param name="strVesselName">Name of the string vessel.</param>
        /// <param name="dblOperatorFK">The double operator fk.</param>
        /// <param name="ACT">The act.</param>
        /// <param name="DsMain">The ds main.</param>
        /// <param name="strVesselID">The string vessel identifier.</param>
        /// <param name="DeleteRecordsFrm_VESSEL_VOYAGE_TRN_TP">The delete records FRM_ vesse l_ voyag e_ tr n_ tp.</param>
        /// <param name="I">The i.</param>
        /// <returns></returns>
        public ArrayList SaveVesselMaster(long dblVesselPK, string strVesselName, long dblOperatorFK, Int16 ACT, DataSet DsMain, string strVesselID, short DeleteRecordsFrm_VESSEL_VOYAGE_TRN_TP = 0, int I = 0)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();

            int RESULT = 0;

            try
            {
                //Update the Details Table
                Int32 flagUpdate = 0;
                // Dim I, PKV As Int16
                long VslPK = 0;
                long VoyPK = 0;
                string VoyNo = null;
                long POLPK = 0;
                string PODPK = null;
                System.DateTime? POLETA = default(System.DateTime);
                System.DateTime? POLETD = default(System.DateTime);
                System.DateTime? POLCUT = default(System.DateTime);
                System.DateTime? PODETA = default(System.DateTime);
                // ADD BY LATHA ON APRIL 2
                System.DateTime? ATDPOL = default(System.DateTime);
                System.DateTime? ATAPOD = default(System.DateTime);
                string CustomsCall = null;
                string CaptainName = null;
                long PortCallpk = 0;
                decimal NRT = default(decimal);
                decimal GRT = default(decimal);
                //END BY LATHA

                Int64 VERNO = default(Int64);
                string VoyTrnFk = null;
                string VoyPks = "";
                OracleCommand VoyInsCommand = new OracleCommand();
                OracleCommand VoyUpdCommand = new OracleCommand();
                OracleCommand InsCommand = new OracleCommand();

                //   For I = 0 To DsMain.Tables(1).Rows.Count - 1
                //For PKV = 0 To DsMain.Tables(1).Rows.Count - 1
                // Next
                var _with1 = DsMain.Tables[1].Rows[I];
                var _with2 = InsCommand;
                _with2.Transaction = TRAN;
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TBL_INS";
                _with2.Parameters.Add("OPERATOR_MST_FK_IN", DsMain.Tables[1].Rows[I]["OPERATOR_PK"]).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("VESSEL_NAME_IN", DsMain.Tables[1].Rows[I]["VESSEL_NAME"]).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("VESSEL_ID_IN", DsMain.Tables[1].Rows[I]["VESSEL_ID"]).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("ACTIVE_IN", DsMain.Tables[1].Rows[I]["ACTIVE"]).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RESULT = _with2.ExecuteNonQuery();
                dblVesselPK = Convert.ToInt32(_with2.Parameters["RETURN_VALUE"].Value);
                _with2.Parameters.Clear();

                VslPK = dblVesselPK;

                VoyPK = Convert.ToInt64(_with1["VESSEL_VOYAGE_TBL_PK"]);
                VoyNo = Convert.ToString(_with1["VOYAGE"]);
                POLPK = Convert.ToInt64(_with1["PORT_MST_POL_FK"]);
                PODPK = Convert.ToString(getDefault(Convert.ToString(_with1["PORT_MST_POD_FK"]), ""));
                if (!string.IsNullOrEmpty(_with1["POL_ETA"].ToString()))
                {
                    POLETA = Convert.ToDateTime((_with1["POL_ETA"].ToString()));
                }
                else
                {
                    POLETA = DateTime.MinValue;
                }
                if (!string.IsNullOrEmpty(_with1["POL_ETD"].ToString()))
                {
                    POLETD = Convert.ToDateTime((_with1["POL_ETD"].ToString()));
                }
                else
                {
                    POLETD = DateTime.MinValue;
                }
                if (!string.IsNullOrEmpty(_with1["POL_CUT_OFF_DATE"].ToString()))
                {
                    POLCUT = Convert.ToDateTime((_with1["POL_CUT_OFF_DATE"].ToString()));
                }
                else
                {
                    POLCUT = DateTime.MinValue;
                }
                if (!string.IsNullOrEmpty(_with1["POD_ETA"].ToString()))
                {
                    PODETA = Convert.ToDateTime((_with1["POD_ETA"].ToString()));
                }
                else
                {
                    PODETA = DateTime.MinValue;
                }

                if (!string.IsNullOrEmpty(_with1["ATD_POL"].ToString()))
                {
                    ATDPOL = Convert.ToDateTime((_with1["ATD_POL"].ToString()));
                }
                else
                {
                    ATDPOL = DateTime.MinValue;
                }
                if (!string.IsNullOrEmpty(_with1["ATA_POD"].ToString()))
                {
                    ATAPOD = Convert.ToDateTime((_with1["ATA_POD"].ToString()));
                }
                else
                {
                    ATAPOD = DateTime.MinValue;
                }

                VERNO = Convert.ToInt32(!string.IsNullOrEmpty(_with1["VERSION_NO"].ToString()) ? 0 : _with1["VERSION_NO"]);

                if (VoyPK == 0)
                {
                    flagUpdate = 1;
                    var _with3 = VoyInsCommand;
                    _with3.Transaction = TRAN;
                    _with3.Connection = objWK.MyConnection;
                    _with3.CommandType = CommandType.StoredProcedure;
                    _with3.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_INS";
                    _with3.Parameters.Add("VESSEL_VOYAGE_TBL_FK_IN", VslPK).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("VOYAGE_IN", VoyNo).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("PORT_MST_POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("PORT_MST_POD_FK_IN", getDefault(PODPK, "")).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("POL_ETA_IN", getDefault((POLETA == DateTime.MinValue ? null : POLETA), null)).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("POL_ETD_IN", getDefault((POLETD != DateTime.MinValue ? null : POLETD), "")).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("POL_CUT_OFF_DATE_IN", getDefault((POLCUT == DateTime.MinValue ? null : POLCUT), "")).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("POD_ETA_IN", getDefault((PODETA == DateTime.MinValue ? null : PODETA), "")).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("ATD_POL_IN", getDefault((ATDPOL == DateTime.MinValue ? null : ATDPOL), "")).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("ATA_POD_IN", getDefault((ATAPOD == DateTime.MinValue ? null : ATAPOD), "")).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("CUSTOMS_CALL_IN", DsMain.Tables[1].Rows[I]["CUSTOMS_CALL"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("CAPTAIN_NAME_IN", DsMain.Tables[1].Rows[I]["CAPTAIN_NAME"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("PORT_CALL_MST_FK_IN", DsMain.Tables[1].Rows[I]["PORT_CALL_MST_FK"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("NRT_IN", DsMain.Tables[1].Rows[I]["NRT"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("GRT_IN", DsMain.Tables[1].Rows[I]["GRT"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 4, "VOYAGE_TRN_PK").Direction = ParameterDirection.Output;
                    RESULT = _with3.ExecuteNonQuery();
                    VoyPK = Convert.ToInt64(_with3.Parameters["RETURN_VALUE"].Value);
                    _with3.Parameters.Clear();
                }
                else
                {
                    var _with4 = VoyUpdCommand;
                    _with4.Transaction = TRAN;
                    _with4.Connection = objWK.MyConnection;
                    _with4.CommandType = CommandType.StoredProcedure;
                    _with4.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_UPD";
                    _with4.Parameters.Add("VOYAGE_TRN_PK_IN", VoyPK).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("VOYAGE_IN", VoyNo).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("PORT_MST_POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("PORT_MST_POD_FK_IN", getDefault(PODPK, "")).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("POL_ETA_IN", getDefault((POLETA == DateTime.MinValue ? null : POLETA), null)).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("POL_ETD_IN", getDefault((POLETD != DateTime.MinValue ? null : POLETD), "")).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("POL_CUT_OFF_DATE_IN", getDefault((POLCUT == DateTime.MinValue ? null : POLCUT), "")).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("POD_ETA_IN", getDefault((PODETA == DateTime.MinValue ? null : PODETA), "")).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("ATD_POL_IN", getDefault((ATDPOL == DateTime.MinValue ? null : ATDPOL), "")).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("ATA_POD_IN", getDefault((ATAPOD == DateTime.MinValue ? null : ATAPOD), "")).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("CUSTOMS_CALL_IN", DsMain.Tables[1].Rows[I]["CUSTOMS_CALL"]).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("CAPTAIN_NAME_IN", DsMain.Tables[1].Rows[I]["CAPTAIN_NAME"]).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("PORT_CALL_MST_FK_IN", DsMain.Tables[1].Rows[I]["PORT_CALL_MST_FK"]).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("NRT_IN", DsMain.Tables[1].Rows[I]["NRT"]).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("GRT_IN", DsMain.Tables[1].Rows[I]["GRT"]).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("VERSION_NO_IN", VERNO).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    RESULT = _with4.ExecuteNonQuery();
                    VoyTrnFk = _with4.Parameters["RETURN_VALUE"].Value.ToString();
                    _with4.Parameters.Clear();
                }

                if ((DsMain.Tables[1].Rows[I]["VESSEL_VOYAGE_TBL_FK"] != null) | !string.IsNullOrEmpty(DsMain.Tables[1].Rows[I]["VESSEL_VOYAGE_TBL_FK"].ToString()))
                {
                    DsMain.Tables[1].Rows[I]["VESSEL_VOYAGE_TBL_FK"] = VslPK;
                }

                if ((DsMain.Tables[1].Rows[I]["VESSEL_VOYAGE_TBL_PK"] != null) | !string.IsNullOrEmpty(DsMain.Tables[1].Rows[I]["VESSEL_VOYAGE_TBL_PK"].ToString()))
                {
                    if (flagUpdate == 1)
                    {
                        DsMain.Tables[1].Rows[I]["VESSEL_VOYAGE_TBL_PK"] = VoyPK;
                    }
                    else
                    {
                        DsMain.Tables[1].Rows[I]["VESSEL_VOYAGE_TBL_PK"] = VoyTrnFk;
                    }
                }

                Int16 J = default(Int16);
                long VoyTpPK = 0;

                long PORTPK = 0;
                Int64 TransistDays = default(Int64);
                System.DateTime PC_POL_ETA = default(System.DateTime);
                System.DateTime PC_POL_ETD = default(System.DateTime);
                System.DateTime PC_POL_CUT = default(System.DateTime);
                OracleCommand VoyTPInsCommand = new OracleCommand();
                OracleCommand VoyTPUpdCommand = new OracleCommand();
                string strsql = null;

                if (DeleteRecordsFrm_VESSEL_VOYAGE_TRN_TP == 1)
                {
                    //if user edits records in POC then DeleteRecordsFrm_VESSEL_VOYAGE_TRN_TP will be 1
                    //else DeleteRecordsFrm_VESSEL_VOYAGE_TRN_TP will be 0
                    //As we are using only Insert proc we have to delete existing records first,
                    //before inserting
                    strsql = "delete from vessel_voyage_trn_tp tp where tp.voyage_trn_fk=" + VoyPK;
                    var _with5 = objWK.MyCommand;
                    _with5.CommandText = strsql;
                    _with5.CommandType = CommandType.Text;
                    _with5.Transaction = TRAN;
                    objWK.MyCommand.ExecuteNonQuery();
                }
                //add by latha
                //If DsMain.Tables(2).Rows.Count = 0 Then
                //    VoyTrnFk = IIf(IsDBNull(.Item("VOYAGE_TRN_FK")), VoyPK, .Item("VOYAGE_TRN_FK"))
                //    PORTPK =
                //    TransistDays = .Item("TRANSIT_DAYS")
                //    PC_POL_ETA = Nothing
                //    PC_POL_ETD = Nothing
                //    PC_POL_CUT = Nothing
                //    With VoyTPInsCommand
                //        .Transaction = TRAN
                //        .Connection = objWK.MyConnection
                //        .CommandType = CommandType.StoredProcedure
                //        .CommandText = objWK.MyUserName & ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_TP_INS"
                //        .Parameters.Add("VOYAGE_TRN_FK_IN", VoyTrnFk).Direction = ParameterDirection.Input
                //        .Parameters.Add("PORT_MST_TP_PORT_FK_IN", PORTPK).Direction = ParameterDirection.Input
                //        .Parameters.Add("POL_ETA_IN", PC_POL_ETA).Direction = ParameterDirection.Input
                //        .Parameters.Add("POL_ETD_IN", PC_POL_ETD).Direction = ParameterDirection.Input
                //        .Parameters.Add("POL_CUT_OFF_DATE_IN", PC_POL_CUT).Direction = ParameterDirection.Input
                //        .Parameters.Add("TRANSIT_DAYS_IN", TransistDays).Direction = ParameterDirection.Input
                //        .Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Int32, 50, "VOYAGE_TRN_TP_PK").Direction = ParameterDirection.Output
                //        RESULT = .ExecuteNonQuery()
                //        .Parameters.Clear()
                //    End With
                //End If
                for (J = 0; J <= DsMain.Tables[2].Rows.Count - 1; J++)
                {
                    var _with6 = DsMain.Tables[2].Rows[J];
                    VoyTrnFk = Convert.ToString(!string.IsNullOrEmpty(_with6["VOYAGE_TRN_FK"].ToString()) ? VoyPK : _with6["VOYAGE_TRN_FK"]);
                    PORTPK = Convert.ToInt64(_with6["PORT_MST_TP_PORT_FK"]);
                    TransistDays = Convert.ToInt64(_with6["TRANSIT_DAYS"]);
                    if (_with6["POL_ETA_PC"] != null)
                    { DateTime eta = Convert.ToDateTime(_with6["POL_ETA_PC"]); PC_POL_ETA = Convert.ToDateTime(eta.ToString("{0:dd/MM/yyyy HH:mm}")); }
                    if (_with6["POL_ETD_PC"] != null)
                    { DateTime etd = Convert.ToDateTime(_with6["POL_ETD_PC"]); PC_POL_ETD = Convert.ToDateTime(etd.ToString("{0:dd/MM/yyyy HH:mm}")); }
                    if (_with6["POL_CUT_OFF_DATE_PC"] != null)
                    { DateTime cutOff = Convert.ToDateTime(_with6["POL_CUT_OFF_DATE_PC"]); PC_POL_CUT = Convert.ToDateTime(cutOff.ToString("{0:dd/MM/yyyy HH:mm}")); }
                    //ADD BY LATHA ON APRIL 2
                    // PC_ATD_POL = System.String.Format("{0:dd/MM/yyyy HH:mm}", CDate(.Item("ATD_POL_PC")))
                    //PC_ATA_POD = System.String.Format("{0:dd/MM/yyyy HH:mm}", CDate(.Item("ATA_POD_PC")))
                    //END BY LATHA

                    var _with7 = VoyTPInsCommand;
                    _with7.Transaction = TRAN;
                    _with7.Connection = objWK.MyConnection;
                    _with7.CommandType = CommandType.StoredProcedure;
                    _with7.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_TP_INS";
                    _with7.Parameters.Add("VOYAGE_TRN_FK_IN", VoyTrnFk).Direction = ParameterDirection.Input;
                    _with7.Parameters.Add("PORT_MST_TP_PORT_FK_IN", PORTPK).Direction = ParameterDirection.Input;
                    _with7.Parameters.Add("TRANSIT_DAYS_IN", TransistDays).Direction = ParameterDirection.Input;
                    _with7.Parameters.Add("POL_ETA_IN", PC_POL_ETA).Direction = ParameterDirection.Input;
                    _with7.Parameters.Add("POL_ETD_IN", PC_POL_ETD).Direction = ParameterDirection.Input;
                    _with7.Parameters.Add("POL_CUT_OFF_DATE_IN", PC_POL_CUT).Direction = ParameterDirection.Input;
                    //ADD BY LATHA ON APRIL 2
                    //.Parameters.Add("ATD_POL_IN", PC_ATD_POL).Direction = ParameterDirection.Input
                    //.Parameters.Add("ATA_POD_IN", PC_ATA_POD).Direction = ParameterDirection.Input
                    //END BY LATHA

                    _with7.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 50, "VOYAGE_TRN_TP_PK").Direction = ParameterDirection.Output;
                    RESULT = _with7.ExecuteNonQuery();
                    _with7.Parameters.Clear();
                }
                // Next

                //update the transaction_tp table

                if (RESULT > 0)
                {
                    TRAN.Commit();
                    //Push to financial system if realtime is selected
                    Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
                    ArrayList schDtls = null;
                    bool errGen = false;
                    if (objSch.GetSchedulerPushType() == true)
                    {
                        //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                        //try {
                        //	schDtls = objSch.FetchSchDtls();
                        //	//'Used to Fetch the Sch Dtls
                        //	if (schDtls[0].ToString().ToUpper() == "BOTH" | schDtls[0].ToString().ToUpper() == "VSL") {
                        //		objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), Convert.ToString(dblVesselPK));
                        //		if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                        //			objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                        //		}
                        //	}
                        //} catch (Exception ex) {
                        //	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                        //		objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                        //	}
                        //}
                    }
                    arrMessage.Add("saved");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //Manjunath  PTS ID:Sep-02  15/09/2011
            }
            catch (OracleException OraExp)
            {
                //Throw OraExp
                arrMessage.Add(OraExp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                //Throw ex
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion " SaveVesselMaster"

        #region "Update active"

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
            strSQL = "UPDATE vessel_voyage_tbl t " + "SET T.ACTIVE = 0," + "T.LAST_MODIFIED_BY_FK =" + M_CREATED_BY_FK + "," + "T.LAST_MODIFIED_DT = SYSDATE," + "T.VERSION_NO = T.VERSION_NO + 1" + "WHERE t.vessel_voyage_tbl_pk =" + ContractPk;
            objWK.MyCommand.CommandType = CommandType.Text;
            objWK.MyCommand.CommandText = strSQL;
            try
            {
                objWK.MyCommand.ExecuteNonQuery();
                arrMessage.Add("All data saved successfully");
                TRAN.Commit();
                return arrMessage;
                //Manjunath  PTS ID:Sep-02  15/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        /// <summary>
        /// Activates the specified contract pk.
        /// </summary>
        /// <param name="ContractPk">The contract pk.</param>
        /// <returns></returns>
        public ArrayList Activate(long ContractPk)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;

            strSQL = "UPDATE vessel_voyage_tbl t " + "SET T.ACTIVE = 1," + "T.LAST_MODIFIED_BY_FK =" + M_CREATED_BY_FK + "," + "T.LAST_MODIFIED_DT = SYSDATE," + "T.VERSION_NO = T.VERSION_NO + 1" + "WHERE t.vessel_voyage_tbl_pk =" + ContractPk;
            objWK.MyCommand.CommandType = CommandType.Text;
            objWK.MyCommand.CommandText = strSQL;
            try
            {
                objWK.MyCommand.ExecuteNonQuery();
                arrMessage.Add("All data saved successfully");
                TRAN.Commit();
                return arrMessage;
                //Manjunath  PTS ID:Sep-02  15/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        #endregion "Update active"

        #region "Vessel Trn_Tp"

        /// <summary>
        /// Saves the trans tp.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="lngVoyagePk">The LNG voyage pk.</param>
        /// <param name="Tran">The tran.</param>
        /// <returns></returns>
        public Int32 SaveTransTp(DataSet dsMain, long lngVoyagePk, OracleTransaction Tran)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            objWK.MyConnection = Tran.Connection;
            Int32 RecAfct = default(Int32);
            Int32 i = default(Int32);
            Int32 j = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            DataRow[] dRow = null;
            try
            {
                var _with8 = insCommand;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_TP_INS";
                var _with9 = _with8.Parameters;
                insCommand.Parameters.Add("VOYAGE_TRN_FK_IN", lngVoyagePk).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("PORT_MST_TP_PORT_FK_IN ", OracleDbType.Int32, 10, "PORT_MST_TP_PORT_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PORT_MST_TP_PORT_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("TRANSIT_DAYS_IN ", OracleDbType.Int32, 2, "TRANSIT_DAYS").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANSIT_DAYS_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 4, "VOYAGE_TRN_TP_PK").Direction = ParameterDirection.Output;
                var _with10 = updCommand;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_TP_UPD";
                updCommand.Parameters.Add("VOYAGE_TRN_FK_IN", OracleDbType.Int32, 10, "VOYAGE_TRN_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("PORT_MST_TP_PORT_FK_IN", OracleDbType.Varchar2, 10, "PORT_MST_TP_PORT_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PORT_MST_TP_PORT_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("TRANSIT_DAYS_IN", OracleDbType.Int32, 2, "TRANSIT_DAYS").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSIT_DAYS_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 4, "VOYAGE_TRN_TP_PK").Direction = ParameterDirection.Output;
                var _with11 = objWK.MyDataAdapter;
                _with11.InsertCommand = insCommand;
                _with11.InsertCommand.Transaction = Tran;
                _with11.UpdateCommand = updCommand;
                _with11.UpdateCommand.Transaction = Tran;
                RecAfct = _with11.Update(dsMain.Tables["tblTransaction"]);
                if (RecAfct > 0)
                {
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            catch (DBConcurrencyException exp)
            {
                throw exp;
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

        #endregion "Vessel Trn_Tp"

        #region "VessselName Duplicate"

        /// <summary>
        /// Vessels the namel dup.
        /// </summary>
        /// <param name="vessel">The vessel.</param>
        /// <returns></returns>
        public bool VesselNamelDup(string vessel)
        {
            string strSQL = null;
            string strconditon = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "select tt.vessel_voyage_tbl_pk,tt.vessel_name from vessel_voyage_tbl tt  where tt.vessel_name = '" + vessel + "'  ";
            try
            {
                string intRowAffected = objWF.ExecuteScaler(strSQL);
                if (!string.IsNullOrEmpty(intRowAffected))
                {
                    return true;
                }
                else
                {
                    return false;
                }
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

        #endregion "VessselName Duplicate"

        #region "VessselID Duplicate"

        /// <summary>
        /// Vessels the identifier dup.
        /// </summary>
        /// <param name="VslID">The VSL identifier.</param>
        /// <returns></returns>
        public bool VesselIDDup(string VslID)
        {
            string strSQL = null;
            string strconditon = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "select v.vessel_voyage_tbl_pk,v.vessel_id from vessel_voyage_tbl v  where v.vessel_id = '" + VslID + "'  ";
            try
            {
                string intRowAffected = objWF.ExecuteScaler(strSQL);
                if (!string.IsNullOrEmpty(intRowAffected))
                {
                    return true;
                }
                else
                {
                    return false;
                }
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

        #endregion "VessselID Duplicate"

        #region "VessselVoyage Duplicate"

        /// <summary>
        /// Vessels the voyage dup.
        /// </summary>
        /// <param name="VslID">The VSL identifier.</param>
        /// <param name="VoyName">Name of the voy.</param>
        /// <returns></returns>
        public bool VesselVoyageDup(string VslID, string VoyName)
        {
            string strSQL = null;
            string strconditon = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "select tt.vessel_voyage_tbl_pk, tt.vessel_name";
            strSQL = strSQL + " from vessel_voyage_tbl tt,";
            strSQL = strSQL + " vessel_voyage_trn vv";
            strSQL = strSQL + " where vv.vessel_voyage_tbl_fk = tt.vessel_voyage_tbl_pk";
            strSQL = strSQL + " and tt.vessel_name = '" + VslID + "'";
            strSQL = strSQL + " and vv.voyage='" + VoyName + "'";
            try
            {
                string intRowAffected = objWF.ExecuteScaler(strSQL);
                if (!string.IsNullOrEmpty(intRowAffected))
                {
                    return true;
                }
                else
                {
                    return false;
                }
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

        #endregion "VessselVoyage Duplicate"

        #region "Fetch VesselVoyage Popup Function "

        // By Jitendra QFOR
        // Added for Location Popup Used for multi selection
        // this is using by location transaction
        /// <summary>
        /// Fetches the name of the vessel voyage.
        /// </summary>
        /// <param name="frmdt">The FRMDT.</param>
        /// <param name="toDt">To dt.</param>
        /// <param name="VesselVoyagePk">The vessel voyage pk.</param>
        /// <param name="VesselVoyageID">The vessel voyage identifier.</param>
        /// <param name="VesselVoyageName">Name of the vessel voyage.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="fctrypk">The fctrypk.</param>
        /// <param name="tctrypk">The tctrypk.</param>
        /// <param name="flocpk">The flocpk.</param>
        /// <param name="tlocpk">The tlocpk.</param>
        /// <returns></returns>
        public DataSet FetchVesselVoyageName(string frmdt, string toDt, string VesselVoyagePk = "", string VesselVoyageID = "", string VesselVoyageName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ", string fctrypk = "0",
        string tctrypk = "0", string flocpk = "0", string tlocpk = "0")
        {
            //, Optional ByVal frmdt As Date =  _
            //) As DataSet

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = "";
            string strSQL1 = "";
            string strSQL2 = "";
            string strSQL3 = "";
            string strCondition = "";
            string strCondition1 = "";
            string strCondition2 = "";
            string strCondition3 = "";
            //Manoharan 27Mar07:
            string FstrPKs = "";
            string TstrPKs = "";
            Int32 TotalRecords = default(Int32);
            bool @from = false;
            bool toCL = false;
            WorkFlow objWF = new WorkFlow();

            if (VesselVoyageID == "Country ID")
            {
                if (VesselVoyageName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(vessel_id) LIKE '%" + VesselVoyageName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (VesselVoyageName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(vessel_name) LIKE '%" + VesselVoyageName.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (VesselVoyagePk != "0")
            {
                if (VesselVoyagePk.Length > 0)
                {
                    strCondition1 += "and vessel_voyage_tbl_pk not in (" + VesselVoyagePk + " )";
                    strCondition2 += "and vessel_voyage_tbl_pk in (" + VesselVoyagePk + " )";
                }
            }

            strCondition += " AND a.active = 1 ";
            strSQL1 = "SELECT Count(*) from (";
            //strSQL = strSQL & vbCrLf & " WHERE 1=1 "
            //strSQL &= vbCrLf & strCondition & strCondition1
            //If strCondition2 <> "" Then
            //    strSQL &= vbCrLf & strCondition2
            //End If
            //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
            //TotalPage = TotalRecords \ M_MasterPageSize
            //If TotalRecords Mod M_MasterPageSize <> 0 Then
            //    TotalPage += 1
            //End If
            //If CurrentPage > TotalPage Then
            //    CurrentPage = 1
            //End If
            //If TotalRecords = 0 Then
            //    CurrentPage = 0
            //End If
            //last = CurrentPage * M_MasterPageSize
            //start = (CurrentPage - 1) * M_MasterPageSize + 1

            strSQL2 = " select * from (";
            strSQL = " SELECT ROWNUM SR_NO,q.* FROM (SELECT * FROM (";
            //If chkall = "0" Then    'Manoharan 08Mar2007:
            //If strCondition2 <> "" Then
            //    strSQL &= vbCrLf & " SELECT "
            //    strSQL &= vbCrLf & "vessel_voyage_tbl_pk, "
            //    strSQL &= vbCrLf & "vessel_id, "
            //    strSQL &= vbCrLf & "Initcap(vessel_name)  vessel_name, "
            //    strSQL &= vbCrLf & " 'true' active"
            //    strSQL &= vbCrLf & "FROM vessel_voyage_tbl a "
            //    strSQL &= vbCrLf & "WHERE  1=1"
            //    strSQL &= vbCrLf & strCondition2
            //    strSQL &= vbCrLf & strCondition
            //    strSQL &= vbCrLf & " union "
            //End If
            //strSQL &= vbCrLf & " SELECT "
            //strSQL &= vbCrLf & "vessel_voyage_tbl_pk, "
            //strSQL &= vbCrLf & "vessel_id, "
            //strSQL &= vbCrLf & "Initcap(vessel_name) vessel_name, "
            //strSQL &= vbCrLf & " 'false' active"
            //strSQL &= vbCrLf & "FROM  vessel_voyage_tbl vvt "
            //strSQL &= vbCrLf & "WHERE  1=1"
            //strSQL &= vbCrLf & strCondition1
            //strSQL &= vbCrLf & strCondition

            //Manoharan 27Mar07:
            if (flocpk != "0")
            {
                //strCondition3 &= ""
                FstrPKs = flocpk;
            }
            else if (fctrypk != "0")
            {
                //strCondition3 &= ""
                FstrPKs = "select l.location_mst_pk from location_mst_tbl l where l.active_flag = 1 and l.country_mst_fk in (" + fctrypk + ")";
            }
            else
            {
                FstrPKs = "select l.location_mst_pk from location_mst_tbl l where l.active_flag = 1 ";
            }

            if (tlocpk != "0")
            {
                //strCondition3 &= ""
                TstrPKs = tlocpk;
            }
            else if (tctrypk != "0")
            {
                //strCondition3 &= ""
                TstrPKs = " select l.location_mst_pk from location_mst_tbl l where l.active_flag = 1 and l.country_mst_fk in (" + tctrypk + ")";
            }
            else
            {
                TstrPKs = " select l.location_mst_pk from location_mst_tbl l where l.active_flag = 1 ";
            }

            if ((frmdt != null))
            {
                if ((toDt != null))
                {
                    strCondition3 += " and ( b.atd_pol <= to_date('" + toDt + "','" + dateFormat + "')";
                }
                strCondition3 += " and ( b.atd_pol >= to_date('" + frmdt + "','" + dateFormat + "') or b.atd_pol is null)";
                //frmdt = Format(frmdt, dateFormat)
            }
            if (!string.IsNullOrEmpty(strCondition2))
            {
                strSQL += " SELECT ";
                strSQL += "vessel_voyage_tbl_pk, ";
                strSQL += "vessel_id, ";
                strSQL += "vessel_name  vessel_name, ";
                strSQL += " 'true' active";
                strSQL += "FROM vessel_voyage_tbl a ";
                strSQL += "WHERE  1=1";
                strSQL += strCondition2;
                strSQL += strCondition;
                strSQL += " union ";
            }
            if (!string.IsNullOrEmpty(FstrPKs))
            {
                strSQL += " Select distinct a.vessel_voyage_tbl_pk, a.vessel_id, a.vessel_name, 'false' active ";
                strSQL += " from vessel_voyage_tbl a, location_mst_tbl x, location_working_ports_trn y ";
                strSQL += " where (a.vessel_voyage_tbl_pk in (select b.vessel_voyage_tbl_fk ";
                strSQL += " from vessel_voyage_trn b, vessel_voyage_trn_tp c where b.voyage_trn_pk = c.voyage_trn_fk ";
                strSQL += " and c.port_mst_tp_port_fk(+) = y.port_mst_fk " + strCondition3 + ") ";
                strSQL += " or a.vessel_voyage_tbl_pk in (select g.vessel_voyage_tbl_fk from vessel_voyage_trn g ";
                strSQL += " where g.port_mst_pol_fk(+) = y.port_mst_fk)) and x.location_mst_pk = y.location_mst_fk and y.location_mst_fk in (" + FstrPKs + ") ";
                strSQL += strCondition1;
                strSQL += strCondition;
                //strSQL &= vbCrLf & strCondition3
                @from = true;
            }
            if (!string.IsNullOrEmpty(TstrPKs))
            {
                if (@from)
                    strSQL += " union ";
                strSQL += " Select distinct a.vessel_voyage_tbl_pk, a.vessel_id, a.vessel_name, 'false' active ";
                strSQL += " from vessel_voyage_tbl a, location_mst_tbl x, location_working_ports_trn y ";
                strSQL += " where (a.vessel_voyage_tbl_pk in (select b.vessel_voyage_tbl_fk ";
                strSQL += " from vessel_voyage_trn b, vessel_voyage_trn_tp k where b.voyage_trn_pk = k.voyage_trn_fk ";
                strSQL += " and k.port_mst_tp_port_fk(+) = y.port_mst_fk" + strCondition3 + ") ";
                strSQL += " or a.vessel_voyage_tbl_pk in (select n.vessel_voyage_tbl_fk from vessel_voyage_trn n ";
                strSQL += " where n.port_mst_pod_fk(+) = y.port_mst_fk)) and x.location_mst_pk = y.location_mst_fk and y.location_mst_fk in (" + TstrPKs + ") ";
                strSQL += strCondition1;
                strSQL += strCondition;
                //strSQL &= vbCrLf & strCondition3
                toCL = true;
            }

            //ElseIf chkall = "1" Then    'Manoharan 08Mar2007: to select all countries in PopUp: from FlexiReport
            //strSQL &= vbCrLf & " SELECT COUNTRY_MST_PK, COUNTRY_ID, Initcap(COUNTRY_NAME) COUNTRY_NAME, "
            //strSQL &= vbCrLf & " 'true' active FROM COUNTRY_MST_TBL CN WHERE CN.ACTIVE_FLAG = 1 "
            //End If
            //sorting definition
            strSQL += " order by vessel_id ) ORDER BY  ACTIVE DESC ,vessel_id)Q) ";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1 + strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
            {
                CurrentPage = 1;
            }
            if (TotalRecords == 0)
            {
                CurrentPage = 0;
            }
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            strSQL3 = " WHERE SR_NO  Between " + start + " and " + last;

            try
            {
                return objWF.GetDataSet(strSQL2 + strSQL + strSQL3);
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

        #endregion "Fetch VesselVoyage Popup Function "

        #region "DeleteVesselMaster"

        /// <summary>
        /// Deletes the vessel master.
        /// </summary>
        /// <param name="VESSEL_VOYAGE_TBL_PK">The vesse l_ voyag e_ tb l_ pk.</param>
        /// <param name="VOYAGE_TRN_PK">The voyag e_ tr n_ pk.</param>
        /// <returns></returns>
        public ArrayList DeleteVesselMaster(long VESSEL_VOYAGE_TBL_PK, string VOYAGE_TRN_PK)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            int RESULT = 0;
            string return_Value = null;
            try
            {
                OracleCommand DelCommand = new OracleCommand();
                Int16 VER = default(Int16);
                var _with12 = DelCommand;
                _with12.Connection = objWK.MyConnection;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TBL_TRN_DEL";
                _with12.Parameters.Add("VESSEL_VOYAGE_TBL_PK_IN", VESSEL_VOYAGE_TBL_PK).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("VOYAGE_TRN_PK_IN", VOYAGE_TRN_PK).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RESULT = _with12.ExecuteNonQuery();
                return_Value = _with12.Parameters["RETURN_VALUE"].Value.ToString();
                _with12.Parameters.Clear();
                arrMessage.Add(return_Value);
                return arrMessage;
            }
            catch (OracleException OraExp)
            {
                //Throw OraExp
                arrMessage.Add(OraExp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                //Throw ex
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "DeleteVesselMaster"

        #region "Fetch Operator ID"

        /// <summary>
        /// Fetches the operator identifier.
        /// </summary>
        /// <param name="OperID">The oper identifier.</param>
        /// <returns></returns>
        public DataSet fetchOperatorID(string OperID)
        {
            WorkFlow objWF = new WorkFlow();
            string strQuery = null;
            string strSQL = null;
            strSQL = "SELECT OP.ACTIVE_FLAG,";
            strSQL = strSQL + "  OP.OPERATOR_ID,";
            strSQL = strSQL + "  OP.OPERATOR_NAME,OP.OPERATOR_MST_PK";
            strSQL = strSQL + "        FROM OPERATOR_MST_TBL     OP";
            strSQL = strSQL + "  Where OP.OPERATOR_ID = '" + OperID + "' ";
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "Fetch Operator ID"

        #region "GettingPOLANDPODPk"

        /// <summary>
        /// Gettings the port pk.
        /// </summary>
        /// <param name="PORID">The porid.</param>
        /// <returns></returns>
        public long GettingPortPk(string PORID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT PMT.PORT_MST_PK FROM PORT_MST_TBL PMT WHERE PMT.BUSINESS_TYPE = 2 AND PMT.PORT_ID  = '" + PORID.ToUpper() + "'");
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "GettingPOLANDPODPk"

        #region "Getting Vessel PK"

        /// <summary>
        /// Gettings the vessel pk.
        /// </summary>
        /// <param name="VSLID">The vslid.</param>
        /// <returns></returns>
        public long GettingVesselPk(string VSLID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT VMT.VESSEL_VOYAGE_TBL_PK FROM VESSEL_VOYAGE_TBL VMT WHERE VMT.VESSEL_ID  = '" + VSLID.ToUpper() + "'");
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "Getting Vessel PK"

        #region "Getting Voyage PK"

        /// <summary>
        /// Gettings the voyage pk.
        /// </summary>
        /// <param name="VOYAGE">The voyage.</param>
        /// <param name="VSLID">The vslid.</param>
        /// <param name="VSLNAME">The vslname.</param>
        /// <returns></returns>
        public long GettingVoyagePk(string VOYAGE, string VSLID, string VSLNAME)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT VVT.VOYAGE_TRN_PK FROM VESSEL_VOYAGE_TRN VVT, VESSEL_VOYAGE_TBL VT WHERE 1 = 1 AND VT.VESSEL_VOYAGE_TBL_PK=VVT.VESSEL_VOYAGE_TBL_FK AND  UPPER(VT.VESSEL_ID) = ' " + VSLID + " ' AND  UPPER(VT.VESSEL_NAME) = ' " + VSLNAME + " '  AND VVT.VOYAGE = '" + VOYAGE.ToUpper() + "'");
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "Getting Voyage PK"

        #region "VessselID Duplicate"

        /// <summary>
        /// Vessels the identifier oper dup.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <param name="ID">The identifier.</param>
        /// <param name="NAME">The name.</param>
        /// <returns></returns>
        public string VesselIDOperDup(string PK, string ID, string NAME)
        {
            string strSQL = null;
            string strSQL1 = null;
            string strconditon = null;
            WorkFlow objWF = new WorkFlow();
            // strSQL1 = "select count(*) from vessel_voyage_tbl v where v.vessel_id = '" & ID & "' and v.vessel_name = '" & NAME & "' "
            try
            {
                // Dim intRowAffected1 As String = objWF.ExecuteScaler(strSQL1)
                // If intRowAffected1 > 0 Then
                strSQL = "select v.operator_mst_fk from vessel_voyage_tbl v,operator_mst_tbl o  where v.operator_mst_fk=o.operator_mst_pk and v.vessel_id = '" + ID + "' and v.vessel_name = '" + NAME + "' and o.operator_mst_pk = '" + PK + "'  ";
                try
                {
                    string intRowAffected = objWF.ExecuteScaler(strSQL);
                    if (!string.IsNullOrEmpty(intRowAffected))
                    {
                        return "1";
                    }
                    else
                    {
                        return "0";
                    }
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
                //Else
                return "1";
                //End If
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        #endregion "VessselID Duplicate"

        #region "Fetch Pending Activites"

        /// <summary>
        /// Fetches the vessel noti grid.
        /// </summary>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="Status">The status.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchVesselNotiGrid(string VslName = "", string Voyage = "", string Status = "", string ProcessType = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with13 = objWF.MyDataAdapter;
                _with13.SelectCommand = new OracleCommand();
                _with13.SelectCommand.Connection = objWF.MyConnection;
                if (Convert.ToInt32(ProcessType) == 1)
                {
                    _with13.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_VESEEL_INFO";
                }
                else
                {
                    _with13.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_LOAD_CONFIRM_PKG.FETCH_VESEEL_INFO_POD";
                }
                _with13.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with13.SelectCommand.Parameters.Add("VESSEL_NAME_IN", (string.IsNullOrEmpty(VslName) ? "" : VslName)).Direction = ParameterDirection.Input;
                _with13.SelectCommand.Parameters.Add("VOYAGE_IN", (string.IsNullOrEmpty(Voyage) ? "" : Voyage)).Direction = ParameterDirection.Input;
                _with13.SelectCommand.Parameters.Add("STATUS_IN", (string.IsNullOrEmpty(Status) ? "" : Status)).Direction = ParameterDirection.Input;
                _with13.SelectCommand.Parameters.Add("LOCATION_IN", (string.IsNullOrEmpty(HttpContext.Current.Session["LOGED_IN_LOC_FK"].ToString()) ? "" : HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                _with13.SelectCommand.Parameters.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with13.Fill(ds);
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

        #endregion "Fetch Pending Activites"

        /// <summary>
        /// Updates the vessel dealy information.
        /// </summary>
        /// <param name="DsMain">The ds main.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public ArrayList UpdateVesselDealyInfo(DataSet DsMain, int ProcessType = 0)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            int rowC = 0;
            System.DateTime REVISED_ETA = default(System.DateTime);
            System.DateTime REVISED_ETD = default(System.DateTime);
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            for (rowC = 0; rowC <= DsMain.Tables[0].Rows.Count - 1; rowC++)
            {
                //REVISED_ETA = System.String.Format("{0:dd/MM/yyyy HH:mm}", CDate(DsMain.Tables(0).Rows(rowC).Item("REVISED_ETA")))
                //REVISED_ETD = System.String.Format("{0:dd/MM/yyyy HH:mm}", CDate(DsMain.Tables(0).Rows(rowC).Item("REVISED_ETD")))
                //REVISED_ETA = CDate(DsMain.Tables(0).Rows(rowC).Item("REVISED_ETA"))
                //REVISED_ETD = CDate(DsMain.Tables(0).Rows(rowC).Item("REVISED_ETD"))

                strSQL = "UPDATE VESSEL_VOYAGE_TRN VVT SET ";
                if (string.IsNullOrEmpty(DsMain.Tables[0].Rows[rowC]["REVISED_ETA"].ToString()))
                {
                    strSQL = strSQL + "  VVT.REVISED_ETA = '',";
                }
                else
                {
                    strSQL = strSQL + "  VVT.REVISED_ETA = to_date('" + DsMain.Tables[0].Rows[rowC]["REVISED_ETA"] + "',datetimeformat24),";
                }
                if (string.IsNullOrEmpty(DsMain.Tables[0].Rows[rowC]["REVISED_ETD"].ToString()))
                {
                    strSQL = strSQL + " VVT.REVISED_ETD = '',";
                }
                else
                {
                    strSQL = strSQL + " VVT.REVISED_ETD = to_date('" + DsMain.Tables[0].Rows[rowC]["REVISED_ETD"] + "',datetimeformat24),";
                }
                strSQL = strSQL + " VVT.VOYAGE_STATUS = '" + DsMain.Tables[0].Rows[rowC]["VOYAGE_STATUS"] + "' ";
                //If ProcessType = 1 Then
                //    strSQL = strSQL & vbCrLf & " WHERE VVT.VOYAGE_TRN_PK ='" & DsMain.Tables(0).Rows(rowC).Item("VOYAGE_TRN_PK") & "' AND VVT.PORT_MST_POL_FK ='" & DsMain.Tables(0).Rows(rowC).Item("PORT_MST_POL_FK") & "'"
                //Else
                //    strSQL = strSQL & vbCrLf & " WHERE VVT.VOYAGE_TRN_PK ='" & DsMain.Tables(0).Rows(rowC).Item("VOYAGE_TRN_PK") & "' AND VVT.PORT_MST_POD_FK ='" & DsMain.Tables(0).Rows(rowC).Item("PORT_MST_POD_FK") & "'"
                //End If
                strSQL = strSQL + " WHERE VVT.VOYAGE_TRN_PK ='" + DsMain.Tables[0].Rows[rowC]["VOYAGE_TRN_PK"] + "' AND VVT.PORT_MST_POL_FK ='" + DsMain.Tables[0].Rows[rowC]["PORT_MST_POL_FK"] + "'";
                objWK.MyCommand.CommandType = CommandType.Text;
                objWK.MyCommand.CommandText = strSQL;
            }
            try
            {
                objWK.MyCommand.ExecuteNonQuery();
                arrMessage.Add("All data saved successfully");
                TRAN.Commit();
                return arrMessage;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        #region " Fetch Document Details"

        /// <summary>
        /// Fetches the document.
        /// </summary>
        /// <param name="documentId">The document identifier.</param>
        /// <returns></returns>
        public long FetchDocument(string documentId)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSqlBuilder.Append(" SELECT DMT.DOCUMENT_MST_PK ");
            strSqlBuilder.Append(" FROM DOCUMENT_MST_TBL DMT, ");
            strSqlBuilder.Append(" DOCUMENT_NAME_MST_TBL DN ");
            strSqlBuilder.Append(" WHERE");
            strSqlBuilder.Append(" UPPER(DN.DOCUMENT_NAME)= UPPER('" + documentId.ToUpper() + "')");
            strSqlBuilder.Append(" AND DN.DOCUMENT_NAME_MST_PK = DMT.DOCUMENT_NAME_MST_FK ");
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(strSqlBuilder.ToString()));
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion " Fetch Document Details"

        #region "Get Message Information"

        /// <summary>
        /// Gets the message information.
        /// </summary>
        /// <param name="lngDocPk">The LNG document pk.</param>
        /// <param name="lngLocPk">The LNG loc pk.</param>
        /// <returns></returns>
        public DataSet GetMessageInfo(long lngDocPk, long lngLocPk)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            DataSet dsMsgInfo = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSqlBuilder.Append(" SELECT ROWNUM SR_NO,  ");
                strSqlBuilder.Append(" 0 USER_MESSAGE_PK, ");
                strSqlBuilder.Append(" USRMSGTRN.SENDER_FK, ");
                strSqlBuilder.Append(" USRMSGTRN.RECEIVER_FK, ");
                strSqlBuilder.Append(" USRMSGTRN.MSG_READ, ");
                strSqlBuilder.Append(" USRMSGTRN.FOLLOWUP_FLAG, ");
                strSqlBuilder.Append(" USRMSGTRN.HAVE_ATTACHMENT, ");
                strSqlBuilder.Append(" DOC.DOCUMENT_SUBJECT  MSG_SUBJECT, ");
                strSqlBuilder.Append(" DOC.DOCUMENT_BODY  MSG_BODY1, ");
                strSqlBuilder.Append(" DOC.DOCUMENT_HEADER || chr(10) ");
                strSqlBuilder.Append(" || '     ' || DOC.DOCUMENT_BODY || chr(10) ");
                strSqlBuilder.Append(" ||  DOC.DOCUMENT_FOOTER ||");
                strSqlBuilder.Append(" chr(10) || ' ' Msg_Body,");

                strSqlBuilder.Append(" DOC.DOCUMENT_HEADER || chr(10) ");
                strSqlBuilder.Append(" || '     ' || DOC.DOCUMENT_BODY || ");
                strSqlBuilder.Append(" chr(10) || ' ' ExternalMsgBody,");
                strSqlBuilder.Append(" DOC.DOCUMENT_FOOTER as ExternalMsgFooter,");

                strSqlBuilder.Append(" USRMSGTRN.READ_RECEIPT, ");
                strSqlBuilder.Append(" USRMSGTRN.DOCUMENT_MST_FK, ");
                strSqlBuilder.Append(" DOC.MESSAGE_FOLDER_MST_FK USER_MESSAGE_FOLDERS_FK, ");
                strSqlBuilder.Append(" USRMSGTRN.MSG_RECEIVED_DT, ");
                strSqlBuilder.Append(" USRMSGTRN.VERSION_NO  ");
                strSqlBuilder.Append(" FROM USER_MESSAGE_TRN USRMSGTRN, ");
                strSqlBuilder.Append(" DOCUMENT_MST_TBL DOC ");
                strSqlBuilder.Append(" WHERE USRMSGTRN.DOCUMENT_MST_FK(+) = DOC.DOCUMENT_MST_PK ");
                strSqlBuilder.Append(" AND USRMSGTRN.DELETE_FLAG IS NULL  AND DOC.document_mst_pk =  " + lngDocPk + "");
                strSqlBuilder.Append(" AND USER_MESSAGE_PK(+) =  -1 ");

                DA = objWF.GetDataAdapter(strSqlBuilder.ToString().Trim());
                DA.Fill(dsMsgInfo, "MsgTrn");
                strSqlBuilder = new StringBuilder();
                strSqlBuilder.Append(" SELECT ROWNUM SR_NO, ");
                strSqlBuilder.Append(" 0 User_Message_Det_Pk, ");
                strSqlBuilder.Append(" 0 User_Message_Fk, ");
                strSqlBuilder.Append(" '' Attachment_Caption, ");
                strSqlBuilder.Append(" '' Attachment_Data, ");
                strSqlBuilder.Append(" doc.attachment_url Url_Page, ");
                strSqlBuilder.Append(" 0 Version_No ");
                strSqlBuilder.Append(" FROM document_mst_tbl doc ");
                strSqlBuilder.Append(" WHERE doc.Active=1 And doc.document_mst_pk = " + lngDocPk + "");

                DA = objWF.GetDataAdapter(strSqlBuilder.ToString());
                DA.Fill(dsMsgInfo, "MsgDet");
                DataRelation DSWFMSg = new DataRelation("WFMsg", new DataColumn[] { dsMsgInfo.Tables["MsgTrn"].Columns["User_Message_Pk"] }, new DataColumn[] { dsMsgInfo.Tables["MsgDet"].Columns["User_Message_Fk"] });
                dsMsgInfo.Relations.Add(DSWFMSg);
                return dsMsgInfo;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Get Message Information"
    }
}