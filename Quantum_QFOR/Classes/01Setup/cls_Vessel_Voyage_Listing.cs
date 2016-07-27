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
using Oracle.DataAccess.Types;
using System;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Vessel_Voyage_Listing : CommonFeatures
    {
        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="ETAPOD">The etapod.</param>
        /// <param name="ETAPOL">The etapol.</param>
        /// <param name="ETDPOL">The etdpol.</param>
        /// <param name="CUTOFFPOL">The cutoffpol.</param>
        /// <param name="SLINE">The sline.</param>
        /// <param name="VName">Name of the v.</param>
        /// <param name="VOYAGE">The voyage.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="IsActive">The is active.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchAll(System.DateTime ETAPOD, System.DateTime ETAPOL, System.DateTime ETDPOL, System.DateTime CUTOFFPOL, string SLINE = "", string VName = "", string VOYAGE = "", string POL = "", string POD = "", Int16 IsActive = 0,
        string SearchType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string SortColumn = "", Int32 flag = 0, Int32 Export = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strAct = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (SLINE.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(op.operator_id) like '%" + SLINE.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(op.operator_id)like '" + SLINE.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (VName.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(v.vessel_name) like '%" + VName.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(v.vessel_name) like '" + VName.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (VOYAGE.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(vt.voyage) like '%" + VOYAGE.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(vt.voyage) like '" + VOYAGE.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (POL.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(POL.PORT_ID) like '%" + POL.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(POL.PORT_ID) like '" + POL.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (POD.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(POD.PORT_ID) like '%" + POD.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(POD.PORT_ID) like '" + POD.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (!(ETAPOL == null) & !(ETDPOL == null) & !(CUTOFFPOL == null))
            {
                strCondition = strCondition + " and vt.pol_eta >= TO_date('" + ETAPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
                strCondition = strCondition + " and vt.pol_etd <= TO_date('" + ETDPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
                strCondition = strCondition + " and vt.pol_cut_off_date >= TO_date('" + CUTOFFPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
            }
            else if (!(ETAPOL == null) & !(ETDPOL == null))
            {
                strCondition = strCondition + " and vt.pol_eta >= TO_date('" + ETAPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
                strCondition = strCondition + " and vt.pol_etd <= TO_date('" + ETDPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
            }
            else if (!(ETAPOL == null) & !(CUTOFFPOL == null))
            {
                strCondition = strCondition + " and vt.pol_eta >= TO_date('" + ETAPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
                strCondition = strCondition + " and vt.pol_cut_off_date >= TO_date('" + CUTOFFPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
            }
            else if (!(ETDPOL == null) & !(CUTOFFPOL == null))
            {
                strCondition = strCondition + " and vt.pol_etd >= TO_date('" + ETDPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
                strCondition = strCondition + " and vt.pol_cut_off_date >= TO_date('" + CUTOFFPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
            }
            else if (!(ETAPOL == null))
            {
                strCondition = strCondition + " and vt.pol_eta >= TO_date('" + ETAPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
            }
            else if (!(ETDPOL == null))
            {
                strCondition = strCondition + " and vt.pol_etd >= TO_date('" + ETDPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
            }
            else if (!(CUTOFFPOL == null))
            {
                strCondition = strCondition + " and vt.pol_cut_off_date >= TO_date('" + CUTOFFPOL + "','& dd/MM/yyyy  HH24:MI:SS &')";
            }

            if (!(ETAPOD == null))
            {
                strCondition = strCondition + " and vt.pod_eta >= TO_date('" + ETAPOD + "','& dd/MM/yyyy  HH24:MI:SS &')";
            }

            if (IsActive == 1)
            {
                strAct += " and V.ACTIVE = 1";
                strCondition = strCondition + "  and V.ACTIVE = 1";
            }

            //strSQL = "SELECT Count(*) from vessel_voyage_tbl v"
            //strSQL = strSQL & vbCrLf & "WHERE ( 1 = 1) "
            //strSQL &= vbCrLf & strAct
            strSQL = " SELECT count(*) from (";
            strSQL = strSQL + " SELECT ROWNUM SR_NO,q.* FROM";
            strSQL = strSQL + " (SELECT";
            strSQL = strSQL + " v.vessel_id";
            strSQL = strSQL + " FROM";
            strSQL = strSQL + " VESSEL_VOYAGE_TBL    V,";
            strSQL = strSQL + " VESSEL_VOYAGE_TRN    VT,";
            strSQL = strSQL + " OPERATOR_MST_TBL     OP,";
            strSQL = strSQL + " PORT_MST_TBL         POL,";
            strSQL = strSQL + " PORT_MST_TBL POD ";
            strSQL = strSQL + " WHERE";
            strSQL = strSQL + " VT.VESSEL_VOYAGE_TBL_FK(+)= V.VESSEL_VOYAGE_TBL_PK";
            strSQL = strSQL + " AND V.OPERATOR_MST_FK = OP.OPERATOR_MST_PK(+)";
            strSQL = strSQL + " AND VT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)";
            strSQL = strSQL + strCondition;
            strSQL = strSQL + " AND VT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            strSQL = strSQL + " )Q)";
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));

            strSQL = " SELECT * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM";
            strSQL += " (SELECT";
            strSQL += " decode(v.Active,1,1,0,0)Active, ";
            strSQL += " OP.OPERATOR_MST_PK AS \"HIDDEN\",";
            strSQL += " OP.OPERATOR_ID AS \"LINE\",";
            strSQL += " V.VESSEL_NAME AS \"VESSEL NAME\",";
            strSQL += " VT.VOYAGE AS \"VOYAGE\",";
            strSQL += " POL.PORT_ID AS \"POL\",";
            strSQL += " POD.PORT_ID AS \"POD\",";
            strSQL += " '' AS \"POC\",";
            //strSQL &= vbCrLf & "to_Char(VT.POL_ETA,  dateTimeFormat ),"
            //strSQL &= vbCrLf & " to_Char(VT.POL_ETD, 'dd/MM/yyyy HH:mm')POL_ETD,"
            //strSQL &= vbCrLf & "to_Char(VT.POL_CUT_OFF_DATE, 'dd/MM/yyyy HH:mm')POL_CUT_OFF_DATE,"
            //strSQL &= vbCrLf & "to_Char(VT.POD_ETA, 'dd/MM/yyyy HH:mm')POD_ETA,"
            strSQL += "VT.POL_ETA, ";
            strSQL += "VT.POL_ETD, ";
            strSQL += "VT.POL_CUT_OFF_DATE, ";
            strSQL += "VT.POD_ETA, ";
            strSQL += " '' AS \"EXCHRATE\",";
            strSQL += " V.VESSEL_VOYAGE_TBL_PK AS \"VOYAGEPK\" ,";
            strSQL += " VT.VOYAGE_TRN_PK,";
            strSQL += " v.vessel_id, ";
            strSQL += "  CASE WHEN ((SELECT COUNT(*)  FROM booking_MST_tbl BST WHERE BST.VESSEL_VOYAGE_FK = VT.VOYAGE_TRN_PK) +";
            strSQL += "   (SELECT COUNT(*) FROM JOB_CARD_TRN JCT WHERE JCT.VOYAGE_TRN_FK =  VT.VOYAGE_TRN_PK)) > 0 THEN 1 ELSE 0 END EDITABLE";
            strSQL += "    , '' SEL ";
            strSQL += " FROM";
            strSQL += " VESSEL_VOYAGE_TBL    V,";
            strSQL += " VESSEL_VOYAGE_TRN    VT,";
            strSQL += " OPERATOR_MST_TBL     OP,";
            strSQL += " PORT_MST_TBL         POL,";
            strSQL += " PORT_MST_TBL POD ";
            strSQL += " WHERE";
            strSQL += " VT.VESSEL_VOYAGE_TBL_FK(+) = V.VESSEL_VOYAGE_TBL_PK";
            strSQL += " AND V.OPERATOR_MST_FK = OP.OPERATOR_MST_PK(+)";
            strSQL += " AND VT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)";
            strSQL += " AND VT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            strSQL += strCondition;
            strSQL += strAct;
            //strSQL &= vbCrLf & " ORDER BY VT.POL_ETA desc, OP.OPERATOR_ID,"
            strSQL += " ORDER BY VT.POL_ETD desc, OP.OPERATOR_ID,";
            strSQL += " V.VESSEL_NAME,";
            strSQL += " VT.VOYAGE,";
            strSQL += " POL.PORT_ID,";
            strSQL += " POD.PORT_ID";
            //strSQL &= vbCrLf & " POD.PORT_ID)Q)"
            //strSQL &= " WHERE SR_NO  Between " & start & " and " & last
            if (Export == 0)
            {
                strSQL += " )Q)  WHERE SR_NO Between " + start + " and " + last;
            }
            else
            {
                strSQL += " )Q) ";
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

        #endregion "Fetch Function"

        #region "Port"

        /// <summary>
        /// Fetches the port.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchPort()
        {
            string strSQL = null;
            strSQL = "select ' ' PORT_ID,";
            strSQL = strSQL + " ' ' PORT_NAME, ";
            strSQL = strSQL + "0 PORT_MST_PK ";
            strSQL = strSQL + "from DUAL ";
            strSQL = strSQL + "UNION ";
            strSQL = strSQL + "Select PORT_ID, ";
            strSQL = strSQL + "PORT_NAME,";
            strSQL = strSQL + "PORT_MST_PK ";
            strSQL = strSQL + "from PORT_mst_tbl Where 1=1 and active_flag=1 ";
            strSQL = strSQL + " order by PORT_ID";
            WorkFlow objWF = new WorkFlow();
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

        #endregion "Port"

        #region "Fetch All"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="shippingID">The shipping identifier.</param>
        /// <returns></returns>
        public DataSet fetchAll(string shippingID = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            strCondition = "AND v.vessel_voyage_tbl_pk = '" + shippingID + "' ";

            strSQL = " select op.operator_name,";
            strSQL = strSQL + "v.vessel_name ,";
            strSQL = strSQL + "vt.voyage AS \"VOYAGE\",";
            strSQL = strSQL + "POL.PORT_ID AS \"POL\", POD.PORT_ID AS \"POD\",";
            strSQL = strSQL + "    '' AS \"POC\",";
            strSQL = strSQL + "vt.pol_eta AS \"ETA_POL\", vt.pol_etd AS \"ETD_POL\",";
            strSQL = strSQL + "vt.pol_cut_off_date AS \"CUT OFF_POL\",vt.pod_eta AS \"ETA_POD\",'' as \"ExchRate\", v.vessel_voyage_tbl_pk as \"VoyagePK\", ";
            strSQL = strSQL + "pol.port_mst_pk,pod.port_mst_pk";
            strSQL = strSQL + " from vessel_voyage_tbl v,vessel_voyage_trn vt,vessel_voyage_trn_tp tp,operator_mst_tbl op,";

            strSQL = strSQL + "PORT_MST_TBL POL, PORT_MST_TBL POD";
            strSQL = strSQL + "where op.operator_mst_pk = v.operator_mst_fk(+) And v.vessel_voyage_tbl_pk = vt.vessel_voyage_tbl_fk(+)";
            strSQL = strSQL + "AND VT.PORT_MST_POL_FK=POL.PORT_MST_PK(+) AND";
            strSQL = strSQL + "VT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            strSQL = strSQL + strCondition;
            strSQL = strSQL + "order by op.operator_name";

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

        #region "Enhance Search"

        /// <summary>
        /// Fetches the name of the vessel identifier.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchVesselIDName(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strReq = null;
            string strOpr = null;
            string strBkgDt = null;
            //Dim lblVoyagePK As String
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strOpr = Convert.ToString(arr.GetValue(2));
            //If arr.Length > 3 Then strBkgDt = arr(3)
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GETVESSELIDNAME";
                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", getDefault(strVES, "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("OPERATOR_MST_FK_IN", getDefault(strOpr, 0)).Direction = ParameterDirection.Input;
                //.Add("BKG_DT_IN", strBkgDt).Direction = ParameterDirection.Input
                _with1.Add("RETURN_VALUE", OracleDbType.Varchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        //Added By : Anand G 05-06-2008 to fetch only vessels
        /// <summary>
        /// Fetches the vessels.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchVessels(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;

            string strReq = null;
            string strLoc = null;
            string stroprpk = null;
            //Dim lblVoyagePK As String

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            strLoc = Convert.ToString(arr.GetValue(3));
            stroprpk = Convert.ToString(arr.GetValue(5));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VESSEL_COMMON";

                var _with2 = selectCommand.Parameters;
                _with2.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with2.Add("OPRPK_IN", (!string.IsNullOrEmpty(stroprpk) ? stroprpk : "")).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        /// <summary>
        /// Fetches the vessels RPT.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchVesselsRpt(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;

            string strReq = null;
            string strLoc = null;
            string stroprpk = null;
            //Dim lblVoyagePK As String

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            strLoc = Convert.ToString(arr.GetValue(3));
            stroprpk = Convert.ToString(arr.GetValue(5));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VESSEL_COMMON_RPT";

                var _with3 = selectCommand.Parameters;
                _with3.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with3.Add("OPRPK_IN", (!string.IsNullOrEmpty(stroprpk) ? stroprpk : "")).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
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
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        //Added By : Anand G 05-06-2008 to fetch only voyages
        /// <summary>
        /// Fetches the voyage.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchVoyage(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;

            string strReq = null;
            string strLoc = null;
            string VesselPk = null;
            //Dim lblVoyagePK As String

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            strLoc = Convert.ToString(arr.GetValue(3));
            VesselPk = Convert.ToString(arr.GetValue(5));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VOYAGE_COMMON_FETCH";
                //"VesselPk_IN" adding by thiyagarajan on 30/12/08 to fecth voyage based on which "Vessel" selected
                var _with4 = selectCommand.Parameters;
                _with4.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with4.Add("VesselPk_IN", VesselPk).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        /// <summary>
        /// Fetches the vessel voyage.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchVesselVoyage(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = "";
            string operatorPK = "";
            string strReq = null;
            string strLoc = null;
            //Dim lblVoyagePK As String
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
            {
                strVOY = Convert.ToString(arr.GetValue(2));
            }
            if (arr.Length > 3)
            {
                operatorPK = Convert.ToString(arr.GetValue(3));
            }
            // If arr.Length > 4 Then
            //lblVoyagePK = arr(4)
            // End If

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VESSEL_VOYAGE_COMMON";

                var _with5 = selectCommand.Parameters;
                _with5.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with5.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with5.Add("OPERATOR_PK_IN", (!string.IsNullOrEmpty(operatorPK) ? operatorPK : "")).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                //.Add("VOYAGE_PK_IN", IIf(lblVoyagePK <> "", lblVoyagePK, "")).Direction = ParameterDirection.Input
                _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        /// <summary>
        /// Fetches the vessel voyage imp cp.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesselVoyageImpCP(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = "";
            string operatorPK = "";
            string strReq = null;
            string strLoc = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
            {
                strVOY = Convert.ToString(arr.GetValue(2));
            }
            if (arr.Length > 3)
            {
                operatorPK = Convert.ToString(arr.GetValue(3));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VESSEL_VOYAGE_COMMON_IMP_CB";

                var _with6 = selectCommand.Parameters;
                _with6.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with6.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with6.Add("OPERATOR_PK_IN", (!string.IsNullOrEmpty(operatorPK) ? operatorPK : "")).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        /// <summary>
        /// Fetches the VSL flight.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchVslFlight(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = "";
            string operatorPK = "";
            string strReq = null;
            string strLoc = null;
            //Dim lblVoyagePK As String
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
            {
                strVOY = Convert.ToString(arr.GetValue(2));
            }
            if (arr.Length > 3)
            {
                operatorPK = Convert.ToString(arr.GetValue(3));
            }
            // If arr.Length > 4 Then
            //lblVoyagePK = arr(4)
            // End If

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VSL_VOY_FLIGHT";

                var _with7 = selectCommand.Parameters;
                _with7.Add("VESFlIGHT_ID_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with7.Add("VSLFLIGHT_NAME_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with7.Add("OPERATOR_PK_IN", (!string.IsNullOrEmpty(operatorPK) ? operatorPK : "")).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with7.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                //.Add("VOYAGE_PK_IN", IIf(lblVoyagePK <> "", lblVoyagePK, "")).Direction = ParameterDirection.Input
                _with7.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        /// <summary>
        /// Fetches the vessel voyage opr.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchVesselVoyageOpr(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = null;
            string strBizType = null;
            string strJobPK = null;
            string strReq = null;
            string strOPR = null;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            strVOY = Convert.ToString(arr.GetValue(2));
            strOPR = Convert.ToString(arr.GetValue(3));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VESSEL_VOYAGE_COMMON_OPR";

                var _with8 = selectCommand.Parameters;
                _with8.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with8.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with8.Add("OPR_IN", (!string.IsNullOrEmpty(strOPR) ? strOPR : "")).Direction = ParameterDirection.Input;
                _with8.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with8.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with8.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        /// <summary>
        /// Fetches the vessel voyage for inv cb.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchVesselVoyageForInvCB(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = "";
            string operatorPK = "";
            string strReq = null;
            string Biz_IN = null;
            string Process_IN = null;
            string Agent_IN = null;
            string strLoc = null;
            //Dim lblVoyagePK As String
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
            {
                strVOY = Convert.ToString(arr.GetValue(2));
            }
            if (arr.Length > 3)
            {
                operatorPK = Convert.ToString(arr.GetValue(3));
            }
            if (arr.Length > 4)
            {
                Biz_IN = Convert.ToString(arr.GetValue(4));
            }
            if (arr.Length > 5)
            {
                Agent_IN = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                Process_IN = Convert.ToString(arr.GetValue(6));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VESSEL_VOYAGE_INV_CB";

                var _with9 = selectCommand.Parameters;
                _with9.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with9.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with9.Add("OPERATOR_PK_IN", (!string.IsNullOrEmpty(operatorPK) ? operatorPK : "")).Direction = ParameterDirection.Input;
                _with9.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with9.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with9.Add("Biz_IN", (!string.IsNullOrEmpty(Biz_IN) ? Biz_IN : "")).Direction = ParameterDirection.Input;
                _with9.Add("Process_IN", (!string.IsNullOrEmpty(Process_IN) ? Process_IN : "")).Direction = ParameterDirection.Input;
                _with9.Add("Agent_IN", (!string.IsNullOrEmpty(Agent_IN) ? Agent_IN : "")).Direction = ParameterDirection.Input;
                //.Add("VOYAGE_PK_IN", IIf(lblVoyagePK <> "", lblVoyagePK, "")).Direction = ParameterDirection.Input
                _with9.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #endregion "Enhance Search"

        // By Amit Singh on 21-April-07

        #region "Enhance Search for Job Card"

        /// <summary>
        /// Fetches the ves voy job card.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVesVoyJobCard(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strOpr = null;
            string strPol = null;
            string strPod = null;
            string strVES = null;
            string strVOY = null;
            string strReq = null;
            string strJCdt = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strOpr = Convert.ToString(arr.GetValue(1));
            strPol = Convert.ToString(arr.GetValue(2));
            strPod = Convert.ToString(arr.GetValue(3));
            strVES = Convert.ToString(arr.GetValue(4));
            strVOY = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strJCdt = Convert.ToString(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VESSEL_VOYAGE_JOBCARD";
                var _with10 = SCM.Parameters;
                _with10.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with10.Add("OPR_IN", ifDBNull(strOpr)).Direction = ParameterDirection.Input;
                _with10.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with10.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with10.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with10.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with10.Add("JCDT_IN", ((strJCdt == null) ? "" : strJCdt)).Direction = ParameterDirection.Input;
                _with10.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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
                SCM.Connection.Close();
            }
        }

        #endregion "Enhance Search for Job Card"

        #region " Supporting Function "

        /// <summary>
        /// Ifs the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        #endregion " Supporting Function "

        // End

        #region "CLP Report MainDS"

        /// <summary>
        /// Fetches the RPT ds.
        /// </summary>
        /// <param name="VOYPK">The voypk.</param>
        /// <returns></returns>
        public DataSet FetchRptDS(string VOYPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = "select rownum as slnr,";
                strSQL += "job_exp.JOB_CARD_TRN_PK job_card_sea_exp_pk,";
                strSQL += "job_exp.booking_MST_FK,";
                strSQL += "job_exp.jobcard_ref_no,";
                strSQL += "job_exp.vessel_name,";
                strSQL += "job_exp.VOYAGE_FLIGHT_NO voyage,";
                strSQL += "job_exp.survey_ref_nr,";
                strSQL += "job_exp.survey_date,";
                strSQL += "job_exp.stuff_loc,";
                strSQL += "chaagent.agent_id,";
                strSQL += "chaagent.agent_name,";
                strSQL += "job_cont.container_number,";
                strSQL += "cont_type.container_type_mst_id,";
                strSQL += "cont_type.container_type_name,";
                strSQL += "cont_type.container_tareweight_tone,";
                strSQL += "'' agent_seal_no,";
                strSQL += "job_cont.seal_number customs_seal_no,";
                strSQL += "pod.port_name,";
                strSQL += "del_place.place_code,";
                strSQL += "del_place.place_name,";
                strSQL += "job_exp.sb_number,";
                strSQL += "job_exp.sb_date,";
                strSQL += "shipper.customer_name shipper,";
                strSQL += "job_cont.pack_count,";
                strSQL += "job_cont.gross_weight,";
                strSQL += "job_exp.marks_numbers,";
                strSQL += "job_exp.goods_description,";
                strSQL += "consignee.customer_name consignee,";
                strSQL += "job_cont.volume_in_cbm,";
                strSQL += "chaagent.agent_name chaagent,";
                strSQL += "'' csno,'' grno,";
                strSQL += "pod.port_id as sbpod,";
                strSQL += "job_exp.remarks,";
                strSQL += "'' invnr";

                strSQL += "from JOB_CARD_TRN   job_exp,";
                strSQL += "agent_mst_tbl          chaagent,";
                strSQL += "job_trn_cont   job_cont,";
                strSQL += "container_type_mst_tbl cont_type,";
                strSQL += "port_mst_tbl           pod,";
                strSQL += "booking_MST_tbl        bkg_sea,";
                strSQL += "place_mst_tbl          del_place,";
                strSQL += "customer_mst_tbl       shipper,";
                strSQL += "customer_mst_tbl       consignee,";
                strSQL += "vessel_voyage_tbl vvt,vessel_voyage_trn vt ";

                strSQL += "where vt.voyage_trn_pk IN( " + VOYPK + " )";
                strSQL += "and vt.vessel_voyage_tbl_fk=vvt.vessel_voyage_tbl_pk";
                strSQL += "and vt.voyage_trn_pk=job_exp.voyage_trn_fk";
                strSQL += "and job_exp.cha_agent_mst_fk = chaagent.agent_mst_pk(+)";
                strSQL += "and job_exp.JOB_CARD_TRN_PK(+) = job_cont.JOB_CARD_TRN_FK";
                strSQL += "and cont_type.container_type_mst_pk(+) = job_cont.container_type_mst_fk";
                strSQL += "and job_exp.booking_MST_FK = bkg_sea.booking_MST_PK";
                strSQL += "and bkg_sea.port_mst_pod_fk = pod.port_mst_pk";
                strSQL += "and bkg_sea.del_place_mst_fk = del_place.place_pk(+)";
                strSQL += "and job_exp.shipper_cust_mst_fk =  shipper.customer_mst_pk(+)";
                strSQL += "and job_exp.consignee_cust_mst_fk =  consignee.customer_mst_pk(+)";
                return (objWF.GetDataSet(strSQL));
                //Manjunath  PTS ID:Sep-02  15/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        #endregion "CLP Report MainDS"
    }
}