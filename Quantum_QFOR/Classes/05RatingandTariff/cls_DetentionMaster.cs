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
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_DetentionMaster : CommonFeatures
    {
        #region "Fetch Existing Containers"

        /// <summary>
        /// Fetches the existing containers.
        /// </summary>
        /// <param name="detentionHdrPk">The detention HDR pk.</param>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <returns></returns>
        public object FetchExistingContainers(int detentionHdrPk, int Biztype = 2, int Cargotype = 1)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (Biztype == 2 & Cargotype == 1)
            {
                sb.Append("SELECT DISTINCT C.CONTAINER_TYPE_MST_FK CONTAINER_TYPE_MST_PK,");
                sb.Append("  CT.CONTAINER_TYPE_MST_ID, CT.CONTAINER_TYPE_NAME, CT.PREFERENCES ");
                sb.Append("  FROM det_slab_trn C, CONTAINER_TYPE_MST_TBL CT");
                sb.Append(" WHERE C.CONTAINER_TYPE_MST_FK = CT.CONTAINER_TYPE_MST_PK");
                sb.Append("  AND C.det_slab_hdr_fk =");
                sb.Append(detentionHdrPk);
                sb.Append(" Order By CT.PREFERENCES ");
            }
            else
            {
                sb.Append("SELECT DISTINCT C.CONTAINER_TYPE_MST_FK CONTAINER_TYPE_MST_PK");
                sb.Append("  FROM det_slab_trn C");
                sb.Append("  WHERE C.det_slab_hdr_fk =");
                sb.Append(detentionHdrPk);
                sb.Append(" Order By c.container_type_mst_fk ");
            }
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Existing Containers"

        #region "FetchContainers"

        /// <summary>
        /// Fetches the containers.
        /// </summary>
        /// <param name="containerPk">The container pk.</param>
        /// <param name="Biztype">The biztype.</param>
        /// <returns></returns>
        public DataSet FetchContainers(string containerPk = "", int Biztype = 2)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            if (Biztype == 2)
            {
                strSQL.Append(" select ");
                strSQL.Append(" CTMT.CONTAINER_TYPE_MST_PK,  CTMT.CONTAINER_TYPE_MST_ID, CTMT.CONTAINER_TYPE_NAME ");
                strSQL.Append(" FROM CONTAINER_TYPE_MST_TBL CTMT ");
                strSQL.Append(" WHERE CTMT.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(containerPk))
                {
                    strSQL.Append(" AND CTMT.CONTAINER_TYPE_MST_PK IN (" + containerPk + ")");
                }
                strSQL.Append(" ORDER BY CTMT.PREFERENCES");
            }
            else
            {
                strSQL.Append(" SELECT '1' CONTAINER_TYPE_MST_PK");
                strSQL.Append(" FROM DUAL");
                strSQL.Append(" UNION");
                strSQL.Append(" SELECT '2' CONTAINER_TYPE_MST_PK");
                strSQL.Append(" FROM DUAL");
                strSQL.Append(" UNION");
                strSQL.Append(" SELECT '3' CONTAINER_TYPE_MST_PK");
                strSQL.Append(" FROM DUAL");
                strSQL.Append(" UNION");
                strSQL.Append(" SELECT '4' CONTAINER_TYPE_MST_PK FROM DUAL");
            }
            try
            {
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

        /// <summary>
        /// Fetches the rate details.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchRateDetails()
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append(" select ");
            strSQL.Append(" '' MIN_RATE, '' RATE_KG, '' RATE_CBM,'' RATE_PALLETE ");
            strSQL.Append(" FROM DUAL");
            try
            {
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

        #endregion "FetchContainers"

        #region "FetchTRN"

        /// <summary>
        /// Fetches the TRN.
        /// </summary>
        /// <param name="PKValue">The pk value.</param>
        /// <param name="dsactive">The dsactive.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchTRN(string PKValue, DataSet dsactive, int BizType = 2)
        {
            string strSql = null;
            int I = 0;
            strSql = string.Empty;
            strSql = "SELECT ROWNUM SR_NO,Q.* FROM (SELECT  ICD_PORT, DECODE(Period,1,'First',2,'Next',3,'Thereafter') Period, DAYS, ";

            if ((dsactive != null))
            {
                for (I = 0; I <= dsactive.Tables[0].Rows.Count - 1; I++)
                {
                    strSql += "SUM(CTPK" + I + ") " + "CTPK" + I + ", ";
                    strSql += "SUM(PK" + I + ") " + "PK" + I + ", ";
                    strSql += "SUM(RATE" + I + ") " + "RATE" + I + ", ";
                }
                strSql = strSql.Substring(1, strSql.Length - 2) + " ";
            }
            strSql += ", DEL ,'' ID, STATUS,CHEFLAG,det_slab_hdr_fk ";

            if ((dsactive != null))
            {
                strSql += " FROM (SELECT ceil(ROWNUM/" + dsactive.Tables[0].Rows.Count + ") sr_no,T.ICD_PORT,t.det_slab_hdr_fk,t.Period,t.Days, ";
                for (I = 0; I <= dsactive.Tables[0].Rows.Count - 1; I++)
                {
                    strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsactive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.CONTAINER_TYPE_MST_FK, NULL) CTPK" + I + ", ";
                    strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsactive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.det_slab_trn_pk, NULL) PK" + I + ", ";
                    strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsactive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",nvl(T.slab_rate,0), NULL) RATE" + I + ", ";
                }
            }
            else
            {
                strSql += " FROM (SELECT ROWNUM sr_no,t.det_slab_hdr_fk,t.Period,t.Days, ";
            }
            strSql += " '' Del, 'A' Status, '' CheFlag ";
            strSql += " FROM  det_slab_trn t ";
            strSql += " WHERE   t.det_slab_hdr_fk = " + PKValue;
            strSql += " ) GROUP BY ICD_PORT,det_slab_hdr_fk, PERIOD, DAYS,DEL, ";
            strSql += "  STATUS,CHEFLAG ";
            //If BizType = 2 Then
            strSql += " ORDER BY ICD_PORT,PERIOD,DAYS)Q";
            //Else
            //    strSql &= " ORDER BY PERIOD)Q"
            //End If

            WorkFlow objWF = new WorkFlow();
            return objWF.GetDataSet(strSql);
        }

        #endregion "FetchTRN"

        #region "FetchHeader"

        /// <summary>
        /// Fetches the header.
        /// </summary>
        /// <param name="dsActive">The ds active.</param>
        /// <returns></returns>
        public DataSet FetchHeader(DataSet dsActive)
        {
            string strSql = null;
            string strDDlchargeCode = null;
            long lngCharge = 0;
            string strddlCurrency = null;
            long lngCurrency = 0;
            string strContainerStatus = null;
            long lngStatus = 0;
            string strToDate = null;
            System.DBNull strNull = null;
            int I = 0;

            strSql = string.Empty;
            strSql = "SELECT SR_NO, '' Port, PERIOD, DAYS, ";

            if ((dsActive != null))
            {
                for (I = 0; I <= dsActive.Tables[0].Rows.Count - 1; I++)
                {
                    strSql += "SUM(CTPK" + I + ") " + "CTPK" + I + ", ";
                    strSql += "SUM(PK" + I + ") " + "PK" + I + ", ";
                    strSql += "SUM(RATE" + I + ") " + "RATE" + I + ", ";
                }
                strSql = strSql.Substring(1, strSql.Length - 2) + " ";
            }
            strSql += ",DEL";
            strSql += ", '' ID,'A' STATUS,'' CHEFLAG, det_slab_hdr_fk";
            strSql += "  FROM (SELECT ROWNUM SR_NO, T.det_slab_hdr_fk, '' PERIOD, '' DAYS, ";
            if ((dsActive != null))
            {
                for (I = 0; I <= dsActive.Tables[0].Rows.Count - 1; I++)
                {
                    strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsActive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.CONTAINER_TYPE_MST_FK, NULL) CTPK" + I + ", ";
                    strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsActive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.det_slab_trn_pk, NULL) PK" + I + ", ";
                    strSql += " DECODE(T.CONTAINER_TYPE_MST_FK, " + dsActive.Tables[0].Rows[I]["CONTAINER_TYPE_MST_PK"] + ",T.slab_rate, NULL) RATE" + I + ", ";
                }
                strSql = strSql.Substring(1, strSql.Length - 2) + " ";
            }
            strSql += ",'' DEL";
            strSql += " FROM   det_slab_trn T WHERE T.det_slab_hdr_fk=-1 ";
            strSql += " ) GROUP BY SR_NO, det_slab_hdr_fk, PERIOD, DAYS,DEL";

            WorkFlow objWF = new WorkFlow();
            return objWF.GetDataSet(strSql);
        }

        /// <summary>
        /// Fetches the header air.
        /// </summary>
        /// <param name="dsActiveAir">The ds active air.</param>
        /// <returns></returns>
        public DataSet FetchHeaderAir(DataSet dsActiveAir)
        {
            string strSql = null;
            string strDDlchargeCode = null;
            long lngCharge = 0;
            string strddlCurrency = null;
            long lngCurrency = 0;
            string strContainerStatus = null;
            long lngStatus = 0;
            string strToDate = null;
            System.DBNull strNull = null;
            int I = 0;

            strSql = string.Empty;
            strSql = "SELECT SR_NO, '' Port, PERIOD, DAYS, ";

            strSql += " '' MIN_RATE, ";
            strSql += " '' RATE_KG, ";
            strSql += " '' RATE_CBM, ";
            strSql += " '' RATE_PALLETE ";
            strSql += " ,DEL";
            strSql += " , '' ID,'A' STATUS,'' CHEFLAG, det_slab_hdr_fk";
            strSql += "  FROM (SELECT ROWNUM SR_NO, T.det_slab_hdr_fk, '' PERIOD, '' DAYS, ";
            strSql += " '' MIN_RATE, ";
            strSql += " '' RATE_KG, ";
            strSql += " '' RATE_CBM, ";
            strSql += " '' RATE_PALLETE ";
            strSql += ",'' DEL";
            strSql += " FROM det_slab_trn T WHERE T.det_slab_hdr_fk=-1 )";

            WorkFlow objWF = new WorkFlow();
            return objWF.GetDataSet(strSql);
        }

        #endregion "FetchHeader"

        #region "FetchData"

        /// <summary>
        /// Fetches the holiday.
        /// </summary>
        /// <param name="hdrpk">The HDRPK.</param>
        /// <param name="from">From.</param>
        /// <param name="todt">The todt.</param>
        /// <returns></returns>
        public DataSet FetchHoliday(string hdrpk, string @from = "", string todt = "")
        {
            string strSql = null;
            string str = null;
            int TotalRecords = 0;
            int start = 0;
            int last = 0;
            WorkFlow objWF = new WorkFlow();
            DataSet ds = null;
            strSql = string.Empty;
            if (!string.IsNullOrEmpty(hdrpk))
            {
                strSql = "SELECT ROWNUM SLNR,q.* FROM (SELECT DSC.DET_SLAB_CALENDER_PK, DSC.HOLIDAY_DATE, DSC.HOLIDAY_DESC, '' SEL FROM DET_SLAB_CALENDER DSC WHERE DSC.DET_SLAB_HDR_FK=" + hdrpk + " ORDER BY DSC.HOLIDAY_DATE)q";
            }
            else
            {
                System.DateTime dt = default(System.DateTime);
                if (!string.IsNullOrEmpty(@from) & !string.IsNullOrEmpty(todt))
                {
                    strSql = "SELECT ROWNUM SLNR,q.* FROM (SELECT '' DET_SLAB_CALENDER_PK, DHC.HOLIDAY_DATE, DHC.HOLIDAY_DESC,'' SEL FROM DET_HOLIDAY_CALENDER DHC WHERE TO_DATE(DHC.HOLIDAY_DATE, dateformat) BETWEEN TO_DATE('" + @from + "', dateformat) AND TO_DATE('" + todt + "', dateformat) ORDER BY DHC.HOLIDAY_DATE)q";
                }
                else if (!string.IsNullOrEmpty(@from))
                {
                    strSql = "SELECT ROWNUM SLNR,q.* FROM (SELECT '' DET_SLAB_CALENDER_PK, DHC.HOLIDAY_DATE, DHC.HOLIDAY_DESC,'' SEL FROM DET_HOLIDAY_CALENDER DHC WHERE TO_DATE(DHC.HOLIDAY_DATE, dateformat) >= TO_DATE('" + @from + "', dateformat) ORDER BY DHC.HOLIDAY_DATE)q";
                }
                else
                {
                    strSql = "SELECT ROWNUM SLNR,q.* FROM (SELECT '' DET_SLAB_CALENDER_PK, DHC.HOLIDAY_DATE, DHC.HOLIDAY_DESC,'' SEL FROM DET_HOLIDAY_CALENDER DHC WHERE TO_DATE(DHC.HOLIDAY_DATE, dateformat) BETWEEN TO_DATE('01/09/2000', dateformat) AND TO_DATE('01/10/2000', dateformat) ORDER BY DHC.HOLIDAY_DATE)q";
                }
            }
            return objWF.GetDataSet(strSql);
        }

        #endregion "FetchData"

        #region "Fetch Currency"

        /// <summary>
        /// Fetches the currency.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCurrency()
        {
            string StrSQL = null;
            try
            {
                StrSQL += "SELECT 0 CURRENCY_MST_PK, ' ' CURRENCY_ID ";
                StrSQL += " FROM DUAL ";
                StrSQL += " UNION ";
                StrSQL += " SELECT C.CURRENCY_MST_PK, C.CURRENCY_ID FROM CURRENCY_TYPE_MST_TBL C";
                StrSQL += " where c.active_flag=1  ORDER BY currency_id";
                WorkFlow objWF = new WorkFlow();
                DataSet objDS = null;
                objDS = objWF.GetDataSet(StrSQL);
                return objDS;
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

        /// <summary>
        /// Fetchcurrencies the specified locpk.
        /// </summary>
        /// <param name="locpk">The locpk.</param>
        /// <returns></returns>
        public string fetchcurrency(string locpk)
        {
            WorkFlow objWF = new WorkFlow();
            string strSql = null;

            strSql = string.Empty;
            strSql += "select currency_mst_pk from currency_type_mst_tbl curr,country_mst_tbl cont,location_mst_tbl loc";
            strSql += " where cont.currency_mst_fk=curr.currency_mst_pk  ";
            strSql += " and loc.country_mst_fk=cont.country_mst_pk and loc.location_mst_pk= " + locpk;

            try
            {
                if (string.IsNullOrEmpty(objWF.ExecuteScaler(strSql)))
                    return "0";
                return objWF.ExecuteScaler(strSql);
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

        #endregion "Fetch Currency"

        #region "Fetch HeaderDetail"

        /// <summary>
        /// Fetches the header detail.
        /// </summary>
        /// <param name="Pk_Value">The PK_ value.</param>
        /// <returns></returns>
        public DataSet FetchHeaderDetail(string Pk_Value)
        {
            string StrSQL = null;
            try
            {
                StrSQL = string.Empty;
                StrSQL += " SELECT Cu.Currency_Mst_Pk, t.valid_from, t.valid_to,t.slab_date,t.reference_nr, t.apply_freedays,t.freedays,t.weekends";
                StrSQL += "   ,t.process,T.TARIFF_TYPE,PMT.PORT_MST_PK,PMT.PORT_ID,PMT.PORT_NAME,t.biz_type,t.cargo_type,t.comm_grp FROM  det_slab_hdr t, Currency_Type_Mst_Tbl Cu, PORT_MST_TBL PMT ";
                StrSQL += "   WHERE t.currency_mst_fk = Cu.Currency_Mst_Pk ";
                StrSQL += "         AND t.det_slab_hdr_pk = " + Pk_Value;
                StrSQL += "        AND t.PORT_MST_FK=PMT.PORT_MST_PK(+) ";
                WorkFlow objWf = new WorkFlow();
                return objWf.GetDataSet(StrSQL);
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

        #endregion "Fetch HeaderDetail"

        #region "Check For Existing Records"

        /// <summary>
        /// HDRs the already exists clashing_without to dt.
        /// </summary>
        /// <param name="PortPK">The port pk.</param>
        /// <param name="ContainerPK">The container pk.</param>
        /// <param name="ValidFrom">The valid from.</param>
        /// <param name="ValidTo">The valid to.</param>
        /// <param name="form_flag">The form_flag.</param>
        /// <param name="pk">The pk.</param>
        /// <param name="process">The process.</param>
        /// <param name="terminal">The terminal.</param>
        /// <returns></returns>
        public long HDRAlreadyExistsClashing_withoutToDt(int PortPK, int ContainerPK, System.DateTime ValidFrom, string ValidTo, string form_flag, int pk, string process, int terminal = 0)
        {
            string strSql = null;
            DataSet TempDs = null;
            DataSet TempDs1 = null;
            long HDRPK = 0;
            if (ValidTo.Trim().Trim().Length > 0)
            {
                strSql = string.Empty;
                strSql += " select DISTINCT t.det_slab_hdr_pk ";
                strSql += " from   det_slab_hdr t, det_slab_trn dt ";
                strSql += " where dt.det_slab_hdr_fk=t.det_slab_hdr_pk AND t.port_mst_fk = " + PortPK + " And dt.container_type_mst_fk = " + ContainerPK + "";

                strSql += " and (( to_date('" + ValidFrom + "') >= t.valid_from ";
                strSql += " and t.valid_to is null ) or ";
                strSql += " (to_date('" + ValidTo + "') between t.valid_from and t.valid_to  ";
                strSql += " ))  AND t.form_flag=" + form_flag;
                strSql += "  AND t.process=" + process;
            }
            else
            {
                strSql = string.Empty;
                strSql += " select DISTINCT t.det_slab_hdr_pk ";
                strSql += " from   det_slab_hdr t, det_slab_trn dt ";
                strSql += " where dt.det_slab_hdr_fk=t.det_slab_hdr_pk AND t.port_mst_fk = " + PortPK + " And dt.container_type_mst_fk = " + ContainerPK + "";

                strSql += " and ( to_date('" + ValidFrom + "') >= t.valid_from ";
                strSql += " and t.valid_to is null )  AND t.form_flag=" + form_flag;
                strSql += "  AND t.process=" + process;
            }
            if (pk != 0)
            {
                strSql += "   AND t.det_slab_hdr_pk<>" + pk;
            }
            WorkFlow objWF = new WorkFlow();
            TempDs = objWF.GetDataSet(strSql);
            if (TempDs.Tables[0].Rows.Count > 0)
            {
                if (TempDs.Tables[0].Rows.Count > 0)
                {
                    return -1;
                }
                else
                {
                    return 0;
                }
            }
            return 0;
        }

        #endregion "Check For Existing Records"

        #region "Region ContainerId"

        /// <summary>
        /// Fetches the container identifier.
        /// </summary>
        /// <param name="containerPk">The container pk.</param>
        /// <returns></returns>
        public object FetchContainerId(int containerPk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("");
            sb.Append(" SELECT CT.CONTAINER_TYPE_MST_ID CONTAINER_ID ");
            sb.Append("  FROM CONTAINER_TYPE_MST_TBL CT ");
            sb.Append(" WHERE CT.CONTAINER_TYPE_MST_PK =");
            sb.Append(containerPk);

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Region ContainerId"

        #region "DeleteSlabTRN.."

        /// <summary>
        /// Deletes the holiday.
        /// </summary>
        /// <param name="DET_SLAB_CALENDER_PK_IN">The de t_ sla b_ calende r_ p k_ in.</param>
        /// <param name="DELETED_BY_FK_IN">The delete d_ b y_ f k_ in.</param>
        /// <param name="CONFIG_MST_FK_IN">The confi g_ ms t_ f k_ in.</param>
        /// <returns></returns>
        public ArrayList DeleteHoliday(string DET_SLAB_CALENDER_PK_IN, long DELETED_BY_FK_IN, long CONFIG_MST_FK_IN)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            OracleCommand delCommand = new OracleCommand();
            Int16 RETURN_VALUE = default(Int16);
            Array a = null;
            if (!string.IsNullOrEmpty(DET_SLAB_CALENDER_PK_IN))
            {
                a = DET_SLAB_CALENDER_PK_IN.Split(',');
            }
            Int16 i = default(Int16);
            for (i = 0; i <= a.Length - 1; i++)
            {
                try
                {
                    var _with1 = delCommand;
                    _with1.Connection = objWK.MyConnection;
                    _with1.CommandType = CommandType.StoredProcedure;
                    _with1.CommandText = objWK.MyUserName + ".det_slab_calender_pkg.det_slab_calender_del";
                    var _with2 = _with1.Parameters;
                    _with2.Clear();
                    delCommand.Parameters.Add("DET_SLAB_CALENDER_PK_IN", (i)).Direction = ParameterDirection.Input;
                    delCommand.Parameters.Add("DELETED_BY_FK_IN", DELETED_BY_FK_IN).Direction = ParameterDirection.Input;
                    delCommand.Parameters.Add("CONFIG_MST_FK_IN", CONFIG_MST_FK_IN).Direction = ParameterDirection.Input;
                    delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with3 = objWK.MyDataAdapter;
                    _with3.DeleteCommand = delCommand;
                    _with3.DeleteCommand.Transaction = TRAN;
                    _with3.DeleteCommand.ExecuteNonQuery();
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                }
            }
            if (arrMessage.Count > 0)
            {
                TRAN.Rollback();
            }
            else
            {
                TRAN.Commit();
            }
            return arrMessage;
        }

        #endregion "DeleteSlabTRN.."

        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_TariffRefNo">The p_ tariff reference no.</param>
        /// <param name="P_Port">The p_ port.</param>
        /// <param name="fromdate">The fromdate.</param>
        /// <param name="todate">The todate.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="LogInLocFK">The log in loc fk.</param>
        /// <param name="IsAdmin">if set to <c>true</c> [is admin].</param>
        /// <param name="ChkONLD">The CHK onld.</param>
        /// <param name="form_flag">The form_flag.</param>
        /// <param name="process">The process.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <returns></returns>
        public DataSet FetchAll(string P_TariffRefNo = "", string P_Port = "", string fromdate = "", string todate = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string LogInLocFK = "0", bool IsAdmin = false, Int32 ChkONLD = 0, string form_flag = "",
        int process = 0, int BizType = 0, int CargoType = 0, int CommGrp = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strSQL1 = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            strCondition = strCondition + " AND T.FORM_FLAG=" + form_flag;
            if (P_Port.ToString().Trim().Length > 0)
            {
                strCondition = strCondition + " AND T.PORT_MST_FK = '" + P_Port + "' ";
            }
            if (process != 0)
            {
                strCondition = strCondition + " AND T.PROCESS=" + process;
            }
            if (BizType != 0)
            {
                strCondition = strCondition + " AND T.BIZ_TYPE=" + BizType;
            }
            if (CommGrp != 0)
            {
                strCondition = strCondition + " AND T.COMM_GRP=" + CommGrp;
            }

            if (CargoType != 0)
            {
                strCondition = strCondition + " AND T.CARGO_TYPE=" + CargoType;
                strCondition = strCondition + " AND T.BIZ_TYPE= 2";
            }
            if (fromdate.Trim().Length > 0)
            {
                strCondition = strCondition + " AND TO_DATE(T.SLAB_DATE,DATEFORMAT) >= TO_DATE(' " + fromdate + " ',DATEFORMAT)  ";
            }
            if (todate.Trim().Length > 0)
            {
                strCondition = strCondition + " AND TO_DATE(T.SLAB_DATE,DATEFORMAT) <= TO_DATE(' " + todate + " ',DATEFORMAT)  ";
            }
            if (P_TariffRefNo.ToString().Trim().Length > 0)
            {
                strCondition = strCondition + " AND UPPER(T.REFERENCE_NR) LIKE  '%" + P_TariffRefNo.ToUpper() + "%' ";
            }
            if (ChkONLD == 0)
            {
                strCondition = strCondition + " AND 1 = 2 ";
            }

            strSQL += " SELECT DISTINCT DECODE(T.BIZ_TYPE, 1, 'AIR', 'SEA') BIZ_TYPE,T.REFERENCE_NR,TO_DATE(T.SLAB_DATE,'" + HttpContext.Current.Session["DATE_FORMAT"] + "') TARIFF_DATE, ";
            strSQL += " DECODE(T.PROCESS,1,'EXPORT','IMPORT') PROCESS,";
            strSQL += " T.PORT_ID, ";
            strSQL += " CASE WHEN T.COMM_GRP=0 THEN '<ALL>' ELSE T.COMMODITY_GROUP_CODE END COMMODITY_GROUP_CODE, ";
            strSQL += " T.CURRENCY_ID,TO_DATE(T.VALID_FROM,'" + HttpContext.Current.Session["DATE_FORMAT"] + "') VALID_FROM, TO_DATE(T.VALID_TO,'" + HttpContext.Current.Session["DATE_FORMAT"] + "') VALID_TO, T.DET_SLAB_HDR_PK";
            strSQL += " FROM  VIEW_DETENTION_LIST T WHERE 1= 1";
            strSQL += strCondition;

            strSQL1 = "SELECT COUNT(*) from (";
            strSQL1 += strSQL.ToString() + ")";
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
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
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;
            strSQL1 = "";
            strSQL1 = " SELECT * FROM (SELECT ROWNUM SR_NO, Q.* FROM(";
            strSQL1 += strSQL.ToString();
            strSQL1 += "ORDER BY TO_DATE(TARIFF_DATE,DATEFORMAT) DESC,T.REFERENCE_NR DESC";
            strSQL1 += " ) Q) WHERE SR_NO  BETWEEN " + start + " AND " + last;

            try
            {
                return objWF.GetDataSet(strSQL1.ToString());
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
            return new DataSet();
        }

        #endregion "Fetch Function"

        #region "Function to check whether a user is an administrator or not"

        /// <summary>
        /// Determines whether the specified string user identifier is administrator.
        /// </summary>
        /// <param name="strUserID">The string user identifier.</param>
        /// <returns></returns>
        public int IsAdministrator(string strUserID)
        {
            string strSQL = null;
            Int16 Admin = default(Int16);
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
            strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
            strSQL = strSQL + "AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
            try
            {
                Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));
                if (Admin == 1)
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
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

        #endregion "Function to check whether a user is an administrator or not"

        #region "Generate Protocol"

        /// <summary>
        /// Generates the protocol.
        /// </summary>
        /// <param name="LocPk">The loc pk.</param>
        /// <param name="Emp_Pk">The emp_ pk.</param>
        /// <param name="userid">The userid.</param>
        /// <param name="STR">The string.</param>
        /// <returns></returns>
        public string GenerateProtocol(long LocPk, long Emp_Pk, long userid, string STR)
        {
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            return GenerateProtocolKey(STR, LocPk, Emp_Pk, DateTime.Now, "", "", "", userid) + "";
        }

        #endregion "Generate Protocol"
    }
}