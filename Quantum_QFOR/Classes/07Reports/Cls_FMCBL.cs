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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_FMCBL : CommonFeatures
    {
        #region "Enhance Search & Lookup Search Block FOR EXP FMC HBL & JobCARD"

        /// <summary>
        /// Fetches for HBL reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForHblRef(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            int strBUSINESS_MODEL_IN = 0;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            if (arr.Length > 2)
                strBUSINESS_MODEL_IN = Convert.ToInt32(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EXP_FMC_HBL_REF_NO_PKG.GET_HBL_REF_COMMON";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBUSINESS_MODEL_IN).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_EXP_FMC_JOB_REF_NO_PKG.GET_JOB_REF_COMMON";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
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
                SCM.Connection.Close();
            }
        }

        /// <summary>
        /// Fetches for vessel.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForVESSEL(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            int strBUSINESS_MODEL_IN = 0;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            if (arr.Length > 2)
                strBUSINESS_MODEL_IN = Convert.ToInt32(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EXP_FMC_HBL_REF_NO_PKG.GET_HBL_VESSEL_COMMON";

                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBUSINESS_MODEL_IN).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 20000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion "Enhance Search & Lookup Search Block FOR EXP FMC HBL & JobCARD"

        #region "Fetch Data"

        /// <summary>
        /// Fetches the FMC sea user export.
        /// </summary>
        /// <param name="VslVoy">The VSL voy.</param>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="ShipperPk">The shipper pk.</param>
        /// <param name="HBLPk">The HBL pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="depDate">The dep date.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public DataSet FetchFMCSeaUserExport(string VslVoy = "", long JobPk = 0, long ShipperPk = 0, long HBLPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, string depDate = "", Int32 flag = 0, int loc = 0)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (VslVoy.Trim().Length > 0)
            {
                strCondition = strCondition + " AND (H.VESSEL_NAME ||" + "'-'" + " || H.VOYAGE) = '" + VslVoy + "'";
            }

            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JS.JOB_CARD_TRN_PK = " + JobPk;
            }

            if (ShipperPk > 0)
            {
                strCondition = strCondition + " And H.SHIPPER_CUST_MST_FK = " + ShipperPk;
            }

            if (HBLPk > 0)
            {
                strCondition = strCondition + " And H.HBL_EXP_TBL_PK = " + HBLPk;
            }

            if (!string.IsNullOrEmpty(depDate))
            {
                strCondition += "                 AND TO_DATE(JS.DEPARTURE_DATE,'dd/MM/yyyy') = TO_DATE('" + depDate + "','" + dateFormat + "')";
            }
            else
            {
                strCondition += "                 AND (to_date(JS.DEPARTURE_DATE,'" + dateFormat + "') <= to_date(SYSDATE,'" + dateFormat + "') OR";
                strCondition += "                      JS.DEPARTURE_DATE IS NULL)";
            }
            strCondition += "                 AND UMT.DEFAULT_LOCATION_FK =" + loc;
            Strsql = "";
            Strsql += "             SELECT COUNT(*)";
            Strsql += "             FROM JOB_CARD_TRN JS,";
            Strsql += "                 BOOKING_MST_TBL      BS,";
            Strsql += "                 HBL_EXP_TBL          H,";
            Strsql += "                 CUSTOMER_MST_TBL     S,";
            Strsql += "                 CUSTOMER_MST_TBL     C,";
            Strsql += "                 PORT_MST_TBL PORT,";
            Strsql += "                 USER_MST_TBL UMT";
            Strsql += "             WHERE H.JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK";
            Strsql += "                 AND JS.BOOKING_MST_FK = BS.BOOKING_MST_PK";
            Strsql += "                 AND H.CONSIGNEE_CUST_MST_FK = C.CUSTOMER_MST_PK(+)";
            Strsql += "                 AND H.SHIPPER_CUST_MST_FK = S.CUSTOMER_MST_PK(+)";
            Strsql += "                 AND BS.PORT_MST_POD_FK = PORT.PORT_MST_PK";
            Strsql += "                 AND UMT.USER_MST_PK = JS.CREATED_BY_FK";
            Strsql += "                 AND PORT.PORT_ID LIKE 'US%'";
            Strsql += "                 AND JS.JOB_CARD_STATUS = 1";

            Strsql += strCondition;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(Strsql));
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

            Strsql = "";
            Strsql += "select *";
            Strsql += "     from (SELECT ROWNUM AS SLNO, Q.*";
            Strsql += "         FROM (SELECT JS.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += "                 JS.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += "                 H.HBL_EXP_TBL_PK AS HBL_PK,";
            Strsql += "                 H.HBL_REF_NO AS HBL_REFNO,";
            Strsql += "                 TO_CHAR(H.HBL_DATE, '" + dateFormat + "') AS HBL_REFDT,";
            Strsql += "                 BS.BOOKING_MST_PK AS BKG_PK,";
            Strsql += "                 BS.BOOKING_REF_NO AS BKG_REFNO,";
            Strsql += "                 S.CUSTOMER_NAME AS SHIPPER,";
            Strsql += "                 C.CUSTOMER_NAME AS CONSIGNEE,";
            Strsql += "                 (H.VESSEL_NAME || '-' || H.VOYAGE) AS VSL_VOY,";
            Strsql += "                 TO_CHAR(H.DEPARTURE_DATE,'" + dateFormat + "') AS DEP_DATE,";
            Strsql += "                 '' AS SEL";
            Strsql += "             FROM JOB_CARD_TRN JS,";
            Strsql += "                 BOOKING_MST_TBL      BS,";
            Strsql += "                 HBL_EXP_TBL          H,";
            Strsql += "                 CUSTOMER_MST_TBL     S,";
            Strsql += "                 CUSTOMER_MST_TBL     C,";
            Strsql += "                 PORT_MST_TBL PORT,";
            Strsql += "                 USER_MST_TBL UMT";
            Strsql += "             WHERE H.JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK";
            Strsql += "                 AND JS.BOOKING_MST_FK = BS.BOOKING_MST_PK";
            Strsql += "                 AND H.CONSIGNEE_CUST_MST_FK = C.CUSTOMER_MST_PK(+)";
            Strsql += "                 AND H.SHIPPER_CUST_MST_FK = S.CUSTOMER_MST_PK(+)";
            Strsql += "                 AND BS.PORT_MST_POD_FK = PORT.PORT_MST_PK";
            Strsql += "                 AND UMT.USER_MST_PK = JS.CREATED_BY_FK";
            Strsql += "                 AND PORT.PORT_ID LIKE 'US%'";
            Strsql += "                 AND JS.JOB_CARD_STATUS = 1";
            Strsql += strCondition;
            Strsql += " ORDER BY H.DEPARTURE_DATE DESC,JS.JOBCARD_REF_NO DESC )q)";
            Strsql += "     WHERE SLNO Between " + start + " and " + last;

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            
        }

        #endregion "Fetch Data"

        #region "Fetch Main"

        /// <summary>
        /// Fetches the FMC main.
        /// </summary>
        /// <param name="HblPk">The HBL pk.</param>
        /// <returns></returns>
        public DataSet FetchFMCMain(string HblPk)
        {
            string Strsql = "";
            WorkFlow ObjWk = new WorkFlow();

            Strsql += "SELECT H.HBL_EXP_TBL_PK,";
            Strsql += "     H.HBL_REF_NO AS BILLOFLADING,";
            Strsql += "     H.HBL_DATE,";
            Strsql += "     BS.BOOKING_MST_PK BOOKING_SEA_PK,";
            Strsql += "     BS.BOOKING_REF_NO AS BOOKINGNO,";
            Strsql += "     JS.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_PK,";
            Strsql += "     JS.JOBCARD_REF_NO AS EXPORTREF,";
            Strsql += "     SHP.CUSTOMER_NAME AS SHIPPER,";
            Strsql += "     SHPDET.ADM_ADDRESS_1 AS SHPADDR1,";
            Strsql += "     SHPDET.ADM_ADDRESS_2 AS SHPADDR2,";
            Strsql += "     SHPDET.ADM_ADDRESS_3 AS SHPADDR3,";
            Strsql += "     SHPDET.ADM_PHONE_NO_1 AS SHPPHONE,";
            Strsql += "     SHPDET.ADM_FAX_NO     AS SHIPFAX,";
            Strsql += "     SHPDET.ADM_EMAIL_ID   AS SHIPEMAIL,";
            Strsql += "     SHPDET.ADM_CITY       AS SHIPCITY,";
            Strsql += "     SHPDET.ADM_ZIP_CODE AS SHPZIP,";
            Strsql += "     SHIPCOUNTRY.COUNTRY_NAME AS SHPCOUNTRY,";
            Strsql += "     CON.CUSTOMER_NAME AS CONSIGNEE,";
            Strsql += "     CONDET.ADM_ADDRESS_1 AS CONADDR1,";
            Strsql += "     CONDET.ADM_ADDRESS_2 AS CONADDR2,";
            Strsql += "     CONDET.ADM_ADDRESS_3 AS CONADDR3,";
            Strsql += " CONDET.ADM_PHONE_NO_1 AS CONPHONE,";
            Strsql += "    CONDET.ADM_FAX_NO     AS CONFAX,";
            Strsql += "     CONDET.ADM_EMAIL_ID   AS CONEMAIL,";
            Strsql += "     CONDET.ADM_CITY      AS CONCITY,";
            Strsql += "     CONDET.ADM_ZIP_CODE AS CONZIP,";
            Strsql += "  CONCOUNTRY.COUNTRY_NAME AS CNCOUNTRY,";
            Strsql += "     COP.CORPORATE_NAME,";
            Strsql += "     COP.ADDRESS_LINE1,";
            Strsql += "     COP.CITY,";
            Strsql += "     COP.ADDRESS_LINE2,";
            Strsql += "     COP.ADDRESS_LINE3,";
            Strsql += "     COP.PHONE,";
            Strsql += "     COP.FAX,";
            Strsql += "     COP.FMC_NO,";
            Strsql += "     CO.COUNTRY_NAME,";
            Strsql += "     NOTI.CUSTOMER_NAME AS NOTIFY,";
            Strsql += "     NOTDET.ADM_ADDRESS_1 AS NOTADDR1,";
            Strsql += "     NOTDET.ADM_ADDRESS_2 AS NOTADDR2,";
            Strsql += "     NOTDET.ADM_ADDRESS_3 AS NOTADDR3,";
            Strsql += " NOTDET.ADM_PHONE_NO_1 AS NOTPHONE,";
            Strsql += "      NOTDET.ADM_FAX_NO    AS NOTFAX,";
            Strsql += "      NOTDET.ADM_EMAIL_ID  AS NOTEMAIL,";
            Strsql += "     NOTDET.ADM_CITY      NOTCITY,";
            Strsql += "     NOTDET.ADM_ZIP_CODE AS NOTZIP,";
            Strsql += "     NOTIFYCOUNTRY.COUNTRY_NAME NOTCOUNTRY,";
            Strsql += "     T.TRANSPORTER_NAME AS PRECARRIAGEBY,";
            Strsql += "PMST.PLACE_NAME PLACENAME, ";
            Strsql += "     (H.VESSEL_NAME || '-' || H.VOYAGE) AS EXPORTCARRIER,";
            Strsql += "     PL.PORT_NAME AS POL,";
            Strsql += "     PD.PORT_NAME AS POD,";
            Strsql += "     P.PLACE_NAME AS PLACEDEL,";
            Strsql += "     H.HBL_ORIGINAL_PRINTS NOOFORGINALS,";
            Strsql += "     AGNT.AGENT_NAME,";
            Strsql += "     AGNTCONTACT.ADM_ADDRESS_1 DPADDRESS1,";
            Strsql += "     AGNTCONTACT.ADM_ADDRESS_2 DPADDRESS2,";
            Strsql += "     AGNTCONTACT.ADM_ADDRESS_3 DPADDRESS3,";
            Strsql += " AGNTCONTACT.ADM_PHONE_NO_1 DPPHONE,";
            Strsql += "   AGNTCONTACT.ADM_FAX_NO    DPFAX,";
            Strsql += "   AGNTCONTACT.ADM_EMAIL_ID  DPEMAIL,";
            Strsql += "     AGNTCONTACT.ADM_CITY DPCITY,";
            Strsql += "     AGNTCONTACT.ADM_ZIP_CODE DPZIP,";
            //'
            Strsql += "     POLCOUNTRY.COUNTRY_ID    POLCOUNTRYID,";
            //'
            Strsql += "     AGNTCOUNTRY.COUNTRY_NAME DPCOUNTRY";
            Strsql += "     FROM HBL_EXP_TBL           H,";
            Strsql += "     JOB_CARD_TRN  JS,";
            Strsql += "     BOOKING_MST_TBL       BS,";
            Strsql += "     CUSTOMER_MST_TBL      SHP,";
            Strsql += "     CUSTOMER_CONTACT_DTLS SHPDET,";
            Strsql += "     CUSTOMER_MST_TBL      CON,";
            Strsql += "     CUSTOMER_CONTACT_DTLS CONDET,";
            Strsql += "     CORPORATE_MST_TBL     COP,";
            Strsql += "     COUNTRY_MST_TBL       CO,";
            Strsql += "     CUSTOMER_MST_TBL      NOTI,";
            Strsql += "     CUSTOMER_CONTACT_DTLS NOTDET,";
            Strsql += "     TRANSPORTER_MST_TBL   T,";
            Strsql += "     PORT_MST_TBL          PL,";
            Strsql += "     PORT_MST_TBL          PD,";
            Strsql += "     PLACE_MST_TBL         P,";
            Strsql += "     AGENT_MST_TBL         AGNT,";
            Strsql += "     AGENT_CONTACT_DTLS    AGNTCONTACT,";
            Strsql += "     COUNTRY_MST_TBL       AGNTCOUNTRY,";
            Strsql += "     COUNTRY_MST_TBL NOTIFYCOUNTRY,";
            Strsql += "     COUNTRY_MST_TBL CONCOUNTRY,";
            Strsql += "     COUNTRY_MST_TBL SHIPCOUNTRY,";
            //'
            Strsql += "     COUNTRY_MST_TBL POLCOUNTRY,";
            //'
            Strsql += "     PLACE_MST_TBL PMST";
            Strsql += "     WHERE H.JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK";
            Strsql += "     AND JS.BOOKING_MST_FK = BS.BOOKING_MST_PK";
            Strsql += "     AND H.HBL_EXP_TBL_PK IN(" + HblPk + " )";
            Strsql += "     AND SHP.CUSTOMER_MST_PK(+) = H.SHIPPER_CUST_MST_FK";
            Strsql += "     AND CON.CUSTOMER_MST_PK(+) = H.CONSIGNEE_CUST_MST_FK";
            Strsql += "     AND PL.COUNTRY_MST_FK      = CO.COUNTRY_MST_PK";
            Strsql += "     AND NOTI.CUSTOMER_MST_PK(+) = H.NOTIFY1_CUST_MST_FK";
            Strsql += "     AND T.TRANSPORTER_MST_PK(+) = JS.TRANSPORTER_CARRIER_FK";
            Strsql += "     AND BS.PORT_MST_POD_FK = PD.PORT_MST_PK";
            Strsql += "     AND BS.PORT_MST_POL_FK = PL.PORT_MST_PK";
            Strsql += "     AND P.PLACE_PK(+) = BS.DEL_PLACE_MST_FK";
            Strsql += "     AND SHPDET.CUSTOMER_MST_FK(+) = SHP.CUSTOMER_MST_PK";
            Strsql += "     AND CONDET.CUSTOMER_MST_FK(+) = CON.CUSTOMER_MST_PK";
            Strsql += "     AND NOTDET.CUSTOMER_MST_FK(+) = NOTI.CUSTOMER_MST_PK";
            Strsql += "     AND H.DP_AGENT_MST_FK         = AGNT.AGENT_MST_PK(+)";
            Strsql += "     AND AGNT.AGENT_MST_PK         = AGNTCONTACT.AGENT_MST_FK(+)";
            Strsql += "     AND AGNTCONTACT.ADM_COUNTRY_MST_FK = AGNTCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "     AND NOTDET.ADM_COUNTRY_MST_FK      = NOTIFYCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "    AND SHPDET.ADM_COUNTRY_MST_FK  = SHIPCOUNTRY.COUNTRY_MST_PK(+)";
            Strsql += "    AND CONDET.ADM_COUNTRY_MST_FK  = CONCOUNTRY.COUNTRY_MST_PK(+)";
            //'
            Strsql += "    AND PL.COUNTRY_MST_FK=POLCOUNTRY.COUNTRY_MST_PK";
            //'
            Strsql += "    AND PMST.PLACE_PK(+)=BS.COL_PLACE_MST_FK";
            try
            {
                return ObjWk.GetDataSet(Strsql);
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

        /// <summary>
        /// Fetches the FMC details.
        /// </summary>
        /// <param name="HblPk">The HBL pk.</param>
        /// <returns></returns>
        public DataSet FetchFMCDetails(string HblPk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = "  SELECT H.HBL_EXP_TBL_PK,H.HBL_REF_NO,H.MARKS_NUMBERS,SUM(JTS.PACK_COUNT) AS NoofPackages";
            Strsql += " ,JTS.CONTAINER_TYPE_MST_FK,JTS.CONTAINER_NUMBER,H.GOODS_DESCRIPTION, ";
            Strsql += "  SUM(JTS.VOLUME_IN_CBM) AS MEASUREMENT,SUM(JTS.GROSS_WEIGHT) AS GROSS,JTS.CONTAINER_NUMBER";
            Strsql += "  FROM HBL_EXP_TBL H,JOB_TRN_CONT JTS";
            Strsql += "  WHERE H.HBL_EXP_TBL_PK IN(" + HblPk + ")";
            Strsql += " AND H.JOB_CARD_SEA_EXP_FK=JTS.JOB_CARD_TRN_FK ";
            Strsql += "  GROUP BY JTS.CONTAINER_TYPE_MST_FK,H.HBL_EXP_TBL_PK,";
            Strsql += "  H.HBL_REF_NO,H.MARKS_NUMBERS,H.GOODS_DESCRIPTION,JTS.CONTAINER_NUMBER";

            try
            {
                return ObjWk.GetDataSet(Strsql);
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
        /// Fetches the FMC freight.
        /// </summary>
        /// <param name="HblPk">The HBL pk.</param>
        /// <returns></returns>
        public DataSet FetchFMCFreight(string HblPk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = "  SELECT H.HBL_EXP_TBL_PK,H.HBL_REF_NO,JS.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_PK,";
            Strsql += " JS.JOBCARD_REF_NO,JT.JOB_CARD_TRN_FK JOB_CARD_SEA_EXP_FK, ";
            Strsql += "  FE.FREIGHT_ELEMENT_ID,JT.BASIS AS BASIS, DMT.DIMENTION_ID AS DMID,JT.FREIGHT_AMT AS TOTALAMT,JT.FREIGHT_TYPE, ";
            Strsql += " JT.Rateperbasis AS RATE";
            Strsql += "  FROM JOB_TRN_FD JT,JOB_CARD_TRN JS,HBL_EXP_TBL H,FREIGHT_ELEMENT_MST_TBL FE, dimention_unit_mst_tbl dmt";
            Strsql += " WHERE JS.JOB_CARD_TRN_PK=JT.JOB_CARD_TRN_FK";
            Strsql += "  AND H.HBL_EXP_TBL_PK IN (" + HblPk + ")";
            Strsql += "  AND H.JOB_CARD_SEA_EXP_FK=JS.JOB_CARD_TRN_PK";
            Strsql += "  AND FE.FREIGHT_ELEMENT_MST_PK=JT.FREIGHT_ELEMENT_MST_FK";
            Strsql += "  AND  dmt.dimention_unit_mst_pk(+) = JT.basis";

            Strsql += "  UNION ";

            Strsql += "  SELECT H.HBL_EXP_TBL_PK,H.HBL_REF_NO,JS.JOB_CARD_TRN_PK,";
            Strsql += " JS.JOBCARD_REF_NO,JT.JOB_CARD_TRN_FK, ";
            Strsql += "  FE.FREIGHT_ELEMENT_ID,0 AS BASIS, '' AS DMID,JT.Amount AS TOTALAMT,JT.FREIGHT_TYPE, ";
            Strsql += " 0 AS RATE";
            Strsql += "  FROM JOB_TRN_OTH_CHRG JT,JOB_CARD_TRN JS,HBL_EXP_TBL H,FREIGHT_ELEMENT_MST_TBL FE";
            Strsql += " WHERE JS.JOB_CARD_TRN_PK=JT.JOB_CARD_TRN_FK";
            Strsql += "  AND H.HBL_EXP_TBL_PK IN (" + HblPk + ")";
            Strsql += "  AND H.JOB_CARD_SEA_EXP_FK=JS.JOB_CARD_TRN_PK";
            Strsql += "  AND FE.FREIGHT_ELEMENT_MST_PK=JT.FREIGHT_ELEMENT_MST_FK";

            try
            {
                return ObjWk.GetDataSet(Strsql);
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

        #endregion "Fetch Main"
    }
}