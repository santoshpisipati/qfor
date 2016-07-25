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
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_Merge_Split_Booking : CommonFeatures
    {

        #region "Private Variables"
        public long _PkValueMain;
        private long _PkValueTrans;
        cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
        public string strRet;
        private Int16 Chk_EBK = 0;
        #endregion
        public int CommCnt = 0;

        #region "Constructor"
        public object Fetch_Booking(string Str_Bookingpk = "", string Str_BizType = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (Str_BizType == "1")
                {
                    sb.Append("SELECT 0 BOOKING_MST_PK, '' BOOKING_REF_NO");
                    sb.Append("  FROM DUAL ");
                    sb.Append(" UNION");
                    sb.Append(" SELECT BT.BOOKING_MST_PK,");
                    sb.Append("       BT.BOOKING_REF_NO FROM BOOKING_MST_TBL BT ");
                    if (!string.IsNullOrEmpty(Str_Bookingpk))
                    {
                        sb.Append("  WHERE BT.BOOKING_MST_PK IN  (" + Str_Bookingpk + ")");
                    }
                    sb.Append(" ORDER BY BOOKING_MST_PK ");
                }
                else
                {
                    sb.Append("     SELECT BT.BOOKING_MST_PK,");
                    sb.Append("     BT.BOOKING_REF_NO FROM BOOKING_MST_TBL BT ");
                    if (!string.IsNullOrEmpty(Str_Bookingpk))
                    {
                        sb.Append("  WHERE BT.BOOKING_MST_PK IN  (" + Str_Bookingpk + ")");
                    }
                    sb.Append(" ORDER BY BT.BOOKING_MST_PK ");
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
        #endregion

        #region "GetPort"
        public DataSet GetPort()
        {

            string strSql = null;
            strSql = string.Empty;
            strSql += "SELECT 0 port_mst_pk, " ;
            strSql += "       '<ALL>' port_id " ;
            strSql += "  FROM dual " ;
            strSql += " Union ";
            strSql += "     SELECT port_mst_pk, " ;
            strSql += "       port_id " ;
            strSql += "  FROM port_mst_tbl " ;
            strSql = strSql + " order by port_id";
            WorkFlow objwf = new WorkFlow();
            try
            {
                return objwf.GetDataSet(strSql);
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
        #endregion

        #region " Supporting Function "

        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }

        #endregion

        #region "Vessel Voyage"
        public string FetchVesVoySplitBooking(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strBizType = null;
            string strPol = null;
            string strPod = null;
            string strSDate = null;
            string strVES = null;
            string strVOY = null;
            string strReq = null;
            string strImp = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strPol = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strPod = Convert.ToString(arr.GetValue(3));
            strSDate = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strVES = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strVOY = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strImp = Convert.ToString(arr.GetValue(7));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.VESSEL_VOYAGE_SPLITBOOKING";
                var _with1 = SCM.Parameters;
                _with1.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with1.Add("BIZTYPE_IN", ifDBNull(strBizType)).Direction = ParameterDirection.Input;
                _with1.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with1.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with1.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with1.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with1.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with1.Add("IMPORT_IN", ((strImp == null) ? "" : strImp)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
        public string FetchBookingNrSplitBooking(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strBizType = null;
            string strPol = null;
            string strPod = null;
            string strSDate = null;
            string strVES = null;
            string strVOY = null;
            string strReq = null;
            string strImp = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strPol = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strPod = Convert.ToString(arr.GetValue(3));
            strSDate = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strVES = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strVOY = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strImp = Convert.ToString(arr.GetValue(7));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.BOOKINGNR_SPLITBOOKING";
                var _with2 = SCM.Parameters;
                _with2.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with2.Add("BIZTYPE_IN", ifDBNull(strBizType)).Direction = ParameterDirection.Input;
                _with2.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with2.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with2.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with2.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with2.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with2.Add("IMPORT_IN", ((strImp == null) ? "" : strImp)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                //  .Add("RETURN_VALUE", OracleDbType.NVarChar, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output
                _with2.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
        #endregion

        #region "Fetch"



        public DataSet Fetch_Booking_Transaction(string str_Bookingpk, string All_BkgNr, string Str_BizType = "", int Cargo_Type = 0)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWF = new WorkFlow();
                //If Str_BizType = 2 Then
                sb.Append("SELECT DISTINCT NULL BOOKING_MST_FK,");
                sb.Append("                TRANS_REFERED_FROM,");
                sb.Append("                TRANS_REF_NO,");
                sb.Append("                JT.CONTAINER_TYPE_MST_FK,");
                sb.Append("                SUM(NO_OF_BOXES) NO_OF_BOXES,");
                sb.Append("                SUM(VOLUME_CBM) VOLUME_CBM,");
                sb.Append("                BT.COMMODITY_GROUP_FK,");
                sb.Append("                BT.COMMODITY_MST_FK,");
                sb.Append("                SUM(ALL_IN_TARIFF) ALL_IN_TARIFF,");
                sb.Append("                BASIS,");
                sb.Append("                SUM(QUANTITY) QUANTITY,");
                sb.Append("                SUM(BUYING_RATE) BUYING_RATE,");
                sb.Append("                SUM(JT.GROSS_WEIGHT) GROSS_WEIGHT,");
                sb.Append("                SUM(JT.NET_WEIGHT) NET_WEIGHT,");
                sb.Append("                SUM(JT.CHARGEABLE_WEIGHT) CHARGEABLE_WEIGHT,");
                sb.Append("                SUM(WEIGHT_MT) WEIGHT_MT,");
                sb.Append("                PACK_TYPE_FK,");
                sb.Append("                (SELECT ROWTOCOL('SELECT F.COMMODITY_MST_FKS");
                sb.Append("  FROM BOOKING_TRN F");
                sb.Append(" WHERE  F.BOOKING_MST_FK IN (" + All_BkgNr + ") ");
                sb.Append(" AND F.TRANS_REF_NO = ''' ||");
                sb.Append("                                 BT.TRANS_REF_NO || '''");
                sb.Append(" AND F.CONTAINER_TYPE_MST_FK = ' ||");
                sb.Append("                                 JT.CONTAINER_TYPE_MST_FK || '')");
                sb.Append("                   FROM DUAL) COMMODITY_MST_FKS");
                sb.Append("  FROM BOOKING_TRN BT, JOB_CARD_TRN JCT, JOB_TRN_CONT JT");
                sb.Append(" WHERE BT.BOOKING_MST_FK IN (" + All_BkgNr + ")");
                sb.Append(" AND BT.BOOKING_MST_FK = JCT.BOOKING_MST_FK ");
                sb.Append(" AND JCT.JOB_CARD_TRN_PK = JT.JOB_CARD_TRN_FK ");
                sb.Append(" GROUP BY TRANS_REFERED_FROM,");
                sb.Append("          TRANS_REF_NO,");
                sb.Append("          JT.CONTAINER_TYPE_MST_FK,");
                sb.Append("          VOLUME_CBM,");
                sb.Append("          BT.COMMODITY_GROUP_FK,");
                sb.Append("          BT.COMMODITY_MST_FK,");
                sb.Append("          BASIS,");
                sb.Append("          PACK_TYPE_FK");
                //Else
                //sb.Append(" SELECT * FROM BOOKING_TRN BT WHERE BT.BOOKING_MST_FK =" & str_Bookingpk & "")
                //End If
                //If Str_BizType = "2" Then
                //    If Cargo_Type = 1 Then
                //        sb.Append("SELECT DISTINCT NULL BOOKING_SEA_FK,")
                //        sb.Append("                TRANS_REFERED_FROM,")
                //        sb.Append("                TRANS_REF_NO,")
                //        sb.Append("                CONTAINER_TYPE_MST_FK,")
                //        sb.Append("                SUM(NO_OF_BOXES) NO_OF_BOXES,")
                //        sb.Append("                SUM(VOLUME_CBM) VOLUME_CBM,")
                //        sb.Append("                COMMODITY_GROUP_FK,")
                //        sb.Append("                COMMODITY_MST_FK,")
                //        sb.Append("                SUM(ALL_IN_TARIFF) ALL_IN_TARIFF,")
                //        sb.Append("                BASIS,")
                //        sb.Append("                SUM(QUANTITY) QUANTITY,")
                //        sb.Append("                SUM(BUYING_RATE) BUYING_RATE,")
                //        sb.Append("                SUM(WEIGHT_MT) WEIGHT_MT,")
                //        sb.Append("                PACK_TYPE_FK,")
                //        sb.Append("                (SELECT ROWTOCOL('SELECT F.COMMODITY_MST_FKS")
                //        sb.Append("  FROM BOOKING_TRN_SEA_FCL_LCL F")
                //        sb.Append(" WHERE  F.BOOKING_SEA_FK IN (" & All_BkgNr & ") ")
                //        sb.Append(" AND F.TRANS_REF_NO = ''' ||")
                //        sb.Append("                                 BT.TRANS_REF_NO || '''")
                //        sb.Append(" AND F.CONTAINER_TYPE_MST_FK = ' ||")
                //        sb.Append("                                 BT.CONTAINER_TYPE_MST_FK || '')")
                //        sb.Append("                   FROM DUAL) COMMODITY_MST_FKS")
                //        sb.Append("  FROM BOOKING_TRN_SEA_FCL_LCL BT")
                //        sb.Append(" WHERE BT.BOOKING_SEA_FK IN (" & All_BkgNr & ")")
                //        sb.Append(" GROUP BY TRANS_REFERED_FROM,")
                //        sb.Append("          TRANS_REF_NO,")
                //        sb.Append("          CONTAINER_TYPE_MST_FK,")
                //        sb.Append("          VOLUME_CBM,")
                //        sb.Append("          COMMODITY_GROUP_FK,")
                //        sb.Append("          COMMODITY_MST_FK,")
                //        sb.Append("          BASIS,")
                //        sb.Append("          QUANTITY,")
                //        sb.Append("          BUYING_RATE,")
                //        sb.Append("          WEIGHT_MT,")
                //        sb.Append("          PACK_TYPE_FK")
                //    Else

                //        sb.Append("SELECT DISTINCT NULL BOOKING_SEA_FK,")
                //        sb.Append("                TRANS_REFERED_FROM,")
                //        sb.Append("                TRANS_REF_NO,")
                //        sb.Append("                JT.CONTAINER_TYPE_MST_FK,")
                //        sb.Append("                SUM(NO_OF_BOXES) NO_OF_BOXES,")
                //        sb.Append("                SUM(JT.VOLUME_IN_CBM) VOLUME_CBM,")
                //        sb.Append("                BK.COMMODITY_GROUP_FK,")
                //        sb.Append("                NULL COMMODITY_MST_FK,")
                //        sb.Append("                SUM(ALL_IN_TARIFF) ALL_IN_TARIFF,")
                //        sb.Append("                BASIS,")
                //        sb.Append("                SUM(BT.QUANTITY) QUANTITY,")
                //        sb.Append("                SUM(BUYING_RATE) BUYING_RATE,")
                //        sb.Append("                SUM(JT.GROSS_WEIGHT) GROSS_WEIGHT,")
                //        sb.Append("                SUM(JT.NET_WEIGHT) NET_WEIGHT,")
                //        sb.Append("                SUM(JT.CHARGEABLE_WEIGHT) CHARGEABLE_WEIGHT,")
                //        sb.Append("                BK.PACK_TYP_MST_FK PACK_TYPE_FK,")
                //        sb.Append("                (SELECT ROWTOCOL('SELECT F.COMMODITY_MST_FKS  FROM BOOKING_TRN_SEA_FCL_LCL F  ")
                //        sb.Append(" WHERE  F.BOOKING_SEA_FK IN (" & All_BkgNr & ") ")
                //        sb.Append("                AND F.TRANS_REF_NO = ''' ||")
                //        sb.Append("                                 BT.TRANS_REF_NO || ''' AND F.BASIS = ' ||")
                //        sb.Append("                                 BT.BASIS || '')")
                //        sb.Append("                   FROM DUAL) COMMODITY_MST_FKS")
                //        sb.Append("  FROM BOOKING_TRN_SEA_FCL_LCL BT,")
                //        sb.Append("       BOOKING_SEA_TBL         BK,")
                //        sb.Append("       JOB_CARD_SEA_EXP_TBL    JC,")
                //        sb.Append("       JOB_TRN_SEA_EXP_CONT    JT")
                //        sb.Append(" WHERE BK.BOOKING_SEA_PK = BT.BOOKING_SEA_FK")
                //        sb.Append("   AND JC.BOOKING_SEA_FK = BK.BOOKING_SEA_PK")
                //        sb.Append("   AND JT.JOB_CARD_SEA_EXP_FK = JC.JOB_CARD_SEA_EXP_PK")
                //        sb.Append("   AND BT.BOOKING_SEA_FK IN (" & All_BkgNr & ")")
                //        sb.Append(" GROUP BY TRANS_REFERED_FROM,")
                //        sb.Append("          TRANS_REF_NO,")
                //        sb.Append("          JT.CONTAINER_TYPE_MST_FK,")
                //        sb.Append("          VOLUME_CBM,")
                //        sb.Append("          BK.COMMODITY_GROUP_FK,")
                //        sb.Append("          BASIS,")
                //        sb.Append("          BK.PACK_TYP_MST_FK")
                //    End If

                //Else
                //    sb.Append(" SELECT * FROM BOOKING_TRN_AIR BT WHERE BT.BOOKING_AIR_FK =" & str_Bookingpk & "")
                //End If


                return (objWF.GetDataSet(sb.ToString()));

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
        public DataSet Fetch_CargoDtl(string str_Bookingpk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append(" SELECT JT.JOB_TRN_CONT_PK JOB_TRN_SEA_EXP_CONT_PK,JT.CONTAINER_TYPE_MST_FK,");
                sb.Append("       NULL BOOKING_TRN_CARGO_PK,");
                sb.Append("       NULL BOOKING_TRN_SEA_FK,");
                sb.Append("       JT.PACK_COUNT,");
                sb.Append("       JT.GROSS_WEIGHT,");
                sb.Append("       JT.NET_WEIGHT,");
                sb.Append("       JT.VOLUME_IN_CBM,");
                sb.Append("       '' REMARK,");
                sb.Append("       JT.COMMODITY_MST_FKS COMMODITY_FKS,");
                sb.Append("       JT.CONTAINER_NUMBER,");
                sb.Append("       JT.SEAL_NUMBER");
                sb.Append("  FROM BOOKING_MST_TBL BK, JOB_CARD_TRN JC, JOB_TRN_CONT JT");
                sb.Append(" WHERE JC.BOOKING_MST_FK = BK.BOOKING_MST_PK ");
                sb.Append("   AND JT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
                sb.Append("   AND BK.BOOKING_MST_PK IN (" + str_Bookingpk + ")");
                sb.Append("       ORDER BY JT.CONTAINER_TYPE_MST_FK ");
                return (objWF.GetDataSet(sb.ToString()));
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
        public DataSet Fetch_TotalCargoDtl(string str_Bookingpk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT SUM(JT.PACK_COUNT) PACK_COUNT,");
                sb.Append("       SUM(JT.GROSS_WEIGHT) GROSS_WEIGHT,");
                sb.Append("       SUM(JT.NET_WEIGHT) NET_WEIGHT,");
                sb.Append("       SUM(JT.CHARGEABLE_WEIGHT) CHARGEABLE_WEIGHT,");
                sb.Append("       SUM(JT.VOLUME_IN_CBM) VOLUME_IN_CBM");
                sb.Append("  FROM BOOKING_MST_TBL BK, JOB_CARD_TRN JC, JOB_TRN_CONT JT");
                sb.Append(" WHERE JC.BOOKING_MST_FK = BK.BOOKING_MST_PK");
                sb.Append("   AND JT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("   AND BK.BOOKING_MST_PK IN (" + str_Bookingpk + ")");

                return (objWF.GetDataSet(sb.ToString()));
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
        public DataSet Fetch_CommodityDtl(string str_Bookingpk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT JCT.JOB_TRN_CONT_PK JOB_TRN_SEA_EXP_CONT_PK,");
                sb.Append("       COMM.JOB_TRN_COMMODITY_PK JOBCARD_COMM_DTL_PK,");
                sb.Append("       COMM.COMMODITY_MST_FK,");
                sb.Append("       COMM.PACK_TYPE_FK,");
                sb.Append("       COMM.PACK_COUNT,");
                sb.Append("       COMM.LENGTH, ");
                sb.Append("       COMM.WIDTH, ");
                sb.Append("       COMM.HEIGHT, ");
                sb.Append("       COMM.UOM, ");
                sb.Append("       COMM.WEIGHT_PER_PKG, ");
                sb.Append("       COMM.CALCULATED_WEIGHT, ");
                sb.Append("       COMM.CHARGEABLE_WEIGHT, ");
                sb.Append("       COMM.GROSS_WEIGHT,");
                sb.Append("       COMM.NET_WEIGHT,");
                sb.Append("       COMM.VOLUME_IN_CBM");
                sb.Append("  FROM JOB_TRN_COMMODITY COMM,");
                sb.Append("       JOB_TRN_CONT  JCT,");
                sb.Append("       JOB_CARD_TRN  JC");
                sb.Append(" WHERE JC.JOB_CARD_TRN_PK = JCT.JOB_CARD_TRN_FK ");
                sb.Append("   AND COMM.JOB_TRN_CONT_FK = JCT.JOB_TRN_CONT_PK ");
                sb.Append("   AND JC.BOOKING_MST_FK IN (" + str_Bookingpk + ")");
                return (objWF.GetDataSet(sb.ToString()));
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

        public DataSet Fetch_Booking_Header(string str_BookingRefno, string Str_BizType = "", string Bkg_Fks = "", int Cargo_Type = 0)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWF = new WorkFlow();
                if (Str_BizType == "2")
                {
                    if (Cargo_Type == 1)
                    {
                        sb.Append(" SELECT * FROM BOOKING_MST_TBL BT WHERE BT.BOOKING_REF_NO ='" + str_BookingRefno + "'");
                    }
                    else
                    {
                        sb.Append("SELECT DISTINCT BOOKING_MST_PK,");
                        sb.Append("                BOOKING_REF_NO,");
                        sb.Append("                BOOKING_DATE,");
                        sb.Append("                CUST_CUSTOMER_MST_FK,");
                        sb.Append("                CONS_CUSTOMER_MST_FK,");
                        sb.Append("                SHIPMENT_DATE,");
                        sb.Append("                CARGO_TYPE,");
                        sb.Append("                COL_PLACE_MST_FK,");
                        sb.Append("                COL_ADDRESS,");
                        sb.Append("                DEL_PLACE_MST_FK,");
                        sb.Append("                DEL_ADDRESS,");
                        sb.Append("                CB_AGENT_MST_FK,");
                        sb.Append("                PACK_TYP_MST_FK,");
                        sb.Append("                (SELECT SUM(B.PACK_COUNT)");
                        sb.Append("                   FROM BOOKING_MST_TBL B");
                        sb.Append("                  WHERE B.BOOKING_MST_PK IN (" + Bkg_Fks + ")) PACK_COUNT,");
                        sb.Append("                (SELECT SUM(B.GROSS_WEIGHT)");
                        sb.Append("                   FROM BOOKING_MST_TBL B");
                        sb.Append("                  WHERE B.BOOKING_MST_PK IN (" + Bkg_Fks + ")) GROSS_WEIGHT,");
                        sb.Append("                (SELECT SUM(B.NET_WEIGHT)");
                        sb.Append("                   FROM BOOKING_MST_TBL B");
                        sb.Append("                  WHERE B.BOOKING_MST_PK IN (" + Bkg_Fks + ")) NET_WEIGHT,");
                        sb.Append("                (SELECT SUM(B.VOLUME_IN_CBM)");
                        sb.Append("                   FROM BOOKING_MST_TBL B");
                        sb.Append("                  WHERE B.BOOKING_MST_PK IN (" + Bkg_Fks + ")) VOLUME_IN_CBM,");
                        sb.Append("                LINE_BKG_NO,");
                        sb.Append("                VESSEL_NAME,");
                        sb.Append("                VOYAGE,");
                        sb.Append("                ETA_DATE,");
                        sb.Append("                ETD_DATE,");
                        sb.Append("                CUT_OFF_DATE,");
                        sb.Append("                CREATED_BY_FK,");
                        sb.Append("                CREATED_DT,");
                        sb.Append("                LAST_MODIFIED_BY_FK,");
                        sb.Append("                LAST_MODIFIED_DT,");
                        sb.Append("                VERSION_NO,");
                        sb.Append("                PORT_MST_POD_FK,");
                        sb.Append("                PORT_MST_POL_FK,");
                        sb.Append("                OPERATOR_MST_FK,");
                        sb.Append("                CL_AGENT_MST_FK,");
                        sb.Append("                STATUS,");
                        sb.Append("                CARGO_MOVE_FK,");
                        sb.Append("                PYMT_TYPE,");
                        sb.Append("                (SELECT SUM(B.CHARGEABLE_WEIGHT)");
                        sb.Append("                   FROM BOOKING_MST_TBL B");
                        sb.Append("                  WHERE B.BOOKING_MST_PK IN (" + Bkg_Fks + ")) CHARGEABLE_WEIGHT,");
                        sb.Append("                COMMODITY_GROUP_FK,");
                        sb.Append("                SHIPPING_TERMS_MST_FK,");
                        sb.Append("                CUSTOMER_REF_NO,");
                        sb.Append("                DP_AGENT_MST_FK,");
                        sb.Append("                VESSEL_VOYAGE_FK,");
                        sb.Append("                OPR_UPDATE_STATUS,");
                        sb.Append("                CREDIT_LIMIT,");
                        sb.Append("                CREDIT_DAYS,");
                        sb.Append("                IS_EBOOKING,");
                        sb.Append("                BASE_CURRENCY_FK,");
                        sb.Append("                ROLLOVER_REMARKS,");
                        sb.Append("                ROLLOVER_DATE,");
                        sb.Append("                REASSVESSEL_VOYAGE_FK,");
                        sb.Append("                GOODS_DESCRIPTION,");
                        sb.Append("                MARKS_NUMBERS,");
                        sb.Append("                POO_FK,");
                        sb.Append("                PFD_FK,");
                        sb.Append("                PROFITABILITY_FLAG");
                        sb.Append("  FROM BOOKING_MST_TBL B");
                        sb.Append(" WHERE B.BOOKING_REF_NO = '" + str_BookingRefno + "' ");
                    }
                }
                else
                {
                    sb.Append(" SELECT * FROM BOOKING_MST_TBL BT WHERE BT.BOOKING_REF_NO ='" + str_BookingRefno + "'");
                }

                return (objWF.GetDataSet(sb.ToString()));

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
        public DataSet Fetch_Booking_CargoCalc(string Str_BookingPK, string Str_Selected_BookingPK, string Str_BizType = "")
        {

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                //If Str_BizType = "1" Then
                sb.Append("       SELECT CARGO_NOP,");
                sb.Append("       ROUND(CARGO_LENGTH, 2) CARGO_LENGTH,");
                sb.Append("       ROUND(CARGO_WIDTH, 2) CARGO_WIDTH,");
                sb.Append("       ROUND(CARGO_HEIGHT, 2) CARGO_HEIGHT,");
                sb.Append("       ROUND(CARGO_CUBE, 3) CARGO_CUBE,");
                sb.Append("       ROUND(CARGO_VOLUME_WT, 3) CARGO_VOLUME_WT,");
                sb.Append("       ROUND(CARGO_ACTUAL_WT, 3) CARGO_ACTUAL_WT,");
                sb.Append("       ROUND((CARGO_CUBE * CARGO_DIVISION_FACT * 6000 / CARGO_ACTUAL_WT), 3) CARGO_DENSITY,");
                sb.Append("       CARGO_MEASUREMENT,");
                sb.Append("       CARGO_WEIGHT_IN,");
                sb.Append("       CARGO_DIVISION_FACT");
                sb.Append("       FROM (SELECT CARGO_NOP,");
                sb.Append("               CARGO_LENGTH,");
                sb.Append("               CARGO_WIDTH,");
                sb.Append("               CARGO_HEIGHT,");
                sb.Append("               SUM(CARGO_LENGTH * CARGO_WIDTH * CARGO_HEIGHT * CARGO_NOP) CARGO_CUBE,");
                sb.Append("               SUM(CARGO_LENGTH * CARGO_WIDTH * CARGO_HEIGHT * CARGO_NOP *");
                sb.Append("                   CARGO_DIVISION_FACT) CARGO_VOLUME_WT,");
                sb.Append("               CARGO_ACTUAL_WT,");
                sb.Append("               CARGO_MEASUREMENT,");
                sb.Append("               CARGO_WEIGHT_IN,");
                sb.Append("               CARGO_DIVISION_FACT");
                sb.Append("          FROM (");
                sb.Append("                ");
                sb.Append("                SELECT SUM(CARGOCAL.CARGO_NOP) CARGO_NOP,");
                sb.Append("                        ");
                sb.Append("                        SUM(CASE");
                sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 0 THEN");
                sb.Append("                               CARGOCAL.CARGO_LENGTH");
                sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 1 THEN");
                sb.Append("                               (CARGOCAL.CARGO_LENGTH / 100)");
                sb.Append("                              ELSE");
                sb.Append("                               (CARGOCAL.CARGO_LENGTH / 39.37)");
                sb.Append("                            END) CARGO_LENGTH,");
                sb.Append("                        ");
                sb.Append("                        SUM(CASE");
                sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 0 THEN");
                sb.Append("                               CARGOCAL.CARGO_WIDTH");
                sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 1 THEN");
                sb.Append("                               (CARGOCAL.CARGO_WIDTH / 100)");
                sb.Append("                              ELSE");
                sb.Append("                               (CARGOCAL.CARGO_WIDTH / 39.37)");
                sb.Append("                            END) CARGO_WIDTH,");
                sb.Append("                        ");
                sb.Append("                        SUM(CASE");
                sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 0 THEN");
                sb.Append("                               CARGOCAL.CARGO_HEIGHT");
                sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 1 THEN");
                sb.Append("                               (CARGOCAL.CARGO_HEIGHT / 100)");
                sb.Append("                              ELSE");
                sb.Append("                               (CARGOCAL.CARGO_HEIGHT / 39.37)");
                sb.Append("                            END) CARGO_HEIGHT,");
                sb.Append("                        ");
                sb.Append("                        SUM(CASE");
                sb.Append("                              WHEN CARGOCAL.CARGO_WEIGHT_IN = 0 THEN");
                sb.Append("                               CARGOCAL.CARGO_ACTUAL_WT");
                sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 1 THEN");
                sb.Append("                               (CARGOCAL.CARGO_ACTUAL_WT / 1000)");
                sb.Append("                              ELSE");
                sb.Append("                               (CARGOCAL.CARGO_ACTUAL_WT * 0.45359237)");
                sb.Append("                            END) CARGO_ACTUAL_WT,");
                sb.Append("                        ");
                sb.Append("                        '0' CARGO_MEASUREMENT,");
                sb.Append("                        '0' CARGO_WEIGHT_IN,");
                sb.Append("                        AVG(CARGOCAL.CARGO_DIVISION_FACT) CARGO_DIVISION_FACT");
                sb.Append("                ");
                sb.Append("                  FROM BOOKING_CARGO_CALC CARGOCAL, BOOKING_MST_TBL BST");
                sb.Append("                 WHERE BST.BOOKING_MST_PK = CARGOCAL.BOOKING_MST_FK");
                sb.Append("                   AND BST.BUSINESS_TYPE=" + Str_BizType);
                sb.Append("                   AND BST.BOOKING_MST_PK IN (" + Str_BookingPK + "))");
                sb.Append("                 GROUP BY CARGO_NOP,");
                sb.Append("                  CARGO_LENGTH,");
                sb.Append("                  CARGO_WIDTH,");
                sb.Append("                  CARGO_HEIGHT,");
                sb.Append("                  CARGO_MEASUREMENT,");
                sb.Append("                  CARGO_WEIGHT_IN,");
                sb.Append("                  CARGO_ACTUAL_WT,");
                sb.Append("                  CARGO_DIVISION_FACT)");
                sb.Append("");
                //Else
                //sb.Append("       SELECT CARGO_NOP,")
                //sb.Append("       ROUND(CARGO_LENGTH, 2) CARGO_LENGTH,")
                //sb.Append("       ROUND(CARGO_WIDTH, 2) CARGO_WIDTH,")
                //sb.Append("       ROUND(CARGO_HEIGHT, 2) CARGO_HEIGHT,")
                //sb.Append("       ROUND(CARGO_CUBE, 3) CARGO_CUBE,")
                //sb.Append("       ROUND(CARGO_VOLUME_WT, 3) CARGO_VOLUME_WT,")
                //sb.Append("       ROUND(CARGO_ACTUAL_WT, 3) CARGO_ACTUAL_WT,")
                //sb.Append("       ROUND((CARGO_CUBE * CARGO_DIVISION_FACT * 6000 / CARGO_ACTUAL_WT), 3) CARGO_DENSITY,")
                //sb.Append("       CARGO_MEASUREMENT,")
                //sb.Append("       CARGO_WEIGHT_IN,")
                //sb.Append("       CARGO_DIVISION_FACT")
                //sb.Append("       FROM (SELECT CARGO_NOP,")
                //sb.Append("               CARGO_LENGTH,")
                //sb.Append("               CARGO_WIDTH,")
                //sb.Append("               CARGO_HEIGHT,")
                //sb.Append("               SUM(CARGO_LENGTH * CARGO_WIDTH * CARGO_HEIGHT * CARGO_NOP) CARGO_CUBE,")
                //sb.Append("               SUM(CARGO_LENGTH * CARGO_WIDTH * CARGO_HEIGHT * CARGO_NOP *")
                //sb.Append("                   CARGO_DIVISION_FACT) CARGO_VOLUME_WT,")
                //sb.Append("               CARGO_ACTUAL_WT,")
                //sb.Append("               CARGO_MEASUREMENT,")
                //sb.Append("               CARGO_WEIGHT_IN,")
                //sb.Append("               CARGO_DIVISION_FACT")
                //sb.Append("          FROM (")
                //sb.Append("                ")
                //sb.Append("                SELECT SUM(CARGOCAL.CARGO_NOP) CARGO_NOP,")
                //sb.Append("                        ")
                //sb.Append("                        SUM(CASE")
                //sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 0 THEN")
                //sb.Append("                               CARGOCAL.CARGO_LENGTH")
                //sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 1 THEN")
                //sb.Append("                               (CARGOCAL.CARGO_LENGTH / 100)")
                //sb.Append("                              ELSE")
                //sb.Append("                               (CARGOCAL.CARGO_LENGTH / 39.37)")
                //sb.Append("                            END) CARGO_LENGTH,")
                //sb.Append("                        ")
                //sb.Append("                        SUM(CASE")
                //sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 0 THEN")
                //sb.Append("                               CARGOCAL.CARGO_WIDTH")
                //sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 1 THEN")
                //sb.Append("                               (CARGOCAL.CARGO_WIDTH / 100)")
                //sb.Append("                              ELSE")
                //sb.Append("                               (CARGOCAL.CARGO_WIDTH / 39.37)")
                //sb.Append("                            END) CARGO_WIDTH,")
                //sb.Append("                        ")
                //sb.Append("                        SUM(CASE")
                //sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 0 THEN")
                //sb.Append("                               CARGOCAL.CARGO_HEIGHT")
                //sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 1 THEN")
                //sb.Append("                               (CARGOCAL.CARGO_HEIGHT / 100)")
                //sb.Append("                              ELSE")
                //sb.Append("                               (CARGOCAL.CARGO_HEIGHT / 39.37)")
                //sb.Append("                            END) CARGO_HEIGHT,")
                //sb.Append("                        ")
                //sb.Append("                        SUM(CASE")
                //sb.Append("                              WHEN CARGOCAL.CARGO_WEIGHT_IN = 0 THEN")
                //sb.Append("                               CARGOCAL.CARGO_ACTUAL_WT")
                //sb.Append("                              WHEN CARGOCAL.CARGO_MEASUREMENT = 1 THEN")
                //sb.Append("                               (CARGOCAL.CARGO_ACTUAL_WT / 1000)")
                //sb.Append("                              ELSE")
                //sb.Append("                               (CARGOCAL.CARGO_ACTUAL_WT * 0.45359237)")
                //sb.Append("                            END) CARGO_ACTUAL_WT,")
                //sb.Append("                        ")
                //sb.Append("                        '0' CARGO_MEASUREMENT,")
                //sb.Append("                        '0' CARGO_WEIGHT_IN,")
                //sb.Append("                        AVG(CARGOCAL.CARGO_DIVISION_FACT) CARGO_DIVISION_FACT")
                //sb.Append("                ")
                //sb.Append("                  FROM BOOKING_AIR_CARGO_CALC CARGOCAL, BOOKING_AIR_TBL BST")
                //sb.Append("                 WHERE BST.BOOKING_AIR_PK = CARGOCAL.BOOKING_AIR_FK")
                //sb.Append("                 AND BST.BOOKING_AIR_PK IN (" & Str_BookingPK & "))")
                //sb.Append("                 GROUP BY CARGO_NOP,")
                //sb.Append("                  CARGO_LENGTH,")
                //sb.Append("                  CARGO_WIDTH,")
                //sb.Append("                  CARGO_HEIGHT,")
                //sb.Append("                  CARGO_MEASUREMENT,")
                //sb.Append("                  CARGO_WEIGHT_IN,")
                //sb.Append("                  CARGO_ACTUAL_WT,")
                //sb.Append("                  CARGO_DIVISION_FACT)")
                //sb.Append("")

                //End If

                return (objWF.GetDataSet(sb.ToString()));

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
        public DataSet Fetch_Booking_OtherCharge(string Str_BookingPK, string Str_Selected_BookingPK, string Str_BizType = "")
        {

            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("   SELECT Q.*, QT.F_AMOUNT");
                sb.Append("   FROM (SELECT FD.*");
                sb.Append("          FROM JOB_CARD_TRN JC, JOB_TRN_OTH_CHRG FD");
                sb.Append("         WHERE JC.JOB_CARD_TRN_PK = FD.JOB_CARD_TRN_FK");
                sb.Append("           AND JC.BOOKING_MST_FK IN (" + Str_BookingPK + ")");
                sb.Append("           AND FD.FREIGHT_ELEMENT_MST_FK NOT IN");
                sb.Append("               (SELECT FD1.FREIGHT_ELEMENT_MST_FK");
                sb.Append("                  FROM JOB_CARD_TRN JC1, JOB_TRN_OTH_CHRG FD1");
                sb.Append("                 WHERE JC1.JOB_CARD_TRN_PK = FD1.JOB_CARD_TRN_FK");
                sb.Append("                   AND JC1.BOOKING_MST_FK = " + Str_Selected_BookingPK);
                sb.Append("           AND JC1.BUSINESS_TYPE=" + Str_BizType + ")");
                sb.Append("        ");
                sb.Append("        UNION");
                sb.Append("        ");
                sb.Append("        SELECT FD.*");
                sb.Append("          FROM JOB_CARD_TRN JC, JOB_TRN_OTH_CHRG FD");
                sb.Append("         WHERE JC.JOB_CARD_TRN_PK = FD.JOB_CARD_TRN_FK");
                sb.Append("           AND JC.BOOKING_MST_FK = " + Str_Selected_BookingPK + ") Q,");
                sb.Append("       (SELECT SUM(FD2.AMOUNT *");
                sb.Append("                   GET_EX_RATE(FD2.CURRENCY_MST_FK, 173, SYSDATE)) F_AMOUNT,");
                sb.Append("               FD2.FREIGHT_ELEMENT_MST_FK");
                sb.Append("          FROM JOB_CARD_TRN JC2, JOB_TRN_OTH_CHRG FD2");
                sb.Append("         WHERE JC2.JOB_CARD_TRN_PK = FD2.JOB_CARD_TRN_FK");
                sb.Append("           AND JC2.BOOKING_MST_FK IN (" + Str_BookingPK + ")");
                sb.Append("           AND JC2.BUSINESS_TYPE=" + Str_BizType);
                sb.Append("         GROUP BY FD2.FREIGHT_ELEMENT_MST_FK, FD2.CURRENCY_MST_FK) QT");
                sb.Append("");
                sb.Append("      WHERE QT.FREIGHT_ELEMENT_MST_FK = Q.FREIGHT_ELEMENT_MST_FK");
                sb.Append("");
                //If Str_BizType = "1" Then
                //    sb.Append("   SELECT Q.*, QT.F_AMOUNT")
                //    sb.Append("   FROM (SELECT FD.*")
                //    sb.Append("          FROM JOB_CARD_SEA_EXP_TBL JC, JOB_TRN_SEA_EXP_OTH_CHRG FD")
                //    sb.Append("         WHERE JC.JOB_CARD_SEA_EXP_PK = FD.JOB_CARD_SEA_EXP_FK")
                //    sb.Append("           AND JC.BOOKING_SEA_FK IN (" & Str_BookingPK & ")")
                //    sb.Append("           AND FD.FREIGHT_ELEMENT_MST_FK NOT IN")
                //    sb.Append("               (SELECT FD1.FREIGHT_ELEMENT_MST_FK")
                //    sb.Append("                  FROM JOB_CARD_SEA_EXP_TBL JC1, JOB_TRN_SEA_EXP_OTH_CHRG FD1")
                //    sb.Append("                 WHERE JC1.JOB_CARD_SEA_EXP_PK = FD1.JOB_CARD_SEA_EXP_FK")
                //    sb.Append("                   AND JC1.BOOKING_SEA_FK = " & Str_Selected_BookingPK & ")")
                //    sb.Append("        ")
                //    sb.Append("        UNION")
                //    sb.Append("        ")
                //    sb.Append("        SELECT FD.*")
                //    sb.Append("          FROM JOB_CARD_SEA_EXP_TBL JC, JOB_TRN_SEA_EXP_OTH_CHRG FD")
                //    sb.Append("         WHERE JC.JOB_CARD_SEA_EXP_PK = FD.JOB_CARD_SEA_EXP_FK")
                //    sb.Append("           AND JC.BOOKING_SEA_FK = " & Str_Selected_BookingPK & ") Q,")
                //    sb.Append("       (SELECT SUM(FD2.AMOUNT *")
                //    sb.Append("                   GET_EX_RATE(FD2.CURRENCY_MST_FK, 173, SYSDATE)) F_AMOUNT,")
                //    sb.Append("               FD2.FREIGHT_ELEMENT_MST_FK")
                //    sb.Append("          FROM JOB_CARD_SEA_EXP_TBL JC2, JOB_TRN_SEA_EXP_OTH_CHRG FD2")
                //    sb.Append("         WHERE JC2.JOB_CARD_SEA_EXP_PK = FD2.JOB_CARD_SEA_EXP_FK")
                //    sb.Append("           AND JC2.BOOKING_SEA_FK IN (" & Str_BookingPK & ")")
                //    sb.Append("         GROUP BY FD2.FREIGHT_ELEMENT_MST_FK, FD2.CURRENCY_MST_FK) QT")
                //    sb.Append("")
                //    sb.Append("      WHERE QT.FREIGHT_ELEMENT_MST_FK = Q.FREIGHT_ELEMENT_MST_FK")
                //    sb.Append("")
                //Else
                //    sb.Append("   SELECT Q.*, QT.F_AMOUNT")
                //    sb.Append("   FROM (SELECT FD.*")
                //    sb.Append("          FROM JOB_CARD_AIR_EXP_TBL JC, JOB_TRN_AIR_EXP_OTH_CHRG FD")
                //    sb.Append("         WHERE JC.JOB_CARD_AIR_EXP_PK = FD.JOB_CARD_AIR_EXP_FK")
                //    sb.Append("           AND JC.BOOKING_AIR_FK IN (" & Str_BookingPK & ")")
                //    sb.Append("           AND FD.FREIGHT_ELEMENT_MST_FK NOT IN")
                //    sb.Append("               (SELECT FD1.FREIGHT_ELEMENT_MST_FK")
                //    sb.Append("                  FROM JOB_CARD_AIR_EXP_TBL JC1, JOB_TRN_AIR_EXP_OTH_CHRG FD1")
                //    sb.Append("                 WHERE JC1.JOB_CARD_AIR_EXP_PK = FD1.JOB_CARD_AIR_EXP_FK")
                //    sb.Append("                   AND JC1.BOOKING_AIR_FK = " & Str_Selected_BookingPK & ")")
                //    sb.Append("        UNION")
                //    sb.Append("        SELECT FD.*")
                //    sb.Append("          FROM JOB_CARD_AIR_EXP_TBL JC, JOB_TRN_AIR_EXP_OTH_CHRG FD")
                //    sb.Append("         WHERE JC.JOB_CARD_AIR_EXP_PK = FD.JOB_CARD_AIR_EXP_FK")
                //    sb.Append("           AND JC.BOOKING_AIR_FK = " & Str_Selected_BookingPK & ") Q,")
                //    sb.Append("       (SELECT SUM(FD2.AMOUNT *")
                //    sb.Append("                   GET_EX_RATE(FD2.CURRENCY_MST_FK, 173, SYSDATE)) F_AMOUNT,")
                //    sb.Append("               FD2.FREIGHT_ELEMENT_MST_FK")
                //    sb.Append("          FROM JOB_CARD_AIR_EXP_TBL JC2, JOB_TRN_AIR_EXP_OTH_CHRG FD2")
                //    sb.Append("         WHERE JC2.JOB_CARD_AIR_EXP_PK = FD2.JOB_CARD_AIR_EXP_FK")
                //    sb.Append("           AND JC2.BOOKING_AIR_FK IN (" & Str_BookingPK & ")")
                //    sb.Append("         GROUP BY FD2.FREIGHT_ELEMENT_MST_FK, FD2.CURRENCY_MST_FK) QT")
                //    sb.Append("    WHERE QT.FREIGHT_ELEMENT_MST_FK = Q.FREIGHT_ELEMENT_MST_FK")

                //End If

                return (objWF.GetDataSet(sb.ToString()));
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
        public DataSet Fetch_Booking_Freight(string Str_BookingPK, string Str_Selected_BookingPK, string Str_BizType = "", int Cargo_Type = 0)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWF = new WorkFlow();


                if (Str_BizType == "1")
                {
                    sb.Append("SELECT DISTINCT FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("       BASE_CURR CURRENCY_MST_FK,");
                    sb.Append("       TRANS_REF_NO,");
                    sb.Append("       CONTAINER_TYPE_MST_FK,");
                    sb.Append("       BASIS,");
                    sb.Append("       (TARIFF_RATE * ROE) F_AMOUNT,");
                    sb.Append("       PYMT_TYPE FREIGHT_TYPE,");
                    sb.Append("       CHARGE_BASIS,");
                    sb.Append("       SURCHARGE");
                    sb.Append("  FROM (SELECT BF.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("               BF.CURRENCY_MST_FK,");
                    sb.Append("               NVL(A.CURRENCY_MST_FK,BF.CURRENCY_MST_FK) BASE_CURR,");
                    sb.Append("               GET_EX_RATE(BF.CURRENCY_MST_FK, NVL(A.CURRENCY_MST_FK,BF.CURRENCY_MST_FK), SYSDATE) ROE,");
                    sb.Append("               BT.TRANS_REF_NO,");
                    sb.Append("               BT.CONTAINER_TYPE_MST_FK,");
                    sb.Append("               BT.BASIS,");
                    sb.Append("               BF.TARIFF_RATE,");
                    sb.Append("               NVL(A.PYMT_TYPE,BF.PYMT_TYPE)PYMT_TYPE,");
                    sb.Append("               FM.CHARGE_BASIS,");
                    sb.Append("               BF.SURCHARGE");
                    sb.Append("          FROM BOOKING_TRN_FRT_DTLS BF,");
                    sb.Append("               BOOKING_TRN BT,");
                    sb.Append("               FREIGHT_ELEMENT_MST_TBL FM,");
                    sb.Append("               (SELECT FRT.BOOKING_TRN_FRT_PK,");
                    sb.Append("                       FRT.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("                       FRT.CURRENCY_MST_FK,");
                    sb.Append("                       FRT.PYMT_TYPE");
                    sb.Append("                  FROM BOOKING_TRN  TRN,");
                    sb.Append("                       BOOKING_TRN_FRT_DTLS FRT");
                    sb.Append("                 WHERE TRN.BOOKING_TRN_PK = FRT.BOOKING_TRN_FK");
                    sb.Append("                   AND TRN.BOOKING_MST_FK = " + Str_Selected_BookingPK + ") A");
                    sb.Append("         WHERE FM.FREIGHT_ELEMENT_MST_PK = BF.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("           AND BT.BOOKING_TRN_PK = BF.BOOKING_TRN_FK");
                    sb.Append("           AND A.FREIGHT_ELEMENT_MST_FK(+) = FM.FREIGHT_ELEMENT_MST_PK");
                    sb.Append("           AND BT.BOOKING_MST_FK IN (" + Str_BookingPK + "))");

                    if (Cargo_Type == 1)
                    {
                        sb.Append(" ORDER BY CONTAINER_TYPE_MST_FK");
                    }
                    else
                    {
                        sb.Append(" ORDER BY BASIS");
                    }
                }
                else
                {
                    sb.Append("   SELECT Q.*, QT.F_AMOUNT");
                    sb.Append("    FROM (SELECT FD.*");
                    sb.Append("        FROM JOB_CARD_TRN JC, JOB_TRN_FD FD");
                    sb.Append("         WHERE JC.JOB_CARD_TRN_PK = FD.JOB_CARD_TRN_FK");
                    sb.Append("           AND JC.BOOKING_MST_FK IN (" + Str_BookingPK + ")");
                    sb.Append("           AND FD.FREIGHT_ELEMENT_MST_FK NOT IN");
                    sb.Append("               (SELECT FD1.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("                  FROM JOB_CARD_TRN JC1, JOB_TRN_FD FD1");
                    sb.Append("                 WHERE JC1.JOB_CARD_TRN_PK = FD1.JOB_CARD_TRN_FK");
                    sb.Append("                   AND JC1.BOOKING_MST_FK =" + Str_Selected_BookingPK + ")");
                    sb.Append("        ");
                    sb.Append("        UNION");
                    sb.Append("        SELECT FD.*");
                    sb.Append("          FROM JOB_CARD_TRN JC, JOB_TRN_FD FD");
                    sb.Append("         WHERE JC.JOB_TRN_FD = FD.JOB_TRN_FD");
                    sb.Append("           AND JC.BOOKING_MST_FK = " + Str_Selected_BookingPK + ") Q,");
                    sb.Append("       (SELECT SUM(FD2.FREIGHT_AMT * ");
                    sb.Append("                   GET_EX_RATE(FD2.CURRENCY_MST_FK, 173, SYSDATE)) F_AMOUNT,");
                    sb.Append("               ");
                    sb.Append("               FD2.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("        ");
                    sb.Append("          FROM JOB_CARD_TRN JC2, JOB_TRN_FD FD2");
                    sb.Append("         WHERE JC2.JOB_TRN_FD = FD2.JOB_TRN_FD");
                    sb.Append("           AND JC2.BOOKING_MST_FK IN (" + Str_BookingPK + ")");
                    sb.Append("         GROUP BY FD2.FREIGHT_ELEMENT_MST_FK, FD2.CURRENCY_MST_FK) QT");
                    sb.Append("         WHERE QT.FREIGHT_ELEMENT_MST_FK = Q.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("");
                }


                return (objWF.GetDataSet(sb.ToString()));
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

        public DataSet NrofContainer(string str_Bookingpk, int BizType)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWF = new WorkFlow();
                sb.Append("  SELECT JTS.* ");
                sb.Append("  FROM JOB_CARD_TRN JC, JOB_TRN_CONT JTS");
                sb.Append("  WHERE JC.JOB_CARD_TRN_PK = JTS.JOB_CARD_TRN_FK");
                sb.Append("  AND JC.BUSINESS_TYPE=" + BizType);
                if (!string.IsNullOrEmpty(str_Bookingpk))
                {
                    sb.Append("  AND JC.BOOKING_MST_FK IN (" + str_Bookingpk + ")");
                }

                return (objWF.GetDataSet(sb.ToString()));
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
        //Function NrofContainerAir(ByVal str_Bookingpk As String) As DataSet
        //    Try
        //        Dim sb As New System.Text.StringBuilder(5000)
        //        Dim objWF As New WorkFlow
        //        sb.Append("  SELECT JTS.* ")
        //        sb.Append("  FROM JOB_CARD_AIR_EXP_TBL JC, JOB_TRN_AIR_EXP_CONT JTS")
        //        sb.Append("  WHERE JC.JOB_CARD_AIR_EXP_PK = JTS.JOB_CARD_AIR_EXP_FK")
        //        If str_Bookingpk <> "" Then
        //            sb.Append("  AND JC.BOOKING_AIR_FK IN (" & str_Bookingpk & ")")
        //        End If

        //        Return (objWF.GetDataSet(sb.ToString()))
        //    Catch sqlExp As OracleException
        //        Throw sqlExp
        //    Catch exp As Exception
        //        Throw exp
        //    End Try
        //End Function
        #endregion

        #region "Fetch Profitability"
        public DataSet FetchProfit(string Str_BookingPK, string Str_Selected_BookingPK, string Str_BizType = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                if (Convert.ToInt32(Str_BizType )== 1)
                {
                    sb.Append(" SELECT DISTINCT CMT.COST_ELEMENT_MST_PK,");
                    sb.Append("                QPT.CURRENCY_TYPE_MST_FK,");
                    sb.Append("                QPT.CONTAINER_TYPE_FK,");
                    sb.Append("                SUM(QPT.PROFITABILITY_RATE)PROFITABILITY_RATE,");
                    sb.Append("                QPT.ROE,");
                    sb.Append("                QPT.BIZ_TYPE,");
                    sb.Append("                QPT.CARGO_TYPE,");
                    sb.Append("                QPT.REF_FLAG,");
                    sb.Append("                SUM(QPT.VOLUME)VOLUME,");
                    sb.Append("                QPT.INC_TEA_STATUS ,");
                    sb.Append("                QPT.POL_FK ,");
                    sb.Append("                QPT.POD_FK ,");
                    sb.Append("                QPT.CARRIER_MST_FK ,");
                    sb.Append("                QPT.CREATED_BY_FK");
                    sb.Append("  FROM BOOKING_MST_TBL BST,");
                    sb.Append("       QUOTATION_PROFITABILITY_TBL QPT,");
                    sb.Append("       COST_ELEMENT_MST_TBL CMT,");
                    sb.Append("       (SELECT QMT.COST_ELEMENT_MST_FK");
                    sb.Append("          FROM QUOTATION_PROFITABILITY_TBL QMT");
                    sb.Append("         WHERE QMT.QUOTATION_FK = " + Str_Selected_BookingPK + ") A");
                    sb.Append(" WHERE CMT.COST_ELEMENT_MST_PK = QPT.COST_ELEMENT_MST_FK");
                    sb.Append("   AND QPT.COST_ELEMENT_MST_FK = A.COST_ELEMENT_MST_FK(+)");
                    sb.Append("   AND BST.BOOKING_MST_PK = QPT.QUOTATION_FK");
                    sb.Append("   AND QPT.QUOTATION_FK IN (" + Str_BookingPK + ")");
                    sb.Append(" GROUP BY ");
                    sb.Append("          CMT.COST_ELEMENT_MST_PK,");
                    sb.Append("          QPT.CURRENCY_TYPE_MST_FK,");
                    sb.Append("          QPT.CONTAINER_TYPE_FK,");
                    sb.Append("          QPT.ROE,");
                    sb.Append("          QPT.BIZ_TYPE,");
                    sb.Append("          QPT.CARGO_TYPE,");
                    sb.Append("          QPT.INC_TEA_STATUS ,");
                    sb.Append("          QPT.POL_FK ,");
                    sb.Append("          QPT.POD_FK ,");
                    sb.Append("          QPT.CARRIER_MST_FK ,");
                    sb.Append("          QPT.REF_FLAG,");
                    sb.Append("          QPT.CREATED_BY_FK");
                    return (objWF.GetDataSet(sb.ToString()));
                }
                else
                {
                    sb.Append(" SELECT DISTINCT CMT.COST_ELEMENT_MST_PK,");
                    sb.Append("                QPT.CURRENCY_TYPE_MST_FK,");
                    sb.Append("                QPT.CONTAINER_TYPE_FK,");
                    sb.Append("                SUM(QPT.PROFITABILITY_RATE)PROFITABILITY_RATE,");
                    sb.Append("                QPT.ROE,");
                    sb.Append("                QPT.BIZ_TYPE,");
                    sb.Append("                QPT.CARGO_TYPE,");
                    sb.Append("                QPT.REF_FLAG,");
                    sb.Append("                SUM(QPT.VOLUME)VOLUME,");
                    sb.Append("                QPT.INC_TEA_STATUS ,");
                    sb.Append("                QPT.POL_FK ,");
                    sb.Append("                QPT.POD_FK ,");
                    sb.Append("                QPT.CARRIER_MST_FK ,");
                    sb.Append("                QPT.CREATED_BY_FK");
                    sb.Append("  FROM BOOKING_MST_TBL BST,");
                    sb.Append("       QUOTATION_PROFITABILITY_TBL QPT,");
                    sb.Append("       COST_ELEMENT_MST_TBL CMT,");
                    sb.Append("       (SELECT QMT.COST_ELEMENT_MST_FK");
                    sb.Append("          FROM QUOTATION_PROFITABILITY_TBL QMT");
                    sb.Append("         WHERE QMT.QUOTATION_FK = " + Str_Selected_BookingPK + ") A");
                    sb.Append(" WHERE CMT.COST_ELEMENT_MST_PK = QPT.COST_ELEMENT_MST_FK");
                    sb.Append("   AND QPT.COST_ELEMENT_MST_FK = A.COST_ELEMENT_MST_FK(+)");
                    sb.Append("   AND BST.BOOKING_MST_PK = QPT.QUOTATION_FK");
                    sb.Append("   AND QPT.QUOTATION_FK IN (" + Str_BookingPK + ")");
                    sb.Append(" GROUP BY ");
                    sb.Append("          CMT.COST_ELEMENT_MST_PK,");
                    sb.Append("          QPT.CURRENCY_TYPE_MST_FK,");
                    sb.Append("          QPT.CONTAINER_TYPE_FK,");
                    sb.Append("          QPT.ROE,");
                    sb.Append("          QPT.BIZ_TYPE,");
                    sb.Append("          QPT.CARGO_TYPE,");
                    sb.Append("          QPT.INC_TEA_STATUS ,");
                    sb.Append("          QPT.POL_FK ,");
                    sb.Append("          QPT.POD_FK ,");
                    sb.Append("          QPT.CARRIER_MST_FK ,");
                    sb.Append("          QPT.REF_FLAG,");
                    sb.Append("          QPT.CREATED_BY_FK");
                    return (objWF.GetDataSet(sb.ToString()));
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
        #endregion

        #region "Fetch Booking.."
        public DataSet getFreightCharge(long business_Mode, long customer_Fk, long POL_Fk, long POD_Fk, long commodity_Group, long cargo_Type, long Voyage_trn_Fk, string Merge_flag, string Split_flag, string str_Bookingpk,
        bool IsPostBack)
        {


            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            if (business_Mode == 1)
            {
                if (cargo_Type == 1)
                {
                    sb.Append("");
                    sb.Append("       SELECT ROWNUM SLNR,");
                    sb.Append("       JC.BOOKING_MST_FK,");
                    sb.Append("       FD.JOB_CARD_TRN_FK,");
                    sb.Append("       FD.JOB_TRN_FD_PK,");
                    sb.Append("       FD.CONTAINER_TYPE_MST_FK,");
                    sb.Append("       CNT.CONTAINER_TYPE_MST_ID,");
                    sb.Append("       FE.FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("       FE.FREIGHT_ELEMENT_ID,");
                    sb.Append("       FE.FREIGHT_ELEMENT_NAME,");
                    sb.Append("       FD.FREIGHT_TYPE,");
                    sb.Append("       DECODE(FD.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE,");
                    sb.Append("       FD.LOCATION_MST_FK,");
                    sb.Append("       LMT.LOCATION_ID,");
                    sb.Append("       FD.FRTPAYER_CUST_MST_FK,");
                    sb.Append("       FRIGHTPAYER.CUSTOMER_ID,");
                    sb.Append("       FD.RATEPERBASIS,");
                    sb.Append("       FD.RATEPERBASIS Amount,");
                    sb.Append("       FD.BASIS,FD.QUANTITY,");
                    sb.Append("       FD.CURRENCY_MST_FK,");
                    sb.Append("       CMT.CURRENCY_ID,");
                    sb.Append("       FD.EXCHANGE_RATE,");
                    sb.Append("       (FD.EXCHANGE_RATE * FD.RATEPERBASIS * FD.QUANTITY) TOTAL");
                    sb.Append("");
                    sb.Append("       FROM JOB_CARD_TRN    JC,");
                    sb.Append("       JOB_TRN_FD           FD,");
                    sb.Append("       CONTAINER_TYPE_MST_TBL  CNT,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL FE,");
                    sb.Append("       LOCATION_MST_TBL        LMT,");
                    sb.Append("       CUSTOMER_MST_TBL        FRIGHTPAYER,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL   CMT");
                    sb.Append("");
                    sb.Append("   WHERE JC.JOB_CARD_PK = FD.JOB_CARD_TRN_FK");
                    sb.Append("   AND FD.CONTAINER_TYPE_MST_FK = CNT.CONTAINER_TYPE_MST_PK");
                    sb.Append("   AND FE.FREIGHT_ELEMENT_MST_PK = FD.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("   AND FD.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                    sb.Append("   AND FRIGHTPAYER.CUSTOMER_MST_PK = FD.FRTPAYER_CUST_MST_FK");
                    sb.Append("   AND FD.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
                    if (!string.IsNullOrEmpty(str_Bookingpk))
                    {
                        sb.Append("   AND JC.Booking_Sea_Fk= " + str_Bookingpk + "");
                    }
                }
                else
                {
                    sb.Append("");
                    sb.Append("       SELECT ROWNUM SLNR,");
                    sb.Append("       JC.BOOKING_MST_FK,");
                    sb.Append("       FD.JOB_CARD_TRN_FK,");
                    sb.Append("       FD.JOB_TRN_FD_PK,");
                    sb.Append("       FD.CONTAINER_TYPE_MST_FK,");
                    sb.Append("       CNT.CONTAINER_TYPE_MST_ID,");
                    sb.Append("       FE.FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("       FE.FREIGHT_ELEMENT_ID,");
                    sb.Append("       FE.FREIGHT_ELEMENT_NAME,");
                    sb.Append("       FD.FREIGHT_TYPE,");
                    sb.Append("       DECODE(FD.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE,");
                    sb.Append("       FD.LOCATION_MST_FK,");
                    sb.Append("       LMT.LOCATION_ID,");
                    sb.Append("       FD.FRTPAYER_CUST_MST_FK,");
                    sb.Append("       FRIGHTPAYER.CUSTOMER_ID,");
                    sb.Append("       FD.RATEPERBASIS,FD.RATEPERBASIS AMOUNT,");
                    sb.Append("       FD.BASIS,FD.QUANTITY,");
                    sb.Append("       FD.CURRENCY_MST_FK,");
                    sb.Append("       CMT.CURRENCY_ID,");
                    sb.Append("       FD.EXCHANGE_RATE,");
                    sb.Append("       (FD.EXCHANGE_RATE * FD.RATEPERBASIS * FD.QUANTITY) TOTAL");
                    sb.Append("");
                    sb.Append("       FROM JOB_CARD_TRN    JC,");
                    sb.Append("       JOB_TRN_FD           FD,");
                    sb.Append("       CONTAINER_TYPE_MST_TBL  CNT,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL FE,");
                    sb.Append("       LOCATION_MST_TBL        LMT,");
                    sb.Append("       CUSTOMER_MST_TBL        FRIGHTPAYER,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL   CMT");
                    sb.Append("");
                    sb.Append("   WHERE JC.JOB_CARD_TRN_PK = FD.JOB_CARD_TRN_FK");
                    sb.Append("   AND FD.CONTAINER_TYPE_MST_FK = CNT.CONTAINER_TYPE_MST_PK(+)");
                    sb.Append("   AND FE.FREIGHT_ELEMENT_MST_PK = FD.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("   AND FD.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                    sb.Append("   AND FRIGHTPAYER.CUSTOMER_MST_PK = FD.FRTPAYER_CUST_MST_FK");
                    sb.Append("   AND FD.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
                    if (!string.IsNullOrEmpty(str_Bookingpk))
                    {
                        sb.Append("   AND JC.Booking_MST_Fk= " + str_Bookingpk + "");
                    }

                }
            }
            else
            {
                sb.Append("       SELECT ROWNUM SLNR,");
                sb.Append("       JC.BOOKING_MST_FK BOOKING_SEA_FK,");
                sb.Append("       FD.JOB_CARD_TRN_FK JOB_CARD_SEA_EXP_FK,");
                sb.Append("       FD.JOB_TRN_FD_PK JOB_TRN_SEA_EXP_FD_PK,");
                sb.Append("       '' CONTAINER_TYPE_MST_FK,");
                sb.Append("       '' CONTAINER_TYPE_MST_ID,");
                sb.Append("       FE.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("       FE.FREIGHT_ELEMENT_ID,");
                sb.Append("       FE.FREIGHT_ELEMENT_NAME,");
                sb.Append("       FD.FREIGHT_TYPE,");
                sb.Append("       '' PAYMENT_TYPE,");
                sb.Append("       FD.LOCATION_MST_FK,");
                sb.Append("       LMT.LOCATION_ID,");
                sb.Append("       FD.FRTPAYER_CUST_MST_FK,");
                sb.Append("       FRIGHTPAYER.CUSTOMER_ID,");
                sb.Append("       FD.RATEPERBASIS,");
                sb.Append("       FD.RATEPERBASIS AMOUNT,");
                sb.Append("       FD.BASIS,");
                sb.Append("       FD.QUANTITY,");
                sb.Append("       FD.CURRENCY_MST_FK,");
                sb.Append("       CMT.CURRENCY_ID,");
                sb.Append("       FD.EXCHANGE_RATE,");
                sb.Append("       (FD.EXCHANGE_RATE * FD.RATEPERBASIS * FD.QUANTITY) TOTAL");
                sb.Append("       FROM JOB_CARD_TRN_TBL    JC,");
                sb.Append("       JOB_TRN_FD      FD,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FE,");
                sb.Append("       LOCATION_MST_TBL        LMT,");
                sb.Append("       CUSTOMER_MST_TBL        FRIGHTPAYER,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CMT");
                sb.Append("    WHERE JC.JOB_CARD_TRN_PK = FD.JOB_CARD_TRN_FK");
                sb.Append("   AND FE.FREIGHT_ELEMENT_MST_PK = FD.FREIGHT_ELEMENT_MST_FK");
                sb.Append("   AND FD.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND FRIGHTPAYER.CUSTOMER_MST_PK = FD.FRTPAYER_CUST_MST_FK");
                sb.Append("   AND FD.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
                if (!string.IsNullOrEmpty(str_Bookingpk))
                {
                    sb.Append("   AND JC.BOOKING_MST_FK= " + str_Bookingpk + "");
                }
            }

            try
            {
                return (objWF.GetDataSet(sb.ToString()));
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

        public DataSet getOtherCharge(long business_Mode, long customer_Fk, long POL_Fk, long POD_Fk, long commodity_Group, long cargo_Type, long Voyage_trn_Fk, string Merge_flag, string Split_flag, string str_Bookingpk,
        bool IsPostBack)
        {

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            //'for sea
            if (business_Mode == 1)
            {
                sb.Append("");
                sb.Append("       SELECT ROWNUM SLNR,");
                sb.Append("       JC.JOB_CARD_TRN_PK,jc.booking_MST_fk,");
                sb.Append("       OTH.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("       FE.FREIGHT_ELEMENT_ID,");
                sb.Append("       FE.FREIGHT_ELEMENT_NAME,");
                sb.Append("       OTH.FREIGHT_TYPE,");
                sb.Append("       '' PAYMENT_TYPE,");
                sb.Append("       OTH.LOCATION_MST_FK,");
                sb.Append("       LMT.LOCATION_ID,");
                sb.Append("       OTH.FRTPAYER_CUST_MST_FK,");
                sb.Append("       FRIGHTPAYER.CUSTOMER_ID,");
                sb.Append("       OTH.AMOUNT,");
                sb.Append("       OTH.CURRENCY_MST_FK,");
                sb.Append("       CMT.CURRENCY_ID,");
                sb.Append("       OTH.EXCHANGE_RATE,");
                sb.Append("       (OTH.EXCHANGE_RATE * OTH.AMOUNT) TOTAL,OTH.JOB_TRN_TRN_OTH_PK");
                sb.Append("");
                sb.Append("       FROM JOB_CARD_TRN     JC,");
                sb.Append("       JOB_TRN_OTH_CHRG OTH,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL  FE,");
                sb.Append("       LOCATION_MST_TBL         LMT,");
                sb.Append("       CUSTOMER_MST_TBL         FRIGHTPAYER,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CMT");
                sb.Append("");
                sb.Append("   WHERE JC.JOB_CARD_TRN_PK = OTH.JOB_CARD_TRN_FK");
                sb.Append("   AND OTH.FREIGHT_ELEMENT_MST_FK = FE.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND OTH.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND FRIGHTPAYER.CUSTOMER_MST_PK = OTH.FRTPAYER_CUST_MST_FK");
                sb.Append("   AND CMT.CURRENCY_MST_PK = OTH.CURRENCY_MST_FK");
                sb.Append("    ");
                sb.Append("");
                if (!string.IsNullOrEmpty(str_Bookingpk))
                {
                    sb.Append("   AND JC.Booking_MST_Fk= " + str_Bookingpk + "");
                }
            }
            else
            {
                sb.Append("");
                sb.Append("       SELECT ROWNUM SLNR,");
                sb.Append("       JC.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_PK,");
                sb.Append("       JC.BOOKING_MST_FK BOOKING_SEA_FK,");
                sb.Append("       OTH.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("       FE.FREIGHT_ELEMENT_ID,");
                sb.Append("       FE.FREIGHT_ELEMENT_NAME,");
                sb.Append("       OTH.FREIGHT_TYPE,");
                sb.Append("       '' PAYMENT_TYPE,");
                sb.Append("       OTH.LOCATION_MST_FK,");
                sb.Append("       LMT.LOCATION_ID,");
                sb.Append("       OTH.FRTPAYER_CUST_MST_FK,");
                sb.Append("       FRIGHTPAYER.CUSTOMER_ID,");
                sb.Append("       OTH.AMOUNT,");
                sb.Append("       OTH.CURRENCY_MST_FK,");
                sb.Append("       CMT.CURRENCY_ID,");
                sb.Append("       OTH.EXCHANGE_RATE,");
                sb.Append("       (OTH.EXCHANGE_RATE * OTH.AMOUNT) TOTAL,");
                sb.Append("       OTH.JOB_TRN_OTH_PK JOB_TRN_OTH_PK");
                sb.Append("");
                sb.Append("       FROM JOB_CARD_TRN     JC,");
                sb.Append("       JOB_TRN_OTH_CHRG OTH,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL  FE,");
                sb.Append("       LOCATION_MST_TBL         LMT,");
                sb.Append("       CUSTOMER_MST_TBL         FRIGHTPAYER,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CMT");
                sb.Append("");
                sb.Append("    WHERE JC.JOB_CARD_TRN_PK = OTH.JOB_CARD_TRN_FK");
                sb.Append("    AND OTH.FREIGHT_ELEMENT_MST_FK = FE.FREIGHT_ELEMENT_MST_PK");
                sb.Append("    AND OTH.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("    AND FRIGHTPAYER.CUSTOMER_MST_PK = OTH.FRTPAYER_CUST_MST_FK");
                sb.Append("    AND CMT.CURRENCY_MST_PK = OTH.CURRENCY_MST_FK");
                if (!string.IsNullOrEmpty(str_Bookingpk))
                {
                    sb.Append("  AND  JC.BOOKING_MST_FK= " + str_Bookingpk + "");
                }
            }


            try
            {
                return (objWF.GetDataSet(sb.ToString()));

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


        public DataSet getNrofBookings(long business_Mode, long customer_Fk, long POL_Fk, long POD_Fk, long commodity_Group, long cargo_Type, long Voyage_trn_Fk, string Merge_flag, string Split_flag, string str_Bookingpk,
        bool IsPostBack)
        {

            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWF = new WorkFlow();

                sb.Append(" SELECT ROWNUM SLNR,Q.* FROM ( ");
                sb.Append("  SELECT BT.BOOKING_MST_PK,");
                sb.Append("       BT.BOOKING_REF_NO,");
                sb.Append("       JC.JOB_CARD_TRN_PK,JTC.JOB_TRN_CONT_PK,");
                if (business_Mode == 2)
                {
                    sb.Append("       CTM.CONTAINER_TYPE_MST_PK,");
                    sb.Append("       CTM.CONTAINER_TYPE_MST_ID,");
                    sb.Append("       JTC.CONTAINER_NUMBER,");
                    sb.Append("       JTC.SEAL_NUMBER,");
                }
                else
                {
                    sb.Append("       AST.AIRFREIGHT_SLABS_TBL_PK CONTAINER_TYPE_MST_PK,");
                    sb.Append("       AST.BREAKPOINT_ID CONTAINER_TYPE_MST_ID,");
                    sb.Append("       JTC.ULD_NUMBER CONTAINER_NUMBER,");
                    sb.Append("       JTC.PALETTE_SIZE SEAL_NUMBER,");
                }
                sb.Append("       JTC.VOLUME_IN_CBM,");
                sb.Append("       JTC.GROSS_WEIGHT,");
                sb.Append("       CASE ");
                sb.Append("       WHEN JC.CARGO_TYPE = 1 AND JC.BUSINESS_TYPE = 2 THEN ");
                sb.Append("       JTC.NET_WEIGHT ");
                sb.Append("       ELSE ");
                sb.Append("       JTC.CHARGEABLE_WEIGHT ");
                sb.Append("       END NET_WEIGHT,");
                sb.Append("       JTC.PACK_TYPE_MST_FK,");
                sb.Append("       PTM.PACK_TYPE_ID,");
                sb.Append("       JTC.PACK_COUNT,");
                sb.Append("       ''MERGED_BOOKINGPK,'' MERGED_BOOKINGNR,'' SEL,'' DEL ");
                sb.Append("       FROM BOOKING_MST_TBL      BT,");
                sb.Append("       JOB_CARD_TRN JC,");
                sb.Append("       JOB_TRN_CONT   JTC,");
                if (business_Mode == 2)
                {
                    sb.Append("       CONTAINER_TYPE_MST_TBL CTM,");
                }
                else
                {
                    sb.Append("       AIRFREIGHT_SLABS_TBL AST,");
                }
                sb.Append("       PACK_TYPE_MST_TBL      PTM");
                sb.Append("    WHERE BT.BOOKING_MST_PK = JC.BOOKING_MST_FK");
                sb.Append("   AND JC.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
                if (business_Mode == 2)
                {
                    sb.Append("   AND JTC.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK(+)");
                }
                else
                {
                    sb.Append("   AND AST.AIRFREIGHT_SLABS_TBL_PK(+) = JTC.AIRFREIGHT_SLABS_TBL_FK");
                }

                sb.Append("   AND JTC.PACK_TYPE_MST_FK = PTM.PACK_TYPE_MST_PK(+)");
                if (!string.IsNullOrEmpty(str_Bookingpk))
                {
                    sb.Append("   AND BT.BOOKING_MST_PK IN (" + str_Bookingpk + ")");
                }
                sb.Append("  AND BT.BUSINESS_TYPE=" + business_Mode);
                sb.Append(" ORDER BY BT.BOOKING_MST_PK )Q ");

                //If business_Mode = 1 Then
                //    If cargo_Type = 1 Then
                //        sb.Append(" SELECT ROWNUM SLNR,Q.* FROM ( ")
                //        sb.Append("  SELECT BT.BOOKING_SEA_PK,")
                //        sb.Append("       BT.BOOKING_REF_NO,")
                //        sb.Append("       JC.JOB_CARD_SEA_EXP_PK,JTC.JOB_TRN_SEA_EXP_CONT_PK,")
                //        sb.Append("       CTM.CONTAINER_TYPE_MST_PK,")
                //        sb.Append("       CTM.CONTAINER_TYPE_MST_ID,")
                //        sb.Append("       JTC.CONTAINER_NUMBER,")
                //        sb.Append("       JTC.SEAL_NUMBER,")
                //        sb.Append("       JTC.VOLUME_IN_CBM,")
                //        sb.Append("       JTC.GROSS_WEIGHT,")
                //        sb.Append("       JTC.NET_WEIGHT,")
                //        sb.Append("       JTC.PACK_TYPE_MST_FK,")
                //        sb.Append("       PTM.PACK_TYPE_ID,")
                //        sb.Append("       JTC.PACK_COUNT,")
                //        sb.Append("       ''MERGED_BOOKINGPK,'' MERGED_BOOKINGNR,'' SEL,'' DEL ")
                //        sb.Append("       FROM BOOKING_SEA_TBL      BT,")
                //        sb.Append("       JOB_CARD_SEA_EXP_TBL JC,")
                //        sb.Append("       ")
                //        sb.Append("       JOB_TRN_SEA_EXP_CONT   JTC,")
                //        sb.Append("       CONTAINER_TYPE_MST_TBL CTM,")
                //        sb.Append("       PACK_TYPE_MST_TBL      PTM")
                //        sb.Append("    WHERE BT.BOOKING_SEA_PK = JC.BOOKING_SEA_FK")
                //        sb.Append("   AND JC.JOB_CARD_SEA_EXP_PK = JTC.JOB_CARD_SEA_EXP_FK")
                //        sb.Append("   AND JTC.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK")
                //        sb.Append("   AND JTC.PACK_TYPE_MST_FK = PTM.PACK_TYPE_MST_PK(+)")
                //        If str_Bookingpk <> "" Then
                //            sb.Append("   AND BT.BOOKING_SEA_PK IN (" & str_Bookingpk & ")")
                //        End If
                //        sb.Append(" ORDER BY BT.BOOKING_SEA_PK )Q ")
                //    ElseIf cargo_Type = 2 Then
                //        sb.Append(" SELECT ROWNUM SLNR,Q.* FROM ( ")
                //        sb.Append("  SELECT BT.BOOKING_SEA_PK,")
                //        sb.Append("       BT.BOOKING_REF_NO,")
                //        sb.Append("       JC.JOB_CARD_SEA_EXP_PK,JTC.JOB_TRN_SEA_EXP_CONT_PK,")
                //        sb.Append("       CTM.CONTAINER_TYPE_MST_PK,")
                //        sb.Append("       CTM.CONTAINER_TYPE_MST_ID,")
                //        sb.Append("       JTC.CONTAINER_NUMBER,")
                //        sb.Append("       JTC.SEAL_NUMBER,")
                //        sb.Append("       JTC.VOLUME_IN_CBM,")
                //        sb.Append("       JTC.GROSS_WEIGHT,")
                //        sb.Append("       JTC.CHARGEABLE_WEIGHT NET_WEIGHT,")
                //        sb.Append("       JTC.PACK_TYPE_MST_FK,")
                //        sb.Append("       PTM.PACK_TYPE_ID,")
                //        sb.Append("       JTC.PACK_COUNT,")
                //        sb.Append("       ''MERGED_BOOKINGPK,'' MERGED_BOOKINGNR,'' SEL,'' DEL ")
                //        sb.Append("")
                //        sb.Append("       FROM BOOKING_SEA_TBL      BT,")
                //        sb.Append("       JOB_CARD_SEA_EXP_TBL JC,")
                //        sb.Append("       ")
                //        sb.Append("       JOB_TRN_SEA_EXP_CONT   JTC,")
                //        sb.Append("       CONTAINER_TYPE_MST_TBL CTM,")
                //        sb.Append("       PACK_TYPE_MST_TBL      PTM")
                //        sb.Append("")
                //        sb.Append("    WHERE BT.BOOKING_SEA_PK = JC.BOOKING_SEA_FK")
                //        sb.Append("   AND JC.JOB_CARD_SEA_EXP_PK = JTC.JOB_CARD_SEA_EXP_FK(+)")
                //        sb.Append("   AND JTC.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK(+)")
                //        sb.Append("   AND JTC.PACK_TYPE_MST_FK = PTM.PACK_TYPE_MST_PK(+)")
                //        If str_Bookingpk <> "" Then
                //            sb.Append("   AND BT.BOOKING_SEA_PK IN (" & str_Bookingpk & ")")
                //        End If
                //        sb.Append(" ORDER BY BT.BOOKING_SEA_PK)Q ")
                //    End If
                //Else
                //    sb.Append(" SELECT ROWNUM SLNR,Q.* FROM ( ")
                //    sb.Append("       SELECT BT.BOOKING_AIR_PK BOOKING_SEA_PK,")
                //    sb.Append("       BT.BOOKING_REF_NO,")
                //    sb.Append("       JC.BOOKING_AIR_FK JOB_CARD_SEA_EXP_PK,")
                //    sb.Append("       JTC.JOB_TRN_AIR_EXP_CONT_PK JOB_TRN_SEA_EXP_CONT_PK,")
                //    sb.Append("       AST.AIRFREIGHT_SLABS_TBL_PK CONTAINER_TYPE_MST_PK,")
                //    sb.Append("       AST.BREAKPOINT_ID CONTAINER_TYPE_MST_ID,")
                //    sb.Append("       JTC.ULD_NUMBER CONTAINER_NUMBER,")
                //    sb.Append("       JTC.PALETTE_SIZE SEAL_NUMBER,")
                //    sb.Append("       JTC.VOLUME_IN_CBM,")
                //    sb.Append("       JTC.GROSS_WEIGHT,")
                //    sb.Append("       JTC.CHARGEABLE_WEIGHT NET_WEIGHT,")
                //    sb.Append("       JTC.PACK_TYPE_MST_FK,")
                //    sb.Append("       PTM.PACK_TYPE_ID,")
                //    sb.Append("       JTC.PACK_COUNT,")
                //    sb.Append("       '' MERGED_BOOKINGPK,")
                //    sb.Append("       '' MERGED_BOOKINGNR,'' SEL,'' DEL ")
                //    sb.Append("       FROM BOOKING_AIR_TBL      BT,")
                //    sb.Append("       JOB_CARD_AIR_EXP_TBL JC,")
                //    sb.Append("       AIRFREIGHT_SLABS_TBL AST,")
                //    sb.Append("       JOB_TRN_AIR_EXP_CONT JTC,")
                //    sb.Append("       PACK_TYPE_MST_TBL    PTM")
                //    sb.Append("")
                //    sb.Append("   WHERE BT.BOOKING_AIR_PK = JC.BOOKING_AIR_FK")
                //    sb.Append("   AND JC.JOB_CARD_AIR_EXP_PK = JTC.JOB_CARD_AIR_EXP_FK(+)")
                //    sb.Append("   AND JTC.PACK_TYPE_MST_FK = PTM.PACK_TYPE_MST_PK(+)")
                //    sb.Append("   AND AST.AIRFREIGHT_SLABS_TBL_PK(+) = JTC.AIRFREIGHT_SLABS_TBL_FK")
                //    If str_Bookingpk <> "" Then
                //        sb.Append("    AND BT.BOOKING_AIR_PK IN (" & str_Bookingpk & ")")
                //    End If
                //    sb.Append(" ORDER BY BT.BOOKING_AIR_PK)Q ")
                //End If

                return (objWF.GetDataSet(sb.ToString()));

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
        #endregion

        #region "Save Fright and Other charge"
        public ArrayList Save_Freight_OtherCharge(DataSet DsFreight, DataSet DsOthercharge, int Int_Bookingpk)
        {

            WorkFlow objWK = new WorkFlow();
            int Rowcnt = 0;
            int Int_Booking_pk = 0;
            try
            {
                OracleTransaction TRAN = null;
                objWK.OpenConnection();

                TRAN = objWK.MyConnection.BeginTransaction();
                arrMessage.Clear();
                OracleCommand insCommand = new OracleCommand();
                OracleCommand updCommand = new OracleCommand();
                objWK.MyCommand.Transaction = TRAN;

                if (Int_Bookingpk > 0)
                {
                    if (DsFreight.Tables[0].Rows.Count > 0)
                    {
                        for (Rowcnt = 0; Rowcnt <= DsFreight.Tables[0].Rows.Count - 1; Rowcnt++)
                        {
                            var _with3 = updCommand;
                            updCommand.Parameters.Clear();
                            _with3.Connection = objWK.MyConnection;
                            _with3.CommandType = CommandType.StoredProcedure;
                            _with3.CommandText = objWK.MyUserName + ".MERGE_BOOKING_SEA_PKG.MERGE_BOOKING_FREIGHT_UPD";
                            var _with4 = _with3.Parameters;
                            _with4.Add("BOOKING_SEA_PK_IN", DsFreight.Tables[0].Rows[Rowcnt]["BOOKING_SEA_FK"]).Direction = ParameterDirection.Input;
                            _with4.Add("BOOKING_TRN_SEA_FRT_PK_IN", DsFreight.Tables[0].Rows[Rowcnt]["FREIGHT_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                            _with4.Add("CURRENCY_MST_FK_IN", DsFreight.Tables[0].Rows[Rowcnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                            _with4.Add("EXCHANGE_RATE_IN", DsFreight.Tables[0].Rows[Rowcnt]["EXCHANGE_RATE"]).Direction = ParameterDirection.Input;
                            _with4.Add("TARIFF_RATE_IN", DsFreight.Tables[0].Rows[Rowcnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                            _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            var _with5 = objWK.MyDataAdapter;
                            _with5.UpdateCommand = updCommand;
                            _with5.UpdateCommand.Transaction = TRAN;
                            _with5.UpdateCommand.ExecuteNonQuery();
                            Int_Booking_pk = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
                        }
                    }
                }
                if (Int_Bookingpk > 0)
                {
                    arrMessage = SaveBooking_Other_Freight(DsOthercharge, Int_Bookingpk, TRAN);
                }
                if (Int_Booking_pk > 0 | Int_Bookingpk > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Save Sucessfully");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    arrMessage.Add("All Data Save Sucessfully");
                    return arrMessage;
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
            finally
            {
                objWK.MyCommand.Connection.Close();
            }

        }

        public ArrayList SaveBooking_Other_Freight(DataSet dsMain, long PkValue, OracleTransaction TRAN = null)
        {
            Int32 Rowcnt = default(Int32);
            Int32 RecAfct = default(Int32);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            objWK.MyConnection = TRAN.Connection;
            try
            {
                if (PkValue > 0)
                {
                    for (Rowcnt = 0; Rowcnt <= dsMain.Tables[0].Rows.Count - 1; Rowcnt++)
                    {
                        var _with6 = insCommand;
                        insCommand.Parameters.Clear();
                        _with6.Connection = objWK.MyConnection;
                        _with6.CommandType = CommandType.StoredProcedure;
                        _with6.CommandText = objWK.MyUserName + ".MERGE_BOOKING_SEA_PKG.MERGE_BOOKING_OTHERCHARGE_INS";
                        var _with7 = _with6.Parameters;
                        _with7.Add("BOOKING_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with7.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with7.Add("CURRENCY_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with7.Add("AMOUNT_IN", dsMain.Tables[0].Rows[Rowcnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with7.Add("FREIGHT_TYPE_IN", dsMain.Tables[0].Rows[Rowcnt]["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;

                        _with7.Add("LOCATION_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                        _with7.Add("FRTPAYER_CUST_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                        _with7.Add("EXCHANGE_RATE_IN", dsMain.Tables[0].Rows[Rowcnt]["EXCHANGE_RATE"]).Direction = ParameterDirection.Input;
                        _with7.Add("PAYMENT_TYPE_IN", dsMain.Tables[0].Rows[Rowcnt]["PAYMENT_TYPE"]).Direction = ParameterDirection.Input;
                        _with7.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        var _with8 = updCommand;
                        updCommand.Parameters.Clear();
                        _with8.Connection = objWK.MyConnection;
                        _with8.CommandType = CommandType.StoredProcedure;
                        _with8.CommandText = objWK.MyUserName + ".MERGE_BOOKING_SEA_PKG.MERGE_BOOKING_OTHERCHARGE_UPD";
                        var _with9 = _with8.Parameters;
                        _with9.Add("BOOKING_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with9.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with9.Add("CURRENCY_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with9.Add("AMOUNT_IN", dsMain.Tables[0].Rows[Rowcnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with9.Add("FREIGHT_TYPE_IN", dsMain.Tables[0].Rows[Rowcnt]["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;

                        _with9.Add("LOCATION_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                        _with9.Add("FRTPAYER_CUST_MST_FK_IN", dsMain.Tables[0].Rows[Rowcnt]["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                        _with9.Add("EXCHANGE_RATE_IN", dsMain.Tables[0].Rows[Rowcnt]["EXCHANGE_RATE"]).Direction = ParameterDirection.Input;
                        _with9.Add("PAYMENT_TYPE_IN", dsMain.Tables[0].Rows[Rowcnt]["PAYMENT_TYPE"]).Direction = ParameterDirection.Input;
                        _with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        var _with10 = objWK.MyDataAdapter;
                        if (string.IsNullOrEmpty(dsMain.Tables[0].Rows[Rowcnt]["JOB_TRN_SEA_EXP_OTH_PK"].ToString()) | Convert.ToInt32(dsMain.Tables[0].Rows[Rowcnt]["JOB_TRN_SEA_EXP_OTH_PK"].ToString()) == 0)
                        {
                            _with10.InsertCommand = insCommand;
                            _with10.InsertCommand.Transaction = TRAN;
                            RecAfct = _with10.InsertCommand.ExecuteNonQuery();
                        }
                        else
                        {
                            _with10.UpdateCommand = updCommand;
                            _with10.UpdateCommand.Transaction = TRAN;
                            RecAfct = _with10.UpdateCommand.ExecuteNonQuery();
                        }
                    }
                }


                if (RecAfct > 0 | PkValue > 0)
                {
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
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
            finally
            {
            }
        }
        #endregion

        #region "Fetch Booking.."

        public DataSet getBookings(long business_Mode, long customer_Fk, long POL_Fk, long POD_Fk, long commodity_Group, long cargo_Type, long Voyage_trn_Fk, string Merge_flag, string Split_flag, string StrFlightNr,
        bool IsPostBack, long Booking_FK)
        {

            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWF = new WorkFlow();

                //If business_Mode = 1 Then

                sb.Append("SELECT ROWNUM SLNR,");
                sb.Append("       '' OPTFLAG,");
                sb.Append("       JC.JOB_CARD_TRN_PK,");
                sb.Append("       BT.BOOKING_MST_PK,");
                sb.Append("       BT.BOOKING_REF_NO,");
                sb.Append("       TO_DATE(BT.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
                sb.Append("       POL.PORT_MST_PK POL_FK,");
                sb.Append("       POL.PORT_ID POLID,");
                sb.Append("       POD.PORT_MST_PK POD_FK,");
                sb.Append("       POD.PORT_ID PODID,");
                sb.Append("       JC.SHIPPER_CUST_MST_FK SHIPPER_FK,");
                sb.Append("       CMT.CUSTOMER_NAME SHIPPER,");
                sb.Append("       CON.CUSTOMER_MST_PK CONSIGNEE_PK,");
                sb.Append("       CON.CUSTOMER_NAME CONSIGNEE,");
                sb.Append("       STM.SHIPPING_TERMS_MST_PK SHIPPING_TERM_FK,");
                sb.Append("       STM.INCO_CODE SHIPPING_TERM,");
                sb.Append("       CMV.CARGO_MOVE_PK CARGO_MOVE_FK,");
                sb.Append("       CMV.CARGO_MOVE_CODE,");
                sb.Append("(SELECT DISTINCT T.TRANS_REF_NO");
                sb.Append("          FROM BOOKING_TRN T");
                sb.Append("         WHERE T.BOOKING_MST_FK = BT.BOOKING_MST_PK");
                sb.Append("           AND ROWNUM = 1) REF_NR,");
                sb.Append("       BT.COMMODITY_GROUP_FK,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BT,");
                sb.Append("       JOB_CARD_TRN    JC,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       CUSTOMER_MST_TBL        CON,");
                sb.Append("       SHIPPING_TERMS_MST_TBL  STM,");
                sb.Append("       CARGO_MOVE_MST_TBL      CMV");
                sb.Append(" WHERE BT.BOOKING_MST_PK = JC.BOOKING_MST_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JC.SHIPPER_CUST_MST_FK");
                sb.Append("   AND CON.CUSTOMER_MST_PK(+) = JC.CONSIGNEE_CUST_MST_FK");
                sb.Append("   AND STM.SHIPPING_TERMS_MST_PK(+) = JC.SHIPPING_TERMS_MST_FK");
                sb.Append("   AND CMV.CARGO_MOVE_PK(+) = JC.CARGO_MOVE_FK");
                sb.Append("   AND POL.PORT_MST_PK = BT.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = BT.PORT_MST_POD_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BT.COMMODITY_GROUP_FK");
                sb.Append("   AND BT.STATUS <> 3");
                sb.Append("   AND 0 = (SELECT COUNT(*)");
                sb.Append("              FROM CONSOL_INVOICE_TBL     CIT,");
                sb.Append("                   CONSOL_INVOICE_TRN_TBL CITT,");
                sb.Append("                   JOB_CARD_TRN   JCS");
                sb.Append("             WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
                sb.Append("               AND JCS.JOB_CARD_TRN_PK = CITT.JOB_CARD_FK");
                sb.Append("               AND CIT.BUSINESS_TYPE = " + business_Mode);
                sb.Append("               AND CITT.JOB_CARD_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("   AND 0 = (SELECT COUNT(*)");
                sb.Append("              FROM HBL_EXP_TBL HBL");
                sb.Append("             WHERE HBL.JOB_CARD_SEA_EXP_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("   AND 0 = (SELECT COUNT(*)");
                sb.Append("              FROM MBL_EXP_TBL MBL");
                sb.Append("             WHERE MBL.MBL_EXP_TBL_PK = JC.MBL_MAWB_FK)");
                sb.Append(" AND BT.BUSINESS_TYPE=" + business_Mode);

                if (business_Mode == 1)
                {
                    sb.Append("           AND BT.VOYAGE_FLIGHT_NO ='" + StrFlightNr + "'");
                }
                else
                {
                    if (Voyage_trn_Fk > 0)
                    {
                        sb.Append("            AND JC.VOYAGE_TRN_FK =" + Voyage_trn_Fk + "");
                    }
                }



                if (customer_Fk > 0)
                {
                    sb.Append("            AND BT.CUST_CUSTOMER_MST_FK=" + customer_Fk + "");
                }

                if (POL_Fk > 0)
                {
                    sb.Append("              AND BT.PORT_MST_POL_FK=" + POL_Fk + "");
                }

                if (POD_Fk > 0)
                {
                    sb.Append("             AND BT.PORT_MST_POD_FK=" + POD_Fk + "");
                }

                if (commodity_Group > 0)
                {
                    sb.Append("             AND BT.COMMODITY_GROUP_FK=" + commodity_Group + "");
                }
                if (business_Mode == 2)
                {
                    if (cargo_Type > 0)
                    {
                        sb.Append("            AND BT.CARGO_TYPE=" + cargo_Type + "");
                    }
                }
                if (Booking_FK > 0)
                {
                    sb.Append("            AND BT.BOOKING_MST_PK=" + Booking_FK + "");
                }
                sb.Append("             ORDER BY BT.BOOKING_DATE DESC");
                //Else
                //    sb.Append("SELECT DISTINCT ROWNUM SLNR,")
                //    sb.Append("                '' OPTFLAG,")
                //    sb.Append("                JC.JOB_CARD_AIR_EXP_PK JOB_CARD_SEA_EXP_PK,")
                //    sb.Append("                BT.BOOKING_AIR_PK BOOKING_SEA_PK,")
                //    sb.Append("                BT.BOOKING_REF_NO,")
                //    sb.Append("                TO_CHAR(BT.BOOKING_DATE, 'dd/MM/yyyy') BOOKING_DATE,")
                //    sb.Append("                BT.PORT_MST_POL_FK,")
                //    sb.Append("                AOO.PORT_ID POLID,")
                //    sb.Append("                BT.PORT_MST_POD_FK,")
                //    sb.Append("                AOD.PORT_ID PODID,")
                //    sb.Append("                JC.SHIPPER_CUST_MST_FK SHIPPER_PK,")
                //    sb.Append("                CMT.CUSTOMER_NAME SHIPPER,")
                //    sb.Append("                JC.CONSIGNEE_CUST_MST_FK CONSIGNEE_PK,")
                //    sb.Append("                CON.CUSTOMER_NAME CONSIGNEE,")
                //    sb.Append("                STM.SHIPPING_TERMS_MST_PK SHIPPING_TERM_FK,")
                //    sb.Append("                STM.INCO_CODE SHIPPING_TERM,")
                //    sb.Append("                CMV.CARGO_MOVE_PK CARGO_MOVE_FK,")
                //    sb.Append("                CMV.CARGO_MOVE_CODE,")
                //    sb.Append("                (SELECT DISTINCT T.TRANS_REF_NO")
                //    sb.Append("                FROM BOOKING_TRN_AIR T")
                //    sb.Append("                WHERE T.BOOKING_AIR_FK = BT.BOOKING_AIR_PK")
                //    sb.Append("                AND ROWNUM = 1) REF_NR,")
                //    sb.Append("                BT.COMMODITY_GROUP_FK,")
                //    sb.Append("                CGMT.COMMODITY_GROUP_CODE")
                //    sb.Append("       FROM BOOKING_AIR_TBL         BT,")
                //    sb.Append("       CUSTOMER_MST_TBL        CT,")
                //    sb.Append("       JOB_CARD_AIR_EXP_TBL    JC,")
                //    sb.Append("       CUSTOMER_MST_TBL        CMT,")
                //    sb.Append("       PORT_MST_TBL            AOO,")
                //    sb.Append("       PORT_MST_TBL            AOD,")
                //    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,")
                //    sb.Append("       CUSTOMER_MST_TBL        CON,")
                //    sb.Append("       SHIPPING_TERMS_MST_TBL  STM,")
                //    sb.Append("       CARGO_MOVE_MST_TBL CMV")
                //    sb.Append("   WHERE CT.CUSTOMER_MST_PK = BT.CUST_CUSTOMER_MST_FK")
                //    sb.Append("   AND BT.BOOKING_AIR_PK = JC.BOOKING_AIR_FK")
                //    sb.Append("   AND CMT.CUSTOMER_MST_PK = JC.SHIPPER_CUST_MST_FK")
                //    sb.Append("   AND AOO.PORT_MST_PK = BT.PORT_MST_POL_FK")
                //    sb.Append("   AND AOD.PORT_MST_PK = BT.PORT_MST_POD_FK")
                //    sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BT.COMMODITY_GROUP_FK")
                //    sb.Append("   AND AOO.BUSINESS_TYPE = 1")
                //    sb.Append("   AND AOD.BUSINESS_TYPE = 1")
                //    sb.Append("   AND CON.CUSTOMER_MST_PK(+) = JC.CONSIGNEE_CUST_MST_FK ")
                //    sb.Append("   AND STM.SHIPPING_TERMS_MST_PK(+) = JC.SHIPPING_TERMS_MST_FK ")
                //    sb.Append("   AND CMV.CARGO_MOVE_PK(+) = JC.CARGO_MOVE_FK ")
                //    sb.Append("   AND 0 = (SELECT COUNT(*)")
                //    sb.Append("              FROM CONSOL_INVOICE_TBL     CIT,")
                //    sb.Append("                   CONSOL_INVOICE_TRN_TBL CITT,")
                //    sb.Append("                   JOB_CARD_AIR_EXP_TBL   JCS")
                //    sb.Append("             WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK")
                //    sb.Append("               AND JCS.JOB_CARD_AIR_EXP_PK = CITT.JOB_CARD_FK")
                //    sb.Append("                  ")
                //    sb.Append("               AND CITT.JOB_CARD_FK = JC.JOB_CARD_AIR_EXP_PK")
                //    sb.Append("               AND CIT.BUSINESS_TYPE = 1)")
                //    sb.Append("      ")
                //    sb.Append("   AND 0 = (SELECT COUNT(*)")
                //    sb.Append("              FROM HAWB_EXP_TBL HBL")
                //    sb.Append("             WHERE HBL.JOB_CARD_AIR_EXP_FK = JC.BOOKING_AIR_FK)")
                //    sb.Append("   AND 0 = (SELECT COUNT(*)")
                //    sb.Append("              FROM MAWB_EXP_TBL MAWB")
                //    sb.Append("             WHERE MAWB.MAWB_EXP_TBL_PK = JC.MAWB_EXP_TBL_FK)")


                //    If StrFlightNr <> "" Then
                //        sb.Append("            AND BT.flight_no='" & StrFlightNr & "'")
                //    End If


                //    If customer_Fk > 0 Then
                //        sb.Append("            AND BT.CUST_CUSTOMER_MST_FK=" & customer_Fk & "")
                //    End If


                //    If POL_Fk > 0 Then
                //        sb.Append("              AND BT.PORT_MST_POL_FK=" & POL_Fk & "")
                //    End If


                //    If POD_Fk > 0 Then
                //        sb.Append("             AND BT.PORT_MST_POD_FK=" & POD_Fk & "")
                //    End If


                //    If commodity_Group > 0 Then
                //        sb.Append("             AND BT.COMMODITY_GROUP_FK=" & commodity_Group & "")
                //    End If
                //    If Booking_FK > 0 Then
                //        sb.Append("            AND BT.BOOKING_AIR_PK=" & Booking_FK & "")
                //    End If
                //End If

                return (objWF.GetDataSet(sb.ToString()));
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
        #endregion

        #region "Merge Booking.."
        public DataSet Container_Details(string Bookingpk, string BizType)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWF = new WorkFlow();
                sb.Append("  SELECT CNT.*");
                sb.Append("  FROM JOB_TRN_CONT CNT, JOB_CARD_TRN JC");
                sb.Append("  WHERE CNT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                if (!string.IsNullOrEmpty(Bookingpk))
                {
                    sb.Append(" AND JC.BOOKING_MST_FK IN  (" + Bookingpk + ") ");
                }
                //If BizType = "1" Then ''sea
                //    sb.Append("  SELECT CNT.*")
                //    sb.Append("  FROM JOB_TRN_SEA_EXP_CONT CNT, JOB_CARD_SEA_EXP_TBL JC")
                //    sb.Append("  WHERE CNT.JOB_CARD_SEA_EXP_FK = JC.JOB_CARD_SEA_EXP_PK")
                //    If Bookingpk <> "" Then
                //        sb.Append(" AND JC.BOOKING_SEA_FK IN  (" & Bookingpk & ") ")
                //    End If

                //Else ''air
                //    sb.Append("  SELECT CNT.*")
                //    sb.Append("  FROM JOB_TRN_AIR_EXP_CONT CNT, JOB_CARD_AIR_EXP_TBL JC")
                //    sb.Append("  WHERE CNT.JOB_CARD_AIR_EXP_FK = JC.JOB_CARD_AIR_EXP_PK")
                //    If Bookingpk <> "" Then
                //        sb.Append(" AND JC.BOOKING_AIR_FK IN  (" & Bookingpk & ") ")
                //    End If
                //End If
                return objWF.GetDataSet(sb.ToString());
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
        public DataSet MergBooking(string bookingpk, string BizType)
        {
            try
            {
                System.Text.StringBuilder strSql = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strSql.Append(" SELECT B.BOOKING_MST_PK, ");
                strSql.Append(" B.Cons_Customer_Mst_Fk, ");
                strSql.Append(" JC.Notify1_Cust_Mst_Fk, ");
                strSql.Append(" B.poo_fk, ");
                strSql.Append(" B.port_mst_pol_fk, ");
                strSql.Append(" B.port_mst_pod_fk, ");
                strSql.Append(" B.PFD_FK, ");
                strSql.Append("  B.commodity_group_fk, ");
                strSql.Append("  B.cargo_type ");
                strSql.Append("  FROM BOOKING_MST_TBL B, JOB_CARD_TRN   JC");
                strSql.Append(" WHERE B.BOOKING_MST_PK = JC.BOOKING_MST_FK");
                if (!string.IsNullOrEmpty(bookingpk))
                {
                    strSql.Append(" AND B.BOOKING_MST_PK IN  (" + bookingpk + ") ");
                }
                //If BizType = "1" Then
                //    strSql.Append(" SELECT B.BOOKING_SEA_PK, ")
                //    strSql.Append(" B.Cons_Customer_Mst_Fk, ")
                //    strSql.Append(" JC.Notify1_Cust_Mst_Fk, ")
                //    strSql.Append(" B.poo_fk, ")
                //    strSql.Append(" B.port_mst_pol_fk, ")
                //    strSql.Append(" B.port_mst_pod_fk, ")
                //    strSql.Append(" B.PFD_FK, ")
                //    strSql.Append("  B.commodity_group_fk, ")
                //    strSql.Append("  B.cargo_type ")
                //    strSql.Append("  FROM BOOKING_SEA_TBL B, JOB_CARD_SEA_EXP_TBL   JC")
                //    strSql.Append(" WHERE B.BOOKING_SEA_PK = JC.BOOKING_SEA_FK")
                //    If bookingpk <> "" Then
                //        strSql.Append(" AND B.BOOKING_SEA_PK IN  (" & bookingpk & ") ")
                //    End If

                //Else
                //    strSql.Append("       SELECT B.BOOKING_AIR_PK,")
                //    strSql.Append("       B.CONS_CUSTOMER_MST_FK,")
                //    strSql.Append("       JC.NOTIFY1_CUST_MST_FK,")
                //    strSql.Append("       '' POO_FK,")
                //    strSql.Append("       B.PORT_MST_POL_FK,")
                //    strSql.Append("       B.PORT_MST_POD_FK,")
                //    strSql.Append("       '' PFD_FK,")
                //    strSql.Append("       B.COMMODITY_GROUP_FK,")
                //    strSql.Append("       '' CARGO_TYPE")
                //    strSql.Append("")
                //    strSql.Append("   FROM BOOKING_AIR_TBL B, JOB_CARD_AIR_EXP_TBL JC")
                //    strSql.Append("   WHERE B.BOOKING_AIR_PK = JC.BOOKING_AIR_FK")
                //    If bookingpk <> "" Then
                //        strSql.Append(" AND B.BOOKING_AIR_PK IN  (" & bookingpk & ") ")
                //    End If


                //End If


                return objWF.GetDataSet(strSql.ToString());
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

        public DataSet MergBooking1(string bookingpk)
        {
            try
            {
                System.Text.StringBuilder strSql = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strSql.Append(" select * from booking_freight_trn bft where bft.booking_trn_fk=" + bookingpk);

                return objWF.GetDataSet(strSql.ToString());
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
        #endregion

        #region "Save Merge Booking"
        public void Freight_OtherCharge(DataSet M_dataset, int int_jobcardpk, int Int_LocationFk, int Int_Bookingpk, bool Merge_Flag, bool Split_Flag, string Merged_Booking_no, int Cargo_type, int Biz_Type, string Str_Bookingpk)
        {


            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder SQL = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            DataSet M_DsFreight = null;
            DataSet M_DsOtherCharge = null;
            DataSet DsFreight = null;
            DataSet DsOtherCharges = null;
            sb.Append("  SELECT FRT.*");
            sb.Append("  FROM JOB_CARD_SEA_EXP_TBL     JC,");
            sb.Append("       JOB_TRN_SEA_EXP_FD       FRT");
            sb.Append("  WHERE  jc.job_card_sea_exp_pk=FRT.JOB_CARD_SEA_EXP_FK");
            if (!string.IsNullOrEmpty(Str_Bookingpk))
            {
                sb.Append("  and  JC.BOOKING_SEA_FK IN (" + Str_Bookingpk + ")");
            }

            SQL.Append("  SELECT OTH.*");
            SQL.Append("  FROM JOB_CARD_SEA_EXP_TBL     JC,");
            sb.Append("        JOB_TRN_SEA_EXP_OTH_CHRG OTH");
            SQL.Append("  WHERE  jc.job_card_sea_exp_pk=OTH.JOB_CARD_SEA_EXP_FK");
            if (!string.IsNullOrEmpty(Str_Bookingpk))
            {
                SQL.Append("  and  JC.BOOKING_SEA_FK IN (" + Str_Bookingpk + ")");
            }


            try
            {
                M_DsFreight = objWF.GetDataSet(sb.ToString());
                M_DsOtherCharge = objWF.GetDataSet(SQL.ToString());
                int Rcnt = 0;
                int Rcnt1 = 0;
                int RcntFlag = 0;
                int colcnt = 0;
                DataRow Drow = null;
                DataRow OtherChargeDataRow = null;
                if (M_DsFreight.Tables[0].Rows.Count > 0)
                {
                    DsFreight = M_DsFreight.Clone();
                    int FreightRcnt = 0;
                    for (Rcnt = 1; Rcnt <= M_DsFreight.Tables[0].Rows.Count; Rcnt++)
                    {
                        if (int_jobcardpk == Convert.ToInt32(M_DsFreight.Tables[0].Rows[Rcnt]["JOB_CARD_SEA_EXP_FK"]))
                        {
                            Drow = DsFreight.Tables[0].NewRow();
                            for (colcnt = 0; colcnt <= M_DsFreight.Tables[0].Columns.Count - 1; colcnt++)
                            {
                                Drow[colcnt] = M_DsFreight.Tables[0].Rows[Rcnt][colcnt];
                            }
                            DsFreight.Tables[0].Rows.Add(Drow);
                        }
                    }
                    for (Rcnt = 1; Rcnt <= M_DsFreight.Tables[0].Rows.Count; Rcnt++)
                    {
                        if (int_jobcardpk != Convert.ToInt32(M_DsFreight.Tables[0].Rows[Rcnt]["JOB_CARD_SEA_EXP_FK"]))
                        {

                            for (Rcnt1 = 1; Rcnt1 <= DsFreight.Tables[0].Rows.Count; Rcnt1++)
                            {
                                if (DsFreight.Tables[0].Rows[Rcnt1]["container_type_mst_fk"] == M_DsFreight.Tables[0].Rows[Rcnt]["container_type_mst_fk"])
                                {
                                    if (DsFreight.Tables[0].Rows[Rcnt1]["freight_element_mst_fk"] == M_DsFreight.Tables[0].Rows[Rcnt]["freight_element_mst_fk"])
                                    {
                                        DsFreight.Tables[0].Rows[Rcnt1]["freight_amt"] = Convert.ToInt32(DsFreight.Tables[0].Rows[Rcnt1]["freight_amt"]) + Convert.ToInt32(M_DsFreight.Tables[0].Rows[Rcnt]["freight_amt"]);

                                    }
                                }
                            }
                        }
                    }
                }

                if (M_DsOtherCharge.Tables[0].Rows.Count > 0)
                {
                    DsOtherCharges = M_DsOtherCharge.Clone();
                    for (Rcnt = 1; Rcnt <= M_DsOtherCharge.Tables[0].Rows.Count; Rcnt++)
                    {
                        if (int_jobcardpk == Convert.ToInt32(M_DsOtherCharge.Tables[0].Rows[Rcnt]["JOB_CARD_SEA_EXP_FK"]))
                        {
                            OtherChargeDataRow = DsOtherCharges.Tables[0].NewRow();
                            for (colcnt = 0; colcnt <= M_DsOtherCharge.Tables[0].Columns.Count - 1; colcnt++)
                            {
                                OtherChargeDataRow[colcnt] = M_DsOtherCharge.Tables[0].Rows[Rcnt][colcnt];
                            }
                            DsOtherCharges.Tables[0].Rows.Add(Drow);
                        }
                    }

                    for (Rcnt = 1; Rcnt <= M_DsOtherCharge.Tables[0].Rows.Count; Rcnt++)
                    {
                        if (int_jobcardpk != Convert.ToInt32(M_DsOtherCharge.Tables[0].Rows[Rcnt]["JOB_CARD_SEA_EXP_FK"]))
                        {
                            for (Rcnt1 = 1; Rcnt1 <= DsOtherCharges.Tables[0].Rows.Count; Rcnt1++)
                            {
                                if (DsFreight.Tables[0].Rows[Rcnt1]["container_type_mst_fk"] == M_DsFreight.Tables[0].Rows[Rcnt]["container_type_mst_fk"])
                                {
                                    if (DsFreight.Tables[0].Rows[Rcnt1]["freight_element_mst_fk"] == M_DsFreight.Tables[0].Rows[Rcnt]["freight_element_mst_fk"])
                                    {
                                        DsFreight.Tables[0].Rows[Rcnt1]["freight_amt"] = Convert.ToInt32(DsFreight.Tables[0].Rows[Rcnt1]["freight_amt"]) + Convert.ToInt32(M_DsFreight.Tables[0].Rows[Rcnt]["freight_amt"]);

                                    }
                                }
                            }
                        }
                    }
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


        public object save_SplitBooking_Air(DataSet M_dataset, int int_jobcardpk, int Int_LocationFk, int Int_Bookingpk, bool Merge_Flag, bool Split_Flag, string Merged_Booking_no, int Cargo_type, int Biz_Type)
        {


            int Rowcnt = 0;
            int Int_Job_Cont_pk = 0;
            int Int_Booking_pk = 0;
            arrMessage.Clear();
            WorkFlow objWF = new WorkFlow();
            try
            {

                if (Merge_Flag == true)
                {
                }
                else
                {
                    OracleTransaction TRAN = null;
                    objWF.OpenConnection();
                    TRAN = objWF.MyConnection.BeginTransaction();
                    OracleCommand insCommand = new OracleCommand();
                    OracleCommand updCommand = new OracleCommand();

                    //air
                    if (Biz_Type == 2)
                    {
                        if (M_dataset.Tables[0].Rows.Count > 0)
                        {
                            for (Rowcnt = 0; Rowcnt <= M_dataset.Tables[0].Rows.Count - 1; Rowcnt++)
                            {
                                if (Int_Bookingpk != 0)
                                {
                                    if ((object.ReferenceEquals(M_dataset.Tables[0].Rows[Rowcnt]["BOOKING_SEA_PK"], DBNull.Value)))
                                    {
                                        var _with11 = insCommand;
                                        insCommand.Parameters.Clear();
                                        _with11.Connection = objWF.MyConnection;
                                        _with11.CommandType = CommandType.StoredProcedure;
                                        _with11.CommandText = objWF.MyUserName + ".MERGE_BOOKING_SEA_PKG.SPLIT_BOOKING_air_INS";
                                        var _with12 = _with11.Parameters;
                                        _with12.Add("BOOKING_air_PK_IN", Int_Bookingpk).Direction = ParameterDirection.Input;
                                        _with12.Add("BOOKING_REF_NO_IN", M_dataset.Tables[0].Rows[Rowcnt]["MERGED_BOOKINGNR"]).Direction = ParameterDirection.Input;
                                        _with12.Add("LOCATION_FK_IN", Int_LocationFk).Direction = ParameterDirection.Input;
                                        _with12.Add("VOLUME_IN_CBM_IN", M_dataset.Tables[0].Rows[Rowcnt]["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                                        _with12.Add("GROSS_WEIGHT_IN", M_dataset.Tables[0].Rows[Rowcnt]["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                                        _with12.Add("NET_WEIGHT_IN", M_dataset.Tables[0].Rows[Rowcnt]["NET_WEIGHT"]).Direction = ParameterDirection.Input;
                                        _with12.Add("CHARGEABLE_WEIGHT_IN", M_dataset.Tables[0].Rows[Rowcnt]["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                                        _with12.Add("PACK_COUNT_IN", M_dataset.Tables[0].Rows[Rowcnt]["PACK_COUNT"]).Direction = ParameterDirection.Input;
                                        _with12.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                        var _with13 = objWF.MyDataAdapter;
                                        _with13.InsertCommand = insCommand;
                                        _with13.InsertCommand.Transaction = TRAN;
                                        _with13.InsertCommand.ExecuteNonQuery();
                                        Int_Booking_pk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);

                                    }
                                    else if (Convert.ToInt32(M_dataset.Tables[0].Rows[Rowcnt]["BOOKING_SEA_PK"]) != Int_Bookingpk)
                                    {
                                    }
                                }
                            }
                        }
                    }
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        objWF.CloseConnection();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Commit();
                        arrMessage.Add("All data saved successfully");
                        objWF.CloseConnection();
                        return arrMessage;
                    }
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;

            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }


        }
        #region "Save Split Bookings"
        public object save_SplitBooking(DataSet M_dataset, int Base_BkgPK, string Base_BkgNr, int Cargo_type, int Biz_Type)
        {

            int Rowcnt = 0;
            int Int_Job_Cont_pk = 0;
            int Int_Booking_pk = 0;
            arrMessage.Clear();
            WorkFlow objWF = new WorkFlow();

            try
            {
                UPDATE_MAINBKGNR(Base_BkgNr);
                arrMessage.Clear();
                OracleTransaction TRAN = null;
                objWF.OpenConnection();
                TRAN = objWF.MyConnection.BeginTransaction();
                OracleCommand insCommand = new OracleCommand();
                OracleCommand updCommand = new OracleCommand();


                //sea
                if (Biz_Type == 1)
                {
                    if (M_dataset.Tables[0].Rows.Count > 0)
                    {
                        for (Rowcnt = 0; Rowcnt <= M_dataset.Tables[0].Rows.Count - 1; Rowcnt++)
                        {
                            var _with14 = insCommand;
                            insCommand.Parameters.Clear();
                            _with14.Connection = objWF.MyConnection;
                            _with14.CommandType = CommandType.StoredProcedure;
                            _with14.CommandText = objWF.MyUserName + ".MERGE_BOOKING_SEA_PKG.NEW_SPLIT_BOOKING_SEA_INS";
                            var _with15 = _with14.Parameters;
                            _with15.Add("BASE_BOOKING_PK_IN", Base_BkgPK).Direction = ParameterDirection.Input;
                            _with15.Add("BASE_BOOKING_NR_IN", Base_BkgNr).Direction = ParameterDirection.Input;
                            _with15.Add("NEW_BOOKING_NR_IN", M_dataset.Tables[0].Rows[Rowcnt]["NEW_BOOKING_NR"]).Direction = ParameterDirection.Input;
                            _with15.Add("JOB_TRN_FKS_IN", M_dataset.Tables[0].Rows[Rowcnt]["JOB_TRN_FKS"]).Direction = ParameterDirection.Input;
                            _with15.Add("NUM_LOCATION", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                            _with15.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            var _with16 = objWF.MyDataAdapter;
                            _with16.InsertCommand = insCommand;
                            _with16.InsertCommand.Transaction = TRAN;
                            _with16.InsertCommand.ExecuteNonQuery();
                            Int_Booking_pk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                        }
                    }
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        objWF.CloseConnection();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Commit();
                        arrMessage.Add("All data saved successfully");
                        objWF.CloseConnection();
                        return arrMessage;
                    }
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }


        }
        public object SAVE_LCL_SplitBooking(DataSet M_dataset, int Base_BkgPK, string Base_BkgNr, int Cargo_type, int Biz_Type)
        {

            int Rowcnt = 0;
            int Int_Job_Cont_pk = 0;
            int Int_Booking_pk = 0;
            arrMessage.Clear();
            WorkFlow objWF = new WorkFlow();
            UPDATE_MAINBKGNR(Base_BkgNr);
            arrMessage.Clear();
            try
            {
                OracleTransaction TRAN = null;
                objWF.OpenConnection();
                TRAN = objWF.MyConnection.BeginTransaction();
                OracleCommand insCommand = new OracleCommand();
                OracleCommand updCommand = new OracleCommand();

                //sea
                if (Biz_Type == 1)
                {
                    if (M_dataset.Tables[0].Rows.Count > 0)
                    {
                        for (Rowcnt = 0; Rowcnt <= M_dataset.Tables[0].Rows.Count - 1; Rowcnt++)
                        {
                            var _with17 = insCommand;
                            insCommand.Parameters.Clear();
                            _with17.Connection = objWF.MyConnection;
                            _with17.CommandType = CommandType.StoredProcedure;
                            _with17.CommandText = objWF.MyUserName + ".MERGE_BOOKING_SEA_PKG.SPLIT_BOOKING_SEA_INS";
                            var _with18 = _with17.Parameters;
                            _with18.Add("BOOKING_SEA_PK_IN", Base_BkgPK).Direction = ParameterDirection.Input;
                            _with18.Add("BOOKING_REF_NO_IN", M_dataset.Tables[0].Rows[Rowcnt]["MERGED_BOOKINGNR"]).Direction = ParameterDirection.Input;
                            _with18.Add("LOCATION_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                            _with18.Add("VOLUME_IN_CBM_IN", M_dataset.Tables[0].Rows[Rowcnt]["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                            _with18.Add("GROSS_WEIGHT_IN", M_dataset.Tables[0].Rows[Rowcnt]["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                            _with18.Add("NET_WEIGHT_IN", M_dataset.Tables[0].Rows[Rowcnt]["NET_WEIGHT"]).Direction = ParameterDirection.Input;
                            _with18.Add("CHARGEABLE_WEIGHT_IN", M_dataset.Tables[0].Rows[Rowcnt]["NET_WEIGHT"]).Direction = ParameterDirection.Input;
                            _with18.Add("PACK_COUNT_IN", M_dataset.Tables[0].Rows[Rowcnt]["PACK_COUNT"]).Direction = ParameterDirection.Input;
                            _with18.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            var _with19 = objWF.MyDataAdapter;
                            _with19.InsertCommand = insCommand;
                            _with19.InsertCommand.Transaction = TRAN;
                            _with19.InsertCommand.ExecuteNonQuery();
                            Int_Booking_pk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);

                        }
                    }
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    objWF.CloseConnection();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All data saved successfully");
                    objWF.CloseConnection();
                    return arrMessage;
                }

                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }
        public object UPDATE_MAINBKGNR(string Base_BkgNr)
        {
            arrMessage.Clear();
            WorkFlow objWF = new WorkFlow();
            try
            {
                OracleTransaction TRAN = null;
                objWF.OpenConnection();
                TRAN = objWF.MyConnection.BeginTransaction();
                OracleCommand insCommand = new OracleCommand();
                var _with20 = insCommand;
                insCommand.Parameters.Clear();
                _with20.Connection = objWF.MyConnection;
                _with20.CommandType = CommandType.StoredProcedure;
                _with20.CommandText = objWF.MyUserName + ".MERGE_BOOKING_SEA_PKG.UPDATE_MERGED_STATUS";
                var _with21 = insCommand;
                _with21.Parameters.Add("MERGED_BKGFK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with21.Parameters["MERGED_BKGFK_IN"].SourceVersion = DataRowVersion.Current;
                _with21.Parameters.Add("MERGED_BKGNR_IN", Base_BkgNr).Direction = ParameterDirection.Input;
                _with21.Parameters["MERGED_BKGNR_IN"].SourceVersion = DataRowVersion.Current;
                _with21.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with22 = objWF.MyDataAdapter;
                _with22.InsertCommand = insCommand;
                _with22.InsertCommand.Transaction = TRAN;
                _with22.InsertCommand.ExecuteNonQuery();
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    objWF.CloseConnection();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All data saved successfully");
                    objWF.CloseConnection();
                    return arrMessage;
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }
        public DataSet GetBookingHeader(DataSet dsHeader, int Bookingpk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("SELECT DISTINCT BK.CUST_CUSTOMER_MST_FK,");
            sb.Append("                BK.CONS_CUSTOMER_MST_FK,");
            sb.Append("                BT.TRANS_REFERED_FROM,");
            sb.Append("                BK.PORT_MST_POL_FK,");
            sb.Append("                BK.PORT_MST_POD_FK,");
            sb.Append("                BK.COMMODITY_GROUP_FK,");
            sb.Append("                BK.OPERATOR_MST_FK,");
            sb.Append("                BK.SHIPMENT_DATE,");
            sb.Append("                BK.CARGO_TYPE,");
            sb.Append("                BK.CARGO_MOVE_FK,");
            sb.Append("                BK.PYMT_TYPE,");
            sb.Append("                BK.COL_PLACE_MST_FK,");
            sb.Append("                BK.COL_ADDRESS,");
            sb.Append("                BK.DEL_PLACE_MST_FK,");
            sb.Append("                BK.DEL_ADDRESS,");
            sb.Append("                BK.LINE_BKG_NO,");
            sb.Append("                BK.VESSEL_NAME,");
            sb.Append("                BK.VOYAGE,");
            sb.Append("                BK.ETA_DATE,");
            sb.Append("                BK.ETD_DATE,");
            sb.Append("                BK.CUT_OFF_DATE,");
            sb.Append("                BK.STATUS,");
            sb.Append("                BK.SHIPPING_TERMS_MST_FK,");
            sb.Append("                BK.CUSTOMER_REF_NO,");
            sb.Append("                BK.DP_AGENT_MST_FK,");
            sb.Append("                BK.VESSEL_VOYAGE_FK,");
            sb.Append("                BK.CREDIT_DAYS,");
            sb.Append("                BK.CREDIT_LIMIT,");
            sb.Append("                BK.BASE_CURRENCY_FK,");
            sb.Append("                BK.MARKS_NUMBERS,");
            sb.Append("                BK.GOODS_DESCRIPTION,");
            sb.Append("                BT.TRANS_REF_NO");
            sb.Append("  FROM BOOKING_SEA_TBL BK, BOOKING_TRN_SEA_FCL_LCL BT");
            sb.Append(" WHERE BK.BOOKING_SEA_PK = BT.BOOKING_SEA_FK");
            sb.Append("   AND BK.BOOKING_SEA_PK = " + Bookingpk);
            return objWF.GetDataSet(sb.ToString());
        }
        public DataSet FillBookingHeader(DataSet M_dataset)
        {
            int i = 0;
            int j = 0;
            DataRow dr = null;
            DataView dv = new DataView();
            DataSet TempDS = new DataSet();
            TempDS = GetEmptyDataSet();

            for (j = 0; j <= M_dataset.Tables.Count - 1; j++)
            {
                dv = M_dataset.Tables[0].DefaultView;
                dv.Sort = "BOOKING_REF_NO";
                dv.RowFilter = "MERGED_BOOKINGNR in ('" + M_dataset.Tables[0].Rows[j]["MERGED_BOOKINGNR"] + "')";
                if (dv.Count != 0)
                {
                    break; // TODO: might not be correct. Was : Exit For
                }
            }

            for (i = 0; i <= dv.Count - 1; i++)
            {
                dr = TempDS.Tables[0].NewRow();
                var _with23 = dr;
                _with23["TRANS_REFERED_FROM"] = DBNull.Value;
                _with23["TRANS_REF_NO"] = DBNull.Value;
                _with23["BOOKING_REF_NO"] = dv[i]["MERGED_BOOKINGNR"];
                _with23["CONTAINER_TYPE_MST_PK"] = dv[i]["CONTAINER_TYPE_MST_PK"];
                _with23["NO_OF_BOXES"] = dv.Count;
                _with23["BASIS"] = DBNull.Value;
                _with23["COMMODITY_GROUP_FK"] = DBNull.Value;
                _with23["COMMODITY_MST_FK"] = DBNull.Value;
                _with23["ALL_IN_TARIFF"] = DBNull.Value;
                _with23["BUYING_RATE"] = DBNull.Value;
                TempDS.Tables[0].Rows.Add(dr);
            }
            return TempDS;
        }

        private DataSet GetEmptyDataSet()
        {
            DataSet DS = new DataSet();
            DataTable dTab = new DataTable();
            DataRow dRow = null;
            Int16 i = default(Int16);
            Int16 j = default(Int16);
            var _with24 = dTab;
            _with24.Columns.Add(new DataColumn("TRANS_REFERED_FROM"));
            _with24.Columns.Add(new DataColumn("TRANS_REF_NO"));
            _with24.Columns.Add(new DataColumn("BOOKING_REF_NO"));
            _with24.Columns.Add(new DataColumn("CONTAINER_TYPE_MST_PK", typeof(System.Double)));
            _with24.Columns.Add(new DataColumn("NO_OF_BOXES", typeof(System.Double)));
            _with24.Columns.Add(new DataColumn("BASIS", typeof(System.Double)));
            _with24.Columns.Add(new DataColumn("QUANTITY", typeof(System.Double)));
            _with24.Columns.Add(new DataColumn("COMMODITY_GROUP_FK", typeof(System.Double)));
            _with24.Columns.Add(new DataColumn("COMMODITY_MST_FK", typeof(System.Double)));
            _with24.Columns.Add(new DataColumn("ALL_IN_TARIFF", typeof(System.Double)));
            _with24.Columns.Add(new DataColumn("BUYING_RATE", typeof(System.Double)));
            DS.Tables.Add(dTab);
            return DS;
        }
        #endregion

        public ArrayList save_MergeBooking(DataSet M_dataset, WorkFlow objWF, int int_jobcardpk, int Int_LocationFk, int Int_Bookingpk, bool Merge_Flag, bool Split_Flag, string Merged_Booking_no, int Cargo_type, int Biz_Type,
        string New_Bkg_Nr, string MainBkgPK)
        {

            int Rowcnt = 0;
            int Int_Job_Cont_pk = 0;
            int Int_Booking_pk = 0;
            arrMessage.Clear();
            try
            {
                if (Merge_Flag == true)
                {
                    if (Biz_Type == 1)
                    {
                        if (M_dataset.Tables[0].Rows.Count > 0)
                        {

                            for (Rowcnt = 0; Rowcnt <= M_dataset.Tables[0].Rows.Count - 1; Rowcnt++)
                            {
                                if (int_jobcardpk != 0)
                                {
                                    if ((Convert.ToInt32(M_dataset.Tables[0].Rows[Rowcnt]["JOB_CARD_SEA_EXP_PK"]) != 0))
                                    {
                                        var _with25 = objWF.MyCommand;
                                        _with25.CommandType = CommandType.StoredProcedure;
                                        _with25.CommandText = objWF.MyUserName + ".MERGE_BOOKING_SEA_PKG.JOB_TRN_SEA_EXP_CONT_INS";
                                        _with25.Parameters.Clear();
                                        _with25.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", int_jobcardpk).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("BOOKING_REF_NO_IN", M_dataset.Tables[0].Rows[Rowcnt]["BOOKING_REF_NO"]).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("CONTAINER_NUMBER_IN", M_dataset.Tables[0].Rows[Rowcnt]["CONTAINER_NUMBER"]).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", M_dataset.Tables[0].Rows[Rowcnt]["CONTAINER_TYPE_MST_PK"]).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("SEAL_NUMBER_IN", M_dataset.Tables[0].Rows[Rowcnt]["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("VOLUME_IN_CBM_IN", M_dataset.Tables[0].Rows[Rowcnt]["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("GROSS_WEIGHT_IN", M_dataset.Tables[0].Rows[Rowcnt]["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("NET_WEIGHT_IN", M_dataset.Tables[0].Rows[Rowcnt]["NET_WEIGHT"]).Direction = ParameterDirection.Input;

                                        _with25.Parameters.Add("CHARGEABLE_WEIGHT_IN", M_dataset.Tables[0].Rows[Rowcnt]["NET_WEIGHT"]).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("PACK_TYPE_MST_FK_IN", M_dataset.Tables[0].Rows[Rowcnt]["PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("PACK_COUNT_IN", M_dataset.Tables[0].Rows[Rowcnt]["PACK_COUNT"]).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("NEW_BKG_NR_IN", New_Bkg_Nr).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("MAIN_BKG_PK_IN", MainBkgPK).Direction = ParameterDirection.Input;
                                        _with25.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                        _with25.ExecuteNonQuery();
                                        strRet = (string.IsNullOrEmpty(_with25.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with25.Parameters["RETURN_VALUE"].Value.ToString());
                                    }
                                }
                            }
                        }
                    }
                }

                arrMessage.Add("All data saved successfully");
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return arrMessage;
        }
        #endregion

        #region "Save"
        public ArrayList SaveBooking(DataSet dsMain, DataSet M_dataset, DataSet M_DSProfit, object txtBookingNo, long nLocationId, long nEmpId, string Measure, string Wt, string Divfac, string Merge_Booking_Fks,
        string strPolPk = "", string strPodPk = "", string strFreightpk = "", Int16 intIsLcl = 0, string strBStatus = "", string strVoyagepk = "", string PODLocfk = "", string ShipperPK = "", string Consigne = "", string StrBookingRefNr = "",
        int Int_Bookingpk = 0)
        {

            string EbkgRefno = null;
            string BookingRefNo = null;
            string BookingRef = "";
            string EBookingRef = null;
            bool IsUpdate = false;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["BOOKING_SEA_PK"].ToString()))
                {

                    BookingRefNo = StrBookingRefNr;

                    var _with26 = objWK.MyCommand;
                    _with26.CommandType = CommandType.StoredProcedure;
                    _with26.CommandText = objWK.MyUserName + ".MERGE_BOOKING_SEA_PKG.UPDATE_MERGED_STATUS";

                    _with26.Parameters.Clear();
                    _with26.Parameters.Add("MERGED_BKGFK_IN", Merge_Booking_Fks).Direction = ParameterDirection.Input;
                    _with26.Parameters["MERGED_BKGFK_IN"].SourceVersion = DataRowVersion.Current;

                    _with26.Parameters.Add("MERGED_BKGNR_IN", BookingRefNo).Direction = ParameterDirection.Input;
                    _with26.Parameters["MERGED_BKGNR_IN"].SourceVersion = DataRowVersion.Current;

                    _with26.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with26.ExecuteNonQuery();

                    objWK.MyCommand.Parameters.Clear();
                    var _with27 = objWK.MyCommand;
                    _with27.CommandType = CommandType.StoredProcedure;
                    _with27.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_INS";

                    _with27.Parameters.Add("BOOKING_REF_NO_IN", BookingRefNo).Direction = ParameterDirection.Input;
                    _with27.Parameters["BOOKING_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("BOOKING_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["BOOKING_DATE"])).Direction = ParameterDirection.Input;
                    _with27.Parameters["BOOKING_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUST_CUSTOMER_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CUST_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["CUST_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("CUSTOMER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CONS_CUSTOMER_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("CONS_CUSTOMER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("CONS_CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CONS_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["CONS_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;


                    _with27.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;
                    _with27.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
                    _with27.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"])).Direction = ParameterDirection.Input;
                    _with27.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("OPERATOR_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with27.Parameters.Add("OPR_UPDATE_STATUS_IN", 1).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
                        _with27.Parameters.Add("OPR_UPDATE_STATUS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["SHIPMENT_DATE"].ToString()))
                    {
                        _with27.Parameters.Add("SHIPMENT_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("SHIPMENT_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["SHIPMENT_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["SHIPMENT_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
                    _with27.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CARGO_MOVE_FK"].ToString()))
                    {
                        _with27.Parameters.Add("CARGO_MOVE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("CARGO_MOVE_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CARGO_MOVE_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PYMT_TYPE"].ToString()))
                    {
                        _with27.Parameters.Add("PYMT_TYPE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("PYMT_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["PYMT_TYPE"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["COL_PLACE_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("COL_PLACE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("COL_PLACE_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COL_PLACE_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["COL_ADDRESS"].ToString()))
                    {
                        _with27.Parameters.Add("COL_ADDRESS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("COL_ADDRESS_IN", dsMain.Tables["tblMaster"].Rows[0]["COL_ADDRESS"]).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["COL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("DEL_PLACE_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["DEL_PLACE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with27.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["DEL_ADDRESS"].ToString()))
                    {
                        _with27.Parameters.Add("DEL_ADDRESS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("DEL_ADDRESS_IN", dsMain.Tables["tblMaster"].Rows[0]["DEL_ADDRESS"]).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CB_AGENT_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("CB_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("CB_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CB_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CL_AGENT_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("CL_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("CL_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CL_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PACK_TYP_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("PACK_TYP_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("PACK_TYP_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PACK_TYP_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["PACK_TYP_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PACK_COUNT"].ToString()))
                    {
                        _with27.Parameters.Add("PACK_COUNT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("PACK_COUNT_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["PACK_COUNT"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["GROSS_WEIGHT"].ToString()))
                    {
                        _with27.Parameters.Add("GROSS_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("GROSS_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["GROSS_WEIGHT"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    if (intIsLcl == -1)
                    {
                        _with27.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with27.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"].ToString()))
                        {
                            _with27.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        _with27.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["NET_WEIGHT"].ToString()))
                        {
                            _with27.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Parameters.Add("NET_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["NET_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        _with27.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        _with27.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with27.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VOLUME_IN_CBM"].ToString()))
                    {
                        _with27.Parameters.Add("VOLUME_IN_CBM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("VOLUME_IN_CBM_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["VOLUME_IN_CBM"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_NO"].ToString()))
                    {
                        _with27.Parameters.Add("LINE_BKG_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("LINE_BKG_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_NO"]).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["LINE_BKG_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("VESSEL_NAME_IN", dsMain.Tables["tblMaster"].Rows[0]["VESSEL_NAME"]).Direction = ParameterDirection.Input;
                    _with27.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("VOYAGE_IN", dsMain.Tables["tblMaster"].Rows[0]["VOYAGE"]).Direction = ParameterDirection.Input;
                    _with27.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["ETD_DATE"].ToString()))
                    {
                        _with27.Parameters.Add("ETD_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("ETD_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUT_OFF_DATE"].ToString()))
                    {
                        _with27.Parameters.Add("CUT_OFF_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with27.Parameters.Add("CUT_OFF_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["CUT_OFF_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["CUT_OFF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["ETA_DATE"].ToString()))
                    {
                        _with27.Parameters.Add("ETA_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with27.Parameters.Add("ETA_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("STATUS_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;
                    _with27.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["SHIPPING_TERMS_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with27.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["SHIPPING_TERMS_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_REF_NO"].ToString()))
                    {
                        _with27.Parameters.Add("CUSTOMER_REF_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with27.Parameters.Add("CUSTOMER_REF_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_REF_NO"]).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["CUSTOMER_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["DP_AGENT_MST_FK"].ToString()))
                    {
                        _with27.Parameters.Add("DP_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with27.Parameters.Add("DP_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["DP_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_VOYAGE_FK"].ToString()))
                    {
                        _with27.Parameters.Add("VESSEL_VOYAGE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with27.Parameters.Add("VESSEL_VOYAGE_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_VOYAGE_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with27.Parameters.Add("CREDIT_LIMIT_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CREDIT_LIMIT"])).Direction = ParameterDirection.Input;
                    _with27.Parameters.Add("CREDIT_DAYS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CREDIT_DAYS"])).Direction = ParameterDirection.Input;

                    _with27.Parameters["VESSEL_VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with27.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;

                    _with27.Parameters.Add("CONFIG_PK_IN", 3050).Direction = ParameterDirection.Input;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["BASE_CURRENCY_FK"].ToString()))
                    {
                        _with27.Parameters.Add("BASE_CURRENCY_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with27.Parameters.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["BASE_CURRENCY_FK"])).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["MARKS_NUMBER"].ToString()))
                    {
                        _with27.Parameters.Add("MARKS_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with27.Parameters.Add("MARKS_NUMBER_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["MARKS_NUMBER"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["GOODS_DESC"].ToString()))
                    {
                        _with27.Parameters.Add("GOODS_DESC_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with27.Parameters.Add("GOODS_DESC_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["GOODS_DESC"])).Direction = ParameterDirection.Input;
                    }

                    _with27.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;


                    _with27.ExecuteNonQuery();

                }
                else
                {
                    IsUpdate = true;

                    if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                    {
                        Int16 BookingPK = default(Int16);
                        string EBKSbr = null;
                        Int16 status = default(Int16);
                        BookingPK = Convert.ToInt16(dsMain.Tables["tblMaster"].Rows[0]["BOOKING_SEA_PK"].ToString());
                        Chk_EBK = FetchEBKN(BookingPK);
                        EBookingRef = txtBookingNo.ToString();
                        status = Convert.ToInt16(dsMain.Tables["tblMaster"].Rows[0]["STATUS"].ToString());
                        if (Chk_EBK == 1 & status == 2)
                        {
                            BookingRefNo = GenerateBookingNo(nLocationId, nEmpId, Convert.ToInt32(HttpContext.Current.Session["USER_PK"]), objWK);
                            if (BookingRefNo == "Protocol Not Defined.")
                            {
                                arrMessage.Add("Protocol Not Defined.");
                                return arrMessage;
                            }
                        }
                        else
                        {
                            BookingRefNo = txtBookingNo.ToString();
                        }
                        txtBookingNo = BookingRefNo;
                        EbkgRefno = BookingRef;
                        BookingRef = BookingRefNo;
                    }
                    else
                    {
                        BookingRefNo = txtBookingNo.ToString();
                    }
                    var _with28 = objWK.MyCommand;
                    _with28.CommandType = CommandType.StoredProcedure;
                    _with28.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_UPD";



                    _with28.Parameters.Clear();
                    _with28.Parameters.Add("BOOKING_SEA_PK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["BOOKING_SEA_PK"])).Direction = ParameterDirection.Input;
                    _with28.Parameters["BOOKING_SEA_PK_IN"].SourceVersion = DataRowVersion.Current;



                    _with28.Parameters.Add("BOOKING_REF_NO_IN", BookingRefNo).Direction = ParameterDirection.Input;
                    _with28.Parameters["BOOKING_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("BOOKING_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["BOOKING_DATE"])).Direction = ParameterDirection.Input;
                    _with28.Parameters["BOOKING_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUST_CUSTOMER_MST_FK"].ToString()))
                    {
                        _with28.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CUST_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["CUST_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CONS_CUSTOMER_MST_FK"].ToString()))
                    {
                        _with28.Parameters.Add("CONS_CUSTOMER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("CONS_CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CONS_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["CONS_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;
                    _with28.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
                    _with28.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"])).Direction = ParameterDirection.Input;
                    _with28.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;


                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"].ToString()))
                    {
                        _with28.Parameters.Add("OPERATOR_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with28.Parameters.Add("OPR_UPDATE_STATUS_IN", 1).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
                        _with28.Parameters.Add("OPR_UPDATE_STATUS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["SHIPMENT_DATE"].ToString()))
                    {
                        _with28.Parameters.Add("SHIPMENT_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("SHIPMENT_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["SHIPMENT_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["SHIPMENT_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
                    _with28.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CARGO_MOVE_FK"].ToString()))
                    {
                        _with28.Parameters.Add("CARGO_MOVE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("CARGO_MOVE_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CARGO_MOVE_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PYMT_TYPE"].ToString()))
                    {
                        _with28.Parameters.Add("PYMT_TYPE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("PYMT_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["PYMT_TYPE"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["COL_PLACE_MST_FK"].ToString()))
                    {
                        _with28.Parameters.Add("COL_PLACE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("COL_PLACE_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COL_PLACE_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["COL_ADDRESS"].ToString()))
                    {
                        _with28.Parameters.Add("COL_ADDRESS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("COL_ADDRESS_IN", dsMain.Tables["tblMaster"].Rows[0]["COL_ADDRESS"]).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["COL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("DEL_PLACE_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["DEL_PLACE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with28.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["DEL_ADDRESS"].ToString()))
                    {
                        _with28.Parameters.Add("DEL_ADDRESS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("DEL_ADDRESS_IN", dsMain.Tables["tblMaster"].Rows[0]["DEL_ADDRESS"]).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CB_AGENT_MST_FK"].ToString()))
                    {
                        _with28.Parameters.Add("CB_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("CB_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CB_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CL_AGENT_MST_FK"].ToString()))
                    {
                        _with28.Parameters.Add("CL_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("CL_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CL_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PACK_TYP_MST_FK"].ToString()))
                    {
                        _with28.Parameters.Add("PACK_TYP_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("PACK_TYP_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PACK_TYP_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["PACK_TYP_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PACK_COUNT"].ToString()))
                    {
                        _with28.Parameters.Add("PACK_COUNT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("PACK_COUNT_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["PACK_COUNT"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["GROSS_WEIGHT"].ToString()))
                    {
                        _with28.Parameters.Add("GROSS_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("GROSS_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["GROSS_WEIGHT"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    if (intIsLcl == -1)
                    {
                        _with28.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with28.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"].ToString()))
                        {
                            _with28.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with28.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        _with28.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["NET_WEIGHT"].ToString()))
                        {
                            _with28.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with28.Parameters.Add("NET_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["NET_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        _with28.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        _with28.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with28.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VOLUME_IN_CBM"].ToString()))
                    {
                        _with28.Parameters.Add("VOLUME_IN_CBM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("VOLUME_IN_CBM_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["VOLUME_IN_CBM"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_NO"].ToString()))
                    {
                        _with28.Parameters.Add("LINE_BKG_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("LINE_BKG_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_NO"]).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["LINE_BKG_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("VESSEL_NAME_IN", dsMain.Tables["tblMaster"].Rows[0]["VESSEL_NAME"]).Direction = ParameterDirection.Input;
                    _with28.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("VOYAGE_IN", dsMain.Tables["tblMaster"].Rows[0]["VOYAGE"]).Direction = ParameterDirection.Input;
                    _with28.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["ETD_DATE"].ToString()))
                    {
                        _with28.Parameters.Add("ETD_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("ETD_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUT_OFF_DATE"].ToString()))
                    {
                        _with28.Parameters.Add("CUT_OFF_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with28.Parameters.Add("CUT_OFF_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["CUT_OFF_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["CUT_OFF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["ETA_DATE"].ToString()))
                    {
                        _with28.Parameters.Add("ETA_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("ETA_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("STATUS_IN", dsMain.Tables["tblMaster"].Rows[0]["STATUS"]).Direction = ParameterDirection.Input;
                    _with28.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["SHIPPING_TERMS_MST_FK"].ToString()))
                    {
                        _with28.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with28.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["SHIPPING_TERMS_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_REF_NO"].ToString()))
                    {
                        _with28.Parameters.Add("CUSTOMER_REF_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with28.Parameters.Add("CUSTOMER_REF_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_REF_NO"]).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["CUSTOMER_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["DP_AGENT_MST_FK"].ToString()))
                    {
                        _with28.Parameters.Add("DP_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with28.Parameters.Add("DP_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["DP_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_VOYAGE_FK"].ToString()))
                    {
                        _with28.Parameters.Add("VESSEL_VOYAGE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with28.Parameters.Add("VESSEL_VOYAGE_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_VOYAGE_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters["VESSEL_VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with28.Parameters.Add("CREDIT_LIMIT_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CREDIT_LIMIT"])).Direction = ParameterDirection.Input;
                    _with28.Parameters.Add("CREDIT_DAYS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CREDIT_DAYS"])).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("VERSION_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["VERSION_NO"]).Direction = ParameterDirection.Input;
                    _with28.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("CONFIG_PK_IN", 3050).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["MARKS_NUMBER"].ToString()))
                    {
                        _with28.Parameters.Add("MARKS_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with28.Parameters.Add("MARKS_NUMBER_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["MARKS_NUMBER"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["GOODS_DESC"].ToString()))
                    {
                        _with28.Parameters.Add("GOODS_DESC_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with28.Parameters.Add("GOODS_DESC_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["GOODS_DESC"])).Direction = ParameterDirection.Input;
                    }

                    _with28.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with28.ExecuteNonQuery();
                }
                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "booking")>0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));

                    if (!IsUpdate)
                    {
                        RollbackProtocolKey("BOOKING (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                    }

                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValueMain = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                arrMessage = SaveBookingCDimension(dsMain, _PkValueMain, objWK.MyCommand, IsUpdate, Measure, Wt, Divfac);
                //Save Cargo Dimension
                arrMessage = SaveBookingOFreight(dsMain, _PkValueMain, objWK.MyCommand, IsUpdate);
                //Save Other Freights/Flat Freights
                arrMessage = SaveBookingTRN(dsMain, _PkValueMain, objWK.MyCommand, IsUpdate);
                //Save the Transaction and Freights
                //'
                arrMessage = SaveProfitability(M_DSProfit, _PkValueMain, objWK);
                //'
                if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                {
                    return arrMessage;
                }
                //Job card inserted only for Booking Confirmation For first time
                if ((Convert.ToInt32(strBStatus) == 2 | (Convert.ToInt32(strBStatus) == 5 & IsUpdate == false)))
                {
                    arrMessage = SaveJobCard(M_dataset, _PkValueMain, objWK, Convert.ToString(nLocationId), PODLocfk, BookingRefNo, Int_Bookingpk, ShipperPK, Consigne, strVoyagepk);
                }
                //Booking Confirmed through updation
                if (IsUpdate == true & Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]) == 2)
                {
                    arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(_PkValueMain), 2, 1, "Job Card", "JOB-INS-SEA-EXP", Convert.ToInt32(nLocationId), objWK, "UPD", Convert.ToInt32(HttpContext.Current.Session["USER_PK"]), "O");
                    //New Confirmed Booking
                }
                else if (IsUpdate == false & Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]) == 2)
                {
                    arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(_PkValueMain), 2, 1, "Job Card", "JOB-INS-SEA-EXP", Convert.ToInt32(nLocationId), objWK, "INS", Convert.ToInt32(HttpContext.Current.Session["USER_PK"]), "O");
                }

                OracleDataReader oRead = null;
                string EmailId = null;
                string CustBID = null;
                string statusBKG = null;
                System.Text.StringBuilder strsql = new System.Text.StringBuilder();
                Int32 chk = 0;

                if (arrMessage.Count > 0)
                {
                    if (string.Compare((Convert.ToString(arrMessage[0]).ToUpper()), "SAVED")>0)
                    {
                        arrMessage.Clear();
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();

                        if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                        {
                            statusBKG = Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]);
                            if (!string.IsNullOrEmpty(BookingRefNo) & Chk_EBK == 1)
                            {
                                string BkgDate = null;
                                BkgDate = FetchBkgDate(BookingRef);
                                if (statusBKG == "3")
                                {
                                    strsql.Append(" select addr.email_id email_id, book.cust_reg_nr custid from syn_ebk_m_booking book,syn_ebk_t_cust_address addr where book.qbso_bkg_ref_nr like '" + BookingRef.ToUpper() + "' ");
                                    strsql.Append(" and book.cust_reg_nr=addr.regn_nr_fk and addr.address_type=0 group by addr.email_id,book.cust_reg_nr ");
                                    oRead = objWK.GetDataReader(strsql.ToString());
                                    while ((oRead.Read()))
                                    {
                                        chk = 1;
                                        EmailId = Convert.ToString(oRead.GetValue(0));
                                        CustBID = Convert.ToString(oRead.GetValue(1));
                                    }
                                    oRead.Close();
                                }
                                DataSet dscust = new DataSet();
                                Int32 Bstatus = Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]);
                                if (statusBKG == "2")
                                {
                                    strsql.Append(" select addr.email_id email_id, book.cust_reg_nr custid from syn_ebk_m_booking book,syn_ebk_t_cust_address addr where book.qbso_bkg_ref_nr like '" + BookingRef.ToUpper() + "' ");
                                    strsql.Append(" and book.cust_reg_nr=addr.regn_nr_fk and addr.address_type=0 group by addr.email_id,book.cust_reg_nr ");
                                    oRead = objWK.GetDataReader(strsql.ToString());
                                    while ((oRead.Read()))
                                    {
                                        chk = 1;
                                        EmailId = Convert.ToString(oRead.GetValue(0));
                                        CustBID = Convert.ToString(oRead.GetValue(1));
                                    }
                                    oRead.Close();
                                }
                                if (statusBKG == "2" | statusBKG == "3")
                                {
                                    if (chk > 0)
                                    {
                                        SendMail(EmailId, CustBID, BookingRef, EbkgRefno, Bstatus);
                                    }
                                }
                                return arrMessage;
                            }
                        }
                        else
                        {
                            txtBookingNo = BookingRefNo;
                        }


                        if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]) == 3)
                        {
                            arrMessage.Clear();
                            arrMessage.Add("Booking Cancelled and JobCard Closed Sucessfully.");

                        }

                        return arrMessage;

                    }
                    else
                    {
                        if (!IsUpdate)
                        {
                            RollbackProtocolKey("BOOKING (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                        }

                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

            }
            catch (OracleException oraexp)
            {
                if (!IsUpdate)
                {
                    RollbackProtocolKey("BOOKING (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                }
                TRAN.Rollback();
                arrMessage.Clear();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                if (!IsUpdate)
                {
                    RollbackProtocolKey("BOOKING (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                }
                TRAN.Rollback();
                arrMessage.Clear();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }

        public ArrayList Save_ContainerInjobcard(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby, string PkStatus,
        OracleTransaction TRAN = null)
        {

            Int32 retVal = default(Int32);
            Int32 RecAfct = default(Int32);
            try
            {
                arrMessage.Clear();
                if ((TRAN != null))
                {
                    objWF.MyCommand.Connection = TRAN.Connection;
                }
                var _with29 = objWF.MyCommand;
                _with29.CommandType = CommandType.StoredProcedure;
                _with29.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
                if ((TRAN != null))
                {
                    _with29.Transaction = TRAN;
                }
                _with29.Parameters.Clear();
                _with29.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("Container_Data_in", DBNull.Value).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with29.ExecuteNonQuery();
                arrMessage.Add("All Data Saved Successfully");
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

        public object SendMail(string MailId, string CUSTOMERID, string BkgRefnr, string EBkgRefnr, Int32 Bstatus)
        {
            System.Web.Mail.MailMessage objMail = new System.Web.Mail.MailMessage();
            string EAttach = null;
            string dsMail = null;
            Int32 intCnt = default(Int32);
            System.Text.StringBuilder strhtml = new System.Text.StringBuilder();


            try
            {
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserver"] = "smtpout.secureserver.net";
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserverport"] = 25;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendusing"] = 2;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpauthenticate"] = 1;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendusername"] = "support_temp@quantum-bso.com";
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendpassword"] = "test123";
                objMail.BodyFormat = System.Web.Mail.MailFormat.Html;
                //or MailFormat.Text 
                objMail.To = MailId;
                objMail.From = "support_temp@quantum-bso.com";
                if (Bstatus == 3)
                {
                    objMail.Subject = "Booking Cancelled";

                    strhtml.Append("<html><body>");
                    strhtml.Append("<p><b>Dear " + CUSTOMERID + " <br>");
                    strhtml.Append("Your Request for the E-Booking " + EBkgRefnr + " is Canceled <br><br>");
                }
                else
                {
                    objMail.Subject = "Booking Confirmation";
                    strhtml.Append("<html><body>");
                    strhtml.Append("<p><b>Dear " + CUSTOMERID + " <br>");
                    strhtml.Append("Your Request for the E-Booking " + EBkgRefnr + " is confirmed. Please refer your Original Booking Int32:" + BkgRefnr + "<br><br>");
                }
                strhtml.Append("This is an Auto Generated Mail. Please do not reply to this Mail-ID.<br>");
                strhtml.Append("</b></p>");
                strhtml.Append("</body></html>");
                objMail.Body = strhtml.ToString();

                System.Web.Mail.SmtpMail.SmtpServer = "smtpout.secureserver.net";
                System.Web.Mail.SmtpMail.Send(objMail);
                objMail = null;
                return "All Data Saved Successfully.";
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;

            }
            catch (Exception ex)
            {
                return "All Data Saved Successfully. Due To Server Problem Mail Has Not Been Sent.";
            }
        }

        public ArrayList SaveJobCard(DataSet M_dataset, long PkValue, WorkFlow objWF, string LocationPK, string PodLocPk, string NewBkgNr, long MainBkgPK, string ShipperPK = "", string ConsignePK = "", string strVoyagePk = "")
        {

            //Dim strValueArgument As String
            //arrMessage.Clear()

            //Try

            //    With objWF.MyCommand
            //        .CommandType = CommandType.StoredProcedure
            //        .CommandText = objWF.MyUserName & ".BOOKING_SEA_PKG.JOB_CARD_SEA_EXP_TBL_INS"
            //        .Parameters.Clear()

            //        .Parameters.Add("BOOKING_SEA_FK_IN", _
            //                                     PkValue).Direction = _
            //                                      ParameterDirection.Input
            //        .Parameters["BOOKING_SEA_FK_IN"].SourceVersion = DataRowVersion.Current
            //        .Parameters.Add("LOCATION_MST_FK_IN", LocationPK).Direction = ParameterDirection.Input
            //        .Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current
            //        .Parameters.Add("POD_FK_IN", getDefault(PodLocPk, DBNull.Value)).Direction = ParameterDirection.Input
            //        .Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current
            //        .Parameters.Add("SHIPPER_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input
            //        .Parameters["SHIPPER_MST_FK_IN"].SourceVersion = DataRowVersion.Current
            //        .Parameters.Add("CONSIGNEE_MST_FK_IN", getDefault(ConsignePK, DBNull.Value)).Direction = ParameterDirection.Input
            //        .Parameters["CONSIGNEE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
            //        .Parameters.Add("VOYAGE_PK_IN", strVoyagePk).Direction = _
            //                                                         ParameterDirection.Input
            //        .Parameters["VOYAGE_PK_IN"].SourceVersion = DataRowVersion.Current
            //        .Parameters.Add("RETURN_VALUE", _
            //            OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = _
            //                                                  ParameterDirection.Output
            //        .ExecuteNonQuery()
            //        strRet = IIf(IsDBNull(.Parameters["RETURN_VALUE"].Value), "", .Parameters["RETURN_VALUE"].Value)
            //    End With


            //    arrMessage.Add("All data saved successfully")
            //    Return arrMessage
            //Catch oraexp As OracleException
            //    Throw oraexp
            //Catch ex As Exception
            //    Throw ex
            //End Try
            string strValueArgument = null;
            int JobFlag = 0;
            arrMessage.Clear();
            JobFlag = CheckJobcard(Convert.ToInt32(PkValue));
            try
            {
                if (JobFlag == 0)
                {
                    var _with30 = objWF.MyCommand;
                    _with30.CommandType = CommandType.StoredProcedure;
                    _with30.CommandText = objWF.MyUserName + ".BOOKING_MST_PKG.AUTO_JOB_CARD_TRN";
                    _with30.Parameters.Clear();
                    _with30.Parameters.Add("BOOKING_MST_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with30.ExecuteNonQuery();
                    strRet = (string.IsNullOrEmpty(_with30.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with30.Parameters["RETURN_VALUE"].Value.ToString());
                    //JCPks = strRet
                    SaveJobCardTrnCost(PkValue, objWF);
                    ///Added By Sushama
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
        public Int32 CheckJobcard(Int32 BookingPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
            int RcdCnt = 0;
            try
            {
                sb.Append("SELECT COUNT(*)");
                sb.Append("      FROM JOB_CARD_TRN JC ");
                sb.Append("     WHERE JC.BOOKING_MST_FK = " + BookingPK);
                return Convert.ToInt32(Objwk.ExecuteScaler(sb.ToString()));
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
        ///Added By Sushama
        public string SaveJobCardTrnCost(long BKGPkValue, WorkFlow objWF)
        {
            string strValueArgument = null;
            int JobFlag = 0;
            DataSet DSBkg = new DataSet();
            long PRCFK = PRECARRIAGE;
            long ONCFK = ONCARRIAGE;
            decimal costAmt = default(decimal);
            string strCostDetails = null;
            long TransporterFK = 0;
            decimal chrgwt = default(decimal);
            long POL_PK = 0;
            long POD_PK = 0;
            long PLR_PK = 0;
            long PFD_PK = 0;
            long CargoType = 0;
            long Location_mst_FK = 0;
            long Base_Currency_FK = 0;
            string strArrRate = null;
            short BizType = 0;
            short ProcessType = 0;
            string[] strArrCostAmt = null;
            DataSet dsCostDetails = new DataSet();
            arrMessage.Clear();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            int JCPKValue = 0;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                sb.Append("SELECT JC.job_card_trn_pk");
                sb.Append("      FROM JOB_CARD_TRN JC ");
                sb.Append("     WHERE JC.BOOKING_MST_FK = " + BKGPkValue);
                objWF.MyCommand.CommandType = CommandType.Text;
                objWF.MyCommand.CommandText = sb.ToString();
                JCPKValue = Convert.ToInt32(objWF.MyCommand.ExecuteScalar());

                sb.Clear();
                sb.Append("SELECT BMT.POO_FK AS PLR_FK,0 PFD_FK,");
                sb.Append("     BMT.PORT_MST_POL_FK AS PORT_MST_POL_FK,");
                sb.Append("     0 AS PORT_MST_POD_FK,");
                sb.Append("     CASE WHEN BMT.CARGO_TYPE = 1 THEN BMT.GROSS_WEIGHT ");
                sb.Append("     ELSE BMT.CHARGEABLE_WEIGHT END CHARGEABLE_WEIGHT,");
                sb.Append("     BMT.TRANSPORTER_PLR_FK AS TRANSPORTER_FK,");
                sb.Append("     BMT.CARGO_TYPE,POL.LOCATION_MST_FK ,CONTT.CURRENCY_MST_FK,BMT.BUSINESS_TYPE,BMT.FROM_FLAG,VMT.VENDOR_ID , 'PRC' PRC_FLAG");
                sb.Append("   FROM BOOKING_MST_TBL BMT, PORT_MST_TBL POL, PORT_MST_TBL POD,");
                sb.Append("   COUNTRY_MST_TBL CONTT, LOCATION_MST_TBL LOC,VENDOR_MST_TBL VMT");
                sb.Append("   WHERE BMT.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                sb.Append("   AND BMT.PORT_MST_POD_FK = POD.PORT_MST_PK AND VMT.VENDOR_MST_PK(+) = BMT.TRANSPORTER_PLR_FK ");
                sb.Append("   AND LOC.COUNTRY_MST_FK = CONTT.COUNTRY_MST_PK ");
                sb.Append("   AND LOC.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                sb.Append("   AND BMT.BOOKING_MST_PK =" + BKGPkValue);
                sb.Append(" UNION SELECT 0 PLR_FK,BMT.PFD_FK AS PFD_FK,");
                sb.Append("     0 AS PORT_MST_POL_FK,");
                sb.Append("     BMT.PORT_MST_POD_FK AS PORT_MST_POD_FK,");
                sb.Append("     CASE WHEN BMT.CARGO_TYPE = 1 THEN BMT.GROSS_WEIGHT ");
                sb.Append("     ELSE BMT.CHARGEABLE_WEIGHT END CHARGEABLE_WEIGHT,");
                sb.Append("     BMT.TRANSPORTER_PFD_FK AS TRANSPORTER_FK,");
                sb.Append("     BMT.CARGO_TYPE,POL.LOCATION_MST_FK ,CONTT.CURRENCY_MST_FK,BMT.BUSINESS_TYPE,BMT.FROM_FLAG,VMT.VENDOR_ID , 'ONC' PRC_FLAG");
                sb.Append("   FROM BOOKING_MST_TBL BMT, PORT_MST_TBL POL, PORT_MST_TBL POD,");
                sb.Append("   COUNTRY_MST_TBL CONTT, LOCATION_MST_TBL LOC,VENDOR_MST_TBL VMT");
                sb.Append("   WHERE BMT.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                sb.Append("   AND BMT.PORT_MST_POD_FK = POD.PORT_MST_PK  AND VMT.VENDOR_MST_PK(+) = BMT.TRANSPORTER_PFD_FK ");
                sb.Append("   AND LOC.COUNTRY_MST_FK = CONTT.COUNTRY_MST_PK ");
                sb.Append("   AND LOC.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                sb.Append("   AND BMT.BOOKING_MST_PK =" + BKGPkValue);
                objWF.MyCommand.CommandType = CommandType.Text;
                objWF.MyCommand.Parameters.Clear();
                objWF.MyCommand.CommandText = sb.ToString();
                objWF.MyDataAdapter = new OracleDataAdapter(objWF.MyCommand);
                objWF.MyDataAdapter.Fill(DSBkg);

                if (DSBkg.Tables[0].Rows.Count > 0)
                {
                    for (int j = 0; j <= DSBkg.Tables[0].Rows.Count - 1; j++)
                    {
                        TransporterFK = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["TRANSPORTER_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["TRANSPORTER_FK"].ToString());
                        chrgwt = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["CHARGEABLE_WEIGHT"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["CHARGEABLE_WEIGHT"].ToString());
                        POL_PK = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["PORT_MST_POL_FK"].ToString()) ? "0": DSBkg.Tables[0].Rows[j]["PORT_MST_POL_FK"]);
                        POD_PK = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["PORT_MST_POD_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["PORT_MST_POD_FK"]);
                        PLR_PK = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["PLR_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["PLR_FK"]);
                        PFD_PK = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["PFD_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["PFD_FK"]);
                        CargoType = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["CARGO_TYPE"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["CARGO_TYPE"]);
                        Location_mst_FK = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["LOCATION_MST_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["LOCATION_MST_FK"]);
                        Base_Currency_FK = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["CURRENCY_MST_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["CURRENCY_MST_FK"]);
                        BizType = Convert.ToInt16(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["BUSINESS_TYPE"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["BUSINESS_TYPE"]);
                        ProcessType = Convert.ToInt16(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["FROM_FLAG"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["FROM_FLAG"]);

                        dsCostDetails = GetPRCONCCostDetails(objWF, TransporterFK, Convert.ToInt32(chrgwt), POL_PK, POD_PK, PLR_PK, PFD_PK, JCPKValue, CargoType, BizType,
                        ProcessType);

                        if (dsCostDetails.Tables[0].Rows.Count > 0)
                        {
                            for (int i = 0; i <= dsCostDetails.Tables[0].Rows.Count - 1; i++)
                            {
                                if ((Convert.ToInt32(dsCostDetails.Tables[0].Rows[i]["cost_element_mst_pk"]) == PRCFK & DSBkg.Tables[0].Rows[j]["PRC_FLAG"] == "PRC") | (Convert.ToInt32(dsCostDetails.Tables[0].Rows[i]["cost_element_mst_pk"]) == ONCFK & DSBkg.Tables[0].Rows[j]["PRC_FLAG"] == "ONC"))
                                {
                                    strArrRate = dsCostDetails.Tables[0].Rows[i]["COST_AMOUNT"].ToString();
                                    strArrCostAmt = strArrRate.Split(',');
                                    if (strArrCostAmt.Length > 3)
                                    {
                                        if (Convert.ToInt64(strArrCostAmt[0]) == 0 | Convert.ToInt64(strArrCostAmt[1]) == 0)
                                        {
                                        }
                                        else
                                        {
                                            if (Convert.ToInt64(DSBkg.Tables[0].Rows[0]["CARGO_TYPE"]) == 1)
                                            {
                                                costAmt = Convert.ToDecimal(Math.Round(Convert.ToDouble(strArrCostAmt[1]) * 100) / 100);
                                            }
                                            else
                                            {
                                                costAmt = Convert.ToDecimal(Math.Round(Convert.ToDouble(Convert.ToInt64(strArrCostAmt[1]) * Convert.ToInt64(DSBkg.Tables[0].Rows[i]["CHARGEABLE_WEIGHT"]) * 100) / 100));
                                            }
                                            objWF.OpenConnection();
                                            var _with31 = objWF.MyCommand;
                                            _with31.CommandType = CommandType.StoredProcedure;
                                            _with31.CommandText = objWF.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_COST_INS";
                                            _with31.Parameters.Clear();
                                            _with31.Parameters.Add("JOB_CARD_TRN_FK_IN", JCPKValue).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("VENDOR_MST_FK_IN", TransporterFK).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("COST_ELEMENT_FK_IN", dsCostDetails.Tables[0].Rows[i]["cost_element_mst_pk"]).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("LOCATION_FK_IN", Location_mst_FK).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("VENDOR_KEY_IN", DSBkg.Tables[0].Rows[j]["VENDOR_ID"]).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("PTMT_TYPE_IN", 1).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("CURRENCY_MST_FK_IN", Base_Currency_FK).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("ESTIMATED_COST_IN", costAmt).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("TOTAL_COST_IN", Convert.ToInt64(strArrCostAmt[3]) * costAmt).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("SURCHARGE_IN", 0.0).Direction = ParameterDirection.Input;
                                            _with31.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                            _with31.ExecuteNonQuery();
                                            strRet = (string.IsNullOrEmpty(_with31.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with31.Parameters["RETURN_VALUE"].Value.ToString());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage[0].ToString();
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
        public DataSet GetPRCONCCostDetails(WorkFlow objWF, long TransporterFK, long chrgwt, long POL_PK, long POD_PK, long PLR_PK, long PFD_PK, long JobCardPK, long CargoType, short bizType,
        short ProcessType)
        {
            string strValueArgument = null;
            int JobFlag = 0;
            DataSet DSCost = new DataSet();
            long PRCFK = PRECARRIAGE;
            long ONCFK = ONCARRIAGE;
            arrMessage.Clear();
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append(" SELECT DISTINCT c.cost_element_mst_pk, c.cost_element_id  ");
                sb.Append(" ,c.cost_element_name  ");
                sb.Append(" , NVL(FETCH_TRSPT_CONTR_RATE(" + TransporterFK + ", " + chrgwt + ", " + PLR_PK + "," + PFD_PK + ", " + bizType);
                sb.Append(" ," + ProcessType + ", c.cost_element_mst_pk , " + JobCardPK + ", " + CargoType + "),0) AS COST_AMOUNT , ");
                sb.Append("  CASE when FCT.BASIS =1 then (FCT.BASIS_VALUE || '" + " % of " + "' || CONCADINATE_FUN_FREIGHTELEMENT(C.COST_ELEMENT_MST_PK, 1,' " + POL_PK + "','" + POD_PK + "'))  else null end as FRT_ELEMENT ");
                sb.Append(" FROM cost_element_mst_tbl c, FREIGHT_CONFIG_TRN_TBL FCT, SECTOR_MST_TBL SMT ");
                sb.Append(" WHERE ");
                sb.Append(" (c.business_type = " + bizType + " or c.business_type = 3)  ");
                sb.Append(" AND C.COST_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK(+) ");
                sb.Append(" AND SMT.SECTOR_MST_PK(+) = FCT.SECTOR_MST_FK ");
                sb.Append(" AND c.vendor_type_mst_fk in ");
                sb.Append("(select vs.vendor_type_fk ");
                sb.Append(" from vendor_services_trn vs");
                sb.Append(" where vs.vendor_mst_fk = " + TransporterFK);
                sb.Append(" and vs.vendor_type_fk in ");
                sb.Append(" (select v.vendor_type_pk ");
                sb.Append(" from vendor_type_mst_tbl v ");
                sb.Append(" where v.active_flag = 1)) ");
                sb.Append(" order by c.cost_element_id ");
                objWF.MyCommand.CommandType = CommandType.Text;
                objWF.MyCommand.Parameters.Clear();
                objWF.MyCommand.CommandText = sb.ToString();
                objWF.MyDataAdapter = new OracleDataAdapter(objWF.MyCommand);
                objWF.MyDataAdapter.Fill(DSCost);
                return DSCost;
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

        public ArrayList SaveProfitability(DataSet DSProfit, long PkValue, WorkFlow objWF)
        {
            arrMessage.Clear();
            int i = 0;
            if (DSProfit.Tables[0].Rows.Count > 0)
            {
                for (i = 0; i <= DSProfit.Tables[0].Rows.Count - 1; i++)
                {
                    var _with32 = objWF.MyCommand;
                    _with32.Parameters.Clear();
                    _with32.Connection = objWF.MyConnection;
                    _with32.CommandType = CommandType.StoredProcedure;
                    _with32.CommandText = objWF.MyUserName + ".QUOTATION_PROFITABILITY_PKG.QUOTATION_PROFITABILITYRPT_INS";
                    var _with33 = _with32.Parameters;
                    _with33.Add("QUOTATION_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with33.Add("COST_ELEMENT_MST_FK_IN", (string.IsNullOrEmpty(DSProfit.Tables[0].Rows[i]["COST_ELEMENT_MST_PK"].ToString()) ? DBNull.Value : DSProfit.Tables[0].Rows[i]["COST_ELEMENT_MST_PK"])).Direction = ParameterDirection.Input;
                    _with33.Add("CURRENCY_TYPE_MST_FK_IN", (string.IsNullOrEmpty(DSProfit.Tables[0].Rows[i]["CURRENCY_TYPE_MST_FK"].ToString()) ? DBNull.Value : DSProfit.Tables[0].Rows[i]["CURRENCY_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
                    if (Convert.ToInt32(DSProfit.Tables[0].Rows[i]["CARGO_TYPE"]) == 1)
                    {
                        _with33.Add("CONTAINER_TYPE_FK_IN", DSProfit.Tables[0].Rows[i]["CONTAINER_TYPE_FK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with33.Add("CONTAINER_TYPE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    _with33.Add("PROFITABILITY_RATE_IN", (string.IsNullOrEmpty(DSProfit.Tables[0].Rows[i]["PROFITABILITY_RATE"].ToString()) ? DBNull.Value : DSProfit.Tables[0].Rows[i]["PROFITABILITY_RATE"])).Direction = ParameterDirection.Input;
                    _with33.Add("ROE_IN", (string.IsNullOrEmpty(DSProfit.Tables[0].Rows[i]["ROE"].ToString()) ? DBNull.Value : DSProfit.Tables[0].Rows[i]["ROE"])).Direction = ParameterDirection.Input;
                    _with33.Add("BIZ_TYPE_IN", DSProfit.Tables[0].Rows[i]["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                    _with33.Add("CARGO_TYPE_IN", DSProfit.Tables[0].Rows[i]["CARGO_TYPE"]).Direction = ParameterDirection.Input;
                    _with33.Add("REF_FLAG_IN", DSProfit.Tables[0].Rows[i]["REF_FLAG"]).Direction = ParameterDirection.Input;
                    _with33.Add("VOLUME_IN", (string.IsNullOrEmpty(DSProfit.Tables[0].Rows[i]["VOLUME"].ToString()) ? 1 : DSProfit.Tables[0].Rows[i]["VOLUME"])).Direction = ParameterDirection.Input;
                    _with33.Add("INC_TEA_STATUS_IN", (string.IsNullOrEmpty(DSProfit.Tables[0].Rows[i]["INC_TEA_STATUS"].ToString()) ? DBNull.Value : DSProfit.Tables[0].Rows[i]["INC_TEA_STATUS"])).Direction = ParameterDirection.Input;
                    _with33.Add("POL_FK_IN", (string.IsNullOrEmpty(DSProfit.Tables[0].Rows[i]["POL_FK"].ToString()) ? DBNull.Value : DSProfit.Tables[0].Rows[i]["POL_FK"])).Direction = ParameterDirection.Input;
                    _with33.Add("POD_FK_IN", (string.IsNullOrEmpty(DSProfit.Tables[0].Rows[i]["POD_FK"].ToString()) ? DBNull.Value : DSProfit.Tables[0].Rows[i]["POD_FK"])).Direction = ParameterDirection.Input;
                    _with33.Add("CARRIER_MST_FK_IN", (string.IsNullOrEmpty(DSProfit.Tables[0].Rows[i]["CARRIER_MST_FK"].ToString()) ? DBNull.Value : DSProfit.Tables[0].Rows[i]["CARRIER_MST_FK"])).Direction = ParameterDirection.Input;
                    _with33.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                    _with33.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with33.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with32.ExecuteNonQuery();
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            return new ArrayList();
        }

        public ArrayList SaveBookingOFreight(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string strValueArgument = null;
            arrMessage.Clear();
            try
            {

                if (!IsUpdate)
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with34 = SelectCommand;
                        _with34.CommandType = CommandType.StoredProcedure;
                        _with34.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_TRN_SEA_OTH_CHRG_INS";
                        SelectCommand.Parameters.Clear();


                        _with34.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with34.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with34.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with34.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with34.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with34.Parameters.Add("AMOUNT_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with34.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                        _with34.Parameters.Add("FREIGHT_TYPE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                        _with34.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                        _with34.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with34.ExecuteNonQuery();

                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;


                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with35 = SelectCommand;
                        _with35.CommandType = CommandType.StoredProcedure;
                        _with35.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_TRN_SEA_OTH_CHRG_UPD";
                        SelectCommand.Parameters.Clear();


                        _with35.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with35.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with35.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with35.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with35.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with35.Parameters.Add("AMOUNT_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with35.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                        _with35.Parameters.Add("FREIGHT_TYPE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                        _with35.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                        _with35.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with35.ExecuteNonQuery();

                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }

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

        public ArrayList SaveBookingCDimension(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string Measure, string Wt, string Divfac)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            try
            {

                if (!IsUpdate)
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblCDimension"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with36 = SelectCommand;
                        _with36.CommandType = CommandType.StoredProcedure;
                        _with36.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_CARGO_CALC_INS";
                        SelectCommand.Parameters.Clear();


                        _with36.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with36.Parameters.Add("CARGO_NOP_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_NOP"]).Direction = ParameterDirection.Input;
                        _with36.Parameters["CARGO_NOP_IN"].SourceVersion = DataRowVersion.Current;

                        _with36.Parameters.Add("CARGO_LENGTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_LENGTH"]).Direction = ParameterDirection.Input;
                        _with36.Parameters["CARGO_LENGTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with36.Parameters.Add("CARGO_WIDTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_WIDTH"]).Direction = ParameterDirection.Input;
                        _with36.Parameters["CARGO_WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with36.Parameters.Add("CARGO_HEIGHT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_HEIGHT"]).Direction = ParameterDirection.Input;
                        _with36.Parameters["CARGO_HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with36.Parameters.Add("CARGO_CUBE_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_CUBE"]).Direction = ParameterDirection.Input;
                        _with36.Parameters["CARGO_CUBE_IN"].SourceVersion = DataRowVersion.Current;

                        _with36.Parameters.Add("CARGO_VOLUME_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_VOLUME_WT"]).Direction = ParameterDirection.Input;
                        _with36.Parameters["CARGO_VOLUME_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with36.Parameters.Add("CARGO_ACTUAL_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_ACTUAL_WT"]).Direction = ParameterDirection.Input;
                        _with36.Parameters["CARGO_ACTUAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with36.Parameters.Add("CARGO_DENSITY_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_DENSITY"]).Direction = ParameterDirection.Input;
                        _with36.Parameters["CARGO_DENSITY_IN"].SourceVersion = DataRowVersion.Current;


                        _with36.Parameters.Add("CARGO_MEASURE_IN", Measure).Direction = ParameterDirection.Input;
                        _with36.Parameters.Add("CARGO_WT_IN", Wt).Direction = ParameterDirection.Input;
                        _with36.Parameters.Add("CARGO_DIVFAC_IN", Divfac).Direction = ParameterDirection.Input;

                        _with36.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with36.ExecuteNonQuery();

                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;


                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblCDimension"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with37 = SelectCommand;
                        _with37.CommandType = CommandType.StoredProcedure;
                        _with37.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_CARGO_CALC_UPD";
                        SelectCommand.Parameters.Clear();


                        _with37.Parameters.Add("BOOKING_SEA_CARGO_CALC_PK_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["BOOKING_SEA_CARGO_CALC_PK"]).Direction = ParameterDirection.Input;
                        _with37.Parameters["BOOKING_SEA_CARGO_CALC_PK_IN"].SourceVersion = DataRowVersion.Current;

                        _with37.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with37.Parameters.Add("CARGO_NOP_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_NOP"]).Direction = ParameterDirection.Input;
                        _with37.Parameters["CARGO_NOP_IN"].SourceVersion = DataRowVersion.Current;

                        _with37.Parameters.Add("CARGO_LENGTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_LENGTH"]).Direction = ParameterDirection.Input;
                        _with37.Parameters["CARGO_LENGTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with37.Parameters.Add("CARGO_WIDTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_WIDTH"]).Direction = ParameterDirection.Input;
                        _with37.Parameters["CARGO_WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with37.Parameters.Add("CARGO_HEIGHT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_HEIGHT"]).Direction = ParameterDirection.Input;
                        _with37.Parameters["CARGO_HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with37.Parameters.Add("CARGO_CUBE_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_CUBE"]).Direction = ParameterDirection.Input;
                        _with37.Parameters["CARGO_CUBE_IN"].SourceVersion = DataRowVersion.Current;

                        _with37.Parameters.Add("CARGO_VOLUME_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_VOLUME_WT"]).Direction = ParameterDirection.Input;
                        _with37.Parameters["CARGO_VOLUME_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with37.Parameters.Add("CARGO_ACTUAL_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_ACTUAL_WT"]).Direction = ParameterDirection.Input;
                        _with37.Parameters["CARGO_ACTUAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with37.Parameters.Add("CARGO_DENSITY_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_DENSITY"]).Direction = ParameterDirection.Input;
                        _with37.Parameters["CARGO_DENSITY_IN"].SourceVersion = DataRowVersion.Current;


                        _with37.Parameters.Add("CARGO_MEASURE_IN", Measure).Direction = ParameterDirection.Input;
                        _with37.Parameters.Add("CARGO_WT_IN", Wt).Direction = ParameterDirection.Input;
                        _with37.Parameters.Add("CARGO_DIVFAC_IN", Divfac).Direction = ParameterDirection.Input;

                        _with37.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with37.ExecuteNonQuery();

                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }

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

        public ArrayList SaveBookingTRN(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string strValueArgument = null;


            arrMessage.Clear();
            try
            {

                if (!IsUpdate)
                {

                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with38 = SelectCommand;
                        _with38.CommandType = CommandType.StoredProcedure;
                        _with38.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_TRN_INS";
                        SelectCommand.Parameters.Clear();


                        _with38.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with38.Parameters.Add("TRANS_REFERED_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]).Direction = ParameterDirection.Input;
                        _with38.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion = DataRowVersion.Current;

                        _with38.Parameters.Add("TRANS_REF_NO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]).Direction = ParameterDirection.Input;
                        _with38.Parameters["TRANS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                        _with38.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                        _with38.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with38.Parameters.Add("NO_OF_BOXES_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["NO_OF_BOXES"]).Direction = ParameterDirection.Input;
                        _with38.Parameters["NO_OF_BOXES_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
                        {
                            _with38.Parameters.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with38.Parameters.Add("BASIS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]).Direction = ParameterDirection.Input;
                        }
                        _with38.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"].ToString()))
                        {
                            _with38.Parameters.Add("QUANTITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with38.Parameters.Add("QUANTITY_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"])).Direction = ParameterDirection.Input;
                        }
                        _with38.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                        _with38.Parameters.Add("COMMODITY_GROUP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;
                        _with38.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with38.Parameters.Add("COMMODITY_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with38.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"].ToString()))
                        {
                            _with38.Parameters.Add("ALL_IN_TARIFF_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with38.Parameters.Add("ALL_IN_TARIFF_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"])).Direction = ParameterDirection.Input;
                        }
                        _with38.Parameters["ALL_IN_TARIFF_IN"].SourceVersion = DataRowVersion.Current;
                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"].ToString()))
                        {
                            _with38.Parameters.Add("BUYING_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with38.Parameters.Add("BUYING_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"])).Direction = ParameterDirection.Input;
                        }
                        _with38.Parameters["BUYING_RATE_IN"].SourceVersion = DataRowVersion.Current;

                        _with38.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with38.ExecuteNonQuery();
                        if (string.Compare(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value), "bookingtrans")>0)
                        {
                            arrMessage.Add(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value));
                            return arrMessage;
                        }
                        else
                        {
                            _PkValueTrans = Convert.ToInt64(SelectCommand.Parameters["RETURN_VALUE"].Value);
                        }
                        if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == 1)
                        {
                            strValueArgument = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]);
                        }
                        else
                        {
                            strValueArgument = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]);
                        }

                        if (dsMain.Tables["tblCargo"].Rows.Count > 0)
                        {
                            arrMessage = SaveBookingCargo(dsMain, _PkValueTrans, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), strValueArgument, Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]));

                        }

                        arrMessage = SaveBookingFRT(dsMain, _PkValueTrans, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), strValueArgument, Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]));

                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }

                        if (arrMessage.Count > 0)
                        {
                            if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") == 0)
                            {
                                return arrMessage;
                            }
                        }
                        if (Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 1 | Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 2)
                        {
                            arrMessage = (ArrayList)UpdateUpStream(dsMain, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]), Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), 0);

                            if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                            {
                                arrMessage.Add("Upstream Updation failed, Please check for valid Data");
                                return arrMessage;
                            }
                        }
                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;


                }
                else
                {

                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with39 = SelectCommand;
                        _with39.CommandType = CommandType.StoredProcedure;
                        _with39.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_TRN_UPD";

                        SelectCommand.Parameters.Clear();


                        _with39.Parameters.Add("BOOKING_TRN_SEA_PK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BOOKING_TRN_SEA_PK"]).Direction = ParameterDirection.Input;
                        _with39.Parameters["BOOKING_TRN_SEA_PK_IN"].SourceVersion = DataRowVersion.Current;


                        _with39.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with39.Parameters.Add("TRANS_REFERED_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]).Direction = ParameterDirection.Input;
                        _with39.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion = DataRowVersion.Current;

                        _with39.Parameters.Add("TRANS_REF_NO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]).Direction = ParameterDirection.Input;
                        _with39.Parameters["TRANS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                        _with39.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                        _with39.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with39.Parameters.Add("NO_OF_BOXES_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["NO_OF_BOXES"]).Direction = ParameterDirection.Input;
                        _with39.Parameters["NO_OF_BOXES_IN"].SourceVersion = DataRowVersion.Current;

                        if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
                        {
                            _with39.Parameters.Add("BASIS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with39.Parameters.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        _with39.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"].ToString()))
                        {
                            _with39.Parameters.Add("QUANTITY_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"])).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with39.Parameters.Add("QUANTITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        _with39.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                        _with39.Parameters.Add("COMMODITY_GROUP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;
                        _with39.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with39.Parameters.Add("COMMODITY_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with39.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"].ToString()))
                        {
                            _with39.Parameters.Add("ALL_IN_TARIFF_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with39.Parameters.Add("ALL_IN_TARIFF_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"])).Direction = ParameterDirection.Input;
                        }
                        _with39.Parameters["ALL_IN_TARIFF_IN"].SourceVersion = DataRowVersion.Current;
                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"].ToString()))
                        {
                            _with39.Parameters.Add("BUYING_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with39.Parameters.Add("BUYING_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"])).Direction = ParameterDirection.Input;
                        }
                        _with39.Parameters["BUYING_RATE_IN"].SourceVersion = DataRowVersion.Current;

                        _with39.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with39.ExecuteNonQuery();
                        if (string.Compare(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value), "bookingtrans")>0)
                        {
                            arrMessage.Add(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value));
                            return arrMessage;
                        }
                        else
                        {
                            _PkValueTrans = Convert.ToInt64(SelectCommand.Parameters["RETURN_VALUE"].Value);
                        }
                        if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == 1)
                        {
                            strValueArgument = Convert.ToString(getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"], ""));
                        }
                        else
                        {
                            strValueArgument = Convert.ToString(getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"], ""));
                        }
                        arrMessage = SaveBookingFRT(dsMain, _PkValueTrans, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), strValueArgument, Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]));
                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }

                        if (arrMessage.Count > 0)
                        {
                            if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") == 0)
                            {
                                return arrMessage;
                            }
                        }

                        if (Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 1 | Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 2)
                        {
                            arrMessage = (ArrayList)UpdateUpStream(dsMain, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]), Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), 0);

                            if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                            {
                                arrMessage.Add("Upstream Updation failed, Please check for valid Data");
                                return arrMessage;
                            }
                        }
                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }

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

        public ArrayList SaveBookingFRT(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string strContractRefNo, string strValueArgument, string isLcl)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            DataView dv_Freight = new DataView();

            Int16 Check = default(Int16);
            dv_Freight = getDataView(dsMain.Tables["tblFreight"], strContractRefNo, strValueArgument, isLcl);
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                Check = FetchEBFrt(PkValue);
                if (Chk_EBK == 1 & Check == 0)
                {
                    IsUpdate = false;
                }
            }
            arrMessage.Clear();
            try
            {
                if (!IsUpdate)
                {
                    var _with40 = SelectCommand;
                    _with40.CommandType = CommandType.StoredProcedure;
                    _with40.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_TRN_FRT_INS";

                    for (nRowCnt = 0; nRowCnt <= dv_Freight.Table.Rows.Count - 1; nRowCnt++)
                    {
                        SelectCommand.Parameters.Clear();


                        _with40.Parameters.Add("BOOKING_SEA_TRN_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with40.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with40.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with40.Parameters.Add("CURRENCY_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with40.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with40.Parameters.Add("CHARGE_BASIS_IN", dv_Freight.Table.Rows[nRowCnt]["CHARGE_BASIS"]).Direction = ParameterDirection.Input;
                        _with40.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"].ToString()))
                        {
                            _with40.Parameters.Add("SURCHARGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with40.Parameters.Add("SURCHARGE_IN", (dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
                        }

                        if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"].ToString()))
                        {
                            _with40.Parameters.Add("MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with40.Parameters.Add("MIN_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"])).Direction = ParameterDirection.Input;
                        }
                        _with40.Parameters["MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"].ToString()))
                        {
                            _with40.Parameters.Add("TARIFF_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with40.Parameters.Add("TARIFF_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"])).Direction = ParameterDirection.Input;
                        }
                        _with40.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;

                        _with40.Parameters.Add("PYMT_TYPE_IN", dv_Freight.Table.Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                        _with40.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                        _with40.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", dv_Freight.Table.Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"]).Direction = ParameterDirection.Input;
                        _with40.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;


                        _with40.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with40.ExecuteNonQuery();
                    }
                }
                else
                {
                    var _with41 = SelectCommand;
                    _with41.CommandType = CommandType.StoredProcedure;
                    _with41.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_TRN_FRT_UPD";
                    for (nRowCnt = 0; nRowCnt <= dv_Freight.Table.Rows.Count - 1; nRowCnt++)
                    {
                        SelectCommand.Parameters.Clear();

                        if (!string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"].ToString()))
                        {
                            _with41.Parameters.Add("BOOKING_TRN_SEA_FRT_PK_IN", dv_Freight.Table.Rows[nRowCnt]["BOOKING_TRN_SEA_FRT_PK"]).Direction = ParameterDirection.Input;
                            _with41.Parameters["BOOKING_TRN_SEA_FRT_PK_IN"].SourceVersion = DataRowVersion.Current;


                            _with41.Parameters.Add("BOOKING_SEA_TRN_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                            _with41.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                            _with41.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                            _with41.Parameters.Add("CURRENCY_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;

                            if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"].ToString()))
                            {
                                _with41.Parameters.Add("SURCHARGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with41.Parameters.Add("SURCHARGE_IN", (dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
                            }

                            _with41.Parameters.Add("CHARGE_BASIS_IN", dv_Freight.Table.Rows[nRowCnt]["CHARGE_BASIS"]).Direction = ParameterDirection.Input;
                            _with41.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                            _with41.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                            if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"].ToString()))
                            {
                                _with41.Parameters.Add("MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with41.Parameters.Add("MIN_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"])).Direction = ParameterDirection.Input;
                            }
                            _with41.Parameters["MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;

                            if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"].ToString()))
                            {
                                _with41.Parameters.Add("TARIFF_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with41.Parameters.Add("TARIFF_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"])).Direction = ParameterDirection.Input;
                            }
                            _with41.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;

                            _with41.Parameters.Add("PYMT_TYPE_IN", dv_Freight.Table.Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                            _with41.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                            _with41.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", dv_Freight.Table.Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"]).Direction = ParameterDirection.Input;
                            _with41.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;


                            _with41.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                            _with41.ExecuteNonQuery();
                        }
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
        #region "Save Cargo Details"
        public ArrayList SaveBookingCargo(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string strContractRefNo, string strValueArgument, string isLcl)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            DataView dv_Cargo = new DataView();
            Int16 CargoPK = default(Int16);
            Int16 TrnPK = default(Int16);
            dv_Cargo = getCargoDataView(dsMain.Tables["tblCargo"], strContractRefNo, strValueArgument, isLcl);
            arrMessage.Clear();
            try
            {
                for (nRowCnt = 0; nRowCnt <= dv_Cargo.Table.Rows.Count - 1; nRowCnt++)
                {
                    SelectCommand.Parameters.Clear();
                    var _with42 = SelectCommand;
                    _with42.CommandType = CommandType.StoredProcedure;
                    _with42.CommandText = objWK.MyUserName + ".BOOKING_TRN_CARGO_DTL_PKG.BOOKING_TRN_CARGO_DTL_INS";


                    _with42.Parameters.Add("BOOKING_TRN_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                    _with42.Parameters.Add("PACK_COUNT_IN", dv_Cargo.Table.Rows[nRowCnt]["PACK_COUNT"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("GROSS_WEIGHT_IN", dv_Cargo.Table.Rows[nRowCnt]["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("NET_WEIGHT_IN", dv_Cargo.Table.Rows[nRowCnt]["NET_WEIGHT"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;


                    _with42.Parameters.Add("VOLUME_IN_CBM_IN", dv_Cargo.Table.Rows[nRowCnt]["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("REMARK_IN", dv_Cargo.Table.Rows[nRowCnt]["REMARK"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["REMARK_IN"].SourceVersion = DataRowVersion.Current;


                    _with42.Parameters.Add("CONTAINER_NUMBER_IN", dv_Cargo.Table.Rows[nRowCnt]["CONTAINER_NUMBER"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("CONTAINER_TYPE_FK_IN", dv_Cargo.Table.Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["CONTAINER_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("SEAL_NUMBER_IN", dv_Cargo.Table.Rows[nRowCnt]["SEAL_NUMBER"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("COMMODITY_FKS_IN", dv_Cargo.Table.Rows[nRowCnt]["COMMODITY_FKS"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["COMMODITY_FKS_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    _with42.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;


                    _with42.Parameters.Add("CONFIG_MST_FK_IN", 4295).Direction = ParameterDirection.Input;
                    _with42.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with42.ExecuteNonQuery();

                    CargoPK = Convert.ToInt16(SelectCommand.Parameters["RETURN_VALUE"].Value);
                    if (CommCnt == 0)
                    {
                        TrnPK = Convert.ToInt16(dsMain.Tables["tblCargo"].Rows[nRowCnt]["JOB_TRN_SEA_EXP_CONT_PK"]);
                    }
                    else
                    {
                        if ((CommCnt + nRowCnt) > dsMain.Tables["tblCargo"].Rows.Count - 1)
                        {
                            break; // TODO: might not be correct. Was : Exit For
                        }
                        else
                        {
                            TrnPK = Convert.ToInt16(dsMain.Tables["tblCargo"].Rows[CommCnt + nRowCnt]["JOB_TRN_SEA_EXP_CONT_PK"]);
                        }
                    }

                    arrMessage = SaveBookingCommodity(dsMain, CargoPK, SelectCommand, Convert.ToString(TrnPK));
                }
                CommCnt = CommCnt + nRowCnt;
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
        public ArrayList SaveBookingCommodity(DataSet dsMain, long PkValue, OracleCommand SelectCommand, string TrnPK)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            DataView dv_Commodity = new DataView();
            dv_Commodity = getCommodityDataView(dsMain.Tables["tblCommodity"], TrnPK);
            arrMessage.Clear();
            try
            {

                for (nRowCnt = 0; nRowCnt <= dv_Commodity.Table.Rows.Count - 1; nRowCnt++)
                {
                    SelectCommand.Parameters.Clear();
                    var _with43 = SelectCommand;
                    _with43.CommandType = CommandType.StoredProcedure;
                    _with43.CommandText = objWK.MyUserName + ".BOOKING_COMMODITY_DTL_PKG.BOOKING_COMMODITY_DTL_INS";



                    _with43.Parameters.Add("BOOKING_CARGO_DTL_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                    _with43.Parameters.Add("COMMODITY_MST_FK_IN", dv_Commodity.Table.Rows[nRowCnt]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("PACK_TYPE_FK_IN", dv_Commodity.Table.Rows[nRowCnt]["PACK_TYPE_FK"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("PACK_COUNT_IN", dv_Commodity.Table.Rows[nRowCnt]["PACK_COUNT"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("GROSS_WEIGHT_IN", dv_Commodity.Table.Rows[nRowCnt]["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("NET_WEIGHT_IN", dv_Commodity.Table.Rows[nRowCnt]["NET_WEIGHT"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;


                    _with43.Parameters.Add("VOLUME_IN_CBM_IN", dv_Commodity.Table.Rows[nRowCnt]["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("LENGTH_IN", dv_Commodity.Table.Rows[nRowCnt]["LENGTH"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["LENGTH_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("WIDTH_IN", dv_Commodity.Table.Rows[nRowCnt]["WIDTH"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("HEIGHT_IN", dv_Commodity.Table.Rows[nRowCnt]["HEIGHT"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("UOM_IN", dv_Commodity.Table.Rows[nRowCnt]["UOM"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["UOM_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("WEIGHT_PER_PKG_IN", dv_Commodity.Table.Rows[nRowCnt]["WEIGHT_PER_PKG"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["WEIGHT_PER_PKG_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("CALCULATED_WEIGHT_IN", dv_Commodity.Table.Rows[nRowCnt]["CALCULATED_WEIGHT"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["CALCULATED_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    _with43.Parameters.Add("CHARGEABLE_WEIGHT_IN", dv_Commodity.Table.Rows[nRowCnt]["CHARGEABLE_WEIGHT"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;



                    _with43.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    _with43.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;


                    _with43.Parameters.Add("FROM_FLAG_IN", "BOOKING").Direction = ParameterDirection.Input;
                    _with43.Parameters["FROM_FLAG_IN"].SourceVersion = DataRowVersion.Current;


                    _with43.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with43.ExecuteNonQuery();
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
        private DataView getCargoDataView(DataTable dtCargo, string strContractRefNo, string strValueArgument, string isLcl)
        {
            try
            {
                DataTable dstemp = new DataTable();
                DataRow dr = null;
                Int32 nRowCnt = default(Int32);
                Int32 nColCnt = default(Int32);
                ArrayList arrValueCondition = new ArrayList();
                string strValueCondition = "";
                dstemp = dtCargo.Clone();
                for (nRowCnt = 0; nRowCnt <= dtCargo.Rows.Count - 1; nRowCnt++)
                {
                    if (strValueArgument == getDefault(dtCargo.Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"], ""))
                    {
                        dr = dstemp.NewRow();
                        for (nColCnt = 0; nColCnt <= dtCargo.Columns.Count - 1; nColCnt++)
                        {
                            dr[nColCnt] = dtCargo.Rows[nRowCnt][nColCnt];
                        }
                        dstemp.Rows.Add(dr);
                    }
                }
                return dstemp.DefaultView;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private DataView getCommodityDataView(DataTable dtCommodity, string TrnPK)
        {
            try
            {
                DataTable dstemp = new DataTable();
                DataRow dr = null;
                Int32 nRowCnt = default(Int32);
                Int32 nColCnt = default(Int32);
                ArrayList arrValueCondition = new ArrayList();
                string strValueCondition = "";
                dstemp = dtCommodity.Clone();
                for (nRowCnt = 0; nRowCnt <= dtCommodity.Rows.Count - 1; nRowCnt++)
                {
                    if (TrnPK == getDefault(dtCommodity.Rows[nRowCnt]["JOB_TRN_SEA_EXP_CONT_PK"], ""))
                    {
                        dr = dstemp.NewRow();
                        for (nColCnt = 0; nColCnt <= dtCommodity.Columns.Count - 1; nColCnt++)
                        {
                            dr[nColCnt] = dtCommodity.Rows[nRowCnt][nColCnt];
                        }
                        dstemp.Rows.Add(dr);
                    }
                }
                return dstemp.DefaultView;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion



        public Int16 FetchEBFrt(long BkgPk)
        {
            string sql = "";
            string res = "";
            Int16 check = 0;
            WorkFlow objWK = new WorkFlow();
            sql = "select BOOKING_TRN_FK from BOOKING_TRN_FRT_DTLS where BOOKING_TRN_FK='" + BkgPk + "'";
            res = objWK.ExecuteScaler(sql);
            if (Convert.ToInt32(res )> 0)
            {
                check = 1;
            }
            else
            {
                check = 0;
            }
            return check;
        }
        public object UpdateUpStream(DataSet dsMain, OracleCommand SelectCommand, bool IsUpdate, string strTranType, string strContractRefNo, long PkValue)
        {

            WorkFlow objWK = new WorkFlow();
            string strValueArgument = null;
            arrMessage.Clear();
            try
            {
                var _with44 = SelectCommand;
                _with44.CommandType = CommandType.StoredProcedure;
                _with44.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_UPDATE_UPSTREAM";
                SelectCommand.Parameters.Clear();

                _with44.Parameters.Add("TRANS_REFERED_FROM_IN", Convert.ToInt64(strTranType)).Direction = ParameterDirection.Input;
                _with44.Parameters.Add("TRANS_REF_NO_IN", Convert.ToString(strContractRefNo)).Direction = ParameterDirection.Input;

                _with44.Parameters.Add("ISUPDATE", IsUpdate.ToString()).Direction = ParameterDirection.Input;

                _with44.Parameters.Add("BOOKING_STATUS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;


                _with44.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                _with44.ExecuteNonQuery();
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

        private DataView getDataView(DataTable dtFreight, string strContractRefNo, string strValueArgument, string isLcl)
        {
            try
            {
                DataTable dstemp = new DataTable();
                DataRow dr = null;
                Int32 nRowCnt = default(Int32);
                Int32 nColCnt = default(Int32);
                ArrayList arrValueCondition = new ArrayList();
                string strValueCondition = "";
                dstemp = dtFreight.Clone();
                if (isLcl == "1")
                {
                    for (nRowCnt = 0; nRowCnt <= dtFreight.Rows.Count - 1; nRowCnt++)
                    {
                        if (strContractRefNo == getDefault(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"], "") & strValueArgument == getDefault(dtFreight.Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"], ""))
                        {
                            dr = dstemp.NewRow();
                            for (nColCnt = 0; nColCnt <= dtFreight.Columns.Count - 1; nColCnt++)
                            {
                                dr[nColCnt] = dtFreight.Rows[nRowCnt][nColCnt];
                            }
                            dstemp.Rows.Add(dr);
                        }
                    }
                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dtFreight.Rows.Count - 1; nRowCnt++)
                    {
                        if (strContractRefNo == getDefault(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"], "") & strValueArgument == getDefault(dtFreight.Rows[nRowCnt]["BASISPK"], ""))
                        {
                            dr = dstemp.NewRow();
                            for (nColCnt = 0; nColCnt <= dtFreight.Columns.Count - 1; nColCnt++)
                            {
                                dr[nColCnt] = dtFreight.Rows[nRowCnt][nColCnt];
                            }
                            dstemp.Rows.Add(dr);
                        }
                    }
                }
                return dstemp.DefaultView;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public bool CheckActiveJobCard(int strABEPk)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            short intCnt = 0;
            WorkFlow objWF = new WorkFlow();
            string strReturn = null;

            strBuilder.Append(" UPDATE JOB_CARD_TRN J ");
            strBuilder.Append(" SET J.JOB_CARD_STATUS = 2, J.JOB_CARD_CLOSED_ON = SYSDATE ");
            strBuilder.Append(" WHERE J.BOOKING_MST_FK = " + strABEPk);

            try
            {
                intCnt = Convert.ToInt16(objWF.ExecuteScaler(strBuilder.ToString()));
                if (intCnt == 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
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


        public string GenerateBookingNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey("BOOKING (SEA)", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, objWK);
            return functionReturnValue;
        }


        #endregion

        #region "Vessel/voyage saving"
        public ArrayList SaveVesselMaster(long dblVesselPK, string strVesselName, long dblOperatorFK, string strVesselID, string VoyNo, OracleCommand SelectCommand, long POLPK, string PODPK, System.DateTime POLETA, System.DateTime POLETD,
        System.DateTime POLCUT, System.DateTime PODETA, System.DateTime ATDPOL , System.DateTime ATAPOD)
        {


            WorkFlow objWK = new WorkFlow();

            int RESULT = 0;
            try
            {
                if (dblVesselPK == 0)
                {
                    OracleCommand InsCommand = new OracleCommand();
                    Int16 VER = default(Int16);
                    var _with45 = SelectCommand;
                    _with45.Parameters.Clear();
                    _with45.CommandType = CommandType.StoredProcedure;
                    _with45.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TBL_INS";
                    _with45.Parameters.Add("OPERATOR_MST_FK_IN", dblOperatorFK).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("VESSEL_NAME_IN", strVesselName).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("VESSEL_ID_IN", strVesselID).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("ACTIVE_IN", 1).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    RESULT = _with45.ExecuteNonQuery();
                    dblVesselPK = Convert.ToInt32(_with45.Parameters["RETURN_VALUE"].Value);
                    _with45.Parameters.Clear();

                }

                var _with46 = SelectCommand;
                _with46.Parameters.Clear();
                _with46.CommandType = CommandType.StoredProcedure;
                _with46.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_INS";
                _with46.Parameters.Add("VESSEL_VOYAGE_TBL_FK_IN", dblVesselPK).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("VOYAGE_IN", VoyNo).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("PORT_MST_POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("PORT_MST_POD_FK_IN", getDefault(PODPK, DBNull.Value)).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("POL_ETA_IN", getDefault((POLETA == DateTime.MinValue ? DateTime.MinValue : POLETA), DBNull.Value)).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("POL_ETD_IN", getDefault((POLETD == DateTime.MinValue ? DateTime.MinValue : POLETD), DBNull.Value)).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("POL_CUT_OFF_DATE_IN", getDefault((POLCUT == DateTime.MinValue ? DateTime.MinValue : POLCUT), DBNull.Value)).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("POD_ETA_IN", getDefault((PODETA == DateTime.MinValue ? DateTime.MinValue : PODETA), DBNull.Value)).Direction = ParameterDirection.Input;

                _with46.Parameters.Add("ATD_POL_IN", getDefault((ATDPOL == DateTime.MinValue ? DateTime.MinValue : ATDPOL), DBNull.Value)).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("ATA_POD_IN", getDefault((ATAPOD == DateTime.MinValue ? DateTime.MinValue : ATAPOD), DBNull.Value)).Direction = ParameterDirection.Input;

                _with46.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with46.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 4, "VOYAGE_TRN_PK").Direction = ParameterDirection.Output;
                RESULT = _with46.ExecuteNonQuery();
                dblVesselPK = Convert.ToInt32(_with46.Parameters["RETURN_VALUE"].Value);
                _with46.Parameters.Clear();
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;

            }
            catch (Exception ex)
            {
                if (string.Compare(ex.Message, "ORA-00001")>0)
                {
                    arrMessage.Add("Vessel or Voyage Already Exist in Database.");
                }
                else
                {
                    arrMessage.Add(ex.Message);
                }
                return arrMessage;
            }
        }
        public Int16 FetchEBKN(Int16 BookingPK)
        {

            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            Int16 IS_EBOOKING = default(Int16);
            try
            {
                sqlStr = "select IS_EBOOKING from booking_MST_tbl where booking_MST_pk='" + BookingPK + "'";
                IS_EBOOKING = Convert.ToInt16(objWF.ExecuteScaler(sqlStr));
                return IS_EBOOKING;
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
        public string FetchBkgDate(string BOOKING_REF_NO)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            string BOOKINGDate = null;
            try
            {
                sqlStr = "select BOOKING_DATE from booking_MST_tbl where BOOKING_REF_NO='" + BOOKING_REF_NO + "' ";
                BOOKINGDate = objWF.ExecuteScaler(sqlStr);
                if (string.IsNullOrEmpty(BOOKINGDate))
                {
                    return "";
                }
                else
                {
                    return BOOKINGDate;
                }
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
        #endregion

        #region "Update New JobCard Values"
        public void UpdateNewJobCardValues(int MergedBkgPk)
        {
            Int32 NewJobPK = Convert.ToInt32(HttpContext.Current.Session["NewJobPK"]);
            WorkFlow objWF = new WorkFlow();
            OracleTransaction TRAN = null;
            try
            {
                objWF.OpenConnection();
                TRAN = objWF.MyConnection.BeginTransaction();
                objWF.MyCommand.Transaction = TRAN;

                var _with47 = objWF.MyCommand;
                _with47.CommandType = CommandType.StoredProcedure;
                _with47.CommandText = objWF.MyUserName + ".MERGE_BOOKING_SEA_PKG.UPDATE_NEWJOB_DETAIL";
                _with47.Parameters.Clear();

                _with47.Parameters.Add("MERGED_BKGFK_IN", MergedBkgPk).Direction = ParameterDirection.Input;
                _with47.Parameters["MERGED_BKGFK_IN"].SourceVersion = DataRowVersion.Current;

                _with47.Parameters.Add("NEW_JOB_FK_IN", NewJobPK).Direction = ParameterDirection.Input;
                _with47.Parameters["NEW_JOB_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with47.ExecuteNonQuery();
                TRAN.Commit();
                HttpContext.Current.Session["NewJobPK"] = null;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyCommand.Connection.Close();
            }

        }
        #endregion

        #region "Check Merge Criteria"
        public string CheckMergeCriteria(string str_Bookingpk)
        {
            string Msg = null;
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWF = new WorkFlow();
                DataSet ChkDS = new DataSet();
                DataSet ChkDS1 = new DataSet();
                int Rcnt = 0;
                int Rcnt1 = 0;
                Msg = "";
                sb.Append("SELECT DISTINCT BK.BOOKING_MST_PK,");
                sb.Append("                 CASE WHEN BK.CARGO_TYPE = 1 THEN");
                sb.Append("                   BT.CONTAINER_TYPE_MST_FK");
                sb.Append("                  ELSE");
                sb.Append("                   BT.BASIS");
                sb.Append("                END CONTAINER_TYPE_MST_FK,");
                sb.Append("                BF.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                BF.TARIFF_RATE,");
                sb.Append("                BF.CURRENCY_MST_FK");
                sb.Append("  FROM BOOKING_MST_TBL          BK,");
                sb.Append("       BOOKING_TRN  BT,");
                sb.Append("       BOOKING_TRN_FRT_DTLS BF");
                sb.Append(" WHERE BK.BOOKING_MST_PK = BT.BOOKING_MST_FK");
                sb.Append("   AND BT.BOOKING_TRN_PK = BF.BOOKING_TRN_FK");
                sb.Append("  AND BK.BOOKING_MST_PK IN (" + str_Bookingpk + ")");
                ChkDS = objWF.GetDataSet(sb.ToString());

                if (ChkDS.Tables[0].Rows.Count > 0)
                {
                    for (Rcnt = 0; Rcnt <= ChkDS.Tables[0].Rows.Count - 1; Rcnt++)
                    {
                        for (Rcnt1 = 0; Rcnt1 <= ChkDS.Tables[0].Rows.Count - 1; Rcnt1++)
                        {
                            if (ChkDS.Tables[0].Rows[Rcnt]["CONTAINER_TYPE_MST_FK"] == ChkDS.Tables[0].Rows[Rcnt1]["CONTAINER_TYPE_MST_FK"] & ChkDS.Tables[0].Rows[Rcnt]["FREIGHT_ELEMENT_MST_FK"] == ChkDS.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_FK"])
                            {
                                if (ChkDS.Tables[0].Rows[Rcnt]["CURRENCY_MST_FK"] != ChkDS.Tables[0].Rows[Rcnt1]["CURRENCY_MST_FK"])
                                {
                                    Msg = "Currencies are Diffirent.Bookings Cannot be merged";
                                    return Msg;
                                }
                                if (ChkDS.Tables[0].Rows[Rcnt]["TARIFF_RATE"] != ChkDS.Tables[0].Rows[Rcnt1]["TARIFF_RATE"])
                                {
                                    Msg = "Rates are Diffirent.Bookings Cannot be merged";
                                    return Msg;
                                }
                            }
                        }
                    }
                }
                return Msg;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
            return Msg;
        }
        #endregion

    }
}