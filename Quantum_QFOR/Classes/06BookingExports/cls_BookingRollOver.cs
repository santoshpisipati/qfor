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
    public class cls_BookingRollOver : CommonFeatures
    {

        #region "Page Level Region"
        #endregion
        OracleTransaction TRAN;

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

        #region "Fetch Listing Screen ReAssignDetails"
        public DataSet FetchListingReAssign(long P_Vessel_PK = 0, long P_Customer_PK = 0, string BkgId = "", string strFromDate = "", string strToDate = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 rec = 0)
        {
            string strCondition = "";
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            DataSet DS = new DataSet();

            if (P_Vessel_PK > 0)
            {
                buildCondition.Append("  AND BST.VESSEL_VOYAGE_FK= " + P_Vessel_PK);
            }
            if (P_Customer_PK > 0)
            {
                buildCondition.Append(" AND BST.CUST_CUSTOMER_MST_FK = " + P_Customer_PK);
            }
            if ((BkgId != null))
            {
                if (!string.IsNullOrEmpty(BkgId.Trim()))
                {
                    buildCondition.Append(" AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + BkgId.ToUpper() + "%'");
                }
            }

            if (strFromDate.Length > 0 & strToDate.Length > 0)
            {
                buildCondition.Append("  and BST.rollover_date between to_date('" + strFromDate + "','" + dateFormat + "') and to_date('" + strToDate + "','" + dateFormat + "')");
            }
            else if (!string.IsNullOrEmpty(strFromDate))
            {
                buildCondition.Append("  and BST.rollover_date = to_date('" + strFromDate + "','" + dateFormat + "')");
            }

            strCondition = buildCondition.ToString();
            strBuilder.Append(" Select count ");
            strBuilder.Append(" (BST.BOOKING_MST_PK )");
            strBuilder.Append(" FROM BOOKING_MST_TBL   BST,");
            strBuilder.Append(" PORT_MST_TBL      POL,");
            strBuilder.Append(" PORT_MST_TBL      POD,");
            strBuilder.Append(" CUSTOMER_MST_TBL  CMT,");
            strBuilder.Append(" VESSEL_VOYAGE_TRN VVT,");
            strBuilder.Append(" VESSEL_VOYAGE_TBL VMT,");
            strBuilder.Append(" VESSEL_VOYAGE_TRN REASSING, ");
            strBuilder.Append(" VESSEL_VOYAGE_TBL REAVMT");
            strBuilder.Append(" WHERE BST.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            strBuilder.Append(" AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
            strBuilder.Append(" AND BST.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strBuilder.Append(" AND BST.VESSEL_VOYAGE_FK = VVT.VOYAGE_TRN_PK");
            strBuilder.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = VMT.VESSEL_VOYAGE_TBL_PK");
            strBuilder.Append(" AND BST.REASSVESSEL_VOYAGE_FK = REASSING.VOYAGE_TRN_PK");
            strBuilder.Append(" AND REASSING.VESSEL_VOYAGE_TBL_FK = REAVMT.VESSEL_VOYAGE_TBL_PK");
            strBuilder.Append(buildCondition);

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strBuilder.ToString()));
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

            strBuilder.Remove(0, strBuilder.Length);

            strBuilder.Append(" Select * from");
            strBuilder.Append("( Select ROWNUM SLNO, q.* from ");
            strBuilder.Append(" ( SELECT  DISTINCT ");
            strBuilder.Append(" BST.BOOKING_MST_PK, ");
            strBuilder.Append(" BST.BOOKING_REF_NO,");
            strBuilder.Append(" BST.CUST_CUSTOMER_MST_FK,");
            strBuilder.Append(" CMT.CUSTOMER_NAME CUSTOMER_ID,");
            strBuilder.Append(" BST.PORT_MST_POL_FK,");
            strBuilder.Append(" POL.PORT_ID POL_ID,");
            strBuilder.Append(" BST.PORT_MST_POD_FK,");
            strBuilder.Append(" POD.PORT_ID POD_ID,");
            strBuilder.Append(" REASSING.POL_ETD,");
            strBuilder.Append(" REAVMT.VESSEL_ID,");
            strBuilder.Append(" BST.VERSION_NO,");
            strBuilder.Append(" VMT.VESSEL_ID AS RVESSEL_ID,");
            strBuilder.Append(" VVT.POL_ETD AS RPOL_ETD");
            strBuilder.Append(" FROM BOOKING_MST_TBL   BST,");
            strBuilder.Append(" PORT_MST_TBL      POL,");
            strBuilder.Append(" PORT_MST_TBL      POD,");
            strBuilder.Append(" CUSTOMER_MST_TBL  CMT,");
            strBuilder.Append(" VESSEL_VOYAGE_TRN VVT,");
            strBuilder.Append(" VESSEL_VOYAGE_TBL VMT,");
            strBuilder.Append(" VESSEL_VOYAGE_TRN REASSING, ");
            strBuilder.Append(" VESSEL_VOYAGE_TBL REAVMT");
            strBuilder.Append("  WHERE BST.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            strBuilder.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
            strBuilder.Append("  AND BST.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strBuilder.Append("  AND BST.VESSEL_VOYAGE_FK = VVT.VOYAGE_TRN_PK");
            strBuilder.Append("  AND VVT.VESSEL_VOYAGE_TBL_FK = VMT.VESSEL_VOYAGE_TBL_PK");
            strBuilder.Append("  AND BST.REASSVESSEL_VOYAGE_FK = REASSING.VOYAGE_TRN_PK");
            strBuilder.Append("  AND REASSING.VESSEL_VOYAGE_TBL_FK = REAVMT.VESSEL_VOYAGE_TBL_PK");
            strBuilder.Append(buildCondition);
            strBuilder.Append("  Order By BST.BOOKING_REF_NO DESC");
            strBuilder.Append("  ) q) where SLNO between " + start + " AND " + last);

            try
            {
                DS = objWF.GetDataSet(strBuilder.ToString());
                return DS;
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
        #endregion

        #region "Fetch ReAssignDetails"
        public DataSet FetchBookings(long P_Vessel_PK, long P_Customer_PK, long P_POD_PK, long P_Booking_PK, string FrmFlg = "")
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                if (P_Customer_PK > 0)
                {
                    strCondition.Append(" AND BT.CUST_CUSTOMER_MST_FK = " + P_Customer_PK);
                }
                if (P_POD_PK > 0)
                {
                    strCondition.Append(" AND BT.PORT_MST_POD_FK = " + P_POD_PK);
                }
                if (P_Booking_PK > 0)
                {
                    strCondition.Append(" AND BT.BOOKING_MST_PK = " + P_Booking_PK);
                }
                if (P_Vessel_PK > 0)
                {
                    strCondition.Append(" AND BT.VESSEL_VOYAGE_FK = " + P_Vessel_PK);
                }
                else
                {
                    strCondition.Append(" AND BT.VESSEL_VOYAGE_FK = 0");
                }

                strBuilder.Append(" Select  * from ( SELECT ROWNUM SLNO,q.* FROM ( ");
                strBuilder.Append(" SELECT");
                strBuilder.Append(" BT.BOOKING_MST_PK,");
                strBuilder.Append(" BT.BOOKING_REF_NO,");
                strBuilder.Append(" TO_DATE(BT.BOOKING_DATE,DATEFORMAT) BOOKING_DATE,");
                strBuilder.Append(" BT.CUST_CUSTOMER_MST_FK,");
                strBuilder.Append(" CMT.CUSTOMER_ID,");
                strBuilder.Append(" BT.PORT_MST_POL_FK,");
                strBuilder.Append(" POL.PORT_ID POL,");
                strBuilder.Append(" BT.PORT_MST_POD_FK,");
                strBuilder.Append(" POD.PORT_ID POD,");
                strBuilder.Append(" BT.Version_No,");
                strBuilder.Append(" '' REASSIGN");
                strBuilder.Append(" FROM BOOKING_MST_TBL  BT,");
                strBuilder.Append(" CUSTOMER_MST_TBL CMT,");
                strBuilder.Append(" PORT_MST_TBL     POL,");
                strBuilder.Append(" PORT_MST_TBL POD ");
                strBuilder.Append(" WHERE BT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
                strBuilder.Append(" AND BT.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                strBuilder.Append(" AND BT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                if (FrmFlg == "BKGRollOver")
                {
                    strBuilder.Append(" AND BT.BOOKING_MST_PK = " + P_Booking_PK);
                }
                else
                {
                    strBuilder.Append(" AND BT.STATUS =2");
                }
                strBuilder.Append(strCondition);
                strBuilder.Append(" )q)");

                DS = objWF.GetDataSet(strBuilder.ToString());
                return DS;
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
        #endregion

        #region "Fetch "


        public string FetchVoyageForBookingRollOver(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = null;
            string strBizType = null;
            string strBookingPK = null;
            string strReq = null;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strVOY = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strBookingPK = Convert.ToString(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_VES_VOY_BOOKINGROLLOVER";

                var _with1 = selectCommand.Parameters;
                _with1.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with1.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", getDefault(strReq, DBNull.Value)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", getDefault(strBizType, 3)).Direction = ParameterDirection.Input;
                _with1.Add("BOOKING_SEA_PK_IN", getDefault(strBookingPK, 0)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
                selectCommand.Connection.Close();
            }
        }

        public string FetchVoyageReAssignRollOver(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = null;
            string strBizType = null;
            string strBookingPK = null;
            string strReq = null;
            string strLoc = null;
            string strPODPK = null;
            string strVesVoyPk = null;
            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strVOY = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strBookingPK = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strPODPK = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strVesVoyPk = Convert.ToString(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_VES_VOY_ReAssignBooking";

                var _with2 = selectCommand.Parameters;
                _with2.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with2.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", getDefault(strReq, "")).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", getDefault(strBizType, 3)).Direction = ParameterDirection.Input;
                _with2.Add("BOOKING_SEA_PK_IN", getDefault(strBookingPK, 0)).Direction = ParameterDirection.Input;
                _with2.Add("POD_PK_IN", getDefault(strPODPK, 0)).Direction = ParameterDirection.Input;
                _with2.Add("VES_VOY_PK_IN", getDefault(strVesVoyPk, 0)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
                selectCommand.Connection.Close();
            }
        }

        public string FetchVslVoyRollListing(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = null;
            string strBizType = null;
            string strBookingPK = null;
            string strReq = null;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strVOY = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strBookingPK = Convert.ToString(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VslVoyRollOverListing";

                var _with3 = selectCommand.Parameters;
                _with3.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with3.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", getDefault(strReq, DBNull.Value)).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", getDefault(strBizType, 3)).Direction = ParameterDirection.Input;
                _with3.Add("BOOKING_SEA_PK_IN", getDefault(strBookingPK, 0)).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
                selectCommand.Connection.Close();
            }
        }

        public string FetchBookingRLNO(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string strSELLOC_FK_IN = "";
            string strVSLVOY_FK_IN = "";
            string strCustomer_FK_IN = "";
            string str_POD_FK_IN = "";
            string strSTATUS_IN = "";
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBusinessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strSELLOC_FK_IN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strVSLVOY_FK_IN = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strCustomer_FK_IN = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                str_POD_FK_IN = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                strSTATUS_IN = Convert.ToString(arr.GetValue(8));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.FetchBookingRLNO";
                var _with4 = SCM.Parameters;
                _with4.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with4.Add("SELLOC_FK_IN", (string.IsNullOrEmpty(strSELLOC_FK_IN) ? "" : strSELLOC_FK_IN)).Direction = ParameterDirection.Input;
                _with4.Add("VslVoy_FK_IN", (string.IsNullOrEmpty(strVSLVOY_FK_IN) ? "" : strVSLVOY_FK_IN)).Direction = ParameterDirection.Input;
                _with4.Add("Customer_FK_IN", (string.IsNullOrEmpty(strCustomer_FK_IN) ? "" : strCustomer_FK_IN)).Direction = ParameterDirection.Input;
                _with4.Add("POD_FK_IN", (string.IsNullOrEmpty(str_POD_FK_IN) ? "" : str_POD_FK_IN)).Direction = ParameterDirection.Input;
                _with4.Add("STATUS_IN", (string.IsNullOrEmpty(strSTATUS_IN) ? "" : strSTATUS_IN)).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
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

        public DataSet FetchRollOverHeaderDetails(long BkgPk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_MST_PK,");
            sb.Append("       BST.BOOKING_REF_NO,");
            sb.Append("       BST.VESSEL_VOYAGE_FK,");
            sb.Append("       BST.REASSVESSEL_VOYAGE_FK,");
            sb.Append("       VVT.VESSEL_ID VID,");
            sb.Append("       VVT.VESSEL_NAME VNAME,");
            sb.Append("       VVT1.VESSEL_ID VID1,");
            sb.Append("       VVT1.VESSEL_NAME VNAME1,");
            sb.Append("       TRN.VOYAGE,");
            sb.Append("       TRN1.VOYAGE VOYAGE1,");
            sb.Append("       BST.ROLLOVER_DATE,");
            sb.Append("       BST.ROLLOVER_REMARKS,");
            sb.Append("       TO_DATE(TRN.POL_ETD,DATEFORMAT) ETD,");
            sb.Append("       TO_DATE(TRN1.POL_ETD,DATEFORMAT) ETD1,");
            sb.Append("       BST.STATUS,");
            sb.Append("       CMT.CUSTOMER_MST_PK,");
            sb.Append("       CMT.CUSTOMER_ID,");
            sb.Append("       CMT.CUSTOMER_NAME,");
            sb.Append("       TRN.VOYAGE_TRN_PK VPK,");
            sb.Append("       TRN1.VOYAGE_TRN_PK VPK1,");
            sb.Append("       POD.PORT_MST_PK PODPK,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       CMT.CUSTOMER_MST_PK,");
            sb.Append("       CMT.CUSTOMER_ID,");
            sb.Append("       CMT.CUSTOMER_NAME");
            sb.Append("  FROM BOOKING_MST_TBL   BST,");
            sb.Append("       VESSEL_VOYAGE_TBL VVT,");
            sb.Append("       VESSEL_VOYAGE_TRN TRN,");
            sb.Append("       VESSEL_VOYAGE_TBL VVT1,");
            sb.Append("       VESSEL_VOYAGE_TRN TRN1,");
            sb.Append("       PORT_MST_TBL      POL,");
            sb.Append("       PORT_MST_TBL      POD,");
            sb.Append("       CUSTOMER_MST_TBL  CMT");
            sb.Append(" WHERE BST.BOOKING_MST_PK = " + BkgPk);
            sb.Append("   AND BST.VESSEL_VOYAGE_FK = TRN.VOYAGE_TRN_PK");
            sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK = TRN.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND BST.REASSVESSEL_VOYAGE_FK = TRN1.VOYAGE_TRN_PK");
            sb.Append("   AND VVT1.VESSEL_VOYAGE_TBL_PK = TRN1.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            sb.Append("   AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND BST.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");

            return (objWF.GetDataSet(sb.ToString()));
        }
        #endregion

        #region "Save Roll Over"

        public ArrayList SaveRollOver(DataSet M_DataSet, Int64 ReAvsl, System.DateTime ReassignDate, string reason)
        {
            WorkFlow ObjWK = new WorkFlow();
            OracleCommand UpdCommand = new OracleCommand();
            Int32 RowCnt = default(Int32);
            ObjWK.OpenConnection();
            TRAN = ObjWK.MyConnection.BeginTransaction();
            int v = 0;
            v = 0;
            try
            {
                var _with5 = UpdCommand;
                _with5.Connection = ObjWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = ObjWK.MyUserName + ".EN_BOOKING_PKG.BOOKING_ROLLOVER_UPDATE";
                for (RowCnt = 0; RowCnt <= M_DataSet.Tables[0].Rows.Count - 1; RowCnt++)
                {
                    if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["REASSIGN"].ToString()))
                    {
                        _with5.Parameters.Clear();
                        var _with6 = _with5.Parameters;
                        _with6.Add("BOOKING_SEA_PK_IN", M_DataSet.Tables[0].Rows[RowCnt]["BOOKING_MST_PK"]).Direction = ParameterDirection.Input;
                        _with6.Add("REASSVESSEL_VOYAGE_FK_IN", ReAvsl).Direction = ParameterDirection.Input;
                        _with6.Add("ROLLOVER_DATE_IN", ReassignDate).Direction = ParameterDirection.Input;
                        _with6.Add("ROLLOVER_REMARKS_IN", (string.IsNullOrEmpty(reason) ? "" : reason)).Direction = ParameterDirection.Input;
                        _with6.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                        _with6.Add("VERSION_NO_IN", M_DataSet.Tables[0].Rows[RowCnt]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        _with6.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                        _with6.Add("return_value", OracleDbType.Varchar2, 100, "return_value").Direction = ParameterDirection.Output;
                        UpdCommand.Parameters["return_value"].SourceVersion = DataRowVersion.Current;
                        var _with7 = ObjWK.MyDataAdapter;
                        _with7.UpdateCommand = UpdCommand;
                        _with7.UpdateCommand.Transaction = TRAN;
                        _with7.UpdateCommand.ExecuteNonQuery();
                    }
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
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
            finally
            {
                ObjWK.MyCommand.Connection.Close();
            }

        }
        #endregion

    }
}