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
using System.Web;

namespace Quantum_QFOR
{
    public class cls_SeaBookingList : CommonFeatures
    {
        #region "Fetch Cargo type"

        public int FetchCargoType(string SBookingRefNo)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT BS.CARGO_TYPE");
            sb.Append("  FROM BOOKING_SEA_TBL BS");
            sb.Append(" WHERE BS.BOOKING_REF_NO = '" + SBookingRefNo + "'");
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #endregion "Fetch Cargo type"

        #region "Fetch Function"

        public DataSet FetchAll(string Shipmentdate = "", string Commodityfk = "0", string BookingType = "0", string BookingPK = "", string CustomerPK = "", string POLPK = "", string PODPK = "", Int16 intCargoType = 0, Int16 intXBkg = 0, string intStatus = "",
        string strSearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, string blnSortAscending = "", long lngUserLocFk = 0, Int16 intCLAgt = 0, Int32 flag = 0, string VesselPk = "",
        bool ShowAllRecords = false)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition = strCondition + " and 1=2 ";
            }

            if (!(Shipmentdate == null))
            {
                strCondition += "      AND BST.SHIPMENT_DATE = TO_DATE('" + Shipmentdate + "','" + dateFormat + "')   ";
            }

            if (Convert.ToInt32(Commodityfk) > 0)
            {
                strCondition = strCondition + " AND BST.Commodity_Group_Fk=" + Commodityfk;
            }

            if (BookingType != "0")
            {
                strCondition = strCondition + " AND BTSFL.TRANS_REFERED_FROM=" + BookingType;
            }

            if (BookingPK.Trim().Length > 0)
            {
                strCondition = strCondition + " And BST.BOOKING_SEA_PK = " + BookingPK;
            }

            if (CustomerPK.Trim().Length > 0)
            {
                strCondition = strCondition + " And BST.CUST_CUSTOMER_MST_FK = " + CustomerPK;
            }

            if (POLPK.Trim().Length > 0)
            {
                strCondition = strCondition + " And BST.PORT_MST_POL_FK = " + POLPK;
            }

            if (PODPK.Trim().Length > 0)
            {
                strCondition = strCondition + " And BST.PORT_MST_POD_FK = " + PODPK;
            }

            if (intXBkg == 1)
            {
                strCondition = strCondition + " And BST.CB_AGENT_MST_FK IS NOT NULL";
            }

            if (intCLAgt == 1)
            {
                strCondition = strCondition + " And BST.Cl_Agent_Mst_Fk IS NOT NULL";
            }
            if (intCargoType > 0)
            {
                strCondition = strCondition + " AND BST.CARGO_TYPE=" + intCargoType;
            }
            if (Convert.ToInt32(VesselPk) > 0)
            {
                strCondition = strCondition + " AND VVT.VOYAGE_TRN_PK = " + VesselPk;
            }

            if (intStatus == "-1")
            {
                strCondition = strCondition + " AND NVL(BST.FROM_FLAG,0)=1";
                strCondition = strCondition + " AND BST.STATUS<>-1";
            }
            else
            {
                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (Convert.ToInt32(intStatus) == 4)
                    {
                        strCondition = strCondition + " AND BST.IS_EBOOKING=1";
                    }
                    else
                    {
                        if (Convert.ToInt32(intStatus) != 7)
                        {
                            strCondition = strCondition + " AND BST.STATUS=" + intStatus;
                        }
                        strCondition = strCondition + " AND BST.IS_EBOOKING=0";
                    }
                }
                else
                {
                    strCondition = strCondition + " AND BST.STATUS=" + intStatus;
                }
            }
            strCondition = strCondition + " AND PMTL.LOCATION_MST_FK = " + lngUserLocFk + " ";

            strSQL += " SELECT DISTINCT ";
            strSQL += " BST.BOOKING_SEA_PK,";
            strSQL += " UPPER(BST.BOOKING_REF_NO),";
            strSQL += " BST.BOOKING_DATE,";
            strSQL += " CMT.CUSTOMER_NAME,";
            //'
            strSQL += " UPPER(JCSET.JOBCARD_REF_NO), ";
            strSQL += " PMTL.PORT_ID AS POL,";
            strSQL += " PMTD.PORT_ID AS POD,";
            strSQL += " AMTCB.AGENT_ID AS CBAGENT,";
            strSQL += " AMTCL.AGENT_ID AS CLAGENT,";
            strSQL += " BST.SHIPMENT_DATE AS SHIPMENTDATE,";
            strSQL += " CGMT.COMMODITY_GROUP_CODE AS COMMGROUP,";
            strSQL += " DECODE(BTSFL.TRANS_REFERED_FROM,0,'All',1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') AS BOOKINGTYPE,";
            ///'
            //'
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                if (Convert.ToInt32(intStatus) == 4)
                {
                    strSQL += " DECODE(BST.STATUS,-1,'Nominated',1,'Pending',2,'Confirm',3,'Rejected',5,'CartedCargo',6,'Shipped',7,'All')STATUS,";
                }
                else
                {
                    strSQL += " DECODE(BST.STATUS,-1,'Nominated',1,'Provisional',2,'Confirm',3,'Cancelled',5,'CartedCargo',6,'Shipped',7,'All')STATUS,";
                }
            }
            else
            {
                strSQL += " DECODE(BST.STATUS,-1,'Nominated',1,'Provisional',2,'Confirm',3,'Cancelled',5,'CartedCargo',6,'Shipped',7,'All')STATUS,";
            }
            strSQL += " JCSET.JOB_CARD_SEA_EXP_PK ";
            strSQL += " , DECODE(BST.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE ";
            strSQL += " FROM BOOKING_SEA_TBL BST,";

            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                strSQL += " V_ALL_CUSTOMER CMT,";
            }
            else
            {
                strSQL += " CUSTOMER_MST_TBL CMT,";
            }

            strSQL += " PORT_MST_TBL PMTL, PORT_MST_TBL PMTD,";
            strSQL += " AGENT_MST_TBL AMTCB, AGENT_MST_TBL AMTCL, JOB_CARD_SEA_EXP_TBL JCSET, ";
            strSQL += " COMMODITY_GROUP_MST_TBL CGMT, BOOKING_TRN_SEA_FCL_LCL BTSFL,VESSEL_VOYAGE_TRN VVT";
            ///
            strSQL += " WHERE ";
            strSQL += " BST.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+)";
            strSQL += " AND BST.PORT_MST_POL_FK=PMTL.PORT_MST_PK(+)";
            strSQL += " AND BST.PORT_MST_POD_FK=PMTD.PORT_MST_PK(+)";
            strSQL += " AND BST.CB_AGENT_MST_FK=AMTCB.AGENT_MST_PK(+)";
            strSQL += " AND BST.CL_AGENT_MST_FK=AMTCL.AGENT_MST_PK(+)";
            strSQL += " AND BST.BOOKING_SEA_PK=JCSET.BOOKING_SEA_FK (+)";
            strSQL += " AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK";
            strSQL += " AND BTSFL.BOOKING_SEA_FK(+)=BST.BOOKING_SEA_PK";
            strSQL += " AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK";
            strSQL += strCondition;

            DataTable tbl = new DataTable();
            tbl = objWF.GetDataTable(strSQL.ToString());
            TotalRecords = (Int32)tbl.Rows.Count;
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SR_NO\", QRY.* FROM ");
            sqlstr.Append("  (" + strSQL.ToString() + " ");
            sqlstr.Append("   ORDER BY TO_DATE(BOOKING_DATE) DESC, UPPER(BST.BOOKING_REF_NO) DESC) QRY )Q ");
            if (!ShowAllRecords)
            {
                sqlstr.Append("  WHERE Q.SR_NO  BETWEEN " + start + " AND " + last + "");
            }
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #region "Fetch Function"

        public DataSet FetchNominations(string Shipmentdate = "", string Commodityfk = "0", string BookingType = "0", string BookingPK = "", string CustomerPK = "", string POLPK = "", string PODPK = "", Int16 intCargoType = 0, Int16 intXBkg = 0, string intStatus = "",
        string strSearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, string blnSortAscending = "", long lngUserLocFk = 0, Int16 intCLAgt = 0, Int32 flag = 0, string VesselPk = "",
        int EXECUTIVE_FK = 0, string PO_NUMBER = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (flag == 0)
            {
                strCondition = strCondition + " and 1=2 ";
            }
            if (!(Shipmentdate == null))
            {
                strCondition += "      AND BST.SHIPMENT_DATE = TO_DATE('" + Shipmentdate + "','" + dateFormat + "')   ";
            }
            if (Convert.ToInt32(Commodityfk) > 0)
            {
                strCondition = strCondition + " AND BST.Commodity_Group_Fk=" + Commodityfk;
            }
            if (BookingType != "0")
            {
                strCondition = strCondition + " AND BTSFL.TRANS_REFERED_FROM=" + BookingType;
            }
            try
            {
                if (Convert.ToInt32(BookingPK) > 0)
                {
                    strCondition = strCondition + " And BST.BOOKING_SEA_PK = " + BookingPK;
                }
            }
            catch (Exception ex)
            {
            }
            try
            {
                if (Convert.ToInt32(CustomerPK) > 0)
                {
                    strCondition = strCondition + " And BST.CUST_CUSTOMER_MST_FK = " + CustomerPK;
                }
            }
            catch (Exception ex)
            {
            }
            try
            {
                if (Convert.ToInt32(POLPK) > 0)
                {
                    strCondition = strCondition + " And BST.PORT_MST_POL_FK = " + POLPK;
                }
            }
            catch (Exception ex)
            {
            }
            try
            {
                if (Convert.ToInt32(PODPK) > 0)
                {
                    strCondition = strCondition + " And BST.PORT_MST_POD_FK = " + PODPK;
                }
            }
            catch (Exception ex)
            {
            }
            if (intXBkg == 1)
            {
                strCondition = strCondition + " And BST.CB_AGENT_MST_FK IS NOT NULL";
            }
            if (intCLAgt == 1)
            {
                strCondition = strCondition + " And BST.Cl_Agent_Mst_Fk IS NOT NULL";
            }
            if (intCargoType > 0)
            {
                strCondition = strCondition + " AND BST.CARGO_TYPE=" + intCargoType;
            }
            if (Convert.ToInt32(VesselPk) > 0)
            {
                strCondition = strCondition + " AND VVT.VOYAGE_TRN_PK = " + VesselPk;
            }

            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                if (Convert.ToInt32(intStatus) == 4)
                {
                    strCondition = strCondition + " AND BST.IS_EBOOKING=1";
                }
                else
                {
                    if (Convert.ToInt32(intStatus) != 7)
                    {
                        strCondition = strCondition + " AND BST.STATUS=" + intStatus;
                    }
                    strCondition = strCondition + " AND BST.IS_EBOOKING=0";
                }
            }
            else
            {
                strCondition = strCondition + " AND BST.STATUS=" + intStatus;
            }

            strCondition = strCondition + " AND NVL(BST.FROM_FLAG,0) = 1";
            if (EXECUTIVE_FK > 0)
            {
                strCondition = strCondition + " AND NVL(EMP.EMPLOYEE_MST_PK,0) = " + EXECUTIVE_FK;
            }
            if (!string.IsNullOrEmpty(PO_NUMBER.Trim()))
            {
                strCondition = strCondition + " AND BST.PO_NUMBER LIKE '%" + PO_NUMBER.Trim().ToUpper() + "%'";
            }
            strCondition = strCondition + " AND PMTD.LOCATION_MST_FK = " + lngUserLocFk + " ";

            strSQL += " SELECT DISTINCT ";
            strSQL += " BST.BOOKING_SEA_PK,";
            strSQL += " UPPER(BST.NOMINATION_REF_NO) BOOKING_REF_NO,";
            strSQL += " TO_CHAR(BST.BOOKING_DATE,DATEFORMAT) BOOKING_DATE,";
            strSQL += " CMT.CUSTOMER_NAME,";
            strSQL += " JCSET.JOB_CARD_SEA_EXP_PK, ";
            strSQL += " UPPER(JCSET.JOBCARD_REF_NO) JOBCARD_REF_NO, ";
            strSQL += " PMTL.PORT_ID AS POL,";
            strSQL += " PMTD.PORT_ID AS POD,";
            strSQL += " TO_CHAR(BST.SHIPMENT_DATE,DATEFORMAT) AS SHIPMENTDATE,";
            strSQL += " BST.PO_NUMBER,";
            strSQL += " TO_CHAR(BST.PO_DATE,DATEFORMAT) PO_DATE, ";
            strSQL += " CGMT.COMMODITY_GROUP_CODE AS COMMGROUP,";
            strSQL += " DECODE(BTSFL.TRANS_REFERED_FROM,0,'All',1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') AS BOOKINGTYPE,";
            strSQL += " DECODE(BST.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE, ";
            strSQL += " EMP.EMPLOYEE_MST_PK EXECUTIVE_FK,";
            strSQL += " EMP.EMPLOYEE_ID EXECUTIVE_ID,";
            strSQL += " EMP.EMPLOYEE_NAME EXECUTIVE_NAME,";
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                if (Convert.ToInt32(intStatus) == 4)
                {
                    strSQL += " DECODE(BST.STATUS,-1,'Nominated',1,'Pending',2,'Confirm',3,'Rejected',5,'CartedCargo',6,'Shipped',7,'All')STATUS ";
                }
                else
                {
                    strSQL += " DECODE(BST.STATUS,-1,'Nominated',1,'Provisional',2,'Confirm',3,'Cancelled',5,'CartedCargo',6,'Shipped',7,'All')STATUS ";
                }
            }
            else
            {
                strSQL += " DECODE(BST.STATUS,-1,'Nominated',1,'Provisional',2,'Confirm',3,'Cancelled',5,'CartedCargo',6,'Shipped',7,'All')STATUS ";
            }
            strSQL += " FROM BOOKING_SEA_TBL BST,";
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                strSQL += " V_ALL_CUSTOMER CMT,";
                strSQL += " CUSTOMER_MST_TBL CUST,";
            }
            else
            {
                strSQL += " CUSTOMER_MST_TBL CMT,";
            }
            strSQL += " EMPLOYEE_MST_TBL  EMP,";
            strSQL += " PORT_MST_TBL PMTL, PORT_MST_TBL PMTD,";
            strSQL += " AGENT_MST_TBL AMTCB, AGENT_MST_TBL AMTCL, JOB_CARD_SEA_EXP_TBL JCSET, ";
            strSQL += " COMMODITY_GROUP_MST_TBL CGMT, BOOKING_TRN_SEA_FCL_LCL BTSFL,VESSEL_VOYAGE_TRN VVT";
            strSQL += " WHERE ";
            strSQL += " BST.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+)";
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                strSQL += " AND CMT.CUSTOMER_MST_PK = CUST.CUSTOMER_MST_PK(+) ";
                strSQL += " AND CUST.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
            }
            else
            {
                strSQL += " AND CMT.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
            }
            strSQL += " AND BST.PORT_MST_POL_FK=PMTL.PORT_MST_PK(+)";
            strSQL += " AND BST.PORT_MST_POD_FK=PMTD.PORT_MST_PK(+)";
            strSQL += " AND BST.CB_AGENT_MST_FK=AMTCB.AGENT_MST_PK(+)";
            strSQL += " AND BST.CL_AGENT_MST_FK=AMTCL.AGENT_MST_PK(+)";
            strSQL += " AND BST.BOOKING_SEA_PK=JCSET.BOOKING_SEA_FK (+)";
            strSQL += " AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK";
            strSQL += " AND BTSFL.BOOKING_SEA_FK(+)=BST.BOOKING_SEA_PK";
            strSQL += " AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK";
            strSQL += strCondition;

            DataTable tbl = new DataTable();
            tbl = objWF.GetDataTable(strSQL.ToString());
            TotalRecords = (Int32)tbl.Rows.Count;
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SR_NO\", QRY.* FROM ");
            sqlstr.Append("  (" + strSQL.ToString() + " ");
            sqlstr.Append("   ORDER BY TO_DATE(BOOKING_DATE) DESC) QRY )Q ");
            sqlstr.Append("  WHERE Q.SR_NO  BETWEEN " + start + " AND " + last + "");
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #region " Enhance Search Functions "

        public string FetchBookingNo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string strSELLOC_FK_IN = "";
            string strCARGOTYPE_IN = "";
            string strSTATUS_IN = "";
            string strReq = null;
            var strNull = "";
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
                strCARGOTYPE_IN = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strSTATUS_IN = Convert.ToString(arr.GetValue(6));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_NO_COMMON";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with1.Add("SELLOC_FK_IN", (string.IsNullOrEmpty(strSELLOC_FK_IN) ? strNull : strSELLOC_FK_IN)).Direction = ParameterDirection.Input;
                _with1.Add("CARGO_TYPE_IN", (string.IsNullOrEmpty(strCARGOTYPE_IN) ? strNull : strCARGOTYPE_IN)).Direction = ParameterDirection.Input;
                _with1.Add("STATUS_IN", (string.IsNullOrEmpty(strSTATUS_IN) ? strNull : strSTATUS_IN)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #region " Supporting Function "

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

        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, ""))
            {
                return "";
            }
            return col;
        }

        #endregion " Supporting Function "

        #endregion " Enhance Search Functions "

        #region "FETCH TRANSPORT NOTE BOOKING NUMBERS"

        public string FetchTransportNoteBookingNo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBusinessType = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_TRANSPORT_BOOKING_NO";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
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

        #endregion "FETCH TRANSPORT NOTE BOOKING NUMBERS"

        #region "Get Booking Count"

        public int GetBookingCount(string BkgRefNr, long BkgPk)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(1000);
                sb.Append(" SELECT BAT.BOOKING_SEA_PK, BAT.BOOKING_REF_NO");
                sb.Append(" FROM BOOKING_SEA_TBL BAT");
                sb.Append(" WHERE BAT.BOOKING_REF_NO LIKE '%" + BkgRefNr + "%'");
                DataSet ds = new DataSet();
                ds = (new WorkFlow()).GetDataSet(sb.ToString());
                if (ds.Tables[0].Rows.Count == 1)
                {
                    BkgRefNr = Convert.ToString(ds.Tables[0].Rows[0][1]);
                    BkgPk = Convert.ToInt32(ds.Tables[0].Rows[0][0]);
                }
                return ds.Tables[0].Rows.Count;
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

        #endregion "Get Booking Count"

        #region "AutoComplete"

        public string GetFilterTextForBkgNr()
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT BST.BOOKING_REF_NO FROM BOOKING_SEA_TBL BST ORDER BY BST.BOOKING_DATE DESC");
            return AutoCompleteTable(strqry.ToString());
        }

        public string GetFilterTextForPOL()
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT POL.PORT_ID FROM PORT_MST_TBL POL WHERE POL.ACTIVE_FLAG = 1 AND POL.BUSINESS_TYPE = 2 AND POL.LOCATION_MST_FK IN ");
            strqry.Append("(SELECT LMT.LOCATION_MST_PK FROM LOCATION_MST_TBL LMT START WITH LMT.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR LMT.LOCATION_MST_PK = LMT.REPORTING_TO_FK)");
            return AutoCompleteTable(strqry.ToString());
        }

        public string GetFilterTextForPOD()
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT POL.PORT_ID FROM PORT_MST_TBL POL WHERE POL.ACTIVE_FLAG = 1 AND POL.BUSINESS_TYPE = 2 AND POL.LOCATION_MST_FK NOT IN ");
            strqry.Append("(SELECT LMT.LOCATION_MST_PK FROM LOCATION_MST_TBL LMT START WITH LMT.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR LMT.LOCATION_MST_PK = LMT.REPORTING_TO_FK)");
            return AutoCompleteTable(strqry.ToString());
        }

        public string GetFilterTextForCustomer()
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT CMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMT WHERE CMT.ACTIVE_FLAG=1 ORDER BY 1");
            return AutoCompleteTable(strqry.ToString());
        }

        public string GetFilterTextForVslVoy(int POL_MST_FK = 0)
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT DISTINCT VVT.VESSEL_NAME  FROM VESSEL_VOYAGE_TBL VVT, VESSEL_VOYAGE_TRN VVTRN ");
            strqry.Append("WHERE VVT.VESSEL_VOYAGE_TBL_PK = VVTRN.VESSEL_VOYAGE_TBL_FK(+) ");
            if (POL_MST_FK > 0)
            {
                strqry.Append(" AND VVTRN.PORT_MST_POL_FK = " + POL_MST_FK);
            }
            return AutoCompleteTable(strqry.ToString());
        }

        public string AutoCompleteTable(string Query)
        {
            WorkFlow objwf = new WorkFlow();
            return AutoCompleteString(objwf.GetDataTable(Query));
        }

        public string AutoCompleteString(DataTable _DataTable)
        {
            string returnValue = "";
            foreach (DataRow row in _DataTable.Rows)
            {
                if (string.IsNullOrEmpty(returnValue))
                {
                    returnValue = "\"" + row[0] + "\"";
                }
                else
                {
                    returnValue += "," + "\"" + row[0] + "\"";
                }
            }
            return returnValue;
        }

        #endregion "AutoComplete"
    }
}