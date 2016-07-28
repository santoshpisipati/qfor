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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    public class clsMAWBListing : CommonFeatures
    {

        #region "Fetch MAWB records"
        public int GetMAWBCount(string MAWBRefNr, int MAWBPk)
        {
            try
            {
                System.Text.StringBuilder strMAWBQuery = new System.Text.StringBuilder(5000);
                strMAWBQuery.Append(" select mawb.mawb_exp_tbl_pk from mawb_exp_tbl mawb where mawb.mawb_ref_no like '%" + MAWBRefNr + "%'");
                WorkFlow objWF = new WorkFlow();
                DataSet objMAWBDS = null;
                objMAWBDS = objWF.GetDataSet(strMAWBQuery.ToString());
                if (objMAWBDS.Tables[0].Rows.Count == 1)
                {
                    MAWBPk = Convert.ToInt32(objMAWBDS.Tables[0].Rows[0][0]);
                }
                return objMAWBDS.Tables[0].Rows.Count;
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
        //End -Sreenivas
        #endregion

        #region "Header Details"
        private enum Header
        {
            SR_NO = 0,
            MAWBPK = 1,
            MAWBREFNO = 2,
            HAWBPK = 3,
            HAWBREFNO = 4,
            HAWBDATE = 5,
            SHIPPK = 6,
            SHIPNAME = 7,
            JAEPK = 8,
            BKGPK = 9,
            POLNAME = 10,
            PODNAME = 11,
            AIRLINE = 12,
            FLIGHT = 13,
            DELIVERDATA = 14,
            DELIVERTO = 15,
            SEL = 16,
            Flag = 17
        }
        #endregion

        #region "Fetch All"
        public DataSet FetchAll(string MAWBRefNo = "", string Shipperid = "", string POLID = "", string PODID = "", string POLname = "", string PODname = "", string MAWBdate = "", string Airline = "", string Consignee = "", string Commodity = "",
        string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long usrLocFK = 0, Int32 ULDType = 0, string ULDNumber = "", Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }
            if ((ULDType != 0))
            {
                strCondition += " AND UPPER(M.airfreight_slabs_tbl_fk) = '" + ULDType + "'";
            }
            if (ULDNumber.Length > 0)
            {
                // strCondition &= " AND UPPER(M.uld_number) = '" & ULDNumber.ToUpper.Replace("'", "''") & "'"
                strCondition += " AND UPPER(M.uld_number) LIKE '%" + ULDNumber.ToUpper().Replace("'", "''") + "%' ";
            }
            if (MAWBRefNo.Length > 0)
            {
                strCondition += " AND UPPER(M.MAWB_REF_NO) LIKE '%" + MAWBRefNo.ToUpper().Replace("'", "''") + "%'";
            }
            if (Shipperid.Length > 0)
            {
                strCondition += " AND UPPER(M.SHIPPER_NAME) = '" + Shipperid.ToUpper().Replace("'", "''") + "'";
            }
            if (POLID.Length > 0)
            {
                strCondition += " AND UPPER(POL.PORT_ID) = '" + POLID.ToUpper().Replace("'", "''") + "'";
            }
            if (PODID.Length > 0)
            {
                strCondition += " AND UPPER(POD.PORT_ID) = '" + PODID.ToUpper().Replace("'", "''") + "'";
            }

            if (MAWBdate.Length > 0)
            {
                strCondition += " AND M.MAWB_DATE = TO_DATE('" + MAWBdate + "','" + dateFormat + "')";
            }
            if (Airline.Length > 0)
            {
                strCondition += "  AND A.AIRLINE_ID LIKE '%" + Airline.ToUpper().Replace("'", "''") + "%'";
            }
            if (Consignee.Length > 0)
            {
                strCondition += " AND UPPER(M.CONSIGNEE_NAME) = '" + Consignee.ToUpper().Replace("'", "''") + "'";
            }
            if (Commodity.Length > 0 & Commodity != "0")
            {
                strCondition += " AND M.COMMODITY_GROUP_FK =" + Commodity;
            }

            strSQL += " SELECT  COUNT(*) ";
            strSQL += "  FROM MAWB_EXP_TBL M,";
            strSQL += " AIRLINE_MST_TBL A,";
            strSQL += " PORT_MST_TBL POL,";
            strSQL += " PORT_MST_TBL POD,";
            strSQL += " COMMODITY_GROUP_MST_TBL COM,";
            strSQL += " USER_MST_TBL UMT ";
            strSQL += " WHERE ";
            strSQL += " A.AIRLINE_MST_PK(+)=M.AIRLINE_MST_FK AND";
            strSQL += " UMT.DEFAULT_LOCATION_FK = " + usrLocFK + "  ";
            strSQL += " AND M.CREATED_BY_FK = UMT.USER_MST_PK ";
            strSQL += " AND POL.PORT_MST_PK(+)=M.PORT_MST_POL_FK AND";
            strSQL += " POD.PORT_MST_PK(+)=M.PORT_MST_POD_FK AND";
            strSQL += " COM.COMMODITY_GROUP_PK(+)=M.COMMODITY_GROUP_FK";
            strSQL += strCondition;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

            strSQL = "";
            strSQL += " SELECT * from (";
            strSQL += " SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += " (SELECT ";
            strSQL += " DISTINCT M.MAWB_EXP_TBL_PK,";
            strSQL += " M.MAWB_REF_NO,";
            strSQL += " M.MAWB_DATE,";
            strSQL += " A.AIRLINE_NAME,";
            strSQL += " POL.PORT_ID POL,";
            strSQL += " POD.PORT_ID POD,";
            strSQL += " COM.COMMODITY_GROUP_CODE,";
            strSQL += " M.SHIPPER_NAME,";
            strSQL += " M.CONSIGNEE_NAME";
            strSQL += " FROM";
            strSQL += " MAWB_EXP_TBL M ,";
            strSQL += " AIRLINE_MST_TBL A,";
            strSQL += " PORT_MST_TBL POL,";
            strSQL += " PORT_MST_TBL POD,";
            strSQL += " COMMODITY_GROUP_MST_TBL COM,";
            strSQL += " USER_MST_TBL UMT,JOB_CARD_TRN JOB";
            strSQL += " WHERE ";
            strSQL += " A.AIRLINE_MST_PK(+)=M.AIRLINE_MST_FK AND";
            strSQL += " UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ";
            strSQL += " AND M.CREATED_BY_FK = UMT.USER_MST_PK ";
            strSQL += " AND POL.PORT_MST_PK(+)=M.PORT_MST_POL_FK AND";
            strSQL += " POD.PORT_MST_PK(+)=M.PORT_MST_POD_FK AND";
            strSQL += " COM.COMMODITY_GROUP_PK(+)=M.COMMODITY_GROUP_FK";
            strSQL += "  AND M.MAWB_EXP_TBL_PK=JOB.MBL_MAWB_FK";
            strSQL += strCondition;

            strSQL += " ORDER BY M.MAWB_DATE DESC,M.MAWB_REF_NO DESC) q  ) ";
            strSQL += " WHERE SR_NO  Between " + start + " and " + last;

            DataSet DS = null;
            DS = objWF.GetDataSet(strSQL);
            try
            {
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

        #region "Fetch Export Cover Sheet Details"
        public DataSet FetchAirUserExport(string MAWBRefNo, Int32 CurrentPage = 0, Int32 TotalPage = 0, int Loc = 0, Int32 flag = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (MAWBRefNo.Length > 0)
            {
                strCondition += " AND UPPER(MAWB.MAWB_REF_NO) LIKE '%" + MAWBRefNo.ToUpper().Replace("'", "''") + "%' ";
            }

            strSQL = "SELECT count(*)";
            strSQL += "FROM HAWB_EXP_TBL HAWB,";
            strSQL += "MAWB_EXP_TBL MAWB,";
            strSQL += "CUSTOMER_MST_TBL CMST,";
            strSQL += "JOB_CARD_TRN JAE,";
            strSQL += "BOOKING_MST_TBL BAT,";
            strSQL += "PORT_MST_TBL POL,";
            strSQL += "PORT_MST_TBL POD,";
            strSQL += "AIRLINE_MST_TBL AMST,";
            strSQL += "user_mst_tbl umt ";
            strSQL += "WHERE MAWB.MAWB_EXP_TBL_PK = HAWB.MAWB_EXP_TBL_FK";
            strSQL += "AND CMST.CUSTOMER_MST_PK(+)=HAWB.SHIPPER_CUST_MST_FK ";
            strSQL += "AND JAE.JOB_CARD_TRN_PK(+)=HAWB.JOB_CARD_AIR_EXP_FK ";
            strSQL += "AND JAE.BOOKING_MST_FK=BAT.BOOKING_MST_PK(+) ";
            strSQL += "AND POL.PORT_MST_PK(+)=BAT.PORT_MST_POL_FK";
            strSQL += "AND POD.PORT_MST_PK(+)=BAT.PORT_MST_POD_FK";
            strSQL += "AND AMST.AIRLINE_MST_PK(+)=BAT.CARRIER_MST_FK";
            strSQL += "AND JAE.created_by_fk= umt.user_mst_pk";
            strSQL += "AND umt.default_location_fk=" + Loc;
            strSQL += "AND  JAE.JOB_CARD_STATUS =1";
            strSQL += strCondition;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
            strSQL = "SELECT * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT MAWB.MAWB_EXP_TBL_PK MAWBPK,";
            strSQL += "MAWB.MAWB_REF_NO MAWBREFNO,";
            strSQL += "HAWB.HAWB_EXP_TBL_PK HAWBPK,";
            strSQL += "HAWB.HAWB_REF_NO HAWBREFNO,";
            strSQL += "TO_DATE(HAWB.HAWB_DATE,'" + dateFormat + "') HAWBDATE,";
            strSQL += "CMST.CUSTOMER_MST_PK SHIPPK,";
            strSQL += "CMST.CUSTOMER_NAME SHIPNAME,";
            strSQL += "JAE.JOB_CARD_TRN_PK,";
            strSQL += "BAT.BOOKING_MST_PK BKGPK,";
            strSQL += "POL.PORT_ID POLNAME,";
            strSQL += "POD.PORT_ID PODNAME,";
            strSQL += "AMST.AIRLINE_NAME AIRLINE,";
            strSQL += "HAWB.FLIGHT_NO FLIGHT,";
            strSQL += "''DELIVERDATA,";
            strSQL += "'' DeliverTo";
            strSQL += "FROM HAWB_EXP_TBL HAWB,";
            strSQL += "MAWB_EXP_TBL MAWB,";
            strSQL += "CUSTOMER_MST_TBL CMST,";
            strSQL += "JOB_CARD_TRN JAE,";
            strSQL += "BOOKING_MST_TBL BAT,";
            strSQL += "PORT_MST_TBL POL,";
            strSQL += "PORT_MST_TBL POD,";
            strSQL += "AIRLINE_MST_TBL AMST,";
            strSQL += "user_mst_tbl umt ";

            strSQL += "WHERE MAWB.MAWB_EXP_TBL_PK = HAWB.MAWB_EXP_TBL_FK";
            strSQL += "AND CMST.CUSTOMER_MST_PK(+)=HAWB.SHIPPER_CUST_MST_FK ";
            strSQL += "AND JAE.JOB_CARD_TRN_PK(+)=HAWB.JOB_CARD_AIR_EXP_FK ";
            strSQL += "AND JAE.BOOKING_MST_FK=BAT.BOOKING_MST_PK(+) ";
            strSQL += "AND POL.PORT_MST_PK(+)=BAT.PORT_MST_POL_FK";
            strSQL += "AND POD.PORT_MST_PK(+)=BAT.PORT_MST_POD_FK";
            strSQL += "AND AMST.AIRLINE_MST_PK(+)=BAT.CARRIER_MST_FK";
            strSQL += "AND  JAE.JOB_CARD_STATUS =1";
            strSQL += "and JAE.created_by_fk= umt.user_mst_pk";
            strSQL += "and umt.default_location_fk=" + Loc;
            strSQL += strCondition;
            strSQL += "ORDER BY HAWB.HAWB_DATE DESC, MAWB.MAWB_REF_NO DESC";
            strSQL += " ) q )WHERE SR_NO  Between " + start + " and " + last;

            DataSet DS = null;
            DS = objWF.GetDataSet(strSQL);
            try
            {
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

        #region "enhance search for MHAWB JOB REF NO"
        public string FetchForMAWBJobRef(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strReq = null;
            string businessType = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            businessType = Convert.ToString(arr.GetValue(2));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_MAWB_REF_NO_PKG.GET_JOB_MAWB_REF_COMMON";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        private object ifDateNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return Convert.ToDateTime(col);
            }
        }
        #endregion

        #region "Save Deliver To  in DOCS_PRINT_DTL_TBL"
        public bool UpdateDetails(object Uwg1, string ProtocolNo)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            OracleTransaction Tran = default(OracleTransaction);
            ObjWk.OpenConnection();
            Tran = ObjWk.MyConnection.BeginTransaction();
            //for (Int16 I = 0; I <= Uwg1.Rows.Count - 1; I++)
            //{
            //    if (Uwg1.Rows(I).Cells(Header.SEL).Value == "true")
            //    {
            //        var _with2 = ObjWk.MyCommand;

            //        _with2.CommandText = ObjWk.MyUserName + ".FILE_COVER_SHEET_PKG.DOCS_PRINT_DTL_TBL_INS";
            //        _with2.CommandType = CommandType.StoredProcedure;
            //        _with2.Transaction = Tran;
            //        _with2.Parameters.Clear();

            //        try
            //        {
            //            _with2.Parameters.Add("BUSINESS_TYPE_IN", 1).Direction = ParameterDirection.Input;
            //            _with2.Parameters.Add("MODE_IN", 1).Direction = ParameterDirection.Input;
            //            _with2.Parameters.Add("JOBCARD_REF_PK_IN", Convert.ToInt32(Uwg1.rows(I).cells(Header.JAEPK).value)).Direction = ParameterDirection.Input;
            //            _with2.Parameters.Add("PROTOCOL_NO_IN", ProtocolNo).Direction = ParameterDirection.Input;

            //            _with2.Parameters.Add("Deliver_To_IN", Uwg1.rows(I).cells(Header.DELIVERDATA).value).Direction = ParameterDirection.Input;
            //            _with2.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
            //            _with2.Parameters.Add("RETURN_VALUE", OracleType.Number, 10, "DOCS_PRINT_DTL_TBL_PK").Direction = ParameterDirection.Output;

            //            _with2.ExecuteNonQuery();

            //        }
            //        catch (OracleException Oraexp)
            //        {
            //            throw Oraexp;
            //        }
            //        catch (Exception ex)
            //        {
            //            Tran.Rollback();
            //            return false;
            //        }
            //    }
            //}
            Tran.Commit();
            return true;
        }
        #endregion

        #region "generate protocol for File Cover Sheet Air"
        public string GenerateAir_Ref_No(Int64 ILocationId, Int64 IEmployeeId, string sPOL)
        {
            string functionReturnValue = null;
            CREATED_BY = this.CREATED_BY;
            functionReturnValue = GenerateProtocolKey("EXPORT FILE COVER SHEET", ILocationId, IEmployeeId, DateTime.Now, "", "", "", CREATED_BY);
            return functionReturnValue;
        }
        #endregion

        #region "Fetch Report Details"
        public DataSet FetchFileCoverReportDetails(string HAWBPK)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = "SELECT MAWB.MAWB_EXP_TBL_PK MAWBPK,";
            Strsql += "MAWB.MAWB_REF_NO MAWBREFNO,";
            Strsql += "HAWB.HAWB_EXP_TBL_PK HAWBPK,";
            Strsql += "HAWB.HAWB_REF_NO HAWBREFNO,";
            Strsql += "JAE.JOB_CARD_TRN_PK JOBCARDPK,";
            Strsql += "HAWB.SHIPPER_CUST_MST_FK SHIPPK,";
            Strsql += "CMST.CUSTOMER_NAME SHIPNAME,";
            Strsql += "CDTLS.Adm_Contact_Person SHIPADD1,";
            Strsql += "CDTLS.ADM_ADDRESS_2 SHIPADD2,";
            Strsql += "CDTLS.ADM_ADDRESS_3 SHIPADD3,";
            Strsql += "CDTLS.ADM_CITY SHIPCITY,";
            Strsql += "CDTLS.ADM_ZIP_CODE SHIPZIP,";
            Strsql += "CMST.COUNTRY_NAME SHIPCOUNTRY,";
            Strsql += " CDTLS.ADM_PHONE_NO_1 SHIPPHONE,";
            Strsql += "CONSIGNEE.CUSTOMER_NAME CONSIGNEE,";
            Strsql += "CBAGENT.AGENT_NAME CBAGENT,";
            Strsql += "NVL(HAWB.CL_AGENT_MST_FK,0) CLAGENT,";
            Strsql += " HAWB.GOODS_DESCRIPTION GOODSDES,";
            Strsql += " STMST.INCO_CODE TERMS,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME,";
            Strsql += " HAWB.TOTAL_GROSS_WEIGHT GROSSWGT,";
            Strsql += " HAWB.TOTAL_VOLUME VOLUME,";
            Strsql += " HAWB.TOTAL_PACK_COUNT PIECES,";
            Strsql += " HAWB.FLIGHT_NO FLIGHT1,";
            Strsql += " HAWB.SEC_FLIGHT_NO FLIGHT2,";
            Strsql += " AIRLINE.AIRLINE_NAME AIRLINE,";
            Strsql += " HAWB.ETA_DATE ETA,";
            Strsql += " HAWB.ETD_DATE ETD,";
            Strsql += " BAT.COL_ADDRESS WAREHOUSE,";
            Strsql += "CMST.SECURITY_CHK_REQD X_RAY,";
            Strsql += "TMT.TRANSPORTER_NAME TRANSPORT_NAME,";
            Strsql += "FRT.FREIGHT_ELEMENT_NAME FRTNAME,";
            Strsql += "JTRAN.FREIGHT_TYPE FRTTYPE,";
            Strsql += "JTRAN.FREIGHT_AMT FRTAMT";
            Strsql += "FROM HAWB_EXP_TBL HAWB,";
            Strsql += " MAWB_EXP_TBL MAWB,";
            Strsql += " CUSTOMER_MST_TBL CMST,";
            Strsql += " CUSTOMER_CONTACT_DTLS CDTLS,";
            Strsql += " CUSTOMER_MST_TBL CONSIGNEE,";
            Strsql += " AGENT_MST_TBL CBAGENT,";
            Strsql += " SHIPPING_TERMS_MST_TBL STMST,";
            Strsql += " JOB_CARD_TRN JAE,";
            Strsql += " TRANSPORTER_MST_TBL TMT,";
            Strsql += " BOOKING_MST_TBL BAT,";
            Strsql += " PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD,";
            Strsql += " AIRLINE_MST_TBL AIRLINE,";
            Strsql += "COUNTRY_MST_TBL CMST,";
            Strsql += "JOB_TRN_FD JTRAN,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FRT";
            Strsql += "WHERE MAWB.MAWB_EXP_TBL_PK(+)=HAWB.MAWB_EXP_TBL_FK ";
            Strsql += " AND   JAE.TRANSPORTER_DEPOT_FK = TMT.TRANSPORTER_MST_PK(+)";
            Strsql += "AND FRT.FREIGHT_ELEMENT_MST_PK = JTRAN.FREIGHT_ELEMENT_MST_FK";
            Strsql += " AND  JAE.JOB_CARD_TRN_PK=JTRAN.JOB_CARD_TRN_FK";
            Strsql += "AND   CMST.CUSTOMER_MST_PK(+)=HAWB.SHIPPER_CUST_MST_FK ";
            Strsql += "AND   CMST.CUSTOMER_MST_PK=CDTLS.CUSTOMER_MST_FK(+) ";
            Strsql += "AND   CONSIGNEE.CUSTOMER_MST_PK(+)=HAWB.CONSIGNEE_CUST_MST_FK";
            Strsql += "AND    CBAGENT.AGENT_MST_PK(+)=HAWB.CB_AGENT_MST_FK";
            Strsql += "AND    MAWB.SHIPPING_TERMS_MST_FK=STMST.SHIPPING_TERMS_MST_PK(+)";
            Strsql += "AND    HAWB.JOB_CARD_AIR_EXP_FK=JAE.JOB_CARD_TRN_PK(+)";
            Strsql += "AND   JAE.BOOKING_MST_FK=BAT.BOOKING_MST_PK(+)";
            Strsql += "AND   BAT.PORT_MST_POL_FK=POL.PORT_MST_PK(+)";
            Strsql += "AND   BAT.PORT_MST_POD_FK=POD.PORT_MST_PK(+)";
            Strsql += "AND  AIRLINE.AIRLINE_MST_PK(+)=BAT.CARRIER_MST_FK";
            Strsql += "AND  CMST.COUNTRY_MST_PK(+)=CDTLS.ADM_COUNTRY_MST_FK";
            Strsql += "AND  HAWB.HAWB_EXP_TBL_PK IN (" + HAWBPK + ")";
            Strsql += " ORDER BY MAWB.MAWB_REF_NO DESC";
            DataSet DS = null;
            DS = ObjWk.GetDataSet(Strsql);
            try
            {
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
        public DataSet FetchFrtElementDetails(string HAWBPK, int currencypk = 0)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = "SELECT JAE.HBL_HAWB_FK HAWBFK, JAE.JOB_CARD_TRN_PK JOBFK,";
            Strsql += "FRT.FREIGHT_ELEMENT_NAME FRTNAME,";
            Strsql += "JTRAN.FREIGHT_TYPE FRTTYPE,";
            Strsql += "(JTRAN.FREIGHT_AMT  * (SELECT GET_EX_RATE( Jtran.CURRENCY_MST_FK," + currencypk + ",TO_DATE(jae.jobcard_date,DATEFORMAT )) FROM DUAL)) FRTAMT";
            Strsql += "FROM";
            Strsql += "JOB_CARD_TRN JAE,";
            Strsql += "JOB_TRN_FD JTRAN,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FRT";
            Strsql += "WHERE FRT.FREIGHT_ELEMENT_MST_PK = JTRAN.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND  JAE.JOB_CARD_TRN_PK=JTRAN.JOB_CARD_TRN_FK";
            Strsql += "AND  JAE.HBL_HAWB_FK IN (" + HAWBPK + ")";
            DataSet DS = null;
            DS = ObjWk.GetDataSet(Strsql);
            try
            {
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
        public DataSet FetchDimensionsDetails(string HAWBPK)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = "SELECT JAE.HBL_HAWB_FK HAWBFK,";
            Strsql += "JAE.JOB_CARD_TRN_PK JOBFK,";
            Strsql += "JAEC.PALETTE_SIZE";
            Strsql += "FROM JOB_TRN_CONT JAEC,";
            Strsql += "JOB_CARD_TRN JAE";
            Strsql += "WHERE JAE.JOB_CARD_TRN_PK = JAEC.JOB_CARD_TRN_FK";
            Strsql += "AND   JAE.HBL_HAWB_FK IN (" + HAWBPK + ") ";
            DataSet DS = null;
            DS = ObjWk.GetDataSet(Strsql);
            try
            {
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

    }
}
