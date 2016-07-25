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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    public class cls_HBL_List : CommonFeatures
    {

        #region " Fetch "
        public DataSet FetchAll(System.DateTime CutOffdate, string HBLRefNo = "", string Shipperid = "", short CargoType = 1, string POLID = "", string PODID = "", string POLname = "", string PODname = "", string HBLdate = "", string sOperator = "",
        string Vessel = "", string Voyage = "", string Consineeid = "", string Status = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", long lngUsrLocFk = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder strBuildCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strBuildCondition.Append(" AND 1=2 ");
            }
            if (HBLRefNo.Length > 0)
            {
                strBuildCondition.Append(" AND UPPER(HBL.HBL_REF_NO) LIKE '%" + HBLRefNo.ToUpper().Replace("'", "''") + "%'  ");
            }

            if (Shipperid.Length > 0)
            {
                strBuildCondition.Append(" AND CUST.CUSTOMER_ID LIKE '%" + Shipperid.ToUpper().Replace("'", "''") + "%'  ");
            }

            if (Consineeid.Length > 0)
            {
                strBuildCondition.Append(" AND CONS.CUSTOMER_ID LIKE '%" + Consineeid.ToUpper().Replace("'", "''") + "%'  ");
            }

            if (HBLdate.Length > 0)
            {
                strBuildCondition.Append(" AND HBL.HBL_DATE = TO_DATE('" + HBLdate + "','" + dateFormat + "')   ");
            }
            if (!(CutOffdate == null))
            {
                strBuildCondition.Append(" AND VVT.POL_CUT_OFF_DATE >= TO_DATE('" + CutOffdate + "','& dd/MM/yyyy  HH24:MI:SS &')   ");
            }

            if (sOperator.Length > 0)
            {
                strBuildCondition.Append(" AND OPR.OPERATOR_ID LIKE '%" + sOperator.ToUpper().Replace("'", "''") + "%'  ");
            }

            if (Vessel.Length > 0)
            {
                strBuildCondition.Append(" AND UPPER(HBL.VESSEL_NAME) LIKE '%" + Vessel.ToUpper().Replace("'", "''") + "%'   ");
            }

            if (Voyage.Length > 0)
            {
                strBuildCondition.Append(" AND UPPER(VVT.VOYAGE) LIKE '%" + Voyage.ToUpper().Replace("'", "''") + "%'   ");
            }

            if (Convert.ToInt32(Status) != 4)
            {
                if (Status.Length > 0)
                {
                    strBuildCondition.Append(" AND HBL.HBL_STATUS =" + Status + "");
                }
            }

            if (PODID.Length > 0)
            {
                strBuildCondition.Append(" AND BOOK.PORT_MST_POD_FK IN");
                strBuildCondition.Append(" (SELECT P.PORT_MST_PK FROM PORT_MST_TBL P ");
                strBuildCondition.Append(" WHERE P.PORT_ID LIKE '%" + PODID.ToUpper().Replace("'", "''") + "%')");
            }

            if (POLID.Length > 0)
            {
                strBuildCondition.Append(" AND BOOK.PORT_MST_POL_FK IN");
                strBuildCondition.Append(" (SELECT P.PORT_MST_PK FROM PORT_MST_TBL P ");
                strBuildCondition.Append(" WHERE P.PORT_ID LIKE '%" + POLID.ToUpper().Replace("'", "''") + "%')");
            }
            if (CargoType > 0)
            {
                strBuildCondition.Append(" AND BOOK.CARGO_TYPE = " + CargoType + "");
            }
            strBuildCondition.Append( " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
            strBuildCondition.Append( " AND HBL.CREATED_BY_FK = UMT.USER_MST_PK ");
            strBuildCondition.AppendLine(" AND JOB.BUSINESS_TYPE=2 ");
            strBuildCondition.AppendLine(" AND (HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK OR HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK OR HBL.HBL_EXP_TBL_PK=JOB.HBL_HAWB_FK) ");

            StrBuilder.Append(" SELECT COUNT(*) FROM ( ");
            StrBuilder.Append("  SELECT HBL.HBL_EXP_TBL_PK FROM ");
            StrBuilder.Append("  HBL_EXP_TBL HBL,");
            StrBuilder.Append("  JOB_CARD_TRN JOB, ");
            StrBuilder.Append("  CUSTOMER_MST_TBL CUST, ");
            StrBuilder.Append(" CUSTOMER_MST_TBL     CONS,");
            //'
            StrBuilder.Append(" BOOKING_MST_TBL BOOK, ");
            StrBuilder.Append(" PORT_MST_TBL PO,");
            StrBuilder.Append(" OPERATOR_MST_TBL OPR,USER_MST_TBL UMT, ");
            StrBuilder.Append(" PORT_MST_TBL PO1, VESSEL_VOYAGE_TBL V, VESSEL_VOYAGE_TRN VVT,TEMP_CUSTOMER_TBL TEMPCUST ");
            StrBuilder.Append(" WHERE ");
            StrBuilder.Append(" OPR.OPERATOR_MST_PK  = BOOK.CARRIER_MST_FK ");
            StrBuilder.Append(" AND    CUST.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK");
            StrBuilder.Append(" AND CONS.CUSTOMER_MST_PK(+) = JOB.CONSIGNEE_CUST_MST_FK ");
            StrBuilder.Append(" AND TEMPCUST.CUSTOMER_MST_PK(+) = JOB.CONSIGNEE_CUST_MST_FK ");
            StrBuilder.Append(" AND    JOB.BOOKING_MST_FK   = BOOK.BOOKING_MST_PK");
            StrBuilder.Append(" AND    BOOK.PORT_MST_POL_FK = PO.PORT_MST_PK ");
            StrBuilder.Append(" AND    BOOK.PORT_MST_POD_FK = PO1.PORT_MST_PK  ");
            StrBuilder.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK (+) ");
            StrBuilder.Append(" AND HBL.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK (+) ");
            StrBuilder.Append(strBuildCondition.ToString());
            //StrBuilder.Append("  UNION ")
            //StrBuilder.Append("  SELECT DISTINCT HBL.HBL_EXP_TBL_PK FROM ")
            //StrBuilder.Append("  HBL_EXP_TBL HBL,")
            //StrBuilder.Append(" JOB_CARD_TRN JOB, ")
            //StrBuilder.Append(" CUSTOMER_MST_TBL CUST, ")
            //StrBuilder.Append(" CUSTOMER_MST_TBL     CONS,")
            //StrBuilder.Append(" BOOKING_MST_TBL BOOK, ")
            //StrBuilder.Append(" PORT_MST_TBL PO,")
            //StrBuilder.Append(" OPERATOR_MST_TBL OPR,USER_MST_TBL UMT, ")
            //StrBuilder.Append(" PORT_MST_TBL PO1, VESSEL_VOYAGE_TBL V, VESSEL_VOYAGE_TRN VVT ")
            //StrBuilder.Append(" WHERE ")
            //StrBuilder.Append(" OPR.OPERATOR_MST_PK  = BOOK.CARRIER_MST_FK ")
            //StrBuilder.Append(" AND    CUST.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK")
            //StrBuilder.Append("  AND CONS.CUSTOMER_MST_PK = JOB.CONSIGNEE_CUST_MST_FK ")
            //StrBuilder.Append(" AND    JOB.BOOKING_MST_FK   = BOOK.BOOKING_MST_PK")
            //StrBuilder.Append(" AND    BOOK.PORT_MST_POL_FK = PO.PORT_MST_PK ")
            //StrBuilder.Append(" AND    BOOK.PORT_MST_POD_FK = PO1.PORT_MST_PK  ")
            //StrBuilder.Append(" AND    HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK ")
            //StrBuilder.Append(strBuildCondition.ToString)
            //StrBuilder.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK (+) ")
            //StrBuilder.Append(" AND HBL.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK(+) ")
            StrBuilder.AppendLine(" ) ");
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrBuilder.ToString()));
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

            StrBuilder = new System.Text.StringBuilder();

            StrBuilder.Append(" SELECT * FROM (");
            StrBuilder.Append(" SELECT ROWNUM SR_NO,q.* FROM (  ");
            StrBuilder.Append(" SELECT HBL.HBL_EXP_TBL_PK, ");
            StrBuilder.Append(" HBL.HBL_REF_NO, ");
            StrBuilder.Append(" HBL.HBL_DATE, ");
            StrBuilder.Append(" CUST.CUSTOMER_ID, ");
            StrBuilder.Append(" NVL(CONS.CUSTOMER_ID,TEMPCUST.CUSTOMER_ID) AS CONSIGNEE,");
            //'
            StrBuilder.Append(" PO.PORT_ID AS POL,PO1.PORT_ID AS POD, ");
            StrBuilder.Append(" OPR.OPERATOR_ID,");
            StrBuilder.Append(" V.VESSEL_NAME,");
            StrBuilder.Append(" VVT.VOYAGE,");
            //'
            StrBuilder.Append(" HBL.DEPARTURE_DATE ATD_POL,");
            StrBuilder.Append(" VVT.POL_CUT_OFF_DATE,");
            StrBuilder.Append(" VVT.POL_ETD,");
            //'
            StrBuilder.Append(" DECODE(BOOK.CARGO_TYPE, '1','FCL','2','LCL','4','BBC' ) CARGO_TYPE, ");
            StrBuilder.Append(" DECODE(HBL.HBL_STATUS, '0','Draft','1','Confirmed','2','Released','3','Cancelled','4','All' ) STATUS,'' SEL");
            StrBuilder.Append(" FROM HBL_EXP_TBL HBL,");
            StrBuilder.Append(" JOB_CARD_TRN JOB, ");
            StrBuilder.Append(" CUSTOMER_MST_TBL CUST,");
            StrBuilder.Append(" CUSTOMER_MST_TBL     CONS,");
            StrBuilder.Append(" BOOKING_MST_TBL BOOK, ");
            StrBuilder.Append(" PORT_MST_TBL PO, ");
            StrBuilder.Append(" OPERATOR_MST_TBL OPR,");
            StrBuilder.Append(" PORT_MST_TBL PO1,USER_MST_TBL UMT,");
            StrBuilder.Append(" VESSEL_VOYAGE_TBL V,");
            StrBuilder.Append(" VESSEL_VOYAGE_TRN VVT,TEMP_CUSTOMER_TBL TEMPCUST ");
            StrBuilder.Append(" WHERE ");
            StrBuilder.Append(" OPR.OPERATOR_MST_PK       =  BOOK.CARRIER_MST_FK ");
            StrBuilder.Append(" AND CUST.CUSTOMER_MST_PK  =  JOB.SHIPPER_CUST_MST_FK ");
            StrBuilder.Append(" AND CONS.CUSTOMER_MST_PK(+) = JOB.CONSIGNEE_CUST_MST_FK ");
            StrBuilder.Append(" AND TEMPCUST.CUSTOMER_MST_PK(+) = JOB.CONSIGNEE_CUST_MST_FK ");
            StrBuilder.Append(" AND JOB.BOOKING_MST_FK    =  BOOK.BOOKING_MST_PK ");
            StrBuilder.Append(" AND BOOK.PORT_MST_POL_FK  =  PO.PORT_MST_PK");
            StrBuilder.Append(" AND BOOK.PORT_MST_POD_FK  =  PO1.PORT_MST_PK");
            StrBuilder.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK (+) ");
            StrBuilder.Append(" AND HBL.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK (+) ");
            StrBuilder.Append(strBuildCondition.ToString());
            //StrBuilder.Append(" UNION ")
            //StrBuilder.Append(" SELECT DISTINCT HBL.HBL_EXP_TBL_PK, ")
            //StrBuilder.Append(" HBL.HBL_REF_NO, ")
            //StrBuilder.Append(" HBL.HBL_DATE, ")
            //StrBuilder.Append(" CUST.CUSTOMER_ID, ")
            //StrBuilder.Append("  CONS.CUSTOMER_ID     AS CONSIGNEE,") ''
            //StrBuilder.Append(" PO.PORT_ID AS POL,PO1.PORT_ID AS POD, ")
            //StrBuilder.Append(" OPR.OPERATOR_ID,")

            //StrBuilder.Append(" V.VESSEL_NAME,")
            //StrBuilder.Append("  VVT.VOYAGE,") ''
            //StrBuilder.Append("  VVT.ATD_POL,")
            //StrBuilder.Append("  VVT.POL_CUT_OFF_DATE,")
            //StrBuilder.Append("  VVT.POL_ETD,") ''
            //StrBuilder.Append(" DECODE(BOOK.CARGO_TYPE, '1','FCL','2','LCL','4','BBC' ) CARGO_TYPE, ")
            //StrBuilder.Append(" DECODE(HBL.HBL_STATUS, '0','Draft','1','Confirmed','2','Released','3','Cancelled','4','All' ) STATUS,'' SEL")
            //StrBuilder.Append(" FROM HBL_EXP_TBL HBL,")
            //StrBuilder.Append(" JOB_CARD_TRN JOB, ")
            //StrBuilder.Append(" CUSTOMER_MST_TBL CUST,")
            //StrBuilder.Append(" CUSTOMER_MST_TBL     CONS,")
            //StrBuilder.Append(" BOOKING_MST_TBL BOOK, ")
            //StrBuilder.Append(" PORT_MST_TBL PO, ")
            //StrBuilder.Append(" OPERATOR_MST_TBL OPR,")
            //StrBuilder.Append(" PORT_MST_TBL PO1,USER_MST_TBL UMT,")
            //StrBuilder.Append(" VESSEL_VOYAGE_TBL V,")
            //StrBuilder.Append(" VESSEL_VOYAGE_TRN VVT ")
            //StrBuilder.Append(" WHERE ")
            //StrBuilder.Append(" OPR.OPERATOR_MST_PK       =  BOOK.CARRIER_MST_FK ")
            //StrBuilder.Append(" AND CUST.CUSTOMER_MST_PK  =  JOB.SHIPPER_CUST_MST_FK ")
            //StrBuilder.Append("  AND CONS.CUSTOMER_MST_PK = JOB.CONSIGNEE_CUST_MST_FK ")
            //StrBuilder.Append(" AND JOB.BOOKING_MST_FK    =  BOOK.BOOKING_MST_PK ")
            //StrBuilder.Append(" AND BOOK.PORT_MST_POL_FK  =  PO.PORT_MST_PK")
            //StrBuilder.Append(" AND BOOK.PORT_MST_POD_FK  =  PO1.PORT_MST_PK")
            //StrBuilder.Append(" AND HBL.NEW_JOB_CARD_SEA_EXP_FK=JOB.JOB_CARD_TRN_PK ")
            //StrBuilder.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK (+) ")
            //StrBuilder.Append(" AND HBL.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK (+) ")
            //StrBuilder.Append(strBuildCondition.ToString)
            //StrBuilder.Append("  ORDER BY " & SortColumn & SortType & " ,HBL_REF_NO DESC ")
            StrBuilder.Append("  ORDER BY HBL_DATE DESC,HBL_REF_NO");
            StrBuilder.Append("  ) q  ) WHERE SR_NO  Between " + start + " and " + last + "");

            DataSet DS = null;
            DS = objWF.GetDataSet(StrBuilder.ToString());
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

        #region " Fetch Export Header Details"
        public DataSet FetchSeaExpHeaderDocment()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "select JobSeaExp.Job_Card_Sea_Exp_Pk,";
            strSQL +=  "JobSeaExp.Jobcard_Ref_No,";
            strSQL +=  "HblExp.Hbl_Exp_Tbl_Pk,";
            strSQL +=  "HblExp.Hbl_Ref_No,";
            strSQL +=  "HblExp.Vessel_Name,";
            strSQL +=  "CustMstShipper.Customer_Name Shipper,";
            strSQL +=  "CustShipperDtls.Adm_Address_1 ShiAddress1,";
            strSQL +=  "CustShipperDtls.Adm_Address_2 ShiAddress2,";
            strSQL +=  "CustShipperDtls.Adm_Address_3 ShiAddress3,";
            strSQL +=  "CustShipperDtls.Adm_City ShiCity,";
            strSQL +=  "CustMstConsignee.Customer_Name Consignee,";
            strSQL +=  "CustConsigneeDtls.Adm_Address_1 ConsiAddress1,";
            strSQL +=  "CustConsigneeDtls.Adm_Address_2 ConsiAddress2,";
            strSQL +=  "CustConsigneeDtls.Adm_Address_3 ConsiAddress3,";
            strSQL +=  "CustConsigneeDtls.Adm_City ConsiCity,";
            strSQL +=  "AgentMst.Agent_Name,";
            strSQL +=  "AgentDtls.Adm_Address_1 AgtAddress1,";
            strSQL +=  "AgentDtls.Adm_Address_2 AgtAddress2,";
            strSQL +=  "AgentDtls.Adm_Address_3 AgtAddress3,";
            strSQL +=  "AgentDtls.Adm_City AgtCity,";
            strSQL +=  "POL.PORT_NAME POL,";
            strSQL +=  "POD.PORT_NAME POD,";
            strSQL +=  "PMT.PLACE_NAME PLD,";
            strSQL +=  "HblExp.Goods_Description";
            strSQL +=  "from Hbl_Exp_Tbl HblExp,";
            strSQL +=  "Job_Card_Sea_Exp_Tbl JobSeaExp,";
            strSQL +=  "Customer_Mst_Tbl CustMstShipper,";
            strSQL +=  "Customer_Mst_Tbl CustMstConsignee,";
            strSQL +=  "Agent_Mst_Tbl AgentMst,";
            strSQL +=  "Booking_Sea_Tbl BkgSea,";
            strSQL +=  "Port_Mst_Tbl POL,";
            strSQL +=  "Port_Mst_Tbl POD,";
            strSQL +=  "Place_Mst_Tbl PMT,";
            strSQL +=  "Customer_Contact_Dtls CustShipperDtls,";
            strSQL +=  "Customer_Contact_Dtls CustConsigneeDtls,";
            strSQL +=  "Agent_Contact_Dtls AgentDtls";
            strSQL +=  "where JobSeaExp.Job_Card_Sea_Exp_Pk = HblExp.Job_Card_Sea_Exp_Fk";
            strSQL +=  "and POL.PORT_MST_PK=BkgSea.Port_Mst_Pol_Fk";
            strSQL +=  "and POD.PORT_MST_PK=BkgSea.Port_Mst_Pod_Fk";
            strSQL +=  "and PMT.PLACE_PK=BkgSea.Del_Place_Mst_Fk";
            strSQL +=  "and HblExp.Shipper_Cust_Mst_Fk=CustMstShipper.Customer_Mst_Pk";
            strSQL +=  "and HblExp.Consignee_Cust_Mst_Fk=CustMstConsignee.Customer_Mst_Pk";
            strSQL +=  "and CustMstShipper.Customer_Mst_Pk=CustShipperDtls.Customer_Mst_Fk";
            strSQL +=  "and CustMstConsignee.Customer_Mst_Pk=CustConsigneeDtls.Customer_Mst_Fk";
            strSQL +=  "and AgentMst.Agent_Mst_Pk=AgentDtls.Agent_Mst_Fk";
            strSQL +=  "and HblExp.Hbl_Exp_Tbl_Pk=1";
            try
            {
                return (objWF.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Procedure Calls (Enhance Search) "
        public string FetchHBLForJobCard(string strCond, string Loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strcustpk = "";
            string AGENT = "";
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strcustpk = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                AGENT = Convert.ToString(arr.GetValue(5));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_PKG.GET_HBL_REF_JOBCARD";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with1.Add("JOB_CARD_PK_IN", getDefault(strJobCardPK, 0)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", Loc).Direction = ParameterDirection.Input;
                _with1.Add("CUSTUMER_PK", (string.IsNullOrEmpty(strcustpk) ? "" : strcustpk)).Direction = ParameterDirection.Input;
                _with1.Add("AGENT_TYPE_IN", (string.IsNullOrEmpty(AGENT) ? "" : AGENT)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 8000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
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
        //' ''for fetching HBL Ref.no for Invoice to CB agent Sea(Exp)
        public string FetchHBLForInv(string strCond, string Loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strcustpk = "";
            string AGENT = "";
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strcustpk = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                AGENT = Convert.ToString(arr.GetValue(5));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_PKG.GET_HBL_REF_INV";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with2.Add("JOB_CARD_PK_IN", getDefault(strJobCardPK, 0)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_IN", Loc).Direction = ParameterDirection.Input;
                _with2.Add("CUSTUMER_PK", (string.IsNullOrEmpty(strcustpk) ? "" : strcustpk)).Direction = ParameterDirection.Input;
                _with2.Add("AGENT_TYPE_IN", (string.IsNullOrEmpty(AGENT) ? "" : AGENT)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 8000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
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
        public string FetchHBLForJobCards(string strCond, string Loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strcustpk = "";
            string AGENT = "";
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strcustpk = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                AGENT = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                Loc = Convert.ToString(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_PKG.GET_HBL_REF_JOBCARDS";
                var _with3 = SCM.Parameters;
                _with3.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with3.Add("JOB_CARD_PK_IN", getDefault(strJobCardPK, 0)).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_IN", (string.IsNullOrEmpty(Loc) ? "" : Loc)).Direction = ParameterDirection.Input;
                _with3.Add("CUSTUMER_PK", (string.IsNullOrEmpty(strcustpk) ? "" : strcustpk)).Direction = ParameterDirection.Input;
                _with3.Add("AGENT_TYPE_IN", (string.IsNullOrEmpty(AGENT) ? "" : AGENT)).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
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
        public string FetchHBLForJobCardImp(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            string AGENT = "";
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                AGENT = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_PKG.GET_HBL_REF_JOBCARD_IMP";
                var _with4 = SCM.Parameters;
                _with4.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with4.Add("JOB_CARD_PK_IN", getDefault(strJobCardPK, 0)).Direction = ParameterDirection.Input;
                _with4.Add("AGENT_TYPE_IN", (string.IsNullOrEmpty(AGENT) ? "" : AGENT)).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
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

        public string FetchHBLREFNo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_PKG.GET_HBL_REF_JOBCARD";
                var _with5 = SCM.Parameters;
                _with5.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with5.Add("JOB_CARD_PK_IN", strJobCardPK).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
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

        #region " HBL Printing"
        public DataSet FetchPackages(string HBLPk, string From = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "  SELECT H.HBL_EXP_TBL_PK,  COUNT(CY.CONTAINER_TYPE_MST_ID) || ' X ' || CY.CONTAINER_TYPE_MST_ID AS pack" ;
            Strsql += " FROM JOB_TRN_CONT JC, CONTAINER_TYPE_MST_TBL CY,HBL_EXP_TBL H " ;
            Strsql += " WHERE CY.CONTAINER_TYPE_MST_PK=JC.CONTAINER_TYPE_MST_FK" ;
            Strsql += " AND (H.JOB_CARD_SEA_EXP_FK=JC.JOB_CARD_TRN_FK or H.NEW_JOB_CARD_SEA_EXP_FK = JC.JOB_CARD_TRN_FK) " ;
            //Added by rabbani passing multible HBLPKs
            if (From == "MSTSEA")
            {
                Strsql += " AND H.HBL_EXP_TBL_PK IN (" + HBLPk + ")" ;
            }
            else
            {
                Strsql += " AND H.HBL_EXP_TBL_PK=" + HBLPk ;
            }
            Strsql += " GROUP BY JC.CONTAINER_TYPE_MST_FK, CY.CONTAINER_TYPE_MST_ID,H.HBL_EXP_TBL_PK" ;
            try
            {
                return Objwk.GetDataSet(Strsql);

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        public DataSet FetchLoadDate(string HblPk, string From = "")
        {
            StringBuilder Strsql = new StringBuilder();
            WorkFlow Objwk = new WorkFlow();
            Strsql.Append(" Select  max(JC.Load_Date) As LoadDt ");
            Strsql.Append(" FROM JOB_TRN_CONT JC,HBL_EXP_TBL H ");
            Strsql.Append("WHERE (H.JOB_CARD_SEA_EXP_FK=JC.JOB_CARD_TRN_FK or H.NEW_JOB_CARD_SEA_EXP_FK = JC.JOB_CARD_TRN_FK) ");
            if (From == "MSTSEA")
            {
                Strsql.Append(" AND H.HBL_EXP_TBL_PK IN (" + HblPk + ") ");
            }
            else
            {
                Strsql.Append(" AND H.HBL_EXP_TBL_PK= " + HblPk + " ");
            }

            try
            {
                return Objwk.GetDataSet(Strsql.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchBlClauses(string HBLPk, string From = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT " ;
            Strsql += " HBL.BL_DESCRIPTION" ;
            Strsql += " FROM" ;
            Strsql += " HBL_BL_CLAUSE_TBL HBL" ;
            Strsql += " WHERE" ;
            if (From == "MSTSEA")
            {
                Strsql += " HBL.HBL_EXP_TBL_FK IN (" + HBLPk + ")" ;
            }
            else
            {
                Strsql += " HBL.HBL_EXP_TBL_FK =" + HBLPk ;
            }

            try
            {
                return Objwk.GetDataSet(Strsql);

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }
        //adding by thiyagarajan on 20/4/09 : to implement letter of credit in HBL report
        public string FetchCredit(string hblpk, string From = "")
        {
            string strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                strsql += "SELECT  HBL.LETTER_OF_CREDIT  FROM HBL_EXP_TBL HBL WHERE HBL.HBL_EXP_TBL_PK IN (" + hblpk + ")" ;
                return Objwk.ExecuteScaler(strsql);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //In the below function Logged in Location parameter Added by Sivachandran.
        public DataSet FetchMainHBL(string HBLPk, string From = "", long LogedInLoc = 0)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT H.HBL_EXP_TBL_PK,JS.DEPARTURE_DATE HBLDATE,BS.BOOKING_REF_NO,AGT.ACCOUNT_NO,JS.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_PK,SHP.CUSTOMER_NAME AS SHIPPER," ;
            Strsql += " SHPDET.ADM_ADDRESS_1 as SHIPPERADD, SHPDET.ADM_ADDRESS_2 as shadd2,SHPDET.ADM_ADDRESS_3 as shadd3,shpdet.adm_city as shcity ,shpdet.adm_zip_code as shzip," ;
            Strsql += " (case when H.IS_TO_ORDER = '0' then CON.customer_name else  'To Order' end) as CONSIGNEE," ;
            Strsql += " (case when H.IS_TO_ORDER = '0' then CONDET.ADM_ADDRESS_1 else h.consignee_address end) as CONSIGNEEADD," ;
            Strsql += " (CASE";
            Strsql += "         WHEN H.IS_TO_ORDER = '0' THEN";
            Strsql += "         CONDET.ADM_ADDRESS_2";
            Strsql += "        ELSE";
            Strsql += "          ''";
            Strsql += "      END) ADM_ADDRESS_2,";
            Strsql += "       (CASE";
            Strsql += "        WHEN H.IS_TO_ORDER = '0' THEN";
            Strsql += "          CONDET.ADM_ADDRESS_3";
            Strsql += "         ELSE";
            Strsql += "        ''";
            Strsql += "       END) ADM_ADDRESS_3,";
            Strsql += "       (CASE";
            Strsql += "        WHEN H.IS_TO_ORDER = '0' THEN";
            Strsql += "          CONDET.ADM_CITY";
            Strsql += "        ELSE";
            Strsql += "         ''";
            Strsql += "       END) ADM_CITY,";
            Strsql += "       (CASE";
            Strsql += "         WHEN H.IS_TO_ORDER = '0' THEN";
            Strsql += "         CONDET.ADM_ZIP_CODE";
            Strsql += "         ELSE";
            Strsql += "          ''";
            Strsql += "     END) ADM_ZIP_CODE,";
            Strsql += "     (case when NVL(H.SAC_N1,0) = 0 then NOTI.CUSTOMER_NAME else 'Same As Consignee' end) as NOTIFY," ;
            //  Strsql &= " NOTI.CUSTOMER_NAME AS NOTIFY," & vbCrLf
            Strsql += "notdet.adm_address_1 as NOTIFYADD,notdet.adm_address_2 as nadd2,notdet.adm_address_3 as nadd3,notdet.adm_city as ncity,notdet.adm_zip_code as nzip," ;
            Strsql += " UPPER((H.VESSEL_NAME || '-' || H.VOYAGE)) AS VSLVOY, nvl(H.HBL_ORIGINAL_PRINTS,0) HBL_ORIGINAL_PRINTS,UPPER(PL.PORT_ID || ', ' || H.POL_COUNTRY) AS POL,UPPER(PD.PORT_ID || ', ' || H.POD_COUNTRY) AS POD," ;
            Strsql += " (CASE WHEN PREC.PLACE_NAME IS NOT NULL THEN " ;
            Strsql += "   UPPER(PREC.PLACE_CODE || ', ' || H.PLR_COUNTRY) ELSE" ;
            //Strsql &= "  UPPER(PL.PORT_ID || ', ' || H.PLR_COUNTRY) END) PRECEIPT," & vbCrLf
            Strsql += "  UPPER(PL.PORT_ID || ', ' || H.POL_COUNTRY) END) PRECEIPT," ;
            Strsql += " (CASE WHEN PDEL.PLACE_NAME IS NOT NULL THEN" ;
            Strsql += "  UPPER(PDEL.PLACE_CODE || ', ' || H.PFD_COUNTRY) ELSE" ;
            //Strsql &= "  UPPER(PD.PORT_ID || ', ' || H.PFD_COUNTRY) END) PDELIVERY," & vbCrLf
            Strsql += "  UPPER(PD.PORT_ID || ', ' || H.POD_COUNTRY) END) PDELIVERY," ;
            Strsql += " BS.PYMT_TYPE," ;
            Strsql += " (CASE WHEN BS.PYMT_TYPE = 1 THEN " ;
            Strsql += "  'ORIGIN'" ;
            Strsql += "  ELSE 'DESTINATION'" ;
            Strsql += "  END)FREIGHTPAYABLE," ;
            Strsql += "  HBLBL.BL_DESCRIPTION, h.hbl_status as status," ;
            ///End
            Strsql += " NVL((SELECT SUM(F.FREIGHT_AMT) FROM JOB_TRN_FD F WHERE F.JOB_CARD_TRN_FK=JS.JOB_CARD_TRN_PK),0)FRGHTAMT," ;
            //Freight Amount - sivachandran GAP-VEK-020
            Strsql += " NVL((SELECT SUM(JOF.AMOUNT) FROM JOB_TRN_OTH_CHRG JOF WHERE JS.JOB_CARD_TRN_PK=JOF.JOB_CARD_TRN_FK),0)OTHRFRGHTAMT," ;
            //Freight Amount - sivachandran GAP-VEK-020
            Strsql += " OPR.OPERATOR_NAME,OPRCT.ADM_ADDRESS_1,OPRCT.ADM_ADDRESS_2,OPRCT.ADM_ADDRESS_3,OPRCT.ADM_CITY," ;
            //Freight Amount - sivachandran GAP-VEK-020
            Strsql += " OPRCT.ADM_ZIP_CODE,OPRCT.ADM_PHONE_NO_1,OPRCT.ADM_FAX_NO,CNT.COUNTRY_NAME, NVL(H.PLACE_ISSUE, (SELECT L.LOCATION_NAME FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK = " + LogedInLoc + ")) PLACE_ISSUE " ;
            //Freight Amount - sivachandran GAP-VEK-020
            Strsql += " FROM HBL_EXP_TBL H,CUSTOMER_MST_TBL SHP,CUSTOMER_CONTACT_DTLS SHPDET,CUSTOMER_MST_TBL CON," ;
            Strsql += " CUSTOMER_CONTACT_DTLS CONDET,CUSTOMER_MST_TBL NOTI,CUSTOMER_CONTACT_DTLS NOTDET," ;
            Strsql += " BOOKING_MST_TBL BS,JOB_CARD_TRN JS,PORT_MST_TBL PL, HBL_BL_CLAUSE_TBL HBLBL,PORT_MST_TBL PD,PLACE_MST_TBL PREC,PLACE_MST_TBL PDEL," ;
            Strsql += " OPERATOR_MST_TBL OPR,OPERATOR_CONTACT_DTLS OPRCT, AGENT_MST_TBL AGT,COUNTRY_MST_TBL CNT" ;
            //Added by sivachandran GAP-VEK-020
            Strsql += " WHERE  H.SHIPPER_CUST_MST_FK(+)=SHP.CUSTOMER_MST_PK" ;
            Strsql += " AND SHPDET.CUSTOMER_MST_FK=SHP.CUSTOMER_MST_PK(+)" ;
            if (From == "MSTSEA")
            {
                Strsql += " AND H.HBL_EXP_TBL_PK IN (" + HBLPk + ")" ;
            }
            else
            {
                Strsql += " AND H.HBL_EXP_TBL_PK=" + HBLPk ;
            }
            Strsql += " AND H.CONSIGNEE_CUST_MST_FK=CON.CUSTOMER_MST_PK(+)" ;
            Strsql += " AND CONDET.CUSTOMER_MST_FK(+)=CON.CUSTOMER_MST_PK" ;
            Strsql += " AND NOTI.CUSTOMER_MST_PK(+)=H.NOTIFY1_CUST_MST_FK" ;
            Strsql += " AND NOTI.CUSTOMER_MST_PK=NOTDET.CUSTOMER_MST_FK(+)" ;
            Strsql += " AND BS.BOOKING_MST_PK=JS.BOOKING_MST_FK" ;
            Strsql += " AND (H.JOB_CARD_SEA_EXP_FK=JS.JOB_CARD_TRN_PK OR H.NEW_JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK)" ;
            Strsql += " AND PL.PORT_MST_PK=BS.PORT_MST_POL_FK" ;
            Strsql += " AND PD.PORT_MST_PK=BS.PORT_MST_POD_FK" ;
            Strsql += " AND PREC.PLACE_PK(+)=BS.COL_PLACE_MST_FK" ;
            Strsql += " AND PDEL.PLACE_PK(+)=BS.DEL_PLACE_MST_FK" ;
            Strsql += " AND HBLBL.HBL_EXP_TBL_FK(+) = H.HBL_EXP_TBL_PK" ;
            Strsql += " AND CNT.COUNTRY_MST_PK=OPRCT.ADM_COUNTRY_MST_FK" ;
            Strsql += " AND HBLBL.HBL_EXP_TBL_FK(+) = H.HBL_EXP_TBL_PK" ;
            Strsql += " AND OPR.OPERATOR_MST_PK(+)=BS.CARRIER_MST_FK ";
            Strsql += " AND OPRCT.OPERATOR_MST_FK=OPR.OPERATOR_MST_PK ";
            Strsql += " AND JS.DP_AGENT_MST_FK  = AGT.AGENT_MST_PK(+) ";
            try
            {
                return Objwk.GetDataSet(Strsql);

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }
        public DataSet FetchHBLDetails(string HBLPk, string From = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT H.HBL_EXP_TBL_PK,H.HBL_REF_NO,J.JOB_CARD_TRN_FK JOB_CARD_SEA_EXP_FK ,H.MARKS_NUMBERS AS Marks," ;
            Strsql += " H.GOODS_DESCRIPTION  AS GDesc,J.CONTAINER_NUMBER, CTYPE.CONTAINER_TYPE_MST_ID, J.SEAL_NUMBER, " ;
            Strsql += "  J.PACK_COUNT,J.GROSS_WEIGHT,(CTYPE.CONTAINER_TAREWEIGHT_TONE * 1000) TAREWEIGHT, " ;
            Strsql += "   H.TOTAL_GROSS_WEIGHT AS GrossWt,H.TOTAL_VOLUME AS Volume,H.VESSEL_NAME || '/' || H.VOYAGE VSLVOY" ;
            Strsql += " FROM HBL_EXP_TBL H,JOB_TRN_CONT J,JOB_CARD_TRN JS,CONTAINER_TYPE_MST_TBL CTYPE" ;
            Strsql += " WHERE (H.JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK OR H.NEW_JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK) " ;
            Strsql += " AND JS.JOB_CARD_TRN_PK=J.JOB_CARD_TRN_FK" ;
            Strsql += " AND CTYPE.CONTAINER_TYPE_MST_PK(+)=J.CONTAINER_TYPE_MST_FK" ;
            if (From == "MSTSEA")
            {
                Strsql += " AND H.HBL_EXP_TBL_PK IN (" + HBLPk + ")" ;
            }
            else
            {
                Strsql += " AND H.HBL_EXP_TBL_PK=" + HBLPk ;
            }
            try
            {
                return Objwk.GetDataSet(Strsql);

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }
        public DataSet FetchHBLBIFA(string HBLPk, string From = "")
        {
            //'ADDED BY MINAKSHI ON 23-FEB-09 FOR BIFA AND FIATA FORMAT
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT H.HBL_EXP_TBL_PK,JS.DEPARTURE_DATE HBLDATE,BS.BOOKING_REF_NO,JS.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_PK,SHP.CUSTOMER_NAME AS SHIPPER," ;
            Strsql += " SHPDET.ADM_ADDRESS_1 as SHIPPERADD, SHPDET.ADM_ADDRESS_2 as shadd2,SHPDET.ADM_ADDRESS_3 as shadd3,shpdet.adm_city as shcity ,shpdet.adm_zip_code as shzip," ;
            Strsql += " (case when H.IS_TO_ORDER = '0' then CON.customer_name else H.consignee_name end) as CONSIGNEE," ;
            Strsql += " (case when H.IS_TO_ORDER = '0' then CONDET.ADM_ADDRESS_1 else h.consignee_address end) as CONSIGNEEADD," ;
            Strsql += " (CASE";
            Strsql += "         WHEN H.IS_TO_ORDER = '0' THEN";
            Strsql += "          CONDET.ADM_ADDRESS_2";
            Strsql += "         ELSE";
            Strsql += "          ''";
            Strsql += "       END) ADM_ADDRESS_2,";
            Strsql += "      (CASE";
            Strsql += "         WHEN H.IS_TO_ORDER = '0' THEN";
            Strsql += "          CONDET.ADM_ADDRESS_3";
            Strsql += "      ELSE";
            Strsql += "          ''";
            Strsql += "      END) ADM_ADDRESS_3,";
            Strsql += "       (CASE";
            Strsql += "        WHEN H.IS_TO_ORDER = '0' THEN";
            Strsql += "        CONDET.ADM_CITY";
            Strsql += "        ELSE";
            Strsql += "         ''";
            Strsql += "      END) ADM_CITY,";
            Strsql += "      (CASE";
            Strsql += "         WHEN H.IS_TO_ORDER = '0' THEN";
            Strsql += "          CONDET.ADM_ZIP_CODE";
            Strsql += "         ELSE";
            Strsql += "         ''";
            Strsql += "      END) ADM_ZIP_CODE,";
            Strsql += "   (case when NVL(H.SAC_N1,0) = 0 then NOTI.CUSTOMER_NAME else 'Same As Consignee' end) as NOTIFY," ;
            // Strsql &= " NOTI.CUSTOMER_NAME AS NOTIFY," & vbCrLf
            Strsql += "notdet.adm_address_1 as NOTIFYADD,notdet.adm_address_2 as nadd2,notdet.adm_address_3 as nadd3,notdet.adm_city as ncity,notdet.adm_zip_code as nzip," ;
            Strsql += " (H.VESSEL_NAME || '-' || H.VOYAGE) AS VSLVOY, nvl(H.HBL_ORIGINAL_PRINTS,0) HBL_ORIGINAL_PRINTS, UPPER(PL.PORT_ID || ', ' || H.POL_COUNTRY) AS POL, UPPER(PD.PORT_ID || ', ' || H.POD_COUNTRY) AS POD," ;
            Strsql += " (CASE WHEN PREC.PLACE_NAME IS NOT NULL THEN " ;
            Strsql += "   UPPER(PREC.PLACE_CODE || ', ' || H.PLR_COUNTRY) ELSE" ;
            Strsql += "  UPPER(PL.PORT_ID || ', ' || H.POL_COUNTRY) END) PRECEIPT," ;
            Strsql += " (CASE WHEN PDEL.PLACE_NAME IS NOT NULL THEN" ;
            Strsql += "  UPPER(PDEL.PLACE_CODE || ', ' || PFD_COUNTRY) ELSE" ;
            Strsql += "   UPPER(PD.PORT_ID || ', ' || H.POD_COUNTRY) END) PDELIVERY," ;
            Strsql += " BS.PYMT_TYPE," ;
            Strsql += " (CASE WHEN BS.PYMT_TYPE = 1 THEN " ;
            Strsql += "  'ORIGIN'" ;
            Strsql += "  ELSE 'DESTINATION'" ;
            Strsql += "  END)FREIGHTPAYABLE," ;
            Strsql += "  HBLBL.BL_DESCRIPTION, h.hbl_status as status, NVL(H.PLACE_ISSUE, (SELECT L.LOCATION_NAME FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ")) PLACE_ISSUE " ;
            Strsql += " FROM HBL_EXP_TBL H,CUSTOMER_MST_TBL SHP,CUSTOMER_CONTACT_DTLS SHPDET,CUSTOMER_MST_TBL CON," ;
            Strsql += " CUSTOMER_CONTACT_DTLS CONDET,CUSTOMER_MST_TBL NOTI,CUSTOMER_CONTACT_DTLS NOTDET," ;
            Strsql += " BOOKING_MST_TBL BS,JOB_CARD_TRN JS,PORT_MST_TBL PL, HBL_BL_CLAUSE_TBL HBLBL,PORT_MST_TBL PD,PLACE_MST_TBL PREC,PLACE_MST_TBL PDEL" ;
            Strsql += " WHERE  H.SHIPPER_CUST_MST_FK(+)=SHP.CUSTOMER_MST_PK" ;
            Strsql += " AND SHPDET.CUSTOMER_MST_FK=SHP.CUSTOMER_MST_PK(+)" ;
            if (From == "MSTSEA")
            {
                Strsql += " AND H.HBL_EXP_TBL_PK IN (" + HBLPk + ")" ;
            }
            else
            {
                Strsql += " AND H.HBL_EXP_TBL_PK=" + HBLPk ;
            }
            Strsql += " AND H.CONSIGNEE_CUST_MST_FK=CON.CUSTOMER_MST_PK(+)" ;
            Strsql += " AND CONDET.CUSTOMER_MST_FK(+)=CON.CUSTOMER_MST_PK" ;
            Strsql += " AND NOTI.CUSTOMER_MST_PK(+)=H.NOTIFY1_CUST_MST_FK" ;
            Strsql += " AND NOTI.CUSTOMER_MST_PK=NOTDET.CUSTOMER_MST_FK(+)" ;
            Strsql += " AND BS.BOOKING_MST_PK=JS.BOOKING_MST_FK" ;
            Strsql += " AND (H.JOB_CARD_SEA_EXP_FK=JS.JOB_CARD_TRN_PK OR H.NEW_JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK)" ;
            Strsql += " AND PL.PORT_MST_PK=BS.PORT_MST_POL_FK" ;
            Strsql += " AND PD.PORT_MST_PK=BS.PORT_MST_POD_FK" ;
            Strsql += " AND PREC.PLACE_PK(+)=BS.COL_PLACE_MST_FK" ;
            Strsql += " AND PDEL.PLACE_PK(+)=BS.DEL_PLACE_MST_FK" ;
            Strsql += " AND HBLBL.HBL_EXP_TBL_FK(+) = H.HBL_EXP_TBL_PK" ;
            try
            {
                return Objwk.GetDataSet(Strsql);

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }
        public DataSet FetchHBLBIFADetails(string HBLPk, string From = "")
        {
            //ADDED BY MINAKSHI FOR BIFA NAD FIATA FORMAT
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql = " SELECT H.HBL_EXP_TBL_PK,H.hbl_ref_no,J.JOB_CARD_TRN_FK JOB_CARD_SEA_EXP_FK ,H.MARKS_NUMBERS AS Marks," ;
            Strsql += " H.GOODS_DESCRIPTION  AS GDesc,J.CONTAINER_NUMBER, CTMT.CONTAINER_TYPE_MST_ID, J.SEAL_NUMBER, " ;
            Strsql += "  J.PACK_COUNT,TRIM(TO_CHAR(J.GROSS_WEIGHT, '999,999,999.000')) GROSS_WEIGHT, TRIM(TO_CHAR((J.VOLUME_IN_CBM), '999,999,999.000')) TAREWEIGHT, " ;
            Strsql += "   TRIM(TO_CHAR((select sum(nvl(jj.GROSS_WEIGHT,0)) from JOB_TRN_CONT jj where jj.JOB_CARD_TRN_FK=JS.JOB_CARD_TRN_PK), '999,999,999.000')) AS GrossWt, TRIM(TO_CHAR((select sum(nvl(jj.VOLUME_IN_CBM,0)) from JOB_TRN_CONT jj where jj.JOB_CARD_TRN_FK=JS.JOB_CARD_TRN_PK), '999,999,999.000')) AS Volume,H.HBL_REF_NO,H.VESSEL_NAME || '/' || H.VOYAGE VSLVOY " ;
            Strsql += " FROM HBL_EXP_TBL H,JOB_TRN_CONT J, CONTAINER_TYPE_MST_TBL CTMT,JOB_CARD_TRN JS " ;
            Strsql += " WHERE (H.JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK OR H.NEW_JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK) " ;
            Strsql += " AND JS.JOB_CARD_TRN_PK=J.JOB_CARD_TRN_FK" ;
            Strsql += " AND CTMT.CONTAINER_TYPE_MST_PK(+) = J.CONTAINER_TYPE_MST_FK " ;


            //Added by rabbani passing multible HBLPKs
            if (From == "MSTSEA")
            {
                Strsql += " AND H.HBL_EXP_TBL_PK IN (" + HBLPk + ")" ;
            }
            else
            {
                Strsql += " AND H.HBL_EXP_TBL_PK=" + HBLPk ;
            }

            // Strsql &= " AND HBLBL.HBL_EXP_TBL_FK(+) = H.HBL_EXP_TBL_PK" & vbCrLf
            try
            {
                return Objwk.GetDataSet(Strsql);

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }
        public DataSet FetchDelAddress(string HBLPk, string From = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT HH.HBL_EXP_TBL_PK,  UPPER(AMST.AGENT_NAME) AGENTNAME,";
            Strsql += "ADTLS.ADM_ADDRESS_1 ADD1," ;
            Strsql += "ADTLS.ADM_ADDRESS_2 ADD2," ;
            Strsql += "ADTLS.ADM_ADDRESS_3 ADD3," ;
            Strsql += "ADTLS.ADM_CITY CITY," ;
            Strsql += "CMST.COUNTRY_NAME COUNTRY," ;
            Strsql += "ADTLS.ADM_ZIP_CODE ZIP," ;
            Strsql += "ADTLS.ADM_PHONE_NO_1 PHONE," ;
            Strsql += "ADTLS.ADM_FAX_NO FAX," ;
            Strsql += "ADTLS.ADM_EMAIL_ID EMAIL" ;
            Strsql += " FROM JOB_CARD_TRN JS,HBL_EXP_TBL HH,AGENT_MST_TBL AMST," ;
            Strsql += " AGENT_CONTACT_DTLS ADTLS," ;
            Strsql += " COUNTRY_MST_TBL CMST" ;
            Strsql += " WHERE JS.JOB_CARD_TRN_PK=HH.JOB_CARD_SEA_EXP_FK" ;
            Strsql += "AND JS.DP_AGENT_MST_FK=AMST.AGENT_MST_PK(+)" ;
            Strsql += "AND AMST.AGENT_MST_PK=ADTLS.AGENT_MST_FK(+)" ;
            Strsql += " AND CMST.COUNTRY_MST_PK(+) =ADTLS.ADM_COUNTRY_MST_FK";
            //Added by rabbani passing multible HBLPKs
            if (From == "MSTSEA")
            {
                Strsql += " AND HH.HBL_EXP_TBL_PK IN (" + HBLPk + ")" ;
            }
            else
            {
                Strsql += " AND HH.HBL_EXP_TBL_PK=" + HBLPk ;
            }

            try
            {
                return Objwk.GetDataSet(Strsql);

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }
        public DataSet FetchDescExtra(string HBLPk, string From = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT HH.HBL_EXP_TBL_PK, HH.GOODS_DESCRIPTION" ;
            Strsql += " FROM HBL_EXP_TBL  HH " ;
            if (From == "MSTSEA")
            {
                Strsql += " WHERE HH.HBL_EXP_TBL_PK IN (" + HBLPk + ")" ;
            }
            else
            {
                Strsql += " WHERE HH.HBL_EXP_TBL_PK=" + HBLPk ;
            }


            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //Added by sivachandran GAP-VEK-020
        public DataSet Get_ConDet(string HBLPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                Strsql = "SELECT H.HBL_EXP_TBL_PK," ;
                Strsql += "J.CONTAINER_NUMBER CONTAINER_NUMBER,J.SEAL_NUMBER,J.PACK_COUNT," ;
                Strsql += "J.CONTAINER_NUMBER AS Cont_Num," ;
                Strsql += "CASE WHEN JS.CARGO_TYPE = 4 THEN   J.CHARGEABLE_WEIGHT" ;
                Strsql += "ELSE  J.GROSS_WEIGHT   END GROSS_WEIGHT, " ;
                Strsql += "(CTYPE.CONTAINER_TAREWEIGHT_TONE) TAREWEIGHT,  H.MARKS_NUMBERS AS Marks,  H.GOODS_DESCRIPTION AS GDesc,CTYPE.CONTAINER_TYPE_MST_ID AS CONT_TYPE,J.VOLUME_IN_CBM VOLUME" ;
                Strsql += "FROM HBL_EXP_TBL  H,JOB_TRN_CONT   J,JOB_CARD_TRN   JS," ;
                Strsql += "CONTAINER_TYPE_MST_TBL CTYPE " ;
                Strsql += "WHERE (H.JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK OR ";
                Strsql += "H.NEW_JOB_CARD_SEA_EXP_FK = JS.JOB_CARD_TRN_PK) " ;
                Strsql += "AND JS.JOB_CARD_TRN_PK = J.JOB_CARD_TRN_FK " ;
                Strsql += "AND CTYPE.CONTAINER_TYPE_MST_PK(+) = J.CONTAINER_TYPE_MST_FK " ;
                Strsql += "AND H.HBL_EXP_TBL_PK in(" + HBLPk + ")" ;
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //End
        }
        #endregion

        #region "Check No of Containers"
        public string FetchContainerCount(string hblpk)
        {
            WorkFlow Objwk = new WorkFlow();
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("  select count(jcnt.container_number) JCount");
                sb.Append("   from job_card_sea_exp_tbl jc,");
                sb.Append("         hbl_exp_tbl hbl, ");
                sb.Append("         job_trn_sea_exp_cont jcnt");
                sb.Append("   where jc.job_card_sea_exp_pk = hbl.job_card_sea_exp_fk");
                sb.Append("   and jc.job_card_sea_exp_pk = jcnt.job_card_sea_exp_fk");
                sb.Append("   and hbl.hbl_exp_tbl_pk = " + hblpk + " ");
                return Objwk.ExecuteScaler(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        //GetHBLCount funtionality has been added by Sreenivas - 02/02/2010
        #region "Fetch HBL Count"
        public int GetHBLCount(string HBlRefNr, int HBLPK)
        {
            System.Text.StringBuilder strHBLQuery = new System.Text.StringBuilder(5000);
            strHBLQuery.Append("select hbl.hbl_exp_tbl_pk from hbl_exp_tbl hbl where hbl.hbl_ref_no like '%" + HBlRefNr + "%'");
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            ds = objWF.GetDataSet(strHBLQuery.ToString());
            if (ds.Tables[0].Rows.Count == 1)
            {
                HBLPK = Convert.ToInt32(ds.Tables[0].Rows[0][0]);
            }
            return ds.Tables[0].Rows.Count;
        }
        //End-Sreenivas
        #endregion

        #region "Fetch Temperature"
        public DataSet FetchTemperatur(string HBLPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(1000);
            DataSet CommDs = new DataSet();
            WorkFlow objWF = new WorkFlow();
            sb.Append("  SELECT CTMT.CONTAINER_TYPE_MST_ID,");
            sb.Append(" C.VENTILATION,");
            sb.Append("  to_char(C.MIN_TEMP) as MIN_TEMP ,");
            sb.Append(" to_char(C.MIN_TEMP_UOM) as MIN_TEMP_UOM,");
            sb.Append(" to_char(C.MAX_TEMP) as MAX_TEMP ,");
            sb.Append(" to_char( C.MAX_TEMP_UOM) as  MAX_TEMP_UOM");
            sb.Append(" FROM BOOKING_TRN_SPL_REQ C, BOOKING_TRN T, CONTAINER_TYPE_MST_TBL CTMT ");
            sb.Append(" WHERE(C.BOOKING_TRN_FK = T.BOOKING_TRN_PK) ");
            sb.Append(" AND CTMT.CONTAINER_TYPE_MST_PK(+) = T.CONTAINER_TYPE_MST_FK ");
            sb.Append(" AND T.BOOKING_MST_FK =");
            sb.Append("(SELECT JCT.BOOKING_MST_FK ");
            sb.Append(" FROM JOB_CARD_TRN JCT");
            sb.Append(" WHERE JCT.JOB_CARD_TRN_PK IN");
            sb.Append(" (SELECT H.JOB_CARD_SEA_EXP_FK");
            sb.Append(" FROM HBL_EXP_TBL H ");
            sb.Append("  WHERE H.HBL_REF_NO IN");
            sb.Append(" (SELECT HBLPK.HBL_REF_NO");
            sb.Append("  FROM HBL_EXP_TBL HBLPK");
            sb.Append(" WHERE HBLPK.HBL_EXP_TBL_PK IN (" + HBLPk + "))))");
            CommDs = objWF.GetDataSet(sb.ToString());
            return CommDs;
        }
        #endregion
    }
}