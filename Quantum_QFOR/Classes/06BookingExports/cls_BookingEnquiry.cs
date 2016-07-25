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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Globalization;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class cls_BookingEnquiry : CommonFeatures
    {

        #region "Fetch the list of enquiry records"

        //This function is called to fetch the list of enquiry records
        public DataSet FetchAll(string strEnqPK = "", string strCustPK = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", int UsrLocFk = 0, int IsCarted = 0, string lblPOLEnqPK = "0", string lblPODEnqPK = "0",
        string txtExecuteEnqPK = "", string toDate = "", string fromDate = "", string ddlCommGroupEnq = "", string ddlCargoTypeEnq = "", string ddlEnqTypeEnq = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCondition = "FROM";
            strCondition += "ENQUIRY_BKG_SEA_TBL ENQ,";
            strCondition += "ENQUIRY_TRN_SEA_FCL_LCL TRN,";
            strCondition += "EMPLOYEE_MST_TBL    EMT,";
            strCondition += "DESIGNATION_MST_TBL DMT,";
            strCondition += "USER_MST_TBL UMT";
            strCondition += "WHERE";
            strCondition += " ENQ.EXECUTED_BY= UMT.USER_MST_PK(+)";
            strCondition += " AND ENQ.ENQUIRY_BKG_SEA_PK = TRN.ENQUIRY_MAIN_SEA_FK(+)";
            strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + UsrLocFk + " ";
            strCondition += "AND ENQ.CREATED_BY_FK = UMT.USER_MST_PK ";
            strCondition += "AND  UMT.EMPLOYEE_MST_FK = EMT.EMPLOYEE_MST_PK ";
            strCondition += "AND EMT.DESIGNATION_MST_FK = DMT.DESIGNATION_MST_PK ";
            if (!string.IsNullOrEmpty(fromDate))
            {
                strCondition = strCondition + " And TO_DATE(ENQ.ENQUIRY_DATE,DATEFORMAT) >= TO_DATE('" + fromDate + "',dateformat)";
            }
            if (!string.IsNullOrEmpty(toDate))
            {
                strCondition = strCondition + " And TO_DATE(ENQ.ENQUIRY_DATE,DATEFORMAT) <= TO_DATE('" + toDate + "',dateformat)";
            }
            if (!string.IsNullOrEmpty(strEnqPK.Trim()))
            {
                strCondition += "AND ENQ.ENQUIRY_BKG_SEA_PK=" + strEnqPK.Trim();
            }

            if (IsCarted == 6)
            {
                strCondition += "AND ENQ.IS_CARTED = 1 ";
            }
            else
            {
                strCondition += "AND ENQ.IS_CARTED = 0 ";
            }

            if (!string.IsNullOrEmpty(strCustPK.Trim()))
            {
                strCondition += "AND ENQ.CUSTOMER_MST_FK=" + strCustPK.Trim();
            }
            if (lblPOLEnqPK.Trim() != "0" & !string.IsNullOrEmpty(lblPOLEnqPK.Trim()))
            {
                strCondition += "AND TRN.PORT_MST_POL_FK=" + lblPOLEnqPK.Trim();
            }
            if (lblPODEnqPK.Trim() != "0" & !string.IsNullOrEmpty(lblPODEnqPK.Trim()))
            {
                strCondition += "AND TRN.PORT_MST_POD_FK=" + lblPODEnqPK.Trim();
            }
            if (!string.IsNullOrEmpty(txtExecuteEnqPK.Trim()))
            {
                strCondition += "AND UMT.USER_MST_PK =" + txtExecuteEnqPK.Trim();
            }
            if (ddlCargoTypeEnq.Trim() != "0")
            {
                strCondition += "AND ENQ.CARGO_TYPE=" + ddlCargoTypeEnq.Trim();
            }
            if (ddlEnqTypeEnq.Trim() != "0")
            {
                strCondition += "AND TRN.TRANS_REFERED_FROM=" + ddlEnqTypeEnq.Trim();
            }
            if (ddlCommGroupEnq.Trim() != "0")
            {
                strCondition += "AND TRN.COMMODITY_GROUP_FK=" + ddlCommGroupEnq.Trim();
            }

            string strCount = null;
            strCount = "SELECT COUNT(DISTINCT ENQ.ENQUIRY_BKG_SEA_PK)  ";
            strCount += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount));
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


            strSQL = " SELECT * FROM (";
            strSQL += "SELECT ROWNUM SR_NO, q.* FROM (";
            strSQL += "SELECT DISTINCT";
            strSQL += "ENQ.ENQUIRY_REF_NO,";
            strSQL += "ENQ.ENQUIRY_BKG_SEA_PK,";
            strSQL += "TO_DATE(ENQ.ENQUIRY_DATE,'' || DATEFORMAT || '') AS \"ENQDATE\",";

            strSQL += "(CASE CUST_TYPE WHEN 0 THEN (SELECT CUMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CUMT WHERE CUMT.CUSTOMER_MST_PK=ENQ.CUSTOMER_MST_FK) WHEN 1 THEN  (SELECT CUMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL CUMT WHERE CUMT.CUSTOMER_MST_PK=ENQ.CUSTOMER_MST_FK) END) CUSTOMER_NAME,";
            strSQL += "DECODE(ENQ.CARGO_TYPE, 1, 'FCL',2,'LCL') AS \"CARGOTYPE\",";
            strSQL += "UMT.USER_NAME AS \"EXECUTEDBY\" ,";
            strSQL += " DMT.DESIGNATION_NAME AS \"ROLE\" ";

            strSQL += strCondition;
            if (SortColumn == "ENQDATE")
            {
                SortColumn = "ENQ.ENQUIRY_DATE";
            }
            else if (SortColumn == "CARGOTYPE")
            {
                SortColumn = "ENQ.CARGO_TYPE";
            }

            strSQL += " ORDER BY ENQ.ENQUIRY_BKG_SEA_PK " + SortType + " ) q  ) ";
            strSQL += " WHERE SR_NO  BETWEEN " + start + " AND " + last;

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

        #region "Fetch the Rates from Tariff"
        //This function is called to fetch the rates from operator tariff 
        //Called for Enquiry on Rates
        public DataSet FetchRates(long nPOLPk, long nPODPk, Int16 nIsLCL, string strDate, long nCommPk, long nOperatorPk = 0)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (nIsLCL == 1)
                {
                    strBuilder.Append(" SELECT POL.PORT_ID AS \"POL\",POD.PORT_ID AS \"POD\",");
                    strBuilder.Append(" OPR.OPERATOR_MST_PK,");
                    strBuilder.Append(" nvl(OPR.OPERATOR_Name,'General') AS OPERATOR_Name,");
                    strBuilder.Append(" FRT.FREIGHT_ELEMENT_ID AS \"FRTELEMENT\",");
                    strBuilder.Append(" CURR.CURRENCY_ID AS \"CURRENCY\",");
                    strBuilder.Append(" CMT.CONTAINER_TYPE_MST_ID AS \"CONTAINER_TYPE\",");
                    strBuilder.Append(" CONT.FCL_REQ_RATE AS \"RATE\",");
                    strBuilder.Append(" com.commodity_group_code AS \"COMMODITY_GROUP\",");
                    strBuilder.Append(" TO_CHAR(HDR.VALID_FROM,'" + dateFormat + "') AS \"VALID_FROM\",");
                    strBuilder.Append(" TO_CHAR(HDR.VALID_TO,'" + dateFormat + "') AS \"VALID_TO\",");
                    strBuilder.Append(" TRN.CHECK_FOR_ALL_IN_RT AS \"CHK\"");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" TARIFF_MAIN_SEA_TBL HDR,");
                    strBuilder.Append(" TARIFF_TRN_SEA_FCL_LCL TRN,");
                    strBuilder.Append(" TARIFF_TRN_SEA_CONT_DTL CONT,");
                    strBuilder.Append(" PORT_MST_TBL POL,");
                    strBuilder.Append(" PORT_MST_TBL POD,");
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL CURR,");
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FRT,");
                    strBuilder.Append(" commodity_group_mst_tbl com,");
                    strBuilder.Append(" CONTAINER_TYPE_MST_TBL CMT,");
                    strBuilder.Append(" OPERATOR_MST_TBL OPR");
                    strBuilder.Append(" WHERE");
                    strBuilder.Append(" HDR.TARIFF_MAIN_SEA_PK = TRN.TARIFF_MAIN_SEA_FK");
                    strBuilder.Append(" AND CONT.TARIFF_TRN_SEA_FK = TRN.TARIFF_TRN_SEA_PK");
                    //Snigdharani
                    strBuilder.Append(" AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK");
                    strBuilder.Append(" AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK");
                    strBuilder.Append(" AND HDR.COMMODITY_GROUP_FK = com.commodity_group_pk(+)");
                    strBuilder.Append(" AND CURR.CURRENCY_MST_PK=TRN.CURRENCY_MST_FK");
                    strBuilder.Append(" AND FRT.FREIGHT_ELEMENT_MST_PK=TRN.FREIGHT_ELEMENT_MST_FK");
                    strBuilder.Append(" AND CMT.CONTAINER_TYPE_MST_PK=CONT.CONTAINER_TYPE_MST_FK");
                    strBuilder.Append(" AND HDR.CARGO_TYPE=1 AND HDR.ACTIVE=1 ");
                    strBuilder.Append(" AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk));
                    strBuilder.Append(" AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk));
                    strBuilder.Append(" AND HDR.OPERATOR_MST_FK=OPR.OPERATOR_MST_PK(+)");
                    if (nOperatorPk != 0)
                    {
                        strBuilder.Append(" AND HDR.OPERATOR_MST_FK=" + Convert.ToString(nOperatorPk));
                    }
                    strBuilder.Append(" AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))");
                    if (nCommPk != 0)
                    {
                        strBuilder = strBuilder.Append(" AND HDR.COMMODITY_GROUP_FK = " + Convert.ToString(nCommPk));
                    }
                    strBuilder.Append(" ORDER BY OPERATOR_MST_PK, OPERATOR_Name, COMMODITY_GROUP, ");
                    strBuilder.Append(" CONTAINER_TYPE, PREFERENCE ");
                }
                else
                {
                    //Lcl
                    strBuilder.Append(" SELECT \"POL\",");
                    strBuilder.Append("        \"POD\",");
                    strBuilder.Append("        OPERATOR_MST_PK,");
                    strBuilder.Append("        OPERATOR_Name,");
                    strBuilder.Append("        \"FRTELEMENT\",");
                    strBuilder.Append("        \"CURRENCY\",");
                    strBuilder.Append("        \"BASIS\",");
                    strBuilder.Append("        \"MIN_RATE\",");
                    strBuilder.Append("        \"RATE\",");
                    strBuilder.Append("        \"COMMODITY_GROUP\",");
                    strBuilder.Append("        \"VALID_FROM\",");
                    strBuilder.Append("        \"VALID_TO\",");
                    strBuilder.Append("        \"CHK\" FROM ( ");

                    strBuilder.Append(" SELECT POL.PORT_ID AS \"POL\",POD.PORT_ID AS \"POD\",");
                    strBuilder.Append(" nvl( OPR.OPERATOR_MST_PK, 0) AS OPERATOR_MST_PK,");
                    strBuilder.Append(" nvl(OPR.OPERATOR_Name,'General') AS OPERATOR_Name,");
                    strBuilder.Append(" FRT.FREIGHT_ELEMENT_ID AS \"FRTELEMENT\",");
                    strBuilder.Append(" CURR.CURRENCY_ID AS \"CURRENCY\",");
                    strBuilder.Append(" UOM.DIMENTION_ID AS \"BASIS\",");
                    strBuilder.Append(" TRN.LCL_TARIFF_MIN_RATE AS \"MIN_RATE\",");
                    strBuilder.Append(" TRN.LCL_TARIFF_RATE AS \"RATE\",");
                    strBuilder.Append(" com.commodity_group_code AS \"COMMODITY_GROUP\",");
                    strBuilder.Append(" TO_CHAR(HDR.VALID_FROM,'" + dateFormat + "') AS \"VALID_FROM\",");
                    strBuilder.Append(" TO_CHAR(HDR.VALID_TO,'" + dateFormat + "') AS \"VALID_TO\",");
                    strBuilder.Append(" TRN.CHECK_FOR_ALL_IN_RT AS \"CHK\", FRT.PREFERENCE ");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" TARIFF_MAIN_SEA_TBL HDR,");
                    strBuilder.Append(" TARIFF_TRN_SEA_FCL_LCL TRN,");
                    strBuilder.Append(" PORT_MST_TBL POL,");
                    strBuilder.Append(" PORT_MST_TBL POD,");
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL CURR,");
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FRT,");
                    strBuilder.Append(" commodity_group_mst_tbl com,");
                    strBuilder.Append(" DIMENTION_UNIT_MST_TBL UOM,");
                    strBuilder.Append(" OPERATOR_MST_TBL OPR");
                    strBuilder.Append(" WHERE");
                    strBuilder.Append(" HDR.TARIFF_MAIN_SEA_PK = TRN.TARIFF_MAIN_SEA_FK");
                    strBuilder.Append(" AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK");
                    strBuilder.Append(" AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK");
                    strBuilder.Append(" AND HDR.COMMODITY_GROUP_FK = com.commodity_group_pk(+)");
                    strBuilder.Append(" AND CURR.CURRENCY_MST_PK=TRN.CURRENCY_MST_FK");
                    strBuilder.Append(" AND FRT.FREIGHT_ELEMENT_MST_PK=TRN.FREIGHT_ELEMENT_MST_FK");
                    strBuilder.Append(" AND TRN.LCL_BASIS=UOM.DIMENTION_UNIT_MST_PK");
                    strBuilder.Append(" AND HDR.CARGO_TYPE=2 AND HDR.ACTIVE=1 ");
                    strBuilder.Append(" AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk));
                    strBuilder.Append(" AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk));
                    strBuilder.Append(" AND HDR.OPERATOR_MST_FK=OPR.OPERATOR_MST_PK(+)");
                    strBuilder.Append(" AND FRT.FREIGHT_ELEMENT_MST_PK IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)");
                    //Added by rabbani on 24/3/07 ,To display BOF as first element in Freight Element Column.
                    if (nOperatorPk != 0)
                    {
                        strBuilder.Append(" AND HDR.OPERATOR_MST_FK=" + Convert.ToString(nOperatorPk));
                    }
                    if (nCommPk != 0)
                    {
                        strBuilder = strBuilder.Append(" AND HDR.COMMODITY_GROUP_FK=" + Convert.ToString(nCommPk));
                    }
                    strBuilder.Append(" AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))");

                    strBuilder.Append(" UNION ALL");

                    strBuilder.Append(" SELECT POL.PORT_ID AS \"POL\",POD.PORT_ID AS \"POD\",");
                    strBuilder.Append(" nvl( OPR.OPERATOR_MST_PK, 0) AS OPERATOR_MST_PK,");
                    strBuilder.Append(" nvl(OPR.OPERATOR_Name,'General') AS OPERATOR_Name,");
                    strBuilder.Append(" FRT.FREIGHT_ELEMENT_ID AS \"FRTELEMENT\",");
                    strBuilder.Append(" CURR.CURRENCY_ID AS \"CURRENCY\",");
                    strBuilder.Append(" UOM.DIMENTION_ID AS \"BASIS\",");
                    strBuilder.Append(" TRN.LCL_TARIFF_MIN_RATE AS \"MIN_RATE\",");
                    //Added by Rabbani raeson USS Gap,introduced New column as "Min.Rate"
                    strBuilder.Append(" TRN.LCL_TARIFF_RATE AS \"RATE\",");
                    strBuilder.Append(" com.commodity_group_code AS \"COMMODITY_GROUP\",");
                    strBuilder.Append(" TO_CHAR(HDR.VALID_FROM,'" + dateFormat + "') AS \"VALID_FROM\",");
                    strBuilder.Append(" TO_CHAR(HDR.VALID_TO,'" + dateFormat + "') AS \"VALID_TO\",");
                    strBuilder.Append(" TRN.CHECK_FOR_ALL_IN_RT AS \"CHK\", FRT.PREFERENCE ");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" TARIFF_MAIN_SEA_TBL HDR,");
                    strBuilder.Append(" TARIFF_TRN_SEA_FCL_LCL TRN,");
                    strBuilder.Append(" PORT_MST_TBL POL,");
                    strBuilder.Append(" PORT_MST_TBL POD,");
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL CURR,");
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FRT,");
                    strBuilder.Append(" commodity_group_mst_tbl com,");
                    strBuilder.Append(" DIMENTION_UNIT_MST_TBL UOM,");
                    strBuilder.Append(" OPERATOR_MST_TBL OPR");
                    strBuilder.Append(" WHERE");
                    strBuilder.Append(" HDR.TARIFF_MAIN_SEA_PK = TRN.TARIFF_MAIN_SEA_FK");
                    strBuilder.Append(" AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK");
                    strBuilder.Append(" AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK");
                    strBuilder.Append(" AND HDR.COMMODITY_GROUP_FK = com.commodity_group_pk(+)");
                    strBuilder.Append(" AND CURR.CURRENCY_MST_PK=TRN.CURRENCY_MST_FK");
                    strBuilder.Append(" AND FRT.FREIGHT_ELEMENT_MST_PK=TRN.FREIGHT_ELEMENT_MST_FK");
                    strBuilder.Append(" AND TRN.LCL_BASIS=UOM.DIMENTION_UNIT_MST_PK");
                    strBuilder.Append(" AND HDR.CARGO_TYPE=2 AND HDR.ACTIVE=1 ");
                    strBuilder.Append(" AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk));
                    strBuilder.Append(" AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk));
                    strBuilder.Append(" AND HDR.OPERATOR_MST_FK=OPR.OPERATOR_MST_PK(+)");
                    strBuilder.Append(" AND FRT.FREIGHT_ELEMENT_MST_PK NOT IN (SELECT PARM.FRT_BOF_FK  FROM PARAMETERS_TBL PARM)");
                    if (nOperatorPk != 0)
                    {
                        strBuilder.Append(" AND HDR.OPERATOR_MST_FK=" + Convert.ToString(nOperatorPk));
                    }
                    strBuilder.Append(" AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))");
                    if (nCommPk != 0)
                    {
                        strBuilder = strBuilder.Append(" AND HDR.COMMODITY_GROUP_FK=" + Convert.ToString(nCommPk));
                    }
                    strBuilder.Append(" ) ORDER BY PREFERENCE ");
                }

                return (new WorkFlow()).GetDataSet(strBuilder.ToString());
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

        #region "Fetch the Rates from Tariff for master"

        //This function is called to fetch the rates from operator tariff 
        //Called for Enquiry on Rates
        public DataSet FetchRatesMst(long nPOLPk, long nPODPk, Int16 nIsLCL, string strDate, long nCommPk, long nOperatorPk = 0)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                strBuilder.Append(" SELECT distinct POL.PORT_ID AS \"POL\",");
                strBuilder.Append(" POD.PORT_ID AS \"POD\",OPR.OPERATOR_MST_PK,nvl(OPR.OPERATOR_ID, 'General') AS OPERATOR_ID,");
                strBuilder.Append(" com.commodity_group_code AS \"COMMODITY_GROUP\", ");
                strBuilder.Append("  TO_CHAR(HDR.VALID_FROM, '" + dateFormat + "') AS \"VALID_FROM\", ");
                strBuilder.Append("  TO_CHAR(HDR.VALID_TO, '" + dateFormat + "') AS \"VALID_TO\",");
                strBuilder.Append("  TRN.CHECK_FOR_ALL_IN_RT AS \"CHK\"");
                strBuilder.Append("   FROM TARIFF_MAIN_SEA_TBL HDR,TARIFF_TRN_SEA_FCL_LCL TRN,PORT_MST_TBL POL,PORT_MST_TBL POD,");
                strBuilder.Append(" commodity_group_mst_tbl com,OPERATOR_MST_TBL OPR");
                strBuilder.Append(" WHERE HDR.TARIFF_MAIN_SEA_PK = TRN.TARIFF_MAIN_SEA_FK");
                strBuilder.Append("  AND TRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                strBuilder.Append("  AND TRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                strBuilder.Append("   AND HDR.COMMODITY_GROUP_FK = com.commodity_group_pk(+)");
                strBuilder.Append(" AND HDR.CARGO_TYPE = 1 AND HDR.ACTIVE = 1");
                strBuilder.Append(" AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk));
                strBuilder.Append(" AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk));
                strBuilder.Append(" AND HDR.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK(+)");
                if (nCommPk != 0)
                {
                    strBuilder.Append(" AND com.commodity_group_pk=" + Convert.ToString(nCommPk));
                }
                if (nOperatorPk != 0)
                {
                    strBuilder.Append(" AND HDR.OPERATOR_MST_FK=" + Convert.ToString(nOperatorPk));
                }
                strBuilder.Append(" AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))");

                return (new WorkFlow()).GetDataSet(strBuilder.ToString());
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

        #region "Fetch the POL,POD, OPERATOR AND BASIS FROM TARIFF"

        //This function is called to fetch the distinct POL,POD,Operator and Basis from operator tariff 
        //Called for Enquiry on Rates (LCL)
        public DataTable GetLCLHeader(long nPOLPk, long nPODPk, string strDate, long nCommPk, long nOperatorPk = 0)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();

            try
            {
                strBuilder.Append(" SELECT DISTINCT POL.PORT_ID AS \"POL\",POD.PORT_ID AS \"POD\",");
                strBuilder.Append(" OPR.OPERATOR_MST_PK,");
                strBuilder.Append(" nvl(OPR.OPERATOR_Name,'General') AS OPERATOR_Name, ");
                strBuilder.Append(" UOM.DIMENTION_ID AS \"BASIS\",");
                strBuilder.Append(" com.commodity_group_code AS \"COMMODITY_GROUP\",");
                strBuilder.Append(" TO_CHAR(HDR.VALID_FROM,'" + dateFormat + "') AS \"VALID_FROM\",");
                strBuilder.Append(" TO_CHAR(HDR.VALID_TO,'" + dateFormat + "') AS \"VALID_TO\"");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" TARIFF_MAIN_SEA_TBL HDR,");
                strBuilder.Append(" TARIFF_TRN_SEA_FCL_LCL TRN,");
                strBuilder.Append(" PORT_MST_TBL POL,");
                strBuilder.Append(" PORT_MST_TBL POD,");
                strBuilder.Append(" commodity_group_mst_tbl com, ");
                strBuilder.Append(" DIMENTION_UNIT_MST_TBL UOM,");
                strBuilder.Append(" OPERATOR_MST_TBL OPR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" HDR.TARIFF_MAIN_SEA_PK = TRN.TARIFF_MAIN_SEA_FK");
                strBuilder.Append(" AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK");
                strBuilder.Append(" AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK");
                strBuilder.Append(" AND HDR.COMMODITY_GROUP_FK = com.commodity_group_pk(+)");
                strBuilder.Append(" AND TRN.LCL_BASIS=UOM.DIMENTION_UNIT_MST_PK");
                strBuilder.Append(" AND HDR.OPERATOR_MST_FK=OPR.OPERATOR_MST_PK(+)");
                strBuilder.Append(" AND HDR.CARGO_TYPE=2 AND HDR.ACTIVE=1 ");
                strBuilder.Append(" AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk));
                strBuilder.Append(" AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk));
                if (nCommPk != 0)
                {
                    strBuilder.Append(" AND com.commodity_group_pk=" + Convert.ToString(nCommPk));
                }
                if (nOperatorPk != 0)
                {
                    strBuilder.Append(" AND HDR.OPERATOR_MST_FK=" + Convert.ToString(nOperatorPk));
                }
                strBuilder.Append(" AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))");
                return (new WorkFlow()).GetDataTable(strBuilder.ToString());
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

        #region "Function to get Container types,frt,port pair, Active Containers"

        //This function is called to fetch the distinct container types from operator tariff 
        //Called for Enquiry on Rates (FCL)
        public DataSet GetContTypes(long nPOLPk, long nPODPk, string strDate, long nCommPk, long nOperatorPk = 0)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT DISTINCT " + "CMT.CONTAINER_TYPE_MST_ID AS \"CONTAINER_TYPE\"" + ",CMT.PREFERENCES FROM " + " TARIFF_MAIN_SEA_TBL HDR," + " TARIFF_TRN_SEA_FCL_LCL TRN,";
                strSQL = strSQL + " TARIFF_TRN_SEA_CONT_DTL CONT," + " PORT_MST_TBL POL," + " PORT_MST_TBL POD," + " CURRENCY_TYPE_MST_TBL CURR," + " FREIGHT_ELEMENT_MST_TBL FRT," + " CONTAINER_TYPE_MST_TBL CMT," + " OPERATOR_MST_TBL OPR" + " WHERE" + " HDR.TARIFF_MAIN_SEA_PK = TRN.TARIFF_MAIN_SEA_FK" + " AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK" + " AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK" + " AND CONT.TARIFF_TRN_SEA_FK = TRN.TARIFF_TRN_SEA_PK" + " AND CURR.CURRENCY_MST_PK=TRN.CURRENCY_MST_FK" + " AND FRT.FREIGHT_ELEMENT_MST_PK=TRN.FREIGHT_ELEMENT_MST_FK" + " AND CMT.CONTAINER_TYPE_MST_PK=CONT.CONTAINER_TYPE_MST_FK" + " AND HDR.OPERATOR_MST_FK=OPR.OPERATOR_MST_PK(+)" + " AND HDR.CARGO_TYPE=1 AND HDR.ACTIVE=1" + " AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk) + " AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk);
                if (nOperatorPk != 0)
                {
                    strSQL += " AND HDR.OPERATOR_MST_FK=" + Convert.ToString(nOperatorPk);
                }
                strSQL += " AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))";
                if (nCommPk != 0)
                {
                    strSQL += " AND HDR.COMMODITY_GROUP_FK=" + Convert.ToString(nCommPk);
                }
                strSQL += " ORDER BY PREFERENCES ASC";
                return (new WorkFlow()).GetDataSet(strSQL);
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

        //This function is called to fetch the distinct freight elements from operator tariff 
        //Called for Enquiry on Rates (FCL)
        public DataSet GetFrtElements(long nPOLPk, long nPODPk, long nOperatorPk, string strDate, long nCommPk)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                strBuilder.Append("SELECT DISTINCT ");
                strBuilder.Append(" FRT.FREIGHT_ELEMENT_ID, FRT.PREFERENCE ");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" TARIFF_MAIN_SEA_TBL HDR,");
                strBuilder.Append(" TARIFF_TRN_SEA_FCL_LCL TRN,");
                strBuilder.Append(" TARIFF_TRN_SEA_CONT_DTL CONT,");
                strBuilder.Append(" PORT_MST_TBL POL,");
                strBuilder.Append(" PORT_MST_TBL POD,");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CURR,");
                strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FRT,");
                strBuilder.Append(" CONTAINER_TYPE_MST_TBL CMT");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" HDR.TARIFF_MAIN_SEA_PK = TRN.TARIFF_MAIN_SEA_FK");
                strBuilder.Append(" AND CONT.TARIFF_TRN_SEA_FK = TRN.TARIFF_TRN_SEA_PK");
                strBuilder.Append(" AND TRN.PORT_MST_POL_FK=POL.PORT_MST_PK");
                strBuilder.Append(" AND TRN.PORT_MST_POD_FK=POD.PORT_MST_PK");
                strBuilder.Append(" AND CURR.CURRENCY_MST_PK=TRN.CURRENCY_MST_FK");
                strBuilder.Append(" AND FRT.FREIGHT_ELEMENT_MST_PK=TRN.FREIGHT_ELEMENT_MST_FK");
                strBuilder.Append(" AND CMT.CONTAINER_TYPE_MST_PK=CONT.CONTAINER_TYPE_MST_FK");
                strBuilder.Append(" AND HDR.CARGO_TYPE=1 AND HDR.ACTIVE=1 ");
                strBuilder.Append(" AND TRN.PORT_MST_POL_FK=" + Convert.ToString(nPOLPk));
                strBuilder.Append(" AND TRN.PORT_MST_POD_FK=" + Convert.ToString(nPODPk));
                if (!(nOperatorPk == 0))
                {
                    strBuilder.Append(" AND HDR.OPERATOR_MST_FK=" + Convert.ToString(nOperatorPk));
                }
                strBuilder.Append(" AND to_date('" + strDate.Trim() + "',dateformat) BETWEEN HDR.VALID_FROM AND NVL(HDR.VALID_TO,to_date('" + strDate.Trim() + "',dateformat))");

                if (nCommPk != 0)
                {
                    strBuilder.Append(" AND HDR.COMMODITY_GROUP_FK=" + Convert.ToString(nCommPk));
                }
                strBuilder.Append(" ORDER BY FRT.PREFERENCE ");

                return (new WorkFlow()).GetDataSet(strBuilder.ToString());
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

        //This function is called to fetch the sea sectors for which the Port of Loading is a Working Port
        //Called for Enquiry on Rates (LCL), Enquiry on New Booking (LCL)
        public DataTable FetchPortPair(long nLocPK, long nBizType)
        {
            try
            {
                string strSQL = null;

                strSQL = "SELECT S.FROM_PORT_FK AS \"POLPK\",S.TO_PORT_FK AS \"PODPK\",POL.PORT_ID AS \"POL\",POD.PORT_ID AS \"POD\"";
                strSQL = strSQL + " FROM SECTOR_MST_TBL S, PORT_MST_TBL POL, PORT_MST_TBL POD";
                strSQL = strSQL + " WHERE S.FROM_PORT_FK = POL.PORT_MST_PK";
                strSQL = strSQL + " AND S.TO_PORT_FK=POD.PORT_MST_PK";
                strSQL = strSQL + " AND S.BUSINESS_TYPE=" + Convert.ToString(nBizType);
                strSQL = strSQL + " AND S.FROM_PORT_FK IN";
                strSQL = strSQL + "(";
                strSQL = strSQL + " SELECT POL.PORT_MST_PK";
                strSQL = strSQL + " FROM PORT_MST_TBL POL,LOC_PORT_MAPPING_TRN LPMT";
                strSQL = strSQL + " WHERE";
                strSQL = strSQL + " POL.PORT_MST_PK = LPMT.PORT_MST_FK";
                strSQL = strSQL + " AND POL.ACTIVE_FLAG=1";
                strSQL = strSQL + " AND POL.BUSINESS_TYPE =" + Convert.ToString(nBizType);
                strSQL = strSQL + " AND LPMT.LOCATION_MST_FK =" + Convert.ToString(nLocPK);
                strSQL = strSQL + " )";

                return (new WorkFlow()).GetDataTable(strSQL);
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

        //This function is called to fetch the active container types
        public DataTable ActiveContainers(bool SelDefault = true)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "";
            if (SelDefault)
            {
                strSQL = "SELECT " + "CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + "(CASE WHEN ROWNUM<=10 THEN '1' ELSE '0' END) CHK " + "FROM CONTAINER_TYPE_MST_TBL CMT " + "WHERE CMT.ACTIVE_FLAG=1  " + "ORDER BY CHK DESC,CMT.PREFERENCES";
            }
            else
            {
                strSQL = "SELECT " + "CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + "0 CHK " + "FROM CONTAINER_TYPE_MST_TBL CMT " + "WHERE CMT.ACTIVE_FLAG=1  " + "ORDER BY ROWNUM,CMT.PREFERENCES";
            }
            try
            {
                return objWF.GetDataTable(strSQL);
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

        #region "Functions to retrive Booking and Enquiry details"

        //This function is called to fetch the rates from spot rate, customer contract,quotation and operator tariff 
        //Called for Enquiry on New Booking
        public DataSet FetchNewBooking(long nBizModel, long nCustomerPk, long nCargoType, long nCommGrp, System.DateTime dtDate, string strSearch, Int16 intTransType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            object objCur = new object();
            try
            {
                objWK.OpenConnection();
                objCommand.Connection = objWK.MyConnection;
                var _with1 = objCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".TEMP_ENQ_NEW_BKG_PKG.GET_RATES";
                var _with2 = objCommand.Parameters;
                _with2.Add("BUSINESS_MODEL_IN", nBizModel).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMER_MST_FK_IN", nCustomerPk).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_TYPE_IN", nCargoType).Direction = ParameterDirection.Input;
                _with2.Add("COMMODITY_GROUP_FK_IN", nCommGrp).Direction = ParameterDirection.Input;
                _with2.Add("SHIPMENT_DATE_IN", dtDate).Direction = ParameterDirection.Input;
                _with2.Add("PORTPAIR_CONTAINER_IN", strSearch).Direction = ParameterDirection.Input;
                _with2.Add("TRANS_TYPE_IN", intTransType).Direction = ParameterDirection.Input;
                _with2.Add("RATE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("CONTAINER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("OPERATOR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objCommand;
                objWK.MyDataAdapter.Fill(dsData);
                return dsData;
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
                objWK.MyCommand.Connection.Close();
            }
        }

        public DataSet FetchExistingEnquiry(long nEnqPK, long nCommGrpPK)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            object objCur = new object();
            try
            {
                objWK.OpenConnection();
                objCommand.Connection = objWK.MyConnection;
                var _with3 = objCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".TEMP_ENQ_NEW_BKG_PKG.GET_ENQ";
                var _with4 = objCommand.Parameters;
                _with4.Add("ENQUIRY_PK_IN", nEnqPK).Direction = ParameterDirection.Input;
                _with4.Add("RATE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with4.Add("CONTAINER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with4.Add("OPERATOR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with4.Add("COMMODITY_GROUP_FK_OUT", OracleDbType.Int32).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objCommand;
                objWK.MyDataAdapter.Fill(dsData);
                nCommGrpPK = Convert.ToInt64(objCommand.Parameters["COMMODITY_GROUP_FK_OUT"].Value);
                return dsData;
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
                objWK.MyCommand.Connection.Close();
            }
        }

        //This function is called to fetch the details of existing booking records 
        //Called for Enquiry on Existing Booking
        public DataSet FetchExistingBooking(long nCustPK = 0, string strBookingPK = "", string strJCPK = "", string strPOLPK = "", string strPODPK = "", string strCargoType = "", string strValidFrom = "", string strValidTo = "", long UsrLocFk = 0, string commGroup = "",
        int CustType = 0, int CntrPK = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {

            try
            {
                string strSQL = null;
                string strNewSql = null;
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                string strCondition = null;
                Int32 TotalRecords = default(Int32);
                WorkFlow objWF = new WorkFlow();
                strSQL = "SELECT ROWNUM AS \"SNO\", Q.* FROM";
                strSQL = strSQL + " (SELECT DISTINCT BKG.BOOKING_MST_PK,";
                strSQL = strSQL + "BKG.CUST_CUSTOMER_MST_FK,";
                strSQL = strSQL + "BKG.CONS_CUSTOMER_MST_FK,";
                strSQL = strSQL + "BKGTRN.COMMODITY_MST_FK,";
                strSQL = strSQL + "BKG.PORT_MST_POL_FK,";
                strSQL = strSQL + "BKG.PORT_MST_POD_FK,";
                strSQL = strSQL + "BKG.BOOKING_REF_NO,";
                strSQL = strSQL + "TO_DATE(BKG.BOOKING_DATE,'" + dateFormat + "') AS BOOKING_DATE,";
                strSQL = strSQL + "CASE WHEN SH.CUSTOMER_MST_PK IS NOT NULL THEN";
                strSQL = strSQL + "SH.CUSTOMER_ID";
                strSQL = strSQL + "ELSE (SELECT T.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL T, BOOKING_MST_TBL BT";
                strSQL = strSQL + "WHERE BT.CUST_CUSTOMER_MST_FK = T.CUSTOMER_MST_PK(+)";
                strSQL = strSQL + "AND BT.BOOKING_MST_PK = BKG.BOOKING_MST_PK)";
                strSQL = strSQL + "END AS \"Shipper\",";
                strSQL = strSQL + "CASE WHEN JOB.BOOKING_MST_FK IS NULL THEN";
                strSQL = strSQL + "CON.CUSTOMER_ID";
                strSQL = strSQL + "ELSE (SELECT C.CUSTOMER_ID FROM CUSTOMER_MST_TBL C, JOB_CARD_TRN J ";
                strSQL = strSQL + "WHERE J.CONSIGNEE_CUST_MST_FK=C.CUSTOMER_MST_PK(+)";
                strSQL = strSQL + "AND J.JOB_CARD_TRN_PK=JOB.JOB_CARD_TRN_PK)";
                strSQL = strSQL + "END AS \"Consignee\",";
                strSQL = strSQL + "DECODE(BKG.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') AS \"Cargo_Type\",";
                strSQL = strSQL + "cgm.commodity_group_code AS \"commodity_Group\",";
                strSQL = strSQL + "POL.PORT_ID AS \"POL\",";
                strSQL = strSQL + "POD.PORT_ID AS \"POD\",";
                strSQL = strSQL + "DECODE(BKG.STATUS, -1, 'Nominated', 0, '', 1, 'Provisional', 2, 'Confirmed', 3, 'Cancelled', 6, 'Shipped') AS \"Status\",";
                strSQL = strSQL + "JOB.JOBCARD_REF_NO,";
                strSQL = strSQL + "JOB.JOB_CARD_TRN_PK";
                strSQL = strSQL + "FROM";
                strSQL = strSQL + "BOOKING_MST_TBL BKG,";
                strSQL = strSQL + "BOOKING_TRN BKGTRN,";
                strSQL = strSQL + "JOB_CARD_TRN JOB,";
                strSQL = strSQL + "CUSTOMER_MST_TBL SH,";
                strSQL = strSQL + "CUSTOMER_MST_TBL CON,";
                strSQL = strSQL + "COMMODITY_GROUP_MST_TBL CGM,";
                strSQL = strSQL + "COMMODITY_MST_TBL COMM,";
                strSQL = strSQL + "PORT_MST_TBL POL,USER_MST_TBL UMT,";
                strSQL = strSQL + "PORT_MST_TBL POD,JOB_TRN_CONT JTC";
                strSQL = strSQL + "WHERE";
                strSQL = strSQL + " BKG.BOOKING_MST_PK = BKGTRN.BOOKING_MST_FK";
                strSQL = strSQL + " AND BKG.BOOKING_MST_PK=JOB.BOOKING_MST_FK(+)";
                strSQL = strSQL + " AND BKG.CUST_CUSTOMER_MST_FK=SH.CUSTOMER_MST_PK(+)";
                strSQL = strSQL + " AND BKG.CONS_CUSTOMER_MST_FK=CON.CUSTOMER_MST_PK(+)";
                strSQL = strSQL + " AND BKGTRN.COMMODITY_GROUP_FK=CGM.COMMODITY_GROUP_PK(+)";
                strSQL = strSQL + " AND BKGTRN.COMMODITY_MST_FK=COMM.COMMODITY_MST_PK(+)";
                strSQL = strSQL + " AND BKG.PORT_MST_POL_FK=POL.PORT_MST_PK";
                strSQL = strSQL + " AND BKG.PORT_MST_POD_FK=POD.PORT_MST_PK";
                strSQL = strSQL + " AND JOB.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK(+)";
                strSQL = strSQL + " AND UMT.DEFAULT_LOCATION_FK = " + UsrLocFk + " ";
                if (Convert.ToInt32(commGroup) != 0)
                {
                    strSQL = strSQL + " AND cgm.commodity_group_pk= " + commGroup + " ";
                }
                else
                {
                    strSQL = strSQL;
                }

                strSQL = strSQL + " AND BKG.CREATED_BY_FK = UMT.USER_MST_PK ";

                if (nCustPK != 0)
                {
                    if (CustType == 0)
                    {
                        strSQL = strSQL + " AND nvl(BKG.CUST_CUSTOMER_MST_FK,0)=" + Convert.ToString(nCustPK);
                    }
                    else
                    {
                        strSQL = strSQL + " AND nvl(BKG.CONS_CUSTOMER_MST_FK,NVL(CONSIGNEE_CUST_MST_FK,0))=" + Convert.ToString(nCustPK);
                    }
                }
                if (!string.IsNullOrEmpty(strBookingPK.Trim()) & strBookingPK.Trim() != "0")
                {
                    strSQL = strSQL + " AND BKG.BOOKING_MST_PK=" + strBookingPK.Trim();
                }
                if (!string.IsNullOrEmpty(strJCPK.Trim()) & strJCPK.Trim() != "0")
                {
                    strSQL = strSQL + " AND JOB.JOB_CARD_TRN_PK=" + strJCPK.Trim();
                }
                if (!string.IsNullOrEmpty(strPOLPK.Trim()) & strPOLPK.Trim() != "0")
                {
                    strSQL = strSQL + " AND BKG.PORT_MST_POL_FK=" + strPOLPK.Trim();
                }
                if (!string.IsNullOrEmpty(strPODPK.Trim()) & strPODPK.Trim() != "0")
                {
                    strSQL = strSQL + " AND BKG.PORT_MST_POD_FK=" + strPODPK.Trim();
                }
                DateTime dateValue;
                string[] formats = {"M/d/yyyy h:mm:ss tt", "M/d/yyyy h:mm tt",
                   "MM/dd/yyyy hh:mm:ss", "M/d/yyyy h:mm:ss",
                   "M/d/yyyy hh:mm tt", "M/d/yyyy hh tt",
                   "M/d/yyyy h:mm", "M/d/yyyy h:mm",
                   "MM/dd/yyyy hh:mm", "M/dd/yyyy hh:mm"};

                if (DateTime.TryParseExact(strValidFrom.Trim(), formats, new CultureInfo("en-US"), DateTimeStyles.None, out dateValue))
                {
                    strSQL = strSQL + " AND BKG.BOOKING_DATE>='" + strValidFrom.Trim() + "'";
                }
                if (DateTime.TryParseExact(strValidTo.Trim(), formats, new CultureInfo("en-US"), DateTimeStyles.None, out dateValue))
                {
                    strSQL = strSQL + " AND BKG.BOOKING_DATE<='" + strValidTo.Trim() + "'";
                }
                if (!string.IsNullOrEmpty(strCargoType.Trim()) & strCargoType.Trim() != "0")
                {
                    strSQL = strSQL + " AND BKG.CARGO_TYPE=" + strCargoType.Trim();
                }
                if (CntrPK != 0)
                {
                    strSQL = strSQL + " AND JTC.JOB_TRN_CONT_PK=" + CntrPK;
                }
                strSQL = strSQL + " AND BKG.BUSINESS_TYPE = 2 ";
                strSQL = strSQL + "ORDER BY TO_DATE(BOOKING_DATE,'" + dateFormat + "') DESC,BKG.BOOKING_REF_NO DESC) Q  ";
                // strSQL &= " WHERE SNO  BETWEEN " & start & " AND " & last

                string strCount = null;
                strCount = " SELECT COUNT(*) FROM (  ";
                strCount += strSQL + ") ";
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount));
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
                string str = null;
                str = " SELECT * FROM ( ";
                strSQL = str + strSQL + " ) ";

                strSQL += " WHERE SNO  BETWEEN " + start + " AND " + last;

                return (new WorkFlow()).GetDataSet(strSQL);
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

        #region "Save"

        //This function is called to save the enquiry
        //Called for Enquiry on New Booking
        public ArrayList Save(DataSet hdrDS, DataSet trnDS, long nConfigPK, string remarks = "", Int16 isCarted = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            bool chkFlag = false;

            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();


            try
            {
                var _with5 = insCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_SEA_TBL_PKG.ENQUIRY_BKG_SEA_TBL_INS";

                _with5.Parameters.Add("ENQUIRY_REF_NO_IN", OracleDbType.Varchar2, 50, "ENQUIRY_REF_NO").Direction = ParameterDirection.Input;
                _with5.Parameters["ENQUIRY_REF_NO_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("ENQUIRY_DATE_IN", OracleDbType.Date, 10, "ENQUIRY_DATE").Direction = ParameterDirection.Input;
                _with5.Parameters["ENQUIRY_DATE_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
                _with5.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("CUSTOMER_CATEGORY_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_CATEGORY_FK").Direction = ParameterDirection.Input;
                _with5.Parameters["CUSTOMER_CATEGORY_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                _with5.Parameters["CARGO_TYPE_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("EXECUTED_BY_IN", OracleDbType.Int32, 10, "EXECUTED_BY").Direction = ParameterDirection.Input;
                _with5.Parameters["EXECUTED_BY_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("SHIPMENT_DATE_IN", OracleDbType.Date, 10, "SHIPMENT_DATE").Direction = ParameterDirection.Input;
                _with5.Parameters["SHIPMENT_DATE_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("CUST_TYPE_IN", OracleDbType.Int32, 1, "CUST_TYPE").Direction = ParameterDirection.Input;
                _with5.Parameters["CUST_TYPE_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("CREATED_BY_FK_IN", OracleDbType.Int32, 10, "CREATED_BY_FK").Direction = ParameterDirection.Input;
                _with5.Parameters["CREATED_BY_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("REMARKS_IN", getDefault(remarks, DBNull.Value)).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("BASE_CURRENCY_FK_IN", OracleDbType.Int32, 10, "BASE_CURRENCY_FK").Direction = ParameterDirection.Input;

                _with5.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with5.Parameters.Add("ISCARTED_IN", isCarted).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ENQUIRY_BKG_SEA_PK").Direction = ParameterDirection.Output;
                _with5.Parameters["RETURN_VALUE"].SourceVersion  = DataRowVersion.Current;

                var _with6 = updCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_SEA_TBL_PKG.ENQUIRY_BKG_SEA_TBL_UPD";

                _with6.Parameters.Add("ENQUIRY_BKG_SEA_PK_IN", OracleDbType.Int32, 10, "ENQUIRY_BKG_SEA_PK").Direction = ParameterDirection.Input;
                _with6.Parameters["ENQUIRY_BKG_SEA_PK_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("ENQUIRY_REF_NO_IN", OracleDbType.Varchar2, 50, "ENQUIRY_REF_NO").Direction = ParameterDirection.Input;
                _with6.Parameters["ENQUIRY_REF_NO_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("ENQUIRY_DATE_IN", OracleDbType.Date, 10, "ENQUIRY_DATE").Direction = ParameterDirection.Input;
                _with6.Parameters["ENQUIRY_DATE_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
                _with6.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("CUSTOMER_CATEGORY_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_CATEGORY_FK").Direction = ParameterDirection.Input;
                _with6.Parameters["CUSTOMER_CATEGORY_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                _with6.Parameters["CARGO_TYPE_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("EXECUTED_BY_IN", OracleDbType.Int32, 10, "EXECUTED_BY").Direction = ParameterDirection.Input;
                _with6.Parameters["EXECUTED_BY_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("SHIPMENT_DATE_IN", OracleDbType.Date, 10, "SHIPMENT_DATE").Direction = ParameterDirection.Input;
                _with6.Parameters["SHIPMENT_DATE_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("CUST_TYPE_IN", OracleDbType.Int32, 1, "CUST_TYPE").Direction = ParameterDirection.Input;
                _with6.Parameters["CUST_TYPE_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("LAST_MODIFIED_BY_FK_IN", OracleDbType.Int32, 10, "LAST_MODIFIED_BY_FK").Direction = ParameterDirection.Input;
                _with6.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
                _with6.Parameters["VERSION_NO_IN"].SourceVersion  = DataRowVersion.Current;
                _with6.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("REMARKS_IN", getDefault(remarks, DBNull.Value)).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("ISCARTED_IN", isCarted).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ENQUIRY_BKG_SEA_PK").Direction = ParameterDirection.Output;
                _with6.Parameters["RETURN_VALUE"].SourceVersion  = DataRowVersion.Current;

                
                arrMessage.Clear();
                TRAN = objWK.MyConnection.BeginTransaction();

                if ((hdrDS.GetChanges(DataRowState.Added) != null))
                {
                    chkFlag = true;
                }
                else
                {
                    chkFlag = false;
                }

                var _with7 = objWK.MyDataAdapter;

                _with7.InsertCommand = insCommand;
                _with7.InsertCommand.Transaction = TRAN;
                _with7.UpdateCommand = updCommand;
                _with7.UpdateCommand.Transaction = TRAN;

                RecAfct = _with7.Update(hdrDS.Tables[0]);
                if (RecAfct > 0)
                {
                    SaveTrn(trnDS, TRAN, Convert.ToInt32(hdrDS.Tables[0].Rows[0][0]));
                }
                //'

                //'
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    if (chkFlag)
                    {
                        RollbackProtocolKey("ENQUIRY_REF_NO", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(hdrDS.Tables[0].Rows[0]["ENQUIRY_REF_NO"]), System.DateTime.Now);
                        chkFlag = false;
                    }
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                }

            }
            catch (OracleException oraexp)
            {
                if ((TRAN != null))
                {
                    if (TRAN.Connection.State == ConnectionState.Open)
                    {
                        TRAN.Rollback();
                        TRAN = null;
                    }
                    if (chkFlag)
                    {
                        RollbackProtocolKey("ENQUIRY_REF_NO", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(hdrDS.Tables[0].Rows[0]["ENQUIRY_REF_NO"]), System.DateTime.Now);
                        chkFlag = false;
                    }
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                if ((TRAN != null))
                {
                    if (TRAN.Connection.State == ConnectionState.Open)
                    {
                        TRAN.Rollback();
                        TRAN = null;
                    }
                    if (chkFlag)
                    {
                        RollbackProtocolKey("ENQUIRY_REF_NO", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(hdrDS.Tables[0].Rows[0]["ENQUIRY_REF_NO"]), System.DateTime.Now);
                        chkFlag = false;
                    }
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
                string CurrFKs = "0";
                System.DateTime ContractDt = default(System.DateTime);
                cls_Operator_Contract objContract = new cls_Operator_Contract();
                ContractDt = Convert.ToDateTime(hdrDS.Tables[0].Rows[0]["ENQUIRY_DATE"]);
                for (int nRowCnt = 0; nRowCnt <= trnDS.Tables[1].Rows.Count - 1; nRowCnt++)
                {
                    CurrFKs += "," + trnDS.Tables[1].Rows[nRowCnt]["CURRENCY_MST_FK"];
                }
                UpdateTempExRate(Convert.ToInt64(hdrDS.Tables[0].Rows[0][0]), CurrFKs, ContractDt, "SEAENQUIRY");
            }
            return arrMessage;
        }

        //This function is called to save the enquiry transaction details
        //Called for Enquiry on New Booking
        private void SaveTrn(DataSet trnDS, OracleTransaction TRAN, long nEnqMainSeaPK)
        {
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();


            try
            {
                objWK.MyConnection = TRAN.Connection;
                var _with8 = delCommand;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.Text;
                _with8.Transaction = TRAN;
                _with8.CommandText = "DELETE FROM ENQUIRY_TRN_SEA_FCL_LCL WHERE ENQUIRY_MAIN_SEA_FK=" + Convert.ToString(nEnqMainSeaPK);
                _with8.ExecuteNonQuery();
                var _with9 = insCommand;
                _with9.Connection = objWK.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_SEA_TBL_PKG.ENQUIRY_TRN_SEA_FCL_LCL_INS";

                _with9.Parameters.Add("ENQUIRY_MAIN_SEA_FK_IN", nEnqMainSeaPK).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POL_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["PORT_MST_POL_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POD_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["PORT_MST_POD_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("TRANS_REFERED_FROM_IN", OracleDbType.Int32, 10, "TRANS_REFERED_FROM").Direction = ParameterDirection.Input;
                _with9.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("TRANS_REF_NO_IN", OracleDbType.Varchar2, 50, "TRANS_REF_NO").Direction = ParameterDirection.Input;
                _with9.Parameters["TRANS_REF_NO_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["OPERATOR_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CONTAINER_TYPE_MST_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("EXPECTED_BOXES_IN", OracleDbType.Int32, 10, "EXPECTED_BOXES").Direction = ParameterDirection.Input;
                _with9.Parameters["EXPECTED_BOXES_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("EXPECTED_WEIGHT_IN", OracleDbType.Int32, 10, "EXPECTED_WEIGHT").Direction = ParameterDirection.Input;
                _with9.Parameters["EXPECTED_WEIGHT_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("EXPECTED_VOLUME_IN", OracleDbType.Int32, 10, "EXPECTED_VOLUME").Direction = ParameterDirection.Input;
                _with9.Parameters["EXPECTED_VOLUME_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("EXPECTED_SHIPMENT_IN", OracleDbType.Date, 10, "EXPECTED_SHIPMENT").Direction = ParameterDirection.Input;
                _with9.Parameters["EXPECTED_SHIPMENT_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "COMMODITY_MST_FK").Direction = ParameterDirection.Input;
                _with9.Parameters["COMMODITY_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("ALL_IN_TARIFF_IN", OracleDbType.Int32, 10, "ALL_IN_TARIFF").Direction = ParameterDirection.Input;
                _with9.Parameters["ALL_IN_TARIFF_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "BASIS").Direction = ParameterDirection.Input;
                _with9.Parameters["BASIS_IN"].SourceVersion  = DataRowVersion.Current;
                _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ENQUIRY_TRN_SEA_PK").Direction = ParameterDirection.Output;
                _with9.Parameters["RETURN_VALUE"].SourceVersion  = DataRowVersion.Current;

                var _with10 = updCommand;
                _with10.Connection = TRAN.Connection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_SEA_TBL_PKG.ENQUIRY_TRN_SEA_FCL_LCL_UPD";

                _with10.Parameters.Add("ENQUIRY_TRN_SEA_PK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_SEA_PK").Direction = ParameterDirection.Input;
                _with10.Parameters["ENQUIRY_TRN_SEA_PK_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("ENQUIRY_MAIN_SEA_FK_IN", OracleDbType.Int32, 10, "ENQUIRY_MAIN_SEA_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["ENQUIRY_MAIN_SEA_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POL_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["PORT_MST_POL_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POD_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["PORT_MST_POD_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("TRANS_REFERED_FROM_IN", OracleDbType.Int32, 10, "TRANS_REFERED_FROM").Direction = ParameterDirection.Input;
                _with10.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("TRANS_REF_NO_IN", OracleDbType.Varchar2, 50, "TRANS_REF_NO").Direction = ParameterDirection.Input;
                _with10.Parameters["TRANS_REF_NO_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["OPERATOR_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CONTAINER_TYPE_MST_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("EXPECTED_BOXES_IN", OracleDbType.Int32, 10, "EXPECTED_BOXES").Direction = ParameterDirection.Input;
                _with10.Parameters["EXPECTED_BOXES_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("EXPECTED_WEIGHT_IN", OracleDbType.Int32, 10, "EXPECTED_WEIGHT").Direction = ParameterDirection.Input;
                _with10.Parameters["EXPECTED_WEIGHT_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("EXPECTED_VOLUME_IN", OracleDbType.Int32, 10, "EXPECTED_VOLUME_CBM").Direction = ParameterDirection.Input;
                _with10.Parameters["EXPECTED_VOLUME_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("EXPECTED_SHIPMENT_IN", OracleDbType.Date, 10, "EXPECTED_SHIPMENT").Direction = ParameterDirection.Input;
                _with10.Parameters["EXPECTED_SHIPMENT_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "COMMODITY_MST_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["COMMODITY_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("ALL_IN_TARIFF_IN", OracleDbType.Int32, 10, "ALL_IN_TARIFF").Direction = ParameterDirection.Input;
                _with10.Parameters["ALL_IN_TARIFF_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "BASIS").Direction = ParameterDirection.Input;
                _with10.Parameters["BASIS_IN"].SourceVersion  = DataRowVersion.Current;
                _with10.Parameters.Add("RETURN_VALUE", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with10.Parameters["RETURN_VALUE"].SourceVersion  = DataRowVersion.Current;
                var _with11 = objWK.MyDataAdapter;
                _with11.InsertCommand = insCommand;
                _with11.InsertCommand.Transaction = TRAN;
                _with11.UpdateCommand = updCommand;
                _with11.UpdateCommand.Transaction = TRAN;
                RecAfct = _with11.Update(trnDS.Tables[0]);
                if (trnDS.Tables[0].Rows.Count == RecAfct)
                {
                    var _with12 = insCommand;
                    _with12.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_SEA_TBL_PKG.ENQUIRY_TRN_SEA_FRT_DTLS_INS";
                    _with12.Parameters.Clear();
                    _with12.Parameters.Add("ENQUIRY_TRN_SEA_FK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_SEA_FK").Direction = ParameterDirection.Input;
                    _with12.Parameters["ENQUIRY_TRN_SEA_FK_IN"].SourceVersion  = DataRowVersion.Current;
                    _with12.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                    _with12.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                    _with12.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", OracleDbType.Int32, 1, "CHECK_FOR_ALL_IN_RT").Direction = ParameterDirection.Input;
                    _with12.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion  = DataRowVersion.Current;
                    _with12.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    _with12.Parameters["CURRENCY_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                    _with12.Parameters.Add("TARIFF_RATE_IN", OracleDbType.Varchar2, 50, "TARIFF_RATE").Direction = ParameterDirection.Input;
                    _with12.Parameters["TARIFF_RATE_IN"].SourceVersion  = DataRowVersion.Current;
                    _with12.Parameters.Add("TARIFF_MIN_RATE_IN", OracleDbType.Varchar2, 50, "MIN_RATE").Direction = ParameterDirection.Input;
                    //Added by Rabbani raeson USS Gap,introduced New column as "Min.Rate"
                    _with12.Parameters["TARIFF_MIN_RATE_IN"].SourceVersion  = DataRowVersion.Current;
                    //Added by Faheem
                    _with12.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 10, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                    _with12.Parameters["CHARGE_BASIS_IN"].SourceVersion  = DataRowVersion.Current;
                    //End
                    _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ENQUIRY_TRN_SEA_FRT_PK").Direction = ParameterDirection.Output;
                    _with12.Parameters["RETURN_VALUE"].SourceVersion  = DataRowVersion.Current;

                    var _with13 = updCommand;
                    _with13.CommandText = objWK.MyUserName + ".ENQUIRY_BKG_SEA_TBL_PKG.ENQUIRY_TRN_SEA_FRT_DTLS_UPD";
                    _with13.Parameters.Clear();
                    _with13.Parameters.Add("ENQUIRY_TRN_SEA_FRT_PK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_SEA_FRT_PK").Direction = ParameterDirection.Input;
                    _with13.Parameters["ENQUIRY_TRN_SEA_FRT_PK_IN"].SourceVersion  = DataRowVersion.Current;
                    _with13.Parameters.Add("ENQUIRY_TRN_SEA_FK_IN", OracleDbType.Int32, 10, "ENQUIRY_TRN_SEA_FK").Direction = ParameterDirection.Input;
                    _with13.Parameters["ENQUIRY_TRN_SEA_FK_IN"].SourceVersion  = DataRowVersion.Current;
                    _with13.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                    _with13.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                    _with13.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", OracleDbType.Int32, 1, "CHECK_FOR_ALL_IN_RT").Direction = ParameterDirection.Input;
                    _with13.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion  = DataRowVersion.Current;
                    _with13.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    _with13.Parameters["CURRENCY_MST_FK_IN"].SourceVersion  = DataRowVersion.Current;
                    _with13.Parameters.Add("TARIFF_RATE_IN", OracleDbType.Varchar2, 50, "TARIFF_RATE").Direction = ParameterDirection.Input;
                    _with13.Parameters["TARIFF_RATE_IN"].SourceVersion  = DataRowVersion.Current;
                    _with13.Parameters.Add("TARIFF_MIN_RATE_IN", OracleDbType.Varchar2, 50, "MIN_RATE").Direction = ParameterDirection.Input;
                    //Added by Rabbani raeson USS Gap,introduced New column as "Min.Rate"
                    _with13.Parameters["TARIFF_MIN_RATE_IN"].SourceVersion  = DataRowVersion.Current;
                    //Added by Faheem
                    _with13.Parameters.Add("CHARGE_BASIS_IN", OracleDbType.Int32, 10, "CHARGE_BASIS").Direction = ParameterDirection.Input;
                    _with13.Parameters["CHARGE_BASIS_IN"].SourceVersion  = DataRowVersion.Current;
                    //End
                    _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ENQUIRY_TRN_SEA_FRT_PK").Direction = ParameterDirection.Output;
                    _with13.Parameters["RETURN_VALUE"].SourceVersion  = DataRowVersion.Current;
                    var _with14 = objWK.MyDataAdapter;
                    _with14.InsertCommand = insCommand;
                    _with14.InsertCommand.Transaction = TRAN;
                    _with14.UpdateCommand = updCommand;
                    _with14.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with14.Update(trnDS.Tables[1]);
                    if (RecAfct != trnDS.Tables[1].Rows.Count)
                    {
                        arrMessage.Add("Save not successful");
                    }
                }
                else
                {
                    arrMessage.Add("Save not successful");
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
            }
        }

        #region "Update Temp ExchangeRate"
        public ArrayList UpdateTempExRate(long PkValue, string CurrFks, System.DateTime FromDt, string FromFlag = "")
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            arrMessage.Clear();
            try
            {
                var _with15 = objWK.MyCommand;
                _with15.Connection = objWK.MyConnection;
                _with15.CommandType = CommandType.StoredProcedure;
                _with15.CommandText = objWK.MyUserName + ".CONT_MAIN_SEA_TBL_PKG.TEMP_EX_RATE_TRN_UPD";
                _with15.Parameters.Clear();
                _with15.Parameters.Add("REF_FK_IN", PkValue).Direction = ParameterDirection.Input;
                _with15.Parameters["REF_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with15.Parameters.Add("BASE_CURRENCY_FK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with15.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion  = DataRowVersion.Current;
                _with15.Parameters.Add("CURRENCY_FKS_IN", CurrFks).Direction = ParameterDirection.Input;
                _with15.Parameters["CURRENCY_FKS_IN"].SourceVersion  = DataRowVersion.Current;
                _with15.Parameters.Add("FROM_DATE_IN", FromDt).Direction = ParameterDirection.Input;
                _with15.Parameters["FROM_DATE_IN"].SourceVersion  = DataRowVersion.Current;
                _with15.Parameters.Add("FROM_FLAG_IN", FromFlag).Direction = ParameterDirection.Input;
                _with15.Parameters["FROM_FLAG_IN"].SourceVersion  = DataRowVersion.Current;
                _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with15.ExecuteNonQuery();
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
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion

        //This function is called to generate the enquiry reference no.
        //Called for Enquiry on New Booking
        public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
        {
            return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
        }

        //This function is called to save the new customer details
        //Called for Enquiry on New Booking
        public ArrayList SaveCustomer(string strID, string strName, string strAddress, string strPhone, string strFax, string strTitle, string strContact, string strEmail, long nBizType)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            string strSQLDet = null;
            string strPK = null;
            OracleCommand objCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            int nRecAfct = 0;

            strSQL = "INSERT INTO TEMP_CUSTOMER_TBL";
            strSQL = strSQL + " (";
            strSQL = strSQL + "CUSTOMER_MST_PK,";
            strSQL = strSQL + "CUSTOMER_ID,";
            strSQL = strSQL + "CUSTOMER_NAME,";
            strSQL = strSQL + "ACTIVE_FLAG,";
            strSQL = strSQL + "BUSINESS_TYPE,";
            strSQL = strSQL + "CREATED_BY_FK";
            strSQL = strSQL + ") ";
            strSQL = strSQL + "VALUES (";
            strSQL = strSQL + "SEQ_TEMP_CUSTOMER_TBL.NEXTVAL,";
            strSQL = strSQL + "'" + strID + "',";
            strSQL = strSQL + "'" + strName + "',";
            strSQL = strSQL + "1,";
            strSQL = strSQL + Convert.ToString(nBizType);
            strSQL = strSQL + "," + CREATED_BY;
            strSQL = strSQL + ")";

            strSQLDet = "INSERT INTO TEMP_CUSTOMER_CONTACT_DTLS";
            strSQLDet = strSQLDet + " (";
            strSQLDet = strSQLDet + "CUSTOMER_MST_FK,";
            strSQLDet = strSQLDet + "ADM_ADDRESS_1,";
            strSQLDet = strSQLDet + "ADM_SALUTATION,";
            strSQLDet = strSQLDet + "ADM_CONTACT_PERSON,";
            strSQLDet = strSQLDet + "ADM_PHONE_NO_1,";
            strSQLDet = strSQLDet + "ADM_FAX_NO,";
            strSQLDet = strSQLDet + "ADM_EMAIL_ID";
            strSQLDet = strSQLDet + ") ";
            strSQLDet = strSQLDet + "VALUES (";
            strSQLDet = strSQLDet + "SEQ_TEMP_CUSTOMER_TBL.CURRVAL,";
            strSQLDet = strSQLDet + "'" + strAddress + "',";
            strSQLDet = strSQLDet + strTitle + ",";
            strSQLDet = strSQLDet + "'" + strContact + "',";
            strSQLDet = strSQLDet + "'" + strPhone + "',";
            strSQLDet = strSQLDet + "'" + strFax + "',";
            strSQLDet = strSQLDet + "'" + strEmail + "'";
            strSQLDet = strSQLDet + ")";

            try
            {
                objWK.OpenConnection();
                var _with16 = objCommand;
                _with16.Connection = objWK.MyConnection;
                _with16.CommandType = CommandType.Text;
                _with16.CommandText = strSQL;
                arrMessage.Clear();
                TRAN = objWK.MyConnection.BeginTransaction();
                objCommand.Transaction = TRAN;
                nRecAfct = objCommand.ExecuteNonQuery();
                if (nRecAfct > 0)
                {
                    objCommand.CommandText = strSQLDet;
                    nRecAfct = objCommand.ExecuteNonQuery();
                    if (nRecAfct > 0)
                    {
                        TRAN.Commit();
                        arrMessage.Add("Customer saved successfully");
                        strPK = Convert.ToString(objWK.ExecuteScaler("SELECT MAX(CUSTOMER_MST_PK) FROM TEMP_CUSTOMER_TBL"));
                        arrMessage.Add(strPK);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        arrMessage.Add("Save not successful");
                        return arrMessage;
                    }
                }
                else
                {
                    TRAN.Rollback();
                    arrMessage.Add("Save not successful");
                    return arrMessage;
                }
            }
            catch (Exception ex)
            {
                if ((TRAN != null))
                {
                    if (TRAN.Connection.State == ConnectionState.Open)
                    {
                        TRAN.Rollback();
                        TRAN = null;
                    }
                }
                arrMessage.Add(ex.Message);
                return arrMessage;
            }           
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion

        #region "Fetch Hdr details,Active Sector,customer and user details"

        //This function is called to fetch the header details of a given enquiry
        public DataSet FetchHDR(long nEnqPK)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT * FROM ENQUIRY_BKG_SEA_TBL WHERE ENQUIRY_BKG_SEA_PK=" + Convert.ToString(nEnqPK);

                return (new WorkFlow()).GetDataSet(strSQL);
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

        public string FetchCurrencyID(long Currpk)
        {
            try
            {
                string strSQL = null;
                strSQL = "select CURR.CURRENCY_ID from currency_type_mst_tbl CURR WHERE CURR.CURRENCY_MST_PK = " + Currpk;

                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        //Function to fetch active sea sectors
        public DataTable FetchActiveSector(long LocationPk, string strPOLPk = "", string strPODPk = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;

            arrPolPk = strPOLPk.Split(',');
            arrPodPk = strPODPk.Split(',');

            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }
            strSQL = "";


            strSQL = "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, " + "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK " + "FROM SECTOR_MST_TBL SMT, " + "PORT_MST_TBL POL, " + "PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM " + "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + "AND   LPM.PORT_MST_FK = POL.PORT_MST_PK " + "AND   POL.BUSINESS_TYPE = 2 " + "AND   POD.BUSINESS_TYPE = 2 " + "AND   LPM.LOCATION_MST_FK =" + LocationPk + "AND   SMT.ACTIVE = 1 " + "AND   SMT.BUSINESS_TYPE = 2 " + "ORDER BY CHK DESC,POL";
            try
            {
                return objWF.GetDataTable(strSQL);
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

        public DataSet FetchCustomer(long nCustPK, long nCustType)
        {
            try
            {
                string strSQL = null;
                if (nCustType == 0)
                {
                    strSQL = "select c.customer_mst_pk , c.Customer_ID, c.Customer_Name ,";
                    strSQL += "cdtl.adm_address_1 || '$' || cdtl.adm_address_2 || '$' || cdtl.adm_address_3 || '$' || cdtl.adm_city || '-' || cdtl.adm_zip_code || '$' || cmt.country_name as address,";
                    strSQL += "cdtl.adm_contact_person,cdtl.adm_phone_no_1,cdtl.adm_fax_no,cdtl.adm_email_id,cdtl.adm_salutation ";
                    strSQL += "from customer_mst_tbl c, customer_contact_dtls cdtl, country_mst_tbl cmt";
                    strSQL += "where";
                    strSQL += "c.customer_mst_pk=cdtl.customer_mst_fk";
                    strSQL += "and cdtl.adm_country_mst_fk=cmt.country_mst_pk (+)";
                    strSQL += "and c.customer_mst_pk=" + Convert.ToString(nCustPK);
                }
                else
                {
                    strSQL = "select c.customer_mst_pk , c.Customer_ID, c.Customer_Name ,";
                    strSQL += "cdtl.adm_address_1 || '$' || cdtl.adm_address_2 || '$' || cdtl.adm_address_3 || '$' || cdtl.adm_zip_code as address,";
                    strSQL += "cdtl.adm_contact_person,cdtl.adm_phone_no_1,cdtl.adm_fax_no,cdtl.adm_email_id,cdtl.adm_salutation  ";
                    strSQL += "from TEMP_CUSTOMER_TBL c, TEMP_CUSTOMER_CONTACT_DTLS cdtl";
                    strSQL += "where";
                    strSQL += "c.customer_mst_pk=cdtl.customer_mst_fk";
                    strSQL += "and c.customer_mst_pk=" + Convert.ToString(nCustPK);
                }
                return (new WorkFlow()).GetDataSet(strSQL);
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

        public string FetchUser(long nUserPK)
        {
            try
            {
                string strSQL = null;
                strSQL = "select user_name from user_mst_tbl usr where usr.user_mst_pk=" + Convert.ToString(nUserPK);
                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        #region "Enhance Search"
        public string GetBooking(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strLoc = null;
            string strCust = null;
            string strFromDt = null;
            string strToDt = null;
            string strJCPk = null;
            string strJCNr = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strLoc = Convert.ToString(arr.GetValue(3));
            strCust = Convert.ToString(arr.GetValue(4));
            string strFCL = null;
            if (arr.Length > 5)
            {
                strFCL = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                strFromDt = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                strToDt = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                strJCPk = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                strJCNr = Convert.ToString(arr.GetValue(9));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_ENQUIRY_SEARCH";

                var _with17 = selectCommand.Parameters;
                _with17.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with17.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with17.Add("LOCATION_MST_FK_IN", strLoc).Direction = ParameterDirection.Input;
                _with17.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with17.Add("CUST_PK_IN", strCust).Direction = ParameterDirection.Input;
                //Manoharan 09July2007: to filter cargo based Booking
                _with17.Add("IS_FCL_IN", ((strFCL == null) ? "" : strFCL)).Direction = ParameterDirection.Input;
                _with17.Add("FROMDT_IN", ((strFromDt == null) ?"" : strFromDt)).Direction = ParameterDirection.Input;
                _with17.Add("TODT_IN", ((strToDt == null) ? "" : strToDt)).Direction = ParameterDirection.Input;
                _with17.Add("JC_PK_IN", ((strJCPk == null) ? "" : strJCPk)).Direction = ParameterDirection.Input;
                _with17.Add("JC_NR_IN", ((strJCNr == null) ? "" : strJCNr)).Direction = ParameterDirection.Input;
                _with17.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        public string GetContainerNumber(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strJC = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strJC = Convert.ToString(arr.GetValue(3));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_CONT_NO";

                var _with18 = selectCommand.Parameters;
                _with18.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with18.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with18.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with18.Add("JOB_CARD_PK_IN", (!string.IsNullOrEmpty(strJC) ? strJC : "")).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion

        #region "ViewSchedule"
        public object FetchSchedule(long POLPK, long PODPK, long CarrierPK, int Flag, string DEPDATE, string DEPFROMDATE, string DEPTODATE, string ARRDATE, string ARRFROMDATE, string ARRTODATE,
        string TransitFrom, string TransitTo, string Day, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            System.Text.StringBuilder Str = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataSet Ds = new DataSet();
            string StrSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string SQL = null;
            try
            {
                Str.Append(" SELECT ROWNUM SR_NO,q.* FROM(select SL.OPERATOR_ID AS Operator,VSL.VESSEL_NAME CARRIER,");
                Str.Append("VSL_TRN.VOYAGE VOYAGENUMBER,");
                Str.Append("VSL_TRN.POL_CUT_OFF_DATE ,");
                Str.Append("VSL_TRN.POL_ETD DEPARTURE,");
                Str.Append("TO_CHAR(VSL_TRN.POL_ETD,'DY') ,");
                Str.Append(" ROUND(VSL_TRN.POD_ETA-VSL_TRN.POL_ETD) TARNSIT_TIME,");
                Str.Append("VSL_TRN.POD_ETA ARRIVAL,");
                Str.Append("TO_CHAR(VSL_TRN.POD_ETA,'DY'), ");
                Str.Append(" 'Book >' BOOKING_BT, ");
                Str.Append(" VSL.VESSEL_VOYAGE_TBL_PK, ");
                Str.Append(" VSL_TRN.VOYAGE_TRN_PK,SL.OPERATOR_MST_PK ");
                Str.Append(" from ");


                Str.Append(" VESSEL_VOYAGE_TBL VSL,");
                Str.Append(" VESSEL_VOYAGE_TRN VSL_TRN ");
                Str.Append(" , OPERATOR_MST_TBL SL ");
                Str.Append(" , VESSEL_VOYAGE_TRN_TP VVTT ");
                Str.Append(" WHERE ");
                Str.Append("VSL.VESSEL_VOYAGE_TBL_PK=VSL_TRN.VESSEL_VOYAGE_TBL_FK");
                Str.Append(" AND vvtt.voyage_trn_fk(+) = VSL_TRN.voyage_trn_pk ");
                Str.Append(" AND (VSL_TRN.PORT_MST_POL_FK= " + POLPK + " ");
                Str.Append(" OR VVTT.PORT_MST_TP_PORT_FK = " + POLPK + ") ");
                Str.Append(" AND (VSL_TRN.PORT_MST_POD_FK= " + PODPK + " ");
                Str.Append(" OR VVTT.PORT_MST_TP_PORT_FK = " + PODPK + " )");
                if (CarrierPK != 0)
                {
                    Str.Append(" AND SL.OPERATOR_MST_PK = " + CarrierPK + " ");
                }
                Str.Append(" AND SL.OPERATOR_MST_PK=VSL.OPERATOR_MST_FK ");

                if (Flag == 1)
                {
                    Str.Append(" AND TO_DATE(VSL_TRN.POL_ETD,'DD/MM/YYYY') = '" + DEPDATE + "'");
                }
                else if (Flag == 2)
                {
                    Str.Append(" AND TO_DATE(VSL_TRN.POL_ETD,'DD/MM/YYYY') < '" + DEPDATE + "'");
                }
                else if (Flag == 3)
                {
                    Str.Append(" AND TO_DATE(VSL_TRN.POL_ETD,'DD/MM/YYYY') > '" + DEPDATE + "'");
                }
                else if (Flag == 4)
                {
                    if (!string.IsNullOrEmpty(DEPTODATE))
                    {
                        Str.Append(" AND TO_DATE(VSL_TRN.POL_ETD,'DD/MM/YYYY') BETWEEN '" + DEPFROMDATE + "' AND '" + DEPTODATE + "' ");
                    }
                    else
                    {
                        Str.Append(" AND TO_DATE(VSL_TRN.POL_ETD,'DD/MM/YYYY') > '" + DEPFROMDATE + "' ");
                    }
                }
                else if (Flag == 5)
                {
                    Str.Append(" AND TO_DATE(VSL_TRN.POD_ETA,'DD/MM/YYYY') = '" + ARRDATE + "'");
                }
                else if (Flag == 6)
                {
                    Str.Append(" AND TO_DATE(VSL_TRN.POD_ETA,'DD/MM/YYYY') < '" + ARRDATE + "'");
                }
                else if (Flag == 7)
                {
                    Str.Append(" AND TO_DATE(VSL_TRN.POD_ETA,'DD/MM/YYYY') > '" + ARRDATE + "'");
                }
                else if (Flag == 8)
                {
                    if (!string.IsNullOrEmpty(ARRTODATE))
                    {
                        Str.Append(" AND TO_DATE(VSL_TRN.POD_ETA,'DD/MM/YYYY') BETWEEN '" + ARRFROMDATE + "' AND '" + ARRTODATE + "' ");
                    }
                    else
                    {
                        Str.Append(" AND TO_DATE(VSL_TRN.POD_ETA,'DD/MM/YYYY') > '" + ARRFROMDATE + "' ");
                    }
                }
                if (TransitFrom.Length > 0 & TransitTo.Length > 0)
                {
                    Str.Append(" AND ROUND(VSL_TRN.POD_ETA - VSL_TRN.POL_ETD) BETWEEN " + TransitFrom + " AND " + TransitTo + " ");
                }
                else if (TransitFrom.Length > 0 & TransitTo.Length == 0)
                {
                    Str.Append(" AND ROUND(VSL_TRN.POD_ETA - VSL_TRN.POL_ETD) > " + TransitFrom + " ");
                }
                if (Day != null)
                {
                    if (Flag <= 4)
                    {
                        Str.Append(" AND TO_CHAR(VSL_TRN.POL_ETD, 'DY') in (" + Day + " )");
                    }
                    else
                    {
                        Str.Append(" AND TO_CHAR(VSL_TRN.POD_ETA, 'DY') in (" + Day + " )");
                    }
                }
                Str.Append(" order by DEPARTURE ASC )q ");
                StrSQL = " SELECT COUNT(*) FROM ( " + Str.ToString() + " )";
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSQL));
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
                SQL = "SELECT * FROM (" + Str.ToString() + ") WHERE SR_NO  Between " + start + " and " + last;
                Ds = objWF.GetDataSet(SQL);
                return Ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }           
        }
        #endregion

        public string FetchContract(int Polpk = 0, int Podpk = 0)
        {
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            buildCondition.Append("Select count(*) from (select main.contract_no ");
            buildCondition.Append(" from CONT_MAIN_SEA_TBL MAIN, CONT_TRN_SEA_FCL_LCL f ");
            buildCondition.Append("  where f.cont_main_sea_fk = MAIN.cont_main_sea_pk ");

            buildCondition.Append("AND ((TO_DATE(SYSDATE , '" + dateFormat + "') BETWEEN");
            buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR");
            buildCondition.Append("    (TO_DATE(SYSDATE , '" + dateFormat + "') BETWEEN");
            buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR");
            buildCondition.Append("    (MAIN.VALID_TO IS NULL))");
            buildCondition.Append(" AND main.ACTIVE = 1 AND main.CONT_APPROVED = 1 ");
            buildCondition.Append(" AND ( ");
            buildCondition.Append("        main.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ");
            buildCondition.Append("       OR main.VALID_TO IS NULL ");
            buildCondition.Append("     ) ");
            buildCondition.Append("   AND f.port_mst_pol_fk=" + Polpk);
            buildCondition.Append("   AND f.port_mst_pod_fk=" + Podpk);
            buildCondition.Append("     ) ");
            try
            {
                return objWF.ExecuteScaler(buildCondition.ToString());
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
    }
}