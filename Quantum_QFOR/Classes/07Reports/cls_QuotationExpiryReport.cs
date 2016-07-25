#region "Comments"

//'***************************************************************************************************************
//'*  Company Name:
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By  :    Santosh on 31-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified DateTime(DD-MON-YYYY)  Modified By     Remarks (Bugs Related)
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
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsQuotationExpiryReport : CommonFeatures
    {
        /// <summary>
        /// Gets the location.
        /// </summary>
        /// <param name="userLocPK">The user loc pk.</param>
        /// <param name="strALL">The string all.</param>
        /// <returns></returns>
        public DataSet GetLocation(string userLocPK, string strALL)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader dr = null;
            try
            {
                strQuery.Append("");
                //strQuery.Append("   SELECT '<ALL>' LOCATION_ID, ")
                //strQuery.Append("       0 LOCATION_MST_PK, ")
                //strQuery.Append("       0 REPORTING_TO_FK, ")
                //strQuery.Append("       0 LOCATION_TYPE_FK ")
                //strQuery.Append("  FROM DUAL ")
                //strQuery.Append("UNION ")
                strQuery.Append(" SELECT L.LOCATION_ID, ");
                strQuery.Append("       L.LOCATION_MST_PK, ");
                strQuery.Append("       L.REPORTING_TO_FK, ");
                strQuery.Append("       L.LOCATION_TYPE_FK ");
                strQuery.Append("  FROM LOCATION_MST_TBL L ");
                strQuery.Append(" START WITH L.LOCATION_TYPE_FK = 1 ");
                strQuery.Append("        AND L.ACTIVE_FLAG = 1 ");
                strQuery.Append("        AND L.LOCATION_MST_PK =" + userLocPK);
                strQuery.Append(" CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK ");
                dr = objWF.GetDataReader(strQuery.ToString());
                while (dr.Read())
                {
                    strReturn += dr["LOCATION_MST_PK"] + "~$";
                }
                dr.Close();
                if (strReturn == "0~$")
                {
                    strQuery = new System.Text.StringBuilder();

                    strQuery.Append(" SELECT L.LOCATION_ID, ");
                    strQuery.Append("       L.LOCATION_MST_PK, ");
                    strQuery.Append("       L.REPORTING_TO_FK, ");
                    strQuery.Append("       L.LOCATION_TYPE_FK ");
                    strQuery.Append("  FROM LOCATION_MST_TBL L ");
                    strQuery.Append("  WHERE L.LOCATION_MST_PK = " + userLocPK);
                    strQuery.Append("UNION ");
                    strQuery.Append(" SELECT L.LOCATION_ID, ");
                    strQuery.Append("       L.LOCATION_MST_PK, ");
                    strQuery.Append("       L.REPORTING_TO_FK, ");
                    strQuery.Append("       L.LOCATION_TYPE_FK ");
                    strQuery.Append("  FROM LOCATION_MST_TBL L ");
                    strQuery.Append("  WHERE L.REPORTING_TO_FK = " + userLocPK);
                    dr = objWF.GetDataReader(strQuery.ToString());
                    while (dr.Read())
                    {
                        strReturn += dr["LOCATION_MST_PK"] + "~$";
                    }
                    dr.Close();
                }

                strALL = strReturn;
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  26/09/2011
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

        #region "Function to check whether a user is an administrator or not"

        /// <summary>
        /// Determines whether the specified string user identifier is administrator.
        /// </summary>
        /// <param name="strUserID">The string user identifier.</param>
        /// <returns></returns>
        public bool IsAdministrator(string strUserID)
        {
            string strSQL = null;
            Int16 Admin = default(Int16);
            WorkFlow objWF = new WorkFlow();

            strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
            strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
            strSQL = strSQL + "AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
            try
            {
                //Admin = objWF.ExecuteScaler(strSQL.ToString());

                if (Admin == 1)
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
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Function to check whether a user is an administrator or not"

        #region "Function to check quotation Expiry "

        /// <summary>
        /// Gets the quotation expiry RPT.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="lngLocationPk">The LNG location pk.</param>
        /// <param name="lngCustomerPk">The LNG customer pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="podpk">The podpk.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="cargo">The cargo.</param>
        /// <param name="ContainerValue">The container value.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <returns></returns>
        public object GetQuotationExpiryRpt(int BizType, string lngLocationPk, string lngCustomerPk, string PolPk, string podpk, string FromDate, string toDate, string cargo = "", string ContainerValue = "", string RefNr = "")
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            string strCondition = null;
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

            if (Convert.ToInt32(lngCustomerPk) != 0)
            {
                strCondition = strCondition + "AND cust.customer_mst_pk=" + lngCustomerPk;
            }

            if (Convert.ToInt32(lngLocationPk) != 0)
            {
                strCondition = strCondition + "AND umt.default_location_fk = " + lngLocationPk;
            }

            if (!((FromDate == null | string.IsNullOrEmpty(FromDate)) & (toDate == null | string.IsNullOrEmpty(toDate))))
            {
                strCondition = strCondition + " AND To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) BETWEEN TO_DATE('" + FromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat) ";
            }
            else if (!(FromDate == null | string.IsNullOrEmpty(FromDate)))
            {
                strCondition = strCondition + " AND To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) >= TO_DATE('" + FromDate + "',dateformat) ";
            }
            else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
            {
                strCondition = strCondition + " AND To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) >= TO_DATE('" + toDate + "',dateformat) ";
            }

            if ((BizType == 2))
            {
                if (cargo == "FCL")
                {
                    strCondition = strCondition + " and quo.cargo_type=1 ";
                    if (!string.IsNullOrEmpty(ContainerValue))
                    {
                        if (ContainerValue != "0")
                        {
                            strCondition = strCondition + " AND   CTRL.CONTAINER_TYPE_MST_PK IN (" + ContainerValue + ")";
                        }
                    }

                    //  ElseIf cargo = "LCL" Or cargo = "BBC" Then
                }
                else if (cargo == "LCL")
                {
                    strCondition = strCondition + " and quo.cargo_type=2 ";
                    if (!string.IsNullOrEmpty(ContainerValue))
                    {
                        strCondition = strCondition + "   AND   DIM3.DIMENTION_UNIT_MST_PK IN (" + ContainerValue + ")";
                    }
                }
                else if (cargo == "BBC")
                {
                    strCondition = strCondition + " and quo.cargo_type=4 ";
                    if (!string.IsNullOrEmpty(ContainerValue))
                    {
                        strCondition = strCondition + "   AND   DIM3.DIMENTION_UNIT_MST_PK IN (" + ContainerValue + ")";
                    }
                }
                strCondition = strCondition + " and quo.status in (1,2,4) ";
            }
            if (BizType != 1)
            {
                if (Convert.ToInt32(PolPk) != 0)
                {
                    strCondition = strCondition + " AND POL.PORT_MST_PK = " + PolPk;
                }
                if (Convert.ToInt32(podpk) != 0)
                {
                    strCondition = strCondition + " AND POD.PORT_MST_PK = " + podpk;
                }
            }

            if (!string.IsNullOrEmpty(RefNr))
            {
                strCondition = strCondition + "   AND    QUO.QUOTATION_REF_NO ='" + RefNr + "'";
            }

            //SEA
            if (BizType == 2 & cargo == "FCL")
            {
                strSQL = " select distinct quo.quotation_ref_no as REf_No,";
                strSQL += " To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) as exp_date,";
                strSQL += " ctrl.CONTAINER_TYPE_MST_ID as ctr_Type,";
                strSQL += " quo1.expected_boxes as Boxes,";
                strSQL += " pol.port_name as pol,";
                strSQL += " pod.port_name as pod,";
                strSQL += " cust.customer_name as customer,";
                //strSQL &= vbCrLf & " curr.currency_id as currency, "
                strSQL += "(SELECT C.CURRENCY_ID  FROM CURRENCY_TYPE_MST_TBL C WHERE C.CURRENCY_MST_PK=" + BaseCurrFk + ") AS currency,";
                //strSQL &= vbCrLf & " quo3.QUOTED_RATE as BOF_QUO_RATE,"
                strSQL += " QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE) AS BOF_QUO_RATE,";
                //strSQL &= vbCrLf & " quo1.ALL_IN_QUOTED_TARIFF as all_in_quote"
                strSQL += "  (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) ";
                strSQL += "            FROM QUOTATION_DTL_TBL A ";
                strSQL += "           WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE ";
                strSQL += " from QUOTATION_MST_TBL quo, ";
                strSQL += " container_type_mst_tbl ctrl,QUOTATION_DTL_TBL quo1,port_mst_tbl pol, ";
                strSQL += "  port_mst_tbl pod,QUOTATION_FREIGHT_TRN quo3,";
                strSQL += "  currency_type_mst_tbl curr,V_ALL_CUSTOMER cust,USER_MST_TBL  UMT,freight_element_mst_tbl frt ";
                strSQL += "  where  curr.currency_mst_pk=quo3.currency_mst_fk ";
                strSQL += "  and quo1.QUOTE_DTL_PK=quo3.QUOTATION_DTL_FK ";
                strSQL += "  and quo.QUOTATION_MST_PK=quo1.QUOTATION_MST_FK ";
                strSQL += "  and quo.CUST_TYPE = CUST.CUSTOMER_TYPE";
                //strSQL &= vbCrLf & "  and loc.location_mst_pk=pol.location_mst_fk"
                strSQL += "  and frt.freight_element_mst_pk=quo3.freight_element_mst_fk";
                strSQL += "  and ctrl.CONTAINER_TYPE_MST_PK = quo1.CONTAINER_TYPE_MST_FK ";
                strSQL += "  and quo.CUSTOMER_MST_FK = cust.CUSTOMER_MST_PK ";
                strSQL += "  and quo1.PORT_MST_POD_FK =pod.port_mst_pk  ";
                strSQL += "  and quo1.PORT_MST_POL_FK =pol.port_mst_pk";
                strSQL += "  and quo.CREATED_BY_FK = UMT.USER_MST_PK";
                //strSQL &= vbCrLf & "  and curr.currency_mst_pk= '" & HttpContext.Current.Session("CURRENCY_MST_PK") & "'"
                //strSQL &= vbCrLf & "  AND cust.customer_mst_pk='" & lngCustomerPk & "'"
                strSQL += " and frt.freight_element_id='BOF'";
                strSQL += strCondition;
            }
            //SEA
            if (BizType == 2 & cargo == "LCL")
            {
                strSQL = " select distinct quo.quotation_ref_no as REf_No,";
                strSQL += " To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) as exp_date,";
                strSQL += "NVL(dim3.DIMENTION_ID,'')   DIMENTION_ID,";
                strSQL += "quo1.EXPECTED_VOLUME    VOLUME,";
                strSQL += " pol.port_name as pol,";
                strSQL += " pod.port_name as pod,";
                strSQL += " cust.customer_name as customer,";
                //strSQL &= vbCrLf & " curr.currency_id as currency, "
                strSQL += "(SELECT C.CURRENCY_ID  FROM CURRENCY_TYPE_MST_TBL C WHERE C.CURRENCY_MST_PK=" + BaseCurrFk + ") AS currency,";

                //strSQL &= vbCrLf & " quo3.QUOTED_RATE as BOF_QUO_RATE,"
                strSQL += " QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)AS BOF_QUO_RATE,";
                //strSQL &= vbCrLf & " quo1.ALL_IN_QUOTED_TARIFF as all_in_quote"
                strSQL += "  (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) ";
                strSQL += "            FROM QUOTATION_FREIGHT_TRN A ";
                strSQL += "           WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE ";
                strSQL += " from QUOTATION_MST_TBL quo, ";
                strSQL += " QUOTATION_DTL_TBL quo1,port_mst_tbl pol, ";
                strSQL += "  port_mst_tbl pod,QUOTATION_FREIGHT_TRN quo3,freight_element_mst_tbl frt,";
                strSQL += "  currency_type_mst_tbl curr,USER_MST_TBL  UMT,DIMENTION_UNIT_MST_TBL  dim3,V_ALL_CUSTOMER cust";
                strSQL += "  where  curr.currency_mst_pk=quo3.currency_mst_fk ";
                strSQL += "  and quo1.QUOTE_DTL_PK=quo3.QUOTATION_DTL_FK ";
                strSQL += "  and quo.CUSTOMER_MST_FK = cust.CUSTOMER_MST_PK ";
                strSQL += "  and quo.QUOTATION_MST_PK= quo1.QUOTATION_MST_FK ";
                strSQL += "  and frt.freight_element_mst_pk=quo3.freight_element_mst_fk";
                //strSQL &= vbCrLf & "  and pol.location_mst_fk=loc.location_mst_pk"
                strSQL += "  and quo1.PORT_MST_POD_FK =pod.port_mst_pk  ";
                strSQL += "  and quo1.PORT_MST_POL_FK =pol.port_mst_pk";
                strSQL += " AND quo1.BASIS    = dim3.DIMENTION_UNIT_MST_PK";
                strSQL += " and quo.CREATED_BY_FK = UMT.USER_MST_PK";
                strSQL += " and frt.freight_element_id='BOF'";
                //strSQL &= vbCrLf & "and curr.currency_mst_pk= '" & HttpContext.Current.Session("CURRENCY_MST_PK") & "'"
                //strSQL &= vbCrLf & "  AND cust.customer_mst_pk='" & lngCustomerPk & "'"
                strSQL += strCondition;
            }

            //SEA
            if (BizType == 2 & cargo == "BBC")
            {
                strSQL = " select distinct quo.quotation_ref_no as REf_No,";
                strSQL += " To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) as exp_date,";
                strSQL += "NVL(dim3.DIMENTION_ID,'')   DIMENTION_ID,";
                strSQL += " NVL(quo1.EXPECTED_BOXES,0) QUANTITY,";
                strSQL += " quo1.EXPECTED_WEIGHT WEIGHT,";
                strSQL += "quo1.EXPECTED_VOLUME    VOLUME,";
                strSQL += " pol.port_name as pol,";
                strSQL += " pod.port_name as pod,";
                strSQL += " cust.customer_name as customer,";
                //strSQL &= vbCrLf & " curr.currency_id as currency, "
                strSQL += "(SELECT C.CURRENCY_ID  FROM CURRENCY_TYPE_MST_TBL C WHERE C.CURRENCY_MST_PK=" + BaseCurrFk + ") AS currency,";

                //strSQL &= vbCrLf & " quo3.QUOTED_RATE as BOF_QUO_RATE,"
                strSQL += " QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE) AS BOF_QUO_RATE,";
                //strSQL &= vbCrLf & " quo1.ALL_IN_QUOTED_TARIFF as all_in_quote"
                strSQL += "  (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) ";
                strSQL += "            FROM QUOTATION_FREIGHT_TRN A ";
                strSQL += "           WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE ";
                strSQL += " from QUOTATION_MST_TBL quo, ";
                strSQL += " QUOTATION_DTL_TBL quo1,port_mst_tbl pol, ";
                strSQL += "  port_mst_tbl pod,QUOTATION_FREIGHT_TRN quo3,freight_element_mst_tbl frt,";
                strSQL += "  currency_type_mst_tbl curr,USER_MST_TBL  UMT,DIMENTION_UNIT_MST_TBL  dim3,V_ALL_CUSTOMER cust";
                strSQL += "  where  curr.currency_mst_pk=quo3.currency_mst_fk ";
                strSQL += "  and quo1.QUOTE_DTL_PK=quo3.QUOTATION_DTL_FK ";
                strSQL += "  and quo.CUSTOMER_MST_FK = cust.CUSTOMER_MST_PK ";
                strSQL += "  and quo.QUOTATION_MST_PK= quo1.QUOTATION_MST_FK ";
                strSQL += "  and frt.freight_element_mst_pk=quo3.freight_element_mst_fk";
                //strSQL &= vbCrLf & "  and pol.location_mst_fk=loc.location_mst_pk"
                strSQL += "  and quo1.PORT_MST_POD_FK =pod.port_mst_pk  ";
                strSQL += "  and quo1.PORT_MST_POL_FK =pol.port_mst_pk";
                strSQL += " AND quo1.BASIS    = dim3.DIMENTION_UNIT_MST_PK";
                strSQL += " and quo.CREATED_BY_FK = UMT.USER_MST_PK";
                strSQL += " and frt.freight_element_id='BOF'";
                //strSQL &= vbCrLf & "and curr.currency_mst_pk= '" & HttpContext.Current.Session("CURRENCY_MST_PK") & "'"
                //strSQL &= vbCrLf & "  AND cust.customer_mst_pk='" & lngCustomerPk & "'"
                strSQL += strCondition;
            }
            //'
            //For Air
            if (BizType == 1)
            {
                strSQL = "select distinct";
                strSQL += "Qry.Quotation_Ref_No,";
                strSQL += "Qry.Exp_Date,";
                strSQL += "Qry.customer,";
                strSQL += "Qry.slap_type,";
                strSQL += "Qry.Aoo,";
                strSQL += "Qry.Aod,";
                strSQL += "Qry.currency,";
                strSQL += "Qry.Minamt";
                strSQL += "from ";

                ///''''''''''
                strSQL += "(select distinct quo.Quotation_Ref_No ,";
                strSQL += "To_char(quo.QUOTATION_DATE + quo.VALID_FOR) Exp_Date ,cust.customer_name as customer,";
                strSQL += "(case when quo.quotation_type=1 then";
                //strSQL &= vbCrLf & "(select slab.breakpoint_id "
                strSQL += " (select decode(slab.BREAKPOINT_TYPE, 1, 'BP', 'ULD')";
                strSQL += "from QUOTATION_DTL_TBL trn , airfreight_slabs_tbl slab where trn.slab_fk=slab.airfreight_slabs_tbl_pk";
                strSQL += "and trn.QUOTATION_MST_FK=quo.QUOTATION_MST_PK AND slab.active_flag = 1   AND ROWNUM=1 group by slab.BREAKPOINT_TYPE)";
                strSQL += "else  null end)slap_type ,";
                strSQL += "( case when  quo.quotation_type=1 then";
                strSQL += "( select  POL.PORT_NAME ";
                strSQL += " from QUOTATION_DTL_TBL trn ,port_mst_tbl pol   where ";
                strSQL += " trn.PORT_MST_POL_FK = POL.PORT_MST_PK  AND ROWNUM=1";
                strSQL += " and trn.QUOTATION_MST_FK=quo.QUOTATION_MST_PK  ";
                strSQL += " group by  POL.PORT_NAME )";
                strSQL += " else (select pod.PORT_NAME";
                strSQL += " from QUOTATION_DTL_TBL main1 ,port_mst_tbl pod where ";
                strSQL += " main1.PORT_MST_POL_FK = Pod.PORT_MST_PK";
                strSQL += " and main1.QUOTATION_MST_FK=quo.QUOTATION_MST_PK  AND ROWNUM=1";
                strSQL += " group by  pod.PORT_NAME )  end )Aoo ,";
                strSQL += "( case when  quo.quotation_type=1 then";
                strSQL += "( select  POL.PORT_MST_PK ";
                strSQL += "from QUOTATION_DTL_TBL trn ,port_mst_tbl pol  where ";
                strSQL += " trn.PORT_MST_POL_FK = POL.PORT_MST_PK";
                strSQL += "and trn.QUOTATION_MST_FK=quo.QUOTATION_MST_PK  AND ROWNUM=1";
                strSQL += "group by  POL.PORT_MST_PK )";
                strSQL += "else (select pod.port_mst_pk";
                strSQL += "from QUOTATION_DTL_TBL main1 ,port_mst_tbl pod  where ";
                strSQL += " main1.PORT_MST_POL_FK = Pod.PORT_MST_PK";
                strSQL += "and main1.QUOTATION_MST_FK=quo.QUOTATION_MST_PK AND ROWNUM=1";
                strSQL += " group by  pod.port_mst_pk )  end )AooPK ,";
                strSQL += " (case when  quo.quotation_type=1 then";
                strSQL += " (select DISTINCT POD.PORT_NAME";
                strSQL += " from QUOTATION_DTL_TBL trn ,port_mst_tbl pod   where ";
                strSQL += " trn.PORT_MST_POD_FK = Pod.PORT_MST_PK";
                strSQL += " and trn.QUOTATION_MST_FK=quo.QUOTATION_MST_PK  AND ROWNUM=1";
                strSQL += " group by  POD.PORT_NAME )";
                strSQL += " else (select DISTINCT pod.PORT_NAME";
                strSQL += " from QUOTATION_DTL_TBL main1 ,port_mst_tbl pod where";
                strSQL += " main1.PORT_MST_POD_FK = Pod.PORT_MST_PK AND ROWNUM=1";
                //strSQL &= vbCrLf & " and main1.QUOTATION_MST_FK=quo.QUOTATION_MST_PK"
                strSQL += " and main1.QUOTATION_MST_FK=quo.QUOTATION_MST_PK group by  pod.PORT_NAME)  end ) Aod ,";
                strSQL += "(case when  quo.quotation_type=1 then";
                strSQL += "(select  POD.PORT_MST_PK";
                strSQL += "from QUOTATION_DTL_TBL trn ,port_mst_tbl pod   where ";
                strSQL += "trn.PORT_MST_POD_FK = Pod.PORT_MST_PK  AND ROWNUM=1";
                strSQL += "and trn.QUOTATION_MST_FK=quo.QUOTATION_MST_PK";
                strSQL += "group by  POD.PORT_MST_PK )";
                strSQL += "else (select pod.PORT_MST_PK";
                strSQL += "from QUOTATION_DTL_TBL main1 ,port_mst_tbl pod where";
                strSQL += "main1.PORT_MST_POD_FK = Pod.PORT_MST_PK AND ROWNUM=1";
                strSQL += " and main1.QUOTATION_MST_FK=quo.QUOTATION_MST_PK group by  pod.PORT_MST_PK)  end ) AodPK ,";
                //-----------
                strSQL += "(case when  quo.quotation_type=1 then ";
                strSQL += "(select cur.currency_id from currency_type_mst_tbl cur ";
                strSQL += "  where";
                strSQL += "  cur.currency_mst_pk = " + BaseCurrFk + "  AND ROWNUM=1 group by  cur.currency_id)";
                strSQL += " Else ";
                strSQL += "(select cur.currency_id from currency_type_mst_tbl cur ";
                strSQL += "  where ";
                strSQL += "  cur.currency_mst_pk = " + BaseCurrFk + "   AND ROWNUM=1 group by  cur.currency_id )";
                strSQL += " end ) currency, ";
                //-----------------
                strSQL += "(case when  quo.quotation_type=1 then ";
                strSQL += "( select min(qrr.quoted_rate*GET_EX_RATE(QRR.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) from QUOTATION_FREIGHT_TRN qrr,";
                strSQL += " QUOTATION_DTL_TBL trn,freight_element_mst_tbl frt ";
                strSQL += " where(trn.QUOTATION_MST_FK = quo.QUOTATION_MST_PK)";
                strSQL += "and trn.QUOTE_DTL_PK=qrr.QUOTATION_DTL_FK and frt.freight_element_mst_pk=qrr.freight_element_mst_fk and frt.freight_element_id='AFC'  AND ROWNUM=1 )";
                strSQL += " Else";
                strSQL += "( select min(quot.QUOTED_MIN_RATE*GET_EX_RATE(quot.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) from QUOTATION_DTL_TBL main1,";
                strSQL += " QUOTATION_FREIGHT_TRN quot,freight_element_mst_tbl frt";
                strSQL += " where(main1.QUOTATION_MST_FK = quo.QUOTATION_MST_PK)";
                strSQL += " and quot.QUOTATION_DTL_FK = main1.QUOTE_DTL_PK and frt.freight_element_mst_pk=quot.freight_element_mst_fk and frt.freight_element_id='AFC' AND ROWNUM=1 )";
                strSQL += "end ) Minamt";
                strSQL += " from QUOTATION_MST_TBL  quo,V_ALL_CUSTOMER cust,";
                strSQL += " user_mst_tbl umt,port_mst_tbl pod,port_mst_tbl pol where";
                strSQL += " quo.CUSTOMER_MST_FK = cust.CUSTOMER_MST_PK";
                strSQL += " and quo.CREATED_BY_FK = UMT.USER_MST_PK";
                strSQL += " and quo.status in (1,2,4)";
                //strSQL &= vbCrLf & " To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) BETWEEN"
                //TO_DATE('01/01/2004', dateformat) AND  TO_DATE('18/09/2008', dateformat)
                //AND umt.default_location_fk = 931

                strSQL += strCondition;
                strSQL += ") Qry ";
                if (Convert.ToInt32(PolPk) != 0)
                {
                    strSQL += "where Qry.AooPK = " + PolPk;
                }
                if (Convert.ToInt32(podpk) != 0 & Convert.ToInt32(PolPk) != 0)
                {
                    strSQL += " and Qry.AodPK=" + podpk;
                }
                if (Convert.ToInt32(podpk) != 0 & Convert.ToInt32(PolPk) == 0)
                {
                    strSQL += " where Qry.AodPK=" + podpk;
                }
            }
            //'Added by Ashish Arya for All biz type
            //*************************************************************************************************************
            if (BizType == 0)
            {
                strSQL = " select distinct quo.quotation_ref_no as REf_No,";
                strSQL += " To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) as exp_date,";
                strSQL += " ctrl.CONTAINER_TYPE_MST_ID as ctr_Type,";
                strSQL += " quo1.expected_boxes as Boxes,";
                strSQL += " pol.port_name as pol,";
                strSQL += " pod.port_name as pod,";
                strSQL += " cust.customer_name as customer,";
                //strSQL &= vbCrLf & " curr.currency_id as currency, "
                strSQL += "(SELECT C.CURRENCY_ID  FROM CURRENCY_TYPE_MST_TBL C WHERE C.CURRENCY_MST_PK=" + BaseCurrFk + ") AS currency,";
                //strSQL &= vbCrLf & " quo3.QUOTED_RATE as BOF_QUO_RATE,"
                strSQL += "  QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE) AS BOF_QUO_RATE,";
                //strSQL &= vbCrLf & " quo1.ALL_IN_QUOTED_TARIFF as all_in_quote"
                strSQL += "  (SELECT SUM(NVL(A.QUOTED_RATE,0)* get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) ";
                strSQL += "            FROM QUOTATION_FREIGHT_TRN A ";
                strSQL += "           WHERE A.QUOTATION_DTL_FK = QUO1.QUOTATION_MST_FK) ALL_IN_QUOTE ";
                strSQL += " from QUOTATION_MST_TBL quo, ";
                strSQL += " container_type_mst_tbl ctrl,QUOTATION_DTL_TBL quo1,port_mst_tbl pol, ";
                strSQL += "  port_mst_tbl pod,QUOTATION_FREIGHT_TRN quo3,";
                strSQL += "  currency_type_mst_tbl curr,V_ALL_CUSTOMER cust,USER_MST_TBL  UMT,freight_element_mst_tbl frt ";
                strSQL += "  where  curr.currency_mst_pk=quo3.currency_mst_fk ";
                strSQL += "  and quo1.QUOTE_DTL_PK=quo3.QUOTATION_DTL_FK ";
                strSQL += "  and quo.QUOTATION_MST_PK=quo1.QUOTATION_MST_FK ";
                strSQL += "  and quo.CUST_TYPE = CUST.CUSTOMER_TYPE";
                //strSQL &= vbCrLf & "  and loc.location_mst_pk=pol.location_mst_fk"
                strSQL += "  and frt.freight_element_mst_pk=quo3.freight_element_mst_fk";
                strSQL += "  and ctrl.CONTAINER_TYPE_MST_PK = quo1.CONTAINER_TYPE_MST_FK ";
                strSQL += "  and quo.CUSTOMER_MST_FK = cust.CUSTOMER_MST_PK ";
                strSQL += "  and quo1.PORT_MST_POD_FK =pod.port_mst_pk  ";
                strSQL += "  and quo1.PORT_MST_POL_FK =pol.port_mst_pk";
                strSQL += "  and quo.CREATED_BY_FK = UMT.USER_MST_PK";
                //strSQL &= vbCrLf & "  and curr.currency_mst_pk= '" & HttpContext.Current.Session("CURRENCY_MST_PK") & "'"
                //strSQL &= vbCrLf & "  AND cust.customer_mst_pk='" & lngCustomerPk & "'"
                strSQL += " and frt.freight_element_id='BOF'";
                strSQL += strCondition;

                strSQL += " UNION ";

                strSQL += " select distinct quo.quotation_ref_no as REf_No,";
                strSQL += " To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) as exp_date,";
                strSQL += "NVL(dim3.DIMENTION_ID,'')   DIMENTION_ID,";
                strSQL += "quo1.EXPECTED_VOLUME    VOLUME,";
                strSQL += " pol.port_name as pol,";
                strSQL += " pod.port_name as pod,";
                strSQL += " cust.customer_name as customer,";
                //strSQL &= vbCrLf & " curr.currency_id as currency, "
                strSQL += "(SELECT C.CURRENCY_ID  FROM CURRENCY_TYPE_MST_TBL C WHERE C.CURRENCY_MST_PK=" + BaseCurrFk + ") AS currency,";
                //strSQL &= vbCrLf & " quo3.QUOTED_RATE as BOF_QUO_RATE,"
                strSQL += " QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE) AS BOF_QUO_RATE,";
                //strSQL &= vbCrLf & " quo1.ALL_IN_QUOTED_TARIFF as all_in_quote"
                strSQL += "  (SELECT SUM(NVL(A.QUOTED_RATE,0)* get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) ";
                strSQL += "            FROM QUOTATION_FREIGHT_TRN A ";
                strSQL += "           WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE ";
                strSQL += " from QUOTATION_MST_TBL quo, ";
                strSQL += " QUOTATION_DTL_TBL quo1,port_mst_tbl pol, ";
                strSQL += "  port_mst_tbl pod,QUOTATION_FREIGHT_TRN quo3,freight_element_mst_tbl frt,";
                strSQL += "  currency_type_mst_tbl curr,USER_MST_TBL  UMT,DIMENTION_UNIT_MST_TBL  dim3,V_ALL_CUSTOMER cust";
                strSQL += "  where  curr.currency_mst_pk=quo3.currency_mst_fk ";
                strSQL += "  and quo1.QUOTE_DTL_PK=quo3.QUOTATION_DTL_FK ";
                strSQL += "  and quo.CUSTOMER_MST_FK = cust.CUSTOMER_MST_PK ";
                strSQL += "  and quo.QUOTATION_MST_PK= quo1.QUOTATION_MST_FK ";
                strSQL += "  and frt.freight_element_mst_pk=quo3.freight_element_mst_fk";
                //strSQL &= vbCrLf & "  and pol.location_mst_fk=loc.location_mst_pk"
                strSQL += "  and quo1.PORT_MST_POD_FK =pod.port_mst_pk  ";
                strSQL += "  and quo1.PORT_MST_POL_FK =pol.port_mst_pk";
                strSQL += " AND quo1.BASIS    = dim3.DIMENTION_UNIT_MST_PK";
                strSQL += " and quo.CREATED_BY_FK = UMT.USER_MST_PK";
                strSQL += " and frt.freight_element_id='BOF'";
                //strSQL &= vbCrLf & "and curr.currency_mst_pk= '" & HttpContext.Current.Session("CURRENCY_MST_PK") & "'"
                //strSQL &= vbCrLf & "  AND cust.customer_mst_pk='" & lngCustomerPk & "'"
                strSQL += strCondition;

                strSQL += " UNION ";

                strSQL += " Select distinct quo.quotation_ref_no as REf_No,";
                strSQL += " To_CHAR(quo.QUOTATION_DATE + quo.VALID_FOR) as exp_date,";
                strSQL += "NVL(dim3.DIMENTION_ID,'')   DIMENTION_ID,";
                //strSQL &= vbCrLf & " NVL(quo1.EXPECTED_BOXES,0) QUANTITY,"
                //strSQL &= vbCrLf & " quo1.EXPECTED_WEIGHT WEIGHT,"
                strSQL += "quo1.EXPECTED_VOLUME    VOLUME,";
                strSQL += " pol.port_name as pol,";
                strSQL += " pod.port_name as pod,";
                strSQL += " cust.customer_name as customer,";
                //strSQL &= vbCrLf & " curr.currency_id as currency, "
                strSQL += "(SELECT C.CURRENCY_ID  FROM CURRENCY_TYPE_MST_TBL C WHERE C.CURRENCY_MST_PK=" + BaseCurrFk + ") AS currency,";
                //strSQL &= vbCrLf & " quo3.QUOTED_RATE as BOF_QUO_RATE,"
                strSQL += " QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE) AS BOF_QUO_RATE,";
                //strSQL &= vbCrLf & " quo1.ALL_IN_QUOTED_TARIFF as all_in_quote"
                strSQL += "  (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) ";
                strSQL += "            FROM QUOTATION_FREIGHT_TRN A ";
                strSQL += "           WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE ";
                strSQL += " from QUOTATION_MST_TBL quo, ";
                strSQL += " QUOTATION_DTL_TBL quo1,port_mst_tbl pol, ";
                strSQL += "  port_mst_tbl pod,QUOTATION_FREIGHT_TRN quo3,freight_element_mst_tbl frt,";
                strSQL += "  currency_type_mst_tbl curr,USER_MST_TBL  UMT,DIMENTION_UNIT_MST_TBL  dim3,V_ALL_CUSTOMER cust";
                strSQL += "  where  curr.currency_mst_pk=quo3.currency_mst_fk ";
                strSQL += "  and quo1.QUOTE_DTL_PK=quo3.QUOTATION_DTL_FK ";
                strSQL += "  and quo.CUSTOMER_MST_FK = cust.CUSTOMER_MST_PK ";
                strSQL += "  and quo.QUOTATION_MST_PK= quo1.QUOTATION_MST_FK ";
                strSQL += "  and frt.freight_element_mst_pk=quo3.freight_element_mst_fk";
                //strSQL &= vbCrLf & "  and pol.location_mst_fk=loc.location_mst_pk"
                strSQL += "  and quo1.PORT_MST_POD_FK =pod.port_mst_pk  ";
                strSQL += "  and quo1.PORT_MST_POL_FK =pol.port_mst_pk";
                strSQL += " AND quo1.BASIS    = dim3.DIMENTION_UNIT_MST_PK";
                strSQL += " and quo.CREATED_BY_FK = UMT.USER_MST_PK";
                strSQL += " and frt.freight_element_id='BOF'";
                //strSQL &= vbCrLf & "and curr.currency_mst_pk= '" & HttpContext.Current.Session("CURRENCY_MST_PK") & "'"
                //strSQL &= vbCrLf & "  AND cust.customer_mst_pk='" & lngCustomerPk & "'"
                strSQL += strCondition;
            }

            //*************************************************************************************************************
            //End If
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

        #endregion "Function to check quotation Expiry "

        #region " GetQuotationExpiryData "

        /// <summary>
        /// Gets the quotation expiry data.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="lngLocationPk">The LNG location pk.</param>
        /// <param name="lngCustomerPk">The LNG customer pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="podpk">The podpk.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="cargo">The cargo.</param>
        /// <param name="ViewType">Type of the view.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ContainerValue">The container value.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <returns></returns>
        public DataSet GetQuotationExpiryData(int BizType, string lngLocationPk, string lngCustomerPk, string PolPk, string podpk, string FromDate, string toDate, string cargo = "", string ViewType = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, string ContainerValue = "", string RefNr = "")
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            string strCondition = null;
            string sql = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);

            try
            {
                if (Convert.ToInt32(lngCustomerPk) != 0)
                {
                    sb1.Append("           AND CUST.CUSTOMER_MST_PK=" + lngCustomerPk + "");
                }
                if (Convert.ToInt32(lngLocationPk) != 0)
                {
                    sb1.Append("           AND UMT.DEFAULT_LOCATION_FK = " + lngLocationPk + "");
                }

                if (!((FromDate == null | string.IsNullOrEmpty(FromDate)) & (toDate == null | string.IsNullOrEmpty(toDate))))
                {
                    sb1.Append("            AND To_date(QUO.QUOTATION_DATE + QUO.VALID_FOR) BETWEEN TO_DATE('" + FromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat) ");
                }
                else if (!(FromDate == null | string.IsNullOrEmpty(FromDate)))
                {
                    sb1.Append("            AND To_date(QUO.QUOTATION_DATE + QUO.VALID_FOR) >= TO_DATE('" + FromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    sb1.Append("            AND To_date(QUO.QUOTATION_DATE + QUO.VALID_FOR) <= TO_DATE('" + toDate + "',dateformat) ");
                }

                if (!string.IsNullOrEmpty(RefNr))
                {
                    sb1.Append("            AND    QUO.QUOTATION_REF_NO ='" + RefNr + "'");
                }

                if ((BizType == 2))
                {
                    if (cargo == "FCL")
                    {
                        sb1.Append("      AND   QUO.CARGO_TYPE = 1");
                        if (!string.IsNullOrEmpty(ContainerValue) & ContainerValue != "0")
                        {
                            sb1.Append("      AND   CTRL.CONTAINER_TYPE_MST_PK IN (" + ContainerValue + ")");
                        }
                        //    ElseIf cargo = "LCL" Or cargo = "BBC" Then
                    }
                    else if (cargo == "LCL")
                    {
                        sb1.Append("     AND   QUO.CARGO_TYPE=2 ");
                        if (!string.IsNullOrEmpty(ContainerValue) & ContainerValue != "0")
                        {
                            sb1.Append("      AND   DIM3.DIMENTION_UNIT_MST_PK IN (" + ContainerValue + ")");
                        }
                    }
                    else if (cargo == "BBC")
                    {
                        sb1.Append("     AND   QUO.CARGO_TYPE=4 ");
                        if (!string.IsNullOrEmpty(ContainerValue) & ContainerValue != "0")
                        {
                            sb1.Append("      AND   DIM3.DIMENTION_UNIT_MST_PK IN (" + ContainerValue + ")");
                        }
                    }
                    sb1.Append("     AND QUO.STATUS IN (1, 2, 4) ");
                }
                //If BizType <> 1 Then
                //    If PolPk <> 0 Then
                //        sb1.Append("             AND POL.PORT_MST_PK = " & PolPk & "")
                //    End If
                //    If podpk <> 0 Then
                //        sb1.Append("             AND POD.PORT_MST_PK = " & podpk & "")
                //    End If
                //End If
                if (BizType == 2)
                {
                    if (Convert.ToInt32(PolPk) != 0)
                    {
                        sb1.Append("             AND POL.PORT_MST_PK = " + PolPk + "");
                    }
                    if (Convert.ToInt32(podpk) != 0)
                    {
                        sb1.Append("             AND POD.PORT_MST_PK = " + podpk + "");
                    }
                    //SEA
                    if (BizType == 2 & cargo == "FCL")
                    {
                        sb.Append("SELECT DISTINCT LOC.LOCATION_ID,");
                        sb.Append("                CUST.CUSTOMER_NAME AS CUSTOMER,");
                        sb.Append("                  QUO.QUOTATION_MST_PK AS QUOTPK,");
                        sb.Append("                QUO.QUOTATION_REF_NO AS REF_NO,");
                        sb.Append("                TO_CHAR(QUO.QUOTATION_DATE + QUO.VALID_FOR) AS EXP_DATE,");
                        sb.Append("                  'SEA' BIZTYPE, ");
                        sb.Append("                 DECODE(QUO.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                        sb.Append("                CTRL.CONTAINER_TYPE_MST_ID AS DIMENTION_ID,");
                        sb.Append("                QUO1.EXPECTED_BOXES AS QUANTITY,");
                        sb.Append("                0 WEIGHT,");
                        sb.Append("                0 VOLUME,");
                        sb.Append("                POL.PORT_ID AS POL,");
                        sb.Append("                POD.PORT_ID AS POD,");
                        sb.Append("                (SELECT C.CURRENCY_ID");
                        sb.Append("                   FROM CURRENCY_TYPE_MST_TBL C");
                        sb.Append("                  WHERE C.CURRENCY_MST_PK = " + BaseCurrFk + ") AS CURRENCY,");
                        sb.Append("                QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)AS BOF_QUO_RATE,");
                        sb.Append(" (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) ");
                        sb.Append("                   FROM QUOTATION_FREIGHT_TRN A");
                        sb.Append("                  WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE, ");
                        sb.Append("                 0 QUOTATION_TYPE, DECODE(QUO.BIZ_TYPE, 1, 'AIR', 2, 'SEA'),DECODE(QUO.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT')");
                        sb.Append("  FROM QUOTATION_MST_TBL          QUO,");
                        sb.Append("       CONTAINER_TYPE_MST_TBL     CTRL,");
                        sb.Append("       QUOTATION_DTL_TBL        QUO1,");
                        sb.Append("       PORT_MST_TBL               POL,");
                        sb.Append("       PORT_MST_TBL               POD,");
                        sb.Append("       QUOTATION_FREIGHT_TRN     QUO3,");
                        sb.Append("       CURRENCY_TYPE_MST_TBL      CURR,");
                        sb.Append("       V_ALL_CUSTOMER             CUST,");
                        //sb.Append("       USER_MST_TBL               UMT,")
                        sb.Append("       FREIGHT_ELEMENT_MST_TBL    FRT,");
                        sb.Append("               USER_MST_TBL      UMT,");
                        sb.Append("                 LOCATION_MST_TBL  LOC");
                        sb.Append(" WHERE CURR.CURRENCY_MST_PK = QUO3.CURRENCY_MST_FK");
                        sb.Append("   AND QUO1.QUOTE_DTL_PK = QUO3.QUOTATION_DTL_FK");
                        sb.Append("   AND QUO.QUOTATION_MST_PK = QUO1.QUOTATION_MST_FK");
                        sb.Append("   AND QUO.CUST_TYPE = CUST.CUSTOMER_TYPE");
                        sb.Append("   AND FRT.FREIGHT_ELEMENT_MST_PK = QUO3.FREIGHT_ELEMENT_MST_FK");
                        sb.Append("   AND CTRL.CONTAINER_TYPE_MST_PK = QUO1.CONTAINER_TYPE_MST_FK");
                        sb.Append("   AND QUO.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                        sb.Append("   AND QUO1.PORT_MST_POD_FK = POD.PORT_MST_PK");
                        sb.Append("   AND QUO1.PORT_MST_POL_FK = POL.PORT_MST_PK");
                        sb.Append("   AND QUO.CREATED_BY_FK = UMT.USER_MST_PK");
                        sb.Append("   AND LOC.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK");
                        if (Convert.ToInt32(lngLocationPk) != 0)
                        {
                            sb.Append("   AND LOC.LOCATION_MST_PK=" + lngLocationPk + "");
                        }
                        sb.Append("    AND QUO.CARGO_TYPE = 1");
                        sb.Append("   AND FRT.FREIGHT_ELEMENT_ID = 'BOF'");
                        sb.Append(sb1.ToString());
                    }

                    if (BizType == 2 & cargo == "LCL")
                    {
                        sb.Append("SELECT DISTINCT LOC.LOCATION_ID,");
                        sb.Append("                CUST.CUSTOMER_NAME AS CUSTOMER,");
                        sb.Append("                  QUO.QUOTATION_MST_PK AS QUOTPK,");
                        sb.Append("                QUO.QUOTATION_REF_NO AS REF_NO,");
                        sb.Append("                TO_CHAR(QUO.QUOTATION_DATE + QUO.VALID_FOR) AS EXP_DATE,");
                        sb.Append("                  'SEA' BIZTYPE, ");
                        sb.Append("                 DECODE(QUO.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                        sb.Append("                CTRL.CONTAINER_TYPE_MST_ID AS DIMENTION_ID,");
                        sb.Append("                QUO1.EXPECTED_BOXES AS QUANTITY,");
                        sb.Append("                0 WEIGHT,");
                        sb.Append("                0 VOLUME,");
                        sb.Append("                POL.PORT_ID AS POL,");
                        sb.Append("                POD.PORT_ID AS POD,");
                        sb.Append("                (SELECT C.CURRENCY_ID");
                        sb.Append("                   FROM CURRENCY_TYPE_MST_TBL C");
                        sb.Append("                  WHERE C.CURRENCY_MST_PK = " + BaseCurrFk + ") AS CURRENCY,");
                        //sb.Append("                QUO3.QUOTED_RATE AS BOF_QUO_RATE,")
                        sb.Append("                QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)AS BOF_QUO_RATE,");
                        //sb.Append("                QUO1.ALL_IN_QUOTED_TARIFF AS ALL_IN_QUOTE")
                        sb.Append(" (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) ");
                        sb.Append("                   FROM QUOTATION_FREIGHT_TRN A");
                        sb.Append("                  WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE, ");
                        sb.Append("                 0 QUOTATION_TYPE, DECODE(QUO.BIZ_TYPE, 1, 'AIR', 2, 'SEA'),DECODE(QUO.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT')");
                        sb.Append("  FROM QUOTATION_MST_TBL          QUO,");
                        sb.Append("       CONTAINER_TYPE_MST_TBL     CTRL,");
                        sb.Append("       QUOTATION_DTL_TBL         QUO1,");
                        sb.Append("       PORT_MST_TBL               POL,");
                        sb.Append("       PORT_MST_TBL               POD,");
                        sb.Append("       QUOTATION_FREIGHT_TRN      QUO3,");
                        sb.Append("       CURRENCY_TYPE_MST_TBL      CURR,");
                        sb.Append("       V_ALL_CUSTOMER             CUST,");
                        //sb.Append("       USER_MST_TBL               UMT,")
                        sb.Append("       FREIGHT_ELEMENT_MST_TBL    FRT,");
                        sb.Append("               USER_MST_TBL      UMT,");
                        sb.Append("                 LOCATION_MST_TBL  LOC");
                        sb.Append(" WHERE CURR.CURRENCY_MST_PK = QUO3.CURRENCY_MST_FK");
                        sb.Append("   AND QUO1.QUOTE_DTL_PK = QUO3.QUOTATION_DTL_FK");
                        sb.Append("   AND QUO.QUOTATION_MST_PK = QUO1.QUOTATION_MST_FK");
                        sb.Append("   AND QUO.CUST_TYPE = CUST.CUSTOMER_TYPE");
                        sb.Append("   AND FRT.FREIGHT_ELEMENT_MST_PK = QUO3.FREIGHT_ELEMENT_MST_FK");
                        sb.Append("   AND CTRL.CONTAINER_TYPE_MST_PK = QUO1.CONTAINER_TYPE_MST_FK");
                        sb.Append("   AND QUO.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                        sb.Append("   AND QUO1.PORT_MST_POD_FK = POD.PORT_MST_PK");
                        sb.Append("   AND QUO1.PORT_MST_POL_FK = POL.PORT_MST_PK");
                        sb.Append("   AND QUO.CREATED_BY_FK = UMT.USER_MST_PK");
                        sb.Append("   AND LOC.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK");
                        if (Convert.ToInt32(lngLocationPk) != 0)
                        {
                            sb.Append("   AND LOC.LOCATION_MST_PK=" + lngLocationPk + "");
                        }
                        sb.Append("    AND QUO.CARGO_TYPE = 2");
                        sb.Append("   AND FRT.FREIGHT_ELEMENT_ID = 'BOF'");
                        sb.Append(sb1.ToString());
                    }

                    if (BizType == 2 & cargo == "BBC")
                    {
                        sb.Append("SELECT DISTINCT LOC.LOCATION_ID,");
                        sb.Append("                CUST.CUSTOMER_NAME AS CUSTOMER,");
                        sb.Append("               QUO.QUOTATION_MST_PK AS QUOTPK,");
                        sb.Append("                QUO.QUOTATION_REF_NO AS REF_NO,");
                        sb.Append("                TO_CHAR(QUO.QUOTATION_DATE + QUO.VALID_FOR) AS EXP_DATE,");
                        sb.Append("                  'SEA' BIZTYPE, ");
                        sb.Append("                 DECODE(QUO.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                        sb.Append("                NVL(DIM3.DIMENTION_ID, '') DIMENTION_ID,");
                        sb.Append("                QUO1.EXPECTED_BOXES QUANTITY,");
                        sb.Append("                QUO1.EXPECTED_WEIGHT WEIGHT,");
                        sb.Append("                QUO1.EXPECTED_VOLUME VOLUME,");
                        sb.Append("                POL.PORT_ID AS POL,");
                        sb.Append("                POD.PORT_ID AS POD,");
                        sb.Append("                (SELECT C.CURRENCY_ID");
                        sb.Append("                   FROM CURRENCY_TYPE_MST_TBL C");
                        sb.Append("                  WHERE C.CURRENCY_MST_PK = " + BaseCurrFk + ") AS CURRENCY,");
                        //sb.Append("                QUO3.QUOTED_RATE AS BOF_QUO_RATE,")
                        sb.Append("                QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE) AS BOF_QUO_RATE,");
                        //sb.Append("                QUO1.ALL_IN_QUOTED_TARIFF AS ALL_IN_QUOTE ")
                        sb.Append(" (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)) ");
                        sb.Append("                   FROM QUOTATION_FREIGHT_TRN  A");
                        sb.Append("                  WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE, ");
                        sb.Append("                 0 QUOTATION_TYPE, DECODE(QUO.BIZ_TYPE, 1, 'AIR', 2, 'SEA'),DECODE(QUO.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT')");
                        sb.Append("  FROM QUOTATION_MST_TBL          QUO,");
                        sb.Append("       QUOTATION_DTL_TBL          QUO1,");
                        sb.Append("       PORT_MST_TBL               POL,");
                        sb.Append("       PORT_MST_TBL               POD,");
                        sb.Append("       QUOTATION_FREIGHT_TRN      QUO3,");
                        sb.Append("       FREIGHT_ELEMENT_MST_TBL    FRT,");
                        sb.Append("       CURRENCY_TYPE_MST_TBL      CURR,");
                        sb.Append("       USER_MST_TBL               UMT,");
                        sb.Append("       DIMENTION_UNIT_MST_TBL     DIM3,");
                        sb.Append("       LOCATION_MST_TBL           LOC,");
                        sb.Append("       V_ALL_CUSTOMER             CUST");
                        sb.Append(" WHERE CURR.CURRENCY_MST_PK = QUO3.CURRENCY_MST_FK");
                        sb.Append("   AND QUO1.QUOTE_DTL_PK = QUO3.QUOTATION_DTL_FK");
                        sb.Append("   AND QUO.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                        sb.Append("   AND QUO.QUOTATION_MST_PK = QUO1.QUOTATION_MST_FK");
                        sb.Append("   AND FRT.FREIGHT_ELEMENT_MST_PK = QUO3.FREIGHT_ELEMENT_MST_FK");
                        sb.Append("   AND QUO1.PORT_MST_POD_FK = POD.PORT_MST_PK");
                        sb.Append("   AND QUO1.PORT_MST_POL_FK = POL.PORT_MST_PK");
                        sb.Append("   AND QUO1.BASIS = DIM3.DIMENTION_UNIT_MST_PK");
                        sb.Append("   AND LOC.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK");
                        if (Convert.ToInt32(lngLocationPk) != 0)
                        {
                            sb.Append("   AND LOC.LOCATION_MST_PK=" + lngLocationPk + "");
                        }
                        sb.Append("    AND QUO.CARGO_TYPE = 4");
                        sb.Append("   AND QUO.CREATED_BY_FK = UMT.USER_MST_PK");
                        sb.Append("   AND FRT.FREIGHT_ELEMENT_ID = 'BOF'");
                        sb.Append(sb1.ToString());
                    }
                }
                //For Air
                if (BizType == 1)
                {
                    sb.Append("SELECT DISTINCT QRY.LOCATION_ID,");
                    sb.Append("                QRY.CUSTOMER,");
                    sb.Append("                QRY.QUOTPK,");
                    sb.Append("                QRY.QUOTATION_REF_NO AS REF_NO,");
                    sb.Append("                QRY.EXP_DATE,");
                    sb.Append("                QRY.BIZTYPE,");
                    sb.Append("                QRY.CARGO_TYPE,");
                    sb.Append("                QRY.SLAP_TYPE        AS DIMENTION_ID,");
                    sb.Append("                QRY.QUANTITY,");
                    sb.Append("                QRY.WEIGHT,");
                    sb.Append("                QRY.VOLUME,");
                    sb.Append("                QRY.AOO              AS POL,");
                    sb.Append("                QRY.AOD              AS POD,");
                    sb.Append("                QRY.CURRENCY,");
                    sb.Append("                QRY.MINAMT           AS BOF_QUO_RATE,");
                    sb.Append("                QRY.ALL_IN_QUOTE, ");
                    sb.Append("                 QRY.QUOTATION_TYPE,QRY.BIZ_TYPE,QRY.PROCESS_TYPE");
                    sb.Append("  FROM (SELECT DISTINCT LOC.LOCATION_ID,");
                    sb.Append("                        CUST.CUSTOMER_NAME AS CUSTOMER,");
                    sb.Append("                         QUO.QUOTATION_MST_PK AS QUOTPK,");
                    sb.Append("                        QUO.QUOTATION_REF_NO,");
                    sb.Append("                        TO_DATE(QUO.QUOTATION_DATE + QUO.VALID_FOR) EXP_DATE,");
                    sb.Append("                         'AIR' BIZTYPE, ");
                    sb.Append("                          DECODE(slab.basis, 1, 'KGS', 2, 'ULD') CARGO_TYPE, ");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT SLAB.BREAKPOINT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL    TRN,");
                    sb.Append("                                   AIRFREIGHT_SLABS_TBL SLAB");
                    sb.Append("                             WHERE TRN.SLAB_FK = SLAB.AIRFREIGHT_SLABS_TBL_PK");
                    sb.Append("                               AND TRN.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND SLAB.ACTIVE_FLAG = 1 AND ROWNUM=1");
                    //If ContainerValue <> "" Then
                    //    sb.Append("      AND   SLAB.AIRFREIGHT_SLABS_TBL_PK IN (" & ContainerValue & ")")
                    //End If
                    sb.Append("                             GROUP BY SLAB.BREAKPOINT_ID)");
                    sb.Append("                          ELSE");
                    sb.Append("                           NULL");
                    sb.Append("                        END) SLAP_TYPE,");
                    //****
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT SLAB.AIRFREIGHT_SLABS_TBL_PK");
                    sb.Append("                              FROM QUOTATION_DTL_TBL    TRN,");
                    sb.Append("                                   AIRFREIGHT_SLABS_TBL SLAB");
                    sb.Append("                             WHERE TRN.SLAB_FK = SLAB.AIRFREIGHT_SLABS_TBL_PK");
                    sb.Append("                               AND TRN.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND SLAB.ACTIVE_FLAG = 1 AND ROWNUM=1");
                    sb.Append("                             GROUP BY SLAB.AIRFREIGHT_SLABS_TBL_PK)");
                    sb.Append("                          ELSE");
                    sb.Append("                           NULL");
                    sb.Append("                        END) SLAP_PK,");
                    //***
                    sb.Append("                        0 QUANTITY,");
                    sb.Append("                        0 WEIGHT,");
                    sb.Append("                        0 VOLUME,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT POL.PORT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL TRN, PORT_MST_TBL POL");
                    sb.Append("                             WHERE TRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("                               AND TRN.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND TRN.QUOTE_DTL_PK = QTA.QUOTE_DTL_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POL.PORT_ID)");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT POD.PORT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL MAIN1,");
                    sb.Append("                                   PORT_MST_TBL         POD");
                    sb.Append("                             WHERE MAIN1.PORT_MST_POL_FK = POD.PORT_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTE_DTL_PK =");
                    sb.Append("                                   QGT.QUOTE_DTL_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POD.PORT_ID)");
                    sb.Append("                        END) AOO,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT POL.PORT_MST_PK");
                    sb.Append("                              FROM QUOTATION_DTL_TBL TRN, PORT_MST_TBL POL");
                    sb.Append("                             WHERE TRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("                               AND TRN.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK AND ROWNUM=1");
                    sb.Append("                            ");
                    sb.Append("                             GROUP BY POL.PORT_MST_PK)");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT POD.PORT_MST_PK");
                    sb.Append("                              FROM QUOTATION_DTL_TBL MAIN1,");
                    sb.Append("                                   PORT_MST_TBL         POD");
                    sb.Append("                             WHERE MAIN1.PORT_MST_POL_FK = POD.PORT_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POD.PORT_MST_PK)");
                    sb.Append("                        END) AOOPK,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT DISTINCT POD.PORT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL TRN, PORT_MST_TBL POD");
                    sb.Append("                             WHERE TRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("                               AND TRN.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND TRN.QUOTE_DTL_PK = QTA.QUOTE_DTL_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POD.PORT_ID)");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT DISTINCT POD.PORT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL MAIN1,");
                    sb.Append("                                   PORT_MST_TBL         POD");
                    sb.Append("                             WHERE MAIN1.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTE_DTL_PK =");
                    sb.Append("                                   QGT.QUOTE_DTL_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POD.PORT_ID)");
                    sb.Append("                        END) AOD,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT POD.PORT_MST_PK");
                    sb.Append("                              FROM QUOTATION_DTL_TBL TRN, PORT_MST_TBL POD");
                    sb.Append("                             WHERE TRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("                               AND TRN.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POD.PORT_MST_PK)");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT POD.PORT_MST_PK");
                    sb.Append("                              FROM QUOTATION_DTL_TBL MAIN1,");
                    sb.Append("                                   PORT_MST_TBL         POD");
                    sb.Append("                             WHERE MAIN1.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POD.PORT_MST_PK)");
                    sb.Append("                        END) AODPK,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT CUR.CURRENCY_ID");
                    sb.Append("                              FROM CURRENCY_TYPE_MST_TBL CUR");
                    sb.Append("                             WHERE CUR.CURRENCY_MST_PK = " + BaseCurrFk + " AND ROWNUM=1");
                    sb.Append("                             GROUP BY CUR.CURRENCY_ID)");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT CUR.CURRENCY_ID");
                    sb.Append("                              FROM CURRENCY_TYPE_MST_TBL CUR");
                    sb.Append("                             WHERE CUR.CURRENCY_MST_PK = " + BaseCurrFk + " AND ROWNUM=1");
                    sb.Append("                             GROUP BY CUR.CURRENCY_ID)");
                    sb.Append("                        END) CURRENCY,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT MIN(QRR.QUOTED_RATE*GET_EX_RATE(QRR.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE))");
                    sb.Append("                              FROM QUOTATION_FREIGHT_TRN QRR,");
                    sb.Append("                                   QUOTATION_DTL_TBL          TRN,");
                    sb.Append("                                   FREIGHT_ELEMENT_MST_TBL    FRT");
                    sb.Append("                             WHERE (TRN.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK)");
                    sb.Append("                               AND TRN.QUOTE_DTL_PK = QRR.QUOTATION_DTL_FK");
                    sb.Append("                               AND FRT.FREIGHT_ELEMENT_MST_PK =");
                    sb.Append("                                   QRR.FREIGHT_ELEMENT_MST_FK AND ROWNUM=1");
                    sb.Append("                               AND FRT.FREIGHT_ELEMENT_ID = 'AFC')");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT MIN(QUOT.QUOTED_MIN_RATE*GET_EX_RATE(QUOT.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE))");
                    sb.Append("                              FROM QUOTATION_DTL_TBL     MAIN1,");
                    sb.Append("                                   QUOTATION_FREIGHT_TRN QUOT,");
                    sb.Append("                                   FREIGHT_ELEMENT_MST_TBL  FRT");
                    sb.Append("                             WHERE (MAIN1.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK)");
                    sb.Append("                               AND QUOT.QUOTATION_DTL_FK =");
                    sb.Append("                                   MAIN1.QUOTE_DTL_PK");
                    sb.Append("                               AND FRT.FREIGHT_ELEMENT_MST_PK =");
                    sb.Append("                                   QUOT.FREIGHT_ELEMENT_MST_FK AND ROWNUM=1");
                    sb.Append("                               AND FRT.FREIGHT_ELEMENT_ID = 'AFC')");
                    sb.Append("                        END) MINAMT,");
                    sb.Append("                        0 ALL_IN_QUOTE,");
                    sb.Append("                 QUO.QUOTATION_TYPE, DECODE(QUO.BIZ_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,DECODE(QUO.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT') PROCESS_TYPE");
                    sb.Append("          FROM QUOTATION_MST_TBL    QUO,");
                    sb.Append("               V_ALL_CUSTOMER       CUST,");
                    sb.Append("               QUOTATION_DTL_TBL QGT,");
                    sb.Append("               QUOTATION_DTL_TBL    QTA,");
                    sb.Append("               USER_MST_TBL         UMT,");
                    sb.Append("               airfreight_slabs_tbl   slab, ");
                    sb.Append("               LOCATION_MST_TBL     LOC");
                    sb.Append("         WHERE QUO.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                    sb.Append("             and QTA.slab_fk = slab.airfreight_slabs_tbl_pk(+)");
                    sb.Append("           AND QUO.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("           AND (QGT.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK OR");
                    sb.Append("               QTA.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK)");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK AND QUO.BIZ_TYPE= 1 ");
                    if (Convert.ToInt32(lngLocationPk) != 0)
                    {
                        sb.Append("   AND LOC.LOCATION_MST_PK=" + lngLocationPk + "");
                    }
                    sb.Append("           AND QUO.STATUS IN (1, 2, 4)");
                    sb.Append("        ");
                    sb.Append(sb1.ToString());
                    sb.Append("        ) QRY");

                    sb.Append("         where 1=1 ");
                    if (Convert.ToInt32(PolPk) != 0)
                    {
                        sb.Append("         and Qry.AooPK = " + PolPk + "");
                    }
                    if (Convert.ToInt32(podpk) != 0 & Convert.ToInt32(PolPk) != 0)
                    {
                        sb.Append("          and Qry.AodPK=" + podpk + "");
                    }
                    if (Convert.ToInt32(podpk) != 0 & Convert.ToInt32(PolPk) == 0)
                    {
                        sb.Append("          and Qry.AodPK=" + podpk + "");
                    }
                    if (!string.IsNullOrEmpty(ContainerValue) & ContainerValue != "0")
                    {
                        sb.Append("      AND   Qry.SLAP_PK IN (" + ContainerValue + ")");
                    }
                }

                //All
                if (BizType == 0)
                {
                    //******************************************************************************
                    if (Convert.ToInt32(PolPk) != 0)
                    {
                        sb1.Append("             AND POL.PORT_MST_PK = " + PolPk + "");
                    }
                    if (Convert.ToInt32(podpk) != 0)
                    {
                        sb1.Append("             AND POD.PORT_MST_PK = " + podpk + "");
                    }

                    sb.Append("SELECT DISTINCT LOC.LOCATION_ID,");
                    sb.Append("                CUST.CUSTOMER_NAME AS CUSTOMER,");
                    sb.Append("                 QUO.QUOTATION_MST_PK AS QUOTPK,");
                    sb.Append("                QUO.QUOTATION_REF_NO AS REF_NO,");
                    sb.Append("                TO_DATE(QUO.QUOTATION_DATE + QUO.VALID_FOR) AS EXP_DATE,");
                    sb.Append("                  'SEA' BIZTYPE, ");
                    sb.Append("                 DECODE(QUO.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                    sb.Append("                CTRL.CONTAINER_TYPE_MST_ID AS DIMENTION_ID,");
                    sb.Append("                QUO1.EXPECTED_BOXES AS QUANTITY,");
                    sb.Append("                0 WEIGHT,");
                    sb.Append("                0 VOLUME,");
                    sb.Append("                POL.PORT_ID AS POL,");
                    sb.Append("                POD.PORT_ID AS POD,");
                    sb.Append("                (SELECT C.CURRENCY_ID");
                    sb.Append("                   FROM CURRENCY_TYPE_MST_TBL C");
                    sb.Append("                  WHERE C.CURRENCY_MST_PK = " + BaseCurrFk + ") AS CURRENCY,");
                    //sb.Append("                QUO3.QUOTED_RATE AS BOF_QUO_RATE,")
                    sb.Append("                QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)AS BOF_QUO_RATE,");
                    //sb.Append("                QUO1.ALL_IN_QUOTED_TARIFF AS ALL_IN_QUOTE")
                    sb.Append(" (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE))");
                    sb.Append("                   FROM QUOTATION_FREIGHT_TRN A");
                    sb.Append("                  WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE,");
                    sb.Append("                 0 QUOTATION_TYPE, DECODE(QUO.BIZ_TYPE, 1, 'AIR', 2, 'SEA'),DECODE(QUO.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT')");
                    sb.Append("  FROM QUOTATION_MST_TBL          QUO,");
                    sb.Append("       CONTAINER_TYPE_MST_TBL     CTRL,");
                    sb.Append("       QUOTATION_DTL_TBL  QUO1,");
                    sb.Append("       PORT_MST_TBL               POL,");
                    sb.Append("       PORT_MST_TBL               POD,");
                    sb.Append("       QUOTATION_FREIGHT_TRN QUO3,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL      CURR,");
                    sb.Append("       V_ALL_CUSTOMER             CUST,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL    FRT,");
                    sb.Append("               USER_MST_TBL      UMT,");
                    sb.Append("                 LOCATION_MST_TBL  LOC");
                    sb.Append(" WHERE CURR.CURRENCY_MST_PK = QUO3.CURRENCY_MST_FK");
                    sb.Append("   AND QUO1.QUOTE_DTL_PK = QUO3.QUOTATION_DTL_FK");
                    sb.Append("   AND QUO.QUOTATION_MST_PK = QUO1.QUOTATION_MST_FK");
                    sb.Append("   AND QUO.CUST_TYPE = CUST.CUSTOMER_TYPE");
                    sb.Append("   AND FRT.FREIGHT_ELEMENT_MST_PK = QUO3.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("   AND CTRL.CONTAINER_TYPE_MST_PK = QUO1.CONTAINER_TYPE_MST_FK");
                    sb.Append("   AND QUO.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                    sb.Append("   AND QUO1.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND QUO1.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND QUO.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND LOC.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK");
                    if (Convert.ToInt32(lngLocationPk) != 0)
                    {
                        sb.Append("   AND LOC.LOCATION_MST_PK=" + lngLocationPk + "");
                    }
                    //sb.Append("   AND LOC.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK")
                    sb.Append("   AND FRT.FREIGHT_ELEMENT_ID = 'BOF' AND QUO.BIZ_TYPE= 2 ");
                    sb.Append(sb1.ToString());

                    sb.Append(" UNION ");

                    sb.Append("SELECT DISTINCT LOC.LOCATION_ID,");
                    sb.Append("                CUST.CUSTOMER_NAME AS CUSTOMER,");
                    sb.Append("                 QUO.QUOTATION_MST_PK AS QUOTPK,");
                    sb.Append("                QUO.QUOTATION_REF_NO AS REF_NO,");
                    sb.Append("                TO_DATE(QUO.QUOTATION_DATE + QUO.VALID_FOR) AS EXP_DATE,");
                    sb.Append("                  'SEA' BIZTYPE, ");
                    sb.Append("                 DECODE(QUO.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                    sb.Append("                NVL(DIM3.DIMENTION_ID, '') DIMENTION_ID,");
                    sb.Append("                0 QUANTITY,");
                    sb.Append("                QUO1.EXPECTED_WEIGHT WEIGHT,");
                    sb.Append("                QUO1.EXPECTED_VOLUME VOLUME,");
                    sb.Append("                POL.PORT_ID AS POL,");
                    sb.Append("                POD.PORT_ID AS POD,");
                    sb.Append("                (SELECT C.CURRENCY_ID");
                    sb.Append("                   FROM CURRENCY_TYPE_MST_TBL C");
                    sb.Append("                  WHERE C.CURRENCY_MST_PK = " + BaseCurrFk + ") AS CURRENCY,");
                    //sb.Append("                QUO3.QUOTED_RATE AS BOF_QUO_RATE,")
                    sb.Append("                QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE)AS BOF_QUO_RATE,");
                    //sb.Append("                QUO1.ALL_IN_QUOTED_TARIFF AS ALL_IN_QUOTE")
                    sb.Append(" (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE))");
                    sb.Append("                   FROM QUOTATION_FREIGHT_TRN A");
                    sb.Append("                  WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE,");
                    sb.Append("                 0 QUOTATION_TYPE, DECODE(QUO.BIZ_TYPE, 1, 'AIR', 2, 'SEA'),DECODE(QUO.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT')");
                    sb.Append("  FROM QUOTATION_MST_TBL          QUO,");
                    sb.Append("       QUOTATION_DTL_TBL  QUO1,");
                    sb.Append("       PORT_MST_TBL               POL,");
                    sb.Append("       PORT_MST_TBL               POD,");
                    sb.Append("       QUOTATION_FREIGHT_TRN QUO3,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL    FRT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL      CURR,");
                    sb.Append("       USER_MST_TBL               UMT,");
                    sb.Append("       DIMENTION_UNIT_MST_TBL     DIM3,");
                    sb.Append("       LOCATION_MST_TBL           LOC,");
                    sb.Append("       V_ALL_CUSTOMER             CUST");
                    sb.Append(" WHERE CURR.CURRENCY_MST_PK = QUO3.CURRENCY_MST_FK");
                    sb.Append("   AND QUO1.QUOTE_DTL_PK = QUO3.QUOTATION_DTL_FK");
                    sb.Append("   AND QUO.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                    sb.Append("   AND QUO.QUOTATION_MST_PK = QUO1.QUOTATION_MST_FK");
                    sb.Append("   AND FRT.FREIGHT_ELEMENT_MST_PK = QUO3.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("   AND QUO1.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND QUO1.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND QUO1.BASIS = DIM3.DIMENTION_UNIT_MST_PK");
                    sb.Append("   AND LOC.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK");
                    if (Convert.ToInt32(lngLocationPk) != 0)
                    {
                        sb.Append("   AND LOC.LOCATION_MST_PK=" + lngLocationPk + "");
                    }
                    sb.Append("   AND QUO.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND FRT.FREIGHT_ELEMENT_ID = 'BOF' AND QUO.BIZ_TYPE= 2");
                    sb.Append(sb1.ToString());

                    sb.Append(" UNION ");

                    sb.Append("SELECT DISTINCT LOC.LOCATION_ID,");
                    sb.Append("                CUST.CUSTOMER_NAME AS CUSTOMER,");
                    sb.Append("                 QUO.QUOTATION_MST_PK AS QUOTPK,");
                    sb.Append("                QUO.QUOTATION_REF_NO AS REF_NO,");
                    sb.Append("                TO_DATE(QUO.QUOTATION_DATE + QUO.VALID_FOR) AS EXP_DATE,");
                    sb.Append("                  'SEA' BIZTYPE, ");
                    sb.Append("                 DECODE(QUO.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                    sb.Append("                NVL(DIM3.DIMENTION_ID, '') DIMENTION_ID,");
                    sb.Append("                QUO1.EXPECTED_BOXES QUANTITY,");
                    sb.Append("                QUO1.EXPECTED_WEIGHT WEIGHT,");
                    sb.Append("                QUO1.EXPECTED_VOLUME VOLUME,");
                    sb.Append("                POL.PORT_ID AS POL,");
                    sb.Append("                POD.PORT_ID AS POD,");
                    sb.Append("                (SELECT C.CURRENCY_ID");
                    sb.Append("                   FROM CURRENCY_TYPE_MST_TBL C");
                    sb.Append("                  WHERE C.CURRENCY_MST_PK = " + BaseCurrFk + ") AS CURRENCY,");
                    //sb.Append("                QUO3.QUOTED_RATE AS BOF_QUO_RATE,")
                    sb.Append("                QUO3.QUOTED_RATE * get_ex_rate(QUO3.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE) AS BOF_QUO_RATE,");
                    sb.Append(" (SELECT SUM(NVL(A.QUOTED_RATE,0) * get_ex_rate(A.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE))");
                    sb.Append("                   FROM QUOTATION_FREIGHT_TRN A");
                    sb.Append("                  WHERE A.QUOTATION_DTL_FK = QUO1.QUOTE_DTL_PK) ALL_IN_QUOTE,");
                    sb.Append("                 0 QUOTATION_TYPE, DECODE(QUO.BIZ_TYPE, 1, 'AIR', 2, 'SEA'),DECODE(QUO.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT')");
                    sb.Append("  FROM QUOTATION_MST_TBL          QUO,");
                    sb.Append("       QUOTATION_DTL_TBL  QUO1,");
                    sb.Append("       PORT_MST_TBL               POL,");
                    sb.Append("       PORT_MST_TBL               POD,");
                    sb.Append("       QUOTATION_FREIGHT_TRN QUO3,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL    FRT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL      CURR,");
                    sb.Append("       USER_MST_TBL               UMT,");
                    sb.Append("       DIMENTION_UNIT_MST_TBL     DIM3,");
                    sb.Append("       LOCATION_MST_TBL           LOC,");
                    sb.Append("       V_ALL_CUSTOMER             CUST");
                    sb.Append(" WHERE CURR.CURRENCY_MST_PK = QUO3.CURRENCY_MST_FK");
                    sb.Append("   AND QUO1.QUOTE_DTL_PK = QUO3.QUOTATION_DTL_FK");
                    sb.Append("   AND QUO.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                    sb.Append("   AND QUO.QUOTATION_MST_PK = QUO1.QUOTATION_MST_FK");
                    sb.Append("   AND FRT.FREIGHT_ELEMENT_MST_PK = QUO3.FREIGHT_ELEMENT_MST_FK");
                    sb.Append("   AND QUO1.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND QUO1.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND QUO1.BASIS = DIM3.DIMENTION_UNIT_MST_PK");
                    sb.Append("   AND LOC.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK");
                    if (Convert.ToInt32(lngLocationPk) != 0)
                    {
                        sb.Append("   AND LOC.LOCATION_MST_PK=" + lngLocationPk + "");
                    }
                    sb.Append("   AND QUO.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND FRT.FREIGHT_ELEMENT_ID = 'BOF' AND QUO.BIZ_TYPE= 2");
                    sb.Append(sb1.ToString());

                    sb.Append(" UNION ");

                    sb.Append("  SELECT DISTINCT LOC.LOCATION_ID,");
                    sb.Append("                        CUST.CUSTOMER_NAME AS CUSTOMER,");
                    sb.Append("                 QUO.QUOTATION_MST_PK AS QUOTPK,");
                    sb.Append("                        QUO.QUOTATION_REF_NO,");
                    sb.Append("                        TO_DATE(QUO.QUOTATION_DATE + QUO.VALID_FOR) EXP_DATE,");
                    sb.Append("                         'AIR' BIZTYPE, ");
                    sb.Append("                          DECODE(slab.basis, 1, 'KGS', 2, 'ULD') CARGO_TYPE, ");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT SLAB.BREAKPOINT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL    TRN,");
                    sb.Append("                                   AIRFREIGHT_SLABS_TBL SLAB");
                    sb.Append("                             WHERE TRN.SLAB_FK = SLAB.AIRFREIGHT_SLABS_TBL_PK");
                    sb.Append("                               AND TRN.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND SLAB.ACTIVE_FLAG = 1 AND ROWNUM=1");
                    sb.Append("                             GROUP BY SLAB.BREAKPOINT_ID)");
                    sb.Append("                          ELSE");
                    sb.Append("                           NULL");
                    sb.Append("                        END) SLAP_TYPE,");
                    sb.Append("                        0 QUANTITY,");
                    sb.Append("                        0 WEIGHT,");
                    sb.Append("                        0 VOLUME,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT POL.PORT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL TRN, PORT_MST_TBL POL");
                    sb.Append("                             WHERE TRN.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("                               AND TRN.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND TRN.QUOTE_DTL_PK = QTA.QUOTE_DTL_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POL.PORT_ID)");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT POD.PORT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL MAIN1,");
                    sb.Append("                                   PORT_MST_TBL         POD");
                    sb.Append("                             WHERE MAIN1.PORT_MST_POL_FK = POD.PORT_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTE_DTL_PK =");
                    sb.Append("                                   QGT.QUOTE_DTL_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POD.PORT_ID)");
                    sb.Append("                        END) AOO,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT DISTINCT POD.PORT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL TRN, PORT_MST_TBL POD");
                    sb.Append("                             WHERE TRN.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("                               AND TRN.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND TRN.QUOTE_DTL_PK = QTA.QUOTE_DTL_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POD.PORT_ID)");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT DISTINCT POD.PORT_ID");
                    sb.Append("                              FROM QUOTATION_DTL_TBL MAIN1,");
                    sb.Append("                                   PORT_MST_TBL         POD");
                    sb.Append("                             WHERE MAIN1.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK");
                    sb.Append("                               AND MAIN1.QUOTE_DTL_PK =");
                    sb.Append("                                   QGT.QUOTE_DTL_PK AND ROWNUM=1");
                    sb.Append("                             GROUP BY POD.PORT_ID)");
                    sb.Append("                        END) AOD,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT CUR.CURRENCY_ID");
                    sb.Append("                              FROM CURRENCY_TYPE_MST_TBL CUR");
                    sb.Append("                             WHERE CUR.CURRENCY_MST_PK = " + BaseCurrFk + " AND ROWNUM=1");
                    sb.Append("                             GROUP BY CUR.CURRENCY_ID)");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT CUR.CURRENCY_ID");
                    sb.Append("                              FROM CURRENCY_TYPE_MST_TBL CUR");
                    sb.Append("                             WHERE CUR.CURRENCY_MST_PK = " + BaseCurrFk + " AND ROWNUM=1");
                    sb.Append("                             GROUP BY CUR.CURRENCY_ID)");
                    sb.Append("                        END) CURRENCY,");
                    sb.Append("                        (CASE");
                    sb.Append("                          WHEN QUO.QUOTATION_TYPE = 1 THEN");
                    sb.Append("                           (SELECT MIN(QRR.QUOTED_RATE*GET_EX_RATE(QRR.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE))");
                    sb.Append("                              FROM QUOTATION_FREIGHT_TRN QRR,");
                    sb.Append("                                   QUOTATION_DTL_TBL          TRN,");
                    sb.Append("                                   FREIGHT_ELEMENT_MST_TBL    FRT");
                    sb.Append("                             WHERE (TRN.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK)");
                    sb.Append("                               AND TRN.QUOTE_DTL_PK = QRR.QUOTATION_DTL_FK");
                    sb.Append("                               AND FRT.FREIGHT_ELEMENT_MST_PK =");
                    sb.Append("                                   QRR.FREIGHT_ELEMENT_MST_FK AND ROWNUM=1");
                    sb.Append("                               AND FRT.FREIGHT_ELEMENT_ID = 'AFC')");
                    sb.Append("                          ELSE");
                    sb.Append("                           (SELECT MIN(QUOT.QUOTED_MIN_RATE*GET_EX_RATE(QUOT.CURRENCY_MST_FK," + BaseCurrFk + ",QUO.QUOTATION_DATE))");
                    sb.Append("                              FROM QUOTATION_DTL_TBL     MAIN1,");
                    sb.Append("                                   QUOTATION_FREIGHT_TRN QUOT,");
                    sb.Append("                                   FREIGHT_ELEMENT_MST_TBL  FRT");
                    sb.Append("                             WHERE (MAIN1.QUOTATION_MST_FK =");
                    sb.Append("                                   QUO.QUOTATION_MST_PK)");
                    sb.Append("                               AND QUOT.QUOTATION_DTL_FK =");
                    sb.Append("                                   MAIN1.QUOTE_DTL_PK");
                    sb.Append("                               AND FRT.FREIGHT_ELEMENT_MST_PK =");
                    sb.Append("                                   QUOT.FREIGHT_ELEMENT_MST_FK AND ROWNUM=1");
                    sb.Append("                               AND FRT.FREIGHT_ELEMENT_ID = 'AFC')");
                    sb.Append("                        END) MINAMT,");
                    sb.Append("                        0 ALL_IN_QUOTE,");
                    sb.Append("                 QUO.QUOTATION_TYPE, DECODE(QUO.BIZ_TYPE, 1, 'AIR', 2, 'SEA'),DECODE(QUO.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT')");
                    sb.Append("          FROM QUOTATION_MST_TBL    QUO,");
                    sb.Append("               V_ALL_CUSTOMER       CUST,");
                    sb.Append("               QUOTATION_DTL_TBL    QGT,");
                    sb.Append("               QUOTATION_DTL_TBL    QTA,");
                    sb.Append("               USER_MST_TBL         UMT,");
                    sb.Append("              PORT_MST_TBL           POL,");
                    sb.Append("                PORT_MST_TBL          POD,");
                    sb.Append("              airfreight_slabs_tbl   slab, ");
                    sb.Append("               LOCATION_MST_TBL     LOC");
                    sb.Append("         WHERE QUO.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                    sb.Append("             and QTA.slab_fk = slab.airfreight_slabs_tbl_pk(+)");
                    sb.Append("           AND QUO.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("             AND QTA.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("           AND QTA.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("           AND (QGT.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK OR");
                    sb.Append("               QTA.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK)");
                    sb.Append("   AND LOC.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK AND QUO.BIZ_TYPE= 1 ");
                    if (Convert.ToInt32(lngLocationPk) != 0)
                    {
                        sb.Append("   AND LOC.LOCATION_MST_PK=" + lngLocationPk + "");
                    }
                    sb.Append(sb1.ToString());
                    //******************************************************************************
                }
                sb.Append(" ORDER BY EXP_DATE DESC ");
                System.Text.StringBuilder strCount = new System.Text.StringBuilder();
                strCount.Append(" SELECT COUNT(*)  from  ");
                strCount.Append((" (" + sb.ToString() + ")"));
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
                strCount.Remove(0, strCount.Length);

                System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
                sqlstr2.Append(" SELECT * FROM ( ");
                sqlstr2.Append(" SELECT ROWNUM SLNO, Qry.* FROM ");
                sqlstr2.Append("  (" + sb.ToString() + " )  ");
                sqlstr2.Append(" Qry ) WHERE SLNO  Between " + start + " and " + last + " ");
                return objWF.GetDataSet(sqlstr2.ToString());
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

        #endregion " GetQuotationExpiryData "
    }
}