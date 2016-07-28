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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_ContractSeaListing : CommonFeatures
    {
        #region " Fetch All "

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="OperatorID">The operator identifier.</param>
        /// <param name="OperatorName">Name of the operator.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="CONTRefNo">The cont reference no.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Commodityfk">The commodityfk.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="STATUS">The status.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="CurrBizType">Type of the curr biz.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string OperatorID = "", string OperatorName = "", string CustomerID = "", string CustomerName = "", string CONTRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string CargoType = "",
        string Commodityfk = "", string FromDate = "", string ToDate = "", int STATUS = 0, bool ActiveOnly = true, string SearchType = "", string SortColumn = " CONT_DATE ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ",
        string CurrBizType = "3", long usrLocFK = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string SrOP = (SearchType == "C" ? "%" : "");
            // OPERATOR
            if (OperatorID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(OPERATOR_ID) LIKE '" + SrOP + OperatorID.ToUpper().Replace("'", "''") + "%'");
            }
            if (OperatorName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(OPERATOR_NAME) LIKE '" + SrOP + OperatorName.ToUpper().Replace("'", "''") + "%'");
            }
            // CUSTOMER
            if (CustomerID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CUSTOMER_ID) LIKE '" + SrOP + CustomerID.ToUpper().Replace("'", "''") + "%'");
            }
            if (CustomerName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CUSTOMER_NAME) LIKE '" + SrOP + CustomerName.ToUpper().Replace("'", "''") + "%'");
            }
            // CONT REF NO
            if (CONTRefNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CONT_REF_NO) LIKE '" + SrOP + CONTRefNo.ToUpper().Replace("'", "''") + "%'");
            }
            //COMMODITY
            //if (Commodityfk > 0)
            //{
            //    buildCondition.Append(" AND MAIN.COMMODITY_GROUP_MST_FK = " + Commodityfk);
            //}

            // PORT OF LOADING
            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            // PORT OF DISCHARGE
            if (PODID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            // CARGO TYPE
            if (CargoType.Length > 0)
            {
                buildCondition.Append(" AND CARGO_TYPE = " + CargoType);
            }

            //If ToDate.Length > 0 And FromDate.Length > 0 Then
            //    buildCondition.Append(" AND (TO_DATE('" & ToDate & "', 'dd/MM/yyyy') >= to_date(MAIN.VALID_TO, dateformat)")
            //    buildCondition.Append(" AND TO_DATE('" & FromDate & "', 'dd/MM/yyyy') <= to_date(MAIN.VALID_FROM, dateformat) ")
            //    buildCondition.Append(" OR MAIN.VALID_TO IS NULL)")

            //    'below logical condition has been changed by sreenivas on 02/03/2010
            //ElseIf ToDate.Length > 0 And Not FromDate.Length > 0 Then
            //    buildCondition.Append(" AND (TO_DATE('" & ToDate & "', 'dd/MM/yyyy') >=  TO_DATE(MAIN.VALID_TO,'" & dateFormat & "') ")
            //    buildCondition.Append("  OR  (MAIN.VALID_TO IS NULL))    ")
            //ElseIf FromDate.Length > 0 And Not ToDate.Length > 0 Then
            //    buildCondition.Append(" AND (TO_DATE('" & FromDate & "', 'dd/MM/yyyy') <= TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "') OR MAIN.VALID_TO IS NULL) ")
            //End If
            //End Sreenivas on 02/03/2010

            if (ToDate.Length > 0 & FromDate.Length > 0)
            {
                buildCondition.Append(" AND (to_date(MAIN.VALID_FROM, dateformat)>= TO_DATE('" + FromDate + "', 'dd/MM/yyyy') or to_date(MAIN.VALID_TO, dateformat)>=TO_DATE('" + FromDate + "', 'dd/MM/yyyy') OR MAIN.VALID_TO IS NULL)");
                buildCondition.Append(" AND to_date(MAIN.VALID_FROM, dateformat)<=TO_DATE('" + ToDate + "', 'dd/MM/yyyy')");
            }
            else if (ToDate.Length > 0 & !(FromDate.Length > 0))
            {
                buildCondition.Append(" AND to_date(MAIN.VALID_FROM, dateformat)<=TO_DATE('" + ToDate + "', 'dd/MM/yyyy')");
            }
            else if (FromDate.Length > 0 & !(ToDate.Length > 0))
            {
                buildCondition.Append(" AND (to_date(MAIN.VALID_FROM, dateformat)>= TO_DATE('" + FromDate + "', 'dd/MM/yyyy') or to_date(MAIN.VALID_TO, dateformat)>=TO_DATE('" + FromDate + "', 'dd/MM/yyyy') OR MAIN.VALID_TO IS NULL)");
            }

            // Modified by Faheem
            if (STATUS != 5)
            {
                if (STATUS == 3)
                {
                    buildCondition.Append(" AND main.APP_STATUS = 2 ");
                }
                else if (STATUS == 2)
                {
                    buildCondition.Append(" AND main.STATUS = 2 ");
                }
                else
                {
                    buildCondition.Append(" AND main.APP_STATUS = " + STATUS);
                }
            }
            // ACTIVE ONLY -- Snigdharani - 11/11/2008,12/12/2008
            if (ActiveOnly == true)
            {
                buildCondition.Append(" and main.active = 1");
                //buildCondition.Append(vbCrLf & " AND ( ")
                //buildCondition.Append(vbCrLf & "        main.VALID_TO >= TO_DATE(SYSDATE , '" & dateFormat & "') ")
                //buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
                //buildCondition.Append(vbCrLf & "     ) ")
            }
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }

            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select count(*) ");
            buildQuery.Append(" FROM ");
            buildQuery.Append(" CONT_CUST_SEA_TBL main, ");
            buildQuery.Append(" OPERATOR_MST_TBL, ");
            buildQuery.Append(" CUSTOMER_MST_TBL, ");
            buildQuery.Append("       commodity_group_mst_tbl cgmt ,");
            //'
            buildQuery.Append(" USER_MST_TBL UMT ");
            buildQuery.Append(" WHERE ");
            // JOIN CONDITION
            buildQuery.Append(" main.OPERATOR_MST_FK = OPERATOR_MST_PK(+) AND ");
            buildQuery.Append(" main.CUSTOMER_MST_FK = CUSTOMER_MST_PK  AND ");
            buildQuery.Append(" UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            buildQuery.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK");
            buildQuery.Append("      AND  main.Commodity_Group_Mst_Fk=commodity_group_pk(+)");
            //'
            buildQuery.Append("  " + strCondition);

            strSQL = buildQuery.ToString();

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

            buildQuery.Remove(0, buildQuery.Length);
            // buildQuery = ""

            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select ");
            buildQuery.Append("       CONT_CUST_SEA_PK, ");
            buildQuery.Append("       main.active ACTIVE, ");
            //Snigdharani - 11/11/2008
            buildQuery.Append("       CONT_REF_NO, ");
            buildQuery.Append("       to_Char(main.CONT_DATE, '" + dateFormat + "') CONT_DATE,  ");
            buildQuery.Append("       OPERATOR_MST_FK, ");
            buildQuery.Append("       OPERATOR_ID, ");
            buildQuery.Append("       OPERATOR_NAME, ");
            buildQuery.Append("       main.CUSTOMER_MST_FK, ");
            buildQuery.Append("       CUSTOMER_ID, ");
            buildQuery.Append("       CUSTOMER_NAME, ");
            buildQuery.Append("       decode(CARGO_TYPE, 1, 'FCL',2, 'LCL') CARGO_TYPE, ");
            buildQuery.Append("       cgmt.commodity_group_code, ");
            //'
            buildQuery.Append("       0 POL, ");
            buildQuery.Append("       0 POD, ");
            buildQuery.Append("       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            //Modofied by Faheem
            //buildQuery.Append(vbCrLf & "       decode(main.STATUS, 0, 'Work In Progress', 1, 'Internal Approved',2, 'Customer Approved') STATUS ")
            buildQuery.Append("       CASE  WHEN MAIN.STATUS = 2 THEN ");
            buildQuery.Append("       DECODE(MAIN.STATUS, 0, 'Work In Progress', 1, 'Internal Approved', 2, 'Customer Approved',4, 'Cancel') ");
            buildQuery.Append("       ELSE DECODE(MAIN.APP_STATUS, 0, 'Work In Progress', 1, 'Internal Approved', 2, 'Internal Rejected',4, 'Cancel') ");
            buildQuery.Append("       END STATUS, ");
            buildQuery.Append("       MAIN.RESTRICTED");
            //End
            //buildQuery.Append(vbCrLf & "       NVL(main.STATUS, 0) STATUS ")
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_CUST_SEA_TBL main, ");
            buildQuery.Append("       OPERATOR_MST_TBL, ");
            buildQuery.Append("       CUSTOMER_MST_TBL ,");
            buildQuery.Append("       commodity_group_mst_tbl cgmt ,");
            //'
            buildQuery.Append("       USER_MST_TBL UMT");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK(+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK ANd ");
            buildQuery.Append("       UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            buildQuery.Append("      AND main.CREATED_BY_FK = UMT.USER_MST_PK");
            buildQuery.Append("      AND  main.Commodity_Group_Mst_Fk=commodity_group_pk(+)");
            //'
            buildQuery.Append("               " + strCondition);
            //buildQuery.Append(vbCrLf & "     Order By " & SortColumn & SortType)
            //buildQuery.Append(vbCrLf & "  ,CONT_REF_NO DESC")
            buildQuery.Append("  Order By MAIN.CONT_DATE DESC, CONT_REF_NO DESC");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);

            strSQL = buildQuery.ToString();

            // band0_SRNO = 0        :   band0_RFQSpotPK = 1  :    band0_Active = 2        :     band0_RFQRefNo = 3
            // band0_RFQDate = 4     :   band0_OperatorFK = 5 :    band0_OperatorID = 6    :     band0_OperatorName = 7
            // band0_CustomerFK = 8  :   band0_CustomerID = 9 :    band0_CustomerName = 10 :     band0_CargoType = 11
            // band0_POLCount = 12   :   band0_PODCount = 13  :    band0_ValidFrom = 14    :     band0_ValidTo = 15
            // band0_Approved = 16
            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CONT_CUST_SEA_PK"], DS.Tables[1].Columns["CONT_CUST_SEA_FK"], true);
                DS.Relations.Add(CONTRel);
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

        /// <summary>
        /// Fetches the expiry all.
        /// </summary>
        /// <param name="OperatorID">The operator identifier.</param>
        /// <param name="OperatorName">Name of the operator.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="CONTRefNo">The cont reference no.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Commodityfk">The commodityfk.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="STATUS">The status.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="CurrBizType">Type of the curr biz.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchExpiryAll(string OperatorID = "", string OperatorName = "", string CustomerID = "", string CustomerName = "", string CONTRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string CargoType = "",
        string Commodityfk = "", string FromDate = "", string ToDate = "", int STATUS = 0, bool ActiveOnly = true, string SearchType = "", string SortColumn = " CONT_DATE ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ",
        string CurrBizType = "3", long usrLocFK = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string SrOP = (SearchType == "C" ? "%" : "");
            // OPERATOR
            if (OperatorID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(OPERATOR_ID) LIKE '" + SrOP + OperatorID.ToUpper().Replace("'", "''") + "%'");
            }
            if (OperatorName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(OPERATOR_NAME) LIKE '" + SrOP + OperatorName.ToUpper().Replace("'", "''") + "%'");
            }
            // CUSTOMER
            if (CustomerID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CUSTOMER_ID) LIKE '" + SrOP + CustomerID.ToUpper().Replace("'", "''") + "%'");
            }
            if (CustomerName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CUSTOMER_NAME) LIKE '" + SrOP + CustomerName.ToUpper().Replace("'", "''") + "%'");
            }
            // CONT REF NO
            if (CONTRefNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CONT_REF_NO) LIKE '" + SrOP + CONTRefNo.ToUpper().Replace("'", "''") + "%'");
            }
            //COMMODITY
            //if (Commodityfk > 0)
            //{
            //    buildCondition.Append(" AND MAIN.COMMODITY_GROUP_MST_FK = " + Commodityfk);
            //}
            // PORT OF LOADING
            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            // PORT OF DISCHARGE
            if (PODID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            // CARGO TYPE
            if (CargoType.Length > 0)
            {
                buildCondition.Append(" AND CARGO_TYPE = " + CargoType);
            }

            //If ToDate.Length > 0 And FromDate.Length > 0 Then
            //    buildCondition.Append(" AND (TO_DATE('" & FromDate & "', 'dd/MM/yyyy') <= TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "'))")
            //    buildCondition.Append(" AND (TO_DATE('" & ToDate & "', 'dd/MM/yyyy') >=  TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "'))")
            //ElseIf ToDate.Length > 0 And Not FromDate.Length > 0 Then
            //    buildCondition.Append(" AND (TO_DATE('" & ToDate & "', 'dd/MM/yyyy') >=  TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "')) ")
            //ElseIf FromDate.Length > 0 And Not ToDate.Length > 0 Then
            //    buildCondition.Append(" AND (TO_DATE('" & FromDate & "', 'dd/MM/yyyy') <= TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "')) ")
            //End If
            if (ToDate.Length > 0 & FromDate.Length > 0)
            {
                buildCondition.Append(" AND (to_date(MAIN.VALID_FROM, dateformat)>= TO_DATE('" + FromDate + "', 'dd/MM/yyyy') or to_date(MAIN.VALID_TO, dateformat)>=TO_DATE('" + FromDate + "', 'dd/MM/yyyy') OR MAIN.VALID_TO IS NULL)");
                buildCondition.Append(" AND to_date(MAIN.VALID_FROM, dateformat)<=TO_DATE('" + ToDate + "', 'dd/MM/yyyy')");
            }
            else if (ToDate.Length > 0 & !(FromDate.Length > 0))
            {
                buildCondition.Append(" AND to_date(MAIN.VALID_FROM, dateformat)<=TO_DATE('" + ToDate + "', 'dd/MM/yyyy')");
            }
            else if (FromDate.Length > 0 & !(ToDate.Length > 0))
            {
                buildCondition.Append(" AND (to_date(MAIN.VALID_FROM, dateformat)>= TO_DATE('" + FromDate + "', 'dd/MM/yyyy') or to_date(MAIN.VALID_TO, dateformat)>=TO_DATE('" + FromDate + "', 'dd/MM/yyyy') OR MAIN.VALID_TO IS NULL)");
            }

            // STATUS OR NOT STATUS
            if (STATUS != 5)
            {
                buildCondition.Append(" AND main.STATUS = " + STATUS);
            }
            // ACTIVE ONLY -- Snigdharani - 11/11/2008,12/12/2008
            //If ActiveOnly = True Then
            buildCondition.Append(" and main.active = 1");
            //    buildCondition.Append(vbCrLf & " AND ( ")
            //    buildCondition.Append(vbCrLf & "        main.VALID_TO >= TO_DATE(SYSDATE , '" & dateFormat & "') ")
            //    buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
            //    buildCondition.Append(vbCrLf & "     ) ")
            //End If
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }

            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select count(*) ");
            buildQuery.Append(" FROM ");
            buildQuery.Append(" CONT_CUST_SEA_TBL main, ");
            buildQuery.Append(" OPERATOR_MST_TBL, ");
            buildQuery.Append(" CUSTOMER_MST_TBL, ");
            buildQuery.Append(" commodity_group_mst_tbl cgmt, ");
            //'
            buildQuery.Append(" USER_MST_TBL UMT ");
            buildQuery.Append(" WHERE ");
            // JOIN CONDITION
            buildQuery.Append(" main.OPERATOR_MST_FK = OPERATOR_MST_PK(+) AND ");
            buildQuery.Append(" main.CUSTOMER_MST_FK = CUSTOMER_MST_PK  AND ");
            buildQuery.Append(" UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            buildQuery.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK");
            buildQuery.Append(" AND  main.Commodity_Group_Mst_Fk = commodity_group_pk(+) ");
            //'
            buildQuery.Append("           and (main.VALID_TO < TO_DATE(SYSDATE, '" + dateFormat + "') and ");
            buildQuery.Append("           main.VALID_TO IS not NULL)");
            buildQuery.Append("  " + strCondition);

            strSQL = buildQuery.ToString();

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

            buildQuery.Remove(0, buildQuery.Length);
            // buildQuery = ""

            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select ");
            buildQuery.Append("       CONT_CUST_SEA_PK, ");
            buildQuery.Append("       main.active ACTIVE, ");
            //Snigdharani - 11/11/2008
            buildQuery.Append("       CONT_REF_NO, ");
            buildQuery.Append("       to_Char(main.CONT_DATE, '" + dateFormat + "') CONT_DATE,  ");
            buildQuery.Append("       OPERATOR_MST_FK, ");
            buildQuery.Append("       OPERATOR_ID, ");
            buildQuery.Append("       OPERATOR_NAME, ");
            buildQuery.Append("       main.CUSTOMER_MST_FK, ");
            buildQuery.Append("       CUSTOMER_ID, ");
            buildQuery.Append("       CUSTOMER_NAME, ");
            buildQuery.Append("       decode(CARGO_TYPE, 1, 'FCL',2, 'LCL') CARGO_TYPE, ");
            buildQuery.Append("       cgmt.commodity_group_code, ");
            //'
            buildQuery.Append("       0 POL, ");
            buildQuery.Append("       0 POD, ");
            buildQuery.Append("       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            buildQuery.Append("       decode(main.STATUS, 0, 'Work In Progress', 1 , 'Internal Approved' , 2 , 'Customer Approved', 3 , 'Internal Rejected') STATUS, ");
            buildQuery.Append("       MAIN.RESTRICTED");
            //buildQuery.Append(vbCrLf & "       NVL(main.STATUS, 0) STATUS ")
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_CUST_SEA_TBL main, ");
            buildQuery.Append("       OPERATOR_MST_TBL, ");
            buildQuery.Append("       CUSTOMER_MST_TBL ,");
            buildQuery.Append("       COMMODITY_GROUP_MST_TBL CGMT ,");
            //'
            buildQuery.Append("       USER_MST_TBL UMT");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK(+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK ANd ");
            buildQuery.Append("       UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            buildQuery.Append("      AND main.CREATED_BY_FK = UMT.USER_MST_PK");
            buildQuery.Append("      AND  main.Commodity_Group_Mst_Fk = commodity_group_pk(+) ");
            //'
            buildQuery.Append("           and (main.VALID_TO < TO_DATE(SYSDATE, '" + dateFormat + "') and ");
            buildQuery.Append("           main.VALID_TO IS not NULL)");
            buildQuery.Append("               " + strCondition);
            //buildQuery.Append(vbCrLf & "     Order By " & SortColumn & SortType)
            //buildQuery.Append(vbCrLf & "  ,CONT_REF_NO DESC")
            buildQuery.Append("  Order By MAIN.CONT_DATE DESC, CONT_REF_NO DESC");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);

            strSQL = buildQuery.ToString();

            // band0_SRNO = 0        :   band0_RFQSpotPK = 1  :    band0_Active = 2        :     band0_RFQRefNo = 3
            // band0_RFQDate = 4     :   band0_OperatorFK = 5 :    band0_OperatorID = 6    :     band0_OperatorName = 7
            // band0_CustomerFK = 8  :   band0_CustomerID = 9 :    band0_CustomerName = 10 :     band0_CargoType = 11
            // band0_POLCount = 12   :   band0_PODCount = 13  :    band0_ValidFrom = 14    :     band0_ValidTo = 15
            // band0_Approved = 16
            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CONT_CUST_SEA_PK"], DS.Tables[1].Columns["CONT_CUST_SEA_FK"], true);
                DS.Relations.Add(CONTRel);
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

        /// <summary>
        /// Alls the master p ks.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        private string AllMasterPKs(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append("-1,");
            for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
            {
                strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["CONT_CUST_SEA_PK"]).Trim() + ",");
            }
            strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion " Fetch All "

        #region " Fetch Childs "

        /// <summary>
        /// Fetches the child for.
        /// </summary>
        /// <param name="CONTSpotPKs">The cont spot p ks.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="SrOp">The sr op.</param>
        /// <returns></returns>
        private DataTable FetchChildFor(string CONTSpotPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            // band1_SRNO() = 0 :  band1_TarifMainFK = 1 : band1_PortPOLFK = 2 :  band1_PortPOLID = 3
            // band1_PortPOLName = 4 : band1_PortPODFK = 5 :  band1_PortPODID = 6 :  band1_PortPODName = 7
            // band1_ValidFrom = 8 :  band1_ValidTo = 9

            if (CONTSpotPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and CONT_CUST_SEA_FK in (" + CONTSpotPKs + ") ");
            }
            if (POLID.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(PORTPOL.PORT_ID) LIKE '" + SrOp + POLID.ToUpper().Replace("'", "''") + "%'");
            }
            if (POLName.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(PORTPOL.PORT_NAME) LIKE '" + SrOp + POLName.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODID.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(PORTPOD.PORT_ID) LIKE '" + SrOp + PODID.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODName.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(PORTPOD.PORT_NAME) LIKE '" + SrOp + PODName.ToUpper().Replace("'", "''") + "%'");
            }

            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select Distinct ");
            buildQuery.Append("       CONT_CUST_SEA_FK, ");
            buildQuery.Append("       PORT_MST_POL_FK, ");
            buildQuery.Append("       PORTPOL.PORT_ID PORT_POL_ID, ");
            buildQuery.Append("       PORTPOL.PORT_NAME PORT_POL_NAME, ");
            buildQuery.Append("       PORT_MST_POD_FK, ");
            buildQuery.Append("       PORTPOD.PORT_ID PORT_POD_ID, ");
            buildQuery.Append("       PORTPOD.PORT_NAME PORT_POD_NAME, ");
            buildQuery.Append("       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO ");
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_CUST_TRN_SEA_TBL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOD ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK AND ");
            buildQuery.Append("       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("      Order By CONT_CUST_SEA_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");

            // band1_SRNO = 17       :   band1_RfqSpotFK = 18    :   band1_PortPOLFK = 19    :   band1_PortPOLID = 20
            // band1_PortPOLName = 21:   band1_PortPODFK = 22    :   band1_PortPODID = 23    :   band1_PortPODName = 24
            // band1_ValidFrom = 25  :   band1_ValidTo = 26

            strSQL = buildQuery.ToString();

            WorkFlow objWF = new WorkFlow();
            DataTable dt = null;
            try
            {
                dt = objWF.GetDataTable(strSQL);
                int RowCnt = 0;
                int Rno = 0;
                int pk = 0;
                pk = -1;
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    //if (dt.Rows[RowCnt]["CONT_CUST_SEA_FK"] != pk)
                    //{
                    //    pk = dt.Rows[RowCnt]["CONT_CUST_SEA_FK"];
                    //    Rno = 0;
                    //}
                    Rno += 1;
                    dt.Rows[RowCnt]["SR_NO"] = Rno;
                }
                return dt;
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

        #endregion " Fetch Childs "

        #region " Enhance Search Function for contract   for CUSTOMER-OPERATOR "

        /// <summary>
        /// Fetches the cont for operator customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchContForOperatorCustomer(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strSEARCH_OP_ID_IN = "";
            string strSEARCH_CS_ID_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            //arr = strCond.Split("~");
            //strReq = arr(0);
            //strSERACH_IN = arr(1);
            //if (arr.Length > 2)
            //    strSEARCH_OP_ID_IN = arr(2);
            //if (arr.Length > 3)
            //    strSEARCH_CS_ID_IN = arr(3);
            //if (arr.Length > 4)
            //    strLOC_MST_IN = arr(4);

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CONT_REF_NO_PKG.GET_OPERATOR_CONT_COMMON";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_OP_ID_IN", ifDBNull(strSEARCH_OP_ID_IN)).Direction = ParameterDirection.Input;
                _with1.Add("SEARCH_CS_ID_IN", ifDBNull(strSEARCH_CS_ID_IN)).Direction = ParameterDirection.Input;
                _with1.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
            finally
            {
                SCM.Connection.Close();
            }
        }

        #endregion " Enhance Search Function for contract   for CUSTOMER-OPERATOR "

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

        /// <summary>
        /// Removes the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, ""))
            {
                return "";
            }
            return col;
        }

        #endregion " Supporting Function "

        #region "Expiry Report"

        /// <summary>
        /// Fetches the expiry report.
        /// </summary>
        /// <param name="OperatorID">The operator identifier.</param>
        /// <param name="OperatorName">Name of the operator.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="CONTRefNo">The cont reference no.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="STATUS">The status.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <returns></returns>
        public DataSet FetchExpiryReport(string OperatorID = "", string OperatorName = "", string CustomerID = "", string CustomerName = "", string CONTRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string CargoType = "",
        string FromDt = "", string Todt = "", int STATUS = 0, bool ActiveOnly = true, string usrLocFK = "", string SearchType = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;

            try
            {
                string SrOP = (SearchType == "C" ? "%" : "");
                // OPERATOR
                if (OperatorID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '" + SrOP + OperatorID.ToUpper().Replace("'", "''") + "%'");
                }
                if (OperatorName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(OPR.OPERATOR_NAME) LIKE '" + SrOP + OperatorName.ToUpper().Replace("'", "''") + "%'");
                }
                // CUSTOMER
                if (CustomerID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUSTOMER_ID) LIKE '" + SrOP + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }
                if (CustomerName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUSTOMER_NAME) LIKE '" + SrOP + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                // CONT REF NO
                if (CONTRefNo.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CONT_REF_NO) LIKE '" + SrOP + CONTRefNo.ToUpper().Replace("'", "''") + "%'");
                }
                // PORT OF LOADING
                if (POLID.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (POLName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                // PORT OF DISCHARGE
                if (PODID.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (PODName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }

                if (STATUS != 5)
                {
                    buildCondition.Append(" AND main.STATUS = " + STATUS);
                }

                if (CargoType.Length > 0)
                {
                    buildCondition.Append(" AND CARGO_TYPE = " + CargoType);
                }
                if (CustomerID.Length > 0)
                {
                    buildCondition.Append(" and CUSTOMER_MST_TBL.CUSTOMER_ID = '" + CustomerID + "'");
                }
                //If FromDt.Length > 0 Then
                //    buildCondition.Append(vbCrLf & " and to_date(main.valid_from,'dd/MM/yyyy') >= to_date('" & FromDt & "','dd/MM/yyyy')")
                //End If
                //If Todt.Length > 0 Then
                //    buildCondition.Append(vbCrLf & "  and to_date(main.valid_to,'dd/MM/yyyy') <= to_date('" & Todt & "','dd/MM/yyyy')")
                //End If
                if (Todt.Length > 0 & FromDt.Length > 0)
                {
                    buildCondition.Append(" AND (to_date(MAIN.VALID_FROM, dateformat)>= TO_DATE('" + FromDt + "', 'dd/MM/yyyy') or to_date(MAIN.VALID_TO, dateformat)>=TO_DATE('" + FromDt + "', 'dd/MM/yyyy') OR MAIN.VALID_TO IS NULL)");
                    buildCondition.Append(" AND to_date(MAIN.VALID_FROM, dateformat)<=TO_DATE('" + Todt + "', 'dd/MM/yyyy')");
                }
                else if (Todt.Length > 0 & !(FromDt.Length > 0))
                {
                    buildCondition.Append(" AND to_date(MAIN.VALID_FROM, dateformat)<=TO_DATE('" + Todt + "', 'dd/MM/yyyy')");
                }
                else if (FromDt.Length > 0 & !(Todt.Length > 0))
                {
                    buildCondition.Append(" AND (to_date(MAIN.VALID_FROM, dateformat)>= TO_DATE('" + FromDt + "', 'dd/MM/yyyy') or to_date(MAIN.VALID_TO, dateformat)>=TO_DATE('" + FromDt + "', 'dd/MM/yyyy') OR MAIN.VALID_TO IS NULL)");
                }

                strCondition = buildCondition.ToString();
                sb.Append("");
                sb.Append("");
                sb.Append("     Select ROWNUM SR_NO, q.CONT_CUST_SEA_PK,");
                sb.Append("        q.CONT_REF_NO,");
                sb.Append("        q.EXP_DATE,");
                sb.Append("        q.OPERATOR_ID,");
                sb.Append("        q.OPERATOR_NAME,");
                sb.Append("        q.CONT_ID,");
                sb.Append("        q.POL,");
                sb.Append("        q.POD,");
                sb.Append("        q.CURRENCY_ID,");
                sb.Append("        q.BOF,");
                sb.Append("        q.AIR,");
                sb.Append("        q.CUSTOMER_MST_FK,");
                sb.Append("        q.CUSTOMER_ID,");
                sb.Append("        q.CUSTOMER_NAME,");
                sb.Append("        q.CARGO_TYPE,");
                sb.Append("        q.commodity_name,");
                sb.Append("        q.comm_grp");
                sb.Append("          from (Select  distinct");
                sb.Append("                 CONT_CUST_SEA_PK,");
                sb.Append("                 CONT_REF_NO,MAIN.CONT_DATE,");
                sb.Append("                 to_Char(main.valid_to, 'dd/MM/yyyy') EXP_DATE,");
                sb.Append("                 OPR.OPERATOR_ID,");
                sb.Append("                 OPR.OPERATOR_NAME,");
                sb.Append("                 CTY.CONTAINER_TYPE_MST_ID CONT_ID,");
                sb.Append("                 PORT.PORT_NAME POL,");
                sb.Append("                 POD.PORT_NAME POD,");
                sb.Append("                 CURR.CURRENCY_ID,");
                sb.Append("                 CTRN.APPROVED_BOF_RATE BOF,");
                sb.Append("                 CTRN.APPROVED_ALL_IN_RATE AIR,");
                sb.Append("                 main.CUSTOMER_MST_FK,");
                sb.Append("                 CUSTOMER_ID,");
                sb.Append("                 CUSTOMER_NAME,");
                sb.Append("                 decode(CARGO_TYPE, 1, 'FCL', 2, 'LCL') CARGO_TYPE,");
                sb.Append("                 com.commodity_name,");
                sb.Append("                 cgm.commodity_group_code comm_grp,cty.preferences");
                sb.Append("                 from CONT_CUST_SEA_TBL main,");
                sb.Append("                       CONT_CUST_TRN_SEA_TBL CTRN,");
                //sb.Append("                       OPERATOR_MST_TBL,")
                sb.Append("                       CUSTOMER_MST_TBL,");
                sb.Append("                       USER_MST_TBL UMT,");
                sb.Append("                       PORT_MST_TBL PORT,");
                sb.Append("                       PORT_MST_TBL POD,");
                sb.Append("                       CONTAINER_TYPE_MST_TBL CTY,");
                sb.Append("                       CURRENCY_TYPE_MST_TBL CURR,");
                sb.Append("                       commodity_mst_tbl com,");
                sb.Append("                      commodity_group_mst_tbl cgm,");
                sb.Append("                      OPERATOR_MST_TBL OPR");
                sb.Append("                 where main.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK(+)");
                sb.Append("                   AND main.CUSTOMER_MST_FK = CUSTOMER_MST_PK");
                sb.Append("                   AND MAIN.CONT_CUST_SEA_PK = CTRN.CONT_CUST_SEA_FK");
                sb.Append("                   ANd UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                sb.Append("                   AND main.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("                   and main.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK(+)");
                //sb.Append("                   AND CARGO_TYPE = " & CargoType & " ")
                //sb.Append("                   and main.active = 1")
                sb.Append("                   AND PORT.PORT_MST_PK = CTRN.PORT_MST_POL_FK");
                sb.Append("                   AND POD.PORT_MST_PK = CTRN.PORT_MST_POD_FK");
                sb.Append("                   AND CTRN.CONTAINER_TYPE_MST_FK = CTY.CONTAINER_TYPE_MST_PK");
                sb.Append("                   and CTRN.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)");
                sb.Append("                   and main.commodity_mst_fk = com.commodity_mst_pk(+)");
                sb.Append("                   and main.commodity_group_mst_fk = cgm.commodity_group_pk(+)");
                sb.Append("  " + strCondition);
                sb.Append("                   and (main.VALID_TO < TO_DATE(SYSDATE , '" + dateFormat + "')");
                sb.Append("                         and main.VALID_TO IS not NULL)");
                sb.Append("   Order By MAIN.CONT_DATE DESC,main.CONT_REF_NO DESC, cty.preferences");
                sb.Append("                   ) q");
                return objWF.GetDataSet(sb.ToString());
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
        /// Fetches the expiry report_ LCL.
        /// </summary>
        /// <param name="OperatorID">The operator identifier.</param>
        /// <param name="OperatorName">Name of the operator.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="CONTRefNo">The cont reference no.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="STATUS">The status.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <returns></returns>
        public DataSet FetchExpiryReport_LCL(string OperatorID = "", string OperatorName = "", string CustomerID = "", string CustomerName = "", string CONTRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string CargoType = "",
        string FromDt = "", string Todt = "", int STATUS = 0, bool ActiveOnly = true, string usrLocFK = "", string SearchType = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;

            try
            {
                string SrOP = (SearchType == "C" ? "%" : "");
                // OPERATOR
                if (OperatorID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '" + SrOP + OperatorID.ToUpper().Replace("'", "''") + "%'");
                }
                if (OperatorName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(OPR.OPERATOR_NAME) LIKE '" + SrOP + OperatorName.ToUpper().Replace("'", "''") + "%'");
                }
                // CUSTOMER
                if (CustomerID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUSTOMER_ID) LIKE '" + SrOP + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }
                if (CustomerName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUSTOMER_NAME) LIKE '" + SrOP + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                // CONT REF NO
                if (CONTRefNo.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CONT_REF_NO) LIKE '" + SrOP + CONTRefNo.ToUpper().Replace("'", "''") + "%'");
                }
                // PORT OF LOADING
                if (POLID.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (POLName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                // PORT OF DISCHARGE
                if (PODID.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (PODName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }

                if (STATUS != 5)
                {
                    buildCondition.Append(" AND main.STATUS = " + STATUS);
                }

                //If CargoType.Length > 0 Then
                //    buildCondition.Append(vbCrLf & " AND main.CARGO_TYPE = " & CargoType)
                //End If
                if (CustomerID.Length > 0)
                {
                    buildCondition.Append(" and CUSTOMER_MST_TBL.CUSTOMER_ID = '" + CustomerID + "'");
                }
                if (FromDt.Length > 0)
                {
                    buildCondition.Append(" and to_date(main.valid_from,'dd/MM/yyyy') >= to_date('" + FromDt + "','dd/MM/yyyy')");
                }
                if (Todt.Length > 0)
                {
                    buildCondition.Append("  and to_date(main.valid_to,'dd/MM/yyyy') <= to_date('" + Todt + "','dd/MM/yyyy')");
                }
                strCondition = buildCondition.ToString();
                sb.Append("  Select ROWNUM SR_NO,");
                sb.Append("                 q.CONT_CUST_SEA_PK,");
                sb.Append("                 q.CONT_REF_NO,");
                sb.Append("                 q.OPERATOR_ID,");
                sb.Append("                 q.OPERATOR_NAME,");
                sb.Append("                 q.CUSTOMER_MST_FK,");
                sb.Append("                 q.CUSTOMER_ID,");
                sb.Append("                 q.CUSTOMER_NAME,");
                sb.Append("                 q.CARGO_TYPE,");
                sb.Append("                 q.POL,");
                sb.Append("                 q.POD,");
                sb.Append("                 q.VALID_FROM,");
                sb.Append("                 q.VALID_TO,");
                sb.Append("                 q.COMMODITY_NAME,");
                sb.Append("                 q.CURRENCY_ID,");
                sb.Append("                 q.BOF,");
                sb.Append("                 q.AIR,");
                sb.Append("                 q.DIMENTION_ID,");
                sb.Append("                 q.commgrp");
                sb.Append("          from (Select");
                sb.Append("                 CONT_CUST_SEA_PK,");
                sb.Append("                 CONT_REF_NO,");
                sb.Append("                 OPR.OPERATOR_ID,");
                sb.Append("                 OPR.OPERATOR_NAME,");
                sb.Append("                 main.CUSTOMER_MST_FK,");
                sb.Append("                 CUSTOMER_ID,");
                sb.Append("                 CUSTOMER_NAME,");
                sb.Append("                 decode(main.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CARGO_TYPE,");
                sb.Append("                 PORT.PORT_NAME POL,");
                sb.Append("                 POD.PORT_NAME POD,");
                sb.Append("                 to_Char(main.VALID_FROM, 'dd/MM/yyyy') VALID_FROM,");
                sb.Append("                 to_Char(main.VALID_TO, 'dd/MM/yyyy') VALID_TO,");
                sb.Append("                 COM.COMMODITY_NAME,");
                sb.Append("                 CURR.CURRENCY_ID,");
                sb.Append("                 CTRN.APPROVED_BOF_RATE BOF,");
                sb.Append("                 CTRN.APPROVED_ALL_IN_RATE AIR,");
                sb.Append("                 UOM.DIMENTION_ID,");
                sb.Append("                 cgm.commodity_group_code commgrp");
                sb.Append("                 from CONT_CUST_SEA_TBL main,");
                sb.Append("                       CONT_CUST_TRN_SEA_TBL CTRN, ");
                //sb.Append("                       OPERATOR_MST_TBL,")
                sb.Append("                       CUSTOMER_MST_TBL,");
                sb.Append("                       USER_MST_TBL UMT,");
                sb.Append("                       PORT_MST_TBL PORT,");
                sb.Append("                       PORT_MST_TBL POD,");
                sb.Append("                       CURRENCY_TYPE_MST_TBL CURR,");
                sb.Append("                       commodity_mst_tbl com,");
                sb.Append("                       DIMENTION_UNIT_MST_TBL UOM,");
                sb.Append("                       commodity_group_mst_tbl cgm,");
                sb.Append("                       OPERATOR_MST_TBL OPR");
                sb.Append("                 where main.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK(+)");
                sb.Append("                   AND CTRN.CONT_CUST_SEA_FK = MAIN.CONT_CUST_SEA_PK");
                sb.Append("                   AND main.CUSTOMER_MST_FK = CUSTOMER_MST_PK");
                sb.Append("                   ANd UMT.DEFAULT_LOCATION_FK = " + usrLocFK + "");
                sb.Append("                   AND main.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("                   AND main.CARGO_TYPE = 2");
                sb.Append("                   and main.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK(+)");
                sb.Append("                   AND PORT.PORT_MST_PK = CTRN.PORT_MST_POL_FK");
                sb.Append("                   AND POD.PORT_MST_PK = CTRN.PORT_MST_POD_FK");
                sb.Append("                   AND CTRN.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK");
                sb.Append("                    and ctrn.currency_mst_fk = CURR.CURRENCY_MST_PK(+)");
                sb.Append("                   AND MAIN.COMMODITY_MST_FK = COM.COMMODITY_MST_PK(+)");
                sb.Append("                   and main.commodity_group_mst_fk = cgm.commodity_group_pk");
                sb.Append("  " + strCondition);
                sb.Append("                   and (main.VALID_TO < TO_DATE(SYSDATE , '" + dateFormat + "')");
                sb.Append("                         and main.VALID_TO IS not NULL)");
                sb.Append("                 Order By MAIN.CONT_DATE DESC, CONT_REF_NO DESC) q");
                sb.Append("               ");

                return objWF.GetDataSet(sb.ToString());
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
        /// Fetches the expiry report HDR.
        /// </summary>
        /// <param name="OperatorID">The operator identifier.</param>
        /// <param name="OperatorName">Name of the operator.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="CONTRefNo">The cont reference no.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="STATUS">The status.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <returns></returns>
        public DataSet FetchExpiryReportHdr(string OperatorID = "", string OperatorName = "", string CustomerID = "", string CustomerName = "", string CONTRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string CargoType = "",
        string FromDt = "", string Todt = "", int STATUS = 0, bool ActiveOnly = true, string usrLocFK = "", string SearchType = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            try
            {
                string SrOP = (SearchType == "C" ? "%" : "");
                // OPERATOR
                if (OperatorID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(OPERATOR_ID) LIKE '" + SrOP + OperatorID.ToUpper().Replace("'", "''") + "%'");
                }
                if (OperatorName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(OPERATOR_NAME) LIKE '" + SrOP + OperatorName.ToUpper().Replace("'", "''") + "%'");
                }
                // CUSTOMER
                if (CustomerID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUSTOMER_ID) LIKE '" + SrOP + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }
                if (CustomerName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUSTOMER_NAME) LIKE '" + SrOP + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                // CONT REF NO
                if (CONTRefNo.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CONT_REF_NO) LIKE '" + SrOP + CONTRefNo.ToUpper().Replace("'", "''") + "%'");
                }
                // PORT OF LOADING
                if (POLID.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (POLName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                // PORT OF DISCHARGE
                if (PODID.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (PODName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_SEA_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_SEA_FK = main.CONT_CUST_SEA_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }

                if (STATUS != 5)
                {
                    buildCondition.Append(" AND main.STATUS = " + STATUS);
                }

                if (CargoType.Length > 0)
                {
                    buildCondition.Append(" AND CARGO_TYPE = " + CargoType);
                }
                if (CustomerID.Length > 0)
                {
                    buildCondition.Append(" and CUSTOMER_MST_TBL.CUSTOMER_ID = '" + CustomerID + "'");
                }
                //If FromDt.Length > 0 Then
                //    buildCondition.Append(vbCrLf & " and to_date(main.valid_from,'dd/MM/yyyy') >= to_date('" & FromDt & "','dd/MM/yyyy')")
                //End If
                //If Todt.Length > 0 Then
                //    buildCondition.Append(vbCrLf & "  and to_date(main.valid_to,'dd/MM/yyyy') <= to_date('" & Todt & "','dd/MM/yyyy')")
                //End If
                if (Todt.Length > 0 & FromDt.Length > 0)
                {
                    buildCondition.Append(" AND (to_date(MAIN.VALID_FROM, dateformat)>= TO_DATE('" + FromDt + "', 'dd/MM/yyyy') or to_date(MAIN.VALID_TO, dateformat)>=TO_DATE('" + FromDt + "', 'dd/MM/yyyy') OR MAIN.VALID_TO IS NULL)");
                    buildCondition.Append(" AND to_date(MAIN.VALID_FROM, dateformat)<=TO_DATE('" + Todt + "', 'dd/MM/yyyy')");
                }
                else if (Todt.Length > 0 & !(FromDt.Length > 0))
                {
                    buildCondition.Append(" AND to_date(MAIN.VALID_FROM, dateformat)<=TO_DATE('" + Todt + "', 'dd/MM/yyyy')");
                }
                else if (FromDt.Length > 0 & !(Todt.Length > 0))
                {
                    buildCondition.Append(" AND (to_date(MAIN.VALID_FROM, dateformat)>= TO_DATE('" + FromDt + "', 'dd/MM/yyyy') or to_date(MAIN.VALID_TO, dateformat)>=TO_DATE('" + FromDt + "', 'dd/MM/yyyy') OR MAIN.VALID_TO IS NULL)");
                }

                strCondition = buildCondition.ToString();
                sb.Append(" Select distinct  main.CUSTOMER_MST_FK,");
                sb.Append("                        CUSTOMER_ID,");
                sb.Append("                        CUSTOMER_NAME,");
                sb.Append("                        decode(CARGO_TYPE,0,'',1,'FCL',2,'LCL') CARGO_TYPE,");
                sb.Append("                        BIZ.BUSINESS_TYPE_DISPLAY");
                sb.Append("           from CONT_CUST_SEA_TBL main,");
                sb.Append("               CUSTOMER_MST_TBL,");
                sb.Append("               USER_MST_TBL UMT,");
                sb.Append("               BUSINESS_TYPE_MST_TBL BIZ,");
                sb.Append("               OPERATOR_MST_TBL");
                sb.Append("           where main.CUSTOMER_MST_FK = CUSTOMER_MST_PK");
                sb.Append("           ANd UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                sb.Append("           AND main.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("           AND BIZ.BUSINESS_TYPE = 2");
                sb.Append("           and main.OPERATOR_MST_FK = OPERATOR_MST_PK(+)");
                sb.Append("  " + strCondition);
                sb.Append("           and (main.VALID_TO < TO_DATE(SYSDATE, '" + dateFormat + "') and ");
                sb.Append("           main.VALID_TO IS not NULL)");

                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Expiry Report"
    }
}