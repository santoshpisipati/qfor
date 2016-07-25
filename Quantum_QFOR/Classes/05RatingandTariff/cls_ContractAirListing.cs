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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_ContractAirListing : CommonFeatures
    {
        #region " Fetch All "

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="AirlineID">The airline identifier.</param>
        /// <param name="AirlineName">Name of the airline.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="CONTRefNo">The cont reference no.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
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
        /// <param name="lngLocPk">The LNG loc pk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string AirlineID = "", string AirlineName = "", string CustomerID = "", string CustomerName = "", string CONTRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string Commodityfk = "",
        string FromDate = "", string ToDate = "", int STATUS = 0, bool ActiveOnly = true, string SearchType = "", string SortColumn = " CONT_DATE ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", string CurrBizType = "3",
        long lngLocPk = 0, Int32 flag = 0)
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

            // AIRLINE
            if (AirlineID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AIRLINE_ID) LIKE '" + SrOP + AirlineID.ToUpper().Replace("'", "''") + "%'");
            }
            if (AirlineName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AIRLINE_NAME) LIKE '" + SrOP + AirlineName.ToUpper().Replace("'", "''") + "%'");
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
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
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
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }

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

            if (STATUS != 5)
            {
                buildCondition.Append(" AND main.CONT_APPROVED = " + STATUS);
            }

            if (ActiveOnly == true)
            {
                buildCondition.Append(" and main.active = 1");
            }
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            strCondition = buildCondition.ToString();
            buildQuery.Append(" Select count(*) ");
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_CUST_AIR_TBL main, ");
            buildQuery.Append("       AIRLINE_MST_TBL, ");
            buildQuery.Append("       commodity_group_mst_tbl cgmt ,");
            //'
            buildQuery.Append("       CUSTOMER_MST_TBL,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            buildQuery.Append("       main.AIRLINE_MST_FK = AIRLINE_MST_PK(+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK ");
            buildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + " ");
            buildQuery.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK");
            buildQuery.Append("      AND  main.Commodity_Group_Mst_Fk=commodity_group_pk(+)");
            buildQuery.Append("      " + strCondition);

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

            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select ");
            buildQuery.Append("       CONT_CUST_AIR_PK, ");
            buildQuery.Append("        main.active ACTIVE, ");
            buildQuery.Append("        CUSTOMER_NAME, ");
            buildQuery.Append("       CONT_REF_NO, ");
            buildQuery.Append("       to_date(main.CONT_DATE, '" + dateFormat + "') CONT_DATE, ");
            buildQuery.Append("       AIRLINE_MST_FK, ");
            buildQuery.Append("       AIRLINE_ID, ");
            buildQuery.Append("       AIRLINE_NAME, ");
            buildQuery.Append("       main.CUSTOMER_MST_FK, ");
            buildQuery.Append("       CUSTOMER_ID, ");
            buildQuery.Append("       0 POL, ");
            buildQuery.Append("       0 POD, ");
            buildQuery.Append("       cgmt.commodity_group_code, ");
            //'
            buildQuery.Append("       to_date(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_date(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            buildQuery.Append("       DECODE(MAIN.CONT_APPROVED, 0, 'Work In Progress', 1, 'Internal Approved', 2, 'Customer Approved', 3, 'Internal Rejected',4, 'Cancel') STATUS,");
            buildQuery.Append("       main.restricted");
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_CUST_AIR_TBL main, ");
            buildQuery.Append("       AIRLINE_MST_TBL, ");
            buildQuery.Append("       commodity_group_mst_tbl cgmt ,");
            //'
            buildQuery.Append("       CUSTOMER_MST_TBL CUST,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            buildQuery.Append("       main.AIRLINE_MST_FK = AIRLINE_MST_PK (+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK ");
            buildQuery.Append("      AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + " ");
            buildQuery.Append("     AND main.CREATED_BY_FK = UMT.USER_MST_PK");
            buildQuery.Append("      AND  main.Commodity_Group_Mst_Fk=commodity_group_pk(+)");
            //'
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("     Order By to_date(cont_date) desc");
            buildQuery.Append("  ,CONT_REF_NO DESC");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);

            strSQL = buildQuery.ToString();

            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CONT_CUST_AIR_PK"], DS.Tables[1].Columns["CONT_CUST_AIR_FK"], true);
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
        /// Fetches the exp all.
        /// </summary>
        /// <param name="AirlineID">The airline identifier.</param>
        /// <param name="AirlineName">Name of the airline.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="CONTRefNo">The cont reference no.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
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
        /// <param name="lngLocPk">The LNG loc pk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchExpAll(string AirlineID = "", string AirlineName = "", string CustomerID = "", string CustomerName = "", string CONTRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string Commodityfk = "",
        string FromDate = "", string ToDate = "", int STATUS = 0, bool ActiveOnly = true, string SearchType = "", string SortColumn = " CONT_DATE ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", string CurrBizType = "3",
        long lngLocPk = 0, Int32 flag = 0)
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

            // AIRLINE
            if (AirlineID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AIRLINE_ID) LIKE '" + SrOP + AirlineID.ToUpper().Replace("'", "''") + "%'");
            }
            if (AirlineName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AIRLINE_NAME) LIKE '" + SrOP + AirlineName.ToUpper().Replace("'", "''") + "%'");
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
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
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
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }

            //If ToDate.Length > 0 And FromDate.Length > 0 Then
            //    buildCondition.Append(" AND (TO_DATE('" & FromDate & "', 'dd/MM/yyyy') <= TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "')) ")
            //    buildCondition.Append(" AND (TO_DATE('" & ToDate & "', 'dd/MM/yyyy') >=  TO_DATE(MAIN.VALID_TO,'" & dateFormat & "') ")
            //    buildCondition.Append("  OR  (MAIN.VALID_TO IS NULL))    ")

            //ElseIf ToDate.Length > 0 And Not FromDate.Length > 0 Then
            //    buildCondition.Append(" AND (TO_DATE('" & ToDate & "', 'dd/MM/yyyy') >=  TO_DATE(MAIN.VALID_TO,'" & dateFormat & "') ")
            //    buildCondition.Append("  OR  (MAIN.VALID_TO IS NULL))    ")
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

            // APPROVED OR NOT APPROVED
            if (STATUS != 5)
            {
                buildCondition.Append(" AND main.CONT_APPROVED = " + STATUS);
            }

            // ACTIVE ONLY - Snigdharani - 11/11/2008,12/12/2008
            if (ActiveOnly == true)
            {
                buildCondition.Append(" and main.active = 1");
                buildCondition.Append(" AND ( ");
                buildCondition.Append("        main.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ");
                buildCondition.Append("       OR main.VALID_TO IS NULL ");
                buildCondition.Append("     ) ");
            }
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            strCondition = buildCondition.ToString();
            buildQuery.Append(" Select count(*) ");
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_CUST_AIR_TBL main, ");
            buildQuery.Append("       AIRLINE_MST_TBL, ");
            buildQuery.Append(" commodity_group_mst_tbl cgmt, ");
            //'
            buildQuery.Append("       CUSTOMER_MST_TBL,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            buildQuery.Append("       main.AIRLINE_MST_FK = AIRLINE_MST_PK(+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK ");
            buildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + " ");
            buildQuery.Append("                   and (main.VALID_TO < TO_DATE(SYSDATE, '" + dateFormat + "') and ");
            buildQuery.Append("                   main.VALID_TO IS not NULL)");
            buildQuery.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK");
            buildQuery.Append(" AND  main.Commodity_Group_Mst_Fk = commodity_group_pk(+) ");
            //'
            buildQuery.Append("      " + strCondition);

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
            buildQuery.Append("       CONT_CUST_AIR_PK, ");
            buildQuery.Append("        main.active ACTIVE, ");
            buildQuery.Append("        CUSTOMER_NAME, ");
            buildQuery.Append("       CONT_REF_NO, ");
            buildQuery.Append("       to_date(main.CONT_DATE, '" + dateFormat + "') CONT_DATE, ");
            buildQuery.Append("       AIRLINE_MST_FK, ");
            buildQuery.Append("       AIRLINE_ID, ");
            buildQuery.Append("       AIRLINE_NAME, ");
            buildQuery.Append("       main.CUSTOMER_MST_FK, ");
            buildQuery.Append("       CUSTOMER_ID, ");
            buildQuery.Append("       0 POL, ");
            buildQuery.Append("       0 POD, ");
            buildQuery.Append("       cgmt.commodity_group_code, ");
            //'
            buildQuery.Append("       to_date(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_date(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            buildQuery.Append("       DECODE(MAIN.CONT_APPROVED, 0, 'Work In Progress', 1, 'Internal Approved', 2, 'Customer Approved', 3, 'Internal Rejected',4, 'Cancel') STATUS, ");
            buildQuery.Append("       main.restricted");
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_CUST_AIR_TBL main, ");
            buildQuery.Append("       AIRLINE_MST_TBL, ");
            buildQuery.Append("       COMMODITY_GROUP_MST_TBL CGMT ,");
            //'
            buildQuery.Append("       CUSTOMER_MST_TBL CUST,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            buildQuery.Append("       main.AIRLINE_MST_FK = AIRLINE_MST_PK (+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK ");
            buildQuery.Append("      AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + " ");
            buildQuery.Append("     AND main.CREATED_BY_FK = UMT.USER_MST_PK");
            buildQuery.Append("      AND  main.Commodity_Group_Mst_Fk = commodity_group_pk(+) ");
            //'
            buildQuery.Append("                   and (main.VALID_TO < TO_DATE(SYSDATE, '" + dateFormat + "') and ");
            buildQuery.Append("                   main.VALID_TO IS not NULL)");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("     Order By " + SortColumn + SortType);
            buildQuery.Append("  ,CONT_REF_NO DESC");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);

            strSQL = buildQuery.ToString();

            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CONT_CUST_AIR_PK"], DS.Tables[1].Columns["CONT_CUST_AIR_FK"], true);
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
                strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["CONT_CUST_AIR_PK"]).Trim() + ",");
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
                buildCondition.Append(" and CONT_CUST_AIR_FK in (" + CONTSpotPKs + ") ");
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
            buildQuery.Append("    ( Select ");
            buildQuery.Append("       CONT_CUST_AIR_FK, ");
            buildQuery.Append("       PORT_MST_POL_FK, ");
            buildQuery.Append("       PORTPOL.PORT_ID PORT_POL_ID, ");
            buildQuery.Append("       PORTPOL.PORT_NAME PORT_POL_NAME, ");
            buildQuery.Append("       PORT_MST_POD_FK, ");
            buildQuery.Append("       PORTPOD.PORT_ID PORT_POD_ID, ");
            buildQuery.Append("       PORTPOD.PORT_NAME PORT_POD_NAME, ");
            buildQuery.Append("       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO ");
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_CUST_TRN_AIR_TBL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOD ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK AND ");
            buildQuery.Append("       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("      Order By CONT_CUST_AIR_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
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
                    //if (dt.Rows[RowCnt]["CONT_CUST_AIR_FK"] != pk)
                    //{
                    //    pk = dt.Rows[RowCnt]["CONT_CUST_AIR_FK"];
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

        #region " Enhance Search Function for CONT for CUSTOMER-AIRLINE "

        /// <summary>
        /// Fetches the cont for airline customer.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCONTForAirlineCustomer(string strCond)
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
            //    strLOC_MST_IN = 0;

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CONT_REF_NO_PKG.GET_AIRLINE_CONT_COMMON";
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

        #endregion " Enhance Search Function for CONT for CUSTOMER-AIRLINE "

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

        #region "Fetch Expiry Report"

        /// <summary>
        /// Fetches the customer.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <returns></returns>
        public DataSet FetchCustomer(long CustPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("select cmt.customer_id, cmt.customer_name from customer_mst_tbl cmt where cmt.customer_mst_pk = " + CustPK);

                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Expiry Report"

        #region "Fetch Expiry Report"

        /// <summary>
        /// Fetches the expiry report HDR.
        /// </summary>
        /// <param name="AirlineID">The airline identifier.</param>
        /// <param name="AirlineName">Name of the airline.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="CONTRefNo">The cont reference no.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="STATUS">The status.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet FetchExpiryReportHdr(string AirlineID = "", string AirlineName = "", string CustomerID = "", string CustomerName = "", string CONTRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string FromDt = "",
        string Todt = "", int STATUS = 0, string SearchType = "", long usrLocFK = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;

            try
            {
                string SrOP = (SearchType == "C" ? "%" : "");

                // AIRLINE
                if (AirlineID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(ATL.AIRLINE_ID) LIKE '" + SrOP + AirlineID.ToUpper().Replace("'", "''") + "%'");
                }
                if (AirlineName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(ATL.AIRLINE_NAME) LIKE '" + SrOP + AirlineName.ToUpper().Replace("'", "''") + "%'");
                }
                // CUSTOMER
                if (CustomerID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUST.CUSTOMER_ID) LIKE '" + SrOP + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }
                if (CustomerName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUST.CUSTOMER_NAME) LIKE '" + SrOP + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                // CONT REF NO
                if (CONTRefNo.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(main.CONT_REF_NO) LIKE '" + SrOP + CONTRefNo.ToUpper().Replace("'", "''") + "%'");
                }
                // PORT OF LOADING
                if (POLID.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_AIR_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (POLName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_AIR_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
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
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_AIR_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (PODName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_AIR_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }

                //If CustomerID.Length > 0 Then
                //    buildCondition.Append(vbCrLf & " and CUST.CUSTOMER_ID = '" & CustomerID & "'")
                //End If

                if (STATUS != 5)
                {
                    buildCondition.Append(" AND main.CONT_APPROVED = " + STATUS);
                }
                //If Todt.Length > 0 And FromDt.Length > 0 Then
                //    buildCondition.Append(" AND (TO_DATE('" & FromDt & "', 'dd/MM/yyyy') <= TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "')) ")
                //    buildCondition.Append(" AND (TO_DATE('" & Todt & "', 'dd/MM/yyyy') >=  TO_DATE(MAIN.VALID_TO,'" & dateFormat & "') ")
                //    buildCondition.Append("  OR  (MAIN.VALID_TO IS NULL))    ")

                //ElseIf Todt.Length > 0 And Not FromDt.Length > 0 Then
                //    buildCondition.Append(" AND (TO_DATE('" & Todt & "', 'dd/MM/yyyy') >=  TO_DATE(MAIN.VALID_TO,'" & dateFormat & "') ")
                //    buildCondition.Append("  OR  (MAIN.VALID_TO IS NULL))    ")
                //ElseIf FromDt.Length > 0 And Not Todt.Length > 0 Then
                //    buildCondition.Append(" AND (TO_DATE('" & FromDt & "', 'dd/MM/yyyy') <= TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "')) ")
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

                sb.Append(" Select main.CUSTOMER_MST_FK,");
                sb.Append("              CUSTOMER_ID,");
                sb.Append("              CUSTOMER_NAME,");
                sb.Append("              '' CARGO_TYPE,");
                sb.Append("              BIZ.BUSINESS_TYPE_DISPLAY");
                sb.Append("              from CONT_CUST_AIR_TBL main,");
                sb.Append("                   CUSTOMER_MST_TBL CUST,");
                sb.Append("                   USER_MST_TBL UMT,");
                sb.Append("                   BUSINESS_TYPE_MST_TBL BIZ,AIRLINE_MST_TBL ATL");
                sb.Append("              where main.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("              AND UMT.DEFAULT_LOCATION_FK =" + usrLocFK + " ");
                sb.Append("              AND BIZ.BUSINESS_TYPE = 1");
                sb.Append("              AND main.AIRLINE_MST_FK = ATL.AIRLINE_MST_PK(+)");
                sb.Append("              AND main.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("  " + strCondition);
                //sb.Append("              and main.active = 1")
                sb.Append("              and (main.VALID_TO < TO_DATE(SYSDATE, '" + dateFormat + "') and ");
                sb.Append("              main.VALID_TO IS not NULL)");

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
        /// Fetches the expiry report.
        /// </summary>
        /// <param name="AirlineID">The airline identifier.</param>
        /// <param name="AirlineName">Name of the airline.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="CONTRefNo">The cont reference no.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="STATUS">The status.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="BaseCurrPk">The base curr pk.</param>
        /// <returns></returns>
        public DataSet FetchExpiryReport(string AirlineID = "", string AirlineName = "", string CustomerID = "", string CustomerName = "", string CONTRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string FromDt = "",
        string Todt = "", int STATUS = 0, string SearchType = "", long usrLocFK = 0, long BaseCurrPk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            try
            {
                string SrOP = (SearchType == "C" ? "%" : "");

                // AIRLINE
                if (AirlineID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(ATL.AIRLINE_ID) LIKE '" + SrOP + AirlineID.ToUpper().Replace("'", "''") + "%'");
                }
                if (AirlineName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(ATL.AIRLINE_NAME) LIKE '" + SrOP + AirlineName.ToUpper().Replace("'", "''") + "%'");
                }
                // CUSTOMER
                if (CustomerID.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUST.CUSTOMER_ID) LIKE '" + SrOP + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }
                if (CustomerName.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CUST.CUSTOMER_NAME) LIKE '" + SrOP + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                // CONT REF NO
                if (CONTRefNo.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(main.CONT_REF_NO) LIKE '" + SrOP + CONTRefNo.ToUpper().Replace("'", "''") + "%'");
                }
                // PORT OF LOADING
                if (POLID.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_AIR_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (POLName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_CUST_TRN_AIR_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
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
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_AIR_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }
                if (PODName.Length > 0)
                {
                    buildCondition.Append(" AND EXISTS ");
                    buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                    buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                    buildCondition.Append("       and PORT_MST_PK IN ");
                    buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_CUST_TRN_AIR_TBL ");
                    buildCondition.Append("           WHERE CONT_CUST_AIR_FK = main.CONT_CUST_AIR_PK ");
                    buildCondition.Append("         ) ");
                    buildCondition.Append("     ) ");
                }

                //If CustomerID.Length > 0 Then
                //    buildCondition.Append(vbCrLf & " and CUST.CUSTOMER_ID = '" & CustomerID & "'")
                //End If

                if (STATUS != 5)
                {
                    buildCondition.Append(" AND main.CONT_APPROVED = " + STATUS);
                }

                //If Todt.Length > 0 And FromDt.Length > 0 Then
                //    buildCondition.Append(" AND (TO_DATE('" & FromDt & "', 'dd/MM/yyyy') <= TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "')) ")
                //    buildCondition.Append(" AND (TO_DATE('" & Todt & "', 'dd/MM/yyyy') >=  TO_DATE(MAIN.VALID_TO,'" & dateFormat & "') ")
                //    buildCondition.Append("  OR  (MAIN.VALID_TO IS NULL))    ")

                //ElseIf Todt.Length > 0 And Not FromDt.Length > 0 Then
                //    buildCondition.Append(" AND (TO_DATE('" & Todt & "', 'dd/MM/yyyy') >=  TO_DATE(MAIN.VALID_TO,'" & dateFormat & "') ")
                //    buildCondition.Append("  OR  (MAIN.VALID_TO IS NULL))    ")
                //ElseIf FromDt.Length > 0 And Not Todt.Length > 0 Then
                //    buildCondition.Append(" AND (TO_DATE('" & FromDt & "', 'dd/MM/yyyy') <= TO_DATE(MAIN.VALID_FROM,'" & dateFormat & "')) ")
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
                sb.Append("                 Select  rownum as SRNO,");
                sb.Append("                             q.CONT_CUST_AIR_PK,");
                sb.Append("                             q.CUSTOMER_NAME,");
                sb.Append("                             q.CONT_REF_NO,");
                sb.Append("                             q.AIRLINE_ID,");
                sb.Append("                             q.AIRLINE_NAME,");
                sb.Append("                             q.CUSTOMER_MST_FK,");
                sb.Append("                             q.CUSTOMER_ID,");
                sb.Append("                             q.AOL,");
                sb.Append("                             q.AOD,");
                sb.Append("                             q.VALID_FROM,");
                sb.Append("                             q.VALID_TO,");
                sb.Append("                             q.commodity_name,");
                sb.Append("                             q.min_amount,");
                sb.Append("                             q.comm_grp");
                sb.Append("                  from (Select  distinct   CONT_CUST_AIR_PK,");
                sb.Append("                             CUSTOMER_NAME,");
                sb.Append("                             CONT_REF_NO,");
                sb.Append("                             ATL.AIRLINE_ID,");
                sb.Append("                             ATL.AIRLINE_NAME,");
                sb.Append("                             main.CUSTOMER_MST_FK,");
                sb.Append("                             CUSTOMER_ID,");
                sb.Append("                             AOO.PORT_NAME AOL,");
                sb.Append("                             AOD.PORT_NAME AOD,");
                sb.Append("                             main.VALID_FROM,");
                sb.Append("                             main.VALID_TO,");
                sb.Append("                             com.commodity_name,");
                sb.Append("                             cgm.commodity_group_code comm_grp,");
                sb.Append(" sum(cfrt.min_amount * get_ex_rate(cfrt.currency_mst_fk, " + BaseCurrPk + ",main.cont_date)) min_amount");
                sb.Append("                 from CONT_CUST_AIR_TBL main,");
                sb.Append("                      CONT_CUST_TRN_AIR_TBL CTRN, ");
                sb.Append("                      AIRLINE_MST_TBL ATL,");
                sb.Append("                      CUSTOMER_MST_TBL CUST,");
                sb.Append("                      USER_MST_TBL UMT,");
                sb.Append("                      PORT_MST_TBL AOO,");
                sb.Append("                      PORT_MST_TBL AOD,");
                sb.Append("                      cont_cust_air_freight_tbl cfrt,");
                sb.Append("                     commodity_mst_tbl com,");
                sb.Append("                     commodity_group_mst_tbl cgm,");
                sb.Append("                     currency_type_mst_tbl curr");
                sb.Append("                 where main.AIRLINE_MST_FK = ATL.AIRLINE_MST_PK(+)");
                sb.Append("                   AND main.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("                   AND MAIN.CONT_CUST_AIR_PK = CTRN.CONT_CUST_AIR_FK");
                sb.Append("                   AND AOO.PORT_MST_PK = CTRN.PORT_MST_POL_FK");
                sb.Append("                   AND AOD.PORT_MST_PK = CTRN.PORT_MST_POD_FK");
                sb.Append("                   AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                sb.Append("                   and cfrt.cont_cust_trn_air_fk(+) = ctrn.cont_cust_air_fk");
                sb.Append("                   AND main.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("                   and com.commodity_mst_pk(+) = main.commodity_mst_fk");
                //sb.Append("                   and main.active = 1")
                sb.Append("                    and main.commodity_group_mst_fk = cgm.commodity_group_pk(+) ");
                sb.Append("                    and cfrt.currency_mst_fk = curr.currency_mst_pk(+)");
                sb.Append("  " + strCondition);
                sb.Append("                   and (main.VALID_TO < TO_DATE(SYSDATE, '" + dateFormat + "') and");
                sb.Append("                   main.VALID_TO IS not NULL)");
                sb.Append("                   group by  CONT_CUST_AIR_PK,");
                sb.Append("                             CUSTOMER_NAME,");
                sb.Append("                             CONT_REF_NO,");
                sb.Append("                             AIRLINE_ID,");
                sb.Append("                             AIRLINE_NAME,");
                sb.Append("                             main.CUSTOMER_MST_FK,");
                sb.Append("                             CUSTOMER_ID,");
                sb.Append("                             CUST.CUSTOMER_NAME,");
                sb.Append("                             AOO.PORT_NAME,");
                sb.Append("                             AOD.PORT_NAME,");
                sb.Append("                             main.VALID_FROM,");
                sb.Append("                             main.VALID_TO,");
                sb.Append("                             com.commodity_name,cgm.commodity_group_code");
                sb.Append("                   Order By CONT_REF_NO DESC,main.valid_to desc ) q");
                sb.Append("     ");

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

        #endregion "Fetch Expiry Report"
    }
}