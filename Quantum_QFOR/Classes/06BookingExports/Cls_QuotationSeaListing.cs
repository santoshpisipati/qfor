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
    public class Cls_QuotationSeaListing : CommonFeatures
    {
        #region " Fetch All "

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="QuotationNo">The quotation no.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="AgentID">The agent identifier.</param>
        /// <param name="AgentName">Name of the agent.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="ApprovalStatus">The approval status.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="lngUserLocFk">The LNG user loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="FromPage">From page.</param>
        /// <param name="CustomerApproved">if set to <c>true</c> [customer approved].</param>
        /// <param name="Priority">The priority.</param>
        /// <returns></returns>
        public DataSet FetchAll(string QuotationNo = "", string FromDate = "", string ToDate = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string CustomerID = "", string CustomerName = "", string AgentID = "",
        string AgentName = "", string CargoType = "", Int16 ApprovalStatus = 1, string SearchType = "", string SortColumn = " QUOTATION_REF_NO ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", long lngUserLocFk = 0, Int32 flag = 0,
        string FromPage = "", bool CustomerApproved = false, int Priority = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);

            string strSQL = "";
            string strCondition = "";
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            string SrOP = (SearchType == "C" ? "%" : "");
            //  SrOP is Search Operator..[% or Nothing] >

            // Quotation NO
            if (QuotationNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(QUOTATION_REF_NO) LIKE '" + SrOP + QuotationNo.ToUpper().Replace("'", "''") + "%'");
            }
            // VALIDITY
            if (FromDate.Length > 0)
            {
                buildCondition.Append(" AND (main.QUOTATION_DATE + main.VALID_FOR) >= TO_DATE('" + FromDate + "' , '" + dateFormat + "') ");
            }
            if (ToDate.Length > 0)
            {
                buildCondition.Append(" AND main.QUOTATION_DATE <= TO_DATE('" + ToDate + "' , '" + dateFormat + "') ");
            }
            // PORT PAIR
            // PORT OF LOADING
            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(portpol.PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%'");
            }
            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(portpol.PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%'");
            }
            // PORT OF DISCHARGE
            if (PODID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(portpod.PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(portpod.PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%'");
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
            // AGENT
            if (AgentID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AGENT_ID) LIKE '" + SrOP + AgentID.ToUpper().Replace("'", "''") + "%'");
            }
            if (AgentName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AGENT_NAME) LIKE '" + SrOP + AgentName.ToUpper().Replace("'", "''") + "%'");
            }
            // CARGO TYPE
            if (CargoType.Length > 0)
            {
                buildCondition.Append(" AND CARGO_TYPE in ( " + CargoType + ")");
            }
            // APPROVAL STATUS
            if (ApprovalStatus != 5)
            {
                buildCondition.Append(" AND main.STATUS = " + ApprovalStatus);
            }
            //CUSTOMER APPROVAL STATUS
            if (CustomerApproved)
            {
                buildCondition.Append(" AND main.CUSTOMER_APPROVED = 1");
            }
            if (Priority > 0)
            {
                buildCondition.Append("  and cust.PRIORITY=" + Priority);
            }
            if (!string.IsNullOrEmpty(FromPage))
            {
                //'Export
                if (FromPage == "simple" | FromPage == "approval")
                {
                    buildCondition.Append(" AND main.FROM_FLAG = 0");
                    //'Import
                }
                else
                {
                    buildCondition.Append(" AND main.FROM_FLAG = 1");
                }
            }
            //USER LOC FK
            buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUserLocFk + " ");
            buildCondition.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK ");

            strCondition = buildCondition.ToString();

            strBuilder.Append(" Select count(DISTINCT QUOTATION_SEA_PK) ");
            strBuilder.Append(" from ");
            strBuilder.Append(" QUOTATION_SEA_TBL main,");
            strBuilder.Append(" QUOTATION_TRN_SEA_FCL_LCL tran,");
            strBuilder.Append(" V_ALL_CUSTOMER cust,");
            strBuilder.Append(" AGENT_MST_TBL agnt,");
            strBuilder.Append(" PORT_MST_TBL portpol, ");
            strBuilder.Append(" PORT_MST_TBL portpod,");
            strBuilder.Append(" USER_MST_TBL UMT");
            strBuilder.Append(" where ");
            strBuilder.Append(" main.QUOTATION_SEA_PK = tran.QUOTATION_SEA_FK");
            strBuilder.Append(" AND main.CUSTOMER_MST_FK = cust.CUSTOMER_MST_PK(+)");
            strBuilder.Append(" AND MAIN.CUST_TYPE=CUST.CUSTOMER_TYPE(+)");
            strBuilder.Append(" AND main.AGENT_MST_FK = agnt.AGENT_MST_PK(+)");
            strBuilder.Append(" AND tran.PORT_MST_POL_FK = portpol.PORT_MST_PK");
            strBuilder.Append(" AND tran.PORT_MST_POD_FK = portpod.PORT_MST_PK" + strCondition);

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strBuilder.ToString()));
            // Getting No of satisfying records.

            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
                TotalPage += 1;
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            strBuilder.Remove(0, strBuilder.Length);
            strBuilder.Append(" Select * from");
            strBuilder.Append("( Select ROWNUM SR_NO, q.* from ");
            strBuilder.Append("  ( Select  DISTINCT ");
            strBuilder.Append(" main.QUOTATION_SEA_PK, ");
            strBuilder.Append(" main.QUOTATION_REF_NO, ");
            strBuilder.Append(" main.QUOTATION_DATE QUOTATION_DATE,");
            strBuilder.Append(" main.CUSTOMER_MST_FK,");
            strBuilder.Append(" cust.CUSTOMER_ID,");
            strBuilder.Append(" cust.CUSTOMER_NAME,");
            strBuilder.Append("  DECODE(cust.PRIORITY, 1, 'Priority 1', 2, 'Priority 2', 3, 'Priority 3',4, 'Priority 4',5, 'Priority 5') PRIORITY, ");
            strBuilder.Append(" main.AGENT_MST_FK,");
            strBuilder.Append(" agnt.AGENT_ID,");
            strBuilder.Append(" agnt.AGENT_NAME,");
            strBuilder.Append(" Decode(main.CARGO_TYPE, 1, 'FCL',2, 'LCL',4,'BBC') CARGO_TYPE,");
            strBuilder.Append(" DECODE(main.STATUS,");
            strBuilder.Append("                              1,");
            strBuilder.Append("                              'Active',");
            strBuilder.Append("                              2,");
            strBuilder.Append("                              'Confirm',");
            strBuilder.Append("                              3,");
            strBuilder.Append("                              'Cancelled',");
            strBuilder.Append("                              4,");
            strBuilder.Append("                              'Used',");
            strBuilder.Append("                              5,");
            strBuilder.Append("                              'All') STATUS,");
            strBuilder.Append(" NVL(main.CUSTOMER_APPROVED,0) CUSTOMER_APPROVED ");
            strBuilder.Append(" from ");
            strBuilder.Append(" QUOTATION_SEA_TBL main,");
            strBuilder.Append(" QUOTATION_TRN_SEA_FCL_LCL tran, ");
            strBuilder.Append(" V_ALL_CUSTOMER cust, ");
            strBuilder.Append(" AGENT_MST_TBL agnt, ");
            strBuilder.Append(" PORT_MST_TBL portpol,");
            strBuilder.Append(" PORT_MST_TBL portpod, ");
            strBuilder.Append(" USER_MST_TBL UMT ");
            strBuilder.Append(" where ");
            strBuilder.Append(" main.QUOTATION_SEA_PK = tran.QUOTATION_SEA_FK ");
            strBuilder.Append(" AND main.CUSTOMER_MST_FK = cust.CUSTOMER_MST_PK(+) ");
            strBuilder.Append(" AND MAIN.CUST_TYPE=CUST.CUSTOMER_TYPE(+)");

            strBuilder.Append(" AND main.AGENT_MST_FK = agnt.AGENT_MST_PK(+) ");
            strBuilder.Append(" AND tran.PORT_MST_POL_FK = portpol.PORT_MST_PK ");
            strBuilder.Append(" AND tran.PORT_MST_POD_FK = portpod.PORT_MST_PK " + strCondition);
            strBuilder.Append(" Order By " + SortColumn + SortType);
            strBuilder.Append(" ,QUOTATION_REF_NO DESC");
            strBuilder.Append("  ) q) where  SR_NO between " + start + " and " + last);

            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(strBuilder.ToString());
                DS.Tables.Add(FetchChilds(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                DataRelation rfqRel = new DataRelation("RFQRelation", DS.Tables[0].Columns["QUOTATION_SEA_PK"], DS.Tables[1].Columns["QUOTATION_SEA_PK"], true);
                DS.Relations.Add(rfqRel);
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

        #endregion " Fetch All "

        #region " All Master Quotation PKs "

        /// <summary>
        /// Alls the master p ks.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        private string AllMasterPKs(DataSet ds)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                strBuilder.Append("-1,");
                for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
                {
                    strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["QUOTATION_SEA_PK"]).Trim() + ",");
                }
                strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
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

        #endregion " All Master Quotation PKs "

        #region " Fetch Child Rows "

        /// <summary>
        /// Fetches the childs.
        /// </summary>
        /// <param name="QuotationPKs">The quotation p ks.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="POLName">Name of the pol.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="PODName">Name of the pod.</param>
        /// <param name="SrOp">The sr op.</param>
        /// <returns></returns>
        private DataTable FetchChilds(string QuotationPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {
            string strSQL = "";
            string strCondition = "";
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();

            if (QuotationPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and QUOTATION_SEA_PK in (" + QuotationPKs + ") ");
            }
            if (POLID.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(portpol.PORT_ID) LIKE '" + SrOp + POLID.ToUpper().Replace("'", "''") + "%'");
            }
            if (POLName.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(portpol.PORT_NAME) LIKE '" + SrOp + POLName.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODID.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(portpod.PORT_ID) LIKE '" + SrOp + PODID.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODName.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(portpod.PORT_NAME) LIKE '" + SrOp + PODName.ToUpper().Replace("'", "''") + "%'");
            }

            strCondition = buildCondition.ToString();

            strSQL = "  Select   DISTINCT                                                       " + "     main.QUOTATION_SEA_PK,                                                " + "     tran.PORT_MST_POL_FK,                                                 " + "     portpol.PORT_ID,                                                      " + "     portpol.PORT_NAME,                                                    " + "     tran.PORT_MST_POD_FK,                                                 " + "     portpod.PORT_ID,                                                      " + "     portpod.PORT_NAME,                                                    " + "     TO_CHAR(main.QUOTATION_DATE,'" + dateFormat + "') VALID_FROM,                 " + "     To_CHAR(main.QUOTATION_DATE + main.VALID_FOR,'" + dateFormat + "') VALID_TO,  " + "     tran.OPERATOR_MST_FK,                                                 " + "     opr.OPERATOR_ID,                                                      " + "     opr.OPERATOR_NAME,                                                    " + "     CASE WHEN MAIN.Cargo_Type=4 THEN  TRAN.Commodity_Mst_Fk ELSE TRAN.COMMODITY_GROUP_FK END COMMODITY_GROUP_FK,                                             " + "     CASE WHEN MAIN.Cargo_Type=4 THEN 'BBC' WHEN MAIN.CARGO_TYPE =2 THEN  'LCL'WHEN MAIN.CARGO_TYPE =1 THEN 'FCL' END CARGO_TYPE,  " + "     CASE WHEN MAIN.Cargo_Type=4 THEN cmt.COMMODITY_NAME ELSE CGRP.COMMODITY_GROUP_CODE END COMMODITY_GROUP_CODE,                                          " + "         DECODE(main.STATUS, " + "                              1," + "                              'Active'," + "                              2," + "                              'Confirm'," + "                              3," + "                              'Cancelled'," + "                              4," + "                              'Used'," + "                              5," + "                              'All') STATUS " + "   from                                                                    " + "     QUOTATION_SEA_TBL              main,                                  " + "     QUOTATION_TRN_SEA_FCL_LCL      tran,                                  " + "     PORT_MST_TBL                   portpol,                               " + "     PORT_MST_TBL                   portpod,                               " + "     OPERATOR_MST_TBL               opr,                                   " + "     COMMODITY_GROUP_MST_TBL        cgrp,                                   " + "     COMMODITY_MST_TBL  CMT                                    " + "   where                                                                   " + "            main.QUOTATION_SEA_PK          = tran.QUOTATION_SEA_FK         " + "     AND    tran.PORT_MST_POL_FK           = portpol.PORT_MST_PK           " + "     AND    tran.PORT_MST_POD_FK           = portpod.PORT_MST_PK           " + "     AND    tran.OPERATOR_MST_FK           = opr.OPERATOR_MST_PK(+)        " + "     AND TRAN.Commodity_Mst_Fk=cmt.commodity_mst_pk(+)                     " + "    AND TRAN.Commodity_Group_Fk=cgrp.commodity_group_pk(+)                " + strCondition + "     Order By QUOTATION_SEA_PK, portpol.PORT_NAME, portpod.PORT_NAME ASC ";

            strSQL = strSQL.Replace("  ", " ");

            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
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

        #endregion " Fetch Child Rows "

        #region " Quotation Printing - Export Sea FCL/LCL "

        /// <summary>
        /// Fetches the quotation sea main.
        /// </summary>
        /// <param name="Qpk">The QPK.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchQuotationSeaMain(Int32 Qpk, Int16 CargoType)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow Objwk = new WorkFlow();
            strBuilder.Append(" SELECT  QS.QUOTATION_SEA_PK, ");
            strBuilder.Append(" QS.QUOTATION_REF_NO,  ");
            strBuilder.Append(" QCL.TRANS_REF_NO, ");
            strBuilder.Append(" C.CUSTOMER_NAME, ");
            strBuilder.Append(" qs.header_content, ");
            strBuilder.Append(" CDET.ADM_ADDRESS_1,");
            strBuilder.Append(" CDET.ADM_ADDRESS_2, ");
            strBuilder.Append(" CDET.ADM_ADDRESS_3, ");
            strBuilder.Append(" CDET.ADM_ZIP_CODE, ");
            strBuilder.Append(" CDET.ADM_CONTACT_PERSON CONTACT_PERSON,");
            strBuilder.Append(" to_Char(round(QCL.EXPECTED_BOXES)) AS NOOFPIECES, ");
            strBuilder.Append(" CY.CONTAINER_TYPE_MST_ID,   ");
            if (CargoType == 1)
            {
                strBuilder.Append(" (CY.CONTAINER_LENGTH_FT || 'X' || CY.CONTAINER_WIDTH_FT ");
                strBuilder.Append(" || 'X' || CY.CONTAINER_HEIGHT_FT)   AS DIMENSIONS,");
                strBuilder.Append(" (CY.CONTAINER_MAX_CAPACITY_TONE)    AS WEIGHT, ");
                strBuilder.Append(" (CY.CONTAINER_LENGTH_FT * CY.CONTAINER_WIDTH_FT * CY.CONTAINER_HEIGHT_FT) AS CUBE, ");
            }
            else
            {
                strBuilder.Append(" 'N/A' AS DIMENSIONS, ");
                strBuilder.Append("  QCL.EXPECTED_WEIGHT AS  WEIGHT, ");
                strBuilder.Append("  QCL.EXPECTED_VOLUME AS  CUBE, ");
            }
            strBuilder.Append(" CG.COMMODITY_GROUP_DESC COMMODITY_NAME,     CMD.CARGO_MOVE_CODE  AS      TERMS,");
            strBuilder.Append(" POLTBL.PORT_NAME AS COLLECTIONPOINT, ");
            strBuilder.Append(" PODTBL.PORT_NAME AS DELIVERPOINT,");
            strBuilder.Append(" 'Sea' AS MODEOFTRANSPORT,    ");
            strBuilder.Append(" QS.CARGO_TYPE,   ");
            strBuilder.Append(" (CY.CONTAINER_TYPE_MST_ID || DECODE(QS.CARGO_TYPE,1,' FCL',' LCL')) AS SERVICE, ");
            strBuilder.Append(" QS.VALID_FOR");

            strBuilder.Append(" FROM ");
            strBuilder.Append(" QUOTATION_SEA_TBL QS, ");
            strBuilder.Append(" QUOTATION_TRN_SEA_FCL_LCL QCL, ");
            strBuilder.Append(" QUOTATION_TRN_SEA_FRT_DTLS  QF,");
            strBuilder.Append(" CUSTOMER_MST_TBL C, ");
            strBuilder.Append(" CUSTOMER_CONTACT_DTLS  CDET,");
            strBuilder.Append(" CONTAINER_TYPE_MST_TBL CY, ");
            strBuilder.Append(" COMMODITY_GROUP_MST_TBL CG, ");
            strBuilder.Append(" PLACE_MST_TBL PL,");
            strBuilder.Append(" PLACE_MST_TBL PD,  ");
            strBuilder.Append(" PORT_MST_TBL              POLTBL, ");
            strBuilder.Append(" PORT_MST_TBL              PODTBL, ");
            strBuilder.Append(" CARGO_MOVE_MST_TBL CMD  ");

            strBuilder.Append(" WHERE  QS.QUOTATION_SEA_PK      =   " + Qpk + "");
            strBuilder.Append(" AND QS.CUSTOMER_MST_FK(+)       =   C.CUSTOMER_MST_PK    ");
            strBuilder.Append(" AND CDET.CUSTOMER_MST_FK(+)     =   C.CUSTOMER_MST_PK    ");
            strBuilder.Append(" AND QS.QUOTATION_SEA_PK         =   QCL.QUOTATION_SEA_FK ");
            strBuilder.Append(" AND QCL.QUOTE_TRN_SEA_PK        =   QF.QUOTE_TRN_SEA_FK  ");
            strBuilder.Append(" AND CY.CONTAINER_TYPE_MST_PK(+) =   QCL.CONTAINER_TYPE_MST_FK ");
            strBuilder.Append(" AND QCL.COMMODITY_GROUP_FK        = CG.COMMODITY_GROUP_PK(+) ");
            strBuilder.Append(" AND QS.COL_PLACE_MST_FK         =   PL.PLACE_PK(+)       ");
            strBuilder.Append(" AND QS.DEL_PLACE_MST_FK         =   PD.PLACE_PK(+)       ");
            strBuilder.Append("  AND POLTBL.PORT_MST_PK =QCL.PORT_MST_POL_FK       ");
            strBuilder.Append(" AND PODTBL.PORT_MST_PK =QCL.PORT_MST_POD_FK       ");
            strBuilder.Append(" AND QS.CARGO_MOVE_FK = CMD.CARGO_MOVE_PK(+)      ");
            try
            {
                return Objwk.GetDataSet(strBuilder.ToString());
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

        /// <summary>
        /// Fetches the transit time.
        /// </summary>
        /// <param name="Qpk">The QPK.</param>
        /// <returns></returns>
        public string FetchTransitTime(Int64 Qpk)
        {
            string strSQL = null;
            WorkFlow ObjWk = new WorkFlow();
            strSQL = "SELECT (VVT.POD_ETA - VVT.POL_ETD) Transit_Days" + "FROM QUOTATION_TRN_SEA_FCL_LCL QTSFL,VESSEL_VOYAGE_TRN VVT,QUOTATION_SEA_TBL QST " + "WHERE VVT.PORT_MST_POL_FK = QTSFL.PORT_MST_POL_FK " + "AND VVT.PORT_MST_POD_FK = QTSFL.PORT_MST_POD_FK" + "AND QST.QUOTATION_SEA_PK=QTSFL.QUOTATION_SEA_FK" + "and QST.EXPECTED_SHIPMENT_DT between VVT.POL_ETA and VVT.POL_ETD" + "AND QST.QUOTATION_SEA_PK=" + Qpk;
            try
            {
                return ObjWk.ExecuteScaler(strSQL);
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

        /// <summary>
        /// Fetches the quotation sea fright.
        /// </summary>
        /// <param name="Qpk">The QPK.</param>
        /// <returns></returns>
        public DataSet FetchQuotationSeaFright(Int32 Qpk)
        {
            string strSQL = null;
            WorkFlow ObjWk = new WorkFlow();

            strSQL = " SELECT   QS.QUOTATION_SEA_PK,            QS.QUOTATION_REF_NO,            " + "           FE.FREIGHT_ELEMENT_NAME     AS  DESCRPTION,                     " + "           qs.remarks,                    qs.footer_content,               " + "           CC.CURRENCY_ID,                 QF.QUOTED_RATE  AS  AMOUNT,     " + "           decode(QS.CARGO_TYPE, 1, CY.CONTAINER_TYPE_MST_ID, DU.DIMENTION_ID ) AS  COMMENTS, " + "           QS.SPECIAL_INSTRUCTIONS                                         " + "   FROM        QUOTATION_SEA_TBL               QS,         " + "               QUOTATION_TRN_SEA_FCL_LCL       QCL,        " + "               QUOTATION_TRN_SEA_FRT_DTLS      QF,         " + "               FREIGHT_ELEMENT_MST_TBL         FE,         " + "               CURRENCY_TYPE_MST_TBL           CC,         " + "               DIMENTION_UNIT_MST_TBL          DU,         " + "               CONTAINER_TYPE_MST_TBL          CY          " + "   WHERE       QS.QUOTATION_SEA_PK         =   " + Qpk + "       AND     QS.QUOTATION_SEA_PK         =   QCL.QUOTATION_SEA_FK        " + "       AND     QCL.QUOTE_TRN_SEA_PK        =   QF.QUOTE_TRN_SEA_FK         " + "       AND     CC.CURRENCY_MST_PK(+)       =   QF.CURRENCY_MST_FK          " + "       AND     DU.DIMENTION_UNIT_MST_PK(+) =   QCL.Basis                   " + "       AND     FE.FREIGHT_ELEMENT_MST_PK   =   QF.FREIGHT_ELEMENT_MST_FK   " + "       AND     CY.CONTAINER_TYPE_MST_PK(+) =   QCL.CONTAINER_TYPE_MST_FK   ";

            strSQL += " union " + " SELECT   QS.QUOTATION_SEA_PK,            QS.QUOTATION_REF_NO,            " + "           FE.FREIGHT_ELEMENT_NAME     AS  DESCRPTION,                     " + "         qs.remarks,                qs.footer_content,                    " + "           CC.CURRENCY_ID,                 QO.AMOUNT   AS  AMOUNT,     " + "           decode(QS.CARGO_TYPE, 1, 'B/L','B/L' ) AS  COMMENTS, " + "           QS.SPECIAL_INSTRUCTIONS                                         " + "   FROM        QUOTATION_SEA_TBL               QS,         " + "               QUOTATION_TRN_SEA_FCL_LCL       QCL,        " + "               QUOTATION_TRN_SEA_OTH_CHRG      QO,         " + "               FREIGHT_ELEMENT_MST_TBL         FE,         " + "               CURRENCY_TYPE_MST_TBL           CC,         " + "               DIMENTION_UNIT_MST_TBL          DU,         " + "               CONTAINER_TYPE_MST_TBL          CY          " + "   WHERE       QS.QUOTATION_SEA_PK         =   " + Qpk + "       AND     QS.QUOTATION_SEA_PK         =   QCL.QUOTATION_SEA_FK        " + "       AND     QCL.QUOTE_TRN_SEA_PK        =   QO.QUOTATION_TRN_SEA_FK     " + "       AND     CC.CURRENCY_MST_PK(+)       =   QO.CURRENCY_MST_FK          " + "       AND     DU.DIMENTION_UNIT_MST_PK(+) =   QCL.Basis                   " + "       AND     FE.FREIGHT_ELEMENT_MST_PK   =   QO.FREIGHT_ELEMENT_MST_FK   " + "       AND     CY.CONTAINER_TYPE_MST_PK(+) =   QCL.CONTAINER_TYPE_MST_FK   ";
            //end
            strSQL = strSQL.Replace("   ", " ");
            strSQL = strSQL.Replace("  ", " ");
            try
            {
                return ObjWk.GetDataSet(strSQL);
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

        #endregion " Quotation Printing - Export Sea FCL/LCL "
    }
}