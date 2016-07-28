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
    public class cls_Opr_Contract_List : CommonFeatures
    {
        private Int32 m_last;
        private Int32 m_start;

        private DataSet DSMain = new DataSet();

        #region "FetchAll"

        public DataSet FetchAll(string OperatorID = "", string OperatorName = "", string CONTNo = "", string CONTDate = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string ValidFrom = "", string ValidTo = "",
        bool ActiveOnly = true, bool ApprovedOnly = true, short CargoType = 1, string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", string CurrBizType = "3", long lngUsrLocFk = 0,
        Int32 flag = 0, string strColumnName = "", Int16 Int_Wf_Status = 3, string Commodityfk = "0")
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
            if (OperatorID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(OPERATOR_ID) LIKE '" + SrOP + OperatorID.ToUpper().Replace("'", "''") + "%'");
            }
            if (OperatorName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(OPERATOR_NAME) LIKE '" + SrOP + OperatorName.ToUpper().Replace("'", "''") + "%'");
            }
            if (CONTNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CONTRACT_NO) LIKE '" + SrOP + CONTNo.ToUpper().Replace("'", "''") + "%'");
            }
            if (CONTDate.Length > 0)
            {
                buildCondition.Append(" AND CONTRACT_DATE = TO_DATE('" + CONTDate + "' , '" + dateFormat + "') ");
            }

            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE CONT_MAIN_SEA_FK = main.CONT_MAIN_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }

            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM CONT_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE CONT_MAIN_SEA_FK = main.CONT_MAIN_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE CONT_MAIN_SEA_FK = main.CONT_MAIN_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM CONT_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE CONT_MAIN_SEA_FK = main.CONT_MAIN_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }

            //If ValidTo.Length > 0 And ValidFrom.Length > 0 Then
            //    buildCondition.Append("AND ((TO_DATE('" & ValidTo & "' , '" & dateFormat & "') BETWEEN")
            //    buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR")
            //    buildCondition.Append("    (TO_DATE('" & ValidFrom & "' , '" & dateFormat & "') BETWEEN")
            //    buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR")
            //    buildCondition.Append("    (MAIN.VALID_TO IS NULL))")
            //ElseIf ValidTo.Length > 0 And Not ValidFrom.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND ( ")
            //    buildCondition.Append(vbCrLf & "        main.VALID_TO <= TO_DATE('" & ValidTo & "' , '" & dateFormat & "') ")
            //    buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
            //    buildCondition.Append(vbCrLf & "     ) ")
            //ElseIf ValidFrom.Length > 0 And Not ValidTo.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND ( ")
            //    buildCondition.Append(vbCrLf & "        main.VALID_FROM >= TO_DATE('" & ValidFrom & "' , '" & dateFormat & "') ")
            //    buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
            //    buildCondition.Append(vbCrLf & "     ) ")
            //End If
            //If ActiveOnly = True Then
            //    buildCondition.Append(vbCrLf & " AND main.ACTIVE = 1 ")
            //    buildCondition.Append(vbCrLf & " AND ( ")
            //    buildCondition.Append(vbCrLf & "        main.VALID_TO >= TO_DATE(SYSDATE , '" & dateFormat & "') ")
            //    buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
            //    buildCondition.Append(vbCrLf & "     ) ")
            //End If

            if (ActiveOnly == true)
            {
                buildCondition.Append(" AND MAIN.ACTIVE = 1 ");
                buildCondition.Append(" AND ( ");
                buildCondition.Append("        MAIN.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ");
                buildCondition.Append("       OR MAIN.VALID_TO IS NULL ");
                buildCondition.Append("     ) ");
            }

            //'Goutam : When From and To date are selected, any contract valid b/w selected period should list in the listing screen.
            if (!string.IsNullOrEmpty(ValidFrom) & string.IsNullOrEmpty(ValidTo))
            {
                buildCondition.Append(" AND TO_DATE('" + ValidFrom + "', '" + dateFormat + "') BETWEEN TO_DATE(MAIN.VALID_FROM, '" + dateFormat + "') AND TO_DATE(NVL(MAIN.VALID_TO, SYSDATE), '" + dateFormat + "') ");
            }

            if (!string.IsNullOrEmpty(ValidTo) & string.IsNullOrEmpty(ValidFrom))
            {
                buildCondition.Append(" AND TO_DATE('" + ValidTo + "', '" + dateFormat + "') BETWEEN TO_DATE(MAIN.VALID_FROM, '" + dateFormat + "') AND TO_DATE(NVL(MAIN.VALID_TO, SYSDATE), '" + dateFormat + "') ");
            }

            if (!string.IsNullOrEmpty(ValidFrom) & !string.IsNullOrEmpty(ValidTo))
            {
                buildCondition.Append(" AND (TO_DATE(MAIN.VALID_FROM,'" + dateFormat + "') BETWEEN TO_DATE('" + ValidFrom.Trim() + "','" + dateFormat + "') AND TO_DATE('" + ValidTo.Trim() + "','" + dateFormat + "')  ");
                buildCondition.Append(" OR TO_DATE(NVL(MAIN.VALID_TO, SYSDATE) ,'" + dateFormat + "') BETWEEN TO_DATE('" + ValidFrom.Trim() + "','" + dateFormat + "') AND TO_DATE('" + ValidTo.Trim() + "','" + dateFormat + "'))  ");
            }
            ///End Goutam

            if (Convert.ToInt32(Commodityfk) > 0)
            {
                buildCondition.Append(" AND MAIN.Commodity_Group_Fk=" + Commodityfk + " ");
            }
            if (ApprovedOnly == true)
            {
                buildCondition.Append(" AND main.CONT_APPROVED = 1 ");
            }
            if (CargoType == 1)
            {
                buildCondition.Append(" AND main.CARGO_TYPE = 1 ");
            }
            else
            {
                buildCondition.Append(" AND main.CARGO_TYPE = 2 ");
            }
            buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + "");
            buildCondition.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK ");
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2");
            }
            strCondition = buildCondition.ToString();
            buildQuery.Append(" Select count(*) ");
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_MAIN_SEA_TBL main, ");
            buildQuery.Append("       OPERATOR_MST_TBL,USER_MST_TBL UMT,COMMODITY_GROUP_MST_TBL CGMT ");
            buildQuery.Append("      where ");
            buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK ");
            buildQuery.Append("       AND MAIN.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK ");
            buildQuery.Append("      " + strCondition);
            strSQL = buildQuery.ToString();
            buildQuery.Remove(0, buildQuery.Length);
            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select ");
            buildQuery.Append(" NVL(main.ACTIVE, 0) ACTIVE,");
            buildQuery.Append(" NVL(main.CONT_APPROVED, 0) APPROVED, ");
            buildQuery.Append(" CONTRACT_NO, ");
            buildQuery.Append(" CONTRACT_DATE CONTRACT_DATE, ");
            buildQuery.Append(" OPERATOR_NAME, ");
            buildQuery.Append(" CONT_MAIN_SEA_PK, ");
            buildQuery.Append(" DECODE(main.CARGO_TYPE,1,'FCL','LCL') As CARGO_TYPE, ");
            buildQuery.Append(" CGMT.COMMODITY_GROUP_CODE, ");
            //'Goutam : Date Sorting should be based on the date in the grid.
            //buildQuery.Append(vbCrLf & " to_Char(main.VALID_FROM, '" & dateFormat & "') VALID_FROM, ")
            //buildQuery.Append(vbCrLf & " to_Char(main.VALID_TO, '" & dateFormat & "') VALID_TO, ")
            buildQuery.Append(" main.VALID_FROM, ");
            buildQuery.Append(" main.VALID_TO, ");
            //'
            buildQuery.Append(" 0 POL, ");
            buildQuery.Append(" 0 POD,decode(main.CONT_APPROVED,0,'Requested',1,'Approved',2,'Rejected') STATUS,main.restricted ");
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_MAIN_SEA_TBL main, ");
            buildQuery.Append("       OPERATOR_MST_TBL,USER_MST_TBL UMT,COMMODITY_GROUP_MST_TBL CGMT ");
            buildQuery.Append("      where ");
            buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK ");
            buildQuery.Append("       AND MAIN.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+) ");
            if (Int_Wf_Status == 0 | Int_Wf_Status == 1 | Int_Wf_Status == 2)
            {
                buildQuery.Append(" and   main.workflow_status =  " + Int_Wf_Status + "");
            }
            buildQuery.Append("        " + strCondition);
            buildQuery.Append("     Order By " + SortColumn + SortType);
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");

            strSQL = buildQuery.ToString();
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("SELECT COUNT(*) FROM (" + strSQL + ")"));
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
            buildQuery.Append("  where  ");
            buildQuery.Append(" SR_NO between " + start + " and " + last);
            strSQL = buildQuery.ToString();
            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                DataRelation trfRel = new DataRelation("TariffRelation", DS.Tables[0].Columns["CONT_MAIN_SEA_PK"], DS.Tables[1].Columns["CONT_MAIN_SEA_FK"], true);
                DS.Relations.Add(trfRel);
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

        #endregion "FetchAll"

        #region "FetchAll"

        private DataTable FetchChildFor(string ContractPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            if (ContractPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and CONT_MAIN_SEA_FK in (" + ContractPKs + ") ");
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
            buildQuery.Append("    ( Select DISTINCT ");
            buildQuery.Append("       CONT_MAIN_SEA_FK, ");
            buildQuery.Append("       PORT_MST_POL_FK, ");
            buildQuery.Append("       PORTPOL.PORT_NAME PORT_POL_NAME, ");
            buildQuery.Append("       PORTPOL.PORT_ID PORT_POL_ID, ");
            buildQuery.Append("       PORTPOD.PORT_NAME PORT_POD_NAME, ");
            buildQuery.Append("       PORT_MST_POD_FK, ");
            buildQuery.Append("       PORTPOD.PORT_ID PORT_POD_ID, ");
            buildQuery.Append("       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO ");
            buildQuery.Append("      from ");
            buildQuery.Append("       CONT_TRN_SEA_FCL_LCL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOD ");
            buildQuery.Append("      where ");
            buildQuery.Append("       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK AND ");
            buildQuery.Append("       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("      Order By CONT_MAIN_SEA_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
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
                    if (Convert.ToInt32(dt.Rows[RowCnt]["CONT_MAIN_SEA_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["CONT_MAIN_SEA_FK"]);
                        Rno = 0;
                    }
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

        #endregion "FetchAll"

        #region "AllMasterPKs"

        private string AllMasterPKs(DataSet ds)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                strBuilder.Append("-1,");
                for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
                {
                    strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["CONT_MAIN_SEA_PK"]).Trim() + ",");
                }
                strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
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

        #endregion "AllMasterPKs"

        #region "Fetch All Container Pk's"

        private string AllCONTMainPKs(DataSet ds)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                string str = "";
                for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
                {
                    str += Convert.ToString(ds.Tables[0].Rows[RowCnt]["CONT_MAIN_SEA_PK"]).Trim() + ",";
                }
                if (str.Length > 0)
                    str = str.Substring(0, str.Length - 1);
                return str;
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

        #endregion "Fetch All Container Pk's"

        #region "Make Child Data Table"

        public DataTable MakeChildDataTable(string strSQl)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataTable dtChild = null;
                dtChild = objWF.GetDataTable(strSQl);
                return dtChild;
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

        #endregion "Make Child Data Table"

        #region "Select Operator When Passing Operator ID"

        public int SelectOperatorFk(string OperatorId)
        {
            try
            {
                string strSql = null;
                int operatorFk = 0;
                WorkFlow objWF = new WorkFlow();
                strSql = "SELECT OPERATOR_MST_PK FROM OPERATOR_MST_TBL WHERE OPERATOR_ID = '" + OperatorId + "' ";
                operatorFk = Convert.ToInt32(objWF.ExecuteScaler(strSql));
                return operatorFk;
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

        #endregion "Select Operator When Passing Operator ID"

        #region "Select Port FK"

        public int SelectPortFk(string Portid)
        {
            try
            {
                string strSql = null;
                int PortFk = 0;
                WorkFlow objWF = new WorkFlow();
                strSql = "SELECT PORT_MST_PK FROM PORT_MST_TBL WHERE PORT_ID = '" + Portid + "' ";
                PortFk = Convert.ToInt32(objWF.ExecuteScaler(strSql));
                return PortFk;
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

        #endregion "Select Port FK"

        #region "SelectTradeFK"

        public int SelectTradeFk(string Tradeid)
        {
            try
            {
                string strSql = null;
                int TradeFk = 0;
                WorkFlow objWF = new WorkFlow();
                strSql = "SELECT TRADE_MST_PK FROM TRADE_MST_TBL WHERE TRADE_ID = '" + Tradeid + "' ";
                TradeFk = Convert.ToInt32(objWF.ExecuteScaler(strSql));
                return TradeFk;
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

        #endregion "SelectTradeFK"
    }
}