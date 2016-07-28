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
    public class Cls_OperatorTarif_Listing : CommonFeatures
    {

        #region " Fetch All "
        public DataSet FetchAll(string OperatorID = "", string OperatorName = "", string AgentId = "", string AgentName = "", string TariffRefNo = "", string TariffDate = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "",
        string CargoType = "", string FromDate = "", string ToDate = "", bool ActiveOnly = true, string ddlStatus = "3", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ",
        string CurrBizType = "3", short TariffType = 1, long lngUsrLocFk = 0, Int32 flag = 0)
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

            //If Operator tariff
            //If TariffType = 1 Then
            if (OperatorID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(OPERATOR_ID) LIKE '" + SrOP + OperatorID.ToUpper().Replace("'", "''") + "%'");
            }
            if (OperatorName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(OPERATOR_NAME) LIKE '" + SrOP + OperatorName.ToUpper().Replace("'", "''") + "%'");
            }
            //End If

            // If Agent Tariff
            //If TariffType = 3 Or TariffType = 4 Then
            if (AgentId.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AGENT_ID) LIKE '" + SrOP + AgentId.ToUpper().Replace("'", "''") + "%'");
            }
            if (AgentName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AGENT_NAME) LIKE '" + SrOP + AgentName.ToUpper().Replace("'", "''") + "%'");
            }
            //End If

            if (TariffRefNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(TARIFF_REF_NO) LIKE '" + SrOP + TariffRefNo.ToUpper().Replace("'", "''") + "%'");
            }
            if (TariffDate.Length > 0)
            {
                buildCondition.Append(" AND TARIFF_DATE = TO_DATE('" + TariffDate + "' , '" + dateFormat + "') ");
            }

            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM TARIFF_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE TARIFF_MAIN_SEA_FK = main.TARIFF_MAIN_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }

            if (POLName.Length > 0)
            {
                buildCondition.Append("  AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM TARIFF_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE TARIFF_MAIN_SEA_FK = main.TARIFF_MAIN_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM TARIFF_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE TARIFF_MAIN_SEA_FK = main.TARIFF_MAIN_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM TARIFF_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE TARIFF_MAIN_SEA_FK = main.TARIFF_MAIN_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (CargoType.Length > 0)
            {
                buildCondition.Append(" AND CARGO_TYPE = " + CargoType);
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
            if (ddlStatus != "3")
            {
                buildCondition.Append(" AND MAIN.STATUS = " + ddlStatus);
            }

            if (ActiveOnly == true)
            {
                buildCondition.Append(" AND main.ACTIVE = 1 ");
                buildCondition.Append(" AND ( ");
                buildCondition.Append("        main.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ");
                buildCondition.Append("       OR main.VALID_TO IS NULL ");
                buildCondition.Append("     ) ");
            }
            buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + "");
            buildCondition.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK ");

            if (TariffType > 0)
            {
                buildCondition.Append(" AND main.TARIFF_TYPE = " + TariffType);
            }

            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            strCondition = buildCondition.ToString();
            buildQuery.Append(" Select count(*) ");
            buildQuery.Append("      from ");
            buildQuery.Append("       TARIFF_MAIN_SEA_TBL main,USER_MST_TBL UMT  ");
            //If TariffType = 1 Then
            buildQuery.Append("     ,OPERATOR_MST_TBL ");
            //End If
            //If TariffType = 3 Or TariffType = 4 Then
            buildQuery.Append("       ,AGENT_MST_TBL ");
            //End If
            buildQuery.Append("      where 1=1 ");
            //If TariffType = 1 Then
            buildQuery.Append("  AND main.OPERATOR_MST_FK = OPERATOR_MST_PK(+) ");
            //End If
            //If TariffType = 3 Or TariffType = 4 Then
            buildQuery.Append("  AND  main.AGENT_MST_FK = AGENT_MST_PK(+) ");
            //End If
            if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                buildQuery.Append("    and MAIN.Tariff_Date between to_date('" + FromDate + "','" + dateFormat + "') and to_date('" + ToDate + "','" + dateFormat + "')");
            }

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
            buildQuery.Append("       TARIFF_MAIN_SEA_PK, ");
            buildQuery.Append("       NVL(main.ACTIVE, 0) ACTIVE, ");
            buildQuery.Append("       TARIFF_REF_NO, ");

            buildQuery.Append("  main.OPERATOR_MST_FK, ");
            buildQuery.Append("  OP.OPERATOR_ID, ");
            buildQuery.Append("  OP.OPERATOR_NAME, ");
            buildQuery.Append("  main.AGENT_MST_FK, ");
            buildQuery.Append("  AGENT.AGENT_ID, ");
            buildQuery.Append("  AGENT.AGENT_NAME, ");

            buildQuery.Append("       decode(TARIFF_TYPE,'1','SL Tariff','2','Gen Tariff','3','Agent Tariff','4','C.P. Tariff') as TARIFF_TYPE, ");
            buildQuery.Append("       MAIN.TARIFF_TYPE TARIFF_TYPE_FLAG, ");
            buildQuery.Append("       decode(CARGO_TYPE, 1, 'FCL',2, 'LCL') CARGO_TYPE, ");
            buildQuery.AppendLine("       CG.COMMODITY_GROUP_PK, ");
            buildQuery.AppendLine("       CG.COMMODITY_GROUP_CODE, ");
            buildQuery.Append("       0 POL, ");
            buildQuery.Append("       0 POD, ");
            buildQuery.Append("       to_date(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_date(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            buildQuery.Append("       DECODE(main.STATUS, 0,'Requested',1, 'Approved',3,'Rejected') STATUS ");
            buildQuery.Append("      from ");
            buildQuery.Append("       TARIFF_MAIN_SEA_TBL main,USER_MST_TBL UMT ");

            buildQuery.Append("       ,OPERATOR_MST_TBL OP");
            buildQuery.Append("       ,AGENT_MST_TBL AGENT");
            buildQuery.Append("       ,COMMODITY_GROUP_MST_TBL CG ");

            buildQuery.Append("      where 1=1");

            buildQuery.Append("   AND  main.OPERATOR_MST_FK = OP.OPERATOR_MST_PK(+) ");
            buildQuery.Append("   AND   main.AGENT_MST_FK = AGENT.AGENT_MST_PK(+) ");
            buildQuery.AppendLine("     AND MAIN.COMMODITY_GROUP_FK=CG.COMMODITY_GROUP_PK(+) ");
            if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                buildQuery.Append("    and MAIN.Tariff_Date between to_date('" + FromDate + "','" + dateFormat + "') and to_date('" + ToDate + "','" + dateFormat + "')");
            }
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("     Order By " + SortColumn + SortType);
            buildQuery.Append("  ,TARIFF_REF_NO DESC ");
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
                DataRelation trfRel = new DataRelation("TariffRelation", DS.Tables[0].Columns["TARIFF_MAIN_SEA_PK"], DS.Tables[1].Columns["TARIFF_MAIN_SEA_FK"], true);
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

        private string AllMasterPKs(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append("-1,");
            for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
            {
                strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["TARIFF_MAIN_SEA_PK"]).Trim() + ",");
            }
            strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion

        #region " Fetch Childs "

        private DataTable FetchChildFor(string TariffPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {

            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            // band1_SRNO() = 0 :  band1_TarifMainFK = 1 : band1_PortPOLFK = 2 :  band1_PortPOLID = 3
            // band1_PortPOLName = 4 : band1_PortPODFK = 5 :  band1_PortPODID = 6 :  band1_PortPODName = 7
            // band1_ValidFrom = 8 :  band1_ValidTo = 9

            if (TariffPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and TARIFF_MAIN_SEA_FK in (" + TariffPKs + ") ");
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
            buildQuery.Append("       TARIFF_MAIN_SEA_FK, ");
            buildQuery.Append("       PORT_MST_POL_FK, ");
            buildQuery.Append("       PORTPOL.PORT_ID PORT_POL_ID, ");
            buildQuery.Append("       PORTPOL.PORT_NAME PORT_POL_NAME, ");
            buildQuery.Append("       PORT_MST_POD_FK, ");
            buildQuery.Append("       PORTPOD.PORT_ID PORT_POD_ID, ");
            buildQuery.Append("       PORTPOD.PORT_NAME PORT_POD_NAME, ");
            buildQuery.Append("       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO ");
            buildQuery.Append("      from ");
            buildQuery.Append("       TARIFF_TRN_SEA_FCL_LCL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOD ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK AND ");
            buildQuery.Append("       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("      Order By TARIFF_MAIN_SEA_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
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
                    if (Convert.ToInt32(dt.Rows[RowCnt]["TARIFF_MAIN_SEA_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["TARIFF_MAIN_SEA_FK"]);
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

        #endregion

        #region " Enhance Search Function for Tariff for Operator "

        public string FetchTariffForOperator(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strSEARCH_OP_ID_IN = "";
            string strLOC_MST_IN = "";
            string strTAR_TYPE_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strSEARCH_OP_ID_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strTAR_TYPE_IN = Convert.ToString(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_TARIFF_REF_NO_PKG.GET_OPERATOR_TARIFF_COMMON";
                var _with1 = SCM.Parameters;
                // .Add("SEARCH_OP_ID_IN", ifDBNull(strSEARCH_OP_ID_IN)).Direction = ParameterDirection.Input
                _with1.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("TARIFF_TYPE_IN", strTAR_TYPE_IN).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion

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

    }
}