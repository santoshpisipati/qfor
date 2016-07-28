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
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class Cls_AirlineTarif_Listing : CommonFeatures
    {
        // TARIFF_TRN_AIR_FCL_LCL TO TARIFF_TRN_AIR_TBL 
        #region " Fetch All "

        public DataSet FetchAll(Int16 intTariffType = 0, string AirlineID = "", string AirlineName = "", string TariffRefNo = "", string TariffDate = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string FromDate = "",
        string ToDate = "", bool ActiveOnly = true, string ddlStatus = "3", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string CurrBizType = "3", long lngUsrLocFk = 0,
        Int32 flag = 0, int AgentMstFk = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            StringBuilder buildQuery = new StringBuilder();
            StringBuilder buildCondition = new StringBuilder();
            string SrOP = (SearchType == "C" ? "%" : "");

            if (intTariffType > 0)
            {
                buildCondition.Append(" AND TARIFF_TYPE = " + intTariffType);
            }

            if (AirlineID.Length > 0)
            {
                buildCondition.Append( " AND UPPER(AIRLINE_ID) LIKE '" + SrOP + AirlineID.ToUpper().Replace("'", "''") + "%'");
            }
            if (AirlineName.Length > 0)
            {
                buildCondition.Append( " AND UPPER(AIRLINE_NAME) LIKE '" + SrOP + AirlineName.ToUpper().Replace("'", "''") + "%'");
            }
            if (TariffRefNo.Length > 0)
            {
                buildCondition.Append( " AND UPPER(TARIFF_REF_NO) LIKE '" + SrOP + TariffRefNo.ToUpper().Replace("'", "''") + "%'");
            }
            if (TariffDate.Length > 0)
            {
                buildCondition.Append( " AND TARIFF_DATE = TO_DATE('" + TariffDate + "' , '" + dateFormat + "') ");
            }
            if (AgentMstFk > 0)
            {
                buildCondition.Append( " AND MAIN.AGENT_MST_FK = " + AgentMstFk);
            }

            if (POLID.Length > 0)
            {
                buildCondition.Append( " AND EXISTS ");
                buildCondition.Append( "     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append( "       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append( "       and PORT_MST_PK IN ");
                buildCondition.Append( "         ( SELECT PORT_MST_POL_FK FROM TARIFF_TRN_AIR_TBL ");
                buildCondition.Append( "           WHERE TARIFF_MAIN_AIR_FK = main.TARIFF_MAIN_AIR_PK ");
                buildCondition.Append( "         ) ");
                buildCondition.Append( "     ) ");
            }

            if (POLName.Length > 0)
            {
                buildCondition.Append( " AND EXISTS ");
                buildCondition.Append( "     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append( "       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append( "       and PORT_MST_PK IN ");
                buildCondition.Append( "         ( SELECT PORT_MST_POL_FK FROM TARIFF_TRN_AIR_TBL ");
                buildCondition.Append( "           WHERE TARIFF_MAIN_AIR_FK = main.TARIFF_MAIN_AIR_PK ");
                buildCondition.Append( "         ) ");
                buildCondition.Append( "     ) ");
            }
            if (PODID.Length > 0)
            {
                buildCondition.Append( " AND EXISTS ");
                buildCondition.Append( "     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append( "       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append( "       and PORT_MST_PK IN ");
                buildCondition.Append( "         ( SELECT PORT_MST_POD_FK FROM TARIFF_TRN_AIR_TBL ");
                buildCondition.Append( "           WHERE TARIFF_MAIN_AIR_FK = main.TARIFF_MAIN_AIR_PK ");
                buildCondition.Append( "         ) ");
                buildCondition.Append( "     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append( " AND EXISTS ");
                buildCondition.Append( "     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append( "       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append( "       and PORT_MST_PK IN ");
                buildCondition.Append( "         ( SELECT PORT_MST_POD_FK FROM TARIFF_TRN_AIR_TBL ");
                buildCondition.Append( "           WHERE TARIFF_MAIN_AIR_FK = main.TARIFF_MAIN_AIR_PK ");
                buildCondition.Append( "         ) ");
                buildCondition.Append( "     ) ");
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

            if (ActiveOnly == true)
            {
                buildCondition.Append( " AND main.ACTIVE = 1 ");
                buildCondition.Append( " AND ( ");
                buildCondition.Append( "        main.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ");
                buildCondition.Append( "       OR main.VALID_TO IS NULL ");
                buildCondition.Append( "     ) ");
            }
            if (ddlStatus != "3")
            {
                buildCondition.Append( " AND MAIN.STATUS = " + ddlStatus);
            }
            //buildCondition.Append(vbCrLf & " AND UMT.DEFAULT_LOCATION_FK = " & lngUsrLocFk & "")
            buildCondition.Append( "   AND main.CREATED_BY_FK = UMT.USER_MST_PK ");
            if (flag == 0)
            {
                buildCondition.Append( " AND 1=2 ");
            }
            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select count(*) ");
            buildQuery.Append( "      from ");
            buildQuery.Append( "       TARIFF_MAIN_AIR_TBL main, ");
            buildQuery.Append( "       AIRLINE_MST_TBL,USER_MST_TBL UMT,");
            buildQuery.AppendLine("            AGENT_MST_TBL AG ");
            buildQuery.Append( "      WHERE MAIN.AIRLINE_MST_FK = AIRLINE_MST_PK(+) ");
            buildQuery.Append( "       AND MAIN.AGENT_MST_FK = AG.AGENT_MST_PK(+) ");
            buildQuery.Append( "      " + strCondition);

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
            buildQuery.Append( "  ( Select ROWNUM SR_NO, Q.* from ");
            buildQuery.Append( "    ( Select DISTINCT ");
            buildQuery.Append( "       MAIN.TARIFF_MAIN_AIR_PK, ");
            buildQuery.Append( "       NVL(MAIN.ACTIVE, 0) ACTIVE, ");
            buildQuery.Append( "       MAIN.TARIFF_REF_NO, ");

            //buildQuery.Append(vbCrLf & "       CASE WHEN MAIN.TARIFF_TYPE IN (3,4) THEN AG.AGENT_MST_PK ELSE MAIN.AIRLINE_MST_FK END AIRLINE_MST_FK, ")
            //buildQuery.Append(vbCrLf & "       CASE WHEN MAIN.TARIFF_TYPE IN (3,4) THEN AG.AGENT_ID ELSE AIRLINE_ID END AIRLINE_ID, ")
            //buildQuery.Append(vbCrLf & "       CASE WHEN MAIN.TARIFF_TYPE IN (3,4) THEN AG.AGENT_NAME ELSE AIRLINE_NAME END AIRLINE_NAME, ")
            buildQuery.Append( "  main.AIRLINE_MST_FK, ");
            buildQuery.Append( "  AIRLINE_ID, ");
            buildQuery.Append( "  AIRLINE_NAME, ");
            buildQuery.Append( "  main.AGENT_MST_FK, ");
            buildQuery.Append( "  AG.AGENT_ID, ");
            buildQuery.Append( "  AG.AGENT_NAME, ");

            buildQuery.Append( "       DECODE(MAIN.TARIFF_TYPE,'1','Airline Tariff','2','General Tariff','3','Agent Tariff','4','Channel Partner Tariff') TARIFF_TYPE, ");
            buildQuery.Append( "       MAIN.TARIFF_TYPE TARIFF_TYPE_FLAG, ");

            buildQuery.Append( "       MAIN.COMMODITY_GROUP_FK, ");
            buildQuery.Append( "       CG.COMMODITY_GROUP_CODE, ");
            buildQuery.Append( "       0 POL, ");
            buildQuery.Append( "       0 POD, ");
            buildQuery.Append( "       to_date(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append( "       to_date(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            buildQuery.Append( "       MAIN.TARIFF_DATE, ");
            buildQuery.Append( "       DECODE(MAIN.STATUS, 0,'Requested',1, 'Approved',3,'Rejected') STATUS ");
            buildQuery.Append( "      from ");
            buildQuery.Append( "       TARIFF_MAIN_AIR_TBL MAIN, ");
            buildQuery.Append( "       AIRLINE_MST_TBL,USER_MST_TBL UMT,");
            //buildQuery.AppendLine("            LOCATION_WORKING_PORTS_TRN LOC, ")
            buildQuery.AppendLine("            AGENT_MST_TBL AG, ");
            buildQuery.AppendLine("            COMMODITY_GROUP_MST_TBL CG ");
            buildQuery.Append( "      WHERE MAIN.AIRLINE_MST_FK = AIRLINE_MST_PK(+) ");
            buildQuery.Append( "       AND MAIN.AGENT_MST_FK = AG.AGENT_MST_PK(+) ");
            buildQuery.Append( "       AND MAIN.COMMODITY_GROUP_FK=CG.COMMODITY_GROUP_PK(+) ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append( "     Order By " + SortColumn + SortType);
            buildQuery.Append( "     ,TARIFF_REF_NO DESC");
            buildQuery.Append( "    ) Q ");
            buildQuery.Append( "  )   ");
            buildQuery.Append( "  where  ");
            buildQuery.Append( "     SR_NO between " + start + " and " + last);

            strSQL = buildQuery.ToString();

            // band0_SRNO() :  band0_TarifMainPK() : Active :  band0_TarifRefNo() :  band0_AirlineFK() :  band0_AirlineID()
            // band0_AirlineName() :  band0_CargoType() :  band0_POLCount() :  band0_PODCount() :  band0_ValidFrom()
            // band0_ValidTo()
            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                DataRelation trfRel = new DataRelation("TariffRelation", DS.Tables[0].Columns["TARIFF_MAIN_AIR_PK"], DS.Tables[1].Columns["TARIFF_MAIN_AIR_FK"], true);
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
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append("-1,");
            for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
            {
                strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["TARIFF_MAIN_AIR_PK"]).Trim() + ",");
            }
            strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion

        #region " Fetch Childs "

        private DataTable FetchChildFor(string TariffPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {

            StringBuilder buildQuery = new StringBuilder();
            StringBuilder buildCondition = new StringBuilder();
            string strCondition = "";
            string strSQL = "";
            // band1_SRNO() = 0 :  band1_TarifMainFK = 1 : band1_PortPOLFK = 2 :  band1_PortPOLID = 3
            // band1_PortPOLName = 4 : band1_PortPODFK = 5 :  band1_PortPODID = 6 :  band1_PortPODName = 7
            // band1_ValidFrom = 8 :  band1_ValidTo = 9

            if (TariffPKs.Trim().Length > 0)
            {
                buildCondition.Append( " and TARIFF_MAIN_AIR_FK in (" + TariffPKs + ") ");
            }
            if (POLID.Trim().Length > 0)
            {
                buildCondition.Append( " and UPPER(PORTPOL.PORT_ID) LIKE '" + SrOp + POLID.ToUpper().Replace("'", "''") + "%'");
            }
            if (POLName.Trim().Length > 0)
            {
                buildCondition.Append( " and UPPER(PORTPOL.PORT_NAME) LIKE '" + SrOp + POLName.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODID.Trim().Length > 0)
            {
                buildCondition.Append( " and UPPER(PORTPOD.PORT_ID) LIKE '" + SrOp + PODID.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODName.Trim().Length > 0)
            {
                buildCondition.Append( " and UPPER(PORTPOD.PORT_NAME) LIKE '" + SrOp + PODName.ToUpper().Replace("'", "''") + "%'");
            }

            strCondition = buildCondition.ToString();

            buildQuery.Append( " Select * from ");
            buildQuery.Append( "  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append( "    ( Select DISTINCT ");
            buildQuery.Append( "       TARIFF_MAIN_AIR_FK, ");
            buildQuery.Append( "       PORT_MST_POL_FK, ");
            buildQuery.Append( "       PORTPOL.PORT_ID PORT_POL_ID, ");
            buildQuery.Append( "       PORTPOL.PORT_NAME PORT_POL_NAME, ");
            buildQuery.Append( "       PORT_MST_POD_FK, ");
            buildQuery.Append( "       PORTPOD.PORT_ID PORT_POD_ID, ");
            buildQuery.Append( "       PORTPOD.PORT_NAME PORT_POD_NAME, ");
            buildQuery.Append( "       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append( "       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO ");
            buildQuery.Append( "      from ");
            buildQuery.Append( "       TARIFF_TRN_AIR_TBL, ");
            buildQuery.Append( "       PORT_MST_TBL PORTPOL, ");
            buildQuery.Append( "       PORT_MST_TBL PORTPOD ");
            buildQuery.Append( "      where ");
            // JOIN CONDITION
            buildQuery.Append( "       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK AND ");
            buildQuery.Append( "       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append( "      Order By TARIFF_MAIN_AIR_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
            buildQuery.Append( "    ) q ");
            buildQuery.Append( "  )   ");

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
                    if (Convert.ToInt32(dt.Rows[RowCnt]["TARIFF_MAIN_AIR_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["TARIFF_MAIN_AIR_FK"]);
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

        #region " Enhance Search Function for Tariff for AIRLINE "

        public string FetchTariffForAirline(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strSEARCH_AR_ID_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));;
            if (arr.Length > 2)
                strSEARCH_AR_ID_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_TARIFF_REF_NO_PKG.GET_AIRLINE_TARIFF_COMMON";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_AR_ID_IN", ifDBNull(strSEARCH_AR_ID_IN)).Direction = ParameterDirection.Input;
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

        //Added by Nippy  This method has to be moved to cls_AirlineRFQEntry.vb, 
        #region "Fetch Called by Select Container/Sector"
        //This function returns all the active sectors from the database.
        //If the given POL and POD are present then the value will come as checked.
        public DataTable ActiveSector(long LocationPk, string strPOLPk = "", string strPODPk = "", long Biztype = 1, long Process = 1, Int32 CurrentPage = 0, Int32 TotalPage = 0, long lngSelectedPolPk = 0, string strPolPodCondition = "", string From = "",
        int strGroup = 0, string strTariffGrpPk = "", string FromFlag = "", string SrchCtra = "")
        {

            //'Biztype = 1-AIR, 2-SEA, 3-BOTH
            //'Process = 1-EXPORT, 2-IMPORT
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPolPk1 = null;
            Array arr = null;
            Array arrPodPk = null;
            Array arrPodPk1 = null;
            string strCondition = null;
            string strPolCondition = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);

            if (!string.IsNullOrEmpty(SrchCtra))
            {
                arr = SrchCtra.Split('~');
            }

            //Code related to implementing paging control added by Nippy on 28/03/2006
            //Spliting the POL and POD Pk's
            arrPolPk1 = strPOLPk.Split(' ');
            arrPodPk1 = strPODPk.Split(' ');

            arrPolPk = strPOLPk.Split(',');
            arrPodPk = strPODPk.Split(',');

            if (FromFlag == "CustomerProfit")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL += "  SELECT count(*) FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";

                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.polfk,t.podfk FROM VIEW_CUSTPROFIT_RPT T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + "   AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + "  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + "   AND T.JCPK =" + Convert.ToString(arr.GetValue(2));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + "   AND T.POLFK in(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                    {
                        strSQL = strSQL + "  AND T.PODFK in(" + Convert.ToString(arr.GetValue(4)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + "  AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')";
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                    {
                        strSQL = strSQL + "   AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(8))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                    {
                        strSQL = strSQL + " AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                    {
                        strSQL = strSQL + " AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(11))) > 0)
                    {
                        strSQL = strSQL + " AND T.COMM_GRP_GK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(11)));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(12))))
                    {
                        strSQL = strSQL + " AND NVL(T.REF_GROUP_CUST_PK,0) IN(" + Convert.ToString(arr.GetValue(12)) + ")";
                    }
                    strSQL = strSQL + " )";
                }
                strSQL += " )";
            }
            else if (FromFlag == "FRTOUT")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL += "  SELECT count(*) FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";

                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.POLFK,T.PODFK FROM VIEW_FRTOUT_RPT T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + " AND (FETCH_FRIEGHT_OUTSTANDING.GETINVOICES(T.JCPK,T.BUSINESS_TYPE," + Convert.ToInt32(arr.GetValue(0)) + ",T.JOB_TYPE) - (FETCH_FRIEGHT_OUTSTANDING.GETCOLLECTIONS(T.JCPK,T.BUSINESS_TYPE," + Convert.ToInt32(arr.GetValue(0)) + ")))>0";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + " AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + " AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + " AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                    {
                        strSQL = strSQL + " AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(4)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + " AND UPPER(T.VSL_ID)='" + Convert.ToString(arr.GetValue(5)).ToUpper() + "'";
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0)
                    {
                        strSQL = strSQL + " AND T.JOB_TYPE =" + Convert.ToString(arr.GetValue(6));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(7))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                    {
                        strSQL = strSQL + " AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8)));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(9)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(10)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(9)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(10)) + "','DD/MM/YYYY')";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(11))))
                    {
                        strSQL = strSQL + " AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(11)) + ")";
                    }
                    strSQL = strSQL + " )";
                }
                strSQL += " )";
            }
            else if (FromFlag == "JCProfit")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL += "  SELECT count(*) FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";

                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.polfk,t.podfk FROM VIEW_JCPROFIT_RPT T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + "   AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + "  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + "   AND T.JCPK =" + Convert.ToString(arr.GetValue(2));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + "   AND T.POLFK in(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                    {
                        strSQL = strSQL + "  AND T.PODFK in(" + Convert.ToString(arr.GetValue(4)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + "  AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')";
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                    {
                        strSQL = strSQL + "   AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(8))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                    {
                        strSQL = strSQL + " AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                    {
                        strSQL = strSQL + " AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(11))) > 0)
                    {
                        strSQL = strSQL + " AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(11)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) != 1)
                    {
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) == 2)
                        {
                            strSQL = strSQL + " AND NVL(T.FD_SERVICE_TYPE_FLAG,0)=0";
                            strSQL = strSQL + " AND NVL(T.CST_SERVICE_TYPE_FLAG,0)=0";
                        }
                        else if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) == 3)
                        {
                            strSQL = strSQL + " AND NVL(T.FD_SERVICE_TYPE_FLAG,1)=1";
                            strSQL = strSQL + " AND NVL(T.CST_SERVICE_TYPE_FLAG,1)=1";
                        }
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(13))))
                    {
                        strSQL = strSQL + " AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(13)) + ")";
                    }
                    strSQL = strSQL + " AND (T.FRTPK IS NOT NULL OR T.CSTPK IS NOT NULL)";
                    strSQL = strSQL + " )";
                }
                strSQL += " )";
            }
            else if (FromFlag == "OTC")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL += "  SELECT count(*) FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";

                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.polfk,t.podfk FROM VIEW_OTC T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + "   AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + "  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + "   AND T.POLFK in(" + Convert.ToString(arr.GetValue(2)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + "  AND T.PODFK in(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + "  AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }

                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6)));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(7))) & Convert.ToString(arr.GetValue(7)) != "All")
                    {
                        strSQL = strSQL + " AND UPPER(T.SHIPMENT_STATUS) ='" + Convert.ToString(arr.GetValue(7)).ToUpper() + "'";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(8))))
                    {
                        strSQL = strSQL + " AND UPPER(T.VESSEL_ID) ='" + Convert.ToString(arr.GetValue(8)).ToUpper() + "'";
                    }
                    strSQL = strSQL + " )";
                }
                strSQL += " )";
            }
            else if (FromFlag == "TOPCUST")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL += "  SELECT count(*) FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";

                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.POLFK,T.PODFK FROM VIEW_TOPCUST_RPT T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + " AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + " AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + " AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + " AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                    {
                        strSQL = strSQL + " AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                    {
                        strSQL = strSQL + " AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                    {
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) == 2)
                        {
                            strSQL = strSQL + " AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9)));
                        }
                    }
                    strSQL = strSQL + " AND ( NUMBEROF_TEUS(T.JCPK, T.PROCESS_TYPE, T.BUSINESS_TYPE," + Convert.ToInt32(Convert.ToString(arr.GetValue(9))) + ",T.JOB_TYPE)>0 OR";
                    strSQL = strSQL + " CASE WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=1 THEN ";
                    strSQL = strSQL + "  FETCH_JOB_CARD_SEA_EXP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=2 THEN ";
                    strSQL = strSQL + "  FETCH_JOB_CARD_SEA_IMP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=1 THEN ";
                    strSQL = strSQL + "  FETCH_JOB_CARD_AIR_EXP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=2 THEN ";
                    strSQL = strSQL + "  FETCH_JOB_CARD_AIR_IMP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=2 THEN ";
                    strSQL = strSQL + "  FETCH_CBJC_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=3 THEN ";
                    strSQL = strSQL + "  FETCH_TPT_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)";
                    strSQL = strSQL + "  END >0)";
                    strSQL = strSQL + " )";
                }
                strSQL += " )";
            }
            else
            {
                if (strGroup == 1)
                {
                    strSQL = " SELECT COUNT(*) FROM (SELECT POLGP.PORT_GRP_MST_PK PORT_MST_PK,";
                    strSQL += " POLGP.PORT_GRP_ID AS POL,";
                    strSQL += " PODGP.PORT_GRP_MST_PK PORT_MST_PK,";
                    strSQL += " PODGP.PORT_GRP_ID PORT_ID,";
                    strSQL += " '1' CHK";
                    strSQL += " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP";
                    strSQL += " WHERE(POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK)";
                    strSQL += " AND POLGP.PORT_GRP_MST_PK IN (" + strPOLPk + ")";
                    strSQL += " AND PODGP.PORT_GRP_MST_PK IN (" + strPODPk + ")";
                    strSQL += " AND POLGP.BIZ_TYPE IN(3,1) ";
                    strSQL += " AND PODGP.BIZ_TYPE IN(3,1) ";
                    strSQL += " )";
                }
                else if (strGroup == 2)
                {
                    strSQL = " SELECT COUNT(*) FROM (SELECT POLGP.PORT_GRP_MST_PK PORT_MST_PK,";
                    strSQL += " POLGP.PORT_GRP_ID AS POL,";
                    strSQL += " PODGP.PORT_GRP_MST_PK PORT_MST_PK,";
                    strSQL += " PODGP.PORT_GRP_ID PORT_ID,";
                    strSQL += " '1' CHK";
                    strSQL += " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP";
                    strSQL += " WHERE(POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK)";
                    strSQL += " AND POLGP.PORT_GRP_MST_PK IN (" + strPOLPk + ")";
                    strSQL += " AND PODGP.PORT_GRP_MST_PK IN (" + strPODPk + ")";
                    strSQL += " AND POLGP.BIZ_TYPE IN(3,1) ";
                    strSQL += " AND PODGP.BIZ_TYPE IN(3,1) ";
                    strSQL += " )";
                }
                else
                {
                    if (!string.IsNullOrEmpty(strPolPodCondition))
                    {
                        if (From != "QUOTATION")
                        {
                            Array SelectedSectors = null;
                            Array SelectedSector = null;
                            SelectedSectors = strPolPodCondition.Split(',');
                            for (i = 0; i <= SelectedSectors.Length - 2; i++)
                            {
                                SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                                if (string.IsNullOrEmpty(strCondition))
                                {
                                    strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                                }
                                else
                                {
                                    strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                                }
                            }
                        }
                        else
                        {
                            strCondition = " ( POL.PORT_MST_PK IN (" + arrPolPk1.GetValue(0) + ") AND POD.PORT_MST_PK IN (" + arrPodPk1.GetValue(0) + "))";
                        }
                    }
                    //Making condition as the record should have only selected POL and POD
                    //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
                    //is the selected sector.
                    if (From != "QUOTATION")
                    {
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
                    }
                    else
                    {
                        strCondition = " (POL.PORT_MST_PK IN (" + arrPolPk1.GetValue(0) + ") AND POD.PORT_MST_PK IN (" + arrPodPk1.GetValue(0) + "))";
                    }




                    strSQL ="";


                    strSQL += " SELECT count(*) FROM (SELECT POL.PORT_MST_PK ,POL.PORT_ID AS \"POL\",";
                    strSQL += " POD.PORT_MST_PK,POD.PORT_ID,'true' CHK ";
                    strSQL += "  FROM PORT_MST_TBL POL, PORT_MST_TBL POD  ";
                    strSQL += " WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  ";
                    if (Biztype != 3 & Biztype != 0)
                    {
                        strSQL += " AND POL.BUSINESS_TYPE IN(3," + Biztype + ") ";
                        strSQL += " AND POD.BUSINESS_TYPE IN(3," + Biztype + ") ";
                    }
                    strSQL += " AND ( " + strCondition + " ) ";
                    if (From != "QUOTATION")
                    {
                        strSQL += "  UNION  ";
                        strSQL += "  SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                        strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                        strSQL += "  (CASE WHEN (" + strCondition + ") ";
                        strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                        strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                        strSQL += "  PORT_MST_TBL POL, ";
                        strSQL += "  PORT_MST_TBL POD ";
                        if (!string.IsNullOrEmpty(FromFlag) & FromFlag == "import")
                        {
                            strSQL += "  WHERE SMT.FROM_PORT_FK = POD.PORT_MST_PK(+) ";
                            strSQL += "  AND  SMT.TO_PORT_FK = POL.PORT_MST_PK(+) ";
                        }
                        else
                        {
                            strSQL += "  WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK(+) ";
                            strSQL += "  AND  SMT.TO_PORT_FK = POD.PORT_MST_PK(+) ";
                        }
                        //'Import side changed by Manoharan 15June2007: 
                        if (Process == 2)
                        {
                            strSQL += "AND   SMT.TO_PORT_FK IN (SELECT LPM.Port_Mst_Fk FROM LOC_PORT_MAPPING_TRN LPM WHERE LPM.LOCATION_MST_FK =" + LocationPk + ") ";
                        }
                        else
                        {
                            strSQL += "AND   SMT.FROM_PORT_FK IN (SELECT LPM.Port_Mst_Fk FROM LOC_PORT_MAPPING_TRN LPM WHERE LPM.LOCATION_MST_FK =" + LocationPk + ") ";
                        }

                        //strSQL = strSQL & "AND   SMT.ACTIVE = 1 " & vbCrLf & _
                        // "AND   SMT.BUSINESS_TYPE IN(3," & Biztype & ") "
                        strSQL = strSQL + " AND   SMT.ACTIVE = 1 ";
                        if (Biztype != 3 & Biztype != 0)
                        {
                            strSQL = strSQL + " AND   SMT.BUSINESS_TYPE IN(3," + Biztype + ") ";
                        }
                        if (lngSelectedPolPk > 0)
                        {
                            strSQL += "AND   POL.PORT_MST_PK  = " + lngSelectedPolPk;
                        }
                    }
                    strSQL += " )";
                }
            }

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            //----------------------------------------------------------------------

            //Creating the sql if the user has already selected one port pair in calling form 
            //incase of veiwing also then that port pair will come and active port pair in the grid.
            //BUSINESS_TYPE = 1 :- Is the business type for AIR     
            strSQL ="";
            if (FromFlag == "CustomerProfit")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL = "SELECT QRY.*  FROM (SELECT ROWNUM SL_NO, Q.* FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where   SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";

                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.polfk,t.podfk FROM VIEW_CUSTPROFIT_RPT T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + "   AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + "  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + "   AND T.JCPK =" + Convert.ToString(arr.GetValue(2));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + "   AND T.POLFK in(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                    {
                        strSQL = strSQL + "  AND T.PODFK in(" + Convert.ToString(arr.GetValue(4)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + "  AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')";
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                    {
                        strSQL = strSQL + "   AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(8))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                    {
                        strSQL = strSQL + " AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                    {
                        strSQL = strSQL + " AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(11))) > 0)
                    {
                        strSQL = strSQL + " AND T.COMM_GRP_GK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(11)));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(12))))
                    {
                        strSQL = strSQL + " AND NVL(T.REF_GROUP_CUST_PK,0) IN(" + Convert.ToString(arr.GetValue(12)) + ")";
                    }
                    strSQL = strSQL + " )";
                }
                strSQL += "ORDER BY CHK DESC )Q ";
                strSQL += ")QRY WHERE QRY.SL_NO between " + start + " and " + last;
            }
            else if (FromFlag == "FRTOUT")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL = "SELECT QRY.*  FROM (SELECT ROWNUM SL_NO, Q.* FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where   SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";

                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.POLFK,T.PODFK FROM VIEW_FRTOUT_RPT T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + " AND (FETCH_FRIEGHT_OUTSTANDING.GETINVOICES(T.JCPK,T.BUSINESS_TYPE," + Convert.ToInt32(arr.GetValue(0)) + ",T.JOB_TYPE) - (FETCH_FRIEGHT_OUTSTANDING.GETCOLLECTIONS(T.JCPK,T.BUSINESS_TYPE," + Convert.ToInt32(arr.GetValue(0)) + ")))>0";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + " AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + " AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + " AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                    {
                        strSQL = strSQL + " AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(4)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + " AND UPPER(T.VSL_ID)='" + Convert.ToString(arr.GetValue(5)).ToUpper() + "'";
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0)
                    {
                        strSQL = strSQL + " AND T.JOB_TYPE =" + Convert.ToString(arr.GetValue(6));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(7))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                    {
                        strSQL = strSQL + " AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8)));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(9)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(10)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(9)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(9)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(10)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(10)) + "','DD/MM/YYYY')";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(11))))
                    {
                        strSQL = strSQL + " AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(11)) + ")";
                    }
                    strSQL = strSQL + " )";
                }
                strSQL += "ORDER BY CHK DESC )Q ";
                strSQL += ")QRY WHERE QRY.SL_NO between " + start + " and " + last;
            }
            else if (FromFlag == "JCProfit")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL = "SELECT QRY.*  FROM (SELECT ROWNUM SL_NO, Q.* FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where   SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";

                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.polfk,t.podfk FROM VIEW_JCPROFIT_RPT T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + "   AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + "  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + "   AND T.JCPK =" + Convert.ToString(arr.GetValue(2));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + "   AND T.POLFK in(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4))))
                    {
                        strSQL = strSQL + "  AND T.PODFK in(" + Convert.ToString(arr.GetValue(4)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + "  AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(6)) + "','DD/MM/YYYY')";
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                    {
                        strSQL = strSQL + "   AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(8))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                    {
                        strSQL = strSQL + " AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(10))) > 0)
                    {
                        strSQL = strSQL + " AND T.JOB_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(10)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(11))) > 0)
                    {
                        strSQL = strSQL + " AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(11)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) != 1)
                    {
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) == 2)
                        {
                            strSQL = strSQL + " AND NVL(T.FD_SERVICE_TYPE_FLAG,0)=0";
                            strSQL = strSQL + " AND NVL(T.CST_SERVICE_TYPE_FLAG,0)=0";
                        }
                        else if (Convert.ToInt32(Convert.ToString(arr.GetValue(12))) == 3)
                        {
                            strSQL = strSQL + " AND NVL(T.FD_SERVICE_TYPE_FLAG,1)=1";
                            strSQL = strSQL + " AND NVL(T.CST_SERVICE_TYPE_FLAG,1)=1";
                        }
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(13))))
                    {
                        strSQL = strSQL + " AND T.REF_GROUP_CUST_PK IN(" + Convert.ToString(arr.GetValue(13)) + ")";
                    }
                    strSQL = strSQL + " AND (T.FRTPK IS NOT NULL OR T.CSTPK IS NOT NULL)";
                    strSQL = strSQL + " )";
                }
                strSQL += "ORDER BY CHK DESC )Q ";
                strSQL += ")QRY WHERE QRY.SL_NO between " + start + " and " + last;
            }
            else if (FromFlag == "OTC")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL = "SELECT QRY.*  FROM (SELECT ROWNUM SL_NO, Q.* FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where   SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";
                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.polfk,t.podfk FROM VIEW_OTC T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + "   AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + "  AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + "   AND T.POLFK in(" + Convert.ToString(arr.GetValue(2)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + "  AND T.PODFK in(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + "  AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(6)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.BOOKING_DATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }

                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6)));
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(7))) & Convert.ToString(arr.GetValue(7)) != "All")
                    {
                        strSQL = strSQL + " AND UPPER(T.SHIPMENT_STATUS) ='" + Convert.ToString(arr.GetValue(7)).ToUpper() + "'";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(8))))
                    {
                        strSQL = strSQL + " AND UPPER(T.VESSEL_ID) ='" + Convert.ToString(arr.GetValue(8)).ToUpper() + "'";
                    }
                    strSQL = strSQL + " )";
                }
                strSQL += "ORDER BY CHK DESC )Q ";
                strSQL += ")QRY WHERE QRY.SL_NO between " + start + " and " + last;
            }
            else if (FromFlag == "TOPCUST")
            {
                if (!string.IsNullOrEmpty(strPolPodCondition))
                {
                    Array SelectedSectors = null;
                    Array SelectedSector = null;
                    SelectedSectors = strPolPodCondition.Split(',');
                    for (i = 0; i <= SelectedSectors.Length - 2; i++)
                    {
                        SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                        if (string.IsNullOrEmpty(strCondition))
                        {
                            strCondition = " (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + SelectedSector.GetValue(0) + " AND POD.PORT_MST_PK =" + SelectedSector.GetValue(1) + ")";
                        }
                    }
                }
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

                strSQL ="";
                strSQL = "SELECT QRY.*  FROM (SELECT ROWNUM SL_NO, Q.* FROM (SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ";
                strSQL += "  POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ";
                strSQL += "  (CASE WHEN (" + strCondition + ") ";
                strSQL += "  THEN 'true' ELSE 'false' END ) CHK ";
                strSQL += "  FROM SECTOR_MST_TBL SMT, ";
                strSQL += "  PORT_MST_TBL POL, ";
                strSQL += "  PORT_MST_TBL POD ";
                strSQL = strSQL + " where   SMT.ACTIVE = 1 ";
                strSQL = strSQL + " AND   smt.from_port_fk=POL.PORT_MST_PK and smt.to_port_fk=POD.PORT_MST_PK ";

                if (arr.Length > 0)
                {
                    strSQL = strSQL + " AND (POL.PORT_MST_PK,POD.PORT_MST_PK) in(";
                    strSQL = strSQL + " SELECT T.POLFK,T.PODFK FROM VIEW_TOPCUST_RPT T WHERE 1=1 ";
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0))))
                    {
                        strSQL = strSQL + " AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1))))
                    {
                        strSQL = strSQL + " AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(1)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2))))
                    {
                        strSQL = strSQL + " AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3))))
                    {
                        strSQL = strSQL + " AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")";
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY') AND TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    else if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + Convert.ToString(arr.GetValue(4)) + "','DD/MM/YYYY')";
                    }
                    else if (string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)).Trim()) & !string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim()))
                    {
                        strSQL = strSQL + " AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + Convert.ToString(arr.GetValue(5)) + "','DD/MM/YYYY')";
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) > 0 & Convert.ToInt32(Convert.ToString(arr.GetValue(6))) != 3)
                    {
                        strSQL = strSQL + " AND T.BUSINESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(6)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(7))) > 0)
                    {
                        strSQL = strSQL + " AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(Convert.ToString(arr.GetValue(7)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(8))) > 0)
                    {
                        strSQL = strSQL + " AND T.PROCESS_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(8)));
                    }
                    if (Convert.ToInt32(Convert.ToString(arr.GetValue(9))) > 0)
                    {
                        if (Convert.ToInt32(Convert.ToString(arr.GetValue(6))) == 2)
                        {
                            strSQL = strSQL + " AND T.CARGO_TYPE =" + Convert.ToInt32(Convert.ToString(arr.GetValue(9)));
                        }
                    }
                    strSQL = strSQL + " AND ( NUMBEROF_TEUS(T.JCPK, T.PROCESS_TYPE, T.BUSINESS_TYPE," + Convert.ToInt32(Convert.ToString(arr.GetValue(9))) + ",T.JOB_TYPE)>0 OR";
                    strSQL = strSQL + " CASE WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=1 THEN ";
                    strSQL = strSQL + "  FETCH_JOB_CARD_SEA_EXP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=2 THEN ";
                    strSQL = strSQL + "  FETCH_JOB_CARD_SEA_IMP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=1 THEN ";
                    strSQL = strSQL + "  FETCH_JOB_CARD_AIR_EXP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=2 THEN ";
                    strSQL = strSQL + "  FETCH_JOB_CARD_AIR_IMP_ACTREV( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ")";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=2 THEN ";
                    strSQL = strSQL + "  FETCH_CBJC_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)";
                    strSQL = strSQL + "  WHEN T.JOB_TYPE=3 THEN ";
                    strSQL = strSQL + "  FETCH_TPT_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(Convert.ToString(arr.GetValue(10))) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)";
                    strSQL = strSQL + "  END >0)";
                    strSQL = strSQL + " )";
                }
                strSQL += "ORDER BY CHK DESC )Q ";
                strSQL += ")QRY WHERE QRY.SL_NO between " + start + " and " + last;
            }
            else
            {
                if (strGroup == 1)
                {
                    strSQL = "SELECT QRY.*  FROM (SELECT ROWNUM SL_NO, Q.* FROM (  ";
                    strSQL += " SELECT POLGP.PORT_GRP_MST_PK POL_PK,";
                    strSQL += " POLGP.PORT_GRP_ID AS POL,";
                    strSQL += " PODGP.PORT_GRP_MST_PK POD_PK,";
                    strSQL += " PODGP.PORT_GRP_ID POD,";
                    strSQL += " '1' CHK";
                    strSQL += " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP";
                    strSQL += " WHERE(POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK)";
                    strSQL += " AND POLGP.PORT_GRP_MST_PK IN (" + strPOLPk + ")";
                    strSQL += " AND PODGP.PORT_GRP_MST_PK IN (" + strPODPk + ")";
                    strSQL += " AND POLGP.BIZ_TYPE IN(3,1) ";
                    strSQL += " AND PODGP.BIZ_TYPE IN(3,1) ";
                    strSQL += " ORDER BY POL,CHK )Q ";
                    strSQL += " )QRY WHERE QRY.SL_NO BETWEEN " + start + " AND " + last;
                }
                else if (strGroup == 2)
                {
                    strSQL = "SELECT QRY.*  FROM (SELECT ROWNUM SL_NO, Q.* FROM (  ";
                    strSQL += " SELECT POLGP.PORT_GRP_MST_PK POL_PK,";
                    strSQL += " POLGP.PORT_GRP_ID AS POL,";
                    strSQL += " PODGP.PORT_GRP_MST_PK POD_PK,";
                    strSQL += " PODGP.PORT_GRP_ID POD,";
                    strSQL += " '1' CHK";
                    strSQL += " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP";
                    strSQL += " WHERE(POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK)";
                    strSQL += " AND POLGP.PORT_GRP_MST_PK IN (" + strPOLPk + ")";
                    strSQL += " AND PODGP.PORT_GRP_MST_PK IN (" + strPODPk + ")";
                    strSQL += " AND POLGP.BIZ_TYPE IN(3,1) ";
                    strSQL += " AND PODGP.BIZ_TYPE IN(3,1) ";
                    strSQL += " ORDER BY POL,CHK )Q ";
                    strSQL += " )QRY WHERE QRY.SL_NO BETWEEN " + start + " AND " + last;
                }
                else
                {
                    strSQL = "SELECT QRY.*  FROM (SELECT ROWNUM SL_NO, Q.* FROM ( SELECT POL.PORT_MST_PK POL_PK,POL.PORT_ID POL, ";
                    if (From != "QUOTATION")
                    {
                        strSQL += "POD.PORT_MST_PK POD_PK,POD.PORT_ID POD,'true' CHK ";
                    }
                    else
                    {
                        //strSQL &= "POD.PORT_MST_PK POD_PK,POD.PORT_ID POD,' ' CHK "
                        strSQL += "POD.PORT_MST_PK POD_PK,POD.PORT_ID POD,'true' CHK ";
                    }
                    strSQL += "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  ";
                    strSQL += "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  ";
                    if (Biztype != 3 & Biztype != 0)
                    {
                        strSQL += "AND POL.BUSINESS_TYPE IN(3," + Biztype + ") ";
                        strSQL += "AND POD.BUSINESS_TYPE IN(3," + Biztype + ") ";
                    }
                    strSQL += "AND ( " + strCondition + " ) ";
                    if (From != "QUOTATION")
                    {
                        strSQL += "UNION ";
                        strSQL += "SELECT POL.PORT_MST_PK POL_PK, POL.PORT_ID POL, ";
                        strSQL += "POD.PORT_MST_PK POD_PK, POD.PORT_ID POD, ";
                        strSQL += "(CASE WHEN (" + strCondition + ") ";
                        strSQL += "THEN 'true' ELSE 'false' END ) CHK ";
                        strSQL += "FROM SECTOR_MST_TBL SMT, ";
                        strSQL += "PORT_MST_TBL POL, ";
                        strSQL += "PORT_MST_TBL POD ";
                        if (!string.IsNullOrEmpty(FromFlag) & FromFlag == "import")
                        {
                            strSQL += "WHERE SMT.FROM_PORT_FK = POD.PORT_MST_PK(+) ";
                            strSQL += " AND   SMT.TO_PORT_FK = POL.PORT_MST_PK(+) ";
                        }
                        else
                        {
                            strSQL += "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK(+) ";
                            strSQL += " AND   SMT.TO_PORT_FK = POD.PORT_MST_PK(+) ";
                        }

                        //'Import side changed by Manoharan 15June2007: 
                        if (Process == 2)
                        {
                            strSQL += "AND   SMT.TO_PORT_FK IN (SELECT LPM.Port_Mst_Fk FROM LOC_PORT_MAPPING_TRN LPM WHERE LPM.LOCATION_MST_FK =" + LocationPk + ") ";
                        }
                        else
                        {
                            strSQL += "AND   SMT.FROM_PORT_FK IN (SELECT LPM.Port_Mst_Fk FROM LOC_PORT_MAPPING_TRN LPM WHERE LPM.LOCATION_MST_FK =" + LocationPk + ") ";
                        }

                        //   strSQL &= " AND   SMT.ACTIVE = 1 " & vbCrLf & _
                        //"AND   SMT.BUSINESS_TYPE IN(3," & Biztype & ") "
                        strSQL += " AND   SMT.ACTIVE = 1 ";
                        if (Biztype != 3 & Biztype != 0)
                        {
                            strSQL += " AND   SMT.BUSINESS_TYPE IN(3," + Biztype + ")";
                        }
                        if (lngSelectedPolPk > 0)
                        {
                            strSQL += " AND   POL.PORT_MST_PK  = " + lngSelectedPolPk;
                        }
                        //'Added By Koteshwari on 02/11/2011
                    }
                    else
                    {
                        strSQL += "  UNION ";
                        strSQL += "  SELECT POL.PORT_MST_PK POL_PK,POL.PORT_ID POL,";
                        strSQL += "  POD.PORT_MST_PK POD_PK,POD.PORT_ID POD,'false' CHK";
                        strSQL += "  FROM PORT_MST_TBL POL, PORT_MST_TBL POD  ";
                        strSQL += "  WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  ";
                        if (Biztype != 0)
                        {
                            strSQL += "AND POL.BUSINESS_TYPE IN(3," + Biztype + ") ";
                            strSQL += "AND POD.BUSINESS_TYPE IN(3," + Biztype + ") ";
                        }
                    }
                    //'End
                    strSQL += "ORDER BY CHK DESC )Q ";
                    strSQL += ")QRY WHERE QRY.SL_NO between " + start + " and " + last;
                }
            }
            try
            {
                return objWF.GetDataTable(strSQL);
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //Catch SQLEX As Exception
                //    Throw SQLEX
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataTable ActiveLocation(string userpk, Int32 PK, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            string strSQL1 = null;
            Array arrPk = null;
            string strCondition = null;
            Int16 i = default(Int16);

            arrPk = userpk.Split(',');
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (!string.IsNullOrEmpty(userpk))
            {
                Array SelectedSectors = null;
                Array SelectedSector = null;
                SelectedSectors = userpk.Split(',');
                for (i = 0; i <= SelectedSectors.Length - 2; i++)
                {
                    SelectedSector = Convert.ToString(SelectedSectors.GetValue(i)).Split('~');
                    if (string.IsNullOrEmpty(strCondition))
                    {
                        strCondition = " (loc.location_mst_pk =" + SelectedSector.GetValue(0) + ")";
                    }
                    else
                    {
                        strCondition += " OR (loc.location_mst_pk =" + SelectedSector.GetValue(0) + ")";
                    }
                }
            }

            for (i = 0; i <= arrPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (loc.location_mst_pk =" + arrPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (loc.location_mst_pk =" +  arrPk.GetValue(i) + ")";
                }
            }

            //strSQL &= vbCrLf & "  select user_mst_pk as ""HIDDEN"", User_ID as ""ID"", User_Name as ""User Name"" ,'1' CHK ,UMT.DEFAULT_LOCATION_FK"
            //strSQL &= vbCrLf & "  from user_mst_tbl UMT,location_mst_tbl LMT "
            //strSQL &= vbCrLf & "  where LMT.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK"
            //strSQL &= vbCrLf & "  and umt.is_activated=1"
            //strSQL &= vbCrLf & "  and lmt.location_mst_pk=" & PK
            //strSQL &= vbCrLf & " and umt.user_mst_pk in ( " & userpk & ")"
            //strSQL &= vbCrLf & " AND ( " & strCondition & " ) "
            //'strSQL &= vbCrLf & "  order by user_id"
            //strSQL &= vbCrLf & " UNION "
            //'strSQL &= vbCrLf & "select rownum slno,"
            //strSQL &= vbCrLf & "  select user_mst_pk as ""HIDDEN"", User_ID as ""ID"", User_Name as ""User Name"" ,"
            //strSQL &= vbCrLf & "(CASE WHEN (" & strCondition & ") "
            //strSQL &= vbCrLf & " THEN 'true' ELSE 'false' END ) CHK ,UMT.DEFAULT_LOCATION_FK"
            //strSQL &= vbCrLf & "  from user_mst_tbl UMT,location_mst_tbl LMT "
            //strSQL &= vbCrLf & "  where LMT.LOCATION_MST_PK=UMT.DEFAULT_LOCATION_FK"
            //strSQL &= vbCrLf & "  and umt.is_activated=1"
            //strSQL &= vbCrLf & "  and lmt.location_mst_pk=" & PK
            //strSQL &= vbCrLf & " and umt.user_mst_pk not in ( " & userpk & ")"
            //strSQL &= vbCrLf & "  order by user_id"

            strSQL +=  " select loc.location_mst_pk as \"HIDDEN\", loc.location_id as \"LocationID\", ";
            strSQL +=  " loc.location_name as \"LocationName\",'1' CHK ";
            strSQL +=  " from location_mst_tbl loc,country_mst_tbl Con  ";
            strSQL +=  " where con.country_mst_pk=loc.country_mst_fk ";
            strSQL +=  " and loc.country_mst_fk= " + PK;
            strSQL +=  " and loc.location_mst_pk in ( " + userpk + ")";
            strSQL +=  " AND  " + strCondition + "  ";
            strSQL +=  " and loc.active_flag=1";
            //strSQL &= vbCrLf & " order by location_id"
            strSQL +=  " UNION ";
            strSQL +=  " select loc.location_mst_pk as \"HIDDEN\", loc.location_id as \"LocationID\", ";
            strSQL +=  " loc.location_name as \"LocationName\", ";
            strSQL +=  "(CASE WHEN (" + strCondition + ") ";
            strSQL +=  " THEN 'true' ELSE 'false' END ) CHK ";
            strSQL +=  " from location_mst_tbl loc,country_mst_tbl Con  ";
            strSQL +=  " where con.country_mst_pk=loc.country_mst_fk ";
            strSQL +=  " and loc.country_mst_fk= " + PK;
            strSQL +=  " and loc.location_mst_pk not in ( " + userpk + ")";
            //strSQL &= vbCrLf & " AND ( " & strCondition & " ) "
            strSQL +=  " and loc.active_flag=1";
            //strSQL &= vbCrLf & " order by location_id"
            strSQL1 = " select count(*) from (";
            strSQL1 += strSQL + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
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

            strSQL1 = " select * from (";
            strSQL1 += strSQL;
            strSQL1 +=  " ) WHERE SlNo Between " + start + " and " + last;

            try
            {
                return objWF.GetDataTable(strSQL);
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }

        }
        #endregion

        //Modified by Mani Reason:To Dispaly AIR (OR) SEA
        #region "Fetch Pol"
        public DataSet FetchPol(long LocationPk, string strPOLPk = "", string strPODPk = "", string Biztype = "", int strGroup = 0, string strTariffGrpPk = "", string FromFlag = "")
        {

            //Dim strSQL As String
            StringBuilder strSQL = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;
            arrPolPk = strPOLPk.Split(',');
            arrPodPk = strPODPk.Split(',');
            //Making condition as the record should have only selected POL and POD
            //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
            //is the selected sector.
            if (strGroup == 1)
            {
                strSQL.Append("  SELECT 0 POL_PK, '<ALL>' POL");
                strSQL.Append("  FROM DUAL ");
                strSQL.Append("  UNION  ");
                strSQL.Append("  SELECT DISTINCT  POLGP.PORT_GRP_MST_PK POL_PK, POLGP.PORT_GRP_ID POL");
                strSQL.Append("  FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP");
                strSQL.Append("  WHERE POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK");
                strSQL.Append("   AND POLGP.PORT_GRP_MST_PK IN (" + strPOLPk + ")");
                strSQL.Append("   AND PODGP.PORT_GRP_MST_PK IN (" + strPODPk + ")");
                strSQL.Append("   AND POLGP.BIZ_TYPE IN(3,1) ");
                strSQL.Append("   AND PODGP.BIZ_TYPE IN(3,1) ");
                strSQL.Append(" ORDER BY POL");
            }
            else if (strGroup == 2)
            {
                strSQL.Append("  SELECT 0 POL_PK, '<ALL>' POL");
                strSQL.Append("  FROM DUAL ");
                strSQL.Append("  UNION  ");
                strSQL.Append("  SELECT DISTINCT  POLGP.PORT_GRP_MST_PK POL_PK, POLGP.PORT_GRP_ID POL");
                strSQL.Append("  FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP");
                strSQL.Append("  WHERE POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK");
                strSQL.Append("   AND POLGP.PORT_GRP_MST_PK IN (" + strPOLPk + ")");
                strSQL.Append("   AND PODGP.PORT_GRP_MST_PK IN (" + strPODPk + ")");
                strSQL.Append("   AND POLGP.BIZ_TYPE IN(3,1) ");
                strSQL.Append("   AND PODGP.BIZ_TYPE IN(3,1) ");
                strSQL.Append(" ORDER BY POL");
            }
            else
            {
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
                //strSQL =""

                //strSQL = "SELECT 0  POL_PK, '<ALL>' POL FROM DUAL " & vbCrLf & _
                //         "UNION " & vbCrLf & _
                //         "SELECT DISTINCT POL.PORT_MST_PK POL_PK, POL.PORT_ID POL " & vbCrLf & _
                //         "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  " & vbCrLf & _
                //         "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " & vbCrLf & _
                //         "AND POL.BUSINESS_TYPE = " & Biztype & vbCrLf & _
                //         "AND POD.BUSINESS_TYPE = " & Biztype & vbCrLf & _
                //         "AND ( " & strCondition & " ) " & vbCrLf & _
                //         "UNION " & vbCrLf & _
                //         "SELECT DISTINCT POL.PORT_MST_PK POL_PK, POL.PORT_ID POL " & vbCrLf & _
                //         "FROM SECTOR_MST_TBL SMT, " & vbCrLf & _
                //         "PORT_MST_TBL POL, " & vbCrLf & _
                //         "PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM " & vbCrLf & _
                //         "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " & vbCrLf & _
                //         "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " & vbCrLf & _
                //         "AND   LPM.PORT_MST_FK = POL.PORT_MST_PK " & vbCrLf & _
                //         "AND   POL.BUSINESS_TYPE =" & Biztype & vbCrLf & _
                //         "AND   POD.BUSINESS_TYPE =" & Biztype & vbCrLf & _
                //         "AND   LPM.LOCATION_MST_FK =" & LocationPk & vbCrLf & _
                //         "AND   SMT.ACTIVE = 1 " & vbCrLf & _
                //         "AND   SMT.BUSINESS_TYPE = " & Biztype & vbCrLf & _
                //         "ORDER BY POL "

                strSQL.Append("  SELECT 0 POL_PK, '<ALL>' POL");
                strSQL.Append("  FROM DUAL ");
                strSQL.Append("  UNION  ");
                strSQL.Append("  SELECT DISTINCT POL.PORT_MST_PK POL_PK, POL.PORT_ID POL");
                strSQL.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD");
                strSQL.Append("  WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK");
                if (Convert.ToInt32(Biztype )!= 0)
                {
                    strSQL.Append("   AND POL.BUSINESS_TYPE = " + Biztype);
                    strSQL.Append("   AND POD.BUSINESS_TYPE = " + Biztype);
                }
                strSQL.Append("  AND ( " + strCondition + " ) ");
                strSQL.Append("  UNION ");
                strSQL.Append("  SELECT DISTINCT POL.PORT_MST_PK POL_PK, POL.PORT_ID POL");
                strSQL.Append("  FROM SECTOR_MST_TBL       SMT,");
                strSQL.Append("       PORT_MST_TBL         POL,");
                strSQL.Append("       PORT_MST_TBL         POD,");
                strSQL.Append("       LOC_PORT_MAPPING_TRN LPM");
                if (!string.IsNullOrEmpty(FromFlag) & FromFlag == "import")
                {
                    strSQL.Append("  WHERE SMT.FROM_PORT_FK = POD.PORT_MST_PK");
                    strSQL.Append("   AND SMT.TO_PORT_FK = POL.PORT_MST_PK");
                }
                else
                {
                    strSQL.Append("  WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK");
                    strSQL.Append("   AND SMT.TO_PORT_FK = POD.PORT_MST_PK");
                }
                strSQL.Append("   AND LPM.PORT_MST_FK = POL.PORT_MST_PK");
                if (Convert.ToInt32(Biztype) != 0)
                {
                    strSQL.Append("   AND  POL.BUSINESS_TYPE =" + Biztype);
                    strSQL.Append("   AND  POD.BUSINESS_TYPE =" + Biztype);
                }
                if (!string.IsNullOrEmpty(FromFlag) & FromFlag == "import")
                {
                    strSQL.Append(" AND   SMT.FROM_PORT_FK IN (SELECT LPM.Port_Mst_Fk FROM LOC_PORT_MAPPING_TRN LPM WHERE LPM.LOCATION_MST_FK =" + LocationPk + ") ");
                }
                else
                {
                    strSQL.Append("   AND LPM.LOCATION_MST_FK = " + LocationPk);
                }

                strSQL.Append("   AND SMT.ACTIVE = 1");
                if (Convert.ToInt32(Biztype) != 0)
                {
                    strSQL.Append("  AND SMT.BUSINESS_TYPE = " + Biztype);
                }
                strSQL.Append(" ORDER BY POL");
            }


            try
            {
                return objWF.GetDataSet(strSQL.ToString());
                //Catch SQLEX As Exception
                //    Throw SQLEX
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
        #endregion
    }
}