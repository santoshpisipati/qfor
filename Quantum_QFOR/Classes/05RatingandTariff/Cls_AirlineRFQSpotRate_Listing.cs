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
    public class Cls_AirlineRFQSpotRate_Listing : CommonFeatures
    {
        #region " Fetch All "

        #region " Logical Flow "
        // This Function Fetch Data for both Bands
        // First it fetches for the the First Band in a DataSet according to All given Criteria
        // Then it fetches for the 2nd Band of the grid in a DataTable
        // then it adds the Child Table in that DataSet
        // After that it establishes the parent-Child Relation between these two DataTables of the same DataSet
        #endregion

        public DataSet FetchAll(string AirlineID = "", string AirlineName = "", string CustomerID = "", string CustomerName = "", string RFQRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string FromDate = "",
        string ToDate = "", int Status = 0, bool ActiveOnly = true, string SearchType = "", string SortColumn = " AIRLINE_NAME ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string CurrBizType = "3", long lngUsrLocFk = 0,
        Int32 flag = 0)
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
            // RFQ REF NO
            if (RFQRefNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(RFQ_REF_NO) LIKE '" + SrOP + RFQRefNo.ToUpper().Replace("'", "''") + "%'");
            }
            // PORT OF LOADING
            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM RFQ_SPOT_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE RFQ_SPOT_AIR_FK = main.RFQ_SPOT_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM RFQ_SPOT_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE RFQ_SPOT_AIR_FK = main.RFQ_SPOT_AIR_PK ");
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
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM RFQ_SPOT_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE RFQ_SPOT_AIR_FK = main.RFQ_SPOT_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM RFQ_SPOT_TRN_AIR_TBL ");
                buildCondition.Append("           WHERE RFQ_SPOT_AIR_FK = main.RFQ_SPOT_AIR_PK ");
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


            buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + "");
            buildCondition.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK ");

            if (Status != 3)
            {
                buildCondition.Append(" AND main.APPROVED =  " + Status);
            }
            // ACTIVE ONLY
            if (ActiveOnly == true)
            {
                buildCondition.Append(" AND main.ACTIVE = 1 ");
                buildCondition.Append(" AND ( ");
                buildCondition.Append("        main.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ");
                buildCondition.Append("       OR main.VALID_TO IS NULL ");
                buildCondition.Append("     ) ");
                //Else
                //    buildCondition.Append(vbCrLf & " AND main.ACTIVE = 0 ")
            }
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select count(*) ");
            buildQuery.Append("      from ");
            buildQuery.Append("       RFQ_SPOT_RATE_AIR_TBL main, ");
            buildQuery.Append("       AIRLINE_MST_TBL, ");
            buildQuery.Append("       CUSTOMER_MST_TBL,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       main.AIRLINE_MST_FK = AIRLINE_MST_PK(+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK(+) ");
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
            buildQuery.Append("    ( Select " );
            buildQuery.Append("       RFQ_SPOT_AIR_PK, ");
            buildQuery.Append("       NVL(main.ACTIVE, 0) ACTIVE, ");
            buildQuery.Append("       RFQ_REF_NO, ");
            buildQuery.Append("       to_Char(main.RFQ_DATE, '" + dateFormat + "') RFQ_DATE1, ");

            buildQuery.Append("       AIRLINE_MST_FK, ");
            buildQuery.Append("       AIRLINE_ID, ");
            buildQuery.Append("       AIRLINE_NAME, ");
            buildQuery.Append("       main.CUSTOMER_MST_FK, ");
            buildQuery.Append("       CUSTOMER_ID, ");
            buildQuery.Append("       CUSTOMER_NAME, ");
            //buildQuery.Append(vbCrLf & "       decode(CARGO_TYPE, 1, 'FCL',2, 'LCL') CARGO_TYPE, ")
            buildQuery.Append("       0 POL, ");
            buildQuery.Append("       0 POD, ");
            buildQuery.Append("       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            buildQuery.Append("       main.RFQ_DATE, ");
            //buildQuery.Append(vbCrLf & "       NVL(main.APPROVED, 0) APPROVED ")
            buildQuery.Append("       DECODE(MAIN.APPROVED,0,'Requested',1,'Approved',2,'Rejected') APPROVED,MAIN.RESTRICTED ");
            buildQuery.Append("      from ");
            buildQuery.Append("       RFQ_SPOT_RATE_AIR_TBL main, ");
            buildQuery.Append("       AIRLINE_MST_TBL, ");
            buildQuery.Append("       CUSTOMER_MST_TBL,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       main.AIRLINE_MST_FK = AIRLINE_MST_PK(+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK(+) ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append(" Order By " + SortColumn + SortType);
            //buildQuery.Append(vbCrLf & "     Order By main.RFQ_DATE desc")
            buildQuery.Append(" ,RFQ_REF_NO DESC");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);

            strSQL = buildQuery.ToString();

            // band0_SRNO = 0        :   band0_RFQSpotPK = 1  :    band0_Active = 2        :     band0_RFQRefNo = 3
            // band0_RFQDate = 4     :   band0_AIRLINEFK = 5 :    band0_AIRLINEID = 6    :     band0_AIRLINEName = 7
            // band0_CustomerFK = 8  :   band0_CustomerID = 9 :    band0_CustomerName = 10 :     band0_CargoType = 11
            // band0_POLCount = 12   :   band0_PODCount = 13  :    band0_ValidFrom = 14    :     band0_ValidTo = 15
            //band0_RFQDate = 16
            // band0_Approved = 17
            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                // Adding Child table in the Dataset
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                // Establishing Relation ship between Parent-Child
                DataRelation rfqRel = new DataRelation("RFQRelation", DS.Tables[0].Columns["RFQ_SPOT_AIR_PK"], DS.Tables[1].Columns["RFQ_SPOT_AIR_FK"], true);
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
        // This function basically returns a string of All mater pk of all selected records
        // this string is used in child query with IN condition.
        private string AllMasterPKs(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append("-1,");
            for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
            {
                strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["RFQ_SPOT_AIR_PK"]).Trim() + ",");
            }
            strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion

        #region " Fetch Childs "
        // It gets all Parent Pks and other conditions that are required for fetching child
        // It returns back a DataTable Object
        private DataTable FetchChildFor(string RFQSpotPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {

            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            // band1_SRNO() = 0 :  band1_TarifMainFK = 1 : band1_PortPOLFK = 2 :  band1_PortPOLID = 3
            // band1_PortPOLName = 4 : band1_PortPODFK = 5 :  band1_PortPODID = 6 :  band1_PortPODName = 7
            // band1_ValidFrom = 8 :  band1_ValidTo = 9

            if (RFQSpotPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and RFQ_SPOT_AIR_FK in (" + RFQSpotPKs + ") ");
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
            buildQuery.Append("       RFQ_SPOT_AIR_FK, ");
            buildQuery.Append("       PORT_MST_POL_FK, ");
            buildQuery.Append("       PORTPOL.PORT_ID PORT_POL_ID, ");
            buildQuery.Append("       PORTPOL.PORT_NAME PORT_POL_NAME, ");
            buildQuery.Append("       PORT_MST_POD_FK, ");
            buildQuery.Append("       PORTPOD.PORT_ID PORT_POD_ID, ");
            buildQuery.Append("       PORTPOD.PORT_NAME PORT_POD_NAME, ");
            buildQuery.Append("       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO ");
            buildQuery.Append("      from ");
            buildQuery.Append("       RFQ_SPOT_TRN_AIR_TBL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOD ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK(+) AND ");
            buildQuery.Append("       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK(+) ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("      Order By RFQ_SPOT_AIR_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
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
                    if (Convert.ToInt32(dt.Rows[RowCnt]["RFQ_SPOT_AIR_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["RFQ_SPOT_AIR_FK"]);
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

        #region " Enhance Search Function for AIRLINE for CUSTOMER "

        public string FetchAirlineForCustomer(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strSEARCH_CS_ID_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strSEARCH_CS_ID_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_AIRLINE_PKG.GET_AIRLINE_CUSTOMER_COMMON";
                var _with1 = SCM.Parameters;
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

        #endregion

        #region " Enhance Search Function for CUSTOMER for AIRLINE "

        public string FetchCustomerForAirline(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strSEARCH_OP_ID_IN = "";
            string strCategory_IN = "";
            int intBUSINESS_TYPE_IN = 3;
            string strSearchTBL_IN = "1";
            string strReq = null;
            var strNull = DBNull.Value;
            string SEARCH_FLAG_IN = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strSEARCH_OP_ID_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(3));
            if (arr.Length > 4)
                strCategory_IN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strSearchTBL_IN = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(6));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GET_CUSTOMER_AIRLINE_COMMON";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_OP_ID_IN", ifDBNull(strSEARCH_OP_ID_IN)).Direction = ParameterDirection.Input;
                _with2.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMER_TYPE_IN", ifDBNull(strCategory_IN)).Direction = ParameterDirection.Input;
                //.Add("TABLE_ENUM_IN", strSearchTBL_IN).Direction = ParameterDirection.Input
                _with2.Add("TABLE_ENUM_IN", (string.IsNullOrEmpty(strSearchTBL_IN) ? "" : strSearchTBL_IN)).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #region " Enhance Search Function for RFQ for CUSTOMER-AIRLINE "

        public string FetchRfqForAirlineCustomer(string strCond)
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
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strSEARCH_OP_ID_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strSEARCH_CS_ID_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_RFQ_REF_NO_PKG.GET_AIRLINE_RFQ_COMMON";
                var _with3 = SCM.Parameters;
                _with3.Add("SEARCH_OP_ID_IN", ifDBNull(strSEARCH_OP_ID_IN)).Direction = ParameterDirection.Input;
                _with3.Add("SEARCH_CS_ID_IN", ifDBNull(strSEARCH_CS_ID_IN)).Direction = ParameterDirection.Input;
                _with3.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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