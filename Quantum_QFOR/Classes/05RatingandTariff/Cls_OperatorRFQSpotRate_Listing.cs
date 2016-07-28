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
    public class Cls_OperatorRFQSpotRate_Listing : CommonFeatures
    {

        #region " Fetch All "
        public DataSet FetchAll(string OperatorID = "", string OperatorName = "", string CustomerID = "", string CustomerName = "", string RFQRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string CargoType = "",
        string FromDate = "", string ToDate = "", int Status = 0, bool ActiveOnly = true, string SearchType = "", string SortColumn = " OPERATOR_NAME ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", string CurrBizType = "3",
        long lngUsrLocFk = 0, Int32 flag = 0)
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

            if (CustomerID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CUSTOMER_ID) LIKE '" + SrOP + CustomerID.ToUpper().Replace("'", "''") + "%'");
            }
            if (CustomerName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CUSTOMER_NAME) LIKE '" + SrOP + CustomerName.ToUpper().Replace("'", "''") + "%'");
            }

            if (RFQRefNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(RFQ_REF_NO) LIKE '" + SrOP + RFQRefNo.ToUpper().Replace("'", "''") + "%'");
            }

            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM RFQ_SPOT_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE RFQ_SPOT_SEA_FK = main.RFQ_SPOT_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM RFQ_SPOT_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE RFQ_SPOT_SEA_FK = main.RFQ_SPOT_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }

            if (PODID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM RFQ_SPOT_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE RFQ_SPOT_SEA_FK = main.RFQ_SPOT_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM RFQ_SPOT_TRN_SEA_FCL_LCL ");
                buildCondition.Append("           WHERE RFQ_SPOT_SEA_FK = main.RFQ_SPOT_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }

            if (CargoType.Length > 0)
            {
                buildCondition.Append(" AND CARGO_TYPE = " + CargoType);
            }
            if (ToDate.Length > 0 & FromDate.Length > 0)
            {
                buildCondition.Append("AND ((TO_DATE('" + ToDate + "' , '" + dateFormat + "') BETWEEN");
                buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR");
                buildCondition.Append("    (TO_DATE('" + FromDate + "' , '" + dateFormat + "') BETWEEN");
                buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR");
                buildCondition.Append("    (MAIN.VALID_TO IS NULL))");
            }
            else if (ToDate.Length > 0 & !(FromDate.Length > 0))
            {
                buildCondition.Append(" AND ( ");
                buildCondition.Append("        main.VALID_TO <= TO_DATE('" + ToDate + "' , '" + dateFormat + "') ");
                buildCondition.Append("       OR main.VALID_TO IS NULL ");
                buildCondition.Append("     ) ");
            }
            else if (FromDate.Length > 0 & !(ToDate.Length > 0))
            {
                buildCondition.Append(" AND ( ");
                buildCondition.Append("        main.VALID_FROM >= TO_DATE('" + FromDate + "' , '" + dateFormat + "') ");
                buildCondition.Append("       OR main.VALID_TO IS NULL ");
                buildCondition.Append("     ) ");
            }
            buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + "");
            buildCondition.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK ");

            if (Status != 3)
            {
                buildCondition.Append(" AND main.APPROVED =  " + Status);
            }

            if (ActiveOnly == true)
            {
                buildCondition.Append(" AND main.ACTIVE = 1 ");
                //'buildCondition.Append(vbCrLf & " AND ( ")
                //'buildCondition.Append(vbCrLf & "        main.VALID_TO >= TO_DATE(SYSDATE , '" & dateFormat & "') ")
                //'buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
                //'buildCondition.Append(vbCrLf & "     ) ")
            }
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select count(*) ");
            buildQuery.Append("      from ");
            buildQuery.Append("       RFQ_SPOT_RATE_SEA_TBL main, ");
            buildQuery.Append("       OPERATOR_MST_TBL, ");
            buildQuery.Append("       CUSTOMER_MST_TBL,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK (+) ");
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
            buildQuery.Append("       RFQ_SPOT_SEA_PK, ");
            buildQuery.Append("       NVL(main.ACTIVE, 0) ACTIVE, ");
            buildQuery.Append("       RFQ_REF_NO, ");
            buildQuery.Append("       to_Char(main.RFQ_DATE, '" + dateFormat + "') RFQ_DATE1, ");
            buildQuery.Append("       OPERATOR_MST_FK, ");
            buildQuery.Append("       OPERATOR_ID, ");
            buildQuery.Append("       OPERATOR_NAME, ");
            buildQuery.Append("       main.CUSTOMER_MST_FK, ");
            buildQuery.Append("       CUSTOMER_ID, ");
            buildQuery.Append("       CUSTOMER_NAME, ");
            buildQuery.Append("       decode(CARGO_TYPE, 1, 'FCL',2, 'LCL') CARGO_TYPE, ");
            buildQuery.Append("       0 POL, ");
            buildQuery.Append("       0 POD, ");
            buildQuery.Append("       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            buildQuery.Append("       main.RFQ_DATE, ");
            buildQuery.Append("       DECODE(MAIN.APPROVED,0,'Requested',1,'Approved',2,'Rejected') APPROVED,MAIN.RESTRICTED ");
            buildQuery.Append("      from ");
            buildQuery.Append("       RFQ_SPOT_RATE_SEA_TBL main, ");
            buildQuery.Append("       OPERATOR_MST_TBL, ");
            buildQuery.Append("       CUSTOMER_MST_TBL,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK (+) ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("     Order By main.RFQ_DATE desc");
            buildQuery.Append("  ,RFQ_REF_NO DESC");
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
                DataRelation rfqRel = new DataRelation("RFQRelation", DS.Tables[0].Columns["RFQ_SPOT_SEA_PK"], DS.Tables[1].Columns["RFQ_SPOT_SEA_FK"], true);
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
        #endregion

        #region "AllMasterPKs"
        private string AllMasterPKs(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            try
            {
                strBuilder.Append("-1,");
                for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
                {
                    strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["RFQ_SPOT_SEA_PK"]).Trim() + ",");
                }
                strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
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

        #region " Fetch Childs "
        private DataTable FetchChildFor(string RFQSpotPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {

            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            if (RFQSpotPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and RFQ_SPOT_SEA_FK in (" + RFQSpotPKs + ") ");
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
            buildQuery.Append("       RFQ_SPOT_SEA_FK, ");
            buildQuery.Append("       PORT_MST_POL_FK, ");
            buildQuery.Append("       PORTPOL.PORT_ID PORT_POL_ID, ");
            buildQuery.Append("       PORTPOL.PORT_NAME PORT_POL_NAME, ");
            buildQuery.Append("       PORT_MST_POD_FK, ");
            buildQuery.Append("       PORTPOD.PORT_ID PORT_POD_ID, ");
            buildQuery.Append("       PORTPOD.PORT_NAME PORT_POD_NAME, ");
            buildQuery.Append("       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO ");
            buildQuery.Append("      from ");
            buildQuery.Append("       RFQ_SPOT_TRN_SEA_FCL_LCL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOD ");
            buildQuery.Append("      where ");
            buildQuery.Append("       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK AND ");
            buildQuery.Append("       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("      Order By RFQ_SPOT_SEA_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
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
                    if (Convert.ToInt32(dt.Rows[RowCnt]["RFQ_SPOT_SEA_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["RFQ_SPOT_SEA_FK"]);
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

        #region " Enhance Search Function for OPERATOR for CUSTOMER "
        public string FetchOperatorForCustomer(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strSEARCH_CS_ID_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            string SEARCH_FLAG_IN = "";
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strSEARCH_CS_ID_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_OPERATOR_PKG.GET_OPERATOR_CUSTOMER_COMMON";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_CS_ID_IN", ifDBNull(strSEARCH_CS_ID_IN)).Direction = ParameterDirection.Input;
                _with1.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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

        #region " Enhance Search Function for CUSTOMER for OPERATOR "

        public string FetchCustomerForOperator(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            string strLOC_MST_IN = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strCategory_IN = "";
            string strSEARCH_OP_ID_IN = "";
            int intBUSINESS_TYPE_IN = 3;
            string strSearchTBL_IN = "1";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strSEARCH_OP_ID_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(3));
            if (arr.Length > 4)
                strCategory_IN = Convert.ToString(arr.GetValue(4));
            //modified by thiyagarajan on 8/11/08 
            if (arr.Length > 5)
                strSearchTBL_IN = Convert.ToString(getDefault(Convert.ToString(arr.GetValue(5)), "0"));
            if (arr.Length > 6)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(6));
            //end
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GET_CUSTOMER_OPERATOR_COMMON";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_OP_ID_IN", ifDBNull(strSEARCH_OP_ID_IN)).Direction = ParameterDirection.Input;
                _with2.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMER_TYPE_IN", ifDBNull(strCategory_IN)).Direction = ParameterDirection.Input;
                _with2.Add("TABLE_ENUM_IN", strSearchTBL_IN).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 2500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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

        #region " Enhance Search Function for RFQ for CUSTOMER-OPERATOR "
        public string FetchRfqForOperatorCustomer(string strCond)
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
                SCM.CommandText = objWF.MyUserName + ".EN_RFQ_REF_NO_PKG.GET_OPERATOR_RFQ_COMMON";
                var _with3 = SCM.Parameters;
                _with3.Add("SEARCH_OP_ID_IN", ifDBNull(strSEARCH_OP_ID_IN)).Direction = ParameterDirection.Input;
                _with3.Add("SEARCH_CS_ID_IN", ifDBNull(strSEARCH_CS_ID_IN)).Direction = ParameterDirection.Input;
                _with3.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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