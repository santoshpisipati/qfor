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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Xml.Linq;
using Oracle.DataAccess.Client;

namespace Quantum_QFOR
{
    public class clsSRRCustToOprListing : CommonFeatures
    {
        #region " Fetch All "

        public DataSet FetchAll(string OperatorID = "", string OperatorName = "", string CustomerID = "", string CustomerName = "", string SRRRefNo = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string CargoType = "",
        string FromDate = "", string ToDate = "", int STATUS = 0, bool ActiveOnly = true, string SearchType = "", string SortColumn = " OPERATOR_NAME ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string CurrBizType = "3",
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
            // SRR REF NO
            if (SRRRefNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(SRR_REF_NO) LIKE '" + SrOP + SRRRefNo.ToUpper().Replace("'", "''") + "%'");
            }
            // PORT OF LOADING
            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM SRR_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE SRR_SEA_FK = main.SRR_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM SRR_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE SRR_SEA_FK = main.SRR_SEA_PK ");
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
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM SRR_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE SRR_SEA_FK = main.SRR_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM SRR_TRN_SEA_TBL ");
                buildCondition.Append("           WHERE SRR_SEA_FK = main.SRR_SEA_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            // CARGO TYPE
            if (CargoType.Length > 0)
            {
                buildCondition.Append(" AND CARGO_TYPE = " + CargoType);
            }
            //Modified by Snigdharani - 13/11/2008,12/12/2008
            // VALIDITY
            //Please do not modify the code without consulting QA or Domain
            //If ToDate.Length > 0 And FromDate.Length > 0 Then
            //    buildCondition.Append("AND ((TO_DATE('" & ToDate & "' , '" & dateFormat & "') BETWEEN")
            //    buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR")
            //    buildCondition.Append("    (TO_DATE('" & FromDate & "' , '" & dateFormat & "') BETWEEN")
            //    buildCondition.Append("    MAIN.VALID_FROM AND MAIN.VALID_TO) OR")
            //    buildCondition.Append("    (MAIN.VALID_TO IS NULL))")
            //ElseIf ToDate.Length > 0 And Not FromDate.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND ( ")
            //    buildCondition.Append(vbCrLf & "        main.VALID_TO <= TO_DATE('" & ToDate & "' , '" & dateFormat & "') ")
            //    buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
            //    buildCondition.Append(vbCrLf & "     ) ")
            //ElseIf FromDate.Length > 0 And Not ToDate.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND ( ")
            //    buildCondition.Append(vbCrLf & "        main.VALID_FROM >= TO_DATE('" & FromDate & "' , '" & dateFormat & "') ")
            //    buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
            //    buildCondition.Append(vbCrLf & "     ) ")

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

            //End If
            //If ToDate.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND ( ")
            //    buildCondition.Append(vbCrLf & "        main.VALID_TO <= TO_DATE('" & ToDate & "' , '" & dateFormat & "') ")
            //    buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
            //    buildCondition.Append(vbCrLf & "     ) ")
            //    If ActiveOnly = True Then
            //        buildCondition.Append(vbCrLf & " AND ( ")
            //        buildCondition.Append(vbCrLf & "        main.VALID_TO >= TO_DATE(SYSDATE , '" & dateFormat & "') ")
            //        buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
            //        buildCondition.Append(vbCrLf & "     ) ")
            //    End If
            //Else
            //    If ActiveOnly = True Then
            //        buildCondition.Append(vbCrLf & " AND ( ")
            //        buildCondition.Append(vbCrLf & "        main.VALID_TO >= TO_DATE(SYSDATE , '" & dateFormat & "') ")
            //        buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
            //        buildCondition.Append(vbCrLf & "     ) ")
            //    End If
            //End If
            //If FromDate.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND main.VALID_FROM >= TO_DATE('" & FromDate & "' , '" & dateFormat & "') ")
            //End If
            // STATUS OR NOT STATUS
            if (STATUS != 4)
            {
                buildCondition.Append(" AND main.STATUS = " + STATUS);
            }
            // ACTIVE ONLY
            //Modified by Snigdharani - 13/11/2008,12/12/2008
            if (ActiveOnly == true)
            {
                buildCondition.Append(" and main.active=1 ");
                //buildCondition.Append(vbCrLf & " AND ( ")
                //buildCondition.Append(vbCrLf & "        main.VALID_TO >= TO_DATE(SYSDATE , '" & dateFormat & "') ")
                //buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
                //buildCondition.Append(vbCrLf & "     ) ")
            }
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + "");
            buildCondition.Append(" AND main.CREATED_BY_FK = UMT.USER_MST_PK ");

            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select count(*) ");
            buildQuery.Append("      from ");
            buildQuery.Append("       SRR_SEA_TBL main, ");
            buildQuery.Append("       OPERATOR_MST_TBL, ");
            buildQuery.Append("       CUSTOMER_MST_TBL,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK(+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK ");
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
            buildQuery.Append("       SRR_SEA_PK, ");
            buildQuery.Append("       main.active ACTIVE, ");
            //Snigdharani - 13/11/2008
            buildQuery.Append("       CUSTOMER_NAME, ");
            buildQuery.Append("       SRR_REF_NO, ");
            buildQuery.Append("       OPERATOR_MST_FK, ");
            buildQuery.Append("       OPERATOR_ID, ");
            buildQuery.Append("       OPERATOR_NAME, ");
            buildQuery.Append("       main.CUSTOMER_MST_FK, ");
            buildQuery.Append("       CUSTOMER_ID, ");
            buildQuery.Append("       main.SRR_DATE SRR_DATE, ");
            buildQuery.Append("       decode(CARGO_TYPE, 1, 'FCL',2, 'LCL') CARGO_TYPE, ");
            buildQuery.Append("       0 POL, ");
            buildQuery.Append("       0 POD, ");
            buildQuery.Append("       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            //This line is added by Ashish Arya on 29th Sept 2011 to get the srr with status other than CA, DTS No.: 7672.
            //buildQuery.Append(vbCrLf & "       decode(main.STATUS, 0, 'NA', 1, 'IA') STATUS
            buildQuery.Append("       decode(main.STATUS, 0, 'Requested', 1, 'Approved', 2, 'Rejected') STATUS,main.restricted ");
            //DTS No.: 7672, Line below is commented by Ashish Arya on 29th Sept 2011
            //buildQuery.Append(vbCrLf & "       decode(main.STATUS, 0, 'NA', 1, 'IA',2, 'CA') STATUS ")

            //buildQuery.Append(vbCrLf & "       NVL(main.STATUS, 0) STATUS ")
            buildQuery.Append("      from ");
            buildQuery.Append("       SRR_SEA_TBL main, ");
            buildQuery.Append("       OPERATOR_MST_TBL, ");
            buildQuery.Append("       CUSTOMER_MST_TBL,USER_MST_TBL UMT ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK(+) AND ");
            buildQuery.Append("       main.CUSTOMER_MST_FK = CUSTOMER_MST_PK ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("     Order By " + SortColumn + SortType);
            buildQuery.Append(" ,SRR_REF_NO DESC");
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
                DataRelation SRRRel = new DataRelation("SRRRelation", DS.Tables[0].Columns["SRR_SEA_PK"], DS.Tables[1].Columns["SRR_SEA_FK"], true);
                DS.Relations.Add(SRRRel);
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
                strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["SRR_SEA_PK"]).Trim() + ",");
            }
            strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion " Fetch All "

        #region " Fetch Childs "

        private DataTable FetchChildFor(string SRRSpotPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            // band1_SRNO() = 0 :  band1_TarifMainFK = 1 : band1_PortPOLFK = 2 :  band1_PortPOLID = 3
            // band1_PortPOLName = 4 : band1_PortPODFK = 5 :  band1_PortPODID = 6 :  band1_PortPODName = 7
            // band1_ValidFrom = 8 :  band1_ValidTo = 9

            if (SRRSpotPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and SRR_SEA_FK in (" + SRRSpotPKs + ") ");
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
            buildQuery.Append("       SRR_SEA_FK, ");
            buildQuery.Append("       PORT_MST_POL_FK, ");
            buildQuery.Append("       PORTPOL.PORT_ID PORT_POL_ID, ");
            buildQuery.Append("       PORTPOL.PORT_NAME PORT_POL_NAME, ");
            buildQuery.Append("       PORT_MST_POD_FK, ");
            buildQuery.Append("       PORTPOD.PORT_ID PORT_POD_ID, ");
            buildQuery.Append("       PORTPOD.PORT_NAME PORT_POD_NAME, ");
            buildQuery.Append("       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO ");
            buildQuery.Append("      from ");
            buildQuery.Append("       SRR_TRN_SEA_TBL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOD ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK AND ");
            buildQuery.Append("       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("      Order By SRR_SEA_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
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
                    if (Convert.ToInt32(dt.Rows[RowCnt]["SRR_SEA_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["SRR_SEA_FK"]);
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

        #endregion " Fetch Childs "

        #region " Enhance Search Function for SRR for CUSTOMER-OPERATOR "

        public string FetchSRRForOperatorCustomer(string strCond)
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
                SCM.CommandText = objWF.MyUserName + ".SRR_REF_NO_PKG.GET_OPERATOR_SRR_COMMON";
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

        #endregion " Enhance Search Function for SRR for CUSTOMER-OPERATOR "

        #region " Supporting Function "

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

        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, ""))
            {
                return "";
            }
            return col;
        }

        #endregion " Supporting Function "
    }
}