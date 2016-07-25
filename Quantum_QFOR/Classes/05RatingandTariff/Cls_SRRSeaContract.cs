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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_SRRSeaContract : CommonFeatures
    {
        private long _PkValue;

        #region "Property"

        public long PkValue
        {
            get { return _PkValue; }
        }

        #endregion "Property"

        #region " Enhance Search Function for OPERATOR TARIFF "

        public string FetchOperatorTariff(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            long OprPk = 0;
            Int16 Cargo = default(Int16);
            long CommdityPk = 0;
            string ValidFrom = null;
            string ValidTo = null;
            string LookUp_Value = null;
            string strCondition = null;
            arr = strCond.Split('~');
            LookUp_Value = Convert.ToString(arr.GetValue(0));
            OprPk = Convert.ToInt32(arr.GetValue(1));
            Cargo = Convert.ToInt16(arr.GetValue(2));
            CommdityPk = Convert.ToInt64(arr.GetValue(3));
            ValidFrom = Convert.ToString(arr.GetValue(4));
            ValidTo = Convert.ToString(arr.GetValue(5));
            strCondition = Convert.ToString(arr.GetValue(6));
            MakeConditionString(strCondition);
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_OPERATOR_AIR_TARIFF.GETOPERATORTARIFF_SEA";

                var _with1 = selectCommand.Parameters;
                _with1.Add("OPR_PK_IN", OprPk).Direction = ParameterDirection.Input;
                _with1.Add("CARGO_TYPE_IN", Cargo).Direction = ParameterDirection.Input;
                _with1.Add("COMMODITY_GROUP_PK_IN", CommdityPk).Direction = ParameterDirection.Input;
                _with1.Add("VALID_FROM", ValidFrom).Direction = ParameterDirection.Input;
                _with1.Add("VALID_TO", (ValidTo == "n" ? "" : ValidTo)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", LookUp_Value).Direction = ParameterDirection.Input;
                _with1.Add("CONDITION_IN", strCondition).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                //Throw ex
                return "~" + ex.Message;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        public string FetchCustomerContract(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            long OprPk = 0;
            Int16 Cargo = default(Int16);
            long CommdityPk = 0;
            string ValidFrom = null;
            string ValidTo = null;
            string LookUp_Value = null;
            string strCondition = null;
            arr = strCond.Split('~');
            LookUp_Value = Convert.ToString(arr.GetValue(0));
            OprPk = Convert.ToInt64(arr.GetValue(1));
            Cargo = Convert.ToInt16(arr.GetValue(2));
            CommdityPk = Convert.ToInt64(arr.GetValue(3));
            ValidFrom = Convert.ToString(arr.GetValue(4));
            ValidTo = Convert.ToString(arr.GetValue(5));
            strCondition = Convert.ToString(arr.GetValue(6));
            MakeConditionString(strCondition);
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GET_CUSTOMER_CONTRACT_SEA";

                var _with2 = selectCommand.Parameters;
                _with2.Add("CUST_PK_IN", OprPk).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_TYPE_IN", Cargo).Direction = ParameterDirection.Input;
                _with2.Add("COMMODITY_GROUP_PK_IN", CommdityPk).Direction = ParameterDirection.Input;
                _with2.Add("VALID_FROM", ValidFrom).Direction = ParameterDirection.Input;
                _with2.Add("VALID_TO", (ValidTo == "n" ? "" : ValidTo)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", LookUp_Value).Direction = ParameterDirection.Input;
                _with2.Add("CONDITION_IN", strCondition).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "~" + ex.Message;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search Function for OPERATOR TARIFF "

        #region "Fetch Called by Select Container/Sector"

        //This function returns all the active sectors from the database.
        //If the given POL and POD are present then the value will come as checked.
        public DataTable ActiveSector(long LocationPk, Int16 BizType, string Sectors = "0", string From = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            Array arrPorts = null;
            string strCont = null;
            string strCondition = null;
            strSQL = " ";
            if (From == "QUOTATION")
            {
                if (Sectors != "0")
                {
                    arrPorts = Sectors.Split('^');
                    if (Convert.ToString(arrPorts.GetValue(1)) == "0")
                    {
                        strCondition = " POL.PORT_MST_PK IN (" + arrPorts.GetValue(0) + ")";
                    }
                    else
                    {
                        strCondition = " POL.PORT_MST_PK IN (" + arrPorts.GetValue(0) + ") AND POD.PORT_MST_PK IN (" + arrPorts.GetValue(1) + ")";
                    }

                    strCont = arrPorts.GetValue(2).ToString().TrimEnd('~');

                    strSQL = "SELECT POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POL, " + 
                        "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, '0' CHK, '" + strCont + "' CONTAINERDATA " + 
                        "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  " + 
                        "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " + 
                        "AND POL.BUSINESS_TYPE = " + BizType + 
                        "AND POD.BUSINESS_TYPE = " + BizType + 
                        "AND ( " + strCondition + " ) ";
                }
                else
                {
                    strSQL += "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + 
                        "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, " + 
                        "'0' CHK, '' CONTAINERDATA " + 
                        "FROM SECTOR_MST_TBL SMT, " +
                        "PORT_MST_TBL POL, " + 
                        "PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM " + 
                        "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + 
                        "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + 
                        "AND   LPM.PORT_MST_FK = POL.PORT_MST_PK " + 
                        "AND   POL.BUSINESS_TYPE = " + BizType + 
                        "AND   POD.BUSINESS_TYPE = " + BizType + 
                        "AND   LPM.LOCATION_MST_FK =" + LocationPk + 
                        "AND   SMT.ACTIVE = 1 " + 
                        "ORDER BY CHK DESC,POL,POD";
                }
            }
            else
            {
                if (Sectors != "0")
                {
                    arrPorts = Sectors.Split('^');

                    strCondition = " POL.PORT_MST_PK =" + arrPorts.GetValue(0) + " AND POD.PORT_MST_PK =" + arrPorts.GetValue(1);

                    strCont = arrPorts.GetValue(2).ToString().TrimEnd('~');

                    strSQL = "SELECT POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POL, " + 
                        "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, '1' CHK, '" + strCont + "' CONTAINERDATA " +
                        "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  " + 
                        "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " + 
                        "AND POL.BUSINESS_TYPE = " + BizType +
                        "AND POD.BUSINESS_TYPE = " + BizType + 
                        "AND ( " + strCondition + " ) " +
                        "UNION ";
                }
                //Creating the sql if the user has already selected one port pair in calling form
                //incase of veiwing also then that port pair will come and active port pair in the grid.
                //BUSINESS_TYPE = 2 :- Is the business type for SEA
                //BUSINESS_TYPE = 1 :- Is the business type for AIR
                strSQL += "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + 
                    "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, " +
                    "'0' CHK, '' CONTAINERDATA " + 
                    "FROM SECTOR_MST_TBL SMT, " + 
                    "PORT_MST_TBL POL, " +
                    "PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM " + 
                    "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + 
                    "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + 
                    "AND   LPM.PORT_MST_FK = POL.PORT_MST_PK " + 
                    "AND   POL.BUSINESS_TYPE = " + BizType + 
                    "AND   POD.BUSINESS_TYPE = " + BizType + 
                    "AND   LPM.LOCATION_MST_FK =" + LocationPk + 
                    "AND   SMT.ACTIVE = 1 " + 
                    "ORDER BY CHK DESC,POL,POD";
            }

            try
            {
                return objWF.GetDataTable(strSQL);
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

        public DataTable ActiveSectorPortGroup(long TariffGrpPK, long Group, long LocationPk, Int16 BizType, string Sectors = "0", string From = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            Array arrPorts = null;
            string strCont = null;
            string strCondition = null;
            strSQL = " ";
            if (From == "QUOTATION")
            {
                if (Sectors != "0")
                {
                    arrPorts = Sectors.Split('^');
                    if (Group == 0)
                    {
                        strCondition = " POL.PORT_MST_PK IN (" + arrPorts.GetValue(0) + ") AND POD.PORT_MST_PK IN (" + arrPorts.GetValue(1) + ")";

                        strCont = arrPorts.GetValue(2).ToString().TrimEnd('~');

                        strSQL = "SELECT DISTINCT PGL.PORT_GRP_MST_PK AS POLPK,PGL.PORT_GRP_CODE as POL, " + 
                            "PGD.PORT_GRP_MST_PK AS PODPK, PGD.PORT_GRP_CODE as POD, '0' CHK, '" + strCont + "' CONTAINERDATA " + 
                            "FROM PORT_MST_TBL POL, PORT_MST_TBL POD, PORT_GROUP_MST_TBL PGL, PORT_GROUP_MST_TBL PGD  " + 
                            "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " +
                            "AND PGL.PORT_GRP_MST_PK = POL.PORT_GRP_MST_FK " + 
                            "AND PGD.PORT_GRP_MST_PK = POD.PORT_GRP_MST_FK " +
                            "AND POL.BUSINESS_TYPE = " + BizType + 
                            "AND POD.BUSINESS_TYPE = " + BizType + 
                            "AND ( " + strCondition + " ) ";

                        //'Port Group
                    }
                    else if (Group == 1)
                    {
                        strCont = arrPorts.GetValue(2).ToString().TrimEnd('~');

                        strSQL = "SELECT DISTINCT POLGP.PORT_GRP_MST_PK AS POLPK,POLGP.PORT_GRP_ID AS POL, " + 
                            " PODGP.PORT_GRP_MST_PK AS PODPK,PODGP.PORT_GRP_ID AS POD, '0' CHK, '" + strCont + "' CONTAINERDATA " + 
                            " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP" + 
                            " WHERE POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK  " + 
                            " AND POLGP.PORT_GRP_MST_PK IN (" + arrPorts.GetValue(0) + ") " + 
                            " AND PODGP.PORT_GRP_MST_PK IN (" + arrPorts.GetValue(1) + ")";

                        //'Tariff Group
                    }
                    else if (Group == 2)
                    {
                        strCont = arrPorts.GetValue(2).ToString().TrimEnd('~');

                        strSQL = "SELECT DISTINCT POLGP.PORT_GRP_MST_PK AS POLPK,POLGP.PORT_GRP_ID AS POL, " + 
                            " PODGP.PORT_GRP_MST_PK AS PODPK,PODGP.PORT_GRP_ID AS POD, '0' CHK, '" + strCont + "' CONTAINERDATA " + 
                            " FROM PORT_GRP_MST_TBL POLGP, PORT_GRP_MST_TBL PODGP" + 
                            " WHERE POLGP.PORT_GRP_MST_PK <> PODGP.PORT_GRP_MST_PK  " + 
                            " AND POLGP.PORT_GRP_MST_PK IN (" + arrPorts.GetValue(0) + ") " +
                            " AND PODGP.PORT_GRP_MST_PK IN (" + arrPorts.GetValue(1) + ")";
                    }
                }
                else
                {
                    strSQL += "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + 
                        "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, " + 
                        "'0' CHK, '' CONTAINERDATA " + 
                        "FROM SECTOR_MST_TBL SMT, " + 
                        "PORT_MST_TBL POL, " + 
                        "PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM " + 
                        "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + 
                        "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + 
                        "AND   LPM.PORT_MST_FK = POL.PORT_MST_PK " + 
                        "AND   POL.BUSINESS_TYPE = " + BizType + 
                        "AND   POD.BUSINESS_TYPE = " + BizType + 
                        "AND   LPM.LOCATION_MST_FK =" + LocationPk + 
                        "AND   SMT.ACTIVE = 1 " + 
                        "ORDER BY CHK DESC,POL,POD";
                }
            }
            else
            {
                if (Sectors != "0")
                {
                    arrPorts = Sectors.Split('^');

                    strCondition = " POL.PORT_MST_PK =" + arrPorts.GetValue(0) + " AND POD.PORT_MST_PK =" + arrPorts.GetValue(1);

                    strCont = arrPorts.GetValue(2).ToString().TrimEnd('~');

                    strSQL = "SELECT POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POL, " +
                        "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, '1' CHK, '" + strCont + "' CONTAINERDATA " + 
                        "FROM PORT_MST_TBL POL, PORT_MST_TBL POD  " + 
                        "WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  " + 
                        "AND POL.BUSINESS_TYPE = " + BizType + 
                        "AND POD.BUSINESS_TYPE = " + BizType +
                        "AND ( " + strCondition + " ) " +
                        "UNION ";
                }
                //Creating the sql if the user has already selected one port pair in calling form
                //incase of veiwing also then that port pair will come and active port pair in the grid.
                //BUSINESS_TYPE = 2 :- Is the business type for SEA
                //BUSINESS_TYPE = 1 :- Is the business type for AIR
                strSQL += "SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, " + 
                    "POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, " + 
                    "'0' CHK, '' CONTAINERDATA " + 
                    "FROM SECTOR_MST_TBL SMT, " + 
                    "PORT_MST_TBL POL, " + 
                    "PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM " + 
                    "WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK " + 
                    "AND   SMT.TO_PORT_FK = POD.PORT_MST_PK " + 
                    "AND   LPM.PORT_MST_FK = POL.PORT_MST_PK " + 
                    "AND   POL.BUSINESS_TYPE = " + BizType + 
                    "AND   POD.BUSINESS_TYPE = " + BizType + 
                    "AND   LPM.LOCATION_MST_FK =" + LocationPk + 
                    "AND   SMT.ACTIVE = 1 " +
                    "ORDER BY CHK DESC,POL,POD";
            }

            try
            {
                return objWF.GetDataTable(strSQL);
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

        //This function returns all the active containers.
        public DataTable ActiveContainers(string StrContainer = "")
        {
            string strSQL = null;
            string strCondition = null;
            Array arrContainer = null;
            WorkFlow objWF = new WorkFlow();
            Int16 i = default(Int16);
            //'
            arrContainer = StrContainer.Split(',');
            if (string.IsNullOrEmpty(arrContainer.GetValue(0).ToString()) | arrContainer.GetValue(0).ToString() == "n")
            {
                arrContainer.SetValue("0",0);
            }
            for (i = 0; i <= arrContainer.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    if (arrContainer.GetValue(i) == "0")
                    {
                        strCondition = " ( CMT.CONTAINER_TYPE_MST_PK IN (" + arrContainer.GetValue(i) + "))";
                    }
                    else
                    {
                        strCondition = " ( CMT.CONTAINER_TYPE_MST_PK IN (" + arrContainer.GetValue(i) + "))";
                    }
                }
                else
                {
                    strCondition += " OR ( CMT.CONTAINER_TYPE_MST_PK =" + arrContainer.GetValue(i) + ")";
                }
            }
            //'
            strSQL = "";
            strSQL = "SELECT CONTAINER_TYPE_MST_PK,CONTAINER_TYPE_MST_ID,CHK FROM( SELECT " + 
                " CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + 
                " '1' CHK,PREFERENCES " + 
                " FROM CONTAINER_TYPE_MST_TBL CMT " + 
                " WHERE CMT.ACTIVE_FLAG = 1  " +
                " AND ( " + strCondition + " ) " + 
                "  UNION " + 
                " SELECT " + 
                " CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + 
                "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK,PREFERENCES " +
                " FROM CONTAINER_TYPE_MST_TBL CMT " + 
                " WHERE CMT.ACTIVE_FLAG=1  " +
                " ORDER BY CHK DESC) ORDER BY PREFERENCES";
            try
            {
                return objWF.GetDataTable(strSQL);
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

        //This function returns all the active containers.
        public DataTable AcitveDimentions()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "";
            strSQL = "SELECT " + 
                "  UOM.DIMENTION_UNIT_MST_PK, " + 
                "  UOM.DIMENTION_ID, " + 
                "  '0' CHK " +
                "FROM " + 
                "  DIMENTION_UNIT_MST_TBL UOM " + 
                "WHERE " + 
                "  UOM.ACTIVE = 1 " + 
                "ORDER BY " + 
                "  UOM.DIMENTION_ID ";
            try
            {
                return objWF.GetDataTable(strSQL);
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

        #endregion "Fetch Called by Select Container/Sector"

        #region "Fetch Clause"

        public DataSet FetchClause(int intCustContPk)
        {
            StringBuilder str = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataSet dsClause = new DataSet();
            try
            {
                str.Append(" select cust.cont_clause, cust.credit_period, ");
                str.Append(" loc.location_mst_pk,loc.location_id,loc.location_name, cust.commodity_mst_fk, c.commodity_id, c.commodity_name, c.commodity_group_fk ");
                str.Append(" from cont_cust_sea_tbl cust, ");
                str.Append(" location_mst_tbl Loc, commodity_mst_tbl c  ");
                str.Append(" where loc.location_mst_pk(+) = cust.pymt_location_mst_fk ");
                str.Append(" and c.commodity_mst_pk(+)=cust.commodity_mst_fk ");
                str.Append(" and cust.cont_cust_sea_pk = " + intCustContPk + " ");
                dsClause = objWF.GetDataSet(str.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dsClause;
        }

        #endregion "Fetch Clause"

        #region "Fetch Queries"

        //This procedure is called when FCL cargo type is selected.
        public void FetchOperatorTariffFCL(long TariffPk, string strSearch, DataSet dsMain, string Valid_From, string Valid_To)
        {
            string strSQL = null;
            Int16 nBOF = default(Int16);
            Int32 nMain = default(Int32);
            DataTable dtTableBOF = new DataTable("BOF");
            DataColumn dcColumn = null;
            DataTable dtTableAllIn = new DataTable("All_In");
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (strSearch.Trim().Length <= 0)
                {
                    strSearch = "(0,0,0)";
                }
                else
                {
                    //Making the condition string which will be used in query while selecting POL,POD & Contianer Type
                    MakeConditionString(strSearch);
                }

                //Fetching AllIn rate for the selected POL, POD & Container Type
                strSQL = "";
                strSQL = "  SELECT SUM(ALLIN) AS All_In ,PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS  " + 
                    "  FROM ( " + 
                    "  SELECT ( " + 
                    "  SELECT Allin * EXCHANGE_RATE FROM v_exchange_rate EX " + 
                    "  WHERE EX.CURRENCY_MST_FK = CURRENCY_MST_PK AND EX.EXCH_RATE_TYPE_FK = 1 " + 
                    "  AND VALID_FROM BETWEEN EX.FROM_DATE AND NVL(EX.TO_DATE,SYSDATE) " + 
                    "  ) AS ALLIN,CURRENCY_MST_PK,PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS " + 
                    "  FROM    " +
                    "  ( " + 
                    "  SELECT SUM(CONT.FCL_REQ_RATE) AS Allin,CURR.CURRENCY_MST_PK, " +
                    "  T.VALID_FROM,T.PORT_MST_POL_FK,T.PORT_MST_POD_FK, " + 
                    "  CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS " +
                    "  FROM TARIFF_TRN_SEA_FCL_LCL T, " +
                    "       TARIFF_MAIN_SEA_TBL TM,  ";
                strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + 
                    "  PORT_MST_TBL POL, " + 
                    "  PORT_MST_TBL POD, " + 
                    "  CURRENCY_TYPE_MST_TBL CURR, " + 
                    "  CONTAINER_TYPE_MST_TBL CTMT, " + 
                    "  FREIGHT_ELEMENT_MST_TBL FRT, " +
                    "  CORPORATE_MST_TBL CORP " + 
                    "  WHERE T.TARIFF_MAIN_SEA_FK= " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " +
                    "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK  " + 
                    "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + 
                    "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + 
                    "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + 
                    "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + 
                    "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + 
                    "  AND T.CHECK_FOR_ALL_IN_RT =1 " + 
                    "  AND TM.STATUS =1 " + 
                    "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + 
                    "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + 
                    "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + 
                    "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + 
                    "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) " + 
                    "  IN (" + strSearch + ") " + 
                    "  GROUP BY CURR.CURRENCY_ID,CURR.CURRENCY_MST_PK,T.VALID_FROM, " + 
                    "  T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CTMT.CONTAINER_TYPE_MST_ID" + 
                    "  )Q,CORPORATE_MST_TBL CORP " +
                    "  ) A WHERE A.Allin > 0  GROUP BY PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS ";
                dtTableAllIn = objWF.GetDataTable(strSQL);

                //Fetching BOF for selected POL,POD & Container Type
                strSQL = "";
                strSQL = "  SELECT " + "  ( SELECT CONT.FCL_REQ_RATE * EXCHANGE_RATE FROM V_EXCHANGE_RATE EX " + "  WHERE(ex.CURRENCY_MST_FK = T.CURRENCY_MST_FK) AND EX.EXCH_RATE_TYPE_FK = 1  " + "  AND T.VALID_FROM BETWEEN EX.FROM_DATE AND NVL(EX.TO_DATE,SYSDATE) " + "  ) AS  BOF,CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS, " + "  T.PORT_MST_POL_FK,T.PORT_MST_POD_FK " + "  FROM TARIFF_TRN_SEA_FCL_LCL T, " + "       TARIFF_MAIN_SEA_TBL TM,  ";
                strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  PORT_MST_TBL POL, " + "  PORT_MST_TBL POD, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  FREIGHT_ELEMENT_MST_TBL FRT, " + "  CORPORATE_MST_TBL CORP " + "  WHERE T.TARIFF_MAIN_SEA_FK=" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK  " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND FRT.FREIGHT_ELEMENT_ID ='BOF' " + "  AND CONT.FCL_REQ_RATE > 0 " + "  AND TM.STATUS =1 " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) IN  " + "  (" + strSearch + ")";

                dtTableBOF = objWF.GetDataTable(strSQL);

                //This query gives dataset having other required data.
                strSQL = "";
                strSQL = "  SELECT DISTINCT POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POL," + "  POD.PORT_MST_PK AS PODPK,POD.PORT_ID AS POD, " + "  CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS,CURR.CURRENCY_ID, " + "  0.00 BOF, 0.00 AS ALL_IN," + "  0.00 REQ_BOF, 0.00 REQ_ALLIN, 0 THL, 0 THD, 0 VOLUME, " + "  '" + Valid_From + "' AS FROMDATE, " + "  '" + Valid_To + "' AS TODATE,CONT.CONTAINER_TYPE_MST_FK,CURR.CURRENCY_MST_PK " + "  FROM TARIFF_TRN_SEA_FCL_LCL T, " + "       TARIFF_MAIN_SEA_TBL TM,  ";
                strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  PORT_MST_TBL POL, " + "  PORT_MST_TBL POD, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  CORPORATE_MST_TBL CORP " + "  WHERE T.TARIFF_MAIN_SEA_FK=" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK  " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND CURR.CURRENCY_MST_PK = CORP.CURRENCY_MST_FK " + "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND TM.STATUS =1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) " + "  IN (" + strSearch + ")";

                dsMain.Tables.Add(objWF.GetDataTable(strSQL));
                dsMain.Tables[0].TableName = "Main";
                dcColumn = new DataColumn("Surcharge", typeof(Int16));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                dcColumn = new DataColumn("Del", typeof(Int16));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                dcColumn = new DataColumn("Srr_Trn_Pk", typeof(Int64));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                //Adding BOF & All In Rate of particular POL,POD and Container Type

                for (nMain = 0; nMain <= dsMain.Tables["Main"].Rows.Count - 1; nMain++)
                {
                    //Adding BOF of particular POL,POD and Container Type
                    for (nBOF = 0; nBOF <= dtTableBOF.Rows.Count - 1; nBOF++)
                    {
                        //If  POL,POD and Container Type is same then add BOF

                        if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableBOF.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POD_FK"])
                        {
                            dsMain.Tables["Main"].Rows[nMain]["BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }

                    //Adding All In RaTe of particular POL,POD and Container Type
                    for (nBOF = 0; nBOF <= dtTableAllIn.Rows.Count - 1; nBOF++)
                    {
                        //If  POL,POD and Container Type is same then add All In Rate

                        if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableAllIn.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POD_FK"])
                        {
                            dsMain.Tables["Main"].Rows[nMain]["ALL_IN"] = dtTableAllIn.Rows[nBOF]["All_In"];
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                }

                //This query fetches all the freight element saved in Operator Tariff
                //for the given POL,POD & Container Type
                strSQL = "";
                strSQL = "  SELECT " + "  T.PORT_MST_POL_FK AS POL, T.PORT_MST_POD_FK AS POD, " + "  FRT.FREIGHT_ELEMENT_ID,TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK, " + "  CURR.CURRENCY_ID,CTMT.CONTAINER_TYPE_MST_ID AS CONT_BASIS , " + "  NVL(CONT.FCL_REQ_RATE,0.00) AS TARIFF_RATE,NVL(CONT.FCL_REQ_RATE,0.00) AS REQUESTED_RATE," + "  CURR.CURRENCY_MST_PK,FRT.FREIGHT_ELEMENT_MST_PK " + "  FROM TARIFF_TRN_SEA_FCL_LCL T, " + "       TARIFF_MAIN_SEA_TBL TM,  ";
                strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  FREIGHT_ELEMENT_MST_TBL FRT " + "  WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND FRT.FREIGHT_ELEMENT_ID <> 'BOF' " + "  AND CONT.FCL_REQ_RATE > 0 " + "  AND TM.STATUS =1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) " + "  IN (" + strSearch + ")";

                dsMain.Tables.Add(objWF.GetDataTable(strSQL));
                dsMain.Tables[1].TableName = "Frt";

                dcColumn = new DataColumn("SRR_SUR_CHRG_SEA_PK", typeof(Int64));
                dsMain.Tables["Frt"].Columns.Add(dcColumn);

                //Making relation  between Main and Frt table of dsMain
                //Relation between:
                //                 Main Table            Frt Table
                //                 ---------             ---------
                //                 1. POLPK              1. POL
                //                 2. PODPK              2. POD
                //                 3. CONTAINER TYPE     3. CONTAINER TYPE

                DataColumn[] dcParent = null;
                DataColumn[] dcChild = null;
                DataRelation re = null;
                dcParent = new DataColumn[] {
                dsMain.Tables["Main"].Columns["POLPK"],
                dsMain.Tables["Main"].Columns["PODPK"],
                dsMain.Tables["Main"].Columns["CONT_BASIS"]
            };
                dcChild = new DataColumn[] {
                dsMain.Tables["Frt"].Columns["POL"],
                dsMain.Tables["Frt"].Columns["POD"],
                dsMain.Tables["Frt"].Columns["CONT_BASIS"]
            };
                re = new DataRelation("rl_Port", dcParent, dcChild);

                //Adding relation to the grid.
                dsMain.Relations.Add(re);
            }
            catch (OracleException oraExp)
            {
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //This procedure is called when LCL cargo type is selected.
        public void FetchOperatorTariffLCL(long TariffPk, string strSearch, DataSet dsMain, string Valid_From, string Valid_To)
        {
            string strSQL = null;
            Int16 nBOF = default(Int16);
            Int32 nMain = default(Int32);
            DataTable dtTableBOF = new DataTable("BOF");
            DataColumn dcColumn = null;
            DataTable dtTableAllIn = new DataTable("All_In");
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (strSearch.Trim().Length <= 0)
                {
                    strSearch = "(0,0,0)";
                }
                else
                {
                    //Making the condition string which will be used in query while selecting POL,POD & Contianer Type
                    MakeConditionString(strSearch);
                }

                //Fetching AllIn rate for the selected POL, POD & UOM
                strSQL = "";
                strSQL = " SELECT SUM(ALLIN) AS ALL_IN, PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS " + " FROM ( " + "  SELECT ( " + "         SELECT Allin * EXCHANGE_RATE FROM V_EXCHANGE_RATE EX " + "         WHERE EX.CURRENCY_MST_FK = CURRENCY_MST_PK AND EX.EXCH_RATE_TYPE_FK = 1 " + "         AND VALID_FROM BETWEEN EX.FROM_DATE AND NVL(EX.TO_DATE,SYSDATE) " + "         ) AS ALLIN, " + "              CURRENCY_MST_PK, " + "              PORT_MST_POL_FK, " + "              PORT_MST_POD_FK, " + "              CONT_BASIS " + "         FROM (SELECT SUM(T.LCL_TARIFF_RATE) AS ALLIN, " + "                      CURR.CURRENCY_MST_PK, " + "                      T.VALID_FROM, " + "                      T.PORT_MST_POL_FK, " + "                      T.PORT_MST_POD_FK, " + "                      UOM.DIMENTION_ID CONT_BASIS " + "                 FROM TARIFF_TRN_SEA_FCL_LCL T, " + "                      TARIFF_MAIN_SEA_TBL TM, " + "                      DIMENTION_UNIT_MST_TBL UOM, " + "                      PORT_MST_TBL POL, " + "                      PORT_MST_TBL POD, " + "                      CURRENCY_TYPE_MST_TBL CURR, " + "                      FREIGHT_ELEMENT_MST_TBL FRT, " + "                      CORPORATE_MST_TBL CORP " + "                WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "                  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "                  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "                  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "                  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "                  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "                  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "                  AND T.CHECK_FOR_ALL_IN_RT = 1 " + "                  AND TM.STATUS =1 " + "                  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "                       OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "                  AND (T.PORT_MST_POL_FK, T.PORT_MST_POD_FK,T.LCL_BASIS) IN " + "                      (" + strSearch + ") " + "                GROUP BY CURR.CURRENCY_ID, " + "                         CURR.CURRENCY_MST_PK, " + "                         T.VALID_FROM, " + "                         T.PORT_MST_POL_FK, " + "                         T.PORT_MST_POD_FK, " + "                         UOM.DIMENTION_ID ) Q, " + "              CORPORATE_MST_TBL CORP) A " + " WHERE(A.ALLIN > 0) " + " GROUP BY PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS";

                dtTableAllIn = objWF.GetDataTable(strSQL);

                //Fetching BOF for selected POL,POD & UOM
                strSQL = "";
                strSQL = "  SELECT " + "     ( SELECT T.LCL_TARIFF_RATE * EXCHANGE_RATE FROM V_EXCHANGE_RATE EX " + "     WHERE(ex.CURRENCY_MST_FK = T.CURRENCY_MST_FK) AND EX.EXCH_RATE_TYPE_FK = 1 " + "     AND T.VALID_FROM BETWEEN EX.FROM_DATE AND NVL(EX.TO_DATE,SYSDATE) " + "     ) AS BOF, " + "     UOM.DIMENTION_ID CONT_BASIS, " + "     T.PORT_MST_POL_FK, " + "     T.PORT_MST_POD_FK " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     FREIGHT_ELEMENT_MST_TBL FRT, " + "     CORPORATE_MST_TBL CORP " + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND FRT.FREIGHT_ELEMENT_ID = 'BOF' " + "  AND T.LCL_BASIS>0 " + "  AND TM.STATUS =1 " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,T.LCL_BASIS) IN " + "      (" + strSearch + ") ";

                dtTableBOF = objWF.GetDataTable(strSQL);

                //This query gives dataset having other required data.
                strSQL = "";
                strSQL = "SELECT DISTINCT " + "         POL.PORT_MST_PK AS POLPK, " + "         POL.PORT_ID AS POL, " + "         POD.PORT_MST_PK AS PODPK, " + "         POD.PORT_ID AS POD, " + "         UOM.DIMENTION_ID CONT_BASIS, " + "         CURR.CURRENCY_ID, " + "         0.00 BOF, " + "         0.00 AS ALL_IN, " + "         0.00 REQ_BOF, " + "         0.00 REQ_ALLIN, " + "         0 THL, " + "         0 THD, " + "         0 VOLUME, " + "         '" + Valid_From + "' AS FROMDATE, " + "         '" + Valid_To + "' AS TODATE, " + "         T.LCL_BASIS, " + "         CURR.CURRENCY_MST_PK " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     CORPORATE_MST_TBL CORP " + "     WHERE T.TARIFF_MAIN_SEA_FK =" + TariffPk + " AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + " AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + " AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + " AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + " AND CURR.CURRENCY_MST_PK = CORP.CURRENCY_MST_FK " + " AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + " AND TM.STATUS = 1 " + " AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "     OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + " AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,T.LCL_BASIS) IN " + "     (" + strSearch + ") ";

                dsMain.Tables.Add(objWF.GetDataTable(strSQL));
                dsMain.Tables[0].TableName = "Main";

                //Adding Surcharge,Delete and Srr_Trn_Pk column
                dcColumn = new DataColumn("Surcharge", typeof(Int16));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                dcColumn = new DataColumn("Del", typeof(Int16));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                dcColumn = new DataColumn("Srr_Trn_Pk", typeof(Int64));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                //Adding BOF & All In Rate of particular POL,POD and Container Type

                for (nMain = 0; nMain <= dsMain.Tables["Main"].Rows.Count - 1; nMain++)
                {
                    //Adding BOF of particular POL,POD and Container Type
                    for (nBOF = 0; nBOF <= dtTableBOF.Rows.Count - 1; nBOF++)
                    {
                        //If  POL,POD and Container Type is same then add BOF

                        if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableBOF.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POD_FK"])
                        {
                            dsMain.Tables["Main"].Rows[nMain]["BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }

                    //Adding All In RaTe of particular POL,POD and UOM
                    for (nBOF = 0; nBOF <= dtTableAllIn.Rows.Count - 1; nBOF++)
                    {
                        //If  POL,POD and Container Type is same then add All In Rate

                        if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableAllIn.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POD_FK"])
                        {
                            dsMain.Tables["Main"].Rows[nMain]["ALL_IN"] = dtTableAllIn.Rows[nBOF]["All_In"];
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                }

                //This query fetches all the freight element saved in Operator Tariff
                //for the given POL,POD & UOM
                strSQL = "";
                strSQL = " SELECT T.PORT_MST_POL_FK AS POL, " + "      T.PORT_MST_POD_FK AS POD, " + "      FRT.FREIGHT_ELEMENT_ID, " + "      TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK, " + "      CURR.CURRENCY_ID, " + "      UOM.DIMENTION_ID AS CONT_BASIS, " + "      NVL(T.LCL_TARIFF_RATE,0.00) AS TARIFF_RATE, " + "      NVL(T.LCL_TARIFF_RATE,0.00) AS REQUESTED_RATE, " + "      CURR.CURRENCY_MST_PK, " + "      FRT.FREIGHT_ELEMENT_MST_PK " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "      TARIFF_MAIN_SEA_TBL TM, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND FRT.FREIGHT_ELEMENT_ID <> 'BOF' " + "  AND T.LCL_BASIS > 0 " + "  AND TM.STATUS =1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK, T.PORT_MST_POD_FK,T.LCL_BASIS ) IN " + "      (" + strSearch + ") ";

                dsMain.Tables.Add(objWF.GetDataTable(strSQL));
                dsMain.Tables[1].TableName = "Frt";

                dcColumn = new DataColumn("SRR_SUR_CHRG_SEA_PK", typeof(Int64));
                dsMain.Tables["Frt"].Columns.Add(dcColumn);

                //Making relation  between Main and Frt table of dsMain
                //Relation between:
                //                 Main Table            Frt Table
                //                 ---------             ---------
                //                 1. POLPK              1. POL
                //                 2. PODPK              2. POD
                //                 3. CONTAINER TYPE     3. CONTAINER TYPE

                DataColumn[] dcParent = null;
                DataColumn[] dcChild = null;
                DataRelation re = null;
                dcParent = new DataColumn[] {
                dsMain.Tables["Main"].Columns["POLPK"],
                dsMain.Tables["Main"].Columns["PODPK"],
                dsMain.Tables["Main"].Columns["CONT_BASIS"]
            };
                dcChild = new DataColumn[] {
                dsMain.Tables["Frt"].Columns["POL"],
                dsMain.Tables["Frt"].Columns["POD"],
                dsMain.Tables["Frt"].Columns["CONT_BASIS"]
            };
                re = new DataRelation("rl_Port", dcParent, dcChild);

                //Adding relation to the grid.
                dsMain.Relations.Add(re);
            }
            catch (OracleException oraExp)
            {
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //FOR FETCHING CONTRACT
        public void Fetch_CustomerContract(long SrrPk, string strSearch, DataSet dsGrid, bool IsLCL, string strFromDt, string strToDt, int oGroup)
        {
            string strSql = null;
            WorkFlow objWF = new WorkFlow();

            try
            {
                if (strSearch.Trim().Length <= 0)
                {
                    strSearch = "0";
                }
                else
                {
                    //Making the condition string which will be used in query while selecting Contianer Type
                    MakeConditionContString(strSearch);
                }

                strSql = "";
                if (!IsLCL)
                {
                    if (oGroup == 0)
                    {
                        strSql = "SELECT POL.PORT_MST_PK AS POLPK, " + "POL.PORT_ID AS POL, " + "POD.PORT_MST_PK AS PODPK, " + "POD.PORT_ID AS POD, " + "CONT.CONTAINER_TYPE_MST_ID CONT_BASIS, " + "CURR.CURRENCY_ID, " + "CON.APPROVED_BOF_RATE BOF, " + "CON.CURRENT_ALL_IN_RATE ALL_IN, " + "CON.APPROVED_BOF_RATE AS REQ_BOF, " + "CON.REQUESTED_ALL_IN_RATE  REQ_ALLIN, " + "(CASE WHEN (CON.ON_THL_OR_THD IN (1, 3)) THEN  '1' ELSE '0' END) AS THL, " + "(CASE WHEN (CON.ON_THL_OR_THD IN (2, 3)) THEN  '1' ELSE '0' END) AS THD, " + " CON.EXPECTED_BOXES EXPECTED_BOXES, " + " '" + strFromDt + "' FROMDATE, " + " '" + strToDt + "' TODATE, " + "CON.CONTAINER_TYPE_MST_FK, " + "CON.CURRENCY_MST_FK, " + "(CASE WHEN CON.SUBJECT_TO_SURCHG_CHG = 1 THEN '1'  ELSE'0'END) AS SURCHARGE, " + "'0' DEL, " + "'0' SRR_TRN_PK  " + "FROM CONT_CUST_TRN_SEA_TBL    CON, " + "    PORT_MST_TBL           POL , " + "   PORT_MST_TBL           POD, " + "  CONTAINER_TYPE_MST_TBL CONT, " + "              CURRENCY_TYPE_MST_TBL CURR " + "             WHERE CON.PORT_MST_POL_FK = POL.PORT_MST_PK " + "  AND CON.PORT_MST_POD_FK = POD.PORT_MST_PK " + " AND CON.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK " + "AND CON.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + " AND CONT.CONTAINER_TYPE_MST_PK IN (" + strSearch + ") " + "AND CON.CONT_CUST_SEA_FK = " + SrrPk;
                    }
                    else
                    {
                        strSql = "SELECT DISTINCT * FROM (SELECT POLGRP.PORT_GRP_MST_PK AS POLPK, " + "POLGRP.PORT_GRP_ID AS POL, " + "PODGRP.PORT_GRP_MST_PK AS PODPK, " + "PODGRP.PORT_GRP_ID AS POD, " + "CONT.CONTAINER_TYPE_MST_ID CONT_BASIS, " + "CURR.CURRENCY_ID, " + "CON.APPROVED_BOF_RATE BOF, " + "CON.CURRENT_ALL_IN_RATE ALL_IN, " + "CON.APPROVED_BOF_RATE AS REQ_BOF, " + "CON.REQUESTED_ALL_IN_RATE  REQ_ALLIN, " + "(CASE WHEN (CON.ON_THL_OR_THD IN (1, 3)) THEN  '1' ELSE '0' END) AS THL, " + "(CASE WHEN (CON.ON_THL_OR_THD IN (2, 3)) THEN  '1' ELSE '0' END) AS THD, " + " CON.EXPECTED_BOXES EXPECTED_BOXES, " + " '" + strFromDt + "' FROMDATE, " + " '" + strToDt + "' TODATE, " + "CON.CONTAINER_TYPE_MST_FK, " + "CON.CURRENCY_MST_FK, " + "(CASE WHEN CON.SUBJECT_TO_SURCHG_CHG = 1 THEN '1'  ELSE'0'END) AS SURCHARGE, " + "'0' DEL, " + "'0' SRR_TRN_PK  " + "FROM CONT_CUST_TRN_SEA_TBL    CON, " + "    PORT_MST_TBL           POL , " + "   PORT_MST_TBL           POD, " + "  CONTAINER_TYPE_MST_TBL CONT, " + "              CURRENCY_TYPE_MST_TBL CURR, PORT_GRP_MST_TBL       POLGRP, PORT_GRP_MST_TBL       PODGRP " + "             WHERE CON.PORT_MST_POL_FK = POL.PORT_MST_PK " + "  AND CON.PORT_MST_POD_FK = POD.PORT_MST_PK AND CON.POL_GRP_FK = POLGRP.PORT_GRP_MST_PK AND CON.POD_GRP_FK = PODGRP.PORT_GRP_MST_PK " + " AND CON.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK " + "AND CON.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "AND CON.CONT_CUST_SEA_FK = " + SrrPk + ")";
                    }
                }
                else
                {
                    if (oGroup == 0)
                    {
                        strSql = " SELECT " + "      POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POL, " + "      POD.PORT_MST_PK AS PODPK,POD.PORT_ID AS POD, " + "      UOM.DIMENTION_ID CONT_BASIS, " + "      CURR.CURRENCY_ID, SRR.APPROVED_BOF_RATE BOF, " + "      SRR.APPROVED_ALL_IN_RATE ALL_IN, SRR.APPROVED_BOF_RATE AS REQ_BOF, " + "      SRR.REQUESTED_ALL_IN_RATE  REQ_ALLIN, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (1,3)) THEN '1' ELSE '0' END) AS THL, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (2,3)) THEN '1' ELSE '0' END) AS THD, " + "      SRR.EXPECTED_VOLUME VOLUME, '" + strFromDt + "' FROMDATE, " + "      '" + strToDt + "' TODATE, SRR.LCL_BASIS, " + "     CURR.CURRENCY_MST_PK, " + "      (CASE WHEN SRR.SUBJECT_TO_SURCHG_CHG =1 THEN '1' ELSE '0' END) AS SURCHARGE, " + "      '0' DEL,'0' SRR_TRN_PK" + "FROM " + "      CONT_CUST_TRN_SEA_TBL SRR, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR " + "WHERE SRR.PORT_MST_POL_FK       = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK       = POD.PORT_MST_PK " + "AND   SRR.LCL_BASIS             = UOM.DIMENTION_UNIT_MST_PK " + "AND   SRR.CURRENCY_MST_FK       = CURR.CURRENCY_MST_PK " + "AND   SRR.CONT_CUST_SEA_FK            = " + SrrPk;
                    }
                    else
                    {
                        strSql = "SELECT DISTINCT * FROM (SELECT " + "      POLGRP.PORT_GRP_MST_PK AS POLPK,POLGRP.PORT_GRP_ID AS POL, " + "      PODGRP.PORT_GRP_MST_PK AS PODPK,PODGRP.PORT_GRP_ID AS POD, " + "      UOM.DIMENTION_ID CONT_BASIS, " + "      CURR.CURRENCY_ID, SRR.APPROVED_BOF_RATE BOF, " + "      SRR.APPROVED_ALL_IN_RATE ALL_IN, SRR.APPROVED_BOF_RATE AS REQ_BOF, " + "      SRR.REQUESTED_ALL_IN_RATE  REQ_ALLIN, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (1,3)) THEN '1' ELSE '0' END) AS THL, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (2,3)) THEN '1' ELSE '0' END) AS THD, " + "      SRR.EXPECTED_VOLUME VOLUME, '" + strFromDt + "' FROMDATE, " + "      '" + strToDt + "' TODATE, SRR.LCL_BASIS, " + "     CURR.CURRENCY_MST_PK, " + "      (CASE WHEN SRR.SUBJECT_TO_SURCHG_CHG =1 THEN '1' ELSE '0' END) AS SURCHARGE, " + "      '0' DEL,'0' SRR_TRN_PK" + "FROM " + "      CONT_CUST_TRN_SEA_TBL SRR, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR, PORT_GRP_MST_TBL       POLGRP, PORT_GRP_MST_TBL       PODGRP " + "WHERE SRR.PORT_MST_POL_FK       = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK       = POD.PORT_MST_PK AND SRR.POL_GRP_FK = POLGRP.PORT_GRP_MST_PK AND SRR.POD_GRP_FK = PODGRP.PORT_GRP_MST_PK " + "AND   SRR.LCL_BASIS             = UOM.DIMENTION_UNIT_MST_PK " + "AND   SRR.CURRENCY_MST_FK       = CURR.CURRENCY_MST_PK " + "AND   SRR.CONT_CUST_SEA_FK            = " + SrrPk + ")";
                    }
                }
                dsGrid.Tables.Add(objWF.GetDataTable(strSql));
                dsGrid.Tables[0].TableName = "Main";
                strSql = "";
                if (!IsLCL)
                {
                    if (oGroup == 0)
                    {
                        strSql = "SELECT" + "POL.PORT_MST_PK AS POLPK, POD.PORT_MST_PK AS PODPK, " + "FRT.FREIGHT_ELEMENT_ID, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "CURR.CURRENCY_ID, CONT.CONTAINER_TYPE_MST_ID AS CONT_BASIS, " + "sur.app_surcharge_amt, SUR.app_surcharge_amt, SUR.CURRENCY_MST_FK," + "SUR.FREIGHT_ELEMENT_MST_FK, SUR.CONT_SUR_CHRG_SEA_PK, " + " (SELECT DISTINCT CRATES.FCL_APP_RATE " + " FROM CONT_CUST_SEA_TBL    CUSTCON," + " TARIFF_MAIN_SEA_TBL  TARIFF," + " CONT_MAIN_SEA_TBL CONTMAIN, " + " CONT_TRN_SEA_FCL_LCL CONTTRN,CONT_TRN_SEA_FCL_RATES CRATES" + " WHERE CUSTCON.CONT_CUST_SEA_PK = CON.CONT_CUST_SEA_FK" + " AND CUSTCON.TARIFF_MAIN_SEA_FK = TARIFF.TARIFF_MAIN_SEA_PK" + " AND TARIFF.CONT_MAIN_SEA_FK = CONTMAIN.CONT_MAIN_SEA_PK(+)" + " AND CONTMAIN.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK" + " AND CONTTRN.CONT_TRN_SEA_PK=CRATES.CONT_TRN_SEA_FK" + " AND CRATES.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_FK" + " AND CONTTRN.FREIGHT_ELEMENT_MST_FK = SUR.FREIGHT_ELEMENT_MST_FK) CONTARCTRATE  " + "FROM " + "cont_cust_trn_sea_tbl CON,  " + "cont_sur_chrg_sea_tbl SUR, " + "PORT_MST_TBL POL, " + "PORT_MST_TBL POD, " + "CURRENCY_TYPE_MST_TBL CURR, " + "CONTAINER_TYPE_MST_TBL CONT, " + "FREIGHT_ELEMENT_MST_TBL FRT " + "WHERE CON.CONT_CUST_TRN_SEA_PK = SUR.CONT_CUST_TRN_SEA_FK " + "AND   CON.CONTAINER_TYPE_MST_FK   = CONT.CONTAINER_TYPE_MST_PK  " + "AND   CON.PORT_MST_POL_FK         = POL.PORT_MST_PK  " + "AND   CON.PORT_MST_POD_FK         = POD.PORT_MST_PK  " + "AND   SUR.CURRENCY_MST_FK         = CURR.CURRENCY_MST_PK  " + "AND   SUR.FREIGHT_ELEMENT_MST_FK  = FRT.FREIGHT_ELEMENT_MST_PK  " + " AND CONT.CONTAINER_TYPE_MST_PK IN (" + strSearch + ") " + "AND   CON.CONT_CUST_SEA_FK        = " + SrrPk + " ORDER BY FRT.PREFERENCE";
                    }
                    else
                    {
                        strSql = "SELECT DISTINCT * FROM (SELECT" + "CON.POL_GRP_FK AS POLPK, CON.POD_GRP_FK AS PODPK, " + "FRT.FREIGHT_ELEMENT_ID, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "CURR.CURRENCY_ID, CONT.CONTAINER_TYPE_MST_ID AS CONT_BASIS, " + "sur.app_surcharge_amt, SUR.app_surcharge_amt app_surcharge_amt1, SUR.CURRENCY_MST_FK," + "SUR.FREIGHT_ELEMENT_MST_FK, 0 CONT_SUR_CHRG_SEA_PK, " + " (SELECT DISTINCT CRATES.FCL_APP_RATE " + " FROM CONT_CUST_SEA_TBL    CUSTCON," + " TARIFF_MAIN_SEA_TBL  TARIFF," + " CONT_MAIN_SEA_TBL CONTMAIN, " + " CONT_TRN_SEA_FCL_LCL CONTTRN,CONT_TRN_SEA_FCL_RATES CRATES" + " WHERE CUSTCON.CONT_CUST_SEA_PK = CON.CONT_CUST_SEA_FK" + " AND CUSTCON.TARIFF_MAIN_SEA_FK = TARIFF.TARIFF_MAIN_SEA_PK" + " AND TARIFF.CONT_MAIN_SEA_FK = CONTMAIN.CONT_MAIN_SEA_PK(+)" + " AND CONTMAIN.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK" + " AND CONTTRN.CONT_TRN_SEA_PK=CRATES.CONT_TRN_SEA_FK" + " AND CRATES.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_FK" + " AND CONTTRN.FREIGHT_ELEMENT_MST_FK = SUR.FREIGHT_ELEMENT_MST_FK) CONTARCTRATE  " + "FROM " + "cont_cust_trn_sea_tbl CON,  " + "cont_sur_chrg_sea_tbl SUR, " + "PORT_MST_TBL POL, " + "PORT_MST_TBL POD, " + "CURRENCY_TYPE_MST_TBL CURR, " + "CONTAINER_TYPE_MST_TBL CONT, " + "FREIGHT_ELEMENT_MST_TBL FRT " + "WHERE CON.CONT_CUST_TRN_SEA_PK = SUR.CONT_CUST_TRN_SEA_FK " + "AND   CON.CONTAINER_TYPE_MST_FK   = CONT.CONTAINER_TYPE_MST_PK  " + "AND   CON.PORT_MST_POL_FK         = POL.PORT_MST_PK  " + "AND   CON.PORT_MST_POD_FK         = POD.PORT_MST_PK  " + "AND   SUR.CURRENCY_MST_FK         = CURR.CURRENCY_MST_PK  " + "AND   SUR.FREIGHT_ELEMENT_MST_FK  = FRT.FREIGHT_ELEMENT_MST_PK  " + "AND   CON.CONT_CUST_SEA_FK        = " + SrrPk + " ORDER BY FRT.PREFERENCE)";
                    }
                }
                else
                {
                    if (oGroup == 0)
                    {
                        strSql = "SELECT " + "      POL.PORT_MST_PK AS POLPK, POD.PORT_MST_PK AS PODPK, " + "      FRT.FREIGHT_ELEMENT_ID, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "      CURR.CURRENCY_ID, UOM.DIMENTION_ID AS CONT_BASIS, " + "      sur.app_surcharge_amt, SUR.app_surcharge_amt, SUR.CURRENCY_MST_FK, " + "      SUR.FREIGHT_ELEMENT_MST_FK,SUR.CONT_SUR_CHRG_SEA_PK, " + " (SELECT DISTINCT CONTTRN.LCL_APPROVED_RATE" + " FROM CONT_CUST_SEA_TBL    CUSTCON," + " TARIFF_MAIN_SEA_TBL  TARIFF," + " CONT_MAIN_SEA_TBL CONTMAIN, " + " CONT_TRN_SEA_FCL_LCL CONTTRN " + " WHERE CUSTCON.CONT_CUST_SEA_PK = SRR.CONT_CUST_SEA_FK" + " AND CUSTCON.TARIFF_MAIN_SEA_FK = TARIFF.TARIFF_MAIN_SEA_PK" + " AND TARIFF.CONT_MAIN_SEA_FK = CONTMAIN.CONT_MAIN_SEA_PK(+)" + " AND CONTMAIN.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK" + " AND CONTTRN.LCL_BASIS = SRR.LCL_BASIS " + " AND CONTTRN.FREIGHT_ELEMENT_MST_FK = SUR.FREIGHT_ELEMENT_MST_FK) CONTARCTRATE  " + "FROM " + "      CONT_CUST_TRN_SEA_TBL SRR, " + "      CONT_SUR_CHRG_SEA_TBL SUR, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + "WHERE SRR.CONT_CUST_TRN_SEA_PK          = SUR.CONT_CUST_TRN_SEA_FK " + "AND   SRR.LCL_BASIS               = UOM.DIMENTION_UNIT_MST_PK " + "AND   SRR.PORT_MST_POL_FK         = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK         = POD.PORT_MST_PK " + "AND   SUR.CURRENCY_MST_FK         = CURR.CURRENCY_MST_PK " + "AND   SUR.FREIGHT_ELEMENT_MST_FK  = FRT.FREIGHT_ELEMENT_MST_PK " + "AND   SRR.CONT_CUST_SEA_FK        = " + SrrPk + " ORDER BY FRT.PREFERENCE";
                    }
                    else
                    {
                        strSql = "SELECT DISTINCT * FROM (SELECT " + "      SRR.POL_GRP_FK AS POLPK, SRR.POD_GRP_FK AS PODPK, " + "      FRT.FREIGHT_ELEMENT_ID, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "      CURR.CURRENCY_ID, UOM.DIMENTION_ID AS CONT_BASIS, " + "      sur.app_surcharge_amt, SUR.app_surcharge_amt app_surcharge_amt1, SUR.CURRENCY_MST_FK, " + "      SUR.FREIGHT_ELEMENT_MST_FK,0 CONT_SUR_CHRG_SEA_PK,  " + " (SELECT DISTINCT CONTTRN.LCL_APPROVED_RATE" + " FROM CONT_CUST_SEA_TBL    CUSTCON," + " TARIFF_MAIN_SEA_TBL  TARIFF," + " CONT_MAIN_SEA_TBL CONTMAIN, " + " CONT_TRN_SEA_FCL_LCL CONTTRN " + " WHERE CUSTCON.CONT_CUST_SEA_PK = SRR.CONT_CUST_SEA_FK" + " AND CUSTCON.TARIFF_MAIN_SEA_FK = TARIFF.TARIFF_MAIN_SEA_PK" + " AND TARIFF.CONT_MAIN_SEA_FK = CONTMAIN.CONT_MAIN_SEA_PK(+)" + " AND CONTMAIN.CONT_MAIN_SEA_PK = CONTTRN.CONT_MAIN_SEA_FK" + " AND CONTTRN.LCL_BASIS = SRR.LCL_BASIS " + " AND CONTTRN.FREIGHT_ELEMENT_MST_FK = SUR.FREIGHT_ELEMENT_MST_FK) CONTARCTRATE  " + "FROM " + "      CONT_CUST_TRN_SEA_TBL SRR, " + "      CONT_SUR_CHRG_SEA_TBL SUR, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + "WHERE SRR.CONT_CUST_TRN_SEA_PK          = SUR.CONT_CUST_TRN_SEA_FK " + "AND   SRR.LCL_BASIS               = UOM.DIMENTION_UNIT_MST_PK " + "AND   SRR.PORT_MST_POL_FK         = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK         = POD.PORT_MST_PK " + "AND   SUR.CURRENCY_MST_FK         = CURR.CURRENCY_MST_PK " + "AND   SUR.FREIGHT_ELEMENT_MST_FK  = FRT.FREIGHT_ELEMENT_MST_PK " + "AND   SRR.CONT_CUST_SEA_FK        = " + SrrPk + " ORDER BY FRT.PREFERENCE)";
                    }
                }
                dsGrid.Tables.Add(objWF.GetDataTable(strSql));
                dsGrid.Tables[1].TableName = "Frt";

                //Making relation  between Main and Frt table of dsGrid
                //Relation between:
                //                 Main Table            Frt Table
                //                 ---------             ---------
                //                 1. POLPK              1. POLPK
                //                 2. PODPK              2. PODPK
                //                 3. CONT_BASIS          3. CONT_BASIS

                DataColumn[] dcParent = null;
                DataColumn[] dcChild = null;
                DataRelation re = null;

                dcParent = new DataColumn[] {
                dsGrid.Tables["Main"].Columns["POLPK"],
                dsGrid.Tables["Main"].Columns["PODPK"],
                dsGrid.Tables["Main"].Columns["CONT_BASIS"]
            };

                dcChild = new DataColumn[] {
                dsGrid.Tables["Frt"].Columns["POLPK"],
                dsGrid.Tables["Frt"].Columns["PODPK"],
                dsGrid.Tables["Frt"].Columns["CONT_BASIS"]
            };

                re = new DataRelation("rl_Port", dcParent, dcChild);
                //Adding relation to the grid.
                dsGrid.Relations.Add(re);
            }
            catch (OracleException oraExp)
            {
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region "Fetch from Tariff"

        public void FetchOperatorTariffFCL(long TariffPk, string strSearch, DataSet dsMain, string Valid_From, string Valid_To, string bas_curr_fk, string oGroup)
        {
            string strSQL = null;
            Int16 nBOF = default(Int16);
            Int32 nMain = default(Int32);
            DataTable dtTableBOF = new DataTable("BOF");
            DataColumn dcColumn = null;
            DataTable dtTableAllIn = new DataTable("All_In");
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (strSearch.Trim().Length <= 0)
                {
                    strSearch = "(0,0,0)";
                }
                else
                {
                    MakeConditionString(strSearch);
                }

                strSQL = "";
                if (Convert.ToInt32(oGroup) == 0)
                {
                    strSQL = " SELECT P.ALL_IN,PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS FROM (SELECT SUM(ALLIN) AS All_In ,PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS,PREFERENCES  " + " FROM ( " + "  SELECT ( ";
                    strSQL = strSQL + " Allin * CASE WHEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", sysdate)>0 THEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", sysdate) ELSE 1 END " + "  ) AS ALLIN,CURRENCY_MST_PK,PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS,PREFERENCES " + "  FROM    " + "  ( " + "  SELECT SUM(CONT.FCL_REQ_RATE) AS Allin,CURR.CURRENCY_MST_PK, " + "  T.VALID_FROM,T.PORT_MST_POL_FK,T.PORT_MST_POD_FK, " + "  CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS, CTMT.PREFERENCES " + "  FROM TARIFF_TRN_SEA_FCL_LCL T, " + "  TARIFF_MAIN_SEA_TBL TM,  ";
                    strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  PORT_MST_TBL POL, " + "  PORT_MST_TBL POD, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  FREIGHT_ELEMENT_MST_TBL FRT, " + "  CORPORATE_MST_TBL CORP " + "  WHERE T.TARIFF_MAIN_SEA_FK= " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK  " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND T.CHECK_FOR_ALL_IN_RT =1 " + "  AND TM.STATUS = 1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) " + "  IN (" + strSearch + ") " + "  GROUP BY CURR.CURRENCY_ID,CURR.CURRENCY_MST_PK,T.VALID_FROM, " + "  T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CTMT.CONTAINER_TYPE_MST_ID,CTMT.PREFERENCES ORDER BY CTMT.PREFERENCES " + "  )Q,CORPORATE_MST_TBL CORP " + "  ) A WHERE A.Allin > 0  GROUP BY PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS,PREFERENCES ORDER BY PREFERENCES) P ";
                }
                else if (Convert.ToInt32(oGroup )== 1 | Convert.ToInt32(oGroup) == 2)
                {
                    strSQL = strSQL + " SELECT SUM(ALLIN) AS ALL_IN, PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS";
                    strSQL = strSQL + "   FROM (SELECT (ALLIN * GET_EX_RATE(CURRENCY_MST_PK, 173, SYSDATE)) AS ALLIN,";
                    strSQL = strSQL + "                CURRENCY_MST_PK,";
                    strSQL = strSQL + "                PORT_MST_POL_FK,";
                    strSQL = strSQL + "                PORT_MST_POD_FK,";
                    strSQL = strSQL + "                CONT_BASIS";
                    strSQL = strSQL + "           FROM (SELECT DISTINCT SUM(CONT.FCL_REQ_RATE) AS ALLIN,";
                    strSQL = strSQL + "                                 CURR.CURRENCY_MST_PK,";
                    strSQL = strSQL + "                                 T.VALID_FROM,";
                    strSQL = strSQL + "                                 T.POL_GRP_FK PORT_MST_POL_FK,";
                    strSQL = strSQL + "                                 T.POD_GRP_FK PORT_MST_POD_FK,";
                    strSQL = strSQL + "                                 CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS";
                    strSQL = strSQL + "                   FROM TARIFF_TRN_SEA_FCL_LCL  T,";
                    strSQL = strSQL + "                        TARIFF_MAIN_SEA_TBL TM, ";
                    strSQL = strSQL + "                        TARIFF_TRN_SEA_CONT_DTL CONT,";
                    strSQL = strSQL + "                        CURRENCY_TYPE_MST_TBL   CURR,";
                    strSQL = strSQL + "                        CONTAINER_TYPE_MST_TBL  CTMT";
                    strSQL = strSQL + "                  WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk;
                    strSQL = strSQL + "                    AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK ";
                    strSQL = strSQL + "                    AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
                    strSQL = strSQL + "                    AND CONT.CONTAINER_TYPE_MST_FK =";
                    strSQL = strSQL + "                        CTMT.CONTAINER_TYPE_MST_PK";
                    strSQL = strSQL + "                    AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK";
                    strSQL = strSQL + "                    AND T.CHECK_FOR_ALL_IN_RT = 1";
                    strSQL = strSQL + "                    AND TM.STATUS =1 ";
                    strSQL = strSQL + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') ";
                    strSQL = strSQL + " BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ";
                    strSQL = strSQL + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') ";
                    strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) ";
                    strSQL = strSQL + "  AND (T.POL_GRP_FK,T.POD_GRP_FK,CONT.CONTAINER_TYPE_MST_FK) ";
                    strSQL = strSQL + "  IN (" + strSearch + ") ";
                    strSQL = strSQL + "                  GROUP BY CURR.CURRENCY_ID,";
                    strSQL = strSQL + "                           CURR.CURRENCY_MST_PK,";
                    strSQL = strSQL + "                           T.VALID_FROM,";
                    strSQL = strSQL + "                           T.PORT_MST_POL_FK,";
                    strSQL = strSQL + "                           T.PORT_MST_POD_FK,";
                    strSQL = strSQL + "                           T.POL_GRP_FK,";
                    strSQL = strSQL + "                           T.POD_GRP_FK,";
                    strSQL = strSQL + "                           CTMT.CONTAINER_TYPE_MST_ID) Q) A";
                    strSQL = strSQL + "  WHERE A.ALLIN > 0";
                    strSQL = strSQL + "  GROUP BY PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS";
                }
                dtTableAllIn = objWF.GetDataTable(strSQL);

                strSQL = "";

                if (Convert.ToInt32(oGroup) == 0)
                {
                    strSQL = "  SELECT " + "  CONT.FCL_REQ_RATE  AS  BOF,CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS, " + "  T.PORT_MST_POL_FK,T.PORT_MST_POD_FK " + "  FROM TARIFF_TRN_SEA_FCL_LCL T, " + "       TARIFF_MAIN_SEA_TBL TM,  ";
                    strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  PORT_MST_TBL POL, " + "  PORT_MST_TBL POD, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  FREIGHT_ELEMENT_MST_TBL FRT, " + "  CORPORATE_MST_TBL CORP " + "  WHERE T.TARIFF_MAIN_SEA_FK=" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK  " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND FRT.FREIGHT_ELEMENT_MST_PK =" + getCommodityGrp(16) + " " + "  AND CONT.FCL_REQ_RATE > 0 " + "  AND TM.STATUS =1 " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) IN  " + "  (" + strSearch + ")";
                }
                else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2)
                {
                    strSQL = strSQL + "SELECT DISTINCT CONT.FCL_REQ_RATE          AS BOF,";
                    strSQL = strSQL + "                CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS,";
                    strSQL = strSQL + "                T.POL_GRP_FK               PORT_MST_POL_FK,";
                    strSQL = strSQL + "                T.POD_GRP_FK               PORT_MST_POD_FK";
                    strSQL = strSQL + "  FROM TARIFF_TRN_SEA_FCL_LCL  T,";
                    strSQL = strSQL + "       TARIFF_MAIN_SEA_TBL TM, ";
                    strSQL = strSQL + "       TARIFF_TRN_SEA_CONT_DTL CONT,";
                    strSQL = strSQL + "       CURRENCY_TYPE_MST_TBL   CURR,";
                    strSQL = strSQL + "       CONTAINER_TYPE_MST_TBL  CTMT,";
                    strSQL = strSQL + "       FREIGHT_ELEMENT_MST_TBL FRT";
                    strSQL = strSQL + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk;
                    strSQL = strSQL + "   AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK ";
                    strSQL = strSQL + "   AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
                    strSQL = strSQL + "   AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK";
                    strSQL = strSQL + "   AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK";
                    strSQL = strSQL + "   AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK";
                    strSQL = strSQL + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') ";
                    strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ";
                    strSQL = strSQL + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') ";
                    strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) ";
                    strSQL = strSQL + "   AND FRT.FREIGHT_ELEMENT_MST_PK = " + getCommodityGrp(16) + "";
                    strSQL = strSQL + "   AND CONT.FCL_REQ_RATE > 0";
                    strSQL = strSQL + "   AND TM.STATUS =1 ";
                    strSQL = strSQL + "   AND (T.POL_GRP_FK, T.POD_GRP_FK, CONT.CONTAINER_TYPE_MST_FK) IN";
                    strSQL = strSQL + "       (" + strSearch + ")";
                }

                dtTableBOF = objWF.GetDataTable(strSQL);

                strSQL = "";

                if (Convert.ToInt32(oGroup) == 0)
                {
                    strSQL = "  SELECT DISTINCT POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POL," + "  POD.PORT_MST_PK AS PODPK,POD.PORT_ID AS POD, " + "  CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS,CURR.CURRENCY_ID, " + "  0.00 BOF, 0.00 AS ALL_IN," + "  0.00 REQ_BOF, 0.00 REQ_ALLIN,0 THL, 0 THD, 0 VOLUME, " + "  '" + Valid_From + "' AS FROMDATE, " + "  '" + Valid_To + "' AS TODATE,CONT.CONTAINER_TYPE_MST_FK,CURR.CURRENCY_MST_PK, CTMT.PREFERENCES " + "  FROM TARIFF_TRN_SEA_FCL_LCL T, " + "       TARIFF_MAIN_SEA_TBL TM,  ";
                    strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  PORT_MST_TBL POL, " + "  PORT_MST_TBL POD, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  CORPORATE_MST_TBL CORP " + "  WHERE T.TARIFF_MAIN_SEA_FK=" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK  " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND CURR.CURRENCY_MST_PK = T.CURRENCY_MST_FK " + "  AND T.CURRENCY_MST_FK = (SELECT DISTINCT CURRENCY_MST_PK FROM TARIFF_TRN_SEA_FCL_LCL TR," + "   CURRENCY_TYPE_MST_TBL CR,FREIGHT_ELEMENT_MST_TBL FR" + "   where " + "   CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK AND" + "   TR.Freight_Element_Mst_Fk = FR.FREIGHT_ELEMENT_MST_PK AND " + "   TR.TARIFF_MAIN_SEA_FK= " + TariffPk + "  AND " + "   FR.FREIGHT_ELEMENT_MST_PK = " + getCommodityGrp(16) + " " + "  AND TR.PORT_MST_POL_FK=T.PORT_MST_POL_FK" + "  AND TR.PORT_MST_POD_FK=T.PORT_MST_POD_FK ) ";
                    strSQL = strSQL + "    AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "   AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "   AND TM.STATUS =1 " + "   AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) " + "  IN (" + strSearch + ")" + " ORDER BY PREFERENCES";
                }
                else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2)
                {
                    strSQL = strSQL + "SELECT DISTINCT POLGRP.PORT_GRP_MST_PK AS POLPK,";
                    strSQL = strSQL + "                POLGRP.PORT_GRP_ID AS POL,";
                    strSQL = strSQL + "                PODGRP.PORT_GRP_MST_PK AS PODPK,";
                    strSQL = strSQL + "                PODGRP.PORT_GRP_ID AS POD,";
                    strSQL = strSQL + "                CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS,";
                    strSQL = strSQL + "                CURR.CURRENCY_ID,";
                    strSQL = strSQL + "                0.00 BOF,";
                    strSQL = strSQL + "                0.00 AS ALL_IN,";
                    strSQL = strSQL + "                0.00 REQ_BOF,";
                    strSQL = strSQL + "                0.00 REQ_ALLIN,";
                    strSQL = strSQL + "                0 THL,";
                    strSQL = strSQL + "                0 THD,";
                    strSQL = strSQL + "                0 VOLUME,";
                    strSQL = strSQL + "                '" + Valid_From + "' AS FROMDATE,";
                    strSQL = strSQL + "                '" + Valid_To + "' AS TODATE,";
                    strSQL = strSQL + "                CONT.CONTAINER_TYPE_MST_FK,";
                    strSQL = strSQL + "                CURR.CURRENCY_MST_PK,CTMT.PREFERENCES";
                    strSQL = strSQL + "  FROM TARIFF_TRN_SEA_FCL_LCL  T,";
                    strSQL = strSQL + "       TARIFF_MAIN_SEA_TBL TM, ";
                    strSQL = strSQL + "       TARIFF_TRN_SEA_CONT_DTL CONT,";
                    strSQL = strSQL + "       PORT_MST_TBL            POL,";
                    strSQL = strSQL + "       PORT_MST_TBL            POD,";
                    strSQL = strSQL + "       CURRENCY_TYPE_MST_TBL   CURR,";
                    strSQL = strSQL + "       CONTAINER_TYPE_MST_TBL  CTMT,";
                    strSQL = strSQL + "       PORT_GRP_MST_TBL        POLGRP,";
                    strSQL = strSQL + "       PORT_GRP_MST_TBL        PODGRP";
                    strSQL = strSQL + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk;
                    strSQL = strSQL + "   AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK ";
                    strSQL = strSQL + "   AND T.PORT_MST_POL_FK = POL.PORT_MST_PK";
                    strSQL = strSQL + "   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK";
                    strSQL = strSQL + "   AND T.POL_GRP_FK = POLGRP.PORT_GRP_MST_PK";
                    strSQL = strSQL + "   AND T.POD_GRP_FK = PODGRP.PORT_GRP_MST_PK";
                    strSQL = strSQL + "   AND CURR.CURRENCY_MST_PK = T.CURRENCY_MST_FK";
                    strSQL = strSQL + "   AND T.CURRENCY_MST_FK =";
                    strSQL = strSQL + "       (SELECT DISTINCT CURRENCY_MST_PK";
                    strSQL = strSQL + "          FROM TARIFF_TRN_SEA_FCL_LCL  TR,";
                    strSQL = strSQL + "               CURRENCY_TYPE_MST_TBL   CR,";
                    strSQL = strSQL + "               FREIGHT_ELEMENT_MST_TBL FR";
                    strSQL = strSQL + "         WHERE CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK";
                    strSQL = strSQL + "           AND TR.FREIGHT_ELEMENT_MST_FK = FR.FREIGHT_ELEMENT_MST_PK";
                    strSQL = strSQL + "           AND TR.TARIFF_MAIN_SEA_FK = " + TariffPk;
                    strSQL = strSQL + "           AND FR.FREIGHT_ELEMENT_MST_PK = " + getCommodityGrp(16) + "";
                    strSQL = strSQL + "           AND TR.PORT_MST_POL_FK = T.PORT_MST_POL_FK";
                    strSQL = strSQL + "           AND TR.PORT_MST_POD_FK = T.PORT_MST_POD_FK)";
                    strSQL = strSQL + "   AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK";
                    strSQL = strSQL + "   AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK";
                    strSQL = strSQL + "   AND TM.STATUS =1 ";
                    strSQL = strSQL + "   AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') ";
                    strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ";
                    strSQL = strSQL + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') ";
                    strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) ";
                    strSQL = strSQL + "   AND (T.POL_GRP_FK, T.POD_GRP_FK, CONT.CONTAINER_TYPE_MST_FK) IN";
                    strSQL = strSQL + "       (" + strSearch + ")";
                }
                DataTable DT = new DataTable();
                DT = objWF.GetDataTable(strSQL);
                DT.Columns.Remove("PREFERENCES");
                DT.AcceptChanges();
                dsMain.Tables.Add(DT);
                dsMain.Tables[0].TableName = "Main";

                dcColumn = new DataColumn("Surcharge", typeof(Int16));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                dcColumn = new DataColumn("Del", typeof(Int16));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                dcColumn = new DataColumn("Srr_Trn_Pk", typeof(Int64));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                for (nMain = 0; nMain <= dsMain.Tables["Main"].Rows.Count - 1; nMain++)
                {
                    for (nBOF = 0; nBOF <= dtTableBOF.Rows.Count - 1; nBOF++)
                    {
                        if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableBOF.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POD_FK"])
                        {
                            dsMain.Tables["Main"].Rows[nMain]["BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
                            dsMain.Tables["Main"].Rows[nMain]["REQ_BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }

                    for (nBOF = 0; nBOF <= dtTableAllIn.Rows.Count - 1; nBOF++)
                    {
                        if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableAllIn.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POD_FK"])
                        {
                            dsMain.Tables["Main"].Rows[nMain]["ALL_IN"] = dtTableAllIn.Rows[nBOF]["All_In"];
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                }

                strSQL = "";

                if (Convert.ToInt32(oGroup) == 0)
                {
                    strSQL = "  SELECT " + "  T.PORT_MST_POL_FK AS POLPK, T.PORT_MST_POD_FK AS PODPK, " + "  FRT.FREIGHT_ELEMENT_ID as FRT_ELEMENT, TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK, " + "  CURR.CURRENCY_ID as CURRENCY,CTMT.CONTAINER_TYPE_MST_ID AS CONT_BASIS, " + "  NVL(CONT.FCL_REQ_RATE,0.00) AS TARIFF_RATE, " + "  NVL(CONT.FCL_REQ_RATE,0.00) AS REQUESTED_RATE," + "  CURR.CURRENCY_MST_PK as CURRENCYPK,FRT.FREIGHT_ELEMENT_MST_PK as FRT_ELEMENTPK,0 as SURCHARGE_SEA_PK,0 as SL_CONTARCT_RATE " + "  FROM TARIFF_TRN_SEA_FCL_LCL T, " + "       TARIFF_MAIN_SEA_TBL TM,  ";

                    strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  FREIGHT_ELEMENT_MST_TBL FRT " + "  WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ";
                    strSQL = strSQL + "   AND T.CURRENCY_MST_FK IN ";
                    strSQL = strSQL + "       (SELECT DISTINCT CURRENCY_MST_PK";
                    strSQL = strSQL + "          FROM TARIFF_TRN_SEA_FCL_LCL  TR,";
                    strSQL = strSQL + "               CURRENCY_TYPE_MST_TBL   CR,";
                    strSQL = strSQL + "               FREIGHT_ELEMENT_MST_TBL FR";
                    strSQL = strSQL + "         WHERE CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK";
                    strSQL = strSQL + "           AND TR.FREIGHT_ELEMENT_MST_FK = FR.FREIGHT_ELEMENT_MST_PK";
                    strSQL = strSQL + "           AND TR.TARIFF_MAIN_SEA_FK = " + TariffPk;
                    //strSQL = strSQL & "           AND FR.FREIGHT_ELEMENT_MST_PK = " & getCommodityGrp(16) & ""
                    strSQL = strSQL + "           AND TR.PORT_MST_POL_FK = T.PORT_MST_POL_FK";
                    strSQL = strSQL + "           AND TR.PORT_MST_POD_FK = T.PORT_MST_POD_FK)" + "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND FRT.FREIGHT_ELEMENT_MST_PK <> " + getCommodityGrp(16) + " " + "  AND CONT.FCL_REQ_RATE > 0 " + "  AND TM.STATUS =1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) " + "  IN (" + strSearch + ")" + "  ORDER BY FRT.PREFERENCE ";
                }
                else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2)
                {
                    strSQL = strSQL + "SELECT DISTINCT *";
                    strSQL = strSQL + "  FROM (SELECT T.POL_GRP_FK AS POLPK,";
                    strSQL = strSQL + "               T.POD_GRP_FK AS PODPK,";
                    strSQL = strSQL + "               FRT.FREIGHT_ELEMENT_ID as FRT_ELEMENT,";
                    strSQL = strSQL + "               TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK,";
                    strSQL = strSQL + "               CURR.CURRENCY_ID as CURRENCY,";
                    strSQL = strSQL + "               CTMT.CONTAINER_TYPE_MST_ID AS CONT_BASIS,";
                    strSQL = strSQL + "               NVL(CONT.FCL_REQ_RATE, 0.00) AS TARIFF_RATE,";
                    strSQL = strSQL + "               NVL(CONT.FCL_REQ_RATE, 0.00) AS REQUESTED_RATE,";
                    strSQL = strSQL + "               CURR.CURRENCY_MST_PK as CURRENCYPK,";
                    strSQL = strSQL + "               FRT.FREIGHT_ELEMENT_MST_PK as FRT_ELEMENTPK, 0 as SURCHARGE_SEA_PK,0 as SL_CONTARCT_RATE";
                    strSQL = strSQL + "          FROM TARIFF_TRN_SEA_FCL_LCL  T,";
                    strSQL = strSQL + "               TARIFF_MAIN_SEA_TBL TM, ";
                    strSQL = strSQL + "               TARIFF_TRN_SEA_CONT_DTL CONT,";
                    strSQL = strSQL + "               CURRENCY_TYPE_MST_TBL   CURR,";
                    strSQL = strSQL + "               CONTAINER_TYPE_MST_TBL  CTMT,";
                    strSQL = strSQL + "               FREIGHT_ELEMENT_MST_TBL FRT";
                    strSQL = strSQL + "         WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk;
                    strSQL = strSQL + "           AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK ";
                    strSQL = strSQL + "           AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
                    strSQL = strSQL + "           AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK";
                    strSQL = strSQL + "           AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK";
                    strSQL = strSQL + "           AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK";
                    strSQL = strSQL + "           AND FRT.FREIGHT_ELEMENT_MST_PK <> " + getCommodityGrp(16) + "";
                    strSQL = strSQL + "           AND CONT.FCL_REQ_RATE > 0";
                    strSQL = strSQL + "           AND TM.STATUS =1 ";
                    strSQL = strSQL + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') ";
                    strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ";
                    strSQL = strSQL + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') ";
                    strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) ";
                    strSQL = strSQL + "           AND (T.POL_GRP_FK, T.POD_GRP_FK, CONT.CONTAINER_TYPE_MST_FK) IN";
                    strSQL = strSQL + "               (" + strSearch + ")";
                    strSQL = strSQL + "         ORDER BY FRT.PREFERENCE)";
                }

                dsMain.Tables.Add(objWF.GetDataTable(strSQL));
                dsMain.Tables[1].TableName = "Frt";

                //Dim validRowCollChild As New List(Of DataRow)
                //Dim parentTableName As String = "Main"
                //Dim ChildTableName As String = "Frt"
                //If dsMain.Tables(parentTableName).Rows.Count = 0 Then
                //    dsMain.Tables(ChildTableName).Rows.Clear()
                //Else
                //    For Each _childRow As DataRow In dsMain.Tables(ChildTableName).Rows
                //        Dim parentExist As Boolean = False
                //        For Each _parentRow As DataRow In dsMain.Tables(ChildTableName).Rows
                //            If _childRow("") = _parentRow("") Then
                //                parentExist = True
                //                Exit For
                //            End If
                //        Next

                //        If parentExist Then
                //            validRowCollChild.Add(_childRow)
                //        End If
                //    Next
                //End If

                //dsMain.Tables(ChildTableName).Rows.Clear()
                //For Each _row As DataRow In validRowCollChild
                //    dsMain.Tables(ChildTableName).Rows.Add(_row)
                //Next

                DataColumn[] dcParent = null;
                DataColumn[] dcChild = null;
                DataRelation re = null;
                dcParent = new DataColumn[] {
                dsMain.Tables["Main"].Columns["POLPK"],
                dsMain.Tables["Main"].Columns["PODPK"],
                dsMain.Tables["Main"].Columns["CONT_BASIS"]
            };
                dcChild = new DataColumn[] {
                dsMain.Tables["Frt"].Columns["POLPK"],
                dsMain.Tables["Frt"].Columns["PODPK"],
                dsMain.Tables["Frt"].Columns["CONT_BASIS"]
            };
                re = new DataRelation("rl_Port", dcParent, dcChild);

                dsMain.Relations.Add(re);
            }
            catch (OracleException exSQL)
            {
                throw exSQL;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void FetchOperatorTariffLCL(long TariffPk, string strSearch, DataSet dsMain, string Valid_From, string Valid_To, string bas_curr_fk, string oGroup)
        {
            string strSQL = null;
            Int16 nBOF = default(Int16);
            Int32 nMain = default(Int32);
            DataTable dtTableBOF = new DataTable("BOF");
            DataColumn dcColumn = null;
            DataTable dtTableAllIn = new DataTable("All_In");
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (strSearch.Trim().Length <= 0)
                {
                    strSearch = "(0,0,0)";
                }
                else
                {
                    MakeConditionString(strSearch);
                }

                strSQL = "";
                if (Convert.ToInt32(oGroup )== 0)
                {
                    strSQL = " SELECT SUM(ALLIN) AS ALL_IN, PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS " + " FROM ( " + "  SELECT ( ";

                    strSQL = strSQL + " Allin * CASE WHEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", VALID_FROM)>0 THEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", VALID_FROM) ELSE 1 END " + "              ) AS ALLIN, " + "              CURRENCY_MST_PK, " + "              PORT_MST_POL_FK, " + "              PORT_MST_POD_FK, " + "              CONT_BASIS " + "         FROM (SELECT SUM(T.LCL_TARIFF_RATE) AS ALLIN, " + "                      CURR.CURRENCY_MST_PK, " + "                      T.VALID_FROM, " + "                      T.PORT_MST_POL_FK, " + "                      T.PORT_MST_POD_FK, " + "                      UOM.DIMENTION_ID CONT_BASIS " + "                 FROM TARIFF_TRN_SEA_FCL_LCL T, " + "                      TARIFF_MAIN_SEA_TBL TM, " + "                      DIMENTION_UNIT_MST_TBL UOM, " + "                      PORT_MST_TBL POL, " + "                      PORT_MST_TBL POD, " + "                      CURRENCY_TYPE_MST_TBL CURR, " + "                      FREIGHT_ELEMENT_MST_TBL FRT, " + "                      CORPORATE_MST_TBL CORP " + "                WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "                  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "                  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "                  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "                  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "                  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "                  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "                  AND T.CHECK_FOR_ALL_IN_RT = 1 " + "                  AND TM.STATUS =1 " + "                  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "                       OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "                  AND (T.PORT_MST_POL_FK, T.PORT_MST_POD_FK,T.LCL_BASIS) IN " + "                      (" + strSearch + ") " + "                GROUP BY CURR.CURRENCY_ID, " + "                         CURR.CURRENCY_MST_PK, " + "                         T.VALID_FROM, " + "                         T.PORT_MST_POL_FK, " + "                         T.PORT_MST_POD_FK, " + "                         UOM.DIMENTION_ID ) Q, " + "              CORPORATE_MST_TBL CORP) A " + " WHERE(A.ALLIN > 0) " + " GROUP BY PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS";
                }
                else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2)
                {
                    strSQL = " SELECT SUM(ALLIN) AS ALL_IN, PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS " + " FROM ( " + "  SELECT ( ";

                    strSQL = strSQL + " Allin * CASE WHEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", VALID_FROM)>0 THEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", VALID_FROM) ELSE 1 END " + "              ) AS ALLIN, " + "              CURRENCY_MST_PK, " + "              PORT_MST_POL_FK, " + "              PORT_MST_POD_FK, " + "              CONT_BASIS " + "         FROM (SELECT DISTINCT SUM(T.LCL_TARIFF_RATE) AS ALLIN, " + "                      CURR.CURRENCY_MST_PK, " + "                      T.VALID_FROM, " + "                      t.pol_grp_fk PORT_MST_POL_FK, " + "                      t.pod_grp_fk PORT_MST_POD_FK, " + "                      UOM.DIMENTION_ID CONT_BASIS " + "                 FROM TARIFF_TRN_SEA_FCL_LCL T, " + "                      TARIFF_MAIN_SEA_TBL TM, " + "                      DIMENTION_UNIT_MST_TBL UOM, " + "                      PORT_MST_TBL POL, " + "                      PORT_MST_TBL POD, " + "                      CURRENCY_TYPE_MST_TBL CURR, " + "                      FREIGHT_ELEMENT_MST_TBL FRT, " + "                      CORPORATE_MST_TBL CORP " + "                WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "                  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "                  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "                  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "                  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "                  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "                  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "                  AND T.CHECK_FOR_ALL_IN_RT = 1 " + "                  AND TM.STATUS =1 " + "                  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "                       OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "                  AND (T.POL_GRP_FK, T.POD_GRP_FK, T.LCL_BASIS) IN " + "                      (" + strSearch + ") " + "                GROUP BY CURR.CURRENCY_ID, " + "                         CURR.CURRENCY_MST_PK, " + "                         T.VALID_FROM, " + "                         T.PORT_MST_POL_FK, " + "                         T.PORT_MST_POD_FK, t.pol_grp_fk, t.pod_grp_fk, " + "                         UOM.DIMENTION_ID ) Q, " + "              CORPORATE_MST_TBL CORP) A " + " WHERE(A.ALLIN > 0) " + " GROUP BY PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS";
                }

                dtTableAllIn = objWF.GetDataTable(strSQL);

                if (Convert.ToInt32(oGroup) == 0)
                {
                    strSQL = "SELECT " + " T.LCL_TARIFF_RATE AS BOF, " + "     UOM.DIMENTION_ID CONT_BASIS, " + "     T.PORT_MST_POL_FK, " + "     T.PORT_MST_POD_FK, " + "     T.Currency_Mst_Fk  CURRENCY_MST_PK  " + "FROM TARIFF_TRN_SEA_FCL_LCL T, " + "    TARIFF_MAIN_SEA_TBL TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     FREIGHT_ELEMENT_MST_TBL FRT, " + "     CORPORATE_MST_TBL CORP " + "WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND FRT.FREIGHT_ELEMENT_MST_PK = " + getCommodityGrp(16) + " " + "  AND T.LCL_BASIS>0 " + "  AND TM.STATUS =1 " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,T.LCL_BASIS) IN " + "      (" + strSearch + ") ";
                }
                else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2)
                {
                    strSQL = "SELECT DISTINCT " + " T.LCL_TARIFF_RATE AS BOF, " + "     UOM.DIMENTION_ID CONT_BASIS, " + "     T.POL_GRP_FK PORT_MST_POL_FK, " + "     T.POD_GRP_FK PORT_MST_POD_FK, " + "     T.Currency_Mst_Fk  CURRENCY_MST_PK  " + "FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     FREIGHT_ELEMENT_MST_TBL FRT, " + "     CORPORATE_MST_TBL CORP " + "WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND FRT.FREIGHT_ELEMENT_ID = 'BOF' " + "  AND T.LCL_BASIS>0 " + "  AND TM.STATUS =1 " + "  AND (T.POL_GRP_FK, T.POD_GRP_FK, T.LCL_BASIS) IN " + "      (" + strSearch + ") ";
                }

                dtTableBOF = objWF.GetDataTable(strSQL);

                strSQL = "";
                if (Convert.ToInt32(oGroup) == 0)
                {
                    strSQL = "SELECT DISTINCT " + "         POL.PORT_MST_PK AS POLPK, " + "         POL.PORT_ID AS POL, " + "         POD.PORT_MST_PK AS PODPK, " + "         POD.PORT_ID AS POD, " + "         UOM.DIMENTION_ID CONT_BASIS, " + "         CURR.CURRENCY_ID, " + "         0.00 BOF, " + "         0.00 AS ALL_IN, " + "         0.00 REQ_BOF, " + "         0.00 REQ_ALLIN, " + "         0 THL, " + "         0 THD, " + "         0 VOLUME, " + "         '" + Valid_From + "' AS FROMDATE, " + "         '" + Valid_To + "' AS TODATE, " + "         T.LCL_BASIS, " + "         CURR.CURRENCY_MST_PK " + "FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     CORPORATE_MST_TBL CORP " + "     WHERE T.TARIFF_MAIN_SEA_FK =" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + " AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + " AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND CURR.CURRENCY_MST_PK = T.CURRENCY_MST_FK " + "  AND T.CURRENCY_MST_FK IN(SELECT DISTINCT CURRENCY_MST_PK FROM TARIFF_TRN_SEA_FCL_LCL TR," + "   CURRENCY_TYPE_MST_TBL CR,FREIGHT_ELEMENT_MST_TBL FR" + "   where " + "   CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK AND" + "   TR.Freight_Element_Mst_Fk = FR.FREIGHT_ELEMENT_MST_PK AND " + "   TR.TARIFF_MAIN_SEA_FK= " + TariffPk + "  AND " + "   FR.FREIGHT_ELEMENT_ID = 'BOF' " + "  AND TR.PORT_MST_POL_FK=T.PORT_MST_POL_FK" + "  AND TR.PORT_MST_POD_FK=T.PORT_MST_POD_FK ) " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND TM.STATUS =1 " + " AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "     OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + " AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,T.LCL_BASIS) IN " + "     (" + strSearch + ") ";
                }
                else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2)
                {
                    strSQL = "SELECT DISTINCT " + "         POLGRP.PORT_GRP_MST_PK AS POLPK, " + "         POLGRP.PORT_GRP_ID AS POL, " + "         PODGRP.PORT_GRP_MST_PK AS PODPK, " + "         PODGRP.PORT_GRP_ID AS POD, " + "         UOM.DIMENTION_ID CONT_BASIS, " + "         CURR.CURRENCY_ID, " + "         0.00 BOF, " + "         0.00 AS ALL_IN, " + "         0.00 REQ_BOF, " + "         0.00 REQ_ALLIN," + "         0 THL, " + "         0 THD, " + "         0 VOLUME, " + "         '" + Valid_From + "' AS FROMDATE, " + "         '" + Valid_To + "' AS TODATE, " + "         T.LCL_BASIS, " + "         CURR.CURRENCY_MST_PK " + "FROM TARIFF_TRN_SEA_FCL_LCL T, " + "    TARIFF_MAIN_SEA_TBL TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, PORT_GRP_MST_TBL       POLGRP, PORT_GRP_MST_TBL       PODGRP, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     CORPORATE_MST_TBL CORP " + "     WHERE T.TARIFF_MAIN_SEA_FK =" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + " AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + " AND T.PORT_MST_POD_FK = POD.PORT_MST_PK AND T.POL_GRP_FK = POLGRP.PORT_GRP_MST_PK AND T.POD_GRP_FK = PODGRP.PORT_GRP_MST_PK " + "  AND CURR.CURRENCY_MST_PK = T.CURRENCY_MST_FK " + "  AND T.CURRENCY_MST_FK IN(SELECT DISTINCT CURRENCY_MST_PK FROM TARIFF_TRN_SEA_FCL_LCL TR," + "   CURRENCY_TYPE_MST_TBL CR,FREIGHT_ELEMENT_MST_TBL FR" + "   where " + "   CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK AND" + "   TR.Freight_Element_Mst_Fk = FR.FREIGHT_ELEMENT_MST_PK AND " + "   TR.TARIFF_MAIN_SEA_FK= " + TariffPk + "  AND " + "   FR.FREIGHT_ELEMENT_ID = 'BOF' " + "  AND TR.PORT_MST_POL_FK=T.PORT_MST_POL_FK" + "  AND TR.PORT_MST_POD_FK=T.PORT_MST_POD_FK ) " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND TM.STATUS =1 " + " AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "     OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + " AND (T.POL_GRP_FK, T.POD_GRP_FK, T.LCL_BASIS) IN " + "     (" + strSearch + ") ";
                }

                dsMain.Tables.Add(objWF.GetDataTable(strSQL));
                dsMain.Tables[0].TableName = "Main";
                dcColumn = new DataColumn("Surcharge", typeof(Int16));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                dcColumn = new DataColumn("Del", typeof(Int16));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                dcColumn = new DataColumn("Srr_Trn_Pk", typeof(Int64));
                dsMain.Tables["Main"].Columns.Add(dcColumn);

                for (nMain = 0; nMain <= dsMain.Tables["Main"].Rows.Count - 1; nMain++)
                {
                    for (nBOF = 0; nBOF <= dtTableBOF.Rows.Count - 1; nBOF++)
                    {
                        if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableBOF.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POD_FK"])
                        {
                            dsMain.Tables["Main"].Rows[nMain]["BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
                            dsMain.Tables["Main"].Rows[nMain]["REQ_BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }

                    for (nBOF = 0; nBOF <= dtTableAllIn.Rows.Count - 1; nBOF++)
                    {
                        if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableAllIn.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POD_FK"])
                        {
                            dsMain.Tables["Main"].Rows[nMain]["ALL_IN"] = dtTableAllIn.Rows[nBOF]["All_In"];
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                }

                strSQL = "";

                if (Convert.ToInt32(oGroup) == 0)
                {
                    strSQL = " SELECT T.PORT_MST_POL_FK AS POLPK, " + "      T.PORT_MST_POD_FK AS PODPK, " + "      FRT.FREIGHT_ELEMENT_ID," + "      TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK, " + "      CURR.CURRENCY_ID, " + "      UOM.DIMENTION_ID AS CONT_BASIS, " + "      NVL(T.LCL_TARIFF_RATE,0.00) AS TARIFF_RATE, " + "      NVL(T.LCL_TARIFF_RATE,0.00) AS APPROVED_RATE, " + "      CURR.CURRENCY_MST_PK, " + "      FRT.FREIGHT_ELEMENT_MST_PK,0 as CONT_SUR_CHRG_SEA_PK,0 as CONTARCTRATE " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "      TARIFF_MAIN_SEA_TBL TM, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND FRT.FREIGHT_ELEMENT_ID <> 'BOF' " + "  AND T.LCL_BASIS > 0 " + "  AND TM.STATUS =1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK, T.PORT_MST_POD_FK,T.LCL_BASIS ) IN " + "      (" + strSearch + ") ";
                }
                else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2)
                {
                    strSQL = " SELECT DISTINCT T.POL_GRP_FK AS POLPK, " + "      T.POD_GRP_FK AS PODPK, " + "      FRT.FREIGHT_ELEMENT_ID," + "      TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK, " + "      CURR.CURRENCY_ID, " + "      UOM.DIMENTION_ID AS CONT_BASIS, " + "      NVL(T.LCL_TARIFF_RATE,0.00) AS TARIFF_RATE, " + "      NVL(T.LCL_TARIFF_RATE,0.00) AS APPROVED_RATE, " + "      CURR.CURRENCY_MST_PK, " + "      FRT.FREIGHT_ELEMENT_MST_PK,0 as CONT_SUR_CHRG_SEA_PK,0 as CONTARCTRATE " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "      TARIFF_MAIN_SEA_TBL TM, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK = TM.TARIFF_MAIN_SEA_PK  " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND FRT.FREIGHT_ELEMENT_ID <> 'BOF' " + "  AND T.LCL_BASIS > 0 " + "  AND TM.STATUS =1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.POL_GRP_FK, T.POD_GRP_FK, T.LCL_BASIS ) IN " + "      (" + strSearch + ") ";
                }

                dsMain.Tables.Add(objWF.GetDataTable(strSQL));
                dsMain.Tables[1].TableName = "Frt";

                dcColumn = new DataColumn("SRR_SUR_CHRG_SEA_PK", typeof(Int64));
                dsMain.Tables["Frt"].Columns.Add(dcColumn);

                DataColumn[] dcParent = null;
                DataColumn[] dcChild = null;
                DataRelation re = null;
                dcParent = new DataColumn[] {
                dsMain.Tables["Main"].Columns["POLPK"],
                dsMain.Tables["Main"].Columns["PODPK"],
                dsMain.Tables["Main"].Columns["CONT_BASIS"]
            };
                dcChild = new DataColumn[] {
                dsMain.Tables["Frt"].Columns["POLPK"],
                dsMain.Tables["Frt"].Columns["PODPK"],
                dsMain.Tables["Frt"].Columns["CONT_BASIS"]
            };
                re = new DataRelation("rl_Port", dcParent, dcChild);

                dsMain.Relations.Add(re);
            }
            catch (OracleException exSQL)
            {
                throw exSQL;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch from Tariff"

        //This proc. is called while editing or viewing the record.
        public void Fetch_SRR(long SrrPk, DataSet dsGrid, DataTable dtMain, bool IsLCL)
        {
            string strSql = null;
            int baseCurrency = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSql = "";
                strSql = "SELECT " + "     OPR.OPERATOR_ID, OPR.OPERATOR_NAME, OPR.OPERATOR_MST_PK, " + "      CASE WHEN HDR.SRR_TYPE=0 THEN contract.CONT_REF_NO ELSE TARIFF.Tariff_Ref_No END AS CONT_REF_NO,CASE WHEN HDR.SRR_TYPE=0 THEN contract.CONT_CUST_SEA_PK ELSE TARIFF.tariff_main_sea_pk END CONT_CUST_SEA_PK, " + "      COMM_GRP.COMMODITY_GROUP_PK, " + "      COMM.COMMODITY_ID COMMODITY_NAME,COMM.COMMODITY_MST_PK, " + "      CUST.CUSTOMER_ID,CUST.CUSTOMER_NAME,CUST.CUSTOMER_MST_PK, " + "      LOC.LOCATION_ID,LOC.LOCATION_MST_PK,LOC.LOCATION_NAME, " + "      HDR.SRR_REF_NO,TO_CHAR(HDR.SRR_DATE,'" + dateFormat + "') AS SRR_DATE," + "      HDR.CARGO_TYPE,TO_CHAR(HDR.VALID_FROM,'" + dateFormat + "') AS VALID_FROM, " + "      TO_CHAR(HDR.VALID_TO,'" + dateFormat + "') AS VALID_TO, " + "      HDR.SRR_CLAUSE,HDR.SRR_REMARKS,HDR.CREDIT_PERIOD,HDR.VERSION_NO,HDR.STATUS,HDR.ACTIVE, " + "      HDR.COL_ADDRESS COLLECTION_ADDRESS,CURR.CURRENCY_MST_PK BASE_CURRENCY_FK,  " + "       CURR.CURRENCY_ID,HDR.REF_NR, HDR.PORT_GROUP,hdr.srr_type " + " FROM " + "      SRR_SEA_TBL HDR, " + "      CONT_CUST_SEA_TBL contract,tariff_main_sea_tbl TARIFF, " + "      OPERATOR_MST_TBL OPR, " + "      COMMODITY_GROUP_MST_TBL COMM_GRP, " + "      COMMODITY_MST_TBL COMM, " + "      CUSTOMER_MST_TBL CUST, " + "      LOCATION_MST_TBL Loc, " + "     CURRENCY_TYPE_MST_TBL   CURR " + "WHERE HDR.TARIFF_MAIN_SEA_FK           = TARIFF.tariff_main_sea_pk(+) " + "AND HDR.TARIFF_MAIN_SEA_FK            = contract.CONT_CUST_SEA_PK(+) " + "AND   HDR.OPERATOR_MST_FK              = OPR.OPERATOR_MST_PK(+) " + "AND   HDR.COMMODITY_GROUP_MST_FK       = COMM_GRP.COMMODITY_GROUP_PK " + "AND   HDR.COMMODITY_MST_FK             = COMM.COMMODITY_MST_PK (+) " + "AND   HDR.CUSTOMER_MST_FK              = CUST.CUSTOMER_MST_PK " + "AND   HDR.PYMT_LOCATION_MST_FK         = LOC.LOCATION_MST_PK (+) " + "AND CURR.CURRENCY_MST_PK(+)            = HDR.BASE_CURRENCY_FK " + "AND   HDR.SRR_SEA_PK                   = " + SrrPk;

                dtMain = objWF.GetDataTable(strSql);

                strSql = "";
                if (!IsLCL)
                {
                    if (Convert.ToInt32(dtMain.Rows[0]["PORT_GROUP"]) == 0)
                    {
                        strSql = "SELECT POLPK,POLID,PODPK,PODID,CONT_BASIS,CURRENCY_ID,CURRENT_BOF_RATE,CURRENT_ALL_IN_RATE, " + " REQUESTED_BOF_RATE,REQUESTED_ALL_IN_RATE,THL,THD,EXPECTED_BOXES,VALID_FROM,VALID_TO, " + " CONTAINER_TYPE_MST_FK,CURRENCY_MST_FK,SURCHARGE,DEL,SRR_TRN_SEA_PK FROM ( " + " SELECT DISTINCT " + "      POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POLID, " + "      POD.PORT_MST_PK AS PODPK,POD.PORT_ID AS PODID, " + "      CONT.CONTAINER_TYPE_MST_ID CONT_BASIS,  " + "      CURR.CURRENCY_ID, SRR.CURRENT_BOF_RATE,  " + "      SRR.CURRENT_ALL_IN_RATE, SRR.REQUESTED_BOF_RATE, " + "    (SUM(FRT.REQ_SURCHARGE_AMT * get_ex_rate( FRT.CURRENCY_MST_FK,173, SYSDATE))+ SRR.REQUESTED_BOF_RATE)REQUESTED_ALL_IN_RATE1, " + "    SUM((SRR.CURRENT_ALL_IN_RATE - " + "                (SRR.CURRENT_BOF_RATE *  " + "                GET_EX_RATE(FRT.CURRENCY_MST_FK, " + baseCurrency + ", SYSDATE))) + " + "                SRR.REQUESTED_BOF_RATE * " + "                GET_EX_RATE(FRT.CURRENCY_MST_FK, " + baseCurrency + ", SYSDATE))REQUESTED_ALL_IN_RATE, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (1,3)) THEN '1' ELSE '0' END) AS THL, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (2,3)) THEN '1' ELSE '0' END) AS THD, " + "      SRR.EXPECTED_BOXES, TO_CHAR(SRR.VALID_FROM,'" + dateFormat + "') VALID_FROM, " + "      TO_CHAR(SRR.VALID_TO,'" + dateFormat + "') VALID_TO, SRR.CONTAINER_TYPE_MST_FK, " + "      SRR.CURRENCY_MST_FK, " + "      (CASE WHEN SRR.SUBJECT_TO_SURCHG_CHG =1 THEN '1' ELSE '0' END) AS SURCHARGE, " + "      '0' DEL,SRR.SRR_TRN_SEA_PK,CONT.PREFERENCES " + "FROM " + "      SRR_TRN_SEA_TBL SRR, " + "       SRR_SUR_CHRG_SEA_TBL FRT, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      CONTAINER_TYPE_MST_TBL CONT, " + "      CURRENCY_TYPE_MST_TBL CURR " + "WHERE SRR.PORT_MST_POL_FK       = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK       = POD.PORT_MST_PK " + " AND FRT.SRR_TRN_SEA_FK(+) = SRR.SRR_TRN_SEA_PK   " + "AND   SRR.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK " + "AND   SRR.CURRENCY_MST_FK       = CURR.CURRENCY_MST_PK " + "AND   SRR.SRR_SEA_FK            = " + SrrPk + "GROUP BY POL.PORT_MST_PK, " + "POL.PORT_ID, " + "POD.PORT_MST_PK, " + "POD.PORT_ID, " + "CONT.CONTAINER_TYPE_MST_ID, " + "CURR.CURRENCY_ID, " + "SRR.CURRENT_BOF_RATE, " + "SRR.CURRENT_ALL_IN_RATE, " + "SRR.REQUESTED_BOF_RATE, " + "SRR.ON_THL_OR_THD, " + "SRR.EXPECTED_BOXES, " + "TO_CHAR(SRR.VALID_FROM, 'dd/MM/yyyy'), " + "TO_CHAR(SRR.VALID_TO, 'dd/MM/yyyy'), " + "SRR.CONTAINER_TYPE_MST_FK, " + "SRR.CURRENCY_MST_FK, " + "SRR.SUBJECT_TO_SURCHG_CHG, " + "SRR.SRR_TRN_SEA_PK, " + "CONT.PREFERENCES " + "   ORDER BY CONT.PREFERENCES) Q";
                        //"     GROUP BY POL.PORT_MST_PK , " & vbCrLf & _
                        //"  POL.PORT_ID,POD.PORT_MST_PK, " & vbCrLf & _
                        //" POD.PORT_ID,CONT.CONTAINER_TYPE_MST_ID,CURR.CURRENCY_ID, " & vbCrLf & _
                        //" SRR.CURRENT_BOF_RATE,SRR.CURRENT_ALL_IN_RATE,SRR.REQUESTED_BOF_RATE, " & vbCrLf & _
                        //" SRR.EXPECTED_BOXES,SRR.ON_THL_OR_THD,SRR.SUBJECT_TO_SURCHG_CHG, " & vbCrLf & _
                        //" SRR.VALID_FROM,SRR.VALID_TO,SRR.CONTAINER_TYPE_MST_FK, " & vbCrLf & _
                        //"  SRR.CURRENCY_MST_FK,SRR.SRR_TRN_SEA_PK,CONT.PREFERENCES ORDER BY CONT.PREFERENCES) Q "
                    }
                    else
                    {
                        strSql = "SELECT DISTINCT " + "      POLGRP.PORT_GRP_MST_PK AS POLPK,POLGRP.PORT_GRP_ID AS POLID, " + "      PODGRP.PORT_GRP_MST_PK AS PODPK,PODGRP.PORT_GRP_ID AS PODID, " + "      CONT.CONTAINER_TYPE_MST_ID CONT_BASIS,  " + "      CURR.CURRENCY_ID, SRR.CURRENT_BOF_RATE,  " + "      SRR.CURRENT_ALL_IN_RATE, SRR.REQUESTED_BOF_RATE, " + "    (SUM(FRT.REQ_SURCHARGE_AMT * get_ex_rate( FRT.CURRENCY_MST_FK,173, SYSDATE))+ SRR.REQUESTED_BOF_RATE)REQUESTED_ALL_IN_RATE, " + "    ((SRR.CURRENT_ALL_IN_RATE - " + "                (SRR.CURRENT_BOF_RATE *  " + "                GET_EX_RATE(FRT.CURRENCY_MST_FK, " + baseCurrency + ", SYSDATE))) + " + "                SRR.REQUESTED_BOF_RATE * " + "                GET_EX_RATE(FRT.CURRENCY_MST_FK, " + baseCurrency + ", SYSDATE))REQUESTED_ALL_IN_RATE, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (1,3)) THEN '1' ELSE '0' END) AS THL, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (2,3)) THEN '1' ELSE '0' END) AS THD, " + "      SRR.EXPECTED_BOXES, TO_CHAR(SRR.VALID_FROM,'" + dateFormat + "') VALID_FROM, " + "      TO_CHAR(SRR.VALID_TO,'" + dateFormat + "') VALID_TO, SRR.CONTAINER_TYPE_MST_FK, " + "      SRR.CURRENCY_MST_FK, " + "      (CASE WHEN SRR.SUBJECT_TO_SURCHG_CHG =1 THEN '1' ELSE '0' END) AS SURCHARGE, " + "      '0' DEL,0 SRR_TRN_SEA_PK " + "FROM " + "      SRR_TRN_SEA_TBL SRR, " + "      SRR_SUR_CHRG_SEA_TBL FRT, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, PORT_GRP_MST_TBL       POLGRP, PORT_GRP_MST_TBL       PODGRP, " + "      CONTAINER_TYPE_MST_TBL CONT, " + "      CURRENCY_TYPE_MST_TBL CURR " + "WHERE SRR.PORT_MST_POL_FK       = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK       = POD.PORT_MST_PK AND SRR.POL_GRP_FK = POLGRP.PORT_GRP_MST_PK AND SRR.POD_GRP_FK = PODGRP.PORT_GRP_MST_PK " + " AND FRT.SRR_TRN_SEA_FK = SRR.SRR_TRN_SEA_PK   " + "AND   SRR.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK " + "AND   SRR.CURRENCY_MST_FK       = CURR.CURRENCY_MST_PK " + "AND   SRR.SRR_SEA_FK            = " + SrrPk;
                        //"     GROUP BY POL.PORT_MST_PK , " & vbCrLf & _
                        //"  POL.PORT_ID,POD.PORT_MST_PK, " & vbCrLf & _
                        //" POD.PORT_ID,POLGRP.PORT_GRP_MST_PK, POLGRP.PORT_GRP_ID, PODGRP.PORT_GRP_MST_PK, PODGRP.PORT_GRP_ID, CONT.CONTAINER_TYPE_MST_ID,CURR.CURRENCY_ID, " & vbCrLf & _
                        //" SRR.CURRENT_BOF_RATE,SRR.CURRENT_ALL_IN_RATE,SRR.REQUESTED_BOF_RATE, " & vbCrLf & _
                        //" SRR.EXPECTED_BOXES,SRR.ON_THL_OR_THD,SRR.SUBJECT_TO_SURCHG_CHG, " & vbCrLf & _
                        //" SRR.VALID_FROM,SRR.VALID_TO,SRR.CONTAINER_TYPE_MST_FK, " & vbCrLf & _
                        //"  SRR.CURRENCY_MST_FK,SRR.SRR_TRN_SEA_PK "
                    }
                }
                else
                {
                    // "    (SUM(FRT.REQ_SURCHARGE_AMT * get_ex_rate( FRT.CURRENCY_MST_FK,173, SYSDATE))+ SRR.REQUESTED_BOF_RATE)REQUESTED_ALL_IN_RATE1, " & vbCrLf & _
                    //'Commented for DTS:9767
                    if (Convert.ToInt32(dtMain.Rows[0]["PORT_GROUP"]) == 0)
                    {
                        strSql = "SELECT DISTINCT " + "      POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POLID, " + "      POD.PORT_MST_PK AS PODPK,POD.PORT_ID AS PODID, " + "      UOM.DIMENTION_ID CONT_BASIS, " + "      CURR.CURRENCY_ID, SRR.CURRENT_BOF_RATE, " + "      SRR.CURRENT_ALL_IN_RATE, SRR.REQUESTED_BOF_RATE, " + "    SUM((SRR.CURRENT_ALL_IN_RATE - " + "                (SRR.CURRENT_BOF_RATE *  " + "                GET_EX_RATE(FRT.CURRENCY_MST_FK, " + baseCurrency + ", SYSDATE))) + " + "                SRR.REQUESTED_BOF_RATE * " + "                GET_EX_RATE(FRT.CURRENCY_MST_FK, " + baseCurrency + ", SYSDATE))REQUESTED_ALL_IN_RATE, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (1,3)) THEN '1' ELSE '0' END) AS THL, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (2,3)) THEN '1' ELSE '0' END) AS THD, " + "      SRR.EXPECTED_VOLUME, TO_CHAR(SRR.VALID_FROM,'" + dateFormat + "') VALID_FROM, " + "      TO_CHAR(SRR.VALID_TO,'" + dateFormat + "') VALID_TO, SRR.LCL_BASIS, " + "      SRR.CURRENCY_MST_FK, " + "      (CASE WHEN SRR.SUBJECT_TO_SURCHG_CHG =1 THEN '1' ELSE '0' END) AS SURCHARGE, " + "      '0' DEL,SRR.SRR_TRN_SEA_PK " + "FROM " + "      SRR_TRN_SEA_TBL SRR, " + "       SRR_SUR_CHRG_SEA_TBL FRT, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR " + "WHERE SRR.PORT_MST_POL_FK       = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK       = POD.PORT_MST_PK " + " AND FRT.SRR_TRN_SEA_FK(+) = SRR.SRR_TRN_SEA_PK   " + "AND   SRR.LCL_BASIS             = UOM.DIMENTION_UNIT_MST_PK " + "AND   SRR.CURRENCY_MST_FK       = CURR.CURRENCY_MST_PK " + "AND   SRR.SRR_SEA_FK            = " + SrrPk + "GROUP BY POL.PORT_MST_PK, " + "POL.PORT_ID, " + "POD.PORT_MST_PK, " + "POD.PORT_ID, " + "UOM.DIMENTION_ID, SRR.EXPECTED_VOLUME, SRR.LCL_BASIS, " + "CURR.CURRENCY_ID, " + "SRR.CURRENT_BOF_RATE, " + "SRR.CURRENT_ALL_IN_RATE, " + "SRR.REQUESTED_BOF_RATE, " + "SRR.ON_THL_OR_THD, " + "SRR.EXPECTED_BOXES, " + "TO_CHAR(SRR.VALID_FROM, 'dd/MM/yyyy'), " + "TO_CHAR(SRR.VALID_TO, 'dd/MM/yyyy'), " + "SRR.CONTAINER_TYPE_MST_FK, " + "SRR.CURRENCY_MST_FK, " + "SRR.SUBJECT_TO_SURCHG_CHG, " + "SRR.SRR_TRN_SEA_PK";

                        //" GROUP BY POL.PORT_MST_PK , " & vbCrLf & _
                        //"  POL.PORT_ID,POD.PORT_MST_PK, " & vbCrLf & _
                        //" POD.PORT_ID, UOM.DIMENTION_ID, SRR.EXPECTED_VOLUME, SRR.LCL_BASIS, CURR.CURRENCY_ID, " & vbCrLf & _
                        //" SRR.CURRENT_BOF_RATE,SRR.CURRENT_ALL_IN_RATE,SRR.REQUESTED_BOF_RATE, " & vbCrLf & _
                        //" SRR.EXPECTED_BOXES,SRR.ON_THL_OR_THD,SRR.SUBJECT_TO_SURCHG_CHG, " & vbCrLf & _
                        //" SRR.VALID_FROM,SRR.VALID_TO,SRR.CONTAINER_TYPE_MST_FK, " & vbCrLf & _
                        //"  SRR.CURRENCY_MST_FK,SRR.SRR_TRN_SEA_PK "
                    }
                    else
                    {
                        strSql = "SELECT DISTINCT " + "      POLGRP.PORT_GRP_MST_PK AS POLPK,POLGRP.PORT_GRP_ID AS POLID, " + "      PODGRP.PORT_GRP_MST_PK AS PODPK,PODGRP.PORT_GRP_ID AS PODID, " + "      UOM.DIMENTION_ID CONT_BASIS, " + "      CURR.CURRENCY_ID, SRR.CURRENT_BOF_RATE, " + "      SRR.CURRENT_ALL_IN_RATE, SRR.REQUESTED_BOF_RATE, " + "    (SUM(FRT.REQ_SURCHARGE_AMT * get_ex_rate( FRT.CURRENCY_MST_FK,173, SYSDATE))+ SRR.REQUESTED_BOF_RATE)REQUESTED_ALL_IN_RATE1, " + "    ((SRR.CURRENT_ALL_IN_RATE - " + "                (SRR.CURRENT_BOF_RATE *  " + "                GET_EX_RATE(FRT.CURRENCY_MST_FK, " + baseCurrency + ", SYSDATE))) + " + "                SRR.REQUESTED_BOF_RATE * " + "                GET_EX_RATE(FRT.CURRENCY_MST_FK, " + baseCurrency + ", SYSDATE)) REQUESTED_ALL_IN_RATE," + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (1,3)) THEN '1' ELSE '0' END) AS THL, " + "      (CASE WHEN (SRR.ON_THL_OR_THD IN (2,3)) THEN '1' ELSE '0' END) AS THD, " + "      SRR.EXPECTED_VOLUME, TO_CHAR(SRR.VALID_FROM,'" + dateFormat + "') VALID_FROM, " + "      TO_CHAR(SRR.VALID_TO,'" + dateFormat + "') VALID_TO, SRR.LCL_BASIS, " + "      SRR.CURRENCY_MST_FK, " + "      (CASE WHEN SRR.SUBJECT_TO_SURCHG_CHG =1 THEN '1' ELSE '0' END) AS SURCHARGE, " + "      '0' DEL, 0 SRR_TRN_SEA_PK " + "FROM " + "      SRR_TRN_SEA_TBL SRR, " + "       SRR_SUR_CHRG_SEA_TBL FRT, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, PORT_GRP_MST_TBL       POLGRP, PORT_GRP_MST_TBL       PODGRP, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR " + "WHERE SRR.PORT_MST_POL_FK       = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK       = POD.PORT_MST_PK AND SRR.POL_GRP_FK = POLGRP.PORT_GRP_MST_PK AND SRR.POD_GRP_FK = PODGRP.PORT_GRP_MST_PK " + " AND FRT.SRR_TRN_SEA_FK(+) = SRR.SRR_TRN_SEA_PK   " + "AND   SRR.LCL_BASIS             = UOM.DIMENTION_UNIT_MST_PK " + "AND   SRR.CURRENCY_MST_FK       = CURR.CURRENCY_MST_PK " + "AND   SRR.SRR_SEA_FK            = " + SrrPk;
                    }
                }

                dsGrid.Tables.Add(objWF.GetDataTable(strSql));
                dsGrid.Tables[0].TableName = "Main";
                strSql = "";
                if (!IsLCL)
                {
                    if (Convert.ToInt32(dtMain.Rows[0]["PORT_GROUP"]) == 0)
                    {
                        strSql = "SELECT  " + "      POL.PORT_MST_PK AS POLPK, POD.PORT_MST_PK AS PODPK, " + "      FRT.FREIGHT_ELEMENT_ID, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "      CURR.CURRENCY_ID, CONT.CONTAINER_TYPE_MST_ID AS CONT_BASIS, " + "      SUR.CURR_SURCHARGE_AMT, SUR.REQ_SURCHARGE_AMT, SUR.CURRENCY_MST_FK, " + "      SUR.FREIGHT_ELEMENT_MST_FK,SUR.SRR_SUR_CHRG_SEA_PK,SUR.SL_CONTRACT_RATE " + "FROM " + "      SRR_TRN_SEA_TBL SRR,  " + "      SRR_SUR_CHRG_SEA_TBL SUR, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      CONTAINER_TYPE_MST_TBL CONT, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + "WHERE SRR.SRR_TRN_SEA_PK          = SUR.SRR_TRN_SEA_FK(+) " + "AND   SRR.CONTAINER_TYPE_MST_FK   = CONT.CONTAINER_TYPE_MST_PK " + "AND   SRR.PORT_MST_POL_FK         = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK         = POD.PORT_MST_PK " + "AND   SUR.CURRENCY_MST_FK         = CURR.CURRENCY_MST_PK " + "AND   SUR.FREIGHT_ELEMENT_MST_FK  = FRT.FREIGHT_ELEMENT_MST_PK " + "AND   SRR.SRR_SEA_FK              = " + SrrPk + " ORDER BY FRT.PREFERENCE";
                    }
                    else
                    {
                        strSql = "SELECT DISTINCT * FROM (SELECT  " + "      SRR.POL_GRP_FK AS POLPK, SRR.POD_GRP_FK AS PODPK, " + "      FRT.FREIGHT_ELEMENT_ID, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "      CURR.CURRENCY_ID, CONT.CONTAINER_TYPE_MST_ID AS CONT_BASIS, " + "      SUR.CURR_SURCHARGE_AMT, SUR.REQ_SURCHARGE_AMT, SUR.CURRENCY_MST_FK, " + "      SUR.FREIGHT_ELEMENT_MST_FK,0 SRR_SUR_CHRG_SEA_PK,SUR.SL_CONTRACT_RATE " + "FROM " + "      SRR_TRN_SEA_TBL SRR,  " + "      SRR_SUR_CHRG_SEA_TBL SUR, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      CONTAINER_TYPE_MST_TBL CONT, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + "WHERE SRR.SRR_TRN_SEA_PK          = SUR.SRR_TRN_SEA_FK(+) " + "AND   SRR.CONTAINER_TYPE_MST_FK   = CONT.CONTAINER_TYPE_MST_PK " + "AND   SRR.PORT_MST_POL_FK         = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK         = POD.PORT_MST_PK " + "AND   SUR.CURRENCY_MST_FK         = CURR.CURRENCY_MST_PK " + "AND   SUR.FREIGHT_ELEMENT_MST_FK  = FRT.FREIGHT_ELEMENT_MST_PK " + "AND   SRR.SRR_SEA_FK              = " + SrrPk + " ORDER BY FRT.PREFERENCE)";
                    }
                }
                else
                {
                    if (Convert.ToInt32(dtMain.Rows[0]["PORT_GROUP"]) == 0)
                    {
                        strSql = "SELECT " + "      POL.PORT_MST_PK AS POLPK, POD.PORT_MST_PK AS PODPK, " + "      FRT.FREIGHT_ELEMENT_ID, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "      CURR.CURRENCY_ID, UOM.DIMENTION_ID AS CONT_BASIS, " + "      SUR.CURR_SURCHARGE_AMT, SUR.REQ_SURCHARGE_AMT, SUR.CURRENCY_MST_FK, " + "      SUR.FREIGHT_ELEMENT_MST_FK,SUR.SRR_SUR_CHRG_SEA_PK,SUR.SL_CONTRACT_RATE  " + "FROM " + "      SRR_TRN_SEA_TBL SRR, " + "      SRR_SUR_CHRG_SEA_TBL SUR, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + "WHERE SRR.SRR_TRN_SEA_PK          = SUR.SRR_TRN_SEA_FK " + "AND   SRR.LCL_BASIS               = UOM.DIMENTION_UNIT_MST_PK " + "AND   SRR.PORT_MST_POL_FK         = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK         = POD.PORT_MST_PK " + "AND   SUR.CURRENCY_MST_FK         = CURR.CURRENCY_MST_PK " + "AND   SUR.FREIGHT_ELEMENT_MST_FK  = FRT.FREIGHT_ELEMENT_MST_PK " + "AND   SRR.SRR_SEA_FK              = " + SrrPk + " ORDER BY FRT.PREFERENCE";
                    }
                    else
                    {
                        strSql = "SELECT DISTINCT * FROM (SELECT " + "      SRR.POL_GRP_FK AS POLPK, SRR.POD_GRP_FK AS PODPK, " + "      FRT.FREIGHT_ELEMENT_ID, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "      CURR.CURRENCY_ID, UOM.DIMENTION_ID AS CONT_BASIS, " + "      SUR.CURR_SURCHARGE_AMT, SUR.REQ_SURCHARGE_AMT, SUR.CURRENCY_MST_FK, " + "      SUR.FREIGHT_ELEMENT_MST_FK,0 SRR_SUR_CHRG_SEA_PK,SUR.SL_CONTRACT_RATE  " + "FROM " + "      SRR_TRN_SEA_TBL SRR, " + "      SRR_SUR_CHRG_SEA_TBL SUR, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + "WHERE SRR.SRR_TRN_SEA_PK          = SUR.SRR_TRN_SEA_FK " + "AND   SRR.LCL_BASIS               = UOM.DIMENTION_UNIT_MST_PK " + "AND   SRR.PORT_MST_POL_FK         = POL.PORT_MST_PK " + "AND   SRR.PORT_MST_POD_FK         = POD.PORT_MST_PK " + "AND   SUR.CURRENCY_MST_FK         = CURR.CURRENCY_MST_PK " + "AND   SUR.FREIGHT_ELEMENT_MST_FK  = FRT.FREIGHT_ELEMENT_MST_PK " + "AND   SRR.SRR_SEA_FK              = " + SrrPk + " ORDER BY FRT.PREFERENCE)";
                    }
                }
                dsGrid.Tables.Add(objWF.GetDataTable(strSql));
                dsGrid.Tables[1].TableName = "Frt";

                //Making relation  between Main and Frt table of dsGrid
                //Relation between:
                //                 Main Table            Frt Table
                //                 ---------             ---------
                //                 1. POLPK              1. POLPK
                //                 2. PODPK              2. PODPK
                //                 3. CONT_BASIS          3. CONT_BASIS

                DataColumn[] dcParent = null;
                DataColumn[] dcChild = null;
                DataRelation re = null;

                dcParent = new DataColumn[] {
                dsGrid.Tables["Main"].Columns["POLPK"],
                dsGrid.Tables["Main"].Columns["PODPK"],
                dsGrid.Tables["Main"].Columns["CONT_BASIS"]
            };

                dcChild = new DataColumn[] {
                dsGrid.Tables["Frt"].Columns["POLPK"],
                dsGrid.Tables["Frt"].Columns["PODPK"],
                dsGrid.Tables["Frt"].Columns["CONT_BASIS"]
            };

                re = new DataRelation("rl_Port", dcParent, dcChild);
                //Adding relation to the grid.
                dsGrid.Relations.Add(re);
            }
            catch (OracleException oraExp)
            {
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Queries"

        #region "Save"

        public ArrayList SaveHDR(DataSet dsMain, object txtSRRRefNo, long nLocationId, long nEmpId, string Mode, bool IsLcl, string RefNr, int status, string Remarks = "", string SrrDt = "",
        Int16 Restricted = 0, string sid = "", string polid = "", string podid = "")
        {
            string SrrRefNo = null;
            WorkFlow objWK = new WorkFlow();
            // Dim status As Integer = 0

            OracleTransaction TRAN = default(OracleTransaction);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            bool IsUpdate = false;
            try
            {
                if (string.IsNullOrEmpty(txtSRRRefNo.ToString()))
                {
                    SrrRefNo = GenerateRFQNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, sid, polid, podid);
                    if (SrrRefNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                    }
                }
                else
                {
                    SrrRefNo = txtSRRRefNo.ToString();
                }
                objWK.MyCommand.Parameters.Clear();

                var _with3 = objWK.MyCommand;

                if (Mode == "NEW" | Mode == "FETCHED")
                {
                    //*******************************************************************************************
                    //******************************MODE = "NEW" OR "FETCHED"************************************
                    //*******************************************************************************************
                    _with3.CommandType = CommandType.StoredProcedure;
                    _with3.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_SEA_TBL_INS";

                    _with3.Parameters.Add("TARIFF_MAIN_SEA_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["TARIFF_MAIN_SEA_FK"])).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("SRR_REF_NO_IN", SrrRefNo).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("REF_NR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("STATUS_IN", status).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["BASE_CURRENCY_FK"].ToString()))
                    {
                        _with3.Parameters.Add("BASE_CURRENCY_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with3.Parameters.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["BASE_CURRENCY_FK"])).Direction = ParameterDirection.Input;
                    }
                }
                else
                {
                    //*******************************************************************************************
                    //************************************MODE = "EDIT"******************************************
                    //*******************************************************************************************
                    _with3.CommandType = CommandType.StoredProcedure;
                    _with3.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_SEA_TBL_UPD";

                    _with3.Parameters.Add("SRR_SEA_PK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["SRR_SEA_PK"])).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    _with3.Parameters.Add("VERSION_NO_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["VERSION_NO"])).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
                }

                //***********************************************************************************************
                //********************************COMMON TO INSERT AND UPDATE************************************
                //***********************************************************************************************

                //.Parameters.Add("OPERATOR_MST_FK_IN", _
                //CLng(dsMain.Tables("Master").Rows(0).Item("OPERATOR_MST_FK"))).Direction = _
                //ParameterDirection.Input
                if (!string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["OPERATOR_MST_FK"].ToString()))
                {
                    if (Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["OPERATOR_MST_FK"]) == 0)
                    {
                        _with3.Parameters.Add("OPERATOR_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with3.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                }
                else
                {
                    _with3.Parameters.Add("OPERATOR_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }

                _with3.Parameters.Add("CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("VALID_FROM_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["VALID_FROM"])).Direction = ParameterDirection.Input;

                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["VALID_TO"].ToString()))
                {
                    _with3.Parameters.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with3.Parameters.Add("VALID_TO_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["VALID_TO"])).Direction = ParameterDirection.Input;
                }

                _with3.Parameters.Add("STRCONDITION", Convert.ToString(dsMain.Tables["Master"].Rows[0]["STRCONDITION"])).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["COMMODITY_GROUP_MST_FK"])).Direction = ParameterDirection.Input;

                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["COMMODITY_MST_FK"].ToString()))
                {
                    _with3.Parameters.Add("COMMODITY_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with3.Parameters.Add("COMMODITY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["SRR_CLAUSE"].ToString()))
                {
                    _with3.Parameters.Add("SRR_CLAUSE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with3.Parameters.Add("SRR_CLAUSE_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["SRR_CLAUSE"])).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["SRR_REMARKS"].ToString()))
                {
                    _with3.Parameters.Add("SRR_REMARKS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with3.Parameters.Add("SRR_REMARKS_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["SRR_REMARKS"])).Direction = ParameterDirection.Input;
                }

                //ADDED BY NIPPY ON 27/03/2006
                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["COLLECTION_ADDRESS"].ToString()))
                {
                    _with3.Parameters.Add("SRR_COLLECTION_ADDRESS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with3.Parameters.Add("SRR_COLLECTION_ADDRESS_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["COLLECTION_ADDRESS"])).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["PYMT_LOCATION_MST_FK"].ToString()))
                {
                    _with3.Parameters.Add("PYMT_LOCATION_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with3.Parameters.Add("PYMT_LOCATION_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["PYMT_LOCATION_MST_FK"])).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["CREDIT_PERIOD"].ToString()))
                {
                    _with3.Parameters.Add("CREDIT_PERIOD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with3.Parameters.Add("CREDIT_PERIOD_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["CREDIT_PERIOD"])).Direction = ParameterDirection.Input;
                }

                _with3.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                //Added by Manoj K Sethi for Saving Active checkbox
                _with3.Parameters.Add("ACTIVE_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["ACTIVE"])).Direction = ParameterDirection.Input;
                //end

                _with3.Parameters.Add("PORT_GROUP_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["PORT_GROUP"])).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("RESTRICTED_IN", Restricted).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("SRR_TYPE_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["SRR_TYPE"])).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;

                _with3.ExecuteNonQuery();

                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "SRR") > 0 | string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "modified") > 0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    //Added by minakshi on 16-feb-09 for protocol rollbacking
                    if (!IsUpdate)
                    {
                        RollbackProtocolKey("SRR SEA", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), SrrRefNo, System.DateTime.Now);
                    }
                    //Ended by minakshi
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }

                arrMessage = SaveSrrTRN(dsMain, objWK, IsLcl);
                //'
                string CurrFKs = "0";
                cls_Operator_Contract objContract = new cls_Operator_Contract();
                for (int nRowCnt = 0; nRowCnt <= dsMain.Tables["Surcharge"].Rows.Count - 1; nRowCnt++)
                {
                    CurrFKs += "," + dsMain.Tables["Surcharge"].Rows[nRowCnt]["CURRENCY_MST_FK"];
                }

                if ((HttpContext.Current.Session["SessionLocalCharges"] != null))
                {
                    SaveLocalCharges(objWK.MyCommand, objWK.MyUserName, (DataSet)HttpContext.Current.Session["SessionLocalCharges"], _PkValue, 2);
                }

                objContract.UpdateTempExRate(_PkValue, objWK, CurrFKs, Convert.ToDateTime(SrrDt), "SRRSEA");
                //'

                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        txtSRRRefNo = SrrRefNo;
                        return arrMessage;
                    }
                    else
                    {
                        //Added by minakshi on 16-feb-09 for protocol rollbacking
                        if (!IsUpdate)
                        {
                            RollbackProtocolKey("SRR SEA", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), SrrRefNo, System.DateTime.Now);
                        }
                        //Ended by minakshi
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                //Added by minakshi on 16-feb-09 for protocol rollbacking
                if (!IsUpdate)
                {
                    RollbackProtocolKey("SRR SEA", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), SrrRefNo, System.DateTime.Now);
                }
                //Ended by minakshi
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
            return new ArrayList();
        }

        private ArrayList SaveSrrTRN(DataSet dsMain, WorkFlow objWK, bool IsLCL)
        {
            Int32 nTransactionRowCnt = default(Int32);
            DataTable dtTransaction = new DataTable();
            long nTransactionPk = 0;
            dtTransaction = dsMain.Tables["Transaction"];
            bool IsUpdate = false;
            string Cont_BasisPk = null;
            if (IsLCL)
            {
                Cont_BasisPk = "LCL_BASIS";
            }
            else
            {
                Cont_BasisPk = "CONTAINER_TYPE_MST_FK";
            }
            try
            {
                for (nTransactionRowCnt = 0; nTransactionRowCnt <= dsMain.Tables["Transaction"].Rows.Count - 1; nTransactionRowCnt++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    arrMessage.Clear();
                    nTransactionPk = 0;

                    var _with4 = objWK.MyCommand;
                    if (Convert.ToInt32(dtTransaction.Rows[nTransactionRowCnt]["DEL"]) < 1)
                    {
                        if (Convert.ToInt32(dsMain.Tables["Transaction"].Rows[nTransactionRowCnt]["SRR_TRN_PK"]) <= 0)
                        {
                            //**********************************INSERT********************************************
                            //************************************************************************************
                            IsUpdate = false;
                            _with4.CommandType = CommandType.StoredProcedure;
                            _with4.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_TRN_SEA_TBL_INS";

                            _with4.Parameters.Add("SRR_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;

                            _with4.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;

                            _with4.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;

                            _with4.Parameters.Add("POL_GRP_FK_IN", dtTransaction.Rows[nTransactionRowCnt]["POL_GRP_FK"]).Direction = ParameterDirection.Input;

                            _with4.Parameters.Add("POD_GRP_FK_IN", dtTransaction.Rows[nTransactionRowCnt]["POD_GRP_FK"]).Direction = ParameterDirection.Input;

                            _with4.Parameters.Add("TARIFF_GRP_FK_IN", dtTransaction.Rows[nTransactionRowCnt]["TARIFF_GRP_FK"]).Direction = ParameterDirection.Input;

                            if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["CONTAINER_TYPE_MST_FK"].ToString()))
                            {
                                _with4.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with4.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["CONTAINER_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
                            }

                            if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["LCL_BASIS"].ToString()))
                            {
                                _with4.Parameters.Add("LCL_BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with4.Parameters.Add("LCL_BASIS_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["LCL_BASIS"])).Direction = ParameterDirection.Input;
                            }

                            _with4.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;

                            if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["CURRENT_BOF_RATE"].ToString()))
                            {
                                _with4.Parameters.Add("CURRENT_BOF_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with4.Parameters.Add("CURRENT_BOF_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["CURRENT_BOF_RATE"])).Direction = ParameterDirection.Input;
                            }

                            if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["CURRENT_ALL_IN_RATE"].ToString()))
                            {
                                _with4.Parameters.Add("CURRENT_ALL_IN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with4.Parameters.Add("CURRENT_ALL_IN_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["CURRENT_ALL_IN_RATE"])).Direction = ParameterDirection.Input;
                            }
                        }
                        else
                        {
                            //**********************************UPDATE********************************************
                            //************************************************************************************
                            IsUpdate = true;
                            _with4.CommandType = CommandType.StoredProcedure;
                            _with4.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_TRN_SEA_TBL_UPD";

                            _with4.Parameters.Add("SRR_TRN_SEA_PK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["SRR_TRN_PK"])).Direction = ParameterDirection.Input;

                            _with4.Parameters.Add("APPROVED_ALL_IN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                            _with4.Parameters.Add("APPROVED_BOF_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        //********************************COMMON TO INSERT AND UPDATE************************************
                        //***********************************************************************************************
                        _with4.Parameters.Add("VALID_FROM_IN", Convert.ToString(dtTransaction.Rows[nTransactionRowCnt]["VALID_FROM"])).Direction = ParameterDirection.Input;

                        if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["VALID_TO"].ToString()))
                        {
                            _with4.Parameters.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Parameters.Add("VALID_TO_IN", Convert.ToString(dtTransaction.Rows[nTransactionRowCnt]["VALID_TO"])).Direction = ParameterDirection.Input;
                        }

                        if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_BOF_RATE"].ToString()))
                        {
                            _with4.Parameters.Add("REQUESTED_BOF_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Parameters.Add("REQUESTED_BOF_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_BOF_RATE"])).Direction = ParameterDirection.Input;
                        }

                        if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_ALL_IN_RATE"].ToString()))
                        {
                            _with4.Parameters.Add("REQUESTED_ALL_IN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Parameters.Add("REQUESTED_ALL_IN_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_ALL_IN_RATE"])).Direction = ParameterDirection.Input;
                        }

                        _with4.Parameters.Add("ON_THL_OR_THD_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["ON_THL_OR_THD"])).Direction = ParameterDirection.Input;

                        if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_VOLUME"].ToString()))
                        {
                            _with4.Parameters.Add("EXPECTED_VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Parameters.Add("EXPECTED_VOLUME_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_VOLUME"])).Direction = ParameterDirection.Input;
                        }

                        if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_BOXES"].ToString()))
                        {
                            _with4.Parameters.Add("EXPECTED_BOXES_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with4.Parameters.Add("EXPECTED_BOXES_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_BOXES"])).Direction = ParameterDirection.Input;
                        }

                        _with4.Parameters.Add("SUBJECT_TO_SURCHG_CHG_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["SUBJECT_TO_SURCHG_CHG"])).Direction = ParameterDirection.Input;

                        _with4.Parameters.Add("OPERATOR_SPEC_SURCRG_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["OPERATOR_SPEC_SURCRG"])).Direction = ParameterDirection.Input;

                        _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with4.ExecuteNonQuery();

                        nTransactionPk = Convert.ToInt64(_with4.Parameters["RETURN_VALUE"].Value);
                        arrMessage = SaveSrrSurcharge(dsMain, objWK, nTransactionPk, IsUpdate, Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POL_FK"]), Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POD_FK"]), Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt][Cont_BasisPk]));

                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(dsMain.Tables["Transaction"].Rows[nTransactionRowCnt]["SRR_TRN_PK"]) > 0)
                        {
                            //**********************************DELETE********************************************
                            //************************************************************************************
                            IsUpdate = false;

                            _with4.CommandType = CommandType.StoredProcedure;
                            _with4.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_TRN_SEA_TBL_DEL";

                            _with4.Parameters.Add("SRR_TRN_SEA_PK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["SRR_TRN_PK"])).Direction = ParameterDirection.Input;

                            _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                            _with4.ExecuteNonQuery();
                            if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "deleted") > 0)
                            {
                                arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                            }
                        }
                    }
                }

                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        private ArrayList SaveSrrSurcharge(DataSet dsMain, WorkFlow objWK, long TransactionPkValue, bool IsUpdate, long PolPk, long PodPk, long CONT_BASIS)
        {
            Int32 nSurchargeRowCnt = default(Int32);
            DataView dv_Surcharge = new DataView();

            dv_Surcharge = getDataView(dsMain.Tables["Surcharge"], PolPk, PodPk, CONT_BASIS);

            arrMessage.Clear();

            try
            {
                for (nSurchargeRowCnt = 0; nSurchargeRowCnt <= dv_Surcharge.Table.Rows.Count - 1; nSurchargeRowCnt++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    var _with5 = objWK.MyCommand;
                    if (!IsUpdate)
                    {
                        //**********************************INSERT********************************************
                        _with5.CommandType = CommandType.StoredProcedure;
                        _with5.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_SUR_CHRG_SEA_TBL_INS";

                        _with5.Parameters.Add("SRR_TRN_SEA_FK_IN", TransactionPkValue).Direction = ParameterDirection.Input;

                        _with5.Parameters.Add("FREIGHT_ELMENT_MST_FK_IN", Convert.ToInt64(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["FREIGHT_ELEMENT_MST_FK"])).Direction = ParameterDirection.Input;

                        _with5.Parameters.Add("CURR_SURCHARGE_AMT_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CURR_SURCHARGE_AMT"])).Direction = ParameterDirection.Input;

                        _with5.Parameters.Add("SURCHARGE_IN", dv_Surcharge.Table.Rows[nSurchargeRowCnt]["SURCHARGE"]).Direction = ParameterDirection.Input;

                        if (!string.IsNullOrEmpty(Convert.ToString(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CONTARCTRATE"])))
                        {
                            _with5.Parameters.Add("SL_CONTARCT_RATE_IN", Convert.ToInt32(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CONTARCTRATE"])).Direction = ParameterDirection.Input;
                            //'
                        }
                        else
                        {
                            _with5.Parameters.Add("SL_CONTARCT_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            //'
                        }
                    }
                    else
                    {
                        //**********************************UPDATE********************************************
                        _with5.CommandType = CommandType.StoredProcedure;
                        _with5.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_SUR_CHRG_SEA_TBL_UPD";

                        _with5.Parameters.Add("SRR_SUR_CHRG_SEA_PK_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["SRR_SUR_CHRG_SEA_PK"])).Direction = ParameterDirection.Input;
                        _with5.Parameters.Add("APP_SURCHARGE_AMT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with5.Parameters.Add("SURCHARGE_IN", dv_Surcharge.Table.Rows[nSurchargeRowCnt]["SURCHARGE"]).Direction = ParameterDirection.Input;

                        if (!string.IsNullOrEmpty(Convert.ToString(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CONTARCTRATE"])))
                        {
                            _with5.Parameters.Add("SL_CONTARCT_RATE_IN", Convert.ToInt32(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CONTARCTRATE"])).Direction = ParameterDirection.Input;
                            //'
                        }
                        else
                        {
                            _with5.Parameters.Add("SL_CONTARCT_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            //'
                        }
                    }

                    _with5.Parameters.Add("REQ_SURCHARGE_AMT_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["REQ_SURCHARGE_AMT"])).Direction = ParameterDirection.Input;

                    _with5.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", Convert.ToInt64(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CHECK_FOR_ALL_IN_RT"])).Direction = ParameterDirection.Input;

                    _with5.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;

                    _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with5.ExecuteNonQuery();
                }

                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        //This function generates the SRR Referrence no. as per the protocol saved by the user.
        public string GenerateRFQNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK, string SID = "", string POLID = "", string PODID = "")
        {
            string functionReturnValue = null;
            try
            {
                functionReturnValue = GenerateProtocolKey("SRR SEA", nLocationId, nEmployeeId, DateTime.Now, "", "", POLID, nCreatedBy, objWK, SID,
                PODID);
                return functionReturnValue;
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
            return functionReturnValue;
        }

        private DataView getDataView(DataTable dtSurcharge, long POLPK, long PODPK, long CONT_BASIS)
        {
            DataTable dstemp = new DataTable();
            DataRow dr = null;
            Int32 nRowCnt = default(Int32);
            Int32 nColCnt = default(Int32);
            string Cont_BasisPk = null;
            try
            {
                dstemp = dtSurcharge.Clone();
                for (nRowCnt = 0; nRowCnt <= dtSurcharge.Rows.Count - 1; nRowCnt++)
                {
                    if (POLPK == Convert.ToInt64(dtSurcharge.Rows[nRowCnt]["PORT_MST_POL_FK"]) & PODPK == Convert.ToInt64(dtSurcharge.Rows[nRowCnt]["PORT_MST_POD_FK"]) & CONT_BASIS == Convert.ToInt64(dtSurcharge.Rows[nRowCnt]["CONT_BASIS"]))
                    {
                        dr = dstemp.NewRow();
                        for (nColCnt = 0; nColCnt <= dtSurcharge.Columns.Count - 1; nColCnt++)
                        {
                            dr[nColCnt] = dtSurcharge.Rows[nRowCnt][nColCnt];
                        }
                        dstemp.Rows.Add(dr);
                    }
                }
                return dstemp.DefaultView;
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

        #endregion "Save"

        #region "Fetch Header Details"

        public DataSet FetchHeader(int SRRSeaPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("SELECT CMT.CUSTOMER_NAME,");
            sb.Append("       CCD.ADM_ADDRESS_1,");
            sb.Append("       CCD.ADM_ADDRESS_2,");
            sb.Append("       CCD.ADM_ADDRESS_3,");
            sb.Append("       CCD.ADM_ZIP_CODE,");
            sb.Append("       CMTS.COUNTRY_NAME,");
            sb.Append("       CCST.SRR_REF_NO,");
            sb.Append("       CASE WHEN CCST.SRR_TYPE=0 THEN CCS.CONT_REF_NO ELSE TARIFF.Tariff_Ref_No END AS CONT_REF_NO,");
            sb.Append("       CCST.SRR_DATE,");
            sb.Append("       'SEA' BIZ_TYPE,");
            sb.Append("       OMT.OPERATOR_NAME,");
            sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
            sb.Append("       CT.COMMODITY_NAME,");
            sb.Append("       DECODE(CCST.CARGO_TYPE,1,'FCL',2,'LCL') CARGO_TYPE,");
            sb.Append("       CCST.VALID_FROM,");
            sb.Append("       CCST.VALID_TO,");
            sb.Append("       CCST.CREDIT_PERIOD,");
            sb.Append("       DECODE(CCST.STATUS,0,'Requested',1,'Internal Approval',2,'Customer Approval')STATUS,");
            sb.Append("       DECODE(CCST.STATUS,0,CUMT.USER_NAME,1,LUMT.USER_NAME,2,LUMT.USER_NAME) USER_ID, ");
            sb.Append("        CCST.SRR_CLAUSE,");
            sb.Append("       CCST.COL_ADDRESS,");
            sb.Append("        LMT.LOCATION_NAME,    ");
            sb.Append("        LUMT.USER_NAME APPD_BY,     ");
            sb.Append("       CCST.LAST_MODIFIED_DT  APP_DT               ");
            sb.Append("  FROM SRR_SEA_TBL     CCST,");
            sb.Append("       CONT_CUST_SEA_TBL CCS,TARIFF_MAIN_SEA_TBL TARIFF,");
            sb.Append("       CUSTOMER_MST_TBL      CMT,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
            sb.Append("       LOCATION_MST_TBL      LMT,");
            sb.Append("       COUNTRY_MST_TBL       CMTS,");
            sb.Append("       OPERATOR_MST_TBL      OMT,");
            sb.Append("       COMMODITY_MST_TBL     CT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       USER_MST_TBL            CUMT,");
            sb.Append("       USER_MST_TBL            LUMT");
            sb.Append(" WHERE CCST.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            sb.Append("   AND CCST.OPERATOR_MST_FK = OMT.OPERATOR_MST_PK(+)");
            sb.Append("   AND CCST.TARIFF_MAIN_SEA_FK = CCS.CONT_CUST_SEA_PK(+) AND CCST.TARIFF_MAIN_SEA_FK = TARIFF.TARIFF_MAIN_SEA_PK(+)");
            sb.Append("   AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
            sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND LMT.COUNTRY_MST_FK = CMTS.COUNTRY_MST_PK");
            sb.Append("   AND CCST.COMMODITY_MST_FK = CT.COMMODITY_MST_PK(+)");
            sb.Append("   AND CCST.COMMODITY_GROUP_MST_FK = CGMT.COMMODITY_GROUP_PK");
            sb.Append("   AND CCST.CREATED_BY_FK = CUMT.USER_MST_PK");
            sb.Append("   AND CCST.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+)");
            sb.Append("   AND CCST.SRR_SEA_PK = " + SRRSeaPk);
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
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

        #endregion "Fetch Header Details"

        #region "Fetch FreightDetails"

        public DataSet FetchFreightDetails(long SRRSeaContractPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT POL.PORT_NAME POL,");
            sb.Append("       POD.PORT_NAME POD,");
            sb.Append("       CASE");
            sb.Append("         WHEN SST.CARGO_TYPE=1 THEN");
            sb.Append("          CTMT.CONTAINER_TYPE_MST_ID");
            sb.Append("         ELSE");
            sb.Append("          DUMT.DIMENTION_ID");
            sb.Append("       END CONTAINER_TYPE_MST_ID,");
            sb.Append("       CTM.CURRENCY_ID,");
            sb.Append("       STST.CURRENT_BOF_RATE,");
            sb.Append("       STST.CURRENT_ALL_IN_RATE,");
            sb.Append("       STST.REQUESTED_BOF_RATE,");
            sb.Append("       STST.REQUESTED_ALL_IN_RATE,");
            sb.Append("       CASE");
            sb.Append("         WHEN STST.EXPECTED_VOLUME IS NULL THEN");
            sb.Append("          STST.EXPECTED_BOXES");
            sb.Append("         ELSE");
            sb.Append("          STST.EXPECTED_VOLUME");
            sb.Append("       END EXPECTED_VOLUME,");
            sb.Append("       STST.VALID_FROM,");
            sb.Append("       STST.VALID_TO,");
            sb.Append("       STST.APPROVED_BOF_RATE,");
            sb.Append("       STST.APPROVED_ALL_IN_RATE,");
            sb.Append("       CTM1.CURRENCY_ID,");
            sb.Append("       SSCS.CURR_SURCHARGE_AMT,");
            sb.Append("       SSCS.REQ_SURCHARGE_AMT,");
            sb.Append("       SSCS.APP_SURCHARGE_AMT,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_NAME");
            sb.Append("  FROM SRR_SEA_TBL             SST,");
            sb.Append("       SRR_TRN_SEA_TBL         STST,");
            sb.Append("       SRR_SUR_CHRG_SEA_TBL    SSCS,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
            sb.Append("       DIMENTION_UNIT_MST_TBL  DUMT,");
            sb.Append("       PORT_MST_TBL            POL,");
            sb.Append("       PORT_MST_TBL            POD,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTM,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTM1,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT");
            sb.Append(" WHERE STST.SRR_SEA_FK = SST.SRR_SEA_PK");
            sb.Append("   AND SSCS.SRR_TRN_SEA_FK = STST.SRR_TRN_SEA_PK");
            sb.Append("   AND STST.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND STST.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND STST.CURRENCY_MST_FK = CTM.CURRENCY_MST_PK");
            sb.Append("   AND STST.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND STST.LCL_BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)");
            sb.Append("   AND SSCS.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND SSCS.CURRENCY_MST_FK = CTM1.CURRENCY_MST_PK");
            sb.Append("        AND SST.SRR_SEA_PK =" + SRRSeaContractPk);
            try
            {
                return objWK.GetDataSet(sb.ToString());
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

        #endregion "Fetch FreightDetails"

        #region " Port Group "

        public string FetchPrtGroup(int QuotPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT NVL(Q.PORT_GROUP,0) PORT_GROUP FROM SRR_SEA_TBL Q WHERE Q.SRR_SEA_PK = " + QuotPK;
                return objWF.ExecuteScaler(strSQL);
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

        public DataSet FetchFromPortGroup(int QuotPK = 0, int PortGrpPK = 0, string POLPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POL_GRP_FK FROM SRR_TRN_SEA_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POL_FK = P.PORT_MST_PK AND T.POL_GRP_FK = " + PortGrpPK);
                    sb.Append(" AND T.SRR_SEA_FK = " + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POL_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1 ");
                    if (POLPK != "0")
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + POLPK + ") ");
                    }
                }
                return (objWF.GetDataSet(sb.ToString()));
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

        public DataSet FetchToPortGroup(int QuotPK = 0, int PortGrpPK = 0, string PODPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK FROM SRR_TRN_SEA_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.POD_GRP_FK = " + PortGrpPK);
                    sb.Append(" AND T.SRR_SEA_FK = " + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POD_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1");
                    if (PODPK != "0")
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + PODPK + ") ");
                    }
                }
                return (objWF.GetDataSet(sb.ToString()));
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

        public DataSet FetchTariffGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append(" SELECT DISTINCT * FROM (");
                sb.Append(" SELECT POL.PORT_MST_PK   POL_PK,");
                sb.Append("       POL.PORT_ID       POL_ID,");
                sb.Append("       POD.PORT_MST_PK   POD_PK,");
                sb.Append("       POD.PORT_ID       POD_ID,");
                sb.Append("       T.POL_GRP_FK      POL_GRP_FK,");
                sb.Append("       T.PORT_MST_POD_FK POD_GRP_FK,");
                sb.Append("       T.TARIFF_GRP_FK   TARIFF_GRP_MST_PK");
                sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, SRR_TRN_SEA_TBL T");
                sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND T.SRR_SEA_FK =" + QuotPK);
                sb.Append("   UNION");
                sb.Append(" SELECT POL.PORT_MST_PK       POL_PK,");
                sb.Append("       POL.PORT_ID           POL_ID,");
                sb.Append("       POD.PORT_MST_PK       POD_PK,");
                sb.Append("       POD.PORT_ID           POD_ID,");
                sb.Append("       TGM.POL_GRP_MST_FK    POL_GRP_FK,");
                sb.Append("       TGM.POD_GRP_MST_FK    POD_GRP_FK,");
                sb.Append("       TGM.TARIFF_GRP_MST_PK");
                sb.Append("  FROM PORT_MST_TBL       POL,");
                sb.Append("       PORT_MST_TBL       POD,");
                sb.Append("       TARIFF_GRP_TRN_TBL TGT,");
                sb.Append("       TARIFF_GRP_MST_TBL TGM");
                sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK = TGT.POL_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = TGT.POD_MST_FK");
                sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                sb.Append("   AND POL.BUSINESS_TYPE = 2");
                sb.Append("   AND POL.ACTIVE_FLAG = 1");
                sb.Append("   )");

                return (objWF.GetDataSet(sb.ToString()));
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

        public DataSet FetchTariffPODGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK, T.TARIFF_GRP_FK FROM CONT_CUST_TRN_SEA_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.TARIFF_GRP_FK = " + TariffPK);
                    sb.Append(" AND T.CONT_CUST_SEA_FK =" + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK,");
                    sb.Append("        P.PORT_ID,");
                    sb.Append("        TGM.POD_GRP_MST_FK POD_GRP_FK,");
                    sb.Append("        TGM.TARIFF_GRP_MST_PK");
                    sb.Append("  FROM PORT_MST_TBL P, TARIFF_GRP_TRN_TBL TGT, TARIFF_GRP_MST_TBL TGM");
                    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                    sb.Append("   AND P.PORT_MST_PK = TGT.POD_MST_FK");
                    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                    sb.Append("   AND P.BUSINESS_TYPE = 2");
                    sb.Append("   AND P.ACTIVE_FLAG = 1");
                }
                return (objWF.GetDataSet(sb.ToString()));
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

        public long FetchFreightGridPK(string CCPK, int CCTrnFK, int CCFreightFK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append("SELECT CFT.SRR_SUR_CHRG_SEA_PK ");
                sb.Append("  FROM SRR_SUR_CHRG_SEA_TBL CFT,");
                sb.Append("       SRR_SEA_TBL     CMT,");
                sb.Append("       SRR_TRN_SEA_TBL CTT");
                sb.Append(" WHERE CMT.SRR_SEA_PK = CTT.SRR_SEA_FK");
                sb.Append("   AND CTT.SRR_TRN_SEA_PK = CFT.SRR_TRN_SEA_FK");
                sb.Append("   AND CMT.SRR_SEA_PK = " + CCPK);
                sb.Append("   AND CTT.SRR_TRN_SEA_PK = " + CCTrnFK);
                sb.Append("   AND CFT.FREIGHT_ELEMENT_MST_FK = " + CCFreightFK);

                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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

        public long FetchTrnGridPK(string CCPK, int CCPOLFK, int CCPODFK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append("SELECT CTT.SRR_TRN_SEA_PK");
                sb.Append("  FROM SRR_SEA_TBL CMT, SRR_TRN_SEA_TBL CTT");
                sb.Append(" WHERE CMT.SRR_SEA_PK = CTT.SRR_SEA_FK");
                sb.Append("   AND CMT.SRR_SEA_PK = " + CCPK);
                sb.Append("   AND CTT.PORT_MST_POL_FK = " + CCPOLFK);
                sb.Append("   AND CTT.PORT_MST_POD_FK = " + CCPODFK);

                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion " Port Group "

        #region "For COntainers"

        public void MakeConditionContString(string strCondition)
        {
            string[] arr = null;
            Int32 i = default(Int32);
            Int32 j = default(Int32);
            string[] Port = null;
            string[] Container = null;
            if (!(strCondition == "n") & !string.IsNullOrEmpty(strCondition) & !(strCondition == "undefined"))
            {
                arr = strCondition.Split('$');
                strCondition = "n";
                for (i = 0; i <= arr.Length - 2; i++)
                {
                    Port = arr[i].Split('^');
                    Container = Port[2].Split(',');
                    for (j = 0; j <= Container.Length - 1; j++)
                    {
                        if ((strCondition == "n"))
                        {
                            strCondition = "(" + Container[j] + ")";
                        }
                        else
                        {
                            strCondition += ", (" + Container[j] + ")";
                        }
                    }
                }
            }
        }

        #endregion "For COntainers"

        public ArrayList SaveLocalCharges(OracleCommand SCM, string UserName, DataSet dsLocalChrgs, long Quotation_Mst_fk = 0, int BizType = 2, bool AmendFlg = false, int FromFlg = 0, int Qout_Type = 0, int QuotOrBkgFlag = 3)
        {
            int Rcnt = 0;
            DataRow Odr = null;
            int delFlage = 0;
            string QoutLocDtlPKs = "0";

            try
            {
                if (dsLocalChrgs.Tables[0].Rows.Count > 0)
                {
                    for (Rcnt = 0; Rcnt <= dsLocalChrgs.Tables[0].Rows.Count - 1; Rcnt++)
                    {
                        if (AmendFlg == true)
                        {
                            dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"] = DBNull.Value;
                        }
                        if (!string.IsNullOrEmpty(Convert.ToString(dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"])) & (dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"] != null))
                        {
                            if (Convert.ToString(dsLocalChrgs.Tables[0].Rows[Rcnt]["SEL"]) == "TRUE" | Convert.ToBoolean(dsLocalChrgs.Tables[0].Rows[Rcnt]["SEL"]) == true)
                            {
                                QoutLocDtlPKs += "," + dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"];
                                SCM.CommandType = CommandType.StoredProcedure;
                                SCM.CommandText = UserName + ".QUOTATION_LOCAL_CHRG_PKG.QUOTATION_LOCAL_TRN_UPD";
                                var _with6 = SCM.Parameters;
                                _with6.Clear();
                                _with6.Add("QUOTATION_LOCAL_PK_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOTATION_LOCAL_PK"]);
                                _with6.Add("TARIFF_TRN_FK_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["TARIFF_PK"]);
                                _with6.Add("MINIMUM_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_MINAMT"]);
                                _with6.Add("SHIPMENT_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_SHIPMENT_AMT"]);
                                _with6.Add("CONT_20_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_CONT_20_AMT"]);
                                _with6.Add("CONT_40_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_CONT_40_AMT"]).Direction = ParameterDirection.Input;
                                _with6.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                                _with6.Add("PROCESS_TYPE_IN", (FromFlg == 0 ? 1 : 2)).Direction = ParameterDirection.Input;
                                _with6.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                                _with6.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                                _with6.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                SCM.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            if (Convert.ToString(dsLocalChrgs.Tables[0].Rows[Rcnt]["SEL"]) == "TRUE" | Convert.ToBoolean(dsLocalChrgs.Tables[0].Rows[Rcnt]["SEL"]) == true)
                            {
                                SCM.CommandType = CommandType.StoredProcedure;
                                SCM.CommandText = UserName + ".QUOTATION_LOCAL_CHRG_PKG.QUOTATION_LOCAL_TRN_INS";
                                var _with7 = SCM.Parameters;
                                _with7.Clear();
                                _with7.Add("QUOTATION_MST_FK_IN", Quotation_Mst_fk);
                                _with7.Add("TARIFF_TRN_FK_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["TARIFF_PK"]);
                                _with7.Add("MINIMUM_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_MINAMT"]);
                                _with7.Add("SHIPMENT_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_SHIPMENT_AMT"]);
                                _with7.Add("CONT_20_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_CONT_20_AMT"]);
                                _with7.Add("CONT_40_AMT_IN", dsLocalChrgs.Tables[0].Rows[Rcnt]["QUOT_CONT_40_AMT"]).Direction = ParameterDirection.Input;
                                _with7.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                                _with7.Add("PROCESS_TYPE_IN", (FromFlg == 0 ? 1 : 2)).Direction = ParameterDirection.Input;
                                _with7.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                                _with7.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                                _with7.Add("FROM_FLAG_IN", QuotOrBkgFlag).Direction = ParameterDirection.Input;
                                _with7.Add("QUOT_TYPE_IN", Qout_Type).Direction = ParameterDirection.Input;
                                _with7.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                SCM.ExecuteNonQuery();
                                QoutLocDtlPKs += "," + SCM.Parameters["RETURN_VALUE"].Value;
                            }
                        }
                    }
                    if (QoutLocDtlPKs.Length > 0)
                    {
                        SCM.CommandType = CommandType.StoredProcedure;
                        SCM.CommandText = UserName + ".QUOTATION_LOCAL_CHRG_PKG.QUOTATION_LOCAL_TRN_DEL";
                        var _with8 = SCM.Parameters;
                        _with8.Clear();
                        _with8.Add("QUOTATION_LOCAL_PKS_IN", QoutLocDtlPKs);
                        _with8.Add("QUOTATION_MST_FK_IN", Quotation_Mst_fk);
                        _with8.Add("QUOT_TYPE_IN", Qout_Type).Direction = ParameterDirection.Input;
                        _with8.Add("FROM_FLAG_IN", QuotOrBkgFlag).Direction = ParameterDirection.Input;
                        _with8.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        SCM.ExecuteNonQuery();
                    }
                    arrMessage.Add("saved");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            return new ArrayList();
        }
    }
}