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

namespace Quantum_QFOR
{
    public class cls_THC_PHC : CommonFeatures
    {
        #region "List of Properties"

        private Int64 M_Protocol_Mst_Pk;
        private string M_Protocol_NAME;

        private Int64 M_Created_By_Fk;
        private string M_Created_Dt;

             private Int64 M_Last_Modified_By_FK;
        private string M_Last_Modified_Dt;

        private string M_Protocol_VALUE;

        public Int64 Protocol_Mst_Pk
        {
            get { return M_Protocol_Mst_Pk; }
            set { M_Protocol_Mst_Pk = value; }
        }

        public string Protocol_Id
        {
            get { return M_Protocol_NAME; }
            set { M_Protocol_NAME = value; }
        }

        public string Protocol_Name
        {
            get { return M_Protocol_VALUE; }
            set { M_Protocol_VALUE = value; }
        }

        public Int64 Created_By_Fk
        {
            get { return M_Created_By_Fk; }
            set { M_Created_By_Fk = value; }
        }

        public string Created_Dt
        {
            get { return M_Created_Dt; }
            set { M_Created_Dt = value; }
        }

        public Int64 Last_Modified_By_FK
        {
            get { return M_Last_Modified_By_FK; }
            set { M_Last_Modified_By_FK = value; }
        }

        public string Last_Modified_Dt
        {
            get { return M_Last_Modified_Dt; }
            set { M_Last_Modified_Dt = value; }
        }

        #endregion "List of Properties"

        #region "Fetch THC Rates"

        public DataTable FetchHDR(string strContId = "", int portID = 0, int countryID = 0, int THC = 0, string fromDate = " ", string toDate = " ", int cargoType = 0, int currencyType = 0, int TradeID = 0, int isActive = 0,
        int locationID = 0, Int32 flag = 0)
        {
            string str = null;
            string str1 = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            DataColumn dcCol = null;
            Int16 i = default(Int16);

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;
            if (portID != 0)
            {
                strCondition += " AND THC.PORT_MST_FK = " + portID ;
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (countryID != 0)
            {
                strCondition += " AND THC.PORT_MST_FK IN(select p.port_mst_pk from port_mst_tbl p,country_mst_tbl c where p.country_mst_fk = c.country_mst_pk and c.country_mst_pk =  " + countryID + ")" ;
            }
            if (THC != 0)
            {
                strCondition += " AND THC.FREIGHT_ELEMENT_MST_FK = " + THC ;
            }
            if (cargoType != 0)
            {
                strCondition += " AND THC.CARGO_TYPE = " + cargoType ;
            }
            if (currencyType != 0)
            {
                strCondition += " AND THC.CURRENCY_MST_FK  = " + currencyType ;
            }

            if (TradeID != 0)
            {
                strCondition += " AND THC.trade_MST_FK = " + TradeID ;
            }
            if ((fromDate != null) & (toDate != null))
            {
                strCondition += " AND ((TO_DATE('" + toDate + "' , '" + dateFormat + "') BETWEEN ";
                strCondition += "     THC.VALID_FROM AND THC.VALID_TO) OR ";
                strCondition += "     (TO_DATE('" + fromDate + "' , '" + dateFormat + "') BETWEEN ";
                strCondition += "     THC.VALID_FROM AND THC.VALID_TO) OR ";
                strCondition += "     (THC.VALID_TO IS NULL))";
            }
            else if ((toDate != null) & fromDate == null)
            {
                strCondition += "  AND ( ";
                strCondition += "         THC.VALID_TO <= TO_DATE('" + toDate + "' , '" + dateFormat + "') ";
                strCondition += "        OR THC.VALID_TO IS NULL ";
                strCondition += "      ) ";
            }
            else if ((fromDate != null) & toDate == null)
            {
                strCondition += "  AND ( ";
                strCondition += "         THC.VALID_FROM >= TO_DATE('" + fromDate + "' , '" + dateFormat + "') ";
                strCondition += "        OR THC.VALID_TO IS NULL ";
                strCondition += "      ) ";
            }
            if (isActive != 0)
            {
                strCondition += " AND THC.ACTIVE  = " + isActive ;
                strCondition += " AND ( ";
                strCondition += "        THC.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ";
                strCondition += "       OR THC.VALID_TO IS NULL ";
                strCondition += "     ) ";
            }
            if (TradeID == 0)
            {
                str = "select";
                str += "        distinct thc.version_no,thc.active, THC.THC_RATES_MST_PK,";
                str += "        thc.port_mst_fk,";
                str += "        port.port_id,";
                str += "        '' trade_code,";
                str += "        thc.freight_element_mst_fk,";
                str += "        frt.freight_element_id,";
                str += "        thc.currency_mst_fk,";
                str += "        curr.currency_id as,";
                str += "        thc.valid_from as,";
                str += "        thc.valid_to";
                str += " from   port_thc_rates_trn thc,";
                str += "        port_mst_tbl port,";
                str += "        freight_element_mst_tbl frt,";
                str += "        currency_type_mst_tbl curr ";
                str += " where  1=1 ";
                str += "        AND THC.PORT_MST_FK = PORT.PORT_MST_PK";
                str += "        AND CURR.CURRENCY_MST_PK = THC.CURRENCY_MST_FK";
                str += "        AND THC.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK ";
                str += "        AND PORT.active_flag = 1 ";
                str += "        AND  THC.TRADE_MST_FK is  null";
                str += strCondition;
                str += "UNION";
            }
            str += "select";
            str += "        distinct thc.version_no,thc.active, THC.THC_RATES_MST_PK,";
            str += "        thc.port_mst_fk,";
            str += "        port.port_id,";
            str += "        tmt.trade_code,";
            str += "        thc.freight_element_mst_fk,";
            str += "        frt.freight_element_id,";
            str += "        thc.currency_mst_fk,";
            str += "        curr.currency_id,";
            str += "        thc.valid_from,";
            str += "        thc.valid_to";
            str += " from   port_thc_rates_trn thc,";
            str += "        port_mst_tbl port,";
            str += "        freight_element_mst_tbl frt,";
            str += "        currency_type_mst_tbl curr, ";
            str += "        TRADE_MST_TBL TMT,";
            str += "        TRADE_MST_TRN TTRN";

            str += " where  1=1 ";
            str += " AND THC.PORT_MST_FK = PORT.PORT_MST_PK";
            str += "        AND CURR.CURRENCY_MST_PK = THC.CURRENCY_MST_FK";
            str += "        AND THC.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK ";
            str += "        AND PORT.active_flag = 1 ";
            str += "        AND THC.TRADE_MST_FK = TMT.TRADE_MST_PK ";
            str += "        AND TTRN.TRADE_MST_FK  = TMT.TRADE_MST_PK(+)";
            str += "        AND TTRN.PORT_MST_FK = PORT.PORT_MST_PK(+)";
            str += strCondition;
            str1 += "     select version_no,";
            str1 += "     active as \"Active\",";
            str1 += "     THC_RATES_MST_PK,";
            str1 += "     port_mst_fk,";
            str1 += "     port_id as \"Port\",";
            str1 += "     trade_code as \"Trade\",";
            str1 += "     freight_element_mst_fk,";
            str1 += "     freight_element_id as \"THC\",";
            str1 += "     currency_mst_fk,";
            str1 += "     currency_id as \"Currency\",";
            str1 += "     valid_from as \"From_Date\",";
            str1 += "     valid_to as \"To_Date\"";
            str1 += "     from (";
            str1 += str;
            str1 += "     )";
            str1 += "     order by valid_from desc";
            try
            {
                dtMain = objWF.GetDataTable(str1);
                dtContainerType = FetchActiveCont(strContId);
                for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                {
                    dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][0]), typeof(decimal));
                    dtMain.Columns.Add(dcCol);
                }

                return dtMain;
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

        #endregion "Fetch THC Rates"

        #region "This function returns only container types."

        public DataTable FetchActiveCont(string strContId)
        {
            string str = null;
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();
            if (strContId == " ")
            {
            }
            else
            {
                strCondition = " AND CTMT.CONTAINER_TYPE_MST_ID IN (" + strContId + ") ";
            }

            str = " SELECT CTMT.CONTAINER_TYPE_MST_ID" + " FROM CONTAINER_TYPE_MST_TBL CTMT WHERE CTMT.ACTIVE_FLAG=1 " + strCondition + "  ORDER BY CTMT.PREFERENCES";
            try
            {
                return objWF.GetDataTable(str);
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

        #endregion "This function returns only container types."

        #region "This function returns only container types."

        public DataTable FetchActiveContForEdit(string strContId, string thcRatesPK = "-1")
        {
            string str = null;
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();

            if (strContId == " ")
            {
                str = "select";
                str += "        cmt.container_type_mst_id";
                str += "from    port_thc_rates_trn thc,";
                str += " PORT_THC_CONT_DTL CONT,";
                str += "        CONTAINER_TYPE_MST_TBL CMT";
                str += "where 1=1 AND ";
                str += " CONT.THC_RATES_MST_FK = thc.THC_RATES_MST_PK AND ";
                strCondition += "        thc.thc_rates_mst_pk = " + thcRatesPK;
                strCondition += "        AND cont.CONTAINER_TYPE_MST_FK=cmt.container_type_mst_pk ";
                strCondition += " AND ROWNUM < 10 ";
                str += strCondition;
                str += " order by cmt.preferences ";
            }
            else
            {
                str = " SELECT CTMT.CONTAINER_TYPE_MST_ID" + " FROM CONTAINER_TYPE_MST_TBL CTMT WHERE CTMT.ACTIVE_FLAG=1 AND CTMT.CONTAINER_TYPE_MST_ID IN (" + strContId + ")  ORDER BY CTMT.PREFERENCES";
            }
            try
            {
                return objWF.GetDataTable(str);
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

        #endregion "This function returns only container types."

        #region "For Enhance Searching"

        public string FetchCurrency(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            dynamic strNull = DBNull.Value;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CURRENCY_PKG.GETCURRENCY_COMMON";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE",OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "For Enhance Searching"

        #region "FetchNewCurrency"

        public string FetchNewCurrency(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string StrProcess = null;
            string strReq = null;
            dynamic strNull = DBNull.Value;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(3));
            StrProcess = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CURRENCY_PKG.GETNEWCURRENCY_COMMON";

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with2.Add("BizType_IN", strBizType).Direction = ParameterDirection.Input;
                _with2.Add("Process_IN", StrProcess).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE",OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "FetchNewCurrency"

        #region "FetchPortCountry"

        public string FetchPortCountry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strCountry = null;
            dynamic strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            strBizType = Convert.ToString(arr.GetValue(3));
            strCountry = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COUNTRY_PORT_PKG.GETPORT_COMMON";
                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with3.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with3.Add("COUNTRY_MST_PK_IN", strCountry).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE",OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "FetchPortCountry"

        #region "FetchPortCountryTRADE"

        public string FetchPortCountryTRADE(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strTRADE = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strCountry = null;
            dynamic strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            strBizType = Convert.ToString(arr.GetValue(3));
            strCountry = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strTRADE = Convert.ToString(arr.GetValue(5));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COUNTRY_PORT_PKG.GETPORT_TRADE";

                var _with4 = selectCommand.Parameters;
                _with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with4.Add("TRADE_MST_FK_IN", (!string.IsNullOrEmpty(strTRADE) ? strTRADE : strNull)).Direction = ParameterDirection.Input;
                _with4.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with4.Add("COUNTRY_MST_PK_IN", strCountry).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE",OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "FetchPortCountryTRADE"

        #region "FetchPortCountryTRADE"

        public string FetchCountry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            dynamic strNull = DBNull.Value;
            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COUNTRY_PKG.GETCOUNTRY_COMMON";

                var _with5 = selectCommand.Parameters;
                _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE",OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "FetchPortCountryTRADE"

        #region "FetchPortCountryTRADE"

        public string FetchTradeCountry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strTradePk = null;
            dynamic strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strTradePk = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_TRADE_PKG.GETTRADE_COUNTRY";

                var _with6 = selectCommand.Parameters;
                _with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with6.Add("TRADE_PK_IN", getDefault(strTradePk, DBNull.Value)).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE",OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion "FetchPortCountryTRADE"

        #region "This function gets the container data depending on the search criteria."

        public DataSet FetchContainerData(string strContId = "", int portID = 0, int countryID = 0, int THC = 0, string fromDate = " ", string toDate = " ", int cargoType = 0, int currencyType = 0, int isActive = -1, int locationID = 0,
        Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataSet dSMain = new DataSet();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string StrSqlCount = null;

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (portID != 0)
            {
                strCondition += " AND THC.PORT_MST_FK = " + portID ;
            }
            if (countryID != 0)
            {
                strCondition += " AND THC.PORT_MST_FK IN(select p.port_mst_pk from port_mst_tbl p,country_mst_tbl c where p.country_mst_fk = c.country_mst_pk and c.country_mst_pk =  " + countryID + ")" ;
            }
            if (THC != 0)
            {
                strCondition += " AND THC.FREIGHT_ELEMENT_MST_FK = " + THC ;
            }

            if (cargoType != 0)
            {
                strCondition += " AND THC.CARGO_TYPE = " + cargoType ;
            }
            if (currencyType != 0)
            {
                strCondition += " AND THC.CURRENCY_MST_FK  = " + currencyType ;
            }

            //If fromDate Is Nothing And toDate Is Nothing Then
            //    strCondition &= vbCrLf & " AND(TO_DATE(TO_CHAR(sysdate,'" & dateFormat & "'),'" & dateFormat & "')  between thc.valid_from and thc.valid_to OR thc.valid_from  >= TO_DATE(TO_CHAR(sysdate,'" & dateFormat & "'),'" & dateFormat & "'))  " & vbCrLf
            //End If
            if ((fromDate != null) & (toDate != null))
            {
                strCondition += " AND ((TO_DATE('" + toDate + "' , '" + dateFormat + "') BETWEEN ";
                strCondition += "     THC.VALID_FROM AND THC.VALID_TO) OR ";
                strCondition += "     (TO_DATE('" + fromDate + "' , '" + dateFormat + "') BETWEEN ";
                strCondition += "     THC.VALID_FROM AND THC.VALID_TO) OR ";
                strCondition += "     (THC.VALID_TO IS NULL))";
            }
            else if ((toDate != null) & fromDate == null)
            {
                strCondition += "  AND ( ";
                strCondition += "         THC.VALID_TO <= TO_DATE('" + toDate + "' , '" + dateFormat + "') ";
                strCondition += "        OR THC.VALID_TO IS NULL ";
                strCondition += "      ) ";
            }
            else if ((fromDate != null) & toDate == null)
            {
                strCondition += "  AND ( ";
                strCondition += "         THC.VALID_FROM >= TO_DATE('" + fromDate + "' , '" + dateFormat + "') ";
                strCondition += "        OR THC.VALID_TO IS NULL ";
                strCondition += "      ) ";
            }
            if (isActive != -1)
            {
                strCondition += " AND THC.ACTIVE  = " + isActive ;
                strCondition += " AND ( ";
                strCondition += "        THC.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ";
                strCondition += "       OR THC.VALID_TO IS NULL ";
                strCondition += "     ) ";
            }
            str = " SELECT ROWNUM SLNO, q.* FROM ";
            str += " (SELECT ";
            str += "        THC.THC_RATES_MST_PK, ";
            str += "        CMT.CONTAINER_TYPE_MST_ID,";
            str += "        CONT.CONTAINER_RATE ";
            str += " from   port_thc_rates_trn thc,";
            str += "        port_mst_tbl port,";
            str += "        freight_element_mst_tbl frt,";
            str += "        currency_type_mst_tbl curr, ";
            str += " PORT_THC_CONT_DTL CONT,";
            str += "        CONTAINER_TYPE_MST_TBL CMT ";
            str += " where  1=1 ";
            str += " AND CONT.THC_RATES_MST_FK = thc.THC_RATES_MST_PK";
            str += "        AND THC.PORT_MST_FK = PORT.PORT_MST_PK";
            str += "        AND CURR.CURRENCY_MST_PK = THC.CURRENCY_MST_FK";
            str += "        AND CMT.CONTAINER_TYPE_MST_PK = CONT.CONTAINER_TYPE_MST_FK";
            str += "        AND THC.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK ";
            str += "        AND PORT.active_flag = 1 ";
            str += strCondition;
            str += "     ) q ";
            StrSqlCount = "SELECT COUNT(*) FROM ( ";
            StrSqlCount += str;
            StrSqlCount += " ) ";
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSqlCount.ToString()));
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
            string StrSqlRecords = null;
            StrSqlRecords = "SELECT * FROM ( ";
            StrSqlRecords += str.ToString();
            StrSqlRecords += " ) WHERE SLNO BETWEEN " + start + " AND " + last;
            try
            {
                dSMain = objWF.GetDataSet(StrSqlRecords.ToString());
                return dSMain;
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

        #endregion "This function gets the container data depending on the search criteria."

        #region "Fetch the THC elements"

        public DataSet FillCombo()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT F.FREIGHT_ELEMENT_MST_PK,F.FREIGHT_ELEMENT_ID FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_ID LIKE 'THL' OR F.FREIGHT_ELEMENT_ID LIKE 'THD' AND F.ACTIVE_FLAG =1  ";

            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "Fetch the THC elements"

        #region "This function returns the THD,THL combination for all the active ports under the pericular user.."

        public DataTable FetchPortCombination(string strContId = "", int portID = 0, int countryID = 0, int tradeID = 0, int locID = 0, string fromDate = " ", string toDate = " ")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            int tradePk = 0;
            tradePk = tradeID;
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;
            if (portID != 0)
            {
                strCondition += " AND PORT.PORT_MST_PK = " + portID ;
            }
            if (tradeID != 0)
            {
                strCondition += " AND ttrn.trade_mst_fk = " + tradeID ;
            }
            if (countryID != 0)
            {
                strCondition += " AND PORT.PORT_MST_PK IN(select p.port_mst_pk from port_mst_tbl p,country_mst_tbl c where p.country_mst_fk = c.country_mst_pk and c.country_mst_pk =  " + countryID + ")" ;
            }
            str = "select";
            str += "        'false' active , 0 thc_mst_pk,  ";
            str += "        port.Port_Mst_Pk,";
            str += "        port.port_id ,";
            if (tradeID != 0)
            {
                str += "        ttrn.trade_mst_fk,";
                str += "         tmt.trade_code,";
            }
            else
            {
                str += "        '',";
                str += "        '',";
            }
            str += "        frt.freight_element_mst_pk,";
            str += "        frt.freight_element_id ,";
            str += "        country.currency_mst_fk,";
            if ((fromDate != null))
            {
                str += "      To_Date('" + fromDate + "',dateformat) From_Date,";
            }
            else
            {
                str += "       To_Date('') From_Date,";
            }
            if ((toDate != null))
            {
                str += "      To_Date('" + toDate + "',dateformat) To_Date,";
            }
            else
            {
                str += "      To_Date('') To_Date,";
            }
            str += "        0 version_no ";
            str += " from   port_mst_tbl port,";
            str += "        freight_element_mst_tbl frt,";
            str += "        currency_type_mst_tbl curr,";
            str += "        country_mst_tbl country,";
            str += "        trade_mst_trn  ttrn,";
            str += "        trade_mst_tbl tmt";
            str += " where  1=1 ";
            str += "        AND FRT.FREIGHT_ELEMENT_ID LIKE 'THL'";
            str += "        AND PORT.ACTIVE_FLAG =1 ";
            str += "        AND PORT.BUSINESS_TYPE =2 ";
            str += "        AND COUNTRY.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
            str += "        AND PORT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK";
            str += "       AND ttrn.trade_mst_fk = tmt.trade_mst_pk";
            str += "       AND ttrn.port_mst_fk = port.port_mst_pk ";
            str += strCondition;
            str += " union ";
            str += "select";
            str += "        'false' active ,0 thc_mst_pk,   ";
            str += "        port.Port_Mst_Pk,";
            str += "        port.port_id ,";
            if (tradeID != 0)
            {
                str += "        ttrn.trade_mst_fk,";
                str += "         tmt.trade_code,";
            }
            else
            {
                str += "        '',";
                str += "        '',";
            }
            str += "        frt.freight_element_mst_pk,";
            str += "        frt.freight_element_id ,";
            str += "        country.currency_mst_fk,";
            if ((fromDate != null))
            {
                str += "      To_Date('" + fromDate + "',dateformat) From_Date,";
            }
            else
            {
                str += "        To_Date('') From_Date,";
            }

            if ((toDate != null))
            {
                str += "      To_Date('" + toDate + "',dateformat) To_Date,";
            }
            else
            {
                str += "      To_Date('') To_Date, ";
            }
            str += "        0 version_no ";
            str += " from   port_mst_tbl port,";
            str += "        freight_element_mst_tbl frt,";
            str += "        currency_type_mst_tbl curr,";
            str += "        country_mst_tbl country, ";
            str += "        trade_mst_trn  ttrn,";
            str += "        trade_mst_tbl tmt";
            str += " where  1=1 ";
            str += "        AND FRT.FREIGHT_ELEMENT_ID LIKE 'THD'";
            str += "        AND PORT.ACTIVE_FLAG =1 ";
            str += "        AND PORT.BUSINESS_TYPE =2 ";
            str += "        AND COUNTRY.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
            str += "        AND PORT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK";
            str += "       AND ttrn.trade_mst_fk = tmt.trade_mst_pk";
            str += "       AND ttrn.port_mst_fk = port.port_mst_pk ";
            str += strCondition;
            str += " order by port_id,freight_element_id desc ";
            try
            {
                dtMain = objWF.GetDataTable(str);
                dtContainerType = FetchActiveCont(strContId);
                for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                {
                    dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][0]), typeof(decimal));
                    dtMain.Columns.Add(dcCol);
                }
                return dtMain;
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

        #endregion "This function returns the THD,THL combination for all the active ports under the pericular user.."

        #region "Currency type"

        public DataSet FetchCurrencyType()
        {
            string strSQL = null;
            strSQL = "SELECT CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID FROM CURRENCY_TYPE_MST_TBL CURR WHERE CURR.ACTIVE_FLAG =1 ORDER BY CURRENCY_ID";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "Currency type"

        #region "Save Function"

        public ArrayList Save(ref DataSet M_DataSet, int Cargo_Type = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
           OracleCommand insCommand = new OracleCommand();
           OracleCommand updCommand = new OracleCommand();
           OracleCommand delCommand = new OracleCommand();

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                DtTbl = M_DataSet.Tables[0];
                var _with7 = insCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".PORT_THC_RATES_TRN_PKG.PORT_THC_RATES_TRN_INS";
                var _with8 = _with7.Parameters;
                insCommand.Parameters.Add("PORT_MST_FK_IN",OracleDbType.Int32, 10, "PORT_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PORT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("cargo_type_in", Cargo_Type).Direction = ParameterDirection.Input;
                insCommand.Parameters["cargo_type_in"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TRADE_MST_FK_IN",OracleDbType.Int32, 10, "TRADE_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRADE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //

                insCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN",OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VALID_FROM_IN",OracleDbType.Date, 20, "VALID_FROM").Direction = ParameterDirection.Input;
                insCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VALID_TO_IN",OracleDbType.Date, 15, "VALID_TO").Direction = ParameterDirection.Input;
                insCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CONTAINER_DTL_FCL_IN",OracleDbType.Varchar2, 300, "CONTAINER_DTL_FCL").Direction = ParameterDirection.Input;
                insCommand.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CURRENCY_MST_FK_IN",OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ACTIVE_IN",OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE",OracleDbType.Int32, 10, "THC_RATES_MST_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with9 = updCommand;
                _with9.Connection = objWK.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".PORT_THC_RATES_TRN_PKG.PORT_THC_RATES_TRN_UPD";
                var _with10 = _with9.Parameters;

                updCommand.Parameters.Add("THC_RATES_MST_PK_IN",OracleDbType.Int32, 10, "THC_RATES_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["THC_RATES_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PORT_MST_FK_IN",OracleDbType.Int32, 10, "PORT_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PORT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CARGO_TYPE_IN", Cargo_Type).Direction = ParameterDirection.Input;
                updCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                //OracleDbType.Int32, 1

                updCommand.Parameters.Add("TRADE_MST_FK_IN",OracleDbType.Int32, 10, "TRADE_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRADE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN",OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VALID_FROM_IN",OracleDbType.Date, 20, "VALID_FROM").Direction = ParameterDirection.Input;
                updCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VALID_TO_IN",OracleDbType.Date, 15, "VALID_TO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONTAINER_DTL_FCL_IN",OracleDbType.Varchar2, 300, "CONTAINER_DTL_FCL").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CURRENCY_MST_FK_IN",OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ACTIVE_IN",OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Last_Modified_By_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN",OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE",OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with11 = objWK.MyDataAdapter;

                _with11.InsertCommand = insCommand;
                _with11.InsertCommand.Transaction = TRAN;

                _with11.UpdateCommand = updCommand;
                _with11.UpdateCommand.Transaction = TRAN;

                RecAfct = _with11.Update(M_DataSet.Tables["Data"]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save Function"

        #region "Fetch THC Rates for edit."

        public DataTable FetchTHCDataForEdit(string thcRatesPK = "-1", string strContId = " ", string TradeId = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            DataColumn dcCol = null;
            Int16 i = default(Int16);

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;
            str = "select";
            str += "        distinct decode(thc.active,1,'true',0,'false') active, THC.THC_RATES_MST_PK,";
            str += "        thc.port_mst_fk,";
            str += "        port.port_id as \"Port\",";
            str += "        thc.trade_mst_fk,";
            if (!string.IsNullOrEmpty(TradeId) & TradeId != "null")
            {
                str += "        tmt.trade_code,";
            }
            else
            {
                str += "      '',";
            }
            str += "        thc.freight_element_mst_fk,";
            str += "        frt.freight_element_id as \"THC\",";
            str += "        thc.currency_mst_fk,";
            str += "        to_char(thc.valid_from,dateformat) valid_from,";
            str += "        to_char(thc.valid_to,dateformat) valid_to,";
            str += "        thc.Version_No";
            str += " from   port_thc_rates_trn thc,";
            str += "        port_mst_tbl port,";
            str += "        freight_element_mst_tbl frt,";
            str += "        currency_type_mst_tbl curr, ";
            str += "        trade_mst_trn tmtrn,";
            str += "        trade_mst_tbl tmt";

            str += " where  1=1 ";
            str += "        AND THC.PORT_MST_FK = PORT.PORT_MST_PK";
            str += "        AND CURR.CURRENCY_MST_PK = THC.CURRENCY_MST_FK";
            str += "        AND THC.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK ";
            str += "        AND PORT.active_flag = 1 ";
            str += "        AND THC.THC_RATES_MST_PK =" + thcRatesPK;
            if (!string.IsNullOrEmpty(TradeId) & TradeId != "null")
            {
                str += "        AND tmtrn.trade_mst_fk = THC.trade_mst_fk";
                str += "        AND thc.PORT_MST_FK = tmtrn.PORT_MST_FK";
                str += "        AND tmtrn.trade_mst_fk = tmt.trade_mst_pk";
            }

            try
            {
                dtMain = objWF.GetDataTable(str);
                dtContainerType = FetchActiveContForEdit(strContId, thcRatesPK);
                for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                {
                    dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][0]), typeof(decimal));
                    dtMain.Columns.Add(dcCol);
                }
                return dtMain;
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

        #endregion "Fetch THC Rates for edit."

        #region "Container data for edit"

        public DataSet FetchContainerDataForEdit(string strContId = "", int thcPK = 0)
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataSet dSMain = new DataSet();

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;
            str = "SELECT";
            str += "        distinct THC.THC_RATES_MST_PK, ";
            str += "        CMT.CONTAINER_TYPE_MST_ID,";
            str += "        CONT.CONTAINER_RATE ";
            str += " from   port_thc_rates_trn thc,";
            str += "        port_mst_tbl port,";
            str += "        freight_element_mst_tbl frt,";
            str += "        currency_type_mst_tbl curr, ";
            str += " PORT_THC_CONT_DTL CONT,";
            str += "        CONTAINER_TYPE_MST_TBL CMT ";
            str += " where  1=1 ";
            str += " AND CONT.THC_RATES_MST_FK = thc.THC_RATES_MST_PK";
            str += "        AND THC.PORT_MST_FK = PORT.PORT_MST_PK";
            str += "        AND CURR.CURRENCY_MST_PK = THC.CURRENCY_MST_FK";
            str += "        AND CMT.CONTAINER_TYPE_MST_PK = CONT.CONTAINER_TYPE_MST_FK";
            str += "        AND THC.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK ";
            str += "        AND PORT.active_flag = 1 ";
            str += "        AND THC.THC_RATES_MST_PK = " + thcPK;

            try
            {
                dSMain = objWF.GetDataSet(str);
                return dSMain;
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

        #endregion "Container data for edit"

        #region "Validate Saved Rates"

        public int ValidateSavedRate(Int64 PortPK, string FrtID, string CntID, string FromDt, string ToDt)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            try
            {
                sb.Append("SELECT COUNT(*)");
                sb.Append("  FROM PORT_THC_RATES_TRN      P,");
                sb.Append("       PORT_THC_CONT_DTL       T,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL F,");
                sb.Append("       CONTAINER_TYPE_MST_TBL  CY");
                sb.Append(" WHERE P.THC_RATES_MST_PK = T.THC_RATES_MST_FK");
                sb.Append("   AND P.FREIGHT_ELEMENT_MST_FK = F.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND CY.CONTAINER_TYPE_MST_PK = T.CONTAINER_TYPE_MST_FK");
                sb.Append("   AND P.PORT_MST_FK = " + PortPK);
                sb.Append("   AND F.FREIGHT_ELEMENT_ID = '" + FrtID + "' ");
                sb.Append("   AND TO_DATE('" + FromDt + "', DATEFORMAT) >= TO_DATE(P.VALID_FROM,DATEFORMAT)");
                sb.Append("   AND TO_DATE('" + ToDt + "', DATEFORMAT) <= TO_DATE(P.VALID_TO,DATEFORMAT)");
                sb.Append("   AND CY.CONTAINER_TYPE_MST_ID = " + CntID + " ");
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "Validate Saved Rates"
    }
}