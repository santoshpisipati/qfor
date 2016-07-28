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
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    public class cls_TARIFF : CommonFeatures
    {
        #region "List of Properties"

        private Int64 M_Protocol_Mst_Pk;
        private string M_Protocol_NAME;

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
            get { return M_CREATED_BY_FK; }
            set { M_CREATED_BY_FK = value; }
        }

        public DateTime Created_Dt
        {
            get { return M_CREATED_DT; }
            set { M_CREATED_DT = value; }
        }

        public Int64 Last_Modified_By_FK
        {
            get { return M_LAST_MODIFIED_BY_FK; }
            set { M_LAST_MODIFIED_BY_FK = value; }
        }

        public DateTime Last_Modified_Dt
        {
            get { return M_LAST_MODIFIED_DT; }
            set { M_LAST_MODIFIED_DT = value; }
        }

        #endregion "List of Properties"

        #region "FetchCurrency.."

        public DataSet FetchCurrency()
        {
            string strSQL = null;
            strSQL = " SELECT '<ALL>' CURRENCY_ID, ";
            strSQL = strSQL + " ' ' CURRENCY_NAME, ";
            strSQL = strSQL + " 0 CURRENCY_MST_PK ";
            strSQL = strSQL + " FROM DUAL ";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + " SELECT CURRENCY_ID, ";
            strSQL = strSQL + " CURRENCY_NAME, ";
            strSQL = strSQL + " CURRENCY_MST_PK ";
            strSQL = strSQL + " FROM CURRENCY_TYPE_MST_TBL  WHERE ACTIVE_FLAG=1 ";
            strSQL = strSQL + " ORDER BY CURRENCY_ID ";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "FetchCurrency.."

        #region "Fetch Freight Tariff Entry.."

        public object FetchFreightTariffEntry(bool flag = false, bool IncludePHC = false, bool IncludeTHC = false)
        {
            string strSql = "";
            if (flag)
            {
                strSql = strSql + " SELECT 0 FREIGHT_ELEMENT_MST_PK , '<ALL>' FREIGHT_ELEMENT_ID FROM DUAL";
                strSql = strSql + " UNION ";
            }
            strSql = strSql + " SELECT ";
            strSql = strSql + " FREIGHT_ELEMENT_MST_PK,";
            strSql = strSql + " FREIGHT_ELEMENT_ID";
            strSql = strSql + " FROM FREIGHT_ELEMENT_MST_TBL";
            strSql = strSql + " WHERE ACTIVE_FLAG = 1 ";
            if (!IncludePHC)
            {
                strSql = strSql + " AND SURCHARGE_TYPE NOT IN ('PHL','PHD')";
            }
            if (!IncludeTHC)
            {
                strSql = strSql + " AND SURCHARGE_TYPE NOT IN ('THD','THL')";
            }

            strSql = strSql + " ORDER BY FREIGHT_ELEMENT_ID";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Fetch Freight Tariff Entry.."

        #region "FetchFreightElement.."

        public DataSet FetchFreightElement()
        {
            string strSql = "";
            strSql = strSql + " SELECT ";
            strSql = strSql + " FREIGHT_ELEMENT_MST_PK,";
            strSql = strSql + " FREIGHT_ELEMENT_ID,";
            strSql = strSql + " CURRENCY_MST_FK";
            strSql = strSql + " FROM FREIGHT_ELEMENT_MST_TBL";
            strSql = strSql + " WHERE SURCHARGE_TYPE = 'BOF' AND ACTIVE_FLAG=1";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "FetchFreightElement.."

        #region "GetLocation.."

        public DataSet GetLocation()
        {
            string strSql = null;
            strSql = string.Empty;
            strSql += "SELECT 0 location_type_mst_pk, ";
            strSql += "       '<ALL>' location_type_desc ";
            strSql += "  FROM location_type_mst_tbl ";
            strSql += "  UNION  ";
            strSql += "SELECT location_type_mst_pk, ";
            strSql += "       location_type_desc ";
            strSql += "  FROM location_type_mst_tbl ";
            strSql += "  ORDER BY location_type_mst_pk desc ";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "GetLocation.."

        #region "FetchLocation.."

        public int FetchLocation(int locPk)
        {
            string strSql = null;
            strSql = string.Empty;
            strSql += "SELECT location_type_mst_pk, ";
            strSql += "       location_type_desc ";
            strSql += "  FROM location_type_mst_tbl ";
            strSql = " SELECT LOCATION_TYPE_FK FROM location_mst_tbl WHERE LOCATION_MST_PK = " + locPk;
            WorkFlow objWF = new WorkFlow();
            try
            {
                return Convert.ToInt32(objWF.GetDataSet(strSql).Tables[0].Rows[0][0]);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "FetchLocation.."

        #region "GetPort.."

        public DataSet GetPort()
        {
            string strSql = null;
            strSql = string.Empty;
            strSql += "SELECT 0 port_mst_pk, ";
            strSql += "       '<ALL>' port_id ";
            strSql += "  FROM dual ";
            strSql += " Union ";
            strSql += "     SELECT port_mst_pk, ";
            strSql += "       port_id ";
            strSql += "  FROM port_mst_tbl where active=1";
            strSql = strSql + " order by port_id";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "GetPort.."

        #region "GetPOL.."

        public DataSet GetPOL(int LocPk)
        {
            string strSql = null;
            strSql = string.Empty;
            strSql += "(SELECT 0 port_mst_pk,  ";
            strSql += "       '<ALL>' port_id  ";
            strSql += "FROM dual  ";
            strSql += "             UNION ";
            strSql += "SELECT P.port_mst_pk, ";
            strSql += "       P.port_id ";
            strSql += "FROM   loc_port_mapping_trn L, ";
            strSql += "       Port_Mst_Tbl P ";
            strSql += "WHERE  L.PORT_MST_FK = P.PORT_MST_PK ";
            strSql += "       AND L.location_mst_fk = " + LocPk;
            strSql = strSql + " ) order by port_id";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "GetPOL.."

        #region "Fetch Header Tariff Listing.."

        public DataSet FetchHeader()
        {
            string strSql = null;
            strSql = string.Empty;
            strSql += "            SELECT rownum sr_no, 0 standard_freight_rate_pk,  ";
            strSql += "                   to_char(0) sel, ";
            strSql += "                   'TLI'||sector_mst_tbl.TLI_REF_NO TLI,    ";
            strSql += "                   POL.port_id POL_ID ,    ";
            strSql += "                   POL.Port_Mst_Pk POL_Pk ,    ";
            strSql += "                   POD.port_id POD_ID,    ";
            strSql += "                   POD.PORT_MST_PK POD_PK,    ";
            strSql += "                   '' CONTAINER_STATUS,  ";
            strSql += "                   '' CONTAINER_STATUS_DESC,  ";
            strSql += "                   ''    freight_element_id,  ";
            strSql += "                   0 freight_element_mst_pk,  ";
            strSql += "                   0 PERCENTAGE_AMOUNT,  ";
            strSql += "                   ''  currency_id,  ";
            strSql += "                   0 currency_mst_pk,  ";
            strSql += "                   nvl(0,null) RATE_20_GP,  ";
            strSql += "                   nvl(0,null) RATE_40_GP, ";
            strSql += "                   nvl(0,null) RATE_40_HC,  ";
            strSql += "                   nvl(0,null) RATE_20_RF,  ";
            strSql += "                   nvl(0,null) RATE_40_RF,  ";
            strSql += "                   nvl(0,null) RATE_45_GP,  ";
            strSql += "                   '' OTH_CONT,  ";
            strSql += "                   nvl(0,null) BUSINESS_MODEL, ";
            strSql += "                   to_date(null,'dd-Mon-yyyy')  effective_from,  ";
            strSql += "                   to_date(null,'dd-Mon-yyyy') effective_to,  ";
            strSql += "                   0 version_no,  ";
            strSql += "                   to_date(null,'dd-Mon-yyyy') LAST_USED_DT  ";
            strSql += "            FROM port_mst_tbl POL,   ";
            strSql += "            port_mst_tbl POD,   ";
            strSql += "            sector_mst_tbl   ";
            strSql += "            WHERE POL.port_mst_pk = sector_mst_tbl.from_port_fk   ";
            strSql += "            AND POD.port_mst_pk = sector_mst_tbl.to_port_fk   ";
            strSql += "            and rownum < 1 ";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion "Fetch Header Tariff Listing.."

        #region "Fetch Record Tariff Listing.."

        public DataSet FetchRecord(string strChargecode, long lngChargefk, string strCurrency, long lngCurr, string strStatus, string strdate, string strToDate, long lngStatus, bool Mode, Int16 Business_Model,
        long lngPol, long lngPod, long sessionLoged, bool sector, long lngTradePk, string strtrade)
        {
            string strSql = null;
            DateTime enteredFromDate = Convert.ToDateTime(strdate);
            DateTime enteredToDate = default(DateTime);

            if (!string.IsNullOrEmpty(strToDate))
            {
                enteredToDate = Convert.ToDateTime(strToDate);
            }
            strSql = string.Empty;
            strSql += " SELECT Rownum SR_NO,QRY.* from (  ";
            strSql += " SELECT NVL(TT.STANDARD_FREIGHT_RATE_FK,0) STANDARD_FREIGHT_RATE_PK, ";
            strSql += " to_char(0) SEL,";
            strSql += " TT.STANDARD_FREIGHT_CODE TLI,";
            strSql += " POL.PORT_ID POL_ID,";
            strSql += " POL.PORT_MST_PK POL_PK,";
            strSql += " POD.PORT_ID POD_ID,";
            strSql += " POD.PORT_MST_PK POD_PK,";
            strSql += " TT.CONTAINER_STATUS,";
            strSql += " (CASE WHEN TT.CONTAINER_STATUS=1 THEN 'EMPTY' ELSE 'FULL' END) CONTAINER_STATUS_DESC,";
            strSql += " FRT.FREIGHT_ELEMENT_ID,";
            strSql += " FRT.FREIGHT_ELEMENT_MST_PK,";
            strSql += " (CASE WHEN TT.PERCENTAGE_AMOUNT=1 THEN 'Amt' ELSE '%' END) PERCENTAGE_AMOUNT,";
            strSql += " CUR.CURRENCY_ID,";
            strSql += " CUR.CURRENCY_MST_PK,";
            strSql += " SUM(CASE WHEN TT.CONTAINTER_TYPE_MST_FK=4 THEN TT.RATE ELSE 0 END ) RATE_20_GP, ";
            strSql += " SUM(CASE WHEN TT.CONTAINTER_TYPE_MST_FK=82 THEN TT.RATE ELSE 0 END ) RATE_40_GP, ";
            strSql += " SUM(CASE WHEN TT.CONTAINTER_TYPE_MST_FK=21 THEN TT.RATE ELSE 0 END ) RATE_40_HC, ";
            strSql += " SUM(CASE WHEN TT.CONTAINTER_TYPE_MST_FK=322 THEN TT.RATE ELSE 0 END ) RATE_20_RF, ";
            strSql += " SUM(CASE WHEN TT.CONTAINTER_TYPE_MST_FK=101 THEN TT.RATE ELSE 0 END ) RATE_40_RF, ";
            strSql += " SUM(CASE WHEN TT.CONTAINTER_TYPE_MST_FK=284 THEN TT.RATE ELSE 0 END ) RATE_45_GP, ";
            strSql += " '' OTH_CONT, ";
            strSql += " TT.BUSINESS_MODEL, ";
            strSql += " TT.EFFECTIVE_FROM, ";
            strSql += " TT.EFFECTIVE_TO, ";
            strSql += " 0 VERSION_NO, ";
            strSql += " NULL LAST_USED_DT ";
            strSql += " FROM TARIFF_TRN TT, ";
            strSql += " PORT_MST_TBL POL, ";
            strSql += " PORT_MST_TBL POD, ";
            strSql += " SECTOR_MST_TBL SMT, ";
            strSql += " FREIGHT_ELEMENT_MST_TBL FRT, ";
            strSql += " CURRENCY_TYPE_MST_TBL CUR, ";
            strSql += " LOC_PORT_MAPPING_TRN LPM ";
            strSql += " WHERE TT.POL_FK=POL.PORT_MST_PK";
            strSql += " AND TT.POD_FK = POD.PORT_MST_PK";
            strSql += " AND TT.FREIGHT_ELEMENT_MST_FK=FRT.FREIGHT_ELEMENT_MST_PK";
            strSql += " AND TT.CURRENCY_MST_FK=CUR.CURRENCY_MST_PK(+)";
            strSql += " AND TT.POL_FK=SMT.FROM_PORT_FK";
            strSql += " AND TT.POD_FK=SMT.TO_PORT_FK";
            strSql += " AND POL.PORT_MST_PK=LPM.PORT_MST_FK  ";
            strSql += " AND LPM.LOCATION_MST_FK= " + sessionLoged;
            strSql += " AND FRT.ACTIVE_FLAG = 1 AND FRT.SURCHARGE_TYPE NOT IN('THD','THL','PHL','PHD') ";
            strSql += " AND TT.BUSINESS_MODEL=" + Business_Model;
            if (lngChargefk > 0)
            {
                strSql += " AND TT.FREIGHT_ELEMENT_MST_FK=" + lngChargefk;
            }
            if (lngStatus > 0)
            {
                strSql += " AND TT.CONTAINER_STATUS=" + lngStatus;
            }
            if (!sector)
            {
                if (lngPol > 0)
                    strSql += "  AND SMT.FROM_PORT_FK  =" + lngPol;
                if (lngPod > 0)
                    strSql += "  AND SMT.TO_PORT_FK =" + lngPod;
            }

            if (lngCurr > 0)
                strSql += " AND CUR.currency_mst_pk = " + lngCurr;
            strSql += " AND to_date('" + System.String.Format("{0:dd-MMM-yyyy}", enteredFromDate) + "','dd-Mon-yyyy') between TT.EFFECTIVE_FROM and ";
            strSql += " nvl(TT.EFFECTIVE_TO,to_date('" + System.String.Format("{0:dd-MMM-yyyy}", enteredFromDate) + "'))";

            strSql += " AND SMT.SECTOR_MST_PK IN (";
            strSql += " SELECT SMT1.SECTOR_MST_PK FROM SECTOR_MST_TBL SMT1 WHERE 1=1 ";
            if (sector)
            {
                if (lngPol > 0 & lngPod > 0)
                {
                    strSql += " AND SMT.FROM_PORT_FK IN (" + lngPol + "," + lngPod + ") AND SMT.TO_PORT_FK IN (" + lngPol + "," + lngPod + ") ";
                }
                else if (lngPol > 0)
                {
                    strSql += " AND SMT.FROM_PORT_FK IN (" + lngPol + ") OR SMT.TO_PORT_FK IN (" + lngPol + ") ";
                }
                else if (lngPod > 0)
                {
                    strSql += " AND SMT.FROM_PORT_FK IN (" + lngPod + ") OR SMT.TO_PORT_FK IN (" + lngPod + ") ";
                }
            }

            strSql += " )";
            if (lngTradePk > 0)
            {
                strSql += " AND SMT.TRADE_MST_FK=" + lngTradePk;
            }
            strSql += " GROUP BY TT.STANDARD_FREIGHT_RATE_FK ,   ";
            strSql += " TT.STANDARD_FREIGHT_CODE ,";
            strSql += " POL.PORT_ID ,POL.PORT_MST_PK ,POD.PORT_ID ,";
            strSql += " POD.PORT_MST_PK ,TT.CONTAINER_STATUS,FRT.FREIGHT_ELEMENT_ID,";
            strSql += " FRT.FREIGHT_ELEMENT_MST_PK,TT.PERCENTAGE_AMOUNT,CUR.CURRENCY_ID,";
            strSql += " CUR.CURRENCY_MST_PK,TT.BUSINESS_MODEL,TT.EFFECTIVE_FROM,";
            strSql += " TT.EFFECTIVE_TO";
            strSql += " ORDER BY TT.STANDARD_FREIGHT_CODE";
            strSql += " ) QRY";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion "Fetch Record Tariff Listing.."

        #region "Save - Update Tariff Listing.."

        public ArrayList Save(DataSet objDs, string updateOrDeactivate)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction updateTrans = null;

            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();

            objWK.OpenConnection();
            updateTrans = objWK.MyConnection.BeginTransaction();

            try
            {
                var _with1 = updCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".STANDARD_FREIGHT_RATE_TRN_PKG.STANDARD_FREIGHT_RATE_TRN_UPD";
                var _with2 = _with1.Parameters;

                updCommand.Parameters.Add("STANDARD_FREIGHT_RATE_PK_IN", OracleDbType.Int32, 10, "STANDARD_FREIGHT_RATE_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["STANDARD_FREIGHT_RATE_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("STANDARD_FREIGHT_CODE_IN", OracleDbType.Varchar2, 20, "TLI").Direction = ParameterDirection.Input;
                updCommand.Parameters["STANDARD_FREIGHT_CODE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("POL_FK_IN", OracleDbType.Int32, 10, "POL_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("POD_FK_IN", OracleDbType.Int32, 10, "POD_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONTAINER_STATUS_IN", OracleDbType.Int32, 10, "CONTAINER_STATUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONTAINER_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PERCENTAGE_AMOUNT_IN", OracleDbType.Int32, 1, "PERCENTAGE_AMOUNT").Direction = ParameterDirection.Input;
                updCommand.Parameters["PERCENTAGE_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RATE_20_GP_IN", OracleDbType.Int32, 10, "RATE_20_GP").Direction = ParameterDirection.Input;
                updCommand.Parameters["RATE_20_GP_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RATE_40_GP_IN", OracleDbType.Int32, 10, "RATE_40_GP").Direction = ParameterDirection.Input;
                updCommand.Parameters["RATE_40_GP_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RATE_40_HC_IN", OracleDbType.Int32, 10, "RATE_40_HC").Direction = ParameterDirection.Input;
                updCommand.Parameters["RATE_40_HC_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RATE_20_RF_IN", OracleDbType.Int32, 10, "RATE_20_RF").Direction = ParameterDirection.Input;
                updCommand.Parameters["RATE_20_RF_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RATE_40_RF_IN", OracleDbType.Int32, 10, "RATE_40_RF").Direction = ParameterDirection.Input;
                updCommand.Parameters["RATE_40_RF_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RATE_45_GP_IN", OracleDbType.Int32, 10, "RATE_45_GP").Direction = ParameterDirection.Input;
                updCommand.Parameters["RATE_45_GP_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BUSINESS_MODEL_IN", OracleDbType.Int32, 1, "BUSINESS_MODEL").Direction = ParameterDirection.Input;
                updCommand.Parameters["BUSINESS_MODEL_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EFFECTIVE_FROM_IN", OracleDbType.Date, 10, "EFFECTIVE_FROM").Direction = ParameterDirection.Input;
                updCommand.Parameters["EFFECTIVE_FROM_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EFFECTIVE_TO_IN", OracleDbType.Date, 10, "EFFECTIVE_TO").Direction = ParameterDirection.Input;
                updCommand.Parameters["EFFECTIVE_TO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("SR_NO_IN", OracleDbType.Int32, 10, "sr_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["SR_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("Update_DeActivate_IN", updateOrDeactivate).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = objWK.MyDataAdapter;
                _with3.UpdateCommand = updCommand;
                _with3.UpdateCommand.Transaction = updateTrans;
                RecAfct = _with3.Update(objDs);
                if (arrMessage.Count > 0)
                {
                    updateTrans.Rollback();
                    return arrMessage;
                }
                else
                {
                    updateTrans.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                updateTrans.Rollback();
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                throw ex;
                updateTrans.Rollback();
                arrMessage.Add(ex.Message);
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save - Update Tariff Listing.."

        #region "Save - Update - Others Tariff Listing"

        public ArrayList SaveOthersTariffListing(DataSet objDs, long businessModel, string standardFreightCode, long POL, long POD, long containerStatus, long chargeCode, long percentageOrAmount, System.DateTime fromDate, string toDate,
        long currency, string updateOrDeactivate, long PK)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction insertTrans = null;
            System.DBNull strNull = null;
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();
            DateTime enteredtodate = default(DateTime);
            if (!string.IsNullOrEmpty(toDate.Trim()))
            {
                enteredtodate = Convert.ToDateTime(toDate);
            }

            objWK.OpenConnection();
            insertTrans = objWK.MyConnection.BeginTransaction();

            try
            {
                var _with4 = updCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".TARIFF_TRN_PKG.TARIFF_TRN_UPD";
                var _with5 = _with4.Parameters;

                updCommand.Parameters.Add("TARIFF_PK_IN", OracleDbType.Int32, 10, "TARIFF_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["TARIFF_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("STANDARD_FREIGHT_RATE_FK_IN", PK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("BUSINESS_MODEL_IN", businessModel).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("STANDARD_FREIGHT_CODE_IN", standardFreightCode).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("POL_FK_IN", POL).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("POD_FK_IN", POD).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CONTAINER_STATUS_IN", containerStatus).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", chargeCode).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("PERCENTAGE_AMOUNT_IN", percentageOrAmount).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CURRENCY_MST_FK_IN", currency).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONTAINTER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CONTAINER_TYPE_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONTAINTER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RATE_IN", OracleDbType.Int32, 10, "RATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["RATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EFFECTIVE_FROM_IN", fromDate).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("EFFECTIVE_TO_IN", (!string.IsNullOrEmpty(toDate.Trim()) ? System.String.Format("{0:dd-MMM-yyyy}", enteredtodate) : "")).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CREATED_DT_IN", M_CREATED_DT).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("LAST_MODIFIED_DT_IN", M_LAST_MODIFIED_DT).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("SR_NO_IN", OracleDbType.Int32, 10, "SLNO").Direction = ParameterDirection.Input;
                updCommand.Parameters["SR_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("Update_DeActivate_IN", updateOrDeactivate).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with6 = objWK.MyDataAdapter;
                _with6.UpdateCommand = updCommand;
                _with6.UpdateCommand.Transaction = insertTrans;
                RecAfct = _with6.Update(objDs);
                if (arrMessage.Count > 0)
                {
                    insertTrans.Rollback();
                    return arrMessage;
                }
                else
                {
                    insertTrans.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                insertTrans.Rollback();
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                throw ex;
                insertTrans.Rollback();
                arrMessage.Add(ex.Message);
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save - Update - Others Tariff Listing"

        #region "Fetch Record Others Tariff Listing.."

        public DataSet FetchHeaderOthersTariffListing(long businessModel, string standardFreightCode, long POL, long POD, long containerStatus, long chargeCode, System.DateTime fromDate, long currency, long sessionLoged)
        {
            string strSql = null;
            DateTime enteredFromDate = fromDate;
            strSql = string.Empty;
            strSql += " Select rownum SLNO,qry.* from ( ";
            strSql += "     SELECT ";
            strSql += "         NVL(TTR.TARIFF_PK,0)TARIFF_PK, ";
            strSql += "         CTT.CONTAINER_TYPE_MST_PK, ";
            strSql += "         CTT.CONTAINER_TYPE_MST_ID, ";
            strSql += "         NVL(TTR.RATE,0)RATE, ";
            strSql += "         NVL(TTR.VERSION_NO,0)VERSION_NO, ";
            strSql += "         TTR.LAST_USED_DT";
            strSql += "     FROM ";
            strSql += "         TARIFF_TRN TTR ,";
            strSql += "         Container_Type_Mst_Tbl CTT";
            strSql += "     WHERE  CTT.CONTAINER_TYPE_MST_PK = TTR.CONTAINTER_TYPE_MST_FK (+)";
            strSql += "         AND CTT.HARD_CODED_CONTAINERS=0";
            strSql += "         AND CTT.Active_Flag=1";
            strSql += "         AND TTR.FREIGHT_ELEMENT_MST_FK(+)= " + chargeCode;
            strSql += "         AND TTR.CONTAINER_STATUS(+)=" + containerStatus;
            strSql += "         AND TTR.POL_FK(+)=" + POL;
            strSql += "         AND TTR.Pod_Fk(+)=" + POD;
            strSql += "         AND TTR.BUSINESS_MODEL (+)= " + businessModel;
            strSql += "         AND to_date('" + System.String.Format("{0:dd-MMM-yyyy}", enteredFromDate) + "','dd-Mon-yyyy') between TTR.effective_from (+)and nvl(TTR.effective_to (+),to_date('" + System.String.Format("{0:dd-MMM-yyyy}", enteredFromDate) + "','dd-Mon-yyyy'))";
            strSql += "     ORDER BY CTT.CONTAINER_LENGTH_FT,CTT.CONTAINER_KIND";
            strSql += " )qry";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion "Fetch Record Others Tariff Listing.."

        #region "Fetch Header Tariff Entry"

        public DataSet FetchHeaderTariff()
        {
            string strSql = null;
            strSql = string.Empty;
            strSql += "            SELECT rownum sr_no,0 standard_freight_rate_pk,  ";
            strSql += "                   to_char(0) sel, ";
            strSql += "                   'TLI'||sector_mst_tbl.TLI_REF_NO TLI,    ";
            strSql += "                   POL.port_id POL_ID ,    ";
            strSql += "                   POL.Port_Mst_Pk POL_Pk ,    ";
            strSql += "                   POD.port_id POD_ID,    ";
            strSql += "                   POD.PORT_MST_PK POD_PK,    ";
            strSql += "                   0  CONTAINER_STATUS,  ";
            strSql += "                   '' CONTAINER_STATUS_DESC,  ";
            strSql += "                   '' freight_element_id,  ";
            strSql += "                   0 freight_element_mst_pk,  ";
            strSql += "                   0 PERCENTAGE_AMOUNT,  ";
            strSql += "                   '' PERCENTAGE_AMOUNT_DESC,  ";
            strSql += "                   '' CURRENCY_ID,  ";
            strSql += "                   0 currency_mst_pk,  ";
            strSql += "                   nvl(0,null) RATE_20_GP,  ";
            strSql += "                   nvl(0,null) RATE_40_GP, ";
            strSql += "                   nvl(0,null) RATE_40_HC,  ";
            strSql += "                   nvl(0,null) RATE_20_RF,  ";
            strSql += "                   nvl(0,null) RATE_40_RF,  ";
            strSql += "                   nvl(0,null) RATE_45_GP,  ";
            strSql += "                   '' OTH_CONT,  ";
            strSql += "                   nvl(0,null) BUSINESS_MODEL, ";
            strSql += "                   to_date(null,'dd-Mon-yyyy')  effective_from,  ";
            strSql += "                   to_date(null,'dd-Mon-yyyy') effective_to,  ";
            strSql += "                   0 version_no  ";
            strSql += "            FROM port_mst_tbl POL,   ";
            strSql += "            port_mst_tbl POD,   ";
            strSql += "            sector_mst_tbl   ";
            strSql += "            WHERE POL.port_mst_pk = sector_mst_tbl.from_port_fk   ";
            strSql += "            AND POD.port_mst_pk = sector_mst_tbl.to_port_fk   ";
            strSql += "            and rownum < 1 ";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion "Fetch Header Tariff Entry"

        #region "Fetch Record For Tariff Entry.."

        public DataSet FetchRecordTariffEntry(string strChargecode, long lngChargefk, string strCurrency, long lngCurr, string strStatus, long lngStatus, string strdate, string strToDate, long lngBusinessModel, string strBusinessModel,
        long lngPol, long lngPod, long sessionLoged, bool sector, long lngTradePk, string strtrade, bool rate)
        {
            string strSql = null;
            DateTime enteredFromDate = Convert.ToDateTime(strdate);
            DateTime enteredToDate = default(DateTime);

            if (!string.IsNullOrEmpty(strToDate))
            {
                enteredToDate = Convert.ToDateTime(strToDate);
            }

            strSql = string.Empty;
            strSql += "Select rownum SR_NO,qry.* from (";
            strSql += "SELECT  ";
            strSql += "       0 STANDARD_FREIGHT_RATE_PK, ";
            strSql += "       to_char(0) SEL, ";
            strSql += "       'TLI'||SMT.TLI_REF_NO TLI,  ";
            strSql += "       POL.PORT_ID POL_ID, ";
            strSql += "       POL.PORT_MST_PK POL_PK, ";
            strSql += "       POD.PORT_ID POD_ID, ";
            strSql += "       POD.PORT_MST_PK POD_PK, ";

            if (lngStatus == 1)
            {
                strSql += " 1 CONTAINER_STATUS , ";
                strSql += " 'EMPTY' CONTAINER_STATUS_DESC , ";
            }
            else if (lngStatus == 2)
            {
                strSql += " 2 CONTAINER_STATUS , ";
                strSql += " 'FULL' CONTAINER_STATUS_DESC , ";
            }

            strSql += " '" + strChargecode + "' FREIGHT_ELEMENT_ID, ";
            strSql += lngChargefk + " FREIGHT_ELEMENT_MST_PK, ";

            if (rate)
            {
                strSql += " 2 PERCENTAGE_AMOUNT ,";
                strSql += " '%' PERCENTAGE_AMOUNT_DESC ,";
                strCurrency = "";
                lngCurr = 0;
            }
            else
            {
                strSql += " 1 PERCENTAGE_AMOUNT ,";
                strSql += " 'Amt' PERCENTAGE_AMOUNT_DESC ,";
            }

            strSql += " '" + strCurrency + "' CURRENCY_ID ,";
            strSql += lngCurr + " CURRENCY_MST_PK, ";
            strSql += "       0 RATE_20_GP, ";
            strSql += "       0 RATE_40_GP, ";
            strSql += "       0 RATE_40_HC, ";
            strSql += "       0 RATE_20_RF, ";
            strSql += "       0 RATE_40_RF, ";
            strSql += "       0 RATE_45_GP, ";
            strSql += "       '' OTH_CONT,  ";
            strSql += lngBusinessModel + " BUSINESS_MODEL,";
            strSql += "'" + enteredFromDate + "' EFFECTIVE_FROM , ";
            if (!string.IsNullOrEmpty(strToDate))
            {
                strSql += "'" + enteredToDate + "' EFFECTIVE_TO , ";
            }
            else
            {
                strSql += "'' EFFECTIVE_TO , ";
            }
            strSql += "       0 VERSION_NO     ";
            strSql += " FROM  ";
            strSql += "     SECTOR_MST_TBL SMT, ";
            strSql += "     PORT_MST_TBL POL, ";
            strSql += "     PORT_MST_TBL POD, ";
            strSql += "     LOC_PORT_MAPPING_TRN LPM ";
            strSql += " WHERE ";
            strSql += "      SMT.FROM_PORT_FK=POL.PORT_MST_PK ";
            strSql += "      And SMT.TO_PORT_FK=POD.PORT_MST_PK ";
            strSql += "      And POL.PORT_MST_PK=LPM.PORT_MST_FK  ";
            strSql += "      AND LPM.LOCATION_MST_FK= " + sessionLoged;

            if (!sector)
            {
                if (lngPol > 0)
                    strSql += "  AND SMT.FROM_PORT_FK  =" + lngPol;
                if (lngPod > 0)
                    strSql += "  AND SMT.TO_PORT_FK =" + lngPod;
            }

            strSql += " AND SMT.SECTOR_MST_PK IN (";
            strSql += " SELECT SMT1.SECTOR_MST_PK FROM SECTOR_MST_TBL SMT1 WHERE 1=1 ";
            if (sector)
            {
                if (lngPol > 0 & lngPod > 0)
                {
                    strSql += " AND SMT.FROM_PORT_FK IN (" + lngPol + "," + lngPod + ") AND SMT.TO_PORT_FK IN (" + lngPol + "," + lngPod + ") ";
                }
                else if (lngPol > 0)
                {
                    strSql += " AND SMT.FROM_PORT_FK IN (" + lngPol + ") OR SMT.TO_PORT_FK IN (" + lngPol + ") ";
                }
                else if (lngPod > 0)
                {
                    strSql += " AND SMT.FROM_PORT_FK IN (" + lngPod + ") OR SMT.TO_PORT_FK IN (" + lngPod + ") ";
                }
            }

            strSql += " )";

            if (lngTradePk > 0)
                strSql += "      AND SMT.TRADE_MST_FK= " + lngTradePk;

            strSql += " ORDER BY  SEL,TLI ";
            strSql += " ) QRY order by SR_NO ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
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

        #endregion "Fetch Record For Tariff Entry.."

        #region "Save - Insert Tariff Entry"

        public ArrayList SaveTariff(DataSet objDs)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction insertTrans = null;

            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();

            objWK.OpenConnection();
            insertTrans = objWK.MyConnection.BeginTransaction();

            try
            {
                var _with7 = insCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".STANDARD_FREIGHT_RATE_TRN_PKG.STANDARD_FREIGHT_RATE_TRN_INS";
                var _with8 = _with7.Parameters;

                insCommand.Parameters.Add("STANDARD_FREIGHT_RATE_PK_IN", OracleDbType.Int32, 10, "STANDARD_FREIGHT_RATE_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["STANDARD_FREIGHT_RATE_PK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("STANDARD_FREIGHT_CODE_IN", OracleDbType.Varchar2, 20, "TLI").Direction = ParameterDirection.Input;
                insCommand.Parameters["STANDARD_FREIGHT_CODE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("POL_FK_IN", OracleDbType.Int32, 10, "POL_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("POD_FK_IN", OracleDbType.Int32, 10, "POD_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CONTAINER_STATUS_IN", OracleDbType.Int32, 10, "CONTAINER_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["CONTAINER_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PERCENTAGE_AMOUNT_IN", OracleDbType.Int32, 1, "PERCENTAGE_AMOUNT").Direction = ParameterDirection.Input;
                insCommand.Parameters["PERCENTAGE_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RATE_20_GP_IN", OracleDbType.Int32, 10, "RATE_20_GP").Direction = ParameterDirection.Input;
                insCommand.Parameters["RATE_20_GP_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RATE_40_GP_IN", OracleDbType.Int32, 10, "RATE_40_GP").Direction = ParameterDirection.Input;
                insCommand.Parameters["RATE_40_GP_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RATE_40_HC_IN", OracleDbType.Int32, 10, "RATE_40_HC").Direction = ParameterDirection.Input;
                insCommand.Parameters["RATE_40_HC_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RATE_20_RF_IN", OracleDbType.Int32, 10, "RATE_20_RF").Direction = ParameterDirection.Input;
                insCommand.Parameters["RATE_20_RF_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RATE_40_RF_IN", OracleDbType.Int32, 10, "RATE_40_RF").Direction = ParameterDirection.Input;
                insCommand.Parameters["RATE_40_RF_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RATE_45_GP_IN", OracleDbType.Int32, 10, "RATE_45_GP").Direction = ParameterDirection.Input;
                insCommand.Parameters["RATE_45_GP_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BUSINESS_MODEL_IN", OracleDbType.Int32, 1, "BUSINESS_MODEL").Direction = ParameterDirection.Input;
                insCommand.Parameters["BUSINESS_MODEL_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("EFFECTIVE_FROM_IN", OracleDbType.Date, 10, "EFFECTIVE_FROM").Direction = ParameterDirection.Input;
                insCommand.Parameters["EFFECTIVE_FROM_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("EFFECTIVE_TO_IN", OracleDbType.Date, 10, "EFFECTIVE_TO").Direction = ParameterDirection.Input;
                insCommand.Parameters["EFFECTIVE_TO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("SR_NO_IN", OracleDbType.Int32, 10, "SR_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["SR_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with9 = objWK.MyDataAdapter;
                _with9.UpdateCommand = insCommand;
                _with9.UpdateCommand.Transaction = insertTrans;
                RecAfct = _with9.Update(objDs);
                if (arrMessage.Count > 0)
                {
                    insertTrans.Rollback();
                    return arrMessage;
                }
                else
                {
                    insertTrans.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                insertTrans.Rollback();
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                throw ex;
                insertTrans.Rollback();
                arrMessage.Add(ex.Message);
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save - Insert Tariff Entry"

        #region "Fetch Header - Others Tariff Entry"

        public DataSet FetchHeaderOthersTariff()
        {
            string strSql = null;
            string strDDlchargeCode = null;
            long lngCharge = 0;
            string strddlCurrency = null;
            long lngCurrency = 0;
            string strContainerStatus = null;
            long lngStatus = 0;
            string strToDate = null;
            System.DBNull strNull = null;
            try
            {
                strSql += "     SELECT ROWNUM SR_NO,QRY.* FROM (";
                strSql += "     SELECT 0 TARIFF_PK,C.CONTAINER_TYPE_MST_PK,C.CONTAINER_TYPE_MST_ID, ";
                strSql += "     NVL(0,NULL) RATE,0 VERSION_NO ";
                strSql += "     FROM CONTAINER_TYPE_MST_TBL C WHERE C.ACTIVE_FLAG=1 ";
                strSql += "     AND C.HARD_CODED_CONTAINERS=0 ";
                strSql += "     ORDER BY C.CONTAINER_LENGTH_FT,C.CONTAINER_KIND)QRY ";
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(strSql);
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

        #endregion "Fetch Header - Others Tariff Entry"

        #region "Save - Insert - Others Tariff Entry"

        public ArrayList SaveOthersTariff(DataSet objDs, long businessModel, string standardFreightCode, long POL, long POD, long containerStatus, long chargeCode, long percentageOrAmount, System.DateTime fromDate, string toDate,
        long currency)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction insertTrans = null;
            System.DBNull strNull = null;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            DateTime enteredtodate = default(DateTime);
            if (!string.IsNullOrEmpty(toDate.Trim()))
            {
                enteredtodate = Convert.ToDateTime(toDate);
            }

            objWK.OpenConnection();
            insertTrans = objWK.MyConnection.BeginTransaction();

            try
            {
                var _with10 = insCommand;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".TARIFF_TRN_PKG.TARIFF_TRN_INS";
                var _with11 = _with10.Parameters;

                insCommand.Parameters.Add("TARIFF_PK_IN", OracleDbType.Int32, 10, "TARIFF_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["TARIFF_PK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("STANDARD_FREIGHT_RATE_FK_IN", "").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("BUSINESS_MODEL_IN", businessModel).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("STANDARD_FREIGHT_CODE_IN", standardFreightCode).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("POL_FK_IN", POL).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("POD_FK_IN", POD).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONTAINER_STATUS_IN", containerStatus).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", chargeCode).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("PERCENTAGE_AMOUNT_IN", percentageOrAmount).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CURRENCY_MST_FK_IN", currency).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONTAINTER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CONTAINER_TYPE_MST_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CONTAINTER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RATE_IN", OracleDbType.Int32, 10, "RATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["RATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("EFFECTIVE_FROM_IN", fromDate).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("EFFECTIVE_TO_IN", (!string.IsNullOrEmpty(toDate.Trim()) ? System.String.Format("{0:dd-MMM-yyyy}", enteredtodate) : "")).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CREATED_DT_IN", M_CREATED_DT).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("LAST_MODIFIED_DT_IN", M_LAST_MODIFIED_DT).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("SR_NO_IN", OracleDbType.Int32, 10, "SR_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["SR_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with12 = objWK.MyDataAdapter;
                _with12.UpdateCommand = insCommand;
                _with12.UpdateCommand.Transaction = insertTrans;
                RecAfct = _with12.Update(objDs);
                if (arrMessage.Count > 0)
                {
                    insertTrans.Rollback();
                    return arrMessage;
                }
                else
                {
                    insertTrans.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                insertTrans.Rollback();
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                throw ex;
                insertTrans.Rollback();
                arrMessage.Add(ex.Message);
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save - Insert - Others Tariff Entry"

        #region "Enum.."

        private enum HeaderTariffGRIContainer
        {
            SLNO = 0,
            TARIFFPK = 1,
            POLID = 2,
            POLPK = 3,
            PODID = 4,
            PODPK = 5,
            CONTAINER_TYPE_MST_ID = 6,
            CONTAINER_TYPE_PK = 7,
            CURRENCY_MST_ID = 8,
            CURRENCY_MST_PK = 9,
            RATE = 10,
            NEWRATE = 11,
            DELFLAG = 12,
            CHKFLAG = 13
        }

        #endregion "Enum.."

        #region "Fetch Tariff Rates.."

        public object FetchTariffRates(long business_Model, long freight_Element_FK, long container_Status, long pol_FK, long pod_FK, string str_FromDate, long sessionLoged)
        {
            string strSql = null;
            DateTime enteredFromDate = Convert.ToDateTime(str_FromDate);

            strSql = " SELECT Rownum SR_NO,QRY.* from ( ";
            strSql = strSql + " SELECT NVL(TT.TARIFF_PK,0) TARIFF_PK, ";
            strSql = strSql + " POL.PORT_ID POL,TT.POL_FK,POD.PORT_ID POD,TT.POD_FK,";
            strSql = strSql + " CMT.CONTAINER_TYPE_MST_ID, ";
            strSql = strSql + " TT.CONTAINTER_TYPE_MST_FK, ";
            strSql = strSql + " CUR.CURRENCY_ID, ";
            strSql = strSql + " CUR.CURRENCY_MST_PK, ";
            strSql = strSql + " TT.RATE, ";
            strSql = strSql + " 0 NEWRATE ";
            strSql = strSql + " FROM ";
            strSql = strSql + " TARIFF_TRN TT, ";
            strSql = strSql + " PORT_MST_TBL POL, ";
            strSql = strSql + " PORT_MST_TBL POD, ";
            strSql = strSql + " FREIGHT_ELEMENT_MST_TBL FRT, ";
            strSql = strSql + " CURRENCY_TYPE_MST_TBL CUR, ";
            strSql = strSql + " CONTAINER_TYPE_MST_TBL CMT, ";
            strSql = strSql + " LOC_PORT_MAPPING_TRN LPM ";
            strSql = strSql + " WHERE TT.POL_FK = POL.PORT_MST_PK ";
            strSql = strSql + " AND TT.POD_FK = POD.PORT_MST_PK";
            strSql = strSql + " AND TT.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK ";
            strSql = strSql + " AND TT.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)";
            strSql = strSql + " AND POL.PORT_MST_PK=LPM.PORT_MST_FK  ";
            strSql = strSql + " AND TT.CONTAINTER_TYPE_MST_FK = CMT.CONTAINER_TYPE_MST_PK";
            strSql = strSql + " AND LPM.LOCATION_MST_FK = " + sessionLoged;
            strSql = strSql + " AND FRT.ACTIVE_FLAG = 1 ";
            strSql = strSql + " AND FRT.SURCHARGE_TYPE NOT IN('THD','THL','PHL','PHD') ";
            strSql = strSql + " AND TT.BUSINESS_MODEL = " + business_Model;
            if (pol_FK > 0)
            {
                strSql = strSql + " AND TT.POL_FK = " + pol_FK;
            }
            if (pod_FK > 0)
            {
                strSql = strSql + " AND TT.POD_FK = " + pod_FK;
            }
            strSql = strSql + " AND TT.FREIGHT_ELEMENT_MST_FK = " + freight_Element_FK;
            strSql = strSql + " AND to_date('" + System.String.Format("{0:dd-MMM-yyyy}", enteredFromDate) + "','dd-Mon-yyyy') between TT.EFFECTIVE_FROM AND ";
            strSql = strSql + " nvl(TT.EFFECTIVE_TO,to_date('" + System.String.Format("{0:dd-MMM-yyyy}", enteredFromDate) + "')) ";
            strSql = strSql + " ORDER BY TT.STANDARD_FREIGHT_CODE ";
            strSql = strSql + " ) QRY ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSql);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Fetch Tariff Rates.."

        #region "Save - Insert GRI"

        public ArrayList SaveGRI(object UWG1, System.DateTime Effective_From_date, System.DateTime Effective_To_Date, int incr_Decr_Rate)
        {
            long RowCnt = 0;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction insertTrans = null;

            OracleCommand insCommand = new OracleCommand();
            var strNull = "";

            objWK.OpenConnection();
            insertTrans = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();

            try
            {
                var _with13 = insCommand;
                _with13.Connection = objWK.MyConnection;
                _with13.CommandType = CommandType.StoredProcedure;
                //for (int RowCnt = 0; RowCnt <= UWG1.Rows.Count() - 1; RowCnt++) {
                //	if ((UWG1.Rows(RowCnt).Cells(HeaderTariffGRIContainer.NEWRATE).Text > 0)) {
                //		_with13.CommandText = objWK.MyUserName + ".TARIFF_GRI_TRN_PKG.TARIFF_GRI_TRN_INS";
                //		_with13.Parameters.Clear();
                //		var _with14 = _with13.Parameters;
                //		_with14.Add("TARIFF_PK_IN", UWG1.Rows(RowCnt).Cells(HeaderTariffGRIContainer.TARIFFPK].Value).Direction = ParameterDirection.Input;
                //		_with14.Add("RATE_IN", UWG1.Rows(RowCnt).Cells(HeaderTariffGRIContainer.RATE].Value).Direction = ParameterDirection.Input;
                //		_with14.Add("INCR_DECR_RATE_IN", incr_Decr_Rate).Direction = ParameterDirection.Input;
                //		_with14.Add("EFFECT_FROM_IN", System.String.Format("{0:dd-MMM-yyyy}", Effective_From_date)).Direction = ParameterDirection.Input;
                //		_with14.Add("EFFECT_TO_IN", System.String.Format("{0:dd-MMM-yyyy}", Effective_To_Date)).Direction = ParameterDirection.Input;
                //		_with14.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                //		_with14.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                //		_with14.Add("SR_NO_IN", RowCnt).Direction = ParameterDirection.Input;
                //		_with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 256, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //		insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                //		var _with15 = objWK.MyDataAdapter;
                //		_with15.InsertCommand = insCommand;
                //		_with15.InsertCommand.Transaction = insertTrans;
                //		_with15.InsertCommand.ExecuteNonQuery();
                //	}
                //}

                if (arrMessage.Count > 0)
                {
                    insertTrans.Rollback();
                    return arrMessage;
                }
                else
                {
                    insertTrans.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    arrMessage.Add(insCommand.Parameters["RETURN_VALUE"].Value);
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                insertTrans.Rollback();
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                throw ex;
                insertTrans.Rollback();
                arrMessage.Add(ex.Message);
            }
        }

        #endregion "Save - Insert GRI"
    }
}