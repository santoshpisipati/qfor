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
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
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
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_Airline_Definition : CommonFeatures
    {
        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="AirLineMSTPK">The air line MSTPK.</param>
        /// <param name="AirLineID">The air line identifier.</param>
        /// <param name="AirLine_Name">Name of the air line_.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int64 AirLineMSTPK = 0, string AirLineID = "", string AirLine_Name = "", string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int ActiveFlag = 1, Int16 SortCol = 3, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (AirLineMSTPK > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and ALM.AIRLINE_MST_PK like '%" + AirLineMSTPK + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and ALM.AIRLINE_MST_PK like '" + AirLineMSTPK + "%'";
                }
            }

            if (AirLineID.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(ALM.AIRLINE_ID) like '%" + AirLineID.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(ALM.AIRLINE_ID) like '" + AirLineID.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (AirLine_Name.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(ALM.AIRLINE_NAME) like '%" + AirLine_Name.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(ALM.AIRLINE_NAME) like '" + AirLine_Name.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (ActiveFlag > 0)
            {
                strCondition = strCondition + " and ALM.ACTIVE_FLAG = 1";
            }

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            strSQL = "SELECT Count(*) from AIRLINE_MST_TBL ALM";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
            strSQL += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

            strSQL = " SELECT * from (";
            strSQL = strSQL + "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL = strSQL + " (SELECT ALM.AIRLINE_MST_PK,";
            strSQL = strSQL + "ALM.ACTIVE_FLAG,";
            strSQL = strSQL + "ALM.AIRLINE_ID, ";
            strSQL = strSQL + "ALM.AIRLINE_NAME,";
            strSQL = strSQL + "ALM.VERSION_NO";

            strSQL = strSQL + " FROM AIRLINE_MST_TBL ALM";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
            strSQL = strSQL + strCondition;
            strSQL = strSQL + "ORDER BY ALM.AIRLINE_ID";
            strSQL = strSQL + ") q  )WHERE SR_NO  Between " + start + " and " + last;
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

        #endregion "Fetch Function"

        #region "Define ENUM"

        /// <summary>
        ///
        /// </summary>
        private enum Header
        {
            /// <summary>
            /// The slno
            /// </summary>
            SLNO = 0,

            /// <summary>
            /// The airlineid
            /// </summary>
            AIRLINEID = 1,

            /// <summary>
            /// The airlinename
            /// </summary>
            AIRLINENAME = 2,

            /// <summary>
            /// The airlinefk
            /// </summary>
            AIRLINEFK = 3,

            /// <summary>
            /// The delete
            /// </summary>
            DELETE = 4
        }

        #endregion "Define ENUM"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
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
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".AIRLINE_MST_TBL_PKG.AIRLINE_MST_TBL_INS";
                var _with2 = _with1.Parameters;

                insCommand.Parameters.Add("AIRLINE_ID_IN", OracleDbType.Varchar2, 20, "AIRLINE_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["AIRLINE_ID_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("AIRLINE_NAME_IN", OracleDbType.Varchar2, 50, "AIRLINE_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["AIRLINE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "AIRLINE_MST_TBL_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = delCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".AIRLINE_MST_TBL_PKG.AIRLINE_MST_TBL_DEL";
                var _with4 = _with3.Parameters;
                delCommand.Parameters.Add("AIRLINE_MST_PK_IN", OracleDbType.Int32, 10, "AIRLINE_MST_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["AIRLINE_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = updCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".AIRLINE_MST_TBL_PKG.AIRLINE_MST_TBL_UPD";
                var _with6 = _with5.Parameters;

                updCommand.Parameters.Add("AIRLINE_MST_PK_IN", OracleDbType.Int32, 10, "AIRLINE_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["AIRLINE_MST_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("AIRLINE_ID_IN", OracleDbType.Varchar2, 20, "AIRLINE_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["AIRLINE_ID_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("AIRLINE_NAME_IN", OracleDbType.Varchar2, 50, "AIRLINE_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["AIRLINE_NAME_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with7 = objWK.MyDataAdapter;

                _with7.InsertCommand = insCommand;
                _with7.UpdateCommand = updCommand;
                _with7.DeleteCommand = delCommand;
                RecAfct = _with7.Update(M_DataSet);
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
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

        #region "Fetch AIRLINEName"

        /// <summary>
        /// Fetches the name of the airline.
        /// </summary>
        /// <param name="AIRLINEPK">The airlinepk.</param>
        /// <returns></returns>
        public object FetchAIRLINEName(long AIRLINEPK = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT";
            strSQL += " ALM.AIRLINE_ID, ";
            strSQL += " ALM.AIRLINE_NAME,";
            strSQL += " ALM.AIRLINE_MST_PK ";
            strSQL += " FROM AIRLINE_MST_TBL ALM ";

            if (AIRLINEPK > 0)
            {
                strSQL += "WHERE ALM.AIRLINE_MST_PK=" + AIRLINEPK;
            }
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

        #endregion "Fetch AIRLINEName"

        #region "Fetch Sectors"

        /// <summary>
        /// Fetches the sectors.
        /// </summary>
        /// <param name="FromCountPK">From count pk.</param>
        /// <param name="FromPortPK">From port pk.</param>
        /// <param name="ToCountryPK">To country pk.</param>
        /// <param name="ToPortPK">To port pk.</param>
        /// <param name="AIRLINEMSTFK">The airlinemstfk.</param>
        /// <param name="ReturnSect">if set to <c>true</c> [return sect].</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchSectors(long FromCountPK = 0, long FromPortPK = 0, long ToCountryPK = 0, long ToPortPK = 0, long AIRLINEMSTFK = 0, bool ReturnSect = false, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQL = "SELECT Count(*) from SECTOR_MST_TBL SMT, PORT_MST_TBL POL,";
            strSQL += "PORT_MST_TBL POD,COUNTRY_MST_TBL POLC,COUNTRY_MST_TBL PODC ";
            strSQL += "WHERE SMT.FROM_PORT_FK=POL.PORT_MST_PK";
            strSQL += "AND SMT.TO_PORT_FK=POD.PORT_MST_PK";
            strSQL += "AND POL.COUNTRY_MST_FK=POLC.COUNTRY_MST_PK";
            strSQL += "AND POD.COUNTRY_MST_FK=PODC.COUNTRY_MST_PK";

            if (FromCountPK > 0 & ReturnSect == false)
            {
                strSQL += "AND (POLC.COUNTRY_MST_PK= " + FromCountPK + ")";
            }
            else if (FromCountPK > 0 & ReturnSect == true)
            {
                strSQL += "  AND (POLC.COUNTRY_MST_PK= " + FromCountPK + "OR PODC.COUNTRY_MST_PK= " + FromCountPK + ")";
            }
            else
            {
            }

            if (ToCountryPK > 0 & ReturnSect == false)
            {
                strSQL += " AND (PODC.COUNTRY_MST_PK= " + ToCountryPK + ")";
            }
            else if (ToCountryPK > 0 & ReturnSect == true)
            {
                strSQL += "AND (PODC.COUNTRY_MST_PK= " + ToCountryPK + "OR POLC.COUNTRY_MST_PK= " + ToCountryPK + ")";
            }
            else
            {
            }

            if (FromPortPK > 0 & ReturnSect == false)
            {
                strSQL += "AND (SMT.FROM_PORT_FK = " + FromPortPK + ")";
            }
            else if (FromPortPK > 0 & ReturnSect == true)
            {
                strSQL += "AND (SMT.FROM_PORT_FK = " + FromPortPK + "OR SMT.TO_PORT_FK= " + FromPortPK + ")";
            }
            else
            {
            }

            if (ToPortPK > 0 & ReturnSect == false)
            {
                strSQL += "AND (SMT.TO_PORT_FK= " + ToPortPK + ")";
            }
            else if (ToPortPK > 0 & ReturnSect == true)
            {
                strSQL += "AND (SMT.TO_PORT_FK= " + ToPortPK + "OR SMT.FROM_PORT_FK = " + ToPortPK + " )";
            }
            else
            {
            }

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            strSQL = "SELECT ROWNUM SR_NO,QRY.* FROM (SELECT";
            strSQL += "SMT.SECTOR_MST_PK,";
            strSQL += "(CASE WHEN NVL(SMT.AIRLINE_MST_FK,0)=0 THEN  0 ELSE 1 END ) ACT,";
            strSQL += "SMT.AIRLINE_MST_FK,";
            strSQL += "SMT.TLI_REF_NO,";
            strSQL += "SMT.SECTOR_ID,";
            strSQL += "POLC.COUNTRY_ID FROM_COUNTRY,";
            strSQL += "PODC.COUNTRY_ID TO_COUNTRY,";
            strSQL += "POL.PORT_ID FROM_PORT,";
            strSQL += "POD.PORT_ID TO_PORT,";
            strSQL += "SMT.VERSION_NO";
            strSQL += "FROM";
            strSQL += "SECTOR_MST_TBL SMT,";
            strSQL += "PORT_MST_TBL POL,";
            strSQL += "PORT_MST_TBL POD,";
            strSQL += "COUNTRY_MST_TBL POLC,";
            strSQL += "COUNTRY_MST_TBL PODC";
            strSQL += "WHERE";
            strSQL += "SMT.FROM_PORT_FK=POL.PORT_MST_PK";
            strSQL += "AND SMT.TO_PORT_FK=POD.PORT_MST_PK";
            strSQL += "AND POL.COUNTRY_MST_FK=POLC.COUNTRY_MST_PK";
            strSQL += "AND POD.COUNTRY_MST_FK=PODC.COUNTRY_MST_PK";

            if (FromCountPK > 0 & ReturnSect == false)
            {
                strSQL += "AND (POLC.COUNTRY_MST_PK= " + FromCountPK + ")";
            }
            else if (FromCountPK > 0 & ReturnSect == true)
            {
                strSQL += "  AND (POLC.COUNTRY_MST_PK= " + FromCountPK + "OR PODC.COUNTRY_MST_PK= " + FromCountPK + ")";
            }
            else
            {
            }

            if (ToCountryPK > 0 & ReturnSect == false)
            {
                strSQL += " AND (PODC.COUNTRY_MST_PK= " + ToCountryPK + ")";
            }
            else if (ToCountryPK > 0 & ReturnSect == true)
            {
                strSQL += "AND (PODC.COUNTRY_MST_PK= " + ToCountryPK + "OR POLC.COUNTRY_MST_PK= " + ToCountryPK + ")";
            }
            else
            {
            }

            if (FromPortPK > 0 & ReturnSect == false)
            {
                strSQL += "AND (SMT.FROM_PORT_FK = " + FromPortPK + ")";
            }
            else if (FromPortPK > 0 & ReturnSect == true)
            {
                strSQL += "AND (SMT.FROM_PORT_FK = " + FromPortPK + "OR SMT.TO_PORT_FK= " + FromPortPK + ")";
            }
            else
            {
            }

            if (ToPortPK > 0 & ReturnSect == false)
            {
                strSQL += "AND (SMT.TO_PORT_FK= " + ToPortPK + ")";
            }
            else if (ToPortPK > 0 & ReturnSect == true)
            {
                strSQL += "AND (SMT.TO_PORT_FK= " + ToPortPK + "OR SMT.FROM_PORT_FK = " + ToPortPK + " )";
            }
            else
            {
            }
            strSQL += "AND (SMT.AIRLINE_MST_FK= " + AIRLINEMSTFK + "OR SMT.AIRLINE_MST_FK IS NULL)";
            strSQL += "ORDER BY SMT.TLI_REF_NO) QRY";

            DataSet objDS = null;

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

        #endregion "Fetch Sectors"

        #region "Update Sector Table"

        /// <summary>
        /// Saves the sector.
        /// </summary>
        /// <param name="SectPK">The sect pk.</param>
        /// <param name="AIRLINEFK">The airlinefk.</param>
        /// <returns></returns>
        public ArrayList SaveSector(long SectPK = 0, long AIRLINEFK = 0)
        {
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);

            Int32 inti = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            objWS.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWS.MyConnection.BeginTransaction();

            var _with8 = insCommand;
            _with8.Connection = objWS.MyConnection;
            _with8.CommandType = CommandType.StoredProcedure;
            _with8.CommandText = objWS.MyUserName + ".AIRLINE_MST_TBL_PKG.update_AIRLINE_mst_pk";
            var _with9 = _with8.Parameters;
            try
            {
                _with9.Add("sector_mst_pk_in", SectPK).Direction = ParameterDirection.Input;
                _with9.Add("AIRLINE_mst_fk_in", AIRLINEFK).Direction = ParameterDirection.Input;
                _with9.Add("last_modified_by_fk_in", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            try
            {
                var _with10 = objWS.MyDataAdapter;
                _with10.InsertCommand = insCommand;
                _with10.InsertCommand.Transaction = TRAN;
                _with10.InsertCommand.ExecuteNonQuery();
                TRAN.Commit();
                arrMessage.Add("Saved");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add("Not Saved");
                return arrMessage;
            }
            finally
            {
                objWS.CloseConnection();
            }
        }

        #endregion "Update Sector Table"

        #region "UpdateSector"

        /// <summary>
        /// Updates the sector.
        /// </summary>
        /// <param name="SectPK">The sect pk.</param>
        /// <returns></returns>
        public object UpdateSector(long SectPK)
        {
            WorkFlow objWS = new WorkFlow();
            Int32 intPkVal = default(Int32);

            Int32 inti = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            objWS.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWS.MyConnection.BeginTransaction();

            var _with11 = insCommand;
            _with11.Connection = objWS.MyConnection;
            _with11.CommandType = CommandType.StoredProcedure;
            _with11.CommandText = objWS.MyUserName + ".AIRLINE_MST_TBL_PKG.Update_sector_mst_tbl";
            var _with12 = _with11.Parameters;
            try
            {
                _with12.Add("sector_mst_pk_in", SectPK).Direction = ParameterDirection.Input;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            try
            {
                var _with13 = objWS.MyDataAdapter;
                _with13.InsertCommand = insCommand;
                _with13.InsertCommand.Transaction = TRAN;
                _with13.InsertCommand.ExecuteNonQuery();
                TRAN.Commit();
                arrMessage.Add("Saved");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add("Not Saved");
                return arrMessage;
            }
            finally
            {
                objWS.CloseConnection();
            }
        }

        #endregion "UpdateSector"

        #region " Enhance Search Function "

        /// <summary>
        /// Fetches the airline.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAirline(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            int locfk = 0;
            dynamic strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (Convert.ToString(arr.GetValue(2)) == "AIRLINE")
            {
                locfk = Convert.ToInt32(arr.GetValue(4));
            }
            else
            {
                locfk = Convert.ToInt32(arr.GetValue(5));
            }
            //locfk = arr(2)

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_AIRLINE_PKG.GETAIRLINE";

                var _with14 = selectCommand.Parameters;
                _with14.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with14.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with14.Add("LOCATION_MST_FK_IN", locfk).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search Function "

        #region " Enhance Search Function "

        /// <summary>
        /// Fetches the airline for enquiry.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAirlineForEnquiry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            int locfk = 0;
            int POLfk = 0;
            int PODfk = 0;
            int ddTariffType = 0;
            dynamic strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            locfk = Convert.ToInt32(arr.GetValue(2));
            POLfk = Convert.ToInt32(arr.GetValue(3));
            PODfk = Convert.ToInt32(arr.GetValue(4));
            if (arr.Length > 5)
            {
                ddTariffType = Convert.ToInt32(arr.GetValue(5)); Convert.ToString(arr.GetValue(5));
            }

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_AIRLINE_PKG.GET_AIRLINE_ENQUIRY_COMMON";

                var _with15 = selectCommand.Parameters;
                _with15.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with15.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with15.Add("LOCATION_MST_FK_IN", locfk).Direction = ParameterDirection.Input;
                _with15.Add("SEARCH_POL_FK_IN", POLfk).Direction = ParameterDirection.Input;
                _with15.Add("SEARCH_POD_FK_IN", PODfk).Direction = ParameterDirection.Input;
                _with15.Add("SEARCH_TARIFF_TYPE_IN", ddTariffType).Direction = ParameterDirection.Input;
                _with15.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search Function "

        #region " Enhance Search Function "

        /// <summary>
        /// Fetches the airline for enquiry export manifest.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAirlineForEnquiryExportManifest(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            int locfk = 0;
            int POLfk = 0;
            int PODfk = 0;
            dynamic strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            locfk = Convert.ToInt32(arr.GetValue(2));
            POLfk = Convert.ToInt32(arr.GetValue(3));
            PODfk = Convert.ToInt32(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_AIRLINE_PKG.GET_AIRLINE_EXPMANIFEST";

                var _with16 = selectCommand.Parameters;
                _with16.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with16.Add("LOCATION_MST_FK_IN", locfk).Direction = ParameterDirection.Input;
                _with16.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with16.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search Function "

        #region "FetchAirline_WF"

        /// <summary>
        /// Fetches the airline_ wf.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAirline_WF(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            int locfk = 0;
            dynamic strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            locfk = Convert.ToInt32(arr.GetValue(2));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_AIRLINE_PKG.GETAIRLINE_WF";

                var _with17 = selectCommand.Parameters;
                _with17.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with17.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with17.Add("LOCATION_MST_FK_IN", locfk).Direction = ParameterDirection.Input;
                _with17.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
                selectCommand.Connection.Close();
            }
        }

        #endregion "FetchAirline_WF"

        #region "FetchAirlineWithPrefix"

        /// <summary>
        /// Fetches the airline with prefix.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAirlineWithPrefix(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            int locfk = 0;
            dynamic strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            locfk = Convert.ToInt32(arr.GetValue(2));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_AIRLINE_WITHPREFIX.GETAIRLINE";

                var _with18 = selectCommand.Parameters;
                _with18.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with18.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with18.Add("LOCATION_MST_FK_IN", locfk).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
                selectCommand.Connection.Close();
            }
        }

        #endregion "FetchAirlineWithPrefix"
    }
}