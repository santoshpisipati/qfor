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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_BANK_DETAILS : CommonFeatures
    {
        /// <summary>
        /// The bank MST pk
        /// </summary>
        public Int32 BankMstPk;

        /// <summary>
        /// The _DT bank
        /// </summary>
        private DataTable _dtBank;

        #region "Property"

        /// <summary>
        /// Gets the bank data.
        /// </summary>
        /// <value>
        /// The bank data.
        /// </value>
        public DataTable BankData
        {
            get { return _dtBank; }
        }

        #endregion "Property"

        #region "Constructors"

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_BANK_DETAILS"/> class.
        /// </summary>
        /// <param name="bShouldFetchData">if set to <c>true</c> [b should fetch data].</param>
        public cls_BANK_DETAILS(bool bShouldFetchData)
        {
            if (bShouldFetchData)
            {
                string strSQL = null;
                strSQL = " SELECT BANK_MST_PK," + " BANK_ID," + " BANK_NAME" + " FROM BANK_MST_TBL" + " WHERE BANK_MST_TBL.ACTIVE=1" + " ORDER BY BANK_ID";
                try
                {
                    _dtBank = (new WorkFlow()).GetDataTable(strSQL);
                    _dtBank.TableName = "BankDetails";
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            }
        }

        #endregion "Constructors"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="Import">if set to <c>true</c> [import].</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, bool Import = false)
        {
            //Sivachandran 05Jun08 Imp-Exp-Wiz16May08
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
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
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".BANK_MST_TBL_PKG.BANK_MST_TBL_INS";
                var _with2 = _with1.Parameters;
                //<PK>1</PK>
                //<ACTIVE_FLAG>1</ACTIVE_FLAG>
                //<BANKCODE>SBIBANK</BANKCODE>
                //<BANK_NAME>State Bank Of India</BANK_NAME>
                //<BANK_ADDR>Delhi</BANK_ADDR>
                //<VERSIONNO>0</VERSIONNO>
                //BANK_ID_IN()
                insCommand.Parameters.Add("BANK_ID_IN", OracleDbType.Varchar2, 20, "BANKCODE").Direction = ParameterDirection.Input;
                insCommand.Parameters["BANK_ID_IN"].SourceVersion = DataRowVersion.Current;
                //BANK_NAME_IN()
                insCommand.Parameters.Add("BANK_NAME_IN", OracleDbType.Varchar2, 100, "BANK_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["BANK_NAME_IN"].SourceVersion = DataRowVersion.Current;
                //BANK_ADDRESS_IN()
                insCommand.Parameters.Add("BANK_ADDRESS_IN", OracleDbType.Varchar2, 250, "BANK_ADDR").Direction = ParameterDirection.Input;
                insCommand.Parameters["BANK_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;
                //ACTIVE_FLAG_IN()
                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 10, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                //CREATED_BY_FK_IN()
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                //CONFIG_PK_IN()
                insCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                //insCommand.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current

                //RETURN_VALUE()
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "BANK_MST_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = delCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".BANK_MST_TBL_PKG.BANK_MST_TBL_DEL";
                var _with4 = _with3.Parameters;
                //<PK>1</PK>
                //<ACTIVE_FLAG>1</ACTIVE_FLAG>
                //<BANKCODE>SBIBANK</BANKCODE>
                //<BANK_NAME>State Bank Of India</BANK_NAME>
                //<BANK_ADDR>Delhi</BANK_ADDR>
                //<VERSIONNO>0</VERSIONNO>
                delCommand.Parameters.Add("BANK_MST_PK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["BANK_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSIONNO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = updCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".BANK_MST_TBL_PKG.BANK_MST_TBL_UPD";
                var _with6 = _with5.Parameters;

                updCommand.Parameters.Add("BANK_MST_PK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["BANK_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BANK_ID_IN", OracleDbType.Varchar2, 20, "BANKCODE").Direction = ParameterDirection.Input;
                updCommand.Parameters["BANK_ID_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BANK_NAME_IN", OracleDbType.Varchar2, 100, "BANK_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["BANK_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BANK_ADDRESS_IN", OracleDbType.Varchar2, 250, "BANK_ADDR").Direction = ParameterDirection.Input;
                updCommand.Parameters["BANK_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 10, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSIONNO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with7 = objWK.MyDataAdapter;

                _with7.InsertCommand = insCommand;
                _with7.InsertCommand.Transaction = TRAN;
                _with7.UpdateCommand = updCommand;
                _with7.UpdateCommand.Transaction = TRAN;
                _with7.DeleteCommand = delCommand;
                _with7.DeleteCommand.Transaction = TRAN;
                RecAfct = _with7.Update(M_DataSet);
                if (arrMessage.Count > 0)
                {
                    //This is if any error occurs then rollback..
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    //This part executes only when no error in the execution..
                    TRAN.Commit();
                    //Sivachandran 05Jun08 Imp-Exp-Wiz
                    if (Import == true)
                    {
                        arrMessage.Add("Data Imported Successfully");
                    }
                    else
                    {
                        arrMessage.Add("All Data Saved Successfully");
                    }
                    //End
                    return arrMessage;
                }
                //TRAN.Commit()
                //If arrMessage.Count > 0 Then
                //    Return arrMessage
                //Else
                //    arrMessage.Add("All Data Saved Successfully")
                //    Return arrMessage
                //End If
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

        #region "Fetch All"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="BankPk">The bank pk.</param>
        /// <param name="BankID">The bank identifier.</param>
        /// <param name="BankName">Name of the bank.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int16 BankPk = 0, string BankID = "", string BankName = "", string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int ActiveFlag = 1, Int16 SortCol = 3, bool blnSortAscending = false,
        Int32 flag = 0, Int32 Export = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (BankPk > 0)
            {
                strCondition += " AND Bank_mst_pk=" + BankPk;
            }
            if (BankID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(BANK_ID) LIKE '" + BankID.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(BANK_ID) LIKE '%" + BankID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(BANK_ID) LIKE '%" + BankID.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (BankName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(Bank_Name) LIKE '" + BankName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(Bank_Name) LIKE '%" + BankName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(Bank_Name) LIKE '%" + BankName.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (ActiveFlag == 1)
            {
                strCondition += " AND ACTIVE = 1 ";
            }
            else
            {
                strCondition += "";
            }

            strSQL = "SELECT Count(*) from BANK_MST_TBL where 1=1";
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

            //'If CInt(SortCol) > 0 Then
            //'    strCondition = strCondition & " order by " & CInt(SortCol)
            //'End If

            strSQL = " select * from (";
            strSQL += "SELECT ROWNUM SLNO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += "BANK_MST_PK PK, ";
            strSQL += "NVL(ACTIVE,0) ACTIVE_FLAG , ";
            strSQL += "BANK_ID BANKCODE,";
            strSQL += "BANK_NAME,BANK_ADDRESS BANK_ADDR, ";
            strSQL += "Version_No VersionNo ";
            strSQL += "FROM BANK_MST_TBL ";
            strSQL += "WHERE 1=1";

            strSQL += strCondition;

            //If SortExpression.Trim.Length > 0 Then
            //    strSQL = strSQL & " " & SortExpression
            //Else
            //    strSQL = strSQL & " order by Currency_ID"
            //End If
            strSQL += "order by BANK_ID";
            if (Export == 0)
            {
                strSQL += ") q  ) WHERE SLNO  Between " + start + " and " + last;
            }
            else
            {
                strSQL += ") q  )";
            }
            //strSQL &= vbCrLf & " Order By SR_NO"''order by active desc, BANK_ID
            try
            {
                return objWF.GetDataSet(strSQL);
                //Modified by Manjunath  PTS ID:Sep-02  12/09/2011
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

        #endregion "Fetch All"

        #region " Fetch for Import"

        //Sivachandran 07Jun08 Imp_Exp_Wiz16May08
        /// <summary>
        /// Fetches this instance.
        /// </summary>
        /// <returns></returns>
        public DataSet Fetch()
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            SQL = "SELECT bank_id as bankcode,bank_name,bank_address as bank_addr,active as active_flag FROM bank_mst_tbl";
            try
            {
                return objWF.GetDataSet(SQL);
                //Modified by Manjunath  PTS ID:Sep-02  12/09/2011
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

        //End

        #endregion " Fetch for Import"

        //Manjunath  11/10/2011  PTS:Oct-001

        #region "Fetch BankID"

        /// <summary>
        /// Fetches the bank.
        /// </summary>
        /// <returns></returns>
        public DataTable FetchBank()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = "SELECT B.BANK_ID BANK_ID, B.BANK_MST_PK FROM BANK_MST_TBL B ";
                strSQL += "order by upper (BANK_ID) ";
                return objWF.GetDataTable(strSQL);
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

        #endregion "Fetch BankID"

        #region "Fetch ItemDetails"

        /// <summary>
        /// Fetches the items.
        /// </summary>
        /// <param name="CURRENTPAGE">The currentpage.</param>
        /// <param name="TOTALPAGE">The totalpage.</param>
        /// <param name="BNKID">The bnkid.</param>
        /// <param name="BNKNAME">The bnkname.</param>
        /// <param name="COUNTRY">The country.</param>
        /// <param name="ACCNR">The accnr.</param>
        /// <param name="AccType">Type of the acc.</param>
        /// <param name="LOCATION">The location.</param>
        /// <param name="MICRNR">The micrnr.</param>
        /// <param name="CUR">The current.</param>
        /// <param name="ACTIVATE">The activate.</param>
        /// <param name="LogPk">The log pk.</param>
        /// <param name="FLAG">The flag.</param>
        /// <param name="ISADMINUSR">if set to <c>true</c> [isadminusr].</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <returns></returns>
        public DataSet FetchItems(Int32 CURRENTPAGE = 0, Int32 TOTALPAGE = 0, string BNKID = "", string BNKNAME = "", string COUNTRY = "", string ACCNR = "", string AccType = "0", string LOCATION = "", string MICRNR = "", string CUR = "",
        Int16 ACTIVATE = 0, string LogPk = "0", Int32 FLAG = 0, bool ISADMINUSR = false, string SearchType = "")
        {
            WorkFlow OBJWF = new WorkFlow();
            Int32 LAST = default(Int32);
            Int32 START = default(Int32);
            Int32 TOTALRECORDS = default(Int32);
            string STRCONDITION = null;
            string STRSQL = "";
            string STRSQL1 = "";
            if (FLAG == 0)
            {
                STRCONDITION = STRCONDITION + " AND  1=2 ";
            }
            if (ACTIVATE == 1)
            {
                STRCONDITION = STRCONDITION + " AND B.ACTIVE= 1 ";
            }
            if (!string.IsNullOrEmpty(BNKID))
            {
                if (SearchType == "S")
                {
                    STRCONDITION = STRCONDITION + " AND UPPER(B.BANK_ID) LIKE '" + BNKID + "%'";
                }
                else
                {
                    STRCONDITION = STRCONDITION + " AND UPPER(B.BANK_ID) LIKE '%" + BNKID + "%'";
                }
            }
            if (!string.IsNullOrEmpty(BNKNAME))
            {
                if (SearchType == "S")
                {
                    STRCONDITION = STRCONDITION + "  AND UPPER(B.BANK_NAME) LIKE '" + BNKNAME + "%'";
                }
                else
                {
                    STRCONDITION = STRCONDITION + "  AND UPPER(B.BANK_NAME) LIKE '%" + BNKNAME + "%'";
                }
            }
            if (!string.IsNullOrEmpty(COUNTRY))
            {
                if (SearchType == "S")
                {
                    STRCONDITION = STRCONDITION + "  AND UPPER(C.COUNTRY_ID) LIKE '" + COUNTRY + "%'";
                }
                else
                {
                    STRCONDITION = STRCONDITION + "  AND UPPER(C.COUNTRY_ID) LIKE '%" + COUNTRY + "%'";
                }
            }
            if (!string.IsNullOrEmpty(ACCNR))
            {
                if (SearchType == "S")
                {
                    STRCONDITION = STRCONDITION + "  AND UPPER(BS.ACCOUNT_NUMBER) LIKE '" + ACCNR + "%'";
                }
                else
                {
                    STRCONDITION = STRCONDITION + "  AND UPPER(BS.ACCOUNT_NUMBER) LIKE '%" + ACCNR + "%'";
                }
            }
            if (!string.IsNullOrEmpty(MICRNR))
            {
                if (SearchType == "S")
                {
                    STRCONDITION = STRCONDITION + " AND ( UPPER(BS.SWIFT_BIC_CODE) LIKE UPPER('" + MICRNR + "%')";
                    STRCONDITION = STRCONDITION + " OR UPPER(BS.IBAN) LIKE UPPER('" + MICRNR + "%')";
                    STRCONDITION = STRCONDITION + " OR UPPER(BS.MICR_NUMBER) LIKE UPPER('" + MICRNR + "%'))";
                }
                else
                {
                    STRCONDITION = STRCONDITION + " AND ( UPPER(BS.SWIFT_BIC_CODE) LIKE UPPER('%" + MICRNR + "%')";
                    STRCONDITION = STRCONDITION + " OR UPPER(BS.IBAN) LIKE UPPER('%" + MICRNR + "%')";
                    STRCONDITION = STRCONDITION + " OR UPPER(BS.MICR_NUMBER) LIKE UPPER('%" + MICRNR + "%'))";
                }
            }
            if (!string.IsNullOrEmpty(LOCATION))
            {
                if (SearchType == "S")
                {
                    STRCONDITION = STRCONDITION + "  AND UPPER(L.LOCATION_ID) LIKE '%" + LOCATION + "%'";
                }
                else
                {
                    STRCONDITION = STRCONDITION + "  AND UPPER(L.LOCATION_ID) LIKE '%" + LOCATION + "%'";
                }
            }

            if (!string.IsNullOrEmpty(CUR))
            {
                STRCONDITION = STRCONDITION + "  AND BS.CURRENCY_MST_FK= " + CUR;
            }
            if (AccType != "0")
            {
                STRCONDITION = STRCONDITION + "  AND BS.ACCOUNT_TYPE= " + AccType;
            }
            // STRSQL = "  SELECT * FROM ( "
            //STRSQL = "SELECT ROWNUM SR_NO,Q.* FROM ("
            STRSQL = " SELECT B.BANK_MST_PK,";
            STRSQL += " C.COUNTRY_NAME COUNTRY_ID,";
            STRSQL += " L.LOCATION_ID,";
            STRSQL += " B.BANK_ID,";
            STRSQL += " B.BANK_NAME,";
            STRSQL += " DECODE(BS.ACCOUNT_TYPE, 2, 'Current A/C', 1, 'Savings A/C') ACCOUNT_TYPE,";
            STRSQL += " BS.ACCOUNT_NUMBER,";
            STRSQL += " CT.CURRENCY_ID,";
            STRSQL += " BS.SWIFT_BIC_CODE,";
            STRSQL += " BS.IBAN,";
            STRSQL += " BS.MICR_NUMBER,";
            STRSQL += " B.BANK_ADDRESS,";
            STRSQL += " BS.BANK_ACCOUNT_SETUP_PK,";
            STRSQL += " B.EMAIL,";
            STRSQL += " B.VERSION_NO,";
            STRSQL += " '' DEL_FLAG";
            STRSQL += " FROM  BANK_MST_TBL B , BANK_ACCOUNT_SETUP  BS,";
            STRSQL += " LOCATION_MST_TBL L, COUNTRY_MST_TBL C,";
            STRSQL += " CURRENCY_TYPE_MST_TBL CT";
            STRSQL += " WHERE 1 = 1";
            STRSQL += " AND B.BANK_MST_PK = BS.BANK_MST_FK(+)";
            //strSQL &= vbCrLf & " AND L.LOCATION_MST_PK = B.LOCATION_MST_FK"
            //strSQL &= vbCrLf & " AND QL.LOCATION_MST_PK =L.QILS_LOCATION_MST_FK"
            STRSQL += " AND C.COUNTRY_MST_PK(+) = B.COUNTRY_MST_FK";
            STRSQL += " AND CT.CURRENCY_MST_PK(+)= BS.CURRENCY_MST_FK";
            STRSQL += " AND L.LOCATION_MST_PK(+) = B.LOCATION_MST_FK";
            //STRSQL &= vbCrLf & " AND QL.LOCATION_MST_PK=L.QILS_LOCATION_MST_FK AND "
            if (ISADMINUSR == false)
            {
                // STRSQL &= vbCrLf & " AND L.LOCATION_MST_PK  IN (SELECT LC.LOCATION_MST_PK FROM LOCATION_MST_TBL LC    START WITH LC.LOCATION_MST_PK  = " & LogPk & "   CONNECT BY PRIOR LC.LOCATION_MST_PK=LC.REPORTING_TO_FK ) "
                //    ' STRSQL &= vbCrLf & " AND L.LOCATION_MST_PK = B.LOCATION_MST_FK"
                //Else
                STRSQL += " AND L.LOCATION_MST_PK =" + LogPk;
            }
            STRSQL += STRCONDITION;
            STRSQL1 = " select count(*) from (";
            STRSQL1 += STRSQL.ToString() + ")";
            TOTALRECORDS = Convert.ToInt32(OBJWF.ExecuteScaler(STRSQL1));
            TOTALPAGE = TOTALRECORDS / M_MasterPageSize;
            if (TOTALRECORDS % M_MasterPageSize != 0)
            {
                TOTALPAGE += 1;
            }
            if (CURRENTPAGE > TOTALPAGE)
            {
                CURRENTPAGE = 1;
            }
            if (TOTALRECORDS == 0)
            {
                CURRENTPAGE = 0;
            }
            LAST = CURRENTPAGE * M_MasterPageSize;
            START = (CURRENTPAGE - 1) * M_MasterPageSize + 1;

            STRSQL1 = " SELECT * FROM (SELECT ROWNUM SR_NO, Q.* FROM(";
            STRSQL1 += STRSQL.ToString();
            STRSQL1 += "ORDER BY B.BANK_NAME )Q )";
            STRSQL1 += "  WHERE SR_NO  BETWEEN " + START + " AND " + LAST;

            try
            {
                return OBJWF.GetDataSet(STRSQL1);
            }
            catch (OracleException SQLEXP)
            {
                ErrorMessage = SQLEXP.Message;
                throw SQLEXP;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch ItemDetails"

        #region "delete for BankSetup"

        /// <summary>
        /// Deletes the save.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="DelFlg">if set to <c>true</c> [delete FLG].</param>
        /// <returns></returns>
        public int DelSave(DataSet M_DataSet, bool DelFlg = false)
        {
            //Public Function DelSave(ByRef Pk As Long, Optional ByVal DelFlg As Boolean = False)
            WorkFlow objWK = new WorkFlow();
            OracleTransaction oraTran = null;
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleDataAdapter da = new OracleDataAdapter();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();

            Int16 i = default(Int16);

            try
            {
                objWK.OpenConnection();

                oraTran = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = oraTran;

                for (i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++)
                {
                    if (Convert.ToBoolean(M_DataSet.Tables[0].Rows[i]["DEL_FLAG"]) == true | M_DataSet.Tables[0].Rows[i]["DEL_FLAG"] == "1")
                    {
                        DelSave1(Convert.ToInt32(M_DataSet.Tables[0].Rows[i]["bank_mst_pk"]));
                        var _with8 = objWK.MyCommand.Parameters;
                        _with8.Add("BANK_MST_PK_IN", M_DataSet.Tables[0].Rows[i]["bank_mst_pk"]).Direction = ParameterDirection.Input;
                        _with8.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                        _with8.Add("Version_No_IN", M_DataSet.Tables[0].Rows[i]["version_no"]).Direction = ParameterDirection.Input;
                        _with8.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                        _with8.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                        objWK.MyCommand.CommandText = objWK.MyUserName + (".BANK_MST_TBL_PKG.BANK_MST_TBL_DEL");

                        if (objWK.MyCommand.ExecuteNonQuery() == 1)
                        {
                        }
                        else
                        {
                            oraTran.Rollback();
                            return -1;
                        }
                    }
                    // AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)
                    objWK.MyCommand.Parameters.Clear();
                }
                oraTran.Commit();
                return 1;
            }
            catch (Exception ex)
            {
                string a = ex.Message;
                oraTran.Rollback();
                return -1;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        #endregion "delete for BankSetup"

        #region "delete details  for banksetup"

        /// <summary>
        /// Deletes the save1.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public object DelSave1(int pk)
        {
            //Public Function DelSave(ByRef Pk As Long, Optional ByVal DelFlg As Boolean = False)
            WorkFlow objWK = new WorkFlow();
            OracleTransaction oraTran = null;
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleDataAdapter da = new OracleDataAdapter();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            Int16 i = default(Int16);

            try
            {
                objWK.OpenConnection();

                oraTran = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = oraTran;

                OracleCommand delCommand = new OracleCommand("delete from bank_account_setup where bank_mst_fk=" + pk, oraTran.Connection);

                var _with9 = objWK.MyDataAdapter;
                _with9.DeleteCommand = delCommand;
                _with9.DeleteCommand.Transaction = oraTran;
                _with9.DeleteCommand.ExecuteNonQuery();
                oraTran.Commit();
            }
            catch (Exception ex)
            {
                string a = ex.Message;
                oraTran.Rollback();
                return -1;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
            return new object();
        }

        #endregion "delete details  for banksetup"

        #region "Fetch Currency"

        /// <summary>
        /// Fetches the currency.
        /// </summary>
        /// <param name="CurrencyPK">The currency pk.</param>
        /// <param name="CurrencyID">The currency identifier.</param>
        /// <param name="CurrencyName">Name of the currency.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <returns></returns>
        public DataSet FetchCurrency(Int16 CurrencyPK = 0, string CurrencyID = "", string CurrencyName = "", bool ActiveOnly = true)
        {
            string strSQL = null;
            strSQL = "select ' ' CURRENCY_ID,";
            strSQL = strSQL + "'' CURRENCY_NAME, ";
            strSQL = strSQL + "0 CURRENCY_MST_PK ";
            strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL ";
            strSQL = strSQL + "UNION ";
            strSQL = strSQL + "Select CURRENCY_ID, ";
            strSQL = strSQL + "CURRENCY_NAME,";
            strSQL = strSQL + "CURRENCY_MST_PK ";
            strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL Where 1=1 ";
            if (ActiveOnly)
            {
                strSQL = strSQL + " And Active_Flag = 1  ";
            }
            strSQL = strSQL + "order by CURRENCY_ID";

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

        #endregion "Fetch Currency"

        #region "Fetch ItemDetails1"

        /// <summary>
        /// Fetches the items1.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="chks">The CHKS.</param>
        /// <returns></returns>
        public DataSet FetchItems1(long PK, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 chks = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            strSQL = " select Count(*)  from bank_account_setup i where i.bank_mst_fk= " + PK;

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

            if (PK != 0)
            {
                strSQL = "  Select * From ( ";
                strSQL = strSQL + "SELECT ROWNUM SLNO,q.* From (";
                //strSQL &= vbCrLf & " select i.bank_account_setup_pk,i.account_number,case when(i.account_type = 1) then 'Savings A/C' else 'Current A/C' end  account_type,i.version_no,i.swift_bic_code,i.banking_number,i.iban,i.CURRENCY_MST_FK,C.CURRENCY_ID,'false' del_flag  from bank_account_setup i ,CURRENCY_TYPE_MST_TBL C where I.currency_mst_fk= C.CURRENCY_MST_PK(+) AND i.bank_mst_fk = " & PK & " "
                strSQL += " select i.bank_account_setup_pk,i.account_number,case when(i.account_type = 1) then 'Savings A/C' else 'Current A/C' end  account_type,i.version_no,i.swift_bic_code,i.banking_number, i.MICR_NUMBER, i.iban,i.CURRENCY_MST_FK,C.CURRENCY_ID,'false' del_flag  from bank_account_setup i ,CURRENCY_TYPE_MST_TBL C where I.currency_mst_fk= C.CURRENCY_MST_PK(+) AND i.bank_mst_fk = " + PK + " ";
                strSQL += " )q ) ";
            }
            else
            {
                strSQL = "  Select * From ( ";
                strSQL = strSQL + "SELECT ROWNUM SLNO,q.* From (";
                //strSQL &= vbCrLf & " select i.bank_account_setup_pk,i.account_number ,i.account_type ,i.version_no,i.swift_bic_code,i.banking_number,i.iban,i.CURRENCY_MST_FK,C.CURRENCY_ID,'false' del_flag  from bank_account_setup i ,CURRENCY_TYPE_MST_TBL C where I.currency_mst_fk= C.CURRENCY_MST_PK(+) AND i.bank_mst_fk = " & PK & " "
                strSQL += " select i.bank_account_setup_pk,i.account_number, i.account_type, i.version_no, i.swift_bic_code,i.banking_number, i.MICR_NUMBER, i.iban,i.CURRENCY_MST_FK,C.CURRENCY_ID,'false' del_flag  from bank_account_setup i ,CURRENCY_TYPE_MST_TBL C where I.currency_mst_fk= C.CURRENCY_MST_PK(+) AND i.bank_mst_fk = " + PK + " ";
                strSQL += " )q ) ";
            }

            if (chks == 1)
            {
                strSQL = "select bank_mst_tbl.bank_mst_pk,";
                strSQL = strSQL + " bank_mst_tbl.bank_id,";
                strSQL += "bank_mst_tbl.bank_name,";
                strSQL += "bank_mst_tbl.bank_address,";
                strSQL += "LOCATION_MST_TBL.location_Name,";
                strSQL += "country_mst_tbl.country_mst_pk,";
                strSQL += "country_mst_tbl.country_name,";
                strSQL += "bank_mst_tbl.person_in_charge,";
                strSQL += "bank_mst_tbl.email,bank_mst_tbl.version_no,bank_mst_tbl.Active,bank_mst_tbl.invoice,LOCATION_MST_TBL.location_mst_pk, ";

                //added by minakshi
                strSQL += "bank_mst_tbl.STATE";
                //ended by minakshi
                strSQL += " from  bank_mst_tbl , LOCATION_MST_TBL,COUNTRY_MST_TBL ";
                strSQL += "where ";
                strSQL += "LOCATION_MST_TBL.LOCATION_MST_PK(+) = bank_mst_tbl.location_mst_fk  ";
                strSQL += " AND COUNTRY_MST_TBL.COUNTRY_MST_PK(+) = BANK_MST_TBL.COUNTRY_MST_FK  ";
                strSQL += "and bank_mst_tbl.bank_mst_pk = " + PK;
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

        #endregion "Fetch ItemDetails1"

        #region " Temp dataset fro banksetup"

        /// <summary>
        /// Temp_s the ds.
        /// </summary>
        /// <returns></returns>
        public object temp_DS()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            //   strSQL = " select  '' tbnkid,'' tbnkname,0 tisactive,0 tisinvoice,'' taddress,0 tlocation,'' tpic,'' temail,'' tstate,0 vers from dual"
            strSQL = " select  '' tbnkid,'' tbnkname,0 tisactive,0 tisinvoice,'' taddress,0 tlocation,'' tpic,'' temail,'' tstate,0 vers, 0 country from dual";
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

        #endregion " Temp dataset fro banksetup"

        #region "Insert for invoice"

        /// <summary>
        /// Chkinvoices the specified locpk.
        /// </summary>
        /// <param name="locpk">The locpk.</param>
        /// <param name="bankid">The bankid.</param>
        /// <returns></returns>
        public int chkinvoice(Int64 locpk, string bankid)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("select count(*) from bank_mst_tbl b where b.location_mst_fk = " + locpk);
            sqlstr.Append(" and b.invoice=1 and upper(b.bank_id) <> '" + bankid.ToUpper() + "' ");

            try
            {
                return Convert.ToInt32(objWK.ExecuteScaler(sqlstr.ToString()));
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

        #endregion "Insert for invoice"

        #region " Swift Code"

        /// <summary>
        /// Swifts this instance.
        /// </summary>
        /// <returns></returns>
        public object swift()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "select upper(bank.swift_bic_code) as swift_bic_code from bank_account_setup bank";
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

        /// <summary>
        /// Swift1s this instance.
        /// </summary>
        /// <returns></returns>
        public object swift1()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "select * from bank_account_setup bank";
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

        #endregion " Swift Code"

        #region "GetBankName"

        /// <summary>
        /// Gets the name of the bank.
        /// </summary>
        /// <param name="BankCod">The bank cod.</param>
        /// <returns></returns>
        public string GetBankName(Int32 BankCod)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                return objWF.ExecuteScaler("SELECT B.BANK_NAME FROM BANK_MST_TBL B WHERE B.BANK_ID = '" + BankCod + "'");
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "GetBankName"

        #region "Check Account/Currency"

        /// <summary>
        /// CHKs the account number.
        /// </summary>
        /// <param name="bankaccount">The bankaccount.</param>
        /// <returns></returns>
        public string chkAccountNumber(string bankaccount)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("select b.account_number from bank_account_setup b where b.account_number = '" + bankaccount + "' ");
            //sqlstr.Append(" and b.invoice=1 and upper(b.bank_id) <> '" & bankid.ToUpper & "' ")

            try
            {
                return objWK.ExecuteScaler(sqlstr.ToString());
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

        /// <summary>
        /// CHKs the currency.
        /// </summary>
        /// <param name="bankid">The bankid.</param>
        /// <param name="currency">The currency.</param>
        /// <returns></returns>
        public string chkCurrency(string bankid, int currency)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("select b.currency_mst_fk from bank_account_setup b, bank_mst_tbl bmt  where b.bank_mst_fk = bmt.bank_mst_pk  and bmt.bank_id ='" + bankid + "' and b.currency_mst_fk = " + currency);
            //sqlstr.Append(" and b.invoice=1 and upper(b.bank_id) <> '" & bankid.ToUpper & "' ")

            try
            {
                return objWK.ExecuteScaler(sqlstr.ToString());
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

        #endregion "Check Account/Currency"
    }
}