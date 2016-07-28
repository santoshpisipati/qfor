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
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class cls_DetentionCalculation : CommonFeatures
    {
        protected Int64 Invpk = 0;
        string strReturnPK;
        WorkFlow objWF = new WorkFlow();
        public int JcOthTrnPk;
        public int ConsInvPk;
        public string pstrReturn
        {
            get { return strReturnPK; }
        }

        #region " Enhcance search:- Jobcard No,Warehouse Id,Detention Ref "
        public string FetchForJobRefDetention(string strSearchFlag, string strCond)
        {
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strCargo = null;
            string strPK = null;
            string strLOCATION_IN = "";
            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strCargo = arr[3];
            strPK = arr[4];
            if (arr.Length > 5)
                strLOCATION_IN = arr[5];

            try
            {
                objWF.OpenConnection();
                objWF.MyCommand.Connection = objWF.MyConnection;
                objWF.MyCommand.CommandType = CommandType.StoredProcedure;
                objWF.MyCommand.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_IMP_DETN";

                var _with1 = objWF.MyCommand.Parameters;
                _with1.Clear();
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("CARGO_TYPE_IN", strCargo).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("PK_IN", (strPK != "0" & !string.IsNullOrEmpty(strPK) ? strPK : "")).Direction = ParameterDirection.Input;
                _with1.Add("SearchFlag", strSearchFlag).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                objWF.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                objWF.MyCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)objWF.MyCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
                //strReturn = CStr(objWF.MyCommand.Parameters["RETURN_VALUE").Value)
                //Return strReturn
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyCommand.Connection.Close();
            }
        }
        public string FetchWarehouse(string strCond)
        {
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strCargo = null;

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strCargo = arr[3];

            try
            {
                objWF.OpenConnection();
                objWF.MyCommand.Connection = objWF.MyConnection;
                objWF.MyCommand.CommandType = CommandType.StoredProcedure;
                //COMMENTED BY THANGADURAI on 4/8/08
                //objWF.MyCommand.CommandText = objWF.MyUserName & ".EN_DEPOT_PKG.GET_WARE_HOUSE"
                //objWF.MyCommand.CommandText = objWF.MyUserName & ".EN_DEPOT_PKG.GET_WAREHOUSE"
                objWF.MyCommand.CommandText = objWF.MyUserName + ".EN_DEPOT_PKG.GET_WAREHOUSE_DEMURAGE";

                var _with2 = objWF.MyCommand.Parameters;
                _with2.Clear();
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_TYPE_IN", strCargo).Direction = ParameterDirection.Input;

                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                objWF.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                objWF.MyCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(objWF.MyCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyCommand.Connection.Close();
            }
        }
        public string FetchDetentionRef(string strCond)
        {
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strCargo = null;
            string strLOCATION_IN = "";
            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strCargo = arr[3];
            if (arr.Length > 4)
                strLOCATION_IN = arr[4];
            try
            {
                objWF.OpenConnection();
                objWF.MyCommand.Connection = objWF.MyConnection;
                objWF.MyCommand.CommandType = CommandType.StoredProcedure;
                objWF.MyCommand.CommandText = objWF.MyUserName + ".EN_DET_CALC_REF_NO_PKG.GET_DET_REF_NO";

                var _with3 = objWF.MyCommand.Parameters;
                _with3.Clear();
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with3.Add("CARGO_TYPE_IN", strCargo).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                objWF.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                objWF.MyCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(objWF.MyCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyCommand.Connection.Close();
            }
        }
        #endregion

        #region " Detention Calculation Listing Screen "

        #region "Fetch Detention List "

        public DataSet FetchDetnList(string JOB_CARD_REF_NO = "", string DETENTION_REF_NO = "", string DEPOT_ID = "", Int16 CARGO_TYPE = 0, string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string SortCol = "",
        Int64 JOB_CARD_SEA_IMP_FK = 0, Int64 DETENTION_CALC_TBL_PK = 0, Int64 DEPOT_MST_FK = 0, long lngUsrLocFk = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (JOB_CARD_SEA_IMP_FK > 0)
            {
                strCondition += " AND DCT.JOB_CARD_SEA_IMP_FK=" + JOB_CARD_SEA_IMP_FK;
            }
            if (DETENTION_CALC_TBL_PK > 0)
            {
                strCondition += " AND DCT.DETENTION_CALC_TBL_PK=" + DETENTION_CALC_TBL_PK;
            }

            if (DEPOT_MST_FK > 0)
            {
                strCondition += " AND DCT.VENDOR_MST_FK=" + DEPOT_MST_FK;
            }

            if (CARGO_TYPE > 0)
            {
                strCondition += "AND DCT.CARGO_TYPE=" + CARGO_TYPE;
            }

            if (JOB_CARD_REF_NO.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(JCSIT.JOBCARD_REF_NO) LIKE '" + JOB_CARD_REF_NO.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        strCondition += " AND UPPER(JCSIT.JOBCARD_REF_NO) LIKE '%" + JOB_CARD_REF_NO.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
                else
                {
                    strCondition += " AND UPPER(JCSIT.JOBCARD_REF_NO) LIKE '%" + JOB_CARD_REF_NO.ToUpper().Replace("'", "''") + "%'" ;
                }
            }
            if (DETENTION_REF_NO.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(DCT.DETENTION_REF_NO) LIKE '" + DETENTION_REF_NO.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        strCondition += " AND UPPER(DCT.DETENTION_REF_NO) LIKE '%" + DETENTION_REF_NO.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
                else
                {
                    strCondition += " AND UPPER(DCT.DETENTION_REF_NO) LIKE '%" + DETENTION_REF_NO.ToUpper().Replace("'", "''") + "%'" ;
                }
            }
            strCondition += "  AND VST.VENDOR_MST_FK=DMT.VENDOR_MST_PK AND VST.VENDOR_TYPE_FK=VT.VENDOR_TYPE_PK AND DMT.VENDOR_MST_PK=VCD.VENDOR_MST_FK ";
            strCondition += "   And vt.vendor_type_id= 'WAREHOUSE' And VCD.ADM_LOCATION_MST_FK = " + lngUsrLocFk;
            // ------------------------------------------
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (DEPOT_ID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(DMT.VENDOR_ID) LIKE '" + DEPOT_ID.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        strCondition += " AND UPPER(DMT.VENDOR_ID) LIKE '%" + DEPOT_ID.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
                else
                {
                    strCondition += " AND UPPER(DMT.VENDOR_ID) LIKE '%" + DEPOT_ID.ToUpper().Replace("'", "''") + "%'" ;
                }
            }
            strCondition += " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + "";
            strCondition += " AND DCT.CREATED_BY_FK = UMT.USER_MST_PK ";
            strSQL = "SELECT Count(*) from DETENTION_CALC_TBL DCT,USER_MST_TBL UMT,VENDOR_MST_TBL DMT,JOB_CARD_TRN JCSIT,vendor_contact_dtls VCD,vendor_type_mst_tbl vt,vendor_services_trn VST where DMT.VENDOR_MST_PK=DCT.VENDOR_MST_FK AND JCSIT.JOB_CARD_TRN_PK=DCT.JOB_CARD_SEA_IMP_FK ";
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

            strSQL = " SELECT * FROM (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += " DCT.JOB_CARD_SEA_IMP_FK, ";
            strSQL += " JCSIT.JOBCARD_REF_NO,";
            strSQL += " DCT.DETENTION_CALC_TBL_PK,";
            strSQL += " DCT.DETENTION_REF_NO,";
            strSQL += " TO_CHAR(DCT.DETENTION_DATE, '" + dateFormat + "'), ";
            strSQL += " DECODE(DCT.CARGO_TYPE,1,'FCL','LCL'),";
            strSQL += " DCT.VENDOR_MST_FK,";
            strSQL += " DMT.VENDOR_ID, ";
            strSQL += " DCT.CURRENCY_MST_FK, ";
            strSQL += " CUR.CURRENCY_ID, ";
            strSQL += " NET_DETENTION_AMT";
            strSQL += " FROM JOB_CARD_TRN JCSIT,vendor_contact_dtls VCD,vendor_type_mst_tbl vt,vendor_services_trn VST , ";
            strSQL += " VENDOR_MST_TBL DMT, ";
            strSQL += " DETENTION_CALC_TBL DCT, ";
            strSQL += " CURRENCY_TYPE_MST_TBL CUR, ";
            strSQL += " USER_MST_TBL UMT ";
            strSQL += " WHERE JCSIT.JOB_CARD_TRN_PK=DCT.JOB_CARD_SEA_IMP_FK ";
            strSQL += " AND DMT.VENDOR_MST_PK=DCT.VENDOR_MST_FK ";
            strSQL += " AND DCT.CURRENCY_MST_FK=CUR.CURRENCY_MST_PK ";
            strSQL += strCondition;
            strSQL += " order by " + SortCol + SortType + ",DETENTION_REF_NO desc ) q  ) WHERE SR_NO  Between " + start + " and " + last;
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

        #endregion

        #endregion

        #region " Detention Entry Screen "

        // 
        /// <summary>
        /// This function is called while viewing records in grid for selected job card.
        /// </summary>
        /// <param name="Job_Card_Pk">Primary Key of Job card import table, sent ByValue</param>
        /// <example>Grid_Details(-1)</example>
        /// <exception cref="sqlExp">Catch SQL Exception</exception>
        /// <remarks>This function retrun datatable</remarks>
        public DataTable Grid_Details(long Job_Card_Pk, bool IsFCL)
        {
            try
            {
                objWF.MyCommand.Parameters.Clear();
                Int16 nCargoType = default(Int16);
                if (IsFCL)
                {
                    nCargoType = 1;
                }
                else
                {
                    nCargoType = 2;
                }
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("JOB_CARD_PK_IN", Job_Card_Pk).Direction = ParameterDirection.Input;
                _with4.Add("CARGO_TYPE_IN", nCargoType).Direction = ParameterDirection.Input;
                _with4.Add("DET_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataTable("DETENTION_CALCULATION_PKG", "GRID_DETAILS");
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        public string Calculate_Detention(string JobContPKList, string DepotList, string CargoList, string Container_Ty, string GLD, string Coll_DT, string Free_Days, string CurrList, bool IsFCL)
        {
            try
            {
                objWF.OpenConnection();
                objWF.MyCommand.Connection = objWF.MyConnection;
                objWF.MyCommand.CommandType = CommandType.StoredProcedure;
                if (IsFCL)
                {
                    objWF.MyCommand.CommandText = objWF.MyUserName + ".DETENTION_FCL";
                    var _with5 = objWF.MyCommand.Parameters;
                    _with5.Clear();
                    _with5.Add("JOB_CARD_SEA_IMP_PK_LIST1", JobContPKList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with5.Add("DEPOT_LIST1", DepotList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with5.Add("CARGO_LIST1", CargoList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;

                    _with5.Add("CNTR_TYPE_LIST1", Container_Ty.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;

                    _with5.Add("GL_DT_LIST1", GLD.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with5.Add("COLL_DT_LIST1", Coll_DT.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with5.Add("FREE_DAYS_LIST1", Free_Days.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with5.Add("CURRENCY_LIST1", CurrList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;

                    _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000).Direction = ParameterDirection.ReturnValue;
                }
                else
                {
                    objWF.MyCommand.CommandText = objWF.MyUserName + ".DETENTION_LCL";
                    var _with6 = objWF.MyCommand.Parameters;
                    _with6.Clear();
                    _with6.Add("JOB_CARD_SEA_IMP_PK_LIST1", JobContPKList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with6.Add("DEPOT_PK_LIST1", DepotList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with6.Add("CARGO_TYPE_LIST1", CargoList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with6.Add("VOL_WT_LIST1", Container_Ty.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with6.Add("GL_DT_LIST1", GLD.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with6.Add("COLL_DT_LIST1", Coll_DT.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with6.Add("FREE_DAYS_LIST1", Free_Days.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with6.Add("CURRENCY_LIST1", CurrList.TrimEnd(Convert.ToChar(","))).Direction = ParameterDirection.Input;
                    _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000).Direction = ParameterDirection.ReturnValue;
                }
                objWF.MyCommand.ExecuteScalar();
                return objWF.MyCommand.Parameters["RETURN_VALUE"].Value.ToString();
            }
            catch (OracleException sqlEx)
            {
                return sqlEx.Message;
            }
            catch (Exception Ex)
            {
                return Ex.Message;
            }
            finally
            {
                objWF.MyCommand.Connection.Close();
            }
        }

        public DataSet FetchHDR(long nDetPK)
        {
            string strSQL = null;
            DataSet DS = null;
            try
            {
                strSQL = "SELECT ";
                strSQL += "DET.JOB_CARD_SEA_IMP_FK,";
                strSQL += "JOB.JOBCARD_REF_NO,";
                strSQL += "DET.DETENTION_REF_NO,";
                strSQL += "DET.DETENTION_DATE,";
                strSQL += "DET.CARGO_TYPE,";
                strSQL += "DET.GROSS_DETENTION_AMT,";
                strSQL += "DET.WAIVER_TYPE,";
                strSQL += "DET.WAIVER_PERCENTAGE,";
                strSQL += "DET.WAIVER_AMT,";
                strSQL += "DET.NET_DETENTION_AMT,";
                strSQL += "DET.APPROVED_BY,";
                strSQL += "EMP.EMPLOYEE_NAME,";
                strSQL += "DET.REFERENCE_NO,";
                strSQL += "DET.CURRENCY_MST_FK,";
                strSQL += "CMT.CURRENCY_ID,";
                strSQL += "DET.VENDOR_MST_FK,";
                strSQL += "DET.HANDLING_CHARGES,";
                strSQL += "VMT.VENDOR_ID";
                strSQL += "FROM";
                strSQL += "DETENTION_CALC_TBL DET,";
                strSQL += "JOB_CARD_TRN JOB,";
                strSQL += "CURRENCY_TYPE_MST_TBL CMT,";
                strSQL += "VENDOR_MST_TBL VMT,";
                strSQL += "EMPLOYEE_MST_TBL EMP";
                strSQL += "WHERE";
                strSQL += "DET.CURRENCY_MST_FK=CMT.CURRENCY_MST_PK (+)";
                strSQL += "AND DET.APPROVED_BY=EMP.EMPLOYEE_MST_PK (+)";
                strSQL += "AND DET.VENDOR_MST_FK=VMT.VENDOR_MST_PK (+)";
                strSQL += "AND DET.JOB_CARD_SEA_IMP_FK=JOB.JOB_CARD_TRN_PK";
                strSQL += "AND DET.DETENTION_CALC_TBL_PK=" + Convert.ToString(nDetPK);
                objWF.OpenConnection();
                DS = objWF.GetDataSet(strSQL);
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        public DataSet FetchTRN(long nDetPK, short nCargoType)
        {
            string strSQL = null;
            DataSet DS = null;
            try
            {
                if (nCargoType == 1)
                {
                    strSQL = "SELECT '' NO_OF_PALETTES, ";
                    //adding by thiyagarajan on 3/1/09 :VEK Gap analysis
                    strSQL += "TRN.CONTAINER_NUMBER AS \"CONTAINER_NUMBER\",";
                    strSQL += "CONT.CONTAINER_TYPE_MST_ID AS \"CONTAINER_TYPE\",";
                    strSQL += "TRN.CONTAINER_TYPE_MST_FK AS \"CONTAINER_PK\",";
                    strSQL += "TO_CHAR(TRN.GEN_LAND_DATE, DATEFORMAT) AS \"GLD\",";
                    strSQL += "TO_CHAR(TRN.DETENTION_CALC_DATE, DATEFORMAT) AS \"DETEN_CAL_DT\",";
                    strSQL += "TO_CHAR(TRN.COLLECTING_DATE_TIME, DATEFORMAT) AS \"COLLECTING_DT\",";
                    strSQL += "TRN.FREE_DAYS  AS \"FREE DAYS\",";
                    strSQL += "TRN.DETENTION_AMT  AS \"DETENTION_AMT\",";
                    strSQL += "'1' AS \"SEL\", ";
                    strSQL += "0 JOB_CARD_SEA_IMP_CONT_PK ";
                    strSQL += "FROM";
                    strSQL += "DETENTION_CALC_TRN TRN,";
                    strSQL += "CONTAINER_TYPE_MST_TBL CONT";
                    strSQL += "WHERE";
                    strSQL += "TRN.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK";
                    strSQL += "AND TRN.DETENTION_CALC_TBL_FK=" + Convert.ToString(nDetPK);
                    strSQL += "ORDER BY TRN.CONTAINER_NUMBER, CONT.CONTAINER_TYPE_MST_ID,TRN.GEN_LAND_DATE";
                }
                else
                {
                    strSQL = "SELECT TRN.NO_OF_PALETTES,";
                    //adding by thiyagarajan on 3/1/09 :VEK Gap analysis
                    strSQL += "TRN.PACK_TYPE_FK,";
                    strSQL += "PMT.PACK_TYPE_ID,";
                    strSQL += "TRN.NO_OF_PACKAGES AS \"NO_OF_PACKAGES\",";
                    strSQL += "TRN.GROSS_WEIGHT AS \"WEIGHT\",";
                    strSQL += "TRN.VOLUME_IN_CBM AS \"VOLUME\",";
                    strSQL += "TO_CHAR(TRN.GEN_LAND_DATE, DATEFORMAT) AS \"GLD\",";
                    strSQL += "TO_CHAR(TRN.DETENTION_CALC_DATE, DATEFORMAT) AS \"DETEN_CAL_DT\",";
                    strSQL += "TO_CHAR(TRN.COLLECTING_DATE_TIME, DATEFORMAT) AS \"COLLECTING_DT\",";
                    strSQL += "TRN.FREE_DAYS AS \"FREE DAYS\",";
                    strSQL += "TRN.DETENTION_AMT AS \"DETENTION_AMT\",";
                    strSQL += "'1' AS \"SEL\",";
                    strSQL += "0 JOB_CARD_SEA_IMP_CONT_PK ";
                    strSQL += "FROM";
                    strSQL += "DETENTION_CALC_TRN TRN,";
                    strSQL += "PACK_TYPE_MST_TBL PMT";
                    strSQL += "WHERE TRN.PACK_TYPE_FK = PMT.PACK_TYPE_MST_PK";
                    strSQL += "AND TRN.DETENTION_CALC_TBL_FK=" + Convert.ToString(nDetPK);
                    strSQL += "ORDER BY TRN.PACK_TYPE_FK,PMT.PACK_TYPE_ID,TRN.GEN_LAND_DATE";
                }
                objWF.OpenConnection();
                DS = objWF.GetDataSet(strSQL);
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        public DataSet FetchDetentionDetails(long nDetPK, short nCargoType, long nJobCardPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;
            try
            {
                if (nCargoType == 1)
                {
                    strSQL = "SELECT '' Palette, ";
                    //adding by thiyagarajan on 3/1/09 :VEK Gap analysis
                    strSQL += "TRN.CONTAINER_NUMBER,";
                    strSQL += "CONT.CONTAINER_TYPE_MST_ID CONTAINER_TYPE,";
                    strSQL += "TRN.CONTAINER_TYPE_MST_FK CONTAINER_PK,";
                    strSQL += "TO_CHAR(TRN.GEN_LAND_DATE, DATEFORMAT) GLD,";
                    strSQL += "TO_CHAR(TRN.DETENTION_CALC_DATE, DATEFORMAT) DETEN_CAL_DT,";
                    strSQL += "TO_CHAR(TRN.COLLECTING_DATE_TIME, DATEFORMAT) COLLECTING_DT,";
                    strSQL += "TRN.FREE_DAYS,";
                    strSQL += "TRN.DETENTION_AMT,";
                    strSQL += "'1' SEL, ";
                    strSQL += "0 JOB_CARD_SEA_IMP_CONT_PK ";
                    strSQL += "FROM";
                    strSQL += "DETENTION_CALC_TRN TRN,";
                    strSQL += "DETENTION_CALC_TBL HDR,";
                    strSQL += "CONTAINER_TYPE_MST_TBL CONT";
                    strSQL += "WHERE";
                    strSQL += "TRN.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK";
                    strSQL += "AND TRN.DETENTION_CALC_TBL_FK=HDR.DETENTION_CALC_TBL_PK";
                    //strSQL &= vbCrLf & "AND TRN.DETENTION_CALC_TBL_FK<>" & CStr(nDetPK) 'modifying by thiyagarajan on 5/1/09
                    strSQL += "AND TRN.DETENTION_CALC_TBL_FK=" + Convert.ToString(nDetPK);
                    strSQL += "AND HDR.JOB_CARD_SEA_IMP_FK=" + Convert.ToString(nJobCardPK);
                    strSQL += "ORDER BY TRN.CONTAINER_NUMBER, CONT.CONTAINER_TYPE_MST_ID,TRN.GEN_LAND_DATE";
                }
                else
                {
                    strSQL = "SELECT TRN.NO_OF_PALETTES, ";
                    //adding by thiyagarajan on 3/1/09 :VEK Gap analysis
                    strSQL += "TRN.PACK_TYPE_FK,";
                    strSQL += "PMT.PACK_TYPE_ID,";
                    strSQL += "TRN.NO_OF_PACKAGES,";
                    strSQL += "TRN.GROSS_WEIGHT WEIGHT,";
                    strSQL += "TRN.VOLUME_IN_CBM VOLUME,";
                    strSQL += "TO_CHAR(TRN.GEN_LAND_DATE, DATEFORMAT) GLD,";
                    strSQL += "TO_CHAR(TRN.DETENTION_CALC_DATE, DATEFORMAT) DETEN_CAL_DT,";
                    strSQL += "TO_CHAR(TRN.COLLECTING_DATE_TIME, DATEFORMAT) COLLECTING_DT,";
                    strSQL += "TRN.FREE_DAYS,";
                    strSQL += "TRN.DETENTION_AMT,";
                    strSQL += "'1' SEL,";
                    strSQL += "0 JOB_CARD_SEA_IMP_CONT_PK ";
                    strSQL += "FROM";
                    strSQL += "DETENTION_CALC_TRN TRN,";
                    strSQL += "DETENTION_CALC_TBL HDR,";
                    strSQL += "PACK_TYPE_MST_TBL PMT";
                    strSQL += "WHERE TRN.PACK_TYPE_FK = PMT.PACK_TYPE_MST_PK";
                    strSQL += "AND TRN.DETENTION_CALC_TBL_FK=HDR.DETENTION_CALC_TBL_PK";
                    //strSQL &= vbCrLf & "AND TRN.DETENTION_CALC_TBL_FK<>" & CStr(nDetPK)'modifying by thiyagarajan on 5/1/09
                    strSQL += "AND TRN.DETENTION_CALC_TBL_FK=" + Convert.ToString(nDetPK);
                    strSQL += "AND HDR.JOB_CARD_SEA_IMP_FK=" + Convert.ToString(nJobCardPK);
                    strSQL += "ORDER BY TRN.PACK_TYPE_FK,PMT.PACK_TYPE_ID,TRN.GEN_LAND_DATE";
                }

                DS = objWK.GetDataSet(strSQL);
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                //objWF.MyConnection.Close()
            }
        }

        public DataSet FetchSlabDetails(long nJobContPK)
        {
            StringBuilder strBuilder = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            try
            {
                strBuilder.Append("SELECT * FROM detention_sea_calc_report WHERE job_card_sea_imp_count_pk=" + Convert.ToString(nJobContPK) + " ");
                strBuilder.Append("ORDER BY FROM_DAY");
                DS = objWK.GetDataSet(strBuilder.ToString());

                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //adding by thiyagarajan on 5/1/09
        public Int32 FetchCalcuBasis(long Depotpk)
        {
            StringBuilder strBuilder = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            try
            {
                strBuilder.Append(" select  CALBASIS.CALCULATION_BASIS from DETENTION_CALC_TRN CALBASIS WHERE CALBASIS.DETENTION_CALC_TBL_FK=" + Depotpk);
                return Convert.ToInt32(objWK.ExecuteScaler(strBuilder.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "Save"
        //This function is called to save the Detention Calculation details
        public ArrayList Save(ref DataSet hdrDS, ref DataSet trnDS, long nConfigPK, ref long nDetPK, ref object DetRefNo, long nLocationId, long nEmpId, long nUserId, long HandlingCharges, DateTime arrivaldate)
        {

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            long nJobCardPK = 0;
            long nDetCalcPK = 0;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            string DetRef = null;
            string InvRefNo = null;
            string strReturn = null;
            bool chkflag = false;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Connection = objWK.MyConnection;
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                if (string.IsNullOrEmpty(DetRefNo.ToString()))
                {
                    DetRef = GenerateProtocolKey("DETENTION CALCULATION", nLocationId, nEmpId, DateTime.Today, "", "", "", nUserId, objWK);
                    if (DetRef == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                    }
                }
                else
                {
                    DetRef = DetRefNo.ToString();
                }
                objWK.MyCommand.Parameters.Clear();
                InvRefNo = GenerateProtocolKey("CONSOLIDATED INVOICE", nLocationId, nEmpId, DateTime.Now, "", "", "", Convert.ToInt64(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]), objWK);
                //adding by thiyagarajan on 5/1/09 :generaing unique ref.nr.
                int UNIQUE = 0;
                string uniqueRefNr = null;
                string uniqueReferenceNr = null;

                if (string.IsNullOrEmpty(uniqueRefNr))
                {
                    DateTime dt = default(DateTime);
                    dt = DateTime.Now;
                    string st = null;
                    st = Convert.ToString(dt.Day + dt.Month + dt.Year + dt.Hour + dt.Minute + dt.Second + dt.Millisecond);
                    uniqueRefNr = GetVEKInvoiceRef(0, 0, st);
                    //While IsUniqueRefNr(uniqueRefNr, objWK.MyCommand) > 0
                    //    uniqueRefNr = GetVEKInvoiceRef(10000000, 99999999)
                    //End While
                }
                uniqueReferenceNr = uniqueRefNr;
                //end

                if (Convert.ToString(InvRefNo) == "Protocol Not Defined.")
                {
                    InvRefNo = "";
                }

                objWK.MyCommand.Parameters.Clear();
                nJobCardPK = Convert.ToInt32(hdrDS.Tables[0].Rows[0]["JOB_CARD_SEA_IMP_FK"]);
                hdrDS.Tables[0].Rows[0]["DETENTION_REF_NO"] = DetRef;
                var _with7 = insCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".DETENTION_CALC_TBL_PKG.DETENTION_CALC_TBL_INS";

                //JOB_CARD_SEA_IMP_FK_IN,
                _with7.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_FK").Direction = ParameterDirection.Input;
                _with7.Parameters["JOB_CARD_SEA_IMP_FK_IN"].SourceVersion = DataRowVersion.Current;
                //DETENTION_REF_NO_IN                   
                _with7.Parameters.Add("DETENTION_REF_NO_IN", OracleDbType.Varchar2, 50, "DETENTION_REF_NO").Direction = ParameterDirection.Input;
                _with7.Parameters["DETENTION_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                //INVOICE_REF_NO_IN 
                _with7.Parameters.Add("INVOICE_REF_NO_IN", InvRefNo).Direction = ParameterDirection.Input;
                //.Parameters.Add("INVOICE_REF_NO_IN", OracleDbType.Varchar2, 50, InvRefNo).Direction = ParameterDirection.Input
                //.Parameters["INVOICE_REF_NO_IN"].SourceVersion = DataRowVersion.Current
                //DETENTION_DATE_IN,
                _with7.Parameters.Add("DETENTION_DATE_IN", OracleDbType.Date, 10, "DETENTION_DATE").Direction = ParameterDirection.Input;
                _with7.Parameters["DETENTION_DATE_IN"].SourceVersion = DataRowVersion.Current;
                //CARGO_TYPE_IN,
                _with7.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                _with7.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                //VENDOR_MST_FK_IN,
                _with7.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_FK").Direction = ParameterDirection.Input;
                _with7.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //GROSS_DETENTION_AMT_IN,
                _with7.Parameters.Add("GROSS_DETENTION_AMT_IN", OracleDbType.Int32, 10, "GROSS_DETENTION_AMT").Direction = ParameterDirection.Input;
                _with7.Parameters["GROSS_DETENTION_AMT_IN"].SourceVersion = DataRowVersion.Current;
                //WAIVER_TYPE_IN,
                _with7.Parameters.Add("WAIVER_TYPE_IN", OracleDbType.Int32, 1, "WAIVER_TYPE").Direction = ParameterDirection.Input;
                _with7.Parameters["WAIVER_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                //WAIVER_PERCENTAGE_IN,
                _with7.Parameters.Add("WAIVER_PERCENTAGE_IN", OracleDbType.Int32, 5, "WAIVER_PERCENTAGE").Direction = ParameterDirection.Input;
                _with7.Parameters["WAIVER_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;
                //WAIVER_AMT_IN,
                _with7.Parameters.Add("WAIVER_AMT_IN", OracleDbType.Int32, 10, "WAIVER_AMT").Direction = ParameterDirection.Input;
                _with7.Parameters["WAIVER_AMT_IN"].SourceVersion = DataRowVersion.Current;
                //NET_DETENTION_AMT_IN,
                _with7.Parameters.Add("NET_DETENTION_AMT_IN", OracleDbType.Int32, 10, "NET_DETENTION_AMT").Direction = ParameterDirection.Input;
                _with7.Parameters["NET_DETENTION_AMT_IN"].SourceVersion = DataRowVersion.Current;
                //APPROVED_BY_IN,
                _with7.Parameters.Add("APPROVED_BY_IN", OracleDbType.Int32, 10, "APPROVED_BY").Direction = ParameterDirection.Input;
                _with7.Parameters["APPROVED_BY_IN"].SourceVersion = DataRowVersion.Current;
                //REFERENCE_NO_IN,
                _with7.Parameters.Add("REFERENCE_NO_IN", OracleDbType.Varchar2, 30, "REFERENCE_NO").Direction = ParameterDirection.Input;
                _with7.Parameters["REFERENCE_NO_IN"].SourceVersion = DataRowVersion.Current;
                //CURRENCY_MST_FK_IN,
                _with7.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with7.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //CREATED_BY_FK_IN,
                _with7.Parameters.Add("CREATED_BY_FK_IN", OracleDbType.Int32, 10, "CREATED_BY_FK").Direction = ParameterDirection.Input;
                _with7.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
                //CONFIG_PK_IN
                _with7.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
                //Handling Charges ' Added BY Jitendra 30/06/07
                _with7.Parameters.Add("HANDLING_CHARGES_IN", HandlingCharges).Direction = ParameterDirection.Input;
                //RETURN_VALUE
                //Invocie Nr. 'by thiyagarajan on 5/1/09
                _with7.Parameters.Add("INVREFNO_IN", uniqueReferenceNr).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 50).Direction = ParameterDirection.Output
                //.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DETENTION_CALC_TBL_PK").Direction = ParameterDirection.Output


                _with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with8 = updCommand;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".DETENTION_CALC_TBL_PKG.DETENTION_CALC_TBL_UPD";

                //DETENTION_CALC_TBL_PK_IN
                _with8.Parameters.Add("DETENTION_CALC_TBL_PK_IN", OracleDbType.Int32, 10, "DETENTION_CALC_TBL_PK").Direction = ParameterDirection.Input;
                _with8.Parameters["DETENTION_CALC_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;
                //JOB_CARD_SEA_IMP_FK_IN,
                _with8.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_FK").Direction = ParameterDirection.Input;
                _with8.Parameters["JOB_CARD_SEA_IMP_FK_IN"].SourceVersion = DataRowVersion.Current;
                //DETENTION_REF_NO_IN                   
                _with8.Parameters.Add("DETENTION_REF_NO_IN", OracleDbType.Varchar2, 50, "DETENTION_REF_NO").Direction = ParameterDirection.Input;
                _with8.Parameters["DETENTION_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                //DETENTION_DATE_IN,
                _with8.Parameters.Add("DETENTION_DATE_IN", OracleDbType.Date, 10, "DETENTION_DATE").Direction = ParameterDirection.Input;
                _with8.Parameters["DETENTION_DATE_IN"].SourceVersion = DataRowVersion.Current;
                //CARGO_TYPE_IN,
                _with8.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                _with8.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                //VENDOR_MST_FK_IN,
                _with8.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_FK").Direction = ParameterDirection.Input;
                _with8.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //GROSS_DETENTION_AMT_IN,
                _with8.Parameters.Add("GROSS_DETENTION_AMT_IN", OracleDbType.Int32, 10, "GROSS_DETENTION_AMT").Direction = ParameterDirection.Input;
                _with8.Parameters["GROSS_DETENTION_AMT_IN"].SourceVersion = DataRowVersion.Current;
                //WAIVER_TYPE_IN,
                _with8.Parameters.Add("WAIVER_TYPE_IN", OracleDbType.Int32, 1, "WAIVER_TYPE").Direction = ParameterDirection.Input;
                _with8.Parameters["WAIVER_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                //WAIVER_PERCENTAGE_IN,
                _with8.Parameters.Add("WAIVER_PERCENTAGE_IN", OracleDbType.Int32, 5, "WAIVER_PERCENTAGE").Direction = ParameterDirection.Input;
                _with8.Parameters["WAIVER_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;
                //WAIVER_AMT_IN,
                _with8.Parameters.Add("WAIVER_AMT_IN", OracleDbType.Int32, 10, "WAIVER_AMT").Direction = ParameterDirection.Input;
                _with8.Parameters["WAIVER_AMT_IN"].SourceVersion = DataRowVersion.Current;
                //NET_DETENTION_AMT_IN,
                _with8.Parameters.Add("NET_DETENTION_AMT_IN", OracleDbType.Int32, 10, "NET_DETENTION_AMT").Direction = ParameterDirection.Input;
                _with8.Parameters["NET_DETENTION_AMT_IN"].SourceVersion = DataRowVersion.Current;
                //APPROVED_BY_IN,
                _with8.Parameters.Add("APPROVED_BY_IN", OracleDbType.Int32, 10, "APPROVED_BY").Direction = ParameterDirection.Input;
                _with8.Parameters["APPROVED_BY_IN"].SourceVersion = DataRowVersion.Current;
                //REFERENCE_NO_IN,
                _with8.Parameters.Add("REFERENCE_NO_IN", OracleDbType.Varchar2, 30, "REFERENCE_NO").Direction = ParameterDirection.Input;
                _with8.Parameters["REFERENCE_NO_IN"].SourceVersion = DataRowVersion.Current;
                //CURRENCY_MST_FK_IN,
                _with8.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with8.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //LAST_MODIFIED_BY_FK_IN,
                _with8.Parameters.Add("LAST_MODIFIED_BY_FK_IN", OracleDbType.Int32, 10, "LAST_MODIFIED_BY_FK").Direction = ParameterDirection.Input;
                _with8.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
                //VERSION_NO_IN
                _with8.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
                _with8.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                //Handling Charges ' Added BY Jitendra 30/06/07
                _with8.Parameters.Add("HANDLING_CHARGES_IN", HandlingCharges).Direction = ParameterDirection.Input;

                //CONFIG_PK_IN
                _with8.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
                _with8.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                _with8.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                
                arrMessage.Clear();

                var _with9 = objWK.MyDataAdapter;
                _with9.InsertCommand = insCommand;
                _with9.InsertCommand.Transaction = TRAN;
                _with9.UpdateCommand = updCommand;
                _with9.UpdateCommand.Transaction = TRAN;
                if ((hdrDS.GetChanges(DataRowState.Added) != null))
                {
                    chkflag = true;
                }
                else
                {
                    chkflag = false;
                }
                RecAfct = _with9.Update(hdrDS.Tables[0]);

                if (RecAfct > 0)
                {
                    //getting detentioncalcpk,consolinvoicefk,custpk from detentioncalcins.
                    strReturnPK = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value).Trim();
                    string[] arr = null;
                    arr = strReturnPK.Split(Convert.ToChar("~"));
                    nDetCalcPK = Convert.ToInt64(arr.GetValue(0));
                    updatearrivaldate(arrivaldate, nDetCalcPK, nJobCardPK);
                    //Snigdharani - 22/04/2009
                    SaveInvoiceHeader(hdrDS, TRAN, nDetCalcPK, InvRefNo, uniqueReferenceNr, nJobCardPK, ref trnDS);
                    //SaveTrn(trnDS, TRAN, nDetCalcPK, nJobCardPK)
                }
                if (arrMessage.Count > 0)
                {
                    if (chkflag)
                    {
                        RollbackProtocolKey("DETENTION CALCULATION", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DetRef, DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), InvRefNo, DateTime.Now);
                    }
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    DetRefNo = DetRef;
                    return arrMessage;
                }

            }
            catch (OracleException oraexp)
            {
                if ((TRAN != null))
                {
                    if (TRAN.Connection.State == ConnectionState.Open)
                    {
                        TRAN.Rollback();
                        TRAN = null;
                    }
                }
                if (chkflag)
                {
                    RollbackProtocolKey("DETENTION CALCULATION", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DetRef, DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), InvRefNo, DateTime.Now);
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                if ((TRAN != null))
                {
                    if (TRAN.Connection.State == ConnectionState.Open)
                    {
                        TRAN.Rollback();
                        TRAN = null;
                    }
                }
                if (chkflag)
                {
                    RollbackProtocolKey("DETENTION CALCULATION", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DetRef, DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), InvRefNo, DateTime.Now);
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
                //Added by sivachandran - To close the connection after Transaction
            }
        }
        public void updatearrivaldate(DateTime arrdt, long nDetCalcPK, long nJobCardPK)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            strSQL = "UPDATE JOB_CARD_TRN JSI set ";
            strSQL = strSQL + "jsi.arrival_date = '" + arrdt.ToShortDateString() + "'";
            strSQL = strSQL + " WHERE ";
            strSQL = strSQL + " jsi.JOB_CARD_TRN_PK = " + nJobCardPK;
            //strSQL = strSQL & vbCrLf & " det.detention_calc_tbl_pk = " & nDetCalcPK
            try
            {
                objWK.ExecuteCommands(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //This function is called to save the Detention Calculation TRN details
        private void SaveTrn(DataSet trnDS, OracleTransaction TRAN, long nDetCalcPK, long nJobCardPK, int OthChgPk = 0)
        {
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            try
            {
                objWK.MyConnection = TRAN.Connection;
                var _with10 = insCommand;
                //.Transaction = TRAN
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".DETENTION_CALC_TBL_PKG.DETENTION_CALC_TRN_INS";
                // DETENTION_CALC_TBL_FK_IN          
                _with10.Parameters.Add("DETENTION_CALC_TBL_FK_IN", nDetCalcPK).Direction = ParameterDirection.Input;
                _with10.Parameters["DETENTION_CALC_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;
                // CONTAINER_NUMBER_IN,
                _with10.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 20, "CONTAINER_NUMBER").Direction = ParameterDirection.Input;
                _with10.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;
                // CONTAINER_TYPE_MST_FK_IN,
                _with10.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CONTAINER_TYPE_MST_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // CALCULATION_BASIS_IN,
                _with10.Parameters.Add("CALCULATION_BASIS_IN", OracleDbType.Int32, 1, "CALCULATION_BASIS").Direction = ParameterDirection.Input;
                _with10.Parameters["CALCULATION_BASIS_IN"].SourceVersion = DataRowVersion.Current;
                // NO_OF_PACKAGES_IN,
                _with10.Parameters.Add("NO_OF_PACKAGES_IN", OracleDbType.Int32, 6, "NO_OF_PACKAGES").Direction = ParameterDirection.Input;
                _with10.Parameters["NO_OF_PACKAGES_IN"].SourceVersion = DataRowVersion.Current;
                // GROSS_WEIGHT_IN,
                _with10.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "GROSS_WEIGHT").Direction = ParameterDirection.Input;
                _with10.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                // VOLUME_IN_CBM_IN,
                _with10.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "VOLUME_IN_CBM").Direction = ParameterDirection.Input;
                _with10.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;
                //GEN_LAND_DATE_IN,
                _with10.Parameters.Add("GEN_LAND_DATE_IN", OracleDbType.Date, 10, "GEN_LAND_DATE").Direction = ParameterDirection.Input;
                _with10.Parameters["GEN_LAND_DATE_IN"].SourceVersion = DataRowVersion.Current;
                //COLLECTING_DATE_TIME_IN,
                _with10.Parameters.Add("COLLECTING_DATE_TIME_IN", OracleDbType.Date, 10, "COLLECTING_DATE_TIME").Direction = ParameterDirection.Input;
                _with10.Parameters["COLLECTING_DATE_TIME_IN"].SourceVersion = DataRowVersion.Current;
                //FREE_DAYS_IN,
                _with10.Parameters.Add("FREE_DAYS_IN", OracleDbType.Int32, 2, "FREE_DAYS").Direction = ParameterDirection.Input;
                _with10.Parameters["FREE_DAYS_IN"].SourceVersion = DataRowVersion.Current;
                //DETENTION_AMT_IN,
                _with10.Parameters.Add("DETENTION_AMT_IN", OracleDbType.Int32, 10, "DETENTION_AMT").Direction = ParameterDirection.Input;
                _with10.Parameters["DETENTION_AMT_IN"].SourceVersion = DataRowVersion.Current;
                //PACK_TYPE_FK_IN,
                _with10.Parameters.Add("PACK_TYPE_FK_IN", OracleDbType.Int32, 10, "PACK_TYPE_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;
                //DETENTION_CALC_DATE_IN,
                _with10.Parameters.Add("DETENTION_CALC_DATE_IN", OracleDbType.Date, 10, "DETENTION_CALC_DATE").Direction = ParameterDirection.Input;
                _with10.Parameters["DETENTION_CALC_DATE_IN"].SourceVersion = DataRowVersion.Current;
                //DETENTION_SLAB_MAIN_FK_IN
                _with10.Parameters.Add("DETENTION_SLAB_MAIN_FK_IN", OracleDbType.Int32, 10, "DETENTION_SLAB_MAIN_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["DETENTION_SLAB_MAIN_FK_IN"].SourceVersion = DataRowVersion.Current;

                //NO_OF_PALETTES 'adding by thiyagarajan on 5/1/09:VEK Gap Analysis:introducing a col. into grid
                _with10.Parameters.Add("NO_OF_PALETTES_IN", OracleDbType.Int32, 4, "NO_OF_PALETTES").Direction = ParameterDirection.Input;
                _with10.Parameters["NO_OF_PALETTES_IN"].SourceVersion = DataRowVersion.Current;

                _with10.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DETENTION_CALC_TRN_PK").Direction = ParameterDirection.Output;
                _with10.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                
                var _with11 = objWK.MyDataAdapter;
                _with11.InsertCommand = insCommand;
                _with11.InsertCommand.Transaction = TRAN;
                RecAfct = _with11.Update(trnDS.Tables[0]);
                if (RecAfct == 0)
                {
                    arrMessage.Add("Save not successful");
                }
                var _with12 = insCommand;

                _with12.Parameters.Clear();
                _with12.CommandText = objWK.MyUserName + ".DETENTION_CALC_TBL_PKG.DETENTION_CALC_SLAB_DTLS_INS";
                // DETENTION_CALC_TRN_FK_IN          
                _with12.Parameters.Add("DETENTION_CALC_TRN_FK_IN", OracleDbType.Int32, 10, "DETENTION_CALC_TRN_FK").Direction = ParameterDirection.Input;
                _with12.Parameters["DETENTION_CALC_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;
                // FROM_DAY_IN,
                _with12.Parameters.Add("FROM_DAY_IN", OracleDbType.Int32, 3, "FROM_DAY").Direction = ParameterDirection.Input;
                _with12.Parameters["FROM_DAY_IN"].SourceVersion = DataRowVersion.Current;
                // TO_DAY_IN,
                _with12.Parameters.Add("TO_DAY_IN", OracleDbType.Int32, 3, "TO_DAY").Direction = ParameterDirection.Input;
                _with12.Parameters["TO_DAY_IN"].SourceVersion = DataRowVersion.Current;
                // NO_OF_DAYS_IN,
                _with12.Parameters.Add("NO_OF_DAYS_IN", OracleDbType.Int32, 3, "NO_OF_DAYS").Direction = ParameterDirection.Input;
                _with12.Parameters["NO_OF_DAYS_IN"].SourceVersion = DataRowVersion.Current;
                // TARIFF_RATE_IN,
                _with12.Parameters.Add("TARIFF_RATE_IN", OracleDbType.Int32, 10, "TARIFF_RATE").Direction = ParameterDirection.Input;
                _with12.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;
                // DETENTION_AMT_IN,
                _with12.Parameters.Add("DETENTION_AMOUNT_IN", OracleDbType.Int32, 10, "DETENTION_AMT").Direction = ParameterDirection.Input;
                _with12.Parameters["DETENTION_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DETENTION_CALC_SLAB_DTLS_PK").Direction = ParameterDirection.Output;
                _with12.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                
                var _with13 = objWK.MyDataAdapter;
                _with13.InsertCommand = insCommand;
                _with13.InsertCommand.Transaction = TRAN;
                RecAfct = _with13.Update(trnDS.Tables[1]);
                JcOthTrnPk = OthChgPk;
                if (RecAfct == 0)
                {
                    arrMessage.Add("Save not successful");
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
            }
        }
        #endregion

        #region "Fetch for Print"

        public DataSet funFetchHdrPrint(string strJobPk, long lngUserPk)
        {
            try
            {
                StringBuilder strBuilder = new StringBuilder();
                WorkFlow objWF = new WorkFlow();

                strBuilder.Append("SELECT ");
                strBuilder.Append("CMT.CUSTOMER_NAME AS CONSIGNEE_NAME, ");
                strBuilder.Append("DECODE(CCD.ADM_SALUTATION,'0',' ','1','Mr ','2','Mrs ') || CCD.ADM_CONTACT_PERSON AS CONTACT_PERSON, ");
                strBuilder.Append("CCD.ADM_FAX_NO AS FAX_NO, ");
                strBuilder.Append("UMT.USER_NAME AS CUSTOMER_NAME, ");
                strBuilder.Append("JCHDR.JOB_CARD_TRN_PK AS JOB_CARD_SEA_IMP_PK ");
                strBuilder.Append("FROM ");
                strBuilder.Append("JOB_CARD_TRN JCHDR, ");
                strBuilder.Append("CUSTOMER_MST_TBL CMT, ");
                strBuilder.Append("CUSTOMER_CONTACT_DTLS CCD, ");
                strBuilder.Append("USER_MST_TBL UMT ");
                strBuilder.Append("WHERE ");
                strBuilder.Append("JCHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK ");
                strBuilder.Append("AND CCD.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+) ");
                strBuilder.Append("AND JCHDR.JOB_CARD_TRN_PK= " + strJobPk + " ");
                strBuilder.Append("AND UMT.USER_MST_PK=" + lngUserPk);

                return objWF.GetDataSet(strBuilder.ToString());

            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public object funFetchTrnPrint(string strJobPk, Int16 intIsFcl, string strDetRefNo = "")
        {
            StringBuilder strBuilder = new StringBuilder();
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();

            if (!string.IsNullOrEmpty(strDetRefNo))
            {
                strCondition = " AND DCHDR.DETENTION_REF_NO='" + strDetRefNo + "' ";
            }
            else
            {
                strCondition = " ";
            }
            if (intIsFcl == 1)
            {
                strBuilder.Append("SELECT distinct ");
                strBuilder.Append("DCHDR.DETENTION_CALC_TBL_PK AS DETENTION_CALC_TBL_PK, ");
                strBuilder.Append("DCHDR.JOB_CARD_SEA_IMP_FK AS JOB_CARD_SEA_IMP_PK, ");
                strBuilder.Append("DCTRN.DETENTION_CALC_TRN_PK AS DETENTION_CALC_TRN_PK, ");
                strBuilder.Append("VMT.VENDOR_ID VENDOR_ID, ");
                strBuilder.Append("TO_CHAR(DCTRN.DETENTION_CALC_DATE,dateformat) AS DETENTION_CALC_DATE, ");
                strBuilder.Append("DCTRN.CONTAINER_NUMBER AS CONTPAL_NUMBER, ");
                strBuilder.Append("CONT.CONTAINER_TYPE_MST_ID AS CONTPACK_TYPE, ");
                strBuilder.Append("TO_CHAR(DCTRN.GEN_LAND_DATE,DATEFORMAT) AS GEN_LAND_DATE, ");
                strBuilder.Append("NULL AS NO_OF_PACKAGES, ");
                strBuilder.Append("NULL AS GROSS_WEIGHT, ");
                strBuilder.Append("NULL AS VOLUME_IN_CBM, ");
                strBuilder.Append("((DCTRN.DETENTION_CALC_DATE - DCTRN.COLLECTING_DATE_TIME) - DCTRN.FREE_DAYS ) AS DETENTION_DAYS, ");
                strBuilder.Append("TO_CHAR(DCTRN.COLLECTING_DATE_TIME,DATEFORMAT) AS COLLECTING_DATE_TIME, ");
                strBuilder.Append("NULL AS CALCULATION_BASIS, ");
                strBuilder.Append("DCTRN.FREE_DAYS AS FREE_DAYS, ");
                strBuilder.Append("DCSD.FROM_DAY AS FROM_DAY, ");
                strBuilder.Append("DCSD.TO_DAY AS TO_DAY, ");
                strBuilder.Append("DCSD.TARIFF_RATE AS TARIFF_RATE, ");
                strBuilder.Append("DCSD.NO_OF_DAYS AS NO_OF_DAYS, ");
                //'Goutam : DTS 9347
                //strBuilder.Append("DCSD.DETENTION_AMOUNT AS DETENTION_AMOUNT, ")
                strBuilder.Append(" DCTRN.DETENTION_AMT AS DETENTION_AMOUNT, ");
                //'
                strBuilder.Append("CTMT.CURRENCY_ID AS CURRENCY_ID ");
                strBuilder.Append("FROM ");
                strBuilder.Append("DETENTION_CALC_TBL DCHDR, ");
                strBuilder.Append("DETENTION_CALC_TRN DCTRN, ");
                strBuilder.Append("DETENTION_CALC_SLAB_DTLS DCSD, ");
                strBuilder.Append("VENDOR_MST_TBL VMT, ");
                strBuilder.Append("PACK_TYPE_MST_TBL PTMT, ");
                strBuilder.Append("DETENTION_SLAB_MAIN_TBL DSMT, ");
                strBuilder.Append("CURRENCY_TYPE_MST_TBL CTMT, ");
                strBuilder.Append("CONTAINER_TYPE_MST_TBL CONT ");
                strBuilder.Append("WHERE ");
                strBuilder.Append("DCTRN.DETENTION_CALC_TBL_FK = DCHDR.DETENTION_CALC_TBL_PK ");
                //'Modified by Goutam  : Outer Join
                strBuilder.Append("AND DCSD.DETENTION_CALC_TRN_FK(+)=DCTRN.DETENTION_CALC_TRN_PK ");
                //'
                strBuilder.Append("AND DCHDR.VENDOR_MST_FK=VMT.VENDOR_MST_PK(+) ");
                strBuilder.Append("AND DCTRN.PACK_TYPE_FK=PTMT.PACK_TYPE_MST_PK(+) ");
                strBuilder.Append("AND DCHDR.VENDOR_MST_FK = DSMT.VENDOR_MST_FK ");
                strBuilder.Append("AND DSMT.CURRENCY_MST_FK=CTMT.CURRENCY_MST_PK ");
                strBuilder.Append("AND DCHDR.CURRENCY_MST_FK=CTMT.CURRENCY_MST_PK ");
                strBuilder.Append("AND DSMT.CARGO_TYPE=1 ");
                strBuilder.Append("AND DCTRN.CONTAINER_TYPE_MST_FK=CONT.CONTAINER_TYPE_MST_PK(+) ");
                strBuilder.Append("AND DCHDR.JOB_CARD_SEA_IMP_FK= " + strJobPk + strCondition + " ");
                strBuilder.Append("ORDER BY DCTRN.DETENTION_CALC_TRN_PK, DCSD.FROM_DAY ");
            }
            else
            {
                strBuilder.Append("SELECT distinct ");
                strBuilder.Append("DCHDR.DETENTION_CALC_TBL_PK AS DETENTION_CALC_TBL_PK, ");
                strBuilder.Append("DCHDR.JOB_CARD_SEA_IMP_FK AS JOB_CARD_SEA_IMP_PK, ");
                strBuilder.Append("DCTRN.DETENTION_CALC_TRN_PK AS DETENTION_CALC_TRN_PK, ");
                strBuilder.Append("VMT.VENDOR_ID VENDOR_ID, ");
                strBuilder.Append("TO_CHAR(DCTRN.DETENTION_CALC_DATE,dateformat) AS DETENTION_CALC_DATE, ");
                strBuilder.Append("DCTRN.CONTAINER_NUMBER AS CONTPAL_NUMBER, ");
                strBuilder.Append("PTMT.PACK_TYPE_ID AS CONTPACK_TYPE, ");
                strBuilder.Append("TO_CHAR(DCTRN.GEN_LAND_DATE,DATEFORMAT) AS GEN_LAND_DATE, ");
                strBuilder.Append("DCTRN.NO_OF_PACKAGES AS NO_OF_PACKAGES, ");
                //strBuilder.Append("DCTRN.GROSS_WEIGHT AS GROSS_WEIGHT, ")
                //modifying by thiyagarajan on 7/1/09 to display no.of palette in report being introducing it into the grid
                strBuilder.Append("DECODE(DCTRN.CALCULATION_BASIS, '1', DCTRN.GROSS_WEIGHT,'3',DCTRN.NO_OF_PALETTES)GROSS_WEIGHT, ");
                strBuilder.Append("DCTRN.VOLUME_IN_CBM AS VOLUME_IN_CBM, ");
                strBuilder.Append("((DCTRN.DETENTION_CALC_DATE - DCTRN.COLLECTING_DATE_TIME) - DCTRN.FREE_DAYS ) AS DETENTION_DAYS, ");
                strBuilder.Append("TO_CHAR(DCTRN.COLLECTING_DATE_TIME,DATEFORMAT) AS COLLECTING_DATE_TIME, ");
                strBuilder.Append("DECODE(DCTRN.CALCULATION_BASIS, '1', 'Weight','2','Volume','3','Palette') AS CALCULATION_BASIS, ");
                //end
                strBuilder.Append("DCTRN.FREE_DAYS AS FREE_DAYS, ");
                strBuilder.Append("DCSD.FROM_DAY AS FROM_DAY, ");
                strBuilder.Append("DCSD.TO_DAY AS TO_DAY, ");
                strBuilder.Append("DCSD.TARIFF_RATE AS TARIFF_RATE, ");
                strBuilder.Append("DCSD.NO_OF_DAYS AS NO_OF_DAYS, ");
                //'Goutam : DTS 9347
                //strBuilder.Append("DCSD.DETENTION_AMOUNT AS DETENTION_AMOUNT, ")
                strBuilder.Append(" DCTRN.DETENTION_AMT AS DETENTION_AMOUNT, ");
                //'
                strBuilder.Append("CTMT.CURRENCY_ID AS CURRENCY_ID ");
                strBuilder.Append("FROM ");
                strBuilder.Append("DETENTION_CALC_TBL DCHDR, ");
                strBuilder.Append("DETENTION_CALC_TRN DCTRN, ");
                strBuilder.Append("DETENTION_CALC_SLAB_DTLS DCSD, ");
                strBuilder.Append("VENDOR_MST_TBL VMT, ");
                strBuilder.Append("PACK_TYPE_MST_TBL PTMT, ");
                strBuilder.Append("DETENTION_SLAB_MAIN_TBL DSMT, ");
                strBuilder.Append("CURRENCY_TYPE_MST_TBL CTMT ");
                strBuilder.Append("WHERE ");
                strBuilder.Append("DCTRN.DETENTION_CALC_TBL_FK = DCHDR.DETENTION_CALC_TBL_PK ");
                strBuilder.Append("AND DCSD.DETENTION_CALC_TRN_FK(+)=DCTRN.DETENTION_CALC_TRN_PK ");
                strBuilder.Append("AND DCHDR.VENDOR_MST_FK=VMT.VENDOR_MST_PK(+) ");
                strBuilder.Append("AND DCTRN.PACK_TYPE_FK=PTMT.PACK_TYPE_MST_PK(+) ");
                strBuilder.Append("AND DCHDR.VENDOR_MST_FK = DSMT.VENDOR_MST_FK ");
                strBuilder.Append("AND DSMT.CURRENCY_MST_FK=CTMT.CURRENCY_MST_PK ");
                strBuilder.Append("AND DCHDR.CURRENCY_MST_FK=CTMT.CURRENCY_MST_PK ");
                strBuilder.Append("AND DSMT.CARGO_TYPE=2 ");
                strBuilder.Append("AND DCHDR.JOB_CARD_SEA_IMP_FK= " + strJobPk + strCondition + " ");
                strBuilder.Append("ORDER BY DCTRN.DETENTION_CALC_TRN_PK, DCSD.FROM_DAY ");
            }
            try
            {
                return objWF.GetDataSet(strBuilder.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        #endregion

        #region "Check for Concurrency"

        public DataTable funResetDataSet(string strJobPk, Int16 isFcl)
        {
            StringBuilder strBuilder = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataTable dsTemp = null;
            if (isFcl == 1)
            {
                strBuilder.Append("SELECT JOB_CONT.CONTAINER_NUMBER AS CONTAINER_NUMBER, ");
                strBuilder.Append("JOB_CONT.CONTAINER_TYPE_MST_FK CONTAINER_PK ");
                strBuilder.Append("FROM JOB_TRN_CONT   JOB_CONT, ");
                strBuilder.Append("JOB_CARD_TRN   JOB_CARD ");
                strBuilder.Append("WHERE(JOB_CONT.JOB_CARD_TRN_FK = JOB_CARD.JOB_CARD_TRN_PK) ");
                strBuilder.Append("AND JOB_CARD.JOB_CARD_TRN_PK = " + strJobPk + " ");
                strBuilder.Append("AND JOB_CONT.CONTAINER_NUMBER NOT IN (SELECT TRN.CONTAINER_NUMBER FROM DETENTION_CALC_TRN TRN, DETENTION_CALC_TBL HDR WHERE TRN.DETENTION_CALC_TBL_FK=HDR.DETENTION_CALC_TBL_PK AND HDR.JOB_CARD_SEA_IMP_FK=" + strJobPk + ") ");
                strBuilder.Append("ORDER BY JOB_CONT.CONTAINER_NUMBER ");
            }
            else
            {
                strBuilder.Append("SELECT ");
                strBuilder.Append("JOB_CONT.PACK_TYPE_MST_FK, ");
                strBuilder.Append("NVL(SUM(JOB_CONT.PACK_COUNT), 0) - (SELECT NVL(SUM(TRN.NO_OF_PACKAGES), 0) ");
                strBuilder.Append("FROM DETENTION_CALC_TRN TRN, DETENTION_CALC_TBL HDR ");
                strBuilder.Append("WHERE(TRN.DETENTION_CALC_TBL_FK = HDR.DETENTION_CALC_TBL_PK) ");
                strBuilder.Append("AND HDR.JOB_CARD_SEA_IMP_FK = " + strJobPk + " ");
                strBuilder.Append("AND TRN.PACK_TYPE_FK = JOB_CONT.PACK_TYPE_MST_FK) AS NO_OF_PACKAGES ");
                strBuilder.Append("FROM JOB_TRN_CONT JOB_CONT ");
                strBuilder.Append("WHERE(JOB_CONT.PACK_TYPE_MST_FK Is Not NULL) ");
                strBuilder.Append("AND JOB_CONT.JOB_CARD_TRN_FK = " + strJobPk + " ");
                strBuilder.Append("GROUP BY JOB_CONT.PACK_TYPE_MST_FK ");
                strBuilder.Append("ORDER BY JOB_CONT.PACK_TYPE_MST_FK ");
            }
            try
            {
                return objWF.GetDataTable(strBuilder.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //adding by thiyagarajan on 3/1/09 :VEK Gap analysis
        public Int32 chkdetjob(string strJobPk)
        {
            StringBuilder strBuilder = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strBuilder.Append("  select count(*) from DETENTION_CALC_TBL dtn where dtn.cargo_type=2 and dtn.job_card_sea_imp_fk=" + strJobPk);
                return Convert.ToInt32(objWF.ExecuteScaler(strBuilder.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "For Handling Charges"
        public int HandlingChargesFCL(string ConTyprPk = "", string DenTypeFk = "", bool IsFcl = true, string CalculationBasis = "1", string Weight = "0.0", string Volume = "0.0", string palette = "0.0")
        {
            try
            {
                StringBuilder strQuery = new StringBuilder();
                WorkFlow objWF = new WorkFlow();
                DataSet dsHandlingCharges = new DataSet();
                double HandlingCharges = 0.0;
                string detentionpk = null;

                detentionpk = Fetch_Detention_Pk();

                if ((detentionpk != null))
                {
                    if (IsFcl == true)
                    {
                        strQuery.Append("  SELECT SUM(CONT.OTH_CHG_PER_CONTAINER) OTH_COST" );
                        strQuery.Append("    FROM TARIFF_SEA_DEPOT_OTH_CHG TSD," );
                        strQuery.Append("         DETENTION_SLAB_MAIN_TBL DSM," );
                        //strQuery.Append("         TABLE(CONTAINER_DTL_FCL) CONT" & vbCrLf)
                        //Snigdharani - 31/10/2008 - Removing v-array
                        strQuery.Append("         TRF_DEPOT_OTHCHG_CONT_DET CONT" );
                        strQuery.Append("   WHERE TSD.DETENTION_SLAB_MAIN_FK = DSM.DETENTION_SLAB_MAIN_PK" );
                        strQuery.Append("   AND CONT.TARF_SEA_DEPOT_OTH_CHG_FK = TSD.TARF_SEA_DEPOT_OTH_CHG_PK" );
                        //Snigdharani
                        strQuery.Append("     AND CONT.CONTAINER_TYPE_MST_FK  in (" + ConTyprPk + ") " );
                        strQuery.Append("     AND DSM.DETENTION_SLAB_MAIN_PK = " + detentionpk);
                        strQuery.Append("" );
                        //adding "palette" by thiyagarajan on 5/1/09:VEK Gap Analysis
                    }
                    else
                    {
                        strQuery.Append("SELECT SUM(DECODE(" + CalculationBasis + "," );
                        strQuery.Append("              1," );
                        strQuery.Append("              TSD.LCL_RATE_PER_TON * ( " + Weight + " / 1000)," );
                        strQuery.Append("              2," );
                        strQuery.Append("              TSD.LCL_RATE_PER_CBM * " + Volume + " ,3 , tsd.lcl_rate_palette * " + palette + ")) OTH_COST" );
                        strQuery.Append("  FROM TARIFF_SEA_DEPOT_OTH_CHG TSD, DETENTION_SLAB_MAIN_TBL DSM" );
                        strQuery.Append(" WHERE TSD.DETENTION_SLAB_MAIN_FK = DSM.DETENTION_SLAB_MAIN_PK" );
                        strQuery.Append("   AND DSM.DETENTION_SLAB_MAIN_PK  =" + detentionpk);
                        strQuery.Append("" );
                    }

                    dsHandlingCharges = objWF.GetDataSet(strQuery.ToString());
                    if (dsHandlingCharges.Tables[0].Rows.Count > 0)
                    {
                        HandlingCharges = Convert.ToDouble(getDefault(dsHandlingCharges.Tables[0].Rows[0][0], 0.0));
                    }

                }
                return Convert.ToInt32(HandlingCharges);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch detention Pk "
        public string Fetch_Detention_Pk()
        {
            try
            {
                StringBuilder strQuery = new StringBuilder();
                WorkFlow objWF = new WorkFlow();
                DataSet dsDetentionCharges = new DataSet();
                string DetentionPk = null;
                strQuery.Append("SELECT DISTINCT T.DETENTION_SLAB_MAIN_FK FROM detention_sea_calc_report T" );
                dsDetentionCharges = objWF.GetDataSet(strQuery.ToString());
                if (dsDetentionCharges.Tables[0].Rows.Count > 0)
                {
                    DetentionPk = Convert.ToString(dsDetentionCharges.Tables[0].Rows[0]["DETENTION_SLAB_MAIN_FK"]);
                }
                return DetentionPk;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Freight Elements  From Parmeters"
        public DataSet Parameter_Fk()
        {
            StringBuilder strBuilder = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;
            string strParameterPk = null;

            try
            {
                strBuilder.Append("select pt.frt_det_charge_fk, femt.freight_element_name from parameters_tbl pt ,freight_element_mst_tbl femt where femt.freight_element_mst_pk = pt.frt_det_charge_fk ");

                DS = objWK.GetDataSet(strBuilder.ToString());
                return DS;
                //If DS.Tables(0).Rows.Count <> 0 Then
                //    strParameterPk = DS.Tables(0).Rows(0).Item("FRT_DET_CHARGE_FK")
                //End If

                //Return strParameterPk
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Save in Conslidated Invoice"
        private void SaveInvoiceHeader(DataSet hdrDS, OracleTransaction TRAN, long nDetCalcPK, string InvRefNo, string uniqueReferenceNr, long nJobCardPK, ref DataSet trnDS)
        {
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            try
            {
                objWK.MyConnection = TRAN.Connection;
                objWK.MyCommand.Parameters.Clear();
                hdrDS.Tables[0].Rows[0]["JOB_CARD_SEA_IMP_FK"] = nJobCardPK;
                var _with14 = insCommand;
                _with14.Transaction = TRAN;
                _with14.Connection = objWK.MyConnection;
                _with14.CommandType = CommandType.StoredProcedure;
                _with14.CommandText = objWK.MyUserName + ".DETENTION_CALC_TBL_PKG.DETENTION_TO_CONS_INV_HDR";

                //JOB_CARD_SEA_IMP_FK_IN,
                _with14.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", hdrDS.Tables[0].Rows[0]["JOB_CARD_SEA_IMP_FK"]).Direction = ParameterDirection.Input;
                //.Parameters["JOB_CARD_SEA_IMP_FK_IN"].SourceVersion = DataRowVersion.Current
                //DETENTION_PK_IN 
                _with14.Parameters.Add("DETENTION_PK_IN", nDetCalcPK).Direction = ParameterDirection.Input;
                //INVOICE_REF_NO_IN 
                _with14.Parameters.Add("INVOICE_REF_NO_IN", InvRefNo).Direction = ParameterDirection.Input;
                //DETENTION_DATE_IN,
                _with14.Parameters.Add("DETENTION_DATE_IN", Convert.ToDateTime(hdrDS.Tables[0].Rows[0]["DETENTION_DATE"]).ToString(dateFormat)).Direction = ParameterDirection.Input;
                //.Parameters["DETENTION_DATE_IN"].SourceVersion = DataRowVersion.Current
                //GROSS_DETENTION_AMT_IN,
                _with14.Parameters.Add("GROSS_DETENTION_AMT_IN", hdrDS.Tables[0].Rows[0]["GROSS_DETENTION_AMT"]).Direction = ParameterDirection.Input;
                //.Parameters["GROSS_DETENTION_AMT_IN"].SourceVersion = DataRowVersion.Current
                //WAIVER_AMT_IN,
                _with14.Parameters.Add("WAIVER_AMT_IN", hdrDS.Tables[0].Rows[0]["WAIVER_AMT"]).Direction = ParameterDirection.Input;
                //.Parameters["WAIVER_AMT_IN"].SourceVersion = DataRowVersion.Current
                //NET_DETENTION_AMT_IN,
                _with14.Parameters.Add("NET_DETENTION_AMT_IN", hdrDS.Tables[0].Rows[0]["NET_DETENTION_AMT"]).Direction = ParameterDirection.Input;
                //.Parameters["NET_DETENTION_AMT_IN"].SourceVersion = DataRowVersion.Current
                //CURRENCY_MST_FK_IN,
                _with14.Parameters.Add("CURRENCY_MST_FK_IN", hdrDS.Tables[0].Rows[0]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                //.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                //CREATED_BY_FK_IN,
                _with14.Parameters.Add("CREATED_BY_FK_IN", hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]).Direction = ParameterDirection.Input;
                //.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current
                //RETURN_VALUE
                _with14.Parameters.Add("INVREFNO_IN", uniqueReferenceNr).Direction = ParameterDirection.Input;

                _with14.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with14.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                //AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)
                arrMessage.Clear();
                RecAfct = insCommand.ExecuteNonQuery();
                //With objWK.MyDataAdapter
                //    .InsertCommand = insCommand
                //    .InsertCommand.Transaction = TRAN
                //    RecAfct = .Update(hdrDS.Tables(0))
                //End With
                ConsInvPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                if (RecAfct > 0)
                {
                    long ConsInvPK = 0;
                    ConsInvPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    SaveJCFDAndInvoiceDet(trnDS, TRAN, nDetCalcPK, nJobCardPK, Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value), Convert.ToInt32(hdrDS.Tables[0].Rows[0]["CURRENCY_MST_FK"]));
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
                //Finally
                //    objWK.CloseConnection()
            }
        }
        private void SaveJCFDAndInvoiceDet(DataSet trnDS, OracleTransaction TRAN, long nDetCalcPK, long nJobCardPK, long ConsInvPK, long CurrPk)
        {
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            int i = 0;
            try
            {
                RecAfct = 0;
                objWK.MyConnection = TRAN.Connection;
                for (i = 0; i <= trnDS.Tables[0].Rows.Count - 1; i++)
                {
                    var _with15 = insCommand;
                    _with15.Transaction = TRAN;
                    _with15.Connection = objWK.MyConnection;
                    _with15.CommandType = CommandType.StoredProcedure;
                    _with15.CommandText = objWK.MyUserName + ".DETENTION_CALC_TBL_PKG.DET_TO_CONS_JCSI_FD";
                    _with15.Parameters.Clear();
                    // CONSOL_INVOICE_PK          
                    _with15.Parameters.Add("CONSOL_INVOICE_PK", ConsInvPK).Direction = ParameterDirection.Input;
                    // JOB_CARD_SEA_IMP_FK_IN          
                    _with15.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", nJobCardPK).Direction = ParameterDirection.Input;
                    // DETENTION_CALC_TBL_FK_IN          
                    _with15.Parameters.Add("DETENTION_CALC_TBL_FK_IN", nDetCalcPK).Direction = ParameterDirection.Input;
                    // CONTAINER_TYPE_MST_FK_IN,
                    _with15.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", trnDS.Tables[0].Rows[i]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                    //DETENTION_AMT_IN,
                    _with15.Parameters.Add("DETENTION_AMT_IN", trnDS.Tables[0].Rows[i]["DETENTION_AMT"]).Direction = ParameterDirection.Input;
                    //CURRENCY_MST_FK_IN,
                    _with15.Parameters.Add("CURRENCY_MST_FK_IN", CurrPk).Direction = ParameterDirection.Input;
                    //RETURN_VALUE
                    _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    RecAfct = _with15.ExecuteNonQuery();
                }
                if (RecAfct == 0)
                {
                    arrMessage.Add("Save not successful");
                }
                else
                {
                    int OthChgPk = 0;
                    OthChgPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    SaveTrn(trnDS, TRAN, nDetCalcPK, nJobCardPK, OthChgPk);
                }
            }
            catch (OracleException ex1)
            {
                arrMessage.Add(ex1.Message);
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
            }
        }
        #endregion

        #region "Update Other Charge Table with the invoice pk"
        public void UpdateJCOth(long InvPk, long OthChgTblPk)
        {
            bool flag = false;
            try
            {
                StringBuilder sb = new StringBuilder(5000);
                sb.Append("UPDATE JOB_TRN_OTH_CHRG");
                sb.Append("   SET CONSOL_INVOICE_TRN_FK = (SELECT TRN.CONSOL_INVOICE_TRN_PK");
                sb.Append("                                  FROM CONSOL_INVOICE_TRN_TBL TRN");
                sb.Append("                                 WHERE ROWNUM = 1 AND TRN.CONSOL_INVOICE_FK = " + InvPk + ")");
                sb.Append(" WHERE JOB_TRN_OTH_PK = " + OthChgTblPk);
                flag = (new WorkFlow()).ExecuteCommands(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        //'Added By Koteshwari on 27/5/2011
        #region "Save Cost"
        public ArrayList SaveCost(ref long JobCardPK, DataSet dsCostDetails)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            int i = 0;
            OracleCommand insCostDetails = new OracleCommand();
            //'Added By Koteshwari 
            OracleCommand updCostDetails = new OracleCommand();
            try
            {
                for (i = 0; i <= dsCostDetails.Tables[0].Rows.Count - 1; i++)
                {

                    if (string.IsNullOrEmpty(dsCostDetails.Tables[0].Rows[i]["JOB_TRN_COST_PK"].ToString()))
                    {
                        var _with16 = objWK.MyCommand;
                        _with16.Transaction = TRAN;
                        _with16.Connection = objWK.MyConnection;
                        _with16.CommandType = CommandType.StoredProcedure;
                        _with16.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_SEA_IMP_COST_INS";
                        _with16.Parameters.Clear();
                        var _with17 = _with16.Parameters;
                        _with17.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                        _with17.Add("VENDOR_MST_FK_IN", dsCostDetails.Tables[0].Rows[i]["VENDOR_MST_PK"]).Direction = ParameterDirection.Input;
                        _with17.Add("COST_ELEMENT_FK_IN", dsCostDetails.Tables[0].Rows[i]["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                        _with17.Add("LOCATION_FK_IN", dsCostDetails.Tables[0].Rows[i]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                        _with17.Add("VENDOR_KEY_IN", dsCostDetails.Tables[0].Rows[i]["VENDOR_KEY"]).Direction = ParameterDirection.Input;
                        _with17.Add("PTMT_TYPE_IN", Convert.ToInt32(dsCostDetails.Tables[0].Rows[0]["PTMT_TYPE"])).Direction = ParameterDirection.Input;
                        _with17.Add("CURRENCY_MST_FK_IN", dsCostDetails.Tables[0].Rows[i]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with17.Add("ESTIMATED_COST_IN", dsCostDetails.Tables[0].Rows[i]["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                        _with17.Add("TOTAL_COST_IN", dsCostDetails.Tables[0].Rows[i]["TOTAL_COST"]).Direction = ParameterDirection.Input;
                        _with17.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        RecAfct = _with16.ExecuteNonQuery();
                    }
                    else
                    {
                        var _with18 = objWK.MyCommand;
                        _with18.Transaction = TRAN;
                        _with18.Connection = objWK.MyConnection;
                        _with18.CommandType = CommandType.StoredProcedure;
                        _with18.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_UPD";
                        _with18.Parameters.Clear();
                        var _with19 = _with18.Parameters;

                        _with19.Add("JOB_TRN_COST_PK_IN", dsCostDetails.Tables[0].Rows[i]["JOB_TRN_SEA_IMP_COST_PK"]).Direction = ParameterDirection.Input;
                        _with19.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                        _with19.Add("VENDOR_MST_FK_IN", dsCostDetails.Tables[0].Rows[i]["VENDOR_MST_PK"]).Direction = ParameterDirection.Input;
                        _with19.Add("COST_ELEMENT_FK_IN", dsCostDetails.Tables[0].Rows[i]["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                        _with19.Add("LOCATION_FK_IN", dsCostDetails.Tables[0].Rows[i]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                        _with19.Add("VENDOR_KEY_IN", dsCostDetails.Tables[0].Rows[i]["VENDOR_KEY"]).Direction = ParameterDirection.Input;
                        _with19.Add("PTMT_TYPE_IN", Convert.ToInt32(dsCostDetails.Tables[0].Rows[i]["PTMT_TYPE"])).Direction = ParameterDirection.Input;
                        _with19.Add("CURRENCY_MST_FK_IN", dsCostDetails.Tables[0].Rows[i]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with19.Add("ESTIMATED_COST_IN", dsCostDetails.Tables[0].Rows[i]["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                        _with19.Add("TOTAL_COST_IN", dsCostDetails.Tables[0].Rows[i]["TOTAL_COST"]).Direction = ParameterDirection.Input;
                        _with19.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        RecAfct = _with18.ExecuteNonQuery();
                    }
                }

                if (RecAfct > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }
        #endregion

        #region "Get COST ELEMENT ID"
        public object GetCostElement(string CostElmt)
        {
            StringBuilder sb = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT CMT.COST_ELEMENT_MST_PK FROM COST_ELEMENT_MST_TBL CMT WHERE CMT.COST_ELEMENT_ID='" + CostElmt + "'");
                return (objWF.GetDataSet(sb.ToString()));
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

        //'End
    }
}