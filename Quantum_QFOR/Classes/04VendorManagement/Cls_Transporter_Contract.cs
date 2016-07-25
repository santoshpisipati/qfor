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
using System.Web;

namespace Quantum_QFOR
{
    class Cls_Transporter_Contract : CommonFeatures
    {
        #region "Fetch weight range"
        public DataSet fetchWeightRange(string ContPk = "")
        {
            string strSQL = "";
            string strCondition = "";

            WorkFlow objWF = new WorkFlow();
            //If Zonepk = 0 Then
            //    strCondition &= vbCrLf & " AND TRANSPORTER_ZONES_FK =" & transPk
            //End If
            if (!string.IsNullOrEmpty(ContPk))
            {
                strCondition += " AND CONT_MAIN_TRANS_FK =" + ContPk;
            }
            else
            {
                strCondition += " AND CONT_MAIN_TRANS_FK =" + 0;
            }
            strSQL += "SELECT ";
            strSQL += "CONT_MAIN_TRANS_FK, ";
            strSQL += "TRANSPORTER_ZONES_FK, ";
            strSQL += "FROM_WEIGHT, ";
            strSQL += "TO_WEIGHT, ";
            strSQL += "RANGE_RATE ";
            strSQL += " FROM CONT_TRN_TRANS ";
            strSQL += " WHERE  1=1";

            strSQL += strCondition;

            //sorting definition
            strSQL += " order by FROM_WEIGHT ";

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

        #region "Contract details"
        public DataSet fetchContract(string transPk, string ContPk = "", int EditFlag = 0, Int32 Btype = 0)
        {
            string strSQL = "";
            string strCondition = "";

            WorkFlow objWF = new WorkFlow();
            if (EditFlag != 1)
            {
                strCondition += " AND ACTIVE =" + 1;
                strCondition += " AND (VALID_TO >= TO_DATE(SYSDATE) OR VALID_TO IS NULL)";
                if (!string.IsNullOrEmpty(ContPk))
                {
                    strCondition += " AND CONT_MAIN_TRANS_PK =" + ContPk;
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(ContPk))
                {
                    strCondition += " AND CONT_MAIN_TRANS_PK =" + ContPk;
                }
            }
            if (!string.IsNullOrEmpty(transPk))
            {
                strCondition += " AND TRANSPORTER_MST_FK =" + transPk;
            }
            //strCondition &= vbCrLf & " AND TRANSPORTER_MST_FK =" & transPk
            strCondition += " AND BUSINESS_TYPE =" + Btype;
            strSQL += "SELECT ";
            strSQL += "CONT_MAIN_TRANS_PK, ";
            strSQL += "TRANSPORTER_MST_FK, ";
            strSQL += "CONTRACT_NO, ";
            strSQL += "CONTRACT_DATE, ";
            strSQL += "CONT_APPROVED, ";
            strSQL += "ACTIVE, ";
            strSQL += "CURRENCY_MST_FK, ";
            strSQL += "VALID_FROM, ";
            strSQL += "VALID_TO, ";
            strSQL += "BUSINESS_TYPE,";
            // By Amit
            strSQL += "RATE_APPLICABILITY_TYPE,";
            strSQL += "BASE_RATE,";
            strSQL += "MIN_RATE,";
            // End
            strSQL += "VERSION_NO,";
            strSQL += "TO_CHAR(LAST_MODIFIED_DT,'" + dateFormat + "') LAST_MODIFIED_DT,workflow_status,AMEND_FLAG";
            strSQL += " FROM CONT_MAIN_TRANS_TBL ";
            strSQL += " WHERE  1=1";
            strSQL += strCondition;
            //sorting definition
            //strSQL &= " order by FROM_WEIGHT "

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
        public string GenerateContractNo(string Protocol, Int64 ILocationId, Int64 IEmployeeId, int createdBy)
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey(Protocol, ILocationId, IEmployeeId, DateTime.Now, "", "", "", createdBy);
            //GenerateServiceNo = GenerateProtocolKey("VIEWCANCELLLATION", ILocationId, IEmployeeId, DateTime.Now, txtVessel.Text, txtVoyage.Text)
            return functionReturnValue;
        }
        public ArrayList SaveContract(int transporterFk, string contracNo, string contracNewNo, System.DateTime contractDate, int approved, int Currency, int active, System.DateTime validFrom, DateTime validTo, int CreatedBy,
        int configFk, DataSet ContractDs, int rateType, int BaseRate = 0, int MinRate = 0, int ContractPk = 0, int ZoneFk = 0, int Biztype = 0, int Version = 0, int Editflag = 0,
        long Location = 0, long employeeId = 0, long Int_Wf_status = 0, bool AmendFlg = false)
        {
            System.DBNull StrNull = null;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand updDateCommand = new OracleCommand();
            OracleCommand insTRNCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            WorkFlow objWK = new WorkFlow();
            string contractNo = null;
            int PrevContPK = 0;
            objWK.OpenConnection();
            OracleTransaction Tran1 = null;
            Tran1 = objWK.MyConnection.BeginTransaction();
            try
            {
                if (Editflag != 1)
                {
                    if (Biztype == 1)
                    {
                        contractNo = GenerateContractNo("AIR TRANSPORTER CONTRACT", Location, employeeId, CreatedBy);
                    }
                    else
                    {
                        contractNo = GenerateContractNo("SEA TRANSPORTER CONTRACT", Location, employeeId, CreatedBy);
                    }
                }
                try
                {
                    //update data in master table
                    if (AmendFlg == true)
                    {
                        Editflag = 0;
                        PrevContPK = ContractPk;
                        ContractPk = 0;
                    }
                    if (Editflag == 1)
                    {
                        if (ContractPk != 0)
                        {
                            //Tran1 = objWK.MyConnection.BeginTransaction()
                            var _with1 = updCommand;
                            _with1.Transaction = Tran1;
                            _with1.Connection = objWK.MyConnection;
                            _with1.CommandType = CommandType.StoredProcedure;
                            _with1.CommandText = objWK.MyUserName + ".CONT_MAIN_TRANS_TBL_PKG_TEMP.CONT_MAIN_TRANS_TBL_UPD";
                            var _with2 = _with1.Parameters;
                            _with2.Add("CONT_MAIN_TRANS_PK_IN", ContractPk);
                            _with2.Add("TRANSPORTER_MST_FK_IN", transporterFk);
                            _with2.Add("CONTRACT_NO_IN", contracNo);
                            _with2.Add("CONTRACT_DATE_IN", contractDate);
                            _with2.Add("CONT_APPROVED_IN", Int_Wf_status);
                            _with2.Add("ACTIVE_IN", active);
                            _with2.Add("CURRENCY_MST_FK_IN", Currency);
                            _with2.Add("VALID_FROM_IN", validFrom);
                            _with2.Add("VALID_TO_IN", (validTo.Date == DateTime.MinValue ? DateTime.MinValue : validTo));
                            _with2.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK);
                            _with2.Add("CONFIG_MST_FK_IN", configFk);
                            _with2.Add("BUSINESS_TYPE_IN", Biztype);
                            //By Amit on 22June07
                            _with2.Add("RATE_APPLICABILITY_TYPE_IN", rateType);
                            _with2.Add("BASE_RATE_IN", BaseRate);
                            _with2.Add("MIN_RATE_IN", MinRate);
                            //End
                            _with2.Add("VERSION_NO_IN", Version);
                            _with2.Add("RETURN_VALUE", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Output;
                            updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            _with1.ExecuteNonQuery();
                            _with1.Parameters.Clear();
                        }

                    }
                    else
                    {
                        if (ContractPk != 0)
                        {
                            //With updDateCommand
                            //    .Transaction = Tran1
                            //    .Connection = objWK.MyConnection
                            //    .CommandType = CommandType.StoredProcedure
                            //    .CommandText = objWK.MyUserName & ".CONT_MAIN_TRANS_TBL_PKG.CONT_DATE_TBL_UPD"
                            //    'validFrom.Date.AddDays(-1) = Date.Today.AddDays(-1)
                            //    With .Parameters
                            //        .Add("CONT_MAIN_TRANS_PK_IN", ContractPk)
                            //        .Add("TRANSPORTER_MST_FK_IN", transporterFk)
                            //        'VALID_TO_IN                   IN   CONT_MAIN_TRANS_TBL.VALID_TO%TYPE,
                            //        .Add("VALID_TO_IN", validFrom.Date.AddDays(-1))
                            //        .Add("LAST_MODIFIED_BY_FK_IN", M_Last_Modified_By_FK)
                            //        .Add("CONFIG_MST_FK_IN", configFk)
                            //        .Add("RETURN_VALUE", ContractPk).Direction = ParameterDirection.Output
                            //        updDateCommand.Parameters("RETURN_VALUE").SourceVersion = DataRowVersion.Current
                            //    End With
                            //    .ExecuteNonQuery()
                            //    .Parameters.Clear()
                            //    'validFrom = Date.Today.AddDays(1)
                            //End With
                        }
                    }
                    Tran1.Commit();

                }
                catch (OracleException oraexp)
                {
                    throw oraexp;
                    Tran1.Rollback();
                }
                catch (Exception ex)
                {
                    throw ex;
                    Tran1.Rollback();
                }
                finally
                {
                    objWK.MyConnection.Close();
                }

                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();


                //If ContractPk = 0 Then
                if (Editflag != 1)
                {
                    var _with3 = insCommand;
                    _with3.Transaction = TRAN;
                    _with3.Connection = objWK.MyConnection;
                    _with3.CommandType = CommandType.StoredProcedure;
                    _with3.CommandText = objWK.MyUserName + ".CONT_MAIN_TRANS_TBL_PKG_TEMP.CONT_MAIN_TRANS_TBL_INS";
                    var _with4 = _with3.Parameters;
                    _with4.Add("TRANSPORTER_MST_FK_IN", transporterFk);
                    ///.Add("CONTRACT_NO_IN", contracNewNo)'Commented By Sivachandran 
                    if (AmendFlg == true)
                    {
                        _with4.Add("CONTRACT_NO_IN", contracNo);
                    }
                    else
                    {
                        _with4.Add("CONTRACT_NO_IN", contractNo);
                    }
                    _with4.Add("CONTRACT_DATE_IN", contractDate);
                    _with4.Add("CONT_APPROVED_IN", approved);
                    _with4.Add("ACTIVE_IN", active);
                    _with4.Add("CURRENCY_MST_FK_IN", Currency);
                    _with4.Add("VALID_FROM_IN", (validFrom.Date == null ? DateTime.Today : validFrom));
                    //.Month.ToString() & "/" & validFrom.Day.ToString() & "/" & validFrom.Year.ToString)
                    _with4.Add("VALID_TO_IN", (validTo.Date == DateTime.MinValue ? DateTime.MinValue : validTo));
                    _with4.Add("CREATED_BY_FK_IN", CreatedBy);
                    _with4.Add("CONFIG_MST_FK_IN", configFk);
                    _with4.Add("BUSINESS_TYPE_IN", Biztype);
                    //'added by subhransu for pts:jun-25
                    _with4.Add("WORKFLOW_STATUS_IN", Int_Wf_status);
                    //By Amit on 22June07
                    _with4.Add("RATE_APPLICABILITY_TYPE_IN", rateType);
                    _with4.Add("BASE_RATE_IN", BaseRate);
                    _with4.Add("MIN_RATE_IN", MinRate);
                    //End
                    _with4.Add("RETURN_VALUE", ContractPk).Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    _with3.ExecuteNonQuery();
                    ContractPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    _with3.Parameters.Clear();
                    //Else
                }
                //inserting tha data in transation table
                DataRow record = null;
                if (ContractDs.Tables[0].Rows.Count != 0)
                {
                    var _with5 = insTRNCommand;
                    _with5.Transaction = TRAN;
                    _with5.Connection = objWK.MyConnection;
                    _with5.CommandType = CommandType.StoredProcedure;
                    _with5.CommandText = objWK.MyUserName + ".CONT_MAIN_TRANS_TBL_PKG.CONT_TRN_TRANS_INS";
                    foreach (DataRow record_loopVariable in ContractDs.Tables[0].Rows)
                    {
                        record = record_loopVariable;
                        //If record.RowState = DataRowState.Added Then
                        if (Editflag != 1)
                        {
                            var _with6 = _with5.Parameters;
                            _with6.Clear();
                            _with6.Add("CONT_MAIN_TRANS_FK_IN", ContractPk);
                            _with6.Add("TRANSPORTER_ZONES_FK_IN", record["TRANSPORTER_ZONES_FK"]);
                            _with6.Add("FROM_WEIGHT_IN", record["FROM_WEIGHT"]).Direction = ParameterDirection.Input;
                            _with6.Add("TO_WEIGHT_IN", record["TO_WEIGHT"]).Direction = ParameterDirection.Input;
                            _with6.Add("RANGE_RATE_IN", record["RANGE_RATE"]).Direction = ParameterDirection.Input;
                            _with6.Add("RETURN_VALUE", ContractPk).Direction = ParameterDirection.Output;

                        }
                        else
                        {
                            var _with7 = _with5.Parameters;
                            _with7.Clear();
                            _with7.Add("CONT_MAIN_TRANS_FK_IN", (Convert.ToInt32(record["CONT_MAIN_TRANS_FK"]) == 1 ? ContractPk : record["CONT_MAIN_TRANS_FK"]));
                            _with7.Add("TRANSPORTER_ZONES_FK_IN", record["TRANSPORTER_ZONES_FK"]);
                            _with7.Add("FROM_WEIGHT_IN", record["FROM_WEIGHT"]).Direction = ParameterDirection.Input;
                            _with7.Add("TO_WEIGHT_IN", record["TO_WEIGHT"]).Direction = ParameterDirection.Input;
                            _with7.Add("RANGE_RATE_IN", record["RANGE_RATE"]).Direction = ParameterDirection.Input;
                            _with7.Add("RETURN_VALUE", ContractPk).Direction = ParameterDirection.Output;
                        }
                        _with5.ExecuteNonQuery();

                        if (Convert.ToInt32(record["CONT_MAIN_TRANS_FK"]) == ContractPk | string.IsNullOrEmpty(record["CONT_MAIN_TRANS_FK"].ToString()) | Convert.ToInt32(record["CONT_MAIN_TRANS_FK"]) == 1)
                        {
                        }
                        //End If
                    }
                }
                //'
                if (AmendFlg == true)
                {
                    string str = null;
                    Int32 intIns = default(Int32);
                    System.DateTime ContDate = default(System.DateTime);
                    OracleCommand updCmdUser = new OracleCommand();
                    updCmdUser.Transaction = TRAN;
                    ContDate = GetContractDt(Convert.ToString(PrevContPK));
                    if (ContDate > DateTime.Today.Date)
                    {
                        str = " update CONT_MAIN_TRANS_TBL CONFT SET CONFT.CONT_STATUS = 2, ";
                        str += " CONFT.AMEND_FLAG=1 ";
                        str += " WHERE CONFT.CONT_MAIN_TRANS_PK=" + PrevContPK;
                    }
                    else
                    {
                        str = " update CONT_MAIN_TRANS_TBL CONFT SET CONFT.VALID_TO = SYSDATE ,";
                        str += " CONFT.AMEND_FLAG=1 ";
                        str += " WHERE CONFT.CONT_MAIN_TRANS_PK=" + PrevContPK;
                    }
                    var _with8 = updCmdUser;
                    _with8.Connection = objWK.MyConnection;
                    _with8.Transaction = TRAN;
                    _with8.CommandType = CommandType.Text;
                    _with8.CommandText = str;
                    intIns = _with8.ExecuteNonQuery();
                }
                //'
                //AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)
                //With objWK.MyDataAdapter
                //    .InsertCommand = insTRNCommand
                //    .InsertCommand.Transaction = TRAN
                //    RecAfct = .Update(ContractDs)
                //End With

                if (arrMessage.Count > 0)
                {
                    if (Editflag != 1)
                    {
                        if (Biztype == 1)
                        {
                            RollbackProtocolKey("AIR TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contractNo, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        else
                        {
                            RollbackProtocolKey("SEA TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contractNo, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                    }
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
                arrMessage.Add(oraexp.Message);
                if (Editflag != 1)
                {
                    if (Biztype == 1)
                    {
                        RollbackProtocolKey("AIR TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contractNo, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
                    else
                    {
                        RollbackProtocolKey("SEA TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contractNo, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
                }
                TRAN.Rollback();
                Tran1.Rollback();
                return arrMessage;
                //Throw oraexp
            }
            catch (Exception ex)
            {
                if (Editflag != 1)
                {
                    if (Biztype == 1)
                    {
                        RollbackProtocolKey("AIR TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contractNo, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
                    else
                    {
                        RollbackProtocolKey("SEA TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contractNo, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
                }
                TRAN.Rollback();
                Tran1.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }
        #endregion

        #region "Enhance Search & Lookup Search Block"
        //Pls do the impact the analysis before changing as this function
        //as might be accesed by other forms also. 
        public string FetchTransporter(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            int strBIZ_TYPE_IN = 0;
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            //strLOC_MST_IN = arr(2)
            strBIZ_TYPE_IN = Convert.ToInt32(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_TRANSPORTER_PKG.GETTRANSPORTER_COMMON";

                var _with9 = selectCommand.Parameters;
                _with9.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                //.Add("BUSINESS_MODEL_IN", strBUSINESS_MODEL_IN).Direction = ParameterDirection.Input
                _with9.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input
                _with9.Add("BIZ_TYPE_IN", strBIZ_TYPE_IN).Direction = ParameterDirection.Input;
                _with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
        #endregion

        //Amit : 14 July 07
        #region "Fetch Rate"
        public DataSet fetchRate(string Contfk = "")
        {
            string strSQL = "";
            string strCondition = "";

            WorkFlow objWF = new WorkFlow();

            strSQL += " SELECT ";
            strSQL += " CMT.RATE_APPLICABILITY_TYPE, ";
            strSQL += " CMT.BASE_RATE, ";
            strSQL += " CMT.MIN_RATE ";
            strSQL += " FROM CONT_MAIN_TRANS_TBL CMT ";
            strSQL += " WHERE CMT.CONT_MAIN_TRANS_PK =" + Contfk;

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

        #region "Fetch Report Detail"
        public DataSet FetchReportDetail(int trnPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWK = new WorkFlow();
                sb.Append("SELECT  VMT.VENDOR_NAME,");
                sb.Append("        VCD.ADM_ADDRESS_1,");
                sb.Append("        VCD.ADM_ADDRESS_2,");
                sb.Append("        VCD.ADM_ADDRESS_3,");
                sb.Append("        VCD.ADM_ZIP_CODE,");
                sb.Append("        VCD.ADM_CITY,");
                sb.Append("        DECODE(CTFT.BUSINESS_TYPE,1,'AIR',2,'SEA') BIZ_TYPE,");
                sb.Append("        DECODE(CTFT.BUSINESS_TYPE,1,'',2,'LCL') CARGO_TYPE,");
                sb.Append("        CTFT.CONTRACT_NO,");
                sb.Append("       CTFT.CONTRACT_DATE,");
                sb.Append("       CTFT.VALID_FROM,");
                sb.Append("       CTFT.VALID_TO,");
                //sb.Append("CASE WHEN CUMT.USER_ID IS NULL THEN")
                //sb.Append("         LUMT.USER_NAME ")
                //sb.Append("       ELSE")
                //sb.Append("         CUMT.USER_NAME")
                //sb.Append("       END APPBY_ID,")
                //sb.Append("       CTFT.LAST_MODIFIED_DT")
                sb.Append(" CASE WHEN CTFT.CONT_APPROVED = 1 THEN");
                sb.Append("         LUMT.USER_NAME ");
                sb.Append("       ELSE");
                sb.Append("         ' '");
                sb.Append("       END APPBY_ID,");
                sb.Append("       CASE WHEN CTFT.CONT_APPROVED = 1 THEN");
                sb.Append("       CTFT.LAST_MODIFIED_DT");
                sb.Append("       ELSE");
                sb.Append("        NULL");
                sb.Append("       END LAST_MODIFIED_DT");
                sb.Append(" FROM  CONT_MAIN_TRANS_TBL CTFT, ");
                sb.Append("      VENDOR_MST_TBL      VMT,");
                sb.Append("      VENDOR_TYPE_MST_TBL VTM,");
                sb.Append("       VENDOR_CONTACT_DTLS VCD,");
                sb.Append("       VENDOR_SERVICES_TRN VST,");
                sb.Append("       USER_MST_TBL        CUMT,");
                sb.Append("       USER_MST_TBL        LUMT");
                sb.Append("     WHERE CTFT.CONT_MAIN_TRANS_PK=" + trnPK);
                sb.Append("      AND CTFT.TRANSPORTER_MST_FK = VMT.VENDOR_MST_PK");
                sb.Append("      AND VST.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                sb.Append("   AND VST.VENDOR_TYPE_FK = VTM.VENDOR_TYPE_PK");
                sb.Append("   AND VCD.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                sb.Append("   AND VTM.VENDOR_TYPE_ID = 'TRANSPORTER'");
                sb.Append("   AND CTFT.CREATED_BY_FK = CUMT.USER_MST_PK(+)");
                sb.Append("   AND CTFT.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+)");
                sb.Append("");
                return objWK.GetDataSet(sb.ToString());
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Freight Detail"
        public DataSet FetchFreightDetail(int trnPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                WorkFlow objWK = new WorkFlow();
                sb.Append("SELECT TZT.ZONE_NAME,");
                sb.Append("       DECODE(CMTT.RATE_APPLICABILITY_TYPE, 0, 'SLAB RATE', 1, 'SLAB RATE OR MINIMUM RATE ', 2, 'SLAB RATE + BASE RATE') RATE_APPLICABILITY_TYPE,");
                sb.Append("       CMTT.BASE_RATE,");
                sb.Append("       CMTT.MIN_RATE,");
                sb.Append("       CTT.FROM_WEIGHT,");
                sb.Append("       CTT.TO_WEIGHT,");
                sb.Append("       CTT.RANGE_RATE");
                sb.Append("  FROM CONT_MAIN_TRANS_TBL   CMTT,");
                sb.Append("       CONT_TRN_TRANS        CTT,");
                sb.Append("       TRANSPORTER_ZONES_TBL TZT");
                sb.Append(" WHERE CMTT.CONT_MAIN_TRANS_PK = " + trnPK);
                sb.Append("   AND CMTT.CONT_MAIN_TRANS_PK = CTT.CONT_MAIN_TRANS_FK");
                sb.Append("   AND TZT.TRANSPORTER_ZONES_PK = CTT.TRANSPORTER_ZONES_FK");
                sb.Append("   ORDER BY FROM_WEIGHT");
                return objWK.GetDataSet(sb.ToString());
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "Update File Name"
        public bool UpdateFileName(long TransPk, string strFileName, Int16 Flag)
        {
            if (strFileName.Trim().Length > 0)
            {
                string RemQuery = null;
                WorkFlow objwk = new WorkFlow();
                if (Flag == 1)
                {
                    RemQuery = " UPDATE CONT_MAIN_TRANS_TBL CMTT SET CMTT.ATTACHED_FILE_NAME='" + strFileName + "'";
                    RemQuery += " WHERE CMTT.CONT_MAIN_TRANS_PK = " + TransPk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (OracleException oraexp)
                    {
                        throw oraexp;
                        //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                        return false;
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
                else if (Flag == 2)
                {
                    RemQuery = " UPDATE CONT_MAIN_TRANS_TBL CMTT SET CMTT.ATTACHED_FILE_NAME='" + DBNull.Value + "'";
                    RemQuery += " WHERE CMTT.CONT_MAIN_TRANS_PK = " + TransPk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (OracleException oraexp)
                    {
                        throw oraexp;
                        //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                        return false;
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
            }
            return false;
        }

        public string FetchFileName(long TransPk)
        {
            string strSQL = null;
            string strUpdFileName = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT ";
            strSQL += " CMTT.ATTACHED_FILE_NAME FROM CONT_MAIN_TRANS_TBL CMTT WHERE CMTT.CONT_MAIN_TRANS_PK = " + TransPk;
            try
            {
                DataSet ds = null;
                strUpdFileName = objWF.ExecuteScaler(strSQL);
                return strUpdFileName;
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

        #region " Fetch Max Ref No."
        public string FetchRefNo(string strRFQNo)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT NVL(MAX(T.CONTRACT_NO),0) FROM CONT_TRANS_FCL_TBL T " + "WHERE T.CONTRACT_NO LIKE '" + strRFQNo + "/%' " + "ORDER BY T.CONTRACT_NO";
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Get Contract Date"
        public System.DateTime GetContractDt(string ContractPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT CONT.CONTRACT_DATE ");
            sb.Append("    FROM CONT_MAIN_TRANS_TBL CONT");
            sb.Append("   WHERE CONT.CONT_MAIN_TRANS_PK = " + ContractPK);
            try
            {
                return Convert.ToDateTime(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }
        #endregion
    }
}
