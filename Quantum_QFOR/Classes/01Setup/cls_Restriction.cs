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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_Restriction : CommonFeatures
    {
        Int32 m_last;
        Int32 m_start;
        DataSet DSMain = new DataSet();
        //Fetch All Records for a particular Operator and Set Heirarchical Data Grid
        #region "Private Variables"
        #endregion
        private long _PkValue;

        #region "Property"
        public long RestrictionPk
        {
            get { return _PkValue; }
        }
        #endregion

        #region "Fetch Function"
        public string FetchAll(string RestrictionRefNo = "", string RestrictionDate = "", int Restrictionfk = 0, Int32 Commoditypk = 0, string RestrictionMessage = "", string IMDGCode = "", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        short Active = 1, string SortType = " ASC ", short IntBizType = 1, Int32 flag = 0, Int16 RestrictType = 0, Int16 RestrictAs = 0)
        {

            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string SearchOption = (SearchType == "C" ? "%" : "");
            try
            {
                strCondition = " FROM RESTRICTION_MST_TBL RES ";
                strCondition += " WHERE 1=1  ";
                //Condition for Restriction Int32
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (RestrictionRefNo.Length > 0)
                {
                    strCondition += "AND UPPER(RES.RESTRICTION_REF_NO) like '" + SearchOption + RestrictionRefNo.ToUpper().Replace("'", "''") + "%' ";
                }
                //Condition for Active
                if (Active == 1)
                {
                    strCondition += " AND RES.ACTIVE = " + Active + " ";
                }

                //Condition for Message
                if (RestrictionMessage.Length > 0)
                {
                    strCondition += "AND UPPER(RES.RESTRICTION_MESSAGE) like '" + SearchOption + RestrictionMessage.ToUpper().Replace("'", "''") + "%' ";
                }

                //Condition for Restriction Date
                if (RestrictionDate.Length > 0)
                {
                    strCondition += " AND RES.RESTRICTION_DT = TO_DATE('" + RestrictionDate + "' , dateFormat)" ;
                }
                //Condition for Biz Type
                if (IntBizType != 0)
                {
                    strCondition += "AND RES.BUSINESS_TYPE = " + IntBizType + "  ";
                }

                if (RestrictType > 0)
                {
                    strCondition += "AND RES.RESTRICTION_TYPE = " + RestrictType + "  ";
                }

                if (RestrictAs > 0)
                {
                    strCondition += "AND RES.RESTRICT_AS = " + RestrictAs + "  ";
                }
                //Pagination Part
                strSQL = "SELECT count(*) ";
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
                m_last = last;
                m_start = start;
                //***************************Fetch Part************************
                strSQL = " SELECT * from (";
                strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
                strSQL += " (SELECT RES.RESTRICTION_PK,";
                strSQL += "  RES.ACTIVE,";
                strSQL += "  RES.RESTRICTION_REF_NO,";
                strSQL += "  RES.RESTRICTION_DT RESTRICTION_DT1,";
                strSQL += " nvl((SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFOR3012' AND DD.DD_VALUE = RES.BUSINESS_TYPE),'Both') BIZ_TYPE,";
                strSQL += " (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'RES_TYPE' AND DD.CONFIG_ID = 'QFOR3012' AND DD.DD_VALUE = RES.RESTRICTION_TYPE) TYPE,";
                strSQL += "  RES.RESTRICTION_MESSAGE,";
                strSQL += " (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'RES_AS' AND DD.CONFIG_ID = 'QFOR3012' AND DD.DD_VALUE = RES.RESTRICT_AS) RESTRICT_AS,";
                strSQL += "  RES.VALID_FROM VALID_FROM,";
                strSQL += "  RES.VALID_TO VALID_TO,";
                strSQL += "  RES.BUSINESS_TYPE";
                strSQL += strCondition;
                strSQL += " ORDER BY RES.RESTRICTION_DT desc ) q  ) ";
                //strSQL += " WHERE SR_NO  Between " + start + " and " + last;
                DSMain = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(DSMain, Formatting.Indented);
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

        #region "Fetch Reference No's"
        public DataTable GetAllRefNos(bool Active = true)
        {
            WorkFlow objWF = new WorkFlow();
            if (Active)
            {
                return objWF.GetDataTable("SELECT * FROM RESTRICTION_MST_TBL WHERE ACTIVE=1 ORDER BY RESTRICTION_REF_NO");
            }
            else
            {
                return objWF.GetDataTable("SELECT * FROM RESTRICTION_MST_TBL ORDER BY RESTRICTION_REF_NO");
            }
        }
        #endregion

        #region "Enhance Search Function"
        public string FetchCommodity(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COMMODITY_PKG.GETCOMMODITY_COMMON";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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
        #endregion

        #region "Update"
        public ArrayList UpdateMainRecord(DataSet dsPort, Int32 RestrictionPk, bool isUpdate, string RestrictionRefNo, string RestrictionDate, short RestrictionType, Int32 RestrictionValid, Int32 FromCountryFk, Int32 ToCountryFk, string EffectiveFromDate,
        string EffectiveToDate, Int32 Commodityfk, short BizType, short Hazardous, string ImdgClassCode, decimal WeightLimit, string RestrictionMessage, short intActive, Int32 VersionNo)
        {
            string retVal = null;
            Int32 RecAfct = default(Int32);
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            string Update_Proc = null;
            string UserName = objWK.MyUserName;
            Update_Proc = UserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_MST_TBL_UPD";
            bool UpdActflag = false;
            //Changes to Make Datarows updated Each time to check the Validity
            int i = 0;
            int tempPolPk = 0;
            int temppodPk = 0;

            if (dsPort.Tables[0].Rows.Count > 0)
            {

                if ((!object.ReferenceEquals(dsPort.Tables[0].Rows[0]["PORT_MST_POL_FK"], DBNull.Value)))
                {
                    tempPolPk = Convert.ToInt32(dsPort.Tables[0].Rows[0]["PORT_MST_POL_FK"]);
                    if (Convert.ToInt32(dsPort.Tables[0].Rows[0]["PORT_MST_POL_FK"]) > 1)
                    {
                        dsPort.Tables[0].Rows[0]["PORT_MST_POL_FK"] = 100;
                    }
                }
                if ((!object.ReferenceEquals(dsPort.Tables[0].Rows[0]["PORT_MST_POD_FK"], DBNull.Value)))
                {
                    temppodPk = Convert.ToInt32(dsPort.Tables[0].Rows[0]["PORT_MST_POD_FK"]);
                    if (Convert.ToInt32(dsPort.Tables[0].Rows[0]["PORT_MST_POD_FK"]) > 1)
                    {
                        dsPort.Tables[0].Rows[0]["PORT_MST_POD_FK"] = 200;
                    }
                }
            }

            //End

            try
            {
                var _with2 = objWK.MyCommand;
                _with2.Connection = objWK.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = Update_Proc;
                _with2.Transaction = TRAN;
                if (RestrictionType == 2)
                {
                    Commodityfk = 0;
                }
                _with2.Parameters.Add("RESTRICTION_PK_IN", RestrictionPk).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RESTRICTION_REF_NO_IN", RestrictionRefNo).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RESTRICTION_DT_IN", RestrictionDate).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RESTRICTION_TYPE_IN", RestrictionType).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RESTRICTION_ALERTBLOCK_IN", RestrictionValid).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("FROM_COUNTRY_IN", (FromCountryFk == 0 ? 0 : FromCountryFk)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("TO_COUNTRY_IN", (ToCountryFk == 0 ? 0 : ToCountryFk)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("VALID_FROM_IN", EffectiveFromDate).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("VALID_TO_IN", (string.IsNullOrEmpty(EffectiveToDate) ? "" : EffectiveToDate)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("COMMODITY_MST_FK_IN", (Commodityfk == 0 ? 0 : Commodityfk)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("HAZARDOUS_IN", Hazardous).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("IMDG_CLASS_CODE_IN", (string.IsNullOrEmpty(ImdgClassCode) ? "" : ImdgClassCode)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("WEIGHT_LIMIT_IN_KG_IN", WeightLimit).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RESTRICTION_MESSAGE_IN", (string.IsNullOrEmpty(RestrictionMessage) ? "" : RestrictionMessage)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("VERSION_NO_IN", VersionNo).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("ACTIVE_IN", intActive).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with2.ExecuteNonQuery();
                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "restriction")>0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    TRAN.Rollback();
                    return arrMessage;
                }
                else if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "saved")>0)
                {
                    //Changes to Make Datarows updated Each time to check the Validity
                    int j = 0;
                    if (dsPort.Tables[0].Rows.Count > 0)
                    {
                        dsPort.Tables[0].Rows[0]["PORT_MST_POL_FK"] = (tempPolPk == 0 ? 0 : tempPolPk);
                        dsPort.Tables[0].Rows[0]["PORT_MST_POD_FK"] = (temppodPk == 0 ? 0 : temppodPk);
                    }
                    //End
                    arrMessage = SaveChild(dsPort, RestrictionPk, Commodityfk, EffectiveFromDate, EffectiveToDate, RestrictionType, RestrictionValid, objWK, BizType);
                }
                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved")>0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }
        #endregion

        #region "Save Master Record"
        public ArrayList SaveRecord(DataSet dsPort, bool isUpdate, string RestrictionPk = " ", string RestrictionRefNo = " ", string RestrictionDate = " ", Int32 RestrictionType = 0, Int32 RestrictionValid = 0, short FromCountryFk = 0, short ToCountryFk = 0, string EffectiveFromDate = " ",
        string EffectiveToDate = "", Int32 Commodityfk = 0, short BizType = 0, short Hazardous = 0, string ImdgClassCode = " ", Int32 WeightLimit = 0, string RestrictionMessage = " ", short intActive = 1, string txtRestrictionRefNo = "", long nLocationId = 0,
        long nEmpId = 0, string strPolPks = "", string strPodPks = "")
        {
            Int32 retVal = default(Int32);
            Int32 RecAfct = default(Int32);
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            string Insert_Proc = null;
            string UserName = objWK.MyUserName;
            objWK.MyCommand.Transaction = TRAN;
            Insert_Proc = UserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_MST_TBL_INS";
            try
            {
                //Protocol Key

                if (string.IsNullOrEmpty(txtRestrictionRefNo))
                {
                    RestrictionRefNo = GenerateRestrictionNo(nLocationId, nEmpId, Convert.ToInt32(M_CREATED_BY_FK), BizType, objWK);
                    if (RestrictionRefNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                    }
                }
                else
                {
                    RestrictionRefNo = txtRestrictionRefNo;
                }
                // INSERT COMMAND ***********************************************************************
                var _with3 = objWK.MyCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = Insert_Proc;
                _with3.Transaction = TRAN;
                _with3.Parameters.Clear();
                _with3.Parameters.Add("RESTRICTION_REF_NO_IN", RestrictionRefNo).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("RESTRICTION_DT_IN", RestrictionDate).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("RESTRICTION_TYPE_IN", RestrictionType).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("RESTRICTION_ALERTBLOCK_IN", RestrictionValid).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("FROM_COUNTRY_IN", (FromCountryFk == 0 ? 0 : FromCountryFk)).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("TO_COUNTRY_IN", (ToCountryFk == 0 ? 0 : ToCountryFk)).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("VALID_FROM_IN", EffectiveFromDate).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("VALID_TO_IN", (string.IsNullOrEmpty(EffectiveToDate) ? "" : EffectiveToDate)).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("COMMODITY_MST_FK_IN", (Commodityfk == 0 ? 0 : Commodityfk)).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("HAZARDOUS_IN", Hazardous).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("IMDG_CLASS_CODE_IN", (string.IsNullOrEmpty(ImdgClassCode) ? "" : ImdgClassCode)).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("WEIGHT_LIMIT_IN_KG_IN", WeightLimit).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("RESTRICTION_MESSAGE_IN", (string.IsNullOrEmpty(RestrictionMessage) ? "": RestrictionMessage)).Direction = ParameterDirection.Input;
                //.Parameters.Add("POLPK_IN", IIf(strPolPks = "", DBNull.Value, strPolPks)).Direction = ParameterDirection.Input
                //.Parameters.Add("PODPK_IN", IIf(strPodPks = "", DBNull.Value, strPodPks)).Direction = ParameterDirection.Input
                _with3.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("ACTIVE_IN", intActive).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 250, "RETURN VALUE").Direction = ParameterDirection.Output;
                _with3.ExecuteNonQuery();
                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "restriction")>0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                arrMessage = SaveChild(dsPort, Convert.ToInt32(_PkValue), Commodityfk, EffectiveFromDate, EffectiveToDate, Convert.ToInt16(RestrictionType), RestrictionValid, objWK, BizType);

                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved")>0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        txtRestrictionRefNo = RestrictionRefNo;
                        return arrMessage;
                    }
                    else if (string.Compare(arrMessage[0].ToString(), "restriction")>0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                //added by surya prasad for protocol roll back
                if (BizType == 1)
                {
                    RollbackProtocolKey("AIR RESTRICTION", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RestrictionRefNo, DateTime.Now);
                }
                else
                {
                    RollbackProtocolKey("SEA RESTRICTION", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RestrictionRefNo, DateTime.Now);
                }
                //end
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                //added by surya prasad for protocol roll back
                if (BizType == 1)
                {
                    RollbackProtocolKey("AIR RESTRICTION", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RestrictionRefNo, DateTime.Now);
                }
                else
                {
                    RollbackProtocolKey("SEA RESTRICTION", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RestrictionRefNo, DateTime.Now);
                }
                //end
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }
        #endregion

        #region "Child Save - Added Ports"
        public ArrayList SaveChild(DataSet PortDataSet, int Restrictionpk, int Commodityfk = 0, string ValidFrom = "", string ValidTo = "", short RestrictionType = 0, Int32 RestrictionValid = 0, WorkFlow objwk = null, short BizType = 1)
        {
            string retVal = null;
            Int32 RecAfct = default(Int32);
            // If objwk Is Nothing Then
            //    objwk = New WorkFlow
            //    objwk.OpenConnection()
            // End If
            // objwk.OpenConnection()
            // Dim TRAN As OracleTransaction
            // TRAN = objwk.MyConnection.BeginTransaction()
            string Insert_Proc = null;
            string Update_Proc = null;
            string UserName = objwk.MyUserName;
            Insert_Proc = UserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_TRN_TBL_INS";
            Update_Proc = UserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_TRN_TBL_UPD";
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            objwk.MyCommand.Parameters.Clear();
            try
            {
                var _with4 = insCommand;
                _with4.Connection = objwk.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = Insert_Proc;
                _with4.Parameters.Add("RESTRICTION_FK_IN", Restrictionpk).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("POLFK_IN", OracleDbType.Int32, 10, "PORT_MST_POL_FK").Direction = ParameterDirection.Input;
                _with4.Parameters.Add("PODFK_IN", OracleDbType.Int32, 10, "PORT_MST_POD_FK").Direction = ParameterDirection.Input;
                _with4.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
                _with4.Parameters.Add("VALID_FROM_IN", ValidFrom).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("VALID_TO_IN", (string.IsNullOrEmpty(ValidTo) ? "" : ValidTo)).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("RESTRICTION_TYPE_IN", RestrictionType).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("COMMODITY_IN", (Commodityfk == 0 ? 0 : Commodityfk)).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 250, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with5 = updCommand;
                _with5.Connection = objwk.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = Update_Proc;
                _with5.Parameters.Add("RESTRICTION_FK_IN", Restrictionpk).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("POLFK_IN", OracleDbType.Int32, 10, "PORT_MST_POL_FK").Direction = ParameterDirection.Input;
                _with5.Parameters.Add("PODFK_IN", OracleDbType.Int32, 10, "PORT_MST_POD_FK").Direction = ParameterDirection.Input;
                _with5.Parameters.Add("VALID_FROM_IN", ValidFrom).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("VALID_TO_IN", (string.IsNullOrEmpty(ValidTo) ? "" : ValidTo)).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("RESTRICTION_TYPE_IN", RestrictionType).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("COMMODITY_IN", (Commodityfk == 0 ? 0 : Commodityfk)).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
                _with5.Parameters.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 250, "RETURN_VALUE").Direction = ParameterDirection.Output;
                // Calling Overrided function Implemented in the same class
                var _with6 = objwk.MyDataAdapter;
                _with6.InsertCommand = insCommand;
                _with6.InsertCommand.Transaction = objwk.MyCommand.Transaction;
                _with6.UpdateCommand = updCommand;
                _with6.UpdateCommand.Transaction = objwk.MyCommand.Transaction;
                RecAfct = _with6.Update(PortDataSet);
                string strReturn = null;
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("saved");
                }

                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch for Detail Form"
        public DataSet FetchHeader(int RestrictionPK)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.OpenConnection();
                var _with7 = objWF.MyCommand.Parameters;
                _with7.Clear();
                _with7.Add("RESTRICTION_PK_IN", RestrictionPK).Direction = ParameterDirection.Input;
                _with7.Add("RES_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_RESTRICTION_PKG", "FETCH_RESTRICTION_HDR");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }
        #endregion

        #region "Fetch Grid Information"
        public DataSet FetchDetails(int RestrictionPK)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.OpenConnection();
                var _with8 = objWF.MyCommand.Parameters;
                _with8.Clear();
                _with8.Add("RESTRICTION_PK_IN", RestrictionPK).Direction = ParameterDirection.Input;
                _with8.Add("RES_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_RESTRICTION_PKG", "FETCH_RESTRICTION_TRN");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        #endregion

        #region "Fetch Container Details"
        public DataSet FetchCntrDetails(int RestrictionTRNPK)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.OpenConnection();
                var _with9 = objWF.MyCommand.Parameters;
                _with9.Clear();
                _with9.Add("RESTRICTION_TRN_PK_IN", RestrictionTRNPK).Direction = ParameterDirection.Input;
                _with9.Add("RES_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_RESTRICTION_PKG", "FETCH_RESTRICTION_CNTR");
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion

        #region "Generating Protocol No"
        public string GenerateRestrictionNo(Int64 ILocationId, Int64 IEmployeeId, int createdBy, int BizType, WorkFlow objWK = null)
        {
            string functionReturnValue = null;
            try
            {
                if (BizType == 1)
                {
                    functionReturnValue = GenerateProtocolKey("AIR RESTRICTION", ILocationId, IEmployeeId, DateTime.Now, "", "","" , 3, objWK);
                }
                else
                {
                    functionReturnValue = GenerateProtocolKey("SEA RESTRICTION", ILocationId, IEmployeeId, DateTime.Now, "","" ,"" , 3, objWK);
                }
                return functionReturnValue;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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
        #endregion

        #region "Get Commodity Id "
        public string ResolveCommodityfk(Int32 Commodityfk = 0)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select COMMODITY_ID from COMMODITY_MST_TBL where COMMODITY_MST_PK = " + Commodityfk + " ";
                string ComName = null;
                ComName = objWF.ExecuteScaler(strSQL);
                return ComName;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #region "Get and Commodity Name"
        public string ResolveCommodityfkN(Int32 Commodityfk = 0)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select COMMODITY_NAME from COMMODITY_MST_TBL where COMMODITY_MST_PK = " + Commodityfk + " ";
                string ComName = null;
                ComName = objWF.ExecuteScaler(strSQL);
                return ComName;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #region "Get Country Id "
        public string ResolveCountryfk(Int32 countryfk = 0)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select COUNTRY_ID from COUNTRY_MST_TBL where COUNTRY_MST_PK = " + countryfk + " ";
                string ComName = null;
                ComName = objWF.ExecuteScaler(strSQL);
                return ComName;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #region "Get Country Name"
        public string ResolveCountryfkN(Int32 Countryfk = 0)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select COUNTRY_NAME from COUNTRY_MST_TBL where COUNTRY_MST_PK = " + Countryfk + " ";
                string ComName = null;
                ComName = objWF.ExecuteScaler(strSQL);
                return ComName;
                //Manjunath  PTS ID:Sep-02  15/09/2011
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

        #region "Save Restriction Details"
        public ArrayList Save(DataSet HeaderDataSet, DataSet GridDs, DataSet CntrDS = null, DataSet AnnDS = null)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            int intPKVal = 0;
            long lngI = 0;
            long lngPK = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            bool chkflag = false;

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                var _with10 = insCommand;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_MST_TBL_INS";
                var _with11 = _with10.Parameters;
                insCommand.Parameters.Add("RESTRICTION_REF_NO_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_REF_NO"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RESTRICT_AS_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICT_AS"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("BUSINESS_TYPE_IN", HeaderDataSet.Tables[0].Rows[0]["BUSINESS_TYPE"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RESTRICTION_DT_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_DT"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RESTRICTION_TYPE_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_TYPE"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("VALID_FROM_IN", HeaderDataSet.Tables[0].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("VALID_TO_IN", HeaderDataSet.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RESTRICT_BY_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_BY_FK"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RESTRICTION_MESSAGE_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_MESSAGE"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("ACTIVE_IN", HeaderDataSet.Tables[0].Rows[0]["ACTIVE"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("POST_ANN_IN", HeaderDataSet.Tables[0].Rows[0]["POST_ANN"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CHK_INT_IN", HeaderDataSet.Tables[0].Rows[0]["CHK_INTERNAL"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CHK_EXT_IN", HeaderDataSet.Tables[0].Rows[0]["CHK_EXTERNAL"]).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



                var _with12 = updCommand;
                _with12.Connection = objWK.MyConnection;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_MST_TBL_UPD";
                var _with13 = _with12.Parameters;
                updCommand.Parameters.Add("RESTRICTION_PK_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_PK"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RESTRICTION_REF_NO_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_REF_NO"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RESTRICT_AS_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICT_AS"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("BUSINESS_TYPE_IN", HeaderDataSet.Tables[0].Rows[0]["BUSINESS_TYPE"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RESTRICTION_DT_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_DT"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RESTRICTION_TYPE_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_TYPE"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VALID_FROM_IN", HeaderDataSet.Tables[0].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VALID_TO_IN", HeaderDataSet.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RESTRICT_BY_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_BY_FK"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RESTRICTION_MESSAGE_IN", HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_MESSAGE"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("ACTIVE_IN", HeaderDataSet.Tables[0].Rows[0]["ACTIVE"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("POST_ANN_IN", HeaderDataSet.Tables[0].Rows[0]["POST_ANN"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CHK_INT_IN", HeaderDataSet.Tables[0].Rows[0]["CHK_INTERNAL"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CHK_EXT_IN", HeaderDataSet.Tables[0].Rows[0]["CHK_EXTERNAL"]).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                
                var _with14 = objWK.MyDataAdapter;
                if (Convert.ToInt32(HeaderDataSet.Tables[0].Rows[0]["RESTRICTION_PK"]) == 0)
                {
                    _with14.InsertCommand = insCommand;
                    _with14.InsertCommand.Transaction = TRAN;
                    RecAfct = _with14.InsertCommand.ExecuteNonQuery();
                    intPKVal = Convert.ToInt32(objWK.MyDataAdapter.InsertCommand.Parameters["RETURN_VALUE"].Value);
                }
                else
                {
                    _with14.UpdateCommand = updCommand;
                    _with14.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with14.UpdateCommand.ExecuteNonQuery();
                    intPKVal = Convert.ToInt32(objWK.MyDataAdapter.UpdateCommand.Parameters["RETURN_VALUE"].Value);
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                }
                else
                {
                    if (SaveGrid(GridDs, CntrDS, intPKVal, TRAN) == 1 & SaveCntrDetails(CntrDS, TRAN, intPKVal) == 1 & SaveAnnDtls(AnnDS, TRAN, intPKVal) == 1)
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        arrMessage.Add(intPKVal);
                        if (Convert.ToInt32(HeaderDataSet.Tables[0].Rows[0]["POST_ANN"]) == 1)
                        {
                            InsertAnnouncementDtls(Convert.ToInt16(HeaderDataSet.Tables[0].Rows[0]["CHK_INTERNAL"]), Convert.ToInt16(HeaderDataSet.Tables[0].Rows[0]["CHK_EXTERNAL"]), intPKVal);
                        }
                    }
                    else
                    {
                        TRAN.Rollback();
                    }
                }
                return arrMessage;
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
        #endregion

        #region "Save Grid Info"
        public int SaveGrid(DataSet GridDs, DataSet M_DataSet, long intPKVal, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            Int32 rowCnt = default(Int32);
            Int32 RecAfct = default(Int32);
            int TRNPK = 0;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            string DelPKs = "0";
            string strQry = null;
            objWK.MyConnection = TRAN.Connection;
            try
            {
                //To delete the Details which has been removed from the Grid.
                for (rowCnt = 0; rowCnt <= GridDs.Tables[0].Rows.Count - 1; rowCnt++)
                {
                    if (!string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["RESTRICTION_TRN_PK"].ToString()))
                    {
                        if (Convert.ToInt32(GridDs.Tables[0].Rows[rowCnt]["RESTRICTION_TRN_PK"]) > 0)
                        {
                            DelPKs = DelPKs + "," + Convert.ToInt32(GridDs.Tables[0].Rows[rowCnt]["RESTRICTION_TRN_PK"]);
                        }
                    }
                }

                strQry = "DELETE FROM RESTRICTION_TRN_TBL RTT WHERE";
                strQry += " RTT.RESTRICTION_MST_FK =" + intPKVal;
                strQry += " AND RTT.RESTRICTION_TRN_PK NOT IN (" + DelPKs + ")";

                var _with15 = delCommand;
                _with15.Connection = objWK.MyConnection;
                _with15.Transaction = TRAN;
                _with15.CommandType = CommandType.Text;
                _with15.CommandText = strQry;
                _with15.ExecuteNonQuery();
                //******************************************************************
                for (rowCnt = 0; rowCnt <= GridDs.Tables[0].Rows.Count - 1; rowCnt++)
                {
                    var _with16 = insCommand;
                    _with16.Connection = objWK.MyConnection;
                    _with16.CommandType = CommandType.StoredProcedure;
                    _with16.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_TRN_TBL_INS";
                    _with16.Parameters.Clear();

                    _with16.Parameters.Add("RESTRICTION_FK_IN", intPKVal).Direction = ParameterDirection.Input;
                    _with16.Parameters["RESTRICTION_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("SHIPPING_LINE_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["SHIPPINGLINE_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["SHIPPINGLINE_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["SHIPPING_LINE_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["CUSTOMER_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["CUSTOMER_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("SHIPPER_MST_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["SHIPPER_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["SHIPPER_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["SHIPPER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("CONSIGNEE_MST_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["CONSIGNEE_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["CONSIGNEE_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["CONSIGNEE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("NOTIFY_MST_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["NOTIFY_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["NOTIFY_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["NOTIFY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("FRM_COUNTRY_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["FRMCTY_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["FRMCTY_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["FRM_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("POO_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["POO_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["POO_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["POO_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("POL_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["POL_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["POL_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("TO_COUNTRY_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["TOCTY_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["TOCTY_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["TO_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("POD_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["POD_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["POD_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PFD_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["PFD_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["PFD_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["PFD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("INCO_TERMS_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["INCOTERMS_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["INCOTERMS_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["INCO_TERMS_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("COMM_GRP_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["COMMGRP_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["COMMGRP_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["COMM_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("COMMODITY_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["COMM_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["COMM_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["COMMODITY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["WEIGHT"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["WEIGHT"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("VOLUME_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["VOLUME"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["VOLUME"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with16.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with17 = updCommand;
                    _with17.Connection = objWK.MyConnection;
                    _with17.CommandType = CommandType.StoredProcedure;
                    _with17.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_TRN_TBL_UPD";
                    _with17.Parameters.Clear();

                    _with17.Parameters.Add("RESTRICTION_TRN_PK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["RESTRICTION_TRN_PK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["RESTRICTION_TRN_PK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["RESTRICTION_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("RESTRICTION_FK_IN", intPKVal).Direction = ParameterDirection.Input;
                    _with17.Parameters["RESTRICTION_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("SHIPPING_LINE_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["SHIPPINGLINE_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["SHIPPINGLINE_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["SHIPPING_LINE_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["CUSTOMER_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["CUSTOMER_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("SHIPPER_MST_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["SHIPPER_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["SHIPPER_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["SHIPPER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("CONSIGNEE_MST_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["CONSIGNEE_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["CONSIGNEE_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["CONSIGNEE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("NOTIFY_MST_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["NOTIFY_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["NOTIFY_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["NOTIFY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("FRM_COUNTRY_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["FRMCTY_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["FRMCTY_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["FRM_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("POO_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["POO_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["POO_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["POO_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("POL_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["POL_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["POL_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("TO_COUNTRY_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["TOCTY_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["TOCTY_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["TO_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("POD_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["POD_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["POD_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("PFD_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["PFD_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["PFD_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["PFD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("INCO_TERMS_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["INCOTERMS_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["INCOTERMS_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["INCO_TERMS_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("COMM_GRP_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["COMMGRP_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["COMMGRP_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["COMM_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("COMMODITY_FK_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["COMM_FK"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["COMM_FK"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["COMMODITY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["WEIGHT"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["WEIGHT"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("VOLUME_IN", (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["VOLUME"].ToString()) ? DBNull.Value : GridDs.Tables[0].Rows[rowCnt]["VOLUME"])).Direction = ParameterDirection.Input;
                    _with17.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with17.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    

                    var _with18 = objWK.MyDataAdapter;
                    if (string.IsNullOrEmpty(GridDs.Tables[0].Rows[rowCnt]["RESTRICTION_TRN_PK"].ToString()))
                    {
                        _with18.InsertCommand = insCommand;
                        _with18.InsertCommand.Transaction = TRAN;
                        RecAfct = _with18.InsertCommand.ExecuteNonQuery();
                        TRNPK = Convert.ToInt32(objWK.MyDataAdapter.InsertCommand.Parameters["RETURN_VALUE"].Value);
                    }
                    else
                    {
                        _with18.UpdateCommand = updCommand;
                        _with18.UpdateCommand.Transaction = TRAN;
                        RecAfct = _with18.UpdateCommand.ExecuteNonQuery();
                        TRNPK = Convert.ToInt32(objWK.MyDataAdapter.UpdateCommand.Parameters["RETURN_VALUE"].Value);
                    }
                    if ((M_DataSet != null))
                    {
                        for (int j = 0; j <= M_DataSet.Tables.Count - 1; j++)
                        {
                            if (M_DataSet.Tables[j].TableName == "CNT" + Convert.ToInt32(GridDs.Tables[0].Rows[rowCnt]["SLNR"]))
                            {
                                for (int i = 0; i <= M_DataSet.Tables[j].Rows.Count - 1; i++)
                                {
                                    if (string.IsNullOrEmpty(M_DataSet.Tables[j].Rows[i]["RESTRICT_TRN_FK"].ToString()))
                                    {
                                        M_DataSet.Tables[j].Rows[i]["RESTRICT_TRN_FK"] = TRNPK;
                                    }
                                    else
                                    {
                                        if (Convert.ToInt32(M_DataSet.Tables[j].Rows[i]["RESTRICT_TRN_FK"]) == 0)
                                        {
                                            M_DataSet.Tables[j].Rows[i]["RESTRICT_TRN_FK"] = TRNPK;
                                        }
                                    }
                                }
                                M_DataSet.Tables[j].AcceptChanges();
                            }
                        }
                    }
                }

                if (arrMessage.Count > 0)
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return 0;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return 0;
            }
        }
        #endregion

        #region "Save Container Details"
        public int SaveCntrDetails(DataSet M_DataSet, OracleTransaction TRAN, Int32 RestrictPK)
        {
            WorkFlow objWK = new WorkFlow();
            WorkFlow objWF = new WorkFlow();
            objWK.MyConnection = TRAN.Connection;
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            string PK_TODel = "0";
            arrMessage.Clear();

            try
            {
                int i = 0;
                int j = 0;
                if ((M_DataSet != null))
                {
                    for (j = 0; j <= M_DataSet.Tables.Count - 1; j++)
                    {
                        for (i = 0; i <= M_DataSet.Tables[j].Rows.Count - 1; i++)
                        {
                            if (M_DataSet.Tables[j].Rows[i]["SEL"] == "0" | M_DataSet.Tables[j].Rows[i]["SEL"] == "false")
                            {
                                if (Convert.ToInt32(M_DataSet.Tables[j].Rows[i]["RESTRICT_CNTR_PK"]) > 0)
                                {
                                    PK_TODel = PK_TODel + "," + M_DataSet.Tables[j].Rows[i]["RESTRICT_CNTR_PK"];
                                }
                            }
                        }
                    }
                }
                objWF.ExecuteCommands("DELETE FROM RESTRICTION_CNTR_DTL RCT WHERE RCT.RESTRICTION_TRN_FK IN (SELECT RTT.RESTRICTION_TRN_PK FROM RESTRICTION_TRN_TBL RTT WHERE RTT.RESTRICTION_MST_FK =" + RestrictPK + ") AND RCT.RESTRICTION_CNTR_DTL_PK IN (" + PK_TODel + ")");

                if ((M_DataSet != null))
                {
                    for (j = 0; j <= M_DataSet.Tables.Count - 1; j++)
                    {
                        for (i = 0; i <= M_DataSet.Tables[j].Rows.Count - 1; i++)
                        {
                            if (M_DataSet.Tables[j].Rows[i]["SEL"] == "1" | M_DataSet.Tables[j].Rows[i]["SEL"] == "true")
                            {
                                var _with19 = insCommand;
                                _with19.Connection = objWK.MyConnection;
                                _with19.CommandType = CommandType.StoredProcedure;
                                _with19.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_CNTR_TBL_INS";
                                _with19.Parameters.Clear();
                                var _with20 = _with19.Parameters;
                                insCommand.Parameters.Add("RESTRICTION_TRN_FK_IN", M_DataSet.Tables[j].Rows[i]["RESTRICT_TRN_FK"]).Direction = ParameterDirection.Input;
                                insCommand.Parameters.Add("CONTAINER_MST_FK_IN", M_DataSet.Tables[j].Rows[i]["CONTAINER_FK"]).Direction = ParameterDirection.Input;
                                insCommand.Parameters.Add("QTY_IN", M_DataSet.Tables[j].Rows[i]["QTY"]).Direction = ParameterDirection.Input;
                                insCommand.Parameters.Add("REMARKS_IN", M_DataSet.Tables[j].Rows[i]["REMARKS"]).Direction = ParameterDirection.Input;
                                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                                var _with21 = updCommand;
                                _with21.Connection = objWK.MyConnection;
                                _with21.CommandType = CommandType.StoredProcedure;
                                _with21.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_CNTR_TBL_UPD";
                                _with21.Parameters.Clear();
                                var _with22 = _with21.Parameters;

                                updCommand.Parameters.Add("RESTRICTION_CNTR_DTL_PK_IN", M_DataSet.Tables[j].Rows[i]["RESTRICT_CNTR_PK"]).Direction = ParameterDirection.Input;
                                updCommand.Parameters.Add("RESTRICTION_TRN_FK_IN", M_DataSet.Tables[j].Rows[i]["RESTRICT_TRN_FK"]).Direction = ParameterDirection.Input;
                                updCommand.Parameters.Add("CONTAINER_MST_FK_IN", M_DataSet.Tables[j].Rows[i]["CONTAINER_FK"]).Direction = ParameterDirection.Input;
                                updCommand.Parameters.Add("QTY_IN", M_DataSet.Tables[j].Rows[i]["QTY"]).Direction = ParameterDirection.Input;
                                updCommand.Parameters.Add("REMARKS_IN", M_DataSet.Tables[j].Rows[i]["REMARKS"]).Direction = ParameterDirection.Input;
                                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                if (Convert.ToInt32(M_DataSet.Tables[j].Rows[i]["RESTRICT_CNTR_PK"]) == 0)
                                {
                                    var _with23 = objWK.MyDataAdapter;
                                    _with23.InsertCommand = insCommand;
                                    _with23.InsertCommand.Transaction = TRAN;
                                    RecAfct = RecAfct + _with23.InsertCommand.ExecuteNonQuery();
                                }
                                else
                                {
                                    var _with24 = objWK.MyDataAdapter;
                                    _with24.UpdateCommand = updCommand;
                                    _with24.UpdateCommand.Transaction = TRAN;
                                    RecAfct = RecAfct + _with24.UpdateCommand.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                }

                if (arrMessage.Count > 0)
                {
                    return 0;
                }
                else
                {
                    return 1;
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
        }
        #endregion

        #region "Save Container Details"
        public int SaveAnnDtls(DataSet M_DataSet, OracleTransaction TRAN, Int32 RestrictPK)
        {
            WorkFlow objWK = new WorkFlow();
            WorkFlow objWF = new WorkFlow();
            objWK.MyConnection = TRAN.Connection;
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            arrMessage.Clear();
            try
            {
                if ((M_DataSet != null))
                {
                    for (int i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++)
                    {
                        if (Convert.ToBoolean(M_DataSet.Tables[0].Rows[i]["SEL"]) == true)
                        {
                            var _with25 = insCommand;
                            _with25.Connection = objWK.MyConnection;
                            _with25.CommandType = CommandType.StoredProcedure;
                            _with25.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_ANN_TBL_INS";
                            _with25.Parameters.Clear();
                            var _with26 = _with25.Parameters;
                            insCommand.Parameters.Add("RESTRICTION_MST_FK_IN", RestrictPK).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("LOCATION_MST_FK_IN", M_DataSet.Tables[0].Rows[i]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("TYPE_FK_IN", 0).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("PARTY_FK_IN", 0).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("BAND_IN", 0).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            var _with27 = objWK.MyDataAdapter;
                            _with27.InsertCommand = insCommand;
                            _with27.InsertCommand.Transaction = TRAN;
                            RecAfct = _with27.InsertCommand.ExecuteNonQuery();
                        }
                        else
                        {
                            var _with28 = delCommand;
                            _with28.Connection = objWK.MyConnection;
                            _with28.CommandType = CommandType.StoredProcedure;
                            _with28.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_ANN_TBL_DEL";
                            _with28.Parameters.Clear();
                            var _with29 = _with28.Parameters;
                            delCommand.Parameters.Add("RESTRICTION_MST_FK_IN", RestrictPK).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("LOCATION_MST_FK_IN", M_DataSet.Tables[0].Rows[i]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("TYPE_FK_IN", 0).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("PARTY_FK_IN", 0).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("BAND_IN", 0).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            var _with30 = objWK.MyDataAdapter;
                            _with30.DeleteCommand = delCommand;
                            _with30.DeleteCommand.Transaction = TRAN;
                            RecAfct = _with30.DeleteCommand.ExecuteNonQuery();
                        }
                    }
                    for (int i = 0; i <= M_DataSet.Tables[2].Rows.Count - 1; i++)
                    {
                        if (Convert.ToBoolean(M_DataSet.Tables[2].Rows[i]["SEL"]) == true)
                        {
                            var _with31 = insCommand;
                            _with31.Connection = objWK.MyConnection;
                            _with31.CommandType = CommandType.StoredProcedure;
                            _with31.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_ANN_TBL_INS";
                            _with31.Parameters.Clear();
                            var _with32 = _with31.Parameters;
                            insCommand.Parameters.Add("RESTRICTION_MST_FK_IN", RestrictPK).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("LOCATION_MST_FK_IN", M_DataSet.Tables[2].Rows[i]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("TYPE_FK_IN", M_DataSet.Tables[2].Rows[i]["TYPE_PK"]).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("PARTY_FK_IN", M_DataSet.Tables[2].Rows[i]["PARTY_PK"]).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("BAND_IN", 2).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                            insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            var _with33 = objWK.MyDataAdapter;
                            _with33.InsertCommand = insCommand;
                            _with33.InsertCommand.Transaction = TRAN;
                            RecAfct = _with33.InsertCommand.ExecuteNonQuery();
                        }
                        else
                        {
                            var _with34 = delCommand;
                            _with34.Connection = objWK.MyConnection;
                            _with34.CommandType = CommandType.StoredProcedure;
                            _with34.CommandText = objWK.MyUserName + ".RESTRICTION_MST_TBL_PKG.RESTRICTION_ANN_TBL_DEL";
                            _with34.Parameters.Clear();
                            var _with35 = _with34.Parameters;
                            delCommand.Parameters.Add("RESTRICTION_MST_FK_IN", RestrictPK).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("LOCATION_MST_FK_IN", M_DataSet.Tables[2].Rows[i]["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("TYPE_FK_IN", M_DataSet.Tables[2].Rows[i]["TYPE_PK"]).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("PARTY_FK_IN", M_DataSet.Tables[2].Rows[i]["PARTY_PK"]).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("BAND_IN", 2).Direction = ParameterDirection.Input;
                            delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                            var _with36 = objWK.MyDataAdapter;
                            _with36.DeleteCommand = delCommand;
                            _with36.DeleteCommand.Transaction = TRAN;
                            RecAfct = _with36.DeleteCommand.ExecuteNonQuery();
                        }
                    }
                }

                if (arrMessage.Count > 0)
                {
                    return 0;
                }
                else
                {
                    return 1;
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
        }
        #endregion

        #region " Fetch Restriction Approval "
        public string FetchRestrictionApproval(int RestrictionTypePK, int ReferencePK, int CustomerPK, Int16 Status, string RestrictionMsg, string S_C, string FromDate, string ToDate, Int32 CurrentPage, Int32 TotalPage,
        Int16 DataonLoad)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();


            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with37 = objWF.MyDataAdapter;
                _with37.SelectCommand = new OracleCommand();
                _with37.SelectCommand.Connection = objWF.MyConnection;
                _with37.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_RESTRICTION_PKG.FETCH_RESTRICTION_APPROVAL";
                _with37.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with37.SelectCommand.Parameters.Add("REFERENCE_TYPE_IN", RestrictionTypePK).Direction = ParameterDirection.Input;
                _with37.SelectCommand.Parameters.Add("REFERENCE_PK_IN", ReferencePK).Direction = ParameterDirection.Input;
                _with37.SelectCommand.Parameters.Add("CUSTOMER_MST_FK_IN", CustomerPK).Direction = ParameterDirection.Input;
                _with37.SelectCommand.Parameters.Add("STATUS_IN", Status).Direction = ParameterDirection.Input;
                _with37.SelectCommand.Parameters.Add("RESTRICTION_MSG_IN", (string.IsNullOrEmpty(RestrictionMsg) ? "" : RestrictionMsg.ToUpper())).Direction = ParameterDirection.Input;
                _with37.SelectCommand.Parameters.Add("S_C_IN", S_C).Direction = ParameterDirection.Input;
                _with37.SelectCommand.Parameters.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDate) ? "" : FromDate)).Direction = ParameterDirection.Input;
                _with37.SelectCommand.Parameters.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDate) ? "" : ToDate)).Direction = ParameterDirection.Input;
                _with37.SelectCommand.Parameters.Add("DATALOAD_IN", DataonLoad).Direction = ParameterDirection.Input;
                _with37.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with37.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = M_MasterPageSize;
                _with37.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with37.SelectCommand.Parameters.Add("RES_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with37.Fill(ds);
                TotalPage = Convert.ToInt32(TotalPage);
                CurrentPage = Convert.ToInt32(CurrentPage);
                return JsonConvert.SerializeObject(ds, Formatting.Indented);
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.CloseConnection();
            }
            return "";
        }
        #endregion
        #region "Get Navigate Information"
        public DataSet GetNavigateInfo(Int16 ReferenceType, int RefPK)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.OpenConnection();
                var _with38 = objWF.MyCommand.Parameters;
                _with38.Clear();
                _with38.Add("REFERENCE_TYPE_IN", ReferenceType).Direction = ParameterDirection.Input;
                _with38.Add("REFERENCE_PK_IN", RefPK).Direction = ParameterDirection.Input;
                _with38.Add("RES_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_RESTRICTION_PKG", "FETCH_NAVIGATE_INFO");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }
        #endregion
        #region "Get Navigate Information"
        public ArrayList UpdateRestrictionApproval(int ApprovalPK, int RestTRNPK, Int16 ReferenceType, int RefPK, string Status, string Remarks)
        {
            WorkFlow objWK = new WorkFlow();
            ArrayList arrMessage = new ArrayList();
            OracleTransaction TRAN = null;
            int intPKVal = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            string StrSql = null;
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                var _with39 = insCommand;
                _with39.Connection = objWK.MyConnection;
                _with39.CommandType = CommandType.StoredProcedure;
                _with39.CommandText = objWK.MyUserName + ".RESTRICTION_APPROVAL_TBL_PKG.RESTRICTION_APPROVAL_TBL_UPD";
                var _with40 = _with39.Parameters;
                insCommand.Parameters.Add("RESTRICTION_APPROVAL_PK_IN", ApprovalPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RESTRICTION_TRN_FK_IN", RestTRNPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("FORM_TYPE_IN", ReferenceType).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("REFERENCE_MST_FK_IN", RefPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("STATUS_FK_IN", Status).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("REMARKS_IN", Remarks).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with41 = objWK.MyDataAdapter;
                _with41.InsertCommand = insCommand;
                _with41.InsertCommand.Transaction = TRAN;
                RecAfct = _with41.InsertCommand.ExecuteNonQuery();
                intPKVal = Convert.ToInt32(objWK.MyDataAdapter.InsertCommand.Parameters["RETURN_VALUE"].Value);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                }
                return arrMessage;
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
        #endregion

        #region "Fetch Announcement Dtls"
        public DataSet FetchAnnDetails(int RestrictionPK, Int16 chkInt, Int16 chkExt)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = null;
            try
            {
                objWF.OpenConnection();
                var _with42 = objWF.MyCommand.Parameters;
                _with42.Clear();
                _with42.Add("RESTRICTION_PK_IN", RestrictionPK).Direction = ParameterDirection.Input;
                _with42.Add("INTERNAL_IN", chkInt).Direction = ParameterDirection.Input;
                _with42.Add("EXTERNAL_IN", chkExt).Direction = ParameterDirection.Input;
                _with42.Add("BAND0", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with42.Add("BAND1", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with42.Add("BAND2", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("FETCH_RESTRICTION_PKG", "FETCH_ANNOUNCEMENT_INFO");

                DataRelation drband1 = new DataRelation("BAND1", new DataColumn[] { ds.Tables[0].Columns["LOCATION_MST_FK"] }, new DataColumn[] { ds.Tables[1].Columns["LOCATION_MST_FK"] });
                drband1.Nested = true;
                ds.Relations.Add(drband1);
                DataRelation drband2 = new DataRelation("BAND2", new DataColumn[] {
                    ds.Tables[1].Columns["LOCATION_MST_FK"],
                    ds.Tables[1].Columns["TYPE_PK"]
                }, new DataColumn[] {
                    ds.Tables[2].Columns["LOCATION_MST_FK"],
                    ds.Tables[2].Columns["TYPE_PK"]
                });
                drband2.Nested = true;
                ds.Relations.Add(drband2);
                return ds;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion

        #region "Update the Announcement Dtls"
        public void InsertAnnouncementDtls(Int16 chkInt, Int16 chkExt, int RestrictPK)
        {
            WorkFlow objWF = new WorkFlow();
            cls_Announcement objAnn = new cls_Announcement();
            string SqlStr = null;
            string locationPKs = null;
            string UserPks = null;
            string CustomerPKs = null;
            string VendorPks = null;
            string AgentPks = null;
            int cnt = 0;
            int AnnPK = 0;
            DataSet dsMain = new DataSet();
            bool Result = false;

            SqlStr = "select rowtocol('select ract.location_fk from RESTRICT_ANN_CONFIG_TBL ract where ract.type_fk=0";
            SqlStr = SqlStr + " and ann_entry=0 and ract.restrict_hdr_fk=" + RestrictPK + " union";
            SqlStr = SqlStr + " select ract.location_fk from RESTRICT_ANN_CONFIG_TBL ract where ract.type_fk=1";
            SqlStr = SqlStr + " and ann_entry=0 and ract.restrict_hdr_fk=" + RestrictPK + "') from dual";
            locationPKs = objWF.ExecuteScaler(SqlStr);

            SqlStr = " select rowtocol('select ract.party_fk from RESTRICT_ANN_CONFIG_TBL ract where ract.type_fk=1";
            SqlStr = SqlStr + " and ann_entry=0 and ract.restrict_hdr_fk=" + RestrictPK + "') from dual";
            UserPks = objWF.ExecuteScaler(SqlStr);

            SqlStr = " select rowtocol('select ract.party_fk from RESTRICT_ANN_CONFIG_TBL ract where ract.type_fk=2";
            SqlStr = SqlStr + " and ann_entry=0 and ract.restrict_hdr_fk=" + RestrictPK + "') from dual";
            CustomerPKs = objWF.ExecuteScaler(SqlStr);

            SqlStr = " select rowtocol('select ract.party_fk from RESTRICT_ANN_CONFIG_TBL ract where ract.type_fk=3";
            SqlStr = SqlStr + " and ann_entry=0 and ract.restrict_hdr_fk=" + RestrictPK + "') from dual";
            VendorPks = objWF.ExecuteScaler(SqlStr);

            SqlStr = " select rowtocol('select ract.party_fk from RESTRICT_ANN_CONFIG_TBL ract where ract.type_fk=4";
            SqlStr = SqlStr + " and ann_entry=0 and ract.restrict_hdr_fk=" + RestrictPK + "') from dual";
            AgentPks = objWF.ExecuteScaler(SqlStr);


            if (!string.IsNullOrEmpty(locationPKs))
            {
                locationPKs = "0," + locationPKs;
            }
            else
            {
                locationPKs = "0";
            }
            if (!string.IsNullOrEmpty(UserPks))
            {
                UserPks = "0," + UserPks;
            }
            else
            {
                UserPks = "0";
            }

            if (!string.IsNullOrEmpty(CustomerPKs))
            {
                CustomerPKs = "0," + CustomerPKs;
            }
            else
            {
                CustomerPKs = "0";
            }

            if (!string.IsNullOrEmpty(VendorPks))
            {
                VendorPks = "0," + VendorPks;
            }
            else
            {
                VendorPks = "0";
            }

            if (!string.IsNullOrEmpty(AgentPks))
            {
                AgentPks = "0," + AgentPks;
            }
            else
            {
                AgentPks = "0";
            }

            dsMain.Tables.Add("tblMaster");
            dsMain.Tables["tblMaster"].Columns.Add("ANNOUNCEMENT_PK", typeof(System.Int32));
            dsMain.Tables["tblMaster"].Columns.Add("ANNOUNCEMENT_DT", typeof(System.DateTime));
            dsMain.Tables["tblMaster"].Columns.Add("ANNOUNCEMENT_ID", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("SUBJECT", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("BODY", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("STATUS", typeof(int));
            dsMain.Tables["tblMaster"].Columns.Add("VALID_FROM", typeof(System.DateTime));
            dsMain.Tables["tblMaster"].Columns.Add("VALID_TO", typeof(System.DateTime));
            dsMain.Tables["tblMaster"].Columns.Add("LOCATION_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("CREATED_FK", typeof(int));
            dsMain.Tables["tblMaster"].Columns.Add("DEPARTMENT_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("USERS_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("DESIGNATION_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("MANAGEMENT_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("AREA_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("AGENT_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("VENDOR_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("REGION_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("SECTOR_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("CUSTOMER_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("TRADE_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("COUNTRY_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("PORTGROUP_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("POL_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("POD_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("COMMODITY_MST_FK", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("TYPE_FLAG", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("ANNOUNCEMENT_DT_EXT", typeof(System.DateTime));
            dsMain.Tables["tblMaster"].Columns.Add("STATUS_EXT", typeof(int));
            dsMain.Tables["tblMaster"].Columns.Add("VALID_FROM_EXT", typeof(System.DateTime));
            dsMain.Tables["tblMaster"].Columns.Add("VALID_TO_EXT", typeof(System.DateTime));
            dsMain.Tables["tblMaster"].Columns.Add("ANNOUNCEMENT_IDEXT", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("PRIORITY", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("SPECIFIC", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("TO_MAIL", typeof(string));
            dsMain.Tables["tblMaster"].Columns.Add("CC_MAIL", typeof(string));
            dsMain.Tables["tblmaster"].Columns.Add("PRIORITY_EXT", typeof(int));

            fillds(dsMain, RestrictPK, locationPKs, UserPks, CustomerPKs, VendorPks, AgentPks, 0);

            if (chkInt == 1)
            {
                SqlStr = "select count(*) from announcement_tbl t where t.type_flag=0 and nvl(t.restrict_mst_fk,0)=" + RestrictPK;
                cnt = Convert.ToInt32(objWF.ExecuteScaler(SqlStr));
                if (cnt == 0)
                {
                    fillds(dsMain, RestrictPK, locationPKs, UserPks, "0", "0", "0", 0);
                    SqlStr = objAnn.SaveAnnouncement(dsMain, 0, "ANNOUNCEMENT", (Int64)HttpContext.Current.Session["LOGED_IN_LOC_FK"], (Int64)HttpContext.Current.Session["EMP_PK"], (Int64)HttpContext.Current.Session["USER_PK"], 0, RestrictPK);
                }
                else
                {
                    SqlStr = " update announcement_tbl ann set ann.location_fk='" + locationPKs + "',ann.users_mst_fk='" + UserPks + "',ann.body='" + dsMain.Tables[0].Rows[0]["BODY"] + "' where ann.type_flag=0 and ann.restrict_mst_fk=" + RestrictPK;
                    Result = objWF.ExecuteCommands(SqlStr);
                }
            }
            if (chkExt == 1)
            {
                if (CustomerPKs != "0" | VendorPks != "0" | AgentPks != "0")
                {
                    fillds(dsMain, RestrictPK, locationPKs, "0", CustomerPKs, VendorPks, AgentPks, 1);
                    AnnPK = Convert.ToInt32(objAnn.SaveAnnouncement(dsMain, 0, "ANNOUNCEMENT", (Int64)HttpContext.Current.Session["LOGED_IN_LOC_FK"], (Int64)HttpContext.Current.Session["EMP_PK"], (Int64)HttpContext.Current.Session["USER_PK"], 1, RestrictPK));
                    //For Sending Mail to Customer,vendor and Agents etc
                    SqlStr = objWF.ExecuteScaler("select ann.announcement_idext from announcement_tbl ann where ann.announcement_pk=" + AnnPK);
                    //objAnn.M_AnnId = SqlStr;
                    //objAnn.Create_By = Convert.ToString(M_CREATED_BY_FK);
                    if (CustomerPKs != "0")
                    {
                        SqlStr = objAnn.SendMail(objAnn.FetchEmail(CustomerPKs, 0), Convert.ToString(dsMain.Tables[0].Rows[0]["SUBJECT"]), Convert.ToString(dsMain.Tables[0].Rows[0]["BODY"]), objAnn.FetchNames(CustomerPKs, 0), "", Convert.ToString(AnnPK));
                    }
                    if (VendorPks != "0")
                    {
                        SqlStr = objAnn.SendMail(objAnn.FetchEmail(VendorPks, 1), Convert.ToString(dsMain.Tables[0].Rows[0]["SUBJECT"]), Convert.ToString(dsMain.Tables[0].Rows[0]["BODY"]), objAnn.FetchNames(VendorPks, 1), "", Convert.ToString(AnnPK));
                    }
                    if (AgentPks != "0")
                    {
                        SqlStr = objAnn.SendMail(objAnn.FetchEmail(AgentPks, 2), Convert.ToString(dsMain.Tables[0].Rows[0]["SUBJECT"]), Convert.ToString(dsMain.Tables[0].Rows[0]["BODY"]), objAnn.FetchNames(AgentPks, 2), "", Convert.ToString(AnnPK));
                    }
                }
            }
            SqlStr = "UPDATE RESTRICT_ANN_CONFIG_TBL RACT SET RACT.ANN_ENTRY=1 WHERE RACT.RESTRICT_HDR_FK=" + RestrictPK + " AND RACT.ANN_ENTRY=0";
            Result = objWF.ExecuteCommands(SqlStr);
        }
        #endregion

        #region "Fill Announcement"
        private void fillds(DataSet dsmain, int intPKVal, string LocPks, string UserPks, string CustomerPks, string VendorPks, string AgentPks, Int16 TypeFlag)
        {
            DataRow dr = null;
            DataSet tempDS = null;
            DataSet tempDtl = null;
            DataSet CntrDs = null;
            string MsgBody = null;
            string ShippingLineDtls = null;
            string Customer = null;
            string Shipper = null;
            string Consignee = null;
            string Notify = null;
            string FrmCntry = null;
            string POO = null;
            string POL = null;
            string ToCntry = null;
            string POD = null;
            string PFD = null;
            string Incoterms = null;
            string CommGrp = null;
            string Comm = null;
            string Weight = null;
            string Volume = null;
            string Cntr = null;
            int i = 0;
            string strsql = null;
            WorkFlow objWF = new WorkFlow();

            tempDS = FetchHeader(intPKVal);
            tempDtl = FetchDetails(intPKVal);
            dsmain.Tables["tblmaster"].Clear();
            dr = dsmain.Tables["tblmaster"].NewRow();
            try
            {
                dr["ANNOUNCEMENT_PK"] = DBNull.Value;
                dr["ANNOUNCEMENT_DT"] = System.DateTime.Now;
                if (Convert.ToInt32(tempDS.Tables[0].Rows[0]["RESTRICTION_TYPE"]) == 1)
                {
                    dr["SUBJECT"] = "Customer Restriction";
                }
                else if (Convert.ToInt32(tempDS.Tables[0].Rows[0]["RESTRICTION_TYPE"]) == 2)
                {
                    dr["SUBJECT"] = "Country Restriction";
                }
                else if (Convert.ToInt32(tempDS.Tables[0].Rows[0]["RESTRICTION_TYPE"]) == 3)
                {
                    dr["SUBJECT"] = "Port Restriction";
                }
                else if (Convert.ToInt32(tempDS.Tables[0].Rows[0]["RESTRICTION_TYPE"]) == 4)
                {
                    dr["SUBJECT"] = "Commodity Group Restriction";
                }
                else if (Convert.ToInt32(tempDS.Tables[0].Rows[0]["RESTRICTION_TYPE"]) == 5)
                {
                    dr["SUBJECT"] = "Container Type Restriction";
                }
                else if (Convert.ToInt32(tempDS.Tables[0].Rows[0]["RESTRICTION_TYPE"] )== 6)
                {
                    dr["SUBJECT"] = "Weight Restriction";
                }
                else if (Convert.ToInt32(tempDS.Tables[0].Rows[0]["RESTRICTION_TYPE"]) == 7)
                {
                    dr["SUBJECT"] = "Volume Restriction";
                }
                else if (Convert.ToInt32(tempDS.Tables[0].Rows[0]["RESTRICTION_TYPE"]) == 8)
                {
                    dr["SUBJECT"] = "Shipping Line Restriction";
                }
                MsgBody = tempDS.Tables[0].Rows[0]["RESTRICTION_MESSAGE"] + "</br>";
                MsgBody = MsgBody + "Below are the details" + "</br>";
                if (tempDtl.Tables[0].Rows.Count > 0)
                {
                    for (i = 0; i <= tempDtl.Tables[0].Rows.Count - 1; i++)
                    {
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["SHIPPINGLINE_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["SHIPPINGLINE_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(ShippingLineDtls))
                                {
                                    ShippingLineDtls = ShippingLineDtls + "," + GetNamesfrmPK(Convert.ToString(tempDtl.Tables[0].Rows[i]["SHIPPINGLINE_FK"]), 0);
                                }
                                else
                                {
                                    ShippingLineDtls = "<li>Carrier&nbsp; : &nbsp;" + GetNamesfrmPK(Convert.ToString(tempDtl.Tables[0].Rows[i]["SHIPPINGLINE_FK"]), 0);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["CUSTOMER_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["CUSTOMER_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(Customer))
                                {
                                    Customer = Customer + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["CUSTOMER_FK"].ToString(), 1);
                                }
                                else
                                {
                                    Customer = "<li>Customer&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["CUSTOMER_FK"].ToString(), 1);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["SHIPPER_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["SHIPPER_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(Shipper))
                                {
                                    Shipper = Shipper + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["SHIPPER_FK"].ToString(), 1);
                                }
                                else
                                {
                                    Shipper = "<li>Shipper&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["SHIPPER_FK"].ToString(), 1);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["CONSIGNEE_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["CONSIGNEE_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(Consignee))
                                {
                                    Consignee = Consignee + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["CONSIGNEE_FK"].ToString(), 1);
                                }
                                else
                                {
                                    Consignee = "<li>Consignee&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["CONSIGNEE_FK"].ToString(), 1);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["NOTIFY_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["NOTIFY_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(Notify))
                                {
                                    Notify = Notify + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["NOTIFY_FK"].ToString(), 1);
                                }
                                else
                                {
                                    Notify = "<li>Notify Party&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["NOTIFY_FK"].ToString(), 1);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["FRMCTY_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["FRMCTY_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(FrmCntry))
                                {
                                    FrmCntry = FrmCntry + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["FRMCTY_FK"].ToString(), 2);
                                }
                                else
                                {
                                    FrmCntry = "<li>From Country&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["FRMCTY_FK"].ToString(), 2);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["POO_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["POO_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(POO))
                                {
                                    POO = POO + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["POO_FK"].ToString(), 4);
                                }
                                else
                                {
                                    POO = "<li>POO&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["POO_FK"].ToString(), 4);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["POL_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["POL_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(POL))
                                {
                                    POL = POL + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["POL_FK"].ToString(), 3);
                                }
                                else
                                {
                                    POL = "<li>POL&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["POL_FK"].ToString(), 3);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["TOCTY_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["TOCTY_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(ToCntry))
                                {
                                    ToCntry = ToCntry + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["TOCTY_FK"].ToString(), 2);
                                }
                                else
                                {
                                    ToCntry = "<li>To Country&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["TOCTY_FK"].ToString(), 2);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["POD_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["POD_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(POD))
                                {
                                    POD = POD + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["POD_FK"].ToString(), 3);
                                }
                                else
                                {
                                    POD = "<li>POD&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["POD_FK"].ToString(), 3);
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["PFD_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["PFD_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(PFD))
                                {
                                    PFD = PFD + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["PFD_FK"].ToString(), 4);
                                }
                                else
                                {
                                    PFD = "<li>PFD&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["PFD_FK"].ToString(), 4);
                                }
                            }
                        }

                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["INCOTERMS_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["INCOTERMS_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(Incoterms))
                                {
                                    Incoterms = Incoterms + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["INCOTERMS_FK"].ToString(), 5);
                                }
                                else
                                {
                                    Incoterms = "<li>Inco Terms&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["INCOTERMS_FK"].ToString(), 5);
                                }
                            }
                        }

                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["COMMGRP_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["COMMGRP_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(CommGrp))
                                {
                                    CommGrp = CommGrp + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["COMMGRP_FK"].ToString(), 6);
                                }
                                else
                                {
                                    CommGrp = "<li>Commodity Group&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["COMMGRP_FK"].ToString(), 6);
                                }
                            }
                        }

                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["COMM_FK"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["COMM_FK"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(Comm))
                                {
                                    Comm = Comm + "," + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["COMM_FK"].ToString(), 7);
                                }
                                else
                                {
                                    Comm = "<li>Commodity&nbsp; : &nbsp;" + GetNamesfrmPK(tempDtl.Tables[0].Rows[i]["COMM_FK"].ToString(), 7);
                                }
                            }
                        }

                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["WEIGHT"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["WEIGHT"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(Weight))
                                {
                                    Weight = Weight + "," + tempDtl.Tables[0].Rows[i]["WEIGHT"];
                                }
                                else
                                {
                                    Weight = "<li>Weight&nbsp; : &nbsp;" + tempDtl.Tables[0].Rows[i]["WEIGHT"];
                                }
                            }
                        }

                        if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["VOLUME"].ToString()))
                        {
                            if (!string.IsNullOrEmpty(tempDtl.Tables[0].Rows[i]["VOLUME"].ToString()))
                            {
                                if (!string.IsNullOrEmpty(Volume))
                                {
                                    Volume = Volume + "," + tempDtl.Tables[0].Rows[i]["VOLUME"];
                                }
                                else
                                {
                                    Volume = "<li>Volume&nbsp; : &nbsp;" + tempDtl.Tables[0].Rows[i]["VOLUME"];
                                }
                            }
                        }
                        strsql = "select ctmt.container_type_mst_id || case when nr_container is not null then ' - ' || nr_container else '' end || case when remarks is not null then ' - ' || remarks else '' end as cntrdtls from ";
                        strsql = strsql + " restriction_cntr_dtl rcd,container_type_mst_tbl ctmt where rcd.container_mst_fk=ctmt.container_type_mst_pk";
                        strsql = strsql + " and rcd.restriction_trn_fk=" + tempDtl.Tables[0].Rows[i]["RESTRICTION_TRN_PK"];
                        CntrDs = objWF.GetDataSet(strsql);
                        if (CntrDs.Tables[0].Rows.Count > 0)
                        {
                            for (int j = 0; j <= CntrDs.Tables[0].Rows.Count - 1; j++)
                            {
                                if (!string.IsNullOrEmpty(CntrDs.Tables[0].Rows[j]["cntrdtls"].ToString()))
                                {
                                    if (!string.IsNullOrEmpty(CntrDs.Tables[0].Rows[j]["cntrdtls"].ToString()))
                                    {
                                        if (!string.IsNullOrEmpty(Cntr))
                                        {
                                            Cntr = Cntr + "," + CntrDs.Tables[0].Rows[j]["cntrdtls"];
                                        }
                                        else
                                        {
                                            Cntr = "<li>Container Details&nbsp; : &nbsp;" + CntrDs.Tables[0].Rows[j]["cntrdtls"];
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if (!string.IsNullOrEmpty(ShippingLineDtls))
                    MsgBody = MsgBody + ShippingLineDtls + "</li>";
                if (!string.IsNullOrEmpty(Customer))
                    MsgBody = MsgBody + Customer + "</li>";
                if (!string.IsNullOrEmpty(Shipper))
                    MsgBody = MsgBody + Shipper + "</li>";
                if (!string.IsNullOrEmpty(Consignee))
                    MsgBody = MsgBody + Consignee + "</li>";
                if (!string.IsNullOrEmpty(Notify))
                    MsgBody = MsgBody + Notify + "</li>";
                if (!string.IsNullOrEmpty(FrmCntry))
                    MsgBody = MsgBody + FrmCntry + "</li>";
                if (!string.IsNullOrEmpty(POO))
                    MsgBody = MsgBody + POO + "</li>";
                if (!string.IsNullOrEmpty(POL))
                    MsgBody = MsgBody + POL + "</li>";
                if (!string.IsNullOrEmpty(ToCntry))
                    MsgBody = MsgBody + ToCntry + "</li>";
                if (!string.IsNullOrEmpty(POD))
                    MsgBody = MsgBody + POD + "</li>";
                if (!string.IsNullOrEmpty(PFD))
                    MsgBody = MsgBody + PFD + "</li>";
                if (!string.IsNullOrEmpty(Incoterms))
                    MsgBody = MsgBody + Incoterms + "</li>";
                if (!string.IsNullOrEmpty(CommGrp))
                    MsgBody = MsgBody + CommGrp + "</li>";
                if (!string.IsNullOrEmpty(Comm))
                    MsgBody = MsgBody + Comm + "</li>";
                if (!string.IsNullOrEmpty(Cntr))
                    MsgBody = MsgBody + Cntr + "</li>";
                if (!string.IsNullOrEmpty(Weight))
                    MsgBody = MsgBody + Weight + "</li>";
                if (!string.IsNullOrEmpty(Volume))
                    MsgBody = MsgBody + Volume + "</li>";

                dr["BODY"] = MsgBody;
                dr["STATUS"] = 0;
                if (string.IsNullOrEmpty(tempDS.Tables[0].Rows[0]["VALID_FROM"].ToString()))
                {
                    dr["VALID_FROM"] = "";
                }
                else
                {
                    dr["VALID_FROM"] = tempDS.Tables[0].Rows[0]["VALID_FROM"];
                }
                if (string.IsNullOrEmpty(tempDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                {
                    dr["VALID_TO"] = "";
                }
                else
                {
                    dr["VALID_TO"] = tempDS.Tables[0].Rows[0]["VALID_TO"];
                }

                dr["LOCATION_FK"] = LocPks;
                dr["CREATED_FK"] = Convert.ToInt32(HttpContext.Current.Session["USER_PK"]);
                dr["DEPARTMENT_MST_FK"] = 0;
                dr["USERS_MST_FK"] = UserPks;
                dr["DESIGNATION_MST_FK"] = 0;
                dr["MANAGEMENT_MST_FK"] = 0;
                dr["AREA_MST_FK"] = 0;
                dr["AGENT_MST_FK"] = AgentPks;
                dr["VENDOR_MST_FK"] = VendorPks;
                dr["REGION_MST_FK"] = 0;
                dr["SECTOR_MST_FK"] = 0;
                dr["CUSTOMER_MST_FK"] = CustomerPks;
                dr["TRADE_MST_FK"] = 0;
                dr["COUNTRY_MST_FK"] = 0;
                dr["PORTGROUP_MST_FK"] = 0;
                dr["POL_MST_FK"] = 0;
                dr["POD_MST_FK"] = 0;
                dr["COMMODITY_MST_FK"] = 0;
                dr["TYPE_FLAG"] = TypeFlag;
                dr["ANNOUNCEMENT_DT_EXT"] = System.DateTime.Now;
                dr["STATUS_EXT"] = 0;
                dr["PRIORITY_EXT"] = 0;
                if (string.IsNullOrEmpty(tempDS.Tables[0].Rows[0]["VALID_FROM"].ToString()))
                {
                    dr["VALID_FROM_EXT"] = "";
                }
                else
                {
                    dr["VALID_FROM_EXT"] = tempDS.Tables[0].Rows[0]["VALID_FROM"];
                }
                if (string.IsNullOrEmpty(tempDS.Tables[0].Rows[0]["VALID_TO"].ToString()))
                {
                    dr["VALID_TO_EXT"] = "";
                }
                else
                {
                    dr["VALID_TO_EXT"] = tempDS.Tables[0].Rows[0]["VALID_TO"];
                }
                dsmain.Tables["tblmaster"].Rows.Add(dr);
            }
            catch (Exception ex)
            {
            }
        }
        #endregion

        #region "Get Names from PKs"
        private string GetNamesfrmPK(string PKs, int FormType)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            //'Carrier
            if (FormType == 0)
            {
                StrSql = "SELECT ROWTOCOL('SELECT OMT.OPERATOR_NAME FROM OPERATOR_MST_TBL OMT WHERE OMT.OPERATOR_MST_PK IN (" + PKs + ") UNION SELECT AMT.AIRLINE_NAME FROM AIRLINE_MST_TBL AMT WHERE AMT.AIRLINE_MST_PK IN (" + PKs + ")') FROM DUAL";
                //Customer
            }
            else if (FormType == 1)
            {
                StrSql = "SELECT ROWTOCOL('SELECT CMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK IN (" + PKs + ") ') FROM DUAL";
                //Country
            }
            else if (FormType == 2)
            {
                StrSql = "SELECT ROWTOCOL('SELECT CMT.COUNTRY_NAME FROM COUNTRY_MST_TBL CMT WHERE CMT.COUNTRY_MST_PK IN (" + PKs + ") ') FROM DUAL";
                //POL/POD
            }
            else if (FormType == 3)
            {
                StrSql = "SELECT ROWTOCOL('SELECT PMT.PORT_NAME FROM PORT_MST_TBL PMT WHERE PMT.PORT_MST_PK  IN (" + PKs + ") ') FROM DUAL";
                //POO/PFD
            }
            else if (FormType == 4)
            {
                StrSql = "SELECT ROWTOCOL('SELECT PMT.PORT_NAME FROM PORT_MST_TBL PMT WHERE PMT.PORT_MST_PK  IN (" + PKs + ") AND PMT.PORT_TYPE =1 UNION SELECT PLC.PLACE_NAME FROM PLACE_MST_TBL PLC WHERE PLC.ACTIVE=1 AND PLC.PLACE_PK IN (" + PKs + ")') FROM DUAL";
                //Incoterms
            }
            else if (FormType == 5)
            {
                StrSql = "SELECT ROWTOCOL('SELECT INCO.INCO_CODE_DESCRIPTION FROM SHIPPING_TERMS_MST_TBL INCO WHERE INCO.SHIPPING_TERMS_MST_PK IN (" + PKs + ") ') FROM DUAL";
                //Comm. Grp
            }
            else if (FormType == 6)
            {
                StrSql = "SELECT ROWTOCOL('SELECT CGMT.COMMODITY_GROUP_DESC FROM COMMODITY_GROUP_MST_TBL CGMT WHERE CGMT.COMMODITY_GROUP_PK IN (" + PKs + ") ') FROM DUAL";
                //Comm.
            }
            else if (FormType == 7)
            {
                StrSql = "SELECT ROWTOCOL('SELECT CMT.COMMODITY_NAME FROM COMMODITY_MST_TBL CMT WHERE CMT.COMMODITY_MST_PK IN (" + PKs + ") ') FROM DUAL";
            }
            try
            {
                return objWF.ExecuteScaler(StrSql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }
        #endregion
    }
}