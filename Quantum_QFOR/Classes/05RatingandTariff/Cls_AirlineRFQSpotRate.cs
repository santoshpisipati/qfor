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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class Cls_AirlineRFQSpotRate : CommonFeatures
    {
        private long _PkValue;
        private bool NewRecord;

        private string TrnPk;

        #region " Contract No :- Enhance Search Function "

        public string FetchContractNoForAirRfq(string strCond)
        {
            Array arr = null;
            arr = strCond.Split('~');
            string LookupVal = Convert.ToString(arr.GetValue(0));
            string strSERACH_IN = Convert.ToString(arr.GetValue(1));
            string Airline = Convert.ToString(arr.GetValue(2));
            string Fdate = Convert.ToString(arr.GetValue(3));
            string TDate = (Convert.ToString(Convert.ToString(arr.GetValue(4))).Length == 0 ? "" : Convert.ToString(arr.GetValue(4)));
            string polfk = Convert.ToString(arr.GetValue(5));
            string podfk = Convert.ToString(arr.GetValue(6));
            string Commodityfk = Convert.ToString(arr.GetValue(7));
            return GetContractNOs(Airline, Fdate, TDate, polfk, podfk, Commodityfk, LookupVal, strSERACH_IN);
        }

        public string GetContractNOs(string Airline, string Fdate, string TDate, string polfk, string podfk, string Commfk, string lookupVal, string strSearchIn)
        {
            try
            {
                string EnStr = null;
                EnStr = " SELECT " + " CN.CONT_MAIN_AIR_PK           AS          \"HIDDEN\", " + " CN.CONTRACT_NO                AS          \"Contract Nr.\"," + " CN.AIRLINE_MST_FK             AS          \"HIDDEN\", " + " AR.AIRLINE_ID                 AS          \"Airline\", " + " AR.AIRLINE_NAME               AS          \"HIDDEN\", " + " CT.PORT_MST_POL_FK            AS          \"HIDDEN\", " + " PL.PORT_ID                    AS          \"HIDDEN\", " + " PL.PORT_NAME                  AS          \"HIDDEN\", " + " CT.PORT_MST_POD_FK            AS          \"HIDDEN\", " + " PD.PORT_ID                    AS          \"HIDDEN\", " + " PD.PORT_NAME                  AS          \"HIDDEN\", " + " CN.COMMODITY_GROUP_FK         AS          \"HIDDEN\", " + " '[ ' || PL.PORT_ID || '-' || PD.PORT_ID || ' ]'       AS \"Sector\", " + " TO_CHAR(CN.CONTRACT_DATE,'" + dateFormat + "')        AS \"Contract dt.\" " + " FROM      CONT_MAIN_AIR_TBL CN,       CONT_TRN_AIR_LCL CT, " + "           AIRLINE_MST_TBL AR,         PORT_MST_TBL PL,    PORT_MST_TBL PD " + " WHERE " + "   CN.CONT_MAIN_AIR_PK     =       CT.CONT_MAIN_AIR_FK         AND " + "   CN.AIRLINE_MST_FK       =       AR.AIRLINE_MST_PK(+)        AND " + "   CT.PORT_MST_POL_FK      =       PL.PORT_MST_PK(+)           AND " + "   CT.PORT_MST_POD_FK      =       PD.PORT_MST_PK(+)           AND " + "   CN.ACTIVE               =       1                           AND " + "   CN.CONT_APPROVED        =       1                               ";

                if (!string.IsNullOrEmpty(strSearchIn))
                {
                    EnStr += " and UPPER(CN.CONTRACT_NO) LIKE '%" + strSearchIn + "%'";
                }

                if (!string.IsNullOrEmpty(Airline.Trim()))
                {
                    EnStr += " and  CN.AIRLINE_MST_FK = " + Airline;
                }
                if (!string.IsNullOrEmpty(Commfk.Trim()))
                {
                    EnStr += " and CN.COMMODITY_GROUP_FK = " + Commfk;
                }
                if (!string.IsNullOrEmpty(polfk.Trim()))
                {
                    EnStr += " and CT.PORT_MST_POL_FK = " + polfk;
                }
                if (!string.IsNullOrEmpty(podfk.Trim()))
                {
                    EnStr += " and  CT.PORT_MST_POD_FK = " + podfk;
                }
                if (!string.IsNullOrEmpty(Fdate.Trim()))
                {
                    EnStr += " and  NVL(CT.VALID_TO,NULL_DATE_FORMAT) >= TO_DATE('" + Fdate + "','" + dateFormat + "')";
                }
                if (!string.IsNullOrEmpty(TDate.Trim()))
                {
                    EnStr += " and  CT.VALID_FROM <= TO_DATE('" + TDate + "','" + dateFormat + "')";
                }
                EnStr += " ORDER BY CONTRACT_NO ";
                EnStr = EnStr.Replace(" ", " ");
                EnStr = EnStr.Replace("  ", " ");
                if (lookupVal != "E")
                    return EnStr;

                DataTable DT = null;
                DT = (new WorkFlow()).GetDataTable(EnStr);
                if (DT.Rows.Count == 0)
                    return "0";
                if (DT.Rows.Count > 1)
                    return "1";
                EnStr = "";
                var _with1 = DT.Rows[0];
                EnStr += Convert.ToString(getDefault(_with1[0], "")) + "~" + Convert.ToString(getDefault(_with1[1], "")) + "~" + Convert.ToString(getDefault(_with1[2], "")) + "~" + Convert.ToString(getDefault(_with1[3], "")) + "~" + Convert.ToString(getDefault(_with1[4], "")) + "~" + Convert.ToString(getDefault(_with1[5], "")) + "~" + Convert.ToString(getDefault(_with1[6], "")) + "~" + Convert.ToString(getDefault(_with1[7], "")) + "~" + Convert.ToString(getDefault(_with1[8], "")) + "~" + Convert.ToString(getDefault(_with1[9], "")) + "~" + Convert.ToString(getDefault(_with1[10], "")) + "~" + Convert.ToString(getDefault(_with1[11], ""));
                return EnStr;
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

        #endregion " Contract No :- Enhance Search Function "

        #region " Cancel "

        public ArrayList Cancel(string SpotPk, string RfqRefNo, string AirwayBillNo)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            string strSQL = null;
            Int16 RecAffected = default(Int16);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                if (AirwayBillNo.Trim().Length > 0)
                {
                    if (clsAirwayBill.CancelUpdateMAWB(RfqRefNo, AirwayBillNo, objWK) == false)
                    {
                        throw new Exception("Error Cancel-Update in Airway Bill Master.");
                    }
                }

                var _with2 = objWK.MyCommand;
                strSQL = " Update RFQ_SPOT_RATE_AIR_TBL rm " + "    Set  ACTIVE = 0, " + "    LAST_MODIFIED_BY_FK  = " + LAST_MODIFIED_BY + ", " + "    LAST_MODIFIED_DT     =  SYSDATE " + "  Where " + "             ACTIVE               =  1    " + "     and     RFQ_SPOT_AIR_PK      =  " + SpotPk;
                _with2.Parameters.Clear();
                _with2.CommandType = CommandType.Text;
                _with2.CommandText = strSQL;
                RecAffected = Convert.ToInt16(_with2.ExecuteNonQuery());

                if (RecAffected <= 0)
                    throw new Exception(" Error in Cancelling spot rate. ");

                if (!string.IsNullOrEmpty(AirwayBillNo))
                {
                    strSQL = " Update RFQ_SPOT_TRN_AIR_TBL rt " + "    Set  MAWB_REF_NO = NULL " + "  Where " + "             MAWB_REF_NO          =  " + AirwayBillNo + "     and     RFQ_SPOT_AIR_FK      =  " + SpotPk;

                    _with2.Parameters.Clear();
                    _with2.CommandType = CommandType.Text;
                    _with2.CommandText = strSQL;
                    RecAffected = Convert.ToInt16(_with2.ExecuteNonQuery());

                    if (RecAffected <= 0)
                        throw new Exception(" Error releasing MAWB NO from spot rate. ");
                }
                arrMessage.Add("All data saved successfully");
                TRAN.Commit();
                return arrMessage;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception eX)
            {
                TRAN.Rollback();
                arrMessage.Add(eX.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion " Cancel "

        #region " Save "

        #region " Check Dependency "

        private Int16 CheckDependency(DataSet MasterDS, DataSet Gridds, OracleCommand CMD = null, string Message = "")
        {
            string Pol = null;
            string Pod = null;
            string Contract = null;
            string FromDate = null;
            string ToDate = null;
            string SpotPk = null;
            string CommodityGroup = null;
            object CustomerPk = null;
            try
            {
                CustomerPk = getDefault(MasterDS.Tables[0].Rows[0]["CUSTOMER_MST_FK"], DBNull.Value);
                Pol = Convert.ToString(getDefault(Gridds.Tables[0].Rows[0]["PORT_MST_POL_FK"], ""));
                Pod = Convert.ToString(getDefault(Gridds.Tables[0].Rows[0]["PORT_MST_POD_FK"], ""));
                Contract = Convert.ToString(getDefault(MasterDS.Tables[0].Rows[0]["CONT_MAIN_AIR_FK"], ""));
                FromDate = Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_FROM"]));
                CommodityGroup = Convert.ToString(getDefault(MasterDS.Tables[0].Rows[0]["COMMODITY_GROUP_FK"], ""));
                if (object.ReferenceEquals(Gridds.Tables[0].Rows[0]["VALID_TO"], DBNull.Value))
                {
                    ToDate = "31/12/9999";
                }
                else if (Gridds.Tables[0].Rows[0]["VALID_TO"] == "  /  /    ")
                {
                    ToDate = "31/12/9999";
                }
                else
                {
                    ToDate = Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[0]["VALID_TO"]));
                }
                if (NewRecord)
                {
                    SpotPk = "-1";
                }
                else
                {
                    SpotPk = Convert.ToString(MasterDS.Tables[0].Rows[0]["RFQ_SPOT_AIR_PK"]);
                }

                string strSQL = null;

                strSQL = "  Select count(DISTINCT rm.RFQ_REF_NO) " + "     from RFQ_SPOT_RATE_AIR_TBL rm, RFQ_SPOT_TRN_AIR_TBL rt " + " where " + "       rm.CONT_MAIN_AIR_FK       =   " + Contract + "  and   rm.RFQ_SPOT_AIR_PK       =   rt.RFQ_SPOT_AIR_FK " + "  and   rt.PORT_MST_POL_FK       =   " + Pol + "  and   rt.PORT_MST_POD_FK       =   " + Pod + "  and   rm.COMMODITY_GROUP_FK    =   " + CommodityGroup + "  AND rm.RFQ_SPOT_AIR_PK         <>  " + SpotPk + "  AND rm.ACTIVE                  = 1 " + "  AND to_date('" + FromDate + "','" + dateFormat + "') <= nvl(rt.VALID_TO,NULL_DATE_FORMAT) " + "  AND to_date('" + ToDate + "','" + dateFormat + "') >= rt.VALID_FROM ";

                if (object.ReferenceEquals(CustomerPk, DBNull.Value))
                {
                    strSQL += " AND rm.CUSTOMER_MST_FK is NULL ";
                }
                else
                {
                    strSQL += " AND rm.CUSTOMER_MST_FK = " + CustomerPk + " ";
                }

                string nRec = null;
                if (CMD == null)
                {
                    nRec = (new WorkFlow()).ExecuteScaler(strSQL);
                }
                else
                {
                    CMD.CommandType = CommandType.Text;
                    CMD.CommandText = strSQL;
                    CMD.Parameters.Clear();
                    nRec = Convert.ToString(CMD.ExecuteScalar());
                }
                if (Convert.ToInt32(nRec) > 0)
                {
                    Message = CheckDependency(FromDate, ToDate, Pol, Pod, Contract, SpotPk, CommodityGroup, CustomerPk);
                }
                return Convert.ToInt16(nRec);
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

        private string CheckDependency(string FromDate, string ToDate, string Pol, string Pod, string Contract, string SpotPk, string CommodityGroup, object CustomerPk)
        {
            string strSQL = null;

            strSQL = "  Select DISTINCT rm.RFQ_REF_NO, rm.APPROVED, rm.VERSION_NO " + "     from RFQ_SPOT_RATE_AIR_TBL rm, RFQ_SPOT_TRN_AIR_TBL rt " + " where " + "        rm.CONT_MAIN_AIR_FK      =   " + Contract + "  and   rm.RFQ_SPOT_AIR_PK       =   rt.RFQ_SPOT_AIR_FK " + "  and   rt.PORT_MST_POL_FK       =   " + Pol + "  and   rt.PORT_MST_POD_FK       =   " + Pod + "  and   rm.COMMODITY_GROUP_FK    =   " + CommodityGroup + "  AND rm.RFQ_SPOT_AIR_PK         <>  " + SpotPk + "  AND rm.ACTIVE                  = 1 " + "  AND to_date('" + FromDate + "','" + dateFormat + "') <= nvl(rt.VALID_TO,NULL_DATE_FORMAT) " + "  AND to_date('" + ToDate + "','" + dateFormat + "') >= rt.VALID_FROM ";

            if (object.ReferenceEquals(CustomerPk, DBNull.Value))
            {
                strSQL += " AND rm.CUSTOMER_MST_FK is NULL ";
            }
            else
            {
                strSQL += " AND rm.CUSTOMER_MST_FK = " + CustomerPk + " ";
            }

            DataTable DT = null;
            DataRow DR = null;
            try
            {
                DT = (new WorkFlow()).GetDataTable(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception eX)
            {
                return eX.Message;
            }
            if (DT.Rows.Count > 0)
            {
                foreach (DataRow DR_loopVariable in DT.Rows)
                {
                    DR = DR_loopVariable;
                    if (Convert.ToInt32(DR["APPROVED"].ToString()) == 1)
                    {
                        return " Already approved spot rate exist. Ref : " + DR["RFQ_REF_NO"];
                    }
                }
                return " Active spot rate exist. Ref : " + DT.Rows[0]["RFQ_REF_NO"];
            }
            return " Error Accessing Record. Refresh Application.";
        }

        #endregion " Check Dependency "

        #region " Header Save "

        #region " Logical Flow "

        // The Save operation deals most of the business rules in this layer itself
        // There are very less that is dealt on database layer. there we simply have
        // procedures to get the save request and act accordingly on the database.
        // The Save Operation works on seven DataBase Tables Simultaneously.
        // All these has to be done in a single transaction so that in the event of any exceptional
        // case all the operation can be rollbacked.
        // The same database procedure will work for both Insertion and Updation
        // It depends upon the value provided in Primary Key field
        // If it is provided a value it means that the record is already exixting so the request is for updation
        // If it is provided as NULL it will refer that the record is to be inserted.
        // This logic has been implemented in the DataBase procedure also.
        //-------------------------------------------------------------------------------------------------------
        // From Here IU will mean Insert & Update Operation
        // The Following is for IU in Master Table
        // It initiate a transaction
        // Through the same Command Object several calls will be made for IU in other Child Tables
        // If a corresponding AirWay Bill no is provided then it will go and update that Airway bill table
        // also within the same transaction.
        // In case of New Record it will generate a new Reference no for the Spot Rate
        // Before IU if verifies for any clash with other existing RFQ.
        // if so the call to CheckDependency method will return a Message and so the transaction will
        // be cancelled.
        // The Spot Rate is always based on Contract and so the validity period should always be kept within
        // that contract. This Validity is taken care by the DataBase procedure which returns -1 in such event
        // In other cases that returns the Primary Key Value.
        // By keeping track on the return value here it can be decided whether that clashes with contract validity.
        // Lastly this invokes a function that performs the IU for other transactions.
        // There we pass the Command Object By Reference that already attached the Transaction
        // So other DataBase Procedures invoked through that command Object will remain within that transaction.
        // The One thing that is important to understand is that passing the return value to child save functions.
        // In case of Update that will be available in the DataTable also
        // But in case of insert we need the newly inserted PK value to be used as FK in insertion of child tables.
        // This is why the returned PK value is passed to the child save function.
        // The same concept continues in child save methods also.

        #endregion " Logical Flow "

        public ArrayList Save(DataSet MasterDS, DataSet GridDS, DataTable OthDt, DataSet CalcDS, string Measure, string Wt, string Divfac, bool NRec = false, string RFQNo = null, long nLocationId = 0,
        long nEmpId = 0, string AirwayBillNo = "", bool forApproval = false, Int16 Restricted = 0, string sid = "", string polid = "", string podid = "")
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            NewRecord = NRec;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                if (CheckDependency(MasterDS, GridDS, objWK.MyCommand, this.ErrorMessage) > 0)
                {
                    arrMessage.Add(this.ErrorMessage);
                    return arrMessage;
                }
                if (NewRecord & string.IsNullOrEmpty(RFQNo))
                {
                    RFQNo = GenerateRFQNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, sid, polid, podid);
                    if (RFQNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                    }
                    MasterDS.Tables[0].Rows[0]["RFQ_REF_NO"] = RFQNo;
                }
                if (AirwayBillNo.Trim().Length > 0)
                {
                    if (clsAirwayBill.UpdateMAWB(RFQNo, AirwayBillNo, "1", objWK) == false)
                    {
                        if (NewRecord)
                            throw new Exception("Error updating Airway Bill Master.");
                    }
                }
                objWK.MyCommand.Parameters.Clear();
                var _with3 = objWK.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".RFQ_SPOT_RATE_AIR_TBL_PKG.RFQ_SPOT_RATE_AIR_TBL_IU";

                var _with4 = _with3.Parameters;
                if (NewRecord)
                {
                    _with4.Add("RFQ_SPOT_AIR_PK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with4.Add("RFQ_SPOT_AIR_PK_IN", MasterDS.Tables[0].Rows[0]["RFQ_SPOT_AIR_PK"]).Direction = ParameterDirection.Input;
                }

                _with4.Add("AIRLINE_MST_FK_IN", MasterDS.Tables[0].Rows[0]["AIRLINE_MST_FK"]).Direction = ParameterDirection.Input;

                _with4.Add("CONT_MAIN_AIR_FK_IN", MasterDS.Tables[0].Rows[0]["CONT_MAIN_AIR_FK"]).Direction = ParameterDirection.Input;

                _with4.Add("CUSTOMER_MST_FK_IN", MasterDS.Tables[0].Rows[0]["CUSTOMER_MST_FK"]).Direction = ParameterDirection.Input;

                _with4.Add("RFQ_REF_NO_IN", MasterDS.Tables[0].Rows[0]["RFQ_REF_NO"]).Direction = ParameterDirection.Input;
                _with4.Add("RFQ_DATE_IN", Convert.ToString(Convert.ToDateTime(MasterDS.Tables[0].Rows[0]["RFQ_DATE"]))).Direction = ParameterDirection.Input;
                _with4.Add("APPROVED_IN", MasterDS.Tables[0].Rows[0]["APPROVED"]).Direction = ParameterDirection.Input;
                _with4.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(MasterDS.Tables[0].Rows[0]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                if ((object.ReferenceEquals(MasterDS.Tables[0].Rows[0]["VALID_TO"], DBNull.Value)))
                {
                    _with4.Add("VALID_TO_IN", MasterDS.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with4.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(MasterDS.Tables[0].Rows[0]["VALID_TO"]))).Direction = ParameterDirection.Input;
                }

                _with4.Add("ACTIVE_IN", MasterDS.Tables[0].Rows[0]["ACTIVE"]).Direction = ParameterDirection.Input;

                _with4.Add("COMMODITY_MST_FK_IN", MasterDS.Tables[0].Rows[0]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;

                _with4.Add("COMMODITY_GROUP_FK_IN", MasterDS.Tables[0].Rows[0]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;

                _with4.Add("AIRLINE_REFERENCE_NO_IN", getDefault(MasterDS.Tables[0].Rows[0]["AIRLINE_REFERENCE_NO"], DBNull.Value)).Direction = ParameterDirection.Input;

                _with4.Add("COL_ADDRESS_IN", getDefault(MasterDS.Tables[0].Rows[0]["COL_ADDRESS"], DBNull.Value)).Direction = ParameterDirection.Input;

                _with4.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                _with4.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with4.Add("VERSION_NO_IN", MasterDS.Tables[0].Rows[0]["VERSION_NO"]).Direction = ParameterDirection.Input;

                _with4.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                _with4.Add("RESTRICTED_IN", Restricted).Direction = ParameterDirection.Input;

                _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with3.ExecuteNonQuery();
                _PkValue = Convert.ToInt64(_with3.Parameters["RETURN_VALUE"].Value);

                if (_PkValue == -1)
                {
                    throw new Exception("Spot Rate should be within Contract Period only.");
                }

                MasterDS.Tables[0].Rows[0]["RFQ_SPOT_AIR_PK"] = _PkValue;
                GridDS.Tables[0].Rows[0]["RFQ_SPOT_AIR_FK"] = _PkValue;

                arrMessage = SaveTransaction(GridDS, OthDt, CalcDS, _PkValue, objWK.MyCommand, objWK.MyUserName, forApproval, Convert.ToString(MasterDS.Tables[0].Rows[0]["CONT_MAIN_AIR_FK"]), Measure, Wt,
                Divfac);

                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        return arrMessage;
                    }
                    else
                    {
                        if (NewRecord)
                        {
                            RollbackProtocolKey("AIRLINE RFQ SPOT RATE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RFQNo, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                if (NewRecord)
                {
                    RollbackProtocolKey("AIRLINE RFQ SPOT RATE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RFQNo, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                if (NewRecord)
                {
                    RollbackProtocolKey("AIRLINE RFQ SPOT RATE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RFQNo, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }

        #endregion " Header Save "

        #region " Transaction Save "

        #region " Logical Flow "

        // Getting the command object from main save function it performs IU on transaction
        // then it invokes other 4 Save operation using the same command object.
        // the contract period should not be violated on port-pair level also
        // the DataBase Procedure returns -1 in such event that will cause a exception to be
        // raised and finally the transaction will be rollbacked.

        #endregion " Logical Flow "

        public ArrayList SaveTransaction(DataSet Gridds, DataTable OthDt, DataSet CalcDS, long PkValue, OracleCommand SCM, string UserName, bool forApproval, string ContractNo, string Measure, string Wt,
        string Divfac)
        {
            Int32 nRowCnt = default(Int32);
            long TranPk = 0;
            long BaseCurrency = 0;
            long gTranPk = 0;
            BaseCurrency = Convert.ToInt32(GetBaseCurrency());
            arrMessage.Clear();
            try
            {
                var _with5 = SCM;
                _with5.CommandType = CommandType.StoredProcedure;
                for (nRowCnt = 0; nRowCnt <= Gridds.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    gTranPk = Convert.ToInt32(Gridds.Tables[0].Rows[nRowCnt][1]);
                    _with5.CommandText = UserName + ".RFQ_SPOT_RATE_AIR_TBL_PKG.RFQ_SPOT_TRN_AIR_IU";
                    var _with6 = _with5.Parameters;
                    _with6.Clear();
                    if (NewRecord)
                    {
                        _with6.Add("RFQ_SPOT_AIR_TRN_PK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Add("RFQ_SPOT_AIR_TRN_PK_IN", Gridds.Tables[0].Rows[nRowCnt]["RFQ_SPOT_AIR_TRN_PK"]).Direction = ParameterDirection.Input;
                    }

                    _with6.Add("RFQ_SPOT_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with6.Add("PORT_MST_POL_FK_IN", Gridds.Tables[0].Rows[nRowCnt]["PORT_MST_POL_FK"]).Direction = ParameterDirection.Input;

                    _with6.Add("PORT_MST_POD_FK_IN", Gridds.Tables[0].Rows[nRowCnt]["PORT_MST_POD_FK"]).Direction = ParameterDirection.Input;

                    _with6.Add("GROSS_WEIGHT_IN", Gridds.Tables[0].Rows[nRowCnt]["GROSS_WEIGHT"]).Direction = ParameterDirection.Input;

                    _with6.Add("VOLUME_IN", Gridds.Tables[0].Rows[0]["VOLUME_IN_CBM"]).Direction = ParameterDirection.Input;

                    _with6.Add("PACK_COUNT_IN", Gridds.Tables[0].Rows[0]["PACK_COUNT"]).Direction = ParameterDirection.Input;

                    _with6.Add("CHARGEABLE_WEIGHT_IN", Gridds.Tables[0].Rows[0]["CHARGEABLE_WEIGHT"]).Direction = ParameterDirection.Input;

                    _with6.Add("VOLUME_WEIGHT_IN", Gridds.Tables[0].Rows[0]["VOLUME_WEIGHT"]).Direction = ParameterDirection.Input;

                    _with6.Add("DENSITY_IN", Gridds.Tables[0].Rows[0]["DENSITY"]).Direction = ParameterDirection.Input;

                    _with6.Add("MAWB_REF_NO_IN", Gridds.Tables[0].Rows[nRowCnt]["MAWB_REF_NO"]).Direction = ParameterDirection.Input;

                    _with6.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[nRowCnt]["VALID_FROM"]))).Direction = ParameterDirection.Input;

                    if ((object.ReferenceEquals(Gridds.Tables[0].Rows[0]["VALID_TO"], DBNull.Value)))
                    {
                        _with6.Add("VALID_TO_IN", Gridds.Tables[0].Rows[nRowCnt]["VALID_TO"]).Direction = ParameterDirection.Input;
                    }
                    else if (Gridds.Tables[0].Rows[nRowCnt]["VALID_TO"] == "  /  /    ")
                    {
                        _with6.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with6.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[nRowCnt]["VALID_TO"]))).Direction = ParameterDirection.Input;
                    }
                    _with6.Add("CONT_MAIN_AIR_FK_IN", ContractNo).Direction = ParameterDirection.Input;

                    _with6.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with5.ExecuteNonQuery();
                    TranPk = Convert.ToInt64(_with5.Parameters["RETURN_VALUE"].Value);
                    if (TranPk == -1)
                    {
                        throw new Exception("Spot Rate should be within Contract Period only.");
                    }
                    arrMessage = SaveFrtTran(Gridds, Convert.ToInt16(nRowCnt), TranPk, SCM, UserName, forApproval);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }
                    arrMessage = SaveSurcharge(Gridds, Convert.ToInt16(nRowCnt), TranPk, SCM, UserName, BaseCurrency, forApproval);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }

                    arrMessage = SaveOthCharge(OthDt, TranPk, gTranPk, SCM, UserName, BaseCurrency, forApproval);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }

                    arrMessage = SaveCargoCalculator(CalcDS, TranPk, SCM, UserName, BaseCurrency, Measure, Wt, Divfac);

                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
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

        #endregion " Transaction Save "

        #region " Other Charge Save "

        #region " Logical Flow "

        // This is a simple call to OtherCharge procedure for IU operation
        // Here the one difference is that we have two Pk Values
        // PkValue and gTranPk
        // The PkValue is the returned value from Transcation and
        // the gTranPk is the Value stored in grid
        // these two values will be same while doing Update Operation
        // but in case of Insert this will differ
        // gTranPk is being used to filter relevent rows to save and
        // PkValue is used as Foreign Key for transaction table in the Save Operation.

        #endregion " Logical Flow "

        private ArrayList SaveOthCharge(DataTable DT, long PkValue, long gTranPk, OracleCommand SCM, string UserName, long BaseCurrency, bool forApproval)
        {
            Int16 nRowCnt = default(Int16);
            long TranPk = 0;
            arrMessage.Clear();
            try
            {
                var _with7 = SCM;
                _with7.CommandType = CommandType.StoredProcedure;
                for (nRowCnt = 0; nRowCnt <= DT.Rows.Count - 1; nRowCnt++)
                {
                    if (Convert.ToInt32(DT.Rows[nRowCnt][0]) == gTranPk)
                    {
                        _with7.CommandText = UserName + ".RFQ_SPOT_RATE_AIR_TBL_PKG.RFQ_SPOT_AIR_OTH_CHRG_IU";
                        var _with8 = _with7.Parameters;
                        _with8.Clear();

                        _with8.Add("RFQ_SPOT_AIR_TRN_FK_IN", PkValue).Direction = ParameterDirection.Input;

                        _with8.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(DT.Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"])).Direction = ParameterDirection.Input;

                        _with8.Add("CURRENCY_MST_FK_IN", DT.Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;

                        _with8.Add("CURRENT_RATE_IN", DT.Rows[nRowCnt]["CURRENT_RATE"]).Direction = ParameterDirection.Input;

                        _with8.Add("REQUEST_RATE_IN", DT.Rows[nRowCnt]["REQUEST_RATE"]).Direction = ParameterDirection.Input;

                        if (forApproval)
                        {
                            _with8.Add("APPROVED_RATE_IN", DT.Rows[nRowCnt]["APPROVED_RATE"]).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with8.Add("APPROVED_RATE_IN", DT.Rows[nRowCnt]["REQUEST_RATE"]).Direction = ParameterDirection.Input;
                        }
                        _with8.Add("CHARGE_BASIS_IN", DT.Rows[nRowCnt]["CHARGE_BASIS"]).Direction = ParameterDirection.Input;

                        _with8.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with7.ExecuteNonQuery();
                        TranPk = Convert.ToInt64(_with7.Parameters["RETURN_VALUE"].Value);
                        if (arrMessage.Count > 0)
                        {
                            if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                            {
                                arrMessage.Add("All data saved successfully");
                            }
                            else
                            {
                                return arrMessage;
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

        #endregion " Other Charge Save "

        #region " Cargo Calculator Save "

        #region " Logical Flow "

        // The functionality of this is a little bit different
        // It first delete all the cargo record for that transaction and then insert all relevent rows.
        // The DataBase procedure is basically for Insert. There is no update operation inside.

        #endregion " Logical Flow "

        private ArrayList SaveCargoCalculator(DataSet DS, long PkValue, OracleCommand SCM, string UserName, long BaseCurrency, string Measure, string Wt, string Divfac)
        {
            Int16 nRowCnt = default(Int16);
            long TranPk = 0;
            DataTable DT = DS.Tables[0];

            arrMessage.Clear();
            try
            {
                var _with9 = SCM;
                // Deleting All Calculator Information for corresponding Transaction.
                _with9.CommandType = CommandType.Text;
                _with9.CommandText = " Delete RFQ_SPOT_AIR_CARGO_CALC where RFQ_SPOT_AIR_TRN_FK = " + PkValue;
                _with9.Parameters.Clear();
                _with9.ExecuteNonQuery();

                _with9.CommandType = CommandType.StoredProcedure;
                for (nRowCnt = 0; nRowCnt <= DT.Rows.Count - 1; nRowCnt++)
                {
                    if (Convert.ToInt32(getDefault(DT.Rows[nRowCnt]["NOP"], 0)) > 0)
                    {
                        _with9.CommandText = UserName + ".RFQ_SPOT_RATE_AIR_TBL_PKG.RFQ_SPOT_AIR_CARGO_CALC_INS";
                        var _with10 = _with9.Parameters;
                        _with10.Clear();

                        _with10.Add("RFQ_SPOT_AIR_TRN_FK_IN", PkValue).Direction = ParameterDirection.Input;

                        _with10.Add("CARGO_NOP_IN", DT.Rows[nRowCnt]["NOP"]).Direction = ParameterDirection.Input;

                        _with10.Add("CARGO_LENGTH_IN", DT.Rows[nRowCnt]["Length"]).Direction = ParameterDirection.Input;

                        _with10.Add("CARGO_WIDTH_IN", DT.Rows[nRowCnt]["Width"]).Direction = ParameterDirection.Input;

                        _with10.Add("CARGO_HEIGHT_IN", DT.Rows[nRowCnt]["Height"]).Direction = ParameterDirection.Input;

                        _with10.Add("CARGO_CUBE_IN", DT.Rows[nRowCnt]["Cube"]).Direction = ParameterDirection.Input;

                        _with10.Add("CARGO_VOLUME_WT_IN", DT.Rows[nRowCnt]["VolWeight"]).Direction = ParameterDirection.Input;

                        _with10.Add("CARGO_ACTUAL_WT_IN", DT.Rows[nRowCnt]["ActWeight"]).Direction = ParameterDirection.Input;

                        _with10.Add("CARGO_DENSITY_IN", DT.Rows[nRowCnt]["Density"]).Direction = ParameterDirection.Input;
                        //Manoharan 30Dec06: to save Measurement, weight and Division factor
                        _with10.Add("CARGO_MEASURE_IN", (string.IsNullOrEmpty(Measure) ? "0" : Measure)).Direction = ParameterDirection.Input;
                        _with10.Add("CARGO_WT_IN", (string.IsNullOrEmpty(Wt) ? "0" : Wt)).Direction = ParameterDirection.Input;
                        _with10.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(Divfac) ? "0" : Divfac)).Direction = ParameterDirection.Input;

                        _with10.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with9.ExecuteNonQuery();
                        TranPk = Convert.ToInt64(_with9.Parameters["RETURN_VALUE"].Value);
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

        #endregion " Cargo Calculator Save "

        #region " Surcharge Save Kg "

        #region " Logical Flow "

        // This is for IU in Surcharge Table
        // It is simply performing IU for corresponding transaction
        // the surcharge information is available in the same row where the Transaction is in the DataTable
        // so in this procedure we loop through the columns and saves the transactions accordingly.
        // The surcharge columns are dynamically built columns.

        #endregion " Logical Flow "

        public ArrayList SaveSurcharge(DataSet Gridds, Int16 nRowCnt, long PkValue, OracleCommand SCM, string UserName, long BaseCurrency, bool forApproval)
        {
            Int16 nColCnt = default(Int16);
            long TranPk = 0;
            string chargeBasis = null;
            arrMessage.Clear();
            try
            {
                var _with11 = SCM;
                _with11.CommandType = CommandType.StoredProcedure;
                for (nColCnt = 17; nColCnt <= Gridds.Tables[0].Columns.Count - 3; nColCnt += 3)
                {
                    _with11.CommandText = UserName + ".RFQ_SPOT_RATE_AIR_TBL_PKG.RFQ_SPOT_AIR_SURCHARGE_IU";
                    var _with12 = _with11.Parameters;
                    _with12.Clear();

                    _with12.Add("RFQ_SPOT_AIR_TRN_FK_IN", PkValue).Direction = ParameterDirection.Input;

                    _with12.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(Gridds.Tables[0].Columns[nColCnt].Caption)).Direction = ParameterDirection.Input;

                    _with12.Add("CURRENCY_MST_FK_IN", BaseCurrency).Direction = ParameterDirection.Input;

                    _with12.Add("CURRENT_RATE_IN", Gridds.Tables[0].Rows[nRowCnt][nColCnt]).Direction = ParameterDirection.Input;

                    _with12.Add("REQUEST_RATE_IN", Gridds.Tables[0].Rows[nRowCnt][nColCnt + 1]).Direction = ParameterDirection.Input;
                    if (forApproval)
                    {
                        _with12.Add("APPROVED_RATE_IN", Gridds.Tables[0].Rows[nRowCnt][nColCnt + 2]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with12.Add("APPROVED_RATE_IN", Gridds.Tables[0].Rows[nRowCnt][nColCnt + 1]).Direction = ParameterDirection.Input;
                    }
                    chargeBasis = Gridds.Tables[0].Columns[nColCnt + 2].Caption;
                    chargeBasis = chargeBasis.Substring(chargeBasis.Length - 1, 1);

                    _with12.Add("CHARGE_BASIS_IN", Convert.ToInt64(chargeBasis)).Direction = ParameterDirection.Input;

                    _with12.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with11.ExecuteNonQuery();
                    TranPk = Convert.ToInt64(_with11.Parameters["RETURN_VALUE"].Value);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
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

        #endregion " Surcharge Save Kg "

        #region " Freight Transaction Save "

        #region " Logical Flow "

        //
        //
        //
        //

        #endregion " Logical Flow "

        public ArrayList SaveFrtTran(DataSet Gridds, Int16 RowNo, long PkValue, OracleCommand SCM, string UserName, bool forApproval)
        {
            Int32 nRowCnt = default(Int32);
            long TranPk = 0;
            arrMessage.Clear();
            try
            {
                var _with13 = SCM;
                _with13.CommandType = CommandType.StoredProcedure;
                for (nRowCnt = 0; nRowCnt <= Gridds.Tables[1].Rows.Count - 1; nRowCnt++)
                {
                    _with13.CommandText = UserName + ".RFQ_SPOT_RATE_AIR_TBL_PKG.RFQ_SPOT_AIR_FRT_IU";
                    var _with14 = _with13.Parameters;
                    _with14.Clear();

                    _with14.Add("RFQ_SPOT_TRN_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    // PkValue Can also be provided

                    _with14.Add("FREIGHT_ELEMENT_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;

                    _with14.Add("CURRENCY_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;

                    _with14.Add("MIN_AMOUNT_IN", Gridds.Tables[1].Rows[nRowCnt]["MIN_AMOUNT"]).Direction = ParameterDirection.Input;

                    _with14.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with13.ExecuteNonQuery();

                    TranPk = Convert.ToInt64(_with13.Parameters["RETURN_VALUE"].Value);
                    arrMessage = SaveSlabs(Gridds, Convert.ToInt16(nRowCnt), TranPk, SCM, UserName, forApproval);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
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

        #endregion " Freight Transaction Save "

        #region " Slabs Save "

        #region " Logical Flow "

        //
        //

        #endregion " Logical Flow "

        public ArrayList SaveSlabs(DataSet Gridds, Int16 nRowCnt, long PkValue, OracleCommand SCM, string UserName, bool forApproval)
        {
            Int32 nColCnt = default(Int32);
            long TranPk = 0;
            arrMessage.Clear();
            try
            {
                var _with15 = SCM;
                _with15.CommandType = CommandType.StoredProcedure;
                for (nColCnt = 9; nColCnt <= Gridds.Tables[1].Columns.Count - 3; nColCnt += 3)
                {
                    _with15.CommandText = UserName + ".RFQ_SPOT_RATE_AIR_TBL_PKG.RFQ_SPOT_AIR_BREAKPOINTS_IU";
                    var _with16 = _with15.Parameters;
                    _with16.Clear();

                    _with16.Add("RFQ_SPOT_AIR_FRT_FK_IN", PkValue).Direction = ParameterDirection.Input;

                    _with16.Add("AIRFREIGHT_SLABS_TBL_FK_IN", Convert.ToInt64(Gridds.Tables[1].Columns[nColCnt].Caption)).Direction = ParameterDirection.Input;

                    _with16.Add("CURRENT_RATE_IN", Gridds.Tables[1].Rows[nRowCnt][nColCnt]).Direction = ParameterDirection.Input;

                    _with16.Add("REQUEST_RATE_IN", Gridds.Tables[1].Rows[nRowCnt][nColCnt + 1]).Direction = ParameterDirection.Input;
                    if (forApproval)
                    {
                        _with16.Add("APPROVED_RATE_IN", Gridds.Tables[1].Rows[nRowCnt][nColCnt + 2]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with16.Add("APPROVED_RATE_IN", Gridds.Tables[1].Rows[nRowCnt][nColCnt + 1]).Direction = ParameterDirection.Input;
                    }
                    _with16.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with15.ExecuteNonQuery();
                    TranPk = Convert.ToInt64(_with15.Parameters["RETURN_VALUE"].Value);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
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

        #endregion " Slabs Save "

        #region " RFQ No Generate Function "

        #region " Logical Flow "

        //
        //

        #endregion " Logical Flow "

        public new string GenerateRFQNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null, string SID = "", string POLID = "", string PODID = "")
        {
            string functionReturnValue = null;
            try
            {
                functionReturnValue = GenerateProtocolKey("AIRLINE RFQ SPOT RATE", nLocationId, nEmployeeId, DateTime.Now, "", "", POLID, nCreatedBy, ObjWK, SID,
                PODID);
                return functionReturnValue;
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

        private bool ForUpdate(DataRow dr, Int16 StartFrom = 8, Int16 UpTo = -1)
        {
            Int16 i = default(Int16);
            if (UpTo < StartFrom)
                UpTo = Convert.ToInt16(Convert.ToInt16(dr.Table.Columns.Count) - 1);
            for (i = StartFrom; i <= UpTo; i++)
            {
                if ((!object.ReferenceEquals(dr[i], DBNull.Value)))
                {
                    if (Convert.ToString(dr[i]).Trim().Length != 0)
                    {
                        Int32 value = 0;
                        if (Int32.TryParse(dr[i].ToString(), out value))
                        {
                            if (value >= 0)
                            {
                                if (Convert.ToDouble(dr[i]) != 0)
                                {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            return false;
        }

        #endregion " RFQ No Generate Function "

        #endregion " Save "

        #region " Fetch One "

        public DataSet FetchOne(DataSet GridDS, DataTable OThDt, string rfqSpotRatePK = "", string contPk = "", string AirlinePk = "", string CommodityPk = "", string CustomerPk = "", string FDate = "", string TDate = "", string POL = "",
        string POD = "", string CollectionAddress = "", DataSet CalcDS = null, string Message = "", string CommodityGroup = "")
        {
            string ContainerPKs = null;
            bool NewRecord = true;
            if (rfqSpotRatePK.Trim().Length > 0)
                NewRecord = false;

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            DataSet ds = null;
            try
            {
                GridDS.Clear();

                if (!NewRecord)
                {
                    strSQL = "    Select " + "       RFQ_SPOT_AIR_PK,                                            " + "       NVL(main.ACTIVE, 0)                          ACTIVE,        " + "       RFQ_REF_NO,                                                 " + "       to_Char(main.RFQ_DATE, '" + dateFormat + "') RFQ_DATE,      " + "       main.AIRLINE_MST_FK,                                        " + "       AIRLINE_ID,                                                 " + "       AIRLINE_NAME,                                               " + "       main.CUSTOMER_MST_FK,                                       " + "       CUSTOMER_ID,                                                " + "       CUSTOMER_NAME,                                              " + "       main.CONT_MAIN_AIR_FK,                                      " + "       CONTRACT_NO,                                                " + "       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM,  " + "       to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO,      " + "       main.COMMODITY_MST_FK,                                      " + "       COMMODITY_ID,                                               " + "       COMMODITY_NAME,                                             " + "       NVL(main.APPROVED, 0) APPROVED,                             " + "       NVL(main.ACTIVE, 0) ACTIVE,                                 " + "       main.APPROVED_BY_FK,                                        " + "       USER_NAME,                                                  " + "      to_Char(main.APPROVED_DT, '" + dateFormat + "') APPROVED_DT, " + "       main.VERSION_NO,                                            " + "       main.AIRLINE_REFERENCE_NO,                                  " + "       main.COL_ADDRESS,                                           " + "       main.COMMODITY_GROUP_FK                                     " + "      from                                                         " + "       RFQ_SPOT_RATE_AIR_TBL                   main,               " + "       AIRLINE_MST_TBL,                                            " + "       CUSTOMER_MST_TBL,                                           " + "       CONT_MAIN_AIR_TBL,                                          " + "       COMMODITY_MST_TBL,                                          " + "       USER_MST_TBL                                                " + "      where  main.RFQ_SPOT_AIR_PK      = " + rfqSpotRatePK + "     " + "       AND main.AIRLINE_MST_FK         =   AIRLINE_MST_PK(+)   " + "       AND main.CUSTOMER_MST_FK        =   CUSTOMER_MST_PK(+)  " + "       AND main.CONT_MAIN_AIR_FK       =   CONT_MAIN_AIR_PK(+) " + "       AND main.COMMODITY_MST_FK       =   COMMODITY_MST_PK(+) " + "       AND main.APPROVED_BY_FK         =   USER_MST_PK(+) ";
                }
                else
                {
                    strSQL = "    Select " + "        CONT_MAIN_AIR_PK                   RFQ_SPOT_AIR_PK,        " + "        1                                  ACTIVE,                 " + "        ''                                 RFQ_REF_NO,             " + "       to_Char(SYSDATE, '" + dateFormat + "') RFQ_DATE,            " + "       main.AIRLINE_MST_FK,                                        " + "       AIRLINE_ID,                                                 " + "       AIRLINE_NAME,                                               " + "    " + getDefault(CustomerPk, "''") + "   CUSTOMER_MST_FK,        " + "    " + (string.IsNullOrEmpty(CustomerPk.Trim()) ? "''" : "") + " CUSTOMER_ID,            " + "    " + (string.IsNullOrEmpty(CustomerPk.Trim()) ? "''" : "") + " CUSTOMER_NAME,          " + "       CONT_MAIN_AIR_PK                    CONT_MAIN_AIR_FK,       " + "       CONTRACT_NO,                                                " + "    '" + FDate + "'                        VALID_FROM,             " + "    '" + TDate + "'                        VALID_TO,               " + "    " + getDefault(CommodityPk, "''") + "   COMMODITY_MST_FK,      " + "    " + (string.IsNullOrEmpty(CommodityPk.Trim()) ? "''" : "") + " COMMODITY_ID,          " + "    " + (string.IsNullOrEmpty(CommodityPk.Trim()) ? "''" : "") + " COMMODITY_NAME,        " + "       0                                   APPROVED,               " + "       0                                   ACTIVE,                 " + "       NULL                                APPROVED_BY_FK,         " + "       NULL                                USER_NAME,              " + "       NULL                                APPROVED_DT,            " + "       0                                   VERSION_NO,             " + "       ''                                  AIRLINE_REFERENCE_NO,   " + "       ''                                  COL_ADDRESS,            " + "       main.COMMODITY_GROUP_FK             COMMODITY_GROUP_FK      " + "      from                                                         " + "       CONT_MAIN_AIR_TBL                       main,               " + "       AIRLINE_MST_TBL                                             " + "   " + (string.IsNullOrEmpty(CustomerPk) ? "" : "          ,CUSTOMER_MST_TBL       ") + "   " + (string.IsNullOrEmpty(CommodityPk) ? "" : "         ,COMMODITY_MST_TBL      ") + "      where  main.CONT_MAIN_AIR_PK     = " + contPk + "                 " + "       AND main.AIRLINE_MST_FK         =   AIRLINE_MST_PK(+)            " + "       AND main.COMMODITY_GROUP_FK     = " + CommodityGroup + "         " + " " + (string.IsNullOrEmpty(CustomerPk.Trim()) ? "" : " And " + CustomerPk + "    = CUSTOMER_MST_PK(+) ") + " " + (string.IsNullOrEmpty(CommodityPk.Trim()) ? "" : " And " + CommodityPk + "  = COMMODITY_MST_PK(+)");
                }

                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                ds = objWF.GetDataSet(strSQL);
                string HeaderPk = Convert.ToString(ds.Tables[0].Rows[0]["RFQ_SPOT_AIR_PK"]);
                CollectionAddress = Convert.ToString(getDefault(ds.Tables[0].Rows[0]["COL_ADDRESS"], ""));

                if (!string.IsNullOrEmpty(CustomerPk) & CollectionAddress.Trim().Length == 0)
                {
                    strSQL = "Select COL_ADDRESS from customer_mst_tbl where CUSTOMER_MST_PK = " + CustomerPk;
                    CollectionAddress = Convert.ToString(getDefault(objWF.ExecuteScaler(strSQL), ""));
                }

                if (!NewRecord)
                {
                    strSQL = "    Select                                                        " + "       main.RFQ_SPOT_AIR_FK,                                       " + "       main.RFQ_SPOT_AIR_TRN_PK,                                   " + "       main.PORT_MST_POL_FK,                                       " + "       PORTPOL.PORT_ID                             PORTPOL_ID,     " + "       PORTPOL.PORT_NAME                           PORTPOL_NAME,   " + "       main.PORT_MST_POD_FK,                                       " + "       PORTPOD.PORT_ID                             PORTPOD_ID,     " + "       PORTPOD.PORT_NAME                           PORTPOD_NAME,   " + "       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM,  " + "       to_Char(main.VALID_TO, '" + dateFormat + "')   VALID_TO,    " + "       MAWB_REF_NO,                                                " + "       GROSS_WEIGHT,                                               " + "       VOLUME_IN_CBM,                                              " + "       PACK_COUNT,                                                 " + "       CHARGEABLE_WEIGHT,                                          " + "       VOLUME_WEIGHT,                                              " + "       DENSITY                                                     " + "      from                                                         " + "       RFQ_SPOT_TRN_AIR_TBL main,                                  " + "       PORT_MST_TBL PORTPOL,                                       " + "       PORT_MST_TBL PORTPOD                                        " + "      where  RFQ_SPOT_AIR_FK       = " + rfqSpotRatePK + "         " + "       AND PORT_MST_POL_FK         =   PORTPOL.PORT_MST_PK         " + "       AND PORT_MST_POD_FK         =   PORTPOD.PORT_MST_PK         ";
                }
                else
                {
                    strSQL = "    Select                                                        " + "       CONT_MAIN_AIR_FK                        RFQ_SPOT_AIR_FK,    " + "       CONT_TRN_AIR_PK                         RFQ_SPOT_AIR_TRN_PK," + "       main.PORT_MST_POL_FK,                                       " + "       PORTPOL.PORT_ID                             PORTPOL_ID,     " + "       PORTPOL.PORT_NAME                           PORTPOL_NAME,   " + "       main.PORT_MST_POD_FK,                                       " + "       PORTPOD.PORT_ID                             PORTPOD_ID,     " + "       PORTPOD.PORT_NAME                           PORTPOD_NAME,   " + "   '" + FDate + "'                                 VALID_FROM,     " + "   '" + TDate + "'                                 VALID_TO,       " + "       ''                                          MAWB_REF_NO,    " + "       0                                           GROSS_WEIGHT,   " + "       0                                           VOLUME_IN_CBM,  " + "       0                                           PACK_COUNT,     " + "       0                                         CHARGEABLE_WEIGHT," + "       0                                           VOLUME_WEIGHT,  " + "       0                                           DENSITY         " + "      from                                                         " + "       CONT_TRN_AIR_LCL                main,                       " + "       PORT_MST_TBL                    PORTPOL,                    " + "       PORT_MST_TBL                    PORTPOD                     " + "      where  CONT_MAIN_AIR_FK      = " + contPk + "                " + "       AND PORT_MST_POL_FK         =   PORTPOL.PORT_MST_PK         " + "       AND PORT_MST_POD_FK         =   PORTPOD.PORT_MST_PK         " + "       AND PORT_MST_POL_FK         =  " + POL + "                  " + "       AND PORT_MST_POD_FK         =  " + POD + "                  ";
                }
                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");

                DataTable ChildDt = null;
                ChildDt = objWF.GetDataTable(strSQL);
                TrnPk = Convert.ToString(ChildDt.Rows[0][1]);

                if (!NewRecord)
                {
                    strSQL = "    Select                                                    " + "        ROWNUM                             SNo,                " + "        CARGO_NOP                          NOP,                " + "        CARGO_LENGTH                       Length,             " + "        CARGO_WIDTH                        Width,              " + "        CARGO_HEIGHT                       Height,             " + "        CARGO_CUBE                         Cube,               " + "        CARGO_VOLUME_WT                    VolWeight,          " + "        CARGO_ACTUAL_WT                    ActWeight,          " + "        CARGO_DENSITY                      Density,            " + "        RFQ_SPOT_AIR_TRN_FK                FK                  " + "     FROM                                                      " + "        RFQ_SPOT_AIR_CARGO_CALC                                " + "     WHERE                                                     " + "        RFQ_SPOT_AIR_TRN_FK                = " + TrnPk + "     ";

                    strSQL = strSQL.Replace("   ", " ");
                    strSQL = strSQL.Replace("  ", " ");

                    CalcDS = objWF.GetDataSet(strSQL);
                }

                DataTable KGFrtDt = null;
                if (!NewRecord)
                {
                    strSQL = "    Select                                                        " + "       RFQ_SPOT_AIR_TRN_FK,                                        " + "       tran.FREIGHT_ELEMENT_MST_FK,                                " + "       FREIGHT_ELEMENT_ID,                                         " + "       FREIGHT_ELEMENT_NAME,                                       " + "       tran.CURRENCY_MST_FK,                                       " + "       CURRENCY_ID,                                                " + "       CURRENCY_NAME,                                              " + "       CURRENT_RATE,                                               " + "       REQUEST_RATE,                                               " + "       APPROVED_RATE,                                              " + "       tran.CHARGE_BASIS                                           " + "      from                                                         " + "       RFQ_SPOT_TRN_AIR_TBL        main,                           " + "       RFQ_SPOT_AIR_SURCHARGE      tran,                           " + "       FREIGHT_ELEMENT_MST_TBL,                                    " + "       CURRENCY_TYPE_MST_TBL                                       " + "      where                                                        " + "               RFQ_SPOT_AIR_TRN_FK    = " + TrnPk + "              " + "       AND     main.RFQ_SPOT_AIR_TRN_PK = tran.RFQ_SPOT_AIR_TRN_FK " + "       AND     FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK     " + "       AND     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1             " + "       AND     CURRENCY_MST_FK        = CURRENCY_MST_PK            " + "       AND     CHARGE_TYPE <> 3                                    " + "       AND     FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)       " + "      Order By FREIGHT_ELEMENT_ID                                  ";
                }
                else
                {
                    strSQL = "    Select                                                        " + "       CONT_TRN_AIR_FK                     RFQ_SPOT_AIR_TRN_FK,    " + "       tran.FREIGHT_ELEMENT_MST_FK,                                " + "       FREIGHT_ELEMENT_ID,                                         " + "       FREIGHT_ELEMENT_NAME,                                       " + "       tran.CURRENCY_MST_FK,                                       " + "       CURRENCY_ID,                                                " + "       CURRENCY_NAME,                                              " + "       APPROVED_RATE                       CURRENT_RATE,           " + "       APPROVED_RATE                       REQUEST_RATE,           " + "       0                                   APPROVED_RATE,          " + "       tran.CHARGE_BASIS                                           " + "      from                                                         " + "       CONT_AIR_SURCHARGE          tran,                           " + "       FREIGHT_ELEMENT_MST_TBL,                                    " + "       CURRENCY_TYPE_MST_TBL                                       " + "      where                                                        " + "               CONT_TRN_AIR_FK        =  " + TrnPk + "             " + "       AND     FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK     " + "       AND     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1             " + "       AND     CURRENCY_MST_FK        = CURRENCY_MST_PK            " + "       AND     CHARGE_TYPE <> 3                                    " + "       AND     FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)       " + "      Order By FREIGHT_ELEMENT_ID                                  ";
                }

                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");

                KGFrtDt = objWF.GetDataTable(strSQL);
                string KGFreights = null;
                KGFreights = getStrFreights(KGFrtDt);
                AddColumns(ChildDt, KGFreights);
                TransferKGFreightsData(ChildDt, KGFrtDt);
                ChildDt.Columns.Add("OthChrg");
                ChildDt.Columns.Add("OTHDTL");

                if (!NewRecord)
                {
                    strSQL = "    Select                                                        " + "       RFQ_SPOT_AIR_TRN_FK,                                        " + "       tran.FREIGHT_ELEMENT_MST_FK,                                " + "       FREIGHT_ELEMENT_ID,                                         " + "       FREIGHT_ELEMENT_NAME,                                       " + "       tran.CURRENCY_MST_FK,                                       " + "       CURRENCY_ID,                                                " + "       CURRENCY_NAME,                                              " + "       CURRENT_RATE,                                               " + "       REQUEST_RATE,                                               " + "       APPROVED_RATE,                                              " + "       tran.CHARGE_BASIS                                           " + "      from                                                         " + "       RFQ_SPOT_TRN_AIR_TBL        main,                           " + "       RFQ_SPOT_AIR_OTH_CHRG       tran,                           " + "       FREIGHT_ELEMENT_MST_TBL,                                    " + "       CURRENCY_TYPE_MST_TBL                                       " + "      where                                                        " + "               RFQ_SPOT_AIR_TRN_FK    = " + TrnPk + "              " + "       AND     main.RFQ_SPOT_AIR_TRN_PK = tran.RFQ_SPOT_AIR_TRN_FK " + "       AND     FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK     " + "       AND     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1             " + "       AND     CURRENCY_MST_FK        = CURRENCY_MST_PK            " + "       AND     CHARGE_TYPE = 3                             " + "       AND     FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)       " + "      Order By FREIGHT_ELEMENT_ID                                  ";
                }
                else
                {
                    strSQL = "    Select                                                        " + "       CONT_TRN_AIR_FK                     RFQ_SPOT_AIR_TRN_FK,    " + "       tran.FREIGHT_ELEMENT_MST_FK,                                " + "       FREIGHT_ELEMENT_ID,                                         " + "       FREIGHT_ELEMENT_NAME,                                       " + "       tran.CURRENCY_MST_FK,                                       " + "       CURRENCY_ID,                                                " + "       CURRENCY_NAME,                                              " + "       APPROVED_RATE                       CURRENT_RATE,           " + "       APPROVED_RATE                       REQUEST_RATE,           " + "       0                                   APPROVED_RATE,          " + "       tran.CHARGE_BASIS                                           " + "      from                                                         " + "       CONT_AIR_OTH_CHRG           tran,                           " + "       FREIGHT_ELEMENT_MST_TBL,                                    " + "       CURRENCY_TYPE_MST_TBL                                       " + "      where                                                        " + "               CONT_TRN_AIR_FK        =  " + TrnPk + "             " + "       AND     FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK     " + "       AND     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1             " + "       AND     CURRENCY_MST_FK        = CURRENCY_MST_PK            " + "       AND     CHARGE_TYPE = 3                             " + "       AND     FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)       " + "      Order By FREIGHT_ELEMENT_ID                                  ";
                }

                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");
                OThDt = objWF.GetDataTable(strSQL);

                if (!NewRecord)
                {
                    strSQL = "    Select                                                        " + "       RFQ_SPOT_TRN_FREIGHT_PK,                                    " + "       RFQ_SPOT_TRN_AIR_FK,                                        " + "       FREIGHT_ELEMENT_MST_FK,                                     " + "       FREIGHT_ELEMENT_ID,                                         " + "       FREIGHT_ELEMENT_NAME,                                       " + "       tran.CURRENCY_MST_FK,                                       " + "       CURRENCY_ID,                                                " + "       CURRENCY_NAME,                                              " + "       MIN_AMOUNT                                                  " + "      from                                                         " + "       RFQ_SPOT_AIR_TRN_FREIGHT_TBL                tran,           " + "       FREIGHT_ELEMENT_MST_TBL,                                    " + "       CURRENCY_TYPE_MST_TBL                                       " + "      where                                                        " + "               RFQ_SPOT_TRN_AIR_FK    = " + TrnPk + "              " + "       AND     FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK     " + "       AND     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1             " + "       AND     CURRENCY_MST_FK             = CURRENCY_MST_PK       " + "       AND     CHARGE_TYPE <> 3                                    " + "       AND     FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)       " + "      Order By FREIGHT_ELEMENT_ID                                  ";
                }
                else
                {
                    strSQL = "    Select                                                        " + "       CONT_AIR_FREIGHT_PK                 RFQ_SPOT_TRN_FREIGHT_PK," + "       CONT_TRN_AIR_FK                     RFQ_SPOT_TRN_AIR_FK,    " + "       FREIGHT_ELEMENT_MST_FK,                                     " + "       FREIGHT_ELEMENT_ID,                                         " + "       FREIGHT_ELEMENT_NAME,                                       " + "       tran.CURRENCY_MST_FK,                                       " + "       CURRENCY_ID,                                                " + "       CURRENCY_NAME,                                              " + "       MIN_AMOUNT                                                  " + "      from                                                         " + "       CONT_AIR_FREIGHT_TBL                        tran,           " + "       FREIGHT_ELEMENT_MST_TBL,                                    " + "       CURRENCY_TYPE_MST_TBL                                       " + "      where                                                        " + "               CONT_TRN_AIR_FK        = " + TrnPk + "              " + "       AND     FREIGHT_ELEMENT_MST_FK  = FREIGHT_ELEMENT_MST_PK    " + "       AND     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1             " + "       AND     CURRENCY_MST_FK         = CURRENCY_MST_PK           " + "       AND     CHARGE_TYPE <> 3                                   " + "       AND     FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)       " + "      Order By FREIGHT_ELEMENT_ID                                  ";
                }

                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");

                DataTable GRFrtDt = null;
                GRFrtDt = objWF.GetDataTable(strSQL);

                string FreightPks = "-1,";
                Int16 rCount = default(Int16);
                for (rCount = 0; rCount <= GRFrtDt.Rows.Count - 1; rCount++)
                {
                    FreightPks += Convert.ToString(GRFrtDt.Rows[rCount][0]) + ",";
                }
                FreightPks = FreightPks.TrimEnd(',');

                if (!NewRecord)
                {
                    strSQL = "    Select                                                        " + "       RFQ_SPOT_AIR_FRT_FK,                                        " + "       tran.FREIGHT_ELEMENT_MST_FK,                                " + "       AIRFREIGHT_SLABS_TBL_PK,                                    " + "       BREAKPOINT_ID,                                              " + "       BREAKPOINT_DESC,                                            " + "       CURRENT_RATE,                                               " + "       REQUEST_RATE,                                               " + "       APPROVED_RATE                                               " + "      from                                                         " + "       RFQ_SPOT_AIR_TRN_FREIGHT_TBL        tran,                   " + "       RFQ_SPOT_AIR_BREAKPOINTS            bpnt,                   " + "       AIRFREIGHT_SLABS_TBL                                        " + "      where                                                        " + "            bpnt.RFQ_SPOT_AIR_FRT_FK in (" + FreightPks + ")       " + "       AND  bpnt.RFQ_SPOT_AIR_FRT_FK  = tran.RFQ_SPOT_TRN_FREIGHT_PK   " + "       AND  AIRFREIGHT_SLABS_TBL.ACTIVE_FLAG  = 1                      " + "       AND  bpnt.AIRFREIGHT_SLABS_TBL_FK  = AIRFREIGHT_SLABS_TBL_PK    " + "       Order By FREIGHT_ELEMENT_MST_FK, BREAKPOINT_RANGE ";
                }
                else
                {
                    strSQL = "    Select                                                        " + "       CONT_AIR_FREIGHT_FK                  RFQ_SPOT_AIR_FRT_FK,   " + "       tran.FREIGHT_ELEMENT_MST_FK,                                " + "       AIRFREIGHT_SLABS_TBL_PK,                                    " + "       BREAKPOINT_ID,                                              " + "       BREAKPOINT_DESC,                                            " + "       APPROVED_RATE                       CURRENT_RATE,           " + "       APPROVED_RATE                       REQUEST_RATE,           " + "       APPROVED_RATE                                               " + "      from                                                         " + "       CONT_AIR_FREIGHT_TBL                tran,                   " + "       CONT_AIR_BREAKPOINTS                bpnt,                   " + "       AIRFREIGHT_SLABS_TBL                                        " + "      where                                                        " + "            bpnt.CONT_AIR_FREIGHT_FK in (" + FreightPks + ")           " + "       AND  bpnt.CONT_AIR_FREIGHT_FK     = tran.CONT_AIR_FREIGHT_PK    " + "       AND  AIRFREIGHT_SLABS_TBL.ACTIVE_FLAG  = 1                      " + "       AND  bpnt.AIRFREIGHT_SLABS_FK     = AIRFREIGHT_SLABS_TBL_PK     " + "       Order By FREIGHT_ELEMENT_MST_FK, SEQUENCE_NO ";
                }

                strSQL = strSQL.Replace("   ", " ");
                strSQL = strSQL.Replace("  ", " ");

                DataTable SlbDt = null;
                SlbDt = objWF.GetDataTable(strSQL);

                string AirSlabs = null;
                AirSlabs = getStrSlabs(SlbDt);
                AddColumns(GRFrtDt, AirSlabs);
                TransferSlabsData(GRFrtDt, SlbDt);

                GridDS.Tables.Add(ChildDt);
                GridDS.Tables.Add(GRFrtDt);
                DataRelation REL = new DataRelation("RFQRelation", new DataColumn[] { GridDS.Tables[0].Columns["RFQ_SPOT_AIR_TRN_PK"] }, new DataColumn[] { GridDS.Tables[1].Columns["RFQ_SPOT_TRN_AIR_FK"] });
                GridDS.Relations.Add(REL);
                if (CheckDependency(ds, GridDS, new OracleCommand(), Message) <= 0)
                {
                    Message = "";
                }
                return ds;
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

        public DataSet getDivFacMW()
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                if ((TrnPk != null))
                {
                    if (!string.IsNullOrEmpty(TrnPk))
                    {
                        strQuery.Append("select distinct cargo_measurement, cargo_weight_in, cargo_division_fact ");
                        strQuery.Append("from rfq_spot_air_cargo_calc where ");
                        strQuery.Append("rfq_spot_air_trn_fk = " + TrnPk);
                        return objWF.GetDataSet(strQuery.ToString());
                    }
                }
                return null;
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

        #region " Transfer Data [ Kg Freights ] "

        private void TransferKGFreightsData(DataTable GridDt, DataTable FrtDt)
        {
            Int16 RowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            for (RowCnt = 0; RowCnt <= FrtDt.Rows.Count - 1; RowCnt++)
            {
                for (ColCnt = 0; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                {
                    if (Convert.ToString(FrtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                    {
                        GridDt.Rows[0][ColCnt] = FrtDt.Rows[RowCnt]["CURRENT_RATE"];
                        GridDt.Rows[0][ColCnt + 1] = FrtDt.Rows[RowCnt]["REQUEST_RATE"];
                        GridDt.Rows[0][ColCnt + 2] = FrtDt.Rows[RowCnt]["APPROVED_RATE"];
                        break; // TODO: might not be correct. Was : Exit For
                    }
                }
            }
        }

        #endregion " Transfer Data [ Kg Freights ] "

        #region " Transfer Data [ Slabs ] "

        private void TransferSlabsData(DataTable GridDt, DataTable SlabDt)
        {
            Int16 GRowCnt = default(Int16);
            Int16 SRowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            string frtpk = null;
            for (SRowCnt = 0; SRowCnt <= SlabDt.Rows.Count - 1; SRowCnt++)
            {
                frtpk = Convert.ToString(SlabDt.Rows[SRowCnt]["FREIGHT_ELEMENT_MST_FK"]);
                for (GRowCnt = 0; GRowCnt <= GridDt.Rows.Count - 1; GRowCnt++)
                {
                    if (GridDt.Rows[GRowCnt]["FREIGHT_ELEMENT_MST_FK"] == frtpk)
                    {
                        for (ColCnt = 0; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(SlabDt.Rows[SRowCnt]["AIRFREIGHT_SLABS_TBL_PK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[GRowCnt][ColCnt] = SlabDt.Rows[SRowCnt]["CURRENT_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 1] = SlabDt.Rows[SRowCnt]["REQUEST_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 2] = SlabDt.Rows[SRowCnt]["APPROVED_RATE"];
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                        break; // TODO: might not be correct. Was : Exit For
                    }
                }
            }
        }

        #endregion " Transfer Data [ Slabs ] "

        #region " Column Add "

        private void AddColumns(DataTable DT, string FRTs)
        {
            Array CHeads = null;
            string hed = null;
            CHeads = FRTs.Split(',');
            Int16 i = default(Int16);
            for (i = 0; i <= CHeads.Length - 3; i += 3)
            {
                DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)), typeof(decimal));
                DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 1)), typeof(decimal));
                DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 2)), typeof(decimal));
            }
        }

        #endregion " Column Add "

        #region " Getting Freights "

        private string getStrFreights(DataTable DT)
        {
            bool WithBasis = false;
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            try
            {
                if (DT.Columns.Contains("CHARGE_BASIS"))
                {
                    WithBasis = true;
                }
                string strChBasis = "";
                string intChBasis = "";
                string frpk = "-1";
                strBuilder.Append("");
                if (DT.Rows.Count > 0)
                {
                    frpk = "-1";
                }
                for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                {
                    if (DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"] != frpk)
                    {
                        if (WithBasis)
                        {
                            if (Convert.ToInt32(DT.Rows[RowCnt]["CHARGE_BASIS"]) == 1)
                            {
                                strChBasis = "(%)";
                            }
                            else if (Convert.ToInt32(DT.Rows[RowCnt]["CHARGE_BASIS"]) == 2)
                            {
                                strChBasis = "(Flat)";
                            }
                            else if (Convert.ToInt32(DT.Rows[RowCnt]["CHARGE_BASIS"]) == 3)
                            {
                                strChBasis = "(Kgs)";
                            }
                            else
                            {
                                strChBasis = "(--)";
                            }
                            intChBasis = "~" + Convert.ToString(DT.Rows[RowCnt]["CHARGE_BASIS"]);
                        }
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"])).Trim() + ",");
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_ID"])).Trim() + strChBasis + ",");
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_NAME"])).Trim() + intChBasis + ",");
                        frpk = Convert.ToString(DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"]);
                    }
                }
                if (DT.Rows.Count > 0)
                    strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
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

        #endregion " Getting Freights "

        #region " Getting Slabs "

        private string getStrSlabs(DataTable DT)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            string frpk = "-1";
            string slpk = "-1";
            try
            {
                if (DT.Rows.Count > 0)
                {
                    frpk = Convert.ToString(DT.Rows[0]["FREIGHT_ELEMENT_MST_FK"]);
                }
                strBuilder.Append("");
                for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                {
                    if (frpk != Convert.ToString(DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"]))
                        break; // TODO: might not be correct. Was : Exit For
                    if (slpk != Convert.ToString(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"]))
                    {
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"])).Trim() + ",");
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["BREAKPOINT_ID"])).Trim() + ",");
                        strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["BREAKPOINT_DESC"])).Trim() + ".,");
                        slpk = Convert.ToString(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"]);
                    }
                }
                if (DT.Rows.Count > 0)
                    strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
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

        #endregion " Getting Slabs "

        #endregion " Fetch One "

        #region " Get Base Currency "

        public string GetBaseCurrency(string ID = "", string Name = "")
        {
            DataTable dt = null;
            try
            {
                dt = (new WorkFlow()).GetDataTable("Select CURRENCY_MST_FK, CURRENCY_ID, CURRENCY_NAME from CORPORATE_MST_TBL, CURRENCY_TYPE_MST_TBL where CURRENCY_MST_FK = CURRENCY_MST_PK ");
                ID = Convert.ToString(dt.Rows[0]["CURRENCY_ID"]);
                Name = Convert.ToString(dt.Rows[0]["CURRENCY_NAME"]);
                return Convert.ToString(dt.Rows[0]["CURRENCY_MST_FK"]);
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

        #endregion " Get Base Currency "

        #region " Supporting Function "

        private new object ifDBNull(object col)
        {
            if (Convert.ToString(col).Trim().Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        private new object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }

        public object CommodityGroupFk(object CommodityFk)
        {
            if (CommodityFk == null)
                return 0;
            if (object.ReferenceEquals(CommodityFk, DBNull.Value))
                return 0;
            DataTable dt = null;
            try
            {
                dt = (new WorkFlow()).GetDataTable("Select COMMODITY_GROUP_FK from COMMODITY_MST_TBL where " + "COMMODITY_MST_PK = " + CommodityFk);
                return dt.Rows[0][0];
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        #endregion " Supporting Function "
    }
}