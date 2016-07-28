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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_CAN : CommonFeatures
    {
        #region "  Fetch For CAN Data"
        public int CANPK;
        public DataSet FetchAirCAN(string JobPk, Int32 CurrentPage = 0, Int32 TotalPage = 0, string Place = "123", string MASTER_PK = "0")
        {

            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                if (string.IsNullOrEmpty(Place))
                {
                    Place = "123";
                }

                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_AIR_IMP_TBL_INS";

                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand.Parameters;

                _with2.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                _with2.Add("MASTER_PK_IN", Convert.ToInt32(MASTER_PK)).Direction = ParameterDirection.Input;
                _with2.Add("Place_IN", Place).Direction = ParameterDirection.Input;
                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CAN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);

                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }

                return dsData;
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
        public DataSet FetchSeaCAN(string JobPk, Int32 CurrentPage = 0, Int32 TotalPage = 0, string Place = "123", string MASTER_PK = "0")
        {

            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                if (string.IsNullOrEmpty(Place))
                {
                    Place = "123";
                }

                var _with3 = objWK.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_SEA_IMP_TBL_INS";

                objWK.MyCommand.Parameters.Clear();
                var _with4 = objWK.MyCommand.Parameters;

                _with4.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                _with4.Add("MASTER_PK_IN", Convert.ToInt32(MASTER_PK)).Direction = ParameterDirection.Input;
                _with4.Add("Place_IN", Place).Direction = ParameterDirection.Input;
                _with4.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("CAN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);

                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }

                return dsData;
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

        #region "Fetch Values"
        public DataSet FetchValues(string JobPk, Int16 BizType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                if (BizType == 2)
                {
                    var _with5 = objWK.MyCommand;
                    _with5.CommandType = CommandType.StoredProcedure;
                    _with5.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_SEA_FETCH";
                }
                else
                {
                    var _with6 = objWK.MyCommand;
                    _with6.CommandType = CommandType.StoredProcedure;
                    _with6.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_AIR_FETCH";
                }


                objWK.MyCommand.Parameters.Clear();
                var _with7 = objWK.MyCommand.Parameters;

                _with7.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                _with7.Add("CAN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
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
        public ArrayList SaveValues(DataSet DSSave, Int32 JobPk, Int32 BizType, string Location, string EmpPk, string podid = "")
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
            DataSet dsData = new DataSet();
            DataSet ds = new DataSet();
            string MSTJCRefNum = "";
            string BlNr = null;
            string custfk = null;
            string podfk = null;
            string pfdfk = null;
            string ETA = null;
            string ATA = null;
            string CANNr = null;
            OracleTransaction TRAN = null;
            int Rcnt = 0;
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyConnection = TRAN.Connection;
                //objWK.MyCommand.Connection = TRAN.Connection ' objWK.MyConnection
                if (DSSave.Tables[0].Rows.Count > 0)
                {
                    for (Rcnt = 0; Rcnt <= DSSave.Tables[0].Rows.Count - 1; Rcnt++)
                    {
                        JobPk = Convert.ToInt32(DSSave.Tables[0].Rows[Rcnt]["PK"]);
                        if (object.ReferenceEquals(DSSave.Tables[0].Rows[Rcnt]["CANNR"], System.DBNull.Value))
                        {
                            short _canCheck = 1;
                            while (_canCheck > 0)
                            {
                                MSTJCRefNum = GenerateProtocolKey("CAN (" + (BizType == 2 ? "SEA" : "AIR") + ")", Convert.ToInt32(Location), Convert.ToInt32(EmpPk), DateTime.Now,"" ,"" ,"" , CREATED_BY, new WorkFlow(),"" ,
                                podid);
                                var _with8 = objWK.MyCommand;
                                _with8.Connection = TRAN.Connection;
                                _with8.Transaction = TRAN;
                                _with8.CommandType = CommandType.Text;
                                _with8.CommandText = "SELECT COUNT(*) FROM CAN_MST_TBL CAN WHERE UPPER(CAN.CAN_REF_NO)=UPPER('" + MSTJCRefNum.ToUpper() + "')";
                                _with8.Parameters.Clear();
                                _canCheck = Convert.ToInt16(_with8.ExecuteScalar());
                            }

                            if (BizType == 2)
                            {
                                //MSTJCRefNum = GenerateProtocolKey("CAN (SEA)", Location, EmpPk, DateTime.Now, , , , CREATED_BY)
                                ds = FetchSeaCAN(JobPk.ToString(),0,0,"","");
                            }
                            else
                            {
                                //MSTJCRefNum = GenerateProtocolKey("CAN (AIR)", Location, EmpPk, DateTime.Now, , , , CREATED_BY)
                                ds = FetchAirCAN(JobPk.ToString(), 0, 0, "", "");
                            }
                        }
                        else
                        {
                            if (BizType == 2)
                            {
                                ds = FetchSeaCAN(JobPk.ToString(), 0, 0, "", "");
                            }
                            else
                            {
                                ds = FetchAirCAN(JobPk.ToString(), 0, 0, "", "");
                            }
                            MSTJCRefNum = Convert.ToString(DSSave.Tables[0].Rows[Rcnt]["CANNR"]);
                        }

                        if ((ds != null))
                        {
                            BlNr = (string.IsNullOrEmpty(ds.Tables[0].Rows[0][3].ToString()) ? "" : ds.Tables[0].Rows[0][3].ToString());
                            custfk = (string.IsNullOrEmpty(ds.Tables[0].Rows[0][4].ToString()) ? "0" : ds.Tables[0].Rows[0][4].ToString());
                            podfk = (string.IsNullOrEmpty(ds.Tables[0].Rows[0][6].ToString()) ? "0" : ds.Tables[0].Rows[0][6].ToString());
                            pfdfk = (string.IsNullOrEmpty(ds.Tables[0].Rows[0][8].ToString()) ? "0" : ds.Tables[0].Rows[0][8].ToString());
                            //ETA = IIf(IsDBNull(DSSave.Tables(0).Rows(0).Item("ETA")), "", DSSave.Tables(0).Rows(0).Item("ETA"))
                            //ATA = IIf(IsDBNull(DSSave.Tables(0).Rows(0).Item("ATA")), "", DSSave.Tables(0).Rows(0).Item("ATA"))
                        }

                        var _with9 = objWK.MyCommand;
                        _with9.Connection = TRAN.Connection;
                        _with9.Transaction = TRAN;
                        _with9.CommandType = CommandType.StoredProcedure;
                        if (BizType == 2)
                        {
                            _with9.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_SEA_SAVE";
                        }
                        else
                        {
                            _with9.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_AIR_SAVE";
                        }

                        objWK.MyCommand.Parameters.Clear();
                        var _with10 = objWK.MyCommand.Parameters;

                        _with10.Add("CAN_REF_IN", MSTJCRefNum).Direction = ParameterDirection.Input;
                        _with10.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                        _with10.Add("BL_REF_IN", (string.IsNullOrEmpty(BlNr) ? "" : BlNr)).Direction = ParameterDirection.Input;
                        _with10.Add("CUS_MST_FK_IN", custfk).Direction = ParameterDirection.Input;
                        _with10.Add("POD_MST_FK_IN", podfk).Direction = ParameterDirection.Input;
                        _with10.Add("PFD_MST_FK_IN", pfdfk).Direction = ParameterDirection.Input;
                        try
                        {
                            if (string.IsNullOrEmpty(DSSave.Tables[0].Rows[0]["ETA"].ToString().Trim()))
                            {
                                _with10.Add("ETA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with10.Add("ETA_IN", Convert.ToString(DSSave.Tables[0].Rows[0]["ETA"])).Direction = ParameterDirection.Input;
                            }
                        }
                        catch (Exception ex)
                        {
                            _with10.Add("ETA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        try
                        {
                            if (string.IsNullOrEmpty(DSSave.Tables[0].Rows[0]["ATA"].ToString().Trim()))
                            {
                                _with10.Add("ATA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with10.Add("ATA_IN", Convert.ToString(DSSave.Tables[0].Rows[0]["ATA"])).Direction = ParameterDirection.Input;
                            }
                        }
                        catch (Exception ex)
                        {
                            _with10.Add("ATA_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        try
                        {
                            _with10.Add("CUST_REF_NO_IN", DSSave.Tables[0].Rows[0]["CUSTOM_REF_NO"]).Direction = ParameterDirection.Input;
                        }
                        catch (Exception ex)
                        {
                            _with10.Add("CUST_REF_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        try
                        {
                            _with10.Add("CUST_REF_DT_IN", Convert.ToString(DSSave.Tables[0].Rows[0]["CUSTOM_REF_DT"])).Direction = ParameterDirection.Input;
                        }
                        catch (Exception ex)
                        {
                            _with10.Add("CUST_REF_DT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        try
                        {
                            _with10.Add("CUST_ITEM_NR_IN", DSSave.Tables[0].Rows[0]["CUSTOM_ITEM_NR"]).Direction = ParameterDirection.Input;
                        }
                        catch (Exception ex)
                        {
                            _with10.Add("CUST_ITEM_NR_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        _with10.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                        _with10.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                        _with10.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        int st = 0;
                        st = _with9.ExecuteNonQuery();


                        if (st > 0)
                        {
                            CANPK = (string.IsNullOrEmpty(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString()) ? 0 : Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString()));
                            //To save the transaction track and trace Anand G 10/11/08
                            arrMessage = objTrackNTrace.SaveTrackAndTrace(JobPk, BizType, 2, "CAN", "CAN-INS", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWK, "INS", Convert.ToInt32(HttpContext.Current.Session["USER_PK"]), "O",
                            TRAN);
                            UpdateCANExport(JobPk, BizType, MSTJCRefNum);
                        }
                        else
                        {
                            if (BizType == 2)
                            {
                                RollbackProtocolKey("CAN (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                            }
                            else
                            {
                                RollbackProtocolKey("CAN (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                            }
                            TRAN.Rollback();
                        }
                    }
                    //' For Rcnt = 0 To DSSave.Tables(0).Rows.Count - 1
                }
                //' If DSSave.Tables(0).Rows.Count > 0 Then

                if (string.Compare(arrMessage[0].ToString(), "Saved") > 0)
                {
                    TRAN.Commit();
                    return arrMessage;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                if (BizType == 2)
                {
                    RollbackProtocolKey("CAN (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                else
                {
                    RollbackProtocolKey("CAN (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }
        #endregion

        #region "FetchData"

        public DataSet GetData(string JobPk, Int16 BizType, string Place = "123")
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                if (string.IsNullOrEmpty(Place))
                {
                    Place = "123";
                }

                var _with11 = objWK.MyCommand;
                _with11.CommandType = CommandType.StoredProcedure;
                if (BizType == 2)
                {
                    _with11.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_SEA_FETCH_DATA1";
                }
                else
                {
                    _with11.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_AIR_FETCH_DATA1";
                }

                objWK.MyCommand.Parameters.Clear();
                var _with12 = objWK.MyCommand.Parameters;

                _with12.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                _with12.Add("Place_IN", Place).Direction = ParameterDirection.Input;
                _with12.Add("CAN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
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
        public DataSet FetchData(string JobPk, Int16 BizType, string MASTER_PK = "0", string Place = "123")
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                if (string.IsNullOrEmpty(Place))
                {
                    Place = "123";
                }

                var _with13 = objWK.MyCommand;
                _with13.CommandType = CommandType.StoredProcedure;
                if (BizType == 2)
                {
                    _with13.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_SEA_FETCH_DATA";
                }
                else
                {
                    _with13.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_AIR_FETCH_DATA";
                }

                objWK.MyCommand.Parameters.Clear();
                var _with14 = objWK.MyCommand.Parameters;

                _with14.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                _with14.Add("Place_IN", Place).Direction = ParameterDirection.Input;
                _with14.Add("MASTER_PK_IN", MASTER_PK).Direction = ParameterDirection.Input;
                _with14.Add("CAN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
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

        #region "CheckPK"
        public Int16 CheckPk(string JobPk, Int32 BizTp, string MASTER_PK = "0")
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            string Check = null;

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with15 = objWK.MyCommand;
                _with15.CommandType = CommandType.StoredProcedure;

                if (BizTp == 2)
                {
                    _with15.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_SEA_CHECK";
                }
                else
                {
                    _with15.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.CAN_AIR_CHECK";
                }


                objWK.MyCommand.Parameters.Clear();
                var _with16 = objWK.MyCommand.Parameters;

                _with16.Add("JOB_CARD_FK_IN", JobPk).Direction = ParameterDirection.Input;
                _with16.Add("MASTER_PK_IN", Convert.ToInt32(MASTER_PK)).Direction = ParameterDirection.Input;
                _with16.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                //Snigdharani - 15/12/2008 
                try
                {
                    objWK.MyCommand.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    return 0;
                }

                Check = Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);

                //If Check = "1" Then
                //Snigdharani - 15/12/2008
                if (Convert.ToInt32(Check) != 0)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
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
        public Int32 FetchPk(string JobRefNo, Int32 BizType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            string PK = null;
            Int32 Job_PK = default(Int32);
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                if (string.IsNullOrEmpty(JobRefNo))
                {
                    JobRefNo = "1111";
                }

                var _with17 = objWK.MyCommand;
                _with17.CommandType = CommandType.StoredProcedure;


                _with17.CommandText = objWK.MyUserName + ".CAN_TBL_PKG.FETCHPK";



                objWK.MyCommand.Parameters.Clear();
                var _with18 = objWK.MyCommand.Parameters;

                _with18.Add("JOB_CARD_REF_IN", JobRefNo).Direction = ParameterDirection.Input;
                _with18.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;


                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                PK = Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                Int32 value = 0;
                if (Int32.TryParse(PK, out value))
                {
                    if (value > 0)
                    {
                        Job_PK = Convert.ToInt32(PK);
                    }
                }
                else
                {
                    Job_PK = 0;
                }
                return Job_PK;
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

        #region "CAN Listing"
        public DataSet FetchCANListing(Int32 txtVslVoyPK = 0, string txtVoyID = "", Int32 lblPODPK = 0, int txtConsigneePK = 0, int txtMJCPK = 0, int txtPFDPK = 0, string txtCustomsNr = "", int txtCanPK = 0, int txtJCPK = 0, int BizType = 0,
        int Status = 0, string ETADate = "", string ATADate = "", int CurrentPage = 0, int TotalPage = 0, int BCurrencyPK = 0, Int32 LocFk = 0, string txtConsignee = "", string SearchType = "")
        {
            Int32 Last = default(Int32);
            Int32 Start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (BizType == 2)
            {
                if (Status == 1)
                {
                    sb.Append("SELECT JC.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("                       (CASE");
                    sb.Append("                         WHEN (NVL(VST.VESSEL_NAME, '') || '/' ||");
                    sb.Append("                              NVL(VVT.VOYAGE, '') = '/') THEN");
                    sb.Append("                          ''");
                    sb.Append("                         ELSE");
                    sb.Append("                          NVL(VST.VESSEL_NAME, '') || '/' ||");
                    sb.Append("                          NVL(VVT.VOYAGE, '')");
                    sb.Append("                       END) AS VESVOYAGE,");
                    sb.Append("                       ''AIRFLIGHT,");
                    sb.Append("                       JC.JOBCARD_REF_NO JOBREFNO,");
                    sb.Append("                       JC.HBL_HAWB_REF_NO,");
                    sb.Append("                       '' HAWB_REF_NO,");
                    // sb.Append("                       CMT.CUSTOMER_NAME CONSIGNEE,")
                    sb.Append("         nvl( CMT.CUSTOMER_NAME ,TEMP_CONS.CUSTOMER_NAME) AS  CONSIGNEE,");
                    sb.Append("                       JC.PORT_MST_POD_FK PODFK,");
                    sb.Append("                       POD.PORT_ID POD,");
                    sb.Append("                       '' AOD,");
                    sb.Append("                       JC.PFD_FK,");
                    sb.Append("                       PFD.PORT_ID PFD,");
                    sb.Append("                       '' AFD,");
                    sb.Append("                       JC.ETA_DATE ETA,");
                    sb.Append("                       JC.ARRIVAL_DATE ATA,");
                    sb.Append("                       '' CUSTOM_REF_NO,");
                    sb.Append("                       '' CUSTOM_REF_DT,");
                    sb.Append("                       '' CUSTOM_ITEM_NR,");
                    sb.Append("                       '' CAN_PK,");
                    sb.Append("                       '' CAN_REF_NO,");
                    sb.Append("                       '' CAN_DATE,");
                    sb.Append("                       '' BTNCLAUSE,");
                    sb.Append("                       '' SEL");
                    sb.Append("                  FROM JOB_CARD_TRN  JC,");
                    sb.Append("   TEMP_CUSTOMER_TBL TEMP_CONS,");
                    sb.Append("                       MASTER_JC_SEA_IMP_TBL M,");
                    sb.Append("                       VESSEL_VOYAGE_TRN     VVT,");
                    sb.Append("                       VESSEL_VOYAGE_TBL     VST,");
                    sb.Append("                       PORT_MST_TBL          POD,");
                    sb.Append("                       PORT_MST_TBL          PFD,");
                    sb.Append("                       CUSTOMER_MST_TBL      CMT,");
                    sb.Append("                       CAN_MST_TBL           CAN,");
                    sb.Append("                       HBL_EXP_TBL           HET");
                    sb.Append("                 WHERE JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK(+)");
                    sb.Append("                   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                    sb.Append("                   AND PFD.PORT_MST_PK(+) = JC.PFD_FK");
                    sb.Append("                   AND JC.JOB_CARD_TRN_PK = HET.JOB_CARD_SEA_EXP_FK(+)");
                    sb.Append("                   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                    sb.Append("                   AND JC.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                    sb.Append("                   AND JC.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                    sb.Append("                   AND JC.JOB_CARD_STATUS = 1");
                    sb.Append("                   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                    sb.Append("                   AND CAN.JOB_CARD_FK IS NULL");
                    sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK=TEMP_CONS.CUSTOMER_MST_PK(+)");
                    sb.Append("                   AND VVT.VOYAGE_TRN_PK IS NOT NULL");
                    sb.Append("                   AND POD.LOCATION_MST_FK =");
                    sb.Append("                       (SELECT L.LOCATION_MST_PK");
                    sb.Append("                          FROM LOCATION_MST_TBL L");
                    sb.Append("                         WHERE L.LOCATION_MST_PK =" + LocFk + " )");
                    sb.Append("                 AND JC.BUSINESS_TYPE = 2 AND JC.PROCESS_TYPE = 2 ");
                }
                else
                {
                    sb.Append("SELECT JC.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("                       (CASE");
                    sb.Append("                         WHEN (NVL(VST.VESSEL_NAME, '') || '/' ||");
                    sb.Append("                              NVL(VVT.VOYAGE, '') = '/') THEN");
                    sb.Append("                          ''");
                    sb.Append("                         ELSE");
                    sb.Append("                          NVL(VST.VESSEL_NAME, '') || '/' ||");
                    sb.Append("                          NVL(VVT.VOYAGE, '')");
                    sb.Append("                       END) AS VESVOYAGE,");
                    sb.Append("                       ''AIRFLIGHT,");
                    sb.Append("                       JC.JOBCARD_REF_NO JOBREFNO,");
                    sb.Append("                       JC.HBL_HAWB_REF_NO,");
                    sb.Append("                       '' HAWB_REF_NO,");
                    sb.Append("         nvl( CMT.CUSTOMER_NAME ,TEMP_CONS.CUSTOMER_NAME) AS  CONSIGNEE,");
                    // sb.Append("                       CMT.CUSTOMER_NAME CONSIGNEE,")
                    sb.Append("                       JC.PORT_MST_POD_FK PODFK,");
                    sb.Append("                       POD.PORT_ID POD,");
                    sb.Append("                       '' AOD,");
                    sb.Append("                       JC.PFD_FK,");
                    sb.Append("                       PFD.PORT_ID PFD,");
                    sb.Append("                       '' AFD,");
                    sb.Append("                       JC.ETA_DATE ETA,");
                    sb.Append("                       JC.ARRIVAL_DATE ATA,");
                    sb.Append("                       CAN.CUSTOM_REF_NO,");
                    sb.Append("                       TO_DATE(CAN.CUSTOM_REF_DT,DATEFORMAT) CUSTOM_REF_DT,");
                    sb.Append("                       CAN.CUSTOM_ITEM_NR,");
                    sb.Append("                       CAN.CAN_PK,");
                    sb.Append("                       CAN.CAN_REF_NO,");
                    sb.Append("                       CAN.CAN_DATE CAN_DATE,");
                    sb.Append("                       '' BTNCLAUSE,");
                    sb.Append("                       '' SEL");
                    sb.Append("                  FROM JOB_CARD_TRN  JC,");
                    sb.Append("   TEMP_CUSTOMER_TBL TEMP_CONS,");
                    sb.Append("                       MASTER_JC_SEA_IMP_TBL M,");
                    sb.Append("                       VESSEL_VOYAGE_TRN     VVT,");
                    sb.Append("                       VESSEL_VOYAGE_TBL     VST,");
                    sb.Append("                       PORT_MST_TBL          POD,");
                    sb.Append("                       PORT_MST_TBL          PFD,");
                    sb.Append("                       CUSTOMER_MST_TBL      CMT,");
                    sb.Append("                       CAN_MST_TBL           CAN,");
                    sb.Append("                       HBL_EXP_TBL           HET");
                    sb.Append("                 WHERE JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK(+)");
                    sb.Append("                   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                    sb.Append("                   AND PFD.PORT_MST_PK(+) = JC.PFD_FK");
                    sb.Append("                   AND JC.JOB_CARD_TRN_PK = HET.JOB_CARD_SEA_EXP_FK(+)");
                    sb.Append("                   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                    sb.Append("                   AND JC.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                    sb.Append("                   AND JC.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                    sb.Append("                   AND JC.JOB_CARD_STATUS = 1");
                    sb.Append("                   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                    sb.Append("                   AND CAN.JOB_CARD_FK IS NOT NULL");
                    sb.Append("                   AND VVT.VOYAGE_TRN_PK IS NOT NULL");
                    sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK=TEMP_CONS.CUSTOMER_MST_PK(+)");
                    sb.Append("                   AND POD.LOCATION_MST_FK =");
                    sb.Append("                       (SELECT L.LOCATION_MST_PK");
                    sb.Append("                          FROM LOCATION_MST_TBL L");
                    sb.Append("                         WHERE L.LOCATION_MST_PK =" + LocFk + " )");
                    sb.Append("                 AND JC.BUSINESS_TYPE = 2 AND JC.PROCESS_TYPE = 2 ");
                }
                if (txtVslVoyPK != 0)
                {
                    sb.Append(" AND VVT.VOYAGE_TRN_PK = " + txtVslVoyPK);
                }
                if (!string.IsNullOrEmpty(txtVoyID))
                {
                    sb.Append(" AND VVT.VOYAGE = '" + txtVoyID + "'");
                }
                if (lblPODPK != 0)
                {
                    sb.Append(" AND POD.PORT_MST_PK = " + lblPODPK);
                }
                if (txtConsigneePK != 0)
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK = " + txtConsigneePK);
                }
                if (txtMJCPK != 0)
                {
                    sb.Append(" AND M.MASTER_JC_SEA_IMP_PK = " + txtMJCPK);
                }
                if (txtPFDPK != 0)
                {
                    sb.Append(" AND PFD.PORT_MST_PK = " + txtPFDPK);
                }
                if (txtCanPK != 0)
                {
                    sb.Append(" AND CAN.CAN_PK = " + txtCanPK);
                }
                if (txtJCPK != 0)
                {
                    sb.Append(" AND JC.JOB_CARD_TRN_PK = " + txtJCPK);
                }
                if (!string.IsNullOrEmpty(ETADate))
                {
                    sb.Append(" And TO_DATE(JC.ETA_DATE,DATEFORMAT) = TO_DATE('" + ETADate + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ATADate))
                {
                    sb.Append(" And TO_DATE(JC.ARRIVAL_DATE,DATEFORMAT) = TO_DATE('" + ATADate + "',dateformat)");
                }
                if (txtCustomsNr.Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        sb.Append(" AND UPPER(CAN.CUSTOM_REF_NO) LIKE '" + txtCustomsNr.ToUpper().Replace("'", "''") + "%'");
                    }
                    else
                    {
                        sb.Append(" AND UPPER(CAN.CUSTOM_REF_NO) LIKE '%" + txtCustomsNr.ToUpper().Replace("'", "''") + "%'");
                    }
                }
            }
            else
            {
                if (Status == 1)
                {
                    sb.Append("SELECT JC.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("       '' VESVOYAGE,");
                    sb.Append("       (CASE");
                    sb.Append("         WHEN (NVL(AMT.AIRLINE_NAME, '') || '/' || NVL(JC.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                    sb.Append("          ''");
                    sb.Append("         ELSE");
                    sb.Append("          NVL(AMT.AIRLINE_NAME, '') || '/' || NVL(JC.VOYAGE_FLIGHT_NO, '')");
                    sb.Append("       END) AS AIRFLIGHT,");
                    sb.Append("       ");
                    sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                    sb.Append("       '' HBL_REF_NO,");
                    sb.Append("       JC.HBL_HAWB_REF_NO,");
                    sb.Append("         nvl( CMT.CUSTOMER_NAME ,TEMP_CONS.CUSTOMER_NAME) AS  CONSIGNEE,");
                    //  sb.Append("       CMT.CUSTOMER_NAME CONSIGNEE,")
                    sb.Append("       JC.PORT_MST_POD_FK PODFK,");
                    sb.Append("       '' POD,");
                    sb.Append("       POD.PORT_ID AOD,");
                    sb.Append("       JC.DEL_PLACE_MST_FK PFD_FK,");
                    sb.Append("       '' PFD,");
                    sb.Append("       PFD.PORT_ID AFD,");
                    sb.Append("       JC.ETA_DATE ETA,");
                    sb.Append("       JC.ARRIVAL_DATE ATA,");
                    sb.Append("       '' CUSTOM_REF_NO,");
                    sb.Append("       '' CUSTOM_REF_DT,");
                    sb.Append("       '' CUSTOM_ITEM_NR,");
                    sb.Append("       '' CAN_PK,");
                    sb.Append("       '' CAN_REF_NO,");
                    sb.Append("       '' CAN_DATE,");
                    sb.Append("       '' BTNCLAUSE,");
                    sb.Append("       '' SEL");
                    sb.Append("  FROM JOB_CARD_TRN  JC,");
                    sb.Append("   TEMP_CUSTOMER_TBL TEMP_CONS,");
                    sb.Append("       MASTER_JC_AIR_IMP_TBL M,");
                    sb.Append("       AIRLINE_MST_TBL       AMT,");
                    sb.Append("       PORT_MST_TBL          POD,");
                    sb.Append("       PORT_MST_TBL          PFD,");
                    sb.Append("       CUSTOMER_MST_TBL      CMT,");
                    sb.Append("       CAN_MST_TBL           CAN,");
                    sb.Append("       HAWB_EXP_TBL          HET");
                    sb.Append(" WHERE JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK(+)");
                    sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                    sb.Append("   AND PFD.PORT_MST_PK(+) = JC.DEL_PLACE_MST_FK");
                    sb.Append("   AND JC.JOB_CARD_TRN_PK = HET.JOB_CARD_AIR_EXP_FK(+)");
                    sb.Append("   AND JC.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+)");
                    sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                    sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                    sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                    sb.Append("   AND CAN.JOB_CARD_FK IS NULL");
                    sb.Append("   AND AMT.AIRLINE_MST_PK IS NOT NULL");
                    sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK=TEMP_CONS.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND POD.LOCATION_MST_FK =");
                    sb.Append("       (SELECT L.LOCATION_MST_PK");
                    sb.Append("          FROM LOCATION_MST_TBL L");
                    sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                    sb.Append("                 AND JC.BUSINESS_TYPE = 1 AND JC.PROCESS_TYPE = 2 ");
                }
                else
                {
                    sb.Append("SELECT JC.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("       '' VESVOYAGE,");
                    sb.Append("       (CASE");
                    sb.Append("         WHEN (NVL(AMT.AIRLINE_NAME, '') || '/' || NVL(JC.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                    sb.Append("          ''");
                    sb.Append("         ELSE");
                    sb.Append("          NVL(AMT.AIRLINE_NAME, '') || '/' || NVL(JC.VOYAGE_FLIGHT_NO, '')");
                    sb.Append("       END) AS AIRFLIGHT,");
                    sb.Append("       ");
                    sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                    sb.Append("       '' HBL_REF_NO,");
                    sb.Append("       JC.HBL_HAWB_REF_NO,");
                    sb.Append("         nvl( CMT.CUSTOMER_NAME ,TEMP_CONS.CUSTOMER_NAME) AS  CONSIGNEE,");
                    // sb.Append("       CMT.CUSTOMER_NAME CONSIGNEE,")
                    sb.Append("       JC.PORT_MST_POD_FK PODFK,");
                    sb.Append("       '' POD,");
                    sb.Append("       POD.PORT_ID AOD,");
                    sb.Append("       JC.DEL_PLACE_MST_FK PFD_FK,");
                    sb.Append("       '' PFD,");
                    sb.Append("       PFD.PORT_ID AFD,");
                    sb.Append("       JC.ETA_DATE ETA,");
                    sb.Append("       JC.ARRIVAL_DATE ATA,");
                    sb.Append("       CAN.CUSTOM_REF_NO,");
                    sb.Append("       TO_DATE(CAN.CUSTOM_REF_DT,DATEFORMAT) CUSTOM_REF_DT,");
                    sb.Append("       CAN.CUSTOM_ITEM_NR,");
                    sb.Append("       CAN.CAN_PK,");
                    sb.Append("       CAN.CAN_REF_NO,");
                    sb.Append("       CAN.CAN_DATE CAN_DATE,");
                    sb.Append("       '' BTNCLAUSE,");
                    sb.Append("       '' SEL");
                    sb.Append("  FROM JOB_CARD_TRN  JC,");
                    sb.Append("   TEMP_CUSTOMER_TBL TEMP_CONS,");
                    sb.Append("       MASTER_JC_AIR_IMP_TBL M,");
                    sb.Append("       AIRLINE_MST_TBL       AMT,");
                    sb.Append("       PORT_MST_TBL          POD,");
                    sb.Append("       PORT_MST_TBL          PFD,");
                    sb.Append("       CUSTOMER_MST_TBL      CMT,");
                    sb.Append("       CAN_MST_TBL           CAN,");
                    sb.Append("       HAWB_EXP_TBL          HET");
                    sb.Append(" WHERE JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK(+)");
                    sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                    sb.Append("   AND PFD.PORT_MST_PK(+) = JC.DEL_PLACE_MST_FK");
                    sb.Append("   AND JC.JOB_CARD_TRN_PK = HET.JOB_CARD_AIR_EXP_FK(+)");
                    sb.Append("   AND JC.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+)");
                    sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                    sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                    sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                    sb.Append("   AND CAN.JOB_CARD_FK IS NOT NULL");
                    sb.Append("   AND AMT.AIRLINE_MST_PK IS NOT NULL");
                    sb.Append("   AND JC.CONSIGNEE_CUST_MST_FK=TEMP_CONS.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND POD.LOCATION_MST_FK =");
                    sb.Append("       (SELECT L.LOCATION_MST_PK");
                    sb.Append("          FROM LOCATION_MST_TBL L");
                    sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                    sb.Append("         AND JC.BUSINESS_TYPE = 1 AND JC.PROCESS_TYPE = 2 ");
                }
                if (txtVslVoyPK != 0)
                {
                    sb.Append(" AND AMT.AIRLINE_MST_PK = " + txtVslVoyPK);
                }
                if (!string.IsNullOrEmpty(txtVoyID))
                {
                    sb.Append(" AND JC.VOYAGE_FLIGHT_NO= '" + txtVoyID + "'");
                }
                if (lblPODPK != 0)
                {
                    sb.Append(" AND POD.PORT_MST_PK = " + lblPODPK);
                }
                if (txtConsigneePK != 0)
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK = " + txtConsigneePK);
                }
                if (txtMJCPK != 0)
                {
                    sb.Append(" AND M.MASTER_JC_AIR_IMP_PK = " + txtMJCPK);
                }
                if (txtPFDPK != 0)
                {
                    sb.Append(" AND PFD.PORT_MST_PK = " + txtPFDPK);
                }
                if (txtCanPK != 0)
                {
                    sb.Append(" AND CAN.CAN_PK = " + txtCanPK);
                }
                if (txtJCPK != 0)
                {
                    sb.Append(" AND JC.JOB_CARD_TRN_PK = " + txtJCPK);
                }
                if (!string.IsNullOrEmpty(ETADate))
                {
                    sb.Append(" And TO_DATE(JC.ETA_DATE,DATEFORMAT) = TO_DATE('" + ETADate + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ATADate))
                {
                    sb.Append(" And TO_DATE(JC.ARRIVAL_DATE,DATEFORMAT) = TO_DATE('" + ATADate + "',dateformat)");
                }
                if (txtCustomsNr.Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        sb.Append(" AND UPPER(CAN.CUSTOM_REF_NO) LIKE '" + txtCustomsNr.ToUpper().Replace("'", "''") + "%'");
                    }
                    else
                    {
                        sb.Append(" AND UPPER(CAN.CUSTOM_REF_NO) LIKE '%" + txtCustomsNr.ToUpper().Replace("'", "''") + "%'");
                    }
                }
            }

            DataTable tbl = new DataTable();
            tbl = objWF.GetDataTable(sb.ToString());
            TotalRecords = (Int32)tbl.Rows.Count;
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;
            Last = CurrentPage * RecordsPerPage;
            Start = (CurrentPage - 1) * RecordsPerPage + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SLNO\", QRY.* FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            if (BizType == 1)
            {
                sqlstr.Append("   ORDER BY JC.JOBCARD_REF_NO Desc, TO_DATE(JC.ETA_DATE),AIRFLIGHT, HET.HAWB_REF_NO) QRY )Q ");
            }
            else
            {
                sqlstr.Append("   ORDER BY JC.JOBCARD_REF_NO Desc, TO_DATE(JC.ETA_DATE),VESVOYAGE, HET.HBL_REF_NO) QRY )Q ");
            }
            sqlstr.Append("  WHERE Q.SLNO  BETWEEN " + Start + " AND " + Last + "");
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #region "Fetching CreditPolicy Details based on Shipper"
        public DataSet fetchVsl(string JobPK, int BizType)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            if (BizType == 2)
            {
                sb.Append("SELECT VST.VESSEL_ID, VST.VESSEL_NAME, VVT.VOYAGE");
                sb.Append("  FROM JOB_CARD_TRN JC, VESSEL_VOYAGE_TRN VVT, VESSEL_VOYAGE_TBL VST");
                sb.Append(" WHERE VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JC.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JC.JOB_CARD_TRN_PK IN (" + JobPK + ")");
                sb.Append("   AND JC.VOYAGE_TRN_FK IS NOT NULL");
            }
            else
            {
                sb.Append("SELECT AMT.AIRLINE_ID VESSEL_ID, AMT.AIRLINE_NAME VESSEL_NAME, JC.VOYAGE_FLIGHT_NO VOYAGE");
                sb.Append("  FROM JOB_CARD_TRN JC, AIRLINE_MST_TBL AMT");
                sb.Append(" WHERE JC.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND JC.JOB_CARD_TRN_PK IN (" + JobPK + ")");
            }
            try
            {
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
        #endregion

        public void UpdateCANExport(int JOB_CARD_PK, int Biz_Type, string RefNr)
        {
            WorkFlow objWK = new WorkFlow();
            int JCCount = 0;
            int JCPK = 0;
            JCCount = Convert.ToInt32(objWK.ExecuteScaler("select count (*) from job_card_trn jct where jct.jobcard_ref_no in (select jc.jobcard_ref_no from job_card_trn jc where jc.job_card_trn_pk=" + JOB_CARD_PK + ") and jct.process_type=1"));
            if (JCCount > 0)
            {
                JCPK = Convert.ToInt32(objWK.ExecuteScaler("select jct.job_card_trn_pk from job_card_trn jct where jct.jobcard_ref_no in (select jc.jobcard_ref_no from job_card_trn jc where jc.job_card_trn_pk=" + JOB_CARD_PK + ") and jct.process_type=1 and rownum=1"));
            }
            else
            {
                return;
            }
            objWK.OpenConnection();
            OracleTransaction Tran = null;
            Tran = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with19 = objWK.MyCommand;
                _with19.CommandType = CommandType.StoredProcedure;
                _with19.CommandText = objWK.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS_EXT";
                _with19.Transaction = Tran;
                _with19.Parameters.Clear();
                _with19.Parameters.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("KEY_FK_IN", JCPK).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("LOCATION_FK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("STATUS_IN", "Cargo Arrival Notice Generated").Direction = ParameterDirection.Input;
                _with19.Parameters.Add("CREATEDUSER_IN", Convert.ToInt32(HttpContext.Current.Session["USER_PK"])).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("DOC_REF_IN", RefNr).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("CREATED_DT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                _with19.ExecuteNonQuery();
                Tran.Commit();
            }
            catch (OracleException oraexp)
            {
                Tran.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                Tran.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }


        #region "CanRemUpdate Function"
        public ArrayList CAN_REM_UPD(int JOB_FK, string REM_DATE, int OPTVAL)
        {
            WorkFlow objWK = new WorkFlow();
            int RetVal = 0;
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();
            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                var _with20 = updCommand;
                _with20.Connection = objWK.MyConnection;
                _with20.CommandType = CommandType.StoredProcedure;
                _with20.CommandText = objWK.MyUserName + (".CAN_TBL_PKG.CAN_REM_UPD");
                _with20.Parameters.Clear();
                _with20.Parameters.Add("JC_PK_IN", JOB_FK).Direction = ParameterDirection.Input;
                if (!string.IsNullOrEmpty(REM_DATE))
                {
                    _with20.Parameters.Add("DATE_IN", Convert.ToDateTime(REM_DATE)).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with20.Parameters.Add("DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                _with20.Parameters.Add("OPTVAL_IN", OPTVAL).Direction = ParameterDirection.Input;
                _with20.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with20.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with21 = objWK.MyDataAdapter;
                _with21.UpdateCommand = updCommand;
                _with21.UpdateCommand.Transaction = TRAN;
                RecAfct = _with21.UpdateCommand.ExecuteNonQuery();
                RetVal = Convert.ToInt32(objWK.MyDataAdapter.UpdateCommand.Parameters["RETURN_VALUE"].Value);
                if (RecAfct > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    arrMessage.Add(RetVal);
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
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
    }
}