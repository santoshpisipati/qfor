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
    public class clsTransporterCntctFCL : CommonFeatures
	{
		System.Text.StringBuilder strQuery;
		WorkFlow objWrkFlow = new WorkFlow();
		DataSet ds = new DataSet();

		string strPortpair;
		#region "Properties"
		public DataSet DatasetMain {
			get { return ds; }
			set { ds = value; }
		}
		long locLogin;
		public long employeeId {
			get { return locLogin; }
			set { locLogin = value; }
		}
		public string PortPair {
			get { return strPortpair; }
			set { strPortpair = value; }
		}
		bool saved;
		public bool isChildRecSaved {
			get { return saved; }
			set { saved = value; }
		}
		#endregion

		#region "Fetch Transaction"
		WorkFlow objWF = new WorkFlow();
		public DataTable FetchCONT_TRANS_FCL_TBL(int CONT_TRANS_FCL_PK = 0)
		{

			try {
				DataTable dt = null;
				objWF.MyCommand.Parameters.Clear();
				var _with1 = objWF.MyCommand.Parameters;
				_with1.Add("CONT_TRANS_FCL_PK_IN", CONT_TRANS_FCL_PK).Direction = ParameterDirection.Input;
				_with1.Add("CONT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dt = objWF.GetDataTable("CONT_TRANS_FCL_TBL_PKG", "FETCH_CONT_TRANS_FCL_TBL");
				return dt;
			} catch (OracleException ex) {
				throw ex;
			} catch (Exception generalEx) {
				throw generalEx;
			}
		}
		public DataTable FetchCONT_TRANS_FCL_TRN(int CONT_TRANS_FCL_FK = 0)
		{
			try {
				DataTable dt = null;
				objWF.MyCommand.Parameters.Clear();
				var _with2 = objWF.MyCommand.Parameters;
				_with2.Add("CONT_TRANS_FCL_FK_IN", CONT_TRANS_FCL_FK).Direction = ParameterDirection.Input;
				_with2.Add("CONT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dt = objWF.GetDataTable("CONT_TRANS_FCL_TBL_PKG", "FETCH_CONT_TRANS_FCL_TRN");
				return dt;
			} catch (OracleException ex) {
				throw ex;
			} catch (Exception generalEx) {
				throw generalEx;
			}
		}
		public DataTable FetchCONT_TRANS_FCL_SLAB(int CONT_TRANS_FCL_TRN_FK = 0)
		{

			try {
				DataTable dt = null;
				objWF.MyCommand.Parameters.Clear();
				var _with3 = objWF.MyCommand.Parameters;
				_with3.Add("CONT_TRANS_FCL_TRN_FK_IN", CONT_TRANS_FCL_TRN_FK).Direction = ParameterDirection.Input;
				_with3.Add("CONT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dt = objWF.GetDataTable("CONT_TRANS_FCL_TBL_PKG", "FETCH_CONT_TRANS_FCL_SLAB");
				return dt;
			} catch (OracleException ex) {
				throw ex;
			} catch (Exception generalEx) {
				throw generalEx;
			}
		}

		#endregion

		#region "Save Function"
		public ArrayList SaveData(Int16 Int_wfstatus, int ContPk, string contRef, string strDelTrn, int prevConPk, bool AmendFlg = false)
		{
			WorkFlow objWS = new WorkFlow();
			objWS.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWS.MyConnection.BeginTransaction();
			Int64 intPkVal = default(Int64);
			isChildRecSaved = false;
			string strPk = null;
			try {
				if (AmendFlg == true) {
					ContPk = 0;
				}
				var _with4 = objWS.MyCommand;
				_with4.Connection = objWS.MyConnection;
				_with4.CommandType = CommandType.StoredProcedure;
				_with4.Transaction = TRAN;
				if (ContPk == 0) {
					if (string.IsNullOrEmpty(contRef)) {
						contRef = GenerateContractNo("SEA TRANSPORTER CONTRACT", LoggedIn_Loc_FK, employeeId, Convert.ToInt32(LAST_MODIFIED_BY), objWS);
						if (contRef == "Protocol Not Defined.") {
							arrMessage.Add("Protocol Not Defined.");
							return arrMessage;
						}
					}
					_with4.CommandText = objWS.MyUserName + ".CONT_TRANS_FCL_TBL_PKG.CONT_TRANS_FCL_TBL_INS";
					_with4.Parameters.Clear();
					_with4.Parameters.Add("CREATED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
					_with4.Parameters.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
				} else {
					isChildRecSaved = true;
					_with4.CommandText = objWS.MyUserName + ".CONT_TRANS_FCL_TBL_PKG.CONT_TRANS_FCL_TBL_UPD";
					_with4.Parameters.Clear();
					_with4.Parameters.Add("CONT_TRANS_FCL_PK_IN", DatasetMain.Tables[0].Rows[0]["CONT_TRANS_FCL_PK"]).Direction = ParameterDirection.Input;
					_with4.Parameters.Add("VERSION_NO_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
					_with4.Parameters.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;
					_with4.Parameters.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
				}
				_with4.Parameters.Add("TRANSPORTER_MST_FK_IN", DatasetMain.Tables[0].Rows[0]["TRANSPORTER_MST_FK"]).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("CONTRACT_NO_IN", contRef).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("CONTRACT_DATE_IN", DatasetMain.Tables[0].Rows[0]["CONTRACT_DATE"]).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("TRANSPORT_MODE_IN", DatasetMain.Tables[0].Rows[0]["TRANSPORT_MODE"]).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("COMMODITY_GROUP_FK_IN", DatasetMain.Tables[0].Rows[0]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("CONT_STATUS_IN", DatasetMain.Tables[0].Rows[0]["CONT_STATUS"]).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("VALID_FROM_IN", DatasetMain.Tables[0].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("VALID_TO_IN", DatasetMain.Tables[0].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("CURRENCY_MST_FK_IN", DatasetMain.Tables[0].Rows[0]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("ATTACHED_FILE_NAME_IN", DatasetMain.Tables[0].Rows[0]["ATTACHED_FILE_NAME"]).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("CREATE_DT_IN", M_CREATED_DT).Direction = ParameterDirection.Input;
				_with4.ExecuteNonQuery();

                if (int.Parse(objWS.MyCommand.Parameters["RETURN_VALUE"].Value.ToString()) > 0)
                {
                    arrMessage.Add(Convert.ToString(objWS.MyCommand.Parameters["RETURN_VALUE"].Value));
                    if (ContPk == 0)
                    {
                        RollbackProtocolKey("SEA TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contRef, System.DateTime.Now);
                    }
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    ContPk = Convert.ToInt32(objWS.MyCommand.Parameters["RETURN_VALUE"].Value);
                    arrMessage = SaveTrnData(ContPk, objWS.MyCommand, objWS.MyUserName, Convert.ToString(DatasetMain.Tables[0].Rows[0]["TRANSPORTER_MST_FK"]), Convert.ToString(DatasetMain.Tables[0].Rows[0]["TRANSPORT_MODE"]), Convert.ToString(DatasetMain.Tables[0].Rows[0]["COMMODITY_GROUP_FK"]));
                    //'
                    if (AmendFlg == true)
                    {
                        string str = null;
                        Int32 intIns = default(Int32);
                        OracleCommand updCmdUser = new OracleCommand();
                        updCmdUser.Transaction = TRAN;
                        System.DateTime ContractDate = default(System.DateTime);
                        ContractDate = GetContractDt(prevConPk);
                        if (ContractDate > DateTime.Today.Date)
                        {
                            str = " update CONT_TRANS_FCL_TBL CONFT SET CONFT.CONT_STATUS = 2, ";
                            str += " CONFT.AMEND_FLAG=1 ";
                            str += " WHERE CONFT.CONT_TRANS_FCL_PK=" + prevConPk;
                        }
                        else
                        {
                            str = " update CONT_TRANS_FCL_TBL CONFT SET CONFT.VALID_TO = SYSDATE ,";
                            str += " CONFT.AMEND_FLAG=1 ";
                            str += " WHERE CONFT.CONT_TRANS_FCL_PK=" + prevConPk;
                        }
                        var _with5 = updCmdUser;
                        _with5.Connection = objWS.MyConnection;
                        _with5.Transaction = TRAN;
                        _with5.CommandType = CommandType.Text;
                        _with5.CommandText = str;
                        intIns = _with5.ExecuteNonQuery();
                    }
                    //'
                    if (string.Compare(arrMessage[0].ToString(), "Saved") > 0 && isChildRecSaved)
                    {
                        if (strDelTrn != "-1")
                        {
                            arrMessage.Clear();
                        }
                        if (string.Compare(arrMessage[0].ToString(), "Saved") > 0)
                        {
                            TRAN.Commit();
                            arrMessage.Clear();
                            arrMessage.Add("All Data Saved Successfully");
                            return arrMessage;
                        }
                        else
                        {
                            if (ContPk == 0)
                            {
                                RollbackProtocolKey("SEA TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contRef, System.DateTime.Now);
                                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                            }
                            TRAN.Rollback();
                            return arrMessage;
                        }
                    }
                    else
                    {
                        if (ContPk == 0)
                        {
                            RollbackProtocolKey("SEA TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contRef, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				if (ContPk == 0) {
					RollbackProtocolKey("SEA TRANSPORTER CONTRACT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), contRef, System.DateTime.Now);
					//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
				}
				TRAN.Rollback();
				arrMessage.Add(ex.Message);
				return arrMessage;
			} finally {
				objWS.CloseConnection();
			}
		}
		public ArrayList SaveTrnData(int mainPk, OracleCommand Cmd, string userName, string tranPk, string mode, string com)
		{
			int nRowCnt = 0;
			int nRowCnt1 = 0;
			int inPk = 0;
			string strUnique = null;
			string strUniqueCheck = null;
			string TrnPK = "0,";
			string SlabPK = "0,";
			string[] arrAllData = null;
			string[] arrRowData = null;
			string strData = null;

			try {
				arrMessage.Clear();
				for (nRowCnt = 0; nRowCnt <= DatasetMain.Tables[1].Rows.Count - 1; nRowCnt++) {
					TrnPK = TrnPK + getDefault(DatasetMain.Tables[1].Rows[nRowCnt]["CONT_TRANS_FCL_TRN_PK"], 0) + ",";

					strData = Convert.ToString(getDefault(DatasetMain.Tables[1].Rows[nRowCnt]["NUM"], ""));
					arrAllData = strData.Split(Convert.ToChar("@~"));

					for (int i = 1; i <= arrAllData.Length - 1; i++) {
						arrRowData = arrAllData[i].Split('~');
						if (arrRowData.Length >= 7) {
							SlabPK = SlabPK + getDefault(arrRowData[7], 0) + ",";
						}
					}
				}
				if (!string.IsNullOrEmpty(TrnPK)) {
					TrnPK = TrnPK.PadLeft(TrnPK.Length-1);
				}
				if (!string.IsNullOrEmpty(SlabPK)) {
					SlabPK = SlabPK.PadLeft(SlabPK.Length - 1);
                }
				arrMessage = DeleteTran(Convert.ToString(mainPk), TrnPK, SlabPK, Cmd, userName);

				var _with6 = Cmd;
				_with6.CommandType = CommandType.StoredProcedure;
				for (nRowCnt = 0; nRowCnt <= DatasetMain.Tables[1].Rows.Count - 1; nRowCnt++) {
					arrMessage.Clear();
					_with6.Parameters.Clear();
					if (Convert.ToInt32(getDefault(DatasetMain.Tables[1].Rows[nRowCnt]["CONT_TRANS_FCL_TRN_PK"], 0)) == 0 || string.IsNullOrEmpty(Convert.ToString(getDefault(DatasetMain.Tables[1].Rows[nRowCnt]["CONT_TRANS_FCL_TRN_PK"], "")))) {
						strUnique = "E~" + tranPk;
						strUnique += "~" + DatasetMain.Tables[1].Rows[nRowCnt]["FROM_PORT_FK"];
						strUnique += "~" + DatasetMain.Tables[1].Rows[nRowCnt]["TO_PORT_FK"];
						strUnique += "~" + DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_TYPE_MST_PK"];
						strUnique += "~" + DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_STATUS"];
						strUnique += "~" + DatasetMain.Tables[1].Rows[nRowCnt]["COMMODITY_MST_PK"];
						strUnique += "~" + DatasetMain.Tables[1].Rows[nRowCnt]["VALID_FROM"];
						strUnique += "~" + DatasetMain.Tables[1].Rows[nRowCnt]["VALID_TO"];
						strUnique += "~" + DatasetMain.Tables[1].Rows[nRowCnt]["CONT_TRANS_FCL_TRN_PK"];
						strUnique += "~" + mode;
						strUnique += "~" + com;
						strUniqueCheck = CheckUnique(strUnique);
						PortPair += strUniqueCheck;
						if (strUniqueCheck != "0") {
							//goto vk;
						} else {
							PortPair = "";
						}

						_with6.CommandText = userName + ".CONT_TRANS_FCL_TBL_PKG.CONT_TRANS_FCL_TRN_INS";
						_with6.Parameters.Add("RETURN_VALUE", inPk).Direction = ParameterDirection.Output;
                        if (int.Parse(DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_TYPE_MST_ID"].ToString()) > 0)
                        {
                            _with6.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_TYPE_MST_PK"]).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with6.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_TYPE_MST_ID"]).Direction = ParameterDirection.Input;
                        }
                        if (int.Parse(DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_STATUS_ID"].ToString()) > 0) {
							_with6.Parameters.Add("CONTAINER_STATUS_IN", DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_STATUS"]).Direction = ParameterDirection.Input;
						} else {
							_with6.Parameters.Add("CONTAINER_STATUS_IN", DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_STATUS_ID"]).Direction = ParameterDirection.Input;
						}
						_with6.Parameters.Add("COMMODITY_MST_FK_IN", DatasetMain.Tables[1].Rows[nRowCnt]["COMMODITY_MST_PK"]).Direction = ParameterDirection.Input;

					} else {
						_with6.CommandText = userName + ".CONT_TRANS_FCL_TBL_PKG.CONT_TRANS_FCL_TRN_UPD";
						_with6.Parameters.Add("CONT_TRANS_FCL_TRN_PK_IN", DatasetMain.Tables[1].Rows[nRowCnt]["CONT_TRANS_FCL_TRN_PK"]).Direction = ParameterDirection.Input;
						_with6.Parameters.Add("RETURN_VALUE", inPk).Direction = ParameterDirection.Output;
						_with6.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_TYPE_MST_PK"]).Direction = ParameterDirection.Input;
						_with6.Parameters.Add("CONTAINER_STATUS_IN", DatasetMain.Tables[1].Rows[nRowCnt]["CONTAINER_STATUS"]).Direction = ParameterDirection.Input;
						_with6.Parameters.Add("COMMODITY_MST_FK_IN", DatasetMain.Tables[1].Rows[nRowCnt]["COMMODITY_MST_PK"]).Direction = ParameterDirection.Input;
					}
					_with6.Parameters.Add("CONT_TRANS_FCL_FK_IN", mainPk).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("FROM_PORT_FK_IN", DatasetMain.Tables[1].Rows[nRowCnt]["FROM_PORT_FK"]).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("TO_PORT_FK_IN", DatasetMain.Tables[1].Rows[nRowCnt]["TO_PORT_FK"]).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("VALID_FROM_IN", Convert.ToDateTime(DatasetMain.Tables[1].Rows[nRowCnt]["VALID_FROM"])).Direction = ParameterDirection.Input;
					if (string.IsNullOrEmpty(DatasetMain.Tables[1].Rows[nRowCnt]["VALID_TO"].ToString())) {
						_with6.Parameters.Add("VALID_TO_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with6.Parameters.Add("VALID_TO_IN", Convert.ToDateTime(DatasetMain.Tables[1].Rows[nRowCnt]["VALID_TO"])).Direction = ParameterDirection.Input;
					}
					_with6.ExecuteNonQuery();
                    if (int.Parse(Cmd.Parameters["RETURN_VALUE"].Value.ToString()) > 0) { 
						arrMessage.Add(Cmd.Parameters["RETURN_VALUE"].Value);
						return arrMessage;
					} else {
						inPk = Convert.ToInt32(Cmd.Parameters["RETURN_VALUE"].Value);
						arrMessage = SaveSlab(inPk, Cmd, userName, getDefault(DatasetMain.Tables[1].Rows[nRowCnt]["NUM"].ToString(), "").ToString());
						if (string.Compare(arrMessage[0].ToString(), "Saved")>0) {
							isChildRecSaved = true;
							arrMessage.Add("All Data Saved Successfully");
						} else {
							return arrMessage;
						}
					}
				}
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
		}
		#region "Delete Function"
		public ArrayList DeleteTran(string HdrPK, string TrnPK, string SlabPK, OracleCommand Cmd, string userName)
		{
			WorkFlow objWS = new WorkFlow();
			Int32 intPkVal = default(Int32);

			try {
				arrMessage.Clear();
				var _with7 = Cmd;
				_with7.CommandType = CommandType.StoredProcedure;
				_with7.CommandText = userName + ".CONT_TRANS_FCL_TBL_PKG.CONT_TRANS_FCL_TRN_DEL";

				_with7.Parameters.Clear();
				_with7.Parameters.Add("CONT_TRANS_FCL_PK_IN", HdrPK).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("CONT_TRANS_FCL_TRN_PK_IN", TrnPK).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("CONT_TRANS_FCL_SLAB_PK_IN", SlabPK).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
				_with7.ExecuteNonQuery();

                if (int.Parse(Cmd.Parameters["RETURN_VALUE"].Value.ToString()) > 0)
                {
                    arrMessage.Add(Cmd.Parameters["RETURN_VALUE"].Value);
                    return arrMessage;
                }

				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}

		}
        #endregion
        public string GenerateContractNo(string Protocol, Int64 ILocationId, Int64 IEmployeeId, int createdBy, object obj)
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey(Protocol, ILocationId, IEmployeeId, DateTime.Now, "", "", "", createdBy, (WorkFlow)obj);
            return functionReturnValue;
        }

		public ArrayList SaveSlab(int TrnPk, OracleCommand Cmd, string userName, string strData)
		{
			Int64 INTPKVAL = default(Int64);
			string[] arrAllData = null;
			string[] arrRowData = null;
			int i = 0;
			int j = 0;
			int exe = 0;
			try {
				arrMessage.Clear();
				arrAllData = strData.Split(Convert.ToChar("@~"));
				if (arrAllData.Length > 1) {
					for (i = 1; i <= arrAllData.Length - 1; i++) {
						arrMessage.Clear();
						arrRowData = arrAllData[i].Split('~');
						Cmd.CommandType = CommandType.StoredProcedure;
						var _with8 = Cmd.Parameters;
						_with8.Clear();

						if ((Convert.ToInt32(getDefault(arrRowData[7], 0)) > 0)) {
							Cmd.CommandText = userName + ".CONT_TRANS_FCL_TBL_PKG.CONT_TRANS_FCL_SLAB_UPD";
							_with8.Add("CONT_TRANS_FCL_SLAB_PK_IN", getDefault(arrRowData[7], "")).Direction = ParameterDirection.Input;
						} else {
							Cmd.CommandText = userName + ".CONT_TRANS_FCL_TBL_PKG.CONT_TRANS_FCL_SLAB_INS";
						}

						_with8.Add("CONT_TRANS_FCL_TRN_FK_IN", TrnPk).Direction = ParameterDirection.Input;
						_with8.Add("FROM_WEIGHT_IN", arrRowData[2]).Direction = ParameterDirection.Input;
						_with8.Add("TO_WEIGHT_IN", getDefault(arrRowData[3], "")).Direction = ParameterDirection.Input;
						_with8.Add("ONE_WAY_CHARGE_IN", getDefault(arrRowData[4], "")).Direction = ParameterDirection.Input;
						_with8.Add("RETURN_TRIP_CHARGE_IN", getDefault(arrRowData[5], "")).Direction = ParameterDirection.Input;
						_with8.Add("RETURN_VALUE", INTPKVAL).Direction = ParameterDirection.Output;
						exe = Cmd.ExecuteNonQuery();
						if (exe == 1) {
							arrMessage.Add("All Data Saved Successfully");
						}
					}
				} else {
					arrMessage.Add("All Data Saved Successfully");
				}
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
		}
		#endregion

		#region "Update File Name"
		public bool UpdateFileName(long TransPk, string strFileName, Int16 Flag)
		{
			if (strFileName.Trim().Length > 0) {
				string RemQuery = null;
				WorkFlow objwk = new WorkFlow();
				if (Flag == 1) {
					RemQuery = " UPDATE CONT_TRANS_FCL_TBL CMTT SET CMTT.ATTACHED_FILE_NAME='" + strFileName + "'";
					RemQuery += " WHERE CMTT.CONT_TRANS_FCL_PK = " + TransPk;
					try {
						objwk.OpenConnection();
						objwk.ExecuteCommands(RemQuery);
						return true;
					} catch (OracleException oraexp) {
						throw oraexp;
					} catch (Exception ex) {
						throw ex;
						return false;
					} finally {
						objwk.MyCommand.Connection.Close();
					}
				} else if (Flag == 2) {
					RemQuery = " UPDATE CONT_TRANS_FCL_TBL CMTT SET CMTT.ATTACHED_FILE_NAME='" + "" + "'";
					RemQuery += " WHERE CMTT.CONT_TRANS_FCL_PK = " + TransPk;
					try {
						objwk.OpenConnection();
						objwk.ExecuteCommands(RemQuery);
						return true;
					} catch (OracleException oraexp) {
						throw oraexp;
					} catch (Exception ex) {
						throw ex;
						return false;
					} finally {
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
			strSQL += " CMTT.ATTACHED_FILE_NAME FROM CONT_TRANS_FCL_TBL CMTT WHERE CMTT.CONT_TRANS_FCL_PK = " + TransPk;
			try {
				DataSet ds = null;
				strUpdFileName = objWF.ExecuteScaler(strSQL);
				return strUpdFileName;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Check Unique"
		public string CheckUnique(string strCond)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strReq = null;
			string TRANSPORTER_FK_IN = null;
			string FROM_PORT_FK_IN = null;
			string TO_PORT_FK_IN = null;
			string CONTAINER_TYPE_MST_FK_IN = null;
			string CONTAINER_STATUS_IN = null;
			string COMMODITY_MST_FK_IN = null;
			string VALID_FROM_IN = null;
			string VALID_TO_IN = null;
			string CONT_TRANS_FCL_TRN_PK = null;
			string COMMODITY_GRP_FK_IN = null;
			string Mode_in = null;
			string strPk = null;
			arr = strCond.Split('~');
			if (arr.Length > 0)
				strReq = Convert.ToString(arr.GetValue(0));
			if (arr.Length > 1)
				TRANSPORTER_FK_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
				FROM_PORT_FK_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
				TO_PORT_FK_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
				CONTAINER_TYPE_MST_FK_IN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
				CONTAINER_STATUS_IN = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
				COMMODITY_MST_FK_IN = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
				VALID_FROM_IN = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
				VALID_TO_IN = Convert.ToString(arr.GetValue(8));
            if (arr.Length > 9)
				CONT_TRANS_FCL_TRN_PK = Convert.ToString(arr.GetValue(9));
            if (arr.Length > 10)
				Mode_in = Convert.ToString(arr.GetValue(10));
            if (arr.Length > 11)
				COMMODITY_GRP_FK_IN = Convert.ToString(arr.GetValue(11));
            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".CONT_TRANS_FCL_TBL_PKG.CHECKUNIQUEKEY";
				var _with9 = selectCommand.Parameters;
				_with9.Add("TRANSPORTER_FK_IN", getDefault(TRANSPORTER_FK_IN, "")).Direction = ParameterDirection.Input;
				_with9.Add("FROM_PORT_FK_IN", getDefault(FROM_PORT_FK_IN, "")).Direction = ParameterDirection.Input;
				_with9.Add("TO_PORT_FK_IN", getDefault(TO_PORT_FK_IN, "")).Direction = ParameterDirection.Input;
				_with9.Add("CONTAINER_TYPE_MST_FK_IN", getDefault(CONTAINER_TYPE_MST_FK_IN, "")).Direction = ParameterDirection.Input;
				_with9.Add("COMMODITY_GRP_IN", getDefault(COMMODITY_GRP_FK_IN, 0)).Direction = ParameterDirection.Input;
				_with9.Add("CONTAINER_STATUS_IN", getDefault(CONTAINER_STATUS_IN, "")).Direction = ParameterDirection.Input;
				_with9.Add("COMMODITY_MST_FK_IN", getDefault((COMMODITY_MST_FK_IN == "null" ? "" : COMMODITY_MST_FK_IN), 0)).Direction = ParameterDirection.Input;
				_with9.Add("VALID_FROM_IN", getDefault(Convert.ToDateTime(VALID_FROM_IN), "")).Direction = ParameterDirection.Input;
				_with9.Add("VALID_TO_IN", getDefault(Convert.ToDateTime(getDefault(VALID_TO_IN, "31/12/9999")), "")).Direction = ParameterDirection.Input;
				_with9.Add("CONT_PK", getDefault(CONT_TRANS_FCL_TRN_PK, "")).Direction = ParameterDirection.Input;
				_with9.Add("MODE_IN", getDefault(Mode_in, 0)).Direction = ParameterDirection.Input;
				_with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();

			}
		}
		#endregion

		#region " Fetch Max Ref No."
		public string FetchRefNo(string strRFQNo)
		{
			try {
				string strSQL = null;
				strSQL = "SELECT NVL(MAX(T.CONTRACT_NO),0) FROM CONT_TRANS_FCL_TBL T " + "WHERE T.CONTRACT_NO LIKE '" + strRFQNo + "/%' " + "ORDER BY T.CONTRACT_NO";
				return (new WorkFlow()).ExecuteScaler(strSQL);
			} catch (OracleException oraexp) {
				throw oraexp;

			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Report Details"
		public DataSet FetchReportDetails(int contractPk)
		{
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				WorkFlow objWK = new WorkFlow();
				sb.Append("SELECT  CTFT.CONTRACT_DATE,");
				sb.Append("        VMT.VENDOR_NAME,");
				sb.Append("        VCD.ADM_ADDRESS_1,");
				sb.Append("        VCD.ADM_ADDRESS_2,");
				sb.Append("        VCD.ADM_ADDRESS_3,");
				sb.Append("        VCD.ADM_ZIP_CODE,");
				sb.Append("        VCD.ADM_CITY,");
				sb.Append("        'SEA' BIZ_TYPE,");
				sb.Append("       'FCL' CARGO_TYPE,");
				sb.Append("      DECODE(CTFT.TRANSPORT_MODE, 1, 'ROAD', 2, 'TRAIN') TRANSPORT_MODE,");
				sb.Append("       LUMT.USER_NAME APPBY_ID,");
				sb.Append("       CTFT.LAST_MODIFIED_DT,");
				sb.Append("       CTFT.CONTRACT_NO,");
				sb.Append("       CTFT.CONTRACT_DATE,");
				sb.Append("       CTFT.VALID_FROM,");
				sb.Append("       CTFT.VALID_TO,");
				sb.Append("       CGMT.COMMODITY_GROUP_DESC");
				sb.Append("        ");
				sb.Append("FROM CONT_TRANS_FCL_TBL CTFT,");
				sb.Append("     VENDOR_MST_TBL      VMT,");
				sb.Append("      VENDOR_TYPE_MST_TBL VTM,");
				sb.Append("       VENDOR_CONTACT_DTLS VCD,");
				sb.Append("       VENDOR_SERVICES_TRN VST,");
				sb.Append("       USER_MST_TBL        CUMT,");
				sb.Append("       USER_MST_TBL        LUMT,");
				sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
				sb.Append("     ");
				sb.Append("     WHERE CTFT.CONT_TRANS_FCL_PK = " + contractPk);
				sb.Append("      AND CTFT.TRANSPORTER_MST_FK = VMT.VENDOR_MST_PK");
				sb.Append("      AND VST.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
				sb.Append("   AND VST.VENDOR_TYPE_FK = VTM.VENDOR_TYPE_PK");
				sb.Append("   AND VCD.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
				sb.Append("   AND VTM.VENDOR_TYPE_ID = 'TRANSPORTER'");
				sb.Append("   AND CTFT.CREATED_BY_FK = CUMT.USER_MST_PK(+)");
				sb.Append("   AND CTFT.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+)");
				sb.Append("   AND CTFT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK");

				return objWK.GetDataSet(sb.ToString());
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#endregion

		#region "FetchFreightDetails"
		public DataSet FetchFreightDetails(int contractPk)
		{
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				WorkFlow objWK = new WorkFlow();
				sb.Append("SELECT POL.PORT_NAME PORT_ID,");
				sb.Append("       POD.PORT_NAME PORT_ID,");
				sb.Append("       CTMT.CONTAINER_TYPE_NAME CONTAINER_TYPE_MST_ID,");
				sb.Append("       DECODE(CONTAINER_STATUS, 1 ,'FULL', 2 ,'MTY') CONTAINER_STATUS,");
				sb.Append("       CMT.COMMODITY_ID,");
				sb.Append("       CTFF.VALID_FROM,");
				sb.Append("       CTFF.VALID_TO,");
				sb.Append("       CTFS.FROM_WEIGHT,");
				sb.Append("       CTFS.TO_WEIGHT,");
				sb.Append("       CTFS.ONE_WAY_CHARGE,");
				sb.Append("       ");
				sb.Append("       CTFS.RETURN_TRIP_CHARGE");
				sb.Append("  FROM CONT_TRANS_FCL_TBL  CTFT,");
				sb.Append("       CONT_TRANS_FCL_TRN  CTFF,");
				sb.Append("       CONT_TRANS_FCL_SLAB CTFS,");
				sb.Append("       COMMODITY_MST_TBL CMT,");
				sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
				sb.Append("       PORT_MST_TBL            POL,");
				sb.Append("       PORT_MST_TBL            POD");
				sb.Append("       ");
				sb.Append("     WHERE CTFT.CONT_TRANS_FCL_PK = " + contractPk);
				sb.Append("   AND CTFT.CONT_TRANS_FCL_PK = CTFF.CONT_TRANS_FCL_FK");
				sb.Append("   AND CTFS.CONT_TRANS_FCL_TRN_FK = CTFF.CONT_TRANS_FCL_TRN_PK");
				sb.Append("   AND CMT.COMMODITY_MST_PK(+) = CTFF.COMMODITY_MST_FK");
				sb.Append("   AND CTMT.CONTAINER_TYPE_MST_PK = CTFF.CONTAINER_TYPE_MST_FK");
				sb.Append("  AND POL.PORT_MST_PK = CTFF.FROM_PORT_FK");
				sb.Append("   AND POD.PORT_MST_PK = CTFF.TO_PORT_FK");
				sb.Append("");
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Weight"
		public DataSet Fetch_weight(Int32 PK)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			DataSet Ds = new DataSet();
			try {
				sb.Append(" SELECT CONT_TRANS_FCL_TRN_FK,FROM_WEIGHT,");
				sb.Append(" TO_WEIGHT,ONE_WAY_CHARGE,RETURN_TRIP_CHARGE,'' NUM");
				sb.Append(" FROM CONT_TRANS_FCL_SLAB CONSLAB");
				sb.Append("  where CONSLAB.CONT_TRANS_FCL_TRN_FK = (");
				sb.Append(" select CONT_TRANS_FCL_TRN_FK FROM CONT_TRANS_FCL_TRN     CONTRN,");
				sb.Append(" PORT_MST_TBL           FPORT,PORT_MST_TBL           TOPORT,");
				sb.Append(" CONTAINER_TYPE_MST_TBL CONT,COMMODITY_MST_TBL      COM,CONT_TRANS_FCL_SLAB CONSLAB");
				sb.Append("  WHERE CONTRN.FROM_PORT_FK = FPORT.PORT_MST_PK(+)");
				sb.Append(" AND CONTRN.TO_PORT_FK = TOPORT.PORT_MST_PK(+)");
				sb.Append(" AND CONTRN.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK(+)");
				sb.Append(" AND CONTRN.COMMODITY_MST_FK = COM.COMMODITY_MST_PK(+)");
				sb.Append(" AND CONTRN.CONT_TRANS_FCL_FK = '" + PK + "'");
				sb.Append(" AND CONSLAB.CONT_TRANS_FCL_TRN_FK = CONT_TRANS_FCL_TRN_PK");
				sb.Append(" AND rownum = 1)");

				Ds = objWF.GetDataSet(sb.ToString());
				return Ds;
			} catch (OracleException ex) {
				throw ex;
			} catch (Exception generalEx) {
				throw generalEx;
			}
		}
		#endregion

		#region "File If Exist"
		public bool CheckFileExistence(long contractPk, string strFileName)
		{
			WorkFlow objwk = new WorkFlow();
			bool Chk = false;
			string RemQuery = "";
			RemQuery = " SELECT COUNT(*) FROM CONT_TRANS_FCL_TBL CTFT ";
			RemQuery += " WHERE CTFT.CONT_TRANS_FCL_PK = " + contractPk + " AND CTFT.ATTACHED_FILE_NAME='" + strFileName + "'";
			try {
				objwk.OpenConnection();
				if (Convert.ToInt32(objwk.ExecuteScaler(RemQuery)) > 0) {
					Chk = true;
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
				return false;
			} finally {
				objwk.MyCommand.Connection.Close();
			}
			return Chk;
		}
		#endregion

		#region "Get Contract Date"
		public System.DateTime GetContractDt(int  ContractPK)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			sb.Append("   SELECT CONFT.CONTRACT_DATE ");
			sb.Append("    FROM CONT_TRANS_FCL_TBL CONFT");
			sb.Append("   WHERE CONFT.CONT_TRANS_FCL_PK = " + ContractPK);
			try {
				return Convert.ToDateTime(objWF.ExecuteScaler(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion
	}
}

