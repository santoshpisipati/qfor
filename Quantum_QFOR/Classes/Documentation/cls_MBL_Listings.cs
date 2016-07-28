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
    public class cls_MBL_Listings : CommonFeatures
	{
		//Sreenivas 03/03/2010
		#region "Fetch MBL Count"
		public int GetMBLCount(string mblRefNr, int mblPK)
		{
			try {
				System.Text.StringBuilder strMBLQuery = new System.Text.StringBuilder(4000);
				strMBLQuery.Append("select mbl.mbl_exp_tbl_pk from mbl_exp_tbl mbl where mbl.mbl_ref_no like '%" + mblRefNr + "%'");
				WorkFlow objWF = new WorkFlow();
				DataSet objMBLDS = new DataSet();
				objMBLDS = objWF.GetDataSet(strMBLQuery.ToString());
				if (objMBLDS.Tables[0].Rows.Count == 1) {
					mblPK = Convert.ToInt32(objMBLDS.Tables[0].Rows[0][0]);
				}
				return objMBLDS.Tables[0].Rows.Count;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		//End Sreenivas
		#endregion
		#region " Fetch All Listing Data"
		public DataSet FetchAll(System.DateTime CutOffdate, System.DateTime ETDdate, string MBLRefNo = "", string Shipperid = "", short CargoType = 1, string POLID = "", string PODID = "", string POLname = "", string PODname = "", string MBLdate = "",
		string sOperator = "", int Commodityfk = 0, string Consignee = "", string VesselVoy = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long lngUsrLocFk = 0, Int32 flag = 0,
		Int32 Status = 0, Int32 FromEDI = 0, string VslVoyNo = "")
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();

			if (flag == 0) {
				strCondition += " AND 1=2 ";
			}
			if (MBLRefNo.Length > 0) {
				strCondition += "      AND UPPER(MBL.MBL_REF_NO) LIKE '%" + MBLRefNo.ToUpper().Replace("'", "''") + "%'  ";
			}

			if (Consignee.Length > 0) {
				strCondition += " AND UPPER(MBL.CONSIGNEE_NAME) = '" + Consignee.ToUpper().Replace("'", "''") + "'";
			}

			if (Shipperid.Length > 0) {
				strCondition += " AND UPPER(MBL.SHIPPER_NAME) = '" + Shipperid.ToUpper().Replace("'", "''") + "'";
			}

			if (Commodityfk > 0) {
				strCondition += "  AND MBL.COMMODITY_GROUP_FK = " + Commodityfk + "";
			}

			if (MBLdate.Length > 0) {
				strCondition += "      AND MBL.MBL_DATE = TO_DATE('" + MBLdate + "','" + dateFormat + "')   ";
			}
			//'
			if (!(CutOffdate == null)) {
				strCondition += "      AND VVT.POL_CUT_OFF_DATE >= TO_DATE('" + CutOffdate + "','& dd/MM/yyyy  HH24:MI:SS &')   ";
			}

			if (!(ETDdate == null)) {
				strCondition += "      AND VVT.POL_ETD >= TO_DATE('" + ETDdate + "','& dd/MM/yyyy  HH24:MI:SS &')   ";
			}

			if (VesselVoy.Length > 0) {
				strCondition += "      AND UPPER(V.VESSEL_NAME || VVT.VOYAGE) LIKE '%" + VesselVoy.ToUpper().Replace("'", "''") + "%'  ";
			}
			//'
			if (sOperator.Length > 0) {
				strCondition += "      AND OPR.OPERATOR_ID LIKE '%" + sOperator.ToUpper().Replace("'", "''") + "%'  ";
			}

			if (CargoType > 0) {
				strCondition += "      AND MBL.CARGO_TYPE = " + CargoType;
			}

			if (PODID.Length > 0) {
				strCondition += "     AND BOOK.PORT_MST_POD_FK IN" + "              (SELECT P.PORT_MST_PK FROM PORT_MST_TBL P " + "               WHERE P.PORT_ID LIKE '%" + PODID.ToUpper().Replace("'", "''") + "%')";
			}

			if (POLID.Length > 0) {
				strCondition += "      AND BOOK.PORT_MST_POL_FK IN" + "                  (SELECT P.PORT_MST_PK FROM PORT_MST_TBL P " + "                  WHERE P.PORT_ID LIKE '%" + POLID.ToUpper().Replace("'", "''") + "%')";

			}
			strCondition += "  AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk;
			strCondition += "  AND MBL.CREATED_BY_FK = UMT.USER_MST_PK ";

			strSQL += " SELECT DISTINCT MBL.MBL_EXP_TBL_PK, ";
			strSQL += " NVL(MBL.MBL_REF_NO,'Generate') MBL_REF_NO, ";
			strSQL += " MBL.MBL_DATE, ";
			strSQL += " PO.PORT_ID AS POL,PO1.PORT_ID AS POD, ";
			strSQL += " COM.COMMODITY_GROUP_CODE As COMMODITY , ";
			strSQL += " DECODE(MBL.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC')  As CARGO , ";
			strSQL += " mbl.shipper_name AS SHIPPER,";
			strSQL += " mbl.consignee_name AS CONSIGNEE,  ";
			//'
			strSQL += " OPR.OPERATOR_ID , ";
			strSQL += " V.VESSEL_NAME, ";
			strSQL += "  VVT.VOYAGE , ";
			strSQL += "  VVT.POL_ETD , ";
			strSQL += " VVT.POL_CUT_OFF_DATE , ";
			strSQL += "  VVT.ATD_POL , ";
			//'
			strSQL += " DECODE(MBL.EDI_STATUS,0,'NG',1,'TM') EDI_STATUS, DECODE(EDI_INTTRA_STATUS,0,'NG',1,'TM') EDI_INTTRA_STATUS, '' SEL  ";
			//by Faheem

			strSQL += " FROM MBL_EXP_TBL MBL,";
			strSQL += " JOB_CARD_TRN JOB, ";
			strSQL += " CUSTOMER_MST_TBL CUST,";
			strSQL += " BOOKING_MST_TBL BOOK, ";
			strSQL += " PORT_MST_TBL PO, ";
			strSQL += " OPERATOR_MST_TBL OPR,";
			strSQL += " PORT_MST_TBL PO1,";
			strSQL += " COMMODITY_GROUP_MST_TBL COM,";
			//'
			strSQL += "   VESSEL_VOYAGE_TBL    V, ";
			strSQL += "  VESSEL_VOYAGE_TRN    VVT, ";
			strSQL += " USER_MST_TBL UMT ";
			strSQL += " WHERE ";

			strSQL += " JOB.BOOKING_MST_FK    =  BOOK.BOOKING_MST_PK ";
			strSQL += " AND MBL.PORT_MST_POL_FK  =  PO.PORT_MST_PK";
			strSQL += " AND MBL.PORT_MST_POD_FK  =  PO1.PORT_MST_PK";
			strSQL += " AND OPR.OPERATOR_MST_PK(+) = MBL.Operator_Mst_Fk ";
			strSQL += " AND CUST.CUSTOMER_MST_PK(+)  = BOOK.Cust_Customer_Mst_Fk ";
			//'
			strSQL += "  AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK(+) ";
			strSQL += " AND MBL.VOYAGE_TRN_FK=VVT.VOYAGE_TRN_PK(+) ";
			//'
			if (FromEDI == 1) {
				if (Status == 0) {
					strSQL += " AND MBL.EDI_STATUS IN (0,1)";
				} else if (Status == 2) {
					strSQL += " AND MBL.EDI_STATUS IN (" + 0 + ")";
				} else {
					strSQL += " AND MBL.EDI_STATUS IN (" + Status + ")";
				}
			} else if (FromEDI == 2) {
				if (Status == 0) {
					strSQL += " AND MBL.EDI_INTTRA_STATUS IN (0,1)";
				} else if (Status == 2) {
					strSQL += " AND MBL.EDI_INTTRA_STATUS IN (" + 0 + ")";
				} else {
					strSQL += " AND MBL.EDI_INTTRA_STATUS IN (" + Status + ")";
				}
			} else {
			}
			strSQL += " AND MBL.Commodity_Group_Fk  = COM.COMMODITY_GROUP_PK(+) ";

			strSQL += " AND JOB.MBL_MAWB_FK= MBL.MBL_EXP_TBL_PK";
			strSQL += strCondition;

			strSQL += " ORDER BY " + SortColumn + SortType + " , MBL_REF_NO DESC  ";

			System.Text.StringBuilder strCount = new System.Text.StringBuilder();
			strCount.Append(" SELECT COUNT(*)  from  ");
			strCount.Append((" (" + strSQL.ToString() + ")"));
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage)
				CurrentPage = 1;
			if (TotalRecords == 0)
				CurrentPage = 0;
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;
			strCount.Remove(0, strCount.Length);

			System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
			sqlstr2.Append(" Select * from ");
			sqlstr2.Append("  ( Select ROWNUM SR_NO, q.* from ");
			sqlstr2.Append("  (" + strSQL.ToString() + " ");
			sqlstr2.Append("  ) q )  WHERE \"SR_NO\"  BETWEEN " + start + " AND " + last + "");
			DataSet DS = null;
			DS = objWF.GetDataSet(sqlstr2.ToString());
			try {
				return DS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region " Fetch Export Header Details"
		//Chandra Added for Reports...........
		public DataSet FetchSeaExpHeaderDocment()
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			strSQL = "select JobSeaExp.Job_Card_Sea_Exp_Pk,";
			strSQL += "JobSeaExp.Jobcard_Ref_No,";
			strSQL += "HblExp.Hbl_Exp_Tbl_Pk,";
			strSQL += "HblExp.Hbl_Ref_No,";
			strSQL += "HblExp.Vessel_Name,";
			strSQL += "CustMstShipper.Customer_Name Shipper,";
			strSQL += "CustShipperDtls.Adm_Address_1 ShiAddress1,";
			strSQL += "CustShipperDtls.Adm_Address_2 ShiAddress2,";
			strSQL += "CustShipperDtls.Adm_Address_3 ShiAddress3,";
			strSQL += "CustShipperDtls.Adm_City ShiCity,";
			strSQL += "CustMstConsignee.Customer_Name Consignee,";
			strSQL += "CustConsigneeDtls.Adm_Address_1 ConsiAddress1,";
			strSQL += "CustConsigneeDtls.Adm_Address_2 ConsiAddress2,";
			strSQL += "CustConsigneeDtls.Adm_Address_3 ConsiAddress3,";
			strSQL += "CustConsigneeDtls.Adm_City ConsiCity,";
			strSQL += "AgentMst.Agent_Name,";
			strSQL += "AgentDtls.Adm_Address_1 AgtAddress1,";
			strSQL += "AgentDtls.Adm_Address_2 AgtAddress2,";
			strSQL += "AgentDtls.Adm_Address_3 AgtAddress3,";
			strSQL += "AgentDtls.Adm_City AgtCity,";
			strSQL += "POL.PORT_NAME POL,";
			strSQL += "POD.PORT_NAME POD,";
			strSQL += "PMT.PLACE_NAME PLD,";
			strSQL += "HblExp.Goods_Description";
			strSQL += "from Hbl_Exp_Tbl HblExp,";
			strSQL += "Job_Card_Sea_Exp_Tbl JobSeaExp,";
			strSQL += "Customer_Mst_Tbl CustMstShipper,";
			strSQL += "Customer_Mst_Tbl CustMstConsignee,";
			strSQL += "Agent_Mst_Tbl AgentMst,";
			strSQL += "Booking_Sea_Tbl BkgSea,";
			strSQL += "Port_Mst_Tbl POL,";
			strSQL += "Port_Mst_Tbl POD,";
			strSQL += "Place_Mst_Tbl PMT,";
			strSQL += "Customer_Contact_Dtls CustShipperDtls,";
			strSQL += "Customer_Contact_Dtls CustConsigneeDtls,";
			strSQL += "Agent_Contact_Dtls AgentDtls";
			strSQL += "where JobSeaExp.Job_Card_Sea_Exp_Pk = HblExp.Job_Card_Sea_Exp_Fk";
			strSQL += "and POL.PORT_MST_PK=BkgSea.Port_Mst_Pol_Fk";
			strSQL += "and POD.PORT_MST_PK=BkgSea.Port_Mst_Pod_Fk";
			strSQL += "and PMT.PLACE_PK=BkgSea.Del_Place_Mst_Fk";
			strSQL += "and HblExp.Shipper_Cust_Mst_Fk=CustMstShipper.Customer_Mst_Pk";
			strSQL += "and HblExp.Consignee_Cust_Mst_Fk=CustMstConsignee.Customer_Mst_Pk";
			strSQL += "and CustMstShipper.Customer_Mst_Pk=CustShipperDtls.Customer_Mst_Fk";
			strSQL += "and CustMstConsignee.Customer_Mst_Pk=CustConsigneeDtls.Customer_Mst_Fk";
			strSQL += "and AgentMst.Agent_Mst_Pk=AgentDtls.Agent_Mst_Fk";
			strSQL += "and HblExp.Hbl_Exp_Tbl_Pk=1";
			try {
				return (objWF.GetDataSet(strSQL));
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region " Enhance Search Functionality FetchHBLForJobCard"
		public string FetchHBLForJobCard(string strCond)
		{
			//Chandra End............
			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = "";
			string strBusiType = "";
			string strJobCardPK = "";
			string strReq = null;
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            try {
				objWF.OpenConnection();
				SCM.Connection = objWF.MyConnection;
				SCM.CommandType = CommandType.StoredProcedure;
				SCM.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_PKG.GET_HBL_REF_JOBCARD";
				var _with1 = SCM.Parameters;
				_with1.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
				_with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with1.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
				_with1.Add("JOB_CARD_PK_IN", strJobCardPK).Direction = ParameterDirection.Input;
				_with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				SCM.ExecuteNonQuery();
				strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				SCM.Connection.Close();
			}
		}
		#endregion

		#region " Enhance Search Functionality FetchForMblRefInMbllist "
		public string FetchForMblRefInMbllist(string strCond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = "";
			string strBusiType = "";
			string strProcessType = "";
			string strLOCATION_IN = "";
			string strReq = null;
			arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            //added location by gopi bcz of fetching MBL Ref Nr which are created in logged in location 
            strProcessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
				strLOCATION_IN = Convert.ToString(arr.GetValue(4));
            try {
				objWF.OpenConnection();
				SCM.Connection = objWF.MyConnection;
				SCM.CommandType = CommandType.StoredProcedure;
				SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_FOR_MBL_LIST.GET_MBL_REF_NO";
				var _with2 = SCM.Parameters;
				_with2.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
				_with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with2.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
				_with2.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
				_with2.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
				_with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 10000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				SCM.ExecuteNonQuery();
				strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				SCM.Connection.Close();
			}
		}
		#endregion

		#region " Enhance Search Functionality FetchForMblRefInSuppler Invoice(Vouchet Entry)"
		public string FetchForMblRefInSuppInv(string strCond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = "";
			string strBusiType = "";
			string strJobCardPK = "";
			string strLOCATION_IN = "";
			string strVENDOR_PK = "";
			string strJOBTYPE = "";
			string strReq = null;
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
			strBusiType = Convert.ToString(arr.GetValue(2));
            //added location by gopi bcz of fetching MBL Ref Nr which are created in logged in location 
            strJobCardPK = Convert.ToString(arr.GetValue(3));
			if (arr.Length > 4)
				strLOCATION_IN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
				strVENDOR_PK = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
				strJOBTYPE = Convert.ToString(arr.GetValue(6));
            try {
				objWF.OpenConnection();
				SCM.Connection = objWF.MyConnection;
				SCM.CommandType = CommandType.StoredProcedure;
				SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_FOR_MBL_LIST.GET_MBL_REF_NO_SUPINV";
				var _with3 = SCM.Parameters;
				_with3.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
				_with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with3.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
				_with3.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
				_with3.Add("VENDOR_MST_FK_IN", (!string.IsNullOrEmpty(strVENDOR_PK) ? strVENDOR_PK : "")).Direction = ParameterDirection.Input;
				_with3.Add("JOBTYPE_IN", (!string.IsNullOrEmpty(strJOBTYPE) ? strJOBTYPE : "")).Direction = ParameterDirection.Input;
				_with3.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
				SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				SCM.ExecuteNonQuery();
				//strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
				OracleClob clob = null;
				clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
				System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
				strReturn = strReader.ReadToEnd();
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				SCM.Connection.Close();
			}
		}
		#endregion

		#region " Enhance Search Functionality FetchForMblRef in Invoice to CB ahent Sea Imp"
		public string FetchForMblRefImp(string strCond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = "";
			string strBusiType = "";
			string strJobCardPK = "";
			string strLOCATION_IN = "";
			string strReq = null;
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            //added location by gopi bcz of fetching MBL Ref Nr which are created in logged in location 
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
				strLOCATION_IN = Convert.ToString(arr.GetValue(4));
            try {
				objWF.OpenConnection();
				SCM.Connection = objWF.MyConnection;
				SCM.CommandType = CommandType.StoredProcedure;
				SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_FOR_MBL_LIST.GET_MBL_REF_NO_IMP_INV";
				var _with4 = SCM.Parameters;
				_with4.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
				_with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with4.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
				_with4.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
				//.Add("RETURN_VALUE", OracleClient.OracleDbType.Clob, "RETURN_VALUE").Direction = ParameterDirection.Output
				//SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
				_with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				SCM.ExecuteNonQuery();
				strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				SCM.Connection.Close();
			}
		}
		#endregion

		//added for Import MBL
		public string FetchForMblRefInMblimplist(string strCond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = "";
			string strBusiType = "";
			string strJobCardPK = "";
			string strLOCATION_IN = "";
			string strReq = null;
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
			strSERACH_IN = Convert.ToString(arr.GetValue(1));
			strBusiType = Convert.ToString(arr.GetValue(2));
            //added location by gopi bcz of fetching MBL Ref Nr which are created in logged in location 
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
				strLOCATION_IN = Convert.ToString(arr.GetValue(4));
            try {
				objWF.OpenConnection();
				SCM.Connection = objWF.MyConnection;
				SCM.CommandType = CommandType.StoredProcedure;
				SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_FOR_MBL_LIST.GET_MBL_REF_NO_IMP";
				var _with5 = SCM.Parameters;
				_with5.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
				_with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with5.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
				_with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				SCM.ExecuteNonQuery();
				strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				SCM.Connection.Close();
			}
		}
		//Added by Faheem for EDI Generation
		#region "EDI Generation"
		public DataSet FetchLocationDetails()
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("SELECT L.LOCATION_ID,");
				sb.Append("       L.LOCATION_NAME,");
				sb.Append("       L.OFFICE_NAME,");
				sb.Append("       L.TELE_PHONE_NO,");
				sb.Append("       L.FAX_NO,");
				sb.Append("       L.E_MAIL_ID");
				sb.Append("  FROM LOCATION_MST_TBL L");
				sb.Append(" WHERE L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchMBLDtls(Int32 MBLPK)
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("SELECT DECODE(MV.CARGO_MOVE_CODE,");
				sb.Append("              'DR/DR',");
				sb.Append("              27,");
				sb.Append("              'DR/CY',");
				sb.Append("              28,");
				sb.Append("              'DR/CFS',");
				sb.Append("              28,");
				sb.Append("              'CY/DR',");
				sb.Append("              29,");
				sb.Append("              'CFS/DR',");
				sb.Append("              29,");
				sb.Append("              'CY/CY',");
				sb.Append("              30,");
				sb.Append("              'CFS/CFS',");
				sb.Append("              30,");
				sb.Append("              'CY/CFS',");
				sb.Append("              30) CARGO_MOVE_CODE,");
				sb.Append("       CASE");
				sb.Append("         WHEN MJC.MASTER_JC_SEA_EXP_PK IS NULL THEN");
				sb.Append("          DECODE(MBL.CARGO_TYPE, 1, 2, 2, 3)");
				sb.Append("         ELSE");
				sb.Append("          DECODE(MJC.CARGO_TYPE, 1, 2, 2, 3)");
				sb.Append("       END CARGO_TYPE,");
				sb.Append("       MBL.INSURANCE_AMT,DECODE(MBL.PYMT_TYPE,1,'P',2,'C')PYMT_TYPE,TO_CHAR(MBL.MBL_DATE,'DD/MM/YYYY')MBL_DATE");
				sb.Append("       ,VM.VESSEL_ID, VM.VESSEL_NAME,VY.VOYAGE,POL.PORT_NAME POL_NAME,POD.PORT_NAME POD_NAME,MBL.AGENT_NAME ");
				sb.Append("      ,MBL.AGENT_ADDRESS,MBL.SHIPPER_NAME,MBL.SHIPPER_ADDRESS,MBL.CONSIGNEE_NAME,MBL.CONSIGNEE_ADDRESS     ");
				sb.Append("      ,OP.OPERATOR_ID,OP.OPERATOR_NAME,MBL.MARKS_NUMBERS,MBL.GOODS_DESCRIPTION");
				sb.Append("  FROM MBL_EXP_TBL MBL, CARGO_MOVE_MST_TBL MV, MASTER_JC_SEA_EXP_TBL MJC");
				sb.Append(" , VESSEL_VOYAGE_TRN VY ,VESSEL_VOYAGE_TBL VM,PORT_MST_TBL POL,PORT_MST_TBL POD,OPERATOR_MST_TBL OP");
				sb.Append(" WHERE MBL.CARGO_MOVE_FK = MV.CARGO_MOVE_PK");
				sb.Append("   AND MBL.MBL_EXP_TBL_PK = MJC.MBL_FK(+)");
				sb.Append("   AND MBL.VOYAGE_TRN_FK = VY.VOYAGE_TRN_PK(+)");
				sb.Append("   AND VY.VESSEL_VOYAGE_TBL_FK = VM.VESSEL_VOYAGE_TBL_PK(+)");
				sb.Append("   AND MBL.PORT_MST_POL_FK=POL.PORT_MST_PK");
				sb.Append("   AND MBL.PORT_MST_POD_FK=POD.PORT_MST_PK");
				sb.Append("   AND MBL.OPERATOR_MST_FK=OP.OPERATOR_MST_PK");
				sb.Append("   AND MBL.MBL_EXP_TBL_PK = " + MBLPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchContainerDtl(Int32 MBLPK)
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

				sb.Append("SELECT CMT.COMMODITY_ID,");
				sb.Append("       CMT.COMMODITY_NAME,");
				sb.Append("       UPPER(PY.PACK_TYPE_DESC) PACK_TYPE_DESC,");
				sb.Append("       JCT.PACK_COUNT,");
				sb.Append("       CY.CONTAINER_TYPE_MST_ID,");
				sb.Append("       JCT.CONTAINER_NUMBER,");
				sb.Append("       JCT.SEAL_NUMBER,");
				sb.Append("       JCT.GROSS_WEIGHT,");
				sb.Append("       JCT.NET_WEIGHT,");
				sb.Append("       JCT.CHARGEABLE_WEIGHT,");
				sb.Append("       JCT.VOLUME_IN_CBM,");
				sb.Append("       CASE");
				sb.Append("         WHEN PY.PACK_TYPE_ID = 'BOX' THEN");
				sb.Append("          'BX'");
				sb.Append("         WHEN PY.PACK_TYPE_ID = 'CTN' THEN");
				sb.Append("          'CT'");
				sb.Append("         WHEN PY.PACK_TYPE_ID = 'PLT' THEN");
				sb.Append("          'PL'");
				sb.Append("         WHEN PY.PACK_TYPE_ID = 'PCS' THEN");
				sb.Append("          'PC'");
				sb.Append("         WHEN PY.PACK_TYPE_ID = 'CASE' THEN");
				sb.Append("          'CS'");
				sb.Append("       END EDI_PCK,");
				sb.Append("       HBL.HBL_REF_NO,");
				sb.Append("       HBL.GOODS_DESCRIPTION,");
				sb.Append("       HBL.MARKS_NUMBERS");
				sb.Append("  FROM JOB_CARD_TRN   JOB,");
				sb.Append("       JOB_TRN_CONT   JCT,");
				sb.Append("       PACK_TYPE_MST_TBL      PY,");
				sb.Append("       CONTAINER_TYPE_MST_TBL CY,");
				sb.Append("       COMMODITY_MST_TBL      CMT,");
				sb.Append("       HBL_EXP_TBL            HBL");
				sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JCT.JOB_CARD_TRN_FK");
				sb.Append("   AND PY.PACK_TYPE_MST_PK = JCT.PACK_TYPE_MST_FK");
				sb.Append("   AND CY.CONTAINER_TYPE_MST_PK = JCT.CONTAINER_TYPE_MST_FK");
				sb.Append("   AND CMT.COMMODITY_MST_PK(+) = JCT.COMMODITY_MST_FK");
				sb.Append("   AND HBL.HBL_EXP_TBL_PK = JOB.HBL_HAWB_FK");
				sb.Append("   AND JOB.MBL_MAWB_FK = " + MBLPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchMBLContainerDtl(Int32 MBLPK)
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("SELECT CMT.COMMODITY_ID,");
				sb.Append("       CMT.COMMODITY_NAME,");
				sb.Append("       UPPER(PY.PACK_TYPE_DESC) PACK_TYPE_DESC,");
				sb.Append("       MBC.PACK_COUNT,");
				sb.Append("       CY.CONTAINER_TYPE_MST_ID,");
				sb.Append("       MBC.CONTAINER_NUMBER,");
				sb.Append("       MBC.SEAL_NUMBER,");
				sb.Append("       MBC.GROSS_WEIGHT,");
				sb.Append("       MBC.NET_WEIGHT,");
				sb.Append("       MBC.CHARGEABLE_WEIGHT,");
				sb.Append("       MBC.VOLUME_IN_CBM,");
				sb.Append("       CASE");
				sb.Append("         WHEN PY.PACK_TYPE_ID = 'BOX' THEN");
				sb.Append("          'BX'");
				sb.Append("         WHEN PY.PACK_TYPE_ID = 'CTN' THEN");
				sb.Append("          'CT'");
				sb.Append("         WHEN PY.PACK_TYPE_ID = 'PLT' THEN");
				sb.Append("          'PL'");
				sb.Append("         WHEN PY.PACK_TYPE_ID = 'PCS' THEN");
				sb.Append("          'PC'");
				sb.Append("       END EDI_PCK,");
				sb.Append("CASE");
				sb.Append("         WHEN CY.CONTAINER_TYPE_MST_ID = '20DV' OR CY.CONTAINER_TYPE_MST_ID = '20GP' THEN");
				sb.Append("          102");
				sb.Append("         WHEN CY.CONTAINER_TYPE_MST_ID = '40DV' OR CY.CONTAINER_TYPE_MST_ID = '40GP' THEN");
				sb.Append("          105");
				sb.Append("         END EDI_TYPE");
				sb.Append("  FROM MBL_EXP_TBL            MBL,");
				sb.Append("       MBL_TRN_EXP_CONTAINER  MBC,");
				sb.Append("       PACK_TYPE_MST_TBL      PY,");
				sb.Append("       COMMODITY_MST_TBL      CMT,");
				sb.Append("       CONTAINER_TYPE_MST_TBL CY");
				sb.Append(" WHERE MBC.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK");
				sb.Append("   AND MBC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK");
				sb.Append("   AND MBC.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK");
				sb.Append("   AND MBC.CONTAINER_TYPE_MST_FK = CY.CONTAINER_TYPE_MST_PK");
				sb.Append("   AND MBL.MBL_EXP_TBL_PK = " + MBLPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchMBLClause(Int32 MBLPK)
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("SELECT MBC.BL_DESCRIPTION");
				sb.Append("  FROM MBL_EXP_TBL MBL, MBL_BL_CLAUSE_TBL MBC");
				sb.Append(" WHERE MBC.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK");
				sb.Append("   AND MBL.MBL_EXP_TBL_PK = " + MBLPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchTtlCargoDtl(Int32 MBLPK)
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("SELECT SUM(MC.GROSS_WEIGHT) GROSS_WEIGHT,");
				sb.Append("       SUM(MC.NET_WEIGHT) NET_WEIGHT,");
				sb.Append("       SUM(MC.CHARGEABLE_WEIGHT) CHARGEABLE_WEIGHT,");
				sb.Append("       SUM(MC.VOLUME_IN_CBM) VOLUME_IN_CBM,");
				sb.Append("       SUM(MC.PACK_COUNT) PACK_COUNT");
				sb.Append("  FROM MBL_EXP_TBL MBL, MBL_TRN_EXP_CONTAINER MC");
				sb.Append(" WHERE MC.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK");
				sb.Append("   AND MBL.MBL_EXP_TBL_PK = " + MBLPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchEmployeeDtl()
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("SELECT U.USER_ID, U.USER_NAME, E.EMAIL_ID, E.PHONE_NO, E.EMPLOYEE_ID");
				sb.Append("  FROM USER_MST_TBL U, EMPLOYEE_MST_TBL E");
				sb.Append(" WHERE U.EMPLOYEE_MST_FK = E.EMPLOYEE_MST_PK");
				sb.Append("   AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"]);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchCorporateName()
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("SELECT CMT.CORPORATE_NAME FROM CORPORATE_MST_TBL CMT");
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchCountryID(int LocPK)
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("SELECT C.COUNTRY_ID FROM COUNTRY_MST_TBL C, LOCATION_MST_TBL LMT ");
				sb.Append(" WHERE C.COUNTRY_MST_PK = LMT.COUNTRY_MST_FK ");
				sb.Append(" AND LMT.LOCATION_MST_PK=" + LocPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchVATID()
		{
			try {
				WorkFlow objWF = new WorkFlow();
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append("SELECT CMT.GST_NO FROM CORPORATE_MST_TBL CMT WHERE CMT.CORPORATE_MST_PK=1");
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region " Fetch All Listing Data"
		public DataSet FetchAllForPendingMBL(System.DateTime ETDdate, string MBLRefNo = "", string Shipperid = "", short CargoType = 1, string POLID = "", string PODID = "", string POLname = "", string PODname = "", string MBLdate = "", string sOperator = "",
		int Commodityfk = 0, string Consignee = "", string VesselVoy = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long lngUsrLocFk = 0, Int32 flag = 0, Int32 Status = 0,
		Int32 FromEDI = 0, string VslVoyNo = "", Int32 ExportExcel = 0)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();

			if (flag == 0) {
				strCondition += " AND 1=2 ";
			}
			if (MBLRefNo.Length > 0) {
				strCondition += "      AND UPPER(MBL.MBL_REF_NO) LIKE '%" + MBLRefNo.ToUpper().Replace("'", "''") + "%'  ";
			}

			if (Consignee.Length > 0) {
				strCondition += " AND UPPER(MBL.CONSIGNEE_NAME) = '" + Consignee.ToUpper().Replace("'", "''") + "'";
			}

			if (Shipperid.Length > 0) {
				strCondition += " AND UPPER(MBL.SHIPPER_NAME) = '" + Shipperid.ToUpper().Replace("'", "''") + "'";
			}

			if (Commodityfk > 0) {
				strCondition += "  AND MBL.COMMODITY_GROUP_FK = " + Commodityfk + "";
			}

			if (MBLdate.Length > 0) {
				strCondition += "      AND MBL.MBL_DATE = TO_DATE('" + MBLdate + "','" + dateFormat + "')   ";
			}
			//'


			if (!(ETDdate == null)) {
				strCondition += "      AND VVT.POL_ETD >= TO_DATE('" + ETDdate + "','& dd/MM/yyyy  HH24:MI:SS &')   ";
			}

			if (VesselVoy.Length > 0) {
				strCondition += "      AND UPPER(V.VESSEL_NAME || VVT.VOYAGE) LIKE '%" + VesselVoy.ToUpper().Replace("'", "''") + "%'  ";
			}
			//'
			if (sOperator.Length > 0) {
				strCondition += "      AND OPR.OPERATOR_ID LIKE '%" + sOperator.ToUpper().Replace("'", "''") + "%'  ";
			}

			if (CargoType > 0) {
				strCondition += "      AND MBL.CARGO_TYPE = " + CargoType;
			}

			if (PODID.Length > 0) {
				strCondition += "     AND BOOK.PORT_MST_POD_FK IN" + "              (SELECT P.PORT_MST_PK FROM PORT_MST_TBL P " + "               WHERE P.PORT_ID LIKE '%" + PODID.ToUpper().Replace("'", "''") + "%')";
			}

			if (POLID.Length > 0) {
				strCondition += "      AND BOOK.PORT_MST_POL_FK IN" + "                  (SELECT P.PORT_MST_PK FROM PORT_MST_TBL P " + "                  WHERE P.PORT_ID LIKE '%" + POLID.ToUpper().Replace("'", "''") + "%')";

			}
			strCondition += "  AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk;
			strCondition += "  AND MBL.CREATED_BY_FK = UMT.USER_MST_PK ";

			strSQL += " SELECT DISTINCT MBL.MBL_EXP_TBL_PK, ";
			strSQL += " NVL(MBL.MBL_REF_NO,'Generate') MBL_REF_NO, ";
			strSQL += " MBL.MBL_DATE, ";
			strSQL += " PO.PORT_ID AS POL,PO1.PORT_ID AS POD, ";
			strSQL += " COM.COMMODITY_GROUP_CODE As COMMODITY , ";
			strSQL += " DECODE(MBL.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC')  As CARGO , ";
			strSQL += " mbl.shipper_name AS SHIPPER,";
			strSQL += " mbl.consignee_name AS CONSIGNEE,  ";
			//'
			strSQL += "   OPR.OPERATOR_NAME as Operator_Id, ";
			//strSQL &= vbCrLf & " V.VESSEL_NAME, "
			//strSQL &= vbCrLf & "  VVT.VOYAGE , "
			strSQL += "   NVL(VVT.ATD_POL,VVT.POL_ETD) AS POL_ETD , ";
			//strSQL &= vbCrLf & " VVT.POL_CUT_OFF_DATE , "


			strSQL += "  (NVL(VVT.ATD_POL,VVT.POL_ETD)+";
			strSQL += "  OPERATOR_CALENDER_PKG.FETCH_CHARGEABLE_DAYS(NVL(VVT.ATD_POL,VVT.POL_ETD),NVL(OPR.NROFDAYS,0),OPR.OPERATOR_MST_PK)) ATD_POL, ";

			//strSQL &= vbCrLf & "  DECODE(MBL.Status, 0, 'DRAFT', 1, 'RELEASED')  MBL_STATUS, mbl.released_dt as  MBL_RELEASED, '' SEL  " 'by Faheem
			strSQL += " DECODE(MBL.Status, 0, 'DRAFT', 1, 'RELEASED')  MBL_STATUS, TO_CHAR(mbl.released_dt,DATETIMEFORMAT) AS MBL_RELEASED, '' SEL ";
			strSQL += " FROM MBL_EXP_TBL MBL,";
			strSQL += " JOB_CARD_TRN JOB, ";
			strSQL += " CUSTOMER_MST_TBL CUST,";
			strSQL += " BOOKING_MST_TBL BOOK, ";
			strSQL += " PORT_MST_TBL PO, ";
			strSQL += " OPERATOR_MST_TBL OPR,";
			strSQL += " PORT_MST_TBL PO1,";
			strSQL += " COMMODITY_GROUP_MST_TBL COM,";
			//'
			strSQL += "   VESSEL_VOYAGE_TBL    V, ";
			strSQL += "  VESSEL_VOYAGE_TRN    VVT, ";
			strSQL += " USER_MST_TBL UMT ";
			strSQL += " WHERE ";

			strSQL += " JOB.BOOKING_MST_FK    =  BOOK.BOOKING_MST_PK ";
			strSQL += " AND MBL.PORT_MST_POL_FK  =  PO.PORT_MST_PK";
			strSQL += " AND MBL.PORT_MST_POD_FK  =  PO1.PORT_MST_PK";
			strSQL += " AND OPR.OPERATOR_MST_PK(+) = MBL.Operator_Mst_Fk ";
			strSQL += "  AND mbl.status=0 ";
			strSQL += " AND CUST.CUSTOMER_MST_PK(+)  = BOOK.Cust_Customer_Mst_Fk ";
			//'
			strSQL += "  AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK(+) ";
			strSQL += " AND MBL.VOYAGE_TRN_FK=VVT.VOYAGE_TRN_PK(+) ";
			//'
			if (FromEDI == 1) {
				if (Status == 0) {
					strSQL += " AND MBL.EDI_STATUS IN (0,1)";
				} else if (Status == 2) {
					strSQL += " AND MBL.EDI_STATUS IN (" + 0 + ")";
				} else {
					strSQL += " AND MBL.EDI_STATUS IN (" + Status + ")";
				}
			} else if (FromEDI == 2) {
				if (Status == 0) {
					strSQL += " AND MBL.EDI_INTTRA_STATUS IN (0,1)";
				} else if (Status == 2) {
					strSQL += " AND MBL.EDI_INTTRA_STATUS IN (" + 0 + ")";
				} else {
					strSQL += " AND MBL.EDI_INTTRA_STATUS IN (" + Status + ")";
				}
			} else {
			}
			strSQL += " AND MBL.Commodity_Group_Fk  = COM.COMMODITY_GROUP_PK(+) ";

			strSQL += " AND JOB.MBL_MAWB_FK= MBL.MBL_EXP_TBL_PK";
			strSQL += strCondition;

			strSQL += " ORDER BY ATD_POL ASC ";

			System.Text.StringBuilder strCount = new System.Text.StringBuilder();
			strCount.Append(" SELECT COUNT(*)  from  ");
			strCount.Append((" (" + strSQL.ToString() + ")"));
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage)
				CurrentPage = 1;
			if (TotalRecords == 0)
				CurrentPage = 0;
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;
			strCount.Remove(0, strCount.Length);

			System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
			sqlstr2.Append(" Select * from ");
			sqlstr2.Append("  ( Select ROWNUM SR_NO, q.* from ");
			sqlstr2.Append("  (" + strSQL.ToString() + " ");


			if (ExportExcel == 1) {
				sqlstr2.Append("  ) q ) order by \"SR_NO\" ASC");
			} else {
				sqlstr2.Append("  ) q )  WHERE \"SR_NO\"  BETWEEN " + start + " AND " + last + "");
			}


			// sqlstr2.Append("  ) q )  WHERE ""SR_NO""  BETWEEN " & start & " AND " & last & "")
			DataSet DS = null;
			DS = objWF.GetDataSet(sqlstr2.ToString());
			try {
				return DS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion
		#region "Update Pending Activities"
		public ArrayList UpdateMblReleaseDate(DataSet ds)
		{
			WorkFlow ObjWk = new WorkFlow();
			OracleCommand objCommand = new OracleCommand();
			OracleTransaction TRAN = null;
			int nRecAfct = 0;
			string strSql = null;
			System.DateTime mblreldate = default(System.DateTime);




			try {
				ObjWk.OpenConnection();
				TRAN = ObjWk.MyConnection.BeginTransaction();
				var _with6 = objCommand;
				_with6.Connection = ObjWk.MyConnection;
				_with6.CommandType = CommandType.Text;
				_with6.Transaction = TRAN;
				for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++) {

					if (Convert.ToInt32(ds.Tables[0].Rows[i]["SEL"]) == 1) {
						strSql = "  update mbl_exp_tbl mbt";
						strSql += "  set mbt.status =" + ds.Tables[0].Rows[i]["MBL_STATUS"];
						strSql += " ,";
						strSql += " mbt.released_dt =TO_DATE('" + getDefault(ds.Tables[0].Rows[i]["MBL_RELEASED"], "") + "','DD/MM/YYYY HH24:MI:SS')";
						strSql += "   where mbt.mbl_exp_tbl_pk =" + ds.Tables[0].Rows[i]["MBL_EXP_TBL_PK"];
						_with6.CommandText = strSql;
						nRecAfct = nRecAfct + _with6.ExecuteNonQuery();
					}
				}

				if (nRecAfct > 0) {
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully.");
				} else {
					TRAN.Rollback();
					arrMessage.Add("Error");
				}
				return arrMessage;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				ObjWk.MyConnection.Close();
			}
		}
		#endregion
	}
}























