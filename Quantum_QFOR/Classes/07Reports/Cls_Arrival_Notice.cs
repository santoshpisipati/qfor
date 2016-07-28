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
using Oracle.ManagedDataAccess.Types;
using System;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    internal class Cls_Arrival_Notice : CommonFeatures
    {
        #region "  Fetch For AIR/SEA Import"

        public DataSet FetchAirUserImport(string VslName = "", string FlightNr = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string strFromDate = "",
        string HBLREFNO = "", string MBLNO = "", string CANNo = "", long VeslPK = 0)
        {
            DataSet objDS = new DataSet();
            try
            {
                objDS = FetchAllUserImport("", FlightNr, JobPk, PolPk, PodPk, CustPk, CurrentPage, TotalPage, flag, strFromDate,
                HBLREFNO, MBLNO, "", "", CANNo, 1, 0, VeslPK);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return objDS;
        }

        public DataSet FetchSeaUserImport(string VeslName = "", string VoyageNr = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string strFromDate = "",
        string HBLREFNO = "", string MBLNO = "", string CargoType = "", string ContainerNo = "", string CANNo = "", long VeslPK = 0)
        {
            DataSet objDS = new DataSet();
            try
            {
                objDS = FetchAllUserImport(VeslName, VoyageNr, JobPk, PolPk, PodPk, CustPk, CurrentPage, TotalPage, flag, strFromDate,
                HBLREFNO, MBLNO, CargoType, ContainerNo, CANNo, 2, 0, VeslPK);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return objDS;
        }

        public DataSet FetchAirSeaUserImport(string VeslName = "", string VoyageNr = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string strFromDate = "",
        string HBLREFNO = "", string MBLNO = "", string CargoType = "", string ContainerNo = "", string CANNo = "", long VeslPK = 0)
        {
            DataSet objDS = new DataSet();
            try
            {
                objDS = FetchAllUserImport(VeslName, VoyageNr, JobPk, PolPk, PodPk, CustPk, CurrentPage, TotalPage, flag, strFromDate,
                HBLREFNO, MBLNO, CargoType, ContainerNo, CANNo, 0, 0, VeslPK);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return objDS;
        }

        public DataSet FetchAllUserImport(string VeslName = "", string VoyageFlightNr = "", long JobPk = 0, long PolPk = 0, long PodPk = 0, long CustPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string strFromDate = "",
        string HBLREFNO = "", string MBLNO = "", string CargoType = "", string ContianerNo = "", string CANNo = "", short BizType = 3, short ProcessType = 3, long VeslPK = 0)
        {
            //Note: BizType-3 for All and ProcessType-3 for all
            string Strsql = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            //If VeslPK.Trim.Length > 0 Then
            //    strCondition = strCondition & " AND JSI.HBL_HAWB_REF_NO='" & HBLREFNO & "'"
            //End If

            if (HBLREFNO.Trim().Length > 0)
            {
                strCondition = strCondition + " AND JSI.HBL_HAWB_REF_NO='" + HBLREFNO + "'";
            }
            if (MBLNO.Trim().Length > 0)
            {
                strCondition = strCondition + " AND JSI.MBL_MAWB_REF_NO='" + MBLNO + "'";
            }
            if (CargoType != "0" & BizType == 2 | BizType == 3)
            {
                strCondition = strCondition + " AND JSI.Cargo_Type='" + CargoType + "'";
            }
            if (ContianerNo.Trim().Length > 0 & BizType == 2 | BizType == 3)
            {
                strCondition = strCondition + " AND JTSIC.Container_Number='" + ContianerNo + "'";
            }
            if (strFromDate.Trim().Length > 0)
            {
                strCondition = strCondition + " AND JSI.ARRIVAL_DATE like To_date('" + strFromDate + "','" + dateFormat + "')";
            }
            if (VeslName.Trim().Length > 0 & BizType == 2 | BizType == 3)
            {
                strCondition = strCondition + " AND JSI.VESSEL_NAME LIKE '%" + VeslName + "%'";
            }
            if (!string.IsNullOrEmpty(VoyageFlightNr))
            {
                strCondition = strCondition + " AND JSI.VOYAGE_FLIGHT_NO ='" + VoyageFlightNr + "'";
            }
            if (JobPk > 0)
            {
                strCondition = strCondition + " AND JSI.JOB_CARD_TRN_PK = " + JobPk;
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (PolPk > 0)
            {
                strCondition = strCondition + " And JSI.PORT_MST_POL_FK = " + PolPk;
            }
            if (PodPk > 0)
            {
                strCondition = strCondition + " And JSI.PORT_MST_POD_FK = " + PodPk;
            }
            if (CustPk > 0)
            {
                strCondition = strCondition + " AND C.CUSTOMER_MST_PK=" + CustPk;
            }
            if (BizType == 1 | BizType == 2)
            {
                strCondition += " AND JSI.BUSINESS_TYPE=" + BizType;
            }
            if (ProcessType == 1 | ProcessType == 2)
            {
                strCondition += " AND JSI.PROCESS_TYPE=" + ProcessType;
            }

            sb.Append(" SELECT * FROM (SELECT ROWNUM AS SLNO,Q.* FROM ");
            sb.Append(" (SELECT JSI.JOB_CARD_TRN_PK AS JOBCARDPK,");
            sb.Append(" JSI.JOBCARD_REF_NO AS JOBREFNO,");
            sb.Append(" CAN.CAN_REF_NO AS CANREFNO,");
            sb.Append(" CAN.CAN_DATE AS CANDATE,");
            sb.Append(" DECODE(JSI.BUSINESS_TYPE,2,'Sea',1,'Air') BIZ_TYPE,");
            sb.Append(" DECODE(JSI.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE, ");
            sb.Append(" JSI.HBL_HAWB_REF_NO AS HBL_HAWBREFNO,JSI.MBL_MAWB_REF_NO AS MBL_MAWBREFNO,");
            sb.Append(" C.CUSTOMER_NAME AS CUSTOMER,");
            sb.Append(" (CASE WHEN JSI.VESSEL_NAME IS NOT NULL AND JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
            sb.Append(" JSI.VESSEL_NAME || '-' || JSI.VOYAGE_FLIGHT_NO ");
            sb.Append(" WHEN JSI.VESSEL_NAME IS NOT NULL THEN JSI.VESSEL_NAME ELSE JSI.VOYAGE_FLIGHT_NO END) AS VSL_FLIGHT,");
            sb.Append(" POD.PORT_ID AS POD,");
            sb.Append(" TO_DATE(ARRIVAL_DATE,DATEFORMAT) AS ETA,TO_DATE(JSI.ETA_DATE,DATEFORMAT) AS ETADATE ");
            sb.Append(" FROM JOB_CARD_TRN JSI,PORT_MST_TBL POL,PORT_MST_TBL POD,");
            sb.Append(" CUSTOMER_MST_TBL C,JOB_TRN_CONT JTSIC,CAN_MST_TBL CAN ");
            sb.Append(" WHERE C.CUSTOMER_MST_PK = JSI.CUST_CUSTOMER_MST_FK");
            sb.Append(" AND CAN.JOB_CARD_FK(+)  = JSI.JOB_CARD_TRN_PK");
            sb.Append(" AND CAN.PORT_MST_POD_FK(+)=JSI.PORT_MST_POD_FK");
            sb.Append(" AND CAN.CUSTOMER_MST_FK(+)=JSI.CONSIGNEE_CUST_MST_FK");
            sb.Append(" AND POD.PORT_MST_PK=JSI.PORT_MST_POD_FK ");
            sb.Append(" AND POL.PORT_MST_PK=JSI.PORT_MST_POL_FK ");
            sb.Append(" AND JTSIC.JOB_CARD_TRN_FK=JSI.JOB_CARD_TRN_PK(+)");

            if (!string.IsNullOrEmpty(CANNo))
            {
                sb.Append(" AND CAN.can_ref_no='" + CANNo + "'");
            }

            sb.Append(" AND JSI.ARRIVAL_DATE is not null");
            sb.Append(strCondition);
            sb.Append(" ORDER BY ETADATE DESC,JOBREFNO DESC ");
            sb.Append(") Q)");

            objWF.MyDataReader = objWF.GetDataReader("SELECT COUNT(*) FROM (" + sb.ToString() + ")");
            while (objWF.MyDataReader.Read())
            {
                TotalRecords = objWF.MyDataReader.GetInt32(0);
            }
            objWF.MyDataReader.Close();
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
            sb.Append(" WHERE SLNO  Between " + start + " and " + last);
            try
            {
                return Objwk.GetDataSet(sb.ToString());
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

        #endregion "  Fetch For AIR/SEA Import"

        #region "Enhance Search & Lookup Search Block "

        public string FetchImpCertificateJob(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strLOCATION_IN = "";
            string CARGO_TYPE = "";
            string strBizType = null;
            string strReq = null;
            string PODPK_IN = null;

            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));

            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOCATION_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                CARGO_TYPE = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                PODPK_IN = Convert.ToString(arr.GetValue(5));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_IMP_CERTIFICATE_JOB_PKG.GET_IMP_REP_JOB_REF_COMMON";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(loc) ? loc : "")).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("CARGO_TYPE_IN", (!string.IsNullOrEmpty(CARGO_TYPE) ? CARGO_TYPE : "")).Direction = ParameterDirection.Input;
                _with1.Add("PODPK_IN", (!string.IsNullOrEmpty(PODPK_IN) ? PODPK_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
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

        #endregion "Enhance Search & Lookup Search Block "

        #region "Enhance Search & Lookup Search Block "

        public string Fetch_Job_Ref_Parent_Child_Imp(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOCATION_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOCATION_IN = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBREF_IMP_PAR_CHILD_PKG.get_job_ref_par_child_Imp";
                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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

        public string Fetch_CAN_REF_NO(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOCATION_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOCATION_IN = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBREF_IMP_PAR_CHILD_PKG.get_can_ref";

                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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

        public string FetchImpCertificateVesFlight(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_IMP_VESFLIGHT_PKG.GET_VES_FLIGHT_COMMON";

                var _with4 = selectCommand.Parameters;
                _with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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

        #endregion "Enhance Search & Lookup Search Block "

        #region "Fetch Job Card Sea/Air Details For Report"

        //
        public DataSet FetchJobCardSeaDetails(string JobCardPK)
        {
            StringBuilder Strsql = new StringBuilder();
            WorkFlow Objwk = new WorkFlow();

            Strsql.Append(" SELECT JSI.JOB_CARD_TRN_PK AS JOBCARDPK,");
            Strsql.Append(" CAN.CAN_REF_NO JOBCARDNO,");
            Strsql.Append(" CAN.CUSTOM_REF_NO,");
            Strsql.Append(" TO_CHAR(CAN.CUSTOM_REF_DT, DATEFORMAT) CUSTOM_REF_DT,");
            Strsql.Append(" CAN.CUSTOM_ITEM_NR,");
            Strsql.Append(" TO_CHAR(JSI.ARRIVAL_DATE) AS UCRNO,");
            Strsql.Append(" CONSIGCUST.CUSTOMER_NAME CONSIGNAME,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_1 CONSIGADD1,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_2 CONSIGADD2,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_3 CONSIGADD3,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_ZIP_CODE CONSIGZIP,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_PHONE_NO_1 CONSIGPHONE,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_CITY CONSIGCITY,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_FAX_NO CONFAX,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_EMAIL_ID CONEMAIL,");
            Strsql.Append(" CONSCOUNTRY.COUNTRY_NAME CONSCOUNTRY,");
            Strsql.Append(" SHIPPERCUST.CUSTOMER_NAME SHIPPERNAME,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_1 SHIPPERADD1,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_2 SHIPPERADD2,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_3 SHIPPERADD3,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_ZIP_CODE SHIPPERZIP,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_CITY SHIPPERCITY,");
            Strsql.Append("  SHIPPERCUSTDTLS.ADM_FAX_NO SHIPPERFAX,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_EMAIL_ID SHIPPEREMAIL,");
            Strsql.Append(" SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,");
            Strsql.Append(" AGENTMST.AGENT_NAME AGENTNAME,");
            Strsql.Append(" AGENTDTLS.ADM_ADDRESS_1 AGENTADD1,");
            Strsql.Append(" AGENTDTLS.ADM_ADDRESS_2 AGENTADD2,");
            Strsql.Append(" AGENTDTLS.ADM_ADDRESS_3 AGENTADD3,");
            Strsql.Append(" AGENTDTLS.ADM_CITY      AGENTCITY,");
            Strsql.Append(" AGENTDTLS.ADM_ZIP_CODE  AGENTZIP,");
            Strsql.Append(" AGENTDTLS.ADM_PHONE_NO_1 AGENTPHONE,");
            Strsql.Append(" AGENTDTLS.ADM_FAX_NO    AGENTFAX,");
            Strsql.Append(" AGENTDTLS.ADM_EMAIL_ID  AGENTEMAIL,");
            Strsql.Append(" AGENTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,");
            Strsql.Append(" (CASE WHEN JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
            Strsql.Append(" JSI.VESSEL_NAME || '/' || JSI.VOYAGE_FLIGHT_NO");
            Strsql.Append(" ELSE");
            Strsql.Append(" JSI.VESSEL_NAME END ) VES_VOY,");
            Strsql.Append(" CTMST.COMMODITY_GROUP_CODE COMMTYPE,");
            Strsql.Append(" (CASE WHEN JSI.HBL_HAWB_REF_NO IS NOT NULL THEN");
            Strsql.Append(" JSI.HBL_HAWB_REF_NO");
            Strsql.Append("  ELSE");
            Strsql.Append(" JSI.MBL_MAWB_REF_NO END ) BLREFNO,");
            Strsql.Append(" POL.PORT_NAME POLNAME,");
            Strsql.Append(" POD.PORT_NAME PODNAME,");
            Strsql.Append(" DELMST.PLACE_NAME DEL_PLACE_NAME,");
            Strsql.Append(" JSI.GOODS_DESCRIPTION,");
            Strsql.Append(" JSI.ETA_DATE ETA,");
            Strsql.Append(" JSI.ETD_DATE ETD,");
            Strsql.Append(" JSI.CLEARANCE_ADDRESS CLEARANCEPOINT,");
            Strsql.Append(" JSI.MARKS_NUMBERS MARKS,");
            Strsql.Append(" STMST.INCO_CODE TERMS,");
            Strsql.Append(" NVL(JSI.INSURANCE_AMT, 0) INSURANCE,");
            Strsql.Append(" JSI.PYMT_TYPE PYMT_TYPE,");
            Strsql.Append(" JSI.CARGO_TYPE CARGO_TYPE,");
            Strsql.Append(" SUM(JTSC.GROSS_WEIGHT) GROSSWEIGHT,");
            Strsql.Append("  SUM(JTSC.NET_WEIGHT) NETWEIGHT,");
            Strsql.Append("  SUM(JTSC.CHARGEABLE_WEIGHT) CHARWT,");
            Strsql.Append(" SUM(JTSC.VOLUME_IN_CBM) VOLUME");
            Strsql.Append(" from JOB_CARD_TRN JSI,");
            Strsql.Append(" CUSTOMER_MST_TBL CONSIGCUST,");
            Strsql.Append(" CUSTOMER_CONTACT_DTLS CONSIGCUSTDTLS,");
            Strsql.Append(" CUSTOMER_CONTACT_DTLS SHIPPERCUSTDTLS,");
            Strsql.Append(" CUSTOMER_MST_TBL SHIPPERCUST,");
            Strsql.Append(" AGENT_MST_TBL AGENTMST,");
            Strsql.Append(" AGENT_CONTACT_DTLS AGENTDTLS,");
            Strsql.Append(" PORT_MST_TBL POL,");
            Strsql.Append(" PORT_MST_TBL POD,");
            Strsql.Append(" JOB_TRN_CONT JTSC,");
            Strsql.Append(" SHIPPING_TERMS_MST_TBL STMST,");
            Strsql.Append(" COUNTRY_MST_TBL SHIPCOUNTRY,");
            Strsql.Append(" COUNTRY_MST_TBL CONSCOUNTRY,");
            Strsql.Append(" COUNTRY_MST_TBL AGENTCOUNTRY,");
            Strsql.Append(" PLACE_MST_TBL DELMST,");
            Strsql.Append(" JOB_TRN_TP JTSIT,");
            Strsql.Append(" COMMODITY_GROUP_MST_TBL CTMST,");
            Strsql.Append(" CAN_MST_TBL CAN");
            Strsql.Append(" WHERE CONSIGCUST.CUSTOMER_MST_PK(+) = JSI.CONSIGNEE_CUST_MST_FK");
            Strsql.Append(" AND CAN.JOB_CARD_FK(+)=JSI.JOB_CARD_TRN_PK");
            Strsql.Append(" AND   SHIPPERCUST.CUSTOMER_MST_PK(+)=JSI.SHIPPER_CUST_MST_FK");
            Strsql.Append(" AND   CONSIGCUSTDTLS.CUSTOMER_MST_FK(+)=CONSIGCUST.CUSTOMER_MST_PK");
            Strsql.Append(" AND   SHIPPERCUSTDTLS.CUSTOMER_MST_FK(+)=SHIPPERCUST.CUSTOMER_MST_PK");
            Strsql.Append(" AND   POL.PORT_MST_PK(+)=JSI.PORT_MST_POL_FK");
            Strsql.Append(" AND   POD.PORT_MST_PK(+)=JSI.PORT_MST_POD_FK");
            //Strsql.Append(" AND   JTSC.JOB_CARD_TRN_FK(+)=JSI.JOB_CARD_TRN_PK")
            Strsql.Append(" AND   STMST.SHIPPING_TERMS_MST_PK(+)=JSI.SHIPPING_TERMS_MST_FK");
            Strsql.Append(" AND   CONSCOUNTRY.COUNTRY_MST_PK(+)=CONSIGCUSTDTLS.ADM_COUNTRY_MST_FK");
            Strsql.Append(" AND   SHIPCOUNTRY.COUNTRY_MST_PK(+)=SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK ");
            Strsql.Append(" AND   AGENTMST.AGENT_MST_PK(+)=JSI.POL_AGENT_MST_FK");
            Strsql.Append(" AND   AGENTDTLS.AGENT_MST_FK(+)=AGENTMST.AGENT_MST_PK");
            Strsql.Append(" AND   AGENTCOUNTRY.COUNTRY_MST_PK(+)=AGENTDTLS.ADM_COUNTRY_MST_FK");
            Strsql.Append(" AND   DELMST.PLACE_PK(+)=JSI.DEL_PLACE_MST_FK");
            Strsql.Append(" AND CTMST.COMMODITY_GROUP_PK(+)=JSI.COMMODITY_GROUP_FK");
            Strsql.Append(" AND JTSIT.JOB_CARD_TRN_FK(+)=JSI.JOB_CARD_TRN_PK");
            Strsql.Append(" AND JTSC.JOB_CARD_TRN_FK(+)=JSI.JOB_CARD_TRN_PK");
            Strsql.Append(" AND  nvl(JTSIT.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_TP JTT WHERE JTT.JOB_CARD_TRN_FK=JTSIT.JOB_CARD_TRN_FK)");
            Strsql.Append(" AND JSI.JOB_CARD_TRN_PK IN (" + JobCardPK + ")");
            Strsql.Append(" GROUP BY JSI.JOB_CARD_TRN_PK,");
            Strsql.Append(" JSI.JOBCARD_REF_NO ,");
            Strsql.Append(" CAN.CAN_REF_NO,");
            Strsql.Append(" CAN.CUSTOM_REF_NO,");
            Strsql.Append(" CAN.CUSTOM_REF_DT,");
            Strsql.Append(" CAN.CUSTOM_ITEM_NR,");
            Strsql.Append(" JSI.ARRIVAL_DATE  ,");
            Strsql.Append(" CONSIGCUST.CUSTOMER_NAME ,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_1 ,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_2 ,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_3 ,");
            Strsql.Append("  CONSIGCUSTDTLS.ADM_ZIP_CODE ,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_PHONE_NO_1 ,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_CITY ,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_FAX_NO ,");
            Strsql.Append(" CONSIGCUSTDTLS.ADM_EMAIL_ID ,");
            Strsql.Append(" CONSCOUNTRY.COUNTRY_NAME ,");
            Strsql.Append(" SHIPPERCUST.CUSTOMER_NAME ,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_1 ,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_2 ,");
            Strsql.Append("  SHIPPERCUSTDTLS.ADM_ADDRESS_3 ,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_ZIP_CODE ,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_PHONE_NO_1 ,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_CITY ,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_FAX_NO ,");
            Strsql.Append(" SHIPPERCUSTDTLS.ADM_EMAIL_ID ,");
            Strsql.Append(" SHIPCOUNTRY.COUNTRY_NAME,");
            Strsql.Append(" AGENTMST.AGENT_NAME ,");
            Strsql.Append(" AGENTDTLS.ADM_ADDRESS_1,");
            Strsql.Append(" AGENTDTLS.ADM_ADDRESS_2 ,");
            Strsql.Append(" AGENTDTLS.ADM_ADDRESS_3,");
            Strsql.Append(" AGENTDTLS.ADM_CITY,");
            Strsql.Append(" AGENTDTLS.ADM_ZIP_CODE ,");
            Strsql.Append("  AGENTDTLS.ADM_PHONE_NO_1,");
            Strsql.Append("  AGENTDTLS.ADM_FAX_NO ,");
            Strsql.Append("  AGENTDTLS.ADM_EMAIL_ID,");
            Strsql.Append(" AGENTCOUNTRY.COUNTRY_NAME,");
            Strsql.Append(" (CASE WHEN JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
            Strsql.Append(" JSI.VESSEL_NAME || '/' || JSI.VOYAGE_FLIGHT_NO");
            Strsql.Append(" ELSE");
            Strsql.Append(" JSI.VESSEL_NAME END ) ,");
            Strsql.Append(" CTMST.COMMODITY_GROUP_CODE,");
            Strsql.Append(" (CASE WHEN JSI.HBL_HAWB_REF_NO IS NOT NULL THEN");
            Strsql.Append("  JSI.HBL_HAWB_REF_NO");
            Strsql.Append("  ELSE");
            Strsql.Append("  JSI.MBL_MAWB_REF_NO END ) ,");
            Strsql.Append(" POL.PORT_NAME ,");
            Strsql.Append(" POD.PORT_NAME ,");
            Strsql.Append(" DELMST.PLACE_NAME ,");
            Strsql.Append(" JSI.GOODS_DESCRIPTION,");
            Strsql.Append(" JSI.ETA_DATE ,");
            Strsql.Append(" JSI.ETD_DATE ,");
            Strsql.Append(" JSI.CLEARANCE_ADDRESS,");
            Strsql.Append(" JSI.MARKS_NUMBERS ,");
            Strsql.Append(" STMST.INCO_CODE,");
            Strsql.Append("  NVL(JSI.INSURANCE_AMT,0),");
            Strsql.Append(" JSI.CARGO_TYPE,");
            Strsql.Append(" JSI.PYMT_TYPE");

            try
            {
                return Objwk.GetDataSet(Strsql.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        public DataSet FetchJobCardAirDetails(string JobCardPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select JAI.JOB_CARD_TRN_PK AS JOBCARDPK,";
            Strsql += "CAN.CAN_REF_NO  JOBCARDNO,";
            Strsql += "CAN.CUSTOM_REF_NO,";
            Strsql += "TO_CHAR(CAN.CUSTOM_REF_DT, DATEFORMAT) CUSTOM_REF_DT,";
            Strsql += "CAN.CUSTOM_ITEM_NR,";
            Strsql += "TO_CHAR(JAI.ARRIVAL_DATE) AS UCRNO,";
            Strsql += "CONSIGCUST.CUSTOMER_NAME CONSIGNAME,";
            Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_1 CONSIGADD1,";
            Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_2 CONSIGADD2,";
            Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_3 CONSIGADD3,";
            Strsql += "CONSIGCUSTDTLS.ADM_ZIP_CODE CONSIGZIP,";
            Strsql += "CONSIGCUSTDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
            Strsql += "CONSIGCUSTDTLS.ADM_CITY CONSIGCITY,";
            Strsql += "CONSIGCUSTDTLS.ADM_FAX_NO CONFAX,";
            Strsql += "CONSIGCUSTDTLS.ADM_EMAIL_ID CONEMAIL,";
            Strsql += "CONSCOUNTRY.COUNTRY_NAME CONSCOUNTRY,";
            Strsql += "SHIPPERCUST.CUSTOMER_NAME SHIPPERNAME,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_1 SHIPPERADD1,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ZIP_CODE SHIPPERZIP,";
            Strsql += "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            Strsql += "SHIPPERCUSTDTLS.ADM_CITY SHIPPERCITY,";
            Strsql += "SHIPPERCUSTDTLS.ADM_FAX_NO SHIPPERFAX,";
            Strsql += "SHIPPERCUSTDTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            Strsql += "AGENTMST.AGENT_NAME AGENTNAME,";
            Strsql += "AGENTDTLS.ADM_ADDRESS_1 AGENTADD1,";
            Strsql += "AGENTDTLS.ADM_ADDRESS_2 AGENTADD2,";
            Strsql += "AGENTDTLS.ADM_ADDRESS_3 AGENTADD3,";
            Strsql += "AGENTDTLS.ADM_CITY      AGENTCITY,";
            Strsql += "AGENTDTLS.ADM_ZIP_CODE  AGENTZIP,";
            Strsql += "AGENTDTLS.ADM_PHONE_NO_1 AGENTPHONE,";
            Strsql += "AGENTDTLS.ADM_FAX_NO    AGENTFAX,";
            Strsql += "AGENTDTLS.ADM_EMAIL_ID  AGENTEMAIL,";
            Strsql += "AGENTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,";
            Strsql += "JAI.VOYAGE_FLIGHT_NO  VES_VOY,";
            Strsql += "CGMST.COMMODITY_GROUP_DESC COMMTYPE,";
            Strsql += "(CASE WHEN JAI.HBL_HAWB_REF_NO IS NOT NULL THEN";
            Strsql += "JAI.HBL_HAWB_REF_NO";
            Strsql += " ELSE";
            Strsql += "JAI.MBL_MAWB_REF_NO END ) BLREFNO,";
            Strsql += "POL.PORT_NAME POLNAME,";
            Strsql += "POD.PORT_NAME PODNAME,";
            Strsql += "DELMST.PLACE_NAME DEL_PLACE_NAME,";
            Strsql += "JAI.GOODS_DESCRIPTION,";
            Strsql += "JAI.ETD_DATE ETD,";
            Strsql += "JAI.ETA_DATE ETA,";
            Strsql += "JAI.CLEARANCE_ADDRESS CLEARANCEPOINT,";
            Strsql += "JAI.MARKS_NUMBERS MARKS,";
            Strsql += "STMST.INCO_CODE TERMS,";
            Strsql += "NVL(JAI.INSURANCE_AMT, 0) INSURANCE,";
            Strsql += "JAI.PYMT_TYPE PYMT_TYPE,";
            Strsql += " 1 CARGO_TYPE,";
            Strsql += "SUM(JTSC.GROSS_WEIGHT) GROSSWEIGHT,";
            Strsql += " '' NETWEIGHT,";
            Strsql += " SUM(JTSC.CHARGEABLE_WEIGHT) CHARWT,";
            Strsql += "SUM(JTSC.VOLUME_IN_CBM) VOLUME";
            Strsql += "from JOB_CARD_TRN JAI,";
            Strsql += "JOB_TRN_TP JTAIT,";
            Strsql += "CUSTOMER_MST_TBL CONSIGCUST,";
            Strsql += "CUSTOMER_CONTACT_DTLS CONSIGCUSTDTLS,";
            Strsql += "CUSTOMER_CONTACT_DTLS SHIPPERCUSTDTLS,";
            Strsql += "CUSTOMER_MST_TBL SHIPPERCUST,";
            Strsql += "AGENT_MST_TBL AGENTMST,";
            Strsql += "AGENT_CONTACT_DTLS AGENTDTLS,";
            Strsql += "PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD,";
            Strsql += "JOB_TRN_CONT JTSC,";
            Strsql += "SHIPPING_TERMS_MST_TBL STMST,";
            Strsql += "COUNTRY_MST_TBL SHIPCOUNTRY,";
            Strsql += "COUNTRY_MST_TBL CONSCOUNTRY,";
            Strsql += "COUNTRY_MST_TBL AGENTCOUNTRY,";
            Strsql += "PLACE_MST_TBL DELMST,";
            Strsql += "COMMODITY_GROUP_MST_TBL CGMST,";
            Strsql += "CAN_MST_TBL CAN";
            Strsql += "WHERE CONSIGCUST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
            Strsql += " AND CAN.JOB_CARD_FK(+)=JAI.JOB_CARD_TRN_PK";
            Strsql += " AND   JTAIT.JOB_CARD_TRN_FK(+)= JAI.JOB_CARD_TRN_PK";
            Strsql += "AND  nvl(JTAIT.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_TP JTT WHERE JTT.JOB_CARD_TRN_FK=JTAIT.JOB_CARD_TRN_FK)";
            Strsql += "AND   SHIPPERCUST.CUSTOMER_MST_PK(+)=JAI.SHIPPER_CUST_MST_FK";
            Strsql += "AND   CONSIGCUSTDTLS.CUSTOMER_MST_FK(+)=CONSIGCUST.CUSTOMER_MST_PK";
            Strsql += "AND   SHIPPERCUSTDTLS.CUSTOMER_MST_FK(+)=SHIPPERCUST.CUSTOMER_MST_PK";
            Strsql += "AND   POL.PORT_MST_PK(+)=JAI.PORT_MST_POL_FK";
            Strsql += "AND   POD.PORT_MST_PK(+)=JAI.PORT_MST_POD_FK";
            Strsql += "AND   JTSC.JOB_CARD_TRN_FK(+)=JAI.JOB_CARD_TRN_PK";
            Strsql += "AND   STMST.SHIPPING_TERMS_MST_PK(+)=JAI.SHIPPING_TERMS_MST_FK";
            Strsql += "AND   CONSCOUNTRY.COUNTRY_MST_PK(+)=CONSIGCUSTDTLS.ADM_COUNTRY_MST_FK";
            Strsql += "AND   SHIPCOUNTRY.COUNTRY_MST_PK(+)=SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK ";
            Strsql += "AND   AGENTMST.AGENT_MST_PK(+)=JAI.POL_AGENT_MST_FK";
            Strsql += "AND   AGENTDTLS.AGENT_MST_FK(+)=AGENTMST.AGENT_MST_PK";
            Strsql += "AND   AGENTCOUNTRY.COUNTRY_MST_PK(+)=AGENTDTLS.ADM_COUNTRY_MST_FK";
            Strsql += "AND   DELMST.PLACE_PK(+)=JAI.DEL_PLACE_MST_FK";
            Strsql += "AND   CGMST.COMMODITY_GROUP_PK(+)=JAI.COMMODITY_GROUP_FK";
            Strsql += "AND   JAI.JOB_CARD_TRN_PK IN (" + JobCardPK + ")";
            Strsql += "GROUP BY JAI.JOB_CARD_TRN_PK,";
            Strsql += "JAI.JOBCARD_REF_NO ,";
            Strsql += "CAN.CAN_REF_NO,";
            Strsql += "CAN.CUSTOM_REF_NO,";
            Strsql += "CAN.CUSTOM_REF_DT,";
            Strsql += "CAN.CUSTOM_ITEM_NR,";
            Strsql += "JAI.ARRIVAL_DATE  ,";
            Strsql += "CONSIGCUST.CUSTOMER_NAME ,";
            Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_1 ,";
            Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_2 ,";
            Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_3 ,";
            Strsql += " CONSIGCUSTDTLS.ADM_ZIP_CODE ,";
            Strsql += "CONSIGCUSTDTLS.ADM_PHONE_NO_1 ,";
            Strsql += "CONSIGCUSTDTLS.ADM_CITY ,";
            Strsql += "CONSIGCUSTDTLS.ADM_FAX_NO ,";
            Strsql += "CONSIGCUSTDTLS.ADM_EMAIL_ID ,";
            Strsql += "CONSCOUNTRY.COUNTRY_NAME ,";
            Strsql += "SHIPPERCUST.CUSTOMER_NAME ,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_1 ,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_2 ,";
            Strsql += " SHIPPERCUSTDTLS.ADM_ADDRESS_3 ,";
            Strsql += "SHIPPERCUSTDTLS.ADM_ZIP_CODE ,";
            Strsql += "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 ,";
            Strsql += "SHIPPERCUSTDTLS.ADM_CITY ,";
            Strsql += "SHIPPERCUSTDTLS.ADM_FAX_NO ,";
            Strsql += "SHIPPERCUSTDTLS.ADM_EMAIL_ID ,";
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME,";
            Strsql += "AGENTMST.AGENT_NAME ,";
            Strsql += "AGENTDTLS.ADM_ADDRESS_1,";
            Strsql += "AGENTDTLS.ADM_ADDRESS_2 ,";
            Strsql += "AGENTDTLS.ADM_ADDRESS_3,";
            Strsql += "AGENTDTLS.ADM_CITY,";
            Strsql += "AGENTDTLS.ADM_ZIP_CODE ,";
            Strsql += " AGENTDTLS.ADM_PHONE_NO_1,";
            Strsql += " AGENTDTLS.ADM_FAX_NO ,";
            Strsql += " AGENTDTLS.ADM_EMAIL_ID,";
            Strsql += "AGENTCOUNTRY.COUNTRY_NAME,";
            Strsql += "JAI.VOYAGE_FLIGHT_NO ,";
            Strsql += "CGMST.COMMODITY_GROUP_DESC,";
            Strsql += "(CASE WHEN JAI.HBL_HAWB_REF_NO IS NOT NULL THEN";
            Strsql += " JAI.HBL_HAWB_REF_NO";
            Strsql += " ELSE";
            Strsql += " JAI.MBL_MAWB_REF_NO END ) ,";
            Strsql += "POL.PORT_NAME ,";
            Strsql += "POD.PORT_NAME ,";
            Strsql += "DELMST.PLACE_NAME ,";
            Strsql += "JAI.GOODS_DESCRIPTION,";
            Strsql += "JAI.ETD_DATE ,";
            Strsql += "JAI.ETA_DATE ,";
            Strsql += "JAI.CLEARANCE_ADDRESS,";
            Strsql += "JAI.MARKS_NUMBERS ,";
            Strsql += "STMST.INCO_CODE,";
            Strsql += " NVL(JAI.INSURANCE_AMT,0),";
            Strsql += "JAI.PYMT_TYPE";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        #endregion "Fetch Job Card Sea/Air Details For Report"

        #region "Fetch Collect Charges"

        public DataSet FetchCollectChargesAirDetails(string JobCardPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select JAFD.JOB_TRN_FD_PK JOBFDIMPPK,";
            Strsql += "JAFD.JOB_CARD_TRN_FK JOBIMPPK,";
            Strsql += "JAFD.FREIGHT_ELEMENT_MST_FK,";
            Strsql += "FREELE.FREIGHT_ELEMENT_ID,";
            Strsql += "FREELE.FREIGHT_ELEMENT_NAME,";
            Strsql += "JAFD.FREIGHT_AMT,";
            Strsql += "CURRTYPE.CURRENCY_ID,";
            Strsql += "CURRTYPE.CURRENCY_NAME";
            Strsql += "from   JOB_TRN_FD JAFD,";
            Strsql += "JOB_CARD_TRN JAI,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FREELE,";
            Strsql += "CURRENCY_TYPE_MST_TBL CURRTYPE";
            Strsql += "WHERE FREELE.FREIGHT_ELEMENT_MST_PK = JAFD.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND    CURRTYPE.CURRENCY_MST_PK=JAFD.CURRENCY_MST_FK";
            Strsql += "AND    JAI.JOB_CARD_TRN_PK=JAFD.JOB_CARD_TRN_FK";
            Strsql += "and JAFD.Freight_Type = 2";
            Strsql += "AND FREELE.FREIGHT_ELEMENT_ID LIKE 'AFC'";
            Strsql += "AND jAfd.JOB_CARD_TRN_FK IN (" + JobCardPK + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
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

        public DataSet FetchCollectChargesSeaDetails(string JobCardPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "select JSFD.JOB_TRN_FD_PK JOBFDIMPPK,";
            Strsql += "JSFD.JOB_CARD_TRN_FK JOBIMPPK,";
            Strsql += "JSFD.FREIGHT_ELEMENT_MST_FK,";
            Strsql += "FREELE.FREIGHT_ELEMENT_ID,";
            Strsql += "FREELE.FREIGHT_ELEMENT_NAME,";
            Strsql += "JSFD.FREIGHT_AMT,";
            Strsql += "CURRTYPE.CURRENCY_ID,";
            Strsql += "CURRTYPE.CURRENCY_NAME";
            Strsql += "from   JOB_TRN_FD JSFD,";
            Strsql += "JOB_CARD_TRN JSI,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FREELE,";
            Strsql += "CURRENCY_TYPE_MST_TBL CURRTYPE";
            Strsql += "WHERE FREELE.FREIGHT_ELEMENT_MST_PK = JSFD.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND    CURRTYPE.CURRENCY_MST_PK=JSFD.CURRENCY_MST_FK";
            Strsql += "AND    JSI.JOB_CARD_TRN_PK=JSFD.JOB_CARD_TRN_FK";
            Strsql += "and JSFD.Freight_Type = 2";
            Strsql += "AND FREELE.FREIGHT_ELEMENT_ID LIKE 'BOF'";
            Strsql += "AND jsfd.JOB_CARD_TRN_FK IN (" + JobCardPK + ")";
            try
            {
                return Objwk.GetDataSet(Strsql);
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

        public DataSet FetchSeaContainers(string JobRefNos)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT JSI.JOB_CARD_TRN_FK JOBPK ,JSI.CONTAINER_NUMBER CONTAINER FROM JOB_TRN_CONT JSI ";
            Strsql += "WHERE JSI.JOB_CARD_TRN_FK IN(" + JobRefNos + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        public DataSet FetchAirPalette(string JobRefNos)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT JAI.JOB_CARD_TRN_FK JOBPK ,JAI.PALETTE_SIZE CONTAINER FROM JOB_TRN_CONT JAI ";
            Strsql += "WHERE JAI.JOB_CARD_TRN_FK IN(" + JobRefNos + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        public DataSet FetchFreightSeaCharge(string JobCardPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();

            Strsql += "select FREELE.FREIGHT_ELEMENT_ID,";
            Strsql += "FREELE.FREIGHT_ELEMENT_NAME,";
            Strsql += "JSFD.FREIGHT_AMT AS AMOUNT,";
            Strsql += "nvl(JSFD.FREIGHT_AMT * GET_EX_RATE(JSFD.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",JSI.JOBCARD_DATE)";
            Strsql += " ,0) LOCALAMT,";
            Strsql += "CURRTYPE.CURRENCY_ID,";
            Strsql += "jsfd.JOB_CARD_TRN_FK,";
            Strsql += "case";
            Strsql += "when JSFD.FREIGHT_TYPE = 1 then";
            Strsql += "'prepaid'";
            Strsql += "else";
            Strsql += "'collect'";
            Strsql += "end PymtType";
            Strsql += "from   JOB_TRN_FD JSFD,";
            Strsql += "JOB_CARD_TRN JSI,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FREELE,";
            Strsql += "CORPORATE_MST_TBL       CO,";
            Strsql += "CURRENCY_TYPE_MST_TBL CURRTYPE";
            Strsql += "WHERE FREELE.FREIGHT_ELEMENT_MST_PK = JSFD.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND    CURRTYPE.CURRENCY_MST_PK=JSFD.CURRENCY_MST_FK";
            Strsql += "AND    JSI.JOB_CARD_TRN_PK=JSFD.JOB_CARD_TRN_FK";
            Strsql += "AND jsfd.JOB_CARD_TRN_FK IN (" + JobCardPK + ")";
            Strsql += "UNION";
            Strsql += "select FREELE.FREIGHT_ELEMENT_ID,";
            Strsql += "FREELE.FREIGHT_ELEMENT_NAME,";
            Strsql += "JSFD.AMOUNT AS AMOUNT,";
            Strsql += "nvl(JSFD.AMOUNT * GET_EX_RATE(JSFD.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",JSI.JOBCARD_DATE)";
            Strsql += " ,0) LOCALAMT,";
            Strsql += "CURRTYPE.CURRENCY_ID,";
            Strsql += "jsfd.JOB_CARD_TRN_FK,";
            Strsql += "case";
            Strsql += "when JSFD.FREIGHT_TYPE = 1 then";
            Strsql += "'prepaid'";
            Strsql += "else";
            Strsql += "'collect'";
            Strsql += "end PymtType";
            Strsql += "from   job_trn_oth_chrg JSFD,";
            Strsql += "JOB_CARD_TRN JSI,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FREELE,";
            Strsql += "CORPORATE_MST_TBL       CO,";
            Strsql += "CURRENCY_TYPE_MST_TBL CURRTYPE";
            Strsql += "WHERE FREELE.FREIGHT_ELEMENT_MST_PK = JSFD.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND    CURRTYPE.CURRENCY_MST_PK=JSFD.CURRENCY_MST_FK";
            Strsql += "AND    JSI.JOB_CARD_TRN_PK=JSFD.JOB_CARD_TRN_FK";
            Strsql += "AND jsfd.JOB_CARD_TRN_FK IN (" + JobCardPK + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
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

        public DataSet FetchFreightAirCharge(string JobCardPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql += "select FREELE.FREIGHT_ELEMENT_ID,";
            Strsql += "FREELE.FREIGHT_ELEMENT_NAME,";
            Strsql += "JAFD.FREIGHT_AMT AS AMOUNT,";
            Strsql += "nvl(JAFD.FREIGHT_AMT * GET_EX_RATE(JAFD.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",JAI.JOBCARD_DATE)";
            Strsql += " ,0) LOCALAMT,";
            Strsql += "CURRTYPE.CURRENCY_ID,";
            Strsql += " 0 JOB_CARD_TRN_FK,";
            Strsql += "case";
            Strsql += "when jai.pymt_type = 1 then";
            Strsql += "'prepaid'";
            Strsql += "else";
            Strsql += "'collect'";
            Strsql += "end PymtType";
            Strsql += "from   JOB_TRN_FD JAFD,";
            Strsql += "JOB_CARD_TRN JAI,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FREELE,";
            Strsql += "CURRENCY_TYPE_MST_TBL CURRTYPE";
            Strsql += "WHERE FREELE.FREIGHT_ELEMENT_MST_PK = JAFD.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND    CURRTYPE.CURRENCY_MST_PK=JAFD.CURRENCY_MST_FK";
            Strsql += "AND    JAI.JOB_CARD_TRN_PK=JAFD.JOB_CARD_TRN_FK";
            Strsql += "AND JAFD.JOB_CARD_TRN_FK IN (" + JobCardPK + ")";
            Strsql += "UNION";
            Strsql += "select FREELE.FREIGHT_ELEMENT_ID,";
            Strsql += "FREELE.FREIGHT_ELEMENT_NAME,";
            Strsql += "JAFD.AMOUNT AS AMOUNT,";
            Strsql += "nvl(JAFD.AMOUNT * GET_EX_RATE(JAFD.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",JAI.JOBCARD_DATE)";
            Strsql += " ,0) LOCALAMT,";
            Strsql += "CURRTYPE.CURRENCY_ID,";
            Strsql += " 0 JOB_CARD_TRN_FK,";
            Strsql += "case";
            Strsql += "when jai.pymt_type = 1 then";
            Strsql += "'prepaid'";
            Strsql += "else";
            Strsql += "'collect'";
            Strsql += "end PymtType";
            Strsql += "from  job_trn_oth_chrg JAFD,";
            Strsql += "JOB_CARD_TRN JAI,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FREELE,";
            Strsql += "CURRENCY_TYPE_MST_TBL CURRTYPE";
            Strsql += "WHERE FREELE.FREIGHT_ELEMENT_MST_PK = JAFD.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND    CURRTYPE.CURRENCY_MST_PK=JAFD.CURRENCY_MST_FK";
            Strsql += "AND    JAI.JOB_CARD_TRN_PK=JAFD.JOB_CARD_TRN_FK";
            Strsql += "AND JAFD.JOB_CARD_TRN_FK IN (" + JobCardPK + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
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

        #endregion "Fetch Collect Charges"

        #region " Enhance Search & Lookup Search Block FOR HBL NO"

        public string Fetch_Arrival_HBL_NO(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string ArrivalDate = "";
            Array arr = null;
            string strLOCATION_IN = null;
            string strSERACH_IN = null;
            int strBUSINESS_MODEL_IN = 0;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            if (arr.Length > 2)
            {
                if (Convert.ToString(arr.GetValue(2)).Trim().Length > 0)
                    strBUSINESS_MODEL_IN = Convert.ToInt32(arr.GetValue(2));
            }
            if (arr.Length > 3)
            {
                if (Convert.ToString(arr.GetValue(3)).Trim().Length > 0)
                    ArrivalDate = "To_date('" + arr.GetValue(0) + "', dateformat)";
            }
            if (arr.Length > 4)
            {
                if (Convert.ToString(arr.GetValue(4)).Trim().Length > 0)
                    strLOCATION_IN = Convert.ToString(arr.GetValue(0));
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_HBL_REF_NO_PKG.GET_HBL_REF_COMMON_ARRIVAL_HBL";
                var _with5 = selectCommand.Parameters;

                _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("BUSINESS_TYPE_IN", strBUSINESS_MODEL_IN).Direction = ParameterDirection.Input;
                _with5.Add("LOCATION_IN", (!string.IsNullOrEmpty(loc.Trim()) ? loc : "")).Direction = ParameterDirection.Input;
                _with5.Add("ARRVIAL_DT", (string.IsNullOrEmpty(ArrivalDate.Trim()) ? "" : ArrivalDate)).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);

                return strReturn;
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

        #endregion " Enhance Search & Lookup Search Block FOR HBL NO"

        #region " Enhance Search Functionality FetchForMblRefInMbllist "

        public string FetchForArrivalMblRefInMbllist(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strArrivalDt = "";
            string strLOCATION_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            if (arr.Length > 2)
            {
                if (Convert.ToString(arr.GetValue(2)).Trim().Length > 0)
                    strBusiType = Convert.ToString(arr.GetValue(2));
            }
            if (arr.Length > 3)
            {
                if (Convert.ToString(arr.GetValue(3)).Trim().Length > 0)
                    strArrivalDt = "To_date('" + Convert.ToString(arr.GetValue(3)) + "', dateformat)";
            }
            if (arr.Length > 4)
            {
                if (Convert.ToString(arr.GetValue(4)).Trim().Length > 0)
                    strLOCATION_IN = Convert.ToString(arr.GetValue(4));
            }

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_FOR_MBL_LIST.GET_MBL_REF_NO_ARRIVAL";
                var _with6 = SCM.Parameters;
                _with6.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN.Trim()) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with6.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with6.Add("ARRVIAL_DT", (string.IsNullOrEmpty(strArrivalDt.Trim()) ? "" : strArrivalDt)).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
                SCM.Connection.Close();
            }
        }

        #endregion " Enhance Search Functionality FetchForMblRefInMbllist "

        #region "Booking Pending for Customs Clearance Function"

        public DataSet FetchCustomsDetails(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 Excel = 0,
        Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = '0';
            Int32 start = '0';
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JCSET.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And JCSET.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }

                sb.Append("SELECT DISTINCT BST.BOOKING_MST_PK,");
                sb.Append("                BST.BOOKING_REF_NO,");
                sb.Append("                JCSET.JOB_CARD_TRN_PK,");
                sb.Append("                JCSET.JOBCARD_REF_NO,");
                sb.Append("                TO_CHAR(JCSET.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("                TO_CHAR(BST.SHIPMENT_DATE,DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POD.PORT_ID,");
                sb.Append("                ");
                sb.Append("                CASE");
                sb.Append("                  WHEN VVT.VESSEL_NAME IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_VOYAGE\",");
                sb.Append("                TO_CHAR(JCSET.ETD_DATE,DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("                TO_CHAR(JCSET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CTMT.CONTAINER_TYPE_MST_ID,");
                sb.Append("                JCONT.CONTAINER_NUMBER,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                JCSET.SB_NUMBER,");
                sb.Append("                TO_CHAR(JCSET.SB_DATE,DATEFORMAT) SB_DATE");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_SEA_EXP_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK ");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND JCSET.CRQ = 0");
                sb.Append("   AND BST.STATUS <> 3");
                sb.Append("   AND JCSET.CRQ_DATE IS NULL");
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append("");
                sb.Append(strCondition);

                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";

                objWF.MyDataReader = objWF.GetDataReader(strSQL);
                while (objWF.MyDataReader.Read())
                {
                    TotalRecords = objWF.MyDataReader.GetInt32(0);
                }
                objWF.MyDataReader.Close();
                //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
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
                sb.Append(" ORDER BY BOOKING_MST_PK DESC");

                strSQL = " SELECT * FROM (SELECT ROWNUM SLNO, q.* FROM(";
                strSQL += sb.ToString();

                if (Excel == 0)
                {
                    strSQL += " )q ) WHERE SLNO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " )q )";
                }
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Booking Pending for Customs Clearance Function"

        #region "Customs Clearance Print"

        public DataSet FetchCustomsDetailsReport(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT BST.BOOKING_MST_PK,");
                sb.Append("                BST.BOOKING_REF_NO,");
                sb.Append("                JCSET.JOB_CARD_TRN_PK,");
                sb.Append("                JCSET.JOBCARD_REF_NO,");
                sb.Append("                JCSET.JOBCARD_DATE,");
                sb.Append("                TO_CHAR(BST.SHIPMENT_DATE,DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POD.PORT_ID,");
                sb.Append("                ");
                sb.Append("                CASE");
                sb.Append("                  WHEN VVT.VESSEL_NAME IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_VOYAGE\",");
                sb.Append("                JCSET.ETD_DATE,");
                sb.Append("                JCSET.ETA_DATE,");
                sb.Append("                DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CTMT.CONTAINER_TYPE_MST_ID,");
                sb.Append("                JCONT.CONTAINER_NUMBER,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                JCSET.SB_NUMBER,");
                sb.Append("                TO_CHAR(JCSET.SB_DATE,DATEFORMAT) SB_DATE");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_SEA_EXP_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK ");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND JCSET.CRQ = 0");
                sb.Append("   AND JCSET.CRQ_DATE IS NULL");
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And BST.VESSEL_NAME = '" + VslName + "'");
                }
                sb.Append(" ORDER BY JCSET.JOBCARD_DATE DESC, JCSET.JOBCARD_REF_NO DESC");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Customs Clearance Print"

        #region "Get PODPK"

        public string GetPodPK(string JOBPK, string BizType)
        {
            StringBuilder sb = new StringBuilder();
            WorkFlow Objwk = new WorkFlow();
            if (Convert.ToInt32(BizType) == 2)
            {
                sb.Append("  SELECT JOB.PORT_MST_POD_FK FROM JOB_CARD_TRN JOB WHERE JOB.JOB_CARD_TRN_PK=" + JOBPK + "");
            }
            else
            {
                sb.Append("  SELECT JOB.PORT_MST_POD_FK FROM JOB_CARD_TRN JOB WHERE JOB.JOB_CARD_TRN_PK=" + JOBPK + "");
            }
            try
            {
                return Objwk.ExecuteScaler(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return "";
        }

        #endregion "Get PODPK"
    }
}