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

//Option Strict On

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
    public class cls_CustomerService : CommonFeatures
    {

        #region "Fetch All Function"
        public int lngPkVal;
        //auto generated function
        public DataSet FetchAll(Int64 P_Customer_Service_Pk, string P_Cs_Ref_No, string P_Cs_Dt, Int64 P_Is_Escallated, Int64 P_Customer_Service_Fk, Int64 P_Csr_Mst_Fk, string P_Caller, Int64 P_Customer_Mst_Fk, Int64 P_Call_Type, Int64 P_Booking_Trn_Fk,
        Int64 P_Booking_Bl_Fk, Int64 P_Voyage_Fk, Int64 P_Assign_To_Fk, string P_Assign_Dt, Int64 P_Escallated_To_Fk, string P_Escallation_Dt, Int64 P_Cs_Problem_Type_Fk, string P_Issue_Description, Int64 P_Problem_Qtn, Int64 P_Problem_Srr,
        Int64 P_Problem_Pbkg, Int64 P_Problem_Vbkg, Int64 P_Problem_Via, Int64 P_Problem_Bkg, Int64 P_Problem_Gtin, Int64 P_Problem_Onboard, Int64 P_Problem_Doc, Int64 P_Problem_Inv, Int64 P_Problem_Coll, Int64 P_Problem_Dischg,
        Int64 P_Status, Int64 P_Resolved_By_Fk, Int64 P_Resolution_Dt, string P_Resolution, string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ROWNUM SR_NO, ";
            strSQL = strSQL + " Customer_Service_Pk,";
            strSQL = strSQL + " Cs_Ref_No,";
            strSQL = strSQL + " Cs_Dt,";
            strSQL = strSQL + " Is_Escallated,";
            strSQL = strSQL + " Customer_Service_Fk,";
            strSQL = strSQL + " Csr_Mst_Fk,";
            strSQL = strSQL + " Caller,";
            strSQL = strSQL + " Customer_Mst_Fk,";
            strSQL = strSQL + " Call_Type,";
            strSQL = strSQL + " Booking_Trn_Fk,";
            strSQL = strSQL + " Booking_Bl_Fk,";
            strSQL = strSQL + " Voyage_Fk,";
            strSQL = strSQL + " Assign_To_Fk,";
            strSQL = strSQL + " Assign_Dt,";
            strSQL = strSQL + " Escallated_To_Fk,";
            strSQL = strSQL + " Escallation_Dt,";
            strSQL = strSQL + " Cs_Problem_Type_Fk,";
            strSQL = strSQL + " Issue_Description,";
            strSQL = strSQL + " Problem_Qtn,";
            strSQL = strSQL + " Problem_Srr,";
            strSQL = strSQL + " Problem_Pbkg,";
            strSQL = strSQL + " Problem_Vbkg,";
            strSQL = strSQL + " Problem_Via,";
            strSQL = strSQL + " Problem_Bkg,";
            strSQL = strSQL + " Problem_Gtin,";
            strSQL = strSQL + " Problem_Onboard,";
            strSQL = strSQL + " Problem_Doc,";
            strSQL = strSQL + " Problem_Inv,";
            strSQL = strSQL + " Problem_Coll,";
            strSQL = strSQL + " Problem_Dischg,";
            strSQL = strSQL + " Status,";
            strSQL = strSQL + " Resolved_By_Fk,";
            strSQL = strSQL + " Resolution_Dt,";
            strSQL = strSQL + " Resolution,";
            strSQL = strSQL + " Created_By_Fk,";
            strSQL = strSQL + " Created_Dt,";
            strSQL = strSQL + " Last_Modified_By_Fk,";
            strSQL = strSQL + " Last_Modified_Dt,";
            strSQL = strSQL + " Version_No ";
            strSQL = strSQL + " FROM CUSTOMER_SERVICE_TRN ";
            strSQL = strSQL + " WHERE ( 1 = 1) ";
            if (P_Customer_Service_Pk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Customer_Service_Pk like '%" + P_Customer_Service_Pk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Customer_Service_Pk like '" + P_Customer_Service_Pk + "%' ";
                }
            }
            else
            {
            }
            if (P_Cs_Ref_No.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Cs_Ref_No like '%" + P_Cs_Ref_No + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Cs_Ref_No like '" + P_Cs_Ref_No + "%' ";
                }
            }
            else
            {
            }
            if (P_Cs_Dt.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Cs_Dt like '%" + P_Cs_Dt + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Cs_Dt like '" + P_Cs_Dt + "%' ";
                }
            }
            else
            {
            }
            if (P_Is_Escallated.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Is_Escallated like '%" + P_Is_Escallated + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Is_Escallated like '" + P_Is_Escallated + "%' ";
                }
            }
            else
            {
            }
            if (P_Customer_Service_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Customer_Service_Fk like '%" + P_Customer_Service_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Customer_Service_Fk like '" + P_Customer_Service_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Csr_Mst_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Csr_Mst_Fk like '%" + P_Csr_Mst_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Csr_Mst_Fk like '" + P_Csr_Mst_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Caller.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Caller like '%" + P_Caller + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Caller like '" + P_Caller + "%' ";
                }
            }
            else
            {
            }
            if (P_Customer_Mst_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Customer_Mst_Fk like '%" + P_Customer_Mst_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Customer_Mst_Fk like '" + P_Customer_Mst_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Call_Type.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Call_Type like '%" + P_Call_Type + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Call_Type like '" + P_Call_Type + "%' ";
                }
            }
            else
            {
            }
            if (P_Booking_Trn_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Booking_Trn_Fk like '%" + P_Booking_Trn_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Booking_Trn_Fk like '" + P_Booking_Trn_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Booking_Bl_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Booking_Bl_Fk like '%" + P_Booking_Bl_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Booking_Bl_Fk like '" + P_Booking_Bl_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Voyage_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Voyage_Fk like '%" + P_Voyage_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Voyage_Fk like '" + P_Voyage_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Assign_To_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Assign_To_Fk like '%" + P_Assign_To_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Assign_To_Fk like '" + P_Assign_To_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Assign_Dt.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Assign_Dt like '%" + P_Assign_Dt + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Assign_Dt like '" + P_Assign_Dt + "%' ";
                }
            }
            else
            {
            }
            if (P_Escallated_To_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Escallated_To_Fk like '%" + P_Escallated_To_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Escallated_To_Fk like '" + P_Escallated_To_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Escallation_Dt.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Escallation_Dt like '%" + P_Escallation_Dt + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Escallation_Dt like '" + P_Escallation_Dt + "%' ";
                }
            }
            else
            {
            }
            if (P_Cs_Problem_Type_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Cs_Problem_Type_Fk like '%" + P_Cs_Problem_Type_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Cs_Problem_Type_Fk like '" + P_Cs_Problem_Type_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Issue_Description.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Issue_Description like '%" + P_Issue_Description + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Issue_Description like '" + P_Issue_Description + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Qtn.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Qtn like '%" + P_Problem_Qtn + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Qtn like '" + P_Problem_Qtn + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Srr.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Srr like '%" + P_Problem_Srr + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Srr like '" + P_Problem_Srr + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Pbkg.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Pbkg like '%" + P_Problem_Pbkg + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Pbkg like '" + P_Problem_Pbkg + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Vbkg.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Vbkg like '%" + P_Problem_Vbkg + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Vbkg like '" + P_Problem_Vbkg + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Via.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Via like '%" + P_Problem_Via + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Via like '" + P_Problem_Via + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Bkg.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Bkg like '%" + P_Problem_Bkg + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Bkg like '" + P_Problem_Bkg + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Gtin.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Gtin like '%" + P_Problem_Gtin + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Gtin like '" + P_Problem_Gtin + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Onboard.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Onboard like '%" + P_Problem_Onboard + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Onboard like '" + P_Problem_Onboard + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Doc.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Doc like '%" + P_Problem_Doc + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Doc like '" + P_Problem_Doc + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Inv.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Inv like '%" + P_Problem_Inv + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Inv like '" + P_Problem_Inv + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Coll.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Coll like '%" + P_Problem_Coll + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Coll like '" + P_Problem_Coll + "%' ";
                }
            }
            else
            {
            }
            if (P_Problem_Dischg.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Problem_Dischg like '%" + P_Problem_Dischg + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Problem_Dischg like '" + P_Problem_Dischg + "%' ";
                }
            }
            else
            {
            }
            if (P_Status.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Status like '%" + P_Status + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Status like '" + P_Status + "%' ";
                }
            }
            else
            {
            }
            if (P_Resolved_By_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Resolved_By_Fk like '%" + P_Resolved_By_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Resolved_By_Fk like '" + P_Resolved_By_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Resolution_Dt.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Resolution_Dt like '%" + P_Resolution_Dt + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Resolution_Dt like '" + P_Resolution_Dt + "%' ";
                }
            }
            else
            {
            }
            if (P_Resolution.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Resolution like '%" + P_Resolution + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Resolution like '" + P_Resolution + "%' ";
                }
            }
            else
            {
            }
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

        #endregion

        #region "FetchList"
        //this function is used to fetch the details for the list screen
        public DataSet FetchList(Int16 CUSTOMER_SERVICE_PK = 0, string CS_REF_NO = "", string FromDate = "", string ToDate = "", string Customer_ID = "", string JobcardNo = "", Int64 status = 0, string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, int ComSchTrnPk = 0, int busstype = 0, int process = 0, Int32 flag = 0, string AssignToPk = "", string EscalateToPk = "", string CallType = "", string AssignTo = "", string EscalateTo = "",
        bool isadminusr = false)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCondition = "";
            if (CUSTOMER_SERVICE_PK != 0)
            {
                strCondition += " AND CST.CUSTOMER_SERVICE_PK = " + CUSTOMER_SERVICE_PK;
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (CS_REF_NO.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And UPPER(CST.CS_REF_NO) like '%" + CS_REF_NO.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And UPPER(CST.CS_REF_NO) like '" + CS_REF_NO.ToUpper().Replace("'", "''") + "%' ";
                }
            }
            else
            {
            }


            if (busstype == 3)
            {
                strCondition = strCondition + " AND CST.BIZ_TYPE in(1,2,3) ";
            }
            else if (busstype == 2)
            {
                strCondition = strCondition + " AND CST.BIZ_TYPE in(2) ";
            }
            else
            {
                busstype = 1;
                strCondition = strCondition + " AND CST.BIZ_TYPE IN(1)";
            }

            if (process != 0)
            {
                strCondition = strCondition + " AND CST.PROCESS = " + process;
            }


            if (!string.IsNullOrEmpty(AssignTo))
            {
                strCondition = strCondition + " AND CST.ASSIGN_TO_FK = " + AssignToPk;
            }
            if (!string.IsNullOrEmpty(EscalateTo))
            {
                strCondition = strCondition + " AND CST.ESCALLATED_TO_FK = " + EscalateToPk;
            }
            if (!string.IsNullOrEmpty(CallType))
            {
                strCondition = strCondition + " AND CST.CALL_TYPE = " + CallType;
            }


            if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                string frmDt = System.String.Format("{0:dd-MMM-yyyy}", FromDate);
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", ToDate);
                strCondition = strCondition + " AND to_date(CST.CS_DT,'dd-mm-yyyy') BETWEEN '" + frmDt.ToUpper() + "' AND to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')";
            }
            else if (!string.IsNullOrEmpty(FromDate) & string.IsNullOrEmpty(ToDate))
            {
                string frmDt = System.String.Format("{0:dd-MMM-yyyy}", FromDate);
                strCondition = strCondition + " AND to_date(CST.CS_DT,'dd-mm-yyyy') >= '" + frmDt.ToUpper() + "'";
            }
            else if (string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                string toDt = System.String.Format("{0:dd-MMM-yyyy}", ToDate);
                strCondition = strCondition + " AND to_date(CST.CS_DT,'dd-mm-yyyy') <= to_date('" + toDt.ToUpper() + "','dd-mon-yyyy')";
            }

            if (Customer_ID.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " AND UPPER(CUS.CUSTOMER_ID)  like '%" + Customer_ID.ToUpper().Replace("'", "''") + "%'";
                }
                else
                {
                    strCondition = strCondition + " AND UPPER(CUS.CUSTOMER_ID)  like '" + Customer_ID.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (JobcardNo.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " AND UPPER(JC.JOBCARD_REF_NO) LIKE   '%" + JobcardNo.ToUpper().Replace("'", "''") + "%'";
                }
                else
                {
                    strCondition = strCondition + " AND UPPER(JC.JOBCARD_REF_NO) LIKE   '" + JobcardNo.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (isadminusr == true)
            {
            }
            else
            {
                strCondition = strCondition + " AND UMT.DEFAULT_LOCATION_FK  =" + Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            }
            if (status == 0)
            {
                strCondition = strCondition + " AND CST.STATUS = " + status;
            }
            else if (status == 1)
            {
                strCondition = strCondition + " AND CST.STATUS = " + status;
            }
            else if (status == 2)
            {
                strCondition = strCondition + " AND CST.STATUS = 0 AND CST.ESCALLATED_TO_FK IS NOT NULL ";
            }
            else if (status == 3)
            {
                strCondition = strCondition + " AND CST.STATUS >=0 ";
            }
            //'Commented By Koteshwari on 1-jan-2011
            //strSQL = String.Empty & vbCrLf
            //strSQL &= "SELECT COUNT(*) FROM (SELECT DISTINCT CST.CUSTOMER_SERVICE_PK, " & vbCrLf
            //strSQL &= "       CST.CS_REF_NO, " & vbCrLf
            //strSQL &= "       TO_CHAR(CST.CS_DT,'" & dateFormat & " ') CS_DT, " & vbCrLf
            //strSQL &= "       JC.JOBCARD_REF_NO, " & vbCrLf
            //strSQL &= "       CUS.CUSTOMER_ID, " & vbCrLf
            //strSQL &= "       (CASE  WHEN  CALL_TYPE =1 THEN 'PROBLEM' WHEN CALL_TYPE = 2 THEN 'ENQUIRY' ELSE '' END) CALL_TYPE, " & vbCrLf
            //strSQL &= "       (CASE WHEN CST.STATUS = 0 THEN 'OPEN' WHEN CST.STATUS=1 THEN 'CLOSED' END ) STATUS        " & vbCrLf
            //strSQL &= " FROM CUSTOMER_SERVICE_TRN CST, " & vbCrLf
            //strSQL &= "     CUSTOMER_MST_TBL CUS, " & vbCrLf
            //If busstype = 1 And process = 1 Then  ' Air Export
            //    strSQL &= "     JOB_CARD_AIR_EXP_TBL JC " & vbCrLf
            //ElseIf busstype = 1 And process = 2 Then  'Air Import
            //    strSQL &= "     JOB_CARD_AIR_IMP_TBL JC " & vbCrLf
            //ElseIf busstype = 2 And process = 1 Then  ' Sea Export
            //    strSQL &= "     JOB_CARD_SEA_EXP_TBL JC " & vbCrLf
            //Else    ' Sea Import
            //    strSQL &= "     JOB_CARD_SEA_IMP_TBL JC " & vbCrLf
            //End If
            //strSQL &= " WHERE CST.CUSTOMER_MST_FK=CUS.CUSTOMER_MST_PK " & vbCrLf

            //If busstype = 1 And process = 1 Then  ' AIR EXPORT
            //    strSQL &= " AND   CST.JOB_CARD_FK=JC.JOB_CARD_AIR_EXP_PK " & vbCrLf
            //ElseIf busstype = 1 And process = 2 Then  'AIR IMPORT
            //    strSQL &= " AND   CST.JOB_CARD_FK=JC.JOB_CARD_AIR_IMP_PK " & vbCrLf
            //ElseIf busstype = 2 And process = 1 Then  ' SEA EXPORT
            //    strSQL &= " AND   CST.JOB_CARD_FK=JC.JOB_CARD_SEA_EXP_PK " & vbCrLf
            //Else    ' SEA IMPORT
            //    strSQL &= " AND   CST.JOB_CARD_FK=JC.JOB_CARD_SEA_IMP_PK " & vbCrLf
            //End If

            //strSQL &= vbCrLf & strCondition
            //strSQL &= ")"
            //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
            //TotalPage = TotalRecords \ RecordsPerPage
            //If TotalRecords Mod RecordsPerPage <> 0 Then
            //    TotalPage += 1
            //End If
            //If CurrentPage > TotalPage Then
            //    CurrentPage = 1
            //End If
            //If TotalRecords = 0 Then
            //    CurrentPage = 0
            //End If
            //last = CurrentPage * RecordsPerPage
            //start = (CurrentPage - 1) * RecordsPerPage + 1


            strSQL = string.Empty;
            //'Modified and Added By Koteshwari for displaying air & sea both records.
            //strSQL &= "SELECT * FROM ( " & vbCrLf
            //strSQL &= "SELECT ROWNUM sr_no, Qry.* FROM (SELECT DISTINCT CST.CUSTOMER_SERVICE_PK, " & vbCrLf
            if (busstype == 0)
            {
                strSQL += " SELECT DISTINCT CST.CUSTOMER_SERVICE_PK, ";
                strSQL += "       CST.CS_REF_NO, ";
                strSQL += "       TO_CHAR(CST.CS_DT,'" + dateFormat + " ') CS_DT, ";
                strSQL += "       JC.JOBCARD_REF_NO, ";
                strSQL += "       CUS.CUSTOMER_NAME, ";
                strSQL += "       (CASE  WHEN  CALL_TYPE =1 THEN 'PROBLEM' WHEN CALL_TYPE = 2 THEN 'ENQUIRY' ELSE '' END) CALL_TYPE, ";
                strSQL += "       (CASE WHEN CST.STATUS = 0 THEN 'OPEN' WHEN CST.STATUS=1 THEN 'CLOSED' END ) STATUS,CST.Biz_Type";
                strSQL += " FROM CUSTOMER_SERVICE_TRN CST, ";
                strSQL += "     CUSTOMER_MST_TBL CUS, ";
                strSQL += "  USER_MST_TBL         UMT,";
                if (process == 1)
                {
                    strSQL += " JOB_CARD_TRN JC";
                }
                else
                {
                    strSQL += " JOB_CARD_TRN JC";
                }
                strSQL += " WHERE CST.CUSTOMER_MST_FK = CUS.CUSTOMER_MST_PK";
                if (process == 1)
                {
                    strSQL += " AND CST.JOB_CARD_FK = JC.JOB_CARD_TRN_PK";
                }
                else
                {
                    strSQL += " AND CST.JOB_CARD_FK = JC.JOB_CARD_TRN_PK ";
                }
                strSQL += "   AND UMT.USER_MST_PK = CST.CREATED_BY_FK ";
                strSQL += " ";
                strSQL += strCondition;
                strSQL += "  UNION";
                strSQL += " SELECT DISTINCT CST.CUSTOMER_SERVICE_PK, ";
                strSQL += "       CST.CS_REF_NO, ";
                strSQL += "       TO_CHAR(CST.CS_DT,'" + dateFormat + " ') CS_DT, ";
                strSQL += "       JC.JOBCARD_REF_NO, ";
                strSQL += "       CUS.CUSTOMER_NAME, ";
                strSQL += "       (CASE  WHEN  CALL_TYPE =1 THEN 'PROBLEM' WHEN CALL_TYPE = 2 THEN 'ENQUIRY' ELSE '' END) CALL_TYPE, ";
                strSQL += "       (CASE WHEN CST.STATUS = 0 THEN 'OPEN' WHEN CST.STATUS=1 THEN 'CLOSED' END ) STATUS,CST.Biz_Type ";
                strSQL += " FROM CUSTOMER_SERVICE_TRN CST, ";
                strSQL += "     CUSTOMER_MST_TBL CUS, ";
                strSQL += "  USER_MST_TBL         UMT,";
                if (process == 1)
                {
                    strSQL += " JOB_CARD_TRN JC";
                }
                else
                {
                    strSQL += " JOB_CARD_TRN JC";
                }
                strSQL += " WHERE CST.CUSTOMER_MST_FK = CUS.CUSTOMER_MST_PK";
                if (process == 1)
                {
                    strSQL += " AND CST.JOB_CARD_FK = JC.JOB_CARD_TRN_PK";
                }
                else
                {
                    strSQL += " AND CST.JOB_CARD_FK = JC.JOB_CARD_TRN_PK";
                }
                strSQL += "   AND UMT.USER_MST_PK = CST.CREATED_BY_FK ";
                strSQL += " ";
                strSQL += strCondition;
            }
            else
            {
                strSQL += " SELECT DISTINCT CST.CUSTOMER_SERVICE_PK, ";
                strSQL += "       CST.CS_REF_NO, ";
                strSQL += "       TO_CHAR(CST.CS_DT,'" + dateFormat + " ') CS_DT, ";
                strSQL += "       JC.JOBCARD_REF_NO, ";
                strSQL += "       CUS.CUSTOMER_NAME, ";
                strSQL += "       (CASE  WHEN  CALL_TYPE =1 THEN 'PROBLEM' WHEN CALL_TYPE = 2 THEN 'ENQUIRY' ELSE '' END) CALL_TYPE, ";
                strSQL += "       (CASE WHEN CST.STATUS = 0 THEN 'OPEN' WHEN CST.STATUS=1 THEN 'CLOSED' END ) STATUS,CST.Biz_Type  ";
                strSQL += " FROM CUSTOMER_SERVICE_TRN CST, ";
                strSQL += "     CUSTOMER_MST_TBL CUS, ";
                strSQL += "  USER_MST_TBL         UMT,";
                // Air Export
                if (busstype == 1 & process == 1)
                {
                    strSQL += "     JOB_CARD_TRN JC ";
                    //Air Import
                }
                else if (busstype == 1 & process == 2)
                {
                    strSQL += "     JOB_CARD_TRN JC ";
                    // Sea Export
                }
                else if (busstype == 2 & process == 1)
                {
                    strSQL += "     JOB_CARD_TRN JC ";
                    // Sea Import
                }
                else
                {
                    strSQL += "     JOB_CARD_TRN JC ";
                }

                strSQL += " WHERE CST.CUSTOMER_MST_FK=CUS.CUSTOMER_MST_PK ";

                // AIR EXPORT
                if (busstype == 1 & process == 1)
                {
                    strSQL += " AND   CST.JOB_CARD_FK=JC.JOB_CARD_TRN_PK ";
                    //AIR IMPORT
                }
                else if (busstype == 1 & process == 2)
                {
                    strSQL += " AND   CST.JOB_CARD_FK=JC.JOB_CARD_TRN_PK ";
                    // SEA EXPORT
                }
                else if (busstype == 2 & process == 1)
                {
                    strSQL += " AND   CST.JOB_CARD_FK=JC.JOB_CARD_TRN_PK ";
                    // SEA IMPORT
                }
                else
                {
                    strSQL += " AND   CST.JOB_CARD_FK=JC.JOB_CARD_TRN_PK ";
                }
                strSQL += "   AND UMT.USER_MST_PK = CST.CREATED_BY_FK ";
                strSQL += " ";
                strSQL += strCondition;
            }
            //'strSQL &= vbCrLf & " ORDER BY CUSTOMER_SERVICE_PK desc) Qry ) WHERE sr_no  Between " & start & " and " & last
            strSQL += " ORDER BY CUSTOMER_SERVICE_PK desc";
            strSQL += "       ";

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*)  from  ");
            strCount.Append((" (" + strSQL.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strCount.Remove(0, strCount.Length);

            System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
            sqlstr2.Append(" SELECT * FROM ( ");
            sqlstr2.Append(" SELECT ROWNUM sr_no, Qry.* FROM ");
            sqlstr2.Append("  (" + strSQL.ToString() + " ");
            sqlstr2.Append("  ) Qry ) WHERE sr_no  Between " + start + " and " + last + "");
            try
            {
                //Return objWF.GetDataSet(strSQL)
                return objWF.GetDataSet(sqlstr2.ToString());
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

        #region " Fetch Track And Trace Detais"
        //Fetch Functionality for Track N Trace
        public DataSet FetchTracknTraceDetails(int intBizType, int intProcess, string strRefPk = "0")
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strBuilder.Append(" SELECT DISTINCT TRK.JOB_CARD_FK REFPK, ");
                strBuilder.Append(" (TO_DATE(TRK.CREATED_ON,'" + dateFormat + "')) ");
                //strBuilder.Append(" CR_DATE,TRK.CREATED_ON CR_TIME, ")
                strBuilder.Append(" CR_DATE,(TO_CHAR(TRK.CREATED_ON,'HH24:Mi')) CR_TIME, ");
                strBuilder.Append(" LOC.LOCATION_ID \"Loc\",");
                strBuilder.Append(" TRK.DOC_REF_NO \"Reference Nr.\",");
                strBuilder.Append(" TRK.STATUS \"Status\", ");
                strBuilder.Append(" DM.DOCUMENT_URL \"URL\" ,");
                strBuilder.Append(" FETCH_REFPK_TRACK_N_TRACE(TRK.DOC_REF_NO,dm.document_url_pk , TRK.BIZ_TYPE, TRK.PROCESS) \"URLREFPK\" ");
                strBuilder.Append(" FROM TRACK_N_TRACE_TBL TRK,USER_MST_TBL USR,");
                strBuilder.Append(" LOCATION_MST_TBL LOC, ");

                if (intBizType == 1 & intProcess == 1)
                {
                    strBuilder.Append(" BOOKING_MST_TBL BOOK,");
                    strBuilder.Append(" JOB_CARD_TRN JC,");
                }
                else if (intBizType == 2 & intProcess == 1)
                {
                    strBuilder.Append(" BOOKING_MST_TBL BOOK,");
                    strBuilder.Append(" JOB_CARD_TRN JC,");
                }
                else if (intBizType == 1 & intProcess == 2)
                {
                    strBuilder.Append(" JOB_CARD_TRN JC,");
                }
                else
                {
                    strBuilder.Append(" JOB_CARD_TRN JC,");
                }

                strBuilder.Append(" DOCUMENT_URL_MST_TBL DM,CUSTOMER_MST_TBL CUST");
                strBuilder.Append(" WHERE TRK.CREATED_BY = USR.USER_MST_PK");
                strBuilder.Append(" AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK");


                if (intBizType == 1 & intProcess == 1)
                {
                    if (!string.IsNullOrEmpty(strRefPk))
                    {
                        strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                    }
                    strBuilder.Append(" AND JC.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
                    strBuilder.Append(" AND BOOK.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                    strBuilder.Append("");
                }
                else if (intBizType == 2 & intProcess == 1)
                {
                    if (!string.IsNullOrEmpty(strRefPk))
                    {
                        strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                    }
                    strBuilder.Append(" AND JC.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
                    strBuilder.Append(" AND BOOK.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                    strBuilder.Append("");
                }
                else if (intBizType == 1 & intProcess == 2)
                {
                    if (!string.IsNullOrEmpty(strRefPk))
                    {
                        strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                    }
                    strBuilder.Append(" AND JC.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                    strBuilder.Append("");
                }
                else
                {
                    if (!string.IsNullOrEmpty(strRefPk))
                    {
                        strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                    }
                    strBuilder.Append(" AND JC.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                    strBuilder.Append("");
                }

                strBuilder.Append(" AND TRK.DOCUMENT_URL_FK = DM.DOCUMENT_URL_PK ");
                strBuilder.Append(" AND TRK.BIZ_TYPE = " + intBizType + "");
                strBuilder.Append(" AND TRK.PROCESS = " + intProcess + "");
                if (!string.IsNullOrEmpty(strRefPk))
                {
                    strBuilder.Append(" AND TRK.JOB_CARD_FK in (" + strRefPk + ")");
                }
                strBuilder.Append(" ORDER BY TO_DATE(CR_DATE) DESC, CR_TIME DESC, TRK.STATUS DESC");
                return objWF.GetDataSet(strBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Details"
        //To display all the details in the entry screen from the list screen
        public DataSet FetchDetails(int CUSTOMER_SERVICE_PK, int busstype, int process, string CS_REF_NO = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += " SELECT CST.CUSTOMER_SERVICE_PK, ";
            strSQL += " CST.CS_REF_NO, ";
            strSQL += " CST.CS_DT, ";
            strSQL += " CST.CALLER, ";
            strSQL += " CST.CUSTOMER_MST_FK, ";
            strSQL += " CST.CALL_TYPE, ";
            strSQL += " CST.ASSIGN_TO_FK, ";
            strSQL += " CST.ASSIGN_DT, ";
            strSQL += " CST.ESCALLATED_TO_FK, ";
            strSQL += " CST.ESCALLATION_DT, ";
            strSQL += " CST.ISSUE_DESCRIPTION, ";
            strSQL += " CST.STATUS, ";
            strSQL += " CST.RESOLVED_BY_FK, ";
            strSQL += " CST.RESOLUTION_DT, ";
            strSQL += " CST.RESOLUTION, ";
            strSQL += " CST.CREATED_BY_FK, ";
            strSQL += " CST.CREATED_DT, ";
            strSQL += " CST.LAST_MODIFIED_BY_FK, ";
            strSQL += " CST.LAST_MODIFIED_DT, ";
            strSQL += " CST.VERSION_NO, ";
            strSQL += " CST.BIZ_TYPE, ";
            strSQL += " CST.PROCESS, ";
            strSQL += " CST.JOB_CARD_FK, ";
            strSQL += " JC.JOBCARD_REF_NO, ";
            strSQL += " CST.PROBLEM, ";
            strSQL += " EXECUTIVE.EMPLOYEE_NAME EXEC_NAME, ";
            strSQL += " ASSIGN.EMPLOYEE_NAME ASSIGN_NAME, ";
            strSQL += " RESOLVED.EMPLOYEE_NAME RESOLVED_NAME ";
            strSQL += " FROM CUSTOMER_SERVICE_TRN CST, ";
            strSQL += " EMPLOYEE_MST_TBL     EXECUTIVE, ";
            strSQL += " EMPLOYEE_MST_TBL     ASSIGN , ";
            strSQL += " EMPLOYEE_MST_TBL RESOLVED, ";

            // Air Export
            if (busstype == 1 & process == 1)
            {
                strSQL += "     JOB_CARD_TRN JC ";
                //Air Import
            }
            else if (busstype == 1 & process == 2)
            {
                strSQL += "     JOB_CARD_TRN JC ";
                // Sea Export
            }
            else if (busstype == 2 & process == 1)
            {
                strSQL += "     JOB_CARD_TRN JC ";
                // Sea Import
            }
            else
            {
                strSQL += "     JOB_CARD_TRN JC ";
            }

            strSQL += " WHERE CST.ESCALLATED_TO_FK = EXECUTIVE.EMPLOYEE_MST_PK (+) ";
            strSQL += " AND CST.ASSIGN_TO_FK = ASSIGN.EMPLOYEE_MST_PK (+) ";
            strSQL += " AND CST.RESOLVED_BY_FK=RESOLVED.EMPLOYEE_MST_PK (+) ";

            // AIR EXPORT
            if (busstype == 1 & process == 1)
            {
                strSQL += " AND   CST.JOB_CARD_FK=JC.JOB_CARD_TRN_PK ";
                //AIR IMPORT
            }
            else if (busstype == 1 & process == 2)
            {
                strSQL += " AND   CST.JOB_CARD_FK=JC.JOB_CARD_TRN_PK ";
                // SEA EXPORT
            }
            else if (busstype == 2 & process == 1)
            {
                strSQL += " AND   CST.JOB_CARD_FK=JC.JOB_CARD_TRN_PK ";
                // SEA IMPORT
            }
            else
            {
                strSQL += " AND   CST.JOB_CARD_FK=JC.JOB_CARD_TRN_PK ";
            }

            strSQL += " AND CST.CUSTOMER_SERVICE_PK = " + CUSTOMER_SERVICE_PK;
            strSQL += " ";

            if (!string.IsNullOrEmpty(CS_REF_NO))
            {
                strSQL += " AND  CST.CS_REF_NO = '" + CS_REF_NO + "'";
            }
            strSQL += " ";
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

        #region "Fetch Problem Type"
        public DataSet FetchCSProblemType()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "(SELECT 0 CS_PROBLEM_TYPE_MST_PK, ";
            strSQL += "       '' PROBLEM_ID, ";
            strSQL += "       '' PROBLEM_DESC        ";
            strSQL += "FROM DUAL ) ";
            strSQL += "UNION ";
            strSQL += "(SELECT PTM.CS_PROBLEM_TYPE_MST_PK, ";
            strSQL += "       PTM.PROBLEM_ID, ";
            strSQL += "       PTM.PROBLEM_DESC ";
            strSQL += "FROM CS_PROBLEM_TYPE_MST_TBL PTM ";
            strSQL += "WHERE PTM.ACTIVE = 1) ";
            strSQL += " ";

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

        #region "Fetch Schedule Details"
        public DataSet FetchScheduleDetails(int COM_SCHEDULE_TRN_PK, string VesselID = "", long VesselPK = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            if (COM_SCHEDULE_TRN_PK != 0)
            {
                strSQL = string.Empty;
                strSQL += " SELECT ROWNUM, QRY.*  ";
                strSQL += "       FROM (SELECT PORT.PORT_ID, CSH.VOYAGE_NO, COMMERCIAL_SCHEDULE_TRN_PK, ";
                strSQL += "       (CASE WHEN CST.ARRIVAL_DT IS NULL THEN  CST.ETA  ELSE  CST.ARRIVAL_DT  END) ETA, ";
                strSQL += "       (CASE WHEN CST.SAIL_DT IS NULL THEN CST.ETD ELSE CST.SAIL_DT END) ETD, V.VESSEL_ID ";
                strSQL += "       FROM COMMERCIAL_SCHEDULE_HDR CSH, COMMERCIAL_SCHEDULE_TRN CST, ";
                strSQL += "       PORT_MST_TBL  PORT, VESSEL_MST_TBL    V, PORT_MST_TBL   POD";
                strSQL += "       WHERE CSH.COMMERCIAL_SCHEDULE_HDR_PK = CST.DEPARTURE_VOYAGE_FK ";
                strSQL += " AND CSH.VESSEL_MST_FK = V.VESSEL_MST_PK  ";
                strSQL += "     AND CST.PORT_MST_FK = PORT.PORT_MST_PK ";
                strSQL += "     AND CST.NPC_FK = POD.PORT_MST_PK   ";
                strSQL += "     AND CST.COMMERCIAL_SCHEDULE_TRN_PK IN (SELECT CST1.COMMERCIAL_SCHEDULE_TRN_PK FROM COMMERCIAL_SCHEDULE_TRN CST1 ";
                if (COM_SCHEDULE_TRN_PK == 0)
                {
                    strSQL += "     WHERE CST1.COMMERCIAL_SCHEDULE_TRN_PK = " + COM_SCHEDULE_TRN_PK + ")";
                }
                else
                {
                    strSQL += "     WHERE CST1.COMMERCIAL_SCHEDULE_TRN_PK >= " + COM_SCHEDULE_TRN_PK + ")";
                }
                if (VesselPK > 0)
                {
                    strSQL += "      AND CSH.VESSEL_MST_FK = " + VesselPK;
                }
                strSQL += "      AND ROWNUM BETWEEN 1 AND ( ";
                strSQL += "                 SELECT COUNT(*)  ";
                strSQL += "                 FROM  ";
                strSQL += "                      COMMERCIAL_SCHEDULE_TRN C1  ";
                strSQL += "                 WHERE C1.DEPARTURE_VOYAGE_FK=CSH.COMMERCIAL_SCHEDULE_HDR_PK) ";
                strSQL += "      ORDER BY (CASE WHEN CST.ARRIVAL_DT IS NULL THEN  CST.ETA ELSE CST.ARRIVAL_DT END )) qry ";
            }
            else
            {
                strSQL = string.Empty;
                strSQL += "SELECT ROWNUM , qry.* FROM (SELECT   ";
                strSQL += "                       PORT.PORT_ID,  ";
                strSQL += "                       CSH.VOYAGE_NO,  ";
                strSQL += "                       COMMERCIAL_SCHEDULE_TRN_PK,               ";
                strSQL += "                       (CASE WHEN CST.ARRIVAL_DT IS NULL THEN  CST.ETA ELSE CST.ARRIVAL_DT END ) ETA,  ";
                strSQL += "                       (CASE WHEN CST.SAIL_DT IS NULL THEN  CST.ETD ELSE CST.SAIL_DT END ) ETD,   ";
                strSQL += "                       V.VESSEL_ID          ";
                strSQL += "                FROM   ";
                strSQL += "                     COMMERCIAL_SCHEDULE_HDR CSH,  ";
                strSQL += "                     COMMERCIAL_SCHEDULE_TRN CST,       ";
                strSQL += "                     PORT_MST_TBL PORT , ";
                strSQL += "                     VESSEL_MST_TBL V ";
                strSQL += "                WHERE CSH.COMMERCIAL_SCHEDULE_HDR_PK= CST.departure_voyage_fk  ";
                strSQL += "                      AND CST.PORT_MST_FK=PORT.PORT_MST_PK  ";
                //strSQL &= "                     /*AND CST.COMMERCIAL_SCHEDULE_TRN_PK >= 0*/ " & vbCrLf
                strSQL += "                      AND CSH.VESSEL_MST_FK = V.VESSEL_MST_PK ";
                strSQL += "                      AND upper(V.VESSEL_ID) = '" + VesselID.ToUpper().Trim() + "' ";
                //strSQL &= "                     /* AND upper(CSH.VOYAGE_NO) = '001'*/ " & vbCrLf
                strSQL += "                      AND ROWNUM BETWEEN 1 AND (  ";
                strSQL += "                                 SELECT COUNT(*)   ";
                strSQL += "                                 FROM   ";
                strSQL += "                                      COMMERCIAL_SCHEDULE_TRN C1   ";
                strSQL += "                                 WHERE C1.DEPARTURE_VOYAGE_FK=CSH.COMMERCIAL_SCHEDULE_HDR_PK)  ";
                strSQL += "                      ORDER BY (CASE WHEN CST.ARRIVAL_DT IS NULL THEN  CST.ETA ELSE CST.ARRIVAL_DT END )) qry  ";
                strSQL += "             ";
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
        #endregion

        #region "Fetch JobCard Details"

        public DataSet FetchJobCardSea_Imp(string jobCardPK = "0")
        {

            // Dim strSQL As StringBuilder = New StringBuilder
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();

            strSQL.Append("SELECT");
            strSQL.Append("    job_imp.cust_customer_mst_fk,");
            strSQL.Append("    cust.customer_id \"customer_id\",");
            strSQL.Append("    cust.customer_name \"customer_name\",");
            strSQL.Append("    job_imp.port_mst_pol_fk,");
            strSQL.Append("    pol.port_id \"POL\",");
            strSQL.Append("    job_imp.port_mst_pod_fk,");
            strSQL.Append("    pod.port_id \"POD\",");
            strSQL.Append("    VVT.VOYAGE_TRN_PK \"VoyagePK\",");
            strSQL.Append("    V.VESSEL_NAME \"vessel_name\",  ");
            strSQL.Append("    VVT.VOYAGE \"voyage\",   ");
            strSQL.Append("    job_imp.departure_date,");
            strSQL.Append("    job_imp.arrival_date");
            strSQL.Append("FROM");
            strSQL.Append("    job_card_TRN job_imp,");
            strSQL.Append("    port_mst_tbl POD,");
            strSQL.Append("    port_mst_tbl POL,");
            strSQL.Append("    customer_mst_tbl cust,");
            strSQL.Append("    customer_mst_tbl consignee,");
            strSQL.Append("    customer_mst_tbl shipper,");
            strSQL.Append("    customer_mst_tbl notify1,");
            strSQL.Append("    customer_mst_tbl notify2,");
            strSQL.Append("    place_mst_tbl del_place,");
            strSQL.Append("    operator_mst_tbl oprator,");
            strSQL.Append("    agent_mst_tbl clagnt, ");
            strSQL.Append("    agent_mst_tbl cbagnt,");
            strSQL.Append("    agent_mst_tbl polagnt,");
            strSQL.Append("    commodity_group_mst_tbl comm, ");
            strSQL.Append("    VESSEL_VOYAGE_TBL V, ");
            strSQL.Append("    VESSEL_VOYAGE_TRN VVT, ");
            strSQL.Append("    vendor_mst_tbl  depot,");
            strSQL.Append("    vendor_mst_tbl  carrier,");
            strSQL.Append("    country_mst_tbl country");
            strSQL.Append("    WHERE");
            strSQL.Append("    job_imp.job_card_TRN_pk          = " + jobCardPK);
            strSQL.Append("    AND job_imp.port_mst_pol_fk          =  pol.port_mst_pk");
            strSQL.Append("    AND job_imp.port_mst_pod_fk          =  pod.port_mst_pk");
            strSQL.Append("    AND job_imp.del_place_mst_fk         =  del_place.place_pk(+)");
            strSQL.Append("    AND job_imp.cust_customer_mst_fk     =  cust.customer_mst_pk(+) ");
            strSQL.Append("    AND job_imp.CARRIER_mst_fk          =  oprator.operator_mst_pk");
            strSQL.Append("    AND job_imp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)");
            strSQL.Append("    AND job_imp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)");
            strSQL.Append("    AND job_imp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)");
            strSQL.Append("    AND job_imp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
            strSQL.Append("    AND job_imp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_imp.Cb_Agent_Mst_Fk          =  cbagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_imp.pol_agent_mst_fk         =  polagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_imp.commodity_group_fk       =  comm.commodity_group_pk(+)");
            strSQL.Append("    AND job_imp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
            strSQL.Append("    AND job_imp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
            strSQL.Append("    AND job_imp.country_origin_fk        =  country.country_mst_pk(+)");
            strSQL.Append("    AND VVT.VESSEL_VOYAGE_TBL_FK         = V.VESSEL_VOYAGE_TBL_PK(+)");
            strSQL.Append("    AND JOB_IMP.VOYAGE_TRN_FK            = VVT.VOYAGE_TRN_PK(+)");

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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
        public DataSet FetchJobCardSea_Exp(string jobCardPK = "0")
        {

            //Dim strSQL As StringBuilder = New StringBuilder
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();

            strSQL.Append("SELECT ");
            strSQL.Append("    bst.cust_customer_mst_fk, ");
            strSQL.Append("    cust.customer_id \"customer_id\",");
            strSQL.Append("    cust.customer_name, ");
            strSQL.Append("    bst.port_mst_pol_fk, ");
            strSQL.Append("    pol.port_name \"POL\",");
            strSQL.Append("    bst.port_mst_pod_fk, ");
            strSQL.Append("    pod.port_name \"POD\",");
            strSQL.Append("    VVT.VOYAGE_TRN_PK \"VoyagePK\",  ");
            strSQL.Append("    V.VESSEL_NAME \"vessel_name\",  ");
            strSQL.Append("    VVT.VOYAGE \"voyage\",   ");
            strSQL.Append("    TO_DATE(job_exp.departure_date,DATEFORMAT) departure_date, ");
            strSQL.Append("    TO_DATE(job_exp.arrival_date,DATEFORMAT) arrival_date ");
            strSQL.Append("    FROM ");
            strSQL.Append("    JOB_CARD_TRN job_exp,");
            strSQL.Append("    BOOKING_MST_TBL bst,");
            strSQL.Append("    port_mst_tbl POD,");
            strSQL.Append("    port_mst_tbl POL,");
            strSQL.Append("    customer_mst_tbl cust,");
            strSQL.Append("    customer_mst_tbl consignee,");
            strSQL.Append("    customer_mst_tbl shipper,");
            strSQL.Append("    customer_mst_tbl notify1,");
            strSQL.Append("    customer_mst_tbl notify2,");
            strSQL.Append("    place_mst_tbl col_place,");
            strSQL.Append("    place_mst_tbl del_place,");
            strSQL.Append("    operator_mst_tbl oprator,");
            strSQL.Append("    agent_mst_tbl clagnt, ");
            strSQL.Append("    agent_mst_tbl dpagnt, ");
            strSQL.Append("    agent_mst_tbl cbagnt, ");
            strSQL.Append("    commodity_group_mst_tbl comm, ");
            strSQL.Append("    VESSEL_VOYAGE_TBL V,  ");
            strSQL.Append("    VESSEL_VOYAGE_TRN VVT, ");
            strSQL.Append("    vendor_mst_tbl  depot,");
            strSQL.Append("    vendor_mst_tbl  carrier,");
            strSQL.Append("    country_mst_tbl country,");
            strSQL.Append("    master_jc_sea_exp_tbl mst,");
            strSQL.Append("    hbl_exp_tbl hbl,");
            strSQL.Append("    mbl_exp_tbl mbl");
            strSQL.Append(" WHERE ");
            strSQL.Append("    job_exp.JOB_CARD_TRN_PK = " + jobCardPK);
            strSQL.Append("    AND job_exp.BOOKING_MST_FK           =  bst.BOOKING_MST_PK");
            strSQL.Append("    AND bst.port_mst_pol_fk              =  pol.port_mst_pk");
            strSQL.Append("    AND bst.port_mst_pod_fk              =  pod.port_mst_pk");
            strSQL.Append("    AND bst.col_place_mst_fk             =  col_place.place_pk(+)");
            strSQL.Append("    AND bst.del_place_mst_fk             =  del_place.place_pk(+)");
            strSQL.Append("    AND bst.cust_customer_mst_fk         =  cust.customer_mst_pk(+) ");
            strSQL.Append("    AND bst.CARRIER_MST_FK              =  oprator.operator_mst_pk(+)");
            strSQL.Append("    AND job_exp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.cb_agent_mst_fk          =  cbagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.dp_agent_mst_fk          =  dpagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.commodity_group_fk       =  comm.commodity_group_pk(+)");
            strSQL.Append("    AND job_exp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.country_origin_fk        =  country.country_mst_pk(+)");
            strSQL.Append("    AND VVT.VESSEL_VOYAGE_TBL_FK         =  V.VESSEL_VOYAGE_TBL_PK(+)  ");
            strSQL.Append("    AND JOB_EXP.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
            strSQL.Append("    AND job_exp.master_jc_fk     =  mst.master_jc_sea_exp_pk(+)");
            strSQL.Append("    and hbl.hbl_exp_tbl_pk(+) = job_exp.HBL_HAWB_FK");
            strSQL.Append("    and mbl.mbl_exp_tbl_pk(+) = job_exp.MBL_MAWB_FK");

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        public DataSet FetchJobCardAir_Imp(string jobCardPK = "0")
        {
            //Dim strSQL As StringBuilder = New StringBuilder
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();

            strSQL.Append("SELECT");
            strSQL.Append("    job_imp.cust_customer_mst_fk,");
            strSQL.Append("    cust.customer_id \"customer_id\",");
            strSQL.Append("    cust.customer_name \"customer_name\",");
            strSQL.Append("    port_mst_pol_fk,");
            strSQL.Append("    pol.port_id \"POL\",");
            strSQL.Append("    port_mst_pod_fk,");
            strSQL.Append("    pod.port_id \"POD\",");
            strSQL.Append("    job_imp.CARRIER_mst_fk airline_mst_fk,");
            //strSQL.Append(vbCrLf & "    airline.airline_id ""airline_id"",")
            strSQL.Append("    airline.airline_name \"airline_name\",");
            strSQL.Append("    job_imp.flight_no,");
            strSQL.Append("    departure_date,");
            strSQL.Append("    arrival_date");
            strSQL.Append("FROM");
            strSQL.Append("    job_card_TRN job_imp,");
            strSQL.Append("    port_mst_tbl POD,");
            strSQL.Append("    port_mst_tbl POL,");
            strSQL.Append("    customer_mst_tbl cust,");
            strSQL.Append("    customer_mst_tbl consignee,");
            strSQL.Append("    customer_mst_tbl shipper,");
            strSQL.Append("    customer_mst_tbl notify1,");
            strSQL.Append("    customer_mst_tbl notify2,");
            strSQL.Append("    place_mst_tbl del_place,");
            strSQL.Append("    airline_mst_tbl airline,");
            strSQL.Append("    agent_mst_tbl clagnt, ");
            strSQL.Append("    agent_mst_tbl cbagnt,");
            strSQL.Append("    agent_mst_tbl polagnt,");
            strSQL.Append("    commodity_group_mst_tbl comm, ");
            strSQL.Append("    vendor_mst_tbl  depot,");
            strSQL.Append("    vendor_mst_tbl  carrier,");
            strSQL.Append("    country_mst_tbl country");
            strSQL.Append("    WHERE");
            strSQL.Append("    job_imp.job_card_TRN_pk          = " + jobCardPK);
            strSQL.Append("    AND job_imp.port_mst_pol_fk          =  pol.port_mst_pk");
            strSQL.Append("    AND job_imp.port_mst_pod_fk          =  pod.port_mst_pk");
            strSQL.Append("    AND job_imp.del_place_mst_fk         =  del_place.place_pk(+)");
            strSQL.Append("    AND job_imp.cust_customer_mst_fk     =  cust.customer_mst_pk(+) ");
            strSQL.Append("    AND job_imp.CARRIER_mst_fk           =  airline.airline_mst_pk");
            strSQL.Append("    AND job_imp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)");
            strSQL.Append("    AND job_imp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)");
            strSQL.Append("    AND job_imp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)");
            strSQL.Append("    AND job_imp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
            strSQL.Append("    AND job_imp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_imp.Cb_Agent_Mst_Fk          =  cbagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_imp.pol_agent_mst_fk         =  polagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_imp.commodity_group_fk       =  comm.commodity_group_pk(+)");
            strSQL.Append("    AND job_imp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
            strSQL.Append("    AND job_imp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
            strSQL.Append("    AND job_imp.country_origin_fk        =  country.country_mst_pk(+)");

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        public DataSet FetchJobCardAir_Exp(string jobCardPK = "0")
        {
            // Dim strSQL As StringBuilder = New StringBuilder
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();

            strSQL.Append("SELECT");
            strSQL.Append("    bat.cust_customer_mst_fk,");
            strSQL.Append("    cust.customer_id,");
            strSQL.Append("    cust.customer_name \"customer_name\",");
            strSQL.Append("    bat.port_mst_pol_fk,");
            strSQL.Append("    pol.port_name \"POL\",");
            strSQL.Append("    bat.port_mst_pod_fk, ");
            strSQL.Append("    pod.port_name \"POD\",");
            strSQL.Append("    bat.CARRIER_MST_FK airline_mst_fk,");
            //strSQL.Append(vbCrLf & "    airline.airline_id ,")
            strSQL.Append("    airline.airline_name,");
            strSQL.Append("    job_exp.VOYAGE_flight_no flight_no,");
            strSQL.Append("    TO_DATE(job_exp.departure_date,DATEFORMAT)  departure_date,");
            strSQL.Append("    TO_DATE(job_exp.arrival_date,DATEFORMAT) arrival_date");
            strSQL.Append("FROM");
            strSQL.Append("    job_card_TRN job_exp,");
            strSQL.Append("    booking_MST_tbl bat,");
            strSQL.Append("    port_mst_tbl POD,");
            strSQL.Append("    port_mst_tbl POL,");
            strSQL.Append("    customer_mst_tbl cust,");
            strSQL.Append("    customer_mst_tbl consignee,");
            strSQL.Append("    customer_mst_tbl shipper,");
            strSQL.Append("    customer_mst_tbl notify1,");
            strSQL.Append("    customer_mst_tbl notify2,");
            strSQL.Append("    place_mst_tbl col_place,");
            strSQL.Append("    place_mst_tbl del_place,");
            strSQL.Append("    airline_mst_tbl airline,");
            strSQL.Append("    agent_mst_tbl clagnt, ");
            strSQL.Append("    agent_mst_tbl dpagnt, ");
            strSQL.Append("    agent_mst_tbl cbagnt,");
            strSQL.Append("    commodity_group_mst_tbl comm, ");
            strSQL.Append("    vendor_mst_tbl  depot,");
            strSQL.Append("    vendor_mst_tbl  carrier,");
            strSQL.Append("    country_mst_tbl country,");
            strSQL.Append("    master_jc_air_exp_tbl mst,");
            strSQL.Append("    hawb_exp_tbl hawb,");
            strSQL.Append("    mawb_exp_tbl mawb");
            strSQL.Append("WHERE");
            strSQL.Append("    job_exp.job_card_TRN_pk = " + jobCardPK);
            strSQL.Append("    AND job_exp.booking_MST_fk           =  bat.booking_MST_pk");
            strSQL.Append("    AND bat.port_mst_pol_fk              =  pol.port_mst_pk");
            strSQL.Append("    AND bat.port_mst_pod_fk              =  pod.port_mst_pk");
            strSQL.Append("    AND bat.col_place_mst_fk             =  col_place.place_pk(+)");
            strSQL.Append("    AND bat.del_place_mst_fk             =  del_place.place_pk(+)");
            strSQL.Append("    AND bat.cust_customer_mst_fk         =  cust.customer_mst_pk(+) ");
            strSQL.Append("    AND bat.CARRIER_mst_fk               =  airline.airline_mst_pk(+)");
            strSQL.Append("    AND job_exp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.cb_agent_mst_fk          =  cbagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.dp_agent_mst_fk          =  dpagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.commodity_group_fk       =  comm.commodity_group_pk(+)");
            strSQL.Append("    AND job_exp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.country_origin_fk        =  country.country_mst_pk(+)");
            strSQL.Append("    AND job_exp.master_jc_fk     =  mst.master_jc_air_exp_pk(+)");
            strSQL.Append("    AND hawb.hawb_exp_tbl_pk(+) = job_exp.HBL_hawb_fk");
            strSQL.Append("    AND mawb.mawb_exp_tbl_pk(+) = job_exp.MBL_mawb_fk");

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        #region "Fetch Job Details"

        #region "Get VIA No."
        public string GetViaNo(int Com_Sch_Trn_Pk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            //strSQL = String.Empty & vbCrLf
            //strSQL &= "SELECT VH.VIA_HDR_PK, " & vbCrLf
            //strSQL &= "       VH.VIA_NO " & vbCrLf
            //strSQL &= "FROM VIA_FILING_HDR VH " & vbCrLf
            //strSQL &= "WHERE VH.VESSEL_FK =  " & VESSEL_FK & vbCrLf
            //strSQL &= "      AND VH.VOYAGE_NO = '" & VOYAGE_NO & "'" & vbCrLf
            //strSQL &= "      AND VH.POL_FK = " & POL_FK & vbCrLf
            //strSQL &= " " & vbCrLf

            strSQL = string.Empty;
            strSQL += "SELECT c.via_no, ";
            strSQL += "       to_char(c.via_dt ,'dd-Mon-yyyy') via_dt ";
            strSQL += "FROM commercial_schedule_trn c ";
            strSQL += "WHERE c.commercial_schedule_trn_pk =  " + Com_Sch_Trn_Pk;
            strSQL += "AND c.via_filing =1 ";

            System.String strEmpty = "";

            try
            {
                if (objWF.GetDataSet(strSQL).Tables[0].Rows.Count > 0)
                {
                    if ((!object.ReferenceEquals(objWF.GetDataSet(strSQL).Tables[0].Rows[0]["via_dt"], DBNull.Value)))
                    {
                        return Convert.ToString(objWF.GetDataSet(strSQL).Tables[0].Rows[0]["via_dt"]);
                    }
                    else
                    {
                        return strEmpty;
                    }
                }
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
            return "";
        }
        #endregion

        #region "Get Invoice No"
        public DataSet GetInvNo(int BOOKING_BL_PK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += " SELECT TO_CHAR(IT.INVOICE_DT, 'dd-Mon-yyyy') INVOICE_DET, IT.INVOICE_TRN_PK, JCT.JOB_CARD_PK ";
            strSQL += " FROM BOOKING_BL_TRN   BBT, JOB_CARD_TRN   JCT,  INVOICE_JOB_CARD_TRN IJCT, INVOICE_TRN   IT ";
            strSQL += " WHERE JCT.BOOKING_BL_FK = BBT.BOOKING_BL_PK  AND IJCT.JOB_CARD_FK = JCT.JOB_CARD_PK ";
            strSQL += " AND IJCT.INVOICE_TRN_FK = IT.INVOICE_TRN_PK AND BBT.BOOKING_BL_PK =  " + BOOKING_BL_PK;

            try
            {
                //If objWF.GetDataSet(strSQL).Tables(0).Rows.Count > 0 Then _
                //    Return objWF.GetDataSet(strSQL).Tables(0).Rows(0)(0)
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

        #region "Get Provision Booking No"
        public string GetProvBkgNo(string BookingID)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT TO_CHAR(PB.BOOKING_DATE,'dd-Mon-yyyy') PROV_BOOKING_DET ";
            strSQL += "FROM PROV_BOOKING_TRN PB ";
            strSQL += "WHERE PB.BOOKING_ID LIKE UPPER('" + BookingID + "')";

            try
            {
                if (objWF.GetDataSet(strSQL).Tables[0].Rows.Count > 0)
                    return Convert.ToString(objWF.GetDataSet(strSQL).Tables[0].Rows[0][0]);

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
            return "";
        }
        public long GetProvBkgPK(string BookingID)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT PB.PROV_BOOKING_PK ";
            strSQL += "FROM PROV_BOOKING_TRN PB ";
            strSQL += "WHERE PB.BOOKING_ID LIKE UPPER('" + BookingID + "')";

            try
            {
                if (objWF.GetDataSet(strSQL).Tables[0].Rows.Count > 0)
                    return Convert.ToInt64(objWF.GetDataSet(strSQL).Tables[0].Rows[0][0]);

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
            return 0;
        }
        #endregion

        #region "Get GateIn, Arrival(Discharge), OnBoard(Departure)"
        public DataSet GetContainDetail(int BOOKING_TRN_PK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT MAX(TO_CHAR(BC.DEPARTURE_DATE,'dd-Mon-yyyy')) DEP_DATE, ";
            strSQL += "       MAX(TO_CHAR(BC.ARRIVAL_DT,'dd-Mon-yyyy')) ARRIVAL_DT, ";
            strSQL += "       MAX(TO_CHAR(BC.GATE_IN_DATE,'dd-Mon-yyyy')) GATE_IN_DATE       ";
            strSQL += "FROM BOOKING_CONTAINERS_TRN BC, ";
            //strSQL &= "     BOOKING_SUMMARY_TRN BS, " & vbCrLf
            strSQL += "     BOOKING_TRN BT ";
            strSQL += "WHERE BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK  ";
            //strSQL &= "      AND BS.BOOKING_TRN_FK= BT.BOOKING_TRN_PK " & vbCrLf
            strSQL += "      AND BT.BOOKING_TRN_PK =  " + BOOKING_TRN_PK;

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

        #region "Get SRR No"
        public DataSet GetSRRNo(int Bkg_Trn_Pk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            //strSQL = String.Empty & vbCrLf
            //strSQL &= "SELECT Srr.Srr_No, " & vbCrLf
            //strSQL &= "       srr.srr_pk " & vbCrLf
            //strSQL &= "FROM BOOKING_TRN BT,  " & vbCrLf
            //strSQL &= "     BOOKING_SUMMARY_TRN BS, " & vbCrLf
            //strSQL &= "     SRR_TRN SRR " & vbCrLf
            //strSQL &= "WHERE BT. BOOKING_TRN_PK = BS.BOOKING_TRN_FK(+) " & vbCrLf
            //strSQL &= "      AND Srr.Srr_Pk(+) = BS.Srr_Trn_Fk " & vbCrLf
            //strSQL &= "      AND BT.BOOKING_TRN_PK =" & Bkg_Trn_Pk & vbCrLf

            strSQL = string.Empty;
            strSQL += "  SELECT Srr.Srr_No,  ";
            strSQL += "               srr.srr_pk, ";
            strSQL += "               SSC.SSC_TRN_PK, ";
            strSQL += "               SSC.SSC_REF_NO                 ";
            strSQL += "            FROM BOOKING_TRN BT,   ";
            strSQL += "                 BOOKING_SUMMARY_TRN BS,  ";
            strSQL += "                 SRR_TRN SRR, ";
            strSQL += "                 SSC_TRN SSC ";
            strSQL += "            WHERE BT. BOOKING_TRN_PK = BS.BOOKING_TRN_FK(+)  ";
            strSQL += "                  AND Srr.Srr_Pk(+) = BS.Srr_Trn_Fk  ";
            strSQL += "                  AND SSC.SSC_TRN_PK(+) = BS.SSC_TRN_FK ";
            strSQL += "                  AND BT.BOOKING_TRN_PK = " + Bkg_Trn_Pk;

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

        public DataSet GetSector(int Bkg_Trn_Pk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = string.Empty;
            strSQL += "SELECT sm.tli_ref_no ";
            strSQL += "FROM SECTOR_MST_TBL SM, ";
            strSQL += "     Booking_Trn BT ";
            strSQL += "WHERE sm.to_port_fk = bt.Pod_Fk ";
            strSQL += "      AND sm.from_port_fk = bt.Pol_Fk  ";
            strSQL += "      AND bt.booking_trn_pk = " + Bkg_Trn_Pk;

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

        #region "Get BL Date"
        public string GetBlDate(int BL_PK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT to_char(b.bl_date,'dd-Mon-yyyy') AS bl_date ";
            strSQL += "FROM booking_bl_trn b ";
            strSQL += "WHERE b.booking_bl_pk= " + BL_PK;

            try
            {
                if (objWF.GetDataSet(strSQL).Tables[0].Rows.Count > 0)
                    return Convert.ToString(objWF.GetDataSet(strSQL).Tables[0].Rows[0][0]);
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
            return "";
        }
        #endregion

        #region "Get Booking Date"
        public string GetBkgDate(int Bt_Trn_PK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT to_char(bt.booking_date,'dd-Mon-yyyy') AS bt_date ";
            strSQL += "FROM booking_trn bt ";
            strSQL += "WHERE bt.booking_trn_pk =  " + Bt_Trn_PK;

            try
            {
                if (objWF.GetDataSet(strSQL).Tables[0].Rows.Count > 0)
                    return Convert.ToString(objWF.GetDataSet(strSQL).Tables[0].Rows[0][0]);
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
            return "";
        }
        #endregion

        #endregion

        #region "Fetch Booking Container Details"
        //to display the containers for a particular booking in another sub-form
        public DataSet FetchBookingContainerList(int booking_trn_pk, string ContainerNo = "")
        {
            DataSet dsgrid = new DataSet();
            WorkFlow objwk = new WorkFlow();
            string strCondition = null;
            string strSQL = null;
            try
            {
                strCondition = "";
                if (!string.IsNullOrEmpty(ContainerNo))
                    strCondition = " AND Upper(BC.CONTAINER_NO) Like '" + ContainerNo.ToUpper() + "%'";
                //Modified By Suresh Kumar 21-Feb-2005
                strSQL = string.Empty;
                strSQL += "SELECT DISTINCT BT.Pod_Fk, ";
                strSQL += "       POD.PORT_ID,  ";
                strSQL += "       BC.CONTAINER_TYPE_MST_FK, ";
                strSQL += "       CTM.CONTAINER_TYPE_MST_ID, ";
                strSQL += "       BC.CONTAINER_STATUS, ";
                strSQL += "       (CASE WHEN BC.CONTAINER_STATUS = 1 THEN 'EMPTY' ELSE 'FULL' END) CONT_STATUS, ";
                strSQL += "       BC.SHIPMENT_TYPE, ";
                strSQL += "       (CASE WHEN BC.SHIPMENT_TYPE = 1 THEN 'TRANSHIPMENT' ELSE 'LOCAL' END) SHIP_STATUS, ";
                strSQL += "       BC.COMMODITY_GROUP_MST_FK, ";
                strSQL += "       CGM.COMMODITY_GROUP_CODE, ";
                strSQL += "       BC.DEPARTURE_DATE, ";
                strSQL += "       BC.ARRIVAL_DT ";
                strSQL += "FROM BOOKING_CONTAINERS_TRN BC, ";
                strSQL += "     BOOKING_TRN BT, ";
                strSQL += "     PORT_MST_TBL POD, ";
                strSQL += "     CONTAINER_TYPE_MST_TBL CTM, ";
                strSQL += "     COMMODITY_GROUP_MST_TBL CGM ";
                strSQL += "WHERE BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK ";
                strSQL += "      AND BT.POD_FK = POD.PORT_MST_PK ";
                strSQL += "      AND BC.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK ";
                strSQL += "      AND BC.COMMODITY_GROUP_MST_FK = CGM.COMMODITY_GROUP_PK ";
                strSQL += "      AND BT.BOOKING_TRN_PK = " + booking_trn_pk;
                strSQL += strCondition;

                OracleDataAdapter DA = new OracleDataAdapter(strSQL, objwk.MyConnection);

                strSQL = string.Empty;
                strSQL += "SELECT BT.Pod_Fk, ";
                strSQL += "       BC.CONTAINER_TYPE_MST_FK, ";
                strSQL += "       BC.CONTAINER_STATUS, ";
                strSQL += "       BC.SHIPMENT_TYPE, ";
                strSQL += "       BC.COMMODITY_GROUP_MST_FK, ";
                strSQL += "       BC.DEPARTURE_DATE, ";
                strSQL += "       BC.ARRIVAL_DT, ";
                strSQL += "       BC.CONTAINER_NO, ";
                strSQL += "       POO.PORT_ID POO_ID, ";
                strSQL += "       PFD.PORT_ID PFD_ID, ";
                strSQL += "       BC.WEIGHT_IN_KG, ";
                //strSQL &= "       (CASE WHEN RRT.REEFER_REQ_PK IS NULL THEN 0 ELSE 1 END) REF_REQ, " & vbCrLf
                //strSQL &= "       (CASE WHEN BHC.BOOKING_HC_REQ_PK IS NULL THEN 0 ELSE 1 END) HAZ_REQ, " & vbCrLf
                //strSQL &= "       (CASE WHEN BOOD.BOOKING_OOD_REQ_PK IS NULL THEN 0 ELSE 1 END) ODC_REQ, " & vbCrLf
                strSQL += "       BC.GATE_IN_DATE, ";
                strSQL += "       BC.STOWAGE_POSITION, ";
                strSQL += "       BC.BOOKING_CONTAINERS_PK ";
                strSQL += "FROM BOOKING_CONTAINERS_TRN BC, ";
                strSQL += "     BOOKING_TRN BT, ";
                strSQL += "     PORT_MST_TBL POO, ";
                //strSQL &= "     REEFER_REQ_TRN RRT, " & vbCrLf
                //strSQL &= "     BOOKING_HC_REQ_TRN BHC, " & vbCrLf
                //strSQL &= "     BOOKING_OOD_REQ_TRN BOOD, " & vbCrLf
                strSQL += "     PORT_MST_TBL PFD ";
                strSQL += "WHERE BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK ";
                strSQL += "      AND BT.POL_FK = POO.PORT_MST_PK ";
                strSQL += "      AND BT.POD_FK = PFD.PORT_MST_PK ";
                //strSQL &= "      AND RRT.BOOKING_TRN_FK = BT.BOOKING_TRN_PK " & vbCrLf
                //strSQL &= "      AND BHC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK " & vbCrLf
                //strSQL &= "      AND BOOD.BOOKING_TRN_FK = BT.BOOKING_TRN_PK " & vbCrLf
                strSQL += "      AND BT.BOOKING_TRN_PK = " + booking_trn_pk;
                strSQL += strCondition;
                //Modified End
                OracleDataAdapter DA1 = new OracleDataAdapter(strSQL, objwk.MyConnection);

                DA.Fill(dsgrid, "Header");
                DA1.Fill(dsgrid, "Detail");

                DataRelation relContainer = new DataRelation("Container", new DataColumn[] {
                    dsgrid.Tables[0].Columns["Pod_Fk"],
                    dsgrid.Tables[0].Columns["CONTAINER_TYPE_MST_FK"],
                    dsgrid.Tables[0].Columns["CONTAINER_STATUS"],
                    dsgrid.Tables[0].Columns["SHIPMENT_TYPE"],
                    dsgrid.Tables[0].Columns["COMMODITY_GROUP_MST_FK"],
                    dsgrid.Tables[0].Columns["DEPARTURE_DATE"],
                    dsgrid.Tables[0].Columns["ARRIVAL_DT"]
                }, new DataColumn[] {
                    dsgrid.Tables[1].Columns["Pod_Fk"],
                    dsgrid.Tables[1].Columns["CONTAINER_TYPE_MST_FK"],
                    dsgrid.Tables[1].Columns["CONTAINER_STATUS"],
                    dsgrid.Tables[1].Columns["SHIPMENT_TYPE"],
                    dsgrid.Tables[1].Columns["COMMODITY_GROUP_MST_FK"],
                    dsgrid.Tables[1].Columns["DEPARTURE_DATE"],
                    dsgrid.Tables[1].Columns["ARRIVAL_DT"]
                });
                dsgrid.Relations.Add(relContainer);
                return dsgrid;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        public DataSet FillBookContainerHeader(int Booking_Trn_Pk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT BT.BOOKING_ID, ";
            strSQL += "       to_char(BT.BOOKING_DATE, 'dd-Mon-yyyy') BOOKING_DATE, ";
            strSQL += "       BL.Service_Bl_No, ";
            strSQL += "       to_char(BL.BL_DATE,'dd-Mon-yyyy') BL_DATE, ";
            strSQL += "       POL.PORT_ID, ";
            strSQL += "       CUS.CUSTOMER_ID ";
            strSQL += "FROM Booking_Trn BT, ";
            strSQL += "     Booking_Bl_Trn BL, ";
            strSQL += "     Port_Mst_Tbl POL, ";
            strSQL += "     Customer_Mst_Tbl CUS ";
            strSQL += "WHERE BT.BOOKING_TRN_PK = BL.BOOKING_TRN_FK(+) ";
            strSQL += "      AND BT.Pol_Fk = POL.PORT_MST_PK(+) ";
            strSQL += "      AND BT.Customer_Mst_Fk = CUS.Customer_Mst_Pk(+) ";
            strSQL += "      AND BT.BOOKING_TRN_PK =  " + Booking_Trn_Pk;

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

        #region "Fetch History"
        public DataSet FetchHistory(int JobCard_FK = 0, int CS_PK = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT ROWNUM,CUSTOMER_SERVICE_PK, CS_REF_NO,CS_DT,EXEC_NAME,ASSIGN_NAME,STATUS,''ChkFlag FROM  ";
            strSQL += "(SELECT CST.CUSTOMER_SERVICE_PK, ";
            strSQL += "        CST.CS_REF_NO, ";
            strSQL += "        TO_DATE(CST.CS_DT,'" + dateFormat + " ') CS_DT, ";
            strSQL += "        EXECUTIVE.EMPLOYEE_NAME EXEC_NAME, ";
            strSQL += "        ASSIGN.EMPLOYEE_NAME ASSIGN_NAME, ";
            strSQL += "        (CASE WHEN CST.STATUS = 1 THEN 'CLOSED' ELSE 'OPEN' END ) STATUS,''ChkFlag ";
            strSQL += "        FROM CUSTOMER_SERVICE_TRN  CST,";
            strSQL += "        EMPLOYEE_MST_TBL EXECUTIVE, ";
            strSQL += "        EMPLOYEE_MST_TBL ASSIGN ";
            strSQL += "        WHERE CST.ESCALLATED_TO_FK= ASSIGN.EMPLOYEE_MST_PK (+) ";
            strSQL += "        AND CST.ASSIGN_TO_FK= EXECUTIVE.EMPLOYEE_MST_PK (+) ";
            strSQL += "        AND CST.JOB_CARD_FK = '" + JobCard_FK + "'";
            if (CS_PK != 0)
                strSQL += "      AND CST.CUSTOMER_SERVICE_PK <>  " + CS_PK;
            strSQL += "ORDER BY CST.CS_DT DESC )";

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

        #region "Get Executive Name"
        public DataSet GetExecutive(int LocationPk, int UserPk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += " SELECT DISTINCT E.EMPLOYEE_MST_PK, E.EMPLOYEE_NAME FROM EMPLOYEE_MST_TBL E, ";
            strSQL += " LOC_PORT_MAPPING_TRN LPM, LOCATION_WORKING_PORTS_TRN LWP, LOCATION_MST_TBL LMT, USER_MST_TBL UMT ";
            strSQL += "     WHERE LPM.PORT_MST_FK=LWP.PORT_MST_FK ";
            strSQL += "     AND UMT.EMPLOYEE_MST_FK = E.EMPLOYEE_MST_PK AND LWP.LOCATION_MST_FK=LMT.LOCATION_MST_PK AND E.LOCATION_MST_FK = LMT.LOCATION_MST_PK ";
            strSQL += "      AND LPM.LOCATION_MST_FK =  " + LocationPk;
            strSQL += "      AND UMT.USER_MST_PK =  " + UserPk;

            try
            {
                if (objWF.GetDataSet(strSQL).Tables[0].Rows.Count > 0)
                    return objWF.GetDataSet(strSQL);
                //Return objWF.GetDataSet(strSQL).Tables(0).Rows(0)("EMPLOYEE_NAME")
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
            return new DataSet();
        }

        #endregion

        #region "Save Function"
        public ArrayList Save(int busstype, int process, System.DateTime csDate, string Caller, int CallType, int JobCardFk, int CustomerMstFk, int AssignFk, System.DateTime AssignDate, string Problem,
        int EscalFk, System.DateTime EscallationDate, string IssueDesc, int status, int ResolFk, System.DateTime ResolutionDate, string Resolution, int CreatedByFk, int CUSTOMER_SERVICE_PK, string userLocation,
        string employeeID, string userID)
        {


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
            ConfigurationPK = 279;
            string CustServiceRefNumber = null;
            //M_DataSet.Tables(0).Rows(0).Item("CUSTOMER_SERVICE_PK") = DBNull.Value

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;

                if (CUSTOMER_SERVICE_PK == 0)
                {
                    CustServiceRefNumber = GenerateProtocolKey("CUSTOMER SERVICE", Convert.ToInt32(userLocation), Convert.ToInt32(employeeID), System.DateTime.Now, "", "", "", Convert.ToInt64(userID));
                }
                var _with1 = insCommand;
                _with1.Transaction = TRAN;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".CUSTOMER_SERVICE_TRN_PKG.CUSTOMER_SERVICE_TRN_INS";
                var _with2 = _with1.Parameters;
                //adding by thiyagarajan on 20/1/09:call reg.updation
                _with2.Add("UPDATEFK", CUSTOMER_SERVICE_PK).Direction = ParameterDirection.Input;

                //BIZ_TYPE_IN()
                _with2.Add("BIZ_TYPE_IN", busstype).Direction = ParameterDirection.Input;
                //PROCESS_IN()
                _with2.Add("PROCESS_IN", process).Direction = ParameterDirection.Input;
                //CS_REF_NO_IN()  adding by thiyagarajan on 20/1/09
                _with2.Add("CS_REF_NO_IN", getDefault(CustServiceRefNumber, "0")).Direction = ParameterDirection.Input;
                //CS_DT_IN()
                _with2.Add("CS_DT_IN", csDate).Direction = ParameterDirection.Input;
                //CALLER_IN()
                _with2.Add("CALLER_IN", getDefault(Caller, DBNull.Value)).Direction = ParameterDirection.Input;
                //CALL_TYPE_IN()
                _with2.Add("CALL_TYPE_IN", CallType).Direction = ParameterDirection.Input;
                //JOB_CARD_FK_IN()
                _with2.Add("JOB_CARD_FK_IN", JobCardFk).Direction = ParameterDirection.Input;
                //CUSTOMER_MST_FK_IN()
                _with2.Add("CUSTOMER_MST_FK_IN", CustomerMstFk).Direction = ParameterDirection.Input;
                //ASSIGN_TO_FK_IN()
                _with2.Add("ASSIGN_TO_FK_IN", getDefault(AssignFk, DBNull.Value)).Direction = ParameterDirection.Input;
                //ASSIGN_DT_IN()
                _with2.Add("ASSIGN_DT_IN", getDefault(AssignDate, DateTime.Today)).Direction = ParameterDirection.Input;
                //PROBLEM_IN()
                _with2.Add("PROBLEM_IN", getDefault(Problem, DBNull.Value)).Direction = ParameterDirection.Input;
                //ESCALLATED_TO_FK_IN()
                _with2.Add("ESCALLATED_TO_FK_IN", getDefault(EscalFk, DBNull.Value)).Direction = ParameterDirection.Input;
                //ESCALLATION_DT_IN()
                _with2.Add("ESCALLATION_DT_IN", getDefault(EscallationDate, DateTime.Today)).Direction = ParameterDirection.Input;
                //ISSUE_DESCRIPTION_IN()
                _with2.Add("ISSUE_DESCRIPTION_IN", getDefault(IssueDesc, DBNull.Value)).Direction = ParameterDirection.Input;
                //STATUS_IN()
                _with2.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
                //RESOLVED_BY_FK_IN()
                _with2.Add("RESOLVED_BY_FK_IN", getDefault(ResolFk, DBNull.Value)).Direction = ParameterDirection.Input;
                //RESOLUTION_DT_IN()
                _with2.Add("RESOLUTION_DT_IN", getDefault(ResolutionDate, DateTime.Today)).Direction = ParameterDirection.Input;
                //RESOLUTION_IN()
                _with2.Add("RESOLUTION_IN", getDefault(Resolution, DBNull.Value)).Direction = ParameterDirection.Input;
                //CONFIG_MST_FK_IN()
                _with2.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //CREATED_BY_FK_IN()
                _with2.Add("CREATED_BY_FK_IN", CreatedByFk).Direction = ParameterDirection.Input;
                //RETURN_VALUE()
                _with2.Add("RETURN_VALUE", OracleDbType.Int32, CUSTOMER_SERVICE_PK).Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                _with1.ExecuteNonQuery();
                CUSTOMER_SERVICE_PK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);

                //  End If
                TRAN.Commit();
                lngPkVal = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;

            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                //added by minakshi on 18-feb-08 for protocol rollbacking
                RollbackProtocolKey("CUSTOMER SERVICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), CustServiceRefNumber, System.DateTime.Now);
                //ended by minakshi
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        public int UpdateCS(int CS_FK, int CS_PK, int EscalPk, string EscalDt)
        {
            string Query = null;
            Int32 i = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string DeptDate = null;
            try
            {
                objWK.OpenConnection();
                //strSQL = strSQL & " And to_char(Valid_From_Dt,'dd-Mon-yyyy') like '%" & System.String.Format("{0:dd-MMM-yyyy}", dtDate) & "%' "

                Query = string.Empty;
                Query += "UPDATE customer_service_trn CST ";
                Query += "SET CST.Is_Escallated = 1 , ";
                Query += "    CST.Customer_Service_Fk = " + CS_PK;
                Query += ",    CST.Escallated_To_Fk = " + EscalPk;
                Query += ",    CST.Escallation_Dt =  '" + EscalDt + "'";
                Query += "WHERE CST.CUSTOMER_SERVICE_PK =   " + CS_FK;

                if (objWK.ExecuteCommands(Query) == true)
                {
                    //Return 1
                }
                else
                {
                    return -1;
                }
                return 1;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Generate protocol"
        public string GenerateCSNo(Int64 ILocationId, Int64 IEmployeeId, string sVSL, string sVOY, string sPOL)
        {
            string functionReturnValue = null;
            CREATED_BY = this.CREATED_BY;
            functionReturnValue = GenerateProtocolKey("CS REF NO", ILocationId, IEmployeeId, DateTime.Now, sVSL, sVOY, sPOL);
            return functionReturnValue;
            return functionReturnValue;
        }
        #endregion

        #region "FetchCustomerBooking"
        public DataSet GetCustBookDtls(int Cust_PK = 0, int Vsl_Pk = 0, int ComSchTrnPk = 0, int PolPk = 0, int BookingPk = 0, int BlPk = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            //SM 02Sep2005...Existing query was deleted and replaced
            strSQL = strSQL + " SELECT BL.BOOKING_BL_PK,";
            strSQL = strSQL + " BL.SERVICE_BL_NO,";
            strSQL = strSQL + " BT.BOOKING_TRN_PK,";
            strSQL = strSQL + " BT.BOOKING_ID,";
            strSQL = strSQL + " BT.COMMERCIAL_SCHEDULE_TRN_FK,";
            strSQL = strSQL + " BT.Pol_Fk POL_MST_FK,";
            strSQL = strSQL + " POL.PORT_ID,";
            strSQL = strSQL + " CSH.VESSEL_MST_FK,";
            strSQL = strSQL + " VSL.VESSEL_ID,";
            strSQL = strSQL + " VSL.VESSEL_NAME,";
            strSQL = strSQL + " CSH.VOYAGE_NO";
            strSQL = strSQL + " FROM BOOKING_BL_TRN BL,";
            strSQL = strSQL + " BOOKING_ROUTING_TRN BKRT,";
            strSQL = strSQL + " BOOKING_TRN BT ,";
            strSQL = strSQL + " COMMERCIAL_SCHEDULE_TRN CST,";
            strSQL = strSQL + " COMMERCIAL_SCHEDULE_HDR CSH,";
            strSQL = strSQL + " VESSEL_MST_TBL VSL,";
            strSQL = strSQL + " PORT_MST_TBL POL";
            strSQL = strSQL + " WHERE BT.BOOKING_TRN_PK = BL.BOOKING_TRN_FK(+)";
            strSQL = strSQL + " AND BKRT.BOOKING_TRN_FK = BT.BOOKING_TRN_PK";
            strSQL = strSQL + " AND BKRT.COMMERCIAL_SCHEDULE_TRN_FK = CST.COMMERCIAL_SCHEDULE_TRN_PK";
            strSQL = strSQL + " AND CST.DEPARTURE_VOYAGE_FK = CSH.COMMERCIAL_SCHEDULE_HDR_PK";
            strSQL = strSQL + " AND POL.PORT_MST_PK = BT.Pol_Fk";
            strSQL = strSQL + " AND CSH.VESSEL_MST_FK = VSL.VESSEL_MST_PK";

            if (Cust_PK > 0)
                strSQL += "      AND BT.CUSTOMER_MST_FK =   " + Cust_PK;
            if (Vsl_Pk > 0)
                strSQL += " AND VSL.VESSEL_MST_PK =   " + Vsl_Pk;
            if (ComSchTrnPk > 0)
                strSQL += "      AND CST.COMMERCIAL_SCHEDULE_TRN_PK =   " + ComSchTrnPk;
            if (PolPk > 0)
                strSQL += "      AND POL.PORT_MST_PK =   " + PolPk;
            if (BookingPk > 0)
                strSQL += "      AND BT.BOOKING_TRN_PK =  " + BookingPk;
            if (BlPk > 0)
                strSQL += "      AND BL.BOOKING_BL_PK =  " + BlPk;

            strSQL += "ORDER BY BT.BOOKING_DATE DESC  ";
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

        #region "Get PK"
        public int GetCustPK(int LocationPk, int lblCustPk)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT C.CUSTOMER_MST_PK  ";
            strSQL += "FROM CUSTOMER_MST_TBL C  ";
            strSQL += "WHERE ";
            // C.LOCATION_MST_FK  =   " & LocationPk & vbCrLf
            strSQL += " C.CUSTOMER_MST_PK = " + lblCustPk;

            try
            {
                if (objWF.GetDataSet(strSQL).Tables[0].Rows.Count > 0)
                    return Convert.ToInt32(objWF.GetDataSet(strSQL).Tables[0].Rows[0]["CUSTOMER_MST_PK"]);
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
            return 0;
        }

        public int GetVoyagePK(string VesselId, string VoyageNo = "", string POL = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT CT.COMMERCIAL_SCHEDULE_TRN_PK ";
            strSQL += "FROM COMMERCIAL_SCHEDULE_TRN CT, ";
            strSQL += "     COMMERCIAL_SCHEDULE_HDR CH, ";
            strSQL += "     VESSEL_MST_TBL V, ";
            strSQL += "     PORT_MST_TBL P ";
            strSQL += "WHERE CH.COMMERCIAL_SCHEDULE_HDR_PK = CT.Departure_Voyage_Fk  ";
            strSQL += "      AND CH.VESSEL_MST_FK = V.VESSEL_MST_PK ";
            strSQL += "      AND CT.PORT_MST_FK = P.PORT_MST_PK ";
            strSQL += "      AND UPPER(V.VESSEL_ID) = '" + VesselId.ToUpper().Trim() + "'";
            strSQL += "      AND CH.VOYAGE_NO = '" + VoyageNo + "'";
            strSQL += "      AND UPPER(P.PORT_ID) = '" + POL.ToUpper().Trim() + "'";
            strSQL += " ";

            try
            {
                if (objWF.GetDataSet(strSQL).Tables[0].Rows.Count > 0)
                    return Convert.ToInt32(objWF.GetDataSet(strSQL).Tables[0].Rows[0]["COMMERCIAL_SCHEDULE_TRN_PK"]);
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
            return 0;
        }

        public DataSet GetPK(string BOOKING_ID = "", string BL_No = "", string ContainerNO = "")
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT distinct BL.BOOKING_BL_PK, ";
            strSQL += "       BT.BOOKING_TRN_PK ";
            //strSQL &= "       BT.COMMERCIAL_SCHEDULE_TRN_FK, " & vbCrLf
            //strSQL &= "       BT.Customer_Mst_Fk " & vbCrLf
            strSQL += "FROM BOOKING_BL_TRN BL, ";
            strSQL += "     BOOKING_TRN BT, ";
            strSQL += "     BOOKING_SUMMARY_TRN BS, ";
            strSQL += "     BOOKING_CONTAINERS_TRN BC ";
            strSQL += "WHERE BL.BOOKING_TRN_FK(+) = BT.BOOKING_TRN_PK ";
            strSQL += "      AND BT.BOOKING_TRN_PK(+) = BS.BOOKING_TRN_FK ";
            strSQL += "      AND BS.BOOKING_SUMMARY_PK(+)= BC.BOOKING_SUMMARY_FK      ";
            if (!string.IsNullOrEmpty(BL_No))
                strSQL += "      AND UPPER(BL.SERVICE_BL_NO) = '" + BL_No.ToUpper().Trim() + "'";
            if (!string.IsNullOrEmpty(BOOKING_ID))
                strSQL += "      AND UPPER(BT.BOOKING_ID) = '" + BOOKING_ID.ToUpper().Trim() + "'";
            if (!string.IsNullOrEmpty(ContainerNO))
                strSQL += "      AND UPPER(BC.CONTAINER_NO) = '" + ContainerNO.ToUpper().Trim() + "'";
            //strSQL &= "      AND BT.Customer_Mst_Fk = 4  " & vbCrLf
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

        public int GetEmpPK(int LocationPk, string EmpName)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += "SELECT E.EMPLOYEE_MST_PK  ";
            strSQL += "FROM EMPLOYEE_MST_TBL E ";
            strSQL += "WHERE E.LOCATION_MST_FK =  " + LocationPk;
            strSQL += "AND TRIM(UPPER(E.EMPLOYEE_NAME)) ='" + EmpName.ToUpper().Trim() + "'";

            try
            {
                if (objWF.GetDataSet(strSQL).Tables[0].Rows.Count > 0)
                    return Convert.ToInt32(objWF.GetDataSet(strSQL).Tables[0].Rows[0]["EMPLOYEE_MST_PK"]);
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
            return 0;
        }


        #endregion

        #region "Fetch Request Details"
        public DataSet FetchRequestDetails(int Bkg_TRN_PK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            if (Bkg_TRN_PK != 0)
            {
                strSQL = string.Empty;
                strSQL += "SELECT BC.BOOKING_CONTAINERS_PK, ";
                strSQL += "       BT.BOOKING_TRN_PK, ";
                //strSQL &= "       BC.BOOKING_REEFER_REQ_FK, " & vbCrLf
                //strSQL &= "       BC.BOOKING_HAZ_REQ_FK, " & vbCrLf
                //strSQL &= "       BC.BOOKING_OOD_REQ_FK ,  " & vbCrLf
                strSQL += "       BC.CONTAINER_NO,              ";
                strSQL += "       CTM.CONTAINER_TYPE_MST_ID, ";
                strSQL += "       CGM.COMMODITY_GROUP_CODE ";
                //strSQL &= "       (CASE WHEN BC.BOOKING_REEFER_REQ_FK IS NULL THEN 0 ELSE 1 END) REF_REQ, " & vbCrLf
                //strSQL &= "       (CASE WHEN BC.BOOKING_HAZ_REQ_FK IS NULL THEN 0 ELSE 1 END) HAZ_REQ, " & vbCrLf
                //strSQL &= "       (CASE WHEN BC.BOOKING_OOD_REQ_FK IS NULL THEN 0 ELSE 1 END) ODC_REQ           " & vbCrLf
                strSQL += "FROM BOOKING_CONTAINERS_TRN BC, ";
                //strSQL &= "     BOOKING_SUMMARY_TRN BS, " & vbCrLf
                strSQL += "     BOOKING_TRN BT, ";
                strSQL += "     CONTAINER_TYPE_MST_TBL CTM, ";
                strSQL += "    COMMODITY_GROUP_MST_TBL CGM ";
                strSQL += "WHERE BC.BOOKING_TRN_FK = BT.BOOKING_TRN_PK ";
                strSQL += "      AND BC.CONTAINER_TYPE_MST_FK = CTM.CONTAINER_TYPE_MST_PK ";
                strSQL += "      AND BC.COMMODITY_GROUP_MST_FK = CGM.COMMODITY_GROUP_PK ";
                strSQL += "      AND BT.BOOKING_TRN_PK =  " + Bkg_TRN_PK;
                //strSQL &= "      AND  BOOKING_REEFER_REQ_FK IS NOT NULL " & vbCrLf
                //strSQL &= "      AND BOOKING_HAZ_REQ_FK IS NOT NULL " & vbCrLf
                //strSQL &= "      AND BOOKING_OOD_REQ_FK IS NOT NULL " & vbCrLf
            }
            else
            {
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
        #endregion

        #region " Report Query"
        public DataSet FetchReport(int LocPk, DateTime FROM_DT, DateTime TO_DT, string GroupBy = "", int CustPk = 0, int ExecPk = 0, int ResolPk = 0, int EscalPk = 0, int CallType = 0, int Status = -1)
        {

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();


            strSQL = string.Empty;
            strSQL += "SELECT         ";
            if (GroupBy == "CS_DT")
            {
                strSQL += "to_char(" + GroupBy + ",'dd-Mon-yyyy') GroupBy, ";
            }
            else if (GroupBy == "RESOLUTION_DT")
            {
                strSQL += "to_char(" + GroupBy + ",'dd-Mon-yyyy') GroupBy, ";
            }
            else if (GroupBy == "CST.STATUS")
            {
                strSQL += "(CASE WHEN " + GroupBy + "= 1 THEN 'Closed' ELSE 'Open' END) GroupBy, ";
            }
            else if (!string.IsNullOrEmpty(GroupBy))
            {
                strSQL += GroupBy + " GroupBy,";
            }
            strSQL += "                   CST.CS_REF_NO, ";
            strSQL += "                   TO_CHAR(CST.CS_DT,'dd-Mon-yyyy HH24:MI') CS_DT,  ";
            strSQL += "                   BT.BOOKING_ID,  ";
            strSQL += "                   BBT.SERVICE_BL_NO,  ";
            strSQL += "                   CUS.CUSTOMER_ID,  ";
            strSQL += "                   CST.Caller, ";
            strSQL += "                   VMT.VESSEL_ID  || (CASE WHEN  VMT.VESSEL_ID IS NULL THEN '' ELSE  '/' END ) || CH.VOYAGE_NO || (CASE WHEN CH.VOYAGE_NO IS NULL THEN '' ELSE '/' END )|| POL.PORT_ID VSL_VOY_POL, ";
            strSQL += "                   (CASE  WHEN  CALL_TYPE =1 THEN 'Enquiry' WHEN CALL_TYPE = 2 THEN 'Problem' ELSE '' END) CALL_TYPE,  ";
            strSQL += "                   EXE.EMPLOYEE_NAME EXE_NAME,  ";
            strSQL += "                   ESCAL.EMPLOYEE_NAME ESCAL_NAME, ";
            strSQL += "                   RESOL.EMPLOYEE_NAME RESOL_NAME, ";
            strSQL += "                   CST.Cs_Problem_Type_Fk, ";
            strSQL += "                   PTM.PROBLEM_ID, ";
            strSQL += "                   (CASE WHEN CST.STATUS = 0 THEN 'Open' WHEN CST.STATUS=1 THEN 'Closed' END ) STATUS, ";
            strSQL += "                   ( SELECT ";
            strSQL += "                      CASE WHEN trunc(mod(months_between(resolution_dt,cs_dt), 12)) = 0 THEN '' ELSE  trunc(mod(months_between(resolution_dt,cs_dt), 12)) || 'M' END || ";
            strSQL += "                      CASE WHEN trunc(resolution_dt-add_months(cs_dt, trunc(months_between(resolution_dt,cs_dt)))) =0 THEN '' ELSE trunc(resolution_dt-add_months(cs_dt, trunc(months_between(resolution_dt,cs_dt)))) || 'D' END || ";
            strSQL += "                      CASE WHEN trunc(mod(resolution_dt-cs_dt,1)*24) = 0 THEN '' ELSE trunc(mod(resolution_dt-cs_dt,1)*24) || 'H' END || ";
            strSQL += "                      CASE WHEN trunc(mod((resolution_dt-cs_dt)*24,1)*60) = 0 THEN '' ELSE trunc(mod((resolution_dt-cs_dt)*24,1)*60) || 'm' END  ";
            strSQL += "                      FROM customer_service_trn cst1 ";
            strSQL += "                      WHERE cst1.customer_service_pk =cst.customer_service_pk) TATime, ";
            strSQL += "                    TO_CHAR(CST.RESOLUTION_DT,'dd-Mon-yyyy HH24:MI') RESOLUTION_DT, ";
            strSQL += "                    CST.RESOLUTION ";
            strSQL += "            FROM customer_service_trn CST,  ";
            strSQL += "                 BOOKING_TRN BT,  ";
            strSQL += "                 BOOKING_BL_TRN BBT,  ";
            strSQL += "                 CUSTOMER_MST_TBL CUS,  ";
            strSQL += "                 COMMERCIAL_SCHEDULE_TRN CT,  ";
            strSQL += "                 COMMERCIAL_SCHEDULE_HDR CH,  ";
            strSQL += "                 VESSEL_MST_TBL VMT,  ";
            strSQL += "                 EMPLOYEE_MST_TBL EXE , ";
            strSQL += "                 EMPLOYEE_MST_TBL ESCAL , ";
            strSQL += "                 EMPLOYEE_MST_TBL RESOL , ";
            strSQL += "                 PORT_MST_TBL POL, ";
            strSQL += "                 LOC_PORT_MAPPING_TRN LPM, ";
            strSQL += "                 CS_PROBLEM_TYPE_MST_TBL PTM ";
            strSQL += "            WHERE CST.BOOKING_TRN_FK = BT.BOOKING_TRN_PK(+) ";
            strSQL += "                  AND CST.BOOKING_BL_FK = BBT.BOOKING_BL_PK(+) ";
            strSQL += "                  AND CST.CUSTOMER_MST_FK = CUS.CUSTOMER_MST_PK(+) ";
            strSQL += "                  AND CST.VOYAGE_FK = CT.COMMERCIAL_SCHEDULE_TRN_PK(+) ";
            strSQL += "                  AND CT.departure_voyage_fk = CH.COMMERCIAL_SCHEDULE_HDR_PK(+) ";
            strSQL += "                  AND CH.VESSEL_MST_FK = VMT.VESSEL_MST_PK(+) ";
            strSQL += "                  AND CST.CSR_MST_FK = EXE.EMPLOYEE_MST_PK(+) ";
            strSQL += "                  AND CST.RESOLVED_BY_FK = RESOL.EMPLOYEE_MST_PK(+) ";
            strSQL += "                  AND CST.ESCALLATED_TO_FK = ESCAL.EMPLOYEE_MST_PK(+) ";
            strSQL += "                  AND BT.Pol_Fk = POL.PORT_MST_PK(+) ";
            strSQL += "                  AND POL.PORT_MST_PK = LPM.PORT_MST_FK(+) ";
            strSQL += "                  AND CST.CS_PROBLEM_TYPE_FK = PTM.CS_PROBLEM_TYPE_MST_PK(+) ";
            strSQL += "      AND LPM.LOCATION_MST_FK(+) =  " + LocPk;
            if (CustPk != 0)
                strSQL += "      AND CUS.CUSTOMER_MST_PK =  " + CustPk;
            if (ExecPk != 0)
                strSQL += "      AND EXE.EMPLOYEE_MST_PK =  " + ExecPk;
            if (ResolPk != 0)
                strSQL += "      AND RESOL.EMPLOYEE_MST_PK =  " + ResolPk;
            if (EscalPk != 0)
                strSQL += "      AND ESCAL.EMPLOYEE_MST_PK =  " + EscalPk;
            if (CallType != 0)
                strSQL += "      AND CST.CALL_TYPE =  " + CallType;
            if (Status != -1)
                strSQL += "      AND CST.STATUS =   " + Status;
            if (FROM_DT != DateTime.MinValue & TO_DT != DateTime.MinValue)
				strSQL += " AND  (To_char(CST.CS_DT,'dd-Mon-yyyy') Between '" + System.String.Format("{0:dd-MMM-yyyy}", FROM_DT) + "' AND '" + System.String.Format("{0:dd-MMM-yyyy}", TO_DT) + "')";

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

        #region "Fetch Port"
        public DataSet FetchPort(int Loc_PK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = string.Empty;
            strSQL += " SELECT 0 PORT_MST_PK, ";
            strSQL += "        ' ' PORT_ID ";
            strSQL += " FROM DUAL ";
            strSQL += " UNION        ";
            strSQL += " SELECT P.PORT_MST_PK, ";
            strSQL += "        P.PORT_ID ";
            strSQL += " FROM PORT_MST_TBL P, ";
            strSQL += "      LOC_PORT_MAPPING_TRN L ";
            strSQL += " WHERE P.PORT_MST_PK = L.PORT_MST_FK ";
            strSQL += "       AND L.LOCATION_MST_FK =  " + Loc_PK;
            strSQL += " ORDER BY PORT_ID  ";
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

    }
}