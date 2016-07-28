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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_BLClause : CommonFeatures
    {


        #region "Fetch All"
        public DataSet FetchAll(string Ref_No = "", string Desc = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int ActiveFlag = 1, bool blnSortAscending = false, int intProcessType = 0, int intClauseType = 0,
        string FromDate = "", string ToDate = "", int intBusType = 0, int intUser = 0, Int32 flag = 0)
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

            if (Ref_No.Trim().Length > 0)
            {
                //strCondition &= vbCrLf & " AND BL_CLAUSE_PK = " & Ref_No & " " & vbCrLf

                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND REFERENCE_NR LIKE '" + Ref_No.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        strCondition += " AND REFERENCE_NR LIKE '%" + Ref_No.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
                else
                {
                    strCondition += " AND UPPER(REFERENCE_NR) LIKE '%" + Ref_No.ToUpper().Replace("'", "''") + "%'" ;
                }
            }

            if (Desc.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND BL_DESCRIPTION LIKE '" + Desc.ToUpper().Replace("'", "''") + "%'" ;
                    }
                    else
                    {
                        strCondition += " AND BL_DESCRIPTION LIKE '%" + Desc.ToUpper().Replace("'", "''") + "%'" ;
                    }
                }
                else
                {
                    strCondition += " AND UPPER(BL_DESCRIPTION) LIKE '%" + Desc.ToUpper().Replace("'", "''") + "%'" ;
                }
            }

            if (ActiveFlag == 1)
            {
                strCondition += " AND ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += "";
            }

            //If intBusType = 3 And intUser = 3 Then
            //    strCondition &= " AND BUSINESS_TYPE IN (1,2,3) "
            //ElseIf intBusType = 3 And intUser = 2 Then
            //    strCondition &= " AND BUSINESS_TYPE IN (2,3) "
            //ElseIf intBusType = 3 And intUser = 1 Then
            //    strCondition &= " AND BUSINESS_TYPE IN (1,3) "
            //Else
            //    strCondition &= " AND BUSINESS_TYPE = " & intBusType & " "
            //End If
            if (intBusType != 0)
            {
                strCondition += " AND BUSINESS_TYPE = " + intBusType + " ";
            }

            if (intProcessType == 3)
            {
                strCondition += " AND PROCESS_TYPE IN  (1,2) ";
            }
            else
            {
                strCondition += " AND PROCESS_TYPE =  " + intProcessType + " ";
            }


            strCondition += " AND CLAUSES_TYPE_MST_FK =  " + intClauseType + " ";

            if (ToDate.Length > 0 & FromDate.Length > 0)
            {
                //strCondition &= " AND ((TO_DATE('" & ToDate & "' , '" & dateFormat & "') BETWEEN"
                //strCondition &= "    VALID_FROM AND VALID_TO) OR"
                //strCondition &= "    (TO_DATE('" & FromDate & "' , '" & dateFormat & "') BETWEEN"
                //strCondition &= "    VALID_FROM AND VALID_TO) OR"
                //strCondition &= "    (VALID_TO IS NULL))"
                strCondition += "AND TO_DATE(VALID_FROM, 'dd/MM/yyyy') >= TO_DATE('" + FromDate + "', 'dd/MM/yyyy') AND TO_DATE(VALID_TO,'dd/MM/yyyy') <= TO_DATE('" + ToDate + "', 'dd/MM/yyyy')";

            }
            else if (ToDate.Length > 0 & !(FromDate.Length > 0))
            {
                strCondition += " AND TO_DATE(VALID_TO,'dd/MM/yyyy') <= TO_DATE('" + ToDate + "', 'dd/MM/yyyy')";
                //strCondition &= "  OR  (VALID_TO IS NULL))    "
            }
            else if (FromDate.Length > 0 & !(ToDate.Length > 0))
            {
                strCondition += "AND TO_DATE(VALID_FROM, 'dd/MM/yyyy') >= TO_DATE('" + FromDate + "', 'dd/MM/yyyy') ";
                //strCondition &= "  OR  (VALID_FROM IS NULL))    "
            }

            strSQL = "SELECT Count(*) from BL_CLAUSE_TBL where 1=1 ";
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

            strSQL = " select * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += "DECODE(ACTIVE_FLAG ,'0','false','1','true') ACTIVE_FLAG  , ";
            // Just changed from 1 to true
            //strSQL &= vbCrLf & "BL_CLAUSE_PK, "                                              ' 0 to false
            strSQL += "BL_CLAUSE_PK ClausePK, ";
            strSQL += "REFERENCE_NR RefNo, ";
            strSQL += " DECODE(CLAUSES_TYPE_MST_FK,1,'General',2,'Quotation',3,'Booking',4,'HBL/HAWB',5,'MBL/MAWB',6,'CAN',7,'DO',8,'Invoice',9,'Invoice to CB Agent',10,'Invoice to Load Agent',11,'Inv. to DP Agent') CLAUSE_TYPE, ";
            strSQL += " DECODE(BUSINESS_TYPE,'1','Air','2','Sea','3','Both') BUSINESS_TYPE, ";
            strSQL += " DECODE(PROCESS_TYPE,'1','Export','2','Import','3','Both') PROCESS_TYPE, ";
            strSQL += "BL_DESCRIPTION,";
            strSQL += " TO_DATE(VALID_FROM,DATEFORMAT) VALID_FROM, ";
            strSQL += " TO_DATE(VALID_TO,DATEFORMAT) VALID_TO";
            //strSQL &= vbCrLf & " VERSION_NO"
            strSQL += " FROM BL_CLAUSE_TBL ";
            strSQL += "WHERE 1=1 ";

            strSQL += strCondition;
            strSQL += "order by BUSINESS_TYPE";
            //If Not strColumnName.Equals("SR_NO") Then
            //    'strSQL &= vbCrLf & "order by " & strColumnName

            //End If

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL += ") q  ) WHERE SR_NO  Between " + start + " and " + last;


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

        public DataSet FetchAvailablePorts(string strCountry, int intRefNo, Int16 intBusinessType)
        {

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();


            if (!(intRefNo == 0) & !(strCountry == "All Countries"))
            {
                strSQL = "  SELECT DD.PORT_MST_PK,";
                strSQL += " 0 ACTIVE_FLAG,";
                strSQL += "  DD.PORT_ID,";
                strSQL += "  DD.PORT_NAME,";
                strSQL += " CC.COUNTRY_ID,";
                strSQL += " CC.COUNTRY_NAME, ";
                strSQL += " CC.COUNTRY_NAME, ";
                strSQL += " 0 SPort_Mst_PK ";
                strSQL += " FROM PORT_MST_TBL DD, ";
                strSQL += " COUNTRY_MST_TBL CC ";
                strSQL += " WHERE DD.ACTIVE_FLAG =1 ";
                //Added by Rijesh  23/11/2005
                strSQL += " AND DD.BUSINESS_TYPE = " + intBusinessType + "";
                strSQL += " AND CC.COUNTRY_NAME LIKE '" + strCountry + "'";
                strSQL += " AND DD.COUNTRY_MST_FK IN( ";
                strSQL += " SELECT COUNTRY_MST_PK ";
                strSQL += " FROM COUNTRY_MST_TBL  ";
                strSQL += " WHERE COUNTRY_NAME ";
                strSQL += " LIKE '" + strCountry + "') ";
                strSQL += " AND DD.PORT_MST_PK ";
                strSQL += " NOT IN( SELECT TRN.PORT_MST_FK FROM ";
                strSQL += " BL_CLAUSE_TRN TRN WHERE ";
                strSQL += " TRN.BL_CLAUSE_FK =" + intRefNo + ")  ORDER BY DD.PORT_ID";
            }
            else if (strCountry == "All Countries")
            {
                strSQL = "  SELECT DD.PORT_MST_PK,";
                strSQL += " 0 ACTIVE_FLAG,";
                strSQL += "  DD.PORT_ID,";
                strSQL += "  DD.PORT_NAME,";
                strSQL += " CC.COUNTRY_ID,";
                strSQL += " CC.COUNTRY_NAME, ";
                strSQL += " 0 SPort_Mst_PK ";
                strSQL += " FROM PORT_MST_TBL DD, ";
                strSQL += " COUNTRY_MST_TBL CC ";
                strSQL += " WHERE DD.ACTIVE_FLAG =1 and DD.COUNTRY_MST_FK = CC.COUNTRY_MST_PK  ";
                //strSQL &= vbCrLf & " AND CC.COUNTRY_NAME LIKE '" & strCountry & "'"
                //strSQL &= vbCrLf & " AND DD.COUNTRY_MST_FK IN( "
                //strSQL &= vbCrLf & " SELECT COUNTRY_MST_PK "
                //strSQL &= vbCrLf & " FROM COUNTRY_MST_TBL  "
                //strSQL &= vbCrLf & " WHERE "
                //strSQL &= vbCrLf & " LIKE '" & strCountry & "') "

                //Added by Rijesh  23/11/2005
                //Added by Rijesh  23/11/2005
                //Commented by Snigdharani as for Sea it is already sending 2 and for air it is sending 1
                //So no need to subtract 1 from intBusinessType - 12/12/2008
                //intBusinessType = intBusinessType - 1
                strSQL += " AND DD.BUSINESS_TYPE = " + intBusinessType + "";
                strSQL += " AND DD.PORT_MST_PK ";
                strSQL += " NOT IN( SELECT TRN.PORT_MST_FK FROM ";
                strSQL += " BL_CLAUSE_TRN TRN WHERE ";
                strSQL += " TRN.BL_CLAUSE_FK =" + intRefNo + ")  ORDER BY DD.PORT_ID";
            }
            else
            {
                strSQL = "  SELECT DD.PORT_MST_PK,";
                strSQL += " 0 ACTIVE_FLAG,";
                strSQL += "  DD.PORT_ID,";
                strSQL += "  DD.PORT_NAME,";
                strSQL += " CC.COUNTRY_ID,";
                strSQL += " CC.COUNTRY_NAME, ";
                strSQL += " 0 SPort_Mst_PK ";
                strSQL += " FROM PORT_MST_TBL DD, ";
                strSQL += " COUNTRY_MST_TBL CC ";
                strSQL += " WHERE DD.ACTIVE_FLAG =1 ";
                strSQL += " AND CC.COUNTRY_NAME LIKE '" + strCountry + "'";
                strSQL += " AND DD.COUNTRY_MST_FK IN( ";
                strSQL += " SELECT COUNTRY_MST_PK ";
                strSQL += " FROM COUNTRY_MST_TBL  ";
                strSQL += " WHERE COUNTRY_NAME ";
                strSQL += " LIKE '" + strCountry + "' ";
                //Added by Rijesh  23/11/2005
                //Added by Rijesh  23/11/2005
                //Commented by Snigdharani as for Sea it is already sending 2 and for air it is sending 1
                //So no need to subtract 1 from intBusinessType - 12/12/2008
                //intBusinessType = intBusinessType - 1
                strSQL += "  AND DD.BUSINESS_TYPE = " + intBusinessType + "";
                strSQL += " )  ORDER BY DD.PORT_ID";
                //strSQL &= vbCrLf & " AND DD.PORT_MST_PK "
                //strSQL &= vbCrLf & " NOT IN( SELECT TRN.PORT_MST_FK FROM "
                //strSQL &= vbCrLf & " BL_CLAUSE_TRN TRN WHERE "
                //strSQL &= vbCrLf & " (TRN.BL_CLAUSE_FK =" & intRefNo & " OR "
                //strSQL &= vbCrLf & " TRN.BL_CLAUSE_FK <>" & intRefNo & " )) "

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

        public DataSet FetchSelectedPorts(int intRefNo)
        {

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "  SELECT PORT.PORT_MST_PK,0 ACTIVE_FLAG,PORT.PORT_ID, ";
            strSQL += " PORT.PORT_NAME,  ";
            strSQL += " COUNTRY.COUNTRY_ID, ";
            strSQL += " COUNTRY.COUNTRY_NAME ";
            strSQL += " FROM BL_CLAUSE_TRN BL, ";
            strSQL += " PORT_MST_TBL PORT,";
            strSQL += " COUNTRY_MST_TBL COUNTRY ";
            strSQL += " WHERE BL.BL_CLAUSE_FK =  " + intRefNo + " ";
            strSQL += " AND BL.PORT_MST_FK = PORT.PORT_MST_PK ";
            strSQL += " AND PORT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK  ORDER BY PORT.PORT_ID";


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

        public DataSet FillCombo()
        {

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "  SELECT D.COUNTRY_NAME FROM COUNTRY_MST_TBL D WHERE D.ACTIVE_FLAG=1 ORDER BY D.COUNTRY_NAME";

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

        public DataSet GetMainData(int refNo)
        {

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "  SELECT C.BL_CLAUSE_PK, C.REFERENCE_NR, C.BL_DESCRIPTION,C.ACTIVE_FLAG,C.CREATED_BY_FK,C.PROCESS_TYPE,C.BUSINESS_TYPE,C.CLAUSES_TYPE_MST_FK,TO_CHAR(C.VALID_FROM,'DD/MM/YYYY') VALID_FROM,TO_CHAR(C.VALID_TO,'DD/MM/YYYY') VALID_TO,C.LAST_MODIFIED_BY_FK,C.VERSION_NO FROM BL_CLAUSE_TBL C WHERE C.BL_CLAUSE_PK = " + refNo;
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

        #region "Save Function"

        public ArrayList Save(string Protocol, DataSet M_DataSet, string ports, string Commodity, string intReferenceNumber, string Valid_From = "", string Valid_To = "", int Version = 0)
        {
            //Public Function Save(ByRef M_DataSet As DataSet, ByVal ports As String, ByVal intReferenceNumber As String, Optional ByVal Valid_From As String = "", Optional ByVal Valid_To As String = "", Optional ByVal Version As Integer = 0) As ArrayList
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            //Dim Commodity As String
            string retVal = null;
            int intPKVal = 0;
            int intClauseType = 0;
            int intProcessType = 0;
            //Dim Protocol As String
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
                //Protocol = GenerateClauseNr(CLng(Session("LOGED_IN_LOC_FK")), CLng(Session("EMP_PK")), M_CREATED_BY_FK, objWK)
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".BL_CLAUSE_TBL_PKG.BL_CLAUSE_TBL_INS";
                var _with2 = _with1.Parameters;

                //insCommand.Parameters.Add("BL_CLAUSE_PK_IN", OracleDbType.Int32, 10, "BL_CLAUSE_PK").Direction = ParameterDirection.Input
                //insCommand.Parameters("BL_CLAUSE_PK_IN").SourceVersion = DataRowVersion.Current

                insCommand.Parameters.Add("BL_DESCRIPTION_IN", OracleDbType.Varchar2, 500, "BL_DESCRIPTION").Direction = ParameterDirection.Input;
                insCommand.Parameters["BL_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("PORT_MST_FK_IN", ports).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("COMM_MST_FK_IN", Commodity).Direction = ParameterDirection.Input;
                //insCommand.Parameters.Add("CREATED_DT_IN", OracleDbType.Int32, 20, "CREATED_DT").Direction = ParameterDirection.Input
                //insCommand.Parameters("CREATED_DT_IN").SourceVersion = DataRowVersion.Current

                insCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CLAUSES_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CLAUSES_TYPE_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CLAUSES_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Date, 30, "VALID_FROM").Direction = ParameterDirection.Input;
                insCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Date, 30, "VALID_TO").Direction = ParameterDirection.Input;
                insCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters("REFERENCE_NR_IN", intReferenceNumber).Direction = DataRowVersion.Current
                //If IsDBNull(M_DataSet.Tables(0).Rows(i).Item("REFERENCE_NR")) Then
                insCommand.Parameters.Add("REFERENCE_IN", Protocol).Direction = ParameterDirection.Input;
                //Else
                //    insCommand.Parameters.Add("REFERENCE_NR_IN", M_DataSet.Tables(0).Rows(i).Item("REFERENCE_NR")).Direction = ParameterDirection.Input
                //End If

                //insCommand.Parameters.Add("VALID_FROM_IN", WebDateChooser).Direction = ParameterDirection.Input

                insCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "BL_CLAUSE_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;




                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".BL_CLAUSE_TBL_PKG.BL_CLAUSE_TBL_UPD";
                var _with4 = _with3.Parameters;

                updCommand.Parameters.Add("BL_CLAUSE_PK_IN", OracleDbType.Int32, 10, "BL_CLAUSE_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["BL_CLAUSE_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BL_DESCRIPTION_IN", OracleDbType.Varchar2, 500, "BL_DESCRIPTION").Direction = ParameterDirection.Input;
                updCommand.Parameters["BL_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CLAUSES_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CLAUSES_TYPE_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CLAUSES_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Date, 20, "VALID_FROM").Direction = ParameterDirection.Input;
                updCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Date, 20, "VALID_TO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PORT_MST_FK_IN", ports).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("COMM_MST_FK_IN", Commodity).Direction = ParameterDirection.Input;
                //updCommand.Parameters("PORTS_ID_IN").SourceVersion = DataRowVersion.Current
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CONFIG_MST_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //updCommand.Parameters.Add("BUSINESS_TYPE_IN", intBusinessType).Direction = ParameterDirection.Input
                //updCommand.Parameters("CONFIG_MST_PK_IN").SourceVersion = DataRowVersion.Current

                //updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", OracleDbType.Int32, 10, "LAST_MODIFIED_BY_FK").Direction = ParameterDirection.Input
                //updCommand.Parameters("LAST_MODIFIED_BY_FK_IN").SourceVersion = DataRowVersion.Current

                //updCommand.Parameters.Add("CONFIG_MST_PK_IN", OracleDbType.Int32, 1, "CONFIG_MST_PK").Direction = ParameterDirection.Input
                //updCommand.Parameters("CONFIG_MST_PK_IN").SourceVersion = DataRowVersion.Current

                //insCommand.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 1, "PORT_MST_FK").Direction = ParameterDirection.Input
                //insCommand.Parameters("PORT_MST_FK_IN").SourceVersion = DataRowVersion.Current


                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;
                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;
                RecAfct = _with5.Update(M_DataSet);
                retVal = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    if ((retVal != null))
                    {
                        M_DataSet.Tables[0].Rows[0]["BL_CLAUSE_PK"] = retVal;
                        M_DataSet.Tables[0].Rows[0]["VERSION_NO"] = 0;
                    }
                    else
                    {
                        M_DataSet.Tables[0].Rows[0]["VERSION_NO"] = Convert.ToInt32(M_DataSet.Tables[0].Rows[0]["VERSION_NO"]) + 1;
                    }
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
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
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion


        public string GetPrimaryDesc(string strDesc)
        {

            WorkFlow objWF = new WorkFlow();
            //common class object.
            string strReturn = "";
            string strSQL = "";

            strSQL = " SELECT A.BL_DESCRIPTION FROM BL_CLAUSE_TBL A WHERE A.BL_CLAUSE_PK =" + strDesc;

            objWF.MyDataReader = objWF.GetDataReader(strSQL);

            try
            {
                while (objWF.MyDataReader.Read())
                {
                    strReturn = Convert.ToString(objWF.MyDataReader[0]);
                }
                //Manjunath  PTS ID:Sep-02   12/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                string s = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.MyDataReader.Close();
                objWF.MyConnection.Close();
            }
            return strReturn;

        }

        public string GetPrimaryBusinessType(string strDesc)
        {

            WorkFlow objWF = new WorkFlow();
            //common class object.
            string strReturn = "";
            string strSQL = "";

            strSQL = " SELECT A.BUSINESS_TYPE FROM BL_CLAUSE_TBL A WHERE A.BL_CLAUSE_PK =" + strDesc;

            objWF.MyDataReader = objWF.GetDataReader(strSQL);

            try
            {
                while (objWF.MyDataReader.Read())
                {
                    strReturn = Convert.ToString(objWF.MyDataReader[0]);
                }
                //Manjunath  PTS ID:Sep-02   12/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                string s = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.MyDataReader.Close();
                objWF.MyConnection.Close();
            }
            return strReturn;

        }

        public string GetPrimaryActiveFlag(string strDesc)
        {

            WorkFlow objWF = new WorkFlow();
            //common class object.
            string strReturn = "";
            string strSQL = "";

            strSQL = " SELECT A.ACTIVE_FLAG FROM BL_CLAUSE_TBL A WHERE A.BL_CLAUSE_PK =" + strDesc;

            objWF.MyDataReader = objWF.GetDataReader(strSQL);

            try
            {
                while (objWF.MyDataReader.Read())
                {
                    strReturn = Convert.ToString(objWF.MyDataReader[0]);
                }
                //Manjunath  PTS ID:Sep-02   12/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                string s = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.MyDataReader.Close();
                objWF.MyConnection.Close();
            }
            return strReturn;

        }


        public string GetPrimaryKey(string strDesc)
        {

            WorkFlow objWF = new WorkFlow();
            //common class object.
            string strReturn = "";
            string strSQL = "";

            strSQL = " SELECT A.BL_CLAUSE_PK FROM BL_CLAUSE_TBL A WHERE A.BL_DESCRIPTION LIKE '" + strDesc.Replace("'", "''") + "' ";

            objWF.MyDataReader = objWF.GetDataReader(strSQL);

            try
            {
                while (objWF.MyDataReader.Read())
                {
                    strReturn = Convert.ToString(objWF.MyDataReader[0]);
                }
                //Manjunath  PTS ID:Sep-02   12/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                string s = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.MyDataReader.Close();
                objWF.MyConnection.Close();
            }
            return strReturn;

        }
        #region "for mawb blclause drop down"
        public DataSet GetRefBLclouse()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "  SELECT  0 BL_CLAUSE_PK,''BL_DESCRIPTION FROM DUAL UNION";
            strSQL += " SELECT BL_CLAUSE_PK, BL_DESCRIPTION FROM BL_CLAUSE_TBL B";
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

        public DataSet FetchPortClause(string PortPk = "0", string ClauseFK = "0", int AfterSave = 1)
        {

            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (string.IsNullOrEmpty(PortPk))
            {
                PortPk = "0";
            }
            if (ClauseFK == "-1")
            {
                sb.Append("SELECT ROWNUM SLNO,");
                sb.Append("       '' ACTIVE_FLAG,");
                //sb.Append("       B.BL_CLAUSE_PK,")
                sb.Append("       PMT.PORT_MST_PK,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       CMT.COUNTRY_NAME,");
                sb.Append("        0 SPort_Mst_PK ");
                sb.Append("  FROM PORT_MST_TBL    PMT,");
                sb.Append("       COUNTRY_MST_TBL CMT");
                sb.Append(" WHERE CMT.COUNTRY_MST_PK = PMT.COUNTRY_MST_FK");
                sb.Append("   AND PMT.PORT_MST_PK IN (" + PortPk + ")");
            }
            else
            {
                if (!string.IsNullOrEmpty(PortPk))
                {
                    sb.Append(" SELECT DISTINCT ROWNUM SLNO, Q.* FROM ( SELECT");
                    sb.Append("       '' ACTIVE_FLAG,");
                    sb.Append("       BCT.PORT_MST_FK,");
                    sb.Append("       PMT.PORT_ID,");
                    sb.Append("       PMT.PORT_NAME,");
                    sb.Append("       CMT.COUNTRY_NAME,");
                    sb.Append("        0 SPort_Mst_PK ");
                    sb.Append("  FROM BL_CLAUSE_TBL   B,");
                    sb.Append("       BL_CLAUSE_TRN   BCT,");
                    sb.Append("       PORT_MST_TBL    PMT,");
                    sb.Append("       COUNTRY_MST_TBL CMT");
                    sb.Append(" WHERE B.BL_CLAUSE_PK(+) = BCT.BL_CLAUSE_FK");
                    sb.Append("   AND PMT.PORT_MST_PK = BCT.PORT_MST_FK(+)");
                    sb.Append("   AND CMT.COUNTRY_MST_PK = PMT.COUNTRY_MST_FK");
                    sb.Append("   AND B.BL_CLAUSE_PK =" + ClauseFK);
                    sb.Append("   union");
                    sb.Append("   SELECT ");
                    sb.Append("       '' ACTIVE_FLAG,");
                    sb.Append("       PMT.PORT_MST_PK,");
                    sb.Append("       PMT.PORT_ID,");
                    sb.Append("       PMT.PORT_NAME,");
                    sb.Append("       CMT.COUNTRY_NAME,");
                    sb.Append("        0 SPort_Mst_PK ");
                    sb.Append("  FROM PORT_MST_TBL PMT, COUNTRY_MST_TBL CMT");
                    sb.Append(" WHERE CMT.COUNTRY_MST_PK = PMT.COUNTRY_MST_FK");
                    sb.Append("   AND PMT.PORT_MST_PK IN (" + PortPk + ") ");
                    sb.Append("   AND PMT.PORT_MST_PK NOT IN ");
                    sb.Append("    (SELECT BCT.PORT_MST_FK ");
                    sb.Append("   FROM BL_CLAUSE_TBL B, BL_CLAUSE_TRN BCT ");
                    sb.Append("   WHERE B.BL_CLAUSE_PK = BCT.BL_CLAUSE_FK ");
                    sb.Append("   AND B.BL_CLAUSE_PK =" + ClauseFK + "))Q ");

                }
                else
                {
                    sb.Append("     SELECT ROWNUM SLNO,");
                    sb.Append("       '' ACTIVE_FLAG,");
                    //sb.Append("       B.BL_CLAUSE_PK,")
                    sb.Append("       BCT.PORT_MST_FK,");
                    sb.Append("       PMT.PORT_ID,");
                    sb.Append("       PMT.PORT_NAME,");
                    sb.Append("       CMT.COUNTRY_NAME,");
                    sb.Append("        0 SPort_Mst_PK ");
                    sb.Append("  FROM BL_CLAUSE_TBL   B,");
                    sb.Append("       BL_CLAUSE_TRN   BCT,");
                    sb.Append("       PORT_MST_TBL    PMT,");
                    sb.Append("       COUNTRY_MST_TBL CMT");
                    sb.Append(" WHERE B.BL_CLAUSE_PK(+) = BCT.BL_CLAUSE_FK");
                    sb.Append("   AND PMT.PORT_MST_PK = BCT.PORT_MST_FK(+)");
                    sb.Append("   AND CMT.COUNTRY_MST_PK = PMT.COUNTRY_MST_FK");
                    sb.Append("   AND B.BL_CLAUSE_PK=" + ClauseFK);
                }
            }

            try
            {
                return objWF.GetDataSet(sb.ToString());
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
        public DataSet FetchClauseComm(string CommPk = "", string ClauseFK = "0", int AfterSave = 0)
        {

            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (string.IsNullOrEmpty(CommPk))
            {
                CommPk = "0";
            }
            if (ClauseFK == "-1")
            {
                sb.Append("     SELECT ROWNUM SLNO,");
                sb.Append("       '' ACTIVE_FLAG,");
                //sb.Append("       B.BL_CLAUSE_PK,")
                sb.Append("       CMT.COMMODITY_MST_PK,");
                sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
                sb.Append("       CMT.COMMODITY_ID,");
                sb.Append("       CMT.COMMODITY_NAME,");
                sb.Append("        0 SComm_Mst_PK ");
                sb.Append("  FROM COMMODITY_MST_TBL    CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE CGMT.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK");
                sb.Append("   AND CMT.COMMODITY_MST_PK IN (" + CommPk + ")");
            }
            else
            {
                if (!string.IsNullOrEmpty(CommPk))
                {
                    sb.Append("     SELECT DISTINCT ROWNUM SLNO, Q.* FROM ( SELECT");
                    sb.Append("       '' ACTIVE_FLAG,");
                    //sb.Append("       B.BL_CLAUSE_PK,")
                    sb.Append("       BCT.Commodity_Mst_Fk,");
                    sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
                    sb.Append("       CMT.COMMODITY_ID,");
                    sb.Append("       CMT.COMMODITY_NAME,");
                    sb.Append("        0 SComm_Mst_PK ");
                    sb.Append("  FROM BL_CLAUSE_TBL   B,");
                    sb.Append("       BL_CLAUSE_COMM_TRN   BCT,");
                    sb.Append("       COMMODITY_MST_TBL    CMT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE B.BL_CLAUSE_PK(+) = BCT.CLAUSE_MST_FK");
                    sb.Append("   AND CMT.COMMODITY_MST_PK = BCT.COMMODITY_MST_FK(+)");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK");
                    sb.Append("   AND B.BL_CLAUSE_PK=" + ClauseFK);
                    sb.Append("   union");
                    sb.Append("     SELECT ");
                    sb.Append("       '' ACTIVE_FLAG,");
                    //sb.Append("       B.BL_CLAUSE_PK,")
                    sb.Append("       CMT.COMMODITY_MST_PK,");
                    sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
                    sb.Append("       CMT.COMMODITY_ID,");
                    sb.Append("       CMT.COMMODITY_NAME,");
                    sb.Append("        0 SComm_Mst_PK ");
                    sb.Append("  FROM COMMODITY_MST_TBL    CMT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE CGMT.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK");
                    sb.Append("   AND CMT.COMMODITY_MST_PK IN (" + CommPk + "))Q");
                }
                else
                {
                    sb.Append("     SELECT ROWNUM SLNO,");
                    sb.Append("       '' ACTIVE_FLAG,");
                    //sb.Append("       B.BL_CLAUSE_PK,")
                    sb.Append("       BCT.Commodity_Mst_Fk,");
                    sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
                    sb.Append("       CMT.COMMODITY_ID,");
                    sb.Append("       CMT.COMMODITY_NAME, ");
                    sb.Append("        0 SComm_Mst_PK ");
                    sb.Append("  FROM BL_CLAUSE_TBL   B,");
                    sb.Append("       BL_CLAUSE_COMM_TRN   BCT,");
                    sb.Append("       COMMODITY_MST_TBL    CMT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE B.BL_CLAUSE_PK(+) = BCT.CLAUSE_MST_FK");
                    sb.Append("   AND CMT.COMMODITY_MST_PK = BCT.COMMODITY_MST_FK(+)");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK");
                    sb.Append("   AND B.BL_CLAUSE_PK=" + ClauseFK);
                }
            }


            try
            {
                return objWF.GetDataSet(sb.ToString());
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
        //Manjunath 17/07/2011
        public ArrayList DeleteClausePort(ArrayList DeletedRow, int ClausePk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction oraTran = null;
            OracleCommand delCommand = new OracleCommand();
            string strReturn = null;
            string[] arrRowDetail = null;
            Int32 i = default(Int32);
            try
            {
                objWK.OpenConnection();
                for (i = 0; i <= DeletedRow.Count - 1; i++)
                {
                    oraTran = objWK.MyConnection.BeginTransaction();
                    var _with6 = objWK.MyCommand;
                    _with6.Transaction = oraTran;
                    _with6.Connection = objWK.MyConnection;
                    _with6.CommandType = CommandType.StoredProcedure;

                    _with6.CommandText = objWK.MyUserName + ".BL_CLAUSE_TBL_PKG.BL_CLAUSE_TRN_PORT_DEL";
                    arrRowDetail = DeletedRow[i].ToString().Split(',');
                    _with6.Parameters.Clear();
                    var _with7 = _with6.Parameters;
                    _with7.Add("BL_CLAUSE_PK_IN", Convert.ToInt32(ClausePk)).Direction = ParameterDirection.Input;
                    _with7.Add("BL_PORT_MST_PK_IN", Convert.ToInt64(arrRowDetail[0])).Direction = ParameterDirection.Input;
                    _with7.Add("DELETED_BY_FK_IN", 1).Direction = ParameterDirection.Input;
                    _with7.Add("CONFIG_PK_IN", 795).Direction = ParameterDirection.Input;
                    _with7.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with6.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    try
                    {
                        if (_with6.ExecuteNonQuery() > 0)
                        {
                            oraTran.Commit();
                        }
                        else
                        {
                            arrMessage.Add("Child Record Found, Record(s)cannot be deleted");
                            oraTran.Rollback();
                        }
                    }
                    catch (Exception ex)
                    {
                        arrMessage.Add("Child Record Found, Record(s)cannot be deleted");
                        oraTran.Rollback();
                    }
                }
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("Success");
                    return arrMessage;
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
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }

        }
        public ArrayList DeleteClauseComm(ArrayList DeletedRow, int ClausePk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction oraTran = null;
            OracleCommand delCommand = new OracleCommand();
            string strReturn = null;
            string[] arrRowDetail = null;
            Int32 i = default(Int32);
            try
            {
                objWK.OpenConnection();
                for (i = 0; i <= DeletedRow.Count - 1; i++)
                {
                    oraTran = objWK.MyConnection.BeginTransaction();
                    var _with8 = objWK.MyCommand;
                    _with8.Transaction = oraTran;
                    _with8.Connection = objWK.MyConnection;
                    _with8.CommandType = CommandType.StoredProcedure;

                    _with8.CommandText = objWK.MyUserName + ".BL_CLAUSE_TBL_PKG.BL_CLAUSE_TRN_COMM_DEL";
                    arrRowDetail = DeletedRow[i].ToString().Split(',');
                    _with8.Parameters.Clear();
                    var _with9 = _with8.Parameters;
                    _with9.Add("BL_CLAUSE_PK_IN", Convert.ToInt32(ClausePk)).Direction = ParameterDirection.Input;
                    _with9.Add("BL_COMM_MST_PK_IN", Convert.ToInt64(arrRowDetail[0])).Direction = ParameterDirection.Input;
                    _with9.Add("DELETED_BY_FK_IN", 1).Direction = ParameterDirection.Input;
                    _with9.Add("CONFIG_PK_IN", 795).Direction = ParameterDirection.Input;
                    _with9.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with8.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    try
                    {
                        if (_with8.ExecuteNonQuery() > 0)
                        {
                            oraTran.Commit();

                        }
                        else
                        {
                            arrMessage.Add("Child Record Found, Record(s)cannot be deleted");
                            oraTran.Rollback();

                        }
                    }
                    catch (Exception ex)
                    {
                        arrMessage.Add("Child Record Found, Record(s)cannot be deleted");
                        oraTran.Rollback();
                    }
                }
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("Success");
                    return arrMessage;
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
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }

        }
        //End Manjunath
        public string GenerateClauseNr(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            string functionReturnValue = null;
            try
            {
                functionReturnValue = GenerateProtocolKey("CLAUSE MASTER", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, objWK);
                return functionReturnValue;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
            return functionReturnValue;
        }
    }
}