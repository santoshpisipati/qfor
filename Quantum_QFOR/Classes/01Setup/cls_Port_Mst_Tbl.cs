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
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    public class clsPORT_MST_TBL : CommonFeatures
    {
        #region "List of Members of the Class"

        private Int64 M_Port_Mst_Pk;
        private string M_Port_Id;
        private string M_Port_Name;
        private Int16 M_Port_Type;
        private string M_Port_Type_Name;
        private Int64 M_Country_Mst_Fk;
        private string M_Country_ID;
        private string M_Country_Name;

        private DataSet M_Container_DataSet;

        #endregion "List of Members of the Class"

        #region "List of Properties"

        public int lngPkVal;

        public Int64 Port_Mst_Pk
        {
            get { return M_Port_Mst_Pk; }
            set { M_Port_Mst_Pk = value; }
        }

        public string Port_Id
        {
            get { return M_Port_Id; }
            set { M_Port_Id = value; }
        }

        public string Port_Name
        {
            get { return M_Port_Name; }
            set { M_Port_Name = value; }
        }

        public Int16 Port_Type
        {
            get { return M_Port_Type; }
            set { M_Port_Type = value; }
        }

        public string Port_Type_Name
        {
            get { return M_Port_Type_Name; }
            set { M_Port_Type_Name = value; }
        }

        public Int64 Country_Mst_Fk
        {
            get { return M_Country_Mst_Fk; }
            set { M_Country_Mst_Fk = value; }
        }

        public string Country_ID
        {
            get { return M_Country_ID; }
            set { M_Country_ID = value; }
        }

        public string Country_Name
        {
            get { return M_Country_Name; }
            set { M_Country_Name = value; }
        }

        public DataSet ContainerDataSet
        {
            get { return M_Container_DataSet; }
            set { M_Container_DataSet = value; }
        }

        #endregion "List of Properties"

        #region "General Function"

        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        #endregion "General Function"

        #region "Enhance Search & Lookup Search Block "

        //Pls do the impact the analysis before changing as this function
        //as might be accesed by other forms also.
        public string FetchPOL(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = "";
            string strSERACH_IN = null;
            string SEARCH_FLAG_IN = "";
            string strBizType = "0";
            string strReq = null;
            string allWrkPort = null;
            string Import = "";
            string Operator_Mst_PK = "0";
            var strNull = "";
            string From_Dt = "";
            string To_Dt = "";
            string pod = "0";
            //Snigdharani
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                allWrkPort = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                Operator_Mst_PK = Convert.ToString(arr.GetValue(8));
            if (arr.Length > 9)
                From_Dt = Convert.ToString(arr.GetValue(9));
            if (arr.Length > 10)
                To_Dt = Convert.ToString(arr.GetValue(10));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOL_COMMON";

                //Comment by prakash No Need Tocheck
                //If strReq = "L" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON"
                //End If
                //If strReq = "F" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON1"
                //End If

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                _with1.Add("POD_FK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
                _with1.Add("OPERATOR_MST_FK_IN", getDefault(Operator_Mst_PK, 0)).Direction = ParameterDirection.Input;
                _with1.Add("FROM_DT_IN", (string.IsNullOrEmpty(From_Dt) ? strNull : From_Dt)).Direction = ParameterDirection.Input;
                _with1.Add("TO_DT_IN", (string.IsNullOrEmpty(To_Dt) ? strNull : To_Dt)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        public string FetchPOLEnquiry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = "";
            string strSERACH_IN = null;
            string SEARCH_FLAG_IN = "";
            string strBizType = "0";
            string strReq = null;
            string allWrkPort = null;
            string Import = "";
            var strNull = "";
            string pod = "0";
            //Snigdharani
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                allWrkPort = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(7));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOL_COMMON_ENQUIRY";

                //Comment by prakash No Need Tocheck
                //If strReq = "L" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON"
                //End If
                //If strReq = "F" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON1"
                //End If

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input;
                _with2.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                _with2.Add("POD_FK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        public string FetchPOL_WORKINGPORTS(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = "";
            string strSERACH_IN = null;
            string SEARCH_FLAG_IN = "";
            string strBizType = "0";
            string strReq = null;
            string allWrkPort = null;
            string Import = "";
            var strNull = "";
            string pod = "0";
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                allWrkPort = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(7));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOL_WORKPORTS";
                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input;
                _with3.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with3.Add("POD_FK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
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

        public string FetchPOLIMP(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = "";
            string strSERACH_IN = null;
            string SEARCH_FLAG_IN = "";
            string strBizType = "0";
            string strReq = null;
            string allWrkPort = null;
            string Import = "";
            var strNull = "";
            string pod = "0";
            //Snigdharani
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                allWrkPort = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(7));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOL_IMP_COMMON";

                //Comment by prakash No Need Tocheck
                //If strReq = "L" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON"
                //End If
                //If strReq = "F" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON1"
                //End If

                var _with4 = selectCommand.Parameters;
                _with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input;
                _with4.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                _with4.Add("POD_FK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        //ADDED BY THANGADURAI TO FETCH OTHER THAN WORKING PORT
        public string FetchPOD_NOTWORKPORT(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = "";
            string strSERACH_IN = null;
            string strBizType = "0";
            string strReq = null;
            string allWrkPort = null;
            string Import = "";
            var strNull = "";
            string pod = "0";
            //Snigdharani
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            //If arr.Length > 4 Then allWrkPort = arr(4)
            // If arr.Length > 5 Then Import = arr(5)
            //If arr.Length > 6 Then pod = arr(6)
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                // selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON"
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOD_COMMON_NOTWORKINGPORT";
                //Comment by prakash No Need Tocheck
                //If strReq = "L" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON"
                //End If
                //If strReq = "F" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON1"
                //End If

                var _with5 = selectCommand.Parameters;
                _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with5.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                // .Add("POD_FK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input
                _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        //end thangadurai
        public string FetchPOD(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strLOC_MST_IN = "";
            string strBizType = null;
            string strReq = null;
            string Import = "";
            string pol = "0";
            string Operator_Mst_PK = "0";
            var strNull = "";
            string SEARCH_FLAG_IN = "";
            string From_Dt = "";
            string To_Dt = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                pol = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                Operator_Mst_PK = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                From_Dt = Convert.ToString(arr.GetValue(8));
            if (arr.Length > 9)
                To_Dt = Convert.ToString(arr.GetValue(9));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOD_COMMON";

                var _with6 = selectCommand.Parameters;
                _with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with6.Add("LOC_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : "")).Direction = ParameterDirection.Input;
                _with6.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with6.Add("POL_FK_IN", ifDBNull(pol)).Direction = ParameterDirection.Input;
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                _with6.Add("OPERATOR_MST_FK_IN", getDefault(Operator_Mst_PK, 0)).Direction = ParameterDirection.Input;
                _with6.Add("FROM_DT_IN", (string.IsNullOrEmpty(From_Dt) ? strNull : From_Dt)).Direction = ParameterDirection.Input;
                _with6.Add("TO_DT_IN", (string.IsNullOrEmpty(To_Dt) ? strNull : To_Dt)).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        public string FetchPODTariff(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strLOC_MST_IN = "";
            string strBizType = null;
            string strReq = null;
            string Import = "";
            string pol = "0";
            string Operator_Mst_PK = "0";
            var strNull = "";
            string SEARCH_FLAG_IN = "";
            string From_Dt = "";
            string To_Dt = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                pol = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                Operator_Mst_PK = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                From_Dt = Convert.ToString(arr.GetValue(8));
            if (arr.Length > 9)
                To_Dt = Convert.ToString(arr.GetValue(9));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOD_COMMON";

                var _with7 = selectCommand.Parameters;
                _with7.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with7.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with7.Add("LOC_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : "")).Direction = ParameterDirection.Input;
                _with7.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with7.Add("POL_FK_IN", ifDBNull(pol)).Direction = ParameterDirection.Input;
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                _with7.Add("OPERATOR_MST_FK_IN", getDefault(Operator_Mst_PK, 0)).Direction = ParameterDirection.Input;
                _with7.Add("FROM_DT_IN", (string.IsNullOrEmpty(From_Dt) ? strNull : From_Dt)).Direction = ParameterDirection.Input;
                _with7.Add("TO_DT_IN", (string.IsNullOrEmpty(To_Dt) ? strNull : To_Dt)).Direction = ParameterDirection.Input;
                _with7.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        public string FetchPODEnquiry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strLOC_MST_IN = "";
            string strBizType = null;
            string strReq = null;
            string Import = "";
            string pol = "0";
            var strNull = "";
            string SEARCH_FLAG_IN = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                pol = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOD_COMMON_ENQUIRY";

                var _with8 = selectCommand.Parameters;
                _with8.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with8.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with8.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                //.Add("LOC_FK_IN", IIf(strLOC_MST_IN <> "", strLOC_MST_IN, "")).Direction = ParameterDirection.Input
                _with8.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with8.Add("POL_FK_IN", ifDBNull(pol)).Direction = ParameterDirection.Input;
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                _with8.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        public string FetchPODIMP(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strLOC_MST_IN = "";
            string strBizType = null;
            string strReq = null;
            string Import = "";
            string pol = "0";
            var strNull = "";
            string SEARCH_FLAG_IN = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                pol = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOD_IMP_COMMON";

                var _with9 = selectCommand.Parameters;
                _with9.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with9.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with9.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", IIf(strLOC_MST_IN <> "", strLOC_MST_IN, "")).Direction = ParameterDirection.Input
                _with9.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with9.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with9.Add("POL_FK_IN", ifDBNull(pol)).Direction = ParameterDirection.Input;
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                _with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        #region "Fetch All Function"

        public DataSet FetchAll(string portID = "", string portName = "", string PortTypeName = "", string CountryID = "", string CountryName = "", string locationName = "", string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        string SortType = " ASC ", bool ActiveFlag = false, Int32 Btype = 0, Int32 flag = 0)
        {
            Int32 Last = default(Int32);
            Int32 Start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (portID.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND UPPER(PORT_ID) LIKE '%" + portID.ToUpper().Replace("'", "''") + "%'";
            }
            else if (portID.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND UPPER(PORT_ID) LIKE '" + portID.ToUpper().Replace("'", "''") + "%'";
            }

            if (portName.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND UPPER(PORT_NAME) LIKE '%" + portName.ToUpper().Replace("'", "''") + "%'";
            }
            else if (portName.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND UPPER(PORT_NAME) LIKE '" + portName.ToUpper().Replace("'", "''") + "%'";
            }

            if (CountryID.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND UPPER(C.COUNTRY_ID) LIKE '%" + CountryID.ToUpper().Replace("'", "''") + "%'";
            }
            else if (CountryID.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND UPPER(C.COUNTRY_ID) LIKE '" + CountryID.ToUpper().Replace("'", "''") + "%'";
            }

            if (CountryName.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND UPPER(C.COUNTRY_NAME) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'";
            }
            else if (CountryName.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND UPPER(C.COUNTRY_NAME) LIKE '" + CountryName.ToUpper().Replace("'", "''") + "%'";
            }
            if (locationName.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND UPPER(L.LOCATION_NAME) LIKE '%" + locationName.ToUpper().Replace("'", "''") + "%'";
            }
            else if (locationName.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND UPPER(L.LOCATION_NAME) LIKE '" + locationName.ToUpper().Replace("'", "''") + "%'";
            }
            if (Btype != 0)
            {
                strCondition += " AND P.BUSINESS_TYPE =" + Btype;
            }
            if (ActiveFlag == true)
            {
                strCondition += " AND P.ACTIVE_FLAG = 1";
            }

            string strCondition2 = " 1=1 ";
            // for Decode column condition

            if (PortTypeName.Trim().Length > 0)
            {
                strCondition2 += " AND PORT_TYPE_NAME = '" + PortTypeName.ToString() + "' ";
            }

            strSQL = " SELECT COUNT(*) FROM (";
            strSQL += "SELECT ROWNUM SR_NO,Q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += "P.PORT_MST_PK, ";
            strSQL += "P.ACTIVE_FLAG ACTIVE_FLAG, ";
            strSQL += "P.COUNTRY_MST_FK, ";
            strSQL += "P.LOCATION_MST_FK, ";
            strSQL += "L.LOCATION_NAME, ";
            strSQL += "C.COUNTRY_NAME, ";
            strSQL += "C.COUNTRY_ID, ";
            strSQL += "SUBSTR(P.PORT_ID, LENGTH(P.PORT_ID) - 2,3) PORT_ID, ";
            strSQL += "P.PORT_NAME PORT_NAME, ";
            strSQL += "DECODE(P.BUSINESS_TYPE, 0, '', 1,'Air',2,'Sea')BUSINESS_TYPE, ";
            strSQL += "P.PORT_TYPE PORT_TYPE, ";
            strSQL += "DECODE(P.PORT_TYPE, 0, '', 1, 'ICD',2,'SEA','') PORT_TYPE_NAME, ";
            strSQL += "P.VERSION_NO,  P.PORT_GRP_MST_FK PORTGRPFK, PGM.PORT_GRP_CODE PORTGRPID";
            strSQL += "FROM PORT_MST_TBL P, COUNTRY_MST_TBL C, PORT_GROUP_MST_TBL PGM, LOCATION_MST_TBL L ";
            strSQL += "WHERE 1=1 AND P.COUNTRY_MST_FK = C.COUNTRY_MST_PK";
            strSQL += " AND P.LOCATION_MST_FK=L.LOCATION_MST_PK(+)";
            strSQL += " AND P.PORT_GRP_MST_FK=PGM.PORT_GRP_MST_PK(+)";
            strSQL += strCondition;
            strSQL = strSQL + " )Q WHERE " + strCondition2 + " )";
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
            Last = CurrentPage * RecordsPerPage;
            Start = (CurrentPage - 1) * RecordsPerPage + 1;

            strSQL = " SELECT * FROM (";
            strSQL += "SELECT ROWNUM SR_NO,Q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += "P.PORT_MST_PK, ";
            strSQL += "P.ACTIVE_FLAG ACTIVE_FLAG, ";
            strSQL += "P.COUNTRY_MST_FK, ";
            strSQL += "C.COUNTRY_NAME, ";
            strSQL += "C.COUNTRY_ID, ";
            strSQL += "SUBSTR(P.PORT_ID, LENGTH(P.PORT_ID) - 2,3) PORT_ID,  ";
            strSQL += "P.PORT_NAME PORT_NAME, ";
            strSQL += "DECODE(P.BUSINESS_TYPE, 0, '', 1,'Air',2,'Sea')BUSINESS_TYPE, ";
            strSQL += "P.PORT_TYPE PORT_TYPE, ";
            strSQL += "DECODE(P.PORT_TYPE, 0, '', 1, 'ICD',2,'SEA','') PORT_TYPE_NAME, ";
            strSQL += "P.LOCATION_MST_FK, ";
            strSQL += "L.LOCATION_NAME, ";
            strSQL += "P.VERSION_NO, P.PORT_GRP_MST_FK PORTGRPFK, PGM.PORT_GRP_CODE PORTGRPID";
            strSQL += "FROM PORT_MST_TBL P, COUNTRY_MST_TBL C, PORT_GROUP_MST_TBL PGM, LOCATION_MST_TBL L";
            strSQL += "WHERE 1=1 AND P.COUNTRY_MST_FK = C.COUNTRY_MST_PK AND P.LOCATION_MST_FK=L.LOCATION_MST_PK(+)";
            strSQL += " AND P.PORT_GRP_MST_FK=PGM.PORT_GRP_MST_PK(+)";
            strSQL += strCondition;
            strSQL += " order by COUNTRY_NAME," + SortColumn + SortType + " )Q WHERE " + strCondition2 + " )";
            strSQL += " WHERE SR_NO  BETWEEN " + Start + " AND " + Last;
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException SQLEXP)
            {
                ErrorMessage = SQLEXP.Message;
                throw SQLEXP;
            }
            catch (Exception EXP)
            {
                ErrorMessage = EXP.Message;
                throw EXP;
            }
            return new DataSet();
        }

        #region " Methods for Other Objects "

        public DataSet Fetch_Port(Int64 PortPK = 0, string PortID = "", string PortName = "")
        {
            string strSQL = null;
            strSQL = "select  ' ' Port_ID,";
            strSQL = strSQL + "' ' Port_NAME,";
            strSQL = strSQL + "0 Port_MST_PK ";
            strSQL = strSQL + " from DUAL ";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + " SELECT ";
            strSQL = strSQL + " Port_ID, ";
            strSQL = strSQL + " Port_NAME, ";
            strSQL = strSQL + " Port_MST_PK ";
            strSQL = strSQL + " FROM Port_MST_TBL ";
            strSQL = strSQL + " where active_flag=1 ";
            if (PortID.Trim().Length > 0)
            {
                strSQL = strSQL + " AND UPPER(Port_ID) LIKE '%" + PortID.ToUpper() + "%'";
            }
            if (PortName.Trim().Length > 0)
            {
                strSQL = strSQL + " AND UPPER(Port_NAME) LIKE '%" + PortName.ToUpper() + "%'";
            }
            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                if (objDS.Tables[0].Rows.Count <= 0)
                {
                    return null;
                }
                else
                {
                    return objDS;
                }
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        #endregion " Methods for Other Objects "

        #endregion "Fetch All Function"

        #region "Save Function"

        public ArrayList Save(DataSet M_DataSet, bool Import = false)
        {
            //sivachandran 11Jun08 : Imp-Exp-Wiz 16May8
            //Dim objPortCost As New Quantum_QFOR.clsPORT_COSTS_TRN
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            int intPKVal = 0;
            long lngI = 0;

            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();

            try
            {
                DataTable dttbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                dttbl = M_DataSet.Tables[0];
                string INS_Proc = null;
                string DEL_Proc = null;
                string UPD_Proc = null;
                INS_Proc = objWK.MyUserName + ".PORT_MST_TBL_PKG.PORT_MST_TBL_INS";
                DEL_Proc = objWK.MyUserName + ".PORT_MST_TBL_PKG.PORT_MST_TBL_DEL";
                UPD_Proc = objWK.MyUserName + ".PORT_MST_TBL_PKG.PORT_MST_TBL_UPD";

                var _with10 = insCommand;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = INS_Proc;

                _with10.Parameters.Add("PORT_ID_IN", OracleDbType.Varchar2, 5, "PORT_ID").Direction = ParameterDirection.Input;
                _with10.Parameters["PORT_ID_IN"].SourceVersion = DataRowVersion.Current;

                _with10.Parameters.Add("PORT_NAME_IN", OracleDbType.Varchar2, 50, "PORT_NAME").Direction = ParameterDirection.Input;
                _with10.Parameters["PORT_NAME_IN"].SourceVersion = DataRowVersion.Current;

                _with10.Parameters.Add("PORT_TYPE_IN", OracleDbType.Int32, 1, "PORT_TYPE").Direction = ParameterDirection.Input;
                _with10.Parameters["PORT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with10.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with10.Parameters.Add("COUNTRY_MST_FK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_FK").Direction = ParameterDirection.Input;
                _with10.Parameters["COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with10.Parameters.Add("PORT_GRP_FK_IN", OracleDbType.Int32, 10, "PORTGRPFK").Direction = ParameterDirection.Input;
                _with10.Parameters["PORT_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with10.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with10.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                _with10.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                _with10.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with10.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                _with10.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "Port_MST_PK").Direction = ParameterDirection.Output;
                _with10.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with11 = delCommand;
                _with11.Connection = objWK.MyConnection;
                _with11.CommandType = CommandType.StoredProcedure;
                _with11.CommandText = DEL_Proc;

                _with11.Parameters.Add("PORT_MST_PK_IN", OracleDbType.Int32, 10, "Port_MST_PK").Direction = ParameterDirection.Input;
                _with11.Parameters["PORT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with11.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with11.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 10, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                _with11.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with11.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with11.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with12 = updCommand;
                _with12.Connection = objWK.MyConnection;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = UPD_Proc;

                _with12.Parameters.Add("PORT_MST_PK_IN", OracleDbType.Int32, 10, "PORT_MST_PK").Direction = ParameterDirection.Input;
                _with12.Parameters["PORT_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with12.Parameters.Add("PORT_ID_IN", OracleDbType.Varchar2, 5, "PORT_ID").Direction = ParameterDirection.Input;
                _with12.Parameters["PORT_ID_IN"].SourceVersion = DataRowVersion.Current;

                _with12.Parameters.Add("PORT_NAME_IN", OracleDbType.Varchar2, 50, "PORT_NAME").Direction = ParameterDirection.Input;
                _with12.Parameters["PORT_NAME_IN"].SourceVersion = DataRowVersion.Current;

                _with12.Parameters.Add("PORT_TYPE_IN", OracleDbType.Int32, 1, "PORT_TYPE").Direction = ParameterDirection.Input;
                _with12.Parameters["PORT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with12.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                _with12.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with12.Parameters.Add("COUNTRY_MST_FK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_FK").Direction = ParameterDirection.Input;
                _with12.Parameters["COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with12.Parameters.Add("PORT_GRP_FK_IN", OracleDbType.Int32, 10, "PORTGRPFK").Direction = ParameterDirection.Input;
                _with12.Parameters["PORT_GRP_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with12.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                _with12.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                _with12.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                _with12.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with12.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                _with12.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with12.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                _with12.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with12.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with13 = objWK.MyDataAdapter;

                _with13.InsertCommand = insCommand;
                _with13.InsertCommand.Transaction = TRAN;
                _with13.UpdateCommand = updCommand;
                _with13.UpdateCommand.Transaction = TRAN;
                _with13.DeleteCommand = delCommand;
                _with13.DeleteCommand.Transaction = TRAN;
                RecAfct = _with13.Update(M_DataSet);
                TRAN.Commit();
                if (arrMessage.Count > 0)
                {
                    //TRAN.Rollback()
                    return arrMessage;
                }
                else
                {
                    //arrMessage.Add("All Data Saved Successfully") 'sivachandran 05Jun08 Imp-Exp-Wiz16May08
                    if (Import == true)
                    {
                        arrMessage.Add("Data Imported Successfully");
                    }
                    else
                    {
                        arrMessage.Add("All Data Saved Successfully");
                    }
                    //End
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

        //ok
        // ok
        public int DelSave(DataSet M_DataSet, bool DelFlg = false)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction oraTran = null;
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            Int16 i = default(Int16);

            try
            {
                objWK.OpenConnection();

                oraTran = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = oraTran;
                for (i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++)
                {
                    if (Convert.ToBoolean(M_DataSet.Tables[0].Rows[i]["DELFLAG"]) == true)
                    {
                        var _with14 = objWK.MyCommand.Parameters;
                        _with14.Add("PORT_MST_PK_IN", M_DataSet.Tables[0].Rows[i]["Port_MST_PK"]).Direction = ParameterDirection.Input;

                        _with14.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                        _with14.Add("CONFIG_pk_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                        _with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                        objWK.MyCommand.CommandText = objWK.MyUserName + ".PORT_MST_TBL_PKG.PORT_MST_TBL_DEL";
                        if (!(objWK.MyCommand.ExecuteNonQuery() == 1))
                            return -1;
                    }
                    objWK.MyCommand.Parameters.Clear();
                }
                oraTran.Commit();
                return 1;
                //Manjunath  PTS ID:Sep-02  14/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                string a = ex.Message;
                return -1;
                oraTran.Rollback();
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        #endregion "Save Function"

        #region " Fetch for Import"

        //Sivachandran 07Jun08 Imp_Exp_Wiz16May08
        public DataSet Fetch()
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            SQL = "SELECT * FROM port_mst_tbl";
            try
            {
                return objWF.GetDataSet(SQL);
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        //End
        public DataSet FetchPortGroup()
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            SQL = "SELECT PGM.PORT_GRP_MST_PK, PGM.PORT_GRP_CODE FROM PORT_GROUP_MST_TBL PGM";
            try
            {
                return objWF.GetDataSet(SQL);
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        //End
        public DataSet FetchCountry()
        {
            WorkFlow objWF = new WorkFlow();
            string SQL = null;
            SQL = "SELECT * FROM COUNTRY_MST_TBL";
            try
            {
                return objWF.GetDataSet(SQL);
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        //End

        #endregion " Fetch for Import"

        //Added by Faheem for Transport Contract

        #region "Get Ports for Transport Contract "

        public string FetchTCPORT(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strLOC_MST_IN = "";
            string strBizType = null;
            string strReq = null;
            string Import = "";
            string PORTPK = "0";
            var strNull = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                PORTPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PORT_PKG.GETPOL_TC";

                var _with15 = selectCommand.Parameters;
                _with15.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with15.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with15.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : "")).Direction = ParameterDirection.Input;
                _with15.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with15.Add("PORT_FK_IN", ifDBNull(PORTPK)).Direction = ParameterDirection.Input;
                _with15.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        #endregion "Get Ports for Transport Contract "

        //Ended by Faheem

        #region " FetchLocation PK"

        public DataSet FetchLocPK(string strLocName)
        {
            try
            {
                string SQL = null;
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

                sb.Append("SELECT L.LOCATION_MST_PK,L.LOCATION_NAME ");
                sb.Append("FROM LOCATION_MST_TBL L WHERE UPPER(L.LOCATION_NAME)='" + strLocName.ToString().ToUpper().Trim() + "'");

                return (new WorkFlow()).GetDataSet(sb.ToString());
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        #endregion " FetchLocation PK"
    }
}

#region " Previous Code "

//'***************************************************************************************************************
//'*          Company Name            :   Quantum BSO Pvt. Ltd.
//'*          Project Title           :
//'***************************************************************************************************************
//'*          Client Name             :
//'*          Form/Module Name        :   clsPORT_MST_TBL.vb
//'*          Description             :   This class module is representation of PORT_MST_TBL
//'*          Created By              :   AutoGenerate
//'*          Module/Project Leader   :
//'*          Created On              :   12-Mar-2004 23:43
//'*          Last Modified On        :
//'*          Last Modified By        :
//'*          Remarks                 :
//'***************************************************************************************************************
//Imports System.Data.ODBC
//Imports System.Data.OracleClient

//Namespace Master
//    Public Class clsPORT_MST_TBL
//        Inherits CommonFeatures

//#Region "List of Members of the Class"

//        Private M_Port_Mst_Pk As Int64
//        Private M_Port_Id As String
//        Private M_Port_Name As String
//        Private M_Port_Type As String
//        Private M_Country_Mst_Fk As Int64
//        Private M_State_Mst_Fk As Int64
//        Private M_Steaming_In_TimeMI As Int32
//        Private M_Steaming_Out_TimeMI As Int32
//        Private M_Max_Draft As Double
//        Private M_Max_Loa As Double
//        Private M_NoOfBerths As Int32
//        Private M_NoOfGantries As Int32
//        Private M_NoOfMoveHour As Int32

//        'Private M_Steaming_In_Rate As Double
//        'Private M_Steaming_Out_Rate As Double
//        'Private M_Other_Hourly_Charges As Double
//        'Private M_Max_Grt_Capacity As Double
//        'Private M_Max_Reefer_Points As Int64
//        'Private M_Max_Size_Capacity As Double

//        Private M_Country_ID As String
//        Private M_Country_Name As String
//        Private M_State_ID As String
//        Private M_State_Name As String
//        Private M_Container_DataSet As DataSet

//#End Region

//#Region "List of Properties"

//        Public lngPkVal As Integer
//        Public Property Port_Mst_Pk() As Int64
//            Get
//                Return M_Port_Mst_Pk
//            End Get
//            Set(ByVal Value As Int64)
//                M_Port_Mst_Pk = Value
//            End Set
//        End Property
//        Public Property Port_Id() As String
//            Get
//                Return M_Port_Id
//            End Get
//            Set(ByVal Value As String)
//                M_Port_Id = Value
//            End Set
//        End Property
//        Public Property Port_Name() As String
//            Get
//                Return M_Port_Name
//            End Get
//            Set(ByVal Value As String)
//                M_Port_Name = Value
//            End Set
//        End Property

//        Public Property ContainerDataSet() As DataSet
//            Get
//                Return M_Container_DataSet
//            End Get
//            Set(ByVal Value As DataSet)
//                M_Container_DataSet = Value
//            End Set
//        End Property
//        Public Property Port_Type() As String
//            Get
//                Return M_Port_Type
//            End Get
//            Set(ByVal Value As String)
//                M_Port_Type = Value
//            End Set
//        End Property
//        Public Property Country_Mst_Fk() As Int64
//            Get
//                Return M_Country_Mst_Fk
//            End Get
//            Set(ByVal Value As Int64)
//                M_Country_Mst_Fk = Value
//            End Set
//        End Property
//        Public Property Country_ID() As String
//            Get
//                Return M_Country_ID
//            End Get
//            Set(ByVal Value As String)
//                M_Country_ID = Value
//            End Set
//        End Property
//        Public Property Country_Name() As String
//            Get
//                Return M_Country_Name
//            End Get
//            Set(ByVal Value As String)
//                M_Country_Name = Value
//            End Set
//        End Property
//        Public Property State_Mst_Fk() As Int64
//            Get
//                Return M_State_Mst_Fk
//            End Get
//            Set(ByVal Value As Int64)
//                M_State_Mst_Fk = Value
//            End Set
//        End Property
//        Public Property State_ID() As String
//            Get
//                Return M_State_ID
//            End Get
//            Set(ByVal Value As String)
//                M_State_ID = Value
//            End Set
//        End Property
//        Public Property State_Name() As String
//            Get
//                Return M_State_Name
//            End Get
//            Set(ByVal Value As String)
//                M_State_Name = Value
//            End Set
//        End Property
//        Public Property Steaming_In_TimeMI() As Int32
//            Get
//                Return M_Steaming_In_TimeMI
//            End Get
//            Set(ByVal Value As Int32)
//                M_Steaming_In_TimeMI = Value
//            End Set
//        End Property

//        Public Property Steaming_Out_TimeMI() As Int32
//            Get
//                Return M_Steaming_Out_TimeMI
//            End Get
//            Set(ByVal Value As Int32)
//                M_Steaming_Out_TimeMI = Value
//            End Set
//        End Property
//        Public Property Max_Draft() As Double
//            Get
//                Return M_Max_Draft
//            End Get
//            Set(ByVal Value As Double)
//                M_Max_Draft = Value
//            End Set
//        End Property
//        Public Property Max_LOA() As Double
//            Get
//                Return M_Max_Loa
//            End Get
//            Set(ByVal Value As Double)
//                M_Max_Loa = Value
//            End Set
//        End Property
//        Public Property No_Of_Berths() As Int32
//            Get
//                Return M_NoOfBerths
//            End Get
//            Set(ByVal Value As Int32)
//                M_NoOfBerths = Value
//            End Set
//        End Property
//        Public Property No_Of_Gantries() As Int32
//            Get
//                Return M_NoOfGantries
//            End Get
//            Set(ByVal Value As Int32)
//                M_NoOfGantries = Value
//            End Set
//        End Property
//        Public Property No_Of_Moves_Hour() As Int32
//            Get
//                Return M_NoOfMoveHour
//            End Get
//            Set(ByVal Value As Int32)
//                M_NoOfMoveHour = Value
//            End Set
//        End Property

//#End Region

//#Region "Insert Function"
//        Public Function Insert() As Integer
//            Dim objWS As New Business.WorkFlow
//            Dim intPkVal As Int32
//            objWS.MyCommand.CommandType = CommandType.StoredProcedure
//            With objWS.MyCommand.Parameters
//                '.Add("Port_Mst_Pk_IN", M_Port_Mst_Pk).Direction = ParameterDirection.Input
//                .Add("Port_Id_IN", M_Port_Id).Direction = ParameterDirection.Input
//                .Add("Port_Name_IN", M_Port_Name).Direction = ParameterDirection.Input
//                .Add("Port_Type_IN", M_Port_Type).Direction = ParameterDirection.Input
//                .Add("Country_Mst_Fk_IN", M_Country_Mst_Fk).Direction = ParameterDirection.Input
//                .Add("State_Mst_Fk_IN", M_State_Mst_Fk).Direction = ParameterDirection.Input
//                .Add("Steaming_In_Time_IN", M_Steaming_In_TimeMI).Direction = ParameterDirection.Input
//                .Add("Steaming_Out_Time_IN", M_Steaming_Out_TimeMI).Direction = ParameterDirection.Input
//                .Add("Max_Draft_IN", M_Max_Draft).Direction = ParameterDirection.Input
//                .Add("Max_LOA_IN", M_Max_Loa).Direction = ParameterDirection.Input
//                .Add("No_Of_Berths_IN", M_NoOfBerths).Direction = ParameterDirection.Input
//                .Add("No_Of_Gantries_IN", M_NoOfGantries).Direction = ParameterDirection.Input
//                .Add("No_Of_Moves_Hour_IN", M_NoOfMoveHour).Direction = ParameterDirection.Input

//                '.Add("Other_Hourly_Charges_IN", M_Other_Hourly_Charges).Direction = ParameterDirection.Input
//                '.Add("Max_Grt_Capacity_IN", M_Max_Grt_Capacity).Direction = ParameterDirection.Input
//                '.Add("Max_Reefer_Points_IN", M_Max_Reefer_Points).Direction = ParameterDirection.Input
//                '.Add("Max_Size_Capacity_IN", M_Max_Size_Capacity).Direction = ParameterDirection.Input
//                .Add("Created_By_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input
//                .Add("Created_DT_IN", M_CREATED_dT).Direction = ParameterDirection.Input
//                .Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output

//            End With
//            objWS.MyCommand.CommandText = "FEEDERUSER.PORT_MST_TBL_PKG.PORT_MST_TBL_Ins"
//            If objWS.ExecuteCommands() = True Then
//                Return intPkVal
//            Else
//                Return -1
//            End If
//        End Function
//#End Region

//#Region "Update Function"
//        Public Function Update() As Integer
//            Dim objWS As New WorkFlow
//            Dim intPkVal As Int32
//            objWS.MyCommand.CommandType = CommandType.StoredProcedure
//            With objWS.MyCommand.Parameters
//                .Add("Port_Mst_Pk_IN", M_Port_Mst_Pk).Direction = ParameterDirection.Input
//                .Add("Port_Id_IN", M_Port_Id).Direction = ParameterDirection.Input
//                .Add("Port_Name_IN", M_Port_Name).Direction = ParameterDirection.Input
//                .Add("Port_Type_IN", M_Port_Type).Direction = ParameterDirection.Input
//                .Add("Country_Mst_Fk_IN", M_Country_Mst_Fk).Direction = ParameterDirection.Input
//                .Add("State_Mst_Fk_IN", M_State_Mst_Fk).Direction = ParameterDirection.Input
//                .Add("Steaming_In_Time_IN", M_Steaming_In_TimeMI).Direction = ParameterDirection.Input
//                .Add("Steaming_Out_Time_IN", M_Steaming_Out_TimeMI).Direction = ParameterDirection.Input
//                .Add("Max_Draft_IN", M_Max_Draft).Direction = ParameterDirection.Input
//                .Add("Max_LOA_IN", M_Max_Loa).Direction = ParameterDirection.Input
//                .Add("No_Of_Berths_IN", M_NoOfBerths).Direction = ParameterDirection.Input
//                .Add("No_Of_Gantries_IN", M_NoOfGantries).Direction = ParameterDirection.Input
//                .Add("No_Of_Moves_Hour_IN", M_NoOfMoveHour).Direction = ParameterDirection.Input
//                .Add("CREATED_DT_IN", M_CREATED_DT).Direction = ParameterDirection.Input
//                .Add("Last_Modified_By_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input
//                '.Add("Last_Modified_At_IN", to_date('" & CType(M_LAST_MODIFIED_AT.GetDateTimeFormats().GetValue(7), Date) & "','" & dateFormat & "')).Direction = ParameterDirection.Input
//                .Add("VERSION_NO_IN", M_VERSION_NO).Direction = ParameterDirection.Input
//                .Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output

//            End With
//            objWS.MyCommand.CommandText = "FEEDERUSER.PORT_MST_TBL_PKG.PORT_MST_TBL_UPD"
//            If objWS.ExecuteCommands() = True Then
//                Return 1
//            Else
//                Return -1
//            End If
//        End Function
//#End Region

//#Region "Delete Function"
//        Public Function Delete() As Integer
//            Dim objWS As New WorkFlow
//            Dim intPkVal As Int32
//            objWS.MyCommand.CommandType = CommandType.StoredProcedure
//            With objWS.MyCommand.Parameters
//                .Add("Port_Mst_Pk_IN", M_Port_Mst_Pk).Direction = ParameterDirection.Input
//                .Add("VERSION_NO_IN", M_VERSION_NO).Direction = ParameterDirection.Input
//                .Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output
//            End With
//            objWS.MyCommand.CommandText = "FEEDERUSER.PORT_MST_TBL_PKG.PORT_MST_TBL_DEL"
//            If objWS.ExecuteCommands() = True Then
//                Return 1
//            Else
//                Return -1
//            End If
//        End Function
//#End Region

//#Region "FetchPort"
//        Public Function Fetch_Port(Optional ByVal PortPK As Int64 = 0, _
//                           Optional ByVal PortID As String = "", _
//                           Optional ByVal PortName As String = "") As DataSet
//            Dim strSQL As String
//            strSQL = "select  ' ' Port_ID,"
//            strSQL = strSQL & vbCrLf & "' ' Port_NAME,"
//            strSQL = strSQL & vbCrLf & "0 Port_MST_PK "
//            strSQL = strSQL & vbCrLf & " from DUAL "
//            strSQL = strSQL & vbCrLf & " UNION "
//            strSQL = strSQL & vbCrLf & " SELECT "
//            strSQL = strSQL & vbCrLf & " Port_ID, "
//            strSQL = strSQL & vbCrLf & " Port_NAME, "
//            strSQL = strSQL & vbCrLf & " Port_MST_PK "
//            strSQL = strSQL & vbCrLf & " FROM Port_MST_TBL "
//            strSQL = strSQL & vbCrLf & " where active=1 "
//            If PortID.Trim.Length > 0 Then
//                strSQL = strSQL & " AND UPPER(Port_ID) LIKE '%" & PortID.ToUpper & "%'"
//            End If
//            If PortName.Trim.Length > 0 Then
//                strSQL = strSQL & " AND UPPER(Port_NAME) LIKE '%" & PortName.ToUpper & "%'"
//            End If
//            Dim objWF As New WorkFlow
//            Dim objDS As DataSet
//            Try
//                objDS = objWF.GetDataSet(strSQL)
//                If objDS.Tables(0).Rows.Count <= 0 Then
//                    Return Nothing
//                Else
//                    Return objDS
//                End If
//            Catch sqlExp As OracleException
//                ErrorMessage = sqlExp.Message
//                Throw sqlExp
//            Catch exp As Exception
//                ErrorMessage = exp.Message
//                Throw exp
//            End Try

//        End Function
//        Public Function FetchActivePort(Optional ByVal PortPK As Int64 = 0, _
//                           Optional ByVal PortID As String = "", _
//                           Optional ByVal PortName As String = "") As DataSet
//            Dim strSQL As String
//            strSQL = "select  ' ' Port_ID,"
//            strSQL = strSQL & vbCrLf & "' ' Port_NAME,"
//            strSQL = strSQL & vbCrLf & "0 Port_MST_PK "
//            strSQL = strSQL & vbCrLf & " from DUAL "
//            strSQL = strSQL & vbCrLf & " UNION "
//            strSQL = strSQL & vbCrLf & "SELECT "
//            strSQL = strSQL & vbCrLf & "Port_ID, "
//            strSQL = strSQL & vbCrLf & "Port_NAME, "
//            strSQL = strSQL & vbCrLf & "Port_MST_PK "
//            strSQL = strSQL & vbCrLf & "FROM Port_MST_TBL "
//            strSQL = strSQL & vbCrLf & "where active=1 "
//            If PortID.Trim.Length > 0 Then
//                strSQL = strSQL & " AND UPPER(Port_ID) LIKE '%" & PortID.ToUpper & "%'"
//            End If
//            If PortName.Trim.Length > 0 Then
//                strSQL = strSQL & " AND UPPER(Port_NAME) LIKE '%" & PortName.ToUpper & "%'"
//            End If
//            Dim objWF As New WorkFlow
//            Dim objDS As DataSet
//            Try
//                objDS = objWF.GetDataSet(strSQL)
//                If objDS.Tables(0).Rows.Count <= 0 Then
//                    Return Nothing
//                Else
//                    Return objDS
//                End If
//            Catch sqlExp As OracleException
//                ErrorMessage = sqlExp.Message
//                Throw sqlExp
//            Catch exp As Exception
//                ErrorMessage = exp.Message
//                Throw exp
//            End Try

//        End Function

//        Public Function FetchPort(Optional ByVal PortPK As Int64 = 0) As DataSet
//            Dim strSQL As String
//            'strSQL = "SELECT P.* , c.COUNTRY_ID  "
//            'strSQL = strSQL & vbCrLf & " FROM PORT_MST_TBL P, "
//            'strSQL = strSQL & vbCrLf & " COUNTRY_MST_TBL C "
//            'strSQL = strSQL & vbCrLf & " WHERE P.COUNTRY_MST_FK = C.COUNTRY_MST_PK "

//            strSQL = strSQL & vbCrLf & "SELECT "
//            strSQL = strSQL & vbCrLf & "PORT_MST_PK, "
//            strSQL = strSQL & vbCrLf & "PORT_NAME,"
//            strSQL = strSQL & vbCrLf & "PORT_ID,"
//            strSQL = strSQL & vbCrLf & "P.COUNTRY_MST_FK,P.STATE_MST_FK,"
//            strSQL = strSQL & vbCrLf & "PORT_TYPE,"
//            strSQL = strSQL & vbCrLf & "TIME_ZONE,"
//            strSQL = strSQL & vbCrLf & "STEAMING_IN_TIME,"
//            strSQL = strSQL & vbCrLf & "STEAMING_OUT_TIME,"
//            strSQL = strSQL & vbCrLf & "NO_OF_BERTHS,"
//            strSQL = strSQL & vbCrLf & "MAX_DRAFT,"
//            strSQL = strSQL & vbCrLf & "MAX_LOA,"
//            strSQL = strSQL & vbCrLf & "NO_OF_GANTRIES,"
//            strSQL = strSQL & vbCrLf & "NO_OF_MOVES_HOUR,"
//            strSQL = strSQL & vbCrLf & "CASE WHEN ASSOCIATED_PORT_FK IS NULL THEN -1 ELSE ASSOCIATED_PORT_FK END ASSOCIATED_PORT_FK,"
//            strSQL = strSQL & vbCrLf & "P.ACTIVE,P.VERSION_NO,"
//            strSQL = strSQL & vbCrLf & "C.COUNTRY_ID,P.CREATED_BY_FK,P.CREATED_DT,P.LAST_MODIFIED_BY_FK"
//            strSQL = strSQL & vbCrLf & "FROM PORT_MST_TBL P, "
//            strSQL = strSQL & vbCrLf & "COUNTRY_MST_TBL C"
//            strSQL = strSQL & vbCrLf & "WHERE P.COUNTRY_MST_FK = C.COUNTRY_MST_PK"

//            ' If PortPK = 0 Then
//            'Else
//            strSQL = strSQL & vbCrLf & " AND PORT_MST_PK = " & PortPK
//            'End If

//            Dim objWF As New WorkFlow
//            Dim objDS As DataSet
//            Try
//                objDS = objWF.GetDataSet(strSQL)
//                If objDS.Tables(0).Rows.Count <= 0 Then
//                    Return objDS
//                Else
//                    Return objDS
//                End If
//            Catch sqlExp As OracleException
//                ErrorMessage = sqlExp.Message
//                Throw sqlExp
//            Catch exp As Exception
//                ErrorMessage = exp.Message
//                Throw exp
//            End Try
//        End Function

//        Public Function FetchLocPort(Optional ByVal PortPK As Int64 = 0) As DataSet
//            Dim strSQL As String
//            strSQL = "SELECT * "
//            strSQL = strSQL & vbCrLf & "FROM Port_MST_TBL "
//            strSQL = "SELECT * FROM location_working_ports_trn lp WHERE  lp.port_mst_fk  =  " & PortPK

//            Dim objWF As New WorkFlow
//            Dim objDS As DataSet
//            Try
//                objDS = objWF.GetDataSet(strSQL)
//                If objDS.Tables(0).Rows.Count <= 0 Then
//                    Return objDS
//                Else
//                    Return objDS
//                End If
//            Catch sqlExp As OracleException
//                ErrorMessage = sqlExp.Message
//                Throw sqlExp
//            Catch exp As Exception
//                ErrorMessage = exp.Message
//                Throw exp
//            End Try
//        End Function
//#End Region

//#Region "Fetch Function"
//        Public Function FetchAssociatedPort(ByVal associated_port_fk As Long) As DataSet
//            Dim strSQL As String
//            'COMMENTED: AKHILESH 06/07/05
//            'strSQL = "SELECT  0 as PORT_MST_PK,' ' as PORT_ID from dual "
//            'strSQL = strSQL & "UNION "
//            'strSQL = strSQL & "SELECT  P.PORT_MST_PK, "
//            'strSQL = strSQL & "P.PORT_ID "
//            'strSQL = strSQL & "FROM Port_MST_TBL P ORDER BY PORT_ID "
//            strSQL = "SELECT  P.PORT_MST_PK , P.PORT_ID FROM Port_MST_TBL P WHERE P.PORT_MST_PK=" & associated_port_fk
//            Dim objWF As New WorkFlow
//            Try
//                Return objWF.GetDataSet(strSQL)
//            Catch dbExp As OracleException
//                ErrorMessage = dbExp.Message
//                Throw dbExp
//            Catch exp As Exception
//                ErrorMessage = exp.Message
//                Throw exp
//            End Try
//        End Function
//        Public Function Fetch(Optional ByVal Navigation As NavigationType = NavigationType.None, _
//               Optional ByVal CurrentPKValue As Int64 = 0, Optional ByVal PortPK As Int64 = 0, _
//                   Optional ByVal PortID As String = "", _
//                   Optional ByVal PortName As String = "") As Boolean
//            Dim strSQL As String
//            strSQL = "SELECT ROWNUM SR_NO, "
//            strSQL = strSQL & "p.Port_MST_PK, "
//            strSQL = strSQL & "p.Port_ID, "
//            strSQL = strSQL & "p.Port_NAME, "
//            strSQL = strSQL & "p.Port_type, "
//            strSQL = strSQL & "p.Country_Mst_FK, "
//            strSQL = strSQL & "c.Country_ID, "
//            strSQL = strSQL & "c.Country_Name, "
//            strSQL = strSQL & "p.State_Mst_FK, "
//            strSQL = strSQL & "s.State_ID, "
//            strSQL = strSQL & "s.State_Name, "
//            strSQL = strSQL & "p.Steaming_In_Time Steaming_In_TimeMI , "
//            strSQL = strSQL & "p.Steaming_Out_Time Steaming_Out_TimeMI , "
//            strSQL = strSQL & "p.Max_Draft,"
//            strSQL = strSQL & "p.Max_LOA,"
//            strSQL = strSQL & "p.No_Of_Berths,"
//            strSQL = strSQL & "p.No_Of_Gantries,"
//            strSQL = strSQL & "p.No_Of_Moves_Hour, "
//            strSQL = strSQL & "p.Version_No "
//            strSQL = strSQL & "FROM Port_MST_TBL p, Country_MST_TBL c, State_Mst_Tbl s "
//            strSQL = strSQL & "WHERE 1=1 AND p.Country_Mst_FK = c.Country_MST_PK "
//            strSQL = strSQL & "AND p.State_Mst_FK = s.State_MST_PK"
//            If PortPK > 0 Then
//                strSQL = strSQL & " AND UPPER(Port_mst_pk) = " & PortPK
//            End If
//            If PortID.Trim.Length > 0 Then
//                strSQL = strSQL & " AND UPPER(Port_ID) LIKE '%" & PortID.ToUpper & "%'"
//            End If
//            If PortName.Trim.Length > 0 Then
//                strSQL = strSQL & " AND UPPER(Port_NAME) LIKE '%" & PortName.ToUpper & "%'"
//            End If
//            If Navigation <> NavigationType.None Then
//                Select Case Navigation
//                    Case NavigationType.FirstRecord
//                        strSQL = strSQL & " AND p.Port_mst_pk=(SELECT MIN(Port_mst_pk) FROM Port_MST_TBL)"
//                    Case NavigationType.PreviousRecord
//                        strSQL = strSQL & " AND p.Port_mst_pk < " & CurrentPKValue & " ORDER BY p.Port_mst_pk DESC"
//                    Case NavigationType.NextRecord
//                        strSQL = strSQL & " AND p.Port_mst_pk > " & CurrentPKValue & " ORDER BY p.Port_mst_pk"
//                    Case NavigationType.LastRecord
//                        strSQL = strSQL & " AND p.Port_mst_pk=(SELECT MAX(Port_mst_pk) FROM Port_MST_TBL)"
//                End Select
//            End If

//            Dim objWF As New WorkFlow
//            Dim objDR As OracleDataReader
//            Try
//                objDR = objWF.GetDataReader(strSQL)
//                While objDR.Read
//                    Port_Id = CType(objDR("Port_ID"), String)
//                    Port_Mst_Pk = CType(objDR("Port_MST_PK"), Int64)
//                    Port_Name = CType(objDR("Port_NAME"), String)
//                    Port_Type = CType(objDR("Port_Type"), String)
//                    Country_Mst_Fk = CType(objDR("Country_Mst_Fk"), Int64)
//                    Country_ID = CType(objDR("Country_ID"), String)
//                    Country_Name = CType(objDR("Country_Name"), String)
//                    State_Mst_Fk = CType(objDR("State_Mst_Fk"), Int64)
//                    State_ID = CType(objDR("State_ID"), String)
//                    State_Name = CType(objDR("State_Name"), String)
//                    Steaming_In_TimeMI = CType(objDR("Steaming_In_TimeMI"), Int32)
//                    Steaming_Out_TimeMI = CType(objDR("Steaming_Out_TimeMI"), Int32)
//                    Max_Draft = CType(objDR("Max_Draft"), Double)
//                    Max_LOA = CType(objDR("Max_LOA"), Double)
//                    No_Of_Berths = CType(objDR("No_Of_Berths"), Int32)
//                    No_Of_Gantries = CType(objDR("No_Of_Gantries"), Int32)
//                    No_Of_Moves_Hour = CType(objDR("No_Of_Moves_Hour"), Int32)
//                    Version_No = CType(objDR("Version_No"), Int32)
//                    Fetch = True
//                End While
//            Catch sqlExp As OracleException
//                M_ErrorMessage = sqlExp.Message
//                Throw sqlExp
//            Catch exp As Exception
//                M_ErrorMessage = exp.Message
//                Throw exp
//            End Try
//        End Function
//#End Region
//        'fetch all function we dont wont modified_by_fk and  created_dt , modofied date
//        'as it will be bounded to grid

//#Region "Fetch All Function"
//        'Public Function FetchAll(Optional ByVal PortID As String = "", _
//        '    Optional ByVal PortName As String = "", Optional ByVal PortType As String = "", _
//        '    Optional ByVal CountryID As String = "", Optional ByVal CountryName As String = "", _
//        '    Optional ByVal StateID As String = "", Optional ByVal StateName As String = "", _
//        '    Optional ByVal SearchType As String = "", _
//        '    Optional ByVal SortExpression As String = "", _
//        '    Optional ByRef CurrentPage As Int32 = 0, _
//        '    Optional ByRef TotalPage As Int32 = 0, _
//        '    Optional ByVal SortCol As Int16 = 2, _
//        '    Optional ByVal Isactive As Boolean = False _
//        '    ) As DataSet
//        Public Function FetchAll( _
//                    Optional ByVal PortID As String = "", _
//                    Optional ByVal PortName As String = "", _
//                    Optional ByVal PortType As String = "", _
//                    Optional ByVal CountryID As String = "", _
//                    Optional ByVal CountryName As String = "", _
//                    Optional ByVal SearchType As String = "", _
//                    Optional ByVal SortExpression As String = "", _
//                    Optional ByRef CurrentPage As Int32 = 0, _
//                    Optional ByRef TotalPage As Int32 = 0, _
//                    Optional ByVal SortCol As Int16 = 2, _
//                    Optional ByVal Isactive As Boolean = False _
//                    ) As DataSet

//            Dim last As Int32
//            Dim start As Int32
//            Dim strSQL As String
//            Dim strCondition As String
//            Dim TotalRecords As Int32
//            Dim objWF As New WorkFlow

//            If PortID.Trim.Length > 0 And SearchType = "C" Then
//                strCondition &= vbCrLf & " AND UPPER(Port_ID) LIKE '%" & PortID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            ElseIf PortID.Trim.Length > 0 And SearchType = "S" Then
//                strCondition &= vbCrLf & " AND UPPER(Port_ID) LIKE '" & PortID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            End If

//            If PortName.Trim.Length > 0 And SearchType = "C" Then
//                strCondition &= vbCrLf & " AND UPPER(Port_NAME) LIKE '%" & PortName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            ElseIf PortName.Trim.Length > 0 And SearchType = "S" Then
//                strCondition &= vbCrLf & " AND UPPER(Port_NAME) LIKE '" & PortName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            End If

//            If PortType.Trim.Length > 0 Then
//                strCondition &= vbCrLf & " AND UPPER(Port_Type) = '" & PortType.ToUpper.Replace("'", "''") & "'" & vbCrLf
//            End If

//            If CountryID.Trim.Length > 0 And SearchType = "C" Then
//                strCondition &= vbCrLf & " AND UPPER(c.Country_ID) LIKE '%" & CountryID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            ElseIf CountryID.Trim.Length > 0 And SearchType = "S" Then
//                strCondition &= vbCrLf & " AND UPPER(c.Country_ID) LIKE '" & CountryID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            End If

//            If CountryName.Trim.Length > 0 And SearchType = "C" Then
//                strCondition &= vbCrLf & " AND UPPER(c.Country_NAME) LIKE '%" & CountryName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            ElseIf CountryName.Trim.Length > 0 And SearchType = "S" Then
//                strCondition &= vbCrLf & " AND UPPER(c.Country_NAME) LIKE '" & CountryName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            End If

//            If StateID.Trim.Length > 0 And SearchType = "C" Then
//                strCondition &= vbCrLf & " AND UPPER(s.State_ID) LIKE '%" & StateID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            ElseIf StateID.Trim.Length > 0 And SearchType = "S" Then
//                strCondition &= vbCrLf & " AND UPPER(s.State_ID) LIKE '" & StateID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            End If

//            If StateName.Trim.Length > 0 And SearchType = "C" Then
//                strCondition &= vbCrLf & " AND UPPER(s.State_Name) LIKE '%" & StateName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            ElseIf StateName.Trim.Length > 0 And SearchType = "S" Then
//                strCondition &= vbCrLf & " AND UPPER(s.State_Name) LIKE '" & StateName.ToUpper.Replace("'", "''") & "%'" & vbCrLf
//            End If

//            If Isactive Then
//                strCondition &= vbCrLf & " AND P.ACTIVE = 1"
//            End If

//            strSQL = "SELECT Count(*) "
//            strSQL &= vbCrLf & "FROM Port_MST_TBL p, Country_MST_TBL c, State_Mst_Tbl s "
//            strSQL &= vbCrLf & "WHERE 1=1 AND p.Country_Mst_FK = c.Country_MST_PK "
//            strSQL &= vbCrLf & "AND p.State_Mst_FK = s.State_MST_PK(+) "
//            strSQL &= vbCrLf & strCondition
//            TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
//            TotalPage = TotalRecords \ M_MasterPageSize
//            If TotalRecords Mod M_MasterPageSize <> 0 Then
//                TotalPage += 1
//            End If
//            If CurrentPage > TotalPage Then
//                CurrentPage = 1
//            End If
//            If TotalRecords = 0 Then
//                CurrentPage = 0
//            End If
//            last = CurrentPage * M_MasterPageSize
//            start = (CurrentPage - 1) * M_MasterPageSize + 1

//            If CInt(SortCol) > 0 Then
//                strCondition = strCondition & " ORDER BY P.ACTIVE DESC," & CInt(SortCol)
//            End If

//            strSQL = " select * from ("
//            strSQL &= "SELECT ROWNUM SR_NO,q.* FROM "
//            strSQL &= vbCrLf & "(SELECT  "
//            strSQL &= vbCrLf & "p.Port_MST_PK, "
//            strSQL &= vbCrLf & "p.Port_ID, "
//            strSQL &= vbCrLf & "Initcap(p.Port_NAME) Port_NAME, "
//            strSQL &= vbCrLf & "p.Port_type, "
//            strSQL &= vbCrLf & "p.Country_Mst_Fk, "
//            strSQL &= vbCrLf & "c.Country_ID, "
//            strSQL &= vbCrLf & "c.Country_Name Country_Name, "
//            'strSQL &= vbCrLf & "p.time_zone, "
//            'strSQL &= vbCrLf & "p.State_Mst_Fk, "
//            'strSQL &= vbCrLf & "s.State_ID, "
//            'strSQL &= vbCrLf & "initcap(s.State_Name), "
//            'strSQL &= vbCrLf & "'...', "
//            'strSQL &= vbCrLf & "p.STEAMING_IN_TIME, "
//            'strSQL &= vbCrLf & "p.STEAMING_OUT_TIME, "
//            'strSQL &= vbCrLf & "p.Max_Draft, "
//            'strSQL &= vbCrLf & "p.Max_LOA, "
//            'strSQL &= vbCrLf & "p.No_Of_Berths, "
//            'strSQL &= vbCrLf & "p.No_Of_Gantries, "
//            'strSQL &= vbCrLf & "p.No_Of_Moves_Hour, "
//            strSQL &= vbCrLf & "p.Version_No, "
//            strSQL &= vbCrLf & "p.Active_flag "
//            strSQL &= vbCrLf & "FROM Port_MST_TBL p, Country_MST_TBL c "
//            strSQL &= vbCrLf & "WHERE 1=1 AND p.Country_Mst_FK = c.Country_MST_PK "
//            'strSQL &= vbCrLf & "AND p.State_Mst_FK = s.State_MST_PK(+) and "
//            strSQL &= vbCrLf & "  ( 1 = 1) "
//            strSQL &= vbCrLf & strCondition

//            ' strSQL = strSQL & " order by Port_ID"

//            strSQL = strSQL & vbCrLf & " )q) WHERE SR_NO  Between " & start & " and " & last
//            'strSQL = strSQL & vbCrLf & " order by Port_ID  "
//            Try
//                Return objWF.GetDataSet(strSQL)
//            Catch sqlExp As OracleException
//                ErrorMessage = sqlExp.Message
//                Throw sqlExp
//            Catch exp As Exception
//                ErrorMessage = exp.Message
//                Throw exp
//            End Try
//        End Function
//#End Region

//#Region "Save Function"

//        Public Function Save(ByRef M_DataSet As DataSet) As ArrayList
//            Dim objPortCost As New Quantum_QFOR.clsPORT_COSTS_TRN

//            Dim objWK As New WorkFlow
//            objWK.OpenConnection()
//            Dim TRAN As OracleTransaction
//            TRAN = objWK.MyConnection.BeginTransaction()

//            Dim ColPara As New OracleClient.OracleParameterCollection
//            Dim intPKVal As Integer
//            Dim lngI As Long
//            Dim RecAfct As Int32
//            Dim insCommand As New OracleClient.OracleCommand
//            Dim updCommand As New OracleClient.OracleCommand
//            Dim delCommand As New OracleClient.OracleCommand

//            Try

//                'Dim dttbl As New DataTable
//                'Dim DtRw As DataRow
//                'Dim i As Integer
//                'dttbl = M_DataSet.Tables(0)
//                With insCommand
//                    .Connection = objWK.MyConnection
//                    .CommandType = CommandType.StoredProcedure
//                    .CommandText = objWK.MyUserName & ".PORT_MST_TBL_PKG.PORT_MST_TBL_INS"
//                    With .Parameters
//                        .Clear()

//                        insCommand.Parameters.Add("PORT_ID_IN", OracleClient.OracleDbType.Varchar2, 20, "PORT_ID").Direction = ParameterDirection.Input
//                        insCommand.Parameters["PORT_ID_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("PORT_NAME_IN", OracleClient.OracleDbType.Varchar2, 50, "PORT_NAME").Direction = ParameterDirection.Input
//                        insCommand.Parameters["PORT_NAME_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("PORT_TYPE_IN", OracleClient.OracleDbType.Varchar2, 3, "PORT_TYPE").Direction = ParameterDirection.Input
//                        insCommand.Parameters["PORT_TYPE_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("COUNTRY_MST_FK_IN", OracleClient.OracleDbType.Int32, 10, "COUNTRY_MST_FK").Direction = ParameterDirection.Input
//                        insCommand.Parameters["COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("ASSOCIATED_PORT_FK_IN", OracleClient.OracleDbType.Int32, 10, "ASSOCIATED_PORT_FK").Direction = ParameterDirection.Input
//                        insCommand.Parameters["ASSOCIATED_PORT_FK_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("TIME_ZONE_IN", OracleClient.OracleDbType.Varchar2, 6, "TIME_ZONE").Direction = ParameterDirection.Input
//                        insCommand.Parameters["TIME_ZONE_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("STATE_MST_FK_IN", OracleClient.OracleDbType.Int32, 10, "STATE_MST_FK").Direction = ParameterDirection.Input
//                        insCommand.Parameters["STATE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
//                        'insCommand.Parameters.Add("STATE_MST_FK_IN", System."").Direction = ParameterDirection.Input

//                        insCommand.Parameters.Add("STEAMING_IN_TIME_IN", OracleClient.OracleDbType.Int32, 6, "STEAMING_IN_TIME").Direction = ParameterDirection.Input
//                        insCommand.Parameters["STEAMING_IN_TIME_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("STEAMING_OUT_TIME_IN", OracleClient.OracleDbType.Int32, 6, "STEAMING_OUT_TIME").Direction = ParameterDirection.Input
//                        insCommand.Parameters["STEAMING_OUT_TIME_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("MAX_DRAFT_IN", OracleClient.OracleDbType.Int32, 10, "MAX_DRAFT").Direction = ParameterDirection.Input
//                        insCommand.Parameters["MAX_DRAFT_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("MAX_LOA_IN", OracleClient.OracleDbType.Int32, 10, "MAX_LOA").Direction = ParameterDirection.Input
//                        insCommand.Parameters["MAX_LOA_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("NO_OF_BERTHS_IN", OracleClient.OracleDbType.Int32, 5, "NO_OF_BERTHS").Direction = ParameterDirection.Input
//                        insCommand.Parameters["NO_OF_BERTHS_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("NO_OF_GANTRIES_IN", OracleClient.OracleDbType.Int32, 5, "NO_OF_GANTRIES").Direction = ParameterDirection.Input
//                        insCommand.Parameters["NO_OF_GANTRIES_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("NO_OF_MOVES_HOUR_IN", OracleClient.OracleDbType.Int32, 6, "NO_OF_MOVES_HOUR").Direction = ParameterDirection.Input
//                        insCommand.Parameters["NO_OF_MOVES_HOUR_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("ACTIVE_IN", OracleClient.OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input
//                        insCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current
//                        insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input
//                        insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
//                        insCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Int32, 10, "Port_MST_PK").Direction = ParameterDirection.Output
//                        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current

//                    End With
//                End With
//                With delCommand
//                    .Connection = objWK.MyConnection
//                    .CommandType = CommandType.StoredProcedure
//                    .CommandText = objWK.MyUserName & ".PORT_MST_TBL_PKG.PORT_MST_TBL_DEL"
//                    With .Parameters
//                        .Clear()
//                        delCommand.Parameters.Add("PORT_MST_PK_IN", OracleClient.OracleDbType.Int32, 10, "Port_MST_PK").Direction = ParameterDirection.Input
//                        delCommand.Parameters["PORT_MST_PK_IN"].SourceVersion = DataRowVersion.Current
//                        delCommand.Parameters.Add("DELETED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input
//                        delCommand.Parameters.Add("Version_No_IN", OracleClient.OracleDbType.Int32, 4, "Version_No").Direction = ParameterDirection.Input
//                        delCommand.Parameters["Version_No_IN"].SourceVersion = DataRowVersion.Current
//                        delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
//                        delCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
//                        delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
//                    End With
//                End With
//                With updCommand
//                    .Connection = objWK.MyConnection

//                    .CommandType = CommandType.StoredProcedure
//                    .CommandText = objWK.MyUserName & ".PORT_MST_TBL_PKG.PORT_MST_TBL_UPD"
//                    With .Parameters
//                        .Clear()
//                        updCommand.Parameters.Add("PORT_MST_PK_IN", OracleClient.OracleDbType.Int32, 10, "PORT_MST_PK").Direction = ParameterDirection.Input
//                        updCommand.Parameters["PORT_MST_PK_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("PORT_ID_IN", OracleClient.OracleDbType.Varchar2, 20, "PORT_ID").Direction = ParameterDirection.Input
//                        updCommand.Parameters["PORT_ID_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("PORT_NAME_IN", OracleClient.OracleDbType.Varchar2, 50, "PORT_NAME").Direction = ParameterDirection.Input
//                        updCommand.Parameters["PORT_NAME_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("PORT_TYPE_IN", OracleClient.OracleDbType.Varchar2, 3, "PORT_TYPE").Direction = ParameterDirection.Input
//                        updCommand.Parameters["PORT_TYPE_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("COUNTRY_MST_FK_IN", OracleClient.OracleDbType.Int32, 10, "COUNTRY_MST_FK").Direction = ParameterDirection.Input
//                        updCommand.Parameters["COUNTRY_MST_FK_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("ASSOCIATED_PORT_FK_IN", OracleClient.OracleDbType.Int32, 10, "ASSOCIATED_PORT_FK").Direction = ParameterDirection.Input
//                        updCommand.Parameters["ASSOCIATED_PORT_FK_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("TIME_ZONE_IN", OracleClient.OracleDbType.Varchar2, 6, "TIME_ZONE").Direction = ParameterDirection.Input
//                        updCommand.Parameters["TIME_ZONE_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("STATE_MST_FK_IN", OracleClient.OracleDbType.Int32, 10, "STATE_MST_FK").Direction = ParameterDirection.Input
//                        updCommand.Parameters["STATE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
//                        '.Add("STATE_MST_FK_IN", System."").Direction = ParameterDirection.Input
//                        updCommand.Parameters.Add("STEAMING_IN_TIME_IN", OracleClient.OracleDbType.Int32, 6, "STEAMING_IN_TIME").Direction = ParameterDirection.Input
//                        updCommand.Parameters["STEAMING_IN_TIME_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("STEAMING_OUT_TIME_IN", OracleClient.OracleDbType.Int32, 6, "STEAMING_OUT_TIME").Direction = ParameterDirection.Input
//                        updCommand.Parameters["STEAMING_OUT_TIME_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("MAX_DRAFT_IN", OracleClient.OracleDbType.Int32, 10, "MAX_DRAFT").Direction = ParameterDirection.Input
//                        updCommand.Parameters["MAX_DRAFT_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("MAX_LOA_IN", OracleClient.OracleDbType.Int32, 10, "MAX_LOA").Direction = ParameterDirection.Input
//                        updCommand.Parameters["MAX_LOA_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("NO_OF_BERTHS_IN", OracleClient.OracleDbType.Int32, 5, "NO_OF_BERTHS").Direction = ParameterDirection.Input
//                        updCommand.Parameters["NO_OF_BERTHS_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("NO_OF_GANTRIES_IN", OracleClient.OracleDbType.Int32, 5, "NO_OF_GANTRIES").Direction = ParameterDirection.Input
//                        updCommand.Parameters["NO_OF_GANTRIES_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("NO_OF_MOVES_HOUR_IN", OracleClient.OracleDbType.Int32, 6, "NO_OF_MOVES_HOUR").Direction = ParameterDirection.Input
//                        updCommand.Parameters["NO_OF_MOVES_HOUR_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("ACTIVE_IN", OracleClient.OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input
//                        updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input
//                        updCommand.Parameters.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input
//                        updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current
//                        updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
//                        updCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
//                        updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current

//                    End With
//                End With
//                ' AddHandler objWK.MyDataAdapter.RowUpdating, New OracleRowUpdatingEventHandler(AddressOf OnRowUpdating)
//                AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)

//                'objWK.MyDataAdapter.ContinueUpdateOnError = True
//                With objWK.MyDataAdapter

//                    .InsertCommand = insCommand
//                    .InsertCommand.Transaction = TRAN
//                    .UpdateCommand = updCommand
//                    .UpdateCommand.Transaction = TRAN
//                    .DeleteCommand = delCommand
//                    .DeleteCommand.Transaction = TRAN
//                    RecAfct = .Update(M_DataSet)
//                    If arrMessage.Count > 0 Then
//                        TRAN.Rollback()
//                        Return arrMessage
//                    Else

//                        lngPkVal = insCommand.Parameters["RETURN_VALUE"].Value

//                        objPortCost.Save(ContainerDataSet, M_DataSet.Tables(0).Rows(0)("Port_mst_pk"), CREATED_BY, LAST_MODIFIED_BY)
//                        TRAN.Commit()
//                        arrMessage.Add("All Data Saved Successfully")
//                        Return arrMessage
//                    End If

//                End With
//            Catch oraexp As OracleException
//                TRAN.Rollback()
//                Throw oraexp
//            Catch ex As Exception
//                TRAN.Rollback()
//                Throw ex
//            End Try
//        End Function

//        Public Function DelSave(ByRef M_DataSet As DataSet, Optional ByVal DelFlg As Boolean = False) As Integer
//            Dim objWK As New WorkFlow
//            Dim ColPara As New OracleClient.OracleParameterCollection
//            Dim oraTran As OracleTransaction
//            Dim intPKVal As Integer
//            Dim lngI As Long
//            Dim RecAfct As Int32
//            Dim insCommand As New OracleClient.OracleCommand
//            Dim updCommand As New OracleClient.OracleCommand
//            Dim delCommand As New OracleClient.OracleCommand
//            Dim i As Int16
//            Try

//                objWK.OpenConnection()

//                oraTran = objWK.MyConnection.BeginTransaction()
//                objWK.MyCommand.Transaction = oraTran
//                For i = 0 To M_DataSet.Tables(0).Rows.Count - 1
//                    If M_DataSet.Tables(0).Rows(i)("DELFLAG") = True Then

//                        With objWK.MyCommand.Parameters
//                            .Add("PORT_MST_PK_IN", M_DataSet.Tables(0).Rows(i)("Port_MST_PK")).Direction = ParameterDirection.Input
//                            .Add("DELETED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input
//                            .Add("Version_No_IN", M_DataSet.Tables(0).Rows(i)("Version_No")).Direction = ParameterDirection.Input
//                            .Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
//                            .Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
//                        End With
//                        'objWK.MyCommand.Parameters["PORT_MST_PK_IN"].SourceVersion = DataRowVersion.Original
//                        'objWK.MyCommand.Parameters["Version_No_IN"].SourceVersion = DataRowVersion.Current
//                        'objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
//                        objWK.MyCommand.CommandType = CommandType.StoredProcedure
//                        objWK.MyCommand.CommandText = objWK.MyUserName & ".PORT_MST_TBL_PKG.PORT_MST_TBL_DEL"
//                        If objWK.MyCommand.ExecuteNonQuery() = 1 Then
//                            'Return intPkVal

//                        Else
//                            Return -1
//                        End If
//                    End If

//                    ' AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)

//                    objWK.MyCommand.Parameters.Clear()
//                Next

//                oraTran.Commit()
//                Return 1
//            Catch ex As Exception

//                Dim a As String = ex.Message
//                Return -1
//                oraTran.Rollback()

//            Finally
//                objWK.MyConnection.Close()
//            End Try
//        End Function

//#End Region

//        'Sub OnRowUpdated(ByVal objsender As Object, ByVal e As OracleRowUpdatedEventArgs)
//        '    Try
//        '        If e.RecordsAffected < 1 Then
//        '            If e.Errors.Message <> "" Then
//        '                If e.StatementType = StatementType.Insert Then
//        '                    arrMessage.Add(CType(e.Row.Item(2), String) & "~" & e.Errors.Message)
//        '                Else
//        '                    arrMessage.Add(CType(e.Command.Parameters[0].Value, String) & "~" & e.Errors.Message)
//        '                End If
//        '            End If
//        '            e.Status = UpdateStatus.SkipCurrentRow
//        '        Else
//        '            If e.StatementType = StatementType.Insert Then
//        '                e.Row.Item(1) = e.Command.Parameters["RETURN_VALUE"].Value
//        '            End If
//        '        End If
//        '    Catch ex As Exception
//        '        Throw ex
//        '    End Try
//        'End Sub

//#Region "Fetch Port Based on Country"
//        Public Function FetchPortBasedOnCountry(Optional ByVal CountryPK As Int16 = 0, Optional ByVal CountryID As String = "", _
//            Optional ByVal CountryName As String = "") As DataSet
//            Dim strSQL As String
//            strSQL &= vbCrLf & " select ' ' Port_Id,"
//            strSQL &= vbCrLf & " ' ' Port_Name,"
//            strSQL &= vbCrLf & "0 Port_MST_PK from Dual "
//            strSQL &= vbCrLf & "Union"
//            strSQL = strSQL & " select port_id,"
//            strSQL &= vbCrLf & " Port_Name,"
//            strSQL &= vbCrLf & " Port_MST_PK "
//            strSQL &= vbCrLf & " from Port_MST_TBL port, COUNTRY_MST_TBL country "
//            strSQL &= vbCrLf & " Where 1=1"
//            strSQL &= vbCrLf & " AND port.COUNTRY_MST_FK=country.COUNTRY_MST_PK"
//            If CountryPK > 0 Then strSQL &= vbCrLf & " AND Country_Mst_Fk = " & CountryPK & " "
//            If CountryID.Trim.Length > 0 Then
//                strSQL &= vbCrLf & " AND Upper(country.Country_ID) Like '" & CountryID.ToUpper.Trim.Replace("'", "''") & "'"
//            End If
//            If CountryName.Trim.Length > 0 Then
//                strSQL &= vbCrLf & " AND Upper(country.Country_Name) Like '" & CountryName.ToString.Trim.Replace("'", "''")
//            End If
//            strSQL = strSQL & " order by 1"
//            Dim objWF As New Business.WorkFlow
//            Dim objDR As DataSet
//            Try
//                Return objWF.GetDataSet(strSQL)

//            Catch sqlExp As OracleException
//                ErrorMessage = sqlExp.Message
//                Throw sqlExp
//            Catch exp As Exception
//                ErrorMessage = exp.Message
//                Throw exp
//            End Try
//        End Function
//#End Region
//    End Class
//End Namespace

#endregion " Previous Code "