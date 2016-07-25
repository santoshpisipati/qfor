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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsDEPT_DESIG_TRN : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ deptdesi g_ tr n_ pk
        /// </summary>
        private Int64 M_DEPTDESIG_TRN_PK;

        /// <summary>
        /// The m_ dep t_ ms t_ fk
        /// </summary>
        private Int64 M_DEPT_MST_FK;

        /// <summary>
        /// The m_ desi g_ ms t_ fk
        /// </summary>
        private Int64 M_DESIG_MST_FK;

        /// <summary>
        /// The m_ active
        /// </summary>
        private Int64 M_ACTIVE;

        #endregion "List of Members of the Class"

        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_Dept_Desig_Trn_Pk">The p_ dept_ desig_ TRN_ pk.</param>
        /// <param name="P_Depart_Mst_Fk">The p_ depart_ MST_ fk.</param>
        /// <param name="P_Designation_Mst_Fk">The p_ designation_ MST_ fk.</param>
        /// <param name="P_Active">The p_ active.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int64 P_Dept_Desig_Trn_Pk, Int64 P_Depart_Mst_Fk, Int64 P_Designation_Mst_Fk, Int64 P_Active, string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ROWNUM SR_NO, ";
            strSQL = strSQL + " Dept_Desig_Trn_Pk,";
            strSQL = strSQL + " Depart_Mst_Fk,";
            strSQL = strSQL + " Designation_Mst_Fk,";
            strSQL = strSQL + " Active,";
            strSQL = strSQL + " Created_By_Fk,";
            strSQL = strSQL + " Created_Dt,";
            strSQL = strSQL + " Last_Modified_By_Fk,";
            strSQL = strSQL + " Last_Modified_Dt,";
            strSQL = strSQL + " Version_No ";
            strSQL = strSQL + " FROM DEPT_DESIG_TRN ";
            strSQL = strSQL + " WHERE ( 1 = 1) ";
            if (P_Dept_Desig_Trn_Pk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Dept_Desig_Trn_Pk like '%" + P_Dept_Desig_Trn_Pk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Dept_Desig_Trn_Pk like '" + P_Dept_Desig_Trn_Pk + "%' ";
                }
            }
            else
            {
            }
            if (P_Depart_Mst_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Depart_Mst_Fk like '%" + P_Depart_Mst_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Depart_Mst_Fk like '" + P_Depart_Mst_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Designation_Mst_Fk.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Designation_Mst_Fk like '%" + P_Designation_Mst_Fk + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Designation_Mst_Fk like '" + P_Designation_Mst_Fk + "%' ";
                }
            }
            else
            {
            }
            if (P_Active.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Active like '%" + P_Active + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Active like '" + P_Active + "%' ";
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

        #endregion "Fetch Function"

        #region "Insert Function"

        /// <summary>
        /// Inserts the specified dept MSTFK.
        /// </summary>
        /// <param name="DeptMSTFK">The dept MSTFK.</param>
        /// <param name="DesigMSTFK">The desig MSTFK.</param>
        /// <param name="Active">The active.</param>
        /// <param name="CreatedByFK">The created by fk.</param>
        /// <returns></returns>
        public int Insert(Int32 DeptMSTFK, Int32 DesigMSTFK, Int32 Active, Int32 CreatedByFK)
        {
            WorkFlow objWK = new WorkFlow();
            int intPKVal = 0;
            objWK.MyCommand.CommandType = CommandType.StoredProcedure;

            var _with1 = objWK.MyCommand.Parameters;
            _with1.Add("DEPART_MST_FK_IN", DeptMSTFK).Direction = ParameterDirection.Input;
            _with1.Add("DESIGNATION_MST_FK_IN", DesigMSTFK).Direction = ParameterDirection.Input;
            _with1.Add("ACTIVE_IN", Active).Direction = ParameterDirection.Input;
            _with1.Add("CREATED_BY_FK_IN", CreatedByFK).Direction = ParameterDirection.Input;
            _with1.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
            objWK.MyCommand.CommandText = objWK.MyUserName + ".DEPT_DESIG_TRN_PKG.DEPT_DESIG_TRN_INS";
            try
            {
                if (objWK.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return -1;
            }
        }

        #endregion "Insert Function"

        #region "Update Function"

        /// <summary>
        /// Updates the specified dept desig TRNPK.
        /// </summary>
        /// <param name="DeptDesigTRNPK">The dept desig TRNPK.</param>
        /// <param name="DeptMSTFK">The dept MSTFK.</param>
        /// <param name="DesigMSTFK">The desig MSTFK.</param>
        /// <param name="Active">The active.</param>
        /// <param name="intVer">The int ver.</param>
        /// <returns></returns>
        public int Update(Int32 DeptDesigTRNPK, Int32 DeptMSTFK, Int32 DesigMSTFK, Int32 Active, Int32 intVer)
        {
            WorkFlow objWK = new WorkFlow();
            int intPKVal = 0;
            try
            {
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with2 = objWK.MyCommand.Parameters;
                _with2.Add("DEPT_DESIG_TRN_PK_IN", DeptDesigTRNPK).Direction = ParameterDirection.Input;
                _with2.Add("DEPART_MST_FK_IN", DeptMSTFK).Direction = ParameterDirection.Input;
                _with2.Add("DESIGNATION_MST_FK_IN", DesigMSTFK).Direction = ParameterDirection.Input;
                _with2.Add("ACTIVE_IN", Active).Direction = ParameterDirection.Input;
                _with2.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("VERSION_NO_IN", intVer).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;

                objWK.MyCommand.CommandText = objWK.MyUserName + ".DEPT_DESIG_TRN_PKG.DEPT_DESIG_TRN_UPD";

                if (objWK.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #endregion "Update Function"

        #region "Delete Function"

        /// <summary>
        /// Deletes this instance.
        /// </summary>
        /// <returns></returns>
        public int Delete()
        {
            WorkFlow objWK = new WorkFlow();
            int intPKVal = 0;
            try
            {
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with3 = objWK.MyCommand.Parameters;

                _with3.Add("DEPT_DESIG_TRN_PK_IN", M_DEPTDESIG_TRN_PK).Direction = ParameterDirection.Input;
                _with3.Add("VERSION_NO_IN", M_VERSION_NO).Direction = ParameterDirection.Input;
                _with3.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", intPKVal).Direction = ParameterDirection.Output;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".DEPT_DESIG_TRN_PKG.DEPT_DESIG_TRN_DEL";
                if (objWK.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #endregion "Delete Function"

        #region "Fetch Designation"

        /// <summary>
        /// Fetches the design TRN.
        /// </summary>
        /// <param name="DesignTRNPK">The design TRNPK.</param>
        /// <returns></returns>
        public DataSet FetchDesignTrn(long DesignTRNPK)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT Count(*) from DEPT_DESIG_TRN DD,";
            strSQL += "  DESIGNATION_MST_TBL DG";
            strSQL += "WHERE ";
            strSQL += "DD.DESIGNATION_MST_FK(+)=DG.DESIGNATION_MST_PK\t";
            strSQL += "AND  DD.DEPART_MST_FK (+)=" + DesignTRNPK;
            strSQL = " select * from ( ";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += "DD.DEPT_DESIG_TRN_PK,";
            strSQL += "DD.DEPART_MST_FK,";
            strSQL += " DG.DESIGNATION_MST_PK,";
            strSQL += " nvl(DD.ACTIVE,0),";
            strSQL += "DG.DESIGNATION_ID,";
            strSQL += " DG.DESIGNATION_NAME,";
            strSQL += "DD.VERSION_NO";
            strSQL += "FROM";
            strSQL += "DEPT_DESIG_TRN DD,DESIGNATION_MST_TBL DG";
            strSQL += "WHERE ";
            strSQL += "DD.DESIGNATION_MST_FK(+)=DG.DESIGNATION_MST_PK\t";
            strSQL += "AND DD.DEPART_MST_FK (+)=" + DesignTRNPK;
            strSQL += "order by nvl(DD.ACTIVE,0) desc,dg.designation_id)q)";
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

        #endregion "Fetch Designation"

        #region "Fetch Department"

        /// <summary>
        /// Fetches the department.
        /// </summary>
        /// <param name="DesigTrnPK">The desig TRN pk.</param>
        /// <returns></returns>
        public DataSet FetchDepartment(long DesigTrnPK = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT";
            strSQL += " Department_ID, ";
            strSQL += " Department_NAME,";
            strSQL += " Department_MST_PK ";
            strSQL += " FROM Department_MST_TBL ";
            if (DesigTrnPK > 0)
            {
                strSQL += "WHERE Department_MST_PK=" + DesigTrnPK;
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

        #endregion "Fetch Department"
    }
}