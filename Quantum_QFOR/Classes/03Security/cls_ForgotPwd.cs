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
using System.Data;
using System.Text;

//Imports System.Data.OracleClient
namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_ForgotPwd : CommonFeatures
    {
        #region "FetchAll Function"

        /// <summary>
        /// Fetches the identifier password.
        /// </summary>
        /// <param name="UserID">The user identifier.</param>
        /// <returns></returns>
        public DataSet FetchIdPwd(string UserID = "")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            System.Text.StringBuilder strSQLNew1 = new System.Text.StringBuilder();
            System.Text.StringBuilder strSQLNew2 = new System.Text.StringBuilder();
            Int32 strUsrType = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strSQLNew1.Append(" SELECT USER_TYPE FROM USER_MST_TBL where  UPPER(USER_ID)='" + UserID + "'");

            strSQLNew2.Append("SELECT * FROM (SELECT CCD.ADM_EMAIL_ID as EmailID,decoder(VUM.PASS_WORD) as Password, UMT.USER_NAME as UserName FROM customer_contact_dtls CCD,customer_mst_tbl CMT,USER_MST_TBL UMT,VIEW_USER_MST_TBL VUM ");
            //Added By Anand For Fetching Customer Email Id and password

            strSQLNew2.Append(" WHERE UMT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK AND 1=1 and CCD.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK AND VUM.USER_MST_PK = UMT.USER_MST_PK AND UMT.IS_ACTIVATED=1  AND UPPER(US.USER_ID)='" + UserID + "' and vum.PASS_WARD_CHANGE_DT is not null ORDER BY VUM.PASS_WARD_CHANGE_DT DESC) q WHERE rownum =1");

            strSQL.Append(" SELECT * FROM (SELECT EE.EMAIL_ID as EmailID, decoder(VUM.PASS_WORD) as Password ,EE.EMPLOYEE_NAME as UserName FROM USER_MST_TBL US,EMPLOYEE_MST_TBL EE, VIEW_USER_MST_TBL VUM ");
            strSQL.Append(" WHERE US.EMPLOYEE_MST_FK=EE.EMPLOYEE_MST_PK AND VUM.USER_MST_PK = US.USER_MST_PK AND 1=1 AND US.IS_ACTIVATED=1 AND UPPER(US.USER_ID)='" + UserID + "' and vum.PASS_WARD_CHANGE_DT is not null ORDER BY VUM.PASS_WARD_CHANGE_DT DESC) q WHERE rownum =1");
            try
            {
                strUsrType = Convert.ToInt32(objWF.ExecuteScaler(strSQLNew1.ToString()));
                if (strUsrType == 0)
                {
                    return objWF.GetDataSet(strSQL.ToString());
                }
                else
                {
                    return objWF.GetDataSet(strSQLNew2.ToString());
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

        #endregion "FetchAll Function"

        #region " Fetch Super User Details"

        /// <summary>
        /// Fetches the super admin.
        /// </summary>
        /// <param name="SuperUser">The super user.</param>
        /// <returns></returns>
        public DataSet FetchSuperAdmin(string SuperUser)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSqlBuilder.Append(" select emt.email_id ");
            strSqlBuilder.Append(" from employee_mst_tbl emt ");
            strSqlBuilder.Append(" WHERE");
            strSqlBuilder.Append(" emt.super_user='" + SuperUser + "'");
            try
            {
                return objWF.GetDataSet(strSqlBuilder.ToString());
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion " Fetch Super User Details"

        #region "Update Wrong Password Count"

        /// <summary>
        /// Updates the wrong password count.
        /// </summary>
        /// <param name="strUserID">The string user identifier.</param>
        /// <returns></returns>
        public string UpdateWrongPasswordCount(string strUserID)
        {
            dynamic strSQL1 = null;
            WorkFlow objWF = new WorkFlow();
            strSQL1 = " UPDATE USER_MST_TBL UMT SET UMT.WRONG_PWD_COUNT = 0 WHERE UPPER(UMT.USER_ID) = '" + strUserID.ToUpper() + "'";
            objWF.ExecuteCommands(strSQL1);
            try
            {
                return objWF.ExecuteCommands(strSQL1.ToString);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Update Wrong Password Count"
    }
}