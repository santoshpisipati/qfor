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

using Newtonsoft.Json;
using Oracle.DataAccess.Client;
using System;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Location_Bank_Trn : CommonFeatures
    {
        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_Location_Mst_Fk">The p_ location_ MST_ fk.</param>
        /// <param name="P_Location_Bank_Pk">The p_ location_ bank_ pk.</param>
        /// <param name="P_Bank_Mst_Fk">The p_ bank_ MST_ fk.</param>
        /// <param name="P_Active">The p_ active.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <returns></returns>
        public string FetchAll(Int64 P_Location_Mst_Fk = 0, Int64 P_Location_Bank_Pk = 0, Int64 P_Bank_Mst_Fk = 0, Int64 P_Active = 0, string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ";
            strSQL = strSQL + " Location_Mst_Fk,";
            strSQL = strSQL + " Bank_Mst_Fk,";
            strSQL = strSQL + " LOCATION_BANK_PK,";
            strSQL = strSQL + " Account_no, ";
            strSQL = strSQL + " swift_code, ";
            strSQL = strSQL + " Bank_gero_no, ";
            strSQL = strSQL + " IBAN, ";
            strSQL = strSQL + " BRANCH, ";
            strSQL = strSQL + " E_BANK_CODE, ";
            strSQL = strSQL + " LOC_NAME, ";
            strSQL = strSQL + " COUNTRY, ";
            strSQL = strSQL + " BANK_NAME, ";
            strSQL = strSQL + " VERSION_NO, ";
            strSQL = strSQL + " BANK_ADDRESS ";
            strSQL = strSQL + " FROM LOCATION_BANK_TRN loc ";
            strSQL = strSQL + " where (1=1) ";
            if (P_Location_Mst_Fk != 0)
            {
                strSQL = strSQL + " And Location_Mst_Fk =" + P_Location_Mst_Fk;
            }
            WorkFlow objWF = new WorkFlow();
            try
            {
                return JsonConvert.SerializeObject(objWF.GetDataSet(strSQL), Formatting.Indented);
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

        #region "Fetch Function for Bank"

        /// <summary>
        /// Gets the working bank.
        /// </summary>
        /// <param name="P_Location_Mst_Fk">The p_ location_ MST_ fk.</param>
        /// <returns></returns>
        public string GetWorkingBank(long P_Location_Mst_Fk)
        {
            string Strsql = null;
            Strsql = string.Empty;
            Strsql += "(SELECT ";
            Strsql += "LOCBANK.LOCATION_BANK_PK LOCBANKPK, ";
            Strsql += "LOCBANK.VERSION_NO VERNO, ";
            Strsql += "LOCBANK.ACCOUNT_NO, ";
            Strsql += "LOCBANK.SWIFT_CODE, ";
            Strsql += "LOCBANK.BANK_GERO_NO, ";
            Strsql += "LOCBANK.IBAN,";
            Strsql += "LOCBANK.BRANCH,";
            Strsql += "LOCBANK.E_BANK_CODE,";
            Strsql += "LOCBANK.LOC_NAME,";
            Strsql += "LOCBANK.COUNTRY,";
            Strsql += "LOCBANK.BANK_NAME,";
            Strsql += "LOCBANK.BANK_ADDRESS";
            Strsql += "FROM ";
            Strsql += "LOCATION_BANK_TRN LOCBANK ";
            Strsql += "WHERE  ";
            Strsql += "LOCBANK.Location_Mst_Fk = " + P_Location_Mst_Fk + ")  ";

            Strsql += " ";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return JsonConvert.SerializeObject(objWF.GetDataSet(Strsql), Formatting.Indented);
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

        #endregion "Fetch Function for Bank"
    }
}