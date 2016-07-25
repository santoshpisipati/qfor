using Newtonsoft.Json;
using Oracle.DataAccess.Client;
using System;

namespace Quantum_QFOR
{
    public class clsLocation_Bank_Trn : CommonFeatures
    {
        #region "Fetch Function"

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