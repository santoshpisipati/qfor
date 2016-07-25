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
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"


using Newtonsoft.Json;
using Oracle.DataAccess.Client;
using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Qcore_ValidateLicense : CommonFeatures
    {
        /// <summary>
        /// FN_CHKs the registration code.
        /// </summary>
        /// <returns></returns>
        public string fn_chkRegistrationCode()
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            try
            {
                sb.Append("SELECT COUNT(*) ");
                sb.Append("  FROM QCOR_GEN_M_CLIENT_LICENSE CL");
                sb.Append(" WHERE CL.CORP_MST_FK =");
                sb.Append("       ( select c.corporate_mst_pk from corporate_mst_tbl c)");
                return JsonConvert.SerializeObject(objWF.ExecuteScaler(sb.ToString()), Formatting.Indented);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return ex.ToString();
            }
        }

        #region " Get Today's date from Oracle"
        public DateTime fn_GetTodayDate()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            DateTime dtime = new DateTime();
            strSQL = "select TO_CHAR(SYSDATE,'dd/MM/yyyy') from dual";
            try
            {
                return Convert.ToDateTime(objWF.ExecuteScaler(strSQL));
            }
            catch (OracleException sqlExp)
            {
                return dtime;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                return dtime;
                throw exp;
            }
        }
        #endregion

        /// <summary>
        /// FN_s the get corporate pk.
        /// </summary>
        /// <returns></returns>
        public Int64 fn_GetCorporatePK()
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("select c.corporate_mst_pk from corporate_mst_tbl c ");
                string value= objWF.ExecuteScaler(sb.ToString());
                return Convert.ToInt64(value);
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

        public string fn_GetRegistrationCode(long CorporatePK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT CL.REGISTRATION_CODE FROM  QCOR_GEN_M_CLIENT_LICENSE CL WHERE CL.corp_mst_fk = " + CorporatePK + "  ");
                return Convert.ToString(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "";
            }
        }

        public string DecryptString128Bit(string vstrStringToBeDecrypted, string vstrDecryptionKey)
        {
            byte[] bytDataToBeDecrypted = null;
            byte[] bytTemp = null;
            byte[] bytIV = { 121, 241, 10, 1, 132, 74, 11, 39, 255, 91, 45, 78, 14, 211, 22, 62 };
            RijndaelManaged objRijndaelManaged = new RijndaelManaged();
            MemoryStream objMemoryStream = default(MemoryStream);
            CryptoStream objCryptoStream = default(CryptoStream);
            byte[] bytDecryptionKey = null;
            string str_Decrypted = null;
            int intLength = 0;
            int intRemaining = 0;
            string strReturnString = string.Empty;

            bytDataToBeDecrypted = Convert.FromBase64String(vstrStringToBeDecrypted);

            intLength = vstrDecryptionKey.Length;

            if (intLength >= 32)
            {
                vstrDecryptionKey = string.Concat(vstrDecryptionKey, 32);
            }
            else
            {
                intLength = vstrDecryptionKey.Length;
                intRemaining = 32 - intLength;
                vstrDecryptionKey = vstrDecryptionKey + new String('X', intRemaining);
            }

            bytDecryptionKey = Encoding.ASCII.GetBytes(vstrDecryptionKey.ToCharArray());

            bytTemp = new byte[bytDataToBeDecrypted.Length + 1];

            objMemoryStream = new MemoryStream(bytDataToBeDecrypted);

            try
            {
                objCryptoStream = new CryptoStream(objMemoryStream, objRijndaelManaged.CreateDecryptor(bytDecryptionKey, bytIV), CryptoStreamMode.Read);

                objCryptoStream.Read(bytTemp, 0, bytTemp.Length);

                objCryptoStream.Flush();
                objMemoryStream.Close();
                objCryptoStream.Close();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            str_Decrypted = StripNullCharacters(Encoding.ASCII.GetString(bytTemp));
            return str_Decrypted;
        }

        public string StripNullCharacters(string vstrStringWithNulls)
        {
            int intPosition = 0;
            string strStringWithOutNulls = null;

            intPosition = 1;
            strStringWithOutNulls = vstrStringWithNulls;

            while (intPosition > 0)
            {
                //intPosition = string.Compare(intPosition, vstrStringWithNulls);

                if (intPosition > 0)
                {
                    //strStringWithOutNulls = strStringWithOutNulls.PadLeft(intPosition - 1) + strStringWithOutNulls.PadRight(strStringWithOutNulls.Length) - intPosition);
                }

                if (intPosition > strStringWithOutNulls.Length)
                {
                    break; // TODO: might not be correct. Was : Exit Do
                }
            }

            return strStringWithOutNulls;

        }


    }
}