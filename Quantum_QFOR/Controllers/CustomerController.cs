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
using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using System.Globalization;
using System.Web.Http;

namespace Quantum_QFOR.Controllers
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="System.Web.Http.ApiController" />
    public class CustomerController : ApiController
    {
        
       

        /// <summary>
        /// Fills the currency.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns></returns>
        [AcceptVerbs("GET", "POST")]
        [HttpGet]
        public object FillCurrency(long id)
        {
            string json = string.Empty;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT C.CURRENCY_MST_PK,C.CURRENCY_ID,C.CURRENCY_NAME FROM CURRENCY_TYPE_MST_TBL C  WHERE C.ACTIVE_FLAG =1 ORDER BY C.CURRENCY_NAME";
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                string value = JsonConvert.SerializeObject(objDS, Formatting.Indented);
                return JsonConvert.DeserializeObject(value);
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

        /// <summary>
        /// Fetches the setup DTLS.
        /// </summary>
        /// <returns></returns>
        public object FetchSetupDtls()
        {
            string json = string.Empty;
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("CURR_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("EXCHANGE_SCHEDULER_SETUP_PKG", "GET_SHD_SETUP_DTLS");
                string value = JsonConvert.SerializeObject(ds, Formatting.Indented);
                return JsonConvert.DeserializeObject(value);
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the setup DTLS.
        /// </summary>
        /// <returns></returns>
        public object FetchCurrency()
        {
            string json = string.Empty;
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                cls_Corporate_Mst_Tbl cs = new cls_Corporate_Mst_Tbl();
                string value = cs.FetchCurrency();
                return JsonConvert.DeserializeObject(value);
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fecthes the ex rateset up.
        /// </summary>
        private void FecthExRatesetUp()
        {
            DataSet GridDS = new DataSet();
            DataSet SetUpDS = new DataSet();
            CultureInfo MyCultureInfo = new CultureInfo("de-DE");
            try
            {
            }
            catch (Exception ex)
            {
            }
        }

        // GET: api/Customer/SaveCustomerProfile()
        /// <summary>
        /// Saves the customer profile.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> SaveCustomerProfile()
        {
            return new string[] { "AdminstratorSetup", "value2" };
        }
    }
}