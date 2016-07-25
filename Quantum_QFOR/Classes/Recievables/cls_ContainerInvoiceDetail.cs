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

using Oracle.DataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class ClsContainerInvoiceDetail : CommonFeatures
    {
        #region "Fetch Header"

        /// <summary>
        /// Fetches the header.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <returns></returns>
        public DataSet FetchHeader(int PK, int JobType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".JOBSPENDIN_DOCREC_PKG.CONTAINER_HEADER_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand.Parameters;

                _with2.Add("REF_PK_IN", PK).Direction = ParameterDirection.Input;
                _with2.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with2.Add("MAIN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Header"

        #region "Fetch dETAIL"

        /// <summary>
        /// Fetches the detail.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <returns></returns>
        public DataSet FetchDetail(int PK, int JobType)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with3 = objWK.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".JOBSPENDIN_DOCREC_PKG.CONTAINER_DETAIL_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with4 = objWK.MyCommand.Parameters;

                _with4.Add("REF_PK_IN", PK).Direction = ParameterDirection.Input;
                _with4.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with4.Add("MAIN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                return dsData;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch dETAIL"
    }
}