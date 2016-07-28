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

namespace Quantum_QFOR
{
    public class Cls_MultiSelect : CommonFeatures
    {
        #region " Freight Element Function "

        public DataTable AllFreightElements()
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "Select ";
            strSQL += " FREIGHT_ELEMENT_ID, ";
            strSQL += " FREIGHT_ELEMENT_NAME, ";
            strSQL += " 0 SELECTED ";
            strSQL += " from ";
            strSQL += " FREIGHT_ELEMENT_MST_TBL ";
            strSQL += " where ";
            strSQL += " ACTIVE_FLAG = 1 ";
            strSQL += " order by ";
            strSQL += " FREIGHT_ELEMENT_ID ";
            try
            {
                return objWF.GetDataSet(strSQL).Tables[0];
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

        #endregion " Freight Element Function "

        #region "Active Containers List"

        public DataTable FetchActiveContainers()
        {
            WorkFlow objWF = new WorkFlow();
            objWF.OpenConnection();
            DataTable dtContainers = null;
            try
            {
                objWF.MyCommand = new OracleCommand();
                objWF.MyCommand.Parameters.Clear();
                objWF.MyCommand.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtContainers = objWF.GetDataTable("AGENCY_MST_TBL_PKG", "FETCH_ACTIVE_CONTAINERS");
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dtContainers;
        }

        public DataTable FetchFrtCntTrnForAgent(short ProcessType = 0, short AgentCntElemFk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            objWF.OpenConnection();
            DataTable dtContainers = null;
            try
            {
                objWF.MyCommand = new OracleCommand();
                objWF.MyCommand.Parameters.Clear();
                objWF.MyCommand.Parameters.Add("AGENT_CNT_ELEM_FK_IN", AgentCntElemFk).Direction = ParameterDirection.Input;
                objWF.MyCommand.Parameters.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                objWF.MyCommand.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtContainers = objWF.GetDataTable("AGENCY_MST_TBL_PKG", "FETCH_AGENT_FRT_CNT_TRN");
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dtContainers;
        }

        #endregion "Active Containers List"
    }
}