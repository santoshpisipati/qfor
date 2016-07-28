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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

#endregion "Comments"


namespace Quantum_QFOR
{
    public class ClsPortofCall : CommonFeatures
	{

		public DataSet FetchPortofCalls(string Voyagepk = "", string vesselname = "", string voyageid = "")
		{

			string strsql = null;
			WorkFlow objWF = new WorkFlow();
			strsql = "select";
			strsql += " voyage.vessel_voyage_tbl_pk,";
			strsql += " VOYAGEPORT.port_mst_tp_port_fk,";
			strsql += " port.port_id,";
			strsql += " port.port_name,";
			strsql += "  to_Char(VOYAGEPORT.POL_ETA ,'dd/MM/yyyy HH24:mi'),";
			strsql += "  to_Char(VOYAGEPORT.POL_ETD, 'dd/MM/yyyy HH24:mi'),";
			strsql += "  to_Char(VOYAGEPORT.POL_CUT_OFF_DATE, 'dd/MM/yyyy HH24:mi'),";
			strsql += " VOYAGEPORT.transit_days,";
			strsql += " 'true' as \"sel\",";
			strsql += " voyage.vessel_name,";
			strsql += " voytrn.voyage_trn_pk,";
			strsql += " voyageport.voyage_trn_fk";
			strsql += " from";
			strsql += " VESSEL_VOYAGE_TBL VOYAGE,";
			strsql += " VESSEL_VOYAGE_TRN VOYTRN,";
			strsql += " VESSEL_VOYAGE_TRN_TP VOYAGEPORT,";
			strsql += " PORT_MST_TBL PORT";
			strsql += " WHERE";
			strsql += " VOYTRN.VESSEL_VOYAGE_TBL_FK = VOYAGE.VESSEL_VOYAGE_TBL_PK";
			strsql += " AND VOYAGEPORT.VOYAGE_TRN_FK = VOYTRN.VOYAGE_TRN_PK";
			strsql += " AND VOYAGEPORT.PORT_MST_TP_PORT_FK = PORT.PORT_MST_PK";
			strsql += " AND VOYAGE.VESSEL_NAME = '" + vesselname + "'";
			strsql += " AND VOYTRN.VOYAGE = '" + voyageid + "'";
			strsql += " AND PORT.BUSINESS_TYPE = 2";
			strsql += " AND PORT.ACTIVE_FLAG =1 ";

			try {
				return objWF.GetDataSet(strsql);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}


		#region "Port"
		public DataSet FetchPort()
		{
			string strSQL = null;
			strSQL = "select ' ' PORT_ID,";
			strSQL = strSQL + " ' ' PORT_NAME, ";
			strSQL = strSQL + "0 PORT_MST_PK ";
			strSQL = strSQL + "from DUAL ";
			strSQL = strSQL + "UNION ";
			strSQL = strSQL + "Select PORT_ID, ";
			strSQL = strSQL + "PORT_NAME,";
			strSQL = strSQL + "PORT_MST_PK ";
			strSQL = strSQL + "from PORT_mst_tbl Where 1=1 and active_flag=1  and  port.business_type=2";
			strSQL = strSQL + " order by PORT_ID";
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion


	}
}
