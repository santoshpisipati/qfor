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
    public class cls_SelectActiveContainers : CommonFeatures
	{
		public DataTable ActiveContainers(string thcPK)
		{
			string Str = "";
			WorkFlow objWF = new WorkFlow();
			Str = string.Empty + "select";
			Str += "          CMT.CONTAINER_TYPE_MST_PK, ";
			Str += "          CMT.CONTAINER_TYPE_MST_ID, ";
			Str += "          (CASE WHEN CMT.CONTAINER_TYPE_MST_ID IN ";
			Str += "                (select  cmt.container_type_mst_id";
			Str += "                 from    port_thc_rates_trn thc,";
			Str += " PORT_THC_CONT_DTL CONT,";
			Str += "                         CONTAINER_TYPE_MST_TBL CMT";
			Str += "                         where 1=1 and";
			Str += "                         thc.thc_rates_mst_pk =" + thcPK;
			Str += "                         AND CONT.THC_RATES_MST_FK = thc.THC_RATES_MST_PK ";
			Str += "                         and cont.CONTAINER_TYPE_MST_FK=cmt.container_type_mst_pk)";
			Str += "          THEN '1' ELSE '0' END) CHK";
			Str += "FROM CONTAINER_TYPE_MST_TBL CMT";
			Str += "WHERE CMT.ACTIVE_FLAG=1   ";
			Str += "ORDER BY CHK DESC,CMT.PREFERENCES";
			try {
				return objWF.GetDataTable(Str);
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
	}
}
