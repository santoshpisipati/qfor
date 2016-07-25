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
using System.Data;

namespace Quantum_QFOR
{
    public class clsLocation_Working_Ports_Trn : CommonFeatures
    {
        public string FetchAll(Int64 P_Location_Mst_Fk = 0, Int64 P_Location_Working_Ports_Pk = 0, Int64 P_Port_Mst_Fk = 0, string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ";
            strSQL = strSQL + " Location_Mst_Fk,";
            strSQL = strSQL + " Port_Mst_Fk,";
            strSQL = strSQL + " Active ";
            strSQL = strSQL + " FROM LOCATION_WORKING_PORTS_TRN loc";
            strSQL = strSQL + " where (1=1) ";
            if (P_Location_Mst_Fk != 0)
            {
                strSQL = strSQL + " And loc.Location_Mst_Fk =" + P_Location_Mst_Fk;
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

        public string FetchAllLocPortMap()
        {
            string strSQL = null;
            strSQL = "SELECT ";
            strSQL = strSQL + " lpm.location_mst_fk,";
            strSQL = strSQL + " lpm.port_mst_fk";
            strSQL = strSQL + " loc_port_mapping_trn lpm";
            strSQL = strSQL + " where (1=2) ";
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

        public string GetWorkingPortForLoc(Int64 P_Location_Mst_Fk, bool ActivePort = false, int Btype = 3)
        {
            string strSQL = null;
            string strCondition = null;
            if (Btype != 3)
            {
                strCondition = "AND Prt.BUSINESS_TYPE = " + Btype;
            }
            strSQL = string.Empty;
            strSQL += "(SELECT  ";
            strSQL += "wrk.location_working_ports_pk Working_Port_FK,  ";
            strSQL += "Prt.Port_Mst_Pk,  ";
            strSQL += "Prt.Port_Id,  ";
            strSQL += "Prt.Port_Name Port_Name,  ";
            strSQL += "DECODE(prt.port_type,0,'AIR',1,'ICD',2,'SEA') Port_type,  ";
            strSQL += "Cntry.Country_Name Country_Name,  ";
            strSQL += "'1' StatFlg,  ";
            strSQL += "to_char(Wrk.Active) Active,  ";
            strSQL += "wrk.version_no  ";
            strSQL += "FROM  ";
            strSQL += "Port_Mst_Tbl Prt,  ";
            strSQL += "Location_Working_Ports_Trn Wrk,  ";
            strSQL += "Country_Mst_Tbl Cntry  ";
            strSQL += "WHERE  ";
            strSQL += "Wrk.Port_Mst_Fk = Prt.Port_Mst_Pk  ";
            strSQL += "AND Prt.Country_Mst_Fk = Cntry.Country_Mst_Pk  ";
            strSQL += "AND Wrk.Location_Mst_Fk = " + P_Location_Mst_Fk + "  ";
            if (ActivePort)
            {
                strSQL += "AND prt.Active_Flag=1    ";
            }
            strSQL += strCondition;
            strSQL += "  ";
            strSQL += "UNION  ";
            strSQL += "  ";
            strSQL += "SELECT  ";
            strSQL += "0 Working_Port_FK,   ";
            strSQL += "Prt.Port_Mst_Pk,  ";
            strSQL += "Prt.Port_Id,  ";
            strSQL += "Prt.Port_Name Port_Name,  ";
            strSQL += "DECODE(prt.port_type,0,'AIR',1,'ICD',2,'SEA') Port_type,  ";
            strSQL += "Cntry.Country_Name Country_Name,  ";
            strSQL += "'0' StatFlg,  ";
            strSQL += "'0' Active,  ";
            strSQL += "0 Version_no  ";
            strSQL += "FROM   ";
            strSQL += "Port_mst_tbl Prt,  ";
            strSQL += "Country_Mst_Tbl Cntry  ";
            strSQL += "WHERE  ";
            strSQL += "Prt.Country_Mst_Fk = cntry.country_mst_pk  ";
            if (ActivePort)
            {
                strSQL += "AND prt.Active_Flag=1    ";
            }
            strSQL += strCondition;
            strSQL += "AND   ";
            strSQL += "Prt.Port_Mst_Pk NOT IN (  ";
            strSQL += "SELECT   ";
            strSQL += "LocWrk.Port_Mst_Fk  ";
            strSQL += "FROM  ";
            strSQL += "Location_Working_Ports_Trn LocWrk  ";
            strSQL += "WHERE  ";
            strSQL += "LocWrk.Location_Mst_Fk =" + P_Location_Mst_Fk + " ))  ";
            strSQL += "ORDER BY StatFlg desc,Port_id   ";
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
    }
}