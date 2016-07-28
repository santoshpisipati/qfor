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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_activate_deactivate_custrpt : CommonFeatures
    {
        /// <summary>
        /// The ds main
        /// </summary>
        private DataSet DSMain = new DataSet();

        #region "Grid Function"

        /// <summary>
        /// Fetches the specified customer pk.
        /// </summary>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <param name="AgentPk">The agent pk.</param>
        /// <param name="CustCat">The customer cat.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="BizStatus">The biz status.</param>
        /// <param name="lngUserLocFk">The LNG user loc fk.</param>
        /// <param name="intStatus">The int status.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet Fetch(Int32 CustomerPK = 0, Int32 AgentPk = 0, string CustCat = "", string BizType = "", string BizStatus = "", string lngUserLocFk = "", string intStatus = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strSQL1 = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (CustomerPK > 0)
                {
                    strCondition = strCondition + " And CUSTOMER_MST_PK = " + CustomerPK;
                }
                if (Convert.ToInt32(CustCat) > 0)
                {
                    strCondition = strCondition + " And CUSTOMER_CATEGORY_MST_PK = " + CustCat;
                }
                if (!string.IsNullOrEmpty(BizType))
                {
                    strCondition = strCondition + " And CMT.BUSINESS_TYPE IN (" + BizType + ")";
                }
                if (!string.IsNullOrEmpty(BizStatus))
                {
                    strCondition = strCondition + " And CMT.TEMP_PARTY IN (" + BizStatus + ")";
                }
                if (!string.IsNullOrEmpty(intStatus))
                {
                    strCondition = strCondition + " And CMT.ACTIVE_FLAG in ( " + intStatus + ")";
                }
                if (!string.IsNullOrEmpty(lngUserLocFk))
                {
                    strCondition = strCondition + " And LMT.LOCATION_MST_PK IN (" + lngUserLocFk + ")";
                }
                if (AgentPk > 0)
                {
                    strCondition = strCondition + " And AGENT_MST_PK = " + AgentPk;
                }
                //strSQL = "SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM"
                strSQL += " SELECT CMT.CUSTOMER_ID, ";
                strSQL += " CMT.CUSTOMER_NAME,";
                strSQL += " cm.customer_category_id,";
                strSQL += " decode(cmt.temp_party,0,'Permanent',1,'Temporary') temp_party,";
                strSQL += " lmt.location_name location_id,";
                strSQL += " AG.AGENT_NAME AGENT_ID,";
                strSQL += "DECODE(CMT.BUSINESS_TYPE,'0','','1','Air','2','Sea','3','Both')BUSINESS_TYPE,";
                strSQL += "DECODE(CMT.ACTIVE_FLAG,'0','INACTIVE','1','ACTIVE','2','ALL')STATUS,";
                strSQL += " CCD.ADM_CONTACT_PERSON,";
                strSQL += " CCD.ADM_PHONE_NO_2,";
                strSQL += " CCD.ADM_EMAIL_ID";
                strSQL += " FROM CUSTOMER_MST_TBL      CMT,";
                strSQL += " CUSTOMER_CONTACT_DTLS CCD,";
                strSQL += " AGENT_MST_TBL         AG,";
                strSQL += " location_mst_tbl lmt,";
                strSQL += " CUSTOMER_CATEGORY_MST_TBL cm,";
                strSQL += " CUSTOMER_CATEGORY_TRN  CCT ";
                strSQL += " WHERE  CCD.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK";
                strSQL += "  AND  lmt.location_mst_pk = CCD.ADM_LOCATION_MST_FK ";
                strSQL += "  AND CMT.DP_AGENT_MST_FK = AG.AGENT_MST_PK(+)";
                strSQL += "  AND cmt.customer_mst_pk=cct.customer_mst_fk";
                strSQL += "  And cm.customer_category_mst_pk=cct.customer_category_mst_fk";
                strSQL += "  AND CMT.DP_AGENT_MST_FK = AG.AGENT_MST_PK(+)";
                //strSQL &= vbCrLf & "  AND CMT.TEMP_PARTY = 0"
                strSQL += strCondition;
                //strSQL &= vbCrLf & "order by CMT.ACTIVE_FLAG desc "
                //strSQL &= vbCrLf & ")q )"
                strSQL1 = " select count(*) from (";
                strSQL1 += strSQL + ")";

                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
                TotalPage = TotalRecords / RecordsPerPage;
                if (TotalRecords % RecordsPerPage != 0)
                {
                    TotalPage += 1;
                }
                if (CurrentPage > TotalPage)
                {
                    CurrentPage = 1;
                }
                if (TotalRecords == 0)
                {
                    CurrentPage = 0;
                }
                last = CurrentPage * RecordsPerPage;
                start = (CurrentPage - 1) * RecordsPerPage + 1;

                strSQL += " order by CMT.ACTIVE_FLAG desc";
                strSQL1 = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL1 += strSQL;
                strSQL1 += " )q ) WHERE SR_NO Between " + start + " and " + last;
                return objWF.GetDataSet(strSQL1);
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

        #endregion "Grid Function"

        #region "Report Function"

        /// <summary>
        /// Gets all.
        /// </summary>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <param name="AgentPk">The agent pk.</param>
        /// <param name="CustCat">The customer cat.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="BizStatus">The biz status.</param>
        /// <param name="lngUserLocFk">The LNG user loc fk.</param>
        /// <param name="intStatus">The int status.</param>
        /// <returns></returns>
        public DataSet GetAll(Int32 CustomerPK = 0, Int32 AgentPk = 0, string CustCat = "", string BizType = "", string BizStatus = "", string lngUserLocFk = "", string intStatus = "")
        {
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (CustomerPK > 0)
                {
                    strCondition = strCondition + " And CUSTOMER_MST_PK = " + CustomerPK;
                }
                if (Convert.ToInt32(CustCat) > 0)
                {
                    strCondition = strCondition + " And CUSTOMER_CATEGORY_MST_PK = " + CustCat;
                }
                strCondition = strCondition + " And CMT.BUSINESS_TYPE IN (" + BizType + ")";
                //If intStatus = 0 Or intStatus = 1 Then
                //    strCondition = strCondition & " And CMT.ACTIVE_FLAG = " & intStatus & vbCrLf
                //End If
                if (!string.IsNullOrEmpty(BizStatus))
                {
                    strCondition = strCondition + " And CMT.TEMP_PARTY IN (" + BizStatus + ")";
                }
                strCondition = strCondition + " And CMT.ACTIVE_FLAG in ( " + intStatus + ")";

                strCondition = strCondition + " And LMT.LOCATION_MST_PK IN (" + lngUserLocFk + ")";

                if (AgentPk > 0)
                {
                    strCondition = strCondition + " And AGENT_MST_PK = " + AgentPk;
                }
                strSQL = "SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM";
                strSQL += "(SELECT CMT.CUSTOMER_ID, ";
                strSQL += " CMT.CUSTOMER_NAME,";
                strSQL += " lmt.location_name location_id,";
                strSQL += " decode(cmt.temp_party,0,'Permanent',1,'Temporary') temp_party,";
                strSQL += " cm.customer_category_id,";
                strSQL += "DECODE(CMT.BUSINESS_TYPE,'0','','1','Air','2','Sea','3','Both')BUSINESS_TYPE,";
                strSQL += "DECODE(CMT.ACTIVE_FLAG,'0','INACTIVE','1','ACTIVE','2','ALL')STATUS,";
                strSQL += " CCD.ADM_CONTACT_PERSON,";
                strSQL += " CCD.ADM_PHONE_NO_2,";
                strSQL += " CCD.ADM_EMAIL_ID,";
                strSQL += " AG.AGENT_NAME AGENT_ID";
                strSQL += " FROM CUSTOMER_MST_TBL      CMT,";
                strSQL += " CUSTOMER_CONTACT_DTLS CCD,";
                strSQL += " AGENT_MST_TBL         AG,";
                strSQL += " location_mst_tbl lmt,";
                strSQL += " CUSTOMER_CATEGORY_MST_TBL cm,";
                strSQL += " CUSTOMER_CATEGORY_TRN  CCT ";
                strSQL += " WHERE  CCD.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK";
                strSQL += "  AND  lmt.location_mst_pk = CCD.ADM_LOCATION_MST_FK ";
                strSQL += "  AND CMT.DP_AGENT_MST_FK = AG.AGENT_MST_PK(+)";
                strSQL += "  AND cmt.customer_mst_pk=cct.customer_mst_fk";
                strSQL += "  And cm.customer_category_mst_pk=cct.customer_category_mst_fk";
                strSQL += "  AND CMT.DP_AGENT_MST_FK = AG.AGENT_MST_PK(+)";
                //strSQL &= vbCrLf & "  AND CMT.TEMP_PARTY = 0"
                strSQL += strCondition;
                //strSQL &= vbCrLf & "order by CMT.ACTIVE_FLAG desc "
                strSQL += ")q )";
                return objWF.GetDataSet(strSQL);
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

        #endregion "Report Function"
    }
}