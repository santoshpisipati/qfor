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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsDetentionSlabListing : CommonFeatures
    {
        #region "Teriff"

        /// <summary>
        /// Fetches the teriff.
        /// </summary>
        /// <param name="teriffDate">The teriff date.</param>
        /// <param name="validFrom">The valid from.</param>
        /// <param name="validTo">The valid to.</param>
        /// <param name="depotPk">The depot pk.</param>
        /// <param name="cargoType">Type of the cargo.</param>
        /// <param name="teriffNo">The teriff no.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchTeriff(System.DateTime teriffDate, System.DateTime validFrom, System.DateTime validTo, int depotPk = 0, int cargoType = 1, string teriffNo = "", bool ActiveOnly = true, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, bool blnSortAscending = false, long lngUsrLocFk = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (strColumnName == "VENDOR_NAME")
            {
                strColumnName += " ,TARIFF_REF_NO";
            }

            if (teriffDate != null)
            {
                strCondition = strCondition + " and TO_DATE(ds.TARIFF_DATE,'" + dateFormat + "') = TO_date('" + teriffDate.Date + "','" + dateFormat + "')";
            }

            //If validFrom = Nothing And validTo = Nothing Then
            //    strCondition &= vbCrLf & " AND (TO_DATE(TO_CHAR(sysdate,'" & dateFormat & "'),'" & dateFormat & "')  between ds.valid_from and nvl(ds.valid_to,NULL_DATE_FORMAT) OR ds.valid_from  >= TO_DATE(TO_CHAR(sysdate,'" & dateFormat & "'),'" & dateFormat & "')) " & vbCrLf
            //End If

            //'Selected date else nothing, so no check on date
            //If Not validFrom = Nothing Then
            //    strCondition &= vbCrLf & " AND (TO_DATE('" & validFrom & "','" & dateFormat & "')  between ds.valid_from and nvl(ds.valid_to,NULL_DATE_FORMAT) OR ds.valid_from  >= TO_DATE('" & validFrom & "','" & dateFormat & "'))    " & vbCrLf
            //End If

            //'Selected date else nothing, so no check on date
            //If Not validTo = Nothing Then
            //    strCondition &= vbCrLf & " AND ds.VALID_TO  <= TO_DATE('" & validTo & "','" & dateFormat & "') " & vbCrLf
            //End If
            //Modified by Snigdharani - 13/11/2008, 12/12/2008
            // VALIDITY
            //Please do not modify the code without consulting QA or Domain
            if (validFrom != null & validTo != null)
            {
                strCondition += " AND ((TO_DATE('" + validTo + "' , '" + dateFormat + "') BETWEEN ";
                strCondition += "     ds.VALID_FROM AND ds.VALID_TO) OR ";
                strCondition += "     (TO_DATE('" + validFrom + "' , '" + dateFormat + "') BETWEEN ";
                strCondition += "     ds.VALID_FROM AND ds.VALID_TO) OR ";
                strCondition += "     (ds.VALID_TO IS NULL))";
            }
            else if (validTo != null & !(validFrom != null))
            {
                strCondition += "  AND ( ";
                strCondition += "         ds.VALID_TO <= TO_DATE('" + validTo + "' , '" + dateFormat + "') ";
                strCondition += "        OR ds.VALID_TO IS NULL ";
                strCondition += "      ) ";
            }
            else if (validFrom != null & !(validTo != null))
            {
                strCondition += "  AND ( ";
                strCondition += "         ds.VALID_FROM >= TO_DATE('" + validFrom + "' , '" + dateFormat + "') ";
                strCondition += "        OR ds.VALID_TO IS NULL ";
                strCondition += "      ) ";
            }

            if (depotPk != 0)
            {
                strCondition = strCondition + " and ds.VENDOR_MST_FK = " + depotPk;
            }
            if (cargoType != 1)
            {
                strCondition = strCondition + " and ds.CARGO_TYPE = " + 2;
            }
            else
            {
                strCondition = strCondition + " and ds.CARGO_TYPE = " + 1;
            }
            if (teriffNo.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(ds.TARIFF_REF_NO) like '%" + teriffNo.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(ds.TARIFF_REF_NO) like '" + teriffNo.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (ActiveOnly)
            {
                strCondition = strCondition + " and ds.ACTIVE= 1";
                strCondition += " AND ( ";
                strCondition += "        ds.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ";
                strCondition += "       OR ds.VALID_TO IS NULL ";
                strCondition += "     ) ";
            }
            strCondition = strCondition + " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk;
            strCondition = strCondition + " AND ds.CREATED_BY_FK = UMT.USER_MST_PK ";
            if (flag == 0)
            {
                strCondition = strCondition + " AND 1=2 ";
            }
            strSQL = "SELECT Count(distinct(ds.TARIFF_REF_NO)) ";
            strSQL = strSQL + " FROM detention_slab_main_tbl ds,vendor_mst_tbl dm,";
            //,CONT_TRN_TRANS trn "
            strSQL = strSQL + " USER_MST_TBL UMT";
            strSQL = strSQL + " WHERE dm.VENDOR_MST_PK=ds.vendor_mst_fk";
            // and trn.CONT_MAIN_TRANS_FK=cont.CONT_MAIN_TRANS_PK "
            strSQL += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
            strSQL = " SELECT * from (";
            strSQL = strSQL + "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL = strSQL + " (SELECT distinct ds.ACTIVE, ";
            strSQL = strSQL + "ds.DETENTION_SLAB_MAIN_PK,";
            strSQL = strSQL + "ds.vendor_mst_fk,";
            strSQL = strSQL + " dm.vendor_NAME,";
            strSQL = strSQL + " ds.TARIFF_REF_NO, ";
            strSQL = strSQL + " TO_DATE(ds.TARIFF_DATE) TARIFF_DATE,";
            strSQL = strSQL + " TO_DATE(ds.VALID_FROM),";
            strSQL = strSQL + " TO_DATE(ds.VALID_TO),";
            strSQL = strSQL + " DECODE(ds.CARGO_TYPE,'2','LCL','1','FCL') CARGO_TYPE,";
            strSQL = strSQL + " ds.VERSION_NO";
            strSQL = strSQL + " FROM detention_slab_main_tbl ds,vendor_mst_tbl dm,";
            //,CONT_MAIN_DEPOT_TBL cont" ',CONT_TRN_TRANS trn "
            strSQL = strSQL + " USER_MST_TBL UMT ";
            strSQL = strSQL + " WHERE dm.vendor_MST_PK=ds.vendor_mst_fk";
            // and trn.CONT_MAIN_TRANS_FK=cont.CONT_MAIN_TRANS_PK"
            strSQL = strSQL + strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }

            if (blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += " ,TARIFF_REF_NO DESC";
            //strSQL = strSQL & vbCrLf & " ORDER BY tra.TRANSPORTER_ID"
            strSQL = strSQL + ") q  )WHERE SR_NO  Between " + start + " and " + last;
            try
            {
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

        #endregion "Teriff"

        #region "depot"

        /// <summary>
        /// Fetches the depot.
        /// </summary>
        /// <param name="depotPK">The depot pk.</param>
        /// <param name="depotID">The depot identifier.</param>
        /// <param name="depotName">Name of the depot.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="Btype">The btype.</param>
        /// <param name="strLocPk">The string loc pk.</param>
        /// <returns></returns>
        public DataSet FetchDepot(Int16 depotPK = 0, string depotID = "", string depotName = "", bool ActiveOnly = true, int Btype = 1, string strLocPk = "0")
        {
            string strSQL = null;
            strSQL = "select ' ' VENDOR_ID,";
            strSQL = strSQL + "' ' VENDOR_NAME, ";
            strSQL = strSQL + "0 VENDOR_MST_PK ";
            strSQL = strSQL + "from DUAL ";
            strSQL = strSQL + "UNION ";
            strSQL = strSQL + "Select VMT.VENDOR_ID, ";
            strSQL = strSQL + "VMT.VENDOR_NAME,";
            strSQL = strSQL + "VMT.VENDOR_MST_PK ";
            strSQL = strSQL + " from VENDOR_MST_TBL VMT,VENDOR_TYPE_MST_TBL VT,Vendor_Services_Trn VST,VENDOR_CONTACT_DTLS VCD Where 1=1 ";
            strSQL = strSQL + "  and VT.VENDOR_TYPE_PK = vst.vendor_type_fk";
            strSQL = strSQL + " and vt.vendor_type_id = 'WAREHOUSE'";
            strSQL = strSQL + " and vmt.vendor_mst_pk = vst.vendor_mst_fk ";
            //Modified to display Warehouse Code based on Logged in Location - same as in Entry Screen : Amit
            strSQL = strSQL + " AND vmt.VENDOR_MST_PK = VCD.VENDOR_MST_FK";

            //commented by thangadurai on 4/8/08
            //strSQL = strSQL & " AND VCD.ADM_LOCATION_MST_FK =" & strLocPk
            strSQL = strSQL + " AND VCD.ADM_LOCATION_MST_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"];
            //
            if (ActiveOnly)
            {
                strSQL = strSQL + " And VMT.Active = 1  ";
            }
            strSQL = strSQL + " And VMT.business_type in (" + Btype + ",3)";
            strSQL = strSQL + " order by VENDOR_ID";
            WorkFlow objWF = new WorkFlow();
            try
            {
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

        #endregion "depot"
    }
}