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
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsDemurrageSlabListing : CommonFeatures
    {
        #region "Fetch Teriff"

        /// <summary>
        /// Fetches the teriff.
        /// </summary>
        /// <param name="teriffDate">The teriff date.</param>
        /// <param name="validFrom">The valid from.</param>
        /// <param name="validTo">The valid to.</param>
        /// <param name="depotPk">The depot pk.</param>
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
        public DataSet FetchTeriff(System.DateTime teriffDate, System.DateTime validFrom, System.DateTime validTo, int depotPk = 0, string teriffNo = "", bool ActiveOnly = true, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        bool blnSortAscending = false, long lngUsrLocFk = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (strColumnName.Trim() == "VENDOR_NAME")
            {
                strColumnName += " ,TARIFF_REF_NO";
            }
            if (teriffDate != null)
            {
                strCondition = strCondition + " and TO_DATE(ds.TARIFF_DATE) = TO_date('" + teriffDate.Date + "','" + dateFormat + "')";
            }

            if (validTo != null)
            {
                strCondition = strCondition + " AND ( ";
                strCondition = strCondition + "        ds.VALID_TO <= TO_DATE('" + validTo + "' , '" + dateFormat + "') ";
                strCondition = strCondition + "       OR ds.VALID_TO IS NULL ";
                strCondition = strCondition + "     ) ";
                if (ActiveOnly == true)
                {
                    strCondition = strCondition + " AND ( ";
                    strCondition = strCondition + "        ds.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ";
                    strCondition = strCondition + "       OR ds.VALID_TO IS NULL ";
                    strCondition = strCondition + "     ) ";
                }
            }
            else
            {
                if (ActiveOnly == true)
                {
                    strCondition = strCondition + " AND ( ";
                    strCondition = strCondition + "        ds.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ";
                    strCondition = strCondition + "       OR ds.VALID_TO IS NULL ";
                    strCondition = strCondition + "     ) ";
                }
            }
            if (validFrom != null)
            {
                strCondition = strCondition + " AND ds.VALID_FROM >= TO_DATE('" + validFrom + "' , '" + dateFormat + "') ";
            }

            if (depotPk != 0)
            {
                strCondition = strCondition + " and ds.DEPOT_MST_FK = " + depotPk;
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
            }
            strCondition = strCondition + " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk;
            strCondition = strCondition + " AND ds.CREATED_BY_FK = UMT.USER_MST_PK ";
            if (flag == 0)
            {
                strCondition = strCondition + " AND 1=2 ";
            }
            strSQL = "SELECT Count(distinct(ds.TARIFF_REF_NO)) ";
            strSQL = strSQL + " FROM DEMURRAGE_SLAB_MAIN_TBL ds,vendor_mst_tbl dm, LOCATION_MST_TBL LMT,VENDOR_CONTACT_DTLS VMTDTL,";
            strSQL = strSQL + " USER_MST_TBL UMT";
            strSQL = strSQL + " WHERE dm.vendor_MST_PK=ds.DEPOT_MST_FK  and dm.vendor_mst_pk=VMTDTL.VENDOR_MST_FK and lmt.location_mst_pk(+)=VMTDTL.Adm_Location_Mst_Fk";
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
            strSQL = strSQL + "ds.DEMURAGE_SLAB_MAIN_PK,";
            strSQL = strSQL + "ds.DEPOT_MST_FK,";
            strSQL = strSQL + " dm.VENDOR_NAME,";
            strSQL = strSQL + " ds.TARIFF_REF_NO, ";
            strSQL = strSQL + " TO_DATE(ds.TARIFF_DATE) TARIFF_DATE,";
            strSQL = strSQL + " TO_DATE(ds.VALID_FROM),";
            strSQL = strSQL + " TO_DATE(ds.VALID_TO),";
            strSQL = strSQL + " ds.VERSION_NO";
            strSQL = strSQL + " FROM DEMURRAGE_SLAB_MAIN_TBL ds,vendor_mst_tbl dm,";
            strSQL = strSQL + " USER_MST_TBL UMT";
            strSQL = strSQL + " WHERE dm.vendor_MST_PK=ds.DEPOT_MST_FK";
            strSQL = strSQL + strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }

            if (blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += " ,TARIFF_REF_NO DESC ";

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

        #endregion "Fetch Teriff"
    }
}