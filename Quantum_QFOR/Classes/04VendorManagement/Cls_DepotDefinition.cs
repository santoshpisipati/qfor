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

using Oracle.DataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class ClsDepotDefinition : CommonFeatures
    {
        #region " Fetch All "

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="DepotId">The depot identifier.</param>
        /// <param name="DepotName">Name of the depot.</param>
        /// <param name="CountryID">The country identifier.</param>
        /// <param name="CountryName">Name of the country.</param>
        /// <param name="businessType">Type of the business.</param>
        /// <param name="Loc_Fk">The loc_ fk.</param>
        /// <param name="currentBusinessType">Type of the current business.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">if set to <c>true</c> [active flag].</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <returns></returns>
        public DataSet FetchAll(string DepotId = "", string DepotName = "", string CountryID = "", string CountryName = "", int businessType = 3, Int64 Loc_Fk = 0, int currentBusinessType = 3, string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = "";
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (DepotId.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(DEPOT_ID) LIKE '" + DepotId.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(DEPOT_ID) LIKE '%" + DepotId.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(DEPOT_ID) LIKE '%" + DepotId.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (DepotName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(DEPOT_NAME) LIKE '" + DepotName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(DEPOT_NAME) LIKE '%" + DepotName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(DEPOT_NAME) LIKE '%" + DepotName.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (CountryID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(COUNTRY_ID) LIKE '" + CountryID.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(COUNTRY_ID) LIKE '%" + CountryID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(COUNTRY_ID) LIKE '%" + CountryID.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (CountryName.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(COUNTRY_NAME) LIKE '" + CountryName.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(COUNTRY_NAME) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(COUNTRY_NAME) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (businessType == 3 & currentBusinessType == 3)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,2,3) ";
            }
            else if (businessType == 3 & currentBusinessType == 2)
            {
                strCondition += " AND BUSINESS_TYPE IN (2,3) ";
            }
            else if (businessType == 3 & currentBusinessType == 1)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,3) ";
            }
            else
            {
                strCondition += " AND BUSINESS_TYPE = " + businessType + " ";
            }

            if (ActiveFlag == true)
            {
                strCondition += " AND dm.ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += " ";
            }

            if (Loc_Fk > 0)
            {
                strCondition += " AND l.location_mst_pk=" + Loc_Fk;
            }
            if (BlankGrid == 0)
            {
                strCondition += " AND 1=2 ";
            }
            strSQL = "SELECT Count(*) from DEPOT_MST_TBL dm, DEPOT_CONTACT_DTLS dc, COUNTRY_MST_TBL cn,location_mst_tbl l,vendor_mst_tbl VM, vendor_contact_dtls VCD ,vendor_services_trn VST,vendor_type_mst_tbl vt where ";
            strSQL += " dm.DEPOT_MST_PK = dc.DEPOT_MST_FK(+) and ";
            strSQL += " dc.ADM_COUNTRY_MST_FK = cn.COUNTRY_MST_PK(+) ";
            strSQL += " and dm.location_mst_fk = l.location_mst_pk(+) ";
            strSQL += strCondition;

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            strSQL = " select * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += "DEPOT_MST_PK, ";
            strSQL += "NVL(dm.ACTIVE_FLAG,0) ACTIVE_FLAG , ";
            strSQL += "DEPOT_ID, ";
            strSQL += "UPPER(DEPOT_NAME) DEPOT_NAME, ";
            strSQL += "UPPER(COUNTRY_NAME) COUNTRY_NAME, ";
            strSQL += "dm.VERSION_NO  VERSION_NO, ";
            strSQL += "decode(dm.BUSINESS_TYPE,1 ,'Air', 2, 'Sea',3, 'Both') BUSINESS_TYPE, ";
            strSQL += " dm.location_mst_fk LocationFk,";
            strSQL += " l.location_name LocationName";
            strSQL += "FROM DEPOT_MST_TBL dm, DEPOT_CONTACT_DTLS dc, COUNTRY_MST_TBL cn,location_mst_tbl l  where ";
            strSQL += " dm.DEPOT_MST_PK = dc.DEPOT_MST_FK and ";
            strSQL += "  l.location_mst_pk= dm.location_mst_fk";
            strSQL += " and dc.ADM_COUNTRY_MST_FK = cn.COUNTRY_MST_PK ";
            strSQL += strCondition;
            strSQL += " order by " + SortColumn + SortType + " ) q  ) ";
            strSQL += " WHERE SR_NO  Between " + start + " and " + last;
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

        #endregion " Fetch All "

        #region "contract"

        /// <summary>
        /// Fetches the contract.
        /// </summary>
        /// <param name="contractDate">The contract date.</param>
        /// <param name="validFrom">The valid from.</param>
        /// <param name="validTo">The valid to.</param>
        /// <param name="DEPOTID">The depotid.</param>
        /// <param name="TranPk">The tran pk.</param>
        /// <param name="DEPOTName">Name of the depot.</param>
        /// <param name="contractNo">The contract no.</param>
        /// <param name="BType">Type of the b.</param>
        /// <param name="CurrentBType">Type of the current b.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="Approvedonly">if set to <c>true</c> [approvedonly].</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Int_WfStatus">The int_ wf status.</param>
        /// <returns></returns>
        public DataSet FetchContract(string contractDate, string validFrom, string validTo, string DEPOTID = "", string TranPk = "", string DEPOTName = "", string contractNo = "", int BType = 0, int CurrentBType = 0, bool ActiveOnly = true,
        bool Approvedonly = true, Int32 Cargotype = 1, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, long lngUsrLocFk = 0, Int32 flag = 0, Int32 Int_WfStatus = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = "";
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (DEPOTID.Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(VM.VENDOR_ID) like '%" + DEPOTID.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(VM.VENDOR_ID) like '" + DEPOTID.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (DEPOTName.Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(VM.VENDOR_NAME) like '%" + DEPOTName.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(VM.VENDOR_NAME) like '" + DEPOTName.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (Convert.ToString(getDefault(contractDate, "")).Length > 0)
            {
                strCondition = strCondition + " and TO_DATE(cont.CONTRACT_DATE) = TO_date('" + contractDate + "', '" + dateFormat + "')";
            }

            if (Convert.ToString(getDefault(validFrom, "")).Length > 0 & Convert.ToString(getDefault(validTo, "")).Length > 0)
            {
                //strCondition = strCondition & vbCrLf & " AND ((TO_DATE('" & validTo & "' , '" & dateFormat & "') BETWEEN "
                //strCondition = strCondition & vbCrLf & "    cont.VALID_FROM AND cont.VALID_TO) OR"
                //strCondition = strCondition & vbCrLf & "    (TO_DATE('" & validFrom & "' , '" & dateFormat & "') BETWEEN "
                //strCondition = strCondition & vbCrLf & "    cont.VALID_FROM AND cont.VALID_TO) OR"
                //strCondition = strCondition & vbCrLf & "    (cont.VALID_TO IS NULL))"
                strCondition = strCondition + " AND ((cont.VALID_FROM >= TO_DATE('" + validFrom + "' , '" + dateFormat + "')) AND ";
                strCondition = strCondition + "    (nvl(cont.VALID_TO,TO_DATE('01/01/0001')) <= TO_DATE('" + validTo + "' , '" + dateFormat + "'))) ";
            }
            else if (Convert.ToString(getDefault(validFrom, "")).Length > 0 & !(Convert.ToString(getDefault(validTo, "")).Length > 0))
            {
                strCondition = strCondition + " AND ( ";
                strCondition = strCondition + "        cont.VALID_FROM >= TO_DATE('" + validFrom + "' , '" + dateFormat + "') ";
                strCondition = strCondition + "       OR cont.VALID_TO IS NULL ";
                strCondition = strCondition + "     ) ";
            }
            else if (Convert.ToString(getDefault(validTo, "")).Length > 0 & !(Convert.ToString(getDefault(validFrom, "")).Length > 0))
            {
                strCondition = strCondition + " AND ( ";
                strCondition = strCondition + "        cont.VALID_TO <= TO_DATE('" + validTo + "' , '" + dateFormat + "') ";
                strCondition = strCondition + "       OR cont.VALID_TO IS NULL ";
                strCondition = strCondition + "     ) ";
            }

            if (contractNo.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " and UPPER(cont.CONTRACT_NO) like '%" + contractNo.ToUpper().Replace("'", "''") + "%'";
                }
                else if (SearchType == "S")
                {
                    strCondition = strCondition + " and UPPER(cont.CONTRACT_NO) like '" + contractNo.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (ActiveOnly)
            {
                strCondition = strCondition + " and cont.ACTIVE = 1";
            }
            if (Approvedonly)
            {
                strCondition = strCondition + " and cont.CONT_APPROVED = 1";
            }
            if (CurrentBType != 0)
            {
                strCondition = strCondition + " and cont.CARGO_TYPE = " + Cargotype;
            }
            if (CurrentBType == 3 & BType == 3)
            {
                strCondition += " AND cont.BUSINESS_TYPE IN (1,2,3) ";
            }
            else if (CurrentBType == 3 & BType == 2)
            {
                strCondition += " AND cont.BUSINESS_TYPE IN (2,3) ";
            }
            else if (CurrentBType == 3 & BType == 1)
            {
                strCondition += " AND cont.BUSINESS_TYPE IN (1,3) ";
            }
            else if (CurrentBType > 0)
            {
                strCondition += " AND cont.BUSINESS_TYPE = " + CurrentBType + " ";
            }

            strCondition += " AND UMT.DEFAULT_LOCATION_FK IN (SELECT L.LOCATION_MST_PK ";
            strCondition += " FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK = " + lngUsrLocFk;
            strCondition += " CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ";
            strCondition += " AND cont.CREATED_BY_FK = UMT.USER_MST_PK ";

            strCondition += "  AND VST.VENDOR_MST_FK=VM.VENDOR_MST_PK AND VST.VENDOR_TYPE_FK=VT.VENDOR_TYPE_PK AND VM.VENDOR_MST_PK=VCD.VENDOR_MST_FK ";
            strCondition += " And VT.VENDOR_TYPE_ID = 'WAREHOUSE' And VCD.ADM_LOCATION_MST_FK in ";
            strCondition += "(SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK = " + lngUsrLocFk;
            strCondition += " CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ";

            //---------------------------------------------------------------------------

            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }

            strSQL = "SELECT Count(distinct(cont.CONTRACT_NO)) ";
            strSQL = strSQL + "  FROM vendor_services_trn VST,VENDOR_MST_TBL VM,VENDOR_TYPE_MST_TBL VT,CONT_MAIN_DEPOT_TBL cont,USER_MST_TBL UMT,vendor_contact_dtls VCD";
            strSQL = strSQL + "  WHERE VM.VENDOR_MST_PK=cont.DEPOT_MST_FK";
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
            strSQL = strSQL + " (SELECT distinct cont.ACTIVE, ";
            strSQL = strSQL + " cont.CONT_APPROVED,";
            strSQL = strSQL + "VM.VENDOR_MST_PK,";
            strSQL = strSQL + "cont.CONT_MAIN_DEPOT_PK,";
            strSQL = strSQL + " cont.CONTRACT_NO,";
            strSQL = strSQL + " VM.VENDOR_ID, ";
            strSQL = strSQL + " VM.VENDOR_NAME,";

            strSQL = strSQL + " TO_DATE(cont.CONTRACT_DATE) CONTRACT_DATE,";
            strSQL = strSQL + " TO_DATE(cont.VALID_FROM),";
            strSQL = strSQL + " TO_DATE(cont.VALID_TO),";
            strSQL = strSQL + "    case when cont.BUSINESS_TYPE =1 then";
            strSQL = strSQL + "  ''  ";
            strSQL = strSQL + "    ELSE ";
            strSQL = strSQL + "DECODE(cont.CARGO_TYPE,'1','FCL','2','LCL') END CARGO_TYPE, ";
            strSQL += " DECODE(cont.BUSINESS_TYPE,'1','Air','2','Sea','3','Both') BUSINESS_TYPE,DECODE(cont.Workflow_Status,null,'Requested','0','Requested','1','Approved','2','Rejected') Status ";
            //CARGO_TYPE
            strSQL = strSQL + " FROM vendor_services_trn VST,VENDOR_TYPE_MST_TBL VT,VENDOR_MST_TBL VM,CONT_MAIN_DEPOT_TBL cont,USER_MST_TBL UMT,vendor_contact_dtls VCD";
            strSQL = strSQL + " WHERE VM.VENDOR_MST_PK=cont.DEPOT_MST_FK";
            if (Int_WfStatus == 0 | Int_WfStatus == 1 | Int_WfStatus == 2)
            {
                strSQL = strSQL + " AND CONT.WORKFLOW_STATUS=" + Int_WfStatus;
            }
            strSQL = strSQL + strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }
            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += " ,CONTRACT_NO DESC";
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

        #endregion "contract"
    }
}