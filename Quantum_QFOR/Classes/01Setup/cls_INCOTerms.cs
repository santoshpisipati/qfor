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
    public class clsINCOTerms : CommonFeatures
	{
        #region "Fetch All"
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="INCOID">The incoid.</param>
        /// <param name="INCODesc">The inco desc.</param>
        /// <param name="intFreight">The int freight.</param>
        /// <param name="intResp">The int resp.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ActiveFlag">The active flag.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchAll(string INCOID = "", string INCODesc = "", int intFreight = 0, int intResp = 0, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int ActiveFlag = 1, bool blnSortAscending = false,
		Int32 flag = 0, Int32 Export = 0)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();

			if (flag == 0) {
				strCondition += " AND 1=2";
			}

			if (INCOID.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " AND UPPER(INCO_CODE) LIKE '" + INCOID.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition += " AND UPPER(INCO_CODE) LIKE '%" + INCOID.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition += " AND UPPER(INCO_CODE) LIKE '%" + INCOID.ToUpper().Replace("'", "''") + "%'" ;
				}
			}

			if (INCODesc.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " AND UPPER(INCO_CODE_DESCRIPTION) LIKE '" + INCODesc.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition += " AND UPPER(INCO_CODE_DESCRIPTION) LIKE '%" + INCODesc.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition += " AND UPPER(INCO_CODE_DESCRIPTION) LIKE '%" + INCODesc.ToUpper().Replace("'", "''") + "%'" ;
				}
			}


			if (!(intFreight == 0)) {
				strCondition += " AND FREIGHT_TYPE = " + intFreight + "" ;

			}



			if (!(intResp == 0)) {
				strCondition += " AND RESPONSIBILITY = " + intResp + "" ;

			}



			if (ActiveFlag == 1) {
				strCondition += " AND ACTIVE_FLAG = 1 ";
			} else {
				strCondition += "";
			}

			strSQL = "SELECT COUNT(*) FROM SHIPPING_TERMS_MST_TBL WHERE 1=1";
			strSQL += strCondition;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / M_MasterPageSize;
			if (TotalRecords % M_MasterPageSize != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * M_MasterPageSize;
			start = (CurrentPage - 1) * M_MasterPageSize + 1;

			strSQL = " select * from (";
			strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
			strSQL += "(SELECT  ";
			strSQL += "SHIPPING_TERMS_MST_PK, ";
			strSQL += "NVL(ACTIVE_FLAG,0) ACTIVE_FLAG , ";
			strSQL += "INCO_CODE, ";
			strSQL += "INCO_CODE_DESCRIPTION INCO_DESCRIPTION, ";
			strSQL += "Decode(FREIGHT_TYPE, 0, '', 1, 'Prepaid',2,'Collect','') FREIGHT_TYPE, ";
			strSQL += " Decode(RESPONSIBILITY  , 0, '', 1, 'Line',2,'Shipper',3,'Consignee','')  RESPONSIBILITY,VERSION_NO  ";

			strSQL += "FROM SHIPPING_TERMS_MST_TBL ";
			strSQL += "WHERE 1=1";

			strSQL += strCondition;

			if (!strColumnName.Equals("SR_NO")) {
				strSQL += "order by " + strColumnName;
			}

			if (!blnSortAscending & !strColumnName.Equals("SR_NO")) {
				strSQL += " DESC";
			}
			if (Export == 0) {
				strSQL += ") q  ) WHERE SR_NO  Between " + start + " and " + last;
			} else {
				strSQL += ") q  )";
			}

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
