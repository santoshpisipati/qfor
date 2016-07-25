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
    public class cls_Container_Type_Mst_Tbl : CommonFeatures
    {
        /// <summary>
        /// The m_ data set
        /// </summary>
        private DataSet M_DataSet;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public DataSet MyDataSet
        {
            get { return M_DataSet; }
            set { M_DataSet = value; }
        }

        #endregion "List of Properties"

        #region "Constructors"

        /// <summary>
        /// Initializes a new instance of the <see cref="cls_Container_Type_Mst_Tbl"/> class.
        /// </summary>
        public cls_Container_Type_Mst_Tbl()
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "select c.container_type_mst_pk,";
            strSQL += "c.container_type_mst_id,";
            strSQL += "c.container_type_name  ";
            strSQL += " from container_type_mst_tbl c  ";
            strSQL += " where c.active_flag = 1 ";
            strSQL += " order by c.PREFERENCES";
            try
            {
                M_DataSet = objWF.GetDataSet(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Constructors"

        #region "FetchContianerType Function"

        /// <summary>
        /// Fetches the type of the contianer.
        /// </summary>
        /// <param name="ContianerTypePK">The contianer type pk.</param>
        /// <param name="ContianerTypeID">The contianer type identifier.</param>
        /// <param name="ContianerTypeName">Name of the contianer type.</param>
        /// <returns></returns>
        public DataSet FetchContianerType(Int16 ContianerTypePK = 0, string ContianerTypeID = "", string ContianerTypeName = "")
        {
            string strSQL = null;
            strSQL = strSQL + " select ";
            strSQL = strSQL + " ' ' Container_Type_Mst_ID,";
            strSQL = strSQL + " ' ' Container_Type_Name,";
            strSQL = strSQL + " 0 Container_Type_Mst_PK ";
            strSQL = strSQL + " from ";
            strSQL = strSQL + " DUAL ";
            strSQL = strSQL + " UNION";
            strSQL = strSQL + " select ";
            strSQL = strSQL + " Container_Type_Mst_ID,";
            strSQL = strSQL + " Container_Type_Name,";
            strSQL = strSQL + " Container_Type_Mst_PK ";
            strSQL = strSQL + " from Container_Type_Mst_tbl";
            strSQL = strSQL + " Where nvl(Active_Flag,0) = 1";
            strSQL = strSQL + " order by Container_Type_Mst_ID ";

            WorkFlow objWF = new WorkFlow();
            OracleDataReader objDR = default(OracleDataReader);
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

        #endregion "FetchContianerType Function"

        #region "FetchContianerType Function for freight details"

        /// <summary>
        /// Fetches the contianer typeforfreightdetails.
        /// </summary>
        /// <param name="ContianerTypePK">The contianer type pk.</param>
        /// <param name="ContianerTypeID">The contianer type identifier.</param>
        /// <param name="ContianerTypeName">Name of the contianer type.</param>
        /// <returns></returns>
        public DataSet FetchContianerTypeforfreightdetails(Int16 ContianerTypePK = 0, string ContianerTypeID = "", string ContianerTypeName = "")
        {
            string strSQL = null;
            strSQL = strSQL + " select ";
            strSQL = strSQL + " ' ' Container_Type_Mst_ID,";
            strSQL = strSQL + " ' ' Container_Type_Name,";
            strSQL = strSQL + " 0 Container_Type_Mst_PK ";
            strSQL = strSQL + " from ";
            strSQL = strSQL + " DUAL ";
            strSQL = strSQL + " UNION";
            strSQL = strSQL + " select ";
            strSQL = strSQL + " Container_Type_Mst_ID,";
            strSQL = strSQL + " Container_Type_Name,";
            strSQL = strSQL + " Container_Type_Mst_PK ";
            strSQL = strSQL + " from Container_Type_Mst_tbl";
            strSQL = strSQL + " Where nvl(Active_Flag,0) = 1";
            strSQL = strSQL + " order by Container_Type_Mst_ID ";

            WorkFlow objWF = new WorkFlow();
            OracleDataReader objDR = default(OracleDataReader);
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

        #endregion "FetchContianerType Function for freight details"

        #region "Fetch KIND"

        /// <summary>
        /// Fetches the kind.
        /// </summary>
        /// <param name="ContainerKindPK">The container kind pk.</param>
        /// <param name="ContainerKind">Kind of the container.</param>
        /// <returns></returns>
        public DataSet FetchKind(Int16 ContainerKindPK = 0, string ContainerKind = "")
        {
            string strSQL = null;
            strSQL = "select c.container_type_mst_pk,c.container_type_id from container_kind_mst_tbl c order by c.container_type_id";
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

        #endregion "Fetch KIND"

        #region "Fetch Type"

        /// <summary>
        /// Fetches the type.
        /// </summary>
        /// <param name="ContainerTypePK">The container type pk.</param>
        /// <param name="ContainerKind">Kind of the container.</param>
        /// <returns></returns>
        public DataSet FetchType(Int16 ContainerTypePK = 0, string ContainerKind = "")
        {
            string strSQL = null;
            strSQL = "select c.container_type_mst_pk, c.container_type_mst_id from container_type_mst_tbl c where Active_Flag = 1 ";
            strSQL = strSQL + " order by c.container_type_mst_id";

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

        #endregion "Fetch Type"

        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_Container_Type_Mst_Id">The p_ container_ type_ MST_ identifier.</param>
        /// <param name="P_Container_Type_Name">Name of the p_ container_ type_.</param>
        /// <param name="ISONr">The iso nr.</param>
        /// <param name="P_Container_Kind">Kind of the p_ container_.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="IsActive">The is active.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string P_Container_Type_Mst_Id, string P_Container_Type_Name, string ISONr = "", string P_Container_Kind = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, Int16 IsActive = 0,
        bool blnSortAscending = false, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            strColumnName = (strColumnName == "PREFERENCE" ? "PREFERENCES" : strColumnName);

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (P_Container_Type_Mst_Id.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(Container_Type_Mst_Id) like '%" + P_Container_Type_Mst_Id.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(Container_Type_Mst_Id) like '" + P_Container_Type_Mst_Id.ToUpper().Replace("'", "''") + "%' ";
                }
            }

            if (P_Container_Type_Name.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(Container_Type_Name) like '%" + P_Container_Type_Name.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(Container_Type_Name) like '" + P_Container_Type_Name.ToUpper().Replace("'", "''") + "%' ";
                }
            }

            if (ISONr.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And UPPER(Iso_Number) like '%" + ISONr.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And UPPER(Iso_Number) like '" + ISONr.ToUpper().Replace("'", "''") + "%' ";
                }
            }

            if (P_Container_Kind.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And UPPER(Container_Kind) like '%" + P_Container_Kind.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And UPPER(Container_Kind) like '" + P_Container_Kind.ToUpper().Replace("'", "''") + "%' ";
                }
            }

            if (IsActive == 1)
            {
                strCondition += " and ACTIVE_FLAG = 1";
            }

            strSQL = "SELECT Count(*) from CONTAINER_TYPE_MST_TBL where 1=1";
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

            strSQL = " select  * from (";
            strSQL += " SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += " Container_Type_Mst_Pk,";
            strSQL += " Nvl(Active_Flag,0) Active_Flag, ";
            strSQL += " Container_Type_Mst_Id,";
            strSQL += " Container_Type_Name,";
            strSQL += " Container_Kind,";
            strSQL += " Iso_Number,";
            strSQL += " Container_Length_Ft,";
            strSQL += " Container_Width_Ft,";
            strSQL += " Container_Height_Ft,";
            strSQL += " Container_Tareweight_Tone,";
            strSQL += " Container_Max_Capacity_Tone,";
            strSQL += " Volume,";
            strSQL += " TO_NUMBER(TEU_FACTOR) TEU_FACTOR,";
            strSQL += " Version_No, ";
            strSQL += " 0 \"DELFLAG\" ,0 \"CHGFLAG\" ,";
            strSQL += " PREFERENCES";
            strSQL += " FROM CONTAINER_TYPE_MST_TBL ";
            strSQL += " WHERE ( 1 = 1) ";

            strSQL += strCondition;

            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += " )q) WHERE SR_NO  Between " + start + " and " + last;
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

        #endregion "Fetch Function"

        #region "Fetch ContainerStatus"

        /// <summary>
        /// Fetches the container status.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchContainerStatus()
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = string.Empty;
            strSQL += "SELECT  -1 CONTAINER_STATUS_PK , ' ' CONTAINER_STATUS  from dual union ";
            strSQL += "select C.CONTAINER_STATUS_PK, C.CONTAINER_STATUS FROM CONTAINER_STATUS_MST_TBL C  ";

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

        #endregion "Fetch ContainerStatus"
    }
}