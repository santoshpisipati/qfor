using Oracle.DataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.Common" />
    public class frmMSTLocation : Common
    {
        /// <summary>
        /// The object location
        /// </summary>
        private clsLocation_Mst_Tbl objLocation = new clsLocation_Mst_Tbl();

        /// <summary>
        /// The object corporate
        /// </summary>
        private clsCorporate_Mst_Tbl objCorporate = new clsCorporate_Mst_Tbl();

        /// <summary>
        /// The object location_ type
        /// </summary>
        private clsLocation_Type_Mst_Tbl objLocation_Type = new clsLocation_Type_Mst_Tbl();

        /// <summary>
        /// The object country
        /// </summary>
        private clsCountry_Mst_Tbl objCountry = new clsCountry_Mst_Tbl();

        /// <summary>
        /// The object agent
        /// </summary>
        private cls_Agent_Details objAGENT = new cls_Agent_Details();

        /// <summary>
        /// The object ds
        /// </summary>
        private DataSet objDS;

        /// <summary>
        /// The object ds location
        /// </summary>
        private DataSet objDSLocation;

        /// <summary>
        /// The dt profit margin
        /// </summary>
        private DataTable dtProfitMargin;

        /// <summary>
        /// Gets the location MST pk.
        /// </summary>
        /// <value>
        /// The location MST pk.
        /// </value>
        public Int32 LocationMstPK
        {
            get
            {
                try
                {
                    if ((Request.QueryString["PK_Value"] != null))
                    {
                        return Convert.ToInt32(Request.QueryString["PK_Value"]);
                    }
                }
                catch (Exception ex)
                {
                }
                return -1;
            }
        }

        /// <summary>
        /// Fetch_s the location.
        /// </summary>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <returns></returns>
        public DataSet Fetch_Location(bool ActiveOnly = false)
        {
            string strSQL = null;
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            strSQL = "select ' ' LOCATION_ID,";
            strSQL += "' ' LOCATION_NAME, ";
            strSQL += "0 LOCATION_MST_PK ";
            strSQL += "FROM DUAL ";
            strSQL += "UNION ";
            strSQL += "SELECT LOCATION_ID, ";
            strSQL += "LOCATION_NAME,";
            strSQL += "LOCATION_MST_PK ";
            strSQL += "FROM LOCATION_MST_TBL WHERE ";

            if (ActiveOnly == true)
            {
                strSQL += " ACTIVE_FLAG in (1,0)";
            }
            else
            {
                strSQL += "COMP_LOCATION = 1 AND ACTIVE_FLAG = 1";
            }
            strSQL += "order by LOCATION_NAME";
            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Location_images the specified locpk.
        /// </summary>
        /// <param name="Locpk">The locpk.</param>
        /// <returns></returns>
        public new string Location_image(long Locpk)
        {
            string strSql = null;
            string strLogoName = null;
            try
            {
                strSql = "Select LOGO_FILE_PATH from Location_Mst_Tbl lmst where lmst.location_mst_pk =" + Locpk;
                strLogoName = (new WorkFlow()).ExecuteScaler(strSql);
                if (string.IsNullOrEmpty(strLogoName))
                {
                    strLogoName = DefaultLogo;
                }
                return strLogoName;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}