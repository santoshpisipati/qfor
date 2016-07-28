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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Configuration;
using System.Data;
using System.Web;
using System.Xml;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    public enum ExtraMessagePosition
    {
        /// <summary>
        /// The before main message
        /// </summary>
        BeforeMainMessage = 0,
        /// <summary>
        /// The after main message
        /// </summary>
        AfterMainMessage = 1
    }

    /// <summary>
    /// 
    /// </summary>
    public class GridHeaderSetting
    {
        /// <summary>
        /// The m_ control identifier
        /// </summary>
        public string M_CtrlID;
        /// <summary>
        /// The m_ settings
        /// </summary>
        public Int64 M_Settings;
        /// <summary>
        /// The m_ text
        /// </summary>
        public string M_TEXT;
        /// <summary>
        /// The m_ tool tip
        /// </summary>
        public string M_ToolTip;
        /// <summary>
        /// The m_ width
        /// </summary>
        public Int64 M_Width;
        /// <summary>
        /// The m_ column align
        /// </summary>
        public Int64 M_ColumnAlign;
        /// <summary>
        /// The m_ column field length
        /// </summary>
        public Int64 M_ColumnFieldLength;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="System.Web.UI.Page" />
    public class Common : System.Web.UI.Page
    {
        /// <summary>
        /// The string style sheet
        /// </summary>
        public string strStyleSheet;

        //To Store Path of CommonFunction.js file.
        /// <summary>
        /// The string common function path
        /// </summary>
        public string strCommonFunctionPath;

        //To Store Path of CommonInfraGRidFunction.js file.
        /// <summary>
        /// The string common grid function path
        /// </summary>
        public string strCommonGridFunctionPath;

        //To Store Path of VBCommonFn.vbs file.
        /// <summary>
        /// The string vb common function path
        /// </summary>
        public string strVBCommonFunctionPath;

        // To store path of invalid login redirect page.
        /// <summary>
        /// The string invalid login redirect
        /// </summary>
        public string strInvalidLoginRedirect = "~/Forms/03Security/frmOTHInvalidAccess.aspx";

        /// <summary>
        /// The int value
        /// </summary>
        public Int64 IntVal;
        /// <summary>
        /// The datasetclone
        /// </summary>
        public DataSet datasetclone = new DataSet();
        /// <summary>
        /// The LNG row count
        /// </summary>
        public long lngRowCount;
        /// <summary>
        /// The default logo
        /// </summary>
        public string DefaultLogo = ConfigurationManager.AppSettings["LOGO"];

        //Public strReleaseMode As String = "Development" For SRS/FS it will be visible
        /// <summary>
        /// FN_s the get product_by_ database user.
        /// </summary>
        /// <param name="DB_UserID">The d b_ user identifier.</param>
        /// <returns></returns>
        public string fn_GetProduct_by_DBUser(string DB_UserID)
        {
            string strSQL = null;
            strSQL = ConfigurationManager.AppSettings["ProductID"].ToUpper();
            try
            {
                return strSQL;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Gets the error message.
        /// </summary>
        /// <param name="ErrorCode">The error code.</param>
        /// <param name="EnvironmnetId">The environmnet identifier.</param>
        /// <param name="ExtraMessage">The extra message.</param>
        /// <param name="PositionOfExtraMessage">The position of extra message.</param>
        /// <param name="FormName">Name of the form.</param>
        /// <param name="EventName">Name of the event.</param>
        /// <returns></returns>
        public string GetErrorMessage(int ErrorCode, double EnvironmnetId = 0, string ExtraMessage = "", ExtraMessagePosition PositionOfExtraMessage = ExtraMessagePosition.AfterMainMessage, string FormName = "", string EventName = "")
        {
            WorkFlow objwk = new WorkFlow();
            try
            {
                XmlDocument xmlDOC = new XmlDocument();
                string strSQL = null;

                strSQL = "select Qm.Err_Msg_Code,Qm.Category,Qm.Message_Type, qm.client_or_server , qm.message";

                //Ask Goutam Environment is always null
                if (EnvironmnetId == 0)
                {
                    strSQL += " from qflx_messages_tbl Qm where qm.environment_fk =1";
                }
                else
                {
                    strSQL += " from qflx_messages_tbl Qm where qm.environment_fk = " + HttpContext.Current.Session["ENVIRONMENT_PK"];
                }
                strSQL += " and qm.err_msg_code ='" + ErrorCode + "'";

                OracleDataReader DR = objwk.GetDataReader(strSQL);
                if (DR.Read())
                {
                    return Convert.ToString(DR["message"]);
                }
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            finally
            {
                objwk.CloseConnection();
            }
            return "";
        }

        /// <summary>
        /// 
        /// </summary>
        public enum BusinessType
        {
            //enums used in most of the masters/transactions
            /// <summary>
            /// The air
            /// </summary>
            Air = 1,

            /// <summary>
            /// The sea
            /// </summary>
            Sea = 2,
            /// <summary>
            /// The both
            /// </summary>
            Both = 3,

            //Added by surya prasad as per CR Q FOR- VEK
            /// <summary>
            /// The removals
            /// </summary>
            Removals = 4

            //End
        }

        /// <summary>
        /// Gets or sets the type of the current business.
        /// </summary>
        /// <value>
        /// The type of the current business.
        /// </value>
        public BusinessType CurrentBusinessType
        {
            get
            {
                if ((HttpContext.Current.Session["BUSINESSTYPE"] == null))
                {
                    return BusinessType.Both;
                }
                else
                {
                    return (BusinessType)HttpContext.Current.Session["BUSINESSTYPE"];
                }
            }
            set { HttpContext.Current.Session["BUSINESSTYPE"] = value; }
        }

        /// <summary>
        /// Returns the deserialized json.
        /// </summary>
        /// <param name="status">The status.</param>
        /// <returns></returns>
        public object ReturnDeserializedJson(CLS_STATUS status)
        {
            string output = JsonConvert.SerializeObject(status);
            return JsonConvert.DeserializeObject(output);
        }
    }
}