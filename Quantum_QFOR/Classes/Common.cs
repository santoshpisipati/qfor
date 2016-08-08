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
using System.Collections;
using System.Configuration;
using System.Data;
using System.Web;
using System.Web.UI.WebControls;
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
    public class Common
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


        #region Added New On 08-08-2016


  //      public enum ExtraMessagePosition
  //      {
  //          BeforeMainMessage = 0,
  //          AfterMainMessage = 1
  //      }
  //      public class GridHeaderSetting
  //      {
  //          public string M_CtrlID;
  //          public Int64 M_Settings;
  //          public string M_TEXT;
  //          public string M_ToolTip;
  //          public Int64 M_Width;
  //          public Int64 M_ColumnAlign;
  //          public Int64 M_ColumnFieldLength;
  //      }

           
  //          public ICollection GridHeaders ;
  //          //Public strReleaseMode As String = "Development" For SRS/FS it will be visible
  //          //Code Modified By: Sumit
  //          //Date: 12-Jul-05
  //          //"Release" ' For SRS/FS it will be invisible
  //          public string strReleaseMode = ConfigurationManager.AppSettings["SCMODE"];
  //          public string strGridRowHeight = "20";
  //          public string strSmallGridRowHeight = "12";
  //          public delegate void DelSearchObject(object sender, System.EventArgs e);
  //          //Public strDateFormat As String = Session("DATE_FORMAT")
  //          public cls_Admin_DashBoard ObjLog = new cls_Admin_DashBoard();
  //          public string BarCodeLength = ConfigurationManager.AppSettings["BarCodeLength"];
  //          public string RemovalsBizType = ConfigurationManager.AppSettings["RemovalsBizType"];
  //          public bool IsActivityTrack = Convert.ToBoolean(ConfigurationManager.AppSettings["IsActivityTrack"]);
  //          public string M_SMTP_SERVER = ConfigurationManager.AppSettings["MailServer"];
  //          public string M_SEND_USERNAME = ConfigurationManager.AppSettings["SEND_USERNAME"];
  //          public string M_SEND_PASSWORD = ConfigurationManager.AppSettings["SEND_PASSWORD"];
  //          public string M_MAIL_SERVER = ConfigurationManager.AppSettings["MailServer"];
  //          public string QFOR_CLIENT = ConfigurationManager.AppSettings["Client"];
  //          public string Client_new = ConfigurationManager.AppSettings["ClientNew"];
  //          public string SecretKey = ConfigurationManager.AppSettings["SecretKey"];
  //          public string xlsProvider = ConfigurationManager.AppSettings["xlsProvider"];
  //          public string xlsExtProp = ConfigurationManager.AppSettings["xlsExtProp"];
		//public string strVersionNo = ConfigurationManager.AppSettings["VersionNo"];
  //          public string QFINGeneral = ConfigurationManager.AppSettings["QFINGeneral"];
  //          public string AutoInvoice = ConfigurationManager.AppSettings["AutoInvoice"];
  //          //' PTS- AUG-005 Amitha 30-8-2011
            

  //          public System.Web.UI.WebControls.FontUnit Footer_FontSize = FontUnit.Parse(ConfigurationManager.AppSettings["Footer_FontSize"]);

  //          public string Back_Color = ConfigurationManager.AppSettings["Back_Color"];
  //          public void EnableTriggerForMenu(int MenuPk, short FLAG = 1)
  //          {
  //              WorkFlow objwf = new WorkFlow();
  //              objwf.ExecuteScaler("SELECT MENU_WISE_ENABLE_TRIGGER(" + MenuPk + "," + FLAG + ") FROM DUAL");
  //          }

         


        //    public virtual void FillBizTypebyuser(ref DropDownList DDL, Int16 DropdownType = 0)
        //    {

        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        int Biztype = 0;
        //        WorkFlow objWF = new WorkFlow();
        //        DDL.Items.Clear();

        //        try
        //        {
        //            sb.Append("select c.biz_type from corporate_mst_tbl c ");
        //        Biztype = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
        //        }
        //        catch (Exception ex)
        //        {
        //        }

        //        if (Biztype == 3)
        //        {
        //            if (DropdownType == 0)
        //            {
        //                if (CurrentBusinessType == BusinessType.Air)
        //                {
        //                    DDL.Items.Add("Air");
        //                    DDL.Items[0].Value = 1;
        //                    DDL.SelectedIndex = 0;
        //                    DDL.Enabled = false;
        //                }
        //                else if (CurrentBusinessType == BusinessType.Sea)
        //                {
        //                    DDL.Items.Add("Sea");
        //                    DDL.Items[0].Value = 2;
        //                    DDL.SelectedIndex = 0;
        //                    DDL.Enabled = false;
        //                }
        //                else
        //                {
        //                    DDL.Items.Add("Air");
        //                    DDL.Items.Add("Sea");
        //                    DDL.Items.Add("Both");
        //                    DDL.Items[0].Value = 1;
        //                    DDL.Items[1].Value = 2;
        //                    DDL.Items[2].Value = 3;
        //                    DDL.SelectedIndex = 2;
        //                }
        //            }
        //            else if (DropdownType == 1)
        //            {
        //                if (CurrentBusinessType == BusinessType.Air)
        //                {
        //                    DDL.Items.Add("Air");
        //                    DDL.Items.Add("Both");
        //                    DDL.Items[0].Value = 1;
        //                    DDL.Items[1].Value = 3;
        //                    DDL.SelectedIndex = 0;
        //                }
        //                else if (CurrentBusinessType == BusinessType.Sea)
        //                {
        //                    DDL.Items.Add("Sea");
        //                    DDL.Items.Add("Both");
        //                    DDL.Items[0].Value = 2;
        //                    DDL.Items[1].Value = 3;
        //                    DDL.SelectedIndex = 0;
        //                }
        //                else
        //                {
        //                    DDL.Items.Add("Air");
        //                    DDL.Items.Add("Sea");
        //                    DDL.Items.Add("Both");
        //                    DDL.Items[0].Value = 1;
        //                    DDL.Items[1].Value = 2;
        //                    DDL.Items[2].Value = 3;
        //                    DDL.SelectedIndex = 2;
        //                }

        //            }
        //            else if (DropdownType == 2)
        //            {
        //                if (CurrentBusinessType == BusinessType.Air)
        //                {
        //                    DDL.Items.Add("Air");
        //                    DDL.Items[0].Value = 1;
        //                    DDL.SelectedIndex = 0;
        //                    DDL.Enabled = false;
        //                }
        //                else if (CurrentBusinessType == BusinessType.Sea)
        //                {
        //                    DDL.Items.Add("Sea");
        //                    DDL.Items[0].Value = 2;
        //                    DDL.SelectedIndex = 0;
        //                    DDL.Enabled = false;
        //                }
        //                else
        //                {
        //                    DDL.Items.Add("Air");
        //                    DDL.Items.Add("Sea");
        //                    DDL.Items[0].Value = 1;
        //                    DDL.Items[1].Value = 2;
        //                    DDL.SelectedIndex = 1;
        //                }

        //            }
        //        }
        //        else if (Biztype == 2)
        //        {
        //            DDL.Items.Add("Sea");
        //            DDL.Items[0].Value = 2;
        //            DDL.SelectedIndex = 0;
        //            DDL.Enabled = false;
        //        }
        //        else if (Biztype == 1)
        //        {
        //            DDL.Items.Add("Air");
        //            DDL.Items[0].Value = 1;
        //            DDL.SelectedIndex = 0;
        //            DDL.Enabled = false;
        //        }


        //    }
        //    //Created By akhilesh
        //    protected enum ValidationType
        //    {
        //        Valildate_Date = 0,
        //        Validate_PositiveNumber = 1,
        //        Validate_String = 2,
        //        Validate_Volume = 3,
        //        Validate_Weight = 4,
        //        Validate_Amount = 5,
        //        Validate_Exchange = 6
        //    }
        //    public void populateBusinessTypeSpecific(ref DropDownList DDL, Int32 chkRemoval = 0)
        //    {
        //        DDL.Items.Clear();
        //        //Commented and Modified by SuryaPrasad for implementing Removal concept

        //        if (CurrentBusinessType == BusinessType.Air)
        //        {
        //            DDL.Items.Add("Air");
        //            DDL.Items[0].Value = 1;
        //            DDL.SelectedIndex = 0;
        //            DDL.Enabled = false;
        //        }
        //        else if (CurrentBusinessType == BusinessType.Sea)
        //        {
        //            DDL.Items.Add("Sea");
        //            DDL.Items[0].Value = 2;
        //            DDL.SelectedIndex = 0;
        //            DDL.Enabled = false;
        //        }
        //        else
        //        {
        //            DDL.Items.Add("Air");
        //            DDL.Items.Add("Sea");
        //            DDL.Items[0].Value = 1;
        //            DDL.Items[1].Value = 2;
        //            DDL.SelectedIndex = 1;
        //        }
               
        //    }
        //    // Qfor 07 Sep 2006 [Akhilesh Yadav]
        //    // This Function will populate the Process Type
        //    public void populateProcess(ref object DDL)
        //    {
        //        if (DDL is System.Web.UI.WebControls.DropDownList)
        //        {
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Clear();
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Export");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Import");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[0].Value = "1";
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[1].Value = "2";
        //            ((System.Web.UI.WebControls.DropDownList)DDL).SelectedIndex = 0;
        //        }
        //        else if (DDL is System.Web.UI.HtmlControls.HtmlSelect)
        //        {
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items.Clear();
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items.Add("Export");
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items.Add("Import");
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items[0].Value = "1";
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items[1].Value = "2";
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).SelectedIndex = 0;
        //        }
        //    }
        //    //'Added by Ashish Arya for all process type
        //    public void PopulateAllProcess(ref object DDL, string ConfigID = "QFORCOMMON")
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        DataSet ds = new DataSet();
        //        WorkFlow objWF = new WorkFlow();
        //        DDL.Items.Clear();
                
        //        try
        //        {
        //            sb.Append("SELECT DD.DD_VALUE, DD.DD_ID");
        //            sb.Append("  FROM QFOR_DROP_DOWN_TBL DD");
        //            sb.Append(" WHERE DD.DD_FLAG = 'PROCESS_TYPE'");
        //            sb.Append("   AND DD.CONFIG_ID = '" + ConfigID + "'");
        //            sb.Append(" ORDER BY DD.DD_VALUE");
        //            ds = objWF.GetDataSet(sb.ToString());

        //            DDL.DataSource = ds;
        //            DDL.DataTextField = "DD_ID";
        //            DDL.DataValueField = "DD_VALUE";
        //            DDL.DataBind();

        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //    }
        //    //Added by shankar as on 08/04/2009
        //    //This Function will populate the Category Type
        //    public void PopulateCategory(ref object DDL)
        //    {
        //        if (DDL is System.Web.UI.WebControls.DropDownList)
        //        {
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Clear();
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Customer");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Agent");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[0].Value = "1";
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[1].Value = "2";
        //            ((System.Web.UI.WebControls.DropDownList)DDL).SelectedIndex = 0;
        //        }
        //        else if (DDL is System.Web.UI.HtmlControls.HtmlSelect)
        //        {
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items.Clear();
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items.Add("Customer");
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items.Add("Agent");
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items[0].Value = "1";
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items[1].Value = "2";
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).SelectedIndex = 0;
        //        }
        //    }
        //    //Ended by shankar as on 08/04/2009

        //    //This Function will populate the Category Type For Payables
        //    //Added by shankar as on 20/04/2009
        //    public void PopulateCategoryPayables(ref object DDL)
        //    {
        //        if (DDL is System.Web.UI.WebControls.DropDownList)
        //        {
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Clear();
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Customer");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Agent");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Vendor");
        //            //CType(DDL, System.Web.UI.WebControls.DropDownList).Items.Add("Shipper")
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[0].Value = "1";
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[1].Value = "2";
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[2].Value = "3";
        //            //CType(DDL, System.Web.UI.WebControls.DropDownList).Items(3).Value = "4"
        //            ((System.Web.UI.WebControls.DropDownList)DDL).SelectedIndex = 0;
        //        }
        //        else if (DDL is System.Web.UI.HtmlControls.HtmlSelect)
        //        {
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).Items.Clear();
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Customer");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Agent");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Vendor");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items.Add("Shipper");
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[0].Value = "1";
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[1].Value = "2";
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[2].Value = "3";
        //            ((System.Web.UI.WebControls.DropDownList)DDL).Items[3].Value = "4";
        //            ((System.Web.UI.HtmlControls.HtmlSelect)DDL).SelectedIndex = 0;
        //        }
        //    }
        //    //'Added By Koteshwari
        //    public virtual void populateAllBusinessType(ref DropDownList DDL, string ConfigID = "QFORCOMMON")
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        DataSet ds = new DataSet();
        //        WorkFlow objWF = new WorkFlow();
        //        DDL.Items.Clear();

        //        //If CurrentBusinessType = BusinessType.Air Then
        //        //    DDL.Items.Add("Air")
        //        //    DDL.Items(0).Value = 1
        //        //    DDL.SelectedIndex = 0
        //        //    DDL.Enabled = False
        //        //ElseIf CurrentBusinessType = BusinessType.Sea Then
        //        //    DDL.Items.Add("Sea")
        //        //    DDL.Items(0).Value = 2
        //        //    DDL.SelectedIndex = 0
        //        //    DDL.Enabled = False
        //        //Else
        //        //    DDL.Items.Add("Both")
        //        //    DDL.Items.Add("Air")
        //        //    DDL.Items.Add("Sea")
        //        //    DDL.Items(0).Value = 0
        //        //    DDL.Items(1).Value = 1
        //        //    DDL.Items(2).Value = 2
        //        //    DDL.SelectedIndex = 0
        //        //End If
        //        try
        //        {
        //            sb.Append("SELECT DD.DD_VALUE, DD.DD_ID");
        //            sb.Append("  FROM QFOR_DROP_DOWN_TBL DD");
        //            sb.Append(" WHERE DD.DD_FLAG = 'BIZ_TYPE'");
        //            sb.Append("   AND DD.CONFIG_ID = '" + ConfigID + "'");
        //            sb.Append(" ORDER BY DD.DD_VALUE");
        //            ds = objWF.GetDataSet(sb.ToString());

        //            DDL.DataSource = ds;
        //            DDL.DataTextField = "DD_ID";
        //            DDL.DataValueField = "DD_VALUE";
        //            DDL.DataBind();


        //            if (CurrentBusinessType == BusinessType.Air)
        //            {
        //                DDL.SelectedValue = 1;
        //                // DDL.Enabled = False
        //            }
        //            else if (CurrentBusinessType == BusinessType.Sea)
        //            {
        //                DDL.SelectedValue = 2;
        //                // DDL.Enabled = False
        //            }
        //            else
        //            {
        //                DDL.SelectedValue = 0;
        //            }

        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //    }
        //    //End
        //    // Qfor 21 Oct 2005 (Rajesh)
        //    // This Function will populate the Business Type DropDown
        //    //commented and modified by surya prasad on 03 - Jan - 2009
        //    public virtual void populateBusinessType(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        if (CurrentBusinessType == BusinessType.Air)
        //        {
        //            DDL.Items.Add("Air");
        //            DDL.Items[0].Value = 1;
        //            DDL.Items.Add("Both");
        //            DDL.Items[1].Value = 3;
        //            DDL.SelectedIndex = 0;
        //        }
        //        else if (CurrentBusinessType == BusinessType.Sea)
        //        {
        //            DDL.Items.Add("Sea");
        //            DDL.Items[0].Value = 2;
        //            DDL.Items.Add("Both");
        //            DDL.Items[1].Value = 3;
        //            DDL.SelectedIndex = 0;
        //        }
        //        else
        //        {
        //            DDL.Items.Add("Air");
        //            DDL.Items.Add("Sea");
        //            DDL.Items.Add("Both");
        //            DDL.Items[0].Value = 1;
        //            DDL.Items[1].Value = 2;
        //            DDL.Items[2].Value = 3;
        //            DDL.SelectedIndex = 2;
        //        }
        //        //If RemovalsBizType <> "1" Then
        //        //    If CurrentBusinessType = BusinessType.Air Then
        //        //        DDL.Items.Add("Air")
        //        //        DDL.Items(0).Value = 1
        //        //        DDL.Items.Add("Both")
        //        //        DDL.Items(1).Value = 3
        //        //        DDL.SelectedIndex = 0
        //        //    ElseIf CurrentBusinessType = BusinessType.Sea Then
        //        //        DDL.Items.Add("Sea")
        //        //        DDL.Items(0).Value = 2
        //        //        DDL.Items.Add("Both")
        //        //        DDL.Items(1).Value = 3
        //        //        DDL.SelectedIndex = 0
        //        //    Else
        //        //        DDL.Items.Add("Air")
        //        //        DDL.Items.Add("Sea")
        //        //        DDL.Items.Add("Both")
        //        //        DDL.Items(0).Value = 1
        //        //        DDL.Items(1).Value = 2
        //        //        DDL.Items(2).Value = 3
        //        //        DDL.SelectedIndex = 1
        //        //    End If

        //        //ElseIf RemovalsBizType = "1" Then
        //        //    If CurrentBusinessType = BusinessType.Air Then
        //        //        DDL.Items.Add("Air")
        //        //        DDL.Items(0).Value = 1
        //        //        DDL.Items.Add("Both")
        //        //        DDL.Items(1).Value = 3
        //        //        DDL.SelectedIndex = 0
        //        //    ElseIf CurrentBusinessType = BusinessType.Sea Then
        //        //        DDL.Items.Add("Sea")
        //        //        DDL.Items(0).Value = 2
        //        //        DDL.Items.Add("Both")
        //        //        DDL.Items(1).Value = 3
        //        //        DDL.SelectedIndex = 0
        //        //    ElseIf CurrentBusinessType = BusinessType.Removals Then
        //        //        DDL.Items.Add("Removals")
        //        //        DDL.Items(0).Value = 4
        //        //        DDL.SelectedIndex = 0
        //        //        DDL.Enabled = False
        //        //    Else
        //        //        DDL.Items.Add("Air")
        //        //        DDL.Items.Add("Sea")
        //        //        DDL.Items.Add("Both")
        //        //        DDL.Items.Add("Removals")
        //        //        DDL.Items(0).Value = 1
        //        //        DDL.Items(1).Value = 2
        //        //        DDL.Items(2).Value = 3
        //        //        DDL.Items(3).Value = 4 ' 1-Sea , 2 - Air , 3 - Both , 4 - Removals
        //        //        DDL.SelectedIndex = 1
        //        //    End If
        //        //End If
        //        //end
        //    }
        //    //adding by thiyagarajan on 10/2/09 : implementing logged loc. time from server's time using GMT
        //    public void GetLocalDate(ref Infragistics.WebUI.WebSchedule.WebDateChooser dtCtrl)
        //    {
        //        try
        //        {
        //            dtCtrl.Value = fun_getTimeZoneDateTime();
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    public DateTime fun_getTimeZoneDateTime()
        //    {
        //        try
        //        {
        //            // commented by uma
        //            //'Once time zone concept is implemented then needs to be implement
        //            ///'Dim User_BranchLoc_TZ_Diff, WebServer_TZ_Diff As String
        //            ///'Dim tmp_TZ1_str1, tmp_TZ2_str1 As String             ''' OPERATOR
        //            ///'Dim tmp_TZ1_str2, tmp_TZ2_str2 As String             ''' EXCLUDING OPERATOR
        //            ///'Dim tmp_TZ1_str3, tmp_TZ2_str3 As String             ''''' SEPARATOR
        //            ///'tmp_TZ1_str3 = ":"
        //            ///'tmp_TZ2_str3 = ":"
        //            ///'Dim tmp_TZ2_TIME() As String            ''  ' HOURSPART , MINS PART
        //            ///'Dim tmp_TZ1_TIME() As String
        //            ///'Dim dbl_HourMin, dbl_HourMin2 As Double
        //            DateTime dt_TZ1 = default(DateTime);
        //            DateTime dt_TZ2 = default(DateTime);
        //            DateTime Final_Datetime = default(DateTime);

        //            ///'WebServer_TZ_Diff = Session("WebServer_TZ_Diff")
        //            ///''step 1 : convert Server based Time to GMT 
        //            ///'tmp_TZ2_str1 = WebServer_TZ_Diff.ToString.Substring(0, 1)
        //            ///'tmp_TZ2_str2 = WebServer_TZ_Diff.ToString.Substring(1, WebServer_TZ_Diff.ToString.Length - 1)
        //            ///'tmp_TZ2_TIME = tmp_TZ2_str2.ToString.Split(tmp_TZ2_str3)

        //            ///'dbl_HourMin2 = Convert.ToDouble(tmp_TZ2_TIME(0)) * 60
        //            ///'dbl_HourMin2 += Convert.ToDouble(tmp_TZ2_TIME(1))

        //            ///'dbl_HourMin2 = CInt(tmp_TZ2_str1 & dbl_HourMin2)

        //            ///'If InStr(dbl_HourMin2, "-") <= 0 Then
        //            ///'    dt_TZ2 = DateAdd(DateInterval.Minute, 0 - dbl_HourMin2, System.DateTime.Now)
        //            ///'Else
        //            ///'    dt_TZ2 = DateAdd(DateInterval.Minute, dbl_HourMin2, System.DateTime.Now)
        //            ///'End If

        //            ///'User_BranchLoc_TZ_Diff = Session("USER_BRANCH_LOC_TZ_DIFF")
        //            ///''step 2: convert  Location based Time from the  GMT
        //            ///'tmp_TZ1_str1 = User_BranchLoc_TZ_Diff.ToString.Substring(0, 1)
        //            ///'tmp_TZ1_str2 = User_BranchLoc_TZ_Diff.ToString.Substring(1, User_BranchLoc_TZ_Diff.ToString.Length - 1)
        //            ///'tmp_TZ1_TIME = tmp_TZ1_str2.ToString.Split(tmp_TZ1_str3)

        //            ///'dbl_HourMin = Convert.ToDouble(tmp_TZ1_TIME(0)) * 60
        //            ///'dbl_HourMin += Convert.ToDouble(tmp_TZ1_TIME(1))
        //            ///'dbl_HourMin = CInt(tmp_TZ1_str1 & dbl_HourMin)
        //            ///'Final_Datetime = DateAdd(DateInterval.Minute, dbl_HourMin, dt_TZ2)
        //            Final_Datetime = System.DateTime.Now;
        //            return Final_Datetime;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    public string fn_ConvertToDateTime(System.DateTime dtValue, bool Time_Flag = true)
        //    {
        //        string str_edit_date_format = null;
        //        string str_server_date_format = null;
        //        Int32 numDD = default(Int32);
        //        Int32 numMM = default(Int32);
        //        Int32 numYYYY = default(Int32);

        //        //str_edit_date_format = Session("DISPLAY_EDIT_DATE_FORMAT")
        //        str_server_date_format = dateFormat;
        //        numDD = DateAndTime.Day(dtValue);
        //        numMM = DateAndTime.Month(dtValue);
        //        numYYYY = DateAndTime.Year(dtValue);

        //        string str_CurrentDt_time = null;
        //        str_CurrentDt_time = fun_getTimeZoneDateTime().ToLongTimeString();
        //        // System.DateTime.Now.ToLongTimeString
        //        ///'''''
        //        if (Time_Flag == true)
        //        {
        //            if (numDD == 1 & numMM == 1 & numYYYY == 1)
        //            {
        //                return "01/01/1901 " + System.DateTime.Now.ToLongTimeString();
        //            }
        //            if (str_server_date_format.ToUpper() == "DD/MM/YYYY")
        //            {
        //                return numDD.ToString() + "/" + numMM.ToString() + "/" + numYYYY.ToString() + " " + str_CurrentDt_time;
        //                //System.DateTime.Now.ToLongTimeString
        //            }
        //            else
        //            {
        //                return numMM.ToString() + "/" + numDD.ToString() + "/" + numYYYY.ToString() + " " + str_CurrentDt_time;
        //                //System.DateTime.Now.ToLongTimeString
        //            }
        //        }
        //        else
        //        {
        //            if (numDD == 1 & numMM == 1 & numYYYY == 1)
        //            {
        //                return "01/01/1901";
        //            }
        //            if (str_server_date_format.ToUpper() == "DD/MM/YYYY")
        //            {
        //                return numDD.ToString() + "/" + numMM.ToString() + "/" + numYYYY.ToString();
        //            }
        //            else
        //            {
        //                return numMM.ToString() + "/" + numDD.ToString() + "/" + numYYYY.ToString();
        //            }
        //        }
        //    }
        //    //end by thiyagrajan on 11/2/09
        //    public virtual void populateBusinessType_Customer(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        //1-Sea , 2 - Air , 3 - Both , 4 - Removals 
        //        if (CurrentBusinessType == BusinessType.Air)
        //        {
        //            DDL.Items.Add(new ListItem("Air", "1"));
        //            DDL.Items.Add(new ListItem("Both", "3"));
        //            DDL.SelectedIndex = 0;
        //        }
        //        else if (CurrentBusinessType == BusinessType.Sea)
        //        {
        //            DDL.Items.Add(new ListItem("Sea", "2"));
        //            DDL.Items.Add(new ListItem("Both", "3"));
        //            DDL.SelectedIndex = 0;
        //        }
        //        else if (CurrentBusinessType == BusinessType.Removals)
        //        {
        //            DDL.Items.Add(new ListItem("Removals", "4"));
        //            DDL.SelectedIndex = 0;
        //            DDL.Enabled = false;
        //        }
        //        else
        //        {
        //            DDL.Items.Add(new ListItem("Air", "1"));
        //            DDL.Items.Add(new ListItem("Sea", "2"));
        //            DDL.Items.Add(new ListItem("Both", "3"));
        //            //DDL.Items.Add(New ListItem("Removals","4")) ''Commented By Koteshwari on 1-Jun-2011
        //            DDL.SelectedIndex = 2;
        //        }
        //    }

        //    //Added By Prakash Chandra
        //    public virtual void populateStatusSeaAir(ref DropDownList DDL, string status)
        //    {
        //        DDL.Items.Clear();
        //        //Code changed by Ashish Arya on 29th Sept 2011 to remove the carted-cargo value from ddl, 
        //        //and added listitem instead of adding text and value separately
        //        if (status == "-1")
        //        {
        //            DDL.Items.Add(new ListItem("Nominated", "-1"));
        //            DDL.Items.Add(new ListItem("Provisional", "1"));
        //            DDL.Items.Add(new ListItem("Confirm", "2"));
        //            DDL.Items.Add(new ListItem("Cancelled", "3"));
        //            //DDL.Items.Add(New ListItem("CartedCargo", "5"))
        //        }
        //        if (status == "1")
        //        {
        //            DDL.Items.Add(new ListItem("Provisional", "1"));
        //            DDL.Items.Add(new ListItem("Confirm", "2"));
        //            DDL.Items.Add(new ListItem("Cancelled", "3"));
        //            //DDL.Items.Add(New ListItem("CartedCargo", "5"))
        //        }
        //        if (status == "2")
        //        {
        //            DDL.Items.Add(new ListItem("Confirm", "2"));
        //            DDL.Items.Add(new ListItem("Cancelled", "3"));
        //        }
        //        if (status == "3")
        //        {
        //            DDL.Items.Add(new ListItem("Cancelled", "3"));
        //        }
        //        if (status == "5")
        //        {
        //            DDL.Items.Add(new ListItem("Confirm", "2"));
        //            DDL.Items.Add(new ListItem("Cancelled", "3"));
        //            //DDL.Items.Add(New ListItem("CartedCargo", "5"))
        //        }
        //        //'Added By Koteshwari on 26/5/2011
        //        if (status == "6")
        //        {
        //            DDL.Items.Add(new ListItem("Shipped", "6"));
        //            DDL.Items.Add(new ListItem("Cancelled", "3"));
        //        }
        //        //If the ddl does not contains the status value, 
        //        //to ensure this loop is added by Ashish Arya on 29th Sept 2011
        //        foreach (ListItem itm in DDL.Items)
        //        {
        //            if (itm.Value == status)
        //            {
        //                DDL.SelectedValue = status;
        //                return;
        //            }
        //        }
        //    }
        //    // Qfor 21 Oct 2005 (Rajesh)
        //    public enum Salutation
        //    {
        //        // enums used for salutation
        //        Mr = 1,
        //        Miss = 2
        //    }
        //    // Qfor 21 Oct 2005 (Rajesh)
        //    // This Function will populate the Salutation DropDown
        //    public void populateSalutation(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("");
        //        DDL.Items.Add("Mr.");
        //        DDL.Items.Add("Ms.");
        //        DDL.Items[0].Value = 0;
        //        DDL.Items[1].Value = 1;
        //        DDL.Items[2].Value = 2;
        //        DDL.SelectedIndex = 1;
        //    }

        //    public enum BookingStatus
        //    {
        //        //enums used in most of the Booking Master/Transaction
        //        Active = 1,
        //        Confirm = 2,
        //        Cancelled = 3
        //    }

        //    public enum formatText
        //    {
        //        Amount = 1,
        //        Weight = 2,
        //        Volume = 3,
        //        Exchange = 4
        //    }
        //    //This Function will populate the Booking Status in DropDown
        //    protected internal virtual void populateBookingStatus(ref DropDownList DDL)
        //    {
        //        //Modified by Ashish Arya on 28th Sept 2011, added listitem in ddl instead of adding text and value separately
        //        DDL.Items.Clear();
        //        //DDL.Items.Add("Active")
        //        DDL.Items.Add(new ListItem("Provisional", "1"));
        //        //Added By Snigdharani - 12/08/2008
        //        DDL.Items.Add(new ListItem("Confirm", "2"));
        //        DDL.Items.Add(new ListItem("Cancelled", "3"));
        //        //DDL.Items.Add(New ListItem("CartedCargo", "5"))
        //        DDL.Items.Add(new ListItem("Shipped", "6"));
        //        //Added By Koteshwari
        //        DDL.Items.Add(new ListItem("All", "7"));
        //        //Manjunath
        //    }

        //    //This Function will populate the Payment Types in DropDown
        //    public enum PaymentType
        //    {
        //        //enums used in most of the Booking Master/Transaction
        //        Prepaid = 1,
        //        Collect = 2
        //    }
        //    //This Function will populate the Payment Types in DropDown
        //    public void populatePaymentType(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("PrePaid");
        //        DDL.Items.Add("Collect");
        //        DDL.Items[0].Value = 1;
        //        DDL.Items[1].Value = 2;
        //    }
        //    public void PopulatePaymenTerms(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("PrePaid");
        //        DDL.Items.Add("Collect");
        //        DDL.Items.Add("Foreign");
        //        DDL.Items[0].Value = 1;
        //        DDL.Items[1].Value = 2;
        //        DDL.Items[2].Value = 3;
        //    }
        //    // Rajesh ( 28-Jan-2006 )
        //    public void populatePaymentType(ref Infragistics.WebUI.UltraWebGrid.ValueList UWGList)
        //    {
        //        UWGList.ValueListItems.Clear();
        //        UWGList.ValueListItems.Add(1, "PrePaid");
        //        UWGList.ValueListItems.Add(2, "Collect");
        //    }
        //    public void PopulatePaymenTerms(ref Infragistics.WebUI.UltraWebGrid.ValueList UWGList)
        //    {
        //        UWGList.ValueListItems.Clear();
        //        UWGList.ValueListItems.Add(1, "PrePaid");
        //        UWGList.ValueListItems.Add(2, "Collect");
        //        UWGList.ValueListItems.Add(3, "Foreign");
        //    }
        //    //Nippy (22/03/2006)
        //    public void populateChargeBasis(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("");
        //        DDL.Items.Add("%");
        //        DDL.Items.Add("Flat Rate");
        //        DDL.Items.Add("Kgs");
        //        DDL.Items.Add("Unit");
        //        DDL.Items[0].Value = 0;
        //        DDL.Items[1].Value = 1;
        //        DDL.Items[2].Value = 2;
        //        DDL.Items[3].Value = 3;
        //        DDL.Items[4].Value = 4;
        //    }
        //    //Nippy (22/03/2006)
        //    public void populateChargeBasis(ref Infragistics.WebUI.UltraWebGrid.ValueList UWGList)
        //    {
        //        UWGList.ValueListItems.Clear();
        //        UWGList.ValueListItems.Add(Convert.ToInt32(0), " ");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(1), "%");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(2), "Flat Rate");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(3), "Kgs");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(4), "Unit");
        //    }
        //    //Added by Faheem
        //    public void NewPopulateChargeBasis(ref Infragistics.WebUI.UltraWebGrid.ValueList UWGList)
        //    {
        //        UWGList.ValueListItems.Clear();
        //        UWGList.ValueListItems.Add(Convert.ToInt32(1), "%");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(2), "Flat Rate");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(3), "Kgs");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(4), "Unit");
        //    }
        //    //End
        //    //Nippy (22/03/2006)
        //    public void populateApplicable(ref Infragistics.WebUI.UltraWebGrid.ValueList UWGList)
        //    {
        //        UWGList.ValueListItems.Clear();
        //        UWGList.ValueListItems.Add(Convert.ToInt32(0), " ");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(1), "BOF");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(2), "PHL");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(3), "PHD");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(4), "MDO");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(5), "OTH");
        //    }
        //    //Nippy (22/03/2006) This Function will populate the Freight Type DropDownList
        //    public void populateFreightType(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("");
        //        DDL.Items.Add("Freight");
        //        DDL.Items.Add("Operator");
        //        DDL.Items.Add("Trade");
        //        DDL.Items[0].Value = 0;
        //        DDL.Items[1].Value = 1;
        //        DDL.Items[2].Value = 2;
        //        DDL.Items[3].Value = 3;
        //    }
        //    public void populateCreditDebit(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("Cr.");
        //        DDL.Items.Add("Dr.");
        //        DDL.Items[0].Value = 0;
        //        DDL.Items[1].Value = 1;
        //    }
        //    public void populateFreightType1(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("All");
        //        DDL.Items.Add("Freight");
        //        DDL.Items.Add("Operator");
        //        DDL.Items.Add("Trade");
        //        DDL.Items[0].Value = 0;
        //        DDL.Items[1].Value = 1;
        //        DDL.Items[2].Value = 2;
        //        DDL.Items[3].Value = 3;
        //    }
        //    public void FillFreightType(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("");
        //        DDL.Items.Add("Freight");
        //        DDL.Items.Add("Surcharge");
        //        DDL.Items.Add("Local");
        //        DDL.Items[0].Value = 0;
        //        DDL.Items[1].Value = 1;
        //        DDL.Items[2].Value = 2;
        //        DDL.Items[3].Value = 3;
        //    }
        //    //Nippy (22/03/2006) This Function will populate the Freight Type Valuelist
        //    public void populateFreightType(ref Infragistics.WebUI.UltraWebGrid.ValueList UWGList)
        //    {
        //        UWGList.ValueListItems.Clear();
        //        UWGList.ValueListItems.Add(Convert.ToInt32(0), " ");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(1), "Freight");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(2), "Operator");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(3), "Trade");
        //    }
        //    //Added By Prakash Chandra on 24/06/2008
        //    public void populateCreditDebit(ref Infragistics.WebUI.UltraWebGrid.ValueList UWGList)
        //    {
        //        UWGList.ValueListItems.Clear();
        //        UWGList.ValueListItems.Add(Convert.ToInt32(0), "Cr");
        //        UWGList.ValueListItems.Add(Convert.ToInt32(1), "Dr");
        //    }
        //    //Enum & Function to Populate the CSV Category Types in DropDown
        //    public enum CSVCategory
        //    {
        //        Customer = 1,
        //        Agent = 2,
        //        Supplier = 3
        //    }

        //    //This Function will populate the CSV Category Types in DropDown
        //    public void populateCSVCategory(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("Customer");
        //        DDL.Items.Add("Agent");
        //        DDL.Items.Add("Supplier");
        //        DDL.Items[0].Value = 1;
        //        DDL.Items[1].Value = 2;
        //        DDL.Items[2].Value = 3;
        //    }
        //    //07-03-2006   'Rijesh
        //    public void populateTariffType(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("Shipping Line");
        //        DDL.Items.Add("General");
        //        DDL.Items.Add("Agent");
        //        DDL.Items.Add("Channel Partner");
        //        DDL.Items[0].Value = 1;
        //        DDL.Items[1].Value = 2;
        //        DDL.Items[2].Value = 3;
        //        DDL.Items[3].Value = 4;
        //    }
        //    //Added by SuryaPrasad on 2-Jan-09 for imolementing Removals Task
        //    public void populateCustType(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("Individual");
        //        DDL.Items.Add("Group");
        //        DDL.Items[0].Value = 1;
        //        DDL.Items[1].Value = 0;
        //        DDL.SelectedValue = 1;
        //    }
        //    public enum ACCESSRIGHTS
        //    {
        //        View = 1,
        //        Add = 2,
        //        Edit = 4,
        //        Delete = 8,
        //        Print = 16,
        //        ExportReport = 32,
        //        EMailReport = 64,
        //        CreateBaplie = 128,
        //        AddCustomer = 256,
        //        AssignTrade = 512,
        //        CostGroup = 1024,
        //        Designation = 2048,
        //        SucceedingMoves = 4096,
        //        Apply = 8192,
        //        BusinessPlan = 16384,
        //        Summary = 32768
        //    }
        //    #region "Function to check whether a user is an administrator or not"
        //    public int IsAdministrator(string strUserID)
        //    {
        //        string strSQL = null;
        //        Int16 Admin = default(Int16);
        //        WorkFlow objWF = new WorkFlow();
        //        strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
        //        strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
        //        strSQL = strSQL + "AND U.USER_MST_PK = " + Session["USER_PK"];
        //        try
        //        {
        //            Admin = objWF.ExecuteScaler(strSQL.ToString());
        //            if (Admin == 1)
        //            {
        //                return 1;
        //            }
        //            else
        //            {
        //                return 0;
        //            }

        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }

        //    }
        //    #endregion
        //    public void SetStyleSheet()
        //    {
        //        try
        //        {
        //            if (string.IsNullOrEmpty(Convert.ToString(Session["STYLESHEET"])) | (Session["STYLESHEET"] == null))
        //            {
        //                strStyleSheet = "../../Supports/CSS/StyleWGrid_7.css";
        //            }
        //            else
        //            {
        //                strStyleSheet = "../../Supports/CSS/" + Session["STYLESHEET"].ToString();
        //            }

        //            strCommonFunctionPath = "<script src=\"../../Supports/Scripts/CommonFunction.js\" type=\"text/javascript\"></script><script type=\"text/javascript\" src=\"../../Supports/Scripts/jquery-1.7.2.min.js\"></script>";
        //            strCommonGridFunctionPath = "<script src=\"../../Supports/Scripts/CommonInfraGRidFunction.js\" type=\"text/javascript\"></script>";
        //            strVBCommonFunctionPath = "<script src=\"../../Supports/Scripts/VBCommonFn.vbs\" type=\"text/vbscript\"></script>";
        //            strInvalidLoginRedirect = "~/Forms/03Security/frmOTHInvalidAccess.aspx";
        //        }
        //        catch (Exception ex)
        //        {
        //            strStyleSheet = "../../Supports/CSS/StyleSheet.css";
        //        }
        //    }
        //    //THIS FUNCTION IS USED TO READ THE EXCEL FILE AND BIND THE DATA TO THE GRID AND RETURN A DATASET OF ALL THE COLUMNS OF THE EXCEL SHEET
        //    public DataSet GetExcel_GetIn_Data(string sFileName)
        //    {
        //        System.Data.OleDb.OleDbConnection con = new System.Data.OleDb.OleDbConnection();
        //        System.Data.OleDb.OleDbCommand cmd = null;
        //        System.Data.OleDb.OleDbDataAdapter objDA = new System.Data.OleDb.OleDbDataAdapter();
        //        DataSet objDS = null;
        //        string SQLQuery = "select * from [sheet1$]";
        //        try
        //        {
        //            con.ConnectionString = "Provider=microsoft.jet.oledb.4.0;Data source=" + sFileName + ";Extended Properties=Excel 8.0";
        //            con.Open();
        //            cmd = new System.Data.OleDb.OleDbCommand(SQLQuery, con);
        //            cmd.CommandType = CommandType.Text;
        //            cmd.CommandText = SQLQuery;
        //            objDA.SelectCommand = cmd;
        //            objDA.Fill(objDS);
        //            return objDS;
        //        }
        //        catch (Exception ex)
        //        {
        //            Server.Transfer("~/Forms/03Security/frmOTHInvalidAccess.aspx");
        //            //Throw ex
        //        }
        //    }
        //    public object GeneralSearch(string sArgPageName, string sArgCtrlName, string sArgSQL)
        //    {
        //        string strScript = null;
        //        string sWinOpn = null;
        //        string f = null;
        //        f = sArgPageName + " &CtrlName = " + sArgCtrlName + " &sql = " + sArgSQL;
        //        //f = "frmDepartmentEntry &CtrlName = txtDepartment_ID~txtDepartment_NAME &sql = select department_id as !!Department^^ID!!, department_name as !!Department^^Name!!  from department_mst_tbl"
        //        //sWinOpn = "ShowDialog('frmSearch.aspx?FormName='" + sArgPageName + "' &CtrlName = '" + sArgCtrlName + "' &sql = '" + sArgSQL + "','Search','height=500, width=700,status= no, resizable= no, scrollbars=no, toolbar=no,location=no,menubar=no');"
        //        //sArgPageName + " &CtrlName = " + sArgCtrlName + " &sql = " + sArgSQL
        //        //f = "frmDepartmentEntry &CtrlName = txtDepartment_ID~txtDepartment_NAME &sql = select department_id as"
        //        //,'Search','height=500, width=800,status= no, resizable= yes, scrollbars=yes, toolbar=no,location=no,menubar=no'
        //        //window.open('frmSearch.aspx?FormName=' + document.forms[0].name + '&CtrlName=' + CtrlName + '&sql=' + sql ,'Search','height=500, width=700,status= no, resizable= no, scrollbars=no, toolbar=no,location=no,menubar=no');				
        //        //sWinOpn = "ShowDialog('frmSearch.aspx?FormName=" & f & "')"
        //        //sWinOpn = "window.open('frmSearch.aspx?FormName=" & f & ",'Search','height=500, 'width=700,status= no, resizable= no, scrollbars=no, toolbar=no,location=no,menubar=no');"
        //        sWinOpn = "window.open('frmSearch.aspx?FormName=" + f + "');";
        //        //sWinOpn = "window.open('frmSearch.aspx');"
        //        strScript = "<html><body onLoad=\"javascript:" + sWinOpn + ";\"></body></html>";
        //        Response.Write(strScript);
        //    }
        //    public object FunctionForJavaSrcipt(string s)
        //    {
        //        ///Example...Write like below in your VB Part Of Form-------------
        //        //Call FunctionForJavaSrcipt("SetFocus('" & txtStateID.ID & "')")
        //        ///---------------------------------------------------------------
        //        string strScript = null;
        //        strScript = "<html><body onLoad=\"javascript:" + s + ";\"></body></html>";
        //        Response.Write(strScript);
        //    }
        //    public string GetConnectionString()
        //    {
        //        return ConfigurationManager.AppSettings["ConnectionString"];
        //    }
        //    public static string GetCookieValue(string CookieName, HttpRequest objRequest)
        //    {
        //        if ((objRequest.Cookies[CookieName] != null))
        //        {
        //            return objRequest.Cookies[CookieName].Value();
        //        }
        //        else
        //        {
        //            return "";
        //        }
        //    }
        //    public static object CreateCookies(string CookieName, string CookieValue, HttpResponse objResponse, HttpRequest objRequest)
        //    {
        //        HttpCookie objCookies = null;
        //        if ((objRequest.Cookies[CookieName] != null))
        //        {
        //            objCookies = objRequest.Cookies[CookieName];
        //        }
        //        else
        //        {
        //            objCookies = new HttpCookie(CookieName);
        //        }
        //        var _with1 = objCookies;
        //        _with1.Value = CookieValue;
        //        _with1.Expires = System.DateTime.Now.AddDays(Convert.ToInt32(ConfigurationManager.AppSettings["CookieExpireDays"]));
        //        objResponse.Cookies.Add(objCookies);
        //    }
        //    public object GetErrorMessage(int ErrorCode, string ExtraMessage = "", ExtraMessagePosition PositionOfExtraMessage = ExtraMessagePosition.AfterMainMessage, string FormName = "", string EventName = "")
        //    {
        //        WorkFlow objwk = new WorkFlow();
        //        try
        //        {
        //            XmlDocument xmlDOC = new XmlDocument();
        //            System.Web.UI.Page objPage = new System.Web.UI.Page();
        //            string strOutput = null;
        //            string strpath = null;
        //            string strSQL = null;

        //            strSQL = "select Qm.Err_Msg_Code,Qm.Category,Qm.Message_Type, qm.client_or_server , qm.message";
        //            if ((Session["ENVIRONMENT_PK"] == null) | Session["ENVIRONMENT_PK"] == 0)
        //            {
        //                strSQL += Constants.vbCrLf + " from qflx_messages_tbl Qm where qm.environment_fk =1";
        //            }
        //            else
        //            {
        //                strSQL += Constants.vbCrLf + " from qflx_messages_tbl Qm where qm.environment_fk = " + Session["ENVIRONMENT_PK"];
        //            }
        //            strSQL += Constants.vbCrLf + " and qm.err_msg_code ='" + ErrorCode + "'";


        //            OracleDataReader DR = objwk.GetDataReader(strSQL);
        //            if (DR.Read())
        //            {
        //                return DR["message"];
        //            }


        //            //strpath = Session("ROOTPATH") & "\Supports\Errors\ErrorMessage.xml"
        //            //objPage.Dispose()
        //            //xmlDOC.Load(strpath)
        //            //Dim xmlNode As XmlNodeList
        //            //xmlNode = xmlDOC.GetElementsByTagName("Error")
        //            //Dim intI As Integer
        //            //For intI = 0 To xmlNode.Count
        //            //    Dim xNode As XmlNode
        //            //    xNode = xmlNode.Item(intI)
        //            //    If ErrorCode = CType(xNode.Attributes(0).InnerText, Integer) Then
        //            //        strOutput = ErrorCode & " : " & xNode.InnerText
        //            //        Exit For
        //            //    End If
        //            //Next
        //            // changed by SUMIT
        //            //Dim xElem As XmlElement = xmlDOC.GetElementById(ErrorCode)
        //            //strOutput = ErrorCode & " : " & xElem.InnerText

        //            //If FormName <> "" And EventName <> "" Then
        //            //    strOutput = "Error From : " & FormName & vbCrLf & " On Event : " & EventName & vbCrLf & strOutput
        //            //End If
        //            //If PositionOfExtraMessage = ExtraMessagePosition.AfterMainMessage Then
        //            //    strOutput = strOutput & ExtraMessage
        //            //Else
        //            //    strOutput = ExtraMessage & strOutput
        //            //End If
        //            //Return strOutput
        //        }
        //        catch (Exception ex)
        //        {
        //            return ex.Message;
        //        }
        //        finally
        //        {
        //            objwk.CloseConnection();
        //        }
        //    }
        //    public static int GetCodeForOracleMessage(string Message)
        //    {
        //        if (Strings.InStr(Message, "ORA") > 0)
        //        {
        //            return "900" + Strings.Mid(Message, 5, 5);
        //        }
        //        switch ("900" + Strings.Mid(Message, 5, 5))
        //        {
        //            case "00001":
        //                return 1502;
        //            case "02292":
        //                return 1503;
        //            case "00002":
        //                return 1504;
        //            case "20997":
        //                return 1505;
        //            // Master Child Delete Violation
        //            case "20998":
        //                return 1507;
        //            case "20999":
        //                return 1506;
        //            case "01400":
        //            case "01407":
        //                return 9011400;
        //            default:
        //                return 100;
        //        }
        //    }
        //    //Public Function CheckForValidAccess(ByVal Request As HttpRequest) As Boolean
        //    //    Dim RemoteIp As String
        //    //    Dim objPage As New System.Web.UI.Page
        //    //    Try
        //    //        If objPage.Session("USER_PK") = "" Then
        //    //            Return False
        //    //            'objPage.Server.Transfer(objPage.Application("ROOTPATH") & "frmLogin.aspx")
        //    //        Else
        //    //            RemoteIp = Cache.Item(Session("USER_ID").ToString & "," & Session.SessionID)
        //    //            Return True
        //    //        End If
        //    //        'strTest = Request.UrlReferrer.ToString
        //    //        'If InStr(strTest, "frmLogin.aspx") > 0 Then
        //    //        'End If
        //    //        'Request.MapPath(Request.Path.ToString())
        //    //    Catch ex As Exception
        //    //        Return False
        //    //    End Try
        //    //End Function

        //    protected bool CheckForValidAccess(HttpRequest Request)
        //    {
        //        string RemoteIp = null;
        //        System.Web.UI.Page objPage = new System.Web.UI.Page();
        //        try
        //        {
        //            if (string.IsNullOrEmpty(objPage.Session["USER_PK"].ToString()))
        //            {
        //                return false;
        //            }
        //            else
        //            {
        //                return true;
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            return false;
        //        }
        //    }

        //    public static void ClearALLControls(Control oControl)
        //    {
        //        Control frmCtrl = null;
        //        foreach (Control frmCtrl_loopVariable in oControl.Controls)
        //        {
        //            frmCtrl = frmCtrl_loopVariable;
        //            if (frmCtrl is TextBox)
        //            {
        //                ((TextBox)frmCtrl).Text = "";
        //                //DirectCast(frmCtrl, TextBox).Enabled = True
        //            }
        //            else if (frmCtrl is ListBox)
        //            {
        //                ((ListBox)frmCtrl).ClearSelection();
        //            }
        //            else if (frmCtrl is DropDownList)
        //            {
        //                ((DropDownList)frmCtrl).ClearSelection();
        //            }
        //            else if (frmCtrl is Button)
        //            {
        //                //DirectCast(frmCtrl, Button).Enabled = True
        //            }
        //            if (frmCtrl.HasControls())
        //            {
        //                ClearALLControls(frmCtrl);
        //            }
        //        }
        //    }
        //    public static void Enabled_Controls(Page MyPage)
        //    {
        //        object objCTL = null;
        //        Int16 intI = default(Int16);
        //        foreach (object objCTL_loopVariable in MyPage.Controls)
        //        {
        //            objCTL = objCTL_loopVariable;
        //            if (objCTL is System.Web.UI.HtmlControls.HtmlForm)
        //            {
        //                objCTL = MyPage.Controls[intI];
        //                object objCtls = null;
        //                foreach (object objCtls_loopVariable in MyPage.Controls[intI].Controls)
        //                {
        //                    objCtls = objCtls_loopVariable;
        //                    if (objCtls is System.Web.UI.WebControls.DropDownList)
        //                    {
        //                        ((System.Web.UI.WebControls.DropDownList)objCtls).Enabled = true;
        //                    }
        //                    else if (objCtls is System.Web.UI.WebControls.TextBox)
        //                    {
        //                        ((System.Web.UI.WebControls.TextBox)objCtls).Enabled = true;
        //                    }
        //                    else if (objCtls is System.Web.UI.WebControls.DataGrid)
        //                    {
        //                        ((System.Web.UI.WebControls.DataGrid)objCtls).Enabled = true;
        //                    }
        //                    else if (objCtls is System.Web.UI.WebControls.Button)
        //                    {
        //                        ((System.Web.UI.WebControls.Button)objCtls).Enabled = true;
        //                    }
        //                }
        //            }
        //            intI += 1;
        //        }
        //    }
        //    public static void Disabled_Controls(Page MyPage)
        //    {
        //        object objCTL = null;
        //        Int16 intI = default(Int16);
        //        foreach (object objCTL_loopVariable in MyPage.Controls)
        //        {
        //            objCTL = objCTL_loopVariable;
        //            if (objCTL is System.Web.UI.HtmlControls.HtmlForm)
        //            {
        //                objCTL = MyPage.Controls[intI];
        //                object objCtls = null;
        //                foreach (object objCtls_loopVariable in MyPage.Controls[intI].Controls)
        //                {
        //                    objCtls = objCtls_loopVariable;
        //                    if (objCtls is System.Web.UI.WebControls.DropDownList)
        //                    {
        //                        ((System.Web.UI.WebControls.DropDownList)objCtls).Enabled = false;
        //                    }
        //                    else if (objCtls is System.Web.UI.WebControls.TextBox)
        //                    {
        //                        ((System.Web.UI.WebControls.TextBox)objCtls).Enabled = false;
        //                    }
        //                    else if (objCtls is System.Web.UI.WebControls.DataGrid)
        //                    {
        //                        ((System.Web.UI.WebControls.DataGrid)objCtls).Enabled = false;
        //                    }
        //                    else if (objCtls is System.Web.UI.WebControls.Button)
        //                    {
        //                        ((System.Web.UI.WebControls.Button)objCtls).Enabled = false;
        //                    }
        //                }

        //            }
        //            intI += 1;
        //        }
        //    }
        //    public string FetchScriptMessges(Page MyPage, string ConfigurationID)
        //    {
        //        string strSQL = null;
        //        try
        //        {
        //            WorkFlow objWK = new WorkFlow();
        //            OracleDataReader dr = null;
        //            string strMessage = null;
        //            strSQL = strSQL + Constants.vbCrLf + " select qm.err_msg_code,qm.message";
        //            strSQL = strSQL + Constants.vbCrLf + " from qflx_messages_tbl qm , config_mst_tbl c";
        //            strSQL = strSQL + Constants.vbCrLf + " where qm.config_mst_fk = c.config_mst_pk ";
        //            strSQL = strSQL + Constants.vbCrLf + " and qm.environment_fk = 1";
        //            strSQL = strSQL + Constants.vbCrLf + " and qm.client_or_server=1";
        //            strSQL = strSQL + Constants.vbCrLf + " and c.config_id = '" + ConfigurationID + "'";
        //            dr = objWK.GetDataReader(strSQL);
        //            strMessage = "<Errors>";
        //            while (dr.Read())
        //            {
        //                strMessage += "<Error ID=\"" + dr["err_msg_code"] + "\">" + dr["message"] + "</Error>";
        //            }
        //            strMessage += "</Errors>";
        //            dr.Close();
        //            return strMessage;
        //        }
        //        catch (Exception EX)
        //        {
        //            throw EX;
        //        }
        //    }
        //    #region "Method For User Activity Tracking"
        //    public object fn_UserActivityTracking_ProcCall(string ConfigurationID, string SESSION_ID, DateTime VISITED_TIME_IN)
        //    {
        //        cls_UserActivity obj = new cls_UserActivity();
        //        obj.fn_UserActivityTracking_ProcCall(ConfigurationID, SESSION_ID, VISITED_TIME_IN, Session["USER_PK"]);
        //    }
        //    #endregion
        //    #region "Fetch Control Settings"
        //    public void DateControlUIFormatting(Infragistics.WebUI.WebSchedule.WebDateChooser WDC)
        //    {
        //        var _with2 = WDC;
        //        _with2.BorderStyle = BorderStyle.None;
        //        var _with3 = _with2.EditStyle;
        //        _with3.Height = Unit.Pixel(17);
        //        _with3.BorderWidth = Unit.Pixel(1);
        //        _with3.BorderColor = Color.SteelBlue;
        //        _with3.BorderStyle = BorderStyle.Solid;
        //        var _with4 = _with3.BorderDetails;
        //        _with4.StyleBottom = BorderStyle.Solid;
        //        _with4.StyleTop = BorderStyle.Solid;
        //        _with4.StyleRight = BorderStyle.None;
        //        _with4.StyleLeft = BorderStyle.Solid;
        //        _with2.CalendarLayout.FooterFormat = "Today: {0:d}";
        //        var _with5 = _with2.DropButton;
        //        _with5.ImageUrl1 = null;
        //        _with5.ImageUrl2 = null;
        //        var _with6 = _with5.HoverStyle;
        //        _with6.Width = Unit.Pixel(17);
        //        _with6.Height = Unit.Pixel(15);
        //        _with6.BorderWidth = Unit.Pixel(1);
        //        _with6.BorderColor = Color.SteelBlue;
        //        _with6.BorderStyle = BorderStyle.Solid;
        //        var _with7 = _with6.BorderDetails;
        //        _with7.StyleBottom = BorderStyle.Solid;
        //        _with7.StyleTop = BorderStyle.Solid;
        //        _with7.StyleRight = BorderStyle.Solid;
        //        _with7.StyleLeft = BorderStyle.None;
        //        var _with8 = _with5.Style;
        //        _with8.Width = Unit.Pixel(17);
        //        _with8.Height = Unit.Pixel(15);
        //        _with8.BorderWidth = Unit.Pixel(1);
        //        _with8.BorderColor = Color.SteelBlue;
        //        _with8.BorderStyle = BorderStyle.Solid;
        //        var _with9 = _with8.BorderDetails;
        //        _with9.WidthLeft = Unit.Pixel(0);
        //        _with9.WidthRight = Unit.Pixel(1);
        //        _with9.WidthTop = Unit.Pixel(1);
        //        _with9.WidthBottom = Unit.Pixel(1);
        //        _with9.StyleBottom = BorderStyle.Solid;
        //        _with9.StyleTop = BorderStyle.Solid;
        //        _with9.StyleRight = BorderStyle.Solid;
        //        _with9.StyleLeft = BorderStyle.None;

        //        _with2.ExpandEffects.ShadowColor = Color.White;
        //    }
        //    #endregion
        //    //Optional ByRef GridHeaders As ICollection = Nothing
        //    // Made the function Overridable [ Rajesh: 06-Feb-2006 ]
        //    public virtual Int32 FetchControlSetting(Page MyPage, string ConfigurationID, string P_Flag = "", int BizType = 0, int ProcessType = 0, int CargoType = 0)
        //    {
        //        Int32 functionReturnValue = default(Int32);
        //        string strSQL = null;
        //        try
        //        {
        //            //' Code Added By rajesh 09-Mar-2006
        //            if ((ViewState["ACCESS_RIGHT"] != null))
        //            {
        //                return ViewState["ACCESS_RIGHT"];
        //            }
        //            //' Code end
        //            if (string.IsNullOrEmpty(Session["USER_PK"]))
        //            {
        //                return false;
        //                return functionReturnValue;
        //            }
        //            ///* ADDED BY umasankar - FOR DASH BOARD */
        //            //-----------------START-----------------------------------
        //            if (IsActivityTrack & ConfigurationID != "QCOR-M-120A" & ConfigurationID != "QFLX0000" & ConfigurationID != "QFLX000T" & ConfigurationID != "QFLX000M" & ConfigurationID != "QFLX000R")
        //            {
        //                if ((Request.QueryString["RefValue"] == null))
        //                {
        //                    fn_UserActivityTracking_ProcCall(ConfigurationID, Session["USERSESSION_ID"], fun_getTimeZoneDateTime());
        //                }
        //            }
        //            //-----------------END------------------------------------
        //            Session["FormHelp"] = setHelpLink(ConfigurationID);
        //            //This is to fetch help link and assign to session for help icon.
        //            Session["KeyHelp"] = setHelpKey(ConfigurationID);
        //            //This is to fetch help link and assign to session for F8 Key.
        //            WorkFlow objWK = new WorkFlow();
        //            DataSet ds = new DataSet();
        //            ObjLog.Log_Trn_Save(Session["USER_PK"], Session["LOGED_IN_LOC_FK"], ConfigurationID);

        //            strSQL = strSQL + Constants.vbCrLf + " SELECT ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CONTROLS_PK, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONMST.CONFIG_MST_PK, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_ID, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONMST.CONFIG_DESCRIPTION, ";
        //            //If Session("USER_PREF_LANG") = "CN-CN" Then
        //            //    strSQL = strSQL & vbCrLf & " CONCTL.CONFIG_CTRL_DEF_TEXT_CN CONFIG_CTRL_DEF_TEXT, "
        //            //Else
        //            //    strSQL = strSQL & vbCrLf & " CONCTL.CONFIG_CTRL_DEF_TEXT, "
        //            //End If
        //            strSQL = strSQL + Constants.vbCrLf + " CONTXT.CONFIG_CONTROL_TEXT CONFIG_CTRL_DEF_TEXT, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONTXT.CONFIG_CONTROL_TOOLTIP, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONTXT.CONFIG_CTRL_MIN_LENGTH, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONTXT.CONFIG_CTRL_MAX_LENGTH, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_FIELD_TYPE, ";

        //            //strSQL = strSQL & vbCrLf & " CONCTL.CONFIG_CONTROL_TOOLTIP, "
        //            //strSQL = strSQL & vbCrLf & " CONCTL.CONFIG_CTRL_MIN_LENGTH, "
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_MAX_LENGTH MAX_LENGTH, ";
        //            //strSQL = strSQL & vbCrLf & " CONCTL.CONFIG_CTRL_FIELD_TYPE, "
        //            strSQL = strSQL + Constants.vbCrLf + " NVL(CONTXT.CONFIG_CONTROL_TEXT,' ') CONFIG_TEXT, ";
        //            strSQL = strSQL + Constants.vbCrLf + " NVL(CONTXT.CONFIG_CONTROL_TOOLTIP,' ') CONFIG_TOOLTIP, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONTXT.CONFIG_GRID_COLUMN_ALIGN, CONTXT.CONFIG_GRID_COL_MAX_LENGTH,";
        //            strSQL = strSQL + Constants.vbCrLf + " CONMST.SET_TAB_INDEX,NVL(CONTXT.CONFIG_TAB_INDEX,0) CONFIG_TAB_INDEX ";
        //            strSQL = strSQL + Constants.vbCrLf + " FROM ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONFIG_CONTROLS_TBL CONCTL, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONFIG_MST_TBL CONMST, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONFIG_CONTROLS_TEXT_TRN CONTXT ";
        //            strSQL = strSQL + Constants.vbCrLf + " WHERE ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONMST.CONFIG_MST_PK=CONCTL.CONFIG_MST_FK ";
        //            strSQL = strSQL + Constants.vbCrLf + " AND CONCTL.CONFIG_CONTROLS_PK= CONTXT.CONFIG_CONTROLS_FK ";
        //            strSQL = strSQL + Constants.vbCrLf + " AND CONMST.CONFIG_ID='" + ConfigurationID + "' ";

        //            //strSQL = strSQL & vbCrLf & " AND conctl.config_ctrl_id = 'btnContainerAdd' "

        //            strSQL = strSQL + Constants.vbCrLf + " AND CONTXT.ENVIRONMENT_TBL_FK(+) = " + (Int64)Session["ENVIRONMENT_PK"] + " ";
        //            if (BizType > 0)
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " AND (CONTXT.BIZ_TYPE= " + BizType + " OR CONTXT.BIZ_TYPE IS NULL)";
        //            }
        //            if (ProcessType > 0)
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " AND (CONTXT.PROCESS_TYPE= " + ProcessType + " OR CONTXT.PROCESS_TYPE IS NULL)";
        //            }
        //            if (BizType == 2 & CargoType > 0)
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " AND (CONTXT.CARGO_TYPE= " + CargoType + " OR CONTXT.CARGO_TYPE IS NULL)";
        //            }
        //            if (!string.IsNullOrEmpty(P_Flag))
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " AND CONCTL.CONFIG_FLAG NOT IN  (" + P_Flag + ") ";
        //            }

        //            ds = objWK.GetDataSet(strSQL);
        //            if (ds.Tables[0].Rows.Count > 0)
        //            {
        //                //nConfigPK = ds.Tables(0).Rows(0).Item(1)
        //                object objCTL = null;
        //                Int16 intI = default(Int16);
        //                System.Diagnostics.Debug Debug = null;
        //                string strFilter = null;
        //                string StrText = null;
        //                int TabIndex = -1;
        //                int CONFIG_MST_PK = ds.Tables[0].Rows[0]["CONFIG_MST_PK"];
        //                DataRow[] dr = null;
        //                Title = ds.Tables[0].Rows[0]["CONFIG_DESCRIPTION"] + " " + strVersionNo;
        //                try
        //                {
        //                    MyPage.Title = Title;
        //                }
        //                catch (Exception ex)
        //                {
        //                }
        //                foreach (object objCTL_loopVariable in MyPage.Controls)
        //                {
        //                    objCTL = objCTL_loopVariable;
        //                    if (objCTL is System.Web.UI.HtmlControls.HtmlForm)
        //                    {
        //                        objCTL = MyPage.Controls[intI];
        //                        object objCtls = null;
        //                        object objTabCTL = null;
        //                        //Snigdharani - 11/12/2008

        //                        foreach (object objCtls_loopVariable in MyPage.Controls[intI].Controls)
        //                        {
        //                            objCtls = objCtls_loopVariable;
        //                            Debug.WriteLine(objCtls.ID);
        //                            //-----------UPDATE TAB INDEX-------------------
        //                            if (objCtls.ToString() != "System.Web.UI.ScriptManager")
        //                            {
        //                                if (ds.Tables[0].Rows[0]["SET_TAB_INDEX"] == 0)
        //                                {
        //                                    if (ds.Tables[0].Select("CONFIG_CTRL_ID='" + (objCtls as Control).ID + "'").Length > 0)
        //                                    {
        //                                        int _tabIndex = 0;
        //                                        try
        //                                        {
        //                                            if (objCtls.ToString() != "System.Web.UI.HtmlControls.HtmlInputButton" & objCtls.ToString() != "System.Web.UI.HtmlControls.HtmlInputText")
        //                                            {
        //                                                _tabIndex = Conversion.Val(objCtls.TabIndex);
        //                                            }
        //                                            else
        //                                            {
        //                                                _tabIndex = Conversion.Val(objCtls.Attributes("TabIndex"));
        //                                            }
        //                                        }
        //                                        catch (Exception ex)
        //                                        {
        //                                            _tabIndex = Conversion.Val(objCtls.Attributes("TabIndex"));
        //                                        }
        //                                        if (_tabIndex > 0)
        //                                        {
        //                                            //strSQL = "UPDATE CONFIG_CONTROLS_TEXT_TRN CC SET CC.CONFIG_TAB_INDEX=" & _tabIndex & " WHERE CC.CONFIG_MST_FK=" & CONFIG_MST_PK & " AND UPPER(CC.CONFIG_CTRL_ID)='" & objCtls.ID.ToString.ToUpper & "'"
        //                                            strSQL = "UPDATE CONFIG_CONTROLS_TEXT_TRN CC SET CC.CONFIG_TAB_INDEX=" + _tabIndex + " WHERE CC.CONFIG_CONTROLS_FK IN ";
        //                                            strSQL = strSQL + Constants.vbCrLf + " (SELECT C.CONFIG_CONTROLS_PK FROM CONFIG_CONTROLS_TBL C WHERE C.CONFIG_MST_FK = " + CONFIG_MST_PK + " AND UPPER(C.CONFIG_CTRL_ID) = '" + objCtls.ID.ToString.ToUpper + "')";
        //                                            if (BizType > 0)
        //                                            {
        //                                                strSQL = strSQL + Constants.vbCrLf + " AND (CC.BIZ_TYPE= " + BizType + " OR CC.BIZ_TYPE IS NULL)";
        //                                            }
        //                                            if (ProcessType > 0)
        //                                            {
        //                                                strSQL = strSQL + Constants.vbCrLf + " AND (CC.PROCESS_TYPE= " + ProcessType + " OR CC.PROCESS_TYPE IS NULL)";
        //                                            }
        //                                            objWK.ExecuteCommands(strSQL);
        //                                        }
        //                                    }
        //                                }
        //                            }
        //                            //----------------------------------------------
        //                            ShowHideEnableDisableControl(ds, ref objCtls);
        //                            if (objCtls is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //                            {
        //                                DateControlUIFormatting(objCtls);
        //                            }
        //                            if (objCtls is System.Web.UI.WebControls.RegularExpressionValidator)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.RegularExpressionValidator)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.RegularExpressionValidator)objCtls).Text = "*";
        //                                    ((System.Web.UI.WebControls.RegularExpressionValidator)objCtls).ErrorMessage = StrText + "";
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.RegularExpressionValidator)objCtls).ToolTip = StrText + "";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.RegularExpressionValidator)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'   
        //                                }

        //                                //implemented by thiyagarajan on 20/12/08 : it will chk all the panels in the form 
        //                            }
        //                            else if (objCtls is Infragistics.WebUI.UltraWebGrid.UltraWebGrid)
        //                            {
        //                                //TryCast(objCtls, Infragistics.WebUI.UltraWebGrid.UltraWebGrid).DisplayLayout.FilterOptionsDefault.AllowRowFiltering = Infragistics.WebUI.UltraWebGrid.RowFiltering.OnClient
        //                                //TryCast(objCtls, Infragistics.WebUI.UltraWebGrid.UltraWebGrid).DisplayLayout.FilterOptionsDefault.FilterUIType = Infragistics.WebUI.UltraWebGrid.FilterUIType.FilterRow

        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.Panel)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Panel)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                Int32 k = default(Int32);
        //                                object objpanel = null;
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    for (k = 0; k <= ((System.Web.UI.WebControls.Panel)objCtls).Controls.Count - 1; k++)
        //                                    {
        //                                        objpanel = ((System.Web.UI.WebControls.Panel)objCtls).Controls[k];
        //                                        if (objpanel is System.Web.UI.WebControls.TextBox)
        //                                        {
        //                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.TextBox)objpanel).ID + "'";
        //                                            dr = ds.Tables[0].Select(strFilter);
        //                                            if (!(dr.Length == 0))
        //                                            {
        //                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                }
        //                                                ((System.Web.UI.WebControls.TextBox)objpanel).ToolTip = StrText + "";
        //                                                if (!Information.IsDBNull(dr[0]["MAX_LENGTH"]))
        //                                                {
        //                                                    ((System.Web.UI.WebControls.TextBox)objpanel).MaxLength = dr[0]["MAX_LENGTH"];
        //                                                }
        //                                                //'For : Tab Index
        //                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                {
        //                                                    ((System.Web.UI.WebControls.TextBox)objpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                }
        //                                                //'
        //                                            }

        //                                        }
        //                                        else if (objpanel is System.Web.UI.WebControls.Label)
        //                                        {
        //                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Label)objpanel).ID + "'";
        //                                            dr = ds.Tables[0].Select(strFilter);
        //                                            if (!(dr.Length == 0))
        //                                            {
        //                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                }
        //                                                ((System.Web.UI.WebControls.Label)objpanel).Text = StrText + "";
        //                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                }
        //                                                ((System.Web.UI.WebControls.Label)objpanel).ToolTip = StrText + "";
        //                                            }
        //                                        }
        //                                        else if (objpanel is System.Web.UI.WebControls.Button)
        //                                        {
        //                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Button)objpanel).ID + "'";
        //                                            dr = ds.Tables[0].Select(strFilter);
        //                                            if (!(dr.Length == 0))
        //                                            {
        //                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                }
        //                                                ((System.Web.UI.WebControls.Button)objpanel).Text = StrText + "";
        //                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                }
        //                                                ((System.Web.UI.WebControls.Button)objpanel).ToolTip = StrText + "";
        //                                                //'For : Tab Index
        //                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                {
        //                                                    ((System.Web.UI.WebControls.Button)objpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                }
        //                                                //'      
        //                                            }

        //                                        }
        //                                        else if (objpanel is System.Web.UI.HtmlControls.HtmlAnchor)
        //                                        {
        //                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlAnchor)objpanel).ID + "'";
        //                                            dr = ds.Tables[0].Select(strFilter);
        //                                            if (!(dr.Length == 0))
        //                                            {
        //                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                }
        //                                                ((System.Web.UI.HtmlControls.HtmlAnchor)objpanel).InnerText = StrText + "";
        //                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                }
        //                                                ((System.Web.UI.HtmlControls.HtmlAnchor)objpanel).Title = StrText + "";
        //                                                //'For : Tab Index
        //                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                {
        //                                                    ((System.Web.UI.HtmlControls.HtmlAnchor)objpanel).Attributes.Add("TABINDEX", dr[0]["CONFIG_TAB_INDEX"]);
        //                                                }
        //                                                //'
        //                                            }
        //                                        }
        //                                        else if (objpanel is System.Web.UI.HtmlControls.HtmlInputText)
        //                                        {
        //                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlInputText)objpanel).ID + "'";
        //                                            dr = ds.Tables[0].Select(strFilter);
        //                                            if (!(dr.Length == 0))
        //                                            {
        //                                                if (dr[0]["CONFIG_TOOLTIP"].ToString() == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"].ToString() + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TOOLTIP"].ToString() + "";
        //                                                }
        //                                                ((System.Web.UI.HtmlControls.HtmlInputText)objCtls).Attributes.Add("TITLE", StrText + "");
        //                                                //'For : Tab Index
        //                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                {
        //                                                    ((System.Web.UI.HtmlControls.HtmlInputText)objCtls).Attributes.Add("TABINDEX", dr[0]["CONFIG_TAB_INDEX"]);
        //                                                }
        //                                                //'     
        //                                            }
        //                                        }
        //                                        else if (objpanel is System.Web.UI.HtmlControls.HtmlInputButton)
        //                                        {
        //                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlInputButton)objpanel).ID + "'";
        //                                            dr = ds.Tables[0].Select(strFilter);
        //                                            if (!(dr.Length == 0))
        //                                            {
        //                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"].ToString() + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TEXT"].ToString() + "";
        //                                                }
        //                                                ((System.Web.UI.HtmlControls.HtmlInputButton)objpanel).Value = StrText + "";
        //                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"].ToString() + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TOOLTIP"].ToString() + "";
        //                                                }
        //                                                ((System.Web.UI.HtmlControls.HtmlInputButton)objpanel).Attributes.Add("TITLE", StrText + "");
        //                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                {
        //                                                    ((System.Web.UI.HtmlControls.HtmlInputButton)objpanel).Attributes.Add("TABINDEX", dr[0]["CONFIG_TAB_INDEX"]);
        //                                                }
        //                                            }
        //                                        }
        //                                        else if (objpanel is System.Web.UI.WebControls.RadioButton)
        //                                        {
        //                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.RadioButton)objpanel).ID + "'";
        //                                            dr = ds.Tables[0].Select(strFilter);
        //                                            if (!(dr.Length == 0))
        //                                            {
        //                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                }
        //                                                ((System.Web.UI.WebControls.RadioButton)objpanel).Text = StrText + "";
        //                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                }
        //                                                ((System.Web.UI.WebControls.RadioButton)objpanel).ToolTip = StrText + "";
        //                                                //'For : Tab Index
        //                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                {
        //                                                    ((System.Web.UI.WebControls.RadioButton)objpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                }
        //                                                //'
        //                                            }
        //                                        }
        //                                        else if (objpanel is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //                                        {
        //                                            strFilter = " CONFIG_CTRL_ID like '" + ((Infragistics.WebUI.WebSchedule.WebDateChooser)objpanel).ID + "'";
        //                                            dr = ds.Tables[0].Select(strFilter);
        //                                            if (!(dr.Length == 0))
        //                                            {
        //                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                }
        //                                                ((Infragistics.WebUI.WebSchedule.WebDateChooser)objpanel).ToolTip = StrText + "";
        //                                                //'For : Tab Index
        //                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                {
        //                                                    ((Infragistics.WebUI.WebSchedule.WebDateChooser)objpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                }
        //                                                //'
        //                                            }

        //                                        }
        //                                        else if (objpanel is System.Web.UI.WebControls.CheckBox)
        //                                        {
        //                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.CheckBox)objpanel).ID + "'";
        //                                            dr = ds.Tables[0].Select(strFilter);
        //                                            if (!(dr.Length == 0))
        //                                            {
        //                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                }
        //                                                ((System.Web.UI.WebControls.CheckBox)objpanel).Text = StrText + "";
        //                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                }
        //                                                else
        //                                                {
        //                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                }
        //                                                ((System.Web.UI.WebControls.CheckBox)objpanel).ToolTip = StrText + "";
        //                                                //'For : Tab Index
        //                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                {
        //                                                    ((System.Web.UI.WebControls.CheckBox)objpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                }
        //                                                //'
        //                                            }
        //                                        }
        //                                    }
        //                                }
        //                                //end by thiyagarajan on 20/12/08 
        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.RequiredFieldValidator)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.RequiredFieldValidator)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.RequiredFieldValidator)objCtls).Text = "*";
        //                                    ((System.Web.UI.WebControls.RequiredFieldValidator)objCtls).ErrorMessage = StrText + "";
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.RequiredFieldValidator)objCtls).ToolTip = StrText + "";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.RequiredFieldValidator)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'
        //                                }

        //                                //ElseIf TypeOf objCtls Is IGERSoft.WebControls.Datepicker Then
        //                                //    CType(objCtls, IGERSoft.WebControls.Datepicker).Format = "[dd]/[MM]/[yyyy]"
        //                                //    CType(objCtls, IGERSoft.WebControls.Datepicker).ResourcesUrl = "..\..\Supports\IGERSoftDatePicker\"
        //                            }
        //                            else if (objCtls is System.Web.UI.HtmlControls.HtmlAnchor)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlAnchor)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                    }
        //                                    ((System.Web.UI.HtmlControls.HtmlAnchor)objCtls).InnerText = StrText + "";
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.HtmlControls.HtmlAnchor)objCtls).Title = StrText + "";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.HtmlControls.HtmlAnchor)objCtls).Attributes.Add("TabIndex", dr[0]["CONFIG_TAB_INDEX"]);
        //                                    }
        //                                    //'
        //                                }
        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.CompareValidator)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.CompareValidator)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.CompareValidator)objCtls).Text = "*";
        //                                    ((System.Web.UI.WebControls.CompareValidator)objCtls).ErrorMessage = StrText + "";
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.CompareValidator)objCtls).ToolTip = StrText + "";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.CompareValidator)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'       
        //                                }

        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.Label)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Label)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);

        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.Label)objCtls).Text = StrText + "";
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.Label)objCtls).ToolTip = StrText + "";
        //                                }

        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.TextBox)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.TextBox)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                string _clientID = (objCtls as Control).ClientID;
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    //CType(objCtls, System.Web.UI.WebControls.TextBox).Text = dr(0).Item("CONFIG_TEXT") & ""
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.TextBox)objCtls).ToolTip = StrText + "";
        //                                    if (!Information.IsDBNull(dr[0]["MAX_LENGTH"]))
        //                                    {
        //                                        ((System.Web.UI.WebControls.TextBox)objCtls).MaxLength = dr[0]["MAX_LENGTH"];
        //                                    }
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.TextBox)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'
        //                                }
        //                                if (((System.Web.UI.WebControls.TextBox)objCtls).CssClass == "NFLATTEXT")
        //                                {
        //                                    ((System.Web.UI.WebControls.TextBox)objCtls).Attributes.Add("onKeyPress", "Check_Numeric('" + _clientID + "');ChangeFlag();");
        //                                }
        //                                else if (((System.Web.UI.WebControls.TextBox)objCtls).CssClass == "AFLATTEXT")
        //                                {
        //                                    ((System.Web.UI.WebControls.TextBox)objCtls).Attributes.Add("onKeyPress", "Check_Alpha('" + _clientID + "');ChangeFlag();");
        //                                }
        //                                else if (((System.Web.UI.WebControls.TextBox)objCtls).CssClass == "NNFLATTEXT")
        //                                {
        //                                    ((System.Web.UI.WebControls.TextBox)objCtls).Attributes.Add("onKeyPress", "Check_Numeric('" + _clientID + "',true);ChangeFlag();");
        //                                }
        //                                else if (((System.Web.UI.WebControls.TextBox)objCtls).CssClass == "CFLATTEXT")
        //                                {
        //                                    ((System.Web.UI.WebControls.TextBox)objCtls).Attributes.Add("onKeyPress", "SetUpperCase('" + _clientID + "');ChangeFlag();");
        //                                }
        //                                else if (((System.Web.UI.WebControls.TextBox)objCtls).CssClass == "FLATTEXT")
        //                                {
        //                                    //CType(objCtls, System.Web.UI.WebControls.TextBox).Attributes.Add("onKeyPress", "SetTextRestrictKey('" & _clientID & "');ChangeFlag();")
        //                                }
        //                                else if (((System.Web.UI.WebControls.TextBox)objCtls).CssClass == "NDFLATTEXT")
        //                                {
        //                                    ((System.Web.UI.WebControls.TextBox)objCtls).Attributes.Add("onKeyPress", "Check_DNumeric('" + _clientID + "',2,true);ChangeFlag();");
        //                                }
        //                                else if (((System.Web.UI.WebControls.TextBox)objCtls).CssClass == "DFLATTEXT")
        //                                {
        //                                    ((System.Web.UI.WebControls.TextBox)objCtls).Attributes.Add("onKeyPress", "Check_DNumeric('" + _clientID + "',2);ChangeFlag();");
        //                                }
        //                                else if (((System.Web.UI.WebControls.TextBox)objCtls).CssClass == "NCFLATTEXT")
        //                                {
        //                                    ((System.Web.UI.WebControls.TextBox)objCtls).Attributes.Add("onKeyPress", "CheckNoSpecials('" + _clientID + "');");
        //                                }
        //                                else
        //                                {
        //                                    //SL for Set/Lost Focus
        //                                    if (((System.Web.UI.WebControls.TextBox)objCtls).CssClass == "SLFLATTEXT")
        //                                    {
        //                                        ((System.Web.UI.WebControls.TextBox)objCtls).Attributes.Add("onFocus", "SetFocusColor('" + _clientID + "');");
        //                                        ((System.Web.UI.WebControls.TextBox)objCtls).Attributes.Add("onBlur", "LostFocusColor('" + _clientID + "');");
        //                                    }
        //                                }
        //                                // CType(objCtls, System.Web.UI.WebControls.TextBox).Attributes.Add("onChange", "JavaScript:ChangeFlag();")

        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.CheckBox)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.CheckBox)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.CheckBox)objCtls).Text = StrText + "";
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.CheckBox)objCtls).ToolTip = StrText + "";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.CheckBox)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'
        //                                }
        //                              ((System.Web.UI.WebControls.CheckBox)objCtls).Attributes.Add("onClick", "JavaScript:ChangeFlag();");
        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.Button)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Button)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.Button)objCtls).Text = StrText + "";
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.Button)objCtls).ToolTip = StrText + "";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.Button)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'
        //                                }
        //                                //added by surya prasad on 14/02/2009
        //                            }
        //                            else if (objCtls is System.Web.UI.HtmlControls.HtmlInputButton)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlInputButton)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TEXT"].ToString() == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"].ToString() + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TEXT"].ToString() + "";
        //                                    }
        //                                    ((System.Web.UI.HtmlControls.HtmlInputButton)objCtls).Value = StrText + "";
        //                                    if (dr[0]["CONFIG_TOOLTIP"].ToString() == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"].ToString() + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"].ToString() + "";
        //                                    }
        //                                    //CType(objCtls, System.Web.UI.HtmlControls.HtmlInputButton).Style.Add("Title", StrText & "")
        //                                    ((System.Web.UI.HtmlControls.HtmlInputButton)objCtls).Attributes.Add("TITLE", StrText + "");
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.HtmlControls.HtmlInputButton)objCtls).Attributes.Add("TABINDEX", dr[0]["CONFIG_TAB_INDEX"]);
        //                                    }
        //                                    //'      
        //                                }
        //                            }
        //                            else if (objCtls is System.Web.UI.HtmlControls.HtmlInputText)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlInputText)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    //If dr(0).Item("CONFIG_TEXT").ToString = " " Then
        //                                    //    StrText = dr(0).Item("CONFIG_CTRL_DEF_TEXT").ToString & ""
        //                                    //Else
        //                                    //    StrText = dr(0).Item("CONFIG_TEXT").ToString & ""
        //                                    //End If
        //                                    //CType(objCtls, System.Web.UI.HtmlControls.HtmlInputText).Value = StrText & ""
        //                                    if (dr[0]["CONFIG_TOOLTIP"].ToString() == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"].ToString() + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"].ToString() + "";
        //                                    }
        //                                    //CType(objCtls, System.Web.UI.HtmlControls.HtmlInputButton).Style.Add("Title", StrText & "")
        //                                    ((System.Web.UI.HtmlControls.HtmlInputText)objCtls).Attributes.Add("TITLE", StrText + "");
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.HtmlControls.HtmlInputText)objCtls).Attributes.Add("TABINDEX", dr[0]["CONFIG_TAB_INDEX"]);
        //                                    }
        //                                    //'     
        //                                }
        //                                //end
        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.LinkButton)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.LinkButton)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.LinkButton)objCtls).Text = StrText + "";
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.LinkButton)objCtls).ToolTip = StrText + "";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.LinkButton)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'
        //                                }
        //                            }
        //                            else if (objCtls is Infragistics.WebUI.WebDataInput.WebDateTimeEdit)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)objCtls).ToolTip = StrText + "";
        //                                    ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)objCtls).CssClass = "WebDateTimeEdit";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'
        //                                }
        //                            }
        //                            else if (objCtls is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((Infragistics.WebUI.WebSchedule.WebDateChooser)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((Infragistics.WebUI.WebSchedule.WebDateChooser)objCtls).ToolTip = StrText + "";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((Infragistics.WebUI.WebSchedule.WebDateChooser)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'
        //                                }

        //                                var _with10 = ((Infragistics.WebUI.WebSchedule.WebDateChooser)objCtls).CalendarLayout;
        //                                _with10.FooterFormat = "Today: {0:d}";
        //                                //.PrevMonthImageUrl = "ig_cal_blueP0.gif"
        //                                //.NextMonthImageUrl = "ig_cal_blueN0.gif"
        //                                //.TitleStyle.BackgroundImage = "ig_cal_blue2.gif"
        //                                //.FooterStyle.BackgroundImage = "ig_cal_blue1.gif"
        //                                //.FooterStyle.CssClass = "CalenderFooter"
        //                                //.SelectedDayStyle.BackColor = Color.Violet
        //                                ((Infragistics.WebUI.WebSchedule.WebDateChooser)objCtls).DropButton.Style.CssClass = "DateDropButtonClass";
        //                                ((Infragistics.WebUI.WebSchedule.WebDateChooser)objCtls).CssClass = "WebDateClass";


        //                                _with10.SelectedDayStyle.CssClass = "CalenderSelectedDay";
        //                                _with10.OtherMonthDayStyle.CssClass = "CelenderOtherMonthDay";
        //                                _with10.NextPrevStyle.CssClass = "CelenderNextPrev";
        //                                _with10.CalendarStyle.CssClass = "CelenderStyle";
        //                                _with10.TodayDayStyle.CssClass = "TodayDayStyle";
        //                                _with10.DayHeaderStyle.CssClass = "DayHeaderStyle";
        //                                _with10.TitleStyle.CssClass = "TitleStyle";
        //                                _with10.DropDownStyle.BorderStyle = BorderStyle.Solid;

        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.DropDownList)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.DropDownList)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                    {
        //                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                    }
        //                                    else
        //                                    {
        //                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                    }
        //                                    ((System.Web.UI.WebControls.DropDownList)objCtls).ToolTip = StrText + "";
        //                                    //'For : Tab Index
        //                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.DropDownList)objCtls).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                    }
        //                                    //'
        //                                }
        //                              ((System.Web.UI.WebControls.DropDownList)objCtls).Attributes.Add("onChange", "JavaScript:ChangeFlag();");
        //                            }
        //                            else if (objCtls is System.Web.UI.WebControls.ListBox)
        //                            {
        //                                //Snigdharani - 11/12/2008 - For controls inside the tab
        //                            }
        //                            else if (objCtls is System.Web.UI.UpdatePanel)
        //                            {
        //                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.UpdatePanel)objCtls).ID + "'";
        //                                dr = ds.Tables[0].Select(strFilter);
        //                                Int32 k = default(Int32);
        //                                Int32 l = default(Int32);
        //                                object objUpdpanel = null;
        //                                if (!(dr.Length == 0))
        //                                {
        //                                    for (l = 0; l <= ((System.Web.UI.UpdatePanel)objCtls).Controls.Count - 1; l++)
        //                                    {
        //                                        for (k = 0; k <= ((System.Web.UI.UpdatePanel)objCtls).Controls[l].Controls.Count - 1; k++)
        //                                        {
        //                                            objUpdpanel = ((System.Web.UI.UpdatePanel)objCtls).Controls[l].Controls[k];
        //                                            if (objUpdpanel is Infragistics.WebUI.UltraWebTab.UltraWebTab)
        //                                            {
        //                                                for (t = 0; t <= ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objUpdpanel).Tabs.Count - 1; t++)
        //                                                {
        //                                                    foreach (object objTabCTL_loopVariable in ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objUpdpanel).Tabs.GetTab(t).ContentPane.Controls)
        //                                                    {
        //                                                        objTabCTL = objTabCTL_loopVariable;
        //                                                        if (objTabCTL is System.Web.UI.WebControls.TextBox)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.TextBox)objTabCTL).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.TextBox)objTabCTL).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((System.Web.UI.WebControls.TextBox)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'
        //                                                            }
        //                                                        }
        //                                                        else if (objTabCTL is System.Web.UI.WebControls.Label)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Label)objTabCTL).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.Label)objTabCTL).Text = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.Label)objTabCTL).ToolTip = StrText + "";
        //                                                            }
        //                                                        }
        //                                                        else if (objTabCTL is System.Web.UI.WebControls.Button)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Button)objTabCTL).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.Button)objTabCTL).Text = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.Button)objTabCTL).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((System.Web.UI.WebControls.Button)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'
        //                                                            }
        //                                                        }
        //                                                        else if (objTabCTL is System.Web.UI.HtmlControls.HtmlAnchor)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlAnchor)objTabCTL).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.HtmlControls.HtmlAnchor)objTabCTL).InnerText = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.HtmlControls.HtmlAnchor)objTabCTL).Title = StrText + "";

        //                                                            }
        //                                                        }
        //                                                        else if (objTabCTL is System.Web.UI.WebControls.RadioButton)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.RadioButton)objTabCTL).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.RadioButton)objTabCTL).Text = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.RadioButton)objTabCTL).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((System.Web.UI.WebControls.RadioButton)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'
        //                                                            }
        //                                                        }
        //                                                        else if (objTabCTL is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((Infragistics.WebUI.WebSchedule.WebDateChooser)objTabCTL).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((Infragistics.WebUI.WebSchedule.WebDateChooser)objTabCTL).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((Infragistics.WebUI.WebSchedule.WebDateChooser)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'
        //                                                            }
        //                                                        }
        //                                                        else if (objTabCTL is System.Web.UI.WebControls.CheckBox)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.CheckBox)objTabCTL).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.CheckBox)objTabCTL).Text = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.CheckBox)objTabCTL).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((System.Web.UI.WebControls.CheckBox)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'
        //                                                            }
        //                                                        }
        //                                                    }
        //                                                }
        //                                            }
        //                                        }
        //                                    }
        //                                }
        //                            }
        //                            else if (objCtls is Infragistics.WebUI.UltraWebTab.UltraWebTab)
        //                            {
        //                                //strFilter = " CONFIG_CTRL_ID like '" & CType(objCtls, Infragistics.WebUI.UltraWebTab.Tabs).GetTab(1).FindControl("") & "'"
        //                                int t = 0;
        //                                for (t = 0; t <= ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.Count - 1; t++)
        //                                {
        //                                    if ((((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t) != null))
        //                                    {
        //                                        strFilter = " CONFIG_CTRL_ID like '" + ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t).Key + "'";
        //                                        //.ContentPane.Controls(0).ClientID
        //                                        dr = ds.Tables[0].Select(strFilter);
        //                                        if (!(dr.Length == 0))
        //                                        {
        //                                            if (dr[0]["CONFIG_TEXT"] == " ")
        //                                            {
        //                                                StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                            }
        //                                            else
        //                                            {
        //                                                StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                            }
        //                                            ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t).Text = StrText + "";
        //                                            if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                            {
        //                                                StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                            }
        //                                            else
        //                                            {
        //                                                StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                            }
        //                                            ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t).Tooltip = StrText + "";
        //                                            //Snigdharani - 02/04/2009
        //                                            ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).CssClass = "UltraWebTab";
        //                                            ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t).HoverStyle.CssClass = "HoverStyle";
        //                                            ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t).Style.CssClass = "TabStyle";
        //                                            ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).SelectedTabStyle.CssClass = "SelectedTabStyle";
        //                                            ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).DefaultTabStyle.CssClass = "DefaultTabStyle";
        //                                        }
        //                                    }
        //                                }
        //                                for (t = 0; t <= ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.Count - 1; t++)
        //                                {
        //                                    if ((((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t) != null))
        //                                    {
        //                                        foreach (object objTabCTL_loopVariable in ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t).ContentPane.Controls)
        //                                        {
        //                                            objTabCTL = objTabCTL_loopVariable;
        //                                            if (objTabCTL is System.Web.UI.WebControls.RegularExpressionValidator)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.RegularExpressionValidator)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.RegularExpressionValidator)objTabCTL).Text = "*";
        //                                                    ((System.Web.UI.WebControls.RegularExpressionValidator)objTabCTL).ErrorMessage = StrText + "";
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.RegularExpressionValidator)objTabCTL).ToolTip = StrText + "";
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.RegularExpressionValidator)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                    }
        //                                                    //'
        //                                                }
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.WebControls.RequiredFieldValidator)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.RequiredFieldValidator)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.RequiredFieldValidator)objTabCTL).Text = "*";
        //                                                    ((System.Web.UI.WebControls.RequiredFieldValidator)objTabCTL).ErrorMessage = StrText + "";
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.RequiredFieldValidator)objTabCTL).ToolTip = StrText + "";
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.RequiredFieldValidator)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                    }
        //                                                    //'
        //                                                }
        //                                                //ElseIf TypeOf objTabCTL Is IGERSoft.WebControls.Datepicker Then
        //                                                //    CType(objTabCTL, IGERSoft.WebControls.Datepicker).Format = "[dd]/[MM]/[yyyy]"
        //                                                //    CType(objTabCTL, IGERSoft.WebControls.Datepicker).ResourcesUrl = "..\..\Supports\IGERSoftDatePicker\"
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.HtmlControls.HtmlAnchor)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlAnchor)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.HtmlControls.HtmlAnchor)objTabCTL).InnerText = StrText + "";
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.HtmlControls.HtmlAnchor)objTabCTL).Title = StrText + "";
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((System.Web.UI.HtmlControls.HtmlAnchor)objTabCTL).Attributes.Add("TABINDEX", dr[0]["CONFIG_TAB_INDEX"]);
        //                                                    }
        //                                                    //'
        //                                                }
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.WebControls.CompareValidator)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.CompareValidator)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.CompareValidator)objTabCTL).Text = "*";
        //                                                    ((System.Web.UI.WebControls.CompareValidator)objTabCTL).ErrorMessage = StrText + "";
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.CompareValidator)objTabCTL).ToolTip = StrText + "";
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.CompareValidator)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                    }
        //                                                    //'
        //                                                }
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.UpdatePanel)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.UpdatePanel)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                Int32 k = default(Int32);
        //                                                object objUpdpanel = null;
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    for (k = 0; k <= ((System.Web.UI.UpdatePanel)objTabCTL).Controls[0].Controls.Count - 1; k++)
        //                                                    {
        //                                                        objUpdpanel = ((System.Web.UI.UpdatePanel)objTabCTL).Controls[0].Controls[k];
        //                                                        if (objUpdpanel is System.Web.UI.WebControls.TextBox)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.TextBox)objUpdpanel).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.TextBox)objUpdpanel).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((System.Web.UI.WebControls.TextBox)objUpdpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'
        //                                                            }
        //                                                        }
        //                                                        else if (objUpdpanel is System.Web.UI.WebControls.Label)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Label)objUpdpanel).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.Label)objUpdpanel).Text = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.Label)objUpdpanel).ToolTip = StrText + "";
        //                                                            }
        //                                                        }
        //                                                        else if (objUpdpanel is System.Web.UI.WebControls.Button)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Button)objUpdpanel).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.Button)objUpdpanel).Text = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.Button)objUpdpanel).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((System.Web.UI.WebControls.Button)objUpdpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'  
        //                                                            }
        //                                                        }
        //                                                        else if (objUpdpanel is System.Web.UI.HtmlControls.HtmlAnchor)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlAnchor)objUpdpanel).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.HtmlControls.HtmlAnchor)objUpdpanel).InnerText = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.HtmlControls.HtmlAnchor)objUpdpanel).Title = StrText + "";

        //                                                            }
        //                                                        }
        //                                                        else if (objUpdpanel is System.Web.UI.WebControls.RadioButton)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.RadioButton)objUpdpanel).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.RadioButton)objUpdpanel).Text = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.RadioButton)objUpdpanel).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((System.Web.UI.WebControls.RadioButton)objUpdpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'
        //                                                            }
        //                                                        }
        //                                                        else if (objUpdpanel is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((Infragistics.WebUI.WebSchedule.WebDateChooser)objUpdpanel).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((Infragistics.WebUI.WebSchedule.WebDateChooser)objUpdpanel).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((Infragistics.WebUI.WebSchedule.WebDateChooser)objUpdpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'
        //                                                            }
        //                                                        }
        //                                                        else if (objUpdpanel is System.Web.UI.WebControls.CheckBox)
        //                                                        {
        //                                                            strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.CheckBox)objUpdpanel).ID + "'";
        //                                                            dr = ds.Tables[0].Select(strFilter);
        //                                                            if (!(dr.Length == 0))
        //                                                            {
        //                                                                if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.CheckBox)objUpdpanel).Text = StrText + "";
        //                                                                if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                                }
        //                                                                else
        //                                                                {
        //                                                                    StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                                }
        //                                                                ((System.Web.UI.WebControls.CheckBox)objUpdpanel).ToolTip = StrText + "";
        //                                                                //'For : Tab Index
        //                                                                if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                                {
        //                                                                    ((System.Web.UI.WebControls.CheckBox)objUpdpanel).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                                }
        //                                                                //'
        //                                                            }
        //                                                        }
        //                                                    }
        //                                                }
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.WebControls.Label)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Label)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);

        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.Label)objTabCTL).Text = StrText + "";
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.Label)objTabCTL).ToolTip = StrText + "";
        //                                                }

        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.WebControls.TextBox)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.TextBox)objTabCTL).ID + "'";
        //                                                string _clientID = (objTabCTL as Control).ClientID;
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    //CType(objTabCTL, System.Web.UI.WebControls.TextBox).Text = dr(0).Item("CONFIG_TEXT") & ""
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.TextBox)objTabCTL).ToolTip = StrText + "";
        //                                                    if (!Information.IsDBNull(dr[0]["MAX_LENGTH"]))
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.TextBox)objTabCTL).MaxLength = dr[0]["MAX_LENGTH"];
        //                                                    }
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.TextBox)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                    }
        //                                                    //'
        //                                                }
        //                                                if (((System.Web.UI.WebControls.TextBox)objTabCTL).CssClass == "NFLATTEXT")
        //                                                {
        //                                                    ((System.Web.UI.WebControls.TextBox)objTabCTL).Attributes.Add("onKeyPress", "Check_Numeric('" + _clientID + "');ChangeFlag();");
        //                                                }
        //                                                else if (((System.Web.UI.WebControls.TextBox)objTabCTL).CssClass == "AFLATTEXT")
        //                                                {
        //                                                    ((System.Web.UI.WebControls.TextBox)objTabCTL).Attributes.Add("onKeyPress", "Check_Alpha('" + _clientID + "');ChangeFlag();");
        //                                                }
        //                                                else if (((System.Web.UI.WebControls.TextBox)objTabCTL).CssClass == "NNFLATTEXT")
        //                                                {
        //                                                    ((System.Web.UI.WebControls.TextBox)objTabCTL).Attributes.Add("onKeyPress", "Check_Numeric('" + _clientID + "',true);ChangeFlag();");
        //                                                }
        //                                                else if (((System.Web.UI.WebControls.TextBox)objTabCTL).CssClass == "CFLATTEXT")
        //                                                {
        //                                                    ((System.Web.UI.WebControls.TextBox)objTabCTL).Attributes.Add("onKeyPress", "SetUpperCase('" + _clientID + "');ChangeFlag();");
        //                                                }
        //                                                else if (((System.Web.UI.WebControls.TextBox)objTabCTL).CssClass == "FLATTEXT")
        //                                                {
        //                                                    //CType(objTabCTL, System.Web.UI.WebControls.TextBox).Attributes.Add("onKeyPress", "SetTextRestrictKey('" & _clientID & "');ChangeFlag();")
        //                                                }
        //                                                else if (((System.Web.UI.WebControls.TextBox)objTabCTL).CssClass == "NDFLATTEXT")
        //                                                {
        //                                                    ((System.Web.UI.WebControls.TextBox)objTabCTL).Attributes.Add("onKeyPress", "Check_DNumeric('" + _clientID + "',2,true);ChangeFlag();");
        //                                                }
        //                                                else if (((System.Web.UI.WebControls.TextBox)objTabCTL).CssClass == "DFLATTEXT")
        //                                                {
        //                                                    ((System.Web.UI.WebControls.TextBox)objTabCTL).Attributes.Add("onKeyPress", "Check_DNumeric('" + _clientID + "',2);ChangeFlag();");
        //                                                }
        //                                                else if (((System.Web.UI.WebControls.TextBox)objTabCTL).CssClass == "NCFLATTEXT")
        //                                                {
        //                                                    ((System.Web.UI.WebControls.TextBox)objTabCTL).Attributes.Add("onKeyPress", "CheckNoSpecials('" + _clientID + "');");
        //                                                }
        //                                                else
        //                                                {
        //                                                    //SL for Set/Lost Focus
        //                                                    if (((System.Web.UI.WebControls.TextBox)objTabCTL).CssClass == "SLFLATTEXT")
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.TextBox)objTabCTL).Attributes.Add("onFocus", "SetFocusColor('" + _clientID + "');");
        //                                                        ((System.Web.UI.WebControls.TextBox)objTabCTL).Attributes.Add("onBlur", "LostFocusColor('" + _clientID + "');");
        //                                                    }
        //                                                }
        //                                                // CType(objTabCTL, System.Web.UI.WebControls.TextBox).Attributes.Add("onChange", "JavaScript:ChangeFlag();")
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.WebControls.CheckBox)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.CheckBox)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.CheckBox)objTabCTL).Text = StrText + "";
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.CheckBox)objTabCTL).ToolTip = StrText + "";
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.CheckBox)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                    }
        //                                                    //'
        //                                                }

        //                                              ((System.Web.UI.WebControls.CheckBox)objTabCTL).Attributes.Add("onClick", "JavaScript:ChangeFlag();");
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.WebControls.Button)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.Button)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.Button)objTabCTL).Text = StrText + "";
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.Button)objTabCTL).ToolTip = StrText + "";
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.Button)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                    }
        //                                                    //'
        //                                                }
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.WebControls.LinkButton)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.LinkButton)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TEXT"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TEXT"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.LinkButton)objTabCTL).Text = StrText + "";
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.LinkButton)objTabCTL).ToolTip = StrText + "";
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.LinkButton)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                    }
        //                                                    //'
        //                                                }
        //                                            }
        //                                            else if (objTabCTL is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((Infragistics.WebUI.WebSchedule.WebDateChooser)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((Infragistics.WebUI.WebSchedule.WebDateChooser)objTabCTL).ToolTip = StrText + "";
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((Infragistics.WebUI.WebSchedule.WebDateChooser)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                    }
        //                                                    //'
        //                                                }
        //                                                var _with11 = ((Infragistics.WebUI.WebSchedule.WebDateChooser)objTabCTL).CalendarLayout;
        //                                                _with11.FooterFormat = "Today: {0:d}";
        //                                                //.PrevMonthImageUrl = "ig_cal_blueP0.gif"
        //                                                //.NextMonthImageUrl = "ig_cal_blueN0.gif"
        //                                                //.TitleStyle.BackgroundImage = "ig_cal_blue2.gif"
        //                                                //.FooterStyle.BackgroundImage = "ig_cal_blue1.gif"
        //                                                //.FooterStyle.CssClass = "CalenderFooter"
        //                                                //.SelectedDayStyle.BackColor = Color.Violet
        //                                                ((Infragistics.WebUI.WebSchedule.WebDateChooser)objTabCTL).DropButton.Style.CssClass = "DateDropButtonClass";
        //                                                ((Infragistics.WebUI.WebSchedule.WebDateChooser)objTabCTL).CssClass = "WebDateClass";


        //                                                _with11.SelectedDayStyle.CssClass = "CalenderSelectedDay";
        //                                                _with11.OtherMonthDayStyle.CssClass = "CelenderOtherMonthDay";
        //                                                _with11.NextPrevStyle.CssClass = "CelenderNextPrev";
        //                                                _with11.CalendarStyle.CssClass = "CelenderStyle";
        //                                                _with11.TodayDayStyle.CssClass = "TodayDayStyle";
        //                                                _with11.DayHeaderStyle.CssClass = "DayHeaderStyle";
        //                                                _with11.TitleStyle.CssClass = "TitleStyle";
        //                                                _with11.DropDownStyle.BorderStyle = BorderStyle.Solid;
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.WebControls.DropDownList)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.WebControls.DropDownList)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    if (dr[0]["CONFIG_TOOLTIP"] == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"] + "";
        //                                                    }
        //                                                    ((System.Web.UI.WebControls.DropDownList)objTabCTL).ToolTip = StrText + "";
        //                                                    //'For : Tab Index
        //                                                    if (dr[0]["SET_TAB_INDEX"] == 1 & Convert.ToInt32(dr[0]["CONFIG_TAB_INDEX"]) > 0)
        //                                                    {
        //                                                        ((System.Web.UI.WebControls.DropDownList)objTabCTL).TabIndex = dr[0]["CONFIG_TAB_INDEX"];
        //                                                    }
        //                                                    //'
        //                                                }
        //                                              ((System.Web.UI.WebControls.DropDownList)objTabCTL).Attributes.Add("onChange", "JavaScript:ChangeFlag();");
        //                                                //Added By Arun on 20/10/2012 (Ref : DTS 12410,12411)
        //                                            }
        //                                            else if (objTabCTL is System.Web.UI.HtmlControls.HtmlInputButton)
        //                                            {
        //                                                strFilter = " CONFIG_CTRL_ID like '" + ((System.Web.UI.HtmlControls.HtmlInputButton)objTabCTL).ID + "'";
        //                                                dr = ds.Tables[0].Select(strFilter);
        //                                                if (!(dr.Length == 0))
        //                                                {
        //                                                    //If dr(0).Item("CONFIG_TEXT").ToString = " " Then
        //                                                    //    StrText = dr(0).Item("CONFIG_CTRL_DEF_TEXT").ToString & ""
        //                                                    //Else
        //                                                    //    StrText = dr(0).Item("CONFIG_TEXT").ToString & ""
        //                                                    //End If
        //                                                    //CType(objTabCTL, System.Web.UI.HtmlControls.HtmlInputButton).Value = StrText & ""
        //                                                    if (dr[0]["CONFIG_TOOLTIP"].ToString() == " ")
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_CONTROL_TOOLTIP"].ToString() + "";
        //                                                    }
        //                                                    else
        //                                                    {
        //                                                        StrText = dr[0]["CONFIG_TOOLTIP"].ToString() + "";
        //                                                    }
        //                                                    ((System.Web.UI.HtmlControls.HtmlInputButton)objTabCTL).Attributes.Add("TITLE", StrText + "");

        //                                                }
        //                                            }
        //                                        }
        //                                    }
        //                                }
        //                                //End Snigdharani
        //                            }
        //                        }
        //                    }
        //                    intI += 1;
        //                }
        //                Int32 I = default(Int32);
        //                strFilter = " CONFIG_CTRL_FIELD_TYPE like 'HD'";

        //                dr = ds.Tables[0].Select(strFilter);
        //                for (I = 0; I <= dr.Length - 1; I++)
        //                {
        //                    GridHeaderSetting objGridHeader = new GridHeaderSetting();
        //                    if (dr[I]["CONFIG_TEXT"] == " ")
        //                    {
        //                        objGridHeader.M_TEXT = dr[I]["CONFIG_CTRL_DEF_TEXT"] + "";
        //                    }
        //                    else
        //                    {
        //                        objGridHeader.M_TEXT = dr[I]["CONFIG_TEXT"] + "";
        //                    }
        //                    if (dr[I]["CONFIG_TOOLTIP"] == " ")
        //                    {
        //                        objGridHeader.M_ToolTip = dr[I]["CONFIG_CONTROL_TOOLTIP"] + "";
        //                    }
        //                    else
        //                    {
        //                        objGridHeader.M_ToolTip = dr[I]["CONFIG_TOOLTIP"] + "";
        //                    }
        //                    objGridHeader.M_Width = dr[I]["CONFIG_CTRL_MAX_LENGTH"] + "";
        //                    objGridHeader.M_Settings = dr[I]["CONFIG_CTRL_MIN_LENGTH"] + "";
        //                    objGridHeader.M_CtrlID = dr[I]["CONFIG_CTRL_ID"] + "";
        //                    objGridHeader.M_ColumnAlign = dr[I]["CONFIG_GRID_COLUMN_ALIGN"] + "";
        //                    objGridHeader.M_ColumnFieldLength = dr[I]["CONFIG_GRID_COL_MAX_LENGTH"] + "";
        //                    GridHeaders.Add(objGridHeader, objGridHeader.M_CtrlID);
        //                }

        //                //'Added by Faheem For checking Extra Rights
        //                fn_Check_AccessRights(MyPage, ConfigurationID, null, null, null, null, null, null);


        //                ((Common)MyPage).ConfigurationPK = ds.Tables[0].Rows[0]["CONFIG_MST_PK"];
        //                int configpk = 0;
        //                configpk = ds.Tables[0].Rows[0]["CONFIG_MST_PK"];

        //                strSQL = " Select UA.ALLOWED_OPERATIONS_VALUE";
        //                strSQL += " FROM USER_ACCESS_TRN UA ";
        //                strSQL += " WHERE UA.USER_MST_FK=" + Session["USER_PK"];
        //                strSQL += " AND UA.LOCATION_MST_FK = " + Session["LOGED_IN_LOC_FK"];
        //                strSQL += " AND UA.CONFIG_MST_FK=" + configpk;
        //                ds = objWK.GetDataSet(strSQL);
        //                if (ds.Tables[0].Rows.Count > 0)
        //                {
        //                    // Code Added By Rajesh 09-Mar-2006
        //                    ViewState["ACCESS_RIGHT"] = (Information.IsDBNull(ds.Tables[0].Rows[0]["ALLOWED_OPERATIONS_VALUE"]) ? 0 : ds.Tables[0].Rows[0]["ALLOWED_OPERATIONS_VALUE"]);
        //                    return ViewState["ACCESS_RIGHT"];
        //                    // Code end
        //                    //Return IIf(IsDBNull(ds.Tables(0).Rows(0).Item("ALLOWED_OPERATIONS_VALUE")), 0, ds.Tables(0).Rows(0).Item("ALLOWED_OPERATIONS_VALUE"))
        //                }
        //                else
        //                {
        //                    strSQL = " Select RA.ALLOWED_OPERATIONS_VALUE";
        //                    strSQL += " FROM USER_MST_TBL UM, ROLE_ACCESS_TRN RA ";
        //                    strSQL += " WHERE RA.ROLE_MST_FK=UM.ROLE_MST_FK ";
        //                    strSQL += " AND UM.USER_MST_PK=" + Session["USER_PK"];
        //                    strSQL += " AND RA.CONFIG_MST_FK=" + configpk;
        //                    ds = objWK.GetDataSet(strSQL);
        //                    if (ds.Tables[0].Rows.Count > 0)
        //                    {
        //                        // Code Added By Rajesh 09-Mar-2006 
        //                        ViewState["ACCESS_RIGHT"] = (Information.IsDBNull(ds.Tables[0].Rows[0]["ALLOWED_OPERATIONS_VALUE"]) ? 0 : ds.Tables[0].Rows[0]["ALLOWED_OPERATIONS_VALUE"]);
        //                        return ViewState["ACCESS_RIGHT"];
        //                        // Code end
        //                        // Return IIf(IsDBNull(ds.Tables(0).Rows(0).Item("ALLOWED_OPERATIONS_VALUE")), 0, ds.Tables(0).Rows(0).Item("ALLOWED_OPERATIONS_VALUE"))
        //                    }
        //                    else
        //                    {
        //                        return 0;
        //                    }
        //                }
        //            }

        //        }
        //        catch (Exception EX)
        //        {
        //            throw EX;
        //        }
        //        return functionReturnValue;
        //    }
        //    private void ShowHideEnableDisableControl(DataSet ds, ref Control objCtls)
        //    {
        //        string strFilter = "";
        //        string StrText = "";
        //        DataRow[] dr = null;

        //        if (objCtls is Infragistics.WebUI.UltraWebTab.UltraWebTab)
        //        {
        //            for (t = 0; t <= ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.Count - 1; t++)
        //            {
        //                if ((((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t) != null))
        //                {
        //                    foreach (object _CTL_loopVariable in ((Infragistics.WebUI.UltraWebTab.UltraWebTab)objCtls).Tabs.GetTab(t).ContentPane.Controls)
        //                    {
        //                        _CTL = _CTL_loopVariable;
        //                        ShowHideEnableDisableControl(ds, ref _CTL);
        //                    }
        //                }
        //            }
        //        }
        //        if ((objCtls.ID == null))
        //            return;
        //        strFilter = " CONFIG_CTRL_ID like '" + objCtls.ID + "'";
        //        dr = ds.Tables[0].Select(strFilter);
        //        if (dr.Length > 0)
        //        {
        //            if (objCtls is System.Web.UI.WebControls.Label)
        //            {
        //                Label _objCtl = objCtls as Label;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Enabled = false;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]);
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.WebControls.TextBox)
        //            {
        //                TextBox _objCtl = objCtls as TextBox;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Enabled = false;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]);
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.HtmlControls.HtmlInputText)
        //            {
        //                System.Web.UI.HtmlControls.HtmlInputText _objCtl = objCtls as System.Web.UI.HtmlControls.HtmlInputText;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Disabled = true;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Attributes.Add("style", "width:" + Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]).ToString());
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.WebControls.Button)
        //            {
        //                Button _objCtl = objCtls as Button;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Enabled = false;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]);
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.HtmlControls.HtmlButton)
        //            {
        //                System.Web.UI.HtmlControls.HtmlButton _objCtl = objCtls as System.Web.UI.HtmlControls.HtmlButton;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Disabled = true;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Attributes.Add("style", "width:" + Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]).ToString());
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.HtmlControls.HtmlInputButton)
        //            {
        //                System.Web.UI.HtmlControls.HtmlInputButton _objCtl = objCtls as System.Web.UI.HtmlControls.HtmlInputButton;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Disabled = true;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Attributes.Add("style", "width:" + Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]).ToString());
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.WebControls.ListControl)
        //            {
        //                ListControl _objCtl = objCtls as ListControl;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Enabled = false;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]);
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //            {
        //                Infragistics.WebUI.WebSchedule.WebDateChooser _objCtl = objCtls as Infragistics.WebUI.WebSchedule.WebDateChooser;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Enabled = false;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]);
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.WebControls.CheckBox)
        //            {
        //                CheckBox _objCtl = objCtls as CheckBox;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Enabled = false;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]);
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.WebControls.CheckBoxList)
        //            {
        //                CheckBoxList _objCtl = objCtls as CheckBoxList;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Enabled = false;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]);
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.WebControls.RadioButton)
        //            {
        //                RadioButton _objCtl = objCtls as RadioButton;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Enabled = false;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]);
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.WebControls.RadioButtonList)
        //            {
        //                RadioButtonList _objCtl = objCtls as RadioButtonList;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Enabled = false;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]);
        //                        }
        //                    }
        //                }
        //            }
        //            else if (objCtls is System.Web.UI.HtmlControls.HtmlTable)
        //            {
        //                System.Web.UI.HtmlControls.HtmlTable _objCtl = objCtls as System.Web.UI.HtmlControls.HtmlTable;
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MIN_LENGTH"]))
        //                {
        //                    if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "2")
        //                    {
        //                        _objCtl.Attributes.Add("style", "display:none;");
        //                    }
        //                    else if (dr[0]["CONFIG_CTRL_MIN_LENGTH"] == "1")
        //                    {
        //                        _objCtl.Disabled = true;
        //                    }
        //                }
        //                if (!Information.IsDBNull(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                {
        //                    if (Information.IsNumeric(dr[0]["CONFIG_CTRL_MAX_LENGTH"]))
        //                    {
        //                        if (dr[0]["CONFIG_CTRL_MAX_LENGTH"] > 0)
        //                        {
        //                            _objCtl.Width = Unit.Pixel(dr[0]["CONFIG_CTRL_MAX_LENGTH"]).ToString();
        //                        }
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    public DataSet GetHelpDataSet(string QueryID, string ColumnsToBehide = "", string strQuery = "")
        //    {
        //        DataSet objDS = null;
        //        switch (QueryID)
        //        {
        //            case "Employee":
        //                break;
        //            //    objDS = (New cls_Employee).MyDataSet
        //            case "CompleteQuery":
        //                objDS = (new WorkFlow()).GetDataSet(strQuery);
        //                return objDS;
        //        }
        //        if (ColumnsToBehide.Trim().Length > 0)
        //        {
        //            string[] str = null;
        //            str = Strings.Split(ColumnsToBehide, "~");
        //            int intI = 0;
        //            int intj = 0;
        //            string strRemove = null;
        //            while (intI < objDS.Tables[0].Columns.Count)
        //            {
        //                for (intj = 0; intj <= str.Length - 1; intj++)
        //                {
        //                    if (objDS.Tables[0].Columns[intI].ColumnName.ToUpper() == Conversion.Str(intj).ToUpper())
        //                    {
        //                        DataColumn objColumn = null;
        //                        objColumn = objDS.Tables[0].Columns[intI];
        //                        objDS.Tables[0].Columns.Remove(objColumn);
        //                        intI -= 1;
        //                        break; // TODO: might not be correct. Was : Exit For
        //                    }
        //                }
        //                intI += 1;
        //            }
        //        }
        //        return objDS;
        //    }
        //    protected override void render(System.Web.UI.HtmlTextWriter output)
        //    {
        //        SetStyleSheet();
        //        base.Render(output);
        //    }

        //    protected void initVersionAndTitle(string strConfigurationID)
        //    {
        //        ConfigurationID = strConfigurationID;
        //        WorkFlow objWF = new WorkFlow();
        //        DataTable dtConfig = new DataTable();
        //        try
        //        {
        //            string strQry = "SELECT C.CONFIG_MST_PK,C.CONFIG_NAME,C.CONFIG_DESCRIPTION FROM CONFIG_MST_TBL C WHERE UPPER(C.CONFIG_ID)='" + strConfigurationID.ToUpper() + "'";
        //            dtConfig = objWF.GetDataTable(strQry);
        //            if (dtConfig.Rows.Count > 0)
        //            {
        //                if (strReleaseMode == "Release")
        //                {
        //                    Title = dtConfig.Rows[0]["CONFIG_NAME"] + " " + VersionNo;
        //                }
        //                else
        //                {
        //                    Title = dtConfig.Rows[0]["CONFIG_NAME"];
        //                }
        //            }
        //            //For Each objCTL As Control In Me.Controls
        //            //    If TypeOf objCTL Is System.Web.UI.HtmlControls.HtmlForm Then
        //            //        FetchChildDateControls(objCTL)
        //            //    End If
        //            //Next
        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //    }

        //    protected void initVersionAndTitle(string strConfigurationID, string PageHeader)
        //    {
        //        // remove the 2nd parameter once it is completely removed from page level, This just writter to avoid crashes.
        //        string strSql = null;
        //        VersionNo = strVersionNo;
        //        ConfigurationID = strConfigurationID;
        //        if (!string.IsNullOrEmpty(PageHeader))
        //        {
        //            ConfigurationDESC = PageHeader;
        //        }
        //        else
        //        {
        //            strSql = " SELECT NVL(T.CONFIG_DESCRIPTION, ' ') C_DESC FROM CONFIG_MST_TBL T  ";
        //            strSql = strSql + Constants.vbCrLf + " WHERE T.CONFIG_ID = '" + strConfigurationID + "'";
        //            ConfigurationDESC = (new WorkFlow()).ExecuteScaler(strSql);
        //        }

        //        if (strReleaseMode == "Release")
        //        {
        //            Title = ConfigurationDESC + " " + VersionNo;
        //        }
        //        else if (strReleaseMode == "Development")
        //        {
        //            Title = ConfigurationDESC + " " + strConfigurationID;
        //        }
        //        else
        //        {
        //            Title = ConfigurationDESC;
        //        }
        //        //For Each objCTL As Control In Me.Controls
        //        //    If TypeOf objCTL Is System.Web.UI.HtmlControls.HtmlForm Then
        //        //        FetchChildDateControls(objCTL)
        //        //    End If
        //        //Next
        //    }

        //    private void FetchChildDateControls(ref Control OBJCONTROL)
        //    {
        //        if (OBJCONTROL is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //        {
        //            Debug.Print(OBJCONTROL.ID);
        //            DateControlUIFormatting(OBJCONTROL);
        //        }
        //        else if (OBJCONTROL.Controls.Count > 0)
        //        {
        //            foreach (Control objCTL in OBJCONTROL.Controls)
        //            {
        //                FetchChildDateControls(ref objCTL);
        //            }
        //        }
        //    }

        //    public void initDateControlUIFormatting(ref Control objCTL)
        //    {
        //        try
        //        {
        //            //For Each objCTL As Control In ObjControl.Controls
        //            if (objCTL is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //            {
        //                DateControlUIFormatting(objCTL);
        //            }
        //            var _ctls = (from _c in objCTL.Controls where _c is Infragistics.WebUI.WebSchedule.WebDateChooser select _c);
        //            foreach (Control _ctl in _ctls)
        //            {
        //                DateControlUIFormatting(_ctl);
        //            }
        //            _ctls = (from _c in objCTL.Controls where _c is Infragistics.WebUI.UltraWebTab.UltraWebTab select _c);
        //            foreach (Control _ctl in _ctls)
        //            {
        //                initDateControlUIFormatting(ref _ctl);
        //            }
        //            _ctls = (from _c in objCTL.Controls where _c is System.Web.UI.WebControls.Panel select _c);
        //            foreach (Control _ctl in _ctls)
        //            {
        //                initDateControlUIFormatting(ref _ctl);
        //            }
        //            //Next
        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //    }
        //    protected void ClearGrid(ref Infragistics.WebUI.UltraWebGrid.UltraWebGrid Grid)
        //    {
        //        Infragistics.WebUI.UltraWebGrid.UltraGridBand infraBand = null;
        //        foreach (Infragistics.WebUI.UltraWebGrid.UltraGridBand infraBand_loopVariable in Grid.Bands)
        //        {
        //            infraBand = infraBand_loopVariable;
        //            infraBand.Columns.Clear();
        //        }
        //    }
        //    // Qfor 07 Sep 2007 (Amit Singh)
        //    // This Procedure will set focus of DDL controls when ever its having any PostBack
        //    public void setFocus(System.Web.UI.Control cntlName)
        //    {
        //        string S = "<SCRIPT language='Javascript'>document.getElementById('" + cntlName.ID + "').focus() </SCRIPT>";
        //        Page.RegisterStartupScript("focus", S);
        //    }
        //    #region "Page Configuration"
        //    private string M_Configuration_ID;
        //    private string M_Configuration_DESC;
        //    private Int64 M_Configuration_PK;
        //    private string M_Version_No = ConfigurationManager.AppSettings["VersionNo"];
        //    //= M_Configuration_ID & " " & M_Version_No
        //    private string M_Title;
        //    //property for close button added by vimlesh kumar
        //    public int PostBackCount
        //    {
        //        get { return Convert.ToInt32(ViewState["PageClose"]); }
        //        set { ViewState["PageClose"] = value; }
        //    }
        //    //Private M_Business_Type As BusinessType

        //    protected string ConfigurationID
        //    {
        //        get { return M_Configuration_ID; }
        //        set { M_Configuration_ID = value; }
        //    }
        //    protected Int64 ConfigurationPK
        //    {
        //        get { return M_Configuration_PK; }
        //        set { M_Configuration_PK = value; }
        //    }
        //    protected string VersionNo
        //    {
        //        get { return M_Version_No; }
        //        set { M_Version_No = value; }
        //    }
        //    protected string Title
        //    {
        //        get { return M_Title; }
        //        set { M_Title = value; }
        //    }

        //    protected string ConfigurationDESC
        //    {
        //        get { return M_Configuration_DESC; }
        //        set { M_Configuration_DESC = value; }
        //    }
        //    // Property Introduced for Current Business Type 
        //    // First this property will be initialized during login with
        //    // the login user Business Type
        //    protected BusinessType CurrentBusinessType
        //    {
        //        get
        //        {
        //            if ((System.Web.HttpContext.Current.Session["BUSINESSTYPE"] == null))
        //            {
        //                return BusinessType.Both;
        //            }
        //            else
        //            {
        //                return (BusinessType)System.Web.HttpContext.Current.Session["BUSINESSTYPE"];
        //            }

        //        }
        //        set { System.Web.HttpContext.Current.Session["BUSINESSTYPE"] = value; }
        //    }
        //    #endregion

        //    #region " Shared Methods "

        //    // Added in QFOR Date : 07-Nov-2005
        //    // Developer          : Rajesh Raushan
        //    // Project Leader     : Suryadharshan G. Alle
        //    // Description        :
        //    //   These are some shared methods which can be used for different purposes
        //    //   Some methods are for server side validation
        //    // Function to Validate a give string whether that is a Valid Mail or not
        //    //
        //    public static object IsValidEmail(string strMail)
        //    {
        //        if (string.IsNullOrEmpty(strMail.Trim()))
        //            return true;
        //        Regex objRegex = new Regex("\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*");
        //        Match objMatch = null;
        //        objMatch = objRegex.Match(strMail);
        //        return objMatch.Success;
        //    }

        //    // Function to Validate a give string whether that is a Valid Phone or not
        //    public static object IsValidPhone(string strPhone)
        //    {
        //        if (string.IsNullOrEmpty(strPhone.Trim()))
        //            return true;
        //        Regex objRegex = new Regex("^([0-9\\s,.+()/-])*$");
        //        Match objMatch = null;
        //        objMatch = objRegex.Match(strPhone);
        //        return objMatch.Success;
        //    }

        //    // Function to Validate a give string whether that is a Valid Fax or not
        //    public static object IsValidFax(string strFax)
        //    {
        //        if (string.IsNullOrEmpty(strFax.Trim()))
        //            return true;
        //        Regex objRegex = new Regex("^([0-9\\s,.+()/-])*$");
        //        Match objMatch = null;
        //        objMatch = objRegex.Match(strFax);
        //        return objMatch.Success;
        //    }

        //    // Function to Validate a give string whether that is a Valid URL or not
        //    // Dim objRegex As New Regex("http://([\w-]+\.)+[\w-]+(/[\w- ./?%&=]*)?")
        //    // Modified on 22-Feb-2006 Rajesh
        //    public static object IsValidURL(string strURL)
        //    {
        //        if (string.IsNullOrEmpty(strURL.Trim()))
        //            return true;
        //        strURL = strURL.ToLower().Trim();
        //        if (Strings.InStr(strURL.Trim(), " ") > 0)
        //            return false;
        //        // URL will not contain any white space in between
        //        if (strURL.StartsWith("http://"))
        //            strURL = strURL.Substring(7);
        //        // It can start with http://
        //        if (!strURL.StartsWith("www."))
        //            return false;
        //        // then It has to start with www.
        //        strURL = strURL.Substring(4);
        //        if (Strings.InStr(strURL, ".") == 0)
        //            return false;
        //        // there must be one (.) character
        //        if (strURL.StartsWith(".") | strURL.EndsWith("."))
        //            return false;
        //        // however the (.) character can not be at either end
        //        if (strURL.Trim().Length == 0)
        //            return false;
        //        // if we provide url it has to keep some value in url address also
        //        return true;
        //    }

        //    // If a value is DBNull.Value then this will return a white space character object
        //    public static object removeDBNull(object col)
        //    {
        //        if (col == null)
        //        {
        //            return "";
        //        }
        //        else if (object.ReferenceEquals(col, DBNull.Value))
        //        {
        //            return "";
        //        }
        //        return col;
        //    }

        //    // Empty String is not Null. while passing empty string to a number column it will not accept
        //    // to change empty string to DBnull this function can be used
        //    public static object ifDBNull(object col)
        //    {
        //        if (col == null)
        //        {
        //            return DBNull.Value;
        //        }
        //        else if (object.ReferenceEquals(col, DBNull.Value))
        //        {
        //            return DBNull.Value;
        //        }
        //        else if (Convert.ToString(col).Length == 0)
        //        {
        //            return DBNull.Value;
        //        }
        //        else
        //        {
        //            return col;
        //        }
        //    }

        //    // For a number database column Store zero and while mapping to 
        //    // databese parameters pass that to this function.
        //    // It will return the same value if that is not Zero
        //    // If that is zero it will return DBNull which is 
        //    public static object ifDBZero(object col, Int16 Zero = 0)
        //    {
        //        if (Convert.ToInt32(col) == Zero)
        //        {
        //            return DBNull.Value;
        //        }
        //        else
        //        {
        //            return col;
        //        }
        //    }
        //    public static object ifDBZeroLong(object col, Int16 Zero = 0)
        //    {
        //        if (Convert.ToDouble(col) == Zero)
        //        {
        //            return DBNull.Value;
        //        }
        //        else
        //        {
        //            return col;
        //        }
        //    }
        //    //
        //    // This method will return default value if the provided value will be in 
        //    // dbnull.value or nothing or null string
        //    // [ Added on 08-Feb-2006  By Rajesh ]
        //    public static object getDefault(object col, object defaultVal)
        //    {
        //        if (object.ReferenceEquals(col, DBNull.Value))
        //        {
        //            return defaultVal;
        //        }
        //        else if (col == null)
        //        {
        //            return defaultVal;
        //        }
        //        else if (Convert.ToString(col).Trim().Length == 0)
        //        {
        //            return defaultVal;
        //        }
        //        else if (Convert.ToString(col).Trim().ToUpper() == "NULL")
        //        {
        //            return defaultVal;
        //        }
        //        else if (Convert.ToString(col).Trim() == "/  /")
        //        {
        //            return defaultVal;
        //        }
        //        else if (Convert.ToString(col) == "  /  /       :  ")
        //        {
        //            return defaultVal;
        //        }
        //        else if (!Information.IsNumeric(col))
        //        {
        //            return col;
        //        }
        //        else if (object.ReferenceEquals(col, (object)0))
        //        {
        //            return defaultVal;
        //        }
        //        else if (Conversion.Val(col) == 0)
        //        {
        //            return defaultVal;
        //        }
        //        else
        //        {
        //            return col;
        //        }
        //    }

        //    // This function can be used for getting an empty string back if a text does not contain any thing
        //    // other wise return back the same string in trimmed format
        //    public static string trimText(object Text)
        //    {
        //        if (Convert.ToString(Text).Trim().Length > 0)
        //        {
        //            return Convert.ToString(Text).Trim();
        //        }
        //        else
        //        {
        //            return "";
        //        }
        //    }
        //    // Public Shared defaultCommodityGroup As String = "General"
        //    // Public Shared defaultCommodity As String = "General/Non-Haz"
        //    // Modified 02-Mar-2006
        //    public string defaultCommodityGroup
        //    {
        //        get
        //        {
        //            if ((Session["DEF_COMMODITY_GROUP"] != null))
        //            {
        //                return Session["DEF_COMMODITY_GROUP"];
        //            }
        //            return "General";
        //        }
        //    }
        //    // Added 06-Mar-2006 Rajesh
        //    public string defaultCommodity
        //    {
        //        get
        //        {
        //            if ((Session["DEF_COMMODITY_NAME"] != null))
        //            {
        //                return Session["DEF_COMMODITY_NAME"];
        //            }
        //            return "General/Non-Haz";
        //        }
        //    }
        //    // Added 06-Mar-2006 Rajesh
        //    public string defaultCommodityPk
        //    {
        //        get
        //        {
        //            string strSQL = null;
        //            if ((Session["DEF_COMMODITY_PK"] != null))
        //            {
        //                return Session["DEF_COMMODITY_PK"];
        //            }
        //            else
        //            {
        //                try
        //                {
        //                    strSQL = " SELECT COMMODITY_MST_PK FROM COMMODITY_MST_TBL " + " WHERE UPPER(COMMODITY_NAME) = '" + defaultCommodity.ToUpper() + "'";
        //                    Session["DEF_COMMODITY_PK"] = (new WorkFlow()).ExecuteScaler(strSQL);
        //                    return Session["DEF_COMMODITY_PK"];
        //                }
        //                catch (Exception ex)
        //                {
        //                }
        //            }
        //            return "";
        //        }
        //    }
        //    // This method sets a dropdown to a given text if that item is availabe [ Rajesh ]
        //    public Int16 GetIndexOfText(ref DropDownList DropDown, string strSearchText)
        //    {
        //        Int16 RowCnt = default(Int16);
        //        for (RowCnt = 0; RowCnt <= DropDown.Items.Count - 1; RowCnt++)
        //        {
        //            if (DropDown.Items[RowCnt].Value == GENERAL)
        //            {
        //                return RowCnt;
        //            }
        //        }
        //        for (RowCnt = 0; RowCnt <= DropDown.Items.Count - 1; RowCnt++)
        //        {
        //            if (DropDown.Items[RowCnt].Text.ToUpper().Trim() == strSearchText.ToUpper().Trim())
        //            {
        //                return RowCnt;
        //            }
        //        }
        //        // DropDown.SelectedValue = GENERAL
        //        return -1;
        //    }
        //    // This method desable all columns of all bands in a grid [ Rajesh ]
        //    public static void DisableGrid(ref Infragistics.WebUI.UltraWebGrid.UltraWebGrid grid)
        //    {
        //        Int16 i = default(Int16);
        //        Int16 j = default(Int16);
        //        for (i = 0; i <= grid.Bands.Count - 1; i++)
        //        {
        //            for (j = 0; j <= grid.Bands[i].Columns.Count - 1; j++)
        //            {
        //                grid.Bands[i].Columns[j].AllowUpdate = Infragistics.WebUI.UltraWebGrid.AllowUpdate.No;
        //            }
        //        }
        //    }

        //    #endregion

        //    #region " Print in Acrobat function [ Common to All Reports ] "
        //    // Rajesh [ 14-Feb-2006 ]
        //    protected void Print_In_Acrobat(CrystalDecisions.CrystalReports.Engine.ReportDocument rptDoc)
        //    {
        //        string Fname = null;
        //        CrystalDecisions.Shared.ExportOptions crExportOptions = null;
        //        CrystalDecisions.Shared.DiskFileDestinationOptions crDiskFileDestinationOptions = null;

        //        rptDoc.PrintOptions.PaperSize = CrystalDecisions.Shared.PaperSize.PaperA4;
        //        rptDoc.PrintOptions.PaperOrientation = CrystalDecisions.Shared.PaperOrientation.Portrait;

        //        //Fname = "c:\" & Session.SessionID.ToString & ".pdf"
        //        Fname = Server.MapPath(".") + "\\" + Session.SessionID.ToString() + ".pdf";
        //        crDiskFileDestinationOptions = new CrystalDecisions.Shared.DiskFileDestinationOptions();

        //        crDiskFileDestinationOptions.DiskFileName = Fname;
        //        crExportOptions = rptDoc.ExportOptions;
        //        var _with12 = crExportOptions;
        //        _with12.DestinationOptions = crDiskFileDestinationOptions;
        //        _with12.ExportDestinationType = CrystalDecisions.Shared.ExportDestinationType.DiskFile;
        //        _with12.ExportFormatType = CrystalDecisions.Shared.ExportFormatType.PortableDocFormat;

        //        rptDoc.Export();
        //        rptDoc.Close();
        //        rptDoc.Dispose();
        //        Response.ClearHeaders();
        //        Response.ClearContent();
        //        Response.ContentType = "application/pdf";
        //        Response.WriteFile(Fname);

        //        Response.Flush();
        //        Response.Close();
        //        System.IO.File.Delete(Fname);
        //    }

        //    #endregion

        //    #region " Current User Name "
        //    // Getting Current Logged in Uder Name or ID [ Rajesh - 19-Feb-2006 ]
        //    public string CurrentUser
        //    {
        //        get
        //        {
        //            try
        //            {
        //                string strSQL = null;
        //                WorkFlow ObjWk = new WorkFlow();
        //                if (ViewState["CURRENT_LOGGED_IN_USER"] == null)
        //                {
        //                    if (UserID == false)
        //                    {
        //                        strSQL = " Select USER_NAME from USER_MST_TBL where USER_MST_PK = " + Session["USER_PK"];
        //                        ViewState["CURRENT_LOGGED_IN_USER"] = ObjWk.ExecuteScaler(strSQL);
        //                    }
        //                    else
        //                    {
        //                        strSQL = " Select USER_ID from USER_MST_TBL where USER_MST_PK = " + Session["USER_PK"];
        //                        ViewState["CURRENT_LOGGED_IN_USER"] = ObjWk.ExecuteScaler(strSQL);
        //                    }
        //                }
        //                return ViewState["CURRENT_LOGGED_IN_USER"];
        //            }
        //            catch (Exception ex)
        //            {
        //            }
        //            return "";
        //        }
        //    }

        //    #endregion

        //    #region " BOF PK "
        //    // 24-Apr-2006
        //    public long BofPK
        //    {
        //        // If problem in getting BOF PK then this property will return -1
        //        get
        //        {
        //            try
        //            {
        //                string strSQL = null;
        //                WorkFlow ObjWk = new WorkFlow();
        //                if (ViewState["BOF_PK"] == null)
        //                {
        //                    strSQL = " Select FREIGHT_ELEMENT_MST_PK from FREIGHT_ELEMENT_MST_TBL " + "  where UPPER(FREIGHT_ELEMENT_ID) = 'BOF' ";
        //                    ViewState["BOF_PK"] = ObjWk.ExecuteScaler(strSQL);
        //                }
        //                return ViewState["BOF_PK"];
        //            }
        //            catch (Exception ex)
        //            {
        //                return -1;
        //            }
        //        }
        //    }

        //    #endregion

        //    #region " Customer Category "
        //    // 27-Apr-2006 Rajesh
        //    public enum EnumCustomerCategory
        //    {
        //        Shipper = 1,
        //        Consignee = 2,
        //        Notify = 3,
        //        Forwarder = 4,
        //        Customer = 5
        //    }
        //    //Snigdharani - 20/12/2008 - Fetching Text and Tooltip for the parameters (Setting Form)
        //    public enum Parameters
        //    {
        //        GENERAL_CARGO_FK = 1,
        //        HAZ_CARGO_FK = 2,
        //        REEFER_CARGO_FK = 3,
        //        ODC_CARGO_FK = 4,
        //        COST_PRECARRIAGE_FK = 5,
        //        COST_ONCARRIAGE_FK = 6,
        //        COST_BOF_FK = 7,
        //        COST_AFC_FK = 8,
        //        COST_FRT_FK = 9,
        //        COST_TPC_FK = 10,
        //        UOM_KG = 11,
        //        UOM_TON = 12,
        //        UOM_LBS = 13,
        //        FRT_BOF_FK = 14,
        //        FRT_AFC_FK = 15,
        //        FRT_DET_CHARGE_FK = 16,
        //        FRT_DEM_CHARGE_FK = 17,
        //        SALES_MANAGER = 18,
        //        SALES_EXECUTIVE = 19,
        //        CONTAINER_CHECK = 20
        //        //This is currently not required.
        //    }
        //    private DataSet M_Parameters_DS = new DataSet();
        //    public static DataSet getParameter(Parameters Param)
        //    {
        //        string Parameter = null;
        //        WorkFlow objWK = new WorkFlow();
        //        System.Data.OracleClient.OracleTransaction TRAN = null;
        //        objWK.OpenConnection();
        //        TRAN = objWK.MyConnection.BeginTransaction();
        //        objWK.MyCommand.Parameters.Clear();
        //        Parameter = Enum.GetName(typeof(Parameters), Param);
        //        var _with13 = objWK.MyCommand;
        //        _with13.CommandType = CommandType.StoredProcedure;
        //        _with13.CommandText = objWK.MyUserName + ".SRR_SEA_TBL_PKG.SRR_SEA_TBL_INS";
        //        _with13.Parameters.Add("PARAMETER_ID_IN", Parameter).Direction = ParameterDirection.Input;
        //        _with13.Parameters.Add("PARAMETER_CUR", System.Data.OracleClient.OracleType.Cursor).Direction = ParameterDirection.Output;

        //        //M_Parameters_DS = 
        //    }
        //    //End Snigdharani


        //    private static DataTable M_CustCategoryDT = new DataTable();
        //    public static long getCategory(EnumCustomerCategory CustType)
        //    {
        //        if (M_CustCategoryDT.Rows.Count == 0)
        //        {
        //            string strSQL = null;
        //            strSQL = " SELECT CUSTOMER_CATEGORY_MST_PK, CUSTOMER_CATEGORY_ID " + "   FROM CUSTOMER_CATEGORY_MST_TBL " + "  WHERE ACTIVE_FLAG = 1 ";
        //            M_CustCategoryDT = (new WorkFlow()).GetDataTable(strSQL);
        //        }
        //        string CategoryID = null;
        //        CategoryID = Enum.GetName(typeof(EnumCustomerCategory), CustType);
        //        DataRow DR = null;
        //        foreach (DataRow DR_loopVariable in M_CustCategoryDT.Rows)
        //        {
        //            DR = DR_loopVariable;
        //            if (Convert.ToString(getDefault(DR["CUSTOMER_CATEGORY_ID"], "")).ToLower().Trim() == CategoryID.ToLower().Trim())
        //            {
        //                return DR["CUSTOMER_CATEGORY_MST_PK"];
        //            }
        //        }
        //        throw new System.Exception(CategoryID + " is not an active customer category.");
        //    }

        //    #endregion

        //    #region " Copy To All Button "
        //    // 04-Jan-2006        : Akhilesh
        //    // Added for Enable Disable Copy button according to revision allowed
        //    //
        //    //<Messaging.MessagingDescription("For Enable")>
        //    protected void CopyEnableDisable(string RefNo, ref object btnCopy)
        //    {
        //        Int16 indexOf = default(Int16);
        //        if (RefNo.Trim().Length > 0)
        //        {
        //            indexOf = RefNo.IndexOf("/");
        //            if (indexOf > 0)
        //            {
        //                indexOf = RefNo.Substring(indexOf + 1);
        //                if (indexOf == 9)
        //                {
        //                    btnCopy.Enabled = false;
        //                }
        //            }
        //        }
        //        else
        //        {
        //            btnCopy.Enabled = false;
        //        }
        //    }

        //    #endregion

        //    #region " Global Formatting Properties "

        //    //*******************************************************************************************************
        //    //Code for Time Format only
        //    //Author: Amit Singh
        //    //Date: 14-June-2007
        //    //******************************************************************************************************* 
        //    protected string M_TimeFormat = "HH:mm";
        //    public string timeFormat
        //    {
        //        get { return M_TimeFormat; }
        //        set { M_TimeFormat = value; }
        //    }

        //    //*******************************************************************************************************
        //    //Code for General Currency Format
        //    //Author: Rajesh Raushan
        //    //Date: 11-Feb-2006
        //    //*******************************************************************************************************       

        //    public string amountFormat
        //    {
        //        get
        //        {
        //            if (forClietSide)
        //            {
        //                return Application["CLIENT_AMOUNT_FORMAT"];
        //            }
        //            else
        //            {
        //                return Application["AMOUNT_FORMAT"];
        //            }
        //        }
        //    }
        //    public string quantityFormat
        //    {
        //        get
        //        {
        //            if (forClietSide)
        //            {
        //                return Application["CLIENT_QUANTITY_FORMAT"];
        //            }
        //            else
        //            {
        //                return Application["QUANTITY_FORMAT"];
        //            }
        //        }
        //    }

        //    public string weightFormat
        //    {
        //        get
        //        {
        //            if (forClietSide)
        //            {
        //                return Application["CLIENT_WEIGHT_FORMAT"];
        //            }
        //            else
        //            {
        //                return Application["WEIGHT_FORMAT"];
        //            }
        //        }
        //    }
        //    public string volumeFormat
        //    {
        //        get
        //        {
        //            if (forClietSide)
        //            {
        //                return Application["CLIENT_VOLUME_FORMAT"];
        //            }
        //            else
        //            {
        //                return Application["VOLUME_FORMAT"];
        //            }
        //        }
        //    }

        //    public string exchangeFormat
        //    {
        //        get
        //        {
        //            if (forClietSide)
        //            {
        //                return Application["CLIENT_EXCHANGE_FORMAT"];
        //            }
        //            else
        //            {
        //                return Application["EXCHANGE_FORMAT"];
        //            }
        //        }
        //    }

        //    // 13-Feb-2006 8:30 PM 
        //    //Modified by akhilesh
        //    //Reason: Making format globally configurable
        //    public string formatTextBox
        //    {
        //        get
        //        {
        //            //Function changed as using val() function is giving error is cases for changing formatted strings
        //            //cdbl() replaces val()
        //            if (Convert.ToString(removeDBNull(strTextBox)).Trim().Length > 0)
        //            {
        //                if (formatStyle == formatText.Amount)
        //                {
        //                    strTextBox = Strings.Format(Convert.ToDouble(strTextBox), Application["AMOUNT_FORMAT"].ToString());
        //                }
        //                else if (formatStyle == formatText.Weight)
        //                {
        //                    strTextBox = Strings.Format(Convert.ToDouble(strTextBox), Application["WEIGHT_FORMAT"].ToString());
        //                }
        //                else if (formatStyle == formatText.Volume)
        //                {
        //                    strTextBox = Strings.Format(Convert.ToDouble(strTextBox), Application["VOLUME_FORMAT"].ToString());
        //                }
        //                else if (formatStyle == formatText.Exchange)
        //                {
        //                    strTextBox = Strings.Format(Convert.ToDouble(strTextBox), Application["EXCHANGE_FORMAT"].ToString());
        //                }
        //            }
        //            return strTextBox;
        //        }
        //    }
        //    // [ 18-Feb-2006 Rajesh ]
        //    // This method is useful for assigning All these formats to any Business class object
        //    // so that these formats can be used inside the class file as well
        //    public void AssignFormat(ref object objClass)
        //    {
        //        try
        //        {
        //            objClass.AmountFormat = amountFormat;
        //            objClass.WeightFormat = weightFormat;
        //            objClass.VolumeFormat = volumeFormat;
        //            objClass.ExchangeFormat = exchangeFormat;
        //        }
        //        catch
        //        {
        //        }
        //    }
        //    public string dateFormat
        //    {
        //        get { return Application["dateFormat"]; }
        //    }

        //    public string GetMailServer
        //    {
        //        get { return ConfigurationManager.AppSettings["MailServer"]; }
        //    }
        //    //Code Author: Rajesh Raushan
        //    //Date: 6th March 2006
        //    //Purpose: Change the date format of the date control
        //    //How to Use: If the controls are few in number the pass the control name else pass the form name i.e me.
        //    public virtual void FormatWebDateTimeControl(ref Control oControl, bool WithTime)
        //    {
        //        Control frmCtrl = null;
        //        if (oControl is Infragistics.WebUI.WebDataInput.WebDateTimeEdit)
        //        {
        //            if (WithTime == true)
        //            {
        //                ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)oControl).DisplayModeFormat = dateFormat + " HHmm";
        //                ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)oControl).EditModeFormat = dateFormat + " HH:mm";
        //            }
        //            else
        //            {
        //                ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)oControl).DisplayModeFormat = dateFormat;
        //                ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)oControl).EditModeFormat = dateFormat;
        //            }
        //        }
        //        else
        //        {
        //            if (oControl.HasControls())
        //            {
        //                foreach (Control frmCtrl_loopVariable in oControl.Controls)
        //                {
        //                    frmCtrl = frmCtrl_loopVariable;
        //                    FormatWebDateTimeControl(ref frmCtrl, WithTime);
        //                }
        //            }
        //        }
        //    }

        //    public void FormatWebDateTimeControl(ref Control frmControl, bool WithTime, params string[] Args)
        //    {
        //        int I = 0;
        //        Control oControl = null;
        //        if (Args.Length <= 0)
        //            return;
        //        // No arguments passed.
        //        for (I = 0; I <= Information.UBound(Args, 1); I++)
        //        {
        //            oControl = frmControl.FindControl(Args[I]);
        //            if (oControl is Infragistics.WebUI.WebDataInput.WebDateTimeEdit)
        //            {
        //                if (WithTime == true)
        //                {
        //                    ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)oControl).DisplayModeFormat = dateFormat + " HHmm";
        //                    ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)oControl).EditModeFormat = dateFormat + " HH:mm";
        //                }
        //                else
        //                {
        //                    ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)oControl).DisplayModeFormat = dateFormat;
        //                    ((Infragistics.WebUI.WebDataInput.WebDateTimeEdit)oControl).EditModeFormat = dateFormat;
        //                }
        //            }
        //        }
        //    }
        //    #endregion

        //    #region "Commodity Group"
        //    int CommodityGrp = 0;
        //    public int GENERAL
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(1);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int HAZARDOUS
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(2);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int REEFER
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(3);
        //            return CommodityGrp;
        //        }
        //    }

        //    public int ODC
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(4);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int DGS
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(5);
        //            return CommodityGrp;
        //        }
        //    }
        //    //MODIFIED BY LATHA FOR PARAMETERS
        //    public int PRECARRIAGE
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(5);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int ONCARRIAGE
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(6);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int BOF
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(7);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int AFC
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(8);
        //            return CommodityGrp;
        //        }
        //    }
        //    //added by AREEF  on 24-07-07
        //    public int OPERATORCOST
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(12);
        //            return CommodityGrp;
        //        }
        //    }
        //    //added by Areef on 24-07-07
        //    public int TRANPORTERCOST
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(13);
        //            return CommodityGrp;
        //        }
        //    }

        //    //added by AREEF  on 24-07-07
        //    public int SALESMANAGER
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(14);
        //            return CommodityGrp;
        //        }
        //    }
        //    //added by AREEF  on 24-07-07
        //    public int SALESEXECUTIVE
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(15);
        //            return CommodityGrp;
        //        }
        //    }
        //    //added by AREEF on 01-08-07
        //    public int FRT_BOF
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(16);
        //            return CommodityGrp;
        //        }
        //    }
        //    //added by AREEF on 01-08-07
        //    public int FRT_AFC
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(17);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int FRT_AIF
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(20);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int FRT_FAC
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(21);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int FRT_MIS
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(22);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int COST_INCENTIVE
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(23);
        //            return CommodityGrp;
        //        }
        //    }
        //    //added by AREEF on 01-08-07
        //    public int FRT_DET
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(18);
        //            return CommodityGrp;
        //        }
        //    }
        //    //added by AREEF on 01-08-07
        //    public int FRT_DEM
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(19);
        //            return CommodityGrp;
        //        }
        //    }
        //    //Manoharan 08Feb07: to add UOM values
        //    public int KG
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(9);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int TON
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(10);
        //            return CommodityGrp;
        //        }
        //    }
        //    public int LBS
        //    {
        //        get
        //        {
        //            CommodityGrp = getCommodityGrp(11);
        //            return CommodityGrp;
        //        }
        //    }
        //    //Add By AREEF for fetching the Path of the LOGO which we are using in Reporting
        //    public string Location_image(long Locpk)
        //    {
        //        string strSql = null;
        //        string strLogoName = null;
        //        try
        //        {
        //            strSql = "Select LOGO_FILE_PATH from Location_Mst_Tbl lmst where lmst.location_mst_pk =" + Locpk;
        //            strLogoName = (new WorkFlow()).ExecuteScaler(strSql);
        //            if (string.IsNullOrEmpty(strLogoName))
        //            {
        //                strLogoName = DefaultLogo;
        //            }
        //            return strLogoName;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    public DataSet ds_image()
        //    {
        //        //Created By Manoharan on 19Sep2007: for common function to avoid repeated code
        //        string ImgLogoFileName = Session["ImageFile"];
        //        DataSet dsImage = new DataSet();
        //        string ImgLogoPath = Server.MapPath("..\\..") + "\\Logos\\" + ImgLogoFileName;
        //        if (!File.Exists(ImgLogoPath))
        //            ImgLogoPath = Server.MapPath("..\\..") + "\\Logos\\" + DefaultLogo;
        //        dsImage = ImageTable(ImgLogoPath);
        //        dsImage.ReadXmlSchema(Server.MapPath("..\\07Reports") + "\\ckim.xsd");
        //        return dsImage;
        //    }

        //    // ADD by AREEF for dynamic Logo Printing in reports. Added on 07-08-07
        //    public static DataSet ImageTable(string ImageFile)
        //    {
        //        DataSet dsBody = new DataSet();
        //        DataSet GridDS = new DataSet();
        //        DataTable data = new DataTable();
        //        DataRow row = null;
        //        try
        //        {
        //            data.TableName = "Images";
        //            data.Columns.Add("img", System.Type.GetType("System.Byte[]"));
        //            FileStream fs = new FileStream(ImageFile, FileMode.Open);
        //            BinaryReader br = new BinaryReader(fs);
        //            row = data.NewRow();
        //            row[0] = br.ReadBytes(br.BaseStream.Length);
        //            data.Rows.Add(row);
        //            dsBody.Tables.Add(data);
        //            br = null;
        //            fs.Close();
        //            fs = null;
        //            return dsBody;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }

        //    }
        //    public int getCommodityGrp(int type)
        //    {
        //        string strQuery = null;
        //        System.Data.OracleClient.OracleDataReader dr = null;
        //        //Manoharan 08Feb07: modified to add UOM values
        //        strQuery = "SELECT P.GENERAL_CARGO_FK,P.HAZ_CARGO_FK,P.REEFER_CARGO_FK,P.ODC_CARGO_FK ,P.COST_PRECARRIAGE_FK," + "P.COST_ONCARRIAGE_FK, P.COST_BOF_FK,P.COST_AFC_FK, P.UOM_KG, P.UOM_TON, P.UOM_LBS, P.COST_FRT_FK, P.COST_TPC_FK, P.SALES_MANAGER, P.SALES_EXECUTIVE, P.FRT_BOF_FK, P.FRT_AFC_FK, P.FRT_DET_CHARGE_FK, P.FRT_DEM_CHARGE_FK,P.FRT_AIF_FK,P.FRT_FAC_FK,P.FRT_MIS_FK, P.COST_INCENTIVE_FK FROM PARAMETERS_TBL P";
        //        dr = (new WorkFlow()).GetDataReader(strQuery);
        //        try
        //        {
        //            while (dr.Read())
        //            {
        //                switch (type)
        //                {
        //                    case 1:
        //                        //general
        //                        return dr[0];
        //                    case 2:
        //                        //Haz
        //                        return dr[1];
        //                    case 3:
        //                        //reefer
        //                        return dr[2];
        //                    case 4:
        //                        //ODC
        //                        return dr[3];
        //                    case 5:
        //                        //precarriage
        //                        return dr[4];
        //                    case 6:
        //                        //oncarriage
        //                        return dr[5];
        //                    case 7:
        //                        //bof
        //                        return dr[6];
        //                    case 8:
        //                        //afc
        //                        return dr[7];
        //                    //Manoharan 08Feb07: to add UOM values
        //                    case 9:
        //                        //kg
        //                        return dr[8];
        //                    case 10:
        //                        //ton
        //                        return dr[9];
        //                    case 11:
        //                        //lbs
        //                        return dr[10];
        //                    case 12:
        //                        // operator cost
        //                        return dr[11];
        //                    case 13:
        //                        // transporter cost
        //                        return dr[12];
        //                    case 14:
        //                        //sales Manager
        //                        return dr[13];
        //                    case 15:
        //                        //sales Executive
        //                        return dr[14];
        //                    case 16:
        //                        //Freight Elements BOF
        //                        return dr[15];
        //                    case 17:
        //                        //Freight Elements AFC
        //                        return dr[16];
        //                    case 18:
        //                        //Freight Elements Detention Charges
        //                        return dr[17];
        //                    case 19:
        //                        //Freight Elements Demurage Charges
        //                        return dr[18];
        //                    case 20:
        //                        //All in Freight  (AIF)
        //                        return dr[19];
        //                    case 21:
        //                        //Forwarder Agency Commission (FAC)
        //                        return dr[20];
        //                    case 22:
        //                        //Miscellaneous Charges
        //                        return dr[21];
        //                    case 23:
        //                        //Incentive/Tea Cost
        //                        return dr[22];
        //                }
        //            }
        //            return 0;
        //        }
        //        catch (Exception ex)
        //        {
        //            return 0;
        //        }
        //        finally
        //        {
        //            dr.Close();
        //        }
        //    }

        //    public void SelectGeneralCommodity(ref System.Web.UI.WebControls.DropDownList ddlCommodity)
        //    {
        //        Int16 i = default(Int16);
        //        int isFound = 0;
        //        ListItem lstItem = new ListItem();
        //        ddlCommodity.SelectedValue = GENERAL;
        //        //For i = 0 To ddlCommodity.Items.Count - 1
        //        //    lstItem = ddlCommodity.Items(i)
        //        //    isFound = lstItem.Text.ToString().ToUpper().IndexOf("GEN", 0)
        //        //    If isFound >= 0 Then
        //        //        ddlCommodity.SelectedValue = lstItem.Value
        //        //        Exit For
        //        //    End If
        //        //Next
        //    }

        //    #endregion

        //    #region "set Default key on enter press"
        //    // Qfor 08 Mar 2007 [vimlesh kumar]
        //    // This Function will be used for setting default key on form.
        //    public void SetDefaultButton(Page pge, Button btn)
        //    {
        //        System.Text.StringBuilder sbScript = new System.Text.StringBuilder();
        //        //StringBuilder(sbScript = New StringBuilder)
        //        sbScript.Append("<script language='JavaScript' type='text/javascript'>");
        //        // sbScript.Append("<!--")
        //        sbScript.Append("function DocumentOnKeyPress()");
        //        sbScript.Append("{");
        //        sbScript.Append(" if ((event.keyCode) && (event.keyCode == 13)) ");
        //        sbScript.Append(" {");
        //        sbScript.Append(" document.getElementById('" + btn.UniqueID + "').click(); ");
        //        sbScript.Append(" return false; ");
        //        sbScript.Append(" }");
        //        sbScript.Append(" else");
        //        sbScript.Append(" {");
        //        sbScript.Append(" return true; ");
        //        sbScript.Append(" }");
        //        sbScript.Append("}");
        //        sbScript.Append("document.onkeypress = DocumentOnKeyPress;");
        //        //sbScript.Append("//-->")
        //        sbScript.Append("</script>");
        //        Page.RegisterClientScriptBlock("AssignEnterKeyScript", sbScript.ToString());
        //    }
        //    #endregion

        //    #region "Auto Buying Cost in PIA for JC"
        //    //Manoharan 14052007: to insert a row in JC Purchase Invoice Entry Grid for Freight cost
        //    public object CreateNewRow_PIA(string JcPk, bool isFCL, string CurrPk, byte Process, byte Biz)
        //    {

        //        WorkFlow objWF = new WorkFlow();
        //        System.Data.OracleClient.OracleCommand objCommand = new System.Data.OracleClient.OracleCommand();
        //        OracleTransaction objTran = null;
        //        System.Data.OracleClient.OracleCommand insPurchaseInvDetails = new System.Data.OracleClient.OracleCommand();
        //        DataSet dsPia = null;
        //        string str = null;
        //        string strIns = null;
        //        long Ret = 0;
        //        Int16 intDel = default(Int16);
        //        Int16 intIns = default(Int16);

        //        try
        //        {
        //            objWF.OpenConnection();
        //            objTran = objWF.MyConnection.BeginTransaction();

        //            //Sea
        //            if (Biz == 2)
        //            {
        //                str = "  delete from job_trn_sea_exp_pia p where p.job_card_sea_exp_fk = " + JcPk;
        //                str += " and p.cost_element_mst_fk = ( select c.cost_element_mst_pk from cost_element_mst_tbl c, parameters_tbl p1 where c.cost_element_mst_pk = p1.cost_frt_fk)";

        //                //FCL - sea export
        //                if (isFCL)
        //                {
        //                    dsPia = getBuyingCost(JcPk, 1, 1, 2);
        //                    //LCL
        //                }
        //                else
        //                {
        //                    dsPia = getBuyingCost(JcPk, 2, 1, 2);
        //                }

        //                var _with14 = objCommand;
        //                _with14.Connection = objWF.MyConnection;
        //                _with14.Transaction = objTran;
        //                _with14.CommandType = CommandType.Text;
        //                _with14.CommandText = str;
        //                intDel = _with14.ExecuteNonQuery;

        //                if ((dsPia != null))
        //                {
        //                    if (dsPia.Tables[0].Rows.Count == 1)
        //                    {
        //                        var _with15 = insPurchaseInvDetails;
        //                        _with15.Connection = objWF.MyConnection;
        //                        _with15.CommandType = CommandType.StoredProcedure;
        //                        _with15.Transaction = objTran;
        //                        _with15.CommandText = objWF.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_PIA_INS";
        //                        var _with16 = _with15.Parameters;
        //                        insPurchaseInvDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JcPk).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("COST_ELEMENT_MST_FK_IN", dsPia.Tables[0].Rows[0][2]).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("VENDOR_KEY_IN", dsPia.Tables[0].Rows[0][1]).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("CURRENCY_MST_FK_IN", CurrPk).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("ESTIMATED_AMT_IN", dsPia.Tables[0].Rows[0][4]).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("VENDOR_MST_FK_IN", dsPia.Tables[0].Rows[0][0]).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("INVOICE_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("INVOICE_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("INVOICE_AMT_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("TAX_PERCENTAGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("TAX_AMT_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("ATTACHED_FILE_NAME_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("RETURN_VALUE", Ret).Direction = ParameterDirection.Output;
        //                        intIns = _with15.ExecuteNonQuery();
        //                    }
        //                }

        //                if (intDel > 0 | intIns > 0)
        //                {
        //                    objTran.Commit();
        //                }
        //                else
        //                {
        //                    objTran.Rollback();
        //                }
        //                //Air
        //            }
        //            else
        //            {
        //                str = "delete from job_trn_air_exp_pia p where p.job_card_air_exp_fk = 1545";
        //                str += "and p.cost_element_mst_fk = ( select c.cost_element_mst_pk from cost_element_mst_tbl c, parameters_tbl p1 where c.cost_element_mst_pk = p1.cost_frt_fk)";

        //                dsPia = getBuyingCost(JcPk, 0, 1, 1);

        //                var _with17 = objCommand;
        //                _with17.Connection = objWF.MyConnection;
        //                _with17.Transaction = objTran;
        //                _with17.CommandType = CommandType.Text;
        //                _with17.CommandText = str;
        //                intDel = _with17.ExecuteNonQuery;

        //                if ((dsPia != null))
        //                {
        //                    if (dsPia.Tables[0].Rows.Count == 1)
        //                    {
        //                        var _with18 = insPurchaseInvDetails;
        //                        _with18.Connection = objWF.MyConnection;
        //                        _with18.CommandType = CommandType.StoredProcedure;
        //                        _with18.Transaction = objTran;
        //                        _with18.CommandText = objWF.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_PIA_INS";
        //                        var _with19 = _with18.Parameters;
        //                        insPurchaseInvDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JcPk).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("COST_ELEMENT_MST_FK_IN", dsPia.Tables[0].Rows[0][2]).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("VENDOR_KEY_IN", dsPia.Tables[0].Rows[0][1]).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("CURRENCY_MST_FK_IN", CurrPk).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("ESTIMATED_AMT_IN", dsPia.Tables[0].Rows[0][4]).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("VENDOR_MST_FK_IN", dsPia.Tables[0].Rows[0][0]).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("INVOICE_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("INVOICE_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("INVOICE_AMT_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("TAX_PERCENTAGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("TAX_AMT_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("ATTACHED_FILE_NAME_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //                        insPurchaseInvDetails.Parameters.Add("RETURN_VALUE", Ret).Direction = ParameterDirection.Output;
        //                        intIns = _with18.ExecuteNonQuery();
        //                    }
        //                }
        //                if (intDel > 0 | intIns > 0)
        //                {
        //                    objTran.Commit();
        //                }
        //                else
        //                {
        //                    objTran.Rollback();
        //                }
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            objTran.Rollback();
        //            throw ex;
        //        }
        //        finally
        //        {
        //            objWF.MyConnection.Close();
        //        }
        //    }
        //    public DataSet getBuyingCost(string JcPk, byte Cargo, byte Process, byte Biz)
        //    {
        //        string strSql = null;
        //        WorkFlow objWF = new WorkFlow();

        //        //Export
        //        if (Process == 1)
        //        {
        //            //Export-Sea
        //            if (Biz == 2)
        //            {
        //                //Fcl                      
        //                if (Cargo == 1)
        //                {
        //                    strSql += "SELECT VM.VENDOR_MST_PK,";
        //                    strSql += "       max(VM.VENDOR_ID),";
        //                    strSql += "       max(cem.cost_element_mst_pk),";
        //                    strSql += "       MAX(CEM.COST_ELEMENT_ID),";
        //                    strSql += "              SUM((NVL(get_ex_rate(CTS.CURRENCY_MST_FK,";
        //                    strSql += "                                   " + Session["CURRENCY_MST_PK"] + " ,";
        //                    strSql += "                                   SYSDATE),";
        //                    strSql += "                       1) * CONT.FCL_APP_RATE) * BTS.NO_OF_BOXES) ESTIMATED_AMT";
        //                    strSql += "  FROM CONT_MAIN_SEA_TBL       CMST,";
        //                    strSql += "       CONT_TRN_SEA_FCL_LCL    CTS,";
        //                    strSql += "       CONT_TRN_SEA_FCL_RATES  CONT,";
        //                    strSql += "       JOB_CARD_TRN    JOB,";
        //                    strSql += "       BOOKING_MST_TBL         BST,";
        //                    strSql += "       BOOKING_TRN BTS,";
        //                    strSql += "       OPERATOR_MST_TBL        OPE,";
        //                    strSql += "       VENDOR_MST_TBL          VM,";
        //                    strSql += "       PARAMETERS_TBL          PAR,";
        //                    strSql += "       COST_ELEMENT_MST_TBL    CEM";
        //                    strSql += " WHERE CMST.CONT_MAIN_SEA_PK = CTS.CONT_MAIN_SEA_FK";
        //                    strSql += "   AND JOB.BOOKING_MST_FK = BST.BOOKING_MST_PK";
        //                    strSql += "   AND BST.BOOKING_MST_PK = BTS.BOOKING_MST_FK";
        //                    strSql += "   AND CMST.OPERATOR_MST_FK = BST.CARRIER_MST_FK";
        //                    strSql += "   AND CTS.PORT_MST_POL_FK = BST.PORT_MST_POL_FK";
        //                    strSql += "   AND CTS.PORT_MST_POD_FK = BST.PORT_MST_POD_FK";
        //                    strSql += "   AND CMST.OPERATOR_MST_FK = OPE.OPERATOR_MST_PK";
        //                    strSql += "   AND PAR.COST_FRT_FK = CEM.COST_ELEMENT_MST_PK";
        //                    strSql += "   AND CMST.CARGO_TYPE = 1";
        //                    strSql += "   AND CMST.CONT_APPROVED = 1";
        //                    strSql += "   AND CMST.ACTIVE=1";
        //                    strSql += "   AND VM.VENDOR_ID = OPE.OPERATOR_ID";
        //                    strSql += "   AND CMST.COMMODITY_GROUP_FK = JOB.COMMODITY_GROUP_FK";
        //                    strSql += "   AND SYSDATE >= CTS.VALID_FROM";
        //                    strSql += "   AND SYSDATE <= case when CTS.VALID_TO is not null then CTS.VALID_TO else sysdate end AND CONT.CONTAINER_TYPE_MST_FK = BTS.CONTAINER_TYPE_MST_FK AND JOB.JOB_CARD_TRN_PK = " + JcPk + " AND CONT.CONT_TRN_SEA_FK = CTS.CONT_TRN_SEA_PK";
        //                    strSql += " group by VM.VENDOR_MST_PK";
        //                    //Lcl
        //                }
        //                else if (Cargo == 2 | Cargo == 4)
        //                {
        //                    strSql = " SELECT VM.VENDOR_MST_PK, max(VM.VENDOR_ID), max(cem.cost_element_mst_pk), MAX(CEM.COST_ELEMENT_ID), SUM( NVL( get_ex_rate(CTS.CURRENCY_MST_FK,  ";
        //                    strSql += " " + Session["CURRENCY_MST_PK"] + " , SYSDATE),1) * (case when uom.dimention_id like '%CBM' then ";
        //                    strSql += " ( CASE WHEN (CTS.LCL_APPROVED_RATE * nvl(BST.VOLUME_IN_CBM,0)) < CTS.LCL_APPROVED_MIN_RATE THEN CTS.LCL_APPROVED_MIN_RATE ";
        //                    strSql += " WHEN (CTS.LCL_APPROVED_RATE * nvl(BST.VOLUME_IN_CBM,0)) > CTS.LCL_APPROVED_MIN_RATE THEN (CTS.LCL_APPROVED_RATE * nvl(BST.VOLUME_IN_CBM,0))  ";
        //                    strSql += " END ) else ( CASE WHEN (CTS.LCL_APPROVED_RATE * nvl(BST.CHARGEABLE_WEIGHT,0)/1000) < CTS.LCL_APPROVED_MIN_RATE THEN CTS.LCL_APPROVED_MIN_RATE ";
        //                    strSql += " WHEN (CTS.LCL_APPROVED_RATE * nvl(BST.CHARGEABLE_WEIGHT,0)/1000) > CTS.LCL_APPROVED_MIN_RATE THEN (CTS.LCL_APPROVED_RATE * nvl(BST.CHARGEABLE_WEIGHT,0)/1000)  ";
        //                    strSql += " END ) end) ) FROM CONT_MAIN_SEA_TBL CMST, CONT_TRN_SEA_FCL_LCL CTS, JOB_CARD_TRN JOB, BOOKING_MST_TBL BST,  ";
        //                    strSql += " BOOKING_TRN BTS, OPERATOR_MST_TBL OPE, VENDOR_MST_TBL VM, PARAMETERS_TBL PAR, COST_ELEMENT_MST_TBL CEM, dimention_unit_mst_tbl UOM  ";
        //                    strSql += " WHERE bts.basis = uom.dimention_unit_mst_pk and cts.lcl_basis = bts.basis and CMST.CONT_MAIN_SEA_PK = CTS.CONT_MAIN_SEA_FK AND ";
        //                    strSql += " JOB.BOOKING_MST_FK = BST.BOOKING_MST_PK AND BST.BOOKING_MST_PK = BTS.BOOKING_MST_FK AND CMST.OPERATOR_MST_FK = BST.CARRIER_MST_FK AND  ";
        //                    strSql += " CTS.PORT_MST_POL_FK = BST.PORT_MST_POL_FK AND CTS.PORT_MST_POD_FK = BST.PORT_MST_POD_FK AND CMST.OPERATOR_MST_FK = OPE.OPERATOR_MST_PK AND  ";
        //                    strSql += " PAR.COST_FRT_FK = CEM.COST_ELEMENT_MST_PK AND CMST.CARGO_TYPE = 2 AND CMST.CONT_APPROVED=1 AND CMST.ACTIVE=1 AND VM.VENDOR_ID = OPE.OPERATOR_ID AND  ";
        //                    strSql += " CMST.COMMODITY_GROUP_FK = JOB.COMMODITY_GROUP_FK AND SYSDATE >= CTS.VALID_FROM AND ";
        //                    strSql += " SYSDATE <= case when CTS.VALID_TO is not null then CTS.VALID_TO else sysdate end AND ";
        //                    strSql += " JOB.JOB_CARD_TRN_PK = " + JcPk + " group by VM.VENDOR_MST_PK";
        //                }
        //                //Export-Air
        //            }
        //            else
        //            {
        //                strSql = " SELECT VM.VENDOR_MST_PK, VM.VENDOR_ID, cem.cost_element_mst_pk, CEM.COST_ELEMENT_ID, (select AUTO_RATING_FRT_AIR(" + JcPk;
        //                strSql += " ) from dual)  FROM JOB_CARD_TRN JOB, BOOKING_MST_TBL BST, airline_mst_tbl OPE,";
        //                strSql += " VENDOR_MST_TBL VM, PARAMETERS_TBL PAR, COST_ELEMENT_MST_TBL CEM WHERE ";
        //                strSql += " JOB.BOOKING_MST_FK = BST.BOOKING_MST_PK AND BST.CARRIER_MST_FK = ope.airline_mst_pk ";
        //                strSql += " AND PAR.COST_FRT_FK = CEM.COST_ELEMENT_MST_PK AND VM.VENDOR_ID = OPE.AIRLINE_ID ";
        //                strSql += " AND JOB.JOB_CARD_TRN_PK = " + JcPk;
        //            }
        //            //Import
        //        }
        //        else if (Process == 2)
        //        {
        //            //Import-Sea
        //            if (Biz == 2)
        //            {
        //                //Fcl
        //                if (Cargo == 1)
        //                {
        //                    strSql = " SELECT VM.VENDOR_MST_PK, max(VM.VENDOR_ID), max(cem.cost_element_mst_pk), MAX(CEM.COST_ELEMENT_ID),";
        //                    strSql += " SUM((NVL(get_ex_rate(CTS.CURRENCY_MST_FK, (SELECT COR.CURRENCY_MST_FK FROM CORPORATE_MST_TBL COR), SYSDATE), 1) * CONT.FCL_APP_RATE) ) ESTIMATED_AMT";
        //                    //Snigdharani - 21/11/2008 - Removing v-array
        //                    //strSql &= " FROM CONT_MAIN_SEA_TBL CMST, CONT_TRN_SEA_FCL_LCL CTS, TABLE(CTS.CONTAINER_DTL_FCL) CONT, JOB_CARD_SEA_IMP_TBL JOB,"
        //                    strSql += " FROM CONT_MAIN_SEA_TBL CMST, CONT_TRN_SEA_FCL_LCL CTS, CONT_TRN_SEA_FCL_RATES CONT, JOB_CARD_TRN JOB,";
        //                    strSql += " JOB_TRN_CONT JCT, OPERATOR_MST_TBL OPE, VENDOR_MST_TBL VM, PARAMETERS_TBL PAR, COST_ELEMENT_MST_TBL CEM";
        //                    strSql += " WHERE CMST.CONT_MAIN_SEA_PK = CTS.CONT_MAIN_SEA_FK And JOB.JOB_CARD_TRN_PK = JCT.JOB_CARD_TRN_FK";
        //                    strSql += " AND CMST.OPERATOR_MST_FK = JOB.CARRIER_MST_FK AND CTS.PORT_MST_POL_FK = JOB.PORT_MST_POL_FK  AND CTS.PORT_MST_POD_FK = JOB.PORT_MST_POD_FK ";
        //                    strSql += " AND CMST.OPERATOR_MST_FK = OPE.OPERATOR_MST_PK AND PAR.COST_FRT_FK = CEM.COST_ELEMENT_MST_PK AND CMST.CARGO_TYPE = 1";
        //                    strSql += " AND CMST.CONT_APPROVED = 1 AND CMST.ACTIVE=1 AND VM.VENDOR_ID = OPE.OPERATOR_ID AND CMST.COMMODITY_GROUP_FK = JOB.COMMODITY_GROUP_FK";
        //                    strSql += " AND SYSDATE >= CTS.VALID_FROM AND SYSDATE <= case when CTS.VALID_TO is not null then CTS.VALID_TO else sysdate end ";
        //                    strSql += " AND CONT.CONTAINER_TYPE_MST_FK = JCT.CONTAINER_TYPE_MST_FK AND JOB.JOB_CARD_TRN_PK = " + JcPk;
        //                    strSql += " AND CONT.CONT_TRN_SEA_FK = CTS.CONT_TRN_SEA_PK group by VM.VENDOR_MST_PK";
        //                    //Snigdharani
        //                    //Lcl
        //                }
        //                else if (Cargo == 2 | Cargo == 4)
        //                {
        //                    strSql = " SELECT VM.VENDOR_MST_PK, max(VM.VENDOR_ID), max(cem.cost_element_mst_pk), MAX(CEM.COST_ELEMENT_ID),";
        //                    strSql += " SUM(NVL(get_ex_rate(CTS.CURRENCY_MST_FK, (SELECT COR.CURRENCY_MST_FK FROM CORPORATE_MST_TBL COR), SYSDATE), 1) * ";
        //                    strSql += " (CASE WHEN (CTS.LCL_APPROVED_RATE * JCT.VOLUME_IN_CBM) < NVL(CTS.LCL_APPROVED_MIN_RATE,1) THEN CTS.LCL_APPROVED_MIN_RATE";
        //                    strSql += " WHEN (CTS.LCL_APPROVED_RATE * JCT.VOLUME_IN_CBM) > NVL(CTS.LCL_APPROVED_MIN_RATE,1) THEN ";
        //                    strSql += " (CTS.LCL_APPROVED_RATE * JCT.VOLUME_IN_CBM) END) ) FROM CONT_MAIN_SEA_TBL CMST, CONT_TRN_SEA_FCL_LCL CTS,";
        //                    strSql += " JOB_CARD_TRN JOB, JOB_TRN_CONT JCT, OPERATOR_MST_TBL OPE, VENDOR_MST_TBL VM, PARAMETERS_TBL PAR,";
        //                    strSql += " COST_ELEMENT_MST_TBL CEM WHERE CMST.CONT_MAIN_SEA_PK = CTS.CONT_MAIN_SEA_FK";
        //                    strSql += " and  cts.lcl_basis = (SELECT D.DIMENTION_UNIT_MST_PK FROM DIMENTION_UNIT_MST_TBL D WHERE D.DIMENTION_ID LIKE '%CBM')";
        //                    strSql += " AND JOB.JOB_CARD_TRN_PK = JCT.JOB_CARD_TRN_FK AND CMST.OPERATOR_MST_FK = JOB.CARRIER_MST_FK";
        //                    strSql += " AND CTS.PORT_MST_POL_FK = JOB.PORT_MST_POL_FK AND CTS.PORT_MST_POD_FK = JOB.PORT_MST_POD_FK";
        //                    strSql += " AND CMST.OPERATOR_MST_FK = OPE.OPERATOR_MST_PK AND PAR.COST_FRT_FK = CEM.COST_ELEMENT_MST_PK";
        //                    strSql += " AND CMST.CARGO_TYPE = 2 AND CMST.CONT_APPROVED = 1 AND CMST.ACTIVE=1";
        //                    strSql += " AND VM.VENDOR_ID = OPE.OPERATOR_ID AND CMST.COMMODITY_GROUP_FK = JOB.COMMODITY_GROUP_FK AND SYSDATE >= CTS.VALID_FROM";
        //                    strSql += " AND SYSDATE <= case when CTS.VALID_TO is not null then CTS.VALID_TO else sysdate end AND JOB.JOB_CARD_TRN_PK = " + JcPk;
        //                    strSql += " group by VM.VENDOR_MST_PK";
        //                }
        //                //Import-Air
        //            }
        //            else
        //            {

        //            }
        //        }
        //        try
        //        {
        //            return objWF.GetDataSet(strSql);
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }

        //    }
        //    #endregion
        //    //'Added By Koteshwari on 7/5/2011
        //    #region "Auto Buying Cost in Estimated Cost for JC"
        //    //Manoharan 14052007: to insert a row in JC Purchase Invoice Entry Grid for Freight cost
        //    public object CreateNewRow_COST(string JcPk, bool isFCL, string CurrPk, byte Process, byte Biz)
        //    {

        //        WorkFlow objWF = new WorkFlow();
        //        System.Data.OracleClient.OracleCommand objCommand = new System.Data.OracleClient.OracleCommand();
        //        OracleTransaction objTran = null;
        //        System.Data.OracleClient.OracleCommand insCostDetails = new System.Data.OracleClient.OracleCommand();
        //        DataSet dsCost = null;
        //        string str = null;
        //        string strIns = null;
        //        long Ret = 0;
        //        int locpk = 0;
        //        Int16 intDel = default(Int16);
        //        Int16 intIns = default(Int16);

        //        try
        //        {
        //            objWF.OpenConnection();
        //            objTran = objWF.MyConnection.BeginTransaction();
        //            locpk = Session["LOGED_IN_LOC_FK"];
        //            str = "  delete from JOB_TRN_COST p where p.JOB_CARD_TRN_FK = " + JcPk;
        //            str += "  and p.COST_ELEMENT_MST_FK = ( select c.cost_element_mst_pk from cost_element_mst_tbl c, parameters_tbl p1 where c.cost_element_mst_pk = p1.cost_frt_fk)";

        //            //Sea
        //            if (Biz == 2)
        //            {
        //                //FCL - sea export
        //                if (isFCL)
        //                {
        //                    dsCost = getBuyingCost(JcPk, 1, 1, 2);
        //                    //LCL
        //                }
        //                else
        //                {
        //                    dsCost = getBuyingCost(JcPk, 2, 1, 2);
        //                }
        //            }
        //            else
        //            {
        //                dsCost = getBuyingCost(JcPk, 0, 1, 1);
        //            }
        //            var _with20 = objCommand;
        //            _with20.Connection = objWF.MyConnection;
        //            _with20.Transaction = objTran;
        //            _with20.CommandType = CommandType.Text;
        //            _with20.CommandText = str;
        //            intDel = _with20.ExecuteNonQuery;

        //            if ((dsCost != null))
        //            {
        //                if (dsCost.Tables[0].Rows.Count == 1)
        //                {
        //                    var _with21 = insCostDetails;
        //                    _with21.Connection = objWF.MyConnection;
        //                    _with21.CommandType = CommandType.StoredProcedure;
        //                    _with21.Transaction = objTran;
        //                    _with21.CommandText = objWF.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_COST_INS";
        //                    var _with22 = _with21.Parameters;
        //                    insCostDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", JcPk).Direction = ParameterDirection.Input;
        //                    insCostDetails.Parameters.Add("VENDOR_MST_FK_IN", dsCost.Tables[0].Rows[0][0]).Direction = ParameterDirection.Input;
        //                    insCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", dsCost.Tables[0].Rows[0][2]).Direction = ParameterDirection.Input;
        //                    insCostDetails.Parameters.Add("LOCATION_FK_IN", locpk).Direction = ParameterDirection.Input;
        //                    insCostDetails.Parameters.Add("VENDOR_KEY_IN", dsCost.Tables[0].Rows[0][1]).Direction = ParameterDirection.Input;
        //                    insCostDetails.Parameters.Add("PTMT_TYPE_IN", 1).Direction = ParameterDirection.Input;
        //                    insCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", CurrPk).Direction = ParameterDirection.Input;
        //                    insCostDetails.Parameters.Add("ESTIMATED_COST_IN", dsCost.Tables[0].Rows[0][4]).Direction = ParameterDirection.Input;
        //                    insCostDetails.Parameters.Add("TOTAL_COST_IN", dsCost.Tables[0].Rows[0][4]).Direction = ParameterDirection.Input;
        //                    insCostDetails.Parameters.Add("RETURN_VALUE", Ret).Direction = ParameterDirection.Output;
        //                    intIns = _with21.ExecuteNonQuery();
        //                }
        //            }

        //            if (intDel > 0 | intIns > 0)
        //            {
        //                objTran.Commit();
        //            }
        //            else
        //            {
        //                objTran.Rollback();
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            objTran.Rollback();
        //            throw ex;
        //        }
        //        finally
        //        {
        //            objWF.MyConnection.Close();
        //        }
        //    }
        //    #endregion
        //    //'End

        //    #region "BarCode"
        //    //Gopinath 15052007: to fetch records from barcode values
        //    public string FetchBarcodeValues(string BarCodeValue, string ConfigurationID)
        //    {
        //        string strSQL = null;
        //        Array BarCode = null;
        //        BarCode = BarCodeValue.Split("$");
        //        try
        //        {
        //            WorkFlow objWK = new WorkFlow();
        //            DataSet dsQuery = null;
        //            DataSet dsResult = null;
        //            string strValues = null;
        //            strSQL = strSQL + Constants.vbCrLf + " select bar1.check_query,bar1.fetch_query from  ";
        //            strSQL = strSQL + Constants.vbCrLf + " barcode_data_mst_tbl bar,barcode_data_mst_tbl bar1 ";
        //            strSQL = strSQL + Constants.vbCrLf + " where bar.config_id_fk = '" + ConfigurationID + "'";
        //            strSQL = strSQL + Constants.vbCrLf + " and bar.bcd_mst_pk = bar1.BCD_MST_FK(+) ";
        //            strSQL = strSQL + Constants.vbCrLf + " and bar1.default_value = 1 ";
        //            dsQuery = objWK.GetDataSet(strSQL);
        //            if (dsQuery.Tables[0].Rows.Count > 0)
        //            {
        //                string objst = dsQuery.Tables[0].Rows[0]["check_query"];
        //                objst += " '" + BarCode(0) + "'";
        //                if ((Convert.ToInt32(objWK.ExecuteScaler(objst)) > 0))
        //                {
        //                    objst = "";
        //                    objst = dsQuery.Tables[0].Rows[0]["fetch_query"];
        //                    objst += " '" + BarCode(0) + "'";
        //                    dsResult = objWK.GetDataSet(objst);
        //                    strValues += dsResult.Tables[0].Rows[0]["Pk"] + "~" + BarCode(0);
        //                }
        //                else
        //                {
        //                    strValues += 0 + "~" + BarCode(0);
        //                }
        //            }
        //            else
        //            {
        //                strValues += 0 + "~" + BarCode(0);
        //            }
        //            return strValues;
        //        }
        //        catch (Exception EX)
        //        {
        //            throw EX;
        //        }
        //    }
        //    public string FetchBarcodeValues(string BarCodeValue, short Biz, short Proc, string field_name)
        //    {
        //        string strSQL = null;
        //        Array BarCode = null;
        //        BarCode = BarCodeValue.Split("$");
        //        try
        //        {
        //            WorkFlow objWK = new WorkFlow();
        //            DataSet dsQuery = null;
        //            DataSet dsResult = null;
        //            string strValues = null;
        //            strSQL = strSQL + Constants.vbCrLf + " Select c.fetch_query,c.check_query from barcode_data_mst_tbl a,  ";
        //            strSQL = strSQL + Constants.vbCrLf + " barcode_data_mst_tbl c where c.BCD_MST_FK = a.bcd_mst_pk ";
        //            strSQL = strSQL + Constants.vbCrLf + " and c.default_value = 1 ";
        //            strSQL = strSQL + Constants.vbCrLf + " and a.field_name = '" + field_name + "'";
        //            //Invoice' " 
        //            strSQL = strSQL + Constants.vbCrLf + " and a.BCD_MST_FK = (select b.bcd_mst_pk from barcode_data_mst_tbl b ";

        //            //Sea & Export
        //            if (Biz == 2 & Proc == 1)
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " where b.field_name='Export Documentation' ";
        //                strSQL = strSQL + Constants.vbCrLf + " and b.BCD_MST_FK=2 ) ";
        //                //Air Export
        //            }
        //            else if (Biz == 1 & Proc == 1)
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " where b.field_name='Export Documentation' ";
        //                strSQL = strSQL + Constants.vbCrLf + " and b.BCD_MST_FK=1 ) ";
        //                //Sea Import
        //            }
        //            else if (Biz == 2 & Proc == 2)
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " where b.field_name='Import Documentation' ";
        //                strSQL = strSQL + Constants.vbCrLf + " and b.BCD_MST_FK=2 ) ";
        //                //Air Import
        //            }
        //            else
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " where b.field_name='Import Documentation' ";
        //                strSQL = strSQL + Constants.vbCrLf + " and b.BCD_MST_FK=1 ) ";
        //            }

        //            dsQuery = objWK.GetDataSet(strSQL);
        //            if (dsQuery.Tables[0].Rows.Count > 0)
        //            {
        //                string objst = dsQuery.Tables[0].Rows[0]["check_query"];
        //                objst += " '" + BarCode(0) + "'";
        //                if ((Convert.ToInt32(objWK.ExecuteScaler(objst)) > 0))
        //                {
        //                    objst = "";
        //                    objst = dsQuery.Tables[0].Rows[0]["fetch_query"];
        //                    objst += " '" + BarCode(0) + "'";
        //                    dsResult = objWK.GetDataSet(objst);
        //                    strValues += dsResult.Tables[0].Rows[0]["Pk"] + "~" + BarCode(0);
        //                }
        //                else
        //                {
        //                    strValues += 0 + "~" + BarCode(0);
        //                }
        //            }
        //            else
        //            {
        //                strValues += 0 + "~" + BarCode(0);
        //            }

        //            return strValues;
        //        }
        //        catch (Exception EX)
        //        {
        //            throw EX;
        //        }
        //    }
        //    #endregion

        //    #region "Credit Limit Function"
        //    public string CreditLimitMgnr(int UsrPk = 0, int CustPk = 0, string TransType = "", int TransPK = 0, int TransValue = 0, short BizType = 3, int POLPK = 0, int PODPK = 0)
        //    {
        //        string strReturn = null;
        //        WorkFlow objWF = new WorkFlow();
        //        OracleCommand selectCommand = new OracleCommand();
        //        objWF.OpenConnection();
        //        selectCommand.Connection = objWF.MyConnection;
        //        selectCommand.CommandType = CommandType.StoredProcedure;
        //        selectCommand.CommandText = objWF.MyUserName + ".CUSTOMER_MST_TBL_PKG.CREDIT_CUSTOMER";

        //        try
        //        {
        //            var _with23 = selectCommand.Parameters;
        //            _with23.Add("USR_PK_IN", UsrPk).Direction = ParameterDirection.Input;
        //            _with23.Add("CUSTPK_IN", CustPk).Direction = ParameterDirection.Input;
        //            _with23.Add("TRANSTYPE_IN", TransType).Direction = ParameterDirection.Input;
        //            _with23.Add("TRANSPK_IN", TransPK).Direction = ParameterDirection.Input;
        //            _with23.Add("TRANSVALUE_IN", TransValue).Direction = ParameterDirection.Input;
        //            _with23.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
        //            _with23.Add("POLPK_IN", POLPK).Direction = ParameterDirection.Input;
        //            _with23.Add("PODPK_IN", PODPK).Direction = ParameterDirection.Input;
        //            _with23.Add("RETURN_VALUE", System.Data.OracleClient.OracleType.VarChar, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            selectCommand.ExecuteNonQuery();
        //            strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
        //            return strReturn;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //        finally
        //        {
        //            selectCommand.Connection.Close();
        //        }
        //    }
        //    #endregion

        //    #region "Online Help Function"
        //    public string fetchHelpLink(strConfigID)
        //    {
        //        string strQuery = " Select cfg.Help_Url from Config_Mst_Tbl cfg where cfg.Config_ID='" + strConfigID + "' ";
        //        string strHelpLink = (new WorkFlow()).ExecuteScaler(strQuery);
        //        if ((!string.IsNullOrEmpty(strHelpLink)))
        //        {
        //            return strHelpLink;
        //        }
        //        else
        //        {
        //            return ConfigurationManager.AppSettings["HelpIntroLink"];
        //        }
        //    }

        //    public string setHelpLink(strConfigID)
        //    {
        //        return "'Javascript:window.open(\"" + fetchHelpLink(strConfigID) + "\", \"QFORHELP\",\"menubar=no,scrollbars=no,status=no,toolbar=no,height=650,width=900,screenx=0,screeny=0,left=0,top=0\");'";
        //        //Return "'Javascript:showModalDialog(""" & fetchHelpLink(strConfigID) & """, ""Q-FOR HELP"",""dialogWidth:800px; dialogHeight:600px; status:no; center:yes; help:no"");'"
        //    }

        //    public string setHelpKey(strConfigID)
        //    {
        //        return "window.open(\"" + fetchHelpLink(strConfigID) + "\", \"helptext\",\"height=850,width=1020,menubar=no,scrollbars=no,status=no,toolbar=no,screenx=0,screeny=0,left=0,top=0\");";
        //    }
        //    #endregion

        //    #region "To Fetch Parameter Table Value"
        //    public string GetParameterValue(strColName)
        //    {
        //        try
        //        {
        //            string strSQL = null;
        //            strSQL = strSQL + Constants.vbCrLf + " SELECT '" + strColName + "'";
        //            strSQL = strSQL + Constants.vbCrLf + " FROM PARAMETERS_TBL ";
        //            return (new WorkFlow()).ExecuteScaler(strSQL);
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    #endregion
        //    #region " For Import Operation"
        //    public DataSet ImportFetch(TableName)
        //    {
        //        try
        //        {
        //            string SQL = null;
        //            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //            if (TableName == "COUNTRY_MST_TBL")
        //            {
        //                sb.Append("SELECT COUNTRY_MST_PK,");
        //                sb.Append("COUNTRY_ID,");
        //                sb.Append("COUNTRY_NAME,");
        //                sb.Append("CURRENCY_MST_FK,");
        //                sb.Append("ACTIVE_FLAG,");
        //                sb.Append("CREATED_BY_FK,");
        //                sb.Append("CREATED_DT,");
        //                sb.Append("LAST_MODIFIED_BY_FK,");
        //                sb.Append("LAST_MODIFIED_DT,");
        //                sb.Append("VERSION_NO,");
        //                sb.Append("EU_NEU European,AREA_MST_FK,");
        //                sb.Append("ENVIRONMENT_FK FROM COUNTRY_MST_TBL");
        //                return (new WorkFlow()).GetDataSet(sb.ToString());
        //            }
        //            else
        //            {
        //                SQL = "SELECT * FROM " + TableName;
        //                return (new WorkFlow()).GetDataSet(SQL);

        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    #endregion

        //    #region "Fetch Mail Id Of particular customer"                  ' Added By Prakash Chandra on 16/06/2008 for geting mailid of particular user.
        //    public string FetchMailTo(ref string custpk)
        //    {
        //        string strSQL = null;
        //        strSQL += Constants.vbCrLf + "select custdtl.adm_email_id from customer_contact_dtls custdtl ,";
        //        strSQL += Constants.vbCrLf + " customer_mst_tbl cust ";
        //        strSQL += Constants.vbCrLf + "where";
        //        strSQL += Constants.vbCrLf + "cust.customer_mst_pk = custdtl.customer_mst_fk";
        //        strSQL += Constants.vbCrLf + "and cust.customer_mst_pk='" + custpk + "'";
        //        try
        //        {
        //            return (new WorkFlow()).ExecuteScaler(strSQL);
        //        }
        //        catch (OracleClient.OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion
        //    //Snigdharani - 06/09/2008
        //    //Use this function when datechooser is not picking up the style even after inserting the control in the config_control_tbl table.
        //    //But before calling this function first insert all the controls in config_control table as per the proper methid.
        //    #region "Date Control StyleSheet"
        //    //Public Function DateControlStyle(ByVal dateControl1 As Infragistics.WebUI.WebSchedule.WebDateChooser, _
        //    //                            Optional ByVal dateControl2 As Infragistics.WebUI.WebSchedule.WebDateChooser = Nothing)
        //    //    dateControl1.CalendarLayout.FooterFormat = "Today: {0:d}"
        //    //    dateControl1.DropButton.Style.CssClass = "DateDropButtonClass"
        //    //    dateControl1.CssClass = "WebDateClass"
        //    //    dateControl1.CalendarLayout.SelectedDayStyle.CssClass = "CalenderSelectedDay"
        //    //    dateControl1.CalendarLayout.OtherMonthDayStyle.CssClass = "CelenderOtherMonthDay"
        //    //    dateControl1.CalendarLayout.NextPrevStyle.CssClass = "CelenderNextPrev"
        //    //    dateControl1.CalendarLayout.CalendarStyle.CssClass = "CelenderStyle"
        //    //    dateControl1.CalendarLayout.TodayDayStyle.CssClass = "TodayDayStyle"
        //    //    dateControl1.CalendarLayout.DayHeaderStyle.CssClass = "DayHeaderStyle"
        //    //    dateControl1.CalendarLayout.TitleStyle.CssClass = "TitleStyle"
        //    //    dateControl1.CalendarLayout.DropDownStyle.BorderStyle = BorderStyle.Solid
        //    //End Function
        //    public void DateControlStyle(Page MyPage)
        //    {
        //        object objCTL = null;
        //        Int16 intI = default(Int16);
        //        foreach (object objCTL_loopVariable in MyPage.Controls)
        //        {
        //            objCTL = objCTL_loopVariable;
        //            if (objCTL is System.Web.UI.HtmlControls.HtmlForm)
        //            {
        //                objCTL = MyPage.Controls[intI];
        //                object objCtls = null;
        //                foreach (object objCtls_loopVariable in MyPage.Controls[intI].Controls)
        //                {
        //                    objCtls = objCtls_loopVariable;
        //                    if (objCtls is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //                    {
        //                        //CType(objCtls, System.Web.UI.WebControls.DropDownList).Enabled = False
        //                        objCtls.CalendarLayout.FooterFormat = "Today: {0:d}";
        //                        objCtls.DropButton.Style.CssClass = "DateDropButtonClass";
        //                        objCtls.CssClass = "WebDateClass";
        //                        objCtls.CalendarLayout.SelectedDayStyle.CssClass = "CalenderSelectedDay";
        //                        objCtls.CalendarLayout.OtherMonthDayStyle.CssClass = "CelenderOtherMonthDay";
        //                        objCtls.CalendarLayout.NextPrevStyle.CssClass = "CelenderNextPrev";
        //                        objCtls.CalendarLayout.CalendarStyle.CssClass = "CelenderStyle";
        //                        objCtls.CalendarLayout.TodayDayStyle.CssClass = "TodayDayStyle";
        //                        objCtls.CalendarLayout.DayHeaderStyle.CssClass = "DayHeaderStyle";
        //                        objCtls.CalendarLayout.TitleStyle.CssClass = "TitleStyle";
        //                        objCtls.CalendarLayout.DropDownStyle.BorderStyle = BorderStyle.Solid;
        //                    }
        //                }
        //            }
        //            intI += 1;
        //        }
        //    }
        //    #endregion
        //    #region "GeneratePDF Format"
        //    //adding by thiyagarajan on 10/11/08 to display  PDF through JOBCARD Entry Screen :PTS Task
        //    public void GeneratePDF(Int32 Jobpk, string customer, Int32 biztype, Int32 process, string Refno, ref CrystalDecisions.CrystalReports.Engine.ReportDocument rptDoc)
        //    {
        //        clsQuotationReport objQuotReport = new clsQuotationReport();
        //        DataSet DsCrInvMain = null;
        //        DataSet AddressDS = new DataSet();
        //        string[] PK = null;
        //        Int32 chk = default(Int32);
        //        Int32 j = default(Int32);
        //        Int32 i = default(Int32);
        //        string filename = null;
        //        clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
        //        DataSet dsamt = new DataSet();
        //        string barcode = "";
        //        DataSet dsdtvat = new DataSet();
        //        QFORBusinessDev.CommonFeatures objrep = new QFORBusinessDev.CommonFeatures();
        //        bool flag = false;
        //        int IsFacInv = 0;
        //        string MBLPK = null;
        //        clsBlClauseForHblMbl objClsBlClause = new clsBlClauseForHblMbl();
        //        DataSet DsFACShipmentDetail = new DataSet();
        //        DataSet DsCrInvDetailsMain = new DataSet();
        //        DataSet DsCrInvDetailsSub = new DataSet();
        //        DataSet DSBankDetails = new DataSet();
        //        DataSet ContactDS = new DataSet();
        //        DataSet DsCurr = new DataSet();
        //        DataSet GridDS = new DataSet();
        //        try
        //        {
        //            string ShowBarCode = ConfigurationManager.AppSettings["ShowBarcode"];
        //            if (ShowBarCode == "1" & objConsInv.GetBarcodeFlag("CONSOLIDATED INVOICE") == "1")
        //            {
        //                barcode = "*" + FetchBarcodes(Refno, customer) + "*";
        //            }
        //            IsFacInv = FacInv(Refno);

        //            if (IsFacInv == 1)
        //            {
        //                MBLPK = getMblPK(Jobpk, biztype);
        //                rptDoc.Load(Server.MapPath("..\\Recievables") + "\\RptFACInvDetail.rpt");
        //                rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

        //                DsCrInvDetailsMain = objConsInv.FAC_INV_DETAIL_MAIN_PRINT(objConsInv.GetInvPk(Refno), biztype, process, Session["User_ID"]);
        //                DsCrInvDetailsMain.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\Inv_Details.xsd");

        //                DsCrInvDetailsSub = objConsInv.CONSOL_INV_DETAIL_SUB_MAIN_FAC_PRINT(objConsInv.GetInvPk(Refno), biztype, process);
        //                DsCrInvDetailsSub.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\Inv_Sub_Details.xsd");

        //                DsFACShipmentDetail = objConsInv.FAC_INV_SHIPMENT_DETAIL_MAIN_PRINT(MBLPK, Convert.ToInt32(Session["CURRENCY_MST_PK"]), Convert.ToInt32(Session["CURRENCY_MST_PK"]), Session["LOGED_IN_LOC_FK"], biztype);
        //                DsFACShipmentDetail.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\rptFAC_Inv_Shipment_Detail.xsd");

        //                DsCrInvMain = objConsInv.CONSOL_FAC_SUPP_RPT_PRINT(objConsInv.GetInvPk(Refno), biztype, process);
        //                DsCrInvMain.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\Inv_Custumer.xsd");

        //                AddressDS = objConsInv.FetchLocation(Session["LOGED_IN_LOC_FK"]);
        //                AddressDS.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\Inv_Location.xsd");
        //                rptDoc.OpenSubreport("Cr_Location").SetDataSource(AddressDS);

        //                DSBankDetails = objConsInv.BankDetails(Convert.ToInt64(Session["LOGED_IN_LOC_FK"]));
        //                DSBankDetails.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\BankDetails.xsd");

        //                ContactDS = objConsInv.FetchLocation(Session["LOGED_IN_LOC_FK"]);
        //                ContactDS.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\ContactAddress.xsd");

        //                DsCurr = objConsInv.CONSOL_INV_DETAIL_CURR_MAIN_PRINT(objConsInv.GetInvPk(Refno), biztype, process);
        //                DsCurr.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\Inv_Curr_Details.xsd");

        //                GridDS = objClsBlClause.FetchBlClausesForHBL("", 8, 1, 1, "0", objConsInv.GetInvPk(Refno));
        //                GridDS.ReadXmlSchema(Server.MapPath("..\\Documentation") + "\\PrintBLClause.XSD");

        //                rptDoc.SetDataSource(DsCrInvMain);
        //                dsamt = objConsInv.FetchAmount(Refno);

        //                //EXPORT SEA 
        //                if (biztype == 2 & process == 1)
        //                {
        //                    objrep.getReportControls(ref rptDoc, "QFOR4078", 5);
        //                    //for sea import 
        //                }
        //                else if (biztype == 2 & process == 2)
        //                {
        //                    objrep.getReportControls(ref rptDoc, "QFOR4078", 5);
        //                    //for Air Export
        //                }
        //                else if (biztype == 1 & process == 1)
        //                {
        //                    objrep.getReportControls(ref rptDoc, "QFOR4078", 5);
        //                    //For Air Import 
        //                }
        //                else if (biztype == 1 & process == 2)
        //                {
        //                    objrep.getReportControls(ref rptDoc, "QFOR4078", 5);
        //                }
        //                rptDoc.SetParameterValue(3, Refno);
        //                rptDoc.SetParameterValue(4, dsamt.Tables[0].Rows[0]["currid"]);
        //                rptDoc.SetParameterValue(5, getDefault(dsamt.Tables[1].Rows[0]["taxamt"], 0));
        //                rptDoc.SetParameterValue(6, getDefault(dsamt.Tables[0].Rows[0]["disamt"], 0));
        //                rptDoc.SetParameterValue(7, "");
        //                rptDoc.SetParameterValue(8, dsamt.Tables[1].Rows[0]["invamt"]);
        //                rptDoc.SetParameterValue(9, Convert.ToString(DateAndTime.Now.Date));
        //                rptDoc.SetParameterValue(10, barcode);
        //                rptDoc.SetParameterValue(28, biztype);
        //                rptDoc.SetParameterValue(29, process);
        //                rptDoc.SetParameterValue(64, Convert.ToString(DateAndTime.Now.Date));
        //                rptDoc.SetParameterValue("UniqueRefNr", getDefault(dsamt.Tables[1].Rows[0]["inv_unique_ref_nr"], ""));
        //                if (DsCrInvDetailsMain.Tables[0].Rows.Count > 0)
        //                {
        //                    rptDoc.SetParameterValue("Vessel", (Information.IsDBNull(DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"]) ? "TBA" : DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"]));
        //                }
        //                else
        //                {
        //                    rptDoc.SetParameterValue("Vessel", "TBA");
        //                }
        //                rptDoc.SetParameterValue("InvAmt", dsamt.Tables[1].Rows[0]["invamt"]);

        //                rptDoc.SetParameterValue("Remarks", "");

        //                rptDoc.SetDataSource(DsCrInvDetailsMain);
        //                rptDoc.OpenSubreport("Cr_Location").SetDataSource(AddressDS);
        //                rptDoc.OpenSubreport("RptCustDetails").SetDataSource(DsCrInvMain);
        //                rptDoc.OpenSubreport("RptSubDetails").SetDataSource(DsCrInvDetailsSub);
        //                rptDoc.OpenSubreport("rptFACShipmentDetails").SetDataSource(DsFACShipmentDetail);
        //                rptDoc.OpenSubreport("RptCurrency").SetDataSource(DsCurr);
        //                rptDoc.OpenSubreport("RptBankDetails").SetDataSource(DSBankDetails);
        //                rptDoc.OpenSubreport("RptContactAddress").SetDataSource(ContactDS);
        //                rptDoc.OpenSubreport("rptBLClause").SetDataSource(GridDS);
        //                filename = "FACInvReport";
        //                string payduedt = null;
        //                dsdtvat = objConsInv.GetDtVat(objConsInv.GetInvPk(Refno));
        //                payduedt = objConsInv.GetInvCrday(objConsInv.GetInvPk(Refno), dsdtvat.Tables[0].Rows[0][0], biztype, process);
        //                rptDoc.SetParameterValue("Paydue", (!string.IsNullOrEmpty(getDefault(payduedt, "")) ? payduedt : "Immediate"));

        //                objrep.getReportControls(ref rptDoc, "QFOR4078", 1);
        //                //PrintInAcrobate(rptDoc, filename, StrSetPrint)
        //                ///'

        //            }
        //            else
        //            {
        //                rptDoc.Load(Server.MapPath("..\\Recievables") + "\\RptConsolInvSummary.rpt");
        //                rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

        //                DsCrInvMain = objConsInv.CONSOL_INV_CUST_PRINT(objConsInv.GetInvPk(Refno), biztype, process);

        //                DsCrInvMain.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\Inv_Custumer.xsd");

        //                AddressDS = objConsInv.FetchLocation(Session["LOGED_IN_LOC_FK"]);
        //                AddressDS.ReadXmlSchema(Server.MapPath("..\\Recievables") + "\\Inv_Location.xsd");
        //                rptDoc.OpenSubreport("Cr_Location").SetDataSource(AddressDS);

        //                rptDoc.SetDataSource(DsCrInvMain);
        //                dsamt = objConsInv.FetchAmount(Refno);

        //                rptDoc.SetParameterValue(0, Refno);
        //                rptDoc.SetParameterValue(1, dsamt.Tables[1].Rows[0]["invamt"]);
        //                rptDoc.SetParameterValue(2, getDefault(dsamt.Tables[1].Rows[0]["taxamt"], 0));
        //                rptDoc.SetParameterValue(3, getDefault(dsamt.Tables[0].Rows[0]["disamt"], 0));
        //                rptDoc.SetParameterValue(4, getDefault(dsamt.Tables[0].Rows[0]["netamt"], 0));
        //                rptDoc.SetParameterValue(5, dsamt.Tables[0].Rows[0]["currid"]);
        //                rptDoc.SetParameterValue(6, Convert.ToString(DateAndTime.Now.Date));
        //                rptDoc.SetParameterValue(7, barcode);

        //                rptDoc.SetParameterValue("ContainerUnit", objConsInv.FetchContainer(Refno));
        //                rptDoc.SetParameterValue("Dimensions", objConsInv.FetchDimensions(Refno));
        //                //rptDoc.SetParameterValue("Dimensions", Cstr(10x8x6))
        //                if (biztype == 2)
        //                {
        //                    flag = true;
        //                }
        //                else
        //                {
        //                    flag = false;
        //                }
        //                rptDoc.SetParameterValue("flag", flag);

        //                rptDoc.SetParameterValue("UniqueRefNr", getDefault(dsamt.Tables[1].Rows[0]["inv_unique_ref_nr"], ""));
        //                //Snigdharani - 08/01/2009 - Unique Invoice Reference Number(VEK Client)
        //                //adding by thiyagarajan on 4/12/08 for introducing "Payment Due" facility in report
        //                dsdtvat = objConsInv.GetDtVat(objConsInv.GetInvPk(Refno));
        //                string payduedt = null;
        //                payduedt = objConsInv.GetInvCrday(objConsInv.GetInvPk(Refno), dsdtvat.Tables[0].Rows[0][0], biztype, process);
        //                rptDoc.SetParameterValue("Paydue", (!string.IsNullOrEmpty(getDefault(payduedt, "")) ? payduedt : "Immediate"));
        //                rptDoc.SetParameterValue("Vatno", getDefault(dsdtvat.Tables[0].Rows[0][1], ""));
        //                //end
        //                DataSet VATds = new DataSet();
        //                VATds = objConsInv.Fetch_VAT(objConsInv.GetInvPk(Refno));
        //                rptDoc.OpenSubreport("subRptVATCode").SetDataSource(VATds);
        //                ReportParameters(Jobpk, biztype, process, ref rptDoc);
        //                objrep.getReportControls(rptDoc, "QFOR4078");
        //            }
        //            //adding by thiagarajan
        //            // To apply multi lingual  settings
        //            //Dim param As String
        //            //param &= "'ANCILLARIES','Reference','Charges To','Local Port/Dock/Terminal','Container/Unit #','Delivery Point','ICollection Point',"
        //            //param &= "'Airport/Port-Loading','Airport/Port-Discharge','Final Destination','Bill of Lading/AWB #','Flight/Vessel','Terms of Shipment',"
        //            //param &= "'Freight Payable at','FOB Value','Insur','Marks and Numbers','Good','HM','Cube m3','Kg Gross','Kg Net',"
        //            //param &= "'Description','Charges','VAT','Code','Tax Code','Freight & Ancillaries','Vat amt','Less','Total Amount Due In'"

        //            //Dim ds As New DataSet
        //            //Dim check As Boolean

        //            //ds = objQuotReport.FetchCaption(param, Session("LOGED_IN_LOC_FK"))
        //            //param = param.Replace("'", "")

        //            //PK = param.Split(",")

        //            //For i = 0 To PK.Length - 1
        //            //    chk = 0
        //            //    check = False
        //            //    For j = 0 To ds.Tables(0).Rows.Count - 1
        //            //        If (ds.Tables(0).Rows(j).Item("mainLang") = PK(i)) Then
        //            //            chk = 1
        //            //            If Not IsDBNull(ds.Tables(0).Rows(j).Item("subLang")) Then
        //            //                rptDoc.SetParameterValue(PK(i), "/" & CStr(ds.Tables(0).Rows(j).Item("subLang")).ToUpper)
        //            //            Else
        //            //                rptDoc.SetParameterValue(PK(i), "")
        //            //            End If
        //            //        End If
        //            //    Next
        //            //    If chk = 0 Then
        //            //        rptDoc.SetParameterValue(PK(i), "")
        //            //    End If
        //            //Next

        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    public void ReportParameters(Int32 Jobpk, Int32 biztype, Int32 process, ref CrystalDecisions.CrystalReports.Engine.ReportDocument repdoc)
        //    {
        //        DataSet dsparameter = new DataSet();
        //        Int32 i = default(Int32);
        //        clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
        //        try
        //        {
        //            if (biztype == 2)
        //            {
        //                dsparameter = objConsInv.FetchJobCardSeaDetails(Jobpk, process);
        //            }
        //            else
        //            {
        //                dsparameter = objConsInv.FetchJobCardAirDetails(Jobpk, process);
        //            }

        //            if (dsparameter.Tables[0].Rows.Count > 0)
        //            {
        //                repdoc.SetParameterValue(8, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGNAME"], ""));
        //                repdoc.SetParameterValue(9, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGADD1"], ""));
        //                repdoc.SetParameterValue(10, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGADD2"], ""));
        //                repdoc.SetParameterValue(11, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGADD3"], ""));
        //                repdoc.SetParameterValue(12, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGZIP"], ""));
        //                repdoc.SetParameterValue(13, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGCITY"], ""));
        //                repdoc.SetParameterValue(14, getDefault(dsparameter.Tables[0].Rows[0]["CONSCOUNTRY"], ""));
        //                repdoc.SetParameterValue(15, getDefault(dsparameter.Tables[0].Rows[0]["POLNAME"], ""));
        //                repdoc.SetParameterValue(16, getDefault(dsparameter.Tables[0].Rows[0]["PODNAME"], ""));
        //                repdoc.SetParameterValue(17, getDefault(dsparameter.Tables[0].Rows[0]["VES_VOY"], ""));
        //                repdoc.SetParameterValue(18, getDefault(dsparameter.Tables[0].Rows[0]["DEL_PLACE_NAME"], ""));
        //                repdoc.SetParameterValue(19, getDefault(dsparameter.Tables[0].Rows[0]["BLREFNO"], ""));

        //                repdoc.SetParameterValue(20, getDefault(dsparameter.Tables[0].Rows[0]["TERMS"], ""));
        //                repdoc.SetParameterValue(21, getDefault(dsparameter.Tables[0].Rows[0]["PYMT_TYPE"], ""));
        //                repdoc.SetParameterValue(22, getDefault(dsparameter.Tables[0].Rows[0]["INSURANCE"], ""));
        //                repdoc.SetParameterValue(23, getDefault(dsparameter.Tables[0].Rows[0]["MARKS"], ""));

        //                repdoc.SetParameterValue(24, getDefault(dsparameter.Tables[0].Rows[0]["GOODS_DESCRIPTION"], ""));

        //                repdoc.SetParameterValue(25, Convert.ToDecimal(getDefault(dsparameter.Tables[0].Rows[0]["VOLUME"], 0.0)));
        //                repdoc.SetParameterValue(26, Convert.ToDecimal(getDefault(dsparameter.Tables[0].Rows[0]["GROSSWEIGHT"], 0.0)));
        //                repdoc.SetParameterValue(27, Convert.ToDecimal(getDefault(dsparameter.Tables[0].Rows[0]["CHARWT"], 0.0)));
        //                repdoc.SetParameterValue(28, getDefault("SGD", ""));
        //            }
        //            else
        //            {
        //                for (i = 8; i <= 28; i += 1)
        //                {
        //                    repdoc.SetParameterValue(i, "");
        //                }
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    public int FacInv(string InvNr)
        //    {
        //        try
        //        {
        //            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //            WorkFlow objWF = new WorkFlow();
        //            sb.Append(" SELECT C.IS_FAC_INV");
        //            sb.Append("  FROM CONSOL_INVOICE_TBL C");
        //            sb.Append(" WHERE  C.INVOICE_REF_NO = '" + InvNr + "'");
        //            return objWF.ExecuteScaler(sb.ToString());
        //        }
        //        catch (OracleClient.OracleException Oraexp)
        //        {
        //            throw Oraexp;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    public string getMblPK(int jobpk, int BizType)
        //    {
        //        try
        //        {
        //            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //            WorkFlow objWF = new WorkFlow();
        //            sb.Append(" SELECT J.MBL_MAWB_FK MBL_EXP_TBL_FK ");
        //            sb.Append("  FROM JOB_CARD_TRN J");
        //            sb.Append(" WHERE  J.JOB_CARD_TRN_PK =" + jobpk);
        //            return objWF.ExecuteScaler(sb.ToString());
        //        }
        //        catch (OracleClient.OracleException Oraexp)
        //        {
        //            throw Oraexp;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    public string FetchBarcodes(string refno, string customer)
        //    {
        //        int strReturnPk = 0;
        //        DataSet DsReturn = null;
        //        string strBarcode = null;
        //        Int16 i = default(Int16);
        //        clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
        //        try
        //        {
        //            strReturnPk = objConsInv.FetchBarCodeManagerPk(2, 1);
        //            DsReturn = objConsInv.FetchBarCodeField(strReturnPk);
        //            strBarcode = "";


        //            if (DsReturn.Tables[0].Rows.Count > 0)
        //            {
        //                for (i = 0; i <= DsReturn.Tables[0].Rows.Count - 1; i++)
        //                {
        //                    var _with24 = DsReturn.Tables[0].Rows[i];
        //                    if (removeDBNull(_with24.Item("field_name")) == "Consolidate Invoice Nr")
        //                    {
        //                        if (!string.IsNullOrEmpty(strBarcode))
        //                        {
        //                            strBarcode = strBarcode + "~" + refno;
        //                        }
        //                        else
        //                        {
        //                            strBarcode = strBarcode + refno;
        //                        }

        //                    }
        //                    else if (removeDBNull(_with24.Item("field_name")) == "Jobcard Nr")
        //                    {
        //                        if (!string.IsNullOrEmpty(strBarcode))
        //                        {
        //                            strBarcode = strBarcode + "~" + customer;
        //                        }
        //                        else
        //                        {
        //                            strBarcode = strBarcode + customer;
        //                        }
        //                    }
        //                }
        //            }
        //            return strBarcode;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    //Added by Ashish

        //    public void GeneratePDFCRConsol(Int32 biztype, Int32 process, string Refno, Int32 CRPK, ref CrystalDecisions.CrystalReports.Engine.ReportDocument rptDoc)
        //    {
        //        WorkFlow objWF = new WorkFlow();
        //        ReportDocument rptDetails = new ReportDocument();
        //        DataSet DsCrMain = null;
        //        DataSet DsCrSub = null;
        //        DataSet DsCurr = new DataSet();
        //        DataSet AddressDS = new DataSet();
        //        DataSet DsQuoteDetails = new DataSet();
        //        int StorageVal = 0;
        //        int StoreorPreviewFlag = 0;
        //        string UniqueRefNo = null;
        //        string strCrType = null;
        //        clsCreditNote objCreditNote = new clsCreditNote();
        //        QFORBusinessDev.CommonFeatures objrep = new QFORBusinessDev.CommonFeatures();
        //        clsTrackNTrace objTrTc = new clsTrackNTrace();

        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        sb.Append("SELECT DISTINCT CRN.CRN_TBL_PK CRN_PK,");
        //        sb.Append("       CRN.CREDIT_NOTE_REF_NR CRN_REF,");
        //        sb.Append("       CRN.CREDIT_NOTE_DATE CRN_DATE,");
        //        sb.Append("       NVL(CRN.CUSTOMER_MST_FK, 0) CUSTOMER_MST_FK,");
        //        sb.Append("       NVL(CRN.CURRENCY_MST_FK, 0) CURRENCY_MST_FK,");
        //        sb.Append("       NVL(CRN.DOCUMENT_TYPE, 0) DOC_TYPE,");
        //        sb.Append("       NVL(CRN.DOCUMENT_REFRENCE, 0) DOC_REF,");
        //        sb.Append("       CRN.CRN_AMMOUNT,");
        //        sb.Append("       JOBEXP.JOBCARD_REF_NO,");
        //        sb.Append("       CMT.CURRENCY_ID,CON.INVOICE_AMT");
        //        sb.Append("  FROM CREDIT_NOTE_TBL        CRN,");
        //        sb.Append("       CREDIT_NOTE_TRN_TBL    CRNT,");
        //        sb.Append("       CONSOL_INVOICE_TBL     CON,");
        //        sb.Append("       CONSOL_INVOICE_TRN_TBL CONTRN,");
        //        sb.Append("       CURRENCY_TYPE_MST_TBL  CMT,");
        //        sb.Append("       JOB_CARD_TRN JOBEXP ");
        //        sb.Append(" WHERE CRN.CRN_TBL_PK = CRNT.CRN_TBL_FK");
        //        sb.Append("     AND CMT.CURRENCY_MST_PK = CRN.CURRENCY_MST_FK");
        //        sb.Append("     AND CRNT.CONSOL_INVOICE_TRN_FK = CON.CONSOL_INVOICE_PK");
        //        sb.Append("     AND CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
        //        sb.Append("     AND JOBEXP.JOB_CARD_TRN_PK = CONTRN.JOB_CARD_FK");
        //        sb.Append("   AND CRN.CRN_TBL_PK =" + CRPK);
        //        DataTable dtCR = new DataTable();
        //        try
        //        {
        //            dtCR = objWF.GetDataTable(sb.ToString());

        //            rptDoc.Load(Server.MapPath("..") + "/Payable/rptCrGeneralEntrySummary.rpt");
        //            rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
        //            AddressDS = objCreditNote.FetchLocation(Session["LOGED_IN_LOC_FK"]);
        //            AddressDS.ReadXmlSchema(Server.MapPath("..") + "/Payable/CR_Location.xsd");
        //            DsCrMain = objCreditNote.Fetch_General_Custumer(dtCR.Rows[0]["CUSTOMER_MST_FK"]);
        //            DsCrMain.ReadXmlSchema(Server.MapPath("..") + "/Payable/GenaralCreditCust.xsd");
        //            rptDoc.OpenSubreport("Cr_Location").SetDataSource(AddressDS);

        //            rptDoc.SetDataSource(DsCrMain);

        //            rptDoc.SetParameterValue("JobCardNr", dtCR.Rows[0]["JOBCARD_REF_NO"]);
        //            rptDoc.SetParameterValue("Curr", dtCR.Rows[0]["CURRENCY_ID"]);
        //            rptDoc.SetParameterValue("CreditAmt", dtCR.Rows[0]["CRN_AMMOUNT"]);
        //            rptDoc.SetParameterValue("CrRefNr", dtCR.Rows[0]["CRN_REF"]);
        //            rptDoc.SetParameterValue("CreditDate", dtCR.Rows[0]["CRN_DATE"]);
        //            rptDoc.SetParameterValue("ApplyBarcode", "");
        //            rptDoc.SetParameterValue("InvAmount", dtCR.Rows[0]["INVOICE_AMT"]);

        //            objrep.getReportControls(ref rptDoc, "QFOR4095", 6);
        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //    }

        //    public void GeneratePDFCR(Int32 biztype, Int32 process, string Refno, Int32 CRPK, ref CrystalDecisions.CrystalReports.Engine.ReportDocument repdoc)
        //    {
        //        DataSet AddressDS = new DataSet();
        //        string barcode = "";
        //        short AgentFlag = 0;
        //        string strConsigneeLbl = "";
        //        DataSet MainDS = new DataSet();
        //        DataSet CntDS = new DataSet();
        //        string CONTAINER = "";
        //        Int32 i = default(Int32);
        //        clsCrNoteCustomerEntry objCreditNo = new clsCrNoteCustomerEntry();
        //        clsInvoiceEntryAir objInvoiceEntry = new clsInvoiceEntryAir();
        //        Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
        //        clsCrNoteCustomerEntryAir objCreditNoair = new clsCrNoteCustomerEntryAir();
        //        clsInvoiceEntryAir objInvoiceEntryair = new clsInvoiceEntryAir();
        //        try
        //        {
        //            //Added by Snigdha for show Barcode validation and Protocol definition setting
        //            if (objCreditNo.GetBarcodeFlag("CUSTOMER CREDIT NOTE SEA EXPORT") == "1" & ConfigurationManager.AppSettings["ShowBarcode"] == "1")
        //            {
        //                barcode = "*" + Refno + "*";
        //            }
        //            repdoc.Load(Server.MapPath("../07Reports") + "\\rptCreditNote.rpt");

        //            if (biztype == 2)
        //            {
        //                MainDS = objCreditNo.FetchReportData(CRPK);
        //                CntDS = objCreditNo.FetchContainerDetails(CRPK);
        //            }
        //            else
        //            {
        //                MainDS = objCreditNoair.FetchReportData(CRPK);
        //                CntDS = objCreditNoair.FetchContainerDetails(CRPK);
        //            }

        //            MainDS.ReadXmlSchema(Server.MapPath("../07Reports") + "\\CREDITNOTE.XSD");
        //            for (i = 0; i <= CntDS.Tables[0].Rows.Count - 1; i++)
        //            {
        //                if (!Information.IsDBNull(CntDS.Tables[0].Rows[i]["CONTAINER"]))
        //                {
        //                    CONTAINER = CONTAINER + CntDS.Tables[0].Rows[i]["CONTAINER"] + ",";
        //                }
        //            }
        //            if ((CONTAINER.LastIndexOf(",") != -1))
        //            {
        //                CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
        //            }
        //            AddressDS = ObjClsTransportNote.FetchLocation(Session["LOGED_IN_LOC_FK"]);
        //            AddressDS.ReadXmlSchema(Server.MapPath("../07Reports") + "\\TN_Location.xsd");

        //            repdoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);

        //            repdoc.SetDataSource(MainDS);

        //            repdoc.SetParameterValue("Container", CONTAINER);
        //            repdoc.SetParameterValue("Type", 2);
        //            repdoc.SetParameterValue("strConsigneLbl", strConsigneeLbl);
        //            repdoc.SetParameterValue("ApplyBarcode", barcode);
        //            Int32 j = default(Int32);
        //            string Headers = "'CREDIT NOTE','Local Port/Dock/Terminal','Consignee','Container/Unit #','Delivery Point','Airport/Port-Loading','Airport/Port-Discharge','ICollection Point','Flight/Vessel','Final Destination','Bill Of Lading/AWB #','Terms of Shipment','Freight Payable at','FOB Value','Insurance','Marks and Numbers','Goods','HM','Cube m3','Kg Gross','Chargeable','Description','Charges','VAT','Tax Code','Ancillaries','Total Amount Due in','Kg Net','Charges To'";
        //            DataSet dsHdr = new DataSet();
        //            dsHdr = objCreditNo.FetchCaption(Headers, Session["LOGED_IN_LOC_FK"]);
        //            Headers = Headers.Replace("'", "");
        //            string[] PKHdr = Headers.Split(",");
        //            if (dsHdr.Tables[0].Rows.Count > 0)
        //            {
        //                for (i = 0; i <= PKHdr.Length - 1; i++)
        //                {
        //                    for (j = 0; j <= dsHdr.Tables[0].Rows.Count - 1; j++)
        //                    {
        //                        if ((dsHdr.Tables[0].Rows[j]["mainLang"] == PKHdr[i]))
        //                        {
        //                            if (!Information.IsDBNull(dsHdr.Tables[0].Rows[j]["subLang"]))
        //                            {
        //                                repdoc.SetParameterValue(PKHdr[i], " / " + dsHdr.Tables[0].Rows[j]["subLang"]);
        //                            }
        //                            else
        //                            {
        //                                repdoc.SetParameterValue(PKHdr[i], "");
        //                            }
        //                        }
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                for (i = 0; i <= PKHdr.Length - 1; i++)
        //                {
        //                    repdoc.SetParameterValue(PKHdr[i], PKHdr[i] + "");
        //                }
        //            }
        //            //this is for captions to be displayed with colon
        //            string param = "'Remarks','Your VAT #','Payment Terms','Payment Due','Reference','ETD','ETA'";
        //            DataSet ds = new DataSet();
        //            ds = objCreditNo.FetchCaption(param, Session["LOGED_IN_LOC_FK"]);
        //            param = param.Replace("'", "");
        //            string[] PK = param.Split(",");
        //            if (ds.Tables[0].Rows.Count > 0)
        //            {
        //                for (i = 0; i <= PK.Length - 1; i++)
        //                {
        //                    for (j = 0; j <= ds.Tables[0].Rows.Count - 1; j++)
        //                    {
        //                        if ((ds.Tables[0].Rows[j]["mainLang"] == PK[i]))
        //                        {
        //                            if (!Information.IsDBNull(ds.Tables[0].Rows[j]["subLang"]))
        //                            {
        //                                repdoc.SetParameterValue(PK[i], "");
        //                            }
        //                            else
        //                            {
        //                                repdoc.SetParameterValue(PK[i], "");
        //                            }
        //                        }
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                for (i = 0; i <= PK.Length - 1; i++)
        //                {
        //                    repdoc.SetParameterValue(PK[i], PK[i] + ":");
        //                }
        //            }
        //            repdoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

        //            QFORBusinessDev.CommonFeatures objrep = new QFORBusinessDev.CommonFeatures();
        //            objrep.getReportControls(repdoc, "QFOR4015");
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    public void GeneratePDFCRAgent(Int32 biztype, Int32 process, string Refno, Int32 CRPK, ref CrystalDecisions.CrystalReports.Engine.ReportDocument repdoc, string agenttype)
        //    {
        //        Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
        //        DataSet MainDS = new DataSet();
        //        DataSet AddressDS = new DataSet();
        //        DataSet CntDS = new DataSet();
        //        Int16 i = default(Int16);
        //        Int16 j = default(Int16);
        //        string CONTAINER = "";
        //        short AgentFlag = 0;
        //        string strConsigneeLbl = "";
        //        string barcode = "";
        //        clsCreditNoteToAgentEntry objCreditNo = new clsCreditNoteToAgentEntry();
        //        clsCreditNoteToAgentAirEntry objSeaCreditNo = new clsCreditNoteToAgentAirEntry();
        //        clsCreditNoteToAgentSeaImpEntry objSICreditNo = new clsCreditNoteToAgentSeaImpEntry();
        //        clsCreditNoteToAgentAirImpEntry objAICreditNo = new clsCreditNoteToAgentAirImpEntry();
        //        try
        //        {
        //            AddressDS = ObjClsTransportNote.FetchLocation(Session["LOGED_IN_LOC_FK"]);
        //            AddressDS.ReadXmlSchema(Server.MapPath("../07Reports") + "\\TN_Location.xsd");

        //            if (objCreditNo.GetBarcodeFlag("AGENT CREDIT NOTE SEA EXPORT") == "1" & ConfigurationManager.AppSettings["ShowBarcode"] == "1")
        //            {
        //                barcode = "*" + Refno + "*";
        //            }
        //            if (agenttype == "CB")
        //            {
        //                AgentFlag = 1;
        //                strConsigneeLbl = "Consignor";
        //            }
        //            else
        //            {
        //                AgentFlag = 2;
        //                strConsigneeLbl = "Consignee";
        //            }
        //            if (biztype == 2 & process == 1)
        //            {
        //                MainDS = objCreditNo.FetchReportData(CRPK, AgentFlag);
        //                CntDS = objCreditNo.FetchContainerDetails(CRPK);
        //            }
        //            else if (biztype == 1 & process == 1)
        //            {
        //                MainDS = objSeaCreditNo.FetchReportData(CRPK, AgentFlag);
        //                CntDS = objSeaCreditNo.FetchContainerDetails(CRPK);
        //            }
        //            else if (biztype == 2 & process == 2)
        //            {
        //                MainDS = objSICreditNo.FetchReportData(CRPK, AgentFlag);
        //                CntDS = objSICreditNo.FetchContainerDetails(CRPK);
        //            }
        //            else if (biztype == 1 & process == 2)
        //            {
        //                MainDS = objAICreditNo.FetchReportData(CRPK, AgentFlag);
        //                CntDS = objAICreditNo.FetchContainerDetails(CRPK);
        //            }

        //            for (i = 0; i <= CntDS.Tables[0].Rows.Count - 1; i++)
        //            {
        //                if (!Information.IsDBNull(CntDS.Tables[0].Rows[i]["CONTAINER"]))
        //                {
        //                    CONTAINER = CONTAINER + CntDS.Tables[0].Rows[i]["CONTAINER"] + ",";
        //                }
        //            }
        //            if ((CONTAINER.LastIndexOf(",") != -1))
        //            {
        //                CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
        //            }
        //            repdoc.Load(Server.MapPath("../07Reports") + "\\rptCreditNote.RPT");
        //            MainDS.ReadXmlSchema(Server.MapPath("../07Reports") + "\\CREDITNOTE.xsd");
        //            repdoc.SetDataSource(MainDS);
        //            repdoc.SetParameterValue("Container", CONTAINER);
        //            repdoc.SetParameterValue("Type", 1);
        //            repdoc.SetParameterValue("strConsigneLbl", strConsigneeLbl);
        //            repdoc.SetParameterValue("ApplyBarcode", barcode);
        //            repdoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
        //            repdoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    //-----------------------------------
        //    //Credit note report by new design

        //    public void GeneratePDFCreditNote(Int32 biztype, Int32 process, string Refno, Int32 CRPK, ref CrystalDecisions.CrystalReports.Engine.ReportDocument repdoc, string agenttype)
        //    {
        //        Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
        //        clsCreditNoteToAgentEntry objCreditNo = new clsCreditNoteToAgentEntry();
        //        clsCreditNoteToAgentAirEntry objSeaCreditNo = new clsCreditNoteToAgentAirEntry();
        //        clsCreditNoteToAgentSeaImpEntry objSICreditNo = new clsCreditNoteToAgentSeaImpEntry();
        //        clsCreditNoteToAgentAirImpEntry objAICreditNo = new clsCreditNoteToAgentAirImpEntry();
        //        cls_PrintManager objPrintMn = new cls_PrintManager();

        //        DataSet MainDS = new DataSet();
        //        DataSet AddressDS = new DataSet();
        //        DataSet CntDS = new DataSet();

        //        Int16 i = default(Int16);
        //        Int16 j = default(Int16);
        //        string CONTAINER = "";
        //        short AgentFlag = 0;
        //        string strConsigneeLbl = "";
        //        string barcode = "";
        //        QFORBusinessDev.CommonFeatures objrep = new QFORBusinessDev.CommonFeatures();
        //        try
        //        {
        //            AddressDS = ObjClsTransportNote.FetchLocation(Session["LOGED_IN_LOC_FK"]);
        //            AddressDS.ReadXmlSchema(Server.MapPath("../07Reports") + "\\TN_Location.xsd");

        //            if (objCreditNo.GetBarcodeFlag("AGENT CREDIT NOTE SEA EXPORT") == "1" & ConfigurationManager.AppSettings["ShowBarcode"] == "1")
        //            {
        //                barcode = "*" + Refno.Trim() + "*";
        //            }
        //            if (Request.QueryString["AgentType"] == "CB")
        //            {
        //                AgentFlag = 1;
        //                strConsigneeLbl = "Consignor";
        //            }
        //            else
        //            {
        //                AgentFlag = 2;
        //                strConsigneeLbl = "Consignee";
        //            }

        //            MainDS = objPrintMn.FetchCRNReportData(CRPK, AgentFlag, biztype, process);
        //            if (biztype == 2 & process == 1)
        //            {
        //                CntDS = objCreditNo.FetchContainerDetails(CRPK);
        //            }
        //            else if (biztype == 1 & process == 1)
        //            {
        //                CntDS = objSeaCreditNo.FetchContainerDetails(CRPK);
        //            }
        //            else if (biztype == 2 & process == 2)
        //            {
        //                CntDS = objSICreditNo.FetchContainerDetails(CRPK);
        //            }
        //            else if (biztype == 1 & process == 2)
        //            {
        //                CntDS = objAICreditNo.FetchContainerDetails(CRPK);
        //            }

        //            for (i = 0; i <= CntDS.Tables[0].Rows.Count - 1; i++)
        //            {
        //                if (!Information.IsDBNull(CntDS.Tables[0].Rows[i]["CONTAINER"]))
        //                {
        //                    CONTAINER = CONTAINER + CntDS.Tables[0].Rows[i]["CONTAINER"] + ",";
        //                }
        //            }
        //            if ((CONTAINER.LastIndexOf(",") != -1))
        //            {
        //                CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
        //            }

        //            repdoc.Load(Server.MapPath("../07Reports") + "\\rptCreditNote_New.RPT");

        //            MainDS.ReadXmlSchema(Server.MapPath("../07Reports") + "\\CreditNote_New.xsd");
        //            repdoc.SetDataSource(MainDS);

        //            repdoc.SetParameterValue("Container", CONTAINER);
        //            repdoc.SetParameterValue("Type", 1);
        //            repdoc.SetParameterValue("strConsigneLbl", strConsigneeLbl);
        //            repdoc.SetParameterValue("ApplyBarcode", barcode);
        //            string Headers = null;
        //            Headers = "'CREDIT NOTE','Consignee','Credit Note Nr.','Credit Note Date','Job Card Nr.','Job Card Date','Currency',";
        //            Headers += "'Invoice Nr.','Invoice Dt.','Gross Amt.','VAT Amt.','Discount','Inv. Cur.','Amount','Cr. Note Amt.','Remarks','Credit Note Amount',";
        //            Headers += "'Prepared By','Verified By','Approved By'";

        //            DataSet dsHdr = new DataSet();
        //            dsHdr = objCreditNo.FetchCaption(Headers, Session["LOGED_IN_LOC_FK"]);
        //            Headers = Headers.Replace("'", "");
        //            string[] PKHdr = Headers.Split(",");
        //            if (dsHdr.Tables[0].Rows.Count > 0)
        //            {
        //                for (i = 0; i <= PKHdr.Length - 1; i++)
        //                {
        //                    bool ParamAdded = false;
        //                    for (j = 0; j <= dsHdr.Tables[0].Rows.Count - 1; j++)
        //                    {
        //                        if ((dsHdr.Tables[0].Rows[j]["mainLang"] == PKHdr[i]))
        //                        {
        //                            if (!Information.IsDBNull(dsHdr.Tables[0].Rows[j]["subLang"]))
        //                            {
        //                                repdoc.SetParameterValue(PKHdr[i], " / " + dsHdr.Tables[0].Rows[j]["subLang"]);
        //                            }
        //                            else
        //                            {
        //                                repdoc.SetParameterValue(PKHdr[i], PKHdr[i]);
        //                            }
        //                            ParamAdded = true;
        //                            break; // TODO: might not be correct. Was : Exit For
        //                        }
        //                    }
        //                    if (!ParamAdded)
        //                    {
        //                        repdoc.SetParameterValue(PKHdr[i], PKHdr[i]);
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                for (i = 0; i <= PKHdr.Length - 1; i++)
        //                {
        //                    repdoc.SetParameterValue(PKHdr[i], PKHdr[i] + "");
        //                }
        //            }

        //            repdoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
        //            repdoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
        //            objrep.getReportControls(repdoc, "QFOR4015");

        //            //CrystalReportViewer1.ReportSource = repdoc
        //            //Dim DsQuoteDetails As New DataSet
        //            //Dim StorageVal, StoreorPreviewFlag As Integer
        //            //Dim UniqueRefNo, strDocName As String
        //            //Dim BP As String = ""
        //            //If biztype = 2 And process = 1 Then
        //            //    BP = "SeaExp"
        //            //ElseIf biztype = 2 And process = 2 Then
        //            //    BP = "SeaImp"
        //            //ElseIf biztype = 1 And process = 1 Then
        //            //    BP = "AirExp"
        //            //ElseIf biztype = 1 And process = 2 Then
        //            //    BP = "AirImp"
        //            //End If
        //            //UniqueRefNo = "CreditNoteCBAgent" & BP & "-" & "-" & Refno & "-" & Format(Date.Now, "ddMMyyyyHHmmss").ToString()

        //            //DsQuoteDetails = objTrTc.FetchCrNoteToCB()
        //            //If DsQuoteDetails.Tables(0).Rows.Count > 0 Then
        //            //    StorageVal = DsQuoteDetails.Tables(0).Rows(0).Item("APPLY_STORAGE")
        //            //End If
        //            //If StoreorPreviewFlag = 2 Then ''Coming From Save
        //            //Else                           ''Coming From Print
        //            //    If StorageVal = 1 Then
        //            //        StoreorPreviewFlag = 3
        //            //        strDocName = UniqueRefNo
        //            //    Else
        //            //        StoreorPreviewFlag = 1
        //            //    End If
        //            //End If
        //            //---------------------------
        //            //StoreorPreviewFlag = 3
        //            //strDocName = UniqueRefNo
        //            //fn_Print_Store_Preview(repdoc, 1, strDocName, StoreorPreviewFlag)
        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //    }
        //    #endregion
        //    #region "Fetch Grid Header for Hard Coded Grids"
        //    //modified by thiyagarajan on 22/1/09:Vek Req.
        //    //"ContType" added newly for Track n trace Html Table
        //    public object FetchGridHeader(Page MyPage, string ConfigurationID, string ContType = "HC", Int32 sort = 0, string CONFIG_CTRL_ID = "")
        //    {
        //        WorkFlow objWK = new WorkFlow();
        //        DataSet ds = new DataSet();
        //        string strSQL = null;
        //        try
        //        {
        //            strSQL = strSQL + Constants.vbCrLf + " SELECT ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CONTROLS_PK, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONMST.CONFIG_MST_PK, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_ID, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_DEF_TEXT, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CONTROL_TOOLTIP, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_MIN_LENGTH, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_MAX_LENGTH, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_FIELD_TYPE, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_GRID_COLUMN_ALIGN, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_GRID_COL_MAX_LENGTH ";
        //            strSQL = strSQL + Constants.vbCrLf + " FROM ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONFIG_CONTROLS_TBL CONCTL, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONFIG_MST_TBL CONMST ";
        //            strSQL = strSQL + Constants.vbCrLf + " WHERE ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_MST_FK = CONMST.CONFIG_MST_PK ";
        //            if (!string.IsNullOrEmpty(ContType))
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " AND CONCTL.CONFIG_CTRL_FIELD_TYPE = '" + ContType + "' ";
        //            }
        //            strSQL = strSQL + Constants.vbCrLf + " AND UPPER(CONMST.CONFIG_ID)='" + ConfigurationID.ToUpper() + "' ";
        //            if (!string.IsNullOrEmpty(CONFIG_CTRL_ID))
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " AND UPPER(CONCTL.CONFIG_CTRL_ID) LIKE '%" + CONFIG_CTRL_ID.ToUpper() + "%' ";
        //            }
        //            if (sort == 1)
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + "  order by CONCTL.config_controls_pk asc ";
        //            }
        //            return (objWK.GetDataSet(strSQL));
        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //    }
        //    //adding by thiyagarajan on 12/1/09
        //    //fetching all the grid col.details :VEK REQ.
        //    public void FetchGridColumns(Page MyPage, string ConfigurationID, string P_Flag = "", string CONFIG_CTRL_ID = "")
        //    {
        //        WorkFlow objWK = new WorkFlow();
        //        DataSet ds = new DataSet();
        //        string strSQL = null;
        //        try
        //        {
        //            strSQL = strSQL + Constants.vbCrLf + " SELECT ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CONTROLS_PK, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONMST.CONFIG_MST_PK, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_ID, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_DEF_TEXT, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CONTROL_TOOLTIP, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_MIN_LENGTH, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_MAX_LENGTH, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_FIELD_TYPE, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_GRID_COLUMN_ALIGN, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_GRID_COL_MAX_LENGTH ";
        //            strSQL = strSQL + Constants.vbCrLf + " FROM ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONFIG_CONTROLS_TBL CONCTL, ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONFIG_MST_TBL CONMST ";
        //            strSQL = strSQL + Constants.vbCrLf + " WHERE ";
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_MST_FK = CONMST.CONFIG_MST_PK ";
        //            strSQL = strSQL + Constants.vbCrLf + " AND CONCTL.CONFIG_CTRL_FIELD_TYPE = 'HD' ";
        //            strSQL = strSQL + Constants.vbCrLf + " AND UPPER(CONMST.CONFIG_ID)='" + ConfigurationID.ToUpper() + "' ";
        //            if (!string.IsNullOrEmpty(CONFIG_CTRL_ID))
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " AND UPPER(CONCTL.CONFIG_CTRL_ID) LIKE '%" + CONFIG_CTRL_ID.ToUpper() + "%' ";
        //            }
        //            if (!string.IsNullOrEmpty(P_Flag))
        //            {
        //                strSQL = strSQL + Constants.vbCrLf + " AND CONCTL.CONFIG_FLAG IN (" + P_Flag + ") ";
        //            }

        //            ds = objWK.GetDataSet(strSQL);

        //            Int32 I = default(Int32);
        //            for (I = 0; I <= ds.Tables[0].Rows.Count - 1; I++)
        //            {
        //                GridHeaderSetting objGridHeader = new GridHeaderSetting();
        //                objGridHeader.M_TEXT = getDefault(ds.Tables[0].Rows[I]["CONFIG_CTRL_DEF_TEXT"], "") + "";
        //                objGridHeader.M_ToolTip = getDefault(ds.Tables[0].Rows[I]["CONFIG_CONTROL_TOOLTIP"], "") + "";
        //                objGridHeader.M_Width = getDefault(ds.Tables[0].Rows[I]["CONFIG_CTRL_MAX_LENGTH"], 0) + "";
        //                objGridHeader.M_Settings = getDefault(ds.Tables[0].Rows[I]["CONFIG_CTRL_MIN_LENGTH"], 0) + "";
        //                objGridHeader.M_CtrlID = getDefault(ds.Tables[0].Rows[I]["CONFIG_CTRL_ID"], "") + "";
        //                GridHeaders.Add(objGridHeader, objGridHeader.M_CtrlID);
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    #endregion

        //    ///'''''''''''Added By Uma on 30/03/09 ''''
        //    // for Message Sending'
        //    #region "Sending Mail in Messaging Center"
        //    public object fn_Messaging_Center(string ConfigID, long TransActionPk, long Party_mst_fk, long DocumentPK, string FileName = "", Hashtable AdnlQry = null, ReportDocument RptDoc_PrintToPrinter = null, long Consignee_Fk = 0, long Dpagent_Fk = 0, long NotifyPartyFk = 0,
        //    string MailID = "", string RefID = "", string RemSubject = "", string MailSubject = "", string MailBody = "")
        //    {
        //        object functionReturnValue = null;
        //        try
        //        {
        //            Int16 col_Count = default(Int16);
        //            WorkFlow.cls_Mailing Mail_obj = new WorkFlow.cls_Mailing();
        //            DataSet ds_msg = new DataSet();
        //            cls_User_DocMessaging objMsg = new cls_User_DocMessaging();
        //            cls_ExternalMail ObjExMail = new cls_ExternalMail();
        //            cls_Announcement ObjAnn = new cls_Announcement();
        //            cls_ApprovalRequests ObjApp = new cls_ApprovalRequests();
        //            cls_Mailing ObMail = new cls_Mailing();
        //            Hashtable hTable = new Hashtable();
        //            string StrFinal = null;
        //            DataTable dt1 = new DataTable();
        //            DataTable dt2 = new DataTable();
        //            string RefNr = "";
        //            int MessageFlag = 0;
        //            ArrayList arrMessageMail = new ArrayList();
        //            if (!string.IsNullOrEmpty(RefID))
        //            {
        //                RefNr = RefID;
        //            }
        //            else
        //            {
        //                if (!string.IsNullOrEmpty(FileName))
        //                {
        //                    FileName = Strings.Left(Strings.Mid(FileName, Strings.InStr(FileName, "-") + 1), Strings.Len(FileName) - Strings.InStr(FileName, "-"));
        //                }
        //            }
        //            if (ConfigID != "COD")
        //            {
        //                ///'getting the table Name,column Name
        //                WorkFlow.cls_Mailing objQrygen = new WorkFlow.cls_Mailing();
        //                ds_msg = objQrygen.fn_BuildQry_msgCenter(DocumentPK, TransActionPk);
        //                if (ds_msg.Tables[0].Rows.Count > 0)
        //                {
        //                    //'Putting the column Name and Collumn value in the Hash table collection
        //                    for (col_Count = 0; col_Count <= ds_msg.Tables[0].Columns.Count - 1; col_Count++)
        //                    {
        //                        hTable.Add(ds_msg.Tables[0].Columns[col_Count].ColumnName, ds_msg.Tables[0].Rows[0][col_Count]);
        //                    }
        //                }
        //                else
        //                {
        //                    arrMessageMail.Add("Error");
        //                    return arrMessageMail;
        //                    return functionReturnValue;
        //                }
        //                ///getting the Customer based Messaging Information
        //                DataTable dt = new DataTable();
        //                dt = objMsg.fn_fetchMsgDetails(ConfigID, TransActionPk, Party_mst_fk, DocumentPK, AdnlQry);
        //                if (dt.Rows.Count <= 0)
        //                {
        //                    arrMessageMail.Add("Error");
        //                    MessageFlag = 1;
        //                }
        //                int i = 0;
        //                if (dt.Rows.Count > 0)
        //                {
        //                    //Cargo,Freight Manifest Mail send to Notify,Consignee,DP Agent
        //                    if (!string.IsNullOrEmpty(MailID))
        //                    {
        //                        if (!Information.IsDBNull(dt.Rows[0]["EMAIL_FLAG"]))
        //                        {
        //                            if (dt.Rows[0]["EMAIL_FLAG"] == 1)
        //                            {
        //                                Mail_obj.M_SMTP_SERVER = Convert.ToString(Application["SMTP_SERVER"]);
        //                                Mail_obj.M_MAIL_SERVER = Convert.ToString(Application["MAIL_SERVER"]);
        //                                Mail_obj.M_SEND_USERNAME = Convert.ToString(Application["SEND_USERNAME"]);
        //                                Mail_obj.M_SEND_PASSWORD = Convert.ToString(Application["SEND_PASSWORD"]);

        //                                Mail_obj.M_MAIL_SUBJECT = fn_ReplaceMessages(dt.Rows[0]["DOC_EMAIL_SUBJECT"], hTable);
        //                                Mail_obj.M_MAIL_BODY = fn_ReplaceMessages(dt.Rows[0]["DOC_EMAIL_MSG"], hTable);
        //                                Mail_obj.DocTypePK = DocumentPK;
        //                                Mail_obj.DocNr = RefNr;
        //                                Mail_obj.Create_by = Session["USER_PK"];
        //                                Mail_obj.M_Name = Convert.ToString(Application["SEND_USERNAME"]);
        //                                Mail_obj.M_Password = Convert.ToString(Application["SEND_PASSWORD"]);
        //                                Mail_obj.M_MAIL_TO = MailID;
        //                                if ((dt.Rows[0]["email_id"] != null) & !Information.IsDBNull(dt.Rows[0]["email_id"]))
        //                                {
        //                                    if (MailID.ToUpper() != Convert.ToString(dt.Rows[0]["email_id"]).ToUpper())
        //                                    {
        //                                        Mail_obj.M_MAIL_TO += "," + dt.Rows[0]["email_id"];
        //                                    }
        //                                }
        //                                if (dt.Rows[i]["DOC_ATTACH"] == 1)
        //                                {
        //                                    Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 1;
        //                                    //Session("FileName") = FileName
        //                                    ObjExMail.FileName = FileName;
        //                                    ObMail.FileName = FileName;
        //                                    ObjAnn.FileName = FileName;
        //                                    ObjApp.FileName = FileName;
        //                                    Mail_obj.M_MAIL_ATTACHMENTS_PATH = FileName;
        //                                    Mail_obj.fn_send_ExternalMail();
        //                                    //'with attachment
        //                                }
        //                                else
        //                                {
        //                                    Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 0;
        //                                    Mail_obj.fn_send_ExternalMail();
        //                                }
        //                            }
        //                        }
        //                    }
        //                    else
        //                    {
        //                        for (i = 0; i <= dt.Rows.Count - 1; i++)
        //                        {
        //                            if (!Information.IsDBNull(dt.Rows[i]["EMAIL_FLAG"]))
        //                            {
        //                                if (dt.Rows[i]["EMAIL_FLAG"] == 1)
        //                                {
        //                                    Mail_obj.M_SMTP_SERVER = Convert.ToString(Application["SMTP_SERVER"]);
        //                                    Mail_obj.M_MAIL_SERVER = Convert.ToString(Application["MAIL_SERVER"]);
        //                                    Mail_obj.M_SEND_USERNAME = Convert.ToString(Application["SEND_USERNAME"]);
        //                                    Mail_obj.M_SEND_PASSWORD = Convert.ToString(Application["SEND_PASSWORD"]);
        //                                    Mail_obj.M_MAIL_SUBJECT = fn_ReplaceMessages(dt.Rows[i]["DOC_EMAIL_SUBJECT"], hTable);
        //                                    if (!string.IsNullOrEmpty(RemSubject))
        //                                    {
        //                                        Mail_obj.M_MAIL_SUBJECT = Mail_obj.M_MAIL_SUBJECT + " " + RemSubject;
        //                                    }
        //                                    Mail_obj.M_MAIL_BODY = fn_ReplaceMessages(dt.Rows[i]["DOC_EMAIL_MSG"], hTable);
        //                                    Mail_obj.DocTypePK = DocumentPK;
        //                                    Mail_obj.DocNr = RefNr;
        //                                    Mail_obj.Create_by = Session["USER_PK"];
        //                                    Mail_obj.M_Name = Convert.ToString(Application["SEND_USERNAME"]);
        //                                    Mail_obj.M_Password = Convert.ToString(Application["SEND_PASSWORD"]);
        //                                    if ((dt.Rows[i]["email_id"] != null) & !Information.IsDBNull(dt.Rows[i]["email_id"]))
        //                                    {
        //                                        Mail_obj.M_MAIL_TO = dt.Rows[i]["email_id"];
        //                                        if (dt.Rows[i]["DOC_ATTACH"] == 1)
        //                                        {
        //                                            Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 1;
        //                                            ObjExMail.FileName = FileName;
        //                                            ObMail.FileName = FileName;
        //                                            ObjAnn.FileName = FileName;
        //                                            ObjApp.FileName = FileName;
        //                                            Mail_obj.M_MAIL_ATTACHMENTS_PATH = FileName;
        //                                            Mail_obj.fn_send_ExternalMail();
        //                                            //'with attachment
        //                                        }
        //                                        else
        //                                        {
        //                                            Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 0;
        //                                            Mail_obj.fn_send_ExternalMail();
        //                                        }
        //                                    }
        //                                }
        //                            }
        //                            if (!Information.IsDBNull(dt.Rows[i]["EMAIL_FLAG"]))
        //                            {
        //                                if (dt.Rows[0]["EMAIL_FLAG"] == 1)
        //                                {
        //                                    if (!Information.IsDBNull(dt.Rows[i]["CONSIGNEE_FLAG"]))
        //                                    {
        //                                        if (dt.Rows[i]["CONSIGNEE_FLAG"] == 1)
        //                                        {
        //                                            dt1 = GetCustomerEmail(Consignee_Fk);
        //                                            if (dt1.Rows.Count > 0)
        //                                            {
        //                                                Mail_obj.M_SMTP_SERVER = Convert.ToString(Application["SMTP_SERVER"]);
        //                                                Mail_obj.M_MAIL_SERVER = Convert.ToString(Application["MAIL_SERVER"]);
        //                                                Mail_obj.M_SEND_USERNAME = Convert.ToString(Application["SEND_USERNAME"]);
        //                                                Mail_obj.M_SEND_PASSWORD = Convert.ToString(Application["SEND_PASSWORD"]);
        //                                                Mail_obj.M_MAIL_SUBJECT = fn_ReplaceMessages(dt.Rows[i]["DOC_EMAIL_SUBJECT"], hTable);
        //                                                Mail_obj.M_MAIL_BODY = fn_ReplaceMessages(dt.Rows[i]["DOC_EMAIL_MSG"], hTable);
        //                                                if ((dt1.Rows[i]["ADM_EMAIL_ID"] != null) & !Information.IsDBNull(dt1.Rows[i]["ADM_EMAIL_ID"]))
        //                                                {
        //                                                    Mail_obj.M_MAIL_TO = dt1.Rows[i]["ADM_EMAIL_ID"];
        //                                                    Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 1;
        //                                                    ObjExMail.FileName = FileName;
        //                                                    ObMail.FileName = FileName;
        //                                                    ObjAnn.FileName = FileName;
        //                                                    ObjApp.FileName = FileName;
        //                                                    Mail_obj.M_MAIL_ATTACHMENTS_PATH = FileName;
        //                                                    Mail_obj.fn_send_ExternalMail();
        //                                                    //'with attachment
        //                                                }
        //                                            }
        //                                        }
        //                                    }
        //                                    if (!Information.IsDBNull(dt.Rows[i]["DP_AGENT_FLAG"]))
        //                                    {
        //                                        if (dt.Rows[i]["DP_AGENT_FLAG"] == 1)
        //                                        {
        //                                            dt2 = GetDpAgentEmail(Dpagent_Fk);
        //                                            if (dt2.Rows.Count > 0)
        //                                            {
        //                                                Mail_obj.M_SMTP_SERVER = Convert.ToString(Application["SMTP_SERVER"]);
        //                                                Mail_obj.M_MAIL_SERVER = Convert.ToString(Application["MAIL_SERVER"]);
        //                                                Mail_obj.M_SEND_USERNAME = Convert.ToString(Application["SEND_USERNAME"]);
        //                                                Mail_obj.M_SEND_PASSWORD = Convert.ToString(Application["SEND_PASSWORD"]);
        //                                                Mail_obj.M_MAIL_SUBJECT = fn_ReplaceMessages(dt.Rows[i]["DOC_EMAIL_SUBJECT"], hTable);
        //                                                Mail_obj.M_MAIL_BODY = fn_ReplaceMessages(dt.Rows[i]["DOC_EMAIL_MSG"], hTable);
        //                                                if ((dt2.Rows[i]["ADM_EMAIL_ID"] != null) & !Information.IsDBNull(dt2.Rows[i]["ADM_EMAIL_ID"]))
        //                                                {
        //                                                    Mail_obj.M_MAIL_TO = dt2.Rows[i]["ADM_EMAIL_ID"];
        //                                                    Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 1;
        //                                                    ObjExMail.FileName = FileName;
        //                                                    ObMail.FileName = FileName;
        //                                                    ObjAnn.FileName = FileName;
        //                                                    ObjApp.FileName = FileName;
        //                                                    Mail_obj.M_MAIL_ATTACHMENTS_PATH = FileName;
        //                                                    Mail_obj.fn_send_ExternalMail();
        //                                                    //'with attachment
        //                                                }
        //                                            }
        //                                        }
        //                                    }
        //                                    if (!Information.IsDBNull(dt.Rows[i]["DP_AGENT_FLAG"]))
        //                                    {
        //                                        if (dt.Rows[i]["NOTIFY_PARTY_FLAG"] == 1)
        //                                        {
        //                                            dt1 = GetCustomerEmail(NotifyPartyFk);
        //                                            if (dt1.Rows.Count > 0)
        //                                            {
        //                                                Mail_obj.M_SMTP_SERVER = Convert.ToString(Application["SMTP_SERVER"]);
        //                                                Mail_obj.M_MAIL_SERVER = Convert.ToString(Application["MAIL_SERVER"]);
        //                                                Mail_obj.M_SEND_USERNAME = Convert.ToString(Application["SEND_USERNAME"]);
        //                                                Mail_obj.M_SEND_PASSWORD = Convert.ToString(Application["SEND_PASSWORD"]);
        //                                                Mail_obj.M_MAIL_SUBJECT = fn_ReplaceMessages(dt.Rows[i]["DOC_EMAIL_SUBJECT"], hTable);
        //                                                Mail_obj.M_MAIL_BODY = fn_ReplaceMessages(dt.Rows[i]["DOC_EMAIL_MSG"], hTable);
        //                                                if ((dt1.Rows[i]["ADM_EMAIL_ID"] != null) & !Information.IsDBNull(dt1.Rows[i]["ADM_EMAIL_ID"]))
        //                                                {
        //                                                    Mail_obj.M_MAIL_TO = dt1.Rows[i]["ADM_EMAIL_ID"];
        //                                                    Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 1;
        //                                                    ObjExMail.FileName = FileName;
        //                                                    ObMail.FileName = FileName;
        //                                                    ObjAnn.FileName = FileName;
        //                                                    ObjApp.FileName = FileName;
        //                                                    Mail_obj.M_MAIL_ATTACHMENTS_PATH = FileName;
        //                                                    Mail_obj.fn_send_ExternalMail();
        //                                                    //'with attachment
        //                                                }
        //                                            }
        //                                        }
        //                                    }
        //                                }
        //                            }
        //                        }

        //                        if (dt.Rows[0]["SMS_FLAG"] == 1)
        //                        {
        //                            if ((dt.Rows[0]["PHONE_NO"] != null) & !Information.IsDBNull(dt.Rows[0]["PHONE_NO"]))
        //                            {
        //                                long Mobile_No = 0;
        //                                string Sms_msg = null;
        //                                string mn = Convert.ToString(dt.Rows[0]["PHONE_NO"]);
        //                                short _c = 0;
        //                                foreach (char c in mn)
        //                                {
        //                                    try
        //                                    {
        //                                        _c = Convert.ToInt16(c.ToString());
        //                                    }
        //                                    catch (Exception ex)
        //                                    {
        //                                        mn = mn.Replace(c, "");
        //                                    }
        //                                }
        //                                mn = mn.Trim();
        //                                Mobile_No = Convert.ToInt64(mn);
        //                                Sms_msg = fn_ReplaceMessages(Convert.ToString(dt.Rows[0]["DOC_SMS_MSG"]), hTable);
        //                                StrFinal = fn_send_SMS(Mobile_No, Sms_msg);
        //                            }
        //                        }

        //                        if (dt.Rows[0]["FAX_FLAG"] == 1)
        //                        {
        //                        }
        //                        if (dt.Rows[0]["PRINT_FLAG"] == 1)
        //                        {
        //                            Session.Add("RptDoc_PrintToPrinter", RptDoc_PrintToPrinter);
        //                            StrFinal = StrFinal + fn_send_PrintToPrinter(FileName);
        //                        }

        //                        if (dt.Rows[0]["SMS_FLAG"] == 1 | dt.Rows[0]["PRINT_FLAG"] == 1)
        //                        {
        //                            fn_Execute_Script(StrFinal);
        //                        }
        //                    }
        //                    //Cargo,Freight Manifest Mail send to Notify,Consignee,DP Agent with standard protocol
        //                }
        //                else
        //                {
        //                    if (!string.IsNullOrEmpty(MailID))
        //                    {
        //                        dt = Fetch_Email_Std_Protocol_Data(DocumentPK, "MANIFEST");
        //                        if (dt.Rows.Count > 0)
        //                        {
        //                            if (dt.Rows[0]["EMAIL_FLAG"] == 1)
        //                            {
        //                                Mail_obj.M_SMTP_SERVER = Convert.ToString(Application["SMTP_SERVER"]);
        //                                Mail_obj.M_MAIL_SERVER = Convert.ToString(Application["MAIL_SERVER"]);
        //                                Mail_obj.M_SEND_USERNAME = Convert.ToString(Application["SEND_USERNAME"]);
        //                                Mail_obj.M_SEND_PASSWORD = Convert.ToString(Application["SEND_PASSWORD"]);
        //                                Mail_obj.M_MAIL_SUBJECT = fn_ReplaceMessages(dt.Rows[0]["DOC_EMAIL_SUBJECT"], hTable);
        //                                Mail_obj.M_MAIL_BODY = fn_ReplaceMessages(dt.Rows[0]["DOC_EMAIL_MSG"], hTable);
        //                                Mail_obj.DocTypePK = DocumentPK;
        //                                Mail_obj.DocNr = RefNr;
        //                                Mail_obj.Create_by = Session["USER_PK"];
        //                                Mail_obj.M_Name = Convert.ToString(Application["SEND_USERNAME"]);
        //                                Mail_obj.M_Password = Convert.ToString(Application["SEND_PASSWORD"]);
        //                                Mail_obj.M_MAIL_TO = MailID;
        //                                if ((dt.Rows[0]["email_id"] != null) & !Information.IsDBNull(dt.Rows[0]["email_id"]))
        //                                {
        //                                    if (MailID.ToUpper() != Convert.ToString(dt.Rows[0]["email_id"]).ToUpper())
        //                                    {
        //                                        Mail_obj.M_MAIL_TO += "," + dt.Rows[0]["email_id"];
        //                                    }
        //                                }
        //                                if (dt.Rows[0]["DOC_ATTACH"] == 1)
        //                                {
        //                                    Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 1;
        //                                    ObjExMail.FileName = FileName;
        //                                    ObMail.FileName = FileName;
        //                                    ObjAnn.FileName = FileName;
        //                                    ObjApp.FileName = FileName;
        //                                    Mail_obj.M_MAIL_ATTACHMENTS_PATH = FileName;
        //                                    Mail_obj.fn_send_ExternalMail();
        //                                    //'with attachment
        //                                }
        //                                else
        //                                {
        //                                    Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 0;
        //                                    Mail_obj.fn_send_ExternalMail();
        //                                }
        //                            }
        //                        }
        //                    }
        //                }
        //                // COD for HBL
        //            }
        //            else
        //            {
        //                if (!string.IsNullOrEmpty(MailID))
        //                {
        //                    Mail_obj.M_SMTP_SERVER = Convert.ToString(Application["SMTP_SERVER"]);
        //                    Mail_obj.M_MAIL_SERVER = Convert.ToString(Application["MAIL_SERVER"]);
        //                    Mail_obj.M_SEND_USERNAME = Convert.ToString(Application["SEND_USERNAME"]);
        //                    Mail_obj.M_SEND_PASSWORD = Convert.ToString(Application["SEND_PASSWORD"]);
        //                    Mail_obj.M_MAIL_SUBJECT = MailSubject;
        //                    Mail_obj.M_MAIL_BODY = MailBody;
        //                    Mail_obj.DocTypePK = DocumentPK;
        //                    Mail_obj.DocNr = RefNr;
        //                    Mail_obj.Create_by = Session["USER_PK"];
        //                    Mail_obj.M_Name = Convert.ToString(Application["SEND_USERNAME"]);
        //                    Mail_obj.M_Password = Convert.ToString(Application["SEND_PASSWORD"]);
        //                    Mail_obj.M_MAIL_TO = MailID;
        //                    Mail_obj.M_MAIL_ATTACHMENTS_COUNT = 0;
        //                    Mail_obj.fn_send_ExternalMail();
        //                }
        //            }
        //            if (MessageFlag == 1)
        //            {
        //                return arrMessageMail;
        //            }
        //            else
        //            {
        //                arrMessageMail.Add("Success");
        //                return arrMessageMail;
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //        return functionReturnValue;
        //    }
        //    ///'Replace this <<....>> value to invoice date,invoice no etc... 
        //    public string fn_ReplaceMessages(string str_Message, Hashtable ht_obj)
        //    {
        //        try
        //        {
        //            string change_msg = null;
        //            Int16 count = default(Int16);
        //            Int32 num_rec = default(Int32);
        //            IDictionaryEnumerator myEnumerator = ht_obj.GetEnumerator();
        //            while (myEnumerator.MoveNext())
        //            {
        //                str_Message = str_Message.Replace("&lt;&lt;" + myEnumerator.Key + "&gt;&gt;", Convert.ToString(myEnumerator.Value));
        //                str_Message = str_Message.Replace("<<" + myEnumerator.Key + ">>", Convert.ToString(myEnumerator.Value));
        //            }
        //            return str_Message;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    public object fn_Execute_Script(string str)
        //    {
        //        try
        //        {
        //            this.RegisterClientScriptBlock("Startup", str);
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }

        //    }



        //    public string fn_send_SMS(long MobileNo, string SMS_Msg)
        //    {
        //        try
        //        {
        //            string str = null;
        //            string SMS_Usr_Id = ConfigurationManager.AppSettings["SMS_UserId"];
        //            string SMS_Pwd = ConfigurationManager.AppSettings["SMS_Pwd"];
        //            string Argument = "'" + Strings.Trim(SMS_Usr_Id) + "','" + Strings.Trim(SMS_Pwd) + "','" + Strings.Trim(MobileNo) + "','" + Strings.Trim(SMS_Msg) + "'";
        //            //Commented for DTS NO:9084
        //            //str &= "<script>"
        //            //str &= "fn_SendSMSMessage(" & Argument & ");"
        //            //str &= "</script>"
        //            str = "";
        //            return str;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    public string fn_send_PrintToPrinter(string FileName)
        //    {
        //        try
        //        {
        //            string str = null;
        //            string Argument = "'" + Strings.Trim(FileName) + "'";
        //            str += "<script>";
        //            str += "fn_PrintToPrinter(" + Argument + ");";
        //            str += "</script>";
        //            return str;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    #region "fn_Print_Store_Preview"
        //    //POrientation>> 1(Potrait)/2(Landscape)
        //    //StoreorPreviewFlag>> 1(Preview)/2(Just store the file)
        //    //DocName>> If [StoreorPreviewFlag] is 2 the file will be saved with [DocName] name in the localhost/qfor/AttachedDocuments folder
        //    //FormatType>> Whether it is html or pdf or anything
        //    public void fn_Print_Store_Preview(ReportDocument rptDoc, Int32 POrientation, ref string DocName, Int32 StoreorPreviewFlag, string FormatType = "PDF")
        //    {
        //        try
        //        {
        //            string Fname = null;
        //            ExportOptions crExportOptions = null;
        //            DiskFileDestinationOptions crDiskFileDestinationOptions = null;
        //            try
        //            {
        //                rptDoc.PrintOptions.PaperSize = PaperSize.PaperA4;
        //            }
        //            catch (Exception ex)
        //            {
        //            }

        //            if (POrientation == 1)
        //            {
        //                rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Portrait;
        //            }
        //            else
        //            {
        //                rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Landscape;
        //            }

        //            // For previewing the pdf file and then deleting it.
        //            if (StoreorPreviewFlag == 1)
        //            {
        //                Fname = Server.MapPath(".") + "\\" + Session.SessionID.ToString() + ".pdf";
        //                crDiskFileDestinationOptions = new DiskFileDestinationOptions();
        //                crDiskFileDestinationOptions.DiskFileName = Fname;
        //                crExportOptions = rptDoc.ExportOptions;
        //                var _with25 = crExportOptions;
        //                _with25.DestinationOptions = crDiskFileDestinationOptions;
        //                _with25.ExportDestinationType = ExportDestinationType.DiskFile;
        //                if (FormatType == "PDF")
        //                {
        //                    _with25.ExportFormatType = ExportFormatType.PortableDocFormat;
        //                }
        //                rptDoc.Export();
        //                rptDoc.Close();
        //                rptDoc.Dispose();
        //                Response.ClearContent();
        //                Response.ClearHeaders();
        //                if (FormatType == "PDF")
        //                {
        //                    Response.ContentType = "application/pdf";
        //                }
        //                Response.WriteFile(Fname);
        //                Response.Flush();
        //                Response.Close();
        //                System.IO.File.Delete(Fname);
        //            }
        //            else if (StoreorPreviewFlag == 3)
        //            {
        //                Fname = Server.MapPath("..\\..") + "\\Documents\\" + DocName + ".pdf";
        //                crDiskFileDestinationOptions = new DiskFileDestinationOptions();
        //                crDiskFileDestinationOptions.DiskFileName = Fname;
        //                crExportOptions = rptDoc.ExportOptions;
        //                var _with27 = crExportOptions;
        //                _with27.DestinationOptions = crDiskFileDestinationOptions;
        //                _with27.ExportDestinationType = ExportDestinationType.DiskFile;
        //                if (FormatType == "PDF")
        //                {
        //                    _with27.ExportFormatType = ExportFormatType.PortableDocFormat;
        //                }
        //                rptDoc.Export();
        //                rptDoc.Close();
        //                rptDoc.Dispose();
        //                DocName = Fname;
        //                Response.ClearContent();
        //                Response.ClearHeaders();
        //                if (FormatType == "PDF")
        //                {
        //                    Response.ContentType = "application/pdf";
        //                }
        //                Response.WriteFile(Fname);
        //                Response.Flush();
        //                Response.Close();
        //                //crDiskFileDestinationOptions = New DiskFileDestinationOptions
        //                //crDiskFileDestinationOptions.DiskFileName = Fname
        //                //crExportOptions = rptDoc.ExportOptions
        //                //With crExportOptions
        //                //    .DestinationOptions = crDiskFileDestinationOptions
        //                //    .ExportDestinationType = ExportDestinationType.DiskFile
        //                //    If FormatType = "PDF" Then
        //                //        .ExportFormatType = ExportFormatType.PortableDocFormat
        //                //    End If
        //                //End With
        //                //rptDoc.Export()
        //                //rptDoc.Close()
        //                //rptDoc.Dispose()

        //                //Response.ClearContent()
        //                //Response.ClearHeaders()
        //                //For storing the file in the specified folder
        //            }
        //            else
        //            {
        //                Fname = Request.PhysicalApplicationPath + "AttachedDocuments" + "\\" + DocName + ".pdf";
        //                crDiskFileDestinationOptions = new DiskFileDestinationOptions();
        //                crDiskFileDestinationOptions.DiskFileName = Fname;
        //                crExportOptions = rptDoc.ExportOptions;
        //                var _with26 = crExportOptions;
        //                _with26.DestinationOptions = crDiskFileDestinationOptions;
        //                _with26.ExportDestinationType = ExportDestinationType.DiskFile;
        //                if (FormatType == "PDF")
        //                {
        //                    _with26.ExportFormatType = ExportFormatType.PortableDocFormat;
        //                }
        //                rptDoc.Export();
        //                rptDoc.Close();
        //                rptDoc.Dispose();
        //                DocName = Fname;
        //                Response.ClearContent();
        //                Response.ClearHeaders();
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    #endregion

        //    public void fn_Print_Store_Preview_MultiplePDF(ReportDocument rptDoc, Int32 POrientation, ref string DocName, Int32 StoreorPreviewFlag, string FormatType = "PDF")
        //    {
        //        string Fname = null;
        //        ExportOptions crExportOptions = null;
        //        DiskFileDestinationOptions crDiskFileDestinationOptions = null;
        //        rptDoc.PrintOptions.PaperSize = PaperSize.PaperA4;
        //        if (POrientation == 1)
        //        {
        //            rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Portrait;
        //        }
        //        else
        //        {
        //            rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Landscape;
        //        }

        //        // For previewing the pdf file and then deleting it.
        //        if (StoreorPreviewFlag == 1)
        //        {
        //            Fname = Server.MapPath(".") + "\\" + Session.SessionID.ToString() + ".pdf";
        //            crDiskFileDestinationOptions = new DiskFileDestinationOptions();
        //            crDiskFileDestinationOptions.DiskFileName = Fname;
        //            crExportOptions = rptDoc.ExportOptions;
        //            var _with28 = crExportOptions;
        //            _with28.DestinationOptions = crDiskFileDestinationOptions;
        //            _with28.ExportDestinationType = ExportDestinationType.DiskFile;
        //            if (FormatType == "PDF")
        //            {
        //                _with28.ExportFormatType = ExportFormatType.PortableDocFormat;
        //            }
        //            rptDoc.Export();
        //            rptDoc.Close();
        //            rptDoc.Dispose();
        //            //Response.ClearContent()
        //            //Response.ClearHeaders()
        //            //If FormatType = "PDF" Then
        //            //    Response.ContentType = "application/pdf"
        //            //End If
        //            //Response.WriteFile(Fname)
        //            //Response.Flush()
        //            //Response.Close()
        //            //System.IO.File.Delete(Fname)
        //            string sScript = null;
        //            string sKey = "../../Documents/" + DocName + ".pdf";

        //            sScript = "<script type='text/javascript'>window.open('" + sKey + "');</script>";
        //            Page.RegisterClientScriptBlock("focus", sScript);
        //            Response.Write(sScript);
        //        }
        //        else if (StoreorPreviewFlag == 3)
        //        {
        //            Fname = Server.MapPath("..\\..") + "\\Documents\\" + DocName + ".pdf";
        //            crDiskFileDestinationOptions = new DiskFileDestinationOptions();
        //            crDiskFileDestinationOptions.DiskFileName = Fname;
        //            crExportOptions = rptDoc.ExportOptions;
        //            var _with30 = crExportOptions;
        //            _with30.DestinationOptions = crDiskFileDestinationOptions;
        //            _with30.ExportDestinationType = ExportDestinationType.DiskFile;
        //            if (FormatType == "PDF")
        //            {
        //                _with30.ExportFormatType = ExportFormatType.PortableDocFormat;
        //            }
        //            rptDoc.Export();
        //            rptDoc.Close();
        //            rptDoc.Dispose();
        //            //'Added by Mayur for Multiple PDF File opening
        //            string sScript = null;
        //            string sKey = "../../Documents/" + DocName + ".pdf";

        //            sScript = "<script type='text/javascript'>window.open('" + sKey + "');</script>";
        //            Page.RegisterClientScriptBlock("focus", sScript);
        //            Response.Write(sScript);
        //            //'Ended by Mayur

        //            //For storing the file in the specified folder
        //        }
        //        else
        //        {


        //            Fname = Request.PhysicalApplicationPath + "AttachedDocuments" + "\\" + DocName + ".pdf";
        //            //Fname = Server.MapPath(".") & "\" & Session.SessionID.ToString & ".pdf"
        //            crDiskFileDestinationOptions = new DiskFileDestinationOptions();
        //            crDiskFileDestinationOptions.DiskFileName = Fname;
        //            crExportOptions = rptDoc.ExportOptions;
        //            var _with29 = crExportOptions;
        //            _with29.DestinationOptions = crDiskFileDestinationOptions;
        //            _with29.ExportDestinationType = ExportDestinationType.DiskFile;
        //            if (FormatType == "PDF")
        //            {
        //                _with29.ExportFormatType = ExportFormatType.PortableDocFormat;
        //            }
        //            rptDoc.Export();
        //            rptDoc.Close();
        //            rptDoc.Dispose();
        //            DocName = Fname;
        //            Response.ClearContent();
        //            Response.ClearHeaders();
        //        }
        //    }
        //    public string Get_Document_Name(int DocPK)
        //    {
        //        string strSql = null;
        //        string strDocName = null;
        //        strSql = "select doc.document_name from qcor_mc_m_documents doc where doc.document_mst_pk =" + DocPK;
        //        strDocName = (new WorkFlow()).ExecuteScaler(strSql);
        //        return strDocName;
        //    }
        //    #endregion
        //    ///''''''''''''''''''''''''''''''''
        //    public string fn_GetProduct_by_DBUser(string DB_UserID)
        //    {
        //        string strSQL = null;
        //        strSQL = Strings.UCase(ConfigurationManager.AppSettings["ProductID"]);

        //        ///strSQL = "select p.qproduct_id from qcor_gen_m_qproduct p where p.db_user_id ='" & DB_UserID & "' "

        //        try
        //        {
        //            return strSQL;

        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #region "Footer Text Format In Grid"         'Snigdharani - 27/05/2009
        //    public object FooterTextFormat(string _Value, Infragistics.WebUI.UltraWebGrid.UltraWebGrid _UltraWebGrid, string _Columns)
        //    {
        //        Array ArrColumns = null;
        //        Array ArrValue = null;
        //        int i = 0;
        //        string str_val = null;
        //        string[] str_val1 = null;
        //        ArrColumns = _Columns.Split("~");
        //        ArrValue = _Value.Split("~");
        //        if (ArrColumns.Length > 0)
        //        {
        //            for (i = 0; i <= ArrColumns.Length - 1; i++)
        //            {
        //                if (Strings.InStr(Convert.ToString(ArrValue(i)), ".") == false)
        //                {
        //                    _UltraWebGrid.Columns[ArrColumns(i)].FooterText = (Information.IsDBNull(ArrValue(i)) ? "0.000" : Convert.ToString(ArrValue(i)) + ".000");
        //                }
        //                else
        //                {
        //                    str_val1 = Convert.ToString(ArrValue(i)).Split(".");
        //                    str_val = str_val1[1].Length;
        //                    if (str_val == 1)
        //                    {
        //                        _UltraWebGrid.Columns[ArrColumns(i)].FooterText = (Information.IsDBNull(ArrValue(i)) ? "0.000" : Convert.ToString(ArrValue(i)) + "00");
        //                    }
        //                    else if (str_val == 2)
        //                    {
        //                        _UltraWebGrid.Columns[ArrColumns(i)].FooterText = (Information.IsDBNull(ArrValue(i)) ? "0.000" : Convert.ToString(ArrValue(i)) + "0");
        //                    }
        //                    else
        //                    {
        //                        _UltraWebGrid.Columns[ArrColumns(i)].FooterText = (Information.IsDBNull(ArrValue(i)) ? "0.000" : Convert.ToDecimal(ArrValue(i)).Round(ArrValue(i), 3));
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    #endregion

        //    #region "For Access Right"
        //    public void fn_Check_AccessRights(ref Page MyPage, string ConfigurationID, Int32 nAccessRights, string Mode, ref object btnSearch, ref object btnAdd, ref object btnSave, ref object btnDelete, bool SAVEFLAG_FOR_ADDMODE = false)
        //    {

        //        try
        //        {
        //            if (Session["CHECK_RIGHTS"] == "TRUE")
        //            {
        //                string VIEW = null;
        //                string ADD = null;
        //                string MODIFY = null;
        //                string DEL = null;
        //                nAccessRights = ViewState["nAccessRights"];
        //                ADD = (Convert.ToBoolean(ACCESSRIGHTS.Add & nAccessRights) ? "Y" : "N");
        //                MODIFY = (Convert.ToBoolean(ACCESSRIGHTS.Edit & nAccessRights) ? "Y" : "N");
        //                VIEW = (Convert.ToBoolean(ACCESSRIGHTS.View & nAccessRights) ? "Y" : "N");
        //                DEL = (Convert.ToBoolean(ACCESSRIGHTS.Delete & nAccessRights) ? "Y" : "N");
        //                /// FIRSTLY DISABLING ALL  4 BUTTONS
        //                /// If Not IsNothing(btnSearch) Then CType(btnSearch, System.Web.UI.WebControls.Button).Enabled = False
        //                if ((btnAdd != null))
        //                    ((System.Web.UI.WebControls.Button)btnAdd).Enabled = false;
        //                if ((btnSave != null))
        //                    ((System.Web.UI.WebControls.Button)btnSave).Enabled = false;
        //                if ((btnDelete != null))
        //                    ((System.Web.UI.WebControls.Button)btnDelete).Enabled = false;

        //                /// DISABLING EXTRA RIGHTS BUTTONS  BASED ON CONFIG AND USER ACCESS MORE RIGHTS
        //                if (Session["CHECK_EXTRA_RIGHTS"] == "TRUE")
        //                {
        //                    fn_Disable_Enable_ExtraRights(ref MyPage, ConfigurationID, Session["USER_PK"]);
        //                }

        //                /// If Not IsNothing(btnSearch) Then If VIEW = "Y" Then CType(btnSearch, System.Web.UI.WebControls.Button).Enabled = True
        //                if ((btnDelete != null))
        //                    if (DEL == "Y")
        //                        ((System.Web.UI.WebControls.Button)btnDelete).Enabled = true;
        //                if ((btnSave != null))
        //                    if (MODIFY == "Y")
        //                        if (Mode == "UPDATE" | Mode == "UNCHANGE")
        //                            ((System.Web.UI.WebControls.Button)btnSave).Enabled = true;


        //                if (ADD == "Y")
        //                {
        //                    //If Mode = "INSERT" Then
        //                    if ((btnAdd != null))
        //                        ((System.Web.UI.WebControls.Button)btnAdd).Enabled = true;
        //                    if ((btnSave != null))
        //                        ((System.Web.UI.WebControls.Button)btnSave).Enabled = true;
        //                    //End If
        //                }


        //                if (SAVEFLAG_FOR_ADDMODE == true)
        //                {
        //                    if (Mode == "INSERT" & ADD == "Y" & MODIFY == "N")
        //                    {
        //                    }
        //                    else
        //                    {
        //                        if (Mode == "UPDATE" & ADD == "Y" & MODIFY == "N")
        //                        {
        //                            throw new Exception("No Modify Rights");
        //                        }
        //                    }
        //                }
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    public object fn_Disable_Enable_ExtraRights(ref Page MyPage, string ConfigurationID, Int32 UserPK)
        //    {
        //        try
        //        {
        //            DataSet ds_ExtraRights = null;
        //            Int32 ds_counter = default(Int32);
        //            string strFilter = null;
        //            DataRow[] dr = null;

        //            ds_ExtraRights = fn_Get_ExtraRights_Controls(ConfigurationID, UserPK);
        //            if ((ds_ExtraRights != null))
        //            {
        //                if (ds_ExtraRights.Tables[0].Rows.Count > 0)
        //                {
        //                    Control objCtrl = null;
        //                    for (int mainCtlCount = 0; mainCtlCount <= MyPage.Controls.Count; mainCtlCount++)
        //                    {
        //                        if (MyPage.Controls[mainCtlCount] is System.Web.UI.HtmlControls.HtmlForm)
        //                        {
        //                            foreach (DataRow rowCtl in ds_ExtraRights.Tables[0].Rows)
        //                            {
        //                                short ActiveFlag = rowCtl["ACTIVE_FLAG"];
        //                                objCtrl = MyPage.Controls[mainCtlCount].FindControl(rowCtl["CONTROL_ID"]);
        //                                if ((objCtrl == null))
        //                                    continue;
        //                                if (objCtrl is System.Web.UI.WebControls.Button)
        //                                {
        //                                    if (ActiveFlag == 1)
        //                                    {
        //                                        ((System.Web.UI.WebControls.Button)objCtrl).Enabled = true;
        //                                    }
        //                                    else if (ActiveFlag == 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.Button)objCtrl).Enabled = false;
        //                                    }
        //                                }
        //                                else if (objCtrl is Infragistics.WebUI.WebSchedule.WebDateChooser)
        //                                {
        //                                    if (ActiveFlag == 1)
        //                                    {
        //                                        ((Infragistics.WebUI.WebSchedule.WebDateChooser)objCtrl).Enabled = true;
        //                                    }
        //                                    else if (ActiveFlag == 0)
        //                                    {
        //                                        ((Infragistics.WebUI.WebSchedule.WebDateChooser)objCtrl).Enabled = false;
        //                                    }
        //                                }
        //                                else if (objCtrl is System.Web.UI.WebControls.Label)
        //                                {
        //                                    if (ActiveFlag == 1)
        //                                    {
        //                                        ((System.Web.UI.WebControls.Label)objCtrl).Text = 1;
        //                                    }
        //                                    else if (ActiveFlag == 0)
        //                                    {
        //                                        ((System.Web.UI.WebControls.Label)objCtrl).Text = 0;
        //                                    }
        //                                }
        //                            }
        //                            break; // TODO: might not be correct. Was : Exit For
        //                        }
        //                    }
        //                }
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    public DataSet fn_Get_ExtraRights_Controls(string ConfigID, Int32 UserPK)
        //    {

        //        try
        //        {
        //            WorkFlow objWK = new WorkFlow();
        //            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //            sb.Append(" SELECT CR.CONTROL_ID, CR.CONTROL_TYPE,  1  ACTIVE_FLAG");
        //            sb.Append("  FROM CONFIG_RIGHTS_TBL CR, USER_ACCESS_RIGHTS_TRN UR, USER_ACCESS_TRN UA");
        //            sb.Append(" WHERE CR.CONFIG_RIGHTS_PK = UR.CONFIG_RIGHTS_FK");
        //            sb.Append("   AND UR.USER_ACCESS_FK = UA.USER_ACCESS_PK");
        //            sb.Append("   AND CR.CONFIG_ID_FK = (SELECT C.PARENT_CONFIG_ID_FK FROM CONFIG_MST_TBL C WHERE C.CONFIG_ID = '" + ConfigID + "')");
        //            sb.Append(" AND UR.ACTIVE_FLAG=1");
        //            sb.Append("   AND UA.USER_MST_FK = " + UserPK + "");
        //            sb.Append(" UNION ALL ");
        //            sb.Append(" SELECT CR.CONTROL_ID, CR.CONTROL_TYPE,  0  ACTIVE_FLAG");
        //            sb.Append("  FROM CONFIG_RIGHTS_TBL CR");
        //            sb.Append(" WHERE CR.CONFIG_ID_FK = (SELECT C.PARENT_CONFIG_ID_FK FROM CONFIG_MST_TBL C WHERE C.CONFIG_ID = '" + ConfigID + "')");
        //            sb.Append("   AND CR.CONFIG_RIGHTS_PK NOT IN");
        //            sb.Append("       (SELECT UR.CONFIG_RIGHTS_FK");
        //            sb.Append("          FROM USER_ACCESS_RIGHTS_TRN UR, USER_ACCESS_TRN UA");
        //            sb.Append("         WHERE UR.USER_ACCESS_FK = UA.USER_ACCESS_PK");
        //            sb.Append(" AND UR.ACTIVE_FLAG=1");
        //            sb.Append("           AND UA.USER_MST_FK = " + UserPK + ")");

        //            return objWK.GetDataSet(sb.ToString());
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    #endregion

        //    #region "Fetch Commodity Name As per Given Commodity PKs"
        //    //Added By Snigdharani - 30/12/2009
        //    public DataSet FetchCommName(string CommPks)
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(1000);
        //        DataSet CommDs = new DataSet();
        //        WorkFlow objWF = new WorkFlow();
        //        sb.Append("SELECT DISTINCT COMM.COMMODITY_NAME");
        //        sb.Append("  FROM COMMODITY_MST_TBL COMM");
        //        sb.Append(" WHERE COMM.COMMODITY_MST_PK IN (" + CommPks + ")");
        //        CommDs = objWF.GetDataSet(sb.ToString());
        //        return CommDs;
        //    }
        //    #endregion

        //    #region "Populate Sort Type"
        //    public void PopulateSortType(ref DropDownList DDL, string ConfigID = "QFORCOMMON", string DDFlag = "")
        //    {
        //        DataSet ds = new DataSet();
        //        WorkFlow objWF = new WorkFlow();
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        try
        //        {
        //            sb.Append("SELECT DD.DD_VALUE, DD.DD_ID");
        //            sb.Append("  FROM QFOR_DROP_DOWN_TBL DD");
        //            sb.Append(" WHERE DD.DD_FLAG =  '" + DDFlag + "'");
        //            sb.Append("   AND DD.CONFIG_ID = '" + ConfigID + "'");
        //            sb.Append(" ORDER BY DD.DD_VALUE");
        //            ds = objWF.GetDataSet(sb.ToString());

        //            DDL.DataSource = ds;
        //            DDL.DataTextField = "DD_ID";
        //            DDL.DataValueField = "DD_VALUE";
        //            DDL.DataBind();
        //            DDL.SelectedValue = 1;

        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //    }
        //    #endregion

        //    #region "Populate Cargo Type"
        //    public void PopulateCargoType(ref DropDownList DDL, string ConfigID = "QFORCOMMON")
        //    {
        //        DataSet ds = new DataSet();
        //        WorkFlow objWF = new WorkFlow();
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

        //        //DDL.Items.Clear()
        //        //DDL.Items.Add("All")
        //        //DDL.Items.Add("FCL")
        //        //DDL.Items.Add("LCL")
        //        //DDL.Items.Add("BBC")
        //        //DDL.Items(0).Value = 0
        //        //DDL.Items(1).Value = 1
        //        //DDL.Items(2).Value = 2
        //        //DDL.Items(3).Value = 4
        //        //DDL.SelectedIndex = 0


        //        try
        //        {
        //            sb.Append("SELECT DD.DD_VALUE, DD.DD_ID");
        //            sb.Append("  FROM QFOR_DROP_DOWN_TBL DD");
        //            sb.Append(" WHERE DD.DD_FLAG = 'CARGO_TYPE'");
        //            sb.Append("   AND DD.CONFIG_ID = '" + ConfigID + "'");
        //            sb.Append(" ORDER BY DD.DD_VALUE");
        //            ds = objWF.GetDataSet(sb.ToString());

        //            DDL.DataSource = ds;
        //            DDL.DataTextField = "DD_ID";
        //            DDL.DataValueField = "DD_VALUE";
        //            DDL.DataBind();
        //            DDL.SelectedValue = 0;

        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //    }
        //    #endregion

        //    #region "Populate Status"
        //    public void PopulateCommonStatus(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        DDL.Items.Add("All");
        //        DDL.Items.Add("Requested");
        //        DDL.Items.Add("Approved");
        //        DDL.Items.Add("Rejected");
        //        DDL.Items[0].Value = 3;
        //        DDL.Items[1].Value = 0;
        //        DDL.Items[2].Value = 1;
        //        DDL.Items[3].Value = 2;
        //        DDL.SelectedIndex = 0;
        //    }
        //    #endregion

        //    #region "Populate Status"
        //    public int CheckWorkFlowPreference(ref string DocID)
        //    {
        //        WorkFlow objWF = new WorkFlow();
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

        //        sb.Append("SELECT COUNT(*)");
        //        sb.Append("  FROM DOCUMENT_PREF_LOC_MST_TBL D, DOCUMENT_PREFERENCE_MST_TBL P");
        //        sb.Append(" WHERE D.DOC_PREFERENCE_FK = P.DOCUMENT_PREFERENCE_MST_PK");
        //        sb.Append("   AND P.DOCUMENT_PREFERENCE_NAME = '" + DocID + "' ");
        //        sb.Append("   AND D.LOCATION_MST_FK = " + Session["LOGED_IN_LOC_FK"]);
        //        return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
        //    }
        //    #endregion

        //    #region "Biz Type as SEA/AIR - default is SEA"
        //    public virtual void FillBizType(ref DropDownList DDL)
        //    {
        //        DDL.Items.Clear();
        //        if (CurrentBusinessType == BusinessType.Air)
        //        {
        //            DDL.Items.Add("Air");
        //            DDL.Items[0].Value = 1;
        //            DDL.SelectedIndex = 0;
        //        }
        //        else if (CurrentBusinessType == BusinessType.Sea)
        //        {
        //            DDL.Items.Add("Sea");
        //            DDL.Items[0].Value = 2;
        //            DDL.SelectedIndex = 0;
        //        }
        //        else
        //        {
        //            DDL.Items.Add("Air");
        //            DDL.Items.Add("Sea");
        //            DDL.Items[0].Value = 1;
        //            DDL.Items[1].Value = 2;
        //            DDL.SelectedIndex = 1;
        //        }
        //    }
        //    #endregion

        //    #region "For Fetching DropDown Values From DataBase"
        //    public static DataSet FetchDropDownValues(string Flag = "", string ConfigID = "")
        //    {
        //        WorkFlow objWF = new WorkFlow();
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        string ErrorMessage = null;
        //        sb.Append("SELECT T.DD_VALUE, T.DD_ID");
        //        sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
        //        sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
        //        sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
        //        sb.Append("    ORDER BY T.DD_VALUE ");
        //        try
        //        {
        //            return objWF.GetDataSet(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            ErrorMessage = sqlExp.Message;
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            ErrorMessage = exp.Message;
        //            throw exp;
        //        }
        //    }
        //    public static DataSet FetchDropDownValuesForIntValues(string Flag = "", string ConfigID = "")
        //    {
        //        //Please note that if you are using this function then all values in DD_VALUE column in dropdown table must have number type values
        //        WorkFlow objWF = new WorkFlow();
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        string ErrorMessage = null;
        //        sb.Append("SELECT T.DD_VALUE, T.DD_ID");
        //        sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
        //        sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
        //        sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
        //        sb.Append("    ORDER BY TO_NUMBER(NVL(T.DD_VALUE,0)) ");
        //        try
        //        {
        //            return objWF.GetDataSet(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            ErrorMessage = sqlExp.Message;
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            ErrorMessage = exp.Message;
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    #region "Fetch Configured Charges based on the Sector"
        //    public static DataSet FetchConfigCharges(string POL_PK, string POD_PK, string BizType = "0", string Flag = "", bool isPort = true)
        //    {
        //        WorkFlow objWF = new WorkFlow();
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        string ErrorMessage = null;
        //        sb.Append("SELECT * ");
        //        sb.Append("  FROM (SELECT FMT.FREIGHT_ELEMENT_MST_PK, FMT.FREIGHT_ELEMENT_ID");

        //        if (Flag == "FRT")
        //        {
        //            sb.Append("            ,NVL(FCT.VATOS_FLAG, 0) VATOS_FLAG , ");
        //            sb.Append("DECODE(NVL(FCT.BASIS,FMT.CHARGE_BASIS),");
        //            sb.Append("              '0',");
        //            sb.Append("              '',");
        //            sb.Append("              '1',");
        //            sb.Append("              '%',");
        //            sb.Append("              '2',");
        //            sb.Append("              'Flat Rate',");
        //            sb.Append("              '3',");
        //            sb.Append("              'Kgs',");
        //            sb.Append("              '4',");
        //            sb.Append("              'Unit') CHARGE_BASIS");
        //        }
        //        sb.Append("          FROM FREIGHT_ELEMENT_MST_TBL FMT,");
        //        sb.Append("               FREIGHT_CONFIG_TRN_TBL  FCT,");
        //        sb.Append("               SECTOR_MST_TBL          SMT, PARAMETERS_TBL PTT ");
        //        sb.Append("         WHERE FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
        //        sb.Append("           AND SMT.SECTOR_MST_PK = FCT.SECTOR_MST_FK");
        //        sb.Append("           AND FMT.BUSINESS_TYPE IN(3," + BizType + ") ");
        //        sb.Append("           AND FCT.FREIGHT_TYPE <> 3");
        //        sb.Append("           AND FMT.FREIGHT_ELEMENT_MST_PK <> NVL(PTT.FRT_AIF_FK,0) ");
        //        //AIF should not display in the Charge List
        //        sb.Append("           AND SMT.SECTOR_MST_PK IN");
        //        sb.Append("               (SELECT S.SECTOR_MST_PK");
        //        sb.Append("                  FROM SECTOR_MST_TBL S");
        //        //sb.Append("                 WHERE S.FROM_PORT_FK = " & POL_PK)
        //        //sb.Append("                   AND S.TO_PORT_FK =  " & POD_PK)
        //        if (isPort == true)
        //        {
        //            sb.Append("                 WHERE S.FROM_PORT_FK IN (" + POL_PK + ")");
        //            sb.Append("                   AND S.TO_PORT_FK IN (" + POD_PK + ")");
        //        }
        //        else
        //        {
        //            sb.Append("                 WHERE S.FROM_PORT_FK IN (select t.port_mst_fk from port_grp_trn_tbl t where t.port_grp_mst_fk in (" + POL_PK + "))");
        //            sb.Append("                   AND S.TO_PORT_FK IN (select t.port_mst_fk from port_grp_trn_tbl t where t.port_grp_mst_fk in (" + POD_PK + "))");
        //        }
        //        sb.Append("        ) ");
        //        if (Flag != "FRT")
        //        {
        //            sb.Append("         UNION");
        //            sb.Append("        SELECT DISTINCT FMT.FREIGHT_ELEMENT_MST_PK, FMT.FREIGHT_ELEMENT_ID");
        //            sb.Append("          FROM FREIGHT_ELEMENT_MST_TBL FMT, FREIGHT_CONFIG_TRN_TBL FCT, PARAMETERS_TBL PTT ");
        //            sb.Append("         WHERE FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
        //            sb.Append("           AND FCT.FREIGHT_TYPE = 3");
        //            sb.Append("           AND FMT.FREIGHT_ELEMENT_MST_PK <> NVL(PTT.FRT_AIF_FK,0) ");
        //            //AIF should not display in the Charge List
        //            //sb.Append("           AND FCT.PORT_MST_FK = " & POD_PK)
        //            if (isPort == true)
        //            {
        //                sb.Append("           AND FCT.PORT_MST_FK IN (" + POD_PK + ")");
        //            }
        //            else
        //            {
        //                sb.Append("           AND FCT.PORT_MST_FK IN (select t.port_mst_fk from port_grp_trn_tbl t where t.port_grp_mst_fk in (" + POD_PK + "))");
        //            }
        //        }
        //        sb.Append("           ) ");
        //        try
        //        {
        //            return objWF.GetDataSet(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            ErrorMessage = sqlExp.Message;
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            ErrorMessage = exp.Message;
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    #region "Common settings for Grid" 'Added by Ashish on 8th Oct 2012
        //    public void CommonGridSettings(ref Infragistics.WebUI.UltraWebGrid.UltraWebGrid UWGSecSer)
        //    {
        //        var _with31 = UWGSecSer;
        //        var _with32 = _with31.DisplayLayout;
        //        _with32.AllowSortingDefault = Infragistics.WebUI.UltraWebGrid.AllowSorting.No;
        //        _with32.ViewType = Infragistics.WebUI.UltraWebGrid.ViewType.Hierarchical;
        //        _with32.AllowAddNewDefault = Infragistics.WebUI.UltraWebGrid.AllowAddNew.Yes;
        //        _with32.AllowUpdateDefault = Infragistics.WebUI.UltraWebGrid.AllowUpdate.No;
        //        _with32.AllowDeleteDefault = Infragistics.WebUI.UltraWebGrid.AllowDelete.Yes;
        //        _with32.AllowColSizingDefault = Infragistics.WebUI.UltraWebGrid.AllowSizing.Free;
        //        _with32.ScrollBar = Infragistics.WebUI.UltraWebGrid.ScrollBar.Auto;
        //        _with32.ScrollBarView = Infragistics.WebUI.UltraWebGrid.ScrollBarView.Horizontal;
        //        _with32.CellClickActionDefault = Infragistics.WebUI.UltraWebGrid.CellClickAction.Edit;
        //        _with32.RowHeightDefault = Unit.Pixel(strGridRowHeight);
        //        _with32.HeaderStyleDefault.Height = Unit.Pixel(strGridRowHeight);
        //        _with32.StationaryMargins = Infragistics.WebUI.UltraWebGrid.StationaryMargins.Header;
        //        _with32.RowStyleDefault.Wrap = false;
        //        foreach (Infragistics.WebUI.UltraWebGrid.UltraGridBand band in _with31.Bands)
        //        {
        //            var _with33 = band;
        //            _with33.HeaderStyle.Height = Unit.Pixel(strGridRowHeight);
        //            _with33.AllowColSizing = Infragistics.WebUI.UltraWebGrid.AllowSizing.Free;
        //            _with33.HeaderStyle.CssClass = "HeaderClass";
        //            _with33.RowAlternateStyle.CssClass = "AltrowClass";
        //            _with33.FooterStyle.CssClass = "FooterClass";
        //            _with33.SelectedRowStyle.CssClass = "SelcellClass";
        //            _with33.RowStyle.CssClass = "ItemClass";
        //            _with33.RowSelectorStyle.CssClass = "RowSelectorClass";
        //            _with33.RowSelectors = Infragistics.WebUI.UltraWebGrid.RowSelectors.No;
        //            _with33.AllowColumnMoving = Infragistics.WebUI.UltraWebGrid.AllowColumnMoving.None;
        //        }
        //    }

        //    public void BindGridHeader(ref Infragistics.WebUI.UltraWebGrid.UltraWebGrid UWGTEMP, Int16 BandIndex, string[] _ENUM, string ENUM_ID = "", string FIELD_TYPE = "HD")
        //    {
        //        DataSet dsgridHdr = null;
        //        dsgridHdr = FetchGridHeader(this, this.ConfigurationID, FIELD_TYPE, 0, ENUM_ID);
        //        var _with34 = UWGTEMP;
        //        if (string.IsNullOrEmpty(ENUM_ID) | _ENUM.Length == 0)
        //        {
        //        }
        //        else
        //        {
        //            try
        //            {
        //                Int32 i = default(Int32);
        //                for (i = 0; i <= _ENUM.Length - 1; i++)
        //                {
        //                    string M_CtrlID = "";
        //                    string M_ToolTip = "";
        //                    string M_TEXT = "";
        //                    short M_Settings = 0;
        //                    short M_ColumnAlign = 1;
        //                    Int32 M_Width = 0;
        //                    Int32 M_ColumnMaxLen = 35;

        //                    string colname = _ENUM.GetValue(i);
        //                    M_CtrlID = ENUM_ID + "." + colname;
        //                    DataRow gh = null;
        //                    if (dsgridHdr.Tables[0].Select("CONFIG_CTRL_ID='" + M_CtrlID + "'").Length > 0)
        //                    {
        //                        gh = dsgridHdr.Tables[0].Select("CONFIG_CTRL_ID='" + M_CtrlID + "'")[0];
        //                        M_TEXT = getDefault(gh["CONFIG_CTRL_DEF_TEXT"], "");
        //                        M_ToolTip = getDefault(gh["CONFIG_CONTROL_TOOLTIP"], "");
        //                        M_Settings = getDefault(gh["CONFIG_CTRL_MIN_LENGTH"], 0);
        //                        M_ColumnAlign = getDefault(gh["CONFIG_GRID_COLUMN_ALIGN"], 1);
        //                        M_Width = getDefault(gh["CONFIG_CTRL_MAX_LENGTH"], 0);
        //                        M_ColumnMaxLen = getDefault(gh["CONFIG_GRID_COL_MAX_LENGTH"], 35);
        //                    }
        //                    //For Each gh As DataRow In dsgridHdr.Tables(0).Rows
        //                    //    If M_CtrlID.ToUpper = CStr(gh("CONFIG_CTRL_ID")).ToUpper Then

        //                    //        Exit For
        //                    //    End If
        //                    //Next
        //                    var _with35 = _with34.Bands(BandIndex);
        //                    if (!string.IsNullOrEmpty(M_TEXT))
        //                    {
        //                        _with35.Columns(i).HeaderText = "<span Title='" + M_ToolTip + "'>" + M_TEXT + (M_Settings == 1 ? "<span style='COLOR:red;' > *</span>" : "") + "</span>";
        //                    }
        //                    if (M_Settings == 2)
        //                        _with35.Columns(i).Hidden = true;
        //                    if (M_Width > 0)
        //                        _with35.Columns(i).Width = Unit.Percentage(M_Width);
        //                    if (M_ColumnAlign != 0)
        //                        _with35.Columns(i).CellStyle.HorizontalAlign = M_ColumnAlign;
        //                    if (M_ColumnMaxLen > 0)
        //                        _with35.Columns(i).FieldLen = M_ColumnMaxLen;
        //                    _with35.Columns(i).AllowUpdate = Infragistics.WebUI.UltraWebGrid.AllowUpdate.No;
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //            }
        //        }
        //    }
        //    #endregion

        //    #region " Fetch base currency Exchange rate export"
        //    public DataSet FetchROE(Int64 baseCurrency, string ROE_Date = "")
        //    {
        //        System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
        //        WorkFlow objWF = new WorkFlow();
        //        try
        //        {
        //            strSQL.Append("SELECT");
        //            strSQL.Append(" CURR.CURRENCY_MST_PK,");
        //            strSQL.Append(" CURR.CURRENCY_ID,");
        //            if (string.IsNullOrEmpty(ROE_Date))
        //            {
        //                strSQL.Append(" ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(sysdate - .5)),6) AS ROE ");
        //            }
        //            else
        //            {
        //                strSQL.Append(" ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(TO_DATE('" + ROE_Date + "',DATEFORMAT) - .5)),6) AS ROE ");
        //            }
        //            strSQL.Append(" FROM CURRENCY_TYPE_MST_TBL CURR ");
        //            strSQL.Append("     WHERE CURR.ACTIVE_FLAG = 1 ");
        //            return objWF.GetDataSet(strSQL.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    public DataSet FetchROE_BUY(Int64 baseCurrency, string ROE_Date = "")
        //    {
        //        System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
        //        WorkFlow objWF = new WorkFlow();
        //        try
        //        {
        //            strSQL.Append("SELECT");
        //            strSQL.Append(" CURR.CURRENCY_MST_PK,");
        //            strSQL.Append(" CURR.CURRENCY_ID,");
        //            if (string.IsNullOrEmpty(ROE_Date))
        //            {
        //                strSQL.Append("  ROUND(GET_EX_RATE_BUY(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(sysdate - .5)),6) AS ROE ");
        //            }
        //            else
        //            {
        //                strSQL.Append("  ROUND(GET_EX_RATE_BUY(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(TO_DATE('" + ROE_Date + "',DATEFORMAT) - .5)),6) AS ROE ");
        //            }
        //            strSQL.Append(" FROM CURRENCY_TYPE_MST_TBL CURR ");
        //            strSQL.Append("     WHERE CURR.ACTIVE_FLAG = 1 ");
        //            return objWF.GetDataSet(strSQL.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    #region "FetchControl"
        //    public virtual Int32 FetchControls(string ConfigurationID, string P_Flag = "")
        //    {
        //        string strSQL = null;
        //        DataSet ds = new DataSet();
        //        WorkFlow objWK = new WorkFlow();
        //        strSQL = strSQL + Constants.vbCrLf + " SELECT ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CONTROLS_PK, ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONMST.CONFIG_MST_PK, ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_ID, ";
        //        //Added by Faheem
        //        if (Session["USER_PREF_LANG"] == "CN-CN")
        //        {
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_DEF_TEXT_CN CONFIG_CTRL_DEF_TEXT, ";
        //        }
        //        else
        //        {
        //            strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_DEF_TEXT, ";
        //        }
        //        strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CONTROL_TOOLTIP, ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_MIN_LENGTH, ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_MAX_LENGTH, ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_CTRL_FIELD_TYPE, ";
        //        strSQL = strSQL + Constants.vbCrLf + " NVL(CONTXT.CONFIG_CONTROL_TEXT,' ') CONFIG_TEXT, ";
        //        strSQL = strSQL + Constants.vbCrLf + " NVL(CONTXT.CONFIG_CONTROL_TOOLTIP,' ') CONFIG_TOOLTIP, ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_GRID_COLUMN_ALIGN, CONCTL.CONFIG_GRID_COL_MAX_LENGTH";
        //        strSQL = strSQL + Constants.vbCrLf + " FROM ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONFIG_CONTROLS_TBL CONCTL, ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONFIG_MST_TBL CONMST, ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONFIG_CONTROLS_TEXT_TRN CONTXT ";
        //        strSQL = strSQL + Constants.vbCrLf + " WHERE ";
        //        strSQL = strSQL + Constants.vbCrLf + " CONCTL.CONFIG_MST_FK = CONMST.CONFIG_MST_PK ";
        //        strSQL = strSQL + Constants.vbCrLf + " AND CONTXT.CONFIG_CONTROLS_FK(+) = CONCTL.CONFIG_CONTROLS_PK ";
        //        strSQL = strSQL + Constants.vbCrLf + " AND CONMST.CONFIG_ID='" + ConfigurationID + "' ";
        //        strSQL = strSQL + Constants.vbCrLf + " AND CONTXT.ENVIRONMENT_TBL_FK(+) = " + (Int64)Session["ENVIRONMENT_PK"] + " ";
        //        strSQL = strSQL + Constants.vbCrLf + " AND CONFIG_CTRL_FIELD_TYPE like 'HD'";
        //        ds = objWK.GetDataSet(strSQL);

        //        Int32 I = default(Int32);
        //        for (I = 0; I <= ds.Tables[0].Rows.Count - 1; I++)
        //        {
        //            GridHeaderSetting objGridHeader = new GridHeaderSetting();
        //            if (ds.Tables[0].Rows[I]["CONFIG_TEXT"] == " ")
        //            {
        //                objGridHeader.M_TEXT = ds.Tables[0].Rows[I]["CONFIG_CTRL_DEF_TEXT"] + "";
        //            }
        //            else
        //            {
        //                objGridHeader.M_TEXT = ds.Tables[0].Rows[I]["CONFIG_TEXT"] + "";
        //            }
        //            if (ds.Tables[0].Rows[I]["CONFIG_TOOLTIP"] == " ")
        //            {
        //                objGridHeader.M_ToolTip = ds.Tables[0].Rows[I]["CONFIG_CONTROL_TOOLTIP"] + "";
        //            }
        //            else
        //            {
        //                objGridHeader.M_ToolTip = ds.Tables[0].Rows[I]["CONFIG_TOOLTIP"] + "";
        //            }
        //            objGridHeader.M_Width = ds.Tables[0].Rows[I]["CONFIG_CTRL_MAX_LENGTH"] + "";
        //            objGridHeader.M_Settings = ds.Tables[0].Rows[I]["CONFIG_CTRL_MIN_LENGTH"] + "";
        //            objGridHeader.M_CtrlID = ds.Tables[0].Rows[I]["CONFIG_CTRL_ID"] + "";
        //            objGridHeader.M_ColumnAlign = ds.Tables[0].Rows[I]["CONFIG_GRID_COLUMN_ALIGN"] + "";
        //            objGridHeader.M_ColumnFieldLength = ds.Tables[0].Rows[I]["CONFIG_GRID_COL_MAX_LENGTH"] + "";
        //            GridHeaders.Add(objGridHeader, objGridHeader.M_CtrlID);
        //        }
        //    }
        //    #endregion

        //    #region "Load Report Failed"
        //    public void PrintInAcrobatNew(ReportDocument rptDoc, string filename, int StrSetPrint = 1, Int16 AdvFlag = 1)
        //    {
        //        string Fname = null;
        //        ExportOptions crExportOptions = null;
        //        DiskFileDestinationOptions crDiskFileDestinationOptions = null;
        //        rptDoc.PrintOptions.PaperSize = PaperSize.PaperA4;
        //        if (StrSetPrint == 1)
        //        {
        //            rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Portrait;
        //        }
        //        else
        //        {
        //            rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Landscape;
        //        }

        //        Fname = Server.MapPath("..\\..\\") + "Supports\\Files_Uploaded\\" + filename + ".pdf";
        //        crDiskFileDestinationOptions = new DiskFileDestinationOptions();
        //        crDiskFileDestinationOptions.DiskFileName = Fname;

        //        crExportOptions = rptDoc.ExportOptions;
        //        var _with36 = crExportOptions;
        //        _with36.DestinationOptions = crDiskFileDestinationOptions;
        //        _with36.ExportDestinationType = ExportDestinationType.DiskFile;
        //        _with36.ExportFormatType = ExportFormatType.PortableDocFormat;
        //        rptDoc.Export();
        //        rptDoc.Close();
        //        rptDoc.Dispose();
        //        if (AdvFlag == 2)
        //        {
        //            Response.ClearContent();
        //            Response.ClearHeaders();
        //            Response.ContentType = "application/pdf";
        //            Response.WriteFile(Fname);
        //            Response.Flush();
        //            Response.Close();
        //            System.IO.File.Delete(Fname);
        //        }
        //    }
        //    public void PrintInAcrobat(ReportDocument rptDoc, int StrSetPrint = 1, int Paper = 1)
        //    {
        //        System.Text.StringBuilder sbScript = new System.Text.StringBuilder();
        //        if (Paper == 1)
        //        {
        //            rptDoc.PrintOptions.PaperSize = PaperSize.PaperA4;
        //        }
        //        else
        //        {
        //            rptDoc.PrintOptions.PaperSize = PaperSize.PaperLetter;
        //        }

        //        if (StrSetPrint == 1)
        //        {
        //            rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Portrait;
        //        }
        //        else
        //        {
        //            rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Landscape;
        //        }

        //        System.IO.MemoryStream oStream = rptDoc.ExportToStream(ExportFormatType.PortableDocFormat);
        //        Response.ClearContent();
        //        Response.ClearHeaders();

        //        Response.ContentType = "application/pdf";
        //        Response.AddHeader("Content-Disposition", "inline; filename=Report.pdf");
        //        Response.BinaryWrite(oStream.ToArray());
        //        rptDoc.Close();
        //        rptDoc.Dispose();
        //        Response.End();
        //        Response.Flush();
        //        Response.Close();
        //    }

        //    public void PrintInAcrobat1(ReportDocument rptDoc, int StrSetPrint = 1)
        //    {
        //        System.Text.StringBuilder sbScript = new System.Text.StringBuilder();
        //        string Fname = null;
        //        ExportOptions crExportOptions = null;
        //        DiskFileDestinationOptions crDiskFileDestinationOptions = null;
        //        try
        //        {
        //            rptDoc.PrintOptions.PaperSize = PaperSize.PaperA4;
        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //        if (StrSetPrint == 1)
        //        {
        //            rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Portrait;
        //        }
        //        else
        //        {
        //            rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Landscape;
        //        }
        //        Fname = Server.MapPath(".") + "\\" + Session.SessionID.ToString() + ".pdf";
        //        crDiskFileDestinationOptions = new DiskFileDestinationOptions();
        //        crDiskFileDestinationOptions.DiskFileName = Fname;
        //        crExportOptions = rptDoc.ExportOptions;
        //        var _with37 = crExportOptions;
        //        _with37.DestinationOptions = crDiskFileDestinationOptions;
        //        _with37.ExportDestinationType = ExportDestinationType.DiskFile;
        //        _with37.ExportFormatType = ExportFormatType.PortableDocFormat;
        //        rptDoc.Export();
        //        rptDoc.Close();
        //        rptDoc.Dispose();
        //        Response.ClearHeaders();
        //        Response.ClearContent();
        //        Response.ContentType = "application/pdf";
        //        Response.WriteFile(Fname);
        //        Response.Flush();
        //        Response.Close();
        //        System.IO.File.Delete(Fname);
        //    }
        //    #endregion

        //    #region "Get Consignee/DP Agent Email"
        //    public DataTable GetCustomerEmail(long ConsigneeFk)
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWF = new WorkFlow();
        //        sb.Append("SELECT CCD.ADM_EMAIL_ID FROM CUSTOMER_CONTACT_DTLS CCD WHERE CCD.CUSTOMER_MST_FK = " + ConsigneeFk);
        //        try
        //        {
        //            return objWF.GetDataTable(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    public DataTable GetDpAgentEmail(long ConsigneeFk)
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWF = new WorkFlow();
        //        sb.Append("SELECT AG.ADM_EMAIL_ID FROM AGENT_CONTACT_DTLS AG WHERE AG.AGENT_MST_FK = " + ConsigneeFk);
        //        try
        //        {
        //            return objWF.GetDataTable(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    #region " exportExcelDetails "
        //    public void exportExcelDetails(DataSet dsData, DataSet dsCaption, string strSourceFileName, string strDestFileName)
        //    {
        //        Microsoft.Office.Interop.Excel.Application xlsApp = new Microsoft.Office.Interop.Excel.Application();
        //        Microsoft.Office.Interop.Excel.Workbook xlsWB = null;
        //        Microsoft.Office.Interop.Excel.Worksheet xlsSheet = null;
        //        Microsoft.Office.Interop.Excel.Range xlsRng = null;
        //        int rowNum = 0;
        //        int i = 0;

        //        try
        //        {
        //            if (dsData.Tables[0].Rows.Count > 0)
        //            {
        //                xlsWB = xlsApp.Workbooks.Open(strSourceFileName, , false);

        //                xlsSheet = xlsWB.ActiveSheet;
        //                xlsSheet.Range["A1", "A65536"].RowHeight = 17;
        //                xlsSheet.Range["A1", "IV65536"].Font.Name = "Arial";
        //                xlsSheet.Range["A1", "IV65536"].Font.Size = 8;
        //                xlsSheet.Application.DisplayAlerts = false;
        //                xlsSheet.Range["A1", "IV65536"].ColumnWidth = 15;
        //                xlsSheet.Range["A1", "IV65536"].Clear();

        //                ///' Corporate Details
        //                xlsSheet.Cells[2, 8].Value = Strings.UCase(dsData.Tables[0].Rows[0]["CORPORATE_NAME"] + "");
        //                xlsSheet.Cells[3, 8].Value = Strings.UCase(dsData.Tables[0].Rows[0]["CORPORATE_ADD1"] + "");
        //                xlsSheet.Cells[4, 8].Value = Strings.UCase(dsData.Tables[0].Rows[0]["CORPORATE_ADD2"] + "") + ", " + Strings.UCase(dsData.Tables[0].Rows[0]["CORPORATE_ADD3"] + "");
        //                xlsSheet.Cells[5, 8].Value = Strings.UCase(dsData.Tables[0].Rows[0]["CORPORATE_CITY"] + "") + "-" + Strings.UCase(dsData.Tables[0].Rows[0]["CORPORATE_ZIP"] + "");
        //                xlsSheet.Cells[6, 8].Value = dsCaption.Tables[0].Rows[0][0] + Strings.UCase(dsData.Tables[0].Rows[0]["CORPORATE_PHONE"] + "") + " Fax : " + Strings.UCase(dsData.Tables[0].Rows[0]["CORPORATE_FAX"] + "");
        //                xlsSheet.Cells[7, 8].Value = dsCaption.Tables[0].Rows[1][0] + Strings.UCase(dsData.Tables[0].Rows[0]["CORPORATE_EMAIL"] + "");

        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[2, 8], xlsSheet.Cells[7, 8]];
        //                xlsRng.Font.Bold = true;
        //                xlsRng.Font.Size = 10;
        //                xlsRng.HorizontalAlignment = Microsoft.Office.Interop.Excel.XlHAlign.xlHAlignLeft;
        //                xlsRng.VerticalAlignment = Microsoft.Office.Interop.Excel.XlVAlign.xlVAlignCenter;

        //                ///' Header
        //                xlsSheet.Cells[8, 1].Value = dsCaption.Tables[0].Rows[21][0];
        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[8, 1], xlsSheet.Cells[8, 10]];
        //                xlsRng.Font.Bold = true;
        //                xlsRng.Font.Underline = true;
        //                xlsRng.Font.Size = 11;
        //                xlsRng.HorizontalAlignment = Microsoft.Office.Interop.Excel.XlHAlign.xlHAlignCenter;
        //                xlsRng.VerticalAlignment = Microsoft.Office.Interop.Excel.XlVAlign.xlVAlignCenter;
        //                xlsRng.Merge();

        //                ///' Agent Details
        //                xlsSheet.Cells[9, 1].Value = dsCaption.Tables[0].Rows[2][0];
        //                xlsSheet.Cells[9, 2].Value = Strings.UCase(dsData.Tables[0].Rows[0]["AGENT_NAME"] + "");
        //                xlsSheet.Cells[10, 2].Value = Strings.UCase(dsData.Tables[0].Rows[0]["AGENT_ADD1"] + "");
        //                xlsSheet.Cells[11, 2].Value = Strings.UCase(dsData.Tables[0].Rows[0]["AGENT_ADD2"] + "") + ", " + Strings.UCase(dsData.Tables[0].Rows[0]["AGENT_ADD3"] + "");
        //                xlsSheet.Cells[12, 2].Value = Strings.UCase(dsData.Tables[0].Rows[0]["AGENT_CITY"] + "") + "-" + Strings.UCase(dsData.Tables[0].Rows[0]["AGENT_ZIP"] + "");
        //                xlsSheet.Cells[13, 2].Value = dsCaption.Tables[0].Rows[3][0] + Strings.UCase(dsData.Tables[0].Rows[0]["AGENT_PHONE_NO"] + "") + " Fax : " + Strings.UCase(dsData.Tables[0].Rows[0]["AGENT_FAX"] + "");
        //                xlsSheet.Cells[14, 2].Value = dsCaption.Tables[0].Rows[4][0] + Strings.UCase(dsData.Tables[0].Rows[0]["AGENT_EMAIL"] + "");

        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[9, 1], xlsSheet.Cells[14, 2]];
        //                xlsRng.Font.Bold = true;
        //                xlsRng.Font.Size = 10;
        //                xlsRng.HorizontalAlignment = Microsoft.Office.Interop.Excel.XlHAlign.xlHAlignLeft;
        //                xlsRng.VerticalAlignment = Microsoft.Office.Interop.Excel.XlVAlign.xlVAlignCenter;

        //                ///' Reference Nr Details
        //                xlsSheet.Cells[9, 7].Value = dsCaption.Tables[0].Rows[5][0];
        //                xlsSheet.Cells[9, 8].Value = Strings.UCase(dsData.Tables[0].Rows[0]["HBL_REF_NO"] + "");
        //                xlsSheet.Cells[10, 7].Value = dsCaption.Tables[0].Rows[6][0];
        //                xlsSheet.Cells[10, 8].Value = Strings.UCase(dsData.Tables[0].Rows[0]["BOOKING_REF_NO"] + "");

        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[9, 7], xlsSheet.Cells[10, 8]];
        //                xlsRng.Font.Bold = true;
        //                xlsRng.Font.Size = 10;
        //                xlsRng.HorizontalAlignment = Microsoft.Office.Interop.Excel.XlHAlign.xlHAlignLeft;
        //                xlsRng.VerticalAlignment = Microsoft.Office.Interop.Excel.XlVAlign.xlVAlignCenter;

        //                xlsSheet.Cells[16, 1].Value = dsCaption.Tables[0].Rows[7][0];

        //                ///' Shipper Consignee Details
        //                xlsSheet.Cells[17, 1].Value = dsCaption.Tables[0].Rows[8][0];
        //                xlsSheet.Cells[17, 2].Value = Strings.UCase(dsData.Tables[0].Rows[0]["SHIPPER_NAME"] + "");
        //                xlsSheet.Cells[18, 2].Value = Strings.UCase(dsData.Tables[0].Rows[0]["SHIPPER_ADD1"] + "");
        //                xlsSheet.Cells[19, 2].Value = Strings.UCase(dsData.Tables[0].Rows[0]["SHIPPER_ADD2"] + "") + ", " + Strings.UCase(dsData.Tables[0].Rows[0]["SHIPPER_ADD3"] + "");
        //                xlsSheet.Cells[20, 2].Value = dsCaption.Tables[0].Rows[9][0] + Strings.UCase(dsData.Tables[0].Rows[0]["SHIPPER_ZIP"] + "");
        //                xlsSheet.Cells[21, 2].Value = dsCaption.Tables[0].Rows[10][0] + Strings.UCase(dsData.Tables[0].Rows[0]["SHIPPER_PHONE_NO"] + "") + " Fax : " + Strings.UCase(dsData.Tables[0].Rows[0]["SHIPPER_FAX"] + "");
        //                xlsSheet.Cells[22, 2].Value = dsCaption.Tables[0].Rows[11][0] + Strings.UCase(dsData.Tables[0].Rows[0]["SHIPPER_EMAIL"] + "");

        //                xlsSheet.Cells[17, 7].Value = dsCaption.Tables[0].Rows[12][0];
        //                xlsSheet.Cells[17, 8].Value = Strings.UCase(dsData.Tables[0].Rows[0]["CONSIGNEE_NAME"] + "");
        //                xlsSheet.Cells[18, 8].Value = Strings.UCase(dsData.Tables[0].Rows[0]["CONSIGNEE_ADD1"] + "");
        //                xlsSheet.Cells[19, 8].Value = Strings.UCase(dsData.Tables[0].Rows[0]["CONSIGNEE_ADD2"] + "") + ", " + Strings.UCase(dsData.Tables[0].Rows[0]["CONSIGNEE_ADD3"] + "");
        //                xlsSheet.Cells[20, 8].Value = dsCaption.Tables[0].Rows[13][0] + Strings.UCase(dsData.Tables[0].Rows[0]["CONSIGNEE_ZIP"] + "");
        //                xlsSheet.Cells[21, 8].Value = dsCaption.Tables[0].Rows[14][0] + Strings.UCase(dsData.Tables[0].Rows[0]["CONSIGNEE_PHONE_NO"] + "") + " Fax : " + Strings.UCase(dsData.Tables[0].Rows[0]["CONSIGNEE_FAX"] + "");
        //                xlsSheet.Cells[22, 8].Value = dsCaption.Tables[0].Rows[15][0] + Strings.UCase(dsData.Tables[0].Rows[0]["CONSIGNEE_EMAIL"] + "");

        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[17, 1], xlsSheet.Cells[22, 8]];
        //                xlsRng.Font.Bold = true;
        //                xlsRng.Font.Size = 10;
        //                xlsRng.HorizontalAlignment = Microsoft.Office.Interop.Excel.XlHAlign.xlHAlignLeft;
        //                xlsRng.VerticalAlignment = Microsoft.Office.Interop.Excel.XlVAlign.xlVAlignCenter;

        //                ///' Schedule Details
        //                xlsSheet.Cells[24, 1].Value = dsCaption.Tables[0].Rows[16][0];
        //                xlsSheet.Cells[24, 2].Value = Strings.UCase(dsData.Tables[0].Rows[0]["POL_PORT"] + "");
        //                xlsSheet.Cells[24, 5].Value = dsCaption.Tables[0].Rows[17][0];
        //                xlsSheet.Cells[24, 6].Value = Strings.UCase(dsData.Tables[0].Rows[0]["ETD_DATE"] + "");
        //                xlsSheet.Cells[24, 9].Value = dsCaption.Tables[0].Rows[18][0];
        //                xlsSheet.Cells[24, 10].Value = Strings.UCase(dsData.Tables[0].Rows[0]["VESSEL_NAME"] + "") + "/" + Strings.UCase(dsData.Tables[0].Rows[0]["VOYAGE_FLIGHT_NO"] + "");

        //                xlsSheet.Cells[25, 1].Value = dsCaption.Tables[0].Rows[19][0];
        //                xlsSheet.Cells[25, 2].Value = Strings.UCase(dsData.Tables[0].Rows[0]["POD_PORT"] + "");
        //                xlsSheet.Cells[25, 5].Value = dsCaption.Tables[0].Rows[20][0];
        //                xlsSheet.Cells[25, 6].Value = Strings.UCase(dsData.Tables[0].Rows[0]["ETA_DATE"] + "");

        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[24, 1], xlsSheet.Cells[25, 1]];
        //                xlsRng.Font.Bold = true;
        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[24, 5], xlsSheet.Cells[25, 5]];
        //                xlsRng.Font.Bold = true;
        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[24, 9], xlsSheet.Cells[25, 9]];
        //                xlsRng.Font.Bold = true;

        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[24, 1], xlsSheet.Cells[25, 10]];
        //                xlsRng.Borders.LineStyle = LineStyle.SingleLine;

        //                rowNum = 27;

        //                /// Container Details Header Caption
        //                for (j = 0; j <= dsData.Tables[1].Columns.Count - 1; j++)
        //                {
        //                    xlsSheet.Cells[rowNum, j + 1].Value = dsCaption.Tables[0].Rows[j + 22][0];
        //                }
        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[rowNum, 1], xlsSheet.Cells[rowNum, dsData.Tables[1].Columns.Count]];
        //                xlsRng.Font.Bold = true;
        //                xlsRng.HorizontalAlignment = Microsoft.Office.Interop.Excel.XlHAlign.xlHAlignCenter;
        //                xlsRng.VerticalAlignment = Microsoft.Office.Interop.Excel.XlVAlign.xlVAlignCenter;
        //                xlsRng.Borders.LineStyle = LineStyle.SingleLine;

        //                /// Container Details Data
        //                rowNum += 1;
        //                for (i = 0; i <= dsData.Tables[1].Rows.Count - 1; i++)
        //                {
        //                    for (j = 0; j <= dsData.Tables[1].Columns.Count - 1; j++)
        //                    {
        //                        xlsSheet.Cells[i + rowNum, j + 1].Value = dsData.Tables[1].Rows[i][j] + "";
        //                    }
        //                }
        //                xlsRng = xlsSheet.Range[xlsSheet.Cells[rowNum, 1], xlsSheet.Cells[rowNum + dsData.Tables[1].Rows.Count - 1, dsData.Tables[1].Columns.Count]];
        //                xlsRng.Borders.LineStyle = LineStyle.SingleLine;

        //                /// Container Summary Details
        //                if (dsData.Tables[2].Rows.Count > 0)
        //                {
        //                    rowNum = rowNum + dsData.Tables[1].Rows.Count + 1;

        //                    xlsSheet.Cells[rowNum, 1].Value = dsCaption.Tables[0].Rows[32][0];
        //                    for (i = 0; i <= dsData.Tables[2].Rows.Count - 1; i++)
        //                    {
        //                        xlsSheet.Cells[rowNum, 3].Value = dsData.Tables[2].Rows[i][0] + "";
        //                        ///' Container Type
        //                        xlsSheet.Cells[rowNum, 4].Value = dsData.Tables[2].Rows[i][1] + "";
        //                        ///' Container Number
        //                    }
        //                    xlsRng = xlsSheet.Range[xlsSheet.Cells[rowNum, 3], xlsSheet.Cells[rowNum + dsData.Tables[2].Rows.Count - 1, 4]];
        //                    xlsRng.Borders.LineStyle = LineStyle.SingleLine;
        //                }

        //                xlsWB.SaveAs(strDestFileName);
        //                xlsWB.Close();
        //                xlsApp.Quit();
        //                System.Runtime.InteropServices.Marshal.ReleaseComObject(xlsRng);
        //                System.Runtime.InteropServices.Marshal.ReleaseComObject(xlsSheet);
        //                System.Runtime.InteropServices.Marshal.ReleaseComObject(xlsWB);
        //                System.Runtime.InteropServices.Marshal.ReleaseComObject(xlsApp);
        //                xlsRng = null;
        //                xlsSheet = null;
        //                xlsWB = null;
        //                xlsApp = null;
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            xlsRng = null;
        //            xlsSheet = null;
        //            xlsWB.Close();
        //            xlsWB = null;
        //            xlsApp.Quit();
        //            xlsApp = null;
        //            throw ex;
        //        }
        //    }
        //    #endregion

        //    #region " exportCargoManifest "
        //    public void exportCargoManifest(long JOBPK, ref string strDocName)
        //    {
        //        Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
        //        Cls_SeaCargoManifest objCargo = new Cls_SeaCargoManifest();
        //        QFORBusinessDev.CommonFeatures objrep = new QFORBusinessDev.CommonFeatures();
        //        ReportDocument repDoc = new ReportDocument();
        //        DataSet MainRptDS = new DataSet();
        //        DataSet AddressDS = new DataSet();
        //        DataSet CntDS = new DataSet();
        //        DataSet dsreefer = new DataSet();
        //        DataSet dsODC = new DataSet();
        //        DataSet dshaz = new DataSet();
        //        int i = 0;
        //        int j = 0;
        //        int K = 0;
        //        Int32 Haz_Reefer = 0;
        //        DataSet CommDS = null;
        //        DataSet subrptDs = new DataSet();
        //        DataSet ds = null;
        //        short CommodityType = 0;
        //        short CargoType = 0;
        //        short BizType = 0;
        //        long MBLPK = 0;

        //        try
        //        {
        //            ds = getJobDetails(JOBPK);

        //            if (ds.Tables[0].Rows.Count > 0)
        //            {
        //                CargoType = (Information.IsDBNull(ds.Tables[0].Rows[0]["CARGO_TYPE"]) ? -1 : ds.Tables[0].Rows[0]["CARGO_TYPE"]);
        //                BizType = (Information.IsDBNull(ds.Tables[0].Rows[0]["BUSINESS_TYPE"]) ? -1 : ds.Tables[0].Rows[0]["BUSINESS_TYPE"]);
        //                MBLPK = (Information.IsDBNull(ds.Tables[0].Rows[0]["MBL_MAWB_FK"]) ? -1 : ds.Tables[0].Rows[0]["MBL_MAWB_FK"]);
        //                CommodityType = (Information.IsDBNull(ds.Tables[0].Rows[0]["COMMODITY_GROUP_FK"]) ? -1 : ds.Tables[0].Rows[0]["COMMODITY_GROUP_FK"]);
        //            }

        //            if (BizType == 2 & CargoType == 4)
        //            {
        //                AddressDS = ObjClsTransportNote.FetchLocation(Session["LOGED_IN_LOC_FK"]);

        //                //'SEA     'RRR                        
        //                if (BizType == 2)
        //                {
        //                    MainRptDS = objCargo.FetchSeacargoReportDetails(MBLPK, JOBPK, CommodityType, 2);
        //                    CommDS = objCargo.FetchCommodityDetails(JOBPK);
        //                    subrptDs = objCargo.FetchFreightDetails(JOBPK);
        //                }
        //                CntDS.Tables.Add();
        //                CntDS.Tables[0].Columns.Add("MBPK", typeof(decimal));
        //                CntDS.Tables[0].Columns.Add("CNTTYPE", typeof(string));
        //                CntDS.Tables[0].Columns.Add("COUNT", typeof(decimal));
        //                DataRow Mdr = null;
        //                DataRow Cdr = null;
        //                bool Flag = false;
        //                foreach (DataRow Mdr_loopVariable in MainRptDS.Tables[0].Rows)
        //                {
        //                    Mdr = Mdr_loopVariable;
        //                    Flag = false;
        //                    if (!Information.IsDBNull(Mdr["CONTAINERTYPE"]))
        //                    {
        //                        foreach (DataRow Cdr_loopVariable in CntDS.Tables[0].Rows)
        //                        {
        //                            Cdr = Cdr_loopVariable;
        //                            if (Cdr["Cnttype"] == Mdr["CONTAINERTYPE"] & Cdr["MBPK"] == Mdr["JOBPK"])
        //                            {
        //                                Cdr["Count"] += 1;
        //                                Flag = true;
        //                                break; // TODO: might not be correct. Was : Exit For
        //                            }
        //                        }
        //                        if (Flag == false)
        //                        {
        //                            Cdr = CntDS.Tables[0].NewRow();
        //                            Cdr["MBPK"] = Mdr["JOBPK"];
        //                            Cdr["Cnttype"] = Mdr["CONTAINERTYPE"];
        //                            Cdr["Count"] = 1;
        //                            CntDS.Tables[0].Rows.Add(Cdr);
        //                        }
        //                    }
        //                }
        //                CntDS.AcceptChanges();
        //                repDoc.Load(Server.MapPath("../07Reports") + "/SeaCargomanifestBB.rpt");
        //                MainRptDS.ReadXmlSchema(Server.MapPath("../07Reports") + "/SeaCargoManifest.xsd");
        //                CntDS.ReadXmlSchema(Server.MapPath("../07Reports") + "/CntDetails.xsd");
        //                repDoc.OpenSubreport("CntDetails").SetDataSource(CntDS);
        //                subrptDs.ReadXmlSchema(Server.MapPath("../07Reports") + "/FreightCargoManifest.xsd");
        //                repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
        //                CommDS.ReadXml(Server.MapPath("../07Reports") + "/CommDetails.xsd");
        //                repDoc.OpenSubreport("CommodityDetails").SetDataSource(CommDS);

        //                dshaz = objCargo.FetchHazardousDetails(2, JOBPK, Strings.UCase(ViewState["Process"]));
        //                dshaz.ReadXmlSchema(Server.MapPath("../07Reports") + "/Hazardous.xsd");
        //                repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);

        //                dsreefer = objCargo.FetchReeferDetails(2, JOBPK, Strings.UCase(ViewState["Process"]));
        //                dsreefer.ReadXmlSchema(Server.MapPath("../07Reports") + "/reefer.xsd");
        //                repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);

        //                dsODC = objCargo.FetchODCDetails(2, JOBPK, Strings.UCase(ViewState["Process"]));
        //                dsODC.ReadXmlSchema(Server.MapPath("../07Reports") + "/ODC_DETAILS.xsd");
        //                repDoc.OpenSubreport("ODCDETAILS").SetDataSource(dsODC);

        //                if (Haz_Reefer != 1)
        //                {
        //                    repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
        //                }
        //                repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);
        //                repDoc.SetDataSource(MainRptDS);
        //                repDoc.SetParameterValue("commgrp", "All");

        //                repDoc.SetParameterValue("Type", "CargoManifest");
        //            }
        //            else
        //            {
        //                AddressDS = ObjClsTransportNote.FetchLocation(Session["LOGED_IN_LOC_FK"]);
        //                AddressDS.ReadXmlSchema(Server.MapPath("..\\07Reports") + "\\TN_Location.xsd");

        //                //'SEA     'RRR
        //                if (BizType == 2)
        //                {
        //                    MainRptDS = objCargo.FetchSeacargoReportDetails(MBLPK, JOBPK, CommodityType, 2, "EXPORT");
        //                    subrptDs = objCargo.FetchFreightDetails(JOBPK);

        //                    CntDS.Tables.Add();
        //                    CntDS.Tables[0].Columns.Add("MBPK", typeof(decimal));
        //                    CntDS.Tables[0].Columns.Add("CNTTYPE", typeof(string));
        //                    CntDS.Tables[0].Columns.Add("COUNT", typeof(decimal));

        //                    DataRow Mdr = null;
        //                    DataRow Cdr = null;
        //                    bool Flag = false;
        //                    foreach (DataRow Mdr_loopVariable in MainRptDS.Tables[0].Rows)
        //                    {
        //                        Mdr = Mdr_loopVariable;
        //                        Flag = false;
        //                        if (!Information.IsDBNull(Mdr["CONTAINERTYPE"]))
        //                        {
        //                            foreach (DataRow Cdr_loopVariable in CntDS.Tables[0].Rows)
        //                            {
        //                                Cdr = Cdr_loopVariable;
        //                                if (Cdr["Cnttype"] == Mdr["CONTAINERTYPE"] & Cdr["MBPK"] == Mdr["JOBPK"])
        //                                {
        //                                    Cdr["Count"] += 1;
        //                                    Flag = true;
        //                                    break; // TODO: might not be correct. Was : Exit For
        //                                }
        //                            }
        //                            if (Flag == false)
        //                            {
        //                                Cdr = CntDS.Tables[0].NewRow();
        //                                Cdr["MBPK"] = Mdr["JOBPK"];
        //                                Cdr["Cnttype"] = Mdr["CONTAINERTYPE"];
        //                                Cdr["Count"] = 1;
        //                                CntDS.Tables[0].Rows.Add(Cdr);
        //                            }
        //                        }
        //                    }
        //                    CntDS.AcceptChanges();
        //                    repDoc.Load(Server.MapPath("../07Reports") + "/SeaCargomanifest.rpt");
        //                    MainRptDS.ReadXmlSchema(Server.MapPath("../07Reports") + "/SeaCargoManifest.xsd");
        //                    subrptDs.ReadXmlSchema(Server.MapPath("../07Reports") + "/FreightCargoManifest.xsd");
        //                    CntDS.ReadXmlSchema(Server.MapPath("../07Reports") + "/CntDetails.xsd");
        //                    dshaz.ReadXmlSchema(Server.MapPath("../07Reports") + "/Hazardous.xsd");
        //                    dsreefer.ReadXmlSchema(Server.MapPath("../07Reports") + "/reefer.xsd");
        //                    dsODC.ReadXmlSchema(Server.MapPath("../07Reports") + "/ODC_DETAILS.xsd");
        //                    repDoc.OpenSubreport("CntDetails").SetDataSource(CntDS);
        //                    repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
        //                    repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);
        //                    repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);
        //                    repDoc.OpenSubreport("ODCDETAILS").SetDataSource(dsODC);
        //                    repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
        //                    repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);
        //                    repDoc.SetDataSource(MainRptDS);
        //                    repDoc.SetParameterValue("commgrp", "All");

        //                    repDoc.SetParameterValue("Type", "CargoManifest");
        //                    //'AIR
        //                }
        //                else
        //                {
        //                    subrptDs = objCargo.FetchAirFreightDetails(ViewState["JobCardPK"]);

        //                    MainRptDS = objCargo.FetchAircargoReportDetails(ViewState["MAWBPk"], ViewState["JobCardPK"], CommodityType, "EXPORT");
        //                    JOBPK = ViewState["JobCardPK"];

        //                    repDoc.Load(Server.MapPath("../07Reports") + "/rptSeaCargoManifest.rpt");
        //                    MainRptDS.ReadXmlSchema(Server.MapPath("../07Reports") + "/Main_SeaCargoManifest.xsd");
        //                    dshaz = objCargo.FetchHazardousDetails(1, JOBPK, Strings.UCase(ViewState["Process"]));
        //                    dsreefer = objCargo.FetchReeferDetails(1, JOBPK, Strings.UCase(ViewState["Process"]));
        //                    dsODC = objCargo.FetchODCDetails(1, JOBPK, Strings.UCase(ViewState["Process"]));
        //                    dshaz.ReadXmlSchema(Server.MapPath("../07Reports") + "/Hazardous.xsd");
        //                    dsreefer.ReadXmlSchema(Server.MapPath("../07Reports") + "/reefer.xsd");
        //                    dsODC.ReadXmlSchema(Server.MapPath("../07Reports") + "/ODC_DETAILS.xsd");
        //                    repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);
        //                    repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);
        //                    repDoc.OpenSubreport("ODCDETAILS").SetDataSource(dsODC);

        //                    subrptDs.ReadXmlSchema(Server.MapPath("../07Reports") + "/FreightCargoManifest.xsd");
        //                    repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);

        //                    if (Haz_Reefer != 1)
        //                    {
        //                        repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
        //                    }
        //                    repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);
        //                    repDoc.SetDataSource(MainRptDS);

        //                    repDoc.SetParameterValue(0, 1);
        //                    repDoc.SetParameterValue(1, getDefault(AddressDS.Tables[0].Rows[0]["corporate_name"], ""));
        //                    repDoc.SetParameterValue(2, "CargoManifest");
        //                }
        //            }

        //            if (BizType == 2)
        //            {
        //                objrep.getReportControls(repDoc, "QFOR3064");
        //            }
        //            else
        //            {
        //                objrep.getReportControls(ref repDoc, "QFOR3064", 2);
        //            }

        //            fn_Print_Store_Preview(repDoc, 2, strDocName, 2);
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    #endregion

        //    #region " getJobDetails "
        //    public DataSet getJobDetails(long JobCardPK)
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWF = new WorkFlow();

        //        sb.Append(" SELECT JCT.CARGO_TYPE, JCT.BUSINESS_TYPE, JCT.COMMODITY_GROUP_FK, JCT.MBL_MAWB_FK, JCT.PORT_MST_POL_FK, JCT.PORT_MST_POD_FK, ");
        //        sb.Append(" JCT.JOBCARD_REF_NO, JCT.HBL_HAWB_FK FROM JOB_CARD_TRN JCT WHERE JCT.JOB_CARD_TRN_PK = " + JobCardPK);

        //        try
        //        {
        //            return objWF.GetDataSet(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    #region " getHBLDetails "
        //    public DataSet getHBLDetails(long HBLPK)
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWF = new WorkFlow();

        //        sb.Append(" SELECT HBL.HBL_REF_NO, TO_CHAR(HBL.HBL_DATE, 'dd/mm/yyyy') HBL_DATE, DECODE(HBL.HBL_STATUS, 0, 'Draft', 1, 'Confirmed', 2, 'Released') HBL_STATUS, ");
        //        sb.Append(" HBL.PLACE_ISSUE FROM HBL_EXP_TBL HBL WHERE HBL.HBL_EXP_TBL_PK = " + HBLPK);

        //        try
        //        {
        //            return objWF.GetDataSet(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    #region " HBL Print "
        //    public string FetchHBLBarcode(string HBLNr)
        //    {
        //        int strReturnPk = 0;
        //        DataSet DsReturn = null;
        //        string strBarcode = null;
        //        string BookinNr = null;
        //        cls_HBL_Entry objHBLEntry = new cls_HBL_Entry();

        //        Int16 i = default(Int16);
        //        try
        //        {
        //            strReturnPk = objHBLEntry.FetchBarCodeManagerPk("QFOR3034");
        //            DsReturn = objHBLEntry.FetchBarCodeField(strReturnPk);
        //            strBarcode = "";

        //            if (DsReturn.Tables[0].Rows.Count > 0)
        //            {
        //                for (i = 0; i <= DsReturn.Tables[0].Rows.Count - 1; i++)
        //                {
        //                    var _with38 = DsReturn.Tables[0].Rows[i];
        //                    if (!string.IsNullOrEmpty(strBarcode))
        //                    {
        //                        strBarcode = strBarcode + "~" + HBLNr;
        //                    }
        //                    else
        //                    {
        //                        strBarcode = strBarcode + HBLNr;
        //                    }
        //                }
        //            }
        //            return strBarcode;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }

        //    public object exportHBLPrint(int JOBPK, ref string strDocName)
        //    {
        //        cls_HBL_List ObjClsHBLprinting = new cls_HBL_List();
        //        ReportDocument rptdoc = new ReportDocument();
        //        DataSet dsmain = null;
        //        DataSet dsdetails = null;
        //        DataSet dsPackage = null;
        //        DataSet dsDelAddr = null;
        //        DataSet dsExtra = null;
        //        DataSet dsLoc = null;
        //        DataSet dsConDet = null;
        //        DataSet dsBlClauses = null;
        //        DataSet dsBlClausesNew = null;
        //        string NoOfPackages = "";
        //        string ContainerDet = null;
        //        string strBlClauses = null;
        //        string @add = "";
        //        int I = 0;
        //        int j = 0;
        //        Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
        //        clsBlClauseForHblMbl objClsBlClause = new clsBlClauseForHblMbl();
        //        QFORBusinessDev.CommonFeatures objdef = new QFORBusinessDev.CommonFeatures();
        //        clsQuotationReport objQuotReport = new clsQuotationReport();
        //        string BarCode = "";
        //        DataSet dsfright = new DataSet();
        //        Int64 frtamt = default(Int64);
        //        Int64 othamt = default(Int64);
        //        DataSet dsamt = new DataSet();
        //        string PODPK = null;
        //        string CommPK = null;
        //        string FormFlag = null;
        //        string strHBLTYPE = null;
        //        DataSet ds = new DataSet();
        //        cls_HBL_Entry objHBLEntry = new cls_HBL_Entry();

        //        string HBLNr = null;
        //        string HBLPk = null;
        //        string HBLDate = null;
        //        string JobCardNr = null;
        //        string PlaceIssue = null;
        //        string HBL_Status = null;
        //        string CargoType = null;

        //        ds = getJobDetails(JOBPK);
        //        if (ds.Tables[0].Rows.Count > 0)
        //        {
        //            PODPK = Convert.ToString((Information.IsDBNull(ds.Tables[0].Rows[0]["PORT_MST_POL_FK"]) ? -1 : ds.Tables[0].Rows[0]["PORT_MST_POL_FK"]) + "," + (Information.IsDBNull(ds.Tables[0].Rows[0]["PORT_MST_POD_FK"]) ? -1 : ds.Tables[0].Rows[0]["PORT_MST_POD_FK"]));
        //            JobCardNr = ds.Tables[0].Rows[0]["JOBCARD_REF_NO"];
        //            CargoType = (Information.IsDBNull(ds.Tables[0].Rows[0]["CARGO_TYPE"]) ? -1 : ds.Tables[0].Rows[0]["CARGO_TYPE"]);
        //            HBLPk = (Information.IsDBNull(ds.Tables[0].Rows[0]["HBL_HAWB_FK"]) ? -1 : ds.Tables[0].Rows[0]["HBL_HAWB_FK"]);
        //        }

        //        ds = getHBLDetails(HBLPk);
        //        if (ds.Tables[0].Rows.Count > 0)
        //        {
        //            HBLNr = ds.Tables[0].Rows[0]["HBL_REF_NO"];
        //            HBLDate = (Information.IsDBNull(ds.Tables[0].Rows[0]["HBL_DATE"]) ? "" : ds.Tables[0].Rows[0]["HBL_DATE"]);
        //            HBL_Status = (Information.IsDBNull(ds.Tables[0].Rows[0]["HBL_STATUS"]) ? "" : ds.Tables[0].Rows[0]["HBL_STATUS"]);
        //            PlaceIssue = (Information.IsDBNull(ds.Tables[0].Rows[0]["PLACE_ISSUE"]) ? "" : ds.Tables[0].Rows[0]["PLACE_ISSUE"]);
        //        }

        //        CommPK = "0";
        //        FormFlag = "Sea";
        //        strHBLTYPE = "Yes";

        //        try
        //        {
        //            if (objHBLEntry.GetBarcodeFlag("HBL") == "1" & ConfigurationManager.AppSettings["ShowBarcode"] == "1")
        //            {
        //                BarCode = "*" + FetchHBLBarcode(HBLNr) + "*";
        //            }

        //            if (strHBLTYPE == "Yes")
        //            {
        //                dsmain = ObjClsHBLprinting.FetchMainHBL(HBLPk, Request.QueryString["From"], Session["LOGED_IN_LOC_FK"]);
        //            }
        //            else if (strHBLTYPE == "No")
        //            {
        //                dsmain = ObjClsHBLprinting.FetchHBLBIFA(HBLPk, Request.QueryString["From"]);
        //            }
        //            if (dsmain.Tables[0].Rows.Count > 0)
        //            {
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["SHIPPERADD"], " ")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["SHIPPERADD"] + Constants.vbCrLf;
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["shadd2"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["shadd2"] + Constants.vbCrLf;
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["shadd3"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["shadd3"] + Constants.vbCrLf;
        //            }
        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["shcity"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["shcity"] + Constants.vbCrLf;
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["shzip"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["shzip"];
        //            }

        //            dsmain.Tables[0].Rows[0]["SHIPPERADD"] = @add;

        //            @add = "";

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["CONSIGNEEADD"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["CONSIGNEEADD"] + Constants.vbCrLf;
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["ADM_ADDRESS_2"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["ADM_ADDRESS_2"] + Constants.vbCrLf;
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["ADM_ADDRESS_3"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["ADM_ADDRESS_3"] + Constants.vbCrLf;
        //            }
        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["adm_city"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["adm_city"] + Constants.vbCrLf;
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["adm_zip_code"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["adm_zip_code"];
        //            }

        //            dsmain.Tables[0].Rows[0]["CONSIGNEEADD"] = @add;

        //            @add = "";

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["NOTIFYADD"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["NOTIFYADD"] + Constants.vbCrLf;
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["nadd2"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["nadd2"] + Constants.vbCrLf;
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["nadd3"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["nadd3"] + Constants.vbCrLf;
        //            }
        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["ncity"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["ncity"] + Constants.vbCrLf;
        //            }

        //            if (Convert.ToString(objdef.getDefault(dsmain.Tables[0].Rows[0]["nzip"], "")).Trim().Length > 0)
        //            {
        //                @add += dsmain.Tables[0].Rows[0]["nzip"];
        //            }

        //            dsmain.Tables[0].Rows[0]["NOTIFYADD"] = @add;
        //            dsPackage = ObjClsHBLprinting.FetchPackages(HBLPk, Request.QueryString["From"]);
        //            for (I = 0; I <= dsPackage.Tables[0].Rows.Count - 1; I++)
        //            {
        //                if (I == 0)
        //                {
        //                    NoOfPackages = dsPackage.Tables[0].Rows[I]["pack"];
        //                }
        //                else
        //                {
        //                    NoOfPackages = NoOfPackages + "," + dsPackage.Tables[0].Rows[I]["pack"];
        //                }
        //            }

        //            if (NoOfPackages.Length <= 0)
        //            {
        //                NoOfPackages = "";
        //            }

        //            if (strHBLTYPE == "Yes")
        //            {
        //                rptdoc.Load(Server.MapPath("../07Reports") + "\\rptHBLPrinting_FIATA.rpt");
        //            }
        //            else if (strHBLTYPE == "No")
        //            {
        //                rptdoc.Load(Server.MapPath("../07Reports") + "\\rptHBLPrinting.rpt");
        //            }
        //            rptdoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
        //            dsmain.ReadXmlSchema(Server.MapPath("../07Reports") + "\\HBLMAIN.XSD");

        //            dsBlClauses = ObjClsHBLprinting.FetchBlClauses(HBLPk, Request.QueryString["From"]);
        //            dsBlClauses.ReadXmlSchema(Server.MapPath("../07Reports") + "\\BlClausesForHBL.XSD");

        //            if (Request.QueryString["From"] == "MSTSEA")
        //            {
        //                dsDelAddr = ObjClsHBLprinting.FetchDelAddress(HBLPk, Request.QueryString["From"]);
        //            }
        //            else
        //            {
        //                dsDelAddr = ObjClsHBLprinting.FetchDelAddress(HBLPk, Request.QueryString["From"]);
        //            }

        //            dsDelAddr.ReadXmlSchema(Server.MapPath("../07Reports") + "\\HBLDelAddr.xsd");

        //            dsExtra = ObjClsHBLprinting.FetchDescExtra(HBLPk, Request.QueryString["From"]);
        //            dsExtra.ReadXmlSchema(Server.MapPath("../07Reports") + "\\HBLDescExtra.xsd");

        //            dsLoc = ObjClsTransportNote.FetchLocation(Session["LOGED_IN_LOC_FK"]);
        //            dsLoc.ReadXmlSchema(Server.MapPath("../07Reports") + "\\TN_Location.xsd");

        //            dsBlClausesNew = objClsBlClause.FetchBlClausesForHBL("", ref 4, ref 1, ref 1, ((PODPK == null) ? "0" : PODPK), ((CommPK == null) ? "0" : CommPK), HBLPk, HBLDate, ((FormFlag == null) ? "" : FormFlag));

        //            dsBlClausesNew.ReadXmlSchema(Server.MapPath("../Documentation") + "\\PrintBLClause.XSD");
        //            dsfright.ReadXmlSchema(Server.MapPath("../07Reports") + "\\frightdtls.XSD");

        //            dsamt = objHBLEntry.getamt(JobCardNr);
        //            if ((dsamt.Tables[0].Rows.Count > 0))
        //            {
        //                frtamt = getDefault(dsamt.Tables[0].Rows[0][0], 0);
        //                othamt = getDefault(dsamt.Tables[0].Rows[1][0], 0);
        //            }
        //            else
        //            {
        //                frtamt = 0;
        //                othamt = 0;
        //            }

        //            if (strHBLTYPE == "Yes")
        //            {
        //                dsdetails = ObjClsHBLprinting.FetchHBLDetails(HBLPk, Request.QueryString["From"]);
        //                dsConDet = ObjClsHBLprinting.Get_ConDet(HBLPk);
        //                dsConDet.ReadXmlSchema(Server.MapPath("../07Reports") + "\\CONDET.XSD");
        //            }
        //            else if (strHBLTYPE == "No")
        //            {
        //                dsdetails = ObjClsHBLprinting.FetchHBLBIFADetails(HBLPk, Request.QueryString["From"]);
        //            }
        //            dsdetails.ReadXmlSchema(Server.MapPath("../07Reports") + "\\HBLDET.XSD");

        //            rptdoc.OpenSubreport("rptHBLMain").SetDataSource(dsmain);
        //            rptdoc.OpenSubreport("rptHBLDelAddr").SetDataSource(dsDelAddr);

        //            if (strHBLTYPE == "Yes")
        //            {
        //                rptdoc.OpenSubreport("rptConDetails").SetDataSource(dsConDet);
        //                rptdoc.OpenSubreport("CntrDetailsAnnexure").SetDataSource(dsConDet);
        //            }
        //            rptdoc.OpenSubreport("rptDescExtra").SetDataSource(dsExtra);
        //            rptdoc.OpenSubreport("BLClause").SetDataSource(dsBlClauses);
        //            rptdoc.OpenSubreport("rptBLClause").SetDataSource(dsBlClausesNew);
        //            rptdoc.OpenSubreport("frightdtls").SetDataSource(dsfright);

        //            for (I = 0; I <= dsBlClauses.Tables[0].Rows.Count - 1; I++)
        //            {
        //                if (I == 0)
        //                {
        //                    strBlClauses = dsBlClauses.Tables[0].Rows[I]["BL_DESCRIPTION"];
        //                }
        //                else
        //                {
        //                    strBlClauses = strBlClauses + "," + dsBlClauses.Tables[0].Rows[I]["BL_DESCRIPTION"];
        //                }
        //            }

        //            for (I = 0; I <= dsdetails.Tables[0].Rows.Count - 1; I++)
        //            {
        //                if (Information.IsDBNull(dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"]))
        //                {
        //                    dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"] = "";
        //                }
        //                else if (Information.IsDBNull(dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"]))
        //                {
        //                    dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"] = "";
        //                }

        //                if (I == 0)
        //                {
        //                    ContainerDet = dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"] + Constants.vbTab + dsdetails.Tables[0].Rows[I]["CONTAINER_TYPE_MST_ID"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["PACK_COUNT"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["GROSS_WEIGHT"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["TAREWEIGHT"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["Volume"];
        //                }
        //                else
        //                {
        //                    ContainerDet = ContainerDet + Constants.vbNewLine + dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"] + Constants.vbTab + dsdetails.Tables[0].Rows[I]["CONTAINER_TYPE_MST_ID"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["PACK_COUNT"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["GROSS_WEIGHT"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["TAREWEIGHT"] + Constants.vbTab + Constants.vbTab + dsdetails.Tables[0].Rows[I]["Volume"];
        //                }
        //            }

        //            rptdoc.SetDataSource(dsdetails);

        //            if (strHBLTYPE == "Yes")
        //            {
        //                if (dsConDet.Tables[0].Rows.Count > 0)
        //                {
        //                    if (dsConDet.Tables[0].Rows.Count > 3 | Convert.ToString(objdef.getDefault(dsConDet.Tables[0].Rows[0]["GDESC"], "")).Trim().Length > 25 | Convert.ToString(objdef.getDefault(dsConDet.Tables[0].Rows[0]["MARKS"], "")).Trim().Length > 25)
        //                    {
        //                        rptdoc.SetParameterValue("ShowAnnexure", "Y");
        //                    }
        //                    else
        //                    {
        //                        rptdoc.SetParameterValue("ShowAnnexure", "N");
        //                    }
        //                }
        //                else
        //                {
        //                    rptdoc.SetParameterValue("ShowAnnexure", "N");
        //                }
        //            }
        //            rptdoc.SetParameterValue(0, NoOfPackages.Trim());
        //            rptdoc.SetParameterValue(1, ContainerDet);

        //            rptdoc.SetParameterValue(2, Strings.Trim(PlaceIssue));
        //            DataSet dsLoadt = new DataSet();
        //            dsLoadt = ObjClsHBLprinting.FetchLoadDate(HBLPk, Request.QueryString["From"]);
        //            if (dsmain.Tables[0].Rows.Count > 0)
        //            {
        //                if (Information.IsDBNull(dsmain.Tables[0].Rows[0]["VSLVOY"]) == true)
        //                {
        //                    rptdoc.SetParameterValue(3, "");
        //                }
        //                else
        //                {
        //                    rptdoc.SetParameterValue(3, dsmain.Tables[0].Rows[0]["VSLVOY"]);
        //                }

        //                if (Information.IsDBNull(dsmain.Tables[0].Rows[0]["POL"]) == true)
        //                {
        //                    rptdoc.SetParameterValue(6, "");
        //                }
        //                else
        //                {
        //                    rptdoc.SetParameterValue(6, dsmain.Tables[0].Rows[0]["POL"]);
        //                }

        //                if (strHBLTYPE == "Yes")
        //                {
        //                    if (Information.IsDBNull(dsmain.Tables[0].Rows[0]["POD"]) == true)
        //                    {
        //                        rptdoc.SetParameterValue(12, "");
        //                    }
        //                    else
        //                    {
        //                        rptdoc.SetParameterValue(12, dsmain.Tables[0].Rows[0]["POD"]);
        //                    }
        //                }
        //                if (Information.IsDBNull(dsmain.Tables[0].Rows[0]["HBLDATE"]) == true)
        //                {
        //                    rptdoc.SetParameterValue(7, "");
        //                }
        //                else
        //                {
        //                    rptdoc.SetParameterValue(7, Strings.Format(dsmain.Tables[0].Rows[0]["HBLDATE"], dateFormat));
        //                }
        //            }
        //            rptdoc.SetParameterValue(8, ObjClsHBLprinting.GetBarcodeFlag("HBL"));
        //            rptdoc.SetParameterValue(9, BarCode);
        //            rptdoc.SetParameterValue(10, HBL_Status);

        //            if (strHBLTYPE == "Yes")
        //            {
        //                rptdoc.SetParameterValue(11, "");

        //                rptdoc.SetParameterValue(13, dsmain.Tables[0].Rows[0]["HBL_ORIGINAL_PRINTS"]);
        //                rptdoc.SetParameterValue(14, dsmain.Tables[0].Rows[0]["FRGHTAMT"]);
        //                rptdoc.SetParameterValue(15, dsmain.Tables[0].Rows[0]["OTHRFRGHTAMT"]);
        //                rptdoc.SetParameterValue(16, CargoType);
        //                rptdoc.SetParameterValue(17, "");

        //                rptdoc.SetParameterValue(18, "0");
        //            }

        //            rptdoc.SetParameterValue(4, dsLoc.Tables[0].Rows[0]["ADDRESS_LINE1"]);
        //            if (Information.IsDBNull(dsmain.Tables[0].Rows[0]["HBLDATE"]) == true)
        //            {
        //                rptdoc.SetParameterValue(5, "");
        //            }
        //            else
        //            {
        //                rptdoc.SetParameterValue(5, Strings.Format(dsmain.Tables[0].Rows[0]["HBLDATE"], dateFormat));
        //            }

        //            rptdoc.SetParameterValue("credit", ObjClsHBLprinting.FetchCredit(HBLPk, Request.QueryString["From"]));

        //            rptdoc.SetParameterValue("fright", 0);
        //            if (strHBLTYPE == "Yes")
        //            {
        //                rptdoc.SetParameterValue("frtamt", Convert.ToString(Strings.FormatNumber(frtamt, 2)));
        //                rptdoc.SetParameterValue("othamt", Convert.ToString(Strings.FormatNumber(othamt, 2)));
        //                rptdoc.SetParameterValue("totamt", Convert.ToString(Strings.FormatNumber(frtamt + othamt, 2)));
        //                rptdoc.SetParameterValue("Currency", Session["CURRENCY_ID"]);
        //            }

        //            if (strHBLTYPE == "Yes")
        //            {
        //                objdef.getReportControls(rptdoc, "QFOR3033");
        //            }
        //            else if (strHBLTYPE == "No")
        //            {
        //                objdef.getReportControls(ref rptdoc, "QFOR3033", 1);
        //            }

        //            fn_Print_Store_Preview(rptdoc, 1, strDocName, 2);
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    #endregion
        //    #region "Fetch Standard Protocol"

        //    public DataTable Fetch_Email_Std_Protocol_Data(long Document_mst_pk = 0, string Flag = "")
        //    {
        //        WorkFlow objWF = new WorkFlow();
        //        DataSet ds = new DataSet();
        //        try
        //        {
        //            objWF.OpenConnection();
        //            objWF.MyDataAdapter = new OracleDataAdapter();
        //            var _with39 = objWF.MyDataAdapter;
        //            _with39.SelectCommand = new OracleCommand();
        //            _with39.SelectCommand.Connection = objWF.MyConnection;
        //            _with39.SelectCommand.CommandText = objWF.MyUserName + ".EMAIL_STANDARD_PROTOCOL_PKG.FETCH_EMAIL_STD_PROTOCOL_DATA";
        //            _with39.SelectCommand.CommandType = CommandType.StoredProcedure;
        //            _with39.SelectCommand.Parameters.Add("DOC_MST_PK_IN", Document_mst_pk).Direction = ParameterDirection.Input;
        //            _with39.SelectCommand.Parameters.Add("FLAG_IN", OracleType.VarChar).Value = Flag;
        //            _with39.SelectCommand.Parameters.Add("CONT_OUT", OracleType.Cursor).Direction = ParameterDirection.Output;
        //            _with39.Fill(ds);
        //            return ds.Tables[0];
        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //        finally
        //        {
        //            objWF.CloseConnection();
        //        }
        //    }
        //    #endregion

        //    #region "FetchContainers"
        //    public DataSet FetchContainers()
        //    {
        //        System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
        //        WorkFlow objWF = new WorkFlow();
        //        strSQL.Append(" SELECT ");
        //        strSQL.Append(" CTMT.CONTAINER_TYPE_MST_PK,  CTMT.CONTAINER_TYPE_MST_ID, CTMT.CONTAINER_TYPE_NAME ");
        //        strSQL.Append(" FROM CONTAINER_TYPE_MST_TBL CTMT ");
        //        strSQL.Append(" WHERE CTMT.ACTIVE_FLAG = 1 ");
        //        strSQL.Append(" ORDER BY CTMT.PREFERENCES");
        //        try
        //        {
        //            return objWF.GetDataSet(strSQL.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    public DataSet FetchContainersKind(string ContainerPk = "", string ContainerId = "")
        //    {
        //        System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
        //        WorkFlow objWF = new WorkFlow();
        //        strSQL.Append("SELECT CK.CONTAINER_TYPE_ID CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL CT, CONTAINER_KIND_MST_TBL CK ");
        //        strSQL.Append(" WHERE CT.CONTAINER_KIND=CK.CONTAINER_TYPE_MST_PK ");
        //        if (ContainerPk > 0)
        //        {
        //            strSQL.Append(" AND CT.CONTAINER_TYPE_MST_PK='" + ContainerPk);
        //        }
        //        if (!string.IsNullOrEmpty(ContainerId.Trim()))
        //        {
        //            strSQL.Append(" AND UPPER(CT.CONTAINER_TYPE_MST_ID)='" + ContainerId.ToUpper() + "' ");
        //        }
        //        try
        //        {
        //            return objWF.GetDataSet(strSQL.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    #region " FetchOutstandingFlag "
        //    public string FetchOutstandingFlag()
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWF = new WorkFlow();

        //        sb.Append("SELECT DECODE(LMT.OUTSTANDING_FLG, 0, 'No', 'Yes') FROM LOCATION_MST_TBL LMT WHERE LMT.LOCATION_MST_PK = " + Session["LOGED_IN_LOC_FK"]);
        //        try
        //        {
        //            return objWF.ExecuteScaler(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    #region "Cheack Outstanding Balance in Notepad"
        //    public DataSet OutStandingbalance(string ShipperPk, string CURRENCY_FK, Int32 Currentpage, Int32 TotalPage)
        //    {
        //        try
        //        {
        //            WorkFlow objWF = new WorkFlow();
        //            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

        //            sb.Append("select ROWNUM as SLNR, ");
        //            sb.Append("       Q.CONSOL_INVOICE_PK INVOICE_TRN_PK,");
        //            sb.Append("       Q.INV_NO,");
        //            sb.Append("       Q.CFK,");
        //            sb.Append("       Q.CUSTOMER_NAME,");
        //            sb.Append("       Q.INV_AMOUNT,");
        //            sb.Append("       Q.EXCHANGE_AMOUT, ");
        //            sb.Append("       Q.RECEIVED_AMOUNT, ");
        //            sb.Append("       (Q.EXCHANGE_AMOUT - Q.RECEIVED_AMOUNT) AS RECEIVABLE_AMT, ");
        //            sb.Append("       ''  AS RECEIVABLE_AMT ");

        //            sb.Append("          from (SELECT IT.CONSOL_INVOICE_PK,");
        //            sb.Append("               IT.INVOICE_REF_NO INV_NO,");
        //            sb.Append("               IT.CURRENCY_MST_FK CFK,");
        //            sb.Append("               CTMT.CURRENCY_ID CURRID,");
        //            sb.Append("               CUST.CUSTOMER_NAME, ");
        //            sb.Append("               SUM(NVL(IT.NET_RECEIVABLE, 0)) INV_AMOUNT,");

        //            sb.Append("               ROUND(SUM(NVL(IT.NET_RECEIVABLE, 0) * GET_EX_RATE(IT.CURRENCY_MST_FK, " + CURRENCY_FK + ", IT.INVOICE_DATE)) - SUM(NVL((SELECT SUM(NVL(CRN.CRN_AMT_IN_CRN_CUR, 0) * ");
        //            sb.Append("               GET_EX_RATE(CNT.CURRENCY_MST_FK, " + CURRENCY_FK + ", CNT.CREDIT_NOTE_DATE)) FROM CREDIT_NOTE_TRN_TBL CRN, CREDIT_NOTE_TBL CNT WHERE ");
        //            sb.Append("               CNT.CRN_TBL_PK = CRN.CRN_TBL_FK AND CRN.CONSOL_INVOICE_TRN_FK = IT.CONSOL_INVOICE_PK), 0)), 2) AS EXCHANGE_AMOUT, ");

        //            sb.Append("               ROUND(SUM(NVL((SELECT SUM(NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(CT.CURRENCY_MST_FK, " + CURRENCY_FK + ", CT.COLLECTIONS_DATE)) ");
        //            sb.Append("               FROM COLLECTIONS_TBL CT, COLLECTIONS_TRN_TBL CTRN WHERE CT.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK ");
        //            sb.Append("               AND CTRN.INVOICE_REF_NR = IT.INVOICE_REF_NO), 0)), 2) RECEIVED_AMOUNT ");

        //            sb.Append("          FROM CONSOL_INVOICE_TBL         IT, ");
        //            sb.Append("               CUSTOMER_MST_TBL        CUST,");
        //            sb.Append("               CURRENCY_TYPE_MST_TBL   CTMT");
        //            sb.Append("         WHERE IT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
        //            sb.Append("           AND IT.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK ");
        //            sb.Append("           AND IT.CHK_INVOICE IN (0, 1) ");
        //            sb.Append(" AND IT.CUSTOMER_MST_FK = " + ShipperPk);

        //            sb.Append("  GROUP BY IT.CONSOL_INVOICE_PK,");
        //            sb.Append("                  IT.INVOICE_REF_NO,");
        //            sb.Append("                  IT.INVOICE_DATE,");
        //            sb.Append("                  IT.CURRENCY_MST_FK,");
        //            sb.Append("                  CUST.CUSTOMER_NAME,");
        //            sb.Append("                  CTMT.CURRENCY_ID");
        //            sb.Append("         ORDER BY IT.INVOICE_DATE DESC) Q WHERE (Q.EXCHANGE_AMOUT - Q.RECEIVED_AMOUNT) > 0 ");
        //            return objWF.GetDataSet(sb.ToString());
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //    }
        //    #endregion

        //    #region " FetchCustomer "
        //    public string FetchCustomer(int CustPK)
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWF = new WorkFlow();

        //        sb.Append(" SELECT CMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK = " + CustPK);
        //        try
        //        {
        //            return objWF.ExecuteScaler(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    #region "Grid for Importing Excel sheet" ''GOUTAM : TO AVOID HARD CODE FOR EXPORT TO EXCEL OPTION
        //    public void sub_gbSetExcelHeader(ref ICollection Excel_GridHeaders, ref System.Type HeaderType, ref Infragistics.WebUI.UltraWebGrid.UltraWebGrid UWG1, ref string HeaderName, Int32 Headercnt)
        //    {
        //        Int32 i = default(Int32);
        //        for (i = 0; i <= Headercnt - 1; i++)
        //        {
        //            string colname = Enum.GetName(HeaderType, i);
        //            GridHeaderSetting obj_GridSet = null;

        //            obj_GridSet = (GridHeaderSetting)Excel_GridHeaders["" + HeaderName + "." + colname];
        //            if (!string.IsNullOrEmpty(obj_GridSet.M_TEXT))
        //            {
        //                UWG1.Columns[i].HeaderText = "" + obj_GridSet.M_TEXT + ((obj_GridSet.M_Settings == 1 ? "" : "")).ToString();
        //            }
        //            if (obj_GridSet.M_Settings == 2)
        //                UWG1.Columns[i].Hidden = true;
        //            if (obj_GridSet.M_Width != 0)
        //                UWG1.Columns[i].Width = Unit.Percentage(obj_GridSet.M_Width);
        //        }
        //    }

        //    public object ExportToExcel(ref Infragistics.WebUI.UltraWebGrid.ExcelExport.UltraWebGridExcelExporter Ultrawebgridexcelexporter1, ref Infragistics.WebUI.UltraWebGrid.UltraWebGrid UWG1)
        //    {
        //        Int32 ColCnt = default(Int32);
        //        for (ColCnt = 0; ColCnt <= UWG1.Columns.Count - 1; ColCnt++)
        //        {
        //            UWG1.Columns[ColCnt].HeaderStyle.ForeColor = Color.Black;
        //            UWG1.Columns[ColCnt].HeaderStyle.BorderColor = Color.Black;
        //            UWG1.Columns[ColCnt].HeaderStyle.BackColor = Color.LightSkyBlue;
        //            UWG1.Columns[ColCnt].HeaderStyle.Font.Bold = true;
        //            UWG1.Columns[ColCnt].HeaderStyle.Font.Size = FontUnit.Point(8);
        //            UWG1.Columns[ColCnt].CellStyle.Font.Size = FontUnit.Point(8);
        //            UWG1.Columns[ColCnt].CellStyle.BorderColor = Color.Black;
        //            UWG1.Columns[ColCnt].CellStyle.BorderColor = System.Drawing.Color.Black;
        //            UWG1.Columns[ColCnt].HeaderStyle.BorderColor = System.Drawing.Color.Black;
        //        }

        //        try
        //        {
        //            Ultrawebgridexcelexporter1.WorksheetName = "Sheet1";
        //            Ultrawebgridexcelexporter1.ExportMode = Infragistics.WebUI.UltraWebGrid.ExcelExport.ExportMode.Download;
        //            Ultrawebgridexcelexporter1.Export(UWG1);
        //            string StName = "File" + Strings.Replace(DateTime.Today, ":", "-");
        //            string StrName = Strings.Replace(StName, "/", "-");
        //            Ultrawebgridexcelexporter1.DownloadName = StrName;
        //        }
        //        catch (ArgumentOutOfRangeException ex)
        //        {
        //            return ex.Message;
        //        }

        //        return "";
        //    }


        //    public void sub_gbSetHeaderSForDataGrid(ref ICollection Temp_GridHeaders, ref System.Type HeaderType, ref Infragistics.WebUI.UltraWebGrid.UltraWebGrid UWG1, ref string HeaderName, Int32 Headercnt)
        //    {
        //        Int32 i = default(Int32);
        //        for (i = 0; i <= Headercnt - 1; i++)
        //        {
        //            string colname = Enum.GetName(HeaderType, i);
        //            GridHeaderSetting obj_GridSet = null;

        //            obj_GridSet = (GridHeaderSetting)Temp_GridHeaders["" + HeaderName + "." + colname];
        //            if (!string.IsNullOrEmpty(obj_GridSet.M_TEXT))
        //            {
        //                UWG1.Columns[i].HeaderText = "<span Title='" + obj_GridSet.M_ToolTip + "'>" + obj_GridSet.M_TEXT + ((obj_GridSet.M_Settings == 1 ? "<span style='COLOR:red;' > *</span>" : "")).ToString() + "</span>";
        //            }
        //            if (obj_GridSet.M_Settings == 2)
        //                UWG1.Columns[i].Hidden = true;
        //            if (obj_GridSet.M_Width != 0)
        //                UWG1.Columns[i].Width = Unit.Percentage(obj_GridSet.M_Width);
        //        }
        //    }
        //    #endregion

        //    #region "Fetch Document PK  For Sending Mail"
        //    public DataTable FetchDocument(string documentId)
        //    {
        //        System.Text.StringBuilder strbldrSQL = new System.Text.StringBuilder();
        //        WorkFlow objWF = new WorkFlow();

        //        strbldrSQL.Append(" Select DMT.DOCUMENT_MST_PK ");
        //        strbldrSQL.Append(" FROM DOCUMENT_MST_TBL DMT ");
        //        strbldrSQL.Append(" WHERE");
        //        strbldrSQL.Append(" DMT.DOCUMENT_ID='" + documentId + "'");
        //        try
        //        {
        //            return objWF.GetDataTable(strbldrSQL.ToString());
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion
        //    #region "GetApprover Dtls"
        //    public DataSet GetApproverDtls(long Docpk, long Lockpk)
        //    {
        //        WorkFlow ObjWF = new WorkFlow();
        //        System.Text.StringBuilder strbld = new System.Text.StringBuilder();
        //        strbld.Append(" SELECT DISTINCT");
        //        strbld.Append(" NVL(UMT.USER_MST_PK,0) AS USER_MST_PK ,NVL(UMT1.USER_MST_PK,0) AS USER_MST_PK1,NVL(UMT2.USER_MST_PK,0) AS USER_MST_PK2,NVL(UMT3.USER_MST_PK,0) AS USER_MST_PK3");
        //        strbld.Append(" FROM WORKFLOW_RULES_TRN WF, DOCUMENT_MST_TBL DOC, LOCATION_MST_TBL LMT,");
        //        strbld.Append(" USER_MST_TBL UMT,USER_MST_TBL UMT1,USER_MST_TBL UMT2,USER_MST_TBL UMT3,WORKFLOW_LOC_TRN WLT");
        //        strbld.Append(" WHERE 1=1 ");
        //        strbld.Append(" AND WF.DOCUMENT_MST_FK =" + Docpk);
        //        strbld.Append(" AND WF.WORKFLOW_RULES_PK =WLT.WORKFLOW_RULES_FK");
        //        strbld.Append(" AND WLT.FROM_LOC_MST_FK = " + Lockpk);
        //        strbld.Append(" AND WF.Active=1");
        //        strbld.Append(" AND DOC.DOCUMENT_MST_PK=WF.DOCUMENT_MST_FK");
        //        strbld.Append(" AND UMT.USER_MST_PK(+) = WF.USER_MST_FK");
        //        strbld.Append(" AND UMT1.USER_MST_PK(+) = WF.COPY_TO1_FK");
        //        strbld.Append(" AND UMT2.USER_MST_PK(+) = WF.COPY_TO2_FK");
        //        strbld.Append(" AND UMT3.USER_MST_PK(+) = WF.COPY_TO3_FK");
        //        try
        //        {
        //            return ObjWF.GetDataSet(strbld.ToString());
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //        finally
        //        {
        //            strbld = null;
        //            ObjWF = null;
        //        }
        //    }
        //    #endregion

        //    #region "Fucntion to Check the Restriction"
        //    public object CheckRestriction(int BizType, int CargoPK, System.DateTime TransDate, int ShippingLinePK, int CustomerPK, int ShipperPK, int ConsigneePK, int NotifyPK, int POOPK, int POLPK,
        //    int PODPK, int PFDPK, int IncoTermsPK, int CommGrpPK, int CommPK, double Weight, double Vol, int ContainerMstPK, int Qty, int CreatedPK,
        //    int FormType, int RefPK)
        //    {
        //        WorkFlow objWK = new WorkFlow();
        //        System.Data.OracleClient.OracleCommand FetchCommand = new System.Data.OracleClient.OracleCommand();
        //        OracleTransaction TRAN = null;
        //        int RecAfct = 0;
        //        string RetValue = null;
        //        try
        //        {
        //            objWK.OpenConnection();
        //            TRAN = objWK.MyConnection.BeginTransaction();
        //            var _with40 = FetchCommand;
        //            _with40.Connection = objWK.MyConnection;
        //            _with40.CommandType = CommandType.StoredProcedure;
        //            _with40.CommandText = objWK.MyUserName + ".GET_RESTRICTION_DTLS";
        //            _with40.Parameters.Clear();
        //            _with40.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("CARGO_TYPE_IN", CargoPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("TRNSACTION_DATE_IN", TransDate).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("SHIPPING_LINE_FK_IN", ShippingLinePK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("CUSTOMER_MST_FK_IN", CustomerPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("SHIPPER_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("CONSIGNEE_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("NOTIFY_MST_FK_IN", NotifyPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("POO_FK_IN", POOPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("POD_FK_IN", PODPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("PFD_FK_IN", PFDPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("INCO_TERMS_FK_IN", IncoTermsPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("COMM_GRP_FK_IN", CommGrpPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("COMMODITY_FK_IN", CommPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("WEIGHT_IN", Weight).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("VOLUME_IN", Vol).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("CONTAINER_MST_FK_IN", ContainerMstPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("CONTAINER_NR_IN", Qty).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("CREATED_BY_FK_IN", CreatedPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("FORM_TYPE_IN", FormType).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("REF_PK_IN", RefPK).Direction = ParameterDirection.Input;
        //            _with40.Parameters.Add("RET_VALUE", System.Data.OracleClient.OracleType.VarChar, 1500, "RET_VALUE").Direction = ParameterDirection.Output;
        //            var _with41 = objWK.MyDataAdapter;
        //            _with41.InsertCommand = FetchCommand;
        //            _with41.InsertCommand.Transaction = TRAN;
        //            RecAfct = _with41.InsertCommand.ExecuteNonQuery();
        //            RetValue = objWK.MyDataAdapter.InsertCommand.Parameters("RET_VALUE").Value;
        //            return RetValue;
        //        }
        //        catch (Exception sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        finally
        //        {
        //            objWK.CloseConnection();
        //        }
        //    }
        //    #endregion

        //    #region "Insert Restriction Approval Information"
        //    public ArrayList SaveRestrictionApproval(int RestrictionFK, int FormType, int RefPK, int CreatedPK, string RefNr, System.DateTime RefDate, int CustPK)
        //    {
        //        WorkFlow objWK = new WorkFlow();
        //        ArrayList arrMessage = new ArrayList();
        //        OracleTransaction TRAN = null;
        //        int intPKVal = 0;
        //        Int32 RecAfct = default(Int32);
        //        System.Data.OracleClient.OracleCommand insCommand = new System.Data.OracleClient.OracleCommand();
        //        string StrSql = null;
        //        try
        //        {
        //            objWK.OpenConnection();
        //            TRAN = objWK.MyConnection.BeginTransaction();
        //            var _with42 = insCommand;
        //            _with42.Connection = objWK.MyConnection;
        //            _with42.CommandType = CommandType.StoredProcedure;
        //            _with42.CommandText = objWK.MyUserName + ".RESTRICTION_APPROVAL_TBL_PKG.RESTRICTION_APPROVAL_TBL_INS";
        //            var _with43 = _with42.Parameters;
        //            insCommand.Parameters.Add("RESTRICTION_TRN_FK_IN", RestrictionFK).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("FORM_TYPE_IN", FormType).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("REFERENCE_MST_FK_IN", RefPK).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("REFERENCE_NR_IN", RefNr).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("REFERENCE_DT_IN", RefDate).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("CUSTOMER_MST_FK_IN", (CustPK == 0 ? DBNull.Value : CustPK)).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("STATUS_FK_IN", 1).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("REMARKS_IN", DBNull.Value).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("CREATED_BY_FK_IN", CreatedPK).Direction = ParameterDirection.Input;
        //            insCommand.Parameters.Add("RETURN_VALUE", System.Data.OracleClient.OracleType.VarChar, 500, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //            insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

        //            var _with44 = objWK.MyDataAdapter;
        //            _with44.InsertCommand = insCommand;
        //            _with44.InsertCommand.Transaction = TRAN;
        //            RecAfct = _with44.InsertCommand.ExecuteNonQuery();
        //            intPKVal = Conversion.Val(objWK.MyDataAdapter.InsertCommand.Parameters("RETURN_VALUE").Value);
        //            if (arrMessage.Count > 0)
        //            {
        //                TRAN.Rollback();
        //            }
        //            else
        //            {
        //                TRAN.Commit();
        //                arrMessage.Add("All Data Saved Successfully");
        //            }
        //            return arrMessage;
        //        }
        //        catch (OracleClient.OracleException oraexp)
        //        {
        //            throw oraexp;
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //        finally
        //        {
        //            objWK.CloseConnection();
        //        }
        //    }
        //    #endregion

        //    #region "Profit Margin related Validation"
        //    public string ValidateProfitMargin(short RefFlag, UInt64 RefPk, int LocationMstFk = 0, ref short POLICY = 0)
        //    {
        //        WorkFlow objWf = new WorkFlow();
        //        cls_ApprovalRequests objMsg = new cls_ApprovalRequests();
        //        //RefFlag:1-srr sea,2-srr air,3-quotation,4-booking
        //        short BizType = 0;
        //        short CargoType = 0;
        //        string _query = "";
        //        string _message = "";
        //        string _module = "";
        //        decimal _margin = 0;
        //        short _policy = 0;
        //        short _profitMarginStatus = 0;
        //        int _createdByFk = 0;
        //        int _locationFk = Convert.ToInt32(Session["LOGED_IN_LOC_FK"]);
        //        DataTable dtDocDetails = new DataTable();

        //        try
        //        {
        //            if (LocationMstFk > 0)
        //            {
        //                _locationFk = LocationMstFk;
        //            }
        //            Session["REF_TYPE_FLAG"] = RefFlag;
        //            //SRR SEA
        //            if (RefFlag == 1)
        //            {
        //                _module = "05RatingandTariff/frmSRRSeaApproval.aspx?SRRPk=" + RefPk + "&frompage=Approval";
        //                //_query = "SELECT S.SRR_REF_NO REF_NO,2 BIZ_TYPE,NVL(S.CARGO_TYPE,0) CARGO_TYPE,NVL(S.PROFIT_AMT,0)PROFIT_AMT,NVL(S.PROFIT_PERCENTAGE,0)PROFIT_PERCENTAGE,S.CREATED_BY_FK,NVL(U.DEFAULT_LOCATION_FK," & Session("LOGED_IN_LOC_FK") & ") DOC_LOC_FK,NVL(S.PROFIT_MARGIN_STATUS,0) MARGIN_STATUS FROM SRR_SEA_TBL S,USER_MST_TBL U WHERE S.CREATED_BY_FK=U.USER_MST_PK(+) AND S.SRR_SEA_PK=" & RefPk
        //                //SRR AIR
        //            }
        //            else if (RefFlag == 2)
        //            {
        //                _module = "05RatingandTariff/frmSRRAirApproval.aspx?SRRPk=" + RefPk + "&frompage=Approval";
        //                //_query = "SELECT A.SRR_REF_NO REF_NO,1 BIZ_TYPE,0 CARGO_TYPE,NVL(A.PROFIT_AMT,0)PROFIT_AMT,NVL(A.PROFIT_PERCENTAGE,0)PROFIT_PERCENTAGE,A.CREATED_BY_FK,NVL(U.DEFAULT_LOCATION_FK," & Session("LOGED_IN_LOC_FK") & ") DOC_LOC_FK,NVL(A.PROFIT_MARGIN_STATUS,0) MARGIN_STATUS FROM SRR_AIR_TBL A,USER_MST_TBL U WHERE A.CREATED_BY_FK=U.USER_MST_PK(+) AND A.SRR_AIR_PK=" & RefPk
        //                //QUOTATION
        //            }
        //            else if (RefFlag == 3)
        //            {
        //                _module = "05RatingandTariff/frmQuotationEntry.aspx?pk=" + RefPk + "&frompage=approval";
        //                //_query = "SELECT Q.QUOTATION_REF_NO REF_NO,Q.BIZ_TYPE,NVL(Q.CARGO_TYPE,0) CARGO_TYPE,NVL(Q.PROFIT_AMT,0)PROFIT_AMT,NVL(Q.PROFIT_PERCENTAGE,0)PROFIT_PERCENTAGE,Q.CREATED_BY_FK,NVL(U.DEFAULT_LOCATION_FK," & Session("LOGED_IN_LOC_FK") & ") DOC_LOC_FK,NVL(Q.PROFIT_MARGIN_STATUS,0) MARGIN_STATUS FROM QUOTATION_MST_TBL Q,USER_MST_TBL U WHERE Q.CREATED_BY_FK=U.USER_MST_PK(+) AND Q.QUOTATION_MST_PK=" & RefPk
        //                //BOOKING
        //            }
        //            else if (RefFlag == 4)
        //            {
        //                _module = "06BookingExports/frmBookingEntry.aspx?BKGPK=" + RefPk;
        //                //_query = "SELECT B.BOOKING_REF_NO REF_NO,B.BUSINESS_TYPE BIZ_TYPE,NVL(B.CARGO_TYPE,0) CARGO_TYPE,NVL(B.PROFIT_AMT,0)PROFIT_AMT,NVL(B.PROFIT_PERCENTAGE,0)PROFIT_PERCENTAGE,B.CREATED_BY_FK,NVL(U.DEFAULT_LOCATION_FK," & Session("LOGED_IN_LOC_FK") & ") DOC_LOC_FK,NVL(B.PROFIT_MARGIN_STATUS,0) MARGIN_STATUS FROM BOOKING_MST_TBL B,USER_MST_TBL U WHERE B.CREATED_BY_FK=U.USER_MST_PK(+) AND B.BOOKING_MST_PK=" & RefPk
        //            }

        //            dtDocDetails = GetBasicRefDetails(RefFlag, RefPk);
        //            //objWf.GetDataTable(_query)

        //            BizType = dtDocDetails.Rows[0]["BIZ_TYPE"];
        //            CargoType = dtDocDetails.Rows[0]["CARGO_TYPE"];
        //            _profitMarginStatus = dtDocDetails.Rows[0]["MARGIN_STATUS"];
        //            _createdByFk = dtDocDetails.Rows[0]["CREATED_BY_FK"];

        //            if (_profitMarginStatus == 3)
        //            {
        //                //PROFIT MARGIN STATUS IS REJECTED
        //                _message = "Profit Margin Status is Rejected, cannot proceed for save!";
        //                if (RefFlag == 1)
        //                {
        //                    _message = "Profit Margin Status is Rejected for this SRR!";
        //                }
        //                else if (RefFlag == 2)
        //                {
        //                    _message = "Profit Margin Status is Rejected for this SRR!";
        //                }
        //                else if (RefFlag == 3)
        //                {
        //                    _message = "Profit Margin Status is Rejected for this Quotation!";
        //                }
        //                else if (RefFlag == 4)
        //                {
        //                    _message = "Profit Margin Status is Rejected for this Booking!";
        //                }
        //                return _message;
        //            }

        //            _query = "SELECT FN_GET_LOC_PROFIT_MARGIN(" + _locationFk + ", " + BizType + "," + CargoType + ") FROM DUAL";
        //            _margin = Convert.ToDecimal(objWf.ExecuteScaler(_query));
        //            _query = "SELECT FN_GET_LOC_PROFIT_MARGIN(" + _locationFk + ", " + BizType + "," + CargoType + ",2) FROM DUAL";
        //            _policy = Convert.ToDecimal(objWf.ExecuteScaler(_query));

        //            if (Convert.ToDecimal(dtDocDetails.Rows[0]["PROFIT_PERCENTAGE"]) < _margin & _policy == 2 & _profitMarginStatus == 1 & !IsWorFlowApprover(Session["USER_PK"], dtDocDetails.Rows[0]["DOC_LOC_FK"], "DOC000575"))
        //            {
        //                _message = "Profit Margin Approval is Pending for this document!";
        //                return _message;
        //            }

        //            if (RefFlag == 1)
        //            {
        //                _module += "&IsLCL=" + (CargoType == 2 ? 1 : 0);
        //            }
        //            else if (RefFlag == 3)
        //            {
        //                _module += "&CargoType=" + (BizType == 2 ? (CargoType == 1 ? "FCL" : (CargoType == 2 ? "LCL" : "BBC")) : "") + "&BizType=" + (BizType == 2 ? "Sea" : "Air");
        //            }
        //            else if (RefFlag == 4)
        //            {
        //                _module += "&BizType=" + BizType + "&ProcessType=1&CargoType=" + (BizType == 2 ? (CargoType == 1 ? "FCL" : (CargoType == 2 ? "LCL" : "BBC")) : (CargoType == 1 ? "KGS" : "ULD"));
        //            }
        //            if (Convert.ToDecimal(dtDocDetails.Rows[0]["PROFIT_PERCENTAGE"]) < _margin)
        //            {
        //                //Alert & Proceed
        //                if (_policy == 1)
        //                {

        //                    //Approve & Proceed
        //                }
        //                else if (_policy == 2)
        //                {
        //                    if (_profitMarginStatus == 0)
        //                    {
        //                        _message = objMsg.SendMessageNew(Session["USER_PK"], RefPk, "PROFIT MARGIN APPROVAL REQUEST", Convert.ToInt64(Session["LOGED_IN_LOC_FK"]), Session["USER_PK"], _module);
        //                        UpdateProfitMarginStatus(RefFlag, RefPk, 1);
        //                        _message = "Mail has been sent to the approver! Once he approves, then only you can confirm the document!";
        //                    }
        //                    else if (_profitMarginStatus == 1)
        //                    {
        //                        _message = objMsg.SendMessageNew(Session["USER_PK"], RefPk, "PROFIT MARGIN APPROVAL", Convert.ToInt64(Session["LOGED_IN_LOC_FK"]), Session["USER_PK"], _module, , , , ,


        //                        , 1, _createdByFk);
        //                        UpdateProfitMarginStatus(RefFlag, RefPk, 2);
        //                        _message = "Profit Margin Approved! Mail has been sent!";
        //                    }
        //                    //Block
        //                }
        //                else if (_policy == 3)
        //                {
        //                    _message = "Transaction Blocked due to low profit!";
        //                }
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            throw ex;
        //        }
        //        return _message;
        //    }
        //    public DataTable GetBasicRefDetails(short RefFlag, UInt64 RefPk)
        //    {
        //        WorkFlow objWf = new WorkFlow();
        //        DataTable dtRef = new DataTable();
        //        int Default_loc = 0;
        //        if ((Session["LOGED_IN_LOC_FK"] != null))
        //        {
        //            Default_loc = Session["LOGED_IN_LOC_FK"];
        //        }
        //        string _query = "";
        //        //SRR SEA
        //        if (RefFlag == 1)
        //        {
        //            _query = "SELECT S.SRR_REF_NO REF_NO,2 BIZ_TYPE,NVL(S.CARGO_TYPE,0) CARGO_TYPE,NVL(S.PROFIT_AMT,0)PROFIT_AMT,NVL(S.PROFIT_PERCENTAGE,0)PROFIT_PERCENTAGE,S.CREATED_BY_FK,NVL(U.DEFAULT_LOCATION_FK," + Default_loc + ") DOC_LOC_FK,NVL(S.PROFIT_MARGIN_STATUS,0) MARGIN_STATUS FROM SRR_SEA_TBL S,USER_MST_TBL U WHERE S.CREATED_BY_FK=U.USER_MST_PK(+) AND S.SRR_SEA_PK=" + RefPk;
        //            //SRR AIR
        //        }
        //        else if (RefFlag == 2)
        //        {
        //            _query = "SELECT A.SRR_REF_NO REF_NO,1 BIZ_TYPE,0 CARGO_TYPE,NVL(A.PROFIT_AMT,0)PROFIT_AMT,NVL(A.PROFIT_PERCENTAGE,0)PROFIT_PERCENTAGE,A.CREATED_BY_FK,NVL(U.DEFAULT_LOCATION_FK," + Default_loc + ") DOC_LOC_FK,NVL(A.PROFIT_MARGIN_STATUS,0) MARGIN_STATUS FROM SRR_AIR_TBL A,USER_MST_TBL U WHERE A.CREATED_BY_FK=U.USER_MST_PK(+) AND A.SRR_AIR_PK=" + RefPk;
        //            //QUOTATION
        //        }
        //        else if (RefFlag == 3)
        //        {
        //            _query = "SELECT Q.QUOTATION_REF_NO REF_NO,Q.BIZ_TYPE,NVL(Q.CARGO_TYPE,0) CARGO_TYPE,NVL(Q.PROFIT_AMT,0)PROFIT_AMT,NVL(Q.PROFIT_PERCENTAGE,0)PROFIT_PERCENTAGE,Q.CREATED_BY_FK,NVL(U.DEFAULT_LOCATION_FK," + Default_loc + ") DOC_LOC_FK,NVL(Q.PROFIT_MARGIN_STATUS,0) MARGIN_STATUS FROM QUOTATION_MST_TBL Q,USER_MST_TBL U WHERE Q.CREATED_BY_FK=U.USER_MST_PK(+) AND Q.QUOTATION_MST_PK=" + RefPk;
        //            //BOOKING
        //        }
        //        else if (RefFlag == 4)
        //        {
        //            _query = "SELECT B.BOOKING_REF_NO REF_NO,B.BUSINESS_TYPE BIZ_TYPE,NVL(B.CARGO_TYPE,0) CARGO_TYPE,NVL(B.PROFIT_AMT,0)PROFIT_AMT,NVL(B.PROFIT_PERCENTAGE,0)PROFIT_PERCENTAGE,B.CREATED_BY_FK,NVL(U.DEFAULT_LOCATION_FK," + Default_loc + ") DOC_LOC_FK,NVL(B.PROFIT_MARGIN_STATUS,0) MARGIN_STATUS FROM BOOKING_MST_TBL B,USER_MST_TBL U WHERE B.CREATED_BY_FK=U.USER_MST_PK(+) AND B.BOOKING_MST_PK=" + RefPk;
        //            //JOBCARD
        //        }
        //        else if (RefFlag == 5)
        //        {
        //            _query = "SELECT C.JOBCARD_REF_NO REF_NO, C.BUSINESS_TYPE BIZ_TYPE,C.PROCESS_TYPE, NVL(C.CARGO_TYPE, 0) CARGO_TYPE, NVL(C.STATUS,0) STATUS, C.CREATED_BY_FK, NVL(U.DEFAULT_LOCATION_FK, " + Default_loc + ") DOC_LOC_FK FROM JOB_CARD_TRN C, USER_MST_TBL U WHERE C.CREATED_BY_FK = U.USER_MST_PK(+) AND C.JOB_CARD_TRN_PK=" + RefPk;
        //            //CBJC
        //        }
        //        else if (RefFlag == 6)
        //        {
        //            _query = "SELECT C.CBJC_NO REF_NO, C.BIZ_TYPE BIZ_TYPE, C.PROCESS_TYPE, NVL(C.CARGO_TYPE, 0) CARGO_TYPE, NVL(C.CBJC_STATUS,0) STATUS, C.CREATED_BY_FK, NVL(U.DEFAULT_LOCATION_FK, " + Default_loc + ") DOC_LOC_FK FROM CBJC_TBL C, USER_MST_TBL U WHERE C.CREATED_BY_FK = U.USER_MST_PK(+) AND C.CBJC_PK=" + RefPk;
        //            //TRANSPORTER NOTE
        //        }
        //        else if (RefFlag == 7)
        //        {
        //            _query = "SELECT C.TRANS_INST_REF_NO REF_NO, C.BUSINESS_TYPE BIZ_TYPE, C.PROCESS_TYPE, NVL(C.CARGO_TYPE, 0) CARGO_TYPE, NVL(C.TP_STATUS,0) STATUS, C.CREATED_BY_FK,NVL(U.DEFAULT_LOCATION_FK, " + Default_loc + ") DOC_LOC_FK FROM TRANSPORT_INST_SEA_TBL C, USER_MST_TBL U WHERE C.CREATED_BY_FK = U.USER_MST_PK(+) AND C.TRANSPORT_INST_SEA_PK=" + RefPk;
        //        }
        //        try
        //        {
        //            dtRef = objWf.GetDataTable(_query);
        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //        return dtRef;
        //    }
        //    public bool UpdateProfitMarginStatus(short RefFlag, UInt64 RefPk, short Status)
        //    {
        //        WorkFlow objWf = new WorkFlow();
        //        string _query = "";
        //        if (RefFlag == 1)
        //        {
        //            _query = "UPDATE SRR_SEA_TBL S SET S.PROFIT_MARGIN_STATUS=" + Status + " WHERE S.SRR_SEA_PK=" + RefPk;
        //        }
        //        else if (RefFlag == 2)
        //        {
        //            _query = "UPDATE SRR_AIR_TBL A SET A.PROFIT_MARGIN_STATUS=" + Status + " WHERE A.SRR_AIR_PK=" + RefPk;
        //        }
        //        else if (RefFlag == 3)
        //        {
        //            _query = "UPDATE QUOTATION_MST_TBL Q SET Q.PROFIT_MARGIN_STATUS=" + Status + " WHERE Q.QUOTATION_MST_PK=" + RefPk;
        //        }
        //        else if (RefFlag == 4)
        //        {
        //            _query = "UPDATE BOOKING_MST_TBL B SET B.PROFIT_MARGIN_STATUS=" + Status + " WHERE B.BOOKING_MST_PK=" + RefPk;
        //        }

        //        if (!string.IsNullOrEmpty(_query))
        //        {
        //            objWf.ExecuteCommands(_query);
        //        }
        //    }
        //    public bool IsWorFlowApprover(int UserMstFk, int LocationMstFk, string DocumentID)
        //    {
        //        long documentPK = 0;
        //        DataTable dtt = null;
        //        DataSet objMsg = null;
        //        string UserFK = null;
        //        string Page_URL = null;
        //        DataSet objUsr = null;
        //        DataTable objSender = null;
        //        string strsql = null;
        //        string strMsgSub = null;
        //        string strMsgBody = null;
        //        string UsrName = null;
        //        string LocName = null;
        //        string SenderUsrName = null;
        //        string SenderLocName = null;
        //        int Colcnt = 0;
        //        bool isValidApprover = false;
        //        int _userFk = UserMstFk;
        //        if (_userFk <= 0)
        //            _userFk = Session["USER_PK"];

        //        //DOC000575 FOR PROFIT MARGIN APPROVAL REQUEST
        //        //DOC000576 FOR PROFIT MARGIN APPROVAL
        //        try
        //        {
        //            dtt = FetchDocument(DocumentID);
        //            documentPK = Convert.ToInt64(dtt.Rows[0][0]);
        //            WorkFlow objWF = new WorkFlow();
        //            objMsg = GetApproverDtls(documentPK, (LocationMstFk == 0 ? Session["LOGED_IN_LOC_FK"] : LocationMstFk));
        //            if (objMsg.Tables[0].Rows.Count > 0)
        //            {
        //                for (Colcnt = 0; Colcnt <= objMsg.Tables[0].Columns.Count - 1; Colcnt++)
        //                {
        //                    if (((!object.ReferenceEquals(objMsg.Tables[0].Rows[0][Colcnt], System.DBNull.Value))))
        //                    {
        //                        if (isValidApprover == false)
        //                        {
        //                            if (objMsg.Tables[0].Rows[0][Colcnt] == _userFk)
        //                            {
        //                                isValidApprover = true;
        //                                break; // TODO: might not be correct. Was : Exit For
        //                            }
        //                        }
        //                    }
        //                }
        //                if (isValidApprover == false)
        //                {
        //                    return false;
        //                }
        //                else
        //                {
        //                    return true;
        //                }
        //            }
        //            else
        //            {
        //                return false;
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //        }
        //        return false;
        //    }

        //    #endregion


        //    #region " GetDataByUsingPkvalues "
        //    public DataSet GetDataByUsingPkvalues(int PkValue, string FLAG = "")
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWF = new WorkFlow();
        //        if (FLAG == "POL" | FLAG == "POD")
        //        {
        //            sb.Append(" SELECT P.PORT_MST_PK,");
        //            sb.Append(" P.PORT_ID, ");
        //            sb.Append(" P.PORT_NAME");
        //            sb.Append(" FROM PORT_MST_TBL P ");
        //            sb.Append(" WHERE P.PORT_MST_PK =  " + PkValue);
        //        }
        //        else if (FLAG == "SLine")
        //        {
        //            sb.Append(" SELECT O.OPERATOR_MST_PK, ");
        //            sb.Append(" O.OPERATOR_ID, ");
        //            sb.Append(" O.OPERATOR_NAME");
        //            sb.Append(" FROM OPERATOR_MST_TBL O");
        //            sb.Append("  WHERE O.OPERATOR_MST_PK = " + PkValue);
        //        }
        //        else if (FLAG == "AirLine")
        //        {
        //            sb.Append("  SELECT A.AIRLINE_MST_PK,");
        //            sb.Append("   A.AIRLINE_ID,");
        //            sb.Append("    A.AIRLINE_NAME");
        //            sb.Append("    FROM AIRLINE_MST_TBL A");
        //            sb.Append("   WHERE A.AIRLINE_MST_PK =" + PkValue);
        //        }
        //        else if (FLAG == "CUST")
        //        {
        //            sb.Append("   SELECT C.CUSTOMER_MST_PK,");
        //            sb.Append("    C.CUSTOMER_ID, ");
        //            sb.Append("    C.CUSTOMER_NAME");
        //            sb.Append("  FROM CUSTOMER_MST_TBL C");
        //            sb.Append(" WHERE C.CUSTOMER_MST_PK = " + PkValue);
        //        }
        //        else if (FLAG == "Agent")
        //        {
        //            sb.Append("   SELECT A.AGENT_MST_PK,");
        //            sb.Append("    A.AGENT_ID, ");
        //            sb.Append("    A.AGENT_NAME");
        //            sb.Append("  FROM AGENT_MST_TBL A ");
        //            sb.Append(" WHERE A.AGENT_MST_PK = " + PkValue);
        //        }
        //        try
        //        {
        //            return objWF.GetDataSet(sb.ToString());
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    public Int32 FetchAccessrights(int ConfigPk)
        //    {
        //        System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
        //        WorkFlow objWF = new WorkFlow();
        //        Int32 Count = 0;
        //        try
        //        {
        //            sb.Append(" SELECT COUNT(*) FROM USER_ACCESS_TRN U ");
        //            sb.Append(" WHERE U.CONFIG_MST_FK = " + ConfigPk);
        //            sb.Append(" AND U.USER_MST_FK = " + Session["USER_PK"]);
        //            Count = cc
        //            if (Count > 0)
        //            {
        //                sb.Clear();
        //                sb.Append(" SELECT U.ALLOWED_OPERATIONS_VALUE FROM USER_ACCESS_TRN U ");
        //                sb.Append(" WHERE U.CONFIG_MST_FK = " + ConfigPk);
        //                sb.Append(" AND U.USER_MST_FK = " + Session["USER_PK"]);
        //                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
        //            }
        //            else
        //            {
        //                return 0;
        //            }
        //        }
        //        catch (OracleException sqlExp)
        //        {
        //            throw sqlExp;
        //        }
        //        catch (Exception exp)
        //        {
        //            throw exp;
        //        }
        //    }
        //    #endregion

        //    ///'Replace this <<....>> value to invoice date,invoice no etc... 

        //#endregion Added New On 08-08-2016
    }
}