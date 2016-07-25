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
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    #region "Class Variables"

    public class clsSurveyEntry : CommonFeatures
    {
        private WorkFlow objWF = new WorkFlow();

        //Protected arrMessage As New ArrayList
        public DataSet FetchSurveyGrid(long SurveyPk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".SURVEY_INFO_PKG.SURVEY_INFO_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand.Parameters;
                _with2.Add("SURVEY_MST_PK_IN", SurveyPk).Direction = ParameterDirection.Input;
                _with2.Add("FET_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet FetchSurvey_Enquiry(long EnqPk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with3 = objWK.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".SURVEY_INFO_PKG.SURVEY_MAST_ENQ";

                objWK.MyCommand.Parameters.Clear();
                var _with4 = objWK.MyCommand.Parameters;
                _with4.Add("ENQUIRY_MST_FK_IN", EnqPk).Direction = ParameterDirection.Input;
                _with4.Add("FET_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
            }
        }

        public DataSet FetchSurveyMater(long SurveyPk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with5 = objWK.MyCommand;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".SURVEY_INFO_PKG.FETCH_SURVEY_MASTER";

                objWK.MyCommand.Parameters.Clear();
                var _with6 = objWK.MyCommand.Parameters;
                _with6.Add("SURVEY_MST_PK_IN", SurveyPk).Direction = ParameterDirection.Input;
                _with6.Add("FET_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        

        #region "Save"

        public ArrayList saveHdr(DataSet gridDs, Int64 SurveyPK, string surveyNr, Int64 EnquiryPk, string SurveyExec, Int64 Partypk, Int64 plrpk, string pickADD1, string pickadd2, string pickcity,
        string pickzip, Int64 pickcontryfk, Int64 pfdfk, string pfdadd1, string pfdadd2, string pfdcity, string pfdzip, Int64 pfdcountryfk, string movedt, string reldeldt,
        string surveydt, Int32 movetype, string servicetype, string notes, Int64 PickupElev, Int64 delElev, string totAmt, string surveypks)
        {
            WorkFlow objWK = new WorkFlow();
            long surveyHeadperPk = 0;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleTransaction insertTrans = null;
            double totarea = 0;

            if (totAmt != null)
            {
                totarea = Convert.ToDouble(totAmt);
            }

            try
            {
                if (SurveyPK == 0)
                {
                    objWK.OpenConnection();
                    objWK.MyCommand.Connection = objWK.MyConnection;
                    insertTrans = objWK.MyConnection.BeginTransaction();

                    var _with7 = insCommand;
                    _with7.Connection = objWK.MyConnection;
                    _with7.CommandType = CommandType.StoredProcedure;
                    _with7.CommandText = objWK.MyUserName + ".SURVEY_INFO_PKG.SURVEY_MST_TBL_SAVE";
                    insCommand.Parameters.Clear();
                    var _with8 = _with7.Parameters;

                    insCommand.Parameters.Add("SURVEY_NR_IN", surveyNr).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(SurveyExec))
                    {
                        _with8.Add("SURVEY_EXEC_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with8.Add("SURVEY_EXEC_IN", getDefault(SurveyExec, "")).Direction = ParameterDirection.Input;
                    }

                    if (EnquiryPk == 0)
                    {
                        insCommand.Parameters.Add("ENQUIRY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("ENQUIRY_MST_FK_IN", getDefault(EnquiryPk, "")).Direction = ParameterDirection.Input;
                    }

                    if (Partypk == 0)
                    {
                        insCommand.Parameters.Add("PARTY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PARTY_MST_FK_IN", getDefault(Partypk, "")).Direction = ParameterDirection.Input;
                    }

                    if (plrpk == 0)
                    {
                        insCommand.Parameters.Add("PLR_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PLR_FK_IN", getDefault(plrpk, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pickADD1))
                    {
                        insCommand.Parameters.Add("PLR_ADD1_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PLR_ADD1_IN", getDefault(pickADD1, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pickadd2))
                    {
                        insCommand.Parameters.Add("PLR_ADD2_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PLR_ADD2_IN", getDefault(pickadd2, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pickcity))
                    {
                        insCommand.Parameters.Add("PLR_CITY_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PLR_CITY_IN", getDefault(pickcity, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pickzip))
                    {
                        insCommand.Parameters.Add("PLR_ZIP_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PLR_ZIP_IN", getDefault(pickzip, "")).Direction = ParameterDirection.Input;
                    }

                    if (pickcontryfk == 0)
                    {
                        insCommand.Parameters.Add("PLR_COUNTRY_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PLR_COUNTRY_FK_IN", getDefault(pickcontryfk, "")).Direction = ParameterDirection.Input;
                    }

                    if (pfdfk == 0)
                    {
                        insCommand.Parameters.Add("PFD_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PFD_FK_IN", getDefault(pfdfk, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pfdadd1))
                    {
                        _with8.Add("PFD_ADD1_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with8.Add("PFD_ADD1_IN", getDefault(pfdadd1, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pfdadd2))
                    {
                        _with8.Add("PFD_ADD2_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with8.Add("PFD_ADD2_IN", getDefault(pfdadd2, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pfdcity))
                    {
                        _with8.Add("PFD_CITY_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with8.Add("PFD_CITY_IN", getDefault(pfdcity, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pfdzip))
                    {
                        _with8.Add("PFD_ZIP_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with8.Add("PFD_ZIP_IN", getDefault(pfdzip, "")).Direction = ParameterDirection.Input;
                    }

                    if (pfdcountryfk == 0)
                    {
                        insCommand.Parameters.Add("PFD_COUNTRY_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PFD_COUNTRY_FK_IN", getDefault(pfdcountryfk, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(movedt.Trim()))
                    {
                        insCommand.Parameters.Add("MVMT_DT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("MVMT_DT_IN", getDefault(movedt, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(reldeldt.Trim()))
                    {
                        insCommand.Parameters.Add("DEL_DT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("DEL_DT_IN", getDefault(reldeldt, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(surveydt.Trim()))
                    {
                        insCommand.Parameters.Add("SURVEY_DT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("SURVEY_DT_IN", getDefault(surveydt, "")).Direction = ParameterDirection.Input;
                    }

                    insCommand.Parameters.Add("MOVE_TYPE_IN", movetype).Direction = ParameterDirection.Input;

                    if (string.IsNullOrEmpty(servicetype))
                    {
                        insCommand.Parameters.Add("SERVICE_TYPE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("SERVICE_TYPE_IN", getDefault(servicetype, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(notes))
                    {
                        insCommand.Parameters.Add("NOTES_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("NOTES_IN", getDefault(notes, "")).Direction = ParameterDirection.Input;
                    }

                    if (PickupElev == 0)
                    {
                        insCommand.Parameters.Add("PICK_UP_ELEVAT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("PICK_UP_ELEVAT_IN", getDefault(PickupElev, "")).Direction = ParameterDirection.Input;
                    }

                    if (delElev == 0)
                    {
                        insCommand.Parameters.Add("DEL_ELEVAT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("DEL_ELEVAT_IN", getDefault(delElev, "")).Direction = ParameterDirection.Input;
                    }

                    if (totarea == 0)
                    {
                        insCommand.Parameters.Add("TOT_AREA_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        insCommand.Parameters.Add("TOT_AREA_IN", getDefault(totarea, "")).Direction = ParameterDirection.Input;
                    }

                    insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    insCommand.Parameters.Add("CREATED_DT_IN", DateTime.Now.Date).Direction = ParameterDirection.Input;

                    insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                    insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    var _with9 = objWK.MyDataAdapter;
                    _with9.InsertCommand = insCommand;
                    _with9.InsertCommand.Transaction = insertTrans;
                    _with9.InsertCommand.ExecuteNonQuery();
                    surveyHeadperPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    arrMessage = savedtl(gridDs, surveyHeadperPk, insertTrans, surveypks);
                    //adding by thiyagarajan on 22/1/09:TrackNTrace Task:VEK Req.
                    SaveInTrackNTrace(surveyNr, Convert.ToInt32(EnquiryPk), "Project Survey Completed", 2, insertTrans);
                    //end
                    if (arrMessage.Count == 1)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "Saved")>0)
                        {
                            arrMessage.Add(surveyHeadperPk);
                        }
                    }
                    insertTrans.Commit();
                    return arrMessage;
                }
                else
                {
                    objWK.OpenConnection();
                    objWK.MyCommand.Connection = objWK.MyConnection;
                    insertTrans = objWK.MyConnection.BeginTransaction();
                    var _with10 = updCommand;
                    _with10.Connection = objWK.MyConnection;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = objWK.MyUserName + ".SURVEY_INFO_PKG.SURVEY_MST_TBL_UPD";
                    updCommand.Parameters.Clear();
                    var _with11 = _with10.Parameters;
                    updCommand.Parameters.Add("SURVEY_MST_PK_IN", SurveyPK).Direction = ParameterDirection.Input;
                    updCommand.Parameters.Add("SURVEY_NR_IN", surveyNr).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(SurveyExec))
                    {
                        _with11.Add("SURVEY_EXEC_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with11.Add("SURVEY_EXEC_IN", getDefault(SurveyExec, "")).Direction = ParameterDirection.Input;
                    }

                    if (EnquiryPk == 0)
                    {
                        updCommand.Parameters.Add("ENQUIRY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("ENQUIRY_MST_FK_IN", getDefault(EnquiryPk, "")).Direction = ParameterDirection.Input;
                    }

                    if (Partypk == 0)
                    {
                        updCommand.Parameters.Add("PARTY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PARTY_MST_FK_IN", getDefault(Partypk, "")).Direction = ParameterDirection.Input;
                    }

                    if (plrpk == 0)
                    {
                        updCommand.Parameters.Add("PLR_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PLR_FK_IN", getDefault(plrpk, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pickADD1))
                    {
                        updCommand.Parameters.Add("PLR_ADD1_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PLR_ADD1_IN", getDefault(pickADD1, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pickADD1))
                    {
                        updCommand.Parameters.Add("PLR_ADD2_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PLR_ADD2_IN", getDefault(pickadd2, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pickcity))
                    {
                        updCommand.Parameters.Add("PLR_CITY_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PLR_CITY_IN", getDefault(pickcity, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pickzip))
                    {
                        updCommand.Parameters.Add("PLR_ZIP_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PLR_ZIP_IN", getDefault(pickzip, "")).Direction = ParameterDirection.Input;
                    }

                    if (pickcontryfk == 0)
                    {
                        updCommand.Parameters.Add("PLR_COUNTRY_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PLR_COUNTRY_FK_IN", getDefault(pickcontryfk, "")).Direction = ParameterDirection.Input;
                    }

                    if (pfdfk == 0)
                    {
                        updCommand.Parameters.Add("PFD_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PFD_FK_IN", getDefault(pfdfk, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pfdadd1))
                    {
                        _with11.Add("PFD_ADD1_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with11.Add("PFD_ADD1_IN", getDefault(pfdadd1, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pfdadd2))
                    {
                        _with11.Add("PFD_ADD2_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with11.Add("PFD_ADD2_IN", getDefault(pfdadd2, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pfdcity))
                    {
                        _with11.Add("PFD_CITY_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with11.Add("PFD_CITY_IN", getDefault(pfdcity, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(pfdzip))
                    {
                        _with11.Add("PFD_ZIP_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with11.Add("PFD_ZIP_IN", getDefault(pfdzip, "")).Direction = ParameterDirection.Input;
                    }

                    if (pfdcountryfk == 0)
                    {
                        updCommand.Parameters.Add("PFD_COUNTRY_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PFD_COUNTRY_FK_IN", getDefault(pfdcountryfk, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(movedt.Trim()))
                    {
                        updCommand.Parameters.Add("MVMT_DT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("MVMT_DT_IN", getDefault(movedt, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(reldeldt.Trim()))
                    {
                        updCommand.Parameters.Add("DEL_DT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("DEL_DT_IN", getDefault(reldeldt, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(surveydt.Trim()))
                    {
                        updCommand.Parameters.Add("SURVEY_DT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("SURVEY_DT_IN", getDefault(surveydt, "")).Direction = ParameterDirection.Input;
                    }

                    updCommand.Parameters.Add("MOVE_TYPE_IN", movetype).Direction = ParameterDirection.Input;

                    if (string.IsNullOrEmpty(servicetype))
                    {
                        updCommand.Parameters.Add("SERVICE_TYPE_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("SERVICE_TYPE_IN", getDefault(servicetype, "")).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(notes))
                    {
                        updCommand.Parameters.Add("NOTES_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("NOTES_IN", getDefault(notes, "")).Direction = ParameterDirection.Input;
                    }

                    if (PickupElev == 0)
                    {
                        updCommand.Parameters.Add("PICK_UP_ELEVAT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("PICK_UP_ELEVAT_IN", getDefault(PickupElev, "")).Direction = ParameterDirection.Input;
                    }

                    if (delElev == 0)
                    {
                        updCommand.Parameters.Add("DEL_ELEVAT_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("DEL_ELEVAT_IN", getDefault(delElev, "")).Direction = ParameterDirection.Input;
                    }

                    if (totarea == 0)
                    {
                        updCommand.Parameters.Add("TOT_AREA_IN", "").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        updCommand.Parameters.Add("TOT_AREA_IN", getDefault(totarea, "")).Direction = ParameterDirection.Input;
                    }

                    updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                    updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                    updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "SURVEY_MST_PK").Direction = ParameterDirection.Output;

                    var _with12 = objWK.MyDataAdapter;
                    _with12.UpdateCommand = updCommand;
                    _with12.UpdateCommand.Transaction = insertTrans;
                    _with12.UpdateCommand.ExecuteNonQuery();
                    surveyHeadperPk = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
                    arrMessage = savedtl(gridDs, surveyHeadperPk, insertTrans, surveypks);
                    if (arrMessage.Count == 1)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "Saved")>0)
                        {
                            arrMessage.Add(surveyHeadperPk);
                            insertTrans.Commit();
                        }
                    }
                    else
                    {
                        insertTrans.Rollback();
                    }
                    return arrMessage;
                }
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                insertTrans.Rollback();
                if (SurveyPK == 0)
                {
                    RollbackProtocolKey("REMOVAL SURVEY", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), surveyNr, System.DateTime.Now);
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
                //added by suryaprasad for implementing session mgmt
            }
        }

        //adding by thiyagarajan on 22/1/09:TrackNTrace Task:VEK Req.
        public void SaveInTrackNTrace(string refno, Int32 refpk, string status, Int32 Doctype, OracleTransaction TRAN)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            Int32 Return_value = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            try
            {
                objWF.OpenConnection();
                objWF.MyConnection = TRAN.Connection;
                insCommand.Connection = objWF.MyConnection;
                insCommand.Transaction = TRAN;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.CommandText = objWF.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
                var _with13 = insCommand.Parameters;
                _with13.Clear();
                _with13.Add("REF_NO_IN", refno).Direction = ParameterDirection.Input;
                _with13.Add("REF_FK_IN", refpk).Direction = ParameterDirection.Input;
                _with13.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with13.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
                _with13.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with13.Add("DOCTYPE_IN", Doctype).Direction = ParameterDirection.Input;
                insCommand.ExecuteNonQuery();
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //end

        public ArrayList savedtl(DataSet M_DataSet, long surveypk, OracleTransaction TRAN, string surveypks)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            int i = 0;
            Int32 RecAfct = default(Int32);
            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;

                for (i = 0; i <= M_DataSet.Tables[0].Rows.Count - 1; i++)
                {
                    if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["SURVEY_DTL_PK"].ToString()))
                    {
                        var _with14 = insCommand;
                        _with14.Connection = objWK.MyConnection;
                        _with14.Transaction = TRAN;
                        _with14.CommandType = CommandType.StoredProcedure;
                        _with14.CommandText = objWK.MyUserName + ".SURVEY_INFO_PKG.SURVEY_DTL_TBL_SAVE";
                        var _with15 = _with14.Parameters;
                        _with15.Clear();
                        insCommand.Parameters.Add("SURVEY_DTL_PK_IN", "").Direction = ParameterDirection.Input;
                        insCommand.Parameters["SURVEY_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("SURVEY_MST_FK_IN", surveypk).Direction = ParameterDirection.Input;
                        insCommand.Parameters["SURVEY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("ITEM_IN", M_DataSet.Tables[0].Rows[i]["ITEM"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["ITEM_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("QTY_IN", M_DataSet.Tables[0].Rows[i]["QUANTITY"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["QTY_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("C_ORIGIN", M_DataSet.Tables[0].Rows[i]["C_ORIGIN"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["C_ORIGIN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("EXP_DEST_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["EXP_DESTINATION"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["EXP_DESTINATION"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["EXP_DEST_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("LENGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["LENGTH"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["LENGTH"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["LENGHT_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["WIDTH"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["WIDTH"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["HEIGHT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["HEIGHT"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("VOLUME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["VOLUME"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["VOLUME"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["WEIGHT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["WEIGHT"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("DISMANTING_IN", M_DataSet.Tables[0].Rows[i]["DISMANTING"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["DISMANTING_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("NO_OF_UNITS_IN", M_DataSet.Tables[0].Rows[i]["NO_OF_UNITS"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["NO_OF_UNITS_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("MODE_IN", M_DataSet.Tables[0].Rows[i]["MODE_OF_TRANSPORT"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["MODE_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("AREA_DESC_IN", M_DataSet.Tables[0].Rows[i]["AREA_DESCRIPTION"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["AREA_DESC_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("ELEVATOR_IN", M_DataSet.Tables[0].Rows[i]["ELEVATOR"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["ELEVATOR_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("PACK_TYPE_FK_IN", M_DataSet.Tables[0].Rows[i]["PACK_TYPE_FK"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("REMARKS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["REMARKS"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["REMARKS"])).Direction = ParameterDirection.Input;
                        insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("PACK_QTY_IN", M_DataSet.Tables[0].Rows[i]["PACK_QTY"]).Direction = ParameterDirection.Input;
                        insCommand.Parameters["PACK_QTY_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                        insCommand.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                        insCommand.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        insCommand.ExecuteNonQuery();
                    }
                    else
                    {
                        var _with16 = updCommand;
                        _with16.Connection = objWK.MyConnection;
                        _with16.Transaction = TRAN;
                        _with16.CommandType = CommandType.StoredProcedure;
                        _with16.CommandText = objWK.MyUserName + ".SURVEY_INFO_PKG.SURVEY_DTL_TBL_UPD";
                        var _with17 = _with16.Parameters;
                        _with17.Clear();
                        updCommand.Parameters.Add("SURVEY_DTL_PK_IN", M_DataSet.Tables[0].Rows[i]["SURVEY_DTL_PK"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["SURVEY_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("SURVEY_MST_FK_IN", surveypk).Direction = ParameterDirection.Input;
                        updCommand.Parameters["SURVEY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("ITEM_IN", M_DataSet.Tables[0].Rows[i]["ITEM"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["ITEM_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("QTY_IN", M_DataSet.Tables[0].Rows[i]["QUANTITY"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["QTY_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("C_ORIGIN_IN", M_DataSet.Tables[0].Rows[i]["C_ORIGIN"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["C_ORIGIN_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["EXP_DESTINATION"].ToString()))
                        {
                            updCommand.Parameters.Add("EXP_DEST_IN", "").Direction = ParameterDirection.Input;
                            updCommand.Parameters["EXP_DEST_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            updCommand.Parameters.Add("EXP_DEST_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["EXP_DESTINATION"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["EXP_DESTINATION"])).Direction = ParameterDirection.Input;
                            updCommand.Parameters["EXP_DEST_IN"].SourceVersion = DataRowVersion.Current;
                        }

                        updCommand.Parameters.Add("LENGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["LENGTH"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["LENGTH"])).Direction = ParameterDirection.Input;
                        updCommand.Parameters["LENGHT_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["WIDTH"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["WIDTH"])).Direction = ParameterDirection.Input;
                        updCommand.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["HEIGHT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["HEIGHT"])).Direction = ParameterDirection.Input;
                        updCommand.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("VOLUME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["VOLUME"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["VOLUME"])).Direction = ParameterDirection.Input;
                        updCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["WEIGHT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["WEIGHT"])).Direction = ParameterDirection.Input;
                        updCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("DISMANTING_IN", M_DataSet.Tables[0].Rows[i]["DISMANTING"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["DISMANTING_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("NO_OF_UNITS_IN", M_DataSet.Tables[0].Rows[i]["NO_OF_UNITS"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["NO_OF_UNITS_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("MODE_IN", M_DataSet.Tables[0].Rows[i]["MODE_OF_TRANSPORT"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["MODE_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("AREA_DESC_IN", M_DataSet.Tables[0].Rows[i]["AREA_DESCRIPTION"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["AREA_DESC_IN"].SourceVersion = DataRowVersion.Current;

                        //updCommand.Parameters.Add("ELEVATOR_IN", OracleClient.OracleDbType.Int32, 1, M_DataSet.Tables(0).Rows(i).Item("ELEVATOR")).Direction = ParameterDirection.Input
                        //updCommand.Parameters["ELEVATOR_IN"].SourceVersion = DataRowVersion.Current

                        if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ELEVATOR"].ToString()))
                        {
                            updCommand.Parameters.Add("ELEVATOR_IN", "").Direction = ParameterDirection.Input;
                            updCommand.Parameters["ELEVATOR_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            updCommand.Parameters.Add("ELEVATOR_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["ELEVATOR"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["ELEVATOR"])).Direction = ParameterDirection.Input;
                            updCommand.Parameters["ELEVATOR_IN"].SourceVersion = DataRowVersion.Current;
                        }

                        //updCommand.Parameters.Add("PACK_TYPE_FK_IN", OracleClient.OracleDbType.Int32, 10, M_DataSet.Tables(0).Rows(i).Item("PACK_TYPE_FK")).Direction = ParameterDirection.Input
                        //updCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current

                        if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["PACK_TYPE_FK"].ToString()))
                        {
                            updCommand.Parameters.Add("PACK_TYPE_FK_IN", "").Direction = ParameterDirection.Input;
                            updCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            updCommand.Parameters.Add("PACK_TYPE_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["PACK_TYPE_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["PACK_TYPE_FK"])).Direction = ParameterDirection.Input;
                            updCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;
                        }

                        //updCommand.Parameters.Add("REMARKS_IN", OracleClient.OracleDbType.Varchar2, 250, IIf(IsDBNull(M_DataSet.Tables(0).Rows(i).Item("REMARKS")), "", M_DataSet.Tables(0).Rows(i).Item("REMARKS"))).Direction = ParameterDirection.Input
                        //updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current

                        if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["REMARKS"].ToString()))
                        {
                            updCommand.Parameters.Add("REMARKS_IN", "").Direction = ParameterDirection.Input;
                            updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            updCommand.Parameters.Add("REMARKS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[i]["REMARKS"].ToString()) ? "" : M_DataSet.Tables[0].Rows[i]["REMARKS"])).Direction = ParameterDirection.Input;
                            updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                        }

                        updCommand.Parameters.Add("PACK_QTY_IN", M_DataSet.Tables[0].Rows[i]["PACK_QTY"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["PACK_QTY_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                        updCommand.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                        //updCommand.Parameters.Add("LAST_MODIFIED_DT_IN", Now.Date).Direction = ParameterDirection.Input

                        updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                        updCommand.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("VERSION_NO_IN", M_DataSet.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                        updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        updCommand.ExecuteNonQuery();
                    }
                }

                //With insCommand
                //    .Connection = objWK.MyConnection
                //    .CommandType = CommandType.StoredProcedure
                //    .CommandText = objWK.MyUserName & ".SURVEY_INFO_PKG.SURVEY_DTL_TBL_SAVE"
                //    With .Parameters
                //        .Clear()
                //        insCommand.Parameters.Add("SURVEY_DTL_PK_IN", OracleClient.OracleDbType.Int32, 20, "SURVEY_DTL_PK").Direction = ParameterDirection.Input
                //        insCommand.Parameters["SURVEY_DTL_PK_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("SURVEY_MST_FK_IN", surveypk).Direction = ParameterDirection.Input
                //        insCommand.Parameters["SURVEY_MST_FK_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("ITEM_IN", OracleClient.OracleDbType.Varchar2, 50, "ITEM").Direction = ParameterDirection.Input
                //        insCommand.Parameters["ITEM_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("QTY_IN", OracleClient.OracleDbType.Int32, 7, "QUANTITY").Direction = ParameterDirection.Input
                //        insCommand.Parameters["QTY_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("C_ORIGIN", OracleClient.OracleDbType.Varchar2, 100, "C_ORIGIN").Direction = ParameterDirection.Input
                //        insCommand.Parameters["C_ORIGIN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("EXP_DEST_IN", OracleClient.OracleDbType.Varchar2, 100, "EXP_DESTINATION").Direction = ParameterDirection.Input
                //        insCommand.Parameters["EXP_DEST_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("LENGHT_IN", OracleClient.OracleDbType.Int32, 11, "LENGTH").Direction = ParameterDirection.Input
                //        insCommand.Parameters["LENGHT_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("WIDTH_IN", OracleClient.OracleDbType.Int32, 11, "WIDTH").Direction = ParameterDirection.Input
                //        insCommand.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("HEIGHT_IN", OracleClient.OracleDbType.Int32, 11, "HEIGHT").Direction = ParameterDirection.Input
                //        insCommand.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("VOLUME_IN", OracleClient.OracleDbType.Int32, 11, "VOLUME").Direction = ParameterDirection.Input
                //        insCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("WEIGHT_IN", OracleClient.OracleDbType.Int32, 11, "WEIGHT").Direction = ParameterDirection.Input
                //        insCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("DISMANTING_IN", OracleClient.OracleDbType.Int32, 1, "DISMANTING").Direction = ParameterDirection.Input
                //        insCommand.Parameters["DISMANTING_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("NO_OF_UNITS_IN", OracleClient.OracleDbType.Int32, 5, "NO_OF_UNITS").Direction = ParameterDirection.Input
                //        insCommand.Parameters["NO_OF_UNITS_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("MODE_IN", OracleClient.OracleDbType.Int32, 1, "MODE_OF_TRANSPORT").Direction = ParameterDirection.Input
                //        insCommand.Parameters["MODE_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("AREA_DESC_IN", OracleClient.OracleDbType.Varchar2, 50, "AREA_DESCRIPTION").Direction = ParameterDirection.Input
                //        insCommand.Parameters["AREA_DESC_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("ELEVATOR_IN", OracleClient.OracleDbType.Int32, 1, "ELEVATOR").Direction = ParameterDirection.Input
                //        insCommand.Parameters["ELEVATOR_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("PACK_TYPE_FK_IN", OracleClient.OracleDbType.Int32, 10, "PACK_TYPE_FK").Direction = ParameterDirection.Input
                //        insCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("REMARKS_IN", OracleClient.OracleDbType.Varchar2, 250, "REMARKS").Direction = ParameterDirection.Input
                //        insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("PACK_QTY_IN", OracleClient.OracleDbType.Int32, 7, "PACK_QTY").Direction = ParameterDirection.Input
                //        insCommand.Parameters["PACK_QTY_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input
                //        insCommand.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input
                //        insCommand.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current

                //        insCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output
                //        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
                //    End With
                //End With
                //With updCommand
                //    .Connection = objWK.MyConnection
                //    .CommandType = CommandType.StoredProcedure
                //    .CommandText = objWK.MyUserName & ".SURVEY_INFO_PKG.SURVEY_DTL_TBL_UPD"
                //    With .Parameters
                //        .Clear()
                //        updCommand.Parameters.Add("SURVEY_DTL_PK_IN", OracleClient.OracleDbType.Int32, 10, "SURVEY_DTL_PK").Direction = ParameterDirection.Input
                //        updCommand.Parameters["SURVEY_DTL_PK_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("SURVEY_MST_FK_IN", surveypk).Direction = ParameterDirection.Input
                //        updCommand.Parameters["SURVEY_MST_FK_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("ITEM_IN", OracleClient.OracleDbType.Varchar2, 50, "ITEM").Direction = ParameterDirection.Input
                //        updCommand.Parameters["ITEM_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("QTY_IN", OracleClient.OracleDbType.Int32, 7, "QUANTITY").Direction = ParameterDirection.Input
                //        updCommand.Parameters["QTY_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("C_ORIGIN_IN", OracleClient.OracleDbType.Varchar2, 100, "C_ORIGIN").Direction = ParameterDirection.Input
                //        updCommand.Parameters["C_ORIGIN_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("EXP_DEST_IN", OracleClient.OracleDbType.Varchar2, 100, "EXP_DESTINATION").Direction = ParameterDirection.Input
                //        updCommand.Parameters["EXP_DEST_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("LENGHT_IN", OracleClient.OracleDbType.Int32, 11, "LENGTH").Direction = ParameterDirection.Input
                //        updCommand.Parameters["LENGHT_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("WIDTH_IN", OracleClient.OracleDbType.Int32, 11, "WIDTH").Direction = ParameterDirection.Input
                //        updCommand.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("HEIGHT_IN", OracleClient.OracleDbType.Int32, 11, "HEIGHT").Direction = ParameterDirection.Input
                //        updCommand.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("VOLUME_IN", OracleClient.OracleDbType.Int32, 11, "VOLUME").Direction = ParameterDirection.Input
                //        updCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("WEIGHT_IN", OracleClient.OracleDbType.Int32, 11, "WEIGHT").Direction = ParameterDirection.Input
                //        updCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("DISMANTING_IN", OracleClient.OracleDbType.Int32, 1, "DISMANTING").Direction = ParameterDirection.Input
                //        updCommand.Parameters["DISMANTING_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("NO_OF_UNITS_IN", OracleClient.OracleDbType.Int32, 5, "NO_OF_UNITS").Direction = ParameterDirection.Input
                //        updCommand.Parameters["NO_OF_UNITS_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("MODE_IN", OracleClient.OracleDbType.Int32, 1, "MODE_OF_TRANSPORT").Direction = ParameterDirection.Input
                //        updCommand.Parameters["MODE_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("AREA_DESC_IN", OracleClient.OracleDbType.Varchar2, 50, "AREA_DESCRIPTION").Direction = ParameterDirection.Input
                //        updCommand.Parameters["AREA_DESC_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("ELEVATOR_IN", OracleClient.OracleDbType.Int32, 1, "ELEVATOR").Direction = ParameterDirection.Input
                //        updCommand.Parameters["ELEVATOR_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("PACK_TYPE_FK_IN", OracleClient.OracleDbType.Int32, 10, "PACK_TYPE_FK").Direction = ParameterDirection.Input
                //        updCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("REMARKS_IN", OracleClient.OracleDbType.Varchar2, 250, "REMARKS").Direction = ParameterDirection.Input
                //        updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("PACK_QTY_IN", OracleClient.OracleDbType.Int32, 7, "PACK_QTY").Direction = ParameterDirection.Input
                //        updCommand.Parameters["PACK_QTY_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input
                //        updCommand.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current

                //        'updCommand.Parameters.Add("LAST_MODIFIED_DT_IN", Now.Date).Direction = ParameterDirection.Input

                //        updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input
                //        updCommand.Parameters["CONFIG_PK_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input
                //        updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current

                //        updCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output
                //        updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
                //    End With
                //End With
                //'AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)
                //'With objWK.MyDataAdapter
                //'    .InsertCommand = insCommand
                //'    .InsertCommand.Transaction = TRAN
                //'    .UpdateCommand = updCommand
                //'    .UpdateCommand.Transaction = TRAN
                //'    RecAfct = .Update(M_DataSet)
                //' If RecAfct > 0 Then
                if (RecAfct == 0)
                {
                    if (!string.IsNullOrEmpty(surveypks))
                    {
                        if (DeleteDtl(surveypks, TRAN) == "1")
                        {
                            arrMessage.Add("All Data Saved Successfully");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        arrMessage.Add("All Data Saved Successfully");
                        return arrMessage;
                    }
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //'End With
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }

        public string DeleteDtl(string SurveyPK, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand delCommand = new OracleCommand();
            //Dim TRAN As OracleTransaction
            string[] Suveyarr = null;
            Int32 i = default(Int32);
            Int32 cnt = default(Int32);
            Int32 surveydtlPk = default(Int32);
            cnt = SurveyPK.IndexOf(",");
            try
            {
                //objWK.OpenConnection()
                //objWK.MyCommand.Connection = objWK.MyConnection
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;
                //TRAN = objWK.MyConnection.BeginTransaction()

                if (cnt > 0)
                {
                    Suveyarr = SurveyPK.Split(',');

                    for (i = 0; i <= Suveyarr.Length - 1; i++)
                    {
                        var _with18 = delCommand;
                        _with18.Connection = objWK.MyConnection;
                        _with18.CommandType = CommandType.StoredProcedure;
                        _with18.CommandText = objWK.MyUserName + ".SURVEY_INFO_PKG.SURVEY_DTL_TBL_DEL";
                        var _with19 = _with18.Parameters;
                        _with19.Clear();
                        _with19.Add("SURVEY_DTL_PK_IN", Convert.ToInt64(Suveyarr[i])).Direction = ParameterDirection.Input;
                        _with19.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                        _with19.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                        //.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 10, "Version_No").Direction = ParameterDirection.Input
                        _with19.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //objWK.MyCommand.ExecuteNonQuery()
                        var _with20 = objWK.MyDataAdapter;
                        _with20.DeleteCommand = delCommand;
                        _with20.DeleteCommand.Transaction = TRAN;
                        _with20.DeleteCommand.ExecuteNonQuery();
                        surveydtlPk = Convert.ToInt32(delCommand.Parameters["RETURN_VALUE"].Value);
                        //arrMessage.Add("All Data Saved Successfully")
                    }
                    return "1";
                }
                else
                {
                    var _with21 = delCommand;
                    _with21.Connection = objWK.MyConnection;
                    _with21.CommandType = CommandType.StoredProcedure;
                    _with21.CommandText = objWK.MyUserName + ".SURVEY_INFO_PKG.SURVEY_DTL_TBL_DEL";
                    var _with22 = _with21.Parameters;
                    _with22.Clear();
                    _with22.Add("SURVEY_DTL_PK_IN", Convert.ToInt64(SurveyPK)).Direction = ParameterDirection.Input;
                    _with22.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with22.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    //.Add("VERSION_NO_IN", OracleClient.OracleDbType.Int32, 10, "Version_No").Direction = ParameterDirection.Input
                    _with22.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    //objWK.MyCommand.ExecuteNonQuery()
                    var _with23 = objWK.MyDataAdapter;
                    _with23.DeleteCommand = delCommand;
                    _with23.DeleteCommand.Transaction = TRAN;
                    _with23.DeleteCommand.ExecuteNonQuery();
                    surveydtlPk = Convert.ToInt32(delCommand.Parameters["RETURN_VALUE"].Value);
                    //arrMessage.Add("All Data Saved Successfully")
                    //TRAN.Commit()
                    return "1";
                }
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
        }

        #endregion "Save"

        #region " Supporting Function "

        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, ""))
            {
                return "";
            }
            return col;
        }

        public DataSet FetchDropDown()
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append(" select 0 PK , ' ' TEXT from dual");
                strQuery.Append(" union select 1 PK ,'Yes' TEXT from dual");
                strQuery.Append("  union SELECT 2 PK ,'No' TEXT from dual  ");
                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet FetchModeDropDown()
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append("  select 0 MODE_PK , ' ' MODE_TEXT from dual");
                strQuery.Append("  union SELECT 1 MODE_PK ,'Air' MODE_TEXT from dual ");
                strQuery.Append("  union select 2 MODE_PK ,'Sea' MODE_TEXT from dual");
                strQuery.Append("  union SELECT 3 MODE_PK ,'Road' MODE_TEXT from dual  ");
                strQuery.Append("  union SELECT 4 MODE_PK ,'Rail' MODE_TEXT from dual ");
                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Supporting Function "

        #region "Enhance-Search Removals Based Customer"

        public string FetchEnquiryNr(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOCATION_IN = "";
            int intBUSINESS_TYPE_IN = 3;
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                intBUSINESS_TYPE_IN = Convert.ToInt32(arr.GetValue(2));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".SURVEY_INFO_PKG.EN_REM_ENQUIRY";
                var _with24 = SCM.Parameters;
                _with24.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with24.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with24.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with24.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        #endregion "Enhance-Search Removals Based Customer"

        #region "GeneratProtocol"

        //This function is called to generate the enquiry reference no.
        //Called for Enquiry on New Booking
        public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
        {
            try
            {
                return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "GeneratProtocol"

        #region "fetch MoveType"

        public DataSet fetchMoveType()
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append("select 0 MOVE_TYPE_PK,'All' MOve_code from dual ");
                strQuery.Append("union ");
                strQuery.Append(" select 1 MOVE_TYPE_PK,'Domestic' Move_code from dual ");
                strQuery.Append(" union ");
                strQuery.Append(" select 2 MOVE_TYPE_PK,'European' Move_code from dual  ");
                strQuery.Append(" union ");
                strQuery.Append(" select 3 MOVE_TYPE_PK,'Overseas' Move_code from dual ");

                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fetchSurveyListing(string Survey_Nr, long plr_fk, long pfd_fk, string MoveType = "", string SearchType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            StringBuilder strQry = new StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (Survey_Nr.Trim().Length > 0)
                {
                    if (SearchType == "C")
                    {
                        strCondition = strCondition + " And upper(surmst.survey_number) like '%" + Survey_Nr.ToUpper().Replace("'", "''") + "%' ";
                    }
                    else
                    {
                        strCondition = strCondition + " And upper(surmst.survey_number) like '" + Survey_Nr.ToUpper().Replace("'", "''") + "%' ";
                    }
                }
                if (plr_fk > 0)
                {
                    strCondition = strCondition + "AND PLR.PLACE_PK = " + plr_fk;
                }
                if (pfd_fk > 0)
                {
                    strCondition = strCondition + "AND PFD.PLACE_PK = " + pfd_fk;
                }
                if (MoveType == "Domestic")
                {
                    strCondition = strCondition + " and surmst.move_type = 0";
                }
                else if (MoveType == "European")
                {
                    strCondition = strCondition + " and surmst.move_type = 1 ";
                }
                else if (MoveType == "Overseas")
                {
                    strCondition = strCondition + " and surmst.move_type = 2";
                }
                strQuery.Append(" SELECT COUNT(*)   ");
                strQuery.Append(" FROM (SELECT ROWNUM SLNR ,");
                strQuery.Append("     QTY.SURVEY_MST_PK,");
                strQuery.Append("     QTY.SURVEY_NUMBER,");
                strQuery.Append("     QTY.CUSTOMER_NAME,");
                strQuery.Append("     QTY.PLACE_NAME,");
                strQuery.Append("     QTY.MOVE_TYPE,");
                strQuery.Append("     TO_CHAR(QTY.SURVEY_DT,dateformat),");
                strQuery.Append("     QTY.DELFLAG FROM");
                strQuery.Append("     (SELECT DISTINCT ");
                strQuery.Append("     SURMST.SURVEY_MST_PK,");
                strQuery.Append("     SURMST.SURVEY_NUMBER,");
                strQuery.Append("     SURMST.SURVEY_DT,");
                strQuery.Append("     PTY.CUSTOMER_NAME,");
                strQuery.Append("     PLR.PLACE_NAME,");
                strQuery.Append("     DECODE(SURMST.MOVE_TYPE,0,'Domestic',1,'European',2,'Overseas') MOVE_TYPE,");
                strQuery.Append("     '' DELFLAG");
                strQuery.Append("     FROM");
                strQuery.Append("     REM_M_SURVEY_MST_TBL SURMST,");
                strQuery.Append("     REM_D_SURVEY_DTL_TBL SURDTL,");
                strQuery.Append("     CUSTOMER_MST_TBL PTY,");
                strQuery.Append("     PLACE_MST_TBL PLR,");
                strQuery.Append("     PLACE_MST_TBL PFD");
                strQuery.Append("     WHERE SURMST.SURVEY_MST_PK = SURDTL.SURVEY_MST_FK");
                strQuery.Append("     AND PTY.CUSTOMER_MST_PK = SURMST.PARTY_MST_FK");
                strQuery.Append("     AND PLR.PLACE_PK = SURMST.PLR_FK");
                strQuery.Append(" " + strCondition + "");
                strQuery.Append("     AND PFD.PLACE_PK = SURMST.PFD_FK)QTY)");
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strQuery.ToString()));
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

                strQry.Append(" select  * from ( ");
                strQry.Append(" SELECT ROWNUM SLNR ,");
                strQry.Append("  QTY.SURVEY_MST_PK,");
                strQry.Append("  QTY.SURVEY_NUMBER,");
                strQry.Append("  QTY.SURVEY_DT,");
                strQry.Append("  QTY.CUSTOMER_NAME,");
                strQry.Append("  QTY.PLR,");
                strQry.Append("  QTY.PFD,");
                strQry.Append("  QTY.MOVE_TYPE,");
                strQry.Append("  QTY.DELFLAG FROM");
                strQry.Append("  (SELECT DISTINCT ");
                strQry.Append("  SURMST.SURVEY_MST_PK,");
                strQry.Append("  SURMST.SURVEY_NUMBER,");
                strQry.Append("  TO_CHAR( SURMST.SURVEY_DT,dateformat) SURVEY_DT,");
                strQry.Append("  PTY.CUSTOMER_NAME,");
                strQry.Append("  PLR.PLACE_NAME PLR,");
                strQry.Append("  PFD.PLACE_NAME PFD,");
                strQry.Append("  DECODE(SURMST.MOVE_TYPE,0,'Domestic',1,'European',2,'Overseas') MOVE_TYPE,");
                strQry.Append("      '' DELFLAG");
                strQry.Append("  FROM");
                strQry.Append("  REM_M_SURVEY_MST_TBL SURMST,");
                strQry.Append("  REM_D_SURVEY_DTL_TBL SURDTL,");
                strQry.Append("  CUSTOMER_MST_TBL PTY,");
                strQry.Append("  PLACE_MST_TBL PLR,");
                strQry.Append("  PLACE_MST_TBL PFD");
                strQry.Append("  WHERE SURMST.SURVEY_MST_PK = SURDTL.SURVEY_MST_FK");
                strQry.Append("  AND PTY.CUSTOMER_MST_PK = SURMST.PARTY_MST_FK");
                strQry.Append("  AND PLR.PLACE_PK = SURMST.PLR_FK");
                strQry.Append(" " + strCondition + "");
                strQry.Append("  AND PFD.PLACE_PK = SURMST.PFD_FK");
                strQry.Append("  order by  to_date(SURVEY_DT) desc )QTY)");

                strQry.Append(" WHERE SLNR  Between " + start + " and " + last + "");

                return (objWK.GetDataSet(strQry.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "fetch MoveType"

        #endregion "Class Variables"

        #region "fetch Summary"

        public DataSet fetchSummaryGrid()
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append("select 'Land' Trns_Mode,'' Weight, '' Volume,'' No_of_Units from dual ");
                strQuery.Append(" union select 'Sea' Trns_Mode,'' Weight, '' Volume,'' No_of_Units from dual ");
                strQuery.Append("union select 'Air' Trns_Mode,'' Weight, '' Volume,'' No_of_Units from dual ");
                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public bool fetchQuotationPK(long quotPK)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            string strquotpk = null;
            try
            {
                strQuery.Append("    SELECT NVL(RQUOT.QUOT_PK,0) FROM REM_M_QUOT_MST_TBL RQUOT WHERE RQUOT.QUOT_SURVEY_FK =" + quotPK + " ");
                strquotpk = objWK.ExecuteScaler(strQuery.ToString());
                if ((strquotpk == null))
                {
                    return false;
                }
                else if (Convert.ToInt32(strquotpk) == 0)
                {
                    return false;
                }
                else
                {
                    return true;
                }
                //Return (objWK.GetDataSet(strQuery.ToString()))
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fetchPackSummaryGrid()
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append(" SELECT ");
                strQuery.Append(" RDTL.PACK_TYPE_FK, ");
                strQuery.Append(" PTYPE.PACK_TYPE_ID, ");
                strQuery.Append(" RDTL.PACK_QTY ");
                strQuery.Append(" FROM PACK_TYPE_MST_TBL PTYPE, ");
                strQuery.Append(" REM_D_SURVEY_DTL_TBL RDTL, ");
                strQuery.Append(" REM_M_SURVEY_MST_TBL RMST ");
                strQuery.Append(" WHERE PTYPE.PACK_TYPE_MST_PK = RDTL.PACK_TYPE_FK ");
                strQuery.Append(" AND RDTL.SURVEY_MST_FK = RMST.SURVEY_MST_PK ");
                strQuery.Append(" AND RMST.SURVEY_MST_PK = 0 ");
                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "fetch Summary"

        #region "Report"

        public DataSet fetchSuryveyRptHdr(long SurveyPK)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append("select distinct smst.survey_mst_pk, ");
                strQuery.Append("smst.survey_number, ");
                strQuery.Append("to_date(smst.survey_dt,'dd/MM/yyyy')  survey_dt, ");
                strQuery.Append("plr.place_name plrname, ");
                strQuery.Append("smst.pick_up_address1, ");
                strQuery.Append("smst.pick_up_address2, ");
                strQuery.Append("smst.pick_up_city, ");
                strQuery.Append("smst.plr_zip_code, ");
                strQuery.Append("ctyplr.country_name plrcountry, ");
                strQuery.Append("pfd.place_name pfdname, ");
                strQuery.Append("smst.delivery_address1, ");
                strQuery.Append("smst.delivery_address2, ");
                strQuery.Append("smst.delivery_city, ");
                strQuery.Append("smst.pfd_zip_code, ");
                strQuery.Append("ctypfd.country_name pfdcountry, ");
                strQuery.Append("cust.customer_name, ");
                strQuery.Append("cdts.adm_address_1, ");
                strQuery.Append("cdts.adm_address_2, ");
                strQuery.Append("cdts.adm_address_3, ");
                strQuery.Append("cdts.adm_zip_code, ");
                strQuery.Append("cdts.adm_phone_no_1, ");
                strQuery.Append("cdts.adm_phone_no_2, ");
                strQuery.Append("cdts.adm_fax_no, ");
                strQuery.Append("cdts.adm_email_id, ctypar.country_name party_cty, ");
                strQuery.Append("smst.survey_executive, ");
                strQuery.Append("sdtl.mode_of_transport, ");
                strQuery.Append("QTY.volume tot_est_vol ");

                strQuery.Append("   from  ");
                strQuery.Append("rem_m_survey_mst_tbl smst, ");
                strQuery.Append("rem_d_survey_dtl_tbl sdtl, ");
                strQuery.Append("customer_mst_tbl cust, ");
                strQuery.Append("place_mst_tbl plr, ");
                strQuery.Append("place_mst_tbl pfd, ");
                strQuery.Append("country_mst_tbl ctyplr, ");
                strQuery.Append("country_mst_tbl ctypfd,country_mst_tbl ctypar, ");
                strQuery.Append("customer_contact_dtls cdts, ");
                strQuery.Append("(select smt.survey_mst_pk,");
                strQuery.Append(" sum(rtl.volume)  volume");
                strQuery.Append(" from");
                strQuery.Append(" rem_m_survey_mst_tbl smt,");
                strQuery.Append(" rem_d_survey_dtl_tbl rtl");
                strQuery.Append(" where smt.survey_mst_pk = rtl.survey_mst_fk");
                strQuery.Append(" and smt.survey_mst_pk =" + SurveyPK + "");
                strQuery.Append(" group by smt.survey_mst_pk)QTY");
                strQuery.Append(" where smst.party_mst_fk = cust.customer_mst_pk ");
                strQuery.Append(" and sdtl.survey_mst_fk = smst.survey_mst_pk ");
                strQuery.Append(" and smst.plr_fk = plr.place_pk ");
                strQuery.Append("  and smst.pfd_fk = pfd.place_pk ");
                strQuery.Append(" and smst.pick_up_country_fk = ctyplr.country_mst_pk ");
                strQuery.Append(" and smst.delivry_country_fk = ctypfd.country_mst_pk ");
                strQuery.Append(" and cust.customer_mst_pk = cdts.customer_mst_fk ");
                strQuery.Append(" and cdts.adm_country_mst_fk = ctypar.country_mst_pk");
                strQuery.Append(" and smst.survey_mst_pk =" + SurveyPK + " ");
                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fetchSuryveyModeDetails(long SurveyPK)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append("select ");
                strQuery.Append("smst.survey_mst_pk, ");
                strQuery.Append("decode(sdtl.mode_of_transport,1,'Air',2,'Sea',3,'Road',4,'Rail'), ");
                strQuery.Append("sum(sdtl.volume), ");
                strQuery.Append("sum(sdtl.weight) ");
                strQuery.Append("from ");
                strQuery.Append("rem_m_survey_mst_tbl smst, ");
                strQuery.Append("rem_d_survey_dtl_tbl sdtl ");
                strQuery.Append("where smst.survey_mst_pk = sdtl.survey_mst_fk ");
                strQuery.Append("and smst.survey_mst_pk =" + SurveyPK + " ");
                strQuery.Append("group by smst.survey_mst_pk,sdtl.mode_of_transport ");
                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fetchSuryveyItemDetails(long SurveyPK)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append("select ");
                strQuery.Append("smst.survey_mst_pk, ");
                strQuery.Append("sdtl.survey_dtl_pk, ");
                strQuery.Append("decode(sdtl.mode_of_transport,1,'Air',2,'Sea',3,'Road',4,'Rail') mode_of_transporte, ");
                strQuery.Append("sdtl.item, ");
                strQuery.Append("sdtl.quantity, ");
                strQuery.Append("decode(sdtl.dismanting,1,'Yes',2,'No') dismanting, ");
                strQuery.Append("sdtl.remarks, ");
                strQuery.Append("sdtl.volume, ");
                strQuery.Append("sdtl.weight, ");
                strQuery.Append("sdtl.mode_of_transport ");
                strQuery.Append("from ");
                strQuery.Append("rem_m_survey_mst_tbl smst, ");
                strQuery.Append("rem_d_survey_dtl_tbl sdtl ");
                strQuery.Append("where smst.survey_mst_pk = sdtl.survey_mst_fk ");
                strQuery.Append("and smst.survey_mst_pk =" + SurveyPK + " ");
                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fetchSuryveyRoomDetails(long SurveyPK)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append(" select smst.survey_mst_pk, ");
                strQuery.Append("    sdtl.survey_dtl_pk, ");
                strQuery.Append("    sum(sdtl.quantity), ");
                strQuery.Append("    sum(sdtl.volume), ");
                strQuery.Append("    sum(sdtl.weight), ");
                strQuery.Append("    sdtl.area_description ");
                strQuery.Append(" from rem_m_survey_mst_tbl smst, rem_d_survey_dtl_tbl sdtl ");
                strQuery.Append(" where smst.survey_mst_pk = sdtl.survey_mst_fk ");
                strQuery.Append(" and smst.survey_mst_pk =" + SurveyPK + "");
                strQuery.Append(" group by smst.survey_mst_pk, sdtl.survey_dtl_pk, sdtl.area_description ");
                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet fetchSuryveyPackDetails(long SurveyPK)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append(" select ");
                strQuery.Append(" smst.survey_mst_pk, ");
                strQuery.Append(" pty.pack_type_id, ");
                strQuery.Append(" sdtl.pack_qty, ");
                strQuery.Append(" sdtl.volume ");
                strQuery.Append(" from rem_m_survey_mst_tbl smst, ");
                strQuery.Append(" rem_d_survey_dtl_tbl sdtl, ");
                strQuery.Append(" pack_type_mst_tbl pty ");
                strQuery.Append(" where smst.survey_mst_pk = sdtl.survey_mst_fk ");
                strQuery.Append(" and pty.pack_type_mst_pk = sdtl.pack_type_fk ");
                strQuery.Append(" and smst.survey_mst_pk =" + SurveyPK + " ");
                return (objWK.GetDataSet(strQuery.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Report"
    }
}