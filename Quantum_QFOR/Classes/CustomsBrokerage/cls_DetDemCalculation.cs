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
    public class clsDetDemCalculation : CommonFeatures
    {
        #region " FetchDemurrageListing "

        /// <summary>
        /// Fetches the demurrage listing.
        /// </summary>
        /// <param name="VslVoyFK">The VSL voy fk.</param>
        /// <param name="CustomerFK">The customer fk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="PODFK">The podfk.</param>
        /// <param name="PFDFK">The PFDFK.</param>
        /// <param name="DocRefFK">The document reference fk.</param>
        /// <param name="DocRefFromDt">The document reference from dt.</param>
        /// <param name="DocRefToDt">The document reference to dt.</param>
        /// <param name="DemRefFK">The dem reference fk.</param>
        /// <param name="DemRefFromDt">The dem reference from dt.</param>
        /// <param name="DemRefToDt">The dem reference to dt.</param>
        /// <param name="History">The history.</param>
        /// <param name="Active">The active.</param>
        /// <param name="ContLooseCargo">The cont loose cargo.</param>
        /// <param name="isAdmin">if set to <c>true</c> [is admin].</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public object FetchDemurrageListing(long VslVoyFK, long CustomerFK, long BizType, long ProcessType, long CargoType, long PODFK, long PFDFK, long DocRefFK, string DocRefFromDt, string DocRefToDt,
        long DemRefFK, string DemRefFromDt, string DemRefToDt, long History, long Active, long ContLooseCargo, bool isAdmin, Int32 CurrentPage, Int32 TotalPage)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with1 = objWF.MyDataAdapter;
                _with1.SelectCommand = new OracleCommand();
                _with1.SelectCommand.Connection = objWF.MyConnection;
                _with1.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_DEMURRAGE_PKG.FETCH_DEMURRAGE_LISTING";
                _with1.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with1.SelectCommand.Parameters.Add("VOYAGE_TRN_PK_IN", OracleDbType.Int32).Value = (VslVoyFK <= 0 ? 0 : VslVoyFK);
                _with1.SelectCommand.Parameters.Add("CUSTOMER_MST_PK_IN", OracleDbType.Int32).Value = (CustomerFK <= 0 ? 0 : CustomerFK);
                _with1.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = BizType;
                _with1.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32).Value = ProcessType;
                _with1.SelectCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32).Value = CargoType;
                _with1.SelectCommand.Parameters.Add("POD_FK_IN", OracleDbType.Int32).Value = (PODFK <= 0 ? 0 : PODFK);
                _with1.SelectCommand.Parameters.Add("PFD_FK_IN", OracleDbType.Int32).Value = (PFDFK <= 0 ? 0 : PFDFK);
                _with1.SelectCommand.Parameters.Add("DOC_REF_FK_IN", OracleDbType.Int32).Value = (DocRefFK <= 0 ? 0 : DocRefFK);
                _with1.SelectCommand.Parameters.Add("DOC_REF_FROM_DT_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(DocRefFromDt) ? "" : DocRefFromDt);
                _with1.SelectCommand.Parameters.Add("DOC_REF_TO_DT_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(DocRefToDt) ? "" : DocRefToDt);
                _with1.SelectCommand.Parameters.Add("DEM_REF_FK_IN", OracleDbType.Int32).Value = (DemRefFK <= 0 ? 0 : DemRefFK);
                _with1.SelectCommand.Parameters.Add("DEM_REF_FROM_DT_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(DemRefFromDt) ? "" : DemRefFromDt);
                _with1.SelectCommand.Parameters.Add("DEM_REF_TO_DT_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(DemRefToDt) ? "" : DemRefToDt);
                _with1.SelectCommand.Parameters.Add("HISTORY_IN", OracleDbType.Int32).Value = History;
                _with1.SelectCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32).Value = Active;
                _with1.SelectCommand.Parameters.Add("CONTLOOSECARGO_IN", OracleDbType.Int32).Value = ContLooseCargo;
                _with1.SelectCommand.Parameters.Add("LOGGED_LOC_MST_PK_IN", OracleDbType.Int32).Value = HttpContext.Current.Session["LOGED_IN_LOC_FK"];
                _with1.SelectCommand.Parameters.Add("IS_ADMIN_IN", OracleDbType.Int32).Value = (isAdmin ? 1 : 0);
                _with1.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = M_MasterPageSize;
                _with1.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with1.SelectCommand.Parameters.Add("DEM_BAND0_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.SelectCommand.Parameters.Add("DEM_BAND1_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with1.Fill(ds);
                TotalPage = Convert.ToInt32(_with1.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(_with1.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);

                if (History == 0)
                {
                    DataRelation objREL = new DataRelation("CONTAINER", new DataColumn[] {
                        ds.Tables[0].Columns["DOC_REF_FK"],
                        ds.Tables[0].Columns["DOC_TYPE"]
                    }, new DataColumn[] {
                        ds.Tables[1].Columns["DEM_DOC_REF_FK"],
                        ds.Tables[1].Columns["DOC_TYPE"]
                    });
                    objREL.Nested = true;
                    ds.Relations.Add(objREL);
                }
                else
                {
                    DataRelation objREL = new DataRelation("CONTAINER", new DataColumn[] {
                        ds.Tables[0].Columns["DEM_REF_PK"],
                        ds.Tables[0].Columns["DOC_TYPE"]
                    }, new DataColumn[] {
                        ds.Tables[1].Columns["DEM_DOC_REF_FK"],
                        ds.Tables[1].Columns["DOC_TYPE"]
                    });
                    objREL.Nested = true;
                    ds.Relations.Add(objREL);
                }

                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        #endregion " FetchDemurrageListing "

        #region " FetchDetDemHeader "

        /// <summary>
        /// Fetches the det dem header.
        /// </summary>
        /// <param name="DocRefFK">The document reference fk.</param>
        /// <param name="DemRefFK">The dem reference fk.</param>
        /// <param name="DocType">Type of the document.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="History">The history.</param>
        /// <returns></returns>
        public DataSet FetchDetDemHeader(long DocRefFK, long DemRefFK, long DocType, long ProcessType, long BizType, long CargoType, long History)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with2 = objWF.MyDataAdapter;
                _with2.SelectCommand = new OracleCommand();
                _with2.SelectCommand.Connection = objWF.MyConnection;
                _with2.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_DEMURRAGE_PKG.FETCH_DEMURRAGE_HEADER";
                _with2.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with2.SelectCommand.Parameters.Add("DOC_REF_FK_IN", OracleDbType.Int32).Value = (DocRefFK <= 0 ? 0 : DocRefFK);
                _with2.SelectCommand.Parameters.Add("DEM_REF_FK_IN", OracleDbType.Int32).Value = (DemRefFK <= 0 ? 0 : DemRefFK);
                _with2.SelectCommand.Parameters.Add("DOC_TYPE_IN", OracleDbType.Int32).Value = DocType;
                _with2.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32).Value = ProcessType;
                _with2.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = BizType;
                _with2.SelectCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32).Value = CargoType;
                _with2.SelectCommand.Parameters.Add("HISTORY_IN", OracleDbType.Int32).Value = History;
                _with2.SelectCommand.Parameters.Add("DEM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with2.Fill(ds);

                return ds;
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion " FetchDetDemHeader "

        #region " FetchDetDemDetail "

        /// <summary>
        /// Fetches the det dem detail.
        /// </summary>
        /// <param name="DocRefFK">The document reference fk.</param>
        /// <param name="DemRefFK">The dem reference fk.</param>
        /// <param name="DocType">Type of the document.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="History">The history.</param>
        /// <param name="ContLooseCargo">The cont loose cargo.</param>
        /// <param name="Basis">The basis.</param>
        /// <param name="CurPK">The current pk.</param>
        /// <returns></returns>
        public DataSet FetchDetDemDetail(long DocRefFK, long DemRefFK, long DocType, long ProcessType, long BizType, long CargoType, long History, long ContLooseCargo, long Basis, long CurPK)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with3 = objWF.MyDataAdapter;
                _with3.SelectCommand = new OracleCommand();
                _with3.SelectCommand.Connection = objWF.MyConnection;
                _with3.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_DEMURRAGE_PKG.FETCH_DEMURRAGE_DETAIL";
                _with3.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with3.SelectCommand.Parameters.Add("DOC_REF_FK_IN", OracleDbType.Int32).Value = (DocRefFK <= 0 ? 0 : DocRefFK);
                _with3.SelectCommand.Parameters.Add("DEM_REF_FK_IN", OracleDbType.Int32).Value = (DemRefFK <= 0 ? 0 : DemRefFK);
                _with3.SelectCommand.Parameters.Add("DOC_TYPE_IN", OracleDbType.Int32).Value = DocType;
                _with3.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32).Value = ProcessType;
                _with3.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = BizType;
                _with3.SelectCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32).Value = CargoType;
                _with3.SelectCommand.Parameters.Add("HISTORY_IN", OracleDbType.Int32).Value = History;
                _with3.SelectCommand.Parameters.Add("CONTLOOSECARGO_IN", OracleDbType.Int32).Value = ContLooseCargo;
                _with3.SelectCommand.Parameters.Add("BASIS_IN", OracleDbType.Int32).Value = Basis;
                _with3.SelectCommand.Parameters.Add("CURRENCY_MST_PK_IN", OracleDbType.Int32).Value = CurPK;
                _with3.SelectCommand.Parameters.Add("DEM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with3.Fill(ds);

                return ds;
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion " FetchDetDemDetail "

        #region " FetchWaiverDetails "

        /// <summary>
        /// Fetches the waiver details.
        /// </summary>
        /// <param name="DemDtlFK">The dem DTL fk.</param>
        /// <param name="POD_FK">The po d_ fk.</param>
        /// <param name="PFD_ID">The pf d_ identifier.</param>
        /// <param name="OperatorFK">The operator fk.</param>
        /// <param name="TariffDate">The tariff date.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="History">The history.</param>
        /// <param name="FormFlag">The form flag.</param>
        /// <param name="ContLooseCargo">The cont loose cargo.</param>
        /// <param name="Basis">The basis.</param>
        /// <param name="ContTypeFK">The cont type fk.</param>
        /// <param name="WaiverType">Type of the waiver.</param>
        /// <param name="WaiverValue">The waiver value.</param>
        /// <param name="CalculationType">Type of the calculation.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="BasisValue">The basis value.</param>
        /// <param name="CurPK">The current pk.</param>
        /// <returns></returns>
        public DataSet FetchWaiverDetails(long DemDtlFK, long POD_FK, string PFD_ID, long OperatorFK, string TariffDate, long ProcessType, long BizType, long CargoType, long History, long FormFlag,
        long ContLooseCargo, long Basis, long ContTypeFK, long WaiverType, double WaiverValue, long CalculationType, string FromDate, string ToDate, double BasisValue, long CurPK)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with4 = objWF.MyDataAdapter;
                _with4.SelectCommand = new OracleCommand();
                _with4.SelectCommand.Connection = objWF.MyConnection;
                _with4.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_DEMURRAGE_PKG.FETCH_WAIVER_DETAILS";
                _with4.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with4.SelectCommand.Parameters.Add("DEM_CALC_DTL_FK_IN", OracleDbType.Int32).Value = (DemDtlFK <= 0 ? 0 : DemDtlFK);
                _with4.SelectCommand.Parameters.Add("POD_IN", OracleDbType.Int32).Value = (POD_FK <= 0 ? 0 : POD_FK);
                _with4.SelectCommand.Parameters.Add("PFD_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(PFD_ID.Trim()) ? "" : PFD_ID);
                _with4.SelectCommand.Parameters.Add("OPERATOR_MST_PK_IN", OracleDbType.Int32).Value = (OperatorFK <= 0 ? 0 : OperatorFK);
                _with4.SelectCommand.Parameters.Add("DATE_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(TariffDate.Trim()) ? "" : TariffDate.Trim().PadLeft(10));
                _with4.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32).Value = ProcessType;
                _with4.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = BizType;
                _with4.SelectCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32).Value = CargoType;
                _with4.SelectCommand.Parameters.Add("HISTORY_IN", OracleDbType.Int32).Value = History;
                _with4.SelectCommand.Parameters.Add("FORM_FLAG_IN", OracleDbType.Int32).Value = FormFlag;
                _with4.SelectCommand.Parameters.Add("CONTLOOSECARGO_IN", OracleDbType.Int32).Value = ContLooseCargo;
                _with4.SelectCommand.Parameters.Add("BASIS_IN", OracleDbType.Int32).Value = Basis;
                _with4.SelectCommand.Parameters.Add("CONTAINER_TYPE_MST_PK_IN", OracleDbType.Int32).Value = (ContTypeFK <= 0 ? 0 : ContTypeFK);
                _with4.SelectCommand.Parameters.Add("WAIVER_TYPE_IN", OracleDbType.Int32).Value = WaiverType;
                _with4.SelectCommand.Parameters.Add("WAIVER_VALUE_IN", OracleDbType.Double).Value = WaiverValue;
                _with4.SelectCommand.Parameters.Add("CALCULATION_TYPE_IN", OracleDbType.Int32).Value = CalculationType;
                _with4.SelectCommand.Parameters.Add("FROM_DATE_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(FromDate.Trim()) ? "" : FromDate.Trim().PadLeft(10));
                _with4.SelectCommand.Parameters.Add("TO_DATE_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(ToDate.Trim()) ? "" : ToDate.Trim().PadLeft(10));
                _with4.SelectCommand.Parameters.Add("BASIS_VALUE_IN", OracleDbType.Int32).Value = BasisValue;
                _with4.SelectCommand.Parameters.Add("CURRENCY_MST_PK_IN", OracleDbType.Int32).Value = CurPK;
                _with4.SelectCommand.Parameters.Add("DEM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with4.Fill(ds);

                return ds;
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion " FetchWaiverDetails "

        #region " Generat Protocol "

        /// <summary>
        /// Generates the service no.
        /// </summary>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <returns></returns>
        public string GenerateServiceNo(Int64 ILocationId, Int64 IEmployeeId)
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey("DEMURRAGE WAIVER", ILocationId, IEmployeeId, DateTime.Now);
            return functionReturnValue;
        }

        #endregion " Generat Protocol "

        #region "Fetch Grid Details"

        /// <summary>
        /// Fetches the grid details.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="VoyTrnPk">The voy TRN pk.</param>
        /// <param name="FlightNr">The flight nr.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="Curr">The curr.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefID">The reference identifier.</param>
        /// <param name="IsActual">The is actual.</param>
        /// <param name="IsEstimated">The is estimated.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchGridDetails(string JCPK = "", string LocFk = "", string CustPK = "", string VoyTrnPk = "", string FlightNr = "", string FromDt = "", string ToDt = "", int BizType = 0, int CargoType = 0, int JobType = 0,
        int Curr = 0, int RefType = 0, string RefID = "", int IsActual = 0, int IsEstimated = 0, Int32 flag = 0, Int32 Excel = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with13 = objWF.MyCommand.Parameters;
                _with13.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with13.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with13.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with13.Add("FLIGHT_NR_IN", (string.IsNullOrEmpty(FlightNr) ? "" : FlightNr)).Direction = ParameterDirection.Input;
                _with13.Add("CUSTOMER_PK_IN", (string.IsNullOrEmpty(CustPK) ? "" : CustPK)).Direction = ParameterDirection.Input;
                _with13.Add("JCPK_IN", (string.IsNullOrEmpty(JCPK) ? "" : JCPK)).Direction = ParameterDirection.Input;
                _with13.Add("REF_TYPE_IN", RefType).Direction = ParameterDirection.Input;
                _with13.Add("REF_NR", (string.IsNullOrEmpty(RefID) ? "" : RefID)).Direction = ParameterDirection.Input;
                _with13.Add("CURR_PK_IN", Curr).Direction = ParameterDirection.Input;
                _with13.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with13.Add("TODATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with13.Add("LOC_PK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with13.Add("IS_ACT", IsActual).Direction = ParameterDirection.Input;
                _with13.Add("IS_EST", IsEstimated).Direction = ParameterDirection.Input;
                _with13.Add("ISREPORT_DATA_IN", Excel).Direction = ParameterDirection.Input;
                _with13.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with13.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with13.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with13.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DEMURRAGE_PKG", "FETCH_DETDEM_ACTUALS");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Fetch Grid Details"
    }
}