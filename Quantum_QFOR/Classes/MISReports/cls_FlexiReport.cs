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
using Oracle.DataAccess.Types;
using System;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_FlexiReport : CommonFeatures
    {
        #region " FetchModules "

        /// <summary>
        /// Fetches the modules.
        /// </summary>
        /// <param name="FavoriteName">Name of the favorite.</param>
        /// <returns></returns>
        public DataSet FetchModules(string FavoriteName)
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
                _with1.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_MODULES";
                _with1.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with1.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", FavoriteName).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with1.Fill(ds);

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

        #endregion " FetchModules "

        #region " FetchFields "

        /// <summary>
        /// Fetches the fields.
        /// </summary>
        /// <param name="FavoriteName">Name of the favorite.</param>
        /// <param name="RepID">The rep identifier.</param>
        /// <returns></returns>
        public DataSet FetchFields(string FavoriteName, string RepID)
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
                _with2.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_FIELDS";
                _with2.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with2.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", FavoriteName).Direction = ParameterDirection.Input;
                _with2.SelectCommand.Parameters.Add("REPORT_ID_IN", RepID).Direction = ParameterDirection.Input;
                _with2.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

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

        #endregion " FetchFields "

        #region " FetchCrtFields "

        /// <summary>
        /// Fetches the CRT fields.
        /// </summary>
        /// <param name="FavoritesName">Name of the favorites.</param>
        /// <returns></returns>
        public DataSet FetchCrtFields(string FavoritesName)
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
                _with3.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_CRT_FIELDS";
                _with3.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with3.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", FavoritesName).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

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

        #endregion " FetchCrtFields "

        #region " FetchGrpFields "

        /// <summary>
        /// Fetches the GRP fields.
        /// </summary>
        /// <param name="FavoritesName">Name of the favorites.</param>
        /// <returns></returns>
        public DataSet FetchGrpFields(string FavoritesName)
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
                _with4.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_GRP_FIELDS";
                _with4.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with4.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", FavoritesName).Direction = ParameterDirection.Input;
                _with4.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

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

        #endregion " FetchGrpFields "

        #region " FetchGroup "

        /// <summary>
        /// Fetches the group.
        /// </summary>
        /// <param name="FavoritesName">Name of the favorites.</param>
        /// <returns></returns>
        public DataSet FetchGroup(string FavoritesName)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with5 = objWF.MyDataAdapter;
                _with5.SelectCommand = new OracleCommand();
                _with5.SelectCommand.Connection = objWF.MyConnection;
                _with5.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_GROUP";
                _with5.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with5.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", FavoritesName).Direction = ParameterDirection.Input;
                _with5.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with5.Fill(ds);

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

        #endregion " FetchGroup "

        #region " FetchTotal "

        /// <summary>
        /// Fetches the total.
        /// </summary>
        /// <param name="FavoritesName">Name of the favorites.</param>
        /// <returns></returns>
        public DataSet FetchTotal(string FavoritesName)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with6 = objWF.MyDataAdapter;
                _with6.SelectCommand = new OracleCommand();
                _with6.SelectCommand.Connection = objWF.MyConnection;
                _with6.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_TOTAL";
                _with6.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with6.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", FavoritesName).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with6.Fill(ds);

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

        #endregion " FetchTotal "

        #region " FetchCriteria "

        /// <summary>
        /// Fetches the criteria.
        /// </summary>
        /// <param name="FavoritesName">Name of the favorites.</param>
        /// <returns></returns>
        public DataSet FetchCriteria(string FavoritesName)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with7 = objWF.MyDataAdapter;
                _with7.SelectCommand = new OracleCommand();
                _with7.SelectCommand.Connection = objWF.MyConnection;
                _with7.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_CRITERIA";
                _with7.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with7.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", FavoritesName).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with7.Fill(ds);

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

        #endregion " FetchCriteria "

        #region " FetchDetails "

        /// <summary>
        /// Fetches the details.
        /// </summary>
        /// <param name="FavoritesName">Name of the favorites.</param>
        /// <returns></returns>
        public DataSet FetchDetails(string FavoritesName)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with8 = objWF.MyDataAdapter;
                _with8.SelectCommand = new OracleCommand();
                _with8.SelectCommand.Connection = objWF.MyConnection;
                _with8.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_DETAILS";
                _with8.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with8.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", FavoritesName).Direction = ParameterDirection.Input;
                _with8.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with8.Fill(ds);

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

        #endregion " FetchDetails "

        #region " FetchCrtSearch "

        /// <summary>
        /// Fetches the CRT search.
        /// </summary>
        /// <param name="FieldPK">The field pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchCrtSearch(long FieldPK, Int32 CurrentPage, Int32 TotalPage)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with9 = objWF.MyDataAdapter;
                _with9.SelectCommand = new OracleCommand();
                _with9.SelectCommand.Connection = objWF.MyConnection;
                _with9.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_CRITERIA_SEARCH";
                _with9.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with9.SelectCommand.Parameters.Add("FR_FIELDS_MST_FK_IN", FieldPK).Direction = ParameterDirection.Input;
                _with9.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with9.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = M_MasterPageSize;
                _with9.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with9.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with9.Fill(ds);
                TotalPage = Convert.ToInt32(objWF.MyDataAdapter.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyDataAdapter.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);

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

        #endregion " FetchCrtSearch "

        #region " FetchFavorites "

        /// <summary>
        /// Fetches the favorites.
        /// </summary>
        /// <param name="FavoriteName">Name of the favorite.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SearchFlg">The search FLG.</param>
        /// <returns></returns>
        public DataSet FetchFavorites(string FavoriteName, Int32 CurrentPage, Int32 TotalPage, string SearchFlg)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with10 = objWF.MyDataAdapter;
                _with10.SelectCommand = new OracleCommand();
                _with10.SelectCommand.Connection = objWF.MyConnection;
                _with10.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_FAVORITES";
                _with10.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with10.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", (string.IsNullOrEmpty(FavoriteName) ? "" : FavoriteName)).Direction = ParameterDirection.Input;
                _with10.SelectCommand.Parameters.Add("SEARCH_FLG_IN", (string.IsNullOrEmpty(SearchFlg) ? "" : SearchFlg)).Direction = ParameterDirection.Input;
                _with10.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = M_MasterPageSize;
                _with10.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with10.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with10.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with10.Fill(ds);
                TotalPage = Convert.ToInt32(objWF.MyDataAdapter.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyDataAdapter.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);

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

        #endregion " FetchFavorites "

        #region " FetchFlexiReportView "

        /// <summary>
        /// Fetches the flexi report view.
        /// </summary>
        /// <param name="FavoriteName">Name of the favorite.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Criteria">The criteria.</param>
        /// <param name="Group">The group.</param>
        /// <param name="StrQry">The string qry.</param>
        /// <param name="ExcelExport">The excel export.</param>
        /// <returns></returns>
        public DataSet FetchFlexiReportView(string FavoriteName, Int32 CurrentPage, Int32 TotalPage, string Criteria, string Group, string StrQry, Int32 ExcelExport = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            int Biz_type = 0;

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with11 = objWF.MyDataAdapter;
                _with11.SelectCommand = new OracleCommand();
                _with11.SelectCommand.Connection = objWF.MyConnection;
                _with11.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_FLEXI_REPORT_PKG.FETCH_FLEXI_REPORT_VIEW";
                _with11.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with11.SelectCommand.Parameters.Add("FR_FAVORITES_NAME_IN", FavoriteName).Direction = ParameterDirection.Input;
                _with11.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;

                //If CType(HttpContext.Current.Session("BIZ_TYPE"), Int16) = 1 Then
                //    Biz_type = 1
                //ElseIf CType(HttpContext.Current.Session("BIZ_TYPE"), Int16) = 2 Then
                //    Biz_type = 2
                //End If
                //.SelectCommand.Parameters.Add("BIZ_TYPE_IN", Biz_type).Direction = ParameterDirection.Input
                _with11.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = M_MasterPageSize;
                _with11.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with11.SelectCommand.Parameters.Add("CRITERIA_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Output;
                _with11.SelectCommand.Parameters.Add("GROUP_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Output;
                _with11.SelectCommand.Parameters.Add("FR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with11.SelectCommand.Parameters.Add("FR_SUM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with11.SelectCommand.Parameters.Add("EXCEL_EXPORT_IN", OracleDbType.Int32).Value = ExcelExport;
                _with11.SelectCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                _with11.SelectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                _with11.Fill(ds);
                TotalPage = Convert.ToInt32(objWF.MyDataAdapter.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyDataAdapter.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);
                Criteria = (!string.IsNullOrEmpty(objWF.MyDataAdapter.SelectCommand.Parameters["CRITERIA_IN"].Value.ToString()) ? "" : objWF.MyDataAdapter.SelectCommand.Parameters["CRITERIA_IN"].Value.ToString());
                Group = (!string.IsNullOrEmpty(objWF.MyDataAdapter.SelectCommand.Parameters["GROUP_IN"].Value.ToString()) ? "" : objWF.MyDataAdapter.SelectCommand.Parameters["GROUP_IN"].Value.ToString());

                OracleClob clob = null;
                clob = (OracleClob)_with11.SelectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                StrQry = strReader.ReadToEnd();

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

        #endregion " FetchFlexiReportView "

        #region " SaveFields "

        /// <summary>
        /// Saves the fields.
        /// </summary>
        /// <param name="DS">The ds.</param>
        /// <returns></returns>
        public int SaveFields(DataSet DS)
        {
            WorkFlow objWK = new WorkFlow();
            Int32 rowCnt = default(Int32);
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            string strQry = null;

            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();

                strQry = " DELETE FROM QCOR_FR_FAVORITES_TBL FFT WHERE FFT.FR_FAVORITES_NAME = '-1' ";

                var _with12 = delCommand;
                _with12.Connection = objWK.MyConnection;
                _with12.Transaction = TRAN;
                _with12.CommandType = CommandType.Text;
                _with12.CommandText = strQry;
                _with12.ExecuteNonQuery();

                for (rowCnt = 0; rowCnt <= DS.Tables[0].Rows.Count - 1; rowCnt++)
                {
                    var _with13 = insCommand;
                    _with13.Connection = objWK.MyConnection;
                    _with13.CommandType = CommandType.StoredProcedure;
                    _with13.CommandText = objWK.MyUserName + ".FETCH_FLEXI_REPORT_PKG.QCOR_FR_FAVORITES_TBL_INS";
                    _with13.Parameters.Clear();

                    _with13.Parameters.Add("FR_FIELDS_MST_FK_IN", (string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["FR_FIELDS_MST_PK"].ToString()) ? "" : DS.Tables[0].Rows[rowCnt]["FR_FIELDS_MST_PK"])).Direction = ParameterDirection.Input;
                    _with13.Parameters["FR_FIELDS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("DISPLAY_ORDER_IN", rowCnt).Direction = ParameterDirection.Input;
                    _with13.Parameters["DISPLAY_ORDER_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with13.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with14 = objWK.MyDataAdapter;
                    _with14.InsertCommand = insCommand;
                    _with14.InsertCommand.Transaction = TRAN;
                    RecAfct = RecAfct + _with14.InsertCommand.ExecuteNonQuery();
                }

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return 0;
                }
                else
                {
                    TRAN.Commit();
                    return 1;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                TRAN.Rollback();
                return 0;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return 0;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion " SaveFields "

        #region " SaveCriteria "

        /// <summary>
        /// Saves the criteria.
        /// </summary>
        /// <param name="DS">The ds.</param>
        /// <returns></returns>
        public int SaveCriteria(DataSet DS)
        {
            WorkFlow objWK = new WorkFlow();
            Int32 rowCnt = default(Int32);
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            string strQry = null;

            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();

                strQry = " UPDATE QCOR_FR_FAVORITES_TBL FFT SET FFT.CRITERIA = 0, FFT.PARAMETER1 = NULL, FFT.PARAMETER2 = NULL WHERE FFT.FR_FAVORITES_NAME = '-1' ";

                var _with15 = delCommand;
                _with15.Connection = objWK.MyConnection;
                _with15.Transaction = TRAN;
                _with15.CommandType = CommandType.Text;
                _with15.CommandText = strQry;
                _with15.ExecuteNonQuery();

                for (rowCnt = 0; rowCnt <= DS.Tables[0].Rows.Count - 1; rowCnt++)
                {
                    var _with16 = updCommand;
                    _with16.Connection = objWK.MyConnection;
                    _with16.CommandType = CommandType.StoredProcedure;
                    _with16.CommandText = objWK.MyUserName + ".FETCH_FLEXI_REPORT_PKG.QCOR_FR_CRITERIA_UPD";
                    _with16.Parameters.Clear();

                    _with16.Parameters.Add("FR_FAVORITES_TBL_PK_IN", (string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["FR_FAVORITES_TBL_PK"].ToString()) ? "" : DS.Tables[0].Rows[rowCnt]["FR_FAVORITES_TBL_PK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["FR_FAVORITES_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("CRITERIA_IN", (string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["CRITERIA_FK"].ToString()) ? "" : DS.Tables[0].Rows[rowCnt]["CRITERIA_FK"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["CRITERIA_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PARAMETER1_IN", (string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["PARAMETER1"].ToString()) ? "" : DS.Tables[0].Rows[rowCnt]["PARAMETER1"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["PARAMETER1_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PARAMETER2_IN", (string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["PARAMETER2"].ToString()) ? "" : DS.Tables[0].Rows[rowCnt]["PARAMETER2"])).Direction = ParameterDirection.Input;
                    _with16.Parameters["PARAMETER2_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with16.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with17 = objWK.MyDataAdapter;
                    _with17.UpdateCommand = updCommand;
                    _with17.UpdateCommand.Transaction = TRAN;
                    RecAfct = RecAfct + _with17.UpdateCommand.ExecuteNonQuery();
                }

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return 0;
                }
                else
                {
                    TRAN.Commit();
                    return 1;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                TRAN.Rollback();
                return 0;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return 0;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion " SaveCriteria "

        #region " SaveGroup "

        /// <summary>
        /// Saves the group.
        /// </summary>
        /// <param name="DS">The ds.</param>
        /// <returns></returns>
        public int SaveGroup(DataSet DS)
        {
            WorkFlow objWK = new WorkFlow();
            Int32 rowCnt = default(Int32);
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            string strQry = null;

            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();

                strQry = " UPDATE QCOR_FR_FAVORITES_TBL FFT SET FFT.GROUP_BY = 0, FFT.ORDER_BY = 0 WHERE FFT.FR_FAVORITES_NAME = '-1' ";

                var _with18 = delCommand;
                _with18.Connection = objWK.MyConnection;
                _with18.Transaction = TRAN;
                _with18.CommandType = CommandType.Text;
                _with18.CommandText = strQry;
                _with18.ExecuteNonQuery();

                for (rowCnt = 0; rowCnt <= DS.Tables[0].Rows.Count - 1; rowCnt++)
                {
                    var _with19 = updCommand;
                    _with19.Connection = objWK.MyConnection;
                    _with19.CommandType = CommandType.StoredProcedure;
                    _with19.CommandText = objWK.MyUserName + ".FETCH_FLEXI_REPORT_PKG.QCOR_FR_GROUP_UPD";
                    _with19.Parameters.Clear();

                    _with19.Parameters.Add("FR_FAVORITES_TBL_PK_IN", (string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["FR_FAVORITES_TBL_PK"].ToString()) ? "" : DS.Tables[0].Rows[rowCnt]["FR_FAVORITES_TBL_PK"])).Direction = ParameterDirection.Input;
                    _with19.Parameters["FR_FAVORITES_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["GROUP_BY"].ToString()))
                    {
                        if (Convert.ToBoolean(DS.Tables[0].Rows[rowCnt]["GROUP_BY"]) == true)
                        {
                            _with19.Parameters.Add("GROUP_BY_IN", 1).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with19.Parameters.Add("GROUP_BY_IN", 0).Direction = ParameterDirection.Input;
                        }
                    }
                    else
                    {
                        _with19.Parameters.Add("GROUP_BY_IN", 0).Direction = ParameterDirection.Input;
                    }
                    _with19.Parameters["GROUP_BY_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("ORDER_BY_IN", (string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["ORDER_BY_FK"].ToString()) ? "" : DS.Tables[0].Rows[rowCnt]["ORDER_BY_FK"])).Direction = ParameterDirection.Input;
                    _with19.Parameters["ORDER_BY_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with19.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with20 = objWK.MyDataAdapter;
                    _with20.UpdateCommand = updCommand;
                    _with20.UpdateCommand.Transaction = TRAN;
                    RecAfct = RecAfct + _with20.UpdateCommand.ExecuteNonQuery();
                }

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return 0;
                }
                else
                {
                    TRAN.Commit();
                    return 1;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                TRAN.Rollback();
                return 0;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return 0;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion " SaveGroup "

        #region " SaveTotal "

        /// <summary>
        /// Saves the total.
        /// </summary>
        /// <param name="DS">The ds.</param>
        /// <returns></returns>
        public int SaveTotal(DataSet DS)
        {
            WorkFlow objWK = new WorkFlow();
            Int32 rowCnt = default(Int32);
            Int32 RecAfct = default(Int32);
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            string strQry = null;

            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();

                strQry = " UPDATE QCOR_FR_FAVORITES_TBL FFT SET FFT.SUM = 0, FFT.AVG = 0, FFT.MIN = 0, FFT.MAX = 0, FFT.COUNT = 0 WHERE FFT.FR_FAVORITES_NAME = '-1' ";

                var _with21 = delCommand;
                _with21.Connection = objWK.MyConnection;
                _with21.Transaction = TRAN;
                _with21.CommandType = CommandType.Text;
                _with21.CommandText = strQry;
                _with21.ExecuteNonQuery();

                for (rowCnt = 0; rowCnt <= DS.Tables[0].Rows.Count - 1; rowCnt++)
                {
                    var _with22 = updCommand;
                    _with22.Connection = objWK.MyConnection;
                    _with22.CommandType = CommandType.StoredProcedure;
                    _with22.CommandText = objWK.MyUserName + ".FETCH_FLEXI_REPORT_PKG.QCOR_FR_TOTAL_UPD";
                    _with22.Parameters.Clear();

                    _with22.Parameters.Add("FR_FAVORITES_TBL_PK_IN", (string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["FR_FAVORITES_TBL_PK"].ToString()) ? "" : DS.Tables[0].Rows[rowCnt]["FR_FAVORITES_TBL_PK"])).Direction = ParameterDirection.Input;
                    _with22.Parameters["FR_FAVORITES_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["SUM"].ToString()))
                    {
                        if (Convert.ToBoolean(DS.Tables[0].Rows[rowCnt]["SUM"]) == true)
                        {
                            _with22.Parameters.Add("SUM_IN", 1).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with22.Parameters.Add("SUM_IN", 0).Direction = ParameterDirection.Input;
                        }
                    }
                    else
                    {
                        _with22.Parameters.Add("SUM_IN", 0).Direction = ParameterDirection.Input;
                    }
                    _with22.Parameters["SUM_IN"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["AVG"].ToString()))
                    {
                        if (Convert.ToBoolean(DS.Tables[0].Rows[rowCnt]["AVG"]) == true)
                        {
                            _with22.Parameters.Add("AVG_IN", 1).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with22.Parameters.Add("AVG_IN", 0).Direction = ParameterDirection.Input;
                        }
                    }
                    else
                    {
                        _with22.Parameters.Add("AVG_IN", 0).Direction = ParameterDirection.Input;
                    }
                    _with22.Parameters["AVG_IN"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["MIN"].ToString()))
                    {
                        if (Convert.ToBoolean(DS.Tables[0].Rows[rowCnt]["MIN"]) == true)
                        {
                            _with22.Parameters.Add("MIN_IN", 1).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with22.Parameters.Add("MIN_IN", 0).Direction = ParameterDirection.Input;
                        }
                    }
                    else
                    {
                        _with22.Parameters.Add("MIN_IN", 0).Direction = ParameterDirection.Input;
                    }
                    _with22.Parameters["MIN_IN"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["MAX"].ToString()))
                    {
                        if (Convert.ToBoolean(DS.Tables[0].Rows[rowCnt]["MAX"]) == true)
                        {
                            _with22.Parameters.Add("MAX_IN", 1).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with22.Parameters.Add("MAX_IN", 0).Direction = ParameterDirection.Input;
                        }
                    }
                    else
                    {
                        _with22.Parameters.Add("MAX_IN", 0).Direction = ParameterDirection.Input;
                    }
                    _with22.Parameters["MAX_IN"].SourceVersion = DataRowVersion.Current;

                    if (!string.IsNullOrEmpty(DS.Tables[0].Rows[rowCnt]["COUNT"].ToString()))
                    {
                        if (Convert.ToBoolean(DS.Tables[0].Rows[rowCnt]["COUNT"]) == true)
                        {
                            _with22.Parameters.Add("COUNT_IN", 1).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with22.Parameters.Add("COUNT_IN", 0).Direction = ParameterDirection.Input;
                        }
                    }
                    else
                    {
                        _with22.Parameters.Add("COUNT_IN", 0).Direction = ParameterDirection.Input;
                    }
                    _with22.Parameters["COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with22.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with23 = objWK.MyDataAdapter;
                    _with23.UpdateCommand = updCommand;
                    _with23.UpdateCommand.Transaction = TRAN;
                    RecAfct = RecAfct + _with23.UpdateCommand.ExecuteNonQuery();
                }

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return 0;
                }
                else
                {
                    TRAN.Commit();
                    return 1;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                TRAN.Rollback();
                return 0;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return 0;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion " SaveTotal "

        #region " SaveFavorites "

        /// <summary>
        /// Saves the favorites.
        /// </summary>
        /// <param name="strFavoriteName">Name of the string favorite.</param>
        /// <param name="strNewFavoriteName">Name of the string new favorite.</param>
        /// <returns></returns>
        public int SaveFavorites(string strFavoriteName, string strNewFavoriteName)
        {
            WorkFlow objWK = new WorkFlow();
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleTransaction TRAN = null;

            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();

                var _with24 = insCommand;
                _with24.Connection = objWK.MyConnection;
                _with24.CommandType = CommandType.StoredProcedure;
                _with24.CommandText = objWK.MyUserName + ".FETCH_FLEXI_REPORT_PKG.QCOR_FR_ADD_FAVORITES";
                _with24.Parameters.Clear();

                _with24.Parameters.Add("FR_FAVORITES_NAME_IN", strFavoriteName).Direction = ParameterDirection.Input;
                _with24.Parameters["FR_FAVORITES_NAME_IN"].SourceVersion = DataRowVersion.Current;

                _with24.Parameters.Add("FR_NEW_FAVORITES_NAME_IN", strNewFavoriteName).Direction = ParameterDirection.Input;
                _with24.Parameters["FR_NEW_FAVORITES_NAME_IN"].SourceVersion = DataRowVersion.Current;

                _with24.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with24.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with25 = objWK.MyDataAdapter;
                _with25.InsertCommand = insCommand;
                _with25.InsertCommand.Transaction = TRAN;
                RecAfct = _with25.InsertCommand.ExecuteNonQuery();

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return 0;
                }
                else
                {
                    TRAN.Commit();
                    return 1;
                }
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                TRAN.Rollback();
                return 0;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return 0;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion " SaveFavorites "
    }
}