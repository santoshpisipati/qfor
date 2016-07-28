#region "Comments"

//'***************************************************************************************************************
//'*  Company Name            :
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By              :    Santosh on 31-May-16
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

using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class Cls_BookingEntry : CommonFeatures
    {
        #region "Private Variables"

        private long _PkValueMain;
        private long _PkValueTrans;
        private cls_CargoDetails objCargoDetails = new cls_CargoDetails();
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
        public string strRet;

        #endregion "Private Variables"

        private Int16 Chk_EBK = 0;

        #region "Property"

        public long PkValueMain
        {
            get { return _PkValueMain; }
        }

        public long PkValueTrans
        {
            get { return _PkValueTrans; }
        }

        private int ComGrp;

        public int CommodityGroup
        {
            get { return ComGrp; }
            set { ComGrp = value; }
        }

        private string _Containers;

        public string Containers
        {
            get { return _Containers; }
            set { _Containers = value; }
        }

        #endregion "Property"

        #region "Listing"

        public DataSet FetchListing(string Shipmentdate = "", string Commodityfk = "0", string BookingType = "0", string BookingPK = "", long CustomerPK = 0, string POLPK = "", string PODPK = "", Int16 intCargoType = 0, Int16 intXBkg = 0, string intStatus = "",
        string strSearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, string blnSortAscending = "", long lngUserLocFk = 0, Int16 intCLAgt = 0, Int32 flag = 0, Int32 BizType = 2,
        Int32 ProcessType = 1, string VesselPk = "", bool ShowAllRecords = false, bool IsAdmin = false, string PONumber = "", long ShipperPK = 0, long ConsigneePK = 0, long NotifyPK = 0, long POO_FK = 0, long PFD_FK = 0,
        Int16 CrossBKG_Agent = 0, Int16 COLD_Agent = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string EbookingIn = Convert.ToString(HttpContext.Current.Session["EBOOKING_AVAILABLE"]);
            if (EbookingIn != "1")
                EbookingIn = "0";
            if (flag == 0)
            {
                //strCondition = strCondition & vbCrLf & " and 1=2 "
            }

            DataSet MainDS = new DataSet();
            objWF.MyConnection.Open();
            objWF.MyCommand = new OracleCommand();
            var _with1 = objWF.MyCommand;
            _with1.Connection = objWF.MyConnection;
            _with1.CommandType = CommandType.StoredProcedure;
            _with1.CommandText = objWF.MyUserName + ".BOOKING_MST_PKG.FETCH_BOOKING";
            _with1.Parameters.Clear();
            _with1.Parameters.Add("BOOKING_REF_NO_IN", BookingPK).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("BOOKING_TYPE_IN", BookingType).Direction = ParameterDirection.Input;
            if (string.IsNullOrEmpty(Shipmentdate))
            {
                _with1.Parameters.Add("BOOKING_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
            }
            else
            {
                _with1.Parameters.Add("BOOKING_DATE_IN", Convert.ToDateTime(Shipmentdate)).Direction = ParameterDirection.Input;
            }
            _with1.Parameters.Add("CUSTOMER_MST_FK_IN", CustomerPK).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("POL_IN", POLPK).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("POD_IN", PODPK).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("VSL_VOY_IN", VesselPk).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("COMM_GRP_IN", Commodityfk).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("CARGO_TYPE_IN", intCargoType).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("PO_NUMBER_IN", (!string.IsNullOrEmpty(PONumber.Trim()) ? PONumber : "")).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("STATUS_IN", intStatus).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("POL_AGENT_IN", intXBkg).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("EXECUTIVE_FK_IN", ExecPk).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("IS_ADMIN_IN", (IsAdmin ? 1 : 0)).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("EBOOKINGS_IN", EbookingIn).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("SHIPPER_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("CONSUGNEE_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("NOTIFY_MST_FK_IN", NotifyPK).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("POO_FK_IN", POO_FK).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("PFD_FK_IN", PFD_FK).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("M_PAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("LOGGED_LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("CROSS_BKG_IN", CrossBKG_Agent).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("CO_LOAD_IN", COLD_Agent).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
            _with1.Parameters.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
            _with1.Parameters.Add("BKG_CURR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            try
            {
                objWF.MyDataAdapter = new OracleDataAdapter(objWF.MyCommand);
                objWF.MyDataAdapter.Fill(MainDS);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTAL_PAGE_IN"].Value);
                objWF.MyCommand.Dispose();
                objWF.MyDataAdapter.Dispose();
                return MainDS;
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
            finally
            {
                objWF.CloseConnection();
            }
        }

        #region "Fetch Cargo type"

        public int FetchCargoType(string SBookingRefNo)
        {
            WorkFlow objWF = new WorkFlow();

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT BS.CARGO_TYPE");
            sb.Append("  FROM BOOKING_MST_TBL BS");
            sb.Append(" WHERE BS.BOOKING_REF_NO = '" + SBookingRefNo + "'");
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #endregion "Fetch Cargo type"

        #region "Get Booking Count"

        public int GetBookingCount(string BkgRefNr, long BkgPk)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(1000);
                sb.Append(" SELECT BAT.BOOKING_MST_PK, BAT.BOOKING_REF_NO");
                sb.Append(" FROM BOOKING_MST_TBL BAT");
                sb.Append(" WHERE BAT.BOOKING_REF_NO LIKE '%" + BkgRefNr + "%'");
                DataSet ds = new DataSet();
                ds = (new WorkFlow()).GetDataSet(sb.ToString());
                if (ds.Tables[0].Rows.Count == 1)
                {
                    BkgRefNr = Convert.ToString(ds.Tables[0].Rows[0][1]);
                    BkgPk = Convert.ToInt32(ds.Tables[0].Rows[0][0]);
                }
                return ds.Tables[0].Rows.Count;
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

        #endregion "Get Booking Count"

        #endregion "Listing"

        #region "Basic Details"

        public DataTable GetBasicBkgDetails(int BookingPk)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtBasic = new DataTable();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT BKG.BOOKING_MST_PK,");
            sb.Append("       BKG.BOOKING_REF_NO,");
            sb.Append("       BKG.CARGO_TYPE,");
            sb.Append("       BKG.BUSINESS_TYPE  BIZ,");
            sb.Append("       BKG.FROM_FLAG      PROCESS");
            sb.Append("  FROM BOOKING_MST_TBL BKG");
            sb.Append(" WHERE BKG.BOOKING_MST_PK = " + BookingPk);
            try
            {
                dtBasic = objWF.GetDataTable(sb.ToString());
            }
            catch (Exception ex)
            {
            }
            return dtBasic;
        }

        #endregion "Basic Details"

        #region "Fetch Sea Existing Booking Entry Details"

        public Int16 fetchBkgStatus(long PKVal)
        {
            try
            {
                string strSQL = "";
                string Status = "";
                WorkFlow objwf = new WorkFlow();
                strSQL = "select BOOKING_TRN_PK from BOOKING_TRN where BOOKING_MST_FK='" + PKVal + "'";
                Status = objwf.ExecuteScaler(strSQL);
                if (!string.IsNullOrEmpty(Status))
                {
                    strSQL = "SELECT BOOKING_TRN_FRT_PK FROM BOOKING_TRN_FRT_DTLS WHERE BOOKING_TRN_FK='" + Status + "'";
                    Status = "";
                    Status = objwf.ExecuteScaler(strSQL);
                    if (!string.IsNullOrEmpty(Status))
                    {
                        return 0;
                    }
                    else
                    {
                        return 1;
                    }
                }
                else
                {
                    return 1;
                }
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

        public DataSet FetchReference_from(Int16 Int_Bookingpk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objwf = new WorkFlow();

            sb.Append("SELECT TRN.TRANS_REFERED_FROM");
            sb.Append("  FROM BOOKING_MST_TBL MAIN, BOOKING_TRN TRN");
            sb.Append(" WHERE MAIN.BOOKING_MST_PK = TRN.BOOKING_MST_FK");
            if (Int_Bookingpk > 0)
            {
                sb.Append(" AND MAIN.BOOKING_MST_PK=" + Int_Bookingpk);
            }

            try
            {
                return (objwf.GetDataSet(sb.ToString()));
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

        public int FetchSurchargeCnt(Int16 EBkgpk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objwf = new WorkFlow();
            DataSet DsFreightCnt = null;
            DataSet DsEbkg = null;
            int Bookingpk = 0;
            try
            {
                //'for the no of record
                sb.Append("   SELECT COUNT(*) SLNR ");
                sb.Append("   FROM BOOKING_MST_TBL          MAIN,");
                sb.Append("       BOOKING_TRN  TRN,");
                sb.Append("       BOOKING_TRN_FRT_DTLS FD");
                sb.Append("    WHERE MAIN.BOOKING_MST_PK = TRN.BOOKING_MST_FK");
                sb.Append("    AND TRN.BOOKING_TRN_PK = FD.BOOKING_TRN_FK");
                sb.Append("    AND MAIN.BOOKING_MST_PK = " + EBkgpk);

                DsFreightCnt = objwf.GetDataSet(sb.ToString());

                //'for the Ebooking
                sb.Remove(0, sb.Length);
                sb.Append("  SELECT MAIN.IS_EBOOKING FROM BOOKING_MST_TBL MAIN ");
                sb.Append("  WHERE MAIN.BOOKING_MST_PK = " + EBkgpk);
                DsEbkg = objwf.GetDataSet(sb.ToString());

                //'record in the transaction
                if (Convert.ToInt32(DsFreightCnt.Tables[0].Rows[0]["SLNR"]) > 0)
                {
                    Bookingpk = EBkgpk;
                    //'if no record in the transaction
                }
                else
                {
                    if (DsEbkg.Tables[0].Rows.Count > 0)
                    {
                        if (Convert.ToInt32(DsEbkg.Tables[0].Rows[0]["IS_EBOOKING"]) == 1)
                        {
                            Bookingpk = 0;
                        }
                        else
                        {
                            Bookingpk = EBkgpk;
                        }
                    }
                    else
                    {
                        Bookingpk = EBkgpk;
                    }
                }

                return (Bookingpk);
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

        public DataSet FetchBEntry(DataSet dsBEntry, long lngSBEPk, Int16 EBkg = 0, Int16 Bsts = 0, short BizType = 2)
        {
            if (BizType == 1)
            {
                return FetchBEntryAir(dsBEntry, lngSBEPk, EBkg, Bsts);
            }
            //**************************************************************************
            try
            {
                DataTable dtMain = new DataTable();
                DataTable dtTrans = new DataTable();
                DataTable dtFreight = new DataTable();
                Int16 intIsLcl = default(Int16);
                dtMain = (DataTable)FetchSBEntryHeader(dtMain, lngSBEPk);
                //Updated
                //If FCL then intIsLcl is 0- (//FCL=1 AND LCL=2//)
                if (Convert.ToInt32(dtMain.Rows[0]["CARGO_TYPE"]) == 1)
                {
                    intIsLcl = 0;
                    //LCL FALSE
                }
                else if (Convert.ToInt32(dtMain.Rows[0]["CARGO_TYPE"]) == 2)
                {
                    intIsLcl = 1;
                    //LCL TRUE
                }
                else
                {
                    intIsLcl = 4;
                    //BBC TRUE
                }

                dtTrans = (DataTable)FetchSBEntryTrans(dtTrans, lngSBEPk, intIsLcl);
                //Updated
                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (EBkg == 1 & Bsts == 1)
                    {
                        dtFreight = (DataTable)FetchEBKGEntryFreight(dtFreight, lngSBEPk, intIsLcl);
                        //Updated 'it will work only for FCL
                    }
                    else
                    {
                        dtFreight = (DataTable)FetchSBEntryFreight(dtFreight, lngSBEPk, intIsLcl, EBkg);
                        //Updated
                    }
                }
                else
                {
                    dtFreight = (DataTable)FetchSBEntryFreight(dtFreight, lngSBEPk, intIsLcl);
                    //Updated
                }
                dsBEntry.Tables.Add(dtTrans);
                dsBEntry.Tables.Add(dtFreight);
                if (dsBEntry.Tables.Count > 1)
                {
                    if (dsBEntry.Tables[1].Rows.Count > 0)
                    {
                        if (intIsLcl == 4)
                        {
                            DataRelation rel = new DataRelation("rl_TRAN_FREIGHT", new DataColumn[] {
                                dsBEntry.Tables[0].Columns["REFNO"],
                                dsBEntry.Tables[0].Columns["COMMODITYPK"]
                            }, new DataColumn[] {
                                dsBEntry.Tables[1].Columns["REFNO"],
                                dsBEntry.Tables[1].Columns["COMMODITY_MST_PK"]
                            });
                            dsBEntry.Relations.Clear();
                            //rel.Nested = True
                            dsBEntry.Relations.Add(rel);
                        }
                        else if (intIsLcl == 0)
                        {
                            DataRelation rel = new DataRelation("rl_TRAN_FREIGHT", new DataColumn[] {
                                dsBEntry.Tables[0].Columns["REFNO"],
                                dsBEntry.Tables[0].Columns["TYPE"]
                            }, new DataColumn[] {
                                dsBEntry.Tables[1].Columns["REFNO"],
                                dsBEntry.Tables[1].Columns["TYPE"]
                            });
                            dsBEntry.Relations.Clear();
                            //rel.Nested = True
                            dsBEntry.Relations.Add(rel);
                        }
                        else
                        {
                            DataRelation rel = new DataRelation("rl_TRAN_FREIGHT", new DataColumn[] {
                                dsBEntry.Tables[0].Columns["REFNO"],
                                dsBEntry.Tables[0].Columns["BASIS"]
                            }, new DataColumn[] {
                                dsBEntry.Tables[1].Columns["REFNO"],
                                dsBEntry.Tables[1].Columns["BASIS"]
                            });
                            dsBEntry.Relations.Clear();
                            //rel.Nested = True
                            dsBEntry.Relations.Add(rel);
                        }
                    }
                }

                DataSet ds = new DataSet();
                ds.Tables.Add(dtMain);
                return ds;
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

        public object FetchSBOFreight(DataTable dtMain, long lngSBEPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING BOOKING
            strBuilder.Append(" SELECT ");
            strBuilder.Append(" BTSOC.FREIGHT_ELEMENT_MST_FK, ");
            strBuilder.Append(" BTSOC.CURRENCY_MST_FK, ");
            strBuilder.Append(" BTSOC.AMOUNT, ");
            strBuilder.Append(" BTSOC.FREIGHT_TYPE ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" BOOKING_MST_TBL BST, ");
            strBuilder.Append(" BOOKING_TRN_OTH_CHRG BTSOC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" BTSOC.BOOKING_MST_FK = BST.BOOKING_MST_PK ");
            strBuilder.Append(" AND BTSOC.BOOKING_MST_FK= " + lngSBEPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
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

        public Int32 FetchBkgPk(Int32 jobpk)
        {
            string strSql = "";
            WorkFlow objwf = new WorkFlow();
            //Fetch the Cargo Dimesion for the existing booking
            strSql = "select sea.BOOKING_MST_FK from job_card_trn sea where sea.job_card_trn_pk=" + jobpk;
            try
            {
                return Convert.ToInt32(objwf.ExecuteScaler(strSql));
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

        public Int32 fetchBKPk(string RefNo)
        {
            string strSql = "";
            string Ref = null;
            WorkFlow objwf = new WorkFlow();
            //Fetch the Cargo Dimesion for the existing booking
            Ref = RefNo.Substring(0, 3);
            if (Ref == "EBK")
            {
                strSql = "select e_bkg_order_nr_pk from syn_ebk_m_booking sea where e_bkg_order_ref_nr='" + RefNo + "'";
            }
            else
            {
                if (Ref == "BKG")
                {
                    strSql = "select e_bkg_order_nr_pk from syn_ebk_m_booking sea where qbso_bkg_ref_nr='" + RefNo + "'";
                }
            }

            try
            {
                return Convert.ToInt32(objwf.ExecuteScaler(strSql));
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

        public object FetchComm(long SeaBkgPK, int status)
        {
            string strSqlCDimension = "";
            WorkFlow objwf = new WorkFlow();
            try
            {
                if (status == 1)
                {
                    strSqlCDimension = " select c1.commodity_name,(select bb.pack_count  from booking_mst_tbl bb where bb.booking_mst_pk=" + SeaBkgPK + ") pack_count, " + " (select pack.pack_type_desc from PACK_TYPE_MST_TBL pack where pack.pack_type_mst_pk in (select bb.pack_typE_mst_fk from booking_mst_tbl bb where bb.booking_mst_pk=" + SeaBkgPK + ")) pack_type_desc " + " from commodity_mst_tbl c1 where c1.commodity_mst_pk in ( " + " select bb.commodity_mst_fk from BOOKING_TRN bb where bb.booking_mst_fk=" + SeaBkgPK + ") ";

                    //ElseIf status = 2 Or status = 3 Then
                }
                else
                {
                    strSqlCDimension = "select distinct(commodity_name) ," + "BST.pack_count," + "PTMT.PACK_TYPE_DESC " + "from commodity_mst_tbl CST," + "job_card_trn JCSE, " + "job_trn_cont JTSE,BOOKING_mst_TBL BST, " + "PACK_TYPE_MST_TBL PTMT" + "where JTSE.commodity_mst_fk=CST.commodity_mst_pk" + "and PTMT.PACK_TYPE_MST_PK=BST.PACK_TYPE_MST_FK " + "and JTSE.job_card_trn_fk=JCSE.job_card_trn_pk  " + "and BST.booking_mst_pk=JCSE.booking_mst_fk" + "and BOOKING_mst_PK= " + SeaBkgPK;
                }
                return objwf.GetDataSet(strSqlCDimension);
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

        public DataTable fetchSpclReq(string strPK, string strRef)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT QUOTATION_HAZ_REF_ODC_SPL_PK,");
                    strQuery.Append("QUOTATION_DTL_FK,");
                    strQuery.Append("OUTER_PACK_TYPE_MST_FK,");
                    strQuery.Append("INNER_PACK_TYPE_MST_FK,");
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("FLASH_PNT_TEMP,");
                    strQuery.Append("FLASH_PNT_TEMP_UOM,");
                    strQuery.Append("IMDG_CLASS_CODE,");
                    strQuery.Append("UN_NO,");
                    strQuery.Append("IMO_SURCHARGE,");
                    strQuery.Append("SURCHARGE_AMT,");
                    strQuery.Append("IS_MARINE_POLLUTANT,");
                    strQuery.Append("EMS_NUMBER FROM QUOTATION_HAZ_SPL_ODC_REQ Q");
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.QUOTATION_DTL_FK=" + strPK);
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable fetchSpclReqReefer(string strPK, string strRef)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT QUOTATION_HAZ_REF_ODC_SPL_PK,");
                    strQuery.Append("QUOTATION_DTL_FK,");
                    strQuery.Append("VENTILATION,");
                    strQuery.Append("AIR_COOL_METHOD,");
                    strQuery.Append("HUMIDITY_FACTOR,");
                    strQuery.Append("IS_PERISHABLE_GOODS,");
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("PACK_TYPE_MST_FK,");
                    strQuery.Append("Q.PACK_COUNT ");
                    strQuery.Append("FROM QUOTATION_HAZ_SPL_ODC_REQ Q");
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.QUOTATION_DTL_FK=" + strPK);
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable fetchSpclReqODC(string strPK, string strRef)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT ");
                    strQuery.Append("QUOTATION_HAZ_REF_ODC_SPL_PK,");
                    strQuery.Append("QUOTATION_DTL_FK,");
                    strQuery.Append("LENGTH,");
                    strQuery.Append("LENGTH_UOM_MST_FK,");
                    strQuery.Append("HEIGHT,");
                    strQuery.Append("HEIGHT_UOM_MST_FK,");
                    strQuery.Append("WIDTH,");
                    strQuery.Append("WIDTH_UOM_MST_FK,");
                    strQuery.Append("WEIGHT,");
                    strQuery.Append("WEIGHT_UOM_MST_FK,");
                    strQuery.Append("VOLUME,");
                    strQuery.Append("VOLUME_UOM_MST_FK,");
                    strQuery.Append("SLOT_LOSS,");
                    strQuery.Append("LOSS_QUANTITY,");
                    strQuery.Append("APPR_REQ, ");
                    strQuery.Append("STOWAGE, ");
                    strQuery.Append("HANDLING_INSTR, ");
                    strQuery.Append("LASHING_INSTR ");
                    strQuery.Append("FROM QUOTATION_HAZ_SPL_ODC_REQ Q");
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.QUOTATION_DTL_FK=" + strPK);
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public List<SpecialReqClass> FetchSpecialReqAll(int BkgTrnSplPk = 0, int BookingMstPk = 0, int BookingTrnFk = 0)
        {
            DataTable dtReq = null;
            List<SpecialReqClass> listReq = new List<SpecialReqClass>();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT BSR.BKG_TRN_SPL_PK         BKG_TRN_SPL_PK,");
            sb.Append("       BSR.BOOKING_TRN_FK         BOOKING_TRN_FK,");
            sb.Append("       BSR.MAX_TEMP_UOM           RH_MAX_TEMP_UOM,");
            sb.Append("       BSR.MIN_TEMP               RH_MIN_TEMP,");
            sb.Append("       BSR.MIN_TEMP_UOM           RH_MIN_TEMP_UOM,");
            sb.Append("       BSR.MAX_TEMP               RH_MAX_TEMP,");
            sb.Append("       BSR.IS_PERISHABLE_GOODS    R_IS_PERISHABLE_GOODS,");
            sb.Append("       BSR.GENSET                 R_GENSET,");
            sb.Append("       BSR.DEHUMIDIFIER           R_DEHUMIDIFIER,");
            sb.Append("       BSR.AIR_COOL_METHOD        R_AIR_COOL_METHOD,");
            sb.Append("       BSR.REQ_SET_TEMP           R_REQ_SET_TEMP,");
            sb.Append("       BSR.HAULAGE                R_HAULAGE,");
            sb.Append("       BSR.AIR_VENTILATION_UOM    R_AIR_VENTILATION_UOM,");
            sb.Append("       BSR.FLOORDRAINS            R_FLOORDRAINS,");
            sb.Append("       BSR.VENTILATION            R_VENTILATION,");
            sb.Append("       BSR.PACK_COUNT             R_PACK_COUNT,");
            sb.Append("       BSR.PACK_TYPE_MST_FK       R_PACK_TYPE_MST_FK,");
            sb.Append("       BSR.HUMIDITY_FACTOR        R_HUMIDITY_FACTOR,");
            sb.Append("       BSR.DEFROSTING_INTERVAL    R_DEFROSTING_INTERVAL,");
            sb.Append("       BSR.AIR_VENTILATION        R_AIR_VENTILATION,");
            sb.Append("       BSR.REF_VENTILATION        R_REF_VENTILATION,");
            sb.Append("       BSR.REQ_SET_TEMP_UOM       R_REQ_SET_TEMP_UOM,");
            sb.Append("       BSR.O2                     R_O2,");
            sb.Append("       BSR.CO2                    R_CO2,");
            sb.Append("       BSR.PACK_CLASS_TYPE        H_PACK_CLASS_TYPE,");
            sb.Append("       BSR.IS_MARINE_POLLUTANT    H_IS_MARINE_POLLUTANT,");
            sb.Append("       BSR.IMO_SURCHARGE          H_IMO_SURCHARGE,");
            sb.Append("       BSR.FLASH_PNT_TEMP_UOM     H_FLASH_PNT_TEMP_UOM,");
            sb.Append("       BSR.IMDG_CLASS_CODE        H_IMDG_CLASS_CODE,");
            sb.Append("       BSR.UN_NO                  H_UN_NO,");
            sb.Append("       BSR.SURCHARGE_AMT          H_SURCHARGE_AMT,");
            sb.Append("       BSR.EMS_NUMBER             H_EMS_NUMBER,");
            sb.Append("       BSR.OUTER_PACK_TYPE_MST_FK H_OUTER_PACK_TYPE_MST_FK,");
            sb.Append("       BSR.PROPER_SHIPPING_NAME   H_PROPER_SHIPPING_NAME,");
            sb.Append("       BSR.FLASH_PNT_TEMP         H_FLASH_PNT_TEMP,");
            sb.Append("       BSR.INNER_PACK_TYPE_MST_FK H_INNER_PACK_TYPE_MST_FK,");
            sb.Append("       BSR.LENGTH                 O_LENGTH,");
            sb.Append("       BSR.SLOT_LOSS              O_SLOT_LOSS,");
            sb.Append("       BSR.LOSS_QUANTITY          O_LOSS_QUANTITY,");
            sb.Append("       BSR.VOLUME_UOM_MST_FK      O_VOLUME_UOM_MST_FK,");
            sb.Append("       BSR.STOWAGE                O_STOWAGE,");
            sb.Append("       BSR.WEIGHT_UOM_MST_FK      O_WEIGHT_UOM_MST_FK,");
            sb.Append("       BSR.WEIGHT                 O_WEIGHT,");
            sb.Append("       BSR.WIDTH                  O_WIDTH,");
            sb.Append("       BSR.HEIGHT_UOM_MST_FK      O_HEIGHT_UOM_MST_FK,");
            sb.Append("       BSR.HEIGHT                 O_HEIGHT,");
            sb.Append("       BSR.LENGTH_UOM_MST_FK      O_LENGTH_UOM_MST_FK,");
            sb.Append("       BSR.VOLUME                 O_VOLUME,");
            sb.Append("       BSR.WIDTH_UOM_MST_FK       O_WIDTH_UOM_MST_FK,");
            sb.Append("       BSR.HANDLING_INSTR         O_HANDLING_INSTR,");
            sb.Append("       BSR.LASHING_INSTR          O_LASHING_INSTR,");
            sb.Append("       BSR.NO_OF_SLOTS            O_NO_OF_SLOTS,");
            sb.Append("       BSR.APPR_REQ               O_APPR_REQ,");
            sb.Append("       BSR.REQ_TYPE               REQ_TYPE");
            sb.Append("  FROM BOOKING_TRN BT, BOOKING_TRN_SPL_REQ BSR");
            sb.Append(" WHERE BT.BOOKING_TRN_PK = BSR.BOOKING_TRN_FK");
            if (BkgTrnSplPk > 0)
            {
                sb.Append("   AND BSR.BKG_TRN_SPL_PK = " + BkgTrnSplPk);
            }
            if (BookingMstPk > 0)
            {
                sb.Append("   AND BT.BOOKING_MST_FK = " + BookingMstPk);
            }
            //If BookingTrnFk > 0 Then
            //    sb.Append("   AND BSR.BOOKING_TRN_FK = " & BookingTrnFk)
            //End If
            sb.Append("   AND BSR.BOOKING_TRN_FK = " + BookingTrnFk);
            try
            {
                dtReq = (new WorkFlow()).GetDataTable(sb.ToString());
                for (int _i = 0; _i <= dtReq.Rows.Count - 1; _i++)
                {
                    DataRow _req = dtReq.Rows[_i];
                    SpecialReqClass _itemReq = new SpecialReqClass();

                    var _with2 = _itemReq;
                    _with2.BKG_TRN_SPL_PK = Convert.ToString(_req["BKG_TRN_SPL_PK"]);
                    _with2.BKG_TRN_FK = Convert.ToString(_req["BOOKING_TRN_FK"]);

                    if (!string.IsNullOrEmpty(_req["RH_MAX_TEMP_UOM"].ToString()))
                        _with2.RH_MAX_TEMP_UOM = Convert.ToString(_req["RH_MAX_TEMP_UOM"]);
                    if (!string.IsNullOrEmpty(_req["RH_MIN_TEMP"].ToString()))
                        _with2.RH_MIN_TEMP = Convert.ToString(_req["RH_MIN_TEMP"]);
                    if (!string.IsNullOrEmpty(_req["RH_MIN_TEMP_UOM"].ToString()))
                        _with2.RH_MIN_TEMP_UOM = Convert.ToString(_req["RH_MIN_TEMP_UOM"]);
                    if (!string.IsNullOrEmpty(_req["RH_MAX_TEMP"].ToString()))
                        _with2.RH_MAX_TEMP = Convert.ToString(_req["RH_MAX_TEMP"]);
                    if (!string.IsNullOrEmpty(_req["R_IS_PERISHABLE_GOODS"].ToString()))
                        _with2.R_IS_PERISHABLE_GOODS = Convert.ToString(_req["R_IS_PERISHABLE_GOODS"]);
                    if (!string.IsNullOrEmpty(_req["R_GENSET"].ToString()))
                        _with2.R_GENSET = Convert.ToString(_req["R_GENSET"]);
                    if (!string.IsNullOrEmpty(_req["R_DEHUMIDIFIER"].ToString()))
                        _with2.R_DEHUMIDIFIER = Convert.ToString(_req["R_DEHUMIDIFIER"]);
                    if (!string.IsNullOrEmpty(_req["R_AIR_COOL_METHOD"].ToString()))
                        _with2.R_AIR_COOL_METHOD = Convert.ToString(_req["R_AIR_COOL_METHOD"]);
                    if (!string.IsNullOrEmpty(_req["R_REQ_SET_TEMP"].ToString()))
                        _with2.R_REQ_SET_TEMP = Convert.ToString(_req["R_REQ_SET_TEMP"]);
                    if (!string.IsNullOrEmpty(_req["R_HAULAGE"].ToString()))
                        _with2.R_HAULAGE = Convert.ToString(_req["R_HAULAGE"]);
                    if (!string.IsNullOrEmpty(_req["R_AIR_VENTILATION_UOM"].ToString()))
                        _with2.R_AIR_VENTILATION_UOM = Convert.ToString(_req["R_AIR_VENTILATION_UOM"]);
                    if (!string.IsNullOrEmpty(_req["R_FLOORDRAINS"].ToString()))
                        _with2.R_FLOORDRAINS = Convert.ToString(_req["R_FLOORDRAINS"]);
                    if (!string.IsNullOrEmpty(_req["R_VENTILATION"].ToString()))
                        _with2.R_VENTILATION = Convert.ToString(_req["R_VENTILATION"]);
                    if (!string.IsNullOrEmpty(_req["R_PACK_COUNT"].ToString()))
                        _with2.R_PACK_COUNT = Convert.ToString(_req["R_PACK_COUNT"]);
                    if (!string.IsNullOrEmpty(_req["R_PACK_TYPE_MST_FK"].ToString()))
                        _with2.R_PACK_TYPE_MST_FK = Convert.ToString(_req["R_PACK_TYPE_MST_FK"]);
                    if (!string.IsNullOrEmpty(_req["R_HUMIDITY_FACTOR"].ToString()))
                        _with2.R_HUMIDITY_FACTOR = Convert.ToString(_req["R_HUMIDITY_FACTOR"]);
                    if (!string.IsNullOrEmpty(_req["R_DEFROSTING_INTERVAL"].ToString()))
                        _with2.R_DEFROSTING_INTERVAL = Convert.ToString(_req["R_DEFROSTING_INTERVAL"]);
                    if (!string.IsNullOrEmpty(_req["R_AIR_VENTILATION"].ToString()))
                        _with2.R_AIR_VENTILATION = Convert.ToString(_req["R_AIR_VENTILATION"]);
                    if (!string.IsNullOrEmpty(_req["R_REF_VENTILATION"].ToString()))
                        _with2.R_REF_VENTILATION = Convert.ToString(_req["R_REF_VENTILATION"]);
                    if (!string.IsNullOrEmpty(_req["R_REQ_SET_TEMP_UOM"].ToString()))
                        _with2.R_REQ_SET_TEMP_UOM = Convert.ToString(_req["R_REQ_SET_TEMP_UOM"]);
                    if (!string.IsNullOrEmpty(_req["R_O2"].ToString()))
                        _with2.R_O2 = Convert.ToString(_req["R_O2"]);
                    if (!string.IsNullOrEmpty(_req["R_CO2"].ToString()))
                        _with2.R_CO2 = Convert.ToString(_req["R_CO2"]);
                    if (!string.IsNullOrEmpty(_req["O_LENGTH"].ToString()))
                        _with2.O_LENGTH = Convert.ToString(_req["O_LENGTH"]);
                    if (!string.IsNullOrEmpty(_req["O_SLOT_LOSS"].ToString()))
                        _with2.O_SLOT_LOSS = Convert.ToString(_req["O_SLOT_LOSS"]);
                    if (!string.IsNullOrEmpty(_req["O_LOSS_QUANTITY"].ToString()))
                        _with2.O_LOSS_QUANTITY = Convert.ToString(_req["O_LOSS_QUANTITY"]);
                    if (!string.IsNullOrEmpty(_req["O_VOLUME_UOM_MST_FK"].ToString()))
                        _with2.O_VOLUME_UOM_MST_FK = Convert.ToString(_req["O_VOLUME_UOM_MST_FK"]);
                    if (!string.IsNullOrEmpty(_req["O_STOWAGE"].ToString()))
                        _with2.O_STOWAGE = Convert.ToString(_req["O_STOWAGE"]);
                    if (!string.IsNullOrEmpty(_req["O_WEIGHT_UOM_MST_FK"].ToString()))
                        _with2.O_WEIGHT_UOM_MST_FK = Convert.ToString(_req["O_WEIGHT_UOM_MST_FK"]);
                    if (!string.IsNullOrEmpty(_req["O_WEIGHT"].ToString()))
                        _with2.O_WEIGHT = Convert.ToString(_req["O_WEIGHT"]);
                    if (!string.IsNullOrEmpty(_req["O_WIDTH"].ToString()))
                        _with2.O_WIDTH = Convert.ToString(_req["O_WIDTH"]);
                    if (!string.IsNullOrEmpty(_req["O_HEIGHT_UOM_MST_FK"].ToString()))
                        _with2.O_HEIGHT_UOM_MST_FK = Convert.ToString(_req["O_HEIGHT_UOM_MST_FK"]);
                    if (!string.IsNullOrEmpty(_req["O_HEIGHT"].ToString()))
                        _with2.O_HEIGHT = Convert.ToString(_req["O_HEIGHT"]);
                    if (!string.IsNullOrEmpty(_req["O_LENGTH_UOM_MST_FK"].ToString()))
                        _with2.O_LENGTH_UOM_MST_FK = Convert.ToString(_req["O_LENGTH_UOM_MST_FK"]);
                    if (!string.IsNullOrEmpty(_req["O_VOLUME"].ToString()))
                        _with2.O_VOLUME = Convert.ToString(_req["O_VOLUME"]);
                    if (!string.IsNullOrEmpty(_req["O_WIDTH_UOM_MST_FK"].ToString()))
                        _with2.O_WIDTH_UOM_MST_FK = Convert.ToString(_req["O_WIDTH_UOM_MST_FK"]);
                    if (!string.IsNullOrEmpty(_req["O_HANDLING_INSTR"].ToString()))
                        _with2.O_HANDLING_INSTR = Convert.ToString(_req["O_HANDLING_INSTR"]);
                    if (!string.IsNullOrEmpty(_req["O_LASHING_INSTR"].ToString()))
                        _with2.O_LASHING_INSTR = Convert.ToString(_req["O_LASHING_INSTR"]);
                    if (!string.IsNullOrEmpty(_req["O_NO_OF_SLOTS"].ToString()))
                        _with2.O_NO_OF_SLOTS = Convert.ToString(_req["O_NO_OF_SLOTS"]);
                    if (!string.IsNullOrEmpty(_req["O_APPR_REQ"].ToString()))
                        _with2.O_APPR_REQ = Convert.ToString(_req["O_APPR_REQ"]);
                    if (!string.IsNullOrEmpty(_req["H_PACK_CLASS_TYPE"].ToString()))
                        _with2.H_PACK_CLASS_TYPE = Convert.ToString(_req["H_PACK_CLASS_TYPE"]);
                    if (!string.IsNullOrEmpty(_req["H_IS_MARINE_POLLUTANT"].ToString()))
                        _with2.H_IS_MARINE_POLLUTANT = Convert.ToString(_req["H_IS_MARINE_POLLUTANT"]);
                    if (!string.IsNullOrEmpty(_req["H_IMO_SURCHARGE"].ToString()))
                        _with2.H_IMO_SURCHARGE = Convert.ToString(_req["H_IMO_SURCHARGE"]);
                    if (!string.IsNullOrEmpty(_req["H_FLASH_PNT_TEMP_UOM"].ToString()))
                        _with2.H_FLASH_PNT_TEMP_UOM = Convert.ToString(_req["H_FLASH_PNT_TEMP_UOM"]);
                    if (!string.IsNullOrEmpty(_req["H_IMDG_CLASS_CODE"].ToString()))
                        _with2.H_IMDG_CLASS_CODE = Convert.ToString(_req["H_IMDG_CLASS_CODE"]);
                    if (!string.IsNullOrEmpty(_req["H_UN_NO"].ToString()))
                        _with2.H_UN_NO = Convert.ToString(_req["H_UN_NO"]);
                    if (!string.IsNullOrEmpty(_req["H_SURCHARGE_AMT"].ToString()))
                        _with2.H_SURCHARGE_AMT = Convert.ToString(_req["H_SURCHARGE_AMT"]);
                    if (!string.IsNullOrEmpty(_req["H_EMS_NUMBER"].ToString()))
                        _with2.H_EMS_NUMBER = Convert.ToString(_req["H_EMS_NUMBER"]);
                    if (!string.IsNullOrEmpty(_req["H_OUTER_PACK_TYPE_MST_FK"].ToString()))
                        _with2.H_OUTER_PACK_TYPE_MST_FK = Convert.ToString(_req["H_OUTER_PACK_TYPE_MST_FK"]);
                    if (!string.IsNullOrEmpty(_req["H_PROPER_SHIPPING_NAME"].ToString()))
                        _with2.H_PROPER_SHIPPING_NAME = Convert.ToString(_req["H_PROPER_SHIPPING_NAME"]);
                    if (!string.IsNullOrEmpty(_req["H_FLASH_PNT_TEMP"].ToString()))
                        _with2.H_FLASH_PNT_TEMP = Convert.ToString(_req["H_FLASH_PNT_TEMP"]);
                    if (!string.IsNullOrEmpty(_req["H_INNER_PACK_TYPE_MST_FK"].ToString()))
                        _with2.H_INNER_PACK_TYPE_MST_FK = Convert.ToString(_req["H_INNER_PACK_TYPE_MST_FK"]);
                    if (!string.IsNullOrEmpty(_req["REQ_TYPE"].ToString()))
                        _with2.REQ_TYPE = Convert.ToInt32(_req["REQ_TYPE"]);

                    listReq.Add(_itemReq);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return listReq;
        }

        public object FetchSBEntryHeader(DataTable dtMain, long lngSBEPK, short BizType = 2)
        {
            WorkFlow objwf = new WorkFlow();
            objwf.MyCommand.Parameters.Clear();
            var _with3 = objwf.MyCommand;
            _with3.Parameters.Add("BOOKING_MST_PK_IN", lngSBEPK).Direction = ParameterDirection.Input;
            _with3.Parameters.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
            _with3.Parameters.Add("PROCESS_TYPE_IN", 1).Direction = ParameterDirection.Input;
            _with3.Parameters.Add("BKG_CURR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

            try
            {
                dtMain = objwf.GetDataTable("BOOKING_MST_PKG", "FETCH_BKG_ENTRY_HDR");
                return dtMain;
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

        public object FetchSBEntryTrans(DataTable dtTrans, long lngSBEPK, Int16 intIsLcl, short BizType = 2)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            //intIsLcl=1 if LCL, 0 IF FCL,4 IF BBC
            WorkFlow objwf = new WorkFlow();
            if (BizType == 1)
            {
                return FetchSBEntryTransAir(dtTrans, lngSBEPK);
            }
            if (intIsLcl == 1)
            {
                sb.Append("SELECT DISTINCT ");
                sb.Append(" NULL AS TRNTYPEPK, ");
                sb.Append(" BTSFL.TRANS_REFERED_FROM AS TRNTYPESTATUS, ");
                sb.Append(" DECODE(BTSFL.TRANS_REFERED_FROM,1,'Quote',2,'Sp Rate',3,'Cust Cont',4,'SL Tariff',5,'SRR',6,'Gen Tariff',7,'Manual',8,'Agent Tariff') AS CONTRACTTYPE, ");
                sb.Append(" BTSFL.TRANS_REF_NO AS REFNO, ");
                sb.Append(" OMT.OPERATOR_ID AS OPERATOR, ");
                sb.Append(" UOM.DIMENTION_ID AS BASIS, ");
                sb.Append(" BTSFL.QUANTITY AS QTY, ");
                sb.Append(" '' AS CARGO, ");
                sb.Append(" '' AS COMMODITY, ");
                sb.Append(" BTSFL.BUYING_RATE AS RATE, ");
                sb.Append(" FETCH_FCL_LCL_TOTALBKGFRT(BTSFL.BOOKING_TRN_PK,2," + HttpContext.Current.Session["CURRENCY_MST_PK"] + "," + BizType + ")AS BKGRATE, ");
                sb.Append(" NULL AS NET, ");
                sb.Append(" FETCH_FCL_LCL_TOTALBKGFRT(BTSFL.BOOKING_TRN_PK,1," + HttpContext.Current.Session["CURRENCY_MST_PK"] + "," + BizType + ")AS TOTALRATE, ");
                sb.Append(" '1' AS SEL, ");
                sb.Append(" NULL AS CONTAINERPK, ");
                sb.Append(" BTC.BOOKING_TRN_CARGO_PK AS CARGOPK, ");
                sb.Append(" BTSFL.COMMODITY_MST_FKS AS COMMODITYPK, ");
                sb.Append(" BST.CARRIER_MST_FK AS OPERATORPK, ");
                sb.Append(" BTSFL.BOOKING_TRN_PK AS TRANSACTIONPK, ");
                sb.Append(" UOM.DIMENTION_UNIT_MST_PK AS BASISPK ");
                sb.Append(" FROM ");
                sb.Append(" BOOKING_MST_TBL BST, ");
                sb.Append(" BOOKING_TRN BTSFL, ");
                sb.Append(" DIMENTION_UNIT_MST_TBL UOM, ");
                sb.Append(" COMMODITY_GROUP_MST_TBL CGMT, ");
                sb.Append(" COMMODITY_MST_TBL CMT, ");
                sb.Append(" OPERATOR_MST_TBL OMT, ");
                sb.Append(" BOOKING_TRN_CARGO_DTL BTC ");
                sb.Append(" WHERE(1 = 1) ");
                sb.Append(" AND BTSFL.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) ");
                sb.Append(" AND BTSFL.BOOKING_MST_FK=BST.BOOKING_MST_PK ");
                sb.Append(" AND BST.CARRIER_MST_FK=OMT.OPERATOR_MST_PK (+)");
                sb.Append(" AND BTSFL.BASIS=UOM.DIMENTION_UNIT_MST_PK (+)");
                sb.Append(" AND BTSFL.BOOKING_TRN_PK=BTC.BOOKING_TRN_FK(+)");
                sb.Append(" AND BST.BOOKING_MST_PK= " + lngSBEPK);
            }
            else if (intIsLcl == 0)
            {
                sb.Append("SELECT  TRNTYPEPK,  TRNTYPESTATUS,  CONTRACTTYPE,  REFNO, ");
                sb.Append("OPERATOR,  TYPE,  BOXES, CARGO, COMMODITY,  RATE,  BKGRATE,  NET,  TOTALRATE, ");
                sb.Append("SEL,CONTAINERPK, CARGOPK,  COMMODITYPK,  OPERATORPK, TRANSACTIONPK,  BASISPK FROM( ");
                sb.Append("SELECT DISTINCT ");
                sb.Append(" NULL AS TRNTYPEPK, ");
                sb.Append(" BTSFL.TRANS_REFERED_FROM AS TRNTYPESTATUS, ");
                sb.Append(" DECODE(BTSFL.TRANS_REFERED_FROM,1,'Quote',2,'Sp Rate',3,'Cust Cont',4,'SL Tariff',5,'SRR',6,'Gen Tariff',7,'Manual',8,'Agent Tariff') AS CONTRACTTYPE, ");
                sb.Append(" BTSFL.TRANS_REF_NO AS REFNO, ");
                sb.Append(" OMT.OPERATOR_ID AS OPERATOR, ");
                sb.Append(" CTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                sb.Append(" BTSFL.NO_OF_BOXES AS BOXES, ");
                sb.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN ");
                sb.Append("       (SELECT BCD.COMMODITY_MST_FK FROM BOOKING_COMMODITY_DTL BCD WHERE BCD.BOOKING_CARGO_DTL_FK IN ");
                sb.Append("       (SELECT BTCD.BOOKING_TRN_CARGO_PK FROM BOOKING_TRN_CARGO_DTL BTCD WHERE BTCD.BOOKING_TRN_FK='|| ");
                sb.Append("       NVL(BTSFL.BOOKING_TRN_PK, 0) || '))') CARGO,");

                sb.Append(" '' AS COMMODITY, ");
                sb.Append(" BTSFL.BUYING_RATE AS RATE, ");
                sb.Append(" FETCH_FCL_LCL_TOTALBKGFRT(BTSFL.BOOKING_TRN_PK,2," + HttpContext.Current.Session["CURRENCY_MST_PK"] + "," + BizType + ")AS BKGRATE, ");
                sb.Append(" NULL AS NET, ");
                sb.Append(" FETCH_FCL_LCL_TOTALBKGFRT(BTSFL.BOOKING_TRN_PK,1," + HttpContext.Current.Session["CURRENCY_MST_PK"] + "," + BizType + ")AS TOTALRATE, ");
                sb.Append(" '1' AS SEL, ");
                sb.Append(" BTSFL.CONTAINER_TYPE_MST_FK AS CONTAINERPK, ");
                sb.Append(" 0 AS CARGOPK, ");

                sb.Append("       ROWTOCOL('SELECT BCD.COMMODITY_MST_FK FROM BOOKING_COMMODITY_DTL BCD WHERE BCD.BOOKING_CARGO_DTL_FK IN ");
                sb.Append("       (SELECT BTCD.BOOKING_TRN_CARGO_PK FROM BOOKING_TRN_CARGO_DTL BTCD WHERE BTCD.BOOKING_TRN_FK='|| ");
                sb.Append("       NVL(BTSFL.BOOKING_TRN_PK, 0) || ')') COMMODITYPK,");

                sb.Append(" BST.CARRIER_MST_FK AS OPERATORPK, ");
                sb.Append(" BTSFL.BOOKING_TRN_PK AS TRANSACTIONPK, ");
                sb.Append(" NULL AS BASISPK , CTMT.Preferences");
                sb.Append(" FROM ");
                sb.Append(" BOOKING_MST_TBL BST, ");
                sb.Append(" BOOKING_TRN BTSFL, ");
                sb.Append(" CONTAINER_TYPE_MST_TBL CTMT, ");
                sb.Append(" COMMODITY_GROUP_MST_TBL CGMT, ");
                sb.Append(" COMMODITY_MST_TBL CMT, ");
                sb.Append(" OPERATOR_MST_TBL OMT, ");
                sb.Append(" BOOKING_TRN_CARGO_DTL BTC ");
                sb.Append(" WHERE(1 = 1) ");
                sb.Append(" AND BTSFL.CONTAINER_TYPE_MST_FK=CTMT.CONTAINER_TYPE_MST_PK (+)");
                sb.Append(" AND BTSFL.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) ");
                sb.Append(" AND BTSFL.BOOKING_MST_FK=BST.BOOKING_MST_PK ");
                sb.Append(" AND BST.CARRIER_MST_FK=OMT.OPERATOR_MST_PK (+)");
                sb.Append(" AND BTSFL.BOOKING_TRN_PK=BTC.BOOKING_TRN_FK(+)");
                sb.Append(" AND BST.BOOKING_MST_PK= " + lngSBEPK);
                sb.Append(" ORDER BY  CTMT.Preferences)");
                //bbc
            }
            else
            {
                sb.Append("SELECT NULL AS TRNTYPEPK,");
                sb.Append("       BTSFL.TRANS_REFERED_FROM AS TRNTYPESTATUS,");
                sb.Append("       DECODE(BTSFL.TRANS_REFERED_FROM,1,'Quote',2,'Sp Rate',3,'Cust Cont',4,'SL Tariff',5,'SRR',6,'Gen Tariff',7,'Manual',8,'Agent Tariff') AS CONTRACTTYPE,");
                sb.Append("       BTSFL.TRANS_REF_NO AS REFNO,");
                sb.Append("       OMT.OPERATOR_ID AS OPERATOR,");
                sb.Append("       CMT.COMMODITY_NAME AS COMMODITY,");
                sb.Append("       PCK.PACK_TYPE_MST_PK PACK_PK,");
                sb.Append("       PCK.PACK_TYPE_ID PACK_TYPE,");
                sb.Append("       UOM.DIMENTION_ID AS BASIS,");
                sb.Append("       BTSFL.QUANTITY QTY,");
                sb.Append("       BTSFL.WEIGHT_MT CARGO_WT,");
                sb.Append("       BTSFL.VOLUME_CBM CARGO_VOL,");
                sb.Append("       '' CARGO_CALC,");
                sb.Append("       BTSFL.BUYING_RATE AS RATE,");
                sb.Append("       ROUND(FETCH_TOTALBKGFRT(BTSFL.BOOKING_TRN_PK, 2), 2) AS BKGRATE,");
                sb.Append("       NULL AS NET,");
                sb.Append("       ROUND(FETCH_TOTALBKGFRT(BTSFL.BOOKING_TRN_PK, 1), 2) AS TOTALRATE,");
                sb.Append("       '1' AS SEL,");
                sb.Append("       ROWNUM AS CONTAINERPK,");
                sb.Append("       BTSFL.COMMODITY_MST_FK AS COMMODITYPK,");
                sb.Append("       BST.CARRIER_MST_FK AS OPERATORPK,");
                sb.Append("       BTSFL.BOOKING_TRN_PK AS TRANSACTIONPK,");
                sb.Append("       UOM.DIMENTION_UNIT_MST_PK AS BASISPK");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       BOOKING_TRN             BTSFL,");
                sb.Append("       DIMENTION_UNIT_MST_TBL  UOM,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       COMMODITY_MST_TBL       CMT,");
                sb.Append("       OPERATOR_MST_TBL        OMT,");
                sb.Append("       PACK_TYPE_MST_TBL       PCK");
                sb.Append(" WHERE (1 = 1)");
                sb.Append("   AND BTSFL.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                sb.Append("   AND BTSFL.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                sb.Append("   AND BST.CARRIER_MST_FK = OMT.OPERATOR_MST_PK(+)");
                sb.Append("   AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK(+)");
                sb.Append("   AND PCK.PACK_TYPE_MST_PK(+) = BTSFL.PACK_TYPE_FK");
                sb.Append("   AND BST.BOOKING_MST_PK= " + lngSBEPK);
            }
            try
            {
                dtTrans = objwf.GetDataTable(sb.ToString());
                return dtTrans;
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

        public object FetchSBEntryFreight(DataTable dtFreight, long lngSBEPK, Int16 intIsLcl, Int16 Ebkg = 0)
        {
            System.Text.StringBuilder strSqlGridChild = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            int BkStatus = 0;
            string strStatus = "SELECT B.STATUS FROM BOOKING_MST_TBL B WHERE B.BOOKING_MST_PK= " + lngSBEPK;
            BkStatus = Convert.ToInt32(objwf.ExecuteScaler(strStatus));
            //SELECT DATA FROM FREIGHT ELEMENTS TABLE FOR EXISTING BOOKING
            if (intIsLcl == 1)
            {
                strSqlGridChild.Append("SELECT TRNTYPEFK,");
                strSqlGridChild.Append("       REFNO,");
                strSqlGridChild.Append("       BASIS,");
                strSqlGridChild.Append("       COMMODITYFK,");
                strSqlGridChild.Append("       POLPK AS PORT_MST_PK,");
                strSqlGridChild.Append("       POL,");
                strSqlGridChild.Append("       PODPK AS PORT_MST_PK1,");
                strSqlGridChild.Append("       POD,");
                strSqlGridChild.Append("       FREIGHT_ELEMENT_MST_FK,");
                strSqlGridChild.Append("       FREIGHT_ELEMENT_ID,");
                strSqlGridChild.Append("       CHARGE_BASIS,");
                strSqlGridChild.Append("       CHECK_FOR_ALL_IN_RT,");
                strSqlGridChild.Append("       CURRENCY_MST_FK,");
                strSqlGridChild.Append("       CURRENCY_ID,");
                strSqlGridChild.Append("       MIN_RATE,");
                strSqlGridChild.Append("       RATE,");
                strSqlGridChild.Append("       abs(BKGRATE) BKGRATE,");
                strSqlGridChild.Append("       TOTAL,");
                strSqlGridChild.Append("       BASIS_PK,");
                strSqlGridChild.Append("       PYMT_TYPE,");
                strSqlGridChild.Append("       Credit,CHECK_ADVATOS,BOOKING_TRN_FRT_PK ");
                strSqlGridChild.Append("  FROM (SELECT NULL AS TRNTYPEFK,");
                strSqlGridChild.Append("               BTSFL.TRANS_REF_NO AS REFNO,");
                strSqlGridChild.Append("               UOM.DIMENTION_ID AS BASIS,");
                strSqlGridChild.Append("               BTSFL.COMMODITY_MST_FK AS COMMODITYFK,");
                strSqlGridChild.Append("               BST.PORT_MST_POL_FK AS POLPK,");
                strSqlGridChild.Append("               PL.PORT_ID AS POL,");
                strSqlGridChild.Append("               BST.PORT_MST_POD_FK AS PODPK,");
                strSqlGridChild.Append("               PD.PORT_ID AS POD,");
                strSqlGridChild.Append("               BTSFD.FREIGHT_ELEMENT_MST_FK,");
                strSqlGridChild.Append("               FEMT.FREIGHT_ELEMENT_ID,");
                strSqlGridChild.Append(" DECODE(BTSFD.CHARGE_BASIS,");
                strSqlGridChild.Append("             '0',");
                strSqlGridChild.Append("             '',");
                strSqlGridChild.Append("             '1',");
                strSqlGridChild.Append("             '%',");
                strSqlGridChild.Append("             '2',");
                strSqlGridChild.Append("             'Flat Rate',");
                strSqlGridChild.Append("             '3',");
                strSqlGridChild.Append("             'Kgs',");
                strSqlGridChild.Append("             '4',");
                strSqlGridChild.Append("             'Unit') CHARGE_BASIS,");

                strSqlGridChild.Append("               DECODE(BTSFD.CHECK_FOR_ALL_IN_RT, 1, 'TRUE', 'FALSE') AS CHECK_FOR_ALL_IN_RT,");
                strSqlGridChild.Append("               BTSFD.CURRENCY_MST_FK,");
                strSqlGridChild.Append("               CTMT.CURRENCY_ID,");
                strSqlGridChild.Append("               MIN_BASIS_RATE AS MIN_RATE,");
                strSqlGridChild.Append("               NULL AS RATE,");
                strSqlGridChild.Append("               BTSFD.TARIFF_RATE AS BKGRATE,");
                //strSqlGridChild.Append("               BTSFD.TARIFF_RATE *(get_ex_rate(BTSFD.CURRENCY_MST_FK,BST.BASE_CURRENCY_FK,BST.BOOKING_DATE)) TOTAL,")
                strSqlGridChild.Append("               BTSFD.TARIFF_RATE as TOTAL,");
                strSqlGridChild.Append("               BTSFL.BASIS AS BASIS_PK,");
                strSqlGridChild.Append("               DECODE(BTSFD.PYMT_TYPE, 1, 'PrePaid', 2, 'Collect', 3, 'Foreign') AS PYMT_TYPE,");
                strSqlGridChild.Append("               BTSFD.BOOKING_TRN_FRT_PK,");
                strSqlGridChild.Append("               FEMT.PREFERENCE,FEMT.Credit,BTSFD.CHECK_ADVATOS ");
                strSqlGridChild.Append("          FROM BOOKING_MST_TBL          BST,");
                strSqlGridChild.Append("               BOOKING_TRN  BTSFL,");
                strSqlGridChild.Append("               BOOKING_TRN_FRT_DTLS BTSFD,");
                strSqlGridChild.Append("               CURRENCY_TYPE_MST_TBL    CTMT,");
                strSqlGridChild.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
                strSqlGridChild.Append("               COMMODITY_MST_TBL        CMT,");
                strSqlGridChild.Append("               PORT_MST_TBL             PL,");
                strSqlGridChild.Append("               PORT_MST_TBL             PD,");
                strSqlGridChild.Append("               DIMENTION_UNIT_MST_TBL   UOM");
                strSqlGridChild.Append("         WHERE (1 = 1)");
                strSqlGridChild.Append("           AND BTSFL.BOOKING_TRN_PK = BTSFD.BOOKING_TRN_FK");
                strSqlGridChild.Append("           AND BTSFD.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFD.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFL.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                strSqlGridChild.Append("           AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK(+)");
                strSqlGridChild.Append("           AND BST.PORT_MST_POL_FK = PL.PORT_MST_PK(+)");
                strSqlGridChild.Append("           AND BST.PORT_MST_POD_FK = PD.PORT_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (Ebkg == 1)
                    {
                        strSqlGridChild.Append(" AND BTSFD.CHECK_FOR_ALL_IN_RT=1");
                    }
                }
                strSqlGridChild.Append("           AND BST.BOOKING_MST_PK = " + lngSBEPK);
                if (BkStatus == 1)
                {
                    strSqlGridChild.Append("        UNION");
                    strSqlGridChild.Append("        SELECT NULL AS TRNTYPEFK,");
                    strSqlGridChild.Append("               BTSFL.TRANS_REF_NO AS REFNO,");
                    strSqlGridChild.Append("               UOM.DIMENTION_ID AS BASIS,");
                    strSqlGridChild.Append("               BTSFL.COMMODITY_MST_FK AS COMMODITYFK,");
                    strSqlGridChild.Append("               BST.PORT_MST_POL_FK AS PORT_MST_PK,");
                    strSqlGridChild.Append("               PL.PORT_ID AS POL,");
                    strSqlGridChild.Append("               BST.PORT_MST_POD_FK AS PORT_MST_PK1,");
                    strSqlGridChild.Append("               PD.PORT_ID AS POD,");
                    strSqlGridChild.Append("               FEMT.FREIGHT_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_FK,");
                    strSqlGridChild.Append("               FEMT.FREIGHT_ELEMENT_ID,");
                    strSqlGridChild.Append(" DECODE(FEMT.CHARGE_BASIS,");
                    strSqlGridChild.Append("             '0',");
                    strSqlGridChild.Append("             '',");
                    strSqlGridChild.Append("             '1',");
                    strSqlGridChild.Append("             '%',");
                    strSqlGridChild.Append("             '2',");
                    strSqlGridChild.Append("             'Flat Rate',");
                    strSqlGridChild.Append("             '3',");
                    strSqlGridChild.Append("             'Kgs',");
                    strSqlGridChild.Append("             '4',");
                    strSqlGridChild.Append("             'Unit') CHARGE_BASIS,");

                    strSqlGridChild.Append("               'FALSE' AS CHECK_FOR_ALL_IN_RT,");
                    strSqlGridChild.Append("               CTMT.CURRENCY_MST_PK CURRENCY_MST_FK,");
                    strSqlGridChild.Append("               CTMT.CURRENCY_ID,");
                    strSqlGridChild.Append("               0.00 AS MIN_RATE,");
                    strSqlGridChild.Append("               0.00 AS RATE,");
                    strSqlGridChild.Append("               0.00 AS BKGRATE,");
                    strSqlGridChild.Append("               0.00 TOTAL,");
                    strSqlGridChild.Append("               BTSFL.BASIS AS BASIS_PK,");
                    strSqlGridChild.Append("               'PrePaid' AS PYMT_TYPE,");
                    strSqlGridChild.Append("               NULL BOOKING_TRN_FRT_PK,");
                    strSqlGridChild.Append("               FEMT.PREFERENCE,FEMT.Credit,0 CHECK_ADVATOS ");
                    strSqlGridChild.Append("          FROM BOOKING_MST_TBL         BST,");
                    strSqlGridChild.Append("               BOOKING_TRN BTSFL,");
                    strSqlGridChild.Append("               CURRENCY_TYPE_MST_TBL   CTMT,");
                    strSqlGridChild.Append("               FREIGHT_ELEMENT_MST_TBL FEMT,");
                    strSqlGridChild.Append("               COMMODITY_MST_TBL       CMT,");
                    strSqlGridChild.Append("               PORT_MST_TBL            PL,");
                    strSqlGridChild.Append("               PORT_MST_TBL            PD,");
                    strSqlGridChild.Append("               DIMENTION_UNIT_MST_TBL  UOM");
                    strSqlGridChild.Append("         WHERE (1 = 1)");
                    strSqlGridChild.Append("           AND FEMT.FREIGHT_ELEMENT_MST_PK NOT IN");
                    strSqlGridChild.Append("               (SELECT BF.FREIGHT_ELEMENT_MST_FK");
                    strSqlGridChild.Append("                  FROM BOOKING_TRN  BT,");
                    strSqlGridChild.Append("                       BOOKING_TRN_FRT_DTLS BF");
                    strSqlGridChild.Append("                 WHERE BF.BOOKING_TRN_FK = BT.BOOKING_TRN_PK");
                    strSqlGridChild.Append("                   AND BT.BOOKING_MST_FK = " + lngSBEPK + ")");
                    strSqlGridChild.Append("           AND FEMT.ACTIVE_FLAG = 1");
                    strSqlGridChild.Append("           AND FEMT.BUSINESS_TYPE = 2");
                    strSqlGridChild.Append("           AND NVL(FEMT.CHARGE_TYPE, 0) <> 3");
                    strSqlGridChild.Append("           AND CTMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                    strSqlGridChild.Append("           AND BTSFL.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                    strSqlGridChild.Append("           AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.PORT_MST_POL_FK = PL.PORT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.PORT_MST_POD_FK = PD.PORT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.BOOKING_MST_PK = " + lngSBEPK);
                }
                strSqlGridChild.Append(" ) Q ORDER BY PREFERENCE");
            }
            else if (intIsLcl == 0)
            {
                strSqlGridChild.Append("SELECT Q.TRNTYPEFK,");
                strSqlGridChild.Append("       REFNO,");
                strSqlGridChild.Append("       TYPE,");
                strSqlGridChild.Append("       COMMODITYFK,");
                strSqlGridChild.Append("       POLPK AS PORT_MST_PK,");
                strSqlGridChild.Append("       POL,");
                strSqlGridChild.Append("       PODPK AS PORT_MST_PK1,");
                strSqlGridChild.Append("       POD,");
                strSqlGridChild.Append("       FREIGHT_ELEMENT_MST_FK,");
                strSqlGridChild.Append("       FREIGHT_ELEMENT_ID,");
                strSqlGridChild.Append("       CHARGE_BASIS,");
                strSqlGridChild.Append("       CHECK_FOR_ALL_IN_RT,");
                strSqlGridChild.Append("       CURRENCY_MST_FK,");
                strSqlGridChild.Append("       CURRENCY_ID,");
                strSqlGridChild.Append("       RATE,");
                strSqlGridChild.Append("       BKGRATE,");
                strSqlGridChild.Append("       TOTAL,");
                strSqlGridChild.Append("       BASIS,");
                strSqlGridChild.Append("       PYMT_TYPE,");
                strSqlGridChild.Append("       Credit,CHECK_ADVATOS,BOOKING_TRN_FRT_PK ");
                strSqlGridChild.Append("  FROM (SELECT NULL AS TRNTYPEFK,");
                strSqlGridChild.Append("               BTSFL.TRANS_REF_NO AS REFNO,");
                strSqlGridChild.Append("               CTMT.CONTAINER_TYPE_MST_ID AS TYPE,");
                strSqlGridChild.Append("               BTSFL.COMMODITY_MST_FK AS COMMODITYFK,");
                strSqlGridChild.Append("               BST.PORT_MST_POL_FK AS POLPK,");
                strSqlGridChild.Append("               PL.PORT_ID AS POL,");
                strSqlGridChild.Append("               BST.PORT_MST_POD_FK AS PODPK,");
                strSqlGridChild.Append("               PD.PORT_ID AS POD,");
                strSqlGridChild.Append("               BTSFD.FREIGHT_ELEMENT_MST_FK,");
                strSqlGridChild.Append("               FEMT.FREIGHT_ELEMENT_ID,");

                strSqlGridChild.Append(" DECODE(BTSFD.CHARGE_BASIS,");
                strSqlGridChild.Append("             '0',");
                strSqlGridChild.Append("             '',");
                strSqlGridChild.Append("             '1',");
                strSqlGridChild.Append("             '%',");
                strSqlGridChild.Append("             '2',");
                strSqlGridChild.Append("             'Flat Rate',");
                strSqlGridChild.Append("             '3',");
                strSqlGridChild.Append("             'Kgs',");
                strSqlGridChild.Append("             '4',");
                strSqlGridChild.Append("             'Unit') CHARGE_BASIS,");

                strSqlGridChild.Append("               DECODE(BTSFD.CHECK_FOR_ALL_IN_RT, 1, 'TRUE', 'FALSE') AS CHECK_FOR_ALL_IN_RT,");
                strSqlGridChild.Append("               BTSFD.CURRENCY_MST_FK,");
                strSqlGridChild.Append("               CTMT.CURRENCY_ID,");
                strSqlGridChild.Append("               NULL AS RATE,");
                strSqlGridChild.Append("               BTSFD.TARIFF_RATE AS BKGRATE,");
                strSqlGridChild.Append("               BTSFD.MIN_BASIS_RATE TOTAL,");
                strSqlGridChild.Append("               BTSFL.BASIS AS BASIS,");
                strSqlGridChild.Append("               DECODE(BTSFD.PYMT_TYPE, 1, 'PrePaid', 2, 'Collect', 3, 'Foreign') AS PYMT_TYPE,");
                strSqlGridChild.Append("               BTSFD.BOOKING_TRN_FRT_PK,");
                strSqlGridChild.Append("               FEMT.PREFERENCE,FEMT.Credit,BTSFD.CHECK_ADVATOS ");
                strSqlGridChild.Append("          FROM BOOKING_MST_TBL          BST,");
                strSqlGridChild.Append("               BOOKING_TRN  BTSFL,");
                strSqlGridChild.Append("               BOOKING_TRN_FRT_DTLS BTSFD,");
                strSqlGridChild.Append("               CURRENCY_TYPE_MST_TBL    CTMT,");
                strSqlGridChild.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
                strSqlGridChild.Append("               CONTAINER_TYPE_MST_TBL   CTMT,");
                strSqlGridChild.Append("               COMMODITY_MST_TBL        CMT,");
                strSqlGridChild.Append("               PORT_MST_TBL             PL,");
                strSqlGridChild.Append("               PORT_MST_TBL             PD");
                strSqlGridChild.Append("         WHERE (1 = 1)");
                strSqlGridChild.Append("           AND BTSFL.BOOKING_TRN_PK = BTSFD.BOOKING_TRN_FK");
                strSqlGridChild.Append("           AND BTSFD.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFD.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFL.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                strSqlGridChild.Append("           AND BTSFL.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
                strSqlGridChild.Append("           AND BST.PORT_MST_POL_FK = PL.PORT_MST_PK(+)");
                strSqlGridChild.Append("           AND BST.PORT_MST_POD_FK = PD.PORT_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");

                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (Ebkg == 1)
                    {
                        strSqlGridChild.Append(" AND BTSFD.CHECK_FOR_ALL_IN_RT=1");
                    }
                }
                strSqlGridChild.Append("           AND BST.BOOKING_MST_PK = " + lngSBEPK);
                if (BkStatus == 1)
                {
                    strSqlGridChild.Append("        UNION");
                    strSqlGridChild.Append("        SELECT NULL AS TRNTYPEFK,");
                    strSqlGridChild.Append("               BTSFL.TRANS_REF_NO AS REFNO,");
                    strSqlGridChild.Append("               CTMT.CONTAINER_TYPE_MST_ID AS TYPE,");
                    strSqlGridChild.Append("               BTSFL.COMMODITY_MST_FK AS COMMODITYFK,");
                    strSqlGridChild.Append("               BST.PORT_MST_POL_FK AS PORT_MST_PK,");
                    strSqlGridChild.Append("               PL.PORT_ID AS POL,");
                    strSqlGridChild.Append("               BST.PORT_MST_POD_FK AS PORT_MST_PK1,");
                    strSqlGridChild.Append("               PD.PORT_ID AS POD,");
                    strSqlGridChild.Append("               FEMT.FREIGHT_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_FK,");
                    strSqlGridChild.Append("               FEMT.FREIGHT_ELEMENT_ID,");

                    strSqlGridChild.Append(" DECODE(FEMT.CHARGE_BASIS,");
                    strSqlGridChild.Append("             '0',");
                    strSqlGridChild.Append("             '',");
                    strSqlGridChild.Append("             '1',");
                    strSqlGridChild.Append("             '%',");
                    strSqlGridChild.Append("             '2',");
                    strSqlGridChild.Append("             'Flat Rate',");
                    strSqlGridChild.Append("             '3',");
                    strSqlGridChild.Append("             'Kgs',");
                    strSqlGridChild.Append("             '4',");
                    strSqlGridChild.Append("             'Unit') CHARGE_BASIS,");

                    strSqlGridChild.Append("               'FALSE' AS CHECK_FOR_ALL_IN_RT,");
                    strSqlGridChild.Append("               CTMT.CURRENCY_MST_PK CURRENCY_MST_FK,");
                    strSqlGridChild.Append("               CTMT.CURRENCY_ID,");
                    strSqlGridChild.Append("               NULL AS RATE,");
                    strSqlGridChild.Append("               0.00 AS BKGRATE,");
                    strSqlGridChild.Append("               0.00 TOTAL,");
                    strSqlGridChild.Append("               BTSFL.BASIS AS BASIS,");
                    strSqlGridChild.Append("               'PrePaid' AS PYMT_TYPE,");
                    strSqlGridChild.Append("               NULL BOOKING_TRN_FRT_PK,");
                    strSqlGridChild.Append("               FEMT.PREFERENCE,FEMT.Credit,0 CHECK_ADVATOS ");
                    strSqlGridChild.Append("          FROM BOOKING_MST_TBL         BST,");
                    strSqlGridChild.Append("               BOOKING_TRN BTSFL,");
                    strSqlGridChild.Append("               CURRENCY_TYPE_MST_TBL   CTMT,");
                    strSqlGridChild.Append("               FREIGHT_ELEMENT_MST_TBL FEMT,");
                    strSqlGridChild.Append("               CONTAINER_TYPE_MST_TBL  CTMT,");
                    strSqlGridChild.Append("               COMMODITY_MST_TBL       CMT,");
                    strSqlGridChild.Append("               PORT_MST_TBL            PL,");
                    strSqlGridChild.Append("               PORT_MST_TBL            PD");
                    strSqlGridChild.Append("         WHERE (1 = 1)");
                    strSqlGridChild.Append("           AND FEMT.FREIGHT_ELEMENT_MST_PK NOT IN");
                    strSqlGridChild.Append("               (SELECT BF.FREIGHT_ELEMENT_MST_FK");
                    strSqlGridChild.Append("                  FROM BOOKING_TRN  BT,");
                    strSqlGridChild.Append("                       BOOKING_TRN_FRT_DTLS BF");
                    strSqlGridChild.Append("                 WHERE BF.BOOKING_TRN_FK = BT.BOOKING_TRN_PK");
                    strSqlGridChild.Append("                   AND BT.BOOKING_MST_FK = " + lngSBEPK + ")");
                    strSqlGridChild.Append("           AND FEMT.ACTIVE_FLAG = 1");
                    strSqlGridChild.Append("           AND FEMT.BUSINESS_TYPE = 2");
                    strSqlGridChild.Append("           AND NVL(FEMT.CHARGE_TYPE, 0) <> 3");
                    strSqlGridChild.Append("           AND BTSFL.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                    strSqlGridChild.Append("           AND BTSFL.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.PORT_MST_POL_FK = PL.PORT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.PORT_MST_POD_FK = PD.PORT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                    strSqlGridChild.Append("           AND CTMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                    strSqlGridChild.Append("           AND BST.BOOKING_MST_PK = " + lngSBEPK + "");
                }
                strSqlGridChild.Append(" ) Q, container_type_mst_tbl ctmt WHERE q.type = ctmt.container_type_mst_id");
                strSqlGridChild.Append("  ORDER BY ctmt.preferences,Q.CHECK_FOR_ALL_IN_RT DESC, Q.PREFERENCE");
                //BBC
            }
            else
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT BTSFD.BOOKING_TRN_FRT_PK AS TRNTYPEFK,");
                sb.Append("       BTSFL.TRANS_REF_NO AS REFNO,");
                sb.Append("       UOM.DIMENTION_ID AS BASIS,");
                sb.Append("       (CMT.COMMODITY_NAME || '$$;' || FEMT.CREDIT) AS COMMODITY,");
                sb.Append("       BST.PORT_MST_POL_FK AS PORT_MST_PK,");
                sb.Append("       PL.PORT_ID          AS POL,");
                sb.Append("       BST.PORT_MST_POD_FK AS PORT_MST_PK1,");
                sb.Append("       PD.PORT_ID AS POD,");
                sb.Append("       BTSFD.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("       FEMT.FREIGHT_ELEMENT_ID,");
                sb.Append("       DECODE(FEMT.CHARGE_BASIS,");
                sb.Append("              '0',");
                sb.Append("              '',");
                sb.Append("              '1',");
                sb.Append("              '%',");
                sb.Append("              '2',");
                sb.Append("              'Flat Rate',");
                sb.Append("              '3',");
                sb.Append("              'Kgs',");
                sb.Append("              '4',");
                sb.Append("              'Unit') CHARGE_BASIS,");
                sb.Append("       DECODE(BTSFD.CHECK_FOR_ALL_IN_RT, 1, 'TRUE', 'FALSE') AS SEL,");
                sb.Append("       BTSFD.CURRENCY_MST_FK CURRENCY_MST_PK,");
                sb.Append("       CTMT.CURRENCY_ID,");
                sb.Append("       ABS(CASE");
                sb.Append("             WHEN BTSFD.SURCHARGE IS NOT NULL THEN");
                sb.Append("              TO_NUMBER(TRIM(SUBSTR(BTSFD.SURCHARGE,");
                sb.Append("                                    0,");
                sb.Append("                                    (INSTR(BTSFD.SURCHARGE, '%', 1, 1) - 1))))");
                sb.Append("             ELSE");
                sb.Append("              BTSFD.MIN_BASIS_RATE");
                sb.Append("           END) SURCHARGE_VALUE,");
                sb.Append("       BTSFD.MIN_BASIS_RATE AS MIN_RATE,");
                sb.Append("       BTSFD.EXCHANGE_RATE RATE,");
                sb.Append("       BTSFD.TARIFF_RATE AS BKGRATE,");
                sb.Append("       UOM.DIMENTION_UNIT_MST_PK AS BASISPK,");
                sb.Append("       DECODE(BTSFD.PYMT_TYPE, 1, 'PrePaid', 2, 'Collect', 3, 'Foreign') AS PYMT_TYPE,");
                sb.Append("       CMT.COMMODITY_MST_PK,");
                sb.Append("       BTSFD.BOOKING_TRN_FRT_PK FreightPK,");
                sb.Append("        BTSFD.EXCHANGE_RATE EXCHANGERATE,");
                sb.Append("       '' SURCHARGE_TYPE,");
                sb.Append("       FEMT.Credit");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       BOOKING_TRN             BTSFL,");
                sb.Append("       BOOKING_TRN_FRT_DTLS    BTSFD,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
                sb.Append("       COMMODITY_MST_TBL       CMT,");
                sb.Append("       PORT_MST_TBL            PL,");
                sb.Append("       PORT_MST_TBL            PD,");
                sb.Append("       DIMENTION_UNIT_MST_TBL  UOM");
                sb.Append(" WHERE (1 = 1)");
                sb.Append("   AND BTSFL.BOOKING_TRN_PK = BTSFD.BOOKING_TRN_FK");
                sb.Append("   AND BTSFD.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
                sb.Append("   AND BTSFD.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("   AND BTSFL.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                sb.Append("   AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK(+)");
                sb.Append("   AND BST.PORT_MST_POL_FK = PL.PORT_MST_PK(+)");
                sb.Append("   AND BST.PORT_MST_POD_FK = PD.PORT_MST_PK(+)");
                sb.Append("   AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");

                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (Ebkg == 1)
                    {
                        sb.Append("   AND BTSFD.CHECK_FOR_ALL_IN_RT = 1");
                    }
                }

                sb.Append(" AND BST.BOOKING_MST_PK=" + lngSBEPK);
                sb.Append(" ORDER BY PREFERENCE ASC");
                strSqlGridChild.Append(sb.ToString());
            }
            try
            {
                dtFreight = objwf.GetDataTable(strSqlGridChild.ToString());
                return dtFreight;
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

        public object FetchCDimension(DataTable dtMain, long lngSBEPK)
        {
            System.Text.StringBuilder strSqlCDimension = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //Fetch the Cargo Dimesion for the existing booking
            strSqlCDimension.Append(" SELECT ");
            strSqlCDimension.Append(" ROWNUM AS SNO, ");
            strSqlCDimension.Append(" BACC.CARGO_NOP AS NOP, ");
            strSqlCDimension.Append(" BACC.CARGO_LENGTH AS LENGTH, ");
            strSqlCDimension.Append(" BACC.CARGO_WIDTH AS WIDTH, ");
            strSqlCDimension.Append(" BACC.CARGO_HEIGHT AS HEIGHT, ");
            strSqlCDimension.Append(" BACC.CARGO_CUBE AS CUBE, ");
            strSqlCDimension.Append(" BACC.CARGO_VOLUME_WT AS VOLWEIGHT, ");
            strSqlCDimension.Append(" BACC.CARGO_ACTUAL_WT AS ACTWEIGHT, ");
            strSqlCDimension.Append(" BACC.CARGO_DENSITY AS DENSITY, ");
            strSqlCDimension.Append(" BACC.BOOKING_CARGO_CALC_PK AS PK, ");
            strSqlCDimension.Append(" BACC.BOOKING_MST_FK,CARGO_MEASUREMENT,CARGO_WEIGHT_IN,CARGO_DIVISION_FACT ");
            strSqlCDimension.Append(" FROM BOOKING_CARGO_CALC BACC ");
            strSqlCDimension.Append(" WHERE BACC.BOOKING_MST_FK= " + lngSBEPK);
            try
            {
                dtMain = objwf.GetDataTable(strSqlCDimension.ToString());
                return dtMain;
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

        #endregion "Fetch Sea Existing Booking Entry Details"

        #region "Fetch Temp Data"

        //Added By : Anand
        //Reason : To Update Temp Customer Table
        //Date : 24/03/08

        public void UpdateTempCus(int CustomerPK)
        {
            WorkFlow objWF = new WorkFlow();
            bool Result = false;
            try
            {
                Result = objWF.ExecuteCommands("update temp_customer_tbl set transaction_type=2 where customer_mst_pk=' " + CustomerPK + "'");
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

        //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
        public object FetchBkgPkAir(string BookingId = "0")
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            Int32 BookingPk = default(Int32);
            try
            {
                if (string.IsNullOrEmpty(BookingId))
                {
                    BookingId = "0";
                }
                sqlStr = "select BOOKING_MST_PK from booking_MST_tbl where booking_ref_no='" + BookingId + "'";
                BookingPk = Convert.ToInt32(objWF.ExecuteScaler(sqlStr));
                return BookingPk;
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

        public object FetchBkgPk(string BookingId = "0")
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            Int32 BookingPk = default(Int32);
            try
            {
                if (string.IsNullOrEmpty(BookingId))
                {
                    BookingId = "0";
                }
                sqlStr = "SELECT BOOKING_MST_PK FROM BOOKING_MST_TBL WHERE BOOKING_REF_NO ='" + BookingId + "'";
                BookingPk = Convert.ToInt32(objWF.ExecuteScaler(sqlStr));
                return BookingPk;
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

        //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
        public object FetchCommText(string commpk = "0")
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            string CommGrpCode = null;
            try
            {
                if (string.IsNullOrEmpty(commpk))
                {
                    commpk = "0";
                }
                sqlStr = "select commodity_group_code from commodity_group_mst_tbl where commodity_group_pk='" + commpk + "'";
                CommGrpCode = objWF.ExecuteScaler(sqlStr);
                return CommGrpCode;
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

        public Int16 FetchEBKN(Int16 BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            Int16 IS_EBOOKING = default(Int16);
            try
            {
                sqlStr = "select IS_EBOOKING from BOOKING_MST_TBL where BOOKING_MST_PK='" + BookingPK + "'";
                IS_EBOOKING = Convert.ToInt16(objWF.ExecuteScaler(sqlStr));
                return IS_EBOOKING;
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

        public string FetchEBKRef(Int16 BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            string BOOKINGRef = null;
            try
            {
                sqlStr = "select BOOKING_REF_NO from BOOKING_MST_TBL where BOOKING_MST_PK='" + BookingPK + "' and BOOKING_REF_NO like 'BKG%'";
                BOOKINGRef = objWF.ExecuteScaler(sqlStr);
                if (string.IsNullOrEmpty(BOOKINGRef))
                {
                    return "";
                }
                else
                {
                    return BOOKINGRef;
                }
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

        public string FetchBkgDate(string BOOKING_REF_NO)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            string BOOKINGDate = null;
            try
            {
                sqlStr = "select BOOKING_DATE from BOOKING_MST_TBL where BOOKING_REF_NO='" + BOOKING_REF_NO + "' ";
                BOOKINGDate = objWF.ExecuteScaler(sqlStr);
                if (string.IsNullOrEmpty(BOOKINGDate))
                {
                    return "";
                }
                else
                {
                    return BOOKINGDate;
                }
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

        public string FetchEBKRef1(Int16 BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            string BOOKINGRef = null;
            try
            {
                sqlStr = "select BOOKING_REF_NO from BOOKING_MST_TBL where BOOKING_MST_PK='" + BookingPK + "' ";
                BOOKINGRef = objWF.ExecuteScaler(sqlStr);
                if (string.IsNullOrEmpty(BOOKINGRef))
                {
                    return "";
                }
                else
                {
                    return BOOKINGRef;
                }
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

        //End Snigdharani

        #endregion "Fetch Temp Data"

        #region "Fetch Sea Quotation Entry Details"

        public void FetchQEntry(DataSet dsQEntry, long lngSQEPk, string strQuotationPOLPK, string strQuotationPODPK)
        {
            try
            {
                DataTable dtMain = new DataTable();
                FetchSQEntryHeader(dtMain, lngSQEPk, strQuotationPOLPK, strQuotationPODPK);
                //Data set ds contains the Master Table details
                dsQEntry.Tables.Add(dtMain);
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

        public void FetchSQEntryHeader(DataTable dtMain, long lngSQEPK, string strQuotationPOLPK, string strQuotationPODPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING QUOTATION
            strBuilder.Append("SELECT DISTINCT ");
            strBuilder.Append(" (CASE WHEN QST.CUST_TYPE=1 THEN ( ");
            strBuilder.Append("       select tmp.customer_mst_pk from temp_customer_tbl tmp where tmp.customer_mst_pk=QST.CUSTOMER_MST_FK ");
            strBuilder.Append(" ) ");
            strBuilder.Append(" ELSE QST.CUSTOMER_MST_FK END) AS CUSTOMER_MST_FK,");

            strBuilder.Append(" (CASE WHEN QST.CUST_TYPE=1 THEN (");
            strBuilder.Append("       select tmp.customer_id from temp_customer_tbl tmp where tmp.customer_mst_pk=QST.CUSTOMER_MST_FK");
            strBuilder.Append(" ) ");
            strBuilder.Append(" ELSE CMT.CUSTOMER_ID END) AS CUSTOMER_ID, ");

            strBuilder.Append(" (CASE WHEN QST.CUST_TYPE=1 THEN (");
            strBuilder.Append("       SELECT TMP.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMP WHERE TMP.CUSTOMER_MST_PK=QST.CUSTOMER_MST_FK");
            strBuilder.Append(" ) ");
            strBuilder.Append(" ELSE CMT.CUSTOMER_NAME END) AS CUSTOMER_NAME, ");

            strBuilder.Append(" (CASE WHEN QST.CUST_TYPE=1 THEN ( ");
            strBuilder.Append("      select tmp.customer_type_fk from temp_customer_tbl tmp where tmp.customer_mst_pk=QST.CUSTOMER_MST_FK ");
            strBuilder.Append("      ) ");
            strBuilder.Append(" ELSE QST.CUSTOMER_CATEGORY_MST_FK END) AS CUSTOMER_CATEGORY_MST_FK, ");

            strBuilder.Append(" CCMT.CUSTOMER_CATEGORY_ID, ");
            strBuilder.Append(" POL.PORT_MST_PK POLPK, ");
            strBuilder.Append(" POL.PORT_ID POLID, ");
            strBuilder.Append(" POL.PORT_NAME POLNAME, ");
            strBuilder.Append(" POD.PORT_MST_PK PODPK, ");
            strBuilder.Append(" POD.PORT_ID PODID, ");
            strBuilder.Append(" POD.PORT_NAME PODNAME, ");
            strBuilder.Append(" QST.COMMODITY_GROUP_MST_FK COMMODITY_GROUP_FK, ");
            strBuilder.Append(" QST.COL_PLACE_MST_FK, ");
            strBuilder.Append(" CPMT.PLACE_CODE CPLACECODE, ");
            strBuilder.Append(" (CASE WHEN QST.COL_ADDRESS IS NULL THEN ");
            strBuilder.Append(" (CASE WHEN QST.CUSTOMER_MST_FK IS NOT NULL THEN ");
            strBuilder.Append(" (SELECT CTRN.COL_ADDRESS FROM CUSTOMER_MST_TBL CTRN WHERE ");
            strBuilder.Append(" CTRN.CUSTOMER_MST_PK = (SELECT DISTINCT QHDR.CUSTOMER_MST_FK FROM ");
            strBuilder.Append(" QUOTATION_MST_TBL QHDR, QUOTATION_DTL_TBL QTRN WHERE ");
            strBuilder.Append(" QTRN.QUOTATION_MST_FK = QHDR.QUOTATION_MST_PK ");
            strBuilder.Append(" AND QHDR.CUST_TYPE= 0 ");
            strBuilder.Append(" AND QHDR.QUOTATION_MST_PK= " + lngSQEPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + strQuotationPOLPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + strQuotationPODPK + " ))");
            strBuilder.Append(" ELSE ");
            strBuilder.Append(" NULL ");
            strBuilder.Append(" END) ");
            strBuilder.Append(" ELSE ");
            strBuilder.Append(" QST.COL_ADDRESS ");
            strBuilder.Append(" END) AS COL_ADDRESS, ");
            strBuilder.Append(" QST.DEL_PLACE_MST_FK, ");
            strBuilder.Append(" DPMT.PLACE_CODE DPLACECODE, ");
            strBuilder.Append(" (CASE WHEN QST.DEL_ADDRESS IS NULL THEN ");
            strBuilder.Append(" (CASE WHEN QST.CUSTOMER_MST_FK IS NOT NULL THEN ");
            strBuilder.Append(" (SELECT CTRN.DEL_ADDRESS FROM CUSTOMER_MST_TBL CTRN WHERE ");
            strBuilder.Append(" CTRN.CUSTOMER_MST_PK = (SELECT DISTINCT QHDR.CUSTOMER_MST_FK FROM ");
            strBuilder.Append(" QUOTATION_MST_TBL QHDR, QUOTATION_DTL_TBL QTRN WHERE ");
            strBuilder.Append(" QTRN.QUOTATION_MST_FK = QHDR.QUOTATION_MST_PK ");
            strBuilder.Append(" AND QHDR.CUST_TYPE= 0 ");
            strBuilder.Append(" AND QHDR.QUOTATION_MST_PK= " + lngSQEPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + strQuotationPOLPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + strQuotationPODPK + " ))");
            strBuilder.Append(" ELSE ");
            strBuilder.Append(" NULL ");
            strBuilder.Append(" END) ");
            strBuilder.Append(" ELSE ");
            strBuilder.Append(" QST.DEL_ADDRESS ");
            strBuilder.Append(" END) AS DEL_ADDRESS, ");
            strBuilder.Append(" QST.PYMT_TYPE, ");
            strBuilder.Append(" QST.AGENT_MST_FK, ");
            strBuilder.Append(" QST.STATUS, ");
            strBuilder.Append(" CBAMT.AGENT_ID, ");
            strBuilder.Append(" CBAMT.AGENT_NAME, ");
            strBuilder.Append(" DPAMT.AGENT_MST_PK DPAGENTPK, ");
            strBuilder.Append(" DPAMT.AGENT_ID DPAGENT, ");
            strBuilder.Append(" DPAMT.AGENT_NAME DPAGENTNAME, ");

            strBuilder.Append(" QST.TARIFF_AGENT_MST_FK, ");
            strBuilder.Append(" TAR_AGENT.AGENT_ID TARIFF_AGENT_ID, ");
            strBuilder.Append(" TAR_AGENT.AGENT_NAME TARIFF_AGENT_NAME, ");
            strBuilder.Append("  NVL(CU.CUSTOMER_MST_PK,0) THDFRTPAYER_PK, ");
            strBuilder.Append("  CU.CUSTOMER_ID THDFRTPAYER_ID, ");
            strBuilder.Append("  CU.CUSTOMER_NAME THDFRTPAYER_NAME, ");
            strBuilder.Append("  NVL(AGT.AGENT_MST_PK,0) FOREIGN_AGENT_PK, ");
            strBuilder.Append("  AGT.AGENT_ID FOREIGN_AGENT_ID, ");
            strBuilder.Append("  AGT.AGENT_NAME FOREIGN_AGENT_NAME, ");
            strBuilder.Append("  NVL(QST.COLLECT_AGENT_FLAG,0) COLLECT_AGENT_FLAG, ");
            strBuilder.Append(" QST.CARGO_TYPE, ");
            strBuilder.Append(" TO_CHAR(QST.EXPECTED_SHIPMENT_DT,'" + dateFormat + "') AS EXP_SHIPMENT_DATE, ");
            strBuilder.Append(" QTSFL.EXPECTED_VOLUME AS VOLUME, CASE WHEN QST.BIZ_TYPE= 1 THEN CASE WHEN DMT.DIMENTION_ID = 'MT' THEN  QTSFL.CHARGEABLE_WEIGHT * 1000 ELSE QTSFL.CHARGEABLE_WEIGHT END ELSE CASE WHEN DMT.DIMENTION_ID = 'MT' THEN  QTSFL.EXPECTED_WEIGHT * 1000 ELSE QTSFL.EXPECTED_WEIGHT END END WEIGHT,QTSFL.FRT_WEIGHT, ");
            strBuilder.Append(" QST.shipping_terms_mst_pk, ");
            //'Added By Sushama
            strBuilder.Append(" QTSFL.PORT_MST_PLR_FK , ");
            strBuilder.Append(" QTSFL.PORT_MST_PFD_FK , ");
            strBuilder.Append(" QTSFL.TRANSPORTER_PLR_FK ,");
            strBuilder.Append(" QTSFL.TRANSPORTER_PFD_FK ");
            //'END Sushama
            strBuilder.Append(" FROM QUOTATION_MST_TBL QST, ");
            strBuilder.Append(" QUOTATION_DTL_TBL QTSFL, ");
            strBuilder.Append(" CUSTOMER_MST_TBL CMT, ");
            strBuilder.Append(" PLACE_MST_TBL CPMT, ");
            strBuilder.Append(" PLACE_MST_TBL DPMT, ");
            strBuilder.Append(" AGENT_MST_TBL CBAMT, ");
            strBuilder.Append(" PORT_MST_TBL POL, ");
            strBuilder.Append(" PORT_MST_TBL POD, ");
            strBuilder.Append(" AGENT_MST_TBL DPAMT, ");
            strBuilder.Append(" CUSTOMER_MST_TBL  CU,");
            strBuilder.Append(" AGENT_MST_TBL  AGT,");
            strBuilder.Append(" AGENT_MST_TBL TAR_AGENT, ");
            strBuilder.Append(" CUSTOMER_CATEGORY_MST_TBL CCMT, ");
            strBuilder.Append(" DIMENTION_UNIT_MST_TBL    DMT ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" QST.QUOTATION_MST_PK=QTSFL.QUOTATION_MST_FK ");
            strBuilder.Append(" AND QST.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+) ");
            strBuilder.Append(" AND CMT.DP_AGENT_MST_FK=DPAMT.AGENT_MST_PK(+) ");
            strBuilder.Append(" AND QST.COL_PLACE_MST_FK=CPMT.PLACE_PK(+) ");
            strBuilder.Append(" AND QST.DEL_PLACE_MST_FK=DPMT.PLACE_PK(+) ");
            strBuilder.Append(" AND QST.AGENT_MST_FK=CBAMT.AGENT_MST_PK(+) ");
            strBuilder.Append(" AND QST.TARIFF_AGENT_MST_FK=TAR_AGENT.AGENT_MST_PK(+) ");
            strBuilder.Append(" AND QST.CUSTOMER_CATEGORY_MST_FK=CCMT.CUSTOMER_CATEGORY_MST_PK(+) ");
            strBuilder.Append(" AND QTSFL.PORT_MST_POL_FK=POL.PORT_MST_PK(+) ");
            strBuilder.Append("  AND CU.CUSTOMER_MST_PK(+) = QST.THIRD_PARTY_FRTPAYER_FK ");
            strBuilder.Append("  AND AGT.AGENT_MST_PK(+) = QST.TARIFF_AGENT_MST_FK ");
            strBuilder.Append(" AND QTSFL.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ");
            strBuilder.Append(" AND QTSFL.BASIS = DMT.DIMENTION_UNIT_MST_PK(+) ");
            strBuilder.Append(" AND QST.QUOTATION_MST_PK= " + lngSQEPK);
            strBuilder.Append(" AND QTSFL.PORT_MST_POL_FK= " + strQuotationPOLPK);
            strBuilder.Append(" AND QTSFL.PORT_MST_POD_FK= " + strQuotationPODPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
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

        public object FetchSQOFreight(DataTable dtMain, long lngSQEPK, long lngPolPk, long lngPodPk)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();

            strBuilder.Append(" SELECT ");
            strBuilder.Append(" QTSOC.FREIGHT_ELEMENT_MST_FK, ");
            strBuilder.Append(" QTSOC.CURRENCY_MST_FK, ");
            strBuilder.Append(" QTSOC.AMOUNT, ");
            strBuilder.Append(" QTSOC.FREIGHT_TYPE ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" QUOTATION_MST_TBL QHDR, ");
            strBuilder.Append(" QUOTATION_DTL_TBL QTRN, ");
            strBuilder.Append(" QUOTATION_OTHER_FREIGHT_TRN QTSOC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" QTSOC.QUOTATION_DTL_FK= QTRN.QUOTE_DTL_PK ");
            strBuilder.Append(" AND QTRN.QUOTATION_MST_FK=QHDR.QUOTATION_MST_PK ");
            strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + lngPolPk);
            strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + lngPodPk);
            strBuilder.Append(" AND QHDR.QUOTATION_MST_PK= " + lngSQEPK);

            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
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

        public object FetchQuotationCDimension(DataTable dtMain, long lngQPK, long lngPOLPK, long lngPODPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING BOOKING
            strBuilder.Append(" SELECT ");
            strBuilder.Append(" ROWNUM AS SNO, ");
            strBuilder.Append(" QCALC.CARGO_NOP AS NOP, ");
            strBuilder.Append(" QCALC.CARGO_LENGTH AS LENGTH, ");
            strBuilder.Append(" QCALC.CARGO_WIDTH AS WIDTH, ");
            strBuilder.Append(" QCALC.CARGO_HEIGHT AS HEIGHT,  ");
            strBuilder.Append(" QCALC.CARGO_CUBE AS CUBE,");
            strBuilder.Append(" QCALC.CARGO_VOLUME_WT AS VOLWEIGHT, ");
            strBuilder.Append(" QCALC.CARGO_ACTUAL_WT AS ACTWEIGHT, ");
            strBuilder.Append(" QCALC.CARGO_DENSITY AS DENSITY, ");
            strBuilder.Append(" NULL AS PK, ");
            strBuilder.Append(" NULL AS FK, 0 CARGO_MEASUREMENT, 0 CARGO_WEIGHT_IN, 0 CARGO_DIVISION_FACT ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" QUOTATION_MST_TBL QHDR,");
            strBuilder.Append(" QUOTATION_DTL_TBL QTRN, ");
            strBuilder.Append(" QUOTATION_CARGO_CALC QCALC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" QTRN.QUOTATION_MST_FK=QHDR.QUOTATION_MST_PK ");
            strBuilder.Append(" AND QCALC.QUOTATION_DTL_FK=QTRN.QUOTE_DTL_PK ");
            strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + lngPOLPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + lngPODPK);
            strBuilder.Append(" AND QTRN.QUOTATION_MST_FK= " + lngQPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
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

        #endregion "Fetch Sea Quotation Entry Details"

        #region "Fetch Sea Spot Rate Entry Details"

        public void FetchSpotRateEntry(DataSet dsSpotRateEntry, long lngSpotRatePk, string strPOLPK, string strPODPK, short BizType = 2)
        {
            DataTable dtMain = new DataTable();
            FetchSpotRateEntryHeader(dtMain, lngSpotRatePk, BizType);
            //Data set ds contains the Master Table details
            dsSpotRateEntry.Tables.Add(dtMain);
        }

        public void FetchSpotRateEntryHeader(DataTable dtMain, long lngSpotRateEntryPK, short BizType = 2)
        {
            System.Text.StringBuilder strBuilderSpotRate = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING Spot Rate
            if (BizType == 2)
            {
                strBuilderSpotRate.Append(" SELECT ");
                strBuilderSpotRate.Append(" HDR.CUSTOMER_MST_FK, ");
                strBuilderSpotRate.Append(" CMT.CUSTOMER_ID, ");
                strBuilderSpotRate.Append(" HDR.COL_ADDRESS, ");
                strBuilderSpotRate.Append(" HDR.PACK_COUNT, ");
                strBuilderSpotRate.Append(" HDR.GROSS_WEIGHT, ");
                strBuilderSpotRate.Append(" HDR.CHARGEABLE_WEIGHT, ");
                strBuilderSpotRate.Append(" HDR.VOLUME_IN_CBM, ");
                strBuilderSpotRate.Append(" HDR.VOLUME_WEIGHT, ");
                strBuilderSpotRate.Append(" HDR.DENSITY, ");
                strBuilderSpotRate.Append(" DPAMT.AGENT_MST_PK DPAGENTPK, ");
                strBuilderSpotRate.Append(" DPAMT.AGENT_ID DPAGENT, ");
                strBuilderSpotRate.Append(" DPAMT.AGENT_NAME DPAGENTNAME ");
                strBuilderSpotRate.Append(" FROM ");
                strBuilderSpotRate.Append(" RFQ_SPOT_RATE_SEA_TBL HDR, ");
                strBuilderSpotRate.Append(" rfq_spot_trn_sea_fcl_lcl trn, ");
                strBuilderSpotRate.Append(" CUSTOMER_MST_TBL CMT, ");
                strBuilderSpotRate.Append(" AGENT_MST_TBL DPAMT ");
                strBuilderSpotRate.Append(" WHERE ");
                strBuilderSpotRate.Append(" TRN.RFQ_SPOT_SEA_FK = HDR.RFQ_SPOT_SEA_PK ");
                strBuilderSpotRate.Append(" AND HDR.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+) ");
                strBuilderSpotRate.Append(" AND CMT.DP_AGENT_MST_FK=DPAMT.AGENT_MST_PK(+) ");
                strBuilderSpotRate.Append(" AND HDR.RFQ_SPOT_SEA_PK= " + lngSpotRateEntryPK);
            }
            else
            {
                strBuilderSpotRate.Append("SELECT ");
                strBuilderSpotRate.Append("HDR.CUSTOMER_MST_FK, ");
                strBuilderSpotRate.Append("CMT.CUSTOMER_NAME CUSTOMER_ID, ");
                strBuilderSpotRate.Append("HDR.COL_ADDRESS, ");
                strBuilderSpotRate.Append("HDR.AIRLINE_REFERENCE_NO, ");
                strBuilderSpotRate.Append("TRN.MAWB_REF_NO ");
                strBuilderSpotRate.Append(" FROM ");
                strBuilderSpotRate.Append("RFQ_SPOT_RATE_AIR_TBL HDR, ");
                strBuilderSpotRate.Append("RFQ_SPOT_TRN_AIR_TBL TRN, ");
                strBuilderSpotRate.Append("CUSTOMER_MST_TBL CMT, ");
                strBuilderSpotRate.Append("AGENT_MST_TBL DPAMT ");
                strBuilderSpotRate.Append("WHERE ");
                strBuilderSpotRate.Append("TRN.RFQ_SPOT_AIR_FK = HDR.RFQ_SPOT_AIR_PK ");
                strBuilderSpotRate.Append("AND HDR.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+) ");
                strBuilderSpotRate.Append("AND CMT.DP_AGENT_MST_FK=DPAMT.AGENT_MST_PK(+) ");
                strBuilderSpotRate.Append("AND HDR.RFQ_SPOT_AIR_PK= " + lngSpotRateEntryPK);
            }

            try
            {
                dtMain = objwf.GetDataTable(strBuilderSpotRate.ToString());
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

        public object FetchSpotRateCDimension(DataTable dtMain, long lngSpotRateEntryPK, short BizType = 2)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING BOOKING
            if (BizType == 2)
            {
                strBuilder.Append(" SELECT ");
                strBuilder.Append(" ROWNUM AS SNO, ");
                strBuilder.Append(" RSACC.CARGO_NOP AS NOP, ");
                strBuilder.Append(" RSACC.CARGO_LENGTH AS LENGTH, ");
                strBuilder.Append(" RSACC.CARGO_WIDTH AS WIDTH, ");
                strBuilder.Append(" RSACC.CARGO_HEIGHT AS HEIGHT, ");
                strBuilder.Append(" RSACC.CARGO_CUBE AS CUBE, ");
                strBuilder.Append(" RSACC.CARGO_VOLUME_WT AS VOLWEIGHT, ");
                strBuilder.Append(" RSACC.CARGO_ACTUAL_WT AS ACTWEIGHT, ");
                strBuilder.Append(" RSACC.CARGO_DENSITY AS DENSITY, ");
                strBuilder.Append(" NULL AS PK, ");
                strBuilder.Append(" NULL AS FK, 0 CARGO_MEASUREMENT, 0 CARGO_WEIGHT_IN, 0 CARGO_DIVISION_FACT ");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" RFQ_SPOT_SEA_CARGO_CALC RSACC ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" RSACC.RFQ_SPOT_SEA_FK= " + lngSpotRateEntryPK);
            }
            else
            {
                strBuilder.Append("SELECT ");
                strBuilder.Append("ROWNUM AS SNO, ");
                strBuilder.Append("RSACC.CARGO_NOP AS NOP, ");
                strBuilder.Append("RSACC.CARGO_LENGTH AS LENGTH, ");
                strBuilder.Append("RSACC.CARGO_WIDTH AS WIDTH, ");
                strBuilder.Append("RSACC.CARGO_HEIGHT AS HEIGHT, ");
                strBuilder.Append("RSACC.CARGO_CUBE AS CUBE, ");
                strBuilder.Append("RSACC.CARGO_VOLUME_WT AS VOLWEIGHT, ");
                strBuilder.Append("RSACC.CARGO_ACTUAL_WT AS ACTWEIGHT, ");
                strBuilder.Append("RSACC.CARGO_DENSITY AS DENSITY, ");
                strBuilder.Append("NULL AS PK, ");
                strBuilder.Append("NULL AS FK ");
                strBuilder.Append("FROM ");
                strBuilder.Append("RFQ_SPOT_RATE_AIR_TBL RSRAT, ");
                strBuilder.Append("RFQ_SPOT_TRN_AIR_TBL RSTAT, ");
                strBuilder.Append("RFQ_SPOT_AIR_CARGO_CALC RSACC ");
                strBuilder.Append("WHERE ");
                strBuilder.Append("RSTAT.RFQ_SPOT_AIR_FK = RSRAT.RFQ_SPOT_AIR_PK ");
                strBuilder.Append("AND RSACC.RFQ_SPOT_AIR_TRN_FK=RSTAT.RFQ_SPOT_AIR_TRN_PK ");
                strBuilder.Append("AND RSTAT.RFQ_SPOT_AIR_FK= " + lngSpotRateEntryPK);
            }

            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
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

        #endregion "Fetch Sea Spot Rate Entry Details"

        #region "Fetch Grid Values Manual"

        public void CheckQus(string QnsNo)
        {
            WorkFlow objWF = new WorkFlow();
            bool Result = false;
            try
            {
                Result = objWF.ExecuteCommands("update quotation_mst_tbl set status=2 where quotation_ref_no='" + QnsNo + "'");
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

        public Int16 CheckSpcl(Int32 BkgPk, Int16 Biz)
        {
            WorkFlow objWF = new WorkFlow();
            string strSql = null;
            string Result = null;

            try
            {
                strSql = "";

                if (Biz == 2)
                {
                    strSql = "select BOOKING_MST_FK from BOOKING_TRN where BOOKING_MST_FK=" + BkgPk;
                }

                if (Biz == 1)
                {
                    strSql = "select BOOKING_MST_FK from booking_trn_air where BOOKING_MST_FK=" + BkgPk;
                }

                Result = objWF.ExecuteScaler(strSql);

                if (!string.IsNullOrEmpty(Result))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
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

        public DataSet FetchGridManualValues(DataSet dsGrid, Int16 intIsFcl = 0, string strPOL = "", string strPOD = "", string strContainer = "", Int32 BaseCurrency = 0, bool Fetch = false)
        {
            try
            {
                DataTable dtMain = new DataTable();
                DataTable dtChild = new DataTable();
                if (string.IsNullOrEmpty(strContainer.Trim()))
                    strContainer = "0";
                dtMain = FetchHeaderManual(intIsFcl, strContainer, Fetch);
                dtChild = FetchFreightManual(intIsFcl, strPOL, strPOD, strContainer, BaseCurrency, Fetch);
                dsGrid.Tables.Add(dtMain);
                dsGrid.Tables.Add(dtChild);
                string relCol = "CONTAINERPK";
                if (intIsFcl == 2 | intIsFcl == 4)
                    relCol = "BASISPK";
                if (intIsFcl == 4)
                {
                    DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] {
                        dsGrid.Tables[0].Columns["BASISPK"],
                        dsGrid.Tables[0].Columns["COMMODITYPK"]
                    }, new DataColumn[] {
                        dsGrid.Tables[1].Columns["BASISPK"],
                        dsGrid.Tables[1].Columns["COMMODITYPK"]
                    });
                    dsGrid.Relations.Clear();
                    dsGrid.Relations.Add(rel);
                }
                else
                {
                    DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] { dsGrid.Tables[0].Columns[relCol] }, new DataColumn[] { dsGrid.Tables[1].Columns[relCol] });
                    dsGrid.Relations.Clear();
                    dsGrid.Relations.Add(rel);
                }

                return dsGrid;
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

        private DataTable FetchHeaderManual(Int16 intIsFcl = 0, string strContainer = "", bool Fetch = false)
        {
            try
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                int intFetch = 2;
                if (Fetch)
                    intFetch = 1;

                //BBC
                if (intIsFcl == 4)
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEPK,");
                    strQuery.Append("       '7' AS TRNTYPESTATUS,");
                    strQuery.Append("       'Manual' AS CONTRACTTYPE,");
                    strQuery.Append("       NULL AS REFNO,");
                    strQuery.Append("       NULL AS OPERATOR,");
                    strQuery.Append("       CMT.COMMODITY_NAME COMMODITY,");
                    strQuery.Append("       0 PACK_PK,");
                    strQuery.Append("       '' PACK_TYPE,");
                    strQuery.Append("       '' AS BASIS,");
                    strQuery.Append("       '' AS QTY,");
                    strQuery.Append("       '0.000' CARGO_WT,");
                    strQuery.Append("       '0.000' CARGO_VOL,");
                    strQuery.Append("       '' CARGO_CALC,");
                    strQuery.Append("       NULL AS RATE,");
                    strQuery.Append("       '' AS BKGRATE,");
                    strQuery.Append("       '' AS NET,");
                    strQuery.Append("       '' AS TOTALRATE,");
                    strQuery.Append("       '0' AS SEL,");
                    strQuery.Append("       ROWNUM CONTAINERPK,");
                    strQuery.Append("       CMT.COMMODITY_MST_PK AS COMMODITYPK,");
                    strQuery.Append("       NULL AS OPERATORPK,");
                    strQuery.Append("       '' AS TRANSACTIONPK,");
                    strQuery.Append("       NULL AS BASISPK");
                    strQuery.Append("  FROM COMMODITY_MST_TBL CMT");
                    strQuery.Append(" WHERE (1 = 1)");
                    strQuery.Append("   AND CMT.COMMODITY_MST_PK IN (" + strContainer + ")");
                    strQuery.Append(" GROUP BY CMT.COMMODITY_NAME, ROWNUM, CMT.COMMODITY_MST_PK");
                }
                else if (intIsFcl != 2)
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEPK,");
                    strQuery.Append("       '7' AS TRNTYPESTATUS,");
                    strQuery.Append("       'Manual' AS CONTRACTTYPE,");
                    strQuery.Append("       NULL AS REFNO,");
                    strQuery.Append("       NULL AS OPERATOR,");
                    strQuery.Append("       CONTAINER_TYPE_MST_ID AS TYPE,");
                    strQuery.Append("       '' AS BOXES,");
                    strQuery.Append("       NULL AS CARGO,");
                    strQuery.Append("       NULL AS COMMODITY,");
                    strQuery.Append("       NULL AS RATE,");
                    strQuery.Append("       '' AS BKGRATE,");
                    strQuery.Append("       '' AS NET,");
                    strQuery.Append("       '' AS TOTALRATE,");
                    strQuery.Append("       '0' AS SEL,");
                    strQuery.Append("       CONTAINER_TYPE_MST_PK AS CONTAINERPK,");
                    strQuery.Append("       NULL AS CARGOPK,");
                    strQuery.Append("       NULL AS COMMODITYPK,");
                    strQuery.Append("       NULL AS OPERATORPK,");
                    strQuery.Append("       '' AS TRANSACTIONPK,");
                    strQuery.Append("       NULL AS BASISPK");
                    strQuery.Append("  FROM(SELECT OCTMT.CONTAINER_TYPE_MST_ID,OCTMT.CONTAINER_TYPE_MST_PK,OCTMT.Preferences");
                    strQuery.Append("  FROM CONTAINER_TYPE_MST_TBL OCTMT");
                    strQuery.Append(" WHERE (1 = " + intFetch + ")");
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK IN");
                        strQuery.Append("       (" + strContainer + ")");
                    }
                    strQuery.Append(" AND OCTMT.ACTIVE_FLAG = 1 ");
                    strQuery.Append(" GROUP BY OCTMT.CONTAINER_TYPE_MST_ID, OCTMT.CONTAINER_TYPE_MST_PK,OCTMT.Preferences ORDER BY OCTMT.Preferences)");
                }
                else
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEPK,");
                    strQuery.Append("       '7' AS TRNTYPESTATUS,");
                    strQuery.Append("       'Manual' AS CONTRACTTYPE,");
                    strQuery.Append("       NULL AS REFNO,");
                    strQuery.Append("       NULL AS OPERATOR,");
                    strQuery.Append("       OUOM.DIMENTION_ID AS BASIS,");
                    strQuery.Append("       '' AS QTY,");
                    strQuery.Append("       NULL AS CARGO,");
                    strQuery.Append("       NULL AS COMMODITY,");
                    strQuery.Append("       NULL AS RATE,");
                    strQuery.Append("       '' AS BKGRATE,");
                    strQuery.Append("       '' AS NET,");
                    strQuery.Append("       '' AS TOTALRATE,");
                    strQuery.Append("       '0' AS SEL,");
                    strQuery.Append("       '' AS CONTAINERPK,");
                    strQuery.Append("       NULL AS CARGOPK,");
                    strQuery.Append("       NULL AS COMMODITYPK,");
                    strQuery.Append("       NULL AS OPERATORPK,");
                    strQuery.Append("       '' AS TRANSACTIONPK,");
                    strQuery.Append("       OUOM.DIMENTION_UNIT_MST_PK AS BASISPK");
                    strQuery.Append("  FROM DIMENTION_UNIT_MST_TBL OUOM");
                    strQuery.Append(" WHERE (1 = 1)");
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND OUOM.DIMENTION_UNIT_MST_PK IN  (" + strContainer + ")");
                    }
                    strQuery.Append("   AND OUOM.ACTIVE = 1");

                    strQuery.Append(" GROUP BY OUOM.DIMENTION_ID, OUOM.DIMENTION_UNIT_MST_PK");
                }

                return (new WorkFlow()).GetDataTable(strQuery.ToString());
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

        private DataTable FetchFreightManual(Int16 intIsFcl = 0, string strPOL = "", string strPOD = "", string strContainer = "", Int32 BaseCurrency = 0, bool Fetch = false)
        {
            try
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                int intFetch = 2;
                if (Fetch)
                    intFetch = 1;
                // BBC
                if (intIsFcl == 4)
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEFK,");
                    strQuery.Append("       NULL AS REFNO,");
                    strQuery.Append("       OFEMT.Credit AS BASIS,");
                    strQuery.Append("       CMT.COMMODITY_NAME AS COMMODITY,");
                    strQuery.Append("       OPL.PORT_MST_PK,");
                    strQuery.Append("       OPL.PORT_ID AS POL,");
                    strQuery.Append("       OPD.PORT_MST_PK,");
                    strQuery.Append("       OPD.PORT_ID AS POD,");
                    strQuery.Append("       OFEMT.FREIGHT_ELEMENT_MST_PK,");
                    strQuery.Append("       OFEMT.FREIGHT_ELEMENT_ID,");
                    strQuery.Append("       DECODE(OFEMT.CHARGE_BASIS,");
                    strQuery.Append("              '0',");
                    strQuery.Append("              '',");
                    strQuery.Append("              '1',");
                    strQuery.Append("              '%',");
                    strQuery.Append("              '2',");
                    strQuery.Append("              'Flat Rate',");
                    strQuery.Append("              '3',");
                    strQuery.Append("              'Kgs',");
                    strQuery.Append("              '4',");
                    strQuery.Append("              'Unit') CHARGE_BASIS,");
                    strQuery.Append("       DECODE(0, 1, 'true', 'false') SEL,");
                    strQuery.Append("       OCUMT.CURRENCY_MST_PK,");
                    strQuery.Append("       OCUMT.CURRENCY_ID,");
                    strQuery.Append("       NULL SURCHARGE_VALUE,");
                    strQuery.Append("       null AS MIN_RATE,");
                    strQuery.Append("       1 AS RATE,");
                    strQuery.Append("       null AS BKGRATE,");
                    strQuery.Append("       NULL AS BASISPK,");
                    // strQuery.Append("       '1' AS PYMT_TYPE,")

                    strQuery.Append("   CASE ");
                    strQuery.Append("    WHEN OFEMT.FREIGHT_ELEMENT_ID = 'THD' THEN ");
                    strQuery.Append("        '2' ");
                    strQuery.Append("       ELSE ");
                    strQuery.Append("      '1' ");
                    strQuery.Append("   END AS PYMT_TYPE, ");

                    strQuery.Append("       CMT.COMMODITY_MST_PK COMMODITYPK,");
                    strQuery.Append("       null FreightPK,");
                    strQuery.Append("       null EXCHANGERATE,");
                    strQuery.Append("       '' SURCHARGE_TYPE,");
                    strQuery.Append("       OFEMT.Credit");
                    strQuery.Append("");
                    strQuery.Append("  FROM PORT_MST_TBL            OPL,");
                    strQuery.Append("       PORT_MST_TBL            OPD,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL OFEMT,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   OCUMT,");
                    strQuery.Append("       COMMODITY_MST_TBL       CMT");
                    strQuery.Append(" WHERE (1 = 1)");
                    strQuery.Append("   AND OPL.PORT_MST_PK = " + strPOL);
                    strQuery.Append("   AND OPD.PORT_MST_PK = " + strPOD);
                    strQuery.Append("   AND OCUMT.CURRENCY_MST_PK = " + BaseCurrency);
                    strQuery.Append("   AND OFEMT.BUSINESS_TYPE IN (2, 3)");
                    strQuery.Append("   AND OFEMT.ACTIVE_FLAG = 1");
                    strQuery.Append("   AND nvl(OFEMT.CHARGE_TYPE, 0) <> 3");
                    strQuery.Append("   AND CMT.COMMODITY_MST_PK IN (" + strContainer + ")");
                    strQuery.Append(" ORDER BY OFEMT.PREFERENCE ASC");
                    // FCL
                }
                else if (intIsFcl != 2)
                {
                    strQuery.Append("SELECT  NULL AS TRNTYPEFK,");
                    strQuery.Append("                NULL AS REFNO,");
                    strQuery.Append("                NULL AS TYPE,");
                    //strQuery.Append("       NULL AS CARGO," & vbCrLf)
                    strQuery.Append("                NULL AS COMMODITY,");
                    strQuery.Append("                OPL.PORT_MST_PK,");
                    strQuery.Append("                OPL.PORT_ID AS POL,");
                    strQuery.Append("                OPD.PORT_MST_PK,");
                    strQuery.Append("                OPD.PORT_ID AS POD,");
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_MST_PK,");
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_ID,");
                    strQuery.Append("                DECODE(OFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
                    strQuery.Append("                DECODE(0, 1, 'true', 'false') SEL,");
                    strQuery.Append("                OCUMT.CURRENCY_MST_PK,");
                    strQuery.Append("                OCUMT.CURRENCY_ID,");
                    strQuery.Append("                '' AS RATE,");
                    strQuery.Append("                '' AS BKGRATE,0 TOTAL,");
                    //strQuery.Append("                0 AS RATE," & vbCrLf)
                    //strQuery.Append("                0 AS BKGRATE," & vbCrLf)
                    strQuery.Append("                '' AS BASISPK,");
                    // strQuery.Append("                '1' AS PYMT_TYPE," & vbCrLf)

                    strQuery.Append("   CASE ");
                    strQuery.Append("    WHEN OFEMT.FREIGHT_ELEMENT_ID = 'THD' THEN ");
                    strQuery.Append("        '2' ");
                    strQuery.Append("       ELSE ");
                    strQuery.Append("      '1' ");
                    strQuery.Append("   END AS PYMT_TYPE, ");

                    strQuery.Append("                OFEMT.Credit,0 CHECK_ADVATOS,CONTAINER_TYPE_MST_PK AS CONTAINERPK ");
                    //strQuery.Append("                OFEMT.PREFERENCE" & vbCrLf)
                    strQuery.Append("  FROM CONTAINER_TYPE_MST_TBL  OCTMT,");
                    strQuery.Append("       PORT_MST_TBL            OPL,");
                    strQuery.Append("       PORT_MST_TBL            OPD,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL OFEMT,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   OCUMT");
                    strQuery.Append(" WHERE (1 = " + intFetch + ")");
                    strQuery.Append("   AND OPL.PORT_MST_PK = " + strPOL);
                    strQuery.Append("   AND OPD.PORT_MST_PK = " + strPOD);
                    strQuery.Append("   AND OCUMT.CURRENCY_MST_PK = " + BaseCurrency);
                    //strQuery.Append("   AND OFEMT.BUSINESS_TYPE IN (2, 3)" & vbCrLf)
                    strQuery.Append("   AND OFEMT.BUSINESS_TYPE =2");
                    //strQuery.Append("   AND OFEMT.ACTIVE_FLAG IN(0,1)" & vbCrLf)
                    strQuery.Append("   AND OFEMT.ACTIVE_FLAG = 1");
                    strQuery.Append("   AND OCTMT.ACTIVE_FLAG = 1");
                    //OFEMT.CHARGE_BASIS
                    strQuery.Append("   AND nvl(OFEMT.CHARGE_TYPE,0) <> 3");
                    //OFEMT.CHARGE_BASIS
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK IN");
                        strQuery.Append("       (" + strContainer + ")");
                        //ACTIVE_FLAG
                    }
                    strQuery.Append("   ORDER BY OFEMT.PREFERENCE");
                    //LCL
                }
                else
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEFK,");
                    strQuery.Append("                NULL AS REFNO,");
                    strQuery.Append("                NULL AS TYPE,");
                    strQuery.Append("                NULL AS COMMODITY,");
                    strQuery.Append("                OPL.PORT_MST_PK,");
                    strQuery.Append("                OPL.PORT_ID AS POL,");
                    strQuery.Append("                OPD.PORT_MST_PK,");
                    strQuery.Append("                OPD.PORT_ID AS POD,");
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_MST_PK,");
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_ID,");
                    strQuery.Append("                DECODE(OFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
                    strQuery.Append("                DECODE(0, 1, 'true', 'false') SEL,");
                    strQuery.Append("                OCUMT.CURRENCY_MST_PK,");
                    strQuery.Append("                OCUMT.CURRENCY_ID,");
                    strQuery.Append("                0 AS MIN_RATE,");
                    strQuery.Append("                0 AS RATE,");
                    strQuery.Append("                0 AS BKGRATE,0 TOTAL,");
                    strQuery.Append("                OUOM.DIMENTION_UNIT_MST_PK AS BASISPK,");
                    //  strQuery.Append("                '1' AS PYMT_TYPE," & vbCrLf)

                    strQuery.Append("   CASE ");
                    strQuery.Append("    WHEN OFEMT.FREIGHT_ELEMENT_ID = 'THD' THEN ");
                    strQuery.Append("        '2' ");
                    strQuery.Append("       ELSE ");
                    strQuery.Append("      '1' ");
                    strQuery.Append("   END AS PYMT_TYPE, ");

                    strQuery.Append("                 OFEMT.Credit,0 CHECK_ADVATOS,OUOM.DIMENTION_UNIT_MST_PK AS BASISPK1 ");
                    strQuery.Append("");
                    strQuery.Append("  FROM PORT_MST_TBL            OPL,");
                    strQuery.Append("       PORT_MST_TBL            OPD,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL OFEMT,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   OCUMT,");
                    strQuery.Append("       DIMENTION_UNIT_MST_TBL  OUOM");
                    strQuery.Append(" WHERE (1 = 1)");
                    strQuery.Append("   AND OPL.PORT_MST_PK = " + strPOL);
                    strQuery.Append("   AND OPD.PORT_MST_PK = " + strPOD);
                    strQuery.Append("   AND OCUMT.CURRENCY_MST_PK = " + BaseCurrency);
                    strQuery.Append("   AND OFEMT.BUSINESS_TYPE IN (2, 3)");
                    strQuery.Append("   AND OFEMT.ACTIVE_FLAG = 1");
                    strQuery.Append("   AND nvl(OFEMT.CHARGE_TYPE,0) <> 3 ");
                    //OFEMT.CHARGE_BASIS
                    strQuery.Append("   AND OUOM.ACTIVE = 1");
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND DIMENTION_UNIT_MST_PK IN (" + strContainer + ")");
                    }
                    strQuery.Append("   ORDER BY OFEMT.PREFERENCE");
                }
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
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

        //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
        public object FetchEBKGEntryFreight(DataTable dtFreight, long lngSBEPK, Int16 intIsLcl)
        {
            WorkFlow objwf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (intIsLcl == 1)
            {
                sb.Append(" SELECT ");
                sb.Append(" NULL AS TRNTYPEFK, ");
                sb.Append(" BTSFL.TRANS_REF_NO AS REFNO, ");
                sb.Append(" UOM.DIMENTION_ID AS BASIS, ");
                sb.Append(" BTSFL.COMMODITY_MST_FK AS COMMODITYFK, ");
                sb.Append(" BST.PORT_MST_POL_FK AS PORT_MST_PK, ");
                sb.Append(" PL.PORT_ID AS POL, ");
                sb.Append(" BST.PORT_MST_POD_FK AS PORT_MST_PK1, ");
                sb.Append(" PD.PORT_ID AS POD, ");
                sb.Append(" FEMT.FREIGHT_ELEMENT_MST_PK,");
                sb.Append(" FEMT.FREIGHT_ELEMENT_ID, ");
                sb.Append("  DECODE(FEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
                sb.Append(" 'FALSE' AS CHECK_FOR_ALL_IN_RT, ");
                sb.Append(" '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' as CURRENCY_MST_FK,");
                sb.Append(" '" + HttpContext.Current.Session["CURRENCY_ID"] + "' as CURRENCY_ID,");
                sb.Append(" NULL AS MIN_RATE, ");
                sb.Append(" NULL AS RATE, ");
                sb.Append(" NULL AS BKGRATE, ");
                sb.Append(" 0 AS TOTAL, ");
                sb.Append(" NULL AS BASIS, ");
                sb.Append(" 'PrePaid' AS PYMT_TYPE, ");
                sb.Append(" '' as BOOKING_TRN_FRT_PK,FEMT.Credit ");
                sb.Append(" FROM");
                sb.Append(" BOOKING_MST_TBL BST, ");
                sb.Append(" BOOKING_TRN BTSFL, ");
                sb.Append(" CURRENCY_TYPE_MST_TBL CTMT, ");
                sb.Append(" FREIGHT_ELEMENT_MST_TBL FEMT, ");
                sb.Append(" COMMODITY_MST_TBL CMT, ");
                sb.Append(" PORT_MST_TBL PL, ");
                sb.Append(" PORT_MST_TBL PD, ");
                sb.Append(" DIMENTION_UNIT_MST_TBL UOM");
                sb.Append(" WHERE(1 = 1)");
                sb.Append(" AND BTSFL.BOOKING_MST_FK=BST.BOOKING_MST_PK");
                sb.Append(" AND BTSFL.BASIS=UOM.DIMENTION_UNIT_MST_PK(+) ");
                sb.Append(" AND BST.PORT_MST_POL_FK=PL.PORT_MST_PK (+)");
                sb.Append(" AND BST.PORT_MST_POD_FK=PD.PORT_MST_PK (+)");
                sb.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) ");
                sb.Append(" AND BST.BOOKING_MST_PK= " + lngSBEPK);
                sb.Append(" and ctmt.currency_mst_pk = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                sb.Append(" and femt.business_type = 2");
                sb.Append(" AND nvl(FEMT.CHARGE_TYPE,0) <> 3");
                sb.Append("           ORDER BY FEMT.PREFERENCE");
            }
            else
            {
                sb.Append(" SELECT ");
                sb.Append(" NULL AS TRNTYPEFK, ");
                sb.Append(" BTSFL.TRANS_REF_NO AS REFNO,  ");
                sb.Append(" CTMT.CONTAINER_TYPE_MST_ID AS TYPE,  ");
                sb.Append(" BTSFL.COMMODITY_MST_FK AS COMMODITYFK, ");
                sb.Append(" BST.PORT_MST_POL_FK AS PORT_MST_PK, ");
                sb.Append(" PL.PORT_ID AS POL, ");
                sb.Append(" BST.PORT_MST_POD_FK AS PORT_MST_PK1, ");
                sb.Append(" PD.PORT_ID AS POD, ");
                sb.Append(" FEMT.FREIGHT_ELEMENT_MST_PK,");
                sb.Append(" FEMT.FREIGHT_ELEMENT_ID, ");
                sb.Append("  DECODE(FEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
                sb.Append(" 'FALSE' AS CHECK_FOR_ALL_IN_RT, ");
                sb.Append(" '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' as CURRENCY_MST_FK, ");
                sb.Append(" '" + HttpContext.Current.Session["CURRENCY_ID"] + "' as CURRENCY_ID, ");
                sb.Append(" NULL AS RATE, ");
                sb.Append(" NULL AS BKGRATE, ");
                sb.Append(" 0 AS TOTAL, ");

                sb.Append(" NULL AS BASIS, ");
                sb.Append(" 'PrePaid' AS PYMT_TYPE, ");
                sb.Append(" '' as BOOKING_TRN_FRT_PK,FEMT.Credit");
                sb.Append(" FROM ");
                sb.Append(" BOOKING_MST_TBL BST, ");
                sb.Append(" BOOKING_TRN BTSFL, ");
                sb.Append(" CURRENCY_TYPE_MST_TBL CTMT, ");
                sb.Append(" FREIGHT_ELEMENT_MST_TBL FEMT, ");
                sb.Append(" CONTAINER_TYPE_MST_TBL CTMT, ");
                sb.Append(" COMMODITY_MST_TBL CMT, ");
                sb.Append(" PORT_MST_TBL PL, ");
                sb.Append(" PORT_MST_TBL PD ");
                sb.Append(" WHERE(1 = 1) ");
                sb.Append(" AND BTSFL.BOOKING_MST_FK = BST.BOOKING_MST_PK ");
                sb.Append(" AND BTSFL.CONTAINER_TYPE_MST_FK=CTMT.CONTAINER_TYPE_MST_PK (+)");
                sb.Append(" AND BST.PORT_MST_POL_FK=PL.PORT_MST_PK (+)");
                sb.Append(" AND BST.PORT_MST_POD_FK=PD.PORT_MST_PK (+)");
                sb.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) ");
                sb.Append(" AND BST.BOOKING_MST_PK= " + lngSBEPK);
                sb.Append(" and ctmt.currency_mst_pk = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                sb.Append(" and femt.business_type = 2");
                sb.Append(" AND nvl(FEMT.CHARGE_TYPE,0) <> 3");
                sb.Append("           ORDER BY FEMT.PREFERENCE");
            }
            try
            {
                dtFreight = objwf.GetDataTable(sb.ToString());
                return dtFreight;
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

        #endregion "Fetch Grid Values Manual"

        #region "Fetch Grid Values"

        //Public Function FetchForAgentTariff(ByRef dsGrid As DataSet, _
        //                                  Optional ByVal intAgentPK As Int32 = 0, _
        //                                  Optional ByVal BizType As Short = 0, _
        //                                  Optional ByVal ProcessType As Short = 0, _
        //                                  Optional ByVal CargoType As Short = 0, _
        //                                  Optional ByVal intCustomerPK As Int32 = 0, _
        //                                  Optional ByVal intSRateStatus As Short = 0, _
        //                                  Optional ByVal intCContractStatus As Short = 0, _
        //                                  Optional ByVal intOTariffStatus As Short = 0, _
        //                                  Optional ByVal intGTariffStatus As Short = 0, _
        //                                  Optional ByVal intSRRContractStatus As Short = 0, _
        //                                  Optional ByVal strPOL As String = "", _
        //                                  Optional ByVal strPOD As String = "", _
        //                                  Optional ByVal intCommodityPK As Int16 = 0, _
        //                                  Optional ByVal strSDate As String = "", _
        //                                  Optional ByVal strContainer As String = "", _
        //                                  Optional ByVal intSpotRatePk As Int32 = 0, _
        //                                  Optional ByVal CustContRefNr As String = "", _
        //                                  Optional ByVal hdnQuotFetch As String = "", _
        //                                  Optional ByVal EBKGSTATUS As Integer = 0, _
        //                                  Optional ByVal BookingPK As Integer = 0, _
        //                                  Optional ByVal intQuotationDtlPK As Int32 = 0, _
        //                                  Optional ByVal intCarrierPK As Int32 = 0) As DataSet
        //    Dim dtMain As New DataTable
        //    Dim dtChild As New DataTable
        //    Dim objT As New WorkFlow
        //    Dim sb As New System.Text.StringBuilder(5000)
        //    If hdnQuotFetch <> "1" Then
        //        If dsGrid.Tables.Count > 0 Then
        //            If dsGrid.Tables(0).Rows.Count > 0 Then
        //                If strContainer = "" Then
        //                    sb.Append("SELECT ROWTOCOL(' SELECT DISTINCT BT.CONTAINER_TYPE_MST_FK ")
        //                    sb.Append("    FROM BOOKING_TRN BT ")
        //                    sb.Append("   WHERE BT.BOOKING_MST_FK = " & dsGrid.Tables(0).Rows(0).Item("BOOKING_MST_PK") & " ")
        //                    sb.Append("   ') FROM DUAL")
        //                    strContainer = objT.ExecuteScaler(sb.ToString())
        //                End If
        //            End If
        //        End If
        //    End If
        //    If CustContRefNr <> "" And CustContRefNr <> "0" Then
        //        intCContractStatus = 1
        //    End If
        //    If intQuotationPK = 0 And intSpotRatePk = 0 Then
        //        'Updated
        //        dtMain = FetchHeader(, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, _
        //                    intSRateStatus, intCContractStatus, intOTariffStatus, intGTariffStatus, intSRRContractStatus, , CustContRefNr, EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK)
        //        'Updated
        //        dtChild = FetchFreight(, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, _
        //                    intSRateStatus, intCContractStatus, intOTariffStatus, intGTariffStatus, intSRRContractStatus, , CustContRefNr, EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK)
        //    ElseIf intSpotRatePk = 0 Then
        //        dtMain = FetchHeader(intQuotationPK, intIsFcl, , strPOL, strPOD, , , strContainer, , , , , , , , EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK)
        //        dtChild = FetchFreight(intQuotationPK, intIsFcl, , strPOL, strPOD, , , strContainer, , , , , , , , EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK)
        //    Else
        //        dtMain = FetchHeader(, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, _
        //                                    0, 0, 0, 0, 0, intSpotRatePk, intQuotationDtlPK, intCarrierPK)
        //        dtChild = FetchFreight(, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, _
        //                    0, 0, 0, 0, 0, intSpotRatePk, intQuotationDtlPK, intCarrierPK)
        //    End If

        //    If dtChild.Columns.Contains("CHECK_ADVATOS") Then
        //    Else
        //        dtChild.Columns.Add("CHECK_ADVATOS")
        //        dtChild.Columns("CHECK_ADVATOS").DefaultValue = 0
        //    End If

        //    dtChild.Columns.Add("FreightPK")
        //    dsGrid.Tables.Add(dtMain)
        //    dsGrid.Tables.Add(dtChild)
        //    If dsGrid.Tables.Count > 1 Then
        //        If dsGrid.Tables(1).Rows.Count > 0 Then
        //            If intIsFcl = 4 Then
        //                If dsGrid.Tables.Count = 3 Then
        //                    dsGrid.Tables.RemoveAt(0)
        //                    Dim rel As New DataRelation("rl_HEAD_TRAN", New DataColumn() {dsGrid.Tables(0).Columns("REFNO"), dsGrid.Tables(0).Columns("BASIS")}, _
        //                                                                       New DataColumn() {dsGrid.Tables(1).Columns("REFNO"), dsGrid.Tables(1).Columns("BASIS")})
        //                    dsGrid.Relations.Clear()
        //                    dsGrid.Relations.Add(rel)
        //                Else
        //                    Dim rel As New DataRelation("rl_HEAD_TRAN", New DataColumn() {dsGrid.Tables(0).Columns("REFNO"), dsGrid.Tables(0).Columns("BASIS"), dsGrid.Tables(0).Columns("COMMODITYPK")}, _
        //                                                                       New DataColumn() {dsGrid.Tables(1).Columns("REFNO"), dsGrid.Tables(1).Columns("BASIS"), dsGrid.Tables(1).Columns("COMMODITYPK")})
        //                    dsGrid.Relations.Clear()
        //                    dsGrid.Relations.Add(rel)
        //                End If
        //            ElseIf intIsFcl = 2 Then
        //                If dsGrid.Tables.Count = 3 Then
        //                    dsGrid.Tables.RemoveAt(0)
        //                    Dim rel As New DataRelation("rl_HEAD_TRAN", New DataColumn() {dsGrid.Tables(0).Columns("REFNO"), dsGrid.Tables(0).Columns("BASIS")}, _
        //                                                                       New DataColumn() {dsGrid.Tables(1).Columns("REFNO"), dsGrid.Tables(1).Columns("BASIS")})
        //                    dsGrid.Relations.Clear()
        //                    dsGrid.Relations.Add(rel)
        //                Else
        //                    Dim rel As New DataRelation("rl_HEAD_TRAN", New DataColumn() {dsGrid.Tables(0).Columns("REFNO"), dsGrid.Tables(0).Columns("BASIS")}, _
        //                                                                       New DataColumn() {dsGrid.Tables(1).Columns("REFNO"), dsGrid.Tables(1).Columns("BASIS")})
        //                    dsGrid.Relations.Clear()
        //                    dsGrid.Relations.Add(rel)
        //                End If
        //            Else
        //                If dsGrid.Tables.Count = 3 Then
        //                    dsGrid.Tables.RemoveAt(0)
        //                    Dim rel As New DataRelation("rl_HEAD_TRAN", New DataColumn() {dsGrid.Tables(0).Columns("REFNO"), dsGrid.Tables(0).Columns("TYPE")}, _
        //                                                  New DataColumn() {dsGrid.Tables(1).Columns("REFNO"), dsGrid.Tables(1).Columns("TYPE")})
        //                    dsGrid.Relations.Clear()
        //                    dsGrid.Relations.Add(rel)
        //                Else
        //                    Dim rel As New DataRelation("rl_HEAD_TRAN", New DataColumn() {dsGrid.Tables(0).Columns("REFNO"), dsGrid.Tables(0).Columns("TYPE")}, _
        //                                                  New DataColumn() {dsGrid.Tables(1).Columns("REFNO"), dsGrid.Tables(1).Columns("TYPE")})
        //                    dsGrid.Relations.Clear()
        //                    dsGrid.Relations.Add(rel)
        //                End If
        //            End If
        //        End If
        //    End If
        //    Try
        //        Return dsGrid
        //    Catch OraExp As OracleException
        //        Throw OraExp
        //    Catch ex As Exception
        //        Throw ex
        //    End Try
        //End Function
        public DataSet FetchGridValues(DataSet dsGrid, Int32 intQuotationPK = 0, Int16 intIsFcl = 0, Int32 intCustomerPK = 0, short intSRateStatus = 0, short intCContractStatus = 0, short intOTariffStatus = 0, short intGTariffStatus = 0, short intSRRContractStatus = 0, string strPOL = "",
        string strPOD = "", Int16 intCommodityPK = 0, string strSDate = "", string strContainer = "", Int32 intSpotRatePk = 0, string CustContRefNr = "", string hdnQuotFetch = "", int EBKGSTATUS = 0, int BookingPK = 0, Int32 intQuotationDtlPK = 0,
        Int32 intCarrierPK = 0, short intAgentTariffStatus = 0, Int32 DPAgentPk = 0)
        {
            DataTable dtMain = new DataTable();
            DataTable dtChild = new DataTable();
            WorkFlow objT = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (hdnQuotFetch != "1")
            {
                if (dsGrid.Tables.Count > 0)
                {
                    if (dsGrid.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(strContainer))
                        {
                            sb.Append("SELECT ROWTOCOL(' SELECT DISTINCT BT.CONTAINER_TYPE_MST_FK ");
                            sb.Append("    FROM BOOKING_TRN BT ");
                            sb.Append("   WHERE BT.BOOKING_MST_FK = " + dsGrid.Tables[0].Rows[0]["BOOKING_MST_PK"] + " ");
                            sb.Append("   ') FROM DUAL");
                            strContainer = objT.ExecuteScaler(sb.ToString());
                        }
                    }
                }
            }
            if (!string.IsNullOrEmpty(CustContRefNr) & CustContRefNr != "0")
            {
                intCContractStatus = 1;
            }
            if (intQuotationPK == 0 & intSpotRatePk == 0)
            {
                //Updated
                dtMain = FetchHeader(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, intSRateStatus, intCContractStatus,
                intOTariffStatus, intGTariffStatus, intSRRContractStatus, 0, CustContRefNr, EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK, intAgentTariffStatus,
                DPAgentPk);
                //Updated
                dtChild = FetchFreight(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, intSRateStatus, intCContractStatus,
                intOTariffStatus, intGTariffStatus, intSRRContractStatus, 0, CustContRefNr, EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK, intAgentTariffStatus,
                DPAgentPk);
            }
            else if (intSpotRatePk == 0)
            {
                dtMain = FetchHeader(intQuotationPK, intIsFcl, 0, strPOL, strPOD, 0, "", strContainer, 0, 0, 0

                , 0, 0, 0, "", EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK, intAgentTariffStatus,
                DPAgentPk);
                dtChild = FetchFreight(intQuotationPK, intIsFcl, 0, strPOL, strPOD, 0, "", strContainer, 0, 0, 0

                , 0, 0, 0, "", EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK, intAgentTariffStatus,
                DPAgentPk);
            }
            else
            {
                dtMain = FetchHeader(intQuotationPK, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, 0, 0,
                0, 0, 0, intSpotRatePk, intQuotationDtlPK.ToString(), intCarrierPK);
                dtChild = FetchFreight(intQuotationPK, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, 0, 0,
                0, 0, 0, intSpotRatePk, intQuotationDtlPK.ToString(), intCarrierPK);
            }

            if (dtChild.Columns.Contains("CHECK_ADVATOS"))
            {
            }
            else
            {
                dtChild.Columns.Add("CHECK_ADVATOS");
                dtChild.Columns["CHECK_ADVATOS"].DefaultValue = 0;
            }

            dtChild.Columns.Add("FreightPK");
            dsGrid.Tables.Add(dtMain);
            dsGrid.Tables.Add(dtChild);
            if (dsGrid.Tables.Count > 1)
            {
                if (dsGrid.Tables[1].Rows.Count > 0)
                {
                    if (intIsFcl == 4)
                    {
                        if (dsGrid.Tables.Count == 3)
                        {
                            dsGrid.Tables.RemoveAt(0);
                            DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] {
                                dsGrid.Tables[0].Columns["REFNO"],
                                dsGrid.Tables[0].Columns["BASIS"]
                            }, new DataColumn[] {
                                dsGrid.Tables[1].Columns["REFNO"],
                                dsGrid.Tables[1].Columns["BASIS"]
                            });
                            dsGrid.Relations.Clear();
                            dsGrid.Relations.Add(rel);
                        }
                        else
                        {
                            DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] {
                                dsGrid.Tables[0].Columns["REFNO"],
                                dsGrid.Tables[0].Columns["BASIS"],
                                dsGrid.Tables[0].Columns["COMMODITYPK"]
                            }, new DataColumn[] {
                                dsGrid.Tables[1].Columns["REFNO"],
                                dsGrid.Tables[1].Columns["BASIS"],
                                dsGrid.Tables[1].Columns["COMMODITYPK"]
                            });
                            dsGrid.Relations.Clear();
                            dsGrid.Relations.Add(rel);
                        }
                    }
                    else if (intIsFcl == 2)
                    {
                        if (dsGrid.Tables.Count == 3)
                        {
                            dsGrid.Tables.RemoveAt(0);
                            DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] {
                                dsGrid.Tables[0].Columns["REFNO"],
                                dsGrid.Tables[0].Columns["BASIS"]
                            }, new DataColumn[] {
                                dsGrid.Tables[1].Columns["REFNO"],
                                dsGrid.Tables[1].Columns["BASIS"]
                            });
                            dsGrid.Relations.Clear();
                            dsGrid.Relations.Add(rel);
                        }
                        else
                        {
                            DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] {
                                dsGrid.Tables[0].Columns["REFNO"],
                                dsGrid.Tables[0].Columns["BASIS"]
                            }, new DataColumn[] {
                                dsGrid.Tables[1].Columns["REFNO"],
                                dsGrid.Tables[1].Columns["BASIS"]
                            });
                            dsGrid.Relations.Clear();
                            dsGrid.Relations.Add(rel);
                        }
                    }
                    else
                    {
                        if (dsGrid.Tables.Count == 3)
                        {
                            dsGrid.Tables.RemoveAt(0);
                            DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] {
                                dsGrid.Tables[0].Columns["REFNO"],
                                dsGrid.Tables[0].Columns["TYPE"]
                            }, new DataColumn[] {
                                dsGrid.Tables[1].Columns["REFNO"],
                                dsGrid.Tables[1].Columns["TYPE"]
                            });
                            dsGrid.Relations.Clear();
                            dsGrid.Relations.Add(rel);
                        }
                        else
                        {
                            DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] {
                                dsGrid.Tables[0].Columns["REFNO"],
                                dsGrid.Tables[0].Columns["TYPE"]
                            }, new DataColumn[] {
                                dsGrid.Tables[1].Columns["REFNO"],
                                dsGrid.Tables[1].Columns["TYPE"]
                            });
                            dsGrid.Relations.Clear();
                            dsGrid.Relations.Add(rel);
                        }
                    }
                }
            }
            try
            {
                return dsGrid;
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

        #endregion "Fetch Grid Values"

        #region "Fetch Header"

        //Search for data if any exist for the customer or Agent with the given details
        //Cargo Type,Port Pair,Commodity Group,
        //Port Pair wise Container Types
        public DataTable FetchHeader(Int32 intQuotationPK = 0, Int16 intIsFcl = 0, Int32 intCustomerPK = 0, string strPOL = "", string strPOD = "", Int16 intCommodityPK = 0, string strSDate = "", string strContainer = "", short intSRateStatus = 0, short intCContractStatus = 0,
        short intOTariffStatus = 0, short intGTariffStatus = 0, short intSRRContractStatus = 0, Int32 intSpotRatePK = 0, string CustContRefNr = "", int EBKGSTATUS = 0, int BookingPK = 0, int intQuotationDtlPK = 0, int intCarrierPK = 0, short intAgentTariffStatus = 0,
        int DPAgentPK = 0)
        {
            try
            {
                string strSql = null;
                ArrayList arrCCondition = new ArrayList();
                string OperatorRate = null;
                string strCustomer = null;
                System.Text.StringBuilder strSRRSBuilder = new System.Text.StringBuilder();
                if (strContainer.Length == 0 | strContainer.ToUpper() == "N" | strContainer.ToUpper() == "UNDEFINED")
                {
                    arrCCondition.Add("");
                    arrCCondition.Add("");
                    arrCCondition.Add("");
                    arrCCondition.Add("");
                    arrCCondition.Add("");
                    arrCCondition.Add("");
                }
                else
                {
                    if (intIsFcl == 2)
                    {
                        arrCCondition.Add("AND QTRN.BASIS IN(" + strContainer + ") ");
                        arrCCondition.Add("AND SRRSTSF.LCL_BASIS IN(" + strContainer + ") ");
                        arrCCondition.Add("AND CCTRN.LCL_BASIS IN(" + strContainer + ") ");
                        arrCCondition.Add("AND OTRN.LCL_BASIS IN(" + strContainer + ") ");
                        arrCCondition.Add("AND SRRTRN.LCL_BASIS IN(" + strContainer + ") ");
                    }
                    else
                    {
                        arrCCondition.Add("AND QCTMT.CONTAINER_TYPE_MST_PK IN(" + strContainer + ") ");
                        arrCCondition.Add("AND SRCTMT.CONTAINER_TYPE_MST_PK IN(" + strContainer + ") ");
                        arrCCondition.Add("AND CCTRN.CONTAINER_TYPE_MST_FK IN(" + strContainer + ") ");
                        arrCCondition.Add("AND OCTMT.CONTAINER_TYPE_MST_PK IN(" + strContainer + ") ");
                        arrCCondition.Add("AND SRRTRN.CONTAINER_TYPE_MST_FK IN(" + strContainer + ") ");
                    }
                }
                if (intCustomerPK == 0)
                {
                    strCustomer = " ";
                }
                else
                {
                    strCustomer = " AND (SRRSRST.CUSTOMER_MST_FK= " + intCustomerPK + " OR SRRSRST.CUSTOMER_MST_FK IS NULL ";
                    strCustomer = strCustomer + "     OR   SRRSRST.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ";
                    strCustomer = strCustomer + "     FROM AFFILIATE_CUSTOMER_DTLS A, RFQ_SPOT_RATE_SEA_TBL CT , CUSTOMER_MST_TBL C ";
                    strCustomer = strCustomer + "    WHERE A.REFERENCE_MST_FK = CT.RFQ_SPOT_SEA_PK                          ";
                    strCustomer = strCustomer + "    AND A.CUST_MST_FK       =  " + Convert.ToString(intCustomerPK) + "               ";
                    strCustomer = strCustomer + "     AND A.CUST_MST_FK=C.CUSTOMER_MST_PK ))        ";
                }
                //QUOTATION
                if (!(intQuotationPK == 0))
                {
                    //Updated
                    strSql = Convert.ToString(funQuotationHeader(arrCCondition, strPOL, strPOD, intQuotationPK, intIsFcl, strContainer, EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK));
                }
                else
                {
                    //SPOT RATE
                    if (!(intSpotRatePK == 0))
                    {
                        strSql = Convert.ToString(funSpotRateHeader(arrCCondition, strCustomer, Convert.ToString(intCommodityPK), strPOL, strPOD, Convert.ToString(intSpotRatePK), strSDate, intIsFcl, 1));
                    }
                    if (intSRateStatus == 1 & intSpotRatePK == 0)
                    {
                        strSql = Convert.ToString(funSpotRateHeader(arrCCondition, strCustomer, Convert.ToString(intCommodityPK), strPOL, strPOD, Convert.ToString(intSpotRatePK), strSDate, intIsFcl, 2));
                    }
                    //CUSTOMER(CONTRACT)
                    if (intCContractStatus == 1)
                    {
                        strSql = Convert.ToString(funCustContHeader(arrCCondition, intCustomerPK, Convert.ToString(intCommodityPK), strPOL, strPOD, strSDate, intIsFcl, getDefault(CustContRefNr, "").ToString(), EBKGSTATUS, BookingPK));
                    }
                    //SPECIAL RATE REQUEST CONTRACT
                    if (intSRRContractStatus == 1)
                    {
                        strSql = Convert.ToString(funSRRHeader(arrCCondition, intCustomerPK, Convert.ToString(intCommodityPK), strPOL, strPOD, strSDate, intIsFcl));
                    }
                    //OPERATOR TARIFF
                    if (intOTariffStatus == 1 | intAgentTariffStatus == 1)
                    {
                        strSql = Convert.ToString(funSLTariffHeader(arrCCondition, Convert.ToString(intCommodityPK), strPOL, strPOD, strSDate, intIsFcl, intAgentTariffStatus, DPAgentPK));
                    }
                    //GENERAL TARIFF
                    if (intGTariffStatus == 1)
                    {
                        strSql = Convert.ToString(funGTariffHeader(arrCCondition, Convert.ToString(intCommodityPK), strPOL, strPOD, strSDate, intIsFcl));
                    }
                }
                //Code Snippet for returning the container details in preference order
                if (Convert.ToInt32(intIsFcl) == 1)
                {
                    string newQuery = "SELECT QQ.* FROM (" + strSql + ") QQ,CONTAINER_TYPE_MST_TBL CTMT WHERE QQ.CONTAINERPK=CTMT.CONTAINER_TYPE_MST_PK(+) ";
                    newQuery += " ORDER BY CTMT.PREFERENCES ";
                    strSql = newQuery;
                }
                return (new WorkFlow()).GetDataTable(strSql);
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

        public object funQuotationHeader(ArrayList arrCCondition, string strPOL, string strPOD, Int32 intQuotationPK, Int16 intIsFcl, string strContainer = "", int EBKGSTATUS = 0, int BookingPK = 0, int intQuotationDtlPK = 0, int intCarrierPK = 0)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strBuilder.Append(" SELECT DISTINCT QHDR.QUOTATION_MST_PK AS TRNTYPEPK, ");
                    strBuilder.Append(" '1' AS TRNTYPESTATUS, ");
                    strBuilder.Append(" 'Quote' AS CONTRACTTYPE, ");
                    strBuilder.Append(" QHDR.QUOTATION_REF_NO AS REFNO, ");
                    strBuilder.Append(" QOMT.OPERATOR_ID AS OPERATOR, ");
                    strBuilder.Append(" QUOM.DIMENTION_ID AS BASIS, ");
                    strBuilder.Append(" case WHEN QUOM.DIMENTION_ID = 'KGS' THEN QTRN.EXPECTED_WEIGHT ");
                    strBuilder.Append(" when QUOM.DIMENTION_ID = 'CBM' THEN QTRN.EXPECTED_VOLUME ");
                    strBuilder.Append(" when QUOM.DIMENTION_ID = 'W/M' then (case when QTRN.EXPECTED_WEIGHT > QTRN.EXPECTED_VOLUME ");
                    strBuilder.Append(" then QTRN.EXPECTED_WEIGHT else QTRN.EXPECTED_VOLUME end) end as QTY,");
                    strBuilder.Append("'' AS CARGO, ");
                    strBuilder.Append(" NULL AS COMMODITY, ");
                    strBuilder.Append(" NULL AS RATE, ");
                    strBuilder.Append(" '' AS BKGRATE, ");
                    strBuilder.Append(" '' AS NET, ");
                    strBuilder.Append(" '' AS TOTALRATE, ");
                    strBuilder.Append(" '1' AS SEL, ");
                    strBuilder.Append(" '' AS CONTAINERPK, ");
                    strBuilder.Append("'' AS CARGOPK, ");
                    strBuilder.Append(" CASE WHEN QTRN.COMMODITY_MST_FKS IS NULL THEN TO_CHAR(QTRN.COMMODITY_MST_FK) ELSE QTRN.COMMODITY_MST_FKS END AS COMMODITYPK, ");
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK AS OPERATORPK, ");
                    strBuilder.Append(" '' AS TRANSACTIONPK,QUOM.DIMENTION_UNIT_MST_PK AS BASISPK, ");
                    strBuilder.Append("       NVL(CU.CUSTOMER_MST_PK,0) THDFRTPAYER_PK,");
                    strBuilder.Append("       CU.CUSTOMER_ID THDFRTPAYER_ID,");
                    strBuilder.Append("       CU.CUSTOMER_NAME THDFRTPAYER_NAME,");
                    strBuilder.Append("       NVL(AGT.AGENT_MST_PK,0) FOREIGN_AGENT_PK,");
                    strBuilder.Append("       AGT.AGENT_ID FOREIGN_AGENT_ID,");
                    strBuilder.Append("       AGT.AGENT_NAME FOREIGN_AGENT_NAME,");
                    strBuilder.Append("       NVL(QHDR.COLLECT_AGENT_FLAG,0) COLLECT_AGENT_FLAG");
                    strBuilder.Append("  ");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" QUOTATION_MST_TBL QHDR, ");
                    strBuilder.Append(" QUOTATION_DTL_TBL QTRN, ");
                    strBuilder.Append(" QUOTATION_FREIGHT_TRN QTRNCHRG, ");
                    strBuilder.Append("       CUSTOMER_MST_TBL           CU,");
                    strBuilder.Append("       AGENT_MST_TBL              AGT,");
                    strBuilder.Append(" OPERATOR_MST_TBL QOMT, ");
                    strBuilder.Append(" COMMODITY_MST_TBL QCMT, ");
                    strBuilder.Append(" DIMENTION_UNIT_MST_TBL QUOM ");
                    strBuilder.Append(" WHERE(1 = 1) ");
                    strBuilder.Append(" AND QTRN.CARRIER_MST_FK=QOMT.OPERATOR_MST_PK(+)");
                    strBuilder.Append(" AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK(+) ");
                    strBuilder.Append("   AND CU.CUSTOMER_MST_PK(+) = QHDR.THIRD_PARTY_FRTPAYER_FK ");
                    strBuilder.Append("   AND AGT.AGENT_MST_PK(+) = QHDR.TARIFF_AGENT_MST_FK ");
                    strBuilder.Append(" AND QHDR.QUOTATION_MST_PK = QTRN.QUOTATION_MST_FK ");
                    strBuilder.Append(" AND QTRN.QUOTE_DTL_PK = QTRNCHRG.QUOTATION_DTL_FK ");

                    strBuilder.Append(" AND QTRN.BASIS=QUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append(" AND QHDR.QUOTATION_MST_PK= " + intQuotationPK);
                    if (intQuotationDtlPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.QUOTE_DTL_PK= " + intQuotationDtlPK);
                    }
                    if (intCarrierPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CARRIER_MST_FK= " + intCarrierPK);
                    }

                    strBuilder.Append("  " + arrCCondition[0] + " ");
                    strBuilder.Append(" GROUP BY QHDR.QUOTATION_MST_PK, QHDR.QUOTATION_REF_NO,QOMT.OPERATOR_ID, ");
                    strBuilder.Append(" QCMT.COMMODITY_NAME, QCMT.COMMODITY_MST_PK, ");
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK, QUOM.DIMENTION_ID, QUOM.DIMENTION_UNIT_MST_PK, QTRN.EXPECTED_WEIGHT, QTRN.EXPECTED_VOLUME,QTRN.COMMODITY_MST_FKS,QTRN.COMMODITY_MST_FK, ");
                    strBuilder.Append("       CU.CUSTOMER_MST_PK,");
                    strBuilder.Append("       CU.CUSTOMER_ID,");
                    strBuilder.Append("       CU.CUSTOMER_NAME,");
                    strBuilder.Append("       AGT.AGENT_MST_PK,");
                    strBuilder.Append("       AGT.AGENT_ID,");
                    strBuilder.Append("       AGT.AGENT_NAME,");
                    strBuilder.Append("       QHDR.COLLECT_AGENT_FLAG");
                    //'
                    if (!string.IsNullOrEmpty(strContainer))
                    {
                        strBuilder.Append(" UNION ");
                        strBuilder.Append("SELECT NULL AS TRNTYPEPK,");
                        strBuilder.Append("       '7' AS TRNTYPESTATUS,");
                        strBuilder.Append("       'Manual' AS CONTRACTTYPE,");
                        strBuilder.Append("       NULL AS REFNO,");
                        strBuilder.Append("       NULL AS OPERATOR,");
                        strBuilder.Append("       OUOM.DIMENTION_ID AS BASIS,");
                        strBuilder.Append("       NULL AS QTY,");
                        strBuilder.Append("       NULL AS CARGO,");
                        strBuilder.Append("       NULL AS COMMODITY,");
                        strBuilder.Append("       NULL AS RATE,");
                        strBuilder.Append("       NULL  AS BKGRATE,");
                        strBuilder.Append("       '' AS NET,");
                        strBuilder.Append("       '' AS TOTALRATE,");
                        strBuilder.Append("       '0' AS SEL,");
                        strBuilder.Append("       '' AS CONTAINERPK,");
                        strBuilder.Append("       NULL AS CARGOPK,");
                        strBuilder.Append("       NULL AS COMMODITYPK,");
                        strBuilder.Append("       NULL AS OPERATORPK,");
                        strBuilder.Append("       '' AS TRANSACTIONPK,");
                        strBuilder.Append("       OUOM.DIMENTION_UNIT_MST_PK AS BASISPK,");
                        strBuilder.Append("       NULL THDFRTPAYER_PK,");
                        strBuilder.Append("       NULL THDFRTPAYER_ID,");
                        strBuilder.Append("       NULL THDFRTPAYER_NAME,");
                        strBuilder.Append("       NULL FOREIGN_AGENT_PK,");
                        strBuilder.Append("       NULL FOREIGN_AGENT_ID,");
                        strBuilder.Append("       NULL FOREIGN_AGENT_NAME,");
                        strBuilder.Append("       0 COLLECT_AGENT_FLAG");
                        strBuilder.Append("  FROM DIMENTION_UNIT_MST_TBL OUOM");
                        strBuilder.Append(" WHERE (1 = 1)");
                        strBuilder.Append("   AND OUOM.DIMENTION_UNIT_MST_PK IN (" + strContainer + ")");
                        strBuilder.Append("   AND OUOM.DIMENTION_UNIT_MST_PK NOT IN");
                        strBuilder.Append("       (SELECT QT.BASIS");
                        strBuilder.Append("          FROM QUOTATION_MST_TBL Q, QUOTATION_DTL_TBL QT");
                        strBuilder.Append("         WHERE Q.QUOTATION_MST_PK = QT.QUOTATION_MST_FK");
                        if (intQuotationDtlPK > 0)
                        {
                            strBuilder.Append(" AND QT.QUOTE_DTL_PK= " + intQuotationDtlPK + "");
                        }
                        if (intCarrierPK > 0)
                        {
                            strBuilder.Append(" AND QT.CARRIER_MST_FK= " + intCarrierPK + "");
                        }
                        strBuilder.Append("           AND Q.QUOTATION_MST_PK = " + intQuotationPK + ")");
                        strBuilder.Append("   AND OUOM.ACTIVE = 1");
                        strBuilder.Append(" GROUP BY OUOM.DIMENTION_ID, OUOM.DIMENTION_UNIT_MST_PK");
                    }
                    //'
                }
                else if (intIsFcl == 1)
                {
                    strBuilder.Append(" SELECT QHDR.QUOTATION_MST_PK AS TRNTYPEPK, ");
                    strBuilder.Append(" '1' AS TRNTYPESTATUS, ");
                    strBuilder.Append(" 'Quote' AS CONTRACTTYPE, ");
                    strBuilder.Append(" QHDR.QUOTATION_REF_NO AS REFNO, ");
                    strBuilder.Append(" QOMT.OPERATOR_ID AS OPERATOR, ");
                    strBuilder.Append(" QCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                    strBuilder.Append(" QTRN.EXPECTED_BOXES AS BOXES, ");
                    strBuilder.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN(' ||");
                    strBuilder.Append("                NVL(QTRN.COMMODITY_MST_FKS, 0) || ')') AS CARGO,");
                    strBuilder.Append(" NULL AS COMMODITY, ");
                    strBuilder.Append(" NULL AS RATE, ");
                    strBuilder.Append(" qtrn.all_in_quoted_tariff AS BKGRATE,");
                    strBuilder.Append(" '' AS NET, ");
                    strBuilder.Append(" qtrn.all_in_quoted_tariff * QTRN.EXPECTED_BOXES AS TOTALRATE, ");
                    strBuilder.Append(" '1' AS SEL, ");
                    strBuilder.Append(" QCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, ");
                    strBuilder.Append("'' AS CARGOPK, ");
                    strBuilder.Append(" CASE WHEN QTRN.COMMODITY_MST_FKS IS NULL THEN TO_CHAR(QTRN.COMMODITY_MST_FK) ELSE QTRN.COMMODITY_MST_FKS END AS COMMODITYPK, ");
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK AS OPERATORPK, ");
                    strBuilder.Append(" '' AS TRANSACTIONPK,NULL AS BASISPK, ");
                    strBuilder.Append("       NVL(CU.CUSTOMER_MST_PK,0) THDFRTPAYER_PK, ");
                    strBuilder.Append("       CU.CUSTOMER_ID THDFRTPAYER_ID, ");
                    strBuilder.Append("       CU.CUSTOMER_NAME THDFRTPAYER_NAME, ");
                    strBuilder.Append("       NVL(AGT.AGENT_MST_PK,0) FOREIGN_AGENT_PK, ");
                    strBuilder.Append("       AGT.AGENT_ID FOREIGN_AGENT_ID, ");
                    strBuilder.Append("       AGT.AGENT_NAME FOREIGN_AGENT_NAME, ");
                    strBuilder.Append("       NVL(QHDR.COLLECT_AGENT_FLAG,0) COLLECT_AGENT_FLAG ");
                    strBuilder.Append("  ");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" QUOTATION_MST_TBL QHDR, ");
                    strBuilder.Append(" QUOTATION_DTL_TBL QTRN, ");
                    strBuilder.Append(" QUOTATION_FREIGHT_TRN QTRNCHRG, ");
                    strBuilder.Append("       CUSTOMER_MST_TBL           CU,");
                    strBuilder.Append("       AGENT_MST_TBL              AGT,");
                    strBuilder.Append(" OPERATOR_MST_TBL QOMT, ");
                    strBuilder.Append(" CONTAINER_TYPE_MST_TBL QCTMT, ");
                    strBuilder.Append(" COMMODITY_MST_TBL QCMT ");
                    strBuilder.Append(" WHERE(1 = 1) ");
                    strBuilder.Append(" AND QTRN.CARRIER_MST_FK=QOMT.OPERATOR_MST_PK(+) ");
                    strBuilder.Append(" AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK(+) ");
                    strBuilder.Append(" AND QHDR.QUOTATION_MST_PK = QTRN.QUOTATION_MST_FK ");
                    strBuilder.Append(" AND QTRN.QUOTE_DTL_PK = QTRNCHRG.QUOTATION_DTL_FK ");
                    strBuilder.Append("  AND CU.CUSTOMER_MST_PK(+) = QHDR.THIRD_PARTY_FRTPAYER_FK ");
                    strBuilder.Append("  AND AGT.AGENT_MST_PK(+) = QHDR.TARIFF_AGENT_MST_FK ");
                    strBuilder.Append(" AND QCTMT.CONTAINER_TYPE_MST_PK=QTRN.CONTAINER_TYPE_MST_FK ");
                    strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append("  " + arrCCondition[0] + " ");
                    strBuilder.Append(" AND QHDR.QUOTATION_MST_PK= " + intQuotationPK);
                    if (intQuotationDtlPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.QUOTE_DTL_PK= " + intQuotationDtlPK);
                    }
                    if (intCarrierPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CARRIER_MST_FK= " + intCarrierPK + "");
                    }
                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CONTAINER_TYPE_MST_FK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN BTRN WHERE BTRN.BOOKING_MST_FK =" + BookingPK + ")");
                    }
                    strBuilder.Append(" GROUP BY QHDR.QUOTATION_MST_PK, qtrn.all_in_tariff, all_in_quoted_tariff,QHDR.QUOTATION_REF_NO,QOMT.OPERATOR_ID, ");
                    strBuilder.Append(" QCTMT.CONTAINER_TYPE_MST_ID, QCMT.COMMODITY_NAME, ");
                    strBuilder.Append(" QCTMT.CONTAINER_TYPE_MST_PK, ");
                    strBuilder.Append(" QCMT.COMMODITY_MST_PK, ");
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK,QTRN.EXPECTED_BOXES,QTRN.COMMODITY_MST_FKS,QTRN.COMMODITY_MST_FK, ");
                    strBuilder.Append("       CU.CUSTOMER_MST_PK,");
                    strBuilder.Append("       CU.CUSTOMER_ID,");
                    strBuilder.Append("       CU.CUSTOMER_NAME,");
                    strBuilder.Append("       AGT.AGENT_MST_PK,");
                    strBuilder.Append("       AGT.AGENT_ID,");
                    strBuilder.Append("       AGT.AGENT_NAME,");
                    strBuilder.Append("       QHDR.COLLECT_AGENT_FLAG");
                    //'
                    if (!string.IsNullOrEmpty(strContainer))
                    {
                        strBuilder.Append(" UNION ");
                        strBuilder.Append(" SELECT NULL AS TRNTYPEPK,");
                        strBuilder.Append("       '7' AS TRNTYPESTATUS,");
                        strBuilder.Append("       'Manual' AS CONTRACTTYPE,");
                        strBuilder.Append("       NULL AS REFNO,");
                        strBuilder.Append("       NULL AS OPERATOR,");
                        strBuilder.Append("       OCTMT.CONTAINER_TYPE_MST_ID AS TYPE,");
                        strBuilder.Append("       NULL AS BOXES,");
                        strBuilder.Append("       NULL AS CARGO,");
                        strBuilder.Append("       NULL AS COMMODITY,");
                        strBuilder.Append("       NULL AS RATE,");
                        strBuilder.Append("       0 AS BKGRATE,");
                        strBuilder.Append("       '' AS NET,");
                        strBuilder.Append("       0 AS TOTALRATE,");
                        strBuilder.Append("       '0' AS SEL,");
                        strBuilder.Append("       OCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK,");
                        strBuilder.Append("       NULL AS CARGOPK,");
                        strBuilder.Append("       NULL AS COMMODITYPK,");
                        strBuilder.Append("       NULL AS OPERATORPK,");
                        strBuilder.Append("       '' AS TRANSACTIONPK,NULL AS BASISPK,");
                        strBuilder.Append("       NULL THDFRTPAYER_PK,");
                        strBuilder.Append("       NULL THDFRTPAYER_ID,");
                        strBuilder.Append("       NULL THDFRTPAYER_NAME,");
                        strBuilder.Append("       NULL FOREIGN_AGENT_PK,");
                        strBuilder.Append("       NULL FOREIGN_AGENT_ID,");
                        strBuilder.Append("       NULL FOREIGN_AGENT_NAME,");
                        strBuilder.Append("       0 COLLECT_AGENT_FLAG");
                        strBuilder.Append("       ");
                        strBuilder.Append("  FROM CONTAINER_TYPE_MST_TBL OCTMT");
                        strBuilder.Append(" WHERE 1 = 1");
                        strBuilder.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK NOT IN");
                        strBuilder.Append("       (SELECT QT.CONTAINER_TYPE_MST_FK");
                        strBuilder.Append("          FROM QUOTATION_MST_TBL Q, QUOTATION_DTL_TBL QT");
                        strBuilder.Append("         WHERE Q.QUOTATION_MST_PK = QT.QUOTATION_MST_FK");
                        if (intQuotationDtlPK > 0)
                        {
                            strBuilder.Append(" AND QT.QUOTE_DTL_PK= " + intQuotationDtlPK + "");
                        }
                        if (intCarrierPK > 0)
                        {
                            strBuilder.Append(" AND QT.CARRIER_MST_FK= " + intCarrierPK + "");
                        }
                        strBuilder.Append("           AND Q.QUOTATION_MST_PK = " + intQuotationPK + ")");
                        strBuilder.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK IN (" + strContainer + ")");
                        strBuilder.Append("   AND OCTMT.ACTIVE_FLAG = 1");
                        strBuilder.Append(" GROUP BY OCTMT.CONTAINER_TYPE_MST_ID, OCTMT.CONTAINER_TYPE_MST_PK");
                    }
                }
                else
                {
                    strBuilder.Append(" SELECT DISTINCT QHDR.QUOTATION_MST_PK AS TRNTYPEPK, ");
                    strBuilder.Append(" '1' AS TRNTYPESTATUS, ");
                    strBuilder.Append(" 'Quote' AS CONTRACTTYPE, ");
                    strBuilder.Append(" QHDR.QUOTATION_REF_NO AS REFNO, ");
                    strBuilder.Append(" QOMT.OPERATOR_ID AS OPERATOR, ");
                    strBuilder.Append("  QCMT.COMMODITY_NAME COMMODITY,");
                    strBuilder.Append("  QTRN.PACK_TYPE_FK PACK_PK,");
                    strBuilder.Append(" PACK.PACK_TYPE_ID PACK_TYPE, ");
                    strBuilder.Append(" QUOM.DIMENTION_ID AS BASIS, ");
                    strBuilder.Append(" case WHEN QUOM.DIMENTION_ID = 'KGS' THEN QTRN.EXPECTED_WEIGHT ");
                    strBuilder.Append(" when QUOM.DIMENTION_ID = 'CBM' THEN QTRN.EXPECTED_VOLUME ");
                    strBuilder.Append(" when QUOM.DIMENTION_ID = 'W/M' then (case when QTRN.EXPECTED_WEIGHT > QTRN.EXPECTED_VOLUME ");
                    strBuilder.Append(" then QTRN.EXPECTED_WEIGHT else QTRN.EXPECTED_VOLUME end) end as QTY,");
                    strBuilder.Append(" QTRN.EXPECTED_WEIGHT CARGO_WT,");
                    strBuilder.Append(" QTRN.EXPECTED_VOLUME CARGO_VOL,");
                    strBuilder.Append("'' AS CARGO, ");
                    strBuilder.Append(" NULL AS RATE, ");
                    strBuilder.Append(" '' AS BKGRATE, ");
                    strBuilder.Append(" '' AS NET, ");
                    strBuilder.Append(" '' AS TOTALRATE, ");
                    strBuilder.Append(" '1' AS SEL, ");
                    strBuilder.Append(" '' AS CONTAINERPK, ");
                    strBuilder.Append(" QTRN.COMMODITY_MST_FK AS COMMODITYPK, ");
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK AS OPERATORPK, ");
                    strBuilder.Append(" '' AS TRANSACTIONPK, ");
                    strBuilder.Append(" QUOM.DIMENTION_UNIT_MST_PK AS BASISPK, ");
                    strBuilder.Append("       NVL(CU.CUSTOMER_MST_PK,0) THDFRTPAYER_PK, ");
                    strBuilder.Append("       CU.CUSTOMER_ID THDFRTPAYER_ID, ");
                    strBuilder.Append("       CU.CUSTOMER_NAME THDFRTPAYER_NAME, ");
                    strBuilder.Append("       NVL(AGT.AGENT_MST_PK,0) FOREIGN_AGENT_PK, ");
                    strBuilder.Append("       AGT.AGENT_ID FOREIGN_AGENT_ID, ");
                    strBuilder.Append("       AGT.AGENT_NAME FOREIGN_AGENT_NAME, ");
                    strBuilder.Append("       NVL(QHDR.COLLECT_AGENT_FLAG,0) COLLECT_AGENT_FLAG ");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" QUOTATION_MST_TBL QHDR, ");
                    strBuilder.Append(" QUOTATION_DTL_TBL QTRN, ");
                    strBuilder.Append(" QUOTATION_FREIGHT_TRN QTRNCHRG, ");
                    strBuilder.Append("       CUSTOMER_MST_TBL           CU,");
                    strBuilder.Append("       AGENT_MST_TBL              AGT,");
                    strBuilder.Append(" OPERATOR_MST_TBL QOMT, ");
                    strBuilder.Append(" COMMODITY_MST_TBL QCMT, ");
                    strBuilder.Append(" DIMENTION_UNIT_MST_TBL QUOM, ");
                    strBuilder.Append(" PACK_TYPE_MST_TBL PACK ");
                    strBuilder.Append(" WHERE(1 = 1) ");
                    strBuilder.Append(" AND QTRN.CARRIER_MST_FK=QOMT.OPERATOR_MST_PK(+)");
                    strBuilder.Append(" AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK(+) ");

                    strBuilder.Append(" AND QHDR.QUOTATION_MST_PK = QTRN.QUOTATION_MST_FK ");
                    strBuilder.Append(" AND QTRN.QUOTE_DTL_PK = QTRNCHRG.QUOTATION_DTL_FK ");
                    strBuilder.Append(" AND QTRN.PACK_TYPE_FK=PACK.PACK_TYPE_MST_PK(+)  ");
                    strBuilder.Append(" AND QTRN.BASIS=QUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append("  AND CU.CUSTOMER_MST_PK(+) = QHDR.THIRD_PARTY_FRTPAYER_FK ");
                    strBuilder.Append("  AND AGT.AGENT_MST_PK(+) = QHDR.TARIFF_AGENT_MST_FK ");
                    strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append(" AND QHDR.QUOTATION_MST_PK= " + intQuotationPK);
                    if (intQuotationDtlPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.QUOTE_DTL_PK= " + intQuotationDtlPK);
                    }
                    if (intCarrierPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CARRIER_MST_FK= " + intCarrierPK);
                    }

                    strBuilder.Append("  " + arrCCondition[0] + " ");
                    strBuilder.Append(" GROUP BY QHDR.QUOTATION_MST_PK, QHDR.QUOTATION_REF_NO,QOMT.OPERATOR_ID, ");
                    strBuilder.Append(" QCMT.COMMODITY_NAME, QCMT.COMMODITY_MST_PK, ");
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK, QUOM.DIMENTION_ID, QUOM.DIMENTION_UNIT_MST_PK, QTRN.EXPECTED_WEIGHT, QTRN.EXPECTED_VOLUME,QTRN.COMMODITY_MST_FKS,QTRN.COMMODITY_MST_FK,QTRN.PACK_TYPE_FK,PACK.PACK_TYPE_ID, ");
                    strBuilder.Append("       CU.CUSTOMER_MST_PK,");
                    strBuilder.Append("       CU.CUSTOMER_ID,");
                    strBuilder.Append("       CU.CUSTOMER_NAME,");
                    strBuilder.Append("       AGT.AGENT_MST_PK,");
                    strBuilder.Append("       AGT.AGENT_ID,");
                    strBuilder.Append("       AGT.AGENT_NAME,");
                    strBuilder.Append("       QHDR.COLLECT_AGENT_FLAG");
                }
                return strBuilder.ToString();
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

        public object funSpotRateHeader(ArrayList arrCCondition, string strCustomer, string intCommodityPK, string strPOL, string strPOD, string intSpotRatePK, string strSDate, Int16 intIsFcl, Int16 intFlag)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    if (intFlag == 1)
                    {
                        strBuilder.Append(" SELECT SRRSRST.RFQ_SPOT_SEA_PK AS TRNTYPEPK, ");
                        strBuilder.Append(" '2' AS TRNTYPESTATUS, ");
                        strBuilder.Append(" 'Sp Rate' AS CONTRACTTYPE, ");
                        strBuilder.Append(" SRRSRST.RFQ_REF_NO AS REFNO, ");
                        strBuilder.Append(" SROMT.OPERATOR_ID AS OPERATOR, ");
                        strBuilder.Append(" SRUOM.DIMENTION_ID AS BASIS,");
                        strBuilder.Append(" '' AS QTY, ");
                        strBuilder.Append(" '' AS CARGO, ");
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, ");
                        strBuilder.Append(" NULL AS RATE, ");
                        strBuilder.Append(" '' AS BKGRATE, ");
                        strBuilder.Append(" '' AS NET, ");
                        strBuilder.Append(" '' AS TOTALRATE, ");
                        strBuilder.Append(" '0' AS SEL, ");
                        strBuilder.Append(" '' AS CONTAINERPK, ");
                        strBuilder.Append(" '' AS CARGOPK, ");
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, ");
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, ");
                        strBuilder.Append(" '' AS TRANSACTIONPK, ");
                        strBuilder.Append(" SRUOM.DIMENTION_UNIT_MST_PK AS BASISPK ");
                        strBuilder.Append(" FROM ");
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, ");
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, ");
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, ");
                        strBuilder.Append(" DIMENTION_UNIT_MST_TBL SRUOM, ");
                        strBuilder.Append(" COMMODITY_GROUP_MST_TBL  SRCOMM, ");
                        strBuilder.Append(" COMMODITY_MST_TBL SRCMT ");
                        strBuilder.Append(" WHERE(1 = 1) ");
                        strBuilder.Append(" AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) ");
                        strBuilder.Append(" AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) ");
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSF.RFQ_SPOT_SEA_FK ");
                        strBuilder.Append(" AND SRRSTSF.LCL_BASIS=SRUOM.DIMENTION_UNIT_MST_PK ");
                        strBuilder.Append(" AND SRRSRST.ACTIVE=1 ");
                        strBuilder.Append(" AND SRRSRST.CARGO_TYPE=2 " + arrCCondition[1]);
                        strBuilder.Append(" AND SRRSRST.APPROVED=1 " + strCustomer);
                        //strBuilder.Append(" AND SRCMT.COMMODITY_GROUP_FK= " & intCommodityPK & vbCrLf)
                        strBuilder.Append(" AND SRCOMM.COMMODITY_GROUP_PK = " + intCommodityPK);
                        strBuilder.Append(" and srcomm.commodity_group_pk(+) = srrsrst.commodity_group_fk ");
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POL_FK= " + strPOL);
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POD_FK= " + strPOD);
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK= " + intSpotRatePK);
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM ");
                        strBuilder.Append(" AND NVL(SRRSRST.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                        strBuilder.Append(" GROUP BY SRRSRST.RFQ_SPOT_SEA_PK,SRRSRST.RFQ_REF_NO,SROMT.OPERATOR_ID, ");
                        strBuilder.Append(" SRCMT.COMMODITY_NAME, SRCMT.COMMODITY_MST_PK, ");
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK,SRUOM.DIMENTION_ID, SRUOM.DIMENTION_UNIT_MST_PK ");
                    }
                    else
                    {
                        strBuilder.Append(" SELECT SRRSRST.RFQ_SPOT_SEA_PK AS TRNTYPEPK, ");
                        strBuilder.Append(" '2' AS TRNTYPESTATUS, ");
                        strBuilder.Append(" 'Sp Rate' AS CONTRACTTYPE, ");
                        strBuilder.Append(" SRRSRST.RFQ_REF_NO AS REFNO, ");
                        strBuilder.Append(" SROMT.OPERATOR_ID AS OPERATOR, ");
                        strBuilder.Append(" SRUOM.DIMENTION_ID AS BASIS,");
                        strBuilder.Append(" '' AS QTY, ");
                        strBuilder.Append(" '' AS CARGO, ");
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, ");
                        strBuilder.Append(" NULL AS RATE, ");
                        strBuilder.Append(" '' AS BKGRATE, ");
                        strBuilder.Append(" '' AS NET, ");
                        strBuilder.Append(" '' AS TOTALRATE, ");
                        strBuilder.Append(" '0' AS SEL, ");
                        strBuilder.Append(" '' AS CONTAINERPK, ");
                        strBuilder.Append(" '' AS CARGOPK, ");
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, ");
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, ");
                        strBuilder.Append(" '' AS TRANSACTIONPK, ");
                        strBuilder.Append(" SRUOM.DIMENTION_UNIT_MST_PK AS BASISPK ");
                        strBuilder.Append(" FROM ");
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, ");
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, ");
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, ");
                        strBuilder.Append(" DIMENTION_UNIT_MST_TBL SRUOM, ");
                        strBuilder.Append(" COMMODITY_GROUP_MST_TBL  SRCOMM, ");
                        strBuilder.Append(" COMMODITY_MST_TBL SRCMT ");
                        strBuilder.Append(" WHERE(1 = 1) ");
                        strBuilder.Append(" AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) ");
                        strBuilder.Append(" AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) ");
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSF.RFQ_SPOT_SEA_FK ");
                        strBuilder.Append(" AND SRRSTSF.LCL_BASIS=SRUOM.DIMENTION_UNIT_MST_PK ");
                        strBuilder.Append(" AND SRRSRST.ACTIVE=1 ");
                        strBuilder.Append(" AND SRRSRST.CARGO_TYPE=2 " + arrCCondition[1]);
                        strBuilder.Append(" AND SRRSRST.APPROVED=1 " + strCustomer);
                        //strBuilder.Append(" AND SRCMT.COMMODITY_GROUP_FK= " & intCommodityPK & vbCrLf)
                        strBuilder.Append(" AND SRCOMM.COMMODITY_GROUP_PK = " + intCommodityPK);
                        strBuilder.Append(" and srcomm.commodity_group_pk(+) = srrsrst.commodity_group_fk ");
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POL_FK= " + strPOL);
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POD_FK= " + strPOD);
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM ");
                        strBuilder.Append(" AND NVL(SRRSRST.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                        strBuilder.Append(" GROUP BY SRRSRST.RFQ_SPOT_SEA_PK,SRRSRST.RFQ_REF_NO,SROMT.OPERATOR_ID, ");
                        strBuilder.Append(" SRCMT.COMMODITY_NAME, SRCMT.COMMODITY_MST_PK, ");
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK,SRUOM.DIMENTION_ID, SRUOM.DIMENTION_UNIT_MST_PK ");
                    }
                }
                else
                {
                    //modifying by thiyagarajan on 23/10/08 to make commodity optional for FCL as per prabhu sugges.
                    if (intFlag == 1)
                    {
                        strBuilder.Append(" SELECT SRRSRST.RFQ_SPOT_SEA_PK AS TRNTYPEPK, ");
                        strBuilder.Append(" '2' AS TRNTYPESTATUS, ");
                        strBuilder.Append(" 'Sp Rate' AS CONTRACTTYPE, ");
                        strBuilder.Append(" SRRSRST.RFQ_REF_NO AS REFNO, ");
                        strBuilder.Append(" SROMT.OPERATOR_ID AS OPERATOR, ");
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                        strBuilder.Append(" '' AS BOXES, ");
                        strBuilder.Append(" '' AS CARGO, ");
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, ");
                        strBuilder.Append(" NULL AS RATE, ");
                        strBuilder.Append(" '' AS BKGRATE, ");
                        strBuilder.Append(" '' AS NET, ");
                        strBuilder.Append(" '' AS TOTALRATE, ");
                        strBuilder.Append(" '0' AS SEL, ");
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, ");
                        strBuilder.Append(" '' AS CARGOPK, ");
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, ");
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, ");
                        strBuilder.Append(" '' AS TRANSACTIONPK, ");
                        strBuilder.Append(" NULL AS BASISPK ");
                        strBuilder.Append(" FROM ");
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, ");
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, ");
                        strBuilder.Append(" CONTAINER_TYPE_MST_TBL SRCTMT, ");
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, ");
                        //Snigdharani - 04/11/2008 - Removing v-array
                        //strBuilder.Append(" TABLE(SRRSTSF.CONTAINER_DTL_FCL) (+) SRRST, " & vbCrLf)
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_CONT_DET SRRST, ");
                        strBuilder.Append(" COMMODITY_GROUP_MST_TBL  SRCOMM, ");
                        strBuilder.Append(" COMMODITY_MST_TBL SRCMT ");
                        strBuilder.Append(" WHERE(1 = 1) ");
                        strBuilder.Append(" AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) ");
                        strBuilder.Append(" AND SRRST.RFQ_SPOT_SEA_TRN_FK=SRRSTSF.RFQ_SPOT_SEA_TRN_PK ");
                        //Snigdharani
                        strBuilder.Append(" AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) ");
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSF.RFQ_SPOT_SEA_FK ");
                        strBuilder.Append(" AND SRCTMT.CONTAINER_TYPE_MST_PK = SRRST.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" AND SRRSRST.ACTIVE=1 ");
                        strBuilder.Append(" AND SRRSRST.CARGO_TYPE=1 " + strCustomer);
                        strBuilder.Append(" AND SRRSRST.APPROVED=1 " + arrCCondition[1]);
                        //strBuilder.Append(" AND SRCMT.COMMODITY_GROUP_FK(+)= " & intCommodityPK & vbCrLf)
                        strBuilder.Append(" AND SRCOMM.COMMODITY_GROUP_PK = " + intCommodityPK);
                        strBuilder.Append(" and srcomm.commodity_group_pk(+) = srrsrst.commodity_group_fk ");
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POL_FK= " + strPOL);
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POD_FK= " + strPOD);
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK= " + intSpotRatePK);
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM ");
                        strBuilder.Append(" AND NVL(SRRSRST.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "'))");
                        strBuilder.Append(" GROUP BY SRRSRST.RFQ_SPOT_SEA_PK,SRRSRST.RFQ_REF_NO,SROMT.OPERATOR_ID, ");
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_ID, SRCMT.COMMODITY_NAME, ");
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK,SRCMT.COMMODITY_MST_PK, ");
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK ");
                    }
                    else
                    {
                        strBuilder.Append(" SELECT SRRSRST.RFQ_SPOT_SEA_PK AS TRNTYPEPK, ");
                        strBuilder.Append(" '2' AS TRNTYPESTATUS, ");
                        strBuilder.Append(" 'Sp Rate' AS CONTRACTTYPE, ");
                        strBuilder.Append(" SRRSRST.RFQ_REF_NO AS REFNO, ");
                        strBuilder.Append(" SROMT.OPERATOR_ID AS OPERATOR, ");
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                        strBuilder.Append(" '' AS BOXES, ");
                        strBuilder.Append(" '' AS CARGO, ");
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, ");
                        strBuilder.Append(" NULL AS RATE, ");
                        strBuilder.Append(" '' AS BKGRATE, ");
                        strBuilder.Append(" '' AS NET, ");
                        strBuilder.Append(" '' AS TOTALRATE, ");
                        strBuilder.Append(" '0' AS SEL, ");
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, ");
                        strBuilder.Append(" '' AS CARGOPK, ");
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, ");
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, ");
                        strBuilder.Append(" '' AS TRANSACTIONPK, ");
                        strBuilder.Append(" NULL AS BASISPK ");
                        strBuilder.Append(" FROM ");
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, ");
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, ");
                        strBuilder.Append(" CONTAINER_TYPE_MST_TBL SRCTMT, ");
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, ");
                        //Snigdharani - 04/11/2008 - Removing v-array
                        //strBuilder.Append(" TABLE(SRRSTSF.CONTAINER_DTL_FCL) (+) SRRST, " & vbCrLf)
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_CONT_DET SRRST, ");
                        strBuilder.Append(" COMMODITY_GROUP_MST_TBL  SRCOMM, ");
                        strBuilder.Append(" COMMODITY_MST_TBL SRCMT ");
                        strBuilder.Append(" WHERE(1 = 1) ");
                        strBuilder.Append(" AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) ");
                        strBuilder.Append(" AND SRRST.RFQ_SPOT_SEA_TRN_FK=SRRSTSF.RFQ_SPOT_SEA_TRN_PK ");
                        //Snigdharani
                        strBuilder.Append(" AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) ");
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSF.RFQ_SPOT_SEA_FK ");
                        strBuilder.Append(" AND SRCTMT.CONTAINER_TYPE_MST_PK = SRRST.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" AND SRRSRST.ACTIVE=1 ");
                        strBuilder.Append(" AND SRRSRST.CARGO_TYPE=1 " + strCustomer);
                        strBuilder.Append(" AND SRRSRST.APPROVED=1 " + arrCCondition[1]);
                        //strBuilder.Append(" AND SRCMT.COMMODITY_GROUP_FK= " & intCommodityPK & vbCrLf)
                        strBuilder.Append(" AND SRCOMM.COMMODITY_GROUP_PK = " + intCommodityPK);
                        strBuilder.Append(" and srcomm.commodity_group_pk(+) = srrsrst.commodity_group_fk ");
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POL_FK= " + strPOL);
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POD_FK= " + strPOD);
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM ");
                        strBuilder.Append(" AND NVL(SRRSRST.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "'))");
                        strBuilder.Append(" GROUP BY SRRSRST.RFQ_SPOT_SEA_PK,SRRSRST.RFQ_REF_NO,SROMT.OPERATOR_ID, ");
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_ID, SRCMT.COMMODITY_NAME, ");
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK,SRCMT.COMMODITY_MST_PK, ");
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK ");
                    }
                }
                return strBuilder.ToString();
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

        public object funCustContHeader(ArrayList arrCCondition, Int32 intCustomerPK, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl, string custcontRefNr = "", int EBKGSTATUS = 0, int BookingPK = 0)
        {
            try
            {
                System.Text.StringBuilder strOperatorRate = new System.Text.StringBuilder();
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strOperatorRate.Append(" ( Select Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0) )                     ");
                    strOperatorRate.Append(" from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx");
                    strOperatorRate.Append(" where mx.ACTIVE                     = 1     AND                         ");
                    strOperatorRate.Append(" mx.CONT_APPROVED              = 1  AND vx.EXCH_RATE_TYPE_FK = 1   AND                         ");
                    strOperatorRate.Append(" mx.CARGO_TYPE                 = 2     AND                         ");
                    strOperatorRate.Append(" mx.OPERATOR_MST_FK            = CCHDR.OPERATOR_MST_FK AND          ");
                    strOperatorRate.Append(" mx.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND ");
                    strOperatorRate.Append(" TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 ");
                    strOperatorRate.Append(" tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)        AND ");
                    strOperatorRate.Append(" tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND ");
                    strOperatorRate.Append(" tx.LCL_BASIS                  = CCTRN.LCL_BASIS               AND ");
                    strOperatorRate.Append(" tx.PORT_MST_POL_FK            = CCTRN.PORT_MST_POL_FK         AND ");
                    strOperatorRate.Append(" tx.PORT_MST_POD_FK            = CCTRN.PORT_MST_POD_FK         AND ");
                    strOperatorRate.Append(" tx.CHECK_FOR_ALL_IN_RT        = 1                             AND ");
                    strOperatorRate.Append(" tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND             ");
                    strOperatorRate.Append(" sysdate between vx.FROM_DATE and vx.TO_DATE )                     ");

                    strBuilder.Append(" SELECT CCHDR.CONT_CUST_SEA_PK AS TRNTYPEPK, ");
                    strBuilder.Append(" '3' AS TRNTYPESTATUS, ");
                    strBuilder.Append(" 'Cust Cont' AS CONTRACTTYPE, ");
                    strBuilder.Append(" CCHDR.CONT_REF_NO AS REFNO, ");
                    strBuilder.Append(" CCOMT.OPERATOR_ID AS OPERATOR, ");
                    strBuilder.Append(" CCUOM.DIMENTION_ID AS BASIS, ");
                    strBuilder.Append(" '' AS QTY, ");
                    strBuilder.Append(" '' AS CARGO, ");
                    strBuilder.Append(" CCCMT.COMMODITY_ID AS COMMODITY, ");
                    strBuilder.Append(" NVL(" + strOperatorRate.ToString() + ", NULL) AS RATE, ");
                    strBuilder.Append(" '' AS BKGRATE, ");
                    strBuilder.Append(" '' AS NET, ");
                    strBuilder.Append(" '' AS TOTALRATE, ");
                    strBuilder.Append(" '0' AS SEL, ");
                    strBuilder.Append(" '' AS CONTAINERPK, ");
                    strBuilder.Append(" '' AS CARGOPK, ");
                    strBuilder.Append(" CCCMT.COMMODITY_MST_PK AS COMMODITYPK, ");
                    strBuilder.Append(" CCOMT.OPERATOR_MST_PK AS OPERATORPK, ");
                    strBuilder.Append(" '' AS TRANSACTIONPK, ");
                    strBuilder.Append(" CCUOM.DIMENTION_UNIT_MST_PK AS BASISPK");
                    strBuilder.Append(" FROM ");
                    strBuilder.Append(" CONT_CUST_SEA_TBL CCHDR, ");
                    strBuilder.Append(" CONT_CUST_TRN_SEA_TBL CCTRN, ");
                    strBuilder.Append(" OPERATOR_MST_TBL CCOMT, ");
                    strBuilder.Append(" DIMENTION_UNIT_MST_TBL CCUOM, ");
                    strBuilder.Append(" COMMODITY_MST_TBL CCCMT ");
                    strBuilder.Append(" WHERE(1 = 1) ");
                    strBuilder.Append(" AND CCHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK(+) ");
                    strBuilder.Append(" AND CCHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)");
                    strBuilder.Append(" AND CCHDR.CONT_CUST_SEA_PK=CCTRN.CONT_CUST_SEA_FK ");
                    strBuilder.Append(" AND CCTRN.LCL_BASIS=CCUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append(" AND CCHDR.STATUS=2 ");
                    strBuilder.Append(" AND CCHDR.CARGO_TYPE=2 " + arrCCondition[2]);
                    strBuilder.Append(" AND (CCHDR.CUSTOMER_MST_FK= " + intCustomerPK);
                    strBuilder.Append("  OR CCHDR.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strBuilder.Append(" FROM AFFILIATE_CUSTOMER_DTLS A, CUSTOMER_MST_TBL C, CONT_CUST_SEA_TBL CT ");
                    strBuilder.Append(" WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strBuilder.Append(" AND A.CUST_MST_FK= " + intCustomerPK);
                    strBuilder.Append(" AND A.CUST_MST_FK=C.CUSTOMER_MST_PK ))");

                    strBuilder.Append(" AND CCHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK);
                    strBuilder.Append(" AND CCTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append(" AND CCTRN.PORT_MST_POD_FK= " + strPOD);
                    if (string.IsNullOrEmpty(custcontRefNr))
                    {
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN CCHDR.VALID_FROM ");
                        strBuilder.Append(" AND NVL(CCHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                    }
                    else
                    {
                        strBuilder.Append(" and CCHDR.CONT_REF_NO = '" + custcontRefNr + "' ");
                    }

                    strBuilder.Append(" GROUP BY CCHDR.CONT_CUST_SEA_PK, CCHDR.CONT_REF_NO,CCOMT.OPERATOR_ID, ");
                    strBuilder.Append(" CCCMT.COMMODITY_ID, CCCMT.COMMODITY_MST_PK, ");
                    strBuilder.Append(" CCOMT.OPERATOR_MST_PK,CCUOM.DIMENTION_ID, CCUOM.DIMENTION_UNIT_MST_PK, ");
                    strBuilder.Append(" CCHDR.OPERATOR_MST_FK, CCTRN.LCL_BASIS, CCTRN.PORT_MST_POL_FK, CCTRN.PORT_MST_POD_FK");
                }
                else
                {
                    strOperatorRate.Append(" ( Select Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0) )                     ");
                    strOperatorRate.Append("   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx,");
                    //Snigdharani - 05/11/2008 - Removing v-array
                    //strOperatorRate.Append("   TABLE(tx.CONTAINER_DTL_FCL) (+) cx                                " & vbCrLf)
                    strOperatorRate.Append("   CONT_TRN_SEA_FCL_RATES cx                                          ");
                    strOperatorRate.Append("   where mx.ACTIVE                     = 1    AND vx.EXCH_RATE_TYPE_FK = 1   AND                         ");
                    strOperatorRate.Append("   mx.CONT_APPROVED              = 1     AND                         ");
                    strOperatorRate.Append("   tx.CONT_TRN_SEA_PK = cx.CONT_TRN_SEA_FK AND                       ");
                    //Snigdharani
                    strOperatorRate.Append("   mx.CARGO_TYPE                 = 1     AND                         ");
                    strOperatorRate.Append("   mx.OPERATOR_MST_FK            = CCHDR.OPERATOR_MST_FK AND         ");
                    strOperatorRate.Append("   mx.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND ");
                    strOperatorRate.Append("   TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                    strOperatorRate.Append("   tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)        AND ");
                    strOperatorRate.Append("   tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND ");
                    strOperatorRate.Append("   cx.CONTAINER_TYPE_MST_FK      = CCTRN.CONTAINER_TYPE_MST_FK   AND ");
                    strOperatorRate.Append("   tx.PORT_MST_POL_FK            = CCTRN.PORT_MST_POL_FK         AND ");
                    strOperatorRate.Append("   tx.PORT_MST_POD_FK            = CCTRN.PORT_MST_POD_FK         AND ");
                    strOperatorRate.Append("   tx.CHECK_FOR_ALL_IN_RT        = 1                             AND ");
                    strOperatorRate.Append("   tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND            ");
                    strOperatorRate.Append("   sysdate between vx.FROM_DATE and vx.TO_DATE )                     ");

                    strBuilder.Append("SELECT CCHDR.CONT_CUST_SEA_PK AS TRNTYPEPK, ");
                    strBuilder.Append("'3'AS TRNTYPESTATUS, ");
                    strBuilder.Append("'Cust Cont' AS CONTRACTTYPE, ");
                    strBuilder.Append("CCHDR.CONT_REF_NO AS REFNO, ");
                    strBuilder.Append("CCOMT.OPERATOR_ID AS OPERATOR, ");
                    strBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                    strBuilder.Append("'' AS BOXES, ");
                    strBuilder.Append(" '' AS CARGO, ");
                    strBuilder.Append("CCCMT.COMMODITY_ID AS COMMODITY, ");
                    strBuilder.Append("NVL(" + strOperatorRate.ToString() + ", NULL) AS RATE, ");
                    strBuilder.Append("'' AS BKGRATE, ");
                    strBuilder.Append("'' AS NET, ");
                    strBuilder.Append("'' AS TOTALRATE, ");
                    strBuilder.Append("'0' AS SEL, ");
                    strBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, ");
                    strBuilder.Append(" '' AS CARGOPK, ");
                    strBuilder.Append("CCCMT.COMMODITY_MST_PK AS COMMODITYPK, ");
                    strBuilder.Append("CCOMT.OPERATOR_MST_PK AS OPERATORPK, ");
                    strBuilder.Append("'' AS TRANSACTIONPK, ");
                    strBuilder.Append("NULL AS BASISPK ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("CONT_CUST_SEA_TBL CCHDR, ");
                    strBuilder.Append("CONT_CUST_TRN_SEA_TBL CCTRN, ");
                    //strBuilder.Append("CONT_SUR_CHRG_SEA_TBL CCTRNCHRG, " & vbCrLf)
                    strBuilder.Append("OPERATOR_MST_TBL CCOMT, ");
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL CCCTMT, ");
                    strBuilder.Append("COMMODITY_MST_TBL CCCMT ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND CCHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK(+) ");
                    strBuilder.Append("AND CCHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) ");
                    strBuilder.Append("AND CCHDR.CONT_CUST_SEA_PK=CCTRN.CONT_CUST_SEA_FK ");
                    //strBuilder.Append("AND CCTRN.CONT_CUST_TRN_SEA_PK=CCTRNCHRG.CONT_CUST_TRN_SEA_FK " & vbCrLf)
                    strBuilder.Append("AND CCCTMT.CONTAINER_TYPE_MST_PK = CCTRN.CONTAINER_TYPE_MST_FK ");
                    strBuilder.Append("AND CCHDR.STATUS=2 ");
                    strBuilder.Append("AND CCHDR.CARGO_TYPE=1 " + arrCCondition[2]);
                    strBuilder.Append("AND (CCHDR.CUSTOMER_MST_FK= " + intCustomerPK);
                    strBuilder.Append("  OR CCHDR.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strBuilder.Append(" FROM AFFILIATE_CUSTOMER_DTLS A, CUSTOMER_MST_TBL C, CONT_CUST_SEA_TBL CT ");
                    strBuilder.Append(" WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strBuilder.Append(" AND A.CUST_MST_FK= " + intCustomerPK);
                    strBuilder.Append(" AND A.CUST_MST_FK=C.CUSTOMER_MST_PK ))");
                    strBuilder.Append("AND CCHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK);
                    strBuilder.Append("AND CCTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND CCTRN.PORT_MST_POD_FK= " + strPOD);

                    //strBuilder.Append("AND TO_DATE('" & strSDate & "','" & dateFormat & "') BETWEEN CCHDR.VALID_FROM " & vbCrLf)
                    //strBuilder.Append("AND NVL(CCHDR.VALID_TO,TO_DATE('" & strSDate & "','" & dateFormat & "')) " & vbCrLf)
                    if (string.IsNullOrEmpty(custcontRefNr))
                    {
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN CCHDR.VALID_FROM ");
                        strBuilder.Append("AND NVL(CCHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                    }
                    else
                    {
                        strBuilder.Append(" and CCHDR.CONT_REF_NO = '" + custcontRefNr + "' ");
                    }
                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND CCCTMT.CONTAINER_TYPE_MST_PK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN BTRN WHERE BTRN.BOOKING_MST_FK =" + BookingPK + ")");
                    }
                    strBuilder.Append("GROUP BY CCHDR.CONT_CUST_SEA_PK, CCHDR.CONT_REF_NO,CCOMT.OPERATOR_ID, ");
                    strBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_ID, CCCMT.COMMODITY_ID, ");
                    strBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_PK, ");
                    strBuilder.Append("CCCMT.COMMODITY_MST_PK , CCOMT.OPERATOR_MST_PK, ");
                    strBuilder.Append("CCHDR.OPERATOR_MST_FK, CCTRN.CONTAINER_TYPE_MST_FK, ");
                    strBuilder.Append("CCTRN.PORT_MST_POL_FK, CCTRN.PORT_MST_POD_FK ");
                }
                return strBuilder.ToString();
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

        public object funSRRHeader(ArrayList arrCCondition, Int32 intCustomerPK, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl)
        {
            try
            {
                string strOperatorRate = null;
                System.Text.StringBuilder strSRRSBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strOperatorRate = " ( Select Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0) )                     " + "   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx" + "   where mx.ACTIVE                     = 1     AND                         " + "   mx.CONT_APPROVED              = 1   AND vx.EXCH_RATE_TYPE_FK = 1    AND                         " + "   mx.CARGO_TYPE                 = 2     AND                         " + "   mx.OPERATOR_MST_FK            = SRRHDR.OPERATOR_MST_FK AND          " + "   mx.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " + "   TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " + "   tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)         AND " + "   tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK            AND " + "   tx.LCL_BASIS                  = SRRTRN.LCL_BASIS               AND " + "   tx.PORT_MST_POL_FK            = SRRTRN.PORT_MST_POL_FK         AND " + "   tx.PORT_MST_POD_FK            = SRRTRN.PORT_MST_POD_FK         AND " + "   tx.CHECK_FOR_ALL_IN_RT        = 1                             AND " + "   tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND             " + "   sysdate between vx.FROM_DATE and vx.TO_DATE )                     ";

                    strSRRSBuilder.Append("SELECT SRRTRN.SRR_TRN_SEA_PK AS TRNTYPEPK, ");
                    strSRRSBuilder.Append("'5' AS TRNTYPESTATUS, ");
                    strSRRSBuilder.Append("'SRR' AS CONTRACTTYPE, ");
                    strSRRSBuilder.Append("SRRHDR.SRR_REF_NO AS REFNO, ");
                    strSRRSBuilder.Append("CCOMT.OPERATOR_ID AS OPERATOR, ");
                    strSRRSBuilder.Append("SRRUOM.DIMENTION_ID AS BASIS, ");
                    strSRRSBuilder.Append("'' AS QTY, ");
                    strSRRSBuilder.Append("'' AS CARGO, ");
                    strSRRSBuilder.Append("CCCMT.COMMODITY_ID AS COMMODITY, ");
                    strSRRSBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, ");
                    strSRRSBuilder.Append("'' AS BKGRATE, ");
                    strSRRSBuilder.Append("'' AS NET, ");
                    strSRRSBuilder.Append("'' AS TOTALRATE, ");
                    strSRRSBuilder.Append("'0' AS SEL, ");
                    strSRRSBuilder.Append("'' AS CONTAINERPK, ");
                    strSRRSBuilder.Append("'' AS CARGOPK, ");
                    strSRRSBuilder.Append("SRRHDR.COMMODITY_MST_FK AS COMMODITYPK, ");
                    strSRRSBuilder.Append("SRRHDR.OPERATOR_MST_FK AS OPERATORPK, ");
                    strSRRSBuilder.Append("'' AS TRANSACTIONPK, ");
                    strSRRSBuilder.Append("SRRTRN.LCL_BASIS AS BASISPK ");
                    strSRRSBuilder.Append("FROM ");
                    strSRRSBuilder.Append("SRR_SEA_TBL SRRHDR, ");
                    strSRRSBuilder.Append("SRR_TRN_SEA_TBL SRRTRN, ");
                    strSRRSBuilder.Append("OPERATOR_MST_TBL CCOMT, ");
                    strSRRSBuilder.Append("DIMENTION_UNIT_MST_TBL SRRUOM, ");
                    strSRRSBuilder.Append("COMMODITY_MST_TBL CCCMT ");
                    strSRRSBuilder.Append("WHERE(1 = 1) ");
                    strSRRSBuilder.Append("AND SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK ");
                    strSRRSBuilder.Append("AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK(+) ");
                    strSRRSBuilder.Append("AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) ");
                    strSRRSBuilder.Append("AND SRRTRN.LCL_BASIS=SRRUOM.DIMENTION_UNIT_MST_PK(+) ");
                    strSRRSBuilder.Append("AND SRRHDR.STATUS=1 ");
                    strSRRSBuilder.Append("AND SRRHDR.CARGO_TYPE=2 ");
                    strSRRSBuilder.Append("AND (SRRHDR.CUSTOMER_MST_FK= " + intCustomerPK + " ");
                    strSRRSBuilder.Append(" OR   SRRHDR.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSRRSBuilder.Append(" FROM AFFILIATE_CUSTOMER_DTLS A, SRR_SEA_TBL CT , CUSTOMER_MST_TBL C ");
                    strSRRSBuilder.Append(" WHERE A.REFERENCE_MST_FK = CT.SRR_SEA_PK ");
                    strSRRSBuilder.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSRRSBuilder.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");

                    strSRRSBuilder.Append("AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK + " ");
                    strSRRSBuilder.Append("AND SRRTRN.PORT_MST_POL_FK= " + strPOL + " ");
                    strSRRSBuilder.Append("AND SRRTRN.PORT_MST_POD_FK= " + strPOD + arrCCondition[4] + " ");
                    strSRRSBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRHDR.VALID_FROM ");
                    strSRRSBuilder.Append("AND NVL(SRRHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                    strSRRSBuilder.Append("GROUP BY ");
                    strSRRSBuilder.Append("SRRTRN.SRR_TRN_SEA_PK , SRRHDR.SRR_REF_NO,CCOMT.OPERATOR_ID, ");
                    strSRRSBuilder.Append("SRRUOM.DIMENTION_ID, CCCMT.COMMODITY_ID, ");
                    strSRRSBuilder.Append("SRRTRN.LCL_BASIS, SRRHDR.COMMODITY_MST_FK, ");
                    strSRRSBuilder.Append("SRRHDR.OPERATOR_MST_FK, SRRTRN.PORT_MST_POL_FK, SRRTRN.PORT_MST_POD_FK ");
                }
                else
                {
                    strOperatorRate = " ( Select Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0) )                     " + "   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx,";
                    // & vbCrLf & _
                    //Snigdharani - 05/11/2008 - Removing v-array
                    strOperatorRate = strOperatorRate + "CONT_TRN_SEA_FCL_RATES cx                                " + "   where mx.ACTIVE                     = 1    AND vx.EXCH_RATE_TYPE_FK = 1   AND                         " + "         mx.CONT_APPROVED              = 1     AND                         " + "         tx.CONT_TRN_SEA_PK = cx.CONT_TRN_SEA_FK AND                       " + "         mx.CARGO_TYPE                 = 1     AND                         " + "         mx.OPERATOR_MST_FK            = SRRHDR.OPERATOR_MST_FK AND         " + "         mx.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " + "         TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " + "           tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)        AND " + "         tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND " + "         cx.CONTAINER_TYPE_MST_FK      = SRRTRN.CONTAINER_TYPE_MST_FK   AND " + "         tx.PORT_MST_POL_FK            = SRRTRN.PORT_MST_POL_FK         AND " + "         tx.PORT_MST_POD_FK            = SRRTRN.PORT_MST_POD_FK         AND " + "         tx.CHECK_FOR_ALL_IN_RT        = 1                             AND " + "         tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND            " + "         sysdate between vx.FROM_DATE and vx.TO_DATE )                     ";

                    strSRRSBuilder.Append("SELECT SRRTRN.SRR_TRN_SEA_PK AS TRNTYPEPK, ");
                    strSRRSBuilder.Append("'5' AS TRNTYPESTATUS, ");
                    strSRRSBuilder.Append("'SRR' AS CONTRACTTYPE, ");
                    strSRRSBuilder.Append("SRRHDR.SRR_REF_NO AS REFNO, ");
                    strSRRSBuilder.Append("CCOMT.OPERATOR_ID AS OPERATOR, ");
                    strSRRSBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                    strSRRSBuilder.Append("'' AS BOXES, ");
                    strSRRSBuilder.Append("'' AS CARGO, ");
                    strSRRSBuilder.Append("CCCMT.COMMODITY_ID AS COMMODITY, ");
                    strSRRSBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, ");
                    strSRRSBuilder.Append("'' AS BKGRATE, ");
                    strSRRSBuilder.Append("'' AS NET, ");
                    strSRRSBuilder.Append("'' AS TOTALRATE, ");
                    strSRRSBuilder.Append("'0' AS SEL, ");
                    strSRRSBuilder.Append("SRRTRN.CONTAINER_TYPE_MST_FK AS CONTAINERPK, ");
                    strSRRSBuilder.Append("'' AS CARGOPK, ");
                    strSRRSBuilder.Append("SRRHDR.COMMODITY_MST_FK AS COMMODITYPK, ");
                    strSRRSBuilder.Append("SRRHDR.OPERATOR_MST_FK AS OPERATORPK, ");
                    strSRRSBuilder.Append("'' AS TRANSACTIONPK, ");
                    strSRRSBuilder.Append("NULL AS BASISPK ");
                    strSRRSBuilder.Append("FROM ");
                    strSRRSBuilder.Append("SRR_SEA_TBL SRRHDR, ");
                    strSRRSBuilder.Append("SRR_TRN_SEA_TBL SRRTRN, ");
                    strSRRSBuilder.Append("OPERATOR_MST_TBL CCOMT, ");
                    strSRRSBuilder.Append("CONTAINER_TYPE_MST_TBL CCCTMT, ");
                    strSRRSBuilder.Append("COMMODITY_MST_TBL CCCMT ");
                    strSRRSBuilder.Append("WHERE(1 = 1) ");
                    strSRRSBuilder.Append("AND SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK ");
                    strSRRSBuilder.Append("AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)");
                    strSRRSBuilder.Append("AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) ");
                    strSRRSBuilder.Append("AND SRRTRN.CONTAINER_TYPE_MST_FK = CCCTMT.CONTAINER_TYPE_MST_PK(+) ");
                    strSRRSBuilder.Append("AND SRRHDR.STATUS=1 ");
                    strSRRSBuilder.Append("AND SRRHDR.CARGO_TYPE=1 ");
                    strSRRSBuilder.Append("AND (SRRHDR.CUSTOMER_MST_FK= " + intCustomerPK + " ");
                    strSRRSBuilder.Append(" OR   SRRHDR.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSRRSBuilder.Append(" FROM AFFILIATE_CUSTOMER_DTLS A, SRR_SEA_TBL CT , CUSTOMER_MST_TBL C ");
                    strSRRSBuilder.Append(" WHERE A.REFERENCE_MST_FK = CT.SRR_SEA_PK ");
                    strSRRSBuilder.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSRRSBuilder.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strSRRSBuilder.Append("AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK + " ");
                    strSRRSBuilder.Append("AND SRRTRN.PORT_MST_POL_FK= " + strPOL + " ");
                    strSRRSBuilder.Append("AND SRRTRN.PORT_MST_POD_FK= " + strPOD + arrCCondition[4] + " ");
                    strSRRSBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRHDR.VALID_FROM ");
                    strSRRSBuilder.Append("AND NVL(SRRHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                    strSRRSBuilder.Append("GROUP BY ");
                    strSRRSBuilder.Append("SRRTRN.SRR_TRN_SEA_PK , SRRHDR.SRR_REF_NO,CCOMT.OPERATOR_ID, ");
                    strSRRSBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_ID, CCCMT.COMMODITY_ID, ");
                    strSRRSBuilder.Append("SRRTRN.CONTAINER_TYPE_MST_FK, SRRHDR.COMMODITY_MST_FK, ");
                    strSRRSBuilder.Append("SRRHDR.OPERATOR_MST_FK, SRRTRN.PORT_MST_POL_FK, SRRTRN.PORT_MST_POD_FK ");
                }
                return strSRRSBuilder.ToString();
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

        public object funSLTariffHeader(ArrayList arrCCondition, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl, short isAgentType = 0, int DPAgentPK = 0)
        {
            try
            {
                string strOperatorRate = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strOperatorRate = " ( Select Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0) )                     " + "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v  " + "   where m.ACTIVE                     = 1  AND v.EXCH_RATE_TYPE_FK = 1     AND                         " + "         m.CONT_APPROVED              = 1     AND                         " + "         m.CARGO_TYPE                 = 2     AND                         " + "         m.OPERATOR_MST_FK            = OHDR.OPERATOR_MST_FK AND          " + "         m.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " + "         TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " + "         t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " + "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " + "         t.LCL_BASIS                  = OTRN.LCL_BASIS               AND " + "         t.PORT_MST_POL_FK            = OTRN.PORT_MST_POL_FK          AND " + "         t.PORT_MST_POD_FK            = OTRN.PORT_MST_POD_FK         AND " + "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " + "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " + "         sysdate between v.FROM_DATE and v.TO_DATE )                     ";

                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, ");
                    if (isAgentType == 1)
                    {
                        strBuilder.Append("'8' AS TRNTYPESTATUS, ");
                        strBuilder.Append("'Agent Tariff' AS CONTRACTTYPE, ");
                    }
                    else
                    {
                        strBuilder.Append("'4' AS TRNTYPESTATUS, ");
                        strBuilder.Append("'SL Tariff' AS CONTRACTTYPE, ");
                    }
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, ");
                    strBuilder.Append("OOMT.OPERATOR_ID AS OPERATOR, ");
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, ");
                    strBuilder.Append("'' AS QTY, ");
                    strBuilder.Append("'' AS CARGO, ");
                    strBuilder.Append("NULL AS COMMODITY, ");
                    strBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, ");
                    strBuilder.Append("'' AS BKGRATE, ");
                    strBuilder.Append("'' AS NET, ");
                    strBuilder.Append("'' AS TOTALRATE, ");
                    strBuilder.Append("'0' AS SEL, ");
                    strBuilder.Append("'' AS CONTAINERPK, ");
                    strBuilder.Append("'' AS CARGOPK, ");
                    strBuilder.Append("NULL AS COMMODITYPK, ");
                    strBuilder.Append("OOMT.OPERATOR_MST_PK AS OPERATORPK, ");
                    strBuilder.Append("'' AS TRANSACTIONPK, ");
                    strBuilder.Append("OUOM.DIMENTION_UNIT_MST_PK AS BASISPK ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, ");
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, ");
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, ");
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL OUOM ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND OHDR.OPERATOR_MST_FK=OOMT.OPERATOR_MST_PK(+) ");
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK ");
                    strBuilder.Append("AND OTRN.LCL_BASIS=OUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append("AND OHDR.ACTIVE=1 ");
                    strBuilder.Append("AND OHDR.STATUS=1 ");
                    strBuilder.Append("AND OHDR.CARGO_TYPE=2 " + arrCCondition[3]);
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK);
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD);
                    if (isAgentType == 1 & DPAgentPK > 0)
                    {
                        strBuilder.AppendLine("AND OHDR.AGENT_MST_FK= " + DPAgentPK);
                        strBuilder.AppendLine("AND OHDR.TARIFF_TYPE= 3");
                        //AGENT TARIFF
                    }
                    else
                    {
                        strBuilder.Append("AND OHDR.TARIFF_TYPE=1 ");
                    }
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM ");
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE(' " + strSDate + " ','" + dateFormat + "')) ");
                    strBuilder.Append("GROUP BY OHDR.TARIFF_MAIN_SEA_PK, OHDR.TARIFF_REF_NO, OOMT.OPERATOR_ID, ");
                    strBuilder.Append("OOMT.OPERATOR_MST_PK, OUOM.DIMENTION_ID, OUOM.DIMENTION_UNIT_MST_PK, ");
                    strBuilder.Append("OHDR.OPERATOR_MST_FK ,OTRN.LCL_BASIS, OTRN.PORT_MST_POL_FK, OTRN.PORT_MST_POD_FK");
                }
                else
                {
                    strOperatorRate = " ( Select Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0) )                     " + "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v,  ";
                    // & vbCrLf & _
                    //Snigdharani - 05/11/2008 - Removing v-array
                    strOperatorRate = strOperatorRate + "CONT_TRN_SEA_FCL_RATES c                                 " + "   where m.ACTIVE                     = 1   AND v.EXCH_RATE_TYPE_FK = 1    AND                         " + "         m.CONT_APPROVED              = 1     AND                         " + "         m.CARGO_TYPE                 = 1     AND                         " + "         t.CONT_TRN_SEA_PK = c.CONT_TRN_SEA_FK AND                        " + "         m.OPERATOR_MST_FK            = OHDR.OPERATOR_MST_FK AND         " + "         m.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " + "         TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " + "           t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " + "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " + "         c.CONTAINER_TYPE_MST_FK      = OTRNCONT.CONTAINER_TYPE_MST_FK   AND " + "         t.PORT_MST_POL_FK            = OTRN.PORT_MST_POL_FK         AND " + "         t.PORT_MST_POD_FK            = OTRN.PORT_MST_POD_FK         AND " + "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " + "         V.CURRENCY_MST_BASE_FK       = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " AND " + "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " + "         sysdate between v.FROM_DATE and v.TO_DATE )                     ";

                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, ");
                    if (isAgentType == 1)
                    {
                        strBuilder.Append("'8' AS TRNTYPESTATUS, ");
                        strBuilder.Append("'Agent Tariff' AS CONTRACTTYPE, ");
                    }
                    else
                    {
                        strBuilder.Append("'4' AS TRNTYPESTATUS, ");
                        strBuilder.Append("'SL Tariff' AS CONTRACTTYPE, ");
                    }
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, ");
                    strBuilder.Append("OOMT.OPERATOR_ID AS OPERATOR, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                    strBuilder.Append("'' AS BOXES, ");
                    strBuilder.Append("'' AS CARGO, ");
                    strBuilder.Append("NULL AS COMMODITY, ");
                    strBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, ");
                    strBuilder.Append("'' AS BKGRATE, ");
                    strBuilder.Append("'' AS NET, ");
                    strBuilder.Append("'' AS TOTALRATE, ");
                    strBuilder.Append("'0' AS SEL, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, ");
                    strBuilder.Append("'' AS CARGOPK, ");
                    strBuilder.Append("NULL AS COMMODITYPK, ");
                    strBuilder.Append("OOMT.OPERATOR_MST_PK AS OPERATORPK, ");
                    strBuilder.Append("'' AS TRANSACTIONPK, ");
                    strBuilder.Append("NULL AS BASISPK ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, ");
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, ");
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //strBuilder.Append("TABLE(OTRN.CONTAINER_DTL_FCL) (+) OTRNCONT, " & vbCrLf)
                    strBuilder.Append("TARIFF_TRN_SEA_CONT_DTL OTRNCONT, ");
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, ");
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL OCTMT ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND OHDR.OPERATOR_MST_FK=OOMT.OPERATOR_MST_PK(+) ");
                    strBuilder.Append("AND OTRNCONT.TARIFF_TRN_SEA_FK = OTRN.TARIFF_TRN_SEA_PK ");
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK ");
                    strBuilder.Append("AND OCTMT.CONTAINER_TYPE_MST_PK=OTRNCONT.CONTAINER_TYPE_MST_FK ");
                    strBuilder.Append("AND OHDR.ACTIVE=1 ");
                    strBuilder.Append("AND OHDR.STATUS=1 ");
                    strBuilder.Append("AND OHDR.CARGO_TYPE=1 " + arrCCondition[3]);
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK);
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD);
                    if (isAgentType == 1 & DPAgentPK > 0)
                    {
                        strBuilder.AppendLine("AND OHDR.TARIFF_TYPE= 3");
                        //AGENT TARIFF
                        strBuilder.AppendLine("AND OHDR.AGENT_MST_FK= " + DPAgentPK);
                    }
                    else
                    {
                        strBuilder.Append("AND OHDR.TARIFF_TYPE=1 ");
                    }

                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM ");
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE(' " + strSDate + " ','" + dateFormat + "')) ");
                    strBuilder.Append("GROUP BY OHDR.TARIFF_MAIN_SEA_PK, OHDR.TARIFF_REF_NO, OOMT.OPERATOR_ID, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK, ");
                    strBuilder.Append("OOMT.OPERATOR_MST_PK, ");
                    strBuilder.Append("OTRNCONT.CONTAINER_TYPE_MST_FK, ");
                    strBuilder.Append("OTRN.PORT_MST_POL_FK, ");
                    strBuilder.Append("OTRN.PORT_MST_POD_FK, ");
                    strBuilder.Append("OHDR.OPERATOR_MST_FK");
                }
                return strBuilder.ToString();
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

        public object funGTariffHeader(ArrayList arrCCondition, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, ");
                    strBuilder.Append("'6' AS TRNTYPESTATUS, ");
                    strBuilder.Append("'Gen Tariff' AS CONTRACTTYPE, ");
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, ");
                    strBuilder.Append("'General' AS OPERATOR, ");
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, ");
                    strBuilder.Append("'' AS QTY, ");
                    strBuilder.Append("'' AS CARGO, ");
                    strBuilder.Append("NULL AS COMMODITY, ");
                    strBuilder.Append("NULL AS RATE, ");
                    strBuilder.Append("'' AS BKGRATE, ");
                    strBuilder.Append("'' AS NET, ");
                    strBuilder.Append("'' AS TOTALRATE, ");
                    strBuilder.Append("'0' AS SEL, ");
                    strBuilder.Append("'' AS CONTAINERPK, ");
                    strBuilder.Append("'' AS CARGOPK, ");
                    strBuilder.Append("NULL AS COMMODITYPK, ");
                    strBuilder.Append("NULL AS OPERATORPK, ");
                    strBuilder.Append("'' AS TRANSACTIONPK, ");
                    strBuilder.Append("OUOM.DIMENTION_UNIT_MST_PK AS BASISPK ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, ");
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, ");
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL OUOM ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK ");
                    strBuilder.Append("AND OTRN.LCL_BASIS=OUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append("AND OHDR.ACTIVE=1 ");
                    strBuilder.Append("AND OHDR.STATUS=1 ");
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=2 ");
                    strBuilder.Append("AND OHDR.CARGO_TYPE=2 " + arrCCondition[3]);
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK);
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM ");
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE(' " + strSDate + " ','" + dateFormat + "')) ");
                    strBuilder.Append("GROUP BY OHDR.TARIFF_MAIN_SEA_PK, OHDR.TARIFF_REF_NO, ");
                    strBuilder.Append("OUOM.DIMENTION_ID, OUOM.DIMENTION_UNIT_MST_PK, ");
                    strBuilder.Append("OTRN.LCL_BASIS, OTRN.PORT_MST_POL_FK, OTRN.PORT_MST_POD_FK");
                }
                else
                {
                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, ");
                    strBuilder.Append("'6' AS TRNTYPESTATUS, ");
                    strBuilder.Append("'Gen Tariff' AS CONTRACTTYPE, ");
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, ");
                    strBuilder.Append("'General' AS OPERATOR, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                    strBuilder.Append("'' AS BOXES, ");
                    strBuilder.Append("'' AS CARGO, ");
                    strBuilder.Append("NULL AS COMMODITY, ");
                    strBuilder.Append("NULL AS RATE, ");
                    strBuilder.Append("'' AS BKGRATE, ");
                    strBuilder.Append("'' AS NET, ");
                    strBuilder.Append("'' AS TOTALRATE, ");
                    strBuilder.Append("'0' AS SEL, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, ");
                    strBuilder.Append("'' AS CARGOPK, ");
                    strBuilder.Append("NULL AS COMMODITYPK, ");
                    strBuilder.Append("NULL AS OPERATORPK, ");
                    strBuilder.Append("'' AS TRANSACTIONPK, ");
                    strBuilder.Append("NULL AS BASISPK ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, ");
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, ");
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //strBuilder.Append("TABLE(OTRN.CONTAINER_DTL_FCL) (+) OTRNCONT, " & vbCrLf)
                    strBuilder.Append("TARIFF_TRN_SEA_CONT_DTL OTRNCONT, ");
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL OCTMT ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK ");
                    strBuilder.Append("AND OTRNCONT.TARIFF_TRN_SEA_FK = OTRN.TARIFF_TRN_SEA_PK ");
                    strBuilder.Append("AND OCTMT.CONTAINER_TYPE_MST_PK=OTRNCONT.CONTAINER_TYPE_MST_FK ");
                    strBuilder.Append("AND OHDR.ACTIVE=1 ");
                    strBuilder.Append("AND OHDR.STATUS=1 ");
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=2 ");
                    strBuilder.Append("AND OHDR.CARGO_TYPE=1 " + arrCCondition[3]);
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK);
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM ");
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE(' " + strSDate + " ','" + dateFormat + "')) ");
                    strBuilder.Append("GROUP BY OHDR.TARIFF_MAIN_SEA_PK, OHDR.TARIFF_REF_NO, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK, ");
                    strBuilder.Append("OTRNCONT.CONTAINER_TYPE_MST_FK, ");
                    strBuilder.Append("OTRN.PORT_MST_POL_FK, ");
                    strBuilder.Append("OTRN.PORT_MST_POD_FK ");
                }
                return strBuilder.ToString();
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

        #endregion "Fetch Header"

        #region "Fetch Freight"

        public DataTable FetchFreight(Int32 intQuotationPK = 0, Int16 intIsFcl = 0, Int32 intCustomerPK = 0, string strPOL = "", string strPOD = "", Int16 intCommodityPk = 0, string strSDate = "", string strContainer = "", short intSRateStatus = 0, short intCContractStatus = 0,
        short intOTariffStatus = 0, short intGTariffStatus = 0, short intSRRContractStatus = 0, Int32 intSpotRatePK = 0, string CustContRefNr = "", int EBKGSTATUS = 0, int BookingPK = 0, int intQuotationDtlPK = 0, int intCarrierPK = 0, short intAgentTariffStatus = 0,
        int DPAgentPK = 0)
        {
            string strSql = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            string strContCondition = null;
            ArrayList arrCCondition = new ArrayList();
            string strContRefNo = null;
            string strFreightElements = null;
            string strSurcharge = null;
            string strContSectors = null;
            string strCustomer = null;
            if (strContainer.Length == 0 | strContainer.ToUpper() == "N" | strContainer.ToUpper() == "UNDEFINED")
            {
                arrCCondition.Add("");
                arrCCondition.Add("");
                arrCCondition.Add("");
                arrCCondition.Add("");
                arrCCondition.Add("");
                arrCCondition.Add("");
                arrCCondition.Add("");
            }
            else
            {
                if (intIsFcl == 2)
                {
                    arrCCondition.Add("AND QTRN.BASIS IN(" + strContainer + ") ");
                    arrCCondition.Add("AND SRRSTSFL.LCL_BASIS in(" + strContainer + ") ");
                    arrCCondition.Add("AND tran10.LCL_BASIS IN(" + strContainer + ") ");
                    arrCCondition.Add("AND tran2.LCL_BASIS IN(" + strContainer + ") ");
                    arrCCondition.Add("AND tran6.LCL_BASIS IN(" + strContainer + ") ");
                    arrCCondition.Add("AND OTRN.LCL_BASIS IN(" + strContainer + ") ");
                    arrCCondition.Add("AND SRRTRN.LCL_BASIS IN(" + strContainer + ") ");
                }
                else
                {
                    arrCCondition.Add("AND QCTMT.CONTAINER_TYPE_MST_PK IN(" + strContainer + ") ");
                    arrCCondition.Add("AND SRCTMT.CONTAINER_TYPE_MST_PK IN(" + strContainer + ") ");
                    arrCCondition.Add("AND tran10.CONTAINER_TYPE_MST_FK IN(" + strContainer + ") ");
                    arrCCondition.Add("AND tran2.CONTAINER_TYPE_MST_FK IN(" + strContainer + ") ");
                    arrCCondition.Add("AND cont6.CONTAINER_TYPE_MST_FK IN(" + strContainer + ") ");
                    arrCCondition.Add("AND OCTMT.CONTAINER_TYPE_MST_PK IN(" + strContainer + ") ");
                    arrCCondition.Add("AND SRRTRN.CONTAINER_TYPE_MST_FK IN (" + strContainer + ") ");
                }
            }

            if (intCustomerPK == 0)
            {
                strCustomer = " ";
            }
            else
            {
                strCustomer = " AND (SRRSRST.CUSTOMER_MST_FK= " + intCustomerPK + " OR SRRSRST.CUSTOMER_MST_FK IS NULL";
                strCustomer = strCustomer + "     OR   SRRSRST.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ";
                strCustomer = strCustomer + "     FROM AFFILIATE_CUSTOMER_DTLS A, RFQ_SPOT_RATE_SEA_TBL CT , CUSTOMER_MST_TBL C ";
                strCustomer = strCustomer + "    WHERE A.REFERENCE_MST_FK = CT.RFQ_SPOT_SEA_PK                          ";
                strCustomer = strCustomer + "    AND A.CUST_MST_FK       =  " + Convert.ToString(intCustomerPK) + "               ";
                strCustomer = strCustomer + "     AND A.CUST_MST_FK=C.CUSTOMER_MST_PK ))  ";
            }

            //QUOTATION
            if (!(intQuotationPK == 0))
            {
                //Updated
                strSql = FunMakeQuotationFreight(arrCCondition, intQuotationPK, strPOL, strPOD, intIsFcl, strContainer, EBKGSTATUS, BookingPK, intQuotationDtlPK, intCarrierPK);
            }
            else
            {
                //SPOT RATE
                if (!(intSpotRatePK == 0))
                {
                    strSql = Convert.ToString(funSpotRateFreight(arrCCondition, strCustomer, Convert.ToString(intCommodityPk), strPOL, strPOD, Convert.ToString(intSpotRatePK), strSDate, intIsFcl, 1));
                }
                if (intSRateStatus == 1 & intSpotRatePK == 0)
                {
                    strSql = Convert.ToString(funSpotRateFreight(arrCCondition, strCustomer, Convert.ToString(intCommodityPk), strPOL, strPOD, Convert.ToString(intSpotRatePK), strSDate, intIsFcl, 2));
                }
                //CUSTOMER(CONTRACT)
                if (intCContractStatus == 1)
                {
                    strSql = Convert.ToString(funCustContFreight(arrCCondition, intCustomerPK, Convert.ToString(intCommodityPk), strPOL, strPOD, strSDate, intIsFcl, getDefault(CustContRefNr, "").ToString(), EBKGSTATUS, BookingPK));
                }
                //SPECIAL RATE REQUEST CONTRACT
                if (intSRRContractStatus == 1)
                {
                    strSql = Convert.ToString(funSRRFreight(arrCCondition, intCustomerPK, Convert.ToString(intCommodityPk), strPOL, strPOD, strSDate, intIsFcl));
                }
                //OPEARATOR TARIFF
                if (intOTariffStatus == 1 | intAgentTariffStatus == 1)
                {
                    strSql = Convert.ToString(funSLTariffFreight(arrCCondition, Convert.ToString(intCommodityPk), strPOL, strPOD, strSDate, intIsFcl, intAgentTariffStatus, DPAgentPK));
                }
                //GENERAL TARIFF
                if (intGTariffStatus == 1)
                {
                    strSql = Convert.ToString(funGTariffFreight(arrCCondition, Convert.ToString(intCommodityPk), strPOL, strPOD, strSDate, intIsFcl));
                }
            }
            try
            {
                dtMain = objWF.GetDataTable(strSql);
                return dtMain;
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

        //Added By : Anand
        //Reason   : To Get Customer PK from Temp Customer Table
        //Date     : 24/03/2008
        public string GetTempOrNewCus(int CustomerPK)
        {
            try
            {
                string strSql = null;
                string Temp = null;
                WorkFlow objWF = new WorkFlow();
                strSql = "select customer_mst_pk from temp_customer_tbl where customer_mst_pk='" + CustomerPK + "'";
                Temp = objWF.ExecuteScaler(strSql);
                return Temp;
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

        //Added By : Anand
        //Reason   : To Get Permanant Customer FK from Temp Customer Table
        //Date     : 24/03/2008
        public string GetTempOrNewCus1(int CustomerPK)
        {
            try
            {
                string strSql = null;
                string Temp = null;
                WorkFlow objWF = new WorkFlow();
                strSql = "select permanent_cust_mst_fk from temp_customer_tbl where customer_mst_pk='" + CustomerPK + "'";
                Temp = objWF.ExecuteScaler(strSql);
                return Temp;
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

        public string FunMakeQuotationFreight(ArrayList arrCCondition, Int32 intQuotationPK = 0, string strPOL = "", string strPOD = "", Int16 intIsFcl = 0, string strContainer = "", int EBKGSTATUS = 0, int BookingPK = 0, int intQuotationDtlPK = 0, int intCarrierPK = 0)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strBuilder.Append("SELECT Q.TRNTYPEFK,  ");
                    strBuilder.Append(" Q.REFNO,  ");
                    strBuilder.Append(" Q.BASIS,  ");
                    strBuilder.Append(" Q.COMMODITY,  ");
                    strBuilder.Append(" Q.POLPK PORT_MST_PK,  ");
                    strBuilder.Append(" Q.POL,  ");
                    strBuilder.Append(" Q.PODPK PORT_MST_PK,  ");
                    strBuilder.Append(" Q.POD,  ");
                    strBuilder.Append(" Q.FREIGHT_ELEMENT_MST_PK,  ");
                    strBuilder.Append(" Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,  ");
                    strBuilder.Append(" Q.CHECK_FOR_ALL_IN_RT SEL,  ");
                    strBuilder.Append(" Q.CURRENCY_MST_PK,  ");
                    strBuilder.Append(" Q.CURRENCY_ID,  ");
                    strBuilder.Append(" Q.MIN_RATE,  ");
                    strBuilder.Append(" Q.RATE,  ");
                    strBuilder.Append(" Q.BKGRATE,Q.TOTAL,  ");
                    strBuilder.Append(" Q.BASISPK,  ");
                    strBuilder.Append(" Q.PYMT_TYPE SEL,Q.CREDIT,Q.CHECK_ADVATOS FROM  (");
                    strBuilder.Append("SELECT QTRN.QUOTE_DTL_PK AS TRNTYPEFK,  ");
                    strBuilder.Append("QHDR.QUOTATION_REF_NO AS REFNO, ");
                    strBuilder.Append("QUOM.DIMENTION_ID AS BASIS, ");
                    strBuilder.Append("QCMT.COMMODITY_ID AS COMMODITY, ");
                    strBuilder.Append("QPL.PORT_MST_PK POLPK,QPL.PORT_ID AS POL, ");
                    strBuilder.Append("QPD.PORT_MST_PK PODPK,QPD.PORT_ID AS POD, ");
                    strBuilder.Append("QFEMT.FREIGHT_ELEMENT_MST_PK, QFEMT.FREIGHT_ELEMENT_ID, ");
                    strBuilder.Append("DECODE(QFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                    strBuilder.Append("DECODE(QTRNCHRG.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS CHECK_FOR_ALL_IN_RT, ");
                    strBuilder.Append("QCUMT.CURRENCY_MST_PK,QCUMT.CURRENCY_ID, ");
                    strBuilder.Append("QTRNCHRG.QUOTED_MIN_RATE AS MIN_RATE, ");
                    strBuilder.Append("QTRNCHRG.QUOTED_RATE AS RATE, ");
                    strBuilder.Append("(CASE WHEN QTRNCHRG.QUOTED_MIN_RATE > QTRNCHRG.QUOTED_RATE THEN QTRNCHRG.QUOTED_MIN_RATE ELSE QTRNCHRG.QUOTED_RATE END) AS BKGRATE, ");
                    strBuilder.Append("QTRNCHRG.TARIFF_RATE AS TOTAL, QTRN.BASIS AS BASISPK, ");
                    strBuilder.Append("DECODE(QTRNCHRG.PYMT_TYPE, 1,'1', 2,'2', 3,'3') AS PYMT_TYPE,QFEMT.CREDIT,QTRNCHRG.CHECK_ADVATOS ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("QUOTATION_MST_TBL QHDR, ");
                    strBuilder.Append("QUOTATION_DTL_TBL QTRN, ");
                    strBuilder.Append("QUOTATION_FREIGHT_TRN QTRNCHRG, ");
                    strBuilder.Append("OPERATOR_MST_TBL QOMT, ");
                    strBuilder.Append("COMMODITY_MST_TBL QCMT, ");
                    strBuilder.Append("PORT_MST_TBL QPL, ");
                    strBuilder.Append("PORT_MST_TBL QPD, ");
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL QFEMT, ");
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL QCUMT, ");
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL QUOM ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND QHDR.QUOTATION_MST_PK = QTRN.QUOTATION_MST_FK ");
                    strBuilder.Append("AND QTRN.QUOTE_DTL_PK = QTRNCHRG.QUOTATION_DTL_FK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK=QPL.PORT_MST_PK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK=QPD.PORT_MST_PK ");
                    strBuilder.Append("AND QTRNCHRG.FREIGHT_ELEMENT_MST_FK=QFEMT.FREIGHT_ELEMENT_MST_PK ");
                    strBuilder.Append("AND QTRNCHRG.CURRENCY_MST_FK=QCUMT.CURRENCY_MST_PK ");
                    strBuilder.Append("AND QTRN.CARRIER_MST_FK=QOMT.OPERATOR_MST_PK (+)");
                    strBuilder.Append("AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK (+) ");
                    strBuilder.Append("AND QTRN.BASIS=QUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append(" " + arrCCondition[0] + "");
                    if (intQuotationDtlPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.QUOTE_DTL_PK= " + intQuotationDtlPK);
                    }
                    if (intCarrierPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CARRIER_MST_FK= " + intCarrierPK);
                    }
                    strBuilder.Append("AND QHDR.QUOTATION_MST_PK= " + intQuotationPK);
                    //'
                    if (!string.IsNullOrEmpty(strContainer))
                    {
                        strBuilder.Append(" UNION ");
                        strBuilder.Append("SELECT NULL AS TRNTYPEFK,");
                        strBuilder.Append("       NULL AS REFNO,");
                        strBuilder.Append("       OUOM.DIMENTION_ID AS TYPE,");
                        strBuilder.Append("       NULL AS COMMODITY,");
                        strBuilder.Append("       OPL.PORT_MST_PK,");
                        strBuilder.Append("       OPL.PORT_ID AS POL,");
                        strBuilder.Append("       OPD.PORT_MST_PK,");
                        strBuilder.Append("       OPD.PORT_ID AS POD,");
                        strBuilder.Append("       OFEMT.FREIGHT_ELEMENT_MST_PK,");
                        strBuilder.Append("       OFEMT.FREIGHT_ELEMENT_ID,");
                        strBuilder.Append("       DECODE(OFEMT.CHARGE_BASIS,");
                        strBuilder.Append("              '1',");
                        strBuilder.Append("              '%',");
                        strBuilder.Append("              '2',");
                        strBuilder.Append("              'Flat Rate',");
                        strBuilder.Append("              '3',");
                        strBuilder.Append("              'Kgs',");
                        strBuilder.Append("              '4',");
                        strBuilder.Append("              'Unit') CHARGE_BASIS,");
                        strBuilder.Append("       DECODE(0, 1, 'true', 'false') SEL,");
                        strBuilder.Append("       OCUMT.CURRENCY_MST_PK,");
                        strBuilder.Append("       OCUMT.CURRENCY_ID,");
                        strBuilder.Append("       0 AS MIN_RATE,");
                        strBuilder.Append("       0 AS RATE,");
                        strBuilder.Append("       0 AS BKGRATE,");
                        strBuilder.Append("       0 TOTAL,");
                        strBuilder.Append("       OUOM.DIMENTION_UNIT_MST_PK AS BASISPK,");
                        strBuilder.Append("       '1' AS PYMT_TYPE,");
                        strBuilder.Append("       OFEMT.CREDIT,0 CHECK_ADVATOS ");
                        strBuilder.Append("  FROM PORT_MST_TBL            OPL,");
                        strBuilder.Append("       PORT_MST_TBL            OPD,");
                        strBuilder.Append("       FREIGHT_ELEMENT_MST_TBL OFEMT,");
                        strBuilder.Append("       CURRENCY_TYPE_MST_TBL   OCUMT,");
                        strBuilder.Append("       DIMENTION_UNIT_MST_TBL  OUOM");
                        strBuilder.Append(" WHERE (1 = 1)");
                        strBuilder.Append("   AND OPL.PORT_MST_PK = " + strPOL);
                        strBuilder.Append("   AND OPD.PORT_MST_PK = " + strPOD);
                        strBuilder.Append("   AND OCUMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ");
                        strBuilder.Append("   AND OFEMT.BUSINESS_TYPE IN (2, 3)");
                        strBuilder.Append("   AND OFEMT.ACTIVE_FLAG = 1");
                        strBuilder.Append("   AND NVL(OFEMT.CHARGE_TYPE, 0) <> 3");
                        strBuilder.Append("   AND OUOM.ACTIVE = 1");
                        strBuilder.Append("   AND DIMENTION_UNIT_MST_PK IN (" + strContainer + ")");
                        strBuilder.Append("   AND OUOM.DIMENTION_UNIT_MST_PK NOT IN");
                        strBuilder.Append("       (SELECT QT.BASIS");
                        strBuilder.Append("          FROM QUOTATION_MST_TBL Q, QUOTATION_DTL_TBL QT");
                        strBuilder.Append("         WHERE Q.QUOTATION_MST_PK = QT.QUOTATION_MST_FK");
                        if (intQuotationDtlPK > 0)
                        {
                            strBuilder.Append(" AND QT.QUOTE_DTL_PK= " + intQuotationDtlPK);
                        }
                        if (intCarrierPK > 0)
                        {
                            strBuilder.Append(" AND QT.CARRIER_MST_FK= " + intCarrierPK);
                        }
                        strBuilder.Append("           AND Q.QUOTATION_MST_PK = " + intQuotationPK + ")");
                    }
                    //'
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                else if (intIsFcl == 1)
                {
                    strBuilder.Append("SELECT Q.TRNTYPEFK,  ");
                    strBuilder.Append(" Q.REFNO,  ");
                    strBuilder.Append(" Q.TYPE,  ");
                    strBuilder.Append(" Q.COMMODITY,  ");
                    strBuilder.Append(" Q.POLPK PORT_MST_PK,  ");
                    strBuilder.Append(" Q.POL,  ");
                    strBuilder.Append(" Q.PODPK PORT_MST_PK,  ");
                    strBuilder.Append(" Q.POD,  ");
                    strBuilder.Append(" Q.FREIGHT_ELEMENT_MST_PK,  ");
                    strBuilder.Append(" Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,  ");
                    strBuilder.Append(" Q.CHECK_FOR_ALL_IN_RT SEL,  ");
                    strBuilder.Append(" Q.CURRENCY_MST_PK,  ");
                    strBuilder.Append(" Q.CURRENCY_ID,  ");
                    strBuilder.Append(" Q.RATE,  ");
                    strBuilder.Append(" Q.BKGRATE,Q.TOTAL,  ");
                    strBuilder.Append(" Q.BASISPK,  ");
                    strBuilder.Append(" Q.PYMT_TYPE SEL,Q.CREDIT,Q.CHECK_ADVATOS FROM  (");
                    strBuilder.Append("SELECT QTRN.QUOTE_DTL_PK AS TRNTYPEFK,  ");
                    strBuilder.Append("QHDR.QUOTATION_REF_NO AS REFNO, ");
                    strBuilder.Append("QCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                    strBuilder.Append("QCMT.COMMODITY_ID AS COMMODITY, ");
                    strBuilder.Append("QPL.PORT_MST_PK POLPK,QPL.PORT_ID AS POL, ");
                    strBuilder.Append("QPD.PORT_MST_PK PODPK,QPD.PORT_ID AS POD, ");
                    strBuilder.Append("QFEMT.FREIGHT_ELEMENT_MST_PK, QFEMT.FREIGHT_ELEMENT_ID, ");
                    strBuilder.Append("DECODE(QFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                    strBuilder.Append("DECODE(QTRNCHRG.CHECK_FOR_ALL_IN_RT, 1,'1','0') AS CHECK_FOR_ALL_IN_RT, ");
                    strBuilder.Append("QCUMT.CURRENCY_MST_PK,QCUMT.CURRENCY_ID, ");
                    strBuilder.Append("QTRNCHRG.QUOTED_RATE AS RATE, ");
                    strBuilder.Append("QTRNCHRG.QUOTED_RATE AS BKGRATE,QTRNCHRG.Tariff_Rate AS TOTAL, QTRN.BASIS AS BASISPK, ");
                    strBuilder.Append("DECODE(QTRNCHRG.PYMT_TYPE, 1,'1',2,'2',3,'3') AS PYMT_TYPE,QFEMT.CREDIT,QTRNCHRG.CHECK_ADVATOS ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("QUOTATION_MST_TBL QHDR, ");
                    strBuilder.Append("QUOTATION_DTL_TBL QTRN, ");
                    strBuilder.Append("QUOTATION_FREIGHT_TRN QTRNCHRG, ");
                    strBuilder.Append("OPERATOR_MST_TBL QOMT,  ");
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL QCTMT,  ");
                    strBuilder.Append("COMMODITY_MST_TBL QCMT,  ");
                    strBuilder.Append("PORT_MST_TBL QPL,  ");
                    strBuilder.Append("PORT_MST_TBL QPD,  ");
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL QFEMT,  ");
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL QCUMT ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND QHDR.QUOTATION_MST_PK = QTRN.QUOTATION_MST_FK ");
                    strBuilder.Append("AND QTRN.QUOTE_DTL_PK = QTRNCHRG.QUOTATION_DTL_FK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK=QPL.PORT_MST_PK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK=QPD.PORT_MST_PK ");
                    strBuilder.Append("AND QTRNCHRG.FREIGHT_ELEMENT_MST_FK=QFEMT.FREIGHT_ELEMENT_MST_PK ");
                    strBuilder.Append("AND QTRNCHRG.CURRENCY_MST_FK=QCUMT.CURRENCY_MST_PK ");
                    strBuilder.Append("AND QTRN.CARRIER_MST_FK=QOMT.OPERATOR_MST_PK (+)");
                    strBuilder.Append("AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK (+)");
                    strBuilder.Append("AND QCTMT.CONTAINER_TYPE_MST_PK=QTRN.CONTAINER_TYPE_MST_FK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append(" " + arrCCondition[0] + "");
                    if (intQuotationDtlPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.QUOTE_DTL_PK= " + intQuotationDtlPK);
                    }
                    if (intCarrierPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CARRIER_MST_FK= " + intCarrierPK);
                    }
                    strBuilder.Append(" AND QHDR.QUOTATION_MST_PK= " + intQuotationPK);

                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CONTAINER_TYPE_MST_FK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN BTRN WHERE BTRN.BOOKING_MST_FK =" + BookingPK + ")");
                    }
                    strBuilder.Append(") Q   ");
                    //'
                    if (!string.IsNullOrEmpty(strContainer))
                    {
                        strBuilder.Append(" UNION");
                        strBuilder.Append("        SELECT DISTINCT NULL AS TRNTYPEFK,");
                        strBuilder.Append("                        NULL AS REFNO,");
                        strBuilder.Append("                     OCTMT.CONTAINER_TYPE_MST_ID AS TYPE,");
                        strBuilder.Append("                        NULL AS COMMODITY,");
                        strBuilder.Append("                        OPL.PORT_MST_PK,");
                        strBuilder.Append("                        OPL.PORT_ID AS POL,");
                        strBuilder.Append("                        OPD.PORT_MST_PK,");
                        strBuilder.Append("                        OPD.PORT_ID AS POD,");
                        strBuilder.Append("                        OFEMT.FREIGHT_ELEMENT_MST_PK,");
                        strBuilder.Append("                        OFEMT.FREIGHT_ELEMENT_ID,");
                        strBuilder.Append("                        DECODE(OFEMT.CHARGE_BASIS,");
                        strBuilder.Append("                               '1',");
                        strBuilder.Append("                               '%',");
                        strBuilder.Append("                               '2',");
                        strBuilder.Append("                               'Flat Rate',");
                        strBuilder.Append("                               '3',");
                        strBuilder.Append("                               'Kgs',");
                        strBuilder.Append("                               '4',");
                        strBuilder.Append("                               'Unit') CHARGE_BASIS,");
                        strBuilder.Append("                        DECODE(0, 1, 'true', 'false') SEL,");
                        strBuilder.Append("                        OCUMT.CURRENCY_MST_PK,");
                        strBuilder.Append("                        OCUMT.CURRENCY_ID,");
                        strBuilder.Append("                        0 AS RATE,");
                        strBuilder.Append("                        0 AS BKGRATE,");
                        strBuilder.Append("                        0 TOTAL,");
                        strBuilder.Append("                        0 AS BASISPK,");
                        strBuilder.Append("                        '1' AS PYMT_TYPE,");
                        strBuilder.Append("                                         OFEMT.CREDIT,0 CHECK_ADVATOS ");
                        strBuilder.Append("          FROM CONTAINER_TYPE_MST_TBL  OCTMT,");
                        strBuilder.Append("               PORT_MST_TBL            OPL,");
                        strBuilder.Append("               PORT_MST_TBL            OPD,");
                        strBuilder.Append("               FREIGHT_ELEMENT_MST_TBL OFEMT,");
                        strBuilder.Append("               CURRENCY_TYPE_MST_TBL   OCUMT");
                        strBuilder.Append("         WHERE (1 = 1)");
                        strBuilder.Append("           AND OPL.PORT_MST_PK = " + strPOL);
                        strBuilder.Append("           AND OPD.PORT_MST_PK = " + strPOD);
                        strBuilder.Append("           AND OCUMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " ");
                        strBuilder.Append("           AND OFEMT.BUSINESS_TYPE = 2");
                        strBuilder.Append("           AND OFEMT.ACTIVE_FLAG = 1");
                        strBuilder.Append("           AND OCTMT.ACTIVE_FLAG = 1");
                        strBuilder.Append("           AND NVL(OFEMT.CHARGE_TYPE, 0) <> 3");
                        strBuilder.Append("           AND OCTMT.CONTAINER_TYPE_MST_PK IN (" + strContainer + ")");
                        strBuilder.Append("           AND OCTMT.CONTAINER_TYPE_MST_PK NOT IN");
                        strBuilder.Append("               (SELECT QT.CONTAINER_TYPE_MST_FK");
                        strBuilder.Append("                  FROM QUOTATION_MST_TBL Q, QUOTATION_DTL_TBL QT");
                        strBuilder.Append("                 WHERE Q.QUOTATION_MST_PK = QT.QUOTATION_MST_FK");
                        if (intQuotationDtlPK > 0)
                        {
                            strBuilder.Append(" AND QT.QUOTE_DTL_PK= " + intQuotationDtlPK);
                        }
                        if (intCarrierPK > 0)
                        {
                            strBuilder.Append(" AND QT.CARRIER_MST_FK= " + intCarrierPK);
                        }
                        strBuilder.Append(" AND Q.QUOTATION_MST_PK = " + intQuotationPK + ")");
                    }
                    //'
                    //strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ")
                    //strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ")
                    //strBuilder.Append(" ORDER BY FRT.PREFERENCE ")
                }
                else
                {
                    strBuilder.Append("SELECT Q.TRNTYPEFK,  ");
                    strBuilder.Append(" Q.REFNO,  ");
                    strBuilder.Append(" Q.BASIS,  ");
                    strBuilder.Append(" Q.COMMODITY,  ");
                    strBuilder.Append(" Q.POLPK PORT_MST_PK,  ");
                    strBuilder.Append(" Q.POL,  ");
                    strBuilder.Append(" Q.PODPK PORT_MST_PK,  ");
                    strBuilder.Append(" Q.POD,  ");
                    strBuilder.Append(" Q.FREIGHT_ELEMENT_MST_PK,  ");
                    strBuilder.Append(" Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,  ");
                    strBuilder.Append(" Q.CHECK_FOR_ALL_IN_RT SEL,  ");
                    strBuilder.Append(" Q.CURRENCY_MST_PK,  ");
                    strBuilder.Append(" Q.CURRENCY_ID,  ");
                    strBuilder.Append(" Q.MIN_RATE SURCHARGE_VALUE,  ");
                    strBuilder.Append(" Q.BKGRATE MIN_RATE,");
                    strBuilder.Append(" Q.EXCHANGERATE RATE,  ");
                    strBuilder.Append(" Q.TOTAL BKGRATE,  ");
                    strBuilder.Append(" Q.BASISPK,  ");
                    strBuilder.Append(" Q.PYMT_TYPE SEL,Q.COMMODITYPK,null FreightPK, Q.EXCHANGERATE,'' SURCHARGE_TYPE,Q.CREDIT FROM  (");
                    strBuilder.Append("SELECT QTRN.QUOTE_DTL_PK AS TRNTYPEFK,  ");
                    strBuilder.Append("QHDR.QUOTATION_REF_NO AS REFNO, ");
                    strBuilder.Append("QUOM.DIMENTION_ID AS BASIS, ");
                    strBuilder.Append("QCMT.COMMODITY_ID AS COMMODITY, ");
                    strBuilder.Append("QPL.PORT_MST_PK POLPK,QPL.PORT_ID AS POL, ");
                    strBuilder.Append("QPD.PORT_MST_PK PODPK,QPD.PORT_ID AS POD, ");
                    strBuilder.Append("QFEMT.FREIGHT_ELEMENT_MST_PK, QFEMT.FREIGHT_ELEMENT_ID, ");
                    strBuilder.Append("DECODE(QFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                    strBuilder.Append("DECODE(QTRNCHRG.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS CHECK_FOR_ALL_IN_RT, ");
                    strBuilder.Append("QCUMT.CURRENCY_MST_PK,QCUMT.CURRENCY_ID, ");
                    strBuilder.Append("QTRNCHRG.QUOTED_MIN_RATE AS MIN_RATE, ");
                    strBuilder.Append("(CASE WHEN QTRNCHRG.QUOTED_MIN_RATE > QTRNCHRG.QUOTED_RATE THEN QTRNCHRG.QUOTED_MIN_RATE ELSE QTRNCHRG.QUOTED_RATE END) AS BKGRATE, ");
                    strBuilder.Append("QTRNCHRG.TARIFF_RATE AS TOTAL, QTRN.BASIS AS BASISPK, QTRN.COMMODITY_MST_FK COMMODITYPK,");
                    strBuilder.Append("DECODE(QTRNCHRG.PYMT_TYPE, 1,'1', 2,'2', 3, '3') AS PYMT_TYPE,QFEMT.CREDIT,QTRNCHRG.CHECK_ADVATOS,NVL(get_ex_rate(QTRNCHRG.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",sysdate),0) EXCHANGERATE ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("QUOTATION_MST_TBL QHDR, ");
                    strBuilder.Append("QUOTATION_DTL_TBL QTRN, ");
                    strBuilder.Append("QUOTATION_FREIGHT_TRN QTRNCHRG, ");
                    strBuilder.Append("OPERATOR_MST_TBL QOMT, ");
                    strBuilder.Append("COMMODITY_MST_TBL QCMT, ");
                    strBuilder.Append("PORT_MST_TBL QPL, ");
                    strBuilder.Append("PORT_MST_TBL QPD, ");
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL QFEMT, ");
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL QCUMT, ");
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL QUOM ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND QHDR.QUOTATION_MST_PK = QTRN.QUOTATION_MST_FK ");
                    strBuilder.Append("AND QTRN.QUOTE_DTL_PK = QTRNCHRG.QUOTATION_DTL_FK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK=QPL.PORT_MST_PK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK=QPD.PORT_MST_PK ");
                    strBuilder.Append("AND QTRNCHRG.FREIGHT_ELEMENT_MST_FK=QFEMT.FREIGHT_ELEMENT_MST_PK ");
                    strBuilder.Append("AND QTRNCHRG.CURRENCY_MST_FK=QCUMT.CURRENCY_MST_PK ");
                    strBuilder.Append("AND QTRN.CARRIER_MST_FK=QOMT.OPERATOR_MST_PK (+)");
                    strBuilder.Append("AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK (+) ");
                    strBuilder.Append("AND QTRN.BASIS=QUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append(" " + arrCCondition[0] + "");
                    if (intQuotationDtlPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.QUOTE_DTL_PK= " + intQuotationDtlPK);
                    }
                    if (intCarrierPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CARRIER_MST_FK= " + intCarrierPK);
                    }
                    strBuilder.Append("AND QHDR.QUOTATION_MST_PK= " + intQuotationPK);
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                return strBuilder.ToString();
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

        public object funSpotRateFreight(ArrayList arrCCondition, string strCustomer, string intCommodityPK, string strPOL, string strPOD, string intSpotRatePK, string strSDate, Int16 intIsFcl, Int16 intFlag)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    if (intFlag == 1)
                    {
                        strBuilder.Append("SELECT SRRSTSFL.RFQ_SPOT_SEA_FK AS TRNTYPEFK, ");
                        strBuilder.Append("SRRSRST.RFQ_REF_NO AS REFNO, ");
                        strBuilder.Append("SRUOM.DIMENTION_ID AS BASIS, ");
                        strBuilder.Append("SRCMT.COMMODITY_ID AS COMMODITY, ");
                        strBuilder.Append("SRPL.PORT_MST_PK,SRPL.PORT_ID AS POL, ");
                        strBuilder.Append("SRPD.PORT_MST_PK,SRPD.PORT_ID AS POD, ");
                        strBuilder.Append("SRFEMT.FREIGHT_ELEMENT_MST_PK, SRFEMT.FREIGHT_ELEMENT_ID, ");
                        strBuilder.Append(" DECODE(SRFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, ");
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, ");
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_MIN_RATE AS MIN_RATE, ");
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS RATE, ");
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS BKGRATE,SRRSTSFL.LCL_APPROVED_RATE AS TOTAL, SRRSTSFL.LCL_BASIS AS BASISPK, ");
                        strBuilder.Append("'1' AS PYMT_TYPE,SRFEMT.CREDIT  ");
                        strBuilder.Append("FROM RFQ_SPOT_RATE_SEA_TBL SRRSRST, ");
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSFL, ");
                        strBuilder.Append("OPERATOR_MST_TBL SROMT, ");
                        strBuilder.Append("COMMODITY_MST_TBL SRCMT, ");
                        strBuilder.Append("COMMODITY_GROUP_MST_TBL  SRCOMM, ");
                        strBuilder.Append("PORT_MST_TBL SRPL, ");
                        strBuilder.Append("PORT_MST_TBL SRPD, ");
                        strBuilder.Append("FREIGHT_ELEMENT_MST_TBL SRFEMT, ");
                        strBuilder.Append("CURRENCY_TYPE_MST_TBL SRCUMT, ");
                        strBuilder.Append("DIMENTION_UNIT_MST_TBL SRUOM ");
                        strBuilder.Append("WHERE (1=1) ");
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSFL.RFQ_SPOT_SEA_FK ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK=SRPL.PORT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK=SRPD.PORT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.FREIGHT_ELEMENT_MST_FK=SRFEMT.FREIGHT_ELEMENT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.CURRENCY_MST_FK=SRCUMT.CURRENCY_MST_PK ");
                        strBuilder.Append("AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) ");
                        strBuilder.Append("AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) ");
                        strBuilder.Append("AND SRRSTSFL.LCL_BASIS=SRUOM.DIMENTION_UNIT_MST_PK ");
                        strBuilder.Append("AND SRRSRST.ACTIVE=1 " + strCustomer);
                        strBuilder.Append("AND SRRSRST.CARGO_TYPE=2 " + arrCCondition[1]);
                        strBuilder.Append("AND SRRSRST.APPROVED=1 ");
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK= " + intSpotRatePK);
                        strBuilder.Append("AND SRCOMM.COMMODITY_GROUP_PK= " + intCommodityPK);
                        strBuilder.Append("and srcomm.commodity_group_pk(+) = srrsrst.commodity_group_fk ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK= " + strPOL);
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK= " + strPOD);
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM ");
                        strBuilder.Append("AND NVL(SRRSRST.VALID_TO, TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ");
                    }
                    else
                    {
                        strBuilder.Append("SELECT SRRSTSFL.RFQ_SPOT_SEA_FK AS TRNTYPEFK, ");
                        strBuilder.Append("SRRSRST.RFQ_REF_NO AS REFNO, ");
                        strBuilder.Append("SRUOM.DIMENTION_ID AS BASIS, ");
                        strBuilder.Append("SRCMT.COMMODITY_ID AS COMMODITY, ");
                        strBuilder.Append("SRPL.PORT_MST_PK,SRPL.PORT_ID AS POL, ");
                        strBuilder.Append("SRPD.PORT_MST_PK,SRPD.PORT_ID AS POD, ");
                        strBuilder.Append("SRFEMT.FREIGHT_ELEMENT_MST_PK, SRFEMT.FREIGHT_ELEMENT_ID, ");
                        strBuilder.Append(" DECODE(SRFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, ");
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, ");
                        strBuilder.Append("SRRSTSFL.LCL_CURRENT_MIN_RATE AS MIN_RATE, ");
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS RATE, ");
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS BKGRATE,SRRSTSFL.LCL_APPROVED_RATE AS TOTAL, SRRSTSFL.LCL_BASIS AS BASISPK, ");
                        strBuilder.Append("'1' AS PYMT_TYPE, SRFEMT.CREDIT ");
                        strBuilder.Append("FROM RFQ_SPOT_RATE_SEA_TBL SRRSRST, ");
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSFL, ");
                        strBuilder.Append("OPERATOR_MST_TBL SROMT, ");
                        strBuilder.Append("COMMODITY_MST_TBL SRCMT, ");
                        strBuilder.Append("COMMODITY_GROUP_MST_TBL  SRCOMM, ");
                        strBuilder.Append("PORT_MST_TBL SRPL, ");
                        strBuilder.Append("PORT_MST_TBL SRPD, ");
                        strBuilder.Append("FREIGHT_ELEMENT_MST_TBL SRFEMT, ");
                        strBuilder.Append("CURRENCY_TYPE_MST_TBL SRCUMT, ");
                        strBuilder.Append("DIMENTION_UNIT_MST_TBL SRUOM ");
                        strBuilder.Append("WHERE (1=1) ");
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSFL.RFQ_SPOT_SEA_FK ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK=SRPL.PORT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK=SRPD.PORT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.FREIGHT_ELEMENT_MST_FK=SRFEMT.FREIGHT_ELEMENT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.CURRENCY_MST_FK=SRCUMT.CURRENCY_MST_PK ");
                        strBuilder.Append("AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) ");
                        strBuilder.Append("AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) ");
                        strBuilder.Append("AND SRRSTSFL.LCL_BASIS=SRUOM.DIMENTION_UNIT_MST_PK ");
                        strBuilder.Append("AND SRRSRST.ACTIVE=1 " + strCustomer);
                        strBuilder.Append("AND SRRSRST.CARGO_TYPE=2 " + arrCCondition[1]);
                        strBuilder.Append("AND SRRSRST.APPROVED=1 ");
                        strBuilder.Append("AND SRCOMM.COMMODITY_GROUP_PK= " + intCommodityPK);
                        strBuilder.Append("and srcomm.commodity_group_pk(+) = srrsrst.commodity_group_fk ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK= " + strPOL);
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK= " + strPOD);
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM ");
                        strBuilder.Append("AND NVL(SRRSRST.VALID_TO, TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ");
                    }
                }
                else
                {
                    //modifying by thiyagarajan on 23/10/08 to make commodity optional for FCL as per prabhu sugges.
                    if (intFlag == 1)
                    {
                        strBuilder.Append("SELECT SRRSTSFL.RFQ_SPOT_SEA_FK AS TRNTYPEFK, ");
                        strBuilder.Append("SRRSRST.RFQ_REF_NO AS REFNO, ");
                        strBuilder.Append("SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                        strBuilder.Append("SRCMT.COMMODITY_ID AS COMMODITY, ");
                        strBuilder.Append("SRPL.PORT_MST_PK,SRPL.PORT_ID AS POL, ");
                        strBuilder.Append("SRPD.PORT_MST_PK,SRPD.PORT_ID AS POD, ");
                        strBuilder.Append("SRFEMT.FREIGHT_ELEMENT_MST_PK, SRFEMT.FREIGHT_ELEMENT_ID, ");
                        strBuilder.Append(" DECODE(SRFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, ");
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, ");
                        strBuilder.Append("SRRST.FCL_APP_RATE AS RATE, ");
                        strBuilder.Append("SRRST.FCL_APP_RATE AS BKGRATE,SRRST.FCL_APP_RATE AS TOTAL, SRRSTSFL.LCL_BASIS AS BASISPK, ");
                        strBuilder.Append("'1' AS PYMT_TYPE,SRFEMT.Credit ");
                        strBuilder.Append("FROM RFQ_SPOT_RATE_SEA_TBL SRRSRST, ");
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSFL, ");
                        strBuilder.Append("OPERATOR_MST_TBL SROMT, ");
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_CONT_DET SRRST, ");
                        strBuilder.Append("CONTAINER_TYPE_MST_TBL SRCTMT, ");
                        strBuilder.Append("COMMODITY_MST_TBL SRCMT, ");
                        strBuilder.Append("COMMODITY_GROUP_MST_TBL  SRCOMM, ");
                        strBuilder.Append("PORT_MST_TBL SRPL, ");
                        strBuilder.Append("PORT_MST_TBL SRPD, ");
                        strBuilder.Append("FREIGHT_ELEMENT_MST_TBL SRFEMT, ");
                        strBuilder.Append("CURRENCY_TYPE_MST_TBL SRCUMT ");
                        strBuilder.Append("WHERE (1=1) ");
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSFL.RFQ_SPOT_SEA_FK ");
                        strBuilder.Append("AND SRRST.RFQ_SPOT_SEA_TRN_FK=SRRSTSFL.RFQ_SPOT_SEA_TRN_PK ");
                        //Snigdharani
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK=SRPL.PORT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK=SRPD.PORT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.FREIGHT_ELEMENT_MST_FK=SRFEMT.FREIGHT_ELEMENT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.CURRENCY_MST_FK=SRCUMT.CURRENCY_MST_PK ");
                        strBuilder.Append("AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK (+)");
                        strBuilder.Append("AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) " + arrCCondition[1]);
                        strBuilder.Append("AND SRCTMT.CONTAINER_TYPE_MST_PK = SRRST.CONTAINER_type_MST_FK ");
                        strBuilder.Append("AND SRRSRST.ACTIVE=1 ");
                        strBuilder.Append("AND SRRSRST.CARGO_TYPE=1 " + strCustomer);
                        strBuilder.Append("AND SRRSRST.APPROVED=1");
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK= " + intSpotRatePK);
                        strBuilder.Append("AND SRCOMM.COMMODITY_GROUP_PK= " + intCommodityPK);
                        strBuilder.Append("and srcomm.commodity_group_pk(+) = srrsrst.commodity_group_fk ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK= " + strPOL);
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK= " + strPOD);
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM ");
                        strBuilder.Append("AND NVL(SRRSRST.VALID_TO, TO_DATE('" + strSDate + "','" + dateFormat + "'))");
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ");
                    }
                    else
                    {
                        strBuilder.Append("SELECT SRRSTSFL.RFQ_SPOT_SEA_FK AS TRNTYPEFK, ");
                        strBuilder.Append("SRRSRST.RFQ_REF_NO AS REFNO, ");
                        strBuilder.Append("SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                        strBuilder.Append("SRCMT.COMMODITY_ID AS COMMODITY, ");
                        strBuilder.Append("SRPL.PORT_MST_PK,SRPL.PORT_ID AS POL, ");
                        strBuilder.Append("SRPD.PORT_MST_PK,SRPD.PORT_ID AS POD, ");
                        strBuilder.Append("SRFEMT.FREIGHT_ELEMENT_MST_PK, SRFEMT.FREIGHT_ELEMENT_ID, ");
                        strBuilder.Append(" DECODE(SRFEMT.CHARGE_BASIS,0,'',1,'%',2,'Flat Rate',3,'Unit')CHARGE_BASIS, ");
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, ");
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, ");
                        strBuilder.Append("SRRST.FCL_APP_RATE AS RATE, ");
                        strBuilder.Append("SRRST.FCL_APP_RATE AS BKGRATE,SRRST.FCL_APP_RATE AS TOTAL, SRRSTSFL.LCL_BASIS AS BASISPK, ");
                        strBuilder.Append("'1' AS PYMT_TYPE, SRFEMT.CREDIT  ");
                        strBuilder.Append("FROM RFQ_SPOT_RATE_SEA_TBL SRRSRST, ");
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSFL, ");
                        strBuilder.Append("OPERATOR_MST_TBL SROMT, ");
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_CONT_DET SRRST, ");
                        strBuilder.Append("CONTAINER_TYPE_MST_TBL SRCTMT, ");
                        strBuilder.Append("COMMODITY_MST_TBL SRCMT, ");
                        strBuilder.Append("COMMODITY_GROUP_MST_TBL  SRCOMM, ");
                        strBuilder.Append("PORT_MST_TBL SRPL, ");
                        strBuilder.Append("PORT_MST_TBL SRPD, ");
                        strBuilder.Append("FREIGHT_ELEMENT_MST_TBL SRFEMT, ");
                        strBuilder.Append("CURRENCY_TYPE_MST_TBL SRCUMT ");
                        strBuilder.Append("WHERE (1=1) ");
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSFL.RFQ_SPOT_SEA_FK ");
                        strBuilder.Append("AND SRRST.RFQ_SPOT_SEA_TRN_FK=SRRSTSFL.RFQ_SPOT_SEA_TRN_PK ");
                        //Snigdharani
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK=SRPL.PORT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK=SRPD.PORT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.FREIGHT_ELEMENT_MST_FK=SRFEMT.FREIGHT_ELEMENT_MST_PK ");
                        strBuilder.Append("AND SRRSTSFL.CURRENCY_MST_FK=SRCUMT.CURRENCY_MST_PK ");
                        strBuilder.Append("AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK (+)");
                        strBuilder.Append("AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) " + arrCCondition[1]);
                        strBuilder.Append("AND SRCTMT.CONTAINER_TYPE_MST_PK = SRRST.CONTAINER_type_MST_FK ");
                        strBuilder.Append("AND SRRSRST.ACTIVE=1 ");
                        strBuilder.Append("AND SRRSRST.CARGO_TYPE=1 ");
                        strBuilder.Append("AND SRRSRST.APPROVED=1" + strCustomer);
                        strBuilder.Append("AND SRCOMM.COMMODITY_GROUP_PK= " + intCommodityPK);
                        strBuilder.Append("and srcomm.commodity_group_pk(+) = srrsrst.commodity_group_fk ");
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK= " + strPOL);
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK= " + strPOD);
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM ");
                        strBuilder.Append("AND NVL(SRRSRST.VALID_TO, TO_DATE('" + strSDate + "','" + dateFormat + "'))");
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ");
                    }
                }
                return strBuilder.ToString();
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

        public object funCustContFreight(ArrayList arrCCondition, Int32 intCustomerPK, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl, string CustContRefNr = "", int EBKGSTATUS = 0, int BookingPK = 0)
        {
            try
            {
                System.Text.StringBuilder strContRefNo = new System.Text.StringBuilder();
                System.Text.StringBuilder strFreightElements = new System.Text.StringBuilder();
                System.Text.StringBuilder strSurcharge = new System.Text.StringBuilder();
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strContRefNo.Append(" (   Select    DISTINCT  CONT_REF_NO ");
                    strContRefNo.Append("from  ");
                    strContRefNo.Append("CONT_CUST_SEA_TBL main7, ");
                    strContRefNo.Append("CONT_CUST_TRN_SEA_TBL tran7 ");
                    strContRefNo.Append("where ");
                    strContRefNo.Append("main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK ");
                    strContRefNo.Append("AND    main7.CARGO_TYPE            = 2 AND MAIN7.Active=1");
                    strContRefNo.Append("AND    main7.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "  ");
                    strContRefNo.Append("AND    main7.CUSTOMER_MST_FK       =  " + intCustomerPK + " ");
                    strContRefNo.Append("AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK ");
                    strContRefNo.Append("AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK ");
                    strContRefNo.Append("AND    tran7.LCL_BASIS             =  tran6.LCL_BASIS  ");

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strContRefNo.Append(" AND main7.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strContRefNo.Append(" AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN ");
                        strContRefNo.Append("tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT) ");
                    }
                    strContRefNo.Append("AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  ) ");
                    strFreightElements.Append(" (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK ");
                    strFreightElements.Append(" from                                                             ");
                    strFreightElements.Append(" CONT_CUST_SEA_TBL              main8,                           ");
                    strFreightElements.Append(" CONT_CUST_TRN_SEA_TBL          tran8,                           ");
                    strFreightElements.Append(" CONT_SUR_CHRG_SEA_TBL          frtd8                            ");
                    strFreightElements.Append(" where                                                                ");
                    strFreightElements.Append(" main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              ");
                    strFreightElements.Append(" AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          ");
                    strFreightElements.Append(" AND    main8.CARGO_TYPE            = 2 AND main8.Active=1                                  ");
                    strFreightElements.Append(" AND    main8.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strFreightElements.Append(" AND    (main8.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strFreightElements.Append(" OR   main8.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strFreightElements.Append(" FROM AFFILIATE_CUSTOMER_DTLS A, CONT_CUST_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strFreightElements.Append(" WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strFreightElements.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strFreightElements.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");

                    strFreightElements.Append(" AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               ");
                    strFreightElements.Append(" AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               ");
                    strFreightElements.Append(" AND    tran8.LCL_BASIS             = tran6.LCL_BASIS                     ");

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strFreightElements.Append(" AND main8.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strFreightElements.Append(" AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                        strFreightElements.Append(" tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        ");
                    }

                    strFreightElements.Append(" AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ");

                    strSurcharge.Append(" ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  ");
                    strSurcharge.Append("    from                                                             ");
                    strSurcharge.Append("     CONT_CUST_SEA_TBL              main9,                           ");
                    strSurcharge.Append("     CONT_CUST_TRN_SEA_TBL          tran9                            ");
                    strSurcharge.Append("    where                                                                ");
                    strSurcharge.Append("            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              ");
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 2  and MAIN9.Active=1               ");
                    strSurcharge.Append("     AND    main9.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strSurcharge.Append("     AND    (main9.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strSurcharge.Append(" OR   main9.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSurcharge.Append(" FROM AFFILIATE_CUSTOMER_DTLS A, CONT_CUST_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strSurcharge.Append(" WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strSurcharge.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSurcharge.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strSurcharge.Append("     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              ");
                    strSurcharge.Append("     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              ");
                    strSurcharge.Append("     AND    tran9.LCL_BASIS             =  tran6.LCL_BASIS                    ");

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strSurcharge.Append(" AND main9.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strSurcharge.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                        strSurcharge.Append("            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        ");
                    }

                    strSurcharge.Append("     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ");

                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.BASIS,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.MIN_RATE,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       abs(Q.BKGRATE)BKGRATE,Q.BKGRATE TOTAL,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE, q.credit");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("    Select     ");
                    strBuilder.Append(" tran2.CONT_CUST_SEA_FK    TRNTYPEFK,  ");
                    strBuilder.Append(" main2.CONT_REF_NO     REFNO,   ");
                    strBuilder.Append(" CCUOM.DIMENTION_ID BASIS,   ");
                    strBuilder.Append(" CCCMT.COMMODITY_ID COMMODITY, ");
                    strBuilder.Append(" CCPL.PORT_MST_PK POLPK, ");
                    strBuilder.Append(" CCPL.PORT_ID POL, ");
                    strBuilder.Append(" CCPD.PORT_MST_PK PODPK, ");
                    strBuilder.Append(" CCPD.PORT_ID POD, ");
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_MST_PK, ");
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_ID, ");
                    strBuilder.Append(" DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                    strBuilder.Append(" 'true' SEL,  ");
                    strBuilder.Append(" curr2.CURRENCY_MST_PK, ");
                    strBuilder.Append(" curr2.CURRENCY_ID, ");
                    strBuilder.Append("  NULL  MIN_RATE, ");
                    strBuilder.Append(" (CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN");
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE ");
                    strBuilder.Append(" Else ");
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE ");
                    strBuilder.Append(" END) AS RATE, ");
                    strBuilder.Append(" (CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN");
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE ");
                    strBuilder.Append(" Else ");
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE ");
                    strBuilder.Append(" END) AS BKGRATE, ");
                    strBuilder.Append(" tran2.LCL_BASIS BASISPK, ");
                    strBuilder.Append(" '1' AS PYMT_TYPE,FRT2.Credit   ");
                    // strBuilder.Append(" frt2.PREFERENCE " & vbCrLf)
                    strBuilder.Append(" from ");
                    strBuilder.Append(" CONT_CUST_SEA_TBL main2, ");
                    strBuilder.Append(" CONT_CUST_TRN_SEA_TBL tran2, ");
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL frt2, ");
                    strBuilder.Append(" OPERATOR_MST_TBL CCOMT, ");
                    strBuilder.Append(" DIMENTION_UNIT_MST_TBL CCUOM, ");
                    strBuilder.Append(" COMMODITY_MST_TBL CCCMT, ");
                    strBuilder.Append(" PORT_MST_TBL CCPL, ");
                    strBuilder.Append(" PORT_MST_TBL CCPD, ");
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL curr2 ");
                    strBuilder.Append(" where ");
                    strBuilder.Append(" main2.CONT_CUST_SEA_PK = tran2.CONT_CUST_SEA_FK ");
                    strBuilder.Append(" AND FRT2.FREIGHT_ELEMENT_ID LIKE '%BOF' ");
                    strBuilder.Append(" AND TRAN2.CURRENCY_MST_FK= curr2.CURRENCY_MST_PK   ");
                    strBuilder.Append(" AND tran2.PORT_MST_POL_FK=CCPL.PORT_MST_PK ");
                    strBuilder.Append(" AND tran2.PORT_MST_POD_FK=CCPD.PORT_MST_PK ");
                    strBuilder.Append(" AND main2.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)");
                    strBuilder.Append(" AND main2.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) ");
                    strBuilder.Append(" AND TRAN2.LCL_BASIS=CCUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append(" AND main2.CARGO_TYPE  = 2  and main2.Active=1     ");
                    strBuilder.Append(" AND main2.STATUS   = 2       ");
                    strBuilder.Append(" AND main2.COMMODITY_GROUP_MST_FK = " + intCommodityPK);
                    strBuilder.Append(" AND (main2.CUSTOMER_MST_FK  = " + intCustomerPK);
                    strBuilder.Append(" OR   main2.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strBuilder.Append(" FROM AFFILIATE_CUSTOMER_DTLS A, CONT_CUST_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strBuilder.Append(" WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strBuilder.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strBuilder.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strBuilder.Append(" AND tran2.PORT_MST_POL_FK = " + strPOL + " " + arrCCondition[3]);
                    strBuilder.Append(" AND tran2.PORT_MST_POD_FK = " + strPOD);

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strBuilder.Append(" AND main2.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN   ");
                        strBuilder.Append(" tran2.VALID_FROM AND NVL(tran2.VALID_TO,NULL_DATE_FORMAT)  ");
                    }

                    strBuilder.Append(" UNION ");
                    strBuilder.Append(" Select ");
                    strBuilder.Append("     tran2.CONT_CUST_SEA_FK                    TRNTYPEFK,            ");
                    strBuilder.Append("     main2.CONT_REF_NO                          REFNO,               ");
                    strBuilder.Append("     CCUOM.DIMENTION_ID                         BASIS,                ");
                    strBuilder.Append("     CCCMT.COMMODITY_ID                         COMMODITY,           ");
                    strBuilder.Append("     CCPL.PORT_MST_PK POLPK                            ,              ");
                    strBuilder.Append("     CCPL.PORT_ID                               POL,                 ");
                    strBuilder.Append("     CCPD.PORT_MST_PK PODPK              ,              ");
                    strBuilder.Append("     CCPD.PORT_ID                               POD,                 ");
                    strBuilder.Append("     frt2.FREIGHT_ELEMENT_MST_PK,              ");
                    strBuilder.Append("     frt2.FREIGHT_ELEMENT_ID                    ,              ");
                    strBuilder.Append("     DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,              ");
                    strBuilder.Append("     DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,       ");
                    strBuilder.Append("     curr2.CURRENCY_MST_PK                      ,             ");
                    strBuilder.Append("     curr2.CURRENCY_ID                          ,             ");
                    strBuilder.Append("      NULL  MIN_RATE,           ");
                    strBuilder.Append("     frtd2.APP_SURCHARGE_AMT                    RATE,                ");
                    strBuilder.Append("     frtd2.APP_SURCHARGE_AMT                    BKGRATE,                ");
                    strBuilder.Append("     tran2.LCL_BASIS                            BASISPK,                   ");
                    strBuilder.Append("     '1' AS PYMT_TYPE,FRT2.Credit               ");
                    strBuilder.Append("    from                                                             ");
                    strBuilder.Append("     CONT_CUST_SEA_TBL              main2,                           ");
                    strBuilder.Append("     CONT_CUST_TRN_SEA_TBL          tran2,                           ");
                    strBuilder.Append("     CONT_SUR_CHRG_SEA_TBL          frtd2,                           ");
                    strBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt2,                            ");
                    strBuilder.Append("     OPERATOR_MST_TBL               CCOMT,                           ");
                    strBuilder.Append("     DIMENTION_UNIT_MST_TBL         CCUOM,                          ");
                    strBuilder.Append("     COMMODITY_MST_TBL              CCCMT,                           ");
                    strBuilder.Append("     PORT_MST_TBL                   CCPL,                            ");
                    strBuilder.Append("     PORT_MST_TBL                   CCPD,                            ");
                    strBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr2                            ");
                    strBuilder.Append("    where                                                                ");
                    strBuilder.Append("            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              ");
                    strBuilder.Append("     AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK          ");
                    strBuilder.Append("     AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK        ");
                    strBuilder.Append("     AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK               ");
                    strBuilder.Append("     AND    tran2.PORT_MST_POL_FK=CCPL.PORT_MST_PK ");
                    strBuilder.Append("     AND    tran2.PORT_MST_POD_FK=CCPD.PORT_MST_PK ");
                    strBuilder.Append("     AND    main2.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)");
                    strBuilder.Append("     AND    main2.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)");
                    strBuilder.Append("     AND    tran2.LCL_BASIS=CCUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append("     AND    main2.CARGO_TYPE            = 2  and main2.Active=1              ");
                    strBuilder.Append("     AND    main2.STATUS                = 2                                   ");
                    strBuilder.Append("     AND    main2.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strBuilder.Append("     AND    (main2.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strBuilder.Append("    OR   main2.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strBuilder.Append("   FROM AFFILIATE_CUSTOMER_DTLS A, CONT_CUST_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strBuilder.Append("   WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strBuilder.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strBuilder.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strBuilder.Append("     AND    tran2.PORT_MST_POL_FK      = " + strPOL);
                    strBuilder.Append("     AND    tran2.PORT_MST_POD_FK      = " + strPOD + arrCCondition[3]);

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strBuilder.Append(" AND main2.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                        strBuilder.Append("            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)        ");
                    }
                    if (EBKGSTATUS == 0)
                    {
                        strBuilder.Append("    UNION  ");
                        strBuilder.Append("   Select            ");
                        strBuilder.Append("     NULL                                       TRNTYPEFK,           ");
                        strBuilder.Append("     " + strContRefNo.ToString() + "                       REFNO,               ");
                        strBuilder.Append("     CCUOM.DIMENTION_ID                         BASIS,               ");
                        strBuilder.Append("     NULL                                       COMMODITY,           ");
                        strBuilder.Append("     COPL.PORT_MST_PK POLPK             ,                         ");
                        strBuilder.Append("     COPL.PORT_ID                               POL,                 ");
                        strBuilder.Append("     COPD.PORT_MST_PK PODPK            ,                         ");
                        strBuilder.Append("     COPD.PORT_ID                               POD,                 ");
                        strBuilder.Append("     frt6.FREIGHT_ELEMENT_MST_PK               ,                     ");
                        strBuilder.Append("     frt6.FREIGHT_ELEMENT_ID                    ,          ");
                        strBuilder.Append("    DECODE(frt6.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,         ");
                        strBuilder.Append("     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 ");
                        strBuilder.Append("     curr6.CURRENCY_MST_PK                      ,        ");
                        strBuilder.Append("     curr6.CURRENCY_ID                          ,        ");
                        strBuilder.Append("  NULL  MIN_RATE, ");
                        strBuilder.Append("     cont6.FCL_REQ_RATE                         RATE,                ");
                        strBuilder.Append("     cont6.FCL_REQ_RATE                         BKGRATE,             ");
                        strBuilder.Append("     tran6.LCL_BASIS                            BASISPK,               ");
                        strBuilder.Append("     '1'                                        PYMT_TYPE,FRT6.Credit  ");
                        strBuilder.Append("    from                                                             ");
                        strBuilder.Append("     TARIFF_MAIN_SEA_TBL            main6,                           ");
                        strBuilder.Append("     TARIFF_TRN_SEA_FCL_LCL         tran6,                           ");
                        strBuilder.Append("     TARIFF_TRN_SEA_CONT_DTL        cont6,                           ");
                        strBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt6,                            ");
                        strBuilder.Append("     OPERATOR_MST_TBL               COOMT,                           ");
                        strBuilder.Append("     DIMENTION_UNIT_MST_TBL         CCUOM,                           ");
                        strBuilder.Append("     PORT_MST_TBL                   COPL,                            ");
                        strBuilder.Append("     PORT_MST_TBL                   COPD,                            ");
                        strBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr6                            ");
                        strBuilder.Append("     where " + strContRefNo.ToString() + " IS NOT NULL AND                       ");
                        strBuilder.Append("            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           ");
                        strBuilder.Append("     AND    cont6.TARIFF_TRN_SEA_FK = tran6.TARIFF_TRN_SEA_PK           ");
                        strBuilder.Append("     AND    tran6.PORT_MST_POL_FK              = COPL.PORT_MST_PK           ");
                        strBuilder.Append("     AND    tran6.PORT_MST_POD_FK              = COPD.PORT_MST_PK           ");
                        strBuilder.Append("     AND    main6.OPERATOR_MST_FK              = COOMT.OPERATOR_MST_PK (+)     ");
                        strBuilder.Append("     AND    tran6.LCL_BASIS                    = CCUOM.DIMENTION_UNIT_MST_PK ");
                        strBuilder.Append("     AND    main6.CARGO_TYPE            = 2 and  main6.Active=1               ");
                        strBuilder.Append("     AND    main6.ACTIVE                = 1                                  ");
                        strBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK       ");
                        strBuilder.Append("     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK              ");
                        strBuilder.Append("     AND    main6.COMMODITY_GROUP_FK    = " + intCommodityPK);
                        strBuilder.Append("     AND    tran6.PORT_MST_POL_FK       = " + strPOL);
                        strBuilder.Append("     AND    tran6.PORT_MST_POD_FK       = " + strPOD + arrCCondition[4]);

                        if (!string.IsNullOrEmpty(CustContRefNr))
                        {
                            //strBuilder.Append(" AND main6.cont_ref_no = '" & CustContRefNr & "'  " & vbCrLf)
                        }
                        else
                        {
                            strBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 ");
                            strBuilder.Append("            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       ");
                        }
                        strBuilder.Append("     AND TRAN6.FREIGHT_ELEMENT_MST_FK NOT IN ( SELECT FRTCHECK.FREIGHT_ELEMENT_MST_PK ");
                        strBuilder.Append("     FROM FREIGHT_ELEMENT_MST_TBL FRTCHECK ");
                        strBuilder.Append("     WHERE FRTCHECK.FREIGHT_ELEMENT_ID LIKE ('%BOF') ) ");
                        strBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements.ToString() + ") ");
                        strBuilder.Append("     AND  " + strSurcharge.ToString() + " = 1 ");
                    }
                    strBuilder.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //Snigdharani - 23/12/2008 - order by preference
                    strBuilder.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append("     ORDER BY FRT.PREFERENCE ");
                }
                else
                {
                    strContRefNo.Append("(   Select    DISTINCT  CONT_REF_NO ");
                    strContRefNo.Append("    from                                                             ");
                    strContRefNo.Append("     CONT_CUST_SEA_TBL              main7,                           ");
                    strContRefNo.Append("     CONT_CUST_TRN_SEA_TBL          tran7                            ");
                    strContRefNo.Append("    where                                                                ");
                    strContRefNo.Append("            main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK              ");
                    strContRefNo.Append("     AND    main7.CARGO_TYPE            = 1  and Main7.Active=1              ");
                    strContRefNo.Append("     AND    main7.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strContRefNo.Append("     AND    main7.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");

                    strContRefNo.Append("     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              ");
                    strContRefNo.Append("     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              ");

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strContRefNo.Append(" AND main7.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strContRefNo.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                        strContRefNo.Append("            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        ");
                    }
                    strContRefNo.Append(" AND    tran7.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        ");
                    strContRefNo.Append(" AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK )    ");

                    strFreightElements.Append(" (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK ");
                    strFreightElements.Append("    from                                                             ");
                    strFreightElements.Append("     CONT_CUST_SEA_TBL              main8,                           ");
                    strFreightElements.Append("     CONT_CUST_TRN_SEA_TBL          tran8,                           ");
                    strFreightElements.Append("     CONT_SUR_CHRG_SEA_TBL          frtd8                            ");
                    strFreightElements.Append("    where                                                                ");
                    strFreightElements.Append("            main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              ");
                    strFreightElements.Append("     AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          ");
                    strFreightElements.Append("     AND    main8.CARGO_TYPE            = 1 and  main8.Active=1                                  ");
                    strFreightElements.Append("     AND    main8.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strFreightElements.Append("     AND    (main8.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strFreightElements.Append("    OR   main8.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strFreightElements.Append("   FROM AFFILIATE_CUSTOMER_DTLS A, CONT_CUST_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strFreightElements.Append("   WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strFreightElements.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strFreightElements.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strFreightElements.Append("     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               ");
                    strFreightElements.Append("     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               ");
                    strFreightElements.Append("     AND    tran8.CONTAINER_TYPE_MST_FK = cont6.CONTAINER_TYPE_MST_FK         ");

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strFreightElements.Append(" AND main8.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strFreightElements.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                        strFreightElements.Append("            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        ");
                    }
                    strFreightElements.Append("     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ");

                    strSurcharge.Append(" ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  ");
                    strSurcharge.Append("    from                                                             ");
                    strSurcharge.Append("     CONT_CUST_SEA_TBL              main9,                           ");
                    strSurcharge.Append("     CONT_CUST_TRN_SEA_TBL          tran9                            ");
                    strSurcharge.Append("    where                                                                ");
                    strSurcharge.Append("            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              ");
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 1  and  main9.Active=1                                 ");
                    strSurcharge.Append("     AND    main9.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strSurcharge.Append("     AND    (main9.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strSurcharge.Append("    OR   main9.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSurcharge.Append("   FROM AFFILIATE_CUSTOMER_DTLS A, CONT_CUST_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strSurcharge.Append("   WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strSurcharge.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSurcharge.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strSurcharge.Append("     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              ");
                    strSurcharge.Append("     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              ");
                    strSurcharge.Append("     AND    tran9.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        ");

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strSurcharge.Append(" AND main9.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strSurcharge.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                        strSurcharge.Append(" tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        ");
                    }
                    strSurcharge.Append("     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK       )   ");
                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.TYPE,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       abs(Q.BKGRATE),Q.BKGRATE TOTAL,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE,q.Credit");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("    Select     ");
                    strBuilder.Append(" tran2.CONT_CUST_SEA_FK    TRNTYPEFK,  ");
                    strBuilder.Append(" main2.CONT_REF_NO     REFNO,   ");
                    strBuilder.Append(" CCCTMT.CONTAINER_TYPE_MST_ID Type, ");
                    strBuilder.Append(" CCCMT.COMMODITY_ID COMMODITY, ");
                    strBuilder.Append(" CCPL.PORT_MST_PK POLPK, ");
                    strBuilder.Append(" CCPL.PORT_ID POL, ");
                    strBuilder.Append(" CCPD.PORT_MST_PK PODPK, ");
                    strBuilder.Append(" CCPD.PORT_ID POD, ");
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_MST_PK, ");
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_ID, ");
                    strBuilder.Append("  DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                    strBuilder.Append(" 'true' SEL,  ");
                    strBuilder.Append(" curr2.CURRENCY_MST_PK, ");
                    strBuilder.Append(" curr2.CURRENCY_ID, ");
                    strBuilder.Append(" (CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN");
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE ");
                    strBuilder.Append(" Else ");
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE ");
                    strBuilder.Append(" END) AS RATE, ");
                    strBuilder.Append(" (ABS(CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN");
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE ");
                    strBuilder.Append(" Else ");
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE ");
                    strBuilder.Append(" END)*DECODE( FRT2.CREDIT ,NULL,1,0,-1,1,1)) AS BKGRATE, ");
                    strBuilder.Append(" tran2.LCL_BASIS BASISPK, ");
                    strBuilder.Append(" '1' AS PYMT_TYPE, FRT2.Credit   ");
                    strBuilder.Append(" from ");
                    strBuilder.Append(" CONT_CUST_SEA_TBL main2, ");
                    strBuilder.Append(" CONT_CUST_TRN_SEA_TBL tran2, ");
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL frt2, ");
                    strBuilder.Append(" OPERATOR_MST_TBL CCOMT, ");
                    strBuilder.Append(" CONTAINER_TYPE_MST_TBL CCCTMT, ");
                    strBuilder.Append(" COMMODITY_MST_TBL CCCMT, ");
                    strBuilder.Append(" PORT_MST_TBL CCPL, ");
                    strBuilder.Append(" PORT_MST_TBL CCPD, ");
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL curr2 ");
                    strBuilder.Append(" where ");
                    strBuilder.Append(" main2.CONT_CUST_SEA_PK = tran2.CONT_CUST_SEA_FK ");
                    strBuilder.Append(" AND FRT2.FREIGHT_ELEMENT_ID LIKE '%BOF' ");
                    strBuilder.Append(" AND TRAN2.CURRENCY_MST_FK= curr2.CURRENCY_MST_PK   ");
                    strBuilder.Append(" AND tran2.PORT_MST_POL_FK=CCPL.PORT_MST_PK ");
                    strBuilder.Append(" AND tran2.PORT_MST_POD_FK=CCPD.PORT_MST_PK ");
                    strBuilder.Append(" AND main2.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)");
                    strBuilder.Append(" AND main2.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) ");
                    strBuilder.Append(" AND tran2.CONTAINER_TYPE_MST_FK=CCCTMT.CONTAINER_TYPE_MST_PK");
                    strBuilder.Append(" AND main2.CARGO_TYPE  = 1 and main2.active=1       ");
                    strBuilder.Append(" AND main2.STATUS   = 2       ");
                    strBuilder.Append(" AND main2.COMMODITY_GROUP_MST_FK = " + intCommodityPK);
                    strBuilder.Append(" AND (main2.CUSTOMER_MST_FK  = " + intCustomerPK);
                    strBuilder.Append("    OR   main2.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strBuilder.Append("   FROM AFFILIATE_CUSTOMER_DTLS A, CONT_CUST_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strBuilder.Append("   WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strBuilder.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strBuilder.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strBuilder.Append(" AND tran2.PORT_MST_POL_FK = " + strPOL);
                    strBuilder.Append(" AND tran2.PORT_MST_POD_FK = " + strPOD + " " + arrCCondition[3]);

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strBuilder.Append(" AND main2.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN   ");
                        strBuilder.Append(" tran2.VALID_FROM AND NVL(tran2.VALID_TO,NULL_DATE_FORMAT)  ");
                    }
                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND CCCTMT.CONTAINER_TYPE_MST_PK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN BTRN WHERE BTRN.BOOKING_MST_FK =" + BookingPK + ")");
                    }
                    strBuilder.Append(" UNION ");
                    strBuilder.Append(" Select ");
                    strBuilder.Append(" tran2.CONT_CUST_SEA_FK                    TRNTYPEFK,            ");
                    strBuilder.Append(" main2.CONT_REF_NO                          REFNO,               ");
                    strBuilder.Append(" CCCTMT.CONTAINER_TYPE_MST_ID               TYPE,                ");
                    strBuilder.Append(" CCCMT.COMMODITY_ID                         COMMODITY,           ");
                    strBuilder.Append(" CCPL.PORT_MST_PK POLPK                     ,              ");
                    strBuilder.Append(" CCPL.PORT_ID                               POL,                 ");
                    strBuilder.Append(" CCPD.PORT_MST_PK PODPK              ,              ");
                    strBuilder.Append(" CCPD.PORT_ID                               POD,                 ");
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_MST_PK,              ");
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_ID                    ,              ");
                    strBuilder.Append("  DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                    strBuilder.Append(" DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,       ");
                    strBuilder.Append(" curr2.CURRENCY_MST_PK                      ,             ");
                    strBuilder.Append(" curr2.CURRENCY_ID                          ,             ");
                    strBuilder.Append(" frtd2.APP_SURCHARGE_AMT                    RATE,                ");
                    strBuilder.Append(" (ABS(frtd2.APP_SURCHARGE_AMT)*DECODE( FRT2.CREDIT ,NULL,1,0,-1,1,1))                    BKGRATE,                ");
                    strBuilder.Append(" tran2.LCL_BASIS                            BASISPK,                   ");
                    strBuilder.Append(" '1' AS PYMT_TYPE , FRT2.Credit             ");
                    strBuilder.Append(" from                                                             ");
                    strBuilder.Append(" CONT_CUST_SEA_TBL              main2,                           ");
                    strBuilder.Append(" CONT_CUST_TRN_SEA_TBL          tran2,                           ");
                    strBuilder.Append(" CONT_SUR_CHRG_SEA_TBL          frtd2,                           ");
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL        frt2,                            ");
                    strBuilder.Append(" OPERATOR_MST_TBL               CCOMT,                           ");
                    strBuilder.Append(" CONTAINER_TYPE_MST_TBL         CCCTMT,                          ");
                    strBuilder.Append(" COMMODITY_MST_TBL              CCCMT,                           ");
                    strBuilder.Append(" PORT_MST_TBL                   CCPL,                            ");
                    strBuilder.Append(" PORT_MST_TBL                   CCPD,                            ");
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL          curr2                            ");
                    strBuilder.Append(" where                                                                ");
                    strBuilder.Append(" main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              ");
                    strBuilder.Append(" AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK          ");
                    strBuilder.Append(" AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK        ");
                    strBuilder.Append(" AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK               ");
                    strBuilder.Append(" AND    tran2.PORT_MST_POL_FK=CCPL.PORT_MST_PK ");
                    strBuilder.Append(" AND    tran2.PORT_MST_POD_FK=CCPD.PORT_MST_PK ");
                    strBuilder.Append(" AND    main2.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)");
                    strBuilder.Append(" AND    main2.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)");
                    strBuilder.Append(" AND    tran2.CONTAINER_TYPE_MST_FK=CCCTMT.CONTAINER_TYPE_MST_PK");
                    strBuilder.Append(" AND    main2.CARGO_TYPE            = 1  and  main2.Active=1              ");
                    strBuilder.Append(" AND    main2.STATUS                = 2                                   ");
                    strBuilder.Append(" AND    main2.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strBuilder.Append(" AND    (main2.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strBuilder.Append("    OR   main2.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strBuilder.Append("   FROM AFFILIATE_CUSTOMER_DTLS A, CONT_CUST_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strBuilder.Append("   WHERE A.REFERENCE_MST_FK = CT.CONT_CUST_SEA_PK ");
                    strBuilder.Append("  AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strBuilder.Append("  AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strBuilder.Append(" AND    tran2.PORT_MST_POL_FK      = " + strPOL);
                    strBuilder.Append(" AND    tran2.PORT_MST_POD_FK      = " + strPOD + arrCCondition[3]);

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strBuilder.Append(" AND main2.cont_ref_no = '" + CustContRefNr + "'  ");
                    }
                    else
                    {
                        strBuilder.Append(" AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                        strBuilder.Append(" tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)        ");
                    }
                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND CCCTMT.CONTAINER_TYPE_MST_PK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN BTRN WHERE BTRN.BOOKING_MST_FK =" + BookingPK + ")");
                    }
                    if (EBKGSTATUS == 0)
                    {
                        strBuilder.Append(" UNION  ");
                        strBuilder.Append(" Select            ");
                        strBuilder.Append(" NULL                                       TRNTYPEFK,           ");
                        strBuilder.Append(" " + strContRefNo.ToString() + "                       REFNO,               ");
                        strBuilder.Append(" COCTMT.CONTAINER_TYPE_MST_ID               TYPE,                ");
                        strBuilder.Append(" NULL                                       COMMODITY,           ");
                        strBuilder.Append(" COPL.PORT_MST_PK POLPK              ,              ");
                        strBuilder.Append(" COPL.PORT_ID                               POL,                 ");
                        strBuilder.Append(" COPD.PORT_MST_PK PODPK               ,              ");
                        strBuilder.Append(" COPD.PORT_ID                               POD,                 ");
                        strBuilder.Append(" frt6.FREIGHT_ELEMENT_MST_PK               ,          ");
                        strBuilder.Append(" frt6.FREIGHT_ELEMENT_ID                    ,          ");
                        strBuilder.Append("  DECODE(frt6.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                        strBuilder.Append(" DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 ");
                        strBuilder.Append(" curr6.CURRENCY_MST_PK                      ,        ");
                        strBuilder.Append(" curr6.CURRENCY_ID                          ,        ");
                        strBuilder.Append(" cont6.FCL_REQ_RATE                         RATE,                ");
                        strBuilder.Append(" (ABS(cont6.FCL_REQ_RATE)*DECODE(FRT6.CREDIT ,NULL,1,0,-1,1,1))     BKGRATE,             ");
                        strBuilder.Append(" tran6.LCL_BASIS                            BASISPK,               ");
                        strBuilder.Append(" '1'                                        PYMT_TYPE,FRT6.Credit           ");
                        strBuilder.Append(" from                                                             ");
                        strBuilder.Append(" TARIFF_MAIN_SEA_TBL            main6,                           ");
                        strBuilder.Append(" TARIFF_TRN_SEA_FCL_LCL         tran6,                           ");
                        strBuilder.Append(" TARIFF_TRN_SEA_CONT_DTL cont6,                       ");
                        strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL        frt6,                            ");
                        strBuilder.Append(" OPERATOR_MST_TBL               COOMT,                           ");
                        strBuilder.Append(" CONTAINER_TYPE_MST_TBL         COCTMT,                          ");
                        strBuilder.Append(" PORT_MST_TBL                   COPL,                            ");
                        strBuilder.Append(" PORT_MST_TBL                   COPD,                            ");
                        strBuilder.Append(" CURRENCY_TYPE_MST_TBL          curr6                            ");
                        strBuilder.Append(" where " + strContRefNo.ToString() + " IS NOT NULL AND                       ");
                        strBuilder.Append(" main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           ");
                        strBuilder.Append(" AND cont6.TARIFF_TRN_SEA_FK = tran6.TARIFF_TRN_SEA_PK           ");
                        //Snigdharani
                        strBuilder.Append(" AND    tran6.PORT_MST_POL_FK              = COPL.PORT_MST_PK           ");
                        strBuilder.Append(" AND    tran6.PORT_MST_POD_FK              = COPD.PORT_MST_PK           ");
                        strBuilder.Append(" AND    main6.OPERATOR_MST_FK              = COOMT.OPERATOR_MST_PK   (+)");
                        strBuilder.Append(" AND    cont6.CONTAINER_TYPE_MST_FK        = COCTMT.CONTAINER_TYPE_MST_PK");
                        strBuilder.Append(" AND    main6.CARGO_TYPE            = 1                                  ");
                        strBuilder.Append(" AND    main6.ACTIVE                = 1                                  ");
                        strBuilder.Append(" AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK       ");
                        strBuilder.Append(" AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK              ");
                        strBuilder.Append(" AND    main6.COMMODITY_GROUP_FK    = " + intCommodityPK);
                        strBuilder.Append(" AND    tran6.PORT_MST_POL_FK       = " + strPOL);
                        strBuilder.Append(" AND    tran6.PORT_MST_POD_FK       = " + strPOD + arrCCondition[4]);

                        if (!string.IsNullOrEmpty(CustContRefNr))
                        {
                            // strBuilder.Append(" AND main6.cont_ref_no = '" & CustContRefNr & "'  " & vbCrLf)
                        }
                        else
                        {
                            strBuilder.Append(" AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 ");
                            strBuilder.Append(" tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       ");
                        }
                        strBuilder.Append(" AND TRAN6.FREIGHT_ELEMENT_MST_FK NOT IN ( SELECT FRTCHECK.FREIGHT_ELEMENT_MST_PK ");
                        strBuilder.Append(" FROM FREIGHT_ELEMENT_MST_TBL FRTCHECK ");
                        strBuilder.Append(" WHERE FRTCHECK.FREIGHT_ELEMENT_ID LIKE ('%BOF') ) ");
                        strBuilder.Append(" AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements.ToString() + ") ");
                        strBuilder.Append(" AND  " + strSurcharge.ToString() + " = 1                  ");
                    }
                    strBuilder.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //Snigdharani - 23/12/2008 - order by preference
                    strBuilder.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append("     ORDER BY FRT.PREFERENCE ");
                }
                return strBuilder.ToString();
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

        public object funSRRFreight(ArrayList arrCCondition, Int32 intCustomerPK, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl)
        {
            try
            {
                System.Text.StringBuilder strContRefNo = new System.Text.StringBuilder();
                System.Text.StringBuilder strFreightElements = new System.Text.StringBuilder();
                System.Text.StringBuilder strSurcharge = new System.Text.StringBuilder();
                System.Text.StringBuilder strSRRSBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strContRefNo.Append(" (   Select    DISTINCT  srr_ref_no ");
                    strContRefNo.Append("    from                                                             ");
                    strContRefNo.Append("     SRR_SEA_TBL              main7,                           ");
                    strContRefNo.Append("     SRR_TRN_SEA_TBL          tran7                            ");
                    strContRefNo.Append("    where                                                                ");
                    strContRefNo.Append("            tran7.srr_sea_fk=main7.srr_sea_pk              ");
                    strContRefNo.Append("     AND    main7.CARGO_TYPE            = 2                                   ");
                    strContRefNo.Append("     AND    main7.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strContRefNo.Append("     AND    main7.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strContRefNo.Append("     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              ");
                    strContRefNo.Append("     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              ");
                    strContRefNo.Append("     AND    TRAN7.LCL_BASIS=TRAN6.LCL_BASIS                                     ");
                    strContRefNo.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                    strContRefNo.Append("            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        ");
                    strContRefNo.Append("     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    ");

                    strFreightElements.Append(" (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK ");
                    strFreightElements.Append("    from                                                             ");
                    strFreightElements.Append("     srr_sea_tbl              main8,                           ");
                    strFreightElements.Append("     srr_trn_sea_tbl          tran8,                           ");
                    strFreightElements.Append("     srr_sur_chrg_sea_tbl          frtd8                            ");
                    strFreightElements.Append("    where                                                                ");
                    strFreightElements.Append("            tran8.srr_sea_fk=main8.srr_sea_pk                ");
                    strFreightElements.Append("     and frtd8.srr_trn_sea_fk=tran8.srr_trn_sea_pk          ");
                    strFreightElements.Append("     AND    main8.CARGO_TYPE            = 2                                   ");
                    strFreightElements.Append("     AND    main8.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strFreightElements.Append("     AND    (main8.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strFreightElements.Append("    OR   main8.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strFreightElements.Append("    FROM AFFILIATE_CUSTOMER_DTLS A, srr_sea_tbl CT, CUSTOMER_MST_TBL C ");
                    strFreightElements.Append("    WHERE A.REFERENCE_MST_FK = CT.srr_sea_pk ");
                    strFreightElements.Append("    AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strFreightElements.Append("    AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strFreightElements.Append("     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               ");
                    strFreightElements.Append("     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               ");
                    strFreightElements.Append("     AND    TRAN8.LCL_BASIS=TRAN6.LCL_BASIS         ");
                    strFreightElements.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                    strFreightElements.Append("            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        ");
                    strFreightElements.Append("     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ");

                    strSurcharge.Append(" ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  ");
                    strSurcharge.Append("    from                                                             ");
                    strSurcharge.Append("     srr_sea_tbl main9,                                ");
                    strSurcharge.Append("     srr_trn_sea_tbl tran9                                ");
                    strSurcharge.Append("    where                                                                ");
                    strSurcharge.Append("            tran9.srr_sea_fk=main9.srr_sea_pk             ");
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 1                                   ");
                    strSurcharge.Append("     AND    main9.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strSurcharge.Append("     AND    (main9.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strSurcharge.Append("    OR   main9.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSurcharge.Append("    FROM AFFILIATE_CUSTOMER_DTLS A, SRR_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strSurcharge.Append("    WHERE A.REFERENCE_MST_FK = CT.srr_sea_pk ");
                    strSurcharge.Append("    AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSurcharge.Append("    AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strSurcharge.Append("     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              ");
                    strSurcharge.Append("     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              ");
                    strSurcharge.Append("     AND    TRAN9.LCL_BASIS=TRAN6.LCL_BASIS                              ");
                    strSurcharge.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                    strSurcharge.Append("            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        ");
                    strSurcharge.Append("     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK         )   ");

                    strSRRSBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strSRRSBuilder.Append("       Q.REFNO,");
                    strSRRSBuilder.Append("       Q.BASIS,");
                    strSRRSBuilder.Append("       Q.COMMODITY,");
                    strSRRSBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strSRRSBuilder.Append("       Q.POL,");
                    strSRRSBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strSRRSBuilder.Append("       Q.POD,");
                    strSRRSBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strSRRSBuilder.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                    strSRRSBuilder.Append("       Q.SEL,");
                    strSRRSBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strSRRSBuilder.Append("       Q.CURRENCY_ID,");
                    strSRRSBuilder.Append("       Q.MIN_RATE,");
                    strSRRSBuilder.Append("       Q.RATE,");
                    strSRRSBuilder.Append("       abs(Q.BKGRATE) BKGRATE,(abs(Q.BKGRATE)*DECODE(q.Credit,NULL,1,0,-1,1,1)) TOTAL,");
                    strSRRSBuilder.Append("       Q.BASISPK,");
                    strSRRSBuilder.Append("       Q.PYMT_TYPE,q.credit");
                    strSRRSBuilder.Append("       FROM  (");

                    strSRRSBuilder.Append("  Select ");
                    strSRRSBuilder.Append("     SRRTRN.SRR_TRN_SEA_PK    TRNTYPEFK,   ");
                    strSRRSBuilder.Append("     SRRHDR.SRR_REF_NO REFNO,   ");
                    strSRRSBuilder.Append("     SRRUOM.DIMENTION_ID AS BASIS,     ");
                    strSRRSBuilder.Append("     CCCMT.COMMODITY_ID     COMMODITY,   ");
                    strSRRSBuilder.Append("     CCPL.PORT_MST_PK POLPK,   ");
                    strSRRSBuilder.Append("     CCPL.PORT_ID POL,    ");
                    strSRRSBuilder.Append("     CCPD.PORT_MST_PK    PODPK,   ");
                    strSRRSBuilder.Append("     CCPD.PORT_ID POD,    ");
                    strSRRSBuilder.Append("     FRT2.FREIGHT_ELEMENT_MST_PK,   ");
                    strSRRSBuilder.Append("     frt2.FREIGHT_ELEMENT_ID    ,   ");
                    strSRRSBuilder.Append("    DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   ");
                    strSRRSBuilder.Append("     'true' SEL,");
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK,");
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID     ,   ");
                    strSRRSBuilder.Append("  NULL  MIN_RATE, ");
                    strSRRSBuilder.Append("   (CASE WHEN SRRTRN.APPROVED_BOF_RATE IS NOT NULL THEN ");
                    strSRRSBuilder.Append("     SRRTRN.APPROVED_BOF_RATE ");
                    strSRRSBuilder.Append("     Else ");
                    strSRRSBuilder.Append("     SRRTRN.CURRENT_BOF_RATE ");
                    strSRRSBuilder.Append("     END) ");
                    strSRRSBuilder.Append("     AS RATE, ");
                    strSRRSBuilder.Append("     (CASE WHEN SRRTRN.APPROVED_BOF_RATE IS NOT NULL THEN ");
                    strSRRSBuilder.Append("     SRRTRN.APPROVED_BOF_RATE ");
                    strSRRSBuilder.Append("     ELSE ");
                    strSRRSBuilder.Append("     SRRTRN.CURRENT_BOF_RATE ");
                    strSRRSBuilder.Append("     END) AS BKGRATE, ");
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,    ");
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE , FRT2.Credit  ");
                    strSRRSBuilder.Append("     from           ");
                    strSRRSBuilder.Append("     SRR_SEA_TBL    SRRHDR,     ");
                    strSRRSBuilder.Append("     SRR_TRN_SEA_TBL SRRTRN,        ");
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL  frt2,     ");
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL   CCOMT,     ");
                    strSRRSBuilder.Append("     DIMENTION_UNIT_MST_TBL   SRRUOM,          ");
                    strSRRSBuilder.Append("     COMMODITY_MST_TBL   CCCMT,     ");
                    strSRRSBuilder.Append("     PORT_MST_TBL    CCPL,     ");
                    strSRRSBuilder.Append("     PORT_MST_TBL    CCPD,     ");
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL  curr2     ");
                    strSRRSBuilder.Append("     where           ");
                    strSRRSBuilder.Append("     SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK   ");
                    strSRRSBuilder.Append("     AND FRT2.FREIGHT_ELEMENT_ID LIKE '%BOF'");
                    strSRRSBuilder.Append("     AND SRRTRN.CURRENCY_MST_FK= curr2.CURRENCY_MST_PK  ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK=CCPL.PORT_MST_PK ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK=CCPD.PORT_MST_PK ");
                    strSRRSBuilder.Append("     AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)");
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)");
                    strSRRSBuilder.Append("     AND SRRTRN.LCL_BASIS=SRRUOM.DIMENTION_UNIT_MST_PK ");
                    strSRRSBuilder.Append("     AND SRRHDR.CARGO_TYPE=2       ");
                    strSRRSBuilder.Append("     AND SRRHDR.STATUS = 1       ");
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK);
                    strSRRSBuilder.Append("     AND (SRRHDR.CUSTOMER_MST_FK = " + intCustomerPK);
                    strSRRSBuilder.Append("    OR   SRRHDR.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSRRSBuilder.Append("    FROM AFFILIATE_CUSTOMER_DTLS A, SRR_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strSRRSBuilder.Append("    WHERE A.REFERENCE_MST_FK = CT.srr_sea_pk ");
                    strSRRSBuilder.Append("    AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSRRSBuilder.Append("    AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK = " + strPOL);
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK= " + strPOD);
                    strSRRSBuilder.Append("     AND TO_DATE('" + strSDate + " ','" + dateFormat + "') BETWEEN   ");
                    strSRRSBuilder.Append("     SRRHDR.VALID_FROM AND NVL(SRRTRN.VALID_TO,NULL_DATE_FORMAT)  ");
                    strSRRSBuilder.Append("     UNION ");
                    strSRRSBuilder.Append("     SELECT ");
                    strSRRSBuilder.Append("     SRRTRN.SRR_TRN_SEA_PK                   TRNTYPEFK,              ");
                    strSRRSBuilder.Append("     SRRHDR.SRR_REF_NO REFNO,                  ");
                    strSRRSBuilder.Append("     SRRUOM.DIMENTION_ID AS BASIS,                   ");
                    strSRRSBuilder.Append("     CCCMT.COMMODITY_ID                         COMMODITY,           ");
                    strSRRSBuilder.Append("     CCPL.PORT_MST_PK POLPK                            ,              ");
                    strSRRSBuilder.Append("     CCPL.PORT_ID                               POL,                 ");
                    strSRRSBuilder.Append("     CCPD.PORT_MST_PK PODPK            ,              ");
                    strSRRSBuilder.Append("     CCPD.PORT_ID                               POD,                 ");
                    strSRRSBuilder.Append("     SRRSUR.FREIGHT_ELEMENT_MST_FK,              ");
                    strSRRSBuilder.Append("     frt2.FREIGHT_ELEMENT_ID                    ,              ");
                    strSRRSBuilder.Append("    DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   ");
                    strSRRSBuilder.Append("     DECODE(SRRSUR.CHECK_FOR_ALL_IN_RT, 1,'true','false') SEL,         ");
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK,                              ");
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID                          ,             ");
                    strSRRSBuilder.Append("  NULL  MIN_RATE, ");
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT RATE,               ");
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT AS BKGRATE,  ");
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,                 ");
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE , FRT2.Credit              ");
                    strSRRSBuilder.Append("     from                                                             ");
                    strSRRSBuilder.Append("     SRR_SEA_TBL                 SRRHDR,                             ");
                    strSRRSBuilder.Append("     SRR_TRN_SEA_TBL SRRTRN,                                         ");
                    strSRRSBuilder.Append("     SRR_SUR_CHRG_SEA_TBL SRRSUR,                                    ");
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt2,                            ");
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL               CCOMT,                           ");
                    strSRRSBuilder.Append("     DIMENTION_UNIT_MST_TBL              SRRUOM,                          ");
                    strSRRSBuilder.Append("     COMMODITY_MST_TBL              CCCMT,                           ");
                    strSRRSBuilder.Append("     PORT_MST_TBL                   CCPL,                            ");
                    strSRRSBuilder.Append("     PORT_MST_TBL                   CCPD,                            ");
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr2                            ");
                    strSRRSBuilder.Append("     where                                                            ");
                    strSRRSBuilder.Append("     SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK              ");
                    strSRRSBuilder.Append("     AND    SRRSUR.SRR_TRN_SEA_FK=SRRTRN.SRR_TRN_SEA_PK          ");
                    strSRRSBuilder.Append("     AND SRRSUR.FREIGHT_ELEMENT_MST_FK= frt2.FREIGHT_ELEMENT_MST_PK       ");
                    strSRRSBuilder.Append("     AND SRRSUR.CURRENCY_MST_FK = curr2.CURRENCY_MST_PK               ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK=CCPL.PORT_MST_PK  ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK=CCPD.PORT_MST_PK  ");
                    strSRRSBuilder.Append("     AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)");
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) ");
                    strSRRSBuilder.Append("     AND SRRTRN.LCL_BASIS=SRRUOM.DIMENTION_UNIT_MST_PK ");
                    strSRRSBuilder.Append("     AND SRRHDR.CARGO_TYPE=2                                   ");
                    strSRRSBuilder.Append("     AND SRRHDR.STATUS = 1                                        ");
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK + "       ");
                    strSRRSBuilder.Append("     AND (SRRHDR.CUSTOMER_MST_FK =  " + intCustomerPK + "               ");
                    strSRRSBuilder.Append("    OR   SRRHDR.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSRRSBuilder.Append("    FROM AFFILIATE_CUSTOMER_DTLS A, SRR_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strSRRSBuilder.Append("    WHERE A.REFERENCE_MST_FK = CT.srr_sea_pk ");
                    strSRRSBuilder.Append("    AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSRRSBuilder.Append("    AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK = " + strPOL);
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK= " + strPOD + arrCCondition[6]);
                    strSRRSBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                    strSRRSBuilder.Append("            SRRHDR.VALID_FROM  AND   NVL(SRRTRN.VALID_TO,NULL_DATE_FORMAT)        ");
                    strSRRSBuilder.Append("    UNION  ");
                    strSRRSBuilder.Append("   Select            ");
                    strSRRSBuilder.Append("     NULL                                       TRNTYPEFK,           ");
                    strSRRSBuilder.Append("     " + strContRefNo.ToString() + "                       REFNO,               ");
                    strSRRSBuilder.Append("     SRRUOM.DIMENTION_ID AS                     BASIS,               ");
                    strSRRSBuilder.Append("     NULL                                       COMMODITY,           ");
                    strSRRSBuilder.Append("     COPL.PORT_MST_PK POLPK             ,              ");
                    strSRRSBuilder.Append("     COPL.PORT_ID                               POL,                 ");
                    strSRRSBuilder.Append("     COPD.PORT_MST_PK PODPK              ,              ");
                    strSRRSBuilder.Append("     COPD.PORT_ID                               POD,                 ");
                    strSRRSBuilder.Append("     frt6.FREIGHT_ELEMENT_MST_PK               ,          ");
                    strSRRSBuilder.Append("     frt6.FREIGHT_ELEMENT_ID                    ,          ");
                    strSRRSBuilder.Append("    DECODE(FRT6.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   ");
                    strSRRSBuilder.Append("     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 ");
                    strSRRSBuilder.Append("     curr6.CURRENCY_MST_PK                      ,        ");
                    strSRRSBuilder.Append("     curr6.CURRENCY_ID                          ,        ");
                    strSRRSBuilder.Append("  NULL  MIN_RATE, ");
                    strSRRSBuilder.Append("     TRAN6.LCL_TARIFF_RATE                      RATE,                ");
                    strSRRSBuilder.Append("     TRAN6.LCL_TARIFF_RATE                      BKGRATE,             ");
                    strSRRSBuilder.Append("     tran6.LCL_BASIS                            BASISPK,               ");
                    strSRRSBuilder.Append("     '1'                                        PYMT_TYPE ,FRT6.Credit           ");
                    strSRRSBuilder.Append("    from                                                             ");
                    strSRRSBuilder.Append("     TARIFF_MAIN_SEA_TBL            main6,                           ");
                    strSRRSBuilder.Append("     TARIFF_TRN_SEA_FCL_LCL         tran6,                           ");
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt6,                            ");
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL               COOMT,                           ");
                    strSRRSBuilder.Append("     DIMENTION_UNIT_MST_TBL         SRRUOM,                                         ");
                    strSRRSBuilder.Append("     PORT_MST_TBL                   COPL,                            ");
                    strSRRSBuilder.Append("     PORT_MST_TBL                   COPD,                            ");
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr6                            ");
                    strSRRSBuilder.Append("     where " + strContRefNo.ToString() + " IS NOT NULL AND                       ");
                    strSRRSBuilder.Append("            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           ");
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POL_FK              = COPL.PORT_MST_PK           ");
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POD_FK              = COPD.PORT_MST_PK           ");
                    strSRRSBuilder.Append("     AND    main6.OPERATOR_MST_FK              = COOMT.OPERATOR_MST_PK     (+) ");
                    strSRRSBuilder.Append("     AND    TRAN6.LCL_BASIS                    = SRRUOM.DIMENTION_UNIT_MST_PK    ");
                    strSRRSBuilder.Append("     AND    main6.CARGO_TYPE            = 2                                  ");
                    strSRRSBuilder.Append("     AND    main6.ACTIVE                = 1                                  ");
                    strSRRSBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK       ");
                    strSRRSBuilder.Append("     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK              ");
                    strSRRSBuilder.Append("     AND    main6.COMMODITY_GROUP_FK    = " + intCommodityPK);
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POL_FK       = " + strPOL);
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POD_FK       = " + strPOD + arrCCondition[4]);
                    strSRRSBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 ");
                    strSRRSBuilder.Append("            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       ");
                    strSRRSBuilder.Append("     AND TRAN6.FREIGHT_ELEMENT_MST_FK NOT IN ( SELECT FRTCHECK.FREIGHT_ELEMENT_MST_PK ");
                    strSRRSBuilder.Append("     FROM FREIGHT_ELEMENT_MST_TBL FRTCHECK ");
                    strSRRSBuilder.Append("     WHERE FRTCHECK.FREIGHT_ELEMENT_ID LIKE ('%BOF') ) ");
                    strSRRSBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements.ToString() + ") ");
                    strSRRSBuilder.Append("     AND  " + strSurcharge.ToString() + " = 1                  ");
                    strSRRSBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //Snigdharani - 23/12/2008 - order by preference'
                    strSRRSBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strSRRSBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                else
                {
                    strContRefNo.Append(" (   Select    DISTINCT  srr_ref_no ");
                    strContRefNo.Append("    from                                                             ");
                    strContRefNo.Append("     SRR_SEA_TBL              main7,                           ");
                    strContRefNo.Append("     SRR_TRN_SEA_TBL          tran7                            ");
                    strContRefNo.Append("    where                                                                ");
                    strContRefNo.Append("            tran7.srr_sea_fk=main7.srr_sea_pk              ");
                    strContRefNo.Append("     AND    main7.CARGO_TYPE            = 1                                   ");
                    strContRefNo.Append("     AND    main7.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strContRefNo.Append("     AND    main7.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strContRefNo.Append("     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              ");
                    strContRefNo.Append("     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              ");
                    strContRefNo.Append("     AND    tran7.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        ");
                    strContRefNo.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                    strContRefNo.Append("            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        ");
                    strContRefNo.Append("     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    ");

                    strFreightElements.Append(" (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK ");
                    strFreightElements.Append("    from                                                             ");
                    strFreightElements.Append("     srr_sea_tbl              main8,                           ");
                    strFreightElements.Append("     srr_trn_sea_tbl          tran8,                           ");
                    strFreightElements.Append("     srr_sur_chrg_sea_tbl          frtd8                            ");
                    strFreightElements.Append("    where                                                                ");
                    strFreightElements.Append("            tran8.srr_sea_fk=main8.srr_sea_pk                ");
                    strFreightElements.Append("     and frtd8.srr_trn_sea_fk=tran8.srr_trn_sea_pk          ");
                    strFreightElements.Append("     AND    main8.CARGO_TYPE            = 1                                   ");
                    strFreightElements.Append("     AND    main8.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strFreightElements.Append("     AND   (main8.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strFreightElements.Append("    OR   main8.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strFreightElements.Append("    FROM AFFILIATE_CUSTOMER_DTLS A, SRR_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strFreightElements.Append("    WHERE A.REFERENCE_MST_FK = CT.srr_sea_pk ");
                    strFreightElements.Append("    AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strFreightElements.Append("    AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strFreightElements.Append("     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               ");
                    strFreightElements.Append("     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               ");
                    strFreightElements.Append("     AND    tran8.CONTAINER_TYPE_MST_FK = cont6.CONTAINER_TYPE_MST_FK         ");
                    strFreightElements.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                    strFreightElements.Append("            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        ");
                    strFreightElements.Append("     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK       )   ");

                    strSurcharge.Append(" ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  ");
                    strSurcharge.Append("    from                                                             ");
                    strSurcharge.Append("     srr_sea_tbl main9,                                ");
                    strSurcharge.Append("     srr_trn_sea_tbl tran9                                ");
                    strSurcharge.Append("    where                                                                ");
                    strSurcharge.Append("            tran9.srr_sea_fk=main9.srr_sea_pk             ");
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 1                                   ");
                    strSurcharge.Append("     AND    main9.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       ");
                    strSurcharge.Append("     AND    (main9.CUSTOMER_MST_FK       =  " + intCustomerPK + "               ");
                    strSurcharge.Append("    OR   main9.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSurcharge.Append("    FROM AFFILIATE_CUSTOMER_DTLS A, SRR_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strSurcharge.Append("    WHERE A.REFERENCE_MST_FK = CT.srr_sea_pk ");
                    strSurcharge.Append("    AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSurcharge.Append("    AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strSurcharge.Append("     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              ");
                    strSurcharge.Append("     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              ");
                    strSurcharge.Append("     AND    tran9.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        ");
                    strSurcharge.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                    strSurcharge.Append("            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        ");
                    strSurcharge.Append("     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK         )   ");

                    strSRRSBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strSRRSBuilder.Append("       Q.REFNO,");
                    strSRRSBuilder.Append("       Q.TYPE,");
                    strSRRSBuilder.Append("       Q.COMMODITY,");
                    strSRRSBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strSRRSBuilder.Append("       Q.POL,");
                    strSRRSBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strSRRSBuilder.Append("       Q.POD,");
                    strSRRSBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strSRRSBuilder.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                    strSRRSBuilder.Append("       Q.SEL,");
                    strSRRSBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strSRRSBuilder.Append("       Q.CURRENCY_ID,");
                    strSRRSBuilder.Append("       Q.RATE,");
                    strSRRSBuilder.Append("       Q.BKGRATE,(abs(Q.BKGRATE)*DECODE(q.Credit,NULL,1,0,-1,1,1)) TOTAL,");
                    strSRRSBuilder.Append("       Q.BASISPK,");
                    strSRRSBuilder.Append("       Q.PYMT_TYPE,q.Credit");
                    strSRRSBuilder.Append("       FROM  (");

                    strSRRSBuilder.Append("  Select ");
                    strSRRSBuilder.Append("     SRRTRN.SRR_TRN_SEA_PK    TRNTYPEFK,   ");
                    strSRRSBuilder.Append("     SRRHDR.SRR_REF_NO REFNO,   ");
                    strSRRSBuilder.Append("     CCCTMT.CONTAINER_TYPE_MST_ID TYPE,    ");
                    strSRRSBuilder.Append("     CCCMT.COMMODITY_ID     COMMODITY,   ");
                    strSRRSBuilder.Append("     CCPL.PORT_MST_PK POLPK,   ");
                    strSRRSBuilder.Append("     CCPL.PORT_ID POL,    ");
                    strSRRSBuilder.Append("     CCPD.PORT_MST_PK    PODPK,   ");
                    strSRRSBuilder.Append("     CCPD.PORT_ID POD,    ");
                    strSRRSBuilder.Append("     FRT2.FREIGHT_ELEMENT_MST_PK,   ");
                    strSRRSBuilder.Append("     frt2.FREIGHT_ELEMENT_ID    ,   ");
                    strSRRSBuilder.Append("    DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   ");
                    strSRRSBuilder.Append("     'true' SEL,");
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK,");
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID     ,   ");
                    strSRRSBuilder.Append("     (CASE WHEN SRRTRN.APPROVED_BOF_RATE IS NOT NULL THEN ");
                    strSRRSBuilder.Append("     SRRTRN.APPROVED_BOF_RATE ");
                    strSRRSBuilder.Append("     Else ");
                    strSRRSBuilder.Append("     SRRTRN.CURRENT_BOF_RATE ");
                    strSRRSBuilder.Append("     END) ");
                    strSRRSBuilder.Append("     AS RATE, ");
                    strSRRSBuilder.Append("     (CASE WHEN SRRTRN.APPROVED_BOF_RATE IS NOT NULL THEN ");
                    strSRRSBuilder.Append("     SRRTRN.APPROVED_BOF_RATE ");
                    strSRRSBuilder.Append("     ELSE ");
                    strSRRSBuilder.Append("     SRRTRN.CURRENT_BOF_RATE ");
                    strSRRSBuilder.Append("     END) AS BKGRATE, ");
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,    ");
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE , FRT2.Credit ");
                    strSRRSBuilder.Append("     from           ");
                    strSRRSBuilder.Append("     SRR_SEA_TBL    SRRHDR,     ");
                    strSRRSBuilder.Append("     SRR_TRN_SEA_TBL SRRTRN,        ");
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL  frt2,     ");
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL   CCOMT,     ");
                    strSRRSBuilder.Append("     CONTAINER_TYPE_MST_TBL  CCCTMT,     ");
                    strSRRSBuilder.Append("     COMMODITY_MST_TBL   CCCMT,     ");
                    strSRRSBuilder.Append("     PORT_MST_TBL    CCPL,     ");
                    strSRRSBuilder.Append("     PORT_MST_TBL    CCPD,     ");
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL  curr2     ");
                    strSRRSBuilder.Append("     where           ");
                    strSRRSBuilder.Append("     SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK   ");
                    strSRRSBuilder.Append("     AND FRT2.FREIGHT_ELEMENT_ID LIKE '%BOF'");
                    strSRRSBuilder.Append("     AND SRRTRN.CURRENCY_MST_FK= curr2.CURRENCY_MST_PK  ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK=CCPL.PORT_MST_PK ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK=CCPD.PORT_MST_PK ");
                    strSRRSBuilder.Append("     AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)");
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)");
                    strSRRSBuilder.Append("     AND SRRTRN.CONTAINER_TYPE_MST_FK=CCCTMT.CONTAINER_TYPE_MST_PK");
                    strSRRSBuilder.Append("     AND SRRHDR.CARGO_TYPE=1       ");
                    strSRRSBuilder.Append("     AND SRRHDR.STATUS = 1       ");
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK);
                    strSRRSBuilder.Append("     AND (SRRHDR.CUSTOMER_MST_FK = " + intCustomerPK);
                    strSRRSBuilder.Append("    OR   SRRHDR.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSRRSBuilder.Append("    FROM AFFILIATE_CUSTOMER_DTLS A, SRR_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strSRRSBuilder.Append("    WHERE A.REFERENCE_MST_FK = CT.srr_sea_pk ");
                    strSRRSBuilder.Append("    AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSRRSBuilder.Append("    AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK = " + strPOL);
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK= " + strPOD + arrCCondition[6]);
                    strSRRSBuilder.Append("     AND TO_DATE('" + strSDate + " ','" + dateFormat + "') BETWEEN   ");
                    strSRRSBuilder.Append("     SRRHDR.VALID_FROM AND NVL(SRRTRN.VALID_TO,NULL_DATE_FORMAT)  ");
                    strSRRSBuilder.Append("     UNION ");
                    strSRRSBuilder.Append("    Select     ");
                    strSRRSBuilder.Append("     SRRTRN.SRR_TRN_SEA_PK                   TRNTYPEFK,              ");
                    strSRRSBuilder.Append("     SRRHDR.SRR_REF_NO REFNO,                  ");
                    strSRRSBuilder.Append("     CCCTMT.CONTAINER_TYPE_MST_ID   TYPE,                   ");
                    strSRRSBuilder.Append("     CCCMT.COMMODITY_ID                         COMMODITY,           ");
                    strSRRSBuilder.Append("     CCPL.PORT_MST_PK POLPK                         ,              ");
                    strSRRSBuilder.Append("     CCPL.PORT_ID                               POL,                 ");
                    strSRRSBuilder.Append("     CCPD.PORT_MST_PK PODPK          ,              ");
                    strSRRSBuilder.Append("     CCPD.PORT_ID                               POD,                 ");
                    strSRRSBuilder.Append("     SRRSUR.FREIGHT_ELEMENT_MST_FK,              ");
                    strSRRSBuilder.Append("     frt2.FREIGHT_ELEMENT_ID                    ,              ");
                    strSRRSBuilder.Append("    DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   ");
                    strSRRSBuilder.Append("     DECODE(SRRSUR.CHECK_FOR_ALL_IN_RT, 1,'true','false') SEL,         ");
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK,                              ");
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID                          ,             ");
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT RATE,               ");
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT AS BKGRATE,  ");
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,                 ");
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE,FRT2.Credit              ");
                    strSRRSBuilder.Append("     from                                                             ");
                    strSRRSBuilder.Append("     SRR_SEA_TBL                 SRRHDR,                             ");
                    strSRRSBuilder.Append("     SRR_TRN_SEA_TBL SRRTRN,                                         ");
                    strSRRSBuilder.Append("     SRR_SUR_CHRG_SEA_TBL SRRSUR,                                    ");
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt2,                            ");
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL               CCOMT,                           ");
                    strSRRSBuilder.Append("     CONTAINER_TYPE_MST_TBL         CCCTMT,                          ");
                    strSRRSBuilder.Append("     COMMODITY_MST_TBL              CCCMT,                           ");
                    strSRRSBuilder.Append("     PORT_MST_TBL                   CCPL,                            ");
                    strSRRSBuilder.Append("     PORT_MST_TBL                   CCPD,                            ");
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr2                            ");
                    strSRRSBuilder.Append("    where                                                                ");
                    strSRRSBuilder.Append("     SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK              ");
                    strSRRSBuilder.Append("     AND    SRRSUR.SRR_TRN_SEA_FK=SRRTRN.SRR_TRN_SEA_PK          ");
                    strSRRSBuilder.Append("     AND SRRSUR.FREIGHT_ELEMENT_MST_FK= frt2.FREIGHT_ELEMENT_MST_PK       ");
                    strSRRSBuilder.Append("     AND SRRSUR.CURRENCY_MST_FK = curr2.CURRENCY_MST_PK               ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK=CCPL.PORT_MST_PK  ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK=CCPD.PORT_MST_PK  ");
                    strSRRSBuilder.Append("     AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)");
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)");
                    strSRRSBuilder.Append("     AND SRRTRN.CONTAINER_TYPE_MST_FK=CCCTMT.CONTAINER_TYPE_MST_PK");
                    strSRRSBuilder.Append("     AND SRRHDR.CARGO_TYPE=1                                   ");
                    strSRRSBuilder.Append("     AND SRRHDR.STATUS = 1                                        ");
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK + "       ");
                    strSRRSBuilder.Append("     AND (SRRHDR.CUSTOMER_MST_FK =  " + intCustomerPK + "               ");
                    strSRRSBuilder.Append("    OR   SRRHDR.CUSTOMER_MST_FK IN (SELECT C.REF_GROUP_CUST_PK ");
                    strSRRSBuilder.Append("    FROM AFFILIATE_CUSTOMER_DTLS A, SRR_SEA_TBL CT, CUSTOMER_MST_TBL C ");
                    strSRRSBuilder.Append("    WHERE A.REFERENCE_MST_FK = CT.srr_sea_pk ");
                    strSRRSBuilder.Append("    AND A.CUST_MST_FK       =  " + intCustomerPK + " ");
                    strSRRSBuilder.Append("    AND A.CUST_MST_FK=C.CUSTOMER_MST_PK )) ");
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK = " + strPOL);
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK= " + strPOD + arrCCondition[6]);
                    strSRRSBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  ");
                    strSRRSBuilder.Append("            SRRHDR.VALID_FROM  AND   NVL(SRRTRN.VALID_TO,NULL_DATE_FORMAT)        ");
                    strSRRSBuilder.Append("    UNION  ");
                    strSRRSBuilder.Append("   Select            ");
                    strSRRSBuilder.Append("     NULL                                       TRNTYPEFK,           ");
                    strSRRSBuilder.Append("     " + strContRefNo.ToString() + "                       REFNO,               ");
                    strSRRSBuilder.Append("     COCTMT.CONTAINER_TYPE_MST_ID               TYPE,                ");
                    strSRRSBuilder.Append("     NULL                                       COMMODITY,           ");
                    strSRRSBuilder.Append("     COPL.PORT_MST_PK POLPK           ,              ");
                    strSRRSBuilder.Append("     COPL.PORT_ID                               POL,                 ");
                    strSRRSBuilder.Append("     COPD.PORT_MST_PK PODPK            ,              ");
                    strSRRSBuilder.Append("     COPD.PORT_ID                               POD,                 ");
                    strSRRSBuilder.Append("     frt6.FREIGHT_ELEMENT_MST_PK               ,          ");
                    strSRRSBuilder.Append("     frt6.FREIGHT_ELEMENT_ID                    ,          ");
                    strSRRSBuilder.Append("    DECODE(FRT6.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   ");
                    strSRRSBuilder.Append("     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 ");
                    strSRRSBuilder.Append("     curr6.CURRENCY_MST_PK                      ,        ");
                    strSRRSBuilder.Append("     curr6.CURRENCY_ID                          ,        ");
                    strSRRSBuilder.Append("     cont6.FCL_REQ_RATE                         RATE,                ");
                    strSRRSBuilder.Append("     cont6.FCL_REQ_RATE                         BKGRATE,             ");
                    strSRRSBuilder.Append("     tran6.LCL_BASIS                            BASISPK,               ");
                    strSRRSBuilder.Append("     '1'                                        PYMT_TYPE,FRT6.Credit            ");
                    strSRRSBuilder.Append("    from                                                             ");
                    strSRRSBuilder.Append("     TARIFF_MAIN_SEA_TBL            main6,                           ");
                    strSRRSBuilder.Append("     TARIFF_TRN_SEA_FCL_LCL         tran6,                           ");
                    strSRRSBuilder.Append("     TARIFF_TRN_SEA_CONT_DTL cont6,                       ");
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt6,                            ");
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL               COOMT,                           ");
                    strSRRSBuilder.Append("     CONTAINER_TYPE_MST_TBL         COCTMT,                          ");
                    strSRRSBuilder.Append("     PORT_MST_TBL                   COPL,                            ");
                    strSRRSBuilder.Append("     PORT_MST_TBL                   COPD,                            ");
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr6                            ");
                    strSRRSBuilder.Append("     where " + strContRefNo.ToString() + " IS NOT NULL AND                       ");
                    strSRRSBuilder.Append("            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           ");
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POL_FK              = COPL.PORT_MST_PK           ");
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POD_FK              = COPD.PORT_MST_PK           ");
                    strSRRSBuilder.Append("     AND    CONT6.TARIFF_TRN_SEA_FK = TRAN6.TARIFF_TRN_SEA_PK           ");
                    //Snigdharani ' modified by thiyagarajan on 21/11/08
                    strSRRSBuilder.Append("     AND    main6.OPERATOR_MST_FK              = COOMT.OPERATOR_MST_PK    (+)  ");
                    strSRRSBuilder.Append("     AND    cont6.CONTAINER_TYPE_MST_FK        = COCTMT.CONTAINER_TYPE_MST_PK");
                    strSRRSBuilder.Append("     AND    main6.CARGO_TYPE            = 1                                  ");
                    strSRRSBuilder.Append("     AND    main6.ACTIVE                = 1                                  ");
                    strSRRSBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK       ");
                    strSRRSBuilder.Append("     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK              ");
                    strSRRSBuilder.Append("     AND    main6.COMMODITY_GROUP_FK    = " + intCommodityPK);
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POL_FK       = " + strPOL);
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POD_FK       = " + strPOD + arrCCondition[4]);
                    strSRRSBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 ");
                    strSRRSBuilder.Append("            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       ");
                    strSRRSBuilder.Append("     AND TRAN6.FREIGHT_ELEMENT_MST_FK NOT IN ( SELECT FRTCHECK.FREIGHT_ELEMENT_MST_PK ");
                    strSRRSBuilder.Append("     FROM FREIGHT_ELEMENT_MST_TBL FRTCHECK ");
                    strSRRSBuilder.Append("     WHERE FRTCHECK.FREIGHT_ELEMENT_ID LIKE ('%BOF') ) ");
                    strSRRSBuilder.Append("     AND tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements.ToString() + ") ");
                    strSRRSBuilder.Append("     AND " + strSurcharge.ToString() + " = 1                  ");
                    strSRRSBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //Snigdharani - 23/12/2008 - order by preference'
                    strSRRSBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strSRRSBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                return strSRRSBuilder.ToString();
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

        public object funSLTariffFreight(ArrayList arrCCondition, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl, short isAgentType = 0, int DPAgentPK = 0)
        {
            try
            {
                string strOperatorRate = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.BASIS,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.MIN_RATE,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       abs(Q.BKGRATE)BKGRATE,(abs(TOTAL)*DECODE(Q.Credit,NULL,1,0,-1,1,1))TOTAL,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE,q.credit");
                    strBuilder.Append("       FROM  (");
                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, ");
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, ");
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, ");
                    strBuilder.Append("NULL AS COMMODITY, ");
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, ");
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, ");
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, ");
                    strBuilder.Append("DECODE(OTRN.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, ");
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, ");
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, ");
                    strBuilder.Append("OTRN.LCL_TARIFF_MIN_RATE AS MIN_RATE, ");
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS RATE, ");
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS BKGRATE,OTRN.LCL_TARIFF_RATE TOTAL, OTRN.LCL_BASIS AS BASISPK, ");
                    strBuilder.Append("'1' AS PYMT_TYPE,OFEMT.credit ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, ");
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, ");
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, ");
                    strBuilder.Append("PORT_MST_TBL OPL, ");
                    strBuilder.Append("PORT_MST_TBL OPD, ");
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL OFEMT, ");
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL OCUMT, ");
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL OUOM ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK ");
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK=OPL.PORT_MST_PK ");
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK=OPD.PORT_MST_PK ");
                    strBuilder.Append("AND OTRN.FREIGHT_ELEMENT_MST_FK=OFEMT.FREIGHT_ELEMENT_MST_PK ");
                    strBuilder.Append("AND OTRN.CURRENCY_MST_FK=OCUMT.CURRENCY_MST_PK ");
                    strBuilder.Append("AND OHDR.OPERATOR_MST_FK=OOMT.OPERATOR_MST_PK ");
                    strBuilder.Append("AND OTRN.LCL_BASIS=OUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append("AND OHDR.STATUS=1 ");
                    strBuilder.Append("AND OHDR.ACTIVE=1 ");
                    if (isAgentType == 1 & DPAgentPK > 0)
                    {
                        strBuilder.AppendLine("AND OHDR.TARIFF_TYPE=3 ");
                        strBuilder.AppendLine("AND OHDR.AGENT_MST_FK = " + DPAgentPK);
                    }
                    else
                    {
                        strBuilder.AppendLine("AND OHDR.TARIFF_TYPE=1 ");
                    }

                    strBuilder.Append("AND OHDR.CARGO_TYPE=2 " + arrCCondition[5]);
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK);
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM ");
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "'))");
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                else
                {
                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.TYPE,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       Q.BKGRATE, (abs(TOTAL)*DECODE(Q.Credit,NULL,1,0,-1,1,1))TOTAL,");
                    strBuilder.Append("       Q.BASISPK,");
                    //strBuilder.Append("       Q.PYMT_TYPE, ''FrtTransPk,q.Credit")
                    strBuilder.Append("       Q.PYMT_TYPE,q.Credit");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, ");
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                    strBuilder.Append("NULL AS COMMODITY, ");
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, ");
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, ");
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, ");
                    strBuilder.Append("DECODE(OTRN.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, ");
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL, ");
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, ");
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS RATE, ");
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS BKGRATE,OTRNCONT.FCL_REQ_RATE TOTAL, OTRN.LCL_BASIS AS BASISPK, ");
                    strBuilder.Append("'1' AS PYMT_TYPE, OFEMT.Credit ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, ");
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, ");
                    strBuilder.Append("TARIFF_TRN_SEA_CONT_DTL OTRNCONT, ");
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, ");
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL OCTMT, ");
                    strBuilder.Append("PORT_MST_TBL OPL, ");
                    strBuilder.Append("PORT_MST_TBL OPD, ");
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL OFEMT, ");
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL OCUMT ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK ");
                    strBuilder.Append("AND OTRNCONT.TARIFF_TRN_SEA_FK = OTRN.TARIFF_TRN_SEA_PK ");
                    //Snigdharani
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK=OPL.PORT_MST_PK ");
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK=OPD.PORT_MST_PK ");
                    strBuilder.Append("AND OTRN.FREIGHT_ELEMENT_MST_FK=OFEMT.FREIGHT_ELEMENT_MST_PK ");
                    strBuilder.Append("AND OTRN.CURRENCY_MST_FK=OCUMT.CURRENCY_MST_PK ");
                    strBuilder.Append("AND OHDR.OPERATOR_MST_FK=OOMT.OPERATOR_MST_PK (+)" + arrCCondition[5]);
                    strBuilder.Append("AND OCTMT.CONTAINER_TYPE_MST_PK=OTRNCONT.CONTAINER_TYPE_MST_FK ");
                    strBuilder.Append("AND OHDR.ACTIVE=1 ");
                    if (isAgentType == 1 & DPAgentPK > 0)
                    {
                        strBuilder.AppendLine("AND OHDR.TARIFF_TYPE=3 ");
                        strBuilder.AppendLine("AND OHDR.AGENT_MST_FK = " + DPAgentPK);
                    }
                    else
                    {
                        strBuilder.AppendLine("AND OHDR.TARIFF_TYPE=1 ");
                    }
                    strBuilder.Append("AND OHDR.STATUS=1 ");
                    strBuilder.Append("AND OHDR.CARGO_TYPE=1 ");
                    strBuilder.Append("AND OTRN.CHECK_FOR_ALL_IN_RT=1");
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK);
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM ");
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                    strBuilder.Append(" ORDER BY OFEMT.PREFERENCE ");
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                return strBuilder.ToString();
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

        public object funGTariffFreight(ArrayList arrCCondition, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    //Snigdharani - 23/12/2008 - order by preference'
                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.BASIS,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.MIN_RATE,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       abs(Q.BKGRATE),(abs(Q.TOTAL)*DECODE(Q.Credit,NULL,1,0,-1,1,1))TOTAL,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE,q.credit");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, ");
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, ");
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, ");
                    strBuilder.Append("NULL AS COMMODITY, ");
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, ");
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, ");
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, ");
                    strBuilder.Append("DECODE(OTRN.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, ");
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, ");
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, ");
                    strBuilder.Append("OTRN.LCL_TARIFF_MIN_RATE AS MIN_RATE, ");
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS RATE, ");
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS BKGRATE,OTRN.LCL_TARIFF_RATE AS TOTAL, OTRN.LCL_BASIS AS BASISPK, ");
                    strBuilder.Append("'1' AS PYMT_TYPE,OFEMT.credit ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, ");
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, ");
                    strBuilder.Append("PORT_MST_TBL OPL, ");
                    strBuilder.Append("PORT_MST_TBL OPD, ");
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL OFEMT, ");
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL OCUMT, ");
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL OUOM ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK ");
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK=OPL.PORT_MST_PK ");
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK=OPD.PORT_MST_PK ");
                    strBuilder.Append("AND OTRN.FREIGHT_ELEMENT_MST_FK=OFEMT.FREIGHT_ELEMENT_MST_PK ");
                    strBuilder.Append("AND OTRN.CURRENCY_MST_FK=OCUMT.CURRENCY_MST_PK ");
                    strBuilder.Append("AND OTRN.LCL_BASIS=OUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append("AND OHDR.ACTIVE=1 ");
                    strBuilder.Append("AND OHDR.STATUS=1 ");
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=2 ");
                    strBuilder.Append("AND OHDR.CARGO_TYPE=2 " + arrCCondition[5]);
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK);
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM ");
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "'))");
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                else
                {
                    //Snigdharani - 23/12/2008 - order by preference'
                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.TYPE,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       Q.BKGRATE,(abs(Q.TOTAL)*DECODE(Q.Credit,NULL,1,0,-1,1,1))TOTAL,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE,q.Credit");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, ");
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, ");
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                    strBuilder.Append("NULL AS COMMODITY, ");
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, ");
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, ");
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, ");
                    strBuilder.Append("DECODE(OTRN.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, ");
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL, ");
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, ");
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS RATE, ");
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS BKGRATE,OTRNCONT.FCL_REQ_RATE AS TOTAL,  OTRN.LCL_BASIS AS BASISPK, ");
                    strBuilder.Append("'1' AS PYMT_TYPE,OFEMT.Credit ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, ");
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, ");
                    strBuilder.Append("TARIFF_TRN_SEA_CONT_DTL OTRNCONT, ");
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL OCTMT, ");
                    strBuilder.Append("PORT_MST_TBL OPL, ");
                    strBuilder.Append("PORT_MST_TBL OPD, ");
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL OFEMT, ");
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL OCUMT ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK ");
                    strBuilder.Append("AND OTRNCONT.TARIFF_TRN_SEA_FK = OTRN.TARIFF_TRN_SEA_PK ");
                    //Snigdharani
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK=OPL.PORT_MST_PK ");
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK=OPD.PORT_MST_PK ");
                    strBuilder.Append("AND OTRN.FREIGHT_ELEMENT_MST_FK=OFEMT.FREIGHT_ELEMENT_MST_PK ");
                    strBuilder.Append("AND OTRN.CURRENCY_MST_FK=OCUMT.CURRENCY_MST_PK " + arrCCondition[5]);
                    strBuilder.Append("AND OCTMT.CONTAINER_TYPE_MST_PK=OTRNCONT.CONTAINER_TYPE_MST_FK ");
                    strBuilder.Append("AND OHDR.ACTIVE=1 ");
                    strBuilder.Append("AND OHDR.STATUS=1 ");
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=2 ");
                    strBuilder.Append("AND OHDR.CARGO_TYPE=1 ");
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK);
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM ");
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) ");
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                return strBuilder.ToString();
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

        #endregion "Fetch Freight"

        #region "FETCH CREDTDAYS AND CREDIT LIMIT"

        public int fetchCredit(string CreditDays, object CreditLimit, string Pk = "0", int type = 0, int CustPk = 0)
        {
            //type
            //1--SRR
            //2--Quotation
            //3--CustomerContract
            try
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                System.Text.StringBuilder strCustQuery = new System.Text.StringBuilder();
                if ((Pk == null))
                {
                    Pk = "0";
                }
                strCustQuery.Append("SELECT C.SEA_CREDIT_DAYS,");
                strCustQuery.Append(" C.SEA_CREDIT_LIMIT");
                strCustQuery.Append("FROM customer_mst_tbl c");
                strCustQuery.Append("WHERE c.customer_mst_pk=" + CustPk);
                OracleDataReader dr = null;
                if (type == 1)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD");
                    strQuery.Append("  FROM SRR_SEA_TBL     C");
                    strQuery.Append("  WHERE C.SRR_SEA_PK=" + Pk);
                }
                else if (type == 2)
                {
                    strQuery.Append("SELECT Q.CREDIT_DAYS,");
                    strQuery.Append("     Q.CREDIT_LIMIT ");
                    strQuery.Append("     FROM QUOTATION_MST_TBL Q");
                    strQuery.Append("     WHERE Q.QUOTATION_MST_PK=" + Pk);
                    strQuery.Append("  --   AND Q.CUSTOMER_MST_FK=" + CustPk);
                }
                else if (type == 3)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD FROM cont_cust_sea_tbl C  ");
                    strQuery.Append("WHERE C.CONT_CUST_SEA_PK=" + Pk);
                }
                else
                {
                    strQuery = strCustQuery;
                }
                DataTable dt = null;
                dt = (new WorkFlow()).GetDataTable(strQuery.ToString());
                if (dt.Rows.Count > 0)
                {
                    CreditDays = Convert.ToString(getDefault(dt.Rows[0][0], ""));
                    if (dt.Columns.Count > 1)
                        CreditLimit = getDefault(dt.Rows[0][1], "");
                }
                else
                {
                    dt = (new WorkFlow()).GetDataTable(strCustQuery.ToString());
                    if (dt.Rows.Count > 0)
                    {
                        CreditDays = Convert.ToString(getDefault(dt.Rows[0][0], ""));
                        if (dt.Columns.Count > 1)
                            CreditLimit = getDefault(dt.Rows[0][1], "");
                    }
                }
                dr = (new WorkFlow()).GetDataReader(strCustQuery.ToString());
                while (dr.Read())
                {
                    if (string.IsNullOrEmpty(Convert.ToString(CreditDays)))
                        CreditDays = Convert.ToString(getDefault(dr[0], ""));
                    if (string.IsNullOrEmpty(Convert.ToString(CreditLimit)))
                        CreditLimit = getDefault(dr[1], "");
                }
                dr.Close();
                if (!string.IsNullOrEmpty(CreditDays))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return 0;
            }
        }

        #endregion "FETCH CREDTDAYS AND CREDIT LIMIT"

        #region "Spetial requirment"

        #region "Spacial Request"

        public string UpdateTransactionHZSpcl(long PkValue, string UserName, string strSpclRequest, string Mode)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    objWF.OpenConnection();
                    selectCommand.Connection = objWF.MyConnection;
                    selectCommand.CommandType = CommandType.StoredProcedure;
                    if (Convert.ToInt32(Mode) == 0)
                    {
                        selectCommand.CommandText = objWF.MyUserName + ".BKG_TRN_SEA_HAZ_SPL_REQ_PKG.BKG_TRN_SEA_HAZ_SPL_REQ_UPD";
                    }
                    else
                    {
                        selectCommand.CommandText = objWF.MyUserName + ".BKG_TRN_AIR_HAZ_SPL_REQ_PKG.BKG_TRN_AIR_HAZ_SPL_REQ_UPD";
                    }
                    var _with4 = selectCommand.Parameters;
                    _with4.Clear();
                    if (Convert.ToInt32(Mode) == 1)
                    {
                        _with4.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with4.Add("BKG_TRN_AIR_HAZ_SPL_PK_IN", getDefault(strParam[14], DBNull.Value)).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with4.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with4.Add("BKG_TRN_SEA_HAZ_SPL_PK_IN", getDefault(strParam[14], DBNull.Value)).Direction = ParameterDirection.Input;
                    }

                    _with4.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[3].ToString()) ? "0" : strParam[3].ToString()), 0)).Direction = ParameterDirection.Input;
                    _with4.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[5].ToString()) ? "0" : strParam[5].ToString()), 0)).Direction = ParameterDirection.Input;
                    _with4.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[7].ToString()) ? "0" : strParam[7].ToString()), 0)).Direction = ParameterDirection.Input;
                    _with4.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    _with4.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    _with4.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    _with4.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("PROPER_SHIPPING_NAME_IN", getDefault(strParam[14], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("PACK_CLASS_TYPE_IN", getDefault(strParam[15], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    selectCommand.ExecuteNonQuery();
                    strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                    return strReturn;
                }
                catch (OracleException OraExp)
                {
                    throw OraExp;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    selectCommand.Connection.Close();
                }
            }
            return "";
        }

        public string SaveTransactionHZSpclNew(long PkValue, string UserName, string strSpclRequest, string Mode)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    objWF.OpenConnection();
                    selectCommand.Connection = objWF.MyConnection;
                    selectCommand.CommandType = CommandType.StoredProcedure;

                    if (Convert.ToInt32(Mode) == 0)
                    {
                        selectCommand.CommandText = objWF.MyUserName + ".BKG_TRN_SEA_HAZ_SPL_REQ_PKG.BKG_TRN_SEA_HAZ_SPL_REQ_INS";
                    }
                    else
                    {
                        selectCommand.CommandText = objWF.MyUserName + ".BKG_TRN_AIR_HAZ_SPL_REQ_PKG.BKG_TRN_AIR_HAZ_SPL_REQ_INS";
                    }

                    var _with5 = selectCommand.Parameters;
                    _with5.Clear();
                    if (Convert.ToInt32(Mode) == 1)
                    {
                        _with5.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with5.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    }
                    _with5.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[3].ToString()) ? "0" : strParam[3].ToString()), 0)).Direction = ParameterDirection.Input;
                    _with5.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[5].ToString()) ? "0" : strParam[5].ToString()), 0)).Direction = ParameterDirection.Input;
                    _with5.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[7].ToString()) ? "0" : strParam[7].ToString()), 0)).Direction = ParameterDirection.Input;
                    _with5.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    _with5.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    _with5.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    _with5.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("PROPER_SHIPPING_NAME_IN", getDefault(strParam[14], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("PACK_CLASS_TYPE_IN", getDefault(strParam[15], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with5.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    selectCommand.ExecuteNonQuery();
                    strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                    return strReturn;
                }
                catch (OracleException OraExp)
                {
                    throw OraExp;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    selectCommand.Connection.Close();
                }
            }
            return "";
        }

        private ArrayList SaveTransactionHZSpcl(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with6 = SCM;
                    _with6.CommandType = CommandType.StoredProcedure;
                    _with6.CommandText = UserName + ".BKG_TRN_SEA_HAZ_SPL_REQ_PKG.BKG_TRN_SEA_HAZ_SPL_REQ_INS";
                    var _with7 = _with6.Parameters;
                    _with7.Clear();
                    _with7.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with7.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? "0" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                    _with7.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "0" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    _with7.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "0" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    _with7.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    _with7.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    _with7.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    _with7.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("PROPER_SHIPPING_NAME_IN", getDefault(strParam[14], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("PACK_CLASS_TYPE_IN", getDefault(strParam[15], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with7.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with6.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        public DataTable fetchSpclReq(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT BKG_TRN_SPL_PK,");
                    strQuery.Append("BOOKING_TRN_FK,");
                    strQuery.Append("OUTER_PACK_TYPE_MST_FK,");
                    strQuery.Append("INNER_PACK_TYPE_MST_FK,");
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("FLASH_PNT_TEMP,");
                    strQuery.Append("FLASH_PNT_TEMP_UOM,");
                    strQuery.Append("IMDG_CLASS_CODE,");
                    strQuery.Append("UN_NO,");
                    strQuery.Append("IMO_SURCHARGE,");
                    strQuery.Append("SURCHARGE_AMT,");
                    strQuery.Append("IS_MARINE_POLLUTANT,");
                    strQuery.Append("EMS_NUMBER,");
                    strQuery.Append("PROPER_SHIPPING_NAME,");
                    strQuery.Append("PACK_CLASS_TYPE FROM BOOKING_TRN_SPL_REQ Q");
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.BOOKING_TRN_FK=" + strPK);
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
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

        private ArrayList SaveTransactionReefer(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with8 = SCM;
                    _with8.CommandType = CommandType.StoredProcedure;
                    _with8.CommandText = UserName + ".BKG_TRN_SEA_REF_SPL_REQ_PKG.BKG_TRN_SEA_REF_SPL_REQ_INS";
                    var _with9 = _with8.Parameters;
                    _with9.Clear();
                    _with9.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with9.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    _with9.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    _with9.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with9.Add("IS_PERISHABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    _with9.Add("MIN_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with9.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "0" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    _with9.Add("MAX_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with9.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "0" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    _with9.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with9.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    _with9.Add("REF_VENTILATION_IN", getDefault(strParam[10], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with9.Add("HAULAGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("GENSET_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("CO2_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("O2_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("REQ_SET_TEMP_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("REQ_SET_TEMP_UOM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("AIR_VENTILATION_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("AIR_VENTILATION_UOM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("DEHUMIDIFIER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("FLOORDRAINS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("DEFROSTING_INTERVAL_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with8.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        public DataTable fetchSpclReqReefer(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT BKG_TRN_SPL_PK,");
                    strQuery.Append("BOOKING_TRN_FK,");
                    strQuery.Append("VENTILATION,");
                    strQuery.Append("AIR_COOL_METHOD,");
                    strQuery.Append("HUMIDITY_FACTOR,");
                    strQuery.Append("IS_PERISHABLE_GOODS,");
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("PACK_TYPE_MST_FK,");
                    strQuery.Append("Q.PACK_COUNT, ");
                    strQuery.Append("Q.REF_VENTILATION ");
                    strQuery.Append("FROM BOOKING_TRN_SPL_REQ Q");
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.BOOKING_TRN_FK=" + strPK);
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
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

        public DataTable fetchSpclReqODC(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT ");
                    strQuery.Append("BKG_TRN_SPL_PK,");
                    strQuery.Append("BOOKING_TRN_FK,");
                    strQuery.Append("LENGTH,");
                    strQuery.Append("LENGTH_UOM_MST_FK,");
                    strQuery.Append("HEIGHT,");
                    strQuery.Append("HEIGHT_UOM_MST_FK,");
                    strQuery.Append("WIDTH,");
                    strQuery.Append("WIDTH_UOM_MST_FK,");
                    strQuery.Append("WEIGHT,");
                    strQuery.Append("WEIGHT_UOM_MST_FK,");
                    strQuery.Append("VOLUME,");
                    strQuery.Append("VOLUME_UOM_MST_FK,");
                    strQuery.Append("SLOT_LOSS,");
                    strQuery.Append("LOSS_QUANTITY,");
                    strQuery.Append("APPR_REQ, ");
                    strQuery.Append("NO_OF_SLOTS, ");
                    strQuery.Append("STOWAGE, ");
                    strQuery.Append("HANDLING_INSTR, ");
                    strQuery.Append("LASHING_INSTR ");
                    strQuery.Append("FROM BOOKING_TRN_SPL_REQ Q");
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.BOOKING_TRN_FK=" + strPK);
                    return (new WorkFlow()).GetDataTable(strQuery.ToString());
                }
                else
                {
                    return null;
                }
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

        private ArrayList SaveSplReqTransaction(OracleCommand SCM, string UserName, SpecialReqClass strSpclRequest, long PkValue)
        {
            //ByRef strSpclRequest As String, _
            string bkgTrnSpPk = "";
            if ((strSpclRequest != null))
            {
                arrMessage.Clear();
                string[] strParam = null;

                try
                {
                    var _with10 = SCM;
                    _with10.CommandType = CommandType.Text;
                    _with10.CommandText = "SELECT BSR.BKG_TRN_SPL_PK FROM BOOKING_TRN_SPL_REQ BSR WHERE BSR.BOOKING_TRN_FK=" + Convert.ToInt32(PkValue).ToString();
                    _with10.Parameters.Clear();
                    bkgTrnSpPk = Convert.ToString(SCM.ExecuteScalar());

                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.Parameters.Clear();
                    if (Convert.ToInt32(bkgTrnSpPk) > 0)
                    {
                        _with10.CommandText = UserName + ".BOOKING_MST_PKG.BKG_TRN_SPL_REQ_UPD";
                        strSpclRequest.BKG_TRN_SPL_PK = Convert.ToString(bkgTrnSpPk);
                        _with10.Parameters.Add("BKG_TRN_SPL_PK_IN", Convert.ToInt32(bkgTrnSpPk)).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.CommandText = UserName + ".BOOKING_MST_PKG.BKG_TRN_SPL_REQ_INS";
                    }

                    var _with11 = _with10.Parameters;
                    _with11.Add("BOOKING_TRN_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with11.Add("OUTER_PACK_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("VENTILATION_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("LENGTH_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("INNER_PACK_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("AIR_COOL_METHOD_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("LENGTH_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("HEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("HUMIDITY_FACTOR_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("MIN_TEMP_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("HEIGHT_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("IS_PERISHABLE_GOODS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("MIN_TEMP_UOM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("MAX_TEMP_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("WIDTH_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("MAX_TEMP_UOM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("WIDTH_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("FLASH_PNT_TEMP_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("WEIGHT_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("FLASH_PNT_TEMP_UOM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("IMDG_CLASS_CODE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("PACK_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("UN_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("PACK_COUNT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("VOLUME_UOM_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("IMO_SURCHARGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("HAULAGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("SLOT_LOSS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("SURCHARGE_AMT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("GENSET_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("LOSS_QUANTITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("IS_MARINE_POLLUTANT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("APPR_REQ_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("CO2_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("NO_OF_SLOTS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("O2_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("EMS_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("REQ_SET_TEMP_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("PROPER_SHIPPING_NAME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("PACK_CLASS_TYPE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("REQ_SET_TEMP_UOM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("STOWAGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("HANDLING_INSTR_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("DEHUMIDIFIER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("FLOORDRAINS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("LASHING_INSTR_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("AIR_VENTILATION_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("DEFROSTING_INTERVAL_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("AIR_VENTILATION_UOM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("REF_VENTILATION_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("REQ_TYPE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    //----------------------------------------

                    SpecialReqClass req = strSpclRequest;
                    _with10.Parameters["REQ_TYPE_IN"].Value = req.REQ_TYPE;
                    //HAZ or REEFER
                    if (req.REQ_TYPE == 1 | req.REQ_TYPE == 2)
                    {
                        _with10.Parameters["MIN_TEMP_IN"].Value = ((req.RH_MIN_TEMP == null) ? "" : req.RH_MIN_TEMP);
                        _with10.Parameters["MIN_TEMP_UOM_IN"].Value = ((req.RH_MIN_TEMP_UOM == null) ? "" : req.RH_MIN_TEMP_UOM);
                        _with10.Parameters["MAX_TEMP_IN"].Value = ((req.RH_MAX_TEMP == null) ? "" : req.RH_MAX_TEMP);
                        _with10.Parameters["MAX_TEMP_UOM_IN"].Value = ((req.RH_MAX_TEMP_UOM == null) ? "" : req.RH_MAX_TEMP_UOM);
                    }
                    //HAZ
                    if (req.REQ_TYPE == 1)
                    {
                        _with10.Parameters["OUTER_PACK_TYPE_MST_FK_IN"].Value = ((req.H_OUTER_PACK_TYPE_MST_FK == null) ? "" : req.H_OUTER_PACK_TYPE_MST_FK);
                        _with10.Parameters["FLASH_PNT_TEMP_IN"].Value = ((req.H_FLASH_PNT_TEMP == null) ? "" : req.H_FLASH_PNT_TEMP);
                        _with10.Parameters["FLASH_PNT_TEMP_UOM_IN"].Value = ((req.H_FLASH_PNT_TEMP_UOM == null) ? "" : req.H_FLASH_PNT_TEMP_UOM);
                        _with10.Parameters["IMDG_CLASS_CODE_IN"].Value = ((req.H_IMDG_CLASS_CODE == null) ? "" : req.H_IMDG_CLASS_CODE);
                        _with10.Parameters["UN_NO_IN"].Value = ((req.H_UN_NO == null) ? "" : req.H_UN_NO);
                        _with10.Parameters["IMO_SURCHARGE_IN"].Value = ((req.H_IMO_SURCHARGE == null) ? "" : req.H_IMO_SURCHARGE);
                        _with10.Parameters["SURCHARGE_AMT_IN"].Value = ((req.H_SURCHARGE_AMT == null) ? "" : req.H_SURCHARGE_AMT);
                        _with10.Parameters["IS_MARINE_POLLUTANT_IN"].Value = ((req.H_IS_MARINE_POLLUTANT == null) ? "" : req.H_IS_MARINE_POLLUTANT);
                        _with10.Parameters["EMS_NUMBER_IN"].Value = ((req.H_EMS_NUMBER == null) ? "" : req.H_EMS_NUMBER);
                        _with10.Parameters["PROPER_SHIPPING_NAME_IN"].Value = ((req.H_PROPER_SHIPPING_NAME == null) ? "" : req.H_PROPER_SHIPPING_NAME);
                        _with10.Parameters["PACK_CLASS_TYPE_IN"].Value = ((req.H_PACK_CLASS_TYPE == null) ? "" : req.H_PACK_CLASS_TYPE);
                        //REEFER
                    }
                    else if (req.REQ_TYPE == 2)
                    {
                        _with10.Parameters["REQ_TYPE_IN"].Value = req.REQ_TYPE;
                        _with10.Parameters["VENTILATION_IN"].Value = ((req.R_VENTILATION == null) ? "" : req.R_VENTILATION);
                        _with10.Parameters["AIR_COOL_METHOD_IN"].Value = ((req.R_AIR_COOL_METHOD == null) ? "" : req.R_AIR_COOL_METHOD);
                        _with10.Parameters["HUMIDITY_FACTOR_IN"].Value = ((req.R_HUMIDITY_FACTOR == null) ? "" : req.R_HUMIDITY_FACTOR);
                        _with10.Parameters["IS_PERISHABLE_GOODS_IN"].Value = ((req.R_IS_PERISHABLE_GOODS == null) ? "" : req.R_IS_PERISHABLE_GOODS);
                        _with10.Parameters["PACK_TYPE_MST_FK_IN"].Value = ((req.R_PACK_TYPE_MST_FK == null) ? "" : req.R_PACK_TYPE_MST_FK);
                        _with10.Parameters["PACK_COUNT_IN"].Value = ((req.R_PACK_COUNT == null) ? "" : req.R_PACK_COUNT);
                        _with10.Parameters["HAULAGE_IN"].Value = ((req.R_HAULAGE == null) ? "" : req.R_HAULAGE);
                        _with10.Parameters["GENSET_IN"].Value = ((req.R_GENSET == null) ? "" : req.R_GENSET);
                        _with10.Parameters["CO2_IN"].Value = ((req.R_CO2 == null) ? "" : req.R_CO2);
                        _with10.Parameters["O2_IN"].Value = ((req.R_O2 == null) ? "" : req.R_O2);
                        _with10.Parameters["REQ_SET_TEMP_IN"].Value = ((req.R_REQ_SET_TEMP == null) ? "" : req.R_REQ_SET_TEMP);
                        _with10.Parameters["REQ_SET_TEMP_UOM_IN"].Value = ((req.R_REQ_SET_TEMP_UOM == null) ? "" : req.R_REQ_SET_TEMP_UOM);
                        _with10.Parameters["DEHUMIDIFIER_IN"].Value = ((req.R_DEHUMIDIFIER == null) ? "" : req.R_DEHUMIDIFIER);
                        _with10.Parameters["FLOORDRAINS_IN"].Value = ((req.R_FLOORDRAINS == null) ? "" : req.R_FLOORDRAINS);
                        _with10.Parameters["AIR_VENTILATION_IN"].Value = ((req.R_AIR_VENTILATION == null) ? "" : req.R_AIR_VENTILATION);
                        _with10.Parameters["DEFROSTING_INTERVAL_IN"].Value = ((req.R_DEFROSTING_INTERVAL == null) ? "" : req.R_DEFROSTING_INTERVAL);
                        _with10.Parameters["AIR_VENTILATION_UOM_IN"].Value = ((req.R_AIR_VENTILATION_UOM == null) ? "" : req.R_AIR_VENTILATION_UOM);
                        _with10.Parameters["REF_VENTILATION_IN"].Value = ((req.R_REF_VENTILATION == null) ? "" : req.R_REF_VENTILATION);
                        //ODC
                    }
                    else if (req.REQ_TYPE == 3)
                    {
                        _with10.Parameters["LENGTH_IN"].Value = ((req.O_LENGTH == null) ? "" : req.O_LENGTH);
                        _with10.Parameters["LENGTH_UOM_MST_FK_IN"].Value = ((req.O_LENGTH_UOM_MST_FK == null) ? "" : req.O_LENGTH_UOM_MST_FK);
                        _with10.Parameters["HEIGHT_IN"].Value = ((req.O_HEIGHT == null) ? "" : req.O_HEIGHT);
                        _with10.Parameters["HEIGHT_UOM_MST_FK_IN"].Value = ((req.O_HEIGHT_UOM_MST_FK == null) ? "" : req.O_HEIGHT_UOM_MST_FK);
                        _with10.Parameters["WIDTH_IN"].Value = ((req.O_WIDTH == null) ? "" : req.O_WIDTH);
                        _with10.Parameters["WIDTH_UOM_MST_FK_IN"].Value = ((req.O_WIDTH_UOM_MST_FK == null) ? "" : req.O_WIDTH_UOM_MST_FK);
                        _with10.Parameters["WEIGHT_IN"].Value = ((req.O_WEIGHT == null) ? "" : req.O_WEIGHT);
                        _with10.Parameters["WEIGHT_UOM_MST_FK_IN"].Value = ((req.O_WEIGHT_UOM_MST_FK == null) ? "" : req.O_WEIGHT_UOM_MST_FK);
                        _with10.Parameters["VOLUME_IN"].Value = ((req.O_VOLUME == null) ? "" : req.O_VOLUME);
                        _with10.Parameters["VOLUME_UOM_MST_FK_IN"].Value = ((req.O_VOLUME_UOM_MST_FK == null) ? "" : req.O_VOLUME_UOM_MST_FK);
                        _with10.Parameters["SLOT_LOSS_IN"].Value = ((req.O_SLOT_LOSS == null) ? "" : req.O_SLOT_LOSS);
                        _with10.Parameters["LOSS_QUANTITY_IN"].Value = ((req.O_LOSS_QUANTITY == null) ? "" : req.O_LOSS_QUANTITY);
                        _with10.Parameters["APPR_REQ_IN"].Value = ((req.O_APPR_REQ == null) ? "" : req.O_APPR_REQ);
                        _with10.Parameters["NO_OF_SLOTS_IN"].Value = ((req.O_NO_OF_SLOTS == null) ? "" : req.O_NO_OF_SLOTS);
                        _with10.Parameters["STOWAGE_IN"].Value = ((req.O_STOWAGE == null) ? "" : req.O_STOWAGE);
                        _with10.Parameters["HANDLING_INSTR_IN"].Value = ((req.O_HANDLING_INSTR == null) ? "" : req.O_HANDLING_INSTR);
                        _with10.Parameters["LASHING_INSTR_IN"].Value = ((req.O_LASHING_INSTR == null) ? "" : req.O_LASHING_INSTR);
                    }
                    _with10.ExecuteNonQuery();
                    if (Convert.ToInt32(strSpclRequest.BKG_TRN_SPL_PK) == 0)
                    {
                        strSpclRequest.BKG_TRN_SPL_PK = Convert.ToString(_with10.Parameters["RETURN_VALUE"].Value);
                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        #endregion "Spacial Request"

        #endregion "Spetial requirment"

        #region "To assign data to Cargo Move Code"

        public object FillddCMCode(int PkValue)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            DataTable dtCM = null;
            WorkFlow objwf = new WorkFlow();
            strBuilder.Append(" SELECT CARGO_MOVE_FK FROM QUOTATION_MST_TBL WHERE QUOTATION_MST_PK =" + PkValue);
            try
            {
                dtCM = objwf.GetDataTable(strBuilder.ToString());
                return dtCM;
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

        #endregion "To assign data to Cargo Move Code"

        #region "Save"

        private enum ParamType
        {
            NumberType = 1,
            DoubleType = 2,
            DateType = 3,
            StringType = 4
        }

        public void SetParameter(WorkFlow objWf, string ParamName, string ParamVal, short NumOrDtOrStr = 4, ParameterDirection ParamDir = ParameterDirection.Input)
        {
            //Refer Enum ParamType for NumOrDtOrStr parameter
            var _with12 = objWf.MyCommand;
            if (string.IsNullOrEmpty(ParamVal) | (ParamVal == null))
            {
                _with12.Parameters.Add(ParamName, DBNull.Value).Direction = ParamDir;
            }
            else if (string.IsNullOrEmpty(Convert.ToString(ParamVal).Trim()))
            {
                _with12.Parameters.Add(ParamName, DBNull.Value).Direction = ParamDir;
            }
            else
            {
                if (NumOrDtOrStr == 1)
                {
                    _with12.Parameters.Add(ParamName, Convert.ToInt64(ParamVal)).Direction = ParamDir;
                }
                else if (NumOrDtOrStr == 4)
                {
                    _with12.Parameters.Add(ParamName, Convert.ToDouble(ParamVal)).Direction = ParamDir;
                }
                else if (NumOrDtOrStr == 3)
                {
                    _with12.Parameters.Add(ParamName, Convert.ToDateTime(ParamVal)).Direction = ParamDir;
                }
                else
                {
                    _with12.Parameters.Add(ParamName, ParamVal).Direction = ParamDir;
                }
            }
        }

        //This region saves and updates the Booking in database
        //Here first the data is entered into the Header Table (BOOKING_MST_TBL) then taking the PkValue of the
        //header the transaction table is filled (BOOKING_TRN,BOOKING_TRN_FRT_DTLS)
        //Transaction control is take care as the OracleCommand itself is sent as a
        //parameter to the function filling transaction tables
        public ArrayList SaveBooking(DataSet dsMain, DataSet dsCtrDetails, object txtBookingNo, long nLocationId, long nEmpId, string Measure, string Wt, string Divfac, string strPolPk = "", string strPodPk = "",
        string strFreightpk = "", Int16 intIsLcl = 0, string strBStatus = "", string strVoyagepk = "", string PODLocfk = "", string ShipperPK = "", string Consigne = "", string strJobPk = "", string hdnQuotFetch = "", string HiddenContainers = "",
        int CargoType = 0, bool Nomination = false, int Cust_Type = 0, short BizType = 2, int PLRTransPK = 0, int PFDTransPK = 0, Int16 Restriction = 0, double CheargableWt = 0.0, Int32 SplitBooking = 0, string HiddenNewPCount = "",
        string HiddenNewAWt = "", string HiddenNewChWt = "", string HiddenNewDensity = "", string HiddenNewULDCnt = "", string HiddenNewVolWt = "", string HiddenNewVolCBM = "", bool VoyageExist = true, string polid = "", string podid = "", string flag_no_in = "")
        {
            string EbkgRefno = null;
            string BookingRefNo = null;
            string NewBookingPK = null;
            string BookingRef = "";
            string EBookingRef = null;
            bool IsUpdate = false;
            Int32 i = default(Int32);
            bool isTrue = false;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            string str = null;
            Int16 SchCnt = 0;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            arrMessage.Clear();

            try
            {
                DataRow rowBkg = dsMain.Tables["tblMaster"].Rows[0];
                if (!Nomination & BizType == 2)
                {
                    if ((string.IsNullOrEmpty(strVoyagepk) || strVoyagepk == "0") & !string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_ID"].ToString()))
                    {
                        strVoyagepk = "0";
                        //arrMessage = SaveVesselMaster(strVoyagepk, getDefault(rowBkg["VESSEL_NAME"], ""), getDefault(rowBkg["CARRIER_MST_FK"], 0), getDefault(rowBkg["VESSEL_ID"], ""), getDefault(rowBkg["VOYAGE_FLIGHT_NO"], ""), objWK.MyCommand, getDefault(rowBkg["PORT_MST_POL_FK"], 0), Convert.ToString(rowBkg["PORT_MST_POD_FK"]), DateTime.MinValue, getDefault(rowBkg["ETD_DATE"], null),
                        //getDefault(rowBkg["CUT_OFF_DATE"], null), getDefault(rowBkg["ETA_DATE"], null));
                        rowBkg["VESSEL_VOYAGE_FK"] = strVoyagepk;
                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }
                    }
                }
                else
                {
                    if (VoyageExist == false)
                    {
                        if (!string.IsNullOrEmpty(txtBookingNo.ToString().Trim()))
                        {
                            str = "select count(*) from booking_mst_tbl bmt where bmt.booking_ref_no='" + txtBookingNo.ToString().Trim() + "'";
                            str = str + " AND upper(BMT.Voyage_Flight_No) = upper('" + getDefault(rowBkg["VOYAGE_FLIGHT_NO"], "") + "')";
                            str = str + " AND  BMT.CARRIER_MST_FK=" + getDefault(rowBkg["CARRIER_MST_FK"], 0);
                            WorkFlow objWF = new WorkFlow();
                            SchCnt = Convert.ToInt16(objWF.ExecuteScaler(str));
                        }
                        if (SchCnt == 0)
                        {
                            arrMessage = SaveAirLineScheduleMst(dsMain, flag_no_in);
                        }
                    }
                    //If (strVoyagepk = "" OrElse strVoyagepk = "0") And Not IsDBNull(dsMain.Tables("tblMaster").Rows(0).Item("VESSEL_ID")) Then
                    //    strVoyagepk = 0
                    //    arrMessage = SaveAirLineScheduleMst(
                    //    getDefault(rowBkg.Item("CARRIER_MST_FK"), ""), _
                    //    getDefault(rowBkg.Item("VOYAGE_FLIGHT_NO"), 0), _
                    //    getDefault(rowBkg.Item("PORT_MST_POL_FK"), ""), _
                    //    getDefault(rowBkg.Item("PORT_MST_POD_FK"), ""), _
                    //    getDefault(rowBkg.Item("ETD_DATE"), ""), _
                    //     getDefault(rowBkg.Item("CUT_OFF_DATE"), 0), _
                    //      getDefault(rowBkg.Item("CUT_OFF_DATE"), 0), _
                    //       getDefault(rowBkg.Item("ETD_DATE"), 0), _
                    //        getDefault(rowBkg.Item("ETD_DATE"), 0), _
                    //       getDefault(rowBkg.Item(""), 0), _
                    //          getDefault(rowBkg.Item(""), 0), _
                    //             getDefault(rowBkg.Item(""), 0), _
                    //               getDefault(rowBkg.Item(""), 0), _
                    //               getDefault(rowBkg.Item(""), 0)

                    //    If Not (InStr(arrMessage(0), "saved") > 0) Then
                    //        Return arrMessage
                    //    End If
                    //End If
                }

                if (string.IsNullOrEmpty(rowBkg["BOOKING_MST_PK"].ToString()))
                {
                    if (string.IsNullOrEmpty(txtBookingNo.ToString()))
                    {
                        if (Nomination)
                        {
                            BookingRefNo = GenerateNominationNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, BizType, polid, podid);
                        }
                        else
                        {
                            BookingRefNo = GenerateBookingNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, BizType, polid, podid);
                        }

                        if (BookingRefNo == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                        int ifBkgRefExist = 1;
                        while (ifBkgRefExist != 0)
                        {
                            objWK.MyCommand.Parameters.Clear();
                            objWK.MyCommand.CommandType = CommandType.Text;
                            objWK.MyCommand.CommandText = "SELECT COUNT(*) FROM BOOKING_MST_TBL BST WHERE BST.BOOKING_REF_NO='" + BookingRefNo + "'";
                            ifBkgRefExist = Convert.ToInt32(objWK.MyCommand.ExecuteScalar());
                            if (ifBkgRefExist == 1)
                            {
                                BookingRefNo = BookingRefNo.Substring(0, BookingRefNo.Length - 3) + (Convert.ToInt32(BookingRefNo.Substring(BookingRefNo.Length - 3, 3)) + 1).ToString().PadLeft(3);
                            }
                        }
                        txtBookingNo = BookingRefNo;
                    }
                    else
                    {
                        BookingRefNo = Convert.ToString(txtBookingNo);
                    }
                }
                else
                {
                    //Updation
                    IsUpdate = true;
                    if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                    {
                        Int16 BookingPK = default(Int16);
                        string EBKSbr = null;
                        Int16 status = default(Int16);
                        BookingPK = Convert.ToInt16(rowBkg["BOOKING_MST_PK"]);
                        Chk_EBK = FetchEBKN(BookingPK);
                        EBookingRef = txtBookingNo.ToString();
                        status = Convert.ToInt16(rowBkg["STATUS"]);
                        if (Chk_EBK == 1 & status == 2)
                        {
                            BookingRefNo = GenerateBookingNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, BizType);
                            if (BookingRefNo == "Protocol Not Defined.")
                            {
                                arrMessage.Add("Protocol Not Defined.");
                                return arrMessage;
                            }
                        }
                        else
                        {
                            BookingRefNo = txtBookingNo.ToString();
                        }
                        txtBookingNo = BookingRefNo;
                        EbkgRefno = BookingRef;
                        BookingRef = BookingRefNo;
                    }
                    else
                    {
                        BookingRefNo = txtBookingNo.ToString();
                    }
                }
                var _with13 = objWK.MyCommand;
                _with13.CommandType = CommandType.StoredProcedure;
                if (IsUpdate)
                {
                    _with13.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_MST_UPD";
                }
                else
                {
                    _with13.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_MST_INS";
                }
                _with13.Parameters.Clear();
                if (IsUpdate)
                {
                    _with13.Parameters.Add("BOOKING_MST_PK_IN", Convert.ToInt64(rowBkg["BOOKING_MST_PK"])).Direction = ParameterDirection.Input;
                    _with13.Parameters.Add("VERSION_NO_IN", rowBkg["VERSION_NO"]).Direction = ParameterDirection.Input;
                    _with13.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with13.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                }

                _with13.Parameters.Add("FROM_FLAG_IN", (Nomination ? 2 : 1)).Direction = ParameterDirection.Input;
                //NOMINATION IS IMPORT SIDE BUSINESS
                _with13.Parameters.Add("BOOKING_REF_NO_IN", BookingRefNo).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("BOOKING_DATE_IN", Convert.ToDateTime(rowBkg["BOOKING_DATE"])).Direction = ParameterDirection.Input;

                SetParameter(objWK, "CUST_CUSTOMER_MST_FK_IN", rowBkg["CUST_CUSTOMER_MST_FK"].ToString(), 1);

                SetParameter(objWK, "CONS_CUSTOMER_MST_FK_IN", rowBkg["CONS_CUSTOMER_MST_FK"].ToString(), 1);

                SetParameter(objWK, "SHIPMENT_DATE_IN", rowBkg["SHIPMENT_DATE"].ToString(), 3);

                SetParameter(objWK, "CARGO_TYPE_IN", rowBkg["CARGO_TYPE"].ToString(), 1);

                SetParameter(objWK, "COL_PLACE_MST_FK_IN", rowBkg["POO_FK"].ToString(), 1);

                SetParameter(objWK, "COL_ADDRESS_IN", rowBkg["COL_ADDRESS"].ToString());

                SetParameter(objWK, "DEL_PLACE_MST_FK_IN", rowBkg["PFD_FK"].ToString(), 1);

                SetParameter(objWK, "DEL_ADDRESS_IN", rowBkg["DEL_ADDRESS"].ToString());

                SetParameter(objWK, "CB_AGENT_MST_FK_IN", rowBkg["CB_AGENT_MST_FK"].ToString(), 1);

                SetParameter(objWK, "PACK_TYPE_MST_FK_IN", rowBkg["PACK_TYPE_MST_FK"].ToString(), 1);

                SetParameter(objWK, "PACK_COUNT_IN", rowBkg["PACK_COUNT"].ToString(), 1);

                SetParameter(objWK, "GROSS_WEIGHT_IN", rowBkg["GROSS_WEIGHT"].ToString(), 2);

                SetParameter(objWK, "NET_WEIGHT_IN", rowBkg["NET_WEIGHT"].ToString(), 2);

                SetParameter(objWK, "CHARGEABLE_WEIGHT_IN", rowBkg["CHARGEABLE_WEIGHT"].ToString(), 2);

                SetParameter(objWK, "FRT_WEIGHT_IN", rowBkg["FRT_WEIGHT"].ToString(), 2);

                //If intIsLcl = -1 Then
                //    .Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input
                //    SetParameter(objWK, "CHARGEABLE_WEIGHT_IN", rowBkg.Item("CHARGEABLE_WEIGHT"), 2)
                //Else
                //    SetParameter(objWK, "NET_WEIGHT_IN", rowBkg.Item("NET_WEIGHT"), 2)
                //    .Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input
                //End If

                SetParameter(objWK, "VOLUME_IN_CBM_IN", rowBkg["VOLUME_IN_CBM"].ToString(), 2);

                SetParameter(objWK, "LINE_BKG_NO_IN", rowBkg["LINE_BKG_NO"].ToString());

                SetParameter(objWK, "VESSEL_NAME_IN", rowBkg["VESSEL_NAME"].ToString());

                SetParameter(objWK, "VOYAGE_FLIGHT_NO_IN", rowBkg["VOYAGE_FLIGHT_NO"].ToString());

                SetParameter(objWK, "ETA_DATE_IN", rowBkg["ETA_DATE"].ToString(), 3);

                SetParameter(objWK, "ETD_DATE_IN", rowBkg["ETD_DATE"].ToString(), 3);

                SetParameter(objWK, "CUT_OFF_DATE_IN", rowBkg["CUT_OFF_DATE"].ToString(), 3);

                if (string.IsNullOrEmpty(rowBkg["CARRIER_MST_FK"].ToString()))
                {
                    _with13.Parameters.Add("CARRIER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with13.Parameters.Add("VSL_AIR_OPR_UPD_STATUS_IN", 1).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with13.Parameters.Add("CARRIER_MST_FK_IN", Convert.ToInt64(rowBkg["CARRIER_MST_FK"])).Direction = ParameterDirection.Input;
                    _with13.Parameters.Add("VSL_AIR_OPR_UPD_STATUS_IN", 0).Direction = ParameterDirection.Input;
                }

                SetParameter(objWK, "CL_AGENT_MST_FK_IN", rowBkg["CL_AGENT_MST_FK"].ToString(), 1);

                _with13.Parameters.Add("STATUS_IN", rowBkg["STATUS"]).Direction = ParameterDirection.Input;

                SetParameter(objWK, "CARGO_MOVE_FK_IN", rowBkg["CARGO_MOVE_FK"].ToString(), 1);

                SetParameter(objWK, "PYMT_TYPE_IN", rowBkg["PYMT_TYPE"].ToString(), 1);

                SetParameter(objWK, "COMMODITY_GROUP_FK_IN", rowBkg["COMMODITY_GROUP_FK"].ToString(), 1);

                SetParameter(objWK, "SHIPPING_TERMS_MST_FK_IN", rowBkg["SHIPPING_TERMS_MST_FK"].ToString(), 1);

                SetParameter(objWK, "CUSTOMER_REF_NO_IN", rowBkg["CUSTOMER_REF_NO"].ToString());

                SetParameter(objWK, "DP_AGENT_MST_FK_IN", rowBkg["DP_AGENT_MST_FK"].ToString(), 1);

                SetParameter(objWK, "VESSEL_VOYAGE_FK_IN", rowBkg["VESSEL_VOYAGE_FK"].ToString(), 1);

                SetParameter(objWK, "CREDIT_LIMIT_IN", rowBkg["CREDIT_LIMIT"].ToString(), 1);

                SetParameter(objWK, "CREDIT_DAYS_IN", rowBkg["CREDIT_DAYS"].ToString(), 1);

                SetParameter(objWK, "IS_EBOOKING_IN", rowBkg["IS_EBOOKING"].ToString(), 1);

                SetParameter(objWK, "BASE_CURRENCY_FK_IN", rowBkg["BASE_CURRENCY_FK"].ToString(), 1);

                SetParameter(objWK, "ROLLOVER_REMARKS_IN", rowBkg["ROLLOVER_REMARKS"].ToString());

                SetParameter(objWK, "ROLLOVER_DATE_IN", rowBkg["ROLLOVER_DATE"].ToString(), 3);

                SetParameter(objWK, "REASSVESSEL_VOYAGE_FK_IN", rowBkg["REASSVESSEL_VOYAGE_FK"].ToString(), 1);

                SetParameter(objWK, "GOODS_DESC_IN", rowBkg["GOODS_DESCRIPTION"].ToString());

                SetParameter(objWK, "MARKS_NUMBERS_IN", rowBkg["MARKS_NUMBER"].ToString());

                SetParameter(objWK, "POO_FK_IN", rowBkg["POO_FK"].ToString(), 1);

                SetParameter(objWK, "PFD_FK_IN", rowBkg["PFD_FK"].ToString(), 1);

                SetParameter(objWK, "PROFITABILITY_FLAG_IN", (string.IsNullOrEmpty(rowBkg["PROFITABILITY_FLAG"].ToString()) ? "0" : rowBkg["PROFITABILITY_FLAG"].ToString()), 1);

                SetParameter(objWK, "EDI_BOOKING_MST_FK_IN", rowBkg["EDI_BOOKING_MST_FK"].ToString(), 1);

                SetParameter(objWK, "UPLOADED_VIA_EDI_IN", rowBkg["UPLOADED_VIA_EDI"].ToString());

                SetParameter(objWK, "EDI_STATUS_IN", rowBkg["EDI_STATUS"].ToString(), 1);

                SetParameter(objWK, "SPACE_REQUEST_PK_IN", rowBkg["SPACE_REQUEST_PK"].ToString(), 1);

                SetParameter(objWK, "SPACE_REQUEST_NR_IN", rowBkg["SPACE_REQUEST_NR"].ToString());

                SetParameter(objWK, "SR_STATUS_IN", rowBkg["SR_STATUS"].ToString(), 1);

                SetParameter(objWK, "SEND_REQ_FLAG_IN", rowBkg["SEND_REQ_FLAG"].ToString(), 1);

                SetParameter(objWK, "CONTAINER_TYPE_MST_FK_IN", rowBkg["CONTAINER_TYPE_MST_FK"].ToString(), 1);

                SetParameter(objWK, "CARGO_READY_BY_IN", rowBkg["CARGO_READY_BY"].ToString(), 3);

                SetParameter(objWK, "PICKUP_AVAIL_FROM_IN", rowBkg["PICKUP_AVAIL_FROM"].ToString(), 3);

                SetParameter(objWK, "PICKUP_AVAIL_TO_IN", rowBkg["PICKUP_AVAIL_TO"].ToString(), 3);

                SetParameter(objWK, "PICKUP_ADDRESS_IN", rowBkg["PICKUP_ADDRESS"].ToString());

                SetParameter(objWK, "DROPOFF_ADDRESS_IN", rowBkg["DROPOFF_ADDRESS"].ToString());

                SetParameter(objWK, "HANDLING_INFO_IN", rowBkg["HANDLING_INFO"].ToString());

                SetParameter(objWK, "REMARKS_IN", rowBkg["REMARKS"].ToString());

                if (Nomination == true & string.IsNullOrEmpty(rowBkg["NOMINATION_REF_NO"].ToString()))
                {
                    SetParameter(objWK, "NOMINATION_REF_NO_IN", BookingRefNo);
                }
                else
                {
                    SetParameter(objWK, "NOMINATION_REF_NO_IN", rowBkg["NOMINATION_REF_NO"].ToString());
                }

                SetParameter(objWK, "PO_NUMBER_IN", rowBkg["PO_NUMBER"].ToString());

                SetParameter(objWK, "PO_DATE_IN", rowBkg["PO_DATE"].ToString(), 3);

                SetParameter(objWK, "SALES_CALL_FK_IN", rowBkg["SALES_CALL_FK"].ToString(), 1);

                SetParameter(objWK, "EXECUTIVE_MST_FK_IN", rowBkg["EXECUTIVE_MST_FK"].ToString(), 1);

                SetParameter(objWK, "REMARKS_NEW_IN", rowBkg["REMARKS_NEW"].ToString());

                SetParameter(objWK, "LINE_BKG_DT_IN", rowBkg["LINE_BKG_DT"].ToString(), 3);

                SetParameter(objWK, "BUSINESS_TYPE_IN", rowBkg["BUSINESS_TYPE"].ToString(), 1);

                SetParameter(objWK, "VOLUME_WEIGHT_IN", rowBkg["VOLUME_WEIGHT"].ToString(), 2);

                SetParameter(objWK, "DENSITY_IN", rowBkg["DENSITY"].ToString(), 2);

                SetParameter(objWK, "NO_OF_BOXES_IN", rowBkg["NO_OF_BOXES"].ToString(), 1);

                SetParameter(objWK, "AIRLINE_SCHEDULE_TRN_FK_IN", rowBkg["AIRLINE_SCHEDULE_TRN_FK"].ToString(), 1);

                SetParameter(objWK, "CUSTOMS_CODE_MST_FK_IN", rowBkg["CUSTOMS_CODE_MST_FK"].ToString(), 1);

                SetParameter(objWK, "PORT_MST_POL_FK_IN", rowBkg["PORT_MST_POL_FK"].ToString(), 1);

                SetParameter(objWK, "PORT_MST_POD_FK_IN", rowBkg["PORT_MST_POD_FK"].ToString(), 1);

                SetParameter(objWK, "CUSTOMER_MST_FK_IN", rowBkg["CUSTOMER_MST_FK"].ToString(), 1);

                SetParameter(objWK, "NOTIFY1_CUST_MST_FK_IN", rowBkg["NOTIFY1_CUST_MST_FK"].ToString(), 1);

                SetParameter(objWK, "TRANSPORTER_PLR_FK_IN", PLRTransPK.ToString(), 1);
                SetParameter(objWK, "TRANSPORTER_PFD_FK_IN", PFDTransPK.ToString(), 1);
                SetParameter(objWK, "CONTAINER_OWNER_TYPE_FK_IN", rowBkg["CONTAINER_OWNER_TYPE_FK"].ToString(), 1);
                SetParameter(objWK, "APPLY_LOCAL_CHARGES_IN", rowBkg["APPLY_LOCAL_CHARGES"].ToString(), 1);

                SetParameter(objWK, "THIRD_PARTY_FRTPAYER_FK_IN", rowBkg["THIRD_PARTY_FRTPAYER_FK"].ToString(), 1);
                SetParameter(objWK, "TARIFF_AGENT_MST_FK_IN", rowBkg["TARIFF_AGENT_MST_FK"].ToString(), 1);
                SetParameter(objWK, "COLLECT_AGENT_FLAG_IN", rowBkg["COLLECT_AGENT_FLAG"].ToString(), 1);
                SetParameter(objWK, "LOCAL_NOMINATE_FLAG_IN", rowBkg["LOCAL_NOMINATE_FLAG"].ToString(), 1);

                //SetParameter(objWK, "CONFIG_PK_IN", M_Configuration_PK)
                _with13.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("RESTRICTED_IN", Restriction).Direction = ParameterDirection.Input;

                //SetParameter(objWK, "COL_PLACE_MST_FK_IN", rowBkg.Item("COL_PLACE_MST_FK"), 1)
                _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Output;

                _with13.ExecuteNonQuery();
                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "booking") > 0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    if (!IsUpdate)
                    {
                        if (Nomination)
                        {
                            RollbackProtocolKey((BizType == 2 ? "NOMINATION SEA" : "NOMINATION AIR"), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                        }
                        else
                        {
                            RollbackProtocolKey((BizType == 2 ? "BOOKING (SEA)" : "BOOKING (AIR)"), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                        }
                    }
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValueMain = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                //Updated
                arrMessage = SaveBookingCDimension(dsMain, _PkValueMain, objWK.MyCommand, IsUpdate, Measure, Wt, Divfac);
                //Save Cargo Dimension
                arrMessage = SaveBookingOFreight(dsMain, _PkValueMain, objWK.MyCommand, IsUpdate);
                //Save Other Freights/Flat Freights
                arrMessage = SaveBookingTRN(dsMain, dsCtrDetails, _PkValueMain, objWK.MyCommand, IsUpdate);
                //Save the Transaction and Freights
                if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                {
                    return arrMessage;
                }
                //If Not IsUpdate Then
                objCargoDetails.TransferToMainTbl(objWK, Convert.ToInt32(_PkValueMain));
                if (Convert.ToString(arrMessage[arrMessage.Count - 1]).ToUpper().IndexOf("SAVED") < 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //'Else

                //If Not IsNothing(Session("ctrdetails")) Then
                //    Dim dsCargo As New DataSet
                //    dsCargo = CType(Session("ctrdetails"), DataSet)
                //    objCargoDetails.SaveCargo(dsCargo, objWK)
                //    Session("CtrDetails") = Nothing
                //End If
                //---------------------------------------------------------------------
                DataTable dtContainer = new DataTable();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT TRN.BOOKING_TRN_PK,");
                sb.Append("                TRN.CONTAINER_TYPE_MST_FK,");
                sb.Append("                TRN.NO_OF_BOXES,");
                sb.Append("                BST.BOOKING_REF_NO,");
                sb.Append("                BST.CARGO_TYPE");
                sb.Append("  FROM BOOKING_TRN TRN, BOOKING_MST_TBL BST");
                sb.Append(" WHERE BST.BOOKING_MST_PK = TRN.BOOKING_MST_FK");
                sb.Append("   AND BST.BUSINESS_TYPE=2 AND BST.CARGO_TYPE = 1");
                sb.Append("   AND TRN.BOOKING_MST_FK =" + _PkValueMain);

                var _with14 = objWK.MyCommand;
                _with14.Parameters.Clear();
                _with14.CommandType = CommandType.Text;
                _with14.CommandText = sb.ToString();
                OracleDataAdapter adp = new OracleDataAdapter();
                adp = new OracleDataAdapter(objWK.MyCommand);
                adp.Fill(dtContainer);
                foreach (DataRow _cont in dtContainer.Rows)
                {
                    DataTable dtCargoDet = new DataTable();
                    int BoxCount = 1;
                    try
                    {
                        BoxCount = Convert.ToInt32(_cont["NO_OF_BOXES"]);
                    }
                    catch (Exception ex)
                    {
                        BoxCount = 1;
                    }
                    var _with15 = objWK.MyCommand;
                    _with15.Parameters.Clear();
                    _with15.CommandType = CommandType.Text;
                    _with15.CommandText = "SELECT * FROM BOOKING_TRN_CARGO_DTL BCD WHERE BCD.BOOKING_TRN_FK=" + _cont["BOOKING_TRN_PK"];
                    adp = new OracleDataAdapter(objWK.MyCommand);
                    adp.Fill(dtCargoDet);
                    if (dtCargoDet.Rows.Count < BoxCount)
                    {
                        for (int count = 0; count <= BoxCount - 1; count++)
                        {
                            var _with16 = objWK.MyCommand;
                            _with16.Parameters.Clear();
                            _with16.CommandType = CommandType.Text;
                            sb = new System.Text.StringBuilder();
                            sb.Append("INSERT INTO BOOKING_TRN_CARGO_DTL");
                            sb.Append("  (BOOKING_TRN_CARGO_PK,BOOKING_TRN_FK,CREATED_DT,LAST_MODIFIED_DT,");
                            sb.Append("   CREATED_BY_FK,LAST_MODIFIED_BY_FK,GROSS_WEIGHT,NET_WEIGHT,PACK_COUNT,VERSION_NO,VOLUME_IN_CBM)");
                            sb.Append("VALUES");
                            sb.Append("  (SEQ_BOOKING_TRN_CARGO_DTL.NEXTVAL," + _cont["BOOKING_TRN_PK"] + ", ");
                            sb.Append("    SYSDATE, SYSDATE," + HttpContext.Current.Session["USER_PK"] + "," + HttpContext.Current.Session["USER_PK"] + ", 0, 0, 0, 0, 0) ");

                            _with16.CommandText = sb.ToString();
                            _with16.ExecuteNonQuery();
                        }
                    }
                }
                //End If

                if ((HttpContext.Current.Session["SessionLocalCharges"] != null))
                {
                    Cls_Quotation ObjQuotation = new Cls_Quotation();
                    //Pass 0 FOR EXPORT AND 1 FOR IMPORT
                    ObjQuotation.SaveLocalCharges(objWK.MyCommand, objWK.MyUserName, (DataSet)HttpContext.Current.Session["SessionLocalCharges"], _PkValueMain, BizType, false, (Nomination ? 1 : 0), 0, 2);
                }
                if (SplitBooking == 1)
                {
                    BookingRefNo = GenerateBookingNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, BizType);
                    if (BookingRefNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        return arrMessage;
                    }
                    arrMessage = SaveNewBooking(_PkValueMain, objWK, HiddenNewPCount, HiddenNewAWt, HiddenNewChWt, HiddenNewDensity, HiddenNewULDCnt, HiddenNewVolWt, HiddenNewVolCBM, BookingRefNo, Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"]));
                    NewBookingPK = arrMessage[1].ToString();
                    arrMessage.Add(BookingRefNo);
                }
                //---------------------------------------------------------------------
                string JCPks = "";
                //Job card inserted only for Booking Confirmation For first time
                if ((Convert.ToInt32(strBStatus) == 2 | (Convert.ToInt32(strBStatus) == 5 & IsUpdate == false)))
                {
                    arrMessage = SaveJobCard(_PkValueMain, objWK, Convert.ToString(nLocationId), PODLocfk, ShipperPK, Consigne, strVoyagepk, JCPks);
                }
                if (Convert.ToInt32(rowBkg["STATUS"]) == 2)
                {
                    if (BizType == 2)
                    {
                        //Booking Confirmed through updation
                        if (IsUpdate == true)
                        {
                            arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(_PkValueMain), 2, 1, "Job Card", "JOB-INS-SEA-EXP", Convert.ToInt32(nLocationId), objWK, "UPD", M_CREATED_BY_FK, "O");
                            //New Confirmed Booking
                        }
                        else if (IsUpdate == false)
                        {
                            arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(_PkValueMain), 2, 1, "Job Card", "JOB-INS-SEA-EXP", Convert.ToInt32(nLocationId), objWK, "INS", M_CREATED_BY_FK, "O");
                        }
                    }
                    else
                    {
                        //Booking Confirmed through updation
                        if (IsUpdate == true)
                        {
                            arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(_PkValueMain), 1, 1, "Job Card", "JOB-INS-AIR-EXP", Convert.ToInt32(nLocationId), objWK, "UPD", M_CREATED_BY_FK, "O");
                            //New Confirmed Booking
                        }
                        else if (IsUpdate == false)
                        {
                            arrMessage = objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(_PkValueMain), 1, 1, "Job Card", "JOB-INS-AIR-EXP", Convert.ToInt32(nLocationId), objWK, "INS", M_CREATED_BY_FK, "O");
                        }
                    }
                }

                //'
                for (i = 0; i <= dsMain.Tables["tblTransaction"].Rows.Count - 1; i++)
                {
                    string _transRefFrom = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[i]["TRANS_REFERED_FROM"]);
                    if (_transRefFrom == "4" | _transRefFrom == "6" | _transRefFrom == "7" | _transRefFrom == "8")
                    {
                        isTrue = true;
                    }
                }
                //'Auto creation of quotation while saving the booking(If quotation not there)
                if (IsUpdate == false & isTrue == true)
                {
                    DataSet DSQuotation = null;
                    //'If quotation is there then Amend the existing quotaion
                    bool Update = false;
                    string QuoteNumber = null;
                    string PrevQuotaion = null;
                    Int16 nIndex = default(Int16);
                    string strExtendedQTN = null;
                    if (Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[0]["TRANS_REFERED_FROM"]) == 1)
                    {
                        PrevQuotaion = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[0]["TRANS_REF_NO"]);
                        DSQuotation = (DataSet)GetQuotationDetails(PrevQuotaion);
                        QuoteNumber = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[0]["TRANS_REF_NO"]);
                        nIndex = Convert.ToInt16(QuoteNumber.Trim().IndexOf("/"));
                        if (nIndex > 0)
                        {
                            strExtendedQTN = QuoteNumber.Substring(nIndex + 1);
                            strExtendedQTN = Convert.ToString(Convert.ToInt32(strExtendedQTN) + 1);
                            QuoteNumber = QuoteNumber.Substring(0, nIndex + 1);
                        }
                        else
                        {
                            strExtendedQTN = "/1";
                        }
                        QuoteNumber = QuoteNumber + strExtendedQTN;
                        Update = true;
                    }
                    else
                    {
                        Update = false;
                    }
                    SaveQuotation(DSQuotation, _PkValueMain, objWK, Update, TRAN, ShipperPK, QuoteNumber, CargoType, PrevQuotaion, Cust_Type,
                    Nomination, BizType);
                }
                //'
                //'
                string CurrFKs = "0";
                System.DateTime BkgDt = default(System.DateTime);
                cls_Operator_Contract objContract = new cls_Operator_Contract();
                BkgDt = Convert.ToDateTime(rowBkg["BOOKING_DATE"]);
                for (int nRowCnt = 0; nRowCnt <= dsMain.Tables["tblFreight"].Rows.Count - 1; nRowCnt++)
                {
                    CurrFKs += "," + dsMain.Tables["tblFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"];
                }
                objContract.UpdateTempExRate(_PkValueMain, objWK, CurrFKs, BkgDt, "BOOKING");
                //'
                OracleDataReader oRead = null;
                string EmailId = null;
                string CustBID = null;
                string statusBKG = null;
                System.Text.StringBuilder strsql = new System.Text.StringBuilder();
                Int32 chk = 0;
                if (arrMessage.Count > 0)
                {
                    if (string.Compare((Convert.ToString(arrMessage[0]).ToUpper()), "SAVED") > 0)
                    {
                        arrMessage.Clear();
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();

                        //Push to financial system if realtime is selected
                        if (!string.IsNullOrEmpty(JCPks))
                        {
                            cls_Scheduler objSch = new cls_Scheduler();
                            ArrayList schDtls = null;
                            bool errGen = false;
                            if (objSch.GetSchedulerPushType() == true)
                            {
                                //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                                //try
                                //{
                                //    schDtls = objSch.FetchSchDtls();
                                //    //'Used to Fetch the Sch Dtls
                                //    objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , JCPks);
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                                //    }
                                //}
                                //catch (Exception ex)
                                //{
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                                //    }
                                //}
                            }
                        }
                        //*****************************************************************
                        if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                        {
                            statusBKG = Convert.ToString(rowBkg["STATUS"]);
                            if (!string.IsNullOrEmpty(BookingRefNo) & Chk_EBK == 1)
                            {
                                string BkgDate = null;
                                BkgDate = FetchBkgDate(BookingRef);
                                if (statusBKG == "3")
                                {
                                    strsql.Append(" select addr.email_id email_id, book.cust_reg_nr custid from syn_ebk_m_booking book,syn_ebk_t_cust_address addr where book.qbso_bkg_ref_nr like '" + BookingRef.ToUpper() + "' ");
                                    strsql.Append(" and book.cust_reg_nr=addr.regn_nr_fk and addr.address_type=0 group by addr.email_id,book.cust_reg_nr ");
                                    oRead = objWK.GetDataReader(strsql.ToString());
                                    while ((oRead.Read()))
                                    {
                                        chk = 1;
                                        EmailId = Convert.ToString(oRead.GetValue(0));
                                        CustBID = Convert.ToString(oRead.GetValue(1));
                                    }
                                    oRead.Close();
                                }
                                DataSet dscust = new DataSet();
                                Int32 Bstatus = Convert.ToInt32(rowBkg["STATUS"]);
                                if (statusBKG == "2")
                                {
                                    strsql.Append(" select addr.email_id email_id, book.cust_reg_nr custid from syn_ebk_m_booking book,syn_ebk_t_cust_address addr where book.qbso_bkg_ref_nr like '" + BookingRef.ToUpper() + "' ");
                                    strsql.Append(" and book.cust_reg_nr=addr.regn_nr_fk and addr.address_type=0 group by addr.email_id,book.cust_reg_nr ");
                                    oRead = objWK.GetDataReader(strsql.ToString());
                                    while ((oRead.Read()))
                                    {
                                        chk = 1;
                                        EmailId = Convert.ToString(oRead.GetValue(0));
                                        CustBID = Convert.ToString(oRead.GetValue(1));
                                    }
                                    oRead.Close();
                                }
                                if (statusBKG == "2" | statusBKG == "3")
                                {
                                    if (chk > 0)
                                    {
                                        SendMail(EmailId, CustBID, BookingRef, EbkgRefno, Bstatus);
                                    }
                                }
                                return arrMessage;
                            }
                        }
                        else
                        {
                            txtBookingNo = BookingRefNo;
                        }
                        //Manoharan 11June2007: to send proper message if Bkg Cancelled.
                        if (Convert.ToInt32(rowBkg["STATUS"]) == 3)
                        {
                            arrMessage.Clear();
                            arrMessage.Add("Booking Cancelled Sucessfully.");
                        }
                        arrMessage.Add(_PkValueMain);
                        arrMessage.Add(BookingRefNo);
                        return arrMessage;
                    }
                    else
                    {
                        if (!IsUpdate)
                        {
                            if (Nomination)
                            {
                                RollbackProtocolKey((BizType == 2 ? "NOMINATION SEA" : "NOMINATION AIR"), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                            }
                            else
                            {
                                RollbackProtocolKey((BizType == 2 ? "BOOKING (SEA)" : "BOOKING (AIR)"), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                            }
                        }
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                if (!IsUpdate)
                {
                    if (Nomination)
                    {
                        RollbackProtocolKey((BizType == 2 ? "NOMINATION SEA" : "NOMINATION AIR"), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                    }
                    else
                    {
                        RollbackProtocolKey((BizType == 2 ? "BOOKING (SEA)" : "BOOKING (AIR)"), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                    }
                }
                TRAN.Rollback();
                arrMessage.Clear();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                if (!IsUpdate)
                {
                    if (Nomination)
                    {
                        RollbackProtocolKey((BizType == 2 ? "NOMINATION SEA" : "NOMINATION AIR"), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                    }
                    else
                    {
                        RollbackProtocolKey((BizType == 2 ? "BOOKING (SEA)" : "BOOKING (AIR)"), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                    }
                }
                TRAN.Rollback();
                arrMessage.Clear();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }

        public ArrayList SaveBookingTRN(DataSet dsMain, DataSet dsCtrDetails, long PkValue, OracleCommand SelectCommand, bool IsUpdate, short BizType = 2)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string strValueArgument = null;
            DataSet dsGrid = new DataSet();
            if ((HttpContext.Current.Session["ctrdetails"] != null))
            {
                dsGrid = (DataSet)HttpContext.Current.Session["ctrdetails"];
            }
            List<SpecialReqClass> listSpReq = null;
            if ((HttpContext.Current.Session["SPECIAL_REQ"] != null))
            {
                listSpReq = HttpContext.Current.Session["SPECIAL_REQ"] as List<SpecialReqClass>;
            }
            arrMessage.Clear();
            SpecialReqClass _itemReq = default(SpecialReqClass);
            try
            {
                if (!IsUpdate)
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        if ((listSpReq != null))
                        {
                            //_itemReq = (from _l in listSpReq_lwhere _l.RECORD_INDEX == nRowCnt).FirstOrDefault();
                        }
                    }
                    var _with1 = SelectCommand;
                    _with1.CommandType = CommandType.StoredProcedure;
                    _with1.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_TRN_INS";
                    SelectCommand.Parameters.Clear();

                    _with1.Parameters.Add("BOOKING_MST_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;
                    _with1.Parameters.Add("TRANS_REFERED_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]).Direction = ParameterDirection.Input;
                    _with1.Parameters.Add("TRANS_REF_NO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString()))
                    {
                        _with1.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                    }
                    _with1.Parameters.Add("NO_OF_BOXES_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["NO_OF_BOXES"]).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
                    {
                        _with1.Parameters.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("BASIS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"].ToString()))
                    {
                        _with1.Parameters.Add("QUANTITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("QUANTITY_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_GROUP_FK"].ToString()))
                    {
                        _with1.Parameters.Add("COMMODITY_GROUP_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("COMMODITY_GROUP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"].ToString()))
                    {
                        _with1.Parameters.Add("COMMODITY_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("COMMODITY_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"].ToString()))
                    {
                        _with1.Parameters.Add("ALL_IN_TARIFF_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("ALL_IN_TARIFF_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"].ToString()))
                    {
                        _with1.Parameters.Add("BUYING_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("BUYING_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VOLUME_CBM"].ToString()))
                    {
                        _with1.Parameters.Add("VOLUME_CBM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("VOLUME_CBM_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["VOLUME_CBM"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["WEIGHT_MT"].ToString()))
                    {
                        _with1.Parameters.Add("WEIGHT_MT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("WEIGHT_MT_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["WEIGHT_MT"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PACK_TYPE_FK"].ToString()))
                    {
                        _with1.Parameters.Add("PACK_TYPE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("PACK_TYPE_FK_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PACK_TYPE_FK"])).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FKS"].ToString()))
                    {
                        _with1.Parameters.Add("COMMODITY_MST_FKS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("COMMODITY_MST_FKS_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FKS"])).Direction = ParameterDirection.Input;
                    }

                    _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with1.ExecuteNonQuery();
                    if (string.Compare(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value.ToString()), "bookingtrans") > 0)
                    {
                        arrMessage.Add(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value));
                        return arrMessage;
                    }
                    else
                    {
                        _PkValueTrans = Convert.ToInt64(SelectCommand.Parameters["RETURN_VALUE"].Value);
                        if ((HttpContext.Current.Session["CtrDetails"] != null))
                        {
                            try
                            {
                                dsGrid = (DataSet)HttpContext.Current.Session["CtrDetails"];
                                foreach (DataRow _row in dsGrid.Tables[nRowCnt].Rows)
                                {
                                    _row["BOOKING_TRN_PK"] = _PkValueTrans;
                                }
                                HttpContext.Current.Session["CtrDetails"] = dsGrid;
                            }
                            catch (Exception ex)
                            {
                            }
                        }
                    }
                    if (Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["BUSINESS_TYPE"]) == "2")
                    {
                        if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == 1)
                        {
                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString()))
                            {
                                strValueArgument = dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString();
                            }
                            else
                            {
                                strValueArgument = "";
                            }
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
                            {
                                strValueArgument = dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString();
                            }
                            else
                            {
                                strValueArgument = "";
                            }
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
                        {
                            strValueArgument = dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString();
                        }
                        else
                        {
                            strValueArgument = "";
                        }
                    }

                    arrMessage = SaveBookingFRT(dsMain, _PkValueTrans, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), strValueArgument, Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]));
                    if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                    {
                        return arrMessage;
                    }

                    //Dim CntTypePK As Integer
                    //CntTypePK = IIf(IsDBNull(dsMain.Tables("tblTransaction").Rows(nRowCnt).Item("CONTAINER_TYPE_MST_FK")), 0, dsMain.Tables("tblTransaction").Rows(nRowCnt).Item("CONTAINER_TYPE_MST_FK"))
                    //Dim i As Integer
                    //Dim strSql As String
                    //Dim drCntKind As String
                    //strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_PK= '" & CntTypePK & "'"
                    //drCntKind = objWK.ExecuteScaler(strSql)

                    if (dsMain.Tables["tblTransaction"].Columns.Contains("strSpclReq"))
                    {
                        //arrMessage = SaveSplReqTransaction(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables("tblTransaction").Rows(nRowCnt).Item("strSpclReq"), ""), _PkValueTrans)
                        arrMessage = SaveSplReqTransaction(SelectCommand, objWK.MyUserName, _itemReq, _PkValueTrans);
                        if (arrMessage.Count > 0)
                        {
                            if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                            {
                                return arrMessage;
                            }
                        }
                    }

                    if (Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 1 | Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 2)
                    {
                        arrMessage = (ArrayList)UpdateUpStream(dsMain, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]), Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), 0);

                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            arrMessage.Add("Upstream Updation failed, Please check for valid Data");
                            return arrMessage;
                        }
                    }


                    if (dsMain.Tables["tblTransaction"].Rows.Count > 0)
                    {
                        arrMessage.Add("All data saved successfully");
                        return arrMessage;
                    }
                    else
                    {
                        arrMessage.Add("No Record selected to save!");
                        return arrMessage;
                    }
                }



                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        DataRow rowTRN = dsMain.Tables["tblTransaction"].Rows[nRowCnt];
                        if ((listSpReq != null))
                        {
                            //_itemReq = (from _l in listSpReq_lwhere _l.BKG_TRN_FK == Conversion.Val(rowTRN["BOOKING_TRN_PK"])).FirstOrDefault();
                        }
                        var _with2 = SelectCommand;
                        _with2.CommandType = CommandType.StoredProcedure;
                        _with2.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_TRN_UPD";

                        SelectCommand.Parameters.Clear();

                        _with2.Parameters.Add("BOOKING_TRN_PK_IN", rowTRN["BOOKING_TRN_PK"]).Direction = ParameterDirection.Input;

                        _with2.Parameters.Add("BOOKING_MST_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with2.Parameters.Add("TRANS_REFERED_FROM_IN", rowTRN["TRANS_REFERED_FROM"]).Direction = ParameterDirection.Input;

                        _with2.Parameters.Add("TRANS_REF_NO_IN", rowTRN["TRANS_REF_NO"]).Direction = ParameterDirection.Input;

                        if (string.IsNullOrEmpty(rowTRN["CONTAINER_TYPE_MST_FK"].ToString()))
                        {
                            _with2.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with2.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", rowTRN["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                        }

                        _with2.Parameters.Add("NO_OF_BOXES_IN", rowTRN["NO_OF_BOXES"]).Direction = ParameterDirection.Input;

                        if (!string.IsNullOrEmpty(rowTRN["BASIS"].ToString()))
                        {
                            _with2.Parameters.Add("BASIS_IN", rowTRN["BASIS"]).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with2.Parameters.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }

                        if (!string.IsNullOrEmpty(rowTRN["QUANTITY"].ToString()))
                        {
                            _with2.Parameters.Add("QUANTITY_IN", Convert.ToDouble(rowTRN["QUANTITY"])).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with2.Parameters.Add("QUANTITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }

                        _with2.Parameters.Add("COMMODITY_GROUP_FK_IN", rowTRN["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;

                        _with2.Parameters.Add("COMMODITY_MST_FK_IN", rowTRN["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;

                        if (string.IsNullOrEmpty(rowTRN["ALL_IN_TARIFF"].ToString()))
                        {
                            _with2.Parameters.Add("ALL_IN_TARIFF_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with2.Parameters.Add("ALL_IN_TARIFF_IN", Convert.ToDouble(rowTRN["ALL_IN_TARIFF"])).Direction = ParameterDirection.Input;
                        }

                        if (string.IsNullOrEmpty(rowTRN["BUYING_RATE"].ToString()))
                        {
                            _with2.Parameters.Add("BUYING_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with2.Parameters.Add("BUYING_RATE_IN", Convert.ToDouble(rowTRN["BUYING_RATE"])).Direction = ParameterDirection.Input;
                        }

                        if (string.IsNullOrEmpty(rowTRN["VOLUME_CBM"].ToString()))
                        {
                            _with2.Parameters.Add("VOLUME_CBM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with2.Parameters.Add("VOLUME_CBM_IN", Convert.ToDouble(rowTRN["VOLUME_CBM"])).Direction = ParameterDirection.Input;
                        }

                        if (string.IsNullOrEmpty(rowTRN["WEIGHT_MT"].ToString()))
                        {
                            _with2.Parameters.Add("WEIGHT_MT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with2.Parameters.Add("WEIGHT_MT_IN", Convert.ToDouble(rowTRN["WEIGHT_MT"])).Direction = ParameterDirection.Input;
                        }

                        if (string.IsNullOrEmpty(rowTRN["PACK_TYPE_FK"].ToString()))
                        {
                            _with2.Parameters.Add("PACK_TYPE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with2.Parameters.Add("PACK_TYPE_FK_IN", Convert.ToDouble(rowTRN["PACK_TYPE_FK"])).Direction = ParameterDirection.Input;
                        }

                        if (string.IsNullOrEmpty(rowTRN["COMMODITY_MST_FKS"].ToString()))
                        {
                            _with2.Parameters.Add("COMMODITY_MST_FKS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with2.Parameters.Add("COMMODITY_MST_FKS_IN", Convert.ToDouble(rowTRN["COMMODITY_MST_FKS"])).Direction = ParameterDirection.Input;
                        }

                        _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with2.ExecuteNonQuery();
                        if (string.Compare(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value), "bookingtrans")>0)
                        {
                            arrMessage.Add(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value));
                            return arrMessage;
                        }
                        else
                        {
                            _PkValueTrans = Convert.ToInt64(SelectCommand.Parameters["RETURN_VALUE"].Value);
                            if ((HttpContext.Current.Session["CtrDetails"] != null))
                            {
                                try
                                {
                                    dsGrid = (DataSet)HttpContext.Current.Session["CtrDetails"];
                                    foreach (DataRow _row in dsGrid.Tables[nRowCnt].Rows)
                                    {
                                        _row["BOOKING_TRN_PK"] = _PkValueTrans;
                                    }

                                    HttpContext.Current.Session["CtrDetails"] = dsGrid;
                                }
                                catch (Exception ex)
                                {
                                }
                            }
                        }
                        if (Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["BUSINESS_TYPE"]) == "2")
                        {
                            if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == 1)
                            {
                                if (!string.IsNullOrEmpty(rowTRN["CONTAINER_TYPE_MST_FK"].ToString()))
                                {
                                    strValueArgument = rowTRN["CONTAINER_TYPE_MST_FK"].ToString();
                                }
                                else
                                {
                                    strValueArgument = "";
                                }
                            }
                            else
                            {
                                if (!string.IsNullOrEmpty(rowTRN["BASIS"].ToString()))
                                {
                                    strValueArgument = rowTRN["BASIS"].ToString();
                                }
                                else
                                {
                                    strValueArgument = "";
                                }
                            }
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(rowTRN["BASIS"].ToString()))
                            {
                                strValueArgument = rowTRN["BASIS"].ToString();
                            }
                            else
                            {
                                strValueArgument = "";
                            }
                        }
                        arrMessage = SaveBookingFRT(dsMain, _PkValueTrans, SelectCommand, IsUpdate, Convert.ToString(rowTRN["TRANS_REF_NO"]), strValueArgument, Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]));
                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }
                        //============special requirment=============
                        //Dim CntTypePK As Integer
                        //If Not IsDBNull(rowTRN.Item("CONTAINER_TYPE_MST_FK")) Then
                        //    CntTypePK = rowTRN.Item("CONTAINER_TYPE_MST_FK")
                        //    Dim i As Integer
                        //    Dim strSql As String
                        //    Dim drCntKind As String
                        //    strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_PK= '" & CntTypePK & "'"
                        //    drCntKind = objWK.ExecuteScaler(strSql)
                        if (dsMain.Tables["tblTransaction"].Columns.Contains("strSpclReq"))
                        {
                            arrMessage = SaveSplReqTransaction(SelectCommand, objWK.MyUserName, _itemReq, _PkValueTrans);
                        }
                        //End If
                        if (arrMessage.Count > 0)
                        {
                            if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                            {
                                return arrMessage;
                            }
                        }
                        //========================end============================
                        if (Convert.ToInt32(rowTRN["TRANS_REFERED_FROM"]) == 1 | Convert.ToInt32(rowTRN["TRANS_REFERED_FROM"]) == 2)
                        {
                            arrMessage = (ArrayList)UpdateUpStream(dsMain, SelectCommand, IsUpdate, Convert.ToString(rowTRN["TRANS_REFERED_FROM"]), Convert.ToString(rowTRN["TRANS_REF_NO"]), 0);

                            if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                            {
                                arrMessage.Add("Upstream Updation failed, Please check for valid Data");
                                return arrMessage;
                            }
                        }
                    }
                    if (dsMain.Tables["tblTransaction"].Rows.Count > 0)
                    {
                        arrMessage.Add("All data saved successfully");

                        HttpContext.Current.Session["SPECIAL_REQ"] = listSpReq;
                        return arrMessage;
                    }
                    else
                    {
                        arrMessage.Add("No Record selected to save!");
                        return arrMessage;
                    }
                }
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

        public ArrayList SaveNewBooking(long PkValue, WorkFlow objWF, string HiddenNewPCount = "", string HiddenNewAWt = "", string HiddenNewChWt = "", string HiddenNewDensity = "", string HiddenNewULDCnt = "", string HiddenNewVolWt = "", string HiddenNewVolCBM = "", string BookingRefNo = "",
        string ChargeableWt = "")
        {
            // Dim objWK As New WorkFlow

            string strValueArgument = null;
            arrMessage.Clear();
            try
            {
                var _with17 = objWF.MyCommand;
                _with17.CommandType = CommandType.StoredProcedure;
                _with17.CommandText = objWF.MyUserName + ".BOOKING_AIR_PKG.SPLIT_NEW_BOOKING_SAVE";
                _with17.Parameters.Clear();
                _with17.Parameters.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                _with17.Parameters["BOOKING_AIR_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("HIDDENNEWPCOUNT_IN", (string.IsNullOrEmpty(HiddenNewPCount) ? "" : HiddenNewPCount)).Direction = ParameterDirection.Input;
                _with17.Parameters["HIDDENNEWPCOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("HIDDENNEWAWT_IN", (string.IsNullOrEmpty(HiddenNewAWt) ? "" : HiddenNewAWt)).Direction = ParameterDirection.Input;
                _with17.Parameters["HIDDENNEWAWT_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("HIDDENNEWCHWT_IN", (string.IsNullOrEmpty(HiddenNewChWt) ? "" : HiddenNewChWt)).Direction = ParameterDirection.Input;
                _with17.Parameters["HIDDENNEWCHWT_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("HIDDENNEWDENSITY_IN", (string.IsNullOrEmpty(HiddenNewDensity) ? "" : HiddenNewDensity)).Direction = ParameterDirection.Input;
                _with17.Parameters["HIDDENNEWDENSITY_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("HIDDENNEWULDCOUNT_IN", (string.IsNullOrEmpty(HiddenNewULDCnt) ? "" : HiddenNewULDCnt)).Direction = ParameterDirection.Input;
                _with17.Parameters["HIDDENNEWULDCOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("HIDDENNEWVOLWT_IN", (string.IsNullOrEmpty(HiddenNewVolWt) ? "" : HiddenNewVolWt)).Direction = ParameterDirection.Input;
                _with17.Parameters["HIDDENNEWPCOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("HIDDENNEWVOLCBM_IN", (string.IsNullOrEmpty(HiddenNewVolCBM) ? "" : HiddenNewVolCBM)).Direction = ParameterDirection.Input;
                _with17.Parameters["HIDDENNEWVOLCBM_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("BOOKING_REF_NO_IN", (string.IsNullOrEmpty(BookingRefNo) ? "" : BookingRefNo)).Direction = ParameterDirection.Input;
                _with17.Parameters["BOOKING_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("CHARGEABLE_WT_IN", (string.IsNullOrEmpty(ChargeableWt) ? "" : ChargeableWt)).Direction = ParameterDirection.Input;
                _with17.Parameters["CHARGEABLE_WT_IN"].SourceVersion = DataRowVersion.Current;

                _with17.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with17.ExecuteNonQuery();
                strRet = (string.IsNullOrEmpty(_with17.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with17.Parameters["RETURN_VALUE"].Value.ToString());
                arrMessage.Add("All data saved successfully");
                arrMessage.Add(strRet.ToString());
                return arrMessage;
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

        
               

                
        
		

        //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
        public object SendMail(string MailId, string CUSTOMERID, string BkgRefnr, string EBkgRefnr, Int32 Bstatus)
        {
            System.Web.Mail.MailMessage objMail = new System.Web.Mail.MailMessage();
            //Dim Mailsend As String = ConfigurationSettings.AppSettings("MailServer")
            string EAttach = null;
            string dsMail = null;
            Int32 intCnt = default(Int32);
            System.Text.StringBuilder strhtml = new System.Text.StringBuilder();

            try
            {
                //****************************** External*********************************
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserver"] = "smtpout.secureserver.net";
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserverport"] = 25;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendusing"] = 2;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpauthenticate"] = 1;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendusername"] = "support_temp@quantum-bso.com";
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendpassword"] = "test123";
                objMail.BodyFormat = System.Web.Mail.MailFormat.Html;
                //or MailFormat.Text
                objMail.To = MailId;
                objMail.From = "support_temp@quantum-bso.com";
                if (Bstatus == 3)
                {
                    objMail.Subject = "Booking Cancelled";
                    //objMail.Body = "Dear " & CUSTOMERID & "," & vbCrLf & vbCrLf
                    //objMail.Body &= "Your Request for the E-Booking  is Cancelled "
                    strhtml.Append("<html><body>");
                    strhtml.Append("<p><b>Dear " + CUSTOMERID + " <br>");
                    strhtml.Append("Your Request for the E-Booking " + EBkgRefnr + " is Canceled <br><br>");
                }
                else
                {
                    objMail.Subject = "Booking Confirmation";
                    //objMail.Body = "Dear " & CUSTOMERID & "," & vbCrLf & vbCrLf
                    //objMail.Body &= "Your Request for the E-Booking " & EBkgRefnr & " is confirmed. Please refer your Original Booking Number:" & BkgRefnr
                    strhtml.Append("<html><body>");
                    strhtml.Append("<p><b>Dear " + CUSTOMERID + " <br>");
                    strhtml.Append("Your Request for the E-Booking " + EBkgRefnr + " is confirmed. Please refer your Original Booking Number:" + BkgRefnr + "<br><br>");
                }
                strhtml.Append("This is an Auto Generated Mail. Please do not reply to this Mail-ID.<br>");
                strhtml.Append("</b></p>");
                strhtml.Append("</body></html>");
                objMail.Body = strhtml.ToString();

                System.Web.Mail.SmtpMail.SmtpServer = "smtpout.secureserver.net";
                System.Web.Mail.SmtpMail.Send(objMail);
                objMail = null;
                return "All Data Saved Successfully.";
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                //Throw ex
                return "All Data Saved Successfully. Due To Server Problem Mail Has Not Been Sent.";
            }
        }

        public ArrayList SaveJobCard(long PkValue, WorkFlow objWF, string LocationPK, string PodLocPk, string ShipperPK = "", string ConsignePK = "", string strVoyagePk = "", string JCPks = "")
        {
            string strValueArgument = null;
            int JobFlag = 0;
            arrMessage.Clear();
            JobFlag = CheckJobcard(Convert.ToInt32(PkValue));
            try
            {
                if (JobFlag == 0)
                {
                    var _with18 = objWF.MyCommand;
                    _with18.CommandType = CommandType.StoredProcedure;
                    _with18.CommandText = objWF.MyUserName + ".BOOKING_MST_PKG.AUTO_JOB_CARD_TRN";
                    _with18.Parameters.Clear();
                    _with18.Parameters.Add("BOOKING_MST_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with18.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with18.ExecuteNonQuery();
                    strRet = (string.IsNullOrEmpty(Convert.ToString(_with18.Parameters["RETURN_VALUE"].Value)) ? "" : _with18.Parameters["RETURN_VALUE"].Value.ToString());
                    JCPks = strRet;
                    SaveJobCardTrnCost(PkValue, objWF);
                    ///Added By Sushama
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
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

        ///Added By Sushama
        public string SaveJobCardTrnCost(long BKGPkValue, WorkFlow objWF)
        {
            string strValueArgument = null;
            int JobFlag = 0;
            DataSet DSBkg = new DataSet();
            long PRCFK = PRECARRIAGE;
            long ONCFK = ONCARRIAGE;
            decimal costAmt = default(decimal);
            string strCostDetails = null;
            long TransporterFK = 0;
            decimal chrgwt = default(decimal);
            long POL_PK = 0;
            long POD_PK = 0;
            long PLR_PK = 0;
            long PFD_PK = 0;
            long CargoType = 0;
            long Location_mst_FK = 0;
            long Base_Currency_FK = 0;
            string strArrRate = null;
            short BizType = 0;
            short ProcessType = 0;
            string[] strArrCostAmt = null;
            DataSet dsCostDetails = new DataSet();
            arrMessage.Clear();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            int JCPKValue = 0;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                sb.Append("SELECT JC.job_card_trn_pk");
                sb.Append("      FROM JOB_CARD_TRN JC ");
                sb.Append("     WHERE JC.BOOKING_MST_FK = " + BKGPkValue);
                objWF.MyCommand.CommandType = CommandType.Text;
                objWF.MyCommand.CommandText = sb.ToString();
                JCPKValue = Convert.ToInt32(objWF.MyCommand.ExecuteScalar());

                sb.Clear();
                sb.Append("SELECT BMT.POO_FK AS PLR_FK,0 PFD_FK,");
                sb.Append("     BMT.PORT_MST_POL_FK AS PORT_MST_POL_FK,");
                sb.Append("     0 AS PORT_MST_POD_FK,");
                sb.Append("     CASE WHEN BMT.CARGO_TYPE = 1 THEN BMT.GROSS_WEIGHT ");
                sb.Append("     ELSE BMT.CHARGEABLE_WEIGHT END CHARGEABLE_WEIGHT,");
                sb.Append("     BMT.TRANSPORTER_PLR_FK AS TRANSPORTER_FK,");
                sb.Append("     BMT.CARGO_TYPE,POL.LOCATION_MST_FK ,CONTT.CURRENCY_MST_FK,BMT.BUSINESS_TYPE,BMT.FROM_FLAG,VMT.VENDOR_ID , 'PRC' PRC_FLAG");
                sb.Append("   FROM BOOKING_MST_TBL BMT, PORT_MST_TBL POL, PORT_MST_TBL POD,");
                sb.Append("   COUNTRY_MST_TBL CONTT, LOCATION_MST_TBL LOC,VENDOR_MST_TBL VMT");
                sb.Append("   WHERE BMT.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                sb.Append("   AND BMT.PORT_MST_POD_FK = POD.PORT_MST_PK AND VMT.VENDOR_MST_PK(+) = BMT.TRANSPORTER_PLR_FK ");
                sb.Append("   AND LOC.COUNTRY_MST_FK = CONTT.COUNTRY_MST_PK ");
                sb.Append("   AND LOC.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                sb.Append("   AND BMT.BOOKING_MST_PK =" + BKGPkValue);
                sb.Append(" UNION SELECT 0 PLR_FK,BMT.PFD_FK AS PFD_FK,");
                sb.Append("     0 AS PORT_MST_POL_FK,");
                sb.Append("     BMT.PORT_MST_POD_FK AS PORT_MST_POD_FK,");
                sb.Append("     CASE WHEN BMT.CARGO_TYPE = 1 THEN BMT.GROSS_WEIGHT ");
                sb.Append("     ELSE BMT.CHARGEABLE_WEIGHT END CHARGEABLE_WEIGHT,");
                sb.Append("     BMT.TRANSPORTER_PFD_FK AS TRANSPORTER_FK,");
                sb.Append("     BMT.CARGO_TYPE,POL.LOCATION_MST_FK ,CONTT.CURRENCY_MST_FK,BMT.BUSINESS_TYPE,BMT.FROM_FLAG,VMT.VENDOR_ID , 'ONC' PRC_FLAG");
                sb.Append("   FROM BOOKING_MST_TBL BMT, PORT_MST_TBL POL, PORT_MST_TBL POD,");
                sb.Append("   COUNTRY_MST_TBL CONTT, LOCATION_MST_TBL LOC,VENDOR_MST_TBL VMT");
                sb.Append("   WHERE BMT.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                sb.Append("   AND BMT.PORT_MST_POD_FK = POD.PORT_MST_PK  AND VMT.VENDOR_MST_PK(+) = BMT.TRANSPORTER_PFD_FK ");
                sb.Append("   AND LOC.COUNTRY_MST_FK = CONTT.COUNTRY_MST_PK ");
                sb.Append("   AND LOC.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                sb.Append("   AND BMT.BOOKING_MST_PK =" + BKGPkValue);
                objWF.MyCommand.CommandType = CommandType.Text;
                objWF.MyCommand.Parameters.Clear();
                objWF.MyCommand.CommandText = sb.ToString();
                objWF.MyDataAdapter = new OracleDataAdapter(objWF.MyCommand);
                objWF.MyDataAdapter.Fill(DSBkg);

                if (DSBkg.Tables[0].Rows.Count > 0)
                {
                    for (int j = 0; j <= DSBkg.Tables[0].Rows.Count - 1; j++)
                    {
                        TransporterFK = Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["TRANSPORTER_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["TRANSPORTER_FK"].ToString());
                        chrgwt =
                        Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["CHARGEABLE_WEIGHT"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["CHARGEABLE_WEIGHT"].ToString());
                        POL_PK =
                        Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["PORT_MST_POL_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["PORT_MST_POL_FK"].ToString());
                        POD_PK =
                        Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["PORT_MST_POD_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["PORT_MST_POD_FK"].ToString());
                        PLR_PK =
                        Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["PLR_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["PLR_FK"].ToString());
                        PFD_PK =
                        Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["PFD_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["PFD_FK"].ToString());
                        CargoType =
                         Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["CARGO_TYPE"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["CARGO_TYPE"].ToString());
                        Location_mst_FK =
                        Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["LOCATION_MST_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["LOCATION_MST_FK"].ToString());
                        Base_Currency_FK =
                        Convert.ToInt64(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["CURRENCY_MST_FK"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["CURRENCY_MST_FK"].ToString());
                        BizType =
                        Convert.ToInt16(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["BUSINESS_TYPE"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["BUSINESS_TYPE"].ToString());
                        ProcessType =
                        Convert.ToInt16(string.IsNullOrEmpty(DSBkg.Tables[0].Rows[j]["FROM_FLAG"].ToString()) ? "0" : DSBkg.Tables[0].Rows[j]["BUSINESSFROM_FLAG_TYPE"].ToString());

                        dsCostDetails = GetPRCONCCostDetails(objWF, TransporterFK, Convert.ToInt64(chrgwt), POL_PK, POD_PK, PLR_PK, PFD_PK, JCPKValue, CargoType, BizType,
                        ProcessType);

                        if (dsCostDetails.Tables[0].Rows.Count > 0)
                        {
                            for (int i = 0; i <= dsCostDetails.Tables[0].Rows.Count - 1; i++)
                            {
                                if ((Convert.ToInt64(dsCostDetails.Tables[0].Rows[i]["cost_element_mst_pk"]) == PRCFK & DSBkg.Tables[0].Rows[j]["PRC_FLAG"] == "PRC") | (Convert.ToInt64(dsCostDetails.Tables[0].Rows[i]["cost_element_mst_pk"]) == ONCFK & DSBkg.Tables[0].Rows[j]["PRC_FLAG"] == "ONC"))
                                {
                                    strArrRate = dsCostDetails.Tables[0].Rows[i]["COST_AMOUNT"].ToString();
                                    strArrCostAmt = strArrRate.Split(',');
                                    if (strArrCostAmt.Length > 3)
                                    {
                                        if (Convert.ToInt32(strArrCostAmt[0]) == 0 | Convert.ToInt32(strArrCostAmt[1]) == 0)
                                        {
                                        }
                                        else
                                        {
                                            if (Convert.ToInt32(DSBkg.Tables[0].Rows[0]["CARGO_TYPE"]) == 1)
                                            {
                                                costAmt = Convert.ToDecimal(Math.Round(Convert.ToDouble(strArrCostAmt[1]) * 100) / 100);
                                            }
                                            else
                                            {
                                                costAmt = Convert.ToDecimal(Math.Round(Convert.ToDouble(strArrCostAmt[1]) * Convert.ToDouble(DSBkg.Tables[0].Rows[i]["CHARGEABLE_WEIGHT"]) * 100) / 100);
                                            }
                                            objWF.OpenConnection();
                                            var _with19 = objWF.MyCommand;
                                            _with19.CommandType = CommandType.StoredProcedure;
                                            _with19.CommandText = objWF.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_COST_INS";
                                            _with19.Parameters.Clear();
                                            _with19.Parameters.Add("JOB_CARD_TRN_FK_IN", JCPKValue).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("VENDOR_MST_FK_IN", TransporterFK).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("COST_ELEMENT_FK_IN", dsCostDetails.Tables[0].Rows[i]["cost_element_mst_pk"]).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("LOCATION_FK_IN", Location_mst_FK).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("VENDOR_KEY_IN", DSBkg.Tables[0].Rows[j]["VENDOR_ID"]).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("PTMT_TYPE_IN", 1).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("CURRENCY_MST_FK_IN", Base_Currency_FK).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("ESTIMATED_COST_IN", costAmt).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("TOTAL_COST_IN", Convert.ToDouble(strArrCostAmt[3]) * Convert.ToDouble(costAmt)).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("SURCHARGE_IN", 0.0).Direction = ParameterDirection.Input;
                                            _with19.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                            _with19.ExecuteNonQuery();
                                            strRet = (string.IsNullOrEmpty(_with19.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with19.Parameters["RETURN_VALUE"].Value.ToString());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                arrMessage.Add("All data saved successfully");
                return Convert.ToString(arrMessage[0].ToString());
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

        public DataSet GetPRCONCCostDetails(WorkFlow objWF, long TransporterFK, long chrgwt, long POL_PK, long POD_PK, long PLR_PK, long PFD_PK, long JobCardPK, long CargoType, short bizType,
        short ProcessType)
        {
            string strValueArgument = null;
            int JobFlag = 0;
            DataSet DSCost = new DataSet();
            long PRCFK = PRECARRIAGE;
            long ONCFK = ONCARRIAGE;
            arrMessage.Clear();
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append(" SELECT DISTINCT c.cost_element_mst_pk, c.cost_element_id  ");
                sb.Append(" ,c.cost_element_name  ");
                sb.Append(" , NVL(FETCH_TRSPT_CONTR_RATE(" + TransporterFK + ", " + chrgwt + ", " + PLR_PK + "," + PFD_PK + ", " + bizType);
                sb.Append(" ," + ProcessType + ", c.cost_element_mst_pk , " + JobCardPK + ", " + CargoType + "),0) AS COST_AMOUNT , ");
                sb.Append("  CASE when FCT.BASIS =1 then (FCT.BASIS_VALUE || '" + " % of " + "' || CONCADINATE_FUN_FREIGHTELEMENT(C.COST_ELEMENT_MST_PK, 1,' " + POL_PK + "','" + POD_PK + "'))  else null end as FRT_ELEMENT ");
                sb.Append(" FROM cost_element_mst_tbl c, FREIGHT_CONFIG_TRN_TBL FCT, SECTOR_MST_TBL SMT ");
                sb.Append(" WHERE ");
                sb.Append(" (c.business_type = " + bizType + " or c.business_type = 3)  ");
                sb.Append(" AND C.COST_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK(+) ");
                sb.Append(" AND SMT.SECTOR_MST_PK(+) = FCT.SECTOR_MST_FK ");
                sb.Append(" AND c.vendor_type_mst_fk in ");
                sb.Append("(select vs.vendor_type_fk ");
                sb.Append(" from vendor_services_trn vs");
                sb.Append(" where vs.vendor_mst_fk = " + TransporterFK);
                sb.Append(" and vs.vendor_type_fk in ");
                sb.Append(" (select v.vendor_type_pk ");
                sb.Append(" from vendor_type_mst_tbl v ");
                sb.Append(" where v.active_flag = 1)) ");
                sb.Append(" order by c.cost_element_id ");
                objWF.MyCommand.CommandType = CommandType.Text;
                objWF.MyCommand.Parameters.Clear();
                objWF.MyCommand.CommandText = sb.ToString();
                objWF.MyDataAdapter = new OracleDataAdapter(objWF.MyCommand);
                objWF.MyDataAdapter.Fill(DSCost);
                return DSCost;
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

        ///END By Sushama
        public ArrayList SaveQuotation(DataSet DSQuot, long PkValue, WorkFlow objWF, bool Update, OracleTransaction TRAN, string ShipperPK = "", string QuoteNumber = "", int CargoType = 0, string PrevQuotaion = "", int Cust_Type = 0,
        bool Nomination = false, short BizType = 2)
        {
            string QuoteNo = null;
            int CustCatgoryPK = 0;
            string QuotePKs = null;
            Array QuoteArry = null;
            Int16 i = default(Int16);
            DataSet dsCust = null;
            arrMessage.Clear();
            try
            {
                if (Update == false)
                {
                    QuoteNo = GenerateQuoteNo(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), M_CREATED_BY_FK, objWF, BizType);
                    if (QuoteNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        QuoteNo = "";
                        return arrMessage;
                    }
                    dsCust = GetCustCategoryPK(ShipperPK, Nomination);
                    if (dsCust.Tables[0].Rows.Count > 0)
                    {
                        CustCatgoryPK = Convert.ToInt32(dsCust.Tables[0].Rows[0]["CUSTOMER_CATEGORY_MST_PK"]);
                    }

                    var _with20 = objWF.MyCommand;
                    _with20.CommandType = CommandType.StoredProcedure;
                    _with20.CommandText = objWF.MyUserName + ".BOOKING_MST_PKG.AUTO_CREATE_QUOTATION";
                    _with20.Parameters.Clear();
                    _with20.Parameters.Add("BOOKING_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with20.Parameters.Add("QUOTATION_NO_IN", QuoteNo).Direction = ParameterDirection.Input;
                    _with20.Parameters.Add("CUST_CATEGORY_PK_IN", CustCatgoryPK).Direction = ParameterDirection.Input;
                    _with20.Parameters.Add("CUST_TYPE_IN", Cust_Type).Direction = ParameterDirection.Input;
                    _with20.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with20.ExecuteNonQuery();
                }
                else
                {
                    var _with21 = objWF.MyCommand;
                    _with21.CommandType = CommandType.StoredProcedure;
                    _with21.CommandText = objWF.MyUserName + ".BOOKING_MST_PKG.CREATE_AMEND_QUOTATION";
                    _with21.Parameters.Clear();
                    _with21.Parameters.Add("BOOKING_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with21.Parameters.Add("QUOTATION_NO_IN", QuoteNumber).Direction = ParameterDirection.Input;
                    _with21.Parameters.Add("QUOTAION_SEA_PK_IN", DSQuot.Tables[0].Rows[0]["QUOTATION_SEA_PK"]).Direction = ParameterDirection.Input;
                    _with21.Parameters.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                    _with21.Parameters.Add("PREV_QUOTATION_IN", PrevQuotaion).Direction = ParameterDirection.Input;
                    _with21.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with21.ExecuteNonQuery();
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                if (string.Compare(oraexp.Message, "ORA-20998") == 1)
                {
                    arrMessage.Add("All data saved successfully");
                }
                else
                {
                    throw oraexp;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }

        public object GetQuotationDetails(string PrevQuotaion)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow Objwk = new WorkFlow();
            try
            {
                sb.Append("SELECT QST.QUOTATION_MST_PK QUOTATION_SEA_PK,");
                sb.Append("       UPPER(QST.QUOTATION_REF_NO) AS QUOTATION_REF_NO");
                sb.Append("  FROM QUOTATION_MST_TBL    QST");
                sb.Append(" WHERE  QST.QUOTATION_REF_NO ='" + PrevQuotaion + "' ");
                return Objwk.GetDataSet(sb.ToString());
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

        public new string GenerateQuoteNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null, short BIZ_TYPE = 2)
        {
            if (BIZ_TYPE == 2)
            {
                return GenerateProtocolKey("QUOTATION (SEA)", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, ObjWK);
            }
            else
            {
                return GenerateProtocolKey("QUOTATION (AIR)", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, ObjWK);
            }
        }

        public DataSet GetCustCategoryPK(string Custpk, bool Nomination = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow Objwk = new WorkFlow();
            int res = 0;
            try
            {
                sb.Append(" SELECT COUNT(*) ");
                sb.Append("  FROM CUSTOMER_MST_TBL          CMT,");
                sb.Append("       CUSTOMER_CATEGORY_MST_TBL CCD,");
                sb.Append("       CUSTOMER_CATEGORY_TRN     CCT");
                sb.Append(" WHERE CMT.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK");
                sb.Append("   AND CCD.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK=" + Custpk);
                if (Nomination == false)
                {
                    sb.Append("   AND CCD.CUSTOMER_CATEGORY_ID = 'SHIPPER'");
                }
                else
                {
                    sb.Append("    AND CCD.CUSTOMER_CATEGORY_ID = 'CONSIGNEE'");
                }
                res = Convert.ToInt32(Objwk.ExecuteScaler(sb.ToString()));
                if (res > 0)
                {
                    sb.Remove(0, sb.Length);
                    sb.Append(" SELECT CCD.CUSTOMER_CATEGORY_MST_PK");
                    sb.Append("  FROM CUSTOMER_MST_TBL          CMT,");
                    sb.Append("       CUSTOMER_CATEGORY_MST_TBL CCD,");
                    sb.Append("       CUSTOMER_CATEGORY_TRN     CCT");
                    sb.Append(" WHERE CMT.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK");
                    sb.Append("   AND CCD.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK");
                    sb.Append("   AND CMT.CUSTOMER_MST_PK=" + Custpk);
                    if (Nomination == false)
                    {
                        sb.Append("   AND CCD.CUSTOMER_CATEGORY_ID = 'SHIPPER'");
                    }
                    else
                    {
                        sb.Append("    AND CCD.CUSTOMER_CATEGORY_ID = 'CONSIGNEE'");
                    }
                }
                else
                {
                    sb.Remove(0, sb.Length);
                    sb.Append("SELECT CCD.CUSTOMER_CATEGORY_MST_PK");
                    sb.Append("  FROM TEMP_CUSTOMER_TBL TEMP, CUSTOMER_CATEGORY_MST_TBL CCD");
                    sb.Append(" WHERE TEMP.CUSTOMER_TYPE_FK = CCD.CUSTOMER_CATEGORY_MST_PK");
                    sb.Append("   AND TEMP.CUSTOMER_MST_PK =" + Custpk);
                    if (Nomination == false)
                    {
                        sb.Append("   AND CCD.CUSTOMER_CATEGORY_ID = 'SHIPPER'");
                    }
                    else
                    {
                        sb.Append("    AND CCD.CUSTOMER_CATEGORY_ID = 'CONSIGNEE'");
                    }
                }
                return Objwk.GetDataSet(sb.ToString());
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

        public Int32 CheckJobcard(Int32 BookingPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
            int RcdCnt = 0;
            try
            {
                sb.Append("SELECT COUNT(*)");
                sb.Append("      FROM JOB_CARD_TRN JC ");
                sb.Append("     WHERE JC.BOOKING_MST_FK = " + BookingPK);
                return Convert.ToInt32(Objwk.ExecuteScaler(sb.ToString()));
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

        public Int32 GetJobcardPK(Int32 BookingPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
            int RcdCnt = 0;
            try
            {
                sb.Append("SELECT JC.job_card_trn_pk");
                sb.Append("      FROM JOB_CARD_TRN JC ");
                sb.Append("     WHERE JC.BOOKING_MST_FK = " + BookingPK);
                return Convert.ToInt32(Objwk.ExecuteScaler(sb.ToString()));
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

        public ArrayList SaveBookingOFreight(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string strValueArgument = null;
            arrMessage.Clear();
            try
            {
                if (!IsUpdate)
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with22 = SelectCommand;
                        _with22.CommandType = CommandType.StoredProcedure;
                        _with22.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_TRN_OTH_CHRG_INS";
                        SelectCommand.Parameters.Clear();

                        //If sea u have to update BOOKING_MST_FK_IN. If AIR u have to update BOOKING_TRN_FK_IN
                        _with22.Parameters.Add("BOOKING_MST_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;
                        //.Parameters.Add("BOOKING_TRN_FK_IN", CLng(PkValue)).Direction = ParameterDirection.Input
                        _with22.Parameters.Add("BOOKING_TRN_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["BOOKING_TRN_FK"]).Direction = ParameterDirection.Input;

                        _with22.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with22.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with22.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with22.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with22.Parameters.Add("AMOUNT_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with22.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                        _with22.Parameters.Add("FREIGHT_TYPE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                        _with22.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                        //Return value of the proc.
                        _with22.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
                        _with22.ExecuteNonQuery();
                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with23 = SelectCommand;
                        _with23.CommandType = CommandType.StoredProcedure;
                        _with23.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_TRN_OTH_CHRG_UPD";
                        SelectCommand.Parameters.Clear();

                        //If sea u have to update BOOKING_MST_FK_IN. If AIR u have to update BOOKING_TRN_FK_IN
                        _with23.Parameters.Add("BOOKING_MST_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;
                        //.Parameters.Add("BOOKING_TRN_FK_IN", CLng(PkValue)).Direction = ParameterDirection.Input
                        _with23.Parameters.Add("BOOKING_TRN_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["BOOKING_TRN_FK"]).Direction = ParameterDirection.Input;

                        _with23.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with23.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with23.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with23.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with23.Parameters.Add("AMOUNT_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with23.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                        _with23.Parameters.Add("FREIGHT_TYPE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                        _with23.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                        //Return value of the proc.
                        _with23.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
                        _with23.ExecuteNonQuery();
                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
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

        //Save and update the Flat rates

        public ArrayList SaveBookingCDimension(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string Measure, string Wt, string Divfac)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            try
            {
                foreach (DataRow _row in dsMain.Tables["tblCDimension"].Select("DELETE_FLAG<>'1'"))
                {
                    var _with24 = SelectCommand;
                    _with24.CommandType = CommandType.StoredProcedure;
                    SelectCommand.Parameters.Clear();
                    if (!IsUpdate)
                    {
                        _with24.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_CARGO_CALC_INS";
                    }
                    else
                    {
                        _with24.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_CARGO_CALC_UPD";
                        _with24.Parameters.Add("BOOKING_CARGO_CALC_PK_IN", _row["BOOKING_CARGO_CALC_PK"]).Direction = ParameterDirection.Input;
                    }

                    //If SEA u have to update booking_mst_fk. If AIR u have to update booking_trn_fk
                    _with24.Parameters.Add("BOOKING_MST_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("BOOKING_TRN_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_NOP_IN", _row["CARGO_NOP"]).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_LENGTH_IN", _row["CARGO_LENGTH"]).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_WIDTH_IN", _row["CARGO_WIDTH"]).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_HEIGHT_IN", _row["CARGO_HEIGHT"]).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_CUBE_IN", _row["CARGO_CUBE"]).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_VOLUME_WT_IN", _row["CARGO_VOLUME_WT"]).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_ACTUAL_WT_IN", _row["CARGO_ACTUAL_WT"]).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_DENSITY_IN", _row["CARGO_DENSITY"]).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_MEASURE_IN", Measure).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_WT_IN", Wt).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("CARGO_DIVFAC_IN", Divfac).Direction = ParameterDirection.Input;

                    _with24.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with24.ExecuteNonQuery();
                }
                if (IsUpdate)
                {
                    //For Delete
                    foreach (DataRow _row in dsMain.Tables["tblCDimension"].Select("DELETE_FLAG='1'"))
                    {
                        var _with25 = SelectCommand;
                        _with25.CommandType = CommandType.StoredProcedure;
                        _with25.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_CARGO_CALC_DEL";
                        _with25.Parameters.Clear();

                        _with25.Parameters.Add("BOOKING_CARGO_CALC_PK_IN", _row["BOOKING_CARGO_CALC_PK"]).Direction = ParameterDirection.Input;

                        _with25.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with25.ExecuteNonQuery();
                    }
                }

                arrMessage.Add("All data saved successfully");
                return arrMessage;
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

        public ArrayList SaveBookingFRT(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string strContractRefNo, string strValueArgument, string isLcl)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            DataView dv_Freight = new DataView();
            Int16 Check = default(Int16);
            if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["BUSINESS_TYPE"]) == 1)
            {
                dv_Freight = getDataViewAir(dsMain.Tables["tblFreight"], strContractRefNo, strValueArgument);
            }
            else
            {
                dv_Freight = getDataView(dsMain.Tables["tblFreight"], strContractRefNo, strValueArgument, isLcl);
            }

            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                Check = FetchEBFrt(PkValue);
                if (Chk_EBK == 1 & Check == 0)
                {
                    IsUpdate = false;
                }
            }
            arrMessage.Clear();
            try
            {
                var _with28 = SelectCommand;
                _with28.CommandType = CommandType.StoredProcedure;

                for (nRowCnt = 0; nRowCnt <= dv_Freight.Table.Rows.Count - 1; nRowCnt++)
                {
                    DataRow _frtRow = dv_Freight.Table.Rows[nRowCnt];
                    _with28.Parameters.Clear();
                    if (!IsUpdate)
                    {
                        _with28.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_TRN_FRT_DTLS_INS";
                    }
                    else
                    {
                        _with28.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_TRN_FRT_DTLS_UPD";
                        _with28.Parameters.Add("BOOKING_TRN_FRT_PK_IN", _frtRow["BOOKING_TRN_FRT_PK"]).Direction = ParameterDirection.Input;
                    }
                    _with28.Parameters.Add("BOOKING_TRN_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", _frtRow["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("CURRENCY_MST_FK_IN", _frtRow["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("CHARGE_BASIS_IN", _frtRow["CHARGE_BASIS"]).Direction = ParameterDirection.Input;

                    if (string.IsNullOrEmpty(_frtRow["SURCHARGE"].ToString()))
                    {
                        _with28.Parameters.Add("SURCHARGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("SURCHARGE_IN", _frtRow["SURCHARGE"]).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(_frtRow["MIN_RATE"].ToString()))
                    {
                        _with28.Parameters.Add("MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("MIN_RATE_IN", Convert.ToDouble(_frtRow["MIN_RATE"])).Direction = ParameterDirection.Input;
                    }

                    if (string.IsNullOrEmpty(_frtRow["TARIFF_RATE"].ToString()))
                    {
                        _with28.Parameters.Add("TARIFF_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with28.Parameters.Add("TARIFF_RATE_IN", Convert.ToDouble(_frtRow["TARIFF_RATE"])).Direction = ParameterDirection.Input;
                    }

                    _with28.Parameters.Add("PYMT_TYPE_IN", _frtRow["PYMT_TYPE"]).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", _frtRow["CHECK_FOR_ALL_IN_RT"]).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("CHECK_ADVATOS_IN", _frtRow["CHECK_ADVATOS"]).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("BASISPK_IN", _frtRow["BASISPK"]).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("EXCHANGE_RATE_IN", _frtRow["EXCHANGE_RATE"]).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with28.ExecuteNonQuery();
                }
                arrMessage.Add("All data saved successfully");
                return arrMessage;
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

        public Int16 FetchEBFrt(long BkgPk)
        {
            string sql = "";
            string res = "";
            Int16 check = 0;
            WorkFlow objWK = new WorkFlow();
            sql = "select BOOKING_TRN_FK from BOOKING_TRN_FRT_DTLS where BOOKING_TRN_FK='" + BkgPk + "'";
            res = objWK.ExecuteScaler(sql);
            if (Convert.ToInt32(res) > 0)
            {
                check = 1;
            }
            else
            {
                check = 0;
            }
            return check;
        }

        public object UpdateUpStream(DataSet dsMain, OracleCommand SelectCommand, bool IsUpdate, string strTranType, string strContractRefNo, long PkValue)
        {
            WorkFlow objWK = new WorkFlow();
            string strValueArgument = null;
            arrMessage.Clear();
            try
            {
                var _with29 = SelectCommand;
                _with29.CommandType = CommandType.StoredProcedure;
                _with29.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BOOKING_UPDATE_UPSTREAM";
                SelectCommand.Parameters.Clear();

                _with29.Parameters.Add("TRANS_REFERED_FROM_IN", Convert.ToInt64(strTranType)).Direction = ParameterDirection.Input;
                _with29.Parameters.Add("TRANS_REF_NO_IN", Convert.ToString(strContractRefNo)).Direction = ParameterDirection.Input;

                _with29.Parameters.Add("ISUPDATE", IsUpdate.ToString()).Direction = ParameterDirection.Input;

                _with29.Parameters.Add("BOOKING_STATUS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;

                //Return value of the proc.
                _with29.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //Execute the command
                _with29.ExecuteNonQuery();
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        private DataView getDataView(DataTable dtFreight, string strContractRefNo, string strValueArgument, string isLcl)
        {
            try
            {
                DataTable dstemp = new DataTable();
                DataRow dr = null;
                Int32 nRowCnt = default(Int32);
                Int32 nColCnt = default(Int32);
                ArrayList arrValueCondition = new ArrayList();
                string strValueCondition = "";
                dstemp = dtFreight.Clone();
                if (isLcl == "1")
                {
                    for (nRowCnt = 0; nRowCnt <= dtFreight.Rows.Count - 1; nRowCnt++)
                    {
                        if (strContractRefNo == getDefault(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"], "") & strValueArgument == getDefault(dtFreight.Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"], ""))
                        {
                            dr = dstemp.NewRow();
                            for (nColCnt = 0; nColCnt <= dtFreight.Columns.Count - 1; nColCnt++)
                            {
                                dr[nColCnt] = dtFreight.Rows[nRowCnt][nColCnt];
                            }
                            dstemp.Rows.Add(dr);
                        }
                    }
                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dtFreight.Rows.Count - 1; nRowCnt++)
                    {
                        if (strContractRefNo == getDefault(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"], "") & strValueArgument == getDefault(dtFreight.Rows[nRowCnt]["BASISPK"], ""))
                        {
                            dr = dstemp.NewRow();
                            for (nColCnt = 0; nColCnt <= dtFreight.Columns.Count - 1; nColCnt++)
                            {
                                dr[nColCnt] = dtFreight.Rows[nRowCnt][nColCnt];
                            }
                            dstemp.Rows.Add(dr);
                        }
                    }
                }
                return dstemp.DefaultView;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private DataView getDataViewAir(DataTable dtFreight, string strContractRefNo, string strValueArgument)
        {
            try
            {
                DataTable dstemp = new DataTable();
                DataRow dr = null;
                Int32 nRowCnt = default(Int32);
                Int32 nColCnt = default(Int32);
                dstemp = dtFreight.Clone();
                for (nRowCnt = 0; nRowCnt <= dtFreight.Rows.Count - 1; nRowCnt++)
                {
                    if (strContractRefNo == (string.IsNullOrEmpty(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"].ToString()) ? 0 : dtFreight.Rows[nRowCnt]["TRANS_REF_NO"]))
                    {
                        dr = dstemp.NewRow();
                        for (nColCnt = 0; nColCnt <= dtFreight.Columns.Count - 1; nColCnt++)
                        {
                            dr[nColCnt] = dtFreight.Rows[nRowCnt][nColCnt];
                        }
                        dstemp.Rows.Add(dr);
                    }
                }
                return dstemp.DefaultView;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public bool CheckActiveJobCard(int strABEPk)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            short intCnt = 0;
            WorkFlow objWF = new WorkFlow();
            string strReturn = null;
            //strBuilder.Append("SELECT JCSET.JOB_CARD_TRN_PK FROM JOB_CARD_TRN JCSET ")
            //strBuilder.Append("WHERE JCSET.JOB_CARD_STATUS=1 ")
            //strBuilder.Append("AND JCSET.BOOKING_MST_FK= " & strABEPk)
            strBuilder.Append(" UPDATE JOB_CARD_TRN J ");
            strBuilder.Append(" SET J.JOB_CARD_STATUS = 2, J.JOB_CARD_CLOSED_ON = SYSDATE ");
            strBuilder.Append(" WHERE J.BOOKING_MST_FK = " + strABEPk);

            try
            {
                intCnt = Convert.ToInt16(objWF.ExecuteScaler(strBuilder.ToString()));
                if (intCnt == 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
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

        //Check for active Job Card

        //This function generates the Booking Referrence no. as per the protocol saved by the user.
        public string GenerateBookingNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK, short BusinessType, string POLID = "", string PODID = "")
        {
            string functionReturnValue = null;
            if (BusinessType == 2)
            {
                functionReturnValue = GenerateProtocolKey("BOOKING (SEA)", nLocationId, nEmployeeId, DateTime.Now, "", "", POLID, nCreatedBy, objWK, "",
                PODID);
            }
            else
            {
                functionReturnValue = GenerateProtocolKey("BOOKING (AIR)", nLocationId, nEmployeeId, DateTime.Now, "", "", POLID, nCreatedBy, objWK, "",
                PODID);
            }
            return functionReturnValue;
        }

        //This function generates the Nomination Reference no. as per the protocol saved by the user.
        public string GenerateNominationNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK, short BusinessType, string POLID = "", string PODID = "")
        {
            if (BusinessType == 2)
            {
                return GenerateProtocolKey("NOMINATION SEA", nLocationId, nEmployeeId, DateTime.Now, "", "", POLID, nCreatedBy, objWK, "",
                PODID);
            }
            else
            {
                return GenerateProtocolKey("NOMINATION AIR", nLocationId, nEmployeeId, DateTime.Now, "", "", POLID, nCreatedBy, objWK, "",
                PODID);
            }
        }

        #endregion "Save"

        #region "Delete Booking transactions"

        public string DeleteBkgTrans(string PkValues)
        {
            WorkFlow objWF = new WorkFlow();
            OracleTransaction TRAN = null;
            objWF.OpenConnection();
            TRAN = objWF.MyConnection.BeginTransaction();
            objWF.MyCommand.Transaction = TRAN;
            try
            {
                var _with30 = objWF.MyCommand;
                _with30.CommandType = CommandType.StoredProcedure;
                _with30.CommandText = objWF.MyUserName + ".BOOKING_MST_PKG.BOOKING_TRN_DELETE";
                _with30.Parameters.Clear();

                _with30.Parameters.Add("BOOKING_TRN_PK_IN", PkValues).Direction = ParameterDirection.Input;
                _with30.Parameters["BOOKING_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with30.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with30.ExecuteNonQuery();
                strRet = (string.IsNullOrEmpty(_with30.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with30.Parameters["RETURN_VALUE"].Value.ToString());
                TRAN.Commit();
                return strRet;
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyCommand.Connection.Close();
            }
        }

        #endregion "Delete Booking transactions"

        #region "Check Customer Credit Status"         'Returns True if credit balance exist

        public bool funCheckCustCredit(DataSet dsMain, long lngCustomerPk, long CreditLimit, short BizType = 2)
        {
            Int32 nRowCnt = default(Int32);
            double dblBookingAmt = 0;
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            DataTable dt = new DataTable();
            WorkFlow objWF = new WorkFlow();
            string strReturn = null;
            string Temp = "0";
            string sql = null;

            try
            {
                sql = "";
                sql = "select customer_mst_pk from customer_mst_tbl where customer_mst_pk=' " + lngCustomerPk + " '";
                Temp = objWF.ExecuteScaler(sql);

                if ((Temp != null) | Convert.ToInt16(Temp) > 0)
                {
                    strBuilder.Append("SELECT CMT.CREDIT_LIMIT, ");
                    strBuilder.Append("(CMT.CREDIT_LIMIT - CMT.CREDIT_LIMIT_USED) AS CREDIT_BALANCE ");
                    strBuilder.Append("FROM CUSTOMER_MST_TBL CMT ");
                    strBuilder.Append("WHERE ");
                    strBuilder.Append("CMT.CUSTOMER_MST_PK= " + lngCustomerPk);

                    dt = objWF.GetDataTable(strBuilder.ToString());
                }
                else
                {
                    return true;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            try
            {
                if (string.IsNullOrEmpty(dt.Rows[0][0].ToString()))
                {
                    return true;
                }
                else if (dt.Rows[0][0] == null)
                {
                    return true;
                }
                else if (dt.Rows[0][0] == "0")
                {
                    return true;
                }

                if (BizType == 2)
                {
                    if (Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == "1")
                    {
                        for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                        {
                            dblBookingAmt = dblBookingAmt + (Convert.ToInt32(getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"], 0)) * Convert.ToInt32(getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["NO_OF_BOXES"], 0)));
                        }
                        for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                        {
                            if (!string.IsNullOrEmpty(dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"].ToString()) & (dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"] != null))
                            {
                                dblBookingAmt = dblBookingAmt + Convert.ToInt32(getDefault(dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"], 0));
                            }
                        }
                    }
                    else
                    {
                        for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                        {
                            dblBookingAmt = dblBookingAmt + (Convert.ToInt32(getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"], 0)) * Convert.ToInt32(getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"], 0)));
                        }
                        for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                        {
                            if (!string.IsNullOrEmpty(dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"].ToString()) & (dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"] != null))
                            {
                                dblBookingAmt = dblBookingAmt + Convert.ToInt32(getDefault(dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"], 0));
                            }
                        }
                    }
                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        dblBookingAmt = dblBookingAmt + Convert.ToInt32((string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"].ToString()) ? 0 : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"]));
                    }

                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                    {
                        if (!string.IsNullOrEmpty(dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"].ToString()) & (dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"] != null))
                        {
                            dblBookingAmt = dblBookingAmt + Convert.ToInt32(dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]);
                        }
                    }
                }

                //CreditLimit = (dt.Rows(0).Item(1) - dblBookingAmt)
                if (!((Convert.ToDouble(dt.Rows[0][0]) - dblBookingAmt) >= 0))
                {
                    return false;
                }

                if (string.IsNullOrEmpty((dt.Rows[0][1].ToString())))
                {
                    return true;
                }
                else if ((dt.Rows[0][1]) == null)
                {
                    return true;
                }
                else if ((dt.Rows[0][1]) == "0")
                {
                    return true;
                }
                else
                {
                    if (((Convert.ToDouble(dt.Rows[0][1]) - dblBookingAmt) >= 0))
                    {
                        CreditLimit = Convert.ToInt32(dblBookingAmt);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Check Customer Credit Status"         'Returns True if credit balance exist

        #region "Retrive Address for Quotation for Sea"

        public object funRAddressSea(string strQRefNo)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            DataTable dt = null;
            //SELECT ADDRESS FROM THE QUOTATION TABLE
            strBuilder.Append("SELECT ");
            strBuilder.Append("QST.COL_PLACE_MST_FK, PMTC.PLACE_CODE, QST.COL_ADDRESS, ");
            strBuilder.Append("QST.DEL_PLACE_MST_FK, PMTD.PLACE_CODE, QST.DEL_ADDRESS ");
            strBuilder.Append("FROM QUOTATION_MST_TBL QST, PLACE_MST_TBL PMTC,PLACE_MST_TBL PMTD ");
            strBuilder.Append("WHERE ");
            strBuilder.Append("QST.COL_PLACE_MST_FK = PMTC.PLACE_PK ");
            strBuilder.Append("AND QST.DEL_PLACE_MST_FK=PMTD.PLACE_PK ");
            strBuilder.Append("AND QST.QUOTATION_REF_NO='" + strQRefNo + "'");
            try
            {
                dt = objwf.GetDataTable(strBuilder.ToString());
                return dt;
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

        #endregion "Retrive Address for Quotation for Sea"

        #region "Check for JobCard existence"

        public string FunJobExist(int strSBEPk)
        {
            try
            {
                bool boolFound = false;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                string strReturn = null;
                strBuilder.Append("SELECT JCSET.JOB_CARD_TRN_PK FROM JOB_CARD_TRN JCSET ");
                strBuilder.Append("WHERE JCSET.BOOKING_MST_FK=" + strSBEPk);
                strReturn = objWF.ExecuteScaler(strBuilder.ToString());
                return strReturn;
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

        #endregion "Check for JobCard existence"

        #region "Check for Restriction"

        public object CheckRestriction(string strCond)
        {
            WorkFlow objWFT = new WorkFlow();
            DataSet Ds = new DataSet();
            string strPolPk = "";
            string strPodPk = "";
            string strSDate = "";
            string strBType = "";
            string strCommodityFk = "";
            string strReturn = "";
            string strRestriction = "";
            string strHazFK = "";
            string strRType = "";
            Int16 i = default(Int16);
            Int16 intCount = 0;
            Int32 intVStatus = 0;
            short intFC = 0;
            short intTC = 0;
            short intFTC = 0;
            short intPL = 0;
            short intPD = 0;
            short intPLPD = 0;
            Array arrCond = null;
            arrCond = strCond.Split('~');
            strPolPk = Convert.ToString(arrCond.GetValue(1));
            strPodPk = Convert.ToString(arrCond.GetValue(2));
            strSDate = Convert.ToString(arrCond.GetValue(3));
            strBType = Convert.ToString(arrCond.GetValue(4));
            //strHazFK 0 for non hazardous and 1 for hazardous
            //strRType 1 for Commodity wise restriction and 2 for General restriction

            try
            {
                Int64 k = default(Int64);
                strHazFK = " 1";
                strRType = "1";
                string strRString = "";
                for (i = 5; i <= arrCond.Length - 1; i++)
                {
                    if (!string.IsNullOrEmpty(Convert.ToString(arrCond.GetValue(i))))
                    {
                        strCommodityFk = Convert.ToString(arrCond.GetValue(i));
                    }
                    else
                    {
                        strCommodityFk = "0";
                    }
                    FunCheckRestriction(Ds, strPolPk, strPodPk, strCommodityFk, strSDate, strBType, strHazFK, strRType);
                    if (Ds.Tables[0].Rows.Count > 0)
                    {
                        strRestriction += MakeRString(Ds, intVStatus, intFC, intTC, intFTC, intPL, intPD, intPLPD, strPolPk, strPodPk,
                        strCommodityFk);
                        Ds.Tables[0].Clear();
                    }
                }
                return strRestriction;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
            }
        }

        public void FunCheckRestriction(DataSet ds, string strPolPk, string strPodPk, string strcommodityfk, string strsdate, string strBType, string strHazFK, string strRType)
        {
            try
            {
                OracleDataAdapter Da = new OracleDataAdapter();
                WorkFlow objWFT = new WorkFlow();

                if ((strcommodityfk == null))
                {
                    strcommodityfk = "0";
                }
                if (string.IsNullOrEmpty(strcommodityfk))
                {
                    strcommodityfk = "0";
                }
                if (strcommodityfk == "null")
                {
                    strcommodityfk = "0";
                }

                objWFT.MyCommand.CommandType = CommandType.StoredProcedure;
                objWFT.MyCommand.CommandText = objWFT.MyUserName + ".BOOKING_RESTRICTION.CHECK_RESTRICTION_ALL";
                var _with31 = objWFT.MyCommand.Parameters;
                _with31.Add("POL_PK_IN", strPolPk).Direction = ParameterDirection.Input;
                _with31.Add("POD_PK_IN", strPodPk).Direction = ParameterDirection.Input;
                //Changed By Snigdharani
                _with31.Add("COMMODITY_MST_FK_IN", getDefault(strcommodityfk, 0)).Direction = ParameterDirection.Input;
                _with31.Add("S_DATE_IN", strsdate).Direction = ParameterDirection.Input;
                _with31.Add("BUSINESS_TYPE_IN", strBType).Direction = ParameterDirection.Input;
                _with31.Add("HAZARDOUS_IN", strHazFK).Direction = ParameterDirection.Input;
                _with31.Add("RESTRICTION_TYPE_IN", strRType).Direction = ParameterDirection.Input;
                _with31.Add("RES_CURSOR_ALL", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (objWFT.ExecuteCommands() == true)
                {
                    Da.SelectCommand = objWFT.MyCommand;
                    Da.Fill(ds);
                }
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

        public string MakeRString(DataSet ds, Int32 intVStatus, short intFC, short intTC, short intFTC, short intPL, short intPD, short intPLPD, string strPolPK = "", string strPodPK = "",
        string strCommodityPK = "")
        {
            try
            {
                string strRString = "";
                Int32 intRowCnt = default(Int32);
                Int32 intColCnt = default(Int32);
                Int32 intSCount = 0;
                short intCStatus = 0;
                bool M = false;
                string strRest = null;
                string strRestriction = null;
                for (intRowCnt = 0; intRowCnt <= ds.Tables[0].Rows.Count - 1; intRowCnt++)
                {
                    for (intColCnt = 3; intColCnt <= ds.Tables[0].Columns.Count - 1; intColCnt++)
                    {
                        if (Convert.ToInt32(ds.Tables[0].Rows[intRowCnt][intColCnt]) == 1)
                        {
                            strRest = ds.Tables[0].Columns[intColCnt].ColumnName;
                            switch (strRest)
                            {
                                case "COMMODITY":
                                    if (intCStatus == 0)
                                    {
                                        if (intVStatus == 0)
                                        {
                                            strRestriction += FindCommodityID(ds.Tables[0].Rows[intRowCnt]["COMMODITY_MST_FK"].ToString());
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindCommodityID(ds.Tables[0].Rows[intRowCnt]["COMMODITY_MST_FK"].ToString());
                                        }
                                        intCStatus = 1;
                                    }
                                    break;

                                case "FROM_COUNTRY":
                                    if (intFC == 0)
                                    {
                                        if (intVStatus == 0)
                                        {
                                            strRestriction += FindCID(strPolPK);
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindCID(strPolPK);
                                        }
                                        intFC = 1;
                                    }
                                    break;

                                case "T0_COUNTRY":
                                    if (intTC == 0)
                                    {
                                        if (intVStatus == 0)
                                        {
                                            strRestriction += FindCID(strPodPK);
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindCID(strPodPK);
                                        }
                                        intTC = 1;
                                    }
                                    break;

                                case "FROM_TO_COUNTRY":
                                    if (intFTC == 0)
                                    {
                                        if (intVStatus == 0)
                                        {
                                            strRestriction += FindCID(strPolPK);
                                            strRestriction += "," + FindCID(strPodPK);
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindCID(strPolPK);
                                            strRestriction += "," + FindCID(strPodPK);
                                        }
                                        intFTC = 1;
                                    }

                                    break;

                                case "ONLY_POL":
                                    if (intPL == 0)
                                    {
                                        if (intVStatus == 0)
                                        {
                                            strRestriction += FindPID(strPolPK);
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindPID(strPolPK);
                                        }
                                        intPL = 1;
                                    }
                                    break;

                                case "ONLY_POD":
                                    if (intPD == 0)
                                    {
                                        if (intVStatus == 0)
                                        {
                                            strRestriction += FindPID(strPodPK);
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindPID(strPodPK);
                                        }
                                        intPD = 1;
                                    }
                                    break;

                                case "POL_POD":
                                    if (intPLPD == 0)
                                    {
                                        if (intVStatus == 0)
                                        {
                                            strRestriction += FindPID(strPolPK);
                                            strRestriction += "," + FindPID(strPodPK);
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindPID(strPolPK);
                                            strRestriction += "," + FindPID(strPodPK);
                                        }
                                        intPLPD = 1;
                                    }
                                    break;
                            }
                        }
                    }
                    if (!string.IsNullOrEmpty(strRestriction))
                    {
                        strRestriction += "$" + ds.Tables[0].Rows[intRowCnt]["RESTRICTION_MESSAGE"];
                    }
                    if (Convert.ToInt32(ds.Tables[0].Rows[intRowCnt]["RESTRICTION_ALERTBLOCK"]) == 2)
                    {
                        M = true;
                    }
                }

                strRestriction = strRestriction + "@" + M;
                return strRestriction;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string FindCommodityID(string strCommodityFK = "")
        {
            // To Retrive Commodity ID
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strBuilder.Append("select COMMODITY_NAME from COMMODITY_MST_TBL where COMMODITY_MST_PK = " + strCommodityFK + " ");
                string ComName = null;
                ComName = objWF.ExecuteScaler(strBuilder.ToString());
                return ComName;
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

        public string FindPID(string strPFK = "")
        {
            // To Retrive the Port ID
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strBuilder.Append("select PORT_ID from PORT_MST_TBL where PORT_MST_PK= " + strPFK + " ");
                string strPName = null;
                strPName = objWF.ExecuteScaler(strBuilder.ToString());
                return strPName;
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

        public string FindCID(string strPFK = "")
        {
            // To Retrive the Country ID
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strBuilder.Append("select COUNTRY_ID from COUNTRY_MST_TBL where COUNTRY_MST_PK= (SELECT COUNTRY_MST_FK ");
                strBuilder.Append("FROM PORT_MST_TBL WHERE PORT_MST_PK=" + strPFK + ")");
                string strCName = null;
                strCName = objWF.ExecuteScaler(strBuilder.ToString());
                return strCName;
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

        #endregion "Check for Restriction"

        #region "Enhance Search Functions "

        public string FetchBookingAddressSea(string strCond)
        {
            string strSql = "";
            WorkFlow objwf = new WorkFlow();
            DataTable dt = null;
            string strAddress = "";
            Int32 j = default(Int32);
            //SELECT ADDRESS FROM THE QUOTATION TABLE
            strSql = "SELECT " + "QST.COL_PLACE_MST_FK, PMTC.PLACE_CODE, QST.COL_ADDRESS, " + "QST.DEL_PLACE_MST_FK, PMTD.PLACE_CODE, QST.DEL_ADDRESS " + "FROM QUOTATION_SEA_TBL QST, PLACE_MST_TBL PMTC,PLACE_MST_TBL PMTD " + "WHERE " + "QST.COL_PLACE_MST_FK = PMTC.PLACE_PK " + "AND QST.DEL_PLACE_MST_FK=PMTD.PLACE_PK " + "AND QST.QUOTATION_REF_NO='" + strCond + "'";
            try
            {
                dt = objwf.GetDataTable(strSql);
                if (dt.Rows.Count > 0)
                {
                    strAddress += dt.Rows[0][0];
                    for (j = 1; j <= 5; j++)
                    {
                        strAddress += "~" + dt.Rows[0][j];
                    }
                }
                else
                {
                    strAddress = "~~~~~";
                }
                return strAddress;
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

        public string FetchForPlace(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string Port = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            //If arr.Length > 2 Then strLOC_MST_IN = arr(2)
            //BusinessType AIR/SEA depends on the USER
            if (arr.Length > 2)
                strBusinessType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                Port = Convert.ToString(arr.GetValue(3));
            else
                Port = "0";

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_PLACE_COMMON";
                var _with32 = SCM.Parameters;
                _with32.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with32.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with32.Add("PORT", ifDBNull(Port)).Direction = ParameterDirection.Input;
                _with32.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with32.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public string FetchForPlaceJC(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string strReq = null;
            string PODPK = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));

            //If arr.Length > 2 Then strLOC_MST_IN = arr(2)
            //BusinessType AIR/SEA depends on the USER
            if (arr.Length > 2)
                strBusinessType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                PODPK = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_PLACE_JC";
                var _with33 = SCM.Parameters;
                _with33.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with33.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input
                _with33.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with33.Add("PODPK_IN", (string.IsNullOrEmpty(PODPK) ? "" : PODPK)).Direction = ParameterDirection.Input;
                _with33.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public string FetchForPackType(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            //If arr.Length > 2 Then strLOC_MST_IN = arr(2)
            //BusinessType AIR/SEA depends on the USER
            if (arr.Length > 2)
                strBusinessType = Convert.ToString(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_PACKTYPE_COMMON";
                var _with34 = SCM.Parameters;
                _with34.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with34.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input
                _with34.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with34.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public string FetchForQuotationNo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string strCargoType = "";
            string strPOL = "";
            string strPod = "";
            string strShipper = "";
            string strReq = null;
            string strConrainer = "";
            int NoOfContainers = 0;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            //If arr.Length > 2 Then strLOC_MST_IN = arr(2)
            //BusinessType AIR/SEA depends on the USER
            if (arr.Length > 2)
                strBusinessType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strPOL = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strPod = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strShipper = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strCargoType = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(7));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".BOOKING_MST_PKG.GET_QUOTATION_NO";
                var _with35 = SCM.Parameters;
                _with35.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with35.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with35.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with35.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with35.Add("POL_IN", getDefault(strPOL, DBNull.Value)).Direction = ParameterDirection.Input;
                _with35.Add("POD_IN", getDefault(strPod, DBNull.Value)).Direction = ParameterDirection.Input;
                _with35.Add("CUSTPK_IN", getDefault(strShipper, DBNull.Value)).Direction = ParameterDirection.Input;
                //Manoharan 11June2007: to get Logged in Based Quotations
                _with35.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with35.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public string FetchForNewQuotationNo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string strCargoType = "";
            string strPOL = "";
            string strPod = "";
            string strShipper = "";
            string strReq = null;
            string strConrainer = "";
            string strCommodity = "";
            dynamic intQuot = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            //If arr.Length > 2 Then strLOC_MST_IN = arr(2)
            //BusinessType AIR/SEA depends on the USER
            if (arr.Length > 2)
                strBusinessType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strPOL = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strPod = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strShipper = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strCargoType = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                strConrainer = Convert.ToString(arr.GetValue(8));
            if (arr.Length > 9)
                strCommodity = Convert.ToString(arr.GetValue(9));
            if (arr.Length > 10)
                intQuot = Convert.ToString(arr.GetValue(10));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_NEW_QUOTATION_NO";
                var _with36 = SCM.Parameters;
                _with36.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with36.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with36.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with36.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with36.Add("POL_IN", getDefault(strPOL, DBNull.Value)).Direction = ParameterDirection.Input;
                _with36.Add("POD_IN", getDefault(strPod, DBNull.Value)).Direction = ParameterDirection.Input;
                _with36.Add("CUSTPK_IN", getDefault(strShipper, DBNull.Value)).Direction = ParameterDirection.Input;
                //Manoharan 11June2007: to get Logged in Based Quotations
                _with36.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with36.Add("CONTAINER_TYPE_FK_IN", getDefault(strConrainer, DBNull.Value)).Direction = ParameterDirection.Input;
                _with36.Add("COMMODITY_GROUP_IN", getDefault(strCommodity, DBNull.Value)).Direction = ParameterDirection.Input;
                _with36.Add("QUOT_TYPE", getDefault(intQuot, 0)).Direction = ParameterDirection.Input;
                _with36.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public DataSet FetchCustomerfrmQTN(int QtnPK)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            StrSql = "SELECT CMT.CUSTOMER_MST_PK,CMT.CUSTOMER_ID,CMT.CUSTOMER_NAME, QMT.CARGO_MOVE_FK FROM CUSTOMER_MST_TBL CMT,CUSTOMER_CATEGORY_MST_TBL CCMT,";
            StrSql = StrSql + " CUSTOMER_CATEGORY_TRN CCT,QUOTATION_MST_TBL QMT WHERE CMT.CUSTOMER_MST_PK=CCT.CUSTOMER_MST_FK";
            StrSql = StrSql + " AND CCMT.CUSTOMER_CATEGORY_MST_PK=CCT.CUSTOMER_CATEGORY_MST_FK";
            StrSql = StrSql + " AND CCMT.CUSTOMER_CATEGORY_ID='CUSTOMER'";
            StrSql = StrSql + " AND CMT.CUSTOMER_MST_PK=QMT.CUSTOMER_MST_FK";
            StrSql = StrSql + " AND QMT.QUOTATION_MST_PK=" + QtnPK;
            return objWF.GetDataSet(StrSql);
        }

        public string FetchForQuotationNoExpiry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string strCargoType = "";
            string strPOL = "";
            string strPod = "";
            string strShipper = "";
            string strReq = null;
            string FROM_DATE_IN = "";
            string TO_DATE_IN = "";

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            //If arr.Length > 2 Then strLOC_MST_IN = arr(2)
            //BusinessType AIR/SEA depends on the USER
            if (arr.Length > 2)
                strBusinessType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strPOL = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strPod = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strShipper = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strCargoType = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                FROM_DATE_IN = Convert.ToString(arr.GetValue(8));
            if (arr.Length > 9)
                TO_DATE_IN = Convert.ToString(arr.GetValue(9));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_QUOTATION_NO_EXPIRY";
                var _with37 = SCM.Parameters;
                _with37.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with37.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with37.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with37.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with37.Add("POL_IN", getDefault(strPOL, DBNull.Value)).Direction = ParameterDirection.Input;
                _with37.Add("POD_IN", getDefault(strPod, DBNull.Value)).Direction = ParameterDirection.Input;
                _with37.Add("CUSTPK_IN", getDefault(strShipper, DBNull.Value)).Direction = ParameterDirection.Input;
                _with37.Add("FROM_DATE_IN", getDefault(FROM_DATE_IN, DBNull.Value)).Direction = ParameterDirection.Input;
                _with37.Add("TO_DATE_IN", getDefault(TO_DATE_IN, DBNull.Value)).Direction = ParameterDirection.Input;
                //Manoharan 11June2007: to get Logged in Based Quotations
                _with37.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;

                _with37.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public string FetchContainerType(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = "";
            string strSERACH_IN = null;
            string SEARCH_FLAG_IN = "";
            string strBizType = "0";
            string strReq = null;
            string allWrkPort = null;
            string Import = "";
            var strNull = DBNull.Value;
            string pod = "0";
            //Snigdharani
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                allWrkPort = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(7));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_CONTAINER";

                //Comment by prakash No Need Tocheck
                //If strReq = "L" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON"
                //End If
                //If strReq = "F" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON1"
                //End If

                var _with38 = selectCommand.Parameters;
                _with38.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with38.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with38.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input
                _with38.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                _with38.Add("CONTAINER_PK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
                _with38.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        public string FetchBasisType(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = "";
            string strSERACH_IN = null;
            string SEARCH_FLAG_IN = "";
            string strBizType = "0";
            string strReq = null;
            string allWrkPort = null;
            string Import = "";
            var strNull = DBNull.Value;
            string pod = "0";
            //Snigdharani
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                allWrkPort = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(7));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BASIS";

                //Comment by prakash No Need Tocheck
                //If strReq = "L" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON"
                //End If
                //If strReq = "F" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON1"
                //End If

                var _with39 = selectCommand.Parameters;
                _with39.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with39.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with39.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input
                _with39.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                _with39.Add("BASIS_PK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
                _with39.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        public string FetchSlabType(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = "";
            string strSERACH_IN = null;
            string SEARCH_FLAG_IN = "";
            string strBizType = "0";
            string strReq = null;
            string allWrkPort = null;
            string Import = "";
            var strNull = DBNull.Value;
            string pod = "0";
            //Snigdharani
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                allWrkPort = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(7));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_SLAB";

                //Comment by prakash No Need Tocheck
                //If strReq = "L" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON"
                //End If
                //If strReq = "F" Then
                //    selectCommand.CommandText = objWF.MyUserName & ".EN_PORT_PKG.GETPOL_COMMON1"
                //End If

                var _with40 = selectCommand.Parameters;
                _with40.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with40.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with40.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input
                _with40.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                _with40.Add("SLAB_PK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
                _with40.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  14/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        public string FetchSpotRatesSea(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strCustomerPk = "";
            string strPol = "";
            string strPod = "";
            string strCommodityPk = "";
            string strSDate = "";
            string strContBasis = "";
            string strCargoType = "";
            arr = strCond.Split('~');
            strCustomerPk = Convert.ToString(arr.GetValue(1));
            strPol = Convert.ToString(arr.GetValue(2));
            strPod = Convert.ToString(arr.GetValue(3));
            strCommodityPk = Convert.ToString(arr.GetValue(4));
            strSDate = Convert.ToString(arr.GetValue(5));
            strContBasis = Convert.ToString(arr.GetValue(6));
            strCargoType = Convert.ToString(arr.GetValue(7));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_SPOT_RATES_SEA";
                var _with41 = SCM.Parameters;
                _with41.Add("CUSTOMER_PK_IN", ifDBNull(strCustomerPk)).Direction = ParameterDirection.Input;
                _with41.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with41.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with41.Add("COMMODITY_PK_IN", ifDBNull(strCommodityPk)).Direction = ParameterDirection.Input;
                _with41.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with41.Add("CONT_BASIS_IN", ifDBNull(strContBasis)).Direction = ParameterDirection.Input;
                _with41.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with41.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public string FetchVesVoy_Booking(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strOpr = null;
            string strPol = null;
            string strPod = null;
            string fromDate = null;
            string toDate = null;
            string strVES = null;
            string strVOY = null;
            string strReq = null;
            string strImp = null;
            string strLOC = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strOpr = Convert.ToString(arr.GetValue(1));
            strPol = Convert.ToString(arr.GetValue(2));
            strPod = Convert.ToString(arr.GetValue(3));
            fromDate = Convert.ToString(arr.GetValue(4));
            toDate = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strVES = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strVOY = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                strLOC = Convert.ToString(arr.GetValue(8));
            if (arr.Length > 9)
                strImp = Convert.ToString(arr.GetValue(9));
            if (string.IsNullOrEmpty(strImp))
            {
                strImp = null;
            }
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".VESSEL_VOYAGE_BOOKING.VESSEL_VOYAGE_BOOKING";
                var _with42 = SCM.Parameters;
                _with42.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with42.Add("OPR_IN", ifDBNull(strOpr)).Direction = ParameterDirection.Input;
                _with42.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with42.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with42.Add("FROM_DATE_IN", ifDBNull(fromDate)).Direction = ParameterDirection.Input;
                _with42.Add("TO_DATE_IN", ifDBNull(toDate)).Direction = ParameterDirection.Input;
                _with42.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with42.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with42.Add("LOC_IN", ifDBNull(strLOC)).Direction = ParameterDirection.Input;
                _with42.Add("IMPORT_IN", ((strImp == null) ? "" : strImp)).Direction = ParameterDirection.Input;
                _with42.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public string FetchVesVoyBooking(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strOpr = null;
            string strPol = null;
            string strPod = null;
            string strSDate = null;
            string strVES = null;
            string strVOY = null;
            string strReq = null;
            string strImp = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strOpr = Convert.ToString(arr.GetValue(1));
            strPol = Convert.ToString(arr.GetValue(2));
            strPod = Convert.ToString(arr.GetValue(3));
            strSDate = Convert.ToString(arr.GetValue(4));
            // strVES = arr(5)
            //strVOY = arr(6)
            if (arr.Length > 5)
                strVES = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strVOY = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strImp = Convert.ToString(arr.GetValue(7));
            if (string.IsNullOrEmpty(strImp))
            {
                strImp = null;
            }
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.VESSEL_VOYAGE_BOOKING";
                var _with43 = SCM.Parameters;
                _with43.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with43.Add("OPR_IN", ifDBNull(strOpr)).Direction = ParameterDirection.Input;
                _with43.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with43.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with43.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with43.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with43.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with43.Add("IMPORT_IN", ((strImp == null) ? "" : strImp)).Direction = ParameterDirection.Input;
                _with43.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();

                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        public string FetchVesVoyDo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strOpr = null;
            string strPol = null;
            string strPod = null;
            string strSDate = null;
            string strVES = null;
            string strVOY = null;
            string strReq = null;
            string strImp = null;
            string LOc = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strOpr = Convert.ToString(arr.GetValue(1));
            strPol = Convert.ToString(arr.GetValue(2));
            strPod = Convert.ToString(arr.GetValue(3));
            strSDate = Convert.ToString(arr.GetValue(4));
            // strVES = arr(5)
            //strVOY = arr(6)
            if (arr.Length > 5)
                strVES = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strVOY = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strImp = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                LOc = Convert.ToString(arr.GetValue(8));
            if (string.IsNullOrEmpty(strImp))
            {
                strImp = null;
            }
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.VESSEL_VOYAGE_DO";
                var _with44 = SCM.Parameters;
                _with44.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with44.Add("OPR_IN", ifDBNull(strOpr)).Direction = ParameterDirection.Input;
                _with44.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with44.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with44.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with44.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with44.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with44.Add("IMPORT_IN", ((strImp == null) ? "" : strImp)).Direction = ParameterDirection.Input;
                _with44.Add("LOCATION", (string.IsNullOrEmpty(LOc) ? "" : LOc)).Direction = ParameterDirection.Input;
                _with44.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        //aaded by minakshi on 8-jan-2009 for removals place
        public string FetchRemovalsPlace(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string Port = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_REMOVAL_PLR";
                var _with45 = SCM.Parameters;
                _with45.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with45.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with45.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        //'ended by minakshi

        #region " Supporting Function "

        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }

        #endregion " Supporting Function "

        #endregion "Enhance Search Functions "

        #region "getDivFacMW"

        public DataSet getDivFacMW(string pk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                if ((pk != null))
                {
                    if (!string.IsNullOrEmpty(pk))
                    {
                        strQuery.Append(" SELECT DISTINCT CARGO_MEASUREMENT, CARGO_WEIGHT_IN, CARGO_DIVISION_FACT ");
                        strQuery.Append(" FROM BOOKING_CARGO_CALC WHERE ");
                        strQuery.Append(" BOOKING_MST_FK = " + pk);
                        return objWF.GetDataSet(strQuery.ToString());
                    }
                }
                return null;
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

        #endregion "getDivFacMW"

        #region " SHARED: Update Other Freight Table with supplied string "

        public static double UpdateOTHFreights(DataTable DT = null, string strRows = "", Int16 FreightCol = 0, Int16 CurrencyCol = 1, Int16 AmountCol = 2, DataTable ExchTable = null, long TransactionPk = -1, Int16 PkCol = 3)
        {
            try
            {
                strRows = strRows.TrimEnd('^');
                bool ToCreate = false;
                if (DT == null)
                {
                    DT = (new WorkFlow()).GetDataTable(" Select FREIGHT_ELEMENT_MST_FK, " + " CURRENCY_MST_FK, 0 AMOUNT, QUOTATION_DTL_FK,1 PYMT_TYPE ,1 CHARGE_BASIS,   0 BASIS_RATE " + " from QUOTATION_OTHER_FREIGHT_TRN where 1 = 2 ");
                    ToCreate = true;
                }
                if (DT.Columns.Count == 0)
                {
                    DT = (new WorkFlow()).GetDataTable(" Select FREIGHT_ELEMENT_MST_FK, " + " CURRENCY_MST_FK, 0 AMOUNT, QUOTATION_DTL_FK,1 PYMT_TYPE, 1 CHARGE_BASIS,   0 BASIS_RATE  " + " from QUOTATION_OTHER_FREIGHT_TRN where 1 = 2 ");
                    ToCreate = true;
                }

                if (ExchTable == null)
                {
                    ExchTable = (new WorkFlow()).GetDataTable(" Select CURRENCY_MST_BASE_FK, " + " CURRENCY_MST_FK, EXCHANGE_RATE " + " from V_EXCHANGE_RATE where " + " sysdate between FROM_DATE and TO_DATE ");
                }
                if (ExchTable.Columns.Count == 0)
                {
                    ExchTable = (new WorkFlow()).GetDataTable(" Select CURRENCY_MST_BASE_FK, " + " CURRENCY_MST_FK, EXCHANGE_RATE " + " from V_EXCHANGE_RATE where " + " sysdate between FROM_DATE and TO_DATE and exch_rate_type_fk = 1 and exch_rate_type_fk = 1");
                }
                double sum = 0;
                Array arr = null;
                arr = strRows.Split('^');
                Int16 i = default(Int16);
                Int16 exRowCnt = default(Int16);
                Int16 RowCnt = default(Int16);
                Int16 ColCnt = default(Int16);
                bool Flag = true;
                DataRow DR = null;
                for (i = 0; i <= arr.Length - 1; i++)
                {
                    Array innerArr = null;
                    innerArr = Convert.ToString(arr.GetValue(i)).Split('~');
                    if (ToCreate)
                    {
                        DR = DT.NewRow();
                        DR[0] = innerArr.GetValue(0);
                        DR[1] = innerArr.GetValue(1);
                        DR[2] = innerArr.GetValue(2);
                        DR[3] = TransactionPk;
                        DR[4] = innerArr.GetValue(3);
                        DT.Rows.Add(DR);
                        for (exRowCnt = 0; exRowCnt <= ExchTable.Rows.Count - 1; exRowCnt++)
                        {
                            if (ExchTable.Rows[exRowCnt]["CURRENCY_MST_FK"] == innerArr.GetValue(1))
                            {
                                sum += Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * Convert.ToInt32(innerArr.GetValue(2));
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                    }
                    else
                    {
                        Flag = false;
                        for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                        {
                            if (TransactionPk == -1 || Convert.ToInt64(DT.Rows[RowCnt][PkCol]) == TransactionPk)
                            {
                                if (DT.Rows[RowCnt][FreightCol] == innerArr.GetValue(0))
                                {
                                    DT.Rows[RowCnt][CurrencyCol] = innerArr.GetValue(1);
                                    DT.Rows[RowCnt][AmountCol] = innerArr.GetValue(2);
                                    Flag = true;
                                    for (exRowCnt = 0; exRowCnt <= ExchTable.Rows.Count - 1; exRowCnt++)
                                    {
                                        if (ExchTable.Rows[exRowCnt]["CURRENCY_MST_FK"] == innerArr.GetValue(1))
                                        {
                                            sum += Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * Convert.ToInt32(innerArr.GetValue(2));
                                            break; // TODO: might not be correct. Was : Exit For
                                        }
                                    }
                                    break; // TODO: might not be correct. Was : Exit For
                                }
                            }
                        }
                        if (Flag == false)
                        {
                            DR = DT.NewRow();
                            if (DT.Rows.Count > 0)
                            {
                                for (ColCnt = 0; ColCnt <= DT.Columns.Count - 1; ColCnt++)
                                {
                                    DR[ColCnt] = DT.Rows[0][ColCnt];
                                }
                            }
                            DR[FreightCol] = innerArr.GetValue(0);
                            DR[CurrencyCol] = innerArr.GetValue(1);
                            DR[AmountCol] = innerArr.GetValue(2);
                            DR[PkCol] = TransactionPk;
                            DR[4] = innerArr.GetValue(3);
                            DT.Rows.Add(DR);
                            for (exRowCnt = 0; exRowCnt <= ExchTable.Rows.Count - 1; exRowCnt++)
                            {
                                if (ExchTable.Rows[exRowCnt]["CURRENCY_MST_FK"] == innerArr.GetValue(1))
                                {
                                    sum += Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * Convert.ToInt32(innerArr.GetValue(2));
                                    break; // TODO: might not be correct. Was : Exit For
                                }
                            }
                        }
                    }
                }
                return sum;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception eX)
            {
                return -1.0;
            }
        }

        #endregion " SHARED: Update Other Freight Table with supplied string "

        #region "Fetch Location of Port "

        public DataSet fetch_Port_Location_Fk(string strPODfk, string JCDate = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                //strQuery.Append("SELECT LOC.LOCATION_MST_PK " & vbCrLf)
                //strQuery.Append("FROM LOCATION_MST_TBL LOC, LOCATION_WORKING_PORTS_TRN LOCT " & vbCrLf)
                //strQuery.Append("WHERE LOC.LOCATION_MST_PK = LOCT.LOCATION_MST_FK" & vbCrLf)
                //strQuery.Append("AND LOCT.PORT_MST_FK=" & strPODfk & vbCrLf)
                //strQuery.Append("ORDER BY LOC.LOCATION_MST_PK " & vbCrLf)
                if (string.IsNullOrEmpty(JCDate))
                {
                    strQuery.Append(" SELECT DISTINCT A.LOCATION_MST_PK, A.LOCATION_ID ");
                    strQuery.Append(" FROM LOCATION_MST_TBL A, PORT_MST_TBL B, LOCATION_WORKING_PORTS_TRN C ");
                    strQuery.Append(" WHERE A.LOCATION_MST_PK = B.LOCATION_MST_FK");
                    strQuery.Append(" AND B.LOCATION_MST_FK = C.LOCATION_MST_FK");
                    strQuery.Append(" AND B.PORT_MST_PK=" + strPODfk);
                    strQuery.Append("");
                }
                else
                {
                    strQuery.Append("SELECT DISTINCT LOC.LOCATION_MST_PK,");
                    strQuery.Append("                LOC.LOCATION_ID,");
                    strQuery.Append("                CNT.CURRENCY_MST_FK,");
                    strQuery.Append("                CMT.CURRENCY_ID,");
                    strQuery.Append("                GET_EX_RATE(CNT.CURRENCY_MST_FK,");
                    strQuery.Append("                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                    strQuery.Append("                            TO_DATE('" + JCDate + "', 'DD/MM/YYYY')) ROE,");
                    strQuery.Append("                GET_EX_RATE_BUY(CNT.CURRENCY_MST_FK,");
                    strQuery.Append("                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                    strQuery.Append("                            TO_DATE('" + JCDate + "', 'DD/MM/YYYY')) ROE_BUY ");
                    strQuery.Append("  FROM LOCATION_MST_TBL           LOC,");
                    strQuery.Append("       PORT_MST_TBL               PORT,");
                    strQuery.Append("       LOCATION_WORKING_PORTS_TRN LOCP,");
                    strQuery.Append("       COUNTRY_MST_TBL            CNT,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL      CMT");
                    strQuery.Append(" WHERE LOC.LOCATION_MST_PK = PORT.LOCATION_MST_FK");
                    strQuery.Append("   AND PORT.LOCATION_MST_FK = LOCP.LOCATION_MST_FK");
                    strQuery.Append("   AND CNT.COUNTRY_MST_PK = LOC.COUNTRY_MST_FK");
                    strQuery.Append("   AND CNT.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK(+)");
                    strQuery.Append("   AND PORT.PORT_MST_PK =" + strPODfk);
                }
                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "Fetch Location of Port "

        #region "GetJobPkRegion"

        public string GetJobPk(string bPK, string refno)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            DataTable dt = null;
            string strJobpk = "";
            Int32 j = default(Int32);
            strBuilder.Append("SELECT J.JOB_CARD_TRN_PK,J.JOBCARD_REF_NO FROM JOB_CARD_TRN J WHERE J.BOOKING_MST_FK=" + bPK);
            try
            {
                objwf.MyDataReader = objwf.GetDataReader(strBuilder.ToString());
                while (objwf.MyDataReader.Read())
                {
                    strJobpk = Convert.ToString(objwf.MyDataReader.GetInt32(0));
                    refno = objwf.MyDataReader.GetString(1);
                }
                objwf.MyDataReader.Close();
                return strJobpk;
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

        #endregion "GetJobPkRegion"

        #region "Vessel/voyage saving"

        //'This function will be used to Insert/update the Vessel Master Details to "vessel_voyage_tbl"
        //' If inserted/Updated then the PK Value will be Returned
        //' If the Insrtion of Updation Failed, then 0 will be passed
        public ArrayList SaveVesselMaster(long dblVesselPK, string strVesselName, long dblOperatorFK, string strVesselID, string VoyNo, OracleCommand SelectCommand, long POLPK, string PODPK, System.DateTime POLETA, System.DateTime POLETD,
        System.DateTime POLCUT, System.DateTime PODETA, DateTime ATDPOL, DateTime ATAPOD)
        {
            WorkFlow objWK = new WorkFlow();
            //Dim TRAN As OracleTransaction
            //objWK.OpenConnection()
            //TRAN = objWK.MyConnection.BeginTransaction()
            int RESULT = 0;
            try
            {
                //Insert
                if (dblVesselPK == 0)
                {
                    OracleCommand InsCommand = new OracleCommand();
                    Int16 VER = default(Int16);
                    var _with46 = SelectCommand;
                    _with46.Parameters.Clear();
                    _with46.CommandType = CommandType.StoredProcedure;
                    _with46.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TBL_INS";
                    _with46.Parameters.Add("OPERATOR_MST_FK_IN", dblOperatorFK).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("VESSEL_NAME_IN", strVesselName).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("VESSEL_ID_IN", strVesselID).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("ACTIVE_IN", 1).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    RESULT = _with46.ExecuteNonQuery();
                    dblVesselPK = Convert.ToInt64(_with46.Parameters["RETURN_VALUE"].Value);
                    _with46.Parameters.Clear();
                }
                //Update the Details Table
                var _with47 = SelectCommand;
                _with47.Parameters.Clear();
                _with47.CommandType = CommandType.StoredProcedure;
                _with47.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_INS";
                _with47.Parameters.Add("VESSEL_VOYAGE_TBL_FK_IN", dblVesselPK).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("VOYAGE_IN", VoyNo).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("PORT_MST_POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("PORT_MST_POD_FK_IN", getDefault(PODPK, DBNull.Value)).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("POL_ETA_IN", getDefault((POLETA == DateTime.MinValue ? DateTime.MinValue : POLETA), DBNull.Value)).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("POL_ETD_IN", getDefault((POLETD == DateTime.MinValue ? DateTime.MinValue : POLETD), DBNull.Value)).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("POL_CUT_OFF_DATE_IN", getDefault((POLCUT == DateTime.MinValue ? DateTime.MinValue : POLCUT), DBNull.Value)).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("POD_ETA_IN", getDefault((PODETA == DateTime.MinValue ? DateTime.MinValue : PODETA), DBNull.Value)).Direction = ParameterDirection.Input;
                //ADD BY LATHA ON APRIL 2
                _with47.Parameters.Add("ATD_POL_IN", getDefault((ATDPOL == DateTime.MinValue ? DateTime.MinValue : ATDPOL), DBNull.Value)).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("ATA_POD_IN", getDefault((ATAPOD == DateTime.MinValue ? DateTime.MinValue : ATAPOD), DBNull.Value)).Direction = ParameterDirection.Input;
                //END BY LATHA
                _with47.Parameters.Add("CUSTOMS_CALL_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("CAPTAIN_NAME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("PORT_CALL_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("NRT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("GRT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with47.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 4, "VOYAGE_TRN_PK").Direction = ParameterDirection.Output;
                RESULT = _with47.ExecuteNonQuery();
                dblVesselPK = Convert.ToInt32(_with47.Parameters["RETURN_VALUE"].Value);
                _with47.Parameters.Clear();
                arrMessage.Add("All data saved successfully");
            }
            catch (OracleException OraExp)
            {
                if (string.Compare(OraExp.Message, "ORA-00001") > 0)
                {
                    arrMessage.Add("Vessel or Voyage Already Exist in Database.");
                }
                else
                {
                    arrMessage.Add(OraExp.Message);
                }
            }
            catch (Exception ex)
            {
                //Throw ex
                if (string.Compare(ex.Message, "ORA-00001") > 0)
                {
                    arrMessage.Add("Vessel or Voyage Already Exist in Database.");
                }
                else
                {
                    arrMessage.Add(ex.Message);
                }
            }
            return arrMessage;
        }

        #endregion "Vessel/voyage saving"

        #region "FETCH QUOTATION PK"

        public string Fetch_Quote_Pk(string ref_nr)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                strQuery.Append(" SELECT QMAIN.QUOTATION_MST_PK FROM quotation_MST_tbl  QMAIN WHERE QMAIN.QUOTATION_REF_NO = '" + ref_nr + "'");
                return ObjWk.ExecuteScaler(strQuery.ToString());
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

        public string Fetch_Quote_Status(int QuotePK, string QType = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string Status = "";
            try
            {
                strQuery.Append(" select TO_CHAR(QMAIN.QUOTATION_MST_PK) QUOTATION_MST_PK,TO_CHAR(QMAIN.Status) Status,TO_CHAR(QMAIN.QUOTATION_TYPE) QUOTATION_TYPE from quotation_MST_tbl  QMAIN where QMAIN.QUOTATION_MST_PK = " + QuotePK);
                ObjWk.MyDataReader = ObjWk.GetDataReader(strQuery.ToString());
                while (ObjWk.MyDataReader.Read())
                {
                    Status = ObjWk.MyDataReader.GetString(1);
                    if ((QType != null))
                    {
                        QType = ObjWk.MyDataReader.GetString(2);
                    }
                }
                ObjWk.MyDataReader.Close();
                return Status;
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

        public string Fetch_Cust_Cont_Pk(string ref_nr)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                strQuery.Append("SELECT QMAIN.CONT_CUST_AIR_PK FROM CONT_CUST_AIR_TBL  QMAIN where QMAIN.CONT_REF_NO ='" + ref_nr + "'");
                return ObjWk.ExecuteScaler(strQuery.ToString());
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

        public Int16 FetchEBKTrans(Int16 BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            Int16 IS_EBOOKING_CHK = 0;
            try
            {
                sqlStr = " SELECT DISTINCT BTRN.BOOKING_TRN_PK  FROM BOOKING_MST_TBL BMT, BOOKING_TRN BTRN ";
                sqlStr += "  WHERE BMT.BOOKING_MST_PK = BTRN.BOOKING_MST_FK  ";
                sqlStr += "  AND BMT.BOOKING_MST_PK = " + BookingPK;

                IS_EBOOKING_CHK = Convert.ToInt16(objWF.ExecuteScaler(sqlStr));
                return IS_EBOOKING_CHK;
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

        #endregion "FETCH QUOTATION PK"

        #region "Fetch Sales Executive"

        public DataSet FetchSalesExecutive(Int16 CustPk = 0, int BkgPK = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            if (BkgPK == 0)
            {
                sb.Append("SELECT NVL(C.REP_EMP_MST_FK, SEMP.EMPLOYEE_MST_PK) REPPK,NVL(E.EMPLOYEE_NAME,SEMP.EMPLOYEE_NAME) SALESREP");
                sb.Append("  FROM CUSTOMER_MST_TBL C, EMPLOYEE_MST_TBL E , EMPLOYEE_MST_TBL SEMP");
                sb.Append(" WHERE C.REP_EMP_MST_FK = E.EMPLOYEE_MST_PK(+) AND C.REQ_SALES_EXE = SEMP.EMPLOYEE_MST_PK(+)");
                sb.Append("   AND C.CUSTOMER_MST_PK = " + CustPk);
            }
            else
            {
                if (Convert.ToInt32(objWF.ExecuteScaler("Select count(*) from booking_mst_tbl b where b.executive_mst_fk is not null and b.booking_mst_pk=" + BkgPK)) > 0)
                {
                    sb.Append("SELECT B.executive_mst_fk REPPK,E.EMPLOYEE_NAME SALESREP");
                    sb.Append("  FROM booking_mst_tbl B, EMPLOYEE_MST_TBL E");
                    sb.Append(" WHERE B.executive_mst_fk = E.EMPLOYEE_MST_PK");
                    sb.Append("   AND b.booking_mst_pk= " + BkgPK);
                }
                else
                {
                    sb.Append("SELECT  '' REPPK, '' SALESREP");
                    sb.Append("  FROM dual");
                    //sb.Append(" WHERE C.REP_EMP_MST_FK = E.EMPLOYEE_MST_PK(+) AND C.REQ_SALES_EXE = SEMP.EMPLOYEE_MST_PK(+)")
                    //sb.Append("   AND C.CUSTOMER_MST_PK = " & CustPk)
                }
            }
            try
            {
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion "Fetch Sales Executive"

        #region "Credit Control"

        public object FetchCreditDetails(string BookingPk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            WorkFlow ObjWk = new WorkFlow();

            strSQL.Append(" SELECT BKG.BOOKING_MST_PK,");
            strSQL.Append(" BKG.CUST_CUSTOMER_MST_FK,");
            strSQL.Append(" BKG.CREDIT_LIMIT,");
            strSQL.Append("  BKG.BOOKING_REF_NO,");
            strSQL.Append(" SUM(BTS.ALL_IN_TARIFF) AS TOTAL");

            strSQL.Append("  FROM BOOKING_TRN_FRT_DTLS BFD,");
            strSQL.Append(" CUSTOMER_MST_TBL         CMT,");
            strSQL.Append("  BOOKING_MST_TBL          BKG,");
            strSQL.Append("  BOOKING_TRN  BTS");

            strSQL.Append(" WHERE BKG.BOOKING_MST_PK=BTS.BOOKING_MST_FK(+)");
            strSQL.Append("  AND BTS.BOOKING_TRN_PK=BFD.BOOKING_TRN_FK(+)");
            strSQL.Append(" AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strSQL.Append(" AND BKG.BOOKING_MST_PK=" + BookingPk);
            strSQL.Append(" GROUP BY BKG.BOOKING_MST_PK,BKG.CUST_CUSTOMER_MST_FK, BKG.CREDIT_LIMIT,BKG.BOOKING_REF_NO");

            try
            {
                return (ObjWk.GetDataSet(strSQL.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Credit Control"

        #region "Fetching CreditPolicy Details based on Customer"

        public object FetchCreditPolicy(string ShipperPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT CMT.CREDIT_LIMIT,");
            strSQL.Append(" cmt.customer_name,");
            strSQL.Append(" CMT.CREDIT_DAYS,");
            strSQL.Append(" CMT.SEA_APP_BOOKING,");
            strSQL.Append(" CMT.SEA_APP_BL_RELEASE,");
            strSQL.Append(" CMT.SEA_APP_RELEASE_ODR");
            strSQL.Append(" FROM CUSTOMER_MST_TBL CMT");
            strSQL.Append(" WHERE CMT.CUSTOMER_MST_PK = " + ShipperPK);

            try
            {
                return (objWF.GetDataSet(strSQL.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetching CreditPolicy Details based on Customer"

        #region "Fetch Shipping Line ID"

        public string FetchSLID(Int32 OprPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strBuilder.Append(" SELECT O.OPERATOR_ID FROM OPERATOR_MST_TBL O WHERE O.OPERATOR_MST_PK=" + OprPK);
            try
            {
                return objWF.ExecuteScaler(strBuilder.ToString());
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

        #endregion "Fetch Shipping Line ID"

        #region "Common Functions"

        public DataTable GetContainerDetails(string Key = "", string FilterBy = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT CONT.VOLUME,");
            sb.Append("       CONT.CONTAINER_LENGTH_FT,");
            sb.Append("       CONT.CONTAINER_WIDTH_FT,");
            sb.Append("       CONT.CONTAINER_HEIGHT_FT,");
            sb.Append("       CONT.CONTAINER_TYPE_MST_PK,");
            sb.Append("       CONT.CONTAINER_TYPE_MST_ID,");
            sb.Append("       CONT.CONTAINER_TYPE_NAME,");
            sb.Append("       CONT.ACTIVE_FLAG,");
            sb.Append("       CONT.CREATED_BY_FK,");
            sb.Append("       CONT.CREATED_DT,");
            sb.Append("       CONT.LAST_MODIFIED_BY_FK,");
            sb.Append("       CONT.LAST_MODIFIED_DT,");
            sb.Append("       CONT.VERSION_NO,");
            sb.Append("       CONT.CONTAINER_KIND,");
            sb.Append("       CONT.CONTAINER_TAREWEIGHT_TONE,");
            sb.Append("       CONT.CONTAINER_MAX_CAPACITY_TONE,");
            sb.Append("       CONT.TEU_FACTOR,");
            sb.Append("       CONT.PREFERENCES");
            sb.Append("  FROM CONTAINER_TYPE_MST_TBL CONT");
            if (!string.IsNullOrEmpty(FilterBy))
            {
                switch (FilterBy.ToUpper())
                {
                    case "ID":
                        sb.Append(" WHERE CONT.CONTAINER_TYPE_MST_ID = '" + Key + "'");
                        break;

                    case "PK":
                        sb.Append(" WHERE CONT.CONTAINER_TYPE_MST_PK IN (" + Key + ")");
                        break;
                }
            }
            WorkFlow objWf = new WorkFlow();
            try
            {
                return objWf.GetDataTable(sb.ToString());
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

        #endregion "Common Functions"

        #region " CRO "

        public DataSet LoadCRODetails(Int32 BkgPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strBuilder.Append(" SELECT B.CRO_PK, TO_CHAR(B.CONT_REQ_DATE,DATEFORMAT) CONT_REQ_DATE, B.CONT_MOVE_BY,");
            strBuilder.Append(" B.CUST_TRANS_DTLS, B.VENDOR_MST_FK, VMT.VENDOR_ID, B.CRO_RECEIVED, B.CRO_NR,");
            strBuilder.Append(" TO_CHAR(B.CRO_DATE,DATEFORMAT) CRO_DATE, TO_CHAR(B.CRO_VALID_TILL,DATEFORMAT) CRO_VALID_TILL,");
            strBuilder.Append(" B.YARD_DETAILS, B.VERSION_NO FROM BOOKING_CRO_TBL B, VENDOR_MST_TBL VMT");
            strBuilder.Append(" WHERE B.VENDOR_MST_FK = VMT.VENDOR_MST_PK(+) AND B.BOOKING_MST_FK=" + BkgPK);
            try
            {
                return objWF.GetDataSet(strBuilder.ToString());
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

        public DataSet CustCRODetails(string BKGPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strBuilder.Append(" SELECT CMT.CUSTOMER_NAME, BOOK.BOOKING_REF_NO,AFDT.FILE_PATH, AFDT.FILE_NAME,CCD.ADM_EMAIL_ID");
            strBuilder.Append("  FROM BOOKING_MST_TBL BOOK, CUSTOMER_MST_TBL CMT, BOOKING_CRO_TBL BCRO,ATTACH_FILE_DTL_TBL AFDT, CUSTOMER_CONTACT_DTLS CCD");
            strBuilder.Append("  WHERE BOOK.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strBuilder.Append("  AND BOOK.BOOKING_MST_PK = BCRO.BOOKING_MST_FK");
            strBuilder.Append("  AND BCRO.CRO_PK = AFDT.CRO_FK(+)");
            strBuilder.Append("  AND CMT.CUSTOMER_MST_PK=CCD.CUSTOMER_MST_FK");
            strBuilder.Append("  AND BOOK.BOOKING_MST_PK =" + BKGPK);
            try
            {
                return objWF.GetDataSet(strBuilder.ToString());
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

        public DataSet LineCRODetails(string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT BOOK.BOOKING_MST_PK,");
            sb.Append("       OPR.OPERATOR_NAME,");
            sb.Append("       POL.PORT_NAME POL,");
            sb.Append("       POD.PORT_NAME POD,");
            sb.Append("       COMM.COMMODITY_GROUP_CODE,");
            sb.Append("       CTMT.CONTAINER_TYPE_MST_ID,");
            sb.Append("       BOOK.LINE_BKG_NO,");
            sb.Append("       BOOK.SHIPMENT_DATE,");
            sb.Append("       BOOK.VESSEL_NAME,");
            sb.Append("       BOOK.BOOKING_REF_NO,");
            sb.Append("       OCD.ADM_EMAIL_ID, ");
            sb.Append("       BCRO.CONT_REQ_DATE");
            sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
            sb.Append("       BOOKING_TRN BKGTRN,");
            sb.Append("       OPERATOR_MST_TBL        OPR,");
            sb.Append("       PORT_MST_TBL            POL,");
            sb.Append("       PORT_MST_TBL            POD,");
            sb.Append("       COMMODITY_GROUP_MST_TBL COMM,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
            sb.Append("       OPERATOR_CONTACT_DTLS   OCD,");
            sb.Append("       BOOKING_CRO_TBL BCRO");
            sb.Append(" WHERE BOOK.BOOKING_MST_PK = BKGTRN.BOOKING_MST_FK");
            sb.Append("   AND BOOK.CARRIER_MST_FK = OPR.OPERATOR_MST_PK");
            sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND BOOK.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK(+)");
            sb.Append("   AND BKGTRN.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND OPR.OPERATOR_MST_PK = OCD.OPERATOR_MST_FK");
            sb.Append("    AND BOOK.BOOKING_MST_PK=BCRO.BOOKING_MST_FK(+)");
            sb.Append("   AND BOOK.BOOKING_MST_PK = " + BKGPK);
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        public DataSet LineDetails(string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT BOOK.BOOKING_MST_PK,");
            sb.Append("       BCRO.CONT_REQ_DATE,");
            sb.Append("       AFDT.FILE_NAME,");
            sb.Append("       AFDT.FILE_PATH");
            sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
            sb.Append("       BOOKING_CRO_TBL         BCRO,");
            sb.Append("       ATTACH_FILE_DTL_TBL     AFDT");
            sb.Append("  WHERE ");
            sb.Append("   BOOK.BOOKING_MST_PK = BCRO.BOOKING_MST_FK");
            sb.Append("   AND BCRO.CRO_PK = AFDT.CRO_FK");
            sb.Append("   AND BOOK.BOOKING_MST_PK = " + BKGPK);
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        public ArrayList Save(Int32 CROPk, Int32 BkgFk, System.DateTime ContReqDate, Int32 ContMoveBy, string CustTransDtls, Int32 VendorFk, Int32 CROReceived, string CRONr, System.DateTime CRODate, System.DateTime CROValidTill,
        string YardDtls, Int32 versionNr)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();

            try
            {
                arrMessage.Clear();
                if (CROPk == 0)
                {
                    var _with48 = objWK.MyCommand;
                    _with48.CommandType = CommandType.StoredProcedure;
                    _with48.CommandText = objWK.MyUserName + ".BOOKING_CRO_TBL_PKG.BOOKING_CRO_TBL_INS";
                    _with48.Parameters.Clear();
                    _with48.Parameters.Add("CONT_REQ_DATE_IN", (ContReqDate == DateTime.MinValue ? DateTime.MinValue : ContReqDate)).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("CONT_MOVE_BY_IN", getDefault(ContMoveBy, 0)).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("CUST_TRANS_DTLS_IN", (string.IsNullOrEmpty(CustTransDtls) ? "" : CustTransDtls)).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("VENDOR_MST_FK_IN", getDefault(VendorFk, 0)).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("CRO_RECEIVED_IN", getDefault(CROReceived, 0)).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("CRO_NR_IN", (string.IsNullOrEmpty(CRONr) ? "" : CRONr)).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("CRO_DATE_IN", (CRODate == DateTime.MinValue ? DateTime.MinValue : CRODate)).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("CRO_VALID_TILL_IN", (CROValidTill == DateTime.MinValue ? DateTime.MinValue : CROValidTill)).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("BOOKING_SEA_FK_IN", BkgFk).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("YARD_DTLS_IN", (string.IsNullOrEmpty(YardDtls) ? "" : YardDtls)).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with48.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with48.ExecuteNonQuery();
                }
                else
                {
                    var _with49 = objWK.MyCommand;
                    _with49.CommandType = CommandType.StoredProcedure;
                    _with49.CommandText = objWK.MyUserName + ".BOOKING_CRO_TBL_PKG.BOOKING_CRO_TBL_UPD";
                    _with49.Parameters.Clear();
                    _with49.Parameters.Add("CRO_PK_IN", CROPk).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("CONT_REQ_DATE_IN", (ContReqDate == DateTime.MinValue ? DateTime.MinValue : ContReqDate)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("CONT_MOVE_BY_IN", getDefault(ContMoveBy, 0)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("CUST_TRANS_DTLS_IN", (string.IsNullOrEmpty(CustTransDtls) ? "" : CustTransDtls)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("VENDOR_MST_FK_IN", getDefault(VendorFk, 0)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("CRO_RECEIVED_IN", getDefault(CROReceived, 0)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("CRO_NR_IN", (string.IsNullOrEmpty(CRONr) ? "" : CRONr)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("CRO_DATE_IN", (CRODate == DateTime.MinValue ? DateTime.MinValue : CRODate)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("CRO_VALID_TILL_IN", (CROValidTill == DateTime.MinValue ? DateTime.MinValue : CROValidTill)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("VERSION_NO_IN", getDefault(versionNr, 0)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("BOOKING_SEA_FK_IN", BkgFk).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("YARD_DTLS_IN", (string.IsNullOrEmpty(YardDtls) ? "" : YardDtls)).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with49.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with49.ExecuteNonQuery();
                }
                if (CROReceived == 1)
                {
                    if (CROPk == 0)
                    {
                        arrMessage = objTrackNTrace.SaveTrackAndTrace(BkgFk, 2, 1, "CRO Issued", "BOOKING-CRO", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWK, "INS", M_CREATED_BY_FK, "O");
                    }
                }
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
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
                objWK.CloseConnection();
            }
        }

        public DataSet loadGridDtls(Int32 intBkgPk, Int32 intCROPk, Int32 cnt = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSql = "";

            strSql = " SELECT COUNT(*) FROM BOOKING_CRO_TRN BCT, BOOKING_CRO_TBL BTC WHERE ";
            strSql += " BTC.CRO_PK = BCT.CRO_FK AND BTC.CRO_PK = " + intCROPk;
            cnt = Convert.ToInt32(objWF.ExecuteScaler(strSql));

            if (cnt == 0)
            {
                sb.Append("SELECT CTMT.CONTAINER_TYPE_MST_PK PK, CTMT.CONTAINER_TYPE_MST_ID CONTAINERTYPE, ");
                sb.Append(" '' CONTAINERNR, '' SEALNR, BTS.NO_OF_BOXES NOOFBOXES, '' TRNPK ");
                sb.Append("                         FROM CONTAINER_TYPE_MST_TBL  CTMT,");
                sb.Append("                         BOOKING_MST_TBL         BST,");
                sb.Append("                         BOOKING_TRN BTS");
                sb.Append("                         WHERE BTS.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                sb.Append("                           AND BTS.CONTAINER_TYPE_MST_FK =");
                sb.Append("                               CTMT.CONTAINER_TYPE_MST_PK");
                sb.Append("                           AND BST.BOOKING_MST_PK = " + intBkgPk);
                sb.Append("                         ORDER BY CTMT.CONTAINER_TYPE_MST_ID");
            }
            else
            {
                sb.Append("SELECT CTMT.CONTAINER_TYPE_MST_PK PK, CTMT.CONTAINER_TYPE_MST_ID CONTAINERTYPE, ");
                sb.Append(" BCT.CONTAINER_NO CONTAINERNR, BCT.SEAL_NO SEALNR, BTS.NO_OF_BOXES NOOFBOXES, BCT.CRO_TRN_PK TRNPK ");
                sb.Append("                         FROM CONTAINER_TYPE_MST_TBL  CTMT,");
                sb.Append("                         BOOKING_CRO_TRN              BCT,");
                sb.Append("                         BOOKING_CRO_TBL              BTC,");
                sb.Append("                         BOOKING_MST_TBL              BST,");
                sb.Append("                         BOOKING_TRN      BTS");
                sb.Append("                         WHERE BTC.CRO_PK = BCT.CRO_FK");
                sb.Append("                           AND BCT.CONTAINER_TYPE_MST_FK =");
                sb.Append("                               CTMT.CONTAINER_TYPE_MST_PK");
                sb.Append("                           AND BST.BOOKING_MST_PK = BTS.BOOKING_MST_FK");
                sb.Append("                           AND BST.BOOKING_MST_PK = BTC.BOOKING_MST_FK");
                sb.Append("                           AND BTS.CONTAINER_TYPE_MST_FK = BCT.CONTAINER_TYPE_MST_FK");
                sb.Append("                           AND BTC.CRO_PK = " + intCROPk);
                sb.Append("                         ORDER BY CTMT.CONTAINER_TYPE_MST_ID");
            }

            return objWF.GetDataSet(sb.ToString());
        }

        public ArrayList saveContainerDetails(int intCROPk, Int32 versionNr, DataSet M_DataSet, Int32 cnt = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataRow dr = null;
            OracleCommand SCM = new OracleCommand();
            arrMessage.Clear();
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;

                if (M_DataSet.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dr_loopVariable in M_DataSet.Tables[0].Rows)
                    {
                        dr = dr_loopVariable;
                        if (Convert.ToInt32(string.IsNullOrEmpty(dr["TRNPK"].ToString()) ? 0 : dr["TRNPK"]) == 0)
                        {
                            SCM.CommandText = objWF.MyUserName + ".BOOKING_CRO_TRN_PKG.BOOKING_CRO_TRN_INS";
                            var _with50 = SCM.Parameters;
                            _with50.Clear();
                            _with50.Add("CRO_FK_IN", intCROPk).Direction = ParameterDirection.Input;
                            _with50.Add("CONTAINER_TYPE_MST_FK_IN", dr["PK"]).Direction = ParameterDirection.Input;
                            _with50.Add("CONTAINER_NO_IN", dr["CONTAINERNR"]).Direction = ParameterDirection.Input;
                            _with50.Add("SEAL_NO_IN", dr["SEALNR"]).Direction = ParameterDirection.Input;
                            _with50.Add("CREATED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                            _with50.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with50.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            SCM.CommandText = objWF.MyUserName + ".BOOKING_CRO_TRN_PKG.BOOKING_CRO_TRN_UPD";
                            var _with51 = SCM.Parameters;
                            _with51.Clear();

                            _with51.Add("CRO_TRN_PK_IN", dr["TRNPK"]).Direction = ParameterDirection.Input;
                            _with51.Add("CRO_FK_IN", intCROPk).Direction = ParameterDirection.Input;
                            _with51.Add("CONTAINER_TYPE_MST_FK_IN", dr["PK"]).Direction = ParameterDirection.Input;
                            _with51.Add("CONTAINER_NO_IN", dr["CONTAINERNR"]).Direction = ParameterDirection.Input;
                            _with51.Add("SEAL_NO_IN", dr["SEALNR"]).Direction = ParameterDirection.Input;
                            _with51.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                            _with51.Add("VERSION_NO_IN", getDefault(versionNr - 1, 0)).Direction = ParameterDirection.Input;
                            _with51.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with51.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        }
                        SCM.ExecuteNonQuery();
                    }
                }
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
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
                objWF.CloseConnection();
            }
        }

        #endregion " CRO "



        #region "GET JOBCARD CONT PKS"

        public DataSet GetJobContPks(string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append(" SELECT JOBTRN.JOB_TRN_CONT_PK");
            sb.Append("  FROM BOOKING_MST_TBL  BKG,JOB_CARD_TRN JOB,JOB_TRN_CONT JOBTRN");
            sb.Append("  WHERE BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
            sb.Append("  AND JOB.JOB_CARD_TRN_PK = JOBTRN.JOB_CARD_TRN_FK ");
            sb.Append(" AND JOB.BOOKING_MST_FK=" + BKGPK);
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "GET JOBCARD CONT PKS"

        #region "Update Container & Seal Number"

        public ArrayList UpdateContNumber(string JobContPK, string ContainerNr = "", string SealNumber = "")
        {
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            Int32 intIns = default(Int32);
            string str = null;

            try
            {
                if (ContainerNr == "0")
                {
                    ContainerNr = "";
                }
                if (SealNumber == "0")
                {
                    SealNumber = "";
                }
                str = "  UPDATE JOB_TRN_CONT JCONT ";
                str += "  SET JCONT.CONTAINER_NUMBER = '" + ContainerNr + "',";
                str += "  JCONT.SEAL_NUMBER = '" + SealNumber + "'";
                str += "  WHERE JCONT.JOB_TRN_CONT_PK = " + JobContPK;
                var _with52 = updCmdUser;
                _with52.Connection = objWK.MyConnection;
                _with52.Transaction = TRAN;
                _with52.CommandType = CommandType.Text;
                _with52.CommandText = str;
                intIns = _with52.ExecuteNonQuery();
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "Update Container & Seal Number"

        public DataSet GetCommNames(int BkgTrnPK = 0, string BkgCargoPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow ObjWk = new WorkFlow();
            if (BkgTrnPK != 0)
            {
                sb.Append("SELECT DISTINCT BTCD.COMMODITY_FKS,");
                sb.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN(' ||");
                sb.Append("                NVL(BTCD.COMMODITY_FKS, 0) || ')') COMMODITY_NAME");
                sb.Append("  FROM BOOKING_TRN_CARGO_DTL BTCD");
                if (!string.IsNullOrEmpty(BkgCargoPks))
                {
                    sb.Append("  WHERE  BTCD.BOOKING_TRN_CARGO_PK IN(" + BkgCargoPks + ")");
                }
                else
                {
                    sb.Append(" WHERE 1=2");
                }
            }
            else
            {
                sb.Append("SELECT DISTINCT BTCD.COMMODITY_FKS,");
                sb.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN(' ||");
                sb.Append("                NVL(BTCD.COMMODITY_FKS, 0) || ')') COMMODITY_NAME");
                sb.Append("  FROM TEMP_BKG_CARGO_TBL BTCD");
                if (!string.IsNullOrEmpty(BkgCargoPks))
                {
                    sb.Append("  WHERE BTCD.BOOKING_TRN_CARGO_PK IN(" + BkgCargoPks + ")");
                }
                else
                {
                    sb.Append(" WHERE 1=2");
                }
            }
            try
            {
                return (ObjWk.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region "AutoComplete"

        public string GetFilterTextForQuotation()
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT QTN.QUOTATION_REF_NO FROM QUOTATION_SEA_TBL QTN WHERE QTN.STATUS=1 AND QTN.EXPECTED_SHIPMENT_DT>=SYSDATE ORDER BY 1");
            return AutoCompleteString(strqry.ToString());
        }

        public string GetFilterTextForPOL()
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT POL.PORT_ID FROM PORT_MST_TBL POL WHERE POL.ACTIVE_FLAG = 1 AND POL.BUSINESS_TYPE = 2 AND POL.LOCATION_MST_FK IN ");
            strqry.Append("(SELECT LMT.LOCATION_MST_PK FROM LOCATION_MST_TBL LMT START WITH LMT.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR LMT.LOCATION_MST_PK = LMT.REPORTING_TO_FK)");
            return AutoCompleteString(strqry.ToString());
        }

        public string GetFilterTextForPOD()
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT POL.PORT_ID FROM PORT_MST_TBL POL WHERE POL.ACTIVE_FLAG = 1 AND POL.BUSINESS_TYPE = 2 AND POL.LOCATION_MST_FK NOT IN ");
            strqry.Append("(SELECT LMT.LOCATION_MST_PK FROM LOCATION_MST_TBL LMT START WITH LMT.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR LMT.LOCATION_MST_PK = LMT.REPORTING_TO_FK)");
            return AutoCompleteString(strqry.ToString());
        }

        public string GetFilterTextForCustomer(int PORT_MST_FK = 0, string FLAG = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            if (FLAG.ToUpper() == "ID")
            {
                sb.Append("SELECT DISTINCT QRY.CUSTOMER_ID");
            }
            else if (FLAG.ToUpper() == "CUST_REF")
            {
                sb.Append("SELECT DISTINCT QRY.CUST_REG_NO");
            }
            else
            {
                sb.Append("SELECT DISTINCT QRY.CUSTOMER_NAME");
            }
            sb.Append("  FROM (SELECT DISTINCT CU.CUSTOMER_MST_PK,");
            sb.Append("                        CU.CUSTOMER_ID,");
            sb.Append("                        CU.CUSTOMER_NAME,");
            sb.Append("                        CU.CUST_REG_NO");
            sb.Append("          FROM CUSTOMER_MST_TBL      CU,");
            sb.Append("               CUSTOMER_MST_TBL      CG,");
            sb.Append("               CUSTOMER_CONTACT_DTLS CCD,");
            sb.Append("               LOCATION_MST_TBL      LMT,");
            sb.Append("               CUSTOMER_CONTACT_TRN  CCT,");
            sb.Append("               PORT_MST_TBL          PMT");
            sb.Append("         WHERE CU.ACTIVE_FLAG = 1");
            sb.Append("           AND CU.TEMP_PARTY <> 1");
            sb.Append("           AND CU.REF_GROUP_CUST_PK = CG.CUSTOMER_MST_PK(+)");
            sb.Append("           AND CU.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK(+)");
            sb.Append("           AND CU.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
            sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
            sb.Append("           AND LMT.LOCATION_MST_PK = PMT.LOCATION_MST_FK(+)");
            sb.Append("           AND CU.BUSINESS_TYPE IN (2, 3)");
            if (string.IsNullOrEmpty(FLAG) | FLAG.ToUpper() == "ID")
            {
                sb.Append("           AND LMT.LOCATION_MST_PK IN");
                sb.Append("               (SELECT L.LOCATION_MST_PK");
                sb.Append("                  FROM LOCATION_MST_TBL L");
                sb.Append("                 START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("                CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");
            }
            if (PORT_MST_FK > 0)
            {
                sb.Append("           AND PMT.PORT_MST_PK = " + PORT_MST_FK);
            }
            if (FLAG.ToUpper() == "ID")
            {
                sb.Append("         ) QRY WHERE QRY.CUSTOMER_ID IS NOT NULL ORDER BY 1 ");
            }
            else if (FLAG.ToUpper() == "CUST_REF")
            {
                sb.Append("         ) QRY WHERE QRY.CUST_REG_NO IS NOT NULL ORDER BY 1 ");
            }
            else
            {
                sb.Append("         ) QRY WHERE QRY.CUSTOMER_NAME IS NOT NULL ORDER BY 1 ");
            }

            return AutoCompleteString(sb.ToString());
        }

        public string GetFilterTextForConsignee()
        {
            return GetFilterTextForCustomer(0, "CONSIGNEE");
        }

        public string GetFilterTextForCustRef()
        {
            return GetFilterTextForCustomer(0, "CUST_REF");
        }

        public string GetFilterTextForAgent()
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT AG.AGENT_NAME FROM AGENT_MST_TBL AG, LOCATION_MST_TBL L ");
            strqry.Append(" WHERE AG.ACTIVE_FLAG = 1 AND AG.LOCATION_MST_FK = L.LOCATION_MST_PK ");
            strqry.Append(" AND AG.BUSINESS_TYPE in (3, 2) ORDER BY AGENT_ID");
            return AutoCompleteString(strqry.ToString());
        }

        public string GetFilterTextForExecutive()
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT DISTINCT E.EMPLOYEE_NAME FROM EMPLOYEE_MST_TBL E,CUSTOMER_MST_TBL C WHERE E.TERMINATED=1 AND C.REP_EMP_MST_FK = E.EMPLOYEE_MST_PK(+) AND E.BUSINESS_TYPE IN (3,2) AND E.LOCATION_MST_FK= " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ORDER BY 1");
            return AutoCompleteString(strqry.ToString());
        }

        public string GetFilterTextForVslVoy(int POL_MST_FK = 0)
        {
            System.Text.StringBuilder strqry = new System.Text.StringBuilder();
            strqry.Append("SELECT DISTINCT VVT.VESSEL_ID FROM VESSEL_VOYAGE_TBL VVT, VESSEL_VOYAGE_TRN VVTRN ");
            strqry.Append("WHERE VVT.VESSEL_VOYAGE_TBL_PK = VVTRN.VESSEL_VOYAGE_TBL_FK(+) ");
            if (POL_MST_FK > 0)
            {
                strqry.Append(" AND VVTRN.PORT_MST_POL_FK = " + POL_MST_FK);
            }
            return AutoCompleteString(strqry.ToString());
        }

        public string AutoCompleteString(string Query)
        {
            WorkFlow objwf = new WorkFlow();
            DataTable _DataTable = new DataTable();
            _DataTable = objwf.GetDataTable(Query);
            string returnValue = "";
            foreach (DataRow row in _DataTable.Rows)
            {
                if (!string.IsNullOrEmpty(row[0].ToString()))
                    returnValue += "\"" + Convert.ToString(row[0]).Trim() + "\" ,";
            }
            if (!string.IsNullOrEmpty(returnValue))
                returnValue = returnValue.Substring(0, returnValue.Length - 1);
            return returnValue;
        }

        #endregion "AutoComplete"

        public DataSet FetchReferenceType(long bkg_Mst_PK)
        {
            try
            {
                string strSql = "";
                DataSet ds = new DataSet();
                WorkFlow ObjWk = new WorkFlow();
                strSql = " SELECT EBS.BOOKING_MST_PK,EBS.EDI_MST_FK,EBS.REFERENCE_TYPE FROM BOOKING_MST_TBL BS,EDI_BOOKING_MST_TBL EBS WHERE BS.BOOKING_MST_PK=" + bkg_Mst_PK + " ";
                strSql += " AND BS.UPLOADED_VIA_EDI='Y' AND BS.EDI_BOOKING_MST_FK = EBS.BOOKING_MST_PK ";
                ds = ObjWk.GetDataSet(strSql);
                return ds;
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

        public Int16 CheckUpdate_Qbso(Int16 PK, Int16 Type)
        {
            try
            {
                string strSql = null;
                string Check = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow ObjWk = new WorkFlow();
                strBuilder.Append(" SELECT BKG_TRN_SPL_PK");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" BOOKING_TRN_SPL_REQ ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" BOOKING_TRN_FK= " + PK);

                Check = ObjWk.ExecuteScaler(strBuilder.ToString());
                if (!string.IsNullOrEmpty(Check))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
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

        public string UpdateSplReqTransaction(DataSet DS, string UserName, Int16 Req_Type)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            WorkFlow ObjWF = new WorkFlow();
            arrMessage.Clear();
            //Req_Type=1-HAZ,2-Reefer,3-ODC
            try
            {
                ObjWF.OpenConnection();
                selectCommand.Connection = ObjWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = ObjWF.MyUserName + ".BOOKING_MST_PKG.BKG_TRN_SPL_REQ_UPD";

                var _with53 = selectCommand.Parameters;
                _with53.Clear();
                _with53.Add("BKG_TRN_SPL_PK_IN", DS.Tables[0].Rows[0]["BKG_TRN_SPL_PK"]).Direction = ParameterDirection.Input;
                _with53.Add("BOOKING_TRN_FK_IN", DS.Tables[0].Rows[0]["BOOKING_TRN_FK"]).Direction = ParameterDirection.Input;
                _with53.Add("OUTER_PACK_TYPE_MST_FK_IN", DS.Tables[0].Rows[0]["OUTER_PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                _with53.Add("VENTILATION_IN", DS.Tables[0].Rows[0]["VENTILATION"]).Direction = ParameterDirection.Input;
                _with53.Add("LENGTH_IN", DS.Tables[0].Rows[0]["LENGTH"]).Direction = ParameterDirection.Input;
                _with53.Add("INNER_PACK_TYPE_MST_FK_IN", DS.Tables[0].Rows[0]["INNER_PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                _with53.Add("AIR_COOL_METHOD_IN", DS.Tables[0].Rows[0]["AIR_COOL_METHOD"]).Direction = ParameterDirection.Input;
                _with53.Add("LENGTH_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["LENGTH_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with53.Add("HEIGHT_IN", DS.Tables[0].Rows[0]["HEIGHT"]).Direction = ParameterDirection.Input;
                _with53.Add("HUMIDITY_FACTOR_IN", DS.Tables[0].Rows[0]["HUMIDITY_FACTOR"]).Direction = ParameterDirection.Input;
                _with53.Add("MIN_TEMP_IN", DS.Tables[0].Rows[0]["MIN_TEMP"]).Direction = ParameterDirection.Input;
                _with53.Add("HEIGHT_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["HEIGHT_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with53.Add("IS_PERISHABLE_GOODS_IN", DS.Tables[0].Rows[0]["IS_PERISHABLE_GOODS"]).Direction = ParameterDirection.Input;
                _with53.Add("MIN_TEMP_UOM_IN", DS.Tables[0].Rows[0]["MIN_TEMP_UOM"]).Direction = ParameterDirection.Input;
                _with53.Add("MAX_TEMP_IN", DS.Tables[0].Rows[0]["MAX_TEMP"]).Direction = ParameterDirection.Input;
                _with53.Add("WIDTH_IN", DS.Tables[0].Rows[0]["WIDTH"]).Direction = ParameterDirection.Input;
                _with53.Add("MAX_TEMP_UOM_IN", DS.Tables[0].Rows[0]["MAX_TEMP_UOM"]).Direction = ParameterDirection.Input;
                _with53.Add("WIDTH_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["WIDTH_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with53.Add("FLASH_PNT_TEMP_IN", DS.Tables[0].Rows[0]["FLASH_PNT_TEMP"]).Direction = ParameterDirection.Input;
                _with53.Add("WEIGHT_IN", DS.Tables[0].Rows[0]["WEIGHT"]).Direction = ParameterDirection.Input;
                _with53.Add("WEIGHT_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["WEIGHT_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with53.Add("FLASH_PNT_TEMP_UOM_IN", DS.Tables[0].Rows[0]["FLASH_PNT_TEMP_UOM"]).Direction = ParameterDirection.Input;
                _with53.Add("IMDG_CLASS_CODE_IN", DS.Tables[0].Rows[0]["IMDG_CLASS_CODE"]).Direction = ParameterDirection.Input;
                _with53.Add("PACK_TYPE_MST_FK_IN", DS.Tables[0].Rows[0]["PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                _with53.Add("VOLUME_IN", DS.Tables[0].Rows[0]["VOLUME"]).Direction = ParameterDirection.Input;
                _with53.Add("UN_NO_IN", DS.Tables[0].Rows[0]["UN_NO"]).Direction = ParameterDirection.Input;
                _with53.Add("PACK_COUNT_IN", DS.Tables[0].Rows[0]["PACK_COUNT"]).Direction = ParameterDirection.Input;
                _with53.Add("VOLUME_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["VOLUME_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with53.Add("IMO_SURCHARGE_IN", DS.Tables[0].Rows[0]["IMO_SURCHARGE"]).Direction = ParameterDirection.Input;
                _with53.Add("HAULAGE_IN", DS.Tables[0].Rows[0]["HAULAGE"]).Direction = ParameterDirection.Input;
                _with53.Add("SLOT_LOSS_IN", DS.Tables[0].Rows[0]["SLOT_LOSS"]).Direction = ParameterDirection.Input;
                _with53.Add("SURCHARGE_AMT_IN", DS.Tables[0].Rows[0]["SURCHARGE_AMT"]).Direction = ParameterDirection.Input;
                _with53.Add("GENSET_IN", DS.Tables[0].Rows[0]["GENSET"]).Direction = ParameterDirection.Input;
                _with53.Add("LOSS_QUANTITY_IN", DS.Tables[0].Rows[0]["LOSS_QUANTITY"]).Direction = ParameterDirection.Input;
                _with53.Add("IS_MARINE_POLLUTANT_IN", DS.Tables[0].Rows[0]["IS_MARINE_POLLUTANT"]).Direction = ParameterDirection.Input;
                _with53.Add("APPR_REQ_IN", DS.Tables[0].Rows[0]["APPR_REQ"]).Direction = ParameterDirection.Input;
                _with53.Add("CO2_IN", DS.Tables[0].Rows[0]["CO2"]).Direction = ParameterDirection.Input;
                _with53.Add("NO_OF_SLOTS_IN", DS.Tables[0].Rows[0]["NO_OF_SLOTS"]).Direction = ParameterDirection.Input;
                _with53.Add("O2_IN", DS.Tables[0].Rows[0]["O2"]).Direction = ParameterDirection.Input;
                _with53.Add("EMS_NUMBER_IN", DS.Tables[0].Rows[0]["EMS_NUMBER"]).Direction = ParameterDirection.Input;
                _with53.Add("REQ_SET_TEMP_IN", DS.Tables[0].Rows[0]["REQ_SET_TEMP"]).Direction = ParameterDirection.Input;
                _with53.Add("PROPER_SHIPPING_NAME_IN", DS.Tables[0].Rows[0]["PROPER_SHIPPING_NAME"]).Direction = ParameterDirection.Input;
                _with53.Add("PACK_CLASS_TYPE_IN", DS.Tables[0].Rows[0]["PACK_CLASS_TYPE"]).Direction = ParameterDirection.Input;
                _with53.Add("REQ_SET_TEMP_UOM_IN", DS.Tables[0].Rows[0]["REQ_SET_TEMP_UOM"]).Direction = ParameterDirection.Input;
                _with53.Add("STOWAGE_IN", DS.Tables[0].Rows[0]["STOWAGE"]).Direction = ParameterDirection.Input;
                _with53.Add("HANDLING_INSTR_IN", DS.Tables[0].Rows[0]["HANDLING_INSTR"]).Direction = ParameterDirection.Input;
                _with53.Add("DEHUMIDIFIER_IN", DS.Tables[0].Rows[0]["DEHUMIDIFIER"]).Direction = ParameterDirection.Input;
                _with53.Add("FLOORDRAINS_IN", DS.Tables[0].Rows[0]["FLOORDRAINS"]).Direction = ParameterDirection.Input;
                _with53.Add("LASHING_INSTR_IN", DS.Tables[0].Rows[0]["LASHING_INSTR"]).Direction = ParameterDirection.Input;
                _with53.Add("AIR_VENTILATION_IN", DS.Tables[0].Rows[0]["AIR_VENTILATION"]).Direction = ParameterDirection.Input;
                _with53.Add("DEFROSTING_INTERVAL_IN", DS.Tables[0].Rows[0]["DEFROSTING_INTERVAL"]).Direction = ParameterDirection.Input;
                _with53.Add("AIR_VENTILATION_UOM_IN", DS.Tables[0].Rows[0]["AIR_VENTILATION_UOM"]).Direction = ParameterDirection.Input;
                _with53.Add("REF_VENTILATION_IN", DS.Tables[0].Rows[0]["REF_VENTILATION"]).Direction = ParameterDirection.Input;
                _with53.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();

                return "0";
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        public string InsSplReqTransaction(DataSet DS, string UserName, Int16 Req_Type)
        {
            OracleCommand SCD = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            WorkFlow ObjWF = new WorkFlow();
            arrMessage.Clear();
            //Req_Type=1-HAZ,2-Reefer,3-ODC
            try
            {
                ObjWF.OpenConnection();
                selectCommand.Connection = ObjWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = ObjWF.MyUserName + ".BOOKING_MST_PKG.BKG_TRN_SPL_REQ_UPD";

                var _with54 = selectCommand.Parameters;
                _with54.Clear();
                //******************
                _with54.Add("BKG_TRN_SPL_PK_IN", DS.Tables[0].Rows[0]["BKG_TRN_SPL_PK"]).Direction = ParameterDirection.Input;
                _with54.Add("BOOKING_TRN_FK_IN", DS.Tables[0].Rows[0]["BOOKING_TRN_FK"]).Direction = ParameterDirection.Input;
                _with54.Add("OUTER_PACK_TYPE_MST_FK_IN", DS.Tables[0].Rows[0]["OUTER_PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                _with54.Add("VENTILATION_IN", DS.Tables[0].Rows[0]["VENTILATION"]).Direction = ParameterDirection.Input;
                _with54.Add("LENGTH_IN", DS.Tables[0].Rows[0]["LENGTH"]).Direction = ParameterDirection.Input;
                _with54.Add("INNER_PACK_TYPE_MST_FK_IN", DS.Tables[0].Rows[0]["INNER_PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                _with54.Add("AIR_COOL_METHOD_IN", DS.Tables[0].Rows[0]["AIR_COOL_METHOD"]).Direction = ParameterDirection.Input;
                _with54.Add("LENGTH_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["LENGTH_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with54.Add("HEIGHT_IN", DS.Tables[0].Rows[0]["HEIGHT"]).Direction = ParameterDirection.Input;
                _with54.Add("HUMIDITY_FACTOR_IN", DS.Tables[0].Rows[0]["HUMIDITY_FACTOR"]).Direction = ParameterDirection.Input;
                _with54.Add("MIN_TEMP_IN", DS.Tables[0].Rows[0]["MIN_TEMP"]).Direction = ParameterDirection.Input;
                _with54.Add("HEIGHT_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["HEIGHT_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with54.Add("IS_PERISHABLE_GOODS_IN", DS.Tables[0].Rows[0]["IS_PERISHABLE_GOODS"]).Direction = ParameterDirection.Input;
                _with54.Add("MIN_TEMP_UOM_IN", DS.Tables[0].Rows[0]["MIN_TEMP_UOM"]).Direction = ParameterDirection.Input;
                _with54.Add("MAX_TEMP_IN", DS.Tables[0].Rows[0]["MAX_TEMP"]).Direction = ParameterDirection.Input;
                _with54.Add("WIDTH_IN", DS.Tables[0].Rows[0]["WIDTH"]).Direction = ParameterDirection.Input;
                _with54.Add("MAX_TEMP_UOM_IN", DS.Tables[0].Rows[0]["MAX_TEMP_UOM"]).Direction = ParameterDirection.Input;
                _with54.Add("WIDTH_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["WIDTH_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with54.Add("FLASH_PNT_TEMP_IN", DS.Tables[0].Rows[0]["FLASH_PNT_TEMP"]).Direction = ParameterDirection.Input;
                _with54.Add("WEIGHT_IN", DS.Tables[0].Rows[0]["WEIGHT"]).Direction = ParameterDirection.Input;
                _with54.Add("WEIGHT_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["WEIGHT_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with54.Add("FLASH_PNT_TEMP_UOM_IN", DS.Tables[0].Rows[0]["FLASH_PNT_TEMP_UOM"]).Direction = ParameterDirection.Input;
                _with54.Add("IMDG_CLASS_CODE_IN", DS.Tables[0].Rows[0]["IMDG_CLASS_CODE"]).Direction = ParameterDirection.Input;
                _with54.Add("PACK_TYPE_MST_FK_IN", DS.Tables[0].Rows[0]["PACK_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                _with54.Add("VOLUME_IN", DS.Tables[0].Rows[0]["VOLUME"]).Direction = ParameterDirection.Input;
                _with54.Add("UN_NO_IN", DS.Tables[0].Rows[0]["UN_NO"]).Direction = ParameterDirection.Input;
                _with54.Add("PACK_COUNT_IN", DS.Tables[0].Rows[0]["PACK_COUNT"]).Direction = ParameterDirection.Input;
                _with54.Add("VOLUME_UOM_MST_FK_IN", DS.Tables[0].Rows[0]["VOLUME_UOM_MST_FK"]).Direction = ParameterDirection.Input;
                _with54.Add("IMO_SURCHARGE_IN", DS.Tables[0].Rows[0]["IMO_SURCHARGE"]).Direction = ParameterDirection.Input;
                _with54.Add("HAULAGE_IN", DS.Tables[0].Rows[0]["HAULAGE"]).Direction = ParameterDirection.Input;
                _with54.Add("SLOT_LOSS_IN", DS.Tables[0].Rows[0]["SLOT_LOSS"]).Direction = ParameterDirection.Input;
                _with54.Add("SURCHARGE_AMT_IN", DS.Tables[0].Rows[0]["SURCHARGE_AMT"]).Direction = ParameterDirection.Input;
                _with54.Add("GENSET_IN", DS.Tables[0].Rows[0]["GENSET"]).Direction = ParameterDirection.Input;
                _with54.Add("LOSS_QUANTITY_IN", DS.Tables[0].Rows[0]["LOSS_QUANTITY"]).Direction = ParameterDirection.Input;
                _with54.Add("IS_MARINE_POLLUTANT_IN", DS.Tables[0].Rows[0]["IS_MARINE_POLLUTANT"]).Direction = ParameterDirection.Input;
                _with54.Add("APPR_REQ_IN", DS.Tables[0].Rows[0]["APPR_REQ"]).Direction = ParameterDirection.Input;
                _with54.Add("CO2_IN", DS.Tables[0].Rows[0]["CO2"]).Direction = ParameterDirection.Input;
                _with54.Add("NO_OF_SLOTS_IN", DS.Tables[0].Rows[0]["NO_OF_SLOTS"]).Direction = ParameterDirection.Input;
                _with54.Add("O2_IN", DS.Tables[0].Rows[0]["O2"]).Direction = ParameterDirection.Input;
                _with54.Add("EMS_NUMBER_IN", DS.Tables[0].Rows[0]["EMS_NUMBER"]).Direction = ParameterDirection.Input;
                _with54.Add("REQ_SET_TEMP_IN", DS.Tables[0].Rows[0]["REQ_SET_TEMP"]).Direction = ParameterDirection.Input;
                _with54.Add("PROPER_SHIPPING_NAME_IN", DS.Tables[0].Rows[0]["PROPER_SHIPPING_NAME"]).Direction = ParameterDirection.Input;
                _with54.Add("PACK_CLASS_TYPE_IN", DS.Tables[0].Rows[0]["PACK_CLASS_TYPE"]).Direction = ParameterDirection.Input;
                _with54.Add("REQ_SET_TEMP_UOM_IN", DS.Tables[0].Rows[0]["REQ_SET_TEMP_UOM"]).Direction = ParameterDirection.Input;
                _with54.Add("STOWAGE_IN", DS.Tables[0].Rows[0]["STOWAGE"]).Direction = ParameterDirection.Input;
                _with54.Add("HANDLING_INSTR_IN", DS.Tables[0].Rows[0]["HANDLING_INSTR"]).Direction = ParameterDirection.Input;
                _with54.Add("DEHUMIDIFIER_IN", DS.Tables[0].Rows[0]["DEHUMIDIFIER"]).Direction = ParameterDirection.Input;
                _with54.Add("FLOORDRAINS_IN", DS.Tables[0].Rows[0]["FLOORDRAINS"]).Direction = ParameterDirection.Input;
                _with54.Add("LASHING_INSTR_IN", DS.Tables[0].Rows[0]["LASHING_INSTR"]).Direction = ParameterDirection.Input;
                _with54.Add("AIR_VENTILATION_IN", DS.Tables[0].Rows[0]["AIR_VENTILATION"]).Direction = ParameterDirection.Input;
                _with54.Add("DEFROSTING_INTERVAL_IN", DS.Tables[0].Rows[0]["DEFROSTING_INTERVAL"]).Direction = ParameterDirection.Input;
                _with54.Add("AIR_VENTILATION_UOM_IN", DS.Tables[0].Rows[0]["AIR_VENTILATION_UOM"]).Direction = ParameterDirection.Input;
                _with54.Add("REF_VENTILATION_IN", DS.Tables[0].Rows[0]["REF_VENTILATION"]).Direction = ParameterDirection.Input;
                _with54.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.ExecuteNonQuery();

                return "0";
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        public DataSet FetchBookingDetails(int SeaBkgPK)
        {
            string strSql = null;
            WorkFlow Objwk = new WorkFlow();
            strSql = "SELECT JSE.JOB_CARD_TRN_PK JOBPK, ";
            strSql += "JSE.JOBCARD_REF_NO JOBREFNO, ";
            strSql += "BST.BOOKING_MST_PK BKGPK, ";
            strSql += "BST.BOOKING_REF_NO BKGREFNO, ";
            strSql += "BST.BOOKING_DATE BKGDATE, ";
            strSql += "(CASE WHEN BST.VOYAGE_FLIGHT_NO IS NOT NULL THEN ";
            strSql += "BST.VESSEL_NAME ||'-' || BST.VOYAGE_FLIGHT_NO ";
            strSql += "ELSE";
            strSql += "BST.VESSEL_NAME END ) VESFLIGHT,";
            strSql += "HBL.HBL_REF_NO HBLREFNO,";
            strSql += " MBL.MBL_REF_NO  MBLREFNO,";
            strSql += " JSE.MARKS_NUMBERS MARKS,";

            strSql += " JSE.GOODS_DESCRIPTION GOODS,";
            strSql += "BST.CARGO_TYPE,";
            strSql += "BST.REMARKS_NEW UCRN0,";
            strSql += "'' CLEARANCEPOINT,";
            strSql += " BST.ETD_DATE ETD,";
            strSql += "BST.CUST_CUSTOMER_MST_FK,";
            strSql += "CMST.CUSTOMER_NAME SHIPNAME,";
            strSql += "CMST.CUST_REG_NO SHIPREFNO,";
            strSql += "CDTLS.ADM_ADDRESS_1 SHIPADD1,";
            strSql += "CDTLS.ADM_ADDRESS_2 SHIPADD2,";
            strSql += "CDTLS.ADM_ADDRESS_3 SHIPADD3,";
            strSql += "CDTLS.ADM_CITY SHIPCITY,";
            strSql += "CDTLS.ADM_ZIP_CODE SHIPZIP,";
            strSql += "CDTLS.ADM_EMAIL_ID AS SHIPEMAIL,";
            strSql += "CDTLS.ADM_PHONE_NO_1 SHIPPHONE,";
            strSql += "CDTLS.ADM_FAX_NO SHIPFAX,";
            strSql += " SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            strSql += " CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGADD1,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGADD2,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGADD3,";
            strSql += "CONSIGNEEDTLS.ADM_CITY CONSIGCITY,";
            strSql += "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGZIP,";
            strSql += "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGEMAIL,";
            strSql += "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
            strSql += "CONSIGNEEDTLS.ADM_FAX_NO CONSIGFAX,";
            strSql += " CONSIGCOUNTRY.COUNTRY_NAME CONSICOUNTRY,";
            strSql += " POL.PORT_NAME POLNAME,";
            strSql += " POD.PORT_NAME PODNAME,";
            strSql += " CASE WHEN  PLD.PLACE_NAME IS NOT  NULL  THEN  PLD.PLACE_NAME  ELSE  POD.PORT_NAME  END DELNAME,";
            strSql += " COLPLD.PLACE_NAME COLNAME,";

            strSql += " DBAMST.AGENT_MST_PK DBAGENTPK,";
            strSql += " DBAMST.AGENT_NAME  DBAGENTNAME,";
            strSql += " DBADTLS.ADM_ADDRESS_1  DBAGENTADD1,";
            strSql += " DBADTLS.ADM_ADDRESS_2  DBAGENTADD2,";
            strSql += " DBADTLS.ADM_ADDRESS_3  DBAGENTADD3,";
            strSql += " DBADTLS.ADM_CITY  DBAGENTCITY,";
            strSql += " DBADTLS.ADM_ZIP_CODE DBAGENTZIP,";
            strSql += " DBADTLS.ADM_EMAIL_ID DBAGENTEMAIL,";
            strSql += " DBADTLS.ADM_PHONE_NO_1 DBAGENTPHONE,";
            strSql += " DBADTLS.ADM_FAX_NO DBAGENTFAX,";
            strSql += " DBCOUNTRY.COUNTRY_NAME DBCOUNTRY,";
            strSql += "STMST.INCO_CODE TERMS,";
            strSql += " NVL(JSE.INSURANCE_AMT,0) INSURANCE,";
            strSql += " BST.PYMT_TYPE ,";
            strSql += " CGMST.commodity_group_desc COMMCODE,";
            strSql += " BST.ETA_DATE ETA,";
            strSql += " BST.GROSS_WEIGHT GROSS,";
            strSql += " BST.CHARGEABLE_WEIGHT CHARWT,";
            strSql += " BST.NET_WEIGHT NETWT,";
            strSql += " BST.VOLUME_IN_CBM VOLUME";

            strSql += "FROM   JOB_CARD_TRN JSE,";
            strSql += " BOOKING_MST_TBL BST,";
            strSql += " CUSTOMER_MST_TBL CMST,";
            strSql += " CUSTOMER_MST_TBL CONSIGNEE,";
            strSql += " CUSTOMER_CONTACT_DTLS CDTLS,";
            strSql += " CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
            strSql += " COUNTRY_MST_TBL SHIPCOUNTRY,";
            strSql += " COUNTRY_MST_TBL CONSIGCOUNTRY,";
            strSql += " PORT_MST_TBL POL,";
            strSql += " PORT_MST_TBL POD,";
            strSql += " PLACE_MST_TBL PLD,";
            strSql += " PLACE_MST_TBL COLPLD,";
            strSql += "AGENT_MST_TBL DBAMST,";
            strSql += "AGENT_CONTACT_DTLS DBADTLS,";
            strSql += "COUNTRY_MST_TBL DBCOUNTRY,";
            strSql += "SHIPPING_TERMS_MST_TBL STMST,";
            strSql += " COMMODITY_GROUP_MST_TBL CGMST,";
            strSql += "HBL_EXP_TBL HBL,";
            strSql += "MBL_EXP_TBL MBL";

            strSql += "WHERE BST.BOOKING_MST_PK IN ('" + SeaBkgPK + "')";
            strSql += "AND JSE.HBL_HAWB_FK=HBL.HBL_EXP_TBL_PK(+)";
            strSql += "AND JSE.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK(+)";
            strSql += "AND   CMST.CUSTOMER_MST_PK(+)=BST.CUST_CUSTOMER_MST_FK";
            strSql += "AND   CONSIGNEE.CUSTOMER_MST_PK(+)=BST.CONS_CUSTOMER_MST_FK";
            strSql += "AND   CDTLS.CUSTOMER_MST_FK(+)=CMST.CUSTOMER_MST_PK";
            strSql += "AND CONSIGNEE.CUSTOMER_MST_PK=CONSIGNEEDTLS.CUSTOMER_MST_FK(+)";
            strSql += " AND SHIPCOUNTRY.COUNTRY_MST_PK(+)=CDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CONSIGNEEDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND   JSE.BOOKING_MST_FK(+)=BST.BOOKING_MST_PK";
            strSql += " AND   BST.PORT_MST_POL_FK=POL.PORT_MST_PK(+)";
            strSql += " AND   BST.PORT_MST_POD_FK=POD.PORT_MST_PK(+)";
            strSql += " AND   BST.DEL_PLACE_MST_FK=PLD.PLACE_PK(+)";
            strSql += " AND   BST.COL_PLACE_MST_FK=COLPLD.PLACE_PK(+)";
            strSql += " AND   BST.DP_AGENT_MST_FK=DBAMST.AGENT_MST_PK(+)";
            strSql += " AND   DBAMST.AGENT_MST_PK=DBADTLS.AGENT_MST_FK(+)";
            strSql += "AND DBCOUNTRY.COUNTRY_MST_PK(+)=DBADTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND  STMST.SHIPPING_TERMS_MST_PK(+)=BST.SHIPPING_TERMS_MST_FK";
            strSql += " AND  BST.COMMODITY_GROUP_FK=CGMST.COMMODITY_GROUP_PK(+)";
            try
            {
                return Objwk.GetDataSet(strSql);
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

        public DataSet FetchSeaContainers(string BkgPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT BST.BOOKING_MST_PK BKGPK, JSE.CONTAINER_NUMBER CONTAINER";
            Strsql += "FROM JOB_TRN_CONT JSE,BOOKING_MST_TBL BST,JOB_CARD_TRN JS";
            Strsql += "WHERE BST.BOOKING_MST_PK = JS.BOOKING_MST_FK";
            Strsql += "AND JSE.JOB_CARD_TRN_FK=JS.JOB_CARD_TRN_PK";
            Strsql += " AND BST.BOOKING_MST_PK IN (" + BkgPK + ")";
            try
            {
                return Objwk.GetDataSet(Strsql);
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

        #region "Fetch for THree table"

        public DataSet Fetch_Surcharge(DataSet dsGrid, int FREIGHT_ELEMENT_MST_PK = 0, string MAIN_TABLE = "", int MAIN_TABLE_PK_VAL = 0, string MAIN_TABLE_PK_COL = "", string TRN_TABLE = "", string TRN_TABLE_PK_COL = "", string TRN_MAIN_FK_COL = "", string SURCHARGE_TABLE = "", string SURCHARGE_TRN_FK_COL = "",
        string SURCHARGE_TRN__FREIGHT_FK_COL = "", int POL_MST_FK = 0, int POD_MST_FK = 0)
        {
            int Rcnt = 0;
            int Rcnt1 = 0;
            int RowCnt = 0;
            DataSet Dssurcharge = null;
            //'for more than 3-table

            try
            {
                if (MAIN_TABLE == "CONT_MAIN_SEA_TBL")
                {
                    for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE")))
                        {
                            dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                        }
                        Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POLPK"]),
                        Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PODPK"]));
                        for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++)
                        {
                            for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                            {
                                if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]) & dsGrid.Tables[1].Rows[Rcnt]["POL"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["POD"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"])
                                {
                                    dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                                }
                            }
                        }
                    }
                    if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE")))
                    {
                        dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                    }
                }
                else if (MAIN_TABLE == "cont_cust_sea_tbl" | MAIN_TABLE == "SRR_SEA_TBL")
                {
                    for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE")))
                        {
                            dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                        }
                        Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POLPK"]),
                       Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["PODPK"]));
                        for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++)
                        {
                            for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                            {
                                if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]) & dsGrid.Tables[1].Rows[Rcnt]["POLPK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["PODPK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"])
                                {
                                    dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                                }
                            }
                        }
                    }
                    if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE")))
                    {
                        dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                    }
                }
                else if (MAIN_TABLE == "QUOTATION_MST_TBL")
                {
                    for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE")))
                        {
                            dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                        }
                        if (POL_MST_FK > 0 & POD_MST_FK > 0)
                        {
                            Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, POL_MST_FK,
                            POD_MST_FK);
                        }
                        else
                        {
                            Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POL_PK"]),
                            Convert.ToInt32(dsGrid.Tables[0].Rows[RowCnt]["POD_PK"]));
                        }

                        for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++)
                        {
                            for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                            {
                                if (POL_MST_FK > 0 & POD_MST_FK > 0)
                                {
                                    if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]))
                                    {
                                        dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                                    }
                                }
                                else
                                {
                                    if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]) & dsGrid.Tables[1].Rows[Rcnt]["POL_PK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["POD_PK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"])
                                    {
                                        dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                                    }
                                }
                            }
                        }
                    }
                    if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE")))
                    {
                        dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                    }
                }
                else if (MAIN_TABLE == "BOOKING_MST_TBL")
                {
                    for (RowCnt = 0; RowCnt <= dsGrid.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE")))
                        {
                            dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                        }
                        Dssurcharge = Fetch_surcharge_forThreetable(MAIN_TABLE, MAIN_TABLE_PK_VAL, MAIN_TABLE_PK_COL, TRN_TABLE, TRN_TABLE_PK_COL, TRN_MAIN_FK_COL, SURCHARGE_TABLE, SURCHARGE_TRN_FK_COL, SURCHARGE_TRN__FREIGHT_FK_COL, POL_MST_FK,
                        POD_MST_FK);
                        for (Rcnt = 0; Rcnt <= dsGrid.Tables[1].Rows.Count - 1; Rcnt++)
                        {
                            for (Rcnt1 = 0; Rcnt1 <= Dssurcharge.Tables[0].Rows.Count - 1; Rcnt1++)
                            {
                                if ((dsGrid.Tables[1].Rows[Rcnt][FREIGHT_ELEMENT_MST_PK] == Dssurcharge.Tables[0].Rows[Rcnt1]["FREIGHT_ELEMENT_MST_PK"]) & dsGrid.Tables[1].Rows[Rcnt]["PORT_MST_PK"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POL_PK"] & dsGrid.Tables[1].Rows[Rcnt]["PORT_MST_PK1"] == Dssurcharge.Tables[0].Rows[Rcnt1]["POD_PK"])
                                {
                                    dsGrid.Tables[1].Rows[Rcnt]["SURCHARGE"] = Dssurcharge.Tables[0].Rows[Rcnt1]["SURCHARGE"];
                                }
                            }
                        }
                    }
                    if (!(dsGrid.Tables[1].Columns.Contains("SURCHARGE")))
                    {
                        dsGrid.Tables[1].Columns.Add(new DataColumn("SURCHARGE", typeof(string)));
                    }
                }
                return (dsGrid);
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

        #endregion "Fetch for THree table"

        #region "fetch thre table"

        public DataSet Fetch_surcharge_forThreetable(string MAIN_TABLE = "", int MAIN_TABLE_PK_VAL = 0, string MAIN_TABLE_PK_COL = "", string TRN_TABLE = "", string TRN_TABLE_PK_COL = "", string TRN_MAIN_FK_COL = "", string SURCHARGE_TABLE = "", string SURCHARGE_TRN_FK_COL = "", string SURCHARGE_TRN__FREIGHT_FK_COL = "", int POL_PK = 0,
        int POD_PK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = null;
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".FETCH_SURCHARGE.FETCH_SURCHARGE_DATA_PKG";
                var _with55 = selectCommand.Parameters;
                _with55.Clear();

                _with55.Add("MAIN_TABLE", MAIN_TABLE).Direction = ParameterDirection.Input;
                _with55.Add("MAIN_TABLE_PK_VAL", MAIN_TABLE_PK_VAL).Direction = ParameterDirection.Input;
                _with55.Add("MAIN_TABLE_PK_COL", MAIN_TABLE_PK_COL).Direction = ParameterDirection.Input;

                _with55.Add("TRN_TABLE", TRN_TABLE).Direction = ParameterDirection.Input;
                _with55.Add("TRN_TABLE_PK_COL", TRN_TABLE_PK_COL).Direction = ParameterDirection.Input;
                _with55.Add("TRN_MAIN_FK_COL", TRN_MAIN_FK_COL).Direction = ParameterDirection.Input;

                _with55.Add("SURCHARGE_TABLE", SURCHARGE_TABLE).Direction = ParameterDirection.Input;
                _with55.Add("SURCHARGE_TRN_FK_COL", SURCHARGE_TRN_FK_COL).Direction = ParameterDirection.Input;
                _with55.Add("SURCHARGE_TRN__FREIGHT_FK_COL", SURCHARGE_TRN__FREIGHT_FK_COL).Direction = ParameterDirection.Input;

                _with55.Add("POL_PK_IN", POL_PK).Direction = ParameterDirection.Input;
                _with55.Add("POD_PK_IN", POD_PK).Direction = ParameterDirection.Input;

                _with55.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value).Trim();
                return (objWF.GetDataSet(strReturn));
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
                selectCommand.Connection.Close();
            }
        }

        #endregion "fetch thre table"

        public void CLEAR_TEMP_DATA()
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                var _with56 = objWK.MyCommand;
                _with56.Parameters.Clear();
                _with56.Parameters.Add("USER_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with56.CommandType = CommandType.StoredProcedure;
                _with56.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.CLEAR_TEMP_DATA";
                _with56.ExecuteNonQuery();
                TRAN.Commit();
            }
            catch (OracleException oraEx)
            {
                TRAN.Rollback();
                throw oraEx;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
        }

        //----------------------------------------------------------------------------------------------------------

        #region "Get ContainerID"

        public DataSet GetContainerID()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT CTMT.CONTAINER_TYPE_MST_PK, CTMT.CONTAINER_TYPE_MST_ID");
            sb.Append("  FROM CONTAINER_TYPE_MST_TBL CTMT");
            sb.Append(" WHERE CTMT.ACTIVE_FLAG = 1");
            sb.Append(" ORDER BY CTMT.PREFERENCES");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Get ContainerID"

        #region "Getting Grid Details"

        public DataSet FetchGridDetails(string Cargotype = "", string bizType = "", string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string BookingPK = "", string ShipmentDate = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 flag = 0)
        {
            string strCondition = null;
            try
            {
                if (flag == 0)
                {
                    strCondition = strCondition + "  AND 1=2";
                }
                if (Convert.ToInt32(lblCarrierPK) > 0)
                {
                    strCondition = strCondition + " AND BST.OPERATOR_MST_FK=" + lblCarrierPK;
                }
                if (Convert.ToInt32(POLPK) > 0)
                {
                    strCondition = strCondition + "  AND POL.PORT_MST_PK= " + POLPK;
                }
                if (Convert.ToInt32(PODPK) > 0)
                {
                    strCondition = strCondition + " AND POD.PORT_MST_PK = " + PODPK;
                }
                if (Convert.ToInt32(bizType) == 2)
                {
                    if (Convert.ToInt32(Cargotype) == 1)
                    {
                        GetGridBookingSeaQueryFCL(strCondition);
                    }
                    else if (Convert.ToInt32(Cargotype) == 2)
                    {
                        GetGridBookingSeaQueryLCL(strCondition);
                    }
                    else
                    {
                        GetGridBookingSeaQueryBBC(strCondition);
                    }
                }
                else
                {
                    GetGridBookingSeaQueryAIR(strCondition);
                }
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
            return new DataSet();
        }

        public DataSet GetGridBookingSeaQueryFCL(string bkgRefNr = "", string Cargotype = "", string bizType = "", string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string BookingPK = "", string ShipmentDate = "",
        Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, Int16 intXBkg = 0, Int16 intCLAgt = 0, string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            DataSet MainDS = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();
            string strCondition = null;
            try
            {
                if (flag == 0)
                {
                    strCondition = strCondition + "  AND 1=2";
                }
                if (Convert.ToInt32(lblCarrierPK) > 0)
                {
                    strCondition = strCondition + " AND BST.CARRIER_MST_FK=" + lblCarrierPK;
                }
                if (Convert.ToInt32(POLPK) > 0)
                {
                    strCondition = strCondition + "  AND BST.PORT_MST_POL_FK = " + POLPK;
                }
                if (Convert.ToInt32(PODPK) > 0)
                {
                    strCondition = strCondition + " AND BST.PORT_MST_POD_FK = " + PODPK;
                }
                if (Convert.ToInt32(CustomerPK) > 0)
                {
                    strCondition = strCondition + " AND BST.CUST_CUSTOMER_MST_FK = " + CustomerPK;
                }
                if (Convert.ToInt32(VslVoyPK) > 0)
                {
                    strCondition = strCondition + " AND BST.VESSEL_VOYAGE_FK =  " + VslVoyPK;
                }
                if (!string.IsNullOrEmpty(ShipmentDate))
                {
                    strCondition = strCondition + "  AND BST.SHIPMENT_DATE = TO_DATE('" + ShipmentDate + "','" + dateFormat + "')   ";
                }
                if (intXBkg == 1)
                {
                    strCondition = strCondition + " And BST.CB_AGENT_MST_FK IS NOT NULL";
                }
                if (intCLAgt == 1)
                {
                    strCondition = strCondition + " And BST.Cl_Agent_Mst_Fk IS NOT NULL";
                }
                if (Convert.ToInt32(CommGroupfk) > 0)
                {
                    strCondition = strCondition + " AND BST.COMMODITY_GROUP_FK =  " + CommGroupfk;
                }
                if (!string.IsNullOrEmpty(bkgRefNr))
                {
                    strCondition += " AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (!string.IsNullOrEmpty(SpaceReqNr))
                {
                    strCondition += " AND UPPER(BST.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (SrStatus != "0")
                {
                    strCondition += "   AND BST.SR_STATUS=" + SrStatus;
                }
                sb.Append("SELECT '' SLNR,");
                sb.Append("       Q.BOOKINGPK,");
                sb.Append("       Q.BOOKINGNR,");
                sb.Append("       Q.BOOKINGDATE,");
                sb.Append("       Q.CUSTOMERPK,");
                sb.Append("       Q.CUSTOMERNAME,");
                sb.Append("       Q.POLPK,");
                sb.Append("       Q.POLID,");
                sb.Append("       Q.PODPK,");
                sb.Append("       Q.PODID,");
                sb.Append("       Q.SHIPMENTDATE,");
                sb.Append("       Q.COMMGRPPK,");
                sb.Append("       Q.COMMODITYGRUOP,");
                sb.Append("       MAX(Q.BOOKINGTYPE) BOOKINGTYPE,");
                sb.Append("       SUM(Q.TWENTYS),");
                sb.Append("       SUM(Q.FOURTYS),");
                sb.Append("       SUM(Q.TEUS),");
                sb.Append("       '' CTRTYPE,");
                sb.Append("       0 QTY,");
                sb.Append("       0 GROSSWT,");
                sb.Append("       0 VOLUME,");
                sb.Append("       Q.SRNR,");
                sb.Append("       Q.SRSTATUSPK,");
                sb.Append("       Q.SRSTATUS,");
                sb.Append("       '' SEL,Q.SEND_REQ_FLAG");
                sb.Append("  FROM");
                sb.Append("  (SELECT ''SLNR,BST.BOOKING_MST_PK BOOKINGPK,");
                sb.Append("               BST.BOOKING_REF_NO BOOKINGNR,");
                sb.Append("               BST.BOOKING_DATE BOOKINGDATE,");
                sb.Append("               CMT.CUSTOMER_MST_PK CUSTOMERPK,");
                sb.Append("               CMT.CUSTOMER_NAME CUSTOMERNAME,");
                sb.Append("               POL.PORT_MST_PK POLPK,");
                sb.Append("               POL.PORT_ID POLID,");
                sb.Append("               POD.PORT_MST_PK PODPK,");
                sb.Append("               POD.PORT_ID PODID,");
                sb.Append("               BST.SHIPMENT_DATE SHIPMENTDATE,");
                sb.Append("               CGMT.COMMODITY_GROUP_PK COMMGRPPK,");
                sb.Append("               CGMT.COMMODITY_GROUP_CODE COMMODITYGRUOP,");
                sb.Append("               DECODE(BTSFL.TRANS_REFERED_FROM,");
                sb.Append("                      1,");
                sb.Append("                      'Quotation',");
                sb.Append("                      2,");
                sb.Append("                      'Spot Rate',");
                sb.Append("                      3,");
                sb.Append("                      'Customer Contract',");
                sb.Append("                      4,");
                sb.Append("                      'SL Tariff',");
                sb.Append("                      5,");
                sb.Append("                      'SRR',");
                sb.Append("                      6,");
                sb.Append("                      'General Tariff',");
                sb.Append("                      7,");
                sb.Append("                      'Manual') BOOKINGTYPE,");
                sb.Append("               CASE");
                sb.Append("                 WHEN SUBSTR(CTMT.CONTAINER_TYPE_MST_ID, 0, 2) = '20' THEN");
                sb.Append("                  NVL(BTSFL.NO_OF_BOXES, 0)");
                sb.Append("                 ELSE");
                sb.Append("                  0");
                sb.Append("               END AS \"TWENTYS\",");
                sb.Append("               CASE");
                sb.Append("                 WHEN SUBSTR(CTMT.CONTAINER_TYPE_MST_ID, 0, 2) = '40' THEN");
                sb.Append("                  NVL(BTSFL.NO_OF_BOXES, 0)");
                sb.Append("                 ELSE");
                sb.Append("                  0");
                sb.Append("               END AS \"FOURTYS\",");
                sb.Append("               NVL(BTSFL.NO_OF_BOXES, 0) * NVL(CTMT.TEU_FACTOR, 0) TEUS,");
                sb.Append("               '' CTRTYPE,");
                sb.Append("               0 QTY,");
                sb.Append("               0 GROSSWT,");
                sb.Append("               0 VOLUME,");
                sb.Append("               BST.SPACE_REQUEST_NR SRNR,");
                sb.Append("               BST.SPACE_REQUEST_PK SRSTATUSPK,");
                sb.Append("               DECODE(BST.SR_STATUS,");
                sb.Append("                      1,");
                sb.Append("                      'Requested',");
                sb.Append("                      2,");
                sb.Append("                      'Confirmed',");
                sb.Append("                      3,");
                sb.Append("                      'Rejected',");
                sb.Append("                      4,");
                sb.Append("                      'Cancelled') SRSTATUS,");
                sb.Append("               '' SEL,BST.SEND_REQ_FLAG");
                sb.Append("          FROM BOOKING_MST_TBL         BST,");
                sb.Append("               USER_MST_TBL            UMT,");
                sb.Append("               CUSTOMER_MST_TBL        CMT,");
                sb.Append("               PORT_MST_TBL            POL,");
                sb.Append("               PORT_MST_TBL            POD,");
                sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("               BOOKING_TRN BTSFL,");
                sb.Append("               CONTAINER_TYPE_MST_TBL  CTMT");
                sb.Append("         WHERE ");
                sb.Append("           CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                sb.Append("           AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("           AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("           AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK");
                sb.Append("           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
                sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK = BTSFL.CONTAINER_TYPE_MST_FK");
                sb.Append("           AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                //sb.Append("           AND POL.LOCATION_MST_FK=" & Session("LOGED_IN_LOC_FK"))
                sb.Append("           AND POL.LOCATION_MST_FK IN ");
                sb.Append("          (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L ");
                sb.Append("                       START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("                        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ");
                sb.Append("           AND BST.CARGO_TYPE = 1");
                sb.Append(" " + strCondition + "");
                sb.Append(" )Q");
                sb.Append("  GROUP BY Q.BOOKINGPK,");
                sb.Append("          Q.BOOKINGNR,");
                sb.Append("          Q.BOOKINGDATE,");
                sb.Append("          Q.CUSTOMERPK,");
                sb.Append("          Q.CUSTOMERNAME,");
                sb.Append("          Q.POLPK,");
                sb.Append("          Q.POLID,");
                sb.Append("          Q.PODPK,");
                sb.Append("          Q.PODID,");
                sb.Append("          Q.SHIPMENTDATE,");
                sb.Append("          Q.COMMGRPPK,");
                sb.Append("          Q.COMMODITYGRUOP,");
                sb.Append("          Q.SRNR,");
                sb.Append("          Q.SRSTATUSPK,");
                sb.Append("          Q.SRSTATUS,Q.SEND_REQ_FLAG");
                sb.Append("          ORDER BY Q.BOOKINGDATE DESC ");
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "PARENT");

                sb.Remove(0, sb.Length);
                sb.Append(" SELECT BST.BOOKING_MST_PK,");
                sb.Append("                CTMT.CONTAINER_TYPE_MST_PK,");
                sb.Append("                CTMT.CONTAINER_TYPE_MST_ID COMMODITYID,");
                sb.Append("                ''COMMODITYNAME,");
                sb.Append("                ''BASIS,");

                sb.Append("CASE");
                sb.Append("         WHEN BST.CARGO_TYPE = 1 THEN");
                sb.Append("          CASE WHEN (SELECT SUM(NVL(BCD.PACK_COUNT, 0))");
                sb.Append("             FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD");
                sb.Append("            WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK");
                sb.Append("              AND BTCD.BOOKING_TRN_FK(+) = BTSFL.BOOKING_TRN_PK)>0 THEN");
                sb.Append("              (SELECT SUM(NVL(BCD.PACK_COUNT, 0))");
                sb.Append("             FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD");
                sb.Append("            WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK");
                sb.Append("              AND BTCD.BOOKING_TRN_FK(+) = BTSFL.BOOKING_TRN_PK)");
                sb.Append("           ELSE");
                sb.Append("             NVL(BTSFL.QUANTITY, 0)");
                sb.Append("           END ");
                sb.Append("         ELSE");
                sb.Append("          NVL(BTSFL.QUANTITY, 0)");
                sb.Append("       END QTY,");
                sb.Append("       CASE");
                sb.Append("         WHEN BST.CARGO_TYPE = 1 THEN");
                sb.Append("          (SELECT SUM(NVL(BTCD.GROSS_WEIGHT, 0))");
                sb.Append("             FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD");
                sb.Append("            WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK");
                sb.Append("              AND BTCD.BOOKING_TRN_FK(+) = BTSFL.BOOKING_TRN_PK)");
                sb.Append("         ELSE");
                sb.Append("          NVL(BST.GROSS_WEIGHT, 0)");
                sb.Append("       END GRSWEIGHT,");
                sb.Append("       CASE");
                sb.Append("         WHEN BST.CARGO_TYPE = 1 THEN");
                sb.Append("          (SELECT SUM(NVL(BCD.NET_WEIGHT, 0))");
                sb.Append("             FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD");
                sb.Append("            WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK");
                sb.Append("              AND BTCD.BOOKING_TRN_FK(+) = BTSFL.BOOKING_TRN_PK)");
                sb.Append("         ELSE");
                sb.Append("          NVL(BST.NET_WEIGHT, 0)");
                sb.Append("       END NETWEIGHT,");
                sb.Append("       CASE");
                sb.Append("         WHEN BST.CARGO_TYPE = 1 THEN");
                sb.Append("          CASE WHEN (SELECT SUM(NVL(BCD.VOLUME_IN_CBM, 0))");
                sb.Append("             FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD");
                sb.Append("            WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK");
                sb.Append("              AND BTCD.BOOKING_TRN_FK(+) = BTSFL.BOOKING_TRN_PK)>0 THEN");
                sb.Append("              (SELECT SUM(NVL(BCD.VOLUME_IN_CBM, 0))");
                sb.Append("             FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD");
                sb.Append("            WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK");
                sb.Append("              AND BTCD.BOOKING_TRN_FK(+) = BTSFL.BOOKING_TRN_PK)");
                sb.Append("            ELSE");
                sb.Append("              NVL(BTSFL.VOLUME_CBM, 0)");
                sb.Append("            END");
                sb.Append("         ELSE");
                sb.Append("              NVL(BTSFL.VOLUME_CBM, 0)");
                sb.Append("       END VOLUME");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       BOOKING_TRN BTSFL,");
                sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
                sb.Append("       PORT_MST_TBL            POL, ");
                sb.Append("       USER_MST_TBL            UMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
                sb.Append("   AND BTSFL.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK");
                sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("           AND POL.LOCATION_MST_FK IN ");
                sb.Append("          (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L ");
                sb.Append("                       START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("                        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ");
                sb.Append("   AND BST.CARGO_TYPE = 1");
                sb.Append(" " + strCondition + "");
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "CHILD");

                DataRelation relBooking_Details = new DataRelation("BOOKING", new DataColumn[] { MainDS.Tables[0].Columns["BOOKINGPK"] }, new DataColumn[] { MainDS.Tables[1].Columns["BOOKING_MST_PK"] });
                relBooking_Details.Nested = true;
                MainDS.Relations.Add(relBooking_Details);
                return MainDS;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }

        public DataSet GetGridBookingSeaQueryLCL(string bkgRefNr = "", string Cargotype = "", string bizType = "", string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string BookingPK = "", string ShipmentDate = "",
        Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, Int16 intXBkg = 0, Int16 intCLAgt = 0, string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            DataSet MainDS = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();
            string strCondition = null;
            try
            {
                if (flag == 0)
                {
                    strCondition = strCondition + "  AND 1=2";
                }
                if (Convert.ToInt32(lblCarrierPK) > 0)
                {
                    strCondition = strCondition + " AND BST.CARRIER_MST_FK=" + lblCarrierPK;
                }
                if (Convert.ToInt32(POLPK) > 0)
                {
                    strCondition = strCondition + "  AND BST.PORT_MST_POL_FK = " + POLPK;
                }
                if (Convert.ToInt32(PODPK) > 0)
                {
                    strCondition = strCondition + " AND  BST.PORT_MST_POD_FK = " + PODPK;
                }
                if (Convert.ToInt32(CustomerPK) > 0)
                {
                    strCondition = strCondition + " AND BST.CUST_CUSTOMER_MST_FK = " + CustomerPK;
                }
                if (Convert.ToInt32(VslVoyPK) > 0)
                {
                    strCondition = strCondition + " AND BST.VESSEL_VOYAGE_FK =  " + VslVoyPK;
                }
                if (intXBkg == 1)
                {
                    strCondition = strCondition + " And BST.CB_AGENT_MST_FK IS NOT NULL";
                }
                if (intCLAgt == 1)
                {
                    strCondition = strCondition + " And BST.Cl_Agent_Mst_Fk IS NOT NULL";
                }
                if (Convert.ToInt32(CommGroupfk) > 0)
                {
                    strCondition = strCondition + " AND BST.COMMODITY_GROUP_FK =  " + CommGroupfk;
                }
                if (!string.IsNullOrEmpty(bkgRefNr))
                {
                    strCondition += " AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (!string.IsNullOrEmpty(SpaceReqNr))
                {
                    strCondition += " AND UPPER(BST.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (SrStatus != "0")
                {
                    strCondition += "   AND BST.SR_STATUS=" + SrStatus;
                }
                if (!string.IsNullOrEmpty(ShipmentDate))
                {
                    strCondition = strCondition + "  AND BST.SHIPMENT_DATE = TO_DATE('" + ShipmentDate + "','" + dateFormat + "')   ";
                }
                sb.Append("SELECT '' SLNR,");
                sb.Append("       Q.BOOKINGPK,");
                sb.Append("       Q.BOOKINGNR,");
                sb.Append("       Q.BOOKINGDATE,");
                sb.Append("       Q.CUSTOMERPK,");
                sb.Append("       Q.CUSTOMERNAME,");
                sb.Append("       Q.POLPK,");
                sb.Append("       Q.POLID,");
                sb.Append("       Q.PODPK,");
                sb.Append("       Q.PODID,");
                sb.Append("       Q.SHIPMENTDATE,");
                sb.Append("       Q.COMMGRPPK,");
                sb.Append("       Q.COMMODITYGRUOP,");
                sb.Append("       MAX(Q.BOOKINGTYPE) BOOKINGTYPE,");
                sb.Append("       0 TWENTYS,");
                sb.Append("       0 FOURTYS,");
                sb.Append("       0 TEUS,");
                sb.Append("       Q.CTRTYPE,");
                sb.Append("       NVL(SUM(Q.QTY), 0) QTY,");
                sb.Append("       NVL(SUM(Q.GROSSWT), 0) GROSSWT,");
                sb.Append("       NVL(SUM(Q.VOLUME), 0) VOLUME,");
                sb.Append("       Q.SRNR,");
                sb.Append("       Q.SRSTATUSPK,");
                sb.Append("       Q.SRSTATUS,");
                sb.Append("       '' SEL,Q.SEND_REQ_FLAG");
                sb.Append("  FROM");
                sb.Append(" (SELECT ''SLNR,BST.BOOKING_MST_PK BOOKINGPK,");
                sb.Append("       BST.BOOKING_REF_NO BOOKINGNR,");
                sb.Append("       BST.BOOKING_DATE BOOKINGDATE,");
                sb.Append("       CMT.CUSTOMER_MST_PK CUSTOMERPK,");
                sb.Append("       CMT.CUSTOMER_NAME CUSTOMERNAME,");
                sb.Append("       POL.PORT_MST_PK POLPK,");
                sb.Append("       POL.PORT_ID POLID,");
                sb.Append("       POD.PORT_MST_PK PODPK,");
                sb.Append("       POD.PORT_ID PODID,");
                sb.Append("       BST.SHIPMENT_DATE SHIPMENTDATE,");
                sb.Append("       CGMT.COMMODITY_GROUP_PK COMMGRPPK,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMODITYGRUOP,");
                sb.Append("       DECODE(BTSFL.TRANS_REFERED_FROM,");
                sb.Append("              1,");
                sb.Append("              'Quotation',");
                sb.Append("              2,");
                sb.Append("              'Spot Rate',");
                sb.Append("              3,");
                sb.Append("              'Customer Contract',");
                sb.Append("              4,");
                sb.Append("              'SL Tariff',");
                sb.Append("              5,");
                sb.Append("              'SRR',");
                sb.Append("              6,");
                sb.Append("              'General Tariff',");
                sb.Append("              7,");
                sb.Append("              'Manual') BOOKINGTYPE,");
                sb.Append("       0 TWENTYS,");
                sb.Append("       0 FOURTYS,");
                sb.Append("       0 TEUS,");
                sb.Append("       CTMT.CONTAINER_TYPE_MST_ID CTRTYPE,");
                sb.Append("       BTSFL.QUANTITY QTY,");
                sb.Append("       BST.CHARGEABLE_WEIGHT GROSSWT,");
                sb.Append("       BST.VOLUME_IN_CBM VOLUME,");
                sb.Append("       BST.SPACE_REQUEST_NR SRNR,");
                sb.Append("       BST.SPACE_REQUEST_PK SRSTATUSPK,");
                sb.Append("       DECODE(BST.SR_STATUS,");
                sb.Append("              1,");
                sb.Append("              'Requested',");
                sb.Append("               2,");
                sb.Append("              'Confirmed',");
                sb.Append("               3,");
                sb.Append("               'Rejected',");
                sb.Append("               4,");
                sb.Append("              'Cancelled') SRSTATUS,");
                sb.Append("       '' SEL,BST.SEND_REQ_FLAG");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       USER_MST_TBL            UMT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       BOOKING_TRN BTSFL,");
                sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
                sb.Append("       DIMENTION_UNIT_MST_TBL  UOM");
                sb.Append(" WHERE ");
                sb.Append("   CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK");
                sb.Append("   AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
                sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK = BTSFL.BASIS");
                sb.Append("   AND UMT.USER_MST_PK = BST.CREATED_BY_FK");
                sb.Append("   AND BST.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
                //sb.Append("   AND POL.LOCATION_MST_FK =" & Session("LOGED_IN_LOC_FK"))
                sb.Append("           AND POL.LOCATION_MST_FK IN ");
                sb.Append("          (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L ");
                sb.Append("                       START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("                        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ");
                sb.Append("   AND BST.CARGO_TYPE = 2");
                sb.Append(" " + strCondition + "");
                sb.Append(" )Q");
                sb.Append("  GROUP BY Q.BOOKINGPK,");
                sb.Append("          Q.BOOKINGNR,");
                sb.Append("          Q.BOOKINGDATE,");
                sb.Append("          Q.CUSTOMERPK,");
                sb.Append("          Q.CUSTOMERNAME,");
                sb.Append("          Q.POLPK,");
                sb.Append("          Q.POLID,");
                sb.Append("          Q.PODPK,");
                sb.Append("          Q.PODID,");
                sb.Append("          Q.SHIPMENTDATE,");
                sb.Append("          Q.COMMGRPPK,");
                sb.Append("          Q.COMMODITYGRUOP,");
                sb.Append("          Q.CTRTYPE,");
                sb.Append("          Q.SRNR,");
                sb.Append("          Q.SRSTATUSPK,");
                sb.Append("          Q.SRSTATUS,Q.SEND_REQ_FLAG");
                sb.Append("          ORDER BY Q.BOOKINGDATE DESC ");
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "PARENT");

                sb.Remove(0, sb.Length);
                sb.Append("SELECT BST.BOOKING_MST_PK,");
                sb.Append("       UOM.DIMENTION_UNIT_MST_PK,");
                sb.Append("       '' COMMODITYID,");
                sb.Append("       '' COMMODITYNAME,");
                sb.Append("       UOM.DIMENTION_ID BASIS,");
                sb.Append("       NVL(BTSFL.QUANTITY,0) QTY,");
                sb.Append("       NVL(BST.GROSS_WEIGHT,0) GRSWEIGHT,");
                sb.Append("       NVL(BST.NET_WEIGHT,0) NETWEIGHT,");
                sb.Append("       NVL(BST.VOLUME_IN_CBM,0) VOLUME");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       BOOKING_TRN BTSFL,");
                sb.Append("       DIMENTION_UNIT_MST_TBL  UOM,");
                sb.Append("       PORT_MST_TBL            POL, ");
                sb.Append("       USER_MST_TBL            UMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
                sb.Append("   AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK");
                sb.Append("   AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("           AND POL.LOCATION_MST_FK IN ");
                sb.Append("          (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L ");
                sb.Append("                       START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("                        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ");
                sb.Append("   AND BST.CARGO_TYPE = 2");
                sb.Append(" " + strCondition + "");
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "CHILD");

                DataRelation relBooking_Details = new DataRelation("BOOKING", new DataColumn[] { MainDS.Tables[0].Columns["BOOKINGPK"] }, new DataColumn[] { MainDS.Tables[1].Columns["BOOKING_MST_PK"] });
                relBooking_Details.Nested = true;
                MainDS.Relations.Add(relBooking_Details);
                return MainDS;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }

        public DataSet GetGridBookingSeaQueryBBC(string bkgRefNr = "", string Cargotype = "", string bizType = "", string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string BookingPK = "", string ShipmentDate = "",
        Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, Int16 intXBkg = 0, Int16 intCLAgt = 0, string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            DataSet MainDS = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();
            string strCondition = null;
            try
            {
                if (flag == 0)
                {
                    strCondition = strCondition + "  AND 1=2";
                }
                if (Convert.ToInt32(lblCarrierPK) > 0)
                {
                    strCondition = strCondition + " AND BST.CARRIER_MST_FK=" + lblCarrierPK;
                }
                if (Convert.ToInt32(POLPK) > 0)
                {
                    strCondition = strCondition + "  AND BST.PORT_MST_POL_FK = " + POLPK;
                }
                if (Convert.ToInt32(PODPK) > 0)
                {
                    strCondition = strCondition + " AND BST.PORT_MST_POD_FK = " + PODPK;
                }
                if (Convert.ToInt32(CustomerPK) > 0)
                {
                    strCondition = strCondition + " AND BST.CUST_CUSTOMER_MST_FK = " + CustomerPK;
                }
                if (Convert.ToInt32(VslVoyPK) > 0)
                {
                    strCondition = strCondition + " AND BST.VESSEL_VOYAGE_FK =  " + VslVoyPK;
                }
                if (intXBkg == 1)
                {
                    strCondition = strCondition + " And BST.CB_AGENT_MST_FK IS NOT NULL";
                }
                if (intCLAgt == 1)
                {
                    strCondition = strCondition + " And BST.Cl_Agent_Mst_Fk IS NOT NULL";
                }
                if (Convert.ToInt32(CommGroupfk) > 0)
                {
                    strCondition = strCondition + " AND BST.COMMODITY_GROUP_FK =  " + CommGroupfk;
                }
                if (!string.IsNullOrEmpty(bkgRefNr))
                {
                    strCondition += " AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (!string.IsNullOrEmpty(SpaceReqNr))
                {
                    strCondition += " AND UPPER(BST.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (SrStatus != "0")
                {
                    strCondition += "   AND BST.SR_STATUS=" + SrStatus;
                }
                if (!string.IsNullOrEmpty(ShipmentDate))
                {
                    strCondition = strCondition + "  AND BST.SHIPMENT_DATE = TO_DATE('" + ShipmentDate + "','" + dateFormat + "')   ";
                }
                sb.Append("SELECT '' SLNR,");
                sb.Append("       Q.BOOKINGPK,");
                sb.Append("       Q.BOOKINGNR,");
                sb.Append("       Q.BOOKINGDATE,");
                sb.Append("       Q.CUSTOMERPK,");
                sb.Append("       Q.CUSTOMERNAME,");
                sb.Append("       Q.POLPK,");
                sb.Append("       Q.POLID,");
                sb.Append("       Q.PODPK,");
                sb.Append("       Q.PODID,");
                sb.Append("       Q.SHIPMENTDATE,");
                sb.Append("       Q.COMMGRPPK,");
                sb.Append("       Q.COMMODITYGRUOP,");
                sb.Append("       Q.BOOKINGTYPE,");
                sb.Append("       0 TWENTYS,");
                sb.Append("       0 FOURTYS,");
                sb.Append("       0 TEUS,");
                sb.Append("       '' CTRTYPE,");
                sb.Append("       NVL(SUM(Q.QTY), 0) QTY,");
                sb.Append("       NVL(SUM(Q.GROSSWT), 0) GROSSWT,");
                sb.Append("       NVL(SUM(Q.VOLUME), 0) VOLUME,");
                sb.Append("       Q.SRNR,");
                sb.Append("       Q.SRSTATUSPK,");
                sb.Append("       Q.SRSTATUS,");
                sb.Append("       '' SEL,Q.SEND_REQ_FLAG");
                sb.Append("  FROM");
                sb.Append("  (SELECT ''SLNR,BST.BOOKING_MST_PK BOOKINGPK,");
                sb.Append("       BST.BOOKING_REF_NO BOOKINGNR,");
                sb.Append("       BST.BOOKING_DATE BOOKINGDATE,");
                sb.Append("       CMT.CUSTOMER_MST_PK CUSTOMERPK,");
                sb.Append("       CMT.CUSTOMER_NAME CUSTOMERNAME,");
                sb.Append("       POL.PORT_MST_PK POLPK,");
                sb.Append("       POL.PORT_ID POLID,");
                sb.Append("       POD.PORT_MST_PK PODPK,");
                sb.Append("       POD.PORT_ID PODID,");
                sb.Append("       BST.SHIPMENT_DATE SHIPMENTDATE,");
                sb.Append("       NVL(CGMT.COMMODITY_GROUP_PK,0) COMMGRPPK,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMODITYGRUOP,");
                sb.Append("       DECODE(BTSFL.TRANS_REFERED_FROM,");
                sb.Append("              1,");
                sb.Append("              'Quotation',");
                sb.Append("              2,");
                sb.Append("              'Spot Rate',");
                sb.Append("              3,");
                sb.Append("              'Customer Contract',");
                sb.Append("              4,");
                sb.Append("              'SL Tariff',");
                sb.Append("              5,");
                sb.Append("              'SRR',");
                sb.Append("              6,");
                sb.Append("              'General Tariff',");
                sb.Append("              7,");
                sb.Append("              'Manual') BOOKINGTYPE,");
                sb.Append("       0 TWENTYS,");
                sb.Append("       0 FOURTYS,");
                sb.Append("       0 TEUS,");
                sb.Append("       '' CTRTYPE,");
                sb.Append("       BTSFL.QUANTITY QTY,");
                sb.Append("       BTSFL.WEIGHT_MT GROSSWT,");
                sb.Append("       BTSFL.VOLUME_CBM VOLUME,");
                sb.Append("       BST.SPACE_REQUEST_NR SRNR,");
                sb.Append("       BST.SPACE_REQUEST_PK SRSTATUSPK,");
                sb.Append("       DECODE(BST.SR_STATUS,");
                sb.Append("              1,");
                sb.Append("              'Requested',");
                sb.Append("              2,");
                sb.Append("              'Confirmed',");
                sb.Append("              3,");
                sb.Append("              'Rejected',");
                sb.Append("               4,");
                sb.Append("               'Cancelled') SRSTATUS,");
                sb.Append("       '' SEL,BST.SEND_REQ_FLAG");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       USER_MST_TBL            UMT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       BOOKING_TRN BTSFL,");
                sb.Append("       DIMENTION_UNIT_MST_TBL  UOM");
                sb.Append(" WHERE ");
                sb.Append("   CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK");
                sb.Append("   AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
                sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK = BTSFL.BASIS");
                sb.Append("   AND UMT.USER_MST_PK = BST.CREATED_BY_FK");
                //sb.Append("   AND POL.LOCATION_MST_FK =" & Session("LOGED_IN_LOC_FK"))
                sb.Append("           AND POL.LOCATION_MST_FK IN ");
                sb.Append("          (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L ");
                sb.Append("                       START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("                        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ");
                sb.Append("   AND BST.CARGO_TYPE = 4");
                sb.Append(" " + strCondition + "");
                sb.Append(" )Q");
                sb.Append("  GROUP BY Q.BOOKINGPK,");
                sb.Append("          Q.BOOKINGNR,");
                sb.Append("          Q.BOOKINGDATE,");
                sb.Append("          Q.CUSTOMERPK,");
                sb.Append("          Q.CUSTOMERNAME,");
                sb.Append("          Q.POLPK,");
                sb.Append("          Q.POLID,");
                sb.Append("          Q.PODPK,");
                sb.Append("          Q.PODID,");
                sb.Append("          Q.SHIPMENTDATE,");
                sb.Append("          Q.COMMGRPPK,");
                sb.Append("          Q.COMMODITYGRUOP,");
                sb.Append("          Q.BOOKINGTYPE,Q.SRNR,");
                sb.Append("          Q.SRSTATUSPK,Q.SRSTATUS,Q.SEND_REQ_FLAG");
                sb.Append("          ORDER BY Q.BOOKINGDATE DESC ");
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "PARENT");

                sb.Remove(0, sb.Length);
                sb.Append("SELECT BST.BOOKING_MST_PK,");
                sb.Append("       CMT.COMMODITY_MST_PK,");
                sb.Append("       CMT.COMMODITY_ID COMMODITYID,");
                sb.Append("       CMT.COMMODITY_NAME COMMODITYNAME,");
                sb.Append("       UOM.DIMENTION_ID BASIS,");
                sb.Append("       NVL(BTSFL.QUANTITY,0) QTY,");
                sb.Append("       NVL(BTSFL.WEIGHT_MT,0) GRSWEIGHT,");
                sb.Append("        0 NETWEIGHT,");
                sb.Append("       NVL(BTSFL.VOLUME_CBM,0) VOLUME");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       BOOKING_TRN BTSFL,");
                sb.Append("       DIMENTION_UNIT_MST_TBL  UOM,");
                sb.Append("       USER_MST_TBL            UMT,");
                sb.Append("       PORT_MST_TBL            POL, ");
                sb.Append("       COMMODITY_MST_TBL       CMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
                sb.Append("   AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK");
                sb.Append("   AND CMT.COMMODITY_MST_PK = BTSFL.COMMODITY_MST_FK");
                sb.Append("   AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("           AND POL.LOCATION_MST_FK IN ");
                sb.Append("          (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L ");
                sb.Append("                       START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                sb.Append("                        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ");
                sb.Append("   AND BST.CARGO_TYPE = 4");
                sb.Append(" " + strCondition + "");
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "CHILD");

                DataRelation relBooking_Details = new DataRelation("BOOKING", new DataColumn[] { MainDS.Tables[0].Columns["BOOKINGPK"] }, new DataColumn[] { MainDS.Tables[1].Columns["BOOKING_MST_PK"] });
                relBooking_Details.Nested = true;
                MainDS.Relations.Add(relBooking_Details);
                return MainDS;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }

        public DataSet GetGridBookingSeaQueryAIR(string bkgRefNr = "", string bizType = "", string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string FlightNr = "", string BookingPK = "", string ShipmentDate = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 flag = 0, Int16 intXBkg = 0, Int16 intCLAgt = 0, string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);

            if (flag == 0)
            {
                strCondition = strCondition + "  AND 1=2";
            }
            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BST.CARRIER_MST_FK=" + lblCarrierPK;
            }
            if (Convert.ToInt32(POLPK) > 0)
            {
                strCondition = strCondition + "  AND POL.PORT_MST_PK= " + POLPK;
            }
            if (Convert.ToInt32(PODPK) > 0)
            {
                strCondition = strCondition + " AND POD.PORT_MST_PK = " + PODPK;
            }
            if (Convert.ToInt32(CustomerPK) > 0)
            {
                strCondition = strCondition + " AND BST.CUST_CUSTOMER_MST_FK = " + CustomerPK;
            }
            if (Convert.ToInt32(FlightNr) > 0)
            {
                strCondition = strCondition + " AND BST.VOYAGE_FLIGHT_NO =  '" + FlightNr + "'";
            }
            if (intXBkg == 1)
            {
                strCondition = strCondition + " And BST.CB_AGENT_MST_FK IS NOT NULL";
            }
            if (intCLAgt == 1)
            {
                strCondition = strCondition + " And BST.Cl_Agent_Mst_Fk IS NOT NULL";
            }
            if (Convert.ToInt32(CommGroupfk) > 0)
            {
                strCondition = strCondition + " AND BST.COMMODITY_GROUP_FK =  " + CommGroupfk;
            }
            if (!string.IsNullOrEmpty(bkgRefNr))
            {
                strCondition += " AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition += " AND UPPER(BST.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition += "   AND BST.SR_STATUS=" + SrStatus;
            }
            if (!string.IsNullOrEmpty(ShipmentDate))
            {
                strCondition = strCondition + "  AND BST.SHIPMENT_DATE = TO_DATE('" + ShipmentDate + "','" + dateFormat + "')   ";
            }
            sb.Append("SELECT BST.BOOKING_MST_PK BOOKINGPK,");
            sb.Append("       BST.BOOKING_REF_NO BOOKINGNR,");
            sb.Append("       BST.BOOKING_DATE BOOKINGDATE,");
            sb.Append("       CMT.CUSTOMER_MST_PK CUSTOMERPK,");
            sb.Append("       CMT.CUSTOMER_NAME CUSTOMERNAME,");
            sb.Append("       POL.PORT_MST_PK POLPK,");
            sb.Append("       POL.PORT_ID POLID,");
            sb.Append("       POD.PORT_MST_PK PODPK,");
            sb.Append("       POD.PORT_ID PODID,");
            sb.Append("       BST.SHIPMENT_DATE SHIPMENTDATE,");
            sb.Append("       CGMT.COMMODITY_GROUP_PK COMMGRPPK,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMODITYGRUOP,");
            sb.Append("       DECODE(BTSFL.TRANS_REFERED_FROM,");
            sb.Append("              1,");
            sb.Append("              'Quotation',");
            sb.Append("              2,");
            sb.Append("              'Spot Rate',");
            sb.Append("              3,");
            sb.Append("              'Customer Contract',");
            sb.Append("              4,");
            sb.Append("              'SL Tariff',");
            sb.Append("              5,");
            sb.Append("              'SRR',");
            sb.Append("              6,");
            sb.Append("              'General Tariff',");
            sb.Append("              7,");
            sb.Append("              'Manual') BOOKINGTYPE,");
            sb.Append("       0 TWENTYS,");
            sb.Append("       0 FOURTYS,");
            sb.Append("       0 TEUS,");
            sb.Append("       '' CTRTYPE,");
            sb.Append("       0 QTY,");
            sb.Append("       NVL(BST.FRT_WEIGHT,BST.CHARGEABLE_WEIGHT) GROSSWT,");
            sb.Append("       BST.VOLUME_IN_CBM VOLUME,");
            sb.Append("       BST.SPACE_REQUEST_NR SRNR,");
            sb.Append("       BST.SPACE_REQUEST_PK SRSTATUSPK,");
            sb.Append("       DECODE(BST.SR_STATUS,");
            sb.Append("              1,");
            sb.Append("              'Requested',");
            sb.Append("              2,");
            sb.Append("              'Confirmed',");
            sb.Append("              3,");
            sb.Append("              'Rejected',");
            sb.Append("               4,");
            sb.Append("               'Cancelled') SRSTATUS,");
            sb.Append("       '' SEL,BST.SEND_REQ_FLAG");
            sb.Append("  FROM BOOKING_MST_TBL         BST,");
            sb.Append("       USER_MST_TBL            UMT,");
            sb.Append("       CUSTOMER_MST_TBL        CMT,");
            sb.Append("       PORT_MST_TBL            POL,");
            sb.Append("       PORT_MST_TBL            POD,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       BOOKING_TRN         BTSFL");
            sb.Append(" WHERE ");
            sb.Append("   CMT.CUSTOMER_MST_PK(+) = BST.CUST_CUSTOMER_MST_FK");
            sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK");
            sb.Append("   AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
            sb.Append("   AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
            //sb.Append("   AND POL.LOCATION_MST_FK =" & Session("LOGED_IN_LOC_FK"))
            sb.Append("           AND POL.LOCATION_MST_FK IN ");
            sb.Append("          (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L ");
            sb.Append("                       START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            sb.Append("                        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK) ");
            sb.Append(" " + strCondition + "");
            sb.Append(" ORDER BY BST.BOOKING_DATE DESC ");

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strSQL = sb.ToString();
            strCount.Append(" SELECT COUNT(*)  from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strCount.Remove(0, strCount.Length);

            System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
            sqlstr2.Append(" Select * from ");
            sqlstr2.Append("  ( Select ROWNUM SLNR, q.* from ");
            sqlstr2.Append("  (" + sb.ToString() + " ");
            sqlstr2.Append("  ) q )  WHERE \"SLNR\"  BETWEEN " + start + " AND " + last + "");
            strSQL = sqlstr2.ToString();

            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion "Getting Grid Details"

        #region "Generete SRNUMBER"

        public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
        {
            return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
        }

        #endregion "Generete SRNUMBER"

        #region "Get CommodityName"

        public DataSet GetCommodityName(string BOOKINGPK, int Cargotype = 1)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (Cargotype == 1)
            {
                sb.Append(" SELECT BOOK.booking_mst_pk, ");
                sb.Append("          ROWTOCOL('SELECT  DISTINCT CMT.COMMODITY_NAME FROM BOOKING_COMMODITY_DTL CDT, ");
                sb.Append("          BOOKING_TRN_CARGO_DTL BCT, COMMODITY_MST_TBL     CMT ");
                sb.Append("           WHERE BCT.BOOKING_TRN_CARGO_PK = CDT.BOOKING_CARGO_DTL_FK(+) ");
                sb.Append("          AND CMT.COMMODITY_MST_PK = CDT.COMMODITY_MST_FK ");
                sb.Append("          AND  BCT.BOOKING_TRN_FK in (' || NVL(BKGTRN.BOOKING_TRN_PK, BKGTRN.Commodity_Mst_Fk) || ')') COMMODITY_NAME ");
                sb.Append("          FROM booking_mst_tbl         BOOK,");
                sb.Append("               booking_trn           BKGTRN");
                sb.Append("         WHERE BOOK.booking_mst_pk = BKGTRN.booking_mst_fk");
                sb.Append("          AND BOOK.booking_mst_pk IN (" + BOOKINGPK + ") ");
            }
            else
            {
                sb.Append(" SELECT BOOK.booking_mst_pk, ");
                sb.Append("          ROWTOCOL('SELECT DISTINCT CMT.COMMODITY_NAME FROM COMMODITY_MST_TBL CMT ");
                sb.Append("          WHERE CMT.COMMODITY_MST_PK  in (' || NVL(BKGTRN.Commodity_Mst_Fks,BKGTRN.Commodity_Mst_Fk) ||')') COMMODITY_NAME ");
                sb.Append("          FROM booking_mst_tbl         BOOK,");
                sb.Append("               booking_trn           BKGTRN");
                sb.Append("         WHERE BOOK.booking_mst_pk = BKGTRN.booking_mst_fk");
                sb.Append("          AND BOOK.booking_mst_pk IN (" + BOOKINGPK + ") ");
            }
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Get CommodityName"

        #region "Space Request Report FCL"

        public DataSet FetchSpaceRequestFCL(string BOOKINGPK, string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string ShipmentDate = "", string bkgRefNr = "", Int16 intXBkg = 0, Int16 intCLAgt = 0,
        string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;

            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.CARRIER_MST_FK=" + lblCarrierPK;
            }
            if (Convert.ToInt32(POLPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.PORT_MST_POL_FK = " + POLPK;
            }
            if (Convert.ToInt32(PODPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.PORT_MST_POD_FK = " + PODPK;
            }
            if (Convert.ToInt32(CustomerPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.CUST_CUSTOMER_MST_FK = " + CustomerPK;
            }
            if (Convert.ToInt32(VslVoyPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.VESSEL_VOYAGE_FK =  " + VslVoyPK;
            }
            if (intXBkg == 1)
            {
                strCondition = strCondition + " And BOOK.CB_AGENT_MST_FK IS NOT NULL";
            }
            if (intCLAgt == 1)
            {
                strCondition = strCondition + " And BOOK.Cl_Agent_Mst_Fk IS NOT NULL";
            }
            if (!string.IsNullOrEmpty(bkgRefNr))
            {
                strCondition = strCondition + " AND UPPER(BOOK.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (Convert.ToInt32(CommGroupfk) > 0)
            {
                strCondition = strCondition + " AND BOOK.COMMODITY_GROUP_FK =  " + CommGroupfk;
            }
            if (Convert.ToInt32(BOOKINGPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.booking_mst_pk IN (" + BOOKINGPK + " )";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition += " AND UPPER(BOOK.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition += "   AND BOOK.SR_STATUS=" + SrStatus;
            }
            if (!string.IsNullOrEmpty(ShipmentDate))
            {
                strCondition = strCondition + "  AND BOOK.SHIPMENT_DATE = TO_DATE('" + ShipmentDate + "','" + dateFormat + "')   ";
            }
            sb.Append(" SELECT Q.booking_mst_pk,");
            sb.Append("        Q.SPACE_REQUEST_PK,");
            sb.Append("        Q.SPACE_REQUEST_NR,");
            sb.Append("        Q.SR_STATUS,");
            sb.Append("        Q.SHIPMENT_DATE,");
            sb.Append("        Q.CARGO_TYPE,");
            sb.Append("        Q.POLID,");
            sb.Append("        Q.PODID,");
            sb.Append("        Q.VESSEL_NAME,");
            sb.Append("        Q.COMMODITY_GROUP_CODE,");
            sb.Append("        Q.OPERATOR_NAME,");
            sb.Append("        Q.OFFICE_NAME,");
            sb.Append("       SUM(Q.TWENTYS) TWENTYS,");
            sb.Append("       SUM(Q.FOURTYS) FOURTYS,");
            sb.Append("       SUM(Q.TEUS) TEUS,");
            sb.Append("       SUM(Q.GROSS_WEIGHT) GROSS_WEIGHT,");
            sb.Append("       SUM(Q.VOLUME_IN_CBM) VOLUME_IN_CBM");
            sb.Append("  FROM (SELECT BOOK.booking_mst_pk,");
            sb.Append("               BOOK.SPACE_REQUEST_PK,");
            sb.Append("               BOOK.SPACE_REQUEST_NR,");
            sb.Append("               BOOK.SHIPMENT_DATE,");
            sb.Append("               BOOK.SR_STATUS,");
            sb.Append("               BOOK.CARGO_TYPE,");
            sb.Append("               POL.PORT_ID POLID,");
            sb.Append("               POD.PORT_ID PODID,");
            sb.Append("               BOOK.VESSEL_NAME || '/' || BOOK.VOYAGE_FLIGHT_NO VESSEL_NAME,");
            sb.Append("               CGMT.COMMODITY_GROUP_CODE,");
            sb.Append("               OPR.OPERATOR_NAME,");
            sb.Append("               LMT.OFFICE_NAME,");
            sb.Append("               CASE");
            sb.Append("                 WHEN SUBSTR(CTMT.CONTAINER_TYPE_MST_ID, 0, 2) = '20' THEN");
            sb.Append("                  NVL(BKGTRN.NO_OF_BOXES, 0)");
            sb.Append("                 ELSE");
            sb.Append("                  0");
            sb.Append("               END AS TWENTYS,");
            sb.Append("               CASE");
            sb.Append("                 WHEN SUBSTR(CTMT.CONTAINER_TYPE_MST_ID, 0, 2) = '40' THEN");
            sb.Append("                  NVL(BKGTRN.NO_OF_BOXES, 0)");
            sb.Append("                 ELSE");
            sb.Append("                  0");
            sb.Append("               END AS FOURTYS,");
            sb.Append("               NVL(BKGTRN.NO_OF_BOXES, 0) * NVL(CTMT.TEU_FACTOR, 0) TEUS,");

            sb.Append("               CASE WHEN BOOK.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(BTCD.GROSS_WEIGHT, 0)) ");
            sb.Append("               FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD ");
            sb.Append("               WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK ");
            sb.Append("               AND BTCD.BOOKING_TRN_FK(+) = BKGTRN.BOOKING_TRN_PK) ");
            sb.Append("               ELSE NVL(BOOK.GROSS_WEIGHT, 0) END GROSS_WEIGHT,");

            sb.Append("               CASE WHEN BOOK.CARGO_TYPE = 1 THEN CASE WHEN (SELECT SUM(NVL(BCD.VOLUME_IN_CBM, 0)) ");
            sb.Append("               FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD ");
            sb.Append("               WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK ");
            sb.Append("               AND BTCD.BOOKING_TRN_FK(+) = BKGTRN.BOOKING_TRN_PK) > 0 THEN ");
            sb.Append("               (SELECT SUM(NVL(BCD.VOLUME_IN_CBM, 0)) FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD ");
            sb.Append("               WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK ");
            sb.Append("               AND BTCD.BOOKING_TRN_FK(+) = BKGTRN.BOOKING_TRN_PK) ELSE ");
            sb.Append("               NVL(BOOK.VOLUME_IN_CBM, 0) END ELSE NVL(BOOK.VOLUME_IN_CBM, 0) END VOLUME_IN_CBM ");

            sb.Append("          FROM booking_mst_tbl         BOOK,");
            sb.Append("               USER_MST_TBL            UMT,");
            sb.Append("               booking_trn BKGTRN,");
            sb.Append("               CONTAINER_TYPE_MST_TBL  CTMT,");
            sb.Append("               PORT_MST_TBL            POL,");
            sb.Append("               PORT_MST_TBL            POD,");
            sb.Append("               OPERATOR_MST_TBL        OPR,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("               LOCATION_MST_TBL        LMT");
            sb.Append("         WHERE BOOK.booking_mst_pk = BKGTRN.booking_mst_fk");
            sb.Append("           AND BKGTRN.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK");
            sb.Append("           AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("           AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("           AND BOOK.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
            sb.Append("           AND BOOK.CARRIER_Mst_Fk = OPR.OPERATOR_MST_PK");
            sb.Append("           AND BOOK.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("           AND LMT.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK");
            sb.Append("           AND BOOK.STATUS = 1");
            sb.Append("           AND BOOK.CARGO_TYPE = 1");
            sb.Append("           AND UMT.DEFAULT_LOCATION_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            sb.Append("       " + strCondition + "");
            sb.Append(" )Q");
            sb.Append(" GROUP BY Q.booking_mst_pk,");
            sb.Append("          Q.SPACE_REQUEST_PK,");
            sb.Append("          Q.SPACE_REQUEST_NR,");
            sb.Append("          Q.SR_STATUS,");
            sb.Append("          Q.SHIPMENT_DATE,");
            sb.Append("          Q.CARGO_TYPE,");
            sb.Append("          Q.POLID,");
            sb.Append("          Q.PODID,");
            sb.Append("          Q.VESSEL_NAME,");
            sb.Append("          Q.COMMODITY_GROUP_CODE,");
            sb.Append("          Q.OPERATOR_NAME,");
            sb.Append("          Q.OFFICE_NAME");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Space Request Report FCL"

        #region "Space Request Report LCL"

        public DataSet FetchSpaceRequestLCL(string BOOKINGPK, string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string ShipmentDate = "", string bkgRefNr = "", Int16 intXBkg = 0, Int16 intCLAgt = 0,
        string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;

            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.CARRIER_Mst_Fk=" + lblCarrierPK;
            }
            if (Convert.ToInt32(POLPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.PORT_MST_POL_FK = " + POLPK;
            }
            if (Convert.ToInt32(PODPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.PORT_MST_POD_FK = " + PODPK;
            }
            if (Convert.ToInt32(CustomerPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.CUST_CUSTOMER_MST_FK = " + CustomerPK;
            }
            if (Convert.ToInt32(VslVoyPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.VESSEL_VOYAGE_FK =  " + VslVoyPK;
            }
            if (intXBkg == 1)
            {
                strCondition = strCondition + " And BOOK.CB_AGENT_MST_FK IS NOT NULL";
            }
            if (intCLAgt == 1)
            {
                strCondition = strCondition + " And BOOK.Cl_Agent_Mst_Fk IS NOT NULL";
            }
            if (!string.IsNullOrEmpty(bkgRefNr))
            {
                strCondition = strCondition + " AND UPPER(BOOK.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (Convert.ToInt32(CommGroupfk) > 0)
            {
                strCondition = strCondition + " AND BOOK.COMMODITY_GROUP_FK =  " + CommGroupfk;
            }
            if (Convert.ToInt32(BOOKINGPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.booking_mst_pk IN (" + BOOKINGPK + " )";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition += " AND UPPER(BOOK.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition += "   AND BOOK.SR_STATUS=" + SrStatus;
            }
            if (!string.IsNullOrEmpty(ShipmentDate))
            {
                strCondition = strCondition + "  AND BOOK.SHIPMENT_DATE = TO_DATE('" + ShipmentDate + "','" + dateFormat + "')   ";
            }
            sb.Append("SELECT Q.booking_mst_pk,");
            sb.Append("       Q.SPACE_REQUEST_PK,");
            sb.Append("       Q.SPACE_REQUEST_NR,");
            sb.Append("       Q.SR_STATUS,");
            sb.Append("       Q.SHIPMENT_DATE,");
            sb.Append("       Q.POL,");
            sb.Append("       Q.POD,");
            sb.Append("       Q.COMMODITY_GROUP_CODE,");
            sb.Append("       Q.VESSEL_NAME,");
            sb.Append("       Q.CARGO_TYPE,");
            sb.Append("       Q.OFFICE_NAME,");
            sb.Append("       NVL(SUM(Q.CHARGEABLE_WEIGHT), 0) CHARGEABLE_WEIGHT,");
            sb.Append("       NVL(SUM(Q.VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
            sb.Append("       Q.OPERATOR_NAME");
            sb.Append("  FROM (SELECT BOOK.booking_mst_pk,");
            sb.Append("               BOOK.SPACE_REQUEST_PK,");
            sb.Append("               BOOK.SPACE_REQUEST_NR,");
            sb.Append("               BOOK.SR_STATUS,");
            sb.Append("               BOOK.SHIPMENT_DATE,");
            sb.Append("               POL.PORT_ID               POL,");
            sb.Append("               POD.PORT_ID               POD,");
            sb.Append("               CGMT.COMMODITY_GROUP_CODE,");
            sb.Append("               BOOK.VESSEL_NAME || '/' || BOOK.VOYAGE_FLIGHT_NO VESSEL_NAME,");
            sb.Append("               BOOK.CARGO_TYPE,");
            sb.Append("               LMT.OFFICE_NAME,");
            sb.Append("               BOOK.CHARGEABLE_WEIGHT,");
            sb.Append("               BOOK.VOLUME_IN_CBM,");
            sb.Append("               OPR.OPERATOR_NAME");
            sb.Append("          FROM booking_mst_tbl         BOOK,");
            sb.Append("               booking_trn BKGTRN,");
            sb.Append("               PORT_MST_TBL            POL,");
            sb.Append("               PORT_MST_TBL            POD,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("               OPERATOR_MST_TBL        OPR,");
            sb.Append("               USER_MST_TBL            UMT,");
            sb.Append("               LOCATION_MST_TBL        LMT");
            sb.Append("         WHERE BOOK.booking_mst_pk = BKGTRN.booking_mst_fk");
            sb.Append("           AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("           AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("           AND BOOK.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
            sb.Append("           AND BOOK.CARRIER_Mst_Fk = OPR.OPERATOR_MST_PK");
            sb.Append("           AND BOOK.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("           AND LMT.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK");
            sb.Append("           AND BOOK.STATUS = 1");
            sb.Append("           AND BOOK.CARGO_TYPE = 2 ");
            sb.Append("           AND UMT.DEFAULT_LOCATION_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            sb.Append("       " + strCondition + "");
            sb.Append("      )Q");
            sb.Append(" GROUP BY Q.booking_mst_pk,");
            sb.Append("          Q.SPACE_REQUEST_PK,");
            sb.Append("          Q.SPACE_REQUEST_NR,");
            sb.Append("          Q.SR_STATUS,");
            sb.Append("          Q.SHIPMENT_DATE,");
            sb.Append("          Q.POL,");
            sb.Append("          Q.POD,");
            sb.Append("          Q.COMMODITY_GROUP_CODE,");
            sb.Append("          Q.VESSEL_NAME,");
            sb.Append("          Q.CARGO_TYPE,");
            sb.Append("          Q.OFFICE_NAME,Q.OPERATOR_NAME");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Space Request Report LCL"

        #region "Space Request Report BBC"

        public DataSet FetchSpaceRequestBBC(string BOOKINGPK, string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string ShipmentDate = "", string bkgRefNr = "", Int16 intXBkg = 0, Int16 intCLAgt = 0,
        string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;

            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.CARRIER_Mst_Fk=" + lblCarrierPK;
            }
            if (Convert.ToInt32(POLPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.PORT_MST_POL_FK = " + POLPK;
            }
            if (Convert.ToInt32(PODPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.PORT_MST_POD_FK = " + PODPK;
            }
            if (Convert.ToInt32(CustomerPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.CUST_CUSTOMER_MST_FK = " + CustomerPK;
            }
            if (Convert.ToInt32(VslVoyPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.VESSEL_VOYAGE_FK =  " + VslVoyPK;
            }
            if (intXBkg == 1)
            {
                strCondition = strCondition + " And BOOK.CB_AGENT_MST_FK IS NOT NULL";
            }
            if (intCLAgt == 1)
            {
                strCondition = strCondition + " And BOOK.Cl_Agent_Mst_Fk IS NOT NULL";
            }
            if (Convert.ToInt32(CommGroupfk) > 0)
            {
                strCondition = strCondition + " AND BOOK.COMMODITY_GROUP_FK =  " + CommGroupfk;
            }
            if (!string.IsNullOrEmpty(bkgRefNr))
            {
                strCondition = strCondition + " AND UPPER(BOOK.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (Convert.ToInt32(BOOKINGPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.booking_mst_pk IN (" + BOOKINGPK + " )";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition += " AND UPPER(BOOK.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition += "   AND BOOK.SR_STATUS=" + SrStatus;
            }
            if (!string.IsNullOrEmpty(ShipmentDate))
            {
                strCondition = strCondition + "  AND BOOK.SHIPMENT_DATE = TO_DATE('" + ShipmentDate + "','" + dateFormat + "')   ";
            }
            sb.Append("SELECT Q.booking_mst_pk,");
            sb.Append("       Q.SPACE_REQUEST_PK,");
            sb.Append("       Q.SPACE_REQUEST_NR,");
            sb.Append("       Q.SR_STATUS,");
            sb.Append("       Q.SHIPMENT_DATE,");
            sb.Append("       Q.POL,");
            sb.Append("       Q.POD,");
            sb.Append("       Q.COMMODITY_GROUP_CODE,");
            sb.Append("       Q.VESSEL_NAME,");
            sb.Append("       Q.CARGO_TYPE,");
            sb.Append("       Q.OFFICE_NAME,");
            sb.Append("       NVL(SUM(Q.CHARGEABLE_WEIGHT), 0) CHARGEABLE_WEIGHT,");
            sb.Append("       NVL(SUM(Q.VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
            sb.Append("       Q.OPERATOR_NAME");
            sb.Append("  FROM (SELECT BOOK.booking_mst_pk,");
            sb.Append("               BOOK.SPACE_REQUEST_PK,");
            sb.Append("               BOOK.SPACE_REQUEST_NR,");
            sb.Append("               BOOK.SR_STATUS,");
            sb.Append("               BOOK.SHIPMENT_DATE,");
            sb.Append("               POL.PORT_ID               POL,");
            sb.Append("               POD.PORT_ID               POD,");
            sb.Append("               CGMT.COMMODITY_GROUP_CODE,");
            sb.Append("               BOOK.VESSEL_NAME || '/' || BOOK.VOYAGE_FLIGHT_NO VESSEL_NAME,");
            sb.Append("               BOOK.CARGO_TYPE,");
            sb.Append("               LMT.OFFICE_NAME,");
            sb.Append("               BKGTRN.WEIGHT_MT          CHARGEABLE_WEIGHT,");
            sb.Append("               BKGTRN.VOLUME_CBM         VOLUME_IN_CBM,");
            sb.Append("               OPR.OPERATOR_NAME");
            sb.Append("          FROM booking_mst_tbl         BOOK,");
            sb.Append("               booking_trn BKGTRN,");
            sb.Append("               PORT_MST_TBL            POL,");
            sb.Append("               PORT_MST_TBL            POD,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("               OPERATOR_MST_TBL        OPR,");
            sb.Append("               USER_MST_TBL            UMT,");
            sb.Append("               LOCATION_MST_TBL        LMT");
            sb.Append("         WHERE BOOK.booking_mst_pk = BKGTRN.booking_mst_fk");
            sb.Append("           AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("           AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("           AND BOOK.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
            sb.Append("           AND BOOK.CARRIER_Mst_Fk = OPR.OPERATOR_MST_PK");
            sb.Append("           AND BOOK.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("           AND LMT.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK");
            sb.Append("           AND BOOK.STATUS = 1");
            sb.Append("           AND BOOK.CARGO_TYPE = 4");
            //    sb.Append("           AND UMT.DEFAULT_LOCATION_FK =" & Session("LOGED_IN_LOC_FK"))
            sb.Append("       " + strCondition + "");
            sb.Append("        ) Q");
            sb.Append(" GROUP BY Q.booking_mst_pk,");
            sb.Append("          Q.SPACE_REQUEST_PK,");
            sb.Append("          Q.SPACE_REQUEST_NR,");
            sb.Append("          Q.SR_STATUS,");
            sb.Append("          Q.SHIPMENT_DATE,");
            sb.Append("          Q.POL,");
            sb.Append("          Q.POD,");
            sb.Append("          Q.COMMODITY_GROUP_CODE,");
            sb.Append("          Q.VESSEL_NAME,");
            sb.Append("          Q.CARGO_TYPE,");
            sb.Append("          Q.OFFICE_NAME,Q.OPERATOR_NAME");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Space Request Report BBC"

        #region "Space Request Report AIR"

        public DataSet FetchSpaceRequestAIR(string BOOKINGPK, string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string FlightNr = "", string ShipmentDate = "", string bkgRefNr = "", Int16 intXBkg = 0, Int16 intCLAgt = 0,
        string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;

            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.CARRIER_MST_FK=" + lblCarrierPK;
            }
            if (Convert.ToInt32(POLPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.PORT_MST_POL_FK = " + POLPK;
            }
            if (Convert.ToInt32(PODPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.PORT_MST_POD_FK = " + PODPK;
            }
            if (Convert.ToInt32(CustomerPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.CUST_CUSTOMER_MST_FK = " + CustomerPK;
            }
            if (Convert.ToInt32(FlightNr) > 0)
            {
                strCondition = strCondition + " AND BOOK.Voyage_Flight_No =  '" + FlightNr + "'";
            }
            if (intXBkg == 1)
            {
                strCondition = strCondition + " And BOOK.CB_AGENT_MST_FK IS NOT NULL";
            }
            if (intCLAgt == 1)
            {
                strCondition = strCondition + " And BOOK.CL_AGENT_MST_FK IS NOT NULL";
            }
            if (!string.IsNullOrEmpty(bkgRefNr))
            {
                strCondition = strCondition + " AND UPPER(BOOK.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (Convert.ToInt32(CommGroupfk) > 0)
            {
                strCondition = strCondition + " AND BOOK.COMMODITY_GROUP_FK =  " + CommGroupfk;
            }
            if (Convert.ToInt32(BOOKINGPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.BOOKING_MST_PK IN (" + BOOKINGPK + " )";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition += " AND UPPER(BOOK.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition += "   AND BOOK.SR_STATUS=" + SrStatus;
            }
            if (!string.IsNullOrEmpty(ShipmentDate))
            {
                strCondition = strCondition + "  AND BOOK.SHIPMENT_DATE = TO_DATE('" + ShipmentDate + "','" + dateFormat + "')   ";
            }
            sb.Append("SELECT Q.BOOKING_MST_PK,");
            sb.Append("       Q.SPACE_REQUEST_PK,");
            sb.Append("       Q.SPACE_REQUEST_NR,");
            sb.Append("       Q.SR_STATUS,");
            sb.Append("       Q.SHIPMENT_DATE,");
            sb.Append("       Q.POL,");
            sb.Append("       Q.POD,");
            sb.Append("       Q.COMMODITY_GROUP_CODE,");
            sb.Append("       Q.VESSEL_NAME,");
            sb.Append("       Q.CARGO_TYPE,");
            sb.Append("       Q.OFFICE_NAME,");
            sb.Append("       NVL(SUM(Q.CHARGEABLE_WEIGHT), 0) CHARGEABLE_WEIGHT,");
            sb.Append("       NVL(SUM(Q.VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
            sb.Append("       Q.OPERATOR_NAME");
            sb.Append("  FROM (SELECT BOOK.BOOKING_MST_PK,");
            sb.Append("               BOOK.SPACE_REQUEST_PK,");
            sb.Append("               BOOK.SPACE_REQUEST_NR,");
            sb.Append("               BOOK.SR_STATUS,");
            sb.Append("               BOOK.SHIPMENT_DATE,");
            sb.Append("               POL.PORT_ID POL,");
            sb.Append("               POD.PORT_ID POD,");
            sb.Append("               CGMT.COMMODITY_GROUP_CODE,");
            sb.Append("               BOOK.Voyage_Flight_No VESSEL_NAME,");
            sb.Append("               0 CARGO_TYPE,");
            sb.Append("               LMT.OFFICE_NAME,");
            sb.Append("               NVL(BOOK.FRT_WEIGHT,BOOK.CHARGEABLE_WEIGHT) CHARGEABLE_WEIGHT,");
            sb.Append("               BOOK.VOLUME_IN_CBM,");
            sb.Append("               AMT.AIRLINE_NAME OPERATOR_NAME");
            sb.Append("          FROM BOOKING_MST_TBL         BOOK,");
            sb.Append("               BOOKING_TRN         BKGTRN,");
            sb.Append("               PORT_MST_TBL            POL,");
            sb.Append("               PORT_MST_TBL            POD,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("               AIRLINE_MST_TBL         AMT,");
            sb.Append("               USER_MST_TBL            UMT,");
            sb.Append("               LOCATION_MST_TBL        LMT");
            sb.Append("         WHERE BOOK.BOOKING_MST_PK = BKGTRN.BOOKING_MST_FK");
            sb.Append("           AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("           AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("           AND BOOK.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
            sb.Append("           AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
            sb.Append("           AND BOOK.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("           AND LMT.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK");
            sb.Append("           AND BOOK.STATUS = 1");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            sb.Append(" " + strCondition + "");
            sb.Append("   ) Q");
            sb.Append(" GROUP BY Q.BOOKING_MST_PK,");
            sb.Append("          Q.SPACE_REQUEST_PK,");
            sb.Append("          Q.SPACE_REQUEST_NR,");
            sb.Append("          Q.SR_STATUS,");
            sb.Append("          Q.SHIPMENT_DATE,");
            sb.Append("          Q.POL,");
            sb.Append("          Q.POD,");
            sb.Append("          Q.COMMODITY_GROUP_CODE,");
            sb.Append("          Q.VESSEL_NAME,");
            sb.Append("          Q.CARGO_TYPE,");
            sb.Append("          Q.OFFICE_NAME,");
            sb.Append("          Q.OPERATOR_NAME");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Space Request Report AIR"

        #region "Get Operator EmailID"

        public DataSet GetOprEmailID(string OprPK, string BizType)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (Convert.ToInt32(BizType) == 2)
            {
                sb.Append("SELECT OPR.OPERATOR_MST_PK, OPRCT.BILL_EMAIL_ID");
                sb.Append("  FROM OPERATOR_MST_TBL OPR, OPERATOR_CONTACT_DTLS OPRCT");
                sb.Append(" WHERE OPR.OPERATOR_MST_PK = OPRCT.OPERATOR_MST_FK");
                sb.Append("  AND OPR.OPERATOR_MST_PK=" + OprPK);
            }
            else
            {
                sb.Append("SELECT AMT.AIRLINE_MST_PK,ACD.BILL_EMAIL_ID");
                sb.Append("  FROM AIRLINE_MST_TBL AMT, AIRLINE_CONTACT_DTLS ACD");
                sb.Append(" WHERE AMT.AIRLINE_MST_PK = ACD.AIRLINE_MST_FK");
                sb.Append("  AND AMT.AIRLINE_MST_PK=" + OprPK);
            }
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Get Operator EmailID"

        #region "Save Space Request Status"

        public ArrayList SaveSRStatus(string BooikgPK, int Status, string BizType)

        {
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;
                var _with57 = updCmdUser;
                _with57.Connection = objWK.MyConnection;
                _with57.Transaction = TRAN;
                _with57.CommandType = CommandType.StoredProcedure;
                _with57.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.SAVE_SPACE_REQUEST_STATUS";
                var _with58 = _with57.Parameters;
                _with58.Add("BOOKING_FK_IN", BooikgPK).Direction = ParameterDirection.Input;
                _with58.Add("SPACE_REQUEST_STATUS_IN", Status).Direction = ParameterDirection.Input;
                intIns = Convert.ToInt16(_with57.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "Save Space Request Status"

        #region "Cancel Space Request Number"

        public ArrayList CancelSRNumber(string BooikgPK, string BizType)
        {
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;
                var _with59 = updCmdUser;
                _with59.Connection = objWK.MyConnection;
                _with59.Transaction = TRAN;
                _with59.CommandType = CommandType.StoredProcedure;
                _with59.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.CANCEL_SPACE_REQUEST";
                _with59.Parameters.Add("BOOKING_FK_IN", BooikgPK).Direction = ParameterDirection.Input;
                intIns = Convert.ToInt16(_with59.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "Cancel Space Request Number"

        #region "Save Send Request Flag "

        public ArrayList SaveRequsetFlag(string BooikgPK, string BizType, string ContainerPK = "")
        {
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;
                var _with60 = updCmdUser;
                _with60.Connection = objWK.MyConnection;
                _with60.Transaction = TRAN;
                _with60.CommandType = CommandType.StoredProcedure;
                _with60.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.SAVE_SPACE_REQUEST_FLAG";
                _with60.Parameters.Add("CONTAINER_TYPE_FK_IN", ContainerPK).Direction = ParameterDirection.Input;
                _with60.Parameters.Add("BOOKING_FK_IN", BooikgPK).Direction = ParameterDirection.Input;
                _with60.Parameters.Add("FLAG", "1").Direction = ParameterDirection.Input;
                intIns = Convert.ToInt16(_with60.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "Save Send Request Flag "

        #region "Generate Space Request Number"

        public ArrayList GenerateSpaceRequestNr(string BKGPK, string SpaceReqNr, string BizType = "2")
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            int Afct = 0;
            objWK.MyCommand.Parameters.Clear();
            try
            {
                var _with61 = objWK.MyCommand;
                _with61.Connection = TRAN.Connection;
                _with61.CommandType = CommandType.StoredProcedure;
                _with61.CommandText = objWK.MyUserName + ".BOOKING_MST_PKG.BKG_SPACE_REQUEST_NO_INS";
                _with61.Parameters.Add("BOOKING_MST_FK_IN", Convert.ToInt64(BKGPK)).Direction = ParameterDirection.Input;
                _with61.Parameters.Add("SPACE_REQUEST_NO_IN", SpaceReqNr).Direction = ParameterDirection.Input;
                _with61.Parameters.Add("SPACE_REQUEST_STATUS_IN", 1).Direction = ParameterDirection.Input;
                _with61.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                Afct = _with61.ExecuteNonQuery();
                if (Afct > 0)
                {
                    arrMessage.Clear();
                    arrMessage.Add("All data saved successfully");
                    TRAN.Commit();
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Clear();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "Generate Space Request Number"

        #region "FetchBlClausesForHBL Function"

        public DataSet FetchBlClausesForHBL(string strBlClause = "", Int32 ClauseTypeFlag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, string PodPk = "0", string CmmPk = "0", long HblPk = 0, string QuotationDt = "", string FormFlag = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            //1-GENERAL
            //2-Quotation
            //3-Booking
            //4-HBL/HAWB
            //5-MBL/MAWB''M.MBL_EXP_TBL_PK
            //6-CAN
            //7-DO
            //8-Invoice
            //9-Invoice To CB Agent
            //10-Invoice To Load Agent
            //11-Invoice To DP Agent
            //12-Customer Contract
            //13-SRR
            //'Quotation
            if (ClauseTypeFlag == 2)
            {
                //If FormFlag = "Sea" Then
                //    strCondition &= vbCrLf & " FROM HBL_BL_CLAUSE_TBL HBL,"
                //    strCondition &= vbCrLf & " BL_CLAUSE_TBL     BLMST,"
                //    strCondition &= vbCrLf & " QUOTATION_MST_TBL QST"
                //    strCondition &= vbCrLf & " WHERE QST.QUOTATION_MST_PK = HBL.HBL_EXP_TBL_FK"
                //    strCondition &= vbCrLf & " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK"
                //    strCondition &= vbCrLf & " AND HBL.CLAUSE_TYPE_FLAG = 2"
                //Else
                //    strCondition &= vbCrLf & " FROM HBL_BL_CLAUSE_TBL HBL,"
                //    strCondition &= vbCrLf & " BL_CLAUSE_TBL     BLMST,"
                //    strCondition &= vbCrLf & " QUOTATION_MST_TBL QAT"
                //    strCondition &= vbCrLf & " WHERE QAT.QUOTATION_AIR_PK = HBL.HBL_EXP_TBL_FK"
                //    strCondition &= vbCrLf & " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK"
                //    strCondition &= vbCrLf & " AND HBL.CLAUSE_TYPE_FLAG = 2"
                //End If
                //Comm by vijay
                strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                strCondition += " BL_CLAUSE_TBL     BLMST,";
                strCondition += " QUOTATION_MST_TBL QST";
                strCondition += " WHERE QST.QUOTATION_MST_PK = HBL.HBL_EXP_TBL_FK";
                strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 2";
                //'3-Booking
            }
            else if (ClauseTypeFlag == 3)
            {
                //If FormFlag = "Sea" Then
                //    strCondition &= vbCrLf & " FROM HBL_BL_CLAUSE_TBL HBL,"
                //    strCondition &= vbCrLf & " BL_CLAUSE_TBL     BLMST,"
                //    strCondition &= vbCrLf & " BOOKING_MST_TBL   B"
                //    strCondition &= vbCrLf & " WHERE B.BOOKING_MST_PK = HBL.HBL_EXP_TBL_FK"
                //    strCondition &= vbCrLf & " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK"
                //    strCondition &= vbCrLf & " AND HBL.CLAUSE_TYPE_FLAG = 3"
                //Else
                //    strCondition &= vbCrLf & " FROM HBL_BL_CLAUSE_TBL HBL,"
                //    strCondition &= vbCrLf & " BL_CLAUSE_TBL     BLMST,"
                //    strCondition &= vbCrLf & " BOOKING_MST_TBL   B"
                //    strCondition &= vbCrLf & " WHERE B.BOOKING_MST_PK = HBL.HBL_EXP_TBL_FK"
                //    strCondition &= vbCrLf & " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK"
                //    strCondition &= vbCrLf & " AND HBL.CLAUSE_TYPE_FLAG = 3"
                //End If
                strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                strCondition += " BL_CLAUSE_TBL     BLMST,";
                strCondition += " BOOKING_MST_TBL   B";
                strCondition += " WHERE B.BOOKING_MST_PK = HBL.HBL_EXP_TBL_FK";
                strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 3";
                //'4-HBL/HAWB
            }
            else if (ClauseTypeFlag == 4)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " HBL_EXP_TBL HET";
                    strCondition += " WHERE HET.HBL_EXP_TBL_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 4";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " HAWB_EXP_TBL HWET";
                    strCondition += " WHERE HWET.HAWB_EXP_TBL_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 4";
                }
                //'5-MBL/MAWB
            }
            else if (ClauseTypeFlag == 5)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " MBL_EXP_TBL M";
                    strCondition += " WHERE M.MBL_EXP_TBL_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 5";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " MAWB_EXP_TBL M";
                    strCondition += " WHERE M.MAWB_EXP_TBL_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 5";
                }
                //'6-CAN
            }
            else if (ClauseTypeFlag == 6)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " JOB_CARD_TRN JC";
                    strCondition += " WHERE JC.JOB_CARD_SEA_IMP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 6";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " JOB_CARD_TRN JC";
                    strCondition += " WHERE JC.JOB_CARD_AIR_IMP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 6";
                }

                //'7-DO
            }
            else if (ClauseTypeFlag == 7)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " DELIVERY_ORDER_MST_TBL D";
                    strCondition += " WHERE D.DELIVERY_ORDER_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 7";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " DELIVERY_ORDER_MST_TBL D";
                    strCondition += " WHERE D.DELIVERY_ORDER_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 7";
                }
                //'8-Invoice
            }
            else if (ClauseTypeFlag == 8)
            {
                strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                strCondition += " BL_CLAUSE_TBL     BLMST,";
                strCondition += " CONSOL_INVOICE_TBL C";
                strCondition += " WHERE C.CONSOL_INVOICE_PK = HBL.HBL_EXP_TBL_FK";
                strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 8";

                //'9-Invoice To CB Agent
            }
            else if (ClauseTypeFlag == 9)
            {
                if (FormFlag == "SeaCBExp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_SEA_EXP_TBL I";
                    strCondition += " WHERE I.INV_AGENT_SEA_EXP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 9";
                }
                else if (FormFlag == "SeaCBImp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_SEA_IMP_TBL I";
                    strCondition += " WHERE I.INV_AGENT_SEA_IMP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 9";
                }
                else if (FormFlag == "AirCBExp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_AIR_EXP_TBL I";
                    strCondition += " WHERE I.INV_AGENT_AIR_EXP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 9";
                }
                else if (FormFlag == "AirCBImp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_AIR_IMP_TBL I";
                    strCondition += " WHERE I.INV_AGENT_AIR_IMP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 9";
                }
                //'10-Invoice To  Load Agent IMP
            }
            else if (ClauseTypeFlag == 10)
            {
                if (FormFlag == "SeaLAImp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_SEA_IMP_TBL I";
                    strCondition += " WHERE I.INV_AGENT_SEA_IMP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 10";
                }
                else if (FormFlag == "AirLAImp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_AIR_IMP_TBL I";
                    strCondition += " WHERE I.INV_AGENT_AIR_IMP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 10";
                }
                //'11-Invoice To DP Agent
            }
            else if (ClauseTypeFlag == 11)
            {
                //'Sea
                if (FormFlag == "SeaDPExp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_SEA_EXP_TBL I";
                    strCondition += " WHERE I.INV_AGENT_SEA_EXP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 11";

                    //'11-AirDPExp
                }
                else if (FormFlag == "AirDPExp")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " INV_AGENT_AIR_EXP_TBL I";
                    strCondition += " WHERE I.INV_AGENT_AIR_EXP_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 11";
                }

                //'12-Customer Contract
            }
            else if (ClauseTypeFlag == 12)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " CONT_CUST_SEA_TBL C";
                    strCondition += " WHERE C.CONT_CUST_SEA_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 12";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " CONT_CUST_AIR_TBL C";
                    strCondition += " WHERE C.CONT_CUST_AIR_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 12";
                }
                //'13-SRR
            }
            else if (ClauseTypeFlag == 13)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " SRR_SEA_TBL S";
                    strCondition += " WHERE S.SRR_SEA_PK = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 13";
                }
                else
                {
                    strCondition += " FROM HBL_BL_CLAUSE_TBL HBL,";
                    strCondition += " BL_CLAUSE_TBL     BLMST,";
                    strCondition += " SRR_AIR_TBL S";
                    strCondition += " WHERE S.SRR_AIR_PK  = HBL.HBL_EXP_TBL_FK";
                    strCondition += " AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK";
                    strCondition += " AND HBL.CLAUSE_TYPE_FLAG = 13";
                }
            }

            //'2-Quotation
            if (HblPk > 0 & ClauseTypeFlag == 2)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND QST.QUOTATION_SEA_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND QAT.QUOTATION_AIR_PK = " + HblPk;
                }
                //'3-Booking
            }
            else if (HblPk > 0 & ClauseTypeFlag == 3)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND B.BOOKING_MST_PK  = " + HblPk;
                }
                else
                {
                    strCondition += "  AND B.BOOKING_MST_PK  = " + HblPk;
                }
                //'4-HBL/HAWB
            }
            else if (HblPk > 0 & ClauseTypeFlag == 4)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND HET.HBL_EXP_TBL_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND HWET.HAWB_EXP_TBL_PK = " + HblPk;
                }
                //'5-MBL/MAWB
            }
            else if (HblPk > 0 & ClauseTypeFlag == 5)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND M.MBL_EXP_TBL_PK  = " + HblPk;
                }
                else
                {
                    strCondition += "  AND M.MAWB_EXP_TBL_PK  = " + HblPk;
                }
                //'6-CAN
            }
            else if (HblPk > 0 & ClauseTypeFlag == 6)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND JC.JOB_CARD_SEA_IMP_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND JC.JOB_CARD_AIR_IMP_PK = " + HblPk;
                }
                //'7-CAN
            }
            else if (HblPk > 0 & ClauseTypeFlag == 7)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND D.DELIVERY_ORDER_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND D.DELIVERY_ORDER_PK = " + HblPk;
                }

                //'8-Invoice
            }
            else if (HblPk > 0 & ClauseTypeFlag == 8)
            {
                strCondition += "  AND C.CONSOL_INVOICE_PK = " + HblPk;
            }
            else if (HblPk > 0 & ClauseTypeFlag == 9)
            {
                if (FormFlag == "SeaCBExp")
                {
                    strCondition += "  AND I.INV_AGENT_SEA_EXP_PK = " + HblPk;
                }
                else if (FormFlag == "SeaCBImp")
                {
                    strCondition += "  AND I.INV_AGENT_SEA_IMP_PK = " + HblPk;
                }
                else if (FormFlag == "AirCBExp")
                {
                    strCondition += "  AND I.INV_AGENT_AIR_EXP_PK = " + HblPk;
                }
                else if (FormFlag == "AirCBImp")
                {
                    strCondition += "  AND I.INV_AGENT_AIR_IMP_PK = " + HblPk;
                }
            }
            else if (HblPk > 0 & ClauseTypeFlag == 10)
            {
                if (FormFlag == "SeaLAImp")
                {
                    strCondition += "  AND I.INV_AGENT_SEA_IMP_PK = " + HblPk;
                }
                else if (FormFlag == "AirLAImp")
                {
                    strCondition += "  AND I.INV_AGENT_AIR_IMP_PK = " + HblPk;
                }
            }
            else if (HblPk > 0 & ClauseTypeFlag == 11)
            {
                if (FormFlag == "SeaDPExp")
                {
                    strCondition += "  AND I.INV_AGENT_SEA_EXP_PK = " + HblPk;
                }
                else if (FormFlag == "AirDPExp")
                {
                    strCondition += "  AND I.INV_AGENT_AIR_EXP_PK = " + HblPk;
                }

                //'12-Customer Contarct
            }
            else if (HblPk > 0 & ClauseTypeFlag == 12)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND C.CONT_CUST_SEA_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND C.CONT_CUST_AIR_PK = " + HblPk;
                }
                //'13-SRR
            }
            else if (HblPk > 0 & ClauseTypeFlag == 13)
            {
                if (FormFlag == "Sea")
                {
                    strCondition += "  AND S.SRR_SEA_PK = " + HblPk;
                }
                else
                {
                    strCondition += "  AND S.SRR_AIR_PK = " + HblPk;
                }
            }
            if (strBlClause != "FormLookup" & !string.IsNullOrEmpty(strBlClause))
            {
                if (strBlClause.Trim().Length > 0)
                {
                    strCondition += " AND UPPER(HBL.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'";
                }
            }
            ///'''''''''''
            sb.Append("SELECT COUNT(*)");
            sb.Append("  FROM (SELECT ROWNUM SR_NO, Q.*");
            sb.Append("          FROM (SELECT HBL.HBL_BL_CLAUSE_TBL_PK,");
            sb.Append("                        HBL.REFERENCE_NR,");
            sb.Append("                        HBL.BL_DESCRIPTION,");
            sb.Append("                        HBL.PRIORITY,");
            sb.Append("                        HBL.BL_CLAUSE_FK         \"PK\",");
            sb.Append("                        CASE WHEN HBL.ACTIVE_FLAG = 1 THEN ");
            sb.Append("                        'true' ELSE 'false' END \"sel\"");
            sb.Append("" + strCondition + "");
            sb.Append("                ");
            if (string.IsNullOrEmpty(strBlClause))
            {
                //' For Print Report not to display all the clause
                sb.Append("   AND HBL.ACTIVE_FLAG = 1");
            }
            else
            {
                sb.Append("                UNION");
                sb.Append("                ");
                sb.Append("                SELECT 0 HBL_BL_CLAUSE_TBL_PK,");
                sb.Append("                       BLCT.REFERENCE_NR,");
                sb.Append("                       BLCT.BL_DESCRIPTION,");
                sb.Append("                       BLCT.PRIORITY,");
                sb.Append("                       BLCT.BL_CLAUSE_PK   \"PK\",");
                sb.Append("                       'false'             \"sel\"");
                sb.Append("                  FROM BL_CLAUSE_TBL BLCT");
                sb.Append("                 WHERE BLCT.ACTIVE_FLAG = '1'");
                if (!string.IsNullOrEmpty(QuotationDt) & QuotationDt != "null")
                {
                    sb.Append("                AND TO_DATE('" + QuotationDt + "','dd/MM/yyyy') BETWEEN BLCT.VALID_FROM AND BLCT.VALID_TO ");
                }
                if (strBlClause != "FormLookup" & !string.IsNullOrEmpty(strBlClause))
                {
                    if (strBlClause.Trim().Length > 0)
                    {
                        sb.Append("               AND UPPER(BLCT.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'");
                    }
                }
                sb.Append("                   AND (BLCT.CLAUSES_TYPE_MST_FK = 1 ");
                sb.Append("                       OR BLCT.CLAUSES_TYPE_MST_FK = " + ClauseTypeFlag + ")");
                sb.Append("                   AND (BLCT.BL_CLAUSE_PK IN");
                sb.Append("                       (SELECT DISTINCT BL.BL_CLAUSE_FK");
                sb.Append("                           FROM BL_CLAUSE_TRN BL");
                sb.Append("                          WHERE BL.PORT_MST_FK IN (" + PodPk + ")");
                sb.Append("                            AND BL.BL_CLAUSE_FK NOT IN");
                sb.Append("                                (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                                   FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                                  WHERE H.HBL_EXP_TBL_FK = " + HblPk + ")) OR");
                sb.Append("                       BLCT.BL_CLAUSE_PK IN");
                sb.Append("                       (SELECT DISTINCT BC.CLAUSE_MST_FK");
                sb.Append("                           FROM BL_CLAUSE_COMM_TRN BC");
                sb.Append("                          WHERE BC.COMMODITY_MST_FK IN (" + CmmPk + ")");
                sb.Append("                            AND BC.CLAUSE_MST_FK NOT IN");
                sb.Append("                                (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                                   FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                                  WHERE H.HBL_EXP_TBL_FK = " + HblPk + ")) ");
                sb.Append("                     AND(BLCT.CLAUSES_TYPE_MST_FK = 1 OR");
                sb.Append("                       BLCT.CLAUSES_TYPE_MST_FK = " + ClauseTypeFlag + ") ");
                sb.Append("                     AND BLCT.BL_CLAUSE_PK NOT IN");
                sb.Append("                       (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                           FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                          WHERE H.HBL_EXP_TBL_FK = " + HblPk + "))ORDER BY \"sel\", BL_DESCRIPTION ");
            }
            sb.Append("         ) Q)");
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            TotalPage = TotalRecords / M_MasterPageSize;

            if (TotalRecords % M_MasterPageSize != 0)
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

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            sb.Length = 0;
            sb.Append("SELECT * ");
            sb.Append("  FROM (SELECT ROWNUM SR_NO, Q.*");
            sb.Append("          FROM (SELECT HBL.HBL_BL_CLAUSE_TBL_PK,");
            sb.Append("                        HBL.REFERENCE_NR,");
            sb.Append("                        HBL.BL_DESCRIPTION,");
            sb.Append("                        HBL.PRIORITY,");
            sb.Append("                        HBL.BL_CLAUSE_FK         \"PK\",");
            //sb.Append("                        HBL.ACTIVE_FLAG          ""sel""")
            sb.Append("                        CASE WHEN HBL.ACTIVE_FLAG = 1 THEN ");
            sb.Append("                        'true' ELSE 'false' END \"sel\"");
            sb.Append("" + strCondition + "");
            sb.Append("                ");
            if (string.IsNullOrEmpty(strBlClause))
            {
                //' For Print Report not to display all the clause
                sb.Append("   AND HBL.ACTIVE_FLAG = 1");
            }
            else
            {
                sb.Append("                UNION");
                sb.Append("                ");
                sb.Append("                SELECT 0 HBL_BL_CLAUSE_TBL_PK,");
                sb.Append("                       BLCT.REFERENCE_NR,");
                sb.Append("                       BLCT.BL_DESCRIPTION,");
                sb.Append("                       BLCT.PRIORITY,");
                sb.Append("                       BLCT.BL_CLAUSE_PK   \"PK\",");
                sb.Append("                       'false'             \"sel\"");
                sb.Append("                  FROM BL_CLAUSE_TBL BLCT");
                sb.Append("                 WHERE BLCT.ACTIVE_FLAG = '1'");
                if (!string.IsNullOrEmpty(QuotationDt) & QuotationDt != "null")
                {
                    sb.Append("                AND TO_DATE('" + QuotationDt + "','dd/MM/yyyy') BETWEEN BLCT.VALID_FROM AND BLCT.VALID_TO ");
                }
                if (strBlClause != "FormLookup" & !string.IsNullOrEmpty(strBlClause))
                {
                    if (strBlClause.Trim().Length > 0)
                    {
                        sb.Append("               AND UPPER(BLCT.BL_DESCRIPTION) LIKE '" + strBlClause.ToUpper().Replace("'", "''") + "%'");
                    }
                }
                sb.Append("                   AND (BLCT.CLAUSES_TYPE_MST_FK = 1 ");
                sb.Append("                       OR BLCT.CLAUSES_TYPE_MST_FK = " + ClauseTypeFlag + ")");
                sb.Append("                   AND (BLCT.BL_CLAUSE_PK IN");
                sb.Append("                       (SELECT DISTINCT BL.BL_CLAUSE_FK");
                sb.Append("                           FROM BL_CLAUSE_TRN BL");
                sb.Append("                          WHERE BL.PORT_MST_FK IN (" + PodPk + ")");
                sb.Append("                            AND BL.BL_CLAUSE_FK NOT IN");
                sb.Append("                                (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                                   FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                                  WHERE H.HBL_EXP_TBL_FK = " + HblPk + ")) OR");
                sb.Append("                       BLCT.BL_CLAUSE_PK IN");
                sb.Append("                       (SELECT DISTINCT BC.CLAUSE_MST_FK");
                sb.Append("                           FROM BL_CLAUSE_COMM_TRN BC");
                sb.Append("                          WHERE BC.COMMODITY_MST_FK IN (" + CmmPk + ")");
                sb.Append("                            AND BC.CLAUSE_MST_FK NOT IN");
                sb.Append("                                (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                                   FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                                  WHERE H.HBL_EXP_TBL_FK = " + HblPk + ")) ");
                sb.Append("                     AND(BLCT.CLAUSES_TYPE_MST_FK = 1 OR");
                sb.Append("                       BLCT.CLAUSES_TYPE_MST_FK = " + ClauseTypeFlag + ") ");
                sb.Append("                     AND BLCT.BL_CLAUSE_PK NOT IN");
                sb.Append("                       (SELECT DISTINCT H.BL_CLAUSE_FK");
                sb.Append("                           FROM HBL_BL_CLAUSE_TBL H");
                sb.Append("                          WHERE H.HBL_EXP_TBL_FK = " + HblPk + "))ORDER BY \"sel\", BL_DESCRIPTION ");
            }
            sb.Append("      ) Q) WHERE SR_NO  BETWEEN " + start + " AND " + last);

            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "FetchBlClausesForHBL Function"

        //*********************************************************************************************************

        #region "AIR Related Fetch"

        #region "Fetch Grid Values Air"

        public DataSet FetchGridValuesAir(DataSet dsGrid, Int32 intCustomerPK = 0, Int32 intQuotationPK = 0, short intIsKGS = 0, short intSRateStatus = 0, short intCContractStatus = 0, short intSRRContractStatus = 0, short intOTariffStatus = 0, short intGTariffStatus = 0, string strPOL = "",
        string strPOD = "", Int16 intCommodityPK = 0, string strSDate = "", string strChargeWeight = "0", Int32 intContainerPk = 0, Int32 intSpotRatePk = 0, Int32 intNoOfContainers = 0, Int32 CustContractPK = 0, Int32 SplitBooking = 0, bool isAgentTariff = false,
        Int32 DPAgentMstFk = 0)
        {
            try
            {
                DataTable dtMain = new DataTable();
                DataTable dtOCharge = new DataTable();
                DataTable dtChild = new DataTable();
                int Int_I = 0;
                string strRefNo = null;
                DataRow[] drRefNo = null;
                dsGrid = new DataSet();
                strChargeWeight = strChargeWeight.Replace(",", "");
                if (intQuotationPK == 0 & intSpotRatePk == 0)
                {
                    if (intIsKGS == 1)
                    {
                        dtMain = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, intSRateStatus, intCContractStatus, intSRRContractStatus,
                        intOTariffStatus, intGTariffStatus, strChargeWeight, 1, 0, 0, 0, CustContractPK, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                        dtChild = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, intSRateStatus, intCContractStatus, intSRRContractStatus,
                        intOTariffStatus, intGTariffStatus, strChargeWeight, 2, 0, 0, 0, CustContractPK, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                    }
                    else
                    {
                        dtMain = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, intSRateStatus, intCContractStatus, intSRRContractStatus,
                        intOTariffStatus, intGTariffStatus, strChargeWeight, 1, intContainerPk, 0, intNoOfContainers, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                        dtChild = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, intSRateStatus, intCContractStatus, intSRRContractStatus,
                        intOTariffStatus, intGTariffStatus, strChargeWeight, 2, intContainerPk, 0, intNoOfContainers, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                    }
                }
                else if (intSpotRatePk == 0)
                {
                    if (intIsKGS == 1)
                    {
                        dtMain = FunRetriveData(intCustomerPK, intQuotationPK, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 1, 0, 0, 0, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                        dtChild = FunRetriveData(intCustomerPK, intQuotationPK, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 2, 0, 0, 0, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                    }
                    else
                    {
                        dtMain = FunRetriveData(intCustomerPK, intQuotationPK, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 1, intContainerPk, 0, intNoOfContainers, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                        dtChild = FunRetriveData(intCustomerPK, intQuotationPK, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 2, intContainerPk, 0, intNoOfContainers, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                    }
                }
                else
                {
                    if (intIsKGS == 1)
                    {
                        dtMain = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 1, 0, intSpotRatePk, 0, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                        dtChild = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 2, 0, intSpotRatePk, 0, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                    }
                    else
                    {
                        dtMain = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 1, intContainerPk, intSpotRatePk, intNoOfContainers, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                        dtChild = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 2, intContainerPk, intSpotRatePk, intNoOfContainers, 0, SplitBooking, isAgentTariff,
                        DPAgentMstFk);
                    }
                }
                if (!dtChild.Columns.Contains("FreightPK"))
                {
                    dtChild.Columns.Add("FreightPK");
                }

                if (dtMain.Rows.Count > 0)
                {
                    FetchOFreights(dtMain, intQuotationPK, intSRateStatus, intCContractStatus, intSRRContractStatus, intOTariffStatus, intGTariffStatus, intSpotRatePk);
                }

                if (dtChild.Rows.Count > 0)
                {
                    ComputeOFreightCharge(dtChild, strSDate);
                }
                dsGrid.Tables.Add(dtMain);
                dsGrid.Tables.Add(dtChild);
                if (dsGrid.Tables.Count > 1)
                {
                    if (dsGrid.Tables[1].Rows.Count > 0)
                    {
                        if (SplitBooking == 1)
                        {
                            drRefNo = dsGrid.Tables[1].Select(" REFNO IS NOT NULL ");
                            for (Int_I = 0; Int_I <= dsGrid.Tables[1].Rows.Count - 1; Int_I++)
                            {
                                dsGrid.Tables[1].Rows[Int_I]["REFNO"] = drRefNo[0][1];
                            }
                        }
                        DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] { dsGrid.Tables[0].Columns["REFNO"] }, new DataColumn[] { dsGrid.Tables[1].Columns["REFNO"] });
                        dsGrid.Relations.Clear();
                        dsGrid.Relations.Add(rel);
                    }
                }
                return dsGrid;
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

        #endregion "Fetch Grid Values Air"

        #region "Procedure Invoke to Retrive Header and Freight Details"

        public DataTable FunRetriveData(Int32 intCustomerPK = 0, Int32 intQuotationPK = 0, short intIsKGS = 0, string strPOL = "", string strPOD = "", Int16 intCommodityPK = 0, string strSDate = "", short intSRateStatus = 0, short intCContractStatus = 0, short intSRRContractStatus = 0,
        short intOTariffStatus = 0, short intGTariffStatus = 0, string strChargeWeightNo = "", short intFetchStatus = 0, Int32 intContainerPk = 0, Int32 intSpotRatePk = 0, Int32 intNoOfContainers = 0, Int32 CustContractPk = 0, Int32 SplitBooking = 0, bool isAgentTariff = false,
        Int32 DPAgentMstFk = 0)
        {
            int result2 = 0;
            OracleDataAdapter Da = new OracleDataAdapter();
            DataTable dt = null;
            WorkFlow objWFT = new WorkFlow();
            objWFT.MyCommand.CommandType = CommandType.StoredProcedure;
            objWFT.MyCommand.Parameters.Clear();
            if (CustContractPk != 0)
            {
                intCContractStatus = 1;
            }
            int SplitBkgFlag = 0;
            if (SplitBooking == 1)
            {
                SplitBkgFlag = 1;
            }
            else
            {
                SplitBkgFlag = 0;
            }
            try
            {
                var _with62 = objWFT.MyCommand.Parameters;
                _with62.Add("CUSTOMER_PK_IN", intCustomerPK).Direction = ParameterDirection.Input;
                _with62.Add("QUOTATION_PK_IN", intQuotationPK).Direction = ParameterDirection.Input;
                _with62.Add("IS_KGS_IN", intIsKGS).Direction = ParameterDirection.Input;
                _with62.Add("POL_IN", strPOL).Direction = ParameterDirection.Input;
                _with62.Add("POD_IN", strPOD).Direction = ParameterDirection.Input;
                _with62.Add("COMMODITY_PK_IN", intCommodityPK).Direction = ParameterDirection.Input;
                _with62.Add("S_DATE_IN", ((strSDate == "") ? DateTime.MinValue : Convert.ToDateTime(strSDate))).Direction = ParameterDirection.Input;
                _with62.Add("SPOT_RATE_STATUS_IN", intSRateStatus).Direction = ParameterDirection.Input;
                _with62.Add("CUST_CONTRACT_STATUS_IN", intCContractStatus).Direction = ParameterDirection.Input;
                _with62.Add("SRR_CONTRACT_STATUS_IN", intSRRContractStatus).Direction = ParameterDirection.Input;
                _with62.Add("OPR_TARIFF_STATUS_IN", intOTariffStatus).Direction = ParameterDirection.Input;
                _with62.Add("GEN_TARIFF_STATUS_IN", intGTariffStatus).Direction = ParameterDirection.Input;
                if (!string.IsNullOrEmpty(strChargeWeightNo))
                    result2 = Convert.ToInt32(strChargeWeightNo);
                _with62.Add("CHARGE_WT_NO_IN", (string.IsNullOrEmpty(strChargeWeightNo) ? "0" : (result2 > 0 ? strChargeWeightNo : "0"))).Direction = ParameterDirection.Input;
                _with62.Add("CONTAINER_PK_IN", intContainerPk).Direction = ParameterDirection.Input;
                _with62.Add("SPOT_RATE_PK_IN", intSpotRatePk).Direction = ParameterDirection.Input;
                _with62.Add("NO_OF_CONTAINERS", intNoOfContainers).Direction = ParameterDirection.Input;
                if (intFetchStatus == 1)
                {
                    _with62.Add("CURRPK", Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                    _with62.Add("SPLIT_CARGO_FLAG", SplitBkgFlag).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with62.Add("CURRPK", Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                    _with62.Add("SPLIT_CARGO_FLAG", SplitBkgFlag).Direction = ParameterDirection.Input;
                }
                _with62.Add("IS_AGENT_TARIFF_IN", (isAgentTariff ? 1 : 0)).Direction = ParameterDirection.Input;
                _with62.Add("AGENT_MST_FK_IN", DPAgentMstFk).Direction = ParameterDirection.Input;
                _with62.Add("RES_CURSOR_ALL", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                //.Add("CURRPK", Session("CURRENCY_MST_PK")).Direction = ParameterDirection.Input
                objWFT.MyCommand.Parameters.Add("CUST_CONTRACT_PK_IN", CustContractPk).Direction = ParameterDirection.Input;

                if (intFetchStatus == 1)
                {
                    dt = objWFT.GetDataTable("BOOKING_AIR_FETCH_PKG", "FETCH_DATA_HEADER");
                }
                else
                {
                    dt = objWFT.GetDataTable("BOOKING_AIR_FETCH_PKG", "FETCH_DATA_FREIGHT");
                }
                return dt;
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

        #endregion "Procedure Invoke to Retrive Header and Freight Details"

        #region "Main Fetch OFreights"

        private void FetchOFreights(DataTable dtMain, Int32 intQuotationPK = 0, short intSRateStatus = 0, short intCContractStatus = 0, short intSRRContractStatus = 0, short intOTariffStatus = 0, short intGTariffStatus = 0, Int32 intSpotRatePk = 0)
        {
            //TRNTYPEPK
            try
            {
                Int32 rowCnt = default(Int32);
                DataTable dtOFreights = new DataTable();
                for (rowCnt = 0; rowCnt <= dtMain.Rows.Count - 1; rowCnt++)
                {
                    if (!(intQuotationPK == 0))
                    {
                        FetchDTQOFreights(dtOFreights, dtMain.Rows[rowCnt]["TRNTYPEPK"].ToString());
                    }
                    else if (intSRateStatus == 1 | !(intSpotRatePk == 0) & intQuotationPK == 0)
                    {
                        FetchDTSOFreights(dtOFreights, dtMain.Rows[rowCnt]["TRNTYPEPK"].ToString());
                    }
                    else if (intCContractStatus == 1 & intQuotationPK == 0)
                    {
                        FetchDTCCOFreights(dtOFreights, dtMain.Rows[rowCnt]["TRNTYPEPK"].ToString());
                    }
                    else if (intSRRContractStatus == 1 & intQuotationPK == 0)
                    {
                        FetchDTSRROFreights(dtOFreights, dtMain.Rows[rowCnt]["TRNTYPEPK"].ToString());
                    }
                    else if (intOTariffStatus == 1 | intGTariffStatus == 1 & intQuotationPK == 0)
                    {
                        FetchDTAGOFreights(dtOFreights, dtMain.Rows[rowCnt]["TRNTYPEPK"].ToString());
                    }
                    if (dtOFreights.Rows.Count > 0)
                    {
                        Cls_FlatRateFreights Cls_FlatRateFreights = new Cls_FlatRateFreights();
                        dtMain.Rows[rowCnt]["OCHARGES"] = Cls_FlatRateFreights.GetOTHstring(dtOFreights, 0, 1, 2, 3, 4, "-1", 0, 0, new DataTable(), 0, 0);
                    }
                }
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

        private void FetchDTQOFreights(DataTable dtOFreights, string strQPk)
        {
            //Fetch Quotation Other Freights
            try
            {
                System.Text.StringBuilder strBuilder1 = new System.Text.StringBuilder();
                System.Text.StringBuilder strSql = new System.Text.StringBuilder();
                Int16 QType = default(Int16);
                //strSql.Append("select a.quotation_type from quotation_mst_tbl a where a.quotation_mst_pk=")
                //strSql.Append("( select t.quotation_mst_fk from QUOTATION_TRN t where t.quote_trn_pk=" & strQPk & ")")
                //QType = (New WorkFlow).ExecuteScaler(strSql.ToString())
                strBuilder1.Append("SELECT ");
                strBuilder1.Append("QTAOC.FREIGHT_ELEMENT_MST_FK, ");
                strBuilder1.Append("QTAOC.CURRENCY_MST_FK, ");
                strBuilder1.Append("QTAOC.CHARGE_BASIS, ");
                strBuilder1.Append("QTAOC.BASIS_RATE, ");
                strBuilder1.Append("'-1' PK, ");
                strBuilder1.Append("QTAOC.FREIGHT_TYPE PYMT_TYPE ");
                strBuilder1.Append("FROM QUOTATION_OTHER_FREIGHT_TRN  QTAOC ");

                //If QType = 0 Then
                //    strBuilder1.Append("QUOT_AIR_OTH_CHRG  QTAOC ")
                //Else
                //    strBuilder1.Append("QUOTATION_TRN_AIR_OTH_CHRG QTAOC ")
                //End If

                strBuilder1.Append("WHERE QTAOC.QUOTATION_DTL_FK= " + strQPk);
                dtOFreights = (new WorkFlow()).GetDataTable(strBuilder1.ToString());
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

        private void FetchDTSOFreights(DataTable dtOFreights, string strSRatePk)
        {
            //Fetch Spot Rate other Freights
            try
            {
                System.Text.StringBuilder strBuilder1 = new System.Text.StringBuilder();
                strBuilder1.Append("SELECT ");
                strBuilder1.Append("RSAOC.FREIGHT_ELEMENT_MST_FK, ");
                strBuilder1.Append("RSAOC.CURRENCY_MST_FK, ");
                strBuilder1.Append("RSAOC.CHARGE_BASIS, ");
                strBuilder1.Append("RSAOC.APPROVED_RATE, ");
                strBuilder1.Append("'-1' PK, ");
                strBuilder1.Append("1 PYMT_TYPE ");
                strBuilder1.Append("FROM ");
                strBuilder1.Append("RFQ_SPOT_AIR_OTH_CHRG RSAOC ");
                strBuilder1.Append("WHERE RSAOC.RFQ_SPOT_AIR_TRN_FK= ");
                strBuilder1.Append(strSRatePk);
                dtOFreights = (new WorkFlow()).GetDataTable(strBuilder1.ToString());
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

        private void FetchDTCCOFreights(DataTable dtOFreights, string strCCPk)
        {
            //Fetch Customer Contract Other Freights
            try
            {
                System.Text.StringBuilder strBuilder1 = new System.Text.StringBuilder();
                strBuilder1.Append("SELECT ");
                strBuilder1.Append("CCAOC.FREIGHT_ELEMENT_MST_FK, ");
                strBuilder1.Append("CCAOC.CURRENCY_MST_FK, ");
                strBuilder1.Append("CCAOC.CHARGE_BASIS, ");
                strBuilder1.Append("CCAOC.APPROVED_RATE, ");
                strBuilder1.Append("'-1' PK, ");
                strBuilder1.Append("1 PYMT_TYPE ");
                strBuilder1.Append("FROM ");
                strBuilder1.Append("CONT_CUST_AIR_OTH_CHRG CCAOC ");
                strBuilder1.Append("WHERE CCAOC.CONT_CUST_TRN_AIR_FK= ");
                strBuilder1.Append(strCCPk);
                dtOFreights = (new WorkFlow()).GetDataTable(strBuilder1.ToString());
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

        private void FetchDTSRROFreights(DataTable dtOFreights, string strSRRPk)
        {
            //Fetch Customer Contract Other Freights
            try
            {
                System.Text.StringBuilder strBuilder1 = new System.Text.StringBuilder();
                strBuilder1.Append("SELECT ");
                strBuilder1.Append("SRRAOC.FREIGHT_ELEMENT_MST_FK, ");
                strBuilder1.Append("SRRAOC.CURRENCY_MST_FK, ");
                strBuilder1.Append("SRRAOC.CHARGE_BASIS, ");
                strBuilder1.Append("SRRAOC.APPROVED_RATE, ");
                strBuilder1.Append("'-1' PK, ");
                strBuilder1.Append("1 PYMT_TYPE ");
                strBuilder1.Append("FROM ");
                strBuilder1.Append("SRR_AIR_OTH_CHRG SRRAOC ");
                strBuilder1.Append("WHERE SRRAOC.SRR_TRN_AIR_FK= ");
                strBuilder1.Append(strSRRPk);
                dtOFreights = (new WorkFlow()).GetDataTable(strBuilder1.ToString());
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

        private void FetchDTAGOFreights(DataTable dtOFreights, string strAGPk)
        {
            //Fetch Air Tariff and Gen Tariff Other Freights
            try
            {
                System.Text.StringBuilder strBuilder1 = new System.Text.StringBuilder();
                strBuilder1.Append("SELECT ");
                strBuilder1.Append("TTAOC.FREIGHT_ELEMENT_MST_FK, ");
                strBuilder1.Append("TTAOC.CURRENCY_MST_FK, ");
                strBuilder1.Append("TTAOC.CHARGE_BASIS, ");
                strBuilder1.Append("TTAOC.TARIFF_RATE, ");
                strBuilder1.Append("'-1' PK, ");
                strBuilder1.Append("1 PYMT_TYPE ");
                strBuilder1.Append("FROM ");
                strBuilder1.Append("TARIFF_TRN_AIR_OTH_CHRG TTAOC ");
                strBuilder1.Append("WHERE TTAOC.TARIFF_TRN_AIR_FK= ");
                strBuilder1.Append(strAGPk);
                dtOFreights = (new WorkFlow()).GetDataTable(strBuilder1.ToString());
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

        #endregion "Main Fetch OFreights"

        #region "Compute the other freight charges based on AFC for % type Freights"

        private void ComputeOFreightCharge(DataTable dtChild, string strSDate)
        {
            try
            {
                string strFCurrPk = null;
                string strTCurrPk = null;
                double dblExRate = 0;
                double dblAFCBkgRate = 0;
                double dblBasisRate = 0;
                double dblBkgRate = 0;
                if (dtChild.Rows.Count > 0)
                {
                    for (int i = 0; i <= dtChild.Rows.Count - 1; i++)
                    {
                        if (getDefault(dtChild.Rows[i][13], "") == "1" | getDefault(dtChild.Rows[i][13], "") == "%")
                        {
                            for (int j = 0; j <= dtChild.Rows.Count - 1; j++)
                            {
                                if (Convert.ToString(dtChild.Rows[j][9]).ToUpper() == "AFC")
                                {
                                    if ((dtChild.Rows[j][9] != null) & !string.IsNullOrEmpty(dtChild.Rows[j][9].ToString()) & !string.IsNullOrEmpty(dtChild.Rows[j][9].ToString()) & (dtChild.Rows[i][13] != null) & !string.IsNullOrEmpty(dtChild.Rows[i][13].ToString()) & !string.IsNullOrEmpty(dtChild.Rows[i][13].ToString()))
                                    {
                                        strFCurrPk = Convert.ToString(dtChild.Rows[j][11]);
                                        strTCurrPk = Convert.ToString(dtChild.Rows[i][11]);
                                        dblExRate = Convert.ToDouble(getExRate(strFCurrPk, strTCurrPk, strSDate));
                                        dblAFCBkgRate = Convert.ToInt32((string.IsNullOrEmpty(dtChild.Rows[j][16].ToString()) ? 0 : dtChild.Rows[j][16]));
                                        dblBasisRate = Convert.ToInt32(getDefault(dtChild.Rows[i][14], 0));
                                        dblBkgRate = (dblAFCBkgRate * dblExRate) * (dblBasisRate / 100);
                                        dtChild.Rows[i][16] = dblBkgRate;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private object getExRate(string strFCurrPk, string strTCurrPk, string strSDate)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strBuilder.Append("SELECT ");
            strBuilder.Append("GET_EX_RATE('" + strFCurrPk + "','");
            strBuilder.Append(strTCurrPk + "',to_date('" + strSDate + "','" + dateFormat + "')) from dual ");
            try
            {
                return objWF.ExecuteScaler(strBuilder.ToString());
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

        #endregion "Compute the other freight charges based on AFC for % type Freights"

        #region "Fetch MAwb Nr If Spot Rate Select"

        public DataSet Fetch_MawbNr_SpotRate(string AirbillRefNr, string AirwayPk, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("select a.airway_bill_no");
                strQuery.Append("from airway_bill_trn a, airway_bill_mst_tbl am");
                strQuery.Append("where am.airway_bill_mst_pk = a.airway_bill_mst_fk");
                strQuery.Append("and am.location_mst_fk=" + usrLocFK);
                strQuery.Append("and am.airline_mst_fk=" + AirwayPk);
                strQuery.Append("and a.reference_no='" + AirbillRefNr + "'");

                return ObjWk.GetDataSet(strQuery.ToString());
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

        #endregion "Fetch MAwb Nr If Spot Rate Select"

        #region "Manual fetch"

        public DataSet FunManualRetriveDataAir(short intIsKGS = 0, string strPOL = "", string strPOD = "", string strChargeWeightNo = "", Int32 intContainerPk = 0, Int32 intNoOfContainers = 0, Int32 BookingPK = 0, Int32 SplitCargo = 0)
        {
            try
            {
                OracleDataAdapter Da = new OracleDataAdapter();
                DataSet dsGrid = new DataSet();
                WorkFlow objWFT = new WorkFlow();
                strChargeWeightNo = strChargeWeightNo.Replace(",", "");
                objWFT.MyCommand.CommandType = CommandType.StoredProcedure;
                objWFT.MyCommand.Parameters.Clear();
                var _with63 = objWFT.MyCommand.Parameters;
                _with63.Add("IS_KGS_IN", intIsKGS).Direction = ParameterDirection.Input;
                _with63.Add("POL_IN", strPOL).Direction = ParameterDirection.Input;
                _with63.Add("POD_IN", strPOD).Direction = ParameterDirection.Input;
                _with63.Add("CHARGE_WT_NO_IN", (string.IsNullOrEmpty(strChargeWeightNo) ? "0" : strChargeWeightNo)).Direction = ParameterDirection.Input;
                _with63.Add("CONTAINER_PK_IN", intContainerPk).Direction = ParameterDirection.Input;
                _with63.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with63.Add("BOOKINGPK", BookingPK).Direction = ParameterDirection.Input;
                _with63.Add("SPLITCARGO", SplitCargo).Direction = ParameterDirection.Input;
                _with63.Add("RES_CURSOR_ALL", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with63.Add("RES_CURSOR_FREIGHT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsGrid = objWFT.GetDataSet("BOOKING_AIR_FETCH_PKG", "FETCH_MANUAL_DATA");
                if (dsGrid.Tables.Count > 1)
                {
                    FetchManualOFreightsAir(dsGrid.Tables[0]);
                    if (dsGrid.Tables[1].Rows.Count > 0)
                    {
                        DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] { dsGrid.Tables[0].Columns["TRNTYPEPK"] }, new DataColumn[] { dsGrid.Tables[1].Columns["TRNTYPEFK"] });
                        dsGrid.Relations.Clear();
                        dsGrid.Relations.Add(rel);
                    }
                }
                return dsGrid;
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

        private void FetchManualOFreightsAir(DataTable dtMain)
        {
            try
            {
                Int32 rowCnt = default(Int32);
                DataTable dtOFreights = new DataTable();
                for (rowCnt = 0; rowCnt <= dtMain.Rows.Count - 1; rowCnt++)
                {
                    FetchManualothFreightsAir(dtOFreights);
                    if (dtOFreights.Rows.Count > 0)
                    {
                        dtMain.Rows[rowCnt]["OCHARGES"] = Cls_FlatRateFreights.GetOTHstring(dtOFreights, 0, 1, 2, 3, 4, "-1", 0, 0, new DataTable(), 0, 0);
                    }
                }
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

        private void FetchManualothFreightsAir(DataTable dtOFreights)
        {
            //Fetch Quotation Other Freights
            try
            {
                System.Text.StringBuilder strBuilder1 = new System.Text.StringBuilder();
                strBuilder1.Append("SELECT ");
                strBuilder1.Append("F.FREIGHT_ELEMENT_MST_PK,  ");
                strBuilder1.Append(" C.CURRENCY_MST_PK,  ");
                strBuilder1.Append("F.CHARGE_BASIS,  ");
                strBuilder1.Append("NULL BASIS_RATE,  ");
                strBuilder1.Append("'-1' PK, ");
                strBuilder1.Append("1 PYMT_TYPE ");
                strBuilder1.Append("FROM ");
                strBuilder1.Append("  FREIGHT_ELEMENT_MST_TBL F , ");
                strBuilder1.Append(" CURRENCY_TYPE_MST_TBL C ");
                strBuilder1.Append("WHERE F.ACTIVE_FLAG=1");
                strBuilder1.Append(" AND C.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                strBuilder1.Append(" AND nvl(FREIGHT_TYPE,0) = 0 ");
                strBuilder1.Append(" and  1 = 2");
                dtOFreights = (new WorkFlow()).GetDataTable(strBuilder1.ToString());
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

        #endregion "Manual fetch"

        #region "Fetch Air Existing Booking Entry"

        public DataSet FetchBEntryAir(DataSet dsBEntry, long lngSBEPk, Int16 EBkg = 0, Int16 Bstatus = 1)
        {
            try
            {
                DataTable dtMain = new DataTable();
                DataTable dtTrans = new DataTable();
                DataTable dtFreight = new DataTable();
                string QuotPk = null;
                string POLPK = null;
                string PODPK = null;
                string CHRWT = null;
                dtMain = (DataTable)FetchSBEntryHeaderAir(dtMain, lngSBEPk);
                dtTrans = (DataTable)FetchSBEntryTransAir(dtTrans, lngSBEPk);
                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (EBkg == 1 & Bstatus == 1)
                    {
                        dtFreight = (DataTable)FetchEBKGEntryFreightAir(dtFreight, lngSBEPk);
                    }
                    else
                    {
                        dtFreight = (DataTable)FetchSBEntryFreightAir(dtFreight, lngSBEPk, EBkg);
                    }
                }
                else
                {
                    dtFreight = (DataTable)FetchSBEntryFreightAir(dtFreight, lngSBEPk);
                }
                FetchXABEOFreightsAir(dtTrans);
                dsBEntry.Tables.Add(dtTrans);
                dsBEntry.Tables.Add(dtFreight);
                if (dsBEntry.Tables.Count > 1)
                {
                    if (dsBEntry.Tables[1].Rows.Count > 0)
                    {
                        DataRelation rel = new DataRelation("rl_TRAN_FREIGHT", new DataColumn[] {
                            dsBEntry.Tables[0].Columns["REFNO"],
                            dsBEntry.Tables[0].Columns["BASIS"]
                        }, new DataColumn[] {
                            dsBEntry.Tables[1].Columns["REFNO"],
                            dsBEntry.Tables[1].Columns["BASIS"]
                        });
                        dsBEntry.Relations.Clear();
                        dsBEntry.Relations.Add(rel);
                    }
                }
                //Data set ds contains the Master Table details
                //It is explicitly returned back to the calling function as value parameter
                //The Transaction records are referred back through reference
                DataSet ds = new DataSet();
                ds.Tables.Add(dtMain);

                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (EBkg == 1 & Bstatus == 1)
                    {
                        if (dtTrans.Rows.Count > 0)
                        {
                            if (Convert.ToInt32(dtTrans.Rows[0]["TRNTYPESTATUS"]) == 1)
                            {
                                QuotPk = Fetch_Quote_Pk(dtTrans.Rows[0]["REFNO"].ToString());
                            }
                        }
                        //dtFreight = FetchEBKGEntryFreight(dtFreight, lngSBEPk)
                    }
                }
                return ds;
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

        public object FetchSBEntryHeaderAir(DataTable dtMain, long lngSBEPK)
        {
            return FetchSBEntryHeader(dtMain, lngSBEPK, 1);
        }

        private void FetchXABEOFreightsAir(DataTable dtTrans)
        {
            Int32 rowCnt = default(Int32);
            DataTable dtOFreights = new DataTable();
            for (rowCnt = 0; rowCnt <= dtTrans.Rows.Count - 1; rowCnt++)
            {
                FetchSBOFreight(dtOFreights, Convert.ToInt64(dtTrans.Rows[rowCnt]["TRANPK"]));
                if (dtOFreights.Rows.Count > 0)
                {
                    Cls_FlatRateFreights Cls_FlatRateFreights = new Cls_FlatRateFreights();
                    dtTrans.Rows[rowCnt]["OCHARGES"] = Cls_FlatRateFreights.GetOTHstring(dtOFreights, 0, 1, 2, 3, 4, "-1", 0, 0, new DataTable(), 0, 0);
                }
            }
        }

        private void FetchSBOFreightAir(DataTable dtOFreights, string strXABEPk)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                sb.Append("SELECT ");
                sb.Append("BTAOC.FREIGHT_ELEMENT_MST_FK, ");
                sb.Append("BTAOC.CURRENCY_MST_FK, ");
                sb.Append("BTAOC.CHARGE_BASIS, ");
                sb.Append("BTAOC.BASIS_RATE, ");
                sb.Append("'-1' PK, ");
                sb.Append("FREIGHT_TYPE ");
                sb.Append("FROM ");
                sb.Append("BOOKING_TRN_OTH_CHRG BTAOC ");
                sb.Append("WHERE BTAOC.BOOKING_TRN_MST_FK= ");
                sb.Append(strXABEPk);
                dtOFreights = (new WorkFlow()).GetDataTable(sb.ToString());
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

        public object FetchSBEntryTransAir(DataTable dtTrans, long lngSBEPK)
        {
            WorkFlow objwf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT NULL AS TRNTYPEPK,");
            sb.Append("                BTA.TRANS_REFERED_FROM AS TRNTYPESTATUS,");
            sb.Append("                DECODE(BTA.TRANS_REFERED_FROM,");
            sb.Append("                       1,");
            sb.Append("                       'Quote',");
            sb.Append("                       2,");
            sb.Append("                       'SpRate',");
            sb.Append("                       3,");
            sb.Append("                       'Cont',");
            sb.Append("                       4,");
            sb.Append("                       'AirTar',");
            sb.Append("                       5,");
            sb.Append("                       'GenTar',");
            sb.Append("                       6,");
            sb.Append("                       'SRR',");
            sb.Append("                       7,");
            sb.Append("                       'Manual') AS TRNTYPE,");
            sb.Append("                BTA.TRANS_REF_NO AS REFNO,");
            sb.Append("                AMT.AIRLINE_ID AS AIRID,");
            sb.Append("                NVL(AST.BREAKPOINT_ID, '') AS BASIS,");
            sb.Append("                BTA.QUANTITY AS QTY,");
            sb.Append("                CMT.COMMODITY_ID AS COMM,");
            sb.Append("                BTA.BUYING_RATE AS RATE,");
            sb.Append("                BTA.ALL_IN_TARIFF AS BKGRATE,");
            sb.Append("                NULL AS NET,");
            sb.Append("                NULL AS TOTALRATE,");
            sb.Append("                NULL AS OCBUTTON,");
            sb.Append("                NULL AS OCHARGES,");
            sb.Append("                '1' AS SEL,");
            sb.Append("                BTA.COMMODITY_MST_FKS AS COMMODITYFK,");
            sb.Append("                NVL(BAT.CARRIER_MST_FK, 0) AS AIRLINEFK,");
            sb.Append("                BTA.BOOKING_TRN_PK AS TRANPK,");
            sb.Append("                NVL(AST.AIRFREIGHT_SLABS_TBL_PK, 0) AS BASISPK");
            sb.Append("  FROM BOOKING_MST_TBL         BAT,");
            sb.Append("       BOOKING_TRN             BTA,");
            sb.Append("       AIRFREIGHT_SLABS_TBL    AST,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       COMMODITY_MST_TBL       CMT,");
            sb.Append("       AIRLINE_MST_TBL         AMT");
            sb.Append(" WHERE (1 = 1)");
            sb.Append("   AND BTA.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
            sb.Append("   AND BTA.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
            sb.Append("   AND BAT.BOOKING_MST_PK = BTA.BOOKING_MST_FK");
            sb.Append("   AND BAT.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+)");
            sb.Append("   AND BTA.BASIS = AST.AIRFREIGHT_SLABS_TBL_PK(+)");
            sb.Append("   AND BAT.BOOKING_MST_PK=" + lngSBEPK);

            try
            {
                dtTrans = objwf.GetDataTable(sb.ToString());
                return dtTrans;
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

        public object FetchEBKGEntryFreightAir(DataTable dtFreight, long lngSBEPK)
        {
            WorkFlow objwf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT NULL AS TRNTYPEFK,");
            sb.Append("       BTA.TRANS_REF_NO AS REFNO,");
            sb.Append("       NVL(AST.BREAKPOINT_ID, '') AS BASIS,");
            sb.Append("       BTA.COMMODITY_MST_FK AS COMMODITYFK,");
            sb.Append("       BAT.PORT_MST_POL_FK AS POLPK,");
            sb.Append("       PL.PORT_ID AS POL,");
            sb.Append("       BAT.PORT_MST_POD_FK AS PODPK,");
            sb.Append("       PD.PORT_ID AS POD,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("       'FALSE' AS CHECK_FOR_ALL_IN_RT,");
            sb.Append("       '' as CURRENCY_MST_FK,");
            sb.Append("       '' as CURRENCY_ID,");
            sb.Append("       DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("              '1',");
            sb.Append("              '%',");
            sb.Append("              '2',");
            sb.Append("              'Flat Rate',");
            sb.Append("              '3',");
            sb.Append("              'Kgs',");
            sb.Append("              '4',");
            sb.Append("              'Unit') as CHARGE_BASIS,");
            sb.Append("       NULL AS BASIS_RATE,");
            sb.Append("       NULL AS RATE,");
            sb.Append("       NULL AS BKGRATE,");
            sb.Append("       NULL AS BASISPK,");
            sb.Append("       'PrePaid' AS PYMT_TYPE,");
            sb.Append("       '' as BOOKING_TRN_AIR_FRT_PK");
            sb.Append("  FROM BOOKING_MST_TBL         BAT,");
            sb.Append("       BOOKING_TRN             BTA,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       COMMODITY_MST_TBL       CMT,");
            sb.Append("       PORT_MST_TBL            PL,");
            sb.Append("       PORT_MST_TBL            PD,");
            sb.Append("       AIRFREIGHT_SLABS_TBL    AST");
            sb.Append(" WHERE (1 = 1)");
            sb.Append("   AND BTA.BOOKING_MST_FK = BAT.BOOKING_MST_PK");
            sb.Append("   AND BTA.BASIS = AST.AIRFREIGHT_SLABS_TBL_PK(+)");
            sb.Append("   AND BAT.PORT_MST_POL_FK = PL.PORT_MST_PK");
            sb.Append("   AND BAT.PORT_MST_POD_FK = PD.PORT_MST_PK");
            sb.Append("   AND BTA.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
            sb.Append("   AND BAT.BOOKING_MST_PK = " + lngSBEPK);
            sb.Append("   AND CTMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
            sb.Append("   AND FEMT.BUSINESS_TYPE = 1");
            sb.Append("   AND nvl(FEMT.FREIGHT_TYPE, 0) <> 0");
            sb.Append(" ORDER BY FEMT.PREFERENCE");

            try
            {
                dtFreight = objwf.GetDataTable(sb.ToString());
                return dtFreight;
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

        public object FetchSBEntryFreightAir(DataTable dtFreight, long lngSBEPK, Int16 Ebkg = 0)
        {
            WorkFlow objwf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT NULL AS TRNTYPEFK,");
            sb.Append("       BTA.TRANS_REF_NO AS REFNO,");
            sb.Append("       AST.BREAKPOINT_ID AS BASIS,");
            sb.Append("       BTA.COMMODITY_MST_FK AS COMMODITYFK,");
            sb.Append("       BAT.PORT_MST_POL_FK AS PORT_MST_PK,");
            //POLPK
            sb.Append("       PL.PORT_ID AS POL,");
            sb.Append("       BAT.PORT_MST_POD_FK AS PORT_MST_PK1,");
            //PODPK
            sb.Append("       PD.PORT_ID AS POD,");
            sb.Append("       BTAFD.FREIGHT_ELEMENT_MST_FK,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("       DECODE(BTAFD.CHECK_FOR_ALL_IN_RT, 1, 'TRUE', 'FALSE') AS CHECK_FOR_ALL_IN_RT,");
            sb.Append("       BTAFD.CURRENCY_MST_FK,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       DECODE(BTAFD.CHARGE_BASIS, 1, '%', 2, 'Flat', 3, 'Kgs', 4, 'Unit'),");
            sb.Append("       BTAFD.MIN_BASIS_RATE MIN_RATE,");
            sb.Append("       NULL AS RATE,");
            sb.Append("       BTAFD.TARIFF_RATE AS BKGRATE,");
            sb.Append("       BTA.BASIS AS BASIS_PK,");
            sb.Append("       DECODE(BTAFD.PYMT_TYPE, 1, 'PrePaid', 2, 'Collect', 3, 'Foreign') AS PYMT_TYPE,");
            sb.Append("       FEMT.CREDIT,BTAFD.CHECK_ADVATOS, BTAFD.BOOKING_TRN_FRT_PK");
            sb.Append("  FROM BOOKING_MST_TBL         BAT,");
            sb.Append("       BOOKING_TRN             BTA,");
            sb.Append("       BOOKING_TRN_FRT_DTLS    BTAFD,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       COMMODITY_MST_TBL       CMT,");
            sb.Append("       PORT_MST_TBL            PL,");
            sb.Append("       PORT_MST_TBL            PD,");
            sb.Append("       AIRFREIGHT_SLABS_TBL    AST");
            sb.Append(" WHERE (1 = 1)");
            sb.Append("   AND BTA.BOOKING_TRN_PK = BTAFD.BOOKING_TRN_FK");
            sb.Append("   AND BTAFD.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK");
            sb.Append("   AND BTAFD.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND BTA.BOOKING_MST_FK = BAT.BOOKING_MST_PK");
            sb.Append("   AND BTA.BASIS = AST.AIRFREIGHT_SLABS_TBL_PK(+)");
            sb.Append("   AND BAT.PORT_MST_POL_FK = PL.PORT_MST_PK");
            sb.Append("   AND BAT.PORT_MST_POD_FK = PD.PORT_MST_PK");
            sb.Append("   AND BTA.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
            //sb.Append("   --AND BAT.BUSINESS_TYPE=1")
            sb.Append("  AND BAT.BOOKING_MST_PK= " + lngSBEPK);
            sb.Append(" ORDER BY FEMT.PREFERENCE");

            try
            {
                dtFreight = objwf.GetDataTable(sb.ToString());
                return dtFreight;
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

        public object FetchCDimensionAir(DataTable dtMain, long lngABEPK)
        {
            WorkFlow objwf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT ROWNUM                     AS SNO,");
            sb.Append("       BACC.CARGO_NOP             AS NOP,");
            sb.Append("       BACC.CARGO_LENGTH          AS LENGTH,");
            sb.Append("       BACC.CARGO_WIDTH           AS WIDTH,");
            sb.Append("       BACC.CARGO_HEIGHT          AS HEIGHT,");
            sb.Append("       BACC.CARGO_CUBE            AS CUBE,");
            sb.Append("       BACC.CARGO_VOLUME_WT       AS VOLWEIGHT,");
            sb.Append("       BACC.CARGO_ACTUAL_WT       AS ACTWEIGHT,");
            sb.Append("       BACC.CARGO_DENSITY         AS DENSITY,");
            sb.Append("       BACC.BOOKING_CARGO_CALC_PK AS PK,");
            sb.Append("       BACC.CARGO_MEASUREMENT     AS MEASURE,");
            sb.Append("       BACC.BOOKING_MST_FK");
            sb.Append("  FROM BOOKING_CARGO_CALC BACC");
            sb.Append("       WHERE BACC.BOOKING_MST_FK= " + lngABEPK);

            try
            {
                dtMain = objwf.GetDataTable(sb.ToString());
                return dtMain;
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

        #endregion "Fetch Air Existing Booking Entry"

        #endregion "AIR Related Fetch"

        //*********************************************************************************************************
        public DataSet FetchBookingRptDetails(long BkgPk)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with64 = objWK.MyCommand.Parameters;
                _with64.Add("BOOKING_MST_FK_IN", BkgPk).Direction = ParameterDirection.Input;
                _with64.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("BOOKING_MST_PKG", "FETCH_BKGRPT_DETAILS");
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

        #region "GetPLRPFDTransporterName"

        //'Added By Sushama
        public string GetPLRPFDTransporterName(string PLRPK, string CargoType, string PLRPFDflag, Int16 BizType = 2)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            if (PLRPFDflag == "TRANSPORTER")
            {
                sb.Append("   SELECT V.VENDOR_NAME FROM VENDOR_MST_TBL V ");
                sb.Append("   WHERE V.VENDOR_MST_PK = " + PLRPK);
            }
            else
            {
                if (BizType == 2)
                {
                    //'FCL
                    if (CargoType == "1")
                    {
                        sb.Append("   SELECT P.PORT_NAME FROM PORT_MST_TBL P ");
                        sb.Append("   WHERE P.PORT_MST_PK = " + PLRPK);
                    }
                    else
                    {
                        sb.Append("   SELECT P.PLACE_NAME FROM PLACE_MST_TBL P ");
                        sb.Append("   WHERE P.PLACE_PK = " + PLRPK);
                    }
                }
                else
                {
                    sb.Append("   SELECT P.PLACE_NAME FROM PLACE_MST_TBL P ");
                    sb.Append("   WHERE P.PLACE_PK = " + PLRPK);
                }
            }
            try
            {
                return objWF.ExecuteScaler(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }

        #endregion "GetPLRPFDTransporterName"

        #region "Get Corporate Details"

        public DataSet GetCorporateDtls()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT CM.CORPORATE_NAME, CMT.COUNTRY_NAME");
            sb.Append(" FROM CORPORATE_MST_TBL CM, COUNTRY_MST_TBL CMT ");
            sb.Append("  WHERE CM.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Get Corporate Details"

        #region "SaveAirLineSchedule"

        public ArrayList SaveAirLineScheduleMst(DataSet dsMain, string flag_no_in = "")
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            int RESULT = 0;
            long airlineMstPK = 0;
            string FLAG_NO = null;
            string TransitTime = null;

            try
            {
                OracleCommand insCommand = new OracleCommand();
                Int16 VER = default(Int16);
                for (int i = 0; i <= dsMain.Tables[0].Rows.Count - 1; i++)
                {
                    var _with65 = insCommand;
                    //If dsMain.Tables(0).Rows(i).Item("AIRLINE_SCHED_MST_PK") = 0 Then
                    _with65.Transaction = TRAN;
                    _with65.Connection = objWK.MyConnection;
                    _with65.CommandType = CommandType.StoredProcedure;
                    _with65.CommandText = objWK.MyUserName + ".AIRLINE_SCHEDULE_PKG.BKG_AL_SCH_TBL_TRN_INS";
                    _with65.Parameters.Add("CARRIER_FK_IN", dsMain.Tables[0].Rows[i]["CARRIER_MST_FK"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("FLIGHT_NO_IN", dsMain.Tables[0].Rows[i]["VOYAGE_FLIGHT_NO"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("AOO_FK_IN", dsMain.Tables[0].Rows[i]["PORT_MST_POL_FK"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("AOD_FK_IN", dsMain.Tables[0].Rows[i]["PORT_MST_POD_FK"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("CUT_OFF_TIME_IN", dsMain.Tables[0].Rows[i]["CUT_OFF_DATE"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("VALID_FROM_DATE_IN", dsMain.Tables[0].Rows[i]["ETD_DATE"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("VALID_TO_DATE_IN", dsMain.Tables[0].Rows[i]["ETD_DATE"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("FLAG_NO_IN", flag_no_in).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("VERSION_NO_IN", dsMain.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with65.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    RESULT = _with65.ExecuteNonQuery();
                    airlineMstPK = Convert.ToInt64(_with65.Parameters["RETURN_VALUE"].Value);
                    _with65.Parameters.Clear();
                    //Else
                    //FLAG_NO = dsMain.Tables(0).Rows(i).Item("FLAG_NO")
                    //airlineMstPK = dsMain.Tables(0).Rows(i).Item("AIRLINE_SCHED_MST_PK")
                    //RESULT = 1
                    //End If
                    //If airlineMstPK > 0 And RESULT = 1 Then
                    //    RESULT = SaveAirLineScheduleTrn(TRAN, airlineMstPK, dsTrn.Tables("TABLE" & FLAG_NO))
                    //End If
                }
                if (RESULT == 1)
                {
                    arrMessage.Add("Saved");
                    TRAN.Commit();
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException OraExp)
            {
                arrMessage.Add(OraExp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        public long SaveAirLineScheduleTrn(OracleTransaction TRAN, long AirLineMstFk, DataTable dtTrn)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.MyConnection = TRAN.Connection;
            int RESULT = 0;
            string return_Value = null;
            string airlineSchedTrnPKs = "0";
            try
            {
                OracleCommand insCommand = new OracleCommand();
                OracleCommand updCommand = new OracleCommand();
                Int16 VER = default(Int16);
                for (int i = 0; i <= dtTrn.Rows.Count - 1; i++)
                {
                    if (Convert.ToInt32(dtTrn.Rows[i]["AIRLINE_SCHEDULE_TRN_PK"]) <= 0)
                    {
                        var _with66 = insCommand;
                        _with66.Transaction = TRAN;
                        _with66.Connection = objWK.MyConnection;
                        _with66.CommandType = CommandType.StoredProcedure;
                        _with66.CommandText = objWK.MyUserName + ".AIRLINE_SCHEDULE_PKG.AIRLINE_SCHEDULE_TRN_INS";
                        _with66.Parameters.Add("AIRLINE_SCHEDULE_MST_FK_IN", AirLineMstFk).Direction = ParameterDirection.Input;
                        _with66.Parameters.Add("ETD_AT_AOO_IN", Convert.ToDateTime(dtTrn.Rows[i]["ETD_AT_AOO"])).Direction = ParameterDirection.Input;
                        _with66.Parameters.Add("CUT_OFF_DATE_IN", Convert.ToDateTime(dtTrn.Rows[i]["CUT_OFF_DATE"])).Direction = ParameterDirection.Input;
                        _with66.Parameters.Add("ETA_AT_AOD_IN", Convert.ToDateTime(dtTrn.Rows[i]["ETA_AT_AOD"])).Direction = ParameterDirection.Input;
                        _with66.Parameters.Add("ACTIVE_IN", dtTrn.Rows[i]["ACTIVE"]).Direction = ParameterDirection.Input;
                        _with66.Parameters.Add("FLAG_NO_IN", dtTrn.Rows[i]["FLAG_NO"]).Direction = ParameterDirection.Input;
                        _with66.Parameters.Add("VERSION_NO_IN", dtTrn.Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        _with66.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                        _with66.Parameters.Add("CONFIG_MST_FK_IN", 4446).Direction = ParameterDirection.Input;
                        _with66.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        RESULT = _with66.ExecuteNonQuery();
                        return_Value = Convert.ToString(_with66.Parameters["RETURN_VALUE"].Value);
                        _with66.Parameters.Clear();
                    }
                    else
                    {
                        var _with67 = updCommand;
                        _with67.Transaction = TRAN;
                        _with67.Connection = objWK.MyConnection;
                        _with67.CommandType = CommandType.StoredProcedure;
                        _with67.CommandText = objWK.MyUserName + ".AIRLINE_SCHEDULE_PKG.AIRLINE_SCHEDULE_TRN_UPD";
                        _with67.Parameters.Add("AIRLINE_SCHEDULE_TRN_PK_IN", dtTrn.Rows[i]["AIRLINE_SCHEDULE_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with67.Parameters.Add("AIRLINE_SCHEDULE_MST_FK_IN", AirLineMstFk).Direction = ParameterDirection.Input;
                        _with67.Parameters.Add("ACTIVE_IN", dtTrn.Rows[i]["ACTIVE"]).Direction = ParameterDirection.Input;
                        _with67.Parameters.Add("VERSION_NO_IN", dtTrn.Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        _with67.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                        _with67.Parameters.Add("CONFIG_MST_FK_IN", 4446).Direction = ParameterDirection.Input;
                        _with67.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        RESULT = _with67.ExecuteNonQuery();
                        return_Value = Convert.ToString(_with67.Parameters["RETURN_VALUE"].Value);
                        _with67.Parameters.Clear();
                    }
                    airlineSchedTrnPKs = airlineSchedTrnPKs + "," + dtTrn.Rows[i]["AIRLINE_SCHEDULE_TRN_PK"];
                }
                if (Convert.ToInt32(dtTrn.Rows[0]["AIRLINE_SCHEDULE_TRN_PK"]) > 0)
                {
                    // DeleteAirlineSchedTrn(TRAN, airlineSchedTrnPKs, dtTrn.Rows(0).Item("VERSION_NO"), 4446, AirLineMstFk)
                }
                return RESULT;
            }
            catch (OracleException OraExp)
            {
                arrMessage.Add(OraExp.Message);
                return RESULT;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return RESULT;
            }
        }

        #endregion "SaveAirLineSchedule"
    }

    public class SpecialReqClass
    {
        public string BKG_TRN_SPL_PK;
        public string BKG_TRN_FK;

        //1-HAZ,2-REEFER,3-ODC
        public int REQ_TYPE;

        public string REFERNCE_NO;
        public string REFERNCE_TYPE;
        public string RECORD_INDEX;

        //REEFER
        public string RH_MAX_TEMP_UOM;

        public string RH_MIN_TEMP;
        public string RH_MAX_TEMP;
        public string RH_MIN_TEMP_UOM;
        public string R_AIR_COOL_METHOD;
        public string R_AIR_VENTILATION;
        public string R_AIR_VENTILATION_UOM;
        public string R_CO2;
        public string R_DEFROSTING_INTERVAL;
        public string R_DEHUMIDIFIER;
        public string R_FLOORDRAINS;
        public string R_GENSET;
        public string R_HAULAGE;
        public string R_HUMIDITY_FACTOR;
        public string R_IS_PERISHABLE_GOODS;
        public string R_O2;
        public string R_PACK_COUNT;
        public string R_PACK_TYPE_MST_FK;
        public string R_REF_VENTILATION;
        public string R_REQ_SET_TEMP;
        public string R_REQ_SET_TEMP_UOM;
        public string R_VENTILATION;

        //ODC
        public string O_APPR_REQ;

        public string O_HANDLING_INSTR;
        public string O_HEIGHT;
        public string O_HEIGHT_UOM_MST_FK;
        public string O_LASHING_INSTR;
        public string O_LENGTH;
        public string O_LENGTH_UOM_MST_FK;
        public string O_LOSS_QUANTITY;
        public string O_NO_OF_SLOTS;
        public string O_SLOT_LOSS;
        public string O_STOWAGE;
        public string O_VOLUME;
        public string O_VOLUME_UOM_MST_FK;
        public string O_WEIGHT;
        public string O_WEIGHT_UOM_MST_FK;
        public string O_WIDTH;
        public string O_WIDTH_UOM_MST_FK;

        //HAZ
        public string H_EMS_NUMBER;

        public string H_FLASH_PNT_TEMP;
        public string H_FLASH_PNT_TEMP_UOM;
        public string H_IMDG_CLASS_CODE;
        public string H_IMO_SURCHARGE;
        public string H_INNER_PACK_TYPE_MST_FK;
        public string H_IS_MARINE_POLLUTANT;
        public string H_OUTER_PACK_TYPE_MST_FK;
        public string H_PACK_CLASS_TYPE;
        public string H_PROPER_SHIPPING_NAME;
        public string H_SURCHARGE_AMT;
        public string H_UN_NO;
    }
}